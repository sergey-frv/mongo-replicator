package replicator

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// defaultClusterEvery — интервал сводки трафика всех процессов цели.
const defaultClusterEvery = 60 * time.Second

// Traffic — счётчики байт через сокеты драйвера: In — получено, Out — отправлено.
// Считаются реальные байты по сети (после TLS и сжатия), включая heartbeat,
// RTT-мониторинг и аутентификацию; заголовки TCP/IP, ретрансмиты и DNS — нет.
type Traffic struct{ SrcIn, SrcOut, DstIn, DstOut atomic.Uint64 }

// TrafficInfo — снимок счётчиков Traffic.
type TrafficInfo struct{ SrcIn, SrcOut, DstIn, DstOut uint64 }

// SrcDialer — диалер клиента источника: options.ClientOptions.SetDialer до подключения.
func (t *Traffic) SrcDialer() options.ContextDialer {
	return countingDialer{in: &t.SrcIn, out: &t.SrcOut}
}

// DstDialer — диалер клиента цели.
func (t *Traffic) DstDialer() options.ContextDialer {
	return countingDialer{in: &t.DstIn, out: &t.DstOut}
}

// Snapshot — текущие значения; у nil — нули.
func (t *Traffic) Snapshot() TrafficInfo {
	if t == nil {
		return TrafficInfo{}
	}
	return TrafficInfo{SrcIn: t.SrcIn.Load(), SrcOut: t.SrcOut.Load(), DstIn: t.DstIn.Load(), DstOut: t.DstOut.Load()}
}

// sub — прирост счётчиков со снимка prev.
func (t TrafficInfo) sub(prev TrafficInfo) TrafficInfo {
	return TrafficInfo{SrcIn: t.SrcIn - prev.SrcIn, SrcOut: t.SrcOut - prev.SrcOut,
		DstIn: t.DstIn - prev.DstIn, DstOut: t.DstOut - prev.DstOut}
}

// ckptFields — счётчики по полям traffic.* документа чекпойнта (long).
func (t TrafficInfo) ckptFields() map[string]int64 {
	//nolint:gosec // байт за процесс меньше 2^63
	return map[string]int64{
		"traffic.src_in": int64(t.SrcIn), "traffic.src_out": int64(t.SrcOut),
		"traffic.dst_in": int64(t.DstIn), "traffic.dst_out": int64(t.DstOut),
	}
}

// saveTraffic передаёт write прирост счётчиков с последней успешной записи в чекпойнт
// и сдвигает снимок, только если write удалась: неудачная дельта войдёт в следующую.
// Вызывается из горутины продления и из Release после возврата Run — не одновременно.
func (r *Replicator) saveTraffic(write func(d TrafficInfo) error) error {
	cur := r.cfg.Traffic.Snapshot()
	if err := write(cur.sub(r.ckptTraffic)); err != nil {
		return err
	}
	r.ckptTraffic = cur
	return nil
}

// ClusterTraffic — сводка трафика по документам чекпойнта всех процессов цели.
type ClusterTraffic struct {
	At        time.Time   // время сводки
	Processes int         // документы с позицией
	Active    int         // из них с живой арендой
	Traffic   TrafficInfo // сумма накопленного в документах
}

// clusterTraffic суммирует трафик, накопленный в документах чекпойнта. Документы
// без позиции (процесс стартует или старт не удался) не учитываются; в документе,
// ещё не продлевавшем аренду, трафика нет — $sum по отсутствующему полю даёт 0.
func (r *Replicator) clusterTraffic(ctx context.Context) (ClusterTraffic, error) {
	group := bson.M{
		"_id":       nil,
		"processes": bson.M{"$sum": 1},
		"active":    bson.M{"$sum": bson.M{"$cond": bson.A{bson.M{"$gt": bson.A{"$lease_until", "$$NOW"}}, 1, 0}}},
	}
	for f := range (TrafficInfo{}).ckptFields() {
		group[f[len("traffic."):]] = bson.M{"$sum": "$" + f}
	}
	cur, err := r.ckpt().Aggregate(ctx, mongo.Pipeline{
		{{Key: "$match", Value: bson.M{"token": bson.M{"$exists": true}}}},
		{{Key: "$group", Value: group}},
	})
	if err != nil {
		return ClusterTraffic{}, fmt.Errorf("сводка трафика: %w", err)
	}
	var res []struct {
		Processes int   `bson:"processes"`
		Active    int   `bson:"active"`
		SrcIn     int64 `bson:"src_in"`
		SrcOut    int64 `bson:"src_out"`
		DstIn     int64 `bson:"dst_in"`
		DstOut    int64 `bson:"dst_out"`
	}
	if err := cur.All(ctx, &res); err != nil {
		return ClusterTraffic{}, fmt.Errorf("сводка трафика: %w", err)
	}
	ct := ClusterTraffic{At: time.Now()}
	if len(res) > 0 {
		g := res[0]
		ct.Processes, ct.Active = g.Processes, g.Active
		//nolint:gosec // суммы записанных дельт неотрицательны
		ct.Traffic = TrafficInfo{SrcIn: uint64(g.SrcIn), SrcOut: uint64(g.SrcOut),
			DstIn: uint64(g.DstIn), DstOut: uint64(g.DstOut)}
	}
	return ct, nil
}

// logCluster раз в clusterEvery пишет сводку трафика всех процессов цели; первую —
// сразу после старта.
func (r *Replicator) logCluster(ctx context.Context) {
	if time.Since(r.lastCluster) < r.clusterEvery || ctx.Err() != nil {
		return
	}
	r.lastCluster = time.Now()
	ct, err := r.clusterTraffic(ctx)
	if err != nil {
		if ctx.Err() == nil {
			log.Warn().Err(err).Msg("сводка трафика всех процессов не получена")
		}
		return
	}
	r.update(func(s *Status) { s.Cluster = &ct })
	log.Info().Int("processes", ct.Processes).Int("active", ct.Active).
		Uint64("src_in", ct.Traffic.SrcIn).Uint64("src_out", ct.Traffic.SrcOut).
		Uint64("dst_in", ct.Traffic.DstIn).Uint64("dst_out", ct.Traffic.DstOut).
		Msg("трафик всех процессов")
}

// trafficFields добавляет в запись лога счётчики с начала процесса и среднюю
// скорость (байт/с) за интервал с прошлого снимка.
func trafficFields(e *zerolog.Event, cur, prev TrafficInfo, elapsed time.Duration) *zerolog.Event {
	bps := func(cur, prev uint64) uint64 {
		if elapsed <= 0 {
			return 0
		}
		return uint64(float64(cur-prev) / elapsed.Seconds())
	}
	return e.Uint64("src_in", cur.SrcIn).Uint64("src_out", cur.SrcOut).
		Uint64("dst_in", cur.DstIn).Uint64("dst_out", cur.DstOut).
		Uint64("src_in_bps", bps(cur.SrcIn, prev.SrcIn)).Uint64("src_out_bps", bps(cur.SrcOut, prev.SrcOut)).
		Uint64("dst_in_bps", bps(cur.DstIn, prev.DstIn)).Uint64("dst_out_bps", bps(cur.DstOut, prev.DstOut))
}

// countingDialer — как диалер драйвера по умолчанию (нулевой net.Dialer), но соединения считают байты.
type countingDialer struct{ in, out *atomic.Uint64 }

func (d countingDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	var nd net.Dialer
	c, err := nd.DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	return &countingConn{Conn: c, in: d.in, out: d.out}, nil
}

// countingConn считает переданные байты, в том числе при ошибке; ошибки не оборачивает:
// драйвер различает таймауты через net.Error.
type countingConn struct {
	net.Conn
	in, out *atomic.Uint64
}

func (c *countingConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	c.in.Add(uint64(n)) //nolint:gosec // n из Read неотрицателен
	return n, err
}

func (c *countingConn) Write(b []byte) (int, error) {
	n, err := c.Conn.Write(b)
	c.out.Add(uint64(n)) //nolint:gosec // n из Write неотрицателен
	return n, err
}
