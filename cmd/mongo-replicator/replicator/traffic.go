package replicator

import (
	"context"
	"net"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// defaultMachineEvery — интервал сводки трафика на машине.
const defaultMachineEvery = 60 * time.Second

// Traffic — счётчики байт через сокеты драйвера: In — получено, Out — отправлено.
// Считаются реальные байты по сети (после TLS и сжатия), включая heartbeat,
// RTT-мониторинг и аутентификацию; заголовки TCP/IP, ретрансмиты и DNS — нет.
type Traffic struct{ SrcIn, SrcOut, DstIn, DstOut atomic.Uint64 }

// TrafficInfo — снимок счётчиков Traffic.
type TrafficInfo struct {
	SrcIn  uint64 `json:"src_in"`
	SrcOut uint64 `json:"src_out"`
	DstIn  uint64 `json:"dst_in"`
	DstOut uint64 `json:"dst_out"`
}

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

// add — сумма счётчиков.
func (t TrafficInfo) add(o TrafficInfo) TrafficInfo {
	return TrafficInfo{SrcIn: t.SrcIn + o.SrcIn, SrcOut: t.SrcOut + o.SrcOut,
		DstIn: t.DstIn + o.DstIn, DstOut: t.DstOut + o.DstOut}
}

// logMachine раз в machineEvery пишет сводку трафика по файлам состояния машины
// (Config.StateDir); первую — сразу после старта.
func (r *Replicator) logMachine(ctx context.Context) {
	if r.cfg.StateDir == "" || time.Since(r.lastMachine) < r.machineEvery || ctx.Err() != nil {
		return
	}
	r.lastMachine = time.Now()
	mt, err := machineTraffic(r.cfg.StateDir, r.lastMachine)
	if err != nil {
		log.Warn().Err(err).Msg("сводка трафика на машине не получена")
		return
	}
	r.update(func(s *Status) { s.Machine = &mt })
	log.Info().Int("processes", mt.Processes).Int("active", mt.Active).
		Uint64("src_in", mt.Traffic.SrcIn).Uint64("src_out", mt.Traffic.SrcOut).
		Uint64("dst_in", mt.Traffic.DstIn).Uint64("dst_out", mt.Traffic.DstOut).
		Msg("трафик на машине")
}

// trafficFields добавляет в запись лога счётчики с начала процесса и среднюю
// скорость (байт/с) за интервал elapsed (не нулевой) с прошлого снимка.
func trafficFields(e *zerolog.Event, cur, prev TrafficInfo, elapsed time.Duration) *zerolog.Event {
	bps := func(cur, prev uint64) uint64 { return uint64(float64(cur-prev) / elapsed.Seconds()) }
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
