package replicator

import (
	"context"
	"net"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

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
