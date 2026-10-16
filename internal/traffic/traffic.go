// Package traffic — счётчики байт через сокеты клиентов драйвера (их диалеры).
package traffic

import (
	"context"
	"net"
	"sync/atomic"

	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Counters — счётчики байт через сокеты драйвера: In — получено, Out — отправлено.
// Считаются реальные байты по сети (после TLS и сжатия), включая heartbeat,
// RTT-мониторинг и аутентификацию; заголовки TCP/IP, ретрансмиты и DNS — нет.
type Counters struct{ SrcIn, SrcOut, DstIn, DstOut atomic.Uint64 }

// Info — снимок счётчиков Counters.
type Info struct {
	SrcIn  uint64 `json:"src_in"`
	SrcOut uint64 `json:"src_out"`
	DstIn  uint64 `json:"dst_in"`
	DstOut uint64 `json:"dst_out"`
}

// SrcDialer — диалер клиента источника: options.ClientOptions.SetDialer до подключения.
func (t *Counters) SrcDialer() options.ContextDialer {
	return countingDialer{in: &t.SrcIn, out: &t.SrcOut}
}

// DstDialer — диалер клиента цели.
func (t *Counters) DstDialer() options.ContextDialer {
	return countingDialer{in: &t.DstIn, out: &t.DstOut}
}

// Snapshot — текущие значения; у nil — нули.
func (t *Counters) Snapshot() Info {
	if t == nil {
		return Info{}
	}
	return Info{SrcIn: t.SrcIn.Load(), SrcOut: t.SrcOut.Load(), DstIn: t.DstIn.Load(), DstOut: t.DstOut.Load()}
}

// Add — сумма счётчиков.
func (t Info) Add(o Info) Info {
	return Info{SrcIn: t.SrcIn + o.SrcIn, SrcOut: t.SrcOut + o.SrcOut,
		DstIn: t.DstIn + o.DstIn, DstOut: t.DstOut + o.DstOut}
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
