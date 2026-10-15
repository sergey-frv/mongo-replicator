package replicator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// echoServer принимает одно соединение: читает want байт и отвечает reply байтами.
func echoServer(t *testing.T, want, reply int) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		if _, err := io.CopyN(io.Discard, c, int64(want)); err != nil {
			return
		}
		_, _ = c.Write(bytes.Repeat([]byte{'r'}, reply))
	}()
	return ln.Addr().String()
}

func TestTrafficDialers(t *testing.T) {
	const sent, got = 1000, 2500
	var tr Traffic
	for _, tc := range []struct {
		name    string
		dialer  options.ContextDialer
		in, out *atomic.Uint64
	}{
		{"источник", tr.SrcDialer(), &tr.SrcIn, &tr.SrcOut},
		{"цель", tr.DstDialer(), &tr.DstIn, &tr.DstOut},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addr := echoServer(t, sent, got)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			c, err := tc.dialer.DialContext(ctx, "tcp", addr)
			require.NoError(t, err)
			defer c.Close()
			require.NoError(t, c.SetDeadline(time.Now().Add(5*time.Second)))

			n, err := c.Write(bytes.Repeat([]byte{'s'}, sent))
			require.NoError(t, err)
			require.Equal(t, sent, n)
			_, err = io.ReadFull(c, make([]byte, got))
			require.NoError(t, err)

			assert.Equal(t, uint64(sent), tc.out.Load())
			assert.Equal(t, uint64(got), tc.in.Load())
		})
	}
	assert.Equal(t, TrafficInfo{SrcIn: got, SrcOut: sent, DstIn: got, DstOut: sent}, tr.Snapshot())
}

// partialConn отдаёт часть байт вместе с ошибкой.
type partialConn struct {
	net.Conn
	n   int
	err error
}

func (c partialConn) Read([]byte) (int, error)  { return c.n, c.err }
func (c partialConn) Write([]byte) (int, error) { return c.n, c.err }

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

func TestCountingConnPartial(t *testing.T) {
	var in, out atomic.Uint64
	c := &countingConn{Conn: partialConn{n: 3, err: timeoutErr{}}, in: &in, out: &out}

	n, err := c.Read(make([]byte, 10))
	assert.Equal(t, 3, n)
	var ne net.Error
	require.ErrorAs(t, err, &ne)
	assert.True(t, ne.Timeout())
	assert.Equal(t, timeoutErr{}, err, "ошибка не обёрнута")

	n, err = c.Write(make([]byte, 10))
	assert.Equal(t, 3, n)
	assert.Equal(t, timeoutErr{}, err)

	assert.Equal(t, uint64(3), in.Load())
	assert.Equal(t, uint64(3), out.Load())
}

func TestCountingConnDeadline(t *testing.T) {
	var tr Traffic
	addr := echoServer(t, 1, 0)
	c, err := tr.SrcDialer().DialContext(context.Background(), "tcp", addr)
	require.NoError(t, err)
	defer c.Close()

	require.NoError(t, c.SetReadDeadline(time.Now().Add(-time.Second)))
	_, err = c.Read(make([]byte, 1))
	var ne net.Error
	require.ErrorAs(t, err, &ne, "таймаут виден драйверу как net.Error")
	assert.True(t, ne.Timeout())
	assert.Zero(t, tr.SrcIn.Load())
}

func TestTrafficDialClosedPort(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())

	var tr Traffic
	c, err := tr.DstDialer().DialContext(context.Background(), "tcp", addr)
	require.Error(t, err)
	assert.Nil(t, c)
	assert.Equal(t, TrafficInfo{}, tr.Snapshot())
}

func TestTrafficSnapshotNil(t *testing.T) {
	var tr *Traffic
	assert.Equal(t, TrafficInfo{}, tr.Snapshot())
}

func TestTrafficFields(t *testing.T) {
	var buf bytes.Buffer
	l := zerolog.New(&buf)
	prev := TrafficInfo{SrcIn: 1000, SrcOut: 100, DstIn: 10, DstOut: 500}
	cur := TrafficInfo{SrcIn: 21000, SrcOut: 200, DstIn: 30, DstOut: 10500}
	trafficFields(l.Info(), cur, prev, 10*time.Second).Send()

	var got map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &got))
	delete(got, "level")
	assert.Equal(t, map[string]any{
		"src_in": 21000.0, "src_out": 200.0, "dst_in": 30.0, "dst_out": 10500.0,
		"src_in_bps": 2000.0, "src_out_bps": 10.0, "dst_in_bps": 2.0, "dst_out_bps": 1000.0,
	}, got)
}

func TestTrafficFieldsZeroElapsed(t *testing.T) {
	var buf bytes.Buffer
	l := zerolog.New(&buf)
	trafficFields(l.Info(), TrafficInfo{SrcIn: 5}, TrafficInfo{}, 0).Send()
	assert.Contains(t, buf.String(), `"src_in_bps":0`)
}

// Дельта трафика в чекпойнт: снимок сдвигается только после успешной записи,
// поэтому неудачная дельта входит в следующую.
func TestSaveTraffic(t *testing.T) {
	tr := &Traffic{}
	r := New(nil, nil, Config{DB: "shop", Traffic: tr})
	var got TrafficInfo
	write := func(err error) func(TrafficInfo) error {
		return func(d TrafficInfo) error {
			got = d
			return err
		}
	}

	tr.SrcIn.Add(100)
	tr.DstOut.Add(40)
	failed := errors.New("сеть")
	require.ErrorIs(t, r.saveTraffic(write(failed)), failed)
	assert.Equal(t, TrafficInfo{SrcIn: 100, DstOut: 40}, got)

	tr.SrcIn.Add(5)
	tr.SrcOut.Add(7)
	require.NoError(t, r.saveTraffic(write(nil)))
	assert.Equal(t, TrafficInfo{SrcIn: 105, SrcOut: 7, DstOut: 40}, got, "с пропущенной дельтой")

	tr.DstIn.Add(3)
	require.NoError(t, r.saveTraffic(write(nil)))
	assert.Equal(t, TrafficInfo{DstIn: 3}, got, "только прирост с успешной записи")
}

// Без счётчиков дельта нулевая: продление и Release пишут нули.
func TestSaveTrafficNil(t *testing.T) {
	r := New(nil, nil, Config{DB: "shop"})
	var got TrafficInfo
	require.NoError(t, r.saveTraffic(func(d TrafficInfo) error {
		got = d
		return nil
	}))
	assert.Equal(t, TrafficInfo{}, got)
}

func TestTrafficCkptFields(t *testing.T) {
	assert.Equal(t, map[string]int64{
		"traffic.src_in": 1, "traffic.src_out": 2, "traffic.dst_in": 3, "traffic.dst_out": 4,
	}, TrafficInfo{SrcIn: 1, SrcOut: 2, DstIn: 3, DstOut: 4}.ckptFields())
}
