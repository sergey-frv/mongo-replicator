package verify

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeClock — подставные часы: after сразу сдвигает время на d и срабатывает,
// при stuck — не срабатывает никогда.
type fakeClock struct {
	mu    sync.Mutex
	start time.Time
	t     time.Time
	stuck bool
}

func newFakeClock() *fakeClock {
	start := time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC)
	return &fakeClock{start: start, t: start}
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) after(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stuck {
		return nil
	}
	c.t = c.t.Add(d)
	ch := make(chan time.Time, 1)
	ch <- c.t
	return ch
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func (c *fakeClock) setStuck(stuck bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stuck = stuck
}

// elapsed — сколько прошло по подставным часам: ожидания ограничителя и advance.
func (c *fakeClock) elapsed() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t.Sub(c.start)
}

// waitAll ждёт маркеры пачками ns по очереди.
func waitAll(t *testing.T, l *limiter, ns ...int) {
	t.Helper()
	for _, n := range ns {
		require.NoError(t, l.wait(t.Context(), n), "wait(%d)", n)
	}
}

func TestLimiterInitialBurst(t *testing.T) {
	c := newFakeClock()
	l := newLimiter(10, c.now, c.after)

	waitAll(t, l, 4, 6)

	assert.Equal(t, time.Duration(0), c.elapsed(), "начальный запас — ёмкость, ожидания нет")
}

func TestLimiterRate(t *testing.T) {
	c := newFakeClock()
	l := newLimiter(10, c.now, c.after)

	waitAll(t, l, 10, 5, 5, 10, 5)

	assert.Equal(t, 2500*time.Millisecond, c.elapsed(), "25 маркеров сверх запаса при 10 в секунду")
}

func TestLimiterFullBatchAfterDrain(t *testing.T) {
	c := newFakeClock()
	l := newLimiter(10, c.now, c.after)

	waitAll(t, l, 10, 10)

	assert.Equal(t, time.Second, c.elapsed(), "пачка размером с ёмкость ждёт полного пополнения")
}

func TestLimiterRefillCappedAtCapacity(t *testing.T) {
	c := newFakeClock()
	l := newLimiter(10, c.now, c.after)
	c.advance(10 * time.Second)

	waitAll(t, l, 10, 10)

	assert.Equal(t, 11*time.Second, c.elapsed(), "простой 10 с копит не больше ёмкости")
}

func TestLimiterPartialRefill(t *testing.T) {
	c := newFakeClock()
	l := newLimiter(10, c.now, c.after)
	waitAll(t, l, 10)
	c.advance(500 * time.Millisecond)

	waitAll(t, l, 10)

	assert.Equal(t, time.Second, c.elapsed(), "0,5 с пополнения и 0,5 с ожидания")
}

func TestLimiterCancel(t *testing.T) {
	c := newFakeClock()
	l := newLimiter(10, c.now, c.after)
	waitAll(t, l, 10)
	c.setStuck(true)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := l.wait(ctx, 5)

	require.ErrorIs(t, err, context.Canceled)
	c.setStuck(false)
	waitAll(t, l, 10)
	assert.Equal(t, time.Second, c.elapsed(), "отменённый резерв возвращён")
}

func TestLimiterOverCapacity(t *testing.T) {
	c := newFakeClock()
	l := newLimiter(3, c.now, c.after)

	err := l.wait(t.Context(), 4)

	require.Error(t, err)
}
