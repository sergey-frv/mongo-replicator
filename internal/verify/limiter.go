package verify

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"
)

// limiter — «маркерное ведро» для --rate: ёмкость и пополнение — rate маркеров в
// секунду, один маркер — один запрошенный документ источника (S1 и S2 считаются
// оба). Часы подставляются: now — текущее время, after — таймер.
type limiter struct {
	rate  float64
	now   func() time.Time
	after func(time.Duration) <-chan time.Time

	mu     sync.Mutex
	tokens float64 // отрицательное — долг зарезервировавших
	last   time.Time
}

func newLimiter(rate int, now func() time.Time, after func(time.Duration) <-chan time.Time) *limiter {
	return &limiter{rate: float64(rate), now: now, after: after, tokens: float64(rate), last: now()}
}

// wait ждёт n маркеров. Маркеры резервируются сразу, в порядке вызовов, и
// ожидается только долг: пачка не обгоняется меньшими. Отмена контекстом
// возвращает резерв. n больше ёмкости — ошибка вызывающего: пачка ограничена
// min(maxBatch, rate).
func (l *limiter) wait(ctx context.Context, n int) error {
	if float64(n) > l.rate {
		return fmt.Errorf("запрошено %d маркеров при ёмкости %.0f", n, l.rate)
	}
	l.mu.Lock()
	now := l.now()
	l.tokens = min(l.rate, l.tokens+now.Sub(l.last).Seconds()*l.rate)
	l.last = now
	l.tokens -= float64(n)
	debt := -l.tokens
	l.mu.Unlock()
	if debt <= 0 {
		return nil
	}
	select {
	case <-l.after(time.Duration(math.Ceil(debt / l.rate * float64(time.Second)))):
		return nil
	case <-ctx.Done():
		l.mu.Lock()
		l.tokens += float64(n)
		l.mu.Unlock()
		return ctx.Err()
	}
}
