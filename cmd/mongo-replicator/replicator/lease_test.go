package replicator

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func TestLeaseHeldErr(t *testing.T) {
	until := time.Date(2026, 10, 15, 12, 1, 33, 0, time.FixedZone("MSK", 3*3600))
	err := leaseHeldErr("shop_v2", "host-a:1:9f3a1c2e", until)
	require.ErrorIs(t, err, ErrLeaseHeld)
	assert.ErrorContains(t, err, "базу shop_v2 уже реплицирует host-a:1:9f3a1c2e")
	assert.ErrorContains(t, err, "аренда до 2026-10-15T09:01:33Z", "время — в UTC")
}

func TestLeaseLostErr(t *testing.T) {
	r := New(nil, nil, Config{DB: "shop", DstDB: "shop_v2"})
	err := fmt.Errorf("сохранение чекпойнта: %w", r.leaseLostErr())
	require.ErrorIs(t, err, ErrLeaseLost)
	assert.ErrorContains(t, err, "базу shop_v2")
	assert.ErrorContains(t, err, r.owner)
}

func TestNewOwner(t *testing.T) {
	host, err := os.Hostname()
	require.NoError(t, err)
	a := New(nil, nil, Config{DB: "a"}).owner
	b := New(nil, nil, Config{DB: "a"}).owner
	re := regexp.MustCompile("^" + regexp.QuoteMeta(host) + fmt.Sprintf(":%d:", os.Getpid()) + "[0-9a-f]{8}$")
	assert.Regexp(t, re, a)
	assert.Regexp(t, re, b)
	assert.NotEqual(t, a, b, "два экземпляра в одном процессе — разные владельцы")
}

func TestNewLeaseDefaults(t *testing.T) {
	r := New(nil, nil, Config{DB: "a"})
	assert.Equal(t, defaultLeaseTTL, r.leaseTTL)
	assert.Equal(t, defaultLeaseRenew, r.leaseRenew)
}

func TestLeaseExpiring(t *testing.T) {
	ok := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	const ttl = 60 * time.Second
	cases := []struct {
		since time.Duration
		want  bool
	}{
		{0, false},
		{10 * time.Second, false},
		{40 * time.Second, false}, // ровно 2/3 TTL — ещё нет
		{40*time.Second + time.Millisecond, true},
		{90 * time.Second, true},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, leaseExpiring(ok, ok.Add(c.since), ttl), "%s", c.since)
	}
}

// Монотонное показание с настенным время сна не разделить (time не даёт их рассогласовать),
// поэтому проверяется путь без монотонного показания у одной из сторон.
func TestLeaseElapsed(t *testing.T) {
	ok := time.Now()
	assert.Equal(t, time.Second, leaseElapsed(ok, ok.Add(time.Second)))
	assert.Equal(t, time.Minute, leaseElapsed(ok, ok.Round(0).Add(time.Minute)), "без монотонного показания")
	assert.Equal(t, time.Minute, leaseElapsed(ok.Round(0).Add(-time.Minute), ok))
}

// Перед записью: без успешного продления дольше 2/3 TTL писать нельзя.
func TestCheckLease(t *testing.T) {
	r := New(nil, nil, Config{DB: "a"})
	require.ErrorIs(t, r.checkLease(), errLeaseExpiring, "аренда не захвачена")
	now := time.Now()
	r.leaseOK.Store(&now)
	require.NoError(t, r.checkLease())
	stale := now.Add(-r.leaseTTL)
	r.leaseOK.Store(&stale)
	err := r.checkLease()
	require.ErrorIs(t, err, errLeaseExpiring)
	assert.NotErrorIs(t, err, ErrLeaseLost, "временная: повтор Run захватит аренду снова")
}

// holdLease отмечает аренду захваченной только что — для тестов записи без Run.
func holdLease(r *Replicator) {
	now := time.Now()
	r.leaseOK.Store(&now)
}

// Без живой аренды ни пачка, ни DDL на цель не уходят: ошибка аренды, а не сети.
func TestWritesRequireLease(t *testing.T) {
	r := New(nil, lazyClient(t), Config{DB: "d"})
	stale := time.Now().Add(-r.leaseTTL)
	r.leaseOK.Store(&stale)
	r.pending.add(namespace{"d", "c"}, "delete", mongo.NewDeleteOneModel().SetFilter(bson.D{{Key: "_id", Value: 1}}))
	require.ErrorIs(t, r.flush(context.Background()), errLeaseExpiring)
	ev := event{OperationType: "rename", NS: namespace{"d", "a"}, To: &namespace{"d", "b"}}
	require.ErrorIs(t, r.applyDDL(context.Background(), &ev), errLeaseExpiring)
	assert.Nil(t, r.Status().LastDDL)
}

// Продление не удаётся (цели нет) дольше 2/3 TTL: Run отменяется с errLeaseExpiring,
// но не раньше.
func TestRenewLeaseExpiring(t *testing.T) {
	quietLog(t)
	r := New(nil, lazyClient(t), Config{DB: "a"})
	r.leaseRenew, r.leaseTTL = 20*time.Millisecond, 300*time.Millisecond
	started := time.Now()
	r.leaseOK.Store(&started)
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)

	done := make(chan struct{})
	go func() {
		defer close(done)
		r.renewLease(ctx, cancel)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("продление не остановилось")
	}
	cause := context.Cause(ctx)
	require.ErrorIs(t, cause, errLeaseExpiring)
	assert.NotErrorIs(t, cause, ErrLeaseLost)
	assert.GreaterOrEqual(t, time.Since(started), r.leaseTTL*2/3, "не раньше 2/3 TTL")
}
