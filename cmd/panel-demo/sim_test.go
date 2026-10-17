package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/sergey-frv/mongo-replicator/internal/replicator"
	"github.com/sergey-frv/mongo-replicator/internal/statefile"
	"github.com/sergey-frv/mongo-replicator/internal/traffic"
)

var simStart = time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)

func TestSimStep(t *testing.T) {
	for _, c := range []struct {
		name    string
		up      time.Duration
		state   string
		applied int64
		rate    float64
		lag     time.Duration
		retry   int
		lastErr string
	}{
		{"запуск", time.Second, replicator.StateStarting, 0, 0, -1, 0, ""},
		{"начало потока", 2 * time.Second, replicator.StateRunning, 2401, 12000, 26*time.Hour + 8*time.Minute, 0, ""},
		{"догон", 10 * time.Second, replicator.StateRunning, 2401, 12000, 18*time.Hour + 40*time.Minute, 0, ""},
		{"хвост выше догона", 29970 * time.Millisecond, replicator.StateRunning, 7, 30, 120060 * time.Millisecond, 0, ""},
		{"начало хвоста", 30 * time.Second, replicator.StateRunning, 7, 30, 2 * time.Minute, 0, ""},
		{"хвост", 60 * time.Second, replicator.StateRunning, 7, 30, time.Minute, 0, ""},
		{"конец хвоста", 86 * time.Second, replicator.StateRunning, 7, 30, 8 * time.Second, 0, ""},
		{"ровный поток", 90 * time.Second, replicator.StateRunning, 3, 11, time.Second, 0, ""},
		{"начало повтора", 100 * time.Second, replicator.StateRetry, 0, 0, -1, 1,
			"bulk write в urlshrinker.links: connection reset by peer (демо)"},
		{"повтор", 105 * time.Second, replicator.StateRetry, 0, 0, -1, 1,
			"bulk write в urlshrinker.links: connection reset by peer (демо)"},
		{"конец повтора", 110 * time.Second, replicator.StateRunning, 3, 10, 1400 * time.Millisecond, 0, ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			demo := newSim(simStart)

			demo.step(simStart.Add(c.up))

			st := demo.status()
			assert.Equal(t, c.state, st.State)
			assert.Equal(t, c.applied, st.Applied)
			assert.Equal(t, c.rate, st.Rate)
			assert.Equal(t, c.lag, st.Lag)
			assert.Equal(t, c.retry, st.Retry)
			assert.Equal(t, c.lastErr, st.LastErr)
		})
	}
}

// После окна повтора поток снова идёт: повтор и ошибка сброшены.
func TestSimLeavesRetry(t *testing.T) {
	demo := newSim(simStart)
	demo.step(simStart.Add(105 * time.Second))

	demo.step(simStart.Add(115 * time.Second))

	st := demo.status()
	assert.Equal(t, replicator.StateRunning, st.State)
	assert.Equal(t, 0, st.Retry)
	assert.Empty(t, st.LastErr)
}

// Applied копится по шагам, а в окне повтора счётчики потока стоят.
func TestSimRetryKeepsCounters(t *testing.T) {
	demo := newSim(simStart)
	demo.step(simStart.Add(10 * time.Second))
	demo.step(simStart.Add(10*time.Second + 200*time.Millisecond))

	demo.step(simStart.Add(105 * time.Second))

	st := demo.status()
	assert.Equal(t, int64(4802), st.Applied, "два шага догона по 2401")
	assert.Equal(t, 12000.0, st.Rate)
	assert.Equal(t, 18*time.Hour+28*time.Minute+48*time.Second, st.Lag)
}

func TestSimAppliedAt(t *testing.T) {
	demo := newSim(simStart)

	demo.step(simStart.Add(97 * time.Second))

	assert.Equal(t, time.Date(2026, 10, 17, 12, 1, 36, 900_000_000, time.UTC), demo.status().AppliedAt)
}

// Пачка, трафик процесса и сводка машины на ровном потоке: 4 операции за шаг.
func TestSimBatchAndTraffic(t *testing.T) {
	demo := newSim(simStart)

	demo.step(simStart.Add(97 * time.Second))

	st := demo.status()
	assert.Equal(t, &replicator.BatchInfo{
		At:    time.Date(2026, 10, 17, 12, 1, 37, 0, time.UTC),
		Ops:   replicator.OpCounts{Ins: 2, Upd: 1, Del: 1},
		Colls: []string{"PrivateUrls", "Counters", "PublicUrls"},
	}, st.LastBatch)
	assert.Equal(t, traffic.Info{SrcIn: 2040, SrcOut: 60, DstIn: 88, DstOut: 680}, st.Traffic)
	assert.Equal(t, &statefile.Machine{
		At: time.Date(2026, 10, 17, 12, 1, 0, 0, time.UTC), Processes: 3, Active: 2,
		Traffic: traffic.Info{SrcIn: 12_884_903_928, SrcOut: 314_572_860, DstIn: 209_715_288, DstOut: 11_811_160_744},
	}, st.Machine)
}

func TestSimLastDDL(t *testing.T) {
	for _, c := range []struct {
		name string
		up   time.Duration
		want *replicator.DDLInfo
	}{
		{"ещё нет", 20 * time.Second, nil},
		{"есть", 21 * time.Second, &replicator.DDLInfo{At: time.Date(2026, 10, 17, 12, 0, 21, 0, time.UTC), Op: "createIndexes",
			NS: "urlshrinker.PublicUrls", Details: "slug_1"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			demo := newSim(simStart)

			demo.step(simStart.Add(c.up))

			assert.Equal(t, c.want, demo.status().LastDDL)
		})
	}
}

func TestSimRetryWarn(t *testing.T) {
	demo := newSim(simStart)

	enter := demo.step(simStart.Add(105 * time.Second))
	again := demo.step(simStart.Add(106 * time.Second))

	assert.Equal(t, []string{"ошибка репликации, повтор (демо)"}, enter, "вход в повтор")
	assert.Empty(t, again, "второй шаг в окне повтора")
}

func TestSimPeriodicWarn(t *testing.T) {
	demo := newSim(simStart)

	before := demo.step(simStart.Add(10 * time.Second))
	first := demo.step(simStart.Add(11 * time.Second))
	early := demo.step(simStart.Add(35*time.Second + 800*time.Millisecond))
	next := demo.step(simStart.Add(36 * time.Second))

	want := []string{"urlshrinker.links: коллекция создана без опций источника, проверьте вручную (демо)"}
	assert.Empty(t, before, "ровно 10 с с запуска")
	assert.Equal(t, want, first, "первый после 10 с")
	assert.Empty(t, early, "24,8 с после предыдущего")
	assert.Equal(t, want, next, "ровно 25 с после предыдущего")
}

func TestSimStopped(t *testing.T) {
	demo := newSim(simStart)
	demo.step(simStart.Add(10 * time.Second))

	demo.stopped()

	assert.Equal(t, replicator.StateStopped, demo.status().State)
}
