package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/sergey-frv/mongo-replicator/internal/replicator"
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
		{"догон", 10 * time.Second, replicator.StateRunning, 2401, 12000, 18*time.Hour + 40*time.Minute, 0, ""},
		{"ровный поток", 35 * time.Second, replicator.StateRunning, 4, 19, 300 * time.Millisecond, 0, ""},
		{"повтор", 45 * time.Second, replicator.StateRetry, 0, 0, -1, 1,
			"bulk write в urlshrinker.links: connection reset by peer (демо)"},
		{"поток после повтора", 55 * time.Second, replicator.StateRunning, 4, 18, 700 * time.Millisecond, 0, ""},
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

func TestSimAppliedAt(t *testing.T) {
	demo := newSim(simStart)

	demo.step(simStart.Add(35 * time.Second))

	assert.Equal(t, simStart.Add(35*time.Second-300*time.Millisecond), demo.status().AppliedAt)
}

func TestSimLastDDL(t *testing.T) {
	for _, c := range []struct {
		name string
		up   time.Duration
		want *replicator.DDLInfo
	}{
		{"ещё нет", 20 * time.Second, nil},
		{"есть", 21 * time.Second, &replicator.DDLInfo{At: simStart.Add(21 * time.Second), Op: "createIndexes",
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

	enter := demo.step(simStart.Add(45 * time.Second))
	again := demo.step(simStart.Add(46 * time.Second))

	assert.Equal(t, []string{"ошибка репликации, повтор (демо)"}, enter, "вход в повтор")
	assert.Empty(t, again, "второй шаг в окне повтора")
}

func TestSimPeriodicWarn(t *testing.T) {
	demo := newSim(simStart)

	first := demo.step(simStart.Add(11 * time.Second))
	early := demo.step(simStart.Add(20 * time.Second))
	next := demo.step(simStart.Add(37 * time.Second))

	want := []string{"urlshrinker.links: коллекция создана без опций источника, проверьте вручную (демо)"}
	assert.Equal(t, want, first, "первый после 10 с")
	assert.Empty(t, early, "9 с после предыдущего")
	assert.Equal(t, want, next, "26 с после предыдущего")
}

func TestSimStopped(t *testing.T) {
	demo := newSim(simStart)
	demo.step(simStart.Add(10 * time.Second))

	demo.stopped()

	assert.Equal(t, replicator.StateStopped, demo.status().State)
}
