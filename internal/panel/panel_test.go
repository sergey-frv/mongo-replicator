package panel

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sergey-frv/mongo-replicator/internal/replicator"
	"github.com/sergey-frv/mongo-replicator/internal/statefile"
	"github.com/sergey-frv/mongo-replicator/internal/traffic"
)

// ansi — цветовые escape-последовательности ConsoleWriter
var ansi = regexp.MustCompile(`\x1b\[[0-9;]*m`)

const (
	KiB = 1 << 10
	MiB = 1 << 20
	GiB = 1 << 30
)

// panelLines — строки панели без цветовых последовательностей
func panelLines(st replicator.Status, at time.Time, prev traffic.Info, prevAt time.Time) []string {
	return strings.Split(ansi.ReplaceAllString(renderPanel(st, at, prev, prevAt), ""), "\n")
}

func clock(h, m, s int) time.Time { return time.Date(2026, 10, 15, h, m, s, 0, time.Local) }

func TestRenderPanel(t *testing.T) {
	at := clock(12, 1, 35)
	st := replicator.Status{
		DB: "urlshrinker", DstDB: "urlshrinker_v2",
		Started:   at.Add(-(2*time.Hour + 14*time.Minute + 20*time.Second)),
		Applied:   1284551,
		Rate:      312,
		AppliedAt: clock(12, 1, 33),
		Lag:       1500 * time.Millisecond,
		LastBatch: &replicator.BatchInfo{At: clock(12, 1, 34), Ops: replicator.OpCounts{Ins: 30, Upd: 15, Del: 3},
			Colls: []string{"links", "clicks"}},
		LastDDL: &replicator.DDLInfo{At: clock(11, 40, 2), Op: "createIndexes", NS: "urlshrinker.links", Details: "slug_1"},
		Traffic: traffic.Info{SrcIn: 3*GiB + 100*MiB, SrcOut: 12 * MiB, DstIn: 40 * MiB, DstOut: 2*GiB + 900*MiB},
		Machine: &statefile.Machine{At: clock(12, 1, 0), Processes: 3, Active: 2,
			Traffic: traffic.Info{SrcIn: 12*GiB + 400*MiB, SrcOut: 300 * MiB, DstIn: 200 * MiB, DstOut: 11*GiB + 900*MiB}},
		State: replicator.StateRunning,
	}
	prev := st.Traffic
	prev.SrcIn -= 420 * KiB
	prev.DstOut -= 390 * KiB

	lines := panelLines(st, at, prev, at.Add(-time.Second))

	assert.Equal(t, []string{
		"urlshrinker → urlshrinker_v2 · с запуска 2ч14м · лаг 1с (применено до 12:01:33)",
		"операции: 1 284 551 всего · 312/с · пачка 12:01:34: 48 (ins 30 · upd 15 · del 3) в links, clicks",
		"DDL: 11:40:02 createIndexes urlshrinker.links (slug_1)",
		"трафик процесса: источник ↓ 3.1 GiB 420 KiB/с ↑ 12 MiB · цель ↓ 40 MiB ↑ 2.9 GiB 390 KiB/с",
		"на машине (3, активно 2, на 12:01): источник ↓ 12.4 GiB ↑ 300 MiB · цель ↓ 200 MiB ↑ 11.9 GiB",
		"состояние: работает",
	}, lines)
}

func TestRenderPanelMinimal(t *testing.T) {
	at := clock(12, 0, 5)
	// без пачки, DDL и сводки, лаг неизвестен, предыдущего снимка нет — без скоростей
	st := replicator.Status{DB: "a", DstDB: "b", Started: at.Add(-5 * time.Second), Rate: 0.3, Lag: -1,
		Traffic: traffic.Info{SrcIn: 512, SrcOut: 1536}, State: replicator.StateStarting}

	lines := panelLines(st, at, traffic.Info{}, time.Time{})

	assert.Equal(t, []string{
		"a → b · с запуска 5с · лаг неизвестен",
		"операции: 0 всего · 0.3/с",
		"трафик процесса: источник ↓ 512 B ↑ 1.5 KiB · цель ↓ 0 B ↑ 0 B",
		"состояние: запуск",
	}, lines)
}

func TestRenderPanelRetry(t *testing.T) {
	at := clock(12, 0, 0)
	st := replicator.Status{DB: "a", DstDB: "a", Started: at.Add(-3*time.Minute - 5*time.Second), Lag: 0,
		LastBatch: &replicator.BatchInfo{At: clock(11, 59, 0), Ops: replicator.OpCounts{Rep: 2},
			Colls: []string{"x", "y", "z"}, More: 2},
		State: replicator.StateRetry, Retry: 2, LastErr: "connection refused"}

	lines := panelLines(st, at, traffic.Info{}, time.Time{})
	raw := renderPanel(st, at, traffic.Info{}, time.Time{})

	assert.Equal(t, []string{
		"a → a · с запуска 3м5с · лаг 0с",
		"операции: 0 всего · 0/с · пачка 11:59:00: 2 (rep 2) в x, y, z и ещё 2",
		"трафик процесса: источник ↓ 0 B ↑ 0 B · цель ↓ 0 B ↑ 0 B",
		"состояние: повтор 2: connection refused",
	}, lines)
	assert.Contains(t, raw, retryStyle.Render("повтор 2"), "повтор выделен")
	assert.Contains(t, raw, errStyle.Render("connection refused"), "ошибка выделена")
}

func TestRenderPanelStopped(t *testing.T) {
	at := clock(12, 0, 0)
	st := replicator.Status{DB: "a", DstDB: "a", Started: at.Add(-time.Minute), Lag: 0,
		State: replicator.StateStopped, LastErr: "аренду забрал другой процесс"}

	lines := panelLines(st, at, traffic.Info{}, time.Time{})

	assert.Equal(t, "состояние: остановлен: аренду забрал другой процесс", lines[len(lines)-1])
}

func TestFmtBytes(t *testing.T) {
	for n, want := range map[uint64]string{
		0:                 "0 B",
		1023:              "1023 B",
		1024:              "1 KiB",
		1536:              "1.5 KiB",
		MiB - 1:           "1024 KiB",
		12 * MiB:          "12 MiB",
		GiB + GiB/10:      "1.1 GiB",
		3000 * GiB:        "3000 GiB",
		100*MiB + 512*KiB: "100.5 MiB",
	} {
		assert.Equal(t, want, fmtBytes(n), "%d", n)
	}
}

func TestFmtDuration(t *testing.T) {
	for d, want := range map[time.Duration]string{
		-time.Second:                    "0с",
		0:                               "0с",
		999 * time.Millisecond:          "0с",
		59 * time.Second:                "59с",
		time.Minute:                     "1м0с",
		3*time.Minute + 5*time.Second:   "3м5с",
		2*time.Hour + 14*time.Minute:    "2ч14м",
		23*time.Hour + 59*time.Minute:   "23ч59м",
		3*24*time.Hour + 2*time.Hour:    "3д2ч",
		3*24*time.Hour + 59*time.Minute: "3д0ч",
	} {
		assert.Equal(t, want, fmtDuration(d), "%s", d)
	}
}

func TestFmtCount(t *testing.T) {
	for n, want := range map[int64]string{0: "0", 999: "999", 1000: "1 000", 1284551: "1 284 551"} {
		assert.Equal(t, want, fmtCount(n), "%d", n)
	}
}

func TestPanelUpdate(t *testing.T) {
	n := 0
	m := newPanel(func() replicator.Status {
		n++
		return replicator.Status{Applied: int64(n), Traffic: traffic.Info{SrcIn: uint64(n)}}
	})
	assert.True(t, m.prevAt.IsZero(), "до первого тика скоростей нет")

	_, cmd := m.Update(tickMsg(time.Now()))
	require.NotNil(t, cmd, "тик планирует следующий")
	assert.Equal(t, int64(2), m.st.Applied)
	assert.Equal(t, uint64(1), m.prev.SrcIn)
	assert.False(t, m.prevAt.IsZero())

	_, cmd = m.Update(finalMsg{})
	assert.Equal(t, int64(3), m.st.Applied, "финальный снимок")
	assert.True(t, m.prevAt.IsZero(), "в финальной панели скоростей нет")
	require.NotNil(t, cmd)
	assert.IsType(t, tea.QuitMsg{}, cmd())
}

// writeTimeout — Write в teaWriter должна вернуться, иначе тест падает, а не висит
func writeTimeout(t *testing.T, w teaWriter, s string) {
	t.Helper()
	res := make(chan error, 1)
	go func() {
		_, err := w.Write([]byte(s))
		res <- err
	}()
	select {
	case err := <-res:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "запись в teaWriter заблокировалась")
	}
}

func TestTeaWriterAfterExit(t *testing.T) {
	var out, fallback bytes.Buffer
	p := tea.NewProgram(newPanel(func() replicator.Status { return replicator.Status{Lag: -1} }),
		tea.WithInput(nil), tea.WithoutSignalHandler(), tea.WithOutput(&out))
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = p.Run()
	}()
	p.Quit()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "программа не завершилась")
	}

	// Run вернулся, но done писатель ещё не видит: Send после выхода — no-op
	writeTimeout(t, teaWriter{p: p, done: make(chan struct{}), fallback: &fallback}, "до done\n")
	beforeDone := fallback.String()
	writeTimeout(t, teaWriter{p: p, done: done, fallback: &fallback}, "после done\n")

	assert.Empty(t, beforeDone, "до done запись не уходит в fallback")
	assert.Equal(t, "после done\n", fallback.String(), "после done запись уходит в fallback")
}

// assertInOrder — строки lines есть в s после after, каждая — после предыдущей
func assertInOrder(t *testing.T, s, after string, lines []string) {
	t.Helper()
	_, rest, ok := strings.Cut(s, after)
	if !assert.True(t, ok, "нет %q", after) {
		return
	}
	for _, line := range lines {
		if _, rest, ok = strings.Cut(rest, line); !assert.True(t, ok, "%s — после предыдущей", line) {
			return
		}
	}
}

func TestStartPanel(t *testing.T) {
	var out bytes.Buffer
	lines := make([]string, 0, 20)
	for i := range 20 {
		lines = append(lines, fmt.Sprintf("WRN строка %02d", i))
	}
	console, stop, kill := Start(func() replicator.Status {
		return replicator.Status{DB: "src", DstDB: "dst", Lag: -1, State: replicator.StateStopped}
	}, &out, tea.WithWindowSize(120, 10)) // вывод не терминал: без размера панель не рисуется

	writeTimeout(t, console.(teaWriter), "WRN над панелью\n")
	for _, line := range lines {
		writeTimeout(t, console.(teaWriter), line+"\n")
	}
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		stop()
		kill() // после stop — no-op
	}()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "панель не остановилась")
	}
	writeTimeout(t, console.(teaWriter), "после панели\n")

	s := out.String()
	assert.Contains(t, s, "WRN над панелью")
	// строки перед stop — все и по порядку: печать не обгоняет друг друга и выход
	assertInOrder(t, s, "WRN над панелью", lines)
	assert.Contains(t, s, "src → dst", "финальная панель")
	assert.Contains(t, s, "после панели\n")
}
