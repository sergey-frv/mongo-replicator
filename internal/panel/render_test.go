package panel

import (
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

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

func TestSparkline(t *testing.T) {
	for _, tc := range []struct {
		name string
		s    series
		want string
	}{
		{"пусто", seriesOf(), "                    "},
		{"все нули", seriesOf(0, 0, 0, 0, 0, 0), "                  ▁▁"},
		{"меньше 60 точек — пробелы слева", seriesOf(1, 1, 1, 2, 2, 2), "                  ▅█"},
		{"60 точек — 20 колонок по 3", seriesOf(numbers(60)...), "▁▂▂▂▃▃▃▄▄▄▅▅▆▆▆▇▇▇██"},
		{"неполная правая колонка — среднее её точек", seriesOf(2, 2, 2, 7), "                  ▃█"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := sparkline(tc.s, sparkCols)

			assert.Equal(t, tc.want, got)
		})
	}
}

func TestSparklineShift(t *testing.T) {
	before := seriesOf(numbers(60)...)
	after := seriesOf(append(numbers(60), 0)...)

	assert.Equal(t, "▁▂▂▂▃▃▃▄▄▄▅▅▆▆▆▇▇▇██", sparkline(before, sparkCols))
	assert.Equal(t, "▂▂▂▃▃▃▄▄▄▅▅▆▆▆▇▇▇██▁", sparkline(after, sparkCols),
		"полные колонки сдвинулись на одну влево, новая точка — в новой колонке")
}

func TestSparkColumns(t *testing.T) {
	s := seriesOf(numbers(62)...)

	cols := sparkColumns(s, sparkCols)

	assert.Equal(t, []float64{5, 8, 11, 14, 17, 20, 23, 26, 29, 32, 35, 38, 41, 44, 47, 50, 53, 56, 59, 61.5}, cols,
		"колонка точек 1–3 неполна после вытеснения и не показывается, правая — две точки")
}

func TestProgressBar(t *testing.T) {
	for _, tc := range []struct {
		frac float64
		want string
	}{
		{0, "░░░░░░░░░░░░░░░░░░░░"},
		{0.58, "████████████░░░░░░░░"},
		{1, "████████████████████"},
		{-0.5, "░░░░░░░░░░░░░░░░░░░░"},
		{1.5, "████████████████████"},
	} {
		assert.Equal(t, tc.want, progressBar(tc.frac, barCols), "доля %v", tc.frac)
	}
}

func TestPad(t *testing.T) {
	for _, tc := range []struct {
		name string
		s    string
		want string
	}{
		{"кириллица", "лаг", "лаг   "},
		{"стилизованный", labelStyle.Render("лаг"), labelStyle.Render("лаг") + "   "},
		{"шире", "скорость", "скорость"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := pad(tc.s, 6)

			assert.Equal(t, tc.want, got)
		})
	}
}

func TestFit(t *testing.T) {
	for _, tc := range []struct {
		name string
		n    int
		want string
	}{
		{"помещается", 15, "всего ↓ 881 MiB"},
		{"усечена", 10, "всего ↓ 8…"},
		{"минимум", 8, "всего ↓…"},
		{"меньше минимума", 7, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := fit("всего ↓ 881 MiB", tc.n)

			assert.Equal(t, tc.want, got)
		})
	}
}

func TestFitStyled(t *testing.T) {
	s := labelStyle.Render("всего ↓ ") + "881 MiB"

	got := fit(s, 10)

	assert.Equal(t, "всего ↓ 8…", ansi.ReplaceAllString(got, ""))
}
