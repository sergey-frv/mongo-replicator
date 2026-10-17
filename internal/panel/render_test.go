package panel

import (
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"charm.land/lipgloss/v2"
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

// panelLines — строки панели v в окне шириной width без цветовых последовательностей
func panelLines(v view, width int) []string {
	return strings.Split(ansi.ReplaceAllString(renderPanel(v, width), ""), "\n")
}

func clock(h, m, s int) time.Time { return time.Date(2026, 10, 15, h, m, s, 0, time.Local) }

// catchUpOf — догон после лагов lags, снятых раз в 10 с; последний — в at
func catchUpOf(at time.Time, lags ...time.Duration) catchUp {
	var c catchUp
	for i, lag := range lags {
		c.update(lag, at.Add(time.Duration(i-len(lags)+1)*10*time.Second))
	}
	return c
}

// workingView — работа после догона: все строки панели заполнены
func workingView() view {
	at := clock(19, 37, 20)
	return view{
		st: replicator.Status{
			DB: "urlshrinker", DstDB: "urlshrinker_v2",
			Started:   at.Add(-25 * time.Minute),
			Applied:   1727851,
			Rate:      19,
			AppliedAt: clock(19, 37, 18),
			Lag:       0,
			LastBatch: &replicator.BatchInfo{At: clock(19, 37, 18), Ops: replicator.OpCounts{Ins: 1},
				Colls: []string{"PrivateUrls"}},
			LastDDL: &replicator.DDLInfo{At: clock(11, 40, 2), Op: "createIndexes", NS: "urlshrinker.PublicUrls",
				Details: "slug_1"},
			Traffic: traffic.Info{SrcIn: 881 * MiB, SrcOut: 2*MiB + 700*KiB, DstIn: 36*MiB + 300*KiB, DstOut: 278 * MiB},
			Machine: &statefile.Machine{At: clock(19, 36, 0), Processes: 1, Active: 1,
				Traffic: traffic.Info{SrcIn: 881 * MiB, SrcOut: 2 * MiB, DstIn: 36 * MiB, DstOut: 277*MiB + 900*KiB}},
			State: replicator.StateRunning,
		},
		at:     at,
		rate:   seriesOf(numbers(60)...),
		srcIn:  seriesOf(2*KiB, 4*KiB+100),
		dstOut: seriesOf(3*KiB + 800),
	}
}

func TestRenderPanel(t *testing.T) {
	v := workingView()

	lines := panelLines(v, 100)

	assert.Equal(t, []string{
		"● работает  urlshrinker → urlshrinker_v2   с запуска 25м0с",
		"── репликация " + strings.Repeat("─", 86),
		"  лаг        0с             применено до 19:37:18",
		"  скорость   19/с           ▏▁▂▂▂▃▃▃▄▄▄▅▅▆▆▆▇▇▇██▕ 2–59/с",
		"  применено  1 727 851      пачка 19:37:18: 1 (ins 1) в PrivateUrls",
		"  DDL        11:40:02       createIndexes urlshrinker.PublicUrls (slug_1)",
		"── трафик " + strings.Repeat("─", 90),
		"  источник   ↓ 4.1 KiB/с    ▏                   █▕ всего ↓ 881 MiB ↑ 2.7 MiB",
		"  цель       ↑ 3.8 KiB/с    ▏                   █▕ всего ↑ 278 MiB ↓ 36.3 MiB",
		"  машина     процессов 1, активно 1 · на 19:36  всего ↓ 881 MiB ↑ 277.9 MiB",
	}, lines)
}

func TestRenderPanelRateThousands(t *testing.T) {
	v := workingView()
	v.st.Rate = 12000
	v.rate = seriesOf(8802, 8803, 8802.5, 12000)

	lines := panelLines(v, 100)

	assert.Equal(t, "  скорость   12 000/с       ▏                  ▆█▕ 8 803–12 000/с", lines[3],
		"число и диапазон — целые с группами разрядов, без дробей средних колонок")
}

func TestRenderPanelStarting(t *testing.T) {
	at := clock(12, 0, 5)
	// лаг неизвестен, точек графиков ещё нет, без пачки, DDL и сводки
	v := view{st: replicator.Status{DB: "a", DstDB: "b", Started: at.Add(-5 * time.Second), Rate: 0.3, Lag: -1,
		Traffic: traffic.Info{SrcIn: 512, SrcOut: 1536}, State: replicator.StateStarting}, at: at}

	lines := panelLines(v, 100)

	assert.Equal(t, []string{
		"◌ запуск  a → b   с запуска 5с",
		"── репликация " + strings.Repeat("─", 86),
		"  лаг        неизвестен",
		"  скорость   0.3/с          ▏                    ▕",
		"  применено  0",
		"── трафик " + strings.Repeat("─", 90),
		"  источник   ↓ —            ▏                    ▕ всего ↓ 512 B ↑ 1.5 KiB",
		"  цель       ↑ —            ▏                    ▕ всего ↑ 0 B ↓ 0 B",
	}, lines)
}

func TestRenderPanelCatchUp(t *testing.T) {
	v := workingView()
	v.st.Lag = 21 * time.Hour
	v.catchUp = catchUpOf(v.at, 50*time.Hour, 21*time.Hour)

	lines := panelLines(v, 100)

	assert.Equal(t, []string{
		"◐ догоняет  urlshrinker → urlshrinker_v2   с запуска 25м0с",
		"── репликация " + strings.Repeat("─", 86),
		"  лаг        21ч0м          ████████████░░░░░░░░ 58% · ~7с до 0",
		"  скорость   19/с           ▏▁▂▂▂▃▃▃▄▄▄▅▅▆▆▆▇▇▇██▕ 2–59/с",
	}, lines[:4])
}

func TestRenderPanelCatchUpNoEstimate(t *testing.T) {
	v := workingView()
	v.st.Lag = 50 * time.Hour
	v.catchUp = catchUpOf(v.at, 50*time.Hour)

	lines := panelLines(v, 100)

	assert.Equal(t, "  лаг        2д2ч           ░░░░░░░░░░░░░░░░░░░░ 0%", lines[2], "одна точка догона — только процент")
}

func TestRenderPanelRetryDuringCatchUp(t *testing.T) {
	v := workingView()
	v.st.Lag = 21 * time.Hour
	v.st.State, v.st.Retry, v.st.LastErr = replicator.StateRetry, 2, "connection refused"
	v.catchUp = catchUpOf(v.at, 50*time.Hour, 21*time.Hour)

	lines := panelLines(v, 100)

	assert.Equal(t, "↻ повтор 2  urlshrinker → urlshrinker_v2   с запуска 25м0с", lines[0])
	assert.Equal(t, "  лаг        21ч0м          применено до 19:37:18", lines[2], "без полосы и оценки")
	assert.Equal(t, "ошибка: connection refused", lines[len(lines)-1])
}

func TestRenderPanelStopped(t *testing.T) {
	for _, tc := range []struct {
		name    string
		lastErr string
		want    []string
	}{
		{"с ошибкой", "аренду забрал другой процесс", []string{
			"✗ остановлен  urlshrinker → urlshrinker_v2   с запуска 25м0с",
			"ошибка: аренду забрал другой процесс",
		}},
		{"без ошибки", "", []string{
			"✗ остановлен  urlshrinker → urlshrinker_v2   с запуска 25м0с",
			"  машина     процессов 1, активно 1 · на 19:36  всего ↓ 881 MiB ↑ 277.9 MiB",
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := workingView()
			v.st.State, v.st.LastErr = replicator.StateStopped, tc.lastErr

			lines := panelLines(v, 100)

			assert.Equal(t, tc.want, []string{lines[0], lines[len(lines)-1]}, "шапка и последняя строка")
		})
	}
}

func TestRenderPanelStateColors(t *testing.T) {
	at := clock(12, 0, 0)
	for _, tc := range []struct {
		name    string
		st      replicator.Status
		catchUp catchUp
		want    string
	}{
		{"работает", replicator.Status{State: replicator.StateRunning}, catchUp{},
			okStyle.Render("● работает")},
		{"догоняет", replicator.Status{State: replicator.StateRunning, Lag: 2 * time.Minute},
			catchUpOf(at, 2*time.Minute), warnStyle.Render("◐ догоняет")},
		{"повтор", replicator.Status{State: replicator.StateRetry, Retry: 3}, catchUp{},
			warnStyle.Render("↻ повтор 3")},
		{"запуск", replicator.Status{State: replicator.StateStarting}, catchUp{},
			labelStyle.Render("◌ запуск")},
		{"остановлен с ошибкой", replicator.Status{State: replicator.StateStopped, LastErr: "x"}, catchUp{},
			errStyle.Render("✗ остановлен")},
		{"остановлен без ошибки", replicator.Status{State: replicator.StateStopped}, catchUp{},
			labelStyle.Render("✗ остановлен")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := view{st: tc.st, at: at, catchUp: tc.catchUp}

			head, _, _ := strings.Cut(renderPanel(v, 100), "\n")

			assert.True(t, strings.HasPrefix(head, tc.want), "шапка %q", head)
		})
	}
}

func TestRenderPanelLagColors(t *testing.T) {
	for _, tc := range []struct {
		lag  time.Duration
		want string
	}{
		{9 * time.Second, lagGoodStyle.Render("9с")},
		{10 * time.Second, lagSlowStyle.Render("10с")},
		{59 * time.Second, lagSlowStyle.Render("59с")},
		{time.Minute, lagBadStyle.Render("1м0с")},
	} {
		v := view{st: replicator.Status{Lag: tc.lag, State: replicator.StateRunning}, at: clock(12, 0, 0)}

		raw := renderPanel(v, 100)

		assert.Contains(t, raw, tc.want, "лаг %s", tc.lag)
	}
}

func TestRenderPanelErrorColor(t *testing.T) {
	v := view{st: replicator.Status{State: replicator.StateStopped, LastErr: "connection refused"}, at: clock(12, 0, 0)}

	raw := renderPanel(v, 100)

	assert.True(t, strings.HasSuffix(raw, errStyle.Render("ошибка: connection refused")), "строка ошибки — последней")
}

func TestRenderPanelNewlines(t *testing.T) {
	v := workingView()
	v.st.LastDDL.Details = "slug_1\nunique"
	v.st.State, v.st.LastErr = replicator.StateStopped, "write failed:\r\nduplicate key"

	lines := panelLines(v, 100)

	assert.Len(t, lines, 11, "переводы строк не добавили строк")
	assert.Equal(t, "  DDL        11:40:02       createIndexes urlshrinker.PublicUrls (slug_1 unique)", lines[5])
	assert.Equal(t, "ошибка: write failed: duplicate key", lines[10])
}

func TestRenderPanelControlChars(t *testing.T) {
	v := workingView()
	v.st.DB = "url\x1b[2Jshrinker"
	v.st.LastBatch.Colls = []string{"Private\x1b]52;c;aGk=\x07Urls"}
	v.st.LastDDL.NS = "urlshrinker.\x9b31mPublicUrls"
	v.st.State, v.st.LastErr = replicator.StateStopped, "write failed: \x1b]8;;http://x\x1b\\key"

	lines := panelLines(v, 120)

	assert.Equal(t, "✗ остановлен  url�[2Jshrinker → urlshrinker_v2   с запуска 25м0с", lines[0])
	assert.Equal(t, "  применено  1 727 851      пачка 19:37:18: 1 (ins 1) в Private�]52;c;aGk=�Urls", lines[4])
	assert.Equal(t, "  DDL        11:40:02       createIndexes urlshrinker.�31mPublicUrls (slug_1)", lines[5])
	assert.Equal(t, "ошибка: write failed: �]8;;http://x�\\key", lines[10])
}

func TestPlainText(t *testing.T) {
	for _, tc := range []struct {
		name, s, want string
	}{
		{"обычный", "urlshrinker.PublicUrls", "urlshrinker.PublicUrls"},
		{"переводы строк", "a\r\nb\nc\rd\te", "a b c d e"},
		{"ESC", "a\x1b[31mb", "a�[31mb"},
		{"BEL и DEL", "a\x07b\x7fc", "a�b�c"},
		{"C1", "a\u009b31mb", "a�31mb"},
		{"битый UTF-8", "a\x9bb", "a�b"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := plainText(tc.s)

			assert.Equal(t, tc.want, got)
		})
	}
}

// assertWithin проверяет, что ни одна строка lines не шире окна width
func assertWithin(t *testing.T, width int, lines []string) {
	t.Helper()
	for _, l := range lines {
		assert.LessOrEqual(t, lipgloss.Width(l), width, "строка %q", l)
	}
}

func TestRenderPanelNarrow(t *testing.T) {
	for _, tc := range []struct {
		width int
		want  []string
	}{
		{80, []string{
			"● работает  urlshrinker → urlshrinker_v2   с запуска 25м0с",
			"── репликация " + strings.Repeat("─", 66),
			"  лаг        0с             применено до 19:37:18",
			"  скорость   19/с           ▏▁▂▂▂▃▃▃▄▄▄▅▅▆▆▆▇▇▇██▕ 2–59/с",
			"  применено  1 727 851      пачка 19:37:18: 1 (ins 1) в PrivateUrls",
			"  DDL        11:40:02       createIndexes urlshrinker.PublicUrls (slug_1)",
			"── трафик " + strings.Repeat("─", 70),
			"  источник   ↓ 4.1 KiB/с    ▏                   █▕ всего ↓ 881 MiB ↑ 2.7 MiB",
			"  цель       ↑ 3.8 KiB/с    ▏                   █▕ всего ↑ 278 MiB ↓ 36.3 MiB",
			"  машина     процессов 1, активно 1 · на 19:36  всего ↓ 881 MiB ↑ 277.9 MiB",
			"ошибка: write failed: connection reset by peer while applying batch to collecti…",
		}},
		{60, []string{
			"● работает  urlshrinker → urlshrinker_v2   с запуска 25м0с",
			"── репликация " + strings.Repeat("─", 46),
			"  лаг        0с             применено до 19:37:18",
			"  скорость   19/с           ▏▁▂▂▂▃▃▃▄▄▄▅▅▆▆▆▇▇▇██▕ 2–59/с",
			"  применено  1 727 851      пачка 19:37:18: 1 (ins 1) в Pri…",
			"  DDL        11:40:02       createIndexes urlshrinker.Publi…",
			"── трафик " + strings.Repeat("─", 50),
			"  источник   ↓ 4.1 KiB/с    ▏                   █▕ всего ↓ …",
			"  цель       ↑ 3.8 KiB/с    ▏                   █▕ всего ↑ …",
			"  машина     процессов 1, активно 1 · на 19:36  всего ↓ 881…",
			"ошибка: write failed: connection reset by peer while applyi…",
		}},
		{40, []string{
			"● работает  urlshrinker → urlshrinker_v…",
			"── репликация " + strings.Repeat("─", 26),
			"  лаг        0с             применено д…",
			"  скорость   19/с           2–59/с",
			"  применено  1 727 851      пачка 19:37…",
			"  DDL        11:40:02       createIndex…",
			"── трафик " + strings.Repeat("─", 30),
			"  источник   ↓ 4.1 KiB/с    всего ↓ 881…",
			"  цель       ↑ 3.8 KiB/с    всего ↑ 278…",
			"  машина     процессов 1, активно 1 · н…",
			"ошибка: write failed: connection reset …",
		}},
	} {
		t.Run(strconv.Itoa(tc.width), func(t *testing.T) {
			v := workingView()
			v.st.LastErr = "write failed: connection reset by peer while applying batch to collection PrivateUrls"

			lines := panelLines(v, tc.width)

			assert.Equal(t, tc.want, lines)
			assertWithin(t, tc.width, lines)
		})
	}
}

func TestRenderPanelNarrowCatchUp(t *testing.T) {
	for _, tc := range []struct {
		width int
		want  string
	}{
		{80, "  лаг        21ч0м          ████████████░░░░░░░░ 58% · ~7с до 0"},
		{60, "  лаг        21ч0м          ████████████░░░░░░░░ 58% · ~7с …"},
		{40, "  лаг        21ч0м          58% · ~7с д…"},
	} {
		t.Run(strconv.Itoa(tc.width), func(t *testing.T) {
			v := workingView()
			v.st.Lag = 21 * time.Hour
			v.catchUp = catchUpOf(v.at, 50*time.Hour, 21*time.Hour)

			lines := panelLines(v, tc.width)

			assert.Equal(t, tc.want, lines[2])
			assertWithin(t, tc.width, lines)
		})
	}
}

func TestRenderPanelNarrowMachine(t *testing.T) {
	for _, tc := range []struct {
		width int
		want  string
	}{
		{40, "  машина     процессов 12, активно 3 · …"},
		{31, "  машина     процессов 12"},
	} {
		t.Run(strconv.Itoa(tc.width), func(t *testing.T) {
			v := workingView()
			v.st.Machine.Processes, v.st.Machine.Active = 12, 3

			lines := panelLines(v, tc.width)

			assert.Equal(t, tc.want, lines[len(lines)-1], "процессов — всегда, остальное — пока остаток не меньше 8")
		})
	}
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

func TestFmtRate(t *testing.T) {
	for v, want := range map[float64]string{0: "0", 0.4: "0.4", 9.94: "9.9", 10: "10", 8802.5: "8 803", 12000: "12 000"} {
		assert.Equal(t, want, fmtRate(v), "%v", v)
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
			got := sparkline(tc.s)

			assert.Equal(t, tc.want, got)
		})
	}
}

func TestSparklineShift(t *testing.T) {
	before := seriesOf(numbers(60)...)
	after := seriesOf(append(numbers(60), 0)...)

	assert.Equal(t, "▁▂▂▂▃▃▃▄▄▄▅▅▆▆▆▇▇▇██", sparkline(before))
	assert.Equal(t, "▂▂▂▃▃▃▄▄▄▅▅▆▆▆▇▇▇██▁", sparkline(after),
		"полные колонки сдвинулись на одну влево, новая точка — в новой колонке")
}

func TestSparkColumns(t *testing.T) {
	s := seriesOf(numbers(62)...)

	cols := sparkColumns(s)

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
