package panel

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode"

	"charm.land/lipgloss/v2"
	xansi "github.com/charmbracelet/x/ansi" // ansi — регулярка в тестах пакета

	"github.com/sergey-frv/mongo-replicator/internal/replicator"
	"github.com/sergey-frv/mongo-replicator/internal/statefile"
)

var (
	labelStyle = lipgloss.NewStyle().Faint(true) // подписи, единицы, заголовки, рамки графиков
	valueStyle = lipgloss.NewStyle().Bold(true)
	okStyle    = lipgloss.NewStyle().Foreground(lipgloss.Green)
	warnStyle  = lipgloss.NewStyle().Foreground(lipgloss.Yellow)
	errStyle   = lipgloss.NewStyle().Foreground(lipgloss.Red)

	lagGoodStyle = valueStyle.Foreground(lipgloss.Green)  // лаг < lagSlow
	lagSlowStyle = valueStyle.Foreground(lipgloss.Yellow) // лаг < lagBad
	lagBadStyle  = valueStyle.Foreground(lipgloss.Red)

	rateStyle = lipgloss.NewStyle().Foreground(lipgloss.Cyan)    // график скорости
	srcStyle  = lipgloss.NewStyle().Foreground(lipgloss.Green)   // график источника
	dstStyle  = lipgloss.NewStyle().Foreground(lipgloss.Magenta) // график цели
	barStyle  = lipgloss.NewStyle().Foreground(lipgloss.Yellow)  // полоса догона
)

const (
	indentCols = 2  // отступ строк блока
	labelCols  = 11 // колонка подписей
	valueCols  = 15 // колонка значений: «↓ 1023.5 KiB/с» — 14

	lagSlow = 10 * time.Second
	lagBad  = time.Minute
)

// view — всё, что показывает панель: снимок st, снятый в at, история для графиков
// (операций/с, байт/с с источника и на цель) и состояние догона.
type view struct {
	st                  replicator.Status
	at                  time.Time
	rate, srcIn, dstOut series
	catchUp             catchUp
}

// renderPanel — текст панели v для окна шириной width.
func renderPanel(v view, width int) string {
	st := v.st
	lines := []string{
		headLine(v, width),
		blockLine("репликация", width),
		lagLine(v, width),
		rateLine(v, width),
		appliedLine(st, width),
	}
	if st.LastDDL != nil {
		lines = append(lines, ddlLine(st.LastDDL, width))
	}
	lines = append(lines,
		blockLine("трафик", width),
		trafficLine("источник", "↓", v.srcIn, srcStyle, labelStyle.Render("всего ↓ ")+fmtBytes(st.Traffic.SrcIn)+
			labelStyle.Render(" ↑ ")+fmtBytes(st.Traffic.SrcOut), width),
		trafficLine("цель", "↑", v.dstOut, dstStyle, labelStyle.Render("всего ↑ ")+fmtBytes(st.Traffic.DstOut)+
			labelStyle.Render(" ↓ ")+fmtBytes(st.Traffic.DstIn), width))
	if st.Machine != nil {
		lines = append(lines, machineLine(st.Machine, width))
	}
	if st.LastErr != "" {
		lines = append(lines, errLine(st.LastErr, width))
	}
	return strings.Join(lines, "\n")
}

// headLine — шапка: значок и состояние, пара баз и время с запуска
func headLine(v view, width int) string {
	st := v.st
	var state string
	switch {
	case st.State == replicator.StateRunning && v.catchUp.active:
		state = warnStyle.Render("◐ догоняет")
	case st.State == replicator.StateRunning:
		state = okStyle.Render("● " + st.State)
	case st.State == replicator.StateRetry:
		state = warnStyle.Render(fmt.Sprintf("↻ %s %d", st.State, st.Retry))
	case st.State == replicator.StateStopped && st.LastErr != "":
		state = errStyle.Render("✗ " + st.State)
	case st.State == replicator.StateStopped:
		state = labelStyle.Render("✗ " + st.State)
	default:
		state = labelStyle.Render("◌ " + st.State)
	}
	extra := plainText(st.DB+" → "+st.DstDB) + labelStyle.Render("   с запуска ") + valueStyle.Render(fmtDuration(v.at.Sub(st.Started)))
	return withExtra(state, lipgloss.Width(state)+2, extra, width)
}

// blockLine — заголовок блока с линией до ширины окна
func blockLine(name string, width int) string {
	s := "── " + name + " "
	return labelStyle.Render(s + strings.Repeat("─", max(width-lipgloss.Width(s), 0)))
}

// lagLine — лаг; при догоне (только в работе и с известным лагом) — полоса, процент и
// оценка, иначе — до какого времени источника всё применено. Доля догона — не больше 99%:
// на хвосте лаг ещё секунды, а от стартового лага в часы это уже округлилось бы до 100%;
// конец догона — исчезновение полосы. Процент — вниз, +1e-9 — поправка на погрешность
// float (0.58*100 = 57.99999999999999).
func lagLine(v view, width int) string {
	st := v.st
	if st.Lag < 0 {
		return row("лаг", labelStyle.Render("неизвестен"), "", "", width)
	}
	value := lagStyle(st.Lag).Render(fmtDuration(st.Lag))
	if st.State == replicator.StateRunning && v.catchUp.active {
		frac := min(v.catchUp.frac(), 0.99)
		extra := valueStyle.Render(fmt.Sprintf("%d%%", int(math.Floor(frac*100+1e-9))))
		if e := v.catchUp.estimate(v.at); e != "" {
			extra += labelStyle.Render(" · " + e)
		}
		return row("лаг", value, barStyle.Render(progressBar(frac, barCols)), extra, width)
	}
	extra := ""
	if !st.AppliedAt.IsZero() {
		extra = labelStyle.Render("применено до ") + st.AppliedAt.Local().Format(time.TimeOnly)
	}
	return row("лаг", value, "", extra, width)
}

// lagStyle — цвет лага по порогам lagSlow и lagBad
func lagStyle(lag time.Duration) lipgloss.Style {
	switch {
	case lag < lagSlow:
		return lagGoodStyle
	case lag < lagBad:
		return lagSlowStyle
	default:
		return lagBadStyle
	}
}

// rateLine — скорость: число — Status.Rate (за интервал прогресса, не скачет от пачек),
// график — по разнице Applied между тиками, диапазон — по средним колонок графика.
func rateLine(v view, width int) string {
	value := valueStyle.Render(fmtRate(v.st.Rate)) + labelStyle.Render("/с")
	extra := ""
	if cols := sparkColumns(v.rate); len(cols) > 0 {
		lo, hi := cols[0], cols[0]
		for _, c := range cols {
			lo, hi = min(lo, c), max(hi, c)
		}
		extra = fmtRate(lo) + "–" + fmtRate(hi) + labelStyle.Render("/с")
	}
	return row("скорость", value, framed(sparkline(v.rate), rateStyle), extra, width)
}

// appliedLine — всего применено и последняя пачка
func appliedLine(st replicator.Status, width int) string {
	extra := ""
	if b := st.LastBatch; b != nil {
		extra = labelStyle.Render("пачка ") + b.At.Local().Format(time.TimeOnly) + labelStyle.Render(": ") + fmtBatch(b)
	}
	return row("применено", valueStyle.Render(fmtCount(st.Applied)), "", extra, width)
}

// ddlLine — последний DDL: время в колонке значений, операция и пространство имён — дальше
func ddlLine(d *replicator.DDLInfo, width int) string {
	extra := plainText(d.Op + " " + d.NS)
	if d.Details != "" {
		extra += labelStyle.Render(" (") + plainText(d.Details) + labelStyle.Render(")")
	}
	return row("DDL", valueStyle.Render(d.At.Local().Format(time.TimeOnly)), "", extra, width)
}

// trafficLine — трафик процесса с одной стороны: скорость по последней точке s (arrow —
// направление), график s и всего байт. Точек ещё нет — «—».
func trafficLine(name, arrow string, s series, style lipgloss.Style, extra string, width int) string {
	speed := "—"
	if vals := s.values(); len(vals) > 0 {
		speed = fmtBytes(uint64(vals[len(vals)-1]))
	}
	value := labelStyle.Render(arrow+" ") + valueStyle.Render(speed)
	if speed != "—" {
		value += labelStyle.Render("/с")
	}
	return row(name, value, framed(sparkline(s), style), extra, width)
}

// machineLine — сводка по файлам состояния машины: число процессов остаётся всегда,
// остальное — сколько поместится.
func machineLine(m *statefile.Machine, width int) string {
	s := strings.Repeat(" ", indentCols) + pad(labelStyle.Render("машина"), labelCols) +
		labelStyle.Render("процессов ") + valueStyle.Render(strconv.Itoa(m.Processes))
	extra := labelStyle.Render(", активно ") + valueStyle.Render(strconv.Itoa(m.Active)) +
		labelStyle.Render(" · на "+m.At.Local().Format("15:04")+"  всего ↓ ") + fmtBytes(m.Traffic.SrcIn) +
		labelStyle.Render(" ↑ ") + fmtBytes(m.Traffic.DstOut)
	return withExtra(s, lipgloss.Width(s), extra, width)
}

// errLine — ошибка, из-за которой Run вернулся, усечённая по ширине окна
func errLine(msg string, width int) string {
	return errStyle.Render(xansi.Truncate("ошибка: "+plainText(msg), width, "…"))
}

// framed — график в тусклой рамке ▏…▕
func framed(graph string, style lipgloss.Style) string {
	return labelStyle.Render("▏") + style.Render(graph) + labelStyle.Render("▕")
}

// row — строка блока: подпись и значение в своих колонках, затем график, если помещается
// целиком, и дополнительная часть, усечённая до остатка ширины.
func row(name, value, graph, extra string, width int) string {
	s := strings.Repeat(" ", indentCols) + pad(labelStyle.Render(name), labelCols) + value
	at := indentCols + labelCols + valueCols
	if graph != "" && at+lipgloss.Width(graph) <= width {
		s = pad(s, at) + graph
		at = lipgloss.Width(s) + 1
	}
	return withExtra(s, at, extra, width)
}

// withExtra — s и с колонки at дополнительная часть extra, усечённая до ширины width;
// если она пуста или не помещается (fit), s остаётся без хвостовых пробелов.
func withExtra(s string, at int, extra string, width int) string {
	if extra == "" {
		return s
	}
	e := fit(extra, width-at)
	if e == "" {
		return s
	}
	return pad(s, at) + e
}

// plainText готовит внешний текст (имена баз и коллекций, детали DDL, ошибки) к выводу:
// переводы строк и табуляция — пробелы (многострочный текст сломал бы раскладку), прочие
// управляющие символы и битый UTF-8 — «�». Иначе ESC из имени коллекции или ошибки сервера
// дошёл бы до терминала: xansi.Truncate сохраняет escape-последовательности.
func plainText(s string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r == '\n' || r == '\r' || r == '\t':
			return ' '
		case unicode.IsControl(r):
			return unicode.ReplacementChar
		}
		return r
	}, strings.ReplaceAll(s, "\r\n", "\n"))
}

const (
	sparkCols  = 20 // колонок графика: минута при seriesLen точках
	sparkPer   = 3  // точек на колонку
	sparkScale = 5  // последних колонок (15 с), по которым берётся масштаб графика
	barCols    = 20 // колонок полосы прогресса
	fitMin     = 8  // меньше этой ширины дополнительная часть строки не выводится
)

// sparkLevels — высоты столбиков графика от нуля до максимума
var sparkLevels = []rune("▁▂▃▄▅▆▇█")

// sparkColumns — средние точек s по колонкам графика, от старых к новым, не больше
// sparkCols. Колонка — sparkPer точек по абсолютному номеру (count), поэтому полные колонки
// при новой точке не пересчитываются, а сдвигаются; правая колонка может быть неполной.
func sparkColumns(s series) []float64 {
	vals := s.values()
	if len(vals) == 0 {
		return nil
	}
	first := s.count - len(vals) // абсолютный номер vals[0]
	last := (s.count - 1) / sparkPer
	from := max(first/sparkPer, last-sparkCols+1)
	sums := make([]float64, last-from+1)
	ns := make([]int, len(sums))
	for i, v := range vals {
		c := (first+i)/sparkPer - from
		if c < 0 {
			continue
		}
		sums[c] += v
		ns[c]++
	}
	for i := range sums {
		sums[i] /= float64(ns[i])
	}
	return sums
}

// sparkline — график s в sparkCols колонках: высота столбика — среднее колонки относительно
// наибольшего среднего последних sparkScale колонок (15 с), чтобы после всплеска (догона)
// график не оставался плоским минуту; старые колонки выше масштаба упираются в верхний
// уровень. Если последние колонки нулевые (повтор, остановка), масштаб — по всему окну.
// Колонки без точек (слева, пока данных меньше минуты) — пробелы.
func sparkline(s series) string {
	avgs := sparkColumns(s)
	top := 0.0
	for _, v := range avgs[max(len(avgs)-sparkScale, 0):] {
		top = max(top, v)
	}
	if top == 0 {
		for _, v := range avgs {
			top = max(top, v)
		}
	}
	var b strings.Builder
	b.WriteString(strings.Repeat(" ", sparkCols-len(avgs)))
	for _, v := range avgs {
		idx := 0
		if top > 0 {
			// ограничение до int: огромное отношение иначе дало бы выход за sparkLevels
			idx = int(math.Round(min(v/top, 1) * float64(len(sparkLevels)-1)))
		}
		b.WriteRune(sparkLevels[idx])
	}
	return b.String()
}

// progressBar — полоса из cols колонок, заполненная на долю frac (ограничена 0–1). Число
// клеток — вниз (+1e-9 — поправка на погрешность float): клетка заполняется, только когда
// доля её покрыла.
func progressBar(frac float64, cols int) string {
	n := int(math.Floor(min(max(frac, 0), 1)*float64(cols) + 1e-9))
	return strings.Repeat("█", n) + strings.Repeat("░", cols-n)
}

// pad дополняет s пробелами до ширины n. Ширина — по lipgloss.Width: fmt %-Ns считал бы
// руны escape-последовательностей, а Style.Width переносит длинное на новую строку.
func pad(s string, n int) string {
	return s + strings.Repeat(" ", max(n-lipgloss.Width(s), 0))
}

// fit усекает дополнительную часть строки до ширины n с «…»; при n < fitMin — пусто.
func fit(s string, n int) string {
	if n < fitMin {
		return ""
	}
	return xansi.Truncate(s, n, "…")
}

// fmtBatch — состав пачки: «48 (ins 30 · upd 15 · del 3) в links, clicks и ещё 2».
func fmtBatch(b *replicator.BatchInfo) string {
	o := b.Ops
	var kinds []string
	for _, k := range []struct {
		name string
		n    int
	}{{"ins", o.Ins}, {"upd", o.Upd}, {"rep", o.Rep}, {"del", o.Del}} {
		if k.n > 0 {
			kinds = append(kinds, fmt.Sprintf("%s %d", k.name, k.n))
		}
	}
	s := fmtCount(int64(o.Ins+o.Upd+o.Rep+o.Del)) + " (" + strings.Join(kinds, " · ") + ") в " + plainText(strings.Join(b.Colls, ", "))
	if b.More > 0 {
		s += fmt.Sprintf(" и ещё %d", b.More)
	}
	return s
}

// fmtBytes — байты в двоичных единицах до GiB: 512 B, 1.5 KiB, 40 MiB, 3.1 GiB.
func fmtBytes(n uint64) string {
	if n < 1024 {
		return strconv.FormatUint(n, 10) + " B"
	}
	v := float64(n) / 1024
	unit := "KiB"
	for _, u := range []string{"MiB", "GiB"} {
		if v < 1024 {
			break
		}
		v /= 1024
		unit = u
	}
	return fmtFloat(v) + " " + unit
}

// fmtFloat — число с одним знаком после точки, без «.0».
func fmtFloat(v float64) string {
	return strings.TrimSuffix(strconv.FormatFloat(v, 'f', 1, 64), ".0")
}

// fmtRate — скорость операций: от 10/с целое с группами разрядов (8 803), ниже — один знак (9.9).
func fmtRate(v float64) string {
	if v >= 10 {
		return fmtCount(int64(math.Round(v)))
	}
	return fmtFloat(v)
}

// fmtCount — целое с группами разрядов через пробел: 1 284 551.
func fmtCount(n int64) string {
	s := strconv.FormatInt(n, 10)
	var b strings.Builder
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			b.WriteByte(' ')
		}
		b.WriteRune(c)
	}
	return b.String()
}

// fmtDuration — длительность до двух единиц, с округлением вниз: 45с, 3м5с, 2ч14м, 3д2ч.
func fmtDuration(d time.Duration) string {
	d = max(d, 0)
	switch s := int64(d / time.Second); {
	case s < 60:
		return fmt.Sprintf("%dс", s)
	case s < 3600:
		return fmt.Sprintf("%dм%dс", s/60, s%60)
	case s < 86400:
		return fmt.Sprintf("%dч%dм", s/3600, s%3600/60)
	default:
		return fmt.Sprintf("%dд%dч", s/86400, s%86400/3600)
	}
}
