package panel

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"charm.land/lipgloss/v2"
	xansi "github.com/charmbracelet/x/ansi" // ansi — регулярка в тестах пакета

	"github.com/sergey-frv/mongo-replicator/internal/replicator"
	"github.com/sergey-frv/mongo-replicator/internal/traffic"
)

var (
	labelStyle = lipgloss.NewStyle().Faint(true)
	retryStyle = lipgloss.NewStyle().Foreground(lipgloss.Yellow)
	errStyle   = lipgloss.NewStyle().Foreground(lipgloss.Red)
)

// renderPanel — текст панели по снимку st, снятому в at; prev, prevAt — трафик
// предыдущего снимка для скоростей (нулевое prevAt — скорости не показываются).
func renderPanel(st replicator.Status, at time.Time, prev traffic.Info, prevAt time.Time) string {
	label := func(s string) string { return labelStyle.Render(s) }
	var lines []string

	head := fmt.Sprintf("%s → %s %s %s", st.DB, st.DstDB, label("· с запуска"), fmtDuration(at.Sub(st.Started)))
	if st.Lag < 0 {
		head += label(" · лаг неизвестен")
	} else {
		head += label(" · лаг ") + fmtDuration(st.Lag)
		if !st.AppliedAt.IsZero() {
			head += label(" (применено до ") + st.AppliedAt.Local().Format(time.TimeOnly) + label(")")
		}
	}
	lines = append(lines, head)

	ops := label("операции: ") + fmtCount(st.Applied) + label(" всего · ") + fmtFloat(st.Rate) + label("/с")
	if b := st.LastBatch; b != nil {
		ops += label(" · пачка ") + b.At.Local().Format(time.TimeOnly) + label(": ") + fmtBatch(b)
	}
	lines = append(lines, ops)

	if d := st.LastDDL; d != nil {
		ddl := label("DDL: ") + d.At.Local().Format(time.TimeOnly) + " " + d.Op + " " + d.NS
		if d.Details != "" {
			ddl += " (" + d.Details + ")"
		}
		lines = append(lines, ddl)
	}

	elapsed := time.Duration(0)
	if !prevAt.IsZero() {
		elapsed = at.Sub(prevAt)
	}
	speed := func(cur, prev uint64) string {
		if elapsed <= 0 {
			return ""
		}
		return " " + fmtBytes(uint64(float64(cur-prev)/elapsed.Seconds())) + label("/с")
	}
	t := st.Traffic
	lines = append(lines, label("трафик процесса: источник ↓ ")+fmtBytes(t.SrcIn)+speed(t.SrcIn, prev.SrcIn)+
		label(" ↑ ")+fmtBytes(t.SrcOut)+
		label(" · цель ↓ ")+fmtBytes(t.DstIn)+
		label(" ↑ ")+fmtBytes(t.DstOut)+speed(t.DstOut, prev.DstOut))

	if m := st.Machine; m != nil {
		mt := m.Traffic
		lines = append(lines, label(fmt.Sprintf("на машине (%d, активно %d, на %s): источник ↓ ",
			m.Processes, m.Active, m.At.Local().Format("15:04")))+fmtBytes(mt.SrcIn)+
			label(" ↑ ")+fmtBytes(mt.SrcOut)+
			label(" · цель ↓ ")+fmtBytes(mt.DstIn)+
			label(" ↑ ")+fmtBytes(mt.DstOut))
	}

	state := st.State
	switch {
	case st.State == replicator.StateRetry:
		state = retryStyle.Render(fmt.Sprintf("%s %d", st.State, st.Retry))
	case st.LastErr != "":
		state = errStyle.Render(st.State)
	}
	if st.LastErr != "" {
		state += label(": ") + errStyle.Render(st.LastErr)
	}
	lines = append(lines, label("состояние: ")+state)

	return strings.Join(lines, "\n")
}

const (
	sparkCols = 20 // колонок графика: минута при seriesLen точках
	sparkPer  = 3  // точек на колонку
	barCols   = 20 // колонок полосы прогресса
	fitMin    = 8  // меньше этой ширины дополнительная часть строки не выводится
)

// sparkLevels — высоты столбиков графика от нуля до максимума
var sparkLevels = []rune("▁▂▃▄▅▆▇█")

// sparkColumns — средние точек s по колонкам графика, от старых к новым, не больше
// cols. Колонка — sparkPer точек по абсолютному номеру (count), поэтому полные колонки
// при новой точке не пересчитываются, а сдвигаются; правая колонка может быть неполной.
func sparkColumns(s series, cols int) []float64 {
	vals := s.values()
	if len(vals) == 0 {
		return nil
	}
	first := s.count - len(vals) // абсолютный номер vals[0]
	last := (s.count - 1) / sparkPer
	from := max(first/sparkPer, last-cols+1)
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

// sparkline — график s в cols колонках: высота столбика — среднее колонки относительно
// наибольшего среднего; колонки без точек (слева, пока данных меньше минуты) — пробелы.
func sparkline(s series, cols int) string {
	avgs := sparkColumns(s, cols)
	top := 0.0
	for _, v := range avgs {
		top = max(top, v)
	}
	var b strings.Builder
	b.WriteString(strings.Repeat(" ", cols-len(avgs)))
	for _, v := range avgs {
		idx := 0
		if top > 0 {
			idx = int(math.Round(v / top * float64(len(sparkLevels)-1)))
		}
		b.WriteRune(sparkLevels[min(max(idx, 0), len(sparkLevels)-1)])
	}
	return b.String()
}

// progressBar — полоса из cols колонок, заполненная на долю frac (ограничена 0–1)
func progressBar(frac float64, cols int) string {
	n := int(math.Round(min(max(frac, 0), 1) * float64(cols)))
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
	s := fmtCount(int64(o.Ins+o.Upd+o.Rep+o.Del)) + " (" + strings.Join(kinds, " · ") + ") в " + strings.Join(b.Colls, ", ")
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
