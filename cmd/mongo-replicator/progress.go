package main

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"mongo-replicator/cmd/mongo-replicator/replicator"
)

// Режимы --progress после разрешения auto.
const (
	progressTTY   = "tty"   // панель статуса, в консоли — только WARN и выше
	progressPlain = "plain" // построчный лог
)

// progressMode разрешает --progress: auto — панель, только если stdout — терминал,
// умеющий управляющие последовательности.
func progressMode(opt string, stdoutTTY bool, termEnv string) string {
	if opt != "auto" {
		return opt
	}
	if stdoutTTY && termEnv != "dumb" {
		return progressTTY
	}
	return progressPlain
}

type (
	tickMsg    time.Time
	logLineMsg string   // строка лога для печати над панелью
	finalMsg   struct{} // репликация закончилась: последний снимок и выход
)

// panel — модель панели статуса. Снимок берётся раз в секунду, скорости трафика —
// по разнице с предыдущим снимком.
type panel struct {
	status func() replicator.Status
	st     replicator.Status
	at     time.Time // когда снят st
	prev   replicator.TrafficInfo
	prevAt time.Time // нулевое — предыдущего снимка нет
}

func newPanel(status func() replicator.Status) *panel {
	return &panel{status: status, st: status(), at: time.Now()}
}

func tick() tea.Cmd {
	return tea.Tick(time.Second, func(t time.Time) tea.Msg { return tickMsg(t) })
}

func (m *panel) Init() tea.Cmd { return tick() }

func (m *panel) snapshot() {
	m.prev, m.prevAt = m.st.Traffic, m.at
	m.st, m.at = m.status(), time.Now()
}

func (m *panel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tickMsg:
		m.snapshot()
		return m, tick()
	case logLineMsg:
		return m, tea.Println(string(msg))
	case finalMsg:
		// репликация закончилась: скорость за доли секунды с последнего тика — шум
		m.snapshot()
		m.prevAt = time.Time{}
		return m, tea.Quit
	}
	return m, nil
}

// View завершается переводом строки: при выходе рендерер стирает экран от последней
// строки кадра, без него пропала бы строка состояния.
func (m *panel) View() tea.View {
	return tea.NewView(renderPanel(m.st, m.at, m.prev, m.prevAt) + "\n")
}

var (
	labelStyle = lipgloss.NewStyle().Faint(true)
	retryStyle = lipgloss.NewStyle().Foreground(lipgloss.Yellow)
	errStyle   = lipgloss.NewStyle().Foreground(lipgloss.Red)
)

// renderPanel — текст панели по снимку st, снятому в at; prev, prevAt — трафик
// предыдущего снимка для скоростей (нулевое prevAt — скорости не показываются).
func renderPanel(st replicator.Status, at time.Time, prev replicator.TrafficInfo, prevAt time.Time) string {
	label := func(s string) string { return labelStyle.Render(s) }
	var lines []string

	head := fmt.Sprintf("%s → %s %s %s", st.DB, st.DstDB, label("· работает"), fmtDuration(at.Sub(st.Started)))
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
		if elapsed <= 0 || cur < prev {
			return ""
		}
		return " " + fmtBytes(uint64(float64(cur-prev)/elapsed.Seconds())) + label("/с")
	}
	t := st.Traffic
	lines = append(lines, label("трафик процесса: источник ↓ ")+fmtBytes(t.SrcIn)+speed(t.SrcIn, prev.SrcIn)+
		label(" ↑ ")+fmtBytes(t.SrcOut)+
		label(" · цель ↓ ")+fmtBytes(t.DstIn)+
		label(" ↑ ")+fmtBytes(t.DstOut)+speed(t.DstOut, prev.DstOut))

	if c := st.Cluster; c != nil {
		ct := c.Traffic
		lines = append(lines, label(fmt.Sprintf("все процессы (%d, активно %d, на %s): источник ↓ ",
			c.Processes, c.Active, c.At.Local().Format("15:04")))+fmtBytes(ct.SrcIn)+
			label(" ↑ ")+fmtBytes(ct.SrcOut)+
			label(" · цель ↓ ")+fmtBytes(ct.DstIn)+
			label(" ↑ ")+fmtBytes(ct.DstOut))
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
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")
	var b strings.Builder
	if neg {
		b.WriteByte('-')
	}
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

// teaWriter — консольный приёмник лога при панели: каждая запись (ConsoleWriter пишет
// событие одним Write) печатается над панелью. Send после выхода программы — no-op,
// поэтому запись не блокируется; после закрытия done строки идут в fallback.
// Program.Println не годится: он пишет в небуферизованный канал без выхода по ctx
// и после завершения программы блокирует навсегда.
type teaWriter struct {
	p        *tea.Program
	done     <-chan struct{} // закрывается, когда Run программы вернулся
	fallback io.Writer
}

func (w teaWriter) Write(b []byte) (int, error) {
	select {
	case <-w.done:
		return w.fallback.Write(b)
	default:
	}
	w.p.Send(logLineMsg(strings.TrimRight(string(b), "\n")))
	return len(b), nil
}

// startPanel запускает панель статуса над терминалом. Возвращает приёмник консольного
// лога, stop — показать финальный снимок и дождаться выхода, и kill — немедленный
// выход с восстановлением терминала (для паники). extra — дополнительные опции программы.
func startPanel(status func() replicator.Status, out io.Writer, extra ...tea.ProgramOption,
) (console io.Writer, stop, kill func()) {
	opts := append([]tea.ProgramOption{tea.WithInput(nil), tea.WithoutSignalHandler(), tea.WithOutput(out)}, extra...)
	p := tea.NewProgram(newPanel(status), opts...)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = p.Run() // ошибку некуда писать: лог уйдёт в fallback
	}()
	// свой done, а не p.Wait: канал Wait создаёт Run, до его старта Wait висит
	stop = func() {
		p.Send(finalMsg{})
		<-done
	}
	kill = func() {
		p.Kill()
		<-done
	}
	return teaWriter{p: p, done: done, fallback: out}, stop, kill
}
