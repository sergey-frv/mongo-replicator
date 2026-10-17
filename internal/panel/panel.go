// Package panel — панель статуса репликации в терминале (bubbletea): лаг, операции,
// DDL и трафик; консольный лог выводится над ней.
package panel

import (
	"fmt"
	"io"
	"os"
	"strings"
	"syscall"
	"time"

	tea "charm.land/bubbletea/v2"
	"golang.org/x/term"

	"github.com/sergey-frv/mongo-replicator/internal/replicator"
)

// defaultWidth — ширина окна, пока bubbletea не сообщил размер
const defaultWidth = 80

type (
	tickMsg  time.Time
	finalMsg struct{} // репликация закончилась: последний снимок и выход
)

// panel — модель панели статуса. Снимок берётся раз в секунду (время — от тика), точки
// графиков — разница с предыдущим снимком за секунду; история графиков и догона копится
// здесь, а не в репликаторе.
type panel struct {
	status func() replicator.Status
	st     replicator.Status
	at     time.Time // когда снят st
	prevSt replicator.Status
	prevAt time.Time // нулевое — предыдущего снимка нет
	width  int       // ширина окна из tea.WindowSizeMsg

	rate, srcIn, dstOut series // операций в секунду, байт в секунду источника ↓ и цели ↑
	catchUp             catchUp

	sendSignal func(os.Signal) // сигнал своему процессу (клавиши Ctrl+C, Ctrl+\)
	jobControl func() bool     // есть ли управление заданиями (можно ли Ctrl+Z)
	exitSig    os.Signal       // отправить после выхода панели (Ctrl+\)
}

func newPanel(status func() replicator.Status) *panel {
	return &panel{status: status, st: status(), at: time.Now(), width: defaultWidth, jobControl: jobControl}
}

func tick() tea.Cmd {
	return tea.Tick(time.Second, func(t time.Time) tea.Msg { return tickMsg(t) })
}

func (m *panel) Init() tea.Cmd { return tick() }

func (m *panel) snapshot(at time.Time) {
	m.prevSt, m.prevAt = m.st, m.at
	m.st, m.at = m.status(), at
}

// addPoints добавляет в графики разницу с предыдущим снимком за секунду. Счётчик,
// который уменьшился (новый процесс, сброс), даёт 0.
func (m *panel) addPoints() {
	dt := m.at.Sub(m.prevAt).Seconds()
	if dt <= 0 {
		return
	}
	m.rate.push(float64(max(m.st.Applied-m.prevSt.Applied, 0)) / dt)
	m.srcIn.push(perSec(m.st.Traffic.SrcIn, m.prevSt.Traffic.SrcIn, dt))
	m.dstOut.push(perSec(m.st.Traffic.DstOut, m.prevSt.Traffic.DstOut, dt))
}

// perSec — прирост счётчика за секунду; сравнение до вычитания: uint64 не уходит в минус
func perSec(cur, prev uint64, dt float64) float64 {
	if cur < prev {
		return 0
	}
	return float64(cur-prev) / dt
}

func (m *panel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyPressMsg:
		cmd := m.key(msg)
		return m, cmd
	case tea.WindowSizeMsg:
		m.width = msg.Width
	case tickMsg:
		// первый тик — без точки: предыдущий снимок снят в newPanel по time.Now, а не по
		// тику, и разница за неполную секунду — шум
		first := m.prevAt.IsZero()
		m.snapshot(time.Time(msg))
		if !first {
			m.addPoints()
		}
		m.catchUp.update(m.st.Lag, m.at)
		return m, tick()
	case finalMsg:
		// репликация закончилась: скорость за доли секунды с последнего тика — шум
		m.snapshot(time.Now())
		m.prevAt = time.Time{}
		return m, tea.Quit
	}
	return m, nil
}

// key переводит клавиши в прежние действия терминала. Панель читает клавиатуру в
// raw-режиме: без него терминал сам выводил бы набранное и Enter поверх кадра и сбивал
// перерисовку. В raw-режиме ISIG выключен и клавиши сигналов приходят нажатиями:
//   - Ctrl+C — SIGINT своему процессу: остановкой управляет main, как без панели;
//   - Ctrl+\ — SIGQUIT, но только после выхода панели (exitSig): рантайм печатает дамп
//     и выходит без defer, а в raw-режиме дамп был бы нечитаем и терминал не
//     восстановился бы;
//   - Ctrl+Z — приостановка, только при управлении заданиями: в осиротевшей группе
//     процессов ядро отбрасывает SIGTSTP, и bubbletea навсегда ждал бы SIGCONT.
//
// Сравнение по Keystroke: он берёт BaseCode, и Ctrl+С на русской раскладке в
// kitty-совместимом терминале — тоже ctrl+c (с ReportAlternateKeys во View).
func (m *panel) key(k tea.KeyPressMsg) tea.Cmd {
	switch k.Keystroke() {
	case "ctrl+c":
		m.sendSignal(os.Interrupt)
	case "ctrl+\\":
		m.exitSig = syscall.SIGQUIT
		return tea.Quit
	case "ctrl+z":
		if m.jobControl() {
			return tea.Suspend
		}
	}
	return nil
}

// View завершается переводом строки: при выходе рендерер стирает экран от последней
// строки кадра, без него пропала бы строка состояния.
func (m *panel) View() tea.View {
	v := tea.NewView(renderPanel(view{
		st: m.st, at: m.at, rate: m.rate, srcIn: m.srcIn, dstOut: m.dstOut, catchUp: m.catchUp,
	}, m.width) + "\n")
	v.KeyboardEnhancements.ReportAlternateKeys = true // BaseCode для key
	return v
}

// teaWriter — консольный приёмник лога при панели: каждая запись (ConsoleWriter пишет
// событие одним Write) печатается над панелью. Сообщение печати отдаётся в цикл
// программы прямо через Send: команда tea.Println из Update выполнялась бы в своей
// горутине, строки перемешивались бы и терялись бы при выходе, обогнанные tea.Quit.
// Send после выхода программы — no-op, поэтому запись не блокируется; после закрытия
// done строки идут в fallback. Program.Println не годится: он пишет в небуферизованный
// канал без выхода по ctx и после завершения программы блокирует навсегда.
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
	w.p.Send(tea.Println(strings.TrimRight(string(b), "\n"))())
	return len(b), nil
}

// Start запускает панель статуса над терминалом. Возвращает приёмник консольного
// лога, stop — показать финальный снимок и дождаться выхода, и kill — выход без
// финального снимка с восстановлением терминала (для паники). extra — дополнительные
// опции программы.
//
// in — терминал, с которого панель читает клавиатуру (на время работы — raw-режим,
// клавиши — по panel.key), nil — клавиатура не читается и терминал не трогается.
// *os.File, а не io.Reader: nil-файл в интерфейсе не равен nil, и bubbletea взял бы
// его вводом. sendSignal отправляет сигнал своему процессу, обязателен при in != nil.
func Start(status func() replicator.Status, out io.Writer, in *os.File, sendSignal func(os.Signal),
	extra ...tea.ProgramOption,
) (console io.Writer, stop, kill func()) {
	input := tea.WithInput(nil)
	if in != nil {
		input = tea.WithInput(in)
	}
	opts := append([]tea.ProgramOption{input, tea.WithoutSignalHandler(), tea.WithOutput(out)}, extra...)
	m := newPanel(status)
	m.sendSignal = sendSignal
	p := tea.NewProgram(m, opts...)
	done := make(chan struct{})
	go func() {
		defer close(done)
		var (
			fd    int
			saved *term.State
		)
		if in != nil {
			fd = int(in.Fd()) //nolint:gosec // дескриптор терминала помещается в int
			saved, _ = term.GetState(fd)
		}
		final, err := p.Run()
		// страховка: при ранней ошибке старта bubbletea выходит, не восстановив терминал
		if saved != nil {
			_ = term.Restore(fd, saved)
		}
		if err != nil {
			_, _ = fmt.Fprintf(out, "панель статуса: %v\n", err)
		}
		// терминал восстановлен — теперь сигнал (Ctrl+\) не испортит его
		if pm, ok := final.(*panel); ok && pm.exitSig != nil {
			sendSignal(pm.exitSig)
		}
	}()
	// свой done, а не p.Wait: канал Wait создаёт Run, до его старта Wait висит
	stop = func() {
		p.Send(finalMsg{})
		<-done
	}
	// Quit, а не Kill: Kill восстанавливает терминал из чужой горутины, параллельно
	// с Run (гонка), а до старта Run съедает shutdownOnce — терминал не восстановится
	kill = func() {
		p.Quit()
		<-done
	}
	return teaWriter{p: p, done: done, fallback: out}, stop, kill
}
