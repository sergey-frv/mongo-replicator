package panel

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sergey-frv/mongo-replicator/internal/replicator"
	"github.com/sergey-frv/mongo-replicator/internal/traffic"
)

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

// cmdMsg — сообщение команды, nil — команды нет
func cmdMsg(cmd tea.Cmd) tea.Msg {
	if cmd == nil {
		return nil
	}
	return cmd()
}

func TestPanelKeys(t *testing.T) {
	for _, tc := range []struct {
		name       string
		key        tea.KeyPressMsg
		jobControl bool
		wantSigs   []os.Signal
		wantMsg    tea.Msg
		wantExit   os.Signal
	}{
		{name: "ctrl+c", key: tea.KeyPressMsg{Code: 'c', Mod: tea.ModCtrl}, wantSigs: []os.Signal{os.Interrupt}},
		{name: "ctrl+c на кириллице", key: tea.KeyPressMsg{Code: 'с', BaseCode: 'c', Mod: tea.ModCtrl},
			wantSigs: []os.Signal{os.Interrupt}},
		{name: "ctrl+\\", key: tea.KeyPressMsg{Code: '\\', Mod: tea.ModCtrl}, wantMsg: tea.QuitMsg{},
			wantExit: syscall.SIGQUIT},
		{name: "ctrl+z с управлением заданиями", key: tea.KeyPressMsg{Code: 'z', Mod: tea.ModCtrl}, jobControl: true,
			wantMsg: tea.SuspendMsg{}},
		{name: "ctrl+z без управления заданиями", key: tea.KeyPressMsg{Code: 'z', Mod: tea.ModCtrl}},
		{name: "буква", key: tea.KeyPressMsg{Code: 'a', Text: "a"}},
		{name: "enter", key: tea.KeyPressMsg{Code: tea.KeyEnter}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var sigs []os.Signal
			m := newPanel(func() replicator.Status { return replicator.Status{} })
			m.sendSignal = func(s os.Signal) { sigs = append(sigs, s) }
			m.jobControl = func() bool { return tc.jobControl }

			_, cmd := m.Update(tc.key)

			assert.Equal(t, tc.wantSigs, sigs, "сигналы при нажатии")
			assert.Equal(t, tc.wantMsg, cmdMsg(cmd), "сообщение команды")
			assert.Equal(t, tc.wantExit, m.exitSig, "сигнал после выхода панели")
		})
	}
}

func TestPanelViewReportsAlternateKeys(t *testing.T) {
	m := newPanel(func() replicator.Status { return replicator.Status{} })

	v := m.View()

	assert.True(t, v.KeyboardEnhancements.ReportAlternateKeys, "BaseCode для Ctrl+С на кириллице")
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
	}, &out, nil, nil, tea.WithWindowSize(120, 10)) // вывод не терминал: без размера панель не рисуется

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
