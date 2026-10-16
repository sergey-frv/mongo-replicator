package panel

import (
	"bytes"
	"fmt"
	"os"
	"syscall"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/sergey-frv/mongo-replicator/internal/replicator"
)

// openPTY — псевдотерминал теста: мастер и слейв, оба без управляющего терминала.
// Мастер открыт до конца теста: закрытый мастер даёт EIO, и Run завершился бы ошибкой.
func openPTY(t *testing.T) (master, slave *os.File) {
	t.Helper()
	fd, err := unix.Open("/dev/ptmx", unix.O_RDWR|unix.O_NOCTTY|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Skipf("псевдотерминал недоступен: %v", err)
	}
	master = os.NewFile(uintptr(fd), "/dev/ptmx")
	t.Cleanup(func() { _ = master.Close() })
	require.NoError(t, unix.IoctlSetPointerInt(fd, unix.TIOCSPTLCK, 0), "разблокировать слейв")
	n, err := unix.IoctlGetUint32(fd, unix.TIOCGPTN)
	require.NoError(t, err, "номер слейва")
	slave, err = os.OpenFile(fmt.Sprintf("/dev/pts/%d", n), os.O_RDWR|unix.O_NOCTTY, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = slave.Close() })
	return master, slave
}

// termios — настройки терминала fd
func termios(t *testing.T, fd int) unix.Termios {
	t.Helper()
	tio, err := unix.IoctlGetTermios(fd, unix.TCGETS)
	require.NoError(t, err)
	return *tio
}

// sigAt — сигнал подставной sendSignal и настройки терминала в момент отправки
type sigAt struct {
	sig os.Signal
	tio *unix.Termios
	err error
}

// startOnPTY запускает панель с вводом со слейва и ждёт raw-режима. Сигналы панели
// приходят в sigs вместе с настройками слейва в момент отправки.
func startOnPTY(t *testing.T, slave *os.File) (sigs <-chan sigAt, stop, kill func()) {
	t.Helper()
	fd := int(slave.Fd())
	ch := make(chan sigAt, 4)
	var out bytes.Buffer // вывод не терминал: в буфер, с размером окна
	_, stop, kill = Start(func() replicator.Status {
		return replicator.Status{DB: "src", DstDB: "dst", Lag: -1}
	}, &out, slave, func(s os.Signal) {
		tio, err := unix.IoctlGetTermios(fd, unix.TCGETS)
		ch <- sigAt{sig: s, tio: tio, err: err}
	}, tea.WithWindowSize(120, 10))
	require.Eventually(t, func() bool {
		tio, err := unix.IoctlGetTermios(fd, unix.TCGETS)
		return err == nil && tio.Lflag&(unix.ECHO|unix.ISIG) == 0
	}, 5*time.Second, 10*time.Millisecond, "панель не перевела терминал в raw-режим")
	return ch, stop, kill
}

// waitDone — f возвращается, иначе тест падает, а не висит
func waitDone(t *testing.T, f func(), what string) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		f()
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		require.FailNow(t, what)
	}
}

func TestPTYRawModeRestoredAfterStop(t *testing.T) {
	_, slave := openPTY(t)
	before := termios(t, int(slave.Fd()))
	_, stop, _ := startOnPTY(t, slave)

	waitDone(t, stop, "панель не остановилась")

	assert.Equal(t, before, termios(t, int(slave.Fd())), "настройки терминала после stop")
}

func TestPTYRawModeRestoredAfterKill(t *testing.T) {
	_, slave := openPTY(t)
	before := termios(t, int(slave.Fd()))
	_, _, kill := startOnPTY(t, slave)

	waitDone(t, kill, "панель не завершилась")

	assert.Equal(t, before, termios(t, int(slave.Fd())), "настройки терминала после kill")
}

func TestPTYCtrlCSendsSIGINT(t *testing.T) {
	master, slave := openPTY(t)
	sigs, stop, _ := startOnPTY(t, slave)

	_, err := master.Write([]byte{0x03})
	require.NoError(t, err)

	select {
	case s := <-sigs:
		assert.Equal(t, os.Interrupt, s.sig)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "Ctrl+C не дал SIGINT")
	}
	waitDone(t, stop, "панель не остановилась")
}

func TestPTYCtrlBackslashSendsSIGQUITAfterRestore(t *testing.T) {
	master, slave := openPTY(t)
	before := termios(t, int(slave.Fd()))
	sigs, stop, _ := startOnPTY(t, slave)

	_, err := master.Write([]byte{0x1c})
	require.NoError(t, err)

	select {
	case s := <-sigs:
		require.NoError(t, s.err)
		assert.Equal(t, syscall.SIGQUIT, s.sig)
		assert.Equal(t, before, *s.tio, "к сигналу терминал уже восстановлен")
	case <-time.After(5 * time.Second):
		require.FailNow(t, "Ctrl+\\ не дал SIGQUIT")
	}
	waitDone(t, stop, "stop после выхода панели не вернулся")
}
