package panel

import (
	"os"

	"golang.org/x/term"
)

// Input выбирает терминал, с которого панель читает клавиши (pickInput): stdin или
// /dev/tty; nil — панель без клавиш. closeIn вызывать после выхода панели. Не
// вызывать из тестов: под go test stdin — /dev/null, и Input откроет настоящий /dev/tty.
func Input(stdin *os.File) (in *os.File, closeIn func()) {
	return pickInput(stdin, openTTY, foreground)
}

// pickInput выбирает терминал, с которого панель читает клавиши: stdin, если он
// терминал, иначе /dev/tty (запуск с < /dev/null в терминале). Терминала нет или
// процесс не на переднем плане (isForeground; запуск с &) — nil, панель без клавиш.
// closeIn закрывает открытый здесь /dev/tty, stdin не трогает; вызывать после выхода панели.
func pickInput(stdin *os.File, openTTY func() (*os.File, error),
	isForeground func(*os.File) bool,
) (in *os.File, closeIn func()) {
	noop := func() {}
	in, closeIn = stdin, noop
	if !term.IsTerminal(int(stdin.Fd())) { //nolint:gosec // дескриптор stdin помещается в int
		tty, err := openTTY()
		if err != nil {
			return nil, noop
		}
		in, closeIn = tty, func() { _ = tty.Close() }
	}
	if !isForeground(in) {
		closeIn()
		return nil, noop
	}
	return in, closeIn
}

// openTTY открывает управляющий терминал процесса. Не tea.OpenTTY: тот возвращает
// один файл и как ввод, и как вывод.
func openTTY() (*os.File, error) {
	return os.OpenFile("/dev/tty", os.O_RDWR, 0)
}

// SignalSelf отправляет сигнал своему процессу — sendSignal для Start: в raw-режиме
// панели терминал не превращает Ctrl+C и Ctrl+\ в сигналы, их шлёт панель.
func SignalSelf(sig os.Signal) {
	if p, err := os.FindProcess(os.Getpid()); err == nil {
		_ = p.Signal(sig)
	}
}
