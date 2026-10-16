package main

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo"
	mongoopts "go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/sergey-frv/mongo-replicator/internal/replicator"
)

// Под панелью INFO в консоль не идёт; stop возвращает обычный консольный лог,
// иначе итог «остановлен по сигналу» отфильтровался бы.
func TestWithPanelRestoresConsoleLog(t *testing.T) {
	for name, finish := range map[string]func(stop, kill func()){
		"stop": func(stop, _ func()) { stop() },
		"kill": func(_, kill func()) { kill() },
	} {
		t.Run(name, func(t *testing.T) {
			restoreLogger(t)
			stdout := captureStdout(t)
			stop, kill := withPanel(options{LogLevel: "info", LogFileLevel: "info"},
				func() replicator.Status { return replicator.Status{Lag: -1} }, nil, nil)

			log.Info().Msg("INFO под панелью")
			finish(stop, kill)
			log.Info().Msg("INFO после панели")
			out, err := os.ReadFile(stdout.Name())

			require.NoError(t, err)
			assert.NotContains(t, string(out), "INFO под панелью")
			assert.Contains(t, string(out), "INFO после панели")
		})
	}
}

// Временная ошибка Run: replicate отмечает повтор в статусе перед паузой и
// выходит без ошибки по отмене контекста.
func TestReplicateSetsRetry(t *testing.T) {
	restoreLogger(t)
	log.Logger = zerolog.Nop()
	dst, err := mongo.Connect(mongoopts.Client().ApplyURI("mongodb://127.0.0.1:1").
		SetServerSelectionTimeout(100 * time.Millisecond))
	require.NoError(t, err)
	t.Cleanup(func() { _ = dst.Disconnect(context.Background()) })
	r := replicator.New(nil, dst, replicator.Config{DB: "d"})
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- replicate(ctx, r) }()
	require.Eventually(t, func() bool { return r.Status().State == replicator.StateRetry },
		5*time.Second, 10*time.Millisecond, "захват аренды без цели — временная ошибка")
	s := r.Status()
	cancel()
	err = <-done

	assert.Equal(t, 1, s.Retry, "повтор отмечен в статусе")
	assert.NotEmpty(t, s.LastErr, "ошибка повтора в статусе")
	require.NoError(t, err, "отмена контекста — не ошибка")
}

// stdin — терминал на переднем плане: панель читает его, /dev/tty не открывается,
// а closeIn stdin не закрывает.
func TestPanelInputTerminalStdin(t *testing.T) {
	stdin := openPTMX(t)
	opened := false
	openTTY := func() (*os.File, error) { opened = true; return nil, errors.New("не нужен") }
	var checked *os.File

	in, closeIn := panelInput(stdin, openTTY, func(f *os.File) bool { checked = f; return true })
	closeIn()
	_, statErr := stdin.Stat()

	assert.Same(t, stdin, in)
	assert.Same(t, stdin, checked, "передний план проверен у stdin")
	assert.False(t, opened, "openTTY вызван при терминале на stdin")
	assert.NoError(t, statErr, "stdin после closeIn")
}

// stdin не терминал (запуск с < /dev/null): панель читает /dev/tty, closeIn его закрывает.
func TestPanelInputOpensTTY(t *testing.T) {
	tty := openPTMX(t)
	var checked *os.File

	in, closeIn := panelInput(openDevNull(t), func() (*os.File, error) { return tty, nil },
		func(f *os.File) bool { checked = f; return true })
	_, statErr := tty.Stat()
	closeIn()

	assert.Same(t, tty, in)
	assert.Same(t, tty, checked, "передний план проверен у /dev/tty, не у stdin")
	assert.NoError(t, statErr, "/dev/tty до closeIn")
	assert.ErrorIs(t, tty.Close(), os.ErrClosed, "/dev/tty после closeIn")
}

// Терминала нет: панель без клавиш, closeIn безвреден.
func TestPanelInputNoTTY(t *testing.T) {
	in, closeIn := panelInput(openDevNull(t), func() (*os.File, error) { return nil, errors.New("нет терминала") },
		func(*os.File) bool { return true })

	assert.Nil(t, in)
	assert.NotPanics(t, closeIn)
}

// Терминал на stdin, но процесс в фоне (запуск с &): панель без клавиш, иначе
// raw-режим остановил бы процесс на SIGTTOU; stdin не закрывается.
func TestPanelInputBackgroundStdin(t *testing.T) {
	stdin := openPTMX(t)

	in, closeIn := panelInput(stdin, func() (*os.File, error) { return nil, errors.New("не нужен") },
		func(*os.File) bool { return false })
	closeIn()
	_, statErr := stdin.Stat()

	assert.Nil(t, in)
	assert.NoError(t, statErr, "stdin после closeIn")
}

// /dev/tty открыт, но процесс в фоне: панель без клавиш, файл закрыт сразу.
func TestPanelInputBackgroundTTY(t *testing.T) {
	tty := openPTMX(t)

	in, _ := panelInput(openDevNull(t), func() (*os.File, error) { return tty, nil },
		func(*os.File) bool { return false })

	assert.Nil(t, in)
	assert.ErrorIs(t, tty.Close(), os.ErrClosed, "/dev/tty не на переднем плане")
}

// signalSelf доставляет сигнал своему процессу (так панель останавливает по Ctrl+C).
func TestSignalSelf(t *testing.T) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGUSR1)
	t.Cleanup(func() { signal.Stop(ch) })

	signalSelf(syscall.SIGUSR1)

	select {
	case s := <-ch:
		assert.Equal(t, syscall.SIGUSR1, s)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "сигнал не пришёл")
	}
}

// openPTMX открывает мастер псевдотерминала: для IsTerminal он терминал, а настоящий
// терминал разработчика тест не трогает.
func openPTMX(t *testing.T) *os.File {
	t.Helper()
	f, err := os.OpenFile("/dev/ptmx", os.O_RDWR|syscall.O_NOCTTY, 0)
	if err != nil {
		t.Skipf("/dev/ptmx недоступен: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

// openDevNull открывает /dev/null — stdin не терминал.
func openDevNull(t *testing.T) *os.File {
	t.Helper()
	f, err := os.Open(os.DevNull)
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })
	return f
}
