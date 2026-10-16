package main

import (
	"context"
	"os"
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
				func() replicator.Status { return replicator.Status{Lag: -1} }, nil)

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
