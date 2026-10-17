package main

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"

	"github.com/sergey-frv/mongo-replicator/internal/replicator"
	"github.com/sergey-frv/mongo-replicator/internal/verify"
)

// withDefects — итог сверки с n дефектами документов: счётчики Report
// встроены неэкспортированной структурой, литералом их не задать.
func withDefects(n int64) verify.Report {
	var r verify.Report
	r.Defects = n
	return r
}

func TestVerifyCode(t *testing.T) {
	cases := []struct {
		name string
		rep  verify.Report
		err  error
		want int
	}{
		{"без дефектов", verify.Report{}, nil, 0},
		{"дефекты документов", withDefects(1), nil, 1},
		{"расхождение схемы", verify.Report{Schema: []verify.SchemaDiff{{NS: "db.c", Reason: "нет на цели"}}}, nil, 1},
		{"известное ограничение схемы", verify.Report{Schema: []verify.SchemaDiff{{NS: "db.c", Reason: "опции различаются: capped", Known: true}}}, nil, 0},
		{"нет --since и позиции", verify.Report{}, verify.ErrNoSince, 2},
		{"файл позиции занят", verify.Report{}, fmt.Errorf("%w: /state/verify/db.lock", verify.ErrLocked), 2},
		{"файл позиции другой сверки", verify.Report{}, fmt.Errorf("%w: /state/verify/db.json", verify.ErrPositionMismatch), 2},
		{"поток выпал из oplog", verify.Report{}, fmt.Errorf("%w: удалите файл", verify.ErrHistoryLost), 1},
		{"чужая пара баз", verify.Report{}, fmt.Errorf("чекпойнт: %w", replicator.ErrDBMismatch), 1},
		{"ошибка выполнения", withDefects(1), errors.New("boom"), 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			restoreLogger(t)
			log.Logger = zerolog.Nop()

			code := verifyCode(context.Background(), c.rep, c.err)

			assert.Equal(t, c.want, code)
		})
	}
}

// Остановка по сигналу после начала цикла приходит без ошибки: код по итогу.
func TestVerifyCodeStoppedBySignal(t *testing.T) {
	restoreLogger(t)
	log.Logger = zerolog.Nop()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	clean := verifyCode(ctx, verify.Report{}, nil)
	defects := verifyCode(ctx, withDefects(1), nil)

	assert.Equal(t, 0, clean, "без дефектов")
	assert.Equal(t, 1, defects, "с дефектами")
}

// Ошибка после сигнала — остановка до итога (схема на старте, открытие потока):
// пустой Report не значит «дефектов нет».
func TestVerifyCodeSignalBeforeResult(t *testing.T) {
	restoreLogger(t)
	log.Logger = zerolog.Nop()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	code := verifyCode(ctx, verify.Report{}, fmt.Errorf("схема: %w", context.Canceled))

	assert.Equal(t, 1, code)
}

func TestVerifyHint(t *testing.T) {
	cases := []struct {
		name string
		err  error
		hint string // подстрока подсказки
	}{
		{"нет --since и позиции", verify.ErrNoSince, "задайте --since"},
		{"файл позиции занят", fmt.Errorf("%w: /state/verify/db.lock", verify.ErrLocked), "уже сверяет другой процесс verify"},
		{"файл позиции другой сверки", fmt.Errorf("%w: /state/verify/db.json", verify.ErrPositionMismatch), "задайте --since"},
		{"поток выпал из oplog", fmt.Errorf("%w: удалите файл", verify.ErrHistoryLost), "начните с более позднего --since"},
		{"чужая пара баз", fmt.Errorf("чекпойнт: %w", replicator.ErrDBMismatch), "должны совпадать с опциями репликатора"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := verifyHint(c.err)

			assert.Contains(t, h, c.hint, "подсказка оператору")
		})
	}
}

func TestVerifyHintNone(t *testing.T) {
	h := verifyHint(errors.New("boom"))

	assert.Empty(t, h, "у прочих ошибок подсказки нет")
}
