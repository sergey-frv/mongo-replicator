package main

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"mongo-replicator/cmd/mongo-replicator/replicator"
)

func TestPermanent(t *testing.T) {
	cases := []struct {
		name string
		err  error
		hint string // подстрока подсказки; "" — ошибка временная
	}{
		{"history lost", fmt.Errorf("run: %w", mongo.CommandError{Code: 286}), "выпала из oplog"},
		{"событие больше 16 МБ", fmt.Errorf("change stream: %w", mongo.CommandError{Code: 10334}), "больше 16 МБ"},
		{"нет точки старта", fmt.Errorf("run: %w", replicator.ErrNoStartPoint), "--start-ts"},
		{"другая база", fmt.Errorf("run: %w", replicator.ErrDBMismatch), "<dst-db>._replicator_checkpoint"},
		{"аренда занята", fmt.Errorf("run: %w", replicator.ErrLeaseHeld), "аренда истечёт через ≤60 с"},
		{"аренда потеряна", fmt.Errorf("сохранение чекпойнта: %w", replicator.ErrLeaseLost), "два репликатора на одну базу"},
		{"событие другой базы", fmt.Errorf("run: %w", replicator.ErrForeignNamespace), "сообщите о баге"},
		{"поток инвалидирован", fmt.Errorf("run: %w", replicator.ErrStreamInvalidated), "<dst-db>._replicator_checkpoint"},
		{"rename в служебное имя", fmt.Errorf("run: %w", replicator.ErrReservedCollection), "переименуйте её на источнике"},
		{"база в другом регистре", fmt.Errorf("bulk write: %w", mongo.CommandError{Code: 13297}), "--dst-db"},
		{"drop коллекции", fmt.Errorf("run: %w", &replicator.DropError{NS: "db.c"}), "--apply-drop db.c"},
		{"dropDatabase", fmt.Errorf("run: %w", &replicator.DropError{NS: "db", Database: true}), "<dst-db>._replicator_checkpoint"},
		{"обычная ошибка", errors.New("boom"), ""},
		{"отмена", context.Canceled, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.hint != "", permanent(c.err))
			if c.hint == "" {
				assert.Empty(t, hint(c.err))
			} else {
				assert.Contains(t, hint(c.err), c.hint)
			}
		})
	}
}

func TestIsAuthFailed(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"неверный пароль", fmt.Errorf("ping: %w", mongo.CommandError{Code: 18, Wrapped: errors.New("auth")}), true},
		{"другой код", fmt.Errorf("ping: %w", mongo.CommandError{Code: 13}), false},
		{"обычная ошибка", errors.New("boom"), false},
		{"nil", nil, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, isAuthFailed(c.err))
		})
	}
}
