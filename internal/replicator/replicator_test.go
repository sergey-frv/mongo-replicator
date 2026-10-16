package replicator

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Пустая база — ошибка вызова пакета: до чекпойнта и потока (клиенты nil) не доходит.
func TestRunNoDB(t *testing.T) {
	err := New(nil, nil, Config{BatchSize: 1000}).Run(context.Background())

	require.ErrorContains(t, err, "не задана база источника")
}

func TestNewDstDB(t *testing.T) {
	assert.Equal(t, "a", New(nil, nil, Config{DB: "a"}).cfg.DstDB, "пусто — как DB")
	assert.Equal(t, "b", New(nil, nil, Config{DB: "a", DstDB: "b"}).cfg.DstDB)
}

func TestNewApplyDrops(t *testing.T) {
	assert.Empty(t, New(nil, nil, Config{DB: "a"}).applyDrops)
	assert.Equal(t, map[string]bool{"a.x": true, "a.y.z": true}, New(nil, nil, Config{DB: "a", ApplyDrops: []string{"a.x", "a.y.z"}}).applyDrops)
}
