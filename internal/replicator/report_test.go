package replicator

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/sergey-frv/mongo-replicator/internal/traffic"
)

// Прогресс пишется не чаще раза в 10 с; пока ничего не сохранено, лага нет и ping не нужен
// (src == nil упал бы на ping).
func TestLogProgressBeforeFirstSave(t *testing.T) {
	var buf bytes.Buffer
	prev := log.Logger
	log.Logger = zerolog.New(&buf)
	t.Cleanup(func() { log.Logger = prev })

	r := New(nil, nil, Config{})
	r.applied.Store(50)
	r.logProgress(context.Background())
	assert.Empty(t, buf.String(), "10 с ещё не прошло")

	r.lastLog = time.Now().Add(-20 * time.Second)
	r.logProgress(context.Background())
	var rec map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &rec))
	assert.InDelta(t, 50.0, rec["applied"], 0)
	assert.InDelta(t, 2.5, rec["rate"], 0.1)
	assert.NotContains(t, rec, "applied_at")
	assert.NotContains(t, rec, "lag")
	assert.NotContains(t, rec, "lag_sec")

	buf.Reset()
	r.logProgress(context.Background())
	assert.Empty(t, buf.String(), "сразу после записи — тишина")
}

// Прогресс обновляет скорость; без сохранённой позиции лаг и applied_at неизвестны.
func TestLogProgressStatus(t *testing.T) {
	quietLog(t)
	r := New(nil, nil, Config{DB: "d"})
	r.applied.Store(50)
	r.lastLog = time.Now().Add(-20 * time.Second)

	r.logProgress(context.Background())
	s := r.Status()

	assert.InDelta(t, 2.5, s.Rate, 0.1)
	assert.Equal(t, time.Duration(-1), s.Lag)
	assert.True(t, s.AppliedAt.IsZero())
}

// Позиция сохранена, но ping источника не удался: время позиции есть, лаг неизвестен.
func TestLogProgressSrcUnavailable(t *testing.T) {
	quietLog(t)
	r := New(lazyClient(t), nil, Config{DB: "d"})
	r.appliedTS = bson.Timestamp{T: 1760529693}
	r.lastLog = time.Now().Add(-20 * time.Second)

	r.logProgress(context.Background())
	s := r.Status()

	assert.Equal(t, time.Unix(1760529693, 0).UTC(), s.AppliedAt)
	assert.Equal(t, time.Duration(-1), s.Lag)
}

func TestTrafficFields(t *testing.T) {
	var buf bytes.Buffer
	l := zerolog.New(&buf)
	prev := traffic.Info{SrcIn: 1000, SrcOut: 100, DstIn: 10, DstOut: 500}
	cur := traffic.Info{SrcIn: 21000, SrcOut: 200, DstIn: 30, DstOut: 10500}

	trafficFields(l.Info(), cur, prev, 10*time.Second).Send()

	var got map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &got))
	delete(got, "level")
	assert.Equal(t, map[string]any{
		"src_in": 21000.0, "src_out": 200.0, "dst_in": 30.0, "dst_out": 10500.0,
		"src_in_bps": 2000.0, "src_out_bps": 10.0, "dst_in_bps": 2.0, "dst_out_bps": 1000.0,
	}, got)
}

func TestLogMachine(t *testing.T) {
	dir := t.TempDir()
	r := newState(t, dir, &traffic.Counters{})
	r.startState()
	r.stopState()

	r.logMachine(context.Background())
	m := r.Status().Machine

	require.NotNil(t, m, "первая сводка — сразу")
	assert.Equal(t, 1, m.Processes)
	assert.Equal(t, 1, m.Active)

	r.logMachine(context.Background())

	assert.Same(t, m, r.Status().Machine, "до machineEvery сводка не пересчитывается")
}

func TestLogMachineNoDir(t *testing.T) {
	quietLog(t)
	r := New(nil, nil, Config{DB: "src", DstDB: "app"})

	r.logMachine(context.Background())

	assert.Nil(t, r.Status().Machine, "без каталога сводки нет")
}
