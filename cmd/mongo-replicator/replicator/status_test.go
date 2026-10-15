package replicator

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// lazyClient — клиент без сервера: mongo.Connect не подключается сразу, операции
// падают по таймауту выбора сервера.
func lazyClient(t *testing.T) *mongo.Client {
	t.Helper()
	c, err := mongo.Connect(options.Client().ApplyURI("mongodb://127.0.0.1:1").
		SetServerSelectionTimeout(100 * time.Millisecond))
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Disconnect(context.Background()) })
	return c
}

func quietLog(t *testing.T) {
	t.Helper()
	prev := log.Logger
	log.Logger = zerolog.Nop()
	t.Cleanup(func() { log.Logger = prev })
}

func TestStatusInitial(t *testing.T) {
	r := New(nil, nil, Config{DB: "shop", DstDB: "shop_v2"})
	s := r.Status()
	assert.Equal(t, "shop", s.DB)
	assert.Equal(t, "shop_v2", s.DstDB)
	assert.Equal(t, StateStarting, s.State)
	assert.Equal(t, time.Duration(-1), s.Lag, "лаг неизвестен до первого прогресса")
	assert.WithinDuration(t, time.Now(), s.Started, time.Second)
	assert.Nil(t, s.LastBatch)
	assert.Nil(t, s.LastDDL)
	assert.Nil(t, s.Cluster)
}

func TestStatusAppliedAndTraffic(t *testing.T) {
	tr := &Traffic{}
	r := New(nil, nil, Config{DB: "shop", Traffic: tr})
	r.applied.Store(7)
	tr.SrcIn.Store(100)
	tr.DstOut.Store(200)
	s := r.Status()
	assert.EqualValues(t, 7, s.Applied)
	assert.Equal(t, TrafficInfo{SrcIn: 100, DstOut: 200}, s.Traffic)
}

func TestBatchCounts(t *testing.T) {
	var b batch
	m := mongo.NewDeleteOneModel()
	for _, op := range []string{"insert", "insert", "update", "replace", "delete", "delete", "delete"} {
		b.add(namespace{"d", "a"}, op, m)
	}
	assert.Equal(t, OpCounts{Ins: 2, Upd: 1, Rep: 1, Del: 3}, b.counts)
	assert.Equal(t, 7, b.n)
}

func TestBatchInfo(t *testing.T) {
	at := time.Date(2026, 10, 15, 12, 1, 34, 0, time.UTC)
	assert.Nil(t, batchInfo(&batch{}, at), "пустая пачка не описывается")

	m := mongo.NewDeleteOneModel()
	var b batch
	b.add(namespace{"d", "links"}, "insert", m)
	b.add(namespace{"d", "clicks"}, "update", m)
	b.add(namespace{"d", "links"}, "delete", m)
	bi := batchInfo(&b, at)
	require.NotNil(t, bi)
	assert.Equal(t, at, bi.At)
	assert.Equal(t, OpCounts{Ins: 1, Upd: 1, Del: 1}, bi.Ops)
	assert.Equal(t, []string{"links", "clicks"}, bi.Colls)
	assert.Zero(t, bi.More)

	for _, c := range []string{"a", "b", "c"} {
		b.add(namespace{"d", c}, "replace", m)
	}
	bi = batchInfo(&b, at)
	assert.Equal(t, []string{"links", "clicks", "a"}, bi.Colls, "первые maxBatchColls в порядке появления")
	assert.Equal(t, 2, bi.More)
	assert.Equal(t, OpCounts{Ins: 1, Upd: 1, Rep: 3, Del: 1}, bi.Ops)
}

// Неудачная запись пачки не попадает в LastBatch.
func TestFlushErrorKeepsLastBatch(t *testing.T) {
	r := New(nil, lazyClient(t), Config{DB: "d"})
	r.pending.add(namespace{"d", "c"}, "delete", mongo.NewDeleteOneModel().SetFilter(bson.D{{Key: "_id", Value: 1}}))
	require.Error(t, r.flush(context.Background()))
	assert.Nil(t, r.Status().LastBatch)
}

func TestFlushEmptyKeepsLastBatch(t *testing.T) {
	r := New(nil, nil, Config{DB: "d"})
	prev := &BatchInfo{At: time.Now()}
	r.st.LastBatch = prev
	require.NoError(t, r.flush(context.Background()))
	assert.Same(t, prev, r.Status().LastBatch, "пустой flush (простой) последнюю пачку не затирает")
}

func TestApplyDDLSkippedStatus(t *testing.T) {
	quietLog(t)
	r := New(nil, lazyClient(t), Config{DB: "d"})

	ev := event{OperationType: "modify", NS: namespace{"d", "c"}}
	require.NoError(t, r.applyDDL(context.Background(), &ev))
	d := r.Status().LastDDL
	require.NotNil(t, d)
	assert.Equal(t, "modify", d.Op)
	assert.Equal(t, "d.c", d.NS)
	assert.Equal(t, "не поддерживается, пропущено", d.Details)
	assert.WithinDuration(t, time.Now(), d.At, time.Second)

	ev = event{OperationType: "create", NS: namespace{"d", "v"},
		OpDesc: mustMarshal(t, bson.D{{Key: "viewOn", Value: "c"}})}
	require.NoError(t, r.applyDDL(context.Background(), &ev))
	assert.Equal(t, "create", r.Status().LastDDL.Op)
	assert.Equal(t, "view, пропущено", r.Status().LastDDL.Details)
}

// Ошибка DDL не обновляет LastDDL.
func TestApplyDDLErrorKeepsLastDDL(t *testing.T) {
	r := New(nil, lazyClient(t), Config{DB: "d"})
	ev := event{OperationType: "dropDatabase", NS: namespace{DB: "d"}}
	require.Error(t, r.applyDDL(context.Background(), &ev))
	assert.Nil(t, r.Status().LastDDL)
}

func TestDDLDetails(t *testing.T) {
	idx := mustMarshal(t, bson.D{{Key: "indexes", Value: bson.A{
		bson.D{{Key: "name", Value: "slug_1"}, {Key: "key", Value: bson.D{{Key: "slug", Value: 1}}}},
		bson.D{{Key: "name", Value: "ts_-1"}},
	}}})
	cases := []struct {
		name string
		ev   event
		want string
	}{
		{"createIndexes", event{OperationType: "createIndexes", OpDesc: idx}, "slug_1, ts_-1"},
		{"dropIndexes", event{OperationType: "dropIndexes", OpDesc: idx}, "slug_1, ts_-1"},
		{"indexes not array", event{OperationType: "createIndexes",
			OpDesc: mustMarshal(t, bson.D{{Key: "indexes", Value: "x"}})}, ""},
		{"no description", event{OperationType: "createIndexes"}, ""},
		{"rename", event{OperationType: "rename", To: &namespace{"d", "new"}}, "→ d.new"},
		{"rename without to", event{OperationType: "rename"}, ""},
		{"drop", event{OperationType: "drop"}, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, ddlDetails(&c.ev))
		})
	}
}

func TestSetRetry(t *testing.T) {
	r := New(nil, nil, Config{DB: "d"})
	r.SetRetry(errors.New("bulk write: timeout"))
	s := r.Status()
	assert.Equal(t, StateRetry, s.State)
	assert.Equal(t, 1, s.Retry)
	assert.Equal(t, "bulk write: timeout", s.LastErr)

	r.SetRetry(errors.New("ещё раз"))
	s = r.Status()
	assert.Equal(t, 2, s.Retry, "повторы подряд считаются")
	assert.Equal(t, "ещё раз", s.LastErr)

	r.streamOpened()
	s = r.Status()
	assert.Equal(t, StateRunning, s.State)
	assert.Zero(t, s.Retry)
	assert.Empty(t, s.LastErr)
}

func TestStopped(t *testing.T) {
	r := New(nil, nil, Config{DB: "d"})
	r.stopped(context.Background(), errors.New("перманентная"))
	s := r.Status()
	assert.Equal(t, StateStopped, s.State)
	assert.Equal(t, "перманентная", s.LastErr)

	r = New(nil, nil, Config{DB: "d"})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r.stopped(ctx, context.Canceled)
	s = r.Status()
	assert.Equal(t, StateStopped, s.State)
	assert.Empty(t, s.LastErr, "отмена вызывающим — не ошибка")
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

// Копия из Status не меняется при последующих обновлениях.
func TestStatusCopyIsStable(t *testing.T) {
	quietLog(t)
	r := New(nil, lazyClient(t), Config{DB: "d"})
	m := mongo.NewDeleteOneModel()
	var b batch
	b.add(namespace{"d", "a"}, "insert", m)
	r.st.LastBatch = batchInfo(&b, time.Now())
	r.st.Cluster = &ClusterTraffic{Processes: 1}
	require.NoError(t, r.applyDDL(context.Background(), &event{OperationType: "modify", NS: namespace{"d", "a"}}))

	old := r.Status()
	oldBatch, oldDDL, oldCluster := *old.LastBatch, *old.LastDDL, *old.Cluster

	b = batch{}
	b.add(namespace{"d", "b"}, "delete", m)
	bi := batchInfo(&b, time.Now())
	r.update(func(s *Status) { s.LastBatch = bi; s.Cluster = &ClusterTraffic{Processes: 2} })
	require.NoError(t, r.applyDDL(context.Background(), &event{OperationType: "shardCollection", NS: namespace{"d", "b"}}))
	r.SetRetry(errors.New("x"))

	assert.Equal(t, oldBatch, *old.LastBatch)
	assert.Equal(t, oldDDL, *old.LastDDL)
	assert.Equal(t, oldCluster, *old.Cluster)
	assert.Equal(t, StateStarting, old.State)
	assert.Equal(t, []string{"b"}, r.Status().LastBatch.Colls)
	assert.Equal(t, "shardCollection", r.Status().LastDDL.Op)
}

// Status читается из другой горутины, пока репликатор обновляет состояние (-race).
func TestStatusConcurrent(t *testing.T) {
	quietLog(t)
	r := New(nil, nil, Config{DB: "d", Traffic: &Traffic{}})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range 1000 {
			r.SetRetry(errors.New("x"))
			r.streamOpened()
			r.noteDDL(&event{OperationType: "create", NS: namespace{"d", "c"}}, "")
		}
	}()
	go func() {
		defer wg.Done()
		for range 1000 {
			s := r.Status()
			if s.LastDDL != nil {
				_ = s.LastDDL.Op
			}
		}
	}()
	wg.Wait()
}
