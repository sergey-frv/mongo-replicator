package replicator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
)

func mustMarshal(t *testing.T, v any) []byte {
	t.Helper()
	b, err := bson.Marshal(v)
	require.NoError(t, err, "bson.Marshal")
	return b
}

// changeEvent собирает DML-событие потока по коллекции shop.orders.
func changeEvent(op string, docKey bson.D, fields ...bson.E) bson.D {
	return append(bson.D{
		{Key: "_id", Value: bson.D{{Key: "_data", Value: "8263"}}},
		{Key: "operationType", Value: op},
		{Key: "ns", Value: bson.D{{Key: "db", Value: "shop"}, {Key: "coll", Value: "orders"}}},
		{Key: "documentKey", Value: docKey},
	}, fields...)
}

// decodeEvent декодирует событие так же, как поток: из BSON в event.
func decodeEvent(t *testing.T, d bson.D) event {
	t.Helper()
	var ev event
	require.NoError(t, bson.Unmarshal(mustMarshal(t, d), &ev), "декодирование события")
	require.NotEmpty(t, ev.ID, "resume token не декодирован")
	return ev
}

func TestToModelInsert(t *testing.T) {
	ev := decodeEvent(t, changeEvent("insert", bson.D{{Key: "_id", Value: int32(7)}},
		bson.E{Key: "clusterTime", Value: bson.Timestamp{T: 100, I: 2}},
		bson.E{Key: "fullDocument", Value: bson.D{{Key: "_id", Value: int32(7)}, {Key: "z", Value: 1.5}, {Key: "a", Value: "x"}}},
	))

	m, ok := toModel(&ev)

	require.True(t, ok, "вставка даёт модель")
	require.IsType(t, &mongo.ReplaceOneModel{}, m, "вставка — replace")
	rm := m.(*mongo.ReplaceOneModel)
	assert.Equal(t, bson.Raw(mustMarshal(t, bson.D{{Key: "_id", Value: int32(7)}})), bson.Raw(mustMarshal(t, rm.Filter)), "фильтр по _id")
	assert.Equal(t, bson.Raw(mustMarshal(t, bson.D{{Key: "_id", Value: int32(7)}, {Key: "z", Value: 1.5}, {Key: "a", Value: "x"}})), rm.Replacement,
		"документ доходит байт в байт (порядок полей и типы)")
	require.NotNil(t, rm.Upsert, "upsert не задан")
	assert.True(t, *rm.Upsert, "upsert не задан")
}

func TestToModelDelete(t *testing.T) {
	cases := []struct {
		name   string
		docKey bson.D
	}{
		{"documentKey из _id", bson.D{{Key: "_id", Value: int32(7)}}},
		{"составной documentKey шардированного источника", bson.D{{Key: "region", Value: "kg"}, {Key: "_id", Value: int32(7)}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ev := decodeEvent(t, changeEvent("delete", c.docKey))

			m, ok := toModel(&ev)

			require.True(t, ok, "удаление даёт модель")
			require.IsType(t, &mongo.DeleteOneModel{}, m, "удаление — delete")
			assert.Equal(t, bson.Raw(mustMarshal(t, bson.D{{Key: "_id", Value: int32(7)}})), bson.Raw(mustMarshal(t, m.(*mongo.DeleteOneModel).Filter)),
				"фильтр строго по _id")
		})
	}
}

// update + updateLookup без документа: к моменту чтения документа на источнике его уже нет.
func TestToModelUpdateWithoutDocument(t *testing.T) {
	cases := []struct {
		name   string
		fields []bson.E
	}{
		{"fullDocument: null", []bson.E{{Key: "fullDocument", Value: nil}}},
		{"нет поля fullDocument", nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ev := decodeEvent(t, changeEvent("update", bson.D{{Key: "_id", Value: int32(7)}}, c.fields...))

			m, ok := toModel(&ev)

			assert.False(t, ok, "update без документа пропускается")
			assert.Nil(t, m, "update без документа пропускается")
		})
	}
}

// Декодированные bson.Raw не должны ссылаться на буфер события:
// драйвер переиспользует его при следующем getMore, а мы держим документы до flush.
func TestRawIsCopied(t *testing.T) {
	buf := mustMarshal(t, bson.D{
		{Key: "_id", Value: bson.D{{Key: "_data", Value: "8263"}}},
		{Key: "operationType", Value: "insert"},
		{Key: "documentKey", Value: bson.D{{Key: "_id", Value: int32(7)}}},
		{Key: "fullDocument", Value: bson.D{{Key: "_id", Value: int32(7)}, {Key: "a", Value: "x"}}},
	})
	var ev event
	require.NoError(t, bson.Unmarshal(buf, &ev))

	copy(buf, bytes.Repeat([]byte{0xFF}, len(buf)))

	assert.Equal(t, bson.Raw(mustMarshal(t, bson.D{{Key: "_data", Value: "8263"}})), ev.ID, "resume token ссылается на буфер события")
	assert.Equal(t, bson.Raw(mustMarshal(t, bson.D{{Key: "_id", Value: int32(7)}})), ev.DocumentKey, "documentKey ссылается на буфер события")
	assert.Equal(t, bson.Raw(mustMarshal(t, bson.D{{Key: "_id", Value: int32(7)}, {Key: "a", Value: "x"}})), ev.FullDocument,
		"fullDocument ссылается на буфер события")
}

func TestBatchOrder(t *testing.T) {
	var b batch
	a, c := namespace{"d", "a"}, namespace{"d", "c"}
	m1 := mongo.NewDeleteOneModel().SetFilter(bson.D{{Key: "_id", Value: 1}})
	m2 := mongo.NewDeleteOneModel().SetFilter(bson.D{{Key: "_id", Value: 2}})
	m3 := mongo.NewDeleteOneModel().SetFilter(bson.D{{Key: "_id", Value: 3}})

	b.add(a, "delete", m1)
	b.add(c, "delete", m2)
	b.add(a, "delete", m3)

	assert.Equal(t, 3, b.n, "операций в пачке")
	assert.Equal(t, []namespace{a, c}, b.order, "коллекции — в порядке первого появления")
	assert.Equal(t, map[namespace][]mongo.WriteModel{a: {m1, m3}, c: {m2}}, b.ops, "порядок операций внутри коллекции")
}

func TestDDLDescriptionParsing(t *testing.T) {
	raw := bson.D{
		{Key: "_id", Value: bson.D{{Key: "_data", Value: "8267"}}},
		{Key: "operationType", Value: "rename"},
		{Key: "ns", Value: bson.D{{Key: "db", Value: "shop"}, {Key: "coll", Value: "a"}}},
		{Key: "to", Value: bson.D{{Key: "db", Value: "shop"}, {Key: "coll", Value: "b"}}},
		{Key: "operationDescription", Value: bson.D{{Key: "indexes", Value: bson.A{
			bson.D{{Key: "v", Value: int32(2)}, {Key: "key", Value: bson.D{{Key: "x", Value: int32(1)}}}, {Key: "name", Value: "x_1"}},
		}}}},
	}

	ev := decodeEvent(t, raw)

	assert.Equal(t, "shop.a", ev.NS.String(), "ns")
	require.NotNil(t, ev.To, "to не разобран")
	assert.Equal(t, "shop.b", ev.To.String(), "to")
	specs, err := ev.OpDesc.Lookup("indexes").Array().Values()
	require.NoError(t, err, "indexes")
	require.Len(t, specs, 1, "indexes")
	assert.Equal(t, "x_1", specs[0].Document().Lookup("name").StringValue(), "имя индекса")
	_, err = ev.OpDesc.LookupErr("viewOn")
	assert.ErrorIs(t, err, bsoncore.ErrElementNotFound, "viewOn должен отсутствовать")
}

func TestIsHistoryLost(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"history lost 286", mongo.CommandError{Code: codeChangeStreamHistoryLost}, true},
		{"change stream fatal 280", mongo.CommandError{Code: codeChangeStreamFatal}, true},
		{"wrapped 286", fmt.Errorf("run: %w", mongo.CommandError{Code: codeChangeStreamHistoryLost}), true},
		{"double wrapped 280", fmt.Errorf("a: %w", fmt.Errorf("b: %w", mongo.CommandError{Code: codeChangeStreamFatal})), true},
		{"namespace not found 26", mongo.CommandError{Code: codeNamespaceNotFound}, false},
		{"wrapped other code", fmt.Errorf("x: %w", mongo.CommandError{Code: 11000}), false},
		{"plain error", errors.New("boom"), false},
		{"nil", nil, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, IsHistoryLost(c.err))
		})
	}
}

func TestIsEventTooLarge(t *testing.T) {
	assert.True(t, IsEventTooLarge(mongo.CommandError{Code: codeBSONObjectTooLarge}))
	assert.True(t, IsEventTooLarge(fmt.Errorf("change stream: %w", mongo.CommandError{Code: codeBSONObjectTooLarge})))
	assert.False(t, IsEventTooLarge(mongo.CommandError{Code: codeChangeStreamHistoryLost}))
	assert.False(t, IsEventTooLarge(errors.New("boom")))
	assert.False(t, IsEventTooLarge(nil))
}

// Поток не тащит updateDescription: с ним событие большого документа превышает 16 МБ.
func TestStreamPipeline(t *testing.T) {
	require.Len(t, streamPipeline, 2)
	assert.Equal(t, bson.D{{Key: "$match", Value: bson.D{{Key: "$or", Value: bson.A{
		bson.D{{Key: "ns.coll", Value: bson.D{{Key: "$ne", Value: "_replicator_checkpoint"}}}},
		bson.D{{Key: "operationType", Value: "rename"}},
	}}}}}, streamPipeline[0], "сначала фильтр служебной коллекции, rename проходит")
	assert.Equal(t, bson.D{{Key: "$unset", Value: "updateDescription"}}, streamPipeline[1])
}

// rename в служебное имя и из него — перманентная ошибка ещё до проверки аренды
// (аренды здесь нет вовсе) и без похода на цель.
func TestApplyDDLRenameReserved(t *testing.T) {
	cases := []struct {
		name     string
		from, to namespace
	}{
		{"в служебное имя", namespace{"d", "foo"}, namespace{"d", ckptColl}},
		{"из служебного имени", namespace{"d", ckptColl}, namespace{"d", "foo"}},
		{"из другой базы в служебное", namespace{"other", "foo"}, namespace{"d", ckptColl}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := New(nil, nil, Config{DB: "d"})
			to := c.to
			ev := event{OperationType: "rename", NS: c.from, To: &to}

			err := r.applyDDL(context.Background(), &ev)

			require.ErrorIs(t, err, ErrReservedCollection)
			assert.NotErrorIs(t, err, errLeaseExpiring)
			assert.Nil(t, r.Status().LastDDL)
		})
	}
}

// Обычный rename доходит до проверки аренды (аренды нет — errLeaseExpiring).
func TestApplyDDLRenameChecksLease(t *testing.T) {
	r := New(nil, nil, Config{DB: "d"})
	ev := event{OperationType: "rename", NS: namespace{"d", "a"}, To: &namespace{"d", "b"}}

	err := r.applyDDL(context.Background(), &ev)

	require.ErrorIs(t, err, errLeaseExpiring, "обычный rename доходит до проверки аренды")
}

func TestIsDBCaseConflict(t *testing.T) {
	bulk := mongo.BulkWriteException{WriteErrors: []mongo.BulkWriteError{{WriteError: mongo.WriteError{Code: codeDatabaseDifferCase}}}}
	assert.True(t, IsDBCaseConflict(fmt.Errorf("bulk write: %w", bulk)))
	assert.True(t, IsDBCaseConflict(mongo.CommandError{Code: codeDatabaseDifferCase}))
	assert.False(t, IsDBCaseConflict(mongo.CommandError{Code: codeNamespaceExists}))
	assert.False(t, IsDBCaseConflict(errors.New("boom")))
}

func TestHasCode(t *testing.T) {
	cases := []struct {
		name  string
		err   error
		codes []int
		want  bool
	}{
		{"single match", mongo.CommandError{Code: 26}, []int{26}, true},
		{"one of many", mongo.CommandError{Code: 27}, []int{26, 27, 48}, true},
		{"wrapped match", fmt.Errorf("drop: %w", mongo.CommandError{Code: 48}), []int{26, 27, 48}, true},
		{"no match", mongo.CommandError{Code: 11000}, []int{26, 27, 48}, false},
		{"no codes", mongo.CommandError{Code: 26}, nil, false},
		{"write exception", mongo.WriteException{WriteErrors: []mongo.WriteError{{Code: 11000}}}, []int{11000}, true},
		{"plain error", errors.New("boom"), []int{26}, false},
		{"nil", nil, []int{26}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, hasCode(c.err, c.codes...))
		})
	}
}

func TestErrNoStartPoint(t *testing.T) {
	require.ErrorIs(t, fmt.Errorf("run: %w", ErrNoStartPoint), ErrNoStartPoint)
	assert.False(t, IsHistoryLost(ErrNoStartPoint))
}

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

func TestIsTempColl(t *testing.T) {
	for name, want := range map[string]bool{
		"tmp.agg_out.0f1e2d3c-4b5a-4697-8a9b-0c1d2e3f4a5b":  true,
		"tmp.agg_out.0f1e2d3c":                              false,
		"tmp.agg_out.archive":                               false,
		"tmp.agg_out.0f1e2d3c-4b5a-4697-8a9b-0c1d2e3f4a5bx": false,
		"tmp.mr.x":                      false,
		"tmpAb12C.renameCollection":     true,
		"tmpAb12C.convertToCapped.c":    true,
		"tmpAb12C.convertToCapped.a\nb": true, // имя коллекции может содержать \n
		"tmp.backup":                    false,
		"tmp":                           false,
		"tmpAb12C.other":                false,
		"tmpAb12.renameCollection":      false,
		"tmpAb12C.renameCollectionX":    false,
		"tmpAb12C.convertToCapped":      false,
		"tmpAb12C.convertToCapped.":     false,
		"xtmpAb12C.renameCollection":    false,
		"tmp.agg_out":                   false,
		"orders":                        false,
	} {
		assert.Equal(t, want, isTempColl(name), name)
	}
}

func TestDropError(t *testing.T) {
	err := fmt.Errorf("run: %w", &DropError{NS: "db.c"})
	var de *DropError
	require.ErrorAs(t, err, &de)
	assert.False(t, de.Database)
	assert.Contains(t, err.Error(), "коллекция db.c")
	assert.Contains(t, (&DropError{NS: "db", Database: true}).Error(), "база db")
}

func TestNSName(t *testing.T) {
	ns := namespace{DB: "a", Coll: "c"}
	assert.Equal(t, "a.c", New(nil, nil, Config{DB: "a"}).nsName(ns))
	assert.Equal(t, "a.c (на цели b.c)", New(nil, nil, Config{DB: "a", DstDB: "b"}).nsName(ns))
}

func TestCheckNSAllowed(t *testing.T) {
	cfg := Config{DB: "a", DstDB: "b"}
	own := namespace{DB: "a", Coll: "c"}
	for _, c := range []struct {
		name string
		ev   event
	}{
		{"своя база", event{OperationType: "insert", NS: own}},
		{"rename внутри базы", event{OperationType: "rename", NS: own, To: &namespace{DB: "a", Coll: "d"}}},
		{"invalidate без ns", event{OperationType: "invalidate"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			err := checkNS(cfg, &c.ev)

			assert.NoError(t, err)
		})
	}
}

func TestCheckNSForeign(t *testing.T) {
	cfg := Config{DB: "a", DstDB: "b"}
	own, foreign := namespace{DB: "a", Coll: "c"}, namespace{DB: "x", Coll: "c"}
	for _, c := range []struct {
		name string
		ev   event
		want string // подстрока ошибки
	}{
		{"чужая ns", event{OperationType: "insert", NS: foreign}, "insert x.c"},
		{"чужая to", event{OperationType: "rename", NS: own, To: &foreign}, "rename a.c → x.c"},
		{"ns базы цели", event{OperationType: "insert", NS: namespace{DB: "b", Coll: "c"}}, "insert b.c"},
	} {
		t.Run(c.name, func(t *testing.T) {
			err := checkNS(cfg, &c.ev)

			require.ErrorIs(t, err, ErrForeignNamespace)
			assert.Contains(t, err.Error(), c.want)
		})
	}
}

func TestCheckDBsSame(t *testing.T) {
	want := dbPair{src: "urlshrinker", dst: "urlshrinker_v2"}

	err := checkDBs(want, want)

	assert.NoError(t, err)
}

func TestCheckDBs(t *testing.T) {
	want := dbPair{src: "urlshrinker", dst: "urlshrinker_v2"}
	for _, c := range []struct {
		name      string
		saved     dbPair
		wantSaved string
	}{
		{"другая база источника", dbPair{src: "a", dst: "urlshrinker_v2"}, `в чекпойнте "a" → "urlshrinker_v2"`},
		{"другая база цели", dbPair{src: "urlshrinker", dst: "urlshrinker"}, `в чекпойнте "urlshrinker" → "urlshrinker"`},
	} {
		t.Run(c.name, func(t *testing.T) {
			err := checkDBs(c.saved, want)

			require.ErrorIs(t, err, ErrDBMismatch)
			assert.Contains(t, err.Error(), c.wantSaved)
			assert.Contains(t, err.Error(), `задано "urlshrinker" → "urlshrinker_v2"`)
		})
	}
}

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
