//go:build integration

package verify

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// createColl создаёт в базе db коллекцию или view name командой create с опциями opts.
func createColl(ctx context.Context, t *testing.T, c *mongo.Client, db, name string, opts ...bson.E) {
	t.Helper()
	require.NoError(t, c.Database(db).RunCommand(ctx, append(bson.D{{Key: "create", Value: name}}, opts...)).Err(),
		"create %s.%s", db, name)
}

// createBoth — createColl на источнике и на цели.
func createBoth(ctx context.Context, t *testing.T, db, name string, opts ...bson.E) {
	t.Helper()
	createColl(ctx, t, srcClient, db, name, opts...)
	createColl(ctx, t, dstClient, db, name, opts...)
}

// createIndex создаёт индекс коллекции db.coll.
func createIndex(ctx context.Context, t *testing.T, c *mongo.Client, db, coll string, ix mongo.IndexModel) {
	t.Helper()
	_, err := c.Database(db).Collection(coll).Indexes().CreateOne(ctx, ix)
	require.NoError(t, err, "индекс %s.%s", db, coll)
}

// indexBoth — createIndex на источнике и на цели.
func indexBoth(ctx context.Context, t *testing.T, db, coll string, ix mongo.IndexModel) {
	t.Helper()
	createIndex(ctx, t, srcClient, db, coll, ix)
	createIndex(ctx, t, dstClient, db, coll, ix)
}

// Одинаковая схема разных видов (опции, collation, индексы, view) — без
// расхождений; служебная коллекция чекпойнта на цели и system.views не считаются.
func TestIntegrationSchemaEqual(t *testing.T) {
	db := prepare(t)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	createBoth(ctx, t, db, "items")
	indexBoth(ctx, t, db, "items", mongo.IndexModel{
		Keys:    bson.D{{Key: "a", Value: 1}, {Key: "b", Value: -1}},
		Options: options.Index().SetUnique(true).SetPartialFilterExpression(bson.D{{Key: "a", Value: bson.D{{Key: "$exists", Value: true}}}}),
	})
	indexBoth(ctx, t, db, "items", mongo.IndexModel{
		Keys: bson.D{{Key: "at", Value: 1}}, Options: options.Index().SetExpireAfterSeconds(3600),
	})
	createBoth(ctx, t, db, "log", bson.E{Key: "capped", Value: true}, bson.E{Key: "size", Value: 4096})
	createBoth(ctx, t, db, "ru", bson.E{Key: "collation", Value: bson.D{{Key: "locale", Value: "ru"}, {Key: "strength", Value: 2}}})
	createBoth(ctx, t, db, "active", bson.E{Key: "viewOn", Value: "items"},
		bson.E{Key: "pipeline", Value: bson.A{bson.D{{Key: "$match", Value: bson.D{{Key: "a", Value: 1}}}}}})
	writeCkpt(t, db, srcNow(t))

	diffs, err := checkSchema(ctx, srcClient, dstClient, db, db, nil)

	require.NoError(t, err)
	assert.Empty(t, diffs)
}

func TestIntegrationSchemaMissingIndex(t *testing.T) {
	db := prepare(t)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	createBoth(ctx, t, db, "items")
	createIndex(ctx, t, srcClient, db, "items", mongo.IndexModel{Keys: bson.D{{Key: "a", Value: 1}}})

	diffs, err := checkSchema(ctx, srcClient, dstClient, db, db, nil)

	require.NoError(t, err)
	assert.Equal(t, []SchemaDiff{{NS: db + ".items", Reason: "индекс a_1: нет на цели"}}, diffs)
}

func TestIntegrationSchemaValidator(t *testing.T) {
	db := prepare(t)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	createColl(ctx, t, srcClient, db, "items", bson.E{Key: "validator", Value: bson.D{{Key: "a", Value: bson.D{{Key: "$type", Value: "int"}}}}})
	createColl(ctx, t, dstClient, db, "items", bson.E{Key: "validator", Value: bson.D{{Key: "a", Value: bson.D{{Key: "$type", Value: "string"}}}}})

	diffs, err := checkSchema(ctx, srcClient, dstClient, db, db, nil)

	require.NoError(t, err)
	assert.Equal(t, []SchemaDiff{{NS: db + ".items", Reason: "опции различаются: validator"}}, diffs)
}

func TestIntegrationSchemaExtraOnTarget(t *testing.T) {
	db := prepare(t)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	createBoth(ctx, t, db, "items")
	createColl(ctx, t, dstClient, db, "extra")

	diffs, err := checkSchema(ctx, srcClient, dstClient, db, db, nil)

	require.NoError(t, err)
	assert.Equal(t, []SchemaDiff{{NS: db + ".extra", Reason: "лишняя на цели"}}, diffs)
}

// view: другой pipeline на цели и view, которого на цели нет.
func TestIntegrationSchemaView(t *testing.T) {
	db := prepare(t)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	createBoth(ctx, t, db, "items")
	createColl(ctx, t, srcClient, db, "active", bson.E{Key: "viewOn", Value: "items"},
		bson.E{Key: "pipeline", Value: bson.A{bson.D{{Key: "$match", Value: bson.D{{Key: "a", Value: 1}}}}}})
	createColl(ctx, t, dstClient, db, "active", bson.E{Key: "viewOn", Value: "items"},
		bson.E{Key: "pipeline", Value: bson.A{bson.D{{Key: "$match", Value: bson.D{{Key: "a", Value: 2}}}}}})
	createColl(ctx, t, srcClient, db, "all", bson.E{Key: "viewOn", Value: "items"}, bson.E{Key: "pipeline", Value: bson.A{}})

	diffs, err := checkSchema(ctx, srcClient, dstClient, db, db, nil)

	require.NoError(t, err)
	assert.Equal(t, []SchemaDiff{
		{NS: db + ".active", Reason: "опции различаются: pipeline"},
		{NS: db + ".all", Reason: "нет на цели"},
	}, diffs)
}

// Коллекция создана в потоке: репликатор создал её без опций источника —
// известное ограничение, а не дефект.
func TestIntegrationSchemaCreatedKnown(t *testing.T) {
	db := prepare(t)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	createColl(ctx, t, srcClient, db, "log", bson.E{Key: "capped", Value: true}, bson.E{Key: "size", Value: 4096})
	createColl(ctx, t, dstClient, db, "log")

	diffs, err := checkSchema(ctx, srcClient, dstClient, db, db, map[string]bool{"log": true})

	require.NoError(t, err)
	assert.Equal(t, []SchemaDiff{{NS: db + ".log", Reason: "опции различаются: capped, size", Known: true}}, diffs)
}

// finalRun — finalSchema в горутине.
type finalRun struct {
	done  chan struct{}
	diffs []SchemaDiff
	err   error
}

func startFinalSchema(ctx context.Context, t *testing.T, db string) *finalRun {
	t.Helper()
	v := New(srcClient, dstClient, itConfig(db, bson.Timestamp{}))
	fr := &finalRun{done: make(chan struct{})}
	go func() {
		defer close(fr.done)
		fr.diffs, fr.err = v.finalSchema(ctx)
	}()
	t.Cleanup(func() { <-fr.done })
	return fr
}

// returned — finalSchema вернулся; без t — для условий Eventually и Never.
func (fr *finalRun) returned() bool {
	select {
	case <-fr.done:
		return true
	default:
		return false
	}
}

// wait ждёт возврата finalSchema.
func (fr *finalRun) wait(t *testing.T) ([]SchemaDiff, error) {
	t.Helper()
	select {
	case <-fr.done:
	case <-time.After(waitTimeout):
		require.FailNow(t, "finalSchema не вернулся")
	}
	return fr.diffs, fr.err
}

// Итоговая схема: расхождение из-за лага репликатора (индекс источника ещё не
// применён) — сверка ждёт, пока чекпойнт пройдёт схему источника, и
// расхождения нет.
func TestIntegrationSchemaFinalWaitsCheckpoint(t *testing.T) {
	db := prepare(t)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	ix := mongo.IndexModel{Keys: bson.D{{Key: "a", Value: 1}}}
	createBoth(ctx, t, db, "items")
	createIndex(ctx, t, srcClient, db, "items", ix)
	writeCkpt(t, db, srcNow(t))
	fr := startFinalSchema(ctx, t, db)

	assert.Never(t, fr.returned, 2*time.Second, 50*time.Millisecond, "итоговая схема не ждёт чекпойнт")
	createIndex(ctx, t, dstClient, db, "items", ix) // репликатор применил индекс
	followCkpt(t, db)
	diffs, err := fr.wait(t)

	require.NoError(t, err)
	assert.Empty(t, diffs)
}

// Итоговая схема: чекпойнт прошёл схему источника, а расхождение осталось —
// дефект.
func TestIntegrationSchemaFinalDefect(t *testing.T) {
	db := prepare(t)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	createBoth(ctx, t, db, "items")
	createIndex(ctx, t, srcClient, db, "items", mongo.IndexModel{Keys: bson.D{{Key: "a", Value: 1}}})
	followCkpt(t, db)

	diffs, err := startFinalSchema(ctx, t, db).wait(t)

	require.NoError(t, err)
	assert.Equal(t, []SchemaDiff{{NS: db + ".items", Reason: "индекс a_1: нет на цели"}}, diffs)
}

// Итоговая схема: чекпойнт не дошёл до схемы источника (репликатор
// остановлен) — расхождения первого чтения, без ошибки.
func TestIntegrationSchemaFinalCheckpointBehind(t *testing.T) {
	db := prepare(t)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	createBoth(ctx, t, db, "items")
	createIndex(ctx, t, srcClient, db, "items", mongo.IndexModel{Keys: bson.D{{Key: "a", Value: 1}}})
	writeCkpt(t, db, srcNow(t))
	fctx, fcancel := context.WithTimeout(ctx, 3*time.Second)
	defer fcancel()

	diffs, err := startFinalSchema(fctx, t, db).wait(t)

	require.NoError(t, err)
	assert.Equal(t, []SchemaDiff{{NS: db + ".items", Reason: "индекс a_1: нет на цели"}}, diffs)
}
