//go:build integration

package replicator

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

// dropDatabase на источнике: drop коллекций идут перед ним, поэтому без разрешений
// репликатор останавливается на первом drop; с разрешением всех — на самом dropDatabase,
// который не разрешается никогда.
func TestIntegrationDropDatabaseStops(t *testing.T) {
	db := prepare(t)
	dstDB := db + "_v2"
	cleanDB(t, dstDB)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()

	startTS := srcNow(t)
	for _, coll := range []string{"a", "b"} {
		for i := range 2 {
			_, err := srcClient.Database(db).Collection(coll).InsertOne(ctx, bson.D{{Key: "_id", Value: int32(i)}})
			require.NoError(t, err)
		}
	}
	require.NoError(t, srcClient.Database(db).Drop(ctx))

	// порядок drop коллекций при dropDatabase произвольный
	r, err := runErr(t, Config{StartTS: startTS, BatchSize: 1000, DB: db, DstDB: dstDB})

	var de *DropError
	require.ErrorAs(t, err, &de)
	assert.False(t, de.Database)
	assert.Contains(t, []string{db + ".a", db + ".b"}, de.NS)
	assert.EqualValues(t, 4, r.Applied())
	colls, err := collNames(ctx, dstClient, dstDB)
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, colls, "коллекции на цели целы")

	// разрешение только a: при любом порядке остановка на b, b на цели цела
	_, err = runErr(t, Config{BatchSize: 1000, DB: db, DstDB: dstDB, ApplyDrops: []string{db + ".a"}})

	require.ErrorAs(t, err, &de)
	assert.False(t, de.Database)
	assert.Equal(t, db+".b", de.NS)
	n, err := dstClient.Database(dstDB).Collection("b").CountDocuments(ctx, bson.D{})
	require.NoError(t, err)
	assert.EqualValues(t, 2, n, "неразрешённая коллекция на цели цела")

	// с разрешением обеих: обе удалены на цели, остановка на dropDatabase
	// (a могла быть удалена прошлым запуском — тогда её разрешение не используется)
	r, err = runErr(t, Config{BatchSize: 1000, DB: db, DstDB: dstDB, ApplyDrops: []string{db + ".a", db + ".b"}})

	require.ErrorAs(t, err, &de)
	assert.True(t, de.Database)
	assert.Equal(t, db, de.NS)
	assert.NotContains(t, r.applyDrops, db+".b", "разрешение b использовано")
	colls, err = collNames(ctx, dstClient, dstDB)
	require.NoError(t, err)
	assert.Empty(t, colls)
	assert.False(t, dbExists(ctx, t, dstClient, db), "база источника %s появилась на цели", db)

	// чекпойнт стоит перед dropDatabase: рестарт останавливается на нём же
	_, err = runErr(t, Config{BatchSize: 1000, DB: db, DstDB: dstDB})

	require.ErrorAs(t, err, &de)
	assert.True(t, de.Database)
}

// drop коллекции, которая есть на цели, останавливает репликатор: всё до drop на цели,
// сама коллекция на цели цела, рестарт с чекпойнта останавливается на том же drop.
func TestIntegrationDropStops(t *testing.T) {
	db := prepare(t)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()

	startTS := srcNow(t)
	src := srcClient.Database(db)
	keep, gone := src.Collection("keep"), src.Collection("gone")
	for i := range 2 {
		_, err := keep.InsertOne(ctx, bson.D{{Key: "_id", Value: int32(i)}})
		require.NoError(t, err)
	}
	for i := range 3 {
		_, err := gone.InsertOne(ctx, bson.D{{Key: "_id", Value: int32(i)}})
		require.NoError(t, err)
	}
	require.NoError(t, gone.Drop(ctx))
	_, err := keep.InsertOne(ctx, bson.D{{Key: "_id", Value: int32(10)}}) // после drop: не применяется
	require.NoError(t, err)

	r, err := runErr(t, Config{StartTS: startTS, BatchSize: 1000, DB: db})

	var de *DropError
	require.ErrorAs(t, err, &de)
	assert.Equal(t, db+".gone", de.NS)
	assert.False(t, de.Database)
	assert.EqualValues(t, 5, r.Applied())
	count := func(coll string) int64 {
		n, countErr := dstClient.Database(db).Collection(coll).CountDocuments(ctx, bson.D{})
		require.NoError(t, countErr)
		return n
	}
	assert.EqualValues(t, 3, count("gone"), "коллекция на цели цела")
	assert.EqualValues(t, 2, count("keep"), "запись после drop не применена")

	r, err = runErr(t, Config{BatchSize: 1000, DB: db})

	require.ErrorAs(t, err, &de)
	assert.Equal(t, db+".gone", de.NS)
	assert.EqualValues(t, 0, r.Applied())
}

// drop коллекции, которой нет на цели (view не переносится), — WARN, репликация идёт дальше.
func TestIntegrationDropAbsentOnTarget(t *testing.T) {
	db := prepare(t)
	rn := start(t, Config{StartTS: srcNow(t), BatchSize: 1000, DB: db})

	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	src := srcClient.Database(db)
	c := src.Collection("c")

	_, err := c.InsertOne(ctx, bson.D{{Key: "_id", Value: int32(1)}})
	require.NoError(t, err)
	require.NoError(t, src.CreateView(ctx, "v", "c", mongo.Pipeline{}))
	require.NoError(t, src.Collection("v").Drop(ctx))
	// time-series приходит как create с viewOn: на цели не создаётся, его drop — тоже пропуск
	require.NoError(t, src.CreateCollection(ctx, "ts", options.CreateCollection().
		SetTimeSeriesOptions(options.TimeSeries().SetTimeField("t"))))
	_, err = src.Collection("ts").InsertOne(ctx, bson.D{{Key: "t", Value: time.Now()}})
	require.NoError(t, err)
	require.NoError(t, src.Collection("ts").Drop(ctx))
	_, err = c.InsertOne(ctx, bson.D{{Key: "_id", Value: int32(2)}})
	require.NoError(t, err)

	rn.waitSame(db)
	rn.waitQuiet()

	assert.EqualValues(t, 2, rn.r.Applied())
	d := rn.r.Status().LastDDL
	require.NotNil(t, d)
	assert.Equal(t, "drop", d.Op)
	assert.Equal(t, db+".ts", d.NS)
	assert.Equal(t, "на цели нет, пропущено", d.Details)
}

// rename коллекции, которой на цели нет (создана до старта, заливка её не перенесла):
// NamespaceNotFound пропускается, а DDL виден в статусе.
func TestIntegrationRenameAbsentOnTargetStatus(t *testing.T) {
	db := prepare(t)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	require.NoError(t, srcClient.Database(db).CreateCollection(ctx, "a"))
	lastSrcSec = srcOpTime(t).T // create — до StartTS
	rn := start(t, Config{StartTS: srcNow(t), BatchSize: 1000, DB: db})

	renameColl(ctx, t, db, "a", "b")
	rn.waitUntil(func(c *assert.CollectT) error {
		d := rn.r.Status().LastDDL
		require.NotNil(c, d, "DDL в статусе")
		assert.Equal(c, "rename", d.Op, "последний DDL")
		return nil
	}, "rename не появился в статусе за %s", waitTimeout)

	s := rn.r.Status()
	assert.Equal(t, db+".a", s.LastDDL.NS)
	assert.Equal(t, StateRunning, s.State, "ошибка цели проигнорирована")
}

// view из заливки есть на цели: его drop применяется без остановки — данных во view нет.
func TestIntegrationDropViewFromDump(t *testing.T) {
	db := prepare(t)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	src, dst := srcClient.Database(db), dstClient.Database(db)
	for _, d := range []*mongo.Database{src, dst} {
		require.NoError(t, d.CreateCollection(ctx, "c"))
		require.NoError(t, d.CreateView(ctx, "v", "c", mongo.Pipeline{}))
	}

	rn := start(t, Config{StartTS: srcNow(t), BatchSize: 1000, DB: db})

	require.NoError(t, src.Collection("v").Drop(ctx))
	_, err := src.Collection("c").InsertOne(ctx, bson.D{{Key: "_id", Value: int32(1)}})
	require.NoError(t, err)
	rn.waitSame(db)
	rn.waitQuiet()

	names, err := dst.ListCollectionNames(ctx, bson.M{"name": "v"})
	require.NoError(t, err)
	assert.Empty(t, names, "view на цели должен быть удалён")
}

// $out идёт через временную коллекцию tmp.agg_out.*: удачный — rename поверх цели,
// неудачный — drop временной коллекции. Ни то, ни другое не останавливает репликатор.
func TestIntegrationFailedOut(t *testing.T) {
	db := prepare(t)
	rn := start(t, Config{StartTS: srcNow(t), BatchSize: 1000, DB: db})

	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	src := srcClient.Database(db)
	c, out := src.Collection("c"), src.Collection("out")

	_, err := c.InsertMany(ctx, []any{
		bson.D{{Key: "_id", Value: int32(1)}, {Key: "k", Value: "a"}},
		bson.D{{Key: "_id", Value: int32(2)}, {Key: "k", Value: "b"}},
	})
	require.NoError(t, err)
	_, err = out.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "k", Value: 1}}, Options: options.Index().SetName("out_k_u").SetUnique(true),
	})
	require.NoError(t, err)

	// удачный $out: индексы out переносятся на временную коллекцию
	cur, err := c.Aggregate(ctx, mongo.Pipeline{{{Key: "$out", Value: "out"}}})
	require.NoError(t, err)
	require.NoError(t, cur.Close(ctx))
	rn.waitSame(db)

	// дубликат k: unique-индекс временной коллекции не примет $out
	_, err = c.InsertOne(ctx, bson.D{{Key: "_id", Value: int32(3)}, {Key: "k", Value: "a"}})
	require.NoError(t, err)
	_, err = c.Aggregate(ctx, mongo.Pipeline{{{Key: "$out", Value: "out"}}})
	require.Error(t, err)
	require.True(t, mongo.IsDuplicateKeyError(err), "%v", err)

	_, err = c.InsertOne(ctx, bson.D{{Key: "_id", Value: int32(4)}, {Key: "k", Value: "d"}})
	require.NoError(t, err)

	rn.waitSame(db)
	rn.waitQuiet()

	tmp, err := dstClient.Database(db).ListCollectionNames(ctx, bson.M{"name": bson.M{"$regex": `^tmp\.agg_out\.`}})
	require.NoError(t, err)
	assert.Empty(t, tmp, "временная коллекция $out осталась на цели")
	n, err := dstClient.Database(db).Collection("out").CountDocuments(ctx, bson.D{})
	require.NoError(t, err)
	assert.EqualValues(t, 2, n)
}

// --apply-drop: разрешённый drop применяется в базе цели один раз; повторный drop
// пересозданной коллекции в том же процессе (Run того же репликатора) — снова остановка.
func TestIntegrationApplyDrop(t *testing.T) {
	db := prepare(t)
	dstDB := db + "_v2"
	cleanDB(t, dstDB)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()

	src := srcClient.Database(db)
	c, keep := src.Collection("c"), src.Collection("keep")
	startTS := srcNow(t)
	_, err := c.InsertMany(ctx, []any{bson.D{{Key: "_id", Value: int32(1)}}, bson.D{{Key: "_id", Value: int32(2)}}})
	require.NoError(t, err)
	_, err = keep.InsertOne(ctx, bson.D{{Key: "_id", Value: int32(1)}})
	require.NoError(t, err)
	require.NoError(t, c.Drop(ctx))
	_, err = keep.InsertOne(ctx, bson.D{{Key: "_id", Value: int32(2)}})
	require.NoError(t, err)

	logs := captureLog(t)
	rn := start(t, Config{StartTS: startTS, BatchSize: 1000, DB: db, DstDB: dstDB, ApplyDrops: []string{db + ".c"}})
	rn.waitSameDBs(db, dstDB)
	rn.waitQuiet()

	// пересоздание: create и insert применяются
	_, err = c.InsertOne(ctx, bson.D{{Key: "_id", Value: int32(3)}})
	require.NoError(t, err)
	rn.waitSameDBs(db, dstDB)
	rn.waitQuiet()
	rn.stop()

	assert.Empty(t, rn.r.applyDrops, "разрешение использовано")
	assert.Contains(t, logs.String(), "drop применён по --apply-drop")
	assert.False(t, dbExists(ctx, t, dstClient, db), "база источника %s появилась на цели", db)

	require.NoError(t, c.Drop(ctx))
	runCtx, runCancel := context.WithTimeout(ctx, waitTimeout)
	defer runCancel()
	err = rn.r.Run(runCtx)

	require.NoError(t, runCtx.Err(), "Run должен остановиться на drop до таймаута")
	var de *DropError
	require.ErrorAs(t, err, &de)
	assert.Equal(t, db+".c", de.NS)
	n, err := dstClient.Database(dstDB).Collection("c").CountDocuments(ctx, bson.D{})
	require.NoError(t, err)
	assert.EqualValues(t, 1, n, "пересозданная коллекция на цели цела")
}
