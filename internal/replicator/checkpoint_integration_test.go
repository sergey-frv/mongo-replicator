//go:build integration

package replicator

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

// Чекпойнт читается с primary, даже если в URI цели readPreference=secondary.
// На single-node replica set secondary нет: унаследованный read preference
// провалил бы loadCheckpoint на выборе сервера.
func TestIntegrationCheckpointReadsPrimary(t *testing.T) {
	db := prepare(t)
	rn := start(t, Config{StartTS: srcNow(t), BatchSize: 1000, DB: db})

	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	items := srcClient.Database(db).Collection("items")
	_, err := items.InsertOne(ctx, bson.D{{Key: "_id", Value: int32(1)}})
	require.NoError(t, err)
	rn.waitSame(db)
	rn.waitQuiet()
	rn.stop()

	secDst, err := mongo.Connect(options.Client().ApplyURI(dstURI).
		SetAppName("cs-replicator").
		SetWriteConcern(writeconcern.Majority()).
		SetReadPreference(readpref.Secondary()).
		SetServerSelectionTimeout(2 * time.Second))
	require.NoError(t, err)
	t.Cleanup(func() { _ = secDst.Disconnect(context.Background()) })

	// без StartTS: Run обязан прочитать чекпойнт, иначе вернёт ErrNoStartPoint
	rn2 := startOn(t, newIT(secDst, Config{BatchSize: 1000, DB: db}))
	_, err = items.InsertOne(ctx, bson.D{{Key: "_id", Value: int32(2)}})
	require.NoError(t, err)
	rn2.waitSame(db)
}

// Два процесса разных баз пишут в один целевой кластер: у каждой базы цели своя
// служебная коллекция чекпойнта, обе базы сходятся независимо.
func TestIntegrationTwoProcessesOneTarget(t *testing.T) {
	dbA := prepare(t)
	dbB := dbA + "_b"
	cleanDB(t, dbB)

	startTS := srcNow(t)
	rnA := start(t, Config{StartTS: startTS, BatchSize: 1000, DB: dbA})
	rnB := start(t, Config{StartTS: startTS, BatchSize: 1000, DB: dbB})

	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	a := srcClient.Database(dbA).Collection("items")
	b := srcClient.Database(dbB).Collection("items")
	for i := range 3 {
		_, err := a.InsertOne(ctx, bson.D{{Key: "_id", Value: int32(i)}, {Key: "db", Value: "a"}})
		require.NoError(t, err)
		_, err = b.InsertOne(ctx, bson.D{{Key: "_id", Value: int32(i)}, {Key: "db", Value: "b"}})
		require.NoError(t, err)
	}
	_, err := a.UpdateOne(ctx, bson.M{"_id": 0}, bson.M{"$set": bson.M{"x": 1}})
	require.NoError(t, err)
	_, err = b.DeleteOne(ctx, bson.M{"_id": 1})
	require.NoError(t, err)

	rnA.waitSame(dbA)
	rnB.waitSame(dbB)
	rnA.waitQuiet()
	rnB.waitQuiet()

	assert.EqualValues(t, 4, rnA.r.Applied(), "3 insert + update — только своей базы")
	assert.EqualValues(t, 4, rnB.r.Applied(), "3 insert + delete — только своей базы")
	assert.Equal(t, dbPair{src: dbA, dst: dbA}, rnA.ckptPair())
	assert.Equal(t, dbPair{src: dbB, dst: dbB}, rnB.ckptPair())

	for _, db := range []string{dbA, dbB} {
		n, err := ckptOf(db).CountDocuments(ctx, bson.D{})
		require.NoError(t, err)
		assert.EqualValues(t, 1, n, "в служебной коллекции %s один документ чекпойнта", db)
	}
}

// Чекпойнт лежит в служебной коллекции базы цели: отдельной базы на цели нет,
// база источника (при --dst-db) не появляется.
func TestIntegrationCheckpointInDstDB(t *testing.T) {
	db := prepare(t)
	dstDB := db + "_v2"
	cleanDB(t, dstDB)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	// база прежнего формата могла остаться в контейнере от старых прогонов
	require.NoError(t, dstClient.Database("_replicator").Drop(ctx))
	rn := start(t, Config{StartTS: srcNow(t), BatchSize: 1000, DB: db, DstDB: dstDB})

	_, err := srcClient.Database(db).Collection("items").InsertOne(ctx, bson.D{{Key: "_id", Value: int32(1)}})
	require.NoError(t, err)
	rn.waitSameDBs(db, dstDB)
	rn.waitQuiet()

	var doc struct {
		DB    string   `bson:"db"`
		DstDB string   `bson:"dst_db"`
		Owner string   `bson:"owner"`
		Token bson.Raw `bson:"token"`
	}
	require.NoError(t, ckptOf(dstDB).FindOne(ctx, bson.M{"_id": ckptID}).Decode(&doc))
	assert.Equal(t, db, doc.DB)
	assert.Equal(t, dstDB, doc.DstDB)
	assert.Equal(t, rn.r.owner, doc.Owner)
	assert.NotNil(t, doc.Token)
	n, err := ckptOf(dstDB).CountDocuments(ctx, bson.D{})
	require.NoError(t, err)
	assert.EqualValues(t, 1, n, "документов в служебной коллекции")

	names, err := dstClient.Database(dstDB).ListCollectionNames(ctx, bson.D{})
	require.NoError(t, err)
	slices.Sort(names)
	assert.Equal(t, []string{ckptColl, "items"}, names)
	assert.False(t, dbExists(ctx, t, dstClient, "_replicator"), "отдельной базы чекпойнтов на цели нет")
	assert.False(t, dbExists(ctx, t, dstClient, db), "база источника %s появилась на цели", db)
}

// Служебную коллекцию удалили вручную: во время работы — ErrLeaseLost на ближайшем
// продлении (документ не пересоздаётся), при рестарте без --start-ts — ErrNoStartPoint.
func TestIntegrationCheckpointCollDropped(t *testing.T) {
	db := prepare(t)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()

	rn := start(t, Config{StartTS: srcNow(t), BatchSize: 1000, DB: db})
	_, err := srcClient.Database(db).Collection("items").InsertOne(ctx, bson.D{{Key: "_id", Value: int32(1)}})
	require.NoError(t, err)
	rn.waitSame(db)
	rn.waitQuiet()

	require.NoError(t, ckptOf(db).Drop(ctx))
	require.ErrorIs(t, rn.waitExit(), ErrLeaseLost)
	rn.stop()

	_, found := readLease(t, db)
	assert.False(t, found, "ни продление, ни Release документ не пересоздают")

	_, err = runErr(t, Config{BatchSize: 1000, DB: db})

	require.ErrorIs(t, err, ErrNoStartPoint)
	_, found = readLease(t, db)
	assert.False(t, found, "документ без позиции удалён при снятии аренды")
}

// Служебная коллекция на источнике (источник был целью при обратной репликации):
// её insert/update/delete/drop не уходят в поток вперемешку с записями обычной
// коллекции — обычная доезжает, а на цели в служебной только документ чекпойнта.
func TestIntegrationCheckpointCollFiltered(t *testing.T) {
	db := prepare(t)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()

	rn := start(t, Config{StartTS: srcNow(t), BatchSize: 1000, DB: db})
	src := srcClient.Database(db)
	items, svc := src.Collection("items"), src.Collection(ckptColl)

	for i := range 20 {
		_, err := items.InsertOne(ctx, bson.D{{Key: "_id", Value: int32(i)}})
		require.NoError(t, err)
		// _id как у чекпойнта: дошедшее до цели событие затёрло бы позицию
		_, err = svc.UpdateOne(ctx, bson.M{"_id": ckptID},
			bson.M{"$set": bson.M{"token": "foreign", "i": i}}, options.UpdateOne().SetUpsert(true))
		require.NoError(t, err)
		_, err = svc.InsertOne(ctx, bson.D{{Key: "_id", Value: int32(i)}})
		require.NoError(t, err)
		if i%2 == 0 {
			_, err = svc.DeleteOne(ctx, bson.M{"_id": int32(i)})
			require.NoError(t, err)
		}
	}
	require.NoError(t, svc.Drop(ctx)) // drop служебной тоже не доходит: иначе *DropError
	_, err := items.InsertOne(ctx, bson.D{{Key: "_id", Value: "last"}})
	require.NoError(t, err)
	rn.waitSame(db)
	rn.waitQuiet()

	var docs []bson.M
	cur, err := ckptOf(db).Find(ctx, bson.D{})
	require.NoError(t, err)
	require.NoError(t, cur.All(ctx, &docs))
	require.Len(t, docs, 1, "%v", docs)
	assert.Equal(t, ckptID, docs[0]["_id"])
	assert.Equal(t, db, docs[0]["db"])
	assert.NotContains(t, docs[0], "i")
	assert.EqualValues(t, 21, rn.r.Applied())
}

// rename на источнике в служебное имя и из него — перманентная ErrReservedCollection:
// на цель не уходит, чекпойнт остаётся перед событием, рестарт упирается в него снова.
func TestIntegrationRenameReserved(t *testing.T) {
	cases := []struct {
		name, from, to string
		dst            []string // коллекции цели после ошибки
	}{
		{"to", "foo", ckptColl, []string{"foo", "keep"}}, // имя теста → имя базы: ≤ 63 байт
		{"from", ckptColl, "foo", []string{"keep"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			db := prepare(t)
			ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
			defer cancel()

			startTS := srcNow(t)
			src := srcClient.Database(db)
			_, err := src.Collection("keep").InsertOne(ctx, bson.D{{Key: "_id", Value: int32(1)}})
			require.NoError(t, err)
			_, err = src.Collection(c.from).InsertOne(ctx, bson.D{{Key: "_id", Value: int32(1)}})
			require.NoError(t, err)
			renameColl(ctx, t, db, c.from, c.to)

			r, err := runErr(t, Config{StartTS: startTS, BatchSize: 1000, DB: db})

			require.ErrorIs(t, err, ErrReservedCollection)
			d := r.Status().LastDDL
			assert.True(t, d == nil || d.Op != "rename", "rename попал в статус как применённый: %+v", d)
			names, err := collNames(ctx, dstClient, db)
			require.NoError(t, err)
			assert.Equal(t, c.dst, names, "rename на цель не ушёл")
			n, err := ckptOf(db).CountDocuments(ctx, bson.D{})
			require.NoError(t, err)
			assert.EqualValues(t, 1, n, "в служебной коллекции цели только чекпойнт")
			ck, found := readLease(t, db)
			require.True(t, found)
			assert.NotNil(t, ck.Token, "DML перед rename закоммичен")

			r, err = runErr(t, Config{BatchSize: 1000, DB: db})

			require.ErrorIs(t, err, ErrReservedCollection)
			assert.EqualValues(t, 0, r.Applied())
		})
	}
}

// Токен не разобран: clusterTime чекпойнта — время последнего события.
func TestIntegrationSaveTokenUnparsedTime(t *testing.T) {
	db := prepare(t)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()

	r := newIT(dstClient, Config{BatchSize: 1000, DB: db})
	_, err := r.acquireLease(ctx)
	require.NoError(t, err, "saveToken пишет только под арендой")
	r.lastTS = bson.Timestamp{T: 1000, I: 3}
	token, err := bson.Marshal(bson.M{"_data": "zz"})
	require.NoError(t, err)

	require.NoError(t, r.saveToken(ctx, token))

	assert.Equal(t, r.lastTS, r.appliedTS)
	rn := &runner{t: t, r: r}
	assert.Equal(t, r.lastTS, rn.ckptTime())
	assert.Equal(t, dbPair{src: db, dst: db}, rn.ckptPair())

	// рестарт: событий ещё нет, lastTS пуст — позиция берётся из чекпойнта
	// и пустой getMore с неразобранным токеном её не обнуляет
	require.NoError(t, r.Release(ctx))
	r2 := newIT(dstClient, Config{BatchSize: 1000, DB: db})
	t.Cleanup(func() { release(t, r2) })

	// Run берёт позицию из чекпойнта до открытия потока; сам поток с токеном-заглушкой не откроется
	require.ErrorContains(t, r2.Run(ctx), "открытие change stream")

	require.Equal(t, r.lastTS, r2.appliedTS)

	token2, err := bson.Marshal(bson.M{"_data": "yy"})
	require.NoError(t, err)
	require.NoError(t, r2.saveToken(ctx, token2))

	assert.Equal(t, r.lastTS, r2.appliedTS)
	assert.Equal(t, r.lastTS, (&runner{t: t, r: r2}).ckptTime())
}

// Чекпойнта на цели нет: позиции нет, ошибки нет.
func TestIntegrationAppliedPositionNoDoc(t *testing.T) {
	db := prepare(t)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()

	ts, ok, err := AppliedPosition(ctx, dstClient, db, db)

	require.NoError(t, err)
	assert.False(t, ok)
	assert.Equal(t, bson.Timestamp{}, ts)
}

// Документ создан захватом аренды и позиции ещё не несёт.
func TestIntegrationAppliedPositionLeaseOnly(t *testing.T) {
	db := prepare(t)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	r := newIT(dstClient, Config{BatchSize: 1000, DB: db})
	_, err := r.acquireLease(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { release(t, r) })

	ts, ok, err := AppliedPosition(ctx, dstClient, db, db)

	require.NoError(t, err)
	assert.False(t, ok)
	assert.Equal(t, bson.Timestamp{}, ts)
}

// Позиция — время из токена, а неразбираемый токен — clusterTime документа.
func TestIntegrationAppliedPosition(t *testing.T) {
	for _, tc := range []struct {
		name, data string
		want       bson.Timestamp
	}{
		{"токен", eventToken, bson.Timestamp{T: 1791910133, I: 2}},
		{"clusterTime", "zz", bson.Timestamp{T: 1000, I: 3}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := prepare(t)
			ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
			defer cancel()
			putCkpt(t, db, bson.M{"token": bson.M{"_data": tc.data}, "clusterTime": bson.Timestamp{T: 1000, I: 3},
				"db": db, "dst_db": db})

			ts, ok, err := AppliedPosition(ctx, dstClient, db, db)

			require.NoError(t, err)
			assert.True(t, ok)
			assert.Equal(t, tc.want, ts)
		})
	}
}

// Чекпойнт записан для другой базы источника: позиция чужая.
func TestIntegrationAppliedPositionOtherPair(t *testing.T) {
	db := prepare(t)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	putCkpt(t, db, bson.M{"token": bson.M{"_data": eventToken}, "db": db + "_other", "dst_db": db})

	_, ok, err := AppliedPosition(ctx, dstClient, db, db)

	require.ErrorIs(t, err, ErrDBMismatch)
	assert.False(t, ok)
}

// putCkpt заменяет документ чекпойнта базы цели dstDB полями doc.
func putCkpt(t *testing.T, dstDB string, doc bson.M) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	doc["_id"] = ckptID
	_, err := ckptOf(dstDB).ReplaceOne(ctx, bson.M{"_id": ckptID}, doc, options.Replace().SetUpsert(true))
	require.NoError(t, err)
}
