//go:build integration

package replicator

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Тесты известных ограничений: тело воспроизводит потерю данных, поэтому они
// пропускаются. Без t.Skip оба должны падать, пока ограничение не устранено.
const limitationSkip = "известное ограничение, см. README#ограничения"

// (a) updateLookup после rename: к моменту чтения update из a коллекции a
// уже нет, fullDocument пустой, событие пропускается, на цели в b остаётся
// старая версия документа.
func TestLimitationUpdateLookupAfterRename(t *testing.T) {
	t.Skip(limitationSkip)
	db := prepare(t)
	startTS := srcNow(t)

	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	a := srcClient.Database(db).Collection("a")

	// всё до запуска репликатора: поток читается уже после rename
	_, err := a.InsertOne(ctx, bson.D{{Key: "_id", Value: "x"}, {Key: "v", Value: int32(1)}})
	require.NoError(t, err)
	_, err = a.UpdateOne(ctx, bson.M{"_id": "x"}, bson.D{{Key: "$set", Value: bson.D{{Key: "v", Value: int32(2)}}}})
	require.NoError(t, err)
	renameColl(ctx, t, db, "a", "b")

	rn := start(t, Config{StartTS: startTS, BatchSize: 1000, DB: db})
	rn.waitQuiet()
	ok, diff := same(db)

	assert.True(t, ok, "на цели в b старая версия x: %s", diff)
}

// (b) rename в окне start-ts → конец дампа: дамп уже содержит b целиком, а
// повтор потока создаёт на цели a только из событий окна и переименовывает её
// в b с dropTarget — документы b, не тронутые в окне, уничтожаются.
func TestLimitationRenameInCatchUpWindow(t *testing.T) {
	t.Skip(limitationSkip)
	db := prepare(t)

	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	src := srcClient.Database(db)
	a := src.Collection("a")

	// данные до окна: в потоке их нет, на цель они попадают только дампом
	for i := range 3 {
		_, err := a.InsertOne(ctx, bson.D{{Key: "_id", Value: int32(i)}, {Key: "gen", Value: "pre"}})
		require.NoError(t, err)
	}
	lastSrcSec = srcOpTime(t).T
	startTS := srcNow(t)

	// окно: DML в a и rename a → b
	_, err := a.InsertOne(ctx, bson.D{{Key: "_id", Value: int32(10)}, {Key: "gen", Value: "window"}})
	require.NoError(t, err)
	renameColl(ctx, t, db, "a", "b")

	// «дамп» после окна: копия коллекций источника на цель
	colls, err := src.ListCollectionNames(ctx, bson.D{})
	require.NoError(t, err)
	for _, coll := range colls {
		cur, err := src.Collection(coll).Find(ctx, bson.D{})
		require.NoError(t, err)
		var docs []bson.Raw
		require.NoError(t, cur.All(ctx, &docs))
		_, err = dstClient.Database(db).Collection(coll).InsertMany(ctx, docs)
		require.NoError(t, err)
	}
	ok, diff := same(db)
	require.True(t, ok, "дамп: %s", diff)

	rn := start(t, Config{StartTS: startTS, BatchSize: 1000, DB: db})
	rn.waitQuiet()
	ok, diff = same(db)

	assert.True(t, ok, "документы 0..2 в b уничтожены повтором rename: %s", diff)
}
