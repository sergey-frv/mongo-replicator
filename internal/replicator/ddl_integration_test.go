//go:build integration

package replicator

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Цель отвергла DDL: Run возвращает ошибку, чекпойнт остаётся перед событием.
func TestIntegrationDDLError(t *testing.T) {
	db := prepare(t)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()

	// индекс с тем же именем, но другим ключом — только на цели: createIndexes
	// источника она не примет (IndexKeySpecsConflict), и это не «уже есть»
	_, err := dstClient.Database(db).Collection("c").Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "k", Value: 1}},
		Options: options.Index().SetName("c_k"),
	})
	require.NoError(t, err)

	startTS := srcNow(t)
	c := srcClient.Database(db).Collection("c")
	_, err = c.InsertOne(ctx, bson.D{{Key: "_id", Value: int32(1)}, {Key: "k", Value: int32(1)}})
	require.NoError(t, err)
	_, err = c.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "k", Value: -1}},
		Options: options.Index().SetName("c_k"),
	})
	require.NoError(t, err)

	// DML перед DDL записан и закоммичен, сам DDL — нет
	r, err := runErr(t, Config{StartTS: startTS, BatchSize: 1000, DB: db})

	require.ErrorContains(t, err, "createIndexes "+db+".c")
	assert.EqualValues(t, 1, r.Applied())

	r, err = runErr(t, Config{BatchSize: 1000, DB: db})

	require.ErrorContains(t, err, "createIndexes "+db+".c")
	assert.EqualValues(t, 0, r.Applied())
}

func TestIntegrationDDL(t *testing.T) {
	db := prepare(t)
	rn := start(t, Config{StartTS: srcNow(t), BatchSize: 1000, DB: db})

	ctx, cancel := context.WithTimeout(context.Background(), 2*waitTimeout)
	defer cancel()
	src := srcClient.Database(db)
	a, b := src.Collection("a"), src.Collection("b")

	// create + все виды индексов. TTL — на поле, которого нет в документах:
	// иначе TTL-монитор удалит документы на кластерах в разное время.
	require.NoError(t, src.CreateCollection(ctx, "a"))
	_, err := a.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "k", Value: 1}}, Options: options.Index().SetName("a_k")},
		{Keys: bson.D{{Key: "code", Value: 1}}, Options: options.Index().SetName("a_code_u").SetUnique(true)},
	})
	require.NoError(t, err)
	_, err = a.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "status", Value: 1}},
		Options: options.Index().SetName("a_status_p").
			SetPartialFilterExpression(bson.D{{Key: "status", Value: bson.D{{Key: "$eq", Value: "active"}}}}),
	})
	require.NoError(t, err)
	_, err = a.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "expireAt", Value: 1}},
		Options: options.Index().SetName("a_ttl").SetExpireAfterSeconds(3600),
	})
	require.NoError(t, err)
	_, err = a.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "k", Value: 1}, {Key: "n", Value: -1}},
		Options: options.Index().SetName("a_k_n"),
	})
	require.NoError(t, err)

	// DML между DDL
	for i := range 5 {
		_, err = a.InsertOne(ctx, bson.D{
			{Key: "_id", Value: int32(i)},
			{Key: "code", Value: fmt.Sprintf("c%d", i)},
			{Key: "k", Value: int32(i % 2)},
			{Key: "n", Value: int32(i)},
			{Key: "status", Value: []string{"active", "idle"}[i%2]},
		})
		require.NoError(t, err)
	}
	_, err = a.UpdateOne(ctx, bson.M{"_id": 1}, bson.D{{Key: "$set", Value: bson.D{{Key: "status", Value: "active"}}}})
	require.NoError(t, err)
	_, err = a.DeleteOne(ctx, bson.M{"_id": 4})
	require.NoError(t, err)

	// b — цель rename: коллекция со своими документами и индексом, всё это уйдёт по dropTarget
	_, err = b.InsertMany(ctx, []any{
		bson.D{{Key: "_id", Value: int32(100)}, {Key: "old", Value: true}},
		bson.D{{Key: "_id", Value: int32(101)}, {Key: "old", Value: true}},
	})
	require.NoError(t, err)
	_, err = b.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "old", Value: 1}}, Options: options.Index().SetName("b_old")})
	require.NoError(t, err)

	require.NoError(t, a.Indexes().DropOne(ctx, "a_k"))

	// до rename ждём совпадения: update в a, прочитанный после rename, придёт
	// с пустым fullDocument (известное ограничение updateLookup)
	rn.waitSame(db)

	assert.Equal(t, []string{"_id_", "a_code_u", "a_k_n", "a_status_p", "a_ttl"}, indexNames(ctx, t, dstClient, db, "a"),
		"индексы источника приняты целью 8.0")

	renameColl(ctx, t, db, "a", "b")

	_, err = b.InsertOne(ctx, bson.D{{Key: "_id", Value: int32(10)}, {Key: "code", Value: "c10"}, {Key: "status", Value: "idle"}})
	require.NoError(t, err)
	_, err = b.UpdateOne(ctx, bson.M{"_id": 0}, bson.D{{Key: "$inc", Value: bson.D{{Key: "n", Value: int32(10)}}}})
	require.NoError(t, err)
	_, err = b.DeleteOne(ctx, bson.M{"_id": 2})
	require.NoError(t, err)
	rn.waitSame(db)

	colls, err := collNames(ctx, dstClient, db)
	require.NoError(t, err)
	assert.Equal(t, []string{"b"}, colls)
	assert.Equal(t, []string{"_id_", "a_code_u", "a_k_n", "a_status_p", "a_ttl"}, indexNames(ctx, t, dstClient, db, "b"),
		"индексы b заменены индексами a")
	n, err := dstClient.Database(db).Collection("b").CountDocuments(ctx, bson.D{})
	require.NoError(t, err)
	assert.EqualValues(t, 4, n, "0, 1, 3 из a и 10; документы старой b удалены dropTarget")
	// rename поверх существующей b — одно событие rename с dropTarget, а не drop: остановки нет
	done, err := rn.exited()
	assert.False(t, done, "Run вернулся: %v", err)
}

// Rename между базами в режиме одной базы не предусмотрен; тесты фиксируют, во что
// он фактически превращается на 8.0. Внутрь — копия через временную коллекцию
// (create, createIndexes, insert, rename с dropTarget): репликация идёт дальше.
func TestIntegrationCrossDBRenameIn(t *testing.T) {
	db := prepare(t)
	other := db + "_other"
	cleanDB(t, other)
	rn := start(t, Config{StartTS: srcNow(t), BatchSize: 1000, DB: db})

	ctx, cancel := context.WithTimeout(context.Background(), 2*waitTimeout)
	defer cancel()

	// в свободное имя: other.y с документами и индексом → db.in
	y := srcClient.Database(other).Collection("y")
	_, err := y.InsertMany(ctx, []any{
		bson.D{{Key: "_id", Value: int32(1)}, {Key: "k", Value: "a"}},
		bson.D{{Key: "_id", Value: int32(2)}, {Key: "k", Value: "b"}},
		bson.D{{Key: "_id", Value: int32(3)}, {Key: "k", Value: "c"}},
	})
	require.NoError(t, err)
	_, err = y.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "k", Value: 1}}, Options: options.Index().SetName("y_k")})
	require.NoError(t, err)
	renameNS(ctx, t, other+".y", db+".in")

	// поверх существующей: other.w → db.exist
	_, err = srcClient.Database(db).Collection("exist").InsertMany(ctx, []any{
		bson.D{{Key: "_id", Value: int32(100)}, {Key: "old", Value: true}},
		bson.D{{Key: "_id", Value: int32(101)}, {Key: "old", Value: true}},
	})
	require.NoError(t, err)
	_, err = srcClient.Database(other).Collection("w").InsertOne(ctx, bson.D{{Key: "_id", Value: int32(1)}})
	require.NoError(t, err)
	renameNS(ctx, t, other+".w", db+".exist")

	// финальная запись: когда она на цели, все события выше пройдены
	_, err = srcClient.Database(db).Collection("final").InsertOne(ctx, bson.D{{Key: "_id", Value: "final"}})
	require.NoError(t, err)

	rn.waitSame(db) // in и exist совпадают с источником по документам и индексам
	rn.waitQuiet()

	colls, err := collNames(ctx, dstClient, db)
	require.NoError(t, err)
	assert.Equal(t, []string{"exist", "final", "in"}, colls, "временных коллекций нет")
	assert.Equal(t, []string{"_id_", "y_k"}, indexNames(ctx, t, dstClient, db, "in"))
	n, err := dstClient.Database(db).Collection("in").CountDocuments(ctx, bson.D{})
	require.NoError(t, err)
	assert.EqualValues(t, 3, n, "документов in на цели")
	n, err = dstClient.Database(db).Collection("exist").CountDocuments(ctx, bson.D{})
	require.NoError(t, err)
	assert.EqualValues(t, 1, n, "старые документы exist удалены dropTarget")
	assert.False(t, dbExists(ctx, t, dstClient, other), "база %s попала на цель", other)
}

// Перенос коллекции из базы наружу в потоке базы виден только как drop: остановка.
func TestIntegrationCrossDBRenameOut(t *testing.T) {
	db := prepare(t)
	other := db + "_other"
	cleanDB(t, other)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()

	startTS := srcNow(t)
	_, err := srcClient.Database(db).Collection("out").InsertMany(ctx, []any{
		bson.D{{Key: "_id", Value: int32(1)}},
		bson.D{{Key: "_id", Value: int32(2)}},
	})
	require.NoError(t, err)
	renameNS(ctx, t, db+".out", other+".z")

	r, err := runErr(t, Config{StartTS: startTS, BatchSize: 1000, DB: db})

	var de *DropError
	require.ErrorAs(t, err, &de)
	assert.Equal(t, db+".out", de.NS)
	assert.EqualValues(t, 2, r.Applied())
	n, err := dstClient.Database(db).Collection("out").CountDocuments(ctx, bson.D{})
	require.NoError(t, err)
	assert.EqualValues(t, 2, n, "коллекция на цели цела")
	assert.False(t, dbExists(ctx, t, dstClient, other), "база %s попала на цель", other)
}

func TestIntegrationUnsupportedDDL(t *testing.T) {
	db := prepare(t)
	rn := start(t, Config{StartTS: srcNow(t), BatchSize: 1000, DB: db})

	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	src := srcClient.Database(db)
	c := src.Collection("c")

	_, err := c.InsertOne(ctx, bson.D{{Key: "_id", Value: int32(1)}, {Key: "v", Value: int32(1)}})
	require.NoError(t, err)
	rn.waitSame(db)

	// collMod: validator не влияет на сравнение документов и индексов
	require.NoError(t, src.RunCommand(ctx, bson.D{
		{Key: "collMod", Value: "c"},
		{Key: "validator", Value: bson.D{{Key: "v", Value: bson.D{{Key: "$type", Value: "int"}}}}},
		{Key: "validationLevel", Value: "moderate"},
	}).Err())

	_, err = c.InsertOne(ctx, bson.D{{Key: "_id", Value: int32(2)}, {Key: "v", Value: int32(2)}})
	require.NoError(t, err)
	_, err = c.UpdateOne(ctx, bson.M{"_id": 1}, bson.D{{Key: "$inc", Value: bson.D{{Key: "v", Value: int32(10)}}}})
	require.NoError(t, err)

	// view не создаётся, capped создаётся обычной коллекцией
	require.NoError(t, src.CreateView(ctx, "v", "c", mongo.Pipeline{}))
	require.NoError(t, src.CreateCollection(ctx, "capped", options.CreateCollection().SetCapped(true).SetSizeInBytes(1<<20)))

	rn.waitSame(db)
	rn.waitQuiet()

	assert.EqualValues(t, 3, rn.r.Applied())
	// событие modify пропущено: на цели валидатора нет
	specs, err := dstClient.Database(db).ListCollectionSpecifications(ctx, bson.M{"name": "c"})
	require.NoError(t, err)
	require.Len(t, specs, 1)
	_, err = specs[0].Options.LookupErr("validator")
	assert.Error(t, err, "collMod не должен переноситься: %s", specs[0].Options)
	names, err := dstClient.Database(db).ListCollectionNames(ctx, bson.M{"name": "v"})
	require.NoError(t, err)
	assert.Empty(t, names, "view не должен создаваться")
	specs, err = dstClient.Database(db).ListCollectionSpecifications(ctx, bson.M{"name": "capped"})
	require.NoError(t, err)
	require.Len(t, specs, 1)
	_, err = specs[0].Options.LookupErr("capped")
	assert.Error(t, err, "опции create не должны переноситься: %s", specs[0].Options)
}

// Smoke: рестарт с чекпойнта, сохранённого после rename, не ломает состояние.
// Окно между applyDDL и saveToken отсюда не поймать.
func TestIntegrationRestartAfterDDL(t *testing.T) {
	db := prepare(t)
	rn := start(t, Config{StartTS: srcNow(t), BatchSize: 1000, DB: db})

	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	a := srcClient.Database(db).Collection("a")

	for i := range 3 {
		_, err := a.InsertOne(ctx, bson.D{{Key: "_id", Value: int32(i)}, {Key: "gen", Value: "old"}})
		require.NoError(t, err)
	}
	renameColl(ctx, t, db, "a", "b")
	rn.waitSame(db)
	rn.waitQuiet()
	rn.stop()
	// новая a с теми же _id: повтор rename поверх неё уничтожил бы b
	const n = 3
	for i := range n {
		_, err := a.InsertOne(ctx, bson.D{{Key: "_id", Value: int32(i)}, {Key: "gen", Value: "new"}})
		require.NoError(t, err)
	}

	rn2 := start(t, Config{BatchSize: 1000, DB: db})
	rn2.waitSame(db)
	rn2.waitQuiet()

	assert.EqualValues(t, n, rn2.r.Applied(), "применено после перезапуска: только вставки в новую a")
	colls, err := collNames(ctx, dstClient, db)
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, colls)
}
