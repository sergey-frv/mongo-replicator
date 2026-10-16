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

// Поток уровня базы: события других баз (DML и DDL) на цель не попадают,
// rename внутри базы применяется.
func TestIntegrationSingleDB(t *testing.T) {
	db := prepare(t)
	other := db + "_other"
	cleanDB(t, other)
	rn := start(t, Config{StartTS: srcNow(t), BatchSize: 1000, DB: db})

	ctx, cancel := context.WithTimeout(context.Background(), 2*waitTimeout)
	defer cancel()

	// сначала события другой базы: они в oplog раньше событий базы теста
	o := srcClient.Database(other).Collection("c")
	require.NoError(t, srcClient.Database(other).CreateCollection(ctx, "c"))
	_, err := o.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "k", Value: 1}}, Options: options.Index().SetName("c_k")})
	require.NoError(t, err)
	_, err = o.InsertMany(ctx, []any{
		bson.D{{Key: "_id", Value: int32(1)}, {Key: "k", Value: int32(1)}},
		bson.D{{Key: "_id", Value: int32(2)}, {Key: "k", Value: int32(2)}},
	})
	require.NoError(t, err)
	_, err = o.UpdateOne(ctx, bson.M{"_id": 1}, bson.D{{Key: "$set", Value: bson.D{{Key: "k", Value: int32(10)}}}})
	require.NoError(t, err)

	// база теста: insert, rename x → y, update и delete уже в y
	x := srcClient.Database(db).Collection("x")
	_, err = x.InsertMany(ctx, []any{
		bson.D{{Key: "_id", Value: int32(1)}, {Key: "v", Value: "a"}},
		bson.D{{Key: "_id", Value: int32(2)}, {Key: "v", Value: "b"}},
		bson.D{{Key: "_id", Value: int32(3)}, {Key: "v", Value: "c"}},
	})
	require.NoError(t, err)
	renameColl(ctx, t, db, "x", "y")
	y := srcClient.Database(db).Collection("y")
	_, err = y.UpdateOne(ctx, bson.M{"_id": 1}, bson.D{{Key: "$set", Value: bson.D{{Key: "v", Value: "a2"}}}})
	require.NoError(t, err)
	_, err = y.DeleteOne(ctx, bson.M{"_id": 3})
	require.NoError(t, err)

	// финальная запись: когда она на цели, события other уже пройдены
	_, err = srcClient.Database(db).Collection("z").InsertOne(ctx, bson.D{{Key: "_id", Value: "final"}})
	require.NoError(t, err)

	rn.waitSame(db)
	rn.waitQuiet()

	colls, err := collNames(ctx, dstClient, db)
	require.NoError(t, err)
	assert.Equal(t, []string{"y", "z"}, colls)
	n, err := dstClient.Database(db).Collection("y").CountDocuments(ctx, bson.D{})
	require.NoError(t, err)
	assert.EqualValues(t, 2, n, "документов y на цели")
	assert.False(t, dbExists(ctx, t, dstClient, other), "база %s попала на цель", other)
	// 3 insert + update + delete + финальный insert; события other не считаются
	assert.EqualValues(t, 6, rn.r.Applied())
}

// --dst-db: DML и DDL, включая обе стороны rename, применяются в базе цели;
// база с именем источника на цели не появляется.
func TestIntegrationDstDB(t *testing.T) {
	db := prepare(t)
	dstDB := db + "_v2"
	cleanDB(t, dstDB)
	rn := start(t, Config{StartTS: srcNow(t), BatchSize: 1000, DB: db, DstDB: dstDB})

	ctx, cancel := context.WithTimeout(context.Background(), 2*waitTimeout)
	defer cancel()
	src := srcClient.Database(db)
	a, b := src.Collection("a"), src.Collection("b")

	require.NoError(t, src.CreateCollection(ctx, "a"))
	_, err := a.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "k", Value: 1}}, Options: options.Index().SetName("a_k")},
		{Keys: bson.D{{Key: "code", Value: 1}}, Options: options.Index().SetName("a_code_u").SetUnique(true)},
	})
	require.NoError(t, err)
	for i := range 3 {
		_, err = a.InsertOne(ctx, bson.D{{Key: "_id", Value: int32(i)}, {Key: "code", Value: fmt.Sprintf("c%d", i)}, {Key: "k", Value: int32(i)}})
		require.NoError(t, err)
	}
	_, err = a.UpdateOne(ctx, bson.M{"_id": 0}, bson.D{{Key: "$set", Value: bson.D{{Key: "k", Value: int32(10)}}}})
	require.NoError(t, err)
	_, err = a.DeleteOne(ctx, bson.M{"_id": 2})
	require.NoError(t, err)
	require.NoError(t, a.Indexes().DropOne(ctx, "a_k"))

	// b — цель второго rename: уйдёт по dropTarget
	_, err = b.InsertOne(ctx, bson.D{{Key: "_id", Value: int32(100)}, {Key: "old", Value: true}})
	require.NoError(t, err)
	_, err = b.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "old", Value: 1}}, Options: options.Index().SetName("b_old")})
	require.NoError(t, err)

	// до rename ждём совпадения: update, прочитанный после rename, придёт с пустым fullDocument
	rn.waitSameDBs(db, dstDB)

	renameColl(ctx, t, db, "a", "c") // в свободное имя
	renameColl(ctx, t, db, "c", "b") // поверх существующей
	_, err = b.InsertOne(ctx, bson.D{{Key: "_id", Value: int32(10)}, {Key: "code", Value: "c10"}})
	require.NoError(t, err)

	rn.waitSameDBs(db, dstDB)
	rn.waitQuiet()

	colls, err := collNames(ctx, dstClient, dstDB)
	require.NoError(t, err)
	assert.Equal(t, []string{"b"}, colls)
	assert.Equal(t, []string{"_id_", "a_code_u"}, indexNames(ctx, t, dstClient, dstDB, "b"), "индексы b заменены индексами a")
	n, err := dstClient.Database(dstDB).Collection("b").CountDocuments(ctx, bson.D{})
	require.NoError(t, err)
	assert.EqualValues(t, 3, n, "0, 1 из a и 10; документ старой b удалён dropTarget")
	assert.False(t, dbExists(ctx, t, dstClient, db), "база источника %s появилась на цели", db)
	assert.Equal(t, dbPair{src: db, dst: dstDB}, rn.ckptPair())
}

// Режим одной базы: чекпойнт помнит базу, рестарт продолжает с токена.
func TestIntegrationSingleDBResume(t *testing.T) {
	db := prepare(t)
	rn := start(t, Config{StartTS: srcNow(t), BatchSize: 1000, DB: db})

	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	items := srcClient.Database(db).Collection("items")

	for i := range 3 {
		_, err := items.InsertOne(ctx, bson.D{{Key: "_id", Value: int32(i)}})
		require.NoError(t, err)
	}
	rn.waitSame(db)
	rn.waitQuiet()
	rn.stop()

	assert.Equal(t, dbPair{src: db, dst: db}, rn.ckptPair())

	const n = 2
	for i := 3; i < 3+n; i++ {
		_, err := items.InsertOne(ctx, bson.D{{Key: "_id", Value: int32(i)}})
		require.NoError(t, err)
	}

	// без StartTS: продолжить можно только с чекпойнта
	rn2 := start(t, Config{BatchSize: 1000, DB: db})
	rn2.waitSame(db)
	rn2.waitQuiet()

	assert.EqualValues(t, n, rn2.r.Applied(), "применено после перезапуска: только вставки после остановки")
	assert.Equal(t, dbPair{src: db, dst: db}, rn2.ckptPair())
}

// Чекпойнт базы цели записан для другой базы источника: Run отказывается стартовать.
// Токен произвольный: проверка идёт до открытия потока.
func TestIntegrationDBMismatch(t *testing.T) {
	cleanDBs(t, "it_mismatch_a", "it_mismatch_b")
	writeCkpt(t, dbPair{src: "it_mismatch_a", dst: "it_mismatch_a"})

	_, err := runErr(t, Config{BatchSize: 1000, DB: "it_mismatch_b", DstDB: "it_mismatch_a"})

	require.ErrorIs(t, err, ErrDBMismatch)
	assert.ErrorContains(t, err, `"it_mismatch_a" → "it_mismatch_a"`)

	// у другой базы цели свой чекпойнт, а его нет
	_, err = runErr(t, Config{BatchSize: 1000, DB: "it_mismatch_b"})

	require.ErrorIs(t, err, ErrNoStartPoint)

	_, err = runErr(t, Config{BatchSize: 1000, DB: "it_mismatch_a"})

	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrDBMismatch)
	assert.ErrorContains(t, err, "открытие change stream", "чекпойнт прочитан, сервер отверг токен-заглушку")
}

// Другая база цели — другой чекпойнт: чужой не подхватывается, стартовать не с чего.
func TestIntegrationDstDBMismatch(t *testing.T) {
	cleanDBs(t, "it_mismatch_a", "it_mismatch_a2", "it_mismatch_a3")
	writeCkpt(t, dbPair{src: "it_mismatch_a", dst: "it_mismatch_a2"})

	_, err := runErr(t, Config{BatchSize: 1000, DB: "it_mismatch_a", DstDB: "it_mismatch_a3"})

	require.ErrorIs(t, err, ErrNoStartPoint)

	_, err = runErr(t, Config{BatchSize: 1000, DB: "it_mismatch_a"})

	require.ErrorIs(t, err, ErrNoStartPoint, "без --dst-db цель — база источника")

	_, err = runErr(t, Config{BatchSize: 1000, DB: "it_mismatch_a", DstDB: "it_mismatch_a2"})

	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrDBMismatch)
	assert.ErrorContains(t, err, "открытие change stream", "чекпойнт прочитан, сервер отверг токен-заглушку")
}
