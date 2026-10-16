//go:build integration

package replicator

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func TestIntegrationDML(t *testing.T) {
	db := prepare(t)
	// маленькая пачка: часть событий уходит на цель по заполнению пачки, а не по простою потока
	rn := start(t, Config{StartTS: srcNow(t), BatchSize: 3, DB: db})

	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	orders := srcClient.Database(db).Collection("orders")
	users := srcClient.Database(db).Collection("users")

	price, err := bson.ParseDecimal128("12345.6789")
	require.NoError(t, err)
	created := bson.NewDateTimeFromTime(time.Date(2026, 10, 13, 12, 30, 15, 123e6, time.UTC))

	// поля в «неалфавитном» порядке, смешанные типы: цель должна получить те же байты
	_, err = orders.InsertMany(ctx, []any{
		bson.D{
			{Key: "_id", Value: int32(1)},
			{Key: "z", Value: "last-first"},
			{Key: "price", Value: price},
			{Key: "created", Value: created},
			{Key: "qty", Value: int64(5)},
			{Key: "nested", Value: bson.D{
				{Key: "b", Value: int32(2)},
				{Key: "a", Value: bson.A{int32(1), "x", bson.D{{Key: "c", Value: true}}, 2.5}},
			}},
			{Key: "tags", Value: bson.A{"new", "hot"}},
		},
		bson.D{{Key: "_id", Value: int32(2)}, {Key: "b", Value: "replace me"}, {Key: "a", Value: int32(1)}},
		bson.D{{Key: "_id", Value: int32(3)}, {Key: "doomed", Value: true}},
		bson.D{{Key: "_id", Value: int32(4)}, {Key: "counter", Value: int32(0)}, {Key: "nested", Value: bson.D{}}},
	})
	require.NoError(t, err)

	_, err = orders.UpdateOne(ctx, bson.M{"_id": 1}, bson.D{
		{Key: "$set", Value: bson.D{{Key: "nested.b", Value: "two"}, {Key: "status", Value: "paid"}}},
		{Key: "$unset", Value: bson.D{{Key: "tags", Value: ""}}},
		{Key: "$inc", Value: bson.D{{Key: "qty", Value: int64(-2)}}},
	})
	require.NoError(t, err)
	_, err = orders.UpdateMany(ctx, bson.M{"_id": bson.M{"$in": bson.A{1, 4}}}, bson.D{
		{Key: "$inc", Value: bson.D{{Key: "counter", Value: int32(3)}}},
		{Key: "$set", Value: bson.D{{Key: "nested.deep.flag", Value: false}}},
	})
	require.NoError(t, err)
	_, err = orders.ReplaceOne(ctx, bson.M{"_id": 2}, bson.D{
		{Key: "y", Value: bson.A{created, price}},
		{Key: "x", Value: bson.D{{Key: "k", Value: bson.Null{}}}},
	})
	require.NoError(t, err)
	_, err = orders.DeleteOne(ctx, bson.M{"_id": 3})
	require.NoError(t, err)

	ids := []bson.ObjectID{bson.NewObjectID(), bson.NewObjectID(), bson.NewObjectID()}
	for i, id := range ids {
		_, err = users.InsertOne(ctx, bson.D{
			{Key: "_id", Value: id},
			{Key: "name", Value: fmt.Sprintf("user-%d", i)},
			{Key: "balance", Value: price},
			{Key: "seen", Value: bson.A{created}},
		})
		require.NoError(t, err)
	}
	_, err = users.UpdateOne(ctx, bson.M{"_id": ids[0]}, bson.D{{Key: "$push", Value: bson.D{{Key: "seen", Value: created}}}})
	require.NoError(t, err)
	_, err = users.DeleteOne(ctx, bson.M{"_id": ids[1]})
	require.NoError(t, err)
	_, err = users.UpdateOne(ctx, bson.M{"_id": ids[2]}, bson.D{{Key: "$unset", Value: bson.D{{Key: "balance", Value: ""}}}})
	require.NoError(t, err)

	rn.waitSame(db)
	// Applied растёт после BulkWrite, поэтому сверяем его, когда поток успокоился
	rn.waitQuiet()

	// цель не пустая: совпадение не из-за того, что обе стороны пусты
	n, err := dstClient.Database(db).Collection("orders").CountDocuments(ctx, bson.D{})
	require.NoError(t, err)
	assert.EqualValues(t, 3, n)
	n, err = dstClient.Database(db).Collection("users").CountDocuments(ctx, bson.D{})
	require.NoError(t, err)
	assert.EqualValues(t, 2, n)
	assert.NotNil(t, rn.ckptToken(), "чекпойнт сохранён")
	// 4 insert + 3 update (UpdateOne + UpdateMany на 2) + replace + delete; 3 insert + 2 update + delete
	assert.EqualValues(t, 15, rn.r.Applied())
}

func TestIntegrationTransaction(t *testing.T) {
	db := prepare(t)
	rn := start(t, Config{StartTS: srcNow(t), BatchSize: 1000, DB: db})

	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	// коллекции заранее: внутри транзакции не должно быть неявного create
	require.NoError(t, srcClient.Database(db).CreateCollection(ctx, "accounts"))
	require.NoError(t, srcClient.Database(db).CreateCollection(ctx, "ledger"))
	accounts := srcClient.Database(db).Collection("accounts")
	ledger := srcClient.Database(db).Collection("ledger")

	_, err := accounts.InsertMany(ctx, []any{
		bson.D{{Key: "_id", Value: "alice"}, {Key: "balance", Value: int64(100)}},
		bson.D{{Key: "_id", Value: "bob"}, {Key: "balance", Value: int64(50)}},
	})
	require.NoError(t, err)
	_, err = ledger.InsertOne(ctx, bson.D{{Key: "_id", Value: int32(1)}, {Key: "status", Value: "open"}})
	require.NoError(t, err)

	sess, err := srcClient.StartSession()
	require.NoError(t, err)
	defer sess.EndSession(context.Background())
	_, err = sess.WithTransaction(ctx, func(sc context.Context) (any, error) {
		ops := []func() error{
			func() error {
				_, e := accounts.InsertOne(sc, bson.D{{Key: "_id", Value: "carol"}, {Key: "balance", Value: int64(30)}})
				return e
			},
			func() error {
				_, e := accounts.UpdateOne(sc, bson.M{"_id": "alice"}, bson.D{{Key: "$inc", Value: bson.D{{Key: "balance", Value: int64(-30)}}}})
				return e
			},
			func() error { _, e := accounts.DeleteOne(sc, bson.M{"_id": "bob"}); return e },
			func() error {
				_, e := ledger.UpdateOne(sc, bson.M{"_id": 1}, bson.D{{Key: "$set", Value: bson.D{{Key: "status", Value: "closed"}}}})
				return e
			},
			func() error {
				_, e := ledger.InsertOne(sc, bson.D{{Key: "_id", Value: int32(2)}, {Key: "from", Value: "alice"}, {Key: "to", Value: "carol"}})
				return e
			},
		}
		for _, op := range ops {
			if e := op(); e != nil {
				return nil, e
			}
		}
		return nil, nil
	})
	require.NoError(t, err)

	rn.waitSame(db)
	rn.waitQuiet()

	n, err := dstClient.Database(db).Collection("accounts").CountDocuments(ctx, bson.D{})
	require.NoError(t, err)
	assert.EqualValues(t, 2, n)
	n, err = dstClient.Database(db).Collection("ledger").CountDocuments(ctx, bson.D{})
	require.NoError(t, err)
	assert.EqualValues(t, 2, n)
	// 3 insert до транзакции + insert, update, delete, update, insert в транзакции
	assert.EqualValues(t, 8, rn.r.Applied())
}

func TestIntegrationResume(t *testing.T) {
	db := prepare(t)
	startTS := srcNow(t)
	rn := start(t, Config{StartTS: startTS, BatchSize: 1000, DB: db})

	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	items := srcClient.Database(db).Collection("items")

	for i := range 3 {
		_, err := items.InsertOne(ctx, bson.D{{Key: "_id", Value: int32(i)}, {Key: "v", Value: int32(0)}})
		require.NoError(t, err)
	}
	rn.waitSame(db)
	rn.waitQuiet()
	rn.stop()
	// пока репликатор стоит: только insert/update без последующего delete,
	// иначе update придёт с пустым fullDocument и не попадёт в Applied
	const n = 5
	for i := 3; i < 6; i++ {
		_, err := items.InsertOne(ctx, bson.D{{Key: "_id", Value: int32(i)}, {Key: "v", Value: int32(0)}})
		require.NoError(t, err)
	}
	_, err := items.UpdateOne(ctx, bson.M{"_id": 0}, bson.D{{Key: "$set", Value: bson.D{{Key: "v", Value: int32(1)}}}})
	require.NoError(t, err)
	_, err = items.UpdateOne(ctx, bson.M{"_id": 4}, bson.D{{Key: "$set", Value: bson.D{{Key: "v", Value: int32(2)}}}})
	require.NoError(t, err)

	// StartTS тот же, что при первом запуске, но при чекпойнте он игнорируется
	rn2 := start(t, Config{StartTS: startTS, BatchSize: 1000, DB: db})
	rn2.waitSame(db)
	rn2.waitQuiet()

	// ровно n: продолжили с токена, события до остановки повторно не применялись
	assert.EqualValues(t, n, rn2.r.Applied())
}

func TestIntegrationReplayIdempotent(t *testing.T) {
	db := prepare(t)

	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()

	// «дамп» до окна: коллекции с индексами на обоих кластерах. В окне их
	// индексы и сами коллекции удаляются, и при повторе ловим 26/27.
	for _, c := range []*mongo.Client{srcClient, dstClient} {
		for _, coll := range []string{"pre", "gone"} {
			_, err := c.Database(db).Collection(coll).Indexes().CreateOne(ctx, mongo.IndexModel{
				Keys:    bson.D{{Key: "k", Value: 1}},
				Options: options.Index().SetName(coll + "_k"),
			})
			require.NoError(t, err)
		}
	}
	lastSrcSec = srcOpTime(t).T
	startTS := srcNow(t)
	// drop gone ожидаемый: разрешён в обоих прогонах
	rn := start(t, Config{StartTS: startTS, BatchSize: 1000, DB: db, ApplyDrops: []string{db + ".gone"}})

	src := srcClient.Database(db)
	require.NoError(t, src.CreateCollection(ctx, "c")) // повтор: 48
	c := src.Collection("c")
	// повтор update X идёт с пустым fullDocument (X уже удалён)
	_, err := c.InsertOne(ctx, bson.D{{Key: "_id", Value: "x"}, {Key: "v", Value: int32(1)}})
	require.NoError(t, err)
	_, err = c.UpdateOne(ctx, bson.M{"_id": "x"}, bson.D{{Key: "$set", Value: bson.D{{Key: "v", Value: int32(2)}}}})
	require.NoError(t, err)
	_, err = c.DeleteOne(ctx, bson.M{"_id": "x"})
	require.NoError(t, err)
	_, err = c.InsertOne(ctx, bson.D{{Key: "_id", Value: "y"}, {Key: "v", Value: int32(1)}})
	require.NoError(t, err)
	_, err = c.UpdateOne(ctx, bson.M{"_id": "y"}, bson.D{{Key: "$inc", Value: bson.D{{Key: "v", Value: int32(1)}}}})
	require.NoError(t, err)

	_, err = c.Indexes().CreateOne(ctx, mongo.IndexModel{ // повтор: индекс уже есть
		Keys:    bson.D{{Key: "v", Value: -1}},
		Options: options.Index().SetName("c_v"),
	})
	require.NoError(t, err)
	require.NoError(t, src.Collection("pre").Indexes().DropOne(ctx, "pre_k"))   // повтор: 27
	require.NoError(t, src.Collection("gone").Indexes().DropOne(ctx, "gone_k")) // повтор: 26
	require.NoError(t, src.Collection("gone").Drop(ctx))

	rn.waitSame(db)
	rn.waitQuiet()
	rn.stop()
	// сброс чекпойнта базы: поток перечитывается с того же StartTS поверх готовой цели
	_, err = ckptOf(db).DeleteOne(ctx, bson.M{"_id": ckptID})
	require.NoError(t, err)

	rn2 := start(t, Config{StartTS: startTS, BatchSize: 1000, DB: db, ApplyDrops: []string{db + ".gone"}})
	// цель уже совпадает, поэтому сначала ждём, что повтор действительно прошёл:
	// insert X, delete X, insert Y, update Y (update X пропущен)
	const m = 4
	rn2.waitUntil(func(c *assert.CollectT) error {
		assert.GreaterOrEqual(c, rn2.r.Applied(), int64(m), "применено операций")
		return nil
	}, "повтор не дошёл до %d применённых операций за %s", m, waitTimeout)
	rn2.waitSame(db)
	rn2.waitQuiet()

	assert.EqualValues(t, m, rn2.r.Applied())
}

func TestIntegrationNoStartPoint(t *testing.T) {
	db := prepare(t)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	r := newIT(dstClient, Config{BatchSize: 1000, DB: db})

	err := r.Run(ctx)

	require.ErrorIs(t, err, ErrNoStartPoint)
	_, found := readLease(t, db)
	require.True(t, found, "документ создан захватом аренды")

	require.NoError(t, r.Release(ctx))

	_, found = readLease(t, db)
	assert.False(t, found, "документ без позиции удалён при снятии аренды")
}

// Цель отвергла запись: Run возвращает ошибку, чекпойнт не проходит дальше
// отвергнутого события, а после исправления цели репликация продолжается.
func TestIntegrationWriteError(t *testing.T) {
	db := prepare(t)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()

	// уникальный индекс только на цели: второй документ с тем же code она не примет
	dstC := dstClient.Database(db).Collection("c")
	_, err := dstC.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "code", Value: 1}},
		Options: options.Index().SetName("c_code_u").SetUnique(true),
	})
	require.NoError(t, err)

	startTS := srcNow(t)
	c := srcClient.Database(db).Collection("c")
	for _, id := range []int32{1, 2} {
		_, err = c.InsertOne(ctx, bson.D{{Key: "_id", Value: id}, {Key: "code", Value: "x"}})
		require.NoError(t, err)
	}

	// пачка из одной операции: первый документ записан и закоммичен, второй — нет
	r, err := runErr(t, Config{StartTS: startTS, BatchSize: 1, DB: db})

	require.ErrorContains(t, err, "bulk write в "+db+".c")
	assert.True(t, mongo.IsDuplicateKeyError(err), "%v", err)
	assert.EqualValues(t, 1, r.Applied())

	// с чекпойнта снова упираемся во второй документ: чекпойнт его не прошёл
	r, err = runErr(t, Config{BatchSize: 1000, DB: db})

	assert.True(t, mongo.IsDuplicateKeyError(err), "%v", err)
	assert.EqualValues(t, 0, r.Applied())

	// цель исправлена; Run того же репликатора, как в ретраях main: хвост
	// неудачного Run не должен примениться второй раз
	require.NoError(t, dstC.Indexes().DropOne(ctx, "c_code_u"))
	rn := startOn(t, r)
	rn.waitSame(db)
	rn.waitQuiet()

	assert.EqualValues(t, 1, rn.r.Applied())
}
