//go:build integration

package replicator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

const (
	waitTimeout = 30 * time.Second
	pollEvery   = 200 * time.Millisecond
	// больше maxAwaitTime потока (1 с): за это время Run успевает сделать
	// хотя бы один пустой getMore и сохранить чекпойнт
	quietInterval = 1600 * time.Millisecond
)

var (
	srcClient, dstClient *mongo.Client
	// секунда последней операции на источнике, сделанной обвязкой предыдущего
	// теста (или prepare текущего); srcNow ждёт, пока время уйдёт дальше
	lastSrcSec uint32
)

func TestMain(m *testing.M) {
	srcURI, dstURI := os.Getenv("SRC_URI"), os.Getenv("DST_URI")
	if srcURI == "" || dstURI == "" {
		log.Fatal("SRC_URI и DST_URI не заданы: интеграционные тесты запускаются через make test-integration")
	}

	var err error
	if srcClient, err = connectIT(options.Client().ApplyURI(srcURI).SetAppName("cs-replicator")); err != nil {
		log.Fatalf("источник: %v", err)
	}
	// как в main: w:majority на цели
	if dstClient, err = connectIT(options.Client().ApplyURI(dstURI).
		SetAppName("cs-replicator").
		SetWriteConcern(writeconcern.Majority())); err != nil {
		log.Fatalf("цель: %v", err)
	}

	code := m.Run()
	_ = srcClient.Disconnect(context.Background())
	_ = dstClient.Disconnect(context.Background())
	os.Exit(code)
}

func connectIT(opts *options.ClientOptions) (*mongo.Client, error) {
	c, err := mongo.Connect(opts)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.Ping(ctx, nil); err != nil {
		return nil, fmt.Errorf("ping: %w", err)
	}
	return c, nil
}

// snake превращает имя теста в имя базы: TestIntegrationDML/sub → integration_dml_sub.
func snake(name string) string {
	rs := []rune(strings.TrimPrefix(name, "Test"))
	var sb strings.Builder
	for i, r := range rs {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			sb.WriteByte('_')
			continue
		}
		if unicode.IsUpper(r) && i > 0 {
			prev := rs[i-1]
			nextLower := i+1 < len(rs) && unicode.IsLower(rs[i+1])
			if unicode.IsLower(prev) || unicode.IsDigit(prev) || (unicode.IsUpper(prev) && nextLower) {
				sb.WriteByte('_')
			}
		}
		sb.WriteRune(unicode.ToLower(r))
	}
	return sb.String()
}

func TestSnake(t *testing.T) {
	assert.Equal(t, "integration_dml", snake("TestIntegrationDML"))
	assert.Equal(t, "integration_ddl_restart", snake("TestIntegrationDDLRestart"))
	assert.Equal(t, "integration_stop_on_cleanup_sub", snake("TestIntegrationStopOnCleanup/sub"))
}

// srcOpTime пишет no-op в oplog источника и возвращает его время:
// на простаивающем кластере operationTime иначе двигается раз в 10 с.
func srcOpTime(t *testing.T) bson.Timestamp {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	raw, err := srcClient.Database("admin").RunCommand(ctx, bson.D{
		{Key: "appendOplogNote", Value: 1},
		{Key: "data", Value: bson.D{{Key: "it", Value: t.Name()}}},
	}).Raw()
	require.NoError(t, err)
	ts, i := raw.Lookup("operationTime").Timestamp()
	return bson.Timestamp{T: ts, I: i}
}

// srcNow возвращает StartTS для теста. StartTS округлён до секунды, а
// startAtOperationTime включительный, поэтому ждём, пока время источника
// уйдёт за секунду последней операции обвязки: иначе поток захватит хвост
// предыдущего теста, включая его cleanup-dropDatabase.
func srcNow(t *testing.T) uint32 {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		ts := srcOpTime(t).T
		if ts > lastSrcSec {
			return ts
		}
		require.True(t, time.Now().Before(deadline), "время источника не двигается: %d", ts)
		time.Sleep(100 * time.Millisecond)
	}
}

func dropDB(t *testing.T, db string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, srcClient.Database(db).Drop(ctx))
	require.NoError(t, dstClient.Database(db).Drop(ctx))
	lastSrcSec = srcOpTime(t).T
}

// prepare выдаёт тесту чистую базу на обоих кластерах и пустой чекпойнт.
// Вызывать до start: cleanup выполняются в обратном порядке, и база
// дропается уже после остановки репликатора.
func prepare(t *testing.T) (db string) {
	t.Helper()
	db = "it_" + snake(t.Name())
	cleanDB(t, db)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, dstClient.Database(ckptDB).Collection(ckptColl).Drop(ctx))
	return db
}

// cleanDB дропает базу на обоих кластерах сейчас и в cleanup теста.
// Как и prepare, вызывать до start.
func cleanDB(t *testing.T, db string) {
	t.Helper()
	dropDB(t, db) // остатки упавших прогонов: контейнеры живут между запусками
	t.Cleanup(func() { dropDB(t, db) })
}

// runner — репликатор, запущенный в горутине.
type runner struct {
	t    *testing.T
	r    *Replicator
	stop func()
	done chan struct{}
	err  error // читать только после закрытия done
}

// start запускает Run в горутине и регистрирует остановку в t.Cleanup:
// упавший require не оставит репликатор работать в следующих тестах.
func start(t *testing.T, cfg Config) *runner {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	rn := &runner{t: t, r: New(srcClient, dstClient, cfg), done: make(chan struct{})}
	go func() {
		defer close(rn.done)
		rn.err = rn.r.Run(ctx)
	}()

	var once sync.Once
	rn.stop = func() {
		once.Do(func() {
			cancel()
			<-rn.done
			assert.ErrorIs(t, rn.err, context.Canceled, "Run должен завершиться только отменой контекста")
		})
	}
	t.Cleanup(rn.stop)
	return rn
}

// exited сообщает, вернулся ли Run, и с какой ошибкой.
func (rn *runner) exited() (bool, error) {
	select {
	case <-rn.done:
		return true, rn.err
	default:
		return false, nil
	}
}

// waitSame ждёт совпадения базы на кластерах; если Run уже вернулся — падает сразу.
func (rn *runner) waitSame(db string) {
	rn.t.Helper()
	deadline := time.Now().Add(waitTimeout)
	var diff string
	for {
		if done, err := rn.exited(); done {
			rn.t.Fatalf("Run вернулся раньше времени: %v", err)
		}
		var ok bool
		if ok, diff = same(db); ok {
			return
		}
		if time.Now().After(deadline) {
			rn.t.Fatalf("база %s не совпала за %s: %s", db, waitTimeout, diff)
		}
		time.Sleep(pollEvery)
	}
}

// waitQuiet ждёт, пока чекпойнт и Applied перестанут меняться. Закрывает
// гонку flush → saveToken: данные на цели уже видны, а чекпойнт ещё старый.
func (rn *runner) waitQuiet() {
	rn.t.Helper()
	deadline := time.Now().Add(waitTimeout)
	prevTok, prevN := rn.ckptToken(), rn.r.Applied()
	for {
		time.Sleep(quietInterval)
		if done, err := rn.exited(); done {
			rn.t.Fatalf("Run вернулся раньше времени: %v", err)
		}
		tok, n := rn.ckptToken(), rn.r.Applied()
		if tok != nil && bytes.Equal(tok, prevTok) && n == prevN {
			return
		}
		if time.Now().After(deadline) {
			rn.t.Fatalf("чекпойнт не успокоился за %s", waitTimeout)
		}
		prevTok, prevN = tok, n
	}
}

func (rn *runner) ckptToken() bson.Raw {
	rn.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var doc struct {
		Token bson.Raw `bson:"token"`
	}
	err := dstClient.Database(ckptDB).Collection(ckptColl).FindOne(ctx, bson.M{"_id": ckptID}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil
	}
	require.NoError(rn.t, err)
	return doc.Token
}

// same сравнивает базу на источнике и цели: списки коллекций, документы по _id
// байт в байт и индексы по name/key/unique/partialFilterExpression/expireAfterSeconds.
func same(db string) (ok bool, diff string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	srcColls, err := collNames(ctx, srcClient, db)
	if err != nil {
		return false, "источник: " + err.Error()
	}
	dstColls, err := collNames(ctx, dstClient, db)
	if err != nil {
		return false, "цель: " + err.Error()
	}
	if !slices.Equal(srcColls, dstColls) {
		return false, fmt.Sprintf("коллекции: источник %v, цель %v", srcColls, dstColls)
	}

	for _, coll := range srcColls {
		if d := diffDocs(ctx, db, coll); d != "" {
			return false, d
		}
		if d := diffIndexes(ctx, db, coll); d != "" {
			return false, d
		}
	}
	return true, ""
}

func collNames(ctx context.Context, c *mongo.Client, db string) ([]string, error) {
	names, err := c.Database(db).ListCollectionNames(ctx, bson.D{})
	slices.Sort(names)
	return names, err
}

func diffDocs(ctx context.Context, db, coll string) string {
	read := func(c *mongo.Client) ([]bson.Raw, error) {
		cur, err := c.Database(db).Collection(coll).Find(ctx, bson.D{}, options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}))
		if err != nil {
			return nil, err
		}
		var docs []bson.Raw
		err = cur.All(ctx, &docs)
		return docs, err
	}
	srcDocs, err := read(srcClient)
	if err != nil {
		return fmt.Sprintf("%s.%s источник: %v", db, coll, err)
	}
	dstDocs, err := read(dstClient)
	if err != nil {
		return fmt.Sprintf("%s.%s цель: %v", db, coll, err)
	}
	for i := range max(len(srcDocs), len(dstDocs)) {
		switch {
		case i >= len(srcDocs):
			return fmt.Sprintf("%s.%s: лишний документ на цели %s", db, coll, dstDocs[i])
		case i >= len(dstDocs):
			return fmt.Sprintf("%s.%s: нет документа на цели %s", db, coll, srcDocs[i])
		case !bytes.Equal(srcDocs[i], dstDocs[i]):
			return fmt.Sprintf("%s.%s: документы различаются\n источник %s\n цель     %s", db, coll, srcDocs[i], dstDocs[i])
		}
	}
	return ""
}

func diffIndexes(ctx context.Context, db, coll string) string {
	read := func(c *mongo.Client) ([]string, error) {
		cur, err := c.Database(db).Collection(coll).Indexes().List(ctx)
		if err != nil {
			return nil, err
		}
		var specs []bson.Raw
		if err := cur.All(ctx, &specs); err != nil {
			return nil, err
		}
		out := make([]string, 0, len(specs))
		for _, s := range specs {
			out = append(out, indexSig(s))
		}
		slices.Sort(out)
		return out, nil
	}
	srcIdx, err := read(srcClient)
	if err != nil {
		return fmt.Sprintf("%s.%s индексы источника: %v", db, coll, err)
	}
	dstIdx, err := read(dstClient)
	if err != nil {
		return fmt.Sprintf("%s.%s индексы цели: %v", db, coll, err)
	}
	if !slices.Equal(srcIdx, dstIdx) {
		return fmt.Sprintf("%s.%s: индексы различаются\n источник %v\n цель     %v", db, coll, srcIdx, dstIdx)
	}
	return ""
}

// indexSig — значимые для сравнения поля спецификации индекса. Версия (v)
// и прочие служебные поля между 8.0 и 5.0 могут отличаться и не сравниваются.
func indexSig(spec bson.Raw) string {
	unique, _ := spec.Lookup("unique").BooleanOK()
	ttl := "-"
	if v, err := spec.LookupErr("expireAfterSeconds"); err == nil {
		n, _ := v.AsInt64OK()
		ttl = fmt.Sprint(n)
	}
	pfe := "-"
	if v, err := spec.LookupErr("partialFilterExpression"); err == nil {
		pfe = v.String()
	}
	return fmt.Sprintf("name=%s key=%s unique=%t partial=%s ttl=%s",
		spec.Lookup("name").StringValue(), spec.Lookup("key"), unique, pfe, ttl)
}

// Проверка обвязки: репликатор, запущенный в подтесте, останавливается его
// cleanup-ом, даже если тест сам stop не вызвал.
func TestIntegrationStopOnCleanup(t *testing.T) {
	var rn *runner
	t.Run("sub", func(t *testing.T) {
		prepare(t)
		rn = start(t, Config{StartTS: srcNow(t), BatchSize: 1000})
	})
	require.NotNil(t, rn)
	done, err := rn.exited()
	require.True(t, done, "репликатор продолжает работать после завершения подтеста")
	require.ErrorIs(t, err, context.Canceled)
}

func TestIntegrationDML(t *testing.T) {
	db := prepare(t)
	// маленькая пачка: часть событий уходит на цель по заполнению пачки, а не по простою потока
	rn := start(t, Config{StartTS: srcNow(t), BatchSize: 3})

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

	// цель не пустая: совпадение не из-за того, что обе стороны пусты
	n, err := dstClient.Database(db).Collection("orders").CountDocuments(ctx, bson.D{})
	require.NoError(t, err)
	assert.EqualValues(t, 3, n)
	n, err = dstClient.Database(db).Collection("users").CountDocuments(ctx, bson.D{})
	require.NoError(t, err)
	assert.EqualValues(t, 2, n)

	// Applied растёт после BulkWrite, поэтому сверяем его, когда поток успокоился
	rn.waitQuiet()
	assert.NotNil(t, rn.ckptToken(), "чекпойнт сохранён")
	// 4 insert + 3 update (UpdateOne + UpdateMany на 2) + replace + delete; 3 insert + 2 update + delete
	assert.EqualValues(t, 15, rn.r.Applied())
}

func TestIntegrationTransaction(t *testing.T) {
	db := prepare(t)
	rn := start(t, Config{StartTS: srcNow(t), BatchSize: 1000})

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

	n, err := dstClient.Database(db).Collection("accounts").CountDocuments(ctx, bson.D{})
	require.NoError(t, err)
	assert.EqualValues(t, 2, n)
	n, err = dstClient.Database(db).Collection("ledger").CountDocuments(ctx, bson.D{})
	require.NoError(t, err)
	assert.EqualValues(t, 2, n)

	rn.waitQuiet()
	// 3 insert до транзакции + insert, update, delete, update, insert в транзакции
	assert.EqualValues(t, 8, rn.r.Applied())
}

func TestIntegrationResume(t *testing.T) {
	db := prepare(t)
	rn := start(t, Config{StartTS: srcNow(t), BatchSize: 1000})

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

	// без StartTS: успех возможен только при старте с сохранённого токена
	rn2 := start(t, Config{BatchSize: 1000})
	rn2.waitSame(db)
	rn2.waitQuiet()
	// ровно n: события до остановки повторно не применялись
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
	rn := start(t, Config{StartTS: startTS, BatchSize: 1000})

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

	// сброс чекпойнта: поток перечитывается с того же StartTS поверх готовой цели
	require.NoError(t, dstClient.Database(ckptDB).Collection(ckptColl).Drop(ctx))
	rn2 := start(t, Config{StartTS: startTS, BatchSize: 1000})

	// цель уже совпадает, поэтому сначала ждём, что повтор действительно прошёл:
	// insert X, delete X, insert Y, update Y (update X пропущен)
	const m = 4
	require.Eventually(t, func() bool { return rn2.r.Applied() >= m }, waitTimeout, pollEvery)
	rn2.waitSame(db)
	rn2.waitQuiet()
	assert.EqualValues(t, m, rn2.r.Applied())
}

func TestIntegrationNoStartPoint(t *testing.T) {
	prepare(t)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	err := New(srcClient, dstClient, Config{BatchSize: 1000}).Run(ctx)
	require.ErrorIs(t, err, ErrNoStartPoint)
}

// renameColl переименовывает коллекцию на источнике с dropTarget, как mongosh renameCollection(to, true).
func renameColl(ctx context.Context, t *testing.T, db, from, to string) {
	t.Helper()
	require.NoError(t, srcClient.Database("admin").RunCommand(ctx, bson.D{
		{Key: "renameCollection", Value: db + "." + from},
		{Key: "to", Value: db + "." + to},
		{Key: "dropTarget", Value: true},
	}).Err())
}

func indexNames(ctx context.Context, t *testing.T, c *mongo.Client, db, coll string) []string {
	t.Helper()
	names, err := c.Database(db).Collection(coll).Indexes().ListSpecifications(ctx)
	require.NoError(t, err)
	out := make([]string, 0, len(names))
	for _, s := range names {
		out = append(out, s.Name)
	}
	slices.Sort(out)
	return out
}

func dbExists(ctx context.Context, t *testing.T, c *mongo.Client, db string) bool {
	t.Helper()
	names, err := c.ListDatabaseNames(ctx, bson.M{"name": db})
	require.NoError(t, err)
	return len(names) > 0
}

func TestIntegrationDDL(t *testing.T) {
	db := prepare(t)
	other := db + "_other" // для dropDatabase
	cleanDB(t, other)
	rn := start(t, Config{StartTS: srcNow(t), BatchSize: 1000})

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
		"индексы 8.0 приняты целью 5.0")

	renameColl(ctx, t, db, "a", "b")

	_, err = b.InsertOne(ctx, bson.D{{Key: "_id", Value: int32(10)}, {Key: "code", Value: "c10"}, {Key: "status", Value: "idle"}})
	require.NoError(t, err)
	_, err = b.UpdateOne(ctx, bson.M{"_id": 0}, bson.D{{Key: "$inc", Value: bson.D{{Key: "n", Value: int32(10)}}}})
	require.NoError(t, err)
	_, err = b.DeleteOne(ctx, bson.M{"_id": 2})
	require.NoError(t, err)

	// неявный create + drop
	_, err = src.Collection("tmp").InsertOne(ctx, bson.D{{Key: "_id", Value: int32(1)}})
	require.NoError(t, err)
	require.NoError(t, src.Collection("tmp").Drop(ctx))

	// dropDatabase отдельной базы: сначала убеждаемся, что она доехала до цели
	for _, coll := range []string{"x", "y"} {
		_, err = srcClient.Database(other).Collection(coll).InsertOne(ctx, bson.D{{Key: "_id", Value: coll}})
		require.NoError(t, err)
	}
	rn.waitSame(other)
	require.True(t, dbExists(ctx, t, dstClient, other))
	require.NoError(t, srcClient.Database(other).Drop(ctx))

	rn.waitSame(db)
	require.Eventually(t, func() bool { return !dbExists(ctx, t, dstClient, other) }, waitTimeout, pollEvery,
		"база %s не удалена на цели", other)

	colls, err := dstClient.Database(db).ListCollectionNames(ctx, bson.D{})
	require.NoError(t, err)
	assert.Equal(t, []string{"b"}, colls)
	assert.Equal(t, []string{"_id_", "a_code_u", "a_k_n", "a_status_p", "a_ttl"}, indexNames(ctx, t, dstClient, db, "b"),
		"индексы b заменены индексами a")
	n, err := dstClient.Database(db).Collection("b").CountDocuments(ctx, bson.D{})
	require.NoError(t, err)
	assert.EqualValues(t, 4, n, "0, 1, 3 из a и 10; документы старой b удалены dropTarget")
}

func TestIntegrationUnsupportedDDL(t *testing.T) {
	db := prepare(t)
	rn := start(t, Config{StartTS: srcNow(t), BatchSize: 1000})

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

	rn.waitSame(db)
	rn.waitQuiet()
	assert.EqualValues(t, 3, rn.r.Applied())

	// событие modify пропущено: на цели валидатора нет
	specs, err := dstClient.Database(db).ListCollectionSpecifications(ctx, bson.M{"name": "c"})
	require.NoError(t, err)
	require.Len(t, specs, 1)
	_, err = specs[0].Options.LookupErr("validator")
	assert.Error(t, err, "collMod не должен переноситься: %s", specs[0].Options)
}

// Smoke: рестарт с чекпойнта, сохранённого после rename, не ломает состояние.
// Окно между applyDDL и saveToken отсюда не поймать.
func TestIntegrationRestartAfterDDL(t *testing.T) {
	db := prepare(t)
	rn := start(t, Config{StartTS: srcNow(t), BatchSize: 1000})

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

	rn2 := start(t, Config{BatchSize: 1000})
	rn2.waitSame(db)
	rn2.waitQuiet()
	assert.EqualValues(t, n, rn2.r.Applied())

	colls, err := dstClient.Database(db).ListCollectionNames(ctx, bson.D{})
	require.NoError(t, err)
	slices.Sort(colls)
	assert.Equal(t, []string{"a", "b"}, colls)
}

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

	rn := start(t, Config{StartTS: startTS, BatchSize: 1000})
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

	rn := start(t, Config{StartTS: startTS, BatchSize: 1000})
	rn.waitQuiet()
	ok, diff = same(db)
	assert.True(t, ok, "документы 0..2 в b уничтожены повтором rename: %s", diff)
}
