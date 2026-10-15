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

	"github.com/rs/zerolog"
	zlog "github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

const (
	waitTimeout = 30 * time.Second
	pollEvery   = 200 * time.Millisecond
	// больше maxAwaitTime потока (1 с): за это время Run успевает сделать
	// хотя бы один пустой getMore и сохранить чекпойнт
	quietInterval = 1600 * time.Millisecond
	// аренда в тестах: истечение ждём секунды, а не минуту
	itLeaseTTL   = 2 * time.Second
	itLeaseRenew = 300 * time.Millisecond
)

var (
	srcClient, dstClient *mongo.Client
	dstURI               string
	// секунда последней операции на источнике, сделанной обвязкой предыдущего
	// теста (или prepare текущего); srcNow ждёт, пока время уйдёт дальше
	lastSrcSec uint32
)

func TestMain(m *testing.M) {
	// свои переменные, а не SRC_URI/DST_URI: тесты удаляют чекпойнт и базы,
	// и шелл с загруженным .env не должен направить их на рабочие кластеры
	srcURI := os.Getenv("IT_SRC_URI")
	dstURI = os.Getenv("IT_DST_URI")
	if srcURI == "" || dstURI == "" {
		log.Fatal("IT_SRC_URI и IT_DST_URI не заданы: интеграционные тесты запускаются через make test-integration")
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

// newIT — New с короткой арендой.
func newIT(dst *mongo.Client, cfg Config) *Replicator {
	r := New(srcClient, dst, cfg)
	r.leaseTTL, r.leaseRenew = itLeaseTTL, itLeaseRenew
	return r
}

// release снимает аренду, как defer в main.
func release(t *testing.T, r *Replicator) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	assert.NoError(t, r.Release(ctx))
}

// runner — репликатор, запущенный в горутине.
type runner struct {
	t        *testing.T
	r        *Replicator
	stop     func()
	done     chan struct{}
	err      error // читать только после закрытия done
	selfExit bool  // Run вернулся сам (waitExit): stop не ждёт отмены
}

// start запускает Run нового репликатора в горутине и регистрирует остановку
// в t.Cleanup: упавший require не оставит репликатор работать в следующих тестах.
func start(t *testing.T, cfg Config) *runner {
	t.Helper()
	return startOn(t, newIT(dstClient, cfg))
}

// startOn — как start, но для уже созданного репликатора (повторный Run, как в ретраях main).
func startOn(t *testing.T, r *Replicator) *runner {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	rn := &runner{t: t, r: r, done: make(chan struct{})}
	go func() {
		defer close(rn.done)
		rn.err = rn.r.Run(ctx)
	}()

	var once sync.Once
	rn.stop = func() {
		once.Do(func() {
			cancel()
			<-rn.done
			if !rn.selfExit {
				assert.ErrorIs(t, rn.err, context.Canceled, "Run должен завершиться только отменой контекста")
			}
			release(t, rn.r)
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

// waitExit ждёт, пока Run вернётся сам, и возвращает его ошибку.
func (rn *runner) waitExit() error {
	rn.t.Helper()
	select {
	case <-rn.done:
	case <-time.After(waitTimeout):
		rn.t.Fatalf("Run не вернулся за %s", waitTimeout)
	}
	rn.selfExit = true
	return rn.err
}

// waitSame ждёт совпадения одноимённой базы на кластерах.
func (rn *runner) waitSame(db string) {
	rn.t.Helper()
	rn.waitSameDBs(db, db)
}

// waitSameDBs ждёт совпадения базы srcDB источника и dstDB цели; если Run уже вернулся — падает сразу.
func (rn *runner) waitSameDBs(srcDB, dstDB string) {
	rn.t.Helper()
	deadline := time.Now().Add(waitTimeout)
	var diff string
	for {
		if done, err := rn.exited(); done {
			rn.t.Fatalf("Run вернулся раньше времени: %v", err)
		}
		var ok bool
		if ok, diff = sameDBs(srcDB, dstDB); ok {
			return
		}
		if time.Now().After(deadline) {
			rn.t.Fatalf("база %s → %s не совпала за %s: %s", srcDB, dstDB, waitTimeout, diff)
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
	ck, err := rn.r.loadCheckpoint(ctx) // читает только dst: безопасно во время Run
	require.NoError(rn.t, err)
	return ck.Token
}

// ckptDB — пара баз, для которой записан чекпойнт.
func (rn *runner) ckptDB() dbPair {
	rn.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ck, err := rn.r.loadCheckpoint(ctx)
	require.NoError(rn.t, err)
	return ck.dbs()
}

// ckptTime — clusterTime чекпойнта: время применённой позиции.
func (rn *runner) ckptTime() bson.Timestamp {
	rn.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ck, err := rn.r.loadCheckpoint(ctx)
	require.NoError(rn.t, err)
	return ck.ClusterTime
}

// same сравнивает одноимённую базу на источнике и цели.
func same(db string) (ok bool, diff string) { return sameDBs(db, db) }

// sameDBs сравнивает базу srcDB источника и dstDB цели: списки коллекций, документы по _id
// байт в байт и индексы по name/key/unique/partialFilterExpression/expireAfterSeconds.
func sameDBs(srcDB, dstDB string) (ok bool, diff string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	srcColls, err := collNames(ctx, srcClient, srcDB)
	if err != nil {
		return false, "источник: " + err.Error()
	}
	dstColls, err := collNames(ctx, dstClient, dstDB)
	if err != nil {
		return false, "цель: " + err.Error()
	}
	if !slices.Equal(srcColls, dstColls) {
		return false, fmt.Sprintf("коллекции: источник %v, цель %v", srcColls, dstColls)
	}

	for _, coll := range srcColls {
		if d := diffDocs(ctx, srcDB, dstDB, coll); d != "" {
			return false, d
		}
		if d := diffIndexes(ctx, srcDB, dstDB, coll); d != "" {
			return false, d
		}
	}
	return true, ""
}

// collNames — обычные коллекции базы: views и system.* не реплицируются.
func collNames(ctx context.Context, c *mongo.Client, db string) ([]string, error) {
	names, err := c.Database(db).ListCollectionNames(ctx, bson.M{
		"type": "collection",
		"name": bson.M{"$not": bson.Regex{Pattern: `^system\.`}},
	})
	slices.Sort(names)
	return names, err
}

// pairNS — коллекция для сообщений сравнения: ns источника и, если база цели другая, ns цели.
func pairNS(srcDB, dstDB, coll string) string {
	if dstDB == srcDB {
		return srcDB + "." + coll
	}
	return srcDB + "." + coll + " → " + dstDB + "." + coll
}

func diffDocs(ctx context.Context, srcDB, dstDB, coll string) string {
	ns := pairNS(srcDB, dstDB, coll)
	read := func(c *mongo.Client, db string) ([]bson.Raw, error) {
		cur, err := c.Database(db).Collection(coll).Find(ctx, bson.D{}, options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}))
		if err != nil {
			return nil, err
		}
		var docs []bson.Raw
		err = cur.All(ctx, &docs)
		return docs, err
	}
	srcDocs, err := read(srcClient, srcDB)
	if err != nil {
		return fmt.Sprintf("%s источник: %v", ns, err)
	}
	dstDocs, err := read(dstClient, dstDB)
	if err != nil {
		return fmt.Sprintf("%s цель: %v", ns, err)
	}
	for i := range max(len(srcDocs), len(dstDocs)) {
		switch {
		case i >= len(srcDocs):
			return fmt.Sprintf("%s: лишний документ на цели %s", ns, dstDocs[i])
		case i >= len(dstDocs):
			return fmt.Sprintf("%s: нет документа на цели %s", ns, srcDocs[i])
		case !bytes.Equal(srcDocs[i], dstDocs[i]):
			return fmt.Sprintf("%s: документы различаются\n источник %s\n цель     %s", ns, srcDocs[i], dstDocs[i])
		}
	}
	return ""
}

func diffIndexes(ctx context.Context, srcDB, dstDB, coll string) string {
	ns := pairNS(srcDB, dstDB, coll)
	read := func(c *mongo.Client, db string) ([]string, error) {
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
	srcIdx, err := read(srcClient, srcDB)
	if err != nil {
		return fmt.Sprintf("%s индексы источника: %v", ns, err)
	}
	dstIdx, err := read(dstClient, dstDB)
	if err != nil {
		return fmt.Sprintf("%s индексы цели: %v", ns, err)
	}
	if !slices.Equal(srcIdx, dstIdx) {
		return fmt.Sprintf("%s: индексы различаются\n источник %v\n цель     %v", ns, srcIdx, dstIdx)
	}
	return ""
}

// indexSig — значимые для сравнения поля спецификации индекса. Версия (v)
// и прочие служебные поля между версиями источника и цели могут отличаться и не сравниваются.
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
		db := prepare(t)
		rn = start(t, Config{StartTS: srcNow(t), BatchSize: 1000, DB: db})
	})
	require.NotNil(t, rn)
	done, err := rn.exited()
	require.True(t, done, "репликатор продолжает работать после завершения подтеста")
	require.ErrorIs(t, err, context.Canceled)
}

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
	_, err = dstClient.Database(ckptDB).Collection(ckptColl).DeleteOne(ctx, bson.M{"_id": db})
	require.NoError(t, err)
	rn2 := start(t, Config{StartTS: startTS, BatchSize: 1000, DB: db, ApplyDrops: []string{db + ".gone"}})

	// цель уже совпадает, поэтому сначала ждём, что повтор действительно прошёл:
	// insert X, delete X, insert Y, update Y (update X пропущен)
	const m = 4
	require.Eventually(t, func() bool { return rn2.r.Applied() >= m }, waitTimeout, pollEvery)
	rn2.waitSame(db)
	rn2.waitQuiet()
	assert.EqualValues(t, m, rn2.r.Applied())
}

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

// runErr выполняет Run нового репликатора синхронно и снимает аренду, как main:
// ожидается ошибка от цели, а не таймаут.
func runErr(t *testing.T, cfg Config) (*Replicator, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	r := newIT(dstClient, cfg)
	err := r.Run(ctx)
	require.NoError(t, ctx.Err(), "Run должен вернуть ошибку цели до таймаута")
	release(t, r)
	return r, err
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

// renameColl переименовывает коллекцию на источнике с dropTarget, как mongosh renameCollection(to, true).
func renameColl(ctx context.Context, t *testing.T, db, from, to string) {
	t.Helper()
	renameNS(ctx, t, db+"."+from, db+"."+to)
}

// renameNS — renameCollection по полным namespace, в том числе между базами.
func renameNS(ctx context.Context, t *testing.T, from, to string) {
	t.Helper()
	require.NoError(t, srcClient.Database("admin").RunCommand(ctx, bson.D{
		{Key: "renameCollection", Value: from},
		{Key: "to", Value: to},
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

	colls, err := dstClient.Database(db).ListCollectionNames(ctx, bson.D{})
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
	assert.EqualValues(t, 2, n)
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
	assert.Equal(t, dbPair{src: db, dst: dstDB}, rn.ckptDB())
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
	assert.Equal(t, dbPair{src: db, dst: db}, rn.ckptDB())

	const n = 2
	for i := 3; i < 3+n; i++ {
		_, err := items.InsertOne(ctx, bson.D{{Key: "_id", Value: int32(i)}})
		require.NoError(t, err)
	}

	// без StartTS: продолжить можно только с чекпойнта
	rn2 := start(t, Config{BatchSize: 1000, DB: db})
	rn2.waitSame(db)
	rn2.waitQuiet()
	assert.EqualValues(t, n, rn2.r.Applied())
	assert.Equal(t, dbPair{src: db, dst: db}, rn2.ckptDB())
}

// writeCkpt записывает на цель чекпойнт базы цели pair.dst с токеном-заглушкой.
func writeCkpt(t *testing.T, pair dbPair) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	_, err := dstClient.Database(ckptDB).Collection(ckptColl).ReplaceOne(ctx,
		bson.M{"_id": pair.dst},
		bson.M{"_id": pair.dst, "token": bson.M{"_data": "00"}, "db": pair.src, "dst_db": pair.dst},
		options.Replace().SetUpsert(true))
	require.NoError(t, err)
}

// Чекпойнт базы цели записан для другой базы источника: Run отказывается стартовать.
// Токен произвольный: проверка идёт до открытия потока.
func TestIntegrationDBMismatch(t *testing.T) {
	prepare(t)
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
	prepare(t)
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

// Два процесса разных баз пишут в один целевой кластер: у каждого свой чекпойнт,
// обе базы сходятся независимо.
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
	assert.Equal(t, dbPair{src: dbA, dst: dbA}, rnA.ckptDB())
	assert.Equal(t, dbPair{src: dbB, dst: dbB}, rnB.ckptDB())

	cur, err := dstClient.Database(ckptDB).Collection(ckptColl).Find(ctx, bson.D{},
		options.Find().SetProjection(bson.M{"_id": 1}).SetSort(bson.M{"_id": 1}))
	require.NoError(t, err)
	var docs []struct {
		ID string `bson:"_id"`
	}
	require.NoError(t, cur.All(ctx, &docs))
	ids := make([]string, 0, len(docs))
	for _, d := range docs {
		ids = append(ids, d.ID)
	}
	assert.Equal(t, []string{dbA, dbB}, ids, "по документу чекпойнта на базу цели")
}

// leaseState — аренда и позиция из документа чекпойнта.
type leaseState struct {
	Owner      string    `bson:"owner"`
	LeaseUntil time.Time `bson:"lease_until"`
	Token      bson.Raw  `bson:"token"`
}

// readLease читает документ чекпойнта базы цели dstDB; false — документа нет.
func readLease(t *testing.T, dstDB string) (leaseState, bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var st leaseState
	err := dstClient.Database(ckptDB).Collection(ckptColl).FindOne(ctx, bson.M{"_id": dstDB}).Decode(&st)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return leaseState{}, false
	}
	require.NoError(t, err)
	return st, true
}

// Второй процесс на ту же базу цели не стартует, пока первый работает; после
// остановки первого (со снятием аренды) — стартует сразу, не дожидаясь её истечения.
func TestIntegrationLeaseHeld(t *testing.T) {
	db := prepare(t)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()

	rnA := start(t, Config{StartTS: srcNow(t), BatchSize: 1000, DB: db})
	items := srcClient.Database(db).Collection("items")
	_, err := items.InsertOne(ctx, bson.D{{Key: "_id", Value: int32(1)}})
	require.NoError(t, err)
	rnA.waitSame(db)
	rnA.waitQuiet()

	_, err = runErr(t, Config{BatchSize: 1000, DB: db})
	require.ErrorIs(t, err, ErrLeaseHeld)
	assert.ErrorContains(t, err, rnA.r.owner)
	st, _ := readLease(t, db)
	assert.Equal(t, rnA.r.owner, st.Owner, "неудачный захват и Release второго не трогают аренду первого")

	rnA.stop()
	st, found := readLease(t, db)
	require.True(t, found, "документ с позицией остаётся")
	assert.Empty(t, st.Owner, "аренда снята")
	assert.True(t, st.LeaseUntil.IsZero())
	assert.NotNil(t, st.Token)

	// без StartTS: B продолжает с чекпойнта A
	rnB := start(t, Config{BatchSize: 1000, DB: db})
	_, err = items.InsertOne(ctx, bson.D{{Key: "_id", Value: int32(2)}})
	require.NoError(t, err)
	rnB.waitSame(db)
	st, _ = readLease(t, db)
	assert.Equal(t, rnB.r.owner, st.Owner)
}

// Документ с чужой истёкшей арендой (процесс упал, не сняв её): захват проходит.
func TestIntegrationLeaseExpired(t *testing.T) {
	db := prepare(t)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	_, err := dstClient.Database(ckptDB).Collection(ckptColl).InsertOne(ctx, bson.M{
		"_id": db, "owner": "crashed:1:deadbeef", "lease_until": time.Now().Add(-time.Minute),
	})
	require.NoError(t, err)

	rn := start(t, Config{StartTS: srcNow(t), BatchSize: 1000, DB: db})
	_, err = srcClient.Database(db).Collection("items").InsertOne(ctx, bson.D{{Key: "_id", Value: int32(1)}})
	require.NoError(t, err)
	rn.waitSame(db)
	st, _ := readLease(t, db)
	assert.Equal(t, rn.r.owner, st.Owner)
}

// Аренду забрали во время работы: Run завершается с ErrLeaseLost, а Release
// не снимает чужую аренду.
func TestIntegrationLeaseLost(t *testing.T) {
	db := prepare(t)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()

	rn := start(t, Config{StartTS: srcNow(t), BatchSize: 1000, DB: db})
	require.Eventually(t, func() bool {
		st, _ := readLease(t, db)
		return st.Owner == rn.r.owner
	}, waitTimeout, pollEvery)

	const intruder = "intruder:1:00000000"
	_, err := dstClient.Database(ckptDB).Collection(ckptColl).UpdateOne(ctx,
		bson.M{"_id": db}, bson.M{"$set": bson.M{"owner": intruder}})
	require.NoError(t, err)

	require.ErrorIs(t, rn.waitExit(), ErrLeaseLost)
	rn.stop()
	st, _ := readLease(t, db)
	assert.Equal(t, intruder, st.Owner)
}

// Повторный Run того же репликатора после временной ошибки захватывает свою же
// аренду, а другой процесс между запусками её не получает.
func TestIntegrationLeaseRerun(t *testing.T) {
	db := prepare(t)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	// токен-заглушка: Run захватывает аренду и падает на открытии потока
	writeCkpt(t, dbPair{src: db, dst: db})

	r := newIT(dstClient, Config{BatchSize: 1000, DB: db})
	r.leaseTTL = time.Minute // не истечёт между запусками
	t.Cleanup(func() { release(t, r) })
	require.ErrorContains(t, r.Run(ctx), "открытие change stream")
	st, _ := readLease(t, db)
	assert.Equal(t, r.owner, st.Owner, "аренда остаётся за репликатором и после возврата Run")

	_, err := runErr(t, Config{BatchSize: 1000, DB: db})
	require.ErrorIs(t, err, ErrLeaseHeld)

	require.ErrorContains(t, r.Run(ctx), "открытие change stream", "своя аренда захватывается снова")
}

// Аренда продлевается, пока основной цикл стоит в долгом DDL (createIndexes с
// w:majority ждёт сборки индекса): другой процесс её не получает и через срок аренды.
func TestIntegrationLeaseRenewDuringLongDDL(t *testing.T) {
	db := prepare(t)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()

	r := newIT(dstClient, Config{StartTS: srcNow(t), BatchSize: 1000, DB: db})
	inDDL := make(chan struct{})
	var once sync.Once
	r.ddlHook = func() {
		once.Do(func() {
			close(inDDL)
			time.Sleep(3 * itLeaseTTL)
		})
	}
	rn := startOn(t, r)
	require.NoError(t, srcClient.Database(db).CreateCollection(ctx, "c"))
	select {
	case <-inDDL:
	case <-ctx.Done():
		t.Fatal("DDL не дошёл до применения")
	}

	before, _ := readLease(t, db)
	time.Sleep(itLeaseTTL + itLeaseRenew) // без продления аренда уже истекла бы
	after, _ := readLease(t, db)
	assert.Equal(t, r.owner, after.Owner)
	assert.True(t, after.LeaseUntil.After(before.LeaseUntil), "аренда продлевается: %v → %v",
		before.LeaseUntil, after.LeaseUntil)

	_, err := runErr(t, Config{BatchSize: 1000, DB: db})
	require.ErrorIs(t, err, ErrLeaseHeld)

	// DDL завершился, репликация продолжается
	_, err = srcClient.Database(db).Collection("c").InsertOne(ctx, bson.D{{Key: "_id", Value: int32(1)}})
	require.NoError(t, err)
	rn.waitSame(db)
}

// Токен не разобран: clusterTime чекпойнта — время последнего события.
func TestIntegrationSaveTokenUnparsedTime(t *testing.T) {
	db := prepare(t)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()

	r := newIT(dstClient, Config{BatchSize: 1000, DB: db})
	require.NoError(t, r.acquireLease(ctx), "saveToken пишет только под арендой")
	r.lastTS = bson.Timestamp{T: 1000, I: 3}
	token, err := bson.Marshal(bson.M{"_data": "zz"})
	require.NoError(t, err)
	require.NoError(t, r.saveToken(ctx, token))

	assert.Equal(t, r.lastTS, r.appliedTS)
	rn := &runner{t: t, r: r}
	assert.Equal(t, r.lastTS, rn.ckptTime())
	assert.Equal(t, dbPair{src: db, dst: db}, rn.ckptDB())

	// рестарт: событий ещё нет, lastTS пуст — позиция берётся из чекпойнта
	// и пустой getMore с неразобранным токеном её не обнуляет
	// Run берёт позицию из чекпойнта до открытия потока; сам поток с токеном-заглушкой не откроется
	require.NoError(t, r.Release(ctx))
	r2 := newIT(dstClient, Config{BatchSize: 1000, DB: db})
	t.Cleanup(func() { release(t, r2) })
	require.ErrorContains(t, r2.Run(ctx), "открытие change stream")
	require.Equal(t, r.lastTS, r2.appliedTS)
	token2, err := bson.Marshal(bson.M{"_data": "yy"})
	require.NoError(t, err)
	require.NoError(t, r2.saveToken(ctx, token2))
	assert.Equal(t, r.lastTS, r2.appliedTS)
	assert.Equal(t, r.lastTS, (&runner{t: t, r: r2}).ckptTime())
}

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

// captureLog пишет лог пакета в буфер до конца теста; читать буфер — после остановки репликатора.
func captureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := zlog.Logger
	zlog.Logger = zerolog.New(&buf)
	t.Cleanup(func() { zlog.Logger = prev })
	return &buf
}

// insertAt вставляет документ на источнике и возвращает operationTime записи.
func insertAt(ctx context.Context, t *testing.T, c *mongo.Collection, id any) bson.Timestamp {
	t.Helper()
	sess, err := srcClient.StartSession()
	require.NoError(t, err)
	defer sess.EndSession(ctx)
	_, err = c.InsertOne(mongo.NewSessionContext(ctx, sess), bson.D{{Key: "_id", Value: id}})
	require.NoError(t, err)
	return *sess.OperationTime()
}

// Применённая позиция (clusterTime чекпойнта) — время токена, а не последнего
// события: на записях только в другие базы она двигается по post-batch token.
func TestIntegrationLagPosition(t *testing.T) {
	db := prepare(t)
	other := db + "_other"
	cleanDB(t, other)
	rn := start(t, Config{StartTS: srcNow(t), BatchSize: 1000, DB: db})

	ctx, cancel := context.WithTimeout(context.Background(), 2*waitTimeout)
	defer cancel()

	own := insertAt(ctx, t, srcClient.Database(db).Collection("items"), int32(1))
	rn.waitSame(db)
	rn.waitQuiet()
	assert.False(t, rn.ckptTime().Before(own), "чекпойнт %v позади записи %v", rn.ckptTime(), own)
	n := rn.r.Applied()

	// время источника для лага: ping с правами на одну базу
	now, err := rn.r.srcTime(ctx)
	require.NoError(t, err)
	assert.False(t, now.Before(own), "время источника %v позади записи %v", now, own)

	foreign := insertAt(ctx, t, srcClient.Database(other).Collection("c"), int32(1))
	require.Eventually(t, func() bool { return !rn.ckptTime().Before(foreign) }, waitTimeout, pollEvery,
		"применённая позиция не дошла до записи в другую базу %v", foreign)
	assert.Equal(t, n, rn.r.Applied(), "событие другой базы применено")
}

// При ровном потоке чаще maxAwaitTime (1 с) пустых getMore нет: пачка должна
// писаться по вычитанному ответу сервера, а не ждать BatchSize событий.
func TestIntegrationSteadyTrafficBoundedLag(t *testing.T) {
	db := prepare(t)
	rn := start(t, Config{StartTS: srcNow(t), BatchSize: 1000, DB: db})

	ctx, cancel := context.WithTimeout(context.Background(), 2*waitTimeout)
	defer cancel()

	items := srcClient.Database(db).Collection("items")
	stop, done := make(chan struct{}), make(chan struct{})
	var writeErr error
	go func() {
		defer close(done)
		for i := int32(0); ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := items.InsertOne(ctx, bson.D{{Key: "_id", Value: i}}); err != nil {
				writeErr = err
				return
			}
			time.Sleep(200 * time.Millisecond)
		}
	}()
	stopWrites := func() {
		select {
		case <-stop:
		default:
			close(stop)
		}
		<-done
	}
	defer stopWrites()

	// записи идут каждые 200 мс и не прекращаются: цель обязана догонять на ходу
	dstItems := dstClient.Database(db).Collection("items")
	require.Eventually(t, func() bool {
		n, err := dstItems.CountDocuments(ctx, bson.D{})
		return err == nil && n >= 5
	}, 5*time.Second, pollEvery, "при непрерывном потоке цель не получила ни одной пачки")

	stopWrites()
	require.NoError(t, writeErr)
	rn.waitSame(db)
}

// update большого документа: fullDocument (~9 МБ) + updateDescription (~8 МБ)
// превысили бы 16 МБ; поток убирает updateDescription, и событие проходит.
func TestIntegrationLargeUpdateEvent(t *testing.T) {
	db := prepare(t)
	rn := start(t, Config{StartTS: srcNow(t), BatchSize: 1000, DB: db})

	ctx, cancel := context.WithTimeout(context.Background(), 2*waitTimeout)
	defer cancel()

	big := srcClient.Database(db).Collection("big")
	_, err := big.InsertOne(ctx, bson.D{
		{Key: "_id", Value: int32(1)},
		{Key: "a", Value: strings.Repeat("a", 1<<20)},
		{Key: "b", Value: strings.Repeat("b", 8<<20)},
	})
	require.NoError(t, err)
	_, err = big.UpdateOne(ctx, bson.M{"_id": int32(1)},
		bson.D{{Key: "$set", Value: bson.D{{Key: "b", Value: strings.Repeat("c", 8<<20)}}}})
	require.NoError(t, err)

	rn.waitSame(db)
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
	assert.EqualValues(t, 3, n)
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
