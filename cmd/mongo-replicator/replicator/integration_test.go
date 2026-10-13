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
	dropDB(t, db) // остатки упавших прогонов: контейнеры живут между запусками
	t.Cleanup(func() { dropDB(t, db) })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, dstClient.Database(ckptDB).Collection(ckptColl).Drop(ctx))
	return db
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
