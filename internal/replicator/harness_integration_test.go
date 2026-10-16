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

	toxiproxy "github.com/Shopify/toxiproxy/v2/client"
	"github.com/rs/zerolog"
	zlog "github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"

	"github.com/sergey-frv/mongo-replicator/internal/statefile"
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
	// запись файла состояния: свежесть в сводке проверяется за секунды
	itStateEvery = 200 * time.Millisecond
)

var (
	srcClient, dstClient *mongo.Client
	srcURI, dstURI       string
	// сетевые тесты: прокси Toxiproxy и URI кластеров через них; без IT_PROXY_API — nil
	srcProxy, dstProxy       *toxiproxy.Proxy
	srcProxyURI, dstProxyURI string
	// секунда последней операции на источнике, сделанной обвязкой предыдущего
	// теста (или prepare текущего); srcNow ждёт, пока время уйдёт дальше
	lastSrcSec uint32
)

func TestMain(m *testing.M) {
	// свои переменные, а не SRC_URI/DST_URI: тесты удаляют чекпойнт и базы,
	// и шелл с загруженным .env не должен направить их на рабочие кластеры
	srcURI = os.Getenv("IT_SRC_URI")
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
	if api := os.Getenv("IT_PROXY_API"); api != "" {
		srcProxyURI, dstProxyURI = os.Getenv("IT_SRC_PROXY_URI"), os.Getenv("IT_DST_PROXY_URI")
		if srcProxyURI == "" || dstProxyURI == "" {
			log.Fatal("IT_PROXY_API задан без IT_SRC_PROXY_URI и IT_DST_PROXY_URI")
		}
		if srcProxy, dstProxy, err = populateProxies(api); err != nil {
			log.Fatalf("toxiproxy: %v", err)
		}
	}

	code := m.Run()
	_ = srcClient.Disconnect(context.Background())
	_ = dstClient.Disconnect(context.Background())
	os.Exit(code)
}

// populateProxies создаёт (пересоздаёт существующие) прокси src и dst. У образа
// нет healthcheck: API может ещё не слушать — повторяем до готовности.
func populateProxies(api string) (src, dst *toxiproxy.Proxy, err error) {
	cl := toxiproxy.NewClient(api)
	cfg := []toxiproxy.Proxy{
		{Name: "src", Listen: "0.0.0.0:27001", Upstream: "mongo-src:27017", Enabled: true},
		{Name: "dst", Listen: "0.0.0.0:27002", Upstream: "mongo-dst:27017", Enabled: true},
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		_, err = cl.Populate(cfg)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			return nil, nil, err
		}
		time.Sleep(200 * time.Millisecond) // повтор до готовности API Toxiproxy: из TestMain, t для Eventually нет
	}
	// прокси из Populate не помечены созданными: их Enable/Disable пытаются создать
	// прокси заново (409), поэтому берём их через Proxy
	if src, err = cl.Proxy("src"); err != nil {
		return nil, nil, err
	}
	dst, err = cl.Proxy("dst")
	return src, dst, err
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
	cases := []struct{ name, in, want string }{
		{"аббревиатура в конце", "TestIntegrationDML", "integration_dml"},
		{"аббревиатура перед словом", "TestIntegrationDDLRestart", "integration_ddl_restart"},
		{"подтест", "TestIntegrationStopOnCleanup/sub", "integration_stop_on_cleanup_sub"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := snake(c.in)

			assert.Equal(t, c.want, got, "имя базы для %s", c.in)
		})
	}
}

// srcOpTime пишет no-op в oplog источника и возвращает его время:
// на простаивающем кластере operationTime иначе двигается раз в 10 с.
func srcOpTime(t *testing.T) bson.Timestamp {
	t.Helper()
	ts, err := appendSrcNote(t.Name())
	require.NoError(t, err)
	return ts
}

// appendSrcNote — srcOpTime без t, для условий Eventually: no-op с меткой name.
func appendSrcNote(name string) (bson.Timestamp, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	raw, err := srcClient.Database("admin").RunCommand(ctx, bson.D{
		{Key: "appendOplogNote", Value: 1},
		{Key: "data", Value: bson.D{{Key: "it", Value: name}}},
	}).Raw()
	if err != nil {
		return bson.Timestamp{}, err
	}
	ts, i := raw.Lookup("operationTime").Timestamp()
	return bson.Timestamp{T: ts, I: i}, nil
}

// srcNow возвращает StartTS для теста. StartTS округлён до секунды, а
// startAtOperationTime включительный, поэтому ждём, пока время источника
// уйдёт за секунду последней операции обвязки: иначе поток захватит хвост
// предыдущего теста, включая его cleanup-dropDatabase.
func srcNow(t *testing.T) uint32 {
	t.Helper()
	name, last := t.Name(), lastSrcSec // условие идёт в своей горутине: глобальное копируем до вызова
	var ts uint32
	var noteErr error
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		op, err := appendSrcNote(name)
		if err != nil {
			noteErr = err // неожиданная ошибка прекращает ожидание и проверяется после него
			return
		}
		ts = op.T
		assert.Greater(c, ts, last, "время источника не двигается: %d", ts)
	}, 5*time.Second, 100*time.Millisecond, "время источника не ушло за секунду последней операции обвязки")
	require.NoError(t, noteErr, "no-op в oplog источника")
	return ts
}

func dropDB(t *testing.T, db string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, srcClient.Database(db).Drop(ctx))
	require.NoError(t, dstClient.Database(db).Drop(ctx))
	lastSrcSec = srcOpTime(t).T
}

// prepare выдаёт тесту чистую базу на обоих кластерах; чекпойнт лежит в базе цели
// и уходит вместе с ней. Вызывать до start: cleanup выполняются в обратном порядке,
// и база дропается уже после остановки репликатора.
func prepare(t *testing.T) (db string) {
	t.Helper()
	db = "it_" + snake(t.Name())
	cleanDB(t, db)
	return db
}

// cleanDB дропает базу на обоих кластерах (на цели — вместе с чекпойнтом) сейчас
// и в cleanup теста. Как и prepare, вызывать до start.
func cleanDB(t *testing.T, db string) {
	t.Helper()
	dropDB(t, db) // остатки упавших прогонов: контейнеры живут между запусками
	t.Cleanup(func() { dropDB(t, db) })
}

// cleanDBs — cleanDB для баз с фиксированными именами (тесты несовпадения пары баз):
// захват аренды создаёт на цели служебную коллекцию и в базах, где позиции нет.
func cleanDBs(t *testing.T, dbs ...string) {
	t.Helper()
	for _, db := range dbs {
		cleanDB(t, db)
	}
}

// newIT — New с короткой арендой.
func newIT(dst *mongo.Client, cfg Config) *Replicator {
	r := New(srcClient, dst, cfg)
	r.leaseTTL, r.leaseRenew, r.stateEvery = itLeaseTTL, itLeaseRenew, itStateEvery
	return r
}

// release снимает аренду, как defer в main.
func release(t *testing.T, r *Replicator) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	assert.NoError(t, r.Release(ctx), "снятие аренды")
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
		require.FailNow(rn.t, fmt.Sprintf("Run не вернулся за %s", waitTimeout))
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
	rn.waitUntil(func(c *assert.CollectT) error {
		ok, diff := sameDBs(srcDB, dstDB)
		assert.True(c, ok, "база %s → %s не совпала: %s", srcDB, dstDB, diff)
		return nil
	}, "база %s → %s не совпала за %s", srcDB, dstDB, waitTimeout)
}

// waitUntil ждёт, пока проверки cond на c пройдут. Ранний выход Run и ошибка,
// которую вернул cond (возвращать до проверок на c), прекращают ожидание
// и проверяются после него.
func (rn *runner) waitUntil(cond func(c *assert.CollectT) error, msg string, args ...any) {
	rn.t.Helper()
	var exited bool
	var exitErr, condErr error
	require.EventuallyWithT(rn.t, func(c *assert.CollectT) {
		if done, err := rn.exited(); done {
			exited, exitErr = true, err
			return
		}
		condErr = cond(c)
	}, waitTimeout, pollEvery, append([]any{msg}, args...)...)
	require.False(rn.t, exited, "Run вернулся раньше времени: %v", exitErr)
	require.NoError(rn.t, condErr, append([]any{msg}, args...)...)
}

// waitQuiet ждёт, пока чекпойнт и Applied перестанут меняться. Закрывает
// гонку flush → saveToken: данные на цели уже видны, а чекпойнт ещё старый.
func (rn *runner) waitQuiet() {
	rn.t.Helper()
	deadline := time.Now().Add(waitTimeout)
	prevTok, prevN := rn.ckptToken(), rn.r.Applied()
	for {
		// окно устойчивости: чекпойнт и Applied не должны меняться весь интервал
		time.Sleep(quietInterval)
		done, err := rn.exited()
		require.False(rn.t, done, "Run вернулся раньше времени: %v", err)
		tok, n := rn.ckptToken(), rn.r.Applied()
		if tok != nil && bytes.Equal(tok, prevTok) && n == prevN {
			return
		}
		require.False(rn.t, time.Now().After(deadline), "чекпойнт не успокоился за %s", waitTimeout)
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

// ckptPair — пара баз, для которой записан чекпойнт.
func (rn *runner) ckptPair() dbPair {
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

// ckptOf — служебная коллекция чекпойнта базы цели dstDB.
func ckptOf(dstDB string) *mongo.Collection {
	return dstClient.Database(dstDB).Collection(ckptColl)
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

// collNames — обычные коллекции базы: views, system.* и служебная коллекция
// чекпойнта не реплицируются.
func collNames(ctx context.Context, c *mongo.Client, db string) ([]string, error) {
	names, err := c.Database(db).ListCollectionNames(ctx, bson.M{
		"type": "collection",
		"name": bson.M{"$not": bson.Regex{Pattern: `^system\.`}, "$ne": ckptColl},
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

	require.NotNil(t, rn, "подтест не запустил репликатор")
	done, err := rn.exited()
	require.True(t, done, "репликатор продолжает работать после завершения подтеста")
	assert.ErrorIs(t, err, context.Canceled, "Run должен завершиться отменой контекста")
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

// writeCkpt записывает на цель чекпойнт базы цели pair.dst с токеном-заглушкой.
func writeCkpt(t *testing.T, pair dbPair) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	_, err := ckptOf(pair.dst).ReplaceOne(ctx,
		bson.M{"_id": ckptID},
		bson.M{"_id": ckptID, "token": bson.M{"_data": "00"}, "db": pair.src, "dst_db": pair.dst},
		options.Replace().SetUpsert(true))
	require.NoError(t, err)
}

// insertPadded вставляет в db.items n документов по ~10 КБ и возвращает их суммарный bsonsize.
func insertPadded(t *testing.T, db string, n int) uint64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	var size uint64
	for i := range n {
		doc := bson.D{{Key: "_id", Value: int32(i)}, {Key: "pad", Value: strings.Repeat("x", 10_000)}}
		raw, err := bson.Marshal(doc)
		require.NoError(t, err)
		size += uint64(len(raw))
		_, err = srcClient.Database(db).Collection("items").InsertOne(ctx, doc)
		require.NoError(t, err)
	}
	return size
}

// withState — cfg с файлом состояния и сводкой по каталогу dir, как в main.
func withState(cfg Config, dir string) Config {
	dst := cfg.DstDB
	if dst == "" {
		dst = cfg.DB
	}
	cfg.StateDir = dir
	cfg.StateFile, cfg.Cluster = statefile.Path(dir, dst, dstURI)
	return cfg
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
	st, ok, err := findLease(dstDB)
	require.NoError(t, err)
	return st, ok
}

// findLease — readLease без t, для условий Eventually.
func findLease(dstDB string) (leaseState, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var st leaseState
	err := ckptOf(dstDB).FindOne(ctx, bson.M{"_id": ckptID}).Decode(&st)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return leaseState{}, false, nil
	}
	if err != nil {
		return leaseState{}, false, err
	}
	return st, true, nil
}

// captureLog пишет лог пакета в буфер до конца теста; читать буфер — после остановки репликатора.
func captureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := zlog.Logger
	zlog.Logger = zerolog.New(zerolog.SyncWriter(&buf)) // пишут Run и горутины продления
	t.Cleanup(func() { zlog.Logger = prev })
	return &buf
}
