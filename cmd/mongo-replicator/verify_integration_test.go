//go:build integration

package main

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
	"unicode"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	mongoopts "go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/sergey-frv/mongo-replicator/internal/replicator"
)

// runVerifyTimeout — ожидание возврата runVerify: проверка ключа — опрос
// чекпойнта раз в секунду и ожидание потока сверки.
const runVerifyTimeout = 60 * time.Second

// Схема без расхождений и ни одного изменённого ключа — код 0.
func TestIntegrationRunVerifyClean(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), runVerifyTimeout)
	defer cancel()
	src, dst, db, since := prepareVerify(ctx, t)
	followCkpt(t, src, dst, db)

	code := waitRunVerify(t, verifyOpts(t, db, since))

	assert.Equal(t, 0, code)
}

// Испорченный документ на цели — дефект, код 1.
func TestIntegrationRunVerifyDefect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), runVerifyTimeout)
	defer cancel()
	src, dst, db, since := prepareVerify(ctx, t)
	_, err := src.Database(db).Collection("items").InsertOne(ctx, bson.D{{Key: "_id", Value: 1}, {Key: "v", Value: "a"}})
	require.NoError(t, err)
	_, err = dst.Database(db).Collection("items").InsertOne(ctx, bson.D{{Key: "_id", Value: 1}, {Key: "v", Value: "испорчен"}})
	require.NoError(t, err)
	followCkpt(t, src, dst, db)

	code := waitRunVerify(t, verifyOpts(t, db, since))

	assert.Equal(t, 1, code)
}

// itURIs — URI кластеров compose. Свои переменные, а не SRC_URI/DST_URI: тесты
// удаляют базы, и шелл с загруженным .env не должен направить их на рабочие кластеры.
func itURIs(t *testing.T) (srcURI, dstURI string) {
	t.Helper()
	srcURI, dstURI = os.Getenv("IT_SRC_URI"), os.Getenv("IT_DST_URI")
	require.NotEmpty(t, srcURI, "IT_SRC_URI не задан: интеграционные тесты запускаются через make test-integration")
	require.NotEmpty(t, dstURI, "IT_DST_URI не задан: интеграционные тесты запускаются через make test-integration")
	return srcURI, dstURI
}

// verifyOpts — опции сверки одноимённой пары баз db с начала since до догона
// чекпойнта; файлы позиции и отчётов — во временном каталоге теста.
func verifyOpts(t *testing.T, db string, since uint32) verifyOptions {
	t.Helper()
	srcURI, dstURI := itURIs(t)
	return verifyOptions{
		commonOptions: commonOptions{Src: srcURI, Dst: dstURI, DB: db, DstDB: db, StateDir: t.TempDir()},
		Since:         since, Rate: 200, Sample: 1, UntilCaughtUp: true,
	}
}

// waitRunVerify запускает runVerify в горутине и ждёт его кода выхода.
func waitRunVerify(t *testing.T, opts verifyOptions) int {
	t.Helper()
	done := make(chan int, 1)
	go func() {
		done <- runVerify(opts, mongoopts.Client().ApplyURI(opts.Src), mongoopts.Client().ApplyURI(opts.Dst))
	}()
	select {
	case code := <-done:
		return code
	case <-time.After(runVerifyTimeout):
		require.FailNow(t, "runVerify не вернулся")
		return 0
	}
}

// prepareVerify подключается к кластерам и выдаёт тесту чистую одноимённую базу на
// обоих (удаляется в cleanup) и начало сверки — секунду источника после удаления
// базы: иначе поток сверки увидел бы dropDatabase остатков и закончился.
func prepareVerify(ctx context.Context, t *testing.T) (src, dst *mongo.Client, db string, since uint32) {
	t.Helper()
	srcURI, dstURI := itURIs(t)
	src, dst = itClient(ctx, t, srcURI), itClient(ctx, t, dstURI)
	db = "it_" + snake(t.Name())
	dropBoth := func(ctx context.Context) error {
		if err := src.Database(db).Drop(ctx); err != nil {
			return err
		}
		return dst.Database(db).Drop(ctx)
	}
	require.NoError(t, dropBoth(ctx), "остатки упавших прогонов: контейнеры живут между запусками")
	t.Cleanup(func() { assert.NoError(t, dropBoth(context.Background())) })

	name := t.Name()
	dropped, err := appendNote(ctx, src, name)
	require.NoError(t, err, "no-op в oplog источника")
	var now bson.Timestamp
	var noteErr error
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		ts, err := appendNote(ctx, src, name)
		if err != nil {
			noteErr = err // неожиданная ошибка прекращает ожидание и проверяется после него
			return
		}
		now = ts
		assert.Greater(c, ts.T, dropped.T, "время источника не двигается: %d", ts.T)
	}, 5*time.Second, 100*time.Millisecond, "время источника не ушло за секунду удаления базы")
	require.NoError(t, noteErr, "no-op в oplog источника")
	return src, dst, db, now.T
}

// itClient подключается к uri; клиент отключается в cleanup.
func itClient(ctx context.Context, t *testing.T, uri string) *mongo.Client {
	t.Helper()
	c, err := connect(ctx, mongoopts.Client().ApplyURI(uri))
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Disconnect(context.Background()) })
	return c
}

// followCkpt каждые 100 мс пишет no-op в oplog источника и ставит на него
// чекпойнт db, до конца теста: «репликатор» без отставания (данные цели тест
// готовит сам), а поток сверки двигается, не дожидаясь no-op сервера раз в 10 с.
func followCkpt(t *testing.T, src, dst *mongo.Client, db string) {
	t.Helper()
	name := t.Name()
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			ts, err := appendNote(ctx, src, name)
			if err == nil {
				// токен — заглушка, которую TokenTime не разбирает: позиция — clusterTime
				_, err = dst.Database(db).Collection(replicator.CheckpointColl).ReplaceOne(ctx,
					bson.M{"_id": "checkpoint"},
					bson.M{"_id": "checkpoint", "token": bson.M{"_data": "00"}, "clusterTime": ts, "db": db, "dst_db": db},
					mongoopts.Replace().SetUpsert(true))
			}
			cancel()
			if !assert.NoError(t, err, "чекпойнт за временем источника") {
				return
			}
		}
	}()
	t.Cleanup(func() {
		close(stop)
		<-done
	})
}

// appendNote пишет no-op с меткой name в oplog источника и возвращает его время;
// без t — для условий Eventually и горутин.
func appendNote(ctx context.Context, src *mongo.Client, name string) (bson.Timestamp, error) {
	raw, err := src.Database("admin").RunCommand(ctx, bson.D{
		{Key: "appendOplogNote", Value: 1},
		{Key: "data", Value: bson.D{{Key: "it", Value: name}}},
	}).Raw()
	if err != nil {
		return bson.Timestamp{}, err
	}
	ts, i := raw.Lookup("operationTime").Timestamp()
	return bson.Timestamp{T: ts, I: i}, nil
}

// snake превращает имя теста в имя базы: TestIntegrationRunVerifyClean → integration_run_verify_clean.
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
