//go:build integration

package verify

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"
	"time"
	"unicode"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"

	"github.com/sergey-frv/mongo-replicator/internal/replicator"
)

const waitTimeout = 30 * time.Second

var (
	srcClient, dstClient *mongo.Client
	srcURI, dstURI       string
	// секунда последней операции на источнике, сделанной обвязкой предыдущего
	// теста (или prepare текущего); srcNow ждёт, пока время уйдёт дальше
	lastSrcSec uint32
)

func TestMain(m *testing.M) {
	// свои переменные, а не SRC_URI/DST_URI: тесты удаляют базы, и шелл
	// с загруженным .env не должен направить их на рабочие кластеры
	srcURI = os.Getenv("IT_SRC_URI")
	dstURI = os.Getenv("IT_DST_URI")
	if srcURI == "" || dstURI == "" {
		log.Fatal("IT_SRC_URI и IT_DST_URI не заданы: интеграционные тесты запускаются через make test-integration")
	}

	var err error
	if srcClient, err = connectIT(options.Client().ApplyURI(srcURI).SetAppName("mongo-replicator-verify")); err != nil {
		log.Fatalf("источник: %v", err)
	}
	if dstClient, err = connectIT(options.Client().ApplyURI(dstURI).
		SetAppName("mongo-replicator-verify").
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

// snake превращает имя теста в имя базы: TestIntegrationSchema/sub → integration_schema_sub.
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

// appendSrcNote пишет no-op с меткой name в oplog источника и возвращает его
// время; без t — для условий Eventually. На простаивающем кластере
// operationTime иначе двигается раз в 10 с.
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

// srcNow — текущее время источника, уже за секундой последней операции
// обвязки: --since округлён до секунды, и сверка с него иначе захватила бы
// хвост предыдущего теста, включая его cleanup-dropDatabase.
func srcNow(t *testing.T) bson.Timestamp {
	t.Helper()
	name, last := t.Name(), lastSrcSec // условие идёт в своей горутине: глобальное копируем до вызова
	var ts bson.Timestamp
	var noteErr error
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		op, err := appendSrcNote(name)
		if err != nil {
			noteErr = err // неожиданная ошибка прекращает ожидание и проверяется после него
			return
		}
		ts = op
		assert.Greater(c, ts.T, last, "время источника не двигается: %d", ts.T)
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
	ts, err := appendSrcNote(t.Name())
	require.NoError(t, err, "no-op в oplog источника")
	lastSrcSec = ts.T
}

// prepare выдаёт тесту чистую одноимённую базу на обоих кластерах (на цели —
// вместе с чекпойнтом) и удаляет её в cleanup.
func prepare(t *testing.T) (db string) {
	t.Helper()
	db = "it_" + snake(t.Name())
	dropDB(t, db) // остатки упавших прогонов: контейнеры живут между запусками
	t.Cleanup(func() { dropDB(t, db) })
	return db
}

// writeCkpt записывает на цель чекпойнт одноимённой пары баз db с позицией at:
// токен — заглушка, которую tokenTime не разбирает, поэтому позиция берётся
// из clusterTime.
func writeCkpt(t *testing.T, db string, at bson.Timestamp) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	_, err := dstClient.Database(db).Collection(replicator.CheckpointColl).ReplaceOne(ctx,
		bson.M{"_id": "checkpoint"},
		bson.M{"_id": "checkpoint", "token": bson.M{"_data": "00"}, "clusterTime": at, "db": db, "dst_db": db},
		options.Replace().SetUpsert(true))
	require.NoError(t, err)
}

// Проверка обвязки: writeCkpt записывает позицию, которую читает AppliedPosition.
func TestIntegrationWriteCkpt(t *testing.T) {
	db := prepare(t)
	at := srcNow(t)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()

	writeCkpt(t, db, at)

	got, ok, err := replicator.AppliedPosition(ctx, dstClient, db, db)
	require.NoError(t, err)
	assert.True(t, ok, "позиция чекпойнта есть")
	assert.Equal(t, at, got)
}
