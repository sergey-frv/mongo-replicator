//go:build integration

package replicator

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

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

	// время источника для лага: ping с правами на одну базу
	now, err := rn.r.srcTime(ctx)

	require.NoError(t, err)
	assert.False(t, now.Before(own), "время источника %v позади записи %v", now, own)

	n := rn.r.Applied()
	foreign := insertAt(ctx, t, srcClient.Database(other).Collection("c"), int32(1))
	rn.waitUntil(func(c *assert.CollectT) error {
		ck, err := rn.r.loadCheckpoint(ctx) // читает только dst: безопасно во время Run
		if err != nil {
			return err
		}
		assert.False(c, ck.ClusterTime.Before(foreign), "применённая позиция %v", ck.ClusterTime)
		return nil
	}, "применённая позиция не дошла до записи в другую базу %v", foreign)

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
			time.Sleep(200 * time.Millisecond) // темп нагрузки: вставка раз в 200 мс, чаще maxAwaitTime
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
	require.NoError(t, writeErr, "запись на источник")
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
