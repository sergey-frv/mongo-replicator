//go:build integration

package replicator

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Снимок статуса: поток открыт — «работает»; пачка DML и DDL видны в Status.
func TestIntegrationStatus(t *testing.T) {
	db := prepare(t)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()

	startTS := srcNow(t)
	a, b := srcClient.Database(db).Collection("a"), srcClient.Database(db).Collection("b")
	_, err := a.InsertOne(ctx, bson.D{{Key: "_id", Value: int32(1)}, {Key: "v", Value: int32(1)}})
	require.NoError(t, err)
	_, err = b.InsertOne(ctx, bson.D{{Key: "_id", Value: int32(1)}})
	require.NoError(t, err)
	_, err = a.UpdateByID(ctx, int32(1), bson.D{{Key: "$set", Value: bson.D{{Key: "v", Value: int32(2)}}}})
	require.NoError(t, err)
	_, err = b.DeleteOne(ctx, bson.D{{Key: "_id", Value: int32(1)}})
	require.NoError(t, err)
	_, err = a.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "v", Value: 1}}, Options: options.Index().SetName("a_v"),
	})
	require.NoError(t, err)

	started := time.Now()
	rn := start(t, withState(Config{StartTS: startTS, BatchSize: 1000, DB: db}, t.TempDir()))
	rn.waitSame(db)
	rn.waitQuiet()

	s := rn.r.Status()
	assert.Equal(t, StateRunning, s.State)
	assert.EqualValues(t, 4, s.Applied)
	require.NotNil(t, s.LastBatch, "DML перед DDL записан пачкой")
	assert.False(t, s.LastBatch.At.Before(started))
	ops := s.LastBatch.Ops
	assert.Positive(t, ops.Ins+ops.Upd+ops.Rep+ops.Del)
	assert.Subset(t, []string{"a", "b"}, s.LastBatch.Colls)
	require.NotNil(t, s.LastDDL)
	assert.Equal(t, "createIndexes", s.LastDDL.Op)
	assert.Equal(t, db+".a", s.LastDDL.NS)
	assert.Equal(t, "a_v", s.LastDDL.Details)
	assert.False(t, s.LastDDL.At.Before(started))
	require.NotNil(t, s.Machine, "первая сводка — сразу после старта")
	assert.Equal(t, 1, s.Machine.Processes, "в каталоге — только файл этого процесса")

	// прогресс — после остановки: lastLog принадлежит горутине Run
	rn.stop()
	rn.r.lastLog = time.Now().Add(-20 * time.Second)
	rn.r.logProgress(ctx)

	s = rn.r.Status()
	assert.GreaterOrEqual(t, s.Lag, time.Duration(0), "позиция сохранена, источник доступен")
	assert.False(t, s.AppliedAt.IsZero())
}

// Временная ошибка, повтор в main и успешный повторный Run: состояние снова «работает».
func TestIntegrationStatusRetry(t *testing.T) {
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

	r := newIT(dstClient, Config{StartTS: startTS, BatchSize: 1, DB: db})

	runErr := r.Run(ctx)

	require.True(t, mongo.IsDuplicateKeyError(runErr), "%v", runErr)
	s := r.Status()
	assert.Equal(t, StateStopped, s.State)
	assert.Equal(t, runErr.Error(), s.LastErr)

	// как цикл повторов main
	r.SetRetry(runErr)

	s = r.Status()
	assert.Equal(t, StateRetry, s.State)
	assert.Equal(t, 1, s.Retry)
	assert.Equal(t, runErr.Error(), s.LastErr)

	require.NoError(t, dstC.Indexes().DropOne(ctx, "c_code_u"))
	rn := startOn(t, r)
	rn.waitSame(db)

	s = r.Status()
	assert.Equal(t, StateRunning, s.State)
	assert.Zero(t, s.Retry)
	assert.Empty(t, s.LastErr)

	rn.stop()

	s = r.Status()
	assert.Equal(t, StateStopped, s.State)
	assert.Empty(t, s.LastErr, "остановка отменой — не ошибка")
}
