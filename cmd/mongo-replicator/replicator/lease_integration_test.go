//go:build integration

package replicator

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Второй процесс на ту же базу цели не стартует, пока первый работает, и не трогает
// общий файл состояния; после остановки первого (со снятием аренды) — стартует
// сразу, не дожидаясь её истечения.
func TestIntegrationLeaseHeld(t *testing.T) {
	db := prepare(t)
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()

	rA := newIT(dstClient, withState(Config{StartTS: srcNow(t), BatchSize: 1000, DB: db}, dir))
	rA.stateEvery = time.Hour // пока A работает, файл может изменить только второй процесс
	rnA := startOn(t, rA)
	items := srcClient.Database(db).Collection("items")
	_, err := items.InsertOne(ctx, bson.D{{Key: "_id", Value: int32(1)}})
	require.NoError(t, err)
	rnA.waitSame(db)
	rnA.waitQuiet()
	before, err := os.ReadFile(rnA.r.cfg.StateFile)
	require.NoError(t, err)

	_, err = runErr(t, withState(Config{BatchSize: 1000, DB: db}, dir))

	require.ErrorIs(t, err, ErrLeaseHeld)
	assert.ErrorContains(t, err, rnA.r.owner)
	after, err := os.ReadFile(rnA.r.cfg.StateFile)
	require.NoError(t, err)
	assert.Equal(t, string(before), string(after), "второй процесс файл не тронул")
	files, err := filepath.Glob(filepath.Join(dir, "*"))
	require.NoError(t, err)
	assert.Equal(t, []string{rnA.r.cfg.StateFile}, files, "без переносов в сторону")
	st, _ := readLease(t, db)
	assert.Equal(t, rnA.r.owner, st.Owner, "неудачный захват и Release второго не трогают аренду первого")

	rnA.stop()

	st, found := readLease(t, db)
	require.True(t, found, "документ с позицией остаётся")
	assert.Empty(t, st.Owner, "аренда снята")
	assert.True(t, st.LeaseUntil.IsZero())
	assert.NotNil(t, st.Token)

	// без StartTS: B продолжает с чекпойнта A
	rnB := start(t, withState(Config{BatchSize: 1000, DB: db}, dir))
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
	_, err := ckptOf(db).InsertOne(ctx, bson.M{
		"_id": ckptID, "owner": "crashed:1:deadbeef", "lease_until": time.Now().Add(-time.Minute),
	})
	require.NoError(t, err)

	rn := start(t, Config{StartTS: srcNow(t), BatchSize: 1000, DB: db})
	_, err = srcClient.Database(db).Collection("items").InsertOne(ctx, bson.D{{Key: "_id", Value: int32(1)}})
	require.NoError(t, err)
	rn.waitSame(db)

	st, _ := readLease(t, db)
	assert.Equal(t, rn.r.owner, st.Owner)
}

// Release процесса, не получившего аренду, не удаляет документ владельца, который
// ещё не сохранил позицию.
func TestIntegrationLeaseHeldNoToken(t *testing.T) {
	db := prepare(t)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	const holder = "starting:1:00000000"
	_, err := ckptOf(db).InsertOne(ctx, bson.M{
		"_id": ckptID, "owner": holder, "lease_until": time.Now().Add(time.Minute),
	})
	require.NoError(t, err)

	_, err = runErr(t, Config{StartTS: srcNow(t), BatchSize: 1000, DB: db})

	require.ErrorIs(t, err, ErrLeaseHeld)
	st, found := readLease(t, db)
	require.True(t, found, "документ владельца не удалён")
	assert.Equal(t, holder, st.Owner)
}

// Без аренды saveToken не создаёт и не двигает чекпойнт: ErrLeaseLost, позиция в
// репликаторе не меняется.
func TestIntegrationSaveTokenNotOwner(t *testing.T) {
	db := prepare(t)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	token, err := bson.Marshal(bson.M{"_data": "00"})
	require.NoError(t, err)
	r := newIT(dstClient, Config{BatchSize: 1000, DB: db})

	err = r.saveToken(ctx, token)

	require.ErrorIs(t, err, ErrLeaseLost, "без захвата аренды")
	_, found := readLease(t, db)
	assert.False(t, found, "без захвата документ не создаётся")

	require.NoError(t, r.acquireLease(ctx))
	const intruder = "intruder:1:00000000"
	_, err = ckptOf(db).UpdateOne(ctx,
		bson.M{"_id": ckptID}, bson.M{"$set": bson.M{"owner": intruder}})
	require.NoError(t, err)

	err = r.saveToken(ctx, token)

	require.ErrorIs(t, err, ErrLeaseLost, "аренду забрали")
	st, _ := readLease(t, db)
	assert.Equal(t, intruder, st.Owner)
	assert.Nil(t, st.Token, "чужой документ не сдвинут")
	assert.Nil(t, r.saved)
	assert.True(t, r.appliedTS.IsZero())
}

// Продления давно не было (процесс стоял дольше аренды): запись на цель не идёт,
// Run завершается временной errLeaseExpiring, даже если горутина продления
// ещё не сработала.
func TestIntegrationLeaseExpiringStopsWrites(t *testing.T) {
	db := prepare(t)
	r := newIT(dstClient, Config{StartTS: srcNow(t), BatchSize: 1000, DB: db})
	r.leaseRenew = time.Hour // продление не придёт: срабатывает проверка перед записью

	rn := startOn(t, r)

	require.ErrorIs(t, rn.waitExit(), errLeaseExpiring)
}

// Аренду забрали во время работы: Run завершается с ErrLeaseLost, а Release
// не снимает чужую аренду.
func TestIntegrationLeaseLost(t *testing.T) {
	db := prepare(t)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()

	rn := start(t, Config{StartTS: srcNow(t), BatchSize: 1000, DB: db})
	owner := rn.r.owner
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		st, _, err := findLease(db)
		require.NoError(c, err)
		assert.Equal(c, owner, st.Owner, "владелец аренды")
	}, waitTimeout, pollEvery, "репликатор не захватил аренду за %s", waitTimeout)

	const intruder = "intruder:1:00000000"
	_, err := ckptOf(db).UpdateOne(ctx,
		bson.M{"_id": ckptID}, bson.M{"$set": bson.M{"owner": intruder}})
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
			// длительность действия: DDL держит основной цикл три срока аренды —
			// хватает на проверку продления и попытку захвата вторым процессом
			time.Sleep(3 * itLeaseTTL)
		})
	}
	rn := startOn(t, r)
	require.NoError(t, srcClient.Database(db).CreateCollection(ctx, "c"))
	select {
	case <-inDDL:
	case <-ctx.Done():
		require.FailNow(t, "DDL не дошёл до применения")
	}
	before, _ := readLease(t, db)

	// серверное время аренды: без продления за этот интервал она уже истекла бы
	time.Sleep(itLeaseTTL + itLeaseRenew)

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
