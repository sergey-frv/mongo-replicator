//go:build integration

package replicator

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"

	"github.com/sergey-frv/mongo-replicator/internal/statefile"
	"github.com/sergey-frv/mongo-replicator/internal/traffic"
)

// Трафик считается через диалеры клиентов: поток источника отдаёт fullDocument
// каждой вставки, а запись на цель отправляет его же — оба счётчика не меньше
// суммарного размера документов. Оценка верна без compressors в URI: со сжатием
// байт по сети может быть меньше bsonsize.
// newTrafficIT — newIT со своими клиентами, считающими трафик (у общих из TestMain
// диалера нет); клиенты закрываются в cleanup теста.
func newTrafficIT(t *testing.T, cfg Config) *Replicator {
	t.Helper()
	tr := &traffic.Counters{}
	src, err := connectIT(options.Client().ApplyURI(srcURI).SetDialer(tr.SrcDialer()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = src.Disconnect(context.Background()) })
	dst, err := connectIT(options.Client().ApplyURI(dstURI).
		SetWriteConcern(writeconcern.Majority()).SetDialer(tr.DstDialer()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = dst.Disconnect(context.Background()) })

	cfg.Traffic = tr
	r := New(src, dst, cfg)
	r.leaseTTL, r.leaseRenew, r.stateEvery = itLeaseTTL, itLeaseRenew, itStateEvery
	return r
}

func TestIntegrationTraffic(t *testing.T) {
	db := prepare(t)
	r := newTrafficIT(t, Config{StartTS: srcNow(t), BatchSize: 1000, DB: db})
	rn := startOn(t, r)

	size := insertPadded(t, db, 20)
	rn.waitSame(db)
	got := r.cfg.Traffic.Snapshot()

	assert.GreaterOrEqual(t, got.SrcIn, size, "поток отдал все документы")
	assert.GreaterOrEqual(t, got.DstOut, size, "запись отправила все документы")
	assert.Positive(t, got.SrcOut)
	assert.Positive(t, got.DstIn)
}

// itActiveFor — срок активности файла состояния в сводках тестов: короче
// statefile.DefaultActiveFor, чтобы остановленный процесс быстро стал неактивным.
const itActiveFor = time.Second

func readStateFile(t *testing.T, path string) statefile.State {
	t.Helper()
	s, err := statefile.Load(path)
	require.NoError(t, err)
	return s
}

// Трафик процесса — в файле состояния на машине, не в документе чекпойнта.
// Писатель запускается с часовым интервалом: в файле только первая запись (до
// вставок) и последняя — из Release.
func TestIntegrationStateFile(t *testing.T) {
	db := prepare(t)
	r := newTrafficIT(t, withState(Config{StartTS: srcNow(t), BatchSize: 1000, DB: db}, t.TempDir()))
	r.stateEvery = time.Hour
	rn := startOn(t, r)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()

	size := insertPadded(t, db, 20)
	rn.waitSame(db)
	rn.waitQuiet()
	rn.stop()
	s := readStateFile(t, r.cfg.StateFile)
	now := r.cfg.Traffic.Snapshot()
	raw, err := ckptOf(db).FindOne(ctx, bson.M{"_id": ckptID}).Raw()

	require.NoError(t, err)
	assert.Equal(t, db, s.DB)
	assert.Equal(t, db, s.DstDB)
	assert.Equal(t, r.cfg.Cluster, s.Cluster)
	assert.Equal(t, r.owner, s.Owner)
	assert.GreaterOrEqual(t, s.Traffic.SrcIn, size, "последняя запись — в Release")
	assert.GreaterOrEqual(t, s.Traffic.DstOut, size, "последняя запись — в Release")
	assert.LessOrEqual(t, s.Traffic.SrcIn, now.SrcIn, "в файле — не больше насчитанного")
	assert.LessOrEqual(t, s.Traffic.DstOut, now.DstOut)
	_, err = raw.LookupErr("traffic")
	assert.Error(t, err, "в чекпойнте трафика нет")
}

// В паузе повтора (Run вернул временную ошибку) писатель продолжает обновлять файл:
// процесс по-прежнему владеет базой и в сводке активен.
func TestIntegrationStateFileRetryPause(t *testing.T) {
	db := prepare(t)
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()

	// уникальный индекс только на цели: второй документ с тем же code она не примет
	_, err := dstClient.Database(db).Collection("c").Indexes().CreateOne(ctx, mongo.IndexModel{
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

	r := newIT(dstClient, withState(Config{StartTS: startTS, BatchSize: 1, DB: db}, dir))
	// повтора Run в тесте нет: аренда, захваченная Run, должна остаться свежей всю паузу,
	// как в main, где повтор раз в 5 с захватывает её заново при TTL 60 с
	r.leaseTTL = time.Minute

	runErr := r.Run(ctx)
	require.True(t, mongo.IsDuplicateKeyError(runErr), "%v", runErr)
	returned := time.Now()
	defer release(t, r)
	// длительность паузы повтора: дольше окна активности, чтобы свежесть файла
	// доказывала запись писателя уже после возврата Run, а не запись до него
	time.Sleep(2 * itActiveFor)
	s := readStateFile(t, r.cfg.StateFile)
	mt, err := statefile.Summary(dir, time.Now(), itActiveFor)

	require.NoError(t, err)
	assert.True(t, s.UpdatedAt.After(returned), "файл обновлён после возврата Run: %s", s.UpdatedAt)
	assert.Equal(t, 1, mt.Processes)
	assert.Equal(t, 1, mt.Active, "процесс в паузе повтора активен")
}

// Следующий процесс на ту же базу продолжает счёт с накопленного в файле.
func TestIntegrationStateFileContinues(t *testing.T) {
	db := prepare(t)
	dir := t.TempDir()
	r1 := newTrafficIT(t, withState(Config{StartTS: srcNow(t), BatchSize: 1000, DB: db}, dir))
	rn1 := startOn(t, r1)
	insertPadded(t, db, 5)
	rn1.waitSame(db)
	rn1.waitQuiet()
	rn1.stop()
	first := readStateFile(t, r1.cfg.StateFile).Traffic
	require.Positive(t, first.SrcIn)
	r2 := newTrafficIT(t, withState(Config{BatchSize: 1000, DB: db}, dir))

	rn2 := startOn(t, r2)
	rn2.waitQuiet()
	rn2.stop()
	s := readStateFile(t, r2.cfg.StateFile)
	second := r2.cfg.Traffic.Snapshot()
	files, err := filepath.Glob(filepath.Join(dir, "*"))

	require.NoError(t, err)
	assert.Equal(t, r2.owner, s.Owner)
	assert.Greater(t, s.Traffic.SrcIn, first.SrcIn, "к прошлому значению прибавлен трафик второго процесса")
	assert.LessOrEqual(t, s.Traffic.SrcIn, first.SrcIn+second.SrcIn)
	assert.Greater(t, s.Traffic.DstOut, first.DstOut)
	assert.LessOrEqual(t, s.Traffic.DstOut, first.DstOut+second.DstOut)
	assert.Equal(t, []string{r2.cfg.StateFile}, files, "один файл, без переносов в сторону")
}

// Аренда истекла в паузе повтора, и базу успел забрать другой процесс с этой машины:
// он дописал в общий файл свой трафик. Повторный Run того же репликатора захватывает
// аренду заново и перечитывает файл — трафик другого процесса не затирается.
func TestIntegrationStateFileLeaseRetaken(t *testing.T) {
	db := prepare(t)
	r := newIT(dstClient, withState(Config{StartTS: srcNow(t), BatchSize: 1000, DB: db}, t.TempDir()))
	path := r.cfg.StateFile
	// писатель переживает Run: остановить и при падении теста до startOn (после Release — no-op)
	t.Cleanup(func() { r.stopState() })
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	runCtx, stopRun := context.WithCancel(ctx)
	runDone := make(chan error, 1)
	go func() { runDone <- r.Run(runCtx) }()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		s, err := statefile.Load(path)
		require.NoError(c, err)
		assert.Equal(c, r.owner, s.Owner)
	}, waitTimeout, pollEvery, "первая запись файла после захвата аренды")
	stopRun()
	require.ErrorIs(t, <-runDone, context.Canceled)
	// писатель переживает Run; без продления аренда устаревает, и он перестаёт писать
	require.Eventually(t, func() bool { return r.checkLease() != nil }, waitTimeout, pollEvery,
		"аренда устарела без продления")
	// другой процесс захватил истёкшую аренду, загрузил файл, добавил свой трафик и упал
	other, err := statefile.Load(path)
	require.NoError(t, err)
	other.Owner = "other:1:00000000"
	other.Traffic = other.Traffic.Add(traffic.Info{SrcIn: 1_000_000})
	other.UpdatedAt = time.Now().UTC()
	require.NoError(t, statefile.Write(path, other))
	_, err = ckptOf(db).UpdateOne(ctx, bson.M{"_id": ckptID},
		bson.M{"$set": bson.M{"owner": other.Owner, "lease_until": time.Now().Add(-time.Minute)}})
	require.NoError(t, err)

	startOn(t, r)

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		s, err := statefile.Load(path)
		require.NoError(c, err)
		assert.Equal(c, r.owner, s.Owner, "файл снова пишет этот процесс")
		assert.GreaterOrEqual(c, s.Traffic.SrcIn, other.Traffic.SrcIn, "трафик другого процесса сохранён")
	}, waitTimeout, pollEvery)
}

// Неверный --db (ErrDBMismatch): файл состояния базы цели не тронут — ни записи,
// ни переноса в сторону как файла другой пары баз.
func TestIntegrationStateFileDBMismatch(t *testing.T) {
	cleanDBs(t, "it_mismatch_a", "it_mismatch_b")
	dir := t.TempDir()
	writeCkpt(t, dbPair{src: "it_mismatch_a", dst: "it_mismatch_a"})
	cfg := withState(Config{BatchSize: 1000, DB: "it_mismatch_b", DstDB: "it_mismatch_a"}, dir)
	require.NoError(t, statefile.Write(cfg.StateFile, statefile.State{DB: "it_mismatch_a", DstDB: "it_mismatch_a",
		Cluster: cfg.Cluster, Traffic: traffic.Info{SrcIn: 12345}}))
	before, err := os.ReadFile(cfg.StateFile)
	require.NoError(t, err)

	_, err = runErr(t, cfg)

	require.ErrorIs(t, err, ErrDBMismatch)
	after, err := os.ReadFile(cfg.StateFile)
	require.NoError(t, err)
	assert.Equal(t, string(before), string(after), "файл состояния не тронут")
	files, err := filepath.Glob(filepath.Join(dir, "*"))
	require.NoError(t, err)
	assert.Equal(t, []string{cfg.StateFile}, files, "без переносов в сторону")
}

// Сводка по машине: сумма файлов состояния каталога, active — свежие файлы.
func TestIntegrationMachineTraffic(t *testing.T) {
	dbA := prepare(t)
	dbB := dbA + "_b"
	cleanDB(t, dbB)
	dir := t.TempDir()
	logs := captureLog(t)
	startTS := srcNow(t)

	rnA := startOn(t, newTrafficIT(t, withState(Config{StartTS: startTS, BatchSize: 1000, DB: dbA}, dir)))
	rnB := startOn(t, newTrafficIT(t, withState(Config{StartTS: startTS, BatchSize: 1000, DB: dbB}, dir)))
	insertPadded(t, dbA, 3)
	insertPadded(t, dbB, 3)
	rnA.waitSame(dbA)
	rnB.waitSame(dbB)
	rnA.waitQuiet()
	rnB.waitQuiet()
	mt, err := statefile.Summary(dir, time.Now(), itActiveFor)

	require.NoError(t, err)
	assert.Equal(t, 2, mt.Processes)
	assert.Equal(t, 2, mt.Active, "оба процесса работают")
	require.NotNil(t, rnA.r.Status().Machine, "первая сводка — сразу после старта")

	rnA.stop()
	rnB.stop()
	a := readStateFile(t, rnA.r.cfg.StateFile).Traffic
	b := readStateFile(t, rnB.r.cfg.StateFile).Traffic
	mt, err = statefile.Summary(dir, time.Now(), itActiveFor)

	require.NoError(t, err)
	assert.Equal(t, 2, mt.Processes)
	assert.Equal(t, traffic.Info{
		SrcIn: a.SrcIn + b.SrcIn, SrcOut: a.SrcOut + b.SrcOut,
		DstIn: a.DstIn + b.DstIn, DstOut: a.DstOut + b.DstOut,
	}, mt.Traffic)
	assert.Positive(t, mt.Traffic.SrcIn)
	require.Eventually(t, func() bool {
		mt, err := statefile.Summary(dir, time.Now(), itActiveFor)
		return err == nil && mt.Processes == 2 && mt.Active == 0
	}, waitTimeout, pollEvery, "после Release файлы больше не обновляются")
	assert.Contains(t, logs.String(), "трафик на машине")
}
