//go:build integration

package verify

import (
	"context"
	"encoding/json"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/sergey-frv/mongo-replicator/internal/replicator"
)

// runTimeout — ожидание возврата Run: проверка ключа — опрос чекпойнта раз в
// секунду и ожидание потока.
const runTimeout = 60 * time.Second

// itConfig — сверка одноимённой пары баз db с начала since.
func itConfig(db string, since bson.Timestamp) Config {
	return Config{DB: db, DstDB: db, Since: since.T, Rate: 200, Sample: 1, SrcURI: srcURI, DstURI: dstURI}
}

// verifyRun — Run в горутине; cleanup отменяет его и ждёт возврата.
type verifyRun struct {
	t      *testing.T
	v      *Verifier
	cancel context.CancelFunc
	done   chan struct{}
	rep    Report
	err    error
}

func startVerify(t *testing.T, cfg Config) *verifyRun {
	t.Helper()
	return startVerifier(t, New(srcClient, dstClient, cfg))
}

// startVerifier — startVerify готовой сверки: её поля меняются до запуска,
// иначе гонка с Run.
func startVerifier(t *testing.T, v *Verifier) *verifyRun {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	vr := &verifyRun{t: t, v: v, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(vr.done)
		vr.rep, vr.err = vr.v.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-vr.done:
		case <-time.After(runTimeout):
			assert.Fail(t, "Run не вернулся после отмены")
		}
	})
	return vr
}

// wait ждёт возврата Run.
func (vr *verifyRun) wait() (Report, error) {
	vr.t.Helper()
	select {
	case <-vr.done:
	case <-time.After(runTimeout):
		require.FailNow(vr.t, "Run не вернулся")
	}
	return vr.rep, vr.err
}

// stop отменяет Run и ждёт возврата.
func (vr *verifyRun) stop() (Report, error) {
	vr.t.Helper()
	vr.cancel()
	return vr.wait()
}

// until ждёт снимка цикла, для которого cond верно. Возврат Run прекращает
// ожидание и валит тест.
func (vr *verifyRun) until(cond func(status) bool, msg string) {
	vr.t.Helper()
	v, done := vr.v, vr.done
	exited := false
	require.Eventually(vr.t, func() bool {
		select {
		case <-done:
			exited = true // прекратить ожидание: Run уже не изменит снимок
			return true
		default:
		}
		return cond(v.status())
	}, runTimeout, 50*time.Millisecond, msg)
	require.False(vr.t, exited, "Run вернулся раньше (%s): %v", msg, vr.err)
}

// pulse каждые 100 мс пишет no-op в oplog источника и вызывает fn с его временем
// (nil — не вызывает), до конца теста: поток сверки продвигается, не дожидаясь
// no-op сервера раз в 10 с.
func pulse(t *testing.T, fn func(context.Context, bson.Timestamp) error) {
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
			ts, err := appendSrcNote(name)
			if !assert.NoError(t, err, "no-op в oplog источника") {
				return
			}
			if fn != nil {
				ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
				err = fn(ctx, ts)
				cancel()
				if !assert.NoError(t, err) {
					return
				}
			}
		}
	}()
	t.Cleanup(func() {
		close(stop)
		<-done
	})
}

// tickSrc — время источника двигается каждые 100 мс.
func tickSrc(t *testing.T) {
	t.Helper()
	pulse(t, nil)
}

// followCkpt — чекпойнт db двигается за временем источника каждые 100 мс:
// «репликатор» без отставания, данные цели тест готовит сам.
func followCkpt(t *testing.T, db string) {
	t.Helper()
	pulse(t, func(ctx context.Context, ts bson.Timestamp) error { return setCkpt(ctx, db, ts) })
}

// doc — документ коллекции items.
func doc(id any, v string) bson.D { return bson.D{{Key: "_id", Value: id}, {Key: "v", Value: v}} }

// insert вставляет документы в db.items.
func insert(ctx context.Context, t *testing.T, c *mongo.Client, db string, docs ...any) {
	t.Helper()
	_, err := c.Database(db).Collection("items").InsertMany(ctx, docs)
	require.NoError(t, err)
}

// setV меняет поле v документа id в db.items.
func setV(ctx context.Context, t *testing.T, c *mongo.Client, db string, id any, v string) {
	t.Helper()
	_, err := c.Database(db).Collection("items").UpdateOne(ctx, bson.M{"_id": id}, bson.M{"$set": bson.M{"v": v}})
	require.NoError(t, err)
}

// srcAt выполняет запись в db.items источника и возвращает её время.
func srcAt(ctx context.Context, t *testing.T, db string, write func(context.Context, *mongo.Collection) error) bson.Timestamp {
	t.Helper()
	sess, err := srcClient.StartSession()
	require.NoError(t, err)
	defer sess.EndSession(ctx)
	require.NoError(t, write(mongo.NewSessionContext(ctx, sess), srcClient.Database(db).Collection("items")))
	return *sess.OperationTime()
}

// Документы, изменённые после --since, скопированы на цель — совпадают;
// --until-caught-up завершает сверку.
func TestIntegrationVerifyMatch(t *testing.T) {
	db := prepare(t)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	since := srcNow(t)
	insert(ctx, t, srcClient, db, doc(1, "a"), doc(2, "a"), doc(3, "a"))
	setV(ctx, t, srcClient, db, 2, "b")
	insert(ctx, t, dstClient, db, doc(1, "a"), doc(2, "b"), doc(3, "a"))
	followCkpt(t, db)
	cfg := itConfig(db, since)
	cfg.UntilCaughtUp = true

	rep, err := startVerify(t, cfg).wait()

	require.NoError(t, err)
	assert.Equal(t, counts{Checked: 3, Matched: 3}, rep.counts)
	assert.True(t, rep.Clean(), "без дефектов")
}

// Испорченный и отсутствующий на цели документы — дефекты; в отчёте — оба
// документа и различающиеся поля.
func TestIntegrationVerifyCorrupt(t *testing.T) {
	db := prepare(t)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	since := srcNow(t)
	insert(ctx, t, srcClient, db, doc(1, "a"), doc(2, "a"))
	insert(ctx, t, dstClient, db, doc(1, "b"))
	followCkpt(t, db)
	cfg := itConfig(db, since)
	cfg.UntilCaughtUp = true
	cfg.StateDir = t.TempDir()

	rep, err := startVerify(t, cfg).wait()

	require.NoError(t, err)
	assert.Equal(t, counts{Checked: 2, Defects: 2}, rep.counts)
	got, err := readReport(rep.DefectsFile)
	require.NoError(t, err)
	src1, dst1 := `{"_id":{"$numberInt":"1"},"v":"a"}`, `{"_id":{"$numberInt":"1"},"v":"b"}`
	src2 := `{"_id":{"$numberInt":"2"},"v":"a"}`
	assert.Equal(t, report{
		{NS: db + ".items", ID: json.RawMessage(`{"$numberInt":"1"}`), Reason: "различается: v", Src: &src1, Dst: &dst1},
		{NS: db + ".items", ID: json.RawMessage(`{"$numberInt":"2"}`), Reason: "нет на цели", Src: &src2},
	}, got)
}

// Документ вставлен и удалён на источнике, а на цели остался — лишний.
func TestIntegrationVerifyExtraOnTarget(t *testing.T) {
	db := prepare(t)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	since := srcNow(t)
	insert(ctx, t, srcClient, db, doc(1, "a"))
	_, err := srcClient.Database(db).Collection("items").DeleteOne(ctx, bson.M{"_id": 1})
	require.NoError(t, err)
	insert(ctx, t, dstClient, db, doc(1, "a"))
	followCkpt(t, db)
	cfg := itConfig(db, since)
	cfg.UntilCaughtUp = true
	cfg.StateDir = t.TempDir()

	rep, err := startVerify(t, cfg).wait()

	require.NoError(t, err)
	assert.Equal(t, counts{Checked: 1, Defects: 1}, rep.counts)
	got, err := readReport(rep.DefectsFile)
	require.NoError(t, err)
	dst := `{"_id":{"$numberInt":"1"},"v":"a"}`
	assert.Equal(t, report{{NS: db + ".items", ID: json.RawMessage(`{"$numberInt":"1"}`), Reason: "лишний на цели", Dst: &dst}}, got)
}

// Документ был на обеих сторонах до --since, удалён на источнике после, а на
// цели удаление не применено.
func TestIntegrationVerifyDeleteMissed(t *testing.T) {
	db := prepare(t)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	insert(ctx, t, srcClient, db, doc(1, "a"), doc(2, "a"))
	insert(ctx, t, dstClient, db, doc(1, "a"), doc(2, "a"))
	since := srcNow(t)
	_, err := srcClient.Database(db).Collection("items").DeleteOne(ctx, bson.M{"_id": 1})
	require.NoError(t, err)
	followCkpt(t, db)
	cfg := itConfig(db, since)
	cfg.UntilCaughtUp = true

	rep, err := startVerify(t, cfg).wait()

	require.NoError(t, err)
	assert.Equal(t, counts{Checked: 1, Defects: 1}, rep.counts, "проверен только удалённый после --since")
}

// Ключ с временем, равным позиции чекпойнта, не проверяется: события с этим
// временем могут быть применены не все. Позиция строго больше — проверяется.
func TestIntegrationVerifyWaitsCheckpoint(t *testing.T) {
	db := prepare(t)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	since := srcNow(t)
	t0 := srcAt(ctx, t, db, func(ctx context.Context, c *mongo.Collection) error {
		_, err := c.InsertOne(ctx, doc(1, "a"))
		return err
	})
	insert(ctx, t, dstClient, db, doc(1, "a"))
	writeCkpt(t, db, t0)
	tickSrc(t)
	vr := startVerify(t, itConfig(db, since))
	vr.until(func(s status) bool { return s.queue == 1 }, "ключ в очереди")

	v := vr.v
	assert.Never(t, func() bool { return v.status().inflight > 0 }, 2*time.Second, 50*time.Millisecond,
		"ключ выдан на проверку при позиции чекпойнта, равной его времени")
	assert.Equal(t, status{queue: 1}, v.status())

	followCkpt(t, db)
	vr.until(func(s status) bool { return s.Matched == 1 }, "ключ проверен после сдвига чекпойнта")
	rep, err := vr.stop()
	require.NoError(t, err)
	assert.Equal(t, counts{Checked: 1, Matched: 1}, rep.counts)
}

// --sample 0.5: из _id 1..10 в выборку попадают 4 и 8 (sha256 _id), остальные
// события отброшены.
func TestIntegrationVerifySample(t *testing.T) {
	db := prepare(t)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	since := srcNow(t)
	docs := []any{
		doc(int32(1), "a"), doc(int32(2), "a"), doc(int32(3), "a"), doc(int32(4), "a"), doc(int32(5), "a"),
		doc(int32(6), "a"), doc(int32(7), "a"), doc(int32(8), "a"), doc(int32(9), "a"), doc(int32(10), "a"),
	}
	insert(ctx, t, srcClient, db, docs...)
	insert(ctx, t, dstClient, db, docs...)
	followCkpt(t, db)
	cfg := itConfig(db, since)
	cfg.UntilCaughtUp = true
	cfg.Sample = 0.5

	rep, err := startVerify(t, cfg).wait()

	require.NoError(t, err)
	assert.Equal(t, counts{Checked: 2, Matched: 2, SampledOut: 8}, rep.counts)
}

// --ids перепроверяет ключи из файла отчёта без файла позиции и --since; его
// отчёты не затирают отчёты основной сверки.
func TestIntegrationVerifyIDs(t *testing.T) {
	db := prepare(t)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	insert(ctx, t, srcClient, db, doc(1, "a"), doc(2, "a"), doc(3, "a"))
	insert(ctx, t, dstClient, db, doc(1, "a"), doc(2, "b"), doc(3, "b"))
	ids := filepath.Join(t.TempDir(), "defects.json")
	var r report
	require.NoError(t, r.add(db+".items", rawID(t, 1), "различается: v", nil, nil))
	require.NoError(t, r.add(db+".items", rawID(t, 2), "различается: v", nil, nil))
	require.NoError(t, writeReport(ids, r))
	followCkpt(t, db)
	// живые записи во время перепроверки: события ключа не из файла в очередь
	// не встают (иначе _id 3 — ещё один дефект)
	pulse(t, func(ctx context.Context, ts bson.Timestamp) error {
		_, err := srcClient.Database(db).Collection("items").
			UpdateOne(ctx, bson.M{"_id": 3}, bson.M{"$set": bson.M{"n": ts.T}})
		return err
	})
	cfg := itConfig(db, bson.Timestamp{})
	cfg.IDs = ids
	cfg.StateDir = t.TempDir()

	rep, err := startVerify(t, cfg).wait()

	require.NoError(t, err)
	assert.Equal(t, counts{Checked: 2, Matched: 1, Defects: 1}, rep.counts)
	files, _, err := positionPaths(cfg.StateDir, db, dstURI)
	require.NoError(t, err)
	assert.Regexp(t, `\.ids\.defects\.json$`, rep.DefectsFile)
	assert.FileExists(t, rep.DefectsFile)
	assert.NoFileExists(t, files.defects, "отчёт основной сверки")
	assert.NoFileExists(t, files.position, "файл позиции")
}

// --ids, остановленная до проверки ключа (чекпойнт стоит): ошибка, а не итог, и
// ключ — в отчёте «не проверено», иначе он пропал бы: файла позиции у --ids нет.
func TestIntegrationVerifyIDsStopped(t *testing.T) {
	db := prepare(t)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	insert(ctx, t, srcClient, db, doc(1, "a"))
	insert(ctx, t, dstClient, db, doc(1, "a"))
	ids := filepath.Join(t.TempDir(), "defects.json")
	var r report
	require.NoError(t, r.add(db+".items", rawID(t, 1), "различается: v", nil, nil))
	require.NoError(t, writeReport(ids, r))
	writeCkpt(t, db, srcNow(t))
	cfg := itConfig(db, bson.Timestamp{})
	cfg.IDs = ids
	cfg.StateDir = t.TempDir()
	vr := startVerify(t, cfg)
	vr.until(func(s status) bool { return s.ckptWaiting == 1 }, "S1 прочитан, проверка ждёт чекпойнт")

	rep, err := vr.stop()

	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, counts{Unverified: 1}, rep.counts)
	got, err := readReport(rep.UnverifiedFile)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, reportEntry{NS: db + ".items", ID: json.RawMessage(`{"$numberInt":"1"}`), Reason: "stopped"}, got[0])
}

// Мягкий предел очереди: при полной очереди поток всё равно читается, пока
// проверка ждёт его время, иначе ключ на проверке ждал бы поток вечно.
func TestIntegrationVerifyQueueFull(t *testing.T) {
	db := prepare(t)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	since := srcNow(t)
	insert(ctx, t, srcClient, db, doc(1, "a"), doc(2, "a"))
	insert(ctx, t, dstClient, db, doc(1, "a"), doc(2, "a"))
	followCkpt(t, db)
	cfg := itConfig(db, since)
	cfg.UntilCaughtUp = true
	v := New(srcClient, dstClient, cfg)
	v.q.limit = 1

	rep, err := startVerifier(t, v).wait()

	require.NoError(t, err)
	assert.Equal(t, counts{Checked: 2, Matched: 2}, rep.counts)
}

// ABA: документ изменён и возвращён между S1 и S2, на цели — промежуточное
// состояние (updateLookup репликатора) — не дефект, ключ проверяется заново.
func TestIntegrationVerifyABA(t *testing.T) {
	db := prepare(t)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	since := srcNow(t)
	insert(ctx, t, srcClient, db, doc(1, "A"))
	insert(ctx, t, dstClient, db, doc(1, "A"))
	writeCkpt(t, db, srcNow(t))
	tickSrc(t)
	vr := startVerify(t, itConfig(db, since))
	vr.until(func(s status) bool { return s.ckptWaiting == 1 }, "S1 прочитан, проверка ждёт чекпойнт")

	setV(ctx, t, srcClient, db, 1, "B")
	e2 := srcAt(ctx, t, db, func(ctx context.Context, c *mongo.Collection) error {
		_, err := c.UpdateOne(ctx, bson.M{"_id": 1}, bson.M{"$set": bson.M{"v": "A"}})
		return err
	})
	setV(ctx, t, dstClient, db, 1, "B")
	writeCkpt(t, db, e2) // применено всё строго до второго изменения: на цели B
	vr.until(func(s status) bool { return s.Checked == 1 }, "первый вердикт")
	setV(ctx, t, dstClient, db, 1, "A")
	followCkpt(t, db)
	vr.until(func(s status) bool { return s.Matched == 1 }, "повторная проверка")

	rep, err := vr.stop()
	require.NoError(t, err)
	assert.Equal(t, counts{Checked: 2, Matched: 1}, rep.counts, "горячий вердикт и совпадение, без дефекта")
}

// Транзакция: события двух документов с одним clusterTime. Позиция чекпойнта,
// равная ему, не значит, что применены оба (токен сохраняется и посреди группы),
// — ключи ждут позиции строго больше.
func TestIntegrationVerifyTransaction(t *testing.T) {
	db := prepare(t)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	since := srcNow(t)
	tx := srcAt(ctx, t, db, func(ctx context.Context, c *mongo.Collection) error {
		_, err := mongo.SessionFromContext(ctx).WithTransaction(ctx, func(ctx context.Context) (any, error) {
			return c.InsertMany(ctx, []any{doc(1, "a"), doc(2, "a")})
		})
		return err
	})
	insert(ctx, t, dstClient, db, doc(1, "a"), doc(2, "a"))
	writeCkpt(t, db, tx)
	tickSrc(t)
	vr := startVerify(t, itConfig(db, since))
	vr.until(func(s status) bool { return s.queue == 2 }, "оба ключа в очереди")

	v := vr.v
	assert.Never(t, func() bool { return v.status().inflight > 0 }, 2*time.Second, 50*time.Millisecond,
		"ключи выданы на проверку при позиции чекпойнта, равной времени транзакции")
	assert.Equal(t, status{queue: 2}, v.status())

	followCkpt(t, db)
	vr.until(func(s status) bool { return s.Matched == 2 }, "ключи проверены после сдвига чекпойнта")
	rep, err := vr.stop()
	require.NoError(t, err)
	assert.Equal(t, counts{Checked: 2, Matched: 2}, rep.counts)
}

// Чекпойнта на цели ещё нет (репликатор не запущен): сверка ждёт его, а не
// падает, и проверяет ключи, когда он появится.
func TestIntegrationVerifyNoCheckpoint(t *testing.T) {
	db := prepare(t)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	since := srcNow(t)
	insert(ctx, t, srcClient, db, doc(1, "a"))
	insert(ctx, t, dstClient, db, doc(1, "a"))
	tickSrc(t)
	vr := startVerify(t, itConfig(db, since))
	vr.until(func(s status) bool { return s.queue == 1 }, "ключ в очереди")

	v := vr.v
	assert.Never(t, func() bool { return v.status().inflight > 0 }, 2*time.Second, 50*time.Millisecond,
		"ключ выдан на проверку без чекпойнта")
	assert.Equal(t, status{queue: 1}, v.status())

	followCkpt(t, db)
	vr.until(func(s status) bool { return s.Matched == 1 }, "ключ проверен после появления чекпойнта")
	rep, err := vr.stop()
	require.NoError(t, err)
	assert.Equal(t, counts{Checked: 1, Matched: 1}, rep.counts)
}

// Чекпойнт цели записан репликатором другой пары баз — сверка
// останавливается, а не сверяет с чужой позицией.
func TestIntegrationVerifyOtherPair(t *testing.T) {
	db := prepare(t)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	since := srcNow(t)
	_, err := dstClient.Database(db).Collection(replicator.CheckpointColl).InsertOne(ctx, bson.M{
		"_id": "checkpoint", "token": bson.M{"_data": "00"}, "clusterTime": since, "db": "other", "dst_db": db,
	})
	require.NoError(t, err)

	_, err = startVerify(t, itConfig(db, since)).wait()

	assert.ErrorIs(t, err, replicator.ErrDBMismatch)
}

// Файл позиции занят другим процессом сверки — отказ до открытия потока.
func TestIntegrationVerifyLocked(t *testing.T) {
	db := prepare(t)
	cfg := itConfig(db, srcNow(t))
	cfg.StateDir = t.TempDir()
	files, _, err := positionPaths(cfg.StateDir, db, dstURI)
	require.NoError(t, err)
	unlock, err := lock(files.lock)
	require.NoError(t, err)
	defer unlock()

	_, err = startVerify(t, cfg).wait()

	assert.ErrorIs(t, err, ErrLocked)
}

// rename в окне: ключ переносится под новое имя и проверяется там —
// испорченный на цели документ в переименованной коллекции — дефект.
func TestIntegrationVerifyRename(t *testing.T) {
	db := prepare(t)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	since := srcNow(t)
	insert(ctx, t, srcClient, db, doc("k", "a"))
	require.NoError(t, srcClient.Database("admin").RunCommand(ctx, bson.D{
		{Key: "renameCollection", Value: db + ".items"}, {Key: "to", Value: db + ".moved"},
	}).Err())
	_, err := dstClient.Database(db).Collection("moved").InsertOne(ctx, doc("k", "b"))
	require.NoError(t, err)
	followCkpt(t, db)
	cfg := itConfig(db, since)
	cfg.UntilCaughtUp = true
	cfg.StateDir = t.TempDir()

	rep, err := startVerify(t, cfg).wait()

	require.NoError(t, err)
	assert.Equal(t, int64(1), rep.Defects, "дефектов после rename")
	got, err := readReport(rep.DefectsFile)
	require.NoError(t, err)
	src, dst := `{"_id":"k","v":"a"}`, `{"_id":"k","v":"b"}`
	assert.Equal(t, report{{NS: db + ".moved", ID: json.RawMessage(`"k"`), Reason: "различается: v", Src: &src, Dst: &dst}}, got)
}

// drop в окне: ждавшие ключи удалённой коллекции — «не проверено» с причиной
// drop, а не дефект.
func TestIntegrationVerifyDrop(t *testing.T) {
	db := prepare(t)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	since := srcNow(t)
	insert(ctx, t, srcClient, db, doc("k", "a"))
	require.NoError(t, srcClient.Database(db).Collection("items").Drop(ctx))
	writeCkpt(t, db, since) // ключ не готов: он ждёт в очереди, когда приходит drop
	tickSrc(t)
	cfg := itConfig(db, since)
	cfg.StateDir = t.TempDir()
	vr := startVerify(t, cfg)
	vr.until(func(s status) bool { return s.Unverified == 1 }, "ключ удалённой коллекции не проверен")

	rep, err := vr.stop()

	require.NoError(t, err)
	assert.Equal(t, counts{Unverified: 1}, rep.counts)
	assert.Zero(t, rep.Queue, "ключей в очереди при выходе")
	got, err := readReport(rep.UnverifiedFile)
	require.NoError(t, err)
	assert.Equal(t, report{{NS: db + ".items", ID: json.RawMessage(`"k"`), Reason: reasonDrop}}, got)
}

// Остановленная сверка продолжается с файла позиции: ключи из очереди проверены
// по одному разу, событие между запусками прочитано с сохранённого токена,
// --since не нужна.
func TestIntegrationVerifyResume(t *testing.T) {
	db := prepare(t)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	since := srcNow(t)
	insert(ctx, t, srcClient, db, doc(1, "a"), doc(2, "a"), doc(3, "a"))
	insert(ctx, t, dstClient, db, doc(1, "a"), doc(2, "a"), doc(3, "a"))
	writeCkpt(t, db, since) // ключи не готовы: очередь не пустеет
	tickSrc(t)
	cfg := itConfig(db, since)
	cfg.StateDir = t.TempDir()
	first := startVerify(t, cfg)
	first.until(func(s status) bool { return s.queue == 3 }, "три ключа в очереди")
	rep, err := first.stop()
	require.NoError(t, err)
	require.Equal(t, 3, rep.Queue, "ключей в очереди при остановке")
	insert(ctx, t, srcClient, db, doc(4, "a"))
	insert(ctx, t, dstClient, db, doc(4, "a"))
	writeCkpt(t, db, srcNow(t)) // до запуска: иначе поток, восстановленный из позиции, уже «догнал» прежний чекпойнт
	followCkpt(t, db)
	cfg.Since = 0
	cfg.UntilCaughtUp = true

	rep, err = startVerify(t, cfg).wait()

	require.NoError(t, err)
	assert.Equal(t, counts{Checked: 4, Matched: 4}, rep.counts)
	assert.Equal(t, 0, rep.Queue, "ключей в очереди при выходе")
}

// startReplicator запускает настоящий репликатор одноимённой пары баз db с
// since; cleanup отменяет его, ждёт возврата и снимает аренду.
func startReplicator(t *testing.T, db string, since bson.Timestamp) {
	t.Helper()
	r := replicator.New(srcClient, dstClient, replicator.Config{StartTS: since.T, BatchSize: 100, DB: db, DstDB: db})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(waitTimeout):
			assert.Fail(t, "репликатор не остановился")
		}
		rctx, rcancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer rcancel()
		assert.NoError(t, r.Release(rctx))
	})
}

// waitApplied ждёт, пока позиция чекпойнта db станет строго позже ts.
func waitApplied(t *testing.T, db string, ts bson.Timestamp) {
	t.Helper()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		pos, ok, err := replicator.AppliedPosition(ctx, dstClient, db, db)
		if !assert.NoError(c, err) || !assert.True(c, ok, "позиция чекпойнта есть") {
			return
		}
		assert.True(c, pos.After(ts), "позиция %v не позже %v", pos, ts)
	}, waitTimeout, 100*time.Millisecond, "репликатор применил запись")
}

// startWriter по кругу обновляет документы db.items с _id 0..49 (upsert), пока
// не вызван stop (и до конца теста).
func startWriter(t *testing.T, db string) (stop func()) {
	t.Helper()
	coll := srcClient.Database(db).Collection("items")
	quit, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		tick := time.NewTicker(10 * time.Millisecond) // темп нагрузки: ~100 записей в секунду
		defer tick.Stop()
		for n := 0; ; n++ {
			select {
			case <-quit:
				return
			case <-tick.C:
			}
			ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
			_, err := coll.UpdateOne(ctx, bson.M{"_id": n % 50}, bson.M{"$set": bson.M{"n": n}}, options.UpdateOne().SetUpsert(true))
			cancel()
			if !assert.NoError(t, err, "запись писателя") {
				return
			}
		}
	}()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			close(quit)
			<-done
		})
	}
	t.Cleanup(stop)
	return stop
}

// Сквозной: настоящий репликатор при живых записях — сверка до догона без
// дефектов; затем испорченный на цели документ — дефект при продолжении.
func TestIntegrationVerifyReplicator(t *testing.T) {
	db := prepare(t)
	ctx, cancel := context.WithTimeout(context.Background(), runTimeout)
	defer cancel()
	since := srcNow(t)
	startReplicator(t, db, since)
	stopWriter := startWriter(t, db)
	tickSrc(t)
	cfg := itConfig(db, since)
	cfg.UntilCaughtUp = true
	cfg.StateDir = t.TempDir()

	rep, err := startVerify(t, cfg).wait()

	require.NoError(t, err)
	assert.Positive(t, rep.Checked, "ключей проверено при живых записях")
	assert.Zero(t, rep.Defects, "дефектов при живых записях")

	stopWriter()
	tk := srcAt(ctx, t, db, func(ctx context.Context, c *mongo.Collection) error {
		_, insErr := c.InsertOne(ctx, doc("k", "a"))
		return insErr
	})
	// строго за tk: позиция, равная tk, не значит, что k применён, и
	// --until-caught-up не стал бы его ждать
	waitApplied(t, db, tk)
	setV(ctx, t, dstClient, db, "k", "b")

	rep, err = startVerify(t, cfg).wait()

	require.NoError(t, err)
	assert.Equal(t, int64(1), rep.Defects, "дефектов после порчи документа k")
	got, err := readReport(rep.DefectsFile)
	require.NoError(t, err)
	src, dst := `{"_id":"k","v":"a"}`, `{"_id":"k","v":"b"}`
	assert.Equal(t, report{{NS: db + ".items", ID: json.RawMessage(`"k"`), Reason: "различается: v", Src: &src, Dst: &dst}}, got)
}
