//go:build integration

package replicator

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	toxiproxy "github.com/Shopify/toxiproxy/v2/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

// netEnv — клиенты сетевого теста через прокси src и dst.
type netEnv struct {
	t        *testing.T
	src, dst *mongo.Client
	traffic  *Traffic
}

// netCase готовит сетевой тест: клиенты через Toxiproxy с диалерами Traffic.
// Вызывать до start: cleanup (сеть восстановлена, токсики сняты, клиенты закрыты)
// выполнится после остановки репликатора. Без Toxiproxy тест пропускается.
func netCase(t *testing.T) *netEnv {
	t.Helper()
	if srcProxy == nil {
		t.Skip("IT_PROXY_API не задан: сетевые тесты запускаются через make test-integration")
	}
	resetProxies(t) // остатки упавшего теста
	nc := &netEnv{t: t, traffic: &Traffic{}}
	var err error
	nc.src, err = connectIT(options.Client().ApplyURI(srcProxyURI).
		SetAppName("cs-replicator").SetDialer(nc.traffic.SrcDialer()))
	require.NoError(t, err)
	nc.dst, err = connectIT(options.Client().ApplyURI(dstProxyURI).
		SetAppName("cs-replicator").SetWriteConcern(writeconcern.Majority()).
		SetDialer(nc.traffic.DstDialer()))
	require.NoError(t, err)
	t.Cleanup(func() {
		resetProxies(t)
		_ = nc.src.Disconnect(context.Background())
		_ = nc.dst.Disconnect(context.Background())
	})
	return nc
}

// resetProxies включает прокси и снимает все токсики.
func resetProxies(t *testing.T) {
	t.Helper()
	for _, p := range []*toxiproxy.Proxy{srcProxy, dstProxy} {
		require.NoError(t, p.Enable(), p.Name)
		toxics, err := p.Toxics()
		require.NoError(t, err, p.Name)
		for _, tx := range toxics {
			require.NoError(t, p.RemoveToxic(tx.Name), p.Name)
		}
	}
}

// newIT — репликатор на клиентах через прокси с короткой арендой; трафик считает nc.traffic.
func (nc *netEnv) newIT(cfg Config) *Replicator {
	cfg.Traffic = nc.traffic
	r := New(nc.src, nc.dst, cfg)
	r.leaseTTL, r.leaseRenew, r.stateEvery = itLeaseTTL, itLeaseRenew, itStateEvery
	return r
}

// Через прокси репликация работает: DML доезжает, трафик идёт через диалеры клиентов прокси.
func TestIntegrationNetProxy(t *testing.T) {
	db := prepare(t)
	nc := netCase(t)
	rn := startOn(t, nc.newIT(Config{StartTS: srcNow(t), BatchSize: 1000, DB: db}))

	size := insertPadded(t, db, 20)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	items := srcClient.Database(db).Collection("items")
	_, err := items.UpdateOne(ctx, bson.M{"_id": 1}, bson.D{{Key: "$set", Value: bson.D{{Key: "pad", Value: "y"}}}})
	require.NoError(t, err)
	_, err = items.DeleteOne(ctx, bson.M{"_id": 2})
	require.NoError(t, err)
	rn.waitSame(db)

	got := nc.traffic.Snapshot()
	assert.GreaterOrEqual(t, got.SrcIn, size, "поток отдал все документы через прокси")
	assert.GreaterOrEqual(t, got.DstOut, size, "запись отправила все документы через прокси")
	assert.Positive(t, got.SrcOut, "запросы к источнику через прокси")
	assert.Positive(t, got.DstIn, "ответы цели через прокси")
}

// netPause — пауза повтора в сетевых тестах (в main — retryTimeout, 5 с).
const netPause = 200 * time.Millisecond

// retrier — репликатор в горутине с повторами Run после ошибок, как replicate в main.
// Сетевые тесты допускают временные ошибки Run и ждут схождения через обрыв.
type retrier struct {
	t    *testing.T
	r    *Replicator
	stop func()

	mu   sync.Mutex
	errs []error // ошибки Run, кроме отмены остановкой
}

// startRetry запускает цикл повторов и регистрирует остановку (отмена, ожидание,
// Release) в t.Cleanup.
func startRetry(t *testing.T, r *Replicator) *retrier {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	rt := &retrier{t: t, r: r}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for ctx.Err() == nil {
			err := r.Run(ctx)
			if ctx.Err() != nil {
				return
			}
			rt.mu.Lock()
			rt.errs = append(rt.errs, err)
			rt.mu.Unlock()
			r.SetRetry(err)
			select {
			case <-ctx.Done():
			case <-time.After(netPause):
			}
		}
	}()
	var once sync.Once
	rt.stop = func() {
		once.Do(func() {
			cancel()
			<-done
			release(t, r)
		})
	}
	t.Cleanup(rt.stop)
	return rt
}

// runErrs — ошибки Run на текущий момент.
func (rt *retrier) runErrs() []error {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return slices.Clone(rt.errs)
}

// waitErr ждёт n-ю ошибку Run не дольше timeout и возвращает её.
func (rt *retrier) waitErr(n int, timeout time.Duration) error {
	rt.t.Helper()
	var err error
	require.Eventually(rt.t, func() bool {
		errs := rt.runErrs()
		if len(errs) < n {
			return false
		}
		err = errs[n-1]
		return true
	}, timeout, 50*time.Millisecond, "Run не вернул ошибку за %s", timeout)
	return err
}

// waitSame ждёт совпадения одноимённой базы на кластерах; ошибки Run допустимы.
func (rt *retrier) waitSame(db string) {
	rt.t.Helper()
	require.EventuallyWithT(rt.t, func(c *assert.CollectT) {
		ok, diff := same(db)
		assert.True(c, ok, "база %s не совпала: %s; ошибки Run: %v", db, diff, rt.runErrs())
	}, waitTimeout, pollEvery, "база %s не совпала за %s", db, waitTimeout)
}

// srcWriter вставляет на источнике (напрямую, мимо прокси) документы с _id 0, 1, …
// в db.items, пока его не остановят, и запоминает operationTime каждой вставки.
type srcWriter struct {
	stop func()
	mu   sync.Mutex
	at   []bson.Timestamp // at[i] — время вставки документа _id i
	err  error
}

func startWriter(t *testing.T, db string) *srcWriter {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	w := &srcWriter{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		c := srcClient.Database(db).Collection("items")
		for i := int32(0); ctx.Err() == nil; i++ {
			sess, err := srcClient.StartSession()
			if err == nil {
				_, err = c.InsertOne(mongo.NewSessionContext(ctx, sess), bson.D{{Key: "_id", Value: i}})
			}
			if err != nil {
				if ctx.Err() == nil {
					w.mu.Lock()
					w.err = err
					w.mu.Unlock()
				}
				if sess != nil {
					sess.EndSession(context.Background())
				}
				return
			}
			w.mu.Lock()
			w.at = append(w.at, *sess.OperationTime())
			w.mu.Unlock()
			sess.EndSession(context.Background())
			time.Sleep(20 * time.Millisecond) // темп нагрузки: вставка раз в 20 мс
		}
	}()
	var once sync.Once
	w.stop = func() {
		once.Do(func() {
			cancel()
			<-done
			assert.NoError(t, w.err, "запись на источник")
		})
	}
	t.Cleanup(w.stop)
	return w
}

// times — времена вставок на текущий момент.
func (w *srcWriter) times() []bson.Timestamp {
	w.mu.Lock()
	defer w.mu.Unlock()
	return slices.Clone(w.at)
}

// waitApplied ждёт, пока на цели появится хотя бы n документов db.items.
func waitApplied(t *testing.T, db string, n int64) {
	t.Helper()
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		got, err := dstClient.Database(db).Collection("items").CountDocuments(ctx, bson.M{})
		return err == nil && got >= n
	}, waitTimeout, pollEvery, "на цели нет %d документов %s.items за %s", n, db, waitTimeout)
}

// hang вешает прокси: timeout-токсик (timeout 0 — данные отбрасываются, соединения
// не закрываются) в обе стороны, иначе запросы доходят до сервера без ответа.
func hang(t *testing.T, p *toxiproxy.Proxy) {
	t.Helper()
	for _, stream := range []string{"upstream", "downstream"} {
		_, err := p.AddToxic("hang_"+stream, "timeout", stream, 1, toxiproxy.Attributes{"timeout": 0})
		require.NoError(t, err, p.Name)
	}
}

// Источник отключён дольше serverSelectionTimeoutMS: Run возвращает временную ошибку
// и повторяется, либо драйвер сам переподключает поток. После восстановления —
// база цели совпадает с источником, без потерь и дублей.
func TestIntegrationNetSrcDown(t *testing.T) {
	db := prepare(t)
	nc := netCase(t)
	rt := startRetry(t, nc.newIT(Config{StartTS: srcNow(t), BatchSize: 1000, DB: db}))
	w := startWriter(t, db)
	waitApplied(t, db, 5)

	require.NoError(t, srcProxy.Disable())
	time.Sleep(time.Second) // обрыв доходит до репликатора: ответы в полёте дочитаны
	cut := nc.traffic.Snapshot().SrcIn
	assert.Never(t, func() bool { return nc.traffic.Snapshot().SrcIn != cut }, 3*time.Second, pollEvery,
		"обрыв дошёл до репликатора: с источника ничего не приходит")
	assert.Equal(t, cut, nc.traffic.Snapshot().SrcIn, "обрыв дошёл до репликатора: с источника ничего не приходит")
	require.NoError(t, srcProxy.Enable())
	time.Sleep(time.Second) // длительность записи после восстановления: писатель вставляет ещё секунду
	w.stop()

	rt.waitSame(db)
	t.Logf("ошибки Run: %v", rt.runErrs())
}

// assertCkptBehind проверяет, что чекпойнт не обогнал данные: все документы источника
// со временем вставки не новее clusterTime чекпойнта уже на цели.
func assertCkptBehind(t *testing.T, db string, w *srcWriter) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var ck checkpoint
	err := ckptOf(db).FindOne(ctx, bson.M{"_id": ckptID}).Decode(&ck)
	require.NoError(t, err)
	at := w.times() // после чтения чекпойнта: все вставки, которые он может покрывать
	var want []int32
	for i, ts := range at {
		if !ts.After(ck.ClusterTime) {
			want = append(want, int32(i)) //nolint:gosec // число вставок теста мало
		}
	}
	got, err := dstClient.Database(db).Collection("items").CountDocuments(ctx, bson.M{"_id": bson.M{"$in": want}})
	require.NoError(t, err)
	assert.Equal(t, int64(len(want)), got, "чекпойнт %v впереди данных на цели", ck.ClusterTime)
}

// Цель отключена: записи на источнике идут, Run повторяется; чекпойнт ни в какой
// момент не новее данных на цели, после восстановления — схождение.
func TestIntegrationNetDstDown(t *testing.T) {
	db := prepare(t)
	nc := netCase(t)
	rt := startRetry(t, nc.newIT(Config{StartTS: srcNow(t), BatchSize: 1000, DB: db}))
	w := startWriter(t, db)
	waitApplied(t, db, 5)

	// длительность действия: обрыв держится 4 с, чекпойнт проверяется каждые 500 мс
	// по ходу обрыва и 2 с после восстановления; читается напрямую, мимо прокси
	require.NoError(t, dstProxy.Disable())
	for range 8 {
		time.Sleep(500 * time.Millisecond)
		assertCkptBehind(t, db, w)
	}
	require.NoError(t, dstProxy.Enable())
	for range 4 {
		time.Sleep(500 * time.Millisecond)
		assertCkptBehind(t, db, w)
	}
	w.stop()

	rt.waitSame(db)
	assertCkptBehind(t, db, w)
	assert.NotEmpty(t, rt.runErrs(), "без цели Run не работает дольше 2/3 срока аренды")
	t.Logf("ошибки Run: %v", rt.runErrs())
}

// Источник завис (данные не идут, соединения не рвутся): Run возвращает ошибку,
// когда драйвер по таймаутам heartbeat прерывает соединения; после снятия — повтор
// и схождение. Ожидание — два таймаута heartbeat, 2 · (connectTimeoutMS +
// heartbeatFrequencyMS) из IT_SRC_PROXY_URI ≈ 5 с, плюс выбор сервера при переоткрытии.
func TestIntegrationNetSrcHang(t *testing.T) {
	db := prepare(t)
	nc := netCase(t)
	rt := startRetry(t, nc.newIT(Config{StartTS: srcNow(t), BatchSize: 1000, DB: db}))
	w := startWriter(t, db)
	waitApplied(t, db, 5)

	before := len(rt.runErrs())
	hung := time.Now()
	hang(t, srcProxy)
	err := rt.waitErr(before+1, 15*time.Second)
	t.Logf("Run вернул ошибку через %s: %v", time.Since(hung).Round(100*time.Millisecond), err)
	resetProxies(t)
	w.stop()

	rt.waitSame(db)
}

// Цель зависла дольше 2/3 срока аренды: процесс прекращает работу (errLeaseExpiring)
// раньше, чем аренда на цели истекает; другой процесс захватывает базу только после
// этого, а первый после восстановления сети получает ErrLeaseHeld.
func TestIntegrationNetDstHangLease(t *testing.T) {
	db := prepare(t)
	nc := netCase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*waitTimeout)
	defer cancel()

	cfg := Config{StartTS: srcNow(t), BatchSize: 1000, DB: db}
	r1 := nc.newIT(cfg)
	var (
		r1Err  error
		exitAt time.Time
	)
	r1Done := make(chan struct{})
	go func() {
		defer close(r1Done)
		r1Err = r1.Run(ctx)
		exitAt = time.Now()
	}()
	t.Cleanup(func() {
		cancel()
		<-r1Done
		release(t, r1)
	})
	_, err := srcClient.Database(db).Collection("items").InsertOne(ctx, bson.D{{Key: "_id", Value: int32(1)}})
	require.NoError(t, err)
	waitApplied(t, db, 1)

	// второй процесс напрямую к цели пытается захватить базу, пока первый работает
	r2 := newIT(dstClient, Config{BatchSize: 1000, DB: db})
	hang(t, dstProxy)
	// r1Until — последний срок аренды первого на цели: после зависания продления
	// до сервера не доходят, и срок перестаёт двигаться
	var r1Until, acquiredAt time.Time
	var unexpected error
	require.Eventually(t, func() bool {
		// неожиданная ошибка прекращает ожидание и проверяется после него
		st, ok, err := findLease(db)
		if err != nil {
			unexpected = err
			return true
		}
		if ok && st.Owner == r1.owner {
			r1Until = st.LeaseUntil
		}
		err = r2.acquireLease(ctx)
		if err == nil {
			acquiredAt = time.Now()
			return true
		}
		if !errors.Is(err, ErrLeaseHeld) {
			unexpected = err
			return true
		}
		return false
	}, waitTimeout, 20*time.Millisecond, "второй процесс не захватил аренду за %s", waitTimeout)
	require.NoError(t, unexpected, "захват аренды вторым процессом: ожидалась только ErrLeaseHeld")
	rn2 := startOn(t, r2) // продлевает захваченную аренду

	select {
	case <-r1Done:
	case <-time.After(waitTimeout):
		require.FailNow(t, "первый процесс не остановился")
	}

	require.ErrorIs(t, r1Err, errLeaseExpiring)
	assert.True(t, exitAt.Before(r1Until), "первый остановился в %s, аренда до %s",
		exitAt.Format(time.StampMilli), r1Until.Format(time.StampMilli))
	assert.True(t, exitAt.Before(acquiredAt), "первый остановился в %s, второй захватил в %s",
		exitAt.Format(time.StampMilli), acquiredAt.Format(time.StampMilli))

	resetProxies(t)
	st, _ := readLease(t, db)
	assert.Equal(t, r2.owner, st.Owner, "аренда у второго процесса")

	// после зависания соединения первого восстанавливаются не сразу
	require.Eventually(t, func() bool {
		rctx, rcancel := context.WithTimeout(ctx, 5*time.Second)
		defer rcancel()
		return errors.Is(r1.Run(rctx), ErrLeaseHeld)
	}, waitTimeout, netPause, "первый процесс получает ErrLeaseHeld")
	rn2.waitSame(db)
}

// После обрыва и восстановления сети счётчики трафика растут, а файл состояния
// снова обновляется.
func TestIntegrationNetTraffic(t *testing.T) {
	db := prepare(t)
	nc := netCase(t)
	cfg := withState(Config{StartTS: srcNow(t), BatchSize: 1000, DB: db}, t.TempDir())
	rt := startRetry(t, nc.newIT(cfg))
	w := startWriter(t, db)
	waitApplied(t, db, 5)

	require.NoError(t, srcProxy.Disable())
	require.NoError(t, dstProxy.Disable())
	time.Sleep(3 * time.Second) // длительность обрыва: обе стороны недоступны 3 с
	require.NoError(t, srcProxy.Enable())
	require.NoError(t, dstProxy.Enable())
	restored, before := time.Now(), nc.traffic.Snapshot()
	time.Sleep(time.Second) // длительность записи после восстановления: писатель вставляет ещё секунду
	w.stop()
	rt.waitSame(db)

	after := nc.traffic.Snapshot()
	assert.Greater(t, after.SrcIn, before.SrcIn, "приём с источника после восстановления")
	assert.Greater(t, after.SrcOut, before.SrcOut, "отправка на источник после восстановления")
	assert.Greater(t, after.DstIn, before.DstIn, "приём с цели после восстановления")
	assert.Greater(t, after.DstOut, before.DstOut, "отправка на цель после восстановления")
	require.Eventually(t, func() bool {
		s, err := loadState(cfg.StateFile)
		return err == nil && s.UpdatedAt.After(restored) && s.Traffic.DstOut > before.DstOut
	}, waitTimeout, pollEvery, "файл состояния обновляется после восстановления")
	t.Logf("ошибки Run: %v", rt.runErrs())
}
