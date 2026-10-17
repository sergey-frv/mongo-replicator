package verify

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"math"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"github.com/sergey-frv/mongo-replicator/internal/replicator"
	"github.com/sergey-frv/mongo-replicator/internal/statefile"
)

const (
	// maxInflight — пачек на проверке одновременно: пока одни ждут чекпойнт,
	// ограничитель выдаёт маркеры S1 следующим.
	maxInflight = 8
	pollEvery   = time.Second      // опрос чекпойнта
	logEvery    = 10 * time.Second // лог прогресса и время источника для лага
	saveEvery   = 10 * time.Second // запись файла позиции
	warnEvery   = time.Minute      // WARN «чекпойнта нет» и ошибки его чтения
	retryPause  = 5 * time.Second  // пауза выдачи пачек после ошибки чтения
	readTimeout = 30 * time.Second // одно чтение пачки или чекпойнта
	// finalTimeout — итоговая сверка схемы, в том числе после отмены Run
	finalTimeout = 30 * time.Second
)

var (
	// ErrNoSince — файла позиции нет, а начало сверки не задано.
	ErrNoSince = errors.New("файла позиции сверки нет и --since не задана")
	// ErrHistoryLost — позиция потока сверки выпала из окна oplog источника.
	ErrHistoryLost = errors.New("поток сверки выпал из окна oplog")
)

// maxTS — время позже любого события: так помечается окончившийся поток.
var maxTS = bson.Timestamp{T: math.MaxUint32, I: math.MaxUint32}

// Config — параметры сверки.
type Config struct {
	DB            string  // база источника
	DstDB         string  // база цели
	Since         uint32  // unix-сек, начало сверки, когда файла позиции нет; 0 — не задано
	Rate          int     // максимум чтений документов источника в секунду, >= 1
	Sample        float64 // доля ключей (0, 1]
	UntilCaughtUp bool    // закончить, когда проверено всё до позиции чекпойнта, которую догнал поток
	// StateDir — каталог файлов состояния: позиция, блокировка и отчёты — в его
	// подкаталоге verify/; пусто — без них
	StateDir   string
	SrcURI     string // URI источника как написан: ключ кластера в файле позиции
	DstURI     string // URI цели как написан: имя файла позиции и ключ кластера
	IDs        string // файл отчёта: перепроверить его ключи и выйти, без файла позиции
	SchemaOnly bool   // сверить только схему и выйти
}

// Report — итог сверки. Счётчики накоплены по всем запускам с одной позицией.
type Report struct {
	counts
	Queue          int          // ключей осталось непроверенными в очереди
	Schema         []SchemaDiff // расхождения схемы при выходе
	DefectsFile    string       // отчёт о дефектах; пусто — не записан
	UnverifiedFile string       // отчёт о непроверенных; пусто — не записан
}

// Clean — дефектов нет ни в документах, ни в схеме (известные ограничения и
// «не проверено» допускаются).
func (r Report) Clean() bool {
	return r.Defects == 0 && !slices.ContainsFunc(r.Schema, func(d SchemaDiff) bool { return !d.Known })
}

// Verifier сверяет с целью документы, изменённые на источнике, и схему базы. На
// цель не пишет и аренду не берёт: читает только чекпойнт репликатора.
type Verifier struct {
	src, dst *mongo.Client
	cfg      Config
	lim      *limiter

	ckpt   *mark                          // позиция чекпойнта: применено всё строго до неё
	stream *mark                          // поток сверки: доставлены все события строго до неё
	srcNow atomic.Pointer[bson.Timestamp] // время источника — для лага в логе; nil — ещё не прочитано
	wake   chan struct{}

	// состояние цикла: им владеет горутина Run
	files      paths
	ident      ident
	since      uint32
	q          *queue
	counts     counts
	created    map[string]bool
	defects    report
	unverified report
	reported   int            // записей в отчётах при последней записи; -1 — не записывались
	token      bson.Raw       // токен потока для файла позиции
	streamTime bson.Timestamp // время потока для файла позиции
	ended      bool           // поток окончен (dropDatabase, invalidate)
	target     bson.Timestamp // --until-caught-up: проверить ключи строго до него; ноль — ещё не задан

	mu   sync.Mutex
	stat status
}

// status — снимок цикла для тестов.
type status struct {
	counts
	queue       int
	inflight    int // пачек на проверке
	ckptWaiting int // из них прочитали S1 и ждут чекпойнт
}

// New — сверка источника src с целью dst; Run — одна на экземпляр.
func New(src, dst *mongo.Client, cfg Config) *Verifier {
	wake := make(chan struct{}, 1)
	return &Verifier{
		src: src, dst: dst, cfg: cfg,
		lim:  newLimiter(cfg.Rate, time.Now, time.After),
		ckpt: newMark(wake), stream: newMark(wake), wake: wake,
		q:        newQueue(cfg.Rate),
		created:  map[string]bool{},
		reported: -1,
	}
}

// Run выполняет сверку в режиме из Config. Отмена ctx после начала цикла
// сверки — штатное завершение: позиция и отчёты записываются, ошибки нет; до
// него (схема, открытие потока) итога нет — ошибка. Исключение — --ids с
// непроверенными ключами: позиции у неё нет, итога тоже — ошибка, ключи — в
// отчёте «не проверено» с причиной stopped.
func (v *Verifier) Run(ctx context.Context) (Report, error) {
	switch {
	case v.cfg.SchemaOnly:
		return v.runSchema(ctx)
	case v.cfg.IDs != "":
		return v.runIDs(ctx)
	}
	return v.runStream(ctx)
}

// runSchema — только сверка схемы.
func (v *Verifier) runSchema(ctx context.Context) (Report, error) {
	diffs, err := checkSchema(ctx, v.src, v.dst, v.cfg.DB, v.cfg.DstDB, nil)
	if err != nil {
		return Report{}, err
	}
	logSchema(diffs)
	rep := Report{Schema: diffs}
	logReport(rep)
	return rep, nil
}

// runStream — сверка ключей из потока изменений с файлом позиции.
func (v *Verifier) runStream(ctx context.Context) (Report, error) {
	pos, unlock, err := v.openPosition()
	if err != nil {
		return Report{}, err
	}
	defer unlock()
	if pos == nil {
		if v.cfg.Since == 0 {
			return Report{}, ErrNoSince
		}
		pos = &position{ident: v.ident, Since: int64(v.cfg.Since)}
	}
	v.restore(pos)

	diffs, err := checkSchema(ctx, v.src, v.dst, v.cfg.DB, v.cfg.DstDB, v.created)
	if err != nil {
		return Report{}, err
	}
	logSchema(diffs)

	cs, err := v.openStream(ctx, v.token, v.since)
	if err != nil {
		return Report{}, err
	}
	err = v.loop(ctx, cs)
	v.save()
	if err != nil {
		return v.report(diffs), err
	}

	fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), finalTimeout)
	defer cancel()
	// итоговая схема не прочитана — ошибка: без неё код 0 был бы по схеме на старте
	final, err := checkSchema(fctx, v.src, v.dst, v.cfg.DB, v.cfg.DstDB, v.created)
	if err == nil {
		diffs = final
		logSchema(diffs)
	} else {
		err = fmt.Errorf("итоговая сверка схемы: %w", err)
	}
	rep := v.report(diffs)
	logReport(rep)
	return rep, err
}

// openPosition берёт блокировку и читает файл позиции; без StateDir — без них.
// nil — позиции нет.
func (v *Verifier) openPosition() (_ *position, unlock func(), err error) {
	v.ident = ident{DB: v.cfg.DB, DstDB: v.cfg.DstDB, SrcCluster: statefile.ClusterKey(v.cfg.SrcURI)}
	if v.cfg.StateDir == "" {
		return nil, func() {}, nil
	}
	if v.files, v.ident.Cluster, err = positionPaths(v.cfg.StateDir, v.cfg.DstDB, v.cfg.DstURI); err != nil {
		return nil, nil, err
	}
	if unlock, err = lock(v.files.lock); err != nil {
		return nil, nil, err
	}
	pos, err := loadPosition(v.files.position, v.ident, v.cfg.Since != 0)
	if err != nil {
		unlock()
		return nil, nil, err
	}
	if pos == nil {
		return nil, unlock, nil
	}
	if v.cfg.Since != 0 {
		log.Warn().Str("file", v.files.position).Msg("--since игнорируется: сверка продолжается с файла позиции")
	}
	// отчёты пишутся целиком: прошлые записи — в начало
	if v.defects, err = loadReport(v.files.defects, pos.Counts.Defects, v.files.position); err == nil {
		v.unverified, err = loadReport(v.files.unverified, pos.Counts.Unverified, v.files.position)
	}
	if err != nil {
		unlock()
		return nil, nil, err
	}
	log.Info().Str("file", v.files.position).Int("pending", len(pos.Pending)).Msg("продолжаю сверку с файла позиции")
	return pos, unlock, nil
}

// loadReport — отчёт прошлого запуска; counted — его записи по счётчику
// позиции posPath. Отчёты пишутся до позиции, поэтому в файле не меньше
// min(counted, reportLimit) записей; меньше (файла нет, он битый или усечён) —
// ошибка errReportIncomplete, файл не трогается: новый отчёт без этих записей
// навсегда оставил бы их ключи без --ids. Битый при counted == 0 (позиция его
// записей не учла) переносится в сторону (statefile.MoveAside) с WARN и
// считается пустым. Не прочитанный по другой причине или не перенесённый —
// ошибка.
func loadReport(path string, counted int64, posPath string) (report, error) {
	r, err := readReport(path)
	switch {
	case errors.Is(err, errReportCorrupt) && counted == 0:
		to, mvErr := statefile.MoveAside(path, "corrupt")
		if mvErr != nil {
			return nil, fmt.Errorf("%w, но не перенесён: %w", err, mvErr)
		}
		log.Warn().Err(err).Str("file", path).Str("moved_to", to).
			Msg("отчёт прошлого запуска сверки повреждён: перенесён в сторону, новый начат пустым")
		return nil, nil
	case err == nil:
		err = fmt.Errorf("%s: записей %d", path, len(r))
	case !errors.Is(err, fs.ErrNotExist) && !errors.Is(err, errReportCorrupt):
		return nil, err
	}
	if want := min(counted, reportLimit); int64(len(r)) < want {
		return nil, fmt.Errorf("%w (%d): %w — верните файл или начните заново, удалив %s",
			errReportIncomplete, want, err, posPath)
	}
	return r, nil
}

// restore переносит позицию в состояние цикла.
func (v *Verifier) restore(pos *position) {
	v.since = uint32(pos.Since) //nolint:gosec // записано из uint32
	v.token = pos.Token
	v.streamTime = pos.StreamTime
	if v.streamTime.IsZero() {
		v.streamTime = bson.Timestamp{T: v.since}
	}
	v.stream.set(v.streamTime)
	v.counts = pos.Counts
	for _, it := range pos.Pending {
		v.q.requeue(keyOf(it.ns, it.id), it)
	}
	for _, c := range pos.Created {
		v.created[c] = true
	}
}

// runIDs перепроверяет ключи из файла отчёта. Поток открывается с текущего
// момента и учитывает события только этих ключей: без них вердикт не видел бы
// изменений в окне (ABA) и давал бы ложный дефект.
func (v *Verifier) runIDs(ctx context.Context) (Report, error) {
	entries, err := readReport(v.cfg.IDs)
	if err != nil {
		return Report{}, err
	}
	for _, e := range entries {
		id, idErr := e.id()
		if idErr != nil {
			return Report{}, idErr
		}
		if !strings.HasPrefix(e.NS, v.cfg.DB+".") {
			return Report{}, fmt.Errorf("ключ %s в %s — не из базы %s", e.NS, v.cfg.IDs, v.cfg.DB)
		}
		v.q.add(e.NS, id, bson.Timestamp{})
	}
	if v.cfg.StateDir != "" {
		// свои файлы отчёта: отчёты основной сверки не затираются
		if v.files, _, err = positionPaths(v.cfg.StateDir, v.cfg.DstDB, v.cfg.DstURI); err != nil {
			return Report{}, err
		}
		v.files.defects = strings.TrimSuffix(v.files.defects, ".defects.json") + ".ids.defects.json"
		v.files.unverified = strings.TrimSuffix(v.files.unverified, ".unverified.json") + ".ids.unverified.json"
	}
	log.Info().Str("file", v.cfg.IDs).Int("keys", v.q.len()).Msg("перепроверяю ключи из файла")

	cs, err := v.openStream(ctx, nil, 0)
	if err != nil {
		return Report{}, err
	}
	err = v.loop(ctx, cs)
	left := v.q.len()
	if err == nil && ctx.Err() != nil && left > 0 {
		// у --ids нет файла позиции: непроверенные ключи — в отчёт, иначе они
		// пропали бы (файл --ids мог быть и прошлым отчётом .ids, его затрёт запись)
		v.stopped()
		err = fmt.Errorf("перепроверка остановлена, ключей не проверено: %d: %w", left, ctx.Err())
	}
	v.writeReports()
	rep := v.report(nil)
	if err == nil {
		logReport(rep)
	}
	return rep, err
}

// stopped переносит ключи очереди в «не проверено» с причиной reasonStopped:
// одна строка лога вместо WARN на ключ.
func (v *Verifier) stopped() {
	items := v.q.snapshot()
	for _, it := range items {
		v.counts.Unverified++
		if err := v.unverified.add(it.ns, it.id, reasonStopped, nil, nil); err != nil {
			log.Warn().Err(err).Msg("непроверенный ключ не добавлен в отчёт")
		}
	}
	log.Warn().Int("keys", len(items)).Str("reason", reasonStopped).Msg("не проверено")
}

// loop — цикл сверки: события потока → очередь, готовые пачки → проверка,
// вердикты → счётчики и отчёты. Выход: отмена ctx (без ошибки), окончание
// режима или перманентная ошибка.
func (v *Verifier) loop(ctx context.Context, cs *mongo.ChangeStream) error {
	sctx, stopStream := context.WithCancel(ctx)
	events := make(chan streamMsg)
	streamDone := make(chan struct{})
	go func() {
		defer close(streamDone)
		readStream(sctx, cs, events)
	}()
	pollErr := make(chan error, 1)
	pollDone := make(chan struct{})
	go func() {
		defer close(pollDone)
		v.pollCkpt(sctx, pollErr)
	}()
	defer func() {
		stopStream()
		<-streamDone
		<-pollDone
		_ = cs.Close(context.Background())
	}()

	wctx, stopWork := context.WithCancel(ctx)
	results := make(chan checked, maxInflight)
	inflight := 0
	defer func() {
		stopWork()
		for ; inflight > 0; inflight-- {
			v.drained(<-results)
		}
		v.publish(0)
	}()

	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	var pauseUntil time.Time
	lastLog, lastSave := time.Now(), time.Now()
	for {
		if pos, ok := v.ckpt.get(); ok && !time.Now().Before(pauseUntil) {
			for inflight < maxInflight {
				b, ready := v.q.next(pos)
				if !ready {
					break
				}
				inflight++
				go func() { results <- v.check(wctx, b) }()
			}
		}
		if v.finished(inflight) {
			return nil
		}
		v.publish(inflight)

		// при полной очереди поток не читается, пока проверке не нужно его время
		var in <-chan streamMsg
		if !v.ended && (!v.q.full() || v.stream.waiting() > 0) {
			in = events
		}
		select {
		case <-ctx.Done():
			return nil
		case err := <-pollErr:
			return err
		case m := <-in:
			if err := v.onStream(m); err != nil {
				if ctx.Err() != nil {
					return nil // ошибка потока из-за отмены — остановка, а не сбой
				}
				return err
			}
		case r := <-results:
			inflight--
			if r.err != nil {
				log.Warn().Err(r.err).Str("ns", r.b.ns).Msg("пачка сверки не проверена, повторю")
				pauseUntil = time.Now().Add(retryPause)
				v.retry(r.b)
				continue
			}
			v.judge(r)
		case <-v.wake:
		case now := <-tick.C:
			if now.Sub(lastLog) >= logEvery {
				v.logProgress()
				lastLog = now
			}
			if now.Sub(lastSave) >= saveEvery {
				v.save()
				lastSave = now
			}
		}
	}
}

// finished — режим выполнен: в --ids и после окончания потока — очередь пуста;
// в --until-caught-up — проверены все ключи строго до позиции чекпойнта, которую
// догнал поток сверки.
func (v *Verifier) finished(inflight int) bool {
	ids := v.cfg.IDs != ""
	if (ids || v.ended) && v.q.len() == 0 && inflight == 0 {
		return true
	}
	if !v.cfg.UntilCaughtUp || ids {
		return false
	}
	if v.target.IsZero() {
		ck, ok := v.ckpt.get()
		if !ok || v.streamTime.Before(ck) {
			return false
		}
		v.target = ck
		log.Info().Uint32("ckpt_t", ck.T).Uint32("ckpt_i", ck.I).
			Msg("поток сверки догнал чекпойнт: дожидаюсь проверки ключей до него")
	}
	return !v.q.anyBefore(v.target)
}

// checked — результат проверки пачки.
type checked struct {
	b         batch
	s1, d, s2 map[string]bson.Raw // по idKey
	t1, t2    bson.Timestamp
	err       error
}

// check проверяет пачку: S1 → чекпойнт строго за T1 → D → S2 → поток сверки
// строго за T2. Вердикт выносит цикл: ему нужны события ключей из потока.
func (v *Verifier) check(ctx context.Context, b batch) checked {
	r := checked{b: b}
	coll := strings.TrimPrefix(b.ns, v.cfg.DB+".")
	ids := make([]bson.RawValue, len(b.items))
	for i, it := range b.items {
		ids[i] = it.id
	}
	if r.err = v.lim.wait(ctx, len(ids)); r.err != nil {
		return r
	}
	if r.s1, r.t1, r.err = readDocs(ctx, v.src, v.cfg.DB, coll, ids); r.err != nil {
		r.err = fmt.Errorf("чтение источника: %w", r.err)
		return r
	}
	if r.err = v.ckpt.waitAfter(ctx, r.t1); r.err != nil {
		return r
	}
	if r.d, _, r.err = readDocs(ctx, v.dst, v.cfg.DstDB, coll, ids); r.err != nil {
		r.err = fmt.Errorf("чтение цели: %w", r.err)
		return r
	}
	if r.err = v.lim.wait(ctx, len(ids)); r.err != nil {
		return r
	}
	if r.s2, r.t2, r.err = readDocs(ctx, v.src, v.cfg.DB, coll, ids); r.err != nil {
		r.err = fmt.Errorf("чтение источника: %w", r.err)
		return r
	}
	r.err = v.stream.waitAfter(ctx, r.t2)
	return r
}

// readDocs читает документы ids коллекции db.coll с primary с read concern
// majority. Документы — по idKey: при collation коллекции не simple $in по
// строке вернёт и документы с «равными» _id, сопоставление — по точным байтам.
// Время — operationTime последнего ответа (сессия без causal consistency).
func readDocs(ctx context.Context, c *mongo.Client, db, coll string, ids []bson.RawValue) (map[string]bson.Raw, bson.Timestamp, error) {
	ctx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()
	sess, err := c.StartSession(options.Session().SetCausalConsistency(false))
	if err != nil {
		return nil, bson.Timestamp{}, err
	}
	defer sess.EndSession(context.WithoutCancel(ctx))
	sctx := mongo.NewSessionContext(ctx, sess)
	cl := c.Database(db).Collection(coll, options.Collection().
		SetReadPreference(readpref.Primary()).
		SetReadConcern(readconcern.Majority()))
	cur, err := cl.Find(sctx, bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}})
	if err != nil {
		return nil, bson.Timestamp{}, err
	}
	defer cur.Close(context.WithoutCancel(ctx)) //nolint:errcheck // курсор вычитан или чтение уже с ошибкой
	docs := make(map[string]bson.Raw, len(ids))
	for cur.Next(sctx) {
		doc := bson.Raw(bytes.Clone(cur.Current))
		docs[idKey(doc.Lookup("_id"))] = doc
	}
	if err := cur.Err(); err != nil {
		return nil, bson.Timestamp{}, err
	}
	ot := sess.OperationTime()
	if ot == nil {
		return nil, bson.Timestamp{}, errors.New("в ответе нет operationTime")
	}
	return docs, *ot, nil
}

// judge выносит вердикты пачки.
func (v *Verifier) judge(r checked) {
	for _, it := range r.b.items {
		if it.dropped {
			v.addUnverified(it.ns, it.id, v.q.done(it, hot, r.t1), nil, nil)
			continue
		}
		k := idKey(it.id)
		s1, d, s2 := r.s1[k], r.d[k], r.s2[k]
		res := verdict(s1, d, s2, v.q.eventIn(it, r.t1, r.t2))
		v.counts.Checked++
		switch res {
		case matched:
			v.counts.Matched++
		case defect:
			v.addDefect(r.b.ns, it.id, s1, d)
		case hot:
		}
		if reason := v.q.done(it, res, r.t1); reason != "" {
			v.addUnverified(it.ns, it.id, reason, s2, d)
		}
	}
}

// retry возвращает ключи пачки, не проверенной из-за ошибки, в очередь.
func (v *Verifier) retry(b batch) {
	for _, it := range b.items {
		if reason := v.q.back(it); reason != "" {
			v.addUnverified(it.ns, it.id, reason, nil, nil)
		}
	}
}

// drained — пачка, прерванная выходом из цикла: её ключи остаются в очереди
// (snapshot сохранит их ждущими), ключи удалённой коллекции — «не проверены».
func (v *Verifier) drained(r checked) {
	for _, it := range r.b.items {
		if it.dropped {
			v.addUnverified(it.ns, it.id, reasonDrop, nil, nil)
		}
	}
}

func (v *Verifier) addDefect(ns string, id bson.RawValue, src, dst bson.Raw) {
	v.counts.Defects++
	reason, diff := defectReason(src, dst)
	log.Error().Str("ns", ns).Str("id", idString(id)).Strs("diff", diff).Str("reason", reason).Msg("дефект")
	if err := v.defects.add(ns, id, reason, src, dst); err != nil {
		log.Warn().Err(err).Msg("дефект не добавлен в отчёт")
	}
}

func (v *Verifier) addUnverified(ns string, id bson.RawValue, reason string, src, dst bson.Raw) {
	v.counts.Unverified++
	log.Warn().Str("ns", ns).Str("id", idString(id)).Str("reason", reason).Msg("не проверено")
	if err := v.unverified.add(ns, id, reason, src, dst); err != nil {
		log.Warn().Err(err).Msg("непроверенный ключ не добавлен в отчёт")
	}
}

// defectReason — причина дефекта для отчёта и различающиеся поля для лога.
func defectReason(src, dst bson.Raw) (reason string, diff []string) {
	switch {
	case dst == nil:
		return "нет на цели", nil
	case src == nil:
		return "лишний на цели", nil
	}
	diff = diffFields(src, dst)
	if len(diff) == 0 {
		return "различается порядок полей", nil
	}
	return "различается: " + strings.Join(diff, ", "), diff
}

// idString — _id в canonical extended JSON для лога.
func idString(id bson.RawValue) string {
	b, err := valueJSON(id)
	if err != nil {
		return id.String()
	}
	return string(b)
}

// save пишет отчёты, затем файл позиции: позиция не должна учитывать записи,
// которых нет в отчётах (иначе после kill они остались бы только в счётчиках,
// и --ids их не перепроверил бы).
func (v *Verifier) save() {
	if v.writeReports() {
		v.savePosition()
	}
}

// savePosition пишет файл позиции; ошибка — WARN: потеря позиции значит
// повторный просмотр, а не потерю расхождений.
func (v *Verifier) savePosition() {
	if v.files.position == "" || v.cfg.IDs != "" {
		return
	}
	p := &position{
		ident: v.ident, Since: int64(v.since), Token: v.token, StreamTime: v.streamTime,
		Pending: v.q.snapshot(), Created: slices.Sorted(maps.Keys(v.created)), Counts: v.counts,
	}
	if err := writePosition(v.files.position, p); err != nil {
		log.Warn().Err(err).Msg("позиция сверки не записана")
	}
}

// writeReports пишет отчёты, если записи добавились с прошлой записи; первая
// запись — всегда: она заменяет отчёты прошлого запуска. false — отчёт не
// записан: позицию писать нельзя, следующая запись повторит оба файла.
func (v *Verifier) writeReports() bool {
	n := len(v.defects) + len(v.unverified)
	if v.files.defects == "" || n == v.reported {
		return true
	}
	ok := true
	for path, r := range map[string]report{v.files.defects: v.defects, v.files.unverified: v.unverified} {
		if err := writeReport(path, r); err != nil {
			log.Warn().Err(err).Msg("отчёт сверки не записан")
			ok = false
		}
	}
	if ok {
		v.reported = n
	}
	return ok
}

func (v *Verifier) report(schema []SchemaDiff) Report {
	return Report{
		counts: v.counts, Queue: v.q.len(), Schema: schema,
		DefectsFile: v.files.defects, UnverifiedFile: v.files.unverified,
	}
}

// publish обновляет снимок для тестов.
func (v *Verifier) publish(inflight int) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.stat = status{counts: v.counts, queue: v.q.len(), inflight: inflight}
}

func (v *Verifier) status() status {
	v.mu.Lock()
	s := v.stat
	v.mu.Unlock()
	s.ckptWaiting = v.ckpt.waiting()
	return s
}

// pollCkpt раз в pollEvery читает позицию чекпойнта, раз в logEvery — время
// источника, до отмены ctx. Чекпойнт другой пары баз — перманентная ошибка в errc.
func (v *Verifier) pollCkpt(ctx context.Context, errc chan<- error) {
	t := time.NewTicker(pollEvery)
	defer t.Stop()
	var lastWarn, lastSrc time.Time
	for {
		pos, ok, err := v.readCkpt(ctx)
		now := time.Now()
		switch {
		case errors.Is(err, replicator.ErrDBMismatch):
			errc <- err
			return
		case err != nil && ctx.Err() == nil && now.Sub(lastWarn) >= warnEvery:
			log.Warn().Err(err).Msg("чекпойнт не прочитан, повторяю")
			lastWarn = now
		case err == nil && !ok && now.Sub(lastWarn) >= warnEvery:
			log.Warn().Str("db", v.cfg.DstDB).
				Msg("чекпойнта на цели ещё нет: жду репликатор (проверьте, что он запущен на эту базу цели)")
			lastWarn = now
		case ok:
			v.ckpt.set(pos)
		}
		if now.Sub(lastSrc) >= logEvery {
			if ts, srcErr := srcTime(ctx, v.src); srcErr == nil {
				v.srcNow.Store(&ts)
				lastSrc = now
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (v *Verifier) readCkpt(ctx context.Context) (bson.Timestamp, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()
	return replicator.AppliedPosition(ctx, v.dst, v.cfg.DB, v.cfg.DstDB)
}

// srcTime — текущее время кластера: operationTime ответа ping (без привилегий).
func srcTime(ctx context.Context, c *mongo.Client) (bson.Timestamp, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	raw, err := c.Database("admin").RunCommand(ctx, bson.D{{Key: "ping", Value: 1}}).Raw()
	if err != nil {
		return bson.Timestamp{}, err
	}
	t, i, ok := raw.Lookup("operationTime").TimestampOK()
	if !ok {
		return bson.Timestamp{}, errors.New("в ответе ping нет operationTime")
	}
	return bson.Timestamp{T: t, I: i}, nil
}

func (v *Verifier) logProgress() {
	e := log.Info().
		Int64("checked", v.counts.Checked).Int64("matched", v.counts.Matched).
		Int64("defects", v.counts.Defects).Int64("unverified", v.counts.Unverified).
		Int("queue", v.q.len())
	if now := v.srcNow.Load(); now != nil {
		e = e.Dur("stream_lag", lagOf(*now, v.streamTime))
	}
	if ck, ok := v.ckpt.get(); ok {
		e = e.Dur("ckpt_lag", lagOf(v.streamTime, ck))
	}
	e.Msg("сверка")
}

// lagOf — на сколько секунд behind отстаёт от ahead; не отстаёт — 0.
func lagOf(ahead, behind bson.Timestamp) time.Duration {
	if ahead.T <= behind.T {
		return 0
	}
	return time.Duration(ahead.T-behind.T) * time.Second
}

// logSchema пишет расхождения схемы: дефекты — ERROR, известные ограничения — WARN.
func logSchema(diffs []SchemaDiff) {
	for _, d := range diffs {
		if d.Known {
			log.Warn().Str("ns", d.NS).Str("reason", d.Reason).Msg("схема: известное ограничение")
			continue
		}
		log.Error().Str("ns", d.NS).Str("reason", d.Reason).Msg("схема: расхождение")
	}
}

func logReport(r Report) {
	schema := 0
	for _, d := range r.Schema {
		if !d.Known {
			schema++
		}
	}
	log.Info().
		Int64("checked", r.Checked).Int64("matched", r.Matched).Int64("defects", r.Defects).
		Int64("unverified", r.Unverified).Int64("sampled_out", r.SampledOut).Int("queue", r.Queue).
		Int("schema_defects", schema).
		Str("defects_file", r.DefectsFile).Str("unverified_file", r.UnverifiedFile).
		Msg("итог сверки")
	if r.Defects > 0 && r.DefectsFile != "" {
		log.Error().Msg("есть дефекты; перепроверьте: mongo-replicator verify --ids " + r.DefectsFile)
	}
}

// mark — монотонная отметка времени, которую можно ждать.
type mark struct {
	mu      sync.Mutex
	ts      bson.Timestamp
	ok      bool
	ch      chan struct{} // закрывается при сдвиге
	waiters atomic.Int32
	wake    chan<- struct{} // будит цикл при сдвиге и при новом ждущем
}

func newMark(wake chan<- struct{}) *mark { return &mark{ch: make(chan struct{}), wake: wake} }

// set сдвигает отметку вперёд; назад не двигает.
func (m *mark) set(ts bson.Timestamp) {
	m.mu.Lock()
	if m.ok && !ts.After(m.ts) {
		m.mu.Unlock()
		return
	}
	m.ts, m.ok = ts, true
	close(m.ch)
	m.ch = make(chan struct{})
	m.mu.Unlock()
	m.poke()
}

func (m *mark) get() (bson.Timestamp, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ts, m.ok
}

// waitAfter ждёт, пока отметка станет строго позже t.
func (m *mark) waitAfter(ctx context.Context, t bson.Timestamp) error {
	m.waiters.Add(1)
	defer m.waiters.Add(-1)
	m.poke()
	for {
		m.mu.Lock()
		if m.ok && m.ts.After(t) {
			m.mu.Unlock()
			return nil
		}
		ch := m.ch
		m.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// waiting — сколько горутин ждут отметку.
func (m *mark) waiting() int { return int(m.waiters.Load()) }

func (m *mark) poke() {
	select {
	case m.wake <- struct{}{}:
	default:
	}
}
