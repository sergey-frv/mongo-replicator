// Package replicator — односторонняя репликация MongoDB через change streams.
//
// Назначение: миграция и непрерывная репликация одной базы между кластерами
// (в т.ч. одной версии, например 8.0 → 8.0), когда oplog replay, mongorestore
// и mongosync неприменимы.
//
// Гарантии: at-least-once + идемпотентные операции (replace-upsert и delete
// по _id). После сбоя поток продолжается с последнего сохранённого resume
// token; повторно применённые события дают то же итоговое состояние.
package replicator

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// ErrNoStartPoint — на цели нет чекпойнта и StartTS не задан: неизвестно, откуда читать поток.
var ErrNoStartPoint = errors.New("чекпойнта нет и start-ts не задан: неизвестно, откуда читать")

// ErrDBMismatch — чекпойнт базы цели записан для другой базы источника, чем задана: продолжать с него значило бы тихо разойтись с источником.
var ErrDBMismatch = errors.New("чекпойнт записан для другой пары баз")

// ErrStreamInvalidated — поток закрыт сервером событием invalidate: продолжить его
// нельзя, токен invalidate не сохраняется. В потоке базы invalidate идёт за dropDatabase,
// на котором репликатор останавливается раньше (*DropError).
var ErrStreamInvalidated = errors.New("поток инвалидирован (dropDatabase базы)")

// ErrForeignNamespace — в потоке уровня базы событие другой базы: так не бывает,
// но применять его на цели значило бы писать не в ту базу.
var ErrForeignNamespace = errors.New("событие другой базы в потоке")

// ErrReservedCollection — на источнике коллекция переименована в служебное имя ckptColl
// или из него: на цели под этим именем лежит чекпойнт, применять такой rename нельзя.
var ErrReservedCollection = errors.New("rename в служебную коллекцию " + ckptColl + " или из неё")

// streamPipeline отсекает события служебной коллекции ckptColl (у источника она есть,
// если источник был целью при обратной репликации) и убирает из событий updateDescription:
// репликатору нужен только fullDocument, а с обоими событие большого документа превышает
// 16 МБ (BSONObjectTooLarge). rename проходит всегда: rename в служебное имя и из него
// останавливает applyDDL. События без ns.coll (dropDatabase, invalidate) $ne пропускает.
var streamPipeline = mongo.Pipeline{
	{{Key: "$match", Value: bson.D{{Key: "$or", Value: bson.A{
		bson.D{{Key: "ns.coll", Value: bson.D{{Key: "$ne", Value: ckptColl}}}},
		bson.D{{Key: "operationType", Value: "rename"}},
	}}}}},
	{{Key: "$unset", Value: "updateDescription"}},
}

// SystemDBs — служебные базы, которые не реплицируются.
var SystemDBs = []string{"admin", "config", "local"}

// Config — параметры репликации.
type Config struct {
	StartTS    uint32   // unix-сек, старт при отсутствии чекпойнта
	BatchSize  int      // максимум операций в одной пачке записи
	DB         string   // база источника
	DstDB      string   // база цели; New: пусто — как DB
	ApplyDrops []string // namespace источника (<DB>.<коллекция>), drop которых разрешён
	Traffic    *Traffic // счётчики байт клиентов (их диалеры); nil — трафик не учитывается
	StateFile  string   // файл состояния процесса на машине (StatePath); пусто — не пишется
	StateDir   string   // каталог файлов состояния для сводки по машине; пусто — сводки нет
	Cluster    string   // ключ кластера цели (StatePath): сверяется с файлом состояния
}

// Replicator переносит изменения из src в dst.
type Replicator struct {
	src, dst *mongo.Client
	cfg      Config

	// разрешённые drop: коллекция вычёркивается сразу после применения её drop.
	// Если чекпойнт за drop не записался, повтор Run снова получит этот drop,
	// но коллекции на цели уже нет — он пропускается веткой «на цели нет»
	applyDrops map[string]bool

	pending batch
	saved   bson.Raw // последний записанный в чекпойнт токен
	applied atomic.Int64
	lastTS  bson.Timestamp // clusterTime последнего прочитанного события

	appliedTS bson.Timestamp // время последнего сохранённого токена: всё до него на цели

	// лог прогресса переживает перезапуски Run: при ретраях лаг тоже виден
	lastLog     time.Time
	lastN       int64
	lastTraffic TrafficInfo

	owner      string        // владелец аренды базы цели (newOwner): один на экземпляр, и для повторных Run
	leaseTTL   time.Duration // срок аренды
	leaseRenew time.Duration // интервал продления
	// отправка последнего успешного захвата или продления; nil — аренда не захвачена.
	// time.Time с монотонными показаниями: перевод часов хоста не сдвигает проверку срока
	leaseOK atomic.Pointer[time.Time]

	// файл состояния (Config.StateFile): читается один раз за процесс, пишет его
	// горутина-писатель от загрузки до Release
	stateLoaded bool          // загрузка была; повторный Run её не повторяет
	stateBase   TrafficInfo   // накопленное прошлыми процессами
	stateEvery  time.Duration // интервал записи
	stateStop   chan struct{} // остановка писателя; nil — писатель не запущен
	stateDone   chan struct{} // закрывается, когда писатель вышел

	machineEvery time.Duration // интервал сводки трафика на машине
	lastMachine  time.Time     // время последней сводки

	mu sync.Mutex // st: читает Status из другой горутины
	st Status     // без Applied и Traffic: их Status берёт из счётчиков

	ddlHook func() // тесты: вызывается в начале applyDDL
}

// New создаёт репликатор. dst должен быть подключён с w:majority,
// чтобы чекпойнт не обогнал данные при failover.
func New(src, dst *mongo.Client, cfg Config) *Replicator {
	if cfg.DstDB == "" {
		cfg.DstDB = cfg.DB
	}
	applyDrops := make(map[string]bool, len(cfg.ApplyDrops))
	for _, ns := range cfg.ApplyDrops {
		applyDrops[ns] = true
	}
	return &Replicator{
		src: src, dst: dst, cfg: cfg, applyDrops: applyDrops,
		lastLog: time.Now(), lastTraffic: cfg.Traffic.Snapshot(),
		owner: newOwner(), leaseTTL: defaultLeaseTTL, leaseRenew: defaultLeaseRenew,
		stateEvery: defaultStateEvery, machineEvery: defaultMachineEvery,
		st: Status{DB: cfg.DB, DstDB: cfg.DstDB, Started: time.Now(), Lag: -1, State: StateStarting},
	}
}

// Applied возвращает число применённых на цели DML-операций.
func (r *Replicator) Applied() int64 { return r.applied.Load() }

// flush применяет накопленные операции на цели.
func (r *Replicator) flush(ctx context.Context) error {
	for _, ns := range r.pending.order {
		// перед каждой коллекцией: пачка по многим коллекциям пишется долго
		if err := r.checkLease(); err != nil {
			return err
		}
		_, err := r.dst.Database(r.cfg.DstDB).Collection(ns.Coll).
			BulkWrite(ctx, r.pending.ops[ns], options.BulkWrite().SetOrdered(true))
		if err != nil {
			return fmt.Errorf("bulk write в %s: %w", r.nsName(ns), err)
		}
	}
	r.applied.Add(int64(r.pending.n))
	if bi := batchInfo(&r.pending, time.Now()); bi != nil {
		r.update(func(s *Status) { s.LastBatch = bi })
	}
	r.pending = batch{}
	return nil
}

// dstNS — namespace цели для коллекции источника.
func (r *Replicator) dstNS(ns namespace) namespace {
	return namespace{DB: r.cfg.DstDB, Coll: ns.Coll}
}

// nsName — ns источника для ошибок, при другой базе цели — с ns цели.
func (r *Replicator) nsName(ns namespace) string {
	if r.cfg.DstDB == r.cfg.DB {
		return ns.String()
	}
	return ns.String() + " (на цели " + r.dstNS(ns).String() + ")"
}

// checkNS проверяет, что событие относится к базе источника. У invalidate ns нет.
func checkNS(cfg Config, ev *event) error {
	if ev.OperationType == "invalidate" {
		return nil
	}
	if ev.NS.DB != cfg.DB {
		return fmt.Errorf("%w: %s %s, поток базы %q", ErrForeignNamespace, ev.OperationType, ev.NS, cfg.DB)
	}
	if ev.To != nil && ev.To.DB != cfg.DB {
		return fmt.Errorf("%w: %s %s → %s, поток базы %q", ErrForeignNamespace, ev.OperationType, ev.NS, ev.To, cfg.DB)
	}
	return nil
}

// dbPair — база источника и база цели.
type dbPair struct{ src, dst string }

// checkDBs сверяет пару баз чекпойнта с заданной.
func checkDBs(saved, want dbPair) error {
	if saved != want {
		return fmt.Errorf("%w: в чекпойнте %q → %q, задано %q → %q",
			ErrDBMismatch, saved.src, saved.dst, want.src, want.dst)
	}
	return nil
}

// Run захватывает аренду базы цели, открывает поток и работает до ошибки или отмены
// контекста. Аренда продлевается в своей горутине; потерянная аренда останавливает Run.
// По возврату аренда не снимается (повторный Run её подхватит): снять — Release.
// Несохранённый хвост прошлого запуска отбрасывается: его перечитаем с чекпойнта.
func (r *Replicator) Run(ctx context.Context) (retErr error) {
	if r.cfg.DB == "" {
		return errors.New("не задана база источника")
	}
	// «остановлен»: при временной ошибке main сразу сменит на «повтор» (SetRetry)
	defer func() { r.stopped(ctx, retErr) }()
	r.pending = batch{}
	// и при ошибке: если Run раз за разом падает, лаг в логе всё равно растёт
	defer r.logProgress(ctx)

	acquired := time.Now()
	if err := r.acquireLease(ctx); err != nil {
		return err
	}
	r.leaseOK.Store(&acquired)
	runCtx, cancel := context.WithCancelCause(ctx)
	renewed := make(chan struct{})
	go func() {
		defer close(renewed)
		r.renewLease(runCtx, cancel)
	}()
	// и при панике: горутина продления не переживает Run
	defer func() {
		cancel(nil)
		<-renewed
	}()

	err := r.stream(runCtx)
	// Run отменён арендой, а не вызывающим: причина важнее ошибки отменённой операции
	if cause := context.Cause(runCtx); cause != nil && ctx.Err() == nil {
		return cause
	}
	return err
}

// stream читает поток с чекпойнта (или StartTS) и применяет события на цели.
func (r *Replicator) stream(ctx context.Context) error {
	opts := options.ChangeStream().
		SetFullDocument(options.UpdateLookup).
		SetShowExpandedEvents(true).
		SetBatchSize(int32(r.cfg.BatchSize)). //nolint:gosec // main ограничивает BatchSize диапазоном int32
		SetMaxAwaitTime(time.Second)

	ck, err := r.loadCheckpoint(ctx)
	switch {
	case err != nil:
		return fmt.Errorf("чтение чекпойнта: %w", err)
	case ck.Token != nil:
		if dbErr := checkDBs(ck.dbs(), dbPair{src: r.cfg.DB, dst: r.cfg.DstDB}); dbErr != nil {
			return dbErr
		}
		opts.SetResumeAfter(ck.Token)
		if r.appliedTS.IsZero() {
			// после рестарта лаг известен сразу, ещё до первого сохранения
			r.appliedTS = ck.appliedAt()
		}
		log.Info().Msg("продолжаю с сохранённого resume token")
	case r.cfg.StartTS > 0:
		opts.SetStartAtOperationTime(&bson.Timestamp{T: r.cfg.StartTS})
		log.Info().Time("from", time.Unix(int64(r.cfg.StartTS), 0).UTC()).Msg("чекпойнта нет, стартую с start-ts")
	default:
		return ErrNoStartPoint
	}
	// после проверки пары баз: с неверным --db файл состояния не трогается
	r.startState()

	// Поток уровня базы требует прав только на неё.
	log.Info().Str("db", r.cfg.DB).Msg("открываю поток уровня базы")
	cs, err := r.src.Database(r.cfg.DB).Watch(ctx, streamPipeline, opts)
	if err != nil {
		return fmt.Errorf("открытие change stream: %w", err)
	}
	defer cs.Close(context.Background()) //nolint:errcheck // поток закрывается при выходе, ошибка не важна
	r.streamOpened()

	var lastToken bson.Raw
	commit := func() error {
		// и на простое: пустой flush аренду не проверяет, а чекпойнт сдвигается
		if err := r.checkLease(); err != nil {
			return err
		}
		if err := r.flush(ctx); err != nil {
			return err
		}
		if err := r.saveToken(ctx, lastToken); err != nil {
			return fmt.Errorf("сохранение чекпойнта: %w", err)
		}
		return nil
	}

	for {
		if cs.TryNext(ctx) {
			var ev event
			if err := cs.Decode(&ev); err != nil {
				return fmt.Errorf("разбор события: %w", err)
			}
			r.lastTS = ev.ClusterTime
			if err := checkNS(r.cfg, &ev); err != nil {
				return err
			}

			switch ev.OperationType {
			case "insert", "replace", "update", "delete":
				if m, ok := toModel(&ev); ok {
					r.pending.add(ev.NS, ev.OperationType, m)
				}
				lastToken = ev.ID
				// Пачка пишется по размеру или когда вычитан ответ сервера: при ровном
				// потоке чаще maxAwaitTime пустых getMore не бывает, и без второго
				// условия цель отставала бы до BatchSize событий.
				if r.pending.n >= r.cfg.BatchSize || cs.RemainingBatchLength() == 0 {
					if err := commit(); err != nil {
						return err
					}
				}
			case "invalidate":
				// В потоке базы invalidate идёт за dropDatabase, а на нём (и на drop
				// коллекций перед ним) репликатор уже остановился: ветка — на случай иного
				// источника invalidate. Его токен не сохраняем: resumeAfter с ним сервер отвергает (260).
				if err := commit(); err != nil {
					return err
				}
				return ErrStreamInvalidated
			default:
				// DDL: сначала всё накопленное, затем само изменение схемы,
				// и сразу чекпойнт — повтор rename/drop после рестарта опасен.
				if err := commit(); err != nil {
					return err
				}
				if err := r.applyDDL(ctx, &ev); err != nil {
					return err
				}
				// разрешение одноразовое: вычёркиваем сразу, до чекпойнта (см. applyDrops)
				if ev.OperationType == "drop" {
					delete(r.applyDrops, ev.NS.String())
				}
				lastToken = ev.ID
				if err := r.saveToken(ctx, lastToken); err != nil {
					return fmt.Errorf("сохранение чекпойнта: %w", err)
				}
			}
		} else {
			if err := cs.Err(); err != nil {
				return fmt.Errorf("change stream: %w", err)
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// Пачка с сервера вычитана. Токен потока двигается и на «пустых»
			// getMore, поэтому чекпойнт не устаревает, даже когда событий нет.
			if t := cs.ResumeToken(); t != nil {
				lastToken = t
			}
			if err := commit(); err != nil {
				return err
			}
		}

		r.logProgress(ctx)
		r.logMachine(ctx)
	}
}

// logProgress раз в 10 с пишет прогресс и лаг: применённая позиция против текущего
// времени источника. Обе величины — время кластера источника, часы репликатора не участвуют.
func (r *Replicator) logProgress(ctx context.Context) {
	elapsed := time.Since(r.lastLog)
	if elapsed < 10*time.Second || ctx.Err() != nil {
		return
	}
	n, prevN := r.applied.Load(), r.lastN
	tr, prevTr := r.cfg.Traffic.Snapshot(), r.lastTraffic
	r.lastLog, r.lastN, r.lastTraffic = time.Now(), n, tr
	rate := math.Round(float64(n-prevN)/elapsed.Seconds()*10) / 10
	e := log.Info().Int64("applied", n).Float64("rate", rate)
	if r.cfg.Traffic != nil {
		e = trafficFields(e, tr, prevTr, elapsed)
	}
	var appliedAt time.Time
	lag := time.Duration(-1)
	// ещё ничего не сохранено — лаг неизвестен, ping не нужен
	if !r.appliedTS.IsZero() {
		appliedAt = time.Unix(int64(r.appliedTS.T), 0).UTC()
		e = e.Time("applied_at", appliedAt)
		if now, err := r.srcTime(ctx); err != nil {
			log.Debug().Err(err).Msg("ping источника: лаг неизвестен")
		} else {
			lag = lagOf(now, r.appliedTS)
			e = e.Str("lag", lag.String()).Int64("lag_sec", int64(lag/time.Second))
		}
	}
	r.update(func(s *Status) { s.Rate, s.AppliedAt, s.Lag = rate, appliedAt, lag })
	e.Msg("прогресс")
}
