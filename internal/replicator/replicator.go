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
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/sergey-frv/mongo-replicator/internal/traffic"
)

// SystemDBs — служебные базы, которые не реплицируются.
var SystemDBs = []string{"admin", "config", "local"}

// Config — параметры репликации.
type Config struct {
	StartTS    uint32            // unix-сек, старт при отсутствии чекпойнта
	BatchSize  int               // максимум операций в одной пачке записи
	DB         string            // база источника
	DstDB      string            // база цели; New: пусто — как DB
	ApplyDrops []string          // namespace источника (<DB>.<коллекция>), drop которых разрешён
	Traffic    *traffic.Counters // счётчики байт клиентов (их диалеры); nil — трафик не учитывается
	StateFile  string            // файл состояния процесса на машине (statefile.Path); пусто — не пишется
	StateDir   string            // каталог файлов состояния для сводки по машине; пусто — сводки нет
	Cluster    string            // ключ кластера цели (statefile.Path): сверяется с файлом состояния
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
	lastTraffic traffic.Info

	owner      string        // владелец аренды базы цели (newOwner): один на экземпляр, и для повторных Run
	leaseTTL   time.Duration // срок аренды
	leaseRenew time.Duration // интервал продления
	// отправка последнего успешного захвата или продления; nil — аренда не захвачена.
	// time.Time с монотонными показаниями: перевод часов хоста не сдвигает проверку срока
	leaseOK atomic.Pointer[time.Time]

	// файл состояния (Config.StateFile): читается один раз за процесс, пишет его
	// горутина-писатель от загрузки до Release
	stateLoaded bool          // загрузка была; повторный Run её не повторяет
	stateBase   traffic.Info  // накопленное прошлыми процессами
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
