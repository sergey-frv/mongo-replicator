// Package replicator — односторонняя репликация MongoDB через change streams.
//
// Назначение: догонять целевой кластер более старой версии (например, 5.0)
// до источника более новой (например, 8.0), когда oplog replay, mongorestore
// и mongosync неприменимы из-за разницы версий.
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
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// ErrNoStartPoint — на цели нет чекпойнта и StartTS не задан: неизвестно, откуда читать поток.
var ErrNoStartPoint = errors.New("чекпойнта нет и start-ts не задан: неизвестно, откуда читать")

// ErrDBMismatch — чекпойнт на цели записан для другой базы, чем задана: продолжать с него значило бы тихо разойтись с источником.
var ErrDBMismatch = errors.New("чекпойнт записан для другой базы")

// ErrStreamInvalidated — поток закрыт сервером событием invalidate (dropDatabase
// отслеживаемой базы): продолжить его нельзя, токен invalidate не сохраняется.
var ErrStreamInvalidated = errors.New("поток инвалидирован (dropDatabase базы)")

// SystemDBs — служебные базы, которые не реплицируются.
var SystemDBs = []string{"admin", "config", "local", ckptDB}

// Config — параметры репликации.
type Config struct {
	StartTS   uint32 // unix-сек, старт при отсутствии чекпойнта
	BatchSize int    // максимум операций в одной пачке записи
	DB        string // база источника
}

// Replicator переносит изменения из src в dst.
type Replicator struct {
	src, dst *mongo.Client
	cfg      Config

	pending batch
	saved   bson.Raw // последний записанный в чекпойнт токен
	applied atomic.Int64
	lastTS  bson.Timestamp // clusterTime последнего прочитанного события

	appliedTS bson.Timestamp // время последнего сохранённого токена: всё до него на цели

	// лог прогресса переживает перезапуски Run: при ретраях лаг тоже виден
	lastLog time.Time
	lastN   int64
}

// New создаёт репликатор. dst должен быть подключён с w:majority,
// чтобы чекпойнт не обогнал данные при failover.
func New(src, dst *mongo.Client, cfg Config) *Replicator {
	return &Replicator{src: src, dst: dst, cfg: cfg, lastLog: time.Now()}
}

// Applied возвращает число применённых на цели DML-операций.
func (r *Replicator) Applied() int64 { return r.applied.Load() }

// flush применяет накопленные операции на цели.
func (r *Replicator) flush(ctx context.Context) error {
	for _, ns := range r.pending.order {
		_, err := r.dst.Database(ns.DB).Collection(ns.Coll).
			BulkWrite(ctx, r.pending.ops[ns], options.BulkWrite().SetOrdered(true))
		if err != nil {
			return fmt.Errorf("bulk write в %s: %w", ns, err)
		}
	}
	r.applied.Add(int64(r.pending.n))
	r.pending = batch{}
	return nil
}

// checkDB сверяет базу чекпойнта с заданной.
func checkDB(saved, want string) error {
	if saved != want {
		return fmt.Errorf("%w: в чекпойнте %q, задано %q", ErrDBMismatch, saved, want)
	}
	return nil
}

// Run открывает поток и работает до ошибки или отмены контекста.
// Несохранённый хвост прошлого запуска отбрасывается: его перечитаем с чекпойнта.
func (r *Replicator) Run(ctx context.Context) error {
	if r.cfg.DB == "" {
		return errors.New("не задана база источника")
	}
	r.pending = batch{}
	// и при ошибке: если Run раз за разом падает, лаг в логе всё равно растёт
	defer r.logProgress(ctx)

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
		if dbErr := checkDB(ck.DB, r.cfg.DB); dbErr != nil {
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

	// Поток уровня базы требует прав только на неё.
	log.Info().Str("db", r.cfg.DB).Msg("открываю поток уровня базы")
	cs, err := r.src.Database(r.cfg.DB).Watch(ctx, mongo.Pipeline{}, opts)
	if err != nil {
		return fmt.Errorf("открытие change stream: %w", err)
	}
	defer cs.Close(context.Background()) //nolint:errcheck // поток закрывается при выходе, ошибка не важна

	var lastToken bson.Raw
	commit := func() error {
		if err := r.flush(ctx); err != nil {
			return err
		}
		return r.saveToken(ctx, lastToken)
	}

	for {
		if cs.TryNext(ctx) {
			var ev event
			if err := cs.Decode(&ev); err != nil {
				return fmt.Errorf("разбор события: %w", err)
			}
			r.lastTS = ev.ClusterTime

			switch ev.OperationType {
			case "insert", "replace", "update", "delete":
				if m, ok := toModel(&ev); ok {
					r.pending.add(ev.NS, m)
				}
				lastToken = ev.ID
				if r.pending.n >= r.cfg.BatchSize {
					if err := commit(); err != nil {
						return err
					}
				}
			case "invalidate":
				// Всё до invalidate, включая применённый dropDatabase, уже на цели.
				// Его токен не сохраняем: resumeAfter с ним сервер отвергает (260).
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
				lastToken = ev.ID
				if err := r.saveToken(ctx, lastToken); err != nil {
					return err
				}
			}
		} else {
			if err := cs.Err(); err != nil {
				return err
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
	r.lastLog, r.lastN = time.Now(), n
	e := log.Info().Int64("applied", n).
		Float64("rate", math.Round(float64(n-prevN)/elapsed.Seconds()*10)/10)
	// ещё ничего не сохранено — лаг неизвестен, ping не нужен
	if !r.appliedTS.IsZero() {
		e = e.Time("applied_at", time.Unix(int64(r.appliedTS.T), 0).UTC())
		if now, err := r.srcTime(ctx); err != nil {
			log.Debug().Err(err).Msg("ping источника: лаг неизвестен")
		} else {
			lag := lagOf(now, r.appliedTS)
			e = e.Str("lag", lag.String()).Int64("lag_sec", int64(lag/time.Second))
		}
	}
	e.Msg("прогресс")
}
