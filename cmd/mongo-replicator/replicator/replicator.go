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
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// ErrNoStartPoint — на цели нет чекпойнта и StartTS не задан: неизвестно, откуда читать поток.
var ErrNoStartPoint = errors.New("чекпойнта нет и start-ts не задан: неизвестно, откуда читать")

// SystemDBs — служебные базы, которые не реплицируются.
var SystemDBs = []string{"admin", "config", "local", ckptDB}

// Config — параметры репликации.
type Config struct {
	StartTS   uint32 // unix-сек, старт при отсутствии чекпойнта
	BatchSize int    // максимум операций в одной пачке записи
	DB        string // "" — все базы, кроме служебных
}

// Replicator переносит изменения из src в dst.
type Replicator struct {
	src, dst *mongo.Client
	cfg      Config

	pending batch
	saved   bson.Raw // последний записанный в чекпойнт токен
	applied atomic.Int64
	lastTS  uint32 // clusterTime последнего прочитанного события
}

// New создаёт репликатор. dst должен быть подключён с w:majority,
// чтобы чекпойнт не обогнал данные при failover.
func New(src, dst *mongo.Client, cfg Config) *Replicator {
	return &Replicator{src: src, dst: dst, cfg: cfg}
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

// Run открывает поток и работает до ошибки или отмены контекста.
// Несохранённый хвост прошлого запуска отбрасывается: его перечитаем с чекпойнта.
func (r *Replicator) Run(ctx context.Context) error {
	r.pending = batch{}

	opts := options.ChangeStream().
		SetFullDocument(options.UpdateLookup).
		SetShowExpandedEvents(true).
		SetBatchSize(int32(r.cfg.BatchSize)). //nolint:gosec // main ограничивает BatchSize диапазоном int32
		SetMaxAwaitTime(time.Second)

	token, err := r.loadToken(ctx)
	switch {
	case err != nil:
		return fmt.Errorf("чтение чекпойнта: %w", err)
	case token != nil:
		opts.SetResumeAfter(token)
		log.Info().Msg("продолжаю с сохранённого resume token")
	case r.cfg.StartTS > 0:
		opts.SetStartAtOperationTime(&bson.Timestamp{T: r.cfg.StartTS})
		log.Info().Time("from", time.Unix(int64(r.cfg.StartTS), 0).UTC()).Msg("чекпойнта нет, стартую с start-ts")
	default:
		return ErrNoStartPoint
	}

	// Служебные базы не реплицируем.
	pipeline := mongo.Pipeline{{{Key: "$match", Value: bson.M{
		"ns.db": bson.M{"$nin": SystemDBs},
	}}}}

	cs, err := r.src.Watch(ctx, pipeline, opts)
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

	lastLog := time.Now()
	for {
		if cs.TryNext(ctx) {
			var ev event
			if err := cs.Decode(&ev); err != nil {
				return fmt.Errorf("разбор события: %w", err)
			}
			r.lastTS = ev.ClusterTime.T

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

		if time.Since(lastLog) >= 10*time.Second {
			lag := time.Duration(0)
			if r.lastTS > 0 {
				lag = time.Since(time.Unix(int64(r.lastTS), 0)).Round(time.Second)
			}
			log.Info().Int64("applied", r.applied.Load()).Str("lag", lag.String()).Msg("прогресс")
			lastLog = time.Now()
		}
	}
}
