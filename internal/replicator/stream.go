package replicator

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

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
