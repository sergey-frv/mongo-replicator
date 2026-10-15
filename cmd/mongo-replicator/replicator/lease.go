package replicator

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Аренда базы цели: в документе чекпойнта владелец (owner) и срок (lease_until,
// по часам сервера цели). Второй процесс на ту же базу цели не стартует,
// пока аренда жива; упавший процесс теряет её через leaseTTL.
const (
	defaultLeaseTTL   = 60 * time.Second
	defaultLeaseRenew = 10 * time.Second
)

// ErrLeaseHeld — базу цели реплицирует другой процесс: его аренда ещё не истекла.
var ErrLeaseHeld = errors.New("аренда базы цели занята")

// ErrLeaseLost — аренду забрал другой процесс: продолжать запись — писать вдвоём.
var ErrLeaseLost = errors.New("аренда базы цели потеряна")

// errLeaseExpiring — продление не удаётся дольше 2/3 TTL: запись прекращается до того,
// как аренду сможет забрать другой процесс. Временная: повтор Run снова её захватит.
var errLeaseExpiring = errors.New("аренда не продлевается и скоро истечёт")

// newOwner — владелец аренды на экземпляр Replicator: hostname:pid:<8 hex>.
// Случайная часть различает контейнеры с одинаковыми hostname и PID 1.
func newOwner() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	var b [4]byte
	_, _ = rand.Read(b[:]) // crypto/rand.Read не возвращает ошибку
	return fmt.Sprintf("%s:%d:%s", host, os.Getpid(), hex.EncodeToString(b[:]))
}

func leaseHeldErr(dst, owner string, until time.Time) error {
	return fmt.Errorf("%w: базу %s уже реплицирует %s, аренда до %s",
		ErrLeaseHeld, dst, owner, until.UTC().Format(time.RFC3339))
}

// leaseExpiring сообщает, что с последнего успешного продления прошло больше 2/3 TTL.
func leaseExpiring(lastOK, now time.Time, ttl time.Duration) bool {
	return leaseElapsed(lastOK, now) > ttl*2/3
}

// leaseElapsed — время с последнего успешного продления: большее из монотонного
// и настенного. Монотонные часы Linux стоят во время сна хоста и паузы ВМ, а аренда
// на цели в это время истекает; настенные отстают при переводе часов назад.
// Перевод вперёд даёт лишнюю самоостановку — временную ошибку, не запись вдвоём.
func leaseElapsed(lastOK, now time.Time) time.Duration {
	return max(now.Sub(lastOK), now.Round(0).Sub(lastOK.Round(0)))
}

// leaseUntil — выражение срока аренды: серверное время + TTL. Часы процессов на разных
// хостах могут расходиться, поэтому срок и его проверка — только по $$NOW.
func (r *Replicator) leaseUntil() bson.M {
	return bson.M{"$add": bson.A{"$$NOW", r.leaseTTL.Milliseconds()}}
}

// acquireLease захватывает аренду базы цели: свободную (без владельца), свою или истёкшую.
// $expr в фильтре upsert сервер не принимает, поэтому в два шага: обновление
// существующего документа, затем upsert для отсутствующего. При живой чужой аренде
// upsert вставляет документ с тем же _id и получает E11000.
func (r *Replicator) acquireLease(ctx context.Context) error {
	update := mongo.Pipeline{{{Key: "$set", Value: bson.M{"owner": r.owner, "lease_until": r.leaseUntil()}}}}
	existing := bson.M{"_id": r.cfg.DstDB, "$or": bson.A{
		bson.M{"owner": bson.M{"$exists": false}},
		bson.M{"owner": r.owner},
		bson.M{"$expr": bson.M{"$lt": bson.A{"$lease_until", "$$NOW"}}},
	}}
	free := bson.M{"_id": r.cfg.DstDB, "owner": bson.M{"$exists": false}}
	for retried := false; ; retried = true {
		res, err := r.ckpt().UpdateOne(ctx, existing, update)
		if err != nil {
			return fmt.Errorf("захват аренды: %w", err)
		}
		if res.MatchedCount > 0 {
			return nil
		}
		_, err = r.ckpt().UpdateOne(ctx, free, update, options.UpdateOne().SetUpsert(true))
		if err == nil {
			return nil
		}
		if !mongo.IsDuplicateKeyError(err) {
			return fmt.Errorf("захват аренды: %w", err)
		}
		var cur struct {
			Owner      string    `bson:"owner"`
			LeaseUntil time.Time `bson:"lease_until"`
		}
		findErr := r.ckpt().FindOne(ctx, bson.M{"_id": r.cfg.DstDB}).Decode(&cur)
		switch {
		case findErr != nil && !errors.Is(findErr, mongo.ErrNoDocuments):
			return fmt.Errorf("захват аренды: чтение владельца: %w", findErr)
		case cur.Owner != "":
			return leaseHeldErr(r.cfg.DstDB, cur.Owner, cur.LeaseUntil)
		case retried:
			return fmt.Errorf("захват аренды: %w", err)
		}
		// владелец освободил аренду между запросами — ещё одна попытка
	}
}

// checkLease возвращает errLeaseExpiring, если аренда не захвачена или с последнего
// успешного продления прошло больше 2/3 TTL. Вызывается и перед каждой записью на цель
// (BulkWrite коллекции, DDL): процесс, простоявший дольше срока аренды (SIGSTOP,
// миграция ВМ), не должен успеть записать поверх забравшего аренду, пока горутина
// продления ещё не заметила потерю. Проверка клиентская: остановка между ней и отправкой
// операции — остаточный риск (README), закрыть его может только проверка на сервере.
func (r *Replicator) checkLease() error {
	p := r.leaseOK.Load()
	if p == nil {
		return fmt.Errorf("%w: аренда не захвачена", errLeaseExpiring)
	}
	lastOK, now := *p, time.Now()
	if leaseExpiring(lastOK, now, r.leaseTTL) {
		return fmt.Errorf("%w: последнее продление %s назад", errLeaseExpiring,
			leaseElapsed(lastOK, now).Round(time.Second))
	}
	return nil
}

// renewLease продлевает аренду раз в leaseRenew, пока ctx жив. Работает в своей
// горутине: продление не ждёт долгих операций Run (createIndexes на большой коллекции).
// Аренду забрали — отменяет Run с ErrLeaseLost; продление не удаётся дольше
// 2/3 TTL — с errLeaseExpiring. Успешные продления отмечаются в leaseOK.
func (r *Replicator) renewLease(ctx context.Context, cancel context.CancelCauseFunc) {
	t := time.NewTicker(r.leaseRenew)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		sent := time.Now()
		err := r.renewOnce(ctx)
		switch {
		case err == nil:
			r.leaseOK.Store(&sent)
		case ctx.Err() != nil:
			return
		case errors.Is(err, ErrLeaseLost):
			cancel(err)
			return
		default:
			log.Warn().Err(err).Msg("продление аренды не удалось, повтор")
			if expErr := r.checkLease(); expErr != nil {
				cancel(expErr)
				return
			}
		}
	}
}

// renewOnce — одно продление. Запрос ограничен интервалом продления, чтобы зависшая
// сеть не отложила проверку срока.
func (r *Replicator) renewOnce(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, r.leaseRenew)
	defer cancel()
	res, err := r.ckpt().UpdateOne(ctx,
		bson.M{"_id": r.cfg.DstDB, "owner": r.owner},
		mongo.Pipeline{{{Key: "$set", Value: bson.M{"lease_until": r.leaseUntil()}}}})
	if err != nil {
		return err
	}
	if res.MatchedCount == 0 {
		return r.leaseLostErr()
	}
	return nil
}

func (r *Replicator) leaseLostErr() error {
	return fmt.Errorf("%w: базу %s реплицирует другой процесс (мы — %s)", ErrLeaseLost, r.cfg.DstDB, r.owner)
}

// Release снимает аренду и останавливает писателя файла состояния. Документ без позиции
// (старт не удался: нет --start-ts, неверный --dst-db) удаляется, чтобы не оставлять
// его на цели. Последняя запись файла — только если аренда была нашей: иначе базу
// реплицирует другой процесс, возможно, с этой же машины и в этот же файл.
// Вызывать после возврата Run; повторный вызов и вызов без захвата безвредны.
func (r *Replicator) Release(ctx context.Context) error {
	writer := r.stopState()
	res, err := r.ckpt().DeleteOne(ctx,
		bson.M{"_id": r.cfg.DstDB, "owner": r.owner, "token": bson.M{"$exists": false}})
	if err != nil {
		return fmt.Errorf("снятие аренды: %w", err)
	}
	owned := res.DeletedCount > 0
	if !owned {
		up, err := r.ckpt().UpdateOne(ctx,
			bson.M{"_id": r.cfg.DstDB, "owner": r.owner},
			bson.M{"$unset": bson.M{"owner": "", "lease_until": ""}})
		if err != nil {
			return fmt.Errorf("снятие аренды: %w", err)
		}
		owned = up.MatchedCount > 0
	}
	if writer && owned {
		r.writeStateFile()
	}
	return nil
}
