package verify

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"github.com/sergey-frv/mongo-replicator/internal/replicator"
)

// streamPipeline — поток сверки: события служебной коллекции чекпойнта
// отсекаются, как у репликатора (rename проходит всегда), а из события остаются
// только ключ, время и пространства имён — документы сверке не нужны, поэтому и
// updateLookup нет.
var streamPipeline = mongo.Pipeline{
	{{Key: "$match", Value: bson.D{{Key: "$or", Value: bson.A{
		bson.D{{Key: "ns.coll", Value: bson.D{{Key: "$ne", Value: replicator.CheckpointColl}}}},
		bson.D{{Key: "operationType", Value: "rename"}},
	}}}}},
	{{Key: "$project", Value: bson.D{
		{Key: "operationType", Value: 1},
		{Key: "ns", Value: 1},
		{Key: "documentKey._id", Value: 1},
		{Key: "clusterTime", Value: 1},
		{Key: "to", Value: 1},
	}}},
}

type namespace struct {
	DB   string `bson:"db"`
	Coll string `bson:"coll"`
}

func (n namespace) String() string { return n.DB + "." + n.Coll }

// streamEvent — поля события потока сверки (streamPipeline).
type streamEvent struct {
	OperationType string         `bson:"operationType"`
	ClusterTime   bson.Timestamp `bson:"clusterTime"`
	NS            namespace      `bson:"ns"`
	To            *namespace     `bson:"to"` // только для rename
	DocumentKey   struct {
		ID bson.RawValue `bson:"_id"`
	} `bson:"documentKey"`
}

// streamMsg — событие или продвижение потока (token — токен после него), либо
// ошибка потока.
type streamMsg struct {
	ev    *streamEvent
	token bson.Raw
	err   error
}

// openStream открывает поток базы источника с primary: с токена, со времени
// since или, если нет обоих, с текущего момента.
func (v *Verifier) openStream(ctx context.Context, token bson.Raw, since uint32) (*mongo.ChangeStream, error) {
	opts := options.ChangeStream().SetShowExpandedEvents(true).SetMaxAwaitTime(time.Second)
	switch {
	case token != nil:
		opts.SetResumeAfter(token)
	case since > 0:
		opts.SetStartAtOperationTime(&bson.Timestamp{T: since})
	}
	db := v.src.Database(v.cfg.DB, options.Database().SetReadPreference(readpref.Primary()))
	cs, err := db.Watch(ctx, streamPipeline, opts)
	if err != nil {
		return nil, v.streamErr(err)
	}
	return cs, nil
}

// streamErr — ошибка потока; выпадение из окна oplog — ErrHistoryLost.
func (v *Verifier) streamErr(err error) error {
	if replicator.IsHistoryLost(err) {
		pos := v.files.position
		if pos == "" {
			pos = "файл позиции"
		}
		return fmt.Errorf("%w: удалите %s и начните с более позднего --since: %w", ErrHistoryLost, pos, err)
	}
	return fmt.Errorf("поток сверки: %w", err)
}

// readStream читает поток и отдаёт в out события и продвижение токена, пока не
// отменён ctx; ошибка потока — последнее сообщение. Поток закрывает вызывающий.
func readStream(ctx context.Context, cs *mongo.ChangeStream, out chan<- streamMsg) {
	for {
		var m streamMsg
		if cs.TryNext(ctx) {
			m.ev = &streamEvent{}
			if err := cs.Decode(m.ev); err != nil {
				m = streamMsg{err: fmt.Errorf("разбор события: %w", err)}
			}
		} else if err := cs.Err(); err != nil {
			m.err = err
		} else if ctx.Err() != nil {
			return
		}
		if m.err == nil {
			m.token = cs.ResumeToken()
		}
		select {
		case out <- m:
		case <-ctx.Done():
			return
		}
		if m.err != nil {
			return
		}
	}
}

// onStream учитывает сообщение потока: событие — в очереди, токен и время — в
// позиции и отметке потока (после события: дождавшиеся отметки проверки видят
// его в очереди).
func (v *Verifier) onStream(m streamMsg) error {
	if m.err != nil {
		return v.streamErr(m.err)
	}
	t := bson.Timestamp{}
	if m.ev != nil {
		v.onEvent(m.ev)
		t = m.ev.ClusterTime
		if m.ev.OperationType == "invalidate" {
			return nil // с токена invalidate поток не продолжить (260)
		}
	}
	if m.token == nil {
		return nil
	}
	v.token = m.token
	if tt, err := replicator.TokenTime(m.token); err == nil {
		t = tt
	}
	if t.After(v.streamTime) {
		v.streamTime = t
		v.stream.set(t)
	}
	return nil
}

// onEvent ставит ключ DML-события в очередь и учитывает DDL.
func (v *Verifier) onEvent(ev *streamEvent) {
	ns := ev.NS.String()
	switch ev.OperationType {
	case "insert", "update", "replace", "delete":
		id := ev.DocumentKey.ID
		switch {
		case v.ids:
			if v.q.has(ns, id) {
				v.q.add(ns, id, ev.ClusterTime)
			}
		case !sampled(id, v.cfg.Sample):
			v.counts.SampledOut++ // события, не ключи: ключи не запоминаются
		default:
			v.q.add(ns, id, ev.ClusterTime)
		}
	case "create":
		v.created[ev.NS.Coll] = true
	case "drop":
		v.dropColl(ns)
	case "rename":
		if ev.To == nil || ev.To.DB != v.cfg.DB {
			v.dropColl(ns) // в другую базу: здесь коллекции больше нет
			return
		}
		v.q.rename(ns, ev.To.String(), ev.ClusterTime)
		if v.created[ev.NS.Coll] {
			delete(v.created, ev.NS.Coll)
			v.created[ev.To.Coll] = true
		}
	case "dropDatabase", "invalidate":
		if !v.ended {
			log.Info().Str("op", ev.OperationType).Msg("поток сверки окончен: дожидаюсь проверки ключей в очереди")
		}
		v.ended = true
		// событий больше не будет: проверки не ждут поток
		v.stream.set(maxTS)
	}
}

// dropColl — коллекция удалена: её ждущие ключи «не проверены».
func (v *Verifier) dropColl(ns string) {
	for _, it := range v.q.drop(ns) {
		v.addUnverified(it.ns, it.id, reasonDrop, nil, nil)
	}
}
