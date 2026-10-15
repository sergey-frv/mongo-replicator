package replicator

import (
	"bytes"
	"context"
	"errors"
	"time"

	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

// Чекпойнт хранится на цели: он двигается только после успешной записи данных.
// Документ на базу цели (_id — её имя): процессы разных баз пишут в один кластер
// и не мешают друг другу.
const (
	ckptDB   = "_replicator"
	ckptColl = "checkpoint"
)

// ckpt читает чекпойнт только с primary, не наследуя read preference из URI цели:
// устаревший токен с отстающей secondary повторил бы rename с dropTarget поверх
// уже применённого состояния и уничтожил бы целевую коллекцию.
func (r *Replicator) ckpt() *mongo.Collection {
	return r.dst.Database(ckptDB).Collection(ckptColl,
		options.Collection().SetReadPreference(readpref.Primary()))
}

// checkpoint — сохранённая позиция; пустой Token — чекпойнта нет (документ может
// быть создан захватом аренды без позиции).
type checkpoint struct {
	Token       bson.Raw       `bson:"token"`
	DB          string         `bson:"db"`     // база источника, для которой записан токен
	DstDB       string         `bson:"dst_db"` // база цели, в которую применялись события
	ClusterTime bson.Timestamp `bson:"clusterTime"`
}

// dbs — пара баз, для которой записан чекпойнт.
func (c checkpoint) dbs() dbPair { return dbPair{src: c.DB, dst: c.DstDB} }

func (r *Replicator) loadCheckpoint(ctx context.Context) (checkpoint, error) {
	var c checkpoint
	err := r.ckpt().FindOne(ctx, bson.M{"_id": r.cfg.DstDB}).Decode(&c)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return checkpoint{}, nil
	}
	return c, err
}

// appliedAt — время применённой позиции: из токена, а если он не разобран —
// clusterTime, записанный при сохранении по последнему событию.
func (c checkpoint) appliedAt() bson.Timestamp {
	if ts, err := tokenTime(c.Token); err == nil {
		return ts
	}
	return c.ClusterTime
}

func (r *Replicator) saveToken(ctx context.Context, token bson.Raw) error {
	if len(token) == 0 || bytes.Equal(token, r.saved) {
		return nil // нечего сохранять: поток не продвинулся
	}
	// clusterTime — время применённой позиции: всё до токена уже на цели
	ts, tsErr := tokenTime(token)
	if tsErr != nil {
		log.Debug().Err(tsErr).Msg("время токена не разобрано, clusterTime — по последнему событию")
		ts = r.lastTS
		// событий после рестарта ещё не было: позицию не откатываем и не обнуляем
		if ts.Before(r.appliedTS) {
			ts = r.appliedTS
		}
	}
	// без upsert: документ создан захватом аренды; owner и traffic не затираются
	res, err := r.ckpt().UpdateOne(ctx,
		bson.M{"_id": r.cfg.DstDB, "owner": r.owner},
		bson.M{"$set": bson.M{
			"token":       token,
			"clusterTime": ts,
			"updatedAt":   time.Now().UTC(),
			"db":          r.cfg.DB,
			"dst_db":      r.cfg.DstDB,
		}})
	if err != nil {
		return err
	}
	if res.MatchedCount == 0 {
		return r.leaseLostErr()
	}
	r.saved = token
	r.appliedTS = ts
	return nil
}
