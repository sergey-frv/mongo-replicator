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
const (
	ckptDB   = "_replicator"
	ckptColl = "checkpoint"
	ckptID   = "main"
)

// ckpt читает чекпойнт только с primary, не наследуя read preference из URI цели:
// устаревший токен с отстающей secondary повторил бы rename с dropTarget поверх
// уже применённого состояния и уничтожил бы целевую коллекцию.
func (r *Replicator) ckpt() *mongo.Collection {
	return r.dst.Database(ckptDB).Collection(ckptColl,
		options.Collection().SetReadPreference(readpref.Primary()))
}

// loadToken возвращает сохранённый токен и базу, для которой он записан ("" — все базы).
func (r *Replicator) loadToken(ctx context.Context) (token bson.Raw, db string, err error) {
	var doc struct {
		Token bson.Raw `bson:"token"`
		DB    string   `bson:"db"`
	}
	err = r.ckpt().FindOne(ctx, bson.M{"_id": ckptID}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, "", nil
	}
	return doc.Token, doc.DB, err
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
	}
	_, err := r.ckpt().ReplaceOne(ctx,
		bson.M{"_id": ckptID},
		bson.M{
			"_id":         ckptID,
			"token":       token,
			"clusterTime": ts,
			"updatedAt":   time.Now().UTC(),
			"db":          r.cfg.DB,
		},
		options.Replace().SetUpsert(true))
	if err == nil {
		r.saved = token
		r.appliedTS = ts
	}
	return err
}
