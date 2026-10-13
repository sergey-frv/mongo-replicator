package replicator

import (
	"bytes"
	"context"
	"errors"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Чекпойнт хранится на цели: он двигается только после успешной записи данных.
const (
	ckptDB   = "_replicator"
	ckptColl = "checkpoint"
	ckptID   = "main"
)

func (r *Replicator) ckpt() *mongo.Collection {
	return r.dst.Database(ckptDB).Collection(ckptColl)
}

func (r *Replicator) loadToken(ctx context.Context) (bson.Raw, error) {
	var doc struct {
		Token bson.Raw `bson:"token"`
	}
	err := r.ckpt().FindOne(ctx, bson.M{"_id": ckptID}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	return doc.Token, err
}

func (r *Replicator) saveToken(ctx context.Context, token bson.Raw) error {
	if len(token) == 0 || bytes.Equal(token, r.saved) {
		return nil // нечего сохранять: поток не продвинулся
	}
	_, err := r.ckpt().ReplaceOne(ctx,
		bson.M{"_id": ckptID},
		bson.M{
			"_id":         ckptID,
			"token":       token,
			"clusterTime": bson.Timestamp{T: r.lastTS},
			"updatedAt":   time.Now().UTC(),
		},
		options.Replace().SetUpsert(true))
	if err == nil {
		r.saved = token
	}
	return err
}
