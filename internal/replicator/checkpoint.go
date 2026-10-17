package replicator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

// Чекпойнт хранится на цели, в служебной коллекции самой базы цели: он двигается
// только после успешной записи данных, на цели достаточно прав на одну базу,
// а удаляется чекпойнт вместе с данными. Документ в коллекции один (_id — ckptID).
const (
	ckptColl = "_replicator_checkpoint"
	ckptID   = "checkpoint"
)

// ckpt читает чекпойнт только с primary, не наследуя read preference из URI цели:
// устаревший токен с отстающей secondary повторил бы rename с dropTarget поверх
// уже применённого состояния и уничтожил бы целевую коллекцию.
func (r *Replicator) ckpt() *mongo.Collection { return ckptOn(r.dst, r.cfg.DstDB) }

// ckptOn — служебная коллекция чекпойнта базы цели dstDB с чтением с primary.
func ckptOn(dst *mongo.Client, dstDB string) *mongo.Collection {
	return dst.Database(dstDB).Collection(ckptColl,
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
	return readCheckpoint(ctx, r.ckpt())
}

// readCheckpoint читает документ чекпойнта; нет документа — пустой чекпойнт.
func readCheckpoint(ctx context.Context, coll *mongo.Collection) (checkpoint, error) {
	var c checkpoint
	err := coll.FindOne(ctx, bson.M{"_id": ckptID}).Decode(&c)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return checkpoint{}, nil
	}
	return c, err
}

// AppliedPosition — время применённой позиции чекпойнта пары баз srcDB → dstDB
// на цели dst (чтение с primary). На цели применено всё, что на источнике
// записано строго до ts; события со временем, равным ts, могут быть применены
// не все: токен сохраняется и посреди группы событий с одним clusterTime
// (транзакция, пакетное удаление). Поэтому сравнивать с позицией — только строго:
// событие t применено, если t < ts. ok=false — документа нет или позиции в нём
// нет (документ создан захватом аренды). Чекпойнт другой пары баз — ErrDBMismatch.
func AppliedPosition(ctx context.Context, dst *mongo.Client, srcDB, dstDB string) (ts bson.Timestamp, ok bool, err error) {
	c, err := readCheckpoint(ctx, ckptOn(dst, dstDB))
	if err != nil {
		return bson.Timestamp{}, false, fmt.Errorf("чтение чекпойнта: %w", err)
	}
	if c.Token == nil {
		return bson.Timestamp{}, false, nil
	}
	if err := checkDBs(c.dbs(), dbPair{src: srcDB, dst: dstDB}); err != nil {
		return bson.Timestamp{}, false, err
	}
	ts = c.appliedAt()
	return ts, !ts.IsZero(), nil
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
	// без upsert: документ создан захватом аренды; owner не затирается
	res, err := r.ckpt().UpdateOne(ctx,
		bson.M{"_id": ckptID, "owner": r.owner},
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
