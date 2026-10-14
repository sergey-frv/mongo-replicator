package replicator

import (
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type namespace struct {
	DB   string `bson:"db"`
	Coll string `bson:"coll"`
}

func (n namespace) String() string { return n.DB + "." + n.Coll }

// event — нужные нам поля события change stream.
// Документы держим как bson.Raw, чтобы передать на цель байт в байт,
// без потери типов и порядка полей.
type event struct {
	ID            bson.Raw       `bson:"_id"` // resume token
	OperationType string         `bson:"operationType"`
	ClusterTime   bson.Timestamp `bson:"clusterTime"`
	NS            namespace      `bson:"ns"`
	To            *namespace     `bson:"to"` // только для rename
	DocumentKey   bson.Raw       `bson:"documentKey"`
	FullDocument  bson.Raw       `bson:"fullDocument"`
	OpDesc        bson.Raw       `bson:"operationDescription"` // только для DDL
}

// toModel превращает DML-событие в идемпотентную операцию записи.
// ok=false — событие не DML либо его нужно пропустить.
func toModel(ev *event) (m mongo.WriteModel, ok bool) {
	// На шардированном источнике documentKey содержит ещё и ключ шардирования,
	// поэтому фильтруем строго по _id.
	filter := bson.D{{Key: "_id", Value: ev.DocumentKey.Lookup("_id")}}

	switch ev.OperationType {
	case "insert", "replace", "update":
		if len(ev.FullDocument) == 0 {
			// update + updateLookup: к моменту чтения документа на источнике его уже нет.
			// Если он удалён — следом в потоке придёт delete. Если коллекцию переименовали
			// или пересоздали — update теряется: известное ограничение (a), см. README.
			// Пропускаем в обоих случаях.
			return nil, false
		}
		return mongo.NewReplaceOneModel().
			SetFilter(filter).
			SetReplacement(ev.FullDocument).
			SetUpsert(true), true
	case "delete":
		return mongo.NewDeleteOneModel().SetFilter(filter), true
	}
	return nil, false
}
