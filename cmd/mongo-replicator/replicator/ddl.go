package replicator

import (
	"context"
	"errors"
	"fmt"

	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// Коды ошибок сервера.
const (
	codeNamespaceNotFound       = 26
	codeIndexNotFound           = 27
	codeNamespaceExists         = 48
	codeChangeStreamFatal       = 280
	codeChangeStreamHistoryLost = 286
)

// IsHistoryLost сообщает, что точка возобновления выпала из oplog источника:
// продолжать поток нельзя, нужна новая полная заливка цели.
func IsHistoryLost(err error) bool {
	return hasCode(err, codeChangeStreamHistoryLost, codeChangeStreamFatal)
}

// applyDDL переносит изменения схемы. Ошибки «уже есть» / «уже нет»
// игнорируются: они означают повторное применение после рестарта.
func (r *Replicator) applyDDL(ctx context.Context, ev *event) error {
	db := r.dst.Database(ev.NS.DB)
	var err error

	switch ev.OperationType {
	case "drop":
		err = db.Collection(ev.NS.Coll).Drop(ctx)

	case "dropDatabase":
		err = db.Drop(ctx)

	case "rename":
		if ev.To == nil {
			return fmt.Errorf("rename %s: в событии нет поля to", ev.NS)
		}
		err = runCommand(ctx, r.dst.Database("admin"), bson.D{
			{Key: "renameCollection", Value: ev.NS.String()},
			{Key: "to", Value: ev.To.String()},
			{Key: "dropTarget", Value: true},
		})

	case "create":
		// Опции (view, capped, validator, collation, timeseries) не переносим:
		// часть из них отличается между версиями. Создаём обычную коллекцию
		// и предупреждаем, если на источнике были нестандартные опции.
		if _, lookupErr := ev.OpDesc.LookupErr("viewOn"); lookupErr == nil {
			log.Warn().Str("ns", ev.NS.String()).Str("desc", ev.OpDesc.String()).
				Msg("view не создаётся автоматически")
			return nil
		}
		if elems, _ := ev.OpDesc.Elements(); len(elems) > 1 { // кроме idIndex
			log.Warn().Str("ns", ev.NS.String()).Str("desc", ev.OpDesc.String()).
				Msg("коллекция создана без опций источника, проверьте вручную")
		}
		err = db.CreateCollection(ctx, ev.NS.Coll)

	case "createIndexes":
		err = runCommand(ctx, db, bson.D{
			{Key: "createIndexes", Value: ev.NS.Coll},
			{Key: "indexes", Value: ev.OpDesc.Lookup("indexes")},
		})

	case "dropIndexes":
		specs, valuesErr := ev.OpDesc.Lookup("indexes").Array().Values()
		if valuesErr != nil {
			return fmt.Errorf("dropIndexes %s: %w", ev.NS, valuesErr)
		}
		for _, spec := range specs {
			name := spec.Document().Lookup("name").StringValue()
			err = runCommand(ctx, db, bson.D{
				{Key: "dropIndexes", Value: ev.NS.Coll},
				{Key: "index", Value: name},
			})
			if err != nil && !hasCode(err, codeNamespaceNotFound, codeIndexNotFound) {
				break
			}
		}

	default:
		// modify (collMod), shardCollection, refineCollectionShardKey и т.п.
		log.Warn().Str("op", ev.OperationType).Str("ns", ev.NS.String()).Str("desc", ev.OpDesc.String()).
			Msg("событие не поддерживается и пропущено")
		return nil
	}

	if hasCode(err, codeNamespaceNotFound, codeIndexNotFound, codeNamespaceExists) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("%s %s: %w", ev.OperationType, ev.NS, err)
	}
	l := log.Info().Str("op", ev.OperationType).Str("ns", ev.NS.String())
	if ev.To != nil {
		l = l.Str("to", ev.To.String())
	}
	l.Msg("DDL применён")
	return nil
}

// runCommand выполняет DDL-команду с w:majority. RunCommand не берёт
// write concern клиента, а без него DDL может откатиться при failover,
// тогда как следующий за ним чекпойнт (w:majority) сохранится.
func runCommand(ctx context.Context, db *mongo.Database, cmd bson.D) error {
	cmd = append(cmd, bson.E{Key: "writeConcern", Value: bson.D{{Key: "w", Value: "majority"}}})
	return db.RunCommand(ctx, cmd).Err()
}

func hasCode(err error, codes ...int) bool {
	var se mongo.ServerError
	if !errors.As(err, &se) {
		return false
	}
	for _, c := range codes {
		if se.HasErrorCode(c) {
			return true
		}
	}
	return false
}
