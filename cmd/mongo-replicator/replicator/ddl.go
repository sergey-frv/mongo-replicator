package replicator

import (
	"context"
	"errors"
	"fmt"
	"regexp"

	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// Коды ошибок сервера.
const (
	codeNamespaceNotFound       = 26
	codeIndexNotFound           = 27
	codeNamespaceExists         = 48
	codeDatabaseDifferCase      = 13297
	codeChangeStreamFatal       = 280
	codeChangeStreamHistoryLost = 286
	codeBSONObjectTooLarge      = 10334
)

// IsEventTooLarge сообщает, что событие потока больше 16 МБ: сервер не может его отдать,
// и повтор с того же токена упрётся в него снова.
func IsEventTooLarge(err error) bool {
	return hasCode(err, codeBSONObjectTooLarge)
}

// IsHistoryLost сообщает, что точка возобновления выпала из oplog источника:
// продолжать поток нельзя, нужна новая полная заливка цели.
func IsHistoryLost(err error) bool {
	return hasCode(err, codeChangeStreamHistoryLost, codeChangeStreamFatal)
}

// IsDBCaseConflict сообщает, что на цели уже есть база с тем же именем в другом регистре:
// сервер отвергает любую запись в --dst-db, ретраи не помогут.
func IsDBCaseConflict(err error) bool {
	return hasCode(err, codeDatabaseDifferCase)
}

// DropError — на источнике удалена коллекция или база, на цели удаление не применено:
// такие операции не предусмотрены, решает оператор.
type DropError struct {
	NS       string // namespace коллекции источника; для dropDatabase — имя базы
	Database bool   // dropDatabase
}

func (e *DropError) Error() string {
	if e.Database {
		return fmt.Sprintf("на источнике удалена база %s, на цели не применено", e.NS)
	}
	return fmt.Sprintf("на источнике удалена коллекция %s, на цели не применено", e.NS)
}

// tempCollRe — имена, которые генерирует сама MongoDB: tmp.agg_out.<UUID> ($out, mapReduce),
// tmpXXXXX.renameCollection (перенос между базами), tmpXXXXX.convertToCapped.<coll>.
// Шаблоны точные, а не префиксы: пользовательская tmp.agg_out.archive под исключение не попадает.
// tmp.mr.* не нужен: на источниках 6.0+ mapReduce идёт через агрегацию.
var tempCollRe = regexp.MustCompile(`^(tmp\.agg_out\.[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$|` +
	`tmp[A-Za-z0-9]{5}\.(renameCollection|convertToCapped\.(?s:.+))$)`)

// isTempColl сообщает, что коллекция — временная коллекция самой MongoDB: её drop
// (например, после неудачного $out) применяется без разрешения. Не префикс "tmp"
// целиком: tmp.backup — пользовательская.
func isTempColl(name string) bool {
	return tempCollRe.MatchString(name)
}

// dropAllowed решает, применять ли drop коллекции: временная, разрешённая --apply-drop
// или view на цели (например, из заливки; данных в нём нет) — да; коллекции нет на цели
// (view, time-series) — нет, терять нечего; иначе — *DropError.
func (r *Replicator) dropAllowed(ctx context.Context, db *mongo.Database, ev *event) (bool, error) {
	if isTempColl(ev.NS.Coll) || r.applyDrops[ev.NS.String()] {
		return true, nil
	}
	specs, err := db.ListCollectionSpecifications(ctx, bson.M{"name": ev.NS.Coll})
	if err != nil {
		return false, fmt.Errorf("drop %s: проверка коллекции на цели: %w", r.nsName(ev.NS), err)
	}
	if len(specs) == 0 {
		log.Warn().Str("ns", ev.NS.String()).
			Msgf("drop %s: на цели такой коллекции нет (view/time-series или уже удалена)", r.nsName(ev.NS))
		return false, nil
	}
	if specs[0].Type == "view" {
		return true, nil
	}
	return false, &DropError{NS: ev.NS.String()}
}

// applyDDL переносит изменения схемы. Ошибки «уже есть» / «уже нет»
// игнорируются: они означают повторное применение после рестарта.
func (r *Replicator) applyDDL(ctx context.Context, ev *event) error {
	if r.ddlHook != nil {
		r.ddlHook()
	}
	db := r.dst.Database(r.cfg.DstDB)
	var err error

	switch ev.OperationType {
	case "drop":
		apply, allowErr := r.dropAllowed(ctx, db, ev)
		if allowErr != nil {
			return allowErr
		}
		if !apply {
			r.noteDDL(ev, "на цели нет, пропущено")
			return nil
		}
		err = db.Collection(ev.NS.Coll).Drop(ctx)

	case "dropDatabase":
		// не разрешается никогда: на цели остаются данные, нужна новая заливка
		return &DropError{NS: ev.NS.DB, Database: true}

	case "rename":
		if ev.To == nil {
			return fmt.Errorf("rename %s: в событии нет поля to", ev.NS)
		}
		err = runCommand(ctx, r.dst.Database("admin"), bson.D{
			{Key: "renameCollection", Value: r.dstNS(ev.NS).String()},
			{Key: "to", Value: r.dstNS(*ev.To).String()},
			{Key: "dropTarget", Value: true},
		})

	case "create":
		// Опции (view, capped, validator, collation, timeseries) не переносим:
		// часть из них отличается между версиями. Создаём обычную коллекцию
		// и предупреждаем, если на источнике были нестандартные опции.
		if _, lookupErr := ev.OpDesc.LookupErr("viewOn"); lookupErr == nil {
			log.Warn().Str("ns", ev.NS.String()).Str("desc", ev.OpDesc.String()).
				Msg("view не создаётся автоматически")
			r.noteDDL(ev, "view, пропущено")
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
			return fmt.Errorf("dropIndexes %s: %w", r.nsName(ev.NS), valuesErr)
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
		r.noteDDL(ev, "не поддерживается, пропущено")
		return nil
	}

	if hasCode(err, codeNamespaceNotFound, codeIndexNotFound, codeNamespaceExists) {
		r.noteDDL(ev, ddlDetails(ev))
		return nil
	}
	if err != nil {
		return fmt.Errorf("%s %s: %w", ev.OperationType, r.nsName(ev.NS), err)
	}
	r.noteDDL(ev, ddlDetails(ev))
	l := log.Info().Str("op", ev.OperationType).Str("ns", ev.NS.String())
	if ev.To != nil {
		l = l.Str("to", ev.To.String())
	}
	if r.cfg.DstDB != r.cfg.DB {
		l = l.Str("dst", r.dstNS(ev.NS).String())
	}
	if ev.OperationType == "drop" && r.applyDrops[ev.NS.String()] {
		l.Msg("drop применён по --apply-drop")
		return nil
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
