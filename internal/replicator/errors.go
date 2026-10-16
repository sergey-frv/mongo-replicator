package replicator

import (
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/mongo"
)

// ErrNoStartPoint — на цели нет чекпойнта и StartTS не задан: неизвестно, откуда читать поток.
var ErrNoStartPoint = errors.New("чекпойнта нет и start-ts не задан: неизвестно, откуда читать")

// ErrDBMismatch — чекпойнт базы цели записан для другой базы источника, чем задана: продолжать с него значило бы тихо разойтись с источником.
var ErrDBMismatch = errors.New("чекпойнт записан для другой пары баз")

// ErrStreamInvalidated — поток закрыт сервером событием invalidate: продолжить его
// нельзя, токен invalidate не сохраняется. В потоке базы invalidate идёт за dropDatabase,
// на котором репликатор останавливается раньше (*DropError).
var ErrStreamInvalidated = errors.New("поток инвалидирован (dropDatabase базы)")

// ErrForeignNamespace — в потоке уровня базы событие другой базы: так не бывает,
// но применять его на цели значило бы писать не в ту базу.
var ErrForeignNamespace = errors.New("событие другой базы в потоке")

// ErrReservedCollection — на источнике коллекция переименована в служебное имя ckptColl
// или из него: на цели под этим именем лежит чекпойнт, применять такой rename нельзя.
var ErrReservedCollection = errors.New("rename в служебную коллекцию " + ckptColl + " или из неё")

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
