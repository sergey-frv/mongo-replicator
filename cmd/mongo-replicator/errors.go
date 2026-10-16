package main

import (
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/sergey-frv/mongo-replicator/internal/replicator"
)

// permanent сообщает, что ретраи ошибку не исправят и нужен оператор.
func permanent(err error) bool { return hint(err) != "" }

// hint — подсказка оператору по перманентной ошибке; "" — ошибка временная.
func hint(err error) string {
	var dropErr *replicator.DropError
	switch {
	case errors.As(err, &dropErr) && dropErr.Database:
		return "база удалена на источнике; на цели данные остались — нужна новая полная заливка: " +
			"удалите <dst-db>._replicator_checkpoint (или всю базу цели) и залейте заново с --start-ts"
	case errors.As(err, &dropErr):
		return fmt.Sprintf("на источнике удалена коллекция %s (вручную, перенос в другую базу или dropDatabase); "+
			"если удаление ожидаемое — перезапустите с --apply-drop %s", dropErr.NS, dropErr.NS)
	case replicator.IsEventTooLarge(err):
		return "событие изменения на источнике больше 16 МБ — change stream не может его отдать; " +
			"нужна новая полная заливка цели с --start-ts позже этого изменения"
	case replicator.IsHistoryLost(err):
		return "точка возобновления выпала из oplog источника — нужна новая полная заливка цели"
	case errors.Is(err, replicator.ErrNoStartPoint):
		return "задайте --start-ts"
	case errors.Is(err, replicator.ErrDBMismatch):
		return "чекпойнт этой базы цели записан для другой базы источника: верните прежний --db " +
			"или удалите <dst-db>._replicator_checkpoint (или всю базу цели) и залейте заново"
	case errors.Is(err, replicator.ErrLeaseHeld):
		return "другой процесс реплицирует эту базу; если он упал — аренда истечёт через ≤60 с"
	case errors.Is(err, replicator.ErrLeaseLost):
		return "аренду забрал другой процесс — проверьте, не запущены ли два репликатора на одну базу"
	case errors.Is(err, replicator.ErrForeignNamespace):
		return "в потоке событие другой базы — сообщите о баге"
	case errors.Is(err, replicator.ErrStreamInvalidated):
		return "поток закрыт сервером; на цели данные остались — нужна новая полная заливка: " +
			"удалите <dst-db>._replicator_checkpoint (или всю базу цели) и залейте заново с --start-ts"
	case errors.Is(err, replicator.ErrReservedCollection):
		return "на источнике коллекция переименована в служебное имя _replicator_checkpoint или из него: " +
			"переименуйте её на источнике в другое имя и сделайте новую полную заливку"
	case replicator.IsDBCaseConflict(err):
		return "на цели есть база с тем же именем в другом регистре — проверьте --dst-db"
	}
	return ""
}

// isAuthFailed сообщает, что сервер отверг аутентификацию (AuthenticationFailed, код 18):
// Ping возвращает её как mongo.CommandError.
func isAuthFailed(err error) bool {
	var se mongo.ServerError
	return errors.As(err, &se) && se.HasErrorCode(18)
}
