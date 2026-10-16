package replicator

import (
	"context"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/sergey-frv/mongo-replicator/internal/traffic"
)

// Состояния репликатора для Status.State.
const (
	StateStarting = "запуск"     // New: Run ещё не открыл поток
	StateRunning  = "работает"   // поток открыт
	StateRetry    = "повтор"     // Run вернул временную ошибку, main ждёт повтора (SetRetry)
	StateStopped  = "остановлен" // Run вернулся: отмена или перманентная ошибка
)

// maxBatchColls — сколько коллекций пачки перечисляет BatchInfo, остальные — числом.
const maxBatchColls = 3

// OpCounts — операции пачки по видам событий.
type OpCounts struct{ Ins, Upd, Rep, Del int }

// BatchInfo — последняя записанная на цели пачка.
type BatchInfo struct {
	At    time.Time
	Ops   OpCounts
	Colls []string // первые maxBatchColls коллекций в порядке появления
	More  int      // сколько коллекций не попало в Colls
}

// DDLInfo — последнее обработанное изменение схемы.
type DDLInfo struct {
	At      time.Time
	Op      string
	NS      string // namespace источника
	Details string // цель rename, имена индексов, «пропущено»
}

// Status — снимок состояния для панели. LastBatch, LastDDL и Machine при обновлении
// заменяются новыми значениями и не изменяются: копию можно читать без блокировки.
type Status struct {
	DB, DstDB string
	Started   time.Time
	Applied   int64
	Rate      float64       // операций/с за последний интервал прогресса
	AppliedAt time.Time     // время источника, до которого всё применено; нулевое — неизвестно
	Lag       time.Duration // -1 — неизвестен
	LastBatch *BatchInfo
	LastDDL   *DDLInfo
	Traffic   traffic.Info    // байты процесса
	Machine   *MachineTraffic // последняя сводка по файлам состояния машины; nil — ещё не было
	State     string
	Retry     int    // номер повтора подряд; 0 — после открытия потока
	LastErr   string // ошибка, из-за которой Run вернулся
}

// Status — текущий снимок; безопасен для вызова из другой горутины.
func (r *Replicator) Status() Status {
	r.mu.Lock()
	s := r.st
	r.mu.Unlock()
	s.Applied = r.applied.Load()
	s.Traffic = r.cfg.Traffic.Snapshot()
	return s
}

// SetRetry отмечает, что Run вернул временную ошибку и будет повторён: вызывается
// из цикла повторов перед паузой. Счётчик повторов сбрасывает Run, открыв поток.
func (r *Replicator) SetRetry(err error) {
	r.update(func(s *Status) {
		s.State = StateRetry
		s.Retry++
		s.LastErr = errText(err)
	})
}

func (r *Replicator) update(f func(s *Status)) {
	r.mu.Lock()
	f(&r.st)
	r.mu.Unlock()
}

// streamOpened — поток открыт: репликатор снова работает.
func (r *Replicator) streamOpened() {
	r.update(func(s *Status) {
		s.State, s.Retry, s.LastErr = StateRunning, 0, ""
	})
}

// stopped — Run вернулся. Отмена вызывающим ошибкой не считается.
func (r *Replicator) stopped(ctx context.Context, err error) {
	r.update(func(s *Status) {
		s.State = StateStopped
		if err != nil && ctx.Err() == nil {
			s.LastErr = errText(err)
		}
	})
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// batchInfo описывает пачку; nil — пустая.
func batchInfo(b *batch, at time.Time) *BatchInfo {
	if b.n == 0 {
		return nil
	}
	bi := &BatchInfo{At: at, Ops: b.counts}
	for i, ns := range b.order {
		if i == maxBatchColls {
			bi.More = len(b.order) - maxBatchColls
			break
		}
		bi.Colls = append(bi.Colls, ns.Coll)
	}
	return bi
}

// noteDDL запоминает обработанное событие схемы.
func (r *Replicator) noteDDL(ev *event, details string) {
	d := &DDLInfo{At: time.Now(), Op: ev.OperationType, NS: ev.NS.String(), Details: details}
	r.update(func(s *Status) { s.LastDDL = d })
}

// ddlDetails — подробности DDL для панели: цель rename или имена индексов.
func ddlDetails(ev *event) string {
	switch ev.OperationType {
	case "rename":
		if ev.To != nil {
			return "→ " + ev.To.String()
		}
	case "createIndexes", "dropIndexes":
		return descIndexNames(ev.OpDesc)
	}
	return ""
}

// descIndexNames — имена индексов из operationDescription через запятую; ошибки разбора — пусто.
func descIndexNames(desc bson.Raw) string {
	arr, ok := desc.Lookup("indexes").ArrayOK()
	if !ok {
		return ""
	}
	vals, err := arr.Values()
	if err != nil {
		return ""
	}
	var names []string
	for _, v := range vals {
		doc, ok := v.DocumentOK()
		if !ok {
			continue
		}
		if name, ok := doc.Lookup("name").StringValueOK(); ok {
			names = append(names, name)
		}
	}
	return strings.Join(names, ", ")
}
