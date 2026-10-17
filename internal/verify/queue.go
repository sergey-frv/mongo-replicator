package verify

import (
	"cmp"
	"slices"

	"go.mongodb.org/mongo-driver/v2/bson"
)

const (
	// queueLimit — предел ключей в очереди: при нём поток не читается, пока
	// очередь не уменьшится.
	queueLimit = 100_000
	// maxBatch — предел _id в одной пачке $in.
	maxBatch = 500
	// maxTries — горячих вердиктов подряд, после которых ключ «не проверен».
	maxTries = 5
)

// Причины «не проверено».
const (
	reasonDrop = "drop" // коллекция удалена
	reasonHot  = "hot"  // источник менялся во время каждой из maxTries проверок
)

// key — ключ очереди: пространство имён и точный raw _id (тип и байты значения).
// При collation коллекции не simple разные строки могут совпасть по $in, поэтому
// ключ — байты, а не значение.
type key struct {
	ns string
	id string
}

func keyOf(ns string, id bson.RawValue) key {
	return key{ns: ns, id: string(append([]byte{byte(id.Type)}, id.Value...))}
}

// item — ключ в очереди или на проверке.
type item struct {
	ns    string
	id    bson.RawValue
	first bson.Timestamp // самое раннее непроверенное событие; по нему — готовность
	last  bson.Timestamp // самое позднее событие до начала проверки
	tries int            // горячих вердиктов подряд

	busy    bool             // на проверке
	events  []bson.Timestamp // события, пришедшие во время проверки
	movedAt bson.Timestamp   // время rename коллекции во время проверки; ноль — не было
	dropped bool             // коллекция удалена во время проверки
}

// note учитывает событие ключа, ждущего в очереди.
func (it *item) note(t bson.Timestamp) {
	if t.Before(it.first) {
		it.first = t
	}
	if t.After(it.last) {
		it.last = t
	}
}

// compareItems — порядок выдачи: по first, затем по коллекции и _id, чтобы
// порядок не зависел от обхода map.
func compareItems(a, b *item) int {
	return cmp.Or(
		a.first.Compare(b.first),
		cmp.Compare(a.ns, b.ns),
		cmp.Compare(keyOf(a.ns, a.id).id, keyOf(b.ns, b.id).id),
	)
}

// batch — пачка готовых ключей одной коллекции.
type batch struct {
	ns    string
	items []*item
}

// queue — ключи, изменённые после начала сверки и ещё не проверенные. Не
// потокобезопасна: ею владеет одна горутина.
type queue struct {
	size  int // предел пачки
	limit int // предел ключей
	items map[key]*item
}

// newQueue — очередь с пачками до min(maxBatch, rate): пачка всегда помещается
// в ёмкость ограничителя.
func newQueue(rate int) *queue {
	return &queue{size: min(maxBatch, rate), limit: queueLimit, items: map[key]*item{}}
}

// len — ключей в очереди и на проверке.
func (q *queue) len() int { return len(q.items) }

// full — достигнут предел: поток не читается, пока очередь не уменьшится.
func (q *queue) full() bool { return len(q.items) >= q.limit }

// add учитывает событие ключа со временем t. Ключ на проверке копит события
// отдельно: по ним вердикт решает, менялся ли источник в окне, а done — вернуть
// ли ключ в очередь.
func (q *queue) add(ns string, id bson.RawValue, t bson.Timestamp) {
	k := keyOf(ns, id)
	switch it := q.items[k]; {
	case it == nil:
		q.items[k] = &item{ns: ns, id: id, first: t, last: t}
	case it.busy:
		it.events = append(it.events, t)
	default:
		it.note(t)
	}
}

// next — пачка готовых ключей: first строго раньше pos (позиция чекпойнта:
// применено всё строго до неё; события с временем pos могут быть применены не
// все). Коллекция пачки — та, где самый ранний готовый ключ; ключи — по
// compareItems, не больше size. Ключи пачки переходят на проверку. false —
// готовых нет.
func (q *queue) next(pos bson.Timestamp) (batch, bool) {
	var ready []*item
	for _, it := range q.items {
		if !it.busy && it.first.Before(pos) {
			ready = append(ready, it)
		}
	}
	if len(ready) == 0 {
		return batch{}, false
	}
	oldest := slices.MinFunc(ready, compareItems)
	ready = slices.DeleteFunc(ready, func(it *item) bool { return it.ns != oldest.ns })
	slices.SortFunc(ready, compareItems)
	ready = ready[:min(len(ready), q.size)]
	for _, it := range ready {
		it.busy = true
	}
	return batch{ns: oldest.ns, items: ready}, true
}

// eventIn — было ли у ключа на проверке событие в (t1, t2]: среди пришедших во
// время проверки или в промежутке [first, last] до неё. Время событий до
// проверки обычно не позже T1 — поток отдаёт только majority-записи, а T1 —
// время более позднего majority-чтения, — но промежуток учитывается целиком:
// отдельные события в нём не хранятся.
func (q *queue) eventIn(it *item, t1, t2 bson.Timestamp) bool {
	if !it.first.After(t2) && it.last.After(t1) {
		return true
	}
	return slices.ContainsFunc(it.events, func(t bson.Timestamp) bool {
		return t.After(t1) && !t.After(t2)
	})
}

// done завершает проверку ключа вердиктом r, T1 = t1. Ключ возвращается в
// очередь, если известно его событие позже T1 (first — самое раннее такое), если
// вердикт горячий (попытка засчитывается) или если коллекцию переименовали во
// время проверки (её документы теперь под новым именем). Возвращает причину,
// если ключ ушёл в «не проверено», иначе "".
func (q *queue) done(it *item, r result, t1 bson.Timestamp) string {
	k := keyOf(it.ns, it.id)
	if q.items[k] == it {
		delete(q.items, k)
	}
	if it.dropped {
		return reasonDrop
	}
	tries := 0
	if r == hot {
		tries = it.tries + 1
		if tries >= maxTries {
			return reasonHot
		}
	}
	first, ok := it.firstAfter(t1)
	switch {
	case ok:
	case !it.movedAt.IsZero():
		first, ok = it.movedAt, true
	case r == hot:
		// источник менялся, но событие позже T1 не дошло: повторить сразу,
		// чекпойнт уже дальше T1
		first, ok = t1, true
	}
	if ok {
		q.requeue(k, &item{ns: it.ns, id: it.id, first: first, last: it.lastEvent(first), tries: tries})
	}
	return ""
}

// firstAfter — самое раннее известное событие ключа позже t.
func (it *item) firstAfter(t bson.Timestamp) (bson.Timestamp, bool) {
	var first bson.Timestamp
	ok := false
	for _, e := range append([]bson.Timestamp{it.first, it.last}, it.events...) {
		if e.After(t) && (!ok || e.Before(first)) {
			first, ok = e, true
		}
	}
	return first, ok
}

// lastEvent — самое позднее известное событие ключа, не раньше floor.
func (it *item) lastEvent(floor bson.Timestamp) bson.Timestamp {
	last := floor
	for _, e := range append([]bson.Timestamp{it.last}, it.events...) {
		if e.After(last) {
			last = e
		}
	}
	return last
}

// requeue ставит ждущий ключ под k, сливая его с уже стоящим там.
func (q *queue) requeue(k key, it *item) {
	have := q.items[k]
	switch {
	case have == nil:
		q.items[k] = it
	case have.busy:
		have.events = append(have.events, it.first, it.last)
	default:
		have.note(it.first)
		have.note(it.last)
		have.tries = max(have.tries, it.tries)
	}
}

// rename переносит ключи коллекции from под to: после rename документы живут в
// to на обеих сторонах. Ключ на проверке получает rename со временем t как
// событие и после вердикта вернётся в очередь под новым именем. Если под новым
// именем ключ уже есть, они сливаются; если оба на проверке, переименованный
// выходит из учёта, и его done сольёт результат с оставшимся.
func (q *queue) rename(from, to string, t bson.Timestamp) {
	var moved []*item
	for k, it := range q.items {
		if it.ns == from {
			delete(q.items, k)
			moved = append(moved, it)
		}
	}
	for _, it := range moved {
		it.ns = to
		if it.busy {
			it.events = append(it.events, t)
			it.movedAt = t
		}
		k := keyOf(to, it.id)
		have := q.items[k]
		switch {
		case have == nil:
			q.items[k] = it
		case !it.busy:
			q.requeue(k, it)
		case !have.busy:
			it.events = append(it.events, have.first, have.last)
			q.items[k] = it
		}
	}
}

// drop убирает ключи удалённой коллекции и возвращает ждавшие в очереди — они
// «не проверены» с причиной reasonDrop. Ключи на проверке выходят из учёта (новые
// события того же ключа в пересозданной коллекции встанут в очередь заново), их
// done вернёт reasonDrop.
func (q *queue) drop(ns string) []*item {
	var gone []*item
	for k, it := range q.items {
		if it.ns != ns {
			continue
		}
		delete(q.items, k)
		if it.busy {
			it.dropped = true
			continue
		}
		gone = append(gone, it)
	}
	slices.SortFunc(gone, compareItems)
	return gone
}
