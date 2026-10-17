package verify

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// itemView — ключ пачки в виде для сравнения: _id int32 и времена в секундах.
type itemView struct {
	id          int32
	first, last uint32
	tries       int
}

// batchView — пачка в виде для сравнения.
type batchView struct {
	ns  string
	ids []int32
}

func at(sec uint32) bson.Timestamp { return bson.Timestamp{T: sec} }

// addAll учитывает события ключа с _id int32 в моменты secs.
func addAll(t *testing.T, q *queue, ns string, id int32, secs ...uint32) {
	t.Helper()
	for _, s := range secs {
		q.add(ns, rawID(t, id), at(s))
	}
}

// takeNext берёт следующую пачку, она обязана быть.
func takeNext(t *testing.T, q *queue, pos uint32) batch {
	t.Helper()
	b, ok := q.next(at(pos))
	require.True(t, ok, "нет готовой пачки при позиции %d", pos)
	return b
}

func viewsOf(b batch) []itemView {
	vs := make([]itemView, 0, len(b.items))
	for _, it := range b.items {
		vs = append(vs, itemView{id: it.id.Int32(), first: it.first.T, last: it.last.T, tries: it.tries})
	}
	return vs
}

func batchViewOf(b batch) batchView {
	v := batchView{ns: b.ns}
	for _, it := range b.items {
		v.ids = append(v.ids, it.id.Int32())
	}
	return v
}

// failHot n раз берёт единственный готовый ключ и завершает проверку горячим
// вердиктом при T1 = t1; возвращает причины done.
func failHot(t *testing.T, q *queue, n int, t1 uint32) []string {
	t.Helper()
	reasons := make([]string, 0, n)
	for range n {
		b := takeNext(t, q, t1+1)
		reasons = append(reasons, q.done(b.items[0], hot, at(t1)))
	}
	return reasons
}

func TestNewQueueBatchSize(t *testing.T) {
	tests := []struct {
		name string
		rate int
		want int
	}{
		{name: "rate меньше 500", rate: 200, want: 200},
		{name: "rate больше 500", rate: 1000, want: 500},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			q := newQueue(tc.rate)

			assert.Equal(t, tc.want, q.size)
		})
	}
}

func TestQueueDedup(t *testing.T) {
	q := newQueue(500)
	addAll(t, q, "db.a", 1, 20, 10, 30)

	b := takeNext(t, q, 100)

	assert.Equal(t, 1, q.len())
	assert.Equal(t, []itemView{{id: 1, first: 10, last: 30}}, viewsOf(b))
}

func TestQueueReadyByFirst(t *testing.T) {
	q := newQueue(500)
	addAll(t, q, "db.a", 1, 10, 20, 30, 40)

	b := takeNext(t, q, 11)

	assert.Equal(t, []itemView{{id: 1, first: 10, last: 40}}, viewsOf(b), "частые события не откладывают готовность")
}

func TestQueueNotReadyAtPosition(t *testing.T) {
	q := newQueue(500)
	addAll(t, q, "db.a", 1, 10)

	_, ok := q.next(at(10))

	assert.False(t, ok, "позиция, равная first: события с этим временем могут быть применены не все")
}

func TestQueueBatchesByCollection(t *testing.T) {
	q := newQueue(2)
	addAll(t, q, "db.b", 1, 5)
	addAll(t, q, "db.a", 1, 10)
	addAll(t, q, "db.a", 2, 12)
	addAll(t, q, "db.a", 3, 11)
	addAll(t, q, "db.b", 2, 20)
	addAll(t, q, "db.c", 1, 200)

	b1 := takeNext(t, q, 100)
	b2 := takeNext(t, q, 100)
	b3 := takeNext(t, q, 100)
	_, more := q.next(at(100))

	assert.Equal(t,
		[]batchView{
			{ns: "db.b", ids: []int32{1, 2}},
			{ns: "db.a", ids: []int32{1, 3}},
			{ns: "db.a", ids: []int32{2}},
		},
		[]batchView{batchViewOf(b1), batchViewOf(b2), batchViewOf(b3)})
	assert.False(t, more, "db.c ещё не готова")
	assert.Equal(t, 6, q.len(), "ключи на проверке остаются в очереди")
}

func TestQueueLimit(t *testing.T) {
	q := newQueue(500)
	q.limit = 2
	addAll(t, q, "db.a", 1, 10, 20)
	fullAfterOne := q.full()

	addAll(t, q, "db.a", 2, 10)

	assert.False(t, fullAfterOne, "один ключ при пределе 2")
	assert.True(t, q.full())
}

func TestQueueEventDuringCheckRequeued(t *testing.T) {
	q := newQueue(500)
	addAll(t, q, "db.a", 1, 10)
	b := takeNext(t, q, 100)
	addAll(t, q, "db.a", 1, 50, 30, 45)

	reason := q.done(b.items[0], matched, at(40))

	assert.Empty(t, reason)
	assert.Equal(t, []itemView{{id: 1, first: 45, last: 50}}, viewsOf(takeNext(t, q, 100)),
		"first — самое раннее событие позже T1=40")
}

func TestQueueEventBeforeT1Settles(t *testing.T) {
	q := newQueue(500)
	addAll(t, q, "db.a", 1, 10)
	b := takeNext(t, q, 100)
	addAll(t, q, "db.a", 1, 30)

	reason := q.done(b.items[0], defect, at(40))

	assert.Empty(t, reason)
	assert.Equal(t, 0, q.len(), "событие до T1=40 покрыто чтением S1")
}

func TestQueueEventIn(t *testing.T) {
	tests := []struct {
		name           string
		before, during []uint32
		want           bool
	}{
		{name: "без событий", before: []uint32{5}, want: false},
		{name: "равно T1", before: []uint32{5}, during: []uint32{10}, want: false},
		{name: "внутри окна", before: []uint32{5}, during: []uint32{15}, want: true},
		{name: "равно T2", before: []uint32{5}, during: []uint32{20}, want: true},
		{name: "после T2", before: []uint32{5}, during: []uint32{25}, want: false},
		{name: "промежуток до проверки пересекает окно", before: []uint32{5, 15}, want: true},
		{name: "промежуток до проверки после окна", before: []uint32{25, 30}, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			q := newQueue(500)
			addAll(t, q, "db.a", 1, tc.before...)
			b := takeNext(t, q, 100)
			addAll(t, q, "db.a", 1, tc.during...)

			got := q.eventIn(b.items[0], at(10), at(20))

			assert.Equal(t, tc.want, got)
		})
	}
}

func TestQueueRename(t *testing.T) {
	q := newQueue(500)
	addAll(t, q, "db.a", 1, 10)
	addAll(t, q, "db.a", 2, 11)
	addAll(t, q, "db.b", 2, 5)

	q.rename("db.a", "db.b", at(20))

	b := takeNext(t, q, 100)
	assert.Equal(t, "db.b", b.ns)
	assert.Equal(t, []itemView{{id: 2, first: 5, last: 11}, {id: 1, first: 10, last: 10}}, viewsOf(b))
	assert.Equal(t, 2, q.len(), "одноимённые ключи слились")
}

func TestQueueRenameDuringCheck(t *testing.T) {
	q := newQueue(500)
	addAll(t, q, "db.a", 1, 10)
	b := takeNext(t, q, 100)
	q.rename("db.a", "db.b", at(30))

	reason := q.done(b.items[0], matched, at(40))

	assert.Empty(t, reason)
	next := takeNext(t, q, 100)
	assert.Equal(t, "db.b", next.ns)
	assert.Equal(t, []itemView{{id: 1, first: 30, last: 30}}, viewsOf(next),
		"проверен под старым именем — повтор под новым после rename")
}

func TestQueueDrop(t *testing.T) {
	q := newQueue(500)
	addAll(t, q, "db.a", 1, 10)
	addAll(t, q, "db.a", 2, 20)
	addAll(t, q, "db.b", 1, 15)
	busy := takeNext(t, q, 11)

	gone := q.drop("db.a")
	reason := q.done(busy.items[0], matched, at(40))

	assert.Equal(t, batchView{ns: "db.a", ids: []int32{2}}, batchViewOf(batch{ns: "db.a", items: gone}),
		"ждавшие ключи — «не проверено»")
	assert.Equal(t, reasonDrop, reason, "ключ на проверке")
	assert.Equal(t, 1, q.len(), "остался ключ db.b")
}

func TestQueueDropThenRecreate(t *testing.T) {
	q := newQueue(500)
	addAll(t, q, "db.a", 1, 10)
	busy := takeNext(t, q, 100)
	q.drop("db.a")
	addAll(t, q, "db.a", 1, 50)

	reason := q.done(busy.items[0], matched, at(40))

	assert.Equal(t, reasonDrop, reason)
	assert.Equal(t, []itemView{{id: 1, first: 50, last: 50}}, viewsOf(takeNext(t, q, 100)),
		"ключ пересозданной коллекции встал в очередь заново")
}

func TestQueueHotRetryFromEvent(t *testing.T) {
	q := newQueue(500)
	addAll(t, q, "db.a", 1, 10)
	b := takeNext(t, q, 100)
	addAll(t, q, "db.a", 1, 45)

	reason := q.done(b.items[0], hot, at(40))

	assert.Empty(t, reason)
	assert.Equal(t, []itemView{{id: 1, first: 45, last: 45, tries: 1}}, viewsOf(takeNext(t, q, 100)))
}

func TestQueueHotGivesUp(t *testing.T) {
	q := newQueue(500)
	addAll(t, q, "db.a", 1, 10)

	reasons := failHot(t, q, 5, 40)

	assert.Equal(t, []string{"", "", "", "", reasonHot}, reasons)
	assert.Equal(t, 0, q.len())
}

func TestQueueHas(t *testing.T) {
	q := newQueue(500)
	addAll(t, q, "db.a", 1, 10)
	takeNext(t, q, 100)
	addAll(t, q, "db.a", 2, 20)

	assert.True(t, q.has("db.a", rawID(t, int32(1))), "ключ на проверке")
	assert.True(t, q.has("db.a", rawID(t, int32(2))), "ждущий ключ")
	assert.False(t, q.has("db.b", rawID(t, int32(1))), "тот же _id другой коллекции")
}

func TestQueueAnyBefore(t *testing.T) {
	q := newQueue(500)
	addAll(t, q, "db.a", 1, 10)
	takeNext(t, q, 100)
	addAll(t, q, "db.a", 2, 20)

	assert.True(t, q.anyBefore(at(11)), "ключ на проверке с first=10")
	assert.False(t, q.anyBefore(at(10)), "first=10 не раньше 10")
}

// Ошибка чтения: ключ возвращается без попытки, события проверки учтены.
func TestQueueBack(t *testing.T) {
	q := newQueue(500)
	addAll(t, q, "db.a", 1, 10)
	b := takeNext(t, q, 100)
	b.items[0].tries = 2
	addAll(t, q, "db.a", 1, 50, 30)

	reason := q.back(b.items[0])

	assert.Empty(t, reason)
	assert.Equal(t, []itemView{{id: 1, first: 10, last: 50, tries: 2}}, viewsOf(takeNext(t, q, 100)))
}

func TestQueueBackDropped(t *testing.T) {
	q := newQueue(500)
	addAll(t, q, "db.a", 1, 10)
	b := takeNext(t, q, 100)
	q.drop("db.a")

	reason := q.back(b.items[0])

	assert.Equal(t, reasonDrop, reason)
	assert.Equal(t, 0, q.len())
}

// Снимок для файла позиции: ключи на проверке — ждущими, с событиями проверки.
func TestQueueSnapshot(t *testing.T) {
	q := newQueue(500)
	addAll(t, q, "db.a", 1, 10)
	takeNext(t, q, 100)
	addAll(t, q, "db.a", 1, 40)
	addAll(t, q, "db.a", 2, 5, 20)

	got := q.snapshot()

	assert.Equal(t, []itemView{{id: 2, first: 5, last: 20}, {id: 1, first: 10, last: 40}},
		viewsOf(batch{items: got}))
	assert.False(t, got[1].busy, "ключ на проверке в снимке — ждущий")
}
