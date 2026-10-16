package replicator

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func mustMarshal(t *testing.T, v any) []byte {
	t.Helper()
	b, err := bson.Marshal(v)
	require.NoError(t, err, "bson.Marshal")
	return b
}

// changeEvent собирает DML-событие потока по коллекции shop.orders.
func changeEvent(op string, docKey bson.D, fields ...bson.E) bson.D {
	return append(bson.D{
		{Key: "_id", Value: bson.D{{Key: "_data", Value: "8263"}}},
		{Key: "operationType", Value: op},
		{Key: "ns", Value: bson.D{{Key: "db", Value: "shop"}, {Key: "coll", Value: "orders"}}},
		{Key: "documentKey", Value: docKey},
	}, fields...)
}

// decodeEvent декодирует событие так же, как поток: из BSON в event.
func decodeEvent(t *testing.T, d bson.D) event {
	t.Helper()
	var ev event
	require.NoError(t, bson.Unmarshal(mustMarshal(t, d), &ev), "декодирование события")
	require.NotEmpty(t, ev.ID, "resume token не декодирован")
	return ev
}

// insert, replace и update с документом — replace с upsert по _id.
func TestToModelUpsert(t *testing.T) {
	for _, op := range []string{"insert", "replace", "update"} {
		t.Run(op, func(t *testing.T) {
			ev := decodeEvent(t, changeEvent(op, bson.D{{Key: "_id", Value: int32(7)}},
				bson.E{Key: "clusterTime", Value: bson.Timestamp{T: 100, I: 2}},
				bson.E{Key: "fullDocument", Value: bson.D{{Key: "_id", Value: int32(7)}, {Key: "z", Value: 1.5}, {Key: "a", Value: "x"}}},
			))

			m, ok := toModel(&ev)

			require.True(t, ok, "%s даёт модель", op)
			require.IsType(t, &mongo.ReplaceOneModel{}, m, "%s — replace", op)
			rm := m.(*mongo.ReplaceOneModel)
			assert.Equal(t, bson.Raw(mustMarshal(t, bson.D{{Key: "_id", Value: int32(7)}})), bson.Raw(mustMarshal(t, rm.Filter)), "фильтр по _id")
			assert.Equal(t, bson.Raw(mustMarshal(t, bson.D{{Key: "_id", Value: int32(7)}, {Key: "z", Value: 1.5}, {Key: "a", Value: "x"}})), rm.Replacement,
				"документ доходит байт в байт (порядок полей и типы)")
			require.NotNil(t, rm.Upsert, "upsert не задан")
			assert.True(t, *rm.Upsert, "upsert не задан")
		})
	}
}

func TestToModelDelete(t *testing.T) {
	cases := []struct {
		name   string
		docKey bson.D
	}{
		{"documentKey из _id", bson.D{{Key: "_id", Value: int32(7)}}},
		{"составной documentKey шардированного источника", bson.D{{Key: "region", Value: "kg"}, {Key: "_id", Value: int32(7)}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ev := decodeEvent(t, changeEvent("delete", c.docKey))

			m, ok := toModel(&ev)

			require.True(t, ok, "удаление даёт модель")
			require.IsType(t, &mongo.DeleteOneModel{}, m, "удаление — delete")
			assert.Equal(t, bson.Raw(mustMarshal(t, bson.D{{Key: "_id", Value: int32(7)}})), bson.Raw(mustMarshal(t, m.(*mongo.DeleteOneModel).Filter)),
				"фильтр строго по _id")
		})
	}
}

// update + updateLookup без документа: к моменту чтения документа на источнике его уже нет.
func TestToModelUpdateWithoutDocument(t *testing.T) {
	cases := []struct {
		name   string
		fields []bson.E
	}{
		{"fullDocument: null", []bson.E{{Key: "fullDocument", Value: nil}}},
		{"нет поля fullDocument", nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ev := decodeEvent(t, changeEvent("update", bson.D{{Key: "_id", Value: int32(7)}}, c.fields...))

			m, ok := toModel(&ev)

			assert.False(t, ok, "update без документа пропускается")
			assert.Nil(t, m, "update без документа пропускается")
		})
	}
}

// Не DML-событие (DDL) модели не даёт.
func TestToModelNotDML(t *testing.T) {
	ev := decodeEvent(t, changeEvent("drop", bson.D{{Key: "_id", Value: int32(7)}}))

	m, ok := toModel(&ev)

	assert.False(t, ok, "drop — не DML")
	assert.Nil(t, m, "drop — не DML")
}

// Декодированные bson.Raw не должны ссылаться на буфер события:
// драйвер переиспользует его при следующем getMore, а мы держим документы до flush.
func TestRawIsCopied(t *testing.T) {
	buf := mustMarshal(t, bson.D{
		{Key: "_id", Value: bson.D{{Key: "_data", Value: "8263"}}},
		{Key: "operationType", Value: "insert"},
		{Key: "documentKey", Value: bson.D{{Key: "_id", Value: int32(7)}}},
		{Key: "fullDocument", Value: bson.D{{Key: "_id", Value: int32(7)}, {Key: "a", Value: "x"}}},
	})
	var ev event
	require.NoError(t, bson.Unmarshal(buf, &ev))

	copy(buf, bytes.Repeat([]byte{0xFF}, len(buf)))

	assert.Equal(t, bson.Raw(mustMarshal(t, bson.D{{Key: "_data", Value: "8263"}})), ev.ID, "resume token ссылается на буфер события")
	assert.Equal(t, bson.Raw(mustMarshal(t, bson.D{{Key: "_id", Value: int32(7)}})), ev.DocumentKey, "documentKey ссылается на буфер события")
	assert.Equal(t, bson.Raw(mustMarshal(t, bson.D{{Key: "_id", Value: int32(7)}, {Key: "a", Value: "x"}})), ev.FullDocument,
		"fullDocument ссылается на буфер события")
}
