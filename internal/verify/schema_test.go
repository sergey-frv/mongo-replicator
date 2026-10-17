package verify

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// schemaOf — нормализованная схема из литералов ответов listCollections и listIndexes.
func schemaOf(t *testing.T, colls ...collInfo) map[string]collSchema {
	t.Helper()
	s, err := normalize(colls)
	require.NoError(t, err, "normalize")
	return s
}

// coll — коллекция из ответа listCollections с индексами из listIndexes.
func coll(t *testing.T, spec bson.D, indexes ...bson.D) collInfo {
	t.Helper()
	ci := collInfo{spec: rawDoc(t, spec)}
	for _, ix := range indexes {
		ci.indexes = append(ci.indexes, rawDoc(t, ix))
	}
	return ci
}

// plainColl — ответ listCollections для обычной коллекции без опций; u
// отличает uuid коллекций разных кластеров.
func plainColl(name string, u byte) bson.D {
	return bson.D{
		{Key: "name", Value: name}, {Key: "type", Value: "collection"}, {Key: "options", Value: bson.D{}},
		{Key: "info", Value: bson.D{
			{Key: "readOnly", Value: false},
			{Key: "uuid", Value: bson.Binary{Subtype: bson.TypeBinaryUUID, Data: []byte{u, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, u}}},
		}},
		{Key: "idIndex", Value: bson.D{{Key: "v", Value: 2}, {Key: "key", Value: bson.D{{Key: "_id", Value: 1}}}, {Key: "name", Value: "_id_"}}},
	}
}

func TestNormalizeDoc(t *testing.T) {
	tests := []struct {
		name string
		doc  bson.D
		drop []string
		want bson.D
	}{
		{
			name: "верхний уровень сортируется, вложенный порядок сохраняется",
			doc: bson.D{
				{Key: "name", Value: "b_1_a_-1"},
				{Key: "key", Value: bson.D{{Key: "b", Value: 1}, {Key: "a", Value: -1}}},
			},
			want: bson.D{
				{Key: "key", Value: bson.D{{Key: "b", Value: 1}, {Key: "a", Value: -1}}},
				{Key: "name", Value: "b_1_a_-1"},
			},
		},
		{
			name: "служебные поля индекса",
			doc: bson.D{
				{Key: "v", Value: 2},
				{Key: "key", Value: bson.D{{Key: "a", Value: 1}}},
				{Key: "name", Value: "a_1"},
				{Key: "ns", Value: "db.c"},
			},
			drop: indexDropFields,
			want: bson.D{{Key: "key", Value: bson.D{{Key: "a", Value: 1}}}, {Key: "name", Value: "a_1"}},
		},
		{
			name: "версия collation",
			doc: bson.D{{Key: "collation", Value: bson.D{
				{Key: "locale", Value: "ru"}, {Key: "strength", Value: 2}, {Key: "version", Value: "57.1"},
			}}},
			want: bson.D{{Key: "collation", Value: bson.D{{Key: "locale", Value: "ru"}, {Key: "strength", Value: 2}}}},
		},
		{
			name: "служебные поля info",
			doc: bson.D{
				{Key: "readOnly", Value: false},
				{Key: "uuid", Value: bson.Binary{Subtype: bson.TypeBinaryUUID, Data: make([]byte, 16)}},
			},
			drop: infoDropFields,
			want: bson.D{},
		},
		{name: "поля нет в ответе", want: bson.D{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := normalizeDoc(rawDoc(t, tt.doc), tt.drop...)

			require.NoError(t, err)
			assert.Equal(t, rawDoc(t, tt.want), got)
		})
	}
}

func TestCompareSchema(t *testing.T) {
	idIndex := bson.D{{Key: "v", Value: 2}, {Key: "key", Value: bson.D{{Key: "_id", Value: 1}}}, {Key: "name", Value: "_id_"}}
	idIndexV1 := bson.D{{Key: "v", Value: 1}, {Key: "key", Value: bson.D{{Key: "_id", Value: 1}}}, {Key: "name", Value: "_id_"}}
	aIndex := bson.D{{Key: "v", Value: 2}, {Key: "key", Value: bson.D{{Key: "a", Value: 1}}}, {Key: "name", Value: "a_1"}}
	aUnique := bson.D{{Key: "v", Value: 2}, {Key: "key", Value: bson.D{{Key: "a", Value: 1}}}, {Key: "name", Value: "a_1"},
		{Key: "unique", Value: true}}
	capped := bson.D{{Key: "name", Value: "log"}, {Key: "type", Value: "collection"},
		{Key: "options", Value: bson.D{{Key: "capped", Value: true}, {Key: "size", Value: 4096}}}}
	cappedReordered := bson.D{{Key: "name", Value: "log"}, {Key: "type", Value: "collection"},
		{Key: "options", Value: bson.D{{Key: "size", Value: 4096}, {Key: "capped", Value: true}}}}
	validated := bson.D{{Key: "name", Value: "log"}, {Key: "type", Value: "collection"},
		{Key: "options", Value: bson.D{{Key: "validator", Value: bson.D{{Key: "a", Value: bson.D{{Key: "$exists", Value: true}}}}}}}}
	view := bson.D{{Key: "name", Value: "v"}, {Key: "type", Value: "view"},
		{Key: "options", Value: bson.D{{Key: "viewOn", Value: "items"}, {Key: "pipeline", Value: bson.A{}}}},
		{Key: "info", Value: bson.D{{Key: "readOnly", Value: true}}}}
	viewAsColl := bson.D{{Key: "name", Value: "v"}, {Key: "type", Value: "collection"}, {Key: "options", Value: bson.D{}}}

	tests := []struct {
		name     string
		src, dst []collInfo
		created  map[string]bool
		want     []SchemaDiff
	}{
		{
			name: "служебные поля и порядок опций не различие",
			src:  []collInfo{coll(t, plainColl("items", 1), idIndex, aIndex), coll(t, capped), coll(t, view)},
			dst:  []collInfo{coll(t, plainColl("items", 2), idIndexV1, aIndex), coll(t, cappedReordered), coll(t, view)},
		},
		{
			name: "нет на цели и лишняя на цели",
			src:  []collInfo{coll(t, plainColl("a", 1), idIndex)},
			dst:  []collInfo{coll(t, plainColl("b", 2), idIndex)},
			want: []SchemaDiff{{NS: "db.a", Reason: "нет на цели"}, {NS: "db.b", Reason: "лишняя на цели"}},
		},
		{
			name: "опции различаются",
			src:  []collInfo{coll(t, capped)},
			dst:  []collInfo{coll(t, validated)},
			want: []SchemaDiff{{NS: "db.log", Reason: "опции различаются: capped, size, validator"}},
		},
		{
			name: "тип различается",
			src:  []collInfo{coll(t, view)},
			dst:  []collInfo{coll(t, viewAsColl, idIndex)},
			want: []SchemaDiff{{NS: "db.v", Reason: "тип различается: источник view, цель collection"}},
		},
		{
			name: "индексы: нет, лишний, различается",
			src:  []collInfo{coll(t, plainColl("a", 1), idIndex, aIndex), coll(t, plainColl("b", 1), idIndex, aIndex)},
			dst:  []collInfo{coll(t, plainColl("a", 2), idIndex), coll(t, plainColl("b", 2), idIndex, aUnique)},
			want: []SchemaDiff{
				{NS: "db.a", Reason: "индекс a_1: нет на цели"},
				{NS: "db.b", Reason: "индекс a_1: различается: unique"},
			},
		},
		{
			name: "лишний индекс на цели",
			src:  []collInfo{coll(t, plainColl("a", 1), idIndex)},
			dst:  []collInfo{coll(t, plainColl("a", 2), idIndex, aIndex)},
			want: []SchemaDiff{{NS: "db.a", Reason: "индекс a_1: лишний на цели"}},
		},
		{
			name:    "создана в потоке: опции — известное ограничение, индекс — дефект",
			src:     []collInfo{coll(t, capped, aIndex)},
			dst:     []collInfo{coll(t, validated)},
			created: map[string]bool{"log": true},
			want: []SchemaDiff{
				{NS: "db.log", Reason: "опции различаются: capped, size, validator", Known: true},
				{NS: "db.log", Reason: "индекс a_1: нет на цели"},
			},
		},
		{
			name:    "создан в потоке: view нет на цели — известное ограничение",
			src:     []collInfo{coll(t, view), coll(t, plainColl("items", 1), idIndex)},
			created: map[string]bool{"v": true, "items": true},
			want: []SchemaDiff{
				{NS: "db.items", Reason: "нет на цели"},
				{NS: "db.v", Reason: "нет на цели", Known: true},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src, dst := schemaOf(t, tt.src...), schemaOf(t, tt.dst...)

			got := compareSchema("db", src, dst, tt.created)

			assert.Equal(t, tt.want, got)
		})
	}
}
