package verify

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"github.com/sergey-frv/mongo-replicator/internal/replicator"
)

// codeNamespaceNotFound — коллекция удалена между listCollections и listIndexes.
const codeNamespaceNotFound = 26

// Служебные поля, которые сервер заполняет сам и которые между кластерами
// различаются всегда, и поля, зависящие от версии сервера. Список пополняется
// по результату первого --schema-only на стенде.
var (
	// info коллекции: uuid у каждой своя, readOnly — режим узла
	infoDropFields = []string{"uuid", "readOnly"}
	// спецификация индекса: v — версия формата индекса, ns — только у старых серверов
	indexDropFields = []string{"v", "ns"}
	// collation (в опциях коллекции и в индексе): version — версия ICU сервера
	collationDropFields = []string{"version"}
)

// SchemaDiff — расхождение схемы одной коллекции или view.
type SchemaDiff struct {
	NS     string // пространство имён источника
	Reason string
	// Known — известное ограничение, а не дефект: коллекция создана после
	// начала сверки, а опции create репликатор не переносит (view не создаёт).
	Known bool
}

// collSchema — нормализованная коллекция: верхнеуровневые поля документов
// отсортированы, служебные и версионные поля убраны.
type collSchema struct {
	typ     string              // collection, view
	options bson.Raw            // options из listCollections
	info    bson.Raw            // info из listCollections без infoDropFields
	indexes map[string]bson.Raw // по имени индекса; у view индексов нет
}

// collInfo — коллекция из listCollections и её индексы из listIndexes, как их
// вернул сервер.
type collInfo struct {
	spec    bson.Raw
	indexes []bson.Raw
}

// checkSchema сверяет схему базы srcDB источника и dstDB цели: коллекции, их
// тип, опции и индексы, views. created — коллекции, созданные в потоке после
// начала сверки: расхождение их опций — известное ограничение. Расхождения —
// по пространствам имён источника в порядке имён.
func checkSchema(ctx context.Context, src, dst *mongo.Client, srcDB, dstDB string, created map[string]bool) ([]SchemaDiff, error) {
	srcSchema, err := readSchema(ctx, src, srcDB)
	if err != nil {
		return nil, fmt.Errorf("схема источника: %w", err)
	}
	dstSchema, err := readSchema(ctx, dst, dstDB)
	if err != nil {
		return nil, fmt.Errorf("схема цели: %w", err)
	}
	return compareSchema(srcDB, srcSchema, dstSchema, created), nil
}

// readSchema читает с primary коллекции базы db (без system.* и служебной
// коллекции чекпойнта) и индексы каждой обычной коллекции.
func readSchema(ctx context.Context, c *mongo.Client, db string) (map[string]collSchema, error) {
	d := c.Database(db, options.Database().SetReadPreference(readpref.Primary()))
	cur, err := d.ListCollections(ctx, bson.M{
		"name": bson.M{"$not": bson.Regex{Pattern: `^system\.`}, "$ne": replicator.CheckpointColl},
	})
	if err != nil {
		return nil, fmt.Errorf("listCollections: %w", err)
	}
	var specs []bson.Raw
	if err = cur.All(ctx, &specs); err != nil {
		return nil, fmt.Errorf("listCollections: %w", err)
	}
	colls := make([]collInfo, 0, len(specs))
	for _, spec := range specs {
		ci := collInfo{spec: spec}
		name, _ := spec.Lookup("name").StringValueOK()
		if typ, _ := spec.Lookup("type").StringValueOK(); typ == "collection" {
			ci.indexes, err = listIndexes(ctx, d.Collection(name))
			var se mongo.ServerError
			if errors.As(err, &se) && se.HasErrorCode(codeNamespaceNotFound) {
				continue // удалена после listCollections: её уже нет
			}
			if err != nil {
				return nil, fmt.Errorf("listIndexes %s: %w", name, err)
			}
		}
		colls = append(colls, ci)
	}
	return normalize(colls)
}

func listIndexes(ctx context.Context, coll *mongo.Collection) ([]bson.Raw, error) {
	cur, err := coll.Indexes().List(ctx)
	if err != nil {
		return nil, err
	}
	var specs []bson.Raw
	err = cur.All(ctx, &specs)
	return specs, err
}

// normalize приводит ответы listCollections и listIndexes к виду для сравнения.
// idIndex из listCollections не нужен: индекс _id_ есть в listIndexes.
func normalize(colls []collInfo) (map[string]collSchema, error) {
	out := make(map[string]collSchema, len(colls))
	for _, ci := range colls {
		var c struct {
			Name    string   `bson:"name"`
			Type    string   `bson:"type"`
			Options bson.Raw `bson:"options"`
			Info    bson.Raw `bson:"info"`
		}
		if err := bson.Unmarshal(ci.spec, &c); err != nil {
			return nil, fmt.Errorf("listCollections: %w", err)
		}
		s := collSchema{typ: c.Type, indexes: make(map[string]bson.Raw, len(ci.indexes))}
		var err error
		if s.options, err = normalizeDoc(c.Options); err != nil {
			return nil, fmt.Errorf("опции %s: %w", c.Name, err)
		}
		if s.info, err = normalizeDoc(c.Info, infoDropFields...); err != nil {
			return nil, fmt.Errorf("info %s: %w", c.Name, err)
		}
		for _, spec := range ci.indexes {
			name, _ := spec.Lookup("name").StringValueOK()
			if s.indexes[name], err = normalizeDoc(spec, indexDropFields...); err != nil {
				return nil, fmt.Errorf("индекс %s.%s: %w", c.Name, name, err)
			}
		}
		out[c.Name] = s
	}
	return out, nil
}

// normalizeDoc убирает верхнеуровневые поля drop и сортирует оставшиеся по
// имени; во вложенном collation убирает collationDropFields. Порядок полей
// глубже не трогается: в ключе индекса и в pipeline он значим. Пустой doc
// (поля нет в ответе) — пустой документ.
func normalizeDoc(doc bson.Raw, drop ...string) (bson.Raw, error) {
	if len(doc) == 0 {
		return bson.Marshal(bson.D{})
	}
	els, err := doc.Elements()
	if err != nil {
		return nil, err
	}
	d := make(bson.D, 0, len(els))
	for _, el := range els {
		if slices.Contains(drop, el.Key()) {
			continue
		}
		v := el.Value()
		if coll, ok := v.DocumentOK(); ok && el.Key() == "collation" {
			if v, err = withoutFields(coll, collationDropFields); err != nil {
				return nil, fmt.Errorf("collation: %w", err)
			}
		}
		d = append(d, bson.E{Key: el.Key(), Value: v})
	}
	slices.SortFunc(d, func(a, b bson.E) int { return cmp.Compare(a.Key, b.Key) })
	return bson.Marshal(d)
}

// withoutFields — документ doc без полей drop, порядок остальных сохраняется.
func withoutFields(doc bson.Raw, drop []string) (bson.RawValue, error) {
	els, err := doc.Elements()
	if err != nil {
		return bson.RawValue{}, err
	}
	d := make(bson.D, 0, len(els))
	for _, el := range els {
		if !slices.Contains(drop, el.Key()) {
			d = append(d, bson.E{Key: el.Key(), Value: el.Value()})
		}
	}
	b, err := bson.Marshal(d)
	return bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: b}, err
}

// compareSchema сравнивает нормализованные схемы; ns расхождений — в базе srcDB.
func compareSchema(srcDB string, src, dst map[string]collSchema, created map[string]bool) []SchemaDiff {
	var diffs []SchemaDiff
	for _, name := range unionKeys(src, dst) {
		ns := srcDB + "." + name
		s, inSrc := src[name]
		d, inDst := dst[name]
		switch {
		case !inDst:
			// view в потоке репликатор не создаёт
			diffs = append(diffs, SchemaDiff{NS: ns, Reason: "нет на цели", Known: created[name] && s.typ == "view"})
		case !inSrc:
			diffs = append(diffs, SchemaDiff{NS: ns, Reason: "лишняя на цели"})
		default:
			diffs = append(diffs, compareColl(ns, s, d, created[name])...)
		}
	}
	return diffs
}

// compareColl сравнивает одну коллекцию, есть на обеих сторонах. created —
// коллекция создана в потоке: расхождение типа и опций — известное ограничение,
// индексы репликатор переносит, и их расхождение — дефект.
func compareColl(ns string, s, d collSchema, created bool) []SchemaDiff {
	var diffs []SchemaDiff
	if s.typ != d.typ {
		return append(diffs, SchemaDiff{NS: ns, Known: created,
			Reason: fmt.Sprintf("тип различается: источник %s, цель %s", s.typ, d.typ)})
	}
	fields := diffFields(s.options, d.options)
	for _, f := range diffFields(s.info, d.info) {
		fields = append(fields, "info."+f)
	}
	if len(fields) > 0 {
		diffs = append(diffs, SchemaDiff{NS: ns, Known: created,
			Reason: "опции различаются: " + strings.Join(fields, ", ")})
	}

	for _, name := range unionKeys(s.indexes, d.indexes) {
		si, inSrc := s.indexes[name]
		di, inDst := d.indexes[name]
		reason := ""
		switch {
		case !inDst:
			reason = "нет на цели"
		case !inSrc:
			reason = "лишний на цели"
		case !bytes.Equal(si, di):
			reason = "различается: " + strings.Join(diffFields(si, di), ", ")
		default:
			continue
		}
		diffs = append(diffs, SchemaDiff{NS: ns, Reason: "индекс " + name + ": " + reason})
	}
	return diffs
}

// unionKeys — имена из обеих карт по порядку.
func unionKeys[V any](a, b map[string]V) []string {
	names := slices.Collect(maps.Keys(a))
	for k := range b {
		if _, ok := a[k]; !ok {
			names = append(names, k)
		}
	}
	slices.Sort(names)
	return names
}
