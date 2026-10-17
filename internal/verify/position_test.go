package verify

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/sergey-frv/mongo-replicator/internal/statefile"
)

// testIdent — пара баз и кластеры позиции в тестах.
var testIdent = ident{DB: "src", DstDB: "dst", Cluster: "mongo-dst:27017", SrcCluster: "mongo-src:27017"}

// objectID — ObjectId из hex-литерала.
func objectID(t *testing.T, hex string) bson.ObjectID {
	t.Helper()
	id, err := bson.ObjectIDFromHex(hex)
	require.NoError(t, err, "bson.ObjectIDFromHex")
	return id
}

func decimal(t *testing.T, s string) bson.Decimal128 {
	t.Helper()
	d, err := bson.ParseDecimal128(s)
	require.NoError(t, err, "bson.ParseDecimal128")
	return d
}

// fillReport — отчёт после n добавлений записей с _id int32.
func fillReport(t *testing.T, n int) report {
	t.Helper()
	var r report
	for i := range n {
		require.NoError(t, r.add("src.c", rawID(t, int32(i)), "defect", nil, nil)) //nolint:gosec // n в пределах int32
	}
	return r
}

func TestPositionPaths(t *testing.T) {
	dir := t.TempDir()

	p, cluster, err := positionPaths(dir, "dst", "mongodb://mongo-dst:27017/?replicaSet=rs")

	require.NoError(t, err)
	assert.Equal(t, paths{
		position:   filepath.Join(dir, "verify", "dst.475b7295.json"),
		lock:       filepath.Join(dir, "verify", "dst.475b7295.lock"),
		defects:    filepath.Join(dir, "verify", "dst.475b7295.defects.json"),
		unverified: filepath.Join(dir, "verify", "dst.475b7295.unverified.json"),
	}, p)
	assert.Equal(t, "mongo-dst:27017", cluster)
	assert.DirExists(t, filepath.Join(dir, "verify"))
}

// Ключи после записи и чтения совпадают побайтно: тип _id сохраняется, порядок
// полей вложенного документа тоже.
func TestPositionRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dst.json")
	want := &position{
		ident:      testIdent,
		Since:      1791900000,
		Token:      rawDoc(t, bson.D{{Key: "_data", Value: "8268F1A2B3000000012B042C0100296E5A1004"}}),
		StreamTime: bson.Timestamp{T: 1791900100, I: 3},
		Pending: []*item{
			{ns: "c1", id: rawID(t, objectID(t, "65a1b2c3d4e5f60718293a4b")), first: at(10), last: at(20), tries: 2},
			{ns: "c1", id: rawID(t, "строка"), first: at(11), last: at(11)},
			{ns: "c2", id: rawID(t, bson.D{{Key: "b", Value: int64(1)}, {Key: "a", Value: "x"}}), first: at(12), last: at(15)},
			{ns: "c2", id: rawID(t, decimal(t, "1.10")), first: at(13), last: at(13)},
		},
		Created: []string{"log"},
		Counts:  counts{Checked: 7, Matched: 4, Defects: 1, Unverified: 2, SampledOut: 3},
	}

	require.NoError(t, writePosition(path, want))
	got, err := loadPosition(path, testIdent, false)

	require.NoError(t, err)
	assert.Equal(t, want, got)
}

// Позиция без токена (поток не начат) и без ключей.
func TestPositionRoundTripEmpty(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dst.json")
	want := &position{ident: testIdent, Since: 1791900000, Pending: []*item{}}

	require.NoError(t, writePosition(path, want))
	got, err := loadPosition(path, testIdent, false)

	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestLoadPositionMissing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dst.json")

	got, err := loadPosition(path, testIdent, false)

	require.NoError(t, err)
	assert.Nil(t, got)
}

// Битый файл переносится в сторону, сверка начинается с --since.
func TestLoadPositionCorrupt(t *testing.T) {
	for name, content := range map[string]string{
		"не JSON":     `{"db":`,
		"битый _id":   `{"pending":[{"ns":"c","id":{"$oid":"zz"}}]}`,
		"нет _id":     `{"pending":[{"ns":"c"}]}`,
		"битый токен": `{"token":{"_data":{"$numberInt":"x"}}}`,
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "dst.json")
			require.NoError(t, os.WriteFile(path, []byte(content), 0o600))

			got, err := loadPosition(path, testIdent, false)

			require.NoError(t, err)
			assert.Nil(t, got)
			assert.NoFileExists(t, path)
			moved, _ := filepath.Glob(path + ".corrupt-*")
			assert.Len(t, moved, 1, "перенесённые файлы")
		})
	}
}

// Позиция другой пары баз или других кластеров при заданной --since переносится в сторону.
func TestLoadPositionMismatch(t *testing.T) {
	for name, written := range map[string]ident{
		"база источника":    {DB: "other", DstDB: "dst", Cluster: "mongo-dst:27017", SrcCluster: "mongo-src:27017"},
		"база цели":         {DB: "src", DstDB: "other", Cluster: "mongo-dst:27017", SrcCluster: "mongo-src:27017"},
		"кластер цели":      {DB: "src", DstDB: "dst", Cluster: "other:27017", SrcCluster: "mongo-src:27017"},
		"кластер источника": {DB: "src", DstDB: "dst", Cluster: "mongo-dst:27017", SrcCluster: "other:27017"},
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "dst.json")
			require.NoError(t, writePosition(path, &position{ident: written, Since: 1791900000}))

			got, err := loadPosition(path, testIdent, true)

			require.NoError(t, err)
			assert.Nil(t, got)
			assert.NoFileExists(t, path)
			moved, _ := filepath.Glob(path + ".mismatch-*")
			assert.Len(t, moved, 1, "перенесённые файлы")
		})
	}
}

// Без --since позиция другой сверки — ошибка, файл остаётся на месте: опечатка
// в --db или URI не отрывает настоящую позицию.
func TestLoadPositionMismatchNoSince(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dst.json")
	written := ident{DB: "other", DstDB: "dst", Cluster: "mongo-dst:27017", SrcCluster: "mongo-src:27017"}
	require.NoError(t, writePosition(path, &position{ident: written, Since: 1791900000}))

	got, err := loadPosition(path, testIdent, false)

	require.ErrorIs(t, err, ErrPositionMismatch)
	assert.Nil(t, got)
	assert.FileExists(t, path)
	moved, _ := filepath.Glob(path + ".mismatch-*")
	assert.Empty(t, moved, "перенесённые файлы")
}

// Запись отчёта читается обратно: _id побайтно, документы — canonical
// extended JSON, null — документа нет.
func TestReportRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dst.defects.json")
	id := rawID(t, int64(42))
	var r report
	require.NoError(t, r.add("src.c", id, "defect", rawDoc(t, bson.D{{Key: "_id", Value: int64(42)}, {Key: "n", Value: int32(1)}}), nil))

	require.NoError(t, writeReport(path, r))
	got, err := readReport(path)

	require.NoError(t, err)
	require.Len(t, got, 1)
	src := `{"_id":{"$numberLong":"42"},"n":{"$numberInt":"1"}}`
	assert.Equal(t, reportEntry{NS: "src.c", ID: []byte(`{"$numberLong":"42"}`), Reason: "defect", Src: &src}, got[0])
	gotID, err := got[0].id()
	require.NoError(t, err)
	assert.Equal(t, id, gotID)
}

// Файлы сверки — только владельцу: в отчётах документы баз.
func TestWriteReportPrivate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dst.defects.json")

	require.NoError(t, writeReport(path, nil))

	fi, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), fi.Mode().Perm())
}

// Пустой отчёт — "[]": отчёт прошлого запуска не остаётся.
func TestWriteReportEmpty(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dst.defects.json")
	require.NoError(t, os.WriteFile(path, []byte(`[{"ns":"old"}]`), 0o600))

	require.NoError(t, writeReport(path, nil))

	b, err := os.ReadFile(path) //nolint:gosec // путь из t.TempDir
	require.NoError(t, err)
	assert.Equal(t, "[]", string(b))
}

// Документ длиннее reportDocMax обрезается до него с «…» в конце.
func TestReportTruncatesDocument(t *testing.T) {
	var r report

	require.NoError(t, r.add("src.c", rawID(t, int32(1)), "defect",
		rawDoc(t, bson.D{{Key: "s", Value: strings.Repeat("a", 5000)}}), nil))

	require.Len(t, r, 1)
	require.NotNil(t, r[0].Src)
	assert.Len(t, *r[0].Src, reportDocMax)
	assert.Equal(t, `{"s":"aaa`, (*r[0].Src)[:9])
	assert.Equal(t, "a…", (*r[0].Src)[reportDocMax-4:])
}

func TestReportLimit(t *testing.T) {
	r := fillReport(t, reportLimit+5)

	assert.Len(t, r, reportLimit)
}

func TestTruncate(t *testing.T) {
	for name, tc := range map[string]struct {
		s    string
		n    int
		want string
	}{
		"короче предела":     {s: "abc", n: 4, want: "abc"},
		"ровно предел":       {s: "абв", n: 6, want: "абв"},
		"срез по границе":    {s: "абв", n: 5, want: "а…"},
		"срез посреди буквы": {s: "абвг", n: 6, want: "а…"},
		"ASCII":              {s: "abcdef", n: 5, want: "ab…"},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, truncate(tc.s, tc.n))
		})
	}
}

// Файлы сверки лежат в подкаталоге verify/: сводка трафика считает только файл
// состояния репликатора.
func TestSummarySkipsVerifyDir(t *testing.T) {
	dir := t.TempDir()
	statePath, _ := statefile.Path(dir, "dst", "mongodb://mongo-dst:27017")
	require.NoError(t, statefile.Write(statePath, statefile.State{DB: "src", DstDB: "dst", UpdatedAt: time.Now()}))
	p, _, err := positionPaths(dir, "dst", "mongodb://mongo-dst:27017")
	require.NoError(t, err)
	require.NoError(t, writePosition(p.position, &position{ident: testIdent, Since: 1791900000}))
	require.NoError(t, writeReport(p.defects, nil))

	m, err := statefile.Summary(dir, time.Now(), time.Minute)

	require.NoError(t, err)
	assert.Equal(t, 1, m.Processes, "файлы состояния в сводке")
}
