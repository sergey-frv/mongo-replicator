package verify

import (
	"context"
	"encoding/json"
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

// Отметка, равная T1, ожидание не завершает: в группе событий с одним временем
// применены могут быть не все.
func TestMarkWaitAfterEqual(t *testing.T) {
	m := newMark(make(chan struct{}, 1))
	m.set(at(10))
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := m.waitAfter(ctx, at(10))

	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestMarkWaitAfterLater(t *testing.T) {
	m := newMark(make(chan struct{}, 1))
	m.set(at(11))

	err := m.waitAfter(context.Background(), at(10))

	assert.NoError(t, err)
}

func TestMarkSetBackward(t *testing.T) {
	m := newMark(make(chan struct{}, 1))
	m.set(at(11))

	m.set(at(5))

	got, ok := m.get()
	assert.True(t, ok, "отметка задана")
	assert.Equal(t, at(11), got)
}

// Ждущий виден в waiting и будит цикл; сдвиг отметки за T1 его отпускает.
func TestMarkWaiting(t *testing.T) {
	wake := make(chan struct{}, 1)
	m := newMark(wake)
	done := make(chan error, 1)
	go func() { done <- m.waitAfter(context.Background(), at(10)) }()
	require.Eventually(t, func() bool { return m.waiting() == 1 }, time.Second, 10*time.Millisecond, "ждущий учтён")

	m.set(at(11))

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		require.FailNow(t, "ожидание не отпущено сдвигом отметки")
	}
	assert.Len(t, wake, 1, "цикл разбужен")
	assert.Equal(t, 0, m.waiting())
}

// writeIDs — файл отчёта с записью ns и _id в виде extended JSON idJSON.
func writeIDs(t *testing.T, ns, idJSON string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "defects.json")
	require.NoError(t, writeReport(path, report{{NS: ns, ID: json.RawMessage(idJSON), Reason: "нет на цели"}}))
	return path
}

// Ключ из файла --ids — другой базы: ошибка до подключения к потоку.
func TestRunIDsOtherDB(t *testing.T) {
	v := unitVerifier(Config{IDs: writeIDs(t, "other.items", `1`)})

	_, err := v.Run(context.Background())

	assert.ErrorContains(t, err, "не из базы db")
}

func TestRunIDsBadID(t *testing.T) {
	v := unitVerifier(Config{IDs: writeIDs(t, "db.items", `{"$oid":"zz"}`)})

	_, err := v.Run(context.Background())

	assert.ErrorContains(t, err, "_id ключа db.items")
}

// Продолжение с файла позиции: отчёты прошлого запуска загружаются, и запись
// позиции дописывает к ним новые записи, а не затирает их.
func TestOpenPositionKeepsReports(t *testing.T) {
	const dstURI = "mongodb://mongo-dst:27017"
	cfg := Config{SrcURI: "mongodb://mongo-src:27017", DstURI: dstURI, StateDir: t.TempDir()}
	v := unitVerifier(cfg)
	files, cluster, err := positionPaths(cfg.StateDir, "db", dstURI)
	require.NoError(t, err)
	written := ident{DB: "db", DstDB: "db", Cluster: cluster, SrcCluster: statefile.ClusterKey(cfg.SrcURI)}
	require.NoError(t, writePosition(files.position, &position{ident: written, Since: 1791900000}))
	require.NoError(t, writeReport(files.defects, report{{NS: "db.items", ID: json.RawMessage(`{"$numberInt":"1"}`), Reason: "нет на цели"}}))
	pos, unlock, err := v.openPosition()
	require.NoError(t, err)
	defer unlock()
	require.NotNil(t, pos, "позиция прошлого запуска")

	v.addDefect("db.items", rawID(t, int32(2)), nil, rawDoc(t, bson.D{{Key: "_id", Value: int32(2)}}))
	v.save()

	got, err := readReport(files.defects)
	require.NoError(t, err)
	assert.Equal(t, []string{`{"$numberInt":"1"}`, `{"$numberInt":"2"}`}, []string{string(got[0].ID), string(got[1].ID)})
}

// Битый отчёт прошлого запуска, чьих записей позиция не учла, переносится в
// сторону, а не затирается.
func TestOpenPositionCorruptReport(t *testing.T) {
	const dstURI = "mongodb://mongo-dst:27017"
	cfg := Config{SrcURI: "mongodb://mongo-src:27017", DstURI: dstURI, StateDir: t.TempDir()}
	v := unitVerifier(cfg)
	files, cluster, err := positionPaths(cfg.StateDir, "db", dstURI)
	require.NoError(t, err)
	written := ident{DB: "db", DstDB: "db", Cluster: cluster, SrcCluster: statefile.ClusterKey(cfg.SrcURI)}
	require.NoError(t, writePosition(files.position, &position{ident: written, Since: 1791900000}))
	require.NoError(t, os.WriteFile(files.defects, []byte(`[{"ns":`), 0o600))

	pos, unlock, err := v.openPosition()

	require.NoError(t, err)
	defer unlock()
	assert.NotNil(t, pos, "позиция прошлого запуска")
	assert.Empty(t, v.defects)
	assert.NoFileExists(t, files.defects)
	moved, _ := filepath.Glob(files.defects + ".corrupt-*")
	require.Len(t, moved, 1, "перенесённый отчёт")
	b, err := os.ReadFile(moved[0]) //nolint:gosec // путь из t.TempDir
	require.NoError(t, err)
	assert.Equal(t, `[{"ns":`, string(b))
}

// Отчёта нет, а счётчики позиции учли его записи (отчёты пишутся до позиции,
// файл удалён) — ошибка: пустой новый отчёт навсегда оставил бы их без --ids.
func TestOpenPositionMissingReport(t *testing.T) {
	const dstURI = "mongodb://mongo-dst:27017"
	cfg := Config{SrcURI: "mongodb://mongo-src:27017", DstURI: dstURI, StateDir: t.TempDir()}
	v := unitVerifier(cfg)
	files, cluster, err := positionPaths(cfg.StateDir, "db", dstURI)
	require.NoError(t, err)
	written := ident{DB: "db", DstDB: "db", Cluster: cluster, SrcCluster: statefile.ClusterKey(cfg.SrcURI)}
	require.NoError(t, writePosition(files.position, &position{ident: written, Since: 1791900000, Counts: counts{Defects: 1}}))
	require.NoError(t, writeReport(files.unverified, nil))

	_, _, err = v.openPosition()

	require.ErrorIs(t, err, errReportIncomplete)
	assert.FileExists(t, files.position, "позиция не тронута")
}

// В отчёте меньше записей, чем учли счётчики позиции, — ошибка, файл не
// трогается: перенос или перезапись оставили бы учтённые ключи без --ids.
func TestOpenPositionIncompleteReport(t *testing.T) {
	for _, tc := range []struct {
		name, content string
	}{
		{name: "битый", content: `[{"ns":`},
		{name: "короче счётчика", content: `[{"ns":"db.items","id":{"$numberInt":"1"},"reason":"нет на цели","src":null,"dst":null}]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const dstURI = "mongodb://mongo-dst:27017"
			cfg := Config{SrcURI: "mongodb://mongo-src:27017", DstURI: dstURI, StateDir: t.TempDir()}
			v := unitVerifier(cfg)
			files, cluster, err := positionPaths(cfg.StateDir, "db", dstURI)
			require.NoError(t, err)
			written := ident{DB: "db", DstDB: "db", Cluster: cluster, SrcCluster: statefile.ClusterKey(cfg.SrcURI)}
			require.NoError(t, writePosition(files.position, &position{ident: written, Since: 1791900000, Counts: counts{Defects: 2}}))
			require.NoError(t, os.WriteFile(files.defects, []byte(tc.content), 0o600))

			_, _, err = v.openPosition()

			require.ErrorIs(t, err, errReportIncomplete)
			b, err := os.ReadFile(files.defects)
			require.NoError(t, err)
			assert.Equal(t, tc.content, string(b), "отчёт не тронут")
		})
	}
}

// Счётчик позиции больше предела отчёта: полного по пределу отчёта достаточно.
func TestLoadReportAtLimit(t *testing.T) {
	const entry = `{"ns":"db.items","id":{"$numberInt":"1"},"reason":"нет на цели","src":null,"dst":null}`
	path := filepath.Join(t.TempDir(), "dst.defects.json")
	require.NoError(t, os.WriteFile(path, []byte("["+strings.Repeat(entry+",", reportLimit-1)+entry+"]"), 0o600))

	r, err := loadReport(path, reportLimit+5, "dst.json")

	require.NoError(t, err)
	assert.Len(t, r, 10_000)
}

// Отчёт, не прочитанный не из-за содержимого, — ошибка: файл не затирается.
func TestOpenPositionUnreadableReport(t *testing.T) {
	const dstURI = "mongodb://mongo-dst:27017"
	cfg := Config{SrcURI: "mongodb://mongo-src:27017", DstURI: dstURI, StateDir: t.TempDir()}
	v := unitVerifier(cfg)
	files, cluster, err := positionPaths(cfg.StateDir, "db", dstURI)
	require.NoError(t, err)
	written := ident{DB: "db", DstDB: "db", Cluster: cluster, SrcCluster: statefile.ClusterKey(cfg.SrcURI)}
	require.NoError(t, writePosition(files.position, &position{ident: written, Since: 1791900000}))
	require.NoError(t, os.Mkdir(files.unverified, 0o750), "каталог вместо файла: чтение не удаётся")

	_, _, err = v.openPosition()

	require.Error(t, err)
	assert.DirExists(t, files.unverified)
}

// reportFailVerifier — сверка с дефектом, чей отчёт не записать: каталога
// отчёта о дефектах нет. Возвращает и этот каталог.
func reportFailVerifier(t *testing.T) (v *Verifier, missing string) {
	t.Helper()
	dir := t.TempDir()
	missing = filepath.Join(dir, "missing")
	v = unitVerifier(Config{})
	v.files = paths{
		position:   filepath.Join(dir, "p.json"),
		defects:    filepath.Join(missing, "p.defects.json"),
		unverified: filepath.Join(dir, "p.unverified.json"),
	}
	v.addDefect("db.items", rawID(t, int32(1)), nil, rawDoc(t, bson.D{{Key: "_id", Value: int32(1)}}))
	return v, missing
}

// Отчёт не записан — позиция не пишется: она учла бы записи, которых нет в
// отчётах.
func TestSaveReportFailed(t *testing.T) {
	v, _ := reportFailVerifier(t)

	v.save()

	assert.NoFileExists(t, v.files.position)
}

// После неудачной записи следующая повторяет отчёты, хотя новых записей нет.
func TestSaveRetriesReports(t *testing.T) {
	v, missing := reportFailVerifier(t)
	v.save()
	require.NoError(t, os.Mkdir(missing, 0o700))

	v.save()

	assert.FileExists(t, v.files.defects)
	assert.FileExists(t, v.files.position)
}

// В --ids запись по таймеру не трогает отчёты: файл --ids мог быть прошлым
// отчётом .ids, и отчёт без ключей очереди затёр бы их до конца перепроверки.
func TestSaveIDsKeepsReports(t *testing.T) {
	dir := t.TempDir()
	v := unitVerifier(Config{IDs: filepath.Join(dir, "p.ids.defects.json")})
	v.files = paths{defects: v.cfg.IDs, unverified: filepath.Join(dir, "p.ids.unverified.json")}
	require.NoError(t, writeReport(v.files.defects, report{{NS: "db.items", ID: json.RawMessage(`{"$numberInt":"1"}`), Reason: "нет на цели"}}))
	v.addDefect("db.items", rawID(t, int32(2)), nil, rawDoc(t, bson.D{{Key: "_id", Value: int32(2)}}))

	v.save()

	got, err := readReport(v.files.defects)
	require.NoError(t, err)
	assert.Equal(t, []string{`{"$numberInt":"1"}`}, []string{string(got[0].ID)}, "файл --ids до записи в конце runIDs")
	assert.NoFileExists(t, v.files.unverified)
}
