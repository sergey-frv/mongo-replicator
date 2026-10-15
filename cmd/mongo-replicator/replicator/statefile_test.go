package replicator

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClusterKey(t *testing.T) {
	for _, tc := range []struct {
		name, uri, want string
	}{
		{"srv", "mongodb+srv://Cluster0.Example.NET/", "srv:cluster0.example.net"},
		{"srv с учёткой и параметрами", "mongodb+srv://u:p@cluster0.example.net/db?srvMaxHosts=2&tls=true",
			"srv:cluster0.example.net"},
		{"srv без пути", "mongodb+srv://cluster0.example.net", "srv:cluster0.example.net"},
		{"схема в верхнем регистре", "MONGODB+SRV://cluster0.example.net/", "srv:cluster0.example.net"},
		{"один хост без порта", "mongodb://h1", "h1:27017"},
		{"один хост с портом", "mongodb://h1:27017/", "h1:27017"},
		{"порядок и регистр", "mongodb://H2:27018,h1/?replicaSet=rs0", "h1:27017,h2:27018"},
		{"пароль с ? и @", "mongodb://u:p?w@d@h1,h2/db", "h1:27017,h2:27017"},
		{"параметры без пути", "mongodb://h1?replicaSet=rs0", "h1:27017"},
		{"ipv6", "mongodb://[::1],[::2]:27018/", "[::1]:27017,[::2]:27018"},
		{"без схемы", "h1,h2", "h1:27017,h2:27017"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, clusterKey(tc.uri))
		})
	}

	srv := clusterKey("mongodb+srv://c.example.net/?srvMaxHosts=1")
	assert.Equal(t, srv, clusterKey("mongodb+srv://c.example.net/?srvMaxHosts=3&appName=x"),
		"srvMaxHosts и параметры не влияют")
	assert.Equal(t, clusterKey("mongodb://h1,h2:27017"), clusterKey("mongodb://h2,H1:27017/db"))
	assert.NotEqual(t, clusterKey("mongodb://h1,h2"), clusterKey("mongodb://h1,h3"), "разные кластеры")
	assert.NotEqual(t, clusterKey("mongodb://h1"), clusterKey("mongodb+srv://h1"), "srv и список хостов")
}

func TestStateFileName(t *testing.T) {
	a := stateFileName("app", clusterKey("mongodb://h1,h2"))
	assert.Regexp(t, regexp.MustCompile(`^app\.[0-9a-f]{8}\.json$`), a)
	assert.Equal(t, a, stateFileName("app", clusterKey("mongodb://h2:27017,H1/")))
	assert.NotEqual(t, a, stateFileName("app", clusterKey("mongodb://h3")), "другой кластер")
	assert.NotEqual(t, a, stateFileName("app2", clusterKey("mongodb://h1,h2")), "другая база")
}

func TestWriteReadState(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.0123abcd.json")

	got, err := readState(path)
	require.NoError(t, err)
	assert.Equal(t, state{}, got, "нет файла — нули")

	at := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	first := state{DB: "app", DstDB: "app", Cluster: "h1:27017", Owner: "host:1:abc",
		StartedAt: at, UpdatedAt: at.Add(time.Second),
		Traffic: TrafficInfo{SrcIn: 1 << 40, SrcOut: 2, DstIn: 3, DstOut: 4}}
	require.NoError(t, writeState(path, first))
	got, err = readState(path)
	require.NoError(t, err)
	assert.Equal(t, first, got)

	second := state{DB: "app", DstDB: "app", UpdatedAt: at.Add(time.Minute), Traffic: TrafficInfo{DstOut: 5}}
	require.NoError(t, writeState(path, second))
	got, err = readState(path)
	require.NoError(t, err)
	assert.Equal(t, second, got, "содержимое заменено целиком")

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1, "временных файлов нет")
	assert.Equal(t, "app.0123abcd.json", entries[0].Name())
	fi, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o640), fi.Mode().Perm(), "группа читает файл для сводки")
}

func TestWriteStateNoDir(t *testing.T) {
	err := writeState(filepath.Join(t.TempDir(), "missing", "app.json"), state{})
	require.Error(t, err)
}

func TestReadStateCorrupt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"traffic": `), 0o600))
	_, err := readState(path)
	require.ErrorIs(t, err, ErrStateCorrupt)
}

func TestMoveAside(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.0123abcd.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"db":"app"}`), 0o600))

	to, err := moveAside(path, "corrupt")
	require.NoError(t, err)
	assert.Regexp(t, regexp.MustCompile(`/app\.0123abcd\.json\.corrupt-\d{8}T\d{6}Z$`), to)
	assert.NoFileExists(t, path)
	b, err := os.ReadFile(to)
	require.NoError(t, err)
	assert.JSONEq(t, `{"db":"app"}`, string(b), "содержимое сохранено")

	_, err = moveAside(path, "mismatch")
	require.Error(t, err, "файла уже нет")
}

func TestMachineTraffic(t *testing.T) {
	now := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)

	t.Run("пустой каталог", func(t *testing.T) {
		mt, err := machineTraffic(t.TempDir(), now)
		require.NoError(t, err)
		assert.Equal(t, MachineTraffic{At: now}, mt)
	})

	t.Run("сумма и активные", func(t *testing.T) {
		dir := t.TempDir()
		write := func(name string, s state) {
			require.NoError(t, writeState(filepath.Join(dir, name), s))
		}
		write("a.00000001.json", state{UpdatedAt: now.Add(-5 * time.Second),
			Traffic: TrafficInfo{SrcIn: 1, SrcOut: 2, DstIn: 3, DstOut: 4}})
		write("b.00000002.json", state{UpdatedAt: now.Add(-29 * time.Second),
			Traffic: TrafficInfo{SrcIn: 10, SrcOut: 20, DstIn: 30, DstOut: 40}})
		write("c.00000003.json", state{UpdatedAt: now.Add(-time.Hour),
			Traffic: TrafficInfo{SrcIn: 100, SrcOut: 200, DstIn: 300, DstOut: 400}})
		// не входят в сводку: битый, перенесённый в сторону, не *.json
		require.NoError(t, os.WriteFile(filepath.Join(dir, "d.00000004.json"), []byte("{"), 0o600))
		write("e.00000005.json.corrupt-20261015T110000Z", state{Traffic: TrafficInfo{SrcIn: 1000}})
		write("notes.txt", state{Traffic: TrafficInfo{SrcIn: 1000}})

		mt, err := machineTraffic(dir, now)
		require.NoError(t, err)
		assert.Equal(t, MachineTraffic{At: now, Processes: 3, Active: 2,
			Traffic: TrafficInfo{SrcIn: 111, SrcOut: 222, DstIn: 333, DstOut: 444}}, mt)
	})
}

func TestStatePath(t *testing.T) {
	dir := t.TempDir()
	path, cluster := StatePath(dir, "app", "mongodb://u:p@H2,h1:27017/app?replicaSet=rs0")
	assert.Equal(t, "h1:27017,h2:27017", cluster)
	assert.Equal(t, filepath.Join(dir, stateFileName("app", cluster)), path)
}

// newState — репликатор с файлом состояния в dir (без кластеров: startState их не трогает).
func newState(t *testing.T, dir string, tr *Traffic) *Replicator {
	t.Helper()
	quietLog(t)
	path, cluster := StatePath(dir, "app", "mongodb://h1")
	r := New(nil, nil, Config{DB: "src", DstDB: "app", Traffic: tr, StateFile: path, StateDir: dir, Cluster: cluster})
	t.Cleanup(func() { r.stopState() })
	return r
}

func TestStartStateNoFile(t *testing.T) {
	dir := t.TempDir()
	tr := &Traffic{}
	tr.SrcIn.Store(5)
	r := newState(t, dir, tr)

	r.startState()
	s, err := loadState(r.cfg.StateFile)
	require.NoError(t, err, "первая запись — сразу после загрузки")
	assert.Equal(t, state{DB: "src", DstDB: "app", Cluster: "h1:27017", Owner: r.owner,
		StartedAt: r.Status().Started.UTC(), UpdatedAt: s.UpdatedAt, Traffic: TrafficInfo{SrcIn: 5}},
		s)
	assert.WithinDuration(t, time.Now(), s.UpdatedAt, 5*time.Second)
	assert.True(t, r.stopState(), "писатель запущен")
	assert.False(t, r.stopState(), "повторная остановка безвредна")
}

func TestStartStateContinues(t *testing.T) {
	dir := t.TempDir()
	tr := &Traffic{}
	r := newState(t, dir, tr)
	require.NoError(t, writeState(r.cfg.StateFile, state{DB: "src", DstDB: "app", Cluster: "h1:27017",
		Owner: "old:1:00000000", Traffic: TrafficInfo{SrcIn: 100, SrcOut: 200, DstIn: 300, DstOut: 400}}))

	r.startState()
	tr.DstOut.Store(7)
	r.stopState()
	r.writeStateFile()
	s, err := loadState(r.cfg.StateFile)
	require.NoError(t, err)
	assert.Equal(t, TrafficInfo{SrcIn: 100, SrcOut: 200, DstIn: 300, DstOut: 407}, s.Traffic, "счёт — от прошлого значения")
	assert.Equal(t, r.owner, s.Owner)
}

// Писатель обновляет файл сам, без Run.
func TestStartStateWriter(t *testing.T) {
	tr := &Traffic{}
	r := newState(t, t.TempDir(), tr)
	r.stateEvery = 10 * time.Millisecond
	r.startState()
	tr.SrcIn.Store(42)
	require.Eventually(t, func() bool {
		s, err := loadState(r.cfg.StateFile)
		return err == nil && s.Traffic.SrcIn == 42
	}, 5*time.Second, 10*time.Millisecond)
}

// База загружается один раз за процесс: повторный Run файл не перечитывает.
func TestStartStateOnce(t *testing.T) {
	tr := &Traffic{}
	r := newState(t, t.TempDir(), tr)
	r.startState()
	r.stopState()
	require.NoError(t, writeState(r.cfg.StateFile, state{DB: "src", DstDB: "app", Cluster: "h1:27017",
		Traffic: TrafficInfo{SrcIn: 1000}}))
	r.startState()
	assert.Nil(t, r.stateStop, "писатель не перезапущен")
	assert.Equal(t, TrafficInfo{}, r.stateBase)
}

func TestStartStateMovesAside(t *testing.T) {
	for _, tc := range []struct {
		name, content, reason string
	}{
		{"битый", `{"traffic": `, "corrupt"},
		{"другая база источника", `{"db":"other","dst_db":"app","cluster":"h1:27017","traffic":{"src_in":9}}`, "mismatch"},
		{"другой кластер", `{"db":"src","dst_db":"app","cluster":"h9:27017","traffic":{"src_in":9}}`, "mismatch"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			r := newState(t, dir, &Traffic{})
			require.NoError(t, os.WriteFile(r.cfg.StateFile, []byte(tc.content), 0o600))

			r.startState()
			s, err := loadState(r.cfg.StateFile)
			require.NoError(t, err)
			assert.Equal(t, TrafficInfo{}, s.Traffic, "счёт с нуля")
			aside, err := filepath.Glob(r.cfg.StateFile + "." + tc.reason + "-*")
			require.NoError(t, err)
			require.Len(t, aside, 1, "старый файл перенесён в сторону")
			b, err := os.ReadFile(aside[0])
			require.NoError(t, err)
			assert.Equal(t, tc.content, string(b), "история не стёрта")
		})
	}
}

// Файл не прочитан (не файл, а каталог): писать в него нельзя — писатель не запускается.
func TestStartStateUnreadable(t *testing.T) {
	r := newState(t, t.TempDir(), &Traffic{})
	require.NoError(t, os.Mkdir(r.cfg.StateFile, 0o750))
	r.startState()
	assert.Nil(t, r.stateStop)
	assert.DirExists(t, r.cfg.StateFile)
}

func TestLogMachine(t *testing.T) {
	dir := t.TempDir()
	r := newState(t, dir, &Traffic{})
	r.startState()
	r.stopState()
	r.logMachine(context.Background())
	m := r.Status().Machine
	require.NotNil(t, m, "первая сводка — сразу")
	assert.Equal(t, 1, m.Processes)
	assert.Equal(t, 1, m.Active)

	r.cfg.StateDir = ""
	r.lastMachine = time.Time{}
	r.update(func(s *Status) { s.Machine = nil })
	r.logMachine(context.Background())
	assert.Nil(t, r.Status().Machine, "без каталога сводки нет")
}
