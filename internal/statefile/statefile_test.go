package statefile

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sergey-frv/mongo-replicator/internal/traffic"
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
			assert.Equal(t, tc.want, ClusterKey(tc.uri))
		})
	}

	srv := ClusterKey("mongodb+srv://c.example.net/?srvMaxHosts=1")

	assert.Equal(t, srv, ClusterKey("mongodb+srv://c.example.net/?srvMaxHosts=3&appName=x"),
		"srvMaxHosts и параметры не влияют")
	assert.Equal(t, ClusterKey("mongodb://h1,h2:27017"), ClusterKey("mongodb://h2,H1:27017/db"))
	assert.NotEqual(t, ClusterKey("mongodb://h1,h2"), ClusterKey("mongodb://h1,h3"), "разные кластеры")
	assert.NotEqual(t, ClusterKey("mongodb://h1"), ClusterKey("mongodb+srv://h1"), "srv и список хостов")
}

func TestName(t *testing.T) {
	a, cluster := Name("app", "mongodb://u:p@h1,h2/app?replicaSet=rs0")

	assert.Regexp(t, regexp.MustCompile(`^app\.[0-9a-f]{8}\.json$`), a)
	assert.Equal(t, "h1:27017,h2:27017", cluster)
	assert.Equal(t, a, first(Name("app", "mongodb://h2:27017,H1/")), "порядок и регистр хостов")
	assert.NotEqual(t, a, first(Name("app", "mongodb://h3")), "другой кластер")
	assert.NotEqual(t, a, first(Name("app2", "mongodb://h1,h2")), "другая база")
}

func TestNameSRV(t *testing.T) {
	name, cluster := Name("app", "mongodb+srv://u:p@Cluster0.Example.NET/?srvMaxHosts=2")

	assert.Equal(t, "srv:cluster0.example.net", cluster)
	assert.Equal(t, name, first(Name("app", "mongodb+srv://cluster0.example.net/?srvMaxHosts=3")),
		"srvMaxHosts не влияет")
	assert.NotEqual(t, name, first(Name("app", "mongodb://cluster0.example.net")), "srv и список хостов")
}

// first — первое из двух возвращаемых значений.
func first(a, _ string) string { return a }

func TestWriteReadState(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.0123abcd.json")

	_, err := Load(path)
	require.ErrorIs(t, err, fs.ErrNotExist, "нет файла")

	at := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	first := State{DB: "app", DstDB: "app", Cluster: "h1:27017", Owner: "host:1:abc",
		StartedAt: at, UpdatedAt: at.Add(time.Second),
		Traffic: traffic.Info{SrcIn: 1 << 40, SrcOut: 2, DstIn: 3, DstOut: 4}}

	require.NoError(t, Write(path, first))
	got, err := Load(path)

	require.NoError(t, err)
	assert.Equal(t, first, got)

	second := State{DB: "app", DstDB: "app", UpdatedAt: at.Add(time.Minute), Traffic: traffic.Info{DstOut: 5}}

	require.NoError(t, Write(path, second))
	got, err = Load(path)

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
	err := Write(filepath.Join(t.TempDir(), "missing", "app.json"), State{})

	require.Error(t, err)
}

func TestWriteJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.0123abcd.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"old": true}`), 0o600))

	err := WriteJSON(path, map[string]any{"db": "app", "pending": []int{1, 2}})

	require.NoError(t, err)
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.JSONEq(t, `{"db": "app", "pending": [1, 2]}`, string(b), "содержимое заменено целиком")
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1, "временных файлов нет")
	assert.Equal(t, "app.0123abcd.json", entries[0].Name())
}

func TestWriteJSONUnmarshalable(t *testing.T) {
	dir := t.TempDir()

	err := WriteJSON(filepath.Join(dir, "app.json"), map[string]any{"ch": make(chan int)})

	require.Error(t, err)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Empty(t, entries, "ни файла, ни временного файла")
}

func TestReadStateCorrupt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"traffic": `), 0o600))

	_, err := Load(path)

	require.ErrorIs(t, err, ErrCorrupt)
}

func TestMoveAside(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.0123abcd.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"db":"app"}`), 0o600))

	to, err := MoveAside(path, "corrupt")

	require.NoError(t, err)
	assert.Regexp(t, regexp.MustCompile(`/app\.0123abcd\.json\.corrupt-\d{8}T\d{6}Z$`), to)
	assert.NoFileExists(t, path)
	b, err := os.ReadFile(to)
	require.NoError(t, err)
	assert.JSONEq(t, `{"db":"app"}`, string(b), "содержимое сохранено")

	_, err = MoveAside(path, "mismatch")

	require.Error(t, err, "файла уже нет")
}

// Имя переноса занято (второй перенос за секунду): прежний перенос не заменяется.
func TestMoveAsideTaken(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.0123abcd.json")
	now := time.Now().UTC()
	for i := range 3 { // имя переноса — с точностью до секунды
		at := now.Add(time.Duration(i) * time.Second).Format("20060102T150405Z")
		require.NoError(t, os.WriteFile(path+".corrupt-"+at, []byte(`old`), 0o600))
	}
	require.NoError(t, os.WriteFile(path, []byte(`new`), 0o600))

	_, err := MoveAside(path, "corrupt")

	require.ErrorIs(t, err, fs.ErrExist)
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, `new`, string(b), "файл не перенесён")
	aside, err := filepath.Glob(path + ".corrupt-*")
	require.NoError(t, err)
	for _, a := range aside {
		b, err := os.ReadFile(a)
		require.NoError(t, err)
		assert.Equal(t, `old`, string(b), "прежний перенос не заменён: %s", a)
	}
}

func TestMachineTraffic(t *testing.T) {
	now := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)

	t.Run("пустой каталог", func(t *testing.T) {
		mt, err := Summary(t.TempDir(), now, DefaultActiveFor)

		require.NoError(t, err)
		assert.Equal(t, Machine{At: now}, mt)
	})

	t.Run("сумма и активные", func(t *testing.T) {
		dir := t.TempDir()
		write := func(name string, s State) {
			require.NoError(t, Write(filepath.Join(dir, name), s))
		}
		write("a.00000001.json", State{UpdatedAt: now.Add(-5 * time.Second),
			Traffic: traffic.Info{SrcIn: 1, SrcOut: 2, DstIn: 3, DstOut: 4}})
		write("b.00000002.json", State{UpdatedAt: now.Add(-29 * time.Second),
			Traffic: traffic.Info{SrcIn: 10, SrcOut: 20, DstIn: 30, DstOut: 40}})
		write("c.00000003.json", State{UpdatedAt: now.Add(-time.Hour),
			Traffic: traffic.Info{SrcIn: 100, SrcOut: 200, DstIn: 300, DstOut: 400}})
		// не входят в сводку: битый, перенесённый в сторону, не *.json
		require.NoError(t, os.WriteFile(filepath.Join(dir, "d.00000004.json"), []byte("{"), 0o600))
		write("e.00000005.json.corrupt-20261015T110000Z", State{Traffic: traffic.Info{SrcIn: 1000}})
		write("notes.txt", State{Traffic: traffic.Info{SrcIn: 1000}})
		require.NoError(t, os.Mkdir(filepath.Join(dir, "f.json"), 0o750))

		mt, err := Summary(dir, now, DefaultActiveFor)

		require.NoError(t, err)
		assert.Equal(t, Machine{At: now, Processes: 3, Active: 2,
			Traffic: traffic.Info{SrcIn: 111, SrcOut: 222, DstIn: 333, DstOut: 444}}, mt)
	})

	t.Run("окно активности задаёт вызывающий", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, Write(filepath.Join(dir, "a.00000001.json"), State{UpdatedAt: now.Add(-45 * time.Second)}))
		require.NoError(t, Write(filepath.Join(dir, "b.00000002.json"), State{UpdatedAt: now.Add(-time.Minute)}))

		mt, err := Summary(dir, now, time.Minute)

		require.NoError(t, err)
		assert.Equal(t, Machine{At: now, Processes: 2, Active: 1}, mt, "обновлённый ровно activeFor назад — не активен")
	})

	t.Run("символы шаблона в пути каталога", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "repl[1]")
		require.NoError(t, os.Mkdir(dir, 0o750))
		require.NoError(t, Write(filepath.Join(dir, "a.00000001.json"),
			State{UpdatedAt: now, Traffic: traffic.Info{SrcIn: 7}}))

		mt, err := Summary(dir, now, DefaultActiveFor)

		require.NoError(t, err)
		assert.Equal(t, Machine{At: now, Processes: 1, Active: 1, Traffic: traffic.Info{SrcIn: 7}}, mt)
	})

	t.Run("нет каталога", func(t *testing.T) {
		_, err := Summary(filepath.Join(t.TempDir(), "missing"), now, DefaultActiveFor)

		require.ErrorIs(t, err, fs.ErrNotExist)
	})
}

func TestStatePath(t *testing.T) {
	dir := t.TempDir()

	path, cluster := Path(dir, "app", "mongodb://u:p@H2,h1:27017/app?replicaSet=rs0")

	assert.Equal(t, "h1:27017,h2:27017", cluster)
	assert.Equal(t, filepath.Join(dir, first(Name("app", "mongodb://h1,h2"))), path)
}
