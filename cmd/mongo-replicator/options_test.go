package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// clearEnv убирает env опций: в контейнере app заданы SRC_URI/DST_URI.
func clearEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{"SRC_URI", "DST_URI", "START_TS", "BATCH_SIZE", "DB", "DST_DB", "APPLY_DROP",
		"LOG_LEVEL", "LOG_FILE", "LOG_FILE_LEVEL", "PROGRESS", "STATE_DIR"} {
		t.Setenv(k, "") // восстановит исходное значение после теста
		require.NoError(t, os.Unsetenv(k))
	}
}

func TestParseOptsEnv(t *testing.T) {
	clearEnv(t)
	t.Setenv("SRC_URI", "mongodb://src")
	t.Setenv("DST_URI", "mongodb://dst")
	t.Setenv("START_TS", "1791900000")
	t.Setenv("BATCH_SIZE", "50")
	t.Setenv("DB", "urlshrinker")
	t.Setenv("LOG_LEVEL", "debug")
	t.Setenv("LOG_FILE", "/var/log/repl.log")
	t.Setenv("LOG_FILE_LEVEL", "warn")
	t.Setenv("PROGRESS", "plain")
	t.Setenv("STATE_DIR", "/var/lib/repl")

	opts, err := parseOpts(nil)
	require.NoError(t, err)
	assert.Equal(t, options{Src: "mongodb://src", Dst: "mongodb://dst", StartTS: 1791900000, BatchSize: 50, DB: "urlshrinker", DstDB: "urlshrinker",
		LogLevel: "debug", LogFile: "/var/log/repl.log", LogFileLevel: "warn", Progress: "plain", StateDir: "/var/lib/repl"}, opts)
}

func TestStateDir(t *testing.T) {
	home, xdg := t.TempDir(), t.TempDir()
	for _, tc := range []struct {
		name, opt, xdg, want string
	}{
		{"задан", filepath.Join(xdg, "a", "b"), "", filepath.Join(xdg, "a", "b")},
		{"XDG_STATE_HOME абсолютный", "", xdg, filepath.Join(xdg, "mongo-replicator")},
		{"XDG_STATE_HOME относительный — игнорируется", "", "rel/state", filepath.Join(home, ".local", "state", "mongo-replicator")},
		{"XDG_STATE_HOME не задан", "", "", filepath.Join(home, ".local", "state", "mongo-replicator")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("HOME", home)
			t.Setenv("XDG_STATE_HOME", tc.xdg)
			if tc.xdg == "" {
				require.NoError(t, os.Unsetenv("XDG_STATE_HOME"))
			}
			got, err := stateDir(tc.opt)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
			assert.DirExists(t, got, "каталог создан")
		})
	}
	_, err := os.Stat("rel")
	assert.ErrorIs(t, err, os.ErrNotExist, "относительный XDG_STATE_HOME не создан")
}

// Путь занят файлом: каталог не создать — main выходит с кодом 2 до подключения.
func TestStateDirIsFile(t *testing.T) {
	file := filepath.Join(t.TempDir(), "state")
	require.NoError(t, os.WriteFile(file, nil, 0o600))
	_, err := stateDir(file)
	require.ErrorContains(t, err, "--state-dir")

	_, err = stateDir(filepath.Join(file, "sub"))
	require.ErrorContains(t, err, "--state-dir")
}

func TestParseOptsDefaults(t *testing.T) {
	clearEnv(t)
	opts, err := parseOpts([]string{"--src", "mongodb://src", "--dst", "mongodb://dst", "--db", "urlshrinker"})
	require.NoError(t, err)
	assert.Equal(t, options{Src: "mongodb://src", Dst: "mongodb://dst", BatchSize: 1000, DB: "urlshrinker", DstDB: "urlshrinker",
		LogLevel: "info", LogFileLevel: "info", Progress: "auto"}, opts)
}

func TestParseOptsFlagsOverrideEnv(t *testing.T) {
	clearEnv(t)
	t.Setenv("SRC_URI", "mongodb://src-env")
	t.Setenv("DST_URI", "mongodb://dst-env")
	t.Setenv("START_TS", "1")
	t.Setenv("BATCH_SIZE", "50")
	t.Setenv("DB", "env")
	t.Setenv("LOG_LEVEL", "warn")
	t.Setenv("LOG_FILE", "env.log")
	t.Setenv("LOG_FILE_LEVEL", "error")
	t.Setenv("PROGRESS", "plain")

	opts, err := parseOpts([]string{"--src", "mongodb://src-flag", "--start-ts=2", "--batch", "7", "--db", "flag",
		"--log-level", "debug", "--log-file", "flag.log", "--log-file-level", "info", "--progress", "tty"})
	require.NoError(t, err)
	assert.Equal(t, options{Src: "mongodb://src-flag", Dst: "mongodb://dst-env", StartTS: 2, BatchSize: 7, DB: "flag", DstDB: "flag",
		LogLevel: "debug", LogFile: "flag.log", LogFileLevel: "info", Progress: "tty"}, opts)
}

func TestParseOptsProgress(t *testing.T) {
	base := []string{"--src", "s", "--dst", "d", "--db", "db"}
	for _, v := range []string{"auto", "tty", "plain"} {
		t.Run(v, func(t *testing.T) {
			clearEnv(t)
			opts, err := parseOpts(append(base, "--progress", v))
			require.NoError(t, err)
			assert.Equal(t, v, opts.Progress)

			t.Setenv("PROGRESS", v)
			opts, err = parseOpts(base)
			require.NoError(t, err)
			assert.Equal(t, v, opts.Progress)
		})
	}
	for name, src := range map[string]struct {
		env  map[string]string
		args []string
	}{
		"неверный флаг":   {args: []string{"--progress", "fancy"}},
		"неверный env":    {env: map[string]string{"PROGRESS": "TTY"}},
		"пустой флаг":     {args: []string{"--progress="}},
		"пустой PROGRESS": {env: map[string]string{"PROGRESS": ""}},
	} {
		t.Run(name, func(t *testing.T) {
			clearEnv(t)
			for k, v := range src.env {
				t.Setenv(k, v)
			}
			_, err := parseOpts(append(base, src.args...))
			require.Error(t, err)
			assert.False(t, isHelp(err))
			assert.Contains(t, err.Error(), "progress")
		})
	}
}

func TestProgressMode(t *testing.T) {
	cases := []struct {
		opt  string
		tty  bool
		term string
		want string
	}{
		{"auto", true, "xterm-256color", progressTTY},
		{"auto", true, "dumb", progressPlain},
		{"auto", false, "xterm-256color", progressPlain},
		{"auto", true, "", progressTTY},
		{"tty", false, "dumb", progressTTY},
		{"plain", true, "xterm", progressPlain},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, progressMode(c.opt, c.tty, c.term), "%+v", c)
	}
}

func TestParseOptsLog(t *testing.T) {
	base := []string{"--src", "s", "--dst", "d", "--db", "db"}
	cases := []struct {
		name                         string
		env                          map[string]string
		args                         []string
		level, logFile, logFileLevel string
	}{
		{"не заданы", nil, nil, "info", "", "info"},
		{"пустой LOG_FILE — файла нет", map[string]string{"LOG_FILE": ""}, nil, "info", "", "info"},
		{"файл без уровня — info", nil, []string{"--log-file", "r.log"}, "info", "r.log", "info"},
		{"LOG_FILE и LOG_FILE_LEVEL", map[string]string{"LOG_FILE": "r.log", "LOG_FILE_LEVEL": "debug"}, nil, "info", "r.log", "debug"},
		{"LOG_FILE_LEVEL с --log-file", map[string]string{"LOG_FILE_LEVEL": "warn"}, []string{"--log-file", "r.log"}, "info", "r.log", "warn"},
		{"флаг перекрывает LOG_LEVEL", map[string]string{"LOG_LEVEL": "error"}, []string{"--log-level", "warn"}, "warn", "", "info"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			clearEnv(t)
			for k, v := range c.env {
				t.Setenv(k, v)
			}
			opts, err := parseOpts(append(base, c.args...))
			require.NoError(t, err)
			assert.Equal(t, c.level, opts.LogLevel)
			assert.Equal(t, c.logFile, opts.LogFile)
			assert.Equal(t, c.logFileLevel, opts.LogFileLevel)
		})
	}
	for _, lvl := range []string{"debug", "info", "warn", "error"} {
		t.Run("уровень "+lvl, func(t *testing.T) {
			clearEnv(t)
			opts, err := parseOpts(append(base, "--log-level", lvl, "--log-file", "r.log", "--log-file-level", lvl))
			require.NoError(t, err)
			assert.Equal(t, lvl, opts.LogLevel)
			assert.Equal(t, lvl, opts.LogFileLevel)
		})
	}
}

func TestParseOptsLogErrors(t *testing.T) {
	base := []string{"--src", "s", "--dst", "d", "--db", "db"}
	cases := []struct {
		name string
		env  map[string]string
		args []string
		want string // подстрока ошибки
	}{
		{"--dbg", nil, []string{"--dbg"}, "unknown flag `dbg'"},
		{"пустой LOG_LEVEL", map[string]string{"LOG_LEVEL": ""}, nil, "log-level"},
		{"пустой LOG_FILE_LEVEL", map[string]string{"LOG_FILE": "r.log", "LOG_FILE_LEVEL": ""}, nil, "log-file-level"},
		{"--log-file-level без --log-file", nil, []string{"--log-file-level", "debug"}, "--log-file-level задан без --log-file"},
		{"LOG_FILE_LEVEL без LOG_FILE", map[string]string{"LOG_FILE_LEVEL": "debug"}, nil, "--log-file-level задан без --log-file"},
		{"LOG_FILE_LEVEL при пустом LOG_FILE", map[string]string{"LOG_FILE": "", "LOG_FILE_LEVEL": "info"}, nil, "--log-file-level задан без --log-file"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			clearEnv(t)
			for k, v := range c.env {
				t.Setenv(k, v)
			}
			_, err := parseOpts(append(base, c.args...))
			require.Error(t, err)
			assert.False(t, isHelp(err))
			assert.Contains(t, err.Error(), c.want)
		})
	}
	// ParseLevel принял бы "1" и "INFO" — choice их отвергает
	for _, v := range []string{"1", "INFO", "trace", "verbose"} {
		t.Run("уровень "+v, func(t *testing.T) {
			for name, src := range map[string]struct {
				env  map[string]string
				args []string
			}{
				"--log-level":      {args: []string{"--log-level", v}},
				"LOG_LEVEL":        {env: map[string]string{"LOG_LEVEL": v}},
				"--log-file-level": {args: []string{"--log-file", "r.log", "--log-file-level", v}},
				"LOG_FILE_LEVEL":   {env: map[string]string{"LOG_FILE": "r.log", "LOG_FILE_LEVEL": v}},
			} {
				clearEnv(t)
				for k, val := range src.env {
					t.Setenv(k, val)
				}
				_, err := parseOpts(append(base, src.args...))
				require.Error(t, err, name)
				assert.Contains(t, err.Error(), "Invalid value `"+v+"'", name)
			}
		})
	}
}

func TestParseOptsDB(t *testing.T) {
	base := []string{"--src", "s", "--dst", "d"}
	cases := []struct {
		name string
		env  map[string]string
		args []string
		db   string
	}{
		{"--db", nil, []string{"--db", "urlshrinker"}, "urlshrinker"},
		{"DB из env", map[string]string{"DB": "urlshrinker"}, nil, "urlshrinker"},
		{"флаг перекрывает env", map[string]string{"DB": "env"}, []string{"--db", "flag"}, "flag"},
		{"имя ровно 63 байта", nil, []string{"--db", strings.Repeat("a", 63)}, strings.Repeat("a", 63)},
		{"_replicator — обычное имя", nil, []string{"--db", "_replicator"}, "_replicator"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			clearEnv(t)
			for k, v := range c.env {
				t.Setenv(k, v)
			}
			opts, err := parseOpts(append(base, c.args...))
			require.NoError(t, err)
			assert.Equal(t, c.db, opts.DB)
		})
	}
}

func TestParseOptsDstDB(t *testing.T) {
	base := []string{"--src", "s", "--dst", "d", "--db", "urlshrinker"}
	cases := []struct {
		name  string
		env   map[string]string
		args  []string
		dstDB string
	}{
		{"не задана — как --db", nil, nil, "urlshrinker"},
		{"пустой DST_DB — как --db", map[string]string{"DST_DB": ""}, nil, "urlshrinker"},
		{"--dst-db", nil, []string{"--dst-db", "urlshrinker_v2"}, "urlshrinker_v2"},
		{"DST_DB из env", map[string]string{"DST_DB": "urlshrinker_v2"}, nil, "urlshrinker_v2"},
		{"флаг перекрывает env", map[string]string{"DST_DB": "env"}, []string{"--dst-db", "flag"}, "flag"},
		{"_replicator — обычное имя", nil, []string{"--dst-db", "_replicator"}, "_replicator"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			clearEnv(t)
			for k, v := range c.env {
				t.Setenv(k, v)
			}
			opts, err := parseOpts(append(base, c.args...))
			require.NoError(t, err)
			assert.Equal(t, "urlshrinker", opts.DB)
			assert.Equal(t, c.dstDB, opts.DstDB)
		})
	}
}

func TestParseOptsApplyDrop(t *testing.T) {
	base := []string{"--src", "s", "--dst", "d", "--db", "db"}
	cases := []struct {
		name  string
		env   map[string]string
		args  []string
		drops []string
	}{
		{"не задана", nil, nil, nil},
		{"флаги", nil, []string{"--apply-drop", "db.a", "--apply-drop", "db.b"}, []string{"db.a", "db.b"}},
		{"APPLY_DROP", map[string]string{"APPLY_DROP": "db.a,db.b"}, nil, []string{"db.a", "db.b"}},
		{"пустой APPLY_DROP", map[string]string{"APPLY_DROP": ""}, nil, nil},
		{"пробелы и пустые элементы", map[string]string{"APPLY_DROP": "db.a, db.b,, "}, nil, []string{"db.a", "db.b"}},
		{"флаг заменяет env", map[string]string{"APPLY_DROP": "db.a,db.b"}, []string{"--apply-drop", "db.c"}, []string{"db.c"}},
		{"точка в имени коллекции", nil, []string{"--apply-drop", "db.a.b"}, []string{"db.a.b"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			clearEnv(t)
			for k, v := range c.env {
				t.Setenv(k, v)
			}
			opts, err := parseOpts(append(base, c.args...))
			require.NoError(t, err)
			assert.Equal(t, c.drops, opts.ApplyDrops)
		})
	}
}

func TestParseOptsApplyDropErrors(t *testing.T) {
	cases := []struct{ name, ns, want string }{
		{"другая база", "other.a", "база не совпадает с --db"},
		{"без коллекции", "db.", "ожидается <база>.<коллекция>"},
		{"без точки", "db", "ожидается <база>.<коллекция>"},
		{"база цели", "db_v2.a", "база не совпадает с --db"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			clearEnv(t)
			_, err := parseOpts([]string{"--src", "s", "--dst", "d", "--db", "db", "--dst-db", "db_v2", "--apply-drop", c.ns})
			require.Error(t, err)
			assert.Contains(t, err.Error(), "--apply-drop")
			assert.Contains(t, err.Error(), c.want)
		})
	}
}

func TestParseOptsErrors(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
		args []string
		want string // подстрока ошибки: случай падает по своей причине
	}{
		{"нет --src", nil, []string{"--dst", "mongodb://dst", "--db", "x"}, "--src"},
		{"нет --dst", nil, []string{"--src", "mongodb://src", "--db", "x"}, "--dst"},
		{"нет обоих", nil, []string{"--db", "x"}, "--src"},
		{"пустой SRC_URI", map[string]string{"SRC_URI": ""}, []string{"--dst", "mongodb://dst", "--db", "x"}, "--src"},
		{"--batch 0", nil, []string{"--src", "s", "--dst", "d", "--db", "x", "--batch", "0"}, "--batch должен быть"},
		{"BATCH_SIZE<0", map[string]string{"BATCH_SIZE": "-5"}, []string{"--src", "s", "--dst", "d", "--db", "x"}, "--batch должен быть"},
		{"--batch больше int32", nil, []string{"--src", "s", "--dst", "d", "--db", "x", "--batch", "3000000000"}, "--batch должен быть"},
		{"отрицательный --start-ts", nil, []string{"--src", "s", "--dst", "d", "--db", "x", "--start-ts=-1"}, "--start-ts"},
		{"нечисловой --start-ts", nil, []string{"--src", "s", "--dst", "d", "--db", "x", "--start-ts", "abc"}, "--start-ts"},
		{"одиночный дефис", nil, []string{"-src", "s", "-dst", "d", "--db", "x"}, "unknown flag"},
		{"неизвестный флаг", nil, []string{"--src", "s", "--dst", "d", "--db", "x", "--foo"}, "unknown flag `foo'"},
		{"нет --db", nil, []string{"--src", "s", "--dst", "d"}, "--db"},
		{"пустой DB", map[string]string{"DB": ""}, []string{"--src", "s", "--dst", "d"}, "--db не может быть пустой"},
		{"пустой --db", nil, []string{"--src", "s", "--dst", "d", "--db", ""}, "--db не может быть пустой"},
		{"--all-dbs", nil, []string{"--src", "s", "--dst", "d", "--db", "x", "--all-dbs"}, "unknown flag `all-dbs'"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			clearEnv(t)
			for k, v := range c.env {
				t.Setenv(k, v)
			}
			_, err := parseOpts(c.args)
			require.Error(t, err)
			assert.False(t, isHelp(err))
			assert.Contains(t, err.Error(), c.want)
		})
	}
}

func TestParseOptsDBName(t *testing.T) {
	cases := []struct{ name, db, want string }{
		{"точка", "a.b", "не может содержать"},
		{"слеш", "a/b", "не может содержать"},
		{"пробел", "a b", "не может содержать"},
		{"доллар", "a$b", "не может содержать"},
		{"кавычка", `a"b`, "не может содержать"},
		{"обратный слеш", `a\b`, "не может содержать"},
		{"NUL", "a\x00b", "не может содержать"},
		{"64 байта", strings.Repeat("a", 64), "длиннее 63 байт"},
		{"65 байт", strings.Repeat("a", 65), "длиннее 63 байт"},
		{"admin", "admin", "служебная база"},
		{"config", "config", "служебная база"},
		{"local", "local", "служебная база"},
		{"Admin", "Admin", "служебная база"},
		{"LOCAL", "LOCAL", "служебная база"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			clearEnv(t)
			_, err := parseOpts([]string{"--src", "s", "--dst", "d", "--db", c.db})
			require.Error(t, err)
			assert.Contains(t, err.Error(), c.want)
			assert.Contains(t, err.Error(), "--db ")

			_, err = parseOpts([]string{"--src", "s", "--dst", "d", "--db", "x", "--dst-db", c.db})
			require.Error(t, err)
			assert.Contains(t, err.Error(), c.want)
			assert.Contains(t, err.Error(), "--dst-db ")
		})
	}
}

func TestParseOptsHelp(t *testing.T) {
	clearEnv(t)
	_, err := parseOpts([]string{"--help"})
	require.Error(t, err)
	assert.True(t, isHelp(err))
	assert.Contains(t, err.Error(), "SRC_URI")
	assert.Contains(t, err.Error(), "DST_URI")
	assert.Contains(t, err.Error(), "запрошен") // описание --src/--dst: пароль спросят в терминале
	assert.Contains(t, err.Error(), "--start-ts")
	assert.Contains(t, err.Error(), "--db")
	assert.Contains(t, err.Error(), "--dst-db")
	assert.Contains(t, err.Error(), "DST_DB")
	assert.Contains(t, err.Error(), "--apply-drop")
	assert.Contains(t, err.Error(), "APPLY_DROP")
	assert.NotContains(t, err.Error(), "--all-dbs")
	assert.Contains(t, err.Error(), "--log-level")
	assert.Contains(t, err.Error(), "LOG_FILE_LEVEL")
	assert.NotContains(t, err.Error(), "--dbg")
}
