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
		"LOG_LEVEL", "LOG_FILE", "LOG_FILE_LEVEL", "PROGRESS", "NO_CLEAR", "STATE_DIR",
		"VERIFY_SINCE", "VERIFY_RATE", "VERIFY_SAMPLE", "VERIFY_UNTIL_CAUGHT_UP"} {
		t.Setenv(k, "") // восстановит исходное значение после теста
		require.NoError(t, os.Unsetenv(k))
	}
}

// setEnv задаёт env случая таблицы; значения восстановятся после теста.
func setEnv(t *testing.T, env map[string]string) {
	t.Helper()
	for k, v := range env {
		t.Setenv(k, v)
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
	assert.Equal(t, runOptions{
		commonOptions: commonOptions{Src: "mongodb://src", Dst: "mongodb://dst", DB: "urlshrinker", DstDB: "urlshrinker",
			LogLevel: "debug", LogFile: "/var/log/repl.log", LogFileLevel: "warn", StateDir: "/var/lib/repl"},
		StartTS: 1791900000, BatchSize: 50, Progress: "plain",
	}, opts)
}

func TestStateDir(t *testing.T) {
	t.Chdir(t.TempDir()) // относительный XDG_STATE_HOME проверяется в чистом рабочем каталоге
	home, xdg := t.TempDir(), t.TempDir()
	for _, tc := range []struct {
		name, opt, xdg, want string
	}{
		{"задан", filepath.Join(xdg, "a", "b"), "", filepath.Join(xdg, "a", "b")},
		{"XDG_STATE_HOME абсолютный", "", xdg, filepath.Join(xdg, "mongo-replicator")},
		{"XDG_STATE_HOME относительный — игнорируется", "", "rel/state", filepath.Join(home, ".local", "state", "mongo-replicator")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("HOME", home)
			t.Setenv("XDG_STATE_HOME", tc.xdg)

			got, err := stateDir(tc.opt)

			require.NoError(t, err)
			assert.Equal(t, tc.want, got, "каталог состояния")
			assert.DirExists(t, got, "каталог создан")
		})
	}
	_, err := os.Stat("rel")
	assert.ErrorIs(t, err, os.ErrNotExist, "относительный XDG_STATE_HOME не создан")
}

func TestStateDirNoXDG(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_STATE_HOME", "")
	require.NoError(t, os.Unsetenv("XDG_STATE_HOME"))

	got, err := stateDir("")

	require.NoError(t, err)
	assert.Equal(t, filepath.Join(home, ".local", "state", "mongo-replicator"), got, "каталог по умолчанию — в HOME")
	assert.DirExists(t, got, "каталог создан")
}

// Путь занят файлом: каталог не создать — main выходит с кодом 2 до подключения.
func TestStateDirIsFile(t *testing.T) {
	for _, c := range []struct{ name, sub string }{
		{"путь — файл", ""},
		{"родитель — файл", "sub"},
	} {
		t.Run(c.name, func(t *testing.T) {
			file := filepath.Join(t.TempDir(), "state")
			require.NoError(t, os.WriteFile(file, nil, 0o600))

			_, err := stateDir(filepath.Join(file, c.sub))

			require.ErrorContains(t, err, "--state-dir")
		})
	}
}

func TestParseOptsDefaults(t *testing.T) {
	clearEnv(t)

	opts, err := parseOpts([]string{"--src", "mongodb://src", "--dst", "mongodb://dst", "--db", "urlshrinker"})

	require.NoError(t, err)
	assert.Equal(t, runOptions{
		commonOptions: commonOptions{Src: "mongodb://src", Dst: "mongodb://dst", DB: "urlshrinker", DstDB: "urlshrinker",
			LogLevel: "info", LogFileLevel: "info"},
		BatchSize: 1000, Progress: "auto",
	}, opts)
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
	assert.Equal(t, runOptions{
		commonOptions: commonOptions{Src: "mongodb://src-flag", Dst: "mongodb://dst-env", DB: "flag", DstDB: "flag",
			LogLevel: "debug", LogFile: "flag.log", LogFileLevel: "info"},
		StartTS: 2, BatchSize: 7, Progress: "tty",
	}, opts)
}

func TestParseOptsProgress(t *testing.T) {
	base := []string{"--src", "s", "--dst", "d", "--db", "db"}
	for _, c := range []struct {
		name string
		env  map[string]string
		args []string
		want string
	}{
		{"флаг auto", nil, []string{"--progress", "auto"}, "auto"},
		{"флаг tty", nil, []string{"--progress", "tty"}, "tty"},
		{"флаг plain", nil, []string{"--progress", "plain"}, "plain"},
		{"PROGRESS auto", map[string]string{"PROGRESS": "auto"}, nil, "auto"},
		{"PROGRESS tty", map[string]string{"PROGRESS": "tty"}, nil, "tty"},
		{"PROGRESS plain", map[string]string{"PROGRESS": "plain"}, nil, "plain"},
	} {
		t.Run(c.name, func(t *testing.T) {
			clearEnv(t)
			setEnv(t, c.env)

			opts, err := parseOpts(append(base, c.args...))

			require.NoError(t, err)
			assert.Equal(t, c.want, opts.Progress, "режим прогресса")
		})
	}
}

func TestParseOptsProgressErrors(t *testing.T) {
	base := []string{"--src", "s", "--dst", "d", "--db", "db"}
	for _, c := range []struct {
		name string
		env  map[string]string
		args []string
	}{
		{"неверный флаг", nil, []string{"--progress", "fancy"}},
		{"неверный env", map[string]string{"PROGRESS": "TTY"}, nil},
		{"пустой флаг", nil, []string{"--progress="}},
		{"пустой PROGRESS", map[string]string{"PROGRESS": ""}, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			clearEnv(t)
			setEnv(t, c.env)

			_, err := parseOpts(append(base, c.args...))

			require.Error(t, err)
			assert.False(t, isHelp(err), "ошибка — не --help")
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

func TestParseOptsNoClear(t *testing.T) {
	base := []string{"--src", "s", "--dst", "d", "--db", "db"}
	for _, c := range []struct {
		name string
		env  map[string]string
		args []string
		want bool
	}{
		{"не задан", nil, nil, false},
		{"флаг", nil, []string{"--no-clear"}, true},
		{"NO_CLEAR=true", map[string]string{"NO_CLEAR": "true"}, nil, true},
		{"NO_CLEAR=false", map[string]string{"NO_CLEAR": "false"}, nil, false},
		{"пустой NO_CLEAR — включён", map[string]string{"NO_CLEAR": ""}, nil, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			clearEnv(t)
			setEnv(t, c.env)

			opts, err := parseOpts(append(base, c.args...))

			require.NoError(t, err)
			assert.Equal(t, c.want, opts.NoClear, "не очищать экран")
		})
	}
}

func TestParseOptsNoClearInvalid(t *testing.T) {
	clearEnv(t)
	t.Setenv("NO_CLEAR", "yes")

	_, err := parseOpts([]string{"--src", "s", "--dst", "d", "--db", "db"})

	require.ErrorContains(t, err, "--no-clear", "NO_CLEAR=yes")
	assert.False(t, isHelp(err), "ошибка — не --help")
}

func TestClearBeforePanel(t *testing.T) {
	cases := []struct {
		mode    string
		noClear bool
		want    bool
	}{
		{progressTTY, false, true},
		{progressTTY, true, false},
		{progressPlain, false, false},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, clearBeforePanel(c.mode, c.noClear), "%+v", c)
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
		{"уровень debug", nil, []string{"--log-level", "debug", "--log-file", "r.log", "--log-file-level", "debug"}, "debug", "r.log", "debug"},
		{"уровень info", nil, []string{"--log-level", "info", "--log-file", "r.log", "--log-file-level", "info"}, "info", "r.log", "info"},
		{"уровень warn", nil, []string{"--log-level", "warn", "--log-file", "r.log", "--log-file-level", "warn"}, "warn", "r.log", "warn"},
		{"уровень error", nil, []string{"--log-level", "error", "--log-file", "r.log", "--log-file-level", "error"}, "error", "r.log", "error"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			clearEnv(t)
			setEnv(t, c.env)

			opts, err := parseOpts(append(base, c.args...))

			require.NoError(t, err)
			assert.Equal(t, c.level, opts.LogLevel, "уровень в консоли")
			assert.Equal(t, c.logFile, opts.LogFile, "файл лога")
			assert.Equal(t, c.logFileLevel, opts.LogFileLevel, "уровень в файле")
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
		// ParseLevel принял бы "1" и "INFO" — choice их отвергает
		{"--log-level 1", nil, []string{"--log-level", "1"}, "Invalid value `1'"},
		{"--log-level INFO", nil, []string{"--log-level", "INFO"}, "Invalid value `INFO'"},
		{"--log-level trace", nil, []string{"--log-level", "trace"}, "Invalid value `trace'"},
		{"--log-level verbose", nil, []string{"--log-level", "verbose"}, "Invalid value `verbose'"},
		{"LOG_LEVEL 1", map[string]string{"LOG_LEVEL": "1"}, nil, "Invalid value `1'"},
		{"LOG_LEVEL INFO", map[string]string{"LOG_LEVEL": "INFO"}, nil, "Invalid value `INFO'"},
		{"LOG_LEVEL trace", map[string]string{"LOG_LEVEL": "trace"}, nil, "Invalid value `trace'"},
		{"LOG_LEVEL verbose", map[string]string{"LOG_LEVEL": "verbose"}, nil, "Invalid value `verbose'"},
		{"--log-file-level 1", nil, []string{"--log-file", "r.log", "--log-file-level", "1"}, "Invalid value `1'"},
		{"--log-file-level INFO", nil, []string{"--log-file", "r.log", "--log-file-level", "INFO"}, "Invalid value `INFO'"},
		{"--log-file-level trace", nil, []string{"--log-file", "r.log", "--log-file-level", "trace"}, "Invalid value `trace'"},
		{"--log-file-level verbose", nil, []string{"--log-file", "r.log", "--log-file-level", "verbose"}, "Invalid value `verbose'"},
		{"LOG_FILE_LEVEL 1", map[string]string{"LOG_FILE": "r.log", "LOG_FILE_LEVEL": "1"}, nil, "Invalid value `1'"},
		{"LOG_FILE_LEVEL INFO", map[string]string{"LOG_FILE": "r.log", "LOG_FILE_LEVEL": "INFO"}, nil, "Invalid value `INFO'"},
		{"LOG_FILE_LEVEL trace", map[string]string{"LOG_FILE": "r.log", "LOG_FILE_LEVEL": "trace"}, nil, "Invalid value `trace'"},
		{"LOG_FILE_LEVEL verbose", map[string]string{"LOG_FILE": "r.log", "LOG_FILE_LEVEL": "verbose"}, nil, "Invalid value `verbose'"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			clearEnv(t)
			setEnv(t, c.env)

			_, err := parseOpts(append(base, c.args...))

			require.Error(t, err)
			assert.False(t, isHelp(err), "ошибка — не --help")
			assert.Contains(t, err.Error(), c.want)
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
			setEnv(t, c.env)

			opts, err := parseOpts(append(base, c.args...))

			require.NoError(t, err)
			assert.Equal(t, c.db, opts.DB, "база источника")
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
			setEnv(t, c.env)

			opts, err := parseOpts(append(base, c.args...))

			require.NoError(t, err)
			assert.Equal(t, "urlshrinker", opts.DB, "база источника")
			assert.Equal(t, c.dstDB, opts.DstDB, "база цели")
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
			setEnv(t, c.env)

			opts, err := parseOpts(append(base, c.args...))

			require.NoError(t, err)
			assert.Equal(t, c.drops, opts.ApplyDrops, "коллекции --apply-drop")
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
			setEnv(t, c.env)

			_, err := parseOpts(c.args)

			require.Error(t, err)
			assert.False(t, isHelp(err), "ошибка — не --help")
			assert.Contains(t, err.Error(), c.want)
		})
	}
}

// badDBNames — недопустимые имена базы для --db и --dst-db и подстрока ошибки.
var badDBNames = []struct{ name, db, want string }{
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

func TestParseOptsDBName(t *testing.T) {
	for _, c := range badDBNames {
		t.Run(c.name, func(t *testing.T) {
			clearEnv(t)

			_, err := parseOpts([]string{"--src", "s", "--dst", "d", "--db", c.db})

			require.Error(t, err)
			assert.Contains(t, err.Error(), c.want)
			assert.Contains(t, err.Error(), "--db ")
		})
	}
}

func TestParseOptsDstDBName(t *testing.T) {
	for _, c := range badDBNames {
		t.Run(c.name, func(t *testing.T) {
			clearEnv(t)

			_, err := parseOpts([]string{"--src", "s", "--dst", "d", "--db", "x", "--dst-db", c.db})

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
	assert.True(t, isHelp(err), "ошибка — --help")
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
	assert.Contains(t, err.Error(), "--no-clear")
	assert.Contains(t, err.Error(), "NO_CLEAR")
	assert.NotContains(t, err.Error(), "--dbg")
	assert.Contains(t, err.Error(), "verify --help") // где искать справку сверки
	assert.NotContains(t, err.Error(), "--since")
	assert.NotContains(t, err.Error(), "--schema-only")
}

// Позиционный аргумент формы запуска — ошибка: verify распознаётся только первым аргументом.
func TestParseOptsPositional(t *testing.T) {
	base := []string{"--src", "s", "--dst", "d", "--db", "db"}
	for _, c := range []struct {
		name string
		args []string
	}{
		{"после значения опции", []string{"--log-level", "debug", "verify"}},
		{"после --", []string{"--", "verify"}},
		{"любой", []string{"extra"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			clearEnv(t)

			_, err := parseOpts(append(base, c.args...))

			require.Error(t, err)
			assert.False(t, isHelp(err), "ошибка — не --help")
			assert.Contains(t, err.Error(), "verify должна быть первым аргументом")
		})
	}
}

func TestParseVerifyOptsDefaults(t *testing.T) {
	clearEnv(t)

	opts, err := parseVerifyOpts([]string{"--src", "mongodb://src", "--dst", "mongodb://dst", "--db", "urlshrinker"})

	require.NoError(t, err)
	assert.Equal(t, verifyOptions{
		commonOptions: commonOptions{Src: "mongodb://src", Dst: "mongodb://dst", DB: "urlshrinker", DstDB: "urlshrinker",
			LogLevel: "info", LogFileLevel: "info"},
		Rate: 200, Sample: 1,
	}, opts)
}

func TestParseVerifyOptsEnv(t *testing.T) {
	clearEnv(t)
	t.Setenv("SRC_URI", "mongodb://src")
	t.Setenv("DST_URI", "mongodb://dst")
	t.Setenv("DB", "urlshrinker")
	t.Setenv("DST_DB", "urlshrinker_v2")
	t.Setenv("LOG_LEVEL", "debug")
	t.Setenv("LOG_FILE", "/var/log/verify.log")
	t.Setenv("LOG_FILE_LEVEL", "warn")
	t.Setenv("STATE_DIR", "/var/lib/repl")
	t.Setenv("VERIFY_SINCE", "1791900000")
	t.Setenv("VERIFY_RATE", "50")
	t.Setenv("VERIFY_SAMPLE", "0.1")
	t.Setenv("VERIFY_UNTIL_CAUGHT_UP", "true")

	opts, err := parseVerifyOpts(nil)

	require.NoError(t, err)
	assert.Equal(t, verifyOptions{
		commonOptions: commonOptions{Src: "mongodb://src", Dst: "mongodb://dst", DB: "urlshrinker", DstDB: "urlshrinker_v2",
			LogLevel: "debug", LogFile: "/var/log/verify.log", LogFileLevel: "warn", StateDir: "/var/lib/repl"},
		Since: 1791900000, Rate: 50, Sample: 0.1, UntilCaughtUp: true,
	}, opts)
}

func TestParseVerifyOptsFlagsOverrideEnv(t *testing.T) {
	clearEnv(t)
	t.Setenv("VERIFY_SINCE", "1")
	t.Setenv("VERIFY_RATE", "50")
	t.Setenv("VERIFY_SAMPLE", "0.1")

	opts, err := parseVerifyOpts([]string{"--src", "s", "--dst", "d", "--db", "db",
		"--since", "1791900000", "--rate", "1", "--sample", "1", "--until-caught-up"})

	require.NoError(t, err)
	assert.Equal(t, verifyOptions{
		commonOptions: commonOptions{Src: "s", Dst: "d", DB: "db", DstDB: "db", LogLevel: "info", LogFileLevel: "info"},
		Since:         1791900000, Rate: 1, Sample: 1, UntilCaughtUp: true,
	}, opts)
}

func TestParseVerifyOptsIDs(t *testing.T) {
	clearEnv(t)

	opts, err := parseVerifyOpts([]string{"--src", "s", "--dst", "d", "--db", "db", "--ids", "/tmp/db.defects.json"})

	require.NoError(t, err)
	assert.Equal(t, "/tmp/db.defects.json", opts.IDs, "файл ключей")
	assert.False(t, opts.SchemaOnly, "только схема")
}

func TestParseVerifyOptsSchemaOnly(t *testing.T) {
	clearEnv(t)

	opts, err := parseVerifyOpts([]string{"--src", "s", "--dst", "d", "--db", "db", "--schema-only"})

	require.NoError(t, err)
	assert.True(t, opts.SchemaOnly, "только схема")
	assert.Empty(t, opts.IDs, "файл ключей")
}

func TestParseVerifyOptsErrors(t *testing.T) {
	base := []string{"--src", "s", "--dst", "d", "--db", "db"}
	for _, c := range []struct {
		name string
		env  map[string]string
		args []string
		want string // подстрока ошибки: случай падает по своей причине
	}{
		{"--rate 0", nil, []string{"--rate", "0"}, "--rate должен быть не меньше 1"},
		{"VERIFY_RATE<0", map[string]string{"VERIFY_RATE": "-1"}, nil, "--rate должен быть не меньше 1"},
		{"--sample 0", nil, []string{"--sample", "0"}, "--sample должна быть в (0, 1]"},
		{"--sample больше 1", nil, []string{"--sample", "1.5"}, "--sample должна быть в (0, 1]"},
		{"--sample NaN", nil, []string{"--sample", "NaN"}, "--sample должна быть в (0, 1]"},
		{"VERIFY_SAMPLE отрицательная", map[string]string{"VERIFY_SAMPLE": "-0.5"}, nil, "--sample должна быть в (0, 1]"},
		{"--since 0", nil, []string{"--since", "0"}, "--since должна быть больше 0"},
		{"пустой --ids", nil, []string{"--ids", ""}, "--ids не может быть пустой"},
		{"--ids и --schema-only", nil, []string{"--ids", "f.json", "--schema-only"}, "--ids и --schema-only несовместимы"},
		{"--since с --ids", nil, []string{"--ids", "f.json", "--since", "1"}, "--since несовместима с --ids"},
		{"VERIFY_SINCE с --ids", map[string]string{"VERIFY_SINCE": "1"}, []string{"--ids", "f.json"}, "--since несовместима с --ids"},
		{"--until-caught-up с --ids", nil, []string{"--ids", "f.json", "--until-caught-up"}, "--until-caught-up несовместима с --ids"},
		{"--sample с --ids", nil, []string{"--ids", "f.json", "--sample", "1"}, "--sample несовместима с --ids"},
		{"--since с --schema-only", nil, []string{"--schema-only", "--since", "1"}, "--since несовместима с --schema-only"},
		{"--until-caught-up с --schema-only", nil, []string{"--schema-only", "--until-caught-up"}, "--until-caught-up несовместима с --schema-only"},
		{"VERIFY_SAMPLE с --schema-only", map[string]string{"VERIFY_SAMPLE": "0.5"}, []string{"--schema-only"}, "--sample несовместима с --schema-only"},
		{"--start-ts", nil, []string{"--start-ts", "1"}, "unknown flag `start-ts'"},
		{"--apply-drop", nil, []string{"--apply-drop", "db.a"}, "unknown flag `apply-drop'"},
		{"--progress", nil, []string{"--progress", "plain"}, "unknown flag `progress'"},
		{"позиционный аргумент", nil, []string{"extra"}, "неожиданный аргумент \"extra\""},
		{"нет --db", nil, []string{"--db", ""}, "--db не может быть пустой"},
		{"служебная --dst-db", nil, []string{"--dst-db", "admin"}, "служебная база"},
		{"--log-file-level без --log-file", nil, []string{"--log-file-level", "debug"}, "--log-file-level задан без --log-file"},
	} {
		t.Run(c.name, func(t *testing.T) {
			clearEnv(t)
			setEnv(t, c.env)

			_, err := parseVerifyOpts(append(base, c.args...))

			require.Error(t, err)
			assert.False(t, isHelp(err), "ошибка — не --help")
			assert.Contains(t, err.Error(), c.want)
		})
	}
}

// VERIFY_UNTIL_CAUGHT_UP=false и пустые env опций сверки не задают опцию: с --ids не конфликтуют.
func TestParseVerifyOptsIDsEnvNotGiven(t *testing.T) {
	clearEnv(t)
	t.Setenv("VERIFY_UNTIL_CAUGHT_UP", "false")

	opts, err := parseVerifyOpts([]string{"--src", "s", "--dst", "d", "--db", "db", "--ids", "f.json"})

	require.NoError(t, err)
	assert.False(t, opts.UntilCaughtUp, "до догона")
}

// У формы сверки своя справка: опции сверки без опций запуска.
func TestParseVerifyOptsHelp(t *testing.T) {
	clearEnv(t)

	_, err := parseVerifyOpts([]string{"--help"})

	require.Error(t, err)
	assert.True(t, isHelp(err), "ошибка — --help")
	assert.Contains(t, err.Error(), "verify [OPTIONS]")
	assert.Contains(t, err.Error(), "SRC_URI")
	assert.Contains(t, err.Error(), "--dst-db")
	assert.Contains(t, err.Error(), "--state-dir")
	assert.Contains(t, err.Error(), "--since")
	assert.Contains(t, err.Error(), "VERIFY_SINCE")
	assert.Contains(t, err.Error(), "--rate")
	assert.Contains(t, err.Error(), "--sample")
	assert.Contains(t, err.Error(), "--until-caught-up")
	assert.Contains(t, err.Error(), "--ids")
	assert.Contains(t, err.Error(), "--schema-only")
	assert.NotContains(t, err.Error(), "--start-ts")
	assert.NotContains(t, err.Error(), "--apply-drop")
	assert.NotContains(t, err.Error(), "--progress")
	assert.NotContains(t, err.Error(), "--no-clear")
}
