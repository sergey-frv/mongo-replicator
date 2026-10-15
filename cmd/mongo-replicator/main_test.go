package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"mongo-replicator/cmd/mongo-replicator/replicator"
)

func TestMaskURI(t *testing.T) {
	cases := []struct{ name, in, want string }{
		{"с паролем", "mongodb://user:secret@h1:27017/db?replicaSet=rs8", "mongodb://user:xxxxx@h1:27017/db?replicaSet=rs8"},
		{"без userinfo", "mongodb://h1:27017/?replicaSet=rs8", "mongodb://h1:27017/?replicaSet=rs8"},
		{"только user", "mongodb://user@h1:27017/", "mongodb://user@h1:27017/"},
		{"multi-host", "mongodb://u:p@h1:27017,h2:27017/admin?replicaSet=x", "mongodb://u:xxxxx@h1:27017,h2:27017/admin?replicaSet=x"},
		{"multi-host без / перед ?", "mongodb://u:p@h1,h2?replicaSet=x", "mongodb://u:xxxxx@h1,h2?replicaSet=x"},
		{"srv", "mongodb+srv://u:p@cluster.example.com/?tls=true", "mongodb+srv://u:xxxxx@cluster.example.com/?tls=true"},
		{"percent-encoding", "mongodb://u%40x:p%40%3Ass@h1/", "mongodb://u%40x:xxxxx@h1/"},
		{"сырой @ в пароле", "mongodb://u:p@ss@h1/", "mongodb://u:xxxxx@h1/"},
		{"? в пароле", "mongodb://u:p?ss@h1/", "mongodb://u:xxxxx@h1/"},
		{"@ в query", "mongodb://h1/?authSource=a@b", "mongodb://h1/?authSource=a@b"},
		{"без хоста", "mongodb://u:p@", "mongodb://u:xxxxx@"},
		{":// не в начале", "mongodb:u:p@h1://x:y@h2/", "mongodb:u:p@h1://x:y@h2/"},
		{"схема с недопустимым символом", "mongo db://u:p@h1/", "mongo db://u:p@h1/"},
		{"не URI", "garbage", "garbage"},
		{"пусто", "", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, maskURI(c.in))
		})
	}
}

func TestLogURI(t *testing.T) {
	const hidden = "(скрыт: в нём может быть пароль)"
	assert.Equal(t, "mongodb://u:xxxxx@h1/", logURI("mongodb://u:Secret@h1/"))
	assert.Equal(t, "mongodb://u@h1:27017/", logURI("mongodb://u@h1:27017/"))
	assert.Equal(t, "garbage", logURI("garbage"))
	assert.Equal(t, hidden, logURI("mongodb://u:Secret/x@h1/"), "неэкранированный / в пароле")
	assert.Equal(t, hidden, logURI("mongodb:u:Secret@h1/"), "без ://")
	assert.Equal(t, hidden, logURI("mongodb:u:Secret@h1://bad"), ":// после пароля")
	assert.Equal(t, hidden, logURI("mongodb:u:Secret@h1://x:y@h2/"), ":// после пароля и userinfo за ним")
	assert.Equal(t, hidden, logURI("mongodb://u:prefix@x/Secret@h/?connectTimeoutMS=bad"),
		"неэкранированные @ и / в пароле: maskURI маскирует только начало")
	assert.Equal(t, hidden, logURI("mongodb://u@x:Secret@h1/"), "неэкранированный @ в имени")
	assert.Equal(t, "mongodb://h1/?authSource=a@b", logURI("mongodb://h1/?authSource=a@b"), "@ в query без пароля")
	assert.Equal(t, "mongodb://u:xxxxx@h1/?tls=true&tlsCertificateKeyFilePassword=xxxxx",
		logURI("mongodb://u:Secret@h1/?tls=true&tlsCertificateKeyFilePassword=Secret"), "пароль и секрет в query")
	assert.Equal(t, "mongodb:h1/?tlsCertificateKeyFilePassword=xxxxx",
		logURI("mongodb:h1/?tlsCertificateKeyFilePassword=Secret"), "секрет в query без ://")
	assert.Equal(t, "mongodb://u:xxxxx@h1/?tlsCertificateKeyFilePassword=xxxxx",
		logURI("mongodb://u:Secret@h1/?tlsCertificateKeyFilePassword=alpha?Secret"), "? в значении секрета")
}

func TestMaskOptions(t *testing.T) {
	cases := []struct{ name, in, want string }{
		{"tls", "mongodb://h1/?tlsCertificateKeyFilePassword=Secret&tls=true",
			"mongodb://h1/?tlsCertificateKeyFilePassword=xxxxx&tls=true"},
		{"ssl, регистр", "mongodb://h1/?SSLClientCertificateKeyPassword=Secret",
			"mongodb://h1/?SSLClientCertificateKeyPassword=xxxxx"},
		{"aws, разделитель ;", "mongodb://h1/?authMechanism=MONGODB-AWS;authMechanismProperties=AWS_SESSION_TOKEN:Secret",
			"mongodb://h1/?authMechanism=MONGODB-AWS;authMechanismProperties=xxxxx"},
		{"экранированный ключ", "mongodb://h1/?tlsCertificateKeyFilePassw%6Frd=Secret",
			"mongodb://h1/?tlsCertificateKeyFilePassw%6Frd=xxxxx"},
		{"неразбираемый ключ", "mongodb://h1/?tls%zz=Secret", "mongodb://h1/?tls%zz=xxxxx"},
		{"пустое значение", "mongodb://h1/?tlsCertificateKeyFilePassword=", "mongodb://h1/?tlsCertificateKeyFilePassword=xxxxx"},
		{"? в значении", "mongodb://h1/?tlsCertificateKeyFilePassword=alpha?Secret&tls=true",
			"mongodb://h1/?tlsCertificateKeyFilePassword=xxxxx&tls=true"},
		{"? в значении после ;", "mongodb://h1/?tls=true;authMechanismProperties=AWS_SESSION_TOKEN:a?Secret",
			"mongodb://h1/?tls=true;authMechanismProperties=xxxxx"},
		{"? в имени пользователя", "mongodb://us?er@h1/?tlsCertificateKeyFilePassword=a?Secret",
			"mongodb://us?er@h1/?tlsCertificateKeyFilePassword=xxxxx"},
		{"без секретов", "mongodb://h1/db?replicaSet=rs&authSource=a@b", "mongodb://h1/db?replicaSet=rs&authSource=a@b"},
		{"не URI", "garbage", "garbage"},
		{"пусто", "", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, maskOptions(c.in))
		})
	}
}

// clearEnv убирает env опций: в контейнере app заданы SRC_URI/DST_URI.
func clearEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{"SRC_URI", "DST_URI", "START_TS", "BATCH_SIZE", "DB", "DST_DB", "APPLY_DROP",
		"LOG_LEVEL", "LOG_FILE", "LOG_FILE_LEVEL"} {
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

	opts, err := parseOpts(nil)
	require.NoError(t, err)
	assert.Equal(t, options{Src: "mongodb://src", Dst: "mongodb://dst", StartTS: 1791900000, BatchSize: 50, DB: "urlshrinker", DstDB: "urlshrinker",
		LogLevel: "debug", LogFile: "/var/log/repl.log", LogFileLevel: "warn"}, opts)
}

func TestParseOptsDefaults(t *testing.T) {
	clearEnv(t)
	opts, err := parseOpts([]string{"--src", "mongodb://src", "--dst", "mongodb://dst", "--db", "urlshrinker"})
	require.NoError(t, err)
	assert.Equal(t, options{Src: "mongodb://src", Dst: "mongodb://dst", BatchSize: 1000, DB: "urlshrinker", DstDB: "urlshrinker",
		LogLevel: "info", LogFileLevel: "info"}, opts)
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

	opts, err := parseOpts([]string{"--src", "mongodb://src-flag", "--start-ts=2", "--batch", "7", "--db", "flag",
		"--log-level", "debug", "--log-file", "flag.log", "--log-file-level", "info"})
	require.NoError(t, err)
	assert.Equal(t, options{Src: "mongodb://src-flag", Dst: "mongodb://dst-env", StartTS: 2, BatchSize: 7, DB: "flag", DstDB: "flag",
		LogLevel: "debug", LogFile: "flag.log", LogFileLevel: "info"}, opts)
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
		{"_replicator", "_replicator", "служебная база"},
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

func TestPermanent(t *testing.T) {
	cases := []struct {
		name string
		err  error
		hint string // подстрока подсказки; "" — ошибка временная
	}{
		{"history lost", fmt.Errorf("run: %w", mongo.CommandError{Code: 286}), "выпала из oplog"},
		{"событие больше 16 МБ", fmt.Errorf("change stream: %w", mongo.CommandError{Code: 10334}), "больше 16 МБ"},
		{"нет точки старта", fmt.Errorf("run: %w", replicator.ErrNoStartPoint), "--start-ts"},
		{"другая база", fmt.Errorf("run: %w", replicator.ErrDBMismatch), `deleteOne({_id: "<dst-db>"})`},
		{"аренда занята", fmt.Errorf("run: %w", replicator.ErrLeaseHeld), "аренда истечёт через ≤60 с"},
		{"аренда потеряна", fmt.Errorf("сохранение чекпойнта: %w", replicator.ErrLeaseLost), "два репликатора на одну базу"},
		{"событие другой базы", fmt.Errorf("run: %w", replicator.ErrForeignNamespace), "сообщите о баге"},
		{"поток инвалидирован", fmt.Errorf("run: %w", replicator.ErrStreamInvalidated), "новая полная заливка"},
		{"база в другом регистре", fmt.Errorf("bulk write: %w", mongo.CommandError{Code: 13297}), "--dst-db"},
		{"drop коллекции", fmt.Errorf("run: %w", &replicator.DropError{NS: "db.c"}), "--apply-drop db.c"},
		{"dropDatabase", fmt.Errorf("run: %w", &replicator.DropError{NS: "db", Database: true}), "новая полная заливка"},
		{"обычная ошибка", errors.New("boom"), ""},
		{"отмена", context.Canceled, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.hint != "", permanent(c.err))
			if c.hint == "" {
				assert.Empty(t, hint(c.err))
			} else {
				assert.Contains(t, hint(c.err), c.hint)
			}
		})
	}
}

func TestIsAuthFailed(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"неверный пароль", fmt.Errorf("ping: %w", mongo.CommandError{Code: 18, Wrapped: errors.New("auth")}), true},
		{"другой код", fmt.Errorf("ping: %w", mongo.CommandError{Code: 13}), false},
		{"обычная ошибка", errors.New("boom"), false},
		{"nil", nil, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, isAuthFailed(c.err))
		})
	}
}
