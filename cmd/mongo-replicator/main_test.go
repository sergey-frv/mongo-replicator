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
		{"не URI", "garbage", "garbage"},
		{"пусто", "", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, maskURI(c.in))
		})
	}
}

// clearEnv убирает env опций: в контейнере app заданы SRC_URI/DST_URI.
func clearEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{"SRC_URI", "DST_URI", "START_TS", "BATCH_SIZE", "DEBUG", "DB", "ALL_DBS"} {
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
	t.Setenv("DEBUG", "true")
	t.Setenv("ALL_DBS", "true")

	opts, err := parseOpts(nil)
	require.NoError(t, err)
	assert.Equal(t, options{Src: "mongodb://src", Dst: "mongodb://dst", StartTS: 1791900000, BatchSize: 50, Debug: true, AllDBs: true}, opts)
}

func TestParseOptsDefaults(t *testing.T) {
	clearEnv(t)
	opts, err := parseOpts([]string{"--src", "mongodb://src", "--dst", "mongodb://dst", "--all-dbs"})
	require.NoError(t, err)
	assert.Equal(t, options{Src: "mongodb://src", Dst: "mongodb://dst", BatchSize: 1000, AllDBs: true}, opts)
}

func TestParseOptsFlagsOverrideEnv(t *testing.T) {
	clearEnv(t)
	t.Setenv("SRC_URI", "mongodb://src-env")
	t.Setenv("DST_URI", "mongodb://dst-env")
	t.Setenv("START_TS", "1")
	t.Setenv("BATCH_SIZE", "50")

	opts, err := parseOpts([]string{"--src", "mongodb://src-flag", "--start-ts=2", "--batch", "7", "--all-dbs"})
	require.NoError(t, err)
	assert.Equal(t, options{Src: "mongodb://src-flag", Dst: "mongodb://dst-env", StartTS: 2, BatchSize: 7, AllDBs: true}, opts)
}

func TestParseOptsMode(t *testing.T) {
	base := []string{"--src", "s", "--dst", "d"}
	cases := []struct {
		name   string
		env    map[string]string
		args   []string
		db     string
		allDBs bool
	}{
		{"--db", nil, []string{"--db", "urlshrinker"}, "urlshrinker", false},
		{"DB из env", map[string]string{"DB": "urlshrinker"}, nil, "urlshrinker", false},
		{"флаг перекрывает env", map[string]string{"DB": "env"}, []string{"--db", "flag"}, "flag", false},
		{"--all-dbs", nil, []string{"--all-dbs"}, "", true},
		{"ALL_DBS из env", map[string]string{"ALL_DBS": "true"}, nil, "", true},
		{"имя ровно 63 байта", nil, []string{"--db", strings.Repeat("a", 63)}, strings.Repeat("a", 63), false},
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
			assert.Equal(t, c.allDBs, opts.AllDBs)
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
		{"нет --src", nil, []string{"--dst", "mongodb://dst"}, "--src"},
		{"нет --dst", nil, []string{"--src", "mongodb://src"}, "--dst"},
		{"нет обоих", nil, nil, "--src"},
		{"пустой SRC_URI", map[string]string{"SRC_URI": ""}, []string{"--dst", "mongodb://dst"}, "--src"},
		{"--batch 0", nil, []string{"--src", "s", "--dst", "d", "--batch", "0"}, "--batch должен быть"},
		{"BATCH_SIZE<0", map[string]string{"BATCH_SIZE": "-5"}, []string{"--src", "s", "--dst", "d"}, "--batch должен быть"},
		{"--batch больше int32", nil, []string{"--src", "s", "--dst", "d", "--batch", "3000000000"}, "--batch должен быть"},
		{"отрицательный --start-ts", nil, []string{"--src", "s", "--dst", "d", "--start-ts=-1"}, "--start-ts"},
		{"нечисловой --start-ts", nil, []string{"--src", "s", "--dst", "d", "--start-ts", "abc"}, "--start-ts"},
		{"одиночный дефис", nil, []string{"-src", "s", "-dst", "d"}, "unknown flag"},
		{"неизвестный флаг", nil, []string{"--src", "s", "--dst", "d", "--foo"}, "unknown flag `foo'"},
		{"нет ни --db, ни --all-dbs", nil, []string{"--src", "s", "--dst", "d"}, "укажите --db <база> или --all-dbs"},
		{"пустой DB без --all-dbs", map[string]string{"DB": ""}, []string{"--src", "s", "--dst", "d"}, "укажите --db <база> или --all-dbs"},
		{"--db и --all-dbs", nil, []string{"--src", "s", "--dst", "d", "--db", "x", "--all-dbs"}, "взаимоисключающие"},
		{"DB из env и --all-dbs", map[string]string{"DB": "x"}, []string{"--src", "s", "--dst", "d", "--all-dbs"}, "взаимоисключающие"},
		{"пустой ALL_DBS", map[string]string{"ALL_DBS": "", "DB": ""}, []string{"--src", "s", "--dst", "d"}, "ALL_DBS задан пустым"},
		{"пустой ALL_DBS и --db", map[string]string{"ALL_DBS": ""}, []string{"--src", "s", "--dst", "d", "--db", "x"}, "ALL_DBS задан пустым"},
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
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			clearEnv(t)
			_, err := parseOpts([]string{"--src", "s", "--dst", "d", "--db", c.db})
			require.Error(t, err)
			assert.Contains(t, err.Error(), c.want)
		})
	}
}

func TestParseOptsHelp(t *testing.T) {
	clearEnv(t)
	_, err := parseOpts([]string{"--help"})
	require.Error(t, err)
	assert.True(t, isHelp(err))
	assert.Contains(t, err.Error(), "SRC_URI")
	assert.Contains(t, err.Error(), "--start-ts")
	assert.Contains(t, err.Error(), "--all-dbs")
}

func TestPermanent(t *testing.T) {
	cases := []struct {
		name string
		err  error
		hint string // подстрока подсказки; "" — ошибка временная
	}{
		{"history lost", fmt.Errorf("run: %w", mongo.CommandError{Code: 286}), "выпала из oplog"},
		{"нет точки старта", fmt.Errorf("run: %w", replicator.ErrNoStartPoint), "--start-ts"},
		{"другая база", fmt.Errorf("run: %w", replicator.ErrDBMismatch), "другой базы"},
		{"поток инвалидирован", fmt.Errorf("run: %w", replicator.ErrStreamInvalidated), "dropDatabase"},
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
