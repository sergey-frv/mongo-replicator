package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	for _, k := range []string{"SRC_URI", "DST_URI", "START_TS", "BATCH_SIZE", "DEBUG"} {
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

	opts, err := parseOpts(nil)
	require.NoError(t, err)
	assert.Equal(t, options{Src: "mongodb://src", Dst: "mongodb://dst", StartTS: 1791900000, BatchSize: 50, Debug: true}, opts)
}

func TestParseOptsDefaults(t *testing.T) {
	clearEnv(t)
	opts, err := parseOpts([]string{"--src", "mongodb://src", "--dst", "mongodb://dst"})
	require.NoError(t, err)
	assert.Equal(t, options{Src: "mongodb://src", Dst: "mongodb://dst", BatchSize: 1000}, opts)
}

func TestParseOptsFlagsOverrideEnv(t *testing.T) {
	clearEnv(t)
	t.Setenv("SRC_URI", "mongodb://src-env")
	t.Setenv("DST_URI", "mongodb://dst-env")
	t.Setenv("START_TS", "1")
	t.Setenv("BATCH_SIZE", "50")

	opts, err := parseOpts([]string{"--src", "mongodb://src-flag", "--start-ts=2", "--batch", "7"})
	require.NoError(t, err)
	assert.Equal(t, options{Src: "mongodb://src-flag", Dst: "mongodb://dst-env", StartTS: 2, BatchSize: 7}, opts)
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

func TestParseOptsHelp(t *testing.T) {
	clearEnv(t)
	_, err := parseOpts([]string{"--help"})
	require.Error(t, err)
	assert.True(t, isHelp(err))
	assert.Contains(t, err.Error(), "SRC_URI")
	assert.Contains(t, err.Error(), "--start-ts")
}
