package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// restoreLogger возвращает log.Logger после теста: setupLog его подменяет
func restoreLogger(t *testing.T) {
	t.Helper()
	saved := log.Logger
	t.Cleanup(func() { log.Logger = saved })
}

// jsonLines разбирает вывод JSON-приёмника построчно
func jsonLines(t *testing.T, s string) []map[string]any {
	t.Helper()
	res := make([]map[string]any, 0, strings.Count(s, "\n"))
	for line := range strings.Lines(s) {
		var m map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &m), "строка не JSON: %q", line)
		res = append(res, m)
	}
	return res
}

// messages — значения поля m по строкам JSON
func messages(t *testing.T, s string) []string {
	t.Helper()
	lines := jsonLines(t, s)
	res := make([]string, 0, len(lines))
	for _, m := range lines {
		res = append(res, m["m"].(string))
	}
	return res
}

// ansi — цветовые escape-последовательности ConsoleWriter
var ansi = regexp.MustCompile(`\x1b\[[0-9;]*m`)

func TestNewLoggerConsoleText(t *testing.T) {
	var cons bytes.Buffer
	l := newLogger(&cons, true, zerolog.InfoLevel, nil, zerolog.InfoLevel)

	l.Info().Str("k", "v").Msg("привет")
	l.Error().Err(errors.New("сломалось")).Msg("ошибка")
	out := ansi.ReplaceAllString(cons.String(), "")

	assert.Contains(t, out, "INF")
	assert.Contains(t, out, "привет")
	assert.Contains(t, out, "k=v")
	assert.Contains(t, out, "ERR")
	assert.Contains(t, out, `e="сломалось"`)
	// имена полей рассинхронизированы с ConsoleWriter — служебные поля вылезут как key=value
	for _, f := range []string{"m=", "l=", "t="} {
		assert.NotContains(t, out, f)
	}
	assert.False(t, json.Valid([]byte(strings.SplitN(out, "\n", 2)[0])), "в терминале ожидался текст")
}

func TestNewLoggerConsoleJSON(t *testing.T) {
	var cons bytes.Buffer
	l := newLogger(&cons, false, zerolog.InfoLevel, nil, zerolog.InfoLevel)

	l.Info().Msg("привет")
	l.Error().Err(errors.New("сломалось")).Msg("ошибка")
	lines := jsonLines(t, cons.String())

	require.Len(t, lines, 2)
	assert.Equal(t, "привет", lines[0]["m"])
	assert.Equal(t, "info", lines[0]["l"])
	assert.NotEmpty(t, lines[0]["t"])
	assert.Equal(t, "error", lines[1]["l"])
	assert.Equal(t, "сломалось", lines[1]["e"])
}

func TestNewLoggerFileJSON(t *testing.T) {
	var cons, file bytes.Buffer
	l := newLogger(&cons, true, zerolog.InfoLevel, &file, zerolog.InfoLevel)

	l.Info().Msg("привет")
	l.Warn().Err(errors.New("сломалось")).Msg("ошибка")
	lines := jsonLines(t, file.String())

	require.Len(t, lines, 2)
	assert.Equal(t, "привет", lines[0]["m"])
	assert.Equal(t, "info", lines[0]["l"])
	assert.NotEmpty(t, lines[0]["t"])
	assert.Equal(t, "warn", lines[1]["l"])
	assert.Equal(t, "сломалось", lines[1]["e"])
	assert.Contains(t, cons.String(), "привет")
}

func TestNewLoggerLevels(t *testing.T) {
	type want struct{ cons, file []string }
	cases := []struct {
		name            string
		consLvl, fileLv zerolog.Level
		want            want
	}{
		{"консоль info, файл debug", zerolog.InfoLevel, zerolog.DebugLevel,
			want{cons: []string{"info", "warn"}, file: []string{"debug", "info", "warn"}}},
		{"консоль debug, файл warn", zerolog.DebugLevel, zerolog.WarnLevel,
			want{cons: []string{"debug", "info", "warn"}, file: []string{"warn"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var cons, file bytes.Buffer
			l := newLogger(&cons, false, c.consLvl, &file, c.fileLv)

			l.Debug().Msg("debug")
			l.Info().Msg("info")
			l.Warn().Msg("warn")

			assert.Equal(t, c.want.cons, messages(t, cons.String()))
			assert.Equal(t, c.want.file, messages(t, file.String()))
		})
	}
}

func TestNewLoggerNoFile(t *testing.T) {
	var cons bytes.Buffer
	l := newLogger(&cons, false, zerolog.WarnLevel, nil, zerolog.DebugLevel)

	require.NotPanics(t, func() {
		l.Debug().Msg("debug")
		l.Info().Msg("info")
		l.Warn().Msg("warn")
	})

	assert.Equal(t, []string{"warn"}, messages(t, cons.String()))
}

// captureStdout подменяет os.Stdout файлом (не терминал → JSON) на время теста
func captureStdout(t *testing.T) *os.File {
	t.Helper()
	f, err := os.Create(filepath.Join(t.TempDir(), "stdout"))
	require.NoError(t, err)
	saved := os.Stdout
	os.Stdout = f
	t.Cleanup(func() {
		os.Stdout = saved
		_ = f.Close()
	})
	return f
}

func TestSetupLogNoDir(t *testing.T) {
	restoreLogger(t)
	captureStdout(t)
	opts := commonOptions{LogLevel: "info", LogFile: filepath.Join(t.TempDir(), "нет", "r.log"), LogFileLevel: "info"}

	f, err := setupLog(opts)

	require.Error(t, err)
	assert.Nil(t, f)
}

func TestSetupLogCreateMode(t *testing.T) {
	restoreLogger(t)
	captureStdout(t)
	path := filepath.Join(t.TempDir(), "r.log")

	f, err := setupLog(commonOptions{LogLevel: "info", LogFile: path, LogFileLevel: "info"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })
	fi, err := os.Stat(path)

	require.NoError(t, err)
	// umask может только снять биты: прочим — ничего, группе — не больше чтения
	assert.Zero(t, fi.Mode().Perm()&^0o640, "права %o", fi.Mode().Perm())
}

func TestSetupLogAppend(t *testing.T) {
	restoreLogger(t)
	stdout := captureStdout(t)
	path := filepath.Join(t.TempDir(), "r.log")
	const old = "старая строка\n"
	require.NoError(t, os.WriteFile(path, []byte(old), 0o600))

	f, err := setupLog(commonOptions{LogLevel: "warn", LogFile: path, LogFileLevel: "debug"})
	require.NoError(t, err)
	require.NotNil(t, f)
	t.Cleanup(func() { _ = f.Close() })
	log.Debug().Msg("отладка")
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	cons, err := os.ReadFile(stdout.Name())
	require.NoError(t, err)

	require.True(t, strings.HasPrefix(string(data), old), "старое содержимое затёрто: %q", data)
	lines := jsonLines(t, strings.TrimPrefix(string(data), old))
	require.Len(t, lines, 2)
	assert.Equal(t, "лог", lines[0]["m"])
	assert.Equal(t, "warn/json", lines[0]["console"])
	assert.Equal(t, path, lines[0]["file"])
	assert.Equal(t, "debug", lines[0]["file_level"])
	assert.Equal(t, "отладка", lines[1]["m"])
	assert.Empty(t, cons, "консоль warn: ни «лог», ни DEBUG в stdout не попали")
}

func TestSetupLogNoFile(t *testing.T) {
	restoreLogger(t)
	stdout := captureStdout(t)
	dir := t.TempDir()
	t.Chdir(dir)
	// typed-nil *os.File в MultiLevelWriter не роняет консоль, а только зовёт ErrorHandler
	savedHandler := zerolog.ErrorHandler
	// обработчик зовётся из пишущей горутины — только assert
	zerolog.ErrorHandler = func(err error) { assert.NoError(t, err, "запись лога") }
	t.Cleanup(func() { zerolog.ErrorHandler = savedHandler })

	f, err := setupLog(commonOptions{LogLevel: "info", LogFileLevel: "info"})
	entries, readDirErr := os.ReadDir(dir)
	cons, readErr := os.ReadFile(stdout.Name())

	require.NoError(t, err)
	assert.Nil(t, f)
	require.NoError(t, readDirErr)
	assert.Empty(t, entries, "файл лога создан без --log-file")
	require.NoError(t, readErr)
	lines := jsonLines(t, string(cons))
	require.Len(t, lines, 1)
	assert.Equal(t, "лог", lines[0]["m"])
	assert.Equal(t, "info/json", lines[0]["console"])
	assert.Empty(t, lines[0]["file"])
	assert.Contains(t, lines[0], "file")
	assert.NotContains(t, lines[0], "file_level")
}
