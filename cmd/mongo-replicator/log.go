package main

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"golang.org/x/term"
)

// logLevels — значения --log-level и --log-file-level (choice уже проверен go-flags);
// не zerolog.ParseLevel: тот принимает "1", "INFO", "trace"
var logLevels = map[string]zerolog.Level{
	"debug": zerolog.DebugLevel,
	"info":  zerolog.InfoLevel,
	"warn":  zerolog.WarnLevel,
	"error": zerolog.ErrorLevel,
}

// newLogger собирает логгер из двух приёмников; file == nil — только консоль.
// Выставляет короткие имена полей zerolog.
func newLogger(console io.Writer, consoleTTY bool, consoleLevel zerolog.Level,
	file io.Writer, fileLevel zerolog.Level,
) zerolog.Logger {
	// ConsoleWriter читает имена полей в момент записи, так что они общие для обоих приёмников
	zerolog.MessageFieldName = "m"
	zerolog.ErrorFieldName = "e"
	zerolog.LevelFieldName = "l"
	zerolog.TimestampFieldName = "t"

	// в терминале — текст для человека, при перенаправлении — JSON
	if consoleTTY {
		console = zerolog.ConsoleWriter{Out: console, TimeFormat: time.RFC3339}
	}
	cons := &zerolog.FilteredLevelWriter{Writer: zerolog.LevelWriterAdapter{Writer: console}, Level: consoleLevel}
	if file == nil {
		return zerolog.New(cons).Level(consoleLevel).With().Timestamp().Logger()
	}

	fileW := &zerolog.FilteredLevelWriter{Writer: zerolog.LevelWriterAdapter{Writer: file}, Level: fileLevel}
	return zerolog.New(zerolog.MultiLevelWriter(cons, fileW)).
		Level(min(consoleLevel, fileLevel)).With().Timestamp().Logger()
}

// setupLog открывает файл (если задан), определяет TTY stdout, ставит log.Logger
// и пишет строку «лог». Возвращает открытый файл (nil без --log-file).
// Запись без буфера: os.Exit и сигналы ничего не теряют, Sync не нужен.
func setupLog(opts commonOptions) (*os.File, error) {
	var f *os.File
	if opts.LogFile != "" {
		var err error
		f, err = os.OpenFile(opts.LogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640) //nolint:gosec // путь и права задаёт оператор (--log-file), группе — чтение
		if err != nil {
			return nil, fmt.Errorf("открыть файл лога: %w", err)
		}
	}

	tty := stdoutTTY()
	log.Logger = newLogger(os.Stdout, tty, logLevels[opts.LogLevel], fileWriter(f), logLevels[opts.LogFileLevel])

	format := "json"
	if tty {
		format = "text"
	}
	ev := log.Info().Str("console", opts.LogLevel+"/"+format).Str("file", opts.LogFile)
	if f != nil {
		ev = ev.Str("file_level", opts.LogFileLevel)
	}
	ev.Msg("лог")
	return f, nil
}

// fileWriter — файл лога как приёмник; nil-файл — nil-интерфейс: typed-nil *os.File не равен nil.
func fileWriter(f *os.File) io.Writer {
	if f == nil {
		return nil
	}
	return f
}

func stdoutTTY() bool {
	return term.IsTerminal(int(os.Stdout.Fd())) //nolint:gosec // дескриптор stdout помещается в int
}
