package main

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/jessevdk/go-flags"

	"github.com/sergey-frv/mongo-replicator/internal/replicator"
)

type options struct {
	Src       string `long:"src" env:"SRC_URI" description:"URI источника (replica set, MongoDB 6.0+); без пароля в URI он будет запрошен в терминале; URI с паролем передавайте через SRC_URI, не флагом" required:"true"`
	Dst       string `long:"dst" env:"DST_URI" description:"URI цели; без пароля в URI он будет запрошен в терминале; URI с паролем передавайте через DST_URI, не флагом" required:"true"`
	StartTS   uint32 `long:"start-ts" env:"START_TS" default:"0" description:"unix-сек, с которых читать поток при отсутствии чекпойнта (раньше начала полного дампа)"`
	BatchSize int    `long:"batch" env:"BATCH_SIZE" default:"1000" description:"максимум операций в одной пачке записи"`
	DB        string `long:"db" env:"DB" required:"true" description:"база источника: поток уровня базы, права на источнике — только на неё. Rename коллекций внутри базы поддерживается, перенос коллекций между базами не предусмотрен"`
	DstDB     string `long:"dst-db" env:"DST_DB" description:"база на цели; по умолчанию — как --db"`
	// флаг заменяет APPLY_DROP, а не дополняет: go-flags сбрасывает значение из env при первом флаге
	ApplyDrops []string `long:"apply-drop" env:"APPLY_DROP" env-delim:"," description:"разрешить drop коллекции источника <db>.<коллекция> (одноразово; можно повторять; флаг заменяет APPLY_DROP, а не дополняет)"`
	// choice проверяется и для env: LOG_LEVEL= и LOG_FILE_LEVEL= — ошибка
	LogLevel     string `long:"log-level" env:"LOG_LEVEL" default:"info" choice:"debug" choice:"info" choice:"warn" choice:"error" description:"уровень лога в консоли (stdout): текст в терминале, JSON при перенаправлении"`
	LogFile      string `long:"log-file" env:"LOG_FILE" description:"путь к файлу лога (JSON, дозапись; каталог должен существовать); ротация — снаружи, logrotate copytruncate"`
	LogFileLevel string `long:"log-file-level" env:"LOG_FILE_LEVEL" choice:"debug" choice:"info" choice:"warn" choice:"error" description:"уровень лога в файле; по умолчанию info, только вместе с --log-file"`
	Progress     string `long:"progress" env:"PROGRESS" default:"auto" choice:"auto" choice:"tty" choice:"plain" description:"панель статуса в терминале (tty; в консоли — только WARN и выше) или построчный лог (plain); auto — панель, если stdout — терминал и TERM не dumb"`
	StateDir     string `long:"state-dir" env:"STATE_DIR" description:"каталог файлов состояния на машине (накопленный трафик процессов, сводка по ним); по умолчанию $XDG_STATE_HOME/mongo-replicator или ~/.local/state/mongo-replicator"`
}

const maxDBNameLen = 63 // MongoDB: имя базы короче 64 байт

// parseOpts разбирает аргументы и env. Любая ошибка означает выход с кодом 2
// (кроме --help, его различает main).
func parseOpts(args []string) (options, error) {
	var opts options
	p := flags.NewParser(&opts, flags.HelpFlag|flags.PassDoubleDash)
	if _, err := p.ParseArgs(args); err != nil {
		return opts, err
	}
	if opts.Src == "" || opts.Dst == "" {
		return opts, errors.New("--src и --dst не могут быть пустыми")
	}
	// batchSize потока в протоколе — int32
	if opts.BatchSize < 1 || opts.BatchSize > math.MaxInt32 {
		return opts, fmt.Errorf("--batch должен быть от 1 до %d, получено %d", math.MaxInt32, opts.BatchSize)
	}
	// DB= из env go-flags считает заданной опцией
	if opts.DB == "" {
		return opts, errors.New("--db не может быть пустой")
	}
	if err := checkDBName("--db", opts.DB); err != nil {
		return opts, err
	}
	if opts.DstDB == "" {
		opts.DstDB = opts.DB
	}
	if err := checkDBName("--dst-db", opts.DstDB); err != nil {
		return opts, err
	}
	drops, err := checkApplyDrops(opts.DB, opts.ApplyDrops)
	if err != nil {
		return opts, err
	}
	opts.ApplyDrops = drops
	// LOG_FILE_LEVEL= отвергает choice, поэтому непустой уровень задан явно
	if opts.LogFileLevel != "" && opts.LogFile == "" {
		return opts, errors.New("--log-file-level задан без --log-file")
	}
	if opts.LogFileLevel == "" {
		opts.LogFileLevel = "info"
	}
	return opts, nil
}

// stateDir возвращает каталог файлов состояния и создаёт его: dir из --state-dir, а пустой —
// $XDG_STATE_HOME/mongo-replicator, иначе ~/.local/state/mongo-replicator. Относительный
// XDG_STATE_HOME игнорируется, как требует спецификация XDG Base Directory.
func stateDir(dir string) (string, error) {
	if dir == "" {
		if x := os.Getenv("XDG_STATE_HOME"); filepath.IsAbs(x) {
			dir = filepath.Join(x, "mongo-replicator")
		} else {
			home, err := os.UserHomeDir()
			if err != nil {
				return "", fmt.Errorf("--state-dir: каталог по умолчанию: %w", err)
			}
			dir = filepath.Join(home, ".local", "state", "mongo-replicator")
		}
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return "", fmt.Errorf("--state-dir: %w", err)
	}
	return dir, nil
}

// checkApplyDrops чистит список --apply-drop (пробелы вокруг, пустые элементы — APPLY_DROP=)
// и проверяет, что каждый элемент — <db>.<коллекция> базы источника.
func checkApplyDrops(db string, nss []string) ([]string, error) {
	var out []string
	for _, ns := range nss {
		ns = strings.TrimSpace(ns)
		if ns == "" {
			continue
		}
		nsDB, coll, ok := strings.Cut(ns, ".")
		if !ok || coll == "" {
			return nil, fmt.Errorf("--apply-drop %q: ожидается <база>.<коллекция>", ns)
		}
		if nsDB != db {
			return nil, fmt.Errorf("--apply-drop %q: база не совпадает с --db %q", ns, db)
		}
		out = append(out, ns)
	}
	return out, nil
}

// checkDBName проверяет имя базы по ограничениям MongoDB и запрещает служебные базы.
// Служебные сравниваются без учёта регистра: Admin на цели сервер всё равно не создаст (13297).
func checkDBName(opt, name string) error {
	if strings.ContainsAny(name, "/\\. \"$\x00") {
		return fmt.Errorf("%s %q: имя базы не может содержать /\\. \"$ и NUL", opt, name)
	}
	if len(name) > maxDBNameLen {
		return fmt.Errorf("%s %q: имя базы длиннее %d байт", opt, name, maxDBNameLen)
	}
	if slices.ContainsFunc(replicator.SystemDBs, func(s string) bool { return strings.EqualFold(s, name) }) {
		return fmt.Errorf("%s %q: служебная база не реплицируется", opt, name)
	}
	return nil
}

func isHelp(err error) bool {
	var flagsErr *flags.Error
	return errors.As(err, &flagsErr) && errors.Is(flagsErr.Type, flags.ErrHelp)
}

// Режимы --progress после разрешения auto.
const (
	progressTTY   = "tty"   // панель статуса, в консоли — только WARN и выше
	progressPlain = "plain" // построчный лог
)

// progressMode разрешает --progress: auto — панель, только если stdout — терминал,
// умеющий управляющие последовательности.
func progressMode(opt string, stdoutTTY bool, termEnv string) string {
	if opt != "auto" {
		return opt
	}
	if stdoutTTY && termEnv != "dumb" {
		return progressTTY
	}
	return progressPlain
}
