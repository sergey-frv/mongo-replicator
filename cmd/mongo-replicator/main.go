// mongo-replicator — односторонняя репликация MongoDB через change streams.
//
// Требования:
//   - источник: replica set, MongoDB 6.0+ (нужен showExpandedEvents для DDL);
//   - цель: на неё пишет только репликатор;
//   - на цели уже восстановлен полный дамп источника, а --start-ts указывает
//     на момент ДО начала этого дампа и ещё попадает в окно oplog источника.
package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/jessevdk/go-flags"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/v2/mongo"
	mongoopts "go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"

	"mongo-replicator/cmd/mongo-replicator/replicator"
)

type options struct {
	Src       string `long:"src" env:"SRC_URI" description:"URI источника (replica set, MongoDB 6.0+); без пароля в URI он будет запрошен в терминале; URI с паролем передавайте через SRC_URI, не флагом" required:"true"`
	Dst       string `long:"dst" env:"DST_URI" description:"URI цели; без пароля в URI он будет запрошен в терминале; URI с паролем передавайте через DST_URI, не флагом" required:"true"`
	StartTS   uint32 `long:"start-ts" env:"START_TS" default:"0" description:"unix-сек, с которых читать поток при отсутствии чекпойнта (раньше начала полного дампа)"`
	BatchSize int    `long:"batch" env:"BATCH_SIZE" default:"1000" description:"максимум операций в одной пачке записи"`
	Debug     bool   `long:"dbg" env:"DEBUG" description:"Debug mode"`
	DB        string `long:"db" env:"DB" description:"реплицировать только эту базу: поток уровня базы, права на источнике — только на неё. Rename коллекций внутри базы поддерживается, перенос коллекций между базами не предусмотрен"`
	AllDBs    bool   `long:"all-dbs" env:"ALL_DBS" description:"реплицировать все базы, кроме служебных (нужна readAnyDatabase на источнике)"`
}

var revision = "unknown"

const (
	appName      = "cs-replicator"
	pingTimeout  = 10 * time.Second
	retryTimeout = 5 * time.Second
	maxDBNameLen = 63 // MongoDB: имя базы короче 64 байт
)

func main() {
	opts, err := parseOpts(os.Args[1:])
	if err != nil {
		if isHelp(err) {
			fmt.Fprintln(os.Stdout, err)
			os.Exit(0)
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	setupLog(opts.Debug)

	log.Info().Str("revision", revision).Send()
	masked := opts
	masked.Src, masked.Dst = maskURI(opts.Src), maskURI(opts.Dst)
	log.Debug().Any("options", masked).Send()

	args := os.Args[1:]
	srcOpts, dstOpts, err := prepareClients(
		side{name: "источник", uri: opts.Src, envName: "SRC_URI", fromFlag: flagGiven(args, "src")},
		side{name: "цель", uri: opts.Dst, envName: "DST_URI", fromFlag: flagGiven(args, "dst")},
		termPrompter{},
		func(name string) {
			log.Warn().Str("cluster", name).Msg("пароль передан в --src/--dst: он остаётся в истории шелла и виден в ps; " +
				"передавайте URI с паролем через SRC_URI/DST_URI или без пароля")
		},
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	os.Exit(run(opts, srcOpts, dstOpts))
}

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
	// режим всех баз — только явно, чтобы не запустить его случайно. go-flags
	// читает пустую bool-переменную как true: ALL_DBS= из шаблона деплоя
	// включил бы все базы.
	if v, ok := os.LookupEnv("ALL_DBS"); ok && v == "" {
		return opts, errors.New("ALL_DBS задан пустым: укажите true/false или уберите переменную")
	}
	switch {
	case opts.DB == "" && !opts.AllDBs:
		return opts, errors.New("укажите --db <база> или --all-dbs")
	case opts.DB != "" && opts.AllDBs:
		return opts, errors.New("--db и --all-dbs взаимоисключающие")
	case opts.DB != "":
		if err := checkDBName(opts.DB); err != nil {
			return opts, err
		}
	}
	return opts, nil
}

// checkDBName проверяет имя базы по ограничениям MongoDB и запрещает служебные базы.
func checkDBName(name string) error {
	if strings.ContainsAny(name, "/\\. \"$\x00") {
		return fmt.Errorf("--db %q: имя базы не может содержать /\\. \"$ и NUL", name)
	}
	if len(name) > maxDBNameLen {
		return fmt.Errorf("--db %q: имя базы длиннее %d байт", name, maxDBNameLen)
	}
	if slices.Contains(replicator.SystemDBs, name) {
		return fmt.Errorf("--db %q: служебная база не реплицируется", name)
	}
	return nil
}

func isHelp(err error) bool {
	var flagsErr *flags.Error
	return errors.As(err, &flagsErr) && errors.Is(flagsErr.Type, flags.ErrHelp)
}

// run подключается к кластерам и крутит репликацию с ретраями. Возвращает код выхода.
// srcOpts/dstOpts — разобранные URI с найденными паролями (prepareClients).
func run(opts options, srcOpts, dstOpts *mongoopts.ClientOptions) int {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	src, err := connect(ctx, srcOpts.SetAppName(appName))
	if err != nil {
		return failCode(ctx, err, "источник")
	}
	defer src.Disconnect(context.Background()) //nolint:errcheck // выходим, ошибка не важна

	// w:majority на цели: чекпойнт не должен обогнать данные при failover.
	dst, err := connect(ctx, dstOpts.SetAppName(appName).SetWriteConcern(writeconcern.Majority()))
	if err != nil {
		return failCode(ctx, err, "цель")
	}
	defer dst.Disconnect(context.Background()) //nolint:errcheck // выходим, ошибка не важна

	r := replicator.New(src, dst, replicator.Config{
		StartTS: opts.StartTS, BatchSize: opts.BatchSize, DB: opts.DB,
	})
	for ctx.Err() == nil {
		err := r.Run(ctx)
		if ctx.Err() != nil {
			break
		}
		if permanent(err) {
			log.Error().Err(err).Msg(hint(err))
			return 1
		}
		log.Warn().Err(err).Str("retry", retryTimeout.String()).Msg("ошибка репликации, повтор")
		select {
		case <-ctx.Done():
		case <-time.After(retryTimeout):
		}
	}
	log.Info().Int64("applied", r.Applied()).
		Msg("остановлен по сигналу; незаписанный хвост будет перечитан при следующем запуске")
	return 0
}

// permanent сообщает, что ретраи ошибку не исправят и нужен оператор.
func permanent(err error) bool { return hint(err) != "" }

// hint — подсказка оператору по перманентной ошибке; "" — ошибка временная.
func hint(err error) string {
	switch {
	case replicator.IsHistoryLost(err):
		return "точка возобновления выпала из oplog источника — нужна новая полная заливка цели"
	case errors.Is(err, replicator.ErrNoStartPoint):
		return "задайте --start-ts"
	case errors.Is(err, replicator.ErrDBMismatch):
		return "чекпойнт на цели записан для другой базы: верните прежние --db/--all-dbs " +
			"или остановите репликатор, удалите чекпойнт и залейте цель заново; " +
			"если на эту цель пишет второй экземпляр — у каждого должна быть своя цель"
	case errors.Is(err, replicator.ErrStreamInvalidated):
		return "база удалена на источнике (dropDatabase), поток закрыт: " +
			"удалите чекпойнт на цели и запустите заново с --start-ts"
	}
	return ""
}

// connect создаёт клиента и проверяет доступность сервера: mongo.Connect ленивый
// и без Ping недоступный кластер превратился бы в вечные ретраи.
func connect(ctx context.Context, opts *mongoopts.ClientOptions) (*mongo.Client, error) {
	c, err := mongo.Connect(opts)
	if err != nil {
		return nil, err
	}
	pingCtx, cancel := context.WithTimeout(ctx, pingTimeout)
	defer cancel()
	if err := c.Ping(pingCtx, nil); err != nil {
		_ = c.Disconnect(context.Background())
		return nil, fmt.Errorf("ping: %w", err)
	}
	return c, nil
}

// failCode логирует ошибку подключения; остановка по сигналу во время подключения — не ошибка.
func failCode(ctx context.Context, err error, what string) int {
	if ctx.Err() != nil {
		log.Info().Msg("остановлен по сигналу")
		return 0
	}
	e := log.Error().Err(err).Str("cluster", what)
	if isAuthFailed(err) {
		e = e.Str("hint", "проверьте пользователя и пароль")
	}
	e.Msg("не удалось подключиться")
	return 1
}

// isAuthFailed сообщает, что сервер отверг аутентификацию (AuthenticationFailed, код 18):
// Ping возвращает её как mongo.CommandError.
func isAuthFailed(err error) bool {
	var se mongo.ServerError
	return errors.As(err, &se) && se.HasErrorCode(18)
}

// maskURI заменяет пароль в URI на xxxxx. Разбор строковый, а не url.Parse:
// multi-host URI (h1:27017,h2:27017) net/url может не разобрать. Userinfo
// ищем до первого "/": драйвер допускает в пароле "?", но не "/".
func maskURI(uri string) string {
	i := strings.Index(uri, "://")
	if i < 0 {
		return uri
	}
	start := i + len("://")
	rest := uri[start:]
	end := strings.IndexByte(rest, '/')
	if end < 0 {
		end = len(rest)
	}
	at := strings.LastIndex(rest[:end], "@")
	if at < 0 {
		return uri
	}
	colon := strings.Index(rest[:at], ":")
	if colon < 0 {
		return uri
	}
	return uri[:start+colon+1] + "xxxxx" + uri[start+at:]
}

func setupLog(debug bool) {
	if debug {
		// В дебаге выводим плоские строки
		cw := zerolog.ConsoleWriter{Out: os.Stdout}
		log.Logger = zerolog.New(cw).With().Timestamp().Logger()
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
		return
	}

	// Иначе логируем в stdout в json'е; время нужно для оценки лага в долгоживущем процессе
	log.Logger = zerolog.New(os.Stdout).With().Timestamp().Logger()
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	zerolog.MessageFieldName = "m"
	zerolog.ErrorFieldName = "e"
	zerolog.LevelFieldName = "l"
	zerolog.TimestampFieldName = "t"
}
