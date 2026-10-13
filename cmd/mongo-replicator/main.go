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
	"os"
	"os/signal"
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
	Src       string `long:"src" env:"SRC_URI" description:"URI источника (replica set, MongoDB 6.0+)" required:"true"`
	Dst       string `long:"dst" env:"DST_URI" description:"URI цели" required:"true"`
	StartTS   uint32 `long:"start-ts" env:"START_TS" default:"0" description:"unix-сек, с которых читать поток при отсутствии чекпойнта (раньше начала полного дампа)"`
	BatchSize int    `long:"batch" env:"BATCH_SIZE" default:"1000" description:"максимум операций в одной пачке записи"`
	Debug     bool   `long:"dbg" env:"DEBUG" description:"Debug mode"`
}

var revision = "unknown"

const (
	appName      = "cs-replicator"
	pingTimeout  = 10 * time.Second
	retryTimeout = 5 * time.Second
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

	os.Exit(run(opts))
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
	if opts.BatchSize < 1 {
		return opts, fmt.Errorf("--batch должен быть >= 1, получено %d", opts.BatchSize)
	}
	return opts, nil
}

func isHelp(err error) bool {
	var flagsErr *flags.Error
	return errors.As(err, &flagsErr) && errors.Is(flagsErr.Type, flags.ErrHelp)
}

// run подключается к кластерам и крутит репликацию с ретраями. Возвращает код выхода.
func run(opts options) int {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	src, err := connect(ctx, mongoopts.Client().ApplyURI(opts.Src).SetAppName(appName))
	if err != nil {
		return failCode(ctx, err, "источник")
	}
	defer src.Disconnect(context.Background()) //nolint:errcheck // выходим, ошибка не важна

	// w:majority на цели: чекпойнт не должен обогнать данные при failover.
	dst, err := connect(ctx, mongoopts.Client().ApplyURI(opts.Dst).
		SetAppName(appName).
		SetWriteConcern(writeconcern.Majority()))
	if err != nil {
		return failCode(ctx, err, "цель")
	}
	defer dst.Disconnect(context.Background()) //nolint:errcheck // выходим, ошибка не важна

	r := replicator.New(src, dst, replicator.Config{StartTS: opts.StartTS, BatchSize: opts.BatchSize})
	for ctx.Err() == nil {
		err := r.Run(ctx)
		if ctx.Err() != nil {
			break
		}
		if replicator.IsHistoryLost(err) {
			log.Error().Err(err).Msg("точка возобновления выпала из oplog источника — нужна новая полная заливка цели")
			return 1
		}
		if errors.Is(err, replicator.ErrNoStartPoint) {
			log.Error().Err(err).Msg("задайте --start-ts")
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
	log.Error().Err(err).Str("cluster", what).Msg("не удалось подключиться")
	return 1
}

// maskURI заменяет пароль в URI на xxxxx. Разбор строковый, а не url.Parse:
// multi-host URI (h1:27017,h2:27017) net/url может не разобрать.
func maskURI(uri string) string {
	i := strings.Index(uri, "://")
	if i < 0 {
		return uri
	}
	start := i + len("://")
	rest := uri[start:]
	end := strings.IndexAny(rest, "/?")
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
