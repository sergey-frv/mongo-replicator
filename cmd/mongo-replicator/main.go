// mongo-replicator — односторонняя репликация MongoDB через change streams.
//
// Требования:
//   - источник: replica set, MongoDB 6.0+ (нужен showExpandedEvents для DDL);
//   - цель: в базу --dst-db пишет только репликатор (чекпойнт — в её коллекции
//     _replicator_checkpoint);
//   - на цели уже восстановлен полный дамп источника, а --start-ts указывает
//     на момент ДО начала этого дампа и ещё попадает в окно oplog источника.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/v2/mongo"
	mongoopts "go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
	"golang.org/x/term"

	"github.com/sergey-frv/mongo-replicator/internal/panel"
	"github.com/sergey-frv/mongo-replicator/internal/replicator"
	"github.com/sergey-frv/mongo-replicator/internal/statefile"
	"github.com/sergey-frv/mongo-replicator/internal/traffic"
)

var revision = "unknown"

const (
	appName        = "cs-replicator"
	pingTimeout    = 10 * time.Second
	retryTimeout   = 5 * time.Second
	releaseTimeout = 5 * time.Second // снятие аренды при выходе
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

	logFile, err := setupLog(opts)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	log.Info().Str("revision", revision).Send()
	if opts.StateDir, err = stateDir(opts.StateDir); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if len(opts.ApplyDrops) > 0 {
		log.Warn().Strs("apply_drop", opts.ApplyDrops).
			Msgf("разрешены drop: %s (уберите из APPLY_DROP после применения)", strings.Join(opts.ApplyDrops, ", "))
	}
	masked := opts
	masked.Src, masked.Dst = logURI(opts.Src), logURI(opts.Dst)
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

	os.Exit(run(opts, srcOpts, dstOpts, logFile))
}

// run подключается к кластерам и крутит репликацию с ретраями. Возвращает код выхода.
// srcOpts/dstOpts — разобранные URI с найденными паролями (prepareClients);
// logFile — файл лога из setupLog (nil без --log-file): с ним логгер пересобирается под панель.
func run(opts options, srcOpts, dstOpts *mongoopts.ClientOptions, logFile *os.File) int {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// диалеры — до подключения: иначе пул и мониторинг пойдут мимо счётчиков
	tr := &traffic.Counters{}
	srcOpts.SetDialer(tr.SrcDialer())
	dstOpts.SetDialer(tr.DstDialer())
	// первым, чтобы сработать последним: после снятия аренды и отключения клиентов
	started := time.Now()
	defer logTraffic(tr, started)

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

	stateFile, cluster := statefile.Path(opts.StateDir, opts.DstDB, opts.Dst)
	log.Info().Str("file", stateFile).Msg("файл состояния")
	r := replicator.New(src, dst, replicator.Config{
		StartTS: opts.StartTS, BatchSize: opts.BatchSize, DB: opts.DB, DstDB: opts.DstDB,
		ApplyDrops: opts.ApplyDrops, Traffic: tr,
		StateFile: stateFile, StateDir: opts.StateDir, Cluster: cluster,
	})
	// на любом выходе, включая return 1 и панику: иначе база цели занята до истечения аренды
	defer func() {
		relCtx, cancel := context.WithTimeout(context.Background(), releaseTimeout)
		defer cancel()
		if relErr := r.Release(relCtx); relErr != nil {
			log.Warn().Err(relErr).Msg("аренда не снята: база цели освободится по её истечении")
		}
	}()
	stopPanel := func() {}
	if progressMode(opts.Progress, stdoutTTY(), os.Getenv("TERM")) == progressTTY {
		in, closeIn := panelInput(os.Stdin, openTTY, foreground)
		stopP, killP := withPanel(opts, r.Status, logFile, in)
		stopPanel = func() { stopP(); closeIn() }
		// после Release в порядке объявления, значит раньше него: паника в основной
		// горутине не оставит терминал под панелью
		defer func() {
			if rec := recover(); rec != nil {
				killP()
				closeIn()
				panic(rec)
			}
		}()
	}
	err = replicate(ctx, r)
	// итоги — обычной печатью под финальной панелью
	stopPanel()
	if err != nil {
		log.Error().Err(err).Msg(hint(err))
		return 1
	}
	log.Info().Int64("applied", r.Applied()).
		Msg("остановлен по сигналу; незаписанный хвост будет перечитан при следующем запуске")
	return 0
}

// replicate крутит Run с повторами временных ошибок. Возвращает перманентную ошибку
// или nil — остановку по сигналу.
func replicate(ctx context.Context, r *replicator.Replicator) error {
	for ctx.Err() == nil {
		err := r.Run(ctx)
		if ctx.Err() != nil {
			break
		}
		if permanent(err) {
			return err
		}
		log.Warn().Err(err).Str("retry", retryTimeout.String()).Msg("ошибка репликации, повтор")
		r.SetRetry(err)
		select {
		case <-ctx.Done():
		case <-time.After(retryTimeout):
		}
	}
	return nil
}

// withPanel показывает панель статуса и переключает на неё консольный лог: над панелью —
// WARN и выше, файл лога — без изменений. stop выводит финальную панель, дожидается
// её выхода и возвращает обычный консольный лог (иначе итог INFO отфильтруется);
// kill — то же без финальной панели. in — терминал для клавиш панели (panelInput)
// или nil: тогда панель клавиатуру не читает.
func withPanel(opts options, status func() replicator.Status, logFile, in *os.File) (stop, kill func()) {
	if logFile == nil {
		log.Warn().Msg("полный лог не пишется: добавьте --log-file")
	}
	file, fileLevel := fileWriter(logFile), logLevels[opts.LogFileLevel]
	console, stopP, killP := panel.Start(status, os.Stdout, in, signalSelf)
	log.Logger = newLogger(console, true, max(zerolog.WarnLevel, logLevels[opts.LogLevel]), file, fileLevel)
	restore := func() {
		log.Logger = newLogger(os.Stdout, stdoutTTY(), logLevels[opts.LogLevel], file, fileLevel)
	}
	return func() { stopP(); restore() }, func() { killP(); restore() }
}

// panelInput выбирает терминал, с которого панель читает клавиши: stdin, если он
// терминал, иначе /dev/tty (запуск с < /dev/null в терминале). Терминала нет или
// процесс не на переднем плане (isForeground; запуск с &) — nil, панель без клавиш.
// closeIn закрывает открытый здесь /dev/tty, stdin не трогает; вызывать после выхода панели.
func panelInput(stdin *os.File, openTTY func() (*os.File, error),
	isForeground func(*os.File) bool,
) (in *os.File, closeIn func()) {
	noop := func() {}
	in, closeIn = stdin, noop
	if !term.IsTerminal(int(stdin.Fd())) { //nolint:gosec // дескриптор stdin помещается в int
		tty, err := openTTY()
		if err != nil {
			return nil, noop
		}
		in, closeIn = tty, func() { _ = tty.Close() }
	}
	if !isForeground(in) {
		closeIn()
		return nil, noop
	}
	return in, closeIn
}

// openTTY открывает управляющий терминал процесса. Не tea.OpenTTY: тот возвращает
// один файл и как ввод, и как вывод.
func openTTY() (*os.File, error) {
	return os.OpenFile("/dev/tty", os.O_RDWR, 0)
}

// signalSelf отправляет сигнал своему процессу: в raw-режиме панели терминал не
// превращает Ctrl+C и Ctrl+\ в сигналы, их шлёт панель.
func signalSelf(sig os.Signal) {
	if p, err := os.FindProcess(os.Getpid()); err == nil {
		_ = p.Signal(sig)
	}
}

// logTraffic пишет итог трафика процесса (байты с начала) при выходе.
func logTraffic(tr *traffic.Counters, started time.Time) {
	t := tr.Snapshot()
	log.Info().Uint64("src_in", t.SrcIn).Uint64("src_out", t.SrcOut).
		Uint64("dst_in", t.DstIn).Uint64("dst_out", t.DstOut).
		Str("uptime", time.Since(started).Round(time.Second).String()).Msg("трафик")
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
