package main

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog/log"
	mongoopts "go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/sergey-frv/mongo-replicator/internal/replicator"
	"github.com/sergey-frv/mongo-replicator/internal/verify"
)

// verifyAppName — appName клиентов сверки: в currentOp и логах серверов сверку
// видно отдельно от репликатора.
const verifyAppName = "mongo-replicator-verify"

// mainVerify — main формы сверки; args — без ведущего verify. Не возвращается.
func mainVerify(args []string) {
	opts, err := parseVerifyOpts(args)
	exitOnOptsErr(err)

	// панели у сверки нет: файл лога нужен только setupLog
	startLog(&opts.commonOptions)
	masked := opts
	masked.Src, masked.Dst = logURI(opts.Src), logURI(opts.Dst)
	log.Debug().Any("options", masked).Send()

	srcOpts, dstOpts := clientOpts(opts.commonOptions, args)
	os.Exit(runVerify(opts, srcOpts, dstOpts))
}

// runVerify подключается к кластерам и сверяет цель с источником. Возвращает код
// выхода (verifyCode). srcOpts/dstOpts — разобранные URI с найденными паролями
// (prepareClients). SIGINT/SIGTERM — штатное завершение: позиция и отчёты
// записываются, код — по итогу; сигнал до итога (подключение, схема на старте) —
// код 1: сверки не было, а 0 значит «дефектов нет».
func runVerify(opts verifyOptions, srcOpts, dstOpts *mongoopts.ClientOptions) int {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// чтения сверки сами задают primary и majority: read preference из URI не мешает
	src, err := connect(ctx, srcOpts.SetAppName(verifyAppName))
	if err != nil {
		failCode(ctx, err, "источник")
		return 1
	}
	defer src.Disconnect(context.Background()) //nolint:errcheck // выходим, ошибка не важна

	dst, err := connect(ctx, dstOpts.SetAppName(verifyAppName))
	if err != nil {
		failCode(ctx, err, "цель")
		return 1
	}
	defer dst.Disconnect(context.Background()) //nolint:errcheck // выходим, ошибка не важна

	rep, err := verify.New(src, dst, verify.Config{
		DB: opts.DB, DstDB: opts.DstDB, Since: opts.Since, Rate: opts.Rate, Sample: opts.Sample,
		UntilCaughtUp: opts.UntilCaughtUp, StateDir: opts.StateDir, SrcURI: opts.Src, DstURI: opts.Dst,
		IDs: opts.IDs, SchemaOnly: opts.SchemaOnly,
	}).Run(ctx)
	return verifyCode(ctx, rep, err)
}

// verifyCode логирует ошибку сверки с подсказкой и возвращает код выхода: 0 —
// дефектов нет («не проверено» допускается), 1 — дефекты или ошибка выполнения,
// 2 — сверку нельзя начать (нет --since и позиции, файл позиции занят или
// записан для другой сверки). Остановка по сигналу после начала цикла сверки
// приходит без ошибки (код по итогу); ошибка после сигнала — остановка до итога,
// код 1. Итог и дефекты логирует сам пакет verify.
func verifyCode(ctx context.Context, rep verify.Report, err error) int {
	if err != nil && ctx.Err() != nil {
		log.Info().Msg("сверка остановлена по сигналу до итога")
		return 1
	}
	switch {
	case err == nil && rep.Clean():
		return 0
	case err == nil:
		return 1
	}
	e := log.Error().Err(err)
	if h := verifyHint(err); h != "" {
		e = e.Str("hint", h)
	}
	e.Msg("сверка прервана")
	if errors.Is(err, verify.ErrNoSince) || errors.Is(err, verify.ErrLocked) || errors.Is(err, verify.ErrPositionMismatch) {
		return 2
	}
	return 1
}

// verifyHint — подсказка оператору по ошибке сверки; "" — подсказки нет.
func verifyHint(err error) string {
	switch {
	case errors.Is(err, verify.ErrNoSince):
		return "задайте --since — unix-сек, с которых сверять изменённые документы; " +
			"файл позиции прошлого запуска ищется в подкаталоге verify/ каталога --state-dir"
	case errors.Is(err, verify.ErrPositionMismatch):
		return "--db и URI должны совпадать с прошлым запуском verify на эту базу цели; " +
			"чтобы начать сверку заново, задайте --since — файл позиции и отчёты будут перенесены в сторону"
	case errors.Is(err, verify.ErrLocked):
		return "эту базу цели уже сверяет другой процесс verify: дождитесь его или остановите (Ctrl+C)"
	case errors.Is(err, verify.ErrHistoryLost):
		return "сверка стояла или отставала дольше окна oplog источника: удалите файл позиции " +
			"и начните с более позднего --since; если сверка не успевает за изменениями — уменьшите --sample"
	case errors.Is(err, replicator.ErrDBMismatch):
		return "чекпойнт на цели записан для другой пары баз: --db и --dst-db сверки должны совпадать " +
			"с опциями репликатора"
	}
	return ""
}
