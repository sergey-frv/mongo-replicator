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
	"errors"
	"fmt"
	"io"
	"math"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
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
	"golang.org/x/term"

	"github.com/sergey-frv/mongo-replicator/cmd/mongo-replicator/replicator"
	"github.com/sergey-frv/mongo-replicator/internal/statefile"
	"github.com/sergey-frv/mongo-replicator/internal/traffic"
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

var revision = "unknown"

const (
	appName        = "cs-replicator"
	pingTimeout    = 10 * time.Second
	retryTimeout   = 5 * time.Second
	releaseTimeout = 5 * time.Second // снятие аренды при выходе
	maxDBNameLen   = 63              // MongoDB: имя базы короче 64 байт
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
		var kill func()
		stopPanel, kill = withPanel(opts, r.Status, logFile)
		// после Release в порядке объявления, значит раньше него: паника в основной
		// горутине не оставит терминал под панелью
		defer func() {
			if rec := recover(); rec != nil {
				kill()
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
// kill — то же без финальной панели.
func withPanel(opts options, status func() replicator.Status, logFile *os.File) (stop, kill func()) {
	if logFile == nil {
		log.Warn().Msg("полный лог не пишется: добавьте --log-file")
	}
	file, fileLevel := fileWriter(logFile), logLevels[opts.LogFileLevel]
	console, stopP, killP := startPanel(status, os.Stdout)
	log.Logger = newLogger(console, true, max(zerolog.WarnLevel, logLevels[opts.LogLevel]), file, fileLevel)
	restore := func() {
		log.Logger = newLogger(os.Stdout, stdoutTTY(), logLevels[opts.LogLevel], file, fileLevel)
	}
	return func() { stopP(); restore() }, func() { killP(); restore() }
}

// logTraffic пишет итог трафика процесса (байты с начала) при выходе.
func logTraffic(tr *traffic.Counters, started time.Time) {
	t := tr.Snapshot()
	log.Info().Uint64("src_in", t.SrcIn).Uint64("src_out", t.SrcOut).
		Uint64("dst_in", t.DstIn).Uint64("dst_out", t.DstOut).
		Str("uptime", time.Since(started).Round(time.Second).String()).Msg("трафик")
}

// permanent сообщает, что ретраи ошибку не исправят и нужен оператор.
func permanent(err error) bool { return hint(err) != "" }

// hint — подсказка оператору по перманентной ошибке; "" — ошибка временная.
func hint(err error) string {
	var dropErr *replicator.DropError
	switch {
	case errors.As(err, &dropErr) && dropErr.Database:
		return "база удалена на источнике; на цели данные остались — нужна новая полная заливка: " +
			"удалите <dst-db>._replicator_checkpoint (или всю базу цели) и залейте заново с --start-ts"
	case errors.As(err, &dropErr):
		return fmt.Sprintf("на источнике удалена коллекция %s (вручную, перенос в другую базу или dropDatabase); "+
			"если удаление ожидаемое — перезапустите с --apply-drop %s", dropErr.NS, dropErr.NS)
	case replicator.IsEventTooLarge(err):
		return "событие изменения на источнике больше 16 МБ — change stream не может его отдать; " +
			"нужна новая полная заливка цели с --start-ts позже этого изменения"
	case replicator.IsHistoryLost(err):
		return "точка возобновления выпала из oplog источника — нужна новая полная заливка цели"
	case errors.Is(err, replicator.ErrNoStartPoint):
		return "задайте --start-ts"
	case errors.Is(err, replicator.ErrDBMismatch):
		return "чекпойнт этой базы цели записан для другой базы источника: верните прежний --db " +
			"или удалите <dst-db>._replicator_checkpoint (или всю базу цели) и залейте заново"
	case errors.Is(err, replicator.ErrLeaseHeld):
		return "другой процесс реплицирует эту базу; если он упал — аренда истечёт через ≤60 с"
	case errors.Is(err, replicator.ErrLeaseLost):
		return "аренду забрал другой процесс — проверьте, не запущены ли два репликатора на одну базу"
	case errors.Is(err, replicator.ErrForeignNamespace):
		return "в потоке событие другой базы — сообщите о баге"
	case errors.Is(err, replicator.ErrStreamInvalidated):
		return "поток закрыт сервером; на цели данные остались — нужна новая полная заливка: " +
			"удалите <dst-db>._replicator_checkpoint (или всю базу цели) и залейте заново с --start-ts"
	case errors.Is(err, replicator.ErrReservedCollection):
		return "на источнике коллекция переименована в служебное имя _replicator_checkpoint или из него: " +
			"переименуйте её на источнике в другое имя и сделайте новую полную заливку"
	case replicator.IsDBCaseConflict(err):
		return "на цели есть база с тем же именем в другом регистре — проверьте --dst-db"
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
	start := schemeEnd(uri)
	if start < 0 {
		return uri
	}
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

// schemeEnd — индекс после "<схема>://" в начале URI, -1 — схемы нет. "://" дальше
// в строке (mongodb:u:pw@h://x) схемой не считается: иначе пароль до него не ищется.
func schemeEnd(uri string) int {
	i := strings.Index(uri, "://")
	if i <= 0 {
		return -1
	}
	for _, r := range uri[:i] {
		switch {
		case 'a' <= r && r <= 'z', 'A' <= r && r <= 'Z', '0' <= r && r <= '9', strings.ContainsRune("+-.", r):
		default:
			return -1
		}
	}
	return i + len("://")
}

// secretOptions — параметры URI (ключи в нижнем регистре), значения которых секретны.
var secretOptions = map[string]bool{
	"tlscertificatekeyfilepassword":   true,
	"sslclientcertificatekeypassword": true,
	"authmechanismproperties":         true, // AWS_SESSION_TOKEN:...
}

// maskOptions заменяет на xxxxx значения секретных параметров URI. Драйвер берёт query
// после первого "?" и делит пары только по "&" и ";": "?" в значении — часть значения.
// Первый "?" query строкой не найти ("?" допустим и в имени пользователя), поэтому
// делим по "&" и ";", а ключ ищем в начале пары и после каждого "?" в ней (maskPair).
func maskOptions(uri string) string {
	var b strings.Builder
	for {
		i := strings.IndexAny(uri, "&;")
		if i < 0 {
			b.WriteString(maskPair(uri))
			return b.String()
		}
		b.WriteString(maskPair(uri[:i]))
		b.WriteByte(uri[i])
		uri = uri[i+1:]
	}
}

// maskPair маскирует до конца пары значение первого секретного ключа, найденного
// в её начале или после "?" (tlsCertificateKeyFilePassword=a?b — значение "a?b").
func maskPair(pair string) string {
	for start := 0; ; {
		if key, _, ok := strings.Cut(pair[start:], "="); ok && secretKey(key) {
			return pair[:start] + key + "=xxxxx"
		}
		q := strings.IndexByte(pair[start:], '?')
		if q < 0 {
			return pair
		}
		start += q + 1
	}
}

// secretKey — ключ секретного параметра. Сравниваем после unescape: драйвер принимает
// и экранированный ключ. Неразбираемый ключ считаем секретным.
func secretKey(key string) bool {
	k, err := url.QueryUnescape(key)
	return err != nil || secretOptions[strings.ToLower(k)]
}

// shownURI — URI для вывода без разбора драйвером: с замаскированными паролем и секретными параметрами.
// false — выводить нельзя: пароль некорректного URI maskURI не нашёл (неэкранированный "/")
// или нашёл не целиком (неэкранированные "@" и "/": mongodb://u:p@x/Secret@h/ — хвост "x/Secret").
// Лишний "@" после схемы корректному URI с паролем не нужен, ложное срабатывание лишь прячет URI.
func shownURI(uri string) (string, bool) {
	userMasked := maskURI(uri)
	masked := maskOptions(userMasked)
	if userMasked == uri {
		return masked, !mayHavePassword(uri)
	}
	return masked, strings.Count(userMasked[schemeEnd(userMasked):], "@") == 1
}

// logURI — URI для лога до его разбора: с замаскированными секретами, а если пароль
// может остаться в тексте (некорректный URI) — не выводит URI вовсе.
func logURI(uri string) string {
	if shown, ok := shownURI(uri); ok {
		return shown
	}
	return "(скрыт: в нём может быть пароль)"
}

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
func setupLog(opts options) (*os.File, error) {
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
