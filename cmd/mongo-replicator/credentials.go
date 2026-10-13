package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"

	mongoopts "go.mongodb.org/mongo-driver/v2/mongo/options"
	"golang.org/x/term"
)

// side — одна из строк подключения: источник или цель.
type side struct {
	name     string // "источник" | "цель"
	uri      string
	envName  string // "SRC_URI" | "DST_URI" — для подсказок
	fromFlag bool   // URI передан флагом --src/--dst, а не из окружения
}

// prompter — ввод пароля в терминале; в тестах подменяется фейком.
type prompter interface {
	IsTerminal() bool
	ReadPassword(prompt string) (string, error)
}

// termPrompter читает пароль из терминала на stdin без эха.
type termPrompter struct{}

func stdinFd() int { return int(os.Stdin.Fd()) } //nolint:gosec // дескриптор stdin помещается в int

// IsTerminal требует терминал и на stderr: иначе приглашение уйдёт в лог
// (запуск с 2>log), а процесс молча будет ждать ввода.
func (termPrompter) IsTerminal() bool {
	return term.IsTerminal(stdinFd()) && term.IsTerminal(int(os.Stderr.Fd())) //nolint:gosec // дескриптор stderr помещается в int
}

// ReadPassword выводит приглашение в stderr и читает строку без эха.
// SIGINT/SIGTERM во время ввода по умолчанию убили бы процесс с выключенным
// эхом, поэтому перехватываем их: восстанавливаем терминал и выходим с 130.
func (termPrompter) ReadPassword(prompt string) (string, error) {
	fd := stdinFd()
	fmt.Fprint(os.Stderr, prompt)
	state, _ := term.GetState(fd)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	done := make(chan struct{})
	go func() {
		select {
		case <-sig:
			if state != nil {
				_ = term.Restore(fd, state)
			}
			fmt.Fprintln(os.Stderr)
			os.Exit(130)
		case <-done:
		}
	}()

	pwd, err := term.ReadPassword(fd)
	close(done)
	signal.Stop(sig)
	fmt.Fprintln(os.Stderr) // ввод без эха перевод строки не печатает
	return string(pwd), err
}

// parseURI разбирает URI стороны; ошибка — без пароля в тексте.
func parseURI(s side) (*mongoopts.ClientOptions, error) {
	opts := mongoopts.Client().ApplyURI(s.uri)
	if err := opts.Validate(); err != nil {
		return nil, uriError(s, err)
	}
	return opts, nil
}

// resolvePassword находит пароль стороны: из URI или из терминала, и кладёт его в opts.
// warn — пароль в URI, переданном флагом (вызывающий пишет WARN).
// Пароль не попадает ни в приглашение, ни в текст ошибок.
func resolvePassword(s side, opts *mongoopts.ClientOptions, p prompter) (warn bool, err error) {
	auth := opts.Auth
	if auth == nil || auth.Username == "" {
		return false, nil // без авторизации
	}
	if strings.EqualFold(auth.AuthMechanism, "MONGODB-X509") || strings.EqualFold(auth.AuthMechanism, "MONGODB-OIDC") {
		return false, nil // пароль не нужен
	}
	// пустой пароль в URI (user:@host) считаем незаданным
	if auth.PasswordSet && auth.Password != "" {
		return s.fromFlag, nil
	}

	who := auth.Username
	if len(opts.Hosts) > 0 {
		who += "@" + opts.Hosts[0]
	}
	if !p.IsTerminal() {
		return false, fmt.Errorf("нет пароля для %s (%s) и нет терминала: "+
			"запустите в терминале или передайте URI с паролем через окружение %s", who, s.name, s.envName)
	}
	pwd, err := p.ReadPassword(fmt.Sprintf("Пароль %s (%s): ", who, s.name))
	switch {
	case errors.Is(err, io.EOF):
		return false, fmt.Errorf("ввод пароля прерван (%s)", s.name)
	case err != nil:
		return false, fmt.Errorf("чтение пароля для %s (%s): %w", who, s.name, err)
	case pwd == "":
		return false, fmt.Errorf("пустой пароль для %s (%s)", who, s.name)
	}
	auth.Password, auth.PasswordSet = pwd, true
	return false, nil
}

// prepareClients разбирает оба URI, затем находит пароли по очереди: источник, цель.
// Ошибка любого URI (в т.ч. DNS для mongodb+srv) — до первого запроса пароля.
// onWarn вызывается сразу для стороны, чей пароль взят из URI, переданного флагом.
// Ошибки уже содержат имя стороны.
func prepareClients(src, dst side, p prompter, onWarn func(side string)) (srcOpts, dstOpts *mongoopts.ClientOptions, _ error) {
	srcOpts, err := parseURI(src)
	if err != nil {
		return nil, nil, err
	}
	if dstOpts, err = parseURI(dst); err != nil {
		return nil, nil, err
	}
	for _, c := range []struct {
		s    side
		opts *mongoopts.ClientOptions
	}{{src, srcOpts}, {dst, dstOpts}} {
		warn, err := resolvePassword(c.s, c.opts, p)
		if err != nil {
			return nil, nil, err
		}
		if warn {
			onWarn(c.s.name)
		}
	}
	return srcOpts, dstOpts, nil
}

// uriError — ошибка разбора URI. Если в URI может быть пароль или секретный параметр,
// текст драйвера не выводим: он может цитировать часть секрета (invalid URL escape "%zz").
func uriError(s side, err error) error {
	masked, ok := shownURI(s.uri)
	switch {
	case !ok:
		// неэкранированные "/" или "@" в пароле: пароль (или его хвост) остался бы в тексте
		return fmt.Errorf("%s: некорректный URI (не выводится: в нём может быть пароль); "+
			"символы / : @ в пароле экранируйте: %%2F %%3A %%40", s.name)
	case masked != s.uri:
		return fmt.Errorf("%s: некорректный URI %s", s.name, masked)
	}
	switch mechanism(s.uri) {
	case "SCRAM-SHA-1", "SCRAM-SHA-256":
		return fmt.Errorf("%s: некорректный URI %s: %w; уберите authMechanism из URI: "+
			"механизм по умолчанию сам выберет SCRAM, пароль будет запрошен", s.name, masked, err)
	case "PLAIN", "MONGODB-AWS":
		return fmt.Errorf("%s: некорректный URI %s: %w; для этого authMechanism пароль не запрашивается: "+
			"передайте URI с паролем через окружение %s", s.name, masked, err, s.envName)
	}
	return fmt.Errorf("%s: некорректный URI %s: %w", s.name, masked, err)
}

// mayHavePassword — грубая проверка для некорректного URI: есть ":" до последнего "@".
// Без схемы в начале (mongodb:user:pw@h, mongodb:user:pw@h://x) проверяется вся строка:
// лучше спрятать, чем вывести пароль. Ложное срабатывание ("@" в параметрах) лишь прячет текст ошибки.
func mayHavePassword(uri string) bool {
	if i := schemeEnd(uri); i >= 0 {
		uri = uri[i:]
	}
	at := strings.LastIndex(uri, "@")
	return at >= 0 && strings.Contains(uri[:at], ":")
}

// mechanism — значение authMechanism из URI в верхнем регистре, "" — не задан.
func mechanism(uri string) string {
	const key = "authmechanism="
	i := strings.Index(strings.ToLower(uri), key)
	if i < 0 {
		return ""
	}
	v, _, _ := strings.Cut(uri[i+len(key):], "&")
	return strings.ToUpper(v)
}

// flagGiven сообщает, передана ли длинная опция в командной строке: "--name" или "--name=...".
// Аргументы после "--" — позиционные. Значение другой опции, равное "--name",
// go-flags не пропустит ("expected argument"), поэтому разбор по арности опций не нужен.
func flagGiven(args []string, name string) bool {
	opt := "--" + name
	for _, a := range args {
		if a == "--" {
			return false
		}
		if a == opt || strings.HasPrefix(a, opt+"=") {
			return true
		}
	}
	return false
}
