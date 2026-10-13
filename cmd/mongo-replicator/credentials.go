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

func (termPrompter) IsTerminal() bool { return term.IsTerminal(stdinFd()) }

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

// clientOptions разбирает URI стороны и находит пароль: из URI или из терминала.
// warn — пароль в URI, переданном флагом (вызывающий пишет WARN).
// Пароль не попадает ни в приглашение, ни в текст ошибок.
func clientOptions(s side, p prompter) (opts *mongoopts.ClientOptions, warn bool, err error) {
	opts = mongoopts.Client().ApplyURI(s.uri)
	if err = opts.Validate(); err != nil {
		return nil, false, uriError(s, err)
	}

	auth := opts.Auth
	if auth == nil || auth.Username == "" {
		return opts, false, nil // без авторизации
	}
	if strings.EqualFold(auth.AuthMechanism, "MONGODB-X509") || strings.EqualFold(auth.AuthMechanism, "MONGODB-OIDC") {
		return opts, false, nil // пароль не нужен
	}
	// пустой пароль в URI (user:@host) считаем незаданным
	if auth.PasswordSet && auth.Password != "" {
		return opts, s.fromFlag, nil
	}

	who := auth.Username
	if len(opts.Hosts) > 0 {
		who += "@" + opts.Hosts[0]
	}
	if !p.IsTerminal() {
		return nil, false, fmt.Errorf("нет пароля для %s (%s) и нет терминала: "+
			"запустите в терминале или передайте URI с паролем через окружение %s", who, s.name, s.envName)
	}
	pwd, err := p.ReadPassword(fmt.Sprintf("Пароль %s (%s): ", who, s.name))
	switch {
	case errors.Is(err, io.EOF):
		return nil, false, fmt.Errorf("ввод пароля прерван (%s)", s.name)
	case err != nil:
		return nil, false, fmt.Errorf("чтение пароля для %s (%s): %w", who, s.name, err)
	case pwd == "":
		return nil, false, fmt.Errorf("пустой пароль для %s (%s)", who, s.name)
	}
	auth.Password, auth.PasswordSet = pwd, true
	return opts, false, nil
}

// prepareClients готовит обе стороны по очереди: источник, затем цель.
// onWarn вызывается сразу для стороны, чей пароль взят из URI, переданного флагом.
// Ошибки clientOptions уже содержат имя стороны.
func prepareClients(src, dst side, p prompter, onWarn func(side string)) (srcOpts, dstOpts *mongoopts.ClientOptions, _ error) {
	prepare := func(s side) (*mongoopts.ClientOptions, error) {
		opts, warn, err := clientOptions(s, p)
		if err != nil {
			return nil, err
		}
		if warn {
			onWarn(s.name)
		}
		return opts, nil
	}
	srcOpts, err := prepare(src)
	if err != nil {
		return nil, nil, err
	}
	dstOpts, err = prepare(dst)
	if err != nil {
		return nil, nil, err
	}
	return srcOpts, dstOpts, nil
}

// uriError — ошибка разбора URI. Если в URI есть пароль, текст драйвера не
// выводим: он может цитировать часть пароля (invalid URL escape "%zz").
func uriError(s side, err error) error {
	masked := maskURI(s.uri)
	if masked != s.uri {
		return fmt.Errorf("%s: некорректный URI %s", s.name, masked)
	}
	if strings.Contains(strings.ToLower(s.uri), "authmechanism=") {
		return fmt.Errorf("%s: некорректный URI %s: %w; уберите authMechanism из URI: "+
			"механизм по умолчанию сам выберет SCRAM, пароль будет запрошен", s.name, masked, err)
	}
	return fmt.Errorf("%s: некорректный URI %s: %w", s.name, masked, err)
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
