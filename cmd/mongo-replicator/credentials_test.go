package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	mongoopts "go.mongodb.org/mongo-driver/v2/mongo/options"
)

// fakePrompter отдаёт заданный ответ и записывает приглашения.
type fakePrompter struct {
	terminal bool
	answers  []string // по одному на вызов ReadPassword
	err      error
	prompts  []string
}

func (f *fakePrompter) IsTerminal() bool { return f.terminal }

func (f *fakePrompter) ReadPassword(prompt string) (string, error) {
	f.prompts = append(f.prompts, prompt)
	if f.err != nil {
		return "", f.err
	}
	if len(f.answers) == 0 {
		return "", errors.New("неожиданный запрос пароля")
	}
	a := f.answers[0]
	f.answers = f.answers[1:]
	return a, nil
}

// clientOptions — разбор URI и поиск пароля одной стороны, как в prepareClients.
func clientOptions(s side, p prompter) (*mongoopts.ClientOptions, bool, error) {
	opts, err := parseURI(s)
	if err != nil {
		return nil, false, err
	}
	warn, err := resolvePassword(s, opts, p)
	if err != nil {
		return nil, false, err
	}
	return opts, warn, nil
}

func srcSide(uri string, fromFlag bool) side {
	return side{name: "источник", uri: uri, envName: "SRC_URI", fromFlag: fromFlag}
}

func TestFlagGiven(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want bool
	}{
		{"--src x", []string{"--src", "x", "--db", "a"}, true},
		{"--src=x", []string{"--dst", "d", "--src=x"}, true},
		{"нет аргументов (только env)", nil, false},
		{"другие опции", []string{"--dst", "d", "--db", "a"}, false},
		{"--srcfoo", []string{"--srcfoo", "x"}, false},
		{"значение другой опции через =", []string{"--db=--src"}, false},
		{"после --", []string{"--dst", "d", "--", "--src"}, false},
		{"одиночный дефис", []string{"-src", "x"}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, flagGiven(c.args, "src"), "--src передан")
		})
	}
}

func TestClientOptionsNoUser(t *testing.T) {
	p := &fakePrompter{terminal: true}

	opts, warn, err := clientOptions(srcSide("mongodb://h1:27017/?replicaSet=rs8", true), p)

	require.NoError(t, err)
	assert.Empty(t, p.prompts, "prompter не должен вызываться")
	assert.False(t, warn, "WARN о пароле во флаге")
	assert.Nil(t, opts.Auth, "без авторизации")
}

func TestClientOptionsNoPrompt(t *testing.T) {
	cases := []struct {
		name     string
		uri      string
		fromFlag bool
		user     string
		password string
		warn     bool
	}{
		{"X509", "mongodb://CN=client@h1/?authMechanism=MONGODB-X509&tls=true", false, "CN=client", "", false},
		{"x509 в нижнем регистре", "mongodb://CN=client@h1/?authMechanism=mongodb-x509&tls=true", false, "CN=client", "", false},
		{"пароль в URI флагом", "mongodb://repl:s3cret@h1:27017/?authSource=admin", true, "repl", "s3cret", true},
		{"пароль в URI из окружения", "mongodb://repl:s3cret@h1:27017/?authSource=admin", false, "repl", "s3cret", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := &fakePrompter{terminal: true}

			opts, warn, err := clientOptions(srcSide(c.uri, c.fromFlag), p)

			require.NoError(t, err)
			assert.Empty(t, p.prompts, "prompter не должен вызываться")
			assert.Equal(t, c.warn, warn, "WARN о пароле во флаге")
			require.NotNil(t, opts.Auth, "учётные данные из URI")
			assert.Equal(t, c.user, opts.Auth.Username, "пользователь")
			assert.Equal(t, c.password, opts.Auth.Password, "пароль")
		})
	}
}

func TestClientOptionsPrompt(t *testing.T) {
	cases := []struct {
		name     string
		uri      string
		fromFlag bool
	}{
		{"нет пароля в URI", "mongodb://repl@h1:27017,h2:27017/?replicaSet=rs8&authSource=admin", true},
		{"пустой пароль в URI", "mongodb://repl:@h1:27017,h2:27017/?authSource=admin", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := &fakePrompter{terminal: true, answers: []string{"t3rm-pass"}}

			opts, warn, err := clientOptions(srcSide(c.uri, c.fromFlag), p)

			require.NoError(t, err)
			assert.False(t, warn, "пароль из терминала — без WARN")
			require.Len(t, p.prompts, 1, "один запрос пароля")
			assert.Contains(t, p.prompts[0], "repl@h1:27017", "приглашение называет пользователя и хост")
			assert.Contains(t, p.prompts[0], "источник", "приглашение называет сторону")
			assert.NotContains(t, p.prompts[0], "t3rm-pass", "пароля нет в приглашении")
			require.NotNil(t, opts.Auth, "учётные данные из URI")
			assert.Equal(t, "repl", opts.Auth.Username, "пользователь")
			assert.Equal(t, "t3rm-pass", opts.Auth.Password, "пароль из терминала")
			assert.True(t, opts.Auth.PasswordSet, "пароль задан")
			assert.Equal(t, "admin", opts.Auth.AuthSource, "authSource из URI")
		})
	}
}

func TestClientOptionsPromptErrors(t *testing.T) {
	const uri = "mongodb://repl@h1:27017/"
	cases := []struct {
		name string
		s    side
		p    *fakePrompter
		want []string
	}{
		{"нет терминала, источник", srcSide(uri, false), &fakePrompter{}, []string{"нет терминала", "repl@h1:27017", "источник", "SRC_URI"}},
		{"нет терминала, цель", side{name: "цель", uri: uri, envName: "DST_URI"}, &fakePrompter{}, []string{"нет терминала", "цель", "DST_URI"}},
		{"пустой ввод", srcSide(uri, false), &fakePrompter{terminal: true, answers: []string{""}}, []string{"пустой пароль", "repl@h1:27017"}},
		{"Ctrl+D", srcSide(uri, false), &fakePrompter{terminal: true, err: io.EOF}, []string{"ввод пароля прерван"}},
		{"ошибка чтения", srcSide(uri, false), &fakePrompter{terminal: true, err: errors.New("ioctl failed")}, []string{"чтение пароля", "ioctl failed"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			opts, _, err := clientOptions(c.s, c.p)

			require.Error(t, err)
			assert.Nil(t, opts, "без опций при ошибке")
			for _, w := range c.want {
				assert.Contains(t, err.Error(), w, "подсказка оператору")
			}
		})
	}
}

func TestClientOptionsURIErrors(t *testing.T) {
	cases := []struct {
		name    string
		uri     string
		want    []string
		notWant []string
	}{
		{"SCRAM без пароля", "mongodb://repl@h1/?authMechanism=SCRAM-SHA-256",
			[]string{"источник", "некорректный URI", "password required", "уберите authMechanism"}, nil},
		{"PLAIN без пароля", "mongodb://repl@h1/?authSource=%24external&authMechanism=PLAIN",
			[]string{"источник", "некорректный URI", "SRC_URI"}, []string{"уберите authMechanism"}},
		{"неизвестный механизм", "mongodb://repl@h1/?authMechanism=BOGUS",
			[]string{"источник", "некорректный URI", "BOGUS"}, []string{"уберите authMechanism", "SRC_URI"}},
		{"кривой URI без пароля", "mongodb://h1/?replicaSet=rs8&connectTimeoutMS=abc",
			[]string{"источник", "некорректный URI", "connectTimeoutMS"}, []string{"authMechanism"}},
		{"кривой URI с паролем", "mongodb://repl:p%zzSecret@h1/",
			[]string{"источник", "некорректный URI", "mongodb://repl:xxxxx@h1/"}, []string{"zz", "Secret", "escape"}},
		{"@ и / в пароле, маскируется только начало", "mongodb://repl:prefix@x/Secret@h1/?connectTimeoutMS=bad",
			[]string{"источник", "некорректный URI", "не выводится"}, []string{"Secret", "prefix", "connectTimeoutMS"}},
		{"пароль ключа TLS в query", "mongodb://h1/?tlsCertificateKeyFilePassword=Secret&connectTimeoutMS=bad",
			[]string{"источник", "некорректный URI", "tlsCertificateKeyFilePassword=xxxxx&connectTimeoutMS=bad"}, []string{"Secret"}},
		{"? в пароле ключа TLS", "mongodb://h1/?tlsCertificateKeyFilePassword=alpha?Secret&connectTimeoutMS=bad",
			[]string{"источник", "некорректный URI", "tlsCertificateKeyFilePassword=xxxxx&connectTimeoutMS=bad"}, []string{"alpha", "Secret"}},
		{"кривой escape в пароле ключа TLS", "mongodb://h1/?tlsCertificateKeyFilePassword=p%zzSecret",
			[]string{"источник", "некорректный URI", "tlsCertificateKeyFilePassword=xxxxx"}, []string{"zz", "Secret", "escape"}},
		{"токен AWS в query", "mongodb://h1/?authMechanism=MONGODB-AWS&authMechanismProperties=AWS_SESSION_TOKEN:Secret&connectTimeoutMS=bad",
			[]string{"источник", "некорректный URI", "authMechanismProperties=xxxxx"}, []string{"Secret"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := &fakePrompter{terminal: true, answers: []string{"x"}}

			opts, _, err := clientOptions(srcSide(c.uri, true), p)

			require.Error(t, err)
			assert.Nil(t, opts, "без опций при ошибке")
			assert.Empty(t, p.prompts, "до запроса пароля")
			for _, w := range c.want {
				assert.Contains(t, err.Error(), w, "подсказка оператору")
			}
			for _, w := range c.notWant {
				assert.NotContains(t, err.Error(), w, "лишнее в тексте ошибки")
			}
		})
	}
}

// TestURIErrorDNS: ошибка DNS выводится с причиной и даже при пароле в URI, сам пароль — нет.
func TestURIErrorDNS(t *testing.T) {
	dnsErr := &net.DNSError{Err: "no such host", Name: "_mongodb._tcp.cluster.example.com", IsNotFound: true}
	wrapped := fmt.Errorf("error parsing uri: see https://pkg.go.dev/x: %w", dnsErr)
	cases := []struct {
		name    string
		uri     string
		want    []string
		notWant []string
	}{
		{"пароль маскируется", "mongodb+srv://repl:Secret@cluster.example.com/?authSource=admin",
			[]string{"источник", "mongodb+srv://repl:xxxxx@cluster.example.com/", "no such host", "_mongodb._tcp.cluster.example.com"},
			[]string{"Secret"}},
		{"@ и / в пароле, URI не выводится", "mongodb+srv://repl:pre@x/Secret@cluster.example.com/",
			[]string{"no such host"}, []string{"Secret", "pre"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := uriError(srcSide(c.uri, false), wrapped)

			require.ErrorIs(t, err, dnsErr, "ошибка DNS сохраняется")
			for _, w := range c.want {
				assert.Contains(t, err.Error(), w, "подсказка оператору")
			}
			for _, w := range c.notWant {
				assert.NotContains(t, err.Error(), w, "пароля нет в тексте ошибки")
			}
		})
	}
}

// TestClientOptionsNoPasswordLeak: пароль не встречается ни в приглашении, ни в ошибках.
func TestClientOptionsNoPasswordLeak(t *testing.T) {
	const pwd = "Leak3dP4ss"
	cases := []struct {
		name string
		uri  string
		p    *fakePrompter
	}{
		{"из терминала", "mongodb://repl@h1/", &fakePrompter{terminal: true, answers: []string{pwd}}},
		{"из URI", "mongodb://repl:" + pwd + "@h1/", &fakePrompter{terminal: true}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, _, err := clientOptions(srcSide(c.uri, true), c.p)

			require.NoError(t, err)
			assertNoPassword(t, c.p.prompts, pwd)
		})
	}
}

// TestClientOptionsNoPasswordLeakErrors: пароль не встречается ни в приглашении, ни в тексте ошибки.
func TestClientOptionsNoPasswordLeakErrors(t *testing.T) {
	const pwd = "Leak3dP4ss"
	cases := []struct {
		name string
		uri  string
		p    *fakePrompter
	}{
		{"кривой URI с паролем", "mongodb://repl:" + pwd + "%zz@h1/", &fakePrompter{terminal: true}},
		{"неэкранированный / в пароле", "mongodb://repl:" + pwd + "/x@h1/", &fakePrompter{terminal: true}},
		{"без ://", "mongodb:repl:" + pwd + "@h1/", &fakePrompter{terminal: true}},
		{":// после пароля", "mongodb:repl:" + pwd + "@h1://bad", &fakePrompter{terminal: true}},
		{":// после пароля и userinfo за ним", "mongodb:repl:" + pwd + "@h1://u:p@h2/", &fakePrompter{terminal: true}},
		{"пароль в URI и неверный механизм", "mongodb://repl:" + pwd + "@h1/?authMechanism=BOGUS", &fakePrompter{terminal: true}},
		{"ошибка чтения", "mongodb://repl@h1/", &fakePrompter{terminal: true, err: errors.New("read")}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, _, err := clientOptions(srcSide(c.uri, true), c.p)

			require.Error(t, err)
			assert.NotContains(t, err.Error(), pwd, "пароля нет в тексте ошибки")
			assert.NotContains(t, err.Error(), pwd[:4], "и части пароля")
			assertNoPassword(t, c.p.prompts, pwd)
		})
	}
}

// assertNoPassword проверяет, что пароля нет ни в одном приглашении.
func assertNoPassword(t *testing.T, prompts []string, pwd string) {
	t.Helper()
	for _, pr := range prompts {
		assert.NotContains(t, pr, pwd, "пароля нет в приглашении")
	}
}

func dstSide(uri string, fromFlag bool) side {
	return side{name: "цель", uri: uri, envName: "DST_URI", fromFlag: fromFlag}
}

// warnRecorder записывает вызовы onWarn вместе с числом запросов пароля на момент вызова.
type warnRecorder struct {
	p     *fakePrompter
	sides []string
	at    []int
}

func (w *warnRecorder) onWarn(name string) {
	w.sides = append(w.sides, name)
	w.at = append(w.at, len(w.p.prompts))
}

func TestPrepareClientsBothPrompted(t *testing.T) {
	// одинаковый пользователь на обеих сторонах — всё равно два запроса
	p := &fakePrompter{terminal: true, answers: []string{"src-pass", "dst-pass"}}
	w := &warnRecorder{p: p}

	srcOpts, dstOpts, err := prepareClients(
		srcSide("mongodb://repl@s1:27017/?authSource=admin", false),
		dstSide("mongodb://repl@d1:27017/?authSource=admin", false), p, w.onWarn)

	require.NoError(t, err)
	require.Len(t, p.prompts, 2, "запрос пароля на каждую сторону")
	assert.Contains(t, p.prompts[0], "repl@s1:27017", "первый запрос — хост источника")
	assert.Contains(t, p.prompts[0], "источник", "первый запрос — источник")
	assert.Contains(t, p.prompts[1], "repl@d1:27017", "второй запрос — хост цели")
	assert.Contains(t, p.prompts[1], "цель", "второй запрос — цель")
	assert.Equal(t, "src-pass", srcOpts.Auth.Password, "пароль источника")
	assert.Equal(t, "dst-pass", dstOpts.Auth.Password, "пароль цели")
	assert.Empty(t, w.sides, "пароли из терминала — без WARN")
}

func TestPrepareClientsMixed(t *testing.T) {
	// источник — пароль в URI из окружения, цель — из терминала
	p := &fakePrompter{terminal: true, answers: []string{"dst-pass"}}
	w := &warnRecorder{p: p}

	srcOpts, dstOpts, err := prepareClients(
		srcSide("mongodb://repl:src-pass@s1/", false),
		dstSide("mongodb://repl@d1/", false), p, w.onWarn)

	require.NoError(t, err)
	require.Len(t, p.prompts, 1, "запрос пароля только для цели")
	assert.Contains(t, p.prompts[0], "цель", "запрос — цель")
	assert.Equal(t, "src-pass", srcOpts.Auth.Password, "пароль источника из URI")
	assert.Equal(t, "dst-pass", dstOpts.Auth.Password, "пароль цели из терминала")
	assert.Empty(t, w.sides, "пароль из окружения — без WARN")
}

func TestPrepareClientsWarnBeforeDstPrompt(t *testing.T) {
	p := &fakePrompter{terminal: true, answers: []string{"dst-pass"}}
	w := &warnRecorder{p: p}

	_, dstOpts, err := prepareClients(
		srcSide("mongodb://repl:src-pass@s1/", true),
		dstSide("mongodb://repl@d1/", false), p, w.onWarn)

	require.NoError(t, err)
	assert.Equal(t, []string{"источник"}, w.sides, "WARN только для источника")
	assert.Equal(t, []int{0}, w.at, "WARN источника — до запроса пароля цели")
	assert.Equal(t, "dst-pass", dstOpts.Auth.Password, "пароль цели из терминала")
}

func TestPrepareClientsWarnBothSides(t *testing.T) {
	p := &fakePrompter{terminal: true}
	w := &warnRecorder{p: p}

	_, _, err := prepareClients(
		srcSide("mongodb://repl:a@s1/", true),
		dstSide("mongodb://repl:b@d1/", true), p, w.onWarn)

	require.NoError(t, err)
	assert.Equal(t, []string{"источник", "цель"}, w.sides, "WARN для обеих сторон")
	assert.Empty(t, p.prompts, "пароли из URI — без запроса")
}

func TestPrepareClientsErrors(t *testing.T) {
	t.Run("ошибка источника — цель не запрашивается", func(t *testing.T) {
		p := &fakePrompter{terminal: true, answers: []string{""}}
		w := &warnRecorder{p: p}

		srcOpts, dstOpts, err := prepareClients(
			srcSide("mongodb://repl@s1/", false),
			dstSide("mongodb://repl@d1/", false), p, w.onWarn)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "источник", "ошибка называет сторону")
		assert.Nil(t, srcOpts, "без опций источника")
		assert.Nil(t, dstOpts, "без опций цели")
		require.Len(t, p.prompts, 1, "один запрос пароля")
		assert.Contains(t, p.prompts[0], "источник", "запрос — источник")
	})
	t.Run("ошибка URI цели — до запроса пароля источника", func(t *testing.T) {
		p := &fakePrompter{terminal: true, answers: []string{"src-pass"}}
		w := &warnRecorder{p: p}

		srcOpts, dstOpts, err := prepareClients(
			srcSide("mongodb://repl:a@s1/", true),
			dstSide("mongodb://h1/?connectTimeoutMS=abc", false), p, w.onWarn)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "цель", "ошибка называет сторону")
		assert.Nil(t, srcOpts, "без опций источника")
		assert.Nil(t, dstOpts, "без опций цели")
		assert.Empty(t, p.prompts, "пароль не запрашивался")
		assert.Empty(t, w.sides, "WARN не выводился")
	})
	t.Run("ошибка пароля цели", func(t *testing.T) {
		p := &fakePrompter{terminal: true, answers: []string{"src-pass", ""}}
		w := &warnRecorder{p: p}

		srcOpts, dstOpts, err := prepareClients(
			srcSide("mongodb://repl@s1/", false),
			dstSide("mongodb://repl@d1/", false), p, w.onWarn)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "цель", "ошибка называет сторону")
		assert.Nil(t, srcOpts, "без опций источника")
		assert.Nil(t, dstOpts, "без опций цели")
		assert.Len(t, p.prompts, 2, "запрос пароля на каждую сторону")
	})
}

func TestMayHavePassword(t *testing.T) {
	cases := []struct {
		name string
		uri  string
		want bool
	}{
		{"/ в пароле", "mongodb://repl:a/b@h1/", true},
		{"пароль", "mongodb://repl:a@h1/", true},
		{"только пользователь", "mongodb://repl@h1:27017/", false},
		{"без пользователя", "mongodb://h1:27017/?replicaSet=rs8", false},
		{"без :// — проверяется вся строка", "mongodb:repl:a@h1/", true},
		{":// не в начале — не схема", "mongodb:repl:a@h1://bad", true},
		{":// в пароле", "mongodb://repl:a://b@h1/", true},
		{"без :// и без пользователя", "mongodb:h1/", false},
		{"не URI", "garbage", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, mayHavePassword(c.uri), "в URI может быть пароль")
		})
	}
}

func TestMechanism(t *testing.T) {
	cases := []struct{ name, uri, want string }{
		{"нижний регистр значения", "mongodb://u@h/?authMechanism=scram-sha-256&authSource=admin", "SCRAM-SHA-256"},
		{"верхний регистр ключа", "mongodb://u@h/?authSource=$external&AUTHMECHANISM=PLAIN", "PLAIN"},
		{"только authMechanismProperties", "mongodb://u@h/?authMechanismProperties=SERVICE_NAME:x", ""},
		{"не задан", "mongodb://u@h/", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, mechanism(c.uri), "authMechanism")
		})
	}
}
