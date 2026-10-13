# Пароли к MongoDB вне connection string

## Overview
- сейчас пароль передаётся только внутри URI (`--src`/`SRC_URI`, `--dst`/`DST_URI`): при запуске с флагами он остаётся в истории шелла и виден всем пользователям машины в `ps` / `/proc/<pid>/cmdline`; README учит `--src "mongodb://user:pass@..."`
- утилита запускается вручную в терминале (ssh на bastion, `tmux`); сервисом — пока нет
- решение: имя пользователя остаётся в URI, пароль запрашивается в терминале без эха — для каждой из двух строк подключения независимо
- отдельных env для паролей нет (решение пользователя на ревью плана): для запуска без терминала полная строка подключения с паролем передаётся через окружение (`SRC_URI`/`DST_URI`) — так она не видна в `ps`
- пароль в URI по-прежнему принимается; WARN — только если такой URI передан **флагом** `--src`/`--dst` (тогда он в истории шелла и `ps`); из окружения — без WARN
- пароль подставляется в уже разобранные `ClientOptions` драйвера — URI не пересобирается, пароль не может попасть в лог через URI

## Context (from discovery)
- `cmd/mongo-replicator/main.go`:
  - `options`: `Src` (`--src`/`SRC_URI`), `Dst` (`--dst`/`DST_URI`), `StartTS`, `BatchSize`, `Debug`, `DB`, `AllDBs`
  - `main`: `parseOpts` → `setupLog` → debug-дамп опций с `maskURI` (только URI) → `os.Exit(run(opts))`
  - `run` (`:126`): `signal.NotifyContext`; `connect(ctx, mongoopts.Client().ApplyURI(opts.Src).SetAppName(appName))`; цель — то же + `SetWriteConcern(writeconcern.Majority())`; ошибка → `failCode(ctx, err, "источник"|"цель")` (exit 1, `:206`)
  - `connect` (`:191`): `mongo.Connect` + `Ping` с таймаутом
  - `hint`/`permanent` (`:169-187`) — только для ошибок `Run`; подсказку про пароль туда **не** добавлять
- `cmd/mongo-replicator/main_test.go`: тесты `parseOpts` (`clearEnv`), `maskURI`, `permanent`
- драйвер v2.9.2 (проверено ревью плана):
  - `ApplyURI` → `connstring.ParseAndValidate` заполняет `Auth` (`Username`, `Password`, `PasswordSet`, `AuthMechanism`, регистр механизма сохраняется); ошибка разбора «липкая», её возвращает `ClientOptions.Validate()` (`options/clientoptions.go:576`)
  - `validateAuth` (`connstring.go:731-806`) **отвергает** URI с пользователем без пароля при явном `authMechanism=SCRAM-SHA-1|SCRAM-SHA-256|PLAIN` (и `MONGODB-AWS` с пользователем без пароля); механизм по умолчанию (`""`) с пользователем без пароля проходит — драйвер сам договаривается о SCRAM
  - разбор `mongodb+srv` делает DNS-запрос внутри `ApplyURI`
  - `mongo.Connect` берёт аутентификацию из `clientOpts.Auth` (`mongo/client.go:224-231`)
  - неверный пароль: `Ping` → `wrapErrors` (`mongo/client.go:439`) → `mongo.CommandError{Code: 18}`, оборачивающий `ConnectionError{auth.Error{driver.Error{Code: 18}}}`
- `golang.org/x/term` v0.46.0: `ReadPassword` снимает ECHO, оставляя `ISIG` — Ctrl+C шлёт SIGINT при выключенном эхе; собственный `defer` восстановления не выполняется, т.к. SIGINT по умолчанию убивает процесс. Требует `golang.org/x/sys` v0.48.0 (сейчас v0.46.0 indirect) — vendor обновится заметно
- `.golangci.yml`: включён gosec (G115 на `int(os.Stdin.Fd())`)
- команды: `make test` (docker, `--no-deps`, сборка из `vendor/`), `make lint`, `make build`

## Development Approach
- **testing approach**: Regular (код, затем тесты в той же задаче)
- complete each task fully before moving to the next
- make small, focused changes
- **CRITICAL: every task MUST include new/updated tests** for code changes in that task
  - tests are not optional - they are a required part of the checklist
  - write unit tests for new functions/methods
  - write unit tests for modified functions/methods
  - add new test cases for new code paths
  - update existing test cases if behavior changes
  - tests cover both success and error scenarios
- **CRITICAL: all tests must pass before starting next task** - no exceptions
- **CRITICAL: update this plan file when scope changes during implementation**
- run `make test` and `make lint` after each task
- совместимость: URI с паролем продолжают работать (с WARN); URI без пользователя (docker-compose, `make run`) — без изменений и без запроса пароля

## Testing Strategy
- **unit tests**: `make test` — логика выбора пароля с подменённым `prompter`; источник URI (флаг/окружение); две стороны; распознавание ошибки аутентификации
- интеграционные тесты не меняются: создают клиентов сами, MongoDB в compose без авторизации
- не автоматизируется (Post-Completion): `termPrompter` (TTY, Ctrl+C/SIGTERM и восстановление эха); подключение с паролем к серверу с авторизацией

## Progress Tracking
- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix
- update plan if implementation deviates from original scope
- keep plan in sync with actual work done

## Solution Overview
- новый файл `cmd/mongo-replicator/credentials.go`: разбор URI в `ClientOptions` и поиск пароля для одной стороны; подготовка обеих сторон; боевой `termPrompter`
- `main` готовит `ClientOptions` источника и цели **до** `run` (после `setupLog`, чтобы WARN шёл в лог) — оба запроса пароля происходят в самом начале, до многочасовой работы
- `run` получает готовые `ClientOptions` вместо URI и больше не вызывает `ApplyURI`
- терминал скрыт за интерфейсом `prompter`: боевая реализация на `golang.org/x/term`, в тестах — фейк
- `authMechanism` с SCRAM/PLAIN в URI без пароля не поддерживается: драйвер отвергает такой URI до поиска пароля; механизм по умолчанию сам выбирает SCRAM — понятная ошибка exit 2 и строка в README (решение по ревью плана: проще, чем вырезать параметр из URI)

## Technical Details

**Типы**
```go
type side struct {
    name     string // "источник" | "цель"
    uri      string
    envName  string // "SRC_URI" | "DST_URI" — для подсказок
    fromFlag bool   // URI передан флагом --src/--dst, а не из окружения
}

type prompter interface {
    IsTerminal() bool
    ReadPassword(prompt string) (string, error)
}

// clientOptions разбирает URI стороны и находит пароль.
// warn — пароль в URI, переданном флагом (вызывающий пишет WARN).
func clientOptions(s side, p prompter) (opts *mongoopts.ClientOptions, warn bool, err error)

// flagGiven сообщает, передана ли длинная опция в командной строке: "--name" или "--name=...".
func flagGiven(args []string, name string) bool

// prepareClients — обе стороны по очереди: источник, затем цель.
// onWarn вызывается сразу для стороны, чей пароль взят из URI.
func prepareClients(src, dst side, p prompter, onWarn func(side string)) (srcOpts, dstOpts *mongoopts.ClientOptions, err error)
```

**Порядок в `clientOptions`** (для каждой стороны независимо)
1. `opts := mongoopts.Client().ApplyURI(s.uri)`; `opts.Validate()` → ошибка (exit 2):
   - текст: сторона + `maskURI(s.uri)` + ошибка драйвера; если в URI есть пароль (`maskURI(s.uri) != s.uri`) — текст драйвера не выводится (он может цитировать часть пароля, напр. `invalid URL escape "%zz"`), только «некорректный URI»
   - если в URI есть `authMechanism` и нет пароля — подсказка: «уберите authMechanism из URI: механизм по умолчанию сам выберет SCRAM, пароль будет запрошен»
   - для `mongodb+srv` сюда же попадает ошибка DNS — до запроса паролей
2. `opts.Auth == nil || opts.Auth.Username == ""` → вернуть как есть (нет авторизации: docker-compose)
3. `AuthMechanism` (через `strings.EqualFold`) ∈ {`MONGODB-X509`, `MONGODB-OIDC`} → вернуть как есть (пароль не нужен). `MONGODB-AWS` с пользователем без пароля драйвер отвергает на шаге 1, `GSSAPI` без build-тега не собран — их не перечисляем
4. `opts.Auth.PasswordSet && opts.Auth.Password != ""` → вернуть, `warn = s.fromFlag`. Пустой пароль в URI (`user:@host`) считается «не задан» — идём дальше
5. `p.IsTerminal()` → `p.ReadPassword("Пароль <user>@<первый host> (<сторона>): ")`; пустой ввод → ошибка `пустой пароль для <user>@<host> (<сторона>)`; `io.EOF` (Ctrl+D) → `ввод пароля прерван`; прочие ошибки чтения — обёрнутые
6. иначе → ошибка `нет пароля для <user>@<host> (<сторона>) и нет терминала: запустите в терминале или передайте URI с паролем через окружение <ENV>`
7. найденный пароль → `opts.Auth.Password = pwd; opts.Auth.PasswordSet = true`

- пароль одной стороны **не** переиспользуется для другой, даже при одинаковом имени пользователя
- пароль никогда не попадает в текст приглашения, ошибок и логов
- полей для паролей в `options` **нет** — иначе они попали бы в debug-дамп опций и `--help`. В `description` для `--src`/`--dst` — одна фраза: «без пароля в URI он будет запрошен в терминале; URI с паролем передавайте через SRC_URI, не флагом»
- `fromFlag`: `flagGiven(os.Args[1:], "src")` / `"dst"`; go-flags даёт флагу приоритет над env, так что при переданном флаге URI точно из командной строки

**Боевой `prompter`** (`termPrompter`)
- `IsTerminal`: `term.IsTerminal(fd)`, `fd := int(os.Stdin.Fd()) //nolint:gosec // дескриптор stdin помещается в int`
- `ReadPassword`:
  - приглашение в `os.Stderr`; `state, _ := term.GetState(fd)`
  - `sig := make(chan os.Signal, 1)`; `signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)`; `done := make(chan struct{})`
  - горутина: `select { case <-sig: term.Restore(fd, state); перевод строки в stderr; os.Exit(130); case <-done: }`
  - `term.ReadPassword(fd)`; затем `close(done)`, `signal.Stop(sig)`, перевод строки в stderr (ввод без эха его не печатает)
  - `signal.Stop` до `signal.NotifyContext` в `run`: SIGINT в промежутке — действие по умолчанию, терминал к этому моменту уже восстановлен

**main / run**
- `main`: после `setupLog` и debug-дампа — `prepareClients(...)` с `termPrompter` и `onWarn` → `log.Warn().Str("cluster", name).Msg("пароль передан в --src/--dst: он остаётся в истории шелла и виден в ps; передавайте URI с паролем через SRC_URI/DST_URI или без пароля")`; ошибка → stderr + exit 2
- `run(opts, srcOpts, dstOpts)`: `connect(ctx, srcOpts.SetAppName(appName))`, цель — `dstOpts.SetAppName(appName).SetWriteConcern(writeconcern.Majority())`
- изменение поведения: ошибки разбора URI (включая DNS для `mongodb+srv`) теперь exit 2 и до запроса паролей, раньше — exit 1 на подключении

**Ошибка аутентификации**
- `isAuthFailed(err error) bool`: `var se mongo.ServerError; errors.As(err, &se) && se.HasErrorCode(18)` — надёжно через `fmt.Errorf("ping: %w")`, т.к. `Ping` возвращает `mongo.CommandError{Code: 18}`
- `failCode`: при `isAuthFailed` — `.Str("hint", "проверьте пользователя и пароль")` в логе ошибки подключения
- клиентские сбои SCRAM (несовпадение подписи сервера, 334 MechanismUnavailable) подсказку не получают — приемлемо, `x/mongo/driver/auth` ради этого не импортируем

**Чего не делаем**
- отдельные env для паролей (`SRC_PASSWORD`/`DST_PASSWORD`): для запуска без терминала достаточно URI с паролем в `SRC_URI`/`DST_URI` (решение пользователя)
- `--*-password-file`: YAGNI, добавить при переходе к запуску сервисом
- повторный запрос при пустом вводе: проще перезапустить
- вырезание `authMechanism` из URI ради SCRAM без пароля: заменено понятной ошибкой

## What Goes Where
- **Implementation Steps**: код, юнит-тесты, README
- **Post-Completion**: ручные проверки в терминале и на реальных кластерах

## Implementation Steps

### Task 1: Выбор пароля для одной стороны

**Files:**
- Create: `cmd/mongo-replicator/credentials.go`
- Create: `cmd/mongo-replicator/credentials_test.go`

- [x] `side`, `prompter`, `clientOptions` (порядок 1–7 из Technical Details), `flagGiven`
- [x] write tests `flagGiven`: `--src x`, `--src=x` → true; только env / `--srcfoo` / значение другой опции, равное `--src`, после `--` → разумное поведение зафиксировать тестом
- [x] write tests (fake `prompter` с записью вызовов): нет пользователя → prompter не вызван, `Auth` не тронут; `authMechanism=MONGODB-X509` (и `mongodb-x509`) → без запроса; пароль в URI и `fromFlag` → используется, `warn`, prompter не вызван; пароль в URI из окружения (`fromFlag=false`) → используется, без `warn`; `user:@host` (пустой пароль в URI) → без `warn`, запрос в терминале
- [x] write tests: терминал → приглашение содержит `user@host` и сторону, `Password`/`PasswordSet` выставлены; нет терминала → ошибка с именем `SRC_URI`/`DST_URI` и подсказкой; пустой ввод → ошибка; `io.EOF` → «ввод пароля прерван»; прочая ошибка `ReadPassword` → ошибка
- [x] write tests разбора URI: `?authMechanism=SCRAM-SHA-256` без пароля → exit-2-ошибка с подсказкой убрать `authMechanism`; кривой URI без пароля → текст драйвера есть; кривой URI с паролем (`user:p%zz@h`) → текста драйвера нет, пароля нет
- [x] write test-проверку во всех случаях: пароль не встречается в тексте приглашения и ошибок
- [x] `make test` и `make lint` — должно пройти

### Task 2: Две стороны

**Files:**
- Modify: `cmd/mongo-replicator/credentials.go`
- Modify: `cmd/mongo-replicator/credentials_test.go`

- [x] `prepareClients(src, dst, p, onWarn)`: источник, затем цель; `onWarn` сразу после стороны с паролем из URI; ошибка любой стороны — с именем стороны
- [x] write tests: два запроса в порядке источник → цель, каждый пароль в своих `ClientOptions`; смешанный (источник с паролем в URI из окружения, цель из терминала); одинаковый пользователь на обеих сторонах → всё равно два запроса и разные пароли; ошибка источника → цель не запрашивается; `onWarn` вызывается для источника до запроса пароля цели
- [x] `make test` и `make lint` — должно пройти

### Task 3: `termPrompter`, зависимость и подключение в main

**Files:**
- Modify: `cmd/mongo-replicator/credentials.go`
- Modify: `cmd/mongo-replicator/main.go`
- Modify: `cmd/mongo-replicator/main_test.go`
- Modify: `go.mod`, `go.sum`, `vendor/`

- [x] `termPrompter` (Technical Details: `done`-канал, SIGINT+SIGTERM, `term.Restore`, exit 130, `//nolint:gosec` на `int(fd)`) — сначала код с импортом `golang.org/x/term`
- [x] затем на хосте: `go get golang.org/x/term && go mod tidy && go mod vendor` (x/term станет прямой зависимостью, x/sys обновится до v0.48.0 — большой diff в `vendor/` ожидаем); в обратном порядке `tidy` выкинет неиспользуемый модуль и `make test` (сборка из `vendor/`) не соберётся
- [x] `main`: `prepareClients` после `setupLog`; ошибка → exit 2; `run(opts, srcOpts, dstOpts)`, `ApplyURI` из `run` убран; `fromFlag` через `flagGiven`; описания `--src`/`--dst` дополнены фразой про запрос пароля и `SRC_URI`/`DST_URI`
- [x] write/update tests в `main_test.go`, если затронуты сигнатуры (`run` юнит-тестами не покрыт — проверить, что `parseOpts`-тесты не меняются)
- [x] `make test`, `make lint`, `make build`; ручная проверка: `bin/mongo-replicator --src "mongodb://u@127.0.0.1:1/" --dst "mongodb://127.0.0.1:2/" --all-dbs </dev/null` → exit 2 с подсказкой про терминал и `SRC_URI`
- ⚠️ фактически `go get` поставил x/term v0.47.0 и x/sys v0.49.0 (новее, чем в Context), а директиву `go 1.26` переписал в `go 1.26.0` — эквивалентно, образ `go/build:1.26` собирает

### Task 4: Подсказка при неверном пароле

**Files:**
- Modify: `cmd/mongo-replicator/main.go`
- Modify: `cmd/mongo-replicator/main_test.go`

- [x] `isAuthFailed(err)` на `mongo.ServerError.HasErrorCode(18)`; в `failCode` — `.Str("hint", "проверьте пользователя и пароль")`
- [x] write tests `isAuthFailed`: форма реальной цепочки — `fmt.Errorf("ping: %w", mongo.CommandError{Code: 18, Wrapped: errors.New("auth")})` → true; `mongo.CommandError{Code: 13}` → false; обычная ошибка и nil → false
- [x] `make test` и `make lint` — должно пройти

### Task 5: README

**Files:**
- Modify: `README.md`

- [x] примеры запуска без пароля в URI: `mongodb://repl@src1:27017,src2:27017/?replicaSet=rs8&authSource=admin` — пароль будет запрошен
- [x] раздел «Пароли»: без пароля в URI — запрос в терминале, два отдельных запроса; запуск без терминала (`nohup`, будущий сервис): URI с паролем через окружение — `read -rs SRC_URI && export SRC_URI` (и `DST_URI`) или `.env` с правами `0600`; не передавать URI с паролем флагом `--src`/`--dst` (WARN: история шелла и `ps`); предупреждение, что `export SRC_URI=mongodb://u:секрет@...` тоже пишет пароль в историю; `authMechanism=SCRAM-*`/`PLAIN` в URI без пароля не поддерживается — механизм по умолчанию сам выбирает SCRAM
- [x] коды выхода: ошибки разбора URI (в т.ч. DNS для `mongodb+srv`) и отсутствие пароля — exit 2 до подключения
- [x] проверочные mongosh-запросы: `mongosh "$SRC_URI"` с URI без пароля тоже спросит пароль сам — одна строка

### Task 6: Verify acceptance criteria
- [x] пароль не нужен ни в URI, ни в командной строке: оба запрашиваются в терминале
- [x] URI с паролем работает: флагом — с WARN, из окружения — без WARN; URI без пользователя (compose, `make run`) — без запроса
- [x] run full test suite: `make test`
- [x] run integration tests: `make test-integration` (не должны измениться)
- [x] `make lint` без замечаний

### Task 7: [Final] Update documentation
- [ ] создать CLAUDE.md, только если появились неочевидные договорённости
- [ ] move this plan to `docs/plans/completed/`

## Post-Completion
*Items requiring manual intervention or external systems - no checkboxes, informational only*

**Manual verification**:
- в терминале: запуск с URI без пароля → два запроса (источник, цель), ввод не отображается; в истории шелла и `ps` пароля нет
- Ctrl+C во время ввода пароля → выход с кодом 130, эхо терминала восстановлено (набранное после выхода видно); то же для `kill <pid>` из соседней панели tmux
- Ctrl+D на пустом вводе → «ввод пароля прерван», exit 2
- неверный пароль на реальном кластере → exit 1, в логе `hint: проверьте пользователя и пароль`
- `read -rs SRC_URI && export SRC_URI` (URI с паролем) + `nohup` → старт без запроса и без WARN
