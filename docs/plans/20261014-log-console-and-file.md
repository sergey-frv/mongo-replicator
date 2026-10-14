# Лог в консоль и в файл

## Overview
- сейчас лог пишется только в stdout: JSON (`m`/`e`/`l`/`t`) или, с `--dbg`, читаемый текст на уровне DEBUG. Чтобы получить файл, README предлагает `nohup … >replicator.log 2>&1`, и тогда в консоли ничего не видно
- утилиту запускают вручную в `tmux` на часы и дни: человеку нужна читаемая консоль, а разбору инцидентов и алертам по `lag_sec` — полный JSON в файле
- решение: два приёмника с раздельными уровнями — консоль (текст в терминале, JSON при перенаправлении) и необязательный JSON-файл на дозапись; ротация — снаружи (`logrotate copytruncate`)
- `--dbg`/`DEBUG` удаляется без алиаса: тулза не запускалась на реальных базах, совместимость опций не нужна

## Context (from discovery)
- `cmd/mongo-replicator/main.go`:
  - `options.Debug` — `long:"dbg" env:"DEBUG"` (`:38`)
  - `setupLog(debug bool)` (`:396`): при `--dbg` — `zerolog.ConsoleWriter{Out: os.Stdout}` + `DebugLevel`; иначе JSON в stdout + `InfoLevel`; короткие имена полей (`zerolog.MessageFieldName` и т.д.) выставляются только в JSON-ветке
  - порядок в `main`: `parseOpts` (ошибки → stderr, exit 2) → `setupLog` → INFO `revision` → DEBUG дамп опций (`masked`) → `prepareClients` (приглашение пароля в stderr, требует TTY stdin+stderr) → `run`
- `cmd/mongo-replicator/main_test.go`: `clearEnv` чистит `DEBUG`; тесты `parseOpts` сравнивают структуру `options`
- `cmd/mongo-replicator/credentials.go`: вывод в stderr, логгер не использует — не меняется
- zerolog v1.35.1 (vendored, проверено ревью плана):
  - `MultiLevelWriter` (`writer.go:127`) вызывает `WriteLevel` у каждого приёмника и пишет в остальные даже при ошибке одного
  - `FilteredLevelWriter` (`writer.go:197`) — методы с pointer receiver; его doc-комментарий рекомендует ставить логгеру минимальный из уровней
  - `ConsoleWriter` не реализует `WriteLevel` → оборачивается `LevelWriterAdapter`; имена полей (`MessageFieldName` и др.) читает из глобальных переменных в момент записи (`console.go:201,303-315`)
  - `ParseLevel` (`log.go:178`) регистронезависим и принимает числа (`"1"` → info) — для проверки опций не годится
- go-flags v1.6.1: тег `choice` проверяется в `Option.Set` (`option.go:253`), в т.ч. для значений из env; env, заданная пустой строкой, заменяет default (`option.go:336`)
- `.golangci.yml`: gosec включён (исключение — только `_test.go`), `gochecknoinits` включён
- `golang.org/x/term` — уже прямая зависимость
- `replicator_test.go:354`, `integration_test.go:1424` подменяют `log.Logger` своим логгером в другом тестовом бинаре — изменения в `main` их не затрагивают
- `Makefile:33` (`make run`) — `--dbg`; `docker compose run` выделяет TTY → текст
- `README.md`: таблица опций (`:31` `--dbg`), пример `nohup … >replicator.log 2>&1` (`:100`), абзац «Логи — JSON в stdout…» (`:183`), `lag_sec` «для алертов по логам» (`:214`), DEBUG при неудачном ping (`:220`)
- команды: `make test`, `make test-integration`, `make lint`, `make build`

## Development Approach
- **testing approach**: Regular (код, затем тесты в той же задаче)
- перед Task 1 создать ветку от `master` (например, `log-console-and-file`)
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
- обратная совместимость опций не нужна (`--dbg` удаляется)

## Testing Strategy
- **unit tests**: `make test` — `parseOpts`, `newLogger`, `setupLog`; без `t.Parallel` — тесты трогают глобальные имена полей zerolog и `log.Logger`; `t.Cleanup` восстанавливает `log.Logger`
- интеграционные тесты не затронуты; `make test-integration` — контрольный прогон в конце

## Progress Tracking
- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix
- update plan if implementation deviates from original scope
- keep plan in sync with actual work done

## Solution Overview
- консоль (stdout): `zerolog.ConsoleWriter` (цветной текст со временем), если stdout — терминал; иначе JSON — при перенаправлении цветной текст только мешает
- файл: JSON, `O_APPEND`, без ротации; ротация — внешняя (`logrotate copytruncate`: с `O_APPEND` запись продолжается с начала обрезанного файла без «дыры»; строки, записанные между копированием и обрезкой, могут потеряться)
- раздельные уровни: каждый приёмник обёрнут `&zerolog.FilteredLevelWriter`, оба — в `zerolog.MultiLevelWriter`; уровень самого логгера — минимум из двух (`.Level(min)`), глобальный уровень zerolog не трогаем
- короткие имена полей (`m`/`e`/`l`/`t`) — всегда, выставляются в `newLogger` (не в `init`: `gochecknoinits`)
- приглашение пароля, ошибки опций и ошибки открытия файла — stderr, вне логгера

## Technical Details

**Опции** (`--dbg`/`DEBUG` удаляются)
| Опция | Env | По умолчанию | |
|---|---|---|---|
| `--log-level` | `LOG_LEVEL` | `info` | уровень консоли |
| `--log-file` | `LOG_FILE` | пусто — файла нет | путь к JSON-файлу, дозапись |
| `--log-file-level` | `LOG_FILE_LEVEL` | `info` (если файл задан) | уровень файла |

- оба уровня: теги `choice:"debug" choice:"info" choice:"warn" choice:"error"` — go-flags проверяет и флаги, и env, допустимые значения видны в `--help`; `"1"`, `"INFO"`, `"trace"` отвергаются (exit 2)
- `--log-level` — `default:"info"`; `--log-file-level` — **без** `default`: не задан → `info`; задан без `--log-file` → exit 2
- пустая env (`LOG_LEVEL=`, `LOG_FILE_LEVEL=`) — ошибка (`choice` не пропускает `""`), как `DB=`; `LOG_FILE=` — файла нет
- после проверки строка уровня переводится в `zerolog.Level` через map (не `ParseLevel`)

**Функции**
```go
// newLogger собирает логгер из двух приёмников; file == nil — только консоль.
// Выставляет короткие имена полей zerolog.
func newLogger(console io.Writer, consoleTTY bool, consoleLevel zerolog.Level,
    file io.Writer, fileLevel zerolog.Level) zerolog.Logger

// setupLog открывает файл (если задан), определяет TTY stdout, ставит log.Logger
// и пишет строку «лог». Возвращает открытый файл (nil без --log-file).
func setupLog(opts options) (*os.File, error)
```
- `newLogger`:
  - `zerolog.MessageFieldName = "m"`, `ErrorFieldName = "e"`, `LevelFieldName = "l"`, `TimestampFieldName = "t"`
  - консоль — `zerolog.ConsoleWriter{Out: console, TimeFormat: time.RFC3339}` при `consoleTTY`, иначе сам `console`
  - каждый приёмник — `&zerolog.FilteredLevelWriter{Writer: zerolog.LevelWriterAdapter{Writer: w}, Level: lvl}`
  - при `file != nil` — `zerolog.MultiLevelWriter(cons, fileW)`, иначе только `cons`
  - `zerolog.New(w).Level(min(consoleLevel, fileLevel)).With().Timestamp().Logger()`; без файла — `consoleLevel`
- `setupLog`:
  - `var file io.Writer` — присваивается только после успешного `OpenFile` (не передавать typed-nil `*os.File`)
  - `os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640) //nolint:gosec // путь и права задаёт оператор (--log-file), группе — чтение` (G302/G304)
  - каталог не создаётся; ошибка → `main` печатает в stderr и exit 2
  - TTY: `term.IsTerminal(int(os.Stdout.Fd())) //nolint:gosec // дескриптор stdout помещается в int`
  - после установки `log.Logger` — строка «лог» (тестируется вместе с `setupLog`)
  - запись без буфера — `os.Exit` и сигналы ничего не теряют, `Sync` не нужен; `main` файл не закрывает (процесс живёт до выхода)

**Строка при старте** (пишется в `setupLog`, после неё в `main` — `revision`)
```
2026-10-14T16:00:00+06:00 INF лог console=info/text file=/var/log/repl/urlshrinker.log file_level=info
```
- без файла: `file=""`, `file_level` не пишется
- строки уровня INFO (`лог`, `revision`) попадают только в приёмники с уровнем `info` и ниже — отметить в README

## What Goes Where
- **Implementation Steps**: код, юнит-тесты, Makefile, README
- **Post-Completion**: ручная проверка в `tmux` и с `logrotate`

## Implementation Steps

### Task 1: Опции логирования

**Files:**
- Modify: `cmd/mongo-replicator/main.go`
- Modify: `cmd/mongo-replicator/main_test.go`

- [x] удалить `Debug` (`--dbg`/`DEBUG`); добавить `LogLevel` (`--log-level`/`LOG_LEVEL`, `default:"info"`, `choice`), `LogFile` (`--log-file`/`LOG_FILE`), `LogFileLevel` (`--log-file-level`/`LOG_FILE_LEVEL`, `choice`, без default)
- [x] `parseOpts`: `--log-file-level` без `--log-file` → ошибка; `LogFileLevel` не задан → `info`
- [x] `main_test.go`: `clearEnv` — убрать `DEBUG`, добавить `LOG_LEVEL`, `LOG_FILE`, `LOG_FILE_LEVEL`; обновить ожидаемые `options` в существующих тестах `parseOpts`
- [x] write tests `parseOpts`: умолчания (`info`, без файла); значения из env; флаги перекрывают env; каждый допустимый уровень; `1`, `INFO`, `trace`, `verbose` → ошибка; `LOG_LEVEL=` и `LOG_FILE_LEVEL=` → ошибка; `LOG_FILE=` → файла нет; `--log-file-level debug` без `--log-file` → ошибка; `--dbg` → неизвестная опция
- [x] `make test` и `make lint` — должно пройти (временно `setupLog(opts.LogLevel == "debug")`, до Task 2)

### Task 2: Сборка логгера: консоль и файл

**Files:**
- Modify: `cmd/mongo-replicator/main.go`
- Create: `cmd/mongo-replicator/log_test.go`

- [ ] `newLogger` и `setupLog(opts) (*os.File, error)` по Technical Details; строка «лог» в `setupLog`; `main`: ошибка `setupLog` → stderr + exit 2
- [ ] write tests `newLogger` (буферы вместо stdout/файла, `t.Cleanup` восстанавливает `log.Logger`):
  - `consoleTTY=true` → консоль — текст: содержит `INF` и сообщение, **не** содержит `m=`, `l=`, `t=` (иначе имена полей рассинхронизированы с `ConsoleWriter`); событие `.Err(...)` даёт `e=` с текстом ошибки и уровень `ERR`
  - `consoleTTY=false` → консоль — JSON с полями `m`, `l`, `t`; у ошибки — `e`
  - файл — всегда JSON с `m`, `l`, `t`
  - консоль `info`, файл `debug` → DEBUG только в файле, INFO в обоих
  - консоль `debug`, файл `warn` → INFO только в консоли, WARN в обоих
  - `file == nil` → пишется только консоль, без паники
- [ ] write tests `setupLog` (`t.TempDir`, файл закрывается в `t.Cleanup`): каталог не существует → ошибка; существующий файл с содержимым → старое содержимое на месте, новые строки дописаны JSON-ом, среди них «лог» с `console`, `file`, `file_level`; без `--log-file` → возвращается `nil`, файл не создаётся
- [ ] `make test` и `make lint` — должно пройти

### Task 3: Makefile и README

**Files:**
- Modify: `Makefile`
- Modify: `README.md`

- [ ] `Makefile` `run`: `--log-level debug` вместо `--dbg`
- [ ] README, таблица опций: `--log-level`, `--log-file`, `--log-file-level` (допустимые уровни, пустые env — ошибка); убрать `--dbg`
- [ ] README, раздел «Логи»: консоль — текст в терминале, JSON при перенаправлении; файл — JSON, дозапись, строка `лог` при старте — граница запусков; строки INFO при старте видны только в приёмниках с уровнем ≤ `info`; раздельные уровни (пример: консоль `info`, файл `debug` при разборе проблемы); ротация не встроена — пример `logrotate` с `copytruncate` и оговорка, что пара строк между копированием и обрезкой может потеряться
- [ ] README, запуск: типичный в `tmux` — `--log-file /var/log/repl/urlshrinker.log`; пример `nohup … --log-file … >/dev/null 2>>replicator.stderr &` — stdout не нужен (JSON уже в файле), а stderr сохраняется: туда идут ошибки опций и открытия файла, подсказка про пароль без терминала, ошибки записи лога и паника; заменить старый пример `>replicator.log 2>&1`
- [ ] README: переписать абзац «Логи — JSON в stdout…» под новые опции; алерты по `lag_sec` — по файлу; DEBUG о неудачном ping — только при `--log-level debug` или `--log-file-level debug`; описание полей прогресса и DDL не менять

### Task 4: Verify acceptance criteria
- [ ] в терминале — читаемый текст, при перенаправлении stdout — JSON; с `--log-file` файл пишется JSON-ом на дозапись
- [ ] уровни консоли и файла независимы; неверные и пустые значения → exit 2
- [ ] `--dbg` нигде не упоминается (`grep` по репозиторию, кроме `docs/plans/`)
- [ ] run full test suite: `make test`
- [ ] run integration tests: `make test-integration`
- [ ] `make lint` без замечаний

### Task 5: [Final] Update documentation
- [ ] создать CLAUDE.md, только если появились неочевидные договорённости
- [ ] move this plan to `docs/plans/completed/`

## Post-Completion
*Items requiring manual intervention or external systems - no checkboxes, informational only*

**Manual verification**:
- запуск в `tmux` с `--log-file`: в консоли цветной текст, в файле JSON, строка `лог` при старте; запрос пароля по-прежнему работает (stderr)
- `logrotate -f` с `copytruncate` на работающем репликаторе: запись продолжается в обрезанный файл без «дыры» из нулевых байтов
- `nohup … --log-file x >/dev/null 2>>replicator.stderr &` → лог в файле, ошибки запуска — в `replicator.stderr`
