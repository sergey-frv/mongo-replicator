# Структура проекта по Go way

## Overview
- сейчас весь код лежит в `cmd/mongo-replicator/`:
  - пакет `main` — `main.go` на 591 строку, `credentials.go`, `progress.go`;
  - ядро — вложенный пакет `cmd/mongo-replicator/replicator`. Его файлы не всегда говорят, что внутри: в `ddl.go` — разбор ошибок драйвера, в `replicator.go` вперемешку `Err*`, поток, запись пачки и лог прогресса;
  - модуль называется `mongo-replicator`, а не путём репозитория
- цели:
  - читаемость и навигация: по имени пакета или файла понятно, где что лежит;
  - раскладка по официальному руководству go.dev/doc/modules/layout: `cmd/<app>` — команда, `internal/` — код, не предназначенный для импорта извне
- что получится:
  - модуль `github.com/sergey-frv/mongo-replicator`;
  - ядро и независимые части — в `internal/`;
  - `main.go` разрезан по темам;
  - файлы ядра разложены по темам, тесты — по правилу `<file>_test.go`
- **перенос, а не переписывание**:
  - тела функций не меняются, кроме переименований экспортируемых имён и замены `stateActiveFor` (см. Technical Details);
  - поведение утилиты не меняется

## Context (from discovery)
- ветка `multi-process-traffic`, HEAD `8023706` («verbose для тестов» — `-v` в `make test`/`make test-integration`, коммит пользователя)
- тесты уже разложены по областям и приведены к правилам `CLAUDE.md` (планы `completed/20261015-split-tests-by-area.md`, `completed/20261016-apply-test-rules.md`)
- `cmd/mongo-replicator/replicator`: ~1670 строк кода в 10 файлах, почти всё — методы `*Replicator`. От `*Replicator` не зависят:
  - счётчики трафика: `Traffic`, `TrafficInfo`, `countingDialer`, `countingConn` в `traffic.go`;
  - чистые функции файла состояния в `statefile.go`
- кто использует имена, которые переезжают:
  - `main.go`: `replicator.Traffic`, `replicator.StatePath`;
  - `progress.go`/`progress_test.go`: `replicator.Status`, `TrafficInfo`, `MachineTraffic`, `BatchInfo`, `StateRetry`;
  - `status.go`: `Status.Traffic TrafficInfo`, `Status.Machine *MachineTraffic`;
  - `TrafficInfo.add` вызывают `machineTraffic` (`statefile.go:211`, переезжает в `statefile`) и `writeStateFile` (`statefile.go:290`, остаётся в `replicator`);
  - тесты: `status_test.go`, `statefile_test.go`, `traffic_integration_test.go`, `network_integration_test.go`, `harness_integration_test.go`
- `stateActiveFor` — пакетная переменная (30 с):
  - её читает `machineTraffic`, который вызывает `logMachine` (`traffic.go:61`);
  - интеграционные тесты меняют её через `shortActive` (`traffic_integration_test.go:60`), но все проверки `Active` в них идут через прямые вызовы `machineTraffic(dir, time.Now())`, а не через `logMachine`
- `progress_test.go` использует `ansi` — регулярное выражение из `log_test.go:50`
- тесты пакета `main` уже разложены по темам: `options_test`, `mask_test`, `errors_test`, `log_test`, `main_test`, `credentials_test`, `auth_integration_test`. `TestProgressMode` лежит в `options_test.go` и использует `progressTTY`/`progressPlain`
- `Makefile`, `Dockerfile.dev`, `docker-compose.yml`, `.golangci.yml` путей пакетов не содержат, кроме `./cmd/mongo-replicator` в `build`/`run`, — он остаётся. `.env` остаётся в `cmd/mongo-replicator/.env`
- `.golangci.yml`: revive включён без пресетов исключений — у каждого экспортируемого имени нужен doc-комментарий, начинающийся с имени
- команды: `make test`, `make test-integration`, `make lint`, `make build`; одиночный прогон — только через `docker compose run`, не локальный `go test`

## Development Approach
- **testing approach**: Regular. Новой логики нет; критерий — неизменные имена тестов и зелёные прогоны
- complete each task fully before moving to the next
- **CRITICAL: all tests must pass before starting next task** - no exceptions
- **CRITICAL: update this plan file when scope changes during implementation**
- каждая задача — отдельный коммит; сборка, тесты и линтер зелёные
- коммитить только файлы задачи: `git add <пути>`, не `git add -A`
- переносить через `git mv` там, где файл переезжает целиком, чтобы `git diff -M` и `git blame -C -C` показывали историю
- **тела функций не меняются**. Допустимы только:
  - переименования из таблиц Technical Details и правка вызовов под них;
  - замена `stateActiveFor` (Technical Details → `internal/statefile`);
  - строки `package`/`import`;
  - doc-комментарии: старое имя → новое, новые комментарии у экспортированных имён (revive);
  - копия `ansi` в `panel_test.go`
- **имена тестов не меняются**, даже если функция переименована (`TestMachineTraffic` проверяет `Summary`, `TestStatePath` — `Path`, `TestTraffic*` — `Counters`). Так задумано: имена сверяются с эталоном
- экспортируемые имена `internal/*` переименовываются свободно: это не внешний API, внешних импортёров быть не может. Совместимость внешних контрактов (флаги и env, коды выхода, формат чекпойнта и файлов состояния) пока не нужна — утилита нигде не используется; этот план их не меняет

## Testing Strategy
- **эталон** (Task 1, `/tmp/go-layout/`):
  - имена тестов:
    - юнит — `docker compose run --rm --no-deps app go test -list '.*' ./...`;
    - интеграционные — `docker compose run --rm app go test -tags integration -list '.*' ./...`: нужен полный стек, проверять статус команды;
    - из вывода берутся строки `grep -E '^(Test|Example|Benchmark|Fuzz)' | sort`;
  - копия исходников: `git archive HEAD cmd | tar -x -C /tmp/go-layout/before`
- **после каждой задачи**:
  - **имена тестов** совпадают с эталоном (меняются только пакеты);
  - **строки кода** — мультимножество строк, механически:
    ```bash
    R='s/\bTrafficInfo\b/traffic.Info/g; s/\*Traffic\b/*traffic.Counters/g; …'   # sed переименований из таблиц, по мере задач
    norm() { grep -hvE '^(package |import |\)$|\t"[^"]+"$|$)' "$@" | sed -E 's/^[ \t]+//; s/[ \t]+/ /g' | sort; }
    diff <(sed -E "$R" $(find /tmp/go-layout/before -name '*.go') | norm /dev/stdin) <(norm $(find cmd internal -name '*.go'))
    ```
    `R` применяется к версии «до», пробелы нормализуются (gofmt выравнивает поля структур под новую длину типов). Ожидаемые отличия отмечаются в коммите:
    - копия `ansi`;
    - аргумент `DefaultActiveFor`/`itActiveFor` у `Summary`;
    - удалённые `shortActive`;
    - doc-комментарии;
    - префиксы пакетов (`traffic.`, `statefile.`) внутри перенесённого кода
  - `git diff -M`: файлы, переехавшие целиком (`git mv`), показаны как переименования. Разделённые `traffic.go`/`statefile.go` (Tasks 3–4) показаны как удаление + добавление — это ожидаемо;
  - `make test`, `make test-integration`, `make lint`
- новых тестов нет

## Progress Tracking
- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix
- update plan if implementation deviates from original scope
- keep plan in sync with actual work done

## Solution Overview
```
github.com/sergey-frv/mongo-replicator
├── cmd/mongo-replicator/     package main — CLI: опции, лог, маскирование, подсказки, запуск
├── internal/replicator/      ядро: *Replicator (поток, запись, DDL, чекпойнт, аренда, статус)
├── internal/statefile/       файл состояния на машине: путь, чтение/запись, сводка
├── internal/traffic/         счётчики байт клиентов (диалер)
└── internal/panel/           панель статуса (bubbletea)
```
- зависимости — в одну сторону, циклов нет: `traffic ← statefile ← replicator ← panel ← main`. Тестам `panel` можно импортировать `statefile`
- `internal/`, а не `pkg/`: код не предназначен для импорта извне, и это гарантирует компилятор
- `package main` из нескольких файлов, без `internal/cli`: команда одна, подкоманд и второго бинарника нет
- порядок — снизу вверх по зависимостям; каждый шаг компилируется сам. После переименования модуля `cmd/mongo-replicator/replicator` может импортировать `internal/` из корня модуля (общий префикс пути)

## Technical Details

### Модуль
- `go.mod`: `module github.com/sergey-frv/mongo-replicator`
- импорт `mongo-replicator/cmd/mongo-replicator/replicator` → `github.com/sergey-frv/mongo-replicator/cmd/mongo-replicator/replicator` (в Task 6 — `.../internal/replicator`)
- `go mod tidy` и `go mod vendor` — в контейнере. Собственный модуль в `vendor/` не попадает; `vendor/modules.txt` должен соответствовать `go.mod`

### `internal/traffic`
| Было (`replicator/traffic.go`) | Стало |
|---|---|
| `Traffic` | `traffic.Counters` |
| `TrafficInfo` | `traffic.Info` |
| `TrafficInfo.add` | `traffic.Info.Add` — обязательно экспортируется: нужен `statefile` и `replicator` |
| `(*Traffic).SrcDialer`, `DstDialer`, `Snapshot` | методы `*Counters` |
| `countingDialer`, `countingConn` | без изменений (неэкспортируемые) |
| `logMachine`, `trafficFields`, `defaultMachineEvery` | остаются в `replicator` (позже — `report.go`) |
- тесты: в `internal/traffic/traffic_test.go` переходят `echoServer`, `partialConn`, `timeoutErr`, `TestTrafficDialers`, `TestCountingConnPartial`, `TestCountingConnDeadline`, `TestTrafficDialClosedPort`, `TestTrafficSnapshotNil`. `TestTrafficFields` остаётся в `replicator`

### `internal/statefile`
| Было (`replicator/statefile.go`) | Стало |
|---|---|
| `state` | `statefile.State` |
| `MachineTraffic` | `statefile.Machine` |
| `ErrStateCorrupt` | `statefile.ErrCorrupt` |
| `StatePath` | `statefile.Path` |
| `clusterKey`, `stateFileName`, `writeTemp` | без изменений (неэкспортируемые) |
| `loadState`, `writeState` | `statefile.Load`, `statefile.Write` |
| `moveAside` | `statefile.MoveAside` |
| `machineTraffic(dir, now)` | `statefile.Summary(dir, now, activeFor)` |
| `var stateActiveFor = 30 * time.Second` | `const statefile.DefaultActiveFor = 30 * time.Second` |
- **`activeFor`**, без нового поля у `Replicator`:
  - `logMachine` вызывает `statefile.Summary(r.cfg.StateDir, r.lastMachine, statefile.DefaultActiveFor)`;
  - в `traffic_integration_test.go`: константа `itActiveFor = time.Second`. Четыре вызова `machineTraffic(dir, time.Now())` становятся `statefile.Summary(dir, time.Now(), itActiveFor)`, `time.Sleep(2 * stateActiveFor)` — `time.Sleep(2 * itActiveFor)`. Хелпер `shortActive` и оба его вызова удаляются;
  - в `internal/statefile`: `TestMachineTraffic` вызывает `Summary(…, DefaultActiveFor)` — его случаи `-29s`/`-5s` рассчитаны на 30 с;
  - `TestLogMachine` полагается на окно 30 с и не меняется;
  - общих изменяемых переменных больше нет, гонок нет
- **остаются в `replicator`** (позже — `state.go`): `startState`, `loadStateBase`, `writeStateFile`, `stopState`, `defaultStateEvery`
- тесты: в `internal/statefile/statefile_test.go` (`package statefile`) переходят `TestClusterKey`, `TestStateFileName`, `TestWriteReadState`, `TestWriteStateNoDir`, `TestReadStateCorrupt`, `TestMoveAside`, `TestMoveAsideTaken`, `TestMachineTraffic`, `TestStatePath`. В `replicator` остаются `newState`, `TestStartState*`, `TestLogMachine*`

### `internal/panel`
- `cmd/mongo-replicator/progress.go` → `internal/panel/panel.go` (`package panel`) без `progressMode`, `progressTTY`, `progressPlain` — они остаются в `main` (их использует `main.go:261` и `TestProgressMode`)
- экспортируется только `startPanel` → `panel.Start` (вызывает `withPanel` в `main`). `panel`, `newPanel`, `tick`, `tickMsg`, `finalMsg`, `teaWriter`, стили, `renderPanel`, `fmt*` остаются неэкспортируемыми
- типы в коде панели — новые: `traffic.Info`, `*statefile.Machine` в `replicator.Status`
- `progress_test.go` → `internal/panel/panel_test.go` (`package panel`). В тестовый файл копируется `var ansi = regexp.MustCompile(...)` вместе с комментарием из `log_test.go:50`; копия в `log_test.go` остаётся

### `cmd/mongo-replicator` (package main)
| Файл | Содержимое |
|---|---|
| `main.go` | `revision`, `appName`, `pingTimeout`, `retryTimeout`, `releaseTimeout`, `main`, `run`, `replicate`, `withPanel`, `connect`, `failCode`, `logTraffic` |
| `options.go` | `options`, `maxDBNameLen`, `parseOpts`, `stateDir`, `checkApplyDrops`, `checkDBName`, `isHelp`, `progressTTY`, `progressPlain`, `progressMode` |
| `log.go` | `logLevels`, `newLogger`, `setupLog`, `fileWriter`, `stdoutTTY` |
| `mask.go` | `secretOptions`, `maskURI`, `schemeEnd`, `maskOptions`, `maskPair`, `secretKey`, `shownURI`, `logURI` |
| `errors.go` | `permanent`, `hint`, `isAuthFailed` |
| `credentials.go` | без изменений |
- тестовые файлы `main` уже соответствуют этой раскладке и не переносятся

### `internal/replicator`
| Файл | Содержимое |
|---|---|
| `replicator.go` | `SystemDBs`, `Config`, `Replicator`, `New`, `Run`, `Applied` |
| `stream.go` | `streamPipeline`, `stream`, `checkNS` |
| `apply.go` | `batch` и `add` (из `batch.go`), `flush`, `dstNS`, `nsName` |
| `event.go` | `namespace`, `event`, `toModel` — без изменений |
| `ddl.go` | `applyDDL`, `dropAllowed`, `tempCollRe`, `isTempColl`, `runCommand` |
| `errors.go` | `ErrNoStartPoint`, `ErrDBMismatch`, `ErrStreamInvalidated`, `ErrForeignNamespace`, `ErrReservedCollection`, `DropError`, `IsEventTooLarge`, `IsHistoryLost`, `IsDBCaseConflict`, `hasCode`, константы кодов ошибок (из `ddl.go`) |
| `checkpoint.go` | как сейчас, плюс `dbPair`, `checkDBs` |
| `lease.go`, `lag.go`, `status.go` | без изменений, кроме типов `traffic`/`statefile` |
| `state.go` | писатель файла состояния: `defaultStateEvery`, `startState`, `loadStateBase`, `writeStateFile`, `stopState` |
| `report.go` | сводки в лог: `defaultMachineEvery`, `logProgress`, `logMachine`, `trafficFields` |
- `ErrLeaseHeld`/`ErrLeaseLost` остаются в `lease.go`; `ckptColl`/`ckptID` — в `checkpoint.go`
- объявление, которого нет в таблице, кладётся рядом с функцией, которая его использует; отметить ➕

**Юнит-тесты.** Правило `<file>_test.go` применяется при разборе `replicator_test.go`, `statefile_test.go` и `traffic_test.go`. Из остальных файлов в `report_test.go` переезжают только `TestLogProgress*`, чтобы тесты `logProgress` были вместе. Остальное в `status_test.go`, `lease_test.go`, `lag_test.go` не трогается: их тесты проверяют статус и аренду через `flush`/`applyDDL`, это их тема.

| Файл | Тесты и хелперы |
|---|---|
| `event_test.go` | `mustMarshal`, `changeEvent`, `decodeEvent`, `TestToModelUpsert`, `TestToModelDelete`, `TestToModelUpdateWithoutDocument`, `TestToModelNotDML`, `TestRawIsCopied` |
| `apply_test.go` | `TestBatchOrder`, `TestNSName` |
| `ddl_test.go` | `TestDDLDescriptionParsing`, `TestApplyDDLRenameReserved`, `TestApplyDDLRenameChecksLease`, `TestIsTempColl` |
| `errors_test.go` | `TestIsHistoryLost`, `TestIsEventTooLarge`, `TestIsDBCaseConflict`, `TestHasCode`, `TestErrNoStartPoint`, `TestDropError` |
| `stream_test.go` | `TestStreamPipeline`, `TestCheckNSAllowed`, `TestCheckNSForeign` |
| `checkpoint_test.go` | `TestCheckDBsSame`, `TestCheckDBs` |
| `replicator_test.go` | `TestRunNoDB`, `TestNewDstDB`, `TestNewApplyDrops` |
| `report_test.go` | `TestLogProgressBeforeFirstSave` (из `replicator_test.go`), `TestLogProgressStatus`, `TestLogProgressSrcUnavailable` (из `status_test.go`), `TestTrafficFields`, `TestLogMachine`, `TestLogMachineNoDir` |
| `state_test.go` | `newState`, `TestStartState*` |
| `lease_test.go`, `lag_test.go`, `status_test.go` | без изменений, кроме ухода `TestLogProgress*` из `status_test.go` |

- `mustMarshal` вызывают `lag_test.go` и `status_test.go`; пакет один, определения в `event_test.go` достаточно
- **интеграционные тесты**, обвязка и сетевые тесты переезжают в `internal/replicator` как есть, раскладка по областям не меняется. `auth_integration_test.go` остаётся в `cmd/mongo-replicator`

### Документация
- `CLAUDE.md`:
  - строку «Код — `cmd/mongo-replicator`…» заменить картой пакетов (схема из Solution Overview);
  - «Где живёт состояние»:
    - `statefile.go` → `internal/statefile` (+ писатель `internal/replicator/state.go`);
    - `checkpoint.go` → `internal/replicator/checkpoint.go`;
    - `moveAside` → `statefile.MoveAside`;
    - `stateActiveFor` → `statefile.DefaultActiveFor`;
  - «Тесты», раскладка:
    - убрать исключение для `main.go`;
    - `mustMarshal` — в `event_test.go`;
    - фразу «компилятор размещение не проверяет: пакет один» уточнить: в пределах пакета; общий тестовый хелпер разных пакетов дублируется (пример — `ansi`);
  - «Интеграционные тесты»: `shortActive` → `itActiveFor`. `t.Parallel` в `internal/replicator` по-прежнему нельзя: тесты делят кластеры
- `README.md`, раздел «Разработка»: короткая схема пакетов
- `.revmux/profile.md` (строка 4 — старый путь) — поправить локально. Каталог `.revmux/` в `.gitignore`, в коммит не идёт
- `docs/plans/completed/` не трогать — это история

## What Goes Where
- **Implementation Steps**: перенос кода и тестов, `go.mod`, документация
- **Post-Completion**: репозиторий на GitHub и `git remote` — вне плана

## Implementation Steps

### Task 1: Эталон и исходное состояние

**Files:**
- нет изменений в репозитории (эталон — в `/tmp/go-layout/`)

- [x] `git status --short` — допустим только неотслеживаемый файл этого плана; при других изменениях — ⚠️ и остановиться (чисто; план уже закоммичен в 98356d0, ветка `go-project-layout`)
- [x] снять имена юнит- и интеграционных тестов (Testing Strategy), отсортировать; сохранить копию исходников «до» (`/tmp/go-layout/unit.txt` — 143, `integ.txt` — 204, `before/` — 39 `.go`)
- [x] `make test`, `make test-integration`, `make lint` — исходное состояние зелёное (пропущены только `TestLimitationUpdateLookupAfterRename`, `TestLimitationRenameInCatchUpWindow` — как и раньше)

### Task 2: Модуль `github.com/sergey-frv/mongo-replicator`

**Files:**
- Modify: `go.mod`, `vendor/modules.txt` (если меняется)
- Modify: все `.go` с импортом `mongo-replicator/...`

- [x] `module github.com/sergey-frv/mongo-replicator` в `go.mod`; переписать импорты (`grep -rl '"mongo-replicator/' --include='*.go' cmd`) (5 файлов в `cmd/mongo-replicator`)
- [x] `docker compose run --rm --no-deps app sh -c 'go mod tidy && go mod vendor'` (`go.sum` и `vendor/` не изменились)
- [x] `grep -rn '"mongo-replicator/' --include='*.go' . --exclude-dir=vendor` — пусто
- [x] `make test`, `make test-integration`, `make lint`; имена тестов совпадают с эталоном (юнит — 143, интеграционные — 204)
- [x] коммит

### Task 3: Пакет `internal/traffic`

**Files:**
- Create: `internal/traffic/traffic.go`, `internal/traffic/traffic_test.go`
- Modify: `cmd/mongo-replicator/replicator/{traffic,traffic_test,statefile,statefile_test,replicator,status,status_test}.go`, интеграционные тесты с `Traffic`/`TrafficInfo` (`traffic_integration_test.go`, `network_integration_test.go`)
- Modify: `cmd/mongo-replicator/{main,progress,progress_test}.go`

- [x] перенести `Traffic`, `TrafficInfo` (+ `Add`), `countingDialer`, `countingConn` с переименованиями; doc-комментарии у экспортированных имён (+ `// Package traffic`)
- [x] перенести тесты и хелперы счётчиков в `internal/traffic/traffic_test.go`
- [x] поправить вызовы: `grep -rln '\bTraffic\b\|TrafficInfo\|\.add(' --include='*.go' cmd`
- [x] проверки из Testing Strategy; `make test`, `make test-integration`, `make lint` (имена: юнит — 143, интеграционные — 204, совпадают; в сверке строк — только doc-комментарии и имена без префикса внутри `internal/traffic`)
- [x] коммит

### Task 4: Пакет `internal/statefile`

**Files:**
- Create: `internal/statefile/statefile.go`, `internal/statefile/statefile_test.go`
- Modify: `cmd/mongo-replicator/replicator/{statefile,statefile_test,traffic,status,status_test,replicator}.go`, `{traffic,network,harness}_integration_test.go`
- Modify: `cmd/mongo-replicator/{main,progress,progress_test}.go`

- [x] перенести чистые функции и типы с переименованиями; писатель остаётся в `replicator` (`defaultStateEvery` остался у писателя в `replicator/statefile.go`)
- [x] `stateActiveFor` → `DefaultActiveFor` и параметр `Summary`; `logMachine`, `itActiveFor`, удаление `shortActive` — по Technical Details
- [x] перенести чистые тесты в `internal/statefile/statefile_test.go`
- [x] проверки из Testing Strategy; `make test`, `make test-integration`, `make lint` (имена: юнит — 143, интеграционные — 204, совпадают; в сверке строк — doc-комментарии, аргумент `DefaultActiveFor`/`itActiveFor`, удалённый `shortActive`, имена без префикса внутри `internal/statefile`)
- [x] коммит

### Task 5: Пакет `internal/panel`

**Files:**
- Move: `cmd/mongo-replicator/progress.go` → `internal/panel/panel.go`, `cmd/mongo-replicator/progress_test.go` → `internal/panel/panel_test.go`
- Modify: `cmd/mongo-replicator/main.go`

- [x] `git mv` обоих файлов; `package panel`; `startPanel` → `Start` (с doc-комментарием; + `// Package panel`)
- [x] `progressMode`, `progressTTY`, `progressPlain` — в `cmd/mongo-replicator/main.go` (переедут в `options.go` в Task 7; встали после `isHelp`)
- [x] скопировать `ansi` с комментарием в `panel_test.go`; поправить `withPanel`
- [x] проверки из Testing Strategy; `make test`, `make test-integration`, `make lint` (имена: юнит — 143, интеграционные — 204, совпадают; в сверке строк — `startPanel` → `Start`/`panel.Start`, копия `ansi`, doc-комментарии)
- [x] коммит

### Task 6: Ядро в `internal/replicator`, файлы по темам

**Files:**
- Move: `cmd/mongo-replicator/replicator/` → `internal/replicator/`
- Rename: `batch.go` → `apply.go`, `statefile.go` → `state.go`, `traffic.go` → `report.go`, `statefile_test.go` → `state_test.go`, `traffic_test.go` → `report_test.go`
- Create: `internal/replicator/{stream,errors}.go`, `internal/replicator/{event,apply,ddl,errors,stream,checkpoint}_test.go`
- Modify: импорты в `cmd/mongo-replicator/*.go` и `internal/panel/*.go` (`grep -rl 'cmd/mongo-replicator/replicator' --include='*.go' cmd internal`)

- [x] `git mv cmd/mongo-replicator/replicator internal/replicator`; поправить импорты; `make test`, `make lint`; отдельный коммит «перенос без правок» (41547c0)
- [x] переименовать файлы через `git mv` (список выше), затем разложить код по таблице `internal/replicator`
  - ➕ переименования — отдельным коммитом без правок: после раскладки `apply.go` и `report_test.go` выросли больше чем вдвое, и `git diff -M` перестал видеть в них `batch.go`/`traffic_test.go`
  - объявлений вне таблицы не нашлось; `hasCode` — в `errors.go` после `DropError`
- [x] разложить юнит-тесты по таблице; интеграционные не трогать
- [x] проверки из Testing Strategy; `make test`, `make test-integration`, `make lint` (имена: юнит — 143, интеграционные — 204, совпадают; сверка строк с деревом до задачи — без отличий)
- [x] коммит

### Task 7: Разрезание `main.go`

**Files:**
- Create: `cmd/mongo-replicator/{options,log,mask,errors}.go`
- Modify: `cmd/mongo-replicator/main.go`

- [ ] разложить объявления `main.go` по таблице, включая константы и переменные
- [ ] проверки из Testing Strategy; `make test`, `make test-integration` (для `auth_integration_test.go`), `make lint`
- [ ] коммит

### Task 8: Документация

**Files:**
- Modify: `CLAUDE.md`, `README.md`
- Modify локально (без коммита): `.revmux/profile.md`

- [ ] `CLAUDE.md` — по Technical Details → Документация
- [ ] `README.md`, раздел «Разработка» — схема пакетов
- [ ] устаревших ссылок не осталось: `grep -rnE 'cmd/mongo-replicator/replicator|StatePath|MachineTraffic|stateActiveFor|shortActive|progress\.go|replicator_test\.go|moveAside|TrafficInfo|loadState|writeState|machineTraffic|statefile\.go|traffic\.go' CLAUDE.md README.md .claude .revmux/profile.md`
- [ ] коммит (`CLAUDE.md`, `README.md`)

### Task 9: Verify acceptance criteria
- [ ] дерево: `cmd/mongo-replicator/` (только `package main`), `internal/{replicator,statefile,traffic,panel}`; нет `cmd/mongo-replicator/replicator` и `progress.go`
- [ ] импорты внутренних пакетов не шире схемы: `docker compose run --rm --no-deps app go list -f '{{.ImportPath}}: {{join .Imports " "}}' ./internal/... ./cmd/...`:
  - `traffic` — без внутренних пакетов;
  - `statefile` — не больше `traffic`;
  - `replicator` — не больше `traffic`, `statefile`;
  - `panel` — не больше `replicator`, `traffic`, `statefile`;
  - `main` — любые из них
- [ ] каждый `<file>.go`, у которого есть юнит-тесты, имеет `<file>_test.go`; исключения — по таблице и ➕
- [ ] имена тестов совпадают с эталоном Task 1
- [ ] `make build` собирает `bin/mongo-replicator` и `--help` работает; после `make build` в `git status` нет изменений `go.mod`/`go.sum`/`vendor`
- [ ] `make test`, `make test-integration`, `make lint`
- [ ] `git diff 8023706 -- Makefile` пустой

### Task 10: [Final] Update documentation
- [ ] README.md и CLAUDE.md обновлены (Task 8)
- [ ] move this plan to `docs/plans/completed/`

## Post-Completion
*Items requiring manual intervention or external systems - no checkboxes, informational only*

- создать репозиторий `github.com/sergey-frv/mongo-replicator` и добавить `git remote`; после этого заработает `go install github.com/sergey-frv/mongo-replicator/cmd/mongo-replicator@latest`
