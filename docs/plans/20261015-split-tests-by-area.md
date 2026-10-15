# Разделение тестов по областям и правила тестов в CLAUDE.md

## Overview
- `cmd/mongo-replicator/replicator/integration_test.go` вырос до 3124 строк (60 тестов без `TestMain`, ~40 хелперов), `cmd/mongo-replicator/main_test.go` — 626 строк; читать, ревьюить и находить нужный тест тяжело
- решение — **чистый перенос**: интеграционные тесты раскладываются по файлам областей (`<область>_integration_test.go`) с общей обвязкой в `harness_integration_test.go`, `main_test.go` — по темам; логика, имена тестов и хелперов не меняются
- в `CLAUDE.md` добавляется раздел «Тесты» — правила читаемости для **новых** тестов и хелперов, чтобы им следовали и люди, и агенты (ralphex, ревью)
- вне объёма: стиль проверок (оставшиеся `t.Fatal*`/`t.Errorf`), конструкторы событий, замена `time.Sleep`, переименование существующих хелперов — существующие тесты разом не переписываются

## Context (from discovery)
- ветка `multi-process-traffic`; планы `20261015-multi-process-traffic-progress` и `20261015-state-in-target-db-and-files` выполнены, `CLAUDE.md` есть (разделы «Где живёт состояние», «Интеграционные тесты», «Сетевые тесты (Toxiproxy)»)
- `integration_test.go`: `//go:build integration`, `package replicator`; константы `waitTimeout`, `pollEvery`, `quietInterval`, `itLeaseTTL`, `itLeaseRenew`, `itStateEvery` (`:32-43`); глобальные переменные (клиенты, прокси, URI, `lastSrcSec`) (`:45-54`, без инициализаторов, `init()` нет)
- `TestMain` читает `IT_SRC_URI`/`IT_DST_URI` (без них — `log.Fatal`), пингует оба кластера и при `IT_PROXY_API` создаёт прокси → `go test -list` с тегом `integration` работает только в контейнере `app` **с зависимостями** (`docker compose run --rm app`, не `--no-deps`); `go test -run '^$'` тоже запускает `TestMain` и для проверки компиляции не годится — для неё `go vet -tags integration ./...`
- `main_test.go`: `TestMaskURI`/`TestLogURI`/`TestMaskOptions` (`:23-98`), `clearEnv` (`:99`), `TestParseOpts*`, `TestStateDir*`, `TestProgressMode` (`:108-529`), `TestPermanent`/`TestIsAuthFailed` (`:530-582`), `TestWithPanelRestoresConsoleLog`/`TestReplicateSetsRetry` (`:583-626`); `restoreLogger` и `captureStdout` живут в `log_test.go` — общие, не переносятся
- остальные юнит-файлы ≤410 строк и соответствуют исходникам — не трогаются; `auth_integration_test.go` (пакет `main`) — не трогается
- в репозитории (вне `vendor` и `docs/plans/completed/`) нет ссылок на `integration_test.go` и его строки — только на имена хелперов и тестов, которые не меняются
- `.golangci.yml`: `build-tags: [integration]`, revive включён (правило `context-as-argument`), для тестов отключены только `dupl` и `gosec`
- команды: `make test`, `make test-integration`, `make lint`

## Development Approach
- **testing approach**: Regular; задача — перенос кода тестов, новых тестов нет, критерий — неизменный набор объявлений и строк и зелёные прогоны
- complete each task fully before moving to the next
- make small, focused changes
- **CRITICAL: every task MUST include new/updated tests** for code changes in that task — здесь это проверка, что набор объявлений, строк и тестов не изменился, и прогоны проходят
- **CRITICAL: all tests must pass before starting next task** - no exceptions
- **CRITICAL: update this plan file when scope changes during implementation**
- run `make test`, `make test-integration`, `make lint` after each task
- **ни одна строка тела теста или хелпера не меняется**: только перенос между файлами плюс заголовки новых файлов (`//go:build`, `package`, `import`)

## Testing Strategy
Эталон и скрипты — в `/tmp/split-tests/` (вне репозитория).

- **имена объявлений** — `/tmp/split-tests/decls.awk` (методы с получателем, элементы блоков `var (`/`const (`/`type (`, однострочные `const`):
  ```awk
  /^func \(/ { s=$0; sub(/^func \([A-Za-z0-9_]* *\*?/,"",s); r=s; sub(/[\[)].*/,"",r); sub(/^[^)]*\) */,"",s); sub(/[(\[].*/,"",s); print "func " r "." s; next }
  /^func /   { s=$2; sub(/[(\[].*/,"",s); print "func " s; next }
  /^(type|var|const) \(/ { blk=$1; next }
  /^(type|var|const) /   { s=$2; sub(/[^A-Za-z0-9_].*/,"",s); print $1 " " s; next }
  blk && /^\)/ { blk=""; next }
  blk && /^\t[A-Za-z_]/ { s=substr($0,2); match(s,/^[A-Za-z_][A-Za-z0-9_]*(, *[A-Za-z_][A-Za-z0-9_]*)*/); n=split(substr(s,RSTART,RLENGTH),a,/, */); for(i=1;i<=n;i++) print blk " " a[i] }
  ```
  по пакету: `awk -f /tmp/split-tests/decls.awk cmd/mongo-replicator/replicator/*_test.go | sort` и то же для `cmd/mongo-replicator/*_test.go` (сейчас 226 и 76 объявлений; 138 из 226 — в `integration_test.go`)
- **строки тел** — мультимножество строк без импортов, build-тегов, `package` и пустых строк; ловит изменённую строку и потерянный doc-комментарий:
  ```bash
  strip() { awk '/^import \(/{i=1;next} i&&/^\)$/{i=0;next} !i && !/^(\/\/go:build|package )/ && NF' "$@" | sort; }
  cd cmd/mongo-replicator/replicator
  diff <(strip /tmp/split-tests/integration_test.go) <(strip *_integration_test.go)
  ```
  для `main_test.go`: `diff <(strip /tmp/split-tests/main_test.go) <(strip {options,mask,errors,main}_test.go)` в `cmd/mongo-replicator`; `diff` должен быть пустым
- **список тестов**:
  - юнит — на хосте: `go test -list '.*' ./... | grep -E '^Test' | sort`
  - интеграционные — в контейнере, с проверкой статуса (упавший пакет даёт укороченный список, который легко принять за следствие переноса):
    ```bash
    docker compose run --rm app sh -c "go test -tags integration -list '.*' ./..." > /tmp/split-tests/list-it.raw \
      || { echo 'list failed'; cat /tmp/split-tests/list-it.raw; exit 1; }
    grep -E '^(Test|Benchmark|Example|Fuzz)' /tmp/split-tests/list-it.raw | sort > /tmp/split-tests/list-it.txt
    ```
- **быстрая проверка компиляции** без сервисов: `docker compose run --rm --no-deps app go vet -tags integration ./...`
- **после каждой задачи**: те же списки и `strip`-diff — пустые; `make test`, `make test-integration`, `make lint`

## Progress Tracking
- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix
- update plan if implementation deviates from original scope
- keep plan in sync with actual work done

## Solution Overview
- один пакет, файлы по областям: имя файла называет область, суффикс `_integration_test.go` — тег (`lease_test.go` рядом с `lease_integration_test.go` не путаются)
- правило для хелперов: нужен тестам ≥2 файлов → `harness_integration_test.go`; одной области → её файл. Раскладка ниже сверена с местами вызова. **Компилятор и линтер размещение не проверяют**: все файлы в одном пакете, хелпер «не в том» файле собирается и проходит `unused` — при переносе сверяться с таблицей
- объявление переносится вместе со своим doc-комментарием; тип — вместе со всеми методами
- порядок объявлений внутри файла — как в исходном
- импорты чистятся вручную (`goimports` на хосте нет и в `.golangci.yml` он не настроен); лишний или недостающий импорт отвергает компилятор
- порядок инициализации не меняется: межфайловых инициализаторов пакетных переменных нет, `TestMain` только присваивает

## Technical Details

### Раскладка интеграционных тестов
Все файлы: `//go:build integration`, `package replicator`. Префикс `TestIntegration` у тестов опущен.

| Файл | Тесты | Свои хелперы |
|---|---|---|
| `harness_integration_test.go` | `TestMain`, `TestSnake`, `StopOnCleanup` | константы и глобальные переменные; `populateProxies`, `connectIT`, `snake`, `srcOpTime`, `srcNow`, `dropDB`, `prepare`, `cleanDB`, `cleanDBs`, `newIT`, `release`, `start`, `startOn`, тип `runner` (`exited`, `waitExit`, `waitSame`, `waitSameDBs`, `waitQuiet`, `ckptToken`, `ckptPair`, `ckptTime`), `runErr`, `ckptOf`, `same`, `sameDBs`, `collNames`, `pairNS`, `diffDocs`, `diffIndexes`, `indexSig`, `renameColl`, `renameNS`, `indexNames`, `dbExists`, `captureLog`, тип `leaseState` + `readLease`, `writeCkpt`, `withState`, `insertPadded` |
| `dml_integration_test.go` | `DML`, `Transaction`, `Resume`, `ReplayIdempotent`, `NoStartPoint`, `WriteError` | — |
| `ddl_integration_test.go` | `DDL`, `DDLError`, `UnsupportedDDL`, `RestartAfterDDL`, `CrossDBRenameIn`, `CrossDBRenameOut` | — |
| `dbs_integration_test.go` | `SingleDB`, `DstDB`, `SingleDBResume`, `DBMismatch`, `DstDBMismatch` | — |
| `checkpoint_integration_test.go` | `CheckpointReadsPrimary`, `CheckpointInDstDB`, `CheckpointCollDropped`, `CheckpointCollFiltered`, `RenameReserved`, `TwoProcessesOneTarget`, `SaveTokenUnparsedTime` | — |
| `lease_integration_test.go` | `LeaseHeld`, `LeaseExpired`, `LeaseHeldNoToken`, `SaveTokenNotOwner`, `LeaseExpiringStopsWrites`, `LeaseLost`, `LeaseRerun`, `LeaseRenewDuringLongDDL` | — |
| `traffic_integration_test.go` | `Traffic`, `StateFile`, `StateFileRetryPause`, `StateFileContinues`, `StateFileDBMismatch`, `MachineTraffic` | `newTrafficIT`, `shortActive`, `readStateFile` |
| `drops_integration_test.go` | `DropDatabaseStops`, `DropStops`, `DropAbsentOnTarget`, `RenameAbsentOnTargetStatus`, `DropViewFromDump`, `FailedOut`, `ApplyDrop` | — |
| `lag_integration_test.go` | `LagPosition`, `SteadyTrafficBoundedLag`, `LargeUpdateEvent` | `insertAt` |
| `status_integration_test.go` | `Status`, `StatusRetry` | — |
| `limitations_integration_test.go` | `TestLimitationUpdateLookupAfterRename`, `TestLimitationRenameInCatchUpWindow` | `limitationSkip` вместе с комментарием над ним (`:2510-2511`, описывает оба теста) |
| `network_integration_test.go` | `NetProxy`, `NetSrcDown`, `NetDstDown`, `NetSrcHang`, `NetDstHangLease`, `NetTraffic` | типы `netEnv` (метод `newIT`) и `netCase`, `resetProxies`, `netPause`, тип `retrier` (`runErrs`, `waitErr`, `waitSame`), `startRetry`, тип `srcWriter` (`times`), `startWriter`, `waitApplied`, `hang`, `ckptBehind` |

- общие хелперы, перешедшие в `harness` по местам вызова: `readLease`/`leaseState` (dml, checkpoint, lease, network), `withState` (lease, status, traffic, network), `writeCkpt` (dbs, traffic, lease), `insertPadded` (traffic, network); `insertAt` нужен только `lag` — в её файле
- если при переносе обнаружится ещё одно объявление вне таблицы или хелпер с вызовами из другой области — разместить по правилу и ➕ отметить в плане
- `integration_test.go` в итоге удаляется

### Раскладка `main_test.go` (код `main.go` не меняется)
| Файл | Тесты и хелперы |
|---|---|
| `options_test.go` | `clearEnv`, `TestParseOptsEnv`, `TestStateDir`, `TestStateDirIsFile`, `TestParseOptsDefaults`, `TestParseOptsFlagsOverrideEnv`, `TestParseOptsProgress`, `TestProgressMode`, `TestParseOptsLog`, `TestParseOptsLogErrors`, `TestParseOptsDB`, `TestParseOptsDstDB`, `TestParseOptsApplyDrop`, `TestParseOptsApplyDropErrors`, `TestParseOptsErrors`, `TestParseOptsDBName`, `TestParseOptsHelp` |
| `mask_test.go` | `TestMaskURI`, `TestLogURI`, `TestMaskOptions` |
| `errors_test.go` | `TestPermanent`, `TestIsAuthFailed` |
| `main_test.go` | `TestWithPanelRestoresConsoleLog`, `TestReplicateSetsRetry` |
- `restoreLogger` и `captureStdout` остаются в `log_test.go`

### Раздел «Тесты» в `CLAUDE.md`
Правила для **новых** тестов и хелперов; существующие при правке не переписываются и не переименовываются:
- **раскладка**: юнит — `<file>_test.go` рядом с `<file>.go`; интеграционный — `<область>_integration_test.go` с тегом `integration`; хелпер, нужный ≥2 файлам, — `harness_integration_test.go`, хелпер одной области — в её файле (компилятор размещение не проверяет); новый тест — в файл своей области, новая область — новый файл
- **структура**: «дано → действие → проверка», блоки через пустую строку; без логики в теле, кроме перебора таблицы; ожидания — литералы
- **проверки**: новые проверки — testify: `require` (продолжать бессмысленно) / `assert` (независимые проверки); `assert.Equal(t, want, got, "контекст")`; ошибки — `ErrorIs`, текст — только когда важен сам текст (подсказки оператору). Правка теста не обязывает переписывать его остальные проверки
- **табличные тесты** — когда случаи отличаются только данными; флаги и `setup`-функции в таблице → отдельные тесты
- **хелперы**: `t.Helper()` первой строкой; `t` — первым аргументом, после `ctx`, если он есть (revive `context-as-argument`); новый хелпер, который может завалить тест, называется `require…`/`assert…` (существующие `prepare`, `srcNow`, `readLease` и т. п. имён не меняют)
- **время**: `require.Eventually` с сообщением вместо `time.Sleep`; сон — только для серверного времени (аренда, `$$NOW`); логика по «сейчас» получает время параметром
- **изоляция**: `t.TempDir`, `t.Setenv`, `t.Cleanup`; тест не зависит от порядка запуска. Исключение намеренное: `lastSrcSec` переносит между тестами время последней операции обвязки, чтобы `srcNow` не захватил хвост предыдущего теста — не «чинить»
- **имена**: интеграционные — `TestIntegration<Что>`, область задаёт имя файла и в имени теста не обязательна; тесты известных ограничений — `TestLimitation<Что>` (на них ссылается README); подтест называет случай, а не номер
- в разделы «Интеграционные тесты» и «Сетевые тесты (Toxiproxy)» — указатели: обвязка (`newIT`, `start`, `withState`, `populateProxies`…) — в `harness_integration_test.go`, сетевые хелперы (`netCase`, `hang`…) — в `network_integration_test.go`

## What Goes Where
- **Implementation Steps**: перенос тестов, `CLAUDE.md`
- **Post-Completion**: нет внешних действий

## Implementation Steps

### Task 1: Эталон набора тестов

**Files:**
- нет изменений в репозитории (эталон — в `/tmp/split-tests/`)

- [x] `mkdir -p /tmp/split-tests`; скопировать `integration_test.go` и `main_test.go` в `/tmp/split-tests/` (база для `strip`-diff)
- [x] сохранить `decls.awk` из Testing Strategy; снять имена объявлений по обоим пакетам в `/tmp/split-tests/decls-{replicator,main}.txt`
- [x] снять список юнит-тестов на хосте и интеграционных в контейнере `app` (с зависимостями, с проверкой статуса) — `/tmp/split-tests/list-{unit,it}.txt`
- [x] `make test`, `make test-integration`, `make lint` — исходное состояние зелёное
- ⚠️ эталон: юнит-тестов 128, в интеграционном списке 189 (вместе с юнит), SKIP в прогоне нет

### Task 2: Разделение интеграционных тестов

**Files:**
- Create: `cmd/mongo-replicator/replicator/harness_integration_test.go`
- Create: `cmd/mongo-replicator/replicator/{dml,ddl,dbs,checkpoint,lease,traffic,drops,lag,status,limitations,network}_integration_test.go`
- Delete: `cmd/mongo-replicator/replicator/integration_test.go`

- [x] перенести объявления по таблице «Раскладка интеграционных тестов» без изменения тел, с doc-комментариями, типы — со всеми методами; порядок внутри файла — как в исходном
- [x] в каждом файле — `//go:build integration`, `package replicator`, только нужные импорты
- [x] удалить `integration_test.go`
- [x] проверка: `go vet -tags integration` в контейнере; имена объявлений и список тестов (с `-tags integration`) совпадают с эталоном; `strip`-diff с `/tmp/split-tests/integration_test.go` пустой
- [x] `make test-integration`, `make lint` — должно пройти
- [x] отдельный коммит; в сообщении — `git blame -C -C` для истории перенесённых строк

### Task 3: Разделение `main_test.go`

**Files:**
- Create: `cmd/mongo-replicator/options_test.go`
- Create: `cmd/mongo-replicator/mask_test.go`
- Create: `cmd/mongo-replicator/errors_test.go`
- Modify: `cmd/mongo-replicator/main_test.go`

- [x] перенести тесты по таблице «Раскладка `main_test.go`» без изменения тел, с doc-комментариями; `restoreLogger` и `captureStdout` остаются в `log_test.go`
- [x] в каждом файле — только нужные импорты
- [x] проверка: имена объявлений и список юнит-тестов совпадают с эталоном; `strip`-diff с `/tmp/split-tests/main_test.go` пустой
- [x] `make test`, `make lint` — должно пройти
- [x] отдельный коммит

### Task 4: Раздел «Тесты» в `CLAUDE.md`

**Files:**
- Modify: `CLAUDE.md`

- [x] добавить раздел «Тесты» по Technical Details
- [x] в разделы «Интеграционные тесты» и «Сетевые тесты (Toxiproxy)» добавить указатели на `harness_integration_test.go` и `network_integration_test.go`
- [x] `grep -rn 'integration_test\.go' --exclude-dir=vendor --exclude-dir=completed .` — нет ссылок на старый файл (совпадения только в этом плане)
- [x] отдельный коммит

### Task 5: Verify acceptance criteria
- [ ] `integration_test.go` нет; самый большой тестовый файл — заметно меньше прежних 3124 строк
- [ ] имена объявлений, списки тестов и `strip`-diff совпадают с эталоном Task 1 (оба пакета)
- [ ] `git diff --stat` от начала работ — только тестовые файлы, `CLAUDE.md` и этот план
- [ ] run full test suite: `make test`
- [ ] run integration tests: `make test-integration`
- [ ] `make lint` без замечаний

### Task 6: [Final] Update documentation
- [ ] README.md — ссылок на `integration_test.go` нет (проверено в Task 4), изменений не ожидается
- [ ] move this plan to `docs/plans/completed/`

## Post-Completion
*Items requiring manual intervention or external systems - no checkboxes, informational only*

- внешних действий нет; история строк перенесённых тестов — `git blame -C -C <файл>`
