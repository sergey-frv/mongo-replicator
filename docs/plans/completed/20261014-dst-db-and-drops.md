# `--dst-db`, только режим одной базы, остановка на drop

## Overview
- **`--dst-db`**: база на цели может называться иначе, чем на источнике — сценарии «база на цели называется иначе» (`urlshrinker` → `urlshrinker_v2`) и «копия рядом с оригиналом» на целевом кластере. На целевой кластер пишет один репликатор → чекпойнт по-прежнему один (`_id: "main"`)
- **режим всех баз удаляется**: `--db` обязательна, поток только уровня базы; `--all-dbs`/`ALL_DBS` и поток уровня кластера уходят
- **удаления не применяются**: `drop` коллекции, которая есть на цели, и `dropDatabase` на источнике останавливают репликатор (exit 1) — такие операции не предусматриваются (решение пользователя). Продолжить после осознанного удаления — `--apply-drop <ns>` / `APPLY_DROP`
- `drop` коллекции, которой на цели нет (view, time-series — их репликатор не переносит), — WARN и дальше: терять нечего (решение пользователя на ревью плана)
- тулза ещё не запускалась на реальных базах: обратная совместимость с чекпойнтами и опциями не нужна

## Context (from discovery)
- `cmd/mongo-replicator/main.go`: `options` (`Src`, `Dst`, `StartTS`, `BatchSize`, `Debug`, `DB`, `AllDBs`), правило «ровно одно из `--db`/`--all-dbs`»; `checkDBName` (сообщение жёстко про `--db`, `maxDBNameLen` 63); `permanent(err) = hint(err) != ""`, `hint` по `IsHistoryLost`/`ErrNoStartPoint`/`ErrDBMismatch`/`ErrStreamInvalidated` (подсказка `ErrDBMismatch` упоминает `--db/--all-dbs`, `:194`); `run` → `replicator.New(src, dst, Config{StartTS, BatchSize, DB})`
- `cmd/mongo-replicator/main_test.go`: `clearEnv` (`:96`) чистит в т.ч. `ALL_DBS`; `TestParseOptsEnv`/`Defaults`/`FlagsOverrideEnv` используют `--all-dbs`/`ALL_DBS` и сравнивают структуру `options`; `TestParseOptsMode`; `TestParseOptsHelp` ищет `--all-dbs`
- `cmd/mongo-replicator/credentials.go` / `credentials_test.go`: логика не меняется; `TestFlagGiven` (`:61,64`) использует `--all-dbs` как токен
- `cmd/mongo-replicator/replicator/replicator.go`: `Config{StartTS, BatchSize, DB}` (`""` = все базы); `Run`: `loadCheckpoint` → `checkDB(saved, want)` → `ErrDBMismatch`; `streamPipeline(db)` (`$nin SystemDBs`); `src.Watch` или `src.Database(DB).Watch`; `invalidate` → `ErrStreamInvalidated` (комментарий про «применённый dropDatabase», `:183`); `flush`: `r.dst.Database(ns.DB)` (`:76`)
- `cmd/mongo-replicator/replicator/checkpoint.go`: `checkpoint{Token, DB "db", ClusterTime}`, `_id: "main"`
- `cmd/mongo-replicator/replicator/ddl.go`: `db := r.dst.Database(ev.NS.DB)` (`:31`) — `create`/`createIndexes`/`dropIndexes`/`drop`/`dropDatabase`; rename — `runCommand` через `admin` с `ev.NS.String()`/`ev.To.String()` (`:45-47`); логи DDL с полями `op`/`ns`/`to`
- `cmd/mongo-replicator/replicator/lag.go` — пишет только в `admin` источника; не меняется. Чекпойнт — фиксированная база `_replicator`. Других мест, где база цели выводится из `ev.NS`, нет (проверено ревью)
- `integration_test.go`: `startOn` регистрирует `stop()`, который требует `context.Canceled` (`:203`) — тесты, ожидающие остановку `Run`, только через `runErr`; хелперы `prepare`, `cleanDB`, `waitSame(db)`, `same(db)`, `diffDocs`/`diffIndexes` (одноимённые базы), `ckptDB()`; ~34 литерала `Config{...}`, ~22 без `DB`; прямые `New(...)` в `TestIntegrationCheckpointReadsPrimary`, `TestIntegrationSaveTokenUnparsedTime`
  - `TestIntegrationDDL` (`:866-970`): пишет в базу `other`, `waitSame(other)` (`:954`), `dropDatabase other`, create+drop коллекции `"tmp"` (`:945-947`), rename `a→b` поверх существующей `b`
  - `TestIntegrationDBMismatch` (`:1063-1097`): случаи `""`/все базы и «чекпойнт без db»
  - `TestIntegrationIdleCheckpoint` (`:803`): фильтрация событий `_replicator` на источнике — смысл только для потока уровня кластера
  - `TestIntegrationReplayIdempotent` (`:656`): `src.Collection("gone").Drop`
  - `TestIntegrationDropDatabaseInvalidates`, `TestIntegrationSingleDBCrossDBRename` — применение `drop`/`dropDatabase`
- поведение потока уровня базы на 8.0 (проверено ревью плана на `mongo-src`):
  - rename `a→b` при существующей `b` — **одно** событие `rename` с `operationDescription.dropTarget`, без отдельного `drop`
  - `$out` (новый и с заменой) и `mapReduce` — через `tmp.agg_out.<uuid>` + `rename`; `drop` нет. **Неудачный `$out` даёт `drop tmp.agg_out.<uuid>`.** `tmp.mr.*` на 8.0 не используется
  - `convertToCapped` — `tmpXXXXX.convertToCapped.<coll>` + `rename … dropTarget`
  - `drop` view и time-series коллекции даёт событие `drop` (для `system.buckets.*` — ничего); `system.js` — ничего; `drop` несуществующей — ничего
  - `dropDatabase` — `drop` по каждой коллекции в произвольном порядке, затем `dropDatabase`, затем `invalidate`
  - перенос коллекции из базы наружу — только `drop`; внутрь — `create tmpXXXXX.renameCollection` + `createIndexes` + `insert` + `rename` (с `dropTarget`, если цель есть)
- команды: `make test`, `make test-integration`, `make lint`, `make build`

## Development Approach
- **testing approach**: Regular (код, затем тесты в той же задаче)
- перед Task 1 создать ветку от `master` (например, `dst-db-and-drops`)
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
- run `make test`, `make test-integration`, `make lint` after each task
- обратная совместимость **не нужна**: ни fallback для чекпойнтов без новых полей, ни поддержки удалённых опций

## Testing Strategy
- **unit tests**: `make test` — `parseOpts`, `New` (значения по умолчанию), `checkDBs`, `checkNS`, `isTempColl`, `hint`/`permanent`
- **integration tests**: `make test-integration` — все тесты на `Config{DB, ...}`; ожидаемые остановки — только через `runErr` (сначала все операции на источнике, потом `runErr`); сравнение источника и цели — по паре имён баз
- e2e UI-тестов в проекте нет

## Progress Tracking
- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix
- update plan if implementation deviates from original scope
- keep plan in sync with actual work done

## Solution Overview
- поток — только `src.Database(cfg.DB).Watch`; всё, что пишется на цель, идёт в `cfg.DstDB` (DML, DDL, обе стороны rename)
- события другой базы в потоке уровня базы не ожидаются (подтверждено пробой); дешёвая проверка `checkNS` — перманентная ошибка вместо записи не в ту базу
- чекпойнт хранит пару `db` → `dst_db`; любое расхождение с опциями — отказ стартовать
- `drop`: временная коллекция MongoDB → применить; разрешена `--apply-drop` → применить; нет на цели → WARN и дальше; иначе — остановка. `dropDatabase` → всегда остановка
- порядок `commit()` → DDL → `saveToken` не меняется: к остановке всё до удаления уже на цели, чекпойнт стоит перед удалением, рестарт останавливается на нём же

## Technical Details

**Опции**
```go
DB         string   `long:"db" env:"DB" required:"true" description:"база источника; поток уровня базы, права на источнике — только на неё"`
DstDB      string   `long:"dst-db" env:"DST_DB" description:"база на цели; по умолчанию — как --db"`
ApplyDrops []string `long:"apply-drop" env:"APPLY_DROP" env-delim:"," description:"разрешить drop коллекции источника <db>.<коллекция> (одноразово; можно повторять; флаг заменяет APPLY_DROP, а не дополняет)"`
```
- `--all-dbs`/`ALL_DBS` и правило «ровно одно из» удаляются
- `parseOpts` (exit 2):
  - `--db` пустая (`DB=` удовлетворяет `required` в go-flags — явная проверка нужна)
  - `checkDBName(opt, name)` для `--db` и `--dst-db` с именем опции в сообщении
  - `--dst-db` пустая → `= --db`
  - `ApplyDrops`: элементы `strings.TrimSpace`, пустые выбрасываются (`APPLY_DROP=` → пустой список); каждый: формат `<db>.<коллекция>` (разделение по первой `.`), `db == --db`, коллекция непустая
- при непустом списке разрешённых drop — WARN в `main` после `parseOpts`: `разрешены drop: … (уберите из APPLY_DROP после применения)`

**Config / New**
```go
type Config struct {
    StartTS    uint32
    BatchSize  int
    DB         string   // база источника
    DstDB      string   // база цели; New: пусто → = DB
    ApplyDrops []string // namespace источника (<DB>.<коллекция>), drop которых разрешён
}
```
- `New` подставляет `DstDB = DB`, если пусто → литералы `Config{DB: db}` в тестах остаются корректными
- `Run`: пустой `DB` → ошибка (защита от неправильного вызова пакета)

**Сопоставление**
- `flush`: `r.dst.Database(r.cfg.DstDB)`; текст ошибки — ns источника и, при `DstDB != DB`, ns цели
- `applyDDL`: `db := r.dst.Database(r.cfg.DstDB)`; rename: `<DstDB>.<ev.NS.Coll>` → `<DstDB>.<ev.To.Coll>`
- `checkNS(cfg, ev) error`: `ev.NS.DB != cfg.DB` или `ev.To != nil && ev.To.DB != cfg.DB` → `ErrForeignNamespace` (перманентная); вызывается для каждого события
- логи DDL: `ns` — источник; при `DstDB != DB` — поле `dst` с namespace на цели

**Чекпойнт**
```js
{_id: "main", token, clusterTime, updatedAt, db: "urlshrinker", dst_db: "urlshrinker_v2"}
```
- `checkDBs(saved, want dbPair) error`: пары не равны → `fmt.Errorf("%w: в чекпойнте %s → %s, задано %s → %s", ErrDBMismatch, ...)`; без fallback
- тестовый хелпер `ckptDB()` возвращает пару

**Удаления** (`applyDDL`, ветки `drop` / `dropDatabase`)
| событие | действие |
|---|---|
| `drop` временной коллекции (`isTempColl`) | применяется молча |
| `drop` коллекции из разрешённых | применяется на `<DstDB>.<coll>`, INFO `drop применён по --apply-drop`; вычёркивается **после** успешного `saveToken` этого события |
| `drop` коллекции, которой нет на цели (`ListCollectionNames` в `DstDB` с фильтром по имени) | не применяется, WARN `drop <ns>: на цели такой коллекции нет (view/time-series или уже удалена)`, дальше |
| `drop` прочей коллекции | не применяется, `*DropError{NS}` |
| `dropDatabase` | не применяется, `*DropError{NS, Database: true}` (никогда не разрешается) |
| `invalidate` | `ErrStreamInvalidated`, как сейчас |

- `isTempColl(name string) bool`: точные шаблоны, не префиксы — `^tmp\.agg_out\.<UUID>$` (пользовательская `tmp.agg_out.archive` не временная), `^tmp[A-Za-z0-9]{5}\.renameCollection$`, `^tmp[A-Za-z0-9]{5}\.convertToCapped\.(?s:.+)$` (непустой суффикс, включая `\n` — имя коллекции может его содержать); `tmp.mr.*` не исключается — на источниках 6.0+ mapReduce идёт через агрегацию; не префикс `tmp` целиком (`tmp.backup` — пользовательская)
- разрешённые drop: `map[string]bool` в `Replicator`; при применении drop запоминается «ожидает вычёркивания» вместе с токеном события; вычёркивание — после успешного `saveToken` с этим токеном. Повтор `Run` после сбоя `saveToken` снова видит тот же `drop` и снова применяет его (идемпотентно: drop несуществующей — код 26, уже игнорируется)
- ошибка — тип `*DropError{NS string; Database bool}`; `permanent`/`hint` через `errors.As` (без отдельного sentinel)
- `hint`:
  - коллекция: «на источнике удалена коллекция <ns> (вручную, перенос в другую базу или dropDatabase); если удаление ожидаемое — перезапустите с --apply-drop <ns>»
  - база: «база удалена на источнике; на цели данные остались — нужна новая полная заливка: удалите чекпойнт, залейте базу, запустите с --start-ts»
  - `ErrForeignNamespace`: «в потоке событие другой базы — сообщите о баге»
  - `ErrDBMismatch`: про пару `--db`/`--dst-db` (не `--all-dbs`)

**Права**
- источник: `read` на `--db`
- цель: `readWrite` на `--dst-db` и на `_replicator` (`listCollections` входит; право `dropDatabase` больше не нужно)

**Make**
- `make run`: `--db test` вместо `--all-dbs`; комментарий к `-p 1` («поток по кластеру») поправить

## What Goes Where
- **Implementation Steps**: код, юнит- и интеграционные тесты, Makefile, README
- **Post-Completion**: ручные проверки на реальных кластерах

## Implementation Steps

### Task 1: Только режим одной базы

**Files:**
- Modify: `cmd/mongo-replicator/main.go`
- Modify: `cmd/mongo-replicator/main_test.go`
- Modify: `cmd/mongo-replicator/credentials_test.go`
- Modify: `cmd/mongo-replicator/replicator/replicator.go`
- Modify: `cmd/mongo-replicator/replicator/checkpoint.go`
- Modify: `cmd/mongo-replicator/replicator/replicator_test.go`
- Modify: `cmd/mongo-replicator/replicator/integration_test.go`
- Modify: `Makefile`, `docker-compose.yml` (если там `ALL_DBS`)

- [x] `--db` обязательна; удалить `AllDBs`, правило «ровно одно из», ветку потока уровня кластера и `$nin`-pipeline (`streamPipeline`); `Run` всегда `src.Database(cfg.DB).Watch`, пустой `DB` → ошибка
- [x] устаревшие тексты: подсказка `ErrDBMismatch` без `--all-dbs`; комментарии `Config.DB`/`checkpoint.DB` («"" — все базы»); `Makefile` `run` → `--db test`, комментарий к `-p 1`
- [x] `main_test.go`: `clearEnv` без `ALL_DBS`; `TestParseOptsEnv`/`Defaults`/`FlagsOverrideEnv` — на `--db`, ожидаемые `options` с `DB`; удалить/переписать `TestParseOptsMode`; `TestParseOptsHelp` — без `--all-dbs`; новые случаи: без `--db` → ошибка, `DB=` → ошибка, `--all-dbs` → неизвестная опция
- [x] `credentials_test.go` `TestFlagGiven`: заменить токен `--all-dbs` на существующий
- [x] `replicator_test.go`: удалить тесты `streamPipeline`
- [x] интеграционные: все `Config{...}` без `DB` → `Config{DB: db, ...}`; `TestIntegrationDDL` — убрать часть с базой `other` (`waitSame(other)`, `dropDatabase other`); `TestIntegrationDBMismatch` — убрать случаи `""`/все базы и «чекпойнт без db»; `TestIntegrationIdleCheckpoint` — адаптировать к потоку уровня базы (позиция двигается от записей в другую базу) или удалить, если дублирует `TestIntegrationLagPosition` (удалён: дублирует `LagPosition`)
- [x] `make test`, `make test-integration`, `make lint` — должно пройти

### Task 2: `--dst-db` и сопоставление на цели

**Files:**
- Modify: `cmd/mongo-replicator/main.go`
- Modify: `cmd/mongo-replicator/main_test.go`
- Modify: `cmd/mongo-replicator/replicator/replicator.go`
- Modify: `cmd/mongo-replicator/replicator/ddl.go`
- Modify: `cmd/mongo-replicator/replicator/replicator_test.go`
- Modify: `cmd/mongo-replicator/replicator/integration_test.go`

- [x] опция `DstDB` (`DST_DB`), `checkDBName(opt, name)` с именем опции; `Config.DstDB`, значение по умолчанию в `New`
- [x] `flush`, `applyDDL`, rename — на `cfg.DstDB`; текст ошибок с ns цели при `DstDB != DB`; поле `dst` в логах DDL
- [x] `checkNS` + `ErrForeignNamespace` (вызов для каждого события в `Run`), добавить в `permanent`/`hint`
- [x] хелперы теста: `sameDBs(srcDB, dstDB)`, `runner.waitSameDBs(srcDB, dstDB)`; `same(db)`/`waitSame(db)` — обёртки; `diffDocs`/`diffIndexes` — с парой имён
- [x] write tests `parseOpts`: `--dst-db` не задана → `= --db`; `DST_DB` из env; невалидное имя → ошибка с `--dst-db` в тексте; `clearEnv` + `DST_DB`
- [x] write unit tests: `New` — пустой `DstDB` → `= DB`; `checkNS` — своя база, чужая `ns`, чужая `to`, `to == nil`
- [x] write integration test `TestIntegrationDstDB`: `cleanDB(t, db+"_v2")`; `Config{DB: db, DstDB: db+"_v2"}`; DML, `create`, `createIndexes`, `dropIndexes`, rename внутри базы, rename поверх существующей коллекции (`dropTarget`); `waitSameDBs(db, db+"_v2")`; на цели нет базы `db`
- [x] `make test`, `make test-integration`, `make lint` — должно пройти

### Task 3: Пара баз в чекпойнте

**Files:**
- Modify: `cmd/mongo-replicator/replicator/checkpoint.go`
- Modify: `cmd/mongo-replicator/replicator/replicator.go`
- Modify: `cmd/mongo-replicator/replicator/replicator_test.go`
- Modify: `cmd/mongo-replicator/replicator/integration_test.go`
- Modify: `cmd/mongo-replicator/main.go`

- [x] поле `dst_db` в чекпойнте (`saveToken` пишет `cfg.DstDB`); `checkDB` → `checkDBs(saved, want dbPair)`; подсказка `ErrDBMismatch` про пару опций
- [x] `ckptDB()` в тестах возвращает пару; обновить `SingleDBResume`, `SaveTokenUnparsedTime`
- [x] write unit tests `checkDBs`: совпадение; другая база источника; другая база цели; текст содержит обе пары
- [x] update `TestIntegrationDBMismatch` под пару; write `TestIntegrationDstDBMismatch`: чекпойнт `a → a2` записан напрямую, `runErr(Config{DB: a, DstDB: a3})` → `ErrDBMismatch`
- [x] `make test`, `make test-integration`, `make lint` — должно пройти

### Task 4: Удаления: остановка, исключения, `--apply-drop`

**Files:**
- Modify: `cmd/mongo-replicator/main.go`
- Modify: `cmd/mongo-replicator/main_test.go`
- Modify: `cmd/mongo-replicator/replicator/ddl.go`
- Modify: `cmd/mongo-replicator/replicator/replicator.go`
- Modify: `cmd/mongo-replicator/replicator/replicator_test.go`
- Modify: `cmd/mongo-replicator/replicator/integration_test.go`

- [x] `*DropError`, `isTempColl`; ветки `drop`/`dropDatabase` в `applyDDL` по таблице «Удаления» (временная → применить; разрешённая → применить; нет на цели → WARN; иначе ошибка; `dropDatabase` → ошибка)
- [x] опция `ApplyDrops` (`APPLY_DROP`, trim, пустые выбрасываются, проверки формата и базы); WARN в `main` при непустом списке; `Config.ApplyDrops`; множество разрешённых в `Replicator`, вычёркивание после успешного `saveToken`
- [x] `permanent`/`hint` для `*DropError` (коллекция / база); обновить комментарий `invalidate` в `Run`
- [x] write unit tests `isTempColl`: `tmp.agg_out.<UUID>`, `tmpAb12C.renameCollection`, `tmpAb12C.convertToCapped.c` → true; `tmp.agg_out.archive`, `tmp.agg_out.<неполный UUID>`, `tmp.mr.x`, `tmpAb12C.convertToCapped.`, `tmp.backup`, `tmp`, `tmpAb12C.other`, `tmpAb12.renameCollection` → false
- [x] write unit tests `parseOpts`: `--apply-drop db.a --apply-drop db.b`; `APPLY_DROP=db.a,db.b`; `APPLY_DROP=` → пустой список, не ошибка; `APPLY_DROP="db.a, db.b"` → пробел срезан; флаг заменяет env; `other.a` при `--db db` → ошибка; `db.`, `db` → ошибка; `db.a.b` → коллекция `a.b`; `clearEnv` + `APPLY_DROP`
- [x] write unit tests `hint`/`permanent`: обёрнутые `*DropError` (коллекция и база), `ErrForeignNamespace`, `ErrStreamInvalidated` → permanent; подсказка коллекции содержит `--apply-drop <ns>`, базы — «новая полная заливка»
- [x] update интеграционных тестов: `TestIntegrationDDL` — убрать create+drop коллекции `"tmp"`, оставить rename поверх существующей (`dropTarget`) и добавить проверку, что `Run` не завершился; `TestIntegrationReplayIdempotent` — `ApplyDrops: ["gone"]` в обоих прогонах; `TestIntegrationSingleDBCrossDBRename` — разделить (`TestIntegrationCrossDBRenameIn` / `TestIntegrationCrossDBRenameOut`): перенос внутрь (в т.ч. поверх существующей) продолжает работу, перенос наружу → остановка; `TestIntegrationDropDatabaseInvalidates` — заменить на `TestIntegrationDropDatabaseStops`
- [x] write integration tests (все остановки — через `runErr` после операций на источнике; `DstDB: db+"_v2"` там, где проверяется цель):
  - `TestIntegrationDropStops`: DML, затем `drop` коллекции → `*DropError`; коллекция на цели цела, всё до `drop` на цели; повторный `runErr` → снова `*DropError`
  - `TestIntegrationDropAbsentOnTarget`: view на источнике (не переносится) → `drop` view → репликация идёт дальше (последующий DML на цели)
  - `TestIntegrationFailedOut` (плюс удачный `$out` перед неудачным): unique-индекс на целевой коллекции `$out`, дубликаты → `$out` падает → репликация идёт дальше, `tmp.agg_out.*` на цели нет
  - `TestIntegrationApplyDrop`: `ApplyDrops: [coll]`, `DstDB` отличается; `drop` → коллекция удалена в базе цели, последующий DML доезжает; пересоздание и повторный `drop` той же коллекции → `runErr` → `*DropError`; база с именем источника на цели не появилась
  - `TestIntegrationDropDatabaseStops`: две коллекции, `DstDB` отличается; без разрешений — остановка на первом `drop`, обе на цели целы; с разрешением обеих — обе удалены на цели, остановка на `dropDatabase` (`Database: true`)
- [x] `make test`, `make test-integration`, `make lint` — должно пройти

### Task 5: README

**Files:**
- Modify: `README.md`

- [x] таблица опций: `--db` обязательна, `--dst-db`/`DST_DB`, `--apply-drop`/`APPLY_DROP` (флаг заменяет env); убрать `--all-dbs`/`ALL_DBS`, режим всех баз и поток уровня кластера
- [x] права: источник — `read` на базу; цель — `readWrite` на базу цели и `_replicator`
- [x] `--dst-db`: пример для «копии рядом с оригиналом»; заливка дампа под другим именем — `mongorestore --nsFrom 'urlshrinker.*' --nsTo 'urlshrinker_v2.*'`
- [x] «Ограничения»: `drop` коллекции, которая есть на цели, перенос коллекции в другую базу и `dropDatabase` останавливают репликатор; почему `dropDatabase` ловится на первом `drop` (порядок событий); исключения — временные коллекции MongoDB и коллекции, которых нет на цели (view, time-series); каждый `$out` создаёт на цели `tmp.agg_out.*` и пишет WARN «создана без опций источника» — это шум, не ошибка
- [x] «После остановки на drop»: `--apply-drop <ns>` — ns **источника**, даже при другом `--dst-db`; совет убрать коллекцию из `APPLY_DROP` после применения; `dropDatabase` — только новая заливка
- [x] поправить устаревшее: список применяемого DDL во вступлении (`:7`), абзац про `make run` и чекпойнт `it_*` (`:162`)

### Task 6: Verify acceptance criteria
- [x] `--db` обязательна, `--all-dbs` нет (`grep` по репозиторию); поток только уровня базы
- [x] с `--dst-db` всё пишется в базу цели, имя базы источника на цели не появляется
- [x] `drop` существующей на цели коллекции и `dropDatabase` → exit 1 с подсказкой; `--apply-drop` разрешает ровно один drop указанной коллекции и переживает повтор `Run`
- [x] run full test suite: `make test`
- [x] run integration tests: `make test-integration`
- [x] `make lint` без замечаний

### Task 7: [Final] Update documentation
- [x] создать CLAUDE.md, только если появились неочевидные договорённости (не создан: неочевидное — исключения для drop в `isTempColl`, `checkNS` вместо записи не в ту базу, сверка пары баз в `checkDBs`, порядок `commit()` → DDL → `saveToken` — описано комментариями в коде; `--apply-drop` по ns источника и ловля `dropDatabase` на первом `drop` — в README)
- [x] move this plan to `docs/plans/completed/`

## Post-Completion
*Items requiring manual intervention or external systems - no checkboxes, informational only*

**Manual verification**:
- реальный источник (`readWrite@urlshrinker`) → цель с `--dst-db`: данные в базе цели, имя базы источника на цели не появилось
- осознанный `drop` коллекции на источнике → остановка с подсказкой → перезапуск с `--apply-drop` → коллекция удалена на цели, репликация продолжается
