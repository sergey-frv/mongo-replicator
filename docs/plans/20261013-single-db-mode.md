# Режим одной базы (`--db`) и явный `--all-dbs`

## Overview
- сейчас репликатор всегда открывает change stream уровня кластера (`src.Watch`, `allChangesForCluster`); для этого пользователю источника нужны `find` + `changeStream` на всех базах (`readAnyDatabase`)
- на реальном источнике у пользователя только `readWrite` на базу `urlshrinker`: поток уровня кластера падает с кодом 13, поток уровня базы (`db.Watch`) работает
- добавляем опцию `--db`: с ней репликатор открывает поток уровня одной базы и реплицирует только её
- режим всех баз включается только явно, флагом `--all-dbs`: нужно указать ровно одно из `--db X` / `--all-dbs`, иначе exit 2 — чтобы никто случайно не запустил репликацию всех баз (решение пользователя на ревью плана)
- списки баз не делаем (YAGNI, решение пользователя)
- rename коллекций внутри базы поддерживается; перенос коллекций между базами не предусмотрен (решение пользователя) — описывается в `--help` и README, без отдельной обработки в коде
- перманентные ситуации режима одной базы — смена базы поверх чекпойнта и `dropDatabase` базы — явный выход с кодом 1 вместо тихого расхождения или вечных ретраев
- лаг репликации считается заново и честно: от позиции, **применённой** на цели (сохранённый чекпойнт), до текущего времени кластера **источника**; лог прогресса раз в 10 с с лагом, скоростью и временем применённой позиции. Сейчас `lag = time.Since(lastTS)` — от последнего **прочитанного** события по локальным часам: растёт на простаивающем источнике, не учитывает непримененную пачку и зависит от расхождения часов

## Context (from discovery)
- `cmd/mongo-replicator/main.go`: go-flags `options` (Src/Dst/StartTS/BatchSize/Debug), `parseOpts`, `maskURI`, `run` с циклом ретраев; перманентные ошибки → exit 1: `replicator.IsHistoryLost`, `errors.Is(err, replicator.ErrNoStartPoint)` (`main.go:117-123`)
- `cmd/mongo-replicator/main_test.go`: `clearEnv` (`:37-43`) чистит `SRC_URI`, `DST_URI`, `START_TS`, `BATCH_SIZE`, `DEBUG`
- `cmd/mongo-replicator/replicator/replicator.go`: `Config{StartTS, BatchSize}`, `Run`: `loadToken` → `resumeAfter` / `startAtOperationTime` / `ErrNoStartPoint`; pipeline `$match {"ns.db": {$nin: [admin, config, local, _replicator]}}`; `r.src.Watch`; цикл событий: DML → batch, `default` → `commit()` + `applyDDL` + `saveToken` (`:132-145`)
- `cmd/mongo-replicator/replicator/checkpoint.go`: `_replicator.checkpoint {_id: "main", token, clusterTime, updatedAt}`, коллекция с `readpref.Primary()`
- лаг сейчас: `replicator.go:164-168` — `time.Since(time.Unix(lastTS))`, `lastTS` = clusterTime последнего **прочитанного** события; `checkpoint.go:50` пишет его же в `clusterTime` чекпойнта
- `cmd/mongo-replicator/replicator/ddl.go`: `applyDDL` (drop, dropDatabase, rename через `runCommand` с w:majority, create, createIndexes, dropIndexes; `default` → WARN и пропуск)
- `cmd/mongo-replicator/replicator/integration_test.go`: `//go:build integration`, `IT_SRC_URI`/`IT_DST_URI`, хелперы `prepare`/`cleanDB`/`srcNow`/`start`/`startOn`/`runner.waitSame`/`waitQuiet`/`ckptToken`/`same`/`dbExists`/`runErr`; `runner.stop` требует `context.Canceled` (`:203`) — тесты, ожидающие ошибку `Run`, используют `runErr` (`:695-703`, как `TestIntegrationWriteError`)
- команды: `make test`, `make test-integration`, `make lint` (через docker compose)

**Поведение change stream на 8.0** (проверено ревью плана на `mongo-src` во временных базах):

| действие на источнике | поток уровня базы `A` |
|---|---|
| rename `A.x → A.y` | `rename` внутри базы — применяется как сейчас |
| rename `B.y → A.x` (внутрь) | `create A.tmp….renameCollection`, `createIndexes`, `insert` ×N, `rename A.tmp… → A.x` — воспроизводится существующим кодом |
| rename `A.x → B.z` (наружу) | только `drop A.x` |
| `dropDatabase A` | `drop` по коллекциям, `dropDatabase`, `invalidate`, курсор закрыт |
| `resumeAfter` = токен `dropDatabase` | сразу снова `invalidate` |
| `resumeAfter` = токен `invalidate` | ошибка 260 `InvalidResumeToken` |

Нынешний код на `invalidate` уходит в `default`, сохраняет токен `invalidate` и затем крутится в пустом цикле (`TryNext` → false без ошибки при cursor id 0).

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
- run tests and `make lint` after each task
- обратная совместимость не нужна: тулза ещё не применялась, старых чекпойнтов и сценариев запуска нет. С `--all-dbs` поведение и pipeline прежние

## Testing Strategy
- **unit tests**: `make test` — чистые функции (`parseOpts`, `streamPipeline`, `checkDB`, `tokenTime`, `lagOf`, `permanent` в `main`)
- внутри пакета `replicator` семантика прежняя: `Config.DB == ""` — все базы; обязательность выбора — только на уровне CLI, интеграционные тесты режима всех баз не меняются
- **integration tests**: `make test-integration` — против `mongo-src` (8.0) и `mongo-dst` (5.0); база теста передаётся как `Config.DB`, вторая база — `<db>_other` через `cleanDB`
- тесты, ожидающие ошибку от `Run`, — через `runErr` (синхронно), не через `start`
- работа на правах только одной базы автоматически не проверяется (compose без auth) — ручная проверка в README

## Progress Tracking
- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix
- update plan if implementation deviates from original scope
- keep plan in sync with actual work done

## Solution Overview
- уровень потока выбирается по `Config.DB`: `""` → `src.Watch` + `$match $nin` (как сейчас); имя базы → `src.Database(db).Watch` с пустым pipeline (сервер сам ограничивает поток базой, права нужны только на неё)
- чекпойнт запоминает, для какой базы он записан (`db`); при расхождении с `--db` — строгий отказ стартовать (`ErrDBMismatch`)
- `invalidate` (порождается `dropDatabase` отслеживаемой базы) — `ErrStreamInvalidated`; токен `invalidate` не сохраняется
- обе ошибки перманентные: `main` выходит с кодом 1 без ретраев
- rename между базами отдельно не обрабатывается: на 8.0 он приходит как `drop` (наружу) или как копия через временную коллекцию (внутрь) и применяется существующим кодом; это закреплено тестом и описано как «не предусмотрено» в `--help` и README
- порядок `commit` → DDL → `saveToken` и применение DML/DDL не меняются

## Technical Details

**Опции**
```go
DB     string `long:"db" env:"DB" description:"реплицировать только эту базу: поток уровня базы, права на источнике — только на неё. Rename коллекций внутри базы поддерживается, перенос коллекций между базами не предусмотрен"`
AllDBs bool   `long:"all-dbs" env:"ALL_DBS" description:"реплицировать все базы, кроме служебных (нужна readAnyDatabase на источнике)"`
```
- `parseOpts` → ошибка (exit 2):
  - ни `--db`, ни `--all-dbs` (`DB=` пустая — то же, что не задана): «укажите --db <база> или --all-dbs»
  - оба сразу
  - имя базы содержит любой из `/\. "$` или NUL; длиннее 63 байт (MongoDB: короче 64); служебная база (`admin`, `config`, `local`, `_replicator`)
- в `Config.DB` передаётся имя базы или `""` при `--all-dbs`

**Config**
```go
type Config struct {
    StartTS   uint32
    BatchSize int
    DB        string // "" — все базы, кроме служебных
}
```

**Поток**
- `streamPipeline(db string) mongo.Pipeline`: `db == ""` → `[{$match: {"ns.db": {$nin: [admin, config, local, _replicator]}}}]`; иначе — пустой pipeline
- `Run`: `db == ""` → `r.src.Watch(ctx, pipeline, opts)`; иначе `r.src.Database(db).Watch(ctx, pipeline, opts)`
- лог старта: `log.Info().Str("db", db-или-"all")`

**Чекпойнт**
```js
{_id: "main", token, clusterTime, updatedAt, db: "urlshrinker"}   // db: "" — все базы
```
- `saveToken` пишет `db: r.cfg.DB`
- `loadToken` возвращает `(token bson.Raw, db string, err error)`
- `checkDB(saved, want string) error`: `saved != want` → `fmt.Errorf("%w: в чекпойнте %q, задано %q", ErrDBMismatch, saved, want)`; вызывается в `Run` только если токен есть, до открытия потока

**invalidate**
- отдельный `case "invalidate"` в цикле `Run` до `default`: `commit()` (сохраняет всё до него, включая применённый `dropDatabase`), затем `return ErrStreamInvalidated`; токен `invalidate` не сохраняется
- рестарт с чекпойнтом на токене `dropDatabase` снова получает `invalidate` → снова `ErrStreamInvalidated`, пока чекпойнт не удалён (ожидаемо, описать в README)

**Лаг репликации**
- **применённая позиция** — время токена последнего **сохранённого** чекпойнта: чекпойнт двигается только после записи данных с `w:majority`, значит всё до этой позиции уже на цели. На простое токен — post-batch resume token, он двигается вместе с oplog источника (в т.ч. периодические no-op и записи в другие базы), так что позиция не «застывает»
- `tokenTime(token bson.Raw) (bson.Timestamp, error)`: `_data` токена — hex; первый байт `0x82` (тип Timestamp в KeyString), затем 8 байт big-endian: `T` (uint32), `I` (uint32). Иначе — ошибка (формат внутренний, но стабилен с 4.2; так же декодируют Debezium и др.)
- `saveToken`: после успешной записи — `r.appliedTS = tokenTime(token)`; в чекпойнт `clusterTime` пишется он же (вместо `lastTS`). Если токен не декодируется — fallback на `lastTS`, как сейчас, и DEBUG-лог
- **текущее время источника** — `operationTime` из `ping` на `src` (без привилегий, работает и с правами на одну базу), раз в 10 с, таймаут 5 с. Обе величины — время кластера источника: расхождение часов машины репликатора и источника не влияет
- `lagOf(srcNow, applied bson.Timestamp) time.Duration` = `srcNow.T − applied.T` секунд, отрицательное → 0; `applied` нулевой (ещё ничего не сохранено) → лаг неизвестен
- лог раз в 10 с (INFO `прогресс`):
  | поле | значение |
  |---|---|
  | `applied` | всего применено операций (как сейчас) |
  | `rate` | операций/с за интервал |
  | `applied_at` | время применённой позиции (RFC3339, UTC) |
  | `lag` | лаг строкой (`1m5s`) |
  | `lag_sec` | лаг в секундах, число — для алертов по логам |
- если `ping` не удался — `lag`/`lag_sec` не пишутся, DEBUG с ошибкой; репликация не прерывается
- `lastTS` остаётся только как fallback для `clusterTime`

**main**
- `permanent(err error) bool` — `IsHistoryLost(err)` или `errors.Is` по `ErrNoStartPoint`, `ErrDBMismatch`, `ErrStreamInvalidated`; `run` при `permanent` → лог с подсказкой оператору + exit 1

**Права**
| Режим | Источник | Цель |
|---|---|---|
| без `--db` | `readAnyDatabase` (`find` + `changeStream` на всех базах) | `readWrite` на реплицируемые базы и `_replicator` + `dropDatabase` |
| `--db X` | `read` на `X` | `readWrite` на `X` и `_replicator` + `dropDatabase` на `X` (`dbAdmin@X` или своя роль) |

Без `dropDatabase` на цели событие `dropDatabase` источника падает с 13 (не перманентная) → вечные ретраи, до `ErrStreamInvalidated` дело не доходит.

## What Goes Where
- **Implementation Steps**: код, юнит- и интеграционные тесты, README
- **Post-Completion**: ручная проверка на реальном источнике с правами только на `urlshrinker`

## Implementation Steps

### Task 1: Опции `--db` / `--all-dbs` и `Config.DB`

**Files:**
- Modify: `cmd/mongo-replicator/main.go`
- Modify: `cmd/mongo-replicator/main_test.go`
- Modify: `cmd/mongo-replicator/replicator/replicator.go`
- Modify: `Makefile`

- [x] добавить `DB` и `AllDBs` в `options` (description из Technical Details) и валидацию в `parseOpts`: ровно одно из двух, плюс проверка имени
- [x] `Makefile`: в цель `run` добавить `--all-dbs` (иначе `make run` упадёт с exit 2)
- [x] добавить `DB string` в `replicator.Config`, передать из `run`
- [x] добавить `DB` и `ALL_DBS` в `clearEnv` в `main_test.go`; обновить существующие тесты `parseOpts`, которые разбирают опции без `--db`/`--all-dbs` (добавить `--all-dbs`)
- [x] write tests `parseOpts`: `--db urlshrinker`, `DB=urlshrinker` из env, флаг перекрывает env, `--all-dbs` → `DB == ""`, `ALL_DBS=true` из env
- [x] write tests `parseOpts` выбора режима: ничего не задано → ошибка с подсказкой; `DB=` пустая без `--all-dbs` → ошибка; `--db x --all-dbs` → ошибка
- [x] write tests `parseOpts` ошибок: `a.b`, `a/b`, `a b`, `a$b`, `a"b`, `a\b`, NUL, 64 и 65 байт, `admin`, `config`, `local`, `_replicator`
- [x] `make test` и `make lint` — должно пройти
- ➕ лимит длины имени — 63 байта (MongoDB: имя базы короче 64 байт), а не 64; список служебных баз вынесен в `replicator.SystemDBs` и используется и в `parseOpts`, и в `$match` потока

### Task 2: Поток уровня базы

**Files:**
- Modify: `cmd/mongo-replicator/replicator/replicator.go`
- Modify: `cmd/mongo-replicator/replicator/replicator_test.go`
- Modify: `cmd/mongo-replicator/replicator/integration_test.go`

- [x] вынести `streamPipeline(db)`; в `Run` выбрать `r.src.Watch` или `r.src.Database(db).Watch`; лог режима при старте
- [x] write unit tests `streamPipeline`: `""` → `$match` с `$nin` (сверка с bson), имя базы → пустой pipeline
- [x] write integration test `TestIntegrationSingleDB`: `cleanDB(t, db+"_other")`; сначала DML + `create` + `createIndexes` в `_other`, затем DML и rename `x → y` внутри базы теста, затем финальная запись в базу теста; `waitSame(db)`, `waitQuiet()`, и только потом `!dbExists(dst, db+"_other")` — события упорядочены, значит события `_other` пройдены
- [x] `make test`, `make test-integration`, `make lint` — должно пройти (прежние тесты режима всех баз не меняются)

### Task 3: База в чекпойнте, `ErrDBMismatch`, `permanent` в main

**Files:**
- Modify: `cmd/mongo-replicator/replicator/checkpoint.go`
- Modify: `cmd/mongo-replicator/replicator/replicator.go`
- Modify: `cmd/mongo-replicator/replicator/replicator_test.go`
- Modify: `cmd/mongo-replicator/replicator/integration_test.go`
- Modify: `cmd/mongo-replicator/main.go`
- Modify: `cmd/mongo-replicator/main_test.go`

- [x] `saveToken` пишет `db`; `loadToken` возвращает `(token, db, err)`; обновить вызовы (`Run`, `runner.ckptToken`); тестовый хелпер `ckptDB(t)` читает поле `db` чекпойнта
- [x] `ErrDBMismatch` + `checkDB(saved, want)`; проверка в `Run` после `loadToken`, если токен есть, до `Watch`
- [x] в `main`: `permanent(err)` (`IsHistoryLost`, `ErrNoStartPoint`, `ErrDBMismatch`); `run` использует его, сообщение оператору по каждой ошибке
- [x] write unit tests `checkDB`: совпадение (в т.ч. `""`/`""`), расхождение имён, `""` → имя, имя → `""`; `errors.Is(err, ErrDBMismatch)` и текст с обоими значениями
- [x] write unit tests `permanent`: каждая ошибка, обёрнутая через `fmt.Errorf("%w")` → true; обычная ошибка и `context.Canceled` → false
- [x] write integration test `TestIntegrationSingleDBResume`: прогон с `DB` через `start`, `waitQuiet`, `stop`; `ckptDB` = база теста; рестарт без `StartTS` продолжает с токена, `Applied() == N` новых событий
- [x] write integration test `TestIntegrationDBMismatch`: чекпойнты записываются тестом напрямую (произвольный токен; `checkDB` идёт до `Watch`): `db: "A"` + запуск с `""` и с `"B"`; `db: ""` + запуск с `DB` → `runErr` сразу возвращает `ErrDBMismatch`
- [x] `make test`, `make test-integration`, `make lint` — должно пройти
- ➕ хелпер чтения базы чекпойнта — метод `runner.ckptDB()` рядом с `ckptToken()`: функция `ckptDB(t)` конфликтует с константой `ckptDB` пакета; подсказки оператору вынесены в `hint(err)`, `permanent(err)` = `hint(err) != ""`

### Task 4: `ErrStreamInvalidated`

**Files:**
- Modify: `cmd/mongo-replicator/replicator/replicator.go`
- Modify: `cmd/mongo-replicator/replicator/integration_test.go`
- Modify: `cmd/mongo-replicator/main.go`
- Modify: `cmd/mongo-replicator/main_test.go`

- [x] `ErrStreamInvalidated`; `case "invalidate"` в цикле `Run` до `default`: `commit()`, затем возврат ошибки; токен `invalidate` не сохраняется
- [x] добавить `ErrStreamInvalidated` в `permanent`; дополнить тест `permanent`
- [x] write integration test `TestIntegrationDropDatabaseInvalidates`: база теста заранее создана на цели напрямую (чтобы проверка удаления была содержательной); `srcNow`, DML и `dropDatabase` на источнике; `runErr(Config{StartTS, DB})` → `ErrStreamInvalidated`; база на цели удалена; повторный `runErr(Config{DB})` с сохранённым чекпойнтом → снова `ErrStreamInvalidated`, не `hasCode(err, 260)`
- [x] `make test`, `make test-integration`, `make lint` — должно пройти

### Task 5: Лаг репликации

**Files:**
- Create: `cmd/mongo-replicator/replicator/lag.go`
- Create: `cmd/mongo-replicator/replicator/lag_test.go`
- Modify: `cmd/mongo-replicator/replicator/checkpoint.go`
- Modify: `cmd/mongo-replicator/replicator/replicator.go`
- Modify: `cmd/mongo-replicator/replicator/integration_test.go`

- [x] `lag.go`: `tokenTime(token)` и `lagOf(srcNow, applied)` (см. Technical Details)
- [x] `saveToken`: `r.appliedTS` и `clusterTime` чекпойнта из `tokenTime(token)`, fallback на `lastTS`
- [x] лог прогресса: `ping` источника с таймаутом 5 с → `applied`, `rate`, `applied_at`, `lag`, `lag_sec`; при ошибке `ping` — без лага, DEBUG
- [x] write unit tests `tokenTime`: реальные токены 8.0 (событие и post-batch, захватить один раз из интеграционного прогона и вписать hex-константами) → ожидаемые `T`/`I`; ошибки: нет `_data`, не hex, первый байт не `0x82`, меньше 9 байт
- [x] write unit tests `lagOf`: обычный случай, источник «позади» применённого (часы/округление) → 0, нулевой `applied` → неизвестно
- [x] write integration test `TestIntegrationLagPosition`: режим `DB`; запись в базу теста, `waitSame`, `waitQuiet` → `clusterTime` чекпойнта ≥ `operationTime` этой записи; затем запись **только** в `<db>_other` → `require.Eventually`: `clusterTime` чекпойнта ≥ `operationTime` записи в `_other` (позиция двигается без событий своей базы — post-batch token), при этом `Applied()` не изменился
- [x] `make test`, `make test-integration`, `make lint` — должно пройти
- ➕ `lastTS` хранится как `bson.Timestamp` (fallback для `clusterTime` с `I`); `rate` округлён до 0.1; время источника — `srcTime` (ping на `admin`) в `lag.go`, лог — `logProgress`

### Task 6: Rename между базами — закрепить фактическое поведение

**Files:**
- Modify: `cmd/mongo-replicator/replicator/integration_test.go`

- [x] write integration test `TestIntegrationSingleDBCrossDBRename` (документирует README «перенос между базами не предусмотрен»): режим `DB`; `_other.y` с документами и индексом → rename `_other.y → db.in`; DML в `db.out` → rename `db.out → _other.z`; финальная запись в базу теста; `waitSame(db)`, `waitQuiet()`; на цели `db.in` совпадает с источником (данные и индексы), `db.out` отсутствует, базы `_other` на цели нет, `Run` продолжает работать
- [x] `make test-integration` — должно пройти
- ➕ хелпер `renameNS(from, to)` по полным namespace; `renameColl` теперь вызывает его

### Task 7: README

**Files:**
- Modify: `README.md`

- [x] опции `--db` / `DB` и `--all-dbs` / `ALL_DBS` в таблице опций, обязательность выбора одной из них; пример запуска с `--db urlshrinker`; обновить существующие примеры запуска (добавить `--db` или `--all-dbs`)
- [x] таблица прав «режим → источник / цель» (см. Technical Details, включая `dropDatabase` на цели); пояснение, что `_replicator` создаётся на цели автоматически при первом сохранении чекпойнта
- [x] проверочные mongosh-запросы: поток по базе (`db.getSiblingDB(X).watch(...)` + `tryNext`), `--start-ts` в окне oplog через `watch` с `startAtOperationTime` (код 286 — выпал), запись в `_replicator` на цели (probe-коллекция с `w: "majority"`, затем drop)
- [x] «Ограничения»: rename коллекций внутри базы поддерживается; перенос коллекций между базами не предусмотрен — если всё же случится, на 8.0 перенос наружу = удаление коллекции на цели, внутрь = копия коллекции; `dropDatabase` базы останавливает репликатор (`ErrStreamInvalidated`)
- [x] «Мониторинг лага»: поля лога `прогресс` (`applied`, `rate`, `applied_at`, `lag`, `lag_sec`) и что именно измеряется (применённая позиция против времени источника); `clusterTime` в чекпойнте = время применённой позиции и на простое двигается; mongosh: `clusterTime` из чекпойнта на цели и `db.adminCommand({ping: 1}).operationTime` на источнике → разница = лаг
- [x] коды выхода: новые причины exit 1; как сменить базу / продолжить после `ErrDBMismatch` и `ErrStreamInvalidated` (остановить, удалить чекпойнт, залить базу заново, новый `--start-ts`); после `dropDatabase` рестарт без удаления чекпойнта снова выходит с 1
- ➕ добавлены проверка права `dropDatabase` на цели через `connectionStatus` + `showPrivileges` и раздел «Восстановление»; mongosh-сниппеты проверены на локальных mongo-src/mongo-dst (на свежем rs без ротации oplog старый `startAtOperationTime` ошибку 286 не даёт)

### Task 8: Verify acceptance criteria
- [x] с `--all-dbs` поведение не изменилось: все прежние интеграционные тесты проходят без правок; без `--db`/`--all-dbs` — exit 2
- [x] с `--db` поток открывается на уровне базы, другие базы на цель не попадают, rename внутри базы работает
- [x] `ErrDBMismatch` и `ErrStreamInvalidated` → exit 1 без ретраев
- [x] лог `прогресс` раз в 10 с содержит `lag`/`lag_sec`; на простаивающем источнике лаг не растёт (проверить вручную через `make run`: 1–2 минуты без записей) — проверено бинарём `--db` против локальных mongo-src/mongo-dst: 100 с простоя, `lag` 0s, `applied_at` двигается
- [x] run full test suite: `make test`
- [x] run integration tests: `make test-integration`
- [x] `make lint` без замечаний
- ➕ e2e бинарём: без режима и с обоими режимами — exit 2; `--db` поверх чекпойнта другой базы — exit 1 сразу; `dropDatabase` отслеживаемой базы — exit 1, рестарт с тем же чекпойнтом — снова exit 1; прежние интеграционные тесты не менялись (правки только в хелперах `ckptToken`/`renameColl`)

### Task 9: [Final] Update documentation
- [ ] создать CLAUDE.md, только если появились неочевидные договорённости
- [ ] move this plan to `docs/plans/completed/`

## Post-Completion
*Items requiring manual intervention or external systems - no checkboxes, informational only*

**Manual verification**:
- запуск на реальном источнике с пользователем `readWrite@urlshrinker`: `--db urlshrinker` стартует, `--all-dbs` — ошибка 13 при открытии потока (ожидаемо), без обоих — exit 2
- проверочные запросы из README против реальных кластеров: поток по базе, окно oplog для `--start-ts`, запись в `_replicator` на цели
- права пользователя цели: `readWrite` на `urlshrinker` и `_replicator` + `dropDatabase` на `urlshrinker`
