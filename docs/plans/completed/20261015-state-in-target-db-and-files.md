# Состояние в базе цели и в файлах на машине, тесты обрыва сети

## Overview
- **чекпойнт и аренда** переезжают из отдельной базы `_replicator` в коллекцию `_replicator_checkpoint` **внутри базы цели** (`--dst-db`): на цели достаточно `readWrite` на одну базу, а чекпойнт живёт и удаляется вместе с данными
- **накопленный трафик и сводка** уходят из чекпойнта на цели в **файлы на машине** (`--state-dir`, файл на процесс): учёт трафика нужен по машине; общий итог по нескольким машинам — вручную
- поток **всегда** отсекает служебную коллекцию (`$match` по `ns.coll`); rename в служебное имя и из него — перманентная ошибка
- **тесты обрыва сети** через Toxiproxy: отключение и «зависание» источника и цели, проверка повторов, чекпойнта, аренды и самоостановки, учёта трафика
- выполняется поверх ветки `multi-process-traffic` (план `docs/plans/completed/20261015-multi-process-traffic-progress.md` выполнен)
- тулза ещё не запускалась на реальных базах: обратная совместимость (в т.ч. с базой `_replicator`) не нужна; провайдер облака и направление репликации не фиксируются

## Context (from discovery)
- `cmd/mongo-replicator/replicator/checkpoint.go`: `ckptDB = "_replicator"`, `ckptColl = "checkpoint"` (`:20-21`); `ckpt()` (`:27-28`); документ `_id = --dst-db`, поля `db`, `dst_db`, `token`, `clusterTime`, `updatedAt`, `owner`, `lease_until`, `traffic.*`
- `cmd/mongo-replicator/replicator/lease.go`: `acquireLease` (`:80`), горутина `renewLease` (`:143`), `renewOnce` (`:175`, дельты `traffic.*` через `ckptFields`, `:180`), `Release` (`:204`, `$inc` + `$unset`, `:216`; удаляет документ без `token`); `checkLease()`/`errNotOwner`
- `cmd/mongo-replicator/replicator/traffic.go`: `Traffic`, `countingConn`, диалеры; `TrafficInfo.ckptFields` (`:52-53`); `saveTraffic`/`ckptTraffic`; `clusterTraffic` (`:84`, агрегация по `_replicator.checkpoint`, INFO `трафик всех процессов`)
- `cmd/mongo-replicator/replicator/replicator.go`: `streamPipeline = [{$unset: updateDescription}]` (`:44`), `SystemDBs = [admin, config, local, ckptDB]` (`:47`), `Watch(ctx, streamPipeline, opts)` (`:250`); пара баз проверяется в `stream()` **после** захвата аренды
- `cmd/mongo-replicator/replicator/ddl.go`: `applyDDL` (ветка `rename`, вызов `checkLease()`)
- `cmd/mongo-replicator/replicator/status.go` — `Status.Cluster`; `cmd/mongo-replicator/progress.go` — строка «все процессы (N, активно M, на HH:MM)»
- `cmd/mongo-replicator/main.go`: doc-комментарий про `_replicator` (`:5`), проверка имён по `SystemDBs` (`:178`); `hint()` — `ErrDBMismatch` с `deleteOne({_id: "<dst-db>"})`, подсказки `dropDatabase`/`ErrStreamInvalidated` «удалите чекпойнт»
- `cmd/mongo-replicator/main_test.go`: `:450` — `{"_replicator", …, "служебная база"}`; `:499` — ожидает строку `deleteOne`
- `integration_test.go`: `collNames()` (`:367`) — все коллекции кроме `system.*`, `sameDBs` сравнивает списки источника и цели; точные списки коллекций — `:1042,1099,1698,1718,2105`; `prepare()` (`:166`) дропает глобальную `_replicator.checkpoint`; `writeCkpt`, `readCkptTraffic`, `TestIntegrationClusterTraffic` (документ `"it_starting"`, `Processes == 2`), `TestIntegrationTrafficCheckpoint`, `TestIntegrationTrafficRelease`, `TestIntegrationDBMismatch`/`DstDBMismatch` (базы `it_mismatch_*` только на цели не чистятся), `TestIntegrationStatus` (`s.Cluster`, `:2343`), `TestIntegrationDropDatabaseStops`, `DstDB`/`SingleDB`/`CrossDBRenameIn` (списки коллекций); всего 49 интеграционных тестов, сетевых нет
- `traffic_test.go`: `TestSaveTraffic`, `TestSaveTrafficNil`, `TestTrafficCkptFields`; `replicator_test.go`: `TestStreamPipelineUnsetsUpdateDescription`
- драйвер v2.9.2 (проверено ревью): `$ne` пропускает события без `ns.coll` (`dropDatabase`, `invalidate`); у rename `ns` — исходная коллекция; отфильтрованные события продвигают post-batch resume token; `ApplyURI` для `mongodb+srv` кладёт в `Hosts` разрешённые из SRV хосты (при `srvMaxHosts` — случайное подмножество); ChangeStream при сетевой ошибке сам один раз переподключается, ожидая выбор сервера до `serverSelectionTimeoutMS` (30 с по умолчанию); используемые соединения прерываются после двух подряд таймаутов heartbeat (`server.go:707-727`), в streaming-режиме каждый — `connectTimeoutMS + heartbeatFrequencyMS`
- Toxiproxy (проверено ревью): `github.com/Shopify/toxiproxy/v2` v2.13.0 требует go 1.27 — **не подходит** (образ `Dockerfile.dev` и `go.mod` — Go 1.26); **v2.12.0** — go 1.23, тот же API клиента (`Populate`, `Enable`/`Disable`, `AddToxic(name, type, stream, toxicity, attrs)`, `RemoveToxic`); образ `ghcr.io/shopify/toxiproxy:2.12.0` — `FROM scratch`, без healthcheck; `timeout`-токсик по умолчанию только `downstream`; `directConnection=true` через прокси: топология Single, хосты из hello игнорируются, change streams и `w:majority` работают
- `Makefile`: комментарий к `-p 1` про общую коллекцию чекпойнтов; `README.md:183` — про чекпойнт `test` после `make test-integration`
- в ветке нет `CLAUDE.md`
- команды: `make test`, `make test-integration`, `make lint`, `make build`

## Development Approach
- **testing approach**: Regular (код, затем тесты в той же задаче)
- ветка: поверх `multi-process-traffic` (в ней же или в новой от неё — решается при старте реализации)
- порядок: сначала файлы состояния (трафик уходит из чекпойнта), потом перенос чекпойнта — так каждая задача проходит тесты без временного кода
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
- обратная совместимость не нужна

## Testing Strategy
- **unit tests**: `make test` — ключ файла по URI, файлы состояния (запись, чтение, перенос в сторону, сводка), значение `--state-dir`, pipeline потока, защита rename, подсказки
- **integration tests**: `make test-integration` — файлы трафика и сводка, чекпойнт в базе цели, фильтр служебной коллекции, два процесса, аренда, **сетевые сценарии через Toxiproxy**
- сетевые тесты идут через прокси своими клиентами; остальные — напрямую, как сейчас
- пустые `StateFile`/`StateDir` в `Config` выключают файлы и сводку — большинство интеграционных тестов без них

## Progress Tracking
- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix
- update plan if implementation deviates from original scope
- keep plan in sync with actual work done

## Solution Overview
- **на цели** (критичное, живёт вместе с данными): позиция, пара баз, аренда — один документ в `<dst-db>._replicator_checkpoint`
- **на машине** (наблюдение, потеря не ломает репликацию): накопленный трафик процесса — файл `<state-dir>/<dst-db>.<key>.json`; сводка — сумма всех файлов каталога (фактически по пользователю и каталогу состояния)
- механизм аренды, счётчики в сокетах, строки лога и панель остаются; меняются место хранения, источник сводки и фильтр потока

## Technical Details

### Файлы состояния
- опция `--state-dir` / `STATE_DIR`; пусто → `$XDG_STATE_HOME/mongo-replicator` (относительный `XDG_STATE_HOME` игнорируется, как требует спецификация) → иначе `~/.local/state/mongo-replicator` (`os.UserHomeDir`); `MkdirAll(dir, 0o750)` при старте; ошибка → exit 2 до подключения
- **ключ кластера** — по URI как написан, не по разрешённым хостам (SRV с `srvMaxHosts` даёт случайное подмножество, замена узла меняла бы хеш):
  - `mongodb+srv://host/…` → `srv:<host в нижнем регистре>`
  - `mongodb://h1,h2:27018/…` → хосты из строки (разбор строкой, как `schemeEnd`), нижний регистр, `:27017` по умолчанию, сортировка, через запятую
  - имя файла `<dst-db>.<первые 8 hex sha256(ключа)>.json`; путь считает `main` → `Config.StateFile`, каталог → `Config.StateDir`; пустые значения — файлы и сводка выключены
- формат:
  ```json
  {"db": "...", "dst_db": "...", "cluster": "<ключ>", "owner": "...",
   "started_at": "RFC3339", "updated_at": "RFC3339",
   "traffic": {"src_in": 0, "src_out": 0, "dst_in": 0, "dst_out": 0}}
  ```
- `replicator/statefile.go` — чистые функции:
  - `clusterKey(uri string) string`, `stateFileName(dstDB, key string) string`
  - `readState(path) (state, error)`: нет файла → нулевое состояние; JSON не разбирается → `ErrStateCorrupt`
  - `writeState(path, state)`: `os.CreateTemp(dir, ".<name>.tmp-*")` → запись → `Sync` → `Close` → `Rename` → `Sync` каталога
  - `moveAside(path, reason)`: `rename` в `<path>.<reason>-<UTC-время>` (`corrupt`, `mismatch`) — история не стирается
  - `machineTraffic(dir, now) (MachineTraffic, error)`: все `*.json` (нечитаемые — WARN, пропуск); сумма `traffic`, `processes` = число файлов, `active` = `updated_at` моложе 30 с
- **жизненный цикл** (только при непустом `StateFile`):
  - загрузка базы — **после** проверки пары баз в `stream()` (после `checkDBs`), а не сразу после захвата аренды: с неверным `--db` файл не трогается
  - база читается **один раз за процесс**: второй процесс на ту же базу запустить нельзя (аренда), а при потере аренды процесс завершается (`ErrLeaseLost`/`ErrLeaseHeld`) и дальше не пишет — пересчитывать базу между `Run` незачем
  - битый файл → WARN, `moveAside(corrupt)`, база 0; другая пара баз или ключ кластера → WARN, `moveAside(mismatch)`, база 0
  - сразу после загрузки базы — первая запись (процесс сразу попадает в сводку как активный)
  - писатель — **отдельная горутина на время жизни процесса** (запускается после загрузки базы, останавливается в `Release`), раз в 10 с: пишет `base + текущие счётчики`; не привязана к `Run` — пишет и в паузах цикла повторов (сеть до цели могла отвалиться, а трафик всё равно растёт); после перманентной потери аренды процесс завершается, писать некому
  - `Release`: запись, только если база загружена и `UpdateOne`/`DeleteOne` совпал по нашему `owner`; иначе не писать (после `ErrLeaseHeld`/`errNotOwner` второй процесс на ту же базу этой машины файл не трогает)
  - ошибка записи → WARN, репликация продолжается
  - процесс в цикле повторов остаётся **активным** в сводке: он по-прежнему единственный владелец базы, а файл продолжает обновляться
- сводка раз в `clusterEvery` → INFO `трафик на машине` (`processes`, `active`, `src_in`, `src_out`, `dst_in`, `dst_out`); `Status.Machine` вместо `Status.Cluster`; панель — «на машине (3, активно 2, на 12:01): …»
- удаляется: дельты `traffic.*` в `renewOnce`/`Release`, `ckptFields`, `saveTraffic`, `ckptTraffic`, `clusterTraffic`, поля `traffic` в чекпойнте
- сброс счёта — удалить файл; удаление чекпойнта на цели файл не трогает

### Чекпойнт в базе цели
```js
// <dst-db>._replicator_checkpoint
{ _id: "checkpoint", db, dst_db, token, clusterTime, updatedAt, owner, lease_until }
```
- `ckptColl = "_replicator_checkpoint"`, `ckptID = "checkpoint"`; `ckptDB` удаляется; `ckpt()` → `r.dst.Database(r.cfg.DstDB).Collection(ckptColl, readpref primary)`
- аренда без изменений по логике — фильтры по `_id: ckptID`
- `SystemDBs` → `[admin, config, local]`; `_replicator` — обычное допустимое имя
- служебную коллекцию удалили вручную: рестарт → `ErrNoStartPoint`; во время работы → `ErrLeaseLost` на следующем продлении или сохранении
- опечатка в `--dst-db`: захват аренды создаёт на цели базу и коллекцию с этим именем; `Release` удаляет документ без `token`, но пустая коллекция и база остаются (не дропаем автоматически — гонка с чужим upsert) — отметить в README
- подсказки `main`: `ErrDBMismatch` — «чекпойнт этой базы цели записан для другой базы источника: верните прежний --db или удалите `<dst-db>._replicator_checkpoint` (или всю базу цели) и залейте заново»; `dropDatabase`/`ErrStreamInvalidated` — «удалите `<dst-db>._replicator_checkpoint` (или всю базу цели) и залейте заново с --start-ts»

### Фильтр потока и служебное имя
```go
var streamPipeline = mongo.Pipeline{
    {{Key: "$match", Value: bson.D{{Key: "$or", Value: bson.A{
        bson.D{{Key: "ns.coll", Value: bson.D{{Key: "$ne", Value: ckptColl}}}},
        bson.D{{Key: "operationType", Value: "rename"}},
    }}}}},
    {{Key: "$unset", Value: "updateDescription"}},
}
```
- rename пропускается фильтром всегда — чтобы rename **в** служебное имя и **из** него дошёл до `applyDDL`
- `applyDDL`, ветка `rename`, **до** `checkLease()`: `ev.NS.Coll == ckptColl || ev.To.Coll == ckptColl` → перманентная `ErrReservedCollection`
- `hint`: «на источнике коллекция переименована в служебное имя _replicator_checkpoint или из него: переименуйте её на источнике в другое имя и сделайте новую полную заливку» (чекпойнт стоит перед этим событием — рестарт снова упрётся в него)
- начальная заливка дампом источника, где есть `_replicator_checkpoint` (источник был целью при обратной репликации): при совпадении имён баз `checkDBs` пройдёт, а чужой токен даст «выпала из oplog» (280/286) — вводящая в заблуждение ошибка; README: `--excludeCollection _replicator_checkpoint` и этот симптом

### Тесты обрыва сети (Toxiproxy)
- версии: модуль `github.com/Shopify/toxiproxy/v2@v2.12.0`, образ `ghcr.io/shopify/toxiproxy:2.12.0` (v2.13.0 требует go 1.27 — не подходит)
- docker-compose: сервис `toxiproxy`, API `toxiproxy:8474`; образ без healthcheck → `TestMain` повторяет `Populate` до готовности (до ~10 с); `app` — `depends_on: toxiproxy`
- env `app`: `IT_PROXY_API=http://toxiproxy:8474`; `IT_SRC_PROXY_URI=mongodb://toxiproxy:27001/?directConnection=true&heartbeatFrequencyMS=500&connectTimeoutMS=2000&serverSelectionTimeoutMS=2000`; `IT_DST_PROXY_URI` — то же для `27002`
- `directConnection=true` обязателен: без него драйвер найдёт узел по адресу из конфигурации replica set и пойдёт мимо прокси
- `TestMain`: `Populate` (пересоздаёт существующие) прокси `src` (`0.0.0.0:27001` → `mongo-src:27017`) и `dst` (`0.0.0.0:27002` → `mongo-dst:27017`); без `IT_PROXY_API` — сетевые тесты `t.Skip`
- хелпер `netCase(t)`: клиенты через прокси с диалерами `Traffic`; `t.Cleanup` — `Enable()`, удаление всех токсиков, `Disconnect`; аренда короткая (TTL ≈ 2 с, продление ≈ 300 мс); «зависание» — `timeout`-токсик **на обоих направлениях** (`upstream` и `downstream`), иначе запросы доходят до сервера и продление/записи проходят без ответа

## What Goes Where
- **Implementation Steps**: код, тесты, compose, Makefile, README
- **Post-Completion**: ручная проверка нескольких процессов на одной машине, сверка с метриками облака, решение по сторожу

## Implementation Steps

### Task 1: Файлы состояния — чистые функции

**Files:**
- Create: `cmd/mongo-replicator/replicator/statefile.go`
- Create: `cmd/mongo-replicator/replicator/statefile_test.go`

- [x] `clusterKey`, `stateFileName`, `readState`, `writeState`, `moveAside`, `machineTraffic` (Technical Details)
- [x] write unit tests `clusterKey`: `mongodb+srv` → `srv:<host>` (регистр не важен, `srvMaxHosts` и параметры не влияют); `mongodb://` — порядок и регистр хостов не важны, `h1` = `h1:27017`; разные кластеры — разные ключи; формат имени `<dst-db>.<8 hex>.json`
- [x] write unit tests `writeState`/`readState`: нет временных файлов после записи, содержимое заменено целиком, повторное чтение — новые значения; нет файла → нули; битый JSON → `ErrStateCorrupt`
- [x] write unit tests `moveAside`: файл переименован с суффиксом причины и времени, оригинала нет
- [x] write unit tests `machineTraffic`: сумма по файлам; `active` по свежести `updated_at` (фиксированное `now`); нечитаемый файл пропущен; пустой каталог → нули
- [x] `make test`, `make lint` — должно пройти

### Task 2: Трафик в файлы, сводка по машине

**Files:**
- Modify: `cmd/mongo-replicator/replicator/lease.go`
- Modify: `cmd/mongo-replicator/replicator/traffic.go`, `cmd/mongo-replicator/replicator/traffic_test.go`
- Modify: `cmd/mongo-replicator/replicator/replicator.go`
- Modify: `cmd/mongo-replicator/replicator/status.go`, `cmd/mongo-replicator/replicator/status_test.go`
- Modify: `cmd/mongo-replicator/progress.go`, `cmd/mongo-replicator/progress_test.go`
- Modify: `cmd/mongo-replicator/main.go`, `cmd/mongo-replicator/main_test.go`
- Modify: `cmd/mongo-replicator/replicator/integration_test.go`

- [x] опция `--state-dir`/`STATE_DIR`, значение по умолчанию (относительный `XDG_STATE_HOME` игнорируется), `MkdirAll` при старте (ошибка → exit 2); ключ кластера по `--dst`/`DST_URI` и путь файла → `Config.StateFile`, `Config.StateDir`; `clearEnv` + `STATE_DIR`
- [x] жизненный цикл файла (Technical Details): загрузка базы один раз после первой успешной `checkDBs`, `moveAside` при битом/чужом файле, первая запись сразу, запись отдельной горутиной процесса раз в 10 с (в т.ч. в паузах повторов), остановка горутины и финальная запись в `Release` только при совпадении `owner`
- [x] удалить дельты `traffic.*` из `renewOnce`/`Release`, `ckptFields`, `saveTraffic`, `ckptTraffic`, `clusterTraffic` и их тесты (`TestSaveTraffic`, `TestSaveTrafficNil`, `TestTrafficCkptFields`, `TestIntegrationClusterTraffic`, `TestIntegrationTrafficCheckpoint`, `TestIntegrationTrafficRelease`, `readCkptTraffic`)
- [x] сводка `machineTraffic` раз в `clusterEvery`, INFO `трафик на машине`; `Status.Machine` вместо `Status.Cluster`; строка панели «на машине»
- [x] write unit tests `main`: значение `--state-dir` по умолчанию (`XDG_STATE_HOME` абсолютный, относительный, не задан; `HOME` через `t.Setenv`); путь — существующий файл → exit 2
- [x] update unit tests `renderPanel` под «на машине»; `status_test` — `Machine`
- [x] write integration tests (`StateDir: t.TempDir()`): после работы файл есть, `traffic` > 0, документ чекпойнта **без** `traffic`; после временной ошибки `Run` (пауза повтора) файл продолжает обновляться (`updated_at` растёт), процесс в сводке активен; второй `Replicator` на ту же базу после `Release` первого продолжает счёт от прошлого значения; запуск с неверным `--db` (`ErrDBMismatch`) — файл не изменён; два процесса с общим `StateDir` → сводка = сумма двух файлов, `processes = 2`, `active` = 2 во время работы и 0 после `Release` (`Eventually`); `TestIntegrationStatus` — `StateDir` и `s.Machine`
- [x] `make test`, `make test-integration`, `make lint` — должно пройти
- ➕ отклонение: путь и ключ кластера считает экспортированная `StatePath(dir, dstDB, dstURI)`; ключ передаётся в `Config.Cluster` (сверка с полем `cluster` файла); `stateActiveFor` — переменная (интеграционные тесты сокращают до 1 с), интервал писателя — поле `stateEvery`

### Task 3: Чекпойнт в коллекции базы цели

**Files:**
- Modify: `cmd/mongo-replicator/replicator/checkpoint.go`
- Modify: `cmd/mongo-replicator/replicator/lease.go`
- Modify: `cmd/mongo-replicator/replicator/replicator.go`
- Modify: `cmd/mongo-replicator/main.go`, `cmd/mongo-replicator/main_test.go`
- Modify: `cmd/mongo-replicator/replicator/integration_test.go`
- Modify: `Makefile`

- [x] `ckptColl = "_replicator_checkpoint"`, `ckptID = "checkpoint"`, удалить `ckptDB`; `ckpt()` — коллекция в `r.cfg.DstDB`; фильтры аренды и сохранения — по `_id: ckptID`
- [x] `SystemDBs` без `_replicator`; doc-комментарий `main.go:5`; подсказки `hint()` для `ErrDBMismatch`, `dropDatabase`, `ErrStreamInvalidated` (Technical Details); комментарий к `-p 1` в `Makefile`
- [x] update unit tests `main_test.go`: `:450` (`_replicator` теперь допустимое имя для `--db` и `--dst-db`), `:499` (новая подсказка `ErrDBMismatch`), `TestPermanent`
- [x] интеграционная обвязка: `collNames` исключает `ckptColl`; убрать глобальный дроп из `prepare`; `cleanDB` для `it_mismatch_a`, `it_mismatch_a2`, `it_mismatch_a3`; `writeCkpt` пишет `_id: ckptID` в базу `pair.dst`; хелперы аренды и двух процессов — на `<dst-db>._replicator_checkpoint`; точные списки коллекций (`:1042,1099,1698,1718,2105`, `DropDatabaseStops`, `DstDB`, `SingleDB`, `CrossDBRenameIn`) — без служебной коллекции
- [x] write integration tests: чекпойнт лежит в `<dst-db>._replicator_checkpoint`, базы `_replicator` на цели нет; два процесса — у каждой базы своя служебная коллекция, обе сходятся; удаление служебной коллекции во время работы → `ErrLeaseLost`, при рестарте → `ErrNoStartPoint`
- [x] `make test`, `make test-integration`, `make lint` — должно пройти
- ➕ проверки списков коллекций цели в `TestIntegrationDDL` и `TestIntegrationRestartAfterDDL` переведены с `ListCollectionNames` на `collNames` (служебная коллекция исключается); хелперы `ckptOf(dstDB)` и `cleanDBs`; `runner.ckptDB()` переименован в `ckptPair()`

### Task 4: Фильтр служебной коллекции и защита rename

**Files:**
- Modify: `cmd/mongo-replicator/replicator/replicator.go`
- Modify: `cmd/mongo-replicator/replicator/ddl.go`
- Modify: `cmd/mongo-replicator/replicator/replicator_test.go`
- Modify: `cmd/mongo-replicator/main.go`, `cmd/mongo-replicator/main_test.go`
- Modify: `cmd/mongo-replicator/replicator/integration_test.go`

- [x] `streamPipeline`: `$match` (`$or`: `ns.coll` ≠ служебной, либо `rename`), затем `$unset`
- [x] `ErrReservedCollection` в ветке `rename` `applyDDL` **до** `checkLease()` (в обе стороны); `permanent`/`hint`
- [x] update unit test `TestStreamPipelineUnsetsUpdateDescription` → обе стадии и их порядок
- [x] write unit tests: rename в служебное имя и из него → `ErrReservedCollection` (собранное вручную событие, без аренды); `permanent`/`hint`
- [x] write integration tests: insert/update/delete в `<src-db>._replicator_checkpoint` на источнике при параллельных записях в обычную коллекцию — обычная доезжает, служебная на цели содержит только документ чекпойнта; rename на источнике `foo → _replicator_checkpoint` → `ErrReservedCollection`
- [x] `make test`, `make test-integration`, `make lint` — должно пройти
- ➕ unit-тест переименован в `TestStreamPipeline`; интеграционный фильтр — `TestIntegrationCheckpointCollFiltered` (в т.ч. `_id: ckptID` и `drop` служебной на источнике), rename — `TestIntegrationRenameReserved` (подтесты `to`/`from`: имя базы ≤ 63 символов)

### Task 5: Toxiproxy в docker-compose и обвязка тестов

**Files:**
- Modify: `docker-compose.yml`
- Modify: `go.mod`, `go.sum`, `vendor/`
- Modify: `cmd/mongo-replicator/replicator/integration_test.go`

- [x] сервис `toxiproxy` (`ghcr.io/shopify/toxiproxy:2.12.0`), `depends_on` у `app`, env `IT_PROXY_API`, `IT_SRC_PROXY_URI`, `IT_DST_PROXY_URI` (Technical Details)
- [x] сначала код с импортом `github.com/Shopify/toxiproxy/v2/client`, затем `go get github.com/Shopify/toxiproxy/v2@v2.12.0 && go mod tidy && go mod vendor`; проверить, что директива `go` в `go.mod` осталась 1.26
- [x] `TestMain`: `Populate` с повтором до готовности; `netCase(t)` с клиентами через прокси, диалерами `Traffic` и `t.Cleanup`
- [x] write integration test-sanity: через прокси репликация работает (DML доезжает), `Traffic` растёт
- [x] `make test-integration`, `make lint` — должно пройти
- ➕ прокси из `Populate` не помечены клиентом как созданные (`Enable`/`Disable` → 409 «proxy already exists»): `populateProxies` после `Populate` берёт их через `Proxy(name)`; сетевой sanity-тест — `TestIntegrationNetProxy`

### Task 6: Сетевые сценарии

**Files:**
- Modify: `cmd/mongo-replicator/replicator/integration_test.go`

- [x] write test «источник отключён»: записи на источнике идут; `src.Disable()` дольше `serverSelectionTimeoutMS` (≈4 с); `Run` либо возвращает временную ошибку и повторяется (как цикл в `main`, через хелпер), либо драйвер сам переподключает поток — допустимы оба исхода; после `Enable` база цели совпадает с источником, без потерь и дублей
- [x] write test «цель отключена»: записи идут; `dst.Disable()` на ≈4 с; после `Enable` — схождение, `clusterTime` чекпойнта не новее последнего записанного на цель документа
- [x] write test «источник завис» (исследовательский): `timeout`-токсик на `src` в обоих направлениях; `Run` возвращает ошибку за ≤ ~15 с (ожидание по драйверу: два таймаута heartbeat ≈ 2 · (`connectTimeoutMS` + `heartbeatFrequencyMS`) ≈ 5 с); после снятия — повтор и схождение. ⚠️ если `Run` зависает — записать в план и ➕ задача «сторож в `Run`» (порог выбирать с учётом умолчаний драйвера: 2 · (30 с + 10 с) ≈ 80 с, иначе сторож сработает раньше драйвера); заранее не делать
- [x] write test «цель зависла дольше 2/3 TTL»: `timeout`-токсик на `dst` в обоих направлениях; первый процесс завершает `Run` с `errLeaseExpiring` до `lease_until` (сверка с `lease_until`, прочитанным напрямую; без более тесных границ — запас ≈0,5 с под `-race`); второй процесс (напрямую к цели) захватывает базу только после этого; после снятия токсика первый получает `ErrLeaseHeld`
- [x] write test «трафик после обрыва»: после обрыва и восстановления счётчики `Traffic` растут, файл состояния снова обновляется (`updated_at` растёт)
- [x] `make test-integration` несколько прогонов подряд — без нестабильности; `make lint` — должно пройти
- ➕ хелперы: `startRetry` (цикл повторов Run, как `replicate` в `main`, пауза 200 мс), `startWriter` (вставки на источник напрямую с `operationTime`), `hang` (timeout-токсик в обе стороны); тесты `TestIntegrationNetSrcDown`, `NetDstDown`, `NetSrcHang`, `NetDstHangLease`, `NetTraffic`
- ➕ «цель отключена»: «чекпойнт не новее данных» проверяется как инвариант во время обрыва и после — все документы источника со временем вставки ≤ `clusterTime` чекпойнта уже на цели (после схождения post-batch token уходит дальше последнего документа, буквальное сравнение неверно)
- ➕ «источник завис»: `Run` вернул ошибку за ≈6 с (`change stream: server selection error`), не завис — сторож в `Run` не нужен

### Task 7: README

**Files:**
- Modify: `README.md`

- [x] права на цели — только `readWrite` на `--dst-db`; убрать всё про базу `_replicator`, `$inc` трафика в чекпойнте и агрегацию по процессам; `:183` — про чекпойнт после `make test-integration`
- [x] чекпойнт: где лежит, как посмотреть (`db.getSiblingDB("<dst-db>")._replicator_checkpoint.findOne()`), как сбросить перед новой заливкой (остановить процесс, `drop` коллекции или всей базы цели); опечатка в `--dst-db` оставляет пустую базу на цели
- [x] `mongodump --excludeCollection _replicator_checkpoint` — для начальной заливки базы источника (источник мог быть целью при обратной репликации; симптом без исключения — «выпала из oplog») и для резервных копий базы цели; rename в служебное имя или из него — перманентная ошибка и что делать
- [x] раздел «Состояние на машине»: `--state-dir`/`STATE_DIR`, значение по умолчанию; сводка фактически по пользователю и каталогу состояния (для всех пользователей машины — общий `--state-dir`); имена и формат файлов; ключ кластера (по URI, SRV — по имени); сводка `трафик на машине`, строка панели; процесс в цикле повторов остаётся активным; файлы `.corrupt-*`/`.mismatch-*`; общий итог по нескольким машинам вручную (`jq -s` по файлам с машин); сброс удалением файла
- [x] «Тесты»: сетевые тесты через Toxiproxy (`make test-integration` поднимает его сам)
- ➕ файлы состояния создаются с правами `0640` (`CreateTemp` даёт `0600`): иначе в общем `--state-dir` сводка пропускает файлы других пользователей; тест — `TestWriteReadState`. Чекпойнт описан в новом подразделе «Чекпойнт» раздела «Запуск», тесты — в «Разработке» (раздела «Тесты» нет)

### Task 8: Verify acceptance criteria
- [x] на цели — только `<dst-db>._replicator_checkpoint`, базы `_replicator` нет; права — `readWrite` на базу цели
- [x] служебная коллекция не уходит в поток; rename в служебное имя и из него — перманентная ошибка
- [x] трафик — в файлах на машине, история не стирается при неверном `--db`; сводка по машине раз в минуту, панель показывает «на машине»
- [x] сетевые сценарии проходят (или ⚠️ по сценарию «источник завис» задокументирован и закрыт ➕ задачей)
- [x] run full test suite: `make test`
- [x] run integration tests: `make test-integration`
- [x] `make lint` без замечаний

### Task 9: [Final] Update documentation
- [x] создать CLAUDE.md, если появились неочевидные договорённости (служебная коллекция в базе цели и фильтр потока, файлы состояния и ключ кластера, Toxiproxy v2.12.0 из-за go 1.26, `directConnection=true`)
- [x] move this plan to `docs/plans/completed/`

## Post-Completion
*Items requiring manual intervention or external systems - no checkboxes, informational only*

**Manual verification**:
- несколько процессов на одной машине: сводка «на машине» у каждого совпадает с суммой файлов в `--state-dir`
- сверка итогов файлов с метриками облака (того, где развёрнуты кластеры): облако ожидаемо на несколько процентов больше
- обнаружение «зависшей» сети с настройками драйвера по умолчанию: ≈ 2 · (`connectTimeoutMS` 30 с + `heartbeatFrequencyMS` 10 с) ≈ 80 с — тесты с ускоренными настройками этого не показывают; если нужна быстрее реакция — задать эти параметры в URI или сделать сторож
