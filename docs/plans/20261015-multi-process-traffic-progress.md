# Несколько процессов, учёт трафика и панель статуса

## Overview
- условия: источник и цель — MongoDB 8.0; конкретный провайдер и направление не фиксируются (кластеры могут смениться, возможна репликация в обратную сторону) — в коде, тестах и README не привязываться к провайдеру; реплицируется несколько баз — **по процессу на базу**, все пишут в один целевой кластер
- сейчас чекпойнт один на целевой кластер (`_replicator.checkpoint`, `_id: "main"`): второй процесс получит `ErrDBMismatch` или перетрёт чужую позицию → чекпойнт становится **на базу цели** (`_id: <dst-db>`) с **арендой**, защищающей от двух процессов на одну базу
- нужно считать **сетевой трафик** (реальные байты, для оценки стоимости и нагрузки): по процессу — в логе, накопительно — в чекпойнте, итог по всем процессам — в логе каждого процесса раз в минуту
- в терминале — **панель статуса** как у `docker build` (bubbletea v2, inline): последняя пачка/DDL, лаг, трафик процесса, итог по всем процессам; WARN/ERROR — над панелью; полный лог — в `--log-file`; флаг `--progress auto|tty|plain`
- тестовая цель docker-compose — `mongo:8.0`; драйвер остаётся v2.9.2
- работа идёт от `master` в ветке `multi-process-traffic`; ветка `target-wire8-dds` (DDS, wire 8) не вливается
- тулза ещё не запускалась на реальных базах: обратная совместимость (в т.ч. с `_id: "main"`) не нужна

## Context (from discovery)
- `cmd/mongo-replicator/replicator/checkpoint.go`: `ckptDB = "_replicator"`, `ckptColl = "checkpoint"`, `ckptID = "main"`; `checkpoint{Token, DB "db", DstDB "dst_db", ClusterTime}`; `loadCheckpoint` — `FindOne` по `_id`; `saveToken` — `ReplaceOne` upsert, **пропускает** неизменный токен (`bytes.Equal(token, r.saved)`) — на простое чекпойнт может не писаться долго; `ckpt()` — с `readpref.Primary()`
- `cmd/mongo-replicator/replicator/replicator.go`: `Config{StartTS, BatchSize, DB, DstDB, ApplyDrops}` (`:49`), `Replicator` (`:58`), `New` (`:81`), `flush` (`:96`), `Run` (`:150`, цикл `:204-273`), `logProgress` раз в 10 с (`:278`)
- `cmd/mongo-replicator/replicator/batch.go`: `batch` считает только `n` — для разбивки по видам операций нужно добавить учёт
- `cmd/mongo-replicator/replicator/ddl.go`: `applyDDL` (`:95`) — `createIndexes` с w:majority ждёт завершения сборки индекса (минуты–часы на большой коллекции)
- `cmd/mongo-replicator/main.go`: `prepareClients` → `run(opts, srcOpts, dstOpts)` (`:188`); цикл повторов (`:209-226`): перманентная ошибка — `return 1` **изнутри цикла** (`:214-216`); `hint` (`:249-252`) — текст `ErrDBMismatch` про «второй экземпляр»; результат `setupLog` (файл) сейчас в `main` отбрасывается
- `integration_test.go`: `runner` (`:175-210`, `stop` проверяет `context.Canceled`), `runErr` (`:737`), `writeCkpt` (`:1096`, `_id: ckptID`), `TestIntegrationDBMismatch`/`DstDBMismatch` (`:1109-1142`), `TestIntegrationSaveTokenUnparsedTime` (`:1145`, прямые `New(...).Run` и `saveToken`); общие `srcClient`/`dstClient` из `TestMain` — без диалера
- `docker-compose.yml`: `mongo-dst` — `mongo:5.0`, `--replSet rs5`; `rs5` в env `app`; ссылки на 5.0 — `integration_test.go:414,923`, `replicator.go:3`, `README.md:5`; Makefile 5.0/`rs5` не упоминает
- `README.md`: `:5` (назначение — «догонять кластер более старой версии»), `:45,81,169,279` (`rs5`), `:74,131,133` (один чекпойнт, `_id: "main"`, создание при первом сохранении), `:96,211` (`checkpoint.drop()`), `:280` (`findOne({_id: "main"})`)
- драйвер v2.9.2 (проверено ревью): `options.ContextDialer` (`mongo/options/clientoptions.go:64`); `opts.Dialer` используется пулом, heartbeat и RTT-мониторингом (`server.go:198,218,811`); TLS оборачивает выданное соединение (`connection.go:282`) → считаются зашифрованные байты; SRV/DNS идёт мимо диалера; по умолчанию — `&net.Dialer{}`
- bubbletea: актуальные — `charm.land/bubbletea/v2` v2.1.0 и `charm.land/lipgloss/v2` v2.0.6 (v1 заморожена на v1.3.10); в v2 `Model.View()` возвращает `tea.View` (`tea.NewView(s)`); есть `WithInput(nil)`, `WithoutSignalHandler`, `Send`, `Kill`, `Wait`, Cmd `tea.Println`; **`Program.Println` — голая отправка в небуферизованный канал без `select` на `ctx.Done()` — блокирует навсегда после выхода программы**; `go.mod` v2 требует go 1.26 — совпадает с проектом
- команды: `make test`, `make test-integration`, `make lint`, `make build`

## Development Approach
- **testing approach**: Regular (код, затем тесты в той же задаче)
- ветка `multi-process-traffic` от `master` (создана вместе с планом)
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
- **unit tests**: `make test` — диалер со счётчиками, дельты трафика, тексты ошибок аренды, `permanent`/`hint`, `status`, `renderPanel`, `parseOpts`, неблокирующий лог после выхода панели
- **integration tests**: `make test-integration` — `mongo:8.0 → mongo:8.0`; несколько репликаторов на один целевой кластер; аренда (интервалы — через поля `Replicator`, в тестах сотни миллисекунд); трафик в чекпойнте и сводка
- терминальный вывод панели — ручная проверка (Post-Completion)

## Progress Tracking
- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix
- update plan if implementation deviates from original scope
- keep plan in sync with actual work done

## Solution Overview
- **чекпойнт на процесс**: `_id` = `--dst-db`; `find()` по `_replicator.checkpoint` показывает все процессы
- **аренда**: в документе `owner` (уникален на экземпляр `Replicator`) + `lease_until` (серверное время `$$NOW`); захват при старте `Run`; продление — **в отдельной горутине по таймеру**, не зависящей от блокирующих операций основного цикла; потеря аренды отменяет `Run`; **самоостановка**, если продление не удаётся дольше 2/3 TTL; снятие при любом выходе `run()` (`defer`)
- **трафик**: свой `Dialer` на каждый клиент, `countingConn` считает байты → реальные байты по сети (с TLS и сжатием); в логе — по процессу, в чекпойнте — накопительно (атомарный пайплайн с `$ifNull`), раз в минуту — агрегация по всем документам
- **панель**: `Replicator` отдаёт снимок `Status()`; bubbletea v2 (inline) перерисовывает раз в секунду; лог WARN/ERROR — через `p.Send` → `tea.Println` (не блокирует после выхода программы); INFO — только в файл
- порядок `commit()` → DDL → `saveToken` и алгоритм применения не меняются

## Technical Details

### Чекпойнт и аренда
```js
{
  _id: "urlshrinker_v2",                         // = --dst-db
  db: "urlshrinker", dst_db: "urlshrinker_v2",
  token, clusterTime, updatedAt,
  owner: "replica-host:12345:9f3a1c2e",          // hostname:pid:<8 hex случайных> — на экземпляр Replicator
  lease_until: ISODate(...),                     // серверное время + TTL
  traffic: {src_in, src_out, dst_in, dst_out}    // накопительно, байты (int64)
}
```
- **owner** генерируется в `New` (`hostname:pid:<nonce>`): два контейнера с одинаковыми hostname и PID 1 и два `Replicator` в одном тестовом бинаре — разные владельцы; повторный `Run` того же `Replicator` — тот же владелец
- поля `Replicator`: `leaseTTL` (60 с), `leaseRenew` (10 с), `clusterEvery` (60 с) — значения по умолчанию из констант, в тестах переопределяются
- **захват** (`acquireLease`, в начале `Run` до `loadCheckpoint`) — ⚠️ в два запроса: `$expr` в фильтре upsert сервер отвергает («$expr is not allowed in the query predicate for an upsert»)
  - update (оба шага): `[{$set: {owner: me, lease_until: {$add: ["$$NOW", ttlMs]}}}]`
  - шаг 1, `UpdateOne` без upsert: `{_id: dst, $or: [{owner: {$exists: false}}, {owner: me}, {$expr: {$lt: ["$lease_until", "$$NOW"]}}]}` — свободная, своя или истёкшая; `MatchedCount > 0` → захвачено
  - шаг 2, `UpdateOne` с upsert: `{_id: dst, owner: {$exists: false}}` — документа нет (или освобождён между шагами); при upsert-вставке `_id` берётся из равенства верхнего уровня; при живой чужой аренде — E11000 (`mongo.IsDuplicateKeyError`); два одновременных старта — проигравший тоже получает E11000
  - E11000 → прочитать документ: есть живой владелец → `ErrLeaseHeld` (`базу <dst> уже реплицирует <owner>, аренда до <lease_until>`); владельца нет (освобождён между попытками) → один повтор захвата
- **продление**: горутина, запускается `Run` после захвата, контекст производный от контекста `Run` (`context.WithCancelCause`); раз в `leaseRenew`:
  - `UpdateOne({_id, owner: me}, [{$set: {lease_until: {$add: ["$$NOW", ttlMs]}, "traffic.src_in": {$add: [{$ifNull: ["$traffic.src_in", 0]}, Δ]}, … (4 поля)}}])` — дельты `int64`; пайплайн с `$ifNull` — один атомарный запрос, поля создаются при первом продлении
  - `MatchedCount == 0` → отмена `Run` с причиной `ErrLeaseLost`
  - сетевая ошибка → повтор на следующем тике; если последнее успешное продление старше `2/3 · leaseTTL` — **самоостановка**: отмена `Run` с причиной `errLeaseExpiring` (временная → цикл повторов → новый захват), чтобы прекратить запись до того, как аренда истечёт и её сможет забрать другой процесс
  - `Run` ждёт завершения горутины перед возвратом; возвращает причину отмены, если она есть
- остаточный риск (README): уже отправленная на сервер операция (например, `BulkWrite`) может завершиться на сервере и после отмены на клиенте
- **сохранение** (`saveToken`): `UpdateOne({_id, owner: me}, {$set: {token, clusterTime, updatedAt, db, dst_db}})` — без upsert (документ создан захватом), не затирает `owner`/`traffic`; `MatchedCount == 0` → `ErrLeaseLost`
- **освобождение** (`Release(ctx)`): `UpdateOne({_id, owner: me}, {$inc: {traffic.*: Δ}, $unset: {owner: "", lease_until: ""}})`; документ без `token` (старт закончился `ErrNoStartPoint`, неверный `--dst-db`) — удаляется (`DeleteOne({_id, owner: me, token: {$exists: false}})`), чтобы не висеть в сводке; ошибки — WARN
- **`main`**: `defer` с `Release` (свой контекст 5 с) сразу после `replicator.New` — срабатывает на всех путях выхода, включая `return 1` при перманентной ошибке и раскрутку паники; итог INFO `трафик` — после него
- `loadCheckpoint`: документ без `token` = «чекпойнта нет»
- `checkDBs`: с ключом по базе цели может разойтись только база источника; другой `--dst-db` → другой документ → `ErrNoStartPoint`
- ошибки `ErrLeaseHeld`, `ErrLeaseLost` — перманентные (`permanent`/`hint`): held — «другой процесс реплицирует эту базу; если он упал — аренда истечёт через ≤60 с»; lost — «аренду забрал другой процесс — проверьте, не запущены ли два репликатора на одну базу»; `errLeaseExpiring` — временная

### Трафик
```go
// replicator/traffic.go
type Traffic struct{ SrcIn, SrcOut, DstIn, DstOut atomic.Uint64 }
func (t *Traffic) SrcDialer() options.ContextDialer
func (t *Traffic) DstDialer() options.ContextDialer
type countingConn struct{ net.Conn; in, out *atomic.Uint64 }  // Read/Write считают n и при ошибке; ошибки не оборачиваются
```
- диалер — обёртка над нулевым `net.Dialer{}` (как у драйвера по умолчанию)
- `main`: `tr := &replicator.Traffic{}`; `srcOpts.SetDialer(tr.SrcDialer())`, `dstOpts.SetDialer(tr.DstDialer())` **до** `connect`; `Config.Traffic = tr` (nil — счётчики не ведутся)
- учитывается: всё через сокеты драйвера (поток, `getMore`, `updateLookup`, записи, чекпойнт, аренда, `ping`, heartbeat и RTT-мониторинг, аутентификация, TLS, сжатие); не учитывается: заголовки TCP/IP и ретрансмиты, DNS, другие программы, внутренняя репликация кластеров, ≤10 с перед `kill -9`
- **лог процесса**: в `прогресс` — `src_in`…`dst_out` (байты с начала процесса) и `*_bps` (средняя за интервал); при выходе — INFO `трафик` (четыре суммы + `uptime`)
- **накопление**: снимок значений на момент последней успешной записи (продление/освобождение); дельта = текущее − снимок; снимок сдвигается только после успеха
- **сводка** (`clusterTraffic`, раз в `clusterEvery`): `Aggregate` по `_replicator.checkpoint`, только документы с `token`: `$group: {_id: null, processes: {$sum: 1}, active: {$sum: {$cond: [{$gt: ["$lease_until", "$$NOW"]}, 1, 0]}}, src_in: {$sum: "$traffic.src_in"}, …}` (`$sum` по отсутствующему полю — 0) → INFO `трафик всех процессов` (`processes`, `active`, `src_in`, `src_out`, `dst_in`, `dst_out` — числа); ошибка → WARN

### Статус для панели
```go
type Status struct {
    DB, DstDB  string
    Started    time.Time
    Applied    int64
    Rate       float64          // операций/с за последний интервал прогресса
    AppliedAt  time.Time
    Lag        time.Duration    // -1 — неизвестен
    LastBatch  *BatchInfo       // время, ins/upd/rep/del, коллекции (до 3 + «и ещё N»)
    LastDDL    *DDLInfo         // время, op, ns, детали
    Traffic    TrafficInfo      // байты процесса
    Cluster    *ClusterTraffic  // последняя сводка (nil — ещё не было) + её время
    State      string           // "запуск" | "работает" | "повтор" | "остановлен"
    Retry      int
    LastErr    string
}
func (r *Replicator) Status() Status
func (r *Replicator) SetRetry(err error)   // из цикла повторов main, перед паузой; Retry++ (сбрасывает Run)
```
- `LastBatch`/`LastDDL`/`Cluster` — при каждом обновлении создаются новые значения, существующие не изменяются (копии из `Status()` безопасны без глубокого копирования)
- обновления: `flush` → `LastBatch` (`batch.add` учитывает вид операции); `applyDDL` → `LastDDL`; `logProgress` → `Rate`, `Lag`, `AppliedAt`; `clusterTraffic` → `Cluster`; **`Run` сам** выставляет `State = "работает"` и сбрасывает `Retry`/`LastErr` после открытия потока
- скорости трафика (`/с`) считает модель панели по разнице снимков между тиками
- ➕ `New` — «запуск»; возврат `Run` — «остановлен» (`LastErr` — если не отмена вызывающим), временную ошибку `main` сразу переводит в «повтор»; `LastDDL` пишется и для пропущенных DDL (view, неподдерживаемые, drop без коллекции на цели) с пометкой в `Details`

### Панель (`cmd/mongo-replicator/progress.go`)
- библиотеки: `charm.land/bubbletea/v2` (v2.1.0), `charm.land/lipgloss/v2` (v2.0.6)
- `--progress auto|tty|plain` / `PROGRESS`: go-flags `choice`, `default:"auto"`; пустое — ошибка; `auto` → `tty`, если stdout — терминал и `TERM != "dumb"`; иначе `plain`
- `tea.NewProgram(model, tea.WithInput(nil), tea.WithoutSignalHandler())`, без alt screen; `Init` → `tea.Tick(time.Second)`; `Update`:
  - `tick` → снимок `r.Status()`, следующий тик
  - `logLineMsg` → `tea.Println(string)` (Cmd печатает над панелью)
  - `finalMsg` → снимок и `tea.Quit`
- `View() tea.View { return tea.NewView(renderPanel(m.status, m.prevTraffic, time.Now())) }`; `renderPanel` — чистая функция, строки:
  ```
  urlshrinker → urlshrinker_v2 · работает 2ч14м · лаг 1с (применено до 12:01:33)
  операции: 1 284 551 всего · 312/с · пачка 12:01:34: 48 (ins 30 · upd 15 · del 3) в links, clicks
  DDL: 11:40:02 createIndexes urlshrinker.links (slug_1)
  трафик процесса: источник ↓ 3.1 GiB 420 KiB/с ↑ 12 MiB · цель ↓ 40 MiB ↑ 2.9 GiB 390 KiB/с
  все процессы (3, активно 2, на 12:01): источник ↓ 12.4 GiB ↑ 0.3 GiB · цель ↓ 0.2 GiB ↑ 11.9 GiB
  состояние: работает
  ```
  lipgloss: подписи приглушённые, «повтор» — жёлтый, ошибка — красный
- **лог в `tty`**: `teaWriter` — простой форвардер: каждый `Write` (`ConsoleWriter` пишет событие одним `Write`) → `p.Send(logLineMsg(strings.TrimRight(line, "\n")))`; `Send` после выхода программы — no-op, не блокирует; фильтр уровня — `FilteredLevelWriter` в `newLogger` (консоль в `tty` — `max(warn, --log-level)`); без `--log-file` — WARN при старте `полный лог не пишется: добавьте --log-file`
- `setupLog` возвращает файл — `main` сохраняет его, чтобы в `run` пересобрать логгер с приёмником `teaWriter` (файл — тот же)
- **жизненный цикл в `run()`**:
  1. пароли и стартовые строки (`revision`, `лог`) — до программы
  2. логгер → консоль через `teaWriter`; `go p.Run()` (паники внутри `Run` и модели bubbletea ловит сама)
  3. `defer func(){ if rec := recover(); rec != nil { p.Kill(); p.Wait(); panic(rec) } }()` — паника в основной горутине не оставит терминал сломанным
  4. выход: `p.Send(finalMsg{})` → `p.Wait()`; **затем вернуть логгер на обычный консольный приёмник** (уровень `--log-level`) — иначе итог INFO `трафик` отфильтруется
  5. `Release` (defer), итог `трафик`, ERROR с подсказкой — обычной печатью
- `plain` — нынешний построчный лог плюс строки `трафик` и `трафик всех процессов`
- набранные на клавиатуре символы при `WithInput(nil)` эхом попадают в терминал поверх панели — допустимо, отметить в README

## What Goes Where
- **Implementation Steps**: код, тесты, compose, README
- **Post-Completion**: ручная проверка панели в терминалах, несколько процессов в `tmux`, сверка трафика с метриками облака

## Implementation Steps

### Task 1: Тестовая цель `mongo:8.0`

**Files:**
- Modify: `docker-compose.yml`
- Modify: `cmd/mongo-replicator/replicator/integration_test.go`, `cmd/mongo-replicator/replicator/replicator.go` (комментарии про 5.0)

- [x] `mongo-dst`: `mongo:8.0`, `--replSet rsdst`, healthcheck как у `mongo-src` (`mongosh`, `db.hello().isWritablePrimary`); env `app`: `DST_URI`/`IT_DST_URI` — `replicaSet=rsdst`
- [x] комментарии «цель 5.0» (`integration_test.go:414,923`, `replicator.go:3`) → «цель 8.0»
- [x] `docker compose down -v`, `make test-integration` — должно пройти без изменений кода
- [x] `make test`, `make lint` — должно пройти

### Task 2: Чекпойнт по базе цели

**Files:**
- Modify: `cmd/mongo-replicator/replicator/checkpoint.go`
- Modify: `cmd/mongo-replicator/main.go`, `cmd/mongo-replicator/main_test.go` (`hint`)
- Modify: `cmd/mongo-replicator/replicator/integration_test.go`

- [x] убрать `ckptID`; `loadCheckpoint`/`saveToken` — по `_id: r.cfg.DstDB`
- [x] `hint` для `ErrDBMismatch`: «чекпойнт этой базы цели записан для другой базы источника: верните прежний --db или удалите чекпойнт этой базы (`deleteOne({_id: "<dst-db>"})`) и залейте заново»; обновить `TestPermanent`
- [x] `writeCkpt` — параметр `_id` = база цели; `ckptDB`-хелпер — по `_id`
- [x] переписать `TestIntegrationDBMismatch`/`DstDBMismatch`: та же база цели, другая база источника → `ErrDBMismatch`; другая база цели → `ErrNoStartPoint`
- [x] write integration test `TestIntegrationTwoProcessesOneTarget`: два репликатора одновременно — `db_a → db_a` и `db_b → db_b`; DML в обе; обе базы сходятся; в `_replicator.checkpoint` два документа
- [x] `make test`, `make test-integration`, `make lint` — должно пройти

### Task 3: Аренда

**Files:**
- Create: `cmd/mongo-replicator/replicator/lease.go`
- Create: `cmd/mongo-replicator/replicator/lease_test.go`
- Modify: `cmd/mongo-replicator/replicator/checkpoint.go`
- Modify: `cmd/mongo-replicator/replicator/replicator.go`
- Modify: `cmd/mongo-replicator/main.go`, `cmd/mongo-replicator/main_test.go`
- Modify: `cmd/mongo-replicator/replicator/integration_test.go`

- [x] `owner` на экземпляр (`hostname:pid:<nonce>`) в `New`; поля `leaseTTL`/`leaseRenew`; `acquireLease` (`UpdateOne` + upsert, E11000 → `ErrLeaseHeld` или один повтор), горутина продления с отменой `Run` (`ErrLeaseLost`, `errLeaseExpiring`), `Run` ждёт горутину; `Release(ctx)` (снятие; удаление документа без `token`)
- [x] `saveToken` → `UpdateOne({_id, owner})` без upsert (`MatchedCount == 0` → `ErrLeaseLost`)
- [x] `main`: `defer Release` сразу после `replicator.New`; `permanent`/`hint` для `ErrLeaseHeld`/`ErrLeaseLost`
- [x] интеграционная обвязка: `runner.stop` и `runErr` вызывают `Release` (контекст с таймаутом); `TestIntegrationSaveTokenUnparsedTime` — явный `acquireLease` перед `saveToken`, `Release` перед `r2.Run`; тестовые `leaseRenew` ≈ 300 мс, `leaseTTL` ≈ 2 с
- [x] write unit tests: тексты `ErrLeaseHeld` (владелец, время); `permanent`/`hint` для обеих ошибок; формат `owner` (уникален между двумя `New`); самоостановка по времени последнего успешного продления (чистая функция/часы через поле)
- [x] write integration tests (`runErr`/`Eventually`): A работает → B на ту же базу цели — `ErrLeaseHeld`; A остановлен (`stop` с `Release`) → B стартует сразу; документ с чужим `owner` и прошедшим `lease_until` → старт; подмена `owner` во время работы A → A завершается с `ErrLeaseLost` (`Eventually`, несколько секунд); повторный `Run` того же `Replicator` после временной ошибки — захват своей аренды; **продление идёт при заблокированном цикле** — хук в `applyDDL` (поле-функция для тестов) засыпает дольше `leaseTTL`, аренда в документе продолжает продлеваться, B получает `ErrLeaseHeld`; `ErrNoStartPoint` → после `Release` документа нет
- [x] `make test`, `make test-integration`, `make lint` — должно пройти

### Task 4: Счётчики трафика

**Files:**
- Create: `cmd/mongo-replicator/replicator/traffic.go`
- Create: `cmd/mongo-replicator/replicator/traffic_test.go`
- Modify: `cmd/mongo-replicator/replicator/replicator.go`
- Modify: `cmd/mongo-replicator/main.go`
- Modify: `cmd/mongo-replicator/replicator/integration_test.go`

- [x] `Traffic`, `countingConn`, `SrcDialer()`/`DstDialer()`; `Config.Traffic`; диалеры в `main` до `connect`
- [x] строка `прогресс`: `src_in`…`dst_out` и `*_bps`; INFO `трафик` при выходе (`main`, после `Release`)
- [x] write unit tests: TCP-слушатель на `127.0.0.1`, через `countingConn` отправить N и получить M байт → `out == N`, `in == M` для обоих диалеров; частичная запись/чтение с ошибкой — `n` учтён, ошибка не обёрнута (`errors.As` на `net.Error`); `Dial` на закрытый порт — ошибка, счётчики 0
- [x] write integration test: свои клиенты с `SetDialer(tr.SrcDialer())`/`DstDialer()` (общие из `TestMain` без диалера), закрываются в cleanup; после DML `SrcIn`/`DstOut` > 0 и `SrcIn` ≥ суммы `bsonsize` записанных документов (комментарий: оценка верна без `compressors` в URI)
- [x] `make test`, `make test-integration`, `make lint` — должно пройти

### Task 5: Трафик в чекпойнте и сводка по процессам

**Files:**
- Modify: `cmd/mongo-replicator/replicator/lease.go`
- Modify: `cmd/mongo-replicator/replicator/traffic.go`
- Modify: `cmd/mongo-replicator/replicator/replicator.go`
- Modify: `cmd/mongo-replicator/replicator/traffic_test.go`
- Modify: `cmd/mongo-replicator/replicator/integration_test.go`

- [x] дельты в пайплайне продления (`$ifNull`) и в `$inc` освобождения; снимок сдвигается только после успеха
- [x] `clusterTraffic` раз в `clusterEvery` (только документы с `token`); INFO `трафик всех процессов`; ошибка → WARN
- [x] write unit tests: дельта (успех сдвигает снимок; неуспех — нет, следующая дельта включает пропущенное)
- [x] write integration tests (`TestIntegrationTrafficCheckpoint`, `TestIntegrationClusterTraffic`; первая сводка — сразу после старта, затем раз в `clusterEvery`; `Release` без своей аренды не сдвигает снимок): первое продление на документе без `traffic` → поля `traffic.*` — `long` > 0; после DML и `Release` — `traffic.src_in`, `traffic.dst_out` > 0; два процесса остановлены и освобождены → `clusterTraffic` = сумма полей двух документов, `processes = 2`, `active = 0`; один процесс работает → `active = 1` (`Eventually`)
- [x] `make test`, `make test-integration`, `make lint` — должно пройти

### Task 6: Снимок статуса

**Files:**
- Create: `cmd/mongo-replicator/replicator/status.go`
- Create: `cmd/mongo-replicator/replicator/status_test.go`
- Modify: `cmd/mongo-replicator/replicator/batch.go`
- Modify: `cmd/mongo-replicator/replicator/replicator.go`
- Modify: `cmd/mongo-replicator/replicator/ddl.go`
- Modify: `cmd/mongo-replicator/main.go`

- [x] `Status`, `BatchInfo`, `DDLInfo`, `Status()`, `SetRetry`; `batch.add` учитывает вид операции; обновления в `flush`, `applyDDL`, `logProgress`, `clusterTraffic`; `Run` выставляет «работает» и сбрасывает повтор после открытия потока; `main` вызывает `SetRetry` перед паузой повтора
- [x] write unit tests (⚠️ успешный `flush` без сервера не проверить: состав пачки — через `batchInfo`, `flush` — ошибка и пустой flush не меняют `LastBatch`, запись пачки — в `TestIntegrationStatus`): `flush` с пачкой (ins/upd/rep/del в нескольких коллекциях) → `LastBatch` (счётчики, список коллекций с усечением «и ещё N»); `applyDDL` → `LastDDL`; `SetRetry` → «повтор»; копия из `Status()` не меняется при последующих обновлениях
- [x] write integration test (`TestIntegrationStatusRetry`, `TestIntegrationStatus`): после временной ошибки и успешного повторного `Run` состояние снова «работает»
- [x] `make test` (с `-race`), `make test-integration`, `make lint` — должно пройти

### Task 7: Панель `--progress` на bubbletea v2

**Files:**
- Create: `cmd/mongo-replicator/progress.go`
- Create: `cmd/mongo-replicator/progress_test.go`
- Modify: `cmd/mongo-replicator/main.go`
- Modify: `cmd/mongo-replicator/main_test.go`
- Modify: `go.mod`, `go.sum`, `vendor/`

- [x] опция `--progress`/`PROGRESS` (`choice`, `default:"auto"`, `TERM=dumb` → `plain`); `clearEnv` + `PROGRESS`
- [x] сначала код с импортами `charm.land/bubbletea/v2` и `charm.land/lipgloss/v2`, затем `go get … && go mod tidy && go mod vendor`
- [x] модель (`tick`, `logLineMsg` → `tea.Println`, `finalMsg` → `tea.Quit`), `renderPanel`, `teaWriter` (форвардер через `p.Send`), жизненный цикл в `run()` по Technical Details (сохранение файла лога из `setupLog`, восстановление логгера после `p.Wait()`, `recover` с `p.Kill()`/`p.Wait()`)
  - ⚠️ вместо `p.Wait()` — свой канал `done` вокруг `go p.Run()`: канал `Wait` создаёт `Run`, до его старта `Wait` висит; после `done` `teaWriter` пишет в stdout напрямую (если `Run` упал на старте, WARN/ERROR не теряются)
  - ⚠️ `View` заканчивается `\n`: при выходе рендерер стирает экран от последней строки кадра — без перевода строки пропадала строка состояния
  - ➕ финальный снимок — без скоростей (доли секунды с последнего тика давали шум); цикл повторов вынесен в `replicate`, итоги (ERROR с подсказкой, «остановлен по сигналу») — после остановки панели; `startPanel` принимает доп. опции программы (в тесте — `WithWindowSize`, вывод не терминал)
- [x] write unit tests `renderPanel`: обычная работа (форматирование байт и длительностей), без DDL и без сводки, «повтор» с ошибкой, лаг неизвестен; форматтеры байт (`B`, `KiB`, `MiB`, `GiB`) и длительностей
- [x] write unit tests `parseOpts`: `auto|tty|plain`, неверное и пустое → ошибка
- [x] write unit test: запись в `teaWriter` после завершения программы (запущенная и сразу остановленная программа с `WithInput(nil)` и выводом в буфер) не блокирует (тест с таймаутом)
- [x] `make test`, `make lint`, `make build`; ручной запуск `make run` — панель видна; Ctrl+C оставляет финальную панель и итог `трафик` (проверено в tmux 170×30: панель, пачка, DDL, сводка; выход по SIGINT и по занятой аренде)
  - ⚠️ эхо `^C` от терминала портит одну-две ячейки финальной панели (рендерер о нём не знает) — упомянуть в README вместе с эхом клавиш

### Task 8: README

**Files:**
- Modify: `README.md`

- [x] `:5` назначение — переформулировать (миграция и непрерывная репликация одной базы между кластерами, в т.ч. одной версии); «Требования»: цель — MongoDB 8.0; права без изменений; `rs5` → `rsdst` (`:45`, `:81`, `:169`, `:279`)
- [x] чекпойнт: `:74`, `:131`, `:133`, `:280` — документ на базу цели (`_id: <dst-db>`), создаётся при захвате аренды; `:96`, `:211` — вместо `checkpoint.drop()` — остановить процесс этой базы и `deleteOne({_id: "<dst-db>"})` (общий `drop()` сотрёт позиции и трафик всех процессов)
- [x] «Несколько процессов»: один процесс на базу; аренда (`ErrLeaseHeld`/`ErrLeaseLost`, ≤60 с после `kill -9`, самоостановка при потере связи с целью, остаточный риск уже отправленной операции); как посмотреть все процессы
- [x] «Трафик»: что считается и что нет (таблица), направления (`src_in` — основной объём, egress источника), поля лога (по процессу) против накопления в чекпойнте (по базе), неточности (≤10 с, двойной учёт одного интервала), сводка раз в минуту (только документы с позицией), запрос итога в GiB, сброс вместе с чекпойнтом, облако насчитает на несколько процентов больше
- [x] «Панель статуса»: `--progress auto|tty|plain` (`TERM=dumb` → `plain`), строки панели, WARN/ERROR над панелью, INFO — только в файл, рекомендация `--log-file`, эхо нажатых клавиш

### Task 9: Verify acceptance criteria
- [ ] несколько процессов на один целевой кластер работают независимо; второй процесс на ту же базу не стартует; аренда не истекает при долгом DDL
- [ ] трафик виден в логе процесса, накапливается в чекпойнте, сводка раз в минуту
- [ ] панель в терминале, `plain` при перенаправлении stdout; WARN/ERROR над панелью; выход по ошибке и по Ctrl+C не зависает
- [ ] run full test suite: `make test`
- [ ] run integration tests: `make test-integration`
- [ ] `make lint` без замечаний

### Task 10: [Final] Update documentation
- [ ] обновить CLAUDE.md, если появились неочевидные договорённости (ключ чекпойнта, аренда в горутине, `$$NOW`, `p.Send` вместо `p.Println`)
- [ ] move this plan to `docs/plans/completed/`

## Post-Completion
*Items requiring manual intervention or external systems - no checkboxes, informational only*

**Manual verification**:
- панель в `tmux` и по ssh: изменение размера окна, длинные имена коллекций, Ctrl+C и выход по перманентной ошибке — терминал в порядке (курсор, эхо), финальная панель и итог остаются на экране
- несколько процессов в разных окнах `tmux`: сводка «все процессы» у каждого совпадает; второй процесс на ту же базу — `ErrLeaseHeld`; `kill -9` → новый процесс стартует не позже чем через 60 с
- долгий `createIndexes` на большой коллекции источника — аренда не истекает, второй процесс не может захватить базу
- сверка счётчиков трафика с метриками облака (того, где развёрнуты кластеры) за сутки: облако ожидаемо на несколько процентов больше
