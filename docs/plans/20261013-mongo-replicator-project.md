# mongo-replicator: полноценный Go-проект

## Overview
- сейчас репликатор — один `main.go` (~420 строк) и `main_test.go` (4 юнит-теста) в корне, без `go.mod`, без окружения для запуска и проверки
- цель — оформить его как проект по образцу `~/sravni-git/f.sravni.ru`: `cmd/<app>/` + внутренний пакет, go-flags + zerolog, vendor, Makefile, docker-compose, golangci-lint
- добавить интеграционные тесты на реальных MongoDB 8.0 (источник) и 5.0 (цель) в docker-compose — именно там живут основные риски (повтор DDL, rename/dropTarget, совместимость индексов 8.0 → 5.0)
- форма — локальная CLI-утилита: прод-Dockerfile, `.k8s/`, `.gitlab-ci.yml`, `catalog-info.yaml`, `/metrics`, health не делаем
- алгоритм репликации не меняется — только перенос, логирование и одно изменение: сброс `pending` переезжает из `main` в начало `Run`

## Context (from discovery)
- исходники: `main.go` (event/namespace/toModel, batch, replicator с loadToken/saveToken/flush/applyDDL/run, hasCode, main с циклом ретраев), `main_test.go` (`TestDecodeAndModel`, `TestRawIsCopied`, `TestBatchOrder`, `TestDDLDescriptionParsing`)
- образец `~/sravni-git/f.sravni.ru`:
  - `cmd/f.sravni.ru/main.go` — `var opts struct` с go-flags (`long`/`env`/`default`/`required`, группы с `namespace`/`env-namespace`), `setupLog(debug)` (ConsoleWriter в dbg, иначе JSON с `m/e/l/t`), `var revision = "unknown"`, `os.Exit(2)` на ошибку флагов
  - внутренние пакеты приложения в `cmd/<app>/{api,store}`
  - `Makefile` (build/lint/test/run/docker-volumes через `docker compose run`), `Dockerfile.dev` на `artifactory.yc.prod.infra.sravni.market/repub/go/build:1.26`, `docker-compose.yml` с external-volumes `go-build-cache`/`go-lint-cache`, сервис `linter`
  - `.golangci.yml` (v2), `.gitignore`, `.dockerignore`, `vendor/`
- зависимости: `go.mongodb.org/mongo-driver/v2`, `github.com/jessevdk/go-flags`, `github.com/rs/zerolog`, `github.com/stretchr/testify`
- каталог — не git-репозиторий

## Development Approach
- **testing approach**: Regular (код уже есть и покрыт базовыми тестами; переносим, затем дописываем тесты)
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
- run tests after each change
- обратная совместимость CLI **не сохраняется**: go-flags не принимает длинные опции с одним дефисом (`-src` разбирается как `-s -r -c`), работает только `--src`; отметить в README

## Testing Strategy
- **unit tests**: `make test` (или `go test -race ./...` локально) — без MongoDB
- **integration tests**: `make test-integration` — `go test -count=1 -race -tags integration` в контейнере `app` против `mongo-src` (8.0) и `mongo-dst` (5.0); `-count=1` обязателен, иначе persistent `go-build-cache` отдаёт `(cached)` без обращения к MongoDB
- e2e UI-тестов в проекте нет

## Progress Tracking
- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix
- update plan if implementation deviates from original scope
- keep plan in sync with actual work done

## Solution Overview
```
mongo-replicator/
  cmd/mongo-replicator/
    main.go              опции, setupLog, подключения, цикл ретраев, коды выхода
    main_test.go         юнит-тесты маскировки URI
    .env                 (gitignored) SRC_URI/DST_URI для локального запуска
    replicator/
      event.go           event, namespace, toModel
      batch.go           batch
      checkpoint.go      константы чекпойнта, loadToken, saveToken
      ddl.go             applyDDL, hasCode, коды ошибок, IsHistoryLost
      replicator.go      Config, Replicator, New, Run, Applied, лог прогресса
      *_test.go          перенесённые юнит-тесты + IsHistoryLost
      integration_test.go  //go:build integration
  docs/plans/
  Dockerfile.dev  docker-compose.yml  Makefile  .golangci.yml
  .gitignore  .dockerignore  go.mod  go.sum  vendor/  README.md
```
- `main` только собирает зависимости; логика — в `replicator`, которую можно гонять в интеграционных тестах без CLI
- интерфейсов над `*mongo.Client` не вводим, имя базы чекпойнта не параметризуем (YAGNI)
- логи — глобальный `zerolog/log`, как в образце

## Technical Details

**Опции (`cmd/mongo-replicator/main.go`)**

| Флаг | Env | Default | |
|---|---|---|---|
| `--src` | `SRC_URI` | required | URI источника |
| `--dst` | `DST_URI` | required | URI цели |
| `--start-ts` | `START_TS` | `0` | unix-сек, старт при отсутствии чекпойнта |
| `--batch` | `BATCH_SIZE` | `1000` | валидация `>= 1` |
| `--dbg` | `DEBUG` | `false` | ConsoleWriter + debug |

- в debug-дампе опций пароль в URI заменяется на `xxxxx`. Маскировка строковая (userinfo между `://` и последним `@` до первого `/` или `?`), а не `url.Parse`: multi-host URI `h1:27017,h2:27017` `net/url` может не разобрать
- `mongo.Connect` в v2 ленивый (падает только на кривом URI), поэтому после него в `main` — `Ping` с таймаутом 10 с на обоих клиентах; недоступный сервер → exit 1, а не вечные ретраи. Алгоритм `Run` не меняется
- «нет чекпойнта и нет `--start-ts`» — перманентная ошибка: `Run` возвращает экспортируемый `ErrNoStartPoint`, `main` по нему выходит с 1 вместо ретраев
- коды выхода: `0` — остановка по сигналу; `1` — `IsHistoryLost`, `ErrNoStartPoint`, кривой URI или неуспешный `Ping`; `2` — ошибка опций/валидации
- `setupLog` как в образце, но JSON-ветка с `.With().Timestamp()` — в образце поле `t` объявлено, но не пишется, а для долгоживущей CLI с логом лага время нужно

**Публичный API `replicator`**
```go
type Config struct {
    StartTS   uint32
    BatchSize int
}
func New(src, dst *mongo.Client, cfg Config) *Replicator
func (r *Replicator) Run(ctx context.Context) error // r.pending = batch{} в начале
func (r *Replicator) Applied() int64                // atomic.Int64: читается из другой горутины в тестах
func IsHistoryLost(err error) bool                  // hasCode(err, 286, 280)
var ErrNoStartPoint = errors.New(...)               // вместо безымянной ошибки «чекпойнта нет и -start-ts не задан»
```

**Логи**: `log.Printf` → zerolog со структурными полями: `op`, `ns`, `to`, `desc` (operationDescription как строка), `applied`, `lag`. WARN для пропущенных событий/view/опций create, INFO для применённого DDL, прогресс — INFO раз в 10 с.

**docker-compose**
- `mongo-src`: `mongo:8.0`, `command: --replSet rs8 --bind_ip_all`; healthcheck `mongosh --quiet --eval "try{rs.status()}catch(e){rs.initiate({_id:'rs8',members:[{_id:0,host:'mongo-src:27017'}]})}; quit(db.hello().isWritablePrimary ? 0 : 1)"`, `interval: 2s`, `start_period: 30s`, `retries: 30` (`rs.status().ok` = 1 ещё в STARTUP2/SECONDARY, а `--eval` всегда exit 0 — поэтому явный `quit` по `isWritablePrimary`)
- `mongo-dst`: `mongo:5.0` (в нём есть `mongosh`, проверено), `--replSet rs5`, такой же healthcheck
- `app`: `Dockerfile.dev`, `./:/src`, `go-build-cache`, `SRC_URI=mongodb://mongo-src:27017/?replicaSet=rs8`, `DST_URI=mongodb://mongo-dst:27017/?replicaSet=rs5`, `depends_on` с `condition: service_healthy`. **Без `env_file`**: `.env` gitignored, и на свежем клоне compose упадёт на всех целях. Запуск против локальных mongo — только через `make run` (с хоста имя `mongo-src` из rs-конфига не резолвится)
- `linter`: как в образце, `go-lint-cache`

**Makefile — важные детали**
- `REVISION := $(shell git describe --always --dirty 2>/dev/null || echo unknown)` и `-ldflags "-X main.revision=$(REVISION)"` (голое `$(git ...)` make развернёт в пустую строку); до первого коммита будет `unknown` — ожидаемо
- `test`: `docker compose run --rm --no-deps app ...` — юнит-тестам mongo не нужны
- `test-integration`: `go test -count=1 -race -tags integration ./...`
- `run`: `go run ./cmd/mongo-replicator --dbg --start-ts $$(date +%s)` (без start-ts на пустом чекпойнте — `ErrNoStartPoint`)
- `down`: `docker compose down -v` (у mongo-образов `VOLUME /data/db` → иначе сироты)

**Интеграционные тесты (обвязка)**
- `TestMain`: `SRC_URI`/`DST_URI` обязательны, иначе `log.Fatal` с подсказкой `make test-integration`; клиент цели собирается как в `main` (`writeconcern.Majority()`, тот же AppName)
- тесты строго последовательные (общий чекпойнт, поток со всего кластера); база `it_<snake(t.Name())>`
- `prepare(t) (db string)`: дропает базу теста на **обоих** кластерах в начале (остатки упавших прогонов — контейнеры живут между запусками) и регистрирует такой же дроп в `t.Cleanup` **до** `start` (LIFO: сначала остановится репликатор, потом дроп); дропает `_replicator.checkpoint`
- `srcNow(t) uint32`: `operationTime.T` источника; ждёт, пока он станет больше секунды последней операции предыдущего теста, чтобы поток (StartTS округлён до секунды, `startAtOperationTime` включительный) не захватывал хвост предыдущего теста, включая его cleanup-`dropDatabase`
- `start(t, cfg) *runner`: `Run` в горутине, ошибка сохраняется; `stop()` идемпотентный (`sync.Once`), отменяет ctx, ждёт возврата, проверяет `errors.Is(err, context.Canceled)`; `start` сам регистрирует `t.Cleanup(stop)` — упавший `require` не оставит репликатор работать в следующих тестах
- `runner.waitSame(db)`: `require.Eventually` на `same(db)`, но сразу `t.Fatal`, если `Run` уже вернулся с ошибкой (не ждать таймаута)
- `runner.waitQuiet()`: чекпойнт-токен и `Applied()` не меняются между двумя чтениями с интервалом > 1.5 с (`maxAwaitTime` = 1 с) — закрывает гонку `flush` → `saveToken`, где данные уже видны, а чекпойнт ещё не сохранён
- `same(db) (ok bool, diff string)`: списки коллекций, документы по `_id` как `bson.Raw` байт в байт, индексы по `name/key/unique/partialFilterExpression/expireAfterSeconds`

## What Goes Where
- **Implementation Steps**: всё в этом репозитории — код, тесты, Makefile/compose, README
- **Post-Completion**: прогон против реальных кластеров, решение по post-images

## Implementation Steps

### Task 1: Каркас репозитория и конфиги

**Files:**
- Create: `go.mod`
- Create: `.gitignore`
- Create: `.dockerignore`
- Create: `.golangci.yml`

- [x] `git init` (репозиторий уже был инициализирован)
- [x] `go.mod`: `module mongo-replicator`, `go 1.26`
- [x] `.gitignore` и `.dockerignore` по образцу f.sravni.ru + `bin/`
- [x] `.golangci.yml` — копия из f.sravni.ru + `run.build-tags: [integration]`, иначе `integration_test.go` не линтуется
- [x] проверка: тестов в задаче нет (только конфиги); `go mod edit -json` разбирает `go.mod`

### Task 2: Пакет `replicator` — перенос кода

**Files:**
- Create: `cmd/mongo-replicator/replicator/event.go`
- Create: `cmd/mongo-replicator/replicator/batch.go`
- Create: `cmd/mongo-replicator/replicator/checkpoint.go`
- Create: `cmd/mongo-replicator/replicator/ddl.go`
- Create: `cmd/mongo-replicator/replicator/replicator.go`
- Create: `cmd/mongo-replicator/replicator/replicator_test.go`
- Delete: `main.go`, `main_test.go`

- [x] разложить код `main.go` по файлам без изменения алгоритма; `replicator` → экспортируемый `Replicator` с `Config`, `New`, `Run`, `Applied`
- [x] перенести `r.pending = batch{}` из цикла ретраев в начало `Run`; `applied` → `atomic.Int64`
- [x] добавить `IsHistoryLost(err)` поверх `hasCode(err, codeChangeStreamHistoryLost, codeChangeStreamFatal)` и `ErrNoStartPoint` вместо безымянной ошибки в `Run`
- [x] заменить `log.Printf` на zerolog со структурными полями (см. Technical Details)
- [x] перенести 4 существующих теста в `replicator_test.go` (`package replicator`) без изменения логики
- [x] написать тест `IsHistoryLost` / `hasCode` на testify (сразу тянем testify, чтобы он попал в vendor в Task 4): table-driven на `mongo.CommandError{Code: 286|280|26|...}`, обёрнутых через `fmt.Errorf("%w")`, обычной ошибке и `nil`
- [x] удалить корневые `main.go`/`main_test.go`
- [x] `go mod tidy`, `go test -race ./...` — должно пройти

### Task 3: CLI `cmd/mongo-replicator/main.go`

**Files:**
- Create: `cmd/mongo-replicator/main.go`
- Create: `cmd/mongo-replicator/main_test.go`

- [x] `var opts` на go-flags (таблица опций выше), `var revision = "unknown"`, `setupLog` из образца + `.Timestamp()` в JSON-ветке
- [x] `parseOpts(args []string) (options, error)` отдельной функцией: ошибка парсинга (кроме `--help`) и `BatchSize < 1` → exit 2
- [x] `maskURI(string) string`; debug-дамп опций с маскированными URI
- [x] подключение src (`AppName cs-replicator`) и dst (`+ writeconcern.Majority()`) + `Ping` с таймаутом 10 с, ошибка → exit 1; цикл ретраев (пауза 5 с) через `replicator.New(...).Run`; `IsHistoryLost`/`ErrNoStartPoint` → exit 1; сигнал → exit 0 с итоговым `applied`
- [x] написать тесты `maskURI`: с паролем, без userinfo, только user, multi-host, multi-host без `/` перед `?` (`mongodb://u:p@h1,h2?replicaSet=x`), `mongodb+srv://`, пароль с `@`/`:` в percent-encoding
- [x] написать тесты `parseOpts`: env-переменные, флаги перекрывают env, отсутствие `--src`/`--dst`, `--batch 0`
- [x] `go vet ./...`, `go test -race ./...` — должно пройти
- ➕ `--help` → exit 0 (в образце 2); пустые `--src`/`--dst` (например, `SRC_URI=`) тоже ошибка опций → exit 2; сигнал во время `Ping` → exit 0

### Task 4: Vendor и сборка

**Files:**
- Create: `Makefile`
- Create: `vendor/` (генерируется)

- [ ] `go mod tidy && go mod vendor`
- [ ] `Makefile`: `REVISION := $(shell ...)` и цель `build` (`tidy`, `vendor`, `go build -ldflags "-X main.revision=$(REVISION)" -o bin/mongo-replicator ./cmd/mongo-replicator`)
- [ ] проверить `make build`; `bin/mongo-replicator --help` показывает опции и env-переменные; без `--src` — exit 2; в логе `revision` = `unknown` (коммитов ещё нет), не пустая строка
- [ ] `go test -race ./...` — должно пройти

### Task 5: Окружение разработки: Dockerfile.dev и docker-compose

**Files:**
- Create: `Dockerfile.dev`
- Create: `docker-compose.yml`
- Modify: `Makefile`

- [ ] `Dockerfile.dev` на `artifactory.yc.prod.infra.sravni.market/repub/go/build:1.26` (без libwebp/swag)
- [ ] `docker-compose.yml`: `mongo-src`, `mongo-dst`, `app`, `linter` (см. Technical Details)
- [ ] Makefile: `default`, `docker-volumes`, `test`, `test-integration`, `lint`, `run`, `down` (детали — в Technical Details)
- [ ] проверить: `make docker-volumes`, `docker compose up -d mongo-src mongo-dst` → оба healthy за < 30 с, `db.hello().isWritablePrimary` = true
- [ ] `make test` (не поднимает mongo) и `make lint` — должны пройти. Правки по линтеру не меняют алгоритм: замечания `gocyclo`/`gocritic` на `Run`/`applyDDL` — через `//nolint:<linter> // причина`
- [ ] ⚠️ если artifactory недоступен локально — зафиксировать в плане и временно проверить на `golang:1.26-alpine`, в файле оставить artifactory-образ

### Task 6: Интеграционная обвязка + сценарий DML

**Files:**
- Create: `cmd/mongo-replicator/replicator/integration_test.go`

- [ ] `//go:build integration`, `TestMain`, хелперы `prepare`, `srcNow`, `start`/`runner` (`stop`, `waitSame`, `waitQuiet`), `same` (см. Technical Details)
- [ ] сценарий 1 `TestIntegrationDML`: insert/update(`$set`, `$unset`, `$inc`)/replace/delete в двух коллекциях; документы с Decimal128, датами, вложенными документами и массивами, нестандартным порядком полей
- [ ] проверить обвязку: тест с заведомо падающим `require` до `stop` не оставляет работающий репликатор (следующий тест проходит)
- [ ] `go mod tidy && go mod vendor`, если добавились зависимости (иначе `inconsistent vendoring`)
- [ ] проверить `make test-integration` — должно пройти; `make test` не цепляет интеграционные тесты

### Task 7: Интеграционные сценарии: транзакция, resume, повтор

**Files:**
- Modify: `cmd/mongo-replicator/replicator/integration_test.go`

- [ ] сценарий 2 `TestIntegrationTransaction`: коллекции создаются **до** транзакции (без неявного `create` внутри); `session.WithTransaction` по двум коллекциям (insert + update + delete)
- [ ] сценарий 3 `TestIntegrationResume`: прогон до совпадения, `waitQuiet`, `stop`; ещё N простых insert/update (без последующего delete тех же `_id` — update с `fullDocument: null` не считается); новый `Replicator` со `StartTS: 0` (успех доказывает старт с токена) → совпадение и `Applied() == N`
- [ ] сценарий 4 `TestIntegrationReplayIdempotent`: в окне — insert → update → delete одного `_id` (повтор update с null `fullDocument`), `create`, `createIndexes`, `dropIndexes` (игнорируемые коды 48/26/27); rename сознательно не включаем (ограничение). Прогон до совпадения, `waitQuiet`, `stop`, сброс чекпойнта, повторный прогон с тем же `StartTS`: сначала `Eventually(Applied() >= M)` (M — число DML-событий окна), потом `waitSame`, и `Run` не вернул ошибку
- [ ] сценарий `TestIntegrationNoStartPoint`: без чекпойнта и без `StartTS` `Run` сразу возвращает `ErrNoStartPoint`
- [ ] `make test-integration` — должно пройти

### Task 8: Интеграционные сценарии DDL

**Files:**
- Modify: `cmd/mongo-replicator/replicator/integration_test.go`

- [ ] сценарий 5 `TestIntegrationDDL`: `create`; `createIndexes` (обычный, unique, partial, TTL, составной); `dropIndexes`; `rename a → b` в существующую `b` (`dropTarget`); `drop`; `dropDatabase` отдельной базы; DML между DDL. TTL-индекс — на поле, которого нет в документах (иначе TTL-монитор удаляет документы на кластерах в разное время)
- [ ] проверить совпадение индексов 8.0 → 5.0; если 5.0 отвергает поле спецификации — ⚠️ зафиксировать и обсудить фильтрацию полей (алгоритм без согласования не менять)
- [ ] сценарий 6 `TestIntegrationUnsupportedDDL`: `collMod` на источнике → событие пропущено, последующий DML доезжает
- [ ] smoke-сценарий `TestIntegrationRestartAfterDDL`: rename `a → b`, `waitQuiet`, `stop`; новые insert в новую `a`; рестарт → состояние совпадает. Это не проверка «чекпойнт сохраняется сразу после DDL» — окно между `applyDDL` и `saveToken` из теста не поймать
- [ ] `make test-integration` — должно пройти

### Task 9: Тесты известных ограничений

**Files:**
- Modify: `cmd/mongo-replicator/replicator/integration_test.go`

- [ ] `TestLimitationUpdateLookupAfterRename`: тело воспроизводит потерю (update X в `a`, rename `a → b` до запуска репликатора, на цели в `b` старая версия X); в начале `t.Skip("известное ограничение, см. README#ограничения")`
- [ ] `TestLimitationRenameInCatchUpWindow`: `StartTS`, DML в `a`, rename `a → b`, «дамп» = копия коллекций источника на цель; прогон уничтожает непотронутые документы `b`; `t.Skip` с той же ссылкой
- [ ] вручную убрать `t.Skip` локально и убедиться, что оба теста действительно падают (затем вернуть)
- [ ] `make test-integration` — должно пройти (тесты помечены как skipped)

### Task 10: README

**Files:**
- Create: `README.md`

- [ ] назначение и требования (из шапки старого `main.go`)
- [ ] запуск: `make build`, опции/env (только `--src`, не `-src` — отличие от старой версии), пример с `.env`; локальная проверка — только `make run` (с хоста rs-имена compose не резолвятся); разработка: `make test`, `make test-integration`, `make lint`, `make down`
- [ ] гарантии: at-least-once + идемпотентные replace-upsert/delete по `_id`, чекпойнт на цели с `w:majority`, коды выхода
- [ ] раздел «Ограничения»: (a) updateLookup после rename/drop+recreate; (b) rename/drop+recreate в окне `start-ts` → конец дампа; будущий фикс — `changeStreamPreAndPostImages` + `fullDocument: required`; неподдерживаемые DDL (collMod, views, опции create, шардирование)

### Task 11: Verify acceptance criteria
- [ ] все пункты Overview реализованы, в корне нет старых `main.go`/`main_test.go`
- [ ] `make build` собирает бинарь с `revision`
- [ ] run full test suite: `make test`
- [ ] run integration tests: `make test-integration`
- [ ] `make lint` без замечаний
- [ ] ручной прогон `make run` (с `--start-ts` = сейчас) против локальных mongo: insert в `mongo-src` через `mongosh` доезжает до `mongo-dst`, Ctrl+C → exit 0
- [ ] `make run` при остановленном `mongo-dst` → exit 1 по `Ping`, без вечных ретраев

### Task 12: [Final] Update documentation
- [ ] update README.md if needed
- [ ] создать CLAUDE.md, только если появились неочевидные договорённости (сейчас его нет)
- [ ] move this plan to `docs/plans/completed/`

## Post-Completion
*Items requiring manual intervention or external systems - no checkboxes, informational only*

**Manual verification**:
- прогон против реальных кластеров (8.0 → 5.0) на стенде: скорость догоняния, лаг на рабочей нагрузке, поведение при failover цели
- проверить, что на источнике в окне догоняния не планируются rename/drop+recreate

**Будущие изменения** (отдельные задачи):
- post-images (`changeStreamPreAndPostImages` + `fullDocument: required`) для устранения ограничения (a)
- если утилита станет сервисом: прод-Dockerfile, `.k8s/`, `.gitlab-ci.yml`, `catalog-info.yaml`, `/metrics` с лагом
