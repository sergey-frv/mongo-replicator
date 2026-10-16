# Применение правил «Тесты» из CLAUDE.md к существующим тестам

## Overview
- раздел «Тесты» в `CLAUDE.md` сейчас действует только для **новых** тестов. Существующие написаны в смешанном стиле:
  - часть проверок через `t.Fatal*`/`t.Error*`;
  - опрос в циклах со `time.Sleep`;
  - ветвление по полям таблицы в теле теста;
  - длинные тела без явных блоков
- цель — привести **все** тесты к правилам, чтобы тест читался одинаково в любом файле, и затем распространить правила на весь код тестов
- применяются четыре правила:
  - проверки — testify;
  - `time.Sleep` → `require.Eventually`/`assert.Never`, кроме законных снов;
  - таблицы с логикой → отдельные тесты;
  - структура «дано → действие → проверка»
- главный риск рефакторинга тестов — тест остаётся зелёным, но перестаёт ловить баг. Поэтому **каждый изменённый тест и хелпер проверяется мутацией**: код временно ломается, тест должен упасть
- код вне `_test.go` не меняется

## Context (from discovery)
- ветка `multi-process-traffic`; тесты разложены по областям (план `completed/20261015-split-tests-by-area.md`), правила — в разделе «Тесты» `CLAUDE.md`
- 26 тестовых файлов, ~6300 строк; testify v1.12.1 (`require.Eventually`, `require.EventuallyWithT`, `assert.Never` есть)
- уже соответствует правилам: все хелперы с `*testing.T` начинаются с `t.Helper()`; подтесты названы по случаю; раскладка по файлам

### Проверки без testify
33 вызова `t.Fatal*`/`t.Error*`, `rn.t.Fatalf`, `rt.t.Fatalf`:
- `replicator/replicator_test.go` — 19: `TestDecodeAndModel`, `TestRawIsCopied`, `TestBatchOrder`, `TestDDLDescriptionParsing`, хелпер `mustMarshal`. `mustMarshal` вызывают ещё `lag_test.go` и `status_test.go`: меняется в Task 8, покрывает и их
- `replicator/harness_integration_test.go` — 5: `runner.waitExit`, `waitSameDBs`, `waitQuiet`
- `replicator/network_integration_test.go` — 3: `retrier.waitErr`, `retrier.waitSame`, таймаут `select` в `NetDstHangLease`
- `progress_test.go` — 3, таймауты `select`
- `replicator/lease_test.go`, `replicator/lease_integration_test.go` — по 1, таймаут `select`
- `log_test.go` — 1, `zerolog.ErrorHandler`

### `time.Sleep` — 20 вызовов по видам
- **опрос в цикле → `Eventually`:**
  - `srcNow` (`harness:185`);
  - `runner.waitSameDBs` (`harness:326`);
  - `retrier.waitErr`/`waitSame` (`network:167,183`);
  - захват аренды в `NetDstHangLease` (`network:406`)
- **«ничего не происходит за интервал» → `assert.Never` плюс финальная проверка после него:**
  - трафик не растёт при обрыве (`network:280`);
  - чужой файл не затёрт (`statefile_test:265`)
- **законные, остаются с комментарием «зачем сон»:**
  - темп нагрузки: писатели `lag_integration:80`, `network:224`;
  - длительность действия:
    - обрыв доходит до репликатора (`network:278`);
    - обрыв или зависание держится N с (`network:283,445,449`);
    - проверки по ходу обрыва в `NetDstDown` (`network:322,327`);
    - долгий DDL в `ddlHook` (`lease_integration:200`);
    - пауза повтора длиннее окна активности в `StateFileRetryPause` (`traffic_integration:136`). Проверки после сна доказывают, что писатель **всё ещё** обновляет файл через 2 с после возврата `Run`; `Eventually` прошёл бы на первом тике и ослабил тест;
  - окно устойчивости `waitQuiet` (`harness:337`);
  - серверное время аренды (`lease_integration:212`);
  - повтор до готовности API Toxiproxy в `populateProxies` (`harness:106`): вызывается из `TestMain`, `t` там нет

### Существующие `Eventually`
Надо проверить, что в условии нет `t`, и что есть сообщение:
- `lease_integration:149` (`readLease(t, …)` внутри условия — **ошибка**, см. Technical Details; без сообщения)
- `dml_integration:266`, `network:249` (`waitApplied`), `statefile_test:247` — без сообщения
- `main_test:57`, `drops_integration:161`, `lag_integration:51,95`, `traffic_integration:234`, `network:425,458`

### Таблицы с логикой в теле
Решение по каждой — в Technical Details → Таблицы.
- `TestDecodeAndModel`
- `TestPermanent`
- `TestCheckNS`
- `TestClientOptionsNoPrompt`
- `TestClientOptionsNoPasswordLeak`
- `TestStateDir`
- `TestWithPanelRestoresConsoleLog`
- `TestIntegrationRenameReserved`
- `TestStatusConcurrent`

### Команды
- `make test`, `make test-integration`, `make lint`
- одиночный тест — только через `docker compose run`, не локальный `go test`
- пакеты:
  - `./cmd/mongo-replicator/` — пакет `main`: `*_test.go` в `cmd/mongo-replicator`, включая `auth_integration_test.go`;
  - `./cmd/mongo-replicator/replicator/` — остальное

## Development Approach
- **testing approach**: Regular. Код не меняется, меняются тесты. Критерий — зелёные прогоны и падение каждого изменённого теста на своей мутации
- задачи по файлам: к каждому тесту файла сразу применяются все четыре правила, тест трогается и мутируется один раз
- complete each task fully before moving to the next
- **CRITICAL: all tests must pass before starting next task** - no exceptions
- **CRITICAL: update this plan file when scope changes during implementation**
- после каждой задачи: `make test` (юнит) или `make test-integration` (интеграционные), затем `make lint`
- **смысл теста не меняется**:
  - те же входы и те же проверяемые свойства;
  - сообщения проверок — с тем же смыслом, на русском, как сейчас;
  - усилить проверку можно, только если это тривиально и видно в diff; ослабить — нельзя
- имена существующих хелперов не меняются, кроме хелперов-проверок (см. Technical Details)
- **новые и переименованные интеграционные тесты**: имя базы `it_` + `snake(t.Name())` — не длиннее 63 символов (лимит MongoDB)
- код вне `_test.go` не меняется (см. «Страховка» ниже)

## Testing Strategy

### Что считается изменённым
- функция `Test…` или хелпер, у которых `git diff -w` показывает изменения
- правка только пустых строк и отступов (разбивка на блоки) мутации не требует

### Мутационная проверка (для каждой задачи)
- **охват**:
  - каждая изменённая функция `Test…` и каждый изменённый хелпер;
  - тест, который не менялся, но вызывает изменённый хелпер, покрывается мутацией этого хелпера;
  - при разбиении таблицы — мутация на каждый новый тест
- **порядок для одного теста**:
  1. Тест зелёный после переписывания.
  2. Внести в **код** (не в тест) минимальную поломку именно того свойства, которое проверяет изменённая проверка: инвертировать условие, пропустить запись, вернуть другую ошибку, сдвинуть время и т. п. Для хелпера ожидания — поломку, при которой ожидание не дождётся: репликатор не применяет один тип операции, `Run` возвращается раньше.
  3. Прогнать только этот тест. Он должен **упасть**, а сообщение — указывать на нарушенное свойство (для ожиданий — с последним диффом или состоянием).
  4. Откатить поломку (см. «Страховка»).
- **хелперы, чей путь падения недостижим из кода**: например, ошибка `bson.Marshal` в `mustMarshal`. Допустим временный негативный контроль в тесте, например вызов `mustMarshal(t, make(chan int))`. Его убирают вручную и отмечают в журнале как «контроль в тесте»
- **`TestLimitation*`** (`t.Skip`): временно закомментировать `t.Skip` и прогнать. Тест должен упасть так, как описано в README. Затем вернуть строку **вручную**
- **команды одиночного прогона** (`-race` требует cgo):
  - юнит: `docker compose run --rm --no-deps app sh -c 'CGO_ENABLED=1 go test -race -count=1 -run "^TestX$" ./cmd/mongo-replicator/<пакет>/'`
  - интеграционный (нужны сервисы, без `--no-deps`): `docker compose run --rm app sh -c 'CGO_ENABLED=1 go test -race -count=1 -tags integration -run "^TestIntegrationX$" ./cmd/mongo-replicator/<пакет>/'`
- **журнал мутаций** — в теле коммита задачи, по строке на тест: `TestX: <файл:строка> <что сломано> → FAIL: <начало сообщения>`
- тест, который не удаётся уронить разумной мутацией, отмечается ⚠️ в плане с описанием. Усиливать его — отдельное решение, не в этой задаче

### Страховка от утечки мутаций и потери работы
- `git checkout -- <файл>` — **только для файлов вне `_test.go`**. Правки тестов (в том числе временно снятый `t.Skip` или негативный контроль) откатываются вручную. Можно коммитить переписанные тесты до мутаций, но нельзя `git checkout`/`git restore` тестовых файлов или `.`
- охранная проверка перед началом задачи, после каждого отката и перед коммитом — должна молча завершиться с кодом 0:
  ```bash
  git diff --quiet -- . ':(exclude)*_test.go' ':(exclude)docs/plans' ':(exclude)CLAUDE.md'
  ```

### Набор тестов
- эталон снимается до начала:
  - юнит — `docker compose run --rm --no-deps app go test -list '.*' ./...`;
  - интеграционные — `docker compose run --rm app go test -tags integration -list '.*' ./...`. Здесь нужен полный стек: `TestMain` подключается к кластерам и пересоздаёт прокси. Проверять статус команды: упавший пакет даёт укороченный список
- после каждой задачи `diff` с эталоном показывает только запланированные изменения (разбитые таблицы); каждое изменение отмечено ➕ в плане
- `-list` показывает только тесты верхнего уровня: переименование подтестов в `diff` не видно, его проверяют по diff кода

## Progress Tracking
- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix
- update plan if implementation deviates from original scope
- keep plan in sync with actual work done

## Solution Overview
- порядок прохода по группам файлов:
  1. обвязка интеграционных тестов — от неё зависят все интеграционные;
  2. сеть, аренда, остальные области;
  3. юнит-тесты `replicator`;
  4. юнит-тесты `main`
- для каждого файла: проверки → testify, сны → по классификации, таблицы с ветвлением → отдельные тесты, тела → блоки «дано → действие → проверка»
- после прохода правила в `CLAUDE.md` распространяются на все тесты. По итогам уточняются: виды законных снов, условия `Eventually`, таймаут в `select`, какие хелперы называются `require…`/`assert…`

## Technical Details

### Проверки
- `if err != nil { t.Fatal(err) }` → `require.NoError(t, err)`
- `t.Fatalf("x=%v, want %v", got, want)` → `require.Equal(t, want, got, "контекст")` или `assert.Equal`: `require`, если дальше продолжать бессмысленно, иначе `assert`
- в методах `runner`/`retrier` — `rn.t`/`rt.t`, не внешний `t`
- таймаут в `select` (`case <-time.After(...): t.Fatal("...")`) → `require.FailNow(t, "...")`. Конструкция `select` остаётся, если ждётся канал
- `zerolog.ErrorHandler` в `log_test.go` → `assert.NoError(t, err, "запись лога")`. Обработчик вызывается в той горутине, которая пишет лог; в общем случае это может быть фоновая горутина, а `require` (`FailNow`) допустим только в горутине теста

### Условия `Eventually`/`Never`/`EventuallyWithT` (testify v1.12.1)
Как это работает в v1.12.1:
- каждое условие выполняется в **новой горутине**;
- по таймауту вызов возвращается, не дожидаясь её;
- `msgAndArgs` вычисляются при **входе** в вызов.

Правила:
- **в условии нет `t` и хелперов, принимающих `t`**: `require` с `t` в чужой горутине завершит только её. Тест висит до таймаута и падает с ложным «Condition never satisfied»
- внутри условия — либо обычный код с возвратом ошибки, либо `EventuallyWithT` с проверками на `c *assert.CollectT`: `require.NoError(c, err)`, `assert.Equal(c, …)`
- общие и глобальные значения (`lastSrcSec`) копируются в локальные переменные **до** вызова. Иначе зависшая горутина условия читает их во время записи следующим тестом, и `-race` сообщает о гонке в постороннем тесте
- **динамика — в проверках на `c`, не в `msgAndArgs`**: дифф, ошибки `Run` и т. п. пишутся в сообщение проверки внутри условия. В отчёт о падении копируются ошибки последнего тика
- **«прекратить ожидание и запомнить»**. Неудача на `CollectT` валит только один тик, и `EventuallyWithT` продолжает опрос до таймаута. Поэтому ранний выход `Run` или неожиданная ошибка записываются в переменные, условие возвращается успешно, а проверка идёт **после** ожидания:
  ```go
  var exited bool
  var exitErr error
  require.EventuallyWithT(rn.t, func(c *assert.CollectT) {
      if done, err := rn.exited(); done {
          exited, exitErr = true, err
          return
      }
      ok, diff := sameDBs(srcDB, dstDB)
      assert.True(c, ok, "база %s → %s не совпала: %s", srcDB, dstDB, diff)
  }, waitTimeout, pollEvery)
  require.False(rn.t, exited, "Run вернулся раньше времени: %v", exitErr)
  ```
  Так `waitSameDBs` по-прежнему падает **сразу**, как обещает его doc-комментарий. Так же устроен захват аренды в `NetDstHangLease`: неожиданная ошибка `acquireLease`, не `ErrLeaseHeld`, прекращает ожидание и проверяется после него. Момент захвата (`acquiredAt`) пишется в переменную внутри условия и читается только после возврата `Eventually`.
- `srcNow`: в условии вызывается не `srcOpTime(t)`, а функция без `t`, которая возвращает `(bson.Timestamp, error)`; ошибка — через `EventuallyWithT` и `require.NoError(c, err)`. `lastSrcSec` копируется до вызова, обновляется после
- **`assert.Never`** возвращается по таймеру без финальной проверки, поэтому после него остаётся финальная проверка исходного теста:
  - `network:280`: `assert.Never(t, func() bool { return nc.traffic.Snapshot().SrcIn != cut }, 3*time.Second, pollEvery, "…")`, затем `assert.Equal(t, cut, nc.traffic.Snapshot().SrcIn, …)`;
  - `statefile_test:265`: `Never` по владельцу файла, затем финальные `loadState` и проверка владельца
- у каждого `Eventually` есть сообщение

### Сны
| Вид | Что делать |
|---|---|
| опрос условия в цикле | `require.Eventually` / `EventuallyWithT` с сообщением, по правилам выше |
| «за интервал ничего не происходит» | `assert.Never` и финальная проверка после него |
| темп нагрузки (писатель вставляет раз в N мс) | остаётся, комментарий «темп нагрузки» |
| длительность действия (обрыв доходит до репликатора; обрыв или зависание держится N с; долгий DDL; пауза повтора длиннее окна активности) | остаётся, комментарий: что длится и почему столько |
| окно устойчивости (`waitQuiet`: значение не менялось весь интервал) | остаётся, комментарий |
| серверное время (истечение аренды, `$$NOW`) | остаётся, комментарий |
| вне теста (`populateProxies` из `TestMain`) | остаётся |

### Таблицы
| Тест | Решение |
|---|---|
| `TestDecodeAndModel` | отдельные тесты по виду результата:<br>• вставка → replace-upsert;<br>• удаление, в том числе с составным `documentKey`, → delete по `_id`;<br>• update без документа (`fullDocument: null` или нет поля) → пропуск.<br>Общая сборка события — хелпер, ожидаемые модели — литералы |
| `TestPermanent` | таблица перманентных ошибок с подсказкой и отдельная таблица неперманентных (`permanent` = false, подсказки нет) |
| `TestCheckNS` | таблица допустимых событий (`NoError`) и таблица чужих (`ErrorIs(ErrForeignNamespace)` и подстрока) |
| `TestClientOptionsNoPrompt` | без пользователя (`opts.Auth == nil`) — отдельный тест; с пользователем — таблица (`user`, `password`, `warn`) |
| `TestClientOptionsNoPasswordLeak` | `if err != nil` молча пропускает проверки → две таблицы:<br>• случаи с ошибкой: `require.Error`, пароля нет в тексте ошибки;<br>• случаи без ошибки: `require.NoError`.<br>В обеих — пароля нет в приглашениях. Ожидаемый исход каждого случая фиксируется по текущему поведению |
| `TestStateDir` | «`XDG_STATE_HOME` не задан» (`Unsetenv`) — отдельный тест, остальное — таблица |
| `TestWithPanelRestoresConsoleLog` | остаётся: случаи отличаются только завершающим действием (`stop`/`kill`), проверки одинаковые — это данные |
| `TestIntegrationRenameReserved` | `if d := …LastDDL; d != nil { assert.NotEqual(…) }` → безусловная проверка `assert.True(t, d == nil \|\| d.Op != "rename", "…")` |
| `TestStatusConcurrent` | остаётся: ветвление в горутине — суть проверки гонки |
| прочие таблицы | проверяются при проходе: поле-флаг, меняющее ход теста, или поле-функция `setup`/`check` → отдельные тесты; поля `want bool`, `tty bool` и т. п. — данные. Новое решение отмечается ➕ |

### Хелперы-проверки
- имена хелперов подготовки, чтения и ожидания (`prepare`, `srcNow`, `readLease`, `writeCkpt`, `cleanDB`, `waitSame`, …) не меняются
- хелпер, чья единственная задача — проверка (ничего не возвращает и не ждёт), получает префикс `assert…` или `require…` — по тому, чем он валит тест
- известный случай: `ckptBehind` → `assertCkptBehind`; остальные найти при проходе и отметить ➕

### Раздел «Тесты» в `CLAUDE.md` после прохода
- убрать оговорку «правила для новых… существующие не переписываются»: правила действуют для всех тестов
- **время**:
  - опрос → `Eventually`;
  - «ничего не происходит» → `assert.Never` и финальная проверка;
  - законные сны (темп нагрузки, длительность действия, окно устойчивости, серверное время) — всегда с комментарием
- **условия `Eventually`/`Never`**:
  - без `t` и хелперов с `t`, проверки — на `CollectT`;
  - общие значения копируются до вызова;
  - динамика — в проверках на `c`;
  - ранний выход и неожиданная ошибка — «прекратить ожидание и запомнить», проверка после
- **проверки**: таймаут в `select` — `require.FailNow`; в колбэке, который может вызваться не из горутины теста, — только `assert`
- **хелперы**: префикс `require…`/`assert…` — для хелперов, чья задача — проверка; хелперы подготовки, чтения и ожидания называются по действию
- **имена интеграционных тестов**: `it_<snake>` ≤ 63 символов
- README и разделы «Интеграционные тесты»/«Сетевые тесты» не ссылаются на переименованные хелперы

## What Goes Where
- **Implementation Steps**: переписывание тестов, мутационная проверка, `CLAUDE.md`
- **Post-Completion**: нет внешних действий

## Implementation Steps

### Task 1: Эталон

**Files:**
- нет изменений в репозитории (эталон — в `/tmp/apply-test-rules/`)

- [x] снять списки юнит- и интеграционных тестов (команды из Testing Strategy, с проверкой статуса), отсортировать
- [x] `make test`, `make test-integration`, `make lint` — исходное состояние зелёное
- [x] пробная мутация: `permanent` всегда возвращает `false` → `TestPermanent` падает. Откатить через `git checkout -- cmd/mongo-replicator/main.go`, охранная проверка проходит

### Task 2: Обвязка интеграционных тестов

**Files:**
- Modify: `cmd/mongo-replicator/replicator/harness_integration_test.go`

- [x] `runner.waitSameDBs` → `require.EventuallyWithT` по образцу из Technical Details: дифф — в проверке на `c`, ранний выход `Run` — «прекратить ожидание и запомнить»
- [x] `runner.waitExit`, `waitQuiet`: `rn.t.Fatalf` → testify. В `waitQuiet` сон окна устойчивости остаётся, с комментарием
- [x] `srcNow` → `EventuallyWithT` без `t` в условии, `lastSrcSec` копируется до вызова; `populateProxies` не трогать
  - ➕ запрос no-op вынесен в `appendSrcNote(name) (bson.Timestamp, error)`, `srcOpTime(t)` — обёртка над ним. `lastSrcSec` по-прежнему пишет только `dropDB`: `srcNow` его не обновлял и не обновляет
- [x] остальные хелперы и `TestSnake`/`TestIntegrationStopOnCleanup` — структура и проверки по правилам
  - ➕ `TestSnake` → таблица с подтестами по случаю (случаи отличаются только данными); верхний уровень `-list` не изменился
- [x] мутации:
  - репликатор не применяет `update` → `waitSame` падает с диффом;
  - `Run` сразу возвращает ошибку → немедленное падение «Run вернулся раньше времени» (не через 30 с);
  - чекпойнт не сохраняется → `waitQuiet`/`ckptToken` падают;
  - плюс по мутации на каждый изменённый тест файла
- [x] `make test-integration`, `make lint`, охранная проверка; коммит с журналом мутаций

### Task 3: Сетевые тесты

**Files:**
- Modify: `cmd/mongo-replicator/replicator/network_integration_test.go`
- Modify: `cmd/mongo-replicator/replicator/harness_integration_test.go` (➕ `findLease`)

- [x] `retrier.waitErr`/`waitSame` → `Eventually`/`EventuallyWithT`; дифф и `rt.runErrs()` — в проверке на `c`
- [x] захват аренды в `NetDstHangLease` → `Eventually`: неожиданная ошибка и `acquiredAt` записываются в переменные, проверка после ожидания. Таймаут `select` → `require.FailNow`
  - ➕ чтение аренды без `t` для условия — `findLease(dstDB) (leaseState, bool, error)` в обвязке, `readLease(t)` — обёртка над ним (пригодится и в Task 4, `TestIntegrationLeaseLost`)
- [x] сны `NetSrcDown`:
  - `network:278` — законный, с комментарием «обрыв доходит до репликатора»;
  - `network:280` → `assert.Never(…, 3*time.Second, pollEvery, …)` и финальный `assert.Equal`;
  - остальные сны — по таблице видов, с комментариями
- [x] `waitApplied` — сообщение в `Eventually`; `ckptBehind` → `assertCkptBehind`; структура тестов «дано → действие → проверка»
- [x] мутации на каждый изменённый тест и хелпер, например:
  - чекпойнт сохраняется до записи данных → `assertCkptBehind` падает;
    - ➕ взята мутация «`toModel` пропускает документ `_id` 7»: чекпойнт проходит дальше недописанного документа детерминированно. Перестановка `saveToken`/`flush` ловилась бы только в окне в несколько мс, а при отключённой цели не пишется ни то, ни другое
  - `Snapshot()` делает `SrcIn.Add(1)` при каждом вызове → `assert.Never` в `NetSrcDown` падает
- [x] `make test-integration`, `make lint`, охранная проверка; коммит с журналом мутаций

### Task 4: Аренда

**Files:**
- Modify: `cmd/mongo-replicator/replicator/lease_test.go`
- Modify: `cmd/mongo-replicator/replicator/lease_integration_test.go`

- [x] таймауты `select` → `require.FailNow`
- [x] комментарии к снам: `lease_integration:200` — длительность долгого DDL, `:212` — серверное время аренды
- [x] ➕ `TestIntegrationLeaseLost` (`:149`): `readLease(t, …)` внутри `Eventually` → условие без `t` (`EventuallyWithT` с чтением чекпойнта и `require.NoError(c, err)`), плюс сообщение
  - ➕ условие — `findLease` из Task 3, владелец копируется в локальную переменную до вызова
- [x] таблицы и структура тестов по правилам
- [x] мутации на каждый изменённый тест, например: продление не останавливается при 2/3 TTL; `checkLease` не проверяет срок
- [x] `make test`, `make test-integration`, `make lint`, охранная проверка; коммит с журналом мутаций

### Task 5: Интеграционные DML, DDL, удаления

**Files:**
- Modify: `cmd/mongo-replicator/replicator/{dml,ddl,drops}_integration_test.go`

- [x] `dml_integration:266` — сообщение в `Eventually`; `drops_integration:161` — проверить условие на `t`
  - ➕ условие `drops_integration:161` без `t` (только `rn.r.Status()`), добавлено сообщение
- [x] структура «дано → действие → проверка» и проверки по правилам во всех тестах файлов
  - ➕ в `TestIntegrationDML`/`TestIntegrationTransaction` `waitQuiet` перенесён к `waitSame` (блок действия), проверки — после него
- [x] мутации на каждый изменённый тест
  - ➕ «код 48 не игнорируется» `TestIntegrationReplayIdempotent` не ловит: `create` существующей коллекции с теми же опциями на 8.0 проходит без ошибки, а ошибки 26/27 при повторе идут после DML и роняют тест в `waitSame`, не в `Eventually`. Для `Eventually` взята мутация счётчика `Applied`
- [x] `make test-integration`, `make lint`, охранная проверка; коммит с журналом мутаций

### Task 6: Интеграционные базы, чекпойнт, статус

**Files:**
- Modify: `cmd/mongo-replicator/replicator/{dbs,checkpoint,status}_integration_test.go`

- [x] `TestIntegrationRenameReserved`: условная проверка `LastDDL` → безусловная (Technical Details → Таблицы)
- [x] структура и проверки по правилам во всех тестах файлов
  - ➕ проверки в файлах уже на testify; правки — блоки пустыми строками, в `TestIntegrationSaveTokenUnparsedTime` комментарий о `Run` перенесён к вызову `Run`
- [x] мутации на каждый изменённый тест
- [x] `make test-integration`, `make lint`, охранная проверка; коммит с журналом мутаций

### Task 7: Интеграционные лаг, трафик, ограничения

**Files:**
- Modify: `cmd/mongo-replicator/replicator/{lag,traffic,limitations}_integration_test.go`

- [x] сны с комментариями:
  - `StateFileRetryPause` (`traffic_integration:136`) — пауза повтора длиннее окна активности, сон **остаётся**;
  - писатель в `SteadyTrafficBoundedLag` — темп нагрузки
  - ➕ писатель `SteadyTrafficBoundedLag` не заменён на `startWriter`: тесту нужен темп 200 мс (реже пачки, чаще `maxAwaitTime`), у `startWriter` — 20 мс
- [x] `Eventually` в `lag_integration:51,95`, `traffic_integration:234` — проверить условие на `t` и сообщение
  - ➕ во всех трёх условиях `t` нет, сообщения есть — без правок
- [x] `TestLimitation*` остаются с `t.Skip`, тела приводятся к правилам. Проверка: временно снять `t.Skip` — тест падает, как описано в README; вернуть строку вручную
  - ➕ тела — только пустые строки; без `t.Skip` оба падают на итоговой проверке `same` («документы различаются» в `b`)
- [x] мутации на каждый изменённый тест
- [x] `make test-integration`, `make lint`, охранная проверка; коммит с журналом мутаций

### Task 8: Юнит-тесты ядра — `replicator_test.go`

**Files:**
- Modify: `cmd/mongo-replicator/replicator/replicator_test.go`

- [x] `TestDecodeAndModel` → отдельные тесты (Technical Details → Таблицы)
  - ➕ `TestToModelInsert`, `TestToModelDelete` (таблица: `documentKey` из `_id` и составной — случай с простым ключом добавлен), `TestToModelUpdateWithoutDocument` (таблица: `fullDocument: null`, нет поля); сборка события — `changeEvent`, декодирование и проверка resume token — `decodeEvent`
- [x] `mustMarshal` и остальные `t.Fatal*` → `require`. `mustMarshal` вызывают `lag_test.go` и `status_test.go`, проверить, что они проходят
- [x] `TestCheckNS` → две таблицы; `TestRawIsCopied`, `TestBatchOrder`, `TestDDLDescriptionParsing` — проверки testify и структура
  - ➕ `TestCheckNS` → `TestCheckNSAllowed` и `TestCheckNSForeign`
  - ➕ `TestRawIsCopied` сравнивает каждое поле с литералом (было — со снимком до порчи буфера); `TestBatchOrder` — модели с разными фильтрами, чтобы `assert.Equal` различал их, и проверка `ops` целиком
- [x] остальные тесты файла — структура и таблицы по правилам
  - ➕ хвост `TestApplyDDLRenameReserved` (обычный rename) → `TestApplyDDLRenameChecksLease`; проверка совпадающей пары из `TestCheckDBs` → `TestCheckDBsSame`, ожидания `TestCheckDBs` — литералы вместо `Sprintf`; в `TestLogProgressBeforeFirstSave` `defer` → `t.Cleanup`
- [x] мутации на каждый изменённый тест, например:
  - delete-модель с полным `documentKey` вместо `_id`;
  - replace без `upsert`;
  - `bson.Raw` без копирования;
  - нарушен порядок пачки.

  Для `mustMarshal` — контроль в тесте (`make(chan int)`)
  - ➕ «`bson.Raw` без копирования» — мутация в вендоренном драйвере (`value_reader.go`, `readValueBytes` возвращает срез буфера), делается и откатывается внутри контейнера: `vendor/` принадлежит root. `UnmarshalBSON` у `event` для мутации не годится: драйвер передаёт ему копию
- [x] `make test`, `make lint`, охранная проверка; коммит с журналом мутаций

### Task 9: Прочие юнит-тесты пакета `replicator`

**Files:**
- Modify: `cmd/mongo-replicator/replicator/{statefile,status,traffic,lag}_test.go`

- [ ] `statefile_test.go:265`: сон → `assert.Never` «чужой файл не затёрт» и финальная проверка владельца; `statefile_test.go:247` — сообщение в `Eventually`
- [ ] `TestStatusConcurrent` остаётся (Technical Details → Таблицы); таблицы, структура и проверки во всех тестах файлов
- [ ] мутации на каждый изменённый тест
- [ ] `make test`, `make lint`, охранная проверка; коммит с журналом мутаций

### Task 10: Юнит-тесты `main`: ошибки, панель, лог

**Files:**
- Modify: `cmd/mongo-replicator/{errors,progress,log,main}_test.go`

- [ ] `TestPermanent` → две таблицы
- [ ] `progress_test.go`: таймауты `select` → `require.FailNow`; `log_test.go`: `ErrorHandler` → `assert.NoError`
- [ ] `TestWithPanelRestoresConsoleLog` остаётся таблицей; `main_test:57` — проверить условие `Eventually` на `t`
- [ ] таблицы, структура и проверки во всех тестах файлов
- [ ] мутации на каждый изменённый тест
- [ ] `make test`, `make lint`, охранная проверка; коммит с журналом мутаций

### Task 11: Юнит-тесты `main`: опции и маскирование

**Files:**
- Modify: `cmd/mongo-replicator/{options,mask}_test.go`

- [ ] `TestStateDir`: случай «`XDG_STATE_HOME` не задан» — отдельный тест
- [ ] таблицы, структура и проверки во всех тестах файлов
- [ ] мутации на каждый изменённый тест
- [ ] `make test`, `make lint`, охранная проверка; коммит с журналом мутаций

### Task 12: Юнит-тесты `main`: учётные данные и авторизация

**Files:**
- Modify: `cmd/mongo-replicator/credentials_test.go`
- Modify: `cmd/mongo-replicator/auth_integration_test.go`

- [ ] `TestClientOptionsNoPrompt` → тест без пользователя и таблица с пользователем
- [ ] `TestClientOptionsNoPasswordLeak` → таблицы «с ошибкой» и «без ошибки» (исход фиксируется по текущему поведению)
- [ ] таблицы, структура и проверки во всех тестах файлов
- [ ] мутации на каждый изменённый тест, например: пароль попадает в текст ошибки разбора URI; prompter вызывается при пароле в URI
- [ ] `make test`, `make test-integration` (для `auth_integration_test.go`, пакет `./cmd/mongo-replicator/`), `make lint`, охранная проверка; коммит с журналом мутаций

### Task 13: Правила в `CLAUDE.md`

**Files:**
- Modify: `CLAUDE.md`

- [ ] раздел «Тесты» — по Technical Details: правила для всех тестов, виды снов, условия `Eventually`/`Never`, таймаут `select`, колбэки, префиксы хелперов-проверок, длина имени интеграционного теста
- [ ] `grep` по `CLAUDE.md` и `README.md` — нет ссылок на переименованные хелперы
- [ ] коммит

### Task 14: Verify acceptance criteria
- [ ] в тестах нет `t.Fatal*`/`t.Error*`, кроме обоснованных и отмеченных в плане: `grep -nE '(\bt|\.t)\.(Fatal|Fatalf|Error|Errorf)\(' -r cmd --include='*_test.go'`
- [ ] каждый оставшийся `time.Sleep` законного вида и с комментарием: `grep -n 'time\.Sleep' -r cmd --include='*_test.go'`
- [ ] в условиях `Eventually`/`Never` нет `t` (просмотр каждого вызова из `grep -nE 'Eventually|Never\(' -r cmd --include='*_test.go'`), у каждого есть сообщение
- [ ] список тестов отличается от эталона только запланированными изменениями
- [ ] журналы мутаций есть в коммитах Task 2–12 для каждого изменённого теста; ⚠️-тесты перечислены в плане
- [ ] `git diff --stat` от начала работ — только `*_test.go`, `CLAUDE.md` и этот план
- [ ] `make test`, `make test-integration`, `make lint`

### Task 15: [Final] Update documentation
- [ ] README.md — только если ссылается на переименованные хелперы или тесты
- [ ] move this plan to `docs/plans/completed/`

## Post-Completion
*Items requiring manual intervention or external systems - no checkboxes, informational only*

- внешних действий нет
- ⚠️-тесты, которые не удалось уронить мутацией, — кандидаты на отдельную задачу по усилению
