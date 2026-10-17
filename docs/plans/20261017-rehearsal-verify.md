# Репетиция перелива и подкоманда verify

## Overview
- Репетиция перед настоящей переливкой: репликатор на машине будущей переливки читает **боевой** источник с живыми записями и пишет в **пустую** mongo 4.4 в Docker, у которой та же схема, что у источника (на машине переливки mongo 5.0+ не запускается; репликатор с целью 4.4 там уже работал). Дампа в репетиции нет: в настоящей переливке он уже будет восстановлен на цели, а репетиция проверяет сам репликатор.
- Готовность определяют два критерия: **данные совпадают** и **процесс устойчив**. Для данных — новая подкоманда `mongo-replicator verify`. Она точно сверяет документы, изменённые после `--since`, при живых записях и с небольшой нагрузкой на источник. Для устойчивости — сценарии сбоев в runbook с ожидаемым поведением.
- Результат:
  - подкоманда `verify` (ядро — `internal/verify`);
  - стенд `deploy/rehearsal/`: compose с авторизацией и скрипт копии схемы `copy-schema.js`;
  - runbook `docs/runbook-rehearsal.md` с журналом и критериями «готовы / не готовы».
- `verify` нужна и в настоящей переливке: перед переключением приложения она отвечает, можно ли переключаться. Поэтому это подкоманда того же бинарника, а не отдельный инструмент.

## Context (from discovery)
- CLI: `cmd/mongo-replicator/options.go` — одна структура `options` (go-flags), `parseOpts` (остаток позиционных аргументов сейчас отбрасывается); `main.go` — `run`, `replicate` (повтор временных ошибок, `permanent` = есть `hint`), `withPanel(opts options, …)`, `connect`, `failCode`; `log.go` — `setupLog(opts options)`; `credentials.go` — `prepareClients` (пароль с терминала), `flagGiven(os.Args[1:], …)`; `mask.go`, `errors.go`.
- Чекпойнт: `internal/replicator/checkpoint.go` — `checkpoint{Token, DB, DstDB, ClusterTime}`, `appliedAt()` (время токена через `tokenTime` из `lag.go`, иначе `ClusterTime`), чтение только с primary (`ckpt()`). Поток (`stream()`) может сохранить токен посреди группы событий с одним `clusterTime` (транзакция, пакетные удаления): позиция «= T» не значит, что применены все события с временем T.
- Запись на цель: `toModel` (`event.go`) пишет `fullDocument` из `updateLookup` как replace — на цели может оказаться состояние источника **новее** события, которое его принесло.
- Состояние на машине: `internal/statefile` — `Path(dir, dstDB, dstURI)` (имя `<dst-db>.<8 hex sha256(clusterKey)>.json`), `MoveAside`, `writeTemp`. Файл `verify` с тем же именем в корне `--state-dir` совпал бы с файлом состояния репликатора: тот загрузил бы и перезаписал его, а `Summary` посчитал бы его процессом. Поэтому позиция `verify` лежит в подкаталоге `verify/` (`Summary` пропускает каталоги).
- Аренда: `ErrLeaseHeld` перманентна (у неё есть подсказка `другой процесс реплицирует эту базу; если он упал — аренда истечёт через ≤60 с`). Запуск сразу после `kill -9` заканчивается кодом `1` с этой подсказкой, запуск через ≥60 с работает.
- DDL на цели: `create` уже существующей коллекции терпится (код 48, `ddl.go`); `drop` останавливает репликатор без `--apply-drop`; уникальные вторичные индексы могут дать E11000 и бесконечный повтор (README «Уникальные индексы»).
- Стенд разработки: `docker-compose.yml` — `mongo-src` (rs8, член `mongo-src:27017`, порты не опубликованы), `mongo-dst` (rsdst) без авторизации, `toxiproxy`, `app`. Интеграционные тесты удаляют базы `it_*`. Обвязка интеграционных тестов (`newIT`, `runner`) — неэкспортированная, в пакете `replicator`.
- Цель 4.4: драйвер `go.mongodb.org/mongo-driver/v2` v2.9.2 поддерживает серверы от 4.4 (`MinSupportedMongoDBVersion`). Решение: тестов на 4.4 не проводим и считаем, что возможности 8.0, которых нет в 4.4 (clustered, time-series, `changeStreamPreAndPostImages` и т. п.), не используются — ни источником, ни репликатором, ни `verify`. Интеграционные тесты остаются на 8.0. В образе `mongo:4.4` нет `mongosh` (только старый `mongo`).
- Зависимости пакетов: `traffic ← statefile ← replicator ← panel ← main`. Новый пакет `verify` зависит от `replicator` и `statefile`, от него — только `main`.

## Development Approach
- **testing approach**: Regular (code first, then tests)
- complete each task fully before moving to the next
- make small, focused changes
- **CRITICAL: every task MUST include new/updated tests** for code changes in that task
- **CRITICAL: all tests must pass before starting next task** — no exceptions
- **CRITICAL: update this plan file when scope changes during implementation**
- тесты только через `make test`, `make test-integration`, `make lint`, `docker compose run` (не локальный `go test`)
- правила тестов — из `CLAUDE.md` (раскладка, «дано → действие → проверка», без логики в теле теста, testify, `Eventually` без `t` в условии, имена `TestIntegration…`, база `it_` + `snake(t.Name())` ≤ 63 байт)
- тесты не открывают `/dev/tty` и не пишут в псевдотерминал байт 0x1a
- обратная совместимость CLI не нужна: инструмент ещё нигде не используется
- облачных провайдеров не называть ни в коде, ни в документах

## Testing Strategy
- **юнит** (`make test`): разбор опций обеих форм, вердикт, выборка по хэшу `_id`, дифф, очередь (дедупликация, самое раннее время, повторная постановка, rename, drop, предел), ограничитель скорости с подставным временем, файл позиции и блокировка, нормализация схемы.
- **интеграционные** (`make test-integration`, compose `mongo-src`/`mongo-dst`): в большинстве тестов чекпойнт пишется вручную — тест детерминирован без репликатора. Один сквозной тест запускает настоящий репликатор при живых записях через свой маленький runner в пакете `verify` (исключение из правила `newIT` — фиксируется в CLAUDE.md).
- **цель 4.4**: тестов на 4.4 нет (решение пользователя); стенд репетиции на `mongo:4.4` только поднимается и проверяется вручную (healthy, вход root).
- **копия схемы**: `make test-copy-schema` — фикстура коллекций с опциями на `mongo-src`, затем `copy-schema.js` (mongosh из одноразового контейнера `mongo:8.0`) на `mongo-dst`, затем `mongo-replicator verify --schema-only` с кодом `0`.
- e2e UI-тестов в проекте нет.

## Progress Tracking
- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix
- update plan if implementation deviates from original scope

## Solution Overview
- **Отдельный процесс, ничего не пишет на цель.** `verify` читает чекпойнт репликатора, аренду не берёт, в базу цели не пишет. Её остановка (Ctrl+C, `kill`) для репликатора незаметна: это «стоп-кран» нагрузки.
- **Что сверяется.** Только документы, изменённые после `--since`: за них отвечает репликатор, а состояние до `--since` — забота дампа. Список изменённых `verify` берёт из **своего** change stream без `updateLookup`. Схема (коллекции, опции, индексы, views) сверяется отдельно, через метаданные. Число документов не сравнивается.
- **Вердикт при живых записях.** Для ключа t₀ — самое раннее непроверенное время события. Ключ готов, когда позиция чекпойнта **строго больше** t₀. Затем:
  1. S1 — документ источника, T1 — `operationTime` чтения;
  2. ожидание, пока позиция чекпойнта станет строго больше T1;
  3. D — документ цели;
  4. S2 — снова документ источника, T2 — `operationTime` чтения;
  5. ожидание, пока **свой поток** `verify` пройдёт T2.

  Итог:
  - `D == S1` или `D == S2` → **совпал**;
  - `S1 == S2`, `D` другой, и у ключа **нет событий в (T1, T2]** → **дефект**: источник весь интервал не менялся, а цель расходится. Из-за `updateLookup` цель может нести состояние из будущего, но не позже момента чтения D, то есть не позже T2;
  - иначе документ горячий → повтор с новым t₀, после 5 попыток → **не проверен**.

  «Отсутствует» — обычное значение: так ловятся лишние документы на цели и недошедшие удаления. Если событие по ключу пришло, пока ключ проверялся, после вердикта он снова ставится в очередь.
- **Чтения источника — только primary, read concern majority.** Secondary у источника может не быть. Чтение с отстающей secondary дало бы ложные дефекты. Read preference из URI для чтений `verify` переопределяется.
- **Нагрузка.**
  - На источник идут второй change stream и чтения по `_id` пачками `$in`. Второй поток повторяет просмотр oplog, но без `updateLookup`.
  - `--rate` ограничивает чтения документов в секунду, `--sample` проверяет детерминированную долю ключей по хэшу `_id`.
  - Пачки идут конвейером: S1 у следующих пачек выполняется, пока предыдущие ждут чекпойнт.
  - Если очередь ключей доходит до предела, чтение потока приостанавливается. Если поток `verify` выпадет из окна oplog (`ChangeStreamHistoryLost`), это перманентная ошибка с подсказкой.
- **Продолжение.** Позиция потока `verify`, непроверенные ключи и счётчики хранятся в своём файле под блокировкой `flock`. Повторный запуск продолжает с него, второй одновременный запуск получает отказ.
- **Форма CLI.** Если первый аргумент — `verify`, разбирается форма сверки, иначе — запуск репликатора, как сейчас. Общие опции — встроенная структура `commonOptions` в обеих формах. Механизм команд go-flags не используется: он пропускал бы опции родителя после имени команды. Позиционные аргументы в форме запуска — ошибка с подсказкой «`verify` должна быть первым аргументом».
- **Стенд.** Цель — `mongo:4.4` в Docker с авторизацией: root для подготовки, пользователь репликатора с `readWrite` только на `--dst-db`. Так репетиция подтверждает, что минимальных прав хватает. Схема копируется скриптом mongosh, данные не читаются, а результат проверяет `verify --schema-only`.

## Technical Details

### CLI
```
mongo-replicator [опции репликации]              # как сейчас
mongo-replicator verify [опции сверки]
```
- `commonOptions`: `--src`/`SRC_URI`, `--dst`/`DST_URI`, `--db`/`DB`, `--dst-db`/`DST_DB`, `--log-level`, `--log-file`, `--log-file-level`, `--state-dir`. Проверки те же: имена баз, служебные базы, `--log-file-level` без `--log-file`.
- `runOptions` = `commonOptions` + `--start-ts`, `--batch`, `--apply-drop`, `--progress`, `--no-clear`.
- `verifyOptions` = `commonOptions` + свои опции:

| опция | env | по умолчанию | |
|---|---|---|---|
| `--since` | `VERIFY_SINCE` | — | unix-сек, начало сверки; обязательна, если файла позиции нет; при найденном файле игнорируется с WARN |
| `--rate` | `VERIFY_RATE` | `200` | максимум чтений документов в секунду с источника, `>= 1` |
| `--sample` | `VERIFY_SAMPLE` | `1` | доля ключей `(0, 1]`, выбор по хэшу `_id`; когда `--rate` не успевает за потоком изменений |
| `--until-caught-up` | `VERIFY_UNTIL_CAUGHT_UP` | — | закончить, когда проверены все ключи с временем события < позиции чекпойнта на момент, когда поток `verify` её достиг |
| `--ids` | — | — | файл `unverified.json`/`defects.json`: перепроверить эти ключи и выйти (без потока, без файла позиции) |
| `--schema-only` | — | — | сверить только схему и выйти |

- Взаимоисключения (иначе код `2`): `--ids` и `--schema-only` между собой; с ними — `--since`, `--until-caught-up`, `--sample`.
- Клиент источника `verify` получает `appName` `mongo-replicator-verify`: в `currentOp` и логах сервера сверку видно отдельно от репликатора.
- Коды выхода: `0` — дефектов нет («не проверено» допускается, оно видно в отчёте); `1` — есть дефекты или ошибка выполнения; `2` — ошибка опций или занятый файл позиции.

### Позиция и отчёты
- Каталог: `<state-dir>/verify/`, создаётся при первом запуске. Имя: `<dst-db>.<8 hex sha256(clusterKey цели)>.json` — через экспортированный хелпер `statefile`.
- Блокировка: `flock(LOCK_EX|LOCK_NB)` на файле `<имя>.lock` на всё время работы. Занят → код `2` с подсказкой «сверка этой базы уже запущена». Вызов — в `lock_unix.go`, в `lock_other.go` — без блокировки, по правилу CLAUDE.md для `x/sys/unix`.
- Файл позиции (JSON, атомарная запись через экспортированный из `statefile` хелпер, раз в 10 с и при выходе):
  ```json
  {"db": "...", "dst_db": "...", "cluster": "...", "src_cluster": "...", "since": 1791900000,
   "token": {...}, "stream_time": {"t": ..., "i": ...},
   "pending": [{"ns": "db.coll", "id": <extJSON canonical>, "first": {...}, "last": {...}, "tries": 0}],
   "counts": {"checked": 0, "matched": 0, "defects": 0, "unverified": 0, "sampled_out": 0}}
  ```
  `src_cluster` — ключ кластера источника: токен потока принадлежит источнику. Чужие `db`/`dst_db`/`cluster`/`src_cluster`, битый файл → `statefile.MoveAside` (`.mismatch-*`/`.corrupt-*`) и старт с `--since` (без него — код `2` с подсказкой).
- `<имя>.defects.json` и `<имя>.unverified.json` рядом с позицией: массив `{"ns", "id", "reason", "src", "dst"}`. В `src`/`dst` — расширенный JSON документа, обрезанный до 4 КиБ. Записываются при выходе. Не больше 10 000 записей каждый, счётчик — полный.

### Очередь ключей
- Ключ — `(ns, raw _id)`. Для ключа хранятся `first` (самое раннее непроверенное время события), `last` (самое позднее) и число попыток. Готовность определяется по `first`.
- Пока ключ проверяется, его новые события копятся отдельно. После вердикта, если пришло событие новее T1, ключ возвращается в очередь с `first` = время этого события.
- Предел — 100 000 ключей. При достижении поток не читается, пока очередь не уменьшится.
- `rename a → b`: ключи `a` переносятся под `b`. Документы после rename живут в `b` на обеих сторонах. Если `updateLookup` их потерял (ограничение (a)), это честный дефект.
- `drop` коллекции: её ключи уходят в «не проверено» с причиной `drop`. `dropDatabase`/`invalidate` — поток окончен, итоговый отчёт.
- `create` в потоке: имя коллекции запоминается. Расхождение опций у неё в сверке схемы — «известное ограничение» (опции `create` не переносятся), а не дефект.

### Сверка документов
- Готовые ключи группируются по коллекции, пачками до `min(500, rate)` `_id`. Тогда пачка всегда помещается в ёмкость ограничителя.
- S1: `find({_id: {$in: ids}})` на источнике, primary, read concern majority, T1 = `operationTime` ответа (сессия без causal consistency).
- Ожидание: позиция чекпойнта > T1. Опрос раз в 1 с, общий для всех ждущих пачек.
- D: тот же `find` на цели, primary, read concern majority.
- S2: повтор S1, T2 = `operationTime`. Затем ожидание, пока поток `verify` пройдёт T2. Noop-записи сервера двигают поток примерно раз в 10 с, поэтому ожидание кончается и на тихом источнике.
- Результаты сопоставляются с ключами по **точному** `raw _id`: при collation коллекции не `simple` `$in` по строковому `_id` может вернуть несколько документов.
- Конвейер: до 8 пачек одновременно. Пока одни ждут чекпойнт, ограничитель выдаёт маркеры S1 следующим.
- Сравнение — побайтное равенство `bson.Raw`: репликатор пишет `fullDocument` как replace, поэтому порядок полей сохраняется. Дифф для лога — имена верхнеуровневых полей, которые различаются, отсутствуют или лишние (не больше 10).
- `--rate`: ограничитель «маркерное ведро» с ёмкостью `rate` и подставляемыми часами. Каждый запрошенный документ источника — один маркер: S1 и S2 считаются оба.
- `--sample p`: ключ берётся, если первые 8 байт `sha256(raw _id)` как uint64 < p·2⁶⁴. Решение детерминировано, поэтому повтор после рестарта выбирает те же ключи. Отброшенные считаются в `sampled_out`.

### Позиция чекпойнта
- Экспорт из `internal/replicator`: `AppliedPosition(ctx, dst *mongo.Client, srcDB, dstDB string) (ts bson.Timestamp, ok bool, err error)`. Он читает `<dst-db>._replicator_checkpoint` с primary и возвращает `appliedAt()`. `ok=false` — документа нет или позиция пустая. Чекпойнт другой пары баз → ошибка через `checkDBs`.
- Doc-комментарий: применено всё **строго до** `ts`. События с временем, равным `ts`, могут быть применены не все: токен сохраняется и посреди группы с одним `clusterTime`.

### Сверка схемы
- Источник и цель: `listCollections` (без `system.*` и `_replicator_checkpoint`), `listIndexes` каждой коллекции.
- Нормализация:
  - у коллекции убрать `info.uuid` и `info.readOnly`, у индекса — `v` и `ns`;
  - верхнеуровневые поля опций и спецификаций индексов сравнивать без учёта порядка, коллекции и индексы — отсортированными по имени.
- Тип (`collection`/`view`) и опции сравниваются как документы целиком (capped/size/max, validator/validationLevel/validationAction, collation, viewOn/pipeline и всё прочее, что вернул сервер). Отдельной логики для возможностей 8.0, которых нет в 4.4, нет: они не используются, а если всё же встретятся, сверка покажет их обычным расхождением.
- Расхождение — дефект с причиной: `нет на цели`, `лишняя на цели`, `опции различаются: <поля>`, `индекс <имя>: нет / различается`. Исключение — коллекции из `create` в потоке: для них расхождение опций — «известное ограничение».
- Источник может быть 6.0+, цель репетиции — 4.4. Поля, которые сервер добавляет сам в зависимости от версии, нормализуются по списку из кода (пополняется по результату первого `--schema-only` на стенде). Runbook требует разобрать любое расхождение первого `--schema-only` до старта.
- Выполняется при старте (результат — в лог) и в итоговом отчёте.

### Вывод
- Раз в 10 с — INFO `сверка`: `checked`, `matched`, `defects`, `unverified`, `queue`, `stream_lag` (поток `verify` от времени источника), `ckpt_lag` (чекпойнт от потока `verify`).
- Каждый дефект — ERROR `дефект` с `ns`, `id`, `diff`.
- Итог — INFO `итог сверки` со всеми счётчиками и путями к файлам отчёта. При дефектах — ERROR с подсказкой `перепроверьте: mongo-replicator verify --ids <файл>`.
- SIGINT/SIGTERM — штатное завершение: записать позицию и отчёты, код по итогу.

### Стенд `deploy/rehearsal/`
- `docker-compose.yml`, сервис `mongo-target`:
  - `mongo:4.4`;
  - команда генерирует keyfile в томе, если его нет (`head -c 756 /dev/urandom | base64` → `chmod 400` → `chown mongodb` — без зависимости от `openssl` в образе), и делает `exec docker-entrypoint.sh mongod --replSet rsrehearsal --keyFile … --bind_ip_all`. Официальный entrypoint создаёт root во временном mongod без `--replSet`/`--keyFile`, поэтому root есть до `rs.initiate`;
  - порт `127.0.0.1:27117:27017`, тома `rehearsal-data` и `rehearsal-key`;
  - `MONGO_INITDB_ROOT_USERNAME/PASSWORD` из `.env` стенда;
  - healthcheck `CMD-SHELL` старым шеллом `mongo` (в образе 4.4 нет `mongosh`) с аутентификацией root (`-u "$$MONGO_INITDB_ROOT_USERNAME" -p "$$MONGO_INITDB_ROOT_PASSWORD" --authenticationDatabase admin`), `rs.initiate` с членом `localhost:27017` и проверка `isMaster().ismaster` (`isWritablePrimary` в 4.4 нет).
- С хоста подключение только с `directConnection=true` — и для репликатора, и для mongosh. Без него драйвер пойдёт на `localhost:27017` из конфигурации replica set, а это может быть другая mongod.
- `create-user.js`: пользователь `repl` с `readWrite` на `DST_DB` (из env), пароль через `passwordPrompt()`.
- `create-user.js` и `copy-schema.js` запускаются **`mongosh` с хоста** (Node.js, AVX не нужен; поддерживает серверы 4.2+), не из контейнера цели.
- `copy-schema.js`: читает `SRC_URI`, `DST_URI`, `DB`, `DST_DB` из env. Пароль спрашивает через `passwordPrompt()`, если в URI есть пользователь без пароля. Порядок:
  1. коллекции с опциями (`createCollection` с `options` из `listCollections` как есть);
  2. индексы (`createIndexes` со всеми полями спецификации, кроме `v`/`ns`; индекс `_id_` пропускается — его создаёт сама коллекция);
  3. views (`viewOn` + `pipeline`).

  Уже существующая коллекция на цели → ошибка и выход: стенд должен быть пустым. Данные не читаются. Ошибка создания (например, опция, которой цель не знает) — остановка с именем коллекции и текстом ошибки.
- `dev-source.override.yml`: публикует `mongo-src` из корневого compose на `127.0.0.1:27018` — для ручной проверки runbook на машине разработки (подключение с `directConnection=true`).
- `test-copy-schema.sh` + цель `make test-copy-schema`:
  1. фикстура `it_copy_schema` на `mongo-src`: capped, validator, collation, TTL, partial, unique, sparse, hidden и view (только то, что есть и в 4.4);
  2. `copy-schema.js` в одноразовом контейнере `mongo:8.0` (ради `mongosh`) в сети compose на `mongo-dst`;
  3. `verify --schema-only` в контейнере `app` → код `0`;
  4. удаление `it_copy_schema` на обеих сторонах.

## What Goes Where
- **Implementation Steps** (`[ ]`): код `verify`, разделение опций, стенд, скрипты, runbook, README, CLAUDE.md, тесты.
- **Post-Completion** (без чекбоксов): сама репетиция на машине переливки против боевого источника, заполнение журнала, решение о готовности.

## Implementation Steps

### Task 1: Экспорт позиции чекпойнта и хелперов файла состояния

**Files:**
- Modify: `internal/replicator/checkpoint.go`
- Modify: `internal/replicator/checkpoint_integration_test.go`
- Modify: `internal/statefile/statefile.go`
- Modify: `internal/statefile/statefile_test.go`

- [x] `replicator.AppliedPosition(ctx, dst, srcDB, dstDB)`: чтение с primary, `appliedAt()`, `ok=false` при отсутствии документа или пустой позиции, ошибка пары баз через `checkDBs`; doc-комментарий о строгом сравнении
- [x] `statefile`: экспорт имени файла без каталога (`Name(dstDB, dstURI) (name, cluster string)`, `Path` выражен через него), ключа кластера (`ClusterKey`) и атомарной записи JSON (`WriteJSON(path string, v any) error` поверх `writeTemp`, `Write` выражен через неё)
- [x] юнит-тесты `Name`/`ClusterKey` (тот же ключ для разного порядка хостов, `srv:`) и `WriteJSON` (содержимое, отсутствие временных файлов после записи)
- [x] интеграционные тесты `AppliedPosition`: нет документа; документ аренды без позиции; позиция из токена; позиция из `clusterTime` при неразбираемом токене; чужая пара баз
- [x] `make test`, `make test-integration`, `make lint` — зелёные

### Task 2: Разделить опции на общие, репликации и сверки

**Files:**
- Modify: `cmd/mongo-replicator/options.go`
- Modify: `cmd/mongo-replicator/options_test.go`
- Modify: `cmd/mongo-replicator/main.go`
- Modify: `cmd/mongo-replicator/main_test.go`
- Modify: `cmd/mongo-replicator/log.go`
- Modify: `cmd/mongo-replicator/credentials.go`

- [x] `commonOptions` (встраивается), `runOptions`, `verifyOptions` по таблице из Technical Details; общие проверки — одна функция для обеих форм; `setupLog` и `withPanel` принимают то, что им нужно (общие опции / опции запуска)
- [x] разбор: режим по первому аргументу `verify`; `parseOpts` (запуск) отвергает любой позиционный аргумент с подсказкой «`verify` должна быть первым аргументом»; `parseVerifyOpts` (сверка); `--help` у каждой формы своя справка
- [x] проверки сверки: `--rate >= 1`, `--sample` в `(0, 1]`, взаимоисключения `--ids`/`--schema-only`/`--since`/`--until-caught-up`/`--sample` (код `2`); `flagGiven` смотрит аргументы без ведущего `verify`
- [x] `main` разветвляется по режиму; поведение запуска репликатора не меняется (существующие тесты остаются зелёными)
- [x] тесты `parseVerifyOpts`: успешные формы, env `VERIFY_*`, каждое взаимоисключение, опции репликации в `verify` (`--start-ts`, `--apply-drop`, `--progress`) → ошибка
- [x] тесты `parseOpts`: `--log-level debug verify` и `-- verify` → ошибка с подсказкой; `verify --help` и `--help` → `isHelp(err)` и разные тексты справки
- [x] `make test`, `make lint` — зелёные
- ➕ `runVerify` пока заглушка в `main.go` (ERROR «сверка ещё не реализована», код `1`); в Task 8 переезжает в `verify.go`

### Task 3: Вердикт, выборка и дифф (`internal/verify`)

**Files:**
- Create: `internal/verify/verdict.go`
- Create: `internal/verify/verdict_test.go`

- [x] `verdict(s1, d, s2 bson.Raw, eventInWindow bool) result` → `matched`/`defect`/`hot`; `nil` — «документа нет»; `S1 == S2 != D` при `eventInWindow` → `hot`
- [x] `sampled(id bson.RawValue, p float64) bool` — sha256, первые 8 байт, `p == 1` — всегда true
- [x] `diffFields(src, dst bson.Raw) []string` — различающиеся, отсутствующие и лишние верхнеуровневые поля, не больше 10, детерминированный порядок
- [x] табличные тесты `verdict`: равны; D==S2 при S1≠S2; S1==S2≠D без события → дефект; S1==S2≠D с событием в окне (ABA) → горячий; все разные; отсутствие на каждой стороне
- [x] тесты `sampled` (детерминизм; доля для p=0.1 на 10 000 синтетических `_id` из хелпера — в пределах ±2%; p=1) и `diffFields` (порядок, лимит 10)
- [x] `make test`, `make lint` — зелёные

### Task 4: Очередь ключей и ограничитель скорости

**Files:**
- Create: `internal/verify/queue.go`
- Create: `internal/verify/queue_test.go`
- Create: `internal/verify/limiter.go`
- Create: `internal/verify/limiter_test.go`

- [x] очередь:
  - добавление с дедупликацией (`first`, `last`), выдача готовых (`first` < позиции) пачками по коллекции до `min(500, rate)`, предел 100 000 (`full()`);
  - учёт событий у ключей в работе; `eventIn(key, t1, t2)` для вердикта; повторная постановка после вердикта;
  - rename (перенос ключей), drop (ключи → «не проверено», причина `drop`), счётчик попыток и «не проверено» после 5
- [x] ограничитель «маркерное ведро» с подставляемыми часами: `wait(ctx, n)` при `n <= ёмкости`, отмена контекстом; `n > ёмкости` — ошибка программиста (паника в тесте не нужна: батч ограничен вызывающим)
- [x] тесты очереди: дедупликация; готовность по `first` при частых событиях; пачки по коллекции; предел; событие во время проверки → повторная постановка; `eventIn` на границах (T1 исключено, T2 включено); rename; drop; 5 попыток → «не проверено»
- [x] тесты ограничителя на подставных часах: начальный запас = ёмкость, дальше не больше `rate` маркеров в секунду, пачка размером с ёмкость не блокируется навсегда, отмена контекстом
- [x] `make test`, `make lint` — зелёные
- ➕ API для Task 7: `newQueue(rate)`, `add(ns, id, t)`, `next(pos) (batch, bool)` (ключи пачки — `*item` на проверке), `eventIn(it, t1, t2)`, `done(it, r, t1) string` (причина «не проверено» или ""), `rename(from, to, t)`, `drop(ns) []*item` (ждавшие ключи — «не проверено» с `reasonDrop`), `len()`, `full()`; `newLimiter(rate, now, after)`, `wait(ctx, n)`
- ➕ `eventIn` учитывает и промежуток `[first, last]` событий до проверки (обычно не позже T1, но отдельные события в нём не хранятся); rename во время проверки — событие ключа, после вердикта ключ повторяется под новым именем; ключи на проверке при drop выходят из учёта, чтобы события пересозданной коллекции встали в очередь заново
- ➕ ограничитель резервирует маркеры сразу (порядок вызовов, отмена возвращает резерв); `n > ёмкости` — ошибка, не паника

### Task 5: Файл позиции, блокировка и отчёты

**Files:**
- Create: `internal/verify/position.go`
- Create: `internal/verify/position_test.go`
- Create: `internal/verify/lock_unix.go`
- Create: `internal/verify/lock_other.go`
- Create: `internal/verify/lock_unix_test.go`

- [x] `positionPath(stateDir, dstDB, dstURI)` → `<state-dir>/verify/<имя>`, создание каталога
- [x] загрузка:
  - нет файла → пусто;
  - битый файл → `statefile.MoveAside(…, "corrupt")`;
  - чужие `db`/`dst_db`/`cluster`/`src_cluster` → `MoveAside(…, "mismatch")`;
  - `_id` сериализуется в canonical extended JSON, ключи восстанавливаются побайтно
- [x] запись позиции через `statefile.WriteJSON`; `defects.json`/`unverified.json` с обрезкой документов до 4 КиБ и лимитом 10 000 записей
- [x] блокировка `flock` на `<имя>.lock` (unix) и заглушка (вне unix)
- [x] тесты: круговая запись и чтение (`_id`: ObjectId, строка, вложенный документ, Decimal128); битый файл и чужая пара переносятся в сторону; обрезка и лимит отчётов; `statefile.Summary` по каталогу с подкаталогом `verify/` считает только файлы состояния
- [x] тест блокировки: второй захват той же блокировки → ошибка, после освобождения — успех
- [x] `make test`, `make lint` — зелёные
- ➕ API для Task 7: `positionPaths(stateDir, dstDB, dstURI) (paths, cluster, error)` — `paths{position, lock, defects, unverified}`, имена `<dst-db>.<hash>.{json,lock,defects.json,unverified.json}`; `position{ident, Since, Token, StreamTime, Pending []*item, Counts}`, `loadPosition(path, ident) (*position, error)` (`nil` — начать с `--since`; не перенесённый файл — ошибка), `writePosition`; `report.add(ns, id, reason, src, dst)`, `writeReport` (пустой — `[]`), `readReport` и `reportEntry.id()` — для `--ids`; `lock(path) (unlock func(), error)`, занято — экспортированная `ErrLocked`
- ➕ документы в отчётах — строки canonical extended JSON (видна разница типов), обрезанные по границе руны с «…»; `null` — документа нет. Отчёты пишутся целиком при выходе: при продолжении Task 7 должна загрузить отчёты прошлого запуска (`readReport`), иначе они затрутся
- ➕ `lock_unix.go` — `unix && !aix`: в `x/sys/unix` для AIX нет `Flock`

### Task 6: Сверка схемы

**Files:**
- Create: `internal/verify/schema.go`
- Create: `internal/verify/schema_test.go`
- Create: `internal/verify/schema_integration_test.go`
- Create: `internal/verify/harness_integration_test.go`

- [x] чтение `listCollections`/`listIndexes` обеих сторон, нормализация по Technical Details (служебные поля, версионные поля, порядок полей), сравнение, список расхождений с причинами
- [x] «известное ограничение» для коллекций из множества `created`
- [x] обвязка интеграционных тестов пакета (`harness_integration_test.go`): `TestMain` с клиентами `IT_SRC_URI`/`IT_DST_URI`, имя базы `it_` + `snake(t.Name())`, удаление баз в cleanup, запись чекпойнта вручную (`writeCkpt`), чтение текущего времени источника (`srcNow`) по правилам `Eventually`
- [x] юнит-тесты нормализации и сравнения на литералах документов (в том числе разный порядок полей, служебные и версионные поля)
- [x] интеграционные: одинаковая схема → без расхождений; нет индекса на цели; другой validator; лишняя коллекция на цели; view; коллекция из `created` с другими опциями → известное ограничение
- [x] `make test`, `make test-integration`, `make lint` — зелёные
- ➕ API для Task 7: `checkSchema(ctx, src, dst, srcDB, dstDB, created map[string]bool) ([]SchemaDiff, error)`, `SchemaDiff{NS, Reason, Known}` (ns — источника, порядок по имени); `replicator.CheckpointColl` — экспорт имени служебной коллекции; обвязка: `prepare`, `srcNow(t) bson.Timestamp`, `writeCkpt(t, db, at)` (одноимённая пара, позиция из `clusterTime`), `appendSrcNote` для условий `Eventually`
- ➕ `info` коллекции сравнивается вместе с опциями (поля `info.*`), `idIndex` — нет (индекс `_id_` есть в `listIndexes`); из `collation` убирается `version` (версия ICU); у коллекции из `created` известное ограничение — и расхождение типа, и view, которого нет на цели (репликатор view не создаёт); индексы такой коллекции — дефект
- ⚠️ числа сравниваются с типом: если `copy-schema.js` (mongosh) запишет ключ индекса или `expireAfterSeconds` другим числовым типом, `--schema-only` покажет расхождение — разобрать в Task 9

### Task 7: Цикл сверки (`Verifier.Run`)

**Files:**
- Create: `internal/verify/verify.go`
- Create: `internal/verify/stream.go`
- Create: `internal/verify/verify_integration_test.go`

- [ ] `Config` (src, dst, DB, DstDB, Since, Rate, Sample, UntilCaughtUp, StateDir, SrcCluster, DstCluster, IDs, SchemaOnly) и `Verifier` с `Run(ctx) (Report, error)`
- [ ] поток: `watch` базы с `startAtOperationTime` (или токеном из позиции), `showExpandedEvents`, `$project` с `_id` (токен), `operationType`, `ns`, `documentKey._id`, `clusterTime`, `to`; без `updateLookup`; события → очередь; пауза чтения при `full()`; `ChangeStreamHistoryLost` → перманентная ошибка с подсказкой («поток сверки выпал из окна oplog: удалите позицию и начните с более позднего --since»)
- [ ] обработчик: готовые пачки по `AppliedPosition` (строго `>`), конвейер до 8 пачек; S1 (primary, majority) → ожидание чекпойнта > T1 → D → S2 → ожидание потока `verify` за T2 → вердикт с `eventIn`; сопоставление по точному `raw _id`; ограничитель на S1 и S2
- [ ] `--until-caught-up`, `--ids` (без потока и позиции), `--schema-only`; лог раз в 10 с, ERROR на дефект, итоговый `Report`, запись позиции раз в 10 с и при выходе, отчёты при выходе
- [ ] интеграционные тесты на вручную записанном чекпойнте:
  - совпадение: документы скопированы на цель руками;
  - испорченный документ цели → дефект с диффом;
  - лишний документ на цели;
  - недошедшее удаление;
  - ключ с временем ≥ позиции не проверяется, пока чекпойнт не станет строго больше;
  - `--sample`;
  - `--ids`;
  - ABA (документ изменён и возвращён между S1 и S2) → не дефект
- [ ] интеграционный тест продолжения: остановить `Run` с непустой очередью, запустить снова → каждый ключ проверен ровно один раз (счётчики), поток продолжен с токена
- [ ] сквозной интеграционный тест: настоящий репликатор через маленький runner пакета (`replicator.New`, `Run` в горутине, отмена, ожидание возврата, `Release` в cleanup; аренда по умолчанию 60 с) при живых записях писателя → `--until-caught-up` даёт 0 дефектов; затем испорченный документ на цели → дефект
- [ ] `make test`, `make test-integration`, `make lint` — зелёные

### Task 8: Подкоманда `verify` в `main`

**Files:**
- Create: `cmd/mongo-replicator/verify.go`
- Create: `cmd/mongo-replicator/verify_test.go`
- Create: `cmd/mongo-replicator/verify_integration_test.go`
- Modify: `cmd/mongo-replicator/main.go`
- Modify: `cmd/mongo-replicator/errors.go`

- [ ] `runVerify(opts, srcOpts, dstOpts, logFile) int`:
  - `prepareClients` (пароль с терминала для обеих сторон), `appName` источника `mongo-replicator-verify`;
  - лог как у репликатора, без панели; `connect`, `stateDir`;
  - SIGINT/SIGTERM → отмена контекста; коды `0`/`1`/`2`
- [ ] подсказки оператору:
  - нет `--since` и нет позиции;
  - файл позиции занят;
  - поток выпал из окна oplog;
  - чужая пара баз в чекпойнте;
  - чекпойнта на цели ещё нет: `verify` ждёт и пишет WARN раз в минуту, а не падает
- [ ] юнит-тесты: код выхода по `Report` и ошибке, тексты подсказок (важен сам текст)
- [ ] интеграционный тест на compose: `runVerify` со схемой без расхождений и без ключей → код `0`; с подложенным дефектом → код `1`
- [ ] `make test`, `make test-integration`, `make lint` — зелёные

### Task 9: Стенд репетиции и копия схемы

**Files:**
- Create: `deploy/rehearsal/docker-compose.yml`
- Create: `deploy/rehearsal/dev-source.override.yml`
- Create: `deploy/rehearsal/.env.example`
- Create: `deploy/rehearsal/.gitignore`
- Create: `deploy/rehearsal/create-user.js`
- Create: `deploy/rehearsal/copy-schema.js`
- Create: `deploy/rehearsal/test-copy-schema.sh`
- Modify: `Makefile`

- [ ] compose цели по Technical Details: keyfile генерируется в контейнере, root из `.env`, порт `127.0.0.1:27117`, тома, healthcheck с аутентификацией, `rs.initiate`, `isWritablePrimary`
- [ ] `dev-source.override.yml` — порт `127.0.0.1:27018` для `mongo-src` корневого compose
- [ ] `create-user.js`: `readWrite` только на `DST_DB`, пароль через `passwordPrompt()`, повторный запуск — понятная ошибка «пользователь уже есть»
- [ ] `copy-schema.js`: коллекции с опциями, индексы без `_id_`, views; непустая цель — отказ; ошибка создания — остановка с именем коллекции; вывод — список созданного
- [ ] `test-copy-schema.sh` и цель `make test-copy-schema`: фикстура из Technical Details; `copy-schema.js` на `mongo-dst`; `verify --schema-only` в `app` → `0`; уборка `it_copy_schema`
- [ ] `make test-copy-schema` — зелёная; `docker compose -f deploy/rehearsal/docker-compose.yml config` — без ошибок; стенд поднимается (`up -d`, healthy) и пускает root с хоста через `directConnection=true` (ручная проверка, не тест)

### Task 10: Runbook репетиции

**Files:**
- Create: `docs/runbook-rehearsal.md`

- [ ] предпроверка:
  - README «Проверка перед запуском» (поток по базе, `--start-ts` в окне);
  - Docker и `mongosh` на машине (пакет `mongodb-mongosh`, AVX не нужен), сборка бинарника;
  - цель репетиции — `mongo:4.4` (на машине переливки mongo 5.0+ не запускается); исходим из того, что возможности 8.0, которых нет в 4.4, не используются;
  - свободное место — оценить по скорости изменений источника за первые минуты догона (`applied`/с × средний размер документа × длительность репетиции) с запасом в 2 раза
- [ ] стенд:
  - `.env`, `docker compose up -d`, `create-user.js`, `copy-schema.js`;
  - `verify --schema-only` → `0`, любое расхождение разобрать до старта;
  - везде `directConnection=true`
- [ ] запуск:
  - `START_TS` = час назад, с пробой окна `startAtOperationTime`;
  - окна tmux, свой `--log-file` у каждого процесса: 1 — репликатор под `repl`, 2 — `verify --since $START_TS --rate 200`, 3 — оператор;
  - в первые минуты сравнить рост очереди `verify` со скоростью сверки и при росте подобрать `--rate`/`--sample`;
  - ожидание догона до лага < 10 с с записью скорости догона
- [ ] сценарии F1–F7 с действием, ожиданием и критерием:
  - F1 — Ctrl+C и перезапуск;
  - F2 — `kill -9`: перезапуск сразу даёт код `1` с подсказкой про аренду, через ≥60 с работает с чекпойнта;
  - F3 — `docker restart` цели;
  - F4 — `docker pause` на 90 с и `unpause`;
  - F5 — второй репликатор на ту же `--dst-db`: код `1`, первый работает;
  - F6 — стоп-кран `verify` на 5 минут;
  - F7 — 1–2 ч ровной работы: лаг, RSS через `ps -o rss`, ERROR в логе.

  После каждого: лаг < 10 с и 0 дефектов. Фактическое поведение F2, F4 и F5 сверить с кодом и интеграционными тестами аренды.
- [ ] известные ситуации:
  - E11000 по уникальному индексу: повторяющийся WARN `ошибка репликации, повтор` с E11000 → удалить мешающий документ на цели (README «Уникальные индексы»); в журнал; на готовность не влияет, если после удаления сверка чистая;
  - DDL между `START_TS` и копией схемы (окно, аналог окна дампа): `create` существующей коллекции терпится, `drop` останавливает репликатор (решение по README «После остановки на drop»), upsert в удалённую коллекцию создаёт её без опций — ссылка на README «Ограничения»
- [ ] «стоп-кран» нагрузки: сначала `verify`, затем при необходимости репликатор (продолжит с чекпойнта, пока позиция в окне oplog); второй поток `verify` повторяет просмотр oplog — это его основная цена
- [ ] финал:
  - остановить `verify` в окне 2, затем `verify --until-caught-up`, перепроверить «не проверено» через `--ids`;
  - критерии готовности;
  - журнал репетиции — таблица: `START_TS`, время и скорость догона, итог F1–F7, финальный отчёт, неожиданные события (rename/drop/E11000);
  - уборка: `down -v`, файлы состояния, каталог `verify/`, `.env`
- [ ] проверить runbook вручную на машине разработки: `mongo-src` через `dev-source.override.yml` → стенд репетиции, до F1 включительно; расхождения исправить в runbook

### Task 11: Verify acceptance criteria
- [ ] все требования Overview реализованы: `verify` не пишет на цель и не берёт аренду (по коду — только `find`/`watch`/`listCollections`/`listIndexes` и чтение чекпойнта)
- [ ] остановка `verify` не влияет на работающий репликатор (сквозной тест из Task 7 или ручная проверка на compose)
- [ ] граничные случаи: горячий документ, ABA, события с одним `clusterTime`, rename в окне, drop в окне, отсутствие чекпойнта, позиция чужой пары, занятый файл позиции
- [ ] `make test`, `make test-integration`, `make test-copy-schema`, `make lint` — зелёные

### Task 12: [Final] Update documentation
- [ ] README «Требования»: цель — replica set MongoDB 4.4+ (интеграционные тесты — на 8.0; возможности 8.0, которых нет в 4.4, не используются)
- [ ] README: раздел «Сверка» — назначение, опции, вердикт, нагрузка и стоп-кран, чтения только с primary, файлы позиции и отчётов, коды выхода, ссылка на `docs/runbook-rehearsal.md`; в таблице опций — пометка, какие опции относятся к какой форме
- [ ] CLAUDE.md:
  - раскладка: `internal/verify`, подкоманда в `cmd/mongo-replicator/verify.go`, `deploy/rehearsal/`;
  - зависимости: `replicator, statefile ← verify ← main`;
  - файл позиции в `<state-dir>/verify/` (имя совпало бы с файлом состояния репликатора);
  - строгое `>` к позиции чекпойнта;
  - исключение из правила `newIT` для сквозного теста `verify`;
  - `make test-copy-schema`;
  - стенд репетиции на `mongo:4.4`: в образе нет `mongosh`, скрипты — `mongosh` с хоста
- [ ] move this plan to `docs/plans/completed/`

## Post-Completion
*Items requiring manual intervention or external systems — no checkboxes, informational only*

**Репетиция на машине переливки:**
- пройти `docs/runbook-rehearsal.md` против боевого источника, заполнить журнал
- решение «готовы / не готовы» — по критериям runbook; при дефектах — разбор `_id`, времени события и строк лога до настоящей переливки
- если за время репетиции на источнике случились rename или drop, записать их в журнал и сверить с разделом README «Ограничения»
