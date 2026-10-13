# mongo-replicator

Односторонняя репликация MongoDB через change streams.

Назначение: догонять целевой кластер более старой версии (например, 5.0) до источника более новой (например, 8.0), когда oplog replay, `mongorestore` и `mongosync` неприменимы из-за разницы версий.

Переносятся DML (`insert`, `update`, `replace`, `delete`) и DDL `create`, `drop`, `dropDatabase`, `rename`, `createIndexes`, `dropIndexes`. Базы `admin`, `config`, `local` и `_replicator` не реплицируются.

## Требования

- источник: replica set, MongoDB 6.0+ (нужен `showExpandedEvents` для DDL);
- цель: на неё пишет только репликатор;
- на цели уже восстановлен полный дамп источника, а `--start-ts` указывает на момент **до** начала этого дампа и ещё попадает в окно oplog источника.

## Запуск

```sh
make build    # bin/mongo-replicator, ревизия из git describe
```

| Опция | Env | По умолчанию | |
|---|---|---|---|
| `--src` | `SRC_URI` | обязательна | URI источника |
| `--dst` | `DST_URI` | обязательна | URI цели |
| `--start-ts` | `START_TS` | `0` | unix-сек, с которых читать поток при отсутствии чекпойнта |
| `--batch` | `BATCH_SIZE` | `1000` | максимум операций в одной пачке записи, `>= 1` |
| `--dbg` | `DEBUG` | `false` | debug-уровень и человекочитаемый вывод вместо JSON |

Опции принимаются только с двумя дефисами: `--src`, а не `-src`, как было в первой версии (`-src` разбирается как набор коротких флагов `-s -r -c`).

```sh
./bin/mongo-replicator \
    --src "mongodb://user:pass@src1:27017,src2:27017/?replicaSet=rs8" \
    --dst "mongodb://user:pass@dst1:27017,dst2:27017/?replicaSet=rs5" \
    --start-ts 1791900000
```

URI удобно держать в `cmd/mongo-replicator/.env` (он в `.gitignore`):

```sh
# cmd/mongo-replicator/.env
SRC_URI=mongodb://user:pass@src1:27017/?replicaSet=rs8
DST_URI=mongodb://user:pass@dst1:27017/?replicaSet=rs5
```

```sh
set -a; . cmd/mongo-replicator/.env; set +a
./bin/mongo-replicator --start-ts 1791900000
```

`--start-ts` нужен только на первом запуске: дальше поток продолжается с чекпойнта на цели, а опция игнорируется.

### Локальная проверка

```sh
make docker-volumes   # один раз: тома go-build-cache и go-lint-cache
make run              # mongo-src (8.0, rs8) + mongo-dst (5.0, rs5) и репликатор с --start-ts = сейчас
```

Запуск против локальных mongo — только через `make run`: в rs-конфиге члены записаны как `mongo-src`/`mongo-dst`, с хоста эти имена не резолвятся.

### Разработка

```sh
make test               # юнит-тесты, mongo не нужна
make test-integration   # интеграционные тесты против mongo-src и mongo-dst
make lint               # golangci-lint
make down               # остановить контейнеры и удалить их тома
```

## Гарантии

- at-least-once: после сбоя поток продолжается с последнего сохранённого resume token, события после него читаются повторно;
- операции идемпотентны: insert/update/replace применяются как replace-upsert полного документа по `_id`, delete — по `_id`; повторное применение даёт то же итоговое состояние. DDL «уже есть» / «уже нет» при повторе игнорируется;
- чекпойнт хранится на цели (`_replicator.checkpoint`) и двигается только после успешной записи данных; клиент цели работает с `w:majority`, чтобы чекпойнт не обогнал данные при failover;
- после DDL чекпойнт сохраняется сразу, чтобы rename/drop не повторялись после рестарта;
- временные ошибки (сеть, failover) — повтор через 5 с без выхода.

Коды выхода:

| Код | Причина |
|---|---|
| `0` | остановка по SIGINT/SIGTERM; незаписанный хвост будет перечитан при следующем запуске |
| `1` | точка возобновления выпала из oplog источника (нужна новая полная заливка цели); нет ни чекпойнта, ни `--start-ts`; некорректный URI или кластер недоступен (`Ping` за 10 с) |
| `2` | ошибка опций |

## Ограничения

Поток читается с `fullDocument: updateLookup`: для `update` на цель пишется текущая версия документа, найденная на источнике в момент чтения события. Отсюда два случая потери данных:

- **(a) updateLookup после rename или drop + recreate.** Если к моменту чтения `update` коллекцию уже переименовали или пересоздали, документ не находится, `fullDocument` пустой и событие пропускается. На цели в новой коллекции остаётся старая версия документа. Воспроизводит `TestLimitationUpdateLookupAfterRename`.
- **(b) rename или drop + recreate в окне `--start-ts` → конец дампа.** Дамп уже содержит итоговую коллекцию `b` целиком, а повтор потока создаёт на цели `a` только из событий окна и переименовывает её в `b` с `dropTarget` — документы `b`, не тронутые в окне, уничтожаются. Воспроизводит `TestLimitationRenameInCatchUpWindow`.

Оба теста помечены `t.Skip`; без него они падают, пока ограничение не устранено. Будущий фикс — включить на источнике `changeStreamPreAndPostImages` и читать поток с `fullDocument: required`: post-image сохраняется в момент изменения и не зависит от последующих rename/drop. До тех пор не выполняйте rename и drop + recreate коллекций в окне дампа и пока репликатор отстаёт.

Не поддерживаются (событие пропускается с WARN в логе, переносите вручную):

- `collMod` и прочие изменения опций существующей коллекции;
- views — не создаются;
- опции `create` (capped, validator, collation, timeseries и т.п.) — коллекция создаётся обычной;
- шардирование (`shardCollection`, `refineCollectionShardKey` и т.п.).
