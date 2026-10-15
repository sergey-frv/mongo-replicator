# mongo-replicator

Репликация одной базы MongoDB между кластерами через change streams. Код — `cmd/mongo-replicator` (CLI, `main.go`) и `cmd/mongo-replicator/replicator`.

## Команды
- `make test` — unit-тесты в контейнере `app`
- `make test-integration` — интеграционные (`-tags integration`, `-p 1`); compose поднимает `mongo-src`, `mongo-dst`, `toxiproxy`
- `make lint`, `make build` (`build` делает `go mod tidy` + `go mod vendor`, зависимости вендорятся)

## Где живёт состояние
- **на цели** — один документ `{_id: "checkpoint"}` в `<dst-db>._replicator_checkpoint` (`ckptColl`/`ckptID`, `checkpoint.go`): позиция, пара баз, аренда. Отдельной служебной базы нет, на цели нужен только `readWrite` на `--dst-db`; `SystemDBs` — только `admin`, `config`, `local`
- служебную коллекцию **всегда** отсекает `streamPipeline` (`$match` по `ns.coll`), но rename проходит фильтр намеренно: rename в `_replicator_checkpoint` или из него ловит `applyDDL` до `checkLease()` и возвращает перманентную `ErrReservedCollection`. Меняя фильтр, сохранять пропуск rename
- в тестах списки коллекций цели сравнивать через `collNames` (исключает служебную), не через `ListCollectionNames`
- **на машине** — трафик процесса в `<state-dir>/<dst-db>.<8 hex sha256(ключа кластера)>.json` (`statefile.go`); потеря файла не ломает репликацию, ошибки записи — WARN. Сводка «на машине» — сумма всех `*.json` каталога
- ключ кластера (`clusterKey`) — по URI цели как написан, не по разрешённым хостам: `srv:<host>` для `mongodb+srv` (SRV с `srvMaxHosts` даёт случайное подмножество), для `mongodb://` — хосты в нижнем регистре с портом, отсортированные
- битый файл или чужая пара баз/кластер — файл переносится в сторону (`.corrupt-*`/`.mismatch-*`), не удаляется; база файла читается один раз за процесс и только после проверки пары баз
- пустые `Config.StateFile`/`StateDir` выключают файлы и сводку — так работает большинство интеграционных тестов

## Сетевые тесты (Toxiproxy)
- модуль `github.com/Shopify/toxiproxy/v2` и образ закреплены на **v2.12.0**: v2.13.0 требует go 1.27, а проект и `Dockerfile.dev` на go 1.26
- URI через прокси (`IT_SRC_PROXY_URI`/`IT_DST_PROXY_URI`) обязаны иметь `directConnection=true`, иначе драйвер возьмёт адрес узла из конфигурации replica set и пойдёт мимо прокси
- прокси из `Populate` нельзя сразу `Enable`/`Disable` (409) — брать их через `Proxy(name)` (`populateProxies`)
- «зависание» — `timeout`-токсик на **обоих** направлениях (`hang`); только `downstream` (умолчание) пропускает запросы до сервера
- без `IT_PROXY_API` сетевые тесты пропускаются
