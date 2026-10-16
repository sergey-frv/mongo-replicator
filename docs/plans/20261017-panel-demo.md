# Демо панели статуса: `cmd/panel-demo`

## Overview
- добавить в репозиторий программу `cmd/panel-demo`: запускается в терминале без параметров и показывает только панель статуса на мок-данных, без кластеров
- зачем: проверять панель вручную (отрисовку, клавиши, восстановление терминала) за секунды, без MongoDB и `--start-ts`; пользователь уже проверил панель этим демо, собранным вне репозитория, — «панель работает»
- демо должно проверять **тот же код**, что и утилита: панель (`internal/panel`) и выбор ввода с отправкой сигналов. Поэтому выбор ввода (`panelInput`, `openTTY`, `foreground`) и `signalSelf` переезжают из `cmd/mongo-replicator` в `internal/panel`, и их вызывают и `main`, и демо
- исходник проверенного демо: `/tmp/claude-1000/-home-sf-github-mongo-replicator/317291ca-6650-49a0-b651-bd52b6bee17b/scratchpad/panel-demo-main.go` — основа для `cmd/panel-demo`

## Context (from discovery)
- ветка `master` (`c286284`), ветка `panel-raw-input` влита: панель читает клавиатуру в raw-режиме; API `panel.Start(status, out, in *os.File, sendSignal func(os.Signal), extra...)`
- выбор ввода и сигнал сейчас в `cmd/mongo-replicator`:
  - `main.go`: `panelInput(stdin, openTTY, isForeground)`, `openTTY` (`os.OpenFile("/dev/tty")`), `signalSelf`; вызов в `run`: `panelInput(os.Stdin, openTTY, foreground)`, `withPanel(..., in)` → `panel.Start(status, os.Stdout, in, signalSelf)`
  - `tty_unix.go` (`//go:build unix`, `foreground` через `TIOCGPGRP`), `tty_other.go` (`false`)
  - тесты: `main_test.go` — `TestPanelInputTerminalStdin`, `TestPanelInputOpensTTY`, `TestPanelInputNoTTY`, `TestPanelInputBackgroundStdin`, `TestPanelInputBackgroundTTY`, `TestSignalSelf`; `tty_unix_test.go` — `TestForegroundNotControllingTerminal`; хелперы `openPTMX`, `openDevNull`
- `internal/panel`: `panel.go`, `jobs_linux.go`/`jobs_other.go`, тесты `panel_test.go`, `jobs_linux_test.go`, `pty_linux_test.go`
- демо (scratchpad): мок-статус `sim` со сценарием запуск (2 с) → догон (лаг ~28 ч тает ~30 с, 12000/с) → ровный поток (5–25/с, лаг 0–1,5 с) → повтор 40–50 с с `LastErr` → поток; DDL после 20 с; WARN над панелью раз в ~25 с; Ctrl+C — итоговый кадр «остановлен» и строка «остановлен по сигналу; применено N (демо)»; stdout не терминал — выход 2; в мок-данных `math/rand/v2`
- `CLAUDE.md`: раздел «Панель и терминал» (ссылается на `run` → `panelInput`, `foreground`), карта пакетов; правила «Тесты»; псевдотерминал в тестах — без байта 0x1a, без `/dev/tty`
- `.golangci.yml`: `gosec` проверяет не-тестовый код (G404 на `math/rand`, G115 на преобразования int) — в демо без `rand`
- `Makefile`: `build` собирает `bin/mongo-replicator` на хосте; цели для демо нет
- в рабочей копии незакоммиченная правка `.gitignore` пользователя — не трогать и не коммитить

## Development Approach
- **testing approach**: Regular
- complete each task fully before moving to the next
- **CRITICAL: every task MUST include new/updated tests** for code changes in that task
- **CRITICAL: all tests must pass before starting next task** - no exceptions
- **CRITICAL: update this plan file when scope changes during implementation**
- тесты — по разделу «Тесты» `CLAUDE.md`; запуск только через `make test`, `make lint`, `docker compose run` (не локальный `go test`); тесты не открывают `/dev/tty` и не пишут в псевдотерминал 0x1a
- перенос кода в Task 1 — без изменения логики: тела функций те же, меняются имена (экспорт) и пакет; тесты переезжают вместе с кодом, их имена сохраняются
- коммитить только файлы задачи (`git add <пути>`), не `-A`

## Testing Strategy
- перенесённые функции — существующими тестами, перенесёнными в `internal/panel` (`make test`, по `-v` — тесты на псевдотерминале выполнились, не пропущены)
- мок-статус демо — юнит-тесты `step` с подставляемым временем: сценарий детерминирован (без `rand`), ожидания — литералы
- демо целиком (клавиши, терминал) — ручная проверка в Post-Completion

## Progress Tracking
- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix
- update plan if implementation deviates from original scope
- keep plan in sync with actual work done

## Solution Overview
- `internal/panel` получает выбор ввода и отправку сигнала; раскладка файлов:
  - `input.go` (без build-тегов): `panel.Input(stdin *os.File) (in *os.File, closeIn func())` — обёртка над неэкспортируемой `pickInput(stdin, openTTY, isForeground)` (бывшая `panelInput`, логика без изменений; не `input` — в `Start` уже есть локальная `input`), `openTTY` (только `os.OpenFile("/dev/tty")`, без unix-вызовов), `SignalSelf(sig os.Signal)` (бывшая `signalSelf`)
  - `tty_unix.go` (`//go:build unix`) / `tty_other.go` (`//go:build !unix`) — только `foreground`; иначе вне Unix `Input` не соберётся
  - тесты: `input_test.go` — `TestPanelInput*`, `TestSignalSelf`, хелперы `openPTMX`/`openDevNull` (здесь они появляются впервые; `openPTY` из `pty_linux_test.go` не трогать — он только для Linux); `tty_unix_test.go` — `TestForegroundNotControllingTerminal`
  - **`panel.Input` не тестировать и не вызывать из тестов** (`withPanel`, `Start`, `sim`): под `go test` stdin — `/dev/null`, и `Input` откроет настоящий `/dev/tty`; логика покрыта тестами `pickInput` с подставными `openTTY`/`isForeground`
- `main`: `in, closeIn := panel.Input(os.Stdin)`, `panel.Start(status, os.Stdout, in, panel.SignalSelf)`; в `cmd/mongo-replicator` остаются только `withPanel` и обвязка
- `cmd/panel-demo` (`package main`):
  - `main.go` — как в scratchpad, но через `panel.Input`/`panel.SignalSelf`; `signal.NotifyContext(SIGINT, SIGTERM)`; `recover` → `kill` панели, как в утилите
  - `sim.go` — мок-статус: `newSim(start time.Time)`, `step(now time.Time) (warns []string)`, `status()`, `stopped()`; значения — детерминированные функции времени с запуска `up` (не `now.Second()`), без `rand`
  - **`step` не логирует сам**: тексты WARN собираются под мьютексом и возвращаются, `main` пишет их в лог после возврата `step`. Иначе взаимоблокировка: `log.Warn` под `sim.mu` → консоль панели `teaWriter.Write` → `p.Send` в небуферизованный канал bubbletea, а цикл событий bubbletea на `tickMsg` ждёт тот же `sim.mu` в `status()` (`internal/panel/panel.go:51-54, 267-274`). В исходнике из scratchpad этот баг есть — не переносить
  - переменная в `main` — `demo` (не `sim`: затеняет тип)
- `make panel-demo` — сборка `bin/mongo-replicator-panel` (имя, под которым пользователь уже проверял демо)

## Technical Details
- сценарий `step` по времени с запуска `up` (как в проверенном демо):
  - `up < 2s` — `StateStarting`, `Applied` 0, `Lag` -1
  - `40s ≤ up < 50s` — `StateRetry`, `Retry` 1, `LastErr` (демо-текст); один WARN при входе в повтор
  - иначе `StateRunning`, `Retry` 0, `LastErr` пусто. Формулы (`sec := int(up / time.Second)`):
    - `catchUp := 28*time.Hour - up*3360` (догон тает к 30 с)
    - скорость: `12000`, пока `catchUp > 0`, затем `5 + float64(sec%21)`
    - лаг: `max(catchUp, time.Duration(sec%16)*100*time.Millisecond)`, `AppliedAt = now - lag`
  - операций за шаг 200 мс: `ops := uint64(rate/5) + 1` (float → uint64, без преобразования знакового в беззнаковое — `gosec` G115); `Applied += int64(ops)` (или `//nolint:gosec` с причиной, как в `internal/traffic/traffic.go:77`)
  - `LastBatch` с `Ins/Upd/Del` (60/10/30 %) и коллекциями `PrivateUrls, Counters, PublicUrls`; трафик: источник ↓ 510 Б, цель ↑ 170 Б на операцию (как в реальном запуске); `Machine` — 3 процесса, 2 активны
  - `up > 20s` — `LastDDL` `createIndexes urlshrinker.PublicUrls (slug_1)`
  - WARN (возвращаются из `step`): при входе в повтор — «ошибка репликации, повтор (демо)»; периодический «… (демо)» — при `up > 10s`, не чаще раза в 25 с
- ожидания тестов (литералы, свежий `newSim(start)` на случай):
  - `up` 1 с: «запуск», `Applied` 0, `Lag` -1
  - `up` 10 с: «работает», `Lag` = `18h40m` (67 200 с), `Rate` 12000, после одного шага `Applied` 2401
  - `up` 35 с: `Rate` 19, `Lag` 300 мс
  - `up` 45 с: «повтор», `Retry` 1, `LastErr` — литерал текста; `step` вернул WARN повтора; повторный шаг в окне повтора — без WARN
  - `up` 55 с: «работает», `Retry` 0, `LastErr` пусто
  - `LastDDL`: нет при 20 с, есть при 21 с
  - периодический WARN: шаги одного `sim` на 11 с — один WARN, 20 с — нет, 37 с — один
  - `stopped()` — «остановлен»
- `main.go` демо: stdout не терминал (`term.IsTerminal(int(os.Stdout.Fd()))` с `//nolint:gosec // дескриптор stdout помещается в int`, как в `cmd/mongo-replicator/log.go`) — сообщение в stderr и выход 2; `in == nil` — WARN «клавиатура не читается…»; Ctrl+C → итоговый кадр, `closeIn`, строка «остановлен по сигналу; применено N (демо)», выход 0
- `Makefile`: цель `panel-demo` — `go build -o bin/mongo-replicator-panel ./cmd/panel-demo` (на хосте, как `build`; без `go mod tidy`/`vendor` и без `-ldflags`: у демо нет `revision`); `.PHONY` и описание `##`, как у `docker-volumes` (у `build`/`test` его нет)

## What Goes Where
- **Implementation Steps**: перенос выбора ввода в `internal/panel`, `cmd/panel-demo`, цель `Makefile`, тесты, README, CLAUDE.md
- **Post-Completion**: ручная проверка демо в настоящем терминале

## Implementation Steps

### Task 1: Выбор ввода и сигнал — в `internal/panel`

**Files:**
- Create: `internal/panel/input.go`, `internal/panel/input_test.go`
- Create: `internal/panel/tty_unix.go`, `internal/panel/tty_other.go`, `internal/panel/tty_unix_test.go`
- Modify: `cmd/mongo-replicator/main.go`, `cmd/mongo-replicator/main_test.go`
- Delete: `cmd/mongo-replicator/tty_unix.go`, `cmd/mongo-replicator/tty_other.go`, `cmd/mongo-replicator/tty_unix_test.go`

- [x] перенести в `internal/panel` по раскладке из Solution Overview: `panelInput` → `pickInput`, `openTTY`, `signalSelf` → `SignalSelf` — в `input.go`; `foreground` — в `tty_unix.go`/`tty_other.go`; логика без изменений; `Input(stdin)` и `SignalSelf` — с doc-комментариями (в комментарии `Input` — не вызывать из тестов)
- [x] `main`: `panel.Input(os.Stdin)` и `panel.SignalSelf` вместо локальных функций; убрать ставшие лишними импорты (`golang.org/x/term` в `main.go`, если больше не нужен; `errors`, `os/signal`, `syscall` в `main_test.go`); обновить комментарии (`withPanel` → `panel.Input`)
- [x] перенести тесты в `internal/panel` с прежними именами (`input_test.go`, `tty_unix_test.go`), хелперы `openPTMX`/`openDevNull` — в `input_test.go`, из `main_test.go` убрать; комментарии тестов — `SignalSelf`/`pickInput`; **`panel.Input` в тестах не вызывать**
- [x] `make test`, `make lint` — должно пройти; по `-v` — перенесённые тесты на псевдотерминале выполнились
- [x] сборка вне Unix не сломана: `docker compose run --rm --no-deps app sh -c 'GOOS=windows go build -o /dev/null ./...'`

### Task 2: Демо `cmd/panel-demo`

**Files:**
- Create: `cmd/panel-demo/main.go`
- Create: `cmd/panel-demo/sim.go`
- Create: `cmd/panel-demo/sim_test.go`

- [ ] `sim.go` — мок-статус по Technical Details: формулы от `up`, без `rand`, `step` возвращает WARN и сам не логирует
- [ ] `main.go` — по проверенному демо из scratchpad, через `panel.Input`/`panel.SignalSelf`; WARN из `step` — в лог после возврата `step` (`for _, w := range demo.step(now) { log.Warn().Msg(w) }`); doc-комментарий пакета: что показывает и какие клавиши работают
- [ ] тесты `sim` — по ожиданиям из Technical Details; случаи «состояние по `up`» — таблицей (отличаются только данными); WARN — отдельными тестами
- [ ] `make test`, `make lint` — должно пройти (`gosec` в `cmd/panel-demo` без замечаний)

### Task 3: Сборка демо

**Files:**
- Modify: `Makefile`

- [ ] цель `panel-demo` по Technical Details: `go build -o bin/mongo-replicator-panel ./cmd/panel-demo`
- [ ] проверить сборку в контейнере (`docker compose run --rm --no-deps app go build -o /dev/null ./cmd/panel-demo`) и что `bin/` в `.gitignore`

### Task 4: Verify acceptance criteria
- [ ] утилита и демо используют одни и те же `panel.Input`, `panel.SignalSelf`, `panel.Start` (`grep` — в `cmd/mongo-replicator` и `cmd/panel-demo` нет своих копий выбора ввода)
- [ ] демо без терминала stdout — выход 2: `docker compose run --rm --no-deps -T app sh -c 'go build -o /tmp/pd ./cmd/panel-demo && /tmp/pd >/dev/null; echo "exit=$?"'` → `exit=2` (не `go run`: тот вернёт 1)
- [ ] демо под псевдотерминалом рисует панель и по SIGINT печатает «остановлен по сигналу»: `make panel-demo`, затем `script -qec "timeout --foreground -s INT 4 ./bin/mongo-replicator-panel" /dev/null` (`--foreground` — иначе `timeout` уводит демо в свою группу, оно не на переднем плане и читает клавиатуру не будет: WARN «клавиатура не читается»); байт 0x1a не отправлять
- [ ] run full test suite: `make test`
- [ ] `make test-integration`
- [ ] `make lint` без замечаний

### Task 5: [Final] Update documentation
- [ ] README, раздел «Панель статуса»: как посмотреть панель без кластеров — `make panel-demo && ./bin/mongo-replicator-panel`, что показывает сценарий, клавиши те же
- [ ] README, раздел «Разработка»: `cmd/panel-demo/` в карте пакетов, `make panel-demo` в списке команд
- [ ] CLAUDE.md: карта пакетов — `cmd/panel-demo/` (демо панели на мок-данных); строка зависимостей — `… ← panel ← main, panel-demo`; «Команды» — `make panel-demo`
- [ ] CLAUDE.md, «Панель и терминал»: выбор ввода и `SignalSelf` — в `internal/panel` (`panel.Input`/`pickInput`), ссылки на `panelInput`/`run` поправить; `panel.Start` ввод сам не выбирает, `panel.Input` вызывают только `main` и демо, тесты — никогда; в демо-`sim` WARN не логируется под мьютексом (взаимоблокировка с `status()` панели)
- [ ] move this plan to `docs/plans/completed/`

## Post-Completion
*Items requiring manual intervention or external systems - no checkboxes, informational only*

**Ручная проверка в настоящем терминале** (`make panel-demo && ./bin/mongo-replicator-panel`):
- набор текста и Enter — ничего не появляется, панель не ломается
- Ctrl+C — итоговый кадр «остановлен», строка «остановлен по сигналу; применено N (демо)», шелл в норме
- Ctrl+Z, затем `fg`; Ctrl+\ — дамп в нормальном терминале
- `./bin/mongo-replicator-panel < /dev/null` — панель с клавишами (ввод с `/dev/tty`)
- утилита на тестовых кластерах ведёт себя как до переноса (Task 1 не меняет логику)
