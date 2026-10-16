# Панель статуса читает клавиатуру (raw-режим bubbletea)

## Overview
- при реальном запуске с панелью (`--progress tty`) любой набор в терминале ломал отрисовку: строки склеивались («7серации», «1сафик процесса»), в истории оставались старые кадры
- причина: панель (`internal/panel/panel.go`, `Start`) запускается с `tea.WithInput(nil)` — терминал остаётся в обычном режиме, сам выводит набранное в позицию курсора, а Enter переводит строку; inline-рендерер bubbletea перерисовывает кадр подъёмом курсора на N строк и после лишнего перевода строки поднимается не туда
- решение (выбрано пользователем) — как в обычных приложениях на bubbletea: панель читает клавиатуру, bubbletea переводит терминал в raw-режим (эха нет, `ISIG` выключен), нажатия приходят в панель сообщениями. В raw-режиме Ctrl+C больше не сигнал, поэтому панель сама превращает клавиши в прежние действия:
  - Ctrl+C → `SIGINT` своему процессу — дальше прежний путь `main` (`signal.NotifyContext`): дописать пачку, снять аренду, выход 0
  - Ctrl+\ → панель завершается (терминал восстановлен), **затем** `SIGQUIT` — дамп горутин печатается в нормальный терминал
  - Ctrl+Z → `tea.Suspend`, **только если есть управление заданиями** (процесс в своей группе внутри сессии шелла); иначе клавиша игнорируется — в осиротевшей группе процессов (контейнер `make run`, `ssh -t host cmd`, `script`) SIGTSTP отбрасывается ядром и bubbletea повис бы в ожидании SIGCONT
  - остальные клавиши игнорируются
- обрезка строк по ширине **не нужна**: рендерер bubbletea v2 рисует кадр в буфер ячеек шириной окна (`vendor/charm.land/bubbletea/v2/cursed_renderer.go:301`)

## Context (from discovery)
- `internal/panel/panel.go`: `Start(status, out, extra...)` — опции `tea.WithInput(nil), tea.WithoutSignalHandler(), tea.WithOutput(out)`; ошибка `p.Run()` отбрасывается (`_, _ = p.Run()`); модель `panel` с `Update` на `tickMsg`/`finalMsg`; тесты `TestPanelUpdate`, `TestTeaWriterAfterExit`, `TestStartPanel` (`panel_test.go`)
- `cmd/mongo-replicator/main.go`: `run` — `signal.NotifyContext(SIGINT, SIGTERM)`; `withPanel(opts, status, logFile)` (`:182`) вызывает `panel.Start(status, os.Stdout)` и возвращает `stop`/`kill`; `kill` — при панике в основной горутине (`recover` в `run`); `main.go` импортирует `os/signal` — не называть переменные/параметры `signal` (gocritic `importShadow`)
- bubbletea v2.1.0 (vendored), проверено ревью:
  - `WithInput(f)` с терминалом `f` — `term.MakeRaw(f)` (`tty_unix.go` `initInput`); `stop` (`finalMsg` → `tea.Quit`) и `kill` (`p.Quit()`) оба идут штатным `shutdown` → `restoreInput`
  - без `WithInput` берёт `os.Stdin`, а не терминал — `OpenTTY` (`tea.go:1036-1043`); ошибка — `Run` возвращает ошибку
  - `tea.OpenTTY` → `uv.openTTY` возвращает **один и тот же** `*os.File` дважды (`vendor/github.com/charmbracelet/ultraviolet/tty_unix.go:20`): закрыть `out` — значит закрыть `in`
  - `WithInput(r)`: `disableInput = r == nil` — интерфейс; `(*os.File)(nil)` как `io.Reader` не равен `nil` (ловушка typed nil)
  - с вводом рендерер включает kitty keyboard (флаг disambiguate) и modifyOtherKeys=2 (`cursed_renderer.go:166-171`); в kitty-совместимых терминалах при русской раскладке Ctrl+С может прийти как `ctrl+с` (кириллица) — нужен `ReportAlternateKeys` и сравнение по `msg.Keystroke()` (`BaseCode`)
  - legacy-байты 0x03, 0x1c, 0x1a декодируются в `ctrl+c`, `ctrl+\`, `ctrl+z` (`ultraviolet/decoder.go` `parseControl`)
  - `tea.Suspend` → `suspendProcess`: `Kill(0, SIGTSTP)` и ожидание SIGCONT без таймаута в цикле событий (`tty_unix.go:40-47`)
  - при старте с вводом уходят запросы терминалу (DECRQM 2026/2027, `RequestKittyKeyboard`)
  - встроенный обработчик сигналов остаётся выключенным (`WithoutSignalHandler`): SIGTERM/SIGINT извне по-прежнему обрабатывает `main`
- SIGQUIT без `signal.Notify` — обработчик рантайма Go: дамп и `exit(2)` без defer; пока панель работает, терминал в raw-режиме (без `OPOST`, kitty keyboard, скрытый курсор) — поэтому сигнал отправляется только после выхода панели
- `go test` даёт тестовому бинарнику stdin `/dev/null` — поэтому выбор ввода в `withPanel` открыл бы `/dev/tty` из теста; выбор ввода делается в `run`, тест передаёт `nil`
- `golang.org/x/sys` уже `## explicit` в `vendor/modules.txt`, `golang.org/x/sys/unix` уже вендорен; в `go.mod` уйдёт только `// indirect`
- утилита работает на Linux; тесты — в контейнере `app` (`make test`, `make lint`), `/dev/ptmx` в Docker доступен
- в рабочей копии незакоммиченная правка `.gitignore` пользователя (`/reports/`, `/research_notes/`) — не трогать и не коммитить с планом

## Development Approach
- **testing approach**: Regular
- complete each task fully before moving to the next
- **CRITICAL: every task MUST include new/updated tests** for code changes in that task
- **CRITICAL: all tests must pass before starting next task** - no exceptions
- **CRITICAL: update this plan file when scope changes during implementation**
- тесты — по разделу «Тесты» `CLAUDE.md` (условия `Eventually` без `t`, с сообщением); запуск только через `make test`, `make lint`, `docker compose run` (не локальный `go test`)
- **в тестах никогда не писать в псевдотерминал байт 0x1a (Ctrl+Z)**: настоящий Suspend остановит группу процессов `go test` или повесит её в контейнере
- **тесты не открывают `/dev/tty`** и не трогают настоящий терминал разработчика: только псевдотерминалы, созданные тестом, и подставные функции
- коммитить только файлы задачи (`git add <пути>`), не `-A`

## Testing Strategy
- клавиши — юнит-тесты `Update` с `tea.KeyPressMsg` и подставной функцией отправки сигнала (запоминает сигналы), без настоящих сигналов процессу; отдельно — проверка «есть ли управление заданиями» подставляется полем модели
- raw-режим, декодирование настоящих байтов и восстановление терминала — тест на псевдотерминале (`internal/panel/pty_linux_test.go`), без новых зависимостей:
  - мастер: `unix.Open("/dev/ptmx", O_RDWR|O_NOCTTY)`, разблокировать (`TIOCSPTLCK` = 0), номер `TIOCGPTN`, слейв `/dev/pts/N` с `O_RDWR|O_NOCTTY`
  - мастер держать открытым до окончания `stop`/`kill` (закрытый мастер даёт EIO, и `Run` завершится с ошибкой)
  - ожидание raw-режима — `require.Eventually` с условием на `unix.IoctlGetTermios` (ошибка возвращается, без `t`) и сообщением
  - если `/dev/ptmx` недоступен — `t.Skip` с причиной; по `-v` убедиться, что в контейнере `app` тест выполнился
- пакет `main` своего псевдотерминала не создаёт: для проверки «stdin — терминал» достаточно открыть мастер `/dev/ptmx` (`os.OpenFile(..., O_RDWR|O_NOCTTY, 0)`) — `IsTerminal` для него true; недоступен — `t.Skip`; тест — в `main_test.go`
- ручная проверка в настоящем терминале — Post-Completion

## Progress Tracking
- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix
- update plan if implementation deviates from original scope
- keep plan in sync with actual work done

## Solution Overview
- `panel.Start` получает **`in *os.File`** (не `io.Reader` — ловушка typed nil) и функцию отправки сигнала:
  - `in == nil` — как сейчас (`tea.WithInput(nil)`), raw-режима нет
  - иначе `tea.WithInput(in)`: bubbletea включает raw-режим и восстанавливает терминал при выходе `Run` (`stop` и `kill`)
  - страховка: до `Run` сохранить `term.GetState(in.Fd())`, после возврата `Run` — `term.Restore` (bubbletea при ранних ошибках старта выходит, не восстановив терминал); ошибку `Run` писать в `fallback`, чтобы панель не пропадала молча
- модель панели обрабатывает `tea.KeyPressMsg` по `msg.Keystroke()` (с `View().KeyboardEnhancements.ReportAlternateKeys = true`, чтобы Ctrl+С на русской раскладке давал `ctrl+c` по `BaseCode`):
  - `ctrl+c` → `sendSignal(SIGINT)`, без команды
  - `ctrl+\` → запомнить в модели `exitSig = SIGQUIT` и вернуть `tea.Quit`; горутина `Start` после возврата `Run` (терминал уже восстановлен) берёт итоговую модель и вызывает `sendSignal(pm.exitSig)`
  - `ctrl+z` → `tea.Suspend`, только если `jobControl()` true; иначе без действия
- проверка управления заданиями (`internal/panel`, Linux): `unix.Getpgrp() != unix.Getsid(0)` — в задании шелла группа процесса отличается от сессии; в контейнере, `ssh -t cmd`, `script` они равны (и это как раз осиротевшие группы). Не-Linux — false
- `main`:
  - выбор ввода — в `run` (не в `withPanel`): `panelInput(os.Stdin, openTTY)` — stdin терминал → stdin; иначе `/dev/tty` через `os.OpenFile("/dev/tty", os.O_RDWR, 0)` (не `tea.OpenTTY`: тот возвращает один файл дважды); ошибка → `nil`. Дополнительно: ввод берётся, только если процесс — в группе переднего плана терминала (`unix.IoctlGetInt(fd, unix.TIOCGPGRP) == unix.Getpgrp()`), иначе `nil` — запуск с `&` не остановится на SIGTTOU при старте панели
  - `withPanel(opts, status, logFile, in)` — ввод параметром; тест передаёт `nil`
  - функция сигнала — отправка своему процессу (`os.FindProcess(os.Getpid())` + `Signal`); переменная — `sendSignal` (не `signal`)
  - открытый `/dev/tty` закрывается после `stopP`/`killP`
- почему не встроенный обработчик сигналов bubbletea: он перехватил бы SIGINT/SIGTERM и завершил бы панель, а остановкой управляет `main` (и без панели — `--progress plain`, systemd)

## Technical Details
- сигнатура (ориентир): `func Start(status func() replicator.Status, out io.Writer, in *os.File, sendSignal func(os.Signal), extra ...tea.ProgramOption) (console io.Writer, stop, kill func())`; `sendSignal` обязателен при `in != nil`
- модель: поля `sendSignal func(os.Signal)`, `jobControl func() bool`, `exitSig os.Signal`; `Start` подставляет настоящие, тесты — свои
- `View`: `v := tea.NewView(...)`; `v.KeyboardEnhancements.ReportAlternateKeys = true`
- горутина `Start`:
  ```go
  saved, _ := term.GetState(in.Fd()) // если in != nil
  m, err := p.Run()
  // восстановить saved, если был; err → fallback
  if pm, ok := m.(*panel); ok && pm.exitSig != nil { sendSignal(pm.exitSig) }
  ```
- `main`: `panelInput(stdin *os.File, openTTY func() (*os.File, error)) (in *os.File, closeIn func())`; `openTTY` — `os.OpenFile("/dev/tty", os.O_RDWR, 0)`; проверка группы переднего плана — внутри `panelInput` (через подставляемую функцию, чтобы тестировать)
- последствия raw-режима для README (заменить абзац «Клавиатуру панель не читает…» в разделе «Панель статуса»):
  - клавиши: Ctrl+C — остановка как раньше; Ctrl+Z — приостановка, продолжать только `fg` (`bg` не работает: возврат raw-режима из фоновой группы останавливает процесс на SIGTTOU); без управления заданиями (контейнер, `ssh -t host cmd`) Ctrl+Z игнорируется; Ctrl+\ — дамп горутин после выхода панели; прочий ввод игнорируется; для работы в фоне — tmux или `--progress plain`
  - Ctrl+Z останавливает и репликацию: если пауза дольше аренды, базу может забрать другой процесс
  - если панель зависла — `kill -INT`/`kill -QUIT <pid>` из другого терминала
  - процесс убит `SIGKILL`, упал паникой не в основной горутине или фатальной ошибкой рантайма — терминал остаётся в raw-режиме с выключенным курсором и расширенной клавиатурой: `reset` (набирать вслепую; если Enter не срабатывает — Ctrl+J)

## What Goes Where
- **Implementation Steps**: панель (клавиши, ввод, восстановление терминала), выбор ввода в `main`, тесты, README, CLAUDE.md
- **Post-Completion**: ручная проверка в настоящих терминалах

## Implementation Steps

### Task 1: Панель читает клавиатуру

**Files:**
- Modify: `internal/panel/panel.go`
- Modify: `internal/panel/panel_test.go`
- Create: `internal/panel/jobs_linux.go`, `internal/panel/jobs_other.go` (проверка управления заданиями)
- Create: `internal/panel/pty_linux_test.go` (помощник псевдотерминала и тесты на нём)
- Modify: `go.mod` (`golang.org/x/sys` — прямая зависимость; `vendor/modules.txt` не меняется)

- [x] `Start`: `in *os.File`, `sendSignal`; `in == nil` — прежнее `tea.WithInput(nil)`; `WithoutSignalHandler` остаётся; страховочное восстановление `term.GetState`/`Restore`; ошибка `Run` — в `fallback`
- [x] модель: `KeyPressMsg` по `Keystroke()` — Ctrl+C → `SIGINT`; Ctrl+\ → `exitSig` и `tea.Quit`, сигнал из горутины `Start` после `Run`; Ctrl+Z → `tea.Suspend` при `jobControl()`, иначе ничего; `ReportAlternateKeys` во `View`; doc-комментарии — почему клавиши переводятся в сигналы и почему Ctrl+\ после выхода
- [x] `jobControl` для Linux (`Getpgrp() != Getsid(0)`) и false для остальных ОС
- [x] обновить существующие вызовы `Start` в тестах (`in = nil`)
- [x] тесты `Update` (таблица по клавишам): `ctrl+c` → `SIGINT`, без команды; Ctrl+С на кириллице с `BaseCode: 'c'` → `SIGINT`; `ctrl+\` → команда `tea.Quit`, сигнала пока нет, `exitSig` = `SIGQUIT`; `ctrl+z` при `jobControl` true → `tea.SuspendMsg`; при false → нет команды; обычная клавиша и Enter → ничего
- [x] тест: `Start` с `(*os.File)(nil)` рисует панель (расширить `TestStartPanel`)
- [x] тесты на псевдотерминале: во время работы `ECHO` и `ISIG` сняты; после `stop` и после `kill` termios как до запуска; байт 0x03 в мастер → подставная функция получила `SIGINT`; байт 0x1c → панель вышла, `SIGQUIT` получен после восстановления termios. **Байт 0x1a не писать**
- [x] `go.mod`: `golang.org/x/sys` без `// indirect` (правкой или `docker compose run --rm --no-deps app sh -c "go mod tidy && go mod vendor"`); `git diff vendor/` — пусто
- [x] `make test`, `make lint` — должно пройти; по `-v` убедиться, что тесты на псевдотерминале выполнились
- ➕ `main.go`: вызов `panel.Start(status, os.Stdout, nil, nil)` временно, чтобы собирался пакет `main`; ввод и `sendSignal` — Task 2

### Task 2: `main` выбирает ввод для панели и пересылает сигналы

**Files:**
- Modify: `cmd/mongo-replicator/main.go`
- Modify: `cmd/mongo-replicator/main_test.go`
- ➕ Create: `cmd/mongo-replicator/tty_unix.go`, `tty_other.go` (`foreground` через `golang.org/x/sys/unix`; вынесено, чтобы не ломать сборку под Windows)

- [x] `panelInput(stdin, openTTY)` по Solution Overview, с проверкой группы переднего плана через подставляемую функцию
- [x] `run`: выбор ввода перед `withPanel`; `withPanel(opts, status, logFile, in)`; `sendSignal` — своему процессу; закрыть `/dev/tty` после `stopP`/`killP`
- [x] тесты `panelInput`: stdin-терминал (мастер `/dev/ptmx`) на переднем плане → он, `openTTY` не вызывается; stdin не терминал и `openTTY` успешен → его файл, закрывается функцией закрытия; `openTTY` с ошибкой → `nil`, закрытие безвредно; не на переднем плане → `nil`
- [x] `TestWithPanelRestoresConsoleLog` передаёт `in = nil`, смысл не меняется
- [x] `make test`, `make lint` — должно пройти

### Task 3: Verify acceptance criteria
- [x] набор текста и Enter не отображаются и не сбивают панель — автоматически: raw-режим во время панели (тест Task 1); вручную — Post-Completion
- [x] Ctrl+C останавливает утилиту прежним путём — автоматически: байт 0x03 → `SIGINT` (тест Task 1); остановка с итогом и снятием аренды — Post-Completion
- [x] Ctrl+\ — терминал восстановлен до `SIGQUIT` (тест Task 1)
- [x] после выхода (`stop` и `kill`) терминал восстановлен (тест Task 1)
- [x] `--progress plain`, запуск без терминала и с `in = nil` работают как раньше
- [x] run full test suite: `make test`
- [x] `make test-integration` (пакет `main` с тегом `integration` собирается и проходит)
- [x] `make lint` без замечаний

### Task 4: [Final] Update documentation
- [x] README, раздел «Панель статуса»: заменить абзац про чтение клавиатуры — по Technical Details (клавиши, `fg` и не `bg`, Ctrl+Z без управления заданиями, Ctrl+Z и аренда, `kill -INT/-QUIT` при зависании, `reset` после падения)
- [x] CLAUDE.md: заметка — панель в raw-режиме читает клавиатуру; `ISIG` выключен, поэтому Ctrl+C → `SIGINT` своему процессу, Ctrl+\ → `SIGQUIT` только после выхода панели, Ctrl+Z только при управлении заданиями; встроенный обработчик сигналов bubbletea выключен (останавливает `main`); ввод — `*os.File` (typed nil), `/dev/tty` через `os.OpenFile`, выбор ввода в `run`, тесты не пишут 0x1a и не открывают `/dev/tty`
- [x] move this plan to `docs/plans/completed/`

## Post-Completion
*Items requiring manual intervention or external systems - no checkboxes, informational only*

**Ручная проверка в настоящих терминалах** (бинарник на тестовых кластерах):
- набор текста и Enter во время панели — ничего не отображается, панель не ломается
- Ctrl+C — остановка с итоговым кадром и логом «остановлен по сигналу», шелл в норме, набранное не выполнилось; то же на русской раскладке в kitty-совместимом терминале (kitty, ghostty, wezterm, foot) и в tmux
- Ctrl+Z, затем `fg` — панель продолжает рисоваться; в `make run` (контейнер) Ctrl+Z игнорируется
- Ctrl+\ — дамп горутин в нормальном терминале, шелл в норме
- запуск с `< /dev/null` в интерактивном терминале — панель видна, ввод с `/dev/tty`; запуск с `&` — панель без чтения клавиатуры, процесс не останавливается
- немедленная перманентная ошибка (`ErrLeaseHeld`) по ssh — проверить, не остаются ли в шелле ответы терминала (`^[[?1u`); если остаются — принять как известное
- `--progress plain` — терминал не трогается
