# Редизайн панели статуса

## Overview
- проблема: панель — сплошные строки «подпись · число · подпись · число»; подписи тусклые, числа обычные, ничего не выровнено — чтобы найти нужное число, надо вчитываться
- решение «C+D» (согласовано в brainstorm): колонка подписей слева и числа в одной колонке (C) плюс динамика (D) — графики за минуту в своих рамках и своим цветом, полоса прогресса **только во время догона**; блоки «репликация» и «трафик» разделены заголовками с линией; значок состояния в шапке; ошибка — отдельной строкой
- логи (WARN и выше) по-прежнему печатаются над панелью (`tea.Println`) и уходят в историю терминала — прокрутка терминалом или tmux; полноэкранного режима нет
- репликатор и `replicator.Status` не меняются: история для графиков и прогресса копится в модели панели
- обратная совместимость вида панели не нужна

## Context (from discovery)
- `internal/panel/panel.go` (333 строки): модель `panel` (`status`, `st`, `at`, `prev`, `prevAt`, клавиши), `snapshot()` ставит `at = time.Now()` (по `tickMsg` раз в секунду и в `newPanel`), `finalMsg` (итоговый кадр, `prevAt` сбрасывается), `View` → `renderPanel(st, at, prev, prevAt)`; стили `labelStyle` (Faint), `retryStyle`, `errStyle`; `fmtBatch`, `fmtBytes`, `fmtFloat`, `fmtCount`, `fmtDuration` (`0с`, `59с`, `1м0с`, `2ч14м`, `3д2ч` — формат не меняется); `Start`, `teaWriter`
- тесты `internal/panel/panel_test.go`: константы `KiB/MiB/GiB`, `clock(h, m, s)`, `panelLines` (снятие ANSI регуляркой `ansi`), `TestRenderPanel`, `TestRenderPanelMinimal`, `TestRenderPanelRetry`, `TestRenderPanelStopped`, `TestFmtBytes`, `TestFmtDuration`, `TestFmtCount`, `TestPanelUpdate`, `TestPanelKeys`, `TestPanelViewReportsAlternateKeys`, `TestTeaWriterAfterExit`, `TestStartPanel`; псевдотерминал — `pty_linux_test.go`
- `replicator.Status`:
  - `Lag` (-1 — неизвестен) и `Rate` меняются только в `logProgress` — не чаще раза в 10 с (`internal/replicator/report.go`)
  - `Applied` растёт пачками при `flush` (`apply.go`): при малом потоке разница за 1 с скачет между 0 и удвоенным значением
  - `SetRetry` лаг не трогает
- lipgloss v2: `Style.Render` всегда выдаёт ANSI (понижение профиля делает рендерер bubbletea при выводе) — цвета проверяемы в тестах; `lipgloss.Width` считает ширину отображения (кириллица и `▁` — по 1); `Style.Width(n)` переносит длинное на новую строку — для колонок не использовать
- bubbletea v2: `tea.WindowSizeMsg` приходит, если stdout — терминал (и без ввода); строки шире окна рендерер обрезает (`cursed_renderer.go:301`)
- `cmd/panel-demo/sim.go`: догон `28h − up·3360` (лаг падает ~56 мин в секунду, между 10 с и 1 мин не задерживается), повтор 40–50 с, тесты `TestSimStep` и др. в `sim_test.go`
- `CLAUDE.md`: раздел «Панель и терминал»; правило раскладки тестов упоминает `ansi` в `log_test.go` и `panel_test.go`
- в рабочей копии незакоммиченная правка `.gitignore` пользователя — не трогать и не коммитить

## Development Approach
- **testing approach**: Regular
- complete each task fully before moving to the next
- **CRITICAL: every task MUST include new/updated tests** for code changes in that task
- **CRITICAL: all tests must pass before starting next task** - no exceptions
- **CRITICAL: update this plan file when scope changes during implementation**
- тесты — по разделу «Тесты» `CLAUDE.md` (литералы, таблицы только для данных, без логики в теле); запуск только через `make test`, `make lint`, `docker compose run`; тесты не открывают `/dev/tty` и не пишут 0x1a в псевдотерминал
- `fmtDuration` и `TestFmtDuration` не меняются; все примеры и литералы — в его формате
- коммитить только файлы задачи (`git add <пути>`), не `-A`

## Testing Strategy
- всё новое — чистые функции без терминала и без `time.Now`: история, догон, график, полоса, подгонка по ширине, `renderPanel(v, width)`
- модель тестируется с явным временем тиков (`tickMsg(clock(…))`), `snapshot(at)` — время параметром
- отрисовка — текст после снятия ANSI (`panelLines`/`ansi`) построчно литералами
- цвета — сравнение с `style.Render(...)` тех же переменных стилей (как `TestRenderPanelRetry`), не с захардкоженными `\x1b[32m`
- данные для `series` в тестах — хелпер `seriesOf(vals ...float64)` в `history_test.go` (используется и `render_test.go`)
- ручная проверка вида — демо (`make panel-demo`), Post-Completion

## Progress Tracking
- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix
- update plan if implementation deviates from original scope
- keep plan in sync with actual work done

## Solution Overview
- раскладка файлов `internal/panel`:
  - `panel.go` — модель, `Update`/`View`, клавиши, `Start`, `teaWriter`
  - `history.go` (новый) — `series` и `catchUp`
  - `render.go` (новый) — `renderPanel` из построчных функций (`headLine`, `lagLine`, `rateLine`, `appliedLine`, `ddlLine`, `trafficLine`, `machineLine`, `errLine`), стили, `sparkline`, `progressBar`, `fit`/`pad`, `fmt*` (переезжают из `panel.go`)
  - тесты: `history_test.go`, `render_test.go` (сюда переезжают `TestRenderPanel*`, `TestFmt*`, `panelLines`, `ansi`, `clock`, `KiB/MiB/GiB` — их используют и тесты модели), `panel_test.go` — модель, клавиши, `Start`
- модель на `tickMsg`: `snapshot(time.Time(msg))`, точки в три буфера, обновление `catchUp`; ширина — из `tea.WindowSizeMsg`; `View` собирает `view` и вызывает чистую `renderPanel(v, width)`
- вид — только 16 базовых ANSI-цветов: оттенок задаёт тема терминала (тёмная, светлая, tmux, ssh); без цвета (`NO_COLOR`) блоки держатся на заголовках, рамках и колонках

## Technical Details

### Раскладка (широкое окно, после догона)
```
● работает  urlshrinker → urlshrinker_v2   с запуска 25м0с
── репликация ──────────────────────────────────────────────────────
  лаг        0с             применено до 19:37:18
  скорость   19/с           ▏▁▂▁▃▂▁▂▅▂▁▂▁▃▂▁▁▂▁▂▁▕ 5–25/с
  применено  1 727 851      пачка 19:37:18: 1 (ins 1) в PrivateUrls
  DDL        11:40:02       createIndexes urlshrinker.PublicUrls (slug_1)
── трафик ──────────────────────────────────────────────────────────
  источник   ↓ 4.1 KiB/с    ▏▂▂▃▂▂▂▄▂▂▃▂▂▂▃▂▂▂▃▂▂▕ всего ↓ 881 MiB ↑ 2.7 MiB
  цель       ↑ 3.8 KiB/с    ▏▁▁▂▁▁▁▃▁▁▂▁▁▁▂▁▁▁▂▁▁▕ всего ↑ 278 MiB ↓ 36.3 MiB
  машина     процессов 1, активно 1 · на 19:36  всего ↓ 881 MiB ↑ 277.9 MiB
```
- догон (полоса — только при `State == работает`, активном `catchUp` и `Lag ≥ 0`): `  лаг        1д4ч           ████████████░░░░░░░░ 58% · ~1м0с до 0` (или `· лаг не снижается`; без оценки — только `58%`)
- повтор или остановлен при активном догоне — только значение лага, без полосы и оценки (состояние `catchUp` сохраняется)
- лаг неизвестен — `  лаг        неизвестен`
- ошибка (`LastErr` не пусто) — последняя строка `ошибка: <LastErr>` красным
- `DDL` — после первого DDL; `машина` — после первой сводки: «всего ↓» — `SrcIn`, «↑» — `DstOut` машины (как у строк источника и цели); время сводки — `на HH:MM`
- переводы строк в `LastErr` и `DDLInfo.Details` заменяются пробелом
- колонки: отступ 2, подпись — 11 символов, значение — 15 (`↓ 1023.5 KiB/с` — 14), график с рамкой — 22, дальше — дополнительная часть
- шапка: значок и состояние, пара баз, «с запуска»:
  - `● работает` зелёный; `◐ догоняет` жёлтый (состояние «работает» и активен `catchUp`)
  - `◌ запуск` тусклый; `↻ повтор N` жёлтый
  - `✗ остановлен` красный при `LastErr`, иначе тусклый (`✗`, не `✖`: тот в части шрифтов двойной ширины)
- заголовки блоков: `── <имя> ` + `─` до ширины, тусклым (`Faint`; не `BrightBlack` — в части тем сливается с фоном)

### Стили
- значения — жирные; подписи, единицы, заголовки, рамки графиков — тусклые (`Faint`)
- лаг: < 10 с зелёный, < 1 мин жёлтый, иначе красный
- графики: скорость — голубой (`Cyan`), источник — зелёный, цель — фиолетовый (`Magenta`); полоса прогресса — жёлтый
- только базовые цвета `lipgloss` (ANSI 16), без RGB

### Значения в колонке
- `скорость` — `Status.Rate` (`fmtFloat` + `/с`); график строится по разнице `Applied`, число — из `Status.Rate` (не скачет между 0 и 2x)
- `источник ↓` / `цель ↑` — последняя точка буфера; буфер пуст (первый кадр, после `finalMsg` без новой точки) — `—`

### История (`history.go`)
- `series`: последние 60 значений `float64` и общий счётчик добавленных `count` (для привязки колонок графика); `push(v)`, `values()` — от старых к новым, длина ≤ 60
- точки — на каждом `tickMsg`, если предыдущий снимок есть и `dt = at − prevAt > 0`:
  - скорость операций: `(Applied − prevApplied) / dt`; `Applied < prevApplied` — 0
  - источник ↓: `(SrcIn − prevSrcIn) / dt`; цель ↑: `(DstOut − prevDstOut) / dt`; для `uint64` сначала сравнить `cur < prev` → 0, только потом вычитать
  - предыдущий снимок — `prevSt` (целиком `Status`) вместо `prev traffic.Info`
- первый тик после `newPanel` точки не добавляет: `prevAt` нулевое до первого тика (`newPanel` снимает статус, но время предыдущего не ставит)
- `finalMsg`: последний снимок (`time.Now()`) без точки, `prevAt` сбрасывается

### Догон (`catchUp`)
- `update(lag time.Duration, at time.Time)`; `lag < 0` (неизвестен) — без изменений
- вход: не активен и `lag ≥ 1 мин` → активен, `start = lag`, точки — `[(at, lag)]`
- активен: `start = max(start, lag)`; точка добавляется **только при изменении лага** относительно последней точки (лаг меняется раз в 10 с; точки каждую секунду дали бы «ступеньки» и прыгающую оценку); хранить точки за последние 30 с, но не меньше двух последних
- выход: активен и `lag < 10 с` → не активен, точки очищены; между 10 с и 1 мин режим не меняется (без мигания)
- процент: `1 − lag/start`, в пределах 0–100
- оценка до нуля — нужны ≥ 2 точки: по первой и последней точке окна `drop = (firstLag − lastLag) / (lastAt − firstAt)` (секунд лага за секунду); `drop ≤ 0` — «лаг не снижается»; иначе `lastLag / drop` в секундах, не больше 99 дней (иначе «лаг не снижается»), формат `~<fmtDuration> до 0`; меньше двух точек — без оценки

### График и полоса (`render.go`)
- `sparkline(s series, cols int)`: 20 колонок, колонка — 3 точки, группировка по **абсолютному** номеру точки (`(count − len + i) / 3`), чтобы график сдвигался, а не пересчитывался; правая колонка может быть неполной; значение колонки — среднее её точек
- уровни `▁▂▃▄▅▆▇█`: `idx = round(v / max × 7)`, `max` — максимум средних по колонкам; `max == 0` — `▁`; колонки без точек (слева, пока данных меньше 60) — пробелы
- рамка `▏…▕` тусклым; диапазон скорости — `мин–макс/с` по средним колонок (совпадает с высотой столбиков)
- `progressBar(frac float64, cols int)`: 20 колонок `█`/`░`, `frac` ограничен 0–1

### Ширина окна
- `m.width` — из `tea.WindowSizeMsg`; до первого сообщения — 80
- правило для каждой строки: фиксированная часть (отступ, подпись, значение) остаётся всегда; график или полоса — если помещается целиком; дополнительная часть (диапазон, «всего», «применено до», пачка, DDL-описание) усекается до остатка ширины с `…`, а при остатке меньше 8 колонок не выводится
- шапка: значок и состояние остаются; затем пара баз и «с запуска» — усекаются тем же правилом
- строка ошибки и заголовки блоков — по ширине
- реализация: `pad(s, n)` дополняет **текст до стилизации** по `lipgloss.Width` (не `fmt` `%-Ns` — считает руны ESC-последовательностей; не `Style.Width` — переносит); усечение — `ansi.Truncate` из `github.com/charmbracelet/x/ansi` (сейчас `// indirect` в `go.mod`; при прямом импорте — `go mod tidy` + `go mod vendor` в контейнере, `go.mod` в коммит) или `lipgloss.NewStyle().MaxWidth(n)`

## What Goes Where
- **Implementation Steps**: история и догон, график и полоса, раскладка, ширина, модель, сценарий демо, тесты, README, CLAUDE.md
- **Post-Completion**: ручная проверка в демо и на реальном запуске

## Implementation Steps

### Task 1: История и состояние догона

**Files:**
- Create: `internal/panel/history.go`
- Create: `internal/panel/history_test.go`

- [ ] `series` (60 значений + `count`) по Technical Details
- [ ] `catchUp`: `update`, процент, оценка до нуля — по Technical Details (точки только при изменении лага, ≥ 2 точки, «лаг не снижается», ограничение 99 дней)
- [ ] тестовый хелпер `seriesOf(vals ...float64)` в `history_test.go`
- [ ] тесты `series`: пусто; меньше 60; ровно 60; больше 60 — старые вытесняются, порядок от старых к новым, `count` растёт
- [ ] тесты `catchUp`: вход при 1 мин; лаг 30 с после входа — режим держится; выход при 9 с; повторный вход; рост лага поднимает `start`; неизвестный лаг не меняет режим; одинаковый лаг на тиках точку не добавляет; процент; оценка по двум изменениям лага (литерал); одна точка — без оценки; лаг не снижается; огромная оценка → «лаг не снижается»
- [ ] `make test`, `make lint` — должно пройти

### Task 2: Перенос отрисовки, график и полоса

**Files:**
- Create: `internal/panel/render.go`
- Create: `internal/panel/render_test.go`
- Modify: `internal/panel/panel.go`, `internal/panel/panel_test.go`

- [ ] перенести `renderPanel`, стили, `fmt*` из `panel.go` в `render.go` без изменений; `TestRenderPanel*`, `TestFmt*`, `panelLines`, `ansi`, `clock`, `KiB/MiB/GiB` — в `render_test.go`; `make test` зелёный до изменения логики
- [ ] `sparkline`, `progressBar`, `pad`, усечение по ширине — по Technical Details
- [ ] тесты `sparkline` (таблица по данным): пусто; все нули; меньше 60 точек — пробелы слева; 60 точек — 20 колонок по 3; неполная правая колонка; столбик максимума — `█`; сдвиг на одну точку не меняет уже полные колонки
- [ ] тесты `progressBar`: 0, 0.58, 1, вне 0–1; `pad` со стилизованным и кириллическим текстом
- [ ] `make test`, `make lint` — должно пройти

### Task 3: Новая раскладка `renderPanel` (широкое окно)

**Files:**
- Modify: `internal/panel/render.go`
- Modify: `internal/panel/render_test.go`

- [ ] `view` (статус, время снимка, три `series`, `catchUp`) и `renderPanel(v view, width int)` из построчных функций — шапка, блоки, стили, значения в колонке по Technical Details
- [ ] переписать `TestRenderPanel*` при ширине 100: работа (пример из Technical Details), запуск (лаг неизвестен, буферы пусты — `—`), догон (полоса, %, оценка), повтор при активном догоне (без полосы, `↻ повтор 2`, строка ошибки), остановлен с ошибкой и без
- [ ] тесты цвета: значок «работает» — зелёный, «догоняет»/«повтор» — жёлтый, «остановлен» с ошибкой — красный; лаг по порогам (сравнение со `style.Render`)
- [ ] тест: перевод строки в `LastErr` и `Details` не ломает раскладку
- [ ] `make test`, `make lint` — должно пройти

### Task 4: Узкое окно

**Files:**
- Modify: `internal/panel/render.go`
- Modify: `internal/panel/render_test.go`

- [ ] правило ширины по Technical Details: фиксированная часть всегда, график/полоса если помещается, дополнительная часть усекается с `…` или пропадает (< 8), шапка усекается после значка
- [ ] тесты при ширине 80, 60, 40: какая часть каждой строки осталась (литералы), числа на месте, ни одна строка не шире окна (`lipgloss.Width` после снятия ANSI)
- [ ] если нужен прямой импорт `github.com/charmbracelet/x/ansi` — `go mod tidy`, `go mod vendor` в контейнере, `go.mod`/`vendor/modules.txt` в коммит задачи
- [ ] `make test`, `make lint` — должно пройти

### Task 5: Модель панели — время тика, ширина, точки

**Files:**
- Modify: `internal/panel/panel.go`
- Modify: `internal/panel/panel_test.go`

- [ ] `snapshot(at time.Time)`: `tickMsg` передаёт `time.Time(msg)`, `finalMsg` — `time.Now()`; `prevSt` вместо `prev traffic.Info`; первый тик после `newPanel` без точки
- [ ] поля `width`, три `series`, `catchUp`; `tea.WindowSizeMsg` → `width`; тик — точки и `catchUp.update(st.Lag, at)`; `finalMsg` — без точки
- [ ] `View`: `renderPanel(view{…}, width)` (по умолчанию 80)
- [ ] тесты `Update` с явными временами (`tickMsg(clock(…))`): `WindowSizeMsg` запоминает ширину; первый тик точку не добавляет; два тика через 1 с — по одной точке с литеральными значениями; `finalMsg` точку не добавляет; лаг из статуса включает догон
- [ ] существующие `TestPanelUpdate`, `TestPanelKeys`, `TestStartPanel` — без изменения смысла (в `TestStartPanel` сравнивать фрагменты без серий пробелов: рендерер может их заменять)
- [ ] `make test`, `make lint` — должно пройти

### Task 6: Сценарий демо под новую панель

**Files:**
- Modify: `cmd/panel-demo/sim.go`
- Modify: `cmd/panel-demo/sim_test.go`

- [ ] медленный хвост догона, чтобы были видны полоса, оценка и выход ниже 10 с без мигания: `lag = max(28h − up·3360, (60s − up)·4)` при `up < 60s` (с 30 с лаг идёт от 2 мин к 0 за 30 с), скорость в хвосте — 300/с; после 60 с — ровный поток как сейчас
- [ ] повтор перенести на 100–110 с (после ухода высоких точек догона из окна 60 с, чтобы провал графиков был виден)
- [ ] обновить ожидания `TestSimStep` и тестов WARN/повтора под новые формулы (литералы)
- [ ] `make test`, `make lint` — должно пройти

### Task 7: Verify acceptance criteria
- [ ] шапка, блоки с заголовками, колонка подписей, графики в рамках своим цветом, полоса только на догоне, ошибка отдельной строкой — по тестам Task 3–4
- [ ] логи над панелью не изменились (`teaWriter` → `tea.Println`, `TestTeaWriterAfterExit`)
- [ ] демо под псевдотерминалом рисует новую панель и по SIGINT выходит: `make panel-demo`, затем `script -qec "timeout --foreground -s INT 4 ./bin/mongo-replicator-panel" /dev/null`; в выводе — `── репликация`, `◐ догоняет`, «остановлен по сигналу» (байт 0x1a не отправлять)
- [ ] run full test suite: `make test`
- [ ] `make test-integration`
- [ ] `make lint` без замечаний

### Task 8: [Final] Update documentation
- [ ] README, раздел «Панель статуса»: новый пример панели (после догона и строка лага при догоне), значки и цвета, полоса только на догоне, графики за минуту, строка «машина», логи над панелью в истории терминала
- [ ] CLAUDE.md, «Панель и терминал»: история графиков и догона — в модели панели (`history.go`; точки догона — только при изменении лага), `renderPanel` чистая (статус, история, ширина; время снимка — от тика), колонки — `pad` по `lipgloss.Width` до стилизации, только базовые ANSI-цвета
- [ ] CLAUDE.md, правило раскладки тестов: `ansi` — в `log_test.go` и `render_test.go`
- [ ] move this plan to `docs/plans/completed/`

## Post-Completion
*Items requiring manual intervention or external systems - no checkboxes, informational only*

**Ручная проверка** (`make panel-demo && ./bin/mongo-replicator-panel`):
- догон: полоса, %, оценка; с 30 с — медленный хвост, при лаге < 10 с полоса исчезает и не мигает
- графики различимы: разные цвета и рамки, не сливаются
- повтор (100–110 с): `↻ повтор`, красная строка ошибки, графики проседают
- узкое окно (60, 40 колонок) — пропадает второстепенное, числа видны
- светлая и тёмная темы терминала, tmux; `NO_COLOR=1` — блоки различимы без цвета
- логи (WARN) над панелью прокручиваются историей терминала
- реальный запуск на тестовых кластерах
