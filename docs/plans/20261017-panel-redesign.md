# Редизайн панели статуса

## Overview
- проблема: панель — сплошные строки «подпись · число · подпись · число»; подписи тусклые, числа обычные, ничего не выровнено — чтобы найти нужное число, надо вчитываться
- решение «C+D» (согласовано в brainstorm): колонка подписей слева и числа в одной колонке (C) плюс динамика (D) — графики за минуту в своих рамках и своим цветом, полоса прогресса **только во время догона**; блоки «репликация» и «трафик» разделены заголовками с линией; значок состояния в шапке; ошибка — отдельной строкой
- при запуске в режиме панели экран очищается до первой строки лога (по умолчанию; отключается флагом `--no-clear` / `NO_CLEAR=true`): панель и строки запуска начинаются с чистого окна, а не под остатками прошлых команд
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
- `DDL` — после первого DDL; `машина` — после первой сводки (значение — `процессов N`, дальше дополнительная часть): «всего ↓» — `SrcIn`, «↑» — `DstOut` машины (как у строк источника и цели); время сводки — `на HH:MM`
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

### Очистка экрана перед стартом
- флаг `--no-clear` (env `NO_CLEAR`, bool, по умолчанию выключен — экран **очищается**): у bool-флага go-flags значение по умолчанию `true` из командной строки не выключить, поэтому флаг отрицательный
- семантика go-flags (проверено ревью по вендору): `NO_CLEAR=true`/`1` → true, `false`/`0` → false, **пустое `NO_CLEAR=` → true**, `NO_CLEAR=yes` → ошибка разбора (код 2); заданный в env `NO_CLEAR=true` из командной строки не снять (`--no-clear=false` — ошибка)
- **не `tea.ClearScreen`**: без полноэкранного режима он перерисовывает только область самой панели (относительные перемещения курсора от её первой строки; `cursed_renderer.go:698-706`, `ultraviolet/terminal_renderer.go`), то, что выше панели, остаётся — фича ничего бы не меняла. Очистка — прямая запись `"\x1b[H\x1b[2J"` (курсор в начало + стереть экран) в stdout **до** старта bubbletea; после неё панель стартует в левом верхнем углу, её относительная модель курсора и вставка строк лога над панелью работают как прежде
- место — `main`, сразу после успешного `parseOpts`, **до** `setupLog` и первой строки лога: если `progressMode(opts.Progress, stdoutTTY(), os.Getenv("TERM")) == progressTTY && !opts.NoClear` — `panel.ClearScreen(os.Stdout)`. Так все строки запуска (`revision`, WARN `разрешены drop`, WARN `пароль передан в --src/--dst`, запрос пароля, `файл состояния`, WARN `полный лог не пишется`) остаются на экране над панелью; стираются только прежние команды терминала. Сигнатура `panel.Start` и `withPanel` не меняются
- `panel.ClearScreen(w io.Writer)` — экспортируемая функция `internal/panel` (одна строка; используют `main` и демо); демо вызывает её перед `panel.Start` всегда (флагов нет, поведение по умолчанию)
- видимые строки до очистки: ED2 историю терминала не стирает, но попадут ли стёртые видимые строки в историю — зависит от терминала (tmux, VTE — да; xterm, urxvt — нет); полный лог — `--log-file`
- `--progress tty` при перенаправленном stdout — последовательность попадёт в файл, как и прочие управляющие последовательности панели; в `plain` экран не трогается

## What Goes Where
- **Implementation Steps**: история и догон, график и полоса, раскладка, ширина, модель, очистка экрана и `--no-clear`, сценарий демо, тесты, README, CLAUDE.md
- **Post-Completion**: ручная проверка в демо и на реальном запуске

## Implementation Steps

### Task 1: История и состояние догона

**Files:**
- Create: `internal/panel/history.go`
- Create: `internal/panel/history_test.go`

- [x] `series` (60 значений + `count`) по Technical Details
- [x] `catchUp`: `update`, процент, оценка до нуля — по Technical Details (точки только при изменении лага, ≥ 2 точки, «лаг не снижается», ограничение 99 дней)
- [x] тестовый хелпер `seriesOf(vals ...float64)` в `history_test.go`
- [x] тесты `series`: пусто; меньше 60; ровно 60; больше 60 — старые вытесняются, порядок от старых к новым, `count` растёт
- [x] тесты `catchUp`: вход при 1 мин; лаг 30 с после входа — режим держится; выход при 9 с; повторный вход; рост лага поднимает `start`; неизвестный лаг не меняет режим; одинаковый лаг на тиках точку не добавляет; процент; оценка по двум изменениям лага (литерал); одна точка — без оценки; лаг не снижается; огромная оценка → «лаг не снижается»
- [x] `make test`, `make lint` — должно пройти

### Task 2: Перенос отрисовки, график и полоса

**Files:**
- Create: `internal/panel/render.go`
- Create: `internal/panel/render_test.go`
- Modify: `internal/panel/panel.go`, `internal/panel/panel_test.go`

- [x] перенести `renderPanel`, стили, `fmt*` из `panel.go` в `render.go` без изменений; `TestRenderPanel*`, `TestFmt*`, `panelLines`, `ansi`, `clock`, `KiB/MiB/GiB` — в `render_test.go`; `make test` зелёный до изменения логики
- [x] `sparkline`, `progressBar`, `pad`, усечение по ширине — по Technical Details
- [x] тесты `sparkline` (таблица по данным): пусто; все нули; меньше 60 точек — пробелы слева; 60 точек — 20 колонок по 3; неполная правая колонка; столбик максимума — `█`; сдвиг на одну точку не меняет уже полные колонки
- [x] тесты `progressBar`: 0, 0.58, 1, вне 0–1; `pad` со стилизованным и кириллическим текстом
- [x] `make test`, `make lint` — должно пройти
- ➕ усечение — `fit(s, n)` через `ansi.Truncate` (`github.com/charmbracelet/x/ansi` теперь прямая зависимость, `go mod tidy` + `go mod vendor` сделаны); импорт под псевдонимом `xansi`: имя `ansi` в пакете занято регуляркой тестов; для диапазона скорости — `sparkColumns` (средние колонок)

### Task 3: Новая раскладка `renderPanel` (широкое окно)

**Files:**
- Modify: `internal/panel/render.go`
- Modify: `internal/panel/render_test.go`

- [x] `view` (статус, время снимка, три `series`, `catchUp`) и `renderPanel(v view, width int)` из построчных функций — шапка, блоки, стили, значения в колонке по Technical Details
- [x] переписать `TestRenderPanel*` при ширине 100: работа (пример из Technical Details), запуск (лаг неизвестен, буферы пусты — `—`), догон (полоса, %, оценка), повтор при активном догоне (без полосы, `↻ повтор 2`, строка ошибки), остановлен с ошибкой и без
- [x] тесты цвета: значок «работает» — зелёный, «догоняет»/«повтор» — жёлтый, «остановлен» с ошибкой — красный; лаг по порогам (сравнение со `style.Render`)
- [x] тест: перевод строки в `LastErr` и `Details` не ломает раскладку
- [x] `make test`, `make lint` — должно пройти
- ➕ подгонка по ширине уже в `row`/`withExtra` (график — если помещается целиком, дополнительная часть — `fit` с колонки после значения или графика; шапка — после значка); Task 4 — тесты узких окон. `panelLines(v)` пока рисует при ширине 100 (unparam), для Task 4 вернуть параметр ширины; `sparkline(s)` без `cols` — всегда `sparkCols` (unparam)

### Task 4: Узкое окно

**Files:**
- Modify: `internal/panel/render.go`
- Modify: `internal/panel/render_test.go`

- [ ] правило ширины по Technical Details: фиксированная часть всегда, график/полоса если помещается, дополнительная часть усекается с `…` или пропадает (< 8), шапка усекается после значка
- [ ] строка `машина`: фиксированная часть — `процессов N`, дополнительная — `, активно M · на HH:MM` и «всего ↓ … ↑ …» (усекается тем же правилом)
- [ ] тесты при ширине 80, 60, 40: какая часть каждой строки осталась (литералы, включая `машина` при 40), числа на месте, ни одна строка не шире окна (`lipgloss.Width` после снятия ANSI)
- [ ] (сделано в Task 2) если нужен прямой импорт `github.com/charmbracelet/x/ansi` — `go mod tidy`, `go mod vendor` в контейнере, `go.mod`/`vendor/modules.txt` в коммит задачи
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

### Task 6: Очистка экрана перед стартом панели

**Files:**
- Create: `internal/panel/clear.go`, `internal/panel/clear_test.go`
- Modify: `cmd/mongo-replicator/options.go`, `cmd/mongo-replicator/options_test.go`
- Modify: `cmd/mongo-replicator/main.go`, `cmd/mongo-replicator/main_test.go`
- Modify: `cmd/panel-demo/main.go`

- [ ] `panel.ClearScreen(w io.Writer)` — пишет `"\x1b[H\x1b[2J"`; doc-комментарий: почему не `tea.ClearScreen` (без полноэкранного режима не очищает экран)
- [ ] флаг `NoClear bool` в `options`: `long:"no-clear" env:"NO_CLEAR"`, описание — «не очищать экран перед панелью статуса»; `"NO_CLEAR"` добавить в `clearEnv` (`options_test.go`)
- [ ] условие очистки — отдельной функцией (ориентир `clearBeforePanel(mode string, noClear bool) bool`: `mode == progressTTY && !noClear`); `main`: после успешного `parseOpts`, до `setupLog` — `if clearBeforePanel(progressMode(…), opts.NoClear) { panel.ClearScreen(os.Stdout) }`
- [ ] демо: `panel.ClearScreen(os.Stdout)` перед `panel.Start` (после проверки, что stdout — терминал)
- [ ] тест `ClearScreen`: в буфер записано ровно `"\x1b[H\x1b[2J"`
- [ ] тесты `parseOpts` (таблица по env/аргументам → `NoClear`): ничего → false; `--no-clear` → true; `NO_CLEAR=true` → true; `NO_CLEAR=false` → false; `NO_CLEAR=` → true; `NO_CLEAR=yes` → ошибка (в `TestParseOptsErrors` или отдельной таблицей)
- [ ] тест `clearBeforePanel` (таблица по данным): `tty`/false → true; `tty`/true → false; `plain`/false → false
- [ ] `make test`, `make lint` — должно пройти

### Task 7: Сценарий демо под новую панель

**Files:**
- Modify: `cmd/panel-demo/sim.go`
- Modify: `cmd/panel-demo/sim_test.go`
- Modify: `cmd/panel-demo/main.go` (doc-комментарий пакета: сценарий, «повтор на 40-й секунде»)

- [ ] медленный хвост догона, чтобы были видны полоса, оценка и выход ниже 10 с без мигания: `lag = max(28h − up·3360, (60s − up)·4)` при `up < 60s` (с 30 с лаг идёт от 2 мин к 0 за 30 с), скорость в хвосте — 300/с; после 60 с — ровный поток как сейчас
- [ ] повтор перенести на 100–110 с (после ухода высоких точек догона из окна 60 с, чтобы провал графиков был виден)
- [ ] обновить ожидания `TestSimStep` и тестов WARN/повтора под новые формулы (литералы); комментарии со сценарием в `sim.go` и doc-комментарий пакета в `main.go`
- [ ] `make test`, `make lint` — должно пройти

### Task 8: Verify acceptance criteria
- [ ] шапка, блоки с заголовками, колонка подписей, графики в рамках своим цветом, полоса только на догоне, ошибка отдельной строкой — по тестам Task 3–4
- [ ] экран очищается перед панелью по умолчанию, `--no-clear` отключает — по тестам Task 6 (`ClearScreen`, `clearBeforePanel`, `parseOpts`)
- [ ] логи над панелью не изменились (`teaWriter` → `tea.Println`, `TestTeaWriterAfterExit`)
- [ ] демо под псевдотерминалом рисует новую панель и по SIGINT выходит: `make panel-demo`, затем `script -qec "timeout --foreground -s INT 4 ./bin/mongo-replicator-panel" /dev/null`; в выводе — последовательность очистки `\x1b[2J` в начале, `── репликация`, `◐ догоняет`, «остановлен по сигналу» (байт 0x1a не отправлять)
- [ ] run full test suite: `make test`
- [ ] `make test-integration`
- [ ] `make lint` без замечаний

### Task 9: [Final] Update documentation
- [ ] README, таблица опций: `--no-clear` / `NO_CLEAR` — по умолчанию экран очищается перед панелью статуса (до строк запуска, они остаются видны); в `plain` не действует; с `--progress tty` в файл попадёт и последовательность очистки
- [ ] README, абзац о значениях env (около строки 40): пустое `NO_CLEAR=` — включено, неверное значение — код `2`, из командной строки не снять; таблица кодов выхода — неверный `NO_CLEAR` среди ошибок опций (код `2`)
- [ ] README, «Панель статуса»: строки запуска печатаются над панелью после очистки; стёртые прежние строки терминала могут не попасть в историю (зависит от терминала), полный лог — `--log-file`
- [ ] README, абзац о демо: новый сценарий (медленный хвост догона с 30 с, повтор на 100–110 с)
- [ ] README, раздел «Панель статуса»: новый пример панели (после догона и строка лага при догоне), значки и цвета, полоса только на догоне, графики за минуту, строка «машина», логи над панелью в истории терминала
- [ ] CLAUDE.md, «Панель и терминал»: история графиков и догона — в модели панели (`history.go`; точки догона — только при изменении лага), `renderPanel` чистая (статус, история, ширина; время снимка — от тика), колонки — `pad` по `lipgloss.Width` до стилизации, только базовые ANSI-цвета
- [ ] CLAUDE.md, правило раскладки тестов: `ansi` — в `log_test.go` и `render_test.go`
- [ ] CLAUDE.md, «Панель и терминал»: очистка экрана — прямая последовательность `\x1b[H\x1b[2J` в `main` до первой строки лога (`panel.ClearScreen`), не `tea.ClearScreen` — без полноэкранного режима тот перерисовывает только область панели
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
- при запуске экран очищается, строки запуска (`revision`, WARN, запрос пароля) видны над панелью; с `--no-clear` — всё под прежним выводом; в tmux и обычном эмуляторе
- реальный запуск на тестовых кластерах
