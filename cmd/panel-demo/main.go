// panel-demo — панель статуса mongo-replicator на мок-данных, без кластеров: запуск,
// догон с лагом больше суток, ровный поток, повтор на 40-й секунде, DDL и WARN над
// панелью. Панель, выбор ввода и клавиши — те же, что в mongo-replicator: Ctrl+C —
// остановка с итоговым кадром, Ctrl+Z — приостановка (fg — продолжить), Ctrl+\ —
// дамп горутин, прочие клавиши игнорируются.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"golang.org/x/term"

	"github.com/sergey-frv/mongo-replicator/internal/panel"
)

func main() {
	if !term.IsTerminal(int(os.Stdout.Fd())) { //nolint:gosec // дескриптор stdout помещается в int
		fmt.Fprintln(os.Stderr, "panel-demo: stdout не терминал — панель не нужна")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	demo := newSim(time.Now())
	in, closeIn := panel.Input(os.Stdin)
	console, stopP, killP := panel.Start(demo.status, os.Stdout, in, panel.SignalSelf)
	log.Logger = zerolog.New(zerolog.ConsoleWriter{Out: console, TimeFormat: "15:04:05"}).
		With().Timestamp().Logger()
	// паника в основной горутине не оставит терминал под панелью
	defer func() {
		if rec := recover(); rec != nil {
			killP()
			closeIn()
			panic(rec)
		}
	}()
	if in == nil {
		log.Warn().Msg("клавиатура не читается: нет терминала ввода на переднем плане")
	}

	t := time.NewTicker(200 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			demo.stopped()
			stopP()
			closeIn()
			fmt.Printf("остановлен по сигналу; применено %d (демо)\n", demo.status().Applied)
			return
		case now := <-t.C:
			// WARN — после step: step держит мьютекс, нужный панели
			for _, w := range demo.step(now) {
				log.Warn().Msg(w)
			}
		}
	}
}
