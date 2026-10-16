package replicator

import (
	"context"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/sergey-frv/mongo-replicator/internal/traffic"
)

// defaultMachineEvery — интервал сводки трафика на машине.
const defaultMachineEvery = 60 * time.Second

// logMachine раз в machineEvery пишет сводку трафика по файлам состояния машины
// (Config.StateDir); первую — сразу после старта.
func (r *Replicator) logMachine(ctx context.Context) {
	if r.cfg.StateDir == "" || time.Since(r.lastMachine) < r.machineEvery || ctx.Err() != nil {
		return
	}
	r.lastMachine = time.Now()
	mt, err := machineTraffic(r.cfg.StateDir, r.lastMachine)
	if err != nil {
		log.Warn().Err(err).Msg("сводка трафика на машине не получена")
		return
	}
	r.update(func(s *Status) { s.Machine = &mt })
	log.Info().Int("processes", mt.Processes).Int("active", mt.Active).
		Uint64("src_in", mt.Traffic.SrcIn).Uint64("src_out", mt.Traffic.SrcOut).
		Uint64("dst_in", mt.Traffic.DstIn).Uint64("dst_out", mt.Traffic.DstOut).
		Msg("трафик на машине")
}

// trafficFields добавляет в запись лога счётчики с начала процесса и среднюю
// скорость (байт/с) за интервал elapsed (не нулевой) с прошлого снимка.
func trafficFields(e *zerolog.Event, cur, prev traffic.Info, elapsed time.Duration) *zerolog.Event {
	bps := func(cur, prev uint64) uint64 { return uint64(float64(cur-prev) / elapsed.Seconds()) }
	return e.Uint64("src_in", cur.SrcIn).Uint64("src_out", cur.SrcOut).
		Uint64("dst_in", cur.DstIn).Uint64("dst_out", cur.DstOut).
		Uint64("src_in_bps", bps(cur.SrcIn, prev.SrcIn)).Uint64("src_out_bps", bps(cur.SrcOut, prev.SrcOut)).
		Uint64("dst_in_bps", bps(cur.DstIn, prev.DstIn)).Uint64("dst_out_bps", bps(cur.DstOut, prev.DstOut))
}
