package replicator

import (
	"errors"
	"fmt"
	"io/fs"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/sergey-frv/mongo-replicator/internal/statefile"
	"github.com/sergey-frv/mongo-replicator/internal/traffic"
)

// defaultStateEvery — интервал записи файла состояния писателем.
const defaultStateEvery = 10 * time.Second

// startState загружает накопленный трафик из файла состояния, сразу пишет файл
// и запускает писателя. Один раз за процесс: второй процесс на ту же базу аренда
// не пустит, а потеряв аренду, процесс завершается — пересчитывать базу незачем.
// Писатель не привязан к Run и пишет и в паузах повторов, пока аренда не устарела
// (checkLease): трафик растёт и тогда. С устаревшей арендой базу может забрать другой
// процесс с этой машины, и запись затёрла бы его файл; пропуск ничего не теряет —
// следующая запись после нового захвата содержит всё накопленное. Так же и загрузка
// с первой записью: аренда устарела (процесс стоял после захвата) — файл не трогается,
// загрузка — при следующем вызове.
func (r *Replicator) startState() {
	if r.cfg.StateFile == "" || r.stateLoaded || r.checkLease() != nil {
		return
	}
	r.stateLoaded = true
	base, ok := r.loadStateBase()
	if !ok {
		return
	}
	r.stateBase = base
	r.writeStateFile()
	stop, done := make(chan struct{}), make(chan struct{})
	r.stateStop, r.stateDone = stop, done
	go func() {
		defer close(done)
		t := time.NewTicker(r.stateEvery)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				if r.checkLease() == nil {
					r.writeStateFile()
				}
			}
		}
	}()
}

// loadStateBase читает накопленный трафик из файла состояния. Битый файл или файл
// другой пары баз или кластера переносится в сторону, счёт — с нуля. false — файл
// не прочитан или не перенесён: писать в него нельзя, иначе история затрётся.
func (r *Replicator) loadStateBase() (traffic.Info, bool) {
	path := r.cfg.StateFile
	s, err := statefile.Load(path)
	var reason, msg string
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return traffic.Info{}, true
	case errors.Is(err, statefile.ErrCorrupt):
		reason, msg = "corrupt", "файл состояния повреждён"
	case err != nil:
		log.Warn().Err(err).Str("file", path).Msg("файл состояния не прочитан: трафик в него не пишется")
		return traffic.Info{}, false
	case s.DB != r.cfg.DB || s.DstDB != r.cfg.DstDB || s.Cluster != r.cfg.Cluster:
		reason, msg = "mismatch", fmt.Sprintf("файл состояния записан для %q → %q (%s)", s.DB, s.DstDB, s.Cluster)
	default:
		return s.Traffic, true
	}
	to, mvErr := statefile.MoveAside(path, reason)
	if mvErr != nil {
		log.Warn().Err(mvErr).Str("file", path).Msg(msg + ", но не перенесён: трафик в него не пишется")
		return traffic.Info{}, false
	}
	log.Warn().Err(err).Str("file", path).Str("moved_to", to).Msg(msg + ": перенесён в сторону, счёт трафика — с нуля")
	return traffic.Info{}, true
}

// writeStateFile пишет в файл состояния накопленное: базу и счётчики процесса.
// Ошибка — WARN: файл только для наблюдения, репликация продолжается.
func (r *Replicator) writeStateFile() {
	s := statefile.State{
		DB: r.cfg.DB, DstDB: r.cfg.DstDB, Cluster: r.cfg.Cluster, Owner: r.owner,
		StartedAt: r.Status().Started.UTC(), UpdatedAt: time.Now().UTC(),
		Traffic: r.stateBase.Add(r.cfg.Traffic.Snapshot()),
	}
	if err := statefile.Write(r.cfg.StateFile, s); err != nil {
		log.Warn().Err(err).Msg("файл состояния не записан")
	}
}

// stopState останавливает писателя файла состояния; true — он был запущен.
func (r *Replicator) stopState() bool {
	if r.stateStop == nil {
		return false
	}
	close(r.stateStop)
	<-r.stateDone
	r.stateStop = nil
	return true
}
