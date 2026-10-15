package replicator

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
)

// ErrStateCorrupt — файл состояния не разбирается как JSON.
var ErrStateCorrupt = errors.New("файл состояния повреждён")

// defaultStateEvery — интервал записи файла состояния писателем.
const defaultStateEvery = 10 * time.Second

// stateActiveFor — файл, обновлённый не раньше этого срока, считается файлом
// работающего процесса (писатель обновляет его раз в defaultStateEvery).
// Переменная: интеграционные тесты уменьшают срок.
var stateActiveFor = 30 * time.Second

// state — содержимое файла состояния процесса на машине.
type state struct {
	DB        string      `json:"db"`
	DstDB     string      `json:"dst_db"`
	Cluster   string      `json:"cluster"`
	Owner     string      `json:"owner"`
	StartedAt time.Time   `json:"started_at"`
	UpdatedAt time.Time   `json:"updated_at"`
	Traffic   TrafficInfo `json:"traffic"`
}

// MachineTraffic — сводка трафика по файлам состояния каталога.
type MachineTraffic struct {
	At        time.Time   // время сводки
	Processes int         // файлы состояния
	Active    int         // из них обновлённые не раньше stateActiveFor
	Traffic   TrafficInfo // сумма накопленного в файлах
}

// clusterKey — ключ кластера цели по URI как написан: разрешённые хосты не
// годятся — SRV с srvMaxHosts даёт случайное подмножество, а замена узла меняла
// бы ключ. mongodb+srv — "srv:<host>"; mongodb — хосты строки в нижнем регистре,
// с портом 27017 по умолчанию, отсортированные, через запятую. Разбор строковый:
// multi-host URI net/url может не разобрать.
func clusterKey(uri string) string {
	scheme, rest := "", uri
	if i := strings.Index(uri, "://"); i >= 0 {
		scheme, rest = strings.ToLower(uri[:i]), uri[i+len("://"):]
	}
	if i := strings.IndexByte(rest, '/'); i >= 0 { // в пароле "/" не бывает, "?" — бывает
		rest = rest[:i]
	}
	if i := strings.LastIndexByte(rest, '@'); i >= 0 {
		rest = rest[i+1:]
	}
	if i := strings.IndexByte(rest, '?'); i >= 0 {
		rest = rest[:i]
	}
	rest = strings.ToLower(rest)
	if scheme == "mongodb+srv" {
		return "srv:" + rest
	}
	var hosts []string
	for h := range strings.SplitSeq(rest, ",") {
		if h == "" {
			continue
		}
		if _, _, err := net.SplitHostPort(h); err != nil {
			h = net.JoinHostPort(strings.Trim(h, "[]"), "27017")
		}
		hosts = append(hosts, h)
	}
	slices.Sort(hosts)
	return strings.Join(hosts, ",")
}

// StatePath — путь файла состояния процесса в каталоге dir и ключ кластера цели
// (Config.StateFile, Config.Cluster) по базе цели и URI цели как написан.
func StatePath(dir, dstDB, dstURI string) (path, cluster string) {
	cluster = clusterKey(dstURI)
	return filepath.Join(dir, stateFileName(dstDB, cluster)), cluster
}

// stateFileName — имя файла состояния процесса: "<dst-db>.<8 hex sha256(key)>.json".
func stateFileName(dstDB, key string) string {
	sum := sha256.Sum256([]byte(key))
	return dstDB + "." + hex.EncodeToString(sum[:4]) + ".json"
}

// readState читает файл состояния; файла нет — нулевое состояние без ошибки.
func readState(path string) (state, error) {
	s, err := loadState(path)
	if errors.Is(err, fs.ErrNotExist) {
		return state{}, nil
	}
	return s, err
}

// loadState читает файл состояния; отсутствие файла — ошибка fs.ErrNotExist.
func loadState(path string) (state, error) {
	b, err := os.ReadFile(path) //nolint:gosec // путь из --state-dir
	if err != nil {
		return state{}, fmt.Errorf("чтение файла состояния: %w", err)
	}
	var s state
	if err := json.Unmarshal(b, &s); err != nil {
		return state{}, fmt.Errorf("%w: %s: %v", ErrStateCorrupt, path, err)
	}
	return s, nil
}

// writeState заменяет файл состояния целиком: временный файл в том же каталоге,
// Sync, Rename, Sync каталога — после сбоя остаётся старое или новое содержимое.
func writeState(path string, s state) error {
	b, err := json.Marshal(s)
	if err != nil {
		return fmt.Errorf("файл состояния: %w", err)
	}
	dir := filepath.Dir(path)
	tmp, err := writeTemp(dir, "."+filepath.Base(path)+".tmp-*", b)
	if err != nil {
		return fmt.Errorf("файл состояния: %w", err)
	}
	if err = os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("файл состояния: %w", err)
	}
	d, err := os.Open(dir) //nolint:gosec // каталог из --state-dir
	if err != nil {
		return fmt.Errorf("файл состояния: %w", err)
	}
	defer d.Close()
	if err = d.Sync(); err != nil {
		return fmt.Errorf("файл состояния: %w", err)
	}
	return nil
}

// writeTemp пишет b во временный файл каталога dir с Sync и возвращает его путь;
// при ошибке файл удаляется. Права 0640 (CreateTemp создаёт 0600): в общем
// --state-dir сводку пользователей одной группы собирают из чужих файлов.
func writeTemp(dir, pattern string, b []byte) (string, error) {
	f, err := os.CreateTemp(dir, pattern)
	if err != nil {
		return "", err
	}
	err = f.Chmod(0o640)
	if err == nil {
		_, err = f.Write(b)
	}
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = os.Remove(f.Name())
		return "", err
	}
	return f.Name(), nil
}

// moveAside переименовывает файл в "<path>.<reason>-<UTC-время>" (corrupt,
// mismatch): накопленная история не стирается, а в сводку файл больше не входит.
func moveAside(path, reason string) (string, error) {
	to := path + "." + reason + "-" + time.Now().UTC().Format("20060102T150405Z")
	if err := os.Rename(path, to); err != nil {
		return "", fmt.Errorf("файл состояния: %w", err)
	}
	return to, nil
}

// machineTraffic суммирует трафик по файлам *.json каталога; нечитаемые файлы
// пропускаются с WARN, исчезнувшие после листинга — молча. Активный — обновлённый
// не раньше stateActiveFor до now.
func machineTraffic(dir string, now time.Time) (MachineTraffic, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil {
		return MachineTraffic{}, fmt.Errorf("сводка трафика: %w", err)
	}
	mt := MachineTraffic{At: now}
	for _, p := range paths {
		s, err := loadState(p)
		if errors.Is(err, fs.ErrNotExist) { // удалён или перенесён в сторону после Glob
			continue
		}
		if err != nil {
			log.Warn().Err(err).Str("file", p).Msg("файл состояния пропущен в сводке")
			continue
		}
		mt.Processes++
		if now.Sub(s.UpdatedAt) < stateActiveFor {
			mt.Active++
		}
		mt.Traffic = mt.Traffic.add(s.Traffic)
	}
	return mt, nil
}

// startState загружает накопленный трафик из файла состояния, сразу пишет файл
// и запускает писателя. Один раз за процесс: второй процесс на ту же базу аренда
// не пустит, а потеряв аренду, процесс завершается — пересчитывать базу незачем.
// Писатель не привязан к Run и пишет и в паузах повторов: трафик растёт и тогда.
func (r *Replicator) startState() {
	if r.cfg.StateFile == "" || r.stateLoaded {
		return
	}
	r.stateLoaded = true
	base, ok := r.loadStateBase()
	if !ok {
		return
	}
	r.stateBase, r.stateStarted = base, r.Status().Started
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
				r.writeStateFile()
			}
		}
	}()
}

// loadStateBase читает накопленный трафик из файла состояния. Битый файл или файл
// другой пары баз или кластера переносится в сторону, счёт — с нуля. false — файл
// не прочитан или не перенесён: писать в него нельзя, иначе история затрётся.
func (r *Replicator) loadStateBase() (TrafficInfo, bool) {
	path := r.cfg.StateFile
	s, err := loadState(path)
	var reason, msg string
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return TrafficInfo{}, true
	case errors.Is(err, ErrStateCorrupt):
		reason, msg = "corrupt", "файл состояния повреждён"
	case err != nil:
		log.Warn().Err(err).Str("file", path).Msg("файл состояния не прочитан: трафик в него не пишется")
		return TrafficInfo{}, false
	case s.DB != r.cfg.DB || s.DstDB != r.cfg.DstDB || s.Cluster != r.cfg.Cluster:
		reason, msg = "mismatch", fmt.Sprintf("файл состояния записан для %q → %q (%s)", s.DB, s.DstDB, s.Cluster)
	default:
		return s.Traffic, true
	}
	to, mvErr := moveAside(path, reason)
	if mvErr != nil {
		log.Warn().Err(mvErr).Str("file", path).Msg(msg + ", но не перенесён: трафик в него не пишется")
		return TrafficInfo{}, false
	}
	log.Warn().Err(err).Str("file", path).Str("moved_to", to).Msg(msg + ": перенесён в сторону, счёт трафика — с нуля")
	return TrafficInfo{}, true
}

// writeStateFile пишет в файл состояния накопленное: базу и счётчики процесса.
// Ошибка — WARN: файл только для наблюдения, репликация продолжается.
func (r *Replicator) writeStateFile() {
	s := state{
		DB: r.cfg.DB, DstDB: r.cfg.DstDB, Cluster: r.cfg.Cluster, Owner: r.owner,
		StartedAt: r.stateStarted.UTC(), UpdatedAt: time.Now().UTC(),
		Traffic: r.stateBase.add(r.cfg.Traffic.Snapshot()),
	}
	if err := writeState(r.cfg.StateFile, s); err != nil {
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
