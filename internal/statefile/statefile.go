// Package statefile — файл состояния процесса на машине: путь, чтение и запись,
// перенос в сторону и сводка трафика по каталогу.
package statefile

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

	"github.com/sergey-frv/mongo-replicator/internal/traffic"
)

// ErrCorrupt — файл состояния не разбирается как JSON.
var ErrCorrupt = errors.New("файл состояния повреждён")

// DefaultActiveFor — файл, обновлённый не раньше этого срока, считается файлом
// работающего процесса (писатель replicator обновляет его раз в 10 с).
const DefaultActiveFor = 30 * time.Second

// State — содержимое файла состояния процесса на машине.
type State struct {
	DB        string       `json:"db"`
	DstDB     string       `json:"dst_db"`
	Cluster   string       `json:"cluster"`
	Owner     string       `json:"owner"`
	StartedAt time.Time    `json:"started_at"`
	UpdatedAt time.Time    `json:"updated_at"`
	Traffic   traffic.Info `json:"traffic"`
}

// Machine — сводка трафика по файлам состояния каталога.
type Machine struct {
	At        time.Time    // время сводки
	Processes int          // файлы состояния
	Active    int          // из них обновлённые не раньше activeFor (Summary)
	Traffic   traffic.Info // сумма накопленного в файлах
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

// Path — путь файла состояния процесса в каталоге dir и ключ кластера цели
// (Config.StateFile, Config.Cluster) по базе цели и URI цели как написан.
func Path(dir, dstDB, dstURI string) (path, cluster string) {
	cluster = clusterKey(dstURI)
	return filepath.Join(dir, stateFileName(dstDB, cluster)), cluster
}

// stateFileName — имя файла состояния процесса: "<dst-db>.<8 hex sha256(key)>.json".
func stateFileName(dstDB, key string) string {
	sum := sha256.Sum256([]byte(key))
	return dstDB + "." + hex.EncodeToString(sum[:4]) + ".json"
}

// Load читает файл состояния; отсутствие файла — ошибка fs.ErrNotExist.
func Load(path string) (State, error) {
	b, err := os.ReadFile(path) //nolint:gosec // путь из --state-dir
	if err != nil {
		return State{}, fmt.Errorf("чтение файла состояния: %w", err)
	}
	var s State
	if err := json.Unmarshal(b, &s); err != nil {
		return State{}, fmt.Errorf("%w: %s: %v", ErrCorrupt, path, err)
	}
	return s, nil
}

// Write заменяет файл состояния целиком: временный файл в том же каталоге,
// Sync, Rename, Sync каталога — после сбоя остаётся старое или новое содержимое.
func Write(path string, s State) error {
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

// MoveAside переименовывает файл в "<path>.<reason>-<UTC-время>" (corrupt,
// mismatch): накопленная история не стирается, а в сводку файл больше не входит.
// Имя занято (второй перенос за секунду) — ошибка: Rename заменил бы прежний
// перенос. Проверка и Rename не атомарны, но переносы одного файла разделены
// арендой: путь файла задают база и кластер цели.
func MoveAside(path, reason string) (string, error) {
	to := path + "." + reason + "-" + time.Now().UTC().Format("20060102T150405Z")
	if _, err := os.Lstat(to); !errors.Is(err, fs.ErrNotExist) {
		if err == nil {
			err = fmt.Errorf("%s: %w", to, fs.ErrExist)
		}
		return "", fmt.Errorf("файл состояния: %w", err)
	}
	if err := os.Rename(path, to); err != nil {
		return "", fmt.Errorf("файл состояния: %w", err)
	}
	return to, nil
}

// Summary суммирует трафик по файлам *.json каталога; нечитаемые файлы
// пропускаются с WARN, исчезнувшие после листинга — молча. Активный — обновлённый
// не раньше activeFor до now. Листинг — ReadDir, не Glob: "[", "*" и "?"
// в пути каталога Glob понял бы как шаблон.
func Summary(dir string, now time.Time, activeFor time.Duration) (Machine, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return Machine{}, fmt.Errorf("сводка трафика: %w", err)
	}
	mt := Machine{At: now}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		p := filepath.Join(dir, e.Name())
		s, err := Load(p)
		if errors.Is(err, fs.ErrNotExist) { // удалён или перенесён в сторону после листинга
			continue
		}
		if err != nil {
			log.Warn().Err(err).Str("file", p).Msg("файл состояния пропущен в сводке")
			continue
		}
		mt.Processes++
		if now.Sub(s.UpdatedAt) < activeFor {
			mt.Active++
		}
		mt.Traffic = mt.Traffic.Add(s.Traffic)
	}
	return mt, nil
}
