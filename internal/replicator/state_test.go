package replicator

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sergey-frv/mongo-replicator/internal/statefile"
	"github.com/sergey-frv/mongo-replicator/internal/traffic"
)

// newState — репликатор с файлом состояния в dir (без кластеров: startState их не трогает).
func newState(t *testing.T, dir string, tr *traffic.Counters) *Replicator {
	t.Helper()
	quietLog(t)
	path, cluster := statefile.Path(dir, "app", "mongodb://h1")
	r := New(nil, nil, Config{DB: "src", DstDB: "app", Traffic: tr, StateFile: path, StateDir: dir, Cluster: cluster})
	now := time.Now()
	r.leaseOK.Store(&now)
	t.Cleanup(func() { r.stopState() })
	return r
}

func TestStartStateNoFile(t *testing.T) {
	dir := t.TempDir()
	tr := &traffic.Counters{}
	tr.SrcIn.Store(5)
	r := newState(t, dir, tr)

	r.startState()
	s, err := statefile.Load(r.cfg.StateFile)

	require.NoError(t, err, "первая запись — сразу после загрузки")
	assert.Equal(t, statefile.State{DB: "src", DstDB: "app", Cluster: "h1:27017", Owner: r.owner,
		StartedAt: r.Status().Started.UTC(), UpdatedAt: s.UpdatedAt, Traffic: traffic.Info{SrcIn: 5}},
		s)
	assert.WithinDuration(t, time.Now(), s.UpdatedAt, 5*time.Second)
	assert.True(t, r.stopState(), "писатель запущен")
	assert.False(t, r.stopState(), "повторная остановка безвредна")
}

func TestStartStateContinues(t *testing.T) {
	dir := t.TempDir()
	tr := &traffic.Counters{}
	r := newState(t, dir, tr)
	require.NoError(t, statefile.Write(r.cfg.StateFile, statefile.State{DB: "src", DstDB: "app", Cluster: "h1:27017",
		Owner: "old:1:00000000", Traffic: traffic.Info{SrcIn: 100, SrcOut: 200, DstIn: 300, DstOut: 400}}))

	r.startState()
	tr.DstOut.Store(7)
	r.stopState()
	r.writeStateFile()
	s, err := statefile.Load(r.cfg.StateFile)

	require.NoError(t, err)
	assert.Equal(t, traffic.Info{SrcIn: 100, SrcOut: 200, DstIn: 300, DstOut: 407}, s.Traffic, "счёт — от прошлого значения")
	assert.Equal(t, r.owner, s.Owner)
}

// Писатель обновляет файл сам, без Run, пока аренда свежая.
func TestStartStateWriter(t *testing.T) {
	tr := &traffic.Counters{}
	r := newState(t, t.TempDir(), tr)
	r.stateEvery = 10 * time.Millisecond
	path := r.cfg.StateFile

	r.startState()
	tr.SrcIn.Store(42)

	require.Eventually(t, func() bool {
		s, err := statefile.Load(path)
		return err == nil && s.Traffic.SrcIn == 42
	}, 5*time.Second, 10*time.Millisecond, "писатель записал счёт в файл")
}

// С устаревшей арендой писатель файл не трогает: базу мог забрать другой процесс
// с этой машины и писать в тот же файл.
func TestStartStateWriterStaleLease(t *testing.T) {
	tr := &traffic.Counters{}
	r := newState(t, t.TempDir(), tr)
	r.stateEvery = 10 * time.Millisecond
	path := r.cfg.StateFile
	r.startState()
	stale := time.Now().Add(-r.leaseTTL)
	r.leaseOK.Store(&stale)
	require.NoError(t, statefile.Write(path, statefile.State{DB: "src", DstDB: "app", Cluster: "h1:27017",
		Owner: "other:1:00000000", Traffic: traffic.Info{SrcIn: 1000}}))

	tr.SrcIn.Store(42)

	assert.Never(t, func() bool {
		s, err := statefile.Load(path)
		return err != nil || s.Owner != "other:1:00000000"
	}, 100*time.Millisecond, 10*time.Millisecond, "файл другого процесса не затёрт")
	s, err := statefile.Load(path)
	require.NoError(t, err)
	assert.Equal(t, "other:1:00000000", s.Owner, "файл другого процесса не затёрт")
}

// Аренда устарела к загрузке (процесс стоял после захвата): файл не читается
// и не пишется, загрузка — при следующем вызове со свежей арендой.
func TestStartStateStaleLease(t *testing.T) {
	r := newState(t, t.TempDir(), &traffic.Counters{})
	other := statefile.State{DB: "src", DstDB: "app", Cluster: "h1:27017", Owner: "other:1:00000000",
		Traffic: traffic.Info{SrcIn: 1000}}
	require.NoError(t, statefile.Write(r.cfg.StateFile, other))
	stale := time.Now().Add(-r.leaseTTL)
	r.leaseOK.Store(&stale)

	r.startState()
	s, err := statefile.Load(r.cfg.StateFile)

	assert.Nil(t, r.stateStop, "писатель не запущен")
	require.NoError(t, err)
	assert.Equal(t, other.Owner, s.Owner, "файл другого процесса не затёрт")

	now := time.Now()
	r.leaseOK.Store(&now)

	r.startState()

	assert.NotNil(t, r.stateStop, "писатель запущен")
	assert.Equal(t, other.Traffic, r.stateBase)
}

// База загружается один раз за процесс: повторный Run файл не перечитывает.
func TestStartStateOnce(t *testing.T) {
	tr := &traffic.Counters{}
	r := newState(t, t.TempDir(), tr)
	r.startState()
	r.stopState()
	require.NoError(t, statefile.Write(r.cfg.StateFile, statefile.State{DB: "src", DstDB: "app", Cluster: "h1:27017",
		Traffic: traffic.Info{SrcIn: 1000}}))

	r.startState()

	assert.Nil(t, r.stateStop, "писатель не перезапущен")
	assert.Equal(t, traffic.Info{}, r.stateBase)
}

func TestStartStateMovesAside(t *testing.T) {
	for _, tc := range []struct {
		name, content, reason string
	}{
		{"битый", `{"traffic": `, "corrupt"},
		{"другая база источника", `{"db":"other","dst_db":"app","cluster":"h1:27017","traffic":{"src_in":9}}`, "mismatch"},
		{"другая база цели", `{"db":"src","dst_db":"other","cluster":"h1:27017","traffic":{"src_in":9}}`, "mismatch"},
		{"другой кластер", `{"db":"src","dst_db":"app","cluster":"h9:27017","traffic":{"src_in":9}}`, "mismatch"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			r := newState(t, dir, &traffic.Counters{})
			require.NoError(t, os.WriteFile(r.cfg.StateFile, []byte(tc.content), 0o600))

			r.startState()
			s, err := statefile.Load(r.cfg.StateFile)

			require.NoError(t, err)
			assert.Equal(t, traffic.Info{}, s.Traffic, "счёт с нуля")
			aside, err := filepath.Glob(r.cfg.StateFile + "." + tc.reason + "-*")
			require.NoError(t, err)
			require.Len(t, aside, 1, "старый файл перенесён в сторону")
			b, err := os.ReadFile(aside[0])
			require.NoError(t, err)
			assert.Equal(t, tc.content, string(b), "история не стёрта")
		})
	}
}

// Битый файл не перенесён в сторону (на месте имени переноса — каталог): писать
// в него нельзя — писатель не запускается, файл не тронут.
func TestStartStateMoveAsideFails(t *testing.T) {
	r := newState(t, t.TempDir(), &traffic.Counters{})
	const content = `{"traffic": `
	require.NoError(t, os.WriteFile(r.cfg.StateFile, []byte(content), 0o600))
	now := time.Now().UTC()
	for i := range 3 { // имя переноса — с точностью до секунды
		at := now.Add(time.Duration(i) * time.Second).Format("20060102T150405Z")
		require.NoError(t, os.Mkdir(r.cfg.StateFile+".corrupt-"+at, 0o750))
	}

	r.startState()

	assert.Nil(t, r.stateStop, "писатель не запущен")
	b, err := os.ReadFile(r.cfg.StateFile)
	require.NoError(t, err)
	assert.Equal(t, content, string(b), "файл не тронут")
}

// Файл не прочитан (не файл, а каталог): писать в него нельзя — писатель не запускается.
func TestStartStateUnreadable(t *testing.T) {
	r := newState(t, t.TempDir(), &traffic.Counters{})
	require.NoError(t, os.Mkdir(r.cfg.StateFile, 0o750))

	r.startState()

	assert.Nil(t, r.stateStop)
	assert.DirExists(t, r.cfg.StateFile)
}
