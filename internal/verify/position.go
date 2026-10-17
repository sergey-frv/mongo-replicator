package verify

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/sergey-frv/mongo-replicator/internal/statefile"
)

// ErrLocked — файл позиции занят другим процессом сверки.
var ErrLocked = errors.New("сверка этой базы уже запущена")

const (
	// reportLimit — предел записей в файле отчёта; счётчик в позиции — полный.
	reportLimit = 10_000
	// reportDocMax — предел расширенного JSON документа в отчёте, байт.
	reportDocMax = 4 << 10
)

// paths — файлы сверки одной базы цели в <state-dir>/verify/. Подкаталог, а не
// корень --state-dir: файл с именем файла состояния репликатор загрузил бы и
// перезаписал, а statefile.Summary посчитал бы процессом (каталоги он пропускает).
type paths struct {
	position   string // <имя>.json
	lock       string // <имя>.lock
	defects    string // <имя>.defects.json
	unverified string // <имя>.unverified.json
}

// positionPaths — файлы сверки базы dstDB цели dstURI и ключ кластера цели;
// каталог создаётся. Имя — statefile.Name без ".json".
func positionPaths(stateDir, dstDB, dstURI string) (paths, string, error) {
	dir := filepath.Join(stateDir, "verify")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return paths{}, "", fmt.Errorf("каталог сверки: %w", err)
	}
	name, cluster := statefile.Name(dstDB, dstURI)
	base := filepath.Join(dir, strings.TrimSuffix(name, ".json"))
	return paths{
		position:   base + ".json",
		lock:       base + ".lock",
		defects:    base + ".defects.json",
		unverified: base + ".unverified.json",
	}, cluster, nil
}

// ident — пара баз и кластеры, для которых записана позиция. Токен потока
// принадлежит источнику, поэтому важен и его кластер.
type ident struct {
	DB         string `json:"db"`
	DstDB      string `json:"dst_db"`
	Cluster    string `json:"cluster"`
	SrcCluster string `json:"src_cluster"`
}

// counts — счётчики сверки, накопленные по всем запускам.
type counts struct {
	Checked    int64 `json:"checked"`
	Matched    int64 `json:"matched"`
	Defects    int64 `json:"defects"`
	Unverified int64 `json:"unverified"`
	SampledOut int64 `json:"sampled_out"`
}

// position — состояние сверки для продолжения после перезапуска.
type position struct {
	ident
	Since      int64          // --since первого запуска, unix-сек
	Token      bson.Raw       // токен потока сверки; nil — поток не начат
	StreamTime bson.Timestamp // время последнего события потока
	Pending    []*item        // непроверенные ключи
	Created    []string       // коллекции, созданные в потоке (сверка схемы)
	Counts     counts
}

// jsonTS — bson.Timestamp в файле позиции.
type jsonTS struct {
	T uint32 `json:"t"`
	I uint32 `json:"i"`
}

func tsJSON(t bson.Timestamp) jsonTS { return jsonTS{T: t.T, I: t.I} }
func (t jsonTS) ts() bson.Timestamp  { return bson.Timestamp{T: t.T, I: t.I} }

// pendingJSON — непроверенный ключ в файле позиции.
type pendingJSON struct {
	NS    string          `json:"ns"`
	ID    json.RawMessage `json:"id"`
	First jsonTS          `json:"first"`
	Last  jsonTS          `json:"last"`
	Tries int             `json:"tries"`
}

// positionJSON — файл позиции.
type positionJSON struct {
	ident
	Since      int64           `json:"since"`
	Token      json.RawMessage `json:"token,omitempty"`
	StreamTime jsonTS          `json:"stream_time"`
	Pending    []pendingJSON   `json:"pending"`
	Created    []string        `json:"created,omitempty"`
	Counts     counts          `json:"counts"`
}

// valueJSON — значение в canonical extended JSON: тип сохраняется ($numberInt,
// $numberLong, $oid…), и после чтения valueFromJSON ключ совпадает побайтно.
func valueJSON(v bson.RawValue) (json.RawMessage, error) {
	b, err := bson.MarshalExtJSON(bson.D{{Key: "v", Value: v}}, true, false)
	if err != nil {
		return nil, err
	}
	var w struct {
		V json.RawMessage `json:"v"`
	}
	if err := json.Unmarshal(b, &w); err != nil {
		return nil, err
	}
	return w.V, nil
}

// valueFromJSON — значение из canonical extended JSON (valueJSON).
func valueFromJSON(m json.RawMessage) (bson.RawValue, error) {
	var doc bson.Raw
	if err := bson.UnmarshalExtJSON([]byte(`{"v":`+string(m)+`}`), true, &doc); err != nil {
		return bson.RawValue{}, err
	}
	return doc.LookupErr("v")
}

// writePosition заменяет файл позиции целиком (statefile.WriteJSON).
func writePosition(path string, p *position) error {
	f := positionJSON{
		ident: p.ident, Since: p.Since, StreamTime: tsJSON(p.StreamTime),
		Pending: make([]pendingJSON, 0, len(p.Pending)), Created: p.Created, Counts: p.Counts,
	}
	if p.Token != nil {
		tok, err := bson.MarshalExtJSON(p.Token, true, false)
		if err != nil {
			return fmt.Errorf("файл позиции сверки: токен: %w", err)
		}
		f.Token = tok
	}
	for _, it := range p.Pending {
		id, err := valueJSON(it.id)
		if err != nil {
			return fmt.Errorf("файл позиции сверки: _id: %w", err)
		}
		f.Pending = append(f.Pending, pendingJSON{
			NS: it.ns, ID: id, First: tsJSON(it.first), Last: tsJSON(it.last), Tries: it.tries,
		})
	}
	if err := statefile.WriteJSON(path, f); err != nil {
		return fmt.Errorf("файл позиции сверки: %w", err)
	}
	return nil
}

// parsePosition разбирает файл позиции; ошибка — файл повреждён.
func parsePosition(b []byte) (*position, error) {
	var f positionJSON
	if err := json.Unmarshal(b, &f); err != nil {
		return nil, err
	}
	p := &position{
		ident: f.ident, Since: f.Since, StreamTime: f.StreamTime.ts(),
		Pending: make([]*item, 0, len(f.Pending)), Created: f.Created, Counts: f.Counts,
	}
	if f.Token != nil {
		if err := bson.UnmarshalExtJSON(f.Token, true, &p.Token); err != nil {
			return nil, fmt.Errorf("токен: %w", err)
		}
	}
	for _, pj := range f.Pending {
		id, err := valueFromJSON(pj.ID)
		if err != nil {
			return nil, fmt.Errorf("_id ключа %s: %w", pj.NS, err)
		}
		p.Pending = append(p.Pending, &item{
			ns: pj.NS, id: id, first: pj.First.ts(), last: pj.Last.ts(), tries: pj.Tries,
		})
	}
	return p, nil
}

// loadPosition читает позицию сверки для want. nil без ошибки — позиции нет:
// файла нет, или он битый, или записан для другой пары баз или других
// кластеров — тогда он переносится в сторону (statefile.MoveAside) с WARN, и
// сверка начинается с --since. Не перенесённый файл — ошибка: следующая запись
// затёрла бы его.
func loadPosition(path string, want ident) (*position, error) {
	b, err := os.ReadFile(path) //nolint:gosec // путь из --state-dir
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("файл позиции сверки: %w", err)
	}
	p, err := parsePosition(b)
	var reason, msg string
	switch {
	case err != nil:
		reason, msg = "corrupt", "файл позиции сверки повреждён"
	case p.ident != want:
		reason, msg = "mismatch", fmt.Sprintf("файл позиции сверки записан для %q → %q (цель %s, источник %s)",
			p.DB, p.DstDB, p.Cluster, p.SrcCluster)
	default:
		return p, nil
	}
	to, mvErr := statefile.MoveAside(path, reason)
	if mvErr != nil {
		return nil, fmt.Errorf("%s, но не перенесён: %w", msg, mvErr)
	}
	log.Warn().Err(err).Str("file", path).Str("moved_to", to).Msg(msg + ": перенесён в сторону, сверка — с --since")
	return nil, nil
}

// reportEntry — запись файла отчёта (дефекты, «не проверено»). Документы — в
// canonical extended JSON (видна разница типов), обрезанные до reportDocMax;
// null — документа нет.
type reportEntry struct {
	NS     string          `json:"ns"`
	ID     json.RawMessage `json:"id"`
	Reason string          `json:"reason"`
	Src    *string         `json:"src"`
	Dst    *string         `json:"dst"`
}

// report — записи одного файла отчёта, не больше reportLimit.
type report []reportEntry

// add добавляет запись, если предел не достигнут.
func (r *report) add(ns string, id bson.RawValue, reason string, src, dst bson.Raw) error {
	if len(*r) >= reportLimit {
		return nil
	}
	idJSON, err := valueJSON(id)
	if err != nil {
		return fmt.Errorf("отчёт сверки: _id: %w", err)
	}
	e := reportEntry{NS: ns, ID: idJSON, Reason: reason}
	if e.Src, err = docJSON(src); err != nil {
		return fmt.Errorf("отчёт сверки: документ источника: %w", err)
	}
	if e.Dst, err = docJSON(dst); err != nil {
		return fmt.Errorf("отчёт сверки: документ цели: %w", err)
	}
	*r = append(*r, e)
	return nil
}

// docJSON — документ в canonical extended JSON, обрезанный до reportDocMax
// байт; nil — документа нет.
func docJSON(d bson.Raw) (*string, error) {
	if d == nil {
		return nil, nil
	}
	b, err := bson.MarshalExtJSON(d, true, false)
	if err != nil {
		return nil, err
	}
	s := truncate(string(b), reportDocMax)
	return &s, nil
}

// truncate обрезает s до n байт по границе руны, с «…» в конце.
func truncate(s string, n int) string {
	const more = "…"
	if len(s) <= n {
		return s
	}
	cut := n - len(more)
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + more
}

// writeReport заменяет файл отчёта целиком; пустой отчёт — "[]", чтобы не
// остался отчёт прошлого запуска.
func writeReport(path string, r report) error {
	if r == nil {
		r = report{}
	}
	if err := statefile.WriteJSON(path, r); err != nil {
		return fmt.Errorf("отчёт сверки: %w", err)
	}
	return nil
}

// readReport читает файл отчёта (для --ids).
func readReport(path string) (report, error) {
	b, err := os.ReadFile(path) //nolint:gosec // путь из --ids
	if err != nil {
		return nil, fmt.Errorf("отчёт сверки: %w", err)
	}
	var r report
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, fmt.Errorf("отчёт сверки %s: %w", path, err)
	}
	return r, nil
}

// id — _id записи отчёта.
func (e reportEntry) id() (bson.RawValue, error) {
	id, err := valueFromJSON(e.ID)
	if err != nil {
		return bson.RawValue{}, fmt.Errorf("отчёт сверки: _id ключа %s: %w", e.NS, err)
	}
	return id, nil
}
