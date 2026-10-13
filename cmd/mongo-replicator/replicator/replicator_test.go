package replicator

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func mustMarshal(t *testing.T, v any) []byte {
	t.Helper()
	b, err := bson.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestDecodeAndModel(t *testing.T) {
	cases := []struct {
		name      string
		ev        bson.D
		wantOK    bool
		wantModel string
	}{
		{"insert", bson.D{
			{Key: "_id", Value: bson.D{{Key: "_data", Value: "8263"}}},
			{Key: "operationType", Value: "insert"},
			{Key: "clusterTime", Value: bson.Timestamp{T: 100, I: 2}},
			{Key: "ns", Value: bson.D{{Key: "db", Value: "shop"}, {Key: "coll", Value: "orders"}}},
			{Key: "documentKey", Value: bson.D{{Key: "_id", Value: int32(7)}}},
			{Key: "fullDocument", Value: bson.D{{Key: "_id", Value: int32(7)}, {Key: "z", Value: 1.5}, {Key: "a", Value: "x"}}},
		}, true, "replace"},
		{"update with deleted doc (fullDocument: null)", bson.D{
			{Key: "_id", Value: bson.D{{Key: "_data", Value: "8264"}}},
			{Key: "operationType", Value: "update"},
			{Key: "ns", Value: bson.D{{Key: "db", Value: "shop"}, {Key: "coll", Value: "orders"}}},
			{Key: "documentKey", Value: bson.D{{Key: "_id", Value: int32(7)}}},
			{Key: "fullDocument", Value: nil},
		}, false, ""},
		{"update without fullDocument field", bson.D{
			{Key: "_id", Value: bson.D{{Key: "_data", Value: "8265"}}},
			{Key: "operationType", Value: "update"},
			{Key: "ns", Value: bson.D{{Key: "db", Value: "shop"}, {Key: "coll", Value: "orders"}}},
			{Key: "documentKey", Value: bson.D{{Key: "_id", Value: int32(7)}}},
		}, false, ""},
		{"delete, sharded documentKey", bson.D{
			{Key: "_id", Value: bson.D{{Key: "_data", Value: "8266"}}},
			{Key: "operationType", Value: "delete"},
			{Key: "ns", Value: bson.D{{Key: "db", Value: "shop"}, {Key: "coll", Value: "orders"}}},
			{Key: "documentKey", Value: bson.D{{Key: "region", Value: "kg"}, {Key: "_id", Value: int32(7)}}},
		}, true, "delete"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var ev event
			if err := bson.Unmarshal(mustMarshal(t, c.ev), &ev); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if len(ev.ID) == 0 {
				t.Fatal("resume token not decoded")
			}
			m, ok := toModel(&ev)
			if ok != c.wantOK {
				t.Fatalf("ok=%v, want %v", ok, c.wantOK)
			}
			if !ok {
				return
			}
			wantFilter := mustMarshal(t, bson.D{{Key: "_id", Value: int32(7)}})
			switch mm := m.(type) {
			case *mongo.ReplaceOneModel:
				if c.wantModel != "replace" {
					t.Fatalf("got replace, want %s", c.wantModel)
				}
				if got := mustMarshal(t, mm.Filter); string(got) != string(wantFilter) {
					t.Fatalf("filter = %s", bson.Raw(got))
				}
				// документ должен дойти байт в байт (порядок полей и типы)
				want := mustMarshal(t, bson.D{{Key: "_id", Value: int32(7)}, {Key: "z", Value: 1.5}, {Key: "a", Value: "x"}})
				if string(mm.Replacement.(bson.Raw)) != string(want) {
					t.Fatalf("replacement = %s", mm.Replacement)
				}
				if mm.Upsert == nil || !*mm.Upsert {
					t.Fatal("upsert not set")
				}
			case *mongo.DeleteOneModel:
				if c.wantModel != "delete" {
					t.Fatalf("got delete, want %s", c.wantModel)
				}
				if got := mustMarshal(t, mm.Filter); string(got) != string(wantFilter) {
					t.Fatalf("filter = %s", bson.Raw(got))
				}
			default:
				t.Fatalf("unexpected model %T", m)
			}
		})
	}
}

// Декодированные bson.Raw не должны ссылаться на буфер события:
// драйвер переиспользует его при следующем getMore, а мы держим документы до flush.
func TestRawIsCopied(t *testing.T) {
	buf := mustMarshal(t, bson.D{
		{Key: "_id", Value: bson.D{{Key: "_data", Value: "8263"}}},
		{Key: "operationType", Value: "insert"},
		{Key: "documentKey", Value: bson.D{{Key: "_id", Value: int32(7)}}},
		{Key: "fullDocument", Value: bson.D{{Key: "_id", Value: int32(7)}, {Key: "a", Value: "x"}}},
	})
	var ev event
	if err := bson.Unmarshal(buf, &ev); err != nil {
		t.Fatal(err)
	}
	before := string(ev.FullDocument) + string(ev.ID) + string(ev.DocumentKey)
	for i := range buf {
		buf[i] = 0xFF
	}
	if after := string(ev.FullDocument) + string(ev.ID) + string(ev.DocumentKey); after != before {
		t.Fatal("bson.Raw fields alias the source buffer")
	}
}

func TestBatchOrder(t *testing.T) {
	var b batch
	a, c := namespace{"d", "a"}, namespace{"d", "c"}
	m1, m2, m3 := mongo.NewDeleteOneModel(), mongo.NewDeleteOneModel(), mongo.NewDeleteOneModel()
	b.add(a, m1)
	b.add(c, m2)
	b.add(a, m3)
	if b.n != 3 || len(b.order) != 2 || b.order[0] != a || b.order[1] != c {
		t.Fatalf("order=%v n=%d", b.order, b.n)
	}
	if len(b.ops[a]) != 2 || b.ops[a][0] != mongo.WriteModel(m1) || b.ops[a][1] != mongo.WriteModel(m3) {
		t.Fatal("per-namespace order broken")
	}
}

func TestDDLDescriptionParsing(t *testing.T) {
	var ev event
	raw := mustMarshal(t, bson.D{
		{Key: "_id", Value: bson.D{{Key: "_data", Value: "8267"}}},
		{Key: "operationType", Value: "rename"},
		{Key: "ns", Value: bson.D{{Key: "db", Value: "shop"}, {Key: "coll", Value: "a"}}},
		{Key: "to", Value: bson.D{{Key: "db", Value: "shop"}, {Key: "coll", Value: "b"}}},
		{Key: "operationDescription", Value: bson.D{{Key: "indexes", Value: bson.A{
			bson.D{{Key: "v", Value: int32(2)}, {Key: "key", Value: bson.D{{Key: "x", Value: int32(1)}}}, {Key: "name", Value: "x_1"}},
		}}}},
	})
	if err := bson.Unmarshal(raw, &ev); err != nil {
		t.Fatal(err)
	}
	if ev.To == nil || ev.To.String() != "shop.b" || ev.NS.String() != "shop.a" {
		t.Fatalf("ns=%v to=%v", ev.NS, ev.To)
	}
	specs, err := ev.OpDesc.Lookup("indexes").Array().Values()
	if err != nil || len(specs) != 1 || specs[0].Document().Lookup("name").StringValue() != "x_1" {
		t.Fatalf("specs=%v err=%v", specs, err)
	}
	if _, err := ev.OpDesc.LookupErr("viewOn"); err == nil {
		t.Fatal("viewOn should be absent")
	}
}

func TestIsHistoryLost(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"history lost 286", mongo.CommandError{Code: codeChangeStreamHistoryLost}, true},
		{"change stream fatal 280", mongo.CommandError{Code: codeChangeStreamFatal}, true},
		{"wrapped 286", fmt.Errorf("run: %w", mongo.CommandError{Code: codeChangeStreamHistoryLost}), true},
		{"double wrapped 280", fmt.Errorf("a: %w", fmt.Errorf("b: %w", mongo.CommandError{Code: codeChangeStreamFatal})), true},
		{"namespace not found 26", mongo.CommandError{Code: codeNamespaceNotFound}, false},
		{"wrapped other code", fmt.Errorf("x: %w", mongo.CommandError{Code: 11000}), false},
		{"plain error", errors.New("boom"), false},
		{"nil", nil, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, IsHistoryLost(c.err))
		})
	}
}

func TestHasCode(t *testing.T) {
	cases := []struct {
		name  string
		err   error
		codes []int
		want  bool
	}{
		{"single match", mongo.CommandError{Code: 26}, []int{26}, true},
		{"one of many", mongo.CommandError{Code: 27}, []int{26, 27, 48}, true},
		{"wrapped match", fmt.Errorf("drop: %w", mongo.CommandError{Code: 48}), []int{26, 27, 48}, true},
		{"no match", mongo.CommandError{Code: 11000}, []int{26, 27, 48}, false},
		{"no codes", mongo.CommandError{Code: 26}, nil, false},
		{"write exception", mongo.WriteException{WriteErrors: []mongo.WriteError{{Code: 11000}}}, []int{11000}, true},
		{"plain error", errors.New("boom"), []int{26}, false},
		{"nil", nil, []int{26}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, hasCode(c.err, c.codes...))
		})
	}
}

func TestErrNoStartPoint(t *testing.T) {
	require.ErrorIs(t, fmt.Errorf("run: %w", ErrNoStartPoint), ErrNoStartPoint)
	assert.False(t, IsHistoryLost(ErrNoStartPoint))
}
