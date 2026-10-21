package verify

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// rawDoc кодирует документ; nil — «документа нет».
func rawDoc(t *testing.T, d bson.D) bson.Raw {
	t.Helper()
	if d == nil {
		return nil
	}
	b, err := bson.Marshal(d)
	require.NoError(t, err, "bson.Marshal")
	return b
}

func rawID(t *testing.T, v any) bson.RawValue {
	t.Helper()
	typ, b, err := bson.MarshalValue(v)
	require.NoError(t, err, "bson.MarshalValue")
	return bson.RawValue{Type: typ, Value: b}
}

// sampledShare — доля выбранных среди n синтетических _id (int32 от 0 до n-1).
func sampledShare(p float64, n int) float64 {
	k := 0
	for i := range n {
		id := bson.RawValue{Type: bson.TypeInt32, Value: binary.LittleEndian.AppendUint32(nil, uint32(i))}
		if sampled(id, p) {
			k++
		}
	}
	return float64(k) / float64(n)
}

func TestVerdict(t *testing.T) {
	a := bson.D{{Key: "_id", Value: 1}, {Key: "v", Value: "a"}}
	b := bson.D{{Key: "_id", Value: 1}, {Key: "v", Value: "b"}}
	c := bson.D{{Key: "_id", Value: 1}, {Key: "v", Value: "c"}}
	tests := []struct {
		name          string
		s1, d, s2     bson.D
		eventInWindow bool
		want          result
	}{
		{name: "все равны", s1: a, d: a, s2: a, want: matched},
		{name: "цель равна S2, источник менялся", s1: a, d: b, s2: b, want: matched},
		{name: "цель равна S1, источник менялся", s1: a, d: a, s2: b, want: matched},
		{name: "источник не менялся, цель другая", s1: a, d: b, s2: a, want: defect},
		{name: "ABA: событие в окне", s1: a, d: b, s2: a, eventInWindow: true, want: hot},
		{name: "все разные", s1: a, d: b, s2: c, want: hot},
		{name: "нет на цели", s1: a, s2: a, want: defect},
		{name: "лишний на цели", d: a, want: defect},
		{name: "нет нигде", want: matched},
		{name: "удалён на источнике во время проверки", s1: a, want: matched},
		{name: "появился на источнике во время проверки", s2: a, want: matched},
		{name: "удалён на источнике, на цели другой", s1: a, d: b, want: hot},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := verdict(rawDoc(t, tc.s1), rawDoc(t, tc.d), rawDoc(t, tc.s2), tc.eventInWindow)

			assert.Equal(t, tc.want, got)
		})
	}
}

// Ожидания для p < 1 посчитаны заранее по sha256 типа и значения _id: решение
// не меняется от запуска к запуску.
func TestSampled(t *testing.T) {
	tests := []struct {
		name string
		id   any
		p    float64
		want bool
	}{
		{name: "int32 1 вне половины", id: int32(1), p: 0.5, want: false},
		{name: "int64 1 в половине: тип важен", id: int64(1), p: 0.5, want: true},
		{name: "int32 4 в половине", id: int32(4), p: 0.5, want: true},
		{name: "строка a в половине", id: "a", p: 0.5, want: true},
		{name: "строка c вне половины", id: "c", p: 0.5, want: false},
		{name: "int32 3 вне 0.9", id: int32(3), p: 0.9, want: false},
		{name: "int32 3 при p=1", id: int32(3), p: 1, want: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := sampled(rawID(t, tc.id), tc.p)

			assert.Equal(t, tc.want, got)
		})
	}
}

func TestSampledShare(t *testing.T) {
	share := sampledShare(0.1, 10000)

	assert.InDelta(t, 0.1, share, 0.02, "доля выбранных _id при p=0.1")
}

func TestDiffFields(t *testing.T) {
	twelve := bson.D{
		{Key: "f01", Value: 1}, {Key: "f02", Value: 1}, {Key: "f03", Value: 1}, {Key: "f04", Value: 1},
		{Key: "f05", Value: 1}, {Key: "f06", Value: 1}, {Key: "f07", Value: 1}, {Key: "f08", Value: 1},
		{Key: "f09", Value: 1}, {Key: "f10", Value: 1}, {Key: "f11", Value: 1}, {Key: "f12", Value: 1},
	}
	tests := []struct {
		name     string
		src, dst bson.D
		want     []string
	}{
		{
			name: "равны",
			src:  bson.D{{Key: "_id", Value: 1}, {Key: "a", Value: 1}},
			dst:  bson.D{{Key: "_id", Value: 1}, {Key: "a", Value: 1}},
			want: nil,
		},
		{
			name: "различающиеся и отсутствующие в порядке src, затем лишние в порядке dst",
			src:  bson.D{{Key: "a", Value: 1}, {Key: "b", Value: 2}, {Key: "c", Value: 3}, {Key: "x", Value: 1}},
			dst:  bson.D{{Key: "z", Value: 1}, {Key: "c", Value: 4}, {Key: "a", Value: 1}, {Key: "b", Value: 2}, {Key: "y", Value: 2}},
			want: []string{"c", "x", "z", "y"},
		},
		{
			name: "то же число другого типа",
			src:  bson.D{{Key: "n", Value: int32(1)}},
			dst:  bson.D{{Key: "n", Value: int64(1)}},
			want: []string{"n"},
		},
		{
			name: "только порядок полей",
			src:  bson.D{{Key: "a", Value: 1}, {Key: "b", Value: 2}},
			dst:  bson.D{{Key: "b", Value: 2}, {Key: "a", Value: 1}},
			want: nil,
		},
		{
			name: "нет на цели",
			src:  bson.D{{Key: "_id", Value: 1}, {Key: "a", Value: 1}},
			want: []string{"_id", "a"},
		},
		{
			name: "лишний на цели",
			dst:  bson.D{{Key: "_id", Value: 1}, {Key: "a", Value: 1}},
			want: []string{"_id", "a"},
		},
		{
			name: "не больше 10",
			src:  twelve,
			dst:  bson.D{},
			want: []string{"f01", "f02", "f03", "f04", "f05", "f06", "f07", "f08", "f09", "f10"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := diffFields(rawDoc(t, tc.src), rawDoc(t, tc.dst))

			assert.Equal(t, tc.want, got)
		})
	}
}
