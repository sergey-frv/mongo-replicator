package replicator

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
)

func TestDDLDescriptionParsing(t *testing.T) {
	raw := bson.D{
		{Key: "_id", Value: bson.D{{Key: "_data", Value: "8267"}}},
		{Key: "operationType", Value: "rename"},
		{Key: "ns", Value: bson.D{{Key: "db", Value: "shop"}, {Key: "coll", Value: "a"}}},
		{Key: "to", Value: bson.D{{Key: "db", Value: "shop"}, {Key: "coll", Value: "b"}}},
		{Key: "operationDescription", Value: bson.D{{Key: "indexes", Value: bson.A{
			bson.D{{Key: "v", Value: int32(2)}, {Key: "key", Value: bson.D{{Key: "x", Value: int32(1)}}}, {Key: "name", Value: "x_1"}},
		}}}},
	}

	ev := decodeEvent(t, raw)

	assert.Equal(t, "shop.a", ev.NS.String(), "ns")
	require.NotNil(t, ev.To, "to не разобран")
	assert.Equal(t, "shop.b", ev.To.String(), "to")
	specs, err := ev.OpDesc.Lookup("indexes").Array().Values()
	require.NoError(t, err, "indexes")
	require.Len(t, specs, 1, "indexes")
	assert.Equal(t, "x_1", specs[0].Document().Lookup("name").StringValue(), "имя индекса")
	_, err = ev.OpDesc.LookupErr("viewOn")
	assert.ErrorIs(t, err, bsoncore.ErrElementNotFound, "viewOn должен отсутствовать")
}

// rename в служебное имя и из него — перманентная ошибка ещё до проверки аренды
// (аренды здесь нет вовсе) и без похода на цель.
func TestApplyDDLRenameReserved(t *testing.T) {
	cases := []struct {
		name     string
		from, to namespace
	}{
		{"в служебное имя", namespace{"d", "foo"}, namespace{"d", ckptColl}},
		{"из служебного имени", namespace{"d", ckptColl}, namespace{"d", "foo"}},
		{"из другой базы в служебное", namespace{"other", "foo"}, namespace{"d", ckptColl}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := New(nil, nil, Config{DB: "d"})
			to := c.to
			ev := event{OperationType: "rename", NS: c.from, To: &to}

			err := r.applyDDL(context.Background(), &ev)

			require.ErrorIs(t, err, ErrReservedCollection)
			assert.NotErrorIs(t, err, errLeaseExpiring)
			assert.Nil(t, r.Status().LastDDL)
		})
	}
}

// Обычный rename доходит до проверки аренды (аренды нет — errLeaseExpiring).
func TestApplyDDLRenameChecksLease(t *testing.T) {
	r := New(nil, nil, Config{DB: "d"})
	ev := event{OperationType: "rename", NS: namespace{"d", "a"}, To: &namespace{"d", "b"}}

	err := r.applyDDL(context.Background(), &ev)

	require.ErrorIs(t, err, errLeaseExpiring, "обычный rename доходит до проверки аренды")
}

func TestIsTempColl(t *testing.T) {
	for name, want := range map[string]bool{
		"tmp.agg_out.0f1e2d3c-4b5a-4697-8a9b-0c1d2e3f4a5b":  true,
		"tmp.agg_out.0f1e2d3c":                              false,
		"tmp.agg_out.archive":                               false,
		"tmp.agg_out.0f1e2d3c-4b5a-4697-8a9b-0c1d2e3f4a5bx": false,
		"tmp.mr.x":                      false,
		"tmpAb12C.renameCollection":     true,
		"tmpAb12C.convertToCapped.c":    true,
		"tmpAb12C.convertToCapped.a\nb": true, // имя коллекции может содержать \n
		"tmp.backup":                    false,
		"tmp":                           false,
		"tmpAb12C.other":                false,
		"tmpAb12.renameCollection":      false,
		"tmpAb12C.renameCollectionX":    false,
		"tmpAb12C.convertToCapped":      false,
		"tmpAb12C.convertToCapped.":     false,
		"xtmpAb12C.renameCollection":    false,
		"tmp.agg_out":                   false,
		"orders":                        false,
	} {
		assert.Equal(t, want, isTempColl(name), name)
	}
}
