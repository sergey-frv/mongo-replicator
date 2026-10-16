package replicator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Поток не тащит updateDescription: с ним событие большого документа превышает 16 МБ.
func TestStreamPipeline(t *testing.T) {
	require.Len(t, streamPipeline, 2)
	assert.Equal(t, bson.D{{Key: "$match", Value: bson.D{{Key: "$or", Value: bson.A{
		bson.D{{Key: "ns.coll", Value: bson.D{{Key: "$ne", Value: "_replicator_checkpoint"}}}},
		bson.D{{Key: "operationType", Value: "rename"}},
	}}}}}, streamPipeline[0], "сначала фильтр служебной коллекции, rename проходит")
	assert.Equal(t, bson.D{{Key: "$unset", Value: "updateDescription"}}, streamPipeline[1])
}

func TestCheckNSAllowed(t *testing.T) {
	cfg := Config{DB: "a", DstDB: "b"}
	own := namespace{DB: "a", Coll: "c"}
	for _, c := range []struct {
		name string
		ev   event
	}{
		{"своя база", event{OperationType: "insert", NS: own}},
		{"rename внутри базы", event{OperationType: "rename", NS: own, To: &namespace{DB: "a", Coll: "d"}}},
		{"invalidate без ns", event{OperationType: "invalidate"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			err := checkNS(cfg, &c.ev)

			assert.NoError(t, err)
		})
	}
}

func TestCheckNSForeign(t *testing.T) {
	cfg := Config{DB: "a", DstDB: "b"}
	own, foreign := namespace{DB: "a", Coll: "c"}, namespace{DB: "x", Coll: "c"}
	for _, c := range []struct {
		name string
		ev   event
		want string // подстрока ошибки
	}{
		{"чужая ns", event{OperationType: "insert", NS: foreign}, "insert x.c"},
		{"чужая to", event{OperationType: "rename", NS: own, To: &foreign}, "rename a.c → x.c"},
		{"ns базы цели", event{OperationType: "insert", NS: namespace{DB: "b", Coll: "c"}}, "insert b.c"},
	} {
		t.Run(c.name, func(t *testing.T) {
			err := checkNS(cfg, &c.ev)

			require.ErrorIs(t, err, ErrForeignNamespace)
			assert.Contains(t, err.Error(), c.want)
		})
	}
}
