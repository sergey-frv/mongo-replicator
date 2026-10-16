package replicator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func TestBatchOrder(t *testing.T) {
	var b batch
	a, c := namespace{"d", "a"}, namespace{"d", "c"}
	m1 := mongo.NewDeleteOneModel().SetFilter(bson.D{{Key: "_id", Value: 1}})
	m2 := mongo.NewDeleteOneModel().SetFilter(bson.D{{Key: "_id", Value: 2}})
	m3 := mongo.NewDeleteOneModel().SetFilter(bson.D{{Key: "_id", Value: 3}})

	b.add(a, "delete", m1)
	b.add(c, "delete", m2)
	b.add(a, "delete", m3)

	assert.Equal(t, 3, b.n, "операций в пачке")
	assert.Equal(t, []namespace{a, c}, b.order, "коллекции — в порядке первого появления")
	assert.Equal(t, map[namespace][]mongo.WriteModel{a: {m1, m3}, c: {m2}}, b.ops, "порядок операций внутри коллекции")
}

func TestNSName(t *testing.T) {
	ns := namespace{DB: "a", Coll: "c"}
	assert.Equal(t, "a.c", New(nil, nil, Config{DB: "a"}).nsName(ns))
	assert.Equal(t, "a.c (на цели b.c)", New(nil, nil, Config{DB: "a", DstDB: "b"}).nsName(ns))
}
