package replicator

import "go.mongodb.org/mongo-driver/v2/mongo"

// batch копит операции по коллекциям. Порядок внутри коллекции сохраняется,
// между коллекциями он не важен.
type batch struct {
	order []namespace
	ops   map[namespace][]mongo.WriteModel
	n     int
}

func (b *batch) add(ns namespace, m mongo.WriteModel) {
	if b.ops == nil {
		b.ops = make(map[namespace][]mongo.WriteModel)
	}
	if _, seen := b.ops[ns]; !seen {
		b.order = append(b.order, ns)
	}
	b.ops[ns] = append(b.ops[ns], m)
	b.n++
}
