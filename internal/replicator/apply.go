package replicator

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// batch копит операции по коллекциям. Порядок внутри коллекции сохраняется,
// между коллекциями он не важен.
type batch struct {
	order  []namespace
	ops    map[namespace][]mongo.WriteModel
	n      int
	counts OpCounts // по видам событий: модели insert/update/replace одинаковые (replace-upsert)
}

// add добавляет операцию события op (insert, update, replace, delete).
func (b *batch) add(ns namespace, op string, m mongo.WriteModel) {
	if b.ops == nil {
		b.ops = make(map[namespace][]mongo.WriteModel)
	}
	if _, seen := b.ops[ns]; !seen {
		b.order = append(b.order, ns)
	}
	b.ops[ns] = append(b.ops[ns], m)
	b.n++
	switch op {
	case "insert":
		b.counts.Ins++
	case "update":
		b.counts.Upd++
	case "replace":
		b.counts.Rep++
	case "delete":
		b.counts.Del++
	}
}

// flush применяет накопленные операции на цели.
func (r *Replicator) flush(ctx context.Context) error {
	for _, ns := range r.pending.order {
		// перед каждой коллекцией: пачка по многим коллекциям пишется долго
		if err := r.checkLease(); err != nil {
			return err
		}
		_, err := r.dst.Database(r.cfg.DstDB).Collection(ns.Coll).
			BulkWrite(ctx, r.pending.ops[ns], options.BulkWrite().SetOrdered(true))
		if err != nil {
			return fmt.Errorf("bulk write в %s: %w", r.nsName(ns), err)
		}
	}
	r.applied.Add(int64(r.pending.n))
	if bi := batchInfo(&r.pending, time.Now()); bi != nil {
		r.update(func(s *Status) { s.LastBatch = bi })
	}
	r.pending = batch{}
	return nil
}

// dstNS — namespace цели для коллекции источника.
func (r *Replicator) dstNS(ns namespace) namespace {
	return namespace{DB: r.cfg.DstDB, Coll: ns.Coll}
}

// nsName — ns источника для ошибок, при другой базе цели — с ns цели.
func (r *Replicator) nsName(ns namespace) string {
	if r.cfg.DstDB == r.cfg.DB {
		return ns.String()
	}
	return ns.String() + " (на цели " + r.dstNS(ns).String() + ")"
}
