// mongo-cs-replicator — односторонняя репликация MongoDB через change streams.
//
// Назначение: догонять целевой кластер более старой версии (например, 5.0)
// до источника более новой (например, 8.0), когда oplog replay, mongorestore
// и mongosync неприменимы из-за разницы версий.
//
// Требования:
//   - источник: replica set, MongoDB 6.0+ (нужен showExpandedEvents для DDL);
//   - цель: на неё пишет только репликатор;
//   - на цели уже восстановлен полный дамп источника, а -start-ts указывает
//     на момент ДО начала этого дампа и ещё попадает в окно oplog источника.
//
// Запуск:
//
//	go mod init mongo-cs-replicator && go mod tidy && go build
//	./mongo-cs-replicator -src "mongodb://.../?replicaSet=rs8" \
//	    -dst "mongodb://.../?replicaSet=rs5" -start-ts 1791900000
//
// Гарантии: at-least-once + идемпотентные операции (replace-upsert и delete
// по _id). После сбоя поток продолжается с последнего сохранённого resume
// token; повторно применённые события дают то же итоговое состояние.
package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os/signal"
	"syscall"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

// Чекпойнт хранится на цели: он двигается только после успешной записи данных.
const (
	ckptDB   = "_replicator"
	ckptColl = "checkpoint"
	ckptID   = "main"
)

// Коды ошибок сервера.
const (
	codeNamespaceNotFound       = 26
	codeIndexNotFound           = 27
	codeNamespaceExists         = 48
	codeChangeStreamFatal       = 280
	codeChangeStreamHistoryLost = 286
)

type namespace struct {
	DB   string `bson:"db"`
	Coll string `bson:"coll"`
}

func (n namespace) String() string { return n.DB + "." + n.Coll }

// event — нужные нам поля события change stream.
// Документы держим как bson.Raw, чтобы передать на цель байт в байт,
// без потери типов и порядка полей.
type event struct {
	ID            bson.Raw       `bson:"_id"` // resume token
	OperationType string         `bson:"operationType"`
	ClusterTime   bson.Timestamp `bson:"clusterTime"`
	NS            namespace      `bson:"ns"`
	To            *namespace     `bson:"to"` // только для rename
	DocumentKey   bson.Raw       `bson:"documentKey"`
	FullDocument  bson.Raw       `bson:"fullDocument"`
	OpDesc        bson.Raw       `bson:"operationDescription"` // только для DDL
}

// toModel превращает DML-событие в идемпотентную операцию записи.
// ok=false — событие не DML либо его нужно пропустить.
func toModel(ev *event) (m mongo.WriteModel, ok bool) {
	// На шардированном источнике documentKey содержит ещё и ключ шардирования,
	// поэтому фильтруем строго по _id.
	filter := bson.D{{Key: "_id", Value: ev.DocumentKey.Lookup("_id")}}

	switch ev.OperationType {
	case "insert", "replace", "update":
		if len(ev.FullDocument) == 0 {
			// update + updateLookup: к моменту чтения документ на источнике уже
			// удалён. Пропускаем — следом в потоке придёт delete.
			return nil, false
		}
		return mongo.NewReplaceOneModel().
			SetFilter(filter).
			SetReplacement(ev.FullDocument).
			SetUpsert(true), true
	case "delete":
		return mongo.NewDeleteOneModel().SetFilter(filter), true
	}
	return nil, false
}

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

type replicator struct {
	src, dst  *mongo.Client
	startTS   uint32
	batchSize int

	pending batch
	saved   bson.Raw // последний записанный в чекпойнт токен
	applied int64
	lastTS  uint32 // clusterTime последнего прочитанного события
}

func (r *replicator) ckpt() *mongo.Collection {
	return r.dst.Database(ckptDB).Collection(ckptColl)
}

func (r *replicator) loadToken(ctx context.Context) (bson.Raw, error) {
	var doc struct {
		Token bson.Raw `bson:"token"`
	}
	err := r.ckpt().FindOne(ctx, bson.M{"_id": ckptID}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	return doc.Token, err
}

func (r *replicator) saveToken(ctx context.Context, token bson.Raw) error {
	if len(token) == 0 || bytes.Equal(token, r.saved) {
		return nil // нечего сохранять: поток не продвинулся
	}
	_, err := r.ckpt().ReplaceOne(ctx,
		bson.M{"_id": ckptID},
		bson.M{
			"_id":         ckptID,
			"token":       token,
			"clusterTime": bson.Timestamp{T: r.lastTS},
			"updatedAt":   time.Now().UTC(),
		},
		options.Replace().SetUpsert(true))
	if err == nil {
		r.saved = token
	}
	return err
}

// flush применяет накопленные операции на цели.
func (r *replicator) flush(ctx context.Context) error {
	for _, ns := range r.pending.order {
		_, err := r.dst.Database(ns.DB).Collection(ns.Coll).
			BulkWrite(ctx, r.pending.ops[ns], options.BulkWrite().SetOrdered(true))
		if err != nil {
			return fmt.Errorf("bulk write в %s: %w", ns, err)
		}
	}
	r.applied += int64(r.pending.n)
	r.pending = batch{}
	return nil
}

// applyDDL переносит изменения схемы. Ошибки «уже есть» / «уже нет»
// игнорируются: они означают повторное применение после рестарта.
func (r *replicator) applyDDL(ctx context.Context, ev *event) error {
	db := r.dst.Database(ev.NS.DB)
	var err error

	switch ev.OperationType {
	case "drop":
		err = db.Collection(ev.NS.Coll).Drop(ctx)

	case "dropDatabase":
		err = db.Drop(ctx)

	case "rename":
		if ev.To == nil {
			return fmt.Errorf("rename %s: в событии нет поля to", ev.NS)
		}
		err = r.dst.Database("admin").RunCommand(ctx, bson.D{
			{Key: "renameCollection", Value: ev.NS.String()},
			{Key: "to", Value: ev.To.String()},
			{Key: "dropTarget", Value: true},
		}).Err()

	case "create":
		// Опции (view, capped, validator, collation, timeseries) не переносим:
		// часть из них отличается между версиями. Создаём обычную коллекцию
		// и предупреждаем, если на источнике были нестандартные опции.
		if _, lookupErr := ev.OpDesc.LookupErr("viewOn"); lookupErr == nil {
			log.Printf("WARN: %s — это view, не создаётся автоматически: %s", ev.NS, ev.OpDesc)
			return nil
		}
		if elems, _ := ev.OpDesc.Elements(); len(elems) > 1 { // кроме idIndex
			log.Printf("WARN: %s создана без опций источника, проверьте вручную: %s", ev.NS, ev.OpDesc)
		}
		err = db.CreateCollection(ctx, ev.NS.Coll)

	case "createIndexes":
		err = db.RunCommand(ctx, bson.D{
			{Key: "createIndexes", Value: ev.NS.Coll},
			{Key: "indexes", Value: ev.OpDesc.Lookup("indexes")},
		}).Err()

	case "dropIndexes":
		specs, valuesErr := ev.OpDesc.Lookup("indexes").Array().Values()
		if valuesErr != nil {
			return fmt.Errorf("dropIndexes %s: %w", ev.NS, valuesErr)
		}
		for _, spec := range specs {
			name := spec.Document().Lookup("name").StringValue()
			err = db.RunCommand(ctx, bson.D{
				{Key: "dropIndexes", Value: ev.NS.Coll},
				{Key: "index", Value: name},
			}).Err()
			if err != nil && !hasCode(err, codeNamespaceNotFound, codeIndexNotFound) {
				break
			}
		}

	default:
		// modify (collMod), shardCollection, refineCollectionShardKey и т.п.
		log.Printf("WARN: событие %q для %s не поддерживается и пропущено: %s",
			ev.OperationType, ev.NS, ev.OpDesc)
		return nil
	}

	if hasCode(err, codeNamespaceNotFound, codeIndexNotFound, codeNamespaceExists) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("%s %s: %w", ev.OperationType, ev.NS, err)
	}
	log.Printf("DDL применён: %s %s", ev.OperationType, ev.NS)
	return nil
}

// run открывает поток и работает до ошибки или отмены контекста.
func (r *replicator) run(ctx context.Context) error {
	opts := options.ChangeStream().
		SetFullDocument(options.UpdateLookup).
		SetShowExpandedEvents(true).
		SetBatchSize(int32(r.batchSize)).
		SetMaxAwaitTime(time.Second)

	token, err := r.loadToken(ctx)
	switch {
	case err != nil:
		return fmt.Errorf("чтение чекпойнта: %w", err)
	case token != nil:
		opts.SetResumeAfter(token)
		log.Print("продолжаю с сохранённого resume token")
	case r.startTS > 0:
		opts.SetStartAtOperationTime(&bson.Timestamp{T: r.startTS})
		log.Printf("чекпойнта нет, стартую с %s", time.Unix(int64(r.startTS), 0).UTC().Format(time.RFC3339))
	default:
		return errors.New("чекпойнта нет и -start-ts не задан: неизвестно, откуда читать")
	}

	// Служебные базы не реплицируем.
	pipeline := mongo.Pipeline{{{Key: "$match", Value: bson.M{
		"ns.db": bson.M{"$nin": bson.A{"admin", "config", "local", ckptDB}},
	}}}}

	cs, err := r.src.Watch(ctx, pipeline, opts)
	if err != nil {
		return fmt.Errorf("открытие change stream: %w", err)
	}
	defer cs.Close(context.Background())

	var lastToken bson.Raw
	commit := func() error {
		if err := r.flush(ctx); err != nil {
			return err
		}
		return r.saveToken(ctx, lastToken)
	}

	lastLog := time.Now()
	for {
		if cs.TryNext(ctx) {
			var ev event
			if err := cs.Decode(&ev); err != nil {
				return fmt.Errorf("разбор события: %w", err)
			}
			r.lastTS = ev.ClusterTime.T

			switch ev.OperationType {
			case "insert", "replace", "update", "delete":
				if m, ok := toModel(&ev); ok {
					r.pending.add(ev.NS, m)
				}
				lastToken = ev.ID
				if r.pending.n >= r.batchSize {
					if err := commit(); err != nil {
						return err
					}
				}
			default:
				// DDL: сначала всё накопленное, затем само изменение схемы,
				// и сразу чекпойнт — повтор rename/drop после рестарта опасен.
				if err := commit(); err != nil {
					return err
				}
				if err := r.applyDDL(ctx, &ev); err != nil {
					return err
				}
				lastToken = ev.ID
				if err := r.saveToken(ctx, lastToken); err != nil {
					return err
				}
			}
		} else {
			if err := cs.Err(); err != nil {
				return err
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// Пачка с сервера вычитана. Токен потока двигается и на «пустых»
			// getMore, поэтому чекпойнт не устаревает, даже когда событий нет.
			if t := cs.ResumeToken(); t != nil {
				lastToken = t
			}
			if err := commit(); err != nil {
				return err
			}
		}

		if time.Since(lastLog) >= 10*time.Second {
			lag := time.Duration(0)
			if r.lastTS > 0 {
				lag = time.Since(time.Unix(int64(r.lastTS), 0)).Round(time.Second)
			}
			log.Printf("применено операций: %d, возраст последнего события: %s", r.applied, lag)
			lastLog = time.Now()
		}
	}
}

func hasCode(err error, codes ...int) bool {
	var se mongo.ServerError
	if !errors.As(err, &se) {
		return false
	}
	for _, c := range codes {
		if se.HasErrorCode(c) {
			return true
		}
	}
	return false
}

func main() {
	srcURI := flag.String("src", "", "URI источника (replica set, MongoDB 6.0+)")
	dstURI := flag.String("dst", "", "URI цели")
	startTS := flag.Uint("start-ts", 0, "unix-время в секундах, с которого читать поток при первом запуске (раньше начала полного дампа)")
	batchSize := flag.Int("batch", 1000, "максимум операций в одной пачке записи")
	flag.Parse()
	if *srcURI == "" || *dstURI == "" || *batchSize < 1 {
		flag.Usage()
		log.Fatal("нужны -src и -dst")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	src, err := mongo.Connect(options.Client().ApplyURI(*srcURI).SetAppName("cs-replicator"))
	if err != nil {
		log.Fatalf("источник: %v", err)
	}
	defer src.Disconnect(context.Background())

	// w:majority на цели: чекпойнт не должен обогнать данные при failover.
	dst, err := mongo.Connect(options.Client().ApplyURI(*dstURI).
		SetAppName("cs-replicator").
		SetWriteConcern(writeconcern.Majority()))
	if err != nil {
		log.Fatalf("цель: %v", err)
	}
	defer dst.Disconnect(context.Background())

	r := &replicator{src: src, dst: dst, startTS: uint32(*startTS), batchSize: *batchSize}

	for {
		err := r.run(ctx)
		if ctx.Err() != nil {
			log.Print("остановлен по сигналу; незаписанный хвост будет перечитан при следующем запуске")
			return
		}
		if hasCode(err, codeChangeStreamHistoryLost, codeChangeStreamFatal) {
			log.Fatalf("точка возобновления выпала из oplog источника — нужна новая полная заливка цели: %v", err)
		}
		log.Printf("ошибка: %v; повтор через 5 с", err)
		r.pending = batch{} // несохранённое перечитаем с чекпойнта
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}
}
