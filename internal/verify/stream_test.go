package verify

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// unitVerifier — сверка базы db без кластеров: для onEvent и onStream.
func unitVerifier(cfg Config) *Verifier {
	cfg.DB, cfg.DstDB, cfg.Rate, cfg.Sample = "db", "db", 200, 1
	return New(nil, nil, cfg)
}

// dml — DML-событие ключа с _id int32 в db.coll в момент sec.
func dml(t *testing.T, coll string, id int32, sec uint32) *streamEvent {
	t.Helper()
	ev := &streamEvent{OperationType: "update", ClusterTime: at(sec), NS: namespace{DB: "db", Coll: coll}}
	ev.DocumentKey.ID = rawID(t, id)
	return ev
}

// В режиме --ids поток ставит в очередь только события ключей из файла.
func TestOnEventIDsFilter(t *testing.T) {
	v := unitVerifier(Config{IDs: "defects.json"})
	v.q.add("db.items", rawID(t, int32(1)), bson.Timestamp{})

	v.onEvent(dml(t, "items", 2, 10))
	v.onEvent(dml(t, "items", 1, 20))

	assert.Equal(t, []itemView{{id: 1, first: 0, last: 20}}, viewsOf(batch{items: v.q.snapshot()}))
}

// Коллекция, созданная после начала сверки, запоминается и переезжает с rename.
func TestOnEventCreatedRename(t *testing.T) {
	v := unitVerifier(Config{})

	v.onEvent(&streamEvent{OperationType: "create", NS: namespace{DB: "db", Coll: "log"}})
	v.onEvent(&streamEvent{OperationType: "rename", ClusterTime: at(10),
		NS: namespace{DB: "db", Coll: "log"}, To: &namespace{DB: "db", Coll: "log2"}})

	assert.Equal(t, map[string]bool{"log2": true}, v.created)
}

// rename в другую базу — коллекции здесь больше нет: ждущие ключи «не проверены».
func TestOnEventRenameOutOfDB(t *testing.T) {
	v := unitVerifier(Config{})
	v.onEvent(dml(t, "items", 1, 5))

	v.onEvent(&streamEvent{OperationType: "rename", ClusterTime: at(10),
		NS: namespace{DB: "db", Coll: "items"}, To: &namespace{DB: "other", Coll: "items"}})

	assert.Equal(t, 0, v.q.len())
	assert.Equal(t, counts{Unverified: 1}, v.counts)
	assert.Equal(t, reasonDrop, v.unverified[0].Reason)
}

// dropDatabase оканчивает поток: проверки больше не ждут его время.
func TestOnEventDropDatabase(t *testing.T) {
	v := unitVerifier(Config{})

	v.onEvent(&streamEvent{OperationType: "dropDatabase", ClusterTime: at(10), NS: namespace{DB: "db"}})

	assert.True(t, v.ended, "поток окончен")
	got, ok := v.stream.get()
	assert.True(t, ok, "отметка потока задана")
	assert.Equal(t, maxTS, got)
}

// С токена invalidate поток не продолжить: позиция остаётся на прежнем токене.
func TestOnStreamInvalidateKeepsToken(t *testing.T) {
	v := unitVerifier(Config{})
	before := rawDoc(t, bson.D{{Key: "_data", Value: "before"}})
	v.token = before

	err := v.onStream(streamMsg{
		ev:    &streamEvent{OperationType: "invalidate", ClusterTime: at(10)},
		token: rawDoc(t, bson.D{{Key: "_data", Value: "after"}}),
	})

	require.NoError(t, err)
	assert.Equal(t, before, v.token)
	assert.True(t, v.ended, "поток окончен")
}

// Выпадение из окна oplog (коды 286 и 280) — ErrHistoryLost с путём файла позиции.
func TestStreamErrHistoryLost(t *testing.T) {
	for name, code := range map[string]int32{"ChangeStreamHistoryLost": 286, "ChangeStreamFatalError": 280} {
		t.Run(name, func(t *testing.T) {
			v := unitVerifier(Config{})
			v.files.position = "/state/verify/db.json"

			err := v.streamErr(mongo.CommandError{Code: code})

			require.ErrorIs(t, err, ErrHistoryLost)
			assert.ErrorContains(t, err, "удалите /state/verify/db.json", "подсказка оператору")
		})
	}
}

func TestStreamErrOther(t *testing.T) {
	v := unitVerifier(Config{})

	err := v.streamErr(mongo.CommandError{Code: 6})

	assert.NotErrorIs(t, err, ErrHistoryLost)
}
