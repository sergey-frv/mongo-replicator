package replicator

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Токены сняты с mongo 8.0 (mongo-src из docker compose).
const (
	// токен события insert, clusterTime события {1791910133, 2}
	eventToken = "826ACE60F5000000022B042C0100296E5A10047AC85395739848878B1796C415A2B8B7463C6F7065726174696F6E54797065003C696E736572740046646F63756D656E744B657900461E5F6964002B02000004"
	// post-batch token пустого getMore потока базы после записи в другую базу,
	// operationTime той записи {1791910139, 2}
	postBatchToken = "826ACE60FB000000022B0429296E1404"
)

func TestTokenTime(t *testing.T) {
	for name, tc := range map[string]struct {
		data string
		want bson.Timestamp
	}{
		"событие":    {eventToken, bson.Timestamp{T: 1791910133, I: 2}},
		"post-batch": {postBatchToken, bson.Timestamp{T: 1791910139, I: 2}},
	} {
		t.Run(name, func(t *testing.T) {
			ts, err := TokenTime(mustMarshal(t, bson.D{{Key: "_data", Value: tc.data}}))

			require.NoError(t, err)
			assert.Equal(t, tc.want, ts)
		})
	}
}

func TestTokenTimeErrors(t *testing.T) {
	for name, tok := range map[string]bson.D{
		"нет _data":          {{Key: "x", Value: "826ACE60F500000002"}},
		"_data не строка":    {{Key: "_data", Value: int32(1)}},
		"не hex":             {{Key: "_data", Value: "82zz"}},
		"не Timestamp":       {{Key: "_data", Value: "836ACE60F500000002"}},
		"меньше 9 байт":      {{Key: "_data", Value: "826ACE60F5000000"}},
		"пустая строка":      {{Key: "_data", Value: ""}},
		"нечётная длина hex": {{Key: "_data", Value: "826ACE60F50000000"}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := TokenTime(mustMarshal(t, tok))

			assert.Error(t, err)
		})
	}
}

func TestCheckpointAppliedAt(t *testing.T) {
	stored := bson.Timestamp{T: 1000, I: 3}
	ck := checkpoint{Token: mustMarshal(t, bson.D{{Key: "_data", Value: eventToken}}), ClusterTime: stored}

	assert.Equal(t, bson.Timestamp{T: 1791910133, I: 2}, ck.appliedAt(), "время из токена")

	ck.Token = mustMarshal(t, bson.D{{Key: "_data", Value: "zz"}})
	assert.Equal(t, stored, ck.appliedAt(), "токен не разобран — clusterTime чекпойнта")
}

func TestLagOf(t *testing.T) {
	lag := lagOf(bson.Timestamp{T: 1065, I: 1}, bson.Timestamp{T: 1000, I: 7})

	assert.Equal(t, 65*time.Second, lag)
	assert.Equal(t, "1m5s", lag.String())

	assert.Zero(t, lagOf(bson.Timestamp{T: 1000, I: 1}, bson.Timestamp{T: 1000, I: 9}), "та же секунда")
	assert.Zero(t, lagOf(bson.Timestamp{T: 999, I: 1}, bson.Timestamp{T: 1000, I: 1}), "источник позади применённой позиции")
}
