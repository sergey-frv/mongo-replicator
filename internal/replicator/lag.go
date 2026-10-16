package replicator

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// tokenTime достаёт время кластера из resume token. _data — hex KeyString:
// первый байт 0x82 (тип Timestamp), затем 8 байт big-endian: T и I.
// Формат внутренний, но стабилен с 4.2.
func tokenTime(token bson.Raw) (bson.Timestamp, error) {
	v, err := token.LookupErr("_data")
	if err != nil {
		return bson.Timestamp{}, fmt.Errorf("в токене нет _data: %w", err)
	}
	s, ok := v.StringValueOK()
	if !ok {
		return bson.Timestamp{}, errors.New("_data токена не строка")
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return bson.Timestamp{}, fmt.Errorf("_data токена не hex: %w", err)
	}
	if len(b) < 9 || b[0] != 0x82 {
		return bson.Timestamp{}, fmt.Errorf("неизвестный формат _data токена: %.20s", s)
	}
	return bson.Timestamp{T: binary.BigEndian.Uint32(b[1:5]), I: binary.BigEndian.Uint32(b[5:9])}, nil
}

// lagOf — лаг применённой позиции относительно текущего времени источника.
// Источник «позади» применённой позиции (округление до секунды) — лаг 0.
func lagOf(srcNow, applied bson.Timestamp) time.Duration {
	if srcNow.T <= applied.T {
		return 0
	}
	return time.Duration(srcNow.T-applied.T) * time.Second
}

// srcTime — текущее время кластера источника: operationTime ответа ping.
// ping не требует привилегий и работает с правами на одну базу.
func (r *Replicator) srcTime(ctx context.Context) (bson.Timestamp, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	raw, err := r.src.Database("admin").RunCommand(ctx, bson.D{{Key: "ping", Value: 1}}).Raw()
	if err != nil {
		return bson.Timestamp{}, err
	}
	t, i, ok := raw.Lookup("operationTime").TimestampOK()
	if !ok {
		return bson.Timestamp{}, errors.New("в ответе ping нет operationTime")
	}
	return bson.Timestamp{T: t, I: i}, nil
}
