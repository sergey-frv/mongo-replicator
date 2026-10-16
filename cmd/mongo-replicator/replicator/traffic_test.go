package replicator

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sergey-frv/mongo-replicator/internal/traffic"
)

func TestTrafficFields(t *testing.T) {
	var buf bytes.Buffer
	l := zerolog.New(&buf)
	prev := traffic.Info{SrcIn: 1000, SrcOut: 100, DstIn: 10, DstOut: 500}
	cur := traffic.Info{SrcIn: 21000, SrcOut: 200, DstIn: 30, DstOut: 10500}

	trafficFields(l.Info(), cur, prev, 10*time.Second).Send()

	var got map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &got))
	delete(got, "level")
	assert.Equal(t, map[string]any{
		"src_in": 21000.0, "src_out": 200.0, "dst_in": 30.0, "dst_out": 10500.0,
		"src_in_bps": 2000.0, "src_out_bps": 10.0, "dst_in_bps": 2.0, "dst_out_bps": 1000.0,
	}, got)
}
