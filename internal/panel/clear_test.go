package panel

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClearScreen(t *testing.T) {
	var buf bytes.Buffer

	ClearScreen(&buf)

	assert.Equal(t, "\x1b[H\x1b[2J", buf.String())
}
