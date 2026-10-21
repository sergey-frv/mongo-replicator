//go:build unix && !aix

package verify

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Вторая блокировка того же файла, пока держится первая, — ErrLocked; после
// освобождения — успех.
func TestLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.lock")
	unlock, err := lock(path)
	require.NoError(t, err)

	_, errHeld := lock(path)
	unlock()
	unlockAgain, errFree := lock(path)

	assert.ErrorIs(t, errHeld, ErrLocked)
	require.NoError(t, errFree)
	unlockAgain()
}
