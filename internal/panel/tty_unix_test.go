//go:build unix

package panel

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Не терминал и терминал, управляющий не этим процессом (TIOCGPGRP — ошибка): не
// передний план, панель без клавиш.
func TestForegroundNotControllingTerminal(t *testing.T) {
	for name, f := range map[string]func(*testing.T) *os.File{
		"/dev/null":      openDevNull,
		"чужой терминал": openPTMX,
	} {
		t.Run(name, func(t *testing.T) {
			assert.False(t, foreground(f(t)))
		})
	}
}
