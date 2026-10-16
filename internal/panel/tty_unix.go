//go:build unix

package panel

import (
	"os"

	"golang.org/x/sys/unix"
)

// foreground — процесс в группе переднего плана терминала f. Из фона (запуск с &)
// включение raw-режима остановило бы процесс на SIGTTOU.
func foreground(f *os.File) bool {
	pgrp, err := unix.IoctlGetInt(int(f.Fd()), unix.TIOCGPGRP) //nolint:gosec // дескриптор терминала помещается в int
	return err == nil && pgrp == unix.Getpgrp()
}
