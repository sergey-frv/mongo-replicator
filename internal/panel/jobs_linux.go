package panel

import "golang.org/x/sys/unix"

// jobControl — есть ли у процесса управление заданиями: в задании шелла группа
// процесса своя, отличная от сессии. Без него (контейнер, ssh -t host cmd, script)
// группа совпадает с сессией и осиротела — ядро отбрасывает SIGTSTP.
func jobControl() bool {
	sid, err := unix.Getsid(0)
	return err == nil && unix.Getpgrp() != sid
}
