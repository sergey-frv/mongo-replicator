package panel

import (
	"os"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

// jobControl — есть ли у процесса управление заданиями: в задании шелла группа
// процесса своя, отличная от сессии. Без него (контейнер, ssh -t host cmd, script)
// группа совпадает с сессией и осиротела — ядро отбрасывает SIGTSTP. Отбрасывается он
// и при унаследованном SIG_IGN (у родителя trap "" TSTP).
func jobControl() bool {
	sid, err := unix.Getsid(0)
	if err != nil || unix.Getpgrp() == sid {
		return false
	}
	status, err := os.ReadFile("/proc/self/status")
	return err == nil && !sigIgnored(string(status), unix.SIGTSTP)
}

// sigIgnored — sig в маске SigIgn из /proc/<pid>/status; нет маски — считается
// игнорируемым. Не signal.Ignored: унаследованный SIG_IGN для SIGTSTP рантайм Go не
// запоминает, и signal.Ignored вернул бы false.
func sigIgnored(status string, sig unix.Signal) bool {
	for line := range strings.Lines(status) {
		if v, ok := strings.CutPrefix(line, "SigIgn:"); ok {
			mask, err := strconv.ParseUint(strings.TrimSpace(v), 16, 64)
			return err != nil || mask&(1<<(sig-1)) != 0
		}
	}
	return true
}
