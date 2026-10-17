//go:build unix && !aix

package verify

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// lock берёт исключительную блокировку flock на файле path (создаётся) на всё
// время сверки; занята — ErrLocked. Блокировку держит открытый файл: она
// снимается вызовом unlock или при выходе процесса, в том числе kill -9. Файл
// не удаляется: удаление между open и flock второго процесса дало бы две
// блокировки на разных файлах.
func lock(path string) (unlock func(), err error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, filePerm) //nolint:gosec // путь из --state-dir
	if err != nil {
		return nil, fmt.Errorf("блокировка сверки: %w", err)
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil { //nolint:gosec // дескриптор файла помещается в int
		_ = f.Close()
		if errors.Is(err, unix.EWOULDBLOCK) {
			return nil, fmt.Errorf("%w: %s", ErrLocked, path)
		}
		return nil, fmt.Errorf("блокировка сверки: %w", err)
	}
	return func() { _ = f.Close() }, nil
}
