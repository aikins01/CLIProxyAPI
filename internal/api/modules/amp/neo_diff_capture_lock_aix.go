//go:build aix

package amp

import (
	"errors"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

func neoOpenDiffCaptureLockFile(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
}

func neoTryLockDiffCaptureFile(file *os.File) (bool, error) {
	lock := unix.Flock_t{Type: unix.F_WRLCK, Whence: io.SeekStart}
	err := unix.FcntlFlock(file.Fd(), unix.F_SETLK, &lock)
	if errors.Is(err, unix.EACCES) || errors.Is(err, unix.EAGAIN) {
		return false, nil
	}
	return err == nil, err
}

func neoUnlockDiffCaptureFile(file *os.File) error {
	lock := unix.Flock_t{Type: unix.F_UNLCK, Whence: io.SeekStart}
	return unix.FcntlFlock(file.Fd(), unix.F_SETLK, &lock)
}
