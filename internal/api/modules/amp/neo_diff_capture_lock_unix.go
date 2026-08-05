//go:build darwin || dragonfly || freebsd || illumos || linux || netbsd || openbsd || solaris

package amp

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

func neoOpenDiffCaptureLockFile(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
}

func neoTryLockDiffCaptureFile(file *os.File) (bool, error) {
	err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
		return false, nil
	}
	return err == nil, err
}

func neoUnlockDiffCaptureFile(file *os.File) error {
	return unix.Flock(int(file.Fd()), unix.LOCK_UN)
}
