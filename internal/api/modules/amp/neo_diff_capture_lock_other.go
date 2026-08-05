//go:build !aix && !darwin && !dragonfly && !freebsd && !illumos && !linux && !netbsd && !openbsd && !solaris && !windows

package amp

import (
	"errors"
	"os"
)

func neoOpenDiffCaptureLockFile(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
}

func neoTryLockDiffCaptureFile(*os.File) (bool, error) {
	return false, errors.New("diff capture file locking is unavailable")
}

func neoUnlockDiffCaptureFile(*os.File) error {
	return nil
}
