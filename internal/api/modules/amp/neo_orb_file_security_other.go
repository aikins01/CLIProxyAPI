//go:build !aix && !darwin && !dragonfly && !freebsd && !illumos && !linux && !netbsd && !openbsd && !solaris

package amp

import (
	"errors"
	"os"
)

func readNeoWorkspaceImage(string, string) ([]byte, error) {
	return nil, errors.New("publish_image workspace reads are unsupported on this platform")
}

func neoOrbFileOwnedByCurrentUser(os.FileInfo) bool {
	return false
}

func neoOrbFileHasSingleLink(os.FileInfo) bool {
	return false
}
