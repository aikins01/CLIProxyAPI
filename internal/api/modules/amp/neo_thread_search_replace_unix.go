//go:build !windows

package amp

import "os"

func replaceNeoLocalThreadSearchSidecar(source, destination string) error {
	return os.Rename(source, destination)
}
