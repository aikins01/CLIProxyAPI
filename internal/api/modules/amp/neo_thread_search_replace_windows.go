//go:build windows

package amp

import "golang.org/x/sys/windows"

func replaceNeoLocalThreadSearchSidecar(source, destination string) error {
	return windows.Rename(source, destination)
}
