//go:build !darwin && !linux && !freebsd && !netbsd && !openbsd

package amp

import "os/exec"

func neoConfigureSpawnedExecutorProcess(cmd *exec.Cmd) {}

func neoSpawnedExecutorDetachedForTest(cmd *exec.Cmd) bool {
	return true
}
