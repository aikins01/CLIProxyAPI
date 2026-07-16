//go:build !aix && !darwin && !dragonfly && !freebsd && !illumos && !linux && !netbsd && !openbsd && !solaris && !windows

package amp

import "os/exec"

func neoConfigureSpawnedExecutorProcess(cmd *exec.Cmd) {}

func neoSpawnedExecutorDetachedForTest(cmd *exec.Cmd) bool {
	return true
}

func neoRecoveredHeadlessPIDSupported() bool {
	return false
}

func neoProcessStatus(int) (bool, bool) {
	return false, false
}

func neoAmpOSRelease() string {
	return ""
}
