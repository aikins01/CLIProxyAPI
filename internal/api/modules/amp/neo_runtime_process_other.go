//go:build !aix && !darwin && !dragonfly && !freebsd && !illumos && !linux && !netbsd && !openbsd && !solaris && !windows

package amp

import (
	"errors"
	"os"
	"os/exec"
)

func neoConfigureSpawnedExecutorProcess(cmd *exec.Cmd) {}

func neoCancelSpawnedExecutorProcess(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	err := cmd.Process.Kill()
	if errors.Is(err, os.ErrProcessDone) {
		return nil
	}
	return err
}

func neoCancelRecoveredExecutorProcess(pid int) error {
	if pid <= 0 {
		return nil
	}
	process, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	err = process.Kill()
	if errors.Is(err, os.ErrProcessDone) {
		return nil
	}
	return err
}

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
