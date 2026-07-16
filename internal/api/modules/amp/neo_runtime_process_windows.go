//go:build windows

package amp

import (
	"errors"
	"fmt"
	"os"
	"os/exec"

	"golang.org/x/sys/windows"
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

func neoProcessStatus(pid int) (bool, bool) {
	if pid <= 0 {
		return false, true
	}
	handle, err := windows.OpenProcess(windows.SYNCHRONIZE, false, uint32(pid))
	if err != nil {
		return errors.Is(err, windows.ERROR_ACCESS_DENIED), true
	}
	defer windows.CloseHandle(handle)
	event, err := windows.WaitForSingleObject(handle, 0)
	return err == nil && event == uint32(windows.WAIT_TIMEOUT), true
}

func neoAmpOSRelease() string {
	version := windows.RtlGetVersion()
	return fmt.Sprintf("%d.%d.%d", version.MajorVersion, version.MinorVersion, version.BuildNumber)
}
