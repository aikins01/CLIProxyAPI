//go:build windows

package amp

import (
	"errors"
	"fmt"
	"os/exec"

	"golang.org/x/sys/windows"
)

func neoConfigureSpawnedExecutorProcess(cmd *exec.Cmd) {}

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
