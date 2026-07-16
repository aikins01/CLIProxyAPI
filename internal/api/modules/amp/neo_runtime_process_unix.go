//go:build aix || darwin || dragonfly || freebsd || illumos || linux || netbsd || openbsd || solaris

package amp

import (
	"errors"
	"os"
	"os/exec"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

func neoConfigureSpawnedExecutorProcess(cmd *exec.Cmd) {
	if cmd == nil {
		return
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func neoSpawnedExecutorDetachedForTest(cmd *exec.Cmd) bool {
	return cmd != nil && cmd.SysProcAttr != nil && cmd.SysProcAttr.Setpgid
}

func neoRecoveredHeadlessPIDSupported() bool {
	return true
}

func neoProcessStatus(pid int) (bool, bool) {
	process, err := os.FindProcess(pid)
	if err != nil {
		return false, true
	}
	err = process.Signal(syscall.Signal(0))
	return err == nil || errors.Is(err, syscall.EPERM), true
}

func neoAmpOSRelease() string {
	var uname unix.Utsname
	if err := unix.Uname(&uname); err != nil {
		return ""
	}
	var builder strings.Builder
	for _, value := range uname.Release {
		if value == 0 {
			break
		}
		builder.WriteByte(byte(value))
	}
	return builder.String()
}
