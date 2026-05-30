//go:build darwin || linux || freebsd || netbsd || openbsd

package amp

import (
	"os/exec"
	"syscall"
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
