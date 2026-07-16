//go:build aix || darwin || dragonfly || freebsd || illumos || linux || netbsd || openbsd || solaris

package amp

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"syscall"
	"time"
)

func ampRunClientVersionProbe(ctx context.Context, command string, args, env []string, waitDelay time.Duration) ([]byte, error) {
	cmd := exec.CommandContext(ctx, command, args...)
	cmd.Env = env
	cmd.WaitDelay = waitDelay
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		return ampTerminateClientVersionProbe(cmd)
	}
	out, err := cmd.Output()
	terminateErr := ampTerminateClientVersionProbe(cmd)
	if err == nil {
		err = terminateErr
	}
	return out, err
}

func ampTerminateClientVersionProbe(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	if errors.Is(err, syscall.ESRCH) || errors.Is(err, os.ErrProcessDone) {
		return nil
	}
	return err
}
