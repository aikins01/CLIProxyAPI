//go:build aix || darwin || dragonfly || freebsd || illumos || linux || netbsd || openbsd || solaris

package amp

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestNeoSpawnedExecutorStopKillsProcessGroup(t *testing.T) {
	childPIDFile := filepath.Join(t.TempDir(), "child.pid")
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", `sleep 30 & echo $! > "$CHILD_PID_FILE"; wait`)
	cmd.Env = append(os.Environ(), "CHILD_PID_FILE="+childPIDFile)
	neoConfigureSpawnedExecutorProcess(cmd)
	cmd.Cancel = func() error {
		return neoCancelSpawnedExecutorProcess(cmd)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start executor process group: %v", err)
	}
	spawned := &neoSpawnedExecutor{cmd: cmd, cancel: cancel}
	t.Cleanup(func() {
		spawned.stop()
		_ = cmd.Wait()
	})

	var childPID int
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		body, err := os.ReadFile(childPIDFile)
		if err == nil {
			childPID, err = strconv.Atoi(strings.TrimSpace(string(body)))
			if err != nil {
				t.Fatalf("parse child pid: %v", err)
			}
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("read child pid: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if childPID <= 0 {
		t.Fatal("executor child did not start")
	}

	spawned.stop()
	_ = cmd.Wait()
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if errors.Is(syscall.Kill(-cmd.Process.Pid, 0), syscall.ESRCH) && errors.Is(syscall.Kill(childPID, 0), syscall.ESRCH) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("executor process group survived stop: leader=%d child=%d", cmd.Process.Pid, childPID)
}
