//go:build windows

package amp

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestAmpWindowsClientVersionProbeReturnsOutput(t *testing.T) {
	out, err := ampRunClientVersionProbe(
		context.Background(),
		"powershell.exe",
		[]string{"-NoProfile", "-Command", "Write-Output '1.2.3'"},
		os.Environ(),
		50*time.Millisecond,
	)
	if err != nil {
		t.Fatalf("Windows version probe: %v", err)
	}
	if got := strings.TrimSpace(string(out)); got != "1.2.3" {
		t.Fatalf("Windows version output = %q, want 1.2.3", got)
	}
}

func TestAmpWindowsClientVersionProbeKillsDescendants(t *testing.T) {
	childPIDFile := filepath.Join(t.TempDir(), "child.pid")
	t.Setenv("TEST_CHILD_PID_FILE", childPIDFile)
	script := `$child = Start-Process powershell.exe -ArgumentList @('-NoProfile', '-Command', 'Start-Sleep -Seconds 30') -PassThru; Set-Content -LiteralPath $env:TEST_CHILD_PID_FILE -Value $child.Id; Write-Output '1.2.3'`

	started := time.Now()
	_, _ = ampRunClientVersionProbe(
		context.Background(),
		"powershell.exe",
		[]string{"-NoProfile", "-Command", script},
		os.Environ(),
		50*time.Millisecond,
	)
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("Windows probe cleanup elapsed = %s, want less than two seconds", elapsed)
	}

	childPIDBytes, err := os.ReadFile(childPIDFile)
	if err != nil {
		t.Fatalf("read child PID: %v", err)
	}
	childPID, err := strconv.Atoi(strings.TrimSpace(string(childPIDBytes)))
	if err != nil {
		t.Fatalf("parse child PID: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for neoProcessAlive(childPID) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if neoProcessAlive(childPID) {
		t.Fatalf("Windows probe child process %d is still running", childPID)
	}
}
