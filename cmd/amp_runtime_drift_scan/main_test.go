package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestScanThreadDirFindsBareShellCommandDone(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "T-test.json"), map[string]any{
		"id": "T-test",
		"messages": []any{
			map[string]any{
				"messageId": "M-assistant",
				"role":      "assistant",
				"content": []any{map[string]any{
					"type": "tool_use",
					"id":   "TU-shell",
					"name": "shell_command",
				}},
			},
			map[string]any{
				"messageId": "M-result",
				"role":      "user",
				"content": []any{map[string]any{
					"type":      "tool_result",
					"toolUseID": "TU-shell",
					"run":       map[string]any{"status": "done"},
				}},
			},
		},
	})

	findings, err := scanRuntimeDrift(scanOptions{threadDir: dir, allowMissing: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 1 {
		t.Fatalf("findings = %#v, want 1", findings)
	}
	if findings[0].Source != "thread" || findings[0].ToolName != "shell_command" || findings[0].CallID != "TU-shell" {
		t.Fatalf("finding = %#v", findings[0])
	}
}

func TestScanThreadDirIgnoresNonPayloadRequiredEmptyResult(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "T-test.json"), map[string]any{
		"id": "T-test",
		"messages": []any{
			map[string]any{
				"messageId": "M-assistant",
				"role":      "assistant",
				"content": []any{map[string]any{
					"type": "tool_use",
					"id":   "TU-glob",
					"name": "glob",
				}},
			},
			map[string]any{
				"messageId": "M-result",
				"role":      "user",
				"content": []any{map[string]any{
					"type":      "tool_result",
					"toolUseID": "TU-glob",
					"run":       map[string]any{"status": "done", "result": []any{}},
				}},
			},
		},
	})

	findings, err := scanRuntimeDrift(scanOptions{threadDir: dir, allowMissing: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 0 {
		t.Fatalf("findings = %#v, want none", findings)
	}
}

func TestScanCaptureDirFindsBareReadThreadDone(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "capture.json"), map[string]any{
		"threadID": "T-test",
		"body": map[string]any{
			"input": []any{
				map[string]any{
					"type":      "function_call",
					"call_id":   "TU-read",
					"name":      "read_thread",
					"arguments": `{"threadID":"T-other"}`,
				},
				map[string]any{
					"type":    "function_call_output",
					"call_id": "TU-read",
					"output":  `{"status":"done"}`,
				},
			},
		},
	})

	findings, err := scanRuntimeDrift(scanOptions{captureDir: dir, allowMissing: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 1 {
		t.Fatalf("findings = %#v, want 1", findings)
	}
	if findings[0].Source != "provider-capture" || findings[0].ToolName != "read_thread" || findings[0].CallID != "TU-read" {
		t.Fatalf("finding = %#v", findings[0])
	}
}

func TestRunReturnsNonZeroWhenDriftFound(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "capture.json"), map[string]any{
		"body": map[string]any{
			"input": []any{
				map[string]any{"type": "function_call", "call_id": "TU-shell", "name": "shell_command"},
				map[string]any{"type": "function_call_output", "call_id": "TU-shell", "output": `{"status":"done"}`},
			},
		},
	})

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := run([]string{"-thread-dir", filepath.Join(dir, "missing"), "-capture-dir", dir}, &stdout, &stderr)
	if code != 2 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if !strings.Contains(stdout.String(), "bare terminal tool-result drift") {
		t.Fatalf("stdout = %s", stdout.String())
	}
}

func writeJSONFile(t *testing.T, path string, value any) {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
}
