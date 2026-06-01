package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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

func TestScanThreadDirSinceFiltersOldThreadMessages(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "T-test.json")
	writeJSONFile(t, path, map[string]any{
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
				"createdAt": "2026-06-01T18:28:59.003825Z",
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
	fileTime := time.Date(2026, 6, 1, 19, 36, 0, 0, time.UTC)
	if err := os.Chtimes(path, fileTime, fileTime); err != nil {
		t.Fatal(err)
	}
	since := time.Date(2026, 6, 1, 19, 35, 4, 0, time.UTC)

	findings, err := scanRuntimeDrift(scanOptions{threadDir: dir, since: since, allowMissing: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 0 {
		t.Fatalf("findings = %#v, want none because the bad message predates -since", findings)
	}
}

func TestScanThreadDirFindsEarlySmartCompaction(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "T-smart.json"), map[string]any{
		"id":        "T-smart",
		"agentMode": "smart",
		"messages": []any{
			map[string]any{
				"createdAt": "2026-06-01T19:59:40.358042Z",
				"messageId": "M-assistant",
				"role":      "assistant",
				"usage": map[string]any{
					"model":            "claude-opus-4-8",
					"totalInputTokens": 101_599,
					"outputTokens":     317,
					"maxInputTokens":   300_000,
				},
			},
		},
		"compactionRecords": []any{map[string]any{
			"createdAt":    "2026-06-01T20:00:09.138603Z",
			"cutMessageId": "M-summary",
		}},
	})

	findings, err := scanRuntimeDrift(scanOptions{threadDir: dir, modelContextWindows: testModelContextWindows(), allowMissing: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 1 {
		t.Fatalf("findings = %#v, want 1", findings)
	}
	if findings[0].ToolName != "compaction" || findings[0].MessageID != "M-assistant" || findings[0].CallID != "M-summary" {
		t.Fatalf("finding = %#v", findings[0])
	}
	if !strings.Contains(findings[0].Detail, "below 75% threshold 249000") {
		t.Fatalf("detail = %q", findings[0].Detail)
	}
}

func TestScanThreadDirIgnoresSmartCompactionAtThreshold(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "T-smart.json"), map[string]any{
		"id":        "T-smart",
		"agentMode": "smart",
		"messages": []any{
			map[string]any{
				"createdAt": "2026-06-01T20:00:00Z",
				"messageId": "M-assistant",
				"role":      "assistant",
				"usage": map[string]any{
					"model":            "claude-opus-4-8",
					"totalInputTokens": 248_999,
					"outputTokens":     1,
					"maxInputTokens":   300_000,
				},
			},
		},
		"compactionRecords": []any{map[string]any{
			"createdAt":    "2026-06-01T20:00:10Z",
			"cutMessageId": "M-summary",
		}},
	})

	findings, err := scanRuntimeDrift(scanOptions{threadDir: dir, modelContextWindows: testModelContextWindows(), allowMissing: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 0 {
		t.Fatalf("findings = %#v, want none", findings)
	}
}

func TestScanThreadDirFindsMissingSmartCompactionAtThreshold(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "T-smart.json"), map[string]any{
		"id":        "T-smart",
		"agentMode": "smart",
		"messages": []any{
			map[string]any{
				"createdAt": "2026-06-01T20:00:00Z",
				"messageId": "M-assistant",
				"role":      "assistant",
				"usage": map[string]any{
					"model":            "claude-opus-4-8",
					"totalInputTokens": 248_999,
					"outputTokens":     1,
					"maxInputTokens":   300_000,
				},
			},
		},
	})

	findings, err := scanRuntimeDrift(scanOptions{threadDir: dir, modelContextWindows: testModelContextWindows(), allowMissing: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 1 {
		t.Fatalf("findings = %#v, want 1", findings)
	}
	if findings[0].ToolName != "compaction" || findings[0].MessageID != "M-assistant" {
		t.Fatalf("finding = %#v", findings[0])
	}
	if !strings.Contains(findings[0].Detail, "no later compaction record") {
		t.Fatalf("detail = %q", findings[0].Detail)
	}
}

func TestScanThreadDirIgnoresSmartUsageWithLaterCompaction(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "T-smart.json"), map[string]any{
		"id":        "T-smart",
		"agentMode": "smart",
		"messages": []any{
			map[string]any{
				"createdAt": "2026-06-01T20:00:00Z",
				"messageId": "M-assistant",
				"role":      "assistant",
				"usage": map[string]any{
					"model":            "claude-opus-4-8",
					"totalInputTokens": 248_999,
					"outputTokens":     1,
					"maxInputTokens":   300_000,
				},
			},
		},
		"compactionRecords": []any{map[string]any{
			"createdAt":    "2026-06-01T20:00:10Z",
			"cutMessageId": "M-summary",
		}},
	})

	findings, err := scanRuntimeDrift(scanOptions{threadDir: dir, modelContextWindows: testModelContextWindows(), allowMissing: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 0 {
		t.Fatalf("findings = %#v, want none", findings)
	}
}

func TestScanThreadDirSinceFiltersOldCompactionRecords(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "T-smart.json")
	writeJSONFile(t, path, map[string]any{
		"id":        "T-smart",
		"agentMode": "smart",
		"messages": []any{
			map[string]any{
				"createdAt": "2026-06-01T19:59:40Z",
				"messageId": "M-assistant",
				"role":      "assistant",
				"usage": map[string]any{
					"model":            "claude-opus-4-8",
					"totalInputTokens": 101_599,
					"outputTokens":     317,
				},
			},
		},
		"compactionRecords": []any{map[string]any{
			"createdAt":    "2026-06-01T20:00:09Z",
			"cutMessageId": "M-summary",
		}},
	})
	fileTime := time.Date(2026, 6, 1, 20, 5, 0, 0, time.UTC)
	if err := os.Chtimes(path, fileTime, fileTime); err != nil {
		t.Fatal(err)
	}
	since := time.Date(2026, 6, 1, 20, 1, 0, 0, time.UTC)

	findings, err := scanRuntimeDrift(scanOptions{threadDir: dir, modelContextWindows: testModelContextWindows(), since: since, allowMissing: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 0 {
		t.Fatalf("findings = %#v, want none because the compaction record predates -since", findings)
	}
}

func TestScanThreadDirSinceFiltersOldUncompactedUsage(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "T-smart.json")
	writeJSONFile(t, path, map[string]any{
		"id":        "T-smart",
		"agentMode": "smart",
		"messages": []any{
			map[string]any{
				"createdAt": "2026-06-01T19:59:40Z",
				"messageId": "M-assistant",
				"role":      "assistant",
				"usage": map[string]any{
					"model":            "claude-opus-4-8",
					"totalInputTokens": 248_999,
					"outputTokens":     1,
				},
			},
		},
	})
	fileTime := time.Date(2026, 6, 1, 20, 5, 0, 0, time.UTC)
	if err := os.Chtimes(path, fileTime, fileTime); err != nil {
		t.Fatal(err)
	}
	since := time.Date(2026, 6, 1, 20, 1, 0, 0, time.UTC)

	findings, err := scanRuntimeDrift(scanOptions{threadDir: dir, modelContextWindows: testModelContextWindows(), since: since, allowMissing: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 0 {
		t.Fatalf("findings = %#v, want none because the usage predates -since", findings)
	}
}

func TestScanThreadDirIgnoresCustomSmartCompactionThreshold(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "T-smart.json"), map[string]any{
		"id":        "T-smart",
		"agentMode": "smart",
		"settings": map[string]any{
			"compactionControl": map[string]any{"contextTokenThreshold": 100_000},
		},
		"messages": []any{
			map[string]any{
				"createdAt": "2026-06-01T20:00:00Z",
				"messageId": "M-assistant",
				"role":      "assistant",
				"usage": map[string]any{
					"model":            "claude-opus-4-8",
					"totalInputTokens": 101_599,
					"outputTokens":     317,
				},
			},
		},
		"compactionRecords": []any{map[string]any{
			"createdAt":    "2026-06-01T20:00:10Z",
			"cutMessageId": "M-summary",
		}},
	})

	findings, err := scanRuntimeDrift(scanOptions{threadDir: dir, modelContextWindows: testModelContextWindows(), allowMissing: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 0 {
		t.Fatalf("findings = %#v, want none for custom threshold", findings)
	}
}

func TestScanThreadDirLoadsModelContextFromBaseline(t *testing.T) {
	dir := t.TempDir()
	baselinePath := filepath.Join(dir, "baseline.json")
	writeJSONFile(t, baselinePath, map[string]any{
		"signals": map[string]any{
			"agent_mode_routes": []any{map[string]any{
				"name":  "smart",
				"model": "claude-opus-4-7",
			}},
			"model_limits": []any{map[string]any{
				"name":           "claude-opus-4-8",
				"context_window": 332_000,
			}},
		},
	})
	writeJSONFile(t, filepath.Join(dir, "T-smart.json"), map[string]any{
		"id":        "T-smart",
		"agentMode": "smart",
		"messages": []any{
			map[string]any{
				"createdAt": "2026-06-01T19:59:40.358042Z",
				"messageId": "M-assistant",
				"role":      "assistant",
				"usage": map[string]any{
					"model":            "claude-opus-4-8",
					"totalInputTokens": 101_599,
					"outputTokens":     317,
					"maxInputTokens":   300_000,
				},
			},
		},
		"compactionRecords": []any{map[string]any{
			"createdAt":    "2026-06-01T20:00:09.138603Z",
			"cutMessageId": "M-summary",
		}},
	})

	findings, err := scanRuntimeDrift(scanOptions{threadDir: dir, baselinePath: baselinePath, allowMissing: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 1 {
		t.Fatalf("findings = %#v, want 1", findings)
	}
	if !strings.Contains(findings[0].Detail, "below 75% threshold 249000") {
		t.Fatalf("detail = %q", findings[0].Detail)
	}
}

func TestScanThreadDirUsesSmartRouteForMappedUsageModel(t *testing.T) {
	dir := t.TempDir()
	baselinePath := filepath.Join(dir, "baseline.json")
	writeJSONFile(t, baselinePath, map[string]any{
		"signals": map[string]any{
			"agent_mode_routes": []any{map[string]any{
				"name":  "smart",
				"model": "claude-opus-4-7",
			}},
			"model_limits": []any{
				map[string]any{
					"name":           "claude-opus-4-7",
					"context_window": 332_000,
				},
				map[string]any{
					"name":           "gpt-5.5",
					"context_window": 400_000,
				},
			},
		},
	})
	writeJSONFile(t, filepath.Join(dir, "T-smart.json"), map[string]any{
		"id":        "T-smart",
		"agentMode": "smart",
		"messages": []any{
			map[string]any{
				"createdAt": "2026-06-01T20:06:05.421894Z",
				"messageId": "M-assistant",
				"role":      "assistant",
				"usage": map[string]any{
					"model":            "gpt-5.5",
					"totalInputTokens": 250_000,
					"outputTokens":     1,
					"maxInputTokens":   272_000,
				},
			},
		},
		"compactionRecords": []any{map[string]any{
			"createdAt":    "2026-06-01T20:06:05.423556Z",
			"cutMessageId": "M-summary",
		}},
	})

	findings, err := scanRuntimeDrift(scanOptions{threadDir: dir, baselinePath: baselinePath, allowMissing: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 0 {
		t.Fatalf("findings = %#v, want none because mapped usage should use the smart route threshold", findings)
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

func testModelContextWindows() map[string]int {
	return map[string]int{"claude-opus-4-8": 332_000}
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
	if !strings.Contains(stdout.String(), "runtime drift") {
		t.Fatalf("stdout = %s", stdout.String())
	}
}

func TestRunJSONNoFindingsPrintsArray(t *testing.T) {
	dir := t.TempDir()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := run([]string{"-thread-dir", dir, "-capture-dir", filepath.Join(dir, "missing"), "-json"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if strings.TrimSpace(stdout.String()) != "[]" {
		t.Fatalf("stdout = %q, want []", stdout.String())
	}
}

func TestRunSinceFileUsesMarkerModTime(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "cliproxyapi.real")
	if err := os.WriteFile(marker, []byte("marker"), 0o600); err != nil {
		t.Fatal(err)
	}
	markerTime := time.Date(2026, 6, 1, 19, 35, 4, 0, time.UTC)
	if err := os.Chtimes(marker, markerTime, markerTime); err != nil {
		t.Fatal(err)
	}
	writeJSONFile(t, filepath.Join(dir, "T-test.json"), map[string]any{
		"id": "T-test",
		"messages": []any{
			map[string]any{
				"messageId": "M-assistant",
				"role":      "assistant",
				"content":   []any{map[string]any{"type": "tool_use", "id": "TU-shell", "name": "shell_command"}},
			},
			map[string]any{
				"createdAt": "2026-06-01T18:28:59.003825Z",
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

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := run([]string{"-thread-dir", dir, "-capture-dir", filepath.Join(dir, "missing"), "-since-file", marker, "-json"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if strings.TrimSpace(stdout.String()) != "[]" {
		t.Fatalf("stdout = %q, want []", stdout.String())
	}
}

func TestRunSinceHomebrewRuntimeUsesReplacementModTime(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "cliproxyapi.real")
	if err := os.WriteFile(marker, []byte("marker"), 0o600); err != nil {
		t.Fatal(err)
	}
	markerTime := time.Date(2026, 6, 1, 19, 35, 4, 0, time.UTC)
	if err := os.Chtimes(marker, markerTime, markerTime); err != nil {
		t.Fatal(err)
	}
	oldHomebrewRuntimeBinaryPath := homebrewRuntimeBinaryPath
	homebrewRuntimeBinaryPath = func() string { return marker }
	t.Cleanup(func() { homebrewRuntimeBinaryPath = oldHomebrewRuntimeBinaryPath })

	writeJSONFile(t, filepath.Join(dir, "T-test.json"), map[string]any{
		"id": "T-test",
		"messages": []any{
			map[string]any{
				"messageId": "M-assistant",
				"role":      "assistant",
				"content":   []any{map[string]any{"type": "tool_use", "id": "TU-shell", "name": "shell_command"}},
			},
			map[string]any{
				"createdAt": "2026-06-01T18:28:59.003825Z",
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

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := run([]string{"-thread-dir", dir, "-capture-dir", filepath.Join(dir, "missing"), "-since-homebrew-runtime", "-json"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if strings.TrimSpace(stdout.String()) != "[]" {
		t.Fatalf("stdout = %q, want []", stdout.String())
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
