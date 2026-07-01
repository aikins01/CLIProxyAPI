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

func TestScanThreadDirFindsNonTerminalToolResultWithoutProgressStatus(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "T-test.json"), map[string]any{
		"id": "T-test",
		"messages": []any{
			map[string]any{
				"messageId": "M-assistant",
				"role":      "assistant",
				"content": []any{map[string]any{
					"type": "tool_use",
					"id":   "TU-review",
					"name": "code_review",
				}},
			},
			map[string]any{
				"messageId": "M-progress",
				"role":      "user",
				"content": []any{map[string]any{
					"type":      "tool_result",
					"toolUseID": "TU-review",
					"run":       map[string]any{"status": "in-progress", "progress": map[string]any{"phase": "checks"}},
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
	if findings[0].Source != "thread" || findings[0].ToolName != "code_review" || findings[0].CallID != "TU-review" {
		t.Fatalf("finding = %#v", findings[0])
	}
	if !strings.Contains(findings[0].Detail, "completionStatus=tool_progress") {
		t.Fatalf("detail = %q", findings[0].Detail)
	}
}

func TestScanThreadDirFindsCancelledAssistantStreamingBlocks(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "T-test.json"), map[string]any{
		"id": "T-test",
		"messages": []any{
			map[string]any{
				"messageId": "M-assistant",
				"role":      "assistant",
				"state":     map[string]any{"type": "cancelled"},
				"content": []any{
					map[string]any{"type": "thinking", "blockState": "streaming"},
					map[string]any{"type": "text", "blockState": "streaming", "text": "partial"},
				},
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
	if findings[0].ToolName != "streaming_state" || findings[0].MessageID != "M-assistant" {
		t.Fatalf("finding = %#v", findings[0])
	}
	if !strings.Contains(findings[0].Detail, "cancelled assistant") {
		t.Fatalf("detail = %q", findings[0].Detail)
	}
}

func TestScanThreadDirFindsCancelledAssistantNonTerminalToolBlock(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "T-test.json"), map[string]any{
		"id": "T-test",
		"messages": []any{
			map[string]any{
				"messageId": "M-assistant",
				"role":      "assistant",
				"state":     map[string]any{"type": "cancelled"},
				"content": []any{
					map[string]any{"type": "thinking", "blockState": "complete"},
					map[string]any{"type": "text", "blockState": "complete", "text": "partial"},
					map[string]any{"type": "tool_use", "id": "TU-cancelled", "name": "Bash"},
				},
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
	if findings[0].ToolName != "streaming_state" || findings[0].CallID != "tool_use" {
		t.Fatalf("finding = %#v", findings[0])
	}
	if !strings.Contains(findings[0].Detail, "non-terminal tool child") {
		t.Fatalf("detail = %q", findings[0].Detail)
	}
}

func TestScanThreadDirIgnoresCancelledAssistantTerminalToolBlock(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "T-test.json"), map[string]any{
		"id": "T-test",
		"messages": []any{
			map[string]any{
				"messageId": "M-assistant",
				"role":      "assistant",
				"state":     map[string]any{"type": "cancelled"},
				"content": []any{
					map[string]any{"type": "tool_use", "id": "TU-cancelled", "name": "Bash", "blockState": "cancelled"},
					map[string]any{"type": "server_tool_use", "id": "TU-done", "name": "read_thread", "complete": true},
				},
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

func TestScanThreadDirIgnoresLiveAssistantStreamingBlocks(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "T-test.json"), map[string]any{
		"id": "T-test",
		"messages": []any{
			map[string]any{
				"messageId": "M-assistant",
				"role":      "assistant",
				"state":     map[string]any{"type": "streaming"},
				"content": []any{
					map[string]any{"type": "thinking", "blockState": "streaming"},
				},
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

func TestScanThreadDirFindsCurrentInferenceMissingAgentMode(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "T-test.json"), map[string]any{
		"id":   "T-test",
		"meta": map[string]any{"cliProxyAPILocalNeo": true},
		"messages": []any{
			map[string]any{
				"messageId": "M-assistant",
				"role":      "assistant",
				"state":     map[string]any{"type": "streaming"},
				"content": []any{
					map[string]any{"type": "text", "blockState": "streaming", "text": "partial"},
				},
			},
		},
		"currentInference": map[string]any{"messageId": "M-assistant"},
	})

	findings, err := scanRuntimeDrift(scanOptions{threadDir: dir, threadScope: "local-runtime", allowMissing: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 1 {
		t.Fatalf("findings = %#v, want 1", findings)
	}
	if findings[0].ToolName != "inference_state" || findings[0].CallID != "currentInference" {
		t.Fatalf("finding = %#v", findings[0])
	}
	if !strings.Contains(findings[0].Detail, "agentMode is missing") {
		t.Fatalf("detail = %q", findings[0].Detail)
	}
}

func TestScanThreadDirFindsCurrentInferenceReferencingCancelledAssistant(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "T-test.json"), map[string]any{
		"id":   "T-test",
		"meta": map[string]any{"cliProxyAPILocalNeo": true},
		"messages": []any{
			map[string]any{
				"messageId": "M-assistant",
				"role":      "assistant",
				"state":     map[string]any{"type": "cancelled"},
				"content": []any{
					map[string]any{"type": "text", "blockState": "complete", "text": "partial"},
				},
			},
		},
		"currentInference": map[string]any{"messageId": "M-assistant", "agentMode": "deep"},
	})

	findings, err := scanRuntimeDrift(scanOptions{threadDir: dir, threadScope: "local-runtime", allowMissing: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 1 {
		t.Fatalf("findings = %#v, want 1", findings)
	}
	if findings[0].ToolName != "inference_state" || findings[0].CallID != "currentInference" || findings[0].MessageID != "M-assistant" {
		t.Fatalf("finding = %#v", findings[0])
	}
	if !strings.Contains(findings[0].Detail, "cancelled assistant") {
		t.Fatalf("detail = %q", findings[0].Detail)
	}
}

func TestScanThreadDirIgnoresPendingInferenceReferencingCancelledAssistant(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "T-test.json"), map[string]any{
		"id":   "T-test",
		"meta": map[string]any{"cliProxyAPILocalNeo": true},
		"messages": []any{
			map[string]any{
				"messageId": "M-assistant",
				"role":      "assistant",
				"state":     map[string]any{"type": "cancelled"},
				"content": []any{
					map[string]any{"type": "text", "blockState": "complete", "text": "partial"},
				},
			},
		},
		"pendingInference": map[string]any{"messageId": "M-assistant", "agentMode": "deep"},
	})

	findings, err := scanRuntimeDrift(scanOptions{threadDir: dir, threadScope: "local-runtime", allowMissing: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 0 {
		t.Fatalf("findings = %#v, want none", findings)
	}
}

func TestScanThreadDirFindsCompactingWithoutInference(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "T-test.json"), map[string]any{
		"id":         "T-test",
		"meta":       map[string]any{"cliProxyAPILocalNeo": true},
		"messages":   []any{},
		"compacting": true,
	})

	findings, err := scanRuntimeDrift(scanOptions{threadDir: dir, threadScope: "local-runtime", allowMissing: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 1 {
		t.Fatalf("findings = %#v, want 1", findings)
	}
	if findings[0].ToolName != "inference_state" || findings[0].CallID != "compacting" {
		t.Fatalf("finding = %#v", findings[0])
	}
}

func TestScanThreadDirIgnoresValidPendingInferenceWithoutMessageID(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "T-test.json"), map[string]any{
		"id":               "T-test",
		"meta":             map[string]any{"cliProxyAPILocalNeo": true},
		"messages":         []any{},
		"pendingInference": map[string]any{"agentMode": "smart", "reasoningEffort": "medium"},
	})

	findings, err := scanRuntimeDrift(scanOptions{threadDir: dir, threadScope: "local-runtime", allowMissing: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 0 {
		t.Fatalf("findings = %#v, want none", findings)
	}
}

func TestScanThreadDirLocalRuntimeScopeIgnoresAmpOwnedThreads(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "T-amp-owned.json"), map[string]any{
		"id": "T-amp-owned",
		"messages": []any{
			map[string]any{
				"messageId": "M-assistant",
				"role":      "assistant",
				"state":     map[string]any{"type": "cancelled"},
				"content": []any{
					map[string]any{"type": "text", "blockState": "streaming", "text": "partial"},
				},
			},
		},
	})

	findings, err := scanRuntimeDrift(scanOptions{threadDir: dir, threadScope: "local-runtime", allowMissing: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 0 {
		t.Fatalf("findings = %#v, want none for unmarked Amp-owned thread", findings)
	}
}

func TestScanThreadDirLocalRuntimeScopeFindsLocalNeoThreads(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "T-local.json"), map[string]any{
		"id":   "T-local",
		"meta": map[string]any{"cliProxyAPILocalNeo": true},
		"messages": []any{
			map[string]any{
				"messageId": "M-assistant",
				"role":      "assistant",
				"state":     map[string]any{"type": "cancelled"},
				"content": []any{
					map[string]any{"type": "text", "blockState": "streaming", "text": "partial"},
				},
			},
		},
	})

	findings, err := scanRuntimeDrift(scanOptions{threadDir: dir, threadScope: "local-runtime", allowMissing: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 1 {
		t.Fatalf("findings = %#v, want 1", findings)
	}
	if findings[0].ThreadID != "T-local" || findings[0].ToolName != "streaming_state" {
		t.Fatalf("finding = %#v", findings[0])
	}
}

func TestScanThreadDirThreadFilterFindsOnlyRequestedThread(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "T-other.json"), map[string]any{
		"id": "T-other",
		"messages": []any{
			map[string]any{
				"messageId": "M-other",
				"role":      "assistant",
				"state":     map[string]any{"type": "cancelled"},
				"content": []any{
					map[string]any{"type": "text", "blockState": "streaming", "text": "other"},
				},
			},
		},
	})
	writeJSONFile(t, filepath.Join(dir, "T-target.json"), map[string]any{
		"id": "T-target",
		"messages": []any{
			map[string]any{
				"messageId": "M-target",
				"role":      "assistant",
				"state":     map[string]any{"type": "cancelled"},
				"content": []any{
					map[string]any{"type": "text", "blockState": "streaming", "text": "target"},
				},
			},
		},
	})

	findings, err := scanRuntimeDrift(scanOptions{threadDir: dir, threadID: "T-target", allowMissing: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 1 {
		t.Fatalf("findings = %#v, want 1", findings)
	}
	if findings[0].ThreadID != "T-target" || findings[0].MessageID != "M-target" {
		t.Fatalf("finding = %#v", findings[0])
	}
}

func TestScanThreadDirIgnoresNonTerminalToolProgressMessage(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "T-test.json"), map[string]any{
		"id": "T-test",
		"messages": []any{
			map[string]any{
				"messageId": "M-progress",
				"role":      "user",
				"content": []any{map[string]any{
					"type":      "tool_result",
					"toolUseID": "TU-review",
					"run":       map[string]any{"status": "blocked-on-user"},
				}},
				"completionStatus": "tool_progress",
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

func TestScanThreadDirIgnoresPayloadRequiredStatusOnlyDoneProgressMessage(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "T-test.json"), map[string]any{
		"id": "T-test",
		"messages": []any{
			map[string]any{
				"messageId": "M-assistant",
				"role":      "assistant",
				"content": []any{map[string]any{
					"type":  "tool_use",
					"id":    "TU-review",
					"name":  "shell_command",
					"input": map[string]any{"command": "amp review"},
				}},
			},
			map[string]any{
				"messageId": "M-progress",
				"role":      "user",
				"content": []any{map[string]any{
					"type":      "tool_result",
					"toolUseID": "TU-review",
					"run":       map[string]any{"status": "done"},
				}},
				"completionStatus": "tool_progress",
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

func TestScanThreadDirSinceFiltersOldNonTerminalToolResult(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "T-test.json")
	writeJSONFile(t, path, map[string]any{
		"id": "T-test",
		"messages": []any{
			map[string]any{
				"createdAt": "2026-06-01T18:00:00Z",
				"messageId": "M-progress",
				"role":      "user",
				"content": []any{map[string]any{
					"type":      "tool_result",
					"toolUseID": "TU-review",
					"run":       map[string]any{"status": "queued"},
				}},
			},
		},
	})
	fileTime := time.Date(2026, 6, 1, 19, 0, 0, 0, time.UTC)
	if err := os.Chtimes(path, fileTime, fileTime); err != nil {
		t.Fatal(err)
	}
	since := time.Date(2026, 6, 1, 18, 30, 0, 0, time.UTC)

	findings, err := scanRuntimeDrift(scanOptions{threadDir: dir, since: since, allowMissing: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 0 {
		t.Fatalf("findings = %#v, want none because the progress message predates -since", findings)
	}
}

func TestScanThreadDirFindsMalformedQueuedMessage(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "T-test.json"), map[string]any{
		"id": "T-test",
		"queuedMessages": []any{
			map[string]any{
				"id": "queued-1",
				"queuedMessage": map[string]any{
					"role":      "assistant",
					"messageId": 41,
					"content":   "queued",
				},
				"steer": "true",
			},
		},
	})

	findings, err := scanRuntimeDrift(scanOptions{threadDir: dir, allowMissing: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 4 {
		t.Fatalf("findings = %#v, want 4", findings)
	}
	details := findingDetails(findings)
	for _, want := range []string{
		"queuedMessage role is not user",
		"queuedMessage.messageId is missing or non-string",
		"queuedMessage.content is missing or non-array",
		"queuedMessages steer is present but not boolean",
	} {
		if !strings.Contains(details, want) {
			t.Fatalf("details = %q, missing %q", details, want)
		}
	}
}

func TestScanThreadDirFindsLegacyFlatQueuedMessage(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "T-test.json"), map[string]any{
		"id": "T-test",
		"queuedMessages": []any{
			map[string]any{
				"id":        "queued-1",
				"role":      "user",
				"messageId": "M-queued",
				"content":   []any{map[string]any{"type": "text", "text": "queued"}},
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
	if findings[0].ToolName != "queued_message" || !strings.Contains(findings[0].Detail, "not wrapped") {
		t.Fatalf("finding = %#v", findings[0])
	}
}

func TestScanThreadDirIgnoresValidQueuedMessage(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "T-test.json"), map[string]any{
		"id": "T-test",
		"queuedMessages": []any{
			map[string]any{
				"id": "queued-1",
				"queuedMessage": map[string]any{
					"role":      "user",
					"messageId": "M-queued",
					"content":   []any{map[string]any{"type": "text", "text": "queued"}},
				},
				"steer": true,
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

func TestScanThreadDirSinceFiltersOldQueuedMessage(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "T-test.json")
	writeJSONFile(t, path, map[string]any{
		"id": "T-test",
		"queuedMessages": []any{
			map[string]any{
				"id": "queued-1",
				"queuedMessage": map[string]any{
					"createdAt": "2026-06-01T18:00:00Z",
					"role":      "assistant",
					"messageId": 41,
					"content":   "queued",
				},
			},
		},
	})
	fileTime := time.Date(2026, 6, 1, 19, 0, 0, 0, time.UTC)
	if err := os.Chtimes(path, fileTime, fileTime); err != nil {
		t.Fatal(err)
	}
	since := time.Date(2026, 6, 1, 18, 30, 0, 0, time.UTC)

	findings, err := scanRuntimeDrift(scanOptions{threadDir: dir, since: since, allowMissing: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 0 {
		t.Fatalf("findings = %#v, want none because the queued message predates -since", findings)
	}
}

func TestScanThreadDirFindsCompleteToolUseWithMalformedJSONFallbackInput(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "T-test.json"), map[string]any{
		"id": "T-test",
		"messages": []any{
			map[string]any{
				"messageId": "M-assistant",
				"role":      "assistant",
				"state":     map[string]any{"type": "complete", "stopReason": "tool_use"},
				"content": []any{map[string]any{
					"type":     "tool_use",
					"id":       "TU-create",
					"name":     "create_file",
					"complete": true,
					"input":    map[string]any{"input": `{"path":"/tmp/setup.c"`},
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
	if findings[0].Source != "thread" || findings[0].ToolName != "create_file" || findings[0].CallID != "TU-create" {
		t.Fatalf("finding = %#v", findings[0])
	}
	if !strings.Contains(findings[0].Detail, "malformed JSON fallback") {
		t.Fatalf("detail = %q", findings[0].Detail)
	}
}

func TestScanThreadDirIgnoresIncompleteToolUsePartialJSON(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "T-test.json"), map[string]any{
		"id": "T-test",
		"messages": []any{
			map[string]any{
				"messageId": "M-assistant",
				"role":      "assistant",
				"state":     map[string]any{"type": "streaming"},
				"content": []any{map[string]any{
					"type":             "tool_use",
					"id":               "TU-create",
					"name":             "create_file",
					"complete":         false,
					"input":            map[string]any{"path": "/tmp/setup.c"},
					"inputPartialJSON": map[string]any{"json": `{"path":"/tmp/setup.c"`},
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

func TestScanThreadDirIgnoresCustomRawInputTool(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "T-test.json"), map[string]any{
		"id": "T-test",
		"messages": []any{
			map[string]any{
				"messageId": "M-assistant",
				"role":      "assistant",
				"state":     map[string]any{"type": "complete", "stopReason": "tool_use"},
				"content": []any{map[string]any{
					"type":     "tool_use",
					"id":       "TU-custom",
					"name":     "apply_patch",
					"complete": true,
					"input":    map[string]any{"input": `{"not closed"`},
					"metadata": map[string]any{"openAICustomTool": map[string]any{"inputField": "input"}},
				}},
			},
		},
	})

	findings, err := scanRuntimeDrift(scanOptions{threadDir: dir, allowMissing: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 0 {
		t.Fatalf("findings = %#v, want none for custom raw-input tool", findings)
	}
}

func TestScanThreadDirFindsDanglingCompleteToolUseBeforeUserMessage(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "T-test.json"), map[string]any{
		"id": "T-test",
		"messages": []any{
			map[string]any{
				"messageId": "M-assistant",
				"role":      "assistant",
				"state":     map[string]any{"type": "complete", "stopReason": "tool_use"},
				"content": []any{map[string]any{
					"type":     "tool_use",
					"id":       "TU-review",
					"name":     "code_review",
					"complete": true,
					"input":    map[string]any{"diff_description": "review current diff"},
				}},
			},
			map[string]any{
				"messageId": "M-user",
				"role":      "user",
				"content":   []any{map[string]any{"type": "text", "text": "go on"}},
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
	if findings[0].Source != "thread" || findings[0].ToolName != "code_review" || findings[0].CallID != "TU-review" {
		t.Fatalf("finding = %#v", findings[0])
	}
	if !strings.Contains(findings[0].Detail, "no matching tool_result") {
		t.Fatalf("detail = %q", findings[0].Detail)
	}
}

func TestScanThreadDirFindsDanglingCompleteToolUseBeforeAssistantMessage(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "T-test.json"), map[string]any{
		"id": "T-test",
		"messages": []any{
			map[string]any{
				"messageId": "M-tool",
				"role":      "assistant",
				"state":     map[string]any{"type": "complete", "stopReason": "tool_use"},
				"content": []any{map[string]any{
					"type":     "tool_use",
					"id":       "TU-shell",
					"name":     "shell_command",
					"complete": true,
					"input":    map[string]any{"cmd": "pwd"},
				}},
			},
			map[string]any{
				"messageId": "M-answer",
				"role":      "assistant",
				"state":     map[string]any{"type": "complete", "stopReason": "end_turn"},
				"content":   []any{map[string]any{"type": "text", "text": "done"}},
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
	if findings[0].CallID != "TU-shell" || !strings.Contains(findings[0].Detail, "later assistant message") {
		t.Fatalf("finding = %#v", findings[0])
	}
}

func TestScanThreadDirIgnoresReviewModeDanglingToolUse(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "T-test.json"), map[string]any{
		"id":        "T-test",
		"agentMode": "review",
		"meta":      map[string]any{"ampcodeLocalRuntime": true},
		"messages": []any{
			map[string]any{
				"messageId": "M-tool",
				"role":      "assistant",
				"state":     map[string]any{"type": "complete", "stopReason": "tool_use"},
				"content": []any{map[string]any{
					"type":     "tool_use",
					"id":       "TU-check",
					"name":     "run_check",
					"complete": true,
					"input":    map[string]any{"checkName": "repo-convention-fit"},
				}},
			},
			map[string]any{
				"messageId": "M-answer",
				"role":      "assistant",
				"state":     map[string]any{"type": "complete", "stopReason": "end_turn"},
				"content":   []any{map[string]any{"type": "text", "text": "done"}},
			},
		},
	})

	findings, err := scanRuntimeDrift(scanOptions{threadDir: dir, threadScope: "local-runtime", allowMissing: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 0 {
		t.Fatalf("findings = %#v, want none", findings)
	}
}

func TestScanThreadDirIgnoresTrailingPendingToolUse(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "T-test.json"), map[string]any{
		"id": "T-test",
		"messages": []any{
			map[string]any{
				"messageId": "M-assistant",
				"role":      "assistant",
				"state":     map[string]any{"type": "complete", "stopReason": "tool_use"},
				"content": []any{map[string]any{
					"type":     "tool_use",
					"id":       "TU-shell",
					"name":     "shell_command",
					"complete": true,
					"input":    map[string]any{"cmd": "pwd"},
				}},
			},
		},
	})

	findings, err := scanRuntimeDrift(scanOptions{threadDir: dir, allowMissing: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 0 {
		t.Fatalf("findings = %#v, want none for trailing pending tool", findings)
	}
}

func TestScanThreadDirIgnoresToolUseWithMatchingResultBeforeContinuation(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "T-test.json"), map[string]any{
		"id": "T-test",
		"messages": []any{
			map[string]any{
				"messageId": "M-assistant",
				"role":      "assistant",
				"state":     map[string]any{"type": "complete", "stopReason": "tool_use"},
				"content": []any{map[string]any{
					"type":     "tool_use",
					"id":       "TU-shell",
					"name":     "shell_command",
					"complete": true,
					"input":    map[string]any{"cmd": "pwd"},
				}},
			},
			map[string]any{
				"messageId": "M-result",
				"role":      "user",
				"content": []any{map[string]any{
					"type":      "tool_result",
					"toolUseID": "TU-shell",
					"run":       map[string]any{"status": "done", "result": "/tmp"},
				}},
			},
			map[string]any{
				"messageId": "M-user",
				"role":      "user",
				"content":   []any{map[string]any{"type": "text", "text": "thanks"}},
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

func TestScanThreadDirSinceFiltersOldDanglingToolTrigger(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "T-test.json")
	writeJSONFile(t, path, map[string]any{
		"id": "T-test",
		"messages": []any{
			map[string]any{
				"createdAt": "2026-06-01T18:00:00Z",
				"messageId": "M-assistant",
				"role":      "assistant",
				"state":     map[string]any{"type": "complete", "stopReason": "tool_use"},
				"content": []any{map[string]any{
					"type":     "tool_use",
					"id":       "TU-shell",
					"name":     "shell_command",
					"complete": true,
					"input":    map[string]any{"cmd": "pwd"},
				}},
			},
			map[string]any{
				"createdAt": "2026-06-01T18:01:00Z",
				"messageId": "M-user",
				"role":      "user",
				"content":   []any{map[string]any{"type": "text", "text": "go on"}},
			},
		},
	})
	fileTime := time.Date(2026, 6, 1, 19, 0, 0, 0, time.UTC)
	if err := os.Chtimes(path, fileTime, fileTime); err != nil {
		t.Fatal(err)
	}
	since := time.Date(2026, 6, 1, 18, 30, 0, 0, time.UTC)

	findings, err := scanRuntimeDrift(scanOptions{threadDir: dir, since: since, allowMissing: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 0 {
		t.Fatalf("findings = %#v, want none because continuation predates -since", findings)
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

func TestScanCaptureDirRequiresProviderRequestSummary(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "capture.json"), map[string]any{
		"threadID": "T-test",
		"body": map[string]any{
			"input": []any{
				map[string]any{"role": "system", "content": "system prompt"},
				map[string]any{"role": "user", "content": "hello"},
			},
		},
	})

	findings, err := scanRuntimeDrift(scanOptions{captureDir: dir, allowMissing: true, requireCaptureSummary: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 1 {
		t.Fatalf("findings = %#v, want 1", findings)
	}
	if findings[0].Source != "provider-capture" || findings[0].ToolName != "provider_request" || findings[0].Detail != "provider capture missing request summary" {
		t.Fatalf("finding = %#v", findings[0])
	}
}

func TestScanCaptureDirAcceptsProviderRequestSummary(t *testing.T) {
	dir := t.TempDir()
	body := map[string]any{
		"input": []any{
			map[string]any{"role": "system", "content": "system prompt"},
			map[string]any{"role": "user", "content": "hello"},
		},
		"tools": []any{map[string]any{"type": "function", "name": "read_thread"}},
	}
	writeJSONFile(t, filepath.Join(dir, "capture.json"), map[string]any{
		"threadID": "T-test",
		"summary":  providerCaptureSummaryForScan(body),
		"body":     body,
	})

	findings, err := scanRuntimeDrift(scanOptions{captureDir: dir, allowMissing: true, requireCaptureSummary: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 0 {
		t.Fatalf("findings = %#v, want none", findings)
	}
}

func TestScanCaptureDirFindsNonTerminalFunctionCallOutput(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "capture.json"), map[string]any{
		"threadID": "T-test",
		"body": map[string]any{
			"input": []any{
				map[string]any{
					"type":    "function_call",
					"call_id": "TU-review",
					"name":    "code_review",
				},
				map[string]any{
					"type":    "function_call_output",
					"call_id": "TU-review",
					"output":  `{"status":"cancellation-requested"}`,
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
	if findings[0].Source != "provider-capture" || findings[0].ToolName != "code_review" || findings[0].CallID != "TU-review" {
		t.Fatalf("finding = %#v", findings[0])
	}
	if !strings.Contains(findings[0].Detail, "non-terminal tool_result status") {
		t.Fatalf("detail = %q", findings[0].Detail)
	}
}

func TestScanCaptureDirFindsNonTerminalAnthropicToolResult(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "capture.json"), map[string]any{
		"threadID": "T-test",
		"body": map[string]any{
			"messages": []any{
				map[string]any{
					"role": "assistant",
					"content": []any{map[string]any{
						"type": "tool_use",
						"id":   "TU-review",
						"name": "code_review",
					}},
				},
				map[string]any{
					"role": "user",
					"content": []any{map[string]any{
						"type":        "tool_result",
						"tool_use_id": "TU-review",
						"content":     `{"status":"blocked-on-user"}`,
					}},
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
	if findings[0].Source != "provider-capture" || findings[0].ToolName != "code_review" || findings[0].CallID != "TU-review" {
		t.Fatalf("finding = %#v", findings[0])
	}
}

func TestScanCaptureDirFindsDanglingFunctionCallBeforeMessage(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "capture.json"), map[string]any{
		"threadID": "T-test",
		"body": map[string]any{
			"input": []any{
				map[string]any{
					"type":    "function_call",
					"call_id": "TU-read",
					"name":    "read_thread",
				},
				map[string]any{
					"role":    "user",
					"content": "continue",
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
	if !strings.Contains(findings[0].Detail, "no matching tool result") {
		t.Fatalf("detail = %q", findings[0].Detail)
	}
}

func TestScanCaptureDirFindsDanglingAnthropicToolUseBeforeUserText(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "capture.json"), map[string]any{
		"provider": "anthropic",
		"threadID": "T-test",
		"body": map[string]any{
			"messages": []any{
				map[string]any{
					"role": "assistant",
					"content": []any{map[string]any{
						"type": "tool_use",
						"id":   "TU-review",
						"name": "code_review",
					}},
				},
				map[string]any{
					"role": "user",
					"content": []any{map[string]any{
						"type": "text",
						"text": "continue",
					}},
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
	if findings[0].Source != "provider-capture" || findings[0].ToolName != "code_review" || findings[0].CallID != "TU-review" {
		t.Fatalf("finding = %#v", findings[0])
	}
}

func TestScanCaptureDirIgnoresPairedProviderToolCalls(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "capture.json"), map[string]any{
		"threadID": "T-test",
		"body": map[string]any{
			"input": []any{
				map[string]any{
					"type":    "function_call",
					"call_id": "TU-read",
					"name":    "read_thread",
				},
				map[string]any{
					"type":    "function_call_output",
					"call_id": "TU-read",
					"output":  `{"status":"done","result":"ok"}`,
				},
				map[string]any{
					"role":    "user",
					"content": "continue",
				},
			},
		},
	})

	findings, err := scanRuntimeDrift(scanOptions{captureDir: dir, allowMissing: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 0 {
		t.Fatalf("findings = %#v, want none", findings)
	}
}

func TestScanCaptureDirFindsMalformedFunctionCallArguments(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "capture.json"), map[string]any{
		"threadID": "T-test",
		"body": map[string]any{
			"input": []any{
				map[string]any{
					"type":      "function_call",
					"call_id":   "TU-create",
					"name":      "create_file",
					"arguments": `{"input":"{\"path\":\"/tmp/setup.c\""}`,
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
	if findings[0].Source != "provider-capture" || findings[0].ToolName != "create_file" || findings[0].CallID != "TU-create" {
		t.Fatalf("finding = %#v", findings[0])
	}
	if !strings.Contains(findings[0].Detail, "malformed function_call arguments") {
		t.Fatalf("detail = %q", findings[0].Detail)
	}
}

func TestScanCaptureDirFindsMalformedAnthropicToolUseInput(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "capture.json"), map[string]any{
		"provider": "anthropic",
		"threadID": "T-test",
		"body": map[string]any{
			"messages": []any{map[string]any{
				"role": "assistant",
				"content": []any{map[string]any{
					"type":  "tool_use",
					"id":    "TU-create",
					"name":  "create_file",
					"input": map[string]any{"input": `{"path":"/tmp/setup.c"`},
				}},
			}},
		},
	})

	findings, err := scanRuntimeDrift(scanOptions{captureDir: dir, allowMissing: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 1 {
		t.Fatalf("findings = %#v, want 1", findings)
	}
	if findings[0].Source != "provider-capture" || findings[0].ToolName != "create_file" || findings[0].CallID != "TU-create" {
		t.Fatalf("finding = %#v", findings[0])
	}
	if !strings.Contains(findings[0].Detail, "malformed tool_use input") {
		t.Fatalf("detail = %q", findings[0].Detail)
	}
}

func TestScanCaptureDirIgnoresCustomRawAnthropicToolUseInput(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "capture.json"), map[string]any{
		"provider": "anthropic",
		"threadID": "T-test",
		"body": map[string]any{
			"messages": []any{map[string]any{
				"role": "assistant",
				"content": []any{map[string]any{
					"type":  "tool_use",
					"id":    "TU-raw",
					"name":  "custom_raw",
					"input": map[string]any{"input": `{"raw":`},
					"metadata": map[string]any{
						"openAICustomTool": map[string]any{"type": "custom"},
					},
				}},
			}},
		},
	})

	findings, err := scanRuntimeDrift(scanOptions{captureDir: dir, allowMissing: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 0 {
		t.Fatalf("findings = %#v, want none for custom raw input", findings)
	}
}

func TestScanCaptureDirFindsAnthropicBase64ImageMissingMediaType(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "capture.json"), map[string]any{
		"provider": "anthropic",
		"threadID": "T-test",
		"body": map[string]any{
			"messages": []any{map[string]any{
				"role": "user",
				"content": []any{map[string]any{
					"type": "image",
					"source": map[string]any{
						"type": "base64",
						"data": "iVBORw0KGgo=",
					},
				}},
			}},
		},
	})

	findings, err := scanRuntimeDrift(scanOptions{captureDir: dir, allowMissing: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 1 {
		t.Fatalf("findings = %#v, want 1", findings)
	}
	if findings[0].ToolName != "image_attachment" || findings[0].ThreadID != "T-test" {
		t.Fatalf("finding = %#v", findings[0])
	}
	if !strings.Contains(findings[0].Detail, "missing media_type") {
		t.Fatalf("detail = %q", findings[0].Detail)
	}
}

func TestScanCaptureDirFindsAnthropicBase64ImageCamelMediaTypeOnly(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "capture.json"), map[string]any{
		"provider": "anthropic",
		"threadID": "T-test",
		"body": map[string]any{
			"messages": []any{map[string]any{
				"role": "user",
				"content": []any{map[string]any{
					"type": "image",
					"source": map[string]any{
						"type":      "base64",
						"mediaType": "image/png",
						"data":      "iVBORw0KGgo=",
					},
				}},
			}},
		},
	})

	findings, err := scanRuntimeDrift(scanOptions{captureDir: dir, allowMissing: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 1 {
		t.Fatalf("findings = %#v, want 1", findings)
	}
	if findings[0].ToolName != "image_attachment" {
		t.Fatalf("finding = %#v", findings[0])
	}
}

func TestScanCaptureDirIgnoresAnthropicBase64ImageWithMediaType(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "capture.json"), map[string]any{
		"provider": "anthropic",
		"threadID": "T-test",
		"body": map[string]any{
			"messages": []any{map[string]any{
				"role": "user",
				"content": []any{map[string]any{
					"type": "image",
					"source": map[string]any{
						"type":       "base64",
						"media_type": "image/png",
						"data":       "iVBORw0KGgo=",
					},
				}},
			}},
		},
	})

	findings, err := scanRuntimeDrift(scanOptions{captureDir: dir, allowMissing: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 0 {
		t.Fatalf("findings = %#v, want none", findings)
	}
}

func TestScanCaptureDirThreadFilterFindsOnlyRequestedThread(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "other.json"), map[string]any{
		"threadID": "T-other",
		"body": map[string]any{
			"input": []any{
				map[string]any{"type": "function_call", "call_id": "TU-other", "name": "read_thread"},
				map[string]any{"type": "function_call_output", "call_id": "TU-other", "output": `{"status":"done"}`},
			},
		},
	})
	writeJSONFile(t, filepath.Join(dir, "target.json"), map[string]any{
		"threadID": "T-target",
		"body": map[string]any{
			"input": []any{
				map[string]any{"type": "function_call", "call_id": "TU-target", "name": "read_thread"},
				map[string]any{"type": "function_call_output", "call_id": "TU-target", "output": `{"status":"done"}`},
			},
		},
	})

	findings, err := scanRuntimeDrift(scanOptions{captureDir: dir, threadID: "T-target", allowMissing: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 1 {
		t.Fatalf("findings = %#v, want 1", findings)
	}
	if findings[0].ThreadID != "T-target" || findings[0].CallID != "TU-target" {
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

func TestSummarizeFindingsGroupsTokenSpecificCompactionDetails(t *testing.T) {
	summary := summarizeFindings([]driftFinding{
		{
			Source:   "thread",
			ToolName: "compaction",
			Detail:   "smart usage observed 249061 tokens at or above 75% threshold 249000 with no later compaction record",
		},
		{
			Source:   "thread",
			ToolName: "compaction",
			Detail:   "smart usage observed 250127 tokens at or above 75% threshold 249000 with no later compaction record",
		},
		{
			Source:   "thread",
			ToolName: "compaction",
			Detail:   "smart compaction observed 100592 tokens below 75% threshold 249000 at 2026-06-01T15:42:09.062188Z",
		},
	})

	if len(summary) != 2 {
		t.Fatalf("summary = %#v, want 2 groups", summary)
	}
	if summary[0].Count != 2 || summary[0].Category != "smart usage at/above compaction threshold without later compaction" {
		t.Fatalf("first summary = %#v", summary[0])
	}
	if summary[1].Count != 1 || summary[1].Category != "smart compaction below threshold" {
		t.Fatalf("second summary = %#v", summary[1])
	}
}

func TestRunSummaryOutputGroupsFindings(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "capture-a.json"), map[string]any{
		"threadID": "T-test",
		"body": map[string]any{
			"input": []any{
				map[string]any{"type": "function_call", "call_id": "TU-a", "name": "read_thread"},
				map[string]any{"type": "function_call_output", "call_id": "TU-a", "output": `{"status":"done"}`},
			},
		},
	})
	writeJSONFile(t, filepath.Join(dir, "capture-b.json"), map[string]any{
		"threadID": "T-test",
		"body": map[string]any{
			"input": []any{
				map[string]any{"type": "function_call", "call_id": "TU-b", "name": "read_thread"},
				map[string]any{"type": "function_call_output", "call_id": "TU-b", "output": `{"status":"done"}`},
			},
		},
	})

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := run([]string{"-thread-dir", filepath.Join(dir, "missing"), "-capture-dir", dir, "-summary"}, &stdout, &stderr)
	if code != 2 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	output := stdout.String()
	if !strings.Contains(output, "found 2 runtime drift(s) in 1 group(s):") {
		t.Fatalf("stdout = %q", output)
	}
	if !strings.Contains(output, "- 2 provider-capture read_thread: terminal done missing result/output payload") {
		t.Fatalf("stdout = %q", output)
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

func TestRunSinceHomebrewRuntimeFallsBackToInstalledBinaryModTime(t *testing.T) {
	dir := t.TempDir()
	installed := filepath.Join(dir, "cliproxyapi")
	if err := os.WriteFile(installed, []byte("marker"), 0o600); err != nil {
		t.Fatal(err)
	}
	markerTime := time.Date(2026, 6, 1, 19, 35, 4, 0, time.UTC)
	if err := os.Chtimes(installed, markerTime, markerTime); err != nil {
		t.Fatal(err)
	}
	oldHomebrewRuntimeBinaryPath := homebrewRuntimeBinaryPath
	oldHomebrewRuntimeFallbackBinaryPath := homebrewRuntimeFallbackBinaryPath
	homebrewRuntimeBinaryPath = func() string { return filepath.Join(dir, "missing.real") }
	homebrewRuntimeFallbackBinaryPath = func() string { return installed }
	t.Cleanup(func() {
		homebrewRuntimeBinaryPath = oldHomebrewRuntimeBinaryPath
		homebrewRuntimeFallbackBinaryPath = oldHomebrewRuntimeFallbackBinaryPath
	})

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

func findingDetails(findings []driftFinding) string {
	var details []string
	for _, finding := range findings {
		details = append(details, finding.Detail)
	}
	return strings.Join(details, "\n")
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
