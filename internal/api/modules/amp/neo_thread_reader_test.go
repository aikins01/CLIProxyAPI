package amp

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
)

func neoReadThreadScriptedInfer(t *testing.T, final, query string, captured *[]neoInferenceRequest, inspect func(int, neoInferenceRequest)) func(*neoRuntime, neoInferenceRequest, neoStreamCallback) (neoInferenceResult, error) {
	t.Helper()
	return func(_ *neoRuntime, request neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		turn := len(*captured)
		*captured = append(*captured, request)
		if inspect != nil {
			inspect(turn, request)
		}
		switch turn % 3 {
		case 0:
			return neoInferenceResult{ToolCalls: []neoToolCall{{ID: fmt.Sprintf("TU-search-%d", turn), Name: "search_thread_messages", Input: map[string]any{"query": query}}}}, nil
		case 1:
			return neoInferenceResult{ToolCalls: []neoToolCall{{ID: fmt.Sprintf("TU-read-%d", turn), Name: "read_thread_messages", Input: map[string]any{"latest": true, "count": 1}}}}, nil
		default:
			return neoInferenceResult{Text: neoReadThreadTestFinalJSON(final)}, nil
		}
	}
}

func neoReadThreadTestFinalJSON(text string) string {
	raw, _ := json.Marshal(text)
	return `{"relevantContent":` + string(raw) + `}`
}

func neoReadThreadAssertAgentRequest(t *testing.T, request neoInferenceRequest) {
	t.Helper()
	route := request.ModelRouteOverride
	if route == nil || route.Provider != "google" || route.Model != neoReadThreadAgentModel {
		t.Fatalf("read_thread route = %#v, want google/%s", route, neoReadThreadAgentModel)
	}
	if request.ReasoningEffort != neoReadThreadAgentEffort || stringValue(request.Settings["reasoning.effort"]) != neoReadThreadAgentEffort || stringValue(request.Settings["gemini.thinkingLevel"]) != neoReadThreadAgentEffort {
		t.Fatalf("read_thread effort request=%q settings=%#v, want high", request.ReasoningEffort, request.Settings)
	}
	if request.DisableSystemPrompt || request.DisableProviderReasoning {
		t.Fatalf("read_thread disabled flags system=%v providerReasoning=%v, want false", request.DisableSystemPrompt, request.DisableProviderReasoning)
	}
	if request.ProviderFeature != "amp.read-thread" {
		t.Fatalf("provider feature = %q, want amp.read-thread", request.ProviderFeature)
	}
	if !strings.Contains(request.SystemPromptOverride, "read_thread subagent") {
		t.Fatalf("system prompt override = %q", request.SystemPromptOverride)
	}
	if !neoReadThreadHasTools(request.Tools, "thread_overview", "search_thread_messages", "read_thread_messages") {
		t.Fatalf("tools = %#v, want read_thread internal tools", request.Tools)
	}
}

func neoReadThreadHasTools(tools []neoToolSpec, names ...string) bool {
	seen := map[string]bool{}
	for _, tool := range tools {
		seen[tool.Name] = true
	}
	for _, name := range names {
		if !seen[name] {
			return false
		}
	}
	return true
}

func neoReadThreadWriteLocalThread(t *testing.T, dir, threadID string, messages []any) {
	t.Helper()
	threadDir := filepath.Join(dir, "threads")
	if err := os.MkdirAll(threadDir, 0o700); err != nil {
		t.Fatalf("mkdir thread dir: %v", err)
	}
	raw, err := json.Marshal(map[string]any{
		"id":       threadID,
		"title":    "read thread test",
		"messages": messages,
	})
	if err != nil {
		t.Fatalf("marshal thread: %v", err)
	}
	if err := os.WriteFile(filepath.Join(threadDir, threadID+".json"), raw, 0o600); err != nil {
		t.Fatalf("write thread: %v", err)
	}
}

func TestNeoReadThreadLocalGateRequiresExecutorBootstrap(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.tools["read_thread"] = neoReadThreadToolSpec()

	if actor.shouldRunLocalActorTool("read_thread") {
		t.Fatal("read_thread should not run locally before executor bootstrap completes")
	}
	actor.executorBootstrapComplete = true
	if !actor.shouldRunLocalActorTool("read_thread") {
		t.Fatal("read_thread should run locally after executor bootstrap, even when registered")
	}
}

func TestNeoReadThreadExcerptHandlesUnicodeCaseExpansion(t *testing.T) {
	text := strings.Repeat("\u0130", 80) + " target survives after expansion"
	excerpt := neoReadThreadExcerpt(text, "target", neoReadThreadSearchTerms("target"), 32)
	if !strings.Contains(excerpt, "target survives") {
		t.Fatalf("excerpt = %q, want match after unicode case expansion", excerpt)
	}
}

func TestNeoReadThreadRejectsFinalUntilSearchReadAndLatest(t *testing.T) {
	dir := t.TempDir()
	oldStoreDir := neoAmpDataDir
	neoAmpDataDir = func() string { return dir }
	t.Cleanup(func() { neoAmpDataDir = oldStoreDir })

	currentThreadID := "T-019e65c0-0310-77a8-b233-4b84d9c0612f"
	targetThreadID := "T-019e65c0-0310-77a8-b233-4b84d9c06130"
	neoReadThreadWriteLocalThread(t, dir, targetThreadID, []any{
		map[string]any{"role": "user", "messageId": "M-early", "content": []any{map[string]any{"type": "text", "text": "Initial decision: use approach A."}}},
		map[string]any{"role": "assistant", "messageId": "M-middle", "content": []any{map[string]any{"type": "text", "text": "Implementation started for approach A."}}},
		map[string]any{"role": "user", "messageId": "M-latest", "content": []any{map[string]any{"type": "text", "text": "Revision: approach A was reverted. Use approach B instead."}}},
	})

	var captured []neoInferenceRequest
	rt := newNeoRuntime(&config.Config{})
	rt.inferStream = func(_ *neoRuntime, request neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		turn := len(captured)
		captured = append(captured, request)
		switch turn {
		case 0:
			return neoInferenceResult{Text: neoReadThreadTestFinalJSON("too early")}, nil
		case 1:
			if !strings.Contains(neoHistoryTestText(request.History), "search_thread_messages") {
				t.Fatalf("turn %d history missing search correction: %s", turn, neoHistoryTestText(request.History))
			}
			return neoInferenceResult{ToolCalls: []neoToolCall{{ID: "TU-search", Name: "search_thread_messages", Input: map[string]any{"query": "approach"}}}}, nil
		case 2:
			return neoInferenceResult{Text: neoReadThreadTestFinalJSON("still too early")}, nil
		case 3:
			if !strings.Contains(neoHistoryTestText(request.History), "read_thread_messages") {
				t.Fatalf("turn %d history missing read correction: %s", turn, neoHistoryTestText(request.History))
			}
			return neoInferenceResult{ToolCalls: []neoToolCall{{ID: "TU-read-old", Name: "read_thread_messages", Input: map[string]any{"startIndex": 0, "count": 1}}}}, nil
		case 4:
			return neoInferenceResult{Text: neoReadThreadTestFinalJSON("missing latest")}, nil
		case 5:
			if !strings.Contains(neoHistoryTestText(request.History), "revisions") {
				t.Fatalf("turn %d history missing latest correction: %s", turn, neoHistoryTestText(request.History))
			}
			return neoInferenceResult{ToolCalls: []neoToolCall{{ID: "TU-read-latest", Name: "read_thread_messages", Input: map[string]any{"latest": true, "count": 2}}}}, nil
		default:
			return neoInferenceResult{Text: neoReadThreadTestFinalJSON("[message 2] approach A was reverted; use approach B.")}, nil
		}
	}
	actor := newNeoActor(rt, "actor-test", "thread-actor", currentThreadID, currentThreadID, neoActorRecord("actor-test", "thread-actor", currentThreadID), nil)
	actor.executorBootstrapComplete = true

	text, err := actor.executeLocalReadThread(neoPendingTool{ID: "TU-read", Name: "read_thread", Input: map[string]any{"threadID": targetThreadID, "question": "Which approach survived?"}, AgentMode: "deep"}, actor.generation)
	if err != nil {
		t.Fatalf("executeLocalReadThread error: %v", err)
	}
	if !strings.Contains(text, "approach B") {
		t.Fatalf("read_thread text = %q, want latest revision", text)
	}
	if len(captured) != 7 {
		t.Fatalf("captured requests = %d, want 7", len(captured))
	}
}

func TestNeoReadThreadCorpusPreservesToolResultStatus(t *testing.T) {
	corpus := neoReadThreadCorpusFromThreadMap("T-tool-status", map[string]any{
		"id": "T-tool-status",
		"messages": []any{
			map[string]any{"role": "assistant", "messageId": "M-tool-use", "content": []any{map[string]any{"type": "tool_use", "id": "TU-edit", "name": "edit_file", "input": map[string]any{"path": "main.go", "old_str": "bad", "new_str": "good"}, "complete": true}}},
			map[string]any{"role": "user", "messageId": "M-tool-result", "content": []any{map[string]any{"type": "tool_result", "toolUseID": "TU-edit", "run": map[string]any{"status": "error", "error": map[string]any{"message": "failed to apply patch"}}}}},
		},
	}, "test")

	run, observation := neoReadThreadExecuteInternalTool(corpus, neoToolCall{Name: "read_thread_messages", Input: map[string]any{"startIndex": 0, "count": 2}})
	if !observation.Read || observation.ReadEnd != 1 {
		t.Fatalf("observation = %#v, want read through message 1", observation)
	}
	text := runToText(run)
	if !strings.Contains(text, "attempted_action") || !strings.Contains(text, "status=error") || !strings.Contains(text, "failed to apply patch") {
		t.Fatalf("tool read result = %s", text)
	}
	if strings.Contains(text, "bad") || strings.Contains(text, "good") {
		t.Fatalf("tool read result leaked edit replacement payloads: %s", text)
	}
}
