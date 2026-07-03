package amp

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
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

func TestNeoReadThreadRangeMatrix(t *testing.T) {
	tests := []struct {
		name        string
		input       map[string]any
		count       int
		wantStart   int
		wantEnd     int
		errContains string
	}{
		{name: "default from start", input: map[string]any{}, count: 20, wantStart: 0, wantEnd: neoReadThreadReadCount - 1},
		{name: "string start and count", input: map[string]any{"startIndex": "3", "count": "2"}, count: 20, wantStart: 3, wantEnd: 4},
		{name: "float start and count truncate", input: map[string]any{"startIndex": 2.9, "count": 2.1}, count: 20, wantStart: 2, wantEnd: 3},
		{name: "json number aliases", input: map[string]any{"messageIndex": json.Number("4"), "limit": json.Number("3")}, count: 20, wantStart: 4, wantEnd: 6},
		{name: "index alias", input: map[string]any{"index": 5, "count": 1}, count: 20, wantStart: 5, wantEnd: 5},
		{name: "latest bool", input: map[string]any{"latest": true, "count": 3}, count: 20, wantStart: 17, wantEnd: 19},
		{name: "latest position", input: map[string]any{"position": " tail ", "count": 4}, count: 20, wantStart: 16, wantEnd: 19},
		{name: "after index", input: map[string]any{"afterIndex": 7, "count": 2}, count: 20, wantStart: 8, wantEnd: 9},
		{name: "negative start clamps", input: map[string]any{"startIndex": -10, "count": 2}, count: 20, wantStart: 0, wantEnd: 1},
		{name: "negative count defaults", input: map[string]any{"startIndex": 2, "count": -5}, count: 20, wantStart: 2, wantEnd: 2 + neoReadThreadReadCount - 1},
		{name: "count caps", input: map[string]any{"startIndex": 10, "count": neoReadThreadReadCountMax + 100}, count: 200, wantStart: 10, wantEnd: 10 + neoReadThreadReadCountMax - 1},
		{name: "end clips to message count", input: map[string]any{"startIndex": 18, "count": 20}, count: 20, wantStart: 18, wantEnd: 19},
		{name: "explicit end overrides count", input: map[string]any{"startIndex": 3, "count": 10, "endIndex": 4}, count: 20, wantStart: 3, wantEnd: 4},
		{name: "start out of range", input: map[string]any{"startIndex": 20}, count: 20, errContains: "outside thread message range"},
		{name: "empty thread out of range", input: map[string]any{}, count: 0, errContains: "outside thread message range"},
		{name: "end before start", input: map[string]any{"startIndex": 8, "endIndex": 7}, count: 20, errContains: "before startIndex"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start, end, err := neoReadThreadRange(tt.input, tt.count)
			if tt.errContains != "" {
				if err == nil || !strings.Contains(err.Error(), tt.errContains) {
					t.Fatalf("err = %v, want containing %q", err, tt.errContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("neoReadThreadRange error: %v", err)
			}
			if start != tt.wantStart || end != tt.wantEnd {
				t.Fatalf("range = %d-%d, want %d-%d", start, end, tt.wantStart, tt.wantEnd)
			}
		})
	}
}

func TestNeoReadThreadSearchTermsMatrix(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  []string
	}{
		{name: "tokenizes allowed path punctuation", query: "Read /api/thread-actors and api/v1:threads plus gpt-5.5", want: []string{"read", "api/thread-actors", "and", "api/v1:threads", "plus", "gpt-5.5"}},
		{name: "trims token edges", query: " _trim. /path/ :key: --dash-- ..dot.. ", want: []string{"trim", "path", "key", "dash", "dot"}},
		{name: "deduplicates case insensitive", query: "Alpha alpha ALPHA beta Beta", want: []string{"alpha", "beta"}},
		{name: "filters one rune terms", query: "a x y id go 7 z_ _q /r/", want: []string{"id", "go"}},
		{name: "keeps unicode letter terms", query: "ÄÖ äö 東京 東", want: []string{"äö", "東京"}},
		{name: "punctuation only", query: " .:/-- _ - / ", want: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := neoReadThreadSearchTerms(tt.query)
			if !slices.Equal(got, tt.want) {
				t.Fatalf("terms = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestNeoReadThreadMergeAdjacentUsersMatrix(t *testing.T) {
	tests := []struct {
		name      string
		history   []neoHistoryMessage
		wantRoles []string
		wantTexts []string
	}{
		{
			name:      "adjacent text users merge",
			history:   []neoHistoryMessage{{Role: "user", Text: " first "}, {Role: "user", Text: " second "}},
			wantRoles: []string{"user"},
			wantTexts: []string{"first\n\nsecond"},
		},
		{
			name:      "assistant boundary prevents merge",
			history:   []neoHistoryMessage{{Role: "user", Text: "first"}, {Role: "assistant", Text: "middle"}, {Role: "user", Text: "second"}},
			wantRoles: []string{"user", "assistant", "user"},
			wantTexts: []string{"first", "middle", "second"},
		},
		{
			name:      "tool call user prevents merge",
			history:   []neoHistoryMessage{{Role: "user", Text: "first"}, {Role: "user", Text: "second", ToolCallID: "TU-tool"}},
			wantRoles: []string{"user", "user"},
			wantTexts: []string{"first", "second"},
		},
		{
			name:      "content user prevents merge",
			history:   []neoHistoryMessage{{Role: "user", Text: "first", Content: []any{map[string]any{"type": "text", "text": "first"}}}, {Role: "user", Text: "second"}},
			wantRoles: []string{"user", "user"},
			wantTexts: []string{"first", "second"},
		},
		{
			name:      "openai items user prevents merge",
			history:   []neoHistoryMessage{{Role: "user", Text: "first", OpenAIItems: []any{map[string]any{"type": "message", "role": "user", "content": "first"}}}, {Role: "user", Text: "second"}},
			wantRoles: []string{"user", "user"},
			wantTexts: []string{"first", "second"},
		},
		{
			name:      "empty adjacent user adopts text",
			history:   []neoHistoryMessage{{Role: "user"}, {Role: "user", Text: "second"}},
			wantRoles: []string{"user"},
			wantTexts: []string{"second"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := neoReadThreadMergeAdjacentUsers(tt.history)
			roles := make([]string, 0, len(got))
			texts := make([]string, 0, len(got))
			for _, message := range got {
				roles = append(roles, message.Role)
				texts = append(texts, message.Text)
			}
			if !slices.Equal(roles, tt.wantRoles) || !slices.Equal(texts, tt.wantTexts) {
				t.Fatalf("history roles/texts = %#v/%#v, want %#v/%#v", roles, texts, tt.wantRoles, tt.wantTexts)
			}
		})
	}
}

func TestNeoReadThreadFinalHistoryFlattensToolContent(t *testing.T) {
	history := neoReadThreadFinalHistory([]neoHistoryMessage{
		{Role: "assistant", ToolCalls: []neoToolCall{{ID: "TU-content", Name: "read_thread_messages"}}},
		{Role: "tool", ToolCallID: "TU-content", ToolName: "read_thread_messages", Content: []any{map[string]any{"type": "text", "text": "content-only tool result"}}},
		{Role: "user", Text: neoReadThreadFinalPrompt},
	})
	if len(history) != 1 || history[0].Role != "user" {
		t.Fatalf("final history = %#v, want one flattened user message", history)
	}
	if !strings.Contains(history[0].Text, "Prior read_thread internal tool result") || !strings.Contains(history[0].Text, "content-only tool result") || !strings.Contains(history[0].Text, neoReadThreadFinalPrompt) {
		t.Fatalf("flattened final history text = %q", history[0].Text)
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

func TestNeoReadThreadAutoReadsLatestWhenBudgetExhausted(t *testing.T) {
	dir := t.TempDir()
	oldStoreDir := neoAmpDataDir
	neoAmpDataDir = func() string { return dir }
	t.Cleanup(func() { neoAmpDataDir = oldStoreDir })

	currentThreadID := "T-019e65c0-0310-77a8-b233-4b84d9c06131"
	targetThreadID := "T-019e65c0-0310-77a8-b233-4b84d9c06132"
	neoReadThreadWriteLocalThread(t, dir, targetThreadID, []any{
		map[string]any{"role": "user", "messageId": "M-early", "content": []any{map[string]any{"type": "text", "text": "Initial plan: keep approach A."}}},
		map[string]any{"role": "assistant", "messageId": "M-middle", "content": []any{map[string]any{"type": "text", "text": "Implementation for approach A is underway."}}},
		map[string]any{"role": "user", "messageId": "M-latest", "content": []any{map[string]any{"type": "text", "text": "Latest decision: approach A is superseded by approach C."}}},
	})

	var captured []neoInferenceRequest
	rt := newNeoRuntime(&config.Config{})
	rt.inferStream = func(_ *neoRuntime, request neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		captured = append(captured, request)
		if len(request.Tools) == 0 {
			historyText := neoHistoryTestText(request.History)
			if !strings.Contains(historyText, "runtime performed the required latest read") || !strings.Contains(historyText, "approach A is superseded by approach C") || !strings.Contains(historyText, neoReadThreadFinalPrompt) {
				t.Fatalf("forced final history missing auto-read latest result: %s", neoHistoryTestText(request.History))
			}
			if request.History[len(request.History)-1].Role != "user" {
				t.Fatalf("forced final auto-read role = %q, want user", request.History[len(request.History)-1].Role)
			}
			for i := 1; i < len(request.History); i++ {
				if request.History[i-1].Role == "user" && request.History[i].Role == "user" {
					t.Fatalf("forced final history has adjacent user messages at %d/%d: %#v", i-1, i, request.History)
				}
			}
			for _, message := range request.History {
				if message.Role == "tool" || len(message.ToolCalls) > 0 {
					t.Fatalf("forced final history kept formal tool messages: %#v", request.History)
				}
			}
			return neoInferenceResult{Text: neoReadThreadTestFinalJSON("[message 2] approach A is superseded by approach C.")}, nil
		}
		switch len(captured) {
		case 1:
			return neoInferenceResult{ToolCalls: []neoToolCall{{ID: newNeoToolCallID(), Name: "search_thread_messages", Input: map[string]any{"query": "approach"}}}}, nil
		case 2:
			return neoInferenceResult{ToolCalls: []neoToolCall{{ID: newNeoToolCallID(), Name: "read_thread_messages", Input: map[string]any{"startIndex": 0, "count": 1}}}}, nil
		case neoReadThreadMaxTurns:
			return neoInferenceResult{ToolCalls: []neoToolCall{{ID: newNeoToolCallID(), Name: "read_thread_messages", Input: map[string]any{"latest": true}, Incomplete: true}}}, nil
		default:
			return neoInferenceResult{Text: neoReadThreadTestFinalJSON("premature answer before tail check")}, nil
		}
	}
	actor := newNeoActor(rt, "actor-test", "thread-actor", currentThreadID, currentThreadID, neoActorRecord("actor-test", "thread-actor", currentThreadID), nil)
	actor.executorBootstrapComplete = true

	text, err := actor.executeLocalReadThread(neoPendingTool{ID: "TU-read", Name: "read_thread", Input: map[string]any{"threadID": targetThreadID, "question": "Which approach survived?"}, AgentMode: "deep"}, actor.generation)
	if err != nil {
		t.Fatalf("executeLocalReadThread error: %v", err)
	}
	if !strings.Contains(text, "approach C") {
		t.Fatalf("read_thread text = %q, want latest decision", text)
	}
	if len(captured) != neoReadThreadMaxTurns+1 {
		t.Fatalf("captured requests = %d, want %d", len(captured), neoReadThreadMaxTurns+1)
	}
}

func TestNeoReadThreadSingleMessageDoesNotNeedAutoLatest(t *testing.T) {
	dir := t.TempDir()
	oldStoreDir := neoAmpDataDir
	neoAmpDataDir = func() string { return dir }
	t.Cleanup(func() { neoAmpDataDir = oldStoreDir })

	currentThreadID := "T-019e65c0-0310-77a8-b233-4b84d9c06135"
	targetThreadID := "T-019e65c0-0310-77a8-b233-4b84d9c06136"
	neoReadThreadWriteLocalThread(t, dir, targetThreadID, []any{
		map[string]any{"role": "user", "messageId": "M-only", "content": []any{map[string]any{"type": "text", "text": "Only decision: use approach Z."}}},
	})

	var captured []neoInferenceRequest
	rt := newNeoRuntime(&config.Config{})
	rt.inferStream = func(_ *neoRuntime, request neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		captured = append(captured, request)
		switch len(captured) {
		case 1:
			return neoInferenceResult{ToolCalls: []neoToolCall{{ID: newNeoToolCallID(), Name: "search_thread_messages", Input: map[string]any{"query": "approach"}}}}, nil
		case 2:
			return neoInferenceResult{ToolCalls: []neoToolCall{{ID: newNeoToolCallID(), Name: "read_thread_messages", Input: map[string]any{"startIndex": 0, "count": 1}}}}, nil
		default:
			if strings.Contains(neoHistoryTestText(request.History), "runtime performed the required latest read") {
				t.Fatalf("single-message thread should not need auto latest read: %#v", request.History)
			}
			return neoInferenceResult{Text: neoReadThreadTestFinalJSON("[message 0] use approach Z.")}, nil
		}
	}
	actor := newNeoActor(rt, "actor-test", "thread-actor", currentThreadID, currentThreadID, neoActorRecord("actor-test", "thread-actor", currentThreadID), nil)
	actor.executorBootstrapComplete = true

	text, err := actor.executeLocalReadThread(neoPendingTool{ID: "TU-read", Name: "read_thread", Input: map[string]any{"threadID": targetThreadID, "question": "Which approach?"}, AgentMode: "deep"}, actor.generation)
	if err != nil {
		t.Fatalf("executeLocalReadThread error: %v", err)
	}
	if !strings.Contains(text, "approach Z") {
		t.Fatalf("read_thread text = %q, want single-message answer", text)
	}
	if len(captured) != 3 {
		t.Fatalf("captured requests = %d, want 3", len(captured))
	}
}

func TestNeoReadThreadBudgetExhaustionStaleGenerationReturnsGracefully(t *testing.T) {
	dir := t.TempDir()
	oldStoreDir := neoAmpDataDir
	neoAmpDataDir = func() string { return dir }
	t.Cleanup(func() { neoAmpDataDir = oldStoreDir })

	currentThreadID := "T-019e65c0-0310-77a8-b233-4b84d9c06133"
	targetThreadID := "T-019e65c0-0310-77a8-b233-4b84d9c06134"
	neoReadThreadWriteLocalThread(t, dir, targetThreadID, []any{
		map[string]any{"role": "user", "messageId": "M-early", "content": []any{map[string]any{"type": "text", "text": "Initial plan: keep approach A."}}},
		map[string]any{"role": "user", "messageId": "M-latest", "content": []any{map[string]any{"type": "text", "text": "Latest decision: approach B."}}},
	})

	var actor *neoActor
	var captured []neoInferenceRequest
	rt := newNeoRuntime(&config.Config{})
	rt.inferStream = func(_ *neoRuntime, request neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		if len(request.Tools) == 0 {
			t.Fatal("forced final should not run after stale generation")
		}
		captured = append(captured, request)
		if len(captured) == neoReadThreadMaxTurns {
			actor.mu.Lock()
			actor.generation++
			actor.mu.Unlock()
		}
		if len(captured)%2 == 1 {
			return neoInferenceResult{ToolCalls: []neoToolCall{{ID: newNeoToolCallID(), Name: "search_thread_messages", Input: map[string]any{"query": "approach"}}}}, nil
		}
		return neoInferenceResult{ToolCalls: []neoToolCall{{ID: newNeoToolCallID(), Name: "read_thread_messages", Input: map[string]any{"startIndex": 0, "count": 1}}}}, nil
	}
	actor = newNeoActor(rt, "actor-test", "thread-actor", currentThreadID, currentThreadID, neoActorRecord("actor-test", "thread-actor", currentThreadID), nil)
	actor.executorBootstrapComplete = true

	text, err := actor.executeLocalReadThread(neoPendingTool{ID: "TU-read", Name: "read_thread", Input: map[string]any{"threadID": targetThreadID, "question": "Which approach survived?"}, AgentMode: "deep"}, actor.generation)
	if err != nil {
		t.Fatalf("executeLocalReadThread error: %v", err)
	}
	if text != "" {
		t.Fatalf("read_thread text = %q, want empty after stale generation", text)
	}
	if len(captured) != neoReadThreadMaxTurns {
		t.Fatalf("captured requests = %d, want %d", len(captured), neoReadThreadMaxTurns)
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
