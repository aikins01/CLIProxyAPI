package amp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
)

type neoReadThreadCancelAfterContext struct {
	context.Context
	cancel context.CancelFunc
	checks int
}

func newNeoReadThreadCancelAfterContext(checks int) *neoReadThreadCancelAfterContext {
	ctx, cancel := context.WithCancel(context.Background())
	return &neoReadThreadCancelAfterContext{Context: ctx, cancel: cancel, checks: checks}
}

func (c *neoReadThreadCancelAfterContext) Err() error {
	c.checks--
	if c.checks <= 0 {
		c.cancel()
	}
	return c.Context.Err()
}

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
	if request.Context == nil {
		t.Fatal("read_thread request is missing its cancellation context")
	}
	route := request.ModelRouteOverride
	if route == nil || route.Provider != neoReadThreadAgentProvider || route.Model != neoReadThreadAgentModel {
		t.Fatalf("read_thread route = %#v, want %s/%s", route, neoReadThreadAgentProvider, neoReadThreadAgentModel)
	}
	if request.ReasoningEffort != neoReadThreadAgentEffort || stringValue(request.Settings["reasoning.effort"]) != neoReadThreadAgentEffort {
		t.Fatalf("read_thread effort request=%q settings=%#v, want %s", request.ReasoningEffort, request.Settings, neoReadThreadAgentEffort)
	}
	if _, ok := request.Settings["gemini.thinkingLevel"]; ok {
		t.Fatalf("read_thread settings = %#v, want no Gemini thinking level for %s/%s", request.Settings, neoReadThreadAgentProvider, neoReadThreadAgentModel)
	}
	if request.DisableSystemPrompt || request.DisableProviderReasoning {
		t.Fatalf("read_thread disabled flags system=%v providerReasoning=%v, want false", request.DisableSystemPrompt, request.DisableProviderReasoning)
	}
	if request.ProviderFeature != "amp.read-thread" {
		t.Fatalf("provider feature = %q, want amp.read-thread", request.ProviderFeature)
	}
	if request.DisableParallelToolCalls {
		t.Fatal("read_thread request disabled parallel tool calls")
	}
	if !strings.Contains(request.SystemPromptOverride, "read_thread subagent") {
		t.Fatalf("system prompt override = %q", request.SystemPromptOverride)
	}
	if len(request.Tools) > 0 && !neoReadThreadHasAnyTool(request.Tools, "thread_overview", "search_thread_messages", "read_thread_messages") {
		t.Fatalf("tools = %#v, want a read_thread retrieval tool", request.Tools)
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

func neoReadThreadHasAnyTool(tools []neoToolSpec, names ...string) bool {
	for _, tool := range tools {
		if slices.Contains(names, tool.Name) {
			return true
		}
	}
	return false
}

func TestNeoReadThreadAgentClearsInheritedGeminiThinkingLevel(t *testing.T) {
	corpus := neoReadThreadCorpus{ThreadID: "T-inherited-gemini", Source: "test", Title: "inherited gemini", Messages: []neoReadThreadMessage{
		{Index: 0, Role: "user", MessageID: "M-0", Text: "The final answer must cite [message 0]."},
	}}
	var captured []neoInferenceRequest
	rt := newNeoRuntime(&config.Config{})
	rt.inferStream = neoReadThreadScriptedInfer(t, "[message 0] inherited Gemini level was ignored.", "Gemini level", &captured, nil)
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-current", "T-current", neoActorRecord("actor-test", "thread-actor", "T-current"), nil)
	actor.mu.Lock()
	actor.settings["gemini.thinkingLevel"] = "minimal"
	actor.settings["reasoning.effort"] = "low"
	actor.mu.Unlock()

	text, err := actor.executeLocalReadThreadAgent(neoPendingTool{ID: "TU-read", Name: "read_thread", Input: map[string]any{"threadID": corpus.ThreadID, "question": "Check inherited Gemini thinking."}, AgentMode: "deep"}, actor.generation, corpus, "Check inherited Gemini thinking.")
	if err != nil {
		t.Fatalf("executeLocalReadThreadAgent error: %v", err)
	}
	if !strings.Contains(text, "inherited Gemini level was ignored") {
		t.Fatalf("read_thread text = %q, want final content", text)
	}
	if len(captured) == 0 {
		t.Fatal("captured no read_thread requests")
	}
	for _, request := range captured {
		neoReadThreadAssertAgentRequest(t, request)
		if request.ModelRouteOverride == nil {
			t.Fatalf("missing route override: %#v", request)
		}
		if got := neoProviderReasoningEffort(request, *request.ModelRouteOverride); got != neoReadThreadAgentEffort {
			t.Fatalf("provider reasoning effort = %q, want %q for settings %#v", got, neoReadThreadAgentEffort, request.Settings)
		}
	}
}

func TestNeoReadThreadSubagentInferRetriesTransientProviderOverload(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	attempts := 0
	var requests []neoInferenceRequest
	rt.inferStream = func(_ *neoRuntime, request neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		attempts++
		requests = append(requests, request)
		if attempts == 1 {
			return neoInferenceResult{}, fmt.Errorf("local provider stream error: %s", `{"error":{"type":"service_unavailable_error","code":"server_is_overloaded"}}`)
		}
		return neoInferenceResult{Text: "done"}, nil
	}
	request := neoInferenceRequest{ThreadID: "T-read-thread-retry", ProviderFeature: "amp.read-thread"}

	result, err := neoReadThreadSubagentInfer(rt, request)
	if err != nil {
		t.Fatalf("neoReadThreadSubagentInfer error: %v", err)
	}
	if result.Text != "done" || attempts != 2 {
		t.Fatalf("result=%#v attempts=%d, want successful second attempt", result, attempts)
	}
	if !reflect.DeepEqual(requests[0], requests[1]) {
		t.Fatalf("retry changed request\nfirst=%#v\nsecond=%#v", requests[0], requests[1])
	}
}

func TestNeoReadThreadSubagentInferDoesNotRetryOtherErrors(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	attempts := 0
	rt.inferStream = func(_ *neoRuntime, _ neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		attempts++
		return neoInferenceResult{}, fmt.Errorf("usage limit reached")
	}

	if _, err := neoReadThreadSubagentInfer(rt, neoInferenceRequest{}); err == nil {
		t.Fatal("neoReadThreadSubagentInfer error = nil, want provider error")
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want no retry for non-transient error", attempts)
	}
}

func TestNeoReadThreadSubagentInferDoesNotRetryStructuredDeterministicError(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	attempts := 0
	rt.inferStream = func(_ *neoRuntime, _ neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		attempts++
		return neoInferenceResult{}, errors.New(`local provider stream error: {"error":{"type":"invalid_request_error","message":"request value server_is_overloaded is invalid"}}`)
	}

	if _, err := neoReadThreadSubagentInfer(rt, neoInferenceRequest{}); err == nil {
		t.Fatal("neoReadThreadSubagentInfer error = nil, want deterministic provider error")
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want no retry for deterministic structured error", attempts)
	}
}

func TestNeoReadThreadSubagentInferDoesNotRetryAfterCancellation(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	ctx, cancel := context.WithCancel(context.Background())
	attempts := 0
	rt.inferStream = func(_ *neoRuntime, _ neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		attempts++
		cancel()
		return neoInferenceResult{}, errors.New(`local provider stream error: {"error":{"type":"overloaded_error"}}`)
	}

	_, err := neoReadThreadSubagentInfer(rt, neoInferenceRequest{Context: ctx})
	if !errors.Is(err, context.Canceled) || attempts != 1 {
		t.Fatalf("cancelled retry error=%v attempts=%d, want context cancellation without retry", err, attempts)
	}
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

func TestNeoReadThreadRejectsForeignOwnedLocalCorpus(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	source := rt.store.ensureThreadActor("T-019f7000-0000-7000-8000-0000000000a1")
	source.mu.Lock()
	source.currentAgentMode = "puck"
	source.meta["ownerUserId"] = "user-a"
	source.mu.Unlock()
	liveTarget := rt.store.ensureThreadActor("T-019f7000-0000-7000-8000-0000000000a2")
	liveTarget.mu.Lock()
	liveTarget.meta["ownerUserId"] = "user-b"
	liveTarget.messages = []neoMessage{{MessageID: "M-foreign-live", Role: "user", Content: []any{map[string]any{"type": "text", "text": "foreign live secret"}}}}
	liveTarget.mu.Unlock()
	if corpus, ok := source.liveReadThreadCorpus(liveTarget.threadID); ok {
		t.Fatalf("foreign live corpus = %#v", corpus)
	}

	persistedThreadID := "T-019f7000-0000-7000-8000-0000000000a3"
	if err := writeNeoLocalThreadSnapshotToDir(neoCloudThreadSnapshot{
		threadID: persistedThreadID,
		meta: map[string]any{
			"cliProxyAPILocalNeo": true,
			"ownerUserId":         "user-b",
		},
		messages: []neoMessage{{MessageID: "M-foreign-file", Role: "user", Content: []any{map[string]any{"type": "text", "text": "foreign persisted secret"}}}},
	}, rt.threadDir); err != nil {
		t.Fatal(err)
	}
	if corpus, ok := source.localReadThreadFileCorpus(persistedThreadID); ok {
		t.Fatalf("foreign persisted corpus = %#v", corpus)
	}
}

func TestNeoReadThreadLiveCorpusConversionHonorsCancellation(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	source := rt.store.ensureThreadActor("T-019f7000-0000-7000-8000-0000000000b1")
	target := rt.store.ensureThreadActor("T-019f7000-0000-7000-8000-0000000000b2")
	source.mu.Lock()
	source.meta["ownerUserId"] = "user-a"
	source.mu.Unlock()
	target.mu.Lock()
	target.meta["ownerUserId"] = "user-a"
	target.messages = []neoMessage{{MessageID: "M-live", Role: "user", Content: []any{map[string]any{"type": "text", "text": "live content"}}}}
	target.mu.Unlock()

	corpus, ok, err := source.liveReadThreadCorpusContext(newNeoReadThreadCancelAfterContext(3), target.threadID)
	if !errors.Is(err, context.Canceled) || ok || len(corpus.Messages) != 0 {
		t.Fatalf("cancelled live corpus = %#v, ok=%v, err=%v", corpus, ok, err)
	}
}

func TestNeoReadThreadFileCorpusConversionHonorsCancellation(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	source := rt.store.ensureThreadActor("T-019f7000-0000-7000-8000-0000000000b3")
	source.mu.Lock()
	source.meta["ownerUserId"] = "user-a"
	source.mu.Unlock()
	threadID := "T-019f7000-0000-7000-8000-0000000000b4"
	if err := writeNeoLocalThreadSnapshotToDir(neoCloudThreadSnapshot{
		threadID: threadID,
		meta:     map[string]any{"cliProxyAPILocalNeo": true, "ownerUserId": "user-a"},
		messages: []neoMessage{{MessageID: "M-file", Role: "user", Content: []any{map[string]any{"type": "text", "text": "file content"}}}},
	}, rt.threadDir); err != nil {
		t.Fatal(err)
	}

	corpus, ok, err := source.localReadThreadFileCorpusContext(newNeoReadThreadCancelAfterContext(5), threadID)
	if !errors.Is(err, context.Canceled) || ok || len(corpus.Messages) != 0 {
		t.Fatalf("cancelled file corpus = %#v, ok=%v, err=%v", corpus, ok, err)
	}
}

func TestNeoReadThreadFileCorpusUsesRuntimeLegacyOwnerMigration(t *testing.T) {
	dataDir := t.TempDir()
	oldDataDir := neoAmpDataDir
	neoAmpDataDir = func() string { return dataDir }
	t.Cleanup(func() { neoAmpDataDir = oldDataDir })
	threadDir := filepath.Join(dataDir, "threads")
	threadID := "T-019f7000-0000-7000-8000-0000000000a4"
	if err := writeNeoLocalThreadSnapshotToDir(neoCloudThreadSnapshot{
		threadID: threadID,
		meta: map[string]any{
			"cliProxyAPILocalNeo": true,
			"ownerUserId":         neoLocalOwnerUserID,
		},
		messages: []neoMessage{{MessageID: "M-legacy-file", Role: "user", Content: []any{map[string]any{"type": "text", "text": "legacy persisted content"}}}},
	}, threadDir); err != nil {
		t.Fatal(err)
	}
	withoutRuntime := &neoActor{meta: map[string]any{"ownerUserId": "user-a"}}
	if corpus, ok := withoutRuntime.localReadThreadFileCorpus(threadID); ok {
		t.Fatalf("base ownership unexpectedly accepted legacy corpus: %#v", corpus)
	}
	rt := newNeoRuntime(&config.Config{})
	rt.threadDir = threadDir
	rt.legacyOwnerMigrationUser = "user-a"
	withRuntime := &neoActor{runtime: rt, meta: map[string]any{"ownerUserId": "user-a"}}
	if corpus, ok := withRuntime.localReadThreadFileCorpus(threadID); !ok || len(corpus.Messages) != 1 {
		t.Fatalf("runtime legacy ownership corpus = %#v, ok=%v", corpus, ok)
	}
	persisted, ok := loadNeoThreadFromDir(threadID, threadDir)
	if !ok || neoThreadOwnerUserID(persisted) != "user-a" {
		t.Fatalf("legacy corpus owner was not durably claimed: %#v", persisted)
	}
	reloaded := newNeoRuntime(&config.Config{})
	reloaded.threadDir = threadDir
	foreign := &neoActor{runtime: reloaded, meta: map[string]any{"ownerUserId": "user-b"}}
	if corpus, ok := foreign.localReadThreadFileCorpus(threadID); ok {
		t.Fatalf("foreign owner read claimed legacy corpus: %#v", corpus)
	}
}

func TestNeoReadThreadFileCorpusNormalizesOwnedDataEnvelope(t *testing.T) {
	dir := t.TempDir()
	threadID := "T-019f7000-0000-7000-8000-0000000000a5"
	raw, err := json.Marshal(map[string]any{
		"title": "enveloped thread",
		"meta":  map[string]any{"ownerUserId": neoLocalOwnerUserID},
		"data": map[string]any{
			"id":   threadID,
			"meta": map[string]any{"ownerUserId": nil},
			"messages": []any{
				map[string]any{"role": "user", "messageId": "M-envelope", "content": []any{map[string]any{"type": "text", "text": "enveloped content"}}},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, threadID+".json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	actor := &neoActor{
		runtime: &neoRuntime{threadDir: dir},
		meta:    map[string]any{"ownerUserId": neoLocalOwnerUserID},
	}
	corpus, ok := actor.localReadThreadFileCorpus(threadID)
	if !ok || corpus.Title != "enveloped thread" || len(corpus.Messages) != 1 || corpus.Messages[0].MessageID != "M-envelope" || corpus.Messages[0].Text != "enveloped content" {
		t.Fatalf("enveloped corpus = %#v, ok=%v", corpus, ok)
	}
}

func TestNeoReadThreadSnapshotCorpusMatchesCloudThreadMap(t *testing.T) {
	threadID := "T-019f7000-0000-7000-8000-0000000000a6"
	snapshot := neoCloudThreadSnapshot{
		threadID: threadID,
		messages: []neoMessage{
			{Role: "user", MessageID: "M-root", CreatedAt: "2026-07-23T12:00:00Z", Content: []any{map[string]any{"type": "text", "text": "root message"}}},
			{Role: "assistant", MessageID: "M-tool", CompletionStatus: "complete", Content: []any{map[string]any{"type": "tool_use", "id": "TU-read", "name": "Read", "input": map[string]any{"path": "main.go"}, "complete": true}}},
			{Role: "user", MessageID: "M-nested", ParentToolUseID: "TU-read", Content: []any{map[string]any{"type": "text", "text": "nested message"}}},
			{Role: "assistant", MessageID: " ", CreatedAt: " ", ParentToolUseID: " ", Content: []any{map[string]any{"type": "text", "text": "blank metadata message"}}},
			{Role: "user", MessageID: "M-result", Content: []any{map[string]any{"type": "tool_result", "toolUseID": "TU-read", "run": map[string]any{"status": "error", "error": map[string]any{"message": "read failed"}}}}},
		},
		queuedMessages: []any{
			map[string]any{"role": "user", "content": []any{map[string]any{"type": "text", "text": "queued message"}}},
			map[string]any{"role": "user", "parentToolCallId": "TU-read", "content": []any{map[string]any{"type": "text", "text": "nested queued message"}}},
		},
	}
	want := neoReadThreadCorpusFromThreadMap(threadID, neoCloudThread(snapshot), "local-live")
	got := neoReadThreadCorpusFromSnapshot(threadID, snapshot, "local-live")
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("snapshot corpus = %#v, want %#v", got, want)
	}
}

func TestNeoReadThreadDefaultsToCurrentThread(t *testing.T) {
	threadID := "T-019e65c0-0310-77a8-b233-4b84d9c06140"
	var captured []neoInferenceRequest
	rt := newNeoRuntime(&config.Config{})
	rt.inferStream = neoReadThreadScriptedInfer(t, "[message 0] current thread context", "current thread", &captured, nil)
	actor := newNeoActor(rt, "actor-test", "thread-actor", threadID, threadID, neoActorRecord("actor-test", "thread-actor", threadID), nil)
	actor.messages = []neoMessage{
		{ThreadID: threadID, MessageID: "M-current", Role: "user", Content: []any{map[string]any{"type": "text", "text": "current thread context"}}, Seq: 1},
	}

	text, err := actor.executeLocalReadThread(neoPendingTool{ID: "TU-read", Name: "read_thread", Input: map[string]any{"goal": "Extract current thread context."}, AgentMode: "deep"}, actor.generation)
	if err != nil {
		t.Fatalf("executeLocalReadThread error: %v", err)
	}
	if !strings.Contains(text, "current thread context") {
		t.Fatalf("read_thread text = %q, want current thread content", text)
	}
	if len(captured) == 0 || !strings.Contains(captured[0].History[0].Text, "Thread ID: "+threadID) {
		t.Fatalf("read_thread initial request did not target current thread: %#v", captured)
	}
}

func TestNeoReadThreadPromptsAvoidCopyableFinalPlaceholder(t *testing.T) {
	combined := neoReadThreadAgentSystemPrompt + "\n" + neoReadThreadFinalPrompt
	if strings.Contains(combined, `"markdown text"`) {
		t.Fatalf("read_thread prompts contain copyable final placeholder: %s", combined)
	}
	if !strings.Contains(combined, "do not return placeholder") {
		t.Fatalf("read_thread prompts do not explicitly reject placeholder output: %s", combined)
	}
	if !strings.Contains(combined, "not nested JSON") {
		t.Fatalf("read_thread prompts do not reject nested JSON final content: %s", combined)
	}
	if !strings.Contains(combined, "Copy relevant identifiers verbatim") {
		t.Fatalf("read_thread prompts do not require verbatim identifiers: %s", combined)
	}
	if !strings.Contains(combined, "Do not mention superseded or irrelevant topics") {
		t.Fatalf("read_thread prompts do not suppress superseded-topic resurfacing: %s", combined)
	}
}

func TestNeoReadThreadCorpusUsesVisibleRootMessages(t *testing.T) {
	threadID := "T-visible-reader"
	thread := map[string]any{
		"id":    threadID,
		"title": "visible reader",
		"messages": []any{
			map[string]any{"role": "user", "messageId": "M-root-0", "content": []any{map[string]any{"type": "text", "text": "start shipping PR #1003"}}},
			map[string]any{"role": "assistant", "messageId": "M-root-1", "content": []any{map[string]any{"type": "tool_use", "id": "TU-read", "name": "read_thread", "input": map[string]any{"goal": "continue"}}}},
			map[string]any{"role": "user", "messageId": "M-child-0", "parentToolUseId": "TU-read", "content": []any{map[string]any{"type": "text", "text": "hidden stale subagent observation: wait for cooldown"}}},
			map[string]any{"role": "user", "messageId": "M-root-2", "content": []any{map[string]any{"type": "tool_result", "toolUseID": "TU-read", "run": map[string]any{"status": "done", "result": "handoff extracted"}}}},
			map[string]any{"role": "user", "messageId": "M-child-1", "parentToolUseID": "TU-read", "content": []any{map[string]any{"type": "text", "text": "hidden tail noise: commit and push still pending"}}},
			map[string]any{"role": "assistant", "messageId": "M-root-3", "content": []any{map[string]any{"type": "text", "text": "PR #1003 was merged and local main is synced."}}},
		},
		"queuedMessages": []any{
			map[string]any{"role": "user", "parentToolCallId": "TU-read", "content": []any{map[string]any{"type": "text", "text": "hidden queued child"}}},
			map[string]any{"role": "user", "content": []any{map[string]any{"type": "text", "text": "visible queued follow-up"}}},
		},
	}

	corpus := neoReadThreadCorpusFromThreadMap(threadID, thread, "test")
	if len(corpus.Messages) != 5 {
		t.Fatalf("visible message count = %d, want 5: %#v", len(corpus.Messages), corpus.Messages)
	}
	rendered := fmt.Sprint(corpus.Messages)
	if strings.Contains(rendered, "hidden stale") || strings.Contains(rendered, "hidden tail noise") || strings.Contains(rendered, "hidden queued child") {
		t.Fatalf("corpus included nested subagent messages: %#v", corpus.Messages)
	}
	if corpus.Messages[3].Index != 5 || !strings.Contains(corpus.Messages[3].Text, "merged") || corpus.Messages[4].Index != 7 {
		t.Fatalf("visible indexes/tail = %#v", corpus.Messages)
	}

	overview := neoReadThreadOverview(corpus)
	if numberFrom(overview["latestMessageIndex"]) != 7 {
		t.Fatalf("overview latestMessageIndex = %#v, want original visible index 7", overview["latestMessageIndex"])
	}
	raw, _ := json.Marshal(overview["latestMessages"])
	if !strings.Contains(string(raw), "PR #1003 was merged") || strings.Contains(string(raw), "commit and push still pending") {
		t.Fatalf("overview latest messages = %s", raw)
	}

	read, end, err := neoReadThreadRead(corpus, map[string]any{"startIndex": 5, "count": 2})
	if err != nil {
		t.Fatalf("read original range: %v", err)
	}
	if end != 7 {
		t.Fatalf("read end = %d, want latest original index 7", end)
	}
	readRange := mapValue(read["range"])
	if numberFrom(readRange["startIndex"]) != 5 || numberFrom(readRange["endIndex"]) != 7 {
		t.Fatalf("read range = %#v, want original indexes 5-7", readRange)
	}
	latestRead, latestEnd, err := neoReadThreadRead(corpus, map[string]any{"latest": true, "startIndex": 0, "count": 2})
	if err != nil {
		t.Fatalf("read mixed latest/start range: %v", err)
	}
	if latestEnd != 7 {
		t.Fatalf("latest mixed end = %d, want latest original index 7", latestEnd)
	}
	latestRange := mapValue(latestRead["range"])
	if numberFrom(latestRange["startIndex"]) != 5 || numberFrom(latestRange["endIndex"]) != 7 {
		t.Fatalf("latest mixed range = %#v, want original indexes 5-7", latestRange)
	}
	sparseRead, sparseEnd, err := neoReadThreadRead(corpus, map[string]any{"startIndex": 4, "endIndex": 4})
	if err != nil {
		t.Fatalf("read sparse gap: %v", err)
	}
	sparseRange := mapValue(sparseRead["range"])
	if sparseEnd != 5 || numberFrom(sparseRange["startIndex"]) != 3 || numberFrom(sparseRange["endIndex"]) != 5 || !boolValue(sparseRead["sparseRangeNormalized"]) {
		t.Fatalf("sparse read = %#v end=%d, want normalized boundary range 3-5", sparseRead, sparseEnd)
	}
	if _, _, err := neoReadThreadRead(corpus, map[string]any{"startIndex": 99}); err == nil || !strings.Contains(err.Error(), "outside thread message range") {
		t.Fatalf("read past sparse tail err = %v, want out-of-range", err)
	}
	if _, _, err := neoReadThreadRead(corpus, map[string]any{"afterIndex": 99}); err == nil || !strings.Contains(err.Error(), "outside thread message range") {
		t.Fatalf("read after sparse tail err = %v, want out-of-range", err)
	}
}

func TestNeoReadThreadLatestReadKeepsDenseTail(t *testing.T) {
	messages := make([]neoReadThreadMessage, neoReadThreadLatestReadCount)
	for i := range messages {
		messages[i] = neoReadThreadMessage{
			Index:     i,
			Role:      "assistant",
			MessageID: fmt.Sprintf("M-dense-%02d", i),
			Text:      fmt.Sprintf("dense message %02d %s", i, strings.Repeat("payload ", 2500)),
		}
	}
	messages[len(messages)-1].Text = "LATEST_DENSE_TAIL_DECISION=ship-suffix-reader " + messages[len(messages)-1].Text
	corpus := neoReadThreadCorpus{ThreadID: "T-dense-tail", Source: "test", Messages: messages}

	read, end, err := neoReadThreadRead(corpus, map[string]any{"latest": true, "count": neoReadThreadLatestReadCount})
	if err != nil {
		t.Fatalf("latest dense read error: %v", err)
	}
	if end != len(messages)-1 {
		t.Fatalf("latest dense read end = %d, want %d", end, len(messages)-1)
	}
	readMessages := arrayValue(read["messages"])
	if len(readMessages) == 0 || len(readMessages) >= len(messages) {
		t.Fatalf("latest dense messages = %d, want a non-empty byte-bounded suffix", len(readMessages))
	}
	last := mapValue(readMessages[len(readMessages)-1])
	if numberFrom(last["index"]) != len(messages)-1 || !strings.Contains(stringValue(last["text"]), "LATEST_DENSE_TAIL_DECISION") {
		t.Fatalf("latest dense tail = %#v, want actual final message", last)
	}
	readRange := mapValue(read["range"])
	if numberFrom(readRange["startIndex"]) <= 0 || numberFrom(readRange["endIndex"]) != len(messages)-1 || !boolValue(read["truncated"]) {
		t.Fatalf("latest dense range = %#v truncated=%#v", readRange, read["truncated"])
	}
	if numberFrom(read["previousEndIndex"]) != numberFrom(readRange["startIndex"])-1 {
		t.Fatalf("previousEndIndex = %#v range=%#v", read["previousEndIndex"], readRange)
	}
}

func TestNeoReadThreadAgentDeduplicatesCalls(t *testing.T) {
	corpus := neoReadThreadCorpus{ThreadID: "T-tool-budget", Source: "test", Messages: []neoReadThreadMessage{
		{Index: 0, Role: "user", Text: "alpha decision"},
		{Index: 1, Role: "assistant", Text: "latest alpha outcome"},
	}}
	recoveryState := neoReadThreadAgentState{}
	prematureRead := neoToolCall{ID: "TU-read-premature", Name: "read_thread_messages", Input: map[string]any{"latest": true, "count": 2}}
	if run, accepted := neoReadThreadExecuteAgentTool(&recoveryState, corpus, prematureRead); !accepted || stringValue(run["status"]) != "error" || len(recoveryState.SeenCalls) != 0 {
		t.Fatalf("premature read accepted=%v state=%#v run=%#v", accepted, recoveryState, run)
	}
	if run, accepted := neoReadThreadExecuteAgentTool(&recoveryState, corpus, neoToolCall{ID: "TU-search-recovery", Name: "search_thread_messages", Input: map[string]any{"query": "alpha"}}); !accepted || stringValue(run["status"]) != "done" {
		t.Fatalf("recovery search accepted=%v run=%#v", accepted, run)
	}
	if run, accepted := neoReadThreadExecuteAgentTool(&recoveryState, corpus, prematureRead); !accepted || stringValue(run["status"]) != "done" || !recoveryState.SawLatest {
		t.Fatalf("recovered read accepted=%v state=%#v run=%#v", accepted, recoveryState, run)
	}

	state := neoReadThreadAgentState{}
	search := neoToolCall{ID: "TU-search-1", Name: "search_thread_messages", Input: map[string]any{"query": " Alpha   Decision ", "limit": 10}}
	if run, accepted := neoReadThreadExecuteAgentTool(&state, corpus, search); !accepted || stringValue(run["status"]) != "done" {
		t.Fatalf("first search accepted=%v run=%#v", accepted, run)
	}
	normalizedSearch, err := neoReadThreadSearch(corpus, search.Input)
	if err != nil || stringValue(normalizedSearch["query"]) != "Alpha Decision" {
		t.Fatalf("normalized search query=%#v err=%v", normalizedSearch["query"], err)
	}
	duplicate := neoToolCall{ID: "TU-search-2", Name: "search_thread_messages", Input: map[string]any{"query": "alpha decision", "limit": 20}}
	if run, accepted := neoReadThreadExecuteAgentTool(&state, corpus, duplicate); accepted || !strings.Contains(runToText(run), "duplicate") {
		t.Fatalf("duplicate search accepted=%v run=%#v", accepted, run)
	}
	read := neoToolCall{ID: "TU-read-1", Name: "read_thread_messages", Input: map[string]any{"latest": true, "count": 40}}
	if run, accepted := neoReadThreadExecuteAgentTool(&state, corpus, read); !accepted || stringValue(run["status"]) != "done" || !state.SawLatest {
		t.Fatalf("latest read accepted=%v state=%#v run=%#v", accepted, state, run)
	}
	equivalentTailRead := neoToolCall{ID: "TU-read-2", Name: "read_thread_messages", Input: map[string]any{"position": "tail", "count": 2}}
	if run, accepted := neoReadThreadExecuteAgentTool(&state, corpus, equivalentTailRead); accepted || !strings.Contains(runToText(run), "duplicate") {
		t.Fatalf("equivalent tail read accepted=%v run=%#v", accepted, run)
	}
	forwardRead := neoToolCall{ID: "TU-read-3", Name: "read_thread_messages", Input: map[string]any{"startIndex": 0, "count": 1}}
	if run, accepted := neoReadThreadExecuteAgentTool(&state, corpus, forwardRead); accepted || !strings.Contains(runToText(run), "already covered") {
		t.Fatalf("covered forward read accepted=%v run=%#v", accepted, run)
	}
	unique := neoToolCall{ID: "TU-search-3", Name: "search_thread_messages", Input: map[string]any{"query": "latest alpha"}}
	if run, accepted := neoReadThreadExecuteAgentTool(&state, corpus, unique); !accepted || stringValue(run["status"]) != "done" {
		t.Fatalf("additional same-turn search accepted=%v run=%#v", accepted, run)
	}
}

func TestNeoReadThreadAgentKeepsDeduplicationAfterHistoryTrim(t *testing.T) {
	corpus := neoReadThreadCorpus{ThreadID: "T-trimmed-dedup", Source: "test", Messages: []neoReadThreadMessage{
		{Index: 0, Role: "user", Text: "alpha decision"},
		{Index: 1, Role: "assistant", Text: "latest alpha outcome"},
	}}
	state := neoReadThreadAgentState{}
	search := neoToolCall{ID: "TU-search-first", Name: "search_thread_messages", Input: map[string]any{"query": "alpha decision"}}
	if run, accepted := neoReadThreadExecuteAgentTool(&state, corpus, search); !accepted || stringValue(run["status"]) != "done" {
		t.Fatalf("initial search accepted=%v run=%#v", accepted, run)
	}
	read := neoToolCall{ID: "TU-read-first", Name: "read_thread_messages", Input: map[string]any{"latest": true, "count": 2}}
	if run, accepted := neoReadThreadExecuteAgentTool(&state, corpus, read); !accepted || stringValue(run["status"]) != "done" {
		t.Fatalf("initial read accepted=%v run=%#v", accepted, run)
	}
	_ = neoReadThreadRequestHistory([]neoHistoryMessage{
		{Role: "user", Text: strings.Repeat("old history ", neoReadThreadHistoryTotalChars)},
		{Role: "tool", ToolCallID: "TU-newer", Text: "newer retained result"},
	})
	if run, accepted := neoReadThreadExecuteAgentTool(&state, corpus, neoToolCall{ID: "TU-search-repeat", Name: search.Name, Input: search.Input}); accepted || !strings.Contains(runToText(run), "duplicate") {
		t.Fatalf("trimmed duplicate search accepted=%v run=%#v", accepted, run)
	}
	if run, accepted := neoReadThreadExecuteAgentTool(&state, corpus, neoToolCall{ID: "TU-read-repeat", Name: read.Name, Input: read.Input}); accepted || !strings.Contains(runToText(run), "duplicate") {
		t.Fatalf("trimmed duplicate read accepted=%v run=%#v", accepted, run)
	}
}

func TestNeoReadThreadAcceptsMoreThanFormerToolCallBudget(t *testing.T) {
	messages := make([]neoReadThreadMessage, 50)
	for i := range messages {
		messages[i] = neoReadThreadMessage{Index: i, Role: "assistant", MessageID: fmt.Sprintf("M-%d", i), Text: fmt.Sprintf("roadmap evidence %d", i)}
	}
	corpus := neoReadThreadCorpus{ThreadID: "T-reserved-final", Source: "test", Messages: messages}
	var captured []neoInferenceRequest
	rt := newNeoRuntime(&config.Config{})
	rt.inferStream = func(_ *neoRuntime, request neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		turn := len(captured)
		captured = append(captured, request)
		if len(request.Tools) == 0 {
			if request.ResponseMimeType != "application/json" || len(request.ResponseJSONSchema) == 0 {
				t.Fatalf("forced final response contract = mime:%q schema:%#v", request.ResponseMimeType, request.ResponseJSONSchema)
			}
			historyText := neoHistoryTestText(request.History)
			if !strings.Contains(historyText, "roadmap evidence") {
				t.Fatalf("final history missing tool evidence: %s", historyText)
			}
			if strings.Contains(historyText, "search_thread_messages evidence:") || strings.Contains(historyText, "Prior read_thread internal tool result") {
				t.Fatalf("final history retained superseded retrieval scaffolding: %s", historyText)
			}
			return neoInferenceResult{Text: neoReadThreadTestFinalJSON("[message 49] roadmap evidence was read after unrestricted same-turn fanout.")}, nil
		}
		switch turn {
		case 0:
			calls := make([]neoToolCall, 0, 25)
			for i := 0; i < 25; i++ {
				calls = append(calls, neoToolCall{ID: fmt.Sprintf("TU-search-%d", i), Name: "search_thread_messages", Input: map[string]any{"query": fmt.Sprintf("roadmap evidence %d", i)}})
			}
			return neoInferenceResult{ToolCalls: calls}, nil
		case 1:
			return neoInferenceResult{ToolCalls: []neoToolCall{
				{ID: "TU-read-0", Name: "read_thread_messages", Input: map[string]any{"startIndex": 0, "count": 10}},
				{ID: "TU-read-1", Name: "read_thread_messages", Input: map[string]any{"startIndex": 10, "count": 10}},
				{ID: "TU-read-2", Name: "read_thread_messages", Input: map[string]any{"startIndex": 20, "count": 10}},
				{ID: "TU-read-3", Name: "read_thread_messages", Input: map[string]any{"startIndex": 30, "count": 10}},
				{ID: "TU-read-latest", Name: "read_thread_messages", Input: map[string]any{"latest": true, "count": 10}},
			}}, nil
		case 2:
			return neoInferenceResult{Text: neoReadThreadTestFinalJSON("[message 49] roadmap evidence was read after unrestricted same-turn fanout.")}, nil
		default:
			t.Fatalf("unexpected normal read_thread turn %d", turn)
		}
		return neoInferenceResult{}, nil
	}
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-current-reserved", "T-current-reserved", neoActorRecord("actor-test", "thread-actor", "T-current-reserved"), nil)

	text, err := actor.executeLocalReadThreadAgent(neoPendingTool{ID: "TU-read", Name: "read_thread", Input: map[string]any{"threadID": corpus.ThreadID, "question": "Extract roadmap evidence."}, AgentMode: "deep"}, actor.generation, corpus, "Extract roadmap evidence.")
	if err != nil {
		t.Fatalf("executeLocalReadThreadAgent error: %v", err)
	}
	if !strings.Contains(text, "unrestricted same-turn fanout") {
		t.Fatalf("read_thread text = %q, want final synthesis", text)
	}
	if len(captured) != 3 {
		t.Fatalf("captured requests = %d, want two tool turns and one final", len(captured))
	}
}

func TestNeoReadThreadLimitsEachRetrievalStageTo64ToolCalls(t *testing.T) {
	corpus := neoReadThreadCorpus{ThreadID: "T-stage-limit", Source: "test", Messages: []neoReadThreadMessage{{Index: 0, Role: "user", Text: "bounded retrieval evidence"}}}
	var captured []neoInferenceRequest
	rt := newNeoRuntime(&config.Config{})
	rt.inferStream = func(_ *neoRuntime, request neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		turn := len(captured)
		captured = append(captured, request)
		switch turn {
		case 0:
			calls := make([]neoToolCall, 0, neoReadThreadMaxCallsPerStage+1)
			for index := 0; index <= neoReadThreadMaxCallsPerStage; index++ {
				calls = append(calls, neoToolCall{ID: fmt.Sprintf("TU-search-%d", index), Name: "search_thread_messages", Input: map[string]any{"query": fmt.Sprintf("bounded retrieval evidence %d", index)}})
			}
			return neoInferenceResult{ToolCalls: calls}, nil
		case 1:
			if !strings.Contains(neoHistoryTestText(request.History), "accepts at most 64 tool calls per retrieval stage") {
				t.Fatal("retrieval history is missing the stage-limit rejection")
			}
			return neoInferenceResult{}, nil
		case 2:
			return neoInferenceResult{Text: neoReadThreadTestFinalJSON("[message 0] bounded retrieval evidence")}, nil
		default:
			t.Fatalf("unexpected read_thread turn %d", turn)
		}
		return neoInferenceResult{}, nil
	}
	actor := newNeoActor(rt, "actor-stage-limit", "thread-actor", "T-current-stage-limit", "T-current-stage-limit", neoActorRecord("actor-stage-limit", "thread-actor", "T-current-stage-limit"), nil)

	if _, err := actor.executeLocalReadThreadAgent(neoPendingTool{ID: "TU-read", Name: "read_thread", AgentMode: "deep"}, actor.generation, corpus, "Extract bounded retrieval evidence."); err != nil {
		t.Fatalf("executeLocalReadThreadAgent error: %v", err)
	}
	actor.mu.Lock()
	accepted := 0
	for _, message := range actor.messages {
		if message.ParentToolUseID == "TU-read" && message.Role == "assistant" {
			accepted++
		}
	}
	actor.mu.Unlock()
	if accepted != neoReadThreadMaxCallsPerStage {
		t.Fatalf("accepted tool calls = %d, want %d", accepted, neoReadThreadMaxCallsPerStage)
	}
}

func TestNeoReadThreadUncitedJSONUsesForcedFinal(t *testing.T) {
	corpus := neoReadThreadCorpus{ThreadID: "T-grounded-final", Source: "test", Messages: []neoReadThreadMessage{
		{Index: 0, Role: "user", Text: "Initial roadmap."},
		{Index: 1, Role: "assistant", Text: "Final roadmap decision."},
	}}
	var captured []neoInferenceRequest
	rt := newNeoRuntime(&config.Config{})
	rt.inferStream = func(_ *neoRuntime, request neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		turn := len(captured)
		captured = append(captured, request)
		switch turn {
		case 0:
			return neoInferenceResult{ToolCalls: []neoToolCall{{ID: "TU-search", Name: "search_thread_messages", Input: map[string]any{"query": "roadmap"}}}}, nil
		case 1:
			return neoInferenceResult{ToolCalls: []neoToolCall{{ID: "TU-latest", Name: "read_thread_messages", Input: map[string]any{"latest": true, "count": 2}}}}, nil
		default:
			if len(request.Tools) != 0 || request.ResponseMimeType != "application/json" {
				t.Fatalf("forced final request = tools:%#v mime:%q", request.Tools, request.ResponseMimeType)
			}
			return neoInferenceResult{Text: neoReadThreadTestFinalJSON("[message 1] Final roadmap decision.")}, nil
		}
	}
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-current-grounded", "T-current-grounded", neoActorRecord("actor-test", "thread-actor", "T-current-grounded"), nil)

	text, err := actor.executeLocalReadThreadAgent(neoPendingTool{ID: "TU-read", Name: "read_thread", AgentMode: "deep"}, actor.generation, corpus, "Extract the roadmap.")
	if err != nil {
		t.Fatalf("executeLocalReadThreadAgent error: %v", err)
	}
	if text != "[message 1] Final roadmap decision." || len(captured) != 3 {
		t.Fatalf("read_thread text/calls = %q/%d, want grounded forced final in three calls", text, len(captured))
	}
}

func TestNeoReadThreadPersistedRunIsBoundedAndKeepsEndpoints(t *testing.T) {
	messages := make([]any, 20)
	for i := range messages {
		results := make([]any, 8)
		for j := range results {
			results[j] = map[string]any{"toolUseID": fmt.Sprintf("TU-%d-%d", i, j), "text": strings.Repeat("result ", 1000)}
		}
		toolUses := []any{map[string]any{"id": fmt.Sprintf("TU-input-%d", i), "name": "edit_file", "input": map[string]any{"patch": strings.Repeat("large-input ", 100000)}}}
		messages[i] = map[string]any{"index": i, "role": "assistant", "text": strings.Repeat("message ", 2000), "toolUses": toolUses, "toolResults": results}
	}
	run := map[string]any{"status": "done", "result": map[string]any{"messages": messages, "range": map[string]any{"startIndex": 0, "endIndex": 19}}}
	stored := neoReadThreadPersistedRun("read_thread_messages", run)
	raw, err := json.Marshal(stored)
	if err != nil {
		t.Fatalf("marshal persisted run: %v", err)
	}
	if len(raw) > 60000 {
		t.Fatalf("persisted run bytes = %d, want <= 60000", len(raw))
	}
	storedResult := mapValue(stored["result"])
	storedMessages := arrayValue(storedResult["messages"])
	if len(storedMessages) != neoReadThreadStoredMessageCount {
		t.Fatalf("persisted messages = %d, want %d", len(storedMessages), neoReadThreadStoredMessageCount)
	}
	if numberFrom(mapValue(storedMessages[0])["index"]) != 0 || numberFrom(mapValue(storedMessages[len(storedMessages)-1])["index"]) != 19 {
		t.Fatalf("persisted endpoints = first:%#v last:%#v", storedMessages[0], storedMessages[len(storedMessages)-1])
	}
	if numberFrom(storedResult["persistedMessagesOmitted"]) != 8 {
		t.Fatalf("persistedMessagesOmitted = %#v, want 8", storedResult["persistedMessagesOmitted"])
	}
	firstToolUse := mapValue(arrayValue(mapValue(storedMessages[0])["toolUses"])[0])
	storedInput := mapValue(firstToolUse["input"])
	if !boolValue(storedInput["persistedInputTruncated"]) || len([]rune(stringValue(storedInput["preview"]))) > neoReadThreadStoredResultChars+128 {
		t.Fatalf("persisted tool input = %#v, want bounded truncation marker", storedInput)
	}
}

func TestNeoReadThreadRunLocalActorToolEmitsBinaryProgress(t *testing.T) {
	threadID := "T-019e65c0-0310-77a8-b233-4b84d9c06141"
	var captured []neoInferenceRequest
	rt := newNeoRuntime(&config.Config{})
	rt.inferStream = neoReadThreadScriptedInfer(t, "[message 0] local progress result", "local progress", &captured, nil)
	actor := newNeoActor(rt, "actor-test", "thread-actor", threadID, threadID, neoActorRecord("actor-test", "thread-actor", threadID), nil)
	actor.executorBootstrapComplete = true
	actor.messages = []neoMessage{
		{ThreadID: threadID, MessageID: "M-assistant", Role: "assistant", Content: []any{map[string]any{"type": "tool_use", "id": "TU-read", "name": "read_thread", "input": map[string]any{"goal": "Extract local progress."}}}, Seq: 1},
		{ThreadID: threadID, MessageID: "M-current", Role: "user", Content: []any{map[string]any{"type": "text", "text": "local progress result"}}, Seq: 2},
	}
	pending := neoPendingTool{ID: "TU-read", Name: "read_thread", Input: map[string]any{"goal": "Extract local progress."}, AgentMode: "deep", MessageID: "M-assistant"}
	actor.pendingTools[pending.ID] = pending

	actor.runLocalActorTool(pending, actor.generation)

	actor.mu.Lock()
	defer actor.mu.Unlock()
	var sawExtracting bool
	for _, replay := range actor.replayEvents {
		payload := mapValue(replay.Payload)
		if payload["type"] != "message_added" && payload["type"] != "message_updated" {
			continue
		}
		message := mapValue(payload["message"])
		for _, raw := range arrayValue(message["content"]) {
			run := mapValue(mapValue(raw)["run"])
			progress := mapValue(run["progress"])
			switch stringValue(progress["statusMessage"]) {
			case "Extracting content from thread...":
				sawExtracting = true
			}
		}
	}
	if !sawExtracting {
		t.Fatalf("read_thread progress events missing extracting status replay=%#v", actor.replayEvents)
	}
	result := actor.messages[actor.messageIndexLocked(toolResultMessageID("TU-read"))]
	run := mapValue(mapValue(result.Content[0])["run"])
	if result.CompletionStatus != "" || stringValue(run["status"]) != "done" || !strings.Contains(stringValue(run["result"]), "local progress result") {
		t.Fatalf("final read_thread result = %#v run=%#v", result, run)
	}
	if len(captured) > 1 && strings.Contains(neoHistoryTestText(captured[1].History), "Loading thread...") {
		t.Fatalf("read_thread agent read its own progress message: %#v", captured[1].History)
	}
	foundFinalHistory := false
	for _, message := range actor.history {
		if message.ToolCallID == "TU-read" && strings.Contains(message.Text, "local progress result") {
			foundFinalHistory = true
			break
		}
	}
	if !foundFinalHistory {
		t.Fatalf("history = %#v, want final read_thread result", actor.history)
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

func TestNeoReadThreadOverviewIncludesContinuationObjectiveTail(t *testing.T) {
	messages := make([]neoReadThreadMessage, 254)
	for i := range messages {
		messages[i] = neoReadThreadMessage{Index: i, Role: "assistant", MessageID: fmt.Sprintf("M-%03d", i), Text: fmt.Sprintf("background tool output %03d", i)}
	}
	messages[71] = neoReadThreadMessage{Index: 71, Role: "assistant", MessageID: "M-old-storage", Text: "Earlier context: add storage validation coverage for position marks and signal-status ranking."}
	messages[235] = neoReadThreadMessage{Index: 235, Role: "user", MessageID: "M-late-question", Text: "so what are going to be the fixes to resolve the expired and late signals"}
	messages[236] = neoReadThreadMessage{Index: 236, Role: "assistant", MessageID: "M-late-plan", Text: "Fix dashboard labels in frontend/src/routes/post-launch/strategy-lab/+page.svelte and add the paper-only late observation variant in data_collection/workers/strategy_paper_trader.py."}
	messages[237] = neoReadThreadMessage{Index: 237, Role: "user", MessageID: "M-latest-objective", Text: "lets do the fixes"}
	for i := 238; i < len(messages); i++ {
		messages[i] = neoReadThreadMessage{Index: i, Role: "tool", MessageID: fmt.Sprintf("M-tool-%03d", i), Text: fmt.Sprintf("tool output after the user instruction %03d", i)}
	}

	overview := neoReadThreadOverview(neoReadThreadCorpus{ThreadID: "T-continuation", Source: "test", Title: "late signal fixes", Messages: messages})
	raw, err := json.Marshal(overview["latestMessages"])
	if err != nil {
		t.Fatalf("marshal latestMessages: %v", err)
	}
	text := string(raw)
	if !strings.Contains(text, "lets do the fixes") || !strings.Contains(text, "strategy_paper_trader.py") {
		t.Fatalf("latestMessages missed continuation objective tail: %s", text)
	}
	if strings.Contains(text, "position marks") || strings.Contains(text, "signal-status ranking") {
		t.Fatalf("latestMessages included stale older context: %s", text)
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

func TestNeoReadThreadForcedFinalAnchorsLatestVisibleTail(t *testing.T) {
	latestDetail := strings.Repeat("latest context padding ", 80) + "EXACT_LATE_TAIL_MARKER"
	corpus := neoReadThreadCorpus{ThreadID: "T-final-tail", Source: "test", Title: "final tail", Messages: []neoReadThreadMessage{
		{Index: 0, Role: "user", MessageID: "M-old", Text: "Old summary says wait for review cooldown."},
		{Index: 1, Role: "assistant", MessageID: "M-merged", Text: "Latest state: PR #1003 was merged and local main is synced. " + latestDetail},
	}}
	var captured []neoInferenceRequest
	rt := newNeoRuntime(&config.Config{})
	rt.inferStream = func(_ *neoRuntime, request neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		captured = append(captured, request)
		return neoInferenceResult{Text: neoReadThreadTestFinalJSON("[message 1] PR #1003 was merged and local main is synced.")}, nil
	}
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-current", "T-current", neoActorRecord("actor-test", "thread-actor", "T-current"), nil)
	text, err := actor.forceLocalReadThreadFinal(
		neoPendingTool{ID: "TU-read", Name: "read_thread"},
		actor.generation,
		neoModelRoute{Provider: neoReadThreadAgentProvider, Model: neoReadThreadAgentModel},
		neoReadThreadAgentEffort,
		actor.id,
		actor.threadID,
		"deep",
		nil,
		map[string]any{"reasoning.effort": neoReadThreadAgentEffort},
		nil,
		[]neoHistoryMessage{
			{Role: "user", Text: "Read thread for continuation."},
			{Role: "tool", ToolCallID: "TU-old", ToolName: "read_thread_messages", Text: "Old tool result: wait for review cooldown."},
		},
		corpus,
	)
	if err != nil {
		t.Fatalf("forceLocalReadThreadFinal error: %v", err)
	}
	if !strings.Contains(text, "merged") {
		t.Fatalf("forced final text = %q, want merged state", text)
	}
	if len(captured) != 1 {
		t.Fatalf("captured requests = %d, want 1", len(captured))
	}
	historyText := neoHistoryTestText(captured[0].History)
	if !strings.Contains(historyText, "Authoritative latest visible target-thread messages") || !strings.Contains(historyText, "PR #1003 was merged") || !strings.Contains(historyText, "EXACT_LATE_TAIL_MARKER") {
		t.Fatalf("forced final history missing latest visible tail: %s", historyText)
	}
	if !strings.Contains(historyText, neoReadThreadFinalPrompt) {
		t.Fatalf("forced final history missing final prompt: %s", historyText)
	}
}

func TestNeoReadThreadMergeRefreshedCorpusPreservesExistingIndexes(t *testing.T) {
	initial := neoReadThreadCorpus{ThreadID: "T-refresh", Title: "old title", Messages: []neoReadThreadMessage{
		{Index: 40, Role: "user", MessageID: "M-old", Text: "old objective"},
		{Index: 72, Role: "assistant", MessageID: "M-shared", Text: "work in progress"},
	}}
	refreshed := neoReadThreadCorpus{ThreadID: "T-refresh", Title: "new title", Messages: []neoReadThreadMessage{
		{Index: 0, Role: "assistant", MessageID: "M-shared", Text: "work completed"},
		{Index: 1, Role: "user", MessageID: "M-new", Text: "newer correction"},
	}}

	merged, changed := neoReadThreadMergeRefreshedCorpus(initial, refreshed)
	if !changed {
		t.Fatal("neoReadThreadMergeRefreshedCorpus changed = false, want refreshed messages")
	}
	if merged.Title != refreshed.Title || len(merged.Messages) != 2 {
		t.Fatalf("merged corpus = %#v, want refreshed title and two authoritative messages", merged)
	}
	if merged.Messages[0].Index != 72 || merged.Messages[0].Text != "work completed" {
		t.Fatalf("merged existing message = %#v, want stable index with updated content", merged.Messages[0])
	}
	if merged.Messages[1].Index != 73 || merged.Messages[1].MessageID != "M-new" {
		t.Fatalf("merged appended message = %#v, want new stable index 73", merged.Messages[1])
	}
}

func TestNeoReadThreadMergeRefreshedCorpusDistinguishesIDLessMessages(t *testing.T) {
	initial := neoReadThreadCorpus{ThreadID: "T-refresh-idless", Messages: []neoReadThreadMessage{
		{Index: 7, Role: "assistant", CreatedAt: "2026-08-01T12:00:00Z", Text: "alpha"},
		{Index: 8, Role: "assistant", CreatedAt: "2026-08-01T12:00:00Z", Text: "beta"},
	}}
	refreshed := neoReadThreadCorpus{ThreadID: "T-refresh-idless", Messages: []neoReadThreadMessage{
		{Role: "assistant", CreatedAt: "2026-08-01T12:00:00Z", Text: "beta"},
		{Role: "assistant", CreatedAt: "2026-08-01T12:00:00Z", Text: "alpha"},
		{Role: "assistant", CreatedAt: "2026-08-01T12:00:00Z", Text: "gamma"},
	}}

	merged, changed := neoReadThreadMergeRefreshedCorpus(initial, refreshed)
	if !changed {
		t.Fatal("neoReadThreadMergeRefreshedCorpus changed = false, want reordered authoritative messages")
	}
	indexes := []int{merged.Messages[0].Index, merged.Messages[1].Index, merged.Messages[2].Index}
	texts := []string{merged.Messages[0].Text, merged.Messages[1].Text, merged.Messages[2].Text}
	if !slices.Equal(indexes, []int{8, 9, 10}) || !slices.Equal(texts, []string{"beta", "alpha", "gamma"}) {
		t.Fatalf("ID-less refreshed messages = indexes %v texts %v, want stable monotonic order", indexes, texts)
	}
	if merged.nextIndex != 11 {
		t.Fatalf("merged citation high-water = %d, want 11", merged.nextIndex)
	}
	withoutHighest, _ := neoReadThreadMergeRefreshedCorpus(merged, neoReadThreadCorpus{ThreadID: "T-refresh-idless", Messages: []neoReadThreadMessage{{Role: "assistant", CreatedAt: "2026-08-01T12:00:00Z", Text: "beta"}}})
	withNewMessage, _ := neoReadThreadMergeRefreshedCorpus(withoutHighest, neoReadThreadCorpus{ThreadID: "T-refresh-idless", Messages: []neoReadThreadMessage{
		{Role: "assistant", CreatedAt: "2026-08-01T12:00:00Z", Text: "beta"},
		{Role: "user", CreatedAt: "2026-08-01T12:01:00Z", Text: "delta"},
	}})
	if got := withNewMessage.Messages[1].Index; got != 11 || withNewMessage.nextIndex != 12 {
		t.Fatalf("new message index/high-water = %d/%d, want 11/12", got, withNewMessage.nextIndex)
	}
}

func TestNeoReadThreadAuthoritativeEvidenceUsesStructuredReadResults(t *testing.T) {
	rawMessages := make([]any, 0, 12)
	originalMessages := make([]neoReadThreadMessage, 0, 12)
	refreshedMessages := make([]neoReadThreadMessage, 0, 12)
	for index := 0; index < 12; index++ {
		messageID := fmt.Sprintf("M-%d", index)
		rawMessages = append(rawMessages, map[string]any{"index": index, "role": "assistant", "messageID": messageID, "text": strings.Repeat(fmt.Sprintf("old-%d ", index), 500)})
		originalMessages = append(originalMessages, neoReadThreadMessage{Index: index, Role: "assistant", MessageID: messageID, Text: fmt.Sprintf("old-%d", index)})
		refreshedMessages = append(refreshedMessages, neoReadThreadMessage{Index: index, Role: "assistant", MessageID: messageID, Text: fmt.Sprintf("current-%d", index)})
	}
	entry, ok := neoReadThreadEvidenceEntry("read_thread_messages", map[string]any{"status": "done", "result": map[string]any{
		"threadID":       "T-evidence",
		"range":          map[string]any{"startIndex": 0, "endIndex": 11},
		"messages":       rawMessages,
		"truncated":      true,
		"nextStartIndex": 12,
	}}, neoReadThreadCorpus{ThreadID: "T-evidence", Messages: originalMessages})
	if !ok {
		t.Fatal("large structured read evidence was rejected")
	}
	refreshed := neoReadThreadAuthoritativeEvidence([]neoReadThreadEvidence{entry}, neoReadThreadCorpus{ThreadID: "T-evidence", Source: "local-live", Messages: refreshedMessages})
	if len(refreshed) != 1 {
		t.Fatalf("refreshed evidence entries = %d, want 1", len(refreshed))
	}
	result := mapValue(refreshed[0].Run["result"])
	messages := arrayValue(result["messages"])
	if len(messages) != 12 || stringValue(mapValue(messages[0])["text"]) != "current-0" || stringValue(mapValue(messages[11])["text"]) != "current-11" {
		t.Fatalf("refreshed structured messages = %#v", messages)
	}
	for _, staleField := range []string{"truncated", "nextStartIndex", "previousEndIndex", "sparseRangeNormalized", "requestedRange"} {
		if _, exists := result[staleField]; exists {
			t.Fatalf("refreshed evidence retained stale %s metadata: %#v", staleField, result)
		}
	}

	reorderedEntry, ok := neoReadThreadEvidenceEntry("read_thread_messages", map[string]any{"status": "done", "result": map[string]any{
		"messages": []any{map[string]any{"index": 0, "role": "assistant", "messageID": "M-alpha", "text": "alpha evidence"}},
	}}, neoReadThreadCorpus{Messages: []neoReadThreadMessage{
		{Index: 0, Role: "assistant", MessageID: "M-alpha", Text: "alpha evidence"},
		{Index: 1, Role: "assistant", MessageID: "M-beta", Text: "beta evidence"},
	}})
	if !ok {
		t.Fatal("reordered evidence was rejected")
	}
	reordered := neoReadThreadAuthoritativeEvidence([]neoReadThreadEvidence{reorderedEntry}, neoReadThreadCorpus{Messages: []neoReadThreadMessage{
		{Index: 1, Role: "assistant", MessageID: "M-beta", Text: "beta evidence"},
		{Index: 2, Role: "assistant", MessageID: "M-alpha", Text: "updated alpha evidence"},
	}})
	if len(reordered) != 1 {
		t.Fatalf("reordered evidence entries = %d, want 1", len(reordered))
	}
	reorderedMessages := arrayValue(mapValue(reordered[0].Run["result"])["messages"])
	if len(reorderedMessages) != 1 || numberFrom(mapValue(reorderedMessages[0])["index"]) != 2 || stringValue(mapValue(reorderedMessages[0])["text"]) != "updated alpha evidence" {
		t.Fatalf("reordered evidence = %#v, want remapped alpha message", reorderedMessages)
	}
}

func TestNeoReadThreadFinalRefreshesTargetChangedDuringSynthesis(t *testing.T) {
	currentThreadID := "T-019f7000-0000-7000-8000-0000000000c1"
	targetThreadID := "T-019f7000-0000-7000-8000-0000000000c2"

	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", currentThreadID, currentThreadID, neoActorRecord("actor-test", "thread-actor", currentThreadID), nil)
	target := rt.store.ensureThreadActor(targetThreadID)
	target.mu.Lock()
	target.title = "old target title"
	target.messages = []neoMessage{{
		Role:      "user",
		MessageID: "M-old",
		Content:   []any{map[string]any{"type": "text", "text": "Old objective remains pending."}},
	}}
	target.messageRevision++
	target.mu.Unlock()
	corpus, ok := actor.liveReadThreadCorpus(targetThreadID)
	if !ok {
		t.Fatal("failed to load initial target corpus")
	}
	var captured []neoInferenceRequest
	rt.inferStream = func(_ *neoRuntime, request neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		captured = append(captured, request)
		switch len(captured) {
		case 1:
			return neoInferenceResult{ToolCalls: []neoToolCall{{ID: "TU-search", Name: "search_thread_messages", Input: map[string]any{"query": "objective"}}}}, nil
		case 2:
			return neoInferenceResult{ToolCalls: []neoToolCall{{ID: "TU-read", Name: "read_thread_messages", Input: map[string]any{"latest": true, "count": 1}}}}, nil
		case 3:
			target.mu.Lock()
			target.title = "current target title"
			target.messages = []neoMessage{{
				Role:      "assistant",
				MessageID: "M-new",
				Content:   []any{map[string]any{"type": "text", "text": "New completion supersedes the old objective."}},
			}}
			target.messageRevision++
			target.mu.Unlock()
			return neoInferenceResult{Text: neoReadThreadTestFinalJSON("[message 0] Old objective remains pending.")}, nil
		default:
			historyText := neoHistoryTestText(request.History)
			if !strings.Contains(historyText, "New completion supersedes the old objective.") || !strings.Contains(historyText, "current target title") {
				t.Fatalf("refreshed final history missing concurrent target update: %s", historyText)
			}
			if strings.Contains(historyText, "Old objective remains pending.") || strings.Contains(historyText, "old target title") {
				t.Fatalf("refreshed final history retained stale target content: %s", historyText)
			}
			return neoInferenceResult{Text: neoReadThreadTestFinalJSON("[message 1] New completion supersedes the old objective.")}, nil
		}
	}

	text, err := actor.executeLocalReadThreadAgent(neoPendingTool{ID: "TU-parent", Name: "read_thread", Input: map[string]any{"threadID": targetThreadID, "question": "What is current?"}, AgentMode: "deep"}, actor.generation, corpus, "What is current?")
	if err != nil {
		t.Fatalf("executeLocalReadThreadAgent error: %v", err)
	}
	if !strings.Contains(text, "New completion") {
		t.Fatalf("read_thread text = %q, want concurrent target update", text)
	}
	if len(captured) != 4 {
		t.Fatalf("captured requests = %d, want one refreshed final retry", len(captured))
	}
}

func TestNeoReadThreadFinalFailsWhenLocalTargetDisappears(t *testing.T) {
	currentThreadID := "T-019f7000-0000-7000-8000-0000000000d1"
	targetThreadID := "T-019f7000-0000-7000-8000-0000000000d2"
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", currentThreadID, currentThreadID, neoActorRecord("actor-test", "thread-actor", currentThreadID), nil)
	target := rt.store.ensureThreadActor(targetThreadID)
	target.mu.Lock()
	target.messages = []neoMessage{{Role: "user", MessageID: "M-old", Content: []any{map[string]any{"type": "text", "text": "stale target content"}}}}
	target.messageRevision++
	target.mu.Unlock()
	corpus, ok := actor.liveReadThreadCorpus(targetThreadID)
	if !ok {
		t.Fatal("failed to load initial target corpus")
	}
	requests := 0
	rt.inferStream = func(_ *neoRuntime, request neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		requests++
		switch requests {
		case 1:
			return neoInferenceResult{ToolCalls: []neoToolCall{{ID: "TU-search", Name: "search_thread_messages", Input: map[string]any{"query": "target"}}}}, nil
		case 2:
			return neoInferenceResult{ToolCalls: []neoToolCall{{ID: "TU-read", Name: "read_thread_messages", Input: map[string]any{"latest": true, "count": 1}}}}, nil
		default:
			rt.store.removeThread(targetThreadID)
			return neoInferenceResult{Text: neoReadThreadTestFinalJSON("[message 0] stale target content")}, nil
		}
	}

	text, err := actor.executeLocalReadThreadAgent(neoPendingTool{ID: "TU-parent", Name: "read_thread", Input: map[string]any{"threadID": targetThreadID, "question": "What remains?"}, AgentMode: "deep"}, actor.generation, corpus, "What remains?")
	if err == nil || !strings.Contains(err.Error(), "target became unavailable") || text != "" {
		t.Fatalf("disappeared target result text=%q error=%v", text, err)
	}
}

func TestNeoReadThreadFinalFailsWhenTargetChangesRepeatedly(t *testing.T) {
	currentThreadID := "T-019f7000-0000-7000-8000-0000000000e1"
	targetThreadID := "T-019f7000-0000-7000-8000-0000000000e2"
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", currentThreadID, currentThreadID, neoActorRecord("actor-test", "thread-actor", currentThreadID), nil)
	target := rt.store.ensureThreadActor(targetThreadID)
	target.mu.Lock()
	target.messages = []neoMessage{{Role: "user", MessageID: "M-0", Content: []any{map[string]any{"type": "text", "text": "version zero"}}}}
	target.messageRevision++
	target.mu.Unlock()
	corpus, ok := actor.liveReadThreadCorpus(targetThreadID)
	if !ok {
		t.Fatal("failed to load initial target corpus")
	}
	requests := 0
	rt.inferStream = func(_ *neoRuntime, request neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		requests++
		switch requests {
		case 1:
			return neoInferenceResult{ToolCalls: []neoToolCall{{ID: "TU-search", Name: "search_thread_messages", Input: map[string]any{"query": "version"}}}}, nil
		case 2:
			return neoInferenceResult{ToolCalls: []neoToolCall{{ID: "TU-read", Name: "read_thread_messages", Input: map[string]any{"latest": true, "count": 1}}}}, nil
		case 3, 4:
			target.mu.Lock()
			version := requests - 2
			target.messages = append(target.messages, neoMessage{Role: "assistant", MessageID: fmt.Sprintf("M-%d", version), Content: []any{map[string]any{"type": "text", "text": fmt.Sprintf("version %d", version)}}})
			target.messageRevision++
			target.mu.Unlock()
			return neoInferenceResult{Text: neoReadThreadTestFinalJSON(fmt.Sprintf("[message %d] version %d", version-1, version-1))}, nil
		default:
			t.Fatalf("unexpected inference request %d", requests)
			return neoInferenceResult{}, nil
		}
	}

	text, err := actor.executeLocalReadThreadAgent(neoPendingTool{ID: "TU-parent", Name: "read_thread", Input: map[string]any{"threadID": targetThreadID, "question": "Which version?"}, AgentMode: "deep"}, actor.generation, corpus, "Which version?")
	if err == nil || !strings.Contains(err.Error(), "changed repeatedly") || text != "" || requests != 4 {
		t.Fatalf("repeated target changes result text=%q error=%v requests=%d", text, err, requests)
	}
}

func TestNeoReadThreadRequestHistoryBoundsLargeToolResults(t *testing.T) {
	huge := strings.Repeat("large read_thread observation ", neoReadThreadHistoryTextChars*2)
	history := []neoHistoryMessage{{Role: "user", Text: "Read the target thread for the goal."}}
	for i := 0; i < 8; i++ {
		id := fmt.Sprintf("TU-large-%d", i)
		history = append(history,
			neoHistoryMessage{Role: "assistant", ToolCalls: []neoToolCall{{ID: id, Name: "read_thread_messages"}}},
			neoHistoryMessage{Role: "tool", ToolCallID: id, ToolName: "read_thread_messages", Text: fmt.Sprintf("result %d\n%s", i, huge), ThinkingBlocks: []neoThinkingBlock{{Thinking: huge}}},
		)
	}

	bounded := neoReadThreadRequestHistory(history)
	if got := neoReadThreadHistoryApproxChars(bounded); got > neoReadThreadHistoryTotalChars+2048 {
		t.Fatalf("bounded history chars = %d, want <= %d", got, neoReadThreadHistoryTotalChars+2048)
	}
	if !strings.Contains(bounded[0].Text, "omitted") {
		t.Fatalf("bounded history head missing omission notice: %#v", bounded[0])
	}
	openToolCalls := map[string]bool{}
	for _, message := range bounded {
		if len(message.OpenAIItems) > 0 || (message.Role != "assistant" && len(message.ThinkingBlocks) > 0) {
			t.Fatalf("bounded history kept resend-only provider blocks: %#v", message)
		}
		for _, call := range message.ToolCalls {
			openToolCalls[call.ID] = true
		}
		if message.Role == "tool" {
			if !openToolCalls[message.ToolCallID] {
				t.Fatalf("tool result %q has no retained assistant tool call in %#v", message.ToolCallID, bounded)
			}
			delete(openToolCalls, message.ToolCallID)
			if !strings.Contains(message.Text, "[truncated") {
				t.Fatalf("tool result was not clipped: len=%d", len(message.Text))
			}
		}
	}
	if len(openToolCalls) != 0 {
		t.Fatalf("bounded history kept dangling assistant tool calls: %#v in %#v", openToolCalls, bounded)
	}
}

func TestNeoReadThreadRequestHistoryPreservesSingleObservationWithinTotalBudget(t *testing.T) {
	marker := "final-marker-survives-after-eager-clip-boundary"
	large := strings.Repeat("x", neoReadThreadHistoryTextChars+512) + marker
	history := []neoHistoryMessage{
		{Role: "user", Text: "Read the target thread for the goal."},
		{Role: "assistant", ToolCalls: []neoToolCall{{ID: "TU-large-fit", Name: "read_thread_messages"}}},
		{Role: "tool", ToolCallID: "TU-large-fit", ToolName: "read_thread_messages", Text: large},
	}
	if neoReadThreadHistoryApproxChars(history) > neoReadThreadHistoryTotalChars {
		t.Fatalf("test setup history exceeds total budget: %d > %d", neoReadThreadHistoryApproxChars(history), neoReadThreadHistoryTotalChars)
	}

	bounded := neoReadThreadRequestHistory(history)
	text := neoHistoryTestText(bounded)
	if !strings.Contains(text, marker) {
		t.Fatalf("bounded history lost marker near end of large observation")
	}
	if strings.Contains(text, "[truncated") {
		t.Fatalf("bounded history truncated observation that fits total budget")
	}
}

func TestNeoReadThreadRequestHistoryTrimsOversizedLatestGroup(t *testing.T) {
	huge := strings.Repeat("large read_thread observation ", neoReadThreadHistoryTextChars*2)
	calls := make([]neoToolCall, 0, 8)
	originalCallIDs := make([]string, 0, 8)
	history := []neoHistoryMessage{{Role: "user", Text: "Read the target thread for the goal."}}
	for i := 0; i < 8; i++ {
		id := fmt.Sprintf("TU-batched-%d", i)
		calls = append(calls, neoToolCall{ID: id, Name: "read_thread_messages"})
		originalCallIDs = append(originalCallIDs, id)
	}
	history = append(history, neoHistoryMessage{Role: "assistant", ToolCalls: calls})
	for i := range calls {
		history = append(history, neoHistoryMessage{Role: "tool", ToolCallID: calls[i].ID, ToolName: "read_thread_messages", Text: fmt.Sprintf("result %d\n%s", i, huge)})
	}

	bounded := neoReadThreadRequestHistory(history)
	if got := neoReadThreadHistoryApproxChars(bounded); got > neoReadThreadHistoryTotalChars {
		t.Fatalf("bounded history chars = %d, want <= %d", got, neoReadThreadHistoryTotalChars)
	}
	afterCallIDs := make([]string, 0, len(history[1].ToolCalls))
	for _, call := range history[1].ToolCalls {
		afterCallIDs = append(afterCallIDs, call.ID)
	}
	if !slices.Equal(afterCallIDs, originalCallIDs) {
		t.Fatalf("original tool calls mutated: got %#v, want %#v", afterCallIDs, originalCallIDs)
	}
	retainedCalls := map[string]bool{}
	retainedResults := map[string]bool{}
	for _, message := range bounded {
		for _, call := range message.ToolCalls {
			retainedCalls[call.ID] = true
		}
		if message.Role == "tool" {
			retainedResults[message.ToolCallID] = true
			if !strings.Contains(message.Text, "[truncated") {
				t.Fatalf("tool result was not clipped: len=%d", len(message.Text))
			}
		}
	}
	if len(retainedResults) == 0 || len(retainedResults) >= len(calls) {
		t.Fatalf("retained tool results = %#v, want a trimmed non-empty subset", retainedResults)
	}
	for id := range retainedCalls {
		if !retainedResults[id] {
			t.Fatalf("retained tool call %q has no matching result; calls=%#v results=%#v history=%#v", id, retainedCalls, retainedResults, bounded)
		}
	}
	for id := range retainedResults {
		if !retainedCalls[id] {
			t.Fatalf("retained tool result %q has no matching call; calls=%#v results=%#v history=%#v", id, retainedCalls, retainedResults, bounded)
		}
	}
}

func TestNeoReadThreadRequestHistoryTrimsLatestReadBeforeOlderHistory(t *testing.T) {
	huge := strings.Repeat("large read_thread observation ", neoReadThreadHistoryTextChars*2)
	readCalls := make([]neoToolCall, 0, 8)
	readResults := make([]neoHistoryMessage, 0, 8)
	for i := 0; i < 8; i++ {
		id := fmt.Sprintf("TU-read-%d", i)
		readCalls = append(readCalls, neoToolCall{ID: id, Name: "read_thread_messages"})
		text := fmt.Sprintf("older read detail %d\n%s", i, huge)
		if i == 7 {
			text = "latest exact detail survived\n" + huge
		}
		readResults = append(readResults, neoHistoryMessage{Role: "tool", ToolCallID: id, ToolName: "read_thread_messages", Text: text})
	}
	history := []neoHistoryMessage{
		{Role: "user", Text: "Read the target thread for the goal."},
		{Role: "assistant", ToolCalls: []neoToolCall{{ID: "TU-search", Name: "search_thread_messages"}}},
		{Role: "tool", ToolCallID: "TU-search", ToolName: "search_thread_messages", Text: "older search hit"},
		{Role: "assistant", ToolCalls: readCalls},
	}
	history = append(history, readResults...)
	history = append(history,
		neoHistoryMessage{Role: "user", Text: neoReadThreadFinalPrompt},
	)

	bounded := neoReadThreadRequestHistory(history)
	historyText := neoHistoryTestText(bounded)
	if !strings.Contains(historyText, "latest exact detail survived") {
		t.Fatalf("bounded history dropped latest read result: %#v", bounded)
	}
	if strings.Contains(historyText, "older search hit") {
		t.Fatalf("bounded history kept older search after trimming latest read: %#v", bounded)
	}
	if bounded[len(bounded)-1].Role != "user" || !strings.Contains(bounded[len(bounded)-1].Text, neoReadThreadFinalPrompt) {
		t.Fatalf("bounded history lost final prompt: %#v", bounded)
	}
}

func TestNeoReadThreadRequestHistoryPreservesAssistantReasoningMetadata(t *testing.T) {
	history := []neoHistoryMessage{
		{Role: "user", Text: "Read the target thread for the goal."},
		{
			Role:      "assistant",
			ToolCalls: []neoToolCall{{ID: "TU-read", Name: "read_thread_messages"}},
			ThinkingBlocks: []neoThinkingBlock{{Provider: "anthropic", Thinking: "exact signed thinking", Signature: "sig-1"}, {
				Provider:  "openai",
				ID:        "rs-1",
				Thinking:  "reasoning summary",
				Signature: strings.Repeat("encrypted", 32),
			}},
		},
		{Role: "tool", ToolCallID: "TU-read", ToolName: "read_thread_messages", Text: "latest read result", ThinkingBlocks: []neoThinkingBlock{{Thinking: "drop tool thinking"}}},
	}

	bounded := neoReadThreadRequestHistory(history)
	var assistant *neoHistoryMessage
	for i := range bounded {
		if bounded[i].Role == "assistant" {
			assistant = &bounded[i]
			break
		}
	}
	if assistant == nil || len(assistant.ThinkingBlocks) != 2 {
		t.Fatalf("assistant thinking blocks = %#v, want preserved metadata in %#v", assistant, bounded)
	}
	for _, message := range bounded {
		if message.Role != "assistant" && len(message.ThinkingBlocks) > 0 {
			t.Fatalf("non-assistant thinking blocks were retained: %#v", bounded)
		}
	}
}

func TestNeoReadThreadRequestHistoryCountsAssistantReasoningMetadata(t *testing.T) {
	huge := strings.Repeat("signed reasoning payload ", neoReadThreadHistoryTextChars*2)
	history := []neoHistoryMessage{
		{Role: "user", Text: "Read the target thread for the goal."},
		{
			Role: "assistant",
			Text: "assistant answer survives",
			ThinkingBlocks: []neoThinkingBlock{{
				Provider:  "openai",
				ID:        "rs-large",
				Thinking:  huge,
				Signature: huge,
			}},
		},
	}
	if got := neoReadThreadHistoryApproxChars(history); got <= neoReadThreadHistoryTotalChars {
		t.Fatalf("test setup history chars = %d, want > %d", got, neoReadThreadHistoryTotalChars)
	}

	bounded := neoReadThreadRequestHistory(history)
	if got := neoReadThreadHistoryApproxChars(bounded); got > neoReadThreadHistoryTotalChars {
		t.Fatalf("bounded history chars = %d, want <= %d", got, neoReadThreadHistoryTotalChars)
	}
	if !strings.Contains(neoHistoryTestText(bounded), "assistant answer survives") {
		t.Fatalf("bounded history dropped assistant answer: %#v", bounded)
	}
	for _, message := range bounded {
		if len(message.ThinkingBlocks) > 0 {
			t.Fatalf("oversized assistant thinking blocks were retained: %#v", bounded)
		}
	}
}

func TestNeoReadThreadTrimHistoryDropsOversizedLatestGroupOnce(t *testing.T) {
	calls := []neoToolCall{
		{ID: "TU-drop-0", Name: "read_thread_messages"},
		{ID: "TU-drop-1", Name: "read_thread_messages"},
		{ID: "TU-drop-2", Name: "read_thread_messages"},
		{ID: "TU-drop-3", Name: "read_thread_messages"},
	}
	history := []neoHistoryMessage{
		{Role: "user", Text: strings.Repeat("oversized head ", 200)},
		{Role: "assistant", ToolCalls: calls},
	}
	for _, call := range calls {
		history = append(history, neoHistoryMessage{Role: "tool", ToolCallID: call.ID, ToolName: call.Name, Text: "tool result"})
	}

	bounded := neoReadThreadTrimHistory(history, 1)
	if len(bounded) != 1 {
		t.Fatalf("bounded history len = %d, want only head message: %#v", len(bounded), bounded)
	}
	if !strings.Contains(bounded[0].Text, "omitted 5") {
		t.Fatalf("bounded head text missing exact omission count: %q", bounded[0].Text)
	}
}

func TestNeoReadThreadAgentSendsBoundedHistoryAfterRepeatedLargeReads(t *testing.T) {
	messages := make([]neoReadThreadMessage, 0, 6)
	for i := 0; i < 6; i++ {
		messages = append(messages, neoReadThreadMessage{
			Index:     i,
			Role:      "assistant",
			MessageID: fmt.Sprintf("M-large-%d", i),
			Text:      fmt.Sprintf("target-marker-%d\n%s", i, strings.Repeat("large transcript payload ", 900)),
		})
	}
	corpus := neoReadThreadCorpus{ThreadID: "T-large-history", Source: "test", Title: "large history", Messages: messages}

	var captured []neoInferenceRequest
	rt := newNeoRuntime(&config.Config{})
	rt.inferStream = func(_ *neoRuntime, request neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		turn := len(captured)
		captured = append(captured, request)
		switch turn {
		case 0:
			return neoInferenceResult{ToolCalls: []neoToolCall{{ID: "TU-search-large", Name: "search_thread_messages", Input: map[string]any{"query": "target-marker"}}}}, nil
		case 1:
			return neoInferenceResult{ToolCalls: []neoToolCall{{ID: "TU-read-large-1", Name: "read_thread_messages", Input: map[string]any{"startIndex": 0, "count": 6}}}}, nil
		default:
			if got := neoReadThreadHistoryApproxChars(request.History); got > neoReadThreadHistoryTotalChars+2048 {
				t.Fatalf("request history chars = %d, want <= %d", got, neoReadThreadHistoryTotalChars+2048)
			}
			if !strings.Contains(neoHistoryTestText(request.History), "[truncated") {
				t.Fatalf("request history missing truncation marker after large repeated reads")
			}
			return neoInferenceResult{Text: neoReadThreadTestFinalJSON("[message 5] target-marker-5 is the latest marker.")}, nil
		}
	}
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-current-large", "T-current-large", neoActorRecord("actor-test", "thread-actor", "T-current-large"), nil)

	text, err := actor.executeLocalReadThreadAgent(neoPendingTool{ID: "TU-read", Name: "read_thread", Input: map[string]any{"threadID": corpus.ThreadID, "question": "Find the latest marker."}, AgentMode: "deep"}, actor.generation, corpus, "Find the latest marker.")
	if err != nil {
		t.Fatalf("executeLocalReadThreadAgent error: %v", err)
	}
	if !strings.Contains(text, "target-marker-5") {
		t.Fatalf("read_thread text = %q, want latest marker", text)
	}
	if len(captured) != 3 {
		t.Fatalf("captured requests = %d, want 3", len(captured))
	}
}

func TestNeoReadThreadOverviewIsOneShotThenRequiresSearch(t *testing.T) {
	corpus := neoReadThreadCorpus{ThreadID: "T-overview-once", Source: "test", Title: "overview loop", Messages: []neoReadThreadMessage{
		{Index: 0, Role: "user", MessageID: "M-0", Text: "Initial task mentions sidebar progress."},
		{Index: 1, Role: "assistant", MessageID: "M-1", Text: "Latest outcome says actor connection is fixed."},
	}}

	var captured []neoInferenceRequest
	rt := newNeoRuntime(&config.Config{})
	rt.inferStream = func(_ *neoRuntime, request neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		turn := len(captured)
		captured = append(captured, request)
		switch turn {
		case 0:
			if !neoReadThreadHasTools(request.Tools, "thread_overview", "search_thread_messages") || neoReadThreadHasTools(request.Tools, "read_thread_messages") {
				t.Fatalf("turn 0 tools = %#v", request.Tools)
			}
			return neoInferenceResult{ToolCalls: []neoToolCall{{ID: "TU-overview", Name: "thread_overview"}}}, nil
		case 1:
			if !neoReadThreadHasTools(request.Tools, "search_thread_messages") || neoReadThreadHasTools(request.Tools, "thread_overview") || neoReadThreadHasTools(request.Tools, "read_thread_messages") {
				t.Fatalf("turn 1 tools = %#v, want search only after overview", request.Tools)
			}
			return neoInferenceResult{ToolCalls: []neoToolCall{{ID: "TU-search", Name: "search_thread_messages", Input: map[string]any{"query": "actor connection sidebar progress"}}}}, nil
		case 2:
			if !neoReadThreadHasTools(request.Tools, "read_thread_messages") || neoReadThreadHasAnyTool(request.Tools, "thread_overview", "search_thread_messages") {
				t.Fatalf("turn 2 tools = %#v, want exact reads only after search", request.Tools)
			}
			return neoInferenceResult{ToolCalls: []neoToolCall{{ID: "TU-read", Name: "read_thread_messages", Input: map[string]any{"latest": true, "count": 2}}}}, nil
		default:
			return neoInferenceResult{Text: neoReadThreadTestFinalJSON("[message 1] actor connection is fixed.")}, nil
		}
	}
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-current-overview", "T-current-overview", neoActorRecord("actor-test", "thread-actor", "T-current-overview"), nil)

	text, err := actor.executeLocalReadThreadAgent(neoPendingTool{ID: "TU-read", Name: "read_thread", Input: map[string]any{"threadID": corpus.ThreadID, "question": "What happened with actor connection?"}, AgentMode: "deep"}, actor.generation, corpus, "What happened with actor connection?")
	if err != nil {
		t.Fatalf("executeLocalReadThreadAgent error: %v", err)
	}
	if !strings.Contains(text, "actor connection") {
		t.Fatalf("read_thread text = %q, want actor connection", text)
	}
	if len(captured) != 4 {
		t.Fatalf("captured requests = %d, want 4", len(captured))
	}
}

func TestNeoReadThreadOverviewClipsToolResultDetails(t *testing.T) {
	corpus := neoReadThreadCorpus{ThreadID: "T-overview-clip", Source: "test", Title: "overview clip", Messages: []neoReadThreadMessage{
		{Index: 0, Role: "user", MessageID: "M-tool-result", Text: "tool result summary " + strings.Repeat("x", 2000), ToolResults: []neoReadThreadToolResult{{
			ToolUseID: "TU-large",
			Status:    "done",
			Text:      "large tool output " + strings.Repeat("y", 6000),
		}}},
	}}

	overview := neoReadThreadOverview(corpus)
	raw, _ := json.Marshal(overview)
	if len(raw) > 3000 {
		t.Fatalf("overview bytes = %d, want clipped result: %s", len(raw), raw)
	}
	if !strings.Contains(string(raw), "[truncated") {
		t.Fatalf("overview missing truncation marker: %s", raw)
	}
}

func TestNeoReadThreadExcerptHandlesUnicodeCaseExpansion(t *testing.T) {
	text := strings.Repeat("\u0130", 80) + " target survives after expansion"
	excerpt := neoReadThreadExcerpt(text, "target", neoReadThreadSearchTerms("target"), 32)
	if !strings.Contains(excerpt, "target survives") {
		t.Fatalf("excerpt = %q, want match after unicode case expansion", excerpt)
	}
}

func BenchmarkNeoReadThreadSearchLargeCorpus(b *testing.B) {
	messages := make([]neoReadThreadMessage, 1386)
	for i := range messages {
		text := strings.Repeat("build output without the requested evidence\n", 140)
		if i%100 == 0 {
			text += "concurrency corruption fixes and deployment constraints"
		}
		messages[i] = neoReadThreadMessage{Index: i, Role: "assistant", MessageID: fmt.Sprintf("M-%d", i), Text: text}
	}
	corpus := neoReadThreadCorpus{ThreadID: "T-large-search", Source: "benchmark", Messages: messages}
	input := map[string]any{"query": "concurrency corruption fixes", "limit": neoReadThreadSearchLimitMax}

	b.ReportAllocs()
	b.SetBytes(int64(len(messages) * len(messages[0].Text)))
	b.ResetTimer()
	for range b.N {
		if _, err := neoReadThreadSearch(corpus, input); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNeoReadThreadFileCorpus(b *testing.B) {
	dir := b.TempDir()
	threadID := "T-019f7000-0000-7000-8000-0000000000b1"
	messages := make([]any, 1000)
	for i := range messages {
		messages[i] = map[string]any{
			"role":      "assistant",
			"messageId": fmt.Sprintf("M-benchmark-%d", i),
			"content": []any{
				map[string]any{"type": "text", "text": strings.Repeat("thread benchmark content ", 80)},
				map[string]any{"type": "tool_use", "id": fmt.Sprintf("TU-benchmark-%d", i), "name": "Read", "input": map[string]any{"path": fmt.Sprintf("src/file-%d.go", i), "line_start": i, "line_end": i + 20}, "complete": true},
				map[string]any{"type": "tool_result", "toolUseID": fmt.Sprintf("TU-benchmark-%d", i), "run": map[string]any{"status": "done", "result": strings.Repeat("tool output ", 120)}},
			},
		}
	}
	raw, err := json.Marshal(map[string]any{
		"id":       threadID,
		"title":    "large persisted thread",
		"meta":     map[string]any{"ownerUserId": neoLocalOwnerUserID},
		"messages": messages,
	})
	if err != nil {
		b.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, threadID+".json"), raw, 0o600); err != nil {
		b.Fatal(err)
	}
	actor := &neoActor{
		runtime: &neoRuntime{threadDir: dir},
		meta:    map[string]any{"ownerUserId": neoLocalOwnerUserID},
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	for range b.N {
		corpus, ok := actor.localReadThreadFileCorpus(threadID)
		if !ok || len(corpus.Messages) != len(messages) {
			b.Fatalf("corpus messages = %d, ok=%v", len(corpus.Messages), ok)
		}
	}
}

func BenchmarkNeoReadThreadLiveCorpus(b *testing.B) {
	threadID := "T-019f7000-0000-7000-8000-0000000000b2"
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-read-thread-live-benchmark", "thread-actor", threadID, threadID, neoActorRecord("actor-read-thread-live-benchmark", "thread-actor", threadID), nil)
	actor.messages = make([]neoMessage, 1000)
	for i := range actor.messages {
		actor.messages[i] = neoMessage{
			Role:      "assistant",
			MessageID: fmt.Sprintf("M-benchmark-%d", i),
			Content: []any{
				map[string]any{"type": "text", "text": strings.Repeat("thread benchmark content ", 80)},
				map[string]any{"type": "tool_use", "id": fmt.Sprintf("TU-benchmark-%d", i), "name": "Read", "input": map[string]any{"path": fmt.Sprintf("src/file-%d.go", i), "line_start": i, "line_end": i + 20}, "complete": true},
				map[string]any{"type": "tool_result", "toolUseID": fmt.Sprintf("TU-benchmark-%d", i), "run": map[string]any{"status": "done", "result": strings.Repeat("tool output ", 120)}},
			},
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		corpus, ok := actor.liveReadThreadCorpus(threadID)
		if !ok || len(corpus.Messages) != len(actor.messages) {
			b.Fatalf("corpus messages = %d, ok=%v", len(corpus.Messages), ok)
		}
	}
}

func TestNeoReadThreadCompletesMissingRetrievalStagesBeforeFinal(t *testing.T) {
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
			if !strings.Contains(neoHistoryTestText(request.History), "Runtime-completed search_thread_messages") {
				t.Fatalf("turn %d history missing automatic search evidence: %s", turn, neoHistoryTestText(request.History))
			}
			return neoInferenceResult{Text: neoReadThreadTestFinalJSON("still too early")}, nil
		default:
			history := neoHistoryTestText(request.History)
			if !strings.Contains(history, "read_thread_messages evidence:") || !strings.Contains(history, "Authoritative latest visible target-thread messages") {
				t.Fatalf("turn %d history missing automatic read/latest evidence: %s", turn, history)
			}
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
	if len(captured) != 3 {
		t.Fatalf("captured requests = %d, want 3 staged requests", len(captured))
	}
}

func TestNeoReadThreadDoesNotEnterOpenEndedRetrievalLoop(t *testing.T) {
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
		turn := len(captured)
		captured = append(captured, request)
		switch turn {
		case 0:
			return neoInferenceResult{ToolCalls: []neoToolCall{{ID: newNeoToolCallID(), Name: "search_thread_messages", Input: map[string]any{"query": "approach"}}}}, nil
		case 1:
			return neoInferenceResult{ToolCalls: []neoToolCall{{ID: newNeoToolCallID(), Name: "read_thread_messages", Input: map[string]any{"startIndex": 0, "count": 1}}}}, nil
		case 2:
			if len(request.Tools) != 0 || request.ResponseMimeType != "application/json" {
				t.Fatalf("final request = tools:%#v mime:%q", request.Tools, request.ResponseMimeType)
			}
			return neoInferenceResult{Text: neoReadThreadTestFinalJSON("[message 2] approach A is superseded by approach C.")}, nil
		default:
			t.Fatalf("unexpected read_thread request %d", turn)
			return neoInferenceResult{}, nil
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
	if len(captured) != 3 {
		t.Fatalf("captured requests = %d, want search, read, final", len(captured))
	}
}

func TestNeoReadThreadMalformedRetrievalFallsBackToAutomaticStages(t *testing.T) {
	corpus := neoReadThreadCorpus{ThreadID: "T-malformed-retrieval", Source: "test", Messages: []neoReadThreadMessage{
		{Index: 0, Role: "user", MessageID: "M-early", Text: "Initial decision: use approach A."},
		{Index: 1, Role: "user", MessageID: "M-latest", Text: "Latest decision: use approach B."},
	}}
	requests := 0
	rt := newNeoRuntime(&config.Config{})
	rt.inferStream = func(_ *neoRuntime, request neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		requests++
		switch requests {
		case 1, 2:
			return neoInferenceResult{ToolCalls: []neoToolCall{{ID: newNeoToolCallID(), Name: "search_thread_messages", Input: map[string]any{}}}}, nil
		case 3:
			if len(request.Tools) != 0 {
				t.Fatalf("forced final retained retrieval tools: %#v", request.Tools)
			}
			return neoInferenceResult{Text: neoReadThreadTestFinalJSON("[message 1] use approach B.")}, nil
		default:
			t.Fatalf("malformed retrieval entered request %d", requests)
			return neoInferenceResult{}, nil
		}
	}
	actor := newNeoActor(rt, "actor-malformed-retrieval", "thread-actor", "T-current", "T-current", neoActorRecord("actor-malformed-retrieval", "thread-actor", "T-current"), nil)

	text, err := actor.executeLocalReadThreadAgent(neoPendingTool{ID: "TU-read", Name: "read_thread", AgentMode: "deep"}, actor.generation, corpus, "Which approach survived?")
	if err != nil || !strings.Contains(text, "approach B") || requests != 3 {
		t.Fatalf("malformed retrieval text=%q error=%v requests=%d", text, err, requests)
	}
}

func TestNeoReadThreadForcedFinalFallsBackToMarkdown(t *testing.T) {
	dir := t.TempDir()
	oldStoreDir := neoAmpDataDir
	neoAmpDataDir = func() string { return dir }
	t.Cleanup(func() { neoAmpDataDir = oldStoreDir })

	currentThreadID := "T-019e65c0-0310-77a8-b233-4b84d9c06137"
	targetThreadID := "T-019e65c0-0310-77a8-b233-4b84d9c06138"
	neoReadThreadWriteLocalThread(t, dir, targetThreadID, []any{
		map[string]any{"role": "user", "messageId": "M-early", "content": []any{map[string]any{"type": "text", "text": "Initial plan: approach A."}}},
		map[string]any{"role": "user", "messageId": "M-latest", "content": []any{map[string]any{"type": "text", "text": "Latest decision: approach D survived."}}},
	})

	var captured []neoInferenceRequest
	rt := newNeoRuntime(&config.Config{})
	rt.inferStream = func(_ *neoRuntime, request neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		captured = append(captured, request)
		if len(request.Tools) == 0 {
			if request.ResponseMimeType != "application/json" || request.ResponseJSONSchema == nil {
				t.Fatalf("forced final response format = %q/%#v, want json schema", request.ResponseMimeType, request.ResponseJSONSchema)
			}
			return neoInferenceResult{Text: "```markdown\n* [message 1] approach D survived.\n```"}, nil
		}
		switch len(captured) {
		case 1:
			return neoInferenceResult{ToolCalls: []neoToolCall{{ID: newNeoToolCallID(), Name: "search_thread_messages", Input: map[string]any{"query": "approach"}}}}, nil
		case 2:
			return neoInferenceResult{ToolCalls: []neoToolCall{{ID: newNeoToolCallID(), Name: "read_thread_messages", Input: map[string]any{"latest": true, "count": 2}}}}, nil
		default:
			return neoInferenceResult{Text: "* premature markdown answer"}, nil
		}
	}
	actor := newNeoActor(rt, "actor-test", "thread-actor", currentThreadID, currentThreadID, neoActorRecord("actor-test", "thread-actor", currentThreadID), nil)
	actor.executorBootstrapComplete = true

	text, err := actor.executeLocalReadThread(neoPendingTool{ID: "TU-read", Name: "read_thread", Input: map[string]any{"threadID": targetThreadID, "question": "Which approach survived?"}, AgentMode: "deep"}, actor.generation)
	if err != nil {
		t.Fatalf("executeLocalReadThread error: %v", err)
	}
	if text != "* [message 1] approach D survived." {
		t.Fatalf("read_thread text = %q, want markdown fallback", text)
	}
	if len(captured) != 3 {
		t.Fatalf("captured requests = %d, want 3", len(captured))
	}
}

func TestNeoReadThreadMarkdownFallbackContentBoundaries(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"plain markdown", "[message 1] latest decision", "[message 1] latest decision"},
		{"numeric bracket markdown", "[1] latest decision", "[1] latest decision"},
		{"dash bracket markdown", "[-] latest decision", "[-] latest decision"},
		{"boolean bracket markdown", "[true finding] latest decision", "[true finding] latest decision"},
		{"double bracket markdown", "[[decision]] latest decision", "[[decision]] latest decision"},
		{"quoted bracket markdown", "[\"decision\"] latest decision", "[\"decision\"] latest decision"},
		{"plain scalar", "true", "true"},
		{"fenced markdown", "```markdown\n[message 1] latest decision\n```", "[message 1] latest decision"},
		{"leading prose fenced markdown", "Here is the relevant content:\n```markdown\n[message 1] latest decision\n```", "[message 1] latest decision"},
		{"fenced text", "```text\n[message 1] latest decision\n```", "[message 1] latest decision"},
		{"json object", "{}", ""},
		{"json array", "[]", ""},
		{"empty relevant content", `{"relevantContent":""}`, ""},
		{"truncated json", `{"relevantContent":"partial`, ""},
		{"truncated json array", `[{"relevantContent":"partial"`, ""},
		{"fenced json", "```json\n{\"relevantContent\":\"partial\"\n```", ""},
		{"unterminated fence", "```markdown\n[message 1] latest decision", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := neoReadThreadMarkdownFallbackContent(tt.input); got != tt.want {
				t.Fatalf("neoReadThreadMarkdownFallbackContent() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestNeoReadThreadGroundedMarkdownFallbackRequiresMessageCitation(t *testing.T) {
	corpus := neoReadThreadCorpus{Messages: []neoReadThreadMessage{
		{Index: 1},
		{Index: 2},
		{Index: 3},
	}}
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"cited markdown", "* [message 1] approach D survived.", "* [message 1] approach D survived."},
		{"tab cited markdown", "* [message\t2] approach D survived.", "* [message\t2] approach D survived."},
		{"spaced cited markdown", "* [message 3 ] approach D survived.", "* [message 3 ] approach D survived."},
		{"fenced cited markdown", "```markdown\n[message 2] verified command passed\n```", "[message 2] verified command passed"},
		{"plural message label", "* [messages] approach D survived.", ""},
		{"message prefix only", "* [message] approach D survived.", ""},
		{"message word", "* [messagepack] approach D survived.", ""},
		{"message without closing bracket", "* [message 1 approach D survived.", ""},
		{"message nonnumeric", "* [message one] approach D survived.", ""},
		{"nonexistent message", "* [message 999] approach D survived.", ""},
		{"ungrounded progress note", "## Verifying Deployment Steps\n\nI'm now focused on final validation procedures.", ""},
		{"json still rejected", `{"relevantContent":"partial"`, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := neoReadThreadGroundedMarkdownFallbackContent(tt.input, corpus); got != tt.want {
				t.Fatalf("neoReadThreadGroundedMarkdownFallbackContent() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestNeoReadThreadGroundedRelevantContentRequiresVisibleMessageCitation(t *testing.T) {
	corpus := neoReadThreadCorpus{Messages: []neoReadThreadMessage{{Index: 2}, {Index: 7}}}

	for _, tt := range []struct {
		name    string
		content string
		wantErr bool
	}{
		{name: "visible sparse index", content: "[message 7] latest decision"},
		{name: "hidden sparse gap", content: "[message 3] unsupported decision", wantErr: true},
		{name: "nonexistent index", content: "[message 999] unsupported decision", wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := neoReadThreadGroundedRelevantContent(neoReadThreadTestFinalJSON(tt.content), corpus)
			if (err != nil) != tt.wantErr {
				t.Fatalf("neoReadThreadGroundedRelevantContent() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
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

func TestNeoReadThreadStaleGenerationReturnsGracefully(t *testing.T) {
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
		captured = append(captured, request)
		if len(captured) == 2 {
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
	if len(captured) != 2 {
		t.Fatalf("captured requests = %d, want cancellation after the second staged request", len(captured))
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
