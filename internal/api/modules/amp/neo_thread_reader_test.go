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
	if !strings.Contains(request.SystemPromptOverride, "read_thread subagent") {
		t.Fatalf("system prompt override = %q", request.SystemPromptOverride)
	}
	if !neoReadThreadHasTools(request.Tools, "search_thread_messages") {
		t.Fatalf("tools = %#v, want read_thread search tool", request.Tools)
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
	if _, _, err := neoReadThreadRead(corpus, map[string]any{"startIndex": 99}); err == nil || !strings.Contains(err.Error(), "outside thread message range") {
		t.Fatalf("read past sparse tail err = %v, want out-of-range", err)
	}
	if _, _, err := neoReadThreadRead(corpus, map[string]any{"afterIndex": 99}); err == nil || !strings.Contains(err.Error(), "outside thread message range") {
		t.Fatalf("read after sparse tail err = %v, want out-of-range", err)
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
	corpus := neoReadThreadCorpus{ThreadID: "T-final-tail", Source: "test", Title: "final tail", Messages: []neoReadThreadMessage{
		{Index: 0, Role: "user", MessageID: "M-old", Text: "Old summary says wait for review cooldown."},
		{Index: 1, Role: "assistant", MessageID: "M-merged", Text: "Latest state: PR #1003 was merged and local main is synced."},
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
	if !strings.Contains(historyText, "Authoritative latest visible target-thread messages") || !strings.Contains(historyText, "PR #1003 was merged") {
		t.Fatalf("forced final history missing latest visible tail: %s", historyText)
	}
	if !strings.Contains(historyText, neoReadThreadFinalPrompt) {
		t.Fatalf("forced final history missing final prompt: %s", historyText)
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
		case 2:
			return neoInferenceResult{ToolCalls: []neoToolCall{{ID: "TU-read-large-2", Name: "read_thread_messages", Input: map[string]any{"startIndex": 0, "count": 6}}}}, nil
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
	if len(captured) != 4 {
		t.Fatalf("captured requests = %d, want 4", len(captured))
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
			if !neoReadThreadHasTools(request.Tools, "thread_overview", "search_thread_messages", "read_thread_messages") {
				t.Fatalf("turn 0 tools = %#v", request.Tools)
			}
			return neoInferenceResult{ToolCalls: []neoToolCall{{ID: "TU-overview", Name: "thread_overview"}}}, nil
		case 1:
			if !neoReadThreadHasTools(request.Tools, "search_thread_messages") || neoReadThreadHasTools(request.Tools, "thread_overview") || neoReadThreadHasTools(request.Tools, "read_thread_messages") {
				t.Fatalf("turn 1 tools = %#v, want search only after overview", request.Tools)
			}
			return neoInferenceResult{ToolCalls: []neoToolCall{{ID: "TU-search", Name: "search_thread_messages", Input: map[string]any{"query": "actor connection sidebar progress"}}}}, nil
		case 2:
			if !neoReadThreadHasTools(request.Tools, "search_thread_messages", "read_thread_messages") || neoReadThreadHasTools(request.Tools, "thread_overview") {
				t.Fatalf("turn 2 tools = %#v, want search/read without overview", request.Tools)
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
	if len(captured) != 4 {
		t.Fatalf("captured requests = %d, want 4", len(captured))
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
		{"ungrounded progress note", "## Verifying Deployment Steps\n\nI'm now focused on final validation procedures.", ""},
		{"json still rejected", `{"relevantContent":"partial"`, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := neoReadThreadGroundedMarkdownFallbackContent(tt.input); got != tt.want {
				t.Fatalf("neoReadThreadGroundedMarkdownFallbackContent() = %q, want %q", got, tt.want)
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
