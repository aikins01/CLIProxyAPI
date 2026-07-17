package amp

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
)

func TestNeoSubagentRegistryMatchesLocalContract(t *testing.T) {
	cases := []struct {
		tool     string
		provider string
		model    string
		effort   string
		tools    []string
	}{
		{"finder", "openai", "gpt-5.6-terra", "low", []string{"Grep", "glob", "Read"}},
		{"oracle", "anthropic", "claude-fable-5", "high", []string{"Read", "Grep", "glob", "web_search", "read_web_page", "read_thread", "find_thread"}},
		{"librarian", "openai", "gpt-5.6-sol", "none", []string{"read_github", "search_github", "commit_search", "diff", "list_directory_github", "list_repositories", "glob_github"}},
		{"run_check", "openai", "gpt-5.6-terra", "low", []string{"Read", "Grep", "glob", "Bash"}},
		// Task inherits the parent model (empty route) and includes finder (a nested subagent).
		{"Task", "", "", "", []string{"Read", "Bash", "edit_file", "create_file", "read_web_page", "web_search", "finder", "skill", "view_media"}},
	}
	for _, tc := range cases {
		def, ok := neoSubagentDefFor(tc.tool)
		if !ok {
			t.Fatalf("subagent %q missing from registry", tc.tool)
		}
		if def.Route.Provider != tc.provider || def.Route.Model != tc.model {
			t.Fatalf("%s route = %s/%s, want %s/%s", tc.tool, def.Route.Provider, def.Route.Model, tc.provider, tc.model)
		}
		if def.ReasoningEffort != tc.effort {
			t.Fatalf("%s reasoning effort = %q, want %q", tc.tool, def.ReasoningEffort, tc.effort)
		}
		if strings.Join(def.IncludeTools, ",") != strings.Join(tc.tools, ",") {
			t.Fatalf("%s includeTools = %v, want %v", tc.tool, def.IncludeTools, tc.tools)
		}
		if strings.TrimSpace(def.SystemPrompt) == "" {
			t.Fatalf("%s has empty system prompt", tc.tool)
		}
	}
}

func setNeoFinderWorkspaceForTest(t *testing.T, actor *neoActor) string {
	t.Helper()
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, ".git"), 0o700); err != nil {
		t.Fatalf("create finder repository marker: %v", err)
	}
	actor.environment = map[string]any{
		"workingDirectory": root,
		"workspaceRoot":    root,
	}
	return root
}

func captureNeoSubagentRouteForTest(t *testing.T, toolName, name string, cfg *config.Config, agentMode string, settings map[string]any) neoModelRoute {
	t.Helper()
	rt := newNeoRuntime(cfg)
	actor := newNeoActor(rt, "actor-"+name, "thread-actor", "T-"+name, "T-"+name, neoActorRecord("actor-"+name, "thread-actor", "T-"+name), nil)
	actor.currentAgentMode = agentMode
	actor.settings = settings

	var seen []neoModelRoute
	rt.inferStream = func(_ *neoRuntime, req neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		if req.ModelRouteOverride == nil {
			t.Fatalf("%s subagent missing model route override", req.ParentToolCallID)
		}
		seen = append(seen, *req.ModelRouteOverride)
		return neoInferenceResult{Text: "done"}, nil
	}

	input := map[string]any{"prompt": "delegate"}
	if toolName == "oracle" {
		input = map[string]any{"task": "advise"}
	}
	if _, err := actor.executeSubagentRun(toolName, input, "TU-"+name, "M-1", actor.generation, 0, ""); err != nil {
		t.Fatalf("%s subagent failed: %v", toolName, err)
	}
	if len(seen) != 1 {
		t.Fatalf("%s routes = %#v, want exactly one route", toolName, seen)
	}
	return seen[0]
}

func TestNeoTaskSubagentInheritsConfigModeModel(t *testing.T) {
	cases := []struct {
		name      string
		cfg       *config.Config
		agentMode string
		settings  map[string]any
		provider  string
		model     string
	}{
		{
			name:      "configured-smart",
			cfg:       &config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{ModeModels: map[string]string{"smart": "anthropic/claude-fable-5"}}}},
			agentMode: "smart",
			provider:  "anthropic",
			model:     "claude-fable-5",
		},
		{name: "default-smart", cfg: &config.Config{}, agentMode: "smart", provider: "anthropic", model: "claude-opus-4-8"},
		{name: "default-deep", cfg: &config.Config{}, agentMode: "deep", provider: "openai", model: "gpt-5.5"},
		{
			name:      "explicit-model",
			cfg:       &config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{ModeModels: map[string]string{"smart": "anthropic/claude-fable-5"}}}},
			agentMode: "smart",
			settings:  map[string]any{"internal.model": map[string]any{"smart": "openai/gpt-5.5"}},
			provider:  "openai",
			model:     "gpt-5.5",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := captureNeoSubagentRouteForTest(t, "Task", tc.name, tc.cfg, tc.agentMode, tc.settings)
			if got.Provider != tc.provider || got.Model != tc.model {
				t.Fatalf("Task route = %#v, want %s/%s", got, tc.provider, tc.model)
			}
		})
	}
}

func TestNeoOracleSubagentIgnoresConfigModeModel(t *testing.T) {
	cfg := &config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{
		ModeModels: map[string]string{"smart": "openai/gpt-5.5"},
	}}}
	got := captureNeoSubagentRouteForTest(t, "oracle", "oracle-mode", cfg, "smart", nil)
	if got.Provider != "anthropic" || got.Model != "claude-fable-5" {
		t.Fatalf("oracle route = %#v, want fixed route anthropic/claude-fable-5", got)
	}
}

func TestNeoFinderSubagentUsesGPT56TerraLow(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-finder", "thread-actor", "T-finder", "T-finder", neoActorRecord("actor-finder", "thread-actor", "T-finder"), nil)
	setNeoFinderWorkspaceForTest(t, actor)
	actor.currentAgentMode = "high"
	actor.settings = map[string]any{"reasoning.effort": "xhigh"}
	actor.tools = map[string]neoToolSpec{
		"Read": {Name: "Read", InputSchema: map[string]any{"type": "object"}},
		"Grep": {Name: "Grep", InputSchema: map[string]any{"type": "object"}},
		"glob": {Name: "glob", InputSchema: map[string]any{"type": "object"}},
	}

	var seen []neoInferenceRequest
	rt.inferStream = func(_ *neoRuntime, req neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		seen = append(seen, req)
		return neoInferenceResult{Text: "done"}, nil
	}

	if _, err := actor.executeSubagentRun("finder", map[string]any{"query": "find local actor routing"}, "TU-finder", "M-1", actor.generation, 0, ""); err != nil {
		t.Fatalf("finder subagent failed: %v", err)
	}
	if len(seen) != 1 {
		t.Fatalf("finder requests = %#v, want exactly one", seen)
	}
	route := seen[0].ModelRouteOverride
	if route == nil || route.Provider != "openai" || route.Model != "gpt-5.6-terra" {
		t.Fatalf("finder route = %#v, want openai/gpt-5.6-terra", route)
	}
	if seen[0].ReasoningEffort != "low" || stringValue(seen[0].Settings["reasoning.effort"]) != "low" {
		t.Fatalf("finder effort request=%q settings=%#v, want low", seen[0].ReasoningEffort, seen[0].Settings)
	}
	if !neoSubagentHasTools(seen[0].Tools, "Read", "Grep", "glob") {
		t.Fatalf("finder tools = %#v, want search tools", seen[0].Tools)
	}
	if def, _ := neoSubagentDefFor("finder"); def.MaxTurns != 6 {
		t.Fatalf("finder max turns = %d, want 6", def.MaxTurns)
	}
}

func TestNeoSubagentForcedSynthesisPropagatesProviderStopReason(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-finder", "thread-actor", "T-finder", "T-finder", neoActorRecord("actor-finder", "thread-actor", "T-finder"), nil)
	setNeoFinderWorkspaceForTest(t, actor)
	actor.currentAgentMode = "smart"
	calls := 0
	rt.inferStream = func(_ *neoRuntime, _ neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		calls++
		if calls == 1 {
			return neoInferenceResult{}, nil
		}
		return neoInferenceResult{Text: "partial", StopReason: "max_tokens"}, nil
	}

	text, err := actor.executeSubagentRun("finder", map[string]any{"query": "find local actor routing"}, "TU-finder", "M-1", actor.generation, 0, "")
	if err == nil || !strings.Contains(err.Error(), "max_tokens") {
		t.Fatalf("forced synthesis error = %v, want max_tokens", err)
	}
	if text != "" || calls != 2 {
		t.Fatalf("forced synthesis result = text:%q calls:%d, want empty text after two calls", text, calls)
	}
}

func TestNeoFinderConcurrencyLimitFailsFastAndRecovers(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-finder-limit", "thread-actor", "T-finder-limit", "T-finder-limit", neoActorRecord("actor-finder-limit", "thread-actor", "T-finder-limit"), nil)
	setNeoFinderWorkspaceForTest(t, actor)
	entered := make(chan struct{}, neoFinderMaxConcurrentRuns+1)
	release := make(chan struct{})
	rt.inferStream = func(_ *neoRuntime, _ neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		entered <- struct{}{}
		<-release
		return neoInferenceResult{Text: "done"}, nil
	}

	errs := make(chan error, neoFinderMaxConcurrentRuns)
	for i := 0; i < neoFinderMaxConcurrentRuns; i++ {
		go func(index int) {
			_, err := actor.executeSubagentRun("finder", map[string]any{"query": "search"}, "TU-finder-"+strconv.Itoa(index), "M-1", actor.generation, 0, "")
			errs <- err
		}(i)
	}
	for i := 0; i < neoFinderMaxConcurrentRuns; i++ {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("finder did not enter inference")
		}
	}

	started := time.Now()
	_, err := actor.executeSubagentRun("finder", map[string]any{"query": "third search"}, "TU-finder-third", "M-1", actor.generation, 0, "")
	if err == nil || !strings.Contains(err.Error(), "concurrency limit") {
		t.Fatalf("third finder error = %v, want concurrency limit", err)
	}
	if time.Since(started) > 250*time.Millisecond {
		t.Fatalf("third finder waited %s instead of failing fast", time.Since(started))
	}

	close(release)
	for i := 0; i < neoFinderMaxConcurrentRuns; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("active finder failed: %v", err)
		}
	}
	if !neoWaitFor(time.Second, func() bool {
		actor.mu.Lock()
		defer actor.mu.Unlock()
		return actor.activeFinderRuns == 0
	}) {
		t.Fatal("finder concurrency slots were not released")
	}
	if _, err := actor.executeSubagentRun("finder", map[string]any{"query": "recovered"}, "TU-finder-recovered", "M-1", actor.generation, 0, ""); err != nil {
		t.Fatalf("finder did not recover after slots were released: %v", err)
	}
}

func TestNeoFinderCancellationReleasesGenerationSlots(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-finder-generation", "thread-actor", "T-finder-generation", "T-finder-generation", neoActorRecord("actor-finder-generation", "thread-actor", "T-finder-generation"), nil)
	setNeoFinderWorkspaceForTest(t, actor)
	entered := make(chan context.Context, neoFinderMaxConcurrentRuns)
	releaseOld := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseOld) }) })
	var callsMu sync.Mutex
	calls := 0
	rt.inferStream = func(_ *neoRuntime, request neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		callsMu.Lock()
		calls++
		call := calls
		callsMu.Unlock()
		if call <= neoFinderMaxConcurrentRuns {
			entered <- request.Context
			<-releaseOld
			return neoInferenceResult{Text: "stale"}, nil
		}
		return neoInferenceResult{Text: "current"}, nil
	}

	oldDone := make(chan error, neoFinderMaxConcurrentRuns)
	oldGeneration := actor.generation
	for index := 0; index < neoFinderMaxConcurrentRuns; index++ {
		go func(index int) {
			_, err := actor.executeSubagentRun("finder", map[string]any{"query": "old"}, "TU-old-"+strconv.Itoa(index), "M-old", oldGeneration, 0, "")
			oldDone <- err
		}(index)
	}
	contexts := make([]context.Context, 0, neoFinderMaxConcurrentRuns)
	for len(contexts) < neoFinderMaxConcurrentRuns {
		select {
		case ctx := <-entered:
			contexts = append(contexts, ctx)
		case <-time.After(time.Second):
			t.Fatal("old finder inference did not start")
		}
	}

	actor.cancel()
	for _, ctx := range contexts {
		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
			t.Fatal("cancelled finder provider context remained active")
		}
	}

	currentDone := make(chan error, 1)
	go func() {
		_, err := actor.executeSubagentRun("finder", map[string]any{"query": "current"}, "TU-current", "M-current", actor.generation, 0, "")
		currentDone <- err
	}()
	select {
	case err := <-currentDone:
		if err != nil {
			t.Fatalf("current finder failed while stale providers were blocked: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("current finder waited for stale generation slots")
	}

	releaseOnce.Do(func() { close(releaseOld) })
	for index := 0; index < neoFinderMaxConcurrentRuns; index++ {
		if err := <-oldDone; err != nil {
			t.Fatalf("stale finder returned error: %v", err)
		}
	}
	actor.mu.Lock()
	defer actor.mu.Unlock()
	if actor.activeFinderRuns != 0 || len(actor.finderRuns) != 0 {
		t.Fatalf("finder runs after stale providers returned = active:%d tracked:%d", actor.activeFinderRuns, len(actor.finderRuns))
	}
}

func TestNeoFinderCancellationBeforeTurnSetupEmitsNoChildWork(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-finder-setup-cancel", "thread-actor", "T-finder-setup-cancel", "T-finder-setup-cancel", neoActorRecord("actor-finder-setup-cancel", "thread-actor", "T-finder-setup-cancel"), nil)
	root := setNeoFinderWorkspaceForTest(t, actor)
	calls := []neoToolCall{
		{ID: "leaf-1", Name: "Grep", Input: map[string]any{"pattern": "one"}},
		{ID: "leaf-2", Name: "Grep", Input: map[string]any{"pattern": "two"}},
	}
	generation := actor.generation
	actor.emissionMu.Lock()
	started := make(chan struct{})
	done := make(chan []neoSubagentToolExchange, 1)
	go func() {
		close(started)
		done <- actor.execFinderTurnTools(context.Background(), calls, root, root, "TU-finder", generation)
	}()
	<-started
	actor.mu.Lock()
	actor.advanceGenerationLocked()
	actor.mu.Unlock()
	actor.emissionMu.Unlock()

	select {
	case exchanges := <-done:
		if len(exchanges) != 0 {
			t.Fatalf("stale finder exchanges = %d, want 0", len(exchanges))
		}
	case <-time.After(time.Second):
		t.Fatal("stale finder turn setup did not return")
	}
	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.messages) != 0 || len(actor.subagentWaiters) != 0 || len(actor.subagentTools) != 0 {
		t.Fatalf("stale finder setup emitted work: messages=%d waiters=%d tools=%d", len(actor.messages), len(actor.subagentWaiters), len(actor.subagentTools))
	}
}

func TestNeoFinderProgressCannotOvertakeCancellation(t *testing.T) {
	actor := newNeoActor(nil, "actor-finder-progress-cancel", "thread-actor", "T-finder-progress-cancel", "T-finder-progress-cancel", neoActorRecord("actor-finder-progress-cancel", "thread-actor", "T-finder-progress-cancel"), nil)
	waiter := make(chan map[string]any, 1)
	actor.subagentWaiters = map[string]chan map[string]any{}
	actor.subagentWaiters["leaf-progress"] = waiter
	actor.subagentTools["leaf-progress"] = neoPendingTool{ID: "leaf-progress", Name: "Grep", ParentToolCallID: "TU-finder"}

	actor.emissionMu.Lock()
	actor.mu.Lock()
	started := make(chan struct{})
	done := make(chan bool, 1)
	go func() {
		close(started)
		done <- actor.routeSubagentLeafToolResult("leaf-progress", map[string]any{"status": "running", "output": "searching"})
	}()
	<-started
	time.Sleep(10 * time.Millisecond)
	actor.mu.Unlock()
	time.Sleep(20 * time.Millisecond)
	actor.mu.Lock()
	actor.advanceGenerationLocked()
	cancellation := actor.takeSubagentCancellationLocked()
	actor.mu.Unlock()
	actor.emissionMu.Unlock()
	actor.finishSubagentCancellation(cancellation, "user:cancelled", "user_canceled")

	select {
	case routed := <-done:
		if routed {
			t.Fatal("stale finder progress was routed after cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("stale finder progress did not return")
	}
	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.messages) != 0 || len(actor.subagentWaiters) != 0 || len(actor.subagentTools) != 0 {
		t.Fatalf("stale finder progress remained: messages=%d waiters=%d tools=%d", len(actor.messages), len(actor.subagentWaiters), len(actor.subagentTools))
	}
}

func TestNeoFinderTurnCapsConcurrentToolCallsAndCompletesEveryCall(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-finder-turn", "thread-actor", "T-finder-turn", "T-finder-turn", neoActorRecord("actor-finder-turn", "thread-actor", "T-finder-turn"), nil)
	root := setNeoFinderWorkspaceForTest(t, actor)
	calls := make([]neoToolCall, 8)
	for i := range calls {
		calls[i] = neoToolCall{ID: "leaf-" + strconv.Itoa(i), Name: "Grep", Input: map[string]any{"pattern": "neoActor"}}
	}

	done := make(chan []neoSubagentToolExchange, 1)
	go func() {
		done <- actor.execFinderTurnTools(context.Background(), calls, root, root, "TU-finder", actor.generation)
	}()
	if !neoWaitFor(time.Second, func() bool {
		actor.mu.Lock()
		defer actor.mu.Unlock()
		return len(actor.subagentWaiters) == neoFinderMaxToolCallsPerTurn
	}) {
		t.Fatal("finder did not lease four leaf tools concurrently")
	}
	actor.mu.Lock()
	leasedIDs := make([]string, 0, len(actor.subagentWaiters))
	for toolCallID := range actor.subagentWaiters {
		leasedIDs = append(leasedIDs, toolCallID)
	}
	actor.mu.Unlock()
	for _, toolCallID := range leasedIDs {
		if !actor.routeSubagentLeafToolResult(toolCallID, map[string]any{"status": "done", "output": "match"}) {
			t.Fatalf("accepted tool %s did not consume its result", toolCallID)
		}
	}

	var exchanges []neoSubagentToolExchange
	select {
	case exchanges = <-done:
	case <-time.After(time.Second):
		t.Fatal("finder turn did not finish")
	}
	if len(exchanges) != len(calls) {
		t.Fatalf("tool exchanges = %d, want %d", len(exchanges), len(calls))
	}
	for i, exchange := range exchanges {
		if exchange.Call.ID != calls[i].ID {
			t.Fatalf("exchange %d call = %q, want %q", i, exchange.Call.ID, calls[i].ID)
		}
		status := stringValue(exchange.Run["status"])
		if i < neoFinderMaxToolCallsPerTurn && status != "done" {
			t.Fatalf("accepted exchange %s status = %q", exchange.Call.ID, status)
		}
		if i >= neoFinderMaxToolCallsPerTurn && status != "error" {
			t.Fatalf("excess exchange %s status = %q", exchange.Call.ID, status)
		}
	}

	terminalCounts := map[string]int{}
	actor.mu.Lock()
	for _, message := range actor.messages {
		if message.Role != "user" || len(message.Content) == 0 {
			continue
		}
		block := mapValue(message.Content[0])
		if stringValue(block["type"]) == "tool_result" && neoRunIsTerminal(mapValue(block["run"])) {
			terminalCounts[stringValue(block["toolUseID"])]++
		}
	}
	actor.mu.Unlock()
	if len(terminalCounts) != len(calls) {
		t.Fatalf("terminal tool result IDs = %d, want %d: %#v", len(terminalCounts), len(calls), terminalCounts)
	}
	for toolCallID, count := range terminalCounts {
		if count != 1 {
			t.Fatalf("tool %s terminal result count = %d, want 1", toolCallID, count)
		}
	}
}

func TestNeoFinderConcurrentRunsNamespaceProviderToolCallIDs(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-finder-namespaces", "thread-actor", "T-finder-namespaces", "T-finder-namespaces", neoActorRecord("actor-finder-namespaces", "thread-actor", "T-finder-namespaces"), nil)
	root := setNeoFinderWorkspaceForTest(t, actor)
	call := neoToolCall{ID: "provider-reused-id", Name: "Grep", Input: map[string]any{"pattern": "neoActor"}}

	results := make(chan []neoSubagentToolExchange, 2)
	for _, parentToolCallID := range []string{"TU-finder-a", "TU-finder-b"} {
		go func(parentToolCallID string) {
			results <- actor.execFinderTurnTools(context.Background(), []neoToolCall{call}, root, root, parentToolCallID, actor.generation)
		}(parentToolCallID)
	}
	if !neoWaitFor(time.Second, func() bool {
		actor.mu.Lock()
		defer actor.mu.Unlock()
		return len(actor.subagentWaiters) == 2
	}) {
		t.Fatal("concurrent finder runs did not register distinct leaf tools")
	}
	actor.mu.Lock()
	leasedIDs := make([]string, 0, len(actor.subagentWaiters))
	for toolCallID := range actor.subagentWaiters {
		leasedIDs = append(leasedIDs, toolCallID)
	}
	actor.mu.Unlock()
	if leasedIDs[0] == leasedIDs[1] || leasedIDs[0] == call.ID || leasedIDs[1] == call.ID {
		t.Fatalf("executor tool call IDs were not namespaced: provider=%q leased=%q", call.ID, leasedIDs)
	}
	for _, toolCallID := range leasedIDs {
		if !actor.routeSubagentLeafToolResult(toolCallID, map[string]any{"status": "done", "output": toolCallID}) {
			t.Fatalf("result for %s was not routed", toolCallID)
		}
	}
	for range 2 {
		select {
		case exchanges := <-results:
			if len(exchanges) != 1 || exchanges[0].Call.ID != call.ID || stringValue(exchanges[0].Run["status"]) != "done" {
				t.Fatalf("finder exchange = %#v", exchanges)
			}
		case <-time.After(time.Second):
			t.Fatal("concurrent finder run did not finish")
		}
	}
}

func TestNeoFinderTurnRejectsDuplicateToolCallIDs(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-finder-duplicate", "thread-actor", "T-finder-duplicate", "T-finder-duplicate", neoActorRecord("actor-finder-duplicate", "thread-actor", "T-finder-duplicate"), nil)
	root := setNeoFinderWorkspaceForTest(t, actor)
	calls := []neoToolCall{
		{ID: "duplicate", Name: "Grep", Input: map[string]any{"pattern": "one"}},
		{ID: "duplicate", Name: "Read", Input: map[string]any{"path": "two.go"}},
	}

	exchanges := actor.execFinderTurnTools(context.Background(), calls, root, root, "TU-finder", actor.generation)
	if len(exchanges) != len(calls) {
		t.Fatalf("duplicate tool exchanges = %d, want %d", len(exchanges), len(calls))
	}
	for index, exchange := range exchanges {
		if stringValue(exchange.Run["status"]) != "error" || !strings.Contains(runToText(exchange.Run), "duplicate tool call id") {
			t.Fatalf("duplicate exchange %d = %#v", index, exchange.Run)
		}
		if exchange.Call.ID == "duplicate" {
			t.Fatalf("duplicate exchange %d retained ambiguous tool call ID", index)
		}
		if index > 0 && exchange.Call.ID == exchanges[index-1].Call.ID {
			t.Fatalf("duplicate exchanges share tool call ID %q", exchange.Call.ID)
		}
	}
	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.subagentWaiters) != 0 || len(actor.subagentTools) != 0 {
		t.Fatalf("duplicate calls were leased: waiters=%d tools=%d", len(actor.subagentWaiters), len(actor.subagentTools))
	}
	toolUses := make(map[string]int)
	toolResults := make(map[string]int)
	for _, message := range actor.messages {
		if len(message.Content) == 0 {
			continue
		}
		block := mapValue(message.Content[0])
		switch stringValue(block["type"]) {
		case "tool_use":
			toolUses[stringValue(block["id"])]++
		case "tool_result":
			toolResults[stringValue(block["toolUseID"])]++
		}
	}
	if len(toolUses) != len(calls) || len(toolResults) != len(calls) {
		t.Fatalf("duplicate transcript pairing uses=%#v results=%#v", toolUses, toolResults)
	}
	for toolCallID, count := range toolUses {
		if count != 1 || toolResults[toolCallID] != 1 {
			t.Fatalf("duplicate transcript tool %q uses=%d results=%d", toolCallID, count, toolResults[toolCallID])
		}
	}
}

func TestNeoFinderCancellationWakesLeavesClearsTrackingAndRevokesOnce(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-finder-cancel", "thread-actor", "T-finder-cancel", "T-finder-cancel", neoActorRecord("actor-finder-cancel", "thread-actor", "T-finder-cancel"), nil)
	var eventsMu sync.Mutex
	revocations := map[string]int{}
	leaseEvents := map[string][]string{}
	socket := &neoSocket{writeMessage: func(_ int, data []byte) error {
		var event map[string]any
		if err := json.Unmarshal(data, &event); err == nil {
			eventType := stringValue(event["type"])
			toolCallID := stringValue(event["toolCallId"])
			if eventType != "tool_lease" && eventType != "executor_tool_lease_revoked" {
				return nil
			}
			eventsMu.Lock()
			leaseEvents[toolCallID] = append(leaseEvents[toolCallID], eventType)
			if eventType == "executor_tool_lease_revoked" {
				revocations[toolCallID]++
			}
			eventsMu.Unlock()
		}
		return nil
	}}
	actor.sockets[socket] = struct{}{}

	results := make(chan map[string]any, neoFinderMaxToolCallsPerTurn)
	for i := 0; i < neoFinderMaxToolCallsPerTurn; i++ {
		call := neoToolCall{ID: "leaf-cancel-" + strconv.Itoa(i), Name: "Grep", Input: map[string]any{"pattern": "x"}}
		go func(call neoToolCall) {
			results <- actor.execSubagentLeafTool(call, "TU-finder", "M-leaf", actor.generation)
		}(call)
	}
	if !neoWaitFor(time.Second, func() bool {
		actor.mu.Lock()
		defer actor.mu.Unlock()
		return len(actor.subagentWaiters) == neoFinderMaxToolCallsPerTurn
	}) {
		t.Fatal("leaf tools did not register")
	}
	actor.mu.Lock()
	actor.pendingTools["leaf-cancel-0"] = neoPendingTool{ID: "leaf-cancel-0", Name: "Grep"}
	actor.mu.Unlock()

	started := time.Now()
	actor.cancel()
	for i := 0; i < neoFinderMaxToolCallsPerTurn; i++ {
		select {
		case run := <-results:
			if stringValue(run["status"]) != "cancelled" {
				t.Fatalf("cancelled leaf result = %#v", run)
			}
		case <-time.After(time.Second):
			t.Fatal("cancellation did not wake a leaf waiter")
		}
	}
	if time.Since(started) > 250*time.Millisecond {
		t.Fatalf("leaf cancellation took %s", time.Since(started))
	}
	actor.mu.Lock()
	waiters := len(actor.subagentWaiters)
	tools := len(actor.subagentTools)
	actor.mu.Unlock()
	if waiters != 0 || tools != 0 {
		t.Fatalf("subagent tracking after cancel = waiters:%d tools:%d", waiters, tools)
	}
	eventsMu.Lock()
	defer eventsMu.Unlock()
	for i := 0; i < neoFinderMaxToolCallsPerTurn; i++ {
		id := "leaf-cancel-" + strconv.Itoa(i)
		if revocations[id] != 1 {
			t.Fatalf("leaf %s revocations = %d, want 1", id, revocations[id])
		}
		if !slices.Equal(leaseEvents[id], []string{"tool_lease", "executor_tool_lease_revoked"}) {
			t.Fatalf("leaf %s lease events = %#v", id, leaseEvents[id])
		}
	}
	if actor.routeSubagentLeafToolResult("leaf-cancel-0", map[string]any{"status": "done", "output": "late"}) {
		t.Fatal("late executor result was accepted after cancellation")
	}
}

func TestNeoFinderExecutorRevocationWakesLeaf(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-finder-revoke", "thread-actor", "T-finder-revoke", "T-finder-revoke", neoActorRecord("actor-finder-revoke", "thread-actor", "T-finder-revoke"), nil)
	childMessageID := actor.storeSubagentToolUseMessage(neoToolCall{ID: "leaf-revoke", Name: "Grep", Input: map[string]any{"pattern": "x"}}, "TU-finder")
	done := make(chan map[string]any, 1)
	go func() {
		done <- actor.execSubagentLeafTool(neoToolCall{ID: "leaf-revoke", Name: "Grep", Input: map[string]any{"pattern": "x"}}, "TU-finder", childMessageID, actor.generation)
	}()
	if !neoWaitFor(time.Second, func() bool {
		actor.mu.Lock()
		defer actor.mu.Unlock()
		return len(actor.subagentWaiters) == 1
	}) {
		t.Fatal("leaf tool did not register")
	}
	actor.revokeToolLease(map[string]any{"toolCallId": "leaf-revoke", "reason": "reassigned"})
	select {
	case run := <-done:
		if stringValue(run["status"]) != "cancelled" || stringValue(run["reason"]) != "system:disposed" {
			t.Fatalf("revoked leaf result = %#v", run)
		}
	case <-time.After(time.Second):
		t.Fatal("executor revocation did not wake leaf")
	}
	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.subagentWaiters) != 0 || len(actor.subagentTools) != 0 {
		t.Fatalf("subagent tracking after revocation = waiters:%d tools:%d", len(actor.subagentWaiters), len(actor.subagentTools))
	}
}

func TestNeoFinderCancellationDuringInferenceDoesNotLeaseReturnedCalls(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-finder-inference-cancel", "thread-actor", "T-finder-inference-cancel", "T-finder-inference-cancel", neoActorRecord("actor-finder-inference-cancel", "thread-actor", "T-finder-inference-cancel"), nil)
	setNeoFinderWorkspaceForTest(t, actor)
	entered := make(chan struct{})
	release := make(chan struct{})
	rt.inferStream = func(_ *neoRuntime, _ neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		close(entered)
		<-release
		return neoInferenceResult{ToolCalls: []neoToolCall{{ID: "late-leaf", Name: "Grep", Input: map[string]any{"pattern": "x"}}}}, nil
	}
	done := make(chan error, 1)
	go func() {
		_, err := actor.executeSubagentRun("finder", map[string]any{"query": "search"}, "TU-finder", "M-1", actor.generation, 0, "")
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("finder inference did not start")
	}
	actor.cancel()
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("cancelled finder returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled finder did not return after inference stopped")
	}
	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.subagentWaiters) != 0 || len(actor.subagentTools) != 0 || len(actor.messages) != 0 || actor.activeFinderRuns != 0 {
		t.Fatalf("cancelled inference left work: waiters=%d tools=%d messages=%d active=%d", len(actor.subagentWaiters), len(actor.subagentTools), len(actor.messages), actor.activeFinderRuns)
	}
}

func TestNeoFinderLateInferenceCannotEmitAfterParentWorkCleared(t *testing.T) {
	for _, tc := range []struct {
		name  string
		clear func(*neoActor)
	}{
		{name: "user interrupt", clear: func(actor *neoActor) {
			actor.interruptActiveToolResultsForBinaryUserMessage()
		}},
		{name: "executor disconnect", clear: func(actor *neoActor) {
			actor.mu.Lock()
			cleanup := actor.clearExecutorWorkForDisconnectLocked(false)
			actor.mu.Unlock()
			actor.broadcastExecutorWorkCleanup(cleanup)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rt := newNeoRuntime(&config.Config{})
			actor := newNeoActor(rt, "actor-finder-late-"+tc.name, "thread-actor", "T-finder-late", "T-finder-late", neoActorRecord("actor-finder-late", "thread-actor", "T-finder-late"), nil)
			setNeoFinderWorkspaceForTest(t, actor)
			entered := make(chan struct{})
			release := make(chan struct{})
			rt.inferStream = func(_ *neoRuntime, _ neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
				close(entered)
				<-release
				return neoInferenceResult{ToolCalls: []neoToolCall{{ID: "late-leaf", Name: "Grep", Input: map[string]any{"pattern": "x"}}}}, nil
			}
			var eventsMu sync.Mutex
			leaseCount := 0
			socket := &neoSocket{writeMessage: func(_ int, data []byte) error {
				var event map[string]any
				if json.Unmarshal(data, &event) == nil && stringValue(event["type"]) == "tool_lease" {
					eventsMu.Lock()
					leaseCount++
					eventsMu.Unlock()
				}
				return nil
			}}
			actor.sockets[socket] = struct{}{}
			actor.pendingTools["TU-finder"] = neoPendingTool{ID: "TU-finder", Name: "finder", MessageID: "M-parent"}
			generation := actor.generation
			done := make(chan error, 1)
			go func() {
				_, err := actor.executeSubagentRun("finder", map[string]any{"query": "search"}, "TU-finder", "M-parent", generation, 0, "")
				done <- err
			}()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("finder inference did not start")
			}
			tc.clear(actor)
			actor.mu.Lock()
			messagesAfterClear := len(actor.messages)
			currentGeneration := actor.generation
			actor.mu.Unlock()
			if currentGeneration == generation {
				t.Fatal("clearing pending parent work did not invalidate the subagent generation")
			}
			close(release)
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("stale finder returned error: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("stale finder did not return after inference completed")
			}
			actor.mu.Lock()
			messages := len(actor.messages)
			waiters := len(actor.subagentWaiters)
			tools := len(actor.subagentTools)
			active := actor.activeFinderRuns
			actor.mu.Unlock()
			eventsMu.Lock()
			leases := leaseCount
			eventsMu.Unlock()
			if messages != messagesAfterClear || waiters != 0 || tools != 0 || active != 0 || leases != 0 {
				t.Fatalf("stale finder emitted work: messages=%d want=%d waiters=%d tools=%d active=%d leases=%d", messages, messagesAfterClear, waiters, tools, active, leases)
			}
		})
	}
}

func TestNeoFinderWorkspaceAndPathScope(t *testing.T) {
	parent := "/remote/workspaces"
	repo := parent + "/repo[1]"
	work := repo + "/internal/pkg"
	scopedWork, scopedRoot, err := neoFinderWorkspaceScope(work, parent)
	if err != nil {
		t.Fatalf("scope remote workspace: %v", err)
	}
	if scopedWork != work || scopedRoot != parent {
		t.Fatalf("remote workspace scope = work:%q root:%q, want work:%q root:%q", scopedWork, scopedRoot, work, parent)
	}
	if _, _, err := neoFinderWorkspaceScope("/", "/"); err == nil {
		t.Fatal("filesystem root was accepted as a Finder workspace")
	}
	if home, homeErr := os.UserHomeDir(); homeErr == nil {
		if _, _, err := neoFinderWorkspaceScope(home, home); err == nil {
			t.Fatal("home directory was accepted as a Finder workspace")
		}
		alias := filepath.Join(t.TempDir(), "workspace")
		if err := os.Symlink(home, alias); err == nil {
			if _, _, err := neoFinderWorkspaceScope(alias, alias); err == nil {
				t.Fatal("home directory symlink was accepted as a Finder workspace")
			}
		}
	}

	outside := parent + "/outside"
	prefixCollision := parent + "/repo-other"
	if _, err := neoScopeFinderToolCall(neoToolCall{ID: "read", Name: "Read", Input: map[string]any{"path": "src/missing.go"}}, repo); err == nil {
		t.Fatal("remote Finder Read was accepted without a verifiable workspace")
	}
	grep, err := neoScopeFinderToolCall(neoToolCall{ID: "grep", Name: "Grep", Input: map[string]any{"pattern": "needle"}}, repo)
	if err != nil || stringValue(grep.Input["path"]) != repo {
		t.Fatalf("unscoped Grep = %#v err=%v", grep.Input, err)
	}
	scopedRemoteGrep, err := neoScopeFinderToolCallForExecutor(neoToolCall{ID: "scoped-remote-grep", Name: "Grep", Input: map[string]any{"pattern": "needle", "path": "src"}}, repo, parent)
	if err != nil || stringValue(scopedRemoteGrep.Input["path"]) != repo+"/src" {
		t.Fatalf("scoped remote Grep = %#v err=%v", scopedRemoteGrep.Input, err)
	}
	grepGlob, err := neoScopeFinderToolCall(neoToolCall{ID: "grep-glob", Name: "Grep", Input: map[string]any{"pattern": "needle", "glob": "**/*.go"}}, repo)
	if err != nil || stringValue(grepGlob.Input["glob"]) != "**/*.go" || stringValue(grepGlob.Input["path"]) != "" {
		t.Fatalf("Grep glob semantics changed: %#v err=%v", grepGlob.Input, err)
	}
	glob, err := neoScopeFinderToolCall(neoToolCall{ID: "glob", Name: "glob", Input: map[string]any{"filePattern": repo + "/**/*.go"}}, repo)
	if err != nil || stringValue(glob.Input["filePattern"]) != "**/*.go" {
		t.Fatalf("absolute in-root glob scope = %#v err=%v", glob.Input, err)
	}
	rebasedRelativeGlob, err := neoScopeFinderToolCallForExecutor(neoToolCall{ID: "rebased-relative-glob", Name: "glob", Input: map[string]any{"filePattern": "**/*.go"}}, repo, parent)
	if err != nil || stringValue(rebasedRelativeGlob.Input["filePattern"]) != `repo\[1\]/**/*.go` {
		t.Fatalf("repository-relative glob scope = %#v err=%v", rebasedRelativeGlob.Input, err)
	}
	rebasedAbsoluteGlob, err := neoScopeFinderToolCallForExecutor(neoToolCall{ID: "rebased-absolute-glob", Name: "glob", Input: map[string]any{"filePattern": repo + "/src/**/*.go"}}, repo, parent)
	if err != nil || stringValue(rebasedAbsoluteGlob.Input["filePattern"]) != `repo\[1\]/src/**/*.go` {
		t.Fatalf("repository-absolute glob scope = %#v err=%v", rebasedAbsoluteGlob.Input, err)
	}
	escapedGlob, err := neoScopeFinderToolCallForExecutor(neoToolCall{ID: "escaped-glob", Name: "glob", Input: map[string]any{"filePattern": `app/\[slug\]/**/*.ts`}}, repo, parent)
	if err != nil || stringValue(escapedGlob.Input["filePattern"]) != `repo\[1\]/app/\[slug\]/**/*.ts` {
		t.Fatalf("escaped repository glob scope = %#v err=%v", escapedGlob.Input, err)
	}
	if _, err := neoScopeFinderToolCallForExecutor(neoToolCall{ID: "sibling-glob", Name: "glob", Input: map[string]any{"filePattern": outside + "/**/*.go"}}, repo, parent); err == nil {
		t.Fatal("Finder accepted a glob under an executor-workspace sibling")
	}

	rejections := []neoToolCall{
		{ID: "parent", Name: "Read", Input: map[string]any{"path": "../outside/secret.go"}},
		{ID: "absolute", Name: "Read", Input: map[string]any{"path": outside + "/secret.go"}},
		{ID: "prefix", Name: "Read", Input: map[string]any{"path": prefixCollision + "/secret.go"}},
		{ID: "grep-path-glob", Name: "Grep", Input: map[string]any{"pattern": "needle", "path": "src", "glob": "**/*.go"}},
		{ID: "glob-parent", Name: "glob", Input: map[string]any{"filePattern": "../**/*.go"}},
		{ID: "glob-brace-parent", Name: "glob", Input: map[string]any{"filePattern": "{..,src}/**/*.go"}},
		{ID: "glob-concatenated-brace-parent", Name: "glob", Input: map[string]any{"filePattern": "{.,src}{.,test}/**/*.go"}},
		{ID: "glob-brace-absolute", Name: "glob", Input: map[string]any{"filePattern": "{/,src}/**/*.go"}},
	}
	for _, call := range rejections {
		if _, err := neoScopeFinderToolCall(call, repo); err == nil {
			t.Fatalf("Finder accepted path escape %s: %#v", call.ID, call.Input)
		}
	}
}

func TestNeoFinderReadRejectsSymlinkEscape(t *testing.T) {
	repo := t.TempDir()
	inside := filepath.Join(repo, "inside.txt")
	if err := os.WriteFile(inside, []byte("inside"), 0o600); err != nil {
		t.Fatal(err)
	}
	realInside, err := filepath.EvalSymlinks(inside)
	if err != nil {
		t.Fatal(err)
	}
	read, err := neoScopeFinderToolCall(neoToolCall{ID: "read", Name: "Read", Input: map[string]any{"path": "inside.txt"}}, repo)
	if err != nil || stringValue(read.Input["path"]) != filepath.ToSlash(realInside) {
		t.Fatalf("verified Read scope = %#v err=%v", read.Input, err)
	}
	insideDir := filepath.Join(repo, "inside")
	if err := os.Mkdir(insideDir, 0o700); err != nil {
		t.Fatal(err)
	}
	insideAliasedFile := filepath.Join(insideDir, "aliased.txt")
	if err := os.WriteFile(insideAliasedFile, []byte("aliased"), 0o600); err != nil {
		t.Fatal(err)
	}
	realInsideAliasedFile, err := filepath.EvalSymlinks(insideAliasedFile)
	if err != nil {
		t.Fatal(err)
	}
	realInsideDir, err := filepath.EvalSymlinks(insideDir)
	if err != nil {
		t.Fatal(err)
	}
	insideAlias := filepath.Join(repo, "inside-alias")
	if err := os.Symlink(insideDir, insideAlias); err != nil {
		t.Skipf("create in-workspace symlink: %v", err)
	}
	aliasedRead, err := neoScopeFinderToolCall(neoToolCall{ID: "aliased-read", Name: "Read", Input: map[string]any{"path": filepath.Join("inside-alias", "aliased.txt")}}, repo)
	if err != nil || stringValue(aliasedRead.Input["path"]) != filepath.ToSlash(realInsideAliasedFile) {
		t.Fatalf("canonical Read scope = %#v err=%v", aliasedRead.Input, err)
	}
	aliasedGrep, err := neoScopeFinderToolCall(neoToolCall{ID: "aliased-grep", Name: "Grep", Input: map[string]any{"pattern": "inside", "path": "inside-alias"}}, repo)
	if err != nil || stringValue(aliasedGrep.Input["path"]) != filepath.ToSlash(realInsideDir) {
		t.Fatalf("canonical Grep scope = %#v err=%v", aliasedGrep.Input, err)
	}
	aliasedGlob, err := neoScopeFinderToolCall(neoToolCall{ID: "aliased-glob", Name: "glob", Input: map[string]any{"filePattern": "inside-alias/**/*.txt"}}, repo)
	if err != nil || stringValue(aliasedGlob.Input["filePattern"]) != "inside-alias/**/*.txt" {
		t.Fatalf("in-workspace symlink glob scope = %#v err=%v", aliasedGlob.Input, err)
	}
	outside := t.TempDir()
	secret := filepath.Join(outside, "secret.txt")
	if err := os.WriteFile(secret, []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(repo, "outside")
	if err := os.Symlink(outside, link); err != nil {
		t.Skipf("create symlink: %v", err)
	}
	if _, err := neoScopeFinderToolCall(neoToolCall{ID: "escape", Name: "Read", Input: map[string]any{"path": filepath.Join("outside", "secret.txt")}}, repo); err == nil {
		t.Fatal("Finder accepted a Read through an out-of-workspace symlink")
	}
	if _, err := neoScopeFinderToolCall(neoToolCall{ID: "grep-escape", Name: "Grep", Input: map[string]any{"pattern": "secret", "path": "outside"}}, repo); err == nil {
		t.Fatal("Finder accepted a Grep through an out-of-workspace symlink")
	}
	nestedScope := filepath.Join(repo, "nested-scope")
	nestedTarget := filepath.Join(repo, "nested-target")
	if err := os.Mkdir(nestedScope, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(nestedTarget, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(nestedTarget, filepath.Join(nestedScope, "alias")); err != nil {
		t.Skipf("create nested in-workspace symlink: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(nestedTarget, "escape")); err != nil {
		t.Skipf("create nested out-of-workspace symlink: %v", err)
	}
	if _, err := neoScopeFinderToolCall(neoToolCall{ID: "nested-glob-escape", Name: "glob", Input: map[string]any{"filePattern": "nested-scope/**/*.txt"}}, repo); err == nil {
		t.Fatal("Finder accepted a glob through nested directory symlinks")
	}
	if scoped, err := neoScopeFinderToolCall(neoToolCall{ID: "narrow-glob", Name: "glob", Input: map[string]any{"filePattern": "inside/**/*.txt"}}, repo); err != nil || stringValue(scoped.Input["filePattern"]) != "inside/**/*.txt" {
		t.Fatalf("narrow in-workspace glob was affected by unrelated symlink: scoped=%#v err=%v", scoped.Input, err)
	}
	if scoped, err := neoScopeFinderToolCall(neoToolCall{ID: "narrow-brace-glob", Name: "glob", Input: map[string]any{"filePattern": "{inside,inside-alias}/**/*.txt"}}, repo); err != nil || stringValue(scoped.Input["filePattern"]) != "{inside,inside-alias}/**/*.txt" {
		t.Fatalf("narrow brace glob was affected by unrelated symlink: scoped=%#v err=%v", scoped.Input, err)
	}
	for _, pattern := range []string{"outside/**/*.txt", "**/*", "!(safe)/**/*.txt"} {
		if _, err := neoScopeFinderToolCall(neoToolCall{ID: "glob-escape", Name: "glob", Input: map[string]any{"filePattern": pattern}}, repo); err == nil {
			t.Fatalf("Finder accepted glob %q with an out-of-workspace symlink", pattern)
		}
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := neoScopeFinderToolCallForExecutorContext(cancelled, neoToolCall{ID: "cancelled-glob", Name: "glob", Input: map[string]any{"filePattern": "**/*"}}, repo, repo); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled glob validation error = %v, want context canceled", err)
	}
	realRepo, err := filepath.EvalSymlinks(repo)
	if err != nil {
		t.Fatal(err)
	}
	canonicalRepo, err := neoFinderPathValue(filepath.ToSlash(realRepo))
	if err != nil {
		t.Fatal(err)
	}
	if err := neoFinderValidateGlobSymlinkScope(context.Background(), repo, canonicalRepo, repo, 1); err == nil || !strings.Contains(err.Error(), "narrow the pattern") {
		t.Fatalf("bounded glob validation error = %v", err)
	}
}

func TestNeoFinderWithoutTool(t *testing.T) {
	tools := neoFinderWithoutTool([]neoToolSpec{{Name: "Read"}, {Name: "Grep"}, {Name: "glob"}}, "Read")
	if neoSubagentHasTools(tools, "Read") || !neoSubagentHasTools(tools, "Grep", "glob") {
		t.Fatalf("remote Finder tools = %#v", tools)
	}
}

func TestNeoFinderEnvironmentPathsAndFileURIs(t *testing.T) {
	workingDir, workspaceRoot := neoFinderEnvironmentPaths(map[string]any{
		"initial": map[string]any{
			"workingDirectory": "file:///remote/My%20Workspace/repo/src",
			"trees":            []any{map[string]any{"uri": "file:///remote/My%20Workspace/repo"}},
		},
	})
	if workingDir != "file:///remote/My%20Workspace/repo/src" || workspaceRoot != "file:///remote/My%20Workspace/repo" {
		t.Fatalf("nested environment paths = work:%q root:%q", workingDir, workspaceRoot)
	}
	scopedWork, scopedRoot, err := neoFinderWorkspaceScope(workingDir, workspaceRoot)
	if err != nil {
		t.Fatalf("scope nested environment paths: %v", err)
	}
	if scopedWork != "/remote/My Workspace/repo/src" || scopedRoot != "/remote/My Workspace/repo" {
		t.Fatalf("decoded nested environment paths = work:%q root:%q", scopedWork, scopedRoot)
	}

	for raw, want := range map[string]string{
		"file://build-host/work/repo":       "//build-host/work/repo",
		"file:///C:/Users/Amp/My%20Repo":    "C:/Users/Amp/My Repo",
		`C:\Users\Amp\My Repo`:              "C:/Users/Amp/My Repo",
		"file://localhost/remote/workspace": "/remote/workspace",
	} {
		got, err := neoFinderCanonicalDirectory(raw)
		if err != nil || got != want {
			t.Fatalf("canonical path %q = %q err=%v, want %q", raw, got, err, want)
		}
	}
}

func TestNeoLibrarianSubagentUsesGPT56SolNone(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-librarian", "thread-actor", "T-librarian", "T-librarian", neoActorRecord("actor-librarian", "thread-actor", "T-librarian"), nil)
	actor.currentAgentMode = "smart"
	actor.settings = map[string]any{"reasoning.effort": "high"}

	var seen []neoInferenceRequest
	rt.inferStream = func(_ *neoRuntime, req neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		seen = append(seen, req)
		return neoInferenceResult{Text: "done"}, nil
	}

	if _, err := actor.executeSubagentRun("librarian", map[string]any{"query": "how does routing work"}, "TU-librarian", "M-1", actor.generation, 0, ""); err != nil {
		t.Fatalf("librarian subagent failed: %v", err)
	}
	if len(seen) != 1 {
		t.Fatalf("librarian requests = %#v, want exactly one", seen)
	}
	route := seen[0].ModelRouteOverride
	if route == nil || route.Provider != "openai" || route.Model != "gpt-5.6-sol" {
		t.Fatalf("librarian route = %#v, want openai/gpt-5.6-sol", route)
	}
	if seen[0].ReasoningEffort != "none" || stringValue(seen[0].Settings["reasoning.effort"]) != "none" {
		t.Fatalf("librarian effort request=%q settings=%#v, want none", seen[0].ReasoningEffort, seen[0].Settings)
	}
}

func TestNeoRunCheckSubagentUsesGPT56TerraLow(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-run-check", "thread-actor", "T-run-check", "T-run-check", neoActorRecord("actor-run-check", "thread-actor", "T-run-check"), nil)
	actor.currentAgentMode = "review"
	actor.settings = map[string]any{"reasoning.effort": "medium"}
	actor.tools = map[string]neoToolSpec{
		"Read": {Name: "Read", InputSchema: map[string]any{"type": "object"}},
		"Grep": {Name: "Grep", InputSchema: map[string]any{"type": "object"}},
		"glob": {Name: "glob", InputSchema: map[string]any{"type": "object"}},
		"Bash": {Name: "Bash", InputSchema: map[string]any{"type": "object"}},
	}

	var seen []neoInferenceRequest
	rt.inferStream = func(_ *neoRuntime, req neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		seen = append(seen, req)
		return neoInferenceResult{Text: `{"comments":[]}`}, nil
	}

	_, err := actor.executeSubagentRun("run_check", map[string]any{
		"checkName":       "repo-convention-fit",
		"checkURI":        "file:///checks/repo-convention-fit.md",
		"checkContent":    "Prefer repository conventions.",
		"diffDescription": "uncommitted changes",
		"files":           []any{"internal/api/modules/amp/neo_subagents.go"},
	}, "TU-run-check", "M-1", actor.generation, 0, "")
	if err != nil {
		t.Fatalf("run_check subagent failed: %v", err)
	}
	if len(seen) != 1 {
		t.Fatalf("run_check requests = %#v, want exactly one", seen)
	}
	route := seen[0].ModelRouteOverride
	if route == nil || route.Provider != "openai" || route.Model != "gpt-5.6-terra" {
		t.Fatalf("run_check route = %#v, want openai/gpt-5.6-terra", route)
	}
	if seen[0].ReasoningEffort != "low" || stringValue(seen[0].Settings["reasoning.effort"]) != "low" {
		t.Fatalf("run_check effort request=%q settings=%#v, want low", seen[0].ReasoningEffort, seen[0].Settings)
	}
	if !neoSubagentHasTools(seen[0].Tools, "Read", "Grep", "glob", "Bash") {
		t.Fatalf("run_check tools = %#v, want review check tools", seen[0].Tools)
	}
}

func neoSubagentHasTools(tools []neoToolSpec, names ...string) bool {
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

func TestIsNeoLocalSubagentTool(t *testing.T) {
	for _, name := range []string{"finder", "oracle", "librarian", "Task", "run_check"} {
		if !isNeoLocalSubagentTool(name) {
			t.Fatalf("%s should be a local subagent tool", name)
		}
	}
	for _, name := range []string{"Read", "Bash", "Grep", "glob", "edit_file", "skill"} {
		if isNeoLocalSubagentTool(name) {
			t.Fatalf("%s must not be treated as a subagent tool (it is executor-runnable)", name)
		}
	}
}

func TestNeoSubagentInputText(t *testing.T) {
	if got := neoSubagentInputText("finder", map[string]any{"query": "find the JWT auth"}); got != "find the JWT auth" {
		t.Fatalf("finder input = %q", got)
	}
	oracle := neoSubagentInputText("oracle", map[string]any{
		"task":    "review the auth design",
		"context": "files attached",
		"files":   []any{"a.go", "b.go"},
	})
	for _, want := range []string{"review the auth design", "## Context", "files attached", "## Relevant files", "- a.go", "- b.go"} {
		if !strings.Contains(oracle, want) {
			t.Fatalf("oracle input missing %q:\n%s", want, oracle)
		}
	}
	if got := neoSubagentInputText("librarian", map[string]any{"query": "how does routing work"}); got != "how does routing work" {
		t.Fatalf("librarian input = %q", got)
	}
	runCheck := neoSubagentInputText("run_check", map[string]any{
		"checkName":       "repo-convention-fit",
		"checkURI":        "file:///checks/repo-convention-fit.md",
		"checkContent":    "Prefer repository logging conventions.",
		"frontmatter":     map[string]any{"name": "repo-convention-fit"},
		"diffDescription": "uncommitted changes",
		"files":           []any{"backend/src/index.ts"},
		"instructions":    "Focus on blocker-level correctness. Ignore style nits unless they affect behavior.",
	})
	for _, want := range []string{
		"<check name=\"repo-convention-fit\" uri=\"file:///checks/repo-convention-fit.md\">",
		"Prefer repository logging conventions.",
		"Additional instructions from the review request:",
		"Focus on blocker-level correctness. Ignore style nits unless they affect behavior.",
		"Diff under review: uncommitted changes",
		"- backend/src/index.ts",
	} {
		if !strings.Contains(runCheck, want) {
			t.Fatalf("run_check input missing %q:\n%s", want, runCheck)
		}
	}
	uriOnlyRunCheck := neoSubagentInputText("run_check", map[string]any{
		"checkName":       "api-and-observability-polish",
		"checkURI":        "file:///checks/api-and-observability-polish.md",
		"frontmatter":     map[string]any{"name": "api-and-observability-polish"},
		"diffDescription": "uncommitted changes",
		"files":           []any{"backend/src/lib/email.ts"},
	})
	for _, want := range []string{
		"<check name=\"api-and-observability-polish\" uri=\"file:///checks/api-and-observability-polish.md\">",
		"Check definition content was not embedded.",
		"For file:// URIs, pass the decoded filesystem path to Read.",
		"- backend/src/lib/email.ts",
	} {
		if !strings.Contains(uriOnlyRunCheck, want) {
			t.Fatalf("uri-only run_check input missing %q:\n%s", want, uriOnlyRunCheck)
		}
	}
	if strings.Contains(uriOnlyRunCheck, "<content>") {
		t.Fatalf("uri-only run_check unexpectedly embedded content:\n%s", uriOnlyRunCheck)
	}
	runCheckDef, ok := neoSubagentDefFor("run_check")
	if !ok || !strings.Contains(runCheckDef.SystemPrompt, "Evaluate adversarially within the check's criteria") {
		t.Fatalf("run_check system prompt missing adversarial check guidance")
	}
}

func TestNeoRunCheckToolSpecAcceptsInstructions(t *testing.T) {
	spec := neoRunCheckToolSpec()
	properties, ok := spec.InputSchema["properties"].(map[string]any)
	if !ok {
		t.Fatalf("run_check schema missing properties: %#v", spec.InputSchema)
	}
	if _, ok := properties["instructions"]; !ok {
		t.Fatalf("run_check schema missing instructions property: %#v", properties)
	}
	required := arrayValue(spec.InputSchema["required"])
	for _, raw := range required {
		if stringValue(raw) == "checkContent" {
			t.Fatalf("run_check schema should not require checkContent: %#v", required)
		}
	}
}

func TestNeoRunIsTerminal(t *testing.T) {
	for _, status := range []string{"done", "error", "cancelled", "success", "failed"} {
		if !neoRunIsTerminal(map[string]any{"status": status}) {
			t.Fatalf("status %q should be terminal", status)
		}
	}
	for _, status := range []string{"", "in-progress", "running", "pending"} {
		if neoRunIsTerminal(map[string]any{"status": status}) {
			t.Fatalf("status %q should not be terminal", status)
		}
	}
}

func TestNeoSubagentLeafToolProgressAndResultAreStoredUnderParent(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	ch := make(chan map[string]any, 1)
	actor.subagentWaiters = map[string]chan map[string]any{"leaf-1": ch}
	actor.subagentTools = map[string]neoPendingTool{
		"leaf-1": {ID: "leaf-1", Name: "search_github", ParentToolCallID: "TU-librarian"},
	}

	if !actor.routeSubagentLeafToolResult("leaf-1", map[string]any{"status": "in-progress", "progress": map[string]any{"output": "Searched GitHub: missing pods"}}) {
		t.Fatal("subagent progress was not consumed")
	}
	select {
	case run := <-ch:
		t.Fatalf("progress resolved waiter early: %#v", run)
	default:
	}

	actor.mu.Lock()
	if len(actor.messages) != 1 {
		t.Fatalf("messages after progress = %#v", actor.messages)
	}
	progressMessage := actor.messages[0]
	progressRun := mapValue(mapValue(progressMessage.Content[0])["run"])
	actor.rebuildHistoryLocked()
	topHistory := scopedNeoHistory(actor.history, "")
	actor.mu.Unlock()

	if progressMessage.ParentToolUseID != "TU-librarian" || progressMessage.CompletionStatus != "tool_progress" {
		t.Fatalf("progress message = %#v", progressMessage)
	}
	if stringValue(progressRun["status"]) != "in-progress" || stringValue(mapValue(progressRun["progress"])["output"]) != "Searched GitHub: missing pods" {
		t.Fatalf("progress run = %#v", progressRun)
	}
	if len(topHistory) != 0 {
		t.Fatalf("top-level history included nested progress: %#v", topHistory)
	}

	if !actor.routeSubagentLeafToolResult("leaf-1", map[string]any{"status": "done", "output": "repo result"}) {
		t.Fatal("subagent result was not consumed")
	}
	select {
	case run := <-ch:
		if stringValue(run["status"]) != "done" || stringValue(run["output"]) != "repo result" {
			t.Fatalf("waiter run = %#v", run)
		}
	case <-time.After(time.Second):
		t.Fatal("terminal result did not resolve waiter")
	}

	actor.mu.Lock()
	if _, exists := actor.subagentWaiters["leaf-1"]; exists {
		t.Fatalf("waiter not cleared")
	}
	if _, exists := actor.subagentTools["leaf-1"]; exists {
		t.Fatalf("subagent tool metadata not cleared")
	}
	if len(actor.messages) != 1 {
		t.Fatalf("messages after result = %#v", actor.messages)
	}
	finalMessage := actor.messages[0]
	finalRun := mapValue(mapValue(finalMessage.Content[0])["run"])
	actor.rebuildHistoryLocked()
	topHistory = scopedNeoHistory(actor.history, "")
	nestedHistory := scopedNeoHistory(actor.history, "TU-librarian")
	actor.mu.Unlock()

	if finalMessage.ParentToolUseID != "TU-librarian" || finalMessage.CompletionStatus != "" {
		t.Fatalf("final message = %#v", finalMessage)
	}
	if stringValue(finalRun["status"]) != "done" || stringValue(finalRun["output"]) != "repo result" {
		t.Fatalf("final run = %#v", finalRun)
	}
	if len(topHistory) != 0 {
		t.Fatalf("top-level history included nested result: %#v", topHistory)
	}
	if len(nestedHistory) != 1 || nestedHistory[0].ToolCallID != "leaf-1" {
		t.Fatalf("nested history = %#v", nestedHistory)
	}
}

func TestNeoSubagentToolProgressFrameIsParented(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.subagentTools = map[string]neoPendingTool{
		"leaf-1": {ID: "leaf-1", Name: "read_github", ParentToolCallID: "TU-librarian"},
	}

	actor.handleToolProgress(map[string]any{"type": "tool_progress", "toolCallId": "leaf-1", "progress": map[string]any{"phase": "running"}})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.messages) != 1 {
		t.Fatalf("messages = %#v", actor.messages)
	}
	message := actor.messages[0]
	run := mapValue(mapValue(message.Content[0])["run"])
	if message.ParentToolUseID != "TU-librarian" || message.CompletionStatus != "tool_progress" {
		t.Fatalf("progress message = %#v", message)
	}
	if stringValue(run["status"]) != "in-progress" || stringValue(mapValue(run["progress"])["phase"]) != "running" {
		t.Fatalf("progress run = %#v", run)
	}
}

// TestNeoSubagentPromptsMatchAmpClassic guards against silent drift: our
// embedded subagent prompts are Amp-server-owned text recovered from the
// amp-classic binary. When amp-classic is present locally, assert the binary
// still contains our embedded prompt's stable opening line. If Amp changes the
// prompt upstream, this fails and the embedded copy must be refreshed.
func TestNeoSubagentPromptsMatchAmpClassic(t *testing.T) {
	bin := findAmpClassicBinary()
	if bin == "" {
		t.Skip("amp-classic binary not found; skipping prompt drift check")
	}
	data, err := os.ReadFile(bin)
	if err != nil {
		t.Skipf("read amp-classic: %v", err)
	}
	haystack := string(data)
	markers := map[string]string{
		"finder":    "You are a fast, parallel code search agent.",
		"oracle":    "You are the Oracle - an expert AI advisor with advanced reasoning capabilities.",
		"librarian": "You are the Librarian, a specialized codebase understanding agent",
		"Task":      "You are a worker agent for one bounded task.",
	}
	for tool, marker := range markers {
		def, _ := neoSubagentDefFor(tool)
		if !strings.Contains(def.SystemPrompt, strings.SplitN(marker, "\n", 2)[0]) {
			t.Fatalf("embedded %s prompt no longer contains its own marker %q", tool, marker)
		}
		if !strings.Contains(haystack, marker) {
			t.Fatalf("amp-classic no longer contains %s marker %q — refresh embedded prompt from the current binary", tool, marker)
		}
	}

	// Model-facing exposure descriptions are also Amp-owned and embedded; guard
	// them against drift the same way so a future Amp change to the tool
	// descriptions is caught rather than silently diverging.
	exposureMarkers := map[string]string{
		"finder":    "Intelligently search your codebase",
		"oracle":    "Consult the oracle",
		"librarian": "The Librarian is a codebase-understanding subagent",
	}
	for tool, marker := range exposureMarkers {
		spec, ok := neoSubagentExposureSpec(tool)
		if !ok || !strings.Contains(spec.Description, marker) {
			t.Fatalf("embedded %s exposure description no longer contains marker %q", tool, marker)
		}
		if !strings.Contains(haystack, marker) {
			t.Fatalf("amp-classic no longer contains %s exposure marker %q — refresh embedded spec from the current binary", tool, marker)
		}
	}
}

// TestNeoSubagentLoopReplaysToParentResult drives the real runSubagent loop with
// a scripted inference (via the runtime seam) and a simulated executor leaf
// result, deterministically replaying a finder session end-to-end without a live
// provider: interception → leaf-tool lease → result routing → parent delivery.
func TestNeoSubagentLoopReplaysToParentResult(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	var mu sync.Mutex
	turn := 0
	rt.inferStream = func(_ *neoRuntime, _ neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		mu.Lock()
		defer mu.Unlock()
		n := turn
		turn++
		if n == 0 {
			// First turn: call a leaf tool, no final text yet.
			return neoInferenceResult{
				Provider:  "anthropic",
				Model:     "claude-haiku-4-5-20251001",
				ToolCalls: []neoToolCall{{ID: "leaf-1", Name: "Grep", Input: map[string]any{"pattern": "neoActor"}}},
			}, nil
		}
		// Second turn: synthesize the final answer (no tool calls).
		return neoInferenceResult{
			Provider: "anthropic",
			Model:    "claude-haiku-4-5-20251001",
			Text:     "FOUND: internal/api/modules/amp/neo_runtime.go:1522",
		}, nil
	}

	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	setNeoFinderWorkspaceForTest(t, actor)
	if actor.pendingTools == nil {
		actor.pendingTools = map[string]neoPendingTool{}
	}
	actor.tools = map[string]neoToolSpec{"Grep": {Name: "Grep"}}
	parent := neoPendingTool{ID: "TU-finder", Name: "finder", Input: map[string]any{"query": "where is neoActor"}, MessageID: "M-1"}
	actor.mu.Lock()
	actor.pendingTools[parent.ID] = parent
	actor.executorReady = false // do not trigger the real continuation inference
	generation := actor.generation
	actor.mu.Unlock()

	done := make(chan struct{})
	go func() { actor.runSubagent(parent, generation); close(done) }()

	var leasedToolCallID string
	if !neoWaitFor(2*time.Second, func() bool {
		actor.mu.Lock()
		for toolCallID := range actor.subagentWaiters {
			leasedToolCallID = toolCallID
			break
		}
		actor.mu.Unlock()
		return leasedToolCallID != ""
	}) {
		t.Fatal("subagent never leased its leaf tool")
	}
	if leasedToolCallID == "leaf-1" {
		t.Fatal("finder leaf tool kept the provider-supplied ID")
	}
	if !actor.routeSubagentLeafToolResult(leasedToolCallID, map[string]any{"status": "done", "output": "neo_runtime.go:1522: type neoActor struct"}) {
		t.Fatal("leaf tool result was not routed to the subagent")
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("subagent loop did not finish")
	}

	actor.mu.Lock()
	_, stillPending := actor.pendingTools[parent.ID]
	var toolText string
	for i := len(actor.history) - 1; i >= 0; i-- {
		if actor.history[i].Role == "tool" && actor.history[i].ToolCallID == parent.ID {
			toolText = actor.history[i].Text
			break
		}
	}
	actor.mu.Unlock()

	if stillPending {
		t.Fatal("parent finder tool was never completed")
	}
	if !strings.Contains(toolText, "FOUND: internal/api/modules/amp/neo_runtime.go:1522") {
		t.Fatalf("parent tool result = %q, want the subagent's final text", toolText)
	}
}

// TestNeoSubagentNestedRecursion verifies that a subagent which calls another
// subagent (Task -> finder) runs the nested subagent locally rather than leasing
// it to the executor, and threads the nested result back to the parent.
func TestNeoSubagentNestedRecursion(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	var mu sync.Mutex
	taskTurn := 0
	finderInvoked := false
	rt.inferStream = func(_ *neoRuntime, req neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		mu.Lock()
		defer mu.Unlock()
		if strings.Contains(req.SystemPromptOverride, "fast, parallel code search agent") {
			// Nested finder subagent: return a result immediately (no leaf calls).
			finderInvoked = true
			return neoInferenceResult{Provider: "anthropic", Model: "claude-haiku-4-5-20251001", Text: "FINDER: internal/api/modules/amp/neo_runtime.go:1522"}, nil
		}
		// Task subagent: turn 0 calls finder; turn 1 synthesizes.
		taskTurn++
		if taskTurn == 1 {
			return neoInferenceResult{ToolCalls: []neoToolCall{{ID: "finder-call-1", Name: "finder", Input: map[string]any{"query": "where is neoActor"}}}}, nil
		}
		return neoInferenceResult{Text: "TASK DONE: neoActor at neo_runtime.go:1522 (via finder)"}, nil
	}

	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	setNeoFinderWorkspaceForTest(t, actor)
	if actor.pendingTools == nil {
		actor.pendingTools = map[string]neoPendingTool{}
	}
	parent := neoPendingTool{ID: "TU-task", Name: "Task", Input: map[string]any{"prompt": "investigate neoActor", "description": "investigate"}, MessageID: "M-1"}
	actor.mu.Lock()
	actor.pendingTools[parent.ID] = parent
	actor.executorReady = false
	generation := actor.generation
	actor.mu.Unlock()

	done := make(chan struct{})
	go func() { actor.runSubagent(parent, generation); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Task subagent did not finish")
	}

	if !finderInvoked {
		t.Fatal("Task did not run the nested finder subagent locally")
	}
	actor.mu.Lock()
	_, stillPending := actor.pendingTools[parent.ID]
	var toolText string
	var nestedFinderResult string
	for i := len(actor.history) - 1; i >= 0; i-- {
		if actor.history[i].Role == "tool" && actor.history[i].ToolCallID == parent.ID {
			toolText = actor.history[i].Text
			break
		}
	}
	for _, message := range actor.messages {
		if message.ParentToolUseID != parent.ID || message.MessageID != toolResultMessageID("finder-call-1") {
			continue
		}
		nestedFinderResult = runToText(mapValue(mapValue(message.Content[0])["run"]))
	}
	actor.mu.Unlock()
	if stillPending {
		t.Fatal("parent Task tool was never completed")
	}
	if !strings.Contains(toolText, "TASK DONE") {
		t.Fatalf("parent Task result = %q, want the Task's final synthesis", toolText)
	}
	if !strings.Contains(nestedFinderResult, "FINDER:") {
		t.Fatalf("nested finder result = %q, want persisted child result", nestedFinderResult)
	}
}

func TestNeoSubagentRunsSyntheticReadThreadAgentWhenExecutorOmitsIt(t *testing.T) {
	currentThreadID := "T-019e65c0-0310-77a8-b233-4b84d9c0612f"
	targetThreadID := "T-019e65c0-0310-77a8-b233-4b84d9c06130"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("# local target\n\nsubagent thread content"))
	}))
	defer upstream.Close()

	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{UpstreamURL: upstream.URL}})
	rt.setSecretSource(NewStaticSecretSource("secret"))
	var mu sync.Mutex
	oracleTurns := 0
	readTurns := 0
	sawSyntheticTool := false
	sawReadAgent := false
	rt.inferStream = func(_ *neoRuntime, req neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		mu.Lock()
		defer mu.Unlock()
		if req.ProviderFeature == "amp.read-thread" {
			sawReadAgent = true
			switch readTurns {
			case 0:
				neoReadThreadAssertAgentRequest(t, req)
				if strings.Contains(req.History[0].Text, "subagent thread content") {
					t.Fatalf("read_thread initial history included whole thread content: %#v", req.History)
				}
				readTurns++
				return neoInferenceResult{ToolCalls: []neoToolCall{{ID: "TU-read-search", Name: "search_thread_messages", Input: map[string]any{"query": "subagent context"}}}}, nil
			case 1:
				if !strings.Contains(neoHistoryTestText(req.History), "subagent thread content") {
					t.Fatalf("read_thread search history = %#v", req.History)
				}
				readTurns++
				return neoInferenceResult{ToolCalls: []neoToolCall{{ID: "TU-read-messages", Name: "read_thread_messages", Input: map[string]any{"latest": true, "count": 1}}}}, nil
			default:
				readTurns++
				return neoInferenceResult{Text: `{"relevantContent":"[message 0] subagent extracted context"}`}, nil
			}
		}
		oracleTurns++
		if oracleTurns == 1 {
			for _, tool := range req.Tools {
				if tool.Name == "read_thread" {
					sawSyntheticTool = true
					break
				}
			}
			if !sawSyntheticTool {
				t.Fatalf("oracle tools = %#v, want synthetic read_thread", req.Tools)
			}
			return neoInferenceResult{ToolCalls: []neoToolCall{{ID: "read-thread-call-1", Name: "read_thread", Input: map[string]any{"threadID": targetThreadID, "goal": "Extract subagent context"}}}}, nil
		}
		if !strings.Contains(req.History[len(req.History)-1].Text, "subagent extracted context") {
			t.Fatalf("oracle history after read_thread = %#v", req.History)
		}
		return neoInferenceResult{Text: "ORACLE DONE: subagent extracted context"}, nil
	}

	actor := newNeoActor(rt, "actor-test", "thread-actor", currentThreadID, currentThreadID, neoActorRecord("actor-test", "thread-actor", currentThreadID), nil)
	actor.executorBootstrapComplete = true
	parent := neoPendingTool{ID: "TU-oracle", Name: "oracle", Input: map[string]any{"task": "read the referenced thread"}, MessageID: "M-1"}
	actor.mu.Lock()
	actor.pendingTools[parent.ID] = parent
	actor.executorReady = false
	generation := actor.generation
	actor.mu.Unlock()

	done := make(chan struct{})
	go func() { actor.runSubagent(parent, generation); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("oracle subagent did not finish")
	}

	if !sawSyntheticTool {
		t.Fatal("oracle did not receive synthetic read_thread")
	}
	if !sawReadAgent {
		t.Fatal("read_thread agent did not run locally")
	}
	actor.mu.Lock()
	_, stillPending := actor.pendingTools[parent.ID]
	var toolText string
	var childReadResult string
	var internalReadMessages int
	for i := len(actor.history) - 1; i >= 0; i-- {
		if actor.history[i].Role == "tool" && actor.history[i].ToolCallID == parent.ID {
			toolText = actor.history[i].Text
			break
		}
	}
	for _, message := range actor.messages {
		if message.ParentToolUseID != parent.ID || message.MessageID != toolResultMessageID("read-thread-call-1") {
			if message.ParentToolUseID == "read-thread-call-1" {
				internalReadMessages++
			}
			continue
		}
		childReadResult = runToText(mapValue(mapValue(message.Content[0])["run"]))
	}
	actor.mu.Unlock()
	if stillPending {
		t.Fatal("parent oracle tool was never completed")
	}
	if !strings.Contains(toolText, "ORACLE DONE") {
		t.Fatalf("parent oracle result = %q, want final synthesis", toolText)
	}
	if !strings.Contains(childReadResult, "subagent extracted context") {
		t.Fatalf("child read_thread result = %q, want persisted child result", childReadResult)
	}
	if internalReadMessages != 4 {
		t.Fatalf("internal read_thread messages = %d, want search/read tool use and result messages", internalReadMessages)
	}
}

func neoWaitFor(d time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}

func findAmpClassicBinary() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	base := filepath.Join(home, ".local", "share")
	entries, err := os.ReadDir(base)
	if err != nil {
		return ""
	}
	for _, e := range entries {
		if !e.IsDir() || !strings.HasPrefix(e.Name(), "amp-classic-") {
			continue
		}
		for _, rel := range []string{
			filepath.Join("node_modules", "@ampcode", "cli-darwin-arm64", "amp"),
			filepath.Join("node_modules", "@ampcode", "cli", "bin", "amp.exe"),
		} {
			cand := filepath.Join(base, e.Name(), rel)
			if st, err := os.Stat(cand); err == nil && st.Size() > 1_000_000 {
				return cand
			}
		}
	}
	return ""
}
