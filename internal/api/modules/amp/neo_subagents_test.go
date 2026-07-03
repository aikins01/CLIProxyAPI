package amp

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
)

func TestNeoSubagentRegistryMatchesBinary(t *testing.T) {
	cases := []struct {
		tool     string
		provider string
		model    string
		effort   string
		tools    []string
	}{
		{"finder", "anthropic", "claude-haiku-4-5-20251001", "", []string{"Grep", "glob", "Read"}},
		{"oracle", "anthropic", "claude-fable-5", "high", []string{"Read", "Grep", "glob", "web_search", "read_web_page", "read_thread", "find_thread"}},
		{"advisor", "anthropic", "claude-fable-5", "high", []string{"Read", "Grep", "glob", "web_search", "read_web_page", "read_thread", "find_thread"}},
		{"librarian", "openai", "gpt-5.5", "none", []string{"read_github", "search_github", "commit_search", "diff", "list_directory_github", "list_repositories", "glob_github"}},
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
	if toolName == "oracle" || toolName == "advisor" {
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

func TestNeoAdvisorSubagentIgnoresConfigModeModel(t *testing.T) {
	cfg := &config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{
		ModeModels: map[string]string{"smart": "openai/gpt-5.5"},
	}}}
	got := captureNeoSubagentRouteForTest(t, "advisor", "advisor-mode", cfg, "smart", nil)
	if got.Provider != "anthropic" || got.Model != "claude-fable-5" {
		t.Fatalf("advisor route = %#v, want fixed route anthropic/claude-fable-5", got)
	}
}

func TestNeoAdvisorSubagentMatchesOracleRouting(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-advisor", "thread-actor", "T-advisor", "T-advisor", neoActorRecord("actor-advisor", "thread-actor", "T-advisor"), nil)
	actor.currentAgentMode = "smart"
	actor.settings = map[string]any{"reasoning.effort": "medium"}
	actor.registerTools([]any{
		map[string]any{"name": "Read"},
		map[string]any{"name": "Grep"},
		map[string]any{"name": "glob"},
		map[string]any{"name": "web_search"},
		map[string]any{"name": "read_web_page"},
		map[string]any{"name": "read_thread"},
		map[string]any{"name": "find_thread"},
	})

	var seen []neoInferenceRequest
	rt.inferStream = func(_ *neoRuntime, req neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		seen = append(seen, req)
		return neoInferenceResult{Text: "done"}, nil
	}

	if _, err := actor.executeSubagentRun("advisor", map[string]any{"task": "review routing", "context": "same as oracle"}, "TU-advisor", "M-1", actor.generation, 0, ""); err != nil {
		t.Fatalf("advisor subagent failed: %v", err)
	}
	if len(seen) != 1 {
		t.Fatalf("advisor requests = %#v, want exactly one", seen)
	}
	route := seen[0].ModelRouteOverride
	if route == nil || route.Provider != "anthropic" || route.Model != "claude-fable-5" {
		t.Fatalf("advisor route = %#v, want anthropic/claude-fable-5", route)
	}
	if seen[0].ReasoningEffort != "high" || stringValue(seen[0].Settings["reasoning.effort"]) != "high" {
		t.Fatalf("advisor effort request=%q settings=%#v, want high", seen[0].ReasoningEffort, seen[0].Settings)
	}
	toolNames := make([]string, 0, len(seen[0].Tools))
	for _, tool := range seen[0].Tools {
		toolNames = append(toolNames, tool.Name)
	}
	if strings.Join(toolNames, ",") != "Read,Grep,glob,web_search,read_web_page,read_thread,find_thread" {
		t.Fatalf("advisor tools = %#v", toolNames)
	}
}

func TestNeoLibrarianSubagentUsesGPT55None(t *testing.T) {
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
	if route == nil || route.Provider != "openai" || route.Model != "gpt-5.5" {
		t.Fatalf("librarian route = %#v, want openai/gpt-5.5", route)
	}
	if seen[0].ReasoningEffort != "none" || stringValue(seen[0].Settings["reasoning.effort"]) != "none" {
		t.Fatalf("librarian effort request=%q settings=%#v, want none", seen[0].ReasoningEffort, seen[0].Settings)
	}
}

func TestIsNeoLocalSubagentTool(t *testing.T) {
	for _, name := range []string{"finder", "oracle", "advisor", "librarian", "Task"} {
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
	advisor := neoSubagentInputText("advisor", map[string]any{
		"task":    "review the auth design",
		"context": "files attached",
		"files":   []any{"a.go", "b.go"},
	})
	if advisor != oracle {
		t.Fatalf("advisor input = %q, want oracle-equivalent %q", advisor, oracle)
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

	if !neoWaitFor(2*time.Second, func() bool {
		actor.mu.Lock()
		_, ok := actor.subagentWaiters["leaf-1"]
		actor.mu.Unlock()
		return ok
	}) {
		t.Fatal("subagent never leased its leaf tool")
	}
	if !actor.routeSubagentLeafToolResult("leaf-1", map[string]any{"status": "done", "output": "neo_runtime.go:1522: type neoActor struct"}) {
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
				return neoInferenceResult{Text: `{"relevantContent":"subagent extracted context"}`}, nil
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
