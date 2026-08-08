package amp

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	"image/color"
	"image/gif"
	"image/jpeg"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
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
		{"oracle", "openai", "gpt-5.6-sol", "high", []string{"Read", "Grep", "glob", "web_search", "read_web_page", "read_thread", "find_thread"}},
		{"librarian", "openai", "gpt-5.6-sol", "none", []string{"read_github", "search_github", "commit_search", "diff", "list_directory_github", "list_repositories", "glob_github"}},
		{"run_check", "openai", "gpt-5.5", "", []string{"Read", "Grep", "glob", "shell_command", "shell_command_status"}},
		// Task inherits the parent model (empty route) and includes finder (a nested subagent).
		{"Task", "", "", "", []string{"Read", "shell_command", "shell_command_status", "apply_patch", "edit_file", "create_file", "read_web_page", "web_search", "finder", "skill", "view_media"}},
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

func TestNeoRunCheckEffectiveReasoningEffortPrecedence(t *testing.T) {
	cases := []struct {
		name      string
		route     neoModelRoute
		inherited string
		want      string
	}{
		{name: "default floor", route: neoModelRoute{}, inherited: "", want: "medium"},
		{name: "low parent floor", route: neoModelRoute{}, inherited: "low", want: "medium"},
		{name: "medium parent", route: neoModelRoute{}, inherited: "medium", want: "medium"},
		{name: "high parent", route: neoModelRoute{}, inherited: "high", want: "high"},
		{name: "max parent", route: neoModelRoute{}, inherited: "max", want: "max"},
		{name: "explicit low suffix", route: neoModelRoute{ThinkingSuffix: "low"}, inherited: "high", want: "low"},
		{name: "explicit max suffix", route: neoModelRoute{ThinkingSuffix: "max"}, inherited: "medium", want: "max"},
		{name: "invalid suffix uses floor", route: neoModelRoute{ThinkingSuffix: "invalid"}, inherited: "low", want: "medium"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := neoSubagentEffectiveReasoningEffort("run_check", tc.route, tc.inherited, ""); got != tc.want {
				t.Fatalf("effective effort = %q, want %q", got, tc.want)
			}
		})
	}
	if got := neoSubagentEffectiveReasoningEffort("finder", neoModelRoute{ThinkingSuffix: "max"}, "high", "low"); got != "low" {
		t.Fatalf("finder effective effort = %q, want configured low", got)
	}
}

func TestNeoRunCheckToolCandidatesRecognizeAmpAliasesOnly(t *testing.T) {
	tests := []struct {
		name string
		want []string
	}{
		{name: "Bash", want: []string{"Bash", "shell_command", "run_terminal_command"}},
		{name: "shell_command", want: []string{"Bash", "shell_command", "run_terminal_command"}},
		{name: "run-terminal-command", want: []string{"Bash", "shell_command", "run_terminal_command"}},
		{name: "Glob", want: []string{"glob", "Glob"}},
		{name: "grep", want: []string{"Grep", "grep"}},
		{name: "read_file", want: []string{"Read", "read_file"}},
		{name: "shell_command_status", want: []string{"shell_command_status"}},
		{name: "apply_patch", want: nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := neoRunCheckToolCandidates(tc.name); !slices.Equal(got, tc.want) {
				t.Fatalf("candidates = %#v, want %#v", got, tc.want)
			}
		})
	}
}

func TestNeoRunCheckPromptRetainsMinorLowSeverityFindings(t *testing.T) {
	for _, want := range []string{
		"Real but minor correctness, testing, maintainability, repository-convention, documentation, auditability, or localized efficiency issue",
		"Do not suppress a valid issue because its correct severity is low",
		"do not promote it merely to make it visible",
		"preference-only style, formatter output, cosmetic nits",
	} {
		if !strings.Contains(neoRunCheckSubagentPrompt, want) {
			t.Fatalf("run_check prompt missing low-severity guidance %q", want)
		}
	}
	if strings.Contains(neoRunCheckSubagentPrompt, "low: Style suggestion") {
		t.Fatalf("run_check prompt still classifies low severity as style-only")
	}
}

func TestNeoRunCheckPromptRequiresSupportedEvidence(t *testing.T) {
	for _, want := range []string{
		"evidence procedure and finding gates as hard requirements",
		"exact import, export, resource path, API shape, or behavior",
		`A nonzero command exit, "no tests found"`,
		"must contain exactly one entry for each pattern index",
		`"outcome": "finding" | "no-finding" | "not-applicable"`,
		`"issueIndexes": [0]`,
		"Every reported issue must be referenced by at least one `finding` entry",
	} {
		if !strings.Contains(neoRunCheckSubagentPrompt, want) {
			t.Fatalf("run_check prompt missing evidence guidance %q", want)
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
			cfg:       &config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{ModeModelRoutes: map[string]config.ModelRouteList{"smart": {"anthropic/claude-fable-5"}}}}},
			agentMode: "smart",
			provider:  "anthropic",
			model:     "claude-fable-5",
		},
		{name: "default-smart", cfg: &config.Config{}, agentMode: "smart", provider: "anthropic", model: "claude-opus-4-8"},
		{name: "default-deep", cfg: &config.Config{}, agentMode: "deep", provider: "openai", model: "gpt-5.5"},
		{
			name:      "explicit-model",
			cfg:       &config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{ModeModelRoutes: map[string]config.ModelRouteList{"smart": {"anthropic/claude-fable-5"}}}}},
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

func TestNeoTaskSubagentInheritsOrderedConfigModeModels(t *testing.T) {
	cfg := &config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{
		ModeModelRoutes: map[string]config.ModelRouteList{
			"ultra": {"openai/gpt-primary", "anthropic/claude-fallback"},
		},
	}}}
	rt := newNeoRuntime(cfg)
	attempts := make([]string, 0, 3)
	rt.inferStream = func(_ *neoRuntime, request neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		route := request.ModelRouteOverride
		attempts = append(attempts, route.Provider+"/"+route.Model)
		if route.Model == "gpt-primary" {
			return neoInferenceResult{}, &neoLocalProviderStatusError{StatusCode: http.StatusServiceUnavailable, Body: `{"error":{"type":"service_unavailable_error","message":"deterministic-looking detail"}}`}
		}
		return neoInferenceResult{Provider: route.Provider, Model: route.Model, Text: "fallback complete"}, nil
	}
	actor := newNeoActor(rt, "actor-task-mode-fallback", "thread-actor", "T-task-mode-fallback", "T-task-mode-fallback", neoActorRecord("actor-task-mode-fallback", "thread-actor", "T-task-mode-fallback"), nil)
	actor.currentAgentMode = "ultra"
	actor.settings["agentMode"] = "ultra"

	result, err := actor.executeSubagentRun("Task", map[string]any{"prompt": "delegate"}, "TU-task-mode-fallback", "M-1", actor.generation, 0, "")
	if err != nil {
		t.Fatalf("Task fallback failed: %v", err)
	}
	if result != "fallback complete" || !slices.Equal(attempts, []string{"openai/gpt-primary", "openai/gpt-primary", "anthropic/claude-fallback"}) {
		t.Fatalf("Task fallback result/attempts = %q/%#v", result, attempts)
	}
}

func TestNeoTaskSubagentInheritsAmpScaffoldContext(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-task-context", "thread-actor", "T-task-context", "T-task-context", neoActorRecord("actor-task-context", "thread-actor", "T-task-context"), nil)
	workspaceRoot := t.TempDir()
	workingDirectory := filepath.Join(workspaceRoot, "internal", "api")
	if err := os.MkdirAll(workingDirectory, 0o700); err != nil {
		t.Fatalf("create working directory: %v", err)
	}
	actor.currentAgentMode = "smart"
	actor.environment = map[string]any{
		"workingDirectory": workingDirectory,
		"workspaceRoot":    workspaceRoot,
		"trees":            []any{map[string]any{"uri": neoFileURLForDirectory(workspaceRoot)}},
	}
	actor.capabilities = map[string]any{"skills": []any{map[string]any{
		"name":        "task-context-skill",
		"description": "Use the task context workflow.",
		"location":    filepath.Join(workspaceRoot, "skills", "task-context-skill", "SKILL.md"),
	}}}
	actor.guidanceSnapshot = map[string]any{"files": []any{map[string]any{
		"content": "TASK_CONTEXT_GUIDANCE",
		"uri":     neoFileURLForDirectory(filepath.Join(workspaceRoot, "AGENTS.md")),
	}}}

	var requests []neoInferenceRequest
	rt.inferStream = func(_ *neoRuntime, request neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		requests = append(requests, request)
		return neoInferenceResult{Text: "done"}, nil
	}

	text, err := actor.executeSubagentRun("Task", map[string]any{"prompt": "inspect the workspace", "description": "inspect"}, "TU-task-context", "M-1", actor.generation, 0, "")
	if err != nil {
		t.Fatalf("Task subagent failed: %v", err)
	}
	if text != "done" || len(requests) != 1 {
		t.Fatalf("Task result = %q requests=%d, want one completed inference", text, len(requests))
	}
	for _, request := range requests {
		for _, want := range []string{
			"# Environment",
			"Working directory: " + workingDirectory,
			"Workspace root: " + workspaceRoot,
			"TASK_CONTEXT_GUIDANCE",
			"task-context-skill",
			"You are a worker agent for one bounded task.",
		} {
			if !strings.Contains(request.SystemPromptOverride, want) {
				t.Fatalf("Task system prompt missing %q:\n%s", want, request.SystemPromptOverride)
			}
		}
		if strings.Index(request.SystemPromptOverride, "# Environment") > strings.Index(request.SystemPromptOverride, "You are a worker agent for one bounded task.") {
			t.Fatalf("Task specialization preceded the Amp scaffold:\n%s", request.SystemPromptOverride)
		}
		if len(request.Capabilities) == 0 || len(request.Guidance) == 0 {
			t.Fatalf("Task request lost inherited context: capabilities=%#v guidance=%#v", request.Capabilities, request.Guidance)
		}
	}
}

func TestNeoOracleSubagentUsesCurrentModeRoute(t *testing.T) {
	cfg := &config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{
		ModeModelRoutes: map[string]config.ModelRouteList{"smart": {"openai/gpt-5.5"}},
	}}}
	cases := []struct {
		mode     string
		provider string
		model    string
	}{
		{mode: "low", provider: "openai", model: "gpt-5.6-sol"},
		{mode: "medium", provider: "openai", model: "gpt-5.6-sol"},
		{mode: "high", provider: "anthropic", model: "claude-fable-5"},
		{mode: "ultra", provider: "openai", model: "gpt-5.6-sol"},
		{mode: "smart", provider: "openai", model: "gpt-5.6-sol"},
	}
	for _, tc := range cases {
		t.Run(tc.mode, func(t *testing.T) {
			got := captureNeoSubagentRouteForTest(t, "oracle", "oracle-"+tc.mode, cfg, tc.mode, nil)
			if got.Provider != tc.provider || got.Model != tc.model {
				t.Fatalf("oracle route = %#v, want %s/%s", got, tc.provider, tc.model)
			}
		})
	}
}

func TestNeoConfiguredSubagentRoutesOverrideDefaults(t *testing.T) {
	cfg := &config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{
		ModeModelRoutes: map[string]config.ModelRouteList{"high": {"google/gemini-3-pro"}},
		SubagentModels: map[string][]string{
			"oracle": {"", "chatgpt-web/gpt-5-6-pro", "anthropic/claude-fable-5"},
			"task":   {"openai/gpt-5.5"},
		},
	}}}

	oracleRoutes := neoConfiguredSubagentRoutes(cfg, "oracle")
	if len(oracleRoutes) != 2 {
		t.Fatalf("oracle routes = %#v, want two non-empty routes", oracleRoutes)
	}
	if oracleRoutes[0].Provider != "openai" || oracleRoutes[0].Model != "gpt-5-6-pro" || !oracleRoutes[0].TextToolBridge {
		t.Fatalf("oracle primary route = %#v, want bridged ChatGPT Web", oracleRoutes[0])
	}
	if oracleRoutes[1].Provider != "anthropic" || oracleRoutes[1].Model != "claude-fable-5" || oracleRoutes[1].TextToolBridge {
		t.Fatalf("oracle fallback route = %#v, want native Anthropic", oracleRoutes[1])
	}
	bareRoute := parseNeoModelRoute("gpt-5-6-pro")
	if bareRoute.Provider != "openai" || bareRoute.TextToolBridge {
		t.Fatalf("bare provider-ambiguous route = %#v, want native OpenAI", bareRoute)
	}
	dynamicRoute := parseNeoModelRoute("chatgpt-web/gpt-5-5-thinking")
	if dynamicRoute.Provider != "openai" || dynamicRoute.Model != "chatgpt-web/gpt-5-5-thinking" || !dynamicRoute.TextToolBridge {
		t.Fatalf("dynamic ChatGPT Web route = %#v, want namespaced text-tool bridge", dynamicRoute)
	}

	oracle := captureNeoSubagentRouteForTest(t, "oracle", "configured-oracle", cfg, "high", nil)
	if oracle != oracleRoutes[0] {
		t.Fatalf("configured high-mode oracle route = %#v, want %#v", oracle, oracleRoutes[0])
	}
	task := captureNeoSubagentRouteForTest(t, "Task", "configured-task", cfg, "high", map[string]any{"internal.model": "anthropic/claude-opus-4-8"})
	if task.Provider != "openai" || task.Model != "gpt-5.5" {
		t.Fatalf("configured Task route = %#v, want openai/gpt-5.5", task)
	}
}

func TestNeoTextToolBridgeRequestAndResult(t *testing.T) {
	tools := []neoToolSpec{
		{Name: "Read", Description: "Read a file", InputSchema: map[string]any{"type": "object", "properties": map[string]any{"path": map[string]any{"type": "string"}}}, Meta: map[string]any{"hidden": "server-only"}},
		{Name: "Grep", Description: "Search files"},
	}
	request := neoInferenceRequest{
		SystemPromptOverride: "Oracle prompt",
		Tools:                tools,
		History: []neoHistoryMessage{
			{Role: "user", Text: "Inspect the runtime"},
			{Role: "assistant", Text: "I will inspect it.", ToolCalls: []neoToolCall{{ID: "TU-read", Name: "Read", Input: map[string]any{"path": "neo_runtime.go"}}}, ThinkingBlocks: []neoThinkingBlock{{Thinking: "hidden"}}},
			{Role: "tool", ToolCallID: "TU-read", ToolName: "Read", Text: "file contents", Content: []any{map[string]any{"type": "text", "text": "file contents"}}},
		},
	}

	wire, err := neoTextToolBridgeRequest(request)
	if err != nil {
		t.Fatalf("bridge request: %v", err)
	}
	if len(wire.Tools) != 0 {
		t.Fatalf("wire tools = %#v, want text-only request", wire.Tools)
	}
	for _, want := range []string{"Oracle prompt", neoTextToolCallsOpen, `"name":"Read"`, `"input_schema"`} {
		if !strings.Contains(wire.SystemPromptOverride, want) {
			t.Fatalf("wire system prompt missing %q:\n%s", want, wire.SystemPromptOverride)
		}
	}
	if strings.Contains(wire.SystemPromptOverride, "server-only") {
		t.Fatalf("wire system prompt exposed tool metadata:\n%s", wire.SystemPromptOverride)
	}
	if len(wire.History) != 3 || wire.History[2].Role != "user" || !strings.Contains(wire.History[2].Text, neoTextToolCallsOpen) || !strings.Contains(wire.History[2].Text, `"input_schema"`) || !strings.Contains(wire.History[2].Text, neoTextToolResultsOpen) || !strings.Contains(wire.History[2].Text, `"name":"Read"`) {
		t.Fatalf("wire history = %#v, want text call/result turns", wire.History)
	}
	for _, message := range wire.History {
		if len(message.Content) != 0 || len(message.OpenAIItems) != 0 || len(message.ToolCalls) != 0 || len(message.ThinkingBlocks) != 0 || message.ToolCallID != "" || message.ToolName != "" {
			t.Fatalf("wire history retained structured fields: %#v", message)
		}
	}

	result, err := neoParseTextToolBridgeResult(neoInferenceResult{
		Provider: "openai",
		Model:    "gpt-5-6-pro",
		Text:     "I need the file.\n" + neoTextToolCallsOpen + `[{"name":"Read","input":{"path":"neo_runtime.go"}}]` + neoTextToolCallsClose,
	}, tools)
	if err != nil {
		t.Fatalf("parse bridge result: %v", err)
	}
	if result.Text != "I need the file." || len(result.ToolCalls) != 1 {
		t.Fatalf("parsed result = %#v", result)
	}
	call := result.ToolCalls[0]
	if !strings.HasPrefix(call.ID, "TU-") || call.Name != "Read" || !reflect.DeepEqual(call.Input, map[string]any{"path": "neo_runtime.go"}) {
		t.Fatalf("parsed tool call = %#v", call)
	}
	markerPattern := neoTextToolCallsOpen + "|" + neoTextToolCallsClose
	result, err = neoParseTextToolBridgeResult(neoInferenceResult{
		Text: neoTextToolCallsOpen + `[{"name":"Grep","input":{"pattern":"` + markerPattern + `"}}]` + neoTextToolCallsClose,
	}, tools)
	if err != nil || len(result.ToolCalls) != 1 || stringValue(result.ToolCalls[0].Input["pattern"]) != markerPattern {
		t.Fatalf("marker-valued tool call = %#v, %v", result, err)
	}
}

func TestNeoTextToolBridgeRejectsMalformedCalls(t *testing.T) {
	tools := []neoToolSpec{{Name: "Read"}}
	tests := map[string]string{
		"unknown tool":    neoTextToolCallsOpen + `[{"name":"Write","input":{}}]` + neoTextToolCallsClose,
		"scalar input":    neoTextToolCallsOpen + `[{"name":"Read","input":"file"}]` + neoTextToolCallsClose,
		"trailing text":   neoTextToolCallsOpen + `[{"name":"Read","input":{}}]` + neoTextToolCallsClose + " trailing",
		"multiple blocks": neoTextToolCallsOpen + `[{"name":"Read","input":{}}]` + neoTextToolCallsClose + neoTextToolCallsOpen + `[{"name":"Read","input":{}}]` + neoTextToolCallsClose,
		"unknown field":   neoTextToolCallsOpen + `[{"name":"Read","input":{},"extra":true}]` + neoTextToolCallsClose,
		"empty calls":     neoTextToolCallsOpen + `[]` + neoTextToolCallsClose,
	}
	for name, text := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := neoParseTextToolBridgeResult(neoInferenceResult{Text: text}, tools); err == nil {
				t.Fatalf("malformed response was accepted: %s", text)
			}
		})
	}
}

func TestNeoTextToolBridgeRepairsMalformedCall(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	route := parseNeoModelRoute("chatgpt-web/gpt-5-6-pro")
	requests := make([]neoInferenceRequest, 0, 2)
	rt.inferStream = func(_ *neoRuntime, request neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		requests = append(requests, request)
		if len(requests) == 1 {
			return neoInferenceResult{Text: neoTextToolCallsOpen + `[{"name":"Unknown","input":{}}]` + neoTextToolCallsClose, Usage: map[string]any{"inputTokens": 2, "outputTokens": 1}}, nil
		}
		return neoInferenceResult{Text: neoTextToolCallsOpen + `[{"name":"Read","input":{"path":"fixed.go"}}]` + neoTextToolCallsClose, Usage: map[string]any{"inputTokens": 3, "outputTokens": 2}}, nil
	}
	result, err := rt.subagentInfer(neoInferenceRequest{
		Context:            context.Background(),
		History:            []neoHistoryMessage{{Role: "user", Text: "Inspect it"}},
		Tools:              []neoToolSpec{{Name: "Read"}},
		ModelRouteOverride: &route,
	}, func(neoInferenceDelta) {})
	if err != nil {
		t.Fatalf("bridge repair failed: %v", err)
	}
	if len(requests) != 2 || len(result.ToolCalls) != 1 || result.ToolCalls[0].Name != "Read" {
		t.Fatalf("bridge repair requests=%d result=%#v", len(requests), result)
	}
	if numberFrom(result.Usage["inputTokens"]) != 5 || numberFrom(result.Usage["outputTokens"]) != 3 {
		t.Fatalf("bridge repair usage = %#v, want both attempts", result.Usage)
	}
	for _, request := range requests {
		if len(request.Tools) != 0 || !strings.Contains(request.SystemPromptOverride, neoTextToolCallsOpen) {
			t.Fatalf("bridge request was not text-only: %#v", request)
		}
	}
	if last := requests[1].History[len(requests[1].History)-1]; last.Role != "user" || !strings.Contains(last.Text, "previous tool request was invalid") {
		t.Fatalf("repair history did not explain protocol failure: %#v", last)
	}
}

func TestNeoTextToolBridgeRequiresToolCallAndRepairsPlainText(t *testing.T) {
	requests := make([]neoInferenceRequest, 0, 2)
	result, err := neoInferTextToolBridge(neoInferenceRequest{
		History:                       []neoHistoryMessage{{Role: "user", Text: "Inspect it"}},
		Tools:                         []neoToolSpec{{Name: "Read"}},
		TextToolBridgeRequireToolCall: true,
	}, func(request neoInferenceRequest) (neoInferenceResult, error) {
		requests = append(requests, request)
		if len(requests) == 1 {
			return neoInferenceResult{Text: "I inspected it without tools.", Usage: map[string]any{"inputTokens": 2, "outputTokens": 1}}, nil
		}
		return neoInferenceResult{Text: neoTextToolCallsOpen + `[{"id":"model-supplied","name":"Read","input":{"path":"fixed.go"}}]` + neoTextToolCallsClose, Usage: map[string]any{"inputTokens": 3, "outputTokens": 2}}, nil
	})
	if err != nil {
		t.Fatalf("required bridge repair failed: %v", err)
	}
	if len(requests) != 2 || len(result.ToolCalls) != 1 || result.ToolCalls[0].Name != "Read" {
		t.Fatalf("required bridge repair requests=%d result=%#v", len(requests), result)
	}
	if !strings.HasPrefix(result.ToolCalls[0].ID, "TU-") || result.ToolCalls[0].ID == "model-supplied" {
		t.Fatalf("required bridge tool ID = %q, want fresh Neo ID", result.ToolCalls[0].ID)
	}
	if numberFrom(result.Usage["inputTokens"]) != 5 || numberFrom(result.Usage["outputTokens"]) != 3 {
		t.Fatalf("required bridge repair usage = %#v, want both attempts", result.Usage)
	}
	for _, request := range requests {
		if len(request.Tools) != 0 || !strings.Contains(request.SystemPromptOverride, "This turn requires at least one valid tool call") {
			t.Fatalf("required bridge request was not enforced: %#v", request)
		}
	}
	last := requests[1].History[len(requests[1].History)-1]
	if last.Role != "user" || !strings.Contains(last.Text, "A final text answer is not allowed on this turn") {
		t.Fatalf("required bridge repair did not reject final text: %#v", last)
	}
}

func TestNeoTextToolBridgeRequiredToolCallRejectsPlainTextAfterRepair(t *testing.T) {
	calls := 0
	result, err := neoInferTextToolBridge(neoInferenceRequest{
		History:                       []neoHistoryMessage{{Role: "user", Text: "Inspect it"}},
		Tools:                         []neoToolSpec{{Name: "Read"}},
		TextToolBridgeRequireToolCall: true,
	}, func(neoInferenceRequest) (neoInferenceResult, error) {
		calls++
		return neoInferenceResult{Text: "Tool-free answer", Usage: map[string]any{"inputTokens": calls, "outputTokens": 1}}, nil
	})
	if err == nil || !strings.Contains(err.Error(), "text tool protocol failed after repair") || !strings.Contains(err.Error(), "required tool call") {
		t.Fatalf("required bridge error = %v", err)
	}
	if calls != 2 {
		t.Fatalf("required bridge calls = %d, want bounded repair", calls)
	}
	if numberFrom(result.Usage["inputTokens"]) != 3 || numberFrom(result.Usage["outputTokens"]) != 2 {
		t.Fatalf("rejected bridge usage = %#v, want both attempts", result.Usage)
	}
}

func TestNeoTextToolBridgeAllowsPlainTextWhenToolCallNotRequired(t *testing.T) {
	calls := 0
	result, err := neoInferTextToolBridge(neoInferenceRequest{
		History: []neoHistoryMessage{{Role: "user", Text: "Advise me"}},
		Tools:   []neoToolSpec{{Name: "Read"}},
	}, func(request neoInferenceRequest) (neoInferenceResult, error) {
		calls++
		if strings.Contains(request.SystemPromptOverride, "This turn requires at least one valid tool call") {
			t.Fatalf("optional bridge unexpectedly required a tool call")
		}
		return neoInferenceResult{Text: "Final advice"}, nil
	})
	if err != nil || calls != 1 || result.Text != "Final advice" || len(result.ToolCalls) != 0 {
		t.Fatalf("optional bridge result=%#v err=%v calls=%d", result, err, calls)
	}
}

func TestInferNeoLocalStreamBridgesChatGPTWebTools(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/api/provider/openai/v1/responses" {
			t.Fatalf("provider path = %q", request.URL.Path)
		}
		payload := readNeoJSON(request.Body)
		if len(arrayValue(payload["tools"])) != 0 {
			t.Fatalf("provider payload retained native tools: %#v", payload)
		}
		rendered := fmt.Sprint(payload["input"])
		for _, want := range []string{neoTextToolCallsOpen, `"name":"Read"`, "Inspect the runtime"} {
			if !strings.Contains(rendered, want) {
				t.Fatalf("provider input missing %q: %#v", want, payload["input"])
			}
		}
		for _, raw := range arrayValue(payload["input"]) {
			itemType := stringValue(mapValue(raw)["type"])
			if itemType == "function_call" || itemType == "function_call_output" || itemType == "reasoning" {
				t.Fatalf("provider input contains structured %q item: %#v", itemType, payload["input"])
			}
		}
		responseText := neoTextToolCallsOpen + `[{"name":"Read","input":{"path":"neo_runtime.go"}}]` + neoTextToolCallsClose
		delta, _ := json.Marshal(map[string]any{"type": "response.output_text.delta", "output_index": 0, "content_index": 0, "delta": responseText})
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintf(w, "data: %s\n\n", delta)
		fmt.Fprint(w, "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":1,\"output_tokens\":1},\"output\":[]}}\n\ndata: [DONE]\n\n")
	}))
	defer upstream.Close()

	var deltas []neoInferenceDelta
	result, err := inferNeoLocalStream(testNeoRuntimeForServer(t, upstream), neoInferenceRequest{
		Context:   context.Background(),
		AgentMode: "smart",
		Settings:  map[string]any{"internal.model": "chatgpt-web/gpt-5-6-pro"},
		History:   []neoHistoryMessage{{Role: "user", Text: "Inspect the runtime"}},
		Tools:     []neoToolSpec{{Name: "Read", Description: "Read a file", InputSchema: map[string]any{"type": "object"}}},
	}, func(delta neoInferenceDelta) {
		deltas = append(deltas, delta)
	})
	if err != nil {
		t.Fatalf("bridged inference failed: %v", err)
	}
	if result.Text != "" || len(result.ToolCalls) != 1 || result.ToolCalls[0].Name != "Read" {
		t.Fatalf("bridged result = %#v", result)
	}
	if len(deltas) != 1 || deltas[0].ToolCall == nil || deltas[0].ToolCall.Name != "Read" || !deltas[0].ToolCall.Complete {
		t.Fatalf("bridged deltas = %#v", deltas)
	}
}

func TestNeoChatGPTWebOracleCompletesToolCycle(t *testing.T) {
	providerCalls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		providerCalls++
		payload := readNeoJSON(request.Body)
		if request.URL.Path != "/api/provider/openai/v1/responses" || stringValue(payload["model"]) != "chatgpt-web/gpt-5-5-thinking" {
			t.Fatalf("provider request path/model = %q/%q", request.URL.Path, stringValue(payload["model"]))
		}
		if len(arrayValue(payload["tools"])) != 0 {
			t.Fatalf("provider payload retained native tools: %#v", payload)
		}
		for _, raw := range arrayValue(payload["input"]) {
			itemType := stringValue(mapValue(raw)["type"])
			if itemType == "function_call" || itemType == "function_call_output" || itemType == "reasoning" {
				t.Fatalf("provider input contains structured %q item: %#v", itemType, payload["input"])
			}
		}
		rendered := fmt.Sprint(payload["input"])
		var responseText string
		switch providerCalls {
		case 1:
			if !strings.Contains(rendered, "Inspect the runtime") || !strings.Contains(rendered, neoTextToolCallsOpen) || !strings.Contains(rendered, "This turn requires at least one valid tool call") {
				t.Fatalf("initial provider input = %#v", payload["input"])
			}
			responseText = "I will inspect the file.\n" + neoTextToolCallsOpen + `[{"id":"model-supplied","name":"Read","input":{"path":"neo_runtime.go"}}]` + neoTextToolCallsClose
		case 2:
			for _, want := range []string{neoTextToolCallsOpen, neoTextToolResultsOpen, "package amp"} {
				if !strings.Contains(rendered, want) {
					t.Fatalf("continuation provider input missing %q: %#v", want, payload["input"])
				}
			}
			if strings.Contains(rendered, "This turn requires at least one valid tool call") {
				t.Fatalf("continuation provider input still required a tool call: %#v", payload["input"])
			}
			responseText = "Oracle completed the inspection."
		default:
			t.Fatalf("provider calls = %d, want two", providerCalls)
		}
		delta, _ := json.Marshal(map[string]any{"type": "response.output_text.delta", "output_index": 0, "content_index": 0, "delta": responseText})
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintf(w, "data: %s\n\n", delta)
		fmt.Fprint(w, "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":1,\"output_tokens\":1},\"output\":[]}}\n\ndata: [DONE]\n\n")
	}))
	defer upstream.Close()

	rt := testNeoRuntimeForServer(t, upstream)
	cfg := *rt.configSnapshot()
	cfg.AmpCode.NeoLocalRuntime.SubagentModels = map[string][]string{
		"oracle": {"chatgpt-web/gpt-5-5-thinking"},
	}
	if err := rt.updateConfig(&cfg); err != nil {
		t.Fatalf("update runtime config: %v", err)
	}
	actor := newNeoActor(rt, "actor-chatgpt-web-oracle", "thread-actor", "T-chatgpt-web-oracle", "T-chatgpt-web-oracle", neoActorRecord("actor-chatgpt-web-oracle", "thread-actor", "T-chatgpt-web-oracle"), nil)
	actor.currentAgentMode = "high"
	actor.tools = map[string]neoToolSpec{
		"Read": {Name: "Read", Description: "Read a file", InputSchema: map[string]any{"type": "object"}},
	}
	leasedToolCallID := ""
	socket := &neoSocket{writeMessage: func(_ int, data []byte) error {
		var event map[string]any
		if err := json.Unmarshal(data, &event); err != nil || stringValue(event["type"]) != "tool_lease" {
			return nil
		}
		leasedToolCallID = stringValue(event["toolCallId"])
		go actor.routeSubagentLeafToolResult(leasedToolCallID, map[string]any{"status": "done", "output": "package amp"})
		return nil
	}}
	actor.sockets[socket] = struct{}{}

	text, err := actor.executeSubagentRun("oracle", map[string]any{"task": "Inspect the runtime"}, "TU-parent-oracle", "M-parent", actor.generation, 0, "")
	if err != nil {
		t.Fatalf("ChatGPT Web Oracle tool cycle failed: %v", err)
	}
	if text != "Oracle completed the inspection." || providerCalls != 2 {
		t.Fatalf("Oracle result/calls = %q/%d", text, providerCalls)
	}
	if !strings.HasPrefix(leasedToolCallID, "TU-") || leasedToolCallID == "model-supplied" {
		t.Fatalf("leased tool call ID = %q, want fresh Neo ID", leasedToolCallID)
	}
	leafUseFound := false
	leafResultText := ""
	actor.mu.Lock()
	for _, message := range actor.messages {
		if message.ParentToolUseID != "TU-parent-oracle" || len(message.Content) != 1 {
			continue
		}
		block := mapValue(message.Content[0])
		if message.Role == "assistant" && stringValue(block["type"]) == "tool_use" && stringValue(block["id"]) == leasedToolCallID && stringValue(block["name"]) == "Read" {
			leafUseFound = true
		}
		if message.Role == "user" && message.MessageID == toolResultMessageID(leasedToolCallID) && stringValue(block["type"]) == "tool_result" && stringValue(block["toolUseID"]) == leasedToolCallID {
			leafResultText = runToText(mapValue(block["run"]))
		}
	}
	actor.mu.Unlock()
	if !leafUseFound || leafResultText != "package amp" {
		t.Fatalf("Oracle leaf persistence = use:%v result:%q, want parent-linked Read leaf and result", leafUseFound, leafResultText)
	}
}

func TestNeoOracleToolCycleCompletion(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		calls []neoToolCall
		want  bool
	}{
		{name: "no calls", calls: nil, want: false},
		{name: "incomplete only", calls: []neoToolCall{{ID: "TU-1", Name: "Read", Incomplete: true}}, want: false},
		{name: "single complete call", calls: []neoToolCall{{ID: "TU-2", Name: "Read"}}, want: true},
		{name: "mixed complete and incomplete", calls: []neoToolCall{
			{ID: "TU-3", Name: "Read", Incomplete: true},
			{ID: "TU-4", Name: "Read"},
		}, want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := neoOracleToolCycleCompleted(test.calls); got != test.want {
				t.Fatalf("neoOracleToolCycleCompleted() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestNeoChatGPTWebOracleIncompleteOnlyCycleKeepsToolCallRequired(t *testing.T) {
	providerCalls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		providerCalls++
		payload := readNeoJSON(request.Body)
		if request.URL.Path != "/api/provider/openai/v1/responses" || stringValue(payload["model"]) != "chatgpt-web/gpt-5-5-thinking" {
			t.Fatalf("provider request path/model = %q/%q", request.URL.Path, stringValue(payload["model"]))
		}
		if len(arrayValue(payload["tools"])) != 0 {
			t.Fatalf("provider payload retained native tools: %#v", payload)
		}
		rendered := fmt.Sprint(payload["input"])
		var responseText string
		switch providerCalls {
		case 1:
			if !strings.Contains(rendered, "This turn requires at least one valid tool call") {
				t.Fatalf("initial oracle request did not require a tool call: %#v", payload["input"])
			}
			responseText = "I will inspect the file.\n" + neoTextToolCallsOpen + `[{"name":"Read","input":{"path":"neo_runtime.go"}}]` + neoTextToolCallsClose
		case 2:
			if strings.Contains(rendered, "This turn requires at least one valid tool call") {
				t.Fatalf("continuation request still required a tool call after a completed cycle: %#v", payload["input"])
			}
			responseText = "Oracle completed the inspection."
		default:
			t.Fatalf("provider calls = %d, want two", providerCalls)
		}
		delta, _ := json.Marshal(map[string]any{"type": "response.output_text.delta", "output_index": 0, "content_index": 0, "delta": responseText})
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintf(w, "data: %s\n\n", delta)
		fmt.Fprint(w, "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":1,\"output_tokens\":1},\"output\":[]}}\n\ndata: [DONE]\n\n")
	}))
	t.Cleanup(upstream.Close)

	rt := testNeoRuntimeForServer(t, upstream)
	cfg := *rt.configSnapshot()
	cfg.AmpCode.NeoLocalRuntime.SubagentModels = map[string][]string{
		"oracle": {"chatgpt-web/gpt-5-5-thinking"},
	}
	if err := rt.updateConfig(&cfg); err != nil {
		t.Fatalf("update runtime config: %v", err)
	}
	actor := newNeoActor(rt, "actor-oracle-incomplete-cycle", "thread-actor", "T-oracle-incomplete-cycle", "T-oracle-incomplete-cycle", neoActorRecord("actor-oracle-incomplete-cycle", "thread-actor", "T-oracle-incomplete-cycle"), nil)
	actor.currentAgentMode = "high"
	actor.tools = map[string]neoToolSpec{
		"Read": {Name: "Read", Description: "Read a file", InputSchema: map[string]any{"type": "object"}},
	}

	socket := &neoSocket{writeMessage: func(_ int, data []byte) error {
		var event map[string]any
		if err := json.Unmarshal(data, &event); err != nil || stringValue(event["type"]) != "tool_lease" {
			return nil
		}
		toolCallID := stringValue(event["toolCallId"])
		go actor.routeSubagentLeafToolResult(toolCallID, map[string]any{"status": "done", "output": "package amp"})
		return nil
	}}
	actor.sockets[socket] = struct{}{}

	text, err := actor.executeSubagentRun("oracle", map[string]any{"task": "Inspect the runtime"}, "TU-parent-oracle-incomplete", "M-parent", actor.generation, 0, "")
	if err != nil {
		t.Fatalf("oracle bridge run failed: %v", err)
	}
	if text != "Oracle completed the inspection." {
		t.Fatalf("oracle bridge result = %q", text)
	}
	if providerCalls != 2 {
		t.Fatalf("provider calls = %d, want two", providerCalls)
	}
}

func TestNeoChatGPTWebOracleToolRefusalDoesNotFallback(t *testing.T) {
	cfg := &config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{SubagentModels: map[string][]string{
		"oracle": {"chatgpt-web/gpt-5-6-pro", "anthropic/claude-fable-5"},
	}}}}
	rt := newNeoRuntime(cfg)
	attempts := make([]string, 0, 3)
	repairRequired := false
	rt.inferStream = func(_ *neoRuntime, request neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		model := request.ModelRouteOverride.Model
		attempts = append(attempts, model)
		switch model {
		case "gpt-5-6-pro":
			if len(request.Tools) != 0 || !request.TextToolBridgeRequireToolCall {
				t.Fatalf("Web Oracle bridge request = %#v, want text-only required call", request)
			}
			if len(attempts) == 2 {
				last := request.History[len(request.History)-1]
				repairRequired = last.Role == "user" && strings.Contains(last.Text, "A final text answer is not allowed on this turn")
			}
			return neoInferenceResult{Text: "Ungrounded Web answer"}, nil
		case "claude-fable-5":
			if request.TextToolBridgeRequireToolCall || len(request.Tools) != 1 {
				t.Fatalf("native Fable request = %#v, want unchanged native tools", request)
			}
			if len(request.History) != 1 || strings.Contains(request.History[0].Text, "Ungrounded Web answer") {
				t.Fatalf("failed Web attempts leaked into fallback history: %#v", request.History)
			}
			return neoInferenceResult{Text: "Grounded fallback answer"}, nil
		default:
			return neoInferenceResult{}, fmt.Errorf("unexpected Oracle model %q", model)
		}
	}

	actor := newNeoActor(rt, "actor-web-refusal", "thread-actor", "T-web-refusal", "T-web-refusal", neoActorRecord("actor-web-refusal", "thread-actor", "T-web-refusal"), nil)
	actor.currentAgentMode = "high"
	actor.tools = map[string]neoToolSpec{
		"Read": {Name: "Read", Description: "Read a file", InputSchema: map[string]any{"type": "object"}},
	}
	text, err := actor.executeSubagentRun("oracle", map[string]any{"task": "Inspect the runtime"}, "TU-parent-oracle", "M-parent", actor.generation, 0, "")
	if err == nil || !strings.Contains(err.Error(), "text tool protocol failed after repair") {
		t.Fatalf("Web Oracle refusal error = %v", err)
	}
	if text != "" || !repairRequired {
		t.Fatalf("Web Oracle refusal result=%q repair_required=%v", text, repairRequired)
	}
	wantAttempts := []string{"gpt-5-6-pro", "gpt-5-6-pro"}
	if !reflect.DeepEqual(attempts, wantAttempts) {
		t.Fatalf("Web Oracle refusal attempts = %#v, want %#v", attempts, wantAttempts)
	}
}

func TestNeoSubagentModelFallbacksAreOrderedAndSticky(t *testing.T) {
	cfg := &config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{SubagentModels: map[string][]string{
		"task": {"openai/model-a", "openai/model-b", "openai/model-c"},
	}}}}
	rt := newNeoRuntime(cfg)
	taskAttempts := make([]string, 0, 4)
	bCalls := 0
	transientErr := errors.New(`local provider stream error: {"type":"error","error":{"type":"overloaded_error","message":"temporarily unavailable"}}`)
	rt.inferStream = func(_ *neoRuntime, request neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		if strings.Contains(request.SystemPromptOverride, "fast, parallel code search agent") {
			return neoInferenceResult{Text: "finder result"}, nil
		}
		model := request.ModelRouteOverride.Model
		taskAttempts = append(taskAttempts, model)
		switch model {
		case "model-a":
			return neoInferenceResult{}, transientErr
		case "model-b":
			bCalls++
			if bCalls == 1 {
				return neoInferenceResult{ToolCalls: []neoToolCall{{Name: "finder", Input: map[string]any{"query": "find it"}}}}, nil
			}
			return neoInferenceResult{}, transientErr
		case "model-c":
			return neoInferenceResult{Text: "completed through fallback"}, nil
		default:
			return neoInferenceResult{}, fmt.Errorf("unexpected task model %q", model)
		}
	}

	actor := newNeoActor(rt, "actor-fallback", "thread-actor", "T-fallback", "T-fallback", neoActorRecord("actor-fallback", "thread-actor", "T-fallback"), nil)
	setNeoFinderWorkspaceForTest(t, actor)
	text, err := actor.executeSubagentRun("Task", map[string]any{"prompt": "investigate", "description": "investigate"}, "TU-task", "M-1", actor.generation, 0, "")
	if err != nil {
		t.Fatalf("Task fallback run failed: %v", err)
	}
	if text != "completed through fallback" {
		t.Fatalf("Task fallback result = %q", text)
	}
	want := []string{"model-a", "model-a", "model-b", "model-b", "model-b", "model-c"}
	if !reflect.DeepEqual(taskAttempts, want) {
		t.Fatalf("Task attempts = %#v, want %#v", taskAttempts, want)
	}
}

func TestNeoSubagentModelFallbacksPreserveAttemptErrors(t *testing.T) {
	firstErr := errors.New(`local provider stream error: {"type":"error","error":{"type":"overloaded_error","message":"primary unavailable"}}`)
	secondErr := errors.New(`local provider stream error: {"type":"error","error":{"type":"overloaded_error","message":"fallback unavailable"}}`)
	cfg := &config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{SubagentModels: map[string][]string{
		"oracle": {"openai/model-a", "anthropic/model-b"},
	}}}}
	rt := newNeoRuntime(cfg)
	attempts := make([]string, 0, 2)
	rt.inferStream = func(_ *neoRuntime, request neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		model := request.ModelRouteOverride.Model
		attempts = append(attempts, model)
		if model == "model-a" {
			return neoInferenceResult{}, firstErr
		}
		return neoInferenceResult{}, secondErr
	}
	actor := newNeoActor(rt, "actor-exhausted", "thread-actor", "T-exhausted", "T-exhausted", neoActorRecord("actor-exhausted", "thread-actor", "T-exhausted"), nil)
	_, err := actor.executeSubagentRun("oracle", map[string]any{"task": "advise"}, "TU-oracle", "M-1", actor.generation, 0, "")
	if err == nil || !errors.Is(err, firstErr) || !errors.Is(err, secondErr) {
		t.Fatalf("exhausted fallback error = %v", err)
	}
	if want := []string{"model-a", "model-a", "model-b", "model-b"}; !reflect.DeepEqual(attempts, want) {
		t.Fatalf("fallback attempts = %#v, want %#v", attempts, want)
	}
}

func TestNeoSubagentModelFallbacksDoNotRetryProviderDeadline(t *testing.T) {
	cfg := &config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{SubagentModels: map[string][]string{
		"oracle": {"openai/model-a", "anthropic/model-b"},
	}}}}
	rt := newNeoRuntime(cfg)
	attempts := 0
	rt.inferStream = func(_ *neoRuntime, _ neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		attempts++
		if attempts == 1 {
			return neoInferenceResult{}, context.DeadlineExceeded
		}
		return neoInferenceResult{Text: "fallback completed"}, nil
	}
	actor := newNeoActor(rt, "actor-cancelled", "thread-actor", "T-cancelled", "T-cancelled", neoActorRecord("actor-cancelled", "thread-actor", "T-cancelled"), nil)
	text, err := actor.executeSubagentRun("oracle", map[string]any{"task": "advise"}, "TU-oracle", "M-1", actor.generation, 0, "")
	if !errors.Is(err, context.DeadlineExceeded) || text != "" || attempts != 1 {
		t.Fatalf("deadline fallback result text=%q err=%v attempts=%d", text, err, attempts)
	}
}

func TestNeoSubagentRetriesTransientLocalProviderStreamError(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	attempts := 0
	rt.inferStream = func(_ *neoRuntime, _ neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		attempts++
		if attempts == 1 {
			return neoInferenceResult{}, errors.New(`local provider stream error: {"type":"error","code":"internal_server_error","message":"stream error: stream ID 975; INTERNAL_ERROR; received from peer"}`)
		}
		return neoInferenceResult{Text: "continued"}, nil
	}
	actor := newNeoActor(rt, "actor-transient-retry", "thread-actor", "T-transient-retry", "T-transient-retry", neoActorRecord("actor-transient-retry", "thread-actor", "T-transient-retry"), nil)

	text, err := actor.executeSubagentRun("Task", map[string]any{"prompt": "continue", "description": "continue"}, "TU-transient-retry", "M-1", actor.generation, 0, "")
	if err != nil || text != "continued" || attempts != 2 {
		t.Fatalf("transient retry result text=%q err=%v attempts=%d", text, err, attempts)
	}
}

func TestNeoSubagentRetriesOverloadedLocalProviderStreamError(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	attempts := 0
	rt.inferStream = func(_ *neoRuntime, _ neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		attempts++
		if attempts == 1 {
			return neoInferenceResult{}, errors.New(`local provider stream error: {"type":"error","error":{"type":"service_unavailable_error","code":"server_is_overloaded","message":"Our servers are currently overloaded. Please try again later.","param":null},"sequence_number":2}`)
		}
		return neoInferenceResult{Text: "continued"}, nil
	}
	actor := newNeoActor(rt, "actor-overload-retry", "thread-actor", "T-overload-retry", "T-overload-retry", neoActorRecord("actor-overload-retry", "thread-actor", "T-overload-retry"), nil)

	text, err := actor.executeSubagentRun("Task", map[string]any{"prompt": "continue", "description": "continue"}, "TU-overload-retry", "M-1", actor.generation, 0, "")
	if err != nil || text != "continued" || attempts != 2 {
		t.Fatalf("overload retry result text=%q err=%v attempts=%d", text, err, attempts)
	}
}

func TestNeoSubagentRetryableInferenceErrorRecognizesOverloadShapes(t *testing.T) {
	for _, payload := range []string{
		`{"type":"error","error":{"type":"service_unavailable_error","code":"server_is_overloaded","message":"Our servers are currently overloaded. Please try again later."}}`,
		`{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}`,
		`{"type":"error","error":{"type":"server_error","message":"Internal server error"}}`,
	} {
		providerErr := errors.New("local provider stream error: " + payload)
		if !neoSubagentRetryableInferenceError(providerErr) {
			t.Fatalf("overload payload was not retryable: %s", payload)
		}
		if !neoSubagentRetryableInferenceError(fmt.Errorf("subagent inference failed: %w", providerErr)) {
			t.Fatalf("wrapped overload payload was not retryable: %s", payload)
		}
	}
	if !neoSubagentRetryableInferenceError(fmt.Errorf("stream failed: %w", io.EOF)) {
		t.Fatal("typed provider EOF was not retryable")
	}
	for _, status := range []int{http.StatusRequestTimeout, http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusServiceUnavailable} {
		if !neoSubagentRetryableInferenceError(&neoLocalProviderStatusError{StatusCode: status}) {
			t.Fatalf("provider status %d was not retryable", status)
		}
	}
	for _, err := range []error{
		&neoLocalProviderStatusError{StatusCode: http.StatusBadRequest},
		errors.New("local provider stream error: EOF"),
		errors.New(`local provider stream error: {"type":"invalid_request_error","message":"connection reset"}`),
		errors.New(`local provider stream error: {"type":"invalid_request_error","message":"unexpected EOF"}`),
		errors.New(`local provider stream error: {"type":"invalid_request_error","message":"stream error: INTERNAL_ERROR"}`),
	} {
		if neoSubagentRetryableInferenceError(err) {
			t.Fatalf("deterministic error was retryable: %v", err)
		}
	}
}

func TestNeoSubagentDoesNotRetryDeterministicInferenceError(t *testing.T) {
	cfg := &config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{SubagentModels: map[string][]string{
		"Task": {"openai/model-a", "anthropic/model-b"},
	}}}}
	rt := newNeoRuntime(cfg)
	attempts := 0
	wantErr := errors.New("invalid tool schema")
	rt.inferStream = func(_ *neoRuntime, _ neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		attempts++
		return neoInferenceResult{}, wantErr
	}
	actor := newNeoActor(rt, "actor-no-retry", "thread-actor", "T-no-retry", "T-no-retry", neoActorRecord("actor-no-retry", "thread-actor", "T-no-retry"), nil)

	_, err := actor.executeSubagentRun("Task", map[string]any{"prompt": "continue", "description": "continue"}, "TU-no-retry", "M-1", actor.generation, 0, "")
	if !errors.Is(err, wantErr) || attempts != 1 {
		t.Fatalf("deterministic retry result err=%v attempts=%d", err, attempts)
	}
}

func TestNeoSubagentDoesNotRetryDeterministicStreamErrorMentioningRetryToken(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	attempts := 0
	wantErr := errors.New(`local provider stream error: {"type":"error","code":"invalid_request_error","message":"the input mentions internal_server_error as an example"}`)
	rt.inferStream = func(_ *neoRuntime, _ neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		attempts++
		return neoInferenceResult{}, wantErr
	}
	actor := newNeoActor(rt, "actor-no-token-retry", "thread-actor", "T-no-token-retry", "T-no-token-retry", neoActorRecord("actor-no-token-retry", "thread-actor", "T-no-token-retry"), nil)

	_, err := actor.executeSubagentRun("Task", map[string]any{"prompt": "continue", "description": "continue"}, "TU-no-token-retry", "M-1", actor.generation, 0, "")
	if !errors.Is(err, wantErr) || attempts != 1 {
		t.Fatalf("deterministic token retry result err=%v attempts=%d", err, attempts)
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

func TestNeoSubagentEmptyResponseUsesForcedSynthesis(t *testing.T) {
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
		return neoInferenceResult{Text: "done"}, nil
	}

	text, err := actor.executeSubagentRun("finder", map[string]any{"query": "find local actor routing"}, "TU-finder", "M-1", actor.generation, 0, "")
	if err != nil {
		t.Fatalf("subagent forced synthesis error = %v", err)
	}
	if text != "done" || calls != 2 {
		t.Fatalf("subagent forced synthesis result = text:%q calls:%d", text, calls)
	}
}

func TestNeoSubagentForcedSynthesisRetriesTransientLocalProviderStreamError(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-finder-synthesis-retry", "thread-actor", "T-finder-synthesis-retry", "T-finder-synthesis-retry", neoActorRecord("actor-finder-synthesis-retry", "thread-actor", "T-finder-synthesis-retry"), nil)
	setNeoFinderWorkspaceForTest(t, actor)
	calls := 0
	rt.inferStream = func(_ *neoRuntime, _ neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		calls++
		switch calls {
		case 1:
			return neoInferenceResult{}, nil
		case 2:
			return neoInferenceResult{}, errors.New(`local provider stream error: {"type": "error", "code": "internal_server_error", "message": "stream error: stream ID 975; INTERNAL_ERROR; received from peer"}`)
		default:
			return neoInferenceResult{Text: "continued"}, nil
		}
	}

	text, err := actor.executeSubagentRun("finder", map[string]any{"query": "find local actor routing"}, "TU-finder-synthesis-retry", "M-1", actor.generation, 0, "")
	if err != nil || text != "continued" || calls != 3 {
		t.Fatalf("forced synthesis retry result text=%q err=%v calls=%d", text, err, calls)
	}
}

func TestNeoSubagentPreflightFailureUsesLargerFallbackRoute(t *testing.T) {
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{SubagentModels: map[string][]string{
		"Task": {"openai/gpt-5.6-sol", "anthropic/claude-sonnet-4-5-20250929"},
	}}}})
	primaryMaxInput := neoEffectiveMaxInputTokens("high", "gpt-5.6-sol")
	if primaryMaxInput <= 0 {
		t.Fatalf("primary max input = %d", primaryMaxInput)
	}
	fallbackMaxInput := neoEffectiveMaxInputTokens("high", "claude-sonnet-4-5-20250929")
	if fallbackMaxInput <= primaryMaxInput {
		t.Fatalf("fallback max input = %d, want larger than primary %d", fallbackMaxInput, primaryMaxInput)
	}
	attempts := make([]string, 0, 2)
	rt.inferStream = func(_ *neoRuntime, request neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		attempts = append(attempts, request.ModelRouteOverride.Model)
		return neoInferenceResult{Text: "fallback answer"}, nil
	}
	actor := newNeoActor(rt, "actor-preflight-fallback", "thread-actor", "T-preflight-fallback", "T-preflight-fallback", neoActorRecord("actor-preflight-fallback", "thread-actor", "T-preflight-fallback"), nil)
	actor.currentAgentMode = "high"

	oversizedPrompt := strings.Repeat("x", (primaryMaxInput+4096)*neoCompactionApproxCharsPerToken)
	text, err := actor.executeSubagentRun("Task", map[string]any{"prompt": oversizedPrompt, "description": "oversized"}, "TU-preflight-fallback", "M-1", actor.generation, 0, "")
	if err != nil || text != "fallback answer" {
		t.Fatalf("preflight fallback text=%q err=%v", text, err)
	}
	if !slices.Equal(attempts, []string{"claude-sonnet-4-5-20250929"}) {
		t.Fatalf("inference attempts = %#v, want a single turn on the larger fallback route", attempts)
	}
}

func TestNeoSubagentCompactionInvariantErrorsAreNotRouteFallbackEligible(t *testing.T) {
	cases := []struct {
		name   string
		suffix []neoHistoryMessage
	}{
		{name: "normal turn"},
		{name: "forced synthesis", suffix: []neoHistoryMessage{{Role: "user", Text: "write the final answer without tools"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rt := newNeoRuntime(&config.Config{})
			actor := newNeoActor(rt, "actor-invalid-compaction", "thread-actor", "T-invalid-compaction", "T-invalid-compaction", neoActorRecord("actor-invalid-compaction", "thread-actor", "T-invalid-compaction"), nil)
			settings := map[string]any{"internal.compactionThresholdPercent": 0}
			route := neoModelRoute{Provider: "openai", Model: "gpt-5.6-sol"}
			conversation := []neoHistoryMessage{
				{Role: "user", Text: "immutable task"},
				{Role: "assistant", Text: "invalid summary slot"},
			}
			state := neoSubagentCompactionState{immutablePrefixLen: 1, summaryPresent: true}
			buildRequest := func(history []neoHistoryMessage) neoInferenceRequest {
				routeCopy := route
				return neoInferenceRequest{Context: context.Background(), ThreadID: actor.threadID, AgentMode: "high", Settings: settings, History: history, ModelRouteOverride: &routeCopy, SystemPromptOverride: "bounded worker"}
			}

			_, _, err := state.prepare(context.Background(), actor, actor.generation, "Task", "high", settings, route, conversation, tc.suffix, buildRequest)
			if err == nil || !strings.Contains(err.Error(), "compaction history is invalid") {
				t.Fatalf("prepare error = %v, want invalid compaction history", err)
			}
			if neoSubagentRouteFallbackEligible(err) {
				t.Fatalf("invariant error was classified as route-capacity fallback: %v", err)
			}
		})
	}
}

func TestNeoSubagentForcedSynthesisUsesModelFallback(t *testing.T) {
	cfg := &config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{SubagentModels: map[string][]string{
		"Task": {"openai/model-a", "anthropic/model-b"},
	}}}}
	rt := newNeoRuntime(cfg)
	attempts := make([]string, 0, 3)
	transientErr := errors.New(`local provider stream error: {"type":"error","error":{"type":"overloaded_error","message":"primary synthesis unavailable"}}`)
	rt.inferStream = func(_ *neoRuntime, request neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		attempts = append(attempts, request.ModelRouteOverride.Model)
		switch len(attempts) {
		case 1:
			return neoInferenceResult{}, nil
		case 2, 3:
			return neoInferenceResult{}, transientErr
		default:
			return neoInferenceResult{Text: "fallback synthesis"}, nil
		}
	}
	actor := newNeoActor(rt, "actor-synthesis-fallback", "thread-actor", "T-synthesis-fallback", "T-synthesis-fallback", neoActorRecord("actor-synthesis-fallback", "thread-actor", "T-synthesis-fallback"), nil)

	text, err := actor.executeSubagentRun("Task", map[string]any{"prompt": "continue", "description": "continue"}, "TU-synthesis-fallback", "M-1", actor.generation, 0, "")
	if err != nil || text != "fallback synthesis" || !slices.Equal(attempts, []string{"model-a", "model-a", "model-a", "model-b"}) {
		t.Fatalf("forced synthesis fallback text=%q err=%v attempts=%#v", text, err, attempts)
	}
}

func TestNeoSubagentForcedSynthesisPreflightFailureUsesLargerFallbackRoute(t *testing.T) {
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{SubagentModels: map[string][]string{
		"Task": {"openai/gpt-5.6-sol", "anthropic/claude-sonnet-4-5-20250929"},
	}}}})
	primaryMaxInput := neoEffectiveMaxInputTokens("high", "gpt-5.6-sol")
	fallbackMaxInput := neoEffectiveMaxInputTokens("high", "claude-sonnet-4-5-20250929")
	if primaryMaxInput <= 0 || fallbackMaxInput <= primaryMaxInput {
		t.Fatalf("route limits primary=%d fallback=%d, want a larger fallback", primaryMaxInput, fallbackMaxInput)
	}
	attempts := make([]string, 0, 8)
	rt.inferStream = func(_ *neoRuntime, request neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		attempts = append(attempts, request.ModelRouteOverride.Model)
		if len(request.Tools) == 0 {
			return neoInferenceResult{Text: "fallback synthesis"}, nil
		}
		return neoInferenceResult{ToolCalls: []neoToolCall{{ID: "leaf-preflight", Name: "Read", Input: map[string]any{"path": "file.go"}}}}, nil
	}
	actor := newNeoActor(rt, "actor-synthesis-preflight-fallback", "thread-actor", "T-synthesis-preflight-fallback", "T-synthesis-preflight-fallback", neoActorRecord("actor-synthesis-preflight-fallback", "thread-actor", "T-synthesis-preflight-fallback"), nil)
	actor.currentAgentMode = "high"
	actor.tools = map[string]neoToolSpec{
		"Read": {Name: "Read", Description: "Read a file", InputSchema: map[string]any{"type": "object"}},
	}
	socket := &neoSocket{writeMessage: func(_ int, data []byte) error {
		var event map[string]any
		if err := json.Unmarshal(data, &event); err != nil || stringValue(event["type"]) != "tool_lease" {
			return nil
		}
		leasedID := stringValue(event["toolCallId"])
		if leasedID == "" {
			return nil
		}
		go actor.routeSubagentLeafToolResult(leasedID, map[string]any{"status": "done", "output": "package amp"})
		return nil
	}}
	actor.sockets[socket] = struct{}{}

	oversizedPrompt := strings.Repeat("x", (primaryMaxInput+1024)*neoCompactionApproxCharsPerToken)
	text, err := actor.executeSubagentRun("Task", map[string]any{"prompt": oversizedPrompt, "description": "oversized"}, "TU-synthesis-preflight-fallback", "M-1", actor.generation, 0, "")
	if err != nil || text != "fallback synthesis" {
		t.Fatalf("synthesis preflight fallback text=%q err=%v", text, err)
	}
	if len(attempts) < 2 || attempts[len(attempts)-1] != "claude-sonnet-4-5-20250929" || slices.ContainsFunc(attempts, func(model string) bool { return model != "claude-sonnet-4-5-20250929" }) {
		t.Fatalf("inference attempts = %#v, want only the fallback route, ending in a synthesis inference", attempts)
	}
}

func TestNeoSubagentForcedSynthesisPropagatesProviderStopReason(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-finder-stop", "thread-actor", "T-finder-stop", "T-finder-stop", neoActorRecord("actor-finder-stop", "thread-actor", "T-finder-stop"), nil)
	setNeoFinderWorkspaceForTest(t, actor)
	calls := 0
	rt.inferStream = func(_ *neoRuntime, _ neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		calls++
		if calls == 1 {
			return neoInferenceResult{}, nil
		}
		return neoInferenceResult{Text: "partial", StopReason: "max_tokens"}, nil
	}

	text, err := actor.executeSubagentRun("finder", map[string]any{"query": "find local actor routing"}, "TU-finder-stop", "M-1", actor.generation, 0, "")
	if err == nil || !strings.Contains(err.Error(), "max_tokens") {
		t.Fatalf("forced synthesis error = %v, want max_tokens", err)
	}
	if text != "" || calls != 2 {
		t.Fatalf("forced synthesis result = text:%q calls:%d, want empty text after two calls", text, calls)
	}
}

func TestNeoSubagentStopReasonRequiresCompletedToolUse(t *testing.T) {
	for name, test := range map[string]struct {
		result       neoInferenceResult
		allowToolUse bool
		wantError    bool
	}{
		"end turn":              {result: neoInferenceResult{StopReason: "end_turn"}},
		"completed tool use":    {result: neoInferenceResult{StopReason: "tool_use", ToolCalls: []neoToolCall{{ID: "TU-test", Name: "Read"}}}, allowToolUse: true},
		"truncated with tool":   {result: neoInferenceResult{StopReason: "max_tokens", ToolCalls: []neoToolCall{{ID: "TU-test", Name: "Read"}}}, allowToolUse: true, wantError: true},
		"empty tool use":        {result: neoInferenceResult{StopReason: "tool_use"}, allowToolUse: true, wantError: true},
		"forced synthesis tool": {result: neoInferenceResult{StopReason: "tool_use", ToolCalls: []neoToolCall{{ID: "TU-test", Name: "Read"}}}, wantError: true},
	} {
		t.Run(name, func(t *testing.T) {
			err := neoSubagentStopReasonError(test.result, test.allowToolUse)
			if (err != nil) != test.wantError {
				t.Fatalf("stop reason error = %v, wantError %v", err, test.wantError)
			}
		})
	}
}

func TestNeoSubagentTurnLimitsMatchAmpClassicRegistry(t *testing.T) {
	for name, maxTurns := range map[string]int{"finder": 6, "oracle": 24, "librarian": 16, "Task": 30} {
		def, ok := neoSubagentDefFor(name)
		if !ok {
			t.Fatalf("subagent %q missing", name)
		}
		if def.MaxTurns != maxTurns {
			t.Fatalf("%s max turns = %d, want %d", name, def.MaxTurns, maxTurns)
		}
	}
	def, ok := neoSubagentDefFor("run_check")
	if !ok || def.MaxTurns != 72 {
		t.Fatalf("run_check max turns = %d, want Amp codereview-check limit 72", def.MaxTurns)
	}
}

func TestNeoTaskSubagentCompactsLargeToolHistory(t *testing.T) {
	summaryCalls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		summaryCalls++
		if r.URL.Path != "/api/provider/openai/v1/chat/completions" {
			t.Fatalf("compaction path = %q", r.URL.Path)
		}
		payload := readNeoJSON(r.Body)
		if !strings.Contains(fmt.Sprint(payload["messages"]), "large tool result") {
			t.Fatalf("compaction transcript omitted tool output: %#v", payload["messages"])
		}
		if strings.Contains(fmt.Sprint(payload["messages"]), "TU-incomplete-read") {
			t.Fatalf("compaction transcript retained an incomplete tool call: %#v", payload["messages"])
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"<summary>Read completed and the relevant runtime evidence was collected.</summary>"}}]}`))
	}))
	t.Cleanup(upstream.Close)

	rt := testNeoRuntimeForServer(t, upstream)
	actor := newNeoActor(rt, "actor-task-compaction", "thread-actor", "T-task-compaction", "T-task-compaction", neoActorRecord("actor-task-compaction", "thread-actor", "T-task-compaction"), nil)
	actor.currentAgentMode = "high"
	actor.settings["internal.compactionThresholdPercent"] = 0
	actor.tools = map[string]neoToolSpec{
		"Read": {Name: "Read", Description: "Read a file", InputSchema: map[string]any{"type": "object"}},
	}
	requests := make([]neoInferenceRequest, 0, 3)
	rt.inferStream = func(_ *neoRuntime, request neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		requests = append(requests, request)
		switch len(requests) {
		case 1:
			return neoInferenceResult{ToolCalls: []neoToolCall{
				{ID: "TU-large-read", Name: "Read", Input: map[string]any{"path": "neo_runtime.go"}},
				{ID: "TU-incomplete-read", Name: "Read", Incomplete: true},
			}}, nil
		case 2:
			return neoInferenceResult{ToolCalls: []neoToolCall{{ID: "TU-after-compaction", Name: "Read", Input: map[string]any{"path": "neo_subagents.go"}}}}, nil
		default:
			return neoInferenceResult{Text: "task completed"}, nil
		}
	}
	socket := &neoSocket{writeMessage: func(_ int, data []byte) error {
		var event map[string]any
		if err := json.Unmarshal(data, &event); err != nil || stringValue(event["type"]) != "tool_lease" {
			return nil
		}
		toolCallID := stringValue(event["toolCallId"])
		output := "follow-up result after compaction"
		if stringValue(mapValue(event["args"])["path"]) == "neo_runtime.go" {
			output = "large tool result\n" + strings.Repeat("runtime evidence ", 12000)
		}
		go actor.routeSubagentLeafToolResult(toolCallID, map[string]any{"status": "done", "output": output})
		return nil
	}}
	actor.sockets[socket] = struct{}{}

	text, err := actor.executeSubagentRun("Task", map[string]any{"prompt": "inspect the runtime", "description": "inspect"}, "TU-task-compaction", "M-1", actor.generation, 0, "")
	if err != nil {
		t.Fatalf("Task compaction run failed: %v", err)
	}
	if text != "task completed" || len(requests) != 3 || summaryCalls != 1 {
		t.Fatalf("Task compaction result=%q requests=%d summaries=%d", text, len(requests), summaryCalls)
	}
	if len(requests[1].History) != 2 || requests[1].History[0].Role != "user" || requests[1].History[1].Role != "user" || !strings.Contains(requests[1].History[1].Text, "runtime evidence was collected") {
		t.Fatalf("compacted Task history = %#v", requests[1].History)
	}
	if len(requests[2].History) != 4 || requests[2].History[2].Role != "assistant" || requests[2].History[3].Role != "tool" || requests[2].History[3].ToolCallID != "TU-after-compaction" || requests[2].History[3].Text != "follow-up result after compaction" {
		t.Fatalf("post-compaction Task history = %#v", requests[2].History)
	}
	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.compactionRecords) != 0 || actor.compacting {
		t.Fatalf("sub-agent compaction leaked into parent state: records=%#v compacting=%v", actor.compactionRecords, actor.compacting)
	}
}

func TestNeoSubagentCompactionPreservesImmutablePrefixAndCompleteTail(t *testing.T) {
	summaryCalls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		summaryCalls++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"<summary>Older checks completed; continue with the retained exchanges.</summary>"}}]}`))
	}))
	t.Cleanup(upstream.Close)

	rt := testNeoRuntimeForServer(t, upstream)
	actor := newNeoActor(rt, "actor-prefix-compaction", "thread-actor", "T-prefix-compaction", "T-prefix-compaction", neoActorRecord("actor-prefix-compaction", "thread-actor", "T-prefix-compaction"), nil)
	settings := map[string]any{"internal.compactionThresholdPercent": 0}
	route := neoModelRoute{Provider: "openai", Model: "gpt-5.6-sol"}
	initial := []neoHistoryMessage{
		{Role: "user", Text: "immutable review snapshot"},
		{Role: "user", Text: "review request with attachment", Content: []any{
			map[string]any{"type": "text", "text": "review request with attachment"},
			map[string]any{"type": "image", "name": "review.png", "source": map[string]any{"type": "base64", "media_type": "image/png", "data": "AA=="}},
		}},
	}
	conversation := append([]neoHistoryMessage(nil), initial...)
	for index := 0; index < 3; index++ {
		callID := fmt.Sprintf("TU-check-%d", index)
		conversation = append(conversation,
			neoHistoryMessage{Role: "assistant", Text: fmt.Sprintf("checking %d", index), ToolCalls: []neoToolCall{{ID: callID, Name: "Read"}}},
			neoHistoryMessage{Role: "tool", ToolCallID: callID, ToolName: "Read", Text: fmt.Sprintf("result %d", index)},
		)
	}
	state := neoSubagentCompactionState{immutablePrefixLen: len(initial)}
	forcedSuffix := []neoHistoryMessage{{Role: "user", Text: "write the final answer without tools"}}
	buildRequest := func(history []neoHistoryMessage) neoInferenceRequest {
		routeCopy := route
		return neoInferenceRequest{Context: context.Background(), ThreadID: actor.threadID, AgentMode: "high", Settings: settings, History: history, ModelRouteOverride: &routeCopy, SystemPromptOverride: "bounded worker"}
	}

	rebuilt, request, err := state.prepare(context.Background(), actor, actor.generation, "run_check", "high", settings, route, conversation, forcedSuffix, buildRequest)
	if err != nil {
		t.Fatalf("prepare sub-agent compaction: %v", err)
	}
	if summaryCalls != 1 || !state.summaryPresent || len(rebuilt) >= len(conversation) {
		t.Fatalf("compaction state calls=%d state=%#v before=%d after=%d", summaryCalls, state, len(conversation), len(rebuilt))
	}
	if !reflect.DeepEqual(rebuilt[:len(initial)], initial) {
		t.Fatalf("immutable prefix changed:\n got %#v\nwant %#v", rebuilt[:len(initial)], initial)
	}
	if rebuilt[len(initial)].Role != "user" || !strings.Contains(rebuilt[len(initial)].Text, "Older checks completed") {
		t.Fatalf("continuation summary = %#v", rebuilt[len(initial)])
	}
	if _, err := neoSubagentCompleteExchangeRanges(rebuilt, len(initial)+1); err != nil {
		t.Fatalf("retained tail split an exchange: %v", err)
	}
	if len(rebuilt) < len(initial)+3 || rebuilt[len(rebuilt)-2].Role != "assistant" || rebuilt[len(rebuilt)-2].ToolCalls[0].ID != "TU-check-2" || rebuilt[len(rebuilt)-1].Role != "tool" || rebuilt[len(rebuilt)-1].ToolCallID != "TU-check-2" {
		t.Fatalf("newest complete exchange was not retained: %#v", rebuilt[len(initial)+1:])
	}
	if len(request.History) != len(rebuilt)+1 || request.History[len(request.History)-1].Text != forcedSuffix[0].Text || len(rebuilt) > 0 && rebuilt[len(rebuilt)-1].Text == forcedSuffix[0].Text {
		t.Fatalf("forced synthesis suffix was not request-local: rebuilt=%#v request=%#v", rebuilt, request.History)
	}
	if len(actor.messages) != 0 || len(actor.compactionRecords) != 0 || actor.compacting {
		t.Fatalf("run-local compaction mutated actor state")
	}
}

func TestNeoSubagentCompactionReplacesPreviousSummary(t *testing.T) {
	summaryCalls := 0
	secondTranscript := ""
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		summaryCalls++
		payload := readNeoJSON(r.Body)
		if summaryCalls == 2 {
			secondTranscript = fmt.Sprint(payload["messages"])
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"choices":[{"message":{"content":"<summary>continuation summary %d</summary>"}}]}`, summaryCalls)
	}))
	t.Cleanup(upstream.Close)

	rt := testNeoRuntimeForServer(t, upstream)
	actor := newNeoActor(rt, "actor-repeat-compaction", "thread-actor", "T-repeat-compaction", "T-repeat-compaction", neoActorRecord("actor-repeat-compaction", "thread-actor", "T-repeat-compaction"), nil)
	settings := map[string]any{"internal.compactionThresholdPercent": 0}
	route := neoModelRoute{Provider: "openai", Model: "gpt-5.6-sol"}
	state := neoSubagentCompactionState{immutablePrefixLen: 1}
	buildRequest := func(history []neoHistoryMessage) neoInferenceRequest {
		routeCopy := route
		return neoInferenceRequest{Context: context.Background(), ThreadID: actor.threadID, AgentMode: "high", Settings: settings, History: history, ModelRouteOverride: &routeCopy, SystemPromptOverride: "bounded worker"}
	}
	appendExchange := func(history []neoHistoryMessage, index int) []neoHistoryMessage {
		callID := fmt.Sprintf("TU-repeat-%d", index)
		return append(history,
			neoHistoryMessage{Role: "assistant", Text: fmt.Sprintf("checking %d", index), ToolCalls: []neoToolCall{{ID: callID, Name: "Read"}}},
			neoHistoryMessage{Role: "tool", ToolCallID: callID, ToolName: "Read", Text: fmt.Sprintf("result %d", index)},
		)
	}
	conversation := []neoHistoryMessage{{Role: "user", Text: "immutable repeated-compaction task"}}
	for index := 0; index < 3; index++ {
		conversation = appendExchange(conversation, index)
	}

	first, _, err := state.prepare(context.Background(), actor, actor.generation, "Task", "high", settings, route, conversation, nil, buildRequest)
	if err != nil {
		t.Fatalf("first compaction: %v", err)
	}
	for index := 3; index < 7; index++ {
		first = appendExchange(first, index)
	}
	second, secondRequest, err := state.prepare(context.Background(), actor, actor.generation, "Task", "high", settings, route, first, nil, buildRequest)
	if err != nil {
		t.Fatalf("second compaction: %v", err)
	}
	if summaryCalls != 2 || !strings.Contains(secondTranscript, "continuation summary 1") {
		t.Fatalf("repeated compaction calls=%d second transcript=%q", summaryCalls, secondTranscript)
	}
	if second[1].Role != "user" || second[1].Text != "continuation summary 2" {
		t.Fatalf("replacement summary = %#v", second[1])
	}
	for index, message := range second[2:] {
		if message.Role == "user" || strings.Contains(message.Text, "continuation summary 1") {
			t.Fatalf("previous summary survived at index %d: %#v", index+2, message)
		}
	}
	if _, err := neoSubagentCompleteExchangeRanges(second, 2); err != nil {
		t.Fatalf("second compaction split an exchange: %v", err)
	}
	if !reflect.DeepEqual(secondRequest.History, second) {
		t.Fatalf("second compaction request history diverged from rebuilt history:\n request %#v\n rebuilt %#v", secondRequest.History, second)
	}
}

func TestNeoSubagentCompactionFailureUsesOneLossyFallback(t *testing.T) {
	summaryCalls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		summaryCalls++
		http.Error(w, "compaction unavailable", http.StatusServiceUnavailable)
	}))
	t.Cleanup(upstream.Close)

	rt := testNeoRuntimeForServer(t, upstream)
	actor := newNeoActor(rt, "actor-lossy-compaction", "thread-actor", "T-lossy-compaction", "T-lossy-compaction", neoActorRecord("actor-lossy-compaction", "thread-actor", "T-lossy-compaction"), nil)
	settings := map[string]any{"internal.compactionThresholdPercent": 0}
	route := neoModelRoute{Provider: "openai", Model: "gpt-5.6-sol"}
	conversation := []neoHistoryMessage{{Role: "user", Text: "immutable task"}}
	for index := 0; index < 3; index++ {
		callID := fmt.Sprintf("TU-lossy-%d", index)
		conversation = append(conversation,
			neoHistoryMessage{Role: "assistant", ToolCalls: []neoToolCall{{ID: callID, Name: "Read"}}},
			neoHistoryMessage{Role: "tool", ToolCallID: callID, ToolName: "Read", Text: strings.Repeat("tool output ", 1000)},
		)
	}
	state := neoSubagentCompactionState{immutablePrefixLen: 1}
	buildRequest := func(history []neoHistoryMessage) neoInferenceRequest {
		routeCopy := route
		return neoInferenceRequest{Context: context.Background(), ThreadID: actor.threadID, AgentMode: "high", Settings: settings, History: history, ModelRouteOverride: &routeCopy, SystemPromptOverride: "bounded worker"}
	}

	rebuilt, lossyRequest, err := state.prepare(context.Background(), actor, actor.generation, "Task", "high", settings, route, conversation, nil, buildRequest)
	if err != nil {
		t.Fatalf("lossy fallback failed: %v", err)
	}
	if summaryCalls != 1 || !strings.Contains(rebuilt[1].Text, "exchanges were elided") || state.retryAfterLen <= len(rebuilt) {
		t.Fatalf("lossy fallback calls=%d state=%#v history=%#v", summaryCalls, state, rebuilt)
	}
	if !reflect.DeepEqual(lossyRequest.History, rebuilt) {
		t.Fatalf("lossy fallback request history diverged from rebuilt history:\n request %#v\n rebuilt %#v", lossyRequest.History, rebuilt)
	}
	if _, err := neoSubagentCompleteExchangeRanges(rebuilt, 2); err != nil {
		t.Fatalf("lossy fallback split an exchange: %v", err)
	}
	again, _, err := state.prepare(context.Background(), actor, actor.generation, "Task", "high", settings, route, rebuilt, nil, buildRequest)
	if err != nil {
		t.Fatalf("unchanged lossy history failed: %v", err)
	}
	if summaryCalls != 1 || !reflect.DeepEqual(again, rebuilt) {
		t.Fatalf("unchanged lossy history retried compaction: calls=%d history=%#v", summaryCalls, again)
	}
}

func TestNeoSubagentCompactionCancellationDoesNotInstallSummary(t *testing.T) {
	requests := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"<summary>must not install</summary>"}}]}`))
	}))
	t.Cleanup(upstream.Close)

	rt := testNeoRuntimeForServer(t, upstream)
	actor := newNeoActor(rt, "actor-cancel-compaction", "thread-actor", "T-cancel-compaction", "T-cancel-compaction", neoActorRecord("actor-cancel-compaction", "thread-actor", "T-cancel-compaction"), nil)
	settings := map[string]any{"internal.compactionThresholdPercent": 0}
	route := neoModelRoute{Provider: "openai", Model: "gpt-5.6-sol"}
	conversation := []neoHistoryMessage{
		{Role: "user", Text: "immutable task"},
		{Role: "assistant", ToolCalls: []neoToolCall{{ID: "TU-cancel", Name: "Read"}}},
		{Role: "tool", ToolCallID: "TU-cancel", ToolName: "Read", Text: "result"},
	}
	state := neoSubagentCompactionState{immutablePrefixLen: 1}
	buildRequest := func(history []neoHistoryMessage) neoInferenceRequest {
		routeCopy := route
		return neoInferenceRequest{AgentMode: "high", Settings: settings, History: history, ModelRouteOverride: &routeCopy, SystemPromptOverride: "bounded worker"}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	rebuilt, _, err := state.prepare(ctx, actor, actor.generation, "Task", "high", settings, route, conversation, nil, buildRequest)
	if !errors.Is(err, context.Canceled) || requests != 0 || state.summaryPresent || !reflect.DeepEqual(rebuilt, conversation) {
		t.Fatalf("cancelled compaction err=%v requests=%d state=%#v history=%#v", err, requests, state, rebuilt)
	}
}

func TestNeoSubagentCompactionInflightCancellationDoesNotInstallSummary(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(entered)
		<-release
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"<summary>must not install</summary>"}}]}`))
	}))
	t.Cleanup(upstream.Close)

	rt := testNeoRuntimeForServer(t, upstream)
	actor := newNeoActor(rt, "actor-inflight-cancel", "thread-actor", "T-inflight-cancel", "T-inflight-cancel", neoActorRecord("actor-inflight-cancel", "thread-actor", "T-inflight-cancel"), nil)
	settings := map[string]any{"internal.compactionThresholdPercent": 0}
	route := neoModelRoute{Provider: "openai", Model: "gpt-5.6-sol"}
	conversation := []neoHistoryMessage{
		{Role: "user", Text: "immutable task"},
		{Role: "assistant", ToolCalls: []neoToolCall{{ID: "TU-inflight", Name: "Read"}}},
		{Role: "tool", ToolCallID: "TU-inflight", ToolName: "Read", Text: "result"},
	}
	state := neoSubagentCompactionState{immutablePrefixLen: 1}
	buildRequest := func(history []neoHistoryMessage) neoInferenceRequest {
		routeCopy := route
		return neoInferenceRequest{AgentMode: "high", Settings: settings, History: history, ModelRouteOverride: &routeCopy, SystemPromptOverride: "bounded worker"}
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	type outcome struct {
		rebuilt []neoHistoryMessage
		err     error
	}
	resultCh := make(chan outcome, 1)
	go func() {
		rebuilt, _, err := state.prepare(ctx, actor, actor.generation, "Task", "high", settings, route, conversation, nil, buildRequest)
		resultCh <- outcome{rebuilt: rebuilt, err: err}
	}()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("summary inference did not reach the upstream")
	}
	cancel()
	close(release)
	result := <-resultCh
	if !errors.Is(result.err, context.Canceled) || state.summaryPresent || !reflect.DeepEqual(result.rebuilt, conversation) {
		t.Fatalf("in-flight cancelled compaction err=%v state=%#v history_changed=%v", result.err, state, !reflect.DeepEqual(result.rebuilt, conversation))
	}
}

func TestNeoSubagentCompactionDefaultThresholdKeepsFittingOriginalOnOversizedSummary(t *testing.T) {
	summaryCalls := 0
	route := neoModelRoute{Provider: "openai", Model: "gpt-5.6-sol"}
	maxInputTokens := neoEffectiveMaxInputTokens("high", route.Model)
	largeSummary := strings.Repeat("s", maxInputTokens*15/100*neoCompactionApproxCharsPerToken)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		summaryCalls++
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"choices":[{"message":{"content":"<summary>%s</summary>"}}]}`, largeSummary)
	}))
	t.Cleanup(upstream.Close)

	rt := testNeoRuntimeForServer(t, upstream)
	actor := newNeoActor(rt, "actor-default-threshold", "thread-actor", "T-default-threshold", "T-default-threshold", neoActorRecord("actor-default-threshold", "thread-actor", "T-default-threshold"), nil)
	settings := map[string]any{}
	conversation := []neoHistoryMessage{
		{Role: "user", Text: strings.Repeat("i", maxInputTokens*92/100*neoCompactionApproxCharsPerToken)},
		{Role: "assistant", ToolCalls: []neoToolCall{{ID: "TU-threshold", Name: "Read"}}},
		{Role: "tool", ToolCallID: "TU-threshold", ToolName: "Read", Text: "small result"},
	}
	state := neoSubagentCompactionState{immutablePrefixLen: 1}
	buildRequest := func(history []neoHistoryMessage) neoInferenceRequest {
		routeCopy := route
		return neoInferenceRequest{Context: context.Background(), ThreadID: actor.threadID, AgentMode: "high", Settings: settings, History: history, ModelRouteOverride: &routeCopy, SystemPromptOverride: "bounded worker"}
	}
	_, originalPressure, _, err := neoSubagentRequestPressureForConversation(rt, route, conversation, nil, buildRequest)
	if err != nil || !originalPressure.shouldCompact("high", settings) || !originalPressure.fitsHardLimit() {
		t.Fatalf("default-threshold pressure = %#v err=%v", originalPressure, err)
	}

	rebuilt, _, err := state.prepare(context.Background(), actor, actor.generation, "Task", "high", settings, route, conversation, nil, buildRequest)
	if err != nil {
		t.Fatalf("default-threshold compaction: %v", err)
	}
	if summaryCalls != 1 || !reflect.DeepEqual(rebuilt, conversation) || state.retryAfterLen <= len(conversation) {
		t.Fatalf("oversized summary should keep fitting original: calls=%d state=%#v before=%d after=%d", summaryCalls, state, len(conversation), len(rebuilt))
	}
}

func TestNeoSubagentRequestPressureUsesFallbackAndTextBridgeWireForms(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	gptRoute := neoModelRoute{Provider: "openai", Model: "gpt-5.6-sol"}
	fableRoute := neoModelRoute{Provider: "anthropic", Model: "claude-fable-5"}
	fableMaxInput := neoEffectiveMaxInputTokens("high", fableRoute.Model)
	conversation := []neoHistoryMessage{{Role: "user", Text: strings.Repeat("f", fableMaxInput*94/100*neoCompactionApproxCharsPerToken)}}
	buildPlainRequest := func(history []neoHistoryMessage) neoInferenceRequest {
		return neoInferenceRequest{AgentMode: "high", History: history, SystemPromptOverride: "bounded worker"}
	}
	_, gptPressure, _, err := neoSubagentRequestPressureForConversation(rt, gptRoute, conversation, nil, buildPlainRequest)
	if err != nil {
		t.Fatalf("primary pressure: %v", err)
	}
	_, fablePressure, _, err := neoSubagentRequestPressureForConversation(rt, fableRoute, conversation, nil, buildPlainRequest)
	if err != nil {
		t.Fatalf("fallback pressure: %v", err)
	}
	if gptPressure.shouldCompact("high", nil) || !fablePressure.shouldCompact("high", nil) {
		t.Fatalf("route-specific pressure primary=%#v fallback=%#v", gptPressure, fablePressure)
	}

	bridgeRoute := neoModelRoute{Provider: "openai", Model: "gpt-5.6-sol", TextToolBridge: true}
	tools := []neoToolSpec{{Name: "Read", Description: strings.Repeat("tool protocol pressure ", 100), InputSchema: map[string]any{"type": "object"}}}
	buildBridgeRequest := func(history []neoHistoryMessage) neoInferenceRequest {
		routeCopy := bridgeRoute
		return neoInferenceRequest{AgentMode: "high", History: history, Tools: tools, ModelRouteOverride: &routeCopy, SystemPromptOverride: "bounded worker"}
	}
	request, bridgePressure, _, err := neoSubagentRequestPressureForConversation(rt, bridgeRoute, []neoHistoryMessage{{Role: "user", Text: "inspect"}}, nil, buildBridgeRequest)
	if err != nil {
		t.Fatalf("bridge pressure: %v", err)
	}
	wireRequest, err := neoTextToolBridgeRequest(request)
	if err != nil {
		t.Fatalf("bridge wire request: %v", err)
	}
	wantTokens := neoEstimateInferenceInputTokens(wireRequest, bridgeRoute)
	plainTokens := neoEstimateInferenceInputTokens(request, bridgeRoute)
	if bridgePressure.estimatedTokens != wantTokens || bridgePressure.estimatedTokens <= plainTokens {
		t.Fatalf("bridge pressure tokens=%d want=%d plain=%d", bridgePressure.estimatedTokens, wantTokens, plainTokens)
	}
}

func TestNeoSubagentRequestPressureIncludesKimiMessageBytes(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	route := neoModelRoute{Provider: "moonshotai", Model: "kimi-k2.5"}
	conversation := []neoHistoryMessage{{Role: "user", Text: strings.Repeat("k", neoKimiCompactionMessageBytes)}}
	buildRequest := func(history []neoHistoryMessage) neoInferenceRequest {
		routeCopy := route
		return neoInferenceRequest{AgentMode: "high", History: history, ModelRouteOverride: &routeCopy, SystemPromptOverride: "bounded worker"}
	}
	_, pressure, _, err := neoSubagentRequestPressureForConversation(rt, route, conversation, nil, buildRequest)
	if err != nil {
		t.Fatalf("Kimi pressure: %v", err)
	}
	if pressure.provider != "moonshotai" || pressure.messageBytes < neoKimiCompactionMessageBytes || !pressure.shouldCompact("high", nil) {
		t.Fatalf("Kimi pressure = %#v", pressure)
	}
}

func TestNeoSubagentCompactionDropsMultipleCompleteExchanges(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	route := neoModelRoute{Provider: "openai", Model: "gpt-5.6-sol"}
	conversation := []neoHistoryMessage{{Role: "user", Text: "immutable task"}}
	for index := 0; index < 3; index++ {
		callID := fmt.Sprintf("TU-multi-drop-%d", index)
		conversation = append(conversation,
			neoHistoryMessage{Role: "assistant", ToolCalls: []neoToolCall{{ID: callID, Name: "Read"}}},
			neoHistoryMessage{Role: "tool", ToolCallID: callID, ToolName: "Read", Text: strings.Repeat(string(rune('a'+index)), 550000)},
		)
	}
	groups, err := neoSubagentCompleteExchangeRanges(conversation, 1)
	if err != nil {
		t.Fatalf("complete exchange ranges: %v", err)
	}
	buildRequest := func(history []neoHistoryMessage) neoInferenceRequest {
		return neoInferenceRequest{AgentMode: "high", History: history, SystemPromptOverride: "bounded worker"}
	}
	dropCount, err := neoSubagentCompactionDropCount(rt, "high", nil, route, conversation, nil, groups, 1, buildRequest)
	if err != nil {
		t.Fatalf("select compaction cut: %v", err)
	}
	if dropCount != 2 {
		t.Fatalf("drop count = %d, want two complete exchanges", dropCount)
	}
	rebuilt := neoSubagentRebuildConversation(conversation, groups, 1, dropCount, "summary")
	if len(rebuilt) != 4 || rebuilt[2].ToolCalls[0].ID != "TU-multi-drop-2" || rebuilt[3].ToolCallID != "TU-multi-drop-2" {
		t.Fatalf("multi-exchange cut = %#v", rebuilt)
	}
}

func TestNeoSubagentCompactionGenerationStalenessDoesNotInstallSummary(t *testing.T) {
	var actor *neoActor
	requests := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
		actor.mu.Lock()
		actor.generation++
		actor.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"<summary>stale summary</summary>"}}]}`))
	}))
	t.Cleanup(upstream.Close)

	rt := testNeoRuntimeForServer(t, upstream)
	actor = newNeoActor(rt, "actor-stale-compaction", "thread-actor", "T-stale-compaction", "T-stale-compaction", neoActorRecord("actor-stale-compaction", "thread-actor", "T-stale-compaction"), nil)
	generation := actor.generation
	settings := map[string]any{"internal.compactionThresholdPercent": 0}
	route := neoModelRoute{Provider: "openai", Model: "gpt-5.6-sol"}
	conversation := []neoHistoryMessage{
		{Role: "user", Text: "immutable task"},
		{Role: "assistant", ToolCalls: []neoToolCall{{ID: "TU-stale", Name: "Read"}}},
		{Role: "tool", ToolCallID: "TU-stale", ToolName: "Read", Text: "result"},
	}
	state := neoSubagentCompactionState{immutablePrefixLen: 1}
	buildRequest := func(history []neoHistoryMessage) neoInferenceRequest {
		routeCopy := route
		return neoInferenceRequest{AgentMode: "high", Settings: settings, History: history, ModelRouteOverride: &routeCopy, SystemPromptOverride: "bounded worker"}
	}

	rebuilt, _, err := state.prepare(context.Background(), actor, generation, "Task", "high", settings, route, conversation, nil, buildRequest)
	if !errors.Is(err, context.Canceled) || requests != 1 || state.summaryPresent || !reflect.DeepEqual(rebuilt, conversation) {
		t.Fatalf("stale compaction err=%v requests=%d state=%#v history_changed=%v", err, requests, state, !reflect.DeepEqual(rebuilt, conversation))
	}
}

func TestNeoSubagentCompactionRejectsIrreducibleImmutablePrefix(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-irreducible-compaction", "thread-actor", "T-irreducible-compaction", "T-irreducible-compaction", neoActorRecord("actor-irreducible-compaction", "thread-actor", "T-irreducible-compaction"), nil)
	route := neoModelRoute{Provider: "openai", Model: "gpt-5.6-sol"}
	maxInputTokens := neoEffectiveMaxInputTokens("high", route.Model)
	conversation := []neoHistoryMessage{{Role: "user", Text: strings.Repeat("x", (maxInputTokens+1024)*neoCompactionApproxCharsPerToken)}}
	state := neoSubagentCompactionState{immutablePrefixLen: 1}
	buildRequest := func(history []neoHistoryMessage) neoInferenceRequest {
		routeCopy := route
		return neoInferenceRequest{AgentMode: "high", History: history, ModelRouteOverride: &routeCopy, SystemPromptOverride: "bounded worker"}
	}

	rebuilt, _, err := state.prepare(context.Background(), actor, actor.generation, "Task", "high", nil, route, conversation, nil, buildRequest)
	if err == nil || !strings.Contains(err.Error(), "immutable_initial_messages=1") || !reflect.DeepEqual(rebuilt, conversation) {
		t.Fatalf("irreducible compaction err=%v history_changed=%v", err, !reflect.DeepEqual(rebuilt, conversation))
	}
}

func TestNeoSubagentCompactionDisabledStillEnforcesHardLimit(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-disabled-compaction-limit", "thread-actor", "T-disabled-compaction-limit", "T-disabled-compaction-limit", neoActorRecord("actor-disabled-compaction-limit", "thread-actor", "T-disabled-compaction-limit"), nil)
	route := neoModelRoute{Provider: "openai", Model: "gpt-5.6-sol"}
	maxInputTokens := neoEffectiveMaxInputTokens("high", route.Model)
	settings := map[string]any{"compactionControl": map[string]any{"enabled": false}}
	conversation := []neoHistoryMessage{{Role: "user", Text: strings.Repeat("x", (maxInputTokens+1024)*neoCompactionApproxCharsPerToken)}}
	state := neoSubagentCompactionState{immutablePrefixLen: 1}
	buildRequest := func(history []neoHistoryMessage) neoInferenceRequest {
		routeCopy := route
		return neoInferenceRequest{AgentMode: "high", History: history, ModelRouteOverride: &routeCopy, SystemPromptOverride: "bounded worker"}
	}

	rebuilt, _, err := state.prepare(context.Background(), actor, actor.generation, "Task", "high", settings, route, conversation, nil, buildRequest)
	if err == nil || !strings.Contains(err.Error(), "sub-agent compaction is disabled") || !strings.Contains(err.Error(), "enable compaction") || !strings.Contains(err.Error(), "immutable_initial_messages=1") || !reflect.DeepEqual(rebuilt, conversation) {
		t.Fatalf("disabled compaction over hard limit err=%v history_changed=%v", err, !reflect.DeepEqual(rebuilt, conversation))
	}

	fitting := []neoHistoryMessage{{Role: "user", Text: "immutable task"}}
	kept, request, err := state.prepare(context.Background(), actor, actor.generation, "Task", "high", settings, route, fitting, nil, buildRequest)
	if err != nil {
		t.Fatalf("disabled compaction fitting history: %v", err)
	}
	if !reflect.DeepEqual(kept, fitting) || !reflect.DeepEqual(request.History, fitting) {
		t.Fatalf("disabled compaction mutated a fitting history: kept=%#v request=%#v", kept, request.History)
	}
}

func TestNeoSubagentCompactionDisabledUsesLargerFallbackRoute(t *testing.T) {
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{SubagentModels: map[string][]string{
		"Task": {"openai/gpt-5.6-sol", "anthropic/claude-sonnet-4-5-20250929"},
	}}}})
	primaryMaxInput := neoEffectiveMaxInputTokens("high", "gpt-5.6-sol")
	fallbackMaxInput := neoEffectiveMaxInputTokens("high", "claude-sonnet-4-5-20250929")
	if primaryMaxInput <= 0 || fallbackMaxInput <= primaryMaxInput {
		t.Fatalf("route limits primary=%d fallback=%d, want a larger fallback", primaryMaxInput, fallbackMaxInput)
	}
	attempts := make([]string, 0, 2)
	rt.inferStream = func(_ *neoRuntime, request neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		attempts = append(attempts, request.ModelRouteOverride.Model)
		return neoInferenceResult{Text: "fallback answer"}, nil
	}
	actor := newNeoActor(rt, "actor-disabled-fallback", "thread-actor", "T-disabled-fallback", "T-disabled-fallback", neoActorRecord("actor-disabled-fallback", "thread-actor", "T-disabled-fallback"), nil)
	actor.currentAgentMode = "high"
	actor.settings["compactionControl"] = map[string]any{"enabled": false}

	oversizedPrompt := strings.Repeat("x", (primaryMaxInput+4096)*neoCompactionApproxCharsPerToken)
	text, err := actor.executeSubagentRun("Task", map[string]any{"prompt": oversizedPrompt, "description": "oversized"}, "TU-disabled-fallback", "M-1", actor.generation, 0, "")
	if err != nil || text != "fallback answer" {
		t.Fatalf("disabled-compaction fallback text=%q err=%v", text, err)
	}
	if !slices.Equal(attempts, []string{"claude-sonnet-4-5-20250929"}) {
		t.Fatalf("inference attempts = %#v, want a single turn on the larger fallback route", attempts)
	}
}

func TestNeoSubagentCompactionProviderOverflowFallsBackLossily(t *testing.T) {
	providerRequests := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		providerRequests++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"<summary>unexpected</summary>"}}]}`))
	}))
	t.Cleanup(upstream.Close)

	rt := testNeoRuntimeForServer(t, upstream)
	actor := newNeoActor(rt, "actor-provider-overflow", "thread-actor", "T-provider-overflow", "T-provider-overflow", neoActorRecord("actor-provider-overflow", "thread-actor", "T-provider-overflow"), nil)
	settings := map[string]any{"compactionControl": map[string]any{
		"contextTokenThreshold": 0,
		"summaryPrompt":         strings.Repeat("oversized compaction prompt ", 50000),
	}}
	route := neoModelRoute{Provider: "openai", Model: "gpt-5.6-sol"}
	conversation := []neoHistoryMessage{{Role: "user", Text: "immutable task"}}
	for index := 0; index < 100; index++ {
		callID := fmt.Sprintf("TU-provider-overflow-%d", index)
		conversation = append(conversation,
			neoHistoryMessage{Role: "assistant", ToolCalls: []neoToolCall{{ID: callID, Name: "Read"}}},
			neoHistoryMessage{Role: "tool", ToolCallID: callID, ToolName: "Read", Text: strings.Repeat("large result ", 400)},
		)
	}
	state := neoSubagentCompactionState{immutablePrefixLen: 1}
	buildRequest := func(history []neoHistoryMessage) neoInferenceRequest {
		routeCopy := route
		return neoInferenceRequest{AgentMode: "high", Settings: settings, History: history, ModelRouteOverride: &routeCopy, SystemPromptOverride: "bounded worker"}
	}

	rebuilt, request, err := state.prepare(context.Background(), actor, actor.generation, "Task", "high", settings, route, conversation, nil, buildRequest)
	if err != nil {
		t.Fatalf("compaction-provider overflow fallback: %v", err)
	}
	if providerRequests != 0 || !strings.Contains(rebuilt[1].Text, "exchanges were elided") {
		t.Fatalf("compaction-provider overflow requests=%d history_len=%d replacement=%q", providerRequests, len(rebuilt), rebuilt[1].Text)
	}
	if len(rebuilt) >= len(conversation) {
		t.Fatalf("lossy fallback removed no exchanges: before=%d after=%d", len(conversation), len(rebuilt))
	}
	rebuiltPressure := neoSubagentRequestPressure{
		estimatedTokens: neoEstimateInferenceInputTokens(request, route),
		maxInputTokens:  neoEffectiveMaxInputTokens("high", route.Model),
		maxInputKnown:   true,
		provider:        route.Provider,
	}
	if !rebuiltPressure.fitsHardLimit() {
		t.Fatalf("lossy fallback request still exceeds the provider limit: %#v", rebuiltPressure)
	}
}

func TestNeoSubagentTurnLimitStopsToolLoop(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-finder-turn-limit", "thread-actor", "T-finder-turn-limit", "T-finder-turn-limit", neoActorRecord("actor-finder-turn-limit", "thread-actor", "T-finder-turn-limit"), nil)
	setNeoFinderWorkspaceForTest(t, actor)
	calls := 0
	rt.inferStream = func(_ *neoRuntime, request neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		calls++
		if calls > 6 {
			if len(request.Tools) != 0 {
				t.Fatalf("forced synthesis retained tools: %#v", request.Tools)
			}
			return neoInferenceResult{Text: "bounded summary"}, nil
		}
		return neoInferenceResult{ToolCalls: []neoToolCall{{Incomplete: true}}}, nil
	}

	text, err := actor.executeSubagentRun("finder", map[string]any{"query": "search"}, "TU-finder-turn-limit", "M-1", actor.generation, 0, "")
	if err != nil || text != "bounded summary" {
		t.Fatalf("finder turn limit synthesis text=%q error=%v", text, err)
	}
	if calls != 7 {
		t.Fatalf("finder inference calls = %d, want 6 tool turns plus synthesis", calls)
	}
}

func TestNeoFinderRunsHaveNoAdmissionLimit(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-finder-admission", "thread-actor", "T-finder-admission", "T-finder-admission", neoActorRecord("actor-finder-admission", "thread-actor", "T-finder-admission"), nil)
	setNeoFinderWorkspaceForTest(t, actor)
	const runCount = 7
	entered := make(chan struct{}, runCount)
	release := make(chan struct{})
	rt.inferStream = func(_ *neoRuntime, _ neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		entered <- struct{}{}
		<-release
		return neoInferenceResult{Text: "done"}, nil
	}

	errs := make(chan error, runCount)
	for i := 0; i < runCount; i++ {
		go func(index int) {
			_, err := actor.executeSubagentRun("finder", map[string]any{"query": "search"}, "TU-finder-"+strconv.Itoa(index), "M-1", actor.generation, 0, "")
			errs <- err
		}(i)
	}
	for i := 0; i < runCount; i++ {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatalf("finder %d did not enter inference", i)
		}
	}
	actor.mu.Lock()
	tracked := actor.activeFinderRuns
	actor.mu.Unlock()
	if tracked != runCount {
		t.Fatalf("tracked finder runs = %d, want %d", tracked, runCount)
	}

	close(release)
	for i := 0; i < runCount; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("finder %d failed: %v", i, err)
		}
	}
	if !neoWaitFor(time.Second, func() bool {
		actor.mu.Lock()
		defer actor.mu.Unlock()
		return actor.activeFinderRuns == 0
	}) {
		t.Fatal("completed finders remained tracked")
	}
}

func TestNeoSubagentRunsHaveNoAdmissionLimit(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-subagent-admission", "thread-actor", "T-subagent-admission", "T-subagent-admission", neoActorRecord("actor-subagent-admission", "thread-actor", "T-subagent-admission"), nil)
	const runCount = 7
	entered := make(chan struct{}, runCount)
	release := make(chan struct{})
	rt.inferStream = func(_ *neoRuntime, _ neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		entered <- struct{}{}
		<-release
		return neoInferenceResult{Text: "done"}, nil
	}

	errs := make(chan error, runCount)
	for index := 0; index < runCount; index++ {
		go func(index int) {
			_, err := actor.executeSubagentRun("Task", map[string]any{"prompt": "inspect", "description": "inspect"}, "TU-task-"+strconv.Itoa(index), "M-1", actor.generation, 0, "")
			errs <- err
		}(index)
	}
	for index := 0; index < runCount; index++ {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatalf("subagent %d did not enter inference", index)
		}
	}
	actor.mu.Lock()
	tracked := len(actor.subagentRuns)
	actor.mu.Unlock()
	if tracked != runCount {
		t.Fatalf("tracked subagent runs = %d, want %d", tracked, runCount)
	}

	close(release)
	for index := 0; index < runCount; index++ {
		if err := <-errs; err != nil {
			t.Fatalf("subagent %d failed: %v", index, err)
		}
	}
	if !neoWaitFor(time.Second, func() bool {
		actor.mu.Lock()
		defer actor.mu.Unlock()
		return len(actor.subagentRuns) == 0
	}) {
		t.Fatal("completed subagents remained tracked")
	}
}

func TestNeoTaskTurnExecutesEveryToolCallWithBoundedConcurrency(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-task-turn", "thread-actor", "T-task-turn", "T-task-turn", neoActorRecord("actor-task-turn", "thread-actor", "T-task-turn"), nil)
	calls := make([]neoToolCall, neoSubagentMaxConcurrentToolCalls*2)
	for i := range calls {
		calls[i] = neoToolCall{ID: "task-leaf-" + strconv.Itoa(i), Name: "Read", Input: map[string]any{"path": "file.go"}}
	}

	done := make(chan []neoSubagentToolExchange, 1)
	go func() {
		done <- actor.execSubagentTurnTools(calls, "TU-task", actor.generation, 0, "smart", "")
	}()
	if !neoWaitFor(time.Second, func() bool {
		actor.mu.Lock()
		defer actor.mu.Unlock()
		return len(actor.subagentWaiters) == neoSubagentMaxConcurrentToolCalls
	}) {
		t.Fatal("Task did not lease the first bounded leaf-tool batch")
	}
	leased := map[string]bool{}
	for batch := 0; batch < len(calls)/neoSubagentMaxConcurrentToolCalls; batch++ {
		actor.mu.Lock()
		leasedIDs := make([]string, 0, len(actor.subagentWaiters))
		for toolCallID := range actor.subagentWaiters {
			if leased[toolCallID] {
				actor.mu.Unlock()
				t.Fatalf("Task leased tool %s in more than one batch", toolCallID)
			}
			leased[toolCallID] = true
			leasedIDs = append(leasedIDs, toolCallID)
		}
		actor.mu.Unlock()
		if len(leasedIDs) != neoSubagentMaxConcurrentToolCalls {
			t.Fatalf("batch %d leased %d tools, want %d", batch, len(leasedIDs), neoSubagentMaxConcurrentToolCalls)
		}
		for _, toolCallID := range leasedIDs {
			if !actor.routeSubagentLeafToolResult(toolCallID, map[string]any{"status": "done", "output": "read"}) {
				t.Fatalf("accepted tool %s did not consume its result", toolCallID)
			}
		}
		if batch+1 < len(calls)/neoSubagentMaxConcurrentToolCalls && !neoWaitFor(time.Second, func() bool {
			actor.mu.Lock()
			defer actor.mu.Unlock()
			return len(actor.subagentWaiters) == neoSubagentMaxConcurrentToolCalls
		}) {
			t.Fatal("Task did not lease the next bounded leaf-tool batch")
		}
	}
	select {
	case exchanges := <-done:
		if len(exchanges) != len(calls) {
			t.Fatalf("Task tool exchanges = %d, want %d", len(exchanges), len(calls))
		}
	case <-time.After(time.Second):
		t.Fatal("Task turn did not finish")
	}
}

func TestNeoTaskTurnBoundsNestedSubagentRuns(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-task-nested", "thread-actor", "T-task-nested", "T-task-nested", neoActorRecord("actor-task-nested", "thread-actor", "T-task-nested"), nil)
	entered := make(chan struct{}, neoSubagentMaxConcurrentNestedRuns+1)
	release := make(chan struct{})
	rt.inferStream = func(_ *neoRuntime, _ neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		entered <- struct{}{}
		<-release
		return neoInferenceResult{Text: "nested complete"}, nil
	}
	calls := make([]neoToolCall, neoSubagentMaxConcurrentNestedRuns+1)
	for index := range calls {
		calls[index] = neoToolCall{ID: "nested-" + strconv.Itoa(index), Name: "Task", Input: map[string]any{"prompt": "inspect", "description": "inspect"}}
	}
	done := make(chan []neoSubagentToolExchange, 1)
	go func() {
		done <- actor.execSubagentTurnTools(calls, "TU-parent", actor.generation, 0, "smart", "")
	}()
	for index := 0; index < neoSubagentMaxConcurrentNestedRuns; index++ {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("nested subagent did not enter inference")
		}
	}
	select {
	case <-entered:
		t.Fatal("nested subagents exceeded the per-turn run limit")
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	select {
	case exchanges := <-done:
		if len(exchanges) != len(calls) {
			t.Fatalf("nested exchanges = %d, want %d", len(exchanges), len(calls))
		}
		for index, exchange := range exchanges {
			if stringValue(exchange.Run["status"]) != "done" {
				t.Fatalf("nested exchange %d = %#v", index, exchange.Run)
			}
		}
	case <-time.After(time.Second):
		t.Fatal("nested Task turn did not finish")
	}
}

func TestNeoTaskTurnRejectsDuplicateToolCallIDs(t *testing.T) {
	actor := newNeoActor(newNeoRuntime(&config.Config{}), "actor-task-duplicate", "thread-actor", "T-task-duplicate", "T-task-duplicate", neoActorRecord("actor-task-duplicate", "thread-actor", "T-task-duplicate"), nil)
	calls := []neoToolCall{
		{ID: "duplicate", Name: "Read", Input: map[string]any{"path": "one.go"}},
		{ID: "duplicate", Name: "Read", Input: map[string]any{"path": "two.go"}},
	}

	done := make(chan []neoSubagentToolExchange, 1)
	go func() {
		done <- actor.execSubagentTurnTools(calls, "TU-task", actor.generation, 0, "smart", "")
	}()
	var exchanges []neoSubagentToolExchange
	select {
	case exchanges = <-done:
	case <-time.After(time.Second):
		t.Fatal("Task duplicate tool calls deadlocked")
	}
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
}

func TestNeoSubagentAbortsAfterThreeIdenticalAllToolErrors(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-task-errors", "thread-actor", "T-task-errors", "T-task-errors", neoActorRecord("actor-task-errors", "thread-actor", "T-task-errors"), nil)
	actor.currentAgentMode = "smart"
	actor.tools = map[string]neoToolSpec{"Read": {Name: "Read", InputSchema: map[string]any{"type": "object"}}}
	inferences := 0
	rt.inferStream = func(_ *neoRuntime, _ neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		inferences++
		return neoInferenceResult{ToolCalls: []neoToolCall{{
			ID:    "read-" + strconv.Itoa(inferences),
			Name:  "Read",
			Input: map[string]any{"path": "missing.go"},
		}}}, nil
	}

	done := make(chan error, 1)
	go func() {
		_, err := actor.executeSubagentRun("Task", map[string]any{"prompt": "inspect", "description": "inspect"}, "TU-task-errors", "M-1", actor.generation, 0, "")
		done <- err
	}()
	for attempt := 0; attempt < neoSubagentRepeatedToolErrorLimit; attempt++ {
		var toolCallID string
		if !neoWaitFor(time.Second, func() bool {
			actor.mu.Lock()
			defer actor.mu.Unlock()
			for id := range actor.subagentWaiters {
				toolCallID = id
				return true
			}
			return false
		}) {
			t.Fatalf("Task did not lease tool on attempt %d", attempt+1)
		}
		if !actor.routeSubagentLeafToolResult(toolCallID, map[string]any{"status": "error", "error": map[string]any{"message": "missing file"}}) {
			t.Fatalf("Task error result %d was not routed", attempt+1)
		}
	}
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "same tool error repeated 3 times") {
			t.Fatalf("Task repeated error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Task did not stop after repeated identical tool errors")
	}
	if inferences != neoSubagentRepeatedToolErrorLimit {
		t.Fatalf("Task inferences = %d, want %d", inferences, neoSubagentRepeatedToolErrorLimit)
	}
}

func TestNeoFinderCancellationReleasesGenerationSlots(t *testing.T) {
	const staleFinderRuns = 2

	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-finder-generation", "thread-actor", "T-finder-generation", "T-finder-generation", neoActorRecord("actor-finder-generation", "thread-actor", "T-finder-generation"), nil)
	setNeoFinderWorkspaceForTest(t, actor)
	entered := make(chan context.Context, staleFinderRuns)
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
		if call <= staleFinderRuns {
			entered <- request.Context
			<-releaseOld
			return neoInferenceResult{Text: "stale"}, nil
		}
		return neoInferenceResult{Text: "current"}, nil
	}

	oldDone := make(chan error, staleFinderRuns)
	oldGeneration := actor.generation
	for index := 0; index < staleFinderRuns; index++ {
		go func(index int) {
			_, err := actor.executeSubagentRun("finder", map[string]any{"query": "old"}, "TU-old-"+strconv.Itoa(index), "M-old", oldGeneration, 0, "")
			oldDone <- err
		}(index)
	}
	contexts := make([]context.Context, 0, staleFinderRuns)
	for len(contexts) < staleFinderRuns {
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
	for index := 0; index < staleFinderRuns; index++ {
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

func TestNeoFinderTurnExecutesEveryToolCallWithBoundedConcurrency(t *testing.T) {
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
		return len(actor.subagentWaiters) == neoFinderMaxConcurrentToolCalls
	}) {
		t.Fatal("finder did not lease the first bounded leaf-tool batch")
	}
	leased := map[string]bool{}
	for batch := 0; batch < len(calls)/neoFinderMaxConcurrentToolCalls; batch++ {
		actor.mu.Lock()
		leasedIDs := make([]string, 0, len(actor.subagentWaiters))
		for toolCallID := range actor.subagentWaiters {
			if leased[toolCallID] {
				actor.mu.Unlock()
				t.Fatalf("finder leased tool %s in more than one batch", toolCallID)
			}
			leased[toolCallID] = true
			leasedIDs = append(leasedIDs, toolCallID)
		}
		actor.mu.Unlock()
		if len(leasedIDs) != neoFinderMaxConcurrentToolCalls {
			t.Fatalf("batch %d leased %d tools, want %d", batch, len(leasedIDs), neoFinderMaxConcurrentToolCalls)
		}
		for _, toolCallID := range leasedIDs {
			if !actor.routeSubagentLeafToolResult(toolCallID, map[string]any{"status": "done", "output": "match"}) {
				t.Fatalf("accepted tool %s did not consume its result", toolCallID)
			}
		}
		if batch+1 < len(calls)/neoFinderMaxConcurrentToolCalls && !neoWaitFor(time.Second, func() bool {
			actor.mu.Lock()
			defer actor.mu.Unlock()
			return len(actor.subagentWaiters) == neoFinderMaxConcurrentToolCalls
		}) {
			t.Fatal("finder did not lease the next bounded leaf-tool batch")
		}
	}
	if len(leased) != len(calls) {
		t.Fatalf("leased tools = %d, want %d", len(leased), len(calls))
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
		if status != "done" {
			t.Fatalf("exchange %s status = %q", exchange.Call.ID, status)
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

func TestNeoSubagentConcurrentRunsNamespaceProviderToolCallIDs(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-subagent-namespaces", "thread-actor", "T-subagent-namespaces", "T-subagent-namespaces", neoActorRecord("actor-subagent-namespaces", "thread-actor", "T-subagent-namespaces"), nil)
	call := neoToolCall{ID: "provider-reused-id", Name: "Read", Input: map[string]any{"path": "go.mod"}}

	results := make(chan []neoSubagentToolExchange, 2)
	for _, parentToolCallID := range []string{"TU-check-a", "TU-check-b"} {
		go func(parentToolCallID string) {
			results <- actor.execSubagentTurnTools([]neoToolCall{call}, parentToolCallID, actor.generation, 0, "review", "")
		}(parentToolCallID)
	}
	if !neoWaitFor(time.Second, func() bool {
		actor.mu.Lock()
		defer actor.mu.Unlock()
		return len(actor.subagentWaiters) == 2
	}) {
		t.Fatal("concurrent subagent runs did not register distinct leaf tools")
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
				t.Fatalf("subagent exchange = %#v", exchanges)
			}
		case <-time.After(time.Second):
			t.Fatal("concurrent subagent run did not finish")
		}
	}

	pairs := map[string]struct {
		useID        string
		resultID     string
		resultOutput string
	}{}
	actor.mu.Lock()
	for _, message := range actor.messages {
		if message.ParentToolUseID == "" || len(message.Content) != 1 {
			continue
		}
		pair := pairs[message.ParentToolUseID]
		block := mapValue(message.Content[0])
		switch stringValue(block["type"]) {
		case "tool_use":
			pair.useID = stringValue(block["id"])
		case "tool_result":
			pair.resultID = stringValue(block["toolUseID"])
			pair.resultOutput = stringValue(mapValue(block["run"])["output"])
		}
		pairs[message.ParentToolUseID] = pair
	}
	actor.mu.Unlock()
	for _, parentToolCallID := range []string{"TU-check-a", "TU-check-b"} {
		pair := pairs[parentToolCallID]
		if pair.useID == "" || pair.useID != pair.resultID || pair.resultOutput != pair.useID {
			t.Fatalf("parent %s transcript pair = %#v", parentToolCallID, pair)
		}
	}
}

func TestNeoStoredSubagentExchangesNamespaceProviderToolCallIDs(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-stored-exchange-namespaces", "thread-actor", "T-stored-exchange-namespaces", "T-stored-exchange-namespaces", neoActorRecord("actor-stored-exchange-namespaces", "thread-actor", "T-stored-exchange-namespaces"), nil)
	exchange := neoSubagentToolExchange{
		Call: neoToolCall{ID: "provider-reused-stored-id", Name: "Read", Input: map[string]any{"path": "go.mod"}},
		Run:  map[string]any{"status": "done", "output": "module"},
	}
	actor.storeSubagentToolExchanges([]neoSubagentToolExchange{exchange}, "TU-read-thread-a", actor.generation)
	actor.storeSubagentToolExchanges([]neoSubagentToolExchange{exchange}, "TU-read-thread-b", actor.generation)

	pairs := map[string][2]string{}
	actor.mu.Lock()
	for _, message := range actor.messages {
		if len(message.Content) != 1 {
			continue
		}
		pair := pairs[message.ParentToolUseID]
		block := mapValue(message.Content[0])
		switch stringValue(block["type"]) {
		case "tool_use":
			pair[0] = stringValue(block["id"])
		case "tool_result":
			pair[1] = stringValue(block["toolUseID"])
		}
		pairs[message.ParentToolUseID] = pair
	}
	actor.mu.Unlock()
	first := pairs["TU-read-thread-a"]
	second := pairs["TU-read-thread-b"]
	if first[0] == "" || second[0] == "" || first[0] != first[1] || second[0] != second[1] || first[0] == second[0] || first[0] == exchange.Call.ID || second[0] == exchange.Call.ID {
		t.Fatalf("stored exchange transcript pairs = first:%#v second:%#v provider:%q", first, second, exchange.Call.ID)
	}
	if exchange.Call.ID != "provider-reused-stored-id" {
		t.Fatalf("stored exchange mutated provider call ID: %#v", exchange.Call)
	}
}

func TestNeoStoredSubagentExchangesRejectStaleGeneration(t *testing.T) {
	actor := newNeoActor(newNeoRuntime(&config.Config{}), "actor-stale-stored-exchange", "thread-actor", "T-stale-stored-exchange", "T-stale-stored-exchange", neoActorRecord("actor-stale-stored-exchange", "thread-actor", "T-stale-stored-exchange"), nil)
	generation := actor.generation
	actor.mu.Lock()
	actor.advanceGenerationLocked()
	actor.mu.Unlock()
	stored := actor.storeSubagentToolExchanges([]neoSubagentToolExchange{{
		Call: neoToolCall{ID: "provider-stale", Name: "read_thread_messages"},
		Run:  map[string]any{"status": "done", "result": "stale"},
	}}, "TU-read-thread", generation)
	if stored {
		t.Fatal("stale generation stored read_thread exchanges")
	}
	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.messages) != 0 {
		t.Fatalf("stale stored exchanges appended messages: %#v", actor.messages)
	}
}

func TestNeoRegisterSubagentLeafToolRejectsOwnedID(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-subagent-owned-id", "thread-actor", "T-subagent-owned-id", "T-subagent-owned-id", neoActorRecord("actor-subagent-owned-id", "thread-actor", "T-subagent-owned-id"), nil)
	call := neoToolCall{ID: "TU-owned", Name: "Read", Input: map[string]any{"path": "go.mod"}}
	first := make(chan map[string]any, 1)
	second := make(chan map[string]any, 1)
	if !actor.registerSubagentLeafTool(call, "TU-parent-a", "M-a", actor.generation, first) {
		t.Fatal("initial leaf registration failed")
	}
	if actor.registerSubagentLeafTool(call, "TU-parent-b", "M-b", actor.generation, second) {
		t.Fatal("duplicate leaf registration replaced the existing owner")
	}
	actor.mu.Lock()
	owner := actor.subagentWaiters[call.ID]
	pending := actor.subagentTools[call.ID]
	actor.mu.Unlock()
	if owner != first || pending.ParentToolCallID != "TU-parent-a" || pending.MessageID != "M-a" {
		t.Fatalf("leaf owner changed after duplicate registration: owner=%p pending=%#v", owner, pending)
	}
	duplicateRun := actor.execSubagentLeafTool(context.Background(), call, "TU-parent-b", "M-b", actor.generation)
	if stringValue(duplicateRun["status"]) != "error" || !strings.Contains(runToText(duplicateRun), "already registered") {
		t.Fatalf("duplicate leaf execution = %#v", duplicateRun)
	}
	actor.mu.Lock()
	owner = actor.subagentWaiters[call.ID]
	pending = actor.subagentTools[call.ID]
	messages := len(actor.messages)
	actor.mu.Unlock()
	if owner != first || pending.ParentToolCallID != "TU-parent-a" || messages != 0 {
		t.Fatalf("duplicate leaf execution changed owner state: owner=%p pending=%#v messages=%d", owner, pending, messages)
	}
	if !actor.routeSubagentLeafToolResult(call.ID, map[string]any{"status": "done", "output": "module"}) {
		t.Fatal("owned leaf result was not routed")
	}
	select {
	case run := <-first:
		if stringValue(run["status"]) != "done" {
			t.Fatalf("owned leaf result = %#v", run)
		}
	case <-time.After(time.Second):
		t.Fatal("owned leaf waiter was not resolved")
	}
	select {
	case run := <-second:
		t.Fatalf("replacement waiter received result: %#v", run)
	default:
	}
}

func TestNeoFinderConcurrentScopeErrorsNamespaceProviderToolCallIDs(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-finder-error-namespaces", "thread-actor", "T-finder-error-namespaces", "T-finder-error-namespaces", neoActorRecord("actor-finder-error-namespaces", "thread-actor", "T-finder-error-namespaces"), nil)
	root := setNeoFinderWorkspaceForTest(t, actor)
	call := neoToolCall{ID: "provider-reused-error-id", Name: "Read", Input: map[string]any{}}

	results := make(chan []neoSubagentToolExchange, 2)
	for _, parentToolCallID := range []string{"TU-finder-error-a", "TU-finder-error-b"} {
		go func(parentToolCallID string) {
			results <- actor.execFinderTurnTools(context.Background(), []neoToolCall{call}, root, root, parentToolCallID, actor.generation)
		}(parentToolCallID)
	}
	for range 2 {
		select {
		case exchanges := <-results:
			if len(exchanges) != 1 || exchanges[0].Call.ID != call.ID || stringValue(exchanges[0].Run["status"]) != "error" {
				t.Fatalf("finder scope-error exchange = %#v", exchanges)
			}
		case <-time.After(time.Second):
			t.Fatal("finder scope-error run did not finish")
		}
	}

	pairs := map[string][2]string{}
	actor.mu.Lock()
	for _, message := range actor.messages {
		if message.ParentToolUseID == "" || len(message.Content) != 1 {
			continue
		}
		pair := pairs[message.ParentToolUseID]
		block := mapValue(message.Content[0])
		switch stringValue(block["type"]) {
		case "tool_use":
			pair[0] = stringValue(block["id"])
		case "tool_result":
			pair[1] = stringValue(block["toolUseID"])
		}
		pairs[message.ParentToolUseID] = pair
	}
	actor.mu.Unlock()
	first := pairs["TU-finder-error-a"]
	second := pairs["TU-finder-error-b"]
	if first[0] == "" || second[0] == "" || first[0] != first[1] || second[0] != second[1] || first[0] == second[0] || first[0] == call.ID || second[0] == call.ID {
		t.Fatalf("finder scope-error transcript pairs = first:%#v second:%#v provider:%q", first, second, call.ID)
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
	const leafCount = 4

	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-finder-cancel", "thread-actor", "T-finder-cancel", "T-finder-cancel", neoActorRecord("actor-finder-cancel", "thread-actor", "T-finder-cancel"), nil)
	runContext, _, acquired := actor.acquireSubagentRun(actor.generation)
	if !acquired {
		t.Fatal("subagent run context was not acquired")
	}
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

	results := make(chan map[string]any, leafCount)
	for i := 0; i < leafCount; i++ {
		call := neoToolCall{ID: "leaf-cancel-" + strconv.Itoa(i), Name: "Grep", Input: map[string]any{"pattern": "x"}}
		go func(call neoToolCall) {
			results <- actor.execSubagentLeafTool(runContext, call, "TU-finder", "M-leaf", actor.generation)
		}(call)
	}
	if !neoWaitFor(time.Second, func() bool {
		actor.mu.Lock()
		defer actor.mu.Unlock()
		return len(actor.subagentWaiters) == leafCount
	}) {
		t.Fatal("leaf tools did not register")
	}
	actor.mu.Lock()
	actor.pendingTools["leaf-cancel-0"] = neoPendingTool{ID: "leaf-cancel-0", Name: "Grep"}
	actor.mu.Unlock()

	started := time.Now()
	actor.cancel()
	for i := 0; i < leafCount; i++ {
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
	for i := 0; i < leafCount; i++ {
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

func TestNeoSubagentCancellationReturnsAuthoritativeReason(t *testing.T) {
	actor := newNeoActor(newNeoRuntime(&config.Config{}), "actor-cancellation-reason", "thread-actor", "T-cancellation-reason", "T-cancellation-reason", neoActorRecord("actor-cancellation-reason", "thread-actor", "T-cancellation-reason"), nil)
	toolCallID := "TU-cancellation-reason"
	waiter := make(chan map[string]any, 1)
	actor.subagentWaiters = map[string]chan map[string]any{}
	actor.subagentWaiters[toolCallID] = waiter
	actor.subagentTools[toolCallID] = neoPendingTool{ID: toolCallID, Name: "Read", ParentToolCallID: "TU-parent"}
	actor.mu.Lock()
	cancellation := actor.takeSubagentCancellationLocked()
	actor.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan map[string]any, 1)
	go func() {
		done <- actor.waitSubagentLeafTool(ctx, toolCallID, waiter)
	}()
	cancel()
	select {
	case run := <-done:
		t.Fatalf("waiter returned before authoritative cancellation: %#v", run)
	case <-time.After(20 * time.Millisecond):
	}
	actor.finishSubagentCancellation(cancellation, "system:disposed", "executor_disconnected")
	select {
	case run := <-done:
		if stringValue(run["status"]) != "cancelled" || stringValue(run["reason"]) != "system:disposed" {
			t.Fatalf("authoritative cancellation run = %#v", run)
		}
	case <-time.After(time.Second):
		t.Fatal("authoritative cancellation did not wake waiter")
	}
}

func TestNeoImportClearsSubagentOwnershipAndRejectsLateResult(t *testing.T) {
	threadID := "T-019f4000-0000-4000-8000-000000000160"
	actor := newNeoActor(newNeoRuntime(&config.Config{}), "actor-import-subagent", "thread-actor", threadID, threadID, neoActorRecord("actor-import-subagent", "thread-actor", threadID), nil)
	toolCallID := "TU-import-leaf"
	waiter := make(chan map[string]any, 1)
	actor.subagentWaiters = map[string]chan map[string]any{}
	events := make(chan map[string]any, 16)
	socket := &neoSocket{writeMessage: func(_ int, data []byte) error {
		var event map[string]any
		if json.Unmarshal(data, &event) == nil {
			events <- event
		}
		return nil
	}}
	socket.markExecutor("executor-import")
	actor.mu.Lock()
	actor.sockets[socket] = struct{}{}
	actor.executorSocket = socket
	actor.executorID = "executor-import"
	actor.executorReady = true
	actor.executorBootstrapComplete = true
	actor.subagentWaiters[toolCallID] = waiter
	actor.subagentTools[toolCallID] = neoPendingTool{ID: toolCallID, Name: "Read", MessageID: "M-old", ParentToolCallID: "TU-old-parent"}
	actor.subagentToolLeaseAcks[toolCallID] = true
	actor.mu.Unlock()

	if err := actor.importThreadLocalOnly(map[string]any{
		"id": threadID, "agentMode": "smart", "messages": []any{map[string]any{
			"role": "user", "messageId": "M-imported", "content": []any{map[string]any{"type": "text", "text": "imported"}},
		}},
	}); err != nil {
		t.Fatalf("import thread: %v", err)
	}
	actor.replayUnacknowledgedSubagentToolLeases()
	select {
	case run := <-waiter:
		if stringValue(run["status"]) != "cancelled" || stringValue(run["reason"]) != "system:disposed" {
			t.Fatalf("import cancellation run = %#v", run)
		}
	case <-time.After(time.Second):
		t.Fatal("import did not wake prior subagent waiter")
	}
	actor.mu.Lock()
	waiters := len(actor.subagentWaiters)
	tools := len(actor.subagentTools)
	acks := len(actor.subagentToolLeaseAcks)
	actor.mu.Unlock()
	if waiters != 0 || tools != 0 || acks != 0 {
		t.Fatalf("subagent ownership after import = waiters:%d tools:%d acks:%d", waiters, tools, acks)
	}
	if actor.routeSubagentLeafToolResult(toolCallID, map[string]any{"status": "done", "output": "late"}) {
		t.Fatal("late result crossed imported generation")
	}
	revocations := 0
	leases := 0
	draining := true
	for draining {
		select {
		case event := <-events:
			switch stringValue(event["type"]) {
			case "executor_tool_lease_revoked":
				if stringValue(event["toolCallId"]) == toolCallID {
					revocations++
				}
			case "tool_lease":
				if stringValue(event["toolCallId"]) == toolCallID {
					leases++
				}
			}
		default:
			draining = false
		}
	}
	if revocations != 1 || leases != 0 {
		t.Fatalf("import lease events = revocations:%d leases:%d", revocations, leases)
	}
}

func TestNeoSubagentLeafContextCancellationClearsOwnedRegistration(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-leaf-context-cancel", "thread-actor", "T-leaf-context-cancel", "T-leaf-context-cancel", neoActorRecord("actor-leaf-context-cancel", "thread-actor", "T-leaf-context-cancel"), nil)
	call := neoToolCall{ID: "TU-context-leaf", Name: "Read", Input: map[string]any{"path": "go.mod"}}
	childMessageID := actor.storeSubagentToolUseMessage(call, "TU-parent")
	events := make(chan map[string]any, 4)
	socket := &neoSocket{writeMessage: func(_ int, data []byte) error {
		var event map[string]any
		if json.Unmarshal(data, &event) == nil {
			events <- event
		}
		return nil
	}}
	actor.sockets[socket] = struct{}{}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan map[string]any, 1)
	go func() {
		done <- actor.execSubagentLeafTool(ctx, call, "TU-parent", childMessageID, actor.generation)
	}()
	if !neoWaitFor(time.Second, func() bool {
		actor.mu.Lock()
		defer actor.mu.Unlock()
		return actor.subagentWaiters[call.ID] != nil
	}) {
		t.Fatal("leaf tool did not register")
	}
	cancel()
	select {
	case run := <-done:
		if stringValue(run["status"]) != "cancelled" || stringValue(run["reason"]) != "user:cancelled" {
			t.Fatalf("context-cancelled leaf result = %#v", run)
		}
	case <-time.After(time.Second):
		t.Fatal("context cancellation did not wake leaf waiter")
	}
	actor.mu.Lock()
	if len(actor.subagentWaiters) != 0 || len(actor.subagentTools) != 0 {
		t.Fatalf("context cancellation left tracking: waiters=%d tools=%d", len(actor.subagentWaiters), len(actor.subagentTools))
	}
	var terminalResult bool
	for _, message := range actor.messages {
		if message.ParentToolUseID != "TU-parent" || len(message.Content) != 1 {
			continue
		}
		block := mapValue(message.Content[0])
		if stringValue(block["type"]) == "tool_result" && stringValue(block["toolUseID"]) == call.ID && stringValue(mapValue(block["run"])["status"]) == "cancelled" {
			terminalResult = true
		}
	}
	actor.mu.Unlock()
	if !terminalResult {
		t.Fatal("context cancellation did not persist a terminal leaf result")
	}
	var leased, revoked bool
	deadline := time.After(time.Second)
	for !leased || !revoked {
		select {
		case event := <-events:
			switch stringValue(event["type"]) {
			case "tool_lease":
				leased = stringValue(event["toolCallId"]) == call.ID
			case "executor_tool_lease_revoked":
				revoked = stringValue(event["toolCallId"]) == call.ID
			}
		case <-deadline:
			t.Fatalf("leaf cancellation events = leased:%v revoked:%v", leased, revoked)
		}
	}
}

func TestNeoFinderTurnContextCancellationWakesLeaf(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-finder-context-cancel", "thread-actor", "T-finder-context-cancel", "T-finder-context-cancel", neoActorRecord("actor-finder-context-cancel", "thread-actor", "T-finder-context-cancel"), nil)
	root := setNeoFinderWorkspaceForTest(t, actor)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan []neoSubagentToolExchange, 1)
	go func() {
		done <- actor.execFinderTurnTools(ctx, []neoToolCall{{ID: "provider-call", Name: "Grep", Input: map[string]any{"pattern": "neoActor"}}}, root, root, "TU-finder", actor.generation)
	}()
	if !neoWaitFor(time.Second, func() bool {
		actor.mu.Lock()
		defer actor.mu.Unlock()
		return len(actor.subagentWaiters) == 1
	}) {
		t.Fatal("finder leaf did not register")
	}
	cancel()
	select {
	case exchanges := <-done:
		if len(exchanges) != 1 || stringValue(exchanges[0].Run["status"]) != "cancelled" {
			t.Fatalf("cancelled finder exchange = %#v", exchanges)
		}
	case <-time.After(time.Second):
		t.Fatal("finder context cancellation did not wake leaf waiter")
	}
	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.subagentWaiters) != 0 || len(actor.subagentTools) != 0 {
		t.Fatalf("finder context cancellation left tracking: waiters=%d tools=%d", len(actor.subagentWaiters), len(actor.subagentTools))
	}
}

func TestNeoFinderExecutorRevocationWakesLeaf(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-finder-revoke", "thread-actor", "T-finder-revoke", "T-finder-revoke", neoActorRecord("actor-finder-revoke", "thread-actor", "T-finder-revoke"), nil)
	childMessageID := actor.storeSubagentToolUseMessage(neoToolCall{ID: "leaf-revoke", Name: "Grep", Input: map[string]any{"pattern": "x"}}, "TU-finder")
	done := make(chan map[string]any, 1)
	go func() {
		done <- actor.execSubagentLeafTool(context.Background(), neoToolCall{ID: "leaf-revoke", Name: "Grep", Input: map[string]any{"pattern": "x"}}, "TU-finder", childMessageID, actor.generation)
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

func TestNeoSubagentLeafLeaseReplaysAfterExecutorReconnect(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-finder-reconnect", "thread-actor", "T-finder-reconnect", "T-finder-reconnect", neoActorRecord("actor-finder-reconnect", "thread-actor", "T-finder-reconnect"), nil)
	leases := make(chan string, 2)
	newSocket := func(executorID string) *neoSocket {
		socket := &neoSocket{writeMessage: func(_ int, data []byte) error {
			var event map[string]any
			if json.Unmarshal(data, &event) == nil && stringValue(event["type"]) == "tool_lease" {
				leases <- stringValue(event["toolCallId"])
			}
			return nil
		}}
		socket.markExecutor(executorID)
		return socket
	}
	oldSocket := newSocket("executor-1")
	actor.mu.Lock()
	actor.sockets[oldSocket] = struct{}{}
	actor.executorID = "executor-1"
	actor.resumeExecutorID = "executor-1"
	actor.executorReady = true
	actor.executorBootstrapComplete = true
	actor.executorSocket = oldSocket
	actor.mu.Unlock()

	result := make(chan map[string]any, 1)
	go func() {
		result <- actor.execSubagentLeafTool(context.Background(), neoToolCall{ID: "leaf-reconnect", Name: "Grep", Input: map[string]any{"pattern": "x"}}, "TU-finder", "M-leaf", actor.generation)
	}()
	select {
	case toolCallID := <-leases:
		if toolCallID != "leaf-reconnect" {
			t.Fatalf("initial lease = %q", toolCallID)
		}
	case <-time.After(time.Second):
		t.Fatal("initial leaf lease was not sent")
	}

	actor.close(oldSocket)
	replacement := newSocket("executor-1")
	actor.mu.Lock()
	actor.sockets[replacement] = struct{}{}
	actor.mu.Unlock()
	actor.executorConnectForSocket(replacement, map[string]any{"clientId": "executor-1"})
	select {
	case toolCallID := <-leases:
		t.Fatalf("lease replayed before tool bootstrap: %q", toolCallID)
	default:
	}
	actor.executorToolsBootstrapComplete(map[string]any{"ok": true})
	select {
	case toolCallID := <-leases:
		if toolCallID != "leaf-reconnect" {
			t.Fatalf("replayed lease = %q", toolCallID)
		}
	case <-time.After(time.Second):
		t.Fatal("unacknowledged leaf lease was not replayed")
	}

	if !actor.routeSubagentLeafToolResult("leaf-reconnect", map[string]any{"status": "done", "output": "found"}) {
		t.Fatal("replayed leaf result was not consumed")
	}
	select {
	case run := <-result:
		if stringValue(run["status"]) != "done" || stringValue(run["output"]) != "found" {
			t.Fatalf("leaf result = %#v", run)
		}
	case <-time.After(time.Second):
		t.Fatal("replayed leaf did not wake subagent")
	}
}

func TestNeoSubagentAcknowledgedLeafLeaseIsNotReplayed(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-finder-ack", "thread-actor", "T-finder-ack", "T-finder-ack", neoActorRecord("actor-finder-ack", "thread-actor", "T-finder-ack"), nil)
	leases := make(chan string, 2)
	newSocket := func(executorID string) *neoSocket {
		socket := &neoSocket{writeMessage: func(_ int, data []byte) error {
			var event map[string]any
			if json.Unmarshal(data, &event) == nil && stringValue(event["type"]) == "tool_lease" {
				leases <- stringValue(event["toolCallId"])
			}
			return nil
		}}
		socket.markExecutor(executorID)
		return socket
	}
	oldSocket := newSocket("executor-1")
	actor.mu.Lock()
	actor.sockets[oldSocket] = struct{}{}
	actor.executorID = "executor-1"
	actor.resumeExecutorID = "executor-1"
	actor.executorReady = true
	actor.executorBootstrapComplete = true
	actor.executorSocket = oldSocket
	actor.mu.Unlock()

	result := make(chan map[string]any, 1)
	go func() {
		result <- actor.execSubagentLeafTool(context.Background(), neoToolCall{ID: "leaf-ack", Name: "Read", Input: map[string]any{"path": "go.mod"}}, "TU-finder", "M-leaf", actor.generation)
	}()
	select {
	case <-leases:
	case <-time.After(time.Second):
		t.Fatal("initial leaf lease was not sent")
	}
	actor.handleForSocket(oldSocket, map[string]any{"type": "executor_tool_lease_ack", "toolCallId": "leaf-ack"})

	actor.close(oldSocket)
	replacement := newSocket("executor-1")
	actor.mu.Lock()
	actor.sockets[replacement] = struct{}{}
	actor.mu.Unlock()
	actor.executorConnectForSocket(replacement, map[string]any{"clientId": "executor-1"})
	actor.executorToolsBootstrapComplete(map[string]any{"ok": true})
	select {
	case toolCallID := <-leases:
		t.Fatalf("acknowledged lease was replayed: %q", toolCallID)
	case <-time.After(50 * time.Millisecond):
	}

	if !actor.routeSubagentLeafToolResult("leaf-ack", map[string]any{"status": "done", "output": "module"}) {
		t.Fatal("acknowledged leaf result was not consumed")
	}
	select {
	case run := <-result:
		if stringValue(run["status"]) != "done" {
			t.Fatalf("leaf result = %#v", run)
		}
	case <-time.After(time.Second):
		t.Fatal("acknowledged leaf did not wake subagent")
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
	if rootWork, rootScope, err := neoFinderWorkspaceScope("/", "/"); err != nil || rootWork != "/" || rootScope != "/" {
		t.Fatalf("filesystem root scope = work:%q root:%q err=%v", rootWork, rootScope, err)
	}
	if home, homeErr := os.UserHomeDir(); homeErr == nil {
		canonicalHome, canonicalErr := neoFinderCanonicalDirectory(home)
		if canonicalErr != nil {
			t.Fatalf("canonicalize home directory: %v", canonicalErr)
		}
		if homeWork, homeScope, err := neoFinderWorkspaceScope(home, home); err != nil || homeWork != canonicalHome || homeScope != canonicalHome {
			t.Fatalf("home directory scope = work:%q root:%q err=%v", homeWork, homeScope, err)
		}
		alias := filepath.Join(t.TempDir(), "workspace")
		if err := os.Symlink(home, alias); err == nil {
			if aliasWork, aliasScope, err := neoFinderWorkspaceScope(alias, alias); err != nil || aliasWork != canonicalHome || aliasScope != canonicalHome {
				t.Fatalf("home symlink scope = work:%q root:%q err=%v", aliasWork, aliasScope, err)
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
	scopedRemoteGrepGlob, err := neoScopeFinderToolCallForExecutor(neoToolCall{ID: "scoped-remote-grep-glob", Name: "Grep", Input: map[string]any{"pattern": "needle", "path": "src", "glob": "**/*.go"}}, repo, parent)
	if err != nil || stringValue(scopedRemoteGrepGlob.Input["path"]) != repo+"/src" || stringValue(scopedRemoteGrepGlob.Input["glob"]) != "**/*.go" {
		t.Fatalf("scoped remote Grep with glob = %#v err=%v", scopedRemoteGrepGlob.Input, err)
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
		{ID: "grep-path-glob-parent", Name: "Grep", Input: map[string]any{"pattern": "needle", "path": "src", "glob": "../**/*.go"}},
		{ID: "grep-path-glob-absolute", Name: "Grep", Input: map[string]any{"pattern": "needle", "path": "src", "glob": repo + "/**/*.go"}},
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

func TestNeoFinderRejectsDangerousGlobRootsWithoutRepositoryEntryLimit(t *testing.T) {
	for _, root := range []string{"/", "/Users", "/home", "/root", "/Volumes", "/mnt", "/media", "/proc", "/sys", "/dev", "C:/"} {
		if !neoFinderDangerousGlobRoot(root) {
			t.Errorf("dangerous glob root %q was accepted", root)
		}
	}
	for _, root := range []string{"/Users/aikins01/Developer/repo", "/home/user/repo", "/Volumes/work/repo", "/mnt/work/repo", "C:/Users/Amp/repo"} {
		if neoFinderDangerousGlobRoot(root) {
			t.Errorf("project glob root %q was rejected", root)
		}
	}
	if _, err := neoScopeFinderToolCall(neoToolCall{ID: "root-glob", Name: "glob", Input: map[string]any{"filePattern": "**/*"}}, "/"); err == nil || !strings.Contains(err.Error(), "project workspace") {
		t.Fatalf("filesystem-root glob error = %v, want project workspace rejection", err)
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
	scopedDir := filepath.Join(repo, "scoped")
	if err := os.Mkdir(scopedDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(scopedDir, "outside")); err != nil {
		t.Skipf("create scoped out-of-workspace symlink: %v", err)
	}
	if _, err := neoScopeFinderToolCall(neoToolCall{ID: "grep-glob-escape", Name: "Grep", Input: map[string]any{"pattern": "secret", "path": "scoped", "glob": "outside/**"}}, repo); err == nil {
		t.Fatal("Finder accepted a Grep glob through an out-of-workspace symlink")
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
	if _, err := neoScopeFinderToolCall(neoToolCall{ID: "nested-glob", Name: "glob", Input: map[string]any{"filePattern": "nested-scope/**/*.txt"}}, repo); err == nil {
		t.Fatal("Finder accepted a glob through a nested out-of-workspace symlink")
	}
	if scoped, err := neoScopeFinderToolCall(neoToolCall{ID: "narrow-glob", Name: "glob", Input: map[string]any{"filePattern": "inside/**/*.txt"}}, repo); err != nil || stringValue(scoped.Input["filePattern"]) != "inside/**/*.txt" {
		t.Fatalf("narrow in-workspace glob was affected by unrelated symlink: scoped=%#v err=%v", scoped.Input, err)
	}
	if scoped, err := neoScopeFinderToolCall(neoToolCall{ID: "narrow-brace-glob", Name: "glob", Input: map[string]any{"filePattern": "{inside,inside-alias}/**/*.txt"}}, repo); err != nil || stringValue(scoped.Input["filePattern"]) != "{inside,inside-alias}/**/*.txt" {
		t.Fatalf("narrow brace glob was affected by unrelated symlink: scoped=%#v err=%v", scoped.Input, err)
	}
	for _, pattern := range []string{"outside/**/*.txt", "**/*", "!(safe)/**/*.txt"} {
		if _, err := neoScopeFinderToolCall(neoToolCall{ID: "glob-symlink", Name: "glob", Input: map[string]any{"filePattern": pattern}}, repo); err == nil {
			t.Fatalf("Finder accepted glob %q through an out-of-workspace symlink", pattern)
		}
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := neoScopeFinderToolCallForExecutorContext(cancelled, neoToolCall{ID: "cancelled-glob", Name: "glob", Input: map[string]any{"filePattern": "**/*"}}, repo, repo); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled glob scope error = %v, want context canceled", err)
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

func TestNeoResolveSubagentToolsHonorsSettings(t *testing.T) {
	actor := &neoActor{
		settings: map[string]any{"tools.disable": []any{"shell_*"}},
		tools: map[string]neoToolSpec{
			"Read":                 {Name: "Read"},
			"shell_command":        {Name: "shell_command"},
			"shell_command_status": {Name: "shell_command_status"},
		},
	}
	tools := actor.resolveSubagentToolsLocked([]string{"Read", "shell_command", "shell_command_status"})
	if !neoSubagentHasTools(tools, "Read") || neoSubagentHasTools(tools, "shell_command") || neoSubagentHasTools(tools, "shell_command_status") {
		t.Fatalf("restricted subagent tools = %#v", tools)
	}
}

func TestNeoResolveSubagentToolsPreservesServerEditFile(t *testing.T) {
	actor := &neoActor{
		settings: map[string]any{},
		tools: map[string]neoToolSpec{
			"edit_file": {Name: "edit_file", Meta: map[string]any{"source": "builtin"}},
		},
	}
	tools := actor.resolveSubagentToolsLocked([]string{"edit_file"})
	if len(tools) != 1 || tools[0].Name != "edit_file" || stringValue(tools[0].Meta["source"]) != "server" {
		t.Fatalf("Task edit_file tools = %#v", tools)
	}
}

func TestNeoTaskSubagentScaffoldAllowlistUsesParentCatalog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "scaffold.yaml")
	if err := os.WriteFile(path, []byte("enableToolSpecs:\n  - name: Task\n  - name: Read\n"), 0o600); err != nil {
		t.Fatalf("write scaffold customization: %v", err)
	}
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-task-scaffold", "thread-actor", "T-task-scaffold", "T-task-scaffold", neoActorRecord("actor-task-scaffold", "thread-actor", "T-task-scaffold"), nil)
	actor.currentAgentMode = "smart"
	actor.settings["internal.scaffoldCustomizationFile"] = path
	actor.tools = map[string]neoToolSpec{
		"Task": {Name: "Task"},
		"Read": {Name: "Read"},
	}
	var requests []neoInferenceRequest
	rt.inferStream = func(_ *neoRuntime, request neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		requests = append(requests, request)
		return neoInferenceResult{Text: "done"}, nil
	}
	if _, err := actor.executeSubagentRun("Task", map[string]any{"prompt": "inspect"}, "TU-task-scaffold", "M-1", actor.generation, 0, ""); err != nil {
		t.Fatalf("Task subagent failed: %v", err)
	}
	if len(requests) != 1 || len(requests[0].Tools) != 1 || requests[0].Tools[0].Name != "Read" {
		t.Fatalf("Task subagent tools = %#v", requests)
	}
}

func TestNeoFinderEnvironmentPathsAndFileURIs(t *testing.T) {
	workingDir, workspaceRoot := neoFinderEnvironmentPaths(map[string]any{
		"initial": map[string]any{
			"workingDirectory": "file:///remote/My%20Workspace/repo/src",
			"trees":            []any{map[string]any{"uri": "file:///remote/My%20Workspace/repo"}},
		},
	})
	if workingDir != "/remote/My Workspace/repo/src" || workspaceRoot != "/remote/My Workspace/repo" {
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

func TestNeoSubagentAttachmentPathUsesExecutorPlatformSyntax(t *testing.T) {
	if got := neoSubagentAttachmentPath(`src\main.go`, `C:\repo`); got != "C:/repo/src/main.go" {
		t.Fatalf("Windows relative attachment path = %q", got)
	}
	if got := neoSubagentAttachmentPath(`D:\other\image.png`, `C:\repo`); got != "D:/other/image.png" {
		t.Fatalf("Windows absolute attachment path = %q", got)
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

func TestNeoRunCheckSubagentUsesGPT55InheritedEffort(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-run-check", "thread-actor", "T-run-check", "T-run-check", neoActorRecord("actor-run-check", "thread-actor", "T-run-check"), nil)
	actor.currentAgentMode = "review"
	actor.currentReasoningEffort = ""
	actor.settings = map[string]any{"reasoning.effort": "medium"}
	actor.guidanceSnapshot = map[string]any{"files": []any{map[string]any{"uri": "file:///workspace/AGENTS.md", "content": "RUN_CHECK_PROJECT_GUIDANCE"}}}
	actor.tools = map[string]neoToolSpec{
		"Read":                 {Name: "Read", InputSchema: map[string]any{"type": "object"}},
		"Grep":                 {Name: "Grep", InputSchema: map[string]any{"type": "object"}},
		"glob":                 {Name: "glob", InputSchema: map[string]any{"type": "object"}},
		"shell_command":        {Name: "shell_command", InputSchema: map[string]any{"type": "object"}},
		"shell_command_status": {Name: "shell_command_status", InputSchema: map[string]any{"type": "object"}},
	}

	var seen []neoInferenceRequest
	rt.inferStream = func(_ *neoRuntime, req neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		seen = append(seen, req)
		return neoInferenceResult{Text: `{"status":"error","errorMessage":"route fixture","issues":[]}`}, nil
	}

	_, err := actor.executeSubagentRun(" run_check ", map[string]any{
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
	if route == nil || route.Provider != "openai" || route.Model != "gpt-5.5" {
		t.Fatalf("run_check route = %#v, want openai/gpt-5.5", route)
	}
	if seen[0].ReasoningEffort != "medium" || stringValue(seen[0].Settings["reasoning.effort"]) != "medium" {
		t.Fatalf("run_check effort request=%q settings=%#v, want inherited medium", seen[0].ReasoningEffort, seen[0].Settings)
	}
	if !neoSubagentHasTools(seen[0].Tools, "Read", "Grep", "glob", "shell_command", "shell_command_status") {
		t.Fatalf("run_check tools = %#v, want review check tools", seen[0].Tools)
	}
	if !strings.Contains(seen[0].SystemPromptOverride, "RUN_CHECK_PROJECT_GUIDANCE") {
		t.Fatalf("run_check system prompt omitted project guidance: %q", seen[0].SystemPromptOverride)
	}
}

func TestNeoRunCheckSubagentRouteSuffixOverridesInheritedEffort(t *testing.T) {
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{SubagentModels: map[string][]string{
		"run_check": {"google/gemini-3-pro(low)"},
	}}}})
	actor := newNeoActor(rt, "actor-run-check-suffix", "thread-actor", "T-run-check-suffix", "T-run-check-suffix", neoActorRecord("actor-run-check-suffix", "thread-actor", "T-run-check-suffix"), nil)
	actor.currentAgentMode = "review"
	actor.currentReasoningEffort = "high"
	actor.settings = map[string]any{"reasoning.effort": "high"}
	var seen neoInferenceRequest
	rt.inferStream = func(_ *neoRuntime, req neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		seen = req
		return neoInferenceResult{Text: `{"status":"error","errorMessage":"route fixture","issues":[]}`}, nil
	}
	_, err := actor.executeSubagentRun("run_check", map[string]any{
		"checkName":    "repo-convention-fit",
		"checkContent": "Prefer repository conventions.",
	}, "TU-run-check-suffix", "M-1", actor.generation, 0, "")
	if err != nil {
		t.Fatalf("run_check subagent failed: %v", err)
	}
	if seen.ReasoningEffort != "low" || stringValue(seen.Settings["reasoning.effort"]) != "low" {
		t.Fatalf("run_check suffix effort request=%q settings=%#v, want explicit low", seen.ReasoningEffort, seen.Settings)
	}
	if seen.ModelRouteOverride == nil || seen.ModelRouteOverride.Provider != "google" || stringValue(seen.Settings["gemini.thinkingLevel"]) != "low" || neoProviderReasoningEffort(seen, *seen.ModelRouteOverride) != "low" {
		t.Fatalf("run_check provider effort route=%#v settings=%#v, want Gemini low", seen.ModelRouteOverride, seen.Settings)
	}
}

func TestNeoRunCheckSubagentMappedFallbackRoutesUsePerRouteEffort(t *testing.T) {
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
		ForceModelMappings: true,
		NeoLocalRuntime: config.AmpNeoLocalRuntime{SubagentModels: map[string][]string{
			"run_check": {"openai/model-a", "openai/model-b"},
		}},
	}})
	rt.setModelMapper(staticNeoModelMapper{
		"model-a": "openai/gpt-5.6-sol(max)",
		"model-b": "google/gemini-3-pro(low)",
	})
	actor := newNeoActor(rt, "actor-run-check-mapped-fallback", "thread-actor", "T-run-check-mapped-fallback", "T-run-check-mapped-fallback", neoActorRecord("actor-run-check-mapped-fallback", "thread-actor", "T-run-check-mapped-fallback"), nil)
	actor.currentAgentMode = "review"
	actor.currentReasoningEffort = "high"
	actor.settings = map[string]any{"reasoning.effort": "high", "gemini.thinkingLevel": "high"}

	transientErr := errors.New(`local provider stream error: {"type":"error","error":{"type":"overloaded_error","message":"temporarily unavailable"}}`)
	requests := make([]neoInferenceRequest, 0, 4)
	rt.inferStream = func(_ *neoRuntime, request neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		requests = append(requests, request)
		if request.ModelRouteOverride != nil && request.ModelRouteOverride.Model == "gpt-5.6-sol" {
			return neoInferenceResult{}, transientErr
		}
		return neoInferenceResult{Text: `{"status":"error","errorMessage":"route fixture","issues":[]}`}, nil
	}

	_, err := actor.executeSubagentRun("run_check", map[string]any{
		"checkName":    "repo-convention-fit",
		"checkContent": "Prefer repository conventions.",
	}, "TU-run-check-mapped-fallback", "M-1", actor.generation, 0, "")
	if err != nil {
		t.Fatalf("run_check mapped fallback failed: %v", err)
	}
	if len(requests) < 2 {
		t.Fatalf("run_check mapped fallback requests = %#v, want primary and fallback", requests)
	}

	for index, request := range requests[:len(requests)-1] {
		route := request.ModelRouteOverride
		if route == nil || route.Provider != "openai" || route.Model != "gpt-5.6-sol" || route.ThinkingSuffix != "max" || !request.ModelRouteOverrideResolved {
			t.Fatalf("primary request %d route = %#v resolved=%v, want mapped OpenAI max", index, route, request.ModelRouteOverrideResolved)
		}
		if request.ReasoningEffort != "max" || stringValue(request.Settings["reasoning.effort"]) != "max" || neoProviderReasoningEffort(request, *route) != "max" {
			t.Fatalf("primary request %d effort request=%q settings=%#v, want max", index, request.ReasoningEffort, request.Settings)
		}
	}
	fallback := requests[len(requests)-1]
	fallbackRoute := fallback.ModelRouteOverride
	if fallbackRoute == nil || fallbackRoute.Provider != "google" || fallbackRoute.Model != "gemini-3-pro" || fallbackRoute.ThinkingSuffix != "low" || !fallback.ModelRouteOverrideResolved {
		t.Fatalf("fallback route = %#v resolved=%v, want mapped Gemini low", fallbackRoute, fallback.ModelRouteOverrideResolved)
	}
	if fallback.ReasoningEffort != "low" || stringValue(fallback.Settings["reasoning.effort"]) != "low" || stringValue(fallback.Settings["gemini.thinkingLevel"]) != "low" || neoProviderReasoningEffort(fallback, *fallbackRoute) != "low" {
		t.Fatalf("fallback effort request=%q settings=%#v, want low", fallback.ReasoningEffort, fallback.Settings)
	}
}

func TestNeoActorPrepareRunCheckCapturesImmutableSnapshot(t *testing.T) {
	repository := t.TempDir()
	runGit := func(args ...string) {
		t.Helper()
		command := exec.Command("git", args...)
		command.Dir = repository
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, output)
		}
	}
	runGit("init")
	runGit("config", "user.email", "test@example.test")
	runGit("config", "user.name", "Test User")
	runGit("config", "commit.gpgsign", "false")
	if err := os.WriteFile(filepath.Join(repository, "tracked.go"), []byte("package sample\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit("add", "tracked.go")
	runGit("commit", "-m", "initial")
	if err := os.WriteFile(filepath.Join(repository, "tracked.go"), []byte("package sample\n\nconst value = 1  \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repository, "new.go"), []byte("package sample\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit("add", "new.go")
	if err := os.WriteFile(filepath.Join(repository, "empty.marker"), nil, 0o600); err != nil {
		t.Fatal(err)
	}

	actor := newNeoActor(newNeoRuntime(&config.Config{}), "actor-review", "thread-actor", "T-review", "T-review", neoActorRecord("actor-review", "thread-actor", "T-review"), nil)
	actor.environment = map[string]any{"workingDirectory": repository, "workspaceRoot": repository}
	actor.messages = append(actor.messages, neoMessage{MessageID: "M-review-1", Role: "user"})
	input := map[string]any{
		"checkName":       "repo-fit",
		"diffDescription": "git diff HEAD -- tracked.go new.go empty.marker",
		"files":           []any{"tracked.go"},
	}
	if _, err := actor.ensureReviewSnapshot(stringValue(input["diffDescription"]), "tracked.go", "new.go", "empty.marker"); err != nil {
		t.Fatalf("prepare exact review-level snapshot: %v", err)
	}
	reviewHash := actor.reviewSnapshot.Hash
	first, err := actor.prepareRunCheckInput(input)
	if err != nil {
		t.Fatalf("prepare first snapshot: %v", err)
	}
	firstHash := stringValue(first[neoReviewSnapshotHashKey])
	firstPacket := stringValue(first[neoReviewSnapshotTextKey])
	if firstHash == "" || !strings.Contains(firstPacket, "tracked.go") {
		t.Fatalf("captured snapshot = %#v", first)
	}
	if strings.Contains(firstPacket, "new.go") || strings.Contains(firstPacket, "empty.marker") {
		t.Fatalf("focused snapshot included unrelated files: %q", firstPacket)
	}
	if !strings.Contains(firstPacket, "+const value = 1  \n") {
		t.Fatalf("captured snapshot did not preserve trailing whitespace: %q", firstPacket)
	}
	mismatchedInput := cloneMap(input)
	mismatchedInput["diffDescription"] = "git diff HEAD -- new.go"
	mismatched, err := actor.prepareRunCheckInput(mismatchedInput)
	if err != nil {
		t.Fatalf("prepare mismatched run_check snapshot: %v", err)
	}
	if got := stringValue(mismatched["diffDescription"]); got != stringValue(input["diffDescription"]) {
		t.Fatalf("mismatched run_check description = %q, want authoritative %q", got, input["diffDescription"])
	}
	if stringValue(mismatched[neoReviewSnapshotHashKey]) != firstHash || stringValue(mismatched[neoReviewSnapshotTextKey]) != firstPacket {
		t.Fatalf("mismatched run_check replaced the established snapshot: %#v", mismatched)
	}
	if err := os.WriteFile(filepath.Join(repository, "tracked.go"), []byte("package sample\n\nconst value = 2\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	second, err := actor.prepareRunCheckInput(input)
	if err != nil {
		t.Fatalf("prepare second snapshot: %v", err)
	}
	if stringValue(second[neoReviewSnapshotHashKey]) != firstHash || stringValue(second[neoReviewSnapshotTextKey]) != firstPacket {
		t.Fatalf("review snapshot changed during the same review\nfirst=%s\nsecond=%s", firstPacket, stringValue(second[neoReviewSnapshotTextKey]))
	}
	restored := newNeoActor(newNeoRuntime(&config.Config{}), "actor-review-restored", "thread-actor", "T-review", "T-review", neoActorRecord("actor-review-restored", "thread-actor", "T-review"), nil)
	restored.environment = map[string]any{"workingDirectory": repository, "workspaceRoot": repository}
	restored.messages = append(restored.messages, neoMessage{MessageID: "M-review-1", Role: "user"})
	restored.meta[neoReviewSnapshotStateMetaKey] = cloneNeoJSONValue(actor.meta[neoReviewSnapshotStateMetaKey])
	restoredInput, err := restored.prepareRunCheckInput(input)
	if err != nil {
		t.Fatalf("restore persisted snapshot: %v", err)
	}
	if stringValue(restoredInput[neoReviewSnapshotHashKey]) != firstHash || stringValue(restoredInput[neoReviewSnapshotTextKey]) != firstPacket {
		t.Fatalf("persisted review snapshot changed after restore: %#v", restoredInput)
	}
	if snapshot, err := restored.reviewSnapshotForValidation(); err != nil || snapshot == nil || snapshot.Hash != reviewHash {
		t.Fatalf("restored submit_review snapshot = %#v err=%v", snapshot, err)
	}
	actor.messages = append(actor.messages, neoMessage{MessageID: "M-review-tool-result", Role: "user", Content: []any{map[string]any{"type": "tool_result", "toolUseID": "TU-run-check", "run": map[string]any{"status": "done"}}}})
	afterToolResult, err := actor.prepareRunCheckInput(input)
	if err != nil {
		t.Fatalf("prepare snapshot after tool result: %v", err)
	}
	if stringValue(afterToolResult[neoReviewSnapshotHashKey]) != firstHash || stringValue(afterToolResult[neoReviewSnapshotTextKey]) != firstPacket {
		t.Fatalf("review snapshot changed after tool result: %#v", afterToolResult)
	}
	actor.messages = append(actor.messages, neoMessage{MessageID: "M-review-2", Role: "user"})
	third, err := actor.prepareRunCheckInput(input)
	if err != nil {
		t.Fatalf("prepare snapshot for new review: %v", err)
	}
	if stringValue(third[neoReviewSnapshotHashKey]) == firstHash {
		t.Fatalf("new review reused stale snapshot: %#v", third)
	}
	actor.environment = map[string]any{"workingDirectory": t.TempDir()}
	actor.messages = append(actor.messages, neoMessage{MessageID: "M-review-3", Role: "user"})
	if _, err := actor.prepareRunCheckInput(input); err == nil {
		t.Fatal("snapshot capture failure did not fail run_check preparation")
	}
	if actor.reviewSnapshot != nil {
		t.Fatal("snapshot capture failure retained an older review snapshot")
	}
}

func TestNeoReviewSnapshotValidationSkipsCommittedRange(t *testing.T) {
	actor := newNeoActor(newNeoRuntime(&config.Config{}), "actor-review-range", "thread-actor", "T-review-range", "T-review-range", neoActorRecord("actor-review-range", "thread-actor", "T-review-range"), nil)
	description := "git diff --merge-base origin/HEAD HEAD"
	if snapshot, err := actor.ensureReviewSnapshot(description, "tracked.go"); err != nil || snapshot != nil {
		t.Fatalf("committed-range capture = %#v err=%v, want not applicable", snapshot, err)
	}
	if snapshot, err := actor.reviewSnapshotForValidation(); err != nil || snapshot != nil {
		t.Fatalf("committed-range validation = %#v err=%v, want skipped immutable validation", snapshot, err)
	}
}

func TestNeoActorReviewSnapshotCapturesNaturalDescriptionWithFiles(t *testing.T) {
	repository := t.TempDir()
	runGit := func(args ...string) {
		t.Helper()
		command := exec.Command("git", args...)
		command.Dir = repository
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, output)
		}
	}
	runGit("init")
	runGit("config", "user.email", "test@example.test")
	runGit("config", "user.name", "Test User")
	runGit("config", "commit.gpgsign", "false")
	filename := filepath.Join(repository, "tracked.go")
	if err := os.WriteFile(filename, []byte("package sample\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit("add", "tracked.go")
	runGit("commit", "-m", "initial")
	if err := os.WriteFile(filename, []byte("package sample\n\nconst changed = true\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	description := "uncommitted scoped parity changes"
	actor := newNeoActor(newNeoRuntime(&config.Config{}), "actor-natural-review", "thread-actor", "T-natural-review", "T-natural-review", neoActorRecord("actor-natural-review", "thread-actor", "T-natural-review"), nil)
	actor.environment = map[string]any{"workingDirectory": repository, "workspaceRoot": repository}
	actor.messages = append(actor.messages, neoMessage{MessageID: "M-natural-review", Role: "user"})
	otherFilename := filepath.Join(repository, "other.go")
	if err := os.WriteFile(otherFilename, []byte("package sample\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	actor.meta[neoReviewSnapshotStateMetaKey] = neoReviewSnapshotState(nil, description, "M-natural-review", nil, nil)

	snapshot, err := actor.ensureReviewSnapshot(description, "tracked.go")
	if err != nil {
		t.Fatalf("capture natural-description snapshot: %v", err)
	}
	if snapshot == nil || !slices.Equal(snapshot.Files, []string{"tracked.go"}) {
		t.Fatalf("natural-description snapshot = %#v", snapshot)
	}
	restored := newNeoActor(newNeoRuntime(&config.Config{}), "actor-natural-review-restored", "thread-actor", "T-natural-review", "T-natural-review", neoActorRecord("actor-natural-review-restored", "thread-actor", "T-natural-review"), nil)
	restored.meta[neoReviewSnapshotStateMetaKey] = cloneNeoJSONValue(actor.meta[neoReviewSnapshotStateMetaKey])
	validated, err := restored.reviewSnapshotForValidation()
	if err != nil || validated == nil || validated.Hash != snapshot.Hash {
		t.Fatalf("restored natural-description snapshot = %#v err=%v", validated, err)
	}
	otherSnapshot, err := actor.ensureReviewSnapshot(description, "other.go")
	if err != nil {
		t.Fatalf("capture differently scoped natural-description snapshot: %v", err)
	}
	if otherSnapshot == nil || !slices.Equal(otherSnapshot.Files, []string{"other.go"}) || otherSnapshot.Hash == snapshot.Hash {
		t.Fatalf("differently scoped snapshot reused prior scope: %#v", otherSnapshot)
	}
}

func TestNeoReviewSnapshotRejectsOversizedPersistedDiff(t *testing.T) {
	diff := strings.Repeat("x", neoReviewMaxChangedBytes+1)
	state := neoReviewSnapshotState(&neoReviewDiffSnapshot{
		Hash:  neoReviewSnapshotHash([]string{"large.min.js"}, map[string]string{"large.min.js": diff}),
		Files: []string{"large.min.js"},
		Diffs: map[string]string{"large.min.js": diff},
	}, "uncommitted changes", "M-review", nil, nil)
	if _, err := neoReviewSnapshotFromState(state, "uncommitted changes", "M-review", nil); err == nil || !strings.Contains(err.Error(), "byte limit") {
		t.Fatalf("oversized persisted review snapshot error = %v", err)
	}
}

func TestNeoReviewGitDiffPathspecSupportsQuotedFilenames(t *testing.T) {
	for _, description := range []string{`git diff -- "src/my file.go"`, `git diff -- 'src/my file.go'`, `git diff -- src/my\ file.go`, `git diff HEAD -- "src/my file.go"`} {
		if pathspec, exact := neoReviewGitDiffPathspec(description); !exact || !slices.Equal(pathspec, []string{"src/my file.go"}) {
			t.Fatalf("neoReviewGitDiffPathspec(%q) = %#v, %v", description, pathspec, exact)
		}
	}
	for _, description := range []string{`git diff -- "src/unfinished.go`, `git diff -- src/trailing\`, `git diff --`, `git diff HEAD --`, `Git diff`, `git Diff`, `GIT DIFF`, `git diff -- tracked.go && cat secret`, `git diff -- tracked.go; cat secret`, `git diff -- tracked.go | cat`} {
		if pathspec, exact := neoReviewGitDiffPathspec(description); exact || pathspec != nil {
			t.Fatalf("neoReviewGitDiffPathspec(%q) = %#v, %v; want malformed input rejected", description, pathspec, exact)
		}
	}
	for description, want := range map[string]string{
		`git diff -- "src\windows\file.go"`:            `src\windows\file.go`,
		"git diff -- \"src/escaped\\$name\\`file.go\"": "src/escaped$name`file.go",
		`git diff -- "src/quote\"slash\\file.go"`:      `src/quote"slash\file.go`,
		`git diff -- "src/and&&name.go"`:               `src/and&&name.go`,
		`git diff HEAD -- --notes.txt`:                 `--notes.txt`,
	} {
		if pathspec, exact := neoReviewGitDiffPathspec(description); !exact || !slices.Equal(pathspec, []string{want}) {
			t.Fatalf("neoReviewGitDiffPathspec(%q) = %#v, %v; want %q", description, pathspec, exact, want)
		}
	}
	if !neoReviewPathValid(`src\windows\file.go`) {
		t.Fatal("valid Git filename containing a backslash was rejected")
	}
	for _, invalid := range []string{`C:\repo\file.go`, `C:secret.go`, `..\secret.go`, `src\..\secret.go`, `src\.\file.go`, `\rooted.go`} {
		if neoReviewPathValid(invalid) {
			t.Fatalf("platform-ambiguous Git filename %q was accepted", invalid)
		}
	}
	if neoReviewPathValid(string([]byte{'b', 'a', 'd', 0xff})) {
		t.Fatal("invalid UTF-8 Git filename crossed the JSON review boundary")
	}
}

func TestNeoReviewDiffDescriptionPreservesEscapedTrailingWhitespace(t *testing.T) {
	description := "git diff -- edge.go\\ "
	history := []neoHistoryMessage{{Role: "user", Text: "Review this diff: " + description}}
	if got := neoReviewDiffDescriptionFromHistory(history); got != description {
		t.Fatalf("review diff description = %q, want %q", got, description)
	}
	if pathspec, exact := neoReviewGitDiffPathspec(description); !exact || !slices.Equal(pathspec, []string{"edge.go "}) {
		t.Fatalf("review diff pathspec = %#v, %v; want literal trailing whitespace", pathspec, exact)
	}
}

func TestNeoNormalizeRunCheckErrorRequiresMessage(t *testing.T) {
	input := map[string]any{"checkName": "strict-error"}
	if _, err := neoNormalizeRunCheckResult(input, map[string]any{"status": "error", "issues": []any{}}); err == nil || !strings.Contains(err.Error(), "errorMessage") {
		t.Fatalf("missing run_check errorMessage error = %v", err)
	}
	normalized, err := neoNormalizeRunCheckResult(input, map[string]any{"status": "error", "issues": []any{}, "errorMessage": "check failed"})
	if err != nil || stringValue(normalized["errorMessage"]) != "check failed" {
		t.Fatalf("valid run_check error = %#v, %v", normalized, err)
	}
}

func TestNeoNormalizeRunCheckCompletedRequiresEvidence(t *testing.T) {
	input := map[string]any{"checkName": "evidence-contract"}
	if _, err := neoNormalizeRunCheckResult(input, map[string]any{"status": "completed", "issues": []any{}}); err == nil || !strings.Contains(err.Error(), "patternsChecked") {
		t.Fatalf("missing run_check patterns error = %v", err)
	}
	if _, err := neoNormalizeRunCheckResult(input, map[string]any{
		"status":          "completed",
		"patternsChecked": []any{"dependency floor"},
		"issues":          []any{},
	}); err == nil || !strings.Contains(err.Error(), "evidence") {
		t.Fatalf("missing run_check evidence error = %v", err)
	}
	if _, err := neoNormalizeRunCheckResult(input, map[string]any{
		"status":          "completed",
		"patternsChecked": []any{"dependency floor"},
		"evidence": []any{map[string]any{
			"patternIndex": 0,
			"observation":  "The floor lacks the direct resource path.",
			"sources":      []any{"package@1.0.0/client.py"},
			"outcome":      "finding",
		}},
		"issues": []any{},
	}); err == nil || !strings.Contains(err.Error(), "issueIndexes") {
		t.Fatalf("unsupported clean run_check evidence error = %v", err)
	}
	if _, err := neoNormalizeRunCheckResult(input, map[string]any{
		"status":          "completed",
		"patternsChecked": []any{"dependency floor"},
		"evidence": []any{map[string]any{
			"patternIndex": 0,
			"observation":  "The floor exposes beta.responses only.",
			"sources":      []any{"package.json:24", "package@1.0.0/client.py"},
			"outcome":      "no-finding",
			"issueIndexes": []any{0},
		}},
		"issues": []any{map[string]any{
			"severity": "high",
			"file":     "wrapper.py",
			"problem":  "The wrapper accesses a resource absent at the published floor.",
		}},
	}); err == nil || !strings.Contains(err.Error(), "cannot reference") {
		t.Fatalf("contradictory run_check evidence error = %v", err)
	}
	normalized, err := neoNormalizeRunCheckResult(input, map[string]any{
		"status":          "completed",
		"patternsChecked": []any{"dependency floor", "private package exclusion"},
		"evidence": []any{
			map[string]any{"patternIndex": 1, "observation": "The package is published.", "sources": []any{"package.json:2"}, "outcome": "not-applicable"},
			map[string]any{"patternIndex": 0, "observation": "The floor exposes beta.responses only.", "sources": []any{"package.json:24", "package@1.0.0/client.py"}, "outcome": "finding", "issueIndexes": []any{0}},
		},
		"issues": []any{map[string]any{
			"severity": "high",
			"file":     "wrapper.py",
			"problem":  "The wrapper accesses a resource absent at the published floor.",
		}},
	})
	if err != nil || len(arrayValue(normalized["evidence"])) != 2 || len(arrayValue(normalized["issues"])) != 1 {
		t.Fatalf("valid run_check evidence = %#v, %v", normalized, err)
	}
}

func TestNeoNormalizePublishedDependencyCapabilityEvidence(t *testing.T) {
	input := map[string]any{"checkName": "published-dependency-capability-floor"}
	result := func(patterns []any, evidence []any) map[string]any {
		return map[string]any{
			"status":          "completed",
			"patternsChecked": patterns,
			"evidence":        evidence,
			"issues":          []any{},
		}
	}
	entry := func(patternIndex int, dependency, floorVersion, accessPath, verification string) map[string]any {
		return map[string]any{
			"patternIndex": patternIndex,
			"observation":  "The exact floor exposes the complete root-owner capability path.",
			"sources":      []any{"exact package archive root-client declaration"},
			"outcome":      "no-finding",
			"issueIndexes": []any{},
			"dependency":   dependency,
			"floorVersion": floorVersion,
			"accessPath":   accessPath,
			"verification": verification,
			"rootEvidence": "exact root client type declaration text",
		}
	}

	if _, err := neoNormalizeRunCheckResult(input, result(
		[]any{"TypeScript root client"},
		[]any{map[string]any{
			"patternIndex": 0,
			"observation":  "An adjacent Responses class exists.",
			"sources":      []any{"responses.ts"},
			"outcome":      "no-finding",
			"issueIndexes": []any{},
		}},
	)); err == nil || !strings.Contains(err.Error(), "requires dependency") {
		t.Fatalf("missing dependency-floor fields error = %v", err)
	}

	if _, err := neoNormalizeRunCheckResult(input, result(
		[]any{"TypeScript root client"},
		[]any{entry(0, "@openrouter/sdk", "1.0.0", "Responses", "exact-export-inspection")},
	)); err == nil || !strings.Contains(err.Error(), "root owner") {
		t.Fatalf("adjacent-class access path was accepted: %v", err)
	}

	if _, err := neoNormalizeRunCheckResult(input, result(
		[]any{"TypeScript root client"},
		[]any{entry(0, "@openrouter/sdk", "1.0.0", "OpenRouter.responses", "adjacent-class-inspection")},
	)); err == nil || !strings.Contains(err.Error(), "invalid verification") {
		t.Fatalf("invalid dependency-floor verification was accepted: %v", err)
	}

	normalized, err := neoNormalizeRunCheckResult(input, result(
		[]any{"TypeScript root client", "Python root client"},
		[]any{
			entry(0, "@openrouter/sdk", "1.0.0", "OpenRouter.responses", "root-type-declaration"),
			entry(1, "openrouter", "1.0.0", "OpenRouter.responses", "root-source-construction"),
		},
	))
	if err != nil || len(arrayValue(normalized["evidence"])) != 2 {
		t.Fatalf("separate TypeScript/Python owner evidence = %#v, %v", normalized, err)
	}

	if _, err := neoNormalizeRunCheckResult(input, result(
		[]any{"first TypeScript root traversal", "duplicate TypeScript root traversal"},
		[]any{
			entry(0, "@openrouter/sdk", "1.0.0", "OpenRouter.responses", "root-type-declaration"),
			entry(1, "@openrouter/sdk", "1.0.0", "OpenRouter.responses", "root-type-declaration"),
		},
	)); err == nil || !strings.Contains(err.Error(), "duplicates capability path") {
		t.Fatalf("duplicate dependency-floor owner evidence was accepted: %v", err)
	}
}

func TestNeoPreparedSubagentToolsDoNotPublishAfterGenerationChange(t *testing.T) {
	actor := newNeoActor(newNeoRuntime(&config.Config{}), "actor-stale-tools", "thread-actor", "T-stale-tools", "T-stale-tools", neoActorRecord("actor-stale-tools", "thread-actor", "T-stale-tools"), nil)
	generation := actor.generation
	actor.mu.Lock()
	actor.advanceGenerationLocked()
	actor.mu.Unlock()
	exchanges := actor.execSubagentTurnTools([]neoToolCall{{ID: "TU-stale", Name: "Read", Input: map[string]any{"path": "stale.go"}}}, "TU-parent", generation, 0, "smart", "")
	if len(exchanges) != 0 || actor.storeSubagentToolResultMessageForGeneration("TU-stale", map[string]any{"status": "done"}, "TU-parent", "", generation) {
		t.Fatalf("stale subagent tools were published: %#v", exchanges)
	}
	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.messages) != 0 {
		t.Fatalf("stale subagent messages = %#v", actor.messages)
	}
}

func TestNeoReviewSplitGitDiffPatches(t *testing.T) {
	diff := "diff --git a/a.go b/a.go\n--- a/a.go\n+++ b/a.go\n@@ -1 +1 @@\n-old\n+new\ndiff --git a/b.go b/b.go\n--- a/b.go\n+++ b/b.go\n@@ -1 +1 @@\n-old\n+diff --git remains content"
	patches := neoReviewSplitGitDiffPatches(diff)
	if len(patches) != 2 || !strings.Contains(patches[0], "+new") || !strings.Contains(patches[1], "+diff --git remains content") {
		t.Fatalf("split review patches = %#v", patches)
	}
}

func TestNeoGitOutputBufferBoundsCapture(t *testing.T) {
	buffer := neoGitOutputBuffer{limit: 4}
	if written, err := buffer.Write([]byte("oversized")); err != nil || written != len("oversized") {
		t.Fatalf("bounded writer = %d, %v", written, err)
	}
	if output := buffer.String(); output != "over" {
		t.Fatalf("bounded output = %q, want first four bytes", output)
	}
}

func TestNeoReviewFilesFromHistory(t *testing.T) {
	history := []neoHistoryMessage{{Role: "user", Text: strings.Join([]string{
		"Repository type detected: Git.",
		"Review this diff: uncommitted changes",
		"Focus on these files:",
		"cmd/amp_binary_audit/main.go",
		" internal/api/modules/amp/neo_builtin_tools.go ",
		"Additional instructions from the user:",
		"Final blocker-only review.",
	}, "\n")}}
	if files := neoReviewFilesFromHistory(history); !slices.Equal(files, []string{"cmd/amp_binary_audit/main.go", " internal/api/modules/amp/neo_builtin_tools.go "}) {
		t.Fatalf("review files = %#v", files)
	}
}

func TestNeoCaptureReviewWorkingTreeSnapshotForFiles(t *testing.T) {
	repository := t.TempDir()
	runGit := func(args ...string) {
		t.Helper()
		command := exec.Command("git", args...)
		command.Dir = repository
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, output)
		}
	}
	runGit("init")
	runGit("config", "user.email", "test@example.test")
	runGit("config", "user.name", "Test User")
	runGit("config", "commit.gpgsign", "false")
	for _, filename := range []string{"focus.go", "other.go"} {
		if err := os.WriteFile(filepath.Join(repository, filename), []byte("package sample\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	runGit("add", ".")
	runGit("commit", "-m", "initial")
	for _, filename := range []string{"focus.go", "other.go"} {
		if err := os.WriteFile(filepath.Join(repository, filename), []byte("package sample\n\nconst changed = true\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(repository, "new.go"), []byte("package sample\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit("add", "new.go")
	if err := os.WriteFile(filepath.Join(repository, "untracked.go"), []byte("package sample\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	snapshot, err := neoCaptureReviewWorkingTreeSnapshotForFiles(repository, []string{"focus.go", "new.go", "untracked.go"})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(snapshot.Files, []string{"focus.go", "new.go", "untracked.go"}) {
		t.Fatalf("focused snapshot files = %#v", snapshot.Files)
	}
	if _, exists := snapshot.Diffs["other.go"]; exists {
		t.Fatalf("focused snapshot included unrelated file: %#v", snapshot.Diffs)
	}
	actor := newNeoActor(newNeoRuntime(&config.Config{}), "actor-focused-run-check", "thread-actor", "T-focused-run-check", "T-focused-run-check", neoActorRecord("actor-focused-run-check", "thread-actor", "T-focused-run-check"), nil)
	actor.environment = map[string]any{"workingDirectory": repository, "workspaceRoot": repository}
	actor.messages = append(actor.messages, neoMessage{MessageID: "M-focused-run-check", Role: "user"})
	if _, err := actor.ensureReviewSnapshot("uncommitted changes", "focus.go", "new.go", "other.go", "untracked.go"); err != nil {
		t.Fatalf("prepare review-level snapshot: %v", err)
	}
	prepared, err := actor.prepareRunCheckInput(map[string]any{
		"checkName":       "focused-review",
		"diffDescription": "uncommitted changes",
		"files":           []any{"focus.go"},
	})
	if err != nil {
		t.Fatalf("prepare focused run_check snapshot: %v", err)
	}
	if files := neoStringSlice(prepared[neoReviewSnapshotFilesKey]); !slices.Equal(files, []string{"focus.go"}) {
		t.Fatalf("focused run_check snapshot files = %#v", files)
	}
	if packet := stringValue(prepared[neoReviewSnapshotTextKey]); !strings.Contains(packet, "focus.go") || strings.Contains(packet, "other.go") || strings.Contains(packet, "new.go") {
		t.Fatalf("focused run_check snapshot packet = %q", packet)
	}
	if actor.reviewSnapshot == nil || !slices.Equal(actor.reviewSnapshot.Files, []string{"focus.go", "new.go", "other.go", "untracked.go"}) {
		t.Fatalf("review-level snapshot was narrowed by focused run_check: %#v", actor.reviewSnapshot)
	}
}

func TestNeoCaptureReviewWorkingTreeSnapshotTreatsFocusedFilenamesLiterally(t *testing.T) {
	repository := t.TempDir()
	runGit := func(args ...string) {
		t.Helper()
		command := exec.Command("git", args...)
		command.Dir = repository
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, output)
		}
	}
	runGit("init")
	runGit("config", "user.email", "test@example.test")
	runGit("config", "user.name", "Test User")
	runGit("config", "commit.gpgsign", "false")
	magic := `:(glob)*.go`
	for _, filename := range []string{magic, "other.go"} {
		if err := os.WriteFile(filepath.Join(repository, filename), []byte("package sample\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	runGit("--literal-pathspecs", "add", "--", magic, "other.go")
	runGit("commit", "-m", "initial")
	for _, filename := range []string{magic, "other.go"} {
		if err := os.WriteFile(filepath.Join(repository, filename), []byte("package sample\n\nconst changed = true\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	snapshot, err := neoCaptureReviewWorkingTreeSnapshotForFiles(repository, []string{magic})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(snapshot.Files, []string{magic}) || snapshot.Diffs[magic] == "" {
		t.Fatalf("literal focused snapshot = %#v", snapshot)
	}
	exactSnapshot, err := neoCaptureWorkingTreeReviewSnapshot(repository, "git diff -- "+magic)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(exactSnapshot.Files, []string{magic}) || exactSnapshot.Diffs[magic] == "" {
		t.Fatalf("literal exact-command snapshot = %#v", exactSnapshot)
	}
}

func TestNeoCaptureReviewSnapshotPreservesRenamePathsAndMetadataRange(t *testing.T) {
	repository := t.TempDir()
	runGit := func(args ...string) {
		t.Helper()
		command := exec.Command("git", args...)
		command.Dir = repository
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, output)
		}
	}
	runGit("init")
	runGit("config", "user.email", "test@example.test")
	runGit("config", "user.name", "Test User")
	if err := os.WriteFile(filepath.Join(repository, "before.go"), []byte("package sample\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit("add", "before.go")
	runGit("commit", "-m", "initial")
	runGit("mv", "before.go", "after.go")

	snapshot, err := neoCaptureReviewWorkingTreeSnapshotForFiles(repository, []string{"after.go"})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(snapshot.Files, []string{"after.go"}) {
		t.Fatalf("rename snapshot files = %#v", snapshot.Files)
	}
	diff := snapshot.Diffs["after.go"]
	if !strings.Contains(diff, "rename from before.go") || !strings.Contains(diff, "rename to after.go") {
		t.Fatalf("rename snapshot did not preserve both paths: %q", diff)
	}
	prepared, err := neoPrepareRunCheckSnapshotInput(map[string]any{"checkName": "rename"}, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if got := neoStringSlice(prepared[neoReviewSnapshotZeroLineKey]); !slices.Equal(got, []string{"after.go"}) {
		t.Fatalf("rename zero-line files = %#v", got)
	}
	if err := neoValidateSubmittedReviewSnapshot(map[string]any{"comments": []any{map[string]any{"filename": "after.go", "startLine": 0, "endLine": 0}}}, snapshot); err != nil {
		t.Fatalf("rename zero-line review rejected: %v", err)
	}
}

func TestNeoCaptureReviewSnapshotPreservesNULDelimitedFilenameWhitespace(t *testing.T) {
	repository := t.TempDir()
	runGit := func(args ...string) {
		t.Helper()
		command := exec.Command("git", args...)
		command.Dir = repository
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, output)
		}
	}
	runGit("init")
	runGit("config", "user.email", "test@example.test")
	runGit("config", "user.name", "Test User")
	filename := " edge.go "
	if err := os.WriteFile(filepath.Join(repository, filename), []byte("package sample\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit("add", filename)
	runGit("commit", "-m", "initial")
	if err := os.WriteFile(filepath.Join(repository, filename), []byte("package sample\n\nconst changed = true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, capture := range []func() (*neoReviewDiffSnapshot, error){
		func() (*neoReviewDiffSnapshot, error) {
			return neoCaptureReviewWorkingTreeSnapshotForFiles(repository, []string{filename})
		},
		func() (*neoReviewDiffSnapshot, error) {
			return neoCaptureGitDiffReviewSnapshot(repository, []string{filename})
		},
	} {
		snapshot, err := capture()
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(snapshot.Files, []string{filename}) {
			t.Fatalf("snapshot filenames = %#v, want exact whitespace", snapshot.Files)
		}
	}
	if _, err := neoCaptureGitDiffReviewSnapshot(repository, []string{string([]byte{'b', 'a', 'd', 0xff})}); err == nil || !strings.Contains(err.Error(), "invalid path") {
		t.Fatalf("invalid UTF-8 review pathspec error = %v", err)
	}
}

func TestNeoCaptureReviewSnapshotIncludesIntentToAdd(t *testing.T) {
	repository := t.TempDir()
	runGit := func(args ...string) {
		t.Helper()
		command := exec.Command("git", args...)
		command.Dir = repository
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, output)
		}
	}
	runGit("init")
	runGit("config", "user.email", "test@example.test")
	runGit("config", "user.name", "Test User")
	if err := os.WriteFile(filepath.Join(repository, "base.go"), []byte("package sample\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit("add", "base.go")
	runGit("commit", "-m", "initial")
	if err := os.WriteFile(filepath.Join(repository, "intent.go"), []byte("package sample\n\nconst intent = true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit("add", "-N", "intent.go")

	snapshot, err := neoCaptureReviewWorkingTreeSnapshot(repository)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(snapshot.Files, []string{"intent.go"}) || !strings.Contains(snapshot.Diffs["intent.go"], "+const intent = true") {
		t.Fatalf("intent-to-add snapshot = %#v", snapshot)
	}
}

func TestNeoCaptureReviewSnapshotUsesCurrentWorktreeOnUnbornBranch(t *testing.T) {
	repository := t.TempDir()
	runGit := func(args ...string) {
		t.Helper()
		command := exec.Command("git", args...)
		command.Dir = repository
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, output)
		}
	}
	runGit("init")
	if err := os.WriteFile(filepath.Join(repository, "new.go"), []byte("package staged\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit("add", "new.go")
	if err := os.WriteFile(filepath.Join(repository, "new.go"), []byte("package worktree\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	snapshot, err := neoCaptureReviewWorkingTreeSnapshot(repository)
	if err != nil {
		t.Fatal(err)
	}
	diff := snapshot.Diffs["new.go"]
	if !slices.Equal(snapshot.Files, []string{"new.go"}) || strings.Count(diff, "diff --git ") != 1 || !strings.Contains(diff, "+package worktree") || strings.Contains(diff, "+package staged") {
		t.Fatalf("unborn worktree snapshot = %#v", snapshot)
	}
	headSnapshot, err := neoCaptureWorkingTreeReviewSnapshot(repository, "git diff HEAD -- new.go")
	if err != nil {
		t.Fatal(err)
	}
	headDiff := headSnapshot.Diffs["new.go"]
	if !slices.Equal(headSnapshot.Files, []string{"new.go"}) || strings.Count(headDiff, "diff --git ") != 1 || !strings.Contains(headDiff, "+package worktree") || strings.Contains(headDiff, "+package staged") {
		t.Fatalf("unborn git diff HEAD snapshot = %#v", headSnapshot)
	}
}

func TestNeoCaptureReviewSnapshotPreservesSupportedGitDiffForms(t *testing.T) {
	repository := t.TempDir()
	runGit := func(args ...string) {
		t.Helper()
		command := exec.Command("git", args...)
		command.Dir = repository
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, output)
		}
	}
	runGit("init")
	runGit("config", "user.email", "test@example.test")
	runGit("config", "user.name", "Test User")
	filename := filepath.Join(repository, "tracked.txt")
	if err := os.WriteFile(filename, []byte("base\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit("add", "tracked.txt")
	runGit("commit", "-m", "initial")
	if err := os.WriteFile(filename, []byte("staged\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit("add", "tracked.txt")
	if err := os.WriteFile(filename, []byte("unstaged\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	for _, description := range []string{"git diff", "git diff -- tracked.txt"} {
		snapshot, err := neoCaptureWorkingTreeReviewSnapshot(repository, description)
		if err != nil {
			t.Fatal(err)
		}
		diff := snapshot.Diffs["tracked.txt"]
		if !strings.Contains(diff, "-staged") || !strings.Contains(diff, "+unstaged") || strings.Contains(diff, "-base") {
			t.Fatalf("%s snapshot = %q", description, diff)
		}
	}
	headSnapshot, err := neoCaptureWorkingTreeReviewSnapshot(repository, "git diff HEAD -- tracked.txt")
	if err != nil {
		t.Fatal(err)
	}
	if diff := headSnapshot.Diffs["tracked.txt"]; !strings.Contains(diff, "-base") || !strings.Contains(diff, "+unstaged") {
		t.Fatalf("git diff HEAD snapshot = %q", diff)
	}
}

func TestNeoCaptureReviewSnapshotContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := neoCaptureReviewWorkingTreeSnapshotContext(ctx, t.TempDir()); !errors.Is(err, context.Canceled) {
		t.Fatalf("capture cancellation error = %v, want context canceled", err)
	}
}

func TestNeoReviewSnapshotCancellationIsNotCached(t *testing.T) {
	repository := t.TempDir()
	command := exec.Command("git", "init")
	command.Dir = repository
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("git init: %v\n%s", err, output)
	}
	actor := newNeoActor(newNeoRuntime(&config.Config{}), "actor-review-cancel", "thread-actor", "T-review-cancel", "T-review-cancel", neoActorRecord("actor-review-cancel", "thread-actor", "T-review-cancel"), nil)
	actor.environment = map[string]any{"workingDirectory": repository, "workspaceRoot": repository}
	actor.messages = append(actor.messages, neoMessage{MessageID: "M-review-cancel", Role: "user"})
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := actor.ensureReviewSnapshotContext(cancelled, "uncommitted changes"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled snapshot error = %v", err)
	}
	if _, err := actor.ensureReviewSnapshotContext(context.Background(), "uncommitted changes"); err != nil {
		t.Fatalf("snapshot retry retained cancellation: %v", err)
	}
}

func TestNeoReviewSnapshotCaptureFailureIsRetryable(t *testing.T) {
	repository := t.TempDir()
	actor := newNeoActor(newNeoRuntime(&config.Config{}), "actor-review-retry", "thread-actor", "T-review-retry", "T-review-retry", neoActorRecord("actor-review-retry", "thread-actor", "T-review-retry"), nil)
	actor.environment = map[string]any{"workingDirectory": repository, "workspaceRoot": repository}
	actor.messages = append(actor.messages, neoMessage{MessageID: "M-review-retry", Role: "user"})
	if _, err := actor.ensureReviewSnapshot("uncommitted changes"); err == nil {
		t.Fatal("snapshot capture outside a Git worktree did not fail")
	}
	command := exec.Command("git", "init")
	command.Dir = repository
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("git init: %v\n%s", err, output)
	}
	if snapshot, err := actor.ensureReviewSnapshot("uncommitted changes"); err != nil || snapshot == nil {
		t.Fatalf("snapshot retry after recoverable capture failure = %#v, %v", snapshot, err)
	}
}

func TestNeoCaptureReviewSnapshotIncludesEmptyUntrackedFile(t *testing.T) {
	repository := t.TempDir()
	command := exec.Command("git", "init")
	command.Dir = repository
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("git init: %v\n%s", err, output)
	}
	if err := os.WriteFile(filepath.Join(repository, "empty.marker"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	snapshot, err := neoCaptureReviewWorkingTreeSnapshot(repository)
	if err != nil {
		t.Fatal(err)
	}
	diff := snapshot.Diffs["empty.marker"]
	if !slices.Equal(snapshot.Files, []string{"empty.marker"}) || !strings.Contains(diff, "new file mode") || neoReviewDiffHasHunk(diff) {
		t.Fatalf("empty untracked snapshot = %#v", snapshot)
	}
	prepared, err := neoPrepareRunCheckSnapshotInput(map[string]any{"files": []any{"empty.marker"}}, snapshot)
	if err != nil || !slices.Equal(neoStringSlice(prepared[neoReviewSnapshotZeroLineKey]), []string{"empty.marker"}) {
		t.Fatalf("empty untracked zero-line snapshot = %#v, error=%v", prepared, err)
	}
}

func TestNeoCaptureReviewSnapshotRejectsConcurrentMutation(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("test uses a shell Git shim")
	}
	repository := t.TempDir()
	runGit := func(args ...string) {
		t.Helper()
		command := exec.Command("git", args...)
		command.Dir = repository
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, output)
		}
	}
	runGit("init")
	runGit("config", "user.email", "test@example.test")
	runGit("config", "user.name", "Test User")
	filename := filepath.Join(repository, "tracked.txt")
	if err := os.WriteFile(filename, []byte("base\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit("add", "tracked.txt")
	runGit("commit", "-m", "initial")
	if err := os.WriteFile(filename, []byte("first\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	realGit, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	binDir := t.TempDir()
	shim := filepath.Join(binDir, "git")
	script := `#!/bin/sh
is_diff=
has_head=
for arg in "$@"; do
  [ "$arg" = "diff" ] && is_diff=1
  [ "$arg" = "HEAD" ] && has_head=1
done
if [ -n "$is_diff" ] && [ -n "$has_head" ] && [ ! -e "$NEO_REVIEW_MUTATION_MARKER" ]; then
  "$NEO_REVIEW_REAL_GIT" "$@"
  result=$?
  printf 'second\n' > "$NEO_REVIEW_MUTATION_FILE"
  : > "$NEO_REVIEW_MUTATION_MARKER"
  exit "$result"
fi
exec "$NEO_REVIEW_REAL_GIT" "$@"
`
	if err := os.WriteFile(shim, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("NEO_REVIEW_REAL_GIT", realGit)
	t.Setenv("NEO_REVIEW_MUTATION_FILE", filename)
	t.Setenv("NEO_REVIEW_MUTATION_MARKER", filepath.Join(repository, "mutation-complete"))
	if _, err := neoCaptureReviewWorkingTreeSnapshot(repository); err == nil || !strings.Contains(err.Error(), "changed during capture") {
		t.Fatalf("concurrent mutation capture error = %v", err)
	}
}

func TestNeoCaptureReviewWorkingTreeSnapshotRejectsMoreThanHundredFiles(t *testing.T) {
	repository := t.TempDir()
	command := exec.Command("git", "init")
	command.Dir = repository
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("git init: %v\n%s", err, output)
	}
	for index := 0; index <= neoReviewMaxChangedFiles; index++ {
		filename := filepath.Join(repository, fmt.Sprintf("file-%03d.go", index))
		if err := os.WriteFile(filename, []byte("package sample\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := neoCaptureReviewWorkingTreeSnapshot(repository); err == nil || !strings.Contains(err.Error(), "more than 100 changed files") {
		t.Fatalf("oversized working-tree snapshot error = %v", err)
	}
	actor := newNeoActor(newNeoRuntime(&config.Config{}), "actor-focused-review", "thread-actor", "T-focused-review", "T-focused-review", neoActorRecord("actor-focused-review", "thread-actor", "T-focused-review"), nil)
	actor.environment = map[string]any{"workingDirectory": repository, "workspaceRoot": repository}
	actor.messages = append(actor.messages, neoMessage{MessageID: "M-focused-review", Role: "user"})
	_, err := actor.prepareRunCheckInput(map[string]any{
		"checkName":       "focused-review",
		"diffDescription": "uncommitted changes",
		"files":           []any{"file-000.go"},
	})
	if err != nil {
		t.Fatalf("focused run_check failed because of unrelated files: %v", err)
	}
	if actor.reviewSnapshot == nil || !slices.Equal(actor.reviewSnapshot.Files, []string{"file-000.go"}) {
		t.Fatalf("focused run_check snapshot = %#v", actor.reviewSnapshot)
	}
}

func TestNeoRunCheckSubagentHonorsFrontmatterTools(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-run-check-tools", "thread-actor", "T-run-check-tools", "T-run-check-tools", neoActorRecord("actor-run-check-tools", "thread-actor", "T-run-check-tools"), nil)
	actor.currentAgentMode = "review"
	actor.tools = map[string]neoToolSpec{
		"Read":          {Name: "Read"},
		"Grep":          {Name: "Grep"},
		"glob":          {Name: "glob"},
		"Bash":          {Name: "Bash"},
		"shell_command": {Name: "shell_command"},
		"apply_patch":   {Name: "apply_patch"},
	}

	var seen neoInferenceRequest
	rt.inferStream = func(_ *neoRuntime, req neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		seen = req
		return neoInferenceResult{Text: `{"status":"error","errorMessage":"tool fixture","issues":[]}`}, nil
	}

	_, err := actor.executeSubagentRun("run_check", map[string]any{
		"checkName":    "implementation-simplicity",
		"checkURI":     "file:///checks/implementation-simplicity.md",
		"checkContent": "Prefer simple implementations.",
		"frontmatter": map[string]any{
			"tools": []any{"Bash", "Grep", "Read", "apply_patch"},
		},
	}, "TU-run-check-tools", "M-1", actor.generation, 0, "")
	if err != nil {
		t.Fatalf("run_check subagent failed: %v", err)
	}
	if !neoSubagentHasTools(seen.Tools, "Bash", "Grep", "Read") || neoSubagentHasTools(seen.Tools, "glob", "shell_command") || len(seen.Tools) != 3 {
		t.Fatalf("run_check frontmatter tools = %#v, want exactly Bash, Grep, and Read", seen.Tools)
	}
}

func TestNeoRunCheckSubagentPreservesStructuredShellLeafResult(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-run-check-shell-result", "thread-actor", "T-run-check-shell-result", "T-run-check-shell-result", neoActorRecord("actor-run-check-shell-result", "thread-actor", "T-run-check-shell-result"), nil)
	actor.currentAgentMode = "review"
	actor.tools = map[string]neoToolSpec{
		"shell_command": {Name: "shell_command", InputSchema: map[string]any{"type": "object"}},
	}

	turn := 0
	rt.inferStream = func(_ *neoRuntime, req neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		turn++
		switch turn {
		case 1:
			return neoInferenceResult{ToolCalls: []neoToolCall{{ID: "provider-shell-call", Name: "shell_command", Input: map[string]any{"command": "go test ./..."}}}}, nil
		case 2:
			if len(req.History) == 0 {
				t.Fatal("run_check continuation omitted tool history")
			}
			leaf := req.History[len(req.History)-1]
			if leaf.ToolName != "shell_command" || !strings.Contains(leaf.Text, `"exitCode":1`) || !strings.Contains(leaf.Text, `"output":"No test files found"`) || strings.Contains(leaf.Text, "display only") {
				t.Fatalf("run_check shell evidence = %#v", leaf)
			}
			return neoInferenceResult{Text: `{"checkName":"structured-shell","status":"completed","patternsChecked":["shell result"],"evidence":[{"patternIndex":0,"observation":"The command exited nonzero with no tests found.","sources":["shell_command result"],"outcome":"no-finding","issueIndexes":[]}],"issues":[]}`}, nil
		default:
			t.Fatalf("run_check inference turns = %d, want two", turn)
			return neoInferenceResult{}, nil
		}
	}
	socket := &neoSocket{writeMessage: func(_ int, data []byte) error {
		var event map[string]any
		if json.Unmarshal(data, &event) == nil && stringValue(event["type"]) == "tool_lease" {
			toolCallID := stringValue(event["toolCallId"])
			go actor.routeSubagentLeafToolResult(toolCallID, map[string]any{
				"status": "done",
				"output": "display only",
				"result": map[string]any{"exitCode": 1, "output": "No test files found"},
			})
		}
		return nil
	}}
	actor.sockets[socket] = struct{}{}

	text, err := actor.executeSubagentRun("run_check", map[string]any{
		"checkName":    "structured-shell",
		"checkURI":     "file:///checks/structured-shell.md",
		"checkContent": "Inspect shell evidence.",
	}, "TU-run-check-shell", "M-parent", actor.generation, 0, "")
	if err != nil {
		t.Fatalf("run_check subagent failed: %v", err)
	}
	if turn != 2 || !strings.Contains(text, `"checkName":"structured-shell"`) {
		t.Fatalf("run_check result/turns = %q/%d", text, turn)
	}
}

func TestNeoRunCheckSubagentRepairsMalformedFinalResultOnce(t *testing.T) {
	valid := `{"checkName":"repair-check","status":"completed","patternsChecked":["repair contract"],"evidence":[{"patternIndex":0,"observation":"The repaired result satisfies the contract.","sources":["fixture"],"outcome":"no-finding","issueIndexes":[]}],"issues":[]}`
	for name, initial := range map[string]string{
		"prose plus JSON":  "Result follows:\n" + valid,
		"fenced JSON":      "```json\n" + valid + "\n```",
		"trailing content": valid + "\nDone.",
		"schema invalid":   `{"checkName":"repair-check","status":"completed","issues":[]}`,
		"malformed JSON":   `{"checkName":"repair-check","status":"completed","issues":[]`,
	} {
		t.Run(name, func(t *testing.T) {
			rt := newNeoRuntime(&config.Config{})
			actor := newNeoActor(rt, "actor-run-check-repair", "thread-actor", "T-run-check-repair", "T-run-check-repair", neoActorRecord("actor-run-check-repair", "thread-actor", "T-run-check-repair"), nil)
			turn := 0
			rt.inferStream = func(_ *neoRuntime, request neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
				turn++
				if turn == 1 {
					return neoInferenceResult{Text: initial}, nil
				}
				if turn != 2 {
					t.Fatalf("repair inference turns = %d, want exactly two", turn)
				}
				if len(request.Tools) != 0 {
					t.Fatalf("repair tools = %#v, want none", request.Tools)
				}
				history := neoHistoryTestText(request.History)
				for _, want := range []string{initial, "Your previous final result was rejected:", "Return exactly one pure JSON object now", "Do not use markdown fences", "patternsChecked", "evidence"} {
					if !strings.Contains(history, want) {
						t.Fatalf("repair history missing %q:\n%s", want, history)
					}
				}
				return neoInferenceResult{Text: valid}, nil
			}

			text, err := actor.executeSubagentRun("run_check", map[string]any{
				"checkName":    "repair-check",
				"checkContent": "Verify the repair contract.",
			}, "TU-run-check-repair", "M-parent", actor.generation, 0, "")
			if err != nil || turn != 2 {
				t.Fatalf("repaired run_check = %q, turns=%d, err=%v", text, turn, err)
			}
			if result, parseErr := neoParseRunCheckResult(map[string]any{"checkName": "repair-check"}, text); parseErr != nil || stringValue(result["status"]) != "completed" {
				t.Fatalf("repaired result = %#v, %v", result, parseErr)
			}
		})
	}
}

func TestNeoRunCheckSubagentRepairsEmptyFinalizationAfterSynthesis(t *testing.T) {
	valid := `{"checkName":"empty-repair","status":"completed","patternsChecked":["empty finalization"],"evidence":[{"patternIndex":0,"observation":"The continuation returned a structured result.","sources":["fixture"],"outcome":"no-finding","issueIndexes":[]}],"issues":[]}`
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-empty-run-check-repair", "thread-actor", "T-empty-run-check-repair", "T-empty-run-check-repair", neoActorRecord("actor-empty-run-check-repair", "thread-actor", "T-empty-run-check-repair"), nil)
	turn := 0
	rt.inferStream = func(_ *neoRuntime, request neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		turn++
		switch turn {
		case 1:
			return neoInferenceResult{}, nil
		case 2:
			if !strings.Contains(neoHistoryTestText(request.History), "Write your complete final answer now") {
				t.Fatalf("empty finalization did not request synthesis: %#v", request.History)
			}
			return neoInferenceResult{Text: "```json\n" + valid + "\n```"}, nil
		case 3:
			if len(request.Tools) != 0 || !strings.Contains(neoHistoryTestText(request.History), "Your previous final result was rejected:") {
				t.Fatalf("structured repair request = tools:%#v history:%#v", request.Tools, request.History)
			}
			return neoInferenceResult{Text: valid}, nil
		default:
			t.Fatalf("empty repair inference turns = %d, want three", turn)
			return neoInferenceResult{}, nil
		}
	}

	text, err := actor.executeSubagentRun("run_check", map[string]any{"checkName": "empty-repair", "checkContent": "Verify empty finalization recovery."}, "TU-empty-run-check-repair", "M-parent", actor.generation, 0, "")
	if err != nil || turn != 3 || text != valid {
		t.Fatalf("empty finalization repair = %q, turns=%d, err=%v", text, turn, err)
	}
}

func TestNeoRunCheckSubagentDoesNotRepeatFailedStructuredRepair(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-failed-run-check-repair", "thread-actor", "T-failed-run-check-repair", "T-failed-run-check-repair", neoActorRecord("actor-failed-run-check-repair", "thread-actor", "T-failed-run-check-repair"), nil)
	turn := 0
	rt.inferStream = func(_ *neoRuntime, _ neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		turn++
		return neoInferenceResult{Text: "not JSON"}, nil
	}

	_, err := actor.executeSubagentRun("run_check", map[string]any{"checkName": "failed-repair", "checkContent": "Verify bounded repair."}, "TU-failed-run-check-repair", "M-parent", actor.generation, 0, "")
	if err == nil || turn != 2 || !strings.Contains(err.Error(), "structured-output repair failed") || !strings.Contains(err.Error(), "initial result") || !strings.Contains(err.Error(), "repair result") {
		t.Fatalf("failed repair = turns:%d err:%v", turn, err)
	}
}

func TestNeoRunCheckRepairPromptProvidesValidSnapshotExample(t *testing.T) {
	input := map[string]any{
		"checkName":                      "published-dependency-capability-floor",
		neoReviewSnapshotHashKey:         "snapshot-hash",
		neoReviewSnapshotFilesKey:        []any{"packages/sdk/src/client.ts", "sdks/python/client.py"},
		neoReviewSnapshotHunksKey:        []any{"packages/sdk/src/client.ts@@+10,2", "sdks/python/client.py@@+20,2"},
		neoReviewSnapshotLinesKey:        []any{"packages/sdk/src/client.ts@@+10,2", "sdks/python/client.py@@+20,2"},
		neoReviewSnapshotDeletedLinesKey: []any{},
		neoReviewSnapshotDeletedKey:      []any{},
		neoReviewSnapshotZeroLineKey:     []any{},
	}
	prompt := neoRunCheckRepairPrompt(input, errors.New("malformed result"))
	const startMarker = "Valid completed example:\n"
	const endMarker = "\n\nFor a failed check instead"
	start := strings.Index(prompt, startMarker)
	end := strings.Index(prompt, endMarker)
	if start < 0 || end < 0 || end <= start {
		t.Fatalf("repair prompt omitted the completed example:\n%s", prompt)
	}
	example := prompt[start+len(startMarker) : end]
	normalized, err := neoParseRunCheckResult(input, example)
	if err != nil {
		t.Fatalf("repair prompt completed example is invalid: %v\n%s", err, example)
	}
	if numberFrom(normalized["filesAnalyzed"]) != 2 || len(arrayValue(normalized["coveredFiles"])) != 2 || len(arrayValue(normalized["coveredHunks"])) != 2 {
		t.Fatalf("repair prompt coverage = %#v", normalized)
	}
	evidence := mapValue(arrayValue(normalized["evidence"])[0])
	if stringValue(evidence["accessPath"]) != "rootOwner.requiredCapability" || stringValue(evidence["verification"]) != "root-type-declaration" {
		t.Fatalf("repair prompt dependency evidence = %#v", evidence)
	}
	if strings.Contains(prompt, "finding|no-finding|not-applicable") || strings.Contains(prompt, `"filesAnalyzed":0`) {
		t.Fatalf("repair prompt retained an invalid copyable example:\n%s", prompt)
	}
}

func TestNeoRunCheckSubagentHonorsExplicitEmptyFrontmatterTools(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-run-check-empty-tools", "thread-actor", "T-run-check-empty-tools", "T-run-check-empty-tools", neoActorRecord("actor-run-check-empty-tools", "thread-actor", "T-run-check-empty-tools"), nil)
	actor.currentAgentMode = "review"
	actor.tools = map[string]neoToolSpec{
		"Read": {Name: "Read"},
		"Grep": {Name: "Grep"},
	}

	var seen neoInferenceRequest
	rt.inferStream = func(_ *neoRuntime, req neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		seen = req
		return neoInferenceResult{Text: `{"status":"error","errorMessage":"tool fixture","issues":[]}`}, nil
	}

	repository := t.TempDir()
	command := exec.Command("git", "init")
	command.Dir = repository
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("git init: %v: %s", err, output)
	}
	checkDirectory := filepath.Join(repository, ".agents", "checks")
	if err := os.MkdirAll(checkDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	actor.environment = map[string]any{"workingDirectory": repository}
	checkPath := filepath.Join(checkDirectory, "no-tools.md")
	if err := os.WriteFile(checkPath, []byte("Evaluate without repository tools."), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := actor.executeSubagentRun("run_check", map[string]any{
		"checkName": "no-tools",
		"checkURI":  (&url.URL{Scheme: "file", Path: checkPath}).String(),
		"frontmatter": map[string]any{
			"tools": []any{},
		},
	}, "TU-run-check-empty-tools", "M-1", actor.generation, 0, "")
	if err != nil {
		t.Fatalf("run_check subagent failed: %v", err)
	}
	if len(seen.Tools) != 0 {
		t.Fatalf("run_check explicit empty frontmatter tools = %#v, want none", seen.Tools)
	}
	if len(seen.History) == 0 || !strings.Contains(seen.History[len(seen.History)-1].Text, "Evaluate without repository tools.") {
		t.Fatalf("run_check did not embed its server-loaded definition: %#v", seen.History)
	}
}

func TestNeoPrepareRunCheckDefinitionRejectsUntrustedFile(t *testing.T) {
	repository := t.TempDir()
	command := exec.Command("git", "init")
	command.Dir = repository
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("git init: %v: %s", err, output)
	}
	untrusted := filepath.Join(t.TempDir(), "secret.md")
	if err := os.WriteFile(untrusted, []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := neoPrepareRunCheckDefinition(map[string]any{
		"checkName": "outside",
		"checkURI":  (&url.URL{Scheme: "file", Path: untrusted}).String(),
	}, repository)
	if err == nil || !strings.Contains(err.Error(), "outside trusted check directories") {
		t.Fatalf("untrusted check definition error = %v", err)
	}
}

func TestNeoRunCheckFileURLPathNormalizesWindowsDrive(t *testing.T) {
	if got := neoRunCheckFileURLPath("/C:/Users/Amp/.config/agents/checks/review.md", "windows"); got != filepath.FromSlash("C:/Users/Amp/.config/agents/checks/review.md") {
		t.Fatalf("Windows file URL path = %q", got)
	}
	if got := neoRunCheckFileURLPath("/Users/amp/.config/agents/checks/review.md", "darwin"); got != filepath.FromSlash("/Users/amp/.config/agents/checks/review.md") {
		t.Fatalf("POSIX file URL path = %q", got)
	}
}

func TestNeoPrepareRunCheckDefinitionAcceptsSymlinkedUserCheckRoot(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	configDirectory := filepath.Join(home, ".config")
	if err := os.MkdirAll(configDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "agents")
	checkDirectory := filepath.Join(target, "checks")
	if err := os.MkdirAll(checkDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(configDirectory, "agents")); err != nil {
		t.Skipf("create user check symlink: %v", err)
	}
	checkPath := filepath.Join(configDirectory, "agents", "checks", "symlinked.md")
	if err := os.WriteFile(checkPath, []byte("Inspect symlinked checks."), 0o600); err != nil {
		t.Fatal(err)
	}
	prepared, err := neoPrepareRunCheckDefinition(map[string]any{
		"checkName": "symlinked",
		"checkURI":  (&url.URL{Scheme: "file", Path: checkPath}).String(),
	}, t.TempDir())
	if err != nil {
		t.Fatalf("symlinked user check rejected: %v", err)
	}
	if stringValue(prepared["checkContent"]) != "Inspect symlinked checks." {
		t.Fatalf("symlinked check content = %#v", prepared["checkContent"])
	}
}

func TestNeoRunCheckDefinitionErrorIsStructured(t *testing.T) {
	repository := t.TempDir()
	command := exec.Command("git", "init")
	command.Dir = repository
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("git init: %v: %s", err, output)
	}
	untrusted := filepath.Join(t.TempDir(), "secret.md")
	if err := os.WriteFile(untrusted, []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	actor := newNeoActor(newNeoRuntime(&config.Config{}), "actor-run-check-error", "thread-actor", "T-run-check-error", "T-run-check-error", neoActorRecord("actor-run-check-error", "thread-actor", "T-run-check-error"), nil)
	actor.environment = map[string]any{"workingDirectory": repository, "workspaceRoot": repository}
	parent := neoPendingTool{ID: "TU-run-check-error", Name: "run_check", Input: map[string]any{
		"checkName":       "outside",
		"checkURI":        (&url.URL{Scheme: "file", Path: untrusted}).String(),
		"diffDescription": "git diff --merge-base origin/HEAD HEAD",
	}}
	actor.pendingTools[parent.ID] = parent
	actor.executorReady = false
	actor.runSubagent(parent, actor.generation)
	last := actor.messages[len(actor.messages)-1]
	run := mapValue(mapValue(last.Content[0])["run"])
	result := mapValue(run["result"])
	if stringValue(run["status"]) != "error" || stringValue(mapValue(run["error"])["message"]) == "" || stringValue(result["checkName"]) != "outside" || stringValue(result["status"]) != "error" || len(arrayValue(result["issues"])) != 0 {
		t.Fatalf("structured run_check definition error = %#v", run)
	}
}

func TestNeoRunCheckSubagentStopCancelsAllInferences(t *testing.T) {
	for _, test := range []struct {
		name string
		stop func(*neoActor)
	}{
		{name: "client cancellation", stop: func(actor *neoActor) { actor.cancel() }},
		{name: "actor disposal", stop: func(actor *neoActor) { actor.dispose() }},
	} {
		t.Run(test.name, func(t *testing.T) {
			const runCount = 7
			rt := newNeoRuntime(&config.Config{})
			actor := newNeoActor(rt, "actor-run-check-stop", "thread-actor", "T-run-check-stop", "T-run-check-stop", neoActorRecord("actor-run-check-stop", "thread-actor", "T-run-check-stop"), nil)
			actor.currentAgentMode = "review"
			entered := make(chan context.Context, runCount)
			rt.inferStream = func(_ *neoRuntime, req neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
				entered <- req.Context
				<-req.Context.Done()
				return neoInferenceResult{}, req.Context.Err()
			}
			done := make(chan error, runCount)
			generation := actor.generation
			for index := 0; index < runCount; index++ {
				go func(index int) {
					_, err := actor.executeSubagentRun("run_check", map[string]any{"checkName": "stop-" + strconv.Itoa(index), "checkURI": "file:///checks/stop.md", "checkContent": "Cancellation check."}, "TU-run-check-stop-"+strconv.Itoa(index), "M-1", generation, 0, "")
					done <- err
				}(index)
			}
			contexts := make([]context.Context, 0, runCount)
			t.Cleanup(func() { actor.cancel() })
			for index := 0; index < runCount; index++ {
				select {
				case runContext := <-entered:
					contexts = append(contexts, runContext)
				case <-time.After(time.Second):
					t.Fatalf("run_check %d did not start", index)
				}
			}
			test.stop(actor)
			for index, runContext := range contexts {
				select {
				case <-runContext.Done():
				case <-time.After(time.Second):
					t.Fatalf("stopped run_check %d provider context remained active", index)
				}
			}
			for index := 0; index < runCount; index++ {
				select {
				case err := <-done:
					if err != nil {
						t.Fatalf("stopped run_check %d error = %v, want stale run to exit quietly", index, err)
					}
				case <-time.After(time.Second):
					t.Fatalf("stopped run_check %d did not return", index)
				}
			}
			actor.mu.Lock()
			defer actor.mu.Unlock()
			if len(actor.subagentRuns) != 0 {
				t.Fatalf("stopped run_checks left tracked runs: %#v", actor.subagentRuns)
			}
		})
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
	for _, name := range []string{"Read", "shell_command", "Grep", "glob", "edit_file", "skill"} {
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
	literalCheckContent := "    indented code\nline with spaces  \n"
	literalRunCheck := neoSubagentInputText("run_check", map[string]any{"checkName": "literal", "checkContent": literalCheckContent})
	if !strings.Contains(literalRunCheck, "<content>\n"+literalCheckContent+"\n</content>") {
		t.Fatalf("run_check content whitespace changed:\n%s", literalRunCheck)
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

func TestNeoOracleInitialTurnAttachesFiles(t *testing.T) {
	workingDirectory := t.TempDir()
	imageData := neoSubagentImageFixtures(t)["image/png"]
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-oracle-attachments", "thread-actor", "T-019f0000-0000-7000-8000-000000000001", "T-019f0000-0000-7000-8000-000000000001", neoActorRecord("actor-oracle-attachments", "thread-actor", "T-019f0000-0000-7000-8000-000000000001"), nil)
	actor.environment = map[string]any{"workingDirectory": workingDirectory, "workspaceRoot": workingDirectory}
	actor.executorBootstrapComplete = true

	readCounts := map[string]int{}
	routeErrors := make(chan string, 2)
	socket := &neoSocket{writeMessage: func(_ int, data []byte) error {
		var event map[string]any
		if err := json.Unmarshal(data, &event); err != nil || stringValue(event["type"]) != "tool_lease" {
			return nil
		}
		path := stringValue(mapValue(event["args"])["path"])
		readCounts[path]++
		run := map[string]any{"status": "done", "result": "1: export default function Page() {}\n2: "}
		if filepath.Ext(path) == ".png" {
			run["result"] = map[string]any{
				"absolutePath": path,
				"content":      imageData,
				"isImage":      true,
				"imageInfo":    map[string]any{"mimeType": "image/png"},
			}
		}
		toolCallID := stringValue(event["toolCallId"])
		go func() {
			if !actor.routeSubagentLeafToolResult(toolCallID, run) {
				routeErrors <- path
			}
		}()
		return nil
	}}
	actor.sockets[socket] = struct{}{}

	var request neoInferenceRequest
	providerCalls := 0
	rt.inferStream = func(_ *neoRuntime, req neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		providerCalls++
		request = req
		return neoInferenceResult{Text: "oracle attachment review complete"}, nil
	}

	_, err := actor.executeSubagentRun("oracle", map[string]any{
		"task":  "review the screenshot and source",
		"files": []any{"shot.png", filepath.Join("app", "page.tsx")},
	}, "TU-oracle-attachments", "M-oracle-attachments", actor.generation, 0, "")
	if err != nil {
		t.Fatalf("oracle attachments: %v", err)
	}
	select {
	case path := <-routeErrors:
		t.Fatalf("oracle attachment result for %q was not routed", path)
	default:
	}
	if providerCalls != 1 || len(request.History) != 1 {
		t.Fatalf("oracle provider calls/history = %d/%d, want 1/1", providerCalls, len(request.History))
	}
	initial := request.History[0]
	for _, want := range []string{
		"Parent thread: " + actor.threadID,
		"You can use the read_thread tool with this ID",
		"# Attached Files",
		"This is an image file (image/png, 0 KB)",
		"```page.tsx\n1: export default function Page() {}",
	} {
		if !strings.Contains(initial.Text, want) {
			t.Fatalf("oracle initial text missing %q:\n%s", want, initial.Text)
		}
	}
	if strings.Contains(initial.Text, imageData) {
		t.Fatal("oracle initial text contains raw image base64")
	}
	if len(initial.Content) != 2 || stringValue(mapValue(initial.Content[0])["text"]) != initial.Text {
		t.Fatalf("oracle initial content = %#v", initial.Content)
	}
	image := mapValue(initial.Content[1])
	source := mapValue(image["source"])
	if stringValue(image["type"]) != "image" || stringValue(source["type"]) != "base64" || stringValue(source["mediaType"]) != "image/png" || stringValue(source["data"]) != imageData {
		t.Fatalf("oracle image block = %#v", image)
	}
	for _, relativePath := range []string{"shot.png", filepath.Join("app", "page.tsx")} {
		absolutePath := filepath.Join(workingDirectory, relativePath)
		if readCounts[absolutePath] != 1 {
			t.Fatalf("executor reads for %q = %d, want 1", absolutePath, readCounts[absolutePath])
		}
	}
}

func neoSubagentImageFixtures(t *testing.T) map[string]string {
	t.Helper()
	var jpegData bytes.Buffer
	if err := jpeg.Encode(&jpegData, image.NewRGBA(image.Rect(0, 0, 1, 1)), nil); err != nil {
		t.Fatal(err)
	}
	paletted := image.NewPaletted(image.Rect(0, 0, 1, 1), color.Palette{color.Black, color.White})
	var gifData bytes.Buffer
	if err := gif.Encode(&gifData, paletted, nil); err != nil {
		t.Fatal(err)
	}
	return map[string]string{
		"image/jpeg": base64.StdEncoding.EncodeToString(jpegData.Bytes()),
		"image/png":  testNeoPNGBase64(t, 1, 1),
		"image/gif":  base64.StdEncoding.EncodeToString(gifData.Bytes()),
		"image/webp": base64.StdEncoding.EncodeToString(testNeoAnimatedWebP(t, 0, 1)),
	}
}

func TestNeoOracleAttachmentImageValidation(t *testing.T) {
	attachment := func(mediaType, data string) (map[string]any, map[string]any) {
		return neoSubagentFileAttachment("/tmp/image", map[string]any{
			"status": "done",
			"result": map[string]any{
				"absolutePath": "/tmp/image",
				"content":      data,
				"isImage":      true,
				"imageInfo":    map[string]any{"mimeType": mediaType},
			},
		})
	}

	images := neoSubagentImageFixtures(t)
	for _, mediaType := range []string{"image/jpeg", "image/png", "image/gif", "image/webp"} {
		t.Run(mediaType, func(t *testing.T) {
			mention, image := attachment(mediaType, images[mediaType])
			if !boolValue(mention["isImage"]) || len(image) == 0 || stringValue(mapValue(image["source"])["mediaType"]) != mediaType {
				t.Fatalf("valid %s attachment = mention %#v image %#v", mediaType, mention, image)
			}
		})
	}
	_, dataURLImage := attachment("image/png", "data:image/png;base64,"+images["image/png"])
	if stringValue(mapValue(dataURLImage["source"])["data"]) != images["image/png"] {
		t.Fatalf("data URL image was not normalized: %#v", dataURLImage)
	}
	unsupportedMention, unsupportedImage := attachment("image/png", "data:image/svg+xml;base64,"+images["image/png"])
	if len(unsupportedImage) != 0 || !strings.Contains(stringValue(unsupportedMention["content"]), "unsupported media type image/svg+xml") {
		t.Fatalf("conflicting unsupported data URL = mention %#v image %#v", unsupportedMention, unsupportedImage)
	}
	conflictingMention, conflictingImage := attachment("image/png", "data:image/jpeg;base64,"+images["image/png"])
	if len(conflictingImage) != 0 || !strings.Contains(stringValue(conflictingMention["content"]), "conflicting media types") {
		t.Fatalf("conflicting supported data URL = mention %#v image %#v", conflictingMention, conflictingImage)
	}
	_, contentURLImage := neoSubagentFileAttachment("/tmp/image", map[string]any{
		"status": "done",
		"result": map[string]any{
			"absolutePath": "/tmp/image",
			"contentURL":   "data:image/webp;base64," + images["image/webp"],
			"isImage":      true,
		},
	})
	contentURLSource := mapValue(contentURLImage["source"])
	if stringValue(contentURLSource["mediaType"]) != "image/webp" || stringValue(contentURLSource["data"]) != images["image/webp"] {
		t.Fatalf("content URL image was not normalized: %#v", contentURLImage)
	}
	roundedMetadata := neoFileMentionsText(map[string]any{"files": []any{map[string]any{
		"uri":       "file:///tmp/image.png",
		"isImage":   true,
		"imageInfo": map[string]any{"mimeType": "image/png", "size": 1536},
	}}})
	if !strings.Contains(roundedMetadata, "image/png, 2 KB") {
		t.Fatalf("rounded image metadata = %q, want 2 KB", roundedMetadata)
	}

	exactLimitRaw, err := base64.StdEncoding.DecodeString(images["image/png"])
	if err != nil {
		t.Fatal(err)
	}
	exactLimitRaw = append(exactLimitRaw, make([]byte, neoSubagentAttachmentMaxImageBytes-len(exactLimitRaw))...)
	exactLimit := base64.StdEncoding.EncodeToString(exactLimitRaw)
	mention, image := attachment("image/png", exactLimit)
	if len(image) == 0 || numberFrom(mapValue(mention["imageInfo"])["size"]) != neoSubagentAttachmentMaxImageBytes {
		t.Fatalf("exact 4 MiB image was rejected: mention %#v image=%t", mention, len(image) > 0)
	}
	overLimit := base64.StdEncoding.EncodeToString(make([]byte, neoSubagentAttachmentMaxImageBytes+1))
	for name, test := range map[string]struct {
		mediaType string
		data      string
		want      string
	}{
		"unsupported media": {mediaType: "image/svg+xml", data: images["image/png"], want: "unsupported media type"},
		"mismatched media":  {mediaType: "image/jpeg", data: images["image/png"], want: "does not match image data"},
		"non-image bytes":   {mediaType: "image/png", data: base64.StdEncoding.EncodeToString([]byte("image")), want: "invalid image content"},
		"invalid base64":    {mediaType: "image/png", data: "%%%", want: "invalid or empty base64"},
		"empty base64":      {mediaType: "image/png", want: "invalid or empty base64"},
		"over 4 MiB":        {mediaType: "image/png", data: overLimit, want: "exceeds the 4 MiB limit"},
	} {
		t.Run(name, func(t *testing.T) {
			mention, image := attachment(test.mediaType, test.data)
			if len(image) != 0 || !strings.Contains(stringValue(mention["content"]), test.want) {
				t.Fatalf("invalid image attachment = mention %#v image %#v", mention, image)
			}
		})
	}
	failedMention, failedImage := neoSubagentFileAttachment("/tmp/failed.txt", map[string]any{
		"status": "error",
		"error":  map[string]any{"message": "permission denied"},
	})
	if len(failedImage) != 0 || stringValue(failedMention["content"]) != "Read did not complete (error): permission denied" || !strings.Contains(stringValue(failedMention["uri"]), "failed.txt") {
		t.Fatalf("failed Read attachment was omitted: mention %#v image %#v", failedMention, failedImage)
	}
}

func TestNeoOracleProcessesEveryRequestedAttachment(t *testing.T) {
	imageData := neoSubagentImageFixtures(t)["image/png"]
	actor := newNeoActor(newNeoRuntime(&config.Config{}), "actor-oracle-limit", "thread-actor", "T-oracle-limit", "T-oracle-limit", neoActorRecord("actor-oracle-limit", "thread-actor", "T-oracle-limit"), nil)
	actor.executorBootstrapComplete = true
	files := []string{" one.png ", "two.png", "three.png", "four.png", "five.png", "six.png", "notes.txt"}
	readCount := 0
	firstReadPath := ""
	socket := &neoSocket{writeMessage: func(_ int, data []byte) error {
		var event map[string]any
		if err := json.Unmarshal(data, &event); err != nil || stringValue(event["type"]) != "tool_lease" {
			return nil
		}
		readCount++
		currentRead := readCount
		if currentRead == 1 {
			firstReadPath = stringValue(mapValue(event["args"])["path"])
		}
		toolCallID := stringValue(event["toolCallId"])
		go func() {
			result := map[string]any{"content": imageData, "isImage": true, "imageInfo": map[string]any{"mimeType": "image/png"}}
			if currentRead == len(files) {
				result = map[string]any{"content": "text-after-images", "isImage": false}
			}
			actor.routeSubagentLeafToolResult(toolCallID, map[string]any{"status": "done", "result": result})
		}()
		return nil
	}}
	actor.sockets[socket] = struct{}{}
	attachments := actor.readSubagentFiles(context.Background(), files, "/tmp", "", "TU-parent", "M-parent", actor.generation)
	if readCount != len(files) || len(attachments.Images) != len(files)-1 {
		t.Fatalf("Oracle attachment reads/images = %d/%d, want %d/%d", readCount, len(attachments.Images), len(files), len(files)-1)
	}
	if firstReadPath != filepath.Join("/tmp", files[0]) {
		t.Fatalf("first Oracle attachment path = %q, want literal %q", firstReadPath, filepath.Join("/tmp", files[0]))
	}
	if !strings.Contains(attachments.Text, "text-after-images") {
		t.Fatalf("Oracle text attachment after images missing: %q", attachments.Text)
	}
}

func TestNeoOracleHydratesTrustedURLBackedAttachment(t *testing.T) {
	oldDataDir := neoAmpDataDir
	dataDir := t.TempDir()
	neoAmpDataDir = func() string { return dataDir }
	t.Cleanup(func() { neoAmpDataDir = oldDataDir })
	imageData := neoSubagentImageFixtures(t)["image/png"]
	raw, err := base64.StdEncoding.DecodeString(imageData)
	if err != nil {
		t.Fatal(err)
	}
	const origin = "http://127.0.0.1:8317"
	id, err := writeNeoLocalAttachment(raw, "image/png", origin)
	if err != nil {
		t.Fatal(err)
	}
	actor := newNeoActor(newNeoRuntime(&config.Config{}), "actor-oracle-url", "thread-actor", "T-oracle-url", "T-oracle-url", neoActorRecord("actor-oracle-url", "thread-actor", "T-oracle-url"), nil)
	actor.executorBootstrapComplete = true
	socket := &neoSocket{writeMessage: func(_ int, data []byte) error {
		var event map[string]any
		if err := json.Unmarshal(data, &event); err != nil || stringValue(event["type"]) != "tool_lease" {
			return nil
		}
		toolCallID := stringValue(event["toolCallId"])
		go actor.routeSubagentLeafToolResult(toolCallID, map[string]any{"status": "done", "result": map[string]any{
			"contentURL": origin + "/attachments/" + id,
			"isImage":    true,
			"imageInfo":  map[string]any{"mimeType": "image/png"},
		}})
		return nil
	}}
	actor.sockets[socket] = struct{}{}
	attachments := actor.readSubagentFiles(context.Background(), []string{"local.png"}, "/tmp", origin, "TU-parent", "M-parent", actor.generation)
	if len(attachments.Images) != 1 {
		t.Fatalf("trusted URL-backed attachment images = %#v text=%q", attachments.Images, attachments.Text)
	}
	source := mapValue(mapValue(attachments.Images[0])["source"])
	if stringValue(source["mediaType"]) != "image/png" || stringValue(source["data"]) != imageData {
		t.Fatalf("trusted URL-backed attachment source = %#v", source)
	}
}

func TestNeoOracleAttachmentAggregateBounds(t *testing.T) {
	imageData := neoSubagentImageFixtures(t)["image/png"]
	actor := newNeoActor(newNeoRuntime(&config.Config{}), "actor-oracle-bounds", "thread-actor", "T-oracle-bounds", "T-oracle-bounds", neoActorRecord("actor-oracle-bounds", "thread-actor", "T-oracle-bounds"), nil)
	actor.executorBootstrapComplete = true
	readCount := 0
	socket := &neoSocket{writeMessage: func(_ int, data []byte) error {
		var event map[string]any
		if err := json.Unmarshal(data, &event); err != nil || stringValue(event["type"]) != "tool_lease" {
			return nil
		}
		readCount++
		toolCallID := stringValue(event["toolCallId"])
		go actor.routeSubagentLeafToolResult(toolCallID, map[string]any{"status": "done", "result": map[string]any{
			"content":   imageData,
			"isImage":   true,
			"imageInfo": map[string]any{"mimeType": "image/png"},
		}})
		return nil
	}}
	actor.sockets[socket] = struct{}{}
	files := make([]string, neoSubagentAttachmentMaxFiles+3)
	for index := range files {
		files[index] = fmt.Sprintf("image-%02d.png", index)
	}
	attachments := actor.readSubagentFiles(context.Background(), files, "/tmp", "", "TU-parent", "M-parent", actor.generation)
	if readCount != neoSubagentAttachmentMaxFiles {
		t.Fatalf("Oracle bounded attachment reads = %d, want %d", readCount, neoSubagentAttachmentMaxFiles)
	}
	if !strings.Contains(attachments.Text, "3 files exceed the 16-file limit") {
		t.Fatalf("Oracle file-count omission missing: %q", attachments.Text)
	}

	raw, err := base64.StdEncoding.DecodeString(imageData)
	if err != nil {
		t.Fatal(err)
	}
	raw = append(raw, make([]byte, neoSubagentAttachmentMaxImageBytes-len(raw))...)
	imageData = base64.StdEncoding.EncodeToString(raw)
	readCount = 0
	attachments = actor.readSubagentFiles(context.Background(), []string{"large-1.png", "large-2.png", "large-3.png", "large-4.png", "large-5.png"}, "/tmp", "", "TU-parent", "M-parent", actor.generation)
	if readCount != 5 || len(attachments.Images) != 4 || !strings.Contains(attachments.Text, "aggregate size exceeds the 16 MiB limit") {
		t.Fatalf("Oracle aggregate attachment bound = reads:%d images:%d text:%q", readCount, len(attachments.Images), attachments.Text)
	}
}

func TestNeoOracleTextAttachmentBounds(t *testing.T) {
	unchanged := "\ufeff leading\r\nsecond\rthird \n"
	if bounded := neoSubagentBoundTextAttachment(unchanged); bounded != unchanged {
		t.Fatalf("unbounded attachment changed from %q to %q", unchanged, bounded)
	}
	lines := make([]string, 600)
	for index := range lines {
		lines[index] = fmt.Sprintf("line %03d", index+1)
	}
	boundedLines := neoSubagentBoundTextAttachment(strings.Join(lines, "\n"))
	if got := len(strings.Split(boundedLines, "\n")); got != neoSubagentAttachmentMaxTextLines+1 {
		t.Fatalf("bounded line count = %d, want %d retained lines plus an omission marker", got, neoSubagentAttachmentMaxTextLines)
	}
	if !strings.Contains(boundedLines, "[... omitted lines 251 to 350 ...]") {
		t.Fatalf("bounded lines omitted marker missing: %s", boundedLines)
	}

	boundedBytes := neoSubagentBoundTextAttachment(strings.Repeat("x", neoSubagentAttachmentMaxTextBytes+1024))
	boundedByteLines := strings.Split(boundedBytes, "\n")
	wideLineSuffix := "…[+30KB]"
	if !strings.HasSuffix(boundedByteLines[0], wideLineSuffix) || len(strings.TrimSuffix(boundedByteLines[0], wideLineSuffix)) != neoSubagentAttachmentMaxLineBytes || !strings.Contains(boundedBytes, "File truncated - showing first 32KB") {
		t.Fatalf("bounded byte content did not enforce classic limits: first=%d content=%q", len(boundedByteLines[0]), boundedBytes)
	}
	utf8Bounded := neoSubagentBoundTextAttachment(strings.Repeat("x", neoSubagentAttachmentMaxLineBytes-1) + "€tail")
	if strings.ToValidUTF8(utf8Bounded, "") != utf8Bounded || !strings.Contains(utf8Bounded, "…[+") {
		t.Fatalf("UTF-8 line boundary was not truncated safely: %q", utf8Bounded)
	}

	unnumbered := neoFileMentionsText(map[string]any{"files": []any{map[string]any{
		"uri":     "file:///tmp/example.txt",
		"content": "alpha\nbeta",
	}}})
	if !strings.Contains(unnumbered, "```example.txt\n1: alpha\n2: beta\n```") {
		t.Fatalf("unnumbered Oracle text attachment = %q", unnumbered)
	}
	numbered := neoFileMentionsText(map[string]any{"files": []any{map[string]any{
		"uri":     "file:///tmp/example.txt",
		"content": neoSubagentBoundTextAttachment("1: alpha\n2: beta"),
	}}})
	if !strings.Contains(numbered, "```example.txt\n1: alpha\n2: beta\n```") || strings.Contains(numbered, "1: 1: alpha") {
		t.Fatalf("line-numbered Oracle text attachment = %q", numbered)
	}
	legitimateLabels := "2024: release started\n1: enabled"
	if bounded := neoSubagentBoundTextAttachment(legitimateLabels); bounded != legitimateLabels {
		t.Fatalf("legitimate numeric labels changed from %q to %q", legitimateLabels, bounded)
	}
	if bounded := neoSubagentBoundTextAttachment("1: enabled"); bounded != "1: enabled" {
		t.Fatalf("single numeric label changed to %q", bounded)
	}
	legitimateSequentialLabels := "1: enabled\n2: disabled"
	if bounded := neoSubagentBoundTextAttachment(legitimateSequentialLabels); bounded != legitimateSequentialLabels {
		t.Fatalf("sequential numeric labels changed from %q to %q", legitimateSequentialLabels, bounded)
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
	var nestedFinderID string
	for i := len(actor.history) - 1; i >= 0; i-- {
		if actor.history[i].Role == "tool" && actor.history[i].ToolCallID == parent.ID {
			toolText = actor.history[i].Text
			break
		}
	}
	for _, message := range actor.messages {
		if message.ParentToolUseID != parent.ID || message.Role != "assistant" || len(message.Content) != 1 {
			continue
		}
		block := mapValue(message.Content[0])
		if stringValue(block["type"]) == "tool_use" && stringValue(block["name"]) == "finder" {
			nestedFinderID = stringValue(block["id"])
		}
	}
	for _, message := range actor.messages {
		if message.ParentToolUseID != parent.ID || message.MessageID != toolResultMessageID(nestedFinderID) {
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

func TestNeoSubagentNestedRecursionStopsAtDepthLimit(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	inferences := 0
	rt.inferStream = func(_ *neoRuntime, _ neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		inferences++
		return neoInferenceResult{Text: "unexpected"}, nil
	}
	actor := newNeoActor(rt, "actor-depth-limit", "thread-actor", "T-depth-limit", "T-depth-limit", neoActorRecord("actor-depth-limit", "thread-actor", "T-depth-limit"), nil)
	exchanges := actor.execSubagentTurnTools([]neoToolCall{{
		ID:    "nested-task",
		Name:  "Task",
		Input: map[string]any{"prompt": "recurse", "description": "recurse"},
	}}, "TU-parent", actor.generation, neoSubagentMaxDepth, "smart", "")
	if len(exchanges) != 1 || stringValue(exchanges[0].Run["status"]) != "error" || !strings.Contains(runToText(exchanges[0].Run), "maximum depth") {
		t.Fatalf("depth-limited exchange = %#v", exchanges)
	}
	if inferences != 0 {
		t.Fatalf("depth-limited nested inferences = %d, want 0", inferences)
	}
}

func TestNeoTaskSubagentReceivesCompactedActiveSkillAndTools(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	sawSkillPrompt := false
	sawSkillTool := false
	rt.inferStream = func(_ *neoRuntime, req neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		if req.ParentToolCallID == "TU-task-skill" {
			sawSkillPrompt = strings.Contains(req.SystemPromptOverride, "Inspect the exact PR body before creation.")
			for _, tool := range req.Tools {
				if tool.Name == "shipping_pr_tool" {
					sawSkillTool = true
				}
			}
			return neoInferenceResult{Text: "TASK SKILL READY"}, nil
		}
		return neoInferenceResult{Text: "continued"}, nil
	}

	threadID := "T-task-active-skill"
	actor := newNeoActor(rt, "actor-task-skill", "thread-actor", threadID, threadID, neoActorRecord("actor-task-skill", "thread-actor", threadID), nil)
	skillBody := `<loaded_skill name="shipping-prs">Inspect the exact PR body before creation.</loaded_skill>`
	actor.mu.Lock()
	actor.currentAgentMode = "deep"
	actor.settings["agentMode"] = "deep"
	actor.messages = []neoMessage{
		{ThreadID: threadID, MessageID: "M-user", Role: "user", Content: []any{map[string]any{"type": "text", "text": "ship the changes"}}},
		{ThreadID: threadID, MessageID: "M-skill-call", Role: "assistant", Content: []any{map[string]any{"type": "tool_use", "id": "TU-skill", "name": "skill", "input": map[string]any{"name": "shipping-prs"}}}},
		{ThreadID: threadID, MessageID: "M-skill-result", Role: "user", Content: []any{map[string]any{"type": "tool_result", "toolUseID": "TU-skill", "run": map[string]any{"status": "done", "result": map[string]any{"content": []any{map[string]any{"type": "text", "text": skillBody}}}}}}},
		neoCompactionSummaryMessage(threadID, "Continue shipping the approved changes."),
		{ThreadID: threadID, MessageID: "M-continue", Role: "user", Content: []any{map[string]any{"type": "text", "text": "continue"}}},
	}
	actor.loadedSkills = neoLoadedSkillsFromMessages(actor.messages)
	actor.activatedSkills = neoActivatedSkillsFromLoads(actor.loadedSkills)
	actor.tools["shipping_pr_tool"] = neoToolSpec{Name: "shipping_pr_tool", Meta: map[string]any{"deferred": true, "skillNames": []any{"shipping-prs"}}}
	actor.rebuildHistoryLocked()
	parent := neoPendingTool{ID: "TU-task-skill", Name: "Task", Input: map[string]any{"prompt": "finish the shipping workflow", "description": "ship"}, AgentMode: "deep", MessageID: "M-task"}
	actor.pendingTools[parent.ID] = parent
	actor.proxyOwnedPendingTools[parent.ID] = true
	actor.executorReady = false
	generation := actor.generation
	actor.mu.Unlock()

	done := make(chan struct{})
	go func() {
		actor.runSubagent(parent, generation)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Task subagent did not finish")
	}
	if !sawSkillPrompt || !sawSkillTool {
		t.Fatalf("Task active skill prompt/tool = %v/%v, want both", sawSkillPrompt, sawSkillTool)
	}
}

func assertNeoCompletedRecoveredTool(t *testing.T, actor *neoActor, stage, toolCallID, wantOutput string) {
	t.Helper()
	actor.mu.Lock()
	defer actor.mu.Unlock()
	if actor.agentState != "idle" || actor.currentInference != nil || actor.pendingInference != nil || len(actor.pendingTools) != 0 || len(actor.approvalQueue) != 0 || len(actor.recoveredProxyOwnedTools) != 0 || len(actor.proxyOwnedPendingTools) != 0 || len(actor.subagentRuns) != 0 || len(actor.subagentTools) != 0 || len(actor.subagentWaiters) != 0 {
		t.Fatalf("%s active state: state=%q current=%#v pendingInference=%#v pendingTools=%#v approvals=%#v recovered=%#v owned=%#v subagentRuns=%#v subagentTools=%#v subagentWaiters=%#v", stage, actor.agentState, actor.currentInference, actor.pendingInference, actor.pendingTools, actor.approvalQueue, actor.recoveredProxyOwnedTools, actor.proxyOwnedPendingTools, actor.subagentRuns, actor.subagentTools, actor.subagentWaiters)
	}
	wantBlock := map[string]any{
		"type":      "tool_result",
		"toolUseID": toolCallID,
		"run":       map[string]any{"status": "done", "output": wantOutput},
	}
	resultCount := 0
	for _, message := range actor.messages {
		for _, rawBlock := range message.Content {
			block := mapValue(rawBlock)
			if stringValue(block["toolUseID"]) != toolCallID {
				continue
			}
			resultCount++
			if message.Role != "user" || message.MessageID != toolResultMessageID(toolCallID) || message.ParentToolUseID != "" || message.CompletionStatus != "" || len(message.Content) != 1 || !reflect.DeepEqual(block, wantBlock) {
				t.Fatalf("%s result shape = message:%#v block:%#v", stage, message, block)
			}
		}
	}
	if resultCount != 1 {
		t.Fatalf("%s result count = %d, want 1", stage, resultCount)
	}
}

func assertNeoPersistedCompletedRecoveredTool(t *testing.T, persisted map[string]any, stage, toolCallID, wantOutput string) {
	t.Helper()
	wantBlock := map[string]any{
		"type":      "tool_result",
		"toolUseID": toolCallID,
		"run":       map[string]any{"status": "done", "output": wantOutput},
	}
	resultCount := 0
	for _, rawMessage := range arrayValue(persisted["messages"]) {
		message := mapValue(rawMessage)
		for _, rawBlock := range arrayValue(message["content"]) {
			block := mapValue(rawBlock)
			if stringValue(block["toolUseID"]) != toolCallID {
				continue
			}
			resultCount++
			createdAt, createdAtOK := message["createdAt"].(string)
			wantMessage := map[string]any{
				"role":              "user",
				"content":           []any{wantBlock},
				"messageId":         toolResultMessageID(toolCallID),
				"protocolMessageID": toolResultMessageID(toolCallID),
				"createdAt":         createdAt,
			}
			if !createdAtOK || strings.TrimSpace(createdAt) == "" || !reflect.DeepEqual(message, wantMessage) {
				t.Fatalf("%s persisted result shape = message:%#v block:%#v", stage, message, block)
			}
		}
	}
	if resultCount != 1 {
		t.Fatalf("%s persisted result count = %d, want 1", stage, resultCount)
	}
}

func TestNeoActorRestartsProxyOwnedTaskAndPreservesNestedTranscriptAfterImport(t *testing.T) {
	const (
		rootToolCallID           = "TU-1111111111111111111111"
		orphanChildToolCallID    = "TU-2222222222222222222222"
		orphanApprovalToolCallID = "TU-3333333333333333333333"
	)

	rt := newNeoRuntime(&config.Config{})
	var mu sync.Mutex
	subagentRuns := 0
	inferenceParentToolCallIDs := []string{}
	rt.inferStream = func(_ *neoRuntime, req neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		mu.Lock()
		inferenceParentToolCallIDs = append(inferenceParentToolCallIDs, req.ParentToolCallID)
		if req.ParentToolCallID == rootToolCallID {
			subagentRuns++
			mu.Unlock()
			return neoInferenceResult{Text: "RESTORED TASK DONE"}, nil
		}
		mu.Unlock()
		return neoInferenceResult{Text: "continued"}, nil
	}

	threadID := "T-restored-proxy-tool"
	nestedResultBlock := map[string]any{"type": "tool_result", "toolUseID": orphanChildToolCallID, "run": map[string]any{"status": "running"}}
	nestedApprovalBlock := map[string]any{"type": "tool_result", "toolUseID": orphanApprovalToolCallID, "run": map[string]any{"status": "blocked-on-user", "reason": "approval needed", "toolName": "shell_command"}}
	userMessageID := "M-0000000000000000000010"
	taskMessageID := "M-0000000000000000000011"
	childMessageID := "M-0000000000000000000012"
	nestedResultMessageID := "M-0000000000000000000013"
	source := newNeoActor(rt, "actor-source", "thread-actor", threadID, threadID, neoActorRecord("actor-source", "thread-actor", threadID), nil)
	source.mu.Lock()
	source.currentAgentMode = "deep"
	source.currentReasoningEffort = "xhigh"
	source.agentState = "awaiting_approval"
	source.settings["agentMode"] = "deep"
	source.messages = []neoMessage{
		{ThreadID: threadID, MessageID: userMessageID, Role: "user", Content: []any{map[string]any{"type": "text", "text": "delegate this"}}},
		{ThreadID: threadID, MessageID: taskMessageID, Role: "assistant", Content: []any{map[string]any{"type": "tool_use", "id": rootToolCallID, "name": "Task", "input": map[string]any{"prompt": "do the work", "description": "work"}}}},
		{ThreadID: threadID, MessageID: childMessageID, Role: "assistant", ParentToolUseID: rootToolCallID, Content: []any{
			map[string]any{"type": "tool_use", "id": orphanChildToolCallID, "name": "shell_command", "input": map[string]any{"command": "go test ./..."}},
			map[string]any{"type": "tool_use", "id": orphanApprovalToolCallID, "name": "shell_command", "input": map[string]any{"command": "rm artifact"}, "complete": false},
		}},
		{ThreadID: threadID, MessageID: nestedResultMessageID, Role: "user", ParentToolUseID: rootToolCallID, CompletionStatus: "tool_progress", Content: []any{nestedResultBlock, nestedApprovalBlock}},
	}
	source.pendingTools[rootToolCallID] = neoPendingTool{ID: rootToolCallID, Name: "Task", Input: map[string]any{"prompt": "do the work", "description": "work"}, AgentMode: "deep", ReasoningEffort: "xhigh", MessageID: taskMessageID}
	source.pendingTools[orphanChildToolCallID] = neoPendingTool{ID: orphanChildToolCallID, Name: "shell_command", Input: map[string]any{"command": "go test ./..."}, AgentMode: "deep", ReasoningEffort: "xhigh", MessageID: childMessageID, ParentToolCallID: rootToolCallID}
	source.proxyOwnedPendingTools[rootToolCallID] = true
	source.meta[neoResumeExecutorIDMetaKey] = "executor-before-restart"
	source.rebuildHistoryLocked()
	source.mu.Unlock()

	snapshot, ok := source.threadSnapshot()
	if !ok {
		t.Fatal("source snapshot missing")
	}
	persisted := marshalNeoThreadForTest(t, neoCloudThread(snapshot))
	expectedPersisted := marshalNeoThreadForTest(t, persisted)
	if got := stringArrayValue(mapValue(persisted["meta"])[neoProxyOwnedToolIDsMetaKey]); len(got) != 1 || stringValue(got[0]) != rootToolCallID {
		t.Fatalf("persisted proxy-owned tools = %#v", got)
	}

	restored := newNeoActor(rt, "actor-restored", "thread-actor", threadID, threadID, neoActorRecord("actor-restored", "thread-actor", threadID), nil)
	if err := restored.importThreadLocalOnly(marshalNeoThreadForTest(t, persisted)); err != nil {
		t.Fatalf("import thread: %v", err)
	}
	restoredSnapshot, ok := restored.threadSnapshot()
	if !ok {
		t.Fatal("restored snapshot missing")
	}
	restoredPersisted := marshalNeoThreadForTest(t, neoCloudThread(restoredSnapshot))
	if !reflect.DeepEqual(arrayValue(restoredPersisted["messages"]), arrayValue(expectedPersisted["messages"])) {
		t.Fatalf("import changed persisted transcript:\n got: %#v\nwant: %#v", arrayValue(restoredPersisted["messages"]), arrayValue(expectedPersisted["messages"]))
	}
	restored.mu.Lock()
	if _, ok := restored.pendingTools[orphanChildToolCallID]; ok {
		restored.mu.Unlock()
		t.Fatal("orphaned nested lease remained pending after import")
	}
	if _, ok := restored.pendingTools[orphanApprovalToolCallID]; ok || len(restored.approvalQueue) != 0 {
		restored.mu.Unlock()
		t.Fatalf("incomplete orphaned approval remained executable: pending=%#v approvals=%#v", restored.pendingTools, restored.approvalQueue)
	}
	if _, ok := restored.pendingTools[rootToolCallID]; !ok {
		restored.mu.Unlock()
		t.Fatal("proxy-owned root Task was not restored as pending")
	}
	if _, ok := restored.recoveredProxyOwnedTools[rootToolCallID]; !ok {
		restored.mu.Unlock()
		t.Fatal("proxy-owned root Task was not queued for recovery")
	}
	if restored.agentState != "running_tools" {
		restored.mu.Unlock()
		t.Fatalf("restored agent state = %q, want running_tools for root recovery", restored.agentState)
	}
	restored.executorReady = true
	restored.executorBootstrapComplete = true
	restored.mu.Unlock()
	restored.resumeRecoveredProxyOwnedTools()

	if !neoWaitFor(3*time.Second, func() bool {
		restored.mu.Lock()
		_, pending := restored.pendingTools[rootToolCallID]
		restored.mu.Unlock()
		return !pending
	}) {
		t.Fatal("restored Task did not complete")
	}
	mu.Lock()
	gotRuns := subagentRuns
	mu.Unlock()
	if gotRuns != 1 {
		t.Fatalf("restored Task runs = %d, want exactly one", gotRuns)
	}
	if !neoWaitFor(3*time.Second, func() bool {
		restored.mu.Lock()
		defer restored.mu.Unlock()
		if restored.agentState != "idle" || restored.currentInference != nil || restored.pendingInference != nil || len(restored.pendingTools) != 0 || len(restored.approvalQueue) != 0 || len(restored.recoveredProxyOwnedTools) != 0 || len(restored.proxyOwnedPendingTools) != 0 || len(restored.subagentRuns) != 0 || len(restored.subagentTools) != 0 || len(restored.subagentWaiters) != 0 {
			return false
		}
		for _, message := range restored.messages {
			if message.Role == "assistant" && message.ParentToolUseID == "" && textFromBlocks(message.Content) == "continued" {
				return true
			}
		}
		return false
	}) {
		t.Fatal("parent continuation did not complete")
	}
	mu.Lock()
	gotInferenceParentToolCallIDs := append([]string(nil), inferenceParentToolCallIDs...)
	mu.Unlock()
	if !slices.Equal(gotInferenceParentToolCallIDs, []string{rootToolCallID, ""}) {
		t.Fatalf("recovery inference parents = %#v, want root Task then parent continuation", gotInferenceParentToolCallIDs)
	}
	assertNeoCompletedRecoveredTool(t, restored, "restored root Task", rootToolCallID, "RESTORED TASK DONE")

	completedSnapshot, ok := restored.threadSnapshot()
	if !ok {
		t.Fatal("completed recovery snapshot missing")
	}
	completedPersisted := marshalNeoThreadForTest(t, neoCloudThread(completedSnapshot))
	completedMessages := arrayValue(completedPersisted["messages"])
	originalMessages := arrayValue(expectedPersisted["messages"])
	if len(completedMessages) < len(originalMessages) || !reflect.DeepEqual(completedMessages[:len(originalMessages)], originalMessages) {
		t.Fatalf("completed recovery changed imported transcript prefix:\n got: %#v\nwant: %#v", completedMessages, originalMessages)
	}
	if _, exists := mapValue(completedPersisted["meta"])[neoProxyOwnedToolIDsMetaKey]; exists {
		t.Fatalf("completed recovery persisted stale proxy ownership: %#v", completedPersisted["meta"])
	}
	assertNeoPersistedCompletedRecoveredTool(t, completedPersisted, "root Task recovery", rootToolCallID, "RESTORED TASK DONE")

	rehydrated := newNeoActor(rt, "actor-rehydrated", "thread-actor", threadID, threadID, neoActorRecord("actor-rehydrated", "thread-actor", threadID), nil)
	if err := rehydrated.importThreadLocalOnly(marshalNeoThreadForTest(t, completedPersisted)); err != nil {
		t.Fatalf("reimport completed recovery: %v", err)
	}
	assertNeoCompletedRecoveredTool(t, rehydrated, "rehydrated root Task", rootToolCallID, "RESTORED TASK DONE")
	rehydratedSnapshot, ok := rehydrated.threadSnapshot()
	if !ok {
		t.Fatal("rehydrated completed recovery snapshot missing")
	}
	rehydratedPersisted := marshalNeoThreadForTest(t, neoCloudThread(rehydratedSnapshot))
	if !reflect.DeepEqual(rehydratedPersisted, completedPersisted) {
		t.Fatalf("completed recovery changed after rehydration:\n got: %#v\nwant: %#v", rehydratedPersisted, completedPersisted)
	}
}

func TestNeoActorRestartsLegacyTaskWithoutProxyOwnershipMetadata(t *testing.T) {
	const rootToolCallID = "TU-4444444444444444444444"

	rt := newNeoRuntime(&config.Config{})
	var mu sync.Mutex
	subagentRuns := 0
	inferenceParentToolCallIDs := []string{}
	rt.inferStream = func(_ *neoRuntime, req neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		mu.Lock()
		inferenceParentToolCallIDs = append(inferenceParentToolCallIDs, req.ParentToolCallID)
		if req.ParentToolCallID == rootToolCallID {
			subagentRuns++
			mu.Unlock()
			return neoInferenceResult{Text: "LEGACY TASK DONE"}, nil
		}
		mu.Unlock()
		return neoInferenceResult{Text: "continued"}, nil
	}

	threadID := "T-restored-legacy-task"
	source := newNeoActor(rt, "actor-source", "thread-actor", threadID, threadID, neoActorRecord("actor-source", "thread-actor", threadID), nil)
	source.mu.Lock()
	source.currentAgentMode = "deep"
	source.currentReasoningEffort = "xhigh"
	source.agentState = "running_tools"
	source.messages = []neoMessage{
		{ThreadID: threadID, MessageID: "M-0000000000000000000040", Role: "user", Content: []any{map[string]any{"type": "text", "text": "delegate legacy work"}}},
		{ThreadID: threadID, MessageID: "M-0000000000000000000041", Role: "assistant", Content: []any{map[string]any{"type": "tool_use", "id": rootToolCallID, "name": "Task", "input": map[string]any{"prompt": "do legacy work", "description": "legacy work"}}}},
	}
	source.pendingTools[rootToolCallID] = neoPendingTool{ID: rootToolCallID, Name: "Task", Input: map[string]any{"prompt": "do legacy work", "description": "legacy work"}, AgentMode: "deep", ReasoningEffort: "xhigh", MessageID: "M-0000000000000000000041"}
	source.rebuildHistoryLocked()
	source.mu.Unlock()

	snapshot, ok := source.threadSnapshot()
	if !ok {
		t.Fatal("legacy source snapshot missing")
	}
	persisted := marshalNeoThreadForTest(t, neoCloudThread(snapshot))
	expectedPersisted := marshalNeoThreadForTest(t, persisted)
	if _, exists := mapValue(persisted["meta"])[neoProxyOwnedToolIDsMetaKey]; exists {
		t.Fatalf("legacy fixture unexpectedly persisted proxy ownership: %#v", persisted["meta"])
	}

	restored := newNeoActor(rt, "actor-restored", "thread-actor", threadID, threadID, neoActorRecord("actor-restored", "thread-actor", threadID), nil)
	if err := restored.importThreadLocalOnly(marshalNeoThreadForTest(t, persisted)); err != nil {
		t.Fatalf("import legacy thread: %v", err)
	}
	restoredSnapshot, ok := restored.threadSnapshot()
	if !ok {
		t.Fatal("restored legacy snapshot missing")
	}
	restoredPersisted := marshalNeoThreadForTest(t, neoCloudThread(restoredSnapshot))
	if !reflect.DeepEqual(arrayValue(restoredPersisted["messages"]), arrayValue(expectedPersisted["messages"])) {
		t.Fatalf("legacy import changed persisted transcript:\n got: %#v\nwant: %#v", arrayValue(restoredPersisted["messages"]), arrayValue(expectedPersisted["messages"]))
	}
	restored.mu.Lock()
	_, pending := restored.pendingTools[rootToolCallID]
	_, recovered := restored.recoveredProxyOwnedTools[rootToolCallID]
	if !pending || !recovered || !restored.proxyOwnedPendingTools[rootToolCallID] || restored.agentState != "running_tools" {
		restored.mu.Unlock()
		t.Fatalf("legacy Task recovery state: pending=%v recovered=%v owned=%v state=%q", pending, recovered, restored.proxyOwnedPendingTools[rootToolCallID], restored.agentState)
	}
	restored.executorReady = true
	restored.executorBootstrapComplete = true
	restored.mu.Unlock()
	restored.resumeRecoveredProxyOwnedTools()

	if !neoWaitFor(3*time.Second, func() bool {
		restored.mu.Lock()
		defer restored.mu.Unlock()
		if restored.currentInference != nil || restored.pendingInference != nil || restored.agentState != "idle" || len(restored.pendingTools) != 0 || len(restored.approvalQueue) != 0 || len(restored.recoveredProxyOwnedTools) != 0 || len(restored.proxyOwnedPendingTools) != 0 || len(restored.subagentRuns) != 0 || len(restored.subagentTools) != 0 || len(restored.subagentWaiters) != 0 {
			return false
		}
		for _, message := range restored.messages {
			if message.Role == "assistant" && message.ParentToolUseID == "" && textFromBlocks(message.Content) == "continued" {
				return true
			}
		}
		return false
	}) {
		t.Fatal("legacy Task recovery did not reach idle continuation")
	}
	mu.Lock()
	gotRuns := subagentRuns
	mu.Unlock()
	if gotRuns != 1 {
		t.Fatalf("legacy Task runs = %d, want exactly one", gotRuns)
	}
	mu.Lock()
	gotInferenceParentToolCallIDs := append([]string(nil), inferenceParentToolCallIDs...)
	mu.Unlock()
	if !slices.Equal(gotInferenceParentToolCallIDs, []string{rootToolCallID, ""}) {
		t.Fatalf("legacy recovery inference parents = %#v, want root Task then parent continuation", gotInferenceParentToolCallIDs)
	}

	assertNeoCompletedRecoveredTool(t, restored, "legacy recovery", rootToolCallID, "LEGACY TASK DONE")

	completedSnapshot, ok := restored.threadSnapshot()
	if !ok {
		t.Fatal("completed legacy recovery snapshot missing")
	}
	completedPersisted := marshalNeoThreadForTest(t, neoCloudThread(completedSnapshot))
	completedMessages := arrayValue(completedPersisted["messages"])
	originalMessages := arrayValue(expectedPersisted["messages"])
	if len(completedMessages) < len(originalMessages) || !reflect.DeepEqual(completedMessages[:len(originalMessages)], originalMessages) {
		t.Fatalf("completed legacy recovery changed imported transcript prefix:\n got: %#v\nwant: %#v", completedMessages, originalMessages)
	}
	if _, exists := mapValue(completedPersisted["meta"])[neoProxyOwnedToolIDsMetaKey]; exists {
		t.Fatalf("completed legacy recovery persisted proxy ownership: %#v", completedPersisted["meta"])
	}
	assertNeoPersistedCompletedRecoveredTool(t, completedPersisted, "legacy Task recovery", rootToolCallID, "LEGACY TASK DONE")

	rehydrated := newNeoActor(rt, "actor-rehydrated", "thread-actor", threadID, threadID, neoActorRecord("actor-rehydrated", "thread-actor", threadID), nil)
	if err := rehydrated.importThreadLocalOnly(marshalNeoThreadForTest(t, completedPersisted)); err != nil {
		t.Fatalf("reimport completed legacy recovery: %v", err)
	}
	assertNeoCompletedRecoveredTool(t, rehydrated, "rehydrated legacy recovery", rootToolCallID, "LEGACY TASK DONE")
	rehydratedSnapshot, ok := rehydrated.threadSnapshot()
	if !ok {
		t.Fatal("rehydrated completed legacy recovery snapshot missing")
	}
	rehydratedPersisted := marshalNeoThreadForTest(t, neoCloudThread(rehydratedSnapshot))
	if !reflect.DeepEqual(rehydratedPersisted, completedPersisted) {
		t.Fatalf("completed legacy recovery changed after rehydration:\n got: %#v\nwant: %#v", rehydratedPersisted, completedPersisted)
	}
}

func TestNeoRestoredProxyOwnedToolsCoversServerOwnedSurfaces(t *testing.T) {
	pending := map[string]neoPendingTool{}
	for _, name := range []string{"Task", "run_check", "finder", "oracle", "librarian", "read_thread", "submit_review", "find_thread", "thread_interact", "read_github", "shell_command"} {
		pending[name] = neoPendingTool{ID: name, Name: name}
	}
	restored := neoRestoredProxyOwnedTools(pending, nil)
	for _, name := range []string{"Task", "run_check", "finder", "oracle", "librarian", "read_thread", "submit_review", "find_thread", "thread_interact", "read_github"} {
		if _, ok := restored[name]; !ok {
			t.Fatalf("server-owned tool %q was not restorable: %#v", name, restored)
		}
	}
	if _, ok := restored["shell_command"]; ok {
		t.Fatalf("executor-owned shell command was classified as proxy-owned: %#v", restored)
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
	var childReadID string
	for i := len(actor.history) - 1; i >= 0; i-- {
		if actor.history[i].Role == "tool" && actor.history[i].ToolCallID == parent.ID {
			toolText = actor.history[i].Text
			break
		}
	}
	for _, message := range actor.messages {
		if message.ParentToolUseID != parent.ID || message.Role != "assistant" || len(message.Content) != 1 {
			continue
		}
		block := mapValue(message.Content[0])
		if stringValue(block["type"]) == "tool_use" && stringValue(block["name"]) == "read_thread" {
			childReadID = stringValue(block["id"])
		}
	}
	for _, message := range actor.messages {
		if childReadID != "" && message.ParentToolUseID == childReadID {
			internalReadMessages++
		}
		if message.ParentToolUseID != parent.ID || message.MessageID != toolResultMessageID(childReadID) {
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
