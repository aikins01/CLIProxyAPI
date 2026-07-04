package amp

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

// Subagent tools (finder/oracle/librarian/Task) are advertised by the Amp
// executor so the model can call them, but the executor has no runnable
// implementation for them — in Amp's cloud architecture the server runs the
// subagent loop. In local-Neo threads our runtime IS the server, so we must run
// the loop here instead of leasing the call to the executor (which fails with
// `tool "finder" not found`). Each subagent runs its own model + includeTools +
// system prompt; its leaf-tool calls (Grep/glob/Read/...) still lease to the
// executor, and the subagent's final message becomes the parent tool result.
//
// The system prompts are Amp-server-owned and are not present in the Neo client
// binary; they are recovered verbatim from the amp-classic binary (which runs
// subagents client-side) and embedded here. See neoSubagentPromptDriftMarkers
// and the drift test for keeping them honest against future Amp updates.

//go:embed neo_subagent_prompts/finder.md
var neoFinderSubagentPrompt string

//go:embed neo_subagent_prompts/oracle.md
var neoOracleSubagentPrompt string

//go:embed neo_subagent_prompts/librarian.md
var neoLibrarianSubagentPrompt string

//go:embed neo_subagent_prompts/task.md
var neoTaskSubagentPrompt string

//go:embed neo_subagent_prompts/run_check.md
var neoRunCheckSubagentPrompt string

// Model-facing tool descriptions (what the calling model sees to decide when to
// invoke the subagent). These are Amp-server-owned and absent from the Neo
// client binary; recovered verbatim from the binary's own provider request.

//go:embed neo_subagent_specs/finder.txt
var neoFinderExposureDescription string

//go:embed neo_subagent_specs/oracle.txt
var neoOracleExposureDescription string

//go:embed neo_subagent_specs/librarian.txt
var neoLibrarianExposureDescription string

//go:embed neo_subagent_specs/task.txt
var neoTaskExposureDescription string

// neoSubagentDef mirrors a single entry of the binary subagent registry (`_7`).
type neoSubagentDef struct {
	Key             string
	DisplayName     string
	Route           neoModelRoute // zero Model => inherit the resolved model from config/settings
	IncludeTools    []string
	SystemPrompt    string // may contain {{WORKING_DIR}} / {{WORKSPACE_ROOT}}
	ReasoningEffort string
	MaxTurns        int
}

// neoSubagentDefs maps the model-facing tool name to its definition. Keys match
// the names the binary advertises and the model calls. Models/includeTools come
// from Amp subagent routing (finder→haiku, oracle→Claude Fable 5, librarian→gpt-5.5).
var neoSubagentDefs = map[string]neoSubagentDef{
	"finder": {
		Key:          "finder",
		DisplayName:  "Finder",
		Route:        neoModelRoute{Provider: "anthropic", Model: "claude-haiku-4-5-20251001"},
		IncludeTools: []string{"Grep", "glob", "Read"},
		SystemPrompt: neoFinderSubagentPrompt,
		MaxTurns:     6,
	},
	"oracle": {
		Key:             "oracle",
		DisplayName:     "Oracle",
		Route:           neoModelRoute{Provider: "anthropic", Model: "claude-fable-5"},
		IncludeTools:    []string{"Read", "Grep", "glob", "web_search", "read_web_page", "read_thread", "find_thread"},
		SystemPrompt:    neoOracleSubagentPrompt,
		ReasoningEffort: "high",
		MaxTurns:        24,
	},
	"librarian": {
		Key:             "librarian",
		DisplayName:     "Librarian",
		Route:           neoModelRoute{Provider: "openai", Model: "gpt-5.5"},
		IncludeTools:    []string{"read_github", "search_github", "commit_search", "diff", "list_directory_github", "list_repositories", "glob_github"},
		SystemPrompt:    neoLibrarianSubagentPrompt,
		ReasoningEffort: "none",
		MaxTurns:        16,
	},
	"Task": {
		Key:          "task-subagent",
		DisplayName:  "Task",
		Route:        neoModelRoute{}, // inherits the resolved model from config/settings
		IncludeTools: []string{"Read", "Bash", "edit_file", "create_file", "read_web_page", "web_search", "finder", "skill", "view_media"},
		SystemPrompt: neoTaskSubagentPrompt,
		MaxTurns:     30,
	},
	// run_check backs the review agent mode. The retired client-side check
	// runner executed per-check agents on Haiku; that route is kept here now
	// that the runner is server-owned.
	"run_check": {
		Key:          "run_check",
		DisplayName:  "Check",
		Route:        neoModelRoute{Provider: "anthropic", Model: "claude-haiku-4-5-20251001"},
		IncludeTools: []string{"Read", "Grep", "glob", "Bash"},
		SystemPrompt: neoRunCheckSubagentPrompt,
		MaxTurns:     12,
	},
}

func neoSubagentDefFor(toolName string) (neoSubagentDef, bool) {
	def, ok := neoSubagentDefs[strings.TrimSpace(toolName)]
	return def, ok
}

// isNeoLocalSubagentTool reports whether a tool call should be executed locally
// as a subagent loop instead of being leased to the executor.
func isNeoLocalSubagentTool(toolName string) bool {
	_, ok := neoSubagentDefs[strings.TrimSpace(toolName)]
	return ok
}

// neoSubagentExposureSpec returns the model-facing tool spec for a subagent so
// it can be advertised to the model in local-Neo threads even when the executor
// does not register it (the executor advertises subagent tools only sometimes;
// in Amp's cloud architecture the server adds them, and in local mode we are the
// server). Descriptions are recovered verbatim from the binary; input schemas
// mirror the binary's exactly.
func neoSubagentExposureSpec(toolName string) (neoToolSpec, bool) {
	meta := map[string]any{"source": "neo-subagent"}
	name := strings.TrimSpace(toolName)
	switch name {
	case "finder":
		return neoToolSpec{
			Name:        "finder",
			Description: neoFinderExposureDescription,
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"query": map[string]any{"type": "string", "description": "The search query describing to the agent what it should. Be specific and include technical terms, file types, or expected code patterns to help the agent find relevant code. Formulate the query in a way that makes it clear to the agent when it has found the right thing."},
				},
				"required":             []any{"query"},
				"additionalProperties": true,
			},
			Meta: meta,
		}, true
	case "oracle":
		return neoToolSpec{
			Name:        name,
			Description: neoOracleExposureDescription,
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"task":    map[string]any{"type": "string", "description": "The task or question you want the oracle to help with. Be specific about what kind of guidance, review, or planning you need."},
					"context": map[string]any{"type": "string", "description": "Optional context about the current situation, what you've tried, or background information that would help the oracle provide better guidance."},
					"files":   map[string]any{"type": "array", "items": map[string]any{"type": "string"}, "description": "Optional list of specific file paths (text files, images) that the oracle should examine as part of its analysis. These files will be attached to the oracle input."},
				},
				"required":             []any{"task"},
				"additionalProperties": true,
			},
			Meta: meta,
		}, true
	case "librarian":
		return neoToolSpec{
			Name:        "librarian",
			Description: neoLibrarianExposureDescription,
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"query":   map[string]any{"type": "string", "description": "Your question about the codebase. Be specific about what you want to understand or explore."},
					"context": map[string]any{"type": "string", "description": "Optional context about what you're trying to achieve or background information."},
				},
				"required":             []any{"query"},
				"additionalProperties": true,
			},
			Meta: meta,
		}, true
	case "Task":
		return neoToolSpec{
			Name:        "Task",
			Description: neoTaskExposureDescription,
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"prompt":      map[string]any{"type": "string", "description": "The task for the agent to perform. Be specific about what needs to be done and include any relevant context."},
					"description": map[string]any{"type": "string", "description": "A very short description of the task that can be displayed to the user."},
				},
				"required":             []any{"prompt", "description"},
				"additionalProperties": true,
			},
			Meta: meta,
		}, true
	default:
		return neoToolSpec{}, false
	}
}

const (
	neoSubagentToolTimeout = 5 * time.Minute
	// neoSubagentMaxDepth bounds nested subagent calls (e.g. Task -> finder) to
	// prevent runaway recursion.
	neoSubagentMaxDepth = 3
)

// runSubagent executes a top-level subagent tool call locally and delivers the
// final message text as the parent tool result. It runs in its own goroutine;
// the parent thread is paused awaiting this tool result, so reusing the actor's
// executor connection for the subagent's leaf-tool calls is safe.
func (a *neoActor) runSubagent(parent neoPendingTool, generation int) {
	text, err := a.executeSubagentRun(parent.Name, parent.Input, parent.ID, parent.MessageID, generation, 0, parent.ClientAPIKey)
	// run_check results are consumed structurally by the amp review CLI from
	// run.result, so the final text is parsed instead of delivered verbatim.
	if err == nil && strings.TrimSpace(parent.Name) == "run_check" {
		a.deliverSubagentRun(parent, map[string]any{"status": "done", "result": neoRunCheckResultFromText(parent.Input, text)})
		return
	}
	a.deliverSubagentResult(parent, text, err)
}

// executeSubagentRun runs a subagent loop and returns its final message text. It
// is reused for nested subagent calls (e.g. Task -> finder); depth bounds the
// recursion. The subagent's leaf-tool calls lease to the executor, except calls
// that are themselves subagent tools, which run as nested subagents.
func (a *neoActor) executeSubagentRun(name string, input map[string]any, parentToolCallID, parentMessageID string, generation, depth int, clientAPIKey string) (string, error) {
	def, ok := neoSubagentDefFor(name)
	if !ok {
		return "", fmt.Errorf("unknown subagent %q", name)
	}

	a.mu.Lock()
	workingDir := firstNonEmptyString(a.environment["workingDirectory"], a.environment["cwd"])
	workspaceRoot := firstNonEmptyString(a.environment["workspaceRoot"], workingDir)
	route := def.Route
	if route.Model == "" {
		route = selectNeoModelRouteWithConfig(a.runtime, a.currentAgentMode, a.settings)
	}
	agentMode := a.currentAgentMode
	tools := a.resolveSubagentToolsLocked(def.IncludeTools)
	settings := cloneMap(a.settings)
	if def.ReasoningEffort != "" {
		settings["reasoning.effort"] = def.ReasoningEffort
	}
	environment := cloneMap(a.environment)
	maxTokens := a.maxTokens
	a.mu.Unlock()

	systemPrompt := strings.NewReplacer(
		"{{WORKING_DIR}}", firstNonEmptyString(workingDir, "unknown"),
		"{{WORKSPACE_ROOT}}", firstNonEmptyString(workspaceRoot, "unknown"),
	).Replace(def.SystemPrompt)

	inputText := neoSubagentInputText(name, input)
	conversation := []neoHistoryMessage{{Role: "user", Text: inputText}}

	// Attach explicitly-referenced files to the subagent input (parity with the
	// binary's oracle file mentions) so it does not have to Read large files
	// itself and exhaust its turn budget before synthesizing. run_check is
	// excluded: its files argument is the whole review target set, which the
	// check agent samples with its own tools instead of inlining.
	if files := neoStringSlice(input["files"]); len(files) > 0 && name != "run_check" {
		if attached := a.readSubagentFiles(files, parentToolCallID, parentMessageID, generation); attached != "" {
			conversation = append(conversation, neoHistoryMessage{Role: "user", Text: attached})
		}
	}

	toolNames := make([]string, 0, len(tools))
	for _, tl := range tools {
		toolNames = append(toolNames, tl.Name)
	}
	log.Debugf("amp neo subagent start tool=%s depth=%d model=%s/%s effort=%s thread=%s call=%s input_len=%d tools=%v", name, depth, route.Provider, route.Model, def.ReasoningEffort, a.threadID, parentToolCallID, len(inputText), toolNames)

	maxTurns := def.MaxTurns
	if maxTurns <= 0 {
		maxTurns = 12
	}

	var finalText string
	var runErr error
	for turn := 0; turn < maxTurns; turn++ {
		if a.subagentGenerationStale(generation) {
			return "", nil
		}
		routeCopy := route
		result, err := a.runtime.subagentInfer(neoInferenceRequest{
			ActorID:              a.id,
			ThreadID:             a.threadID,
			MessageID:            newNeoMessageID(),
			AgentMode:            agentMode,
			ReasoningEffort:      def.ReasoningEffort,
			ParentToolCallID:     parentToolCallID,
			MaxTokens:            maxTokens,
			Settings:             settings,
			History:              append([]neoHistoryMessage(nil), conversation...),
			Tools:                tools,
			Environment:          environment,
			ModelRouteOverride:   &routeCopy,
			SystemPromptOverride: systemPrompt,
		}, func(neoInferenceDelta) {})
		if err != nil {
			log.Debugf("amp neo subagent turn tool=%s turn=%d error=%v", name, turn, err)
			runErr = err
			break
		}
		turnCallNames := make([]string, 0, len(result.ToolCalls))
		for _, c := range result.ToolCalls {
			turnCallNames = append(turnCallNames, c.Name+":"+neoSubagentToolCallTarget(c))
		}
		log.Debugf("amp neo subagent turn tool=%s turn=%d text_len=%d thinking=%d tool_calls=%d calls=%v", name, turn, len(strings.TrimSpace(result.Text)), len(result.ThinkingBlocks), len(result.ToolCalls), turnCallNames)
		conversation = append(conversation, neoHistoryMessage{
			Role:           "assistant",
			Text:           result.Text,
			ToolCalls:      result.ToolCalls,
			ThinkingBlocks: result.ThinkingBlocks,
		})
		if len(result.ToolCalls) == 0 {
			finalText = result.Text
			break
		}
		for _, call := range result.ToolCalls {
			if call.Incomplete {
				continue
			}
			childMessageID := a.storeSubagentToolUseMessage(call, parentToolCallID)
			var run map[string]any
			if isNeoLocalSubagentTool(call.Name) && depth < neoSubagentMaxDepth {
				// Nested subagent (e.g. Task calling finder): run its own loop
				// rather than leasing it to the executor, which cannot run it.
				nestedText, nestedErr := a.executeSubagentRun(call.Name, call.Input, call.ID, childMessageID, generation, depth+1, clientAPIKey)
				if nestedErr != nil {
					run = map[string]any{"status": "error", "error": map[string]any{"message": nestedErr.Error()}}
				} else {
					run = map[string]any{"status": "done", "output": nestedText}
				}
				a.storeSubagentToolResultMessage(call.ID, run, parentToolCallID, "")
			} else if call.Name == "read_thread" && a.shouldRunLocalActorTool(call.Name) {
				text, err := a.executeLocalReadThread(neoPendingTool{ID: call.ID, Name: call.Name, Input: call.Input, AgentMode: agentMode, ParentToolCallID: parentToolCallID, MessageID: childMessageID, ClientAPIKey: clientAPIKey}, generation)
				if a.subagentGenerationStale(generation) {
					return "", nil
				}
				if err != nil {
					run = map[string]any{"status": "error", "error": map[string]any{"message": err.Error()}}
				} else {
					run = map[string]any{"status": "done", "result": strings.TrimSpace(text)}
				}
				a.storeSubagentToolResultMessage(call.ID, run, parentToolCallID, "")
			} else if isNeoGitHubTool(call.Name) {
				run = a.execSubagentLocalGitHubTool(call, parentToolCallID, childMessageID, clientAPIKey)
			} else {
				run = a.execSubagentLeafTool(call, parentToolCallID, childMessageID, generation)
			}
			conversation = append(conversation, neoHistoryMessage{
				Role:            "tool",
				ToolCallID:      call.ID,
				ToolName:        call.Name,
				Text:            runToText(run),
				Content:         neoToolRunHistoryContent(run),
				ParentToolUseID: parentToolCallID,
			})
		}
	}

	// Forcing function: if the loop ended without a final text answer (a reasoning
	// model can keep calling tools until the turn cap, especially on large files),
	// make one more pass with NO tools so the subagent must synthesize a final
	// answer from what it gathered instead of returning empty.
	if runErr == nil && strings.TrimSpace(finalText) == "" && len(conversation) > 1 && !a.subagentGenerationStale(generation) {
		forced := append(append([]neoHistoryMessage(nil), conversation...),
			neoHistoryMessage{Role: "user", Text: "You have gathered enough context. Write your complete final answer now based on what you have. Do NOT call any tools."})
		routeCopy := route
		result, err := a.runtime.subagentInfer(neoInferenceRequest{
			ActorID:              a.id,
			ThreadID:             a.threadID,
			MessageID:            newNeoMessageID(),
			AgentMode:            agentMode,
			ReasoningEffort:      def.ReasoningEffort,
			ParentToolCallID:     parentToolCallID,
			MaxTokens:            maxTokens,
			Settings:             settings,
			History:              forced,
			Environment:          environment,
			ModelRouteOverride:   &routeCopy,
			SystemPromptOverride: systemPrompt,
		}, func(neoInferenceDelta) {})
		if err == nil {
			finalText = result.Text
		}
		log.Debugf("amp neo subagent force-synthesis tool=%s text_len=%d err=%v", name, len(strings.TrimSpace(result.Text)), err)
	}

	log.Debugf("amp neo subagent done tool=%s depth=%d thread=%s call=%s result_len=%d err=%v", name, depth, a.threadID, parentToolCallID, len(strings.TrimSpace(finalText)), runErr)
	return finalText, runErr
}

// resolveSubagentToolsLocked maps a subagent's includeTools names to the tool
// specs the executor registered. Leaf tools the executor cannot run are skipped.
func (a *neoActor) resolveSubagentToolsLocked(names []string) []neoToolSpec {
	tools := make([]neoToolSpec, 0, len(names))
	for _, name := range names {
		if spec, ok := a.tools[name]; ok {
			tools = append(tools, spec)
			continue
		}
		if a.executorBootstrapComplete {
			if spec, ok := neoSyntheticLocalToolSpec(name); ok {
				tools = append(tools, spec)
			}
		}
	}
	return tools
}

func (a *neoActor) storeSubagentToolUseMessage(call neoToolCall, parentToolCallID string) string {
	messageID := newNeoMessageID()
	a.mu.Lock()
	_, event := a.storeMessageEventLocked(neoMessage{
		ThreadID:        a.threadID,
		Role:            "assistant",
		MessageID:       messageID,
		Content:         []any{neoToolUseBlock(call, true)},
		State:           map[string]any{"type": "complete", "stopReason": "tool_use"},
		CreatedAt:       time.Now().UTC().Format(time.RFC3339Nano),
		ParentToolUseID: parentToolCallID,
	})
	a.mu.Unlock()

	a.broadcast(event)
	a.syncCloudAsync()
	return messageID
}

func (a *neoActor) storeSubagentToolResultMessage(toolCallID string, run map[string]any, parentToolCallID, completionStatus string) {
	a.mu.Lock()
	_, event := a.storeMessageEventLocked(neoMessage{
		ThreadID:         a.threadID,
		Role:             "user",
		MessageID:        toolResultMessageID(toolCallID),
		Content:          []any{map[string]any{"type": "tool_result", "toolUseID": toolCallID, "run": run}},
		CreatedAt:        time.Now().UTC().Format(time.RFC3339Nano),
		ParentToolUseID:  parentToolCallID,
		CompletionStatus: completionStatus,
	})
	a.mu.Unlock()

	a.broadcast(event)
	a.syncCloudAsync()
}

// execSubagentLeafTool leases a subagent's leaf-tool call to the executor and
// blocks until the executor returns a terminal run, routed back via a waiter.
func (a *neoActor) execSubagentLeafTool(call neoToolCall, parentToolCallID, childMessageID string, generation int) map[string]any {
	ch := make(chan map[string]any, 1)
	a.mu.Lock()
	if a.subagentWaiters == nil {
		a.subagentWaiters = map[string]chan map[string]any{}
	}
	if a.subagentTools == nil {
		a.subagentTools = map[string]neoPendingTool{}
	}
	a.subagentWaiters[call.ID] = ch
	a.subagentTools[call.ID] = neoPendingTool{
		ID:               call.ID,
		Name:             call.Name,
		Input:            call.Input,
		MessageID:        childMessageID,
		ParentToolCallID: parentToolCallID,
	}
	a.mu.Unlock()

	a.broadcast(withNeoParentToolCallID(map[string]any{
		"type":       "tool_lease",
		"toolCallId": call.ID,
		"toolName":   call.Name,
		"args":       call.Input,
		"messageId":  childMessageID,
	}, parentToolCallID))

	select {
	case run := <-ch:
		return run
	case <-time.After(neoSubagentToolTimeout):
		a.mu.Lock()
		delete(a.subagentWaiters, call.ID)
		delete(a.subagentTools, call.ID)
		a.mu.Unlock()
		return map[string]any{"status": "error", "error": map[string]any{"message": "subagent tool " + call.Name + " timed out"}}
	}
}

// routeSubagentLeafToolResult delivers an executor tool result to the waiting
// subagent loop. Returns true when the result was consumed by a subagent.
func (a *neoActor) routeSubagentLeafToolResult(toolCallID string, run map[string]any) bool {
	a.mu.Lock()
	ch, ok := a.subagentWaiters[toolCallID]
	pending := neoPendingTool{ID: toolCallID}
	if a.subagentTools != nil {
		if tracked, trackedOK := a.subagentTools[toolCallID]; trackedOK {
			pending = tracked
		}
	}
	a.mu.Unlock()
	if !ok {
		return false
	}
	if !neoToolRunTerminalForPending(pending, run) {
		a.storeSubagentToolResultMessage(toolCallID, run, pending.ParentToolCallID, "tool_progress")
		return true
	}
	a.mu.Lock()
	delete(a.subagentWaiters, toolCallID)
	delete(a.subagentTools, toolCallID)
	a.mu.Unlock()
	normalized := normalizeNeoExecutorToolRun(context.Background(), a.runtime, pending, run, a.threadID)
	a.storeSubagentToolResultMessage(toolCallID, normalized, pending.ParentToolCallID, "")
	select {
	case ch <- normalized:
	default:
	}
	a.broadcast(map[string]any{"type": "executor_tool_result_ack", "toolCallId": toolCallID})
	return true
}

func neoRunIsTerminal(run map[string]any) bool {
	switch strings.ToLower(strings.TrimSpace(stringValue(run["status"]))) {
	case "done", "error", "cancelled", "canceled", "complete", "completed", "success", "failed":
		return true
	default:
		return false
	}
}

// deliverSubagentResult completes the parent subagent tool call with the
// subagent's final message and resumes the parent inference loop. Mirrors the
// terminal path of receiveToolResult.
func (a *neoActor) deliverSubagentResult(parent neoPendingTool, text string, runErr error) {
	var run map[string]any
	if runErr != nil {
		run = map[string]any{"status": "error", "error": map[string]any{"message": runErr.Error()}}
	} else {
		run = map[string]any{"status": "done", "output": strings.TrimSpace(text)}
	}
	a.deliverSubagentRun(parent, run)
}

// deliverSubagentRun completes the parent subagent tool call with an explicit
// terminal run payload and resumes the parent inference loop.
func (a *neoActor) deliverSubagentRun(parent neoPendingTool, run map[string]any) {
	a.mu.Lock()
	if _, ok := a.pendingTools[parent.ID]; !ok {
		a.mu.Unlock()
		return
	}
	delete(a.pendingTools, parent.ID)
	_, event := a.storeMessageEventLocked(neoMessage{
		ThreadID:        a.threadID,
		Role:            "user",
		MessageID:       toolResultMessageID(parent.ID),
		Content:         []any{map[string]any{"type": "tool_result", "toolUseID": parent.ID, "run": run}},
		CreatedAt:       time.Now().UTC().Format(time.RFC3339Nano),
		ParentToolUseID: parent.ParentToolCallID,
	})
	a.history = append(a.history, neoHistoryMessage{
		Role:            "tool",
		ToolCallID:      parent.ID,
		ToolName:        parent.Name,
		Text:            runToText(run),
		Content:         neoToolRunHistoryContent(run),
		ParentToolUseID: parent.ParentToolCallID,
	})
	remaining := len(a.pendingTools)
	ready := a.executorReady
	a.mu.Unlock()

	a.broadcast(event)
	a.syncCloudAsync()
	if remaining == 0 && ready {
		go a.runInferenceForParent(parent.AgentMode, parent.ReasoningEffort, parent.ParentToolCallID)
	}
}

func (a *neoActor) subagentGenerationStale(generation int) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.generation != generation
}

// neoSubagentInputText renders the subagent tool call args into the first user
// message of the subagent conversation, matching how the binary seeds each one.
func neoSubagentInputText(toolName string, input map[string]any) string {
	switch toolName {
	case "oracle":
		var b strings.Builder
		if task := stringValue(input["task"]); task != "" {
			b.WriteString(task)
		}
		if ctx := stringValue(input["context"]); ctx != "" {
			b.WriteString("\n\n## Context\n")
			b.WriteString(ctx)
		}
		if files := neoStringSlice(input["files"]); len(files) > 0 {
			b.WriteString("\n\n## Relevant files\n")
			for _, f := range files {
				b.WriteString("- ")
				b.WriteString(f)
				b.WriteString("\n")
			}
		}
		return strings.TrimSpace(b.String())
	case "librarian":
		query := firstNonEmptyString(input["query"], input["task"])
		if ctx := stringValue(input["context"]); ctx != "" {
			return strings.TrimSpace(query + "\n\n## Context\n" + ctx)
		}
		return query
	case "run_check":
		var b strings.Builder
		b.WriteString("Run this review check against the changes under review.\n\n")
		frontmatter, _ := json.Marshal(mapValue(input["frontmatter"]))
		checkURI := stringValue(input["checkURI"])
		checkContent := strings.TrimSpace(stringValue(input["checkContent"]))
		fmt.Fprintf(&b, "<check name=%q uri=%q>\n<frontmatter>%s</frontmatter>\n",
			stringValue(input["checkName"]), checkURI, frontmatter)
		if checkContent != "" {
			fmt.Fprintf(&b, "<content>\n%s\n</content>\n", checkContent)
		}
		b.WriteString("</check>\n")
		if checkContent == "" {
			b.WriteString("\nCheck definition content was not embedded. Read the check definition from the check URI before evaluating it.")
			if checkURI != "" {
				b.WriteString(" For file:// URIs, pass the decoded filesystem path to Read.")
			}
			b.WriteString("\n")
		}
		if instructions := stringValue(input["instructions"]); strings.TrimSpace(instructions) != "" {
			b.WriteString("\nAdditional instructions from the review request:\n")
			b.WriteString(strings.TrimSpace(instructions))
			b.WriteString("\n")
		}
		if diff := stringValue(input["diffDescription"]); diff != "" {
			b.WriteString("\nDiff under review: ")
			b.WriteString(diff)
			b.WriteString("\n")
		}
		if files := neoStringSlice(input["files"]); len(files) > 0 {
			b.WriteString("\nFiles under review:\n")
			for _, f := range files {
				b.WriteString("- ")
				b.WriteString(f)
				b.WriteString("\n")
			}
		}
		return strings.TrimSpace(b.String())
	default: // finder and any other query-shaped subagent
		return firstNonEmptyString(input["query"], input["task"], input["prompt"], input["description"])
	}
}

// readSubagentFiles reads each referenced file through the executor (which has
// workspace access) and renders them as attached content for the subagent's
// first turn, mirroring how the binary attaches oracle file mentions.
func (a *neoActor) readSubagentFiles(files []string, parentToolCallID, parentMessageID string, generation int) string {
	var b strings.Builder
	for _, path := range files {
		path = strings.TrimSpace(path)
		if path == "" || a.subagentGenerationStale(generation) {
			continue
		}
		call := neoToolCall{ID: newNeoToolCallID(), Name: "Read", Input: map[string]any{"path": path}}
		content := strings.TrimSpace(runToText(a.execSubagentLeafTool(call, parentToolCallID, parentMessageID, generation)))
		if content == "" {
			continue
		}
		b.WriteString("\n\n<file path=\"")
		b.WriteString(path)
		b.WriteString("\">\n")
		b.WriteString(content)
		b.WriteString("\n</file>")
	}
	if b.Len() == 0 {
		return ""
	}
	return "The following files are attached for your analysis:" + b.String()
}

// neoSubagentToolCallTarget extracts the primary argument of a leaf tool call
// (path/pattern/query/cmd) for diagnostic logging of subagent progress.
func neoSubagentToolCallTarget(call neoToolCall) string {
	for _, key := range []string{"path", "filePath", "pattern", "query", "cmd", "command", "url"} {
		if v := stringValue(call.Input[key]); v != "" {
			if len(v) > 48 {
				v = v[:48]
			}
			return v
		}
	}
	return ""
}

func neoStringSlice(value any) []string {
	switch v := value.(type) {
	case []string:
		return v
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			if s := stringValue(item); s != "" {
				out = append(out, s)
			}
		}
		return out
	case string:
		if v == "" {
			return nil
		}
		return []string{v}
	default:
		return nil
	}
}
