package amp

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
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

var neoSubagentDefs = map[string]neoSubagentDef{
	"finder": {
		Key:             "finder",
		DisplayName:     "Finder",
		Route:           neoModelRoute{Provider: "openai", Model: "gpt-5.6-terra"},
		IncludeTools:    []string{"Grep", "glob", "Read"},
		SystemPrompt:    neoFinderSubagentPrompt,
		ReasoningEffort: "low",
		MaxTurns:        6,
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
		Route:           neoModelRoute{Provider: "openai", Model: "gpt-5.6-sol"},
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
	"run_check": {
		Key:             "run_check",
		DisplayName:     "Check",
		Route:           neoModelRoute{Provider: "openai", Model: "gpt-5.6-terra"},
		IncludeTools:    []string{"Read", "Grep", "glob", "Bash"},
		SystemPrompt:    neoRunCheckSubagentPrompt,
		ReasoningEffort: "low",
		MaxTurns:        12,
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
	neoSubagentToolTimeout       = 5 * time.Minute
	neoFinderMaxConcurrentRuns   = 2
	neoFinderMaxToolCallsPerTurn = 4
	neoFinderGlobWalkEntryLimit  = 4096
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
	runContext := context.Background()
	if name == "finder" {
		finderContext, finderRunID, acquired := a.acquireFinderRun(generation)
		if !acquired {
			if a.subagentGenerationStale(generation) {
				return "", nil
			}
			return "", fmt.Errorf("finder concurrency limit reached; retry after an active search completes")
		}
		runContext = finderContext
		defer a.releaseFinderRun(finderRunID)
	}

	a.mu.Lock()
	route := def.Route
	if route.Model == "" {
		route = selectNeoModelRouteWithConfig(a.runtime, a.currentAgentMode, a.settings)
	}
	agentMode := a.currentAgentMode
	tools := a.resolveSubagentToolsLocked(def.IncludeTools)
	settings := cloneMap(a.settings)
	if def.ReasoningEffort != "" {
		settings["reasoning.effort"] = def.ReasoningEffort
		if (route.Provider == "google" || route.Provider == "vertexai") && validNeoGeminiThinkingLevel(def.ReasoningEffort) {
			settings["gemini.thinkingLevel"] = def.ReasoningEffort
		}
	}
	environment := cloneMap(a.environment)
	maxTokens := a.maxTokens
	a.mu.Unlock()
	workingDir, workspaceRoot := neoFinderEnvironmentPaths(environment)
	finderExecutorRoot := ""
	if name == "finder" {
		var err error
		finderExecutorRoot, err = neoFinderCanonicalDirectory(firstNonEmptyString(workspaceRoot, workingDir))
		if err != nil {
			return "", fmt.Errorf("finder requires a valid executor workspace root: %w", err)
		}
		workingDir, workspaceRoot, err = neoFinderWorkspaceScope(workingDir, workspaceRoot)
		if err != nil {
			return "", err
		}
		if !neoFinderPathWithin(finderExecutorRoot, workspaceRoot) {
			return "", fmt.Errorf("finder root %q is outside executor workspace root %q", workspaceRoot, finderExecutorRoot)
		}
		environment["workingDirectory"] = workingDir
		environment["workspaceRoot"] = workspaceRoot
		environment["cwd"] = workingDir
		if !neoFinderReadTargetVerifiable(workspaceRoot) {
			tools = neoFinderWithoutTool(tools, "Read")
		}
	}

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
			Context:              runContext,
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
		if a.subagentGenerationStale(generation) {
			return "", nil
		}
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
		if name == "finder" {
			finderCalls, duplicateCallIDs := neoFinderUniqueToolCallIDs(result.ToolCalls)
			conversation[len(conversation)-1].ToolCalls = finderCalls
			exchanges := a.execPreparedFinderTurnTools(runContext, finderCalls, duplicateCallIDs, workspaceRoot, finderExecutorRoot, parentToolCallID, generation)
			if a.subagentGenerationStale(generation) {
				return "", nil
			}
			for _, exchange := range exchanges {
				conversation = append(conversation, neoHistoryMessage{
					Role:            "tool",
					ToolCallID:      exchange.Call.ID,
					ToolName:        exchange.Call.Name,
					Text:            runToText(exchange.Run),
					Content:         neoToolRunHistoryContent(exchange.Run),
					ParentToolUseID: parentToolCallID,
				})
			}
			continue
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
				readPending := neoPendingTool{ID: call.ID, Name: call.Name, Input: call.Input, AgentMode: agentMode, ParentToolCallID: parentToolCallID, MessageID: childMessageID, ClientAPIKey: clientAPIKey}
				text, err := a.executeLocalReadThreadWithProgress(readPending, generation, func(statusMessage string) {
					a.storeSubagentToolResultMessage(call.ID, neoReadThreadProgressRun(statusMessage), parentToolCallID, "tool_progress")
				})
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
			Context:              runContext,
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
		if a.subagentGenerationStale(generation) {
			return "", nil
		}
		if err == nil {
			finalText = result.Text
		} else {
			runErr = err
		}
		log.Debugf("amp neo subagent force-synthesis tool=%s text_len=%d err=%v", name, len(strings.TrimSpace(result.Text)), err)
	}

	log.Debugf("amp neo subagent done tool=%s depth=%d thread=%s call=%s result_len=%d err=%v", name, depth, a.threadID, parentToolCallID, len(strings.TrimSpace(finalText)), runErr)
	return finalText, runErr
}

type neoFinderRun struct {
	generation int
	cancel     context.CancelFunc
}

func (a *neoActor) acquireFinderRun(generation int) (context.Context, uint64, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if generation != a.generation || a.activeFinderRuns >= neoFinderMaxConcurrentRuns {
		return nil, 0, false
	}
	ctx, cancel := context.WithCancel(context.Background())
	if a.finderRuns == nil {
		a.finderRuns = map[uint64]neoFinderRun{}
	}
	a.finderRunSeq++
	runID := a.finderRunSeq
	a.finderRuns[runID] = neoFinderRun{generation: generation, cancel: cancel}
	a.activeFinderRuns = len(a.finderRuns)
	return ctx, runID, true
}

func (a *neoActor) releaseFinderRun(runID uint64) {
	a.mu.Lock()
	if run, ok := a.finderRuns[runID]; ok {
		delete(a.finderRuns, runID)
		run.cancel()
	}
	a.activeFinderRuns = len(a.finderRuns)
	a.mu.Unlock()
}

func (a *neoActor) advanceGenerationLocked() int {
	a.generation++
	for runID, run := range a.finderRuns {
		if run.generation == a.generation {
			continue
		}
		delete(a.finderRuns, runID)
		run.cancel()
	}
	a.activeFinderRuns = len(a.finderRuns)
	return a.generation
}

type neoFinderTurnTool struct {
	original       neoToolCall
	executable     neoToolCall
	childMessageID string
	run            map[string]any
	waiter         chan map[string]any
}

func (a *neoActor) execFinderTurnTools(ctx context.Context, calls []neoToolCall, workspaceRoot, executorRoot, parentToolCallID string, generation int) []neoSubagentToolExchange {
	calls, duplicateCallIDs := neoFinderUniqueToolCallIDs(calls)
	return a.execPreparedFinderTurnTools(ctx, calls, duplicateCallIDs, workspaceRoot, executorRoot, parentToolCallID, generation)
}

func (a *neoActor) execPreparedFinderTurnTools(ctx context.Context, calls []neoToolCall, duplicateCallIDs map[string]string, workspaceRoot, executorRoot, parentToolCallID string, generation int) []neoSubagentToolExchange {
	items := make([]neoFinderTurnTool, 0, len(calls))
	executable := 0
	for _, call := range calls {
		if call.Incomplete {
			continue
		}
		item := neoFinderTurnTool{original: call, executable: call}
		if duplicateID, duplicate := duplicateCallIDs[call.ID]; duplicate {
			item.run = neoFinderToolError(fmt.Sprintf("finder received duplicate tool call id %q", duplicateID))
			items = append(items, item)
			continue
		}
		if executable >= neoFinderMaxToolCallsPerTurn {
			item.run = neoFinderToolError(fmt.Sprintf("finder accepts at most %d tool calls per turn; narrow the remaining searches", neoFinderMaxToolCallsPerTurn))
			items = append(items, item)
			continue
		}
		executable++
		scoped, err := neoScopeFinderToolCallForExecutorContext(ctx, call, workspaceRoot, executorRoot)
		if err != nil {
			item.run = neoFinderToolError(err.Error())
			items = append(items, item)
			continue
		}
		scoped.ID = newNeoToolCallID()
		item.executable = scoped
		items = append(items, item)
	}
	if !a.prepareFinderTurnTools(items, parentToolCallID, generation) {
		return nil
	}

	var wg sync.WaitGroup
	for index := range items {
		if items[index].run != nil {
			continue
		}
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			item := &items[index]
			item.run = a.waitSubagentLeafTool(item.executable, parentToolCallID, item.waiter, generation)
		}(index)
	}
	wg.Wait()

	exchanges := make([]neoSubagentToolExchange, 0, len(items))
	for _, item := range items {
		exchanges = append(exchanges, neoSubagentToolExchange{Call: item.original, Run: item.run})
	}
	return exchanges
}

func neoFinderUniqueToolCallIDs(calls []neoToolCall) ([]neoToolCall, map[string]string) {
	counts := make(map[string]int, len(calls))
	for _, call := range calls {
		if !call.Incomplete {
			counts[call.ID]++
		}
	}
	normalized := append([]neoToolCall(nil), calls...)
	duplicates := make(map[string]string)
	for index := range normalized {
		call := &normalized[index]
		if call.Incomplete || counts[call.ID] <= 1 {
			continue
		}
		originalID := call.ID
		call.ID = newNeoToolCallID()
		duplicates[call.ID] = originalID
	}
	return normalized, duplicates
}

func (a *neoActor) prepareFinderTurnTools(items []neoFinderTurnTool, parentToolCallID string, generation int) bool {
	a.emissionMu.Lock()
	a.mu.Lock()
	if generation != a.generation {
		a.mu.Unlock()
		a.emissionMu.Unlock()
		return false
	}
	if a.subagentWaiters == nil {
		a.subagentWaiters = map[string]chan map[string]any{}
	}
	if a.subagentTools == nil {
		a.subagentTools = map[string]neoPendingTool{}
	}
	sockets := a.socketListLocked()
	emissions := make([]map[string]any, 0, len(items)*2)
	for index := range items {
		item := &items[index]
		item.childMessageID = newNeoMessageID()
		_, useEvent := a.storeMessageEventLocked(neoMessage{
			ThreadID:        a.threadID,
			Role:            "assistant",
			MessageID:       item.childMessageID,
			Content:         []any{neoToolUseBlock(item.executable, true)},
			State:           map[string]any{"type": "complete", "stopReason": "tool_use"},
			CreatedAt:       time.Now().UTC().Format(time.RFC3339Nano),
			ParentToolUseID: parentToolCallID,
		})
		emissions = append(emissions, useEvent)
		if item.run != nil {
			emissions = append(emissions, a.storeSubagentToolResultMessageLocked(item.original.ID, item.run, parentToolCallID, ""))
			continue
		}
		item.waiter = make(chan map[string]any, 1)
		a.subagentWaiters[item.executable.ID] = item.waiter
		a.subagentTools[item.executable.ID] = neoPendingTool{
			ID:               item.executable.ID,
			Name:             item.executable.Name,
			Input:            item.executable.Input,
			MessageID:        item.childMessageID,
			ParentToolCallID: parentToolCallID,
		}
		emissions = append(emissions, withNeoParentToolCallID(map[string]any{
			"type":       "tool_lease",
			"toolCallId": item.executable.ID,
			"toolName":   item.executable.Name,
			"args":       item.executable.Input,
			"messageId":  item.childMessageID,
		}, parentToolCallID))
	}
	a.mu.Unlock()
	for _, payload := range emissions {
		for _, socket := range sockets {
			if socket == nil || !socket.canSend() {
				continue
			}
			socket.send(payload)
		}
	}
	a.emissionMu.Unlock()
	if len(emissions) > 0 {
		a.syncCloudAsync()
	}
	return true
}

func neoFinderToolError(message string) map[string]any {
	return map[string]any{"status": "error", "error": map[string]any{"message": message}}
}

func neoFinderWorkspaceScope(workingDir, workspaceRoot string) (string, string, error) {
	root, err := neoFinderCanonicalDirectory(firstNonEmptyString(workspaceRoot, workingDir))
	if err != nil {
		return "", "", fmt.Errorf("finder requires a valid workspace root: %w", err)
	}
	work := root
	if strings.TrimSpace(workingDir) != "" {
		work, err = neoFinderCanonicalDirectory(workingDir)
		if err != nil {
			return "", "", fmt.Errorf("finder requires a valid working directory: %w", err)
		}
	}
	if !neoFinderPathWithin(root, work) {
		return "", "", fmt.Errorf("finder working directory %q is outside workspace root %q", work, root)
	}
	if neoFinderPathIsRoot(root) {
		return "", "", fmt.Errorf("finder refuses filesystem-root workspace %q", root)
	}
	if home, homeErr := os.UserHomeDir(); homeErr == nil {
		if canonicalHome, canonicalErr := neoFinderCanonicalDirectory(home); canonicalErr == nil && neoFinderPathEqual(root, canonicalHome) {
			return "", "", fmt.Errorf("finder refuses home-directory workspace %q; select a repository or narrower workspace", root)
		}
	}
	return work, root, nil
}

func neoFinderCanonicalDirectory(value string) (string, error) {
	resolved, err := neoFinderPathValue(value)
	if err != nil {
		return "", err
	}
	if !neoFinderPathIsAbsolute(resolved) {
		return "", fmt.Errorf("path %q is not absolute", resolved)
	}
	localPath, err := neoFinderLocalPath(resolved)
	if err != nil {
		return resolved, nil
	}
	realPath, err := filepath.EvalSymlinks(localPath)
	if err != nil {
		return resolved, nil
	}
	canonical, err := neoFinderPathValue(filepath.ToSlash(realPath))
	if err != nil {
		return "", err
	}
	if neoFinderPathIsAbsolute(canonical) {
		return canonical, nil
	}
	return resolved, nil
}

func neoFinderEnvironmentPaths(environment map[string]any) (string, string) {
	initial := mapValue(environment["initial"])
	workingDir := neoFinderFirstEnvironmentPath(
		environment["workingDirectory"],
		environment["working_directory"],
		environment["cwd"],
		initial["workingDirectory"],
		initial["working_directory"],
		initial["cwd"],
	)
	workspaceRoot := neoFinderFirstEnvironmentPath(
		environment["workspaceRoot"],
		environment["workspace_root"],
		initial["workspaceRoot"],
		initial["workspace_root"],
	)
	if workingDir == "" || workspaceRoot == "" {
		treePath := neoFinderFirstTreePath(environment["trees"], initial["trees"])
		if workingDir == "" {
			workingDir = treePath
		}
		if workspaceRoot == "" {
			workspaceRoot = treePath
		}
	}
	if workingDir == "" {
		workingDir = workspaceRoot
	}
	if workspaceRoot == "" {
		workspaceRoot = workingDir
	}
	return workingDir, workspaceRoot

}

func neoFinderFirstEnvironmentPath(values ...any) string {
	for _, value := range values {
		candidate := firstNonEmptyString(
			value,
			nestedString(value, "uri"),
			nestedString(value, "path"),
			nestedString(value, "fsPath"),
			nestedString(value, "workingDirectory"),
			nestedString(value, "workspaceRoot"),
		)
		if strings.TrimSpace(candidate) != "" {
			return candidate
		}
	}
	return ""
}

func neoFinderFirstTreePath(treeSets ...any) string {
	for _, rawTrees := range treeSets {
		for _, tree := range arrayValue(rawTrees) {
			if candidate := neoFinderFirstEnvironmentPath(tree); candidate != "" {
				return candidate
			}
		}
	}
	return ""
}

func neoFinderPathValue(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("path is empty")
	}
	if strings.HasPrefix(strings.ToLower(value), "file:") {
		parsed, err := url.Parse(value)
		if err != nil {
			return "", fmt.Errorf("parse file URI: %w", err)
		}
		if !strings.EqualFold(parsed.Scheme, "file") {
			return "", fmt.Errorf("unsupported path URI scheme %q", parsed.Scheme)
		}
		decoded, err := url.PathUnescape(parsed.EscapedPath())
		if err != nil {
			return "", fmt.Errorf("decode file URI path: %w", err)
		}
		if parsed.Host != "" && !strings.EqualFold(parsed.Host, "localhost") {
			value = "//" + parsed.Host + "/" + strings.TrimPrefix(decoded, "/")
		} else {
			value = decoded
		}
		if len(value) >= 3 && value[0] == '/' && neoFinderWindowsDrivePath(value[1:]) {
			value = value[1:]
		}
	} else if parsed, err := url.Parse(value); err == nil && parsed.Scheme != "" && !neoFinderWindowsDrivePath(value) {
		return "", fmt.Errorf("unsupported path URI scheme %q", parsed.Scheme)
	}
	value = strings.ReplaceAll(value, `\`, "/")
	unc := strings.HasPrefix(value, "//")
	value = path.Clean(value)
	if unc && !strings.HasPrefix(value, "//") {
		value = "/" + value
	}
	if neoFinderWindowsDrivePath(value) {
		value = strings.ToUpper(value[:1]) + value[1:]
	}
	return value, nil
}

func neoFinderWindowsDrivePath(value string) bool {
	return len(value) >= 3 && ((value[0] >= 'A' && value[0] <= 'Z') || (value[0] >= 'a' && value[0] <= 'z')) && value[1] == ':' && (value[2] == '/' || value[2] == '\\')
}

func neoFinderPathIsAbsolute(value string) bool {
	return strings.HasPrefix(value, "/") || neoFinderWindowsDrivePath(value)
}

func neoFinderPathIsRoot(value string) bool {
	return value == "/" || (neoFinderWindowsDrivePath(value) && len(value) == 3)
}

func neoFinderJoin(root, value string) string {
	joined, err := neoFinderPathValue(strings.TrimSuffix(root, "/") + "/" + value)
	if err != nil {
		return ""
	}
	return joined
}

func neoFinderRelative(root, target string) (string, bool) {
	if !neoFinderPathWithin(root, target) {
		return "", false
	}
	if neoFinderPathEqual(root, target) {
		return ".", true
	}
	return strings.TrimPrefix(target[len(strings.TrimSuffix(root, "/")):], "/"), true
}

func neoScopeFinderToolCall(call neoToolCall, workspaceRoot string) (neoToolCall, error) {
	return neoScopeFinderToolCallForExecutorContext(context.Background(), call, workspaceRoot, workspaceRoot)
}

func neoScopeFinderToolCallForExecutor(call neoToolCall, workspaceRoot, executorRoot string) (neoToolCall, error) {
	return neoScopeFinderToolCallForExecutorContext(context.Background(), call, workspaceRoot, executorRoot)
}

func neoScopeFinderToolCallForExecutorContext(ctx context.Context, call neoToolCall, workspaceRoot, executorRoot string) (neoToolCall, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if !neoFinderPathWithin(executorRoot, workspaceRoot) {
		return call, fmt.Errorf("finder workspace root %q is outside executor root %q", workspaceRoot, executorRoot)
	}
	call.Input = cloneMap(call.Input)
	switch call.Name {
	case "Read":
		key := neoFinderInputPathKey(call.Input, "path", "filePath", "file_path")
		if key == "" {
			return call, fmt.Errorf("finder Read requires a path inside workspace %q", workspaceRoot)
		}
		scoped, err := neoFinderScopedReadPath(workspaceRoot, stringValue(call.Input[key]))
		if err != nil {
			return call, err
		}
		call.Input[key] = scoped
	case "Grep":
		key := neoFinderInputPathKey(call.Input, "path", "directory")
		patternKey := neoFinderInputPathKey(call.Input, "glob", "filePattern")
		if key != "" && patternKey != "" {
			return call, fmt.Errorf("finder Grep path and glob are mutually exclusive")
		}
		if key == "" && patternKey == "" {
			call.Input["path"] = workspaceRoot
		} else if key != "" {
			scoped, err := neoFinderScopedSearchPath(workspaceRoot, stringValue(call.Input[key]))
			if err != nil {
				return call, err
			}
			call.Input[key] = scoped
		}
		if patternKey != "" {
			scoped, err := neoFinderScopedGlobPattern(ctx, workspaceRoot, executorRoot, stringValue(call.Input[patternKey]))
			if err != nil {
				return call, err
			}
			call.Input[patternKey] = scoped
		}
	case "glob":
		key := neoFinderInputPathKey(call.Input, "filePattern", "pattern", "glob")
		if key == "" {
			return call, fmt.Errorf("finder glob requires a file pattern inside workspace %q", workspaceRoot)
		}
		scoped, err := neoFinderScopedGlobPattern(ctx, workspaceRoot, executorRoot, stringValue(call.Input[key]))
		if err != nil {
			return call, err
		}
		call.Input[key] = scoped
	default:
		return call, fmt.Errorf("finder cannot execute unsupported tool %q", call.Name)
	}
	return call, nil
}

func neoFinderWithoutTool(tools []neoToolSpec, name string) []neoToolSpec {
	out := make([]neoToolSpec, 0, len(tools))
	for _, tool := range tools {
		if tool.Name != name {
			out = append(out, tool)
		}
	}
	return out
}

func neoFinderInputPathKey(input map[string]any, keys ...string) string {
	for _, key := range keys {
		if strings.TrimSpace(stringValue(input[key])) != "" {
			return key
		}
	}
	return ""
}

func neoFinderScopedPath(root, value string) (string, error) {
	resolved, err := neoFinderPathValue(value)
	if err != nil {
		return "", fmt.Errorf("resolve finder path %q: %w", value, err)
	}
	if resolved == "" {
		return "", fmt.Errorf("finder path is empty")
	}
	if !neoFinderPathIsAbsolute(resolved) {
		resolved = neoFinderJoin(root, resolved)
	}
	if !neoFinderPathWithin(root, resolved) {
		return "", fmt.Errorf("finder path %q is outside workspace root %q", resolved, root)
	}
	return resolved, nil
}

func neoFinderReadTargetVerifiable(root string) bool {
	localRoot, err := neoFinderLocalPath(root)
	if err != nil {
		return false
	}
	_, err = filepath.EvalSymlinks(localRoot)
	return err == nil
}

func neoFinderScopedReadPath(root, value string) (string, error) {
	scoped, err := neoFinderScopedPath(root, value)
	if err != nil {
		return "", err
	}
	localRoot, err := neoFinderLocalPath(root)
	if err != nil {
		return "", fmt.Errorf("finder Read is unavailable because workspace %q cannot be verified on the proxy host", root)
	}
	localTarget, err := neoFinderLocalPath(scoped)
	if err != nil {
		return "", fmt.Errorf("finder Read path %q cannot be verified on the proxy host", scoped)
	}
	realRoot, err := filepath.EvalSymlinks(localRoot)
	if err != nil {
		return "", fmt.Errorf("finder Read is unavailable because workspace %q cannot be verified on the proxy host", root)
	}
	realTarget, err := filepath.EvalSymlinks(localTarget)
	if err != nil {
		return "", fmt.Errorf("finder Read path %q cannot be verified: %w", scoped, err)
	}
	canonicalRoot, err := neoFinderPathValue(filepath.ToSlash(realRoot))
	if err != nil {
		return "", err
	}
	canonicalTarget, err := neoFinderPathValue(filepath.ToSlash(realTarget))
	if err != nil {
		return "", err
	}
	if !neoFinderPathWithin(canonicalRoot, canonicalTarget) {
		return "", fmt.Errorf("finder Read path %q resolves outside workspace root %q", scoped, root)
	}
	return canonicalTarget, nil
}

func neoFinderScopedSearchPath(root, value string) (string, error) {
	scoped, err := neoFinderScopedPath(root, value)
	if err != nil {
		return "", err
	}
	if neoFinderPathEqual(root, scoped) {
		return scoped, nil
	}
	localRoot, err := neoFinderLocalPath(root)
	if err != nil {
		return scoped, nil
	}
	localTarget, err := neoFinderLocalPath(scoped)
	if err != nil {
		return "", fmt.Errorf("finder Grep path %q cannot be verified on the proxy host", scoped)
	}
	realRoot, err := filepath.EvalSymlinks(localRoot)
	if err != nil {
		if os.IsNotExist(err) {
			return scoped, nil
		}
		return "", fmt.Errorf("finder Grep path %q cannot be verified on the proxy host", scoped)
	}
	realTarget, err := filepath.EvalSymlinks(localTarget)
	if err != nil {
		return "", fmt.Errorf("finder Grep path %q cannot be verified: %w", scoped, err)
	}
	canonicalRoot, err := neoFinderPathValue(filepath.ToSlash(realRoot))
	if err != nil {
		return "", err
	}
	canonicalTarget, err := neoFinderPathValue(filepath.ToSlash(realTarget))
	if err != nil {
		return "", err
	}
	if !neoFinderPathWithin(canonicalRoot, canonicalTarget) {
		return "", fmt.Errorf("finder Grep path %q resolves outside workspace root %q", scoped, root)
	}
	return canonicalTarget, nil
}

func neoFinderLocalPath(value string) (string, error) {
	canonical, err := neoFinderPathValue(value)
	if err != nil {
		return "", err
	}
	if neoFinderWindowsDrivePath(canonical) && filepath.Separator != '\\' {
		return "", fmt.Errorf("Windows path is not local to this host")
	}
	if strings.HasPrefix(canonical, "//") && filepath.Separator != '\\' {
		return "", fmt.Errorf("UNC path is not local to this host")
	}
	return filepath.FromSlash(canonical), nil
}

func neoFinderScopedGlobPattern(ctx context.Context, root, executorRoot, value string) (string, error) {
	protectedValue, escapes := neoFinderProtectGlobEscapes(value)
	parsedValue, err := neoFinderPathValue(protectedValue)
	if err != nil {
		return "", fmt.Errorf("resolve finder glob pattern %q: %w", value, err)
	}
	if parsedValue == "" {
		return "", fmt.Errorf("finder glob pattern is empty")
	}
	expandedPatterns, err := neoFinderBraceExpand(parsedValue, 64)
	if err != nil {
		return "", fmt.Errorf("resolve finder glob pattern %q: %w", value, err)
	}
	scopedPatterns := make([]string, 0, len(expandedPatterns))
	for _, expanded := range expandedPatterns {
		for _, part := range strings.Split(expanded, "/") {
			if part == ".." {
				return "", fmt.Errorf("finder glob pattern %q cannot traverse parent directories", parsedValue)
			}
		}
		resolvedExpansion, err := neoFinderScopedPath(root, expanded)
		if err != nil {
			return "", err
		}
		relativeExpansion, ok := neoFinderRelative(root, resolvedExpansion)
		if !ok {
			return "", fmt.Errorf("finder glob pattern %q is outside workspace root %q", expanded, root)
		}
		scopedPatterns = append(scopedPatterns, neoFinderRestoreGlobEscapes(relativeExpansion, escapes))
	}
	resolved, err := neoFinderScopedPath(root, parsedValue)
	if err != nil {
		return "", err
	}
	patternRelativeToRoot, ok := neoFinderRelative(root, resolved)
	if !ok {
		return "", fmt.Errorf("finder glob pattern %q is outside workspace root %q", parsedValue, root)
	}
	workspacePrefix, ok := neoFinderRelative(executorRoot, root)
	if !ok {
		return "", fmt.Errorf("finder glob pattern %q is outside executor workspace root %q", value, executorRoot)
	}
	if err := neoFinderValidateGlobSymlinks(ctx, root, scopedPatterns); err != nil {
		return "", err
	}
	if workspacePrefix == "." {
		return neoFinderRestoreGlobEscapes(patternRelativeToRoot, escapes), nil
	}
	return neoFinderEscapeGlobLiteralPath(workspacePrefix) + "/" + neoFinderRestoreGlobEscapes(patternRelativeToRoot, escapes), nil
}

type neoFinderGlobEscape struct {
	token string
	value string
}

func neoFinderProtectGlobEscapes(value string) (string, []neoFinderGlobEscape) {
	prefix := "__cliproxy_glob_escape_"
	for strings.Contains(value, prefix) {
		prefix += "_"
	}
	var protected strings.Builder
	escapes := make([]neoFinderGlobEscape, 0)
	for index := 0; index < len(value); index++ {
		if value[index] != '\\' || index+1 >= len(value) || !strings.ContainsRune(`*?[]{}()!+@,`, rune(value[index+1])) {
			protected.WriteByte(value[index])
			continue
		}
		token := fmt.Sprintf("%s%d__", prefix, len(escapes))
		escapes = append(escapes, neoFinderGlobEscape{token: token, value: value[index : index+2]})
		protected.WriteString(token)
		index++
	}
	return protected.String(), escapes
}

func neoFinderRestoreGlobEscapes(value string, escapes []neoFinderGlobEscape) string {
	for _, escape := range escapes {
		value = strings.ReplaceAll(value, escape.token, escape.value)
	}
	return value
}

func neoFinderBraceExpand(pattern string, limit int) ([]string, error) {
	start, end, alternatives, ok, err := neoFinderFirstBraceAlternatives(pattern)
	if err != nil {
		return nil, err
	}
	if !ok {
		return []string{pattern}, nil
	}
	expanded := make([]string, 0, len(alternatives))
	for _, alternative := range alternatives {
		items, err := neoFinderBraceExpand(pattern[:start]+alternative+pattern[end+1:], limit-len(expanded))
		if err != nil {
			return nil, err
		}
		expanded = append(expanded, items...)
		if len(expanded) > limit {
			return nil, fmt.Errorf("glob brace expansion exceeds %d alternatives", limit)
		}
	}
	return expanded, nil
}

func neoFinderFirstBraceAlternatives(pattern string) (int, int, []string, bool, error) {
	for start := 0; start < len(pattern); start++ {
		if pattern[start] != '{' || neoFinderGlobCharacterEscaped(pattern, start) {
			continue
		}
		depth := 0
		end := -1
		for index := start + 1; index < len(pattern); index++ {
			if neoFinderGlobCharacterEscaped(pattern, index) {
				continue
			}
			switch pattern[index] {
			case '{':
				depth++
			case '}':
				if depth == 0 {
					end = index
					index = len(pattern)
				} else {
					depth--
				}
			}
		}
		if end < 0 {
			return 0, 0, nil, false, fmt.Errorf("unclosed glob brace")
		}
		alternatives := make([]string, 0, 2)
		depth = 0
		partStart := start + 1
		for index := partStart; index < end; index++ {
			if neoFinderGlobCharacterEscaped(pattern, index) {
				continue
			}
			switch pattern[index] {
			case '{':
				depth++
			case '}':
				depth--
			case ',':
				if depth == 0 {
					alternatives = append(alternatives, pattern[partStart:index])
					partStart = index + 1
				}
			}
		}
		if len(alternatives) == 0 {
			continue
		}
		alternatives = append(alternatives, pattern[partStart:end])
		return start, end, alternatives, true, nil
	}
	return 0, 0, nil, false, nil
}

func neoFinderGlobCharacterEscaped(value string, index int) bool {
	backslashes := 0
	for index--; index >= 0 && value[index] == '\\'; index-- {
		backslashes++
	}
	return backslashes%2 == 1
}

func neoFinderValidateGlobSymlinks(ctx context.Context, root string, patterns []string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	localRoot, err := neoFinderLocalPath(root)
	if err != nil {
		return nil
	}
	realRoot, err := filepath.EvalSymlinks(localRoot)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("finder glob workspace %q cannot be verified: %w", root, err)
	}
	canonicalRoot, err := neoFinderPathValue(filepath.ToSlash(realRoot))
	if err != nil {
		return err
	}
	scopes := make(map[string]struct{}, len(patterns))
	for _, pattern := range patterns {
		if err := ctx.Err(); err != nil {
			return err
		}
		prefix := neoFinderGlobLiteralPrefix(pattern)
		localScope := localRoot
		if prefix != "" && prefix != "." {
			localScope = filepath.Join(localRoot, filepath.FromSlash(prefix))
		}
		info, err := os.Lstat(localScope)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return fmt.Errorf("finder glob path %q cannot be verified: %w", localScope, err)
		}
		resolvedScope, err := filepath.EvalSymlinks(localScope)
		if err != nil {
			return fmt.Errorf("finder glob path %q cannot be verified: %w", localScope, err)
		}
		canonicalScope, err := neoFinderPathValue(filepath.ToSlash(resolvedScope))
		if err != nil {
			return err
		}
		if !neoFinderPathWithin(canonicalRoot, canonicalScope) {
			return fmt.Errorf("finder glob path %q resolves outside workspace root %q", localScope, root)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			scopes[resolvedScope] = struct{}{}
		} else {
			scopes[localScope] = struct{}{}
		}
	}
	for scope := range scopes {
		if err := neoFinderValidateGlobSymlinkScope(ctx, root, canonicalRoot, scope, neoFinderGlobWalkEntryLimit); err != nil {
			return err
		}
	}
	return nil
}

func neoFinderValidateGlobSymlinkScope(ctx context.Context, root, canonicalRoot, scope string, entryLimit int) error {
	entries := 0
	return neoFinderValidateGlobSymlinkPath(ctx, root, canonicalRoot, scope, entryLimit, &entries, make(map[string]struct{}))
}

func neoFinderValidateGlobSymlinkPath(ctx context.Context, root, canonicalRoot, scope string, entryLimit int, entries *int, visited map[string]struct{}) error {
	resolvedScope, err := filepath.EvalSymlinks(scope)
	if err != nil {
		return fmt.Errorf("finder glob path %q cannot be verified: %w", scope, err)
	}
	canonicalScope, err := neoFinderPathValue(filepath.ToSlash(resolvedScope))
	if err != nil {
		return err
	}
	if !neoFinderPathWithin(canonicalRoot, canonicalScope) {
		return fmt.Errorf("finder glob path %q resolves outside workspace root %q", scope, root)
	}
	resolvedScope = filepath.Clean(resolvedScope)
	info, err := os.Stat(resolvedScope)
	if err != nil {
		return fmt.Errorf("finder glob path %q cannot be verified: %w", scope, err)
	}
	if info.IsDir() {
		if _, seen := visited[resolvedScope]; seen {
			return nil
		}
		visited[resolvedScope] = struct{}{}
	}
	return filepath.WalkDir(resolvedScope, func(candidate string, entry os.DirEntry, walkErr error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if walkErr != nil {
			return fmt.Errorf("finder glob workspace %q cannot be verified: %w", root, walkErr)
		}
		*entries++
		if entryLimit > 0 && *entries > entryLimit {
			return fmt.Errorf("finder glob safety validation exceeded %d entries; narrow the pattern to a directory", entryLimit)
		}
		candidate = filepath.Clean(candidate)
		if candidate != resolvedScope && entry.IsDir() {
			if _, seen := visited[candidate]; seen {
				return filepath.SkipDir
			}
			visited[candidate] = struct{}{}
		}
		if entry.Type()&os.ModeSymlink == 0 {
			return nil
		}
		realTarget, err := filepath.EvalSymlinks(candidate)
		if err != nil {
			return fmt.Errorf("finder glob symlink %q cannot be verified: %w", candidate, err)
		}
		canonicalTarget, err := neoFinderPathValue(filepath.ToSlash(realTarget))
		if err != nil {
			return err
		}
		if !neoFinderPathWithin(canonicalRoot, canonicalTarget) {
			return fmt.Errorf("finder glob symlink %q resolves outside workspace root %q", candidate, root)
		}
		targetInfo, err := os.Stat(realTarget)
		if err != nil {
			return fmt.Errorf("finder glob symlink %q cannot be verified: %w", candidate, err)
		}
		if targetInfo.IsDir() {
			return neoFinderValidateGlobSymlinkPath(ctx, root, canonicalRoot, realTarget, entryLimit, entries, visited)
		}
		return nil
	})
}

func neoFinderGlobLiteralPrefix(pattern string) string {
	parts := strings.Split(pattern, "/")
	literal := make([]string, 0, len(parts))
	for _, part := range parts {
		if part == "" || neoFinderGlobComponentHasMeta(part) {
			break
		}
		literal = append(literal, neoFinderUnescapeGlobLiteral(part))
	}
	return strings.Join(literal, "/")
}

func neoFinderGlobComponentHasMeta(component string) bool {
	for index := 0; index < len(component); index++ {
		if neoFinderGlobCharacterEscaped(component, index) {
			continue
		}
		if strings.ContainsRune("*?[{", rune(component[index])) {
			return true
		}
		if strings.ContainsRune("!+@", rune(component[index])) && index+1 < len(component) && component[index+1] == '(' {
			return true
		}
	}
	return false
}

func neoFinderUnescapeGlobLiteral(value string) string {
	var unescaped strings.Builder
	for index := 0; index < len(value); index++ {
		if value[index] == '\\' && index+1 < len(value) {
			index++
		}
		unescaped.WriteByte(value[index])
	}
	return unescaped.String()
}

func neoFinderEscapeGlobLiteralPath(path string) string {
	var escaped strings.Builder
	for _, char := range path {
		if strings.ContainsRune(`\\*?[]{}()!+@`, char) {
			escaped.WriteByte('\\')
		}
		escaped.WriteRune(char)
	}
	return escaped.String()
}

func neoFinderPathWithin(root, target string) bool {
	root, rootErr := neoFinderPathValue(root)
	target, targetErr := neoFinderPathValue(target)
	if rootErr != nil || targetErr != nil || !neoFinderPathIsAbsolute(root) || !neoFinderPathIsAbsolute(target) {
		return false
	}
	if neoFinderPathEqual(root, target) {
		return true
	}
	if neoFinderWindowsDrivePath(root) || strings.HasPrefix(root, "//") {
		return strings.HasPrefix(strings.ToLower(target), strings.ToLower(strings.TrimSuffix(root, "/")+"/"))
	}
	return strings.HasPrefix(target, strings.TrimSuffix(root, "/")+"/")
}

func neoFinderPathEqual(left, right string) bool {
	if neoFinderWindowsDrivePath(left) || neoFinderWindowsDrivePath(right) || strings.HasPrefix(left, "//") || strings.HasPrefix(right, "//") {
		return strings.EqualFold(left, right)
	}
	return left == right
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
	event := a.storeSubagentToolResultMessageLocked(toolCallID, run, parentToolCallID, completionStatus)
	a.mu.Unlock()

	a.broadcast(event)
	a.syncCloudAsync()
}

func (a *neoActor) storeSubagentToolResultMessageLocked(toolCallID string, run map[string]any, parentToolCallID, completionStatus string) map[string]any {
	_, event := a.storeMessageEventLocked(neoMessage{
		ThreadID:         a.threadID,
		Role:             "user",
		MessageID:        toolResultMessageID(toolCallID),
		Content:          []any{map[string]any{"type": "tool_result", "toolUseID": toolCallID, "run": run}},
		CreatedAt:        time.Now().UTC().Format(time.RFC3339Nano),
		ParentToolUseID:  parentToolCallID,
		CompletionStatus: completionStatus,
	})
	return event
}

type neoSubagentToolExchange struct {
	Call neoToolCall
	Run  map[string]any
}

func (a *neoActor) storeSubagentToolExchanges(exchanges []neoSubagentToolExchange, parentToolCallID string) {
	if len(exchanges) == 0 {
		return
	}
	events := make([]map[string]any, 0, len(exchanges)*2)
	a.mu.Lock()
	for _, exchange := range exchanges {
		_, useEvent := a.storeMessageEventLocked(neoMessage{
			ThreadID:        a.threadID,
			Role:            "assistant",
			MessageID:       newNeoMessageID(),
			Content:         []any{neoToolUseBlock(exchange.Call, true)},
			State:           map[string]any{"type": "complete", "stopReason": "tool_use"},
			CreatedAt:       time.Now().UTC().Format(time.RFC3339Nano),
			ParentToolUseID: parentToolCallID,
		})
		events = append(events, useEvent)
		_, resultEvent := a.storeMessageEventLocked(neoMessage{
			ThreadID:        a.threadID,
			Role:            "user",
			MessageID:       toolResultMessageID(exchange.Call.ID),
			Content:         []any{map[string]any{"type": "tool_result", "toolUseID": exchange.Call.ID, "run": exchange.Run}},
			CreatedAt:       time.Now().UTC().Format(time.RFC3339Nano),
			ParentToolUseID: parentToolCallID,
		})
		events = append(events, resultEvent)
	}
	a.mu.Unlock()

	for _, event := range events {
		a.broadcast(event)
	}
	a.syncCloudAsync()
}

// execSubagentLeafTool leases a subagent's leaf-tool call to the executor and
// blocks until the executor returns a terminal run, routed back via a waiter.
func (a *neoActor) execSubagentLeafTool(call neoToolCall, parentToolCallID, childMessageID string, generation int) map[string]any {
	ch := make(chan map[string]any, 1)
	if !a.registerSubagentLeafTool(call, parentToolCallID, childMessageID, generation, ch) {
		return map[string]any{"status": "cancelled", "reason": "user:cancelled"}
	}
	return a.waitSubagentLeafTool(call, parentToolCallID, ch, generation)
}

func (a *neoActor) waitSubagentLeafTool(call neoToolCall, parentToolCallID string, ch chan map[string]any, generation int) map[string]any {
	timer := time.NewTimer(neoSubagentToolTimeout)
	defer timer.Stop()
	select {
	case run := <-ch:
		return run
	case <-timer.C:
		run := map[string]any{"status": "error", "error": map[string]any{"message": "subagent tool " + call.Name + " timed out"}}
		a.emissionMu.Lock()
		a.mu.Lock()
		owner := generation == a.generation && a.subagentWaiters[call.ID] == ch
		var event map[string]any
		var sockets []*neoSocket
		if owner {
			delete(a.subagentWaiters, call.ID)
			delete(a.subagentTools, call.ID)
			event = a.storeSubagentToolResultMessageLocked(call.ID, run, parentToolCallID, "")
			sockets = a.socketListLocked()
		}
		a.mu.Unlock()
		if owner {
			for _, socket := range sockets {
				if socket == nil || !socket.canSend() {
					continue
				}
				socket.send(event)
				socket.send(map[string]any{"type": "executor_tool_lease_revoked", "toolCallId": call.ID, "reason": "reassigned"})
			}
		}
		a.emissionMu.Unlock()
		if !owner {
			select {
			case run := <-ch:
				return run
			default:
				return map[string]any{"status": "cancelled", "reason": "system:disposed"}
			}
		}
		a.syncCloudAsync()
		return run
	}
}

func (a *neoActor) registerSubagentLeafTool(call neoToolCall, parentToolCallID, childMessageID string, generation int, ch chan map[string]any) bool {
	a.emissionMu.Lock()
	defer a.emissionMu.Unlock()
	a.mu.Lock()
	if generation != a.generation {
		a.mu.Unlock()
		return false
	}
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
	sockets := a.socketListLocked()
	a.mu.Unlock()

	payload := withNeoParentToolCallID(map[string]any{
		"type":       "tool_lease",
		"toolCallId": call.ID,
		"toolName":   call.Name,
		"args":       call.Input,
		"messageId":  childMessageID,
	}, parentToolCallID)
	for _, socket := range sockets {
		if socket == nil || !socket.canSend() {
			continue
		}
		socket.send(payload)
	}
	return true
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
	terminal := neoToolRunTerminalForPending(pending, run)
	if !ok {
		a.mu.Unlock()
		return false
	}
	a.mu.Unlock()
	if !terminal {
		a.emissionMu.Lock()
		a.mu.Lock()
		if a.subagentWaiters[toolCallID] != ch {
			a.mu.Unlock()
			a.emissionMu.Unlock()
			return false
		}
		event := a.storeSubagentToolResultMessageLocked(toolCallID, run, pending.ParentToolCallID, "tool_progress")
		sockets := a.socketListLocked()
		a.mu.Unlock()
		for _, socket := range sockets {
			if socket == nil || !socket.canSend() {
				continue
			}
			socket.send(event)
		}
		a.emissionMu.Unlock()
		a.syncCloudAsync()
		return true
	}
	normalized := normalizeNeoExecutorToolRun(context.Background(), a.runtime, pending, run, a.threadID)
	a.emissionMu.Lock()
	a.mu.Lock()
	owner := a.subagentWaiters[toolCallID] == ch
	var event map[string]any
	var sockets []*neoSocket
	if owner {
		delete(a.subagentWaiters, toolCallID)
		delete(a.subagentTools, toolCallID)
		event = a.storeSubagentToolResultMessageLocked(toolCallID, normalized, pending.ParentToolCallID, "")
		sockets = a.socketListLocked()
	}
	a.mu.Unlock()
	if !owner {
		a.emissionMu.Unlock()
		return false
	}
	ack := map[string]any{"type": "executor_tool_result_ack", "toolCallId": toolCallID}
	for _, socket := range sockets {
		if socket == nil || !socket.canSend() {
			continue
		}
		socket.send(event)
		socket.send(ack)
	}
	select {
	case ch <- normalized:
	default:
	}
	a.emissionMu.Unlock()
	a.syncCloudAsync()
	return true
}

type neoSubagentCancellation struct {
	IDs     []string
	Waiters map[string]chan map[string]any
}

func (a *neoActor) takeSubagentCancellationLocked() neoSubagentCancellation {
	ids := make([]string, 0, len(a.subagentTools)+len(a.subagentWaiters))
	seen := map[string]bool{}
	for id := range a.subagentTools {
		ids = append(ids, id)
		seen[id] = true
	}
	for id := range a.subagentWaiters {
		if !seen[id] {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	waiters := a.subagentWaiters
	a.subagentWaiters = map[string]chan map[string]any{}
	a.subagentTools = map[string]neoPendingTool{}
	return neoSubagentCancellation{IDs: ids, Waiters: waiters}
}

func (a *neoActor) finishSubagentCancellation(cancellation neoSubagentCancellation, runReason, leaseReason string) {
	run := map[string]any{"status": "cancelled", "reason": runReason}
	for _, id := range cancellation.IDs {
		if ch := cancellation.Waiters[id]; ch != nil {
			select {
			case ch <- cloneMap(run):
			default:
			}
		}
		a.broadcast(map[string]any{"type": "executor_tool_lease_revoked", "toolCallId": id, "reason": leaseReason})
	}
}

func neoToolIDsExcluding(ids, excluded []string) []string {
	if len(excluded) == 0 {
		return ids
	}
	blocked := make(map[string]struct{}, len(excluded))
	for _, id := range excluded {
		blocked[id] = struct{}{}
	}
	filtered := make([]string, 0, len(ids))
	for _, id := range ids {
		if _, ok := blocked[id]; !ok {
			filtered = append(filtered, id)
		}
	}
	return filtered
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
