package amp

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
)

const neoReadThreadPromptTemplate = `You are helping me extract relevant information from the mentioned thread based on a goal.

## Task

I am talking to another user. They mentioned a thread (a conversation) in their message last message. I turned the thread into Markdown and provided it to you, along with a goal of what I want you to extract.

Your job is to:
1. Analyze the mentioned thread's content
2. Identify information that is relevant to the goal
3. Extract and preserve those relevant parts with full fidelity
4. Omit clearly irrelevant content to keep the context concise

## Guidelines

**Preserve Fidelity**: When content IS relevant, include it completely with all important details, code snippets, explanations, and context.
**Be Selective**: When content is clearly NOT relevant to the user's query, omit it entirely.
**Maintain Structure**: Keep the extracted content well-organized and coherent. If multiple parts are relevant, preserve their logical flow.
**Technical Precision**: Preserve exact technical details like file paths, function names, error messages, and code snippets that are relevant.

## Examples

### Example 1: Extract implementation details

**Goal**: "Extract the implementation details of the authentication mechanism in the mentioned thread"

**Good Extraction**:
- Includes: Authentication logic, security considerations, code examples, relevant files
- Omits: Unrelated features, general discussion, tangential topics

### Example 2: Referencing a bug fix

**Goal**: "Extract how the bug was fixed in the mentioned thread"

**Good Extraction**:
- Includes: The bug description, root cause, the fix/solution, relevant code changes
- Omits: Initial troubleshooting steps, unrelated changes, meeting notes

### Example 3: Learning from past work

**Goal**: "Describe what pattern was used to implemented the widget Foo in the mentioned thread"

**Good Extraction**:
- Includes: The design pattern, implementation approach, example code, key decisions
- Omits: Project-specific details that don't apply, alternative approaches that were rejected

## Goal

{GOAL}

## Your Response

Format your response as JSON with:
- ` + "`relevantContent`" + `: The extracted relevant information (as markdown text)`

func (a *neoActor) shouldRunLocalActorTool(name string) bool {
	toolName := strings.TrimSpace(name)
	switch {
	case toolName == "read_thread" || toolName == "submit_review":
	case isNeoGitHubTool(toolName):
	case isNeoThreadTool(toolName):
	default:
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	_, registered := a.tools[toolName]
	return a.executorBootstrapComplete && !registered
}

func (a *neoActor) runLocalActorTool(pending neoPendingTool, generation int) {
	toolName := strings.TrimSpace(pending.Name)
	if isNeoGitHubTool(toolName) {
		a.runLocalGitHubActorTool(pending, generation)
		return
	}
	if isNeoThreadTool(toolName) {
		a.runLocalThreadActorTool(pending, generation)
		return
	}
	switch toolName {
	case "read_thread", "submit_review":
	default:
		return
	}
	switch toolName {
	case "read_thread":
		text, err := a.executeLocalReadThread(pending, generation)
		if a.subagentGenerationStale(generation) {
			return
		}
		var run map[string]any
		if err != nil {
			run = map[string]any{"status": "error", "error": map[string]any{"message": err.Error()}}
		} else {
			run = map[string]any{"status": "done", "result": strings.TrimSpace(text)}
		}
		a.receiveToolResult(map[string]any{"type": "executor_tool_result", "toolCallId": pending.ID, "run": run})
	case "submit_review":
		result, err := executeLocalSubmitReview(pending.Input)
		if a.subagentGenerationStale(generation) {
			return
		}
		var run map[string]any
		if err != nil {
			run = map[string]any{"status": "error", "error": map[string]any{"message": err.Error()}}
		} else {
			run = map[string]any{"status": "done", "result": result}
		}
		a.receiveToolResult(map[string]any{"type": "executor_tool_result", "toolCallId": pending.ID, "run": run})
	}
}

func (a *neoActor) executeLocalReadThread(pending neoPendingTool, generation int) (string, error) {
	input := pending.Input
	rawThreadID := firstNonEmptyString(input["threadID"], input["threadId"], input["thread_id"], input["thread"], input["url"])
	threadID := neoToolInputThreadID(input)
	if threadID == "" {
		return "", fmt.Errorf("Reading thread failed: Invalid thread ID or thread URL: %s", rawThreadID)
	}
	goal := strings.TrimSpace(firstNonEmptyString(input["question"], input["goal"]))
	if goal == "" {
		return "", fmt.Errorf("Reading thread failed: missing required question")
	}
	if a.runtime == nil {
		return "", fmt.Errorf("Reading thread failed: missing local runtime")
	}
	if a.subagentGenerationStale(generation) {
		return "", nil
	}
	markdown, err := a.readThreadMarkdown(threadID, pending.ClientAPIKey)
	if err != nil {
		return "", fmt.Errorf("Reading thread failed: %w", err)
	}
	if a.subagentGenerationStale(generation) {
		return "", nil
	}

	a.mu.Lock()
	settings := cloneMap(a.settings)
	environment := cloneMap(a.environment)
	maxTokens := a.maxTokens
	actorID := a.id
	currentThreadID := a.threadID
	a.mu.Unlock()

	route := neoModelRoute{Provider: "google", Model: "gemini-3-flash-preview"}
	result, err := a.runtime.subagentInfer(neoInferenceRequest{
		ActorID:                  actorID,
		ThreadID:                 currentThreadID,
		MessageID:                newNeoMessageID(),
		AgentMode:                pending.AgentMode,
		ParentToolCallID:         pending.ParentToolCallID,
		MaxTokens:                maxTokens,
		Settings:                 settings,
		History:                  neoReadThreadHistory(markdown, goal),
		Environment:              environment,
		ModelRouteOverride:       &route,
		DisableSystemPrompt:      true,
		DisableProviderReasoning: true,
		ProviderFeature:          "amp.read-thread",
		ResponseMimeType:         "application/json",
		ResponseJSONSchema:       neoReadThreadResponseJSONSchema(),
	}, func(neoInferenceDelta) {})
	if err != nil {
		return "", fmt.Errorf("Reading thread failed: %w", err)
	}
	if a.subagentGenerationStale(generation) {
		return "", nil
	}
	text, err := neoReadThreadRelevantContent(result.Text)
	if err != nil {
		return "", fmt.Errorf("Reading thread failed: %w", err)
	}
	return text, nil
}

func (a *neoActor) readThreadMarkdown(threadID, clientAPIKey string) (string, error) {
	return a.fetchUpstreamThreadMarkdown(threadID, clientAPIKey)
}

func (a *neoActor) fetchUpstreamThreadMarkdown(threadID, clientAPIKey string) (string, error) {
	if a == nil || a.runtime == nil {
		return "", fmt.Errorf("Thread %s not found locally and cannot fetch from server: API key not configured", threadID)
	}
	cfg := a.runtime.configSnapshot()
	if cfg == nil || strings.TrimSpace(cfg.AmpCode.UpstreamURL) == "" {
		return "", fmt.Errorf("Thread %s not found locally and cannot fetch from server: API key not configured", threadID)
	}
	apiKey, err := a.upstreamThreadFetchAPIKey(cfg, clientAPIKey)
	if err != nil {
		return "", fmt.Errorf("Thread %s not found locally and cannot fetch from server: %w", threadID, err)
	}
	if strings.TrimSpace(apiKey) == "" {
		return "", fmt.Errorf("Thread %s not found locally and cannot fetch from server: API key not configured", threadID)
	}
	base, err := url.Parse(strings.TrimSpace(cfg.AmpCode.UpstreamURL))
	if err != nil {
		return "", err
	}
	base.Path = strings.TrimRight(base.Path, "/") + "/api/threads/" + url.PathEscape(threadID) + ".md"
	base.RawQuery = "truncate_tool_results=1"

	req, err := http.NewRequest(http.MethodGet, base.String(), nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(apiKey))
	setAmpInternalClientHeaders(req, ampUpstreamClientVersion(&cfg.AmpCode))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("Thread %s not found (server returned %d)", threadID, resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

func (a *neoActor) upstreamThreadFetchAPIKey(cfg *config.Config, clientAPIKey string) (string, error) {
	if a != nil && a.runtime != nil {
		if source := a.runtime.getSecretSource(); source != nil {
			ctx := context.Background()
			if clientAPIKey = strings.TrimSpace(clientAPIKey); clientAPIKey != "" {
				ctx = context.WithValue(ctx, clientAPIKeyContextKey{}, clientAPIKey)
			}
			key, err := source.Get(ctx)
			if err != nil {
				return "", err
			}
			if strings.TrimSpace(key) != "" {
				return strings.TrimSpace(key), nil
			}
		}
	}
	if cfg == nil {
		return "", nil
	}
	return strings.TrimSpace(cfg.AmpCode.UpstreamAPIKey), nil
}

func neoReadThreadHistory(markdown, goal string) []neoHistoryMessage {
	return []neoHistoryMessage{
		{Role: "user", Text: strings.Join([]string{
			"Here is the mentioned thread content:",
			"",
			"<mentionedThread>",
			markdown,
			"</mentionedThread>",
		}, "\n")},
		{Role: "user", Text: strings.ReplaceAll(neoReadThreadPromptTemplate, "{GOAL}", goal)},
	}
}

func neoReadThreadRelevantContent(text string) (string, error) {
	raw := strings.TrimSpace(text)
	var parsed struct {
		RelevantContent string `json:"relevantContent"`
	}
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		if extracted, ok := neoReadThreadJSONEnvelope(raw); ok {
			if errEnvelope := json.Unmarshal([]byte(extracted), &parsed); errEnvelope == nil {
				raw = extracted
				err = nil
			}
		}
		if err != nil {
			return "", fmt.Errorf("Failed to parse JSON from thread extraction result: %w", err)
		}
	}
	if strings.TrimSpace(parsed.RelevantContent) == "" {
		return "", fmt.Errorf("thread extraction result missing relevantContent")
	}
	return strings.TrimSpace(parsed.RelevantContent), nil
}

func neoReadThreadResponseJSONSchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"relevantContent": map[string]any{
				"type":        "string",
				"description": "Extracted relevant information from the thread based on the goal. Preserve fidelity and details for relevant parts. Omit irrelevant content.",
			},
		},
		"required": []any{"relevantContent"},
	}
}

func neoReadThreadJSONEnvelope(text string) (string, bool) {
	if start := strings.Index(text, "```"); start >= 0 {
		afterFence := text[start+3:]
		if newline := strings.IndexByte(afterFence, '\n'); newline >= 0 {
			afterFence = afterFence[newline+1:]
		}
		if end := strings.Index(afterFence, "```"); end >= 0 {
			candidate := strings.TrimSpace(afterFence[:end])
			if strings.HasPrefix(candidate, "{") && strings.HasSuffix(candidate, "}") {
				return candidate, true
			}
		}
	}
	start := strings.IndexByte(text, '{')
	end := strings.LastIndexByte(text, '}')
	if start >= 0 && end > start {
		return strings.TrimSpace(text[start : end+1]), true
	}
	return "", false
}
