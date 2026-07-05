package amp

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"unicode"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
)

func (a *neoActor) shouldRunLocalActorTool(name string) bool {
	toolName := strings.TrimSpace(name)
	if toolName == "read_thread" {
		a.mu.Lock()
		defer a.mu.Unlock()
		return a.executorBootstrapComplete
	}
	switch {
	case toolName == "submit_review":
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
	corpus, err := a.readThreadCorpus(threadID, pending.ClientAPIKey)
	if err != nil {
		return "", fmt.Errorf("Reading thread failed: %w", err)
	}
	if a.subagentGenerationStale(generation) {
		return "", nil
	}
	return a.executeLocalReadThreadAgent(pending, generation, corpus, goal)
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

func neoReadThreadMarkdownFallbackContent(text string) string {
	raw := strings.TrimSpace(text)
	if raw == "" {
		return ""
	}
	if fenced := neoReadThreadMarkdownFenceContent(raw); fenced != "" {
		return fenced
	}
	if strings.HasPrefix(raw, "```") || neoReadThreadFallbackLooksLikeJSON(raw) {
		return ""
	}
	return raw
}

func neoReadThreadGroundedMarkdownFallbackContent(text string) string {
	fallback := neoReadThreadMarkdownFallbackContent(text)
	if fallback == "" || !neoReadThreadFallbackHasMessageCitation(fallback) {
		return ""
	}
	return fallback
}

func neoReadThreadFallbackHasMessageCitation(text string) bool {
	lower := strings.ToLower(text)
	for {
		index := strings.Index(lower, "[message")
		if index < 0 {
			return false
		}
		rest := lower[index+len("[message"):]
		if rest == "" || !unicode.IsSpace(rune(rest[0])) {
			lower = lower[index+1:]
			continue
		}
		rest = strings.TrimLeftFunc(rest, unicode.IsSpace)
		digitCount := 0
		for _, r := range rest {
			if !unicode.IsDigit(r) {
				break
			}
			digitCount++
		}
		if digitCount == 0 {
			lower = lower[index+1:]
			continue
		}
		rest = rest[digitCount:]
		rest = strings.TrimLeftFunc(rest, unicode.IsSpace)
		if strings.HasPrefix(rest, "]") {
			return true
		}
		lower = lower[index+1:]
	}
}

func neoReadThreadMarkdownFenceContent(text string) string {
	start := strings.Index(text, "```")
	if start < 0 {
		return ""
	}
	afterFence := text[start+3:]
	newline := strings.IndexByte(afterFence, '\n')
	if newline < 0 {
		return ""
	}
	lang := strings.ToLower(strings.TrimSpace(afterFence[:newline]))
	if fields := strings.Fields(lang); len(fields) > 0 {
		lang = fields[0]
	}
	if lang != "" && lang != "markdown" && lang != "md" && lang != "text" && lang != "txt" {
		return ""
	}
	body := afterFence[newline+1:]
	end := strings.Index(body, "```")
	if end < 0 {
		return ""
	}
	inner := strings.TrimSpace(body[:end])
	if inner == "" || neoReadThreadFallbackLooksLikeJSON(inner) {
		return ""
	}
	return inner
}

func neoReadThreadFallbackLooksLikeJSON(text string) bool {
	raw := strings.TrimSpace(text)
	if strings.HasPrefix(raw, "{") {
		return true
	}
	if !strings.HasPrefix(raw, "[") {
		return false
	}
	afterBracket := strings.TrimSpace(raw[1:])
	if afterBracket == "" {
		return true
	}
	if json.Valid([]byte(raw)) {
		return true
	}
	first := afterBracket[0]
	if first == '{' || first == ']' {
		return true
	}
	return false
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
