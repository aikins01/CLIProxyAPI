package amp

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"unicode"

	log "github.com/sirupsen/logrus"
)

const (
	neoReadThreadAgentModel        = "gemini-3.5-flash"
	neoReadThreadAgentEffort       = "high"
	neoReadThreadMaxTurns          = 16
	neoReadThreadSearchLimit       = 12
	neoReadThreadSearchLimitMax    = 40
	neoReadThreadReadCount         = 12
	neoReadThreadReadCountMax      = 40
	neoReadThreadSearchExcerpt     = 1400
	neoReadThreadReadMessageChars  = 14000
	neoReadThreadReadTotalChars    = 90000
	neoReadThreadOverviewTailCount = 5
)

const neoReadThreadAgentSystemPrompt = `You are Amp's read_thread subagent. Your job is to search and read a target thread, then extract the information relevant to the caller's goal.

Use the thread tools instead of relying on a whole-thread dump. Search broadly, read exact message ranges, and verify later messages before you answer.

Rules:
- Do not stop at the first relevant hit. Check newer messages that revise, supersede, revert, or contradict it.
- Tool calls record attempted actions, not outcomes. Trust an action only after reading the corresponding tool result and its status.
- Prefer the latest unreverted decision when the thread contains multiple revisions.
- Preserve exact technical details: file paths, commands, model names, errors, decisions, and code snippets.
- Omit unrelated material, but include enough surrounding context for the caller to use the extracted information safely.
- Cite message indexes such as [message 12] when making claims from the target thread.
- Your final answer must be JSON only: {"relevantContent":"markdown text"}.`

const neoReadThreadFinalPrompt = `Return the final answer now as JSON only with one key: relevantContent. Include the relevant thread content as markdown text. Do not call tools.`

type neoReadThreadCorpus struct {
	ThreadID string
	Source   string
	Title    string
	Messages []neoReadThreadMessage
}

type neoReadThreadMessage struct {
	Index           int
	Role            string
	MessageID       string
	CreatedAt       string
	ParentToolUseID string
	Text            string
	ToolUses        []neoReadThreadToolUse
	ToolResults     []neoReadThreadToolResult
	Completion      string
}

type neoReadThreadToolUse struct {
	ID       string         `json:"id,omitempty"`
	Name     string         `json:"name,omitempty"`
	Input    map[string]any `json:"input,omitempty"`
	Complete bool           `json:"complete"`
}

type neoReadThreadToolResult struct {
	ToolUseID        string `json:"toolUseID,omitempty"`
	Status           string `json:"status,omitempty"`
	CompletionStatus string `json:"completionStatus,omitempty"`
	Text             string `json:"text,omitempty"`
}

type neoReadThreadToolObservation struct {
	Searched bool
	Read     bool
	ReadEnd  int
}

func (a *neoActor) readThreadCorpus(threadID, clientAPIKey string) (neoReadThreadCorpus, error) {
	if corpus, ok := a.liveReadThreadCorpus(threadID); ok {
		return corpus, nil
	}
	if corpus, ok := a.localReadThreadFileCorpus(threadID); ok {
		return corpus, nil
	}
	markdown, err := a.fetchUpstreamThreadMarkdown(threadID, clientAPIKey)
	if err != nil {
		return neoReadThreadCorpus{}, err
	}
	corpus := neoReadThreadCorpusFromMarkdown(threadID, markdown, "upstream-markdown")
	if len(corpus.Messages) == 0 {
		return neoReadThreadCorpus{}, errors.New("thread has no readable messages")
	}
	return corpus, nil
}

func (a *neoActor) liveReadThreadCorpus(threadID string) (neoReadThreadCorpus, bool) {
	if a == nil || a.runtime == nil || a.runtime.store == nil || !neoThreadIDExactPattern.MatchString(threadID) {
		return neoReadThreadCorpus{}, false
	}
	target := a.runtime.store.lookupThreadActor(threadID)
	if target == nil || !target.hasLocalThreadState() {
		return neoReadThreadCorpus{}, false
	}
	snapshot, ok := target.threadSnapshot()
	if !ok {
		return neoReadThreadCorpus{}, false
	}
	corpus := neoReadThreadCorpusFromThreadMap(threadID, neoCloudThread(snapshot), "local-live")
	return corpus, len(corpus.Messages) > 0
}

func (a *neoActor) localReadThreadFileCorpus(threadID string) (neoReadThreadCorpus, bool) {
	if !neoThreadIDExactPattern.MatchString(threadID) {
		return neoReadThreadCorpus{}, false
	}
	dir := ""
	if a != nil && a.runtime != nil {
		dir = a.runtime.threadDir
	}
	if strings.TrimSpace(dir) == "" {
		dir = neoAmpThreadStoreDir()
	}
	if strings.TrimSpace(dir) == "" {
		return neoReadThreadCorpus{}, false
	}
	raw, err := os.ReadFile(filepath.Join(dir, threadID+".json"))
	if err != nil {
		return neoReadThreadCorpus{}, false
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		log.Warnf("amp neo read_thread local corpus read failed thread=%s: %v", threadID, err)
		return neoReadThreadCorpus{}, false
	}
	thread := neoCloudThreadDocumentForImport(decoded, threadID)
	if firstNonEmptyString(thread["id"], findThreadID(thread)) != threadID {
		return neoReadThreadCorpus{}, false
	}
	corpus := neoReadThreadCorpusFromThreadMap(threadID, thread, "local-json")
	return corpus, len(corpus.Messages) > 0
}

func neoReadThreadCorpusFromThreadMap(threadID string, thread map[string]any, source string) neoReadThreadCorpus {
	corpus := neoReadThreadCorpus{ThreadID: threadID, Source: source, Title: strings.TrimSpace(stringValue(thread["title"]))}
	for _, raw := range arrayValue(thread["messages"]) {
		if message, ok := neoReadThreadMessageFromMap(mapValue(raw), len(corpus.Messages)); ok {
			corpus.Messages = append(corpus.Messages, message)
		}
	}
	for _, raw := range arrayValue(thread["queuedMessages"]) {
		messageMap := mapValue(raw)
		if len(messageMap) == 0 {
			messageMap = map[string]any{"role": "user", "content": raw}
		}
		if message, ok := neoReadThreadMessageFromMap(messageMap, len(corpus.Messages)); ok {
			if message.Completion == "" {
				message.Completion = "queued"
			}
			corpus.Messages = append(corpus.Messages, message)
		}
	}
	return corpus
}

func neoReadThreadMessageFromMap(message map[string]any, index int) (neoReadThreadMessage, bool) {
	role := strings.TrimSpace(stringValue(message["role"]))
	if role == "" {
		role = "unknown"
	}
	text, toolUses, toolResults := neoReadThreadContentText(message["content"], stringValue(message["completionStatus"]))
	text = strings.TrimSpace(text)
	if text == "" && len(toolUses) == 0 && len(toolResults) == 0 {
		return neoReadThreadMessage{}, false
	}
	return neoReadThreadMessage{
		Index:           index,
		Role:            role,
		MessageID:       firstNonEmptyString(message["messageId"], message["messageID"], message["protocolMessageID"], message["id"]),
		CreatedAt:       firstNonEmptyString(message["createdAt"], message["created_at"], message["timestamp"]),
		ParentToolUseID: firstNonEmptyString(message["parentToolUseId"], message["parentToolUseID"], message["parent_tool_use_id"]),
		Text:            text,
		ToolUses:        toolUses,
		ToolResults:     toolResults,
		Completion:      stringValue(message["completionStatus"]),
	}, true
}

func neoReadThreadContentText(raw any, completionStatus string) (string, []neoReadThreadToolUse, []neoReadThreadToolResult) {
	if text := stringValue(raw); text != "" {
		return text, nil, nil
	}
	blocks := arrayValue(raw)
	if blocks == nil {
		return "", nil, nil
	}
	var out strings.Builder
	toolUses := make([]neoReadThreadToolUse, 0)
	toolResults := make([]neoReadThreadToolResult, 0)
	for _, rawBlock := range blocks {
		block := mapValue(rawBlock)
		switch stringValue(block["type"]) {
		case "text":
			if text := strings.TrimSpace(stringValue(block["text"])); text != "" {
				out.WriteString(text)
				out.WriteString("\n")
			}
		case "tool_use":
			name := strings.TrimSpace(stringValue(block["name"]))
			id := strings.TrimSpace(stringValue(block["id"]))
			input := neoThreadMarkdownToolInput(mapValue(block["input"]))
			complete := neoBinaryToolUseBlockComplete(block)
			toolUses = append(toolUses, neoReadThreadToolUse{ID: id, Name: name, Input: input, Complete: complete})
			out.WriteString("[tool_use attempted_action")
			if name != "" {
				out.WriteString(" name=" + name)
			}
			if id != "" {
				out.WriteString(" id=" + id)
			}
			if !complete {
				out.WriteString(" incomplete=true")
			}
			out.WriteString("]\n")
			if len(input) > 0 {
				out.WriteString(clipNeoDebugJSON(input, 8192))
				out.WriteString("\n")
			}
		case "tool_result":
			toolUseID := firstNonEmptyString(block["toolUseID"], block["toolUseId"], block["tool_use_id"], block["toolCallId"])
			run := mapValue(block["run"])
			status := strings.TrimSpace(stringValue(run["status"]))
			text := strings.TrimSpace(runToText(run))
			toolResults = append(toolResults, neoReadThreadToolResult{ToolUseID: toolUseID, Status: status, CompletionStatus: completionStatus, Text: neoClipRunes(text, 4000)})
			out.WriteString("[tool_result")
			if toolUseID != "" {
				out.WriteString(" toolUseID=" + toolUseID)
			}
			if status != "" {
				out.WriteString(" status=" + status)
			}
			if completionStatus != "" {
				out.WriteString(" completionStatus=" + completionStatus)
			}
			out.WriteString("]\n")
			if text != "" {
				out.WriteString(text)
				out.WriteString("\n")
			}
		case "summary":
			if text := neoCompactionSummaryText(mapValue(block["summary"])); text != "" {
				out.WriteString("[summary]\n")
				out.WriteString(text)
				out.WriteString("\n")
			}
		case "manual_bash_invocation":
			out.WriteString("[manual_bash_invocation]\n")
			out.WriteString(clipNeoDebugJSON(block, 4096))
			out.WriteString("\n")
		default:
			if len(block) > 0 {
				out.WriteString("[" + stringValue(block["type"]) + "]\n")
				out.WriteString(clipNeoDebugJSON(block, 4096))
				out.WriteString("\n")
			}
		}
	}
	return out.String(), toolUses, toolResults
}

func neoReadThreadCorpusFromMarkdown(threadID, markdown, source string) neoReadThreadCorpus {
	corpus := neoReadThreadCorpus{ThreadID: threadID, Source: source}
	lines := strings.Split(markdown, "\n")
	role := "metadata"
	var buf strings.Builder
	flush := func() {
		text := strings.TrimSpace(buf.String())
		buf.Reset()
		if text == "" {
			return
		}
		if role == "metadata" && strings.HasPrefix(text, "---") && len(corpus.Messages) > 0 {
			return
		}
		corpus.Messages = append(corpus.Messages, neoReadThreadMessage{Index: len(corpus.Messages), Role: role, Text: text})
	}
	for _, line := range lines {
		if strings.HasPrefix(line, "## ") {
			heading := strings.ToLower(strings.TrimSpace(strings.TrimPrefix(line, "## ")))
			switch heading {
			case "user", "assistant", "info", "tool", "system":
				flush()
				role = heading
				continue
			}
		}
		if strings.HasPrefix(line, "# ") && corpus.Title == "" {
			corpus.Title = strings.TrimSpace(strings.TrimPrefix(line, "# "))
		}
		buf.WriteString(line)
		buf.WriteByte('\n')
	}
	flush()
	if len(corpus.Messages) == 1 && len([]rune(corpus.Messages[0].Text)) > neoReadThreadReadMessageChars {
		corpus.Messages = neoReadThreadChunkMarkdownMessage(corpus.Messages[0])
	}
	return corpus
}

func neoReadThreadChunkMarkdownMessage(message neoReadThreadMessage) []neoReadThreadMessage {
	text := []rune(message.Text)
	chunkSize := neoReadThreadReadMessageChars
	out := make([]neoReadThreadMessage, 0, (len(text)/chunkSize)+1)
	for start := 0; start < len(text); start += chunkSize {
		end := start + chunkSize
		if end > len(text) {
			end = len(text)
		}
		out = append(out, neoReadThreadMessage{Index: len(out), Role: message.Role, Text: string(text[start:end])})
	}
	return out
}

func (a *neoActor) executeLocalReadThreadAgent(pending neoPendingTool, generation int, corpus neoReadThreadCorpus, goal string) (string, error) {
	if len(corpus.Messages) == 0 {
		return "", errors.New("thread has no readable messages")
	}
	a.mu.Lock()
	settings := cloneMap(a.settings)
	settings["reasoning.effort"] = neoReadThreadAgentEffort
	settings["gemini.thinkingLevel"] = neoReadThreadAgentEffort
	environment := cloneMap(a.environment)
	maxTokens := a.maxTokens
	actorID := a.id
	currentThreadID := a.threadID
	agentMode := firstNonEmptyString(pending.AgentMode, a.currentAgentMode)
	a.mu.Unlock()

	route := neoModelRoute{Provider: "google", Model: neoReadThreadAgentModel}
	conversation := []neoHistoryMessage{{Role: "user", Text: neoReadThreadAgentInput(corpus, goal)}}
	tools := neoReadThreadInternalToolSpecs()
	sawSearch := false
	sawRead := false
	sawLatestRead := len(corpus.Messages) <= 1
	var lastErr error

	for turn := 0; turn < neoReadThreadMaxTurns; turn++ {
		if a.subagentGenerationStale(generation) {
			return "", nil
		}
		routeCopy := route
		result, err := a.runtime.subagentInfer(neoInferenceRequest{
			ActorID:              actorID,
			ThreadID:             currentThreadID,
			MessageID:            newNeoMessageID(),
			AgentMode:            agentMode,
			ReasoningEffort:      neoReadThreadAgentEffort,
			ParentToolCallID:     pending.ID,
			MaxTokens:            maxTokens,
			Settings:             settings,
			History:              append([]neoHistoryMessage(nil), conversation...),
			Tools:                tools,
			Environment:          environment,
			ModelRouteOverride:   &routeCopy,
			SystemPromptOverride: neoReadThreadAgentSystemPrompt,
			ProviderFeature:      "amp.read-thread",
		}, func(neoInferenceDelta) {})
		if err != nil {
			return "", err
		}
		conversation = append(conversation, neoHistoryMessage{Role: "assistant", Text: result.Text, ToolCalls: result.ToolCalls, ThinkingBlocks: result.ThinkingBlocks})
		if len(result.ToolCalls) == 0 {
			if correction := neoReadThreadGateCorrection(sawSearch, sawRead, sawLatestRead, corpus); correction != "" {
				conversation = append(conversation, neoHistoryMessage{Role: "user", Text: correction})
				continue
			}
			text, parseErr := neoReadThreadRelevantContent(result.Text)
			if parseErr == nil {
				return text, nil
			}
			lastErr = parseErr
			forcedText, forcedErr := a.forceLocalReadThreadFinal(pending, generation, route, actorID, currentThreadID, agentMode, maxTokens, settings, environment, conversation)
			if forcedErr == nil {
				return forcedText, nil
			}
			lastErr = forcedErr
			conversation = append(conversation, neoHistoryMessage{Role: "user", Text: "Your previous answer was not valid read_thread JSON. Return JSON only with relevantContent."})
			continue
		}
		for _, call := range result.ToolCalls {
			if call.Incomplete {
				continue
			}
			childMessageID := a.storeSubagentToolUseMessage(call, pending.ID)
			run, observation := neoReadThreadExecuteInternalTool(corpus, call)
			if observation.Searched {
				sawSearch = true
			}
			if observation.Read {
				sawRead = true
				if observation.ReadEnd >= len(corpus.Messages)-1 {
					sawLatestRead = true
				}
			}
			a.storeSubagentToolResultMessage(call.ID, run, pending.ID, "")
			conversation = append(conversation, neoHistoryMessage{
				Role:            "tool",
				ToolCallID:      call.ID,
				ToolName:        call.Name,
				Text:            runToText(run),
				Content:         neoToolRunHistoryContent(run),
				ParentToolUseID: pending.ID,
			})
			_ = childMessageID
		}
	}

	if correction := neoReadThreadGateCorrection(sawSearch, sawRead, sawLatestRead, corpus); correction != "" {
		return "", fmt.Errorf("read_thread subagent did not complete required search/read checks after %d turns: %s", neoReadThreadMaxTurns, correction)
	}
	forcedText, forcedErr := a.forceLocalReadThreadFinal(pending, generation, route, actorID, currentThreadID, agentMode, maxTokens, settings, environment, conversation)
	if forcedErr != nil {
		if lastErr != nil {
			return "", lastErr
		}
		return "", forcedErr
	}
	return forcedText, nil
}

func (a *neoActor) forceLocalReadThreadFinal(pending neoPendingTool, generation int, route neoModelRoute, actorID, currentThreadID, agentMode string, maxTokens any, settings, environment map[string]any, conversation []neoHistoryMessage) (string, error) {
	if a.subagentGenerationStale(generation) {
		return "", nil
	}
	forced := append(append([]neoHistoryMessage(nil), conversation...), neoHistoryMessage{Role: "user", Text: neoReadThreadFinalPrompt})
	routeCopy := route
	result, err := a.runtime.subagentInfer(neoInferenceRequest{
		ActorID:              actorID,
		ThreadID:             currentThreadID,
		MessageID:            newNeoMessageID(),
		AgentMode:            agentMode,
		ReasoningEffort:      neoReadThreadAgentEffort,
		ParentToolCallID:     pending.ID,
		MaxTokens:            maxTokens,
		Settings:             settings,
		History:              forced,
		Environment:          environment,
		ModelRouteOverride:   &routeCopy,
		SystemPromptOverride: neoReadThreadAgentSystemPrompt,
		ProviderFeature:      "amp.read-thread",
		ResponseMimeType:     "application/json",
		ResponseJSONSchema:   neoReadThreadResponseJSONSchema(),
	}, func(neoInferenceDelta) {})
	if err != nil {
		return "", err
	}
	if a.subagentGenerationStale(generation) {
		return "", nil
	}
	return neoReadThreadRelevantContent(result.Text)
}

func neoReadThreadAgentInput(corpus neoReadThreadCorpus, goal string) string {
	var out strings.Builder
	out.WriteString("Read this Amp thread for the caller's goal.\n\n")
	out.WriteString("Thread ID: " + corpus.ThreadID + "\n")
	out.WriteString("Source: " + corpus.Source + "\n")
	if corpus.Title != "" {
		out.WriteString("Title: " + corpus.Title + "\n")
	}
	out.WriteString("Message count: " + strconv.Itoa(len(corpus.Messages)) + "\n")
	out.WriteString("Latest message index: " + strconv.Itoa(len(corpus.Messages)-1) + "\n\n")
	out.WriteString("Goal:\n")
	out.WriteString(goal)
	out.WriteString("\n\nStart with thread_overview or search_thread_messages. After finding relevant hits, read exact messages and later/latest messages before final JSON.")
	return out.String()
}

func neoReadThreadGateCorrection(sawSearch, sawRead, sawLatestRead bool, corpus neoReadThreadCorpus) string {
	if !sawSearch {
		return "Before your final answer, call search_thread_messages for the goal and likely related terms."
	}
	if !sawRead {
		return "Before your final answer, call read_thread_messages for the relevant search hits and their surrounding context."
	}
	if !sawLatestRead && len(corpus.Messages) > 1 {
		return "Before your final answer, call read_thread_messages on later/latest messages through index " + strconv.Itoa(len(corpus.Messages)-1) + " to check for revisions, superseding decisions, reverts, or contradictions."
	}
	return ""
}

func neoReadThreadInternalToolSpecs() []neoToolSpec {
	return []neoToolSpec{
		{
			Name:        "thread_overview",
			Description: "Get target thread metadata, role counts, and excerpts from the beginning and end of the thread. Use this for orientation before searching or when deciding what latest range to read.",
			InputSchema: map[string]any{"type": "object", "properties": map[string]any{}, "required": []any{}},
			Meta:        map[string]any{"source": "read_thread"},
		},
		{
			Name:        "search_thread_messages",
			Description: "Search indexed target-thread messages by keyword. Returns ranked hits with message indexes and excerpts. Use more than one query when the goal has aliases, file paths, errors, or model names.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"query":       map[string]any{"type": "string", "description": "Search query, exact phrase, file path, error text, symbol, model name, or decision term."},
					"limit":       map[string]any{"type": "integer", "description": "Maximum hits to return."},
					"afterIndex":  map[string]any{"type": "integer", "description": "Only search messages after this index."},
					"beforeIndex": map[string]any{"type": "integer", "description": "Only search messages before this index."},
					"roles":       map[string]any{"type": "array", "items": map[string]any{"type": "string"}, "description": "Optional roles to include, such as user, assistant, info, metadata."},
				},
				"required": []any{"query"},
			},
			Meta: map[string]any{"source": "read_thread"},
		},
		{
			Name:        "read_thread_messages",
			Description: "Read exact target-thread messages by ordered index range. Use this after searching and again on later/latest messages to verify revisions or tool outcomes.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"startIndex": map[string]any{"type": "integer", "description": "First message index to read."},
					"endIndex":   map[string]any{"type": "integer", "description": "Last message index to read, inclusive."},
					"count":      map[string]any{"type": "integer", "description": "Number of messages to read from startIndex."},
					"afterIndex": map[string]any{"type": "integer", "description": "Read messages immediately after this index."},
					"latest":     map[string]any{"type": "boolean", "description": "Read the latest tail messages."},
				},
				"required": []any{},
			},
			Meta: map[string]any{"source": "read_thread"},
		},
	}
}

func neoReadThreadExecuteInternalTool(corpus neoReadThreadCorpus, call neoToolCall) (map[string]any, neoReadThreadToolObservation) {
	switch strings.TrimSpace(call.Name) {
	case "thread_overview":
		return map[string]any{"status": "done", "result": neoReadThreadOverview(corpus)}, neoReadThreadToolObservation{}
	case "search_thread_messages":
		result, err := neoReadThreadSearch(corpus, call.Input)
		if err != nil {
			return map[string]any{"status": "error", "error": map[string]any{"message": err.Error()}}, neoReadThreadToolObservation{}
		}
		return map[string]any{"status": "done", "result": result}, neoReadThreadToolObservation{Searched: true}
	case "read_thread_messages":
		result, end, err := neoReadThreadRead(corpus, call.Input)
		if err != nil {
			return map[string]any{"status": "error", "error": map[string]any{"message": err.Error()}}, neoReadThreadToolObservation{}
		}
		return map[string]any{"status": "done", "result": result}, neoReadThreadToolObservation{Read: true, ReadEnd: end}
	default:
		return map[string]any{"status": "error", "error": map[string]any{"message": "unsupported read_thread internal tool: " + call.Name}}, neoReadThreadToolObservation{}
	}
}

func neoReadThreadOverview(corpus neoReadThreadCorpus) map[string]any {
	roleCounts := map[string]any{}
	for _, message := range corpus.Messages {
		roleCounts[message.Role] = numberFrom(roleCounts[message.Role]) + 1
	}
	first := make([]any, 0)
	last := make([]any, 0)
	for i, message := range corpus.Messages {
		if i < neoReadThreadOverviewTailCount {
			first = append(first, message.summaryMap(700))
		}
		if len(corpus.Messages)-i <= neoReadThreadOverviewTailCount {
			last = append(last, message.summaryMap(700))
		}
	}
	return map[string]any{
		"threadID":           corpus.ThreadID,
		"source":             corpus.Source,
		"title":              corpus.Title,
		"messageCount":       len(corpus.Messages),
		"latestMessageIndex": len(corpus.Messages) - 1,
		"roleCounts":         roleCounts,
		"firstMessages":      first,
		"latestMessages":     last,
	}
}

func neoReadThreadSearch(corpus neoReadThreadCorpus, input map[string]any) (map[string]any, error) {
	query := strings.TrimSpace(stringValue(input["query"]))
	if query == "" {
		return nil, errors.New("search_thread_messages requires query")
	}
	limit := numberFrom(input["limit"])
	if limit <= 0 {
		limit = neoReadThreadSearchLimit
	}
	if limit > neoReadThreadSearchLimitMax {
		limit = neoReadThreadSearchLimitMax
	}
	after, hasAfter := neoReadThreadInputInt(input, "afterIndex")
	before, hasBefore := neoReadThreadInputInt(input, "beforeIndex")
	roles := map[string]bool{}
	for _, role := range neoStringSlice(input["roles"]) {
		roles[strings.ToLower(strings.TrimSpace(role))] = true
	}
	terms := neoReadThreadSearchTerms(query)
	type hit struct {
		message neoReadThreadMessage
		score   int
	}
	hits := make([]hit, 0)
	queryLower := strings.ToLower(query)
	for _, message := range corpus.Messages {
		if hasAfter && message.Index <= after {
			continue
		}
		if hasBefore && message.Index >= before {
			continue
		}
		if len(roles) > 0 && !roles[strings.ToLower(message.Role)] {
			continue
		}
		haystack := strings.ToLower(message.searchText())
		score := 0
		if strings.Contains(haystack, queryLower) {
			score += 100
		}
		for _, term := range terms {
			count := strings.Count(haystack, term)
			if count > 5 {
				count = 5
			}
			score += count * 12
		}
		if score == 0 {
			continue
		}
		hits = append(hits, hit{message: message, score: score})
	}
	sort.SliceStable(hits, func(i, j int) bool {
		if hits[i].score != hits[j].score {
			return hits[i].score > hits[j].score
		}
		return hits[i].message.Index < hits[j].message.Index
	})
	if len(hits) > limit {
		hits = hits[:limit]
	}
	items := make([]any, 0, len(hits))
	for _, hit := range hits {
		message := hit.message
		items = append(items, map[string]any{
			"index":             message.Index,
			"role":              message.Role,
			"messageID":         message.MessageID,
			"createdAt":         message.CreatedAt,
			"parentToolUseID":   message.ParentToolUseID,
			"score":             hit.score,
			"excerpt":           neoReadThreadExcerpt(message.Text, query, terms, neoReadThreadSearchExcerpt),
			"hasLaterMessages":  message.Index < len(corpus.Messages)-1,
			"laterMessageCount": len(corpus.Messages) - message.Index - 1,
			"recommendedRead": map[string]any{
				"startIndex": message.Index,
				"count":      neoReadThreadRecommendedReadCount(message.Index, len(corpus.Messages)),
			},
		})
	}
	return map[string]any{
		"threadID":           corpus.ThreadID,
		"source":             corpus.Source,
		"query":              query,
		"messageCount":       len(corpus.Messages),
		"latestMessageIndex": len(corpus.Messages) - 1,
		"hits":               items,
	}, nil
}

func neoReadThreadRead(corpus neoReadThreadCorpus, input map[string]any) (map[string]any, int, error) {
	if len(corpus.Messages) == 0 {
		return nil, -1, errors.New("thread has no readable messages")
	}
	start, end, err := neoReadThreadRange(input, len(corpus.Messages))
	if err != nil {
		return nil, -1, err
	}
	messages := make([]any, 0, end-start+1)
	total := 0
	actualEnd := start - 1
	truncated := false
	for i := start; i <= end; i++ {
		message := corpus.Messages[i]
		entry := message.detailMap(neoReadThreadReadMessageChars)
		raw, _ := json.Marshal(entry)
		if total+len(raw) > neoReadThreadReadTotalChars && len(messages) > 0 {
			truncated = true
			break
		}
		total += len(raw)
		actualEnd = i
		messages = append(messages, entry)
	}
	result := map[string]any{
		"threadID":           corpus.ThreadID,
		"source":             corpus.Source,
		"messageCount":       len(corpus.Messages),
		"latestMessageIndex": len(corpus.Messages) - 1,
		"range": map[string]any{
			"startIndex": start,
			"endIndex":   actualEnd,
		},
		"messages": messages,
	}
	if truncated || actualEnd < end {
		result["truncated"] = true
		if actualEnd+1 < len(corpus.Messages) {
			result["nextStartIndex"] = actualEnd + 1
		}
	}
	return result, actualEnd, nil
}

func neoReadThreadRange(input map[string]any, messageCount int) (int, int, error) {
	count, hasCount := neoReadThreadInputInt(input, "count", "limit")
	if !hasCount || count <= 0 {
		count = neoReadThreadReadCount
	}
	if count > neoReadThreadReadCountMax {
		count = neoReadThreadReadCountMax
	}
	start, hasStart := neoReadThreadInputInt(input, "startIndex", "messageIndex", "index")
	if latest := boolValue(input["latest"]); latest {
		start = messageCount - count
		hasStart = true
	}
	if position := strings.ToLower(strings.TrimSpace(stringValue(input["position"]))); position == "latest" || position == "tail" {
		start = messageCount - count
		hasStart = true
	}
	if after, ok := neoReadThreadInputInt(input, "afterIndex"); ok {
		start = after + 1
		hasStart = true
	}
	if !hasStart {
		start = 0
	}
	if start < 0 {
		start = 0
	}
	if start >= messageCount {
		return 0, 0, fmt.Errorf("startIndex %d is outside thread message range 0-%d", start, messageCount-1)
	}
	end, hasEnd := neoReadThreadInputInt(input, "endIndex")
	if !hasEnd {
		end = start + count - 1
	}
	if end >= messageCount {
		end = messageCount - 1
	}
	if end < start {
		return 0, 0, fmt.Errorf("endIndex %d is before startIndex %d", end, start)
	}
	return start, end, nil
}

func (m neoReadThreadMessage) summaryMap(limit int) map[string]any {
	out := m.baseMap()
	out["excerpt"] = neoClipRunes(m.Text, limit)
	if len(m.ToolUses) > 0 {
		out["toolUses"] = m.ToolUses
	}
	if len(m.ToolResults) > 0 {
		out["toolResults"] = m.ToolResults
	}
	return out
}

func (m neoReadThreadMessage) detailMap(limit int) map[string]any {
	out := m.baseMap()
	out["text"] = neoClipRunes(m.Text, limit)
	if len([]rune(m.Text)) > limit {
		out["truncated"] = true
	}
	if len(m.ToolUses) > 0 {
		out["toolUses"] = m.ToolUses
	}
	if len(m.ToolResults) > 0 {
		out["toolResults"] = m.ToolResults
	}
	return out
}

func (m neoReadThreadMessage) baseMap() map[string]any {
	out := map[string]any{"index": m.Index, "role": m.Role}
	if m.MessageID != "" {
		out["messageID"] = m.MessageID
	}
	if m.CreatedAt != "" {
		out["createdAt"] = m.CreatedAt
	}
	if m.ParentToolUseID != "" {
		out["parentToolUseID"] = m.ParentToolUseID
	}
	if m.Completion != "" {
		out["completionStatus"] = m.Completion
	}
	return out
}

func (m neoReadThreadMessage) searchText() string {
	parts := []string{m.Role, m.MessageID, m.ParentToolUseID, m.Completion, m.Text}
	for _, tool := range m.ToolUses {
		parts = append(parts, tool.ID, tool.Name, clipNeoDebugJSON(tool.Input, 4096))
	}
	for _, result := range m.ToolResults {
		parts = append(parts, result.ToolUseID, result.Status, result.CompletionStatus, result.Text)
	}
	return strings.Join(parts, "\n")
}

func neoReadThreadInputInt(input map[string]any, keys ...string) (int, bool) {
	for _, key := range keys {
		value, exists := input[key]
		if !exists || value == nil {
			continue
		}
		if n, ok := neoClientNumber(value); ok {
			return n, true
		}
		if s := strings.TrimSpace(stringValue(value)); s != "" {
			if n, err := strconv.Atoi(s); err == nil {
				return n, true
			}
		}
	}
	return 0, false
}

func neoReadThreadSearchTerms(query string) []string {
	seen := map[string]bool{}
	terms := make([]string, 0)
	for _, term := range strings.FieldsFunc(strings.ToLower(query), func(r rune) bool {
		return !(unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' || r == '-' || r == '.' || r == '/' || r == ':')
	}) {
		term = strings.Trim(term, "._-/: ")
		if len([]rune(term)) < 2 || seen[term] {
			continue
		}
		seen[term] = true
		terms = append(terms, term)
	}
	return terms
}

func neoReadThreadExcerpt(text, query string, terms []string, limit int) string {
	if strings.TrimSpace(text) == "" {
		return ""
	}
	needles := make([]string, 0, len(terms)+1)
	if needle := strings.TrimSpace(query); needle != "" {
		needles = append(needles, needle)
	}
	needles = append(needles, terms...)
	prefixRunes := neoReadThreadMatchPrefixRunes(text, needles)
	if prefixRunes < 0 {
		return neoClipRunes(text, limit)
	}
	runes := []rune(text)
	start := prefixRunes - limit/3
	if start < 0 {
		start = 0
	}
	end := start + limit
	if end > len(runes) {
		end = len(runes)
		start = end - limit
		if start < 0 {
			start = 0
		}
	}
	excerpt := string(runes[start:end])
	if start > 0 {
		excerpt = "..." + excerpt
	}
	if end < len(runes) {
		excerpt += "..."
	}
	return excerpt
}

func neoReadThreadMatchPrefixRunes(text string, needles []string) int {
	for _, raw := range needles {
		needle := strings.ToLower(strings.TrimSpace(raw))
		if needle == "" {
			continue
		}
		prefixRunes := 0
		for byteIndex := range text {
			if strings.HasPrefix(strings.ToLower(text[byteIndex:]), needle) {
				return prefixRunes
			}
			prefixRunes++
		}
	}
	return -1
}

func neoReadThreadRecommendedReadCount(index, messageCount int) int {
	remaining := messageCount - index
	if remaining < 1 {
		return 1
	}
	if remaining < neoReadThreadReadCount {
		return remaining
	}
	return neoReadThreadReadCount
}

func neoClipRunes(text string, limit int) string {
	if limit <= 0 {
		return ""
	}
	runes := []rune(text)
	if len(runes) <= limit {
		return text
	}
	return string(runes[:limit]) + "\n[truncated " + strconv.Itoa(len(runes)-limit) + " chars]"
}
