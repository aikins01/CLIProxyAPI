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
	"unicode/utf8"

	log "github.com/sirupsen/logrus"
)

const (
	neoReadThreadAgentProvider      = "openai"
	neoReadThreadAgentModel         = "gpt-5.6-sol"
	neoReadThreadAgentEffort        = "medium"
	neoReadThreadMaxTurns           = 16
	neoReadThreadSearchLimit        = 12
	neoReadThreadSearchLimitMax     = 40
	neoReadThreadReadCount          = 12
	neoReadThreadReadCountMax       = 40
	neoReadThreadLatestReadCount    = 24
	neoReadThreadOverviewExcerpt    = 260
	neoReadThreadOverviewResultText = 160
	neoReadThreadSearchExcerpt      = 1400
	neoReadThreadReadMessageChars   = 14000
	neoReadThreadReadTotalChars     = 90000
	neoReadThreadOverviewTailCount  = neoReadThreadLatestReadCount
	neoReadThreadHistoryTextChars   = 24000
	neoReadThreadHistoryTotalChars  = 100000
	neoReadThreadHistoryNoticeChars = 512
	neoReadThreadMaxToolCalls       = 24
	neoReadThreadMaxSearchCalls     = 12
	neoReadThreadMaxReadCalls       = 12
	neoReadThreadMaxCallsPerTurn    = 4
	neoReadThreadForceFinalCalls    = neoReadThreadMaxToolCalls - neoReadThreadMaxCallsPerTurn
	neoReadThreadStoredHitCount     = 12
	neoReadThreadStoredMessageCount = 12
	neoReadThreadStoredExcerptChars = 600
	neoReadThreadStoredMessageChars = 1000
	neoReadThreadStoredResultChars  = 600
	neoReadThreadStoredToolBlocks   = 4
	neoReadThreadEvidenceEntries    = 8
	neoReadThreadEvidenceEntryChars = 7000
)

const neoReadThreadAgentSystemPrompt = `You are Amp's read_thread subagent. Your job is to search and read a target thread, then extract the information relevant to the caller's goal.

Use the thread tools instead of relying on a whole-thread dump. Search broadly, read exact message ranges, and verify later messages before you answer.

Rules:
- Do not stop at the first relevant hit. Check newer messages that revise, supersede, revert, or contradict it.
- Tool calls record attempted actions, not outcomes. Trust an action only after reading the corresponding tool result and its status.
- Use compactions and summaries for orientation, but inspect original messages when exact requirements, wording, code, commands, chronology, edits, or verification matter.
- Prefer the latest unreverted decision when the thread contains multiple revisions.
- Do not repeat a search with only a larger limit. Refine the query when more evidence is needed.
- Use each search hit's recommendedRead range, and do not reread a range already covered by an earlier result.
- For continuation or handoff goals, identify the latest explicit user objective first, then read the assistant and tool outcomes that followed it.
- Preserve exact technical details: file paths, commands, model names, errors, decisions, and code snippets.
- When reporting verification or build status, include the complete command string that ran and the latest pass/fail result.
- Copy relevant file paths, symbols, type names, env vars, commands, model names, and error strings verbatim. Do not replace exact identifiers with categories.
- Do not mention superseded or irrelevant topics just to say they are not relevant. Omit them unless the caller specifically asks for that contrast.
- Omit unrelated material, but include enough surrounding context for the caller to use the extracted information safely.
- Cite message indexes such as [message 12] when making claims from the target thread.
- Your final answer must be JSON only with exactly one key named relevantContent. Put the actual extracted thread details in that value as markdown prose or bullets, not nested JSON; do not return placeholder or template text.`

const neoReadThreadFinalPrompt = `Return the final answer now as JSON only with one key named relevantContent. Include the actual relevant thread content as markdown prose or bullets, not nested JSON. Copy relevant identifiers verbatim. Do not call tools and do not return placeholder or template text.`

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
	searchTextLower string
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

type neoReadThreadPositionRange struct {
	Start int
	End   int
}

type neoReadThreadAgentState struct {
	SawOverview bool
	SawSearch   bool
	SawRead     bool
	SawLatest   bool
	ToolCalls   int
	SearchCalls int
	ReadCalls   int
	SeenCalls   map[string]struct{}
	ReadRanges  []neoReadThreadPositionRange
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
	target := a
	if a.threadID != threadID && a.key != threadID {
		target = a.runtime.store.lookupThreadActor(threadID)
	}
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
	rawMessages := arrayValue(thread["messages"])
	for sourceIndex, raw := range rawMessages {
		messageMap := mapValue(raw)
		if neoReadThreadMessageIsNested(messageMap) {
			continue
		}
		if message, ok := neoReadThreadMessageFromMap(messageMap, sourceIndex); ok {
			corpus.Messages = append(corpus.Messages, message)
		}
	}
	for queuedIndex, raw := range arrayValue(thread["queuedMessages"]) {
		messageMap := mapValue(raw)
		if len(messageMap) == 0 {
			messageMap = map[string]any{"role": "user", "content": raw}
		}
		if neoReadThreadMessageIsNested(messageMap) {
			continue
		}
		if message, ok := neoReadThreadMessageFromMap(messageMap, len(rawMessages)+queuedIndex); ok {
			if message.Completion == "" {
				message.Completion = "queued"
			}
			corpus.Messages = append(corpus.Messages, message)
		}
	}
	return corpus
}

func neoReadThreadMessageIsNested(message map[string]any) bool {
	return firstNonEmptyString(message["parentToolUseId"], message["parentToolUseID"], message["parent_tool_use_id"], message["parentToolCallId"]) != ""
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
	return a.executeLocalReadThreadAgentWithRoute(pending, generation, corpus, goal, neoModelRoute{Provider: neoReadThreadAgentProvider, Model: neoReadThreadAgentModel}, neoReadThreadAgentEffort)
}

func (a *neoActor) executeLocalReadThreadAgentWithRoute(pending neoPendingTool, generation int, corpus neoReadThreadCorpus, goal string, route neoModelRoute, effort string) (string, error) {
	if len(corpus.Messages) == 0 {
		return "", errors.New("thread has no readable messages")
	}
	if route.Model == "" {
		route = neoModelRoute{Provider: neoReadThreadAgentProvider, Model: neoReadThreadAgentModel}
	}
	if route.Provider == "" {
		route.Provider = providerForNeoModel(route.Model)
	}
	if effort == "" {
		effort = neoReadThreadAgentEffort
	}
	a.mu.Lock()
	settings := cloneMap(a.settings)
	settings["reasoning.effort"] = effort
	delete(settings, "gemini.thinkingLevel")
	environment := cloneMap(a.environment)
	maxTokens := a.maxTokens
	actorID := a.id
	currentThreadID := a.threadID
	agentMode := firstNonEmptyString(pending.AgentMode, a.currentAgentMode)
	a.mu.Unlock()

	conversation := []neoHistoryMessage{{Role: "user", Text: neoReadThreadAgentInput(corpus, goal)}}
	state := neoReadThreadAgentState{
		SawLatest: len(corpus.Messages) <= 1,
		SeenCalls: map[string]struct{}{},
	}
	evidence := make([]string, 0)
	var lastErr error

	for turn := 0; turn < neoReadThreadMaxTurns; turn++ {
		if a.subagentGenerationStale(generation) {
			return "", nil
		}
		routeCopy := route
		result, err := a.runtime.subagentInfer(neoInferenceRequest{
			ActorID:                  actorID,
			ThreadID:                 currentThreadID,
			MessageID:                newNeoMessageID(),
			AgentMode:                agentMode,
			ReasoningEffort:          effort,
			ParentToolCallID:         pending.ID,
			MaxTokens:                maxTokens,
			Settings:                 settings,
			History:                  neoReadThreadRequestHistory(conversation),
			Tools:                    neoReadThreadInternalToolSpecsForAgentState(state),
			Environment:              environment,
			ModelRouteOverride:       &routeCopy,
			SystemPromptOverride:     neoReadThreadAgentSystemPrompt,
			ProviderFeature:          "amp.read-thread",
			DisableParallelToolCalls: true,
		}, func(neoInferenceDelta) {})
		if err != nil {
			return "", err
		}
		conversation = append(conversation, neoHistoryMessage{Role: "assistant", Text: result.Text, ToolCalls: result.ToolCalls, ThinkingBlocks: result.ThinkingBlocks})
		if len(result.ToolCalls) == 0 {
			if correction := neoReadThreadGateCorrection(state.SawSearch, state.SawRead, state.SawLatest, corpus); correction != "" {
				conversation = append(conversation, neoHistoryMessage{Role: "user", Text: correction})
				continue
			}
			text, parseErr := neoReadThreadGroundedRelevantContent(result.Text, corpus)
			if parseErr == nil {
				return text, nil
			}
			lastErr = parseErr
			forcedText, forcedErr := a.forceLocalReadThreadFinalWithEvidence(pending, generation, route, effort, actorID, currentThreadID, agentMode, maxTokens, settings, environment, conversation, corpus, evidence)
			if forcedErr == nil {
				return forcedText, nil
			}
			lastErr = forcedErr
			conversation = append(conversation, neoHistoryMessage{Role: "user", Text: "Your previous answer was not valid read_thread JSON. Return JSON only with relevantContent."})
			continue
		}
		exchanges := make([]neoSubagentToolExchange, 0, len(result.ToolCalls))
		acceptedThisTurn := 0
		for _, call := range result.ToolCalls {
			if call.Incomplete {
				continue
			}
			run, accepted := neoReadThreadExecuteAgentTool(&state, corpus, call, acceptedThisTurn)
			if accepted {
				acceptedThisTurn++
				exchanges = append(exchanges, neoSubagentToolExchange{Call: call, Run: neoReadThreadPersistedRun(call.Name, run)})
				if entry := neoReadThreadEvidenceEntry(call.Name, run); entry != "" {
					evidence = append(evidence, entry)
				}
			}
			conversation = append(conversation, neoHistoryMessage{
				Role:            "tool",
				ToolCallID:      call.ID,
				ToolName:        call.Name,
				Text:            runToText(run),
				Content:         neoToolRunHistoryContent(run),
				ParentToolUseID: pending.ID,
			})
		}
		a.storeSubagentToolExchanges(exchanges, pending.ID)
		if neoReadThreadReadyToForceFinal(state) {
			return a.forceLocalReadThreadFinalWithEvidence(pending, generation, route, effort, actorID, currentThreadID, agentMode, maxTokens, settings, environment, conversation, corpus, evidence)
		}
	}

	if a.subagentGenerationStale(generation) {
		return "", nil
	}
	if state.SawSearch && state.SawRead && !state.SawLatest && len(corpus.Messages) > 1 {
		latest, readEnd, err := neoReadThreadRead(corpus, map[string]any{"latest": true, "count": neoReadThreadLatestReadCount})
		if err != nil {
			return "", fmt.Errorf("read_thread auto latest read failed: %w", err)
		}
		if readEnd >= neoReadThreadLatestMessageIndex(corpus) {
			state.SawLatest = true
			conversation = append(conversation, neoHistoryMessage{
				Role: "user",
				Text: "The runtime performed the required latest read before finalization because the turn budget was exhausted. Use this latest-read result to check for revisions, superseding decisions, reverts, or contradictions before returning final JSON.\n\n" + runToText(map[string]any{"status": "done", "result": latest}),
			})
		}
	}
	if correction := neoReadThreadGateCorrection(state.SawSearch, state.SawRead, state.SawLatest, corpus); correction != "" {
		return "", fmt.Errorf("read_thread subagent did not complete required search/read checks after %d turns: %s", neoReadThreadMaxTurns, correction)
	}
	forcedText, forcedErr := a.forceLocalReadThreadFinalWithEvidence(pending, generation, route, effort, actorID, currentThreadID, agentMode, maxTokens, settings, environment, conversation, corpus, evidence)
	if forcedErr != nil {
		if lastErr != nil {
			return "", lastErr
		}
		return "", forcedErr
	}
	return forcedText, nil
}

func (a *neoActor) forceLocalReadThreadFinal(pending neoPendingTool, generation int, route neoModelRoute, effort, actorID, currentThreadID, agentMode string, maxTokens any, settings, environment map[string]any, conversation []neoHistoryMessage, corpus neoReadThreadCorpus) (string, error) {
	return a.forceLocalReadThreadFinalWithEvidence(pending, generation, route, effort, actorID, currentThreadID, agentMode, maxTokens, settings, environment, conversation, corpus, nil)
}

func (a *neoActor) forceLocalReadThreadFinalWithEvidence(pending neoPendingTool, generation int, route neoModelRoute, effort, actorID, currentThreadID, agentMode string, maxTokens any, settings, environment map[string]any, conversation []neoHistoryMessage, corpus neoReadThreadCorpus, evidence []string) (string, error) {
	if a.subagentGenerationStale(generation) {
		return "", nil
	}
	forced := append([]neoHistoryMessage(nil), conversation...)
	if evidenceContext := neoReadThreadFinalEvidence(evidence); evidenceContext != "" {
		forced = append(forced, neoHistoryMessage{Role: "user", Text: evidenceContext})
	}
	if latestContext := neoReadThreadLatestFinalContext(corpus); latestContext != "" {
		forced = append(forced, neoHistoryMessage{Role: "user", Text: latestContext})
	}
	if len(forced) > 0 && forced[len(forced)-1].Role == "user" {
		forced[len(forced)-1].Text = strings.TrimSpace(forced[len(forced)-1].Text) + "\n\n" + neoReadThreadFinalPrompt
	} else {
		forced = append(forced, neoHistoryMessage{Role: "user", Text: neoReadThreadFinalPrompt})
	}
	forced = neoReadThreadFinalHistory(neoReadThreadRequestHistory(forced))
	routeCopy := route
	result, err := a.runtime.subagentInfer(neoInferenceRequest{
		ActorID:              actorID,
		ThreadID:             currentThreadID,
		MessageID:            newNeoMessageID(),
		AgentMode:            agentMode,
		ReasoningEffort:      effort,
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
	text, parseErr := neoReadThreadGroundedRelevantContent(result.Text, corpus)
	if parseErr == nil {
		return text, nil
	}
	if fallback := neoReadThreadGroundedMarkdownFallbackContent(result.Text, corpus); fallback != "" {
		return fallback, nil
	}
	return "", fmt.Errorf("read_thread forced final did not return valid JSON or markdown fallback: %w", parseErr)
}

func neoReadThreadLatestFinalContext(corpus neoReadThreadCorpus) string {
	if len(corpus.Messages) == 0 {
		return ""
	}
	count := neoReadThreadLatestReadCount
	if count > len(corpus.Messages) {
		count = len(corpus.Messages)
	}
	latest, _, err := neoReadThreadRead(corpus, map[string]any{"latest": true, "count": count})
	if err != nil {
		return ""
	}
	return "Authoritative latest visible target-thread messages before finalization. Prefer this tail over older summaries or earlier search hits when they conflict.\n\n" + runToText(map[string]any{"status": "done", "result": latest})
}

func neoReadThreadFinalEvidence(entries []string) string {
	positions := neoReadThreadStoredPositions(len(entries), neoReadThreadEvidenceEntries)
	if len(positions) == 0 {
		return ""
	}
	selected := make([]string, 0, len(positions))
	for _, position := range positions {
		selected = append(selected, entries[position])
	}
	return "Grounded evidence retained from completed read_thread tools. Use these observations and the authoritative latest tail to synthesize the answer; do not resume searching.\n\n" + strings.Join(selected, "\n\n")
}

func neoReadThreadEvidenceEntry(toolName string, run map[string]any) string {
	if stringValue(run["status"]) != "done" || (toolName != "search_thread_messages" && toolName != "read_thread_messages") {
		return ""
	}
	compacted := neoReadThreadPersistedRun(toolName, run)
	return toolName + " evidence:\n" + neoClipRunes(runToText(compacted), neoReadThreadEvidenceEntryChars)
}

func neoReadThreadRequestHistory(history []neoHistoryMessage) []neoHistoryMessage {
	if len(history) == 0 {
		return nil
	}
	compacted := make([]neoHistoryMessage, 0, len(history))
	for _, message := range history {
		compacted = append(compacted, neoReadThreadCompactHistoryMessage(message))
	}
	return neoReadThreadTrimHistory(compacted, neoReadThreadHistoryTotalChars)
}

func neoReadThreadCompactHistoryMessage(message neoHistoryMessage) neoHistoryMessage {
	message.Text = strings.TrimSpace(message.Text)
	message.Content = neoReadThreadCompactHistoryContent(message.Content)
	message.OpenAIItems = nil
	if message.Role != "assistant" {
		message.ThinkingBlocks = nil
	}
	return message
}

func neoReadThreadCompactHistoryContent(content []any) []any {
	if len(content) == 0 {
		return nil
	}
	out := make([]any, 0, len(content))
	for _, raw := range content {
		block := mapValue(raw)
		if len(block) == 0 {
			out = append(out, raw)
			continue
		}
		copied := cloneMap(block)
		if text := stringValue(copied["text"]); text != "" {
			copied["text"] = strings.TrimSpace(text)
		}
		out = append(out, copied)
	}
	return out
}

func neoReadThreadTrimHistory(history []neoHistoryMessage, limit int) []neoHistoryMessage {
	if limit <= 0 || neoReadThreadHistoryApproxChars(history) <= limit || len(history) <= 1 {
		return history
	}
	head := history[0]
	groups := neoReadThreadHistoryGroups(history[1:])
	kept := make([][]neoHistoryMessage, 0, len(groups))
	total := neoReadThreadHistoryApproxChars([]neoHistoryMessage{head})
	omitted := 0
	for i := len(groups) - 1; i >= 0; i-- {
		group := groups[i]
		groupSize := neoReadThreadHistoryApproxChars(group)
		if total+groupSize <= limit {
			kept = append(kept, group)
			total += groupSize
			continue
		}
		groupLimit := limit - total
		if groupLimit > neoReadThreadHistoryNoticeChars {
			groupLimit -= neoReadThreadHistoryNoticeChars
		}
		trimmed, dropped := neoReadThreadTrimHistoryGroup(group, groupLimit)
		omitted += dropped
		if len(trimmed) > 0 {
			groupSize = neoReadThreadHistoryApproxChars(trimmed)
			if total+groupSize <= limit {
				kept = append(kept, trimmed)
				total += groupSize
				if dropped == 0 {
					continue
				}
				for j := i - 1; j >= 0; j-- {
					omitted += len(groups[j])
				}
				break
			}
			omitted += len(trimmed)
		}
		for j := i - 1; j >= 0; j-- {
			omitted += len(groups[j])
		}
		break
	}
	if omitted > 0 {
		notice := fmt.Sprintf("[read_thread omitted %d older internal observation messages from this subagent context to stay within the model window. Search or read exact message ranges again if those details are needed.]", omitted)
		head.Text = strings.TrimSpace(head.Text) + "\n\n" + notice
	}
	out := make([]neoHistoryMessage, 0, 1+len(history)-omitted)
	out = append(out, head)
	for i := len(kept) - 1; i >= 0; i-- {
		out = append(out, kept[i]...)
	}
	return out
}

func neoReadThreadTrimHistoryGroup(group []neoHistoryMessage, limit int) ([]neoHistoryMessage, int) {
	if limit <= 0 || len(group) == 0 {
		return nil, len(group)
	}
	trimmed := make([]neoHistoryMessage, len(group))
	copy(trimmed, group)
	dropped := 0
	for neoReadThreadHistoryApproxChars(trimmed) > limit {
		index := neoReadThreadFirstToolMessageIndex(trimmed)
		if index < 0 {
			break
		}
		toolCallID := trimmed[index].ToolCallID
		if neoReadThreadToolMessageCount(trimmed) == 1 {
			otherSize := neoReadThreadHistoryApproxChars(trimmed[:index]) + neoReadThreadHistoryApproxChars(trimmed[index+1:])
			trimmed[index] = neoReadThreadClipHistoryMessageToApproxChars(trimmed[index], limit-otherSize)
			if neoReadThreadHistoryApproxChars(trimmed) <= limit {
				break
			}
		}
		trimmed = append(trimmed[:index], trimmed[index+1:]...)
		dropped++
		if toolCallID != "" {
			for i := range trimmed {
				trimmed[i].ToolCalls = neoReadThreadFilterToolCalls(trimmed[i].ToolCalls, toolCallID)
			}
		}
		var emptyDropped int
		trimmed, emptyDropped = neoReadThreadDropEmptyAssistantToolCallMessages(trimmed)
		dropped += emptyDropped
	}
	for neoReadThreadHistoryApproxChars(trimmed) > limit && len(trimmed) > 1 {
		trimmed = trimmed[1:]
		dropped++
	}
	if neoReadThreadHistoryApproxChars(trimmed) > limit && len(trimmed) == 1 {
		trimmed[0] = neoReadThreadClipHistoryMessageToApproxChars(trimmed[0], limit)
	}
	if neoReadThreadHistoryApproxChars(trimmed) > limit {
		return nil, dropped + len(trimmed)
	}
	return trimmed, dropped
}

func neoReadThreadFirstToolMessageIndex(history []neoHistoryMessage) int {
	for i, message := range history {
		if message.Role == "tool" {
			return i
		}
	}
	return -1
}

func neoReadThreadToolMessageCount(history []neoHistoryMessage) int {
	count := 0
	for _, message := range history {
		if message.Role == "tool" {
			count++
		}
	}
	return count
}

func neoReadThreadFilterToolCalls(calls []neoToolCall, dropID string) []neoToolCall {
	if len(calls) == 0 || dropID == "" {
		return calls
	}
	out := make([]neoToolCall, 0, len(calls))
	for _, call := range calls {
		if call.ID != dropID {
			out = append(out, call)
		}
	}
	return out
}

func neoReadThreadDropEmptyAssistantToolCallMessages(history []neoHistoryMessage) ([]neoHistoryMessage, int) {
	out := history[:0]
	dropped := 0
	for _, message := range history {
		if message.Role == "assistant" && strings.TrimSpace(message.Text) == "" && len(message.Content) == 0 && len(message.ToolCalls) == 0 {
			dropped++
			continue
		}
		out = append(out, message)
	}
	return out, dropped
}

func neoReadThreadClipHistoryMessageToApproxChars(message neoHistoryMessage, limit int) neoHistoryMessage {
	message.Content = nil
	message.OpenAIItems = nil
	message.ToolCalls = nil
	message.ThinkingBlocks = nil
	overhead := len(message.Role) + len(message.ToolCallID) + len(message.ToolName) + len(message.ParentToolUseID) + 64
	textLimit := limit - overhead
	if textLimit < 0 {
		textLimit = 0
	}
	message.Text = neoClipRunes(strings.TrimSpace(message.Text), textLimit)
	return message
}

func neoReadThreadHistoryGroups(history []neoHistoryMessage) [][]neoHistoryMessage {
	groups := make([][]neoHistoryMessage, 0, len(history))
	for i := 0; i < len(history); {
		start := i
		i++
		for i < len(history) && history[i].Role == "tool" {
			i++
		}
		group := make([]neoHistoryMessage, i-start)
		copy(group, history[start:i])
		groups = append(groups, group)
	}
	return groups
}

func neoReadThreadHistoryApproxChars(history []neoHistoryMessage) int {
	total := 0
	for _, message := range history {
		total += len(message.Role) + len(message.Text) + len(message.ToolCallID) + len(message.ToolName) + len(message.ParentToolUseID)
		if len(message.ToolCalls) > 0 {
			raw, _ := json.Marshal(message.ToolCalls)
			total += len(raw)
		}
		if len(message.Content) > 0 {
			raw, _ := json.Marshal(message.Content)
			total += len(raw)
		}
		if len(message.ThinkingBlocks) > 0 {
			raw, _ := json.Marshal(message.ThinkingBlocks)
			total += len(raw)
		}
	}
	return total
}

func neoReadThreadFinalHistory(history []neoHistoryMessage) []neoHistoryMessage {
	sanitized := sanitizeNeoHistoryToolPairs(history)
	plain := make([]neoHistoryMessage, 0, len(sanitized))
	for _, message := range sanitized {
		switch message.Role {
		case "assistant":
			message.ToolCalls = nil
			message.Content = nil
			message.OpenAIItems = nil
			message.ThinkingBlocks = nil
			if strings.TrimSpace(message.Text) == "" {
				continue
			}
			plain = append(plain, message)
		case "tool":
			text := strings.TrimSpace(message.Text)
			if text == "" {
				text = runToText(map[string]any{"status": "done", "result": message.Content})
			}
			if text != "" {
				plain = append(plain, neoHistoryMessage{Role: "user", Text: "Prior read_thread internal tool result:\n" + text})
			}
		default:
			plain = append(plain, message)
		}
	}
	return neoReadThreadMergeAdjacentUsers(plain)
}

func neoReadThreadMergeAdjacentUsers(history []neoHistoryMessage) []neoHistoryMessage {
	merged := make([]neoHistoryMessage, 0, len(history))
	for _, message := range history {
		message.Content = append([]any(nil), message.Content...)
		last := len(merged) - 1
		if message.Role == "user" && last >= 0 && merged[last].Role == "user" && message.ToolCallID == "" && len(message.ToolCalls) == 0 && len(message.Content) == 0 && len(message.OpenAIItems) == 0 && merged[last].ToolCallID == "" && len(merged[last].ToolCalls) == 0 && len(merged[last].Content) == 0 && len(merged[last].OpenAIItems) == 0 {
			left := strings.TrimSpace(merged[last].Text)
			right := strings.TrimSpace(message.Text)
			switch {
			case left == "":
				merged[last].Text = right
			case right != "":
				merged[last].Text = left + "\n\n" + right
			}
			continue
		}
		merged = append(merged, message)
	}
	return merged
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
	out.WriteString("Latest message index: " + strconv.Itoa(neoReadThreadLatestMessageIndex(corpus)) + "\n\n")
	out.WriteString("Goal:\n")
	out.WriteString(goal)
	out.WriteString("\n\nStart with search_thread_messages. If you need orientation first, call thread_overview once, then search. For continuation goals, anchor on the latest user instruction in the tail before older matching topics. After finding relevant hits, read exact messages and later/latest messages before final JSON.")
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
		return "Before your final answer, call read_thread_messages with latest=true and count=" + strconv.Itoa(neoReadThreadLatestReadCount) + " through index " + strconv.Itoa(neoReadThreadLatestMessageIndex(corpus)) + " to check for revisions, superseding decisions, reverts, contradictions, and continuation instructions."
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

func neoReadThreadInternalToolSpecsForState(sawOverview, sawSearch bool) []neoToolSpec {
	tools := neoReadThreadInternalToolSpecs()
	if !sawOverview && !sawSearch {
		return tools
	}
	out := make([]neoToolSpec, 0, len(tools))
	for _, tool := range tools {
		switch tool.Name {
		case "thread_overview":
			continue
		case "read_thread_messages":
			if !sawSearch {
				continue
			}
		}
		out = append(out, tool)
	}
	return out
}

func neoReadThreadInternalToolSpecsForAgentState(state neoReadThreadAgentState) []neoToolSpec {
	if state.ToolCalls >= neoReadThreadMaxToolCalls {
		return nil
	}
	tools := neoReadThreadInternalToolSpecsForState(state.SawOverview, state.SawSearch)
	out := make([]neoToolSpec, 0, len(tools))
	for _, tool := range tools {
		switch tool.Name {
		case "search_thread_messages":
			if state.SearchCalls >= neoReadThreadMaxSearchCalls {
				continue
			}
		case "read_thread_messages":
			if state.ReadCalls >= neoReadThreadMaxReadCalls {
				continue
			}
		}
		out = append(out, tool)
	}
	return out
}

func neoReadThreadReadyToForceFinal(state neoReadThreadAgentState) bool {
	return state.ToolCalls >= neoReadThreadForceFinalCalls && state.SawSearch && state.SawRead
}

func neoReadThreadExecuteAgentTool(state *neoReadThreadAgentState, corpus neoReadThreadCorpus, call neoToolCall, acceptedThisTurn int) (map[string]any, bool) {
	name := strings.TrimSpace(call.Name)
	if acceptedThisTurn >= neoReadThreadMaxCallsPerTurn {
		return neoReadThreadRejectedRun(fmt.Sprintf("read_thread accepts at most %d tool calls per model turn; continue with the completed results", neoReadThreadMaxCallsPerTurn)), false
	}
	if state.ToolCalls >= neoReadThreadMaxToolCalls {
		return neoReadThreadRejectedRun("read_thread tool-call budget is exhausted; synthesize the answer from completed results"), false
	}
	if neoReadThreadReadyToForceFinal(*state) {
		return neoReadThreadRejectedRun("read_thread reserved the remaining tool budget for synthesis from completed results"), false
	}
	if state.SeenCalls == nil {
		state.SeenCalls = map[string]struct{}{}
	}
	key := neoReadThreadToolCallKey(corpus, call)
	if _, duplicate := state.SeenCalls[key]; duplicate {
		return neoReadThreadRejectedRun("duplicate read_thread tool call; use the earlier result or refine the query/range"), false
	}
	if name == "read_thread_messages" {
		if start, end, err := neoReadThreadRangeForCorpus(call.Input, corpus); err == nil && neoReadThreadPositionRangeCovered(state.ReadRanges, start, end) {
			return neoReadThreadRejectedRun("read_thread message range was already covered; use the earlier result or read a different range"), false
		}
	}
	switch name {
	case "search_thread_messages":
		if state.SearchCalls >= neoReadThreadMaxSearchCalls {
			return neoReadThreadRejectedRun("read_thread search budget is exhausted; read the strongest existing hits"), false
		}
		state.SearchCalls++
	case "read_thread_messages":
		if state.ReadCalls >= neoReadThreadMaxReadCalls {
			return neoReadThreadRejectedRun("read_thread message-read budget is exhausted; synthesize the answer from completed reads"), false
		}
		state.ReadCalls++
	}
	state.ToolCalls++
	run, observation := neoReadThreadExecuteInternalToolForState(corpus, call, state.SawOverview, state.SawSearch)
	if stringValue(run["status"]) == "done" {
		state.SeenCalls[key] = struct{}{}
	}
	if name == "thread_overview" && stringValue(run["status"]) == "done" {
		state.SawOverview = true
	}
	if observation.Searched {
		state.SawSearch = true
	}
	if observation.Read {
		state.SawRead = true
		if readRange, ok := neoReadThreadResultPositionRange(run, corpus); ok {
			state.ReadRanges = append(state.ReadRanges, readRange)
		}
		if observation.ReadEnd >= neoReadThreadLatestMessageIndex(corpus) {
			state.SawLatest = true
		}
	}
	return run, true
}

func neoReadThreadPositionRangeCovered(ranges []neoReadThreadPositionRange, start, end int) bool {
	if start < 0 || end < start || len(ranges) == 0 {
		return false
	}
	for position := start; position <= end; position++ {
		covered := false
		for _, readRange := range ranges {
			if position >= readRange.Start && position <= readRange.End {
				covered = true
				break
			}
		}
		if !covered {
			return false
		}
	}
	return true
}

func neoReadThreadResultPositionRange(run map[string]any, corpus neoReadThreadCorpus) (neoReadThreadPositionRange, bool) {
	resultRange := mapValue(mapValue(run["result"])["range"])
	rawStart, hasStart := neoReadThreadInputInt(resultRange, "startIndex")
	rawEnd, hasEnd := neoReadThreadInputInt(resultRange, "endIndex")
	if !hasStart || !hasEnd {
		return neoReadThreadPositionRange{}, false
	}
	start := neoReadThreadFirstVisiblePositionAtOrAfter(corpus, rawStart)
	end := neoReadThreadLastVisiblePositionAtOrBefore(corpus, rawEnd)
	if start < 0 || end < start {
		return neoReadThreadPositionRange{}, false
	}
	return neoReadThreadPositionRange{Start: start, End: end}, true
}

func neoReadThreadRejectedRun(message string) map[string]any {
	return map[string]any{"status": "error", "error": map[string]any{"message": message}}
}

func neoReadThreadPersistedRun(toolName string, run map[string]any) map[string]any {
	raw, err := json.Marshal(run)
	if err != nil {
		return cloneMap(run)
	}
	stored := map[string]any{}
	if err := json.Unmarshal(raw, &stored); err != nil {
		return cloneMap(run)
	}
	result := mapValue(stored["result"])
	if len(result) == 0 {
		return stored
	}
	switch strings.TrimSpace(toolName) {
	case "search_thread_messages":
		hits := arrayValue(result["hits"])
		kept := hits
		if len(kept) > neoReadThreadStoredHitCount {
			kept = kept[:neoReadThreadStoredHitCount]
			result["persistedHitsOmitted"] = len(hits) - len(kept)
		}
		compacted := make([]any, 0, len(kept))
		for _, rawHit := range kept {
			hit := cloneMap(mapValue(rawHit))
			hit["excerpt"] = neoClipRunes(stringValue(hit["excerpt"]), neoReadThreadStoredExcerptChars)
			compacted = append(compacted, hit)
		}
		result["hits"] = compacted
	case "read_thread_messages":
		messages := arrayValue(result["messages"])
		positions := neoReadThreadStoredPositions(len(messages), neoReadThreadStoredMessageCount)
		compacted := make([]any, 0, len(positions))
		for _, position := range positions {
			message := cloneMap(mapValue(messages[position]))
			message["text"] = neoClipRunes(stringValue(message["text"]), neoReadThreadStoredMessageChars)
			for _, field := range []string{"toolUses", "toolResults"} {
				blocks := arrayValue(message[field])
				kept := blocks
				if len(kept) > neoReadThreadStoredToolBlocks {
					kept = kept[:neoReadThreadStoredToolBlocks]
					omittedField := "persistedToolUsesOmitted"
					if field == "toolResults" {
						omittedField = "persistedToolResultsOmitted"
					}
					message[omittedField] = len(blocks) - len(kept)
				}
				clipped := make([]any, 0, len(kept))
				for _, rawBlock := range kept {
					block := cloneMap(mapValue(rawBlock))
					if text := stringValue(block["text"]); text != "" {
						block["text"] = neoClipRunes(text, neoReadThreadStoredResultChars)
					}
					if input, ok := block["input"]; field == "toolUses" && ok {
						block["input"] = neoReadThreadPersistedToolInput(input)
					}
					clipped = append(clipped, block)
				}
				if len(blocks) > 0 {
					message[field] = clipped
				}
			}
			compacted = append(compacted, message)
		}
		if len(positions) < len(messages) {
			result["persistedMessagesOmitted"] = len(messages) - len(positions)
		}
		result["messages"] = compacted
	}
	stored["result"] = result
	return stored
}

func neoReadThreadPersistedToolInput(input any) any {
	raw, err := json.Marshal(input)
	if err == nil && len(raw) <= neoReadThreadStoredResultChars {
		return input
	}
	preview := ""
	if err == nil {
		preview = neoClipRunes(string(raw), neoReadThreadStoredResultChars)
	}
	return map[string]any{"persistedInputTruncated": true, "preview": preview}
}

func neoReadThreadStoredPositions(length, limit int) []int {
	if length <= 0 || limit <= 0 {
		return nil
	}
	if length <= limit {
		positions := make([]int, length)
		for i := range positions {
			positions[i] = i
		}
		return positions
	}
	firstCount := limit / 2
	positions := make([]int, 0, limit)
	for i := 0; i < firstCount; i++ {
		positions = append(positions, i)
	}
	for i := length - (limit - firstCount); i < length; i++ {
		positions = append(positions, i)
	}
	return positions
}

func neoReadThreadToolCallKey(corpus neoReadThreadCorpus, call neoToolCall) string {
	name := strings.TrimSpace(call.Name)
	if name == "read_thread_messages" {
		if start, end, err := neoReadThreadRangeForCorpus(call.Input, corpus); err == nil {
			mode := "forward"
			if neoReadThreadLatestRequested(call.Input) {
				mode = "tail"
			}
			return fmt.Sprintf("%s:%s:%d:%d", name, mode, start, end)
		}
	}
	input := cloneNeoJSONMap(call.Input)
	if input == nil {
		input = map[string]any{}
	}
	if name == "search_thread_messages" {
		input["query"] = strings.Join(strings.Fields(strings.ToLower(stringValue(input["query"]))), " ")
		if roles := neoStringSlice(input["roles"]); len(roles) > 0 {
			for i := range roles {
				roles[i] = strings.ToLower(strings.TrimSpace(roles[i]))
			}
			sort.Strings(roles)
			input["roles"] = roles
		}
		delete(input, "limit")
	}
	raw, _ := json.Marshal(input)
	return name + ":" + string(raw)
}

func neoReadThreadExecuteInternalTool(corpus neoReadThreadCorpus, call neoToolCall) (map[string]any, neoReadThreadToolObservation) {
	return neoReadThreadExecuteInternalToolForState(corpus, call, false, true)
}

func neoReadThreadExecuteInternalToolForState(corpus neoReadThreadCorpus, call neoToolCall, sawOverview, sawSearch bool) (map[string]any, neoReadThreadToolObservation) {
	switch strings.TrimSpace(call.Name) {
	case "thread_overview":
		if sawOverview {
			return map[string]any{"status": "error", "error": map[string]any{"message": "thread_overview was already called; use search_thread_messages next."}}, neoReadThreadToolObservation{}
		}
		return map[string]any{"status": "done", "result": neoReadThreadOverview(corpus)}, neoReadThreadToolObservation{}
	case "search_thread_messages":
		result, err := neoReadThreadSearch(corpus, call.Input)
		if err != nil {
			return map[string]any{"status": "error", "error": map[string]any{"message": err.Error()}}, neoReadThreadToolObservation{}
		}
		return map[string]any{"status": "done", "result": result}, neoReadThreadToolObservation{Searched: true}
	case "read_thread_messages":
		if !sawSearch {
			return map[string]any{"status": "error", "error": map[string]any{"message": "search_thread_messages is required before read_thread_messages."}}, neoReadThreadToolObservation{}
		}
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
			first = append(first, message.summaryMap(neoReadThreadOverviewExcerpt))
		}
		if len(corpus.Messages)-i <= neoReadThreadOverviewTailCount {
			last = append(last, message.summaryMap(neoReadThreadOverviewExcerpt))
		}
	}
	return map[string]any{
		"threadID":           corpus.ThreadID,
		"source":             corpus.Source,
		"title":              corpus.Title,
		"messageCount":       len(corpus.Messages),
		"latestMessageIndex": neoReadThreadLatestMessageIndex(corpus),
		"roleCounts":         roleCounts,
		"firstMessages":      first,
		"latestMessages":     last,
	}
}

func neoReadThreadSearch(corpus neoReadThreadCorpus, input map[string]any) (map[string]any, error) {
	query := strings.Join(strings.Fields(stringValue(input["query"])), " ")
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
		position int
		score    int
	}
	hits := make([]hit, 0)
	queryLower := strings.ToLower(query)
	for position := range corpus.Messages {
		message := &corpus.Messages[position]
		if hasAfter && message.Index <= after {
			continue
		}
		if hasBefore && message.Index >= before {
			continue
		}
		if len(roles) > 0 && !roles[strings.ToLower(message.Role)] {
			continue
		}
		haystack := message.normalizedSearchText()
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
		hits = append(hits, hit{position: position, score: score})
	}
	sort.SliceStable(hits, func(i, j int) bool {
		if hits[i].score != hits[j].score {
			return hits[i].score > hits[j].score
		}
		return corpus.Messages[hits[i].position].Index < corpus.Messages[hits[j].position].Index
	})
	if len(hits) > limit {
		hits = hits[:limit]
	}
	items := make([]any, 0, len(hits))
	for _, hit := range hits {
		message := corpus.Messages[hit.position]
		items = append(items, map[string]any{
			"index":             message.Index,
			"role":              message.Role,
			"messageID":         message.MessageID,
			"createdAt":         message.CreatedAt,
			"parentToolUseID":   message.ParentToolUseID,
			"score":             hit.score,
			"excerpt":           neoReadThreadExcerpt(message.Text, query, terms, neoReadThreadSearchExcerpt),
			"hasLaterMessages":  hit.position < len(corpus.Messages)-1,
			"laterMessageCount": neoReadThreadLaterVisibleMessageCount(corpus, hit.position),
			"recommendedRead": map[string]any{
				"startIndex": message.Index,
				"count":      neoReadThreadRecommendedReadCount(hit.position, len(corpus.Messages)),
			},
		})
	}
	return map[string]any{
		"threadID":           corpus.ThreadID,
		"source":             corpus.Source,
		"query":              query,
		"messageCount":       len(corpus.Messages),
		"latestMessageIndex": neoReadThreadLatestMessageIndex(corpus),
		"hits":               items,
	}, nil
}

func neoReadThreadRead(corpus neoReadThreadCorpus, input map[string]any) (map[string]any, int, error) {
	if len(corpus.Messages) == 0 {
		return nil, -1, errors.New("thread has no readable messages")
	}
	start, end, err := neoReadThreadRangeForCorpus(input, corpus)
	if err != nil {
		return nil, -1, err
	}
	if neoReadThreadLatestRequested(input) {
		messages := make([]any, 0, end-start+1)
		total := 0
		actualStartPosition := end + 1
		for i := end; i >= start; i-- {
			entry := corpus.Messages[i].detailMap(neoReadThreadReadMessageChars)
			raw, _ := json.Marshal(entry)
			if total+len(raw) > neoReadThreadReadTotalChars && len(messages) > 0 {
				break
			}
			total += len(raw)
			actualStartPosition = i
			messages = append(messages, entry)
		}
		for left, right := 0, len(messages)-1; left < right; left, right = left+1, right-1 {
			messages[left], messages[right] = messages[right], messages[left]
		}
		result := map[string]any{
			"threadID":           corpus.ThreadID,
			"source":             corpus.Source,
			"messageCount":       len(corpus.Messages),
			"latestMessageIndex": neoReadThreadLatestMessageIndex(corpus),
			"range": map[string]any{
				"startIndex": corpus.Messages[actualStartPosition].Index,
				"endIndex":   corpus.Messages[end].Index,
			},
			"messages": messages,
		}
		if actualStartPosition > start {
			result["truncated"] = true
			result["previousEndIndex"] = corpus.Messages[actualStartPosition-1].Index
		}
		neoReadThreadAnnotateSparseRange(result, input)
		return result, corpus.Messages[end].Index, nil
	}
	messages := make([]any, 0, end-start+1)
	total := 0
	actualEnd := -1
	actualEndPosition := start - 1
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
		actualEnd = message.Index
		actualEndPosition = i
		messages = append(messages, entry)
	}
	result := map[string]any{
		"threadID":           corpus.ThreadID,
		"source":             corpus.Source,
		"messageCount":       len(corpus.Messages),
		"latestMessageIndex": neoReadThreadLatestMessageIndex(corpus),
		"range": map[string]any{
			"startIndex": corpus.Messages[start].Index,
			"endIndex":   actualEnd,
		},
		"messages": messages,
	}
	if truncated || actualEndPosition < end {
		result["truncated"] = true
		if actualEndPosition+1 < len(corpus.Messages) {
			if nextPosition := neoReadThreadFirstVisiblePositionAfter(corpus, actualEnd); nextPosition >= 0 {
				result["nextStartIndex"] = corpus.Messages[nextPosition].Index
			}
		}
	}
	neoReadThreadAnnotateSparseRange(result, input)
	return result, actualEnd, nil
}

func neoReadThreadAnnotateSparseRange(result, input map[string]any) {
	if neoReadThreadLatestRequested(input) {
		return
	}
	if _, hasAfter := neoReadThreadInputInt(input, "afterIndex"); hasAfter {
		return
	}
	requestedStart, hasStart := neoReadThreadInputInt(input, "startIndex", "messageIndex", "index")
	requestedEnd, hasEnd := neoReadThreadInputInt(input, "endIndex")
	if !hasStart || !hasEnd {
		return
	}
	actualRange := mapValue(result["range"])
	actualStart, actualStartOK := neoReadThreadInputInt(actualRange, "startIndex")
	actualEnd, actualEndOK := neoReadThreadInputInt(actualRange, "endIndex")
	if !actualStartOK || !actualEndOK || (actualStart >= requestedStart && actualEnd <= requestedEnd) {
		return
	}
	result["sparseRangeNormalized"] = true
	result["requestedRange"] = map[string]any{"startIndex": requestedStart, "endIndex": requestedEnd}
}

func neoReadThreadLatestRequested(input map[string]any) bool {
	if boolValue(input["latest"]) {
		return true
	}
	position := strings.ToLower(strings.TrimSpace(stringValue(input["position"])))
	return position == "latest" || position == "tail"
}

func neoReadThreadRangeForCorpus(input map[string]any, corpus neoReadThreadCorpus) (int, int, error) {
	count, hasCount := neoReadThreadInputInt(input, "count", "limit")
	if !hasCount || count <= 0 {
		count = neoReadThreadReadCount
	}
	if count > neoReadThreadReadCountMax {
		count = neoReadThreadReadCountMax
	}

	start := 0
	hasStart := false
	explicitIndexLookup := false
	rawStart, hasRawStart := neoReadThreadInputInt(input, "startIndex", "messageIndex", "index")
	if hasRawStart {
		start = neoReadThreadFirstVisiblePositionAtOrAfter(corpus, rawStart)
		hasStart = true
		explicitIndexLookup = true
	}
	if latest := boolValue(input["latest"]); latest {
		start = len(corpus.Messages) - count
		hasStart = true
		explicitIndexLookup = false
		hasRawStart = false
	}
	if position := strings.ToLower(strings.TrimSpace(stringValue(input["position"]))); position == "latest" || position == "tail" {
		start = len(corpus.Messages) - count
		hasStart = true
		explicitIndexLookup = false
		hasRawStart = false
	}
	if after, ok := neoReadThreadInputInt(input, "afterIndex"); ok {
		start = neoReadThreadFirstVisiblePositionAfter(corpus, after)
		hasStart = true
		explicitIndexLookup = true
		hasRawStart = false
	}
	if !hasStart {
		start = 0
	}
	if start < 0 {
		if explicitIndexLookup {
			return 0, 0, fmt.Errorf("startIndex is outside thread message range 0-%d", neoReadThreadLatestMessageIndex(corpus))
		}
		start = 0
	}
	if start >= len(corpus.Messages) || start < 0 {
		return 0, 0, fmt.Errorf("startIndex is outside thread message range 0-%d", neoReadThreadLatestMessageIndex(corpus))
	}

	end := start + count - 1
	rawEnd, hasRawEnd := neoReadThreadInputInt(input, "endIndex")
	if hasRawEnd {
		if hasRawStart && rawEnd < rawStart {
			return 0, 0, fmt.Errorf("endIndex is before startIndex")
		}
		end = neoReadThreadLastVisiblePositionAtOrBefore(corpus, rawEnd)
	}
	if end >= len(corpus.Messages) {
		end = len(corpus.Messages) - 1
	}
	if end < start {
		if hasRawStart && hasRawEnd {
			before := neoReadThreadLastVisiblePositionAtOrBefore(corpus, rawEnd)
			after := neoReadThreadFirstVisiblePositionAtOrAfter(corpus, rawStart)
			switch {
			case before >= 0 && after >= 0 && before < after:
				return before, after, nil
			case after >= 0:
				return after, after, nil
			case before >= 0:
				return before, before, nil
			}
		}
		return 0, 0, fmt.Errorf("endIndex is before startIndex")
	}
	return start, end, nil
}

func neoReadThreadLatestMessageIndex(corpus neoReadThreadCorpus) int {
	if len(corpus.Messages) == 0 {
		return -1
	}
	return corpus.Messages[len(corpus.Messages)-1].Index
}

func neoReadThreadMessagePosition(corpus neoReadThreadCorpus, index int) int {
	for position, message := range corpus.Messages {
		if message.Index == index {
			return position
		}
	}
	return -1
}

func neoReadThreadLaterVisibleMessageCount(corpus neoReadThreadCorpus, position int) int {
	if position < 0 || position >= len(corpus.Messages) {
		return 0
	}
	return len(corpus.Messages) - position - 1
}

func neoReadThreadFirstVisiblePositionAtOrAfter(corpus neoReadThreadCorpus, index int) int {
	for position, message := range corpus.Messages {
		if message.Index >= index {
			return position
		}
	}
	return -1
}

func neoReadThreadFirstVisiblePositionAfter(corpus neoReadThreadCorpus, index int) int {
	for position, message := range corpus.Messages {
		if message.Index > index {
			return position
		}
	}
	return -1
}

func neoReadThreadLastVisiblePositionAtOrBefore(corpus neoReadThreadCorpus, index int) int {
	for position := len(corpus.Messages) - 1; position >= 0; position-- {
		if corpus.Messages[position].Index <= index {
			return position
		}
	}
	return -1
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
		out["toolResults"] = neoReadThreadClipToolResults(m.ToolResults, neoReadThreadOverviewResultText)
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

func neoReadThreadClipToolResults(results []neoReadThreadToolResult, limit int) []neoReadThreadToolResult {
	if len(results) == 0 {
		return nil
	}
	out := make([]neoReadThreadToolResult, 0, len(results))
	for _, result := range results {
		result.Text = neoClipRunes(result.Text, limit)
		out = append(out, result)
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

func (m *neoReadThreadMessage) normalizedSearchText() string {
	if m.searchTextLower == "" {
		m.searchTextLower = strings.ToLower(m.searchText())
	}
	return m.searchTextLower
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
	lowerText := strings.ToLower(text)
	if utf8.RuneCountInString(lowerText) == utf8.RuneCountInString(text) {
		for _, raw := range needles {
			needle := strings.ToLower(strings.TrimSpace(raw))
			if needle == "" {
				continue
			}
			if byteIndex := strings.Index(lowerText, needle); byteIndex >= 0 {
				return utf8.RuneCountInString(lowerText[:byteIndex])
			}
		}
		return -1
	}
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
