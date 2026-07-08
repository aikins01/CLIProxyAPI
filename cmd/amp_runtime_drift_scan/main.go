package main

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type scanOptions struct {
	threadDir             string
	captureDir            string
	baselinePath          string
	threadScope           string
	threadID              string
	modelContextWindows   map[string]int
	smartModel            string
	since                 time.Time
	allowMissing          bool
	jsonOutput            bool
	requireCaptureSummary bool
}

type driftFinding struct {
	Source    string `json:"source"`
	File      string `json:"file"`
	ThreadID  string `json:"thread_id,omitempty"`
	MessageID string `json:"message_id,omitempty"`
	CallID    string `json:"call_id,omitempty"`
	ToolName  string `json:"tool_name"`
	Detail    string `json:"detail"`
}

var payloadRequiredTools = map[string]struct{}{
	"bash":          {},
	"find_thread":   {},
	"read_thread":   {},
	"shell_command": {},
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("amp_runtime_drift_scan", flag.ContinueOnError)
	flags.SetOutput(stderr)
	threadDir := flags.String("thread-dir", defaultThreadDir(), "Amp local thread JSON directory to scan")
	captureDir := flags.String("capture-dir", defaultCaptureDir(), "Neo provider request capture directory to scan")
	baselinePath := flags.String("baseline", defaultBaselinePath(), "Amp binary parity baseline JSON used for model context windows")
	threadScope := flags.String("thread-scope", "local-runtime", "thread files to scan: local-runtime or all")
	threadID := flags.String("thread", "", "only scan one Amp thread ID across thread files and provider captures")
	sinceRaw := flags.String("since", "", "only scan files modified at or after this RFC3339 timestamp")
	sinceFile := flags.String("since-file", "", "only scan files and timestamped thread messages at or after this file's modification time")
	sinceHomebrewRuntime := flags.Bool("since-homebrew-runtime", false, "only scan data at or after the Homebrew cliproxyapi.real replacement time")
	allowMissing := flags.Bool("allow-missing", true, "treat missing scan directories as empty")
	jsonOutput := flags.Bool("json", false, "print findings as JSON")
	summaryOutput := flags.Bool("summary", false, "print grouped finding summary instead of individual findings")
	requireCaptureSummary := flags.Bool("require-capture-summary", false, "flag provider captures missing compact summary metadata")
	if err := flags.Parse(args); err != nil {
		return 2
	}

	since, err := parseSinceCutoff(*sinceRaw, *sinceFile, *sinceHomebrewRuntime)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}

	findings, err := scanRuntimeDrift(scanOptions{
		threadDir:             *threadDir,
		captureDir:            *captureDir,
		baselinePath:          *baselinePath,
		threadScope:           *threadScope,
		threadID:              *threadID,
		since:                 since,
		allowMissing:          *allowMissing,
		jsonOutput:            *jsonOutput,
		requireCaptureSummary: *requireCaptureSummary,
	})
	if err != nil {
		fmt.Fprintf(stderr, "scan runtime drift: %v\n", err)
		return 1
	}

	if *jsonOutput {
		if err := json.NewEncoder(stdout).Encode(findings); err != nil {
			fmt.Fprintf(stderr, "write JSON: %v\n", err)
			return 1
		}
	} else if len(findings) == 0 {
		fmt.Fprintln(stdout, "no runtime drift found")
	} else if *summaryOutput {
		printFindingSummary(stdout, findings)
	} else {
		fmt.Fprintf(stdout, "found %d runtime drift(s):\n", len(findings))
		for _, finding := range findings {
			fmt.Fprintf(stdout, "- %s %s %s %s: %s\n", finding.Source, finding.ToolName, finding.CallID, finding.File, finding.Detail)
		}
	}

	if len(findings) > 0 {
		return 2
	}
	return 0
}

type findingSummary struct {
	Count    int
	Source   string
	ToolName string
	Category string
}

func printFindingSummary(stdout io.Writer, findings []driftFinding) {
	summary := summarizeFindings(findings)
	fmt.Fprintf(stdout, "found %d runtime drift(s) in %d group(s):\n", len(findings), len(summary))
	for _, item := range summary {
		fmt.Fprintf(stdout, "- %d %s %s: %s\n", item.Count, item.Source, item.ToolName, item.Category)
	}
}

func summarizeFindings(findings []driftFinding) []findingSummary {
	type summaryKey struct {
		source   string
		toolName string
		category string
	}
	counts := map[summaryKey]int{}
	for _, finding := range findings {
		key := summaryKey{
			source:   finding.Source,
			toolName: finding.ToolName,
			category: findingCategory(finding.Detail),
		}
		counts[key]++
	}
	summary := make([]findingSummary, 0, len(counts))
	for key, count := range counts {
		summary = append(summary, findingSummary{
			Count:    count,
			Source:   key.source,
			ToolName: key.toolName,
			Category: key.category,
		})
	}
	sort.Slice(summary, func(i, j int) bool {
		if summary[i].Count != summary[j].Count {
			return summary[i].Count > summary[j].Count
		}
		if summary[i].Source != summary[j].Source {
			return summary[i].Source < summary[j].Source
		}
		if summary[i].ToolName != summary[j].ToolName {
			return summary[i].ToolName < summary[j].ToolName
		}
		return summary[i].Category < summary[j].Category
	})
	return summary
}

func findingCategory(detail string) string {
	detail = strings.TrimSpace(detail)
	switch {
	case strings.HasPrefix(detail, "smart usage observed ") && strings.Contains(detail, " with no later compaction record"):
		return "smart usage at/above compaction threshold without later compaction"
	case strings.HasPrefix(detail, "smart compaction observed "):
		return "smart compaction below threshold"
	case strings.HasPrefix(detail, "currentInference ") || strings.HasPrefix(detail, "pendingInference "):
		return "invalid persisted inference state"
	case detail == "thread compacting persisted without current or pending inference":
		return "invalid persisted inference state"
	case strings.HasPrefix(detail, "complete tool_use has no matching tool_result before "):
		return "complete tool_use missing matching tool_result"
	case strings.HasPrefix(detail, "model input contains tool call with no matching tool result before "):
		return "provider tool call missing matching tool result"
	case strings.HasPrefix(detail, "model input contains non-terminal tool_result status "):
		return "provider input contains non-terminal tool_result"
	case strings.HasPrefix(detail, "non-terminal tool_result status "):
		return "thread contains non-terminal tool_result"
	case detail == `terminal "done" has no result/output payload` || detail == `model input contains terminal "done" with no result/output payload`:
		return "terminal done missing result/output payload"
	case strings.HasPrefix(detail, "provider capture summary "):
		return "provider capture summary mismatch"
	case detail == "provider capture missing request summary":
		return "provider capture missing request summary"
	default:
		return detail
	}
}

func parseSinceCutoff(sinceRaw, sinceFile string, sinceHomebrewRuntime bool) (time.Time, error) {
	var since time.Time
	if strings.TrimSpace(sinceRaw) != "" {
		parsed, err := time.Parse(time.RFC3339, strings.TrimSpace(sinceRaw))
		if err != nil {
			return time.Time{}, fmt.Errorf("parse -since: %w", err)
		}
		since = parsed
	}
	if strings.TrimSpace(sinceFile) != "" {
		info, err := os.Stat(strings.TrimSpace(sinceFile))
		if err != nil {
			return time.Time{}, fmt.Errorf("stat -since-file: %w", err)
		}
		fileTime := info.ModTime()
		if since.IsZero() || fileTime.After(since) {
			since = fileTime
		}
	}
	if sinceHomebrewRuntime {
		fileTime, err := fileModTime(homebrewRuntimeBinaryPath())
		if err != nil && errors.Is(err, os.ErrNotExist) {
			fileTime, err = fileModTime(homebrewRuntimeFallbackBinaryPath())
		}
		if err != nil {
			return time.Time{}, fmt.Errorf("stat -since-homebrew-runtime: %w", err)
		}
		if since.IsZero() || fileTime.After(since) {
			since = fileTime
		}
	}
	return since, nil
}

func fileModTime(path string) (time.Time, error) {
	info, err := os.Stat(path)
	if err != nil {
		return time.Time{}, err
	}
	return info.ModTime(), nil
}

var homebrewRuntimeBinaryPath = func() string {
	return filepath.Join(string(os.PathSeparator), "opt", "homebrew", "opt", "cliproxyapi", "bin", "cliproxyapi.real")
}

var homebrewRuntimeFallbackBinaryPath = func() string {
	return filepath.Join(string(os.PathSeparator), "opt", "homebrew", "opt", "cliproxyapi", "bin", "cliproxyapi")
}

func defaultThreadDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".local", "share", "amp", "threads")
}

func defaultCaptureDir() string {
	if dir := strings.TrimSpace(os.Getenv("CLIPROXYAPI_NEO_PROVIDER_REQUEST_CAPTURE_DIR")); dir != "" {
		return dir
	}
	if dir := strings.TrimSpace(os.Getenv("CLIPROXY_NEO_PROVIDER_REQUEST_DUMP_DIR")); dir != "" {
		return dir
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".config", "cliproxyapi", "neo-provider-requests")
}

func defaultBaselinePath() string {
	if cwd, err := os.Getwd(); err == nil {
		for dir := cwd; ; dir = filepath.Dir(dir) {
			candidate := filepath.Join(dir, "dev", "amp-binary-parity-baseline.json")
			if _, err := os.Stat(candidate); err == nil {
				return candidate
			}
			parent := filepath.Dir(dir)
			if parent == dir {
				break
			}
		}
	}
	return filepath.Join("dev", "amp-binary-parity-baseline.json")
}

func scanRuntimeDrift(options scanOptions) ([]driftFinding, error) {
	modelContextWindows := options.modelContextWindows
	smartModel := strings.TrimSpace(options.smartModel)
	if modelContextWindows == nil && strings.TrimSpace(options.baselinePath) != "" {
		loaded, loadedSmartModel, err := loadBaselineModelContext(options.baselinePath)
		if err != nil {
			return nil, err
		}
		modelContextWindows = loaded
		smartModel = loadedSmartModel
	}
	findings := []driftFinding{}
	if strings.TrimSpace(options.threadDir) != "" {
		threadFindings, err := scanThreadDir(options.threadDir, options.since, options.allowMissing, normalizeThreadScope(options.threadScope), strings.TrimSpace(options.threadID), modelContextWindows, smartModel)
		if err != nil {
			return nil, err
		}
		findings = append(findings, threadFindings...)
	}
	if strings.TrimSpace(options.captureDir) != "" {
		captureFindings, err := scanCaptureDir(options.captureDir, options.since, options.allowMissing, strings.TrimSpace(options.threadID), options.requireCaptureSummary)
		if err != nil {
			return nil, err
		}
		findings = append(findings, captureFindings...)
	}
	sort.Slice(findings, func(i, j int) bool {
		if findings[i].File != findings[j].File {
			return findings[i].File < findings[j].File
		}
		if findings[i].CallID != findings[j].CallID {
			return findings[i].CallID < findings[j].CallID
		}
		return findings[i].MessageID < findings[j].MessageID
	})
	return findings, nil
}

func normalizeThreadScope(scope string) string {
	scope = strings.TrimSpace(strings.ToLower(scope))
	if scope == "" {
		return "all"
	}
	return scope
}

func loadBaselineModelContext(path string) (map[string]int, string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, "", fmt.Errorf("read baseline model limits: %w", err)
	}
	var baseline struct {
		Signals struct {
			AgentModeRoutes []struct {
				Name  string `json:"name"`
				Model string `json:"model"`
			} `json:"agent_mode_routes"`
			ModelLimits []struct {
				Name          string `json:"name"`
				ContextWindow int    `json:"context_window"`
			} `json:"model_limits"`
		} `json:"signals"`
	}
	if err := json.Unmarshal(raw, &baseline); err != nil {
		return nil, "", fmt.Errorf("parse baseline model limits: %w", err)
	}
	windows := map[string]int{}
	for _, limit := range baseline.Signals.ModelLimits {
		name := strings.TrimSpace(limit.Name)
		if name == "" || limit.ContextWindow <= 0 {
			continue
		}
		windows[name] = limit.ContextWindow
	}
	smartModel := ""
	for _, route := range baseline.Signals.AgentModeRoutes {
		if strings.EqualFold(strings.TrimSpace(route.Name), "smart") {
			smartModel = strings.TrimSpace(route.Model)
			break
		}
	}
	return windows, smartModel, nil
}

func scanThreadDir(dir string, since time.Time, allowMissing bool, threadScope, onlyThreadID string, modelContextWindows map[string]int, smartModel string) ([]driftFinding, error) {
	files, err := scanJSONFiles(dir, since, allowMissing)
	if err != nil {
		return nil, err
	}
	if threadScope != "all" && threadScope != "local-runtime" {
		return nil, fmt.Errorf("invalid thread-scope %q", threadScope)
	}
	var findings []driftFinding
	for _, file := range files {
		raw, err := os.ReadFile(file)
		if err != nil {
			return nil, err
		}
		var thread map[string]any
		if err := json.Unmarshal(raw, &thread); err != nil {
			return nil, fmt.Errorf("%s: %w", file, err)
		}
		threadID := firstNonEmptyString(thread["id"], nestedValue(thread, "data", "id"))
		if onlyThreadID != "" && threadID != onlyThreadID {
			continue
		}
		if threadScope == "local-runtime" && !threadLocalRuntimeMarked(thread) {
			continue
		}
		messages := arrayValue(thread["messages"])
		findings = append(findings, scanThreadCompactionDrift(file, threadID, thread, messages, since, modelContextWindows, smartModel)...)
		findings = append(findings, scanThreadCancelledStreamingBlockDrift(file, threadID, messages, since)...)
		findings = append(findings, scanThreadInferenceStateDrift(file, threadID, thread, messages)...)
		if !threadReviewMode(thread) {
			findings = append(findings, scanThreadDanglingToolUseDrift(file, threadID, messages, since)...)
		}
		findings = append(findings, scanThreadQueuedMessageDrift(file, threadID, thread, since)...)
		toolNames := map[string]string{}
		for _, rawMessage := range messages {
			for _, rawBlock := range arrayValue(mapValue(rawMessage)["content"]) {
				block := mapValue(rawBlock)
				if stringValue(block["type"]) != "tool_use" {
					continue
				}
				if id := firstNonEmptyString(block["id"], block["toolUseID"], block["toolUseId"], block["tool_use_id"], block["toolCallId"]); id != "" {
					toolNames[id] = stringValue(block["name"])
				}
			}
		}
		for _, rawMessage := range messages {
			message := mapValue(rawMessage)
			if threadMessageBeforeSince(message, since) {
				continue
			}
			messageID := firstNonEmptyString(message["messageId"], message["messageID"], message["id"])
			for _, rawBlock := range arrayValue(message["content"]) {
				block := mapValue(rawBlock)
				switch stringValue(block["type"]) {
				case "tool_use":
					toolID := firstNonEmptyString(block["id"], block["toolUseID"], block["toolUseId"], block["tool_use_id"], block["toolCallId"])
					toolName := stringValue(block["name"])
					if preview, ok := completedToolUseMalformedJSONFallback(message, block); ok {
						findings = append(findings, driftFinding{
							Source:    "thread",
							File:      file,
							ThreadID:  threadID,
							MessageID: messageID,
							CallID:    toolID,
							ToolName:  normalizeToolName(toolName),
							Detail:    "complete tool_use input contains malformed JSON fallback string: " + preview,
						})
					}
				case "tool_result":
					toolID := firstNonEmptyString(block["toolUseID"], block["toolUseId"], block["tool_use_id"], block["toolCallId"], block["id"])
					toolName := firstNonEmptyString(block["name"], toolNames[toolID])
					run := mapValue(block["run"])
					if status := nonTerminalToolRunStatus(run); status != "" && !toolProgressCompletionStatus(message) {
						findings = append(findings, driftFinding{
							Source:    "thread",
							File:      file,
							ThreadID:  threadID,
							MessageID: messageID,
							CallID:    toolID,
							ToolName:  normalizeToolName(toolName),
							Detail:    fmt.Sprintf("non-terminal tool_result status %q persisted without completionStatus=tool_progress", status),
						})
					}
					if bareTerminalDoneForPayloadRequiredTool(toolName, run) && !toolProgressCompletionStatus(message) {
						findings = append(findings, driftFinding{
							Source:    "thread",
							File:      file,
							ThreadID:  threadID,
							MessageID: messageID,
							CallID:    toolID,
							ToolName:  normalizeToolName(toolName),
							Detail:    `terminal "done" has no result/output payload`,
						})
					}
				}
			}
		}
	}
	return findings, nil
}

func threadLocalRuntimeMarked(thread map[string]any) bool {
	if len(thread) == 0 {
		return false
	}
	meta := mapValue(thread["meta"])
	if boolValue(meta["usesThreadActors"]) ||
		boolValue(meta["usesDtw"]) ||
		boolValue(meta["ampcodeConnectorLocalNeo"]) ||
		boolValue(meta["cliProxyAPILocalNeo"]) ||
		boolValue(meta["ampcodeLocalRuntime"]) ||
		strings.EqualFold(stringValue(meta["ampcodeConnectorMode"]), "local-neo") {
		return true
	}
	if data := mapValue(thread["data"]); len(data) > 0 {
		return threadLocalRuntimeMarked(data)
	}
	return false
}

func threadReviewMode(thread map[string]any) bool {
	mode := firstNonEmptyString(thread["agentMode"], nestedValue(thread, "settings", "agentMode"), nestedValue(thread, "data", "agentMode"), nestedValue(thread, "data", "settings", "agentMode"))
	return strings.EqualFold(strings.TrimSpace(mode), "review")
}

func scanThreadCancelledStreamingBlockDrift(file, threadID string, messages []any, since time.Time) []driftFinding {
	var findings []driftFinding
	for _, rawMessage := range messages {
		message := mapValue(rawMessage)
		if threadMessageBeforeSince(message, since) {
			continue
		}
		if !strings.EqualFold(stringValue(message["role"]), "assistant") {
			continue
		}
		if !strings.EqualFold(stringValue(mapValue(message["state"])["type"]), "cancelled") {
			continue
		}
		streamingTypes := make([]string, 0)
		nonTerminalToolTypes := make([]string, 0)
		for _, rawBlock := range arrayValue(message["content"]) {
			block := mapValue(rawBlock)
			blockType := strings.TrimSpace(stringValue(block["type"]))
			if blockType == "" {
				blockType = "unknown"
			}
			if strings.EqualFold(stringValue(block["blockState"]), "streaming") {
				streamingTypes = append(streamingTypes, blockType)
				continue
			}
			if cancelledMessageToolBlockStillLive(block) {
				nonTerminalToolTypes = append(nonTerminalToolTypes, blockType)
			}
		}
		messageID := firstNonEmptyString(message["messageId"], message["messageID"], message["id"])
		if len(streamingTypes) > 0 {
			findings = append(findings, driftFinding{
				Source:    "thread",
				File:      file,
				ThreadID:  threadID,
				MessageID: messageID,
				CallID:    strings.Join(streamingTypes, ","),
				ToolName:  "streaming_state",
				Detail:    "cancelled assistant message retains streaming child blockState",
			})
		}
		if len(nonTerminalToolTypes) > 0 {
			findings = append(findings, driftFinding{
				Source:    "thread",
				File:      file,
				ThreadID:  threadID,
				MessageID: messageID,
				CallID:    strings.Join(nonTerminalToolTypes, ","),
				ToolName:  "streaming_state",
				Detail:    "cancelled assistant message retains non-terminal tool child blockState",
			})
		}
	}
	return findings
}

func cancelledMessageToolBlockStillLive(block map[string]any) bool {
	blockType := strings.TrimSpace(stringValue(block["type"]))
	if blockType != "tool_use" && blockType != "server_tool_use" {
		return false
	}
	if boolValue(block["complete"]) {
		return false
	}
	return !terminalToolBlockState(stringValue(block["blockState"]))
}

func terminalToolBlockState(state string) bool {
	switch strings.TrimSpace(strings.ToLower(state)) {
	case "aborted", "cancelled", "complete", "done", "error", "failed", "rejected-by-user":
		return true
	default:
		return false
	}
}

func scanThreadInferenceStateDrift(file, threadID string, thread map[string]any, messages []any) []driftFinding {
	var findings []driftFinding
	messageByID := threadMessagesByID(messages)
	for _, key := range []string{"currentInference", "pendingInference"} {
		inference := mapValue(thread[key])
		if len(inference) == 0 {
			continue
		}
		messageID := firstNonEmptyString(inference["messageId"], inference["messageID"], inference["message_id"], inference["id"])
		agentMode := strings.TrimSpace(firstNonEmptyString(inference["agentMode"], inference["agent_mode"], inference["mode"]))
		if agentMode == "" {
			findings = append(findings, driftFinding{
				Source:    "thread",
				File:      file,
				ThreadID:  threadID,
				MessageID: messageID,
				CallID:    key,
				ToolName:  "inference_state",
				Detail:    key + " agentMode is missing or non-string",
			})
		}
		if key != "currentInference" || messageID == "" {
			continue
		}
		message := messageByID[messageID]
		if len(message) == 0 {
			continue
		}
		if strings.EqualFold(stringValue(message["role"]), "assistant") && strings.EqualFold(stringValue(mapValue(message["state"])["type"]), "cancelled") {
			findings = append(findings, driftFinding{
				Source:    "thread",
				File:      file,
				ThreadID:  threadID,
				MessageID: messageID,
				CallID:    key,
				ToolName:  "inference_state",
				Detail:    key + " references cancelled assistant message",
			})
		}
	}
	if boolValue(thread["compacting"]) && len(mapValue(thread["currentInference"])) == 0 && len(mapValue(thread["pendingInference"])) == 0 {
		findings = append(findings, driftFinding{
			Source:   "thread",
			File:     file,
			ThreadID: threadID,
			CallID:   "compacting",
			ToolName: "inference_state",
			Detail:   "thread compacting persisted without current or pending inference",
		})
	}
	return findings
}

func threadMessagesByID(messages []any) map[string]map[string]any {
	out := map[string]map[string]any{}
	for _, rawMessage := range messages {
		message := mapValue(rawMessage)
		messageID := firstNonEmptyString(message["messageId"], message["messageID"], message["message_id"], message["id"])
		if messageID == "" {
			continue
		}
		out[messageID] = message
	}
	return out
}

type pendingThreadToolUse struct {
	messageID    string
	parentToolID string
	toolID       string
	toolName     string
}

func scanThreadDanglingToolUseDrift(file, threadID string, messages []any, since time.Time) []driftFinding {
	pending := map[string]pendingThreadToolUse{}
	var findings []driftFinding
	for _, rawMessage := range messages {
		message := mapValue(rawMessage)
		role := strings.ToLower(strings.TrimSpace(stringValue(message["role"])))
		if role == "user" {
			for _, rawBlock := range arrayValue(message["content"]) {
				block := mapValue(rawBlock)
				if stringValue(block["type"]) != "tool_result" {
					continue
				}
				toolID := firstNonEmptyString(block["toolUseID"], block["toolUseId"], block["tool_use_id"], block["toolCallId"], block["id"])
				if toolID != "" {
					delete(pending, toolID)
				}
			}
			if threadUserMessageHasNonToolResultContent(message) {
				parentToolID := threadMessageParentToolUseID(message)
				findings = appendDanglingToolUseFindings(findings, file, threadID, pending, message, "later user message", since, parentToolID)
				pending = pendingAfterDanglingBoundary(pending, parentToolID)
			}
			continue
		}
		if role != "assistant" {
			continue
		}
		parentToolID := threadMessageParentToolUseID(message)
		findings = appendDanglingToolUseFindings(findings, file, threadID, pending, message, "later assistant message", since, parentToolID)
		pending = pendingAfterDanglingBoundary(pending, parentToolID)
		for _, rawBlock := range arrayValue(message["content"]) {
			block := mapValue(rawBlock)
			if !completedToolUseBlock(message, block) {
				continue
			}
			toolID := firstNonEmptyString(block["id"], block["toolUseID"], block["toolUseId"], block["tool_use_id"], block["toolCallId"])
			if toolID == "" {
				continue
			}
			pending[toolID] = pendingThreadToolUse{
				messageID:    firstNonEmptyString(message["messageId"], message["messageID"], message["id"]),
				parentToolID: parentToolID,
				toolID:       toolID,
				toolName:     stringValue(block["name"]),
			}
		}
	}
	return findings
}

func appendDanglingToolUseFindings(findings []driftFinding, file, threadID string, pending map[string]pendingThreadToolUse, triggerMessage map[string]any, trigger string, since time.Time, activeParentToolID string) []driftFinding {
	if len(pending) == 0 || threadMessageBeforeSince(triggerMessage, since) {
		return findings
	}
	for _, item := range pending {
		if activeParentToolID != "" && item.toolID == activeParentToolID {
			continue
		}
		if activeParentToolID != "" && item.parentToolID != activeParentToolID {
			continue
		}
		findings = append(findings, driftFinding{
			Source:    "thread",
			File:      file,
			ThreadID:  threadID,
			MessageID: item.messageID,
			CallID:    item.toolID,
			ToolName:  normalizeToolName(item.toolName),
			Detail:    "complete tool_use has no matching tool_result before " + trigger,
		})
	}
	return findings
}

func pendingAfterDanglingBoundary(pending map[string]pendingThreadToolUse, activeParentToolID string) map[string]pendingThreadToolUse {
	if activeParentToolID == "" {
		return map[string]pendingThreadToolUse{}
	}
	out := map[string]pendingThreadToolUse{}
	for toolID, item := range pending {
		if activeParentToolID != "" && item.toolID == activeParentToolID {
			out[toolID] = item
			continue
		}
		if item.parentToolID != activeParentToolID {
			out[toolID] = item
		}
	}
	return out
}

func threadMessageParentToolUseID(message map[string]any) string {
	return firstNonEmptyString(message["parentToolUseId"], message["parentToolUseID"], message["parent_tool_use_id"], message["parentToolCallId"])
}

func threadUserMessageHasNonToolResultContent(message map[string]any) bool {
	for _, rawBlock := range arrayValue(message["content"]) {
		block := mapValue(rawBlock)
		if stringValue(block["type"]) != "tool_result" {
			return true
		}
	}
	return false
}

func scanThreadQueuedMessageDrift(file, threadID string, thread map[string]any, since time.Time) []driftFinding {
	items := firstArray(thread["queuedMessages"], thread["queued_messages"])
	if len(items) == 0 {
		return nil
	}
	var findings []driftFinding
	for index, rawItem := range items {
		item := mapValue(rawItem)
		message := mapValue(item["queuedMessage"])
		if len(message) == 0 {
			findings = append(findings, queueDriftFinding(file, threadID, index, item, "queuedMessages item is not wrapped in queuedMessage"))
			continue
		}
		if queuedMessageBeforeSince(message, since) {
			continue
		}
		if firstNonEmptyString(item["id"]) == "" {
			findings = append(findings, queueDriftFinding(file, threadID, index, item, "queuedMessages item id is missing or non-string"))
		}
		if !strings.EqualFold(strings.TrimSpace(stringValue(message["role"])), "user") {
			findings = append(findings, queueDriftFinding(file, threadID, index, item, "queuedMessage role is not user"))
		}
		if firstNonEmptyString(message["messageId"], message["messageID"]) == "" {
			findings = append(findings, queueDriftFinding(file, threadID, index, item, "queuedMessage.messageId is missing or non-string"))
		}
		if arrayValue(message["content"]) == nil {
			findings = append(findings, queueDriftFinding(file, threadID, index, item, "queuedMessage.content is missing or non-array"))
		}
		if steer, exists := item["steer"]; exists {
			if _, ok := steer.(bool); !ok {
				findings = append(findings, queueDriftFinding(file, threadID, index, item, "queuedMessages steer is present but not boolean"))
			}
		}
	}
	return findings
}

func queuedMessageBeforeSince(message map[string]any, since time.Time) bool {
	if since.IsZero() {
		return false
	}
	if createdAt, ok := parseTimeValue(message["createdAt"], message["timestamp"], message["created_at"]); ok {
		return createdAt.Before(since)
	}
	return false
}

func queueDriftFinding(file, threadID string, index int, item map[string]any, detail string) driftFinding {
	message := mapValue(item["queuedMessage"])
	return driftFinding{
		Source:    "thread",
		File:      file,
		ThreadID:  threadID,
		MessageID: firstNonEmptyString(message["messageId"], message["messageID"], item["queuedMessageId"], item["queuedMessageID"]),
		CallID:    firstNonEmptyString(item["id"], item["queuedMessageId"], item["queuedMessageID"], fmt.Sprintf("queuedMessages[%d]", index)),
		ToolName:  "queued_message",
		Detail:    detail,
	}
}

func scanThreadCompactionDrift(file, threadID string, thread map[string]any, messages []any, since time.Time, modelContextWindows map[string]int, smartModel string) []driftFinding {
	if !smartModeThread(thread, messages) || threadHasCustomCompactionThreshold(thread) {
		return nil
	}
	var findings []driftFinding
	compactionRecords := firstArray(thread["compactionRecords"], thread["compaction_records"])
	compactionTimes := make([]time.Time, 0, len(compactionRecords))
	for _, rawRecord := range compactionRecords {
		record := mapValue(rawRecord)
		recordCreatedAt, ok := parseTimeValue(record["createdAt"], record["created_at"])
		if !ok {
			continue
		}
		compactionTimes = append(compactionTimes, recordCreatedAt)
		if !since.IsZero() && recordCreatedAt.Before(since) {
			continue
		}
		message, usage := latestAssistantUsageBefore(messages, recordCreatedAt)
		if len(usage) == 0 {
			continue
		}
		observedTokens := compactionUsageTokens(usage)
		thresholdTokens := smartCompactionObservedThresholdTokens(usage, modelContextWindows, smartModel)
		if observedTokens <= 0 || thresholdTokens <= 0 || float64(observedTokens) >= thresholdTokens {
			continue
		}
		findings = append(findings, driftFinding{
			Source:    "thread",
			File:      file,
			ThreadID:  threadID,
			MessageID: firstNonEmptyString(message["messageId"], message["messageID"], message["id"]),
			CallID:    firstNonEmptyString(record["cutMessageId"], record["cut_message_id"]),
			ToolName:  "compaction",
			Detail:    fmt.Sprintf("smart compaction observed %d tokens below 75%% threshold %.0f at %s", observedTokens, thresholdTokens, recordCreatedAt.Format(time.RFC3339Nano)),
		})
	}
	for _, rawMessage := range messages {
		message := mapValue(rawMessage)
		if !strings.EqualFold(stringValue(message["role"]), "assistant") {
			continue
		}
		messageCreatedAt, ok := parseTimeValue(message["createdAt"], message["created_at"], message["timestamp"])
		if !ok {
			continue
		}
		if !since.IsZero() && messageCreatedAt.Before(since) {
			continue
		}
		usage := mapValue(message["usage"])
		if len(usage) == 0 {
			continue
		}
		observedTokens := compactionUsageTokens(usage)
		thresholdTokens := smartCompactionObservedThresholdTokens(usage, modelContextWindows, smartModel)
		if observedTokens <= 0 || thresholdTokens <= 0 || float64(observedTokens) < thresholdTokens {
			continue
		}
		if hasCompactionAtOrAfter(compactionTimes, messageCreatedAt) {
			continue
		}
		findings = append(findings, driftFinding{
			Source:    "thread",
			File:      file,
			ThreadID:  threadID,
			MessageID: firstNonEmptyString(message["messageId"], message["messageID"], message["id"]),
			ToolName:  "compaction",
			Detail:    fmt.Sprintf("smart usage observed %d tokens at or above 75%% threshold %.0f with no later compaction record", observedTokens, thresholdTokens),
		})
	}
	return findings
}

func hasCompactionAtOrAfter(compactionTimes []time.Time, messageCreatedAt time.Time) bool {
	for _, compactedAt := range compactionTimes {
		if !compactedAt.Before(messageCreatedAt) {
			return true
		}
	}
	return false
}

func smartModeThread(thread map[string]any, messages []any) bool {
	mode := strings.ToLower(strings.TrimSpace(firstNonEmptyString(thread["agentMode"], thread["agent_mode"], nestedValue(thread, "settings", "agentMode"), nestedValue(thread, "settings", "agent_mode"), nestedValue(thread, "data", "agentMode"))))
	if mode == "" {
		for _, rawMessage := range messages {
			message := mapValue(rawMessage)
			if strings.EqualFold(stringValue(message["role"]), "user") {
				if candidate := strings.ToLower(strings.TrimSpace(firstNonEmptyString(message["agentMode"], message["agent_mode"]))); candidate != "" {
					mode = candidate
					break
				}
			}
		}
	}
	return mode == "" || mode == "smart"
}

func threadHasCustomCompactionThreshold(thread map[string]any) bool {
	settings := mapValue(thread["settings"])
	if len(settings) == 0 {
		settings = mapValue(nestedValue(thread, "data", "settings"))
	}
	if len(settings) == 0 {
		return false
	}
	if _, ok := settings["internal.compactionThresholdPercent"]; ok {
		return true
	}
	if _, ok := settings["compactionControl.contextTokenThreshold"]; ok {
		return true
	}
	if _, ok := settings["compaction.contextTokenThreshold"]; ok {
		return true
	}
	control := mapValue(settings["compactionControl"])
	if len(control) == 0 {
		control = mapValue(settings["compaction_control"])
	}
	_, ok := control["contextTokenThreshold"]
	return ok
}

func latestAssistantUsageBefore(messages []any, before time.Time) (map[string]any, map[string]any) {
	var latestMessage map[string]any
	var latestUsage map[string]any
	var latestCreatedAt time.Time
	for _, rawMessage := range messages {
		message := mapValue(rawMessage)
		if !strings.EqualFold(stringValue(message["role"]), "assistant") {
			continue
		}
		createdAt, ok := parseTimeValue(message["createdAt"], message["created_at"], message["timestamp"])
		if !ok || !createdAt.Before(before) {
			continue
		}
		usage := mapValue(message["usage"])
		if len(usage) == 0 {
			continue
		}
		if latestUsage == nil || createdAt.After(latestCreatedAt) {
			latestMessage = message
			latestUsage = usage
			latestCreatedAt = createdAt
		}
	}
	return latestMessage, latestUsage
}

func compactionUsageTokens(usage map[string]any) int {
	totalInput := numberValue(usage["totalInputTokens"], usage["total_input_tokens"])
	output := numberValue(usage["outputTokens"], usage["output_tokens"], usage["completion_tokens"], usage["candidatesTokenCount"])
	if _, ok := usage["totalInputTokens"]; ok {
		return totalInput + output
	}
	if _, ok := usage["total_input_tokens"]; ok {
		return totalInput + output
	}
	input := numberValue(usage["inputTokens"], usage["input_tokens"], usage["prompt_tokens"], usage["promptTokenCount"])
	cacheCreation := numberValue(usage["cacheCreationInputTokens"], usage["cache_creation_input_tokens"])
	cacheRead := numberValue(usage["cacheReadInputTokens"], usage["cache_read_input_tokens"], usage["cachedContentTokenCount"])
	if input > 0 || cacheCreation > 0 || cacheRead > 0 {
		return input + cacheCreation + cacheRead + output
	}
	return numberValue(usage["total_tokens"], usage["totalTokenCount"])
}

func smartCompactionObservedThresholdTokens(usage map[string]any, modelContextWindows map[string]int, smartModel string) float64 {
	model := strings.TrimSpace(stringValue(usage["model"]))
	contextWindow := 0
	if smartUsageModelIsBinaryAnthropic(model) {
		contextWindow = modelContextWindows[model]
	}
	if contextWindow <= 0 && strings.TrimSpace(smartModel) != "" {
		contextWindow = modelContextWindows[strings.TrimSpace(smartModel)]
	}
	if contextWindow <= 0 {
		contextWindow = modelContextWindows[model]
	}
	if contextWindow <= 0 {
		contextWindow = numberValue(usage["contextWindow"], usage["context_window"])
	}
	if contextWindow <= 0 {
		contextWindow = numberValue(usage["maxInputTokens"], usage["max_input_tokens"])
	}
	if contextWindow <= 0 {
		return 0
	}
	return float64(contextWindow) * 75 / 100
}

func smartUsageModelIsBinaryAnthropic(model string) bool {
	model = strings.ToLower(strings.TrimSpace(model))
	return strings.HasPrefix(model, "claude-opus-")
}

func parseTimeValue(values ...any) (time.Time, bool) {
	for _, value := range values {
		text := strings.TrimSpace(stringValue(value))
		if text == "" {
			continue
		}
		if parsed, err := time.Parse(time.RFC3339Nano, text); err == nil {
			return parsed, true
		}
		if parsed, err := time.Parse(time.RFC3339, text); err == nil {
			return parsed, true
		}
	}
	return time.Time{}, false
}

func threadMessageBeforeSince(message map[string]any, since time.Time) bool {
	if since.IsZero() {
		return false
	}
	if createdAt, ok := parseTimeValue(message["createdAt"], message["timestamp"], message["created_at"]); ok {
		return createdAt.Before(since)
	}
	return false
}

func scanCaptureDir(dir string, since time.Time, allowMissing bool, onlyThreadID string, requireCaptureSummary bool) ([]driftFinding, error) {
	files, err := scanJSONFiles(dir, since, allowMissing)
	if err != nil {
		return nil, err
	}
	var findings []driftFinding
	for _, file := range files {
		raw, err := os.ReadFile(file)
		if err != nil {
			return nil, err
		}
		var capture map[string]any
		if err := json.Unmarshal(raw, &capture); err != nil {
			return nil, fmt.Errorf("%s: %w", file, err)
		}
		threadID := captureThreadID(capture)
		if onlyThreadID != "" && threadID != onlyThreadID {
			continue
		}
		body := mapValue(capture["body"])
		if requireCaptureSummary {
			findings = append(findings, scanProviderCaptureSummaryDrift(file, threadID, capture, body)...)
		}
		inputs := firstArray(body["input"], body["messages"])
		findings = append(findings, scanProviderAnthropicImageDrift(file, threadID, capture, inputs)...)
		findings = append(findings, scanProviderDanglingToolUseDrift(file, threadID, inputs)...)
		toolNames := map[string]string{}
		for _, rawItem := range inputs {
			item := mapValue(rawItem)
			if stringValue(item["type"]) != "function_call" {
				for _, rawBlock := range arrayValue(item["content"]) {
					block := mapValue(rawBlock)
					if stringValue(block["type"]) != "tool_use" {
						continue
					}
					callID := firstNonEmptyString(block["id"], block["tool_use_id"], block["toolUseID"], block["toolUseId"], block["toolCallId"])
					if callID != "" {
						toolNames[callID] = stringValue(block["name"])
					}
					if preview, ok := providerToolUseMalformedJSONFallback(block); ok {
						findings = append(findings, driftFinding{
							Source:   "provider-capture",
							File:     file,
							ThreadID: threadID,
							CallID:   callID,
							ToolName: normalizeToolName(stringValue(block["name"])),
							Detail:   "model input contains malformed tool_use input: " + preview,
						})
					}
				}
				continue
			}
			callID := firstNonEmptyString(item["call_id"], item["callId"], item["id"])
			if callID != "" {
				toolNames[callID] = stringValue(item["name"])
			}
			if preview, ok := functionCallMalformedJSONFallback(item["arguments"]); ok {
				findings = append(findings, driftFinding{
					Source:   "provider-capture",
					File:     file,
					ThreadID: threadID,
					CallID:   callID,
					ToolName: normalizeToolName(stringValue(item["name"])),
					Detail:   "model input contains malformed function_call arguments: " + preview,
				})
			}
		}
		for _, rawItem := range inputs {
			item := mapValue(rawItem)
			switch stringValue(item["type"]) {
			case "function_call_output":
				callID := firstNonEmptyString(item["call_id"], item["callId"], item["id"])
				toolName := toolNames[callID]
				run := decodeRunOutput(item["output"])
				findings = appendProviderToolRunFindings(findings, file, threadID, callID, toolName, run)
			default:
				for _, rawBlock := range arrayValue(item["content"]) {
					block := mapValue(rawBlock)
					if stringValue(block["type"]) != "tool_result" {
						continue
					}
					callID := firstNonEmptyString(block["tool_use_id"], block["toolUseID"], block["toolUseId"], block["toolCallId"], block["id"])
					toolName := firstNonEmptyString(block["name"], toolNames[callID])
					run := decodeToolResultRun(block)
					findings = appendProviderToolRunFindings(findings, file, threadID, callID, toolName, run)
				}
			}
		}
	}
	return findings, nil
}

func scanProviderCaptureSummaryDrift(file, threadID string, capture, body map[string]any) []driftFinding {
	expected := providerCaptureSummaryForScan(body)
	if len(expected) == 0 {
		return nil
	}
	summary := mapValue(capture["summary"])
	if len(summary) == 0 {
		return []driftFinding{{
			Source:   "provider-capture",
			File:     file,
			ThreadID: threadID,
			ToolName: "provider_request",
			Detail:   "provider capture missing request summary",
		}}
	}
	var findings []driftFinding
	for _, key := range []string{"bodySHA256", "systemSHA256"} {
		want := stringValue(expected[key])
		if want == "" {
			continue
		}
		if got := stringValue(summary[key]); got != want {
			findings = append(findings, providerCaptureSummaryFinding(file, threadID, fmt.Sprintf("provider capture summary %s mismatch", key)))
		}
	}
	for _, key := range []string{"inputCount", "systemBytes"} {
		want, ok := expected[key]
		if !ok {
			continue
		}
		if numberValue(summary[key]) != numberValue(want) {
			findings = append(findings, providerCaptureSummaryFinding(file, threadID, fmt.Sprintf("provider capture summary %s mismatch", key)))
		}
	}
	if wantTools := stringSliceValue(expected["toolNames"]); len(wantTools) > 0 {
		gotTools := stringSliceValue(summary["toolNames"])
		if strings.Join(gotTools, "\x00") != strings.Join(wantTools, "\x00") {
			findings = append(findings, providerCaptureSummaryFinding(file, threadID, "provider capture summary toolNames mismatch"))
		}
	}
	return findings
}

func providerCaptureSummaryFinding(file, threadID, detail string) driftFinding {
	return driftFinding{
		Source:   "provider-capture",
		File:     file,
		ThreadID: threadID,
		ToolName: "provider_request",
		Detail:   detail,
	}
}

func providerCaptureSummaryForScan(body map[string]any) map[string]any {
	if len(body) == 0 {
		return nil
	}
	summary := map[string]any{}
	if hash := providerCaptureHashForScan(body); hash != "" {
		summary["bodySHA256"] = hash
	}
	if count := providerCaptureInputCountForScan(body); count > 0 {
		summary["inputCount"] = count
	}
	if system := providerCaptureSystemTextForScan(body); system != "" {
		summary["systemSHA256"] = providerCaptureHashForScan(system)
		summary["systemBytes"] = len([]byte(system))
	}
	if tools := providerCaptureToolNamesForScan(body); len(tools) > 0 {
		summary["toolNames"] = tools
	}
	return summary
}

func providerCaptureHashForScan(value any) string {
	raw, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(raw)
	return fmt.Sprintf("%x", sum[:])
}

func providerCaptureInputCountForScan(body map[string]any) int {
	for _, key := range []string{"input", "messages", "contents"} {
		if items := arrayValue(body[key]); len(items) > 0 {
			return len(items)
		}
	}
	return 0
}

func providerCaptureSystemTextForScan(body map[string]any) string {
	parts := make([]string, 0, 2)
	if text := providerCaptureTextForScan(body["instructions"]); text != "" {
		parts = append(parts, text)
	}
	if text := providerCaptureTextForScan(body["system"]); text != "" {
		parts = append(parts, text)
	}
	for _, raw := range firstArray(body["input"], body["messages"]) {
		item := mapValue(raw)
		switch strings.ToLower(strings.TrimSpace(stringValue(item["role"]))) {
		case "system", "developer":
			if text := providerCaptureTextForScan(item["content"]); text != "" {
				parts = append(parts, text)
			}
		}
	}
	return strings.Join(parts, "\n")
}

func providerCaptureTextForScan(value any) string {
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v)
	case []any:
		parts := make([]string, 0, len(v))
		for _, item := range v {
			if text := providerCaptureTextForScan(item); text != "" {
				parts = append(parts, text)
			}
		}
		return strings.Join(parts, "\n")
	case map[string]any:
		parts := make([]string, 0, 2)
		for _, key := range []string{"text", "content", "input"} {
			if text := providerCaptureTextForScan(v[key]); text != "" {
				parts = append(parts, text)
			}
		}
		return strings.Join(parts, "\n")
	default:
		return ""
	}
}

func providerCaptureToolNamesForScan(body map[string]any) []string {
	seen := map[string]struct{}{}
	var add func(any)
	add = func(value any) {
		switch v := value.(type) {
		case []any:
			for _, item := range v {
				add(item)
			}
		case map[string]any:
			if name := strings.TrimSpace(stringValue(v["name"])); name != "" {
				seen[name] = struct{}{}
			}
			if fn := mapValue(v["function"]); len(fn) > 0 {
				add(fn)
			}
			if declarations := arrayValue(v["functionDeclarations"]); len(declarations) > 0 {
				add(declarations)
			}
		}
	}
	add(body["tools"])
	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

type pendingProviderToolUse struct {
	callID   string
	toolName string
}

func scanProviderDanglingToolUseDrift(file, threadID string, inputs []any) []driftFinding {
	pending := map[string]pendingProviderToolUse{}
	var findings []driftFinding
	for _, rawItem := range inputs {
		item := mapValue(rawItem)
		itemType := stringValue(item["type"])
		switch itemType {
		case "function_call":
			if _, malformed := functionCallMalformedJSONFallback(item["arguments"]); malformed {
				continue
			}
			callID := firstNonEmptyString(item["call_id"], item["callId"], item["id"])
			if callID != "" {
				pending[callID] = pendingProviderToolUse{callID: callID, toolName: stringValue(item["name"])}
			}
			continue
		case "function_call_output":
			callID := firstNonEmptyString(item["call_id"], item["callId"], item["id"])
			if callID != "" {
				delete(pending, callID)
			}
			continue
		}

		role := strings.ToLower(strings.TrimSpace(stringValue(item["role"])))
		switch role {
		case "user":
			for _, rawBlock := range arrayValue(item["content"]) {
				block := mapValue(rawBlock)
				if stringValue(block["type"]) != "tool_result" {
					continue
				}
				callID := firstNonEmptyString(block["tool_use_id"], block["toolUseID"], block["toolUseId"], block["toolCallId"], block["id"])
				if callID != "" {
					delete(pending, callID)
				}
			}
			if threadUserMessageHasNonToolResultContent(item) {
				findings = appendProviderDanglingToolUseFindings(findings, file, threadID, pending, "later user message")
				pending = map[string]pendingProviderToolUse{}
			}
		case "assistant":
			findings = appendProviderDanglingToolUseFindings(findings, file, threadID, pending, "later assistant message")
			pending = map[string]pendingProviderToolUse{}
			for _, rawBlock := range arrayValue(item["content"]) {
				block := mapValue(rawBlock)
				if stringValue(block["type"]) != "tool_use" {
					continue
				}
				if hasCustomRawInputMetadata(block) {
					continue
				}
				if _, malformed := providerToolUseMalformedJSONFallback(block); malformed {
					continue
				}
				callID := firstNonEmptyString(block["id"], block["tool_use_id"], block["toolUseID"], block["toolUseId"], block["toolCallId"])
				if callID != "" {
					pending[callID] = pendingProviderToolUse{callID: callID, toolName: stringValue(block["name"])}
				}
			}
		default:
			if len(arrayValue(item["content"])) == 0 {
				findings = appendProviderDanglingToolUseFindings(findings, file, threadID, pending, "later provider input item")
				pending = map[string]pendingProviderToolUse{}
			}
		}
	}
	return appendProviderDanglingToolUseFindings(findings, file, threadID, pending, "end of provider input")
}

func appendProviderDanglingToolUseFindings(findings []driftFinding, file, threadID string, pending map[string]pendingProviderToolUse, trigger string) []driftFinding {
	for _, item := range pending {
		findings = append(findings, driftFinding{
			Source:   "provider-capture",
			File:     file,
			ThreadID: threadID,
			CallID:   item.callID,
			ToolName: normalizeToolName(item.toolName),
			Detail:   "model input contains tool call with no matching tool result before " + trigger,
		})
	}
	return findings
}

func captureThreadID(capture map[string]any) string {
	return firstNonEmptyString(capture["threadID"], capture["threadId"], capture["thread_id"], nestedValue(capture, "metadata", "threadID"), nestedValue(capture, "metadata", "threadId"))
}

func scanProviderAnthropicImageDrift(file, threadID string, capture map[string]any, inputs []any) []driftFinding {
	provider := strings.ToLower(strings.TrimSpace(firstNonEmptyString(capture["provider"], nestedValue(capture, "metadata", "provider"))))
	if provider != "" && !strings.Contains(provider, "anthropic") {
		return nil
	}
	var findings []driftFinding
	for itemIndex, rawItem := range inputs {
		item := mapValue(rawItem)
		for blockIndex, rawBlock := range arrayValue(item["content"]) {
			block := mapValue(rawBlock)
			if stringValue(block["type"]) != "image" {
				continue
			}
			source := mapValue(block["source"])
			if stringValue(source["type"]) != "base64" || firstNonEmptyString(source["data"], source["base64"]) == "" {
				continue
			}
			if strings.TrimSpace(stringValue(source["media_type"])) != "" {
				continue
			}
			findings = append(findings, driftFinding{
				Source:   "provider-capture",
				File:     file,
				ThreadID: threadID,
				CallID:   fmt.Sprintf("messages[%d].content[%d]", itemIndex, blockIndex),
				ToolName: "image_attachment",
				Detail:   "anthropic base64 image source missing media_type",
			})
		}
	}
	return findings
}

func appendProviderToolRunFindings(findings []driftFinding, file, threadID, callID, toolName string, run map[string]any) []driftFinding {
	if status := nonTerminalToolRunStatus(run); status != "" {
		findings = append(findings, driftFinding{
			Source:   "provider-capture",
			File:     file,
			ThreadID: threadID,
			CallID:   callID,
			ToolName: normalizeToolName(toolName),
			Detail:   fmt.Sprintf("model input contains non-terminal tool_result status %q", status),
		})
	}
	if bareTerminalDoneForPayloadRequiredTool(toolName, run) {
		findings = append(findings, driftFinding{
			Source:   "provider-capture",
			File:     file,
			ThreadID: threadID,
			CallID:   callID,
			ToolName: normalizeToolName(toolName),
			Detail:   `model input contains terminal "done" with no result/output payload`,
		})
	}
	return findings
}

func completedToolUseMalformedJSONFallback(message, block map[string]any) (string, bool) {
	if !completedToolUseBlock(message, block) || hasCustomRawInputMetadata(block) {
		return "", false
	}
	return malformedJSONFallbackInput(mapValue(block["input"]))
}

func providerToolUseMalformedJSONFallback(block map[string]any) (string, bool) {
	if stringValue(block["type"]) != "tool_use" || hasCustomRawInputMetadata(block) {
		return "", false
	}
	if _, exists := block["inputPartialJSON"]; exists {
		return "", false
	}
	if _, exists := block["inputPartialJSONDelta"]; exists {
		return "", false
	}
	return malformedJSONFallbackInput(mapValue(block["input"]))
}

func completedToolUseBlock(message, block map[string]any) bool {
	if stringValue(block["type"]) != "tool_use" {
		return false
	}
	if _, exists := block["inputPartialJSON"]; exists {
		return false
	}
	if _, exists := block["inputPartialJSONDelta"]; exists {
		return false
	}
	if value, exists := block["complete"]; exists {
		return boolValue(value)
	}
	if strings.EqualFold(stringValue(block["blockState"]), "complete") {
		return true
	}
	state := mapValue(message["state"])
	return strings.EqualFold(stringValue(state["type"]), "complete") && strings.EqualFold(stringValue(state["stopReason"]), "tool_use")
}

func hasCustomRawInputMetadata(block map[string]any) bool {
	metadata := mapValue(block["metadata"])
	if len(metadata) == 0 {
		return false
	}
	return len(mapValue(metadata["openAICustomTool"])) > 0
}

func functionCallMalformedJSONFallback(arguments any) (string, bool) {
	if input := mapValue(arguments); len(input) > 0 {
		return malformedJSONFallbackInput(input)
	}
	text := strings.TrimSpace(stringValue(arguments))
	if text == "" || !looksLikeJSON(text) {
		return "", false
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(text), &decoded); err != nil {
		return clipFindingText(text), true
	}
	return malformedJSONFallbackInput(decoded)
}

func malformedJSONFallbackInput(input map[string]any) (string, bool) {
	if len(input) != 1 {
		return "", false
	}
	text := strings.TrimSpace(stringValue(input["input"]))
	if text == "" || !looksLikeJSON(text) {
		return "", false
	}
	var decoded any
	if err := json.Unmarshal([]byte(text), &decoded); err == nil {
		return "", false
	}
	return clipFindingText(text), true
}

func looksLikeJSON(text string) bool {
	text = strings.TrimSpace(text)
	return strings.HasPrefix(text, "{") || strings.HasPrefix(text, "[")
}

func clipFindingText(text string) string {
	text = strings.TrimSpace(text)
	const limit = 120
	if len(text) <= limit {
		return text
	}
	return text[:limit] + "..."
}

func scanJSONFiles(dir string, since time.Time, allowMissing bool) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if allowMissing && errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var files []string
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		if !since.IsZero() {
			info, err := entry.Info()
			if err != nil {
				return nil, err
			}
			if info.ModTime().Before(since) {
				continue
			}
		}
		files = append(files, path)
	}
	sort.Strings(files)
	return files, nil
}

func decodeRunOutput(value any) map[string]any {
	if run := mapValue(value); len(run) > 0 {
		return run
	}
	text := stringValue(value)
	if strings.TrimSpace(text) == "" {
		return nil
	}
	var decoded any
	if err := json.Unmarshal([]byte(text), &decoded); err != nil {
		return nil
	}
	return mapValue(decoded)
}

func decodeToolResultRun(block map[string]any) map[string]any {
	if run := mapValue(block["run"]); len(run) > 0 {
		return run
	}
	if run := decodeRunOutput(block["content"]); len(run) > 0 {
		return run
	}
	return decodeRunOutput(block["output"])
}

func nonTerminalToolRunStatus(run map[string]any) string {
	status := strings.ToLower(strings.TrimSpace(stringValue(run["status"])))
	switch status {
	case "in-progress", "queued", "blocked-on-user", "cancellation-requested":
		return status
	default:
		return ""
	}
}

func toolProgressCompletionStatus(message map[string]any) bool {
	status := firstNonEmptyString(message["completionStatus"], message["completion_status"])
	return strings.EqualFold(strings.TrimSpace(status), "tool_progress")
}

func bareTerminalDoneForPayloadRequiredTool(toolName string, run map[string]any) bool {
	if _, ok := payloadRequiredTools[normalizeToolName(toolName)]; !ok {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(stringValue(run["status"])), "done") {
		return false
	}
	return !toolRunHasDonePayload(run)
}

func toolRunHasDonePayload(run map[string]any) bool {
	for _, key := range []string{"result", "output", "displayMessage", "message", "text"} {
		value, exists := run[key]
		if !exists || value == nil {
			continue
		}
		if s, ok := value.(string); ok && strings.TrimSpace(s) == "" {
			continue
		}
		if m, ok := value.(map[string]any); ok && len(m) == 0 {
			continue
		}
		if items, ok := value.([]any); ok && len(items) == 0 {
			continue
		}
		return true
	}
	if value := firstNonEmptyString(nestedValue(run, "error", "message"), run["error"], run["reason"]); value != "" {
		return true
	}
	return false
}

func normalizeToolName(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

func mapValue(value any) map[string]any {
	if value == nil {
		return nil
	}
	if m, ok := value.(map[string]any); ok {
		return m
	}
	return nil
}

func arrayValue(value any) []any {
	if value == nil {
		return nil
	}
	if items, ok := value.([]any); ok {
		return items
	}
	return nil
}

func firstArray(values ...any) []any {
	for _, value := range values {
		if items := arrayValue(value); items != nil {
			return items
		}
	}
	return nil
}

func stringValue(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case fmt.Stringer:
		return v.String()
	default:
		return ""
	}
}

func stringSliceValue(value any) []string {
	items := arrayValue(value)
	out := make([]string, 0, len(items))
	for _, item := range items {
		if text := strings.TrimSpace(stringValue(item)); text != "" {
			out = append(out, text)
		}
	}
	return out
}

func boolValue(value any) bool {
	switch v := value.(type) {
	case bool:
		return v
	case string:
		return strings.EqualFold(strings.TrimSpace(v), "true")
	default:
		return false
	}
}

func firstNonEmptyString(values ...any) string {
	for _, value := range values {
		if text := strings.TrimSpace(stringValue(value)); text != "" {
			return text
		}
	}
	return ""
}

func numberValue(values ...any) int {
	for _, value := range values {
		switch v := value.(type) {
		case float64:
			return int(v)
		case float32:
			return int(v)
		case int:
			return v
		case int64:
			return int(v)
		case int32:
			return int(v)
		case json.Number:
			if i, err := v.Int64(); err == nil {
				return int(i)
			}
			if f, err := v.Float64(); err == nil {
				return int(f)
			}
		case string:
			if strings.TrimSpace(v) == "" {
				continue
			}
			if parsed, err := json.Number(strings.TrimSpace(v)).Int64(); err == nil {
				return int(parsed)
			}
			if parsed, err := json.Number(strings.TrimSpace(v)).Float64(); err == nil {
				return int(parsed)
			}
		}
	}
	return 0
}

func nestedValue(root map[string]any, path ...string) any {
	var current any = root
	for _, key := range path {
		currentMap := mapValue(current)
		if len(currentMap) == 0 {
			return nil
		}
		current = currentMap[key]
	}
	return current
}
