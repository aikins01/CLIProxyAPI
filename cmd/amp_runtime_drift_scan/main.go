package main

import (
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
	threadDir           string
	captureDir          string
	baselinePath        string
	modelContextWindows map[string]int
	smartModel          string
	since               time.Time
	allowMissing        bool
	jsonOutput          bool
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
	sinceRaw := flags.String("since", "", "only scan files modified at or after this RFC3339 timestamp")
	sinceFile := flags.String("since-file", "", "only scan files and timestamped thread messages at or after this file's modification time")
	sinceHomebrewRuntime := flags.Bool("since-homebrew-runtime", false, "only scan data at or after the Homebrew cliproxyapi.real replacement time")
	allowMissing := flags.Bool("allow-missing", true, "treat missing scan directories as empty")
	jsonOutput := flags.Bool("json", false, "print findings as JSON")
	if err := flags.Parse(args); err != nil {
		return 2
	}

	since, err := parseSinceCutoff(*sinceRaw, *sinceFile, *sinceHomebrewRuntime)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}

	findings, err := scanRuntimeDrift(scanOptions{
		threadDir:    *threadDir,
		captureDir:   *captureDir,
		baselinePath: *baselinePath,
		since:        since,
		allowMissing: *allowMissing,
		jsonOutput:   *jsonOutput,
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

func defaultThreadDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".local", "share", "amp", "threads")
}

func defaultCaptureDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".cli-proxy-api", "logs", "neo-provider-requests")
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
		threadFindings, err := scanThreadDir(options.threadDir, options.since, options.allowMissing, modelContextWindows, smartModel)
		if err != nil {
			return nil, err
		}
		findings = append(findings, threadFindings...)
	}
	if strings.TrimSpace(options.captureDir) != "" {
		captureFindings, err := scanCaptureDir(options.captureDir, options.since, options.allowMissing)
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

func scanThreadDir(dir string, since time.Time, allowMissing bool, modelContextWindows map[string]int, smartModel string) ([]driftFinding, error) {
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
		var thread map[string]any
		if err := json.Unmarshal(raw, &thread); err != nil {
			return nil, fmt.Errorf("%s: %w", file, err)
		}
		threadID := stringValue(thread["id"])
		messages := arrayValue(thread["messages"])
		findings = append(findings, scanThreadCompactionDrift(file, threadID, thread, messages, since, modelContextWindows, smartModel)...)
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
				if stringValue(block["type"]) != "tool_result" {
					continue
				}
				toolID := firstNonEmptyString(block["toolUseID"], block["toolUseId"], block["tool_use_id"], block["toolCallId"], block["id"])
				toolName := firstNonEmptyString(block["name"], toolNames[toolID])
				if bareTerminalDoneForPayloadRequiredTool(toolName, mapValue(block["run"])) {
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
	return findings, nil
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

func scanCaptureDir(dir string, since time.Time, allowMissing bool) ([]driftFinding, error) {
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
		body := mapValue(capture["body"])
		inputs := firstArray(body["input"], body["messages"])
		toolNames := map[string]string{}
		for _, rawItem := range inputs {
			item := mapValue(rawItem)
			if stringValue(item["type"]) != "function_call" {
				continue
			}
			callID := firstNonEmptyString(item["call_id"], item["callId"], item["id"])
			if callID != "" {
				toolNames[callID] = stringValue(item["name"])
			}
		}
		for _, rawItem := range inputs {
			item := mapValue(rawItem)
			if stringValue(item["type"]) != "function_call_output" {
				continue
			}
			callID := firstNonEmptyString(item["call_id"], item["callId"], item["id"])
			toolName := toolNames[callID]
			run := decodeRunOutput(item["output"])
			if bareTerminalDoneForPayloadRequiredTool(toolName, run) {
				findings = append(findings, driftFinding{
					Source:   "provider-capture",
					File:     file,
					ThreadID: stringValue(capture["threadID"]),
					CallID:   callID,
					ToolName: normalizeToolName(toolName),
					Detail:   `model input contains terminal "done" with no result/output payload`,
				})
			}
		}
	}
	return findings, nil
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
