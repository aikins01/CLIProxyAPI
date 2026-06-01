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
	threadDir    string
	captureDir   string
	since        time.Time
	allowMissing bool
	jsonOutput   bool
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
	sinceRaw := flags.String("since", "", "only scan files modified at or after this RFC3339 timestamp")
	allowMissing := flags.Bool("allow-missing", true, "treat missing scan directories as empty")
	jsonOutput := flags.Bool("json", false, "print findings as JSON")
	if err := flags.Parse(args); err != nil {
		return 2
	}

	var since time.Time
	if strings.TrimSpace(*sinceRaw) != "" {
		parsed, err := time.Parse(time.RFC3339, strings.TrimSpace(*sinceRaw))
		if err != nil {
			fmt.Fprintf(stderr, "parse -since: %v\n", err)
			return 2
		}
		since = parsed
	}

	findings, err := scanRuntimeDrift(scanOptions{
		threadDir:    *threadDir,
		captureDir:   *captureDir,
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
		fmt.Fprintln(stdout, "no bare terminal tool-result drift found")
	} else {
		fmt.Fprintf(stdout, "found %d bare terminal tool-result drift(s):\n", len(findings))
		for _, finding := range findings {
			fmt.Fprintf(stdout, "- %s %s %s %s: %s\n", finding.Source, finding.ToolName, finding.CallID, finding.File, finding.Detail)
		}
	}

	if len(findings) > 0 {
		return 2
	}
	return 0
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

func scanRuntimeDrift(options scanOptions) ([]driftFinding, error) {
	var findings []driftFinding
	if strings.TrimSpace(options.threadDir) != "" {
		threadFindings, err := scanThreadDir(options.threadDir, options.since, options.allowMissing)
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

func scanThreadDir(dir string, since time.Time, allowMissing bool) ([]driftFinding, error) {
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
