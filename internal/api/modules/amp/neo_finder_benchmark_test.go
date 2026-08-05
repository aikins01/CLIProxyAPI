package amp

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
)

type neoFinderBenchmarkCase struct {
	Name           string
	Query          string
	Files          map[string]string
	RequiredGroups [][]string
	AllowedFiles   []string
	ForbiddenFiles []string
}

type neoFinderBenchmarkCandidate struct {
	Name   string
	Route  neoModelRoute
	Effort string
}

type neoFinderBenchmarkMetrics struct {
	Turns                    int
	ToolCalls                int
	MaxParallel              int
	RuntimeAcceptedToolCalls int
	ScopeRejectedToolCalls   int
	MaxRuntimeParallel       int
	MaxRuntimeInFlight       int
	ForcedFinals             int
	InputTokens              int
	OutputTokens             int
	TotalInput               int
	ModelMillis              int64
	ToolResultByte           int
}

func TestNeoFinderBenchmarkFixtures(t *testing.T) {
	cases := neoFinderBenchmarkCases()
	if len(cases) < 8 {
		t.Fatalf("finder benchmark cases = %d, want at least 8", len(cases))
	}
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			if strings.TrimSpace(tc.Query) == "" || len(tc.Files) < 4 || len(tc.RequiredGroups) == 0 || len(tc.AllowedFiles) == 0 {
				t.Fatalf("incomplete finder fixture: %#v", tc)
			}
			for _, group := range tc.RequiredGroups {
				if len(group) == 0 {
					t.Fatalf("empty required group: %#v", tc)
				}
				for _, file := range group {
					if _, ok := tc.Files[file]; !ok {
						t.Fatalf("required file %q missing from corpus", file)
					}
				}
			}
			for _, file := range append(append([]string(nil), tc.AllowedFiles...), tc.ForbiddenFiles...) {
				if _, ok := tc.Files[file]; !ok {
					t.Fatalf("scored file %q missing from corpus", file)
				}
			}
		})
	}
}

func TestNeoFinderDefaultBenchmarkCandidates(t *testing.T) {
	t.Setenv("AMP_FINDER_MODEL_BENCHMARK_CANDIDATES", "")
	candidates := neoFinderBenchmarkCandidates(t)
	got := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		got = append(got, candidate.Route.Provider+"/"+candidate.Route.Model+"@"+firstNonEmptyString(candidate.Effort, "default"))
	}
	want := []string{
		"openai/gpt-5.6-terra@low",
		"openai/gpt-5.6-luna@low",
		"openai/gpt-5.6-sol@low",
		"google/gemini-3.5-flash@low",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("default finder benchmark candidates = %#v, want %#v", got, want)
	}
}

func TestNeoFinderBenchmarkScore(t *testing.T) {
	tc := neoFinderBenchmarkCase{
		Files: map[string]string{
			"internal/auth/middleware.go": "auth",
			"internal/auth/token.go":      "token",
			"internal/auth/cache.go":      "cache",
			"docs/auth.md":                "docs",
		},
		RequiredGroups: [][]string{{"internal/auth/middleware.go"}, {"internal/auth/token.go"}},
		AllowedFiles:   []string{"internal/auth/middleware.go", "internal/auth/token.go"},
		ForbiddenFiles: []string{"docs/auth.md"},
	}
	output := "Relevant files:\n- [internal/auth/middleware.go#L1-L20](file:///benchmark/repository/internal/auth/middleware.go#L1-L20)\n- [internal/auth/token.go#L1-L10](file:///benchmark/repository/internal/auth/token.go#L1-L10)"
	passed, missing, unexpected := neoFinderBenchmarkScore(output, tc)
	if !passed || len(missing) != 0 || len(unexpected) != 0 {
		t.Fatalf("score passed=%v missing=%#v unexpected=%#v", passed, missing, unexpected)
	}
	passed, missing, _ = neoFinderBenchmarkScore("Relevant files:\n- internal/auth/middleware.go\n- internal/auth/token.go\n- unrelated.go#L1-L2", tc)
	if passed || len(missing) == 0 {
		t.Fatal("score accepted required files without their own line ranges")
	}
	passed, missing, unexpected = neoFinderBenchmarkScore(output+"\n- docs/auth.md", tc)
	if passed || len(missing) != 0 || len(unexpected) == 0 {
		t.Fatalf("score accepted forbidden file: missing=%#v unexpected=%#v", missing, unexpected)
	}
	passed, missing, unexpected = neoFinderBenchmarkScore(output+"\n- internal/auth/cache.go", tc)
	if passed || len(missing) != 0 || !slices.Contains(unexpected, "internal/auth/cache.go") {
		t.Fatalf("score accepted file outside allowlist: missing=%#v unexpected=%#v", missing, unexpected)
	}
}

func TestNeoFinderBenchmarkRuntimeFanout(t *testing.T) {
	files := map[string]string{"internal/runtime.go": "type runtime struct{}"}
	root := neoFinderBenchmarkWorkspace(t, files)
	calls := make([]neoToolCall, 8)
	for i := range calls {
		calls[i] = neoToolCall{ID: "call-" + strconv.Itoa(i), Name: "Grep", Input: map[string]any{"pattern": "runtime"}}
	}
	metrics := &neoFinderBenchmarkMetrics{}
	exchanges := neoFinderBenchmarkExecuteTurn(root, files, calls, metrics)
	if len(exchanges) != len(calls) {
		t.Fatalf("benchmark exchanges = %d, want %d", len(exchanges), len(calls))
	}
	if metrics.RuntimeAcceptedToolCalls != len(calls) {
		t.Fatalf("benchmark fanout metrics = %#v", metrics)
	}
	if metrics.MaxRuntimeParallel != len(calls) || metrics.MaxRuntimeInFlight != len(calls) {
		t.Fatalf("benchmark parallel metrics = %#v", metrics)
	}
	for i, exchange := range exchanges {
		if !neoRunIsTerminal(exchange.Run) {
			t.Fatalf("exchange %d is not terminal: %#v", i, exchange.Run)
		}
	}
}

func TestNeoFinderSyntheticModelBenchmark(t *testing.T) {
	if !neoReadThreadTruthyEnv("AMP_FINDER_MODEL_BENCHMARK") {
		t.Skip("set AMP_FINDER_MODEL_BENCHMARK=1 to run live finder model benchmark")
	}
	candidates := neoFinderBenchmarkCandidates(t)
	cases := neoFinderSelectedBenchmarkCases(t)
	repetitions := neoFinderBenchmarkRepetitions(t)
	strict := neoReadThreadTruthyEnv("AMP_FINDER_MODEL_BENCHMARK_STRICT")
	results := make([]map[string]any, 0, len(candidates)*len(cases)*repetitions)
	for caseIndex, tc := range cases {
		for rep := 1; rep <= repetitions; rep++ {
			offset := (caseIndex + rep - 1) % len(candidates)
			for candidateIndex := range candidates {
				candidate := candidates[(candidateIndex+offset)%len(candidates)]
				tc := tc
				rep := rep
				t.Run(fmt.Sprintf("%s/%s/%d", candidate.Name, neoReadThreadBenchmarkSafeName(tc.Name), rep), func(t *testing.T) {
					result := neoFinderRunSyntheticModelBenchmark(t, candidate, tc, rep)
					results = append(results, result)
					raw, _ := json.Marshal(result)
					t.Log(string(raw))
					if stringValue(result["error"]) != "" {
						t.Fatalf("benchmark provider error: %s", string(raw))
					}
					if strict && !boolValue(result["passed"]) {
						t.Fatalf("benchmark miss: %s", string(raw))
					}
				})
			}
		}
	}
	for _, summary := range neoFinderBenchmarkSummaries(results) {
		raw, _ := json.Marshal(summary)
		t.Log("finder benchmark summary: " + string(raw))
	}
}

func neoFinderRunSyntheticModelBenchmark(t *testing.T, candidate neoFinderBenchmarkCandidate, tc neoFinderBenchmarkCase, rep int) map[string]any {
	t.Helper()
	root := neoFinderBenchmarkWorkspace(t, tc.Files)
	rt := newNeoRuntime(neoFinderBenchmarkConfig(t))
	tools := neoFinderBenchmarkToolSpecs()
	systemPrompt := strings.NewReplacer(
		"{{WORKING_DIR}}", root,
		"{{WORKSPACE_ROOT}}", root,
	).Replace(neoFinderSubagentPrompt)
	conversation := []neoHistoryMessage{{Role: "user", Text: tc.Query}}
	settings := map[string]any{}
	if candidate.Effort != "" {
		settings["reasoning.effort"] = candidate.Effort
	}
	metrics := &neoFinderBenchmarkMetrics{}
	finalText := ""
	var runErr error
	started := time.Now()
	maxTurns := 6
	for turn := 0; turn < maxTurns; turn++ {
		route := candidate.Route
		request := neoInferenceRequest{
			ActorID:              "actor-finder-benchmark",
			ThreadID:             "T-019f5000-0000-7000-8000-000000000002",
			MessageID:            newNeoMessageID(),
			AgentMode:            "low",
			ReasoningEffort:      candidate.Effort,
			MaxTokens:            4096,
			Settings:             settings,
			History:              append([]neoHistoryMessage(nil), conversation...),
			Tools:                tools,
			Environment:          map[string]any{"workingDirectory": root, "workspaceRoot": root},
			ModelRouteOverride:   &route,
			SystemPromptOverride: systemPrompt,
		}
		modelStarted := time.Now()
		inference, err := inferNeoLocalStream(rt, request, func(neoInferenceDelta) {})
		metrics.ModelMillis += time.Since(modelStarted).Milliseconds()
		metrics.Turns++
		metrics.ToolCalls += len(inference.ToolCalls)
		metrics.MaxParallel = max(metrics.MaxParallel, len(inference.ToolCalls))
		metrics.InputTokens += neoRunCheckBenchmarkUsageInt(inference.Usage, "inputTokens", "input_tokens", "prompt_tokens", "promptTokenCount")
		metrics.OutputTokens += neoRunCheckBenchmarkUsageInt(inference.Usage, "outputTokens", "output_tokens", "completion_tokens", "candidatesTokenCount")
		metrics.TotalInput += neoRunCheckBenchmarkUsageInt(inference.Usage, "totalInputTokens", "total_input_tokens", "prompt_tokens", "promptTokenCount")
		if err != nil {
			runErr = err
			break
		}
		conversation = append(conversation, neoHistoryMessage{Role: "assistant", Text: inference.Text, ToolCalls: inference.ToolCalls, ThinkingBlocks: inference.ThinkingBlocks})
		if len(inference.ToolCalls) == 0 {
			finalText = inference.Text
			break
		}
		for _, exchange := range neoFinderBenchmarkExecuteTurn(root, tc.Files, inference.ToolCalls, metrics) {
			text := runToText(exchange.Run)
			metrics.ToolResultByte += len(text)
			conversation = append(conversation, neoHistoryMessage{
				Role:       "tool",
				ToolCallID: exchange.Call.ID,
				ToolName:   exchange.Call.Name,
				Text:       text,
				Content:    neoToolRunHistoryContent(exchange.Run),
			})
		}
	}
	if runErr == nil && strings.TrimSpace(finalText) == "" && len(conversation) > 1 {
		metrics.ForcedFinals++
		route := candidate.Route
		forced := append(append([]neoHistoryMessage(nil), conversation...), neoHistoryMessage{Role: "user", Text: "You have enough search results. Return the concise final file list now. Do not call tools."})
		modelStarted := time.Now()
		inference, err := inferNeoLocalStream(rt, neoInferenceRequest{
			ActorID:              "actor-finder-benchmark",
			ThreadID:             "T-019f5000-0000-7000-8000-000000000002",
			MessageID:            newNeoMessageID(),
			AgentMode:            "low",
			ReasoningEffort:      candidate.Effort,
			MaxTokens:            4096,
			Settings:             settings,
			History:              forced,
			Environment:          map[string]any{"workingDirectory": root, "workspaceRoot": root},
			ModelRouteOverride:   &route,
			SystemPromptOverride: systemPrompt,
		}, func(neoInferenceDelta) {})
		metrics.ModelMillis += time.Since(modelStarted).Milliseconds()
		metrics.Turns++
		metrics.InputTokens += neoRunCheckBenchmarkUsageInt(inference.Usage, "inputTokens", "input_tokens", "prompt_tokens", "promptTokenCount")
		metrics.OutputTokens += neoRunCheckBenchmarkUsageInt(inference.Usage, "outputTokens", "output_tokens", "completion_tokens", "candidatesTokenCount")
		metrics.TotalInput += neoRunCheckBenchmarkUsageInt(inference.Usage, "totalInputTokens", "total_input_tokens", "prompt_tokens", "promptTokenCount")
		if err != nil {
			runErr = err
		} else {
			finalText = inference.Text
		}
	}
	passed, missing, unexpected := neoFinderBenchmarkScore(finalText, tc)
	result := map[string]any{
		"candidate":                candidate.Name,
		"provider":                 candidate.Route.Provider,
		"model":                    candidate.Route.Model,
		"effort":                   firstNonEmptyString(candidate.Effort, "default"),
		"case":                     tc.Name,
		"rep":                      rep,
		"passed":                   passed && runErr == nil,
		"durationMillis":           time.Since(started).Milliseconds(),
		"modelMillis":              metrics.ModelMillis,
		"turns":                    metrics.Turns,
		"toolCalls":                metrics.ToolCalls,
		"maxParallel":              metrics.MaxParallel,
		"runtimeAcceptedToolCalls": metrics.RuntimeAcceptedToolCalls,
		"scopeRejectedToolCalls":   metrics.ScopeRejectedToolCalls,
		"maxRuntimeParallel":       metrics.MaxRuntimeParallel,
		"maxRuntimeInFlight":       metrics.MaxRuntimeInFlight,
		"forcedFinals":             metrics.ForcedFinals,
		"inputTokens":              metrics.InputTokens,
		"outputTokens":             metrics.OutputTokens,
		"totalInputTokens":         metrics.TotalInput,
		"toolResultBytes":          metrics.ToolResultByte,
		"missing":                  missing,
		"unexpected":               unexpected,
		"output":                   neoClipRunes(finalText, 1800),
	}
	if runErr != nil {
		result["error"] = runErr.Error()
	}
	return result
}

func neoFinderBenchmarkWorkspace(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, ".git"), 0o700); err != nil {
		t.Fatalf("create benchmark repository: %v", err)
	}
	for name, content := range files {
		target := filepath.Join(root, filepath.FromSlash(name))
		if !neoFinderPathWithin(root, target) {
			t.Fatalf("benchmark fixture path escapes repository: %q", name)
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
			t.Fatalf("create benchmark fixture directory: %v", err)
		}
		if err := os.WriteFile(target, []byte(content), 0o600); err != nil {
			t.Fatalf("write benchmark fixture: %v", err)
		}
	}
	canonical, err := neoFinderCanonicalDirectory(root)
	if err != nil {
		t.Fatalf("canonicalize benchmark repository: %v", err)
	}
	return canonical
}

func neoFinderBenchmarkExecuteTurn(root string, files map[string]string, calls []neoToolCall, metrics *neoFinderBenchmarkMetrics) []neoSubagentToolExchange {
	exchanges := make([]neoSubagentToolExchange, 0, len(calls))
	executable := make([]int, 0, len(calls))
	for _, call := range calls {
		if call.Incomplete {
			continue
		}
		exchange := neoSubagentToolExchange{Call: call}
		scoped, err := neoScopeFinderToolCall(call, root)
		if err != nil {
			exchange.Run = neoFinderToolError(err.Error())
			metrics.ScopeRejectedToolCalls++
			exchanges = append(exchanges, exchange)
			continue
		}
		exchange.Call = scoped
		executable = append(executable, len(exchanges))
		exchanges = append(exchanges, exchange)
	}
	metrics.RuntimeAcceptedToolCalls += len(executable)
	if len(executable) == 0 {
		return exchanges
	}

	metrics.MaxRuntimeParallel = max(metrics.MaxRuntimeParallel, len(executable))
	var wg sync.WaitGroup
	var ready sync.WaitGroup
	var activeMu sync.Mutex
	active := 0
	start := make(chan struct{})
	ready.Add(len(executable))
	for _, index := range executable {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			activeMu.Lock()
			active++
			metrics.MaxRuntimeInFlight = max(metrics.MaxRuntimeInFlight, active)
			activeMu.Unlock()
			ready.Done()
			<-start
			exchanges[index].Run = neoFinderBenchmarkExecuteTool(root, files, exchanges[index].Call)
			activeMu.Lock()
			active--
			activeMu.Unlock()
		}(index)
	}
	ready.Wait()
	close(start)
	wg.Wait()
	return exchanges
}

func neoFinderBenchmarkSummaries(results []map[string]any) []map[string]any {
	grouped := map[string][]map[string]any{}
	for _, result := range results {
		grouped[stringValue(result["candidate"])] = append(grouped[stringValue(result["candidate"])], result)
	}
	names := make([]string, 0, len(grouped))
	for name := range grouped {
		names = append(names, name)
	}
	sort.Strings(names)
	summaries := make([]map[string]any, 0, len(names))
	for _, name := range names {
		runs := grouped[name]
		passes := 0
		errors := 0
		for _, run := range runs {
			if boolValue(run["passed"]) {
				passes++
			}
			if stringValue(run["error"]) != "" {
				errors++
			}
		}
		summaries = append(summaries, map[string]any{
			"candidate":                    name,
			"provider":                     stringValue(runs[0]["provider"]),
			"model":                        stringValue(runs[0]["model"]),
			"effort":                       stringValue(runs[0]["effort"]),
			"runs":                         len(runs),
			"passes":                       passes,
			"passRate":                     float64(passes) / float64(len(runs)),
			"providerErrors":               errors,
			"durationMedianMillis":         neoFinderBenchmarkPercentile(runs, "durationMillis", 50),
			"durationP95Millis":            neoFinderBenchmarkPercentile(runs, "durationMillis", 95),
			"modelMedianMillis":            neoFinderBenchmarkPercentile(runs, "modelMillis", 50),
			"modelP95Millis":               neoFinderBenchmarkPercentile(runs, "modelMillis", 95),
			"turnsMedian":                  neoFinderBenchmarkPercentile(runs, "turns", 50),
			"toolCallsMedian":              neoFinderBenchmarkPercentile(runs, "toolCalls", 50),
			"modelRequestedParallelMedian": neoFinderBenchmarkPercentile(runs, "maxParallel", 50),
			"runtimeAcceptedCallsMedian":   neoFinderBenchmarkPercentile(runs, "runtimeAcceptedToolCalls", 50),
			"runtimeParallelMedian":        neoFinderBenchmarkPercentile(runs, "maxRuntimeParallel", 50),
			"runtimeInFlightMax":           neoFinderBenchmarkMaximum(runs, "maxRuntimeInFlight"),
			"capRejectedCalls":             neoFinderBenchmarkSum(runs, "capRejectedToolCalls"),
			"scopeRejectedCalls":           neoFinderBenchmarkSum(runs, "scopeRejectedToolCalls"),
			"forcedFinals":                 neoFinderBenchmarkSum(runs, "forcedFinals"),
			"inputTokensMedian":            neoFinderBenchmarkPercentile(runs, "inputTokens", 50),
			"outputTokensMedian":           neoFinderBenchmarkPercentile(runs, "outputTokens", 50),
			"totalInputTokensMedian":       neoFinderBenchmarkPercentile(runs, "totalInputTokens", 50),
			"toolResultBytesMedian":        neoFinderBenchmarkPercentile(runs, "toolResultBytes", 50),
		})
	}
	return summaries
}

func neoFinderBenchmarkPercentile(results []map[string]any, key string, percentile int) int {
	values := make([]int, 0, len(results))
	for _, result := range results {
		values = append(values, numberFrom(result[key]))
	}
	sort.Ints(values)
	if len(values) == 0 {
		return 0
	}
	index := (percentile*len(values) + 99) / 100
	return values[max(0, min(index-1, len(values)-1))]
}

func neoFinderBenchmarkMaximum(results []map[string]any, key string) int {
	maximum := 0
	for _, result := range results {
		maximum = max(maximum, numberFrom(result[key]))
	}
	return maximum
}

func neoFinderBenchmarkSum(results []map[string]any, key string) int {
	total := 0
	for _, result := range results {
		total += numberFrom(result[key])
	}
	return total
}

func neoFinderBenchmarkScore(output string, tc neoFinderBenchmarkCase) (bool, []string, []string) {
	normalized := strings.ToLower(output)
	missing := make([]string, 0)
	for _, group := range tc.RequiredGroups {
		found := false
		for _, file := range group {
			pattern := regexp.MustCompile(regexp.QuoteMeta(strings.ToLower(file)) + `#l\d+(?:-l\d+)?`)
			if pattern.MatchString(normalized) {
				found = true
				break
			}
		}
		if !found {
			missing = append(missing, strings.Join(group, " | "))
		}
	}
	allowed := make(map[string]struct{}, len(tc.AllowedFiles))
	for _, file := range tc.AllowedFiles {
		allowed[strings.ToLower(file)] = struct{}{}
	}
	unexpected := make([]string, 0)
	for file := range tc.Files {
		fileLower := strings.ToLower(file)
		if _, ok := allowed[fileLower]; !ok && strings.Contains(normalized, fileLower) {
			unexpected = append(unexpected, file)
		}
	}
	sort.Strings(missing)
	sort.Strings(unexpected)
	return len(missing) == 0 && len(unexpected) == 0, missing, unexpected
}

func neoFinderBenchmarkToolSpecs() []neoToolSpec {
	return []neoToolSpec{
		{
			Name:        "Grep",
			Description: "Search file contents using a regular expression. Returns matching file paths, line numbers, and lines. Use path or glob to scope the search.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"pattern":       map[string]any{"type": "string"},
					"path":          map[string]any{"type": "string"},
					"glob":          map[string]any{"type": "string"},
					"caseSensitive": map[string]any{"type": "boolean"},
					"literal":       map[string]any{"type": "boolean"},
				},
				"required": []any{"pattern"},
			},
		},
		{
			Name:        "glob",
			Description: "Find files whose paths match a glob pattern.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"filePattern": map[string]any{"type": "string"},
				},
				"required": []any{"filePattern"},
			},
		},
		{
			Name:        "Read",
			Description: "Read a text file with line numbers. Use offset and limit to select a range.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"path":   map[string]any{"type": "string"},
					"offset": map[string]any{"type": "integer"},
					"limit":  map[string]any{"type": "integer"},
				},
				"required": []any{"path"},
			},
		},
	}
}

func neoFinderBenchmarkExecuteTool(root string, files map[string]string, call neoToolCall) map[string]any {
	var result any
	var err error
	switch call.Name {
	case "Grep":
		result, err = neoFinderBenchmarkGrep(root, files, call.Input)
	case "glob":
		result, err = neoFinderBenchmarkGlob(root, files, call.Input)
	case "Read":
		result, err = neoFinderBenchmarkRead(root, files, call.Input)
	default:
		err = fmt.Errorf("unsupported benchmark tool %q", call.Name)
	}
	if err != nil {
		return map[string]any{"status": "error", "error": map[string]any{"message": err.Error()}}
	}
	return map[string]any{"status": "done", "result": result}
}

func neoFinderBenchmarkGrep(root string, files map[string]string, input map[string]any) ([]string, error) {
	patternText := strings.TrimSpace(stringValue(input["pattern"]))
	if patternText == "" {
		return nil, fmt.Errorf("Grep requires pattern")
	}
	if boolValue(input["literal"]) {
		patternText = regexp.QuoteMeta(patternText)
	}
	if !boolValue(input["caseSensitive"]) {
		patternText = "(?i)" + patternText
	}
	pattern, err := regexp.Compile(patternText)
	if err != nil {
		pattern = regexp.MustCompile("(?i)" + regexp.QuoteMeta(strings.TrimPrefix(patternText, "(?i)")))
	}
	pathScope := neoFinderBenchmarkRelativePath(root, firstNonEmptyString(input["path"], input["directory"]))
	globPattern := firstNonEmptyString(input["glob"], input["filePattern"])
	paths := neoFinderBenchmarkSortedPaths(files)
	matches := make([]string, 0)
	for _, file := range paths {
		if pathScope != "" && file != pathScope && !strings.HasPrefix(file, strings.TrimSuffix(pathScope, "/")+"/") {
			continue
		}
		if globPattern != "" && !neoFinderBenchmarkGlobMatch(root, globPattern, file) {
			continue
		}
		for index, line := range strings.Split(files[file], "\n") {
			if pattern.MatchString(line) {
				matches = append(matches, fmt.Sprintf("%s/%s:%d:%s", root, file, index+1, line))
				if len(matches) >= 60 {
					return matches, nil
				}
			}
		}
	}
	return matches, nil
}

func neoFinderBenchmarkGlob(root string, files map[string]string, input map[string]any) ([]string, error) {
	pattern := strings.TrimSpace(firstNonEmptyString(input["filePattern"], input["pattern"], input["glob"]))
	if pattern == "" {
		return nil, fmt.Errorf("glob requires filePattern")
	}
	result := make([]string, 0)
	for _, file := range neoFinderBenchmarkSortedPaths(files) {
		if neoFinderBenchmarkGlobMatch(root, pattern, file) {
			result = append(result, root+"/"+file)
		}
	}
	return result, nil
}

func neoFinderBenchmarkRead(root string, files map[string]string, input map[string]any) (string, error) {
	file := neoFinderBenchmarkRelativePath(root, firstNonEmptyString(input["path"], input["filePath"], input["file_path"]))
	content, ok := files[file]
	if !ok {
		return "", fmt.Errorf("file not found: %s", file)
	}
	lines := strings.Split(content, "\n")
	offset := numberFrom(input["offset"])
	if offset <= 0 {
		offset = 1
	}
	limit := numberFrom(input["limit"])
	if limit <= 0 || limit > 200 {
		limit = 200
	}
	start := min(max(offset-1, 0), len(lines))
	end := min(start+limit, len(lines))
	var out strings.Builder
	for index := start; index < end; index++ {
		fmt.Fprintf(&out, "%d: %s\n", index+1, lines[index])
	}
	return root + "/" + file + "\n" + out.String(), nil
}

func neoFinderBenchmarkRelativePath(root, value string) string {
	value = strings.TrimSpace(value)
	value = strings.TrimPrefix(value, "file://")
	value = strings.TrimPrefix(value, root)
	return strings.TrimPrefix(path.Clean("/"+value), "/")
}

func neoFinderBenchmarkGlobMatch(root, pattern, file string) bool {
	pattern = strings.TrimPrefix(neoFinderBenchmarkRelativePath(root, pattern), "./")
	if pattern == "" || pattern == "." || pattern == "*" || pattern == "**" || pattern == "**/*" {
		return true
	}
	quoted := regexp.QuoteMeta(pattern)
	quoted = strings.ReplaceAll(quoted, `\*\*`, `.*`)
	quoted = strings.ReplaceAll(quoted, `\*`, `[^/]*`)
	quoted = strings.ReplaceAll(quoted, `\?`, `[^/]`)
	matched, _ := regexp.MatchString("^"+quoted+"$", file)
	if matched || !strings.HasPrefix(pattern, "**/") {
		return matched
	}
	return neoFinderBenchmarkGlobMatch(root, strings.TrimPrefix(pattern, "**/"), file)
}

func neoFinderBenchmarkSortedPaths(files map[string]string) []string {
	paths := make([]string, 0, len(files))
	for file := range files {
		paths = append(paths, file)
	}
	sort.Strings(paths)
	return paths
}

func neoFinderBenchmarkCandidates(t *testing.T) []neoFinderBenchmarkCandidate {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv("AMP_FINDER_MODEL_BENCHMARK_CANDIDATES"))
	if raw == "" {
		return []neoFinderBenchmarkCandidate{
			{Name: "gpt-5.6-terra-low", Route: neoModelRoute{Provider: "openai", Model: "gpt-5.6-terra"}, Effort: "low"},
			{Name: "gpt-5.6-luna-low", Route: neoModelRoute{Provider: "openai", Model: "gpt-5.6-luna"}, Effort: "low"},
			{Name: "gpt-5.6-sol-low", Route: neoModelRoute{Provider: "openai", Model: "gpt-5.6-sol"}, Effort: "low"},
			{Name: "gemini-3.5-flash-low", Route: neoModelRoute{Provider: "google", Model: "gemini-3.5-flash"}, Effort: "low"},
		}
	}
	candidates := make([]neoFinderBenchmarkCandidate, 0)
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		routeText := item
		effort := ""
		if left, right, ok := strings.Cut(item, "@"); ok {
			routeText = strings.TrimSpace(left)
			effort = strings.TrimSpace(right)
			if effort == "default" {
				effort = ""
			}
		}
		route := parseNeoModelRoute(routeText)
		if route.Model == "" {
			t.Fatalf("invalid finder benchmark candidate %q", item)
		}
		if route.Provider == "" {
			route.Provider = providerForNeoModel(route.Model)
		}
		nameEffort := firstNonEmptyString(effort, "default")
		candidates = append(candidates, neoFinderBenchmarkCandidate{
			Name:   neoReadThreadBenchmarkSafeName(route.Provider + "-" + route.Model + "-" + nameEffort),
			Route:  route,
			Effort: effort,
		})
	}
	if len(candidates) == 0 {
		t.Fatal("AMP_FINDER_MODEL_BENCHMARK_CANDIDATES did not contain any candidates")
	}
	return candidates
}

func neoFinderSelectedBenchmarkCases(t *testing.T) []neoFinderBenchmarkCase {
	t.Helper()
	cases := neoFinderBenchmarkCases()
	raw := strings.TrimSpace(os.Getenv("AMP_FINDER_MODEL_BENCHMARK_CASES"))
	if raw == "" {
		return cases
	}
	wanted := map[string]bool{}
	for _, name := range strings.Split(raw, ",") {
		if name = strings.TrimSpace(name); name != "" {
			wanted[name] = true
		}
	}
	selected := make([]neoFinderBenchmarkCase, 0, len(wanted))
	for _, tc := range cases {
		if wanted[tc.Name] {
			selected = append(selected, tc)
			delete(wanted, tc.Name)
		}
	}
	if len(wanted) > 0 {
		unknown := make([]string, 0, len(wanted))
		for name := range wanted {
			unknown = append(unknown, name)
		}
		sort.Strings(unknown)
		t.Fatalf("unknown finder benchmark cases: %v", unknown)
	}
	return selected
}

func neoFinderBenchmarkConfig(t *testing.T) *config.Config {
	t.Helper()
	rawURL := strings.TrimSpace(os.Getenv("AMP_FINDER_MODEL_BENCHMARK_URL"))
	if rawURL == "" {
		rawURL = "http://127.0.0.1:8317"
	}
	parsed, err := url.Parse(rawURL)
	if err != nil {
		t.Fatalf("parse AMP_FINDER_MODEL_BENCHMARK_URL: %v", err)
	}
	portText := parsed.Port()
	if portText == "" {
		if parsed.Scheme == "https" {
			portText = "443"
		} else {
			portText = "80"
		}
	}
	port, err := strconv.Atoi(portText)
	if err != nil || parsed.Hostname() == "" {
		t.Fatalf("invalid AMP_FINDER_MODEL_BENCHMARK_URL %q", rawURL)
	}
	cfg := &config.Config{Host: parsed.Hostname(), Port: port}
	if parsed.Scheme == "https" {
		cfg.TLS.Enable = true
	}
	if key := strings.TrimSpace(os.Getenv("AMP_FINDER_MODEL_BENCHMARK_API_KEY")); key != "" {
		cfg.APIKeys = []string{key}
	}
	return cfg
}

func neoFinderBenchmarkRepetitions(t *testing.T) int {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv("AMP_FINDER_MODEL_BENCHMARK_REPS"))
	if raw == "" {
		return 1
	}
	repetitions, err := strconv.Atoi(raw)
	if err != nil || repetitions <= 0 {
		t.Fatalf("AMP_FINDER_MODEL_BENCHMARK_REPS = %q, want positive integer", raw)
	}
	return repetitions
}

func neoFinderBenchmarkCases() []neoFinderBenchmarkCase {
	return []neoFinderBenchmarkCase{
		{
			Name:  "compaction lifecycle",
			Query: "Find the local runtime compaction implementation. Return files and line ranges for auto-compaction triggers, status events, summary persistence, and failure handling. Focus under internal/api/modules/amp.",
			Files: map[string]string{
				"internal/api/modules/amp/neo_runtime.go": `package amp
func (a *neoActor) shouldAutoCompact() bool { return a.estimatedTokens >= a.compactionThreshold() }
func (a *neoActor) runCompaction() error {
	a.broadcast(map[string]any{"type": "compaction_started"})
	summary, err := a.inferCompactionSummary()
	if err != nil { a.broadcast(map[string]any{"type": "compaction_failed"}); return err }
	a.messages = append(a.messages, neoMessage{Role: "info", Content: summary})
	a.compactionRecords = append(a.compactionRecords, newCompactionRecord(summary))
	a.persistSnapshot()
	a.broadcast(map[string]any{"type": "compaction_complete"})
	return nil
}`,
				"internal/api/modules/amp/neo_runtime_test.go": `package amp
func TestAutoCompactionEmitsLifecycleAndPersistsSummary(t *testing.T) {}
func TestCompactionFailureKeepsOriginalHistory(t *testing.T) {}`,
				"internal/api/modules/amp/fallback_handlers.go": `package amp
func looksLikeAmpCompactionRequest(body []byte) bool { return bytes.Contains(body, []byte("continuation summary")) }`,
				"internal/api/modules/amp/neo_thread_reader_agent.go": `package amp
func renderSummaryBlock(block map[string]any) string { return neoCompactionSummaryText(block) }`,
				"internal/api/modules/amp/neo_code_review_render.go": `package amp
func renderReviewSummary(summary map[string]any) string { return stringValue(summary["status"]) }`,
				"docs/context-management.md": `Compaction keeps long conversations manageable.`,
			},
			RequiredGroups: [][]string{{"internal/api/modules/amp/neo_runtime.go"}, {"internal/api/modules/amp/neo_runtime_test.go"}},
			AllowedFiles:   []string{"internal/api/modules/amp/neo_runtime.go", "internal/api/modules/amp/neo_runtime_test.go"},
			ForbiddenFiles: []string{"internal/api/modules/amp/neo_code_review_render.go", "docs/context-management.md"},
		},
		{
			Name:  "tool result round trip",
			Query: "Trace where local Neo runtime web_search tool calls are delegated, where inbound tool results are normalized and persisted, and how the model receives them on the next turn. Return exact files and line ranges.",
			Files: map[string]string{
				"internal/api/modules/amp/neo_runtime.go": `package amp
func (a *neoActor) leaseTool(call neoToolCall) { a.pendingTools[call.ID] = neoPendingTool{Name: call.Name} }
func (a *neoActor) handleToolResult(msg map[string]any) {
	run := neoPromoteToolRunOutput(mapValue(msg["run"]))
	a.storeToolResultMessage(msg, run)
	a.rebuildHistoryLocked()
	a.startInferenceFromHistory()
}`,
				"internal/api/modules/amp/web_local_inference.go": `package amp
func bridgeExecutorSocket(conn *websocket.Conn) { forwardRegisteredTool("web_search"); forwardToolResult(conn) }`,
				"internal/api/modules/amp/neo_runtime_test.go": `package amp
func TestWebSearchToolResultResumesInference(t *testing.T) {}`,
				"internal/api/modules/amp/neo_builtin_tools.go": `package amp
func neoSyntheticLocalToolSpec(name string) (neoToolSpec, bool) { return neoToolSpec{}, false }`,
				"internal/api/modules/amp/fallback_handlers.go": `package amp
func proxyWebSearch2(c *gin.Context) { c.Request.URL.Path = "/api/internal/webSearch2" }`,
				"internal/api/modules/amp/gemini_bridge.go": `package amp
func bridgeGeminiResponse() {}`,
			},
			RequiredGroups: [][]string{{"internal/api/modules/amp/neo_runtime.go"}, {"internal/api/modules/amp/web_local_inference.go"}},
			AllowedFiles:   []string{"internal/api/modules/amp/neo_runtime.go", "internal/api/modules/amp/web_local_inference.go", "internal/api/modules/amp/neo_runtime_test.go", "internal/api/modules/amp/fallback_handlers.go"},
			ForbiddenFiles: []string{"internal/api/modules/amp/gemini_bridge.go"},
		},
		{
			Name:  "exact local agent paths",
			Query: "Locate the exact local-agent session API and library files: apps/web/app/api/local-agent/sessions/route.ts, sessions/[id]/route.ts, heartbeat/route.ts, link/route.ts, apps/web/lib/local-agent-sessions.ts, and its test. Return all paths with line ranges.",
			Files: map[string]string{
				"apps/web/app/api/local-agent/sessions/route.ts":                `export async function POST(req: Request) { return createLocalAgentSession(req) }`,
				"apps/web/app/api/local-agent/sessions/[id]/route.ts":           `export async function GET(req: Request) { return getLocalAgentSession(req) }`,
				"apps/web/app/api/local-agent/sessions/[id]/heartbeat/route.ts": `export async function POST(req: Request) { return heartbeatLocalAgent(req) }`,
				"apps/web/app/api/local-agent/sessions/link/route.ts":           `export async function POST(req: Request) { return linkLocalAgent(req) }`,
				"apps/web/lib/local-agent-sessions.ts":                          `export async function createLocalAgentSession(req: Request) { return db.insert(localAgentSessions) }`,
				"apps/web/lib/local-agent-sessions.test.ts":                     `test("workspace isolation", async () => expect(await createLocalAgentSession(req)).toBeDefined())`,
				"apps/web/app/local/connect/page.tsx":                           `export default function ConnectPage() { return <LocalAgentConnect /> }`,
				"docs/local-agent.md":                                           `Local agents connect cloud sessions to a workstation.`,
			},
			RequiredGroups: [][]string{
				{"apps/web/app/api/local-agent/sessions/route.ts"},
				{"apps/web/app/api/local-agent/sessions/[id]/route.ts"},
				{"apps/web/app/api/local-agent/sessions/[id]/heartbeat/route.ts"},
				{"apps/web/app/api/local-agent/sessions/link/route.ts"},
				{"apps/web/lib/local-agent-sessions.ts"},
				{"apps/web/lib/local-agent-sessions.test.ts"},
			},
			AllowedFiles: []string{
				"apps/web/app/api/local-agent/sessions/route.ts",
				"apps/web/app/api/local-agent/sessions/[id]/route.ts",
				"apps/web/app/api/local-agent/sessions/[id]/heartbeat/route.ts",
				"apps/web/app/api/local-agent/sessions/link/route.ts",
				"apps/web/lib/local-agent-sessions.ts",
				"apps/web/lib/local-agent-sessions.test.ts",
			},
			ForbiddenFiles: []string{"docs/local-agent.md"},
		},
		{
			Name:  "auth and workspace boundaries",
			Query: "Find all implementation paths for local-agent bearer-token authentication, link-code validation, workspace authorization, proxy IP trust, and rate limiting. Return source files and line ranges, excluding docs.",
			Files: map[string]string{
				"packages/auth/src/server.ts": `export function getBearerToken(req: Request) { return req.headers.get("authorization") }
export async function requireWorkspaceAccess(userId: string, workspaceId: string) { return memberships.find(userId, workspaceId) }`,
				"apps/web/lib/local-agent-sessions.ts": `export async function validateLinkCode(code: string) { return timingSafeEqual(hash(code), row.linkCodeHash) }
export function trustedClientIP(req: Request) { return parseForwardedFor(req, TRUSTED_PROXY_HOPS) }`,
				"apps/web/lib/rate-limit.ts":                          `export async function enforceRateLimit(key: string) { return limiter.consume(key) }`,
				"apps/web/app/api/local-agent/sessions/link/route.ts": `export async function POST(req: Request) { await enforceRateLimit(trustedClientIP(req)); return validateLinkCode(await req.text()) }`,
				"apps/web/app/api/local-agent/sessions/[id]/route.ts": `export async function GET(req: Request) { const user = await requireSession(req); await requireWorkspaceAccess(user.id, params.workspaceId) }`,
				"apps/web/components/local-agent-card.tsx":            `export function LocalAgentCard() { return <div>Connected</div> }`,
				"docs/security.md":                                    `Bearer tokens and link codes protect local agents.`,
			},
			RequiredGroups: [][]string{{"packages/auth/src/server.ts"}, {"apps/web/lib/local-agent-sessions.ts"}, {"apps/web/lib/rate-limit.ts"}, {"apps/web/app/api/local-agent/sessions/link/route.ts"}, {"apps/web/app/api/local-agent/sessions/[id]/route.ts"}},
			AllowedFiles:   []string{"packages/auth/src/server.ts", "apps/web/lib/local-agent-sessions.ts", "apps/web/lib/rate-limit.ts", "apps/web/app/api/local-agent/sessions/link/route.ts", "apps/web/app/api/local-agent/sessions/[id]/route.ts"},
			ForbiddenFiles: []string{"apps/web/components/local-agent-card.tsx", "docs/security.md"},
		},
		{
			Name:  "schema migration completeness",
			Query: "Locate packages/db/src/schema/localAgentSessions.ts, packages/db/src/schema/index.ts, packages/db/drizzle/0000_local_agent_sessions.sql, packages/db/drizzle/meta/0000_snapshot.json, and packages/db/drizzle/meta/_journal.json. Find the schema, migration, generated snapshot, indexes, foreign keys, enums, and timestamps. Return all exact paths and line ranges.",
			Files: map[string]string{
				"packages/db/src/schema/localAgentSessions.ts": `export const localAgentSessions = pgTable("local_agent_sessions", { id: uuid().primaryKey(), workspaceId: uuid().references(() => workspaces.id), status: localAgentStatus(), expiresAt: timestamp(), createdAt: timestamp() }, table => [index("local_agent_workspace_idx").on(table.workspaceId)])`,
				"packages/db/src/schema/index.ts":              `export * from "./localAgentSessions"`,
				"packages/db/drizzle/0000_local_agent_sessions.sql": `CREATE TYPE local_agent_status AS ENUM ('pending', 'connected', 'expired');
CREATE TABLE local_agent_sessions (id uuid PRIMARY KEY, workspace_id uuid REFERENCES workspaces(id), expires_at timestamp, created_at timestamp);
CREATE INDEX local_agent_workspace_idx ON local_agent_sessions(workspace_id);`,
				"packages/db/drizzle/meta/0000_snapshot.json": `{"tables":{"local_agent_sessions":{"indexes":{"local_agent_workspace_idx":{}},"foreignKeys":{"workspace_id":{}}}},"enums":{"local_agent_status":["pending","connected","expired"]}}`,
				"packages/db/drizzle/meta/_journal.json":      `{"entries":[{"idx":0,"tag":"0000_local_agent_sessions"}]}`,
				"packages/db/src/client.ts":                   `export const db = drizzle(pool)`,
				"apps/web/lib/local-agent-sessions.ts":        `import { localAgentSessions } from "@repo/db/schema"`,
			},
			RequiredGroups: [][]string{{"packages/db/src/schema/localAgentSessions.ts"}, {"packages/db/src/schema/index.ts"}, {"packages/db/drizzle/0000_local_agent_sessions.sql"}, {"packages/db/drizzle/meta/0000_snapshot.json"}, {"packages/db/drizzle/meta/_journal.json"}},
			AllowedFiles:   []string{"packages/db/src/schema/localAgentSessions.ts", "packages/db/src/schema/index.ts", "packages/db/drizzle/0000_local_agent_sessions.sql", "packages/db/drizzle/meta/0000_snapshot.json", "packages/db/drizzle/meta/_journal.json"},
		},
		{
			Name:  "telemetry pii redaction",
			Query: "Where is personally identifiable information scrubbed, redacted, sanitized, or masked from telemetry? Find the implementation, configuration, and tests. Return source paths and line ranges.",
			Files: map[string]string{
				"telemetry/redaction.py": `PII_KEYS = {"email", "phone", "ip_address"}
def scrub_pii(payload):
    return {key: "[REDACTED]" if key in PII_KEYS else value for key, value in payload.items()}`,
				"telemetry/exporter.py": `from telemetry.redaction import scrub_pii
def export_event(event): transport.send(scrub_pii(event))`,
				"config/telemetry.py": `TELEMETRY_REDACTION_ENABLED = env.bool("TELEMETRY_REDACTION_ENABLED", default=True)`,
				"tests/telemetry/test_redaction.py": `def test_scrub_pii_masks_email_phone_and_ip():
    assert scrub_pii({"email":"a@b.com"})["email"] == "[REDACTED]"`,
				"frontend/components/privacy.tsx": `export function PrivacyNotice() { return <p>We protect PII</p> }`,
				"docs/privacy.md":                 `Telemetry is sanitized before export.`,
			},
			RequiredGroups: [][]string{{"telemetry/redaction.py"}, {"telemetry/exporter.py"}, {"config/telemetry.py"}, {"tests/telemetry/test_redaction.py"}},
			AllowedFiles:   []string{"telemetry/redaction.py", "telemetry/exporter.py", "config/telemetry.py", "tests/telemetry/test_redaction.py"},
			ForbiddenFiles: []string{"frontend/components/privacy.tsx", "docs/privacy.md"},
		},
		{
			Name:  "strategy lifecycle",
			Query: "Find the complete post_launch_2h strategy flow: candidate entry filters, quarantine, position opening, stop loss, take profit, trailing stop, max hold, and close persistence. Return files, symbols, and line ranges.",
			Files: map[string]string{
				"arena/strategy/post_launch_2h.py": `def evaluate_candidate(candidate):
    if candidate.quarantined or candidate.score < MIN_SCORE: return Reject()
    return Entry(size=position_size(candidate))`,
				"arena/trading_engine.py": `async def open_position(entry): return await executor.execute_buy(entry)
async def manage_position(position):
    if hit_stop_loss(position) or hit_take_profit(position) or trailing_stop(position) or max_hold_elapsed(position):
        return await close_position(position)`,
				"arena/risk.py": `def hit_stop_loss(position): return position.pnl_pct <= -STOP_LOSS
def hit_take_profit(position): return position.pnl_pct >= TAKE_PROFIT
def trailing_stop(position): return position.price <= position.trailing_high * TRAILING_FACTOR
def max_hold_elapsed(position): return utcnow() - position.opened_at >= MAX_HOLD`,
				"data_collection/workers/strategy_paper_trader.py": `async def close_position(position):
    fill = await executor.execute_sell(position.quantity)
    await store_strategy_paper_trade(position, fill, close_reason=position.exit_reason, closed_at=utcnow())`,
				"tests/arena/test_post_launch_2h.py": `def test_quarantine_and_all_exit_paths(): pass`,
				"frontend/strategy-dashboard.tsx":    `export function StrategyCard() { return <div>post_launch_2h</div> }`,
				"docs/strategies.md":                 `The post-launch strategy manages entries and exits.`,
			},
			RequiredGroups: [][]string{{"arena/strategy/post_launch_2h.py"}, {"arena/trading_engine.py"}, {"arena/risk.py"}, {"data_collection/workers/strategy_paper_trader.py"}},
			AllowedFiles:   []string{"arena/strategy/post_launch_2h.py", "arena/trading_engine.py", "arena/risk.py", "data_collection/workers/strategy_paper_trader.py", "tests/arena/test_post_launch_2h.py"},
			ForbiddenFiles: []string{"frontend/strategy-dashboard.tsx", "docs/strategies.md"},
		},
		{
			Name:  "changed hunk discovery",
			Query: "Review only the uncommitted local-agent changes in packages/npx/bin/cairn.mjs, packages/npx/bin/env.mjs, and packages/npx/bin/env.test.mjs. Locate the relevant implementation and tests; do not include unrelated package files.",
			Files: map[string]string{
				"packages/npx/bin/cairn.mjs":     `export async function localAgent(args) { const env = resolveLocalAgentEnv(args); return connectAgent(env) }`,
				"packages/npx/bin/env.mjs":       `export function resolveLocalAgentEnv(args) { return { cloudUrl: args.cloudUrl ?? process.env.CAIRN_CLOUD_URL } }`,
				"packages/npx/bin/env.test.mjs":  `test("CLI cloud URL overrides environment", () => expect(resolveLocalAgentEnv(args).cloudUrl).toBe("https://test"))`,
				"packages/npx/bin/commands.mjs":  `export const commands = ["login", "logout", "status"]`,
				"packages/npx/package.json":      `{"name":"@cairn/npx","bin":{"cairn":"bin/cairn.mjs"}}`,
				"packages/mcp/bin/cairn-mcp.mjs": `export async function startMCP() {}`,
			},
			RequiredGroups: [][]string{{"packages/npx/bin/cairn.mjs"}, {"packages/npx/bin/env.mjs"}, {"packages/npx/bin/env.test.mjs"}},
			AllowedFiles:   []string{"packages/npx/bin/cairn.mjs", "packages/npx/bin/env.mjs", "packages/npx/bin/env.test.mjs"},
			ForbiddenFiles: []string{"packages/npx/bin/commands.mjs", "packages/npx/package.json", "packages/mcp/bin/cairn-mcp.mjs"},
		},
	}
}
