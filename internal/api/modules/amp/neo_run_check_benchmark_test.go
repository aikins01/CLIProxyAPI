package amp

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
)

type neoRunCheckBenchmarkCase struct {
	Name                  string
	CheckName             string
	CheckContent          string
	Diff                  string
	Files                 []string
	ExpectedMinIssues     int
	ExpectedMaxIssues     int
	RequiredFile          string
	ExpectedSeverity      string
	RequiredKeywordGroups [][]string
}

type neoRunCheckBenchmarkCandidate struct {
	Name   string
	Route  neoModelRoute
	Effort string
}

func TestNeoRunCheckBenchmarkFixtures(t *testing.T) {
	cases := neoRunCheckBenchmarkCases()
	if len(cases) < 12 {
		t.Fatalf("run_check benchmark cases = %d, want at least 12", len(cases))
	}
	seenPositive := false
	seenClean := false
	requiredCases := map[string]bool{
		"canonical private state missing invariant comment":    true,
		"canonical private state documents invariant":          true,
		"satisfier repeats canonicalization of unchanged term": true,
		"satisfier normalizes rebound term at method boundary": true,
	}
	for _, tc := range cases {
		delete(requiredCases, tc.Name)
		t.Run(tc.Name, func(t *testing.T) {
			if tc.CheckName == "" || strings.TrimSpace(tc.CheckContent) == "" || strings.TrimSpace(tc.Diff) == "" || len(tc.Files) == 0 {
				t.Fatalf("incomplete fixture: %#v", tc)
			}
			if tc.ExpectedMinIssues < 0 || tc.ExpectedMaxIssues < tc.ExpectedMinIssues {
				t.Fatalf("invalid expected issue range: %#v", tc)
			}
			if tc.ExpectedMinIssues > 0 {
				seenPositive = true
				if tc.RequiredFile == "" || len(tc.RequiredKeywordGroups) == 0 {
					t.Fatalf("positive fixture missing rubric: %#v", tc)
				}
			} else if tc.ExpectedMaxIssues == 0 {
				seenClean = true
			}
		})
	}
	if !seenPositive || !seenClean {
		t.Fatalf("fixtures positive=%v clean=%v, want both", seenPositive, seenClean)
	}
	if len(requiredCases) != 0 {
		t.Fatalf("run_check benchmark missing PR 4849 fixture cases: %#v", requiredCases)
	}
}

func TestNeoRunCheckDefaultBenchmarkCandidates(t *testing.T) {
	t.Setenv("AMP_RUN_CHECK_MODEL_BENCHMARK_CANDIDATES", "")
	candidates := neoRunCheckBenchmarkCandidates(t)
	got := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		effort := candidate.Effort
		if effort == "" {
			effort = "default"
		}
		got = append(got, candidate.Route.Provider+"/"+candidate.Route.Model+"@"+effort)
	}
	want := []string{
		"openai/gpt-5.5@medium",
		"openai/gpt-5.5@high",
		"openai/gpt-5.6-sol@low",
		"openai/gpt-5.6-sol@medium",
		"openai/gpt-5.6-sol@high",
		"openai/gpt-5.6-terra@low",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("default run_check benchmark candidates = %#v, want %#v", got, want)
	}
}

func TestNeoRunCheckBenchmarkDefaultRepetitions(t *testing.T) {
	t.Setenv("AMP_RUN_CHECK_MODEL_BENCHMARK_REPS", "")
	if got := neoRunCheckBenchmarkRepetitions(t); got != 3 {
		t.Fatalf("default run_check benchmark repetitions = %d, want 3", got)
	}
}

func TestNeoRunCheckBenchmarkScore(t *testing.T) {
	tc := neoRunCheckBenchmarkCase{
		Diff:              "diff --git a/internal/router/local.go b/internal/router/local.go\n--- a/internal/router/local.go\n+++ b/internal/router/local.go\n@@ -1 +1 @@\n-old\n+new\n",
		Files:             []string{"internal/router/local.go"},
		ExpectedMinIssues: 1,
		ExpectedMaxIssues: 1,
		RequiredFile:      "internal/router/local.go",
		ExpectedSeverity:  "low",
		RequiredKeywordGroups: [][]string{
			{"near-miss", "false positive"},
			{"test", "coverage"},
		},
	}
	result := map[string]any{
		"status":       "completed",
		"coveredFiles": []any{"internal/router/local.go"},
		"coveredHunks": []any{"internal/router/local.go@@+1,1"},
		"issues": []any{map[string]any{
			"severity": "low",
			"file":     "internal/router/local.go",
			"problem":  "blocker: broad matching lacks near-miss coverage",
			"why":      "A false positive can route the wrong request.",
			"fix":      "Add a focused test for the boundary.",
		}},
	}
	passed, failures := neoRunCheckBenchmarkScore(result, tc)
	if !passed || len(failures) != 0 {
		t.Fatalf("score passed=%v failures=%#v", passed, failures)
	}
	mapValue(arrayValue(result["issues"])[0])["severity"] = "medium"
	passed, failures = neoRunCheckBenchmarkScore(result, tc)
	if passed || len(failures) == 0 {
		t.Fatalf("score accepted promoted severity")
	}
	mapValue(arrayValue(result["issues"])[0])["severity"] = "low"
	result["issues"] = []any{}
	passed, failures = neoRunCheckBenchmarkScore(result, tc)
	if passed || len(failures) == 0 {
		t.Fatalf("score accepted missing issue")
	}
	tc.ExpectedMaxIssues = 2
	result["issues"] = []any{
		map[string]any{"file": "internal/router/local.go", "problem": "follow-up: broad matching lacks near-miss handling"},
		map[string]any{"file": "internal/router/local.go", "problem": "follow-up: add test coverage"},
	}
	passed, failures = neoRunCheckBenchmarkScore(result, tc)
	if passed || len(failures) == 0 {
		t.Fatalf("score combined separate partial issues")
	}
}

func TestNeoRunCheckSyntheticModelBenchmark(t *testing.T) {
	if !neoReadThreadTruthyEnv("AMP_RUN_CHECK_MODEL_BENCHMARK") {
		t.Skip("set AMP_RUN_CHECK_MODEL_BENCHMARK=1 to run live run_check model benchmark")
	}
	candidates := neoRunCheckBenchmarkCandidates(t)
	cases := neoRunCheckSelectedBenchmarkCases(t)
	repetitions := neoRunCheckBenchmarkRepetitions(t)
	strict := neoReadThreadTruthyEnv("AMP_RUN_CHECK_MODEL_BENCHMARK_STRICT")
	failedRuns := 0
	for caseIndex, tc := range cases {
		for rep := 1; rep <= repetitions; rep++ {
			candidateOffset := (caseIndex + rep - 1) % len(candidates)
			for candidateIndex := range candidates {
				candidate := candidates[(candidateIndex+candidateOffset)%len(candidates)]
				tc := tc
				rep := rep
				t.Run(fmt.Sprintf("%s/%s/%d", candidate.Name, neoReadThreadBenchmarkSafeName(tc.Name), rep), func(t *testing.T) {
					result := neoRunCheckRunSyntheticModelBenchmark(t, candidate, tc, rep)
					raw, _ := json.Marshal(result)
					t.Log(string(raw))
					if !boolValue(result["passed"]) {
						failedRuns++
						if strict {
							t.Fatalf("benchmark miss: %s", string(raw))
						}
					}
				})
			}
		}
	}
	if failedRuns != 0 {
		t.Fatalf("%d live run_check benchmark runs failed", failedRuns)
	}
}

func neoRunCheckRunSyntheticModelBenchmark(t *testing.T, candidate neoRunCheckBenchmarkCandidate, tc neoRunCheckBenchmarkCase, rep int) map[string]any {
	t.Helper()
	rt := newNeoRuntime(neoRunCheckBenchmarkConfig(t))
	frontmatter := map[string]any{"name": tc.CheckName}
	if tc.ExpectedSeverity != "" {
		frontmatter["severity-default"] = tc.ExpectedSeverity
	}
	input := map[string]any{
		"checkName":       tc.CheckName,
		"checkURI":        "file:///benchmark/checks/" + tc.CheckName + ".md",
		"checkContent":    tc.CheckContent,
		"frontmatter":     frontmatter,
		"diffDescription": "synthetic working tree diff",
		"files":           stringArrayValue(tc.Files),
		"instructions":    "Evaluate only added or modified lines. The immutable review diff snapshot and all relevant context are embedded below.",
	}
	input, err := neoPrepareRunCheckSnapshotInput(input, neoRunCheckBenchmarkSnapshot(tc))
	if err != nil {
		t.Fatalf("prepare immutable snapshot: %v", err)
	}
	inputText := neoSubagentInputText("run_check", input)
	systemPrompt := strings.NewReplacer(
		"{{WORKING_DIR}}", "/benchmark/repository",
		"{{WORKSPACE_ROOT}}", "/benchmark/repository",
	).Replace(neoRunCheckSubagentPrompt) + "\n\nFor this benchmark, the immutable diff snapshot and relevant context are embedded in the user request. Do not call tools."
	settings := map[string]any{}
	if candidate.Effort != "" {
		settings["reasoning.effort"] = candidate.Effort
	}
	route := candidate.Route
	request := neoInferenceRequest{
		ActorID:              "actor-run-check-benchmark",
		ThreadID:             "T-019f5000-0000-7000-8000-000000000001",
		MessageID:            newNeoMessageID(),
		AgentMode:            "review",
		ReasoningEffort:      candidate.Effort,
		Settings:             settings,
		History:              []neoHistoryMessage{{Role: "user", Text: inputText}},
		Environment:          map[string]any{"workingDirectory": "/benchmark/repository", "workspaceRoot": "/benchmark/repository"},
		ModelRouteOverride:   &route,
		SystemPromptOverride: systemPrompt,
	}
	started := time.Now()
	inference, err := inferNeoLocalStream(rt, request, func(neoInferenceDelta) {})
	duration := time.Since(started)
	structured := neoRunCheckResultFromText(input, inference.Text)
	passed, failures := neoRunCheckBenchmarkScore(structured, tc)
	result := map[string]any{
		"candidate":      candidate.Name,
		"provider":       candidate.Route.Provider,
		"model":          candidate.Route.Model,
		"effort":         candidate.Effort,
		"case":           tc.Name,
		"rep":            rep,
		"passed":         passed && err == nil,
		"durationMillis": duration.Milliseconds(),
		"inputTokens":    neoRunCheckBenchmarkUsageInt(inference.Usage, "inputTokens", "input_tokens", "prompt_tokens", "promptTokenCount"),
		"outputTokens":   neoRunCheckBenchmarkUsageInt(inference.Usage, "outputTokens", "output_tokens", "completion_tokens", "candidatesTokenCount"),
		"issueCount":     len(arrayValue(structured["issues"])),
		"status":         stringValue(structured["status"]),
		"failures":       failures,
		"output":         neoClipRunes(inference.Text, 1800),
	}
	if err != nil {
		result["error"] = err.Error()
	}
	return result
}

func neoRunCheckBenchmarkSnapshot(tc neoRunCheckBenchmarkCase) *neoReviewDiffSnapshot {
	snapshot := &neoReviewDiffSnapshot{Files: append([]string(nil), tc.Files...), Diffs: map[string]string{}}
	for _, filename := range tc.Files {
		startMarker := "diff --git a/" + filename + " b/" + filename
		start := strings.Index(tc.Diff, startMarker)
		if start < 0 {
			continue
		}
		end := len(tc.Diff)
		if next := strings.Index(tc.Diff[start+len(startMarker):], "\ndiff --git "); next >= 0 {
			end = start + len(startMarker) + next
		}
		section := strings.TrimSpace(tc.Diff[start:end])
		snapshot.Diffs[filename] = section
		snapshot.Hunks = append(snapshot.Hunks, neoReviewDiffHunks(filename, section)...)
	}
	return snapshot
}

func TestNeoRunCheckBenchmarkSnapshotDoesNotDependOnFileOrder(t *testing.T) {
	tc := neoRunCheckBenchmarkCase{
		Diff:  "diff --git a/first.go b/first.go\n--- a/first.go\n+++ b/first.go\n@@ -1 +1 @@\n-old first\n+new first\ndiff --git a/second.go b/second.go\n--- a/second.go\n+++ b/second.go\n@@ -1 +1 @@\n-old second\n+new second\n",
		Files: []string{"second.go", "first.go"},
	}
	snapshot := neoRunCheckBenchmarkSnapshot(tc)
	if strings.Contains(snapshot.Diffs["first.go"], "second.go") {
		t.Fatalf("first file snapshot includes second file: %s", snapshot.Diffs["first.go"])
	}
	if !strings.Contains(snapshot.Diffs["second.go"], "+new second") {
		t.Fatalf("second file snapshot = %q", snapshot.Diffs["second.go"])
	}
}

func neoRunCheckBenchmarkScore(result map[string]any, tc neoRunCheckBenchmarkCase) (bool, []string) {
	failures := make([]string, 0)
	if stringValue(result["status"]) != "completed" {
		failures = append(failures, "status is not completed")
	}
	expectedSnapshot := neoRunCheckBenchmarkSnapshot(tc)
	coveredFiles, filesOK := neoRunCheckStringSlice(result["coveredFiles"])
	if !filesOK || !neoReviewSameStringSet(coveredFiles, expectedSnapshot.Files) {
		failures = append(failures, "coveredFiles does not match the immutable snapshot")
	}
	expectedHunks := make([]string, 0, len(expectedSnapshot.Hunks))
	for _, hunk := range expectedSnapshot.Hunks {
		expectedHunks = append(expectedHunks, hunk.ID)
	}
	coveredHunks, hunksOK := neoRunCheckStringSlice(result["coveredHunks"])
	if !hunksOK || !neoReviewSameStringSet(coveredHunks, expectedHunks) {
		failures = append(failures, "coveredHunks does not match the immutable snapshot")
	}
	issues := arrayValue(result["issues"])
	if len(issues) < tc.ExpectedMinIssues || len(issues) > tc.ExpectedMaxIssues {
		failures = append(failures, fmt.Sprintf("issue count %d outside %d..%d", len(issues), tc.ExpectedMinIssues, tc.ExpectedMaxIssues))
	}
	rubricIssues := issues
	if tc.RequiredFile != "" {
		rubricIssues = make([]any, 0, len(issues))
		for _, raw := range issues {
			if stringValue(mapValue(raw)["file"]) == tc.RequiredFile {
				rubricIssues = append(rubricIssues, raw)
			}
		}
		if len(rubricIssues) == 0 {
			failures = append(failures, "missing issue for "+tc.RequiredFile)
		}
	}
	if len(tc.RequiredKeywordGroups) > 0 || tc.ExpectedSeverity != "" {
		matched := false
		bestMissing := make([]string, 0, len(tc.RequiredKeywordGroups)+1)
		if tc.ExpectedSeverity != "" {
			bestMissing = append(bestMissing, "severity does not match "+tc.ExpectedSeverity)
		}
		for _, group := range tc.RequiredKeywordGroups {
			bestMissing = append(bestMissing, "missing one of: "+strings.Join(group, " | "))
		}
		for _, issue := range rubricIssues {
			issueMap := mapValue(issue)
			rawIssue, _ := json.Marshal(issue)
			normalized := strings.ToLower(string(rawIssue))
			missing := make([]string, 0, len(tc.RequiredKeywordGroups)+1)
			if tc.ExpectedSeverity != "" && !strings.EqualFold(stringValue(issueMap["severity"]), tc.ExpectedSeverity) {
				missing = append(missing, fmt.Sprintf("severity %q, want %q", stringValue(issueMap["severity"]), tc.ExpectedSeverity))
			}
			for _, group := range tc.RequiredKeywordGroups {
				found := false
				for _, keyword := range group {
					if strings.Contains(normalized, strings.ToLower(keyword)) {
						found = true
						break
					}
				}
				if !found {
					missing = append(missing, "missing one of: "+strings.Join(group, " | "))
				}
			}
			if len(missing) == 0 {
				matched = true
				break
			}
			if len(missing) < len(bestMissing) {
				bestMissing = missing
			}
		}
		if !matched {
			failures = append(failures, bestMissing...)
		}
	}
	return len(failures) == 0, failures
}

func neoRunCheckBenchmarkCandidates(t *testing.T) []neoRunCheckBenchmarkCandidate {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv("AMP_RUN_CHECK_MODEL_BENCHMARK_CANDIDATES"))
	if raw == "" {
		return []neoRunCheckBenchmarkCandidate{
			{Name: "gpt-5.5-medium", Route: neoModelRoute{Provider: "openai", Model: "gpt-5.5"}, Effort: "medium"},
			{Name: "gpt-5.5-high", Route: neoModelRoute{Provider: "openai", Model: "gpt-5.5"}, Effort: "high"},
			{Name: "gpt-5.6-sol-low", Route: neoModelRoute{Provider: "openai", Model: "gpt-5.6-sol"}, Effort: "low"},
			{Name: "gpt-5.6-sol-medium", Route: neoModelRoute{Provider: "openai", Model: "gpt-5.6-sol"}, Effort: "medium"},
			{Name: "gpt-5.6-sol-high", Route: neoModelRoute{Provider: "openai", Model: "gpt-5.6-sol"}, Effort: "high"},
			{Name: "gpt-5.6-terra-low", Route: neoModelRoute{Provider: "openai", Model: "gpt-5.6-terra"}, Effort: "low"},
		}
	}
	candidates := make([]neoRunCheckBenchmarkCandidate, 0)
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		routeText := item
		effort := "low"
		if left, right, ok := strings.Cut(item, "@"); ok {
			routeText = strings.TrimSpace(left)
			effort = strings.TrimSpace(right)
			if effort == "default" {
				effort = ""
			}
		}
		route := parseNeoModelRoute(routeText)
		if route.Model == "" {
			t.Fatalf("invalid run_check benchmark candidate %q", item)
		}
		if route.Provider == "" {
			route.Provider = providerForNeoModel(route.Model)
		}
		nameEffort := effort
		if nameEffort == "" {
			nameEffort = "default"
		}
		candidates = append(candidates, neoRunCheckBenchmarkCandidate{
			Name:   neoReadThreadBenchmarkSafeName(route.Provider + "-" + route.Model + "-" + nameEffort),
			Route:  route,
			Effort: effort,
		})
	}
	if len(candidates) == 0 {
		t.Fatal("AMP_RUN_CHECK_MODEL_BENCHMARK_CANDIDATES did not contain any candidates")
	}
	return candidates
}

func neoRunCheckSelectedBenchmarkCases(t *testing.T) []neoRunCheckBenchmarkCase {
	t.Helper()
	cases := neoRunCheckBenchmarkCases()
	raw := strings.TrimSpace(os.Getenv("AMP_RUN_CHECK_MODEL_BENCHMARK_CASES"))
	if raw == "" {
		return cases
	}
	wanted := map[string]bool{}
	for _, name := range strings.Split(raw, ",") {
		if name = strings.TrimSpace(name); name != "" {
			wanted[name] = true
		}
	}
	selected := make([]neoRunCheckBenchmarkCase, 0, len(wanted))
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
		slices.Sort(unknown)
		t.Fatalf("unknown run_check benchmark cases: %v", unknown)
	}
	return selected
}

func neoRunCheckBenchmarkConfig(t *testing.T) *config.Config {
	t.Helper()
	rawURL := strings.TrimSpace(os.Getenv("AMP_RUN_CHECK_MODEL_BENCHMARK_URL"))
	if rawURL == "" {
		rawURL = "http://127.0.0.1:8317"
	}
	parsed, err := url.Parse(rawURL)
	if err != nil {
		t.Fatalf("parse AMP_RUN_CHECK_MODEL_BENCHMARK_URL: %v", err)
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
		t.Fatalf("invalid AMP_RUN_CHECK_MODEL_BENCHMARK_URL %q", rawURL)
	}
	cfg := &config.Config{Host: parsed.Hostname(), Port: port}
	if parsed.Scheme == "https" {
		cfg.TLS.Enable = true
	}
	if key := strings.TrimSpace(os.Getenv("AMP_RUN_CHECK_MODEL_BENCHMARK_API_KEY")); key != "" {
		cfg.APIKeys = []string{key}
	}
	return cfg
}

func neoRunCheckBenchmarkRepetitions(t *testing.T) int {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv("AMP_RUN_CHECK_MODEL_BENCHMARK_REPS"))
	if raw == "" {
		return 3
	}
	repetitions, err := strconv.Atoi(raw)
	if err != nil || repetitions <= 0 {
		t.Fatalf("AMP_RUN_CHECK_MODEL_BENCHMARK_REPS = %q, want positive integer", raw)
	}
	return repetitions
}

func neoRunCheckBenchmarkUsageInt(usage map[string]any, keys ...string) int {
	for _, key := range keys {
		if value := numberFrom(usage[key]); value > 0 {
			return value
		}
	}
	return 0
}

func neoRunCheckBenchmarkCases() []neoRunCheckBenchmarkCase {
	classifierCheck := `Only apply this check when changed lines add or modify matching, routing, parsing, or coercion logic. Broad substring or prefix matching requires behavior-matrix tests for accepted inputs, rejected inputs, boundaries, and near-miss false positives. A bug fix must test the exact broken branch. Point each finding to the changed classifier line, not the test file. Report a blocker only when a concrete untested input can cause current incorrect routing; otherwise report a follow-up. Start every problem with blocker: or follow-up:.`
	regressionCheck := `Only apply this check when the diff changes tests, error handling, credentials, or dependencies. Report new empty or log-only error handling that converts a hard failure into silent success. Do not report unchanged pre-existing code. Start every problem with blocker: or follow-up:.`
	artifactCheck := `Apply when changed code emits a command or remediation another consumer will act on. Verify the suggestion preserves the original selector, project, scope, path, or resource id, and require a test through the downstream parser or consumer when practical. A string-only assertion is insufficient when it does not prove the command reaches the intended final state. Start every problem with blocker: or follow-up:.`
	conventionCheck := `Apply when changed code has nearby repository conventions. Compare surrounding code and report concrete divergence in naming, logging, formatting, or scope. Do not report preferences without an established convention. Start every problem with blocker: or follow-up:.`
	invariantCommentCheck := `Apply when changed code adds or materially repurposes private state used by multiple methods or boundary consumers. Report a missing declaration comment only when the name and type do not expose a correctness-critical invariant such as canonical identity, the diff proves consumers rely on that invariant, and comparable nearby state is documented. Request only the shortest comment stating the invariant. Do not request routine private-field documentation or implementation narration. Start every problem with blocker: or follow-up:.`
	duplicateNormalizationCheck := `Apply when changed logic repeats normalization, validation, or conversion. Report it only when exact data flow proves the later operation consumes the already-normalized unchanged value without mutation, transformation, or rebinding, and the duplication obscures where the invariant is established. Passing the unchanged value through a call chain does not exempt the repeated operation. Do not report query-time versus storage-time normalization, distinct local copies, or normalization after mutation, transformation, or rebinding. Start every problem with blocker: or follow-up:.`
	return []neoRunCheckBenchmarkCase{
		{
			Name:              "classifier broad route missing near miss",
			CheckName:         "classifier-detector-matrix",
			CheckContent:      classifierCheck,
			Files:             []string{"internal/router/local.go", "internal/router/local_test.go"},
			ExpectedMinIssues: 1,
			ExpectedMaxIssues: 1,
			RequiredFile:      "internal/router/local.go",
			RequiredKeywordGroups: [][]string{
				{"near-miss", "false positive", "boundary"},
				{"test", "coverage"},
				{"gateway"},
			},
			Diff: `diff --git a/internal/router/local.go b/internal/router/local.go
index 1111111..2222222 100644
--- a/internal/router/local.go
+++ b/internal/router/local.go
@@ -1,4 +1,7 @@
 package router
 
 import "strings"
 
+func shouldBridge(path string) bool {
+    return strings.Contains(path, "/gateway/")
+}
diff --git a/internal/router/local_test.go b/internal/router/local_test.go
index 3333333..4444444 100644
--- a/internal/router/local_test.go
+++ b/internal/router/local_test.go
@@ -4,1 +4,4 @@ func TestShouldBridge(t *testing.T) {
+    if !shouldBridge("/api/gateway/thread") {
+        t.Fatal("expected gateway path")
+    }
 }`,
		},
		{
			Name:              "classifier exact route covered",
			CheckName:         "classifier-detector-matrix",
			CheckContent:      classifierCheck,
			Files:             []string{"internal/router/local.go", "internal/router/local_test.go"},
			ExpectedMaxIssues: 0,
			Diff: `diff --git a/internal/router/local.go b/internal/router/local.go
index 1111111..2222222 100644
--- a/internal/router/local.go
+++ b/internal/router/local.go
@@ -1,4 +1,7 @@
 package router
 
 import "strings"
 
+func shouldBridge(path string) bool {
+    return path == "/gateway" || strings.HasPrefix(path, "/gateway/")
+}
diff --git a/internal/router/local_test.go b/internal/router/local_test.go
index 3333333..4444444 100644
--- a/internal/router/local_test.go
+++ b/internal/router/local_test.go
@@ -4,1 +4,13 @@ func TestShouldBridge(t *testing.T) {
+    cases := map[string]bool{
+        "/gateway": true,
+        "/gateway/": true,
+        "/gateway/thread": true,
+        "/gatewayx": false,
+        "/gatewayish/thread": false,
+        "/api/not-gateway/thread": false,
+        "": false,
+    }
+    for path, want := range cases {
+        if got := shouldBridge(path); got != want { t.Fatalf("%s = %v", path, got) }
+    }
 }`,
		},
		{
			Name:              "regression swallowed persistence error",
			CheckName:         "regression-safety",
			CheckContent:      regressionCheck,
			Files:             []string{"internal/sync/persist.go"},
			ExpectedMinIssues: 1,
			ExpectedMaxIssues: 1,
			RequiredFile:      "internal/sync/persist.go",
			RequiredKeywordGroups: [][]string{
				{"swallow", "silent", "returns nil", "returned as nil", "nil return", "ignored", "reported as success", "apparent success"},
				{"error", "failure"},
				{"blocker:"},
			},
			Diff: `diff --git a/internal/sync/persist.go b/internal/sync/persist.go
index 1111111..2222222 100644
--- a/internal/sync/persist.go
+++ b/internal/sync/persist.go
@@ -20,5 +20,6 @@ func persistSnapshot(snapshot Snapshot) error {
     if err := store.Save(snapshot); err != nil {
-        return fmt.Errorf("save snapshot: %w", err)
+        log.Printf("save snapshot failed: %v", err)
+        return nil
     }
     return nil
 }`,
		},
		{
			Name:              "regression wrapped persistence error",
			CheckName:         "regression-safety",
			CheckContent:      regressionCheck,
			Files:             []string{"internal/sync/persist.go"},
			ExpectedMaxIssues: 0,
			Diff: `diff --git a/internal/sync/persist.go b/internal/sync/persist.go
index 1111111..2222222 100644
--- a/internal/sync/persist.go
+++ b/internal/sync/persist.go
@@ -20,5 +20,5 @@ func persistSnapshot(snapshot Snapshot) error {
     if err := store.Save(snapshot); err != nil {
-        return err
+        return fmt.Errorf("save snapshot: %w", err)
     }
     return nil
 }`,
		},
		{
			Name:              "artifact remediation drops project",
			CheckName:         "generated-artifact-consumer-contract",
			CheckContent:      artifactCheck,
			Files:             []string{"internal/doctor/remediation.go", "internal/doctor/remediation_test.go"},
			ExpectedMinIssues: 1,
			ExpectedMaxIssues: 2,
			RequiredFile:      "internal/doctor/remediation.go",
			RequiredKeywordGroups: [][]string{
				{"--project", "project"},
				{"intent", "target", "scope", "selector", "routing", "context"},
				{"consumer", "parser", "execute", "end-to-end"},
			},
			Diff: `diff --git a/internal/doctor/remediation.go b/internal/doctor/remediation.go
index 1111111..2222222 100644
--- a/internal/doctor/remediation.go
+++ b/internal/doctor/remediation.go
@@ -12,2 +12,2 @@ func repairHint(project string) string {
-    return fmt.Sprintf("tool repair --project %s", shellquote(project))
+    return "tool repair"
 }
diff --git a/internal/doctor/remediation_test.go b/internal/doctor/remediation_test.go
index 3333333..4444444 100644
--- a/internal/doctor/remediation_test.go
+++ b/internal/doctor/remediation_test.go
@@ -8,2 +8,2 @@ func TestRepairHint(t *testing.T) {
-    require.Equal(t, "tool repair --project alpha", repairHint("alpha"))
+    require.Contains(t, repairHint("alpha"), "tool repair")
 }`,
		},
		{
			Name:              "artifact remediation preserves project",
			CheckName:         "generated-artifact-consumer-contract",
			CheckContent:      artifactCheck,
			Files:             []string{"internal/doctor/remediation.go", "internal/doctor/remediation_test.go"},
			ExpectedMaxIssues: 0,
			Diff: `diff --git a/internal/doctor/remediation.go b/internal/doctor/remediation.go
index 1111111..2222222 100644
--- a/internal/doctor/remediation.go
+++ b/internal/doctor/remediation.go
@@ -12,2 +12,2 @@ func repairHint(project string) string {
-    return "tool repair"
+    return fmt.Sprintf("tool repair --project %s", shellquote(project))
 }
diff --git a/internal/doctor/remediation_test.go b/internal/doctor/remediation_test.go
index 3333333..4444444 100644
--- a/internal/doctor/remediation_test.go
+++ b/internal/doctor/remediation_test.go
@@ -8,1 +8,5 @@ func TestRepairHint(t *testing.T) {
+    args := parseCommand(repairHint("alpha"))
+    state := runRepair(args)
+    require.Equal(t, "alpha", state.RepairedProject)
+    require.False(t, state.RepairedAllProjects)
 }`,
		},
		{
			Name:              "unchanged swallowed error stays out of scope",
			CheckName:         "regression-safety",
			CheckContent:      regressionCheck,
			Files:             []string{"internal/cache/cache.go"},
			ExpectedMaxIssues: 0,
			Diff: `diff --git a/internal/cache/cache.go b/internal/cache/cache.go
index 1111111..2222222 100644
--- a/internal/cache/cache.go
+++ b/internal/cache/cache.go
@@ -5,4 +5,4 @@ func warmCache() {
     _ = loadLegacyCache()
-    const batchSize = 50
+    const batchSize = 100
     runWarmup(batchSize)
 }`,
		},
		{
			Name:              "repository logging convention drift",
			CheckName:         "repo-convention-fit",
			CheckContent:      conventionCheck,
			Files:             []string{"internal/api/handler.go"},
			ExpectedMinIssues: 1,
			ExpectedMaxIssues: 1,
			RequiredFile:      "internal/api/handler.go",
			RequiredKeywordGroups: [][]string{
				{"logrus", "structured log", "WithField"},
				{"convention", "nearby", "existing"},
				{"fmt.Printf", "fmt"},
			},
			Diff: `diff --git a/internal/api/handler.go b/internal/api/handler.go
index 1111111..2222222 100644
--- a/internal/api/handler.go
+++ b/internal/api/handler.go
@@ -15,3 +15,4 @@ func handleRequest(req Request) error {
     logrus.WithField("request_id", req.ID).Debug("request received")
+    fmt.Printf("handling request %s\n", req.ID)
     return dispatch(req)
 }`,
		},
		{
			Name:              "canonical private state missing invariant comment",
			CheckName:         "repo-convention-fit",
			CheckContent:      invariantCommentCheck,
			Files:             []string{"lib/src/solver/partial_solution.dart"},
			ExpectedMinIssues: 1,
			ExpectedMaxIssues: 1,
			RequiredFile:      "lib/src/solver/partial_solution.dart",
			ExpectedSeverity:  "low",
			RequiredKeywordGroups: [][]string{
				{"canonical", "canonicalized"},
				{"invariant", "workspace reference", "root reference"},
				{"comment", "document"},
			},
			Diff: `diff --git a/lib/src/solver/partial_solution.dart b/lib/src/solver/partial_solution.dart
index 1111111..2222222 100644
--- a/lib/src/solver/partial_solution.dart
+++ b/lib/src/solver/partial_solution.dart
@@ -25,4 +25,16 @@ class PartialSolution {
   // Canonical terms retained as the solver's assignment source of truth.
   final List<Term> _assignments = [];

+  final Map<String, PackageRef> _rootRefs = {};
+
+  void rememberRoot(PackageRef ref) {
+    final canonical = ref.canonical();
+    _rootRefs[canonical.workspaceName] = canonical;
+  }
+
+  PackageRef? rootRefFor(String workspaceName) => _rootRefs[workspaceName];
+
+  void forgetRoot(PackageRef ref) {
+    _rootRefs.remove(ref.canonical().workspaceName);
+  }
 }`,
		},
		{
			Name:              "canonical private state documents invariant",
			CheckName:         "repo-convention-fit",
			CheckContent:      invariantCommentCheck,
			Files:             []string{"lib/src/solver/partial_solution.dart"},
			ExpectedMaxIssues: 0,
			Diff: `diff --git a/lib/src/solver/partial_solution.dart b/lib/src/solver/partial_solution.dart
index 1111111..2222222 100644
--- a/lib/src/solver/partial_solution.dart
+++ b/lib/src/solver/partial_solution.dart
@@ -25,4 +25,17 @@ class PartialSolution {
   // Canonical terms retained as the solver's assignment source of truth.
   final List<Term> _assignments = [];

+  // Canonical package refs keyed by canonical workspace names.
+  final Map<String, PackageRef> _rootRefs = {};
+
+  void rememberRoot(PackageRef ref) {
+    final canonical = ref.canonical();
+    _rootRefs[canonical.workspaceName] = canonical;
+  }
+
+  PackageRef? rootRefFor(String workspaceName) => _rootRefs[workspaceName];
+
+  void forgetRoot(PackageRef ref) {
+    _rootRefs.remove(ref.canonical().workspaceName);
+  }
 }`,
		},
		{
			Name:              "satisfier repeats canonicalization of unchanged term",
			CheckName:         "implementation-simplicity-and-cost",
			CheckContent:      duplicateNormalizationCheck,
			Files:             []string{"lib/src/solver/partial_solution.dart"},
			ExpectedMinIssues: 1,
			ExpectedMaxIssues: 1,
			RequiredFile:      "lib/src/solver/partial_solution.dart",
			ExpectedSeverity:  "low",
			RequiredKeywordGroups: [][]string{
				{"duplicate", "redundant", "repeated", "repeats", "again"},
				{"canonical", "normalize"},
				{"unchanged", "same term", "already"},
				{"satisfier", "satisfies", "relation"},
			},
			Diff: `diff --git a/lib/src/solver/partial_solution.dart b/lib/src/solver/partial_solution.dart
index 1111111..2222222 100644
--- a/lib/src/solver/partial_solution.dart
+++ b/lib/src/solver/partial_solution.dart
@@ -40,1 +40,27 @@ class PartialSolution {
+  Assignment? satisfier(Term term) {
+    term = canonicalizeTerm(term);
+    for (final prefix in _prefixes) {
+      if (prefix.satisfies(term)) return prefix;
+    }
+    return null;
+  }
+
+  void derive(Term term) {
+    term = canonicalizeTerm(term);
+    _assignments.add(term);
+  }
+}
+
+class Assignment {
+  bool satisfies(Term term) => relation(term) == SetRelation.subset;
+
+  SetRelation relation(Term term) {
+    term = canonicalizeTerm(term);
+    return _relationTo(term);
+  }
+}
+
+final class Term {
+  const Term(this.package);
+  final Package package;
 }`,
		},
		{
			Name:              "satisfier normalizes rebound term at method boundary",
			CheckName:         "implementation-simplicity-and-cost",
			CheckContent:      duplicateNormalizationCheck,
			Files:             []string{"lib/src/solver/partial_solution.dart"},
			ExpectedMaxIssues: 0,
			Diff: `diff --git a/lib/src/solver/partial_solution.dart b/lib/src/solver/partial_solution.dart
index 1111111..2222222 100644
--- a/lib/src/solver/partial_solution.dart
+++ b/lib/src/solver/partial_solution.dart
@@ -40,1 +40,30 @@ class PartialSolution {
+  Assignment? satisfier(Term term) {
+    term = canonicalizeTerm(term);
+    for (final prefix in _prefixes) {
+      term = prefix.project(term);
+      if (prefix.satisfies(term)) return prefix;
+    }
+    return null;
+  }
+
+  void derive(Term term) {
+    term = canonicalizeTerm(term);
+    _assignments.add(term);
+  }
+}
+
+class Assignment {
+  Term project(Term term) => term.forPackage(package);
+
+  bool satisfies(Term term) => relation(term) == SetRelation.subset;
+
+  SetRelation relation(Term term) {
+    term = canonicalizeTerm(term);
+    return _relationTo(term);
+  }
+}
+
+final class Term {
+  const Term(this.package);
+  final Package package;
 }`,
		},
	}
}
