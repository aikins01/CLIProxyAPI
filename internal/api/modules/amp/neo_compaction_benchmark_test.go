package amp

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
)

type neoCompactionSyntheticBenchmarkCase struct {
	Name             string
	ThreadID         string
	Messages         []neoMessage
	GoldenSummary    string
	MustInclude      []string
	MustIncludeOneOf [][]string
	MustNotInclude   []string
	MaxSummaryBytes  int
}

type neoCompactionBenchmarkCandidate struct {
	Name   string
	Route  neoModelRoute
	Effort string
}

type neoCompactionBenchmarkMetrics struct {
	ModelMillis int64
}

type neoCompactionRecentReplayCase struct {
	ThreadID              string
	Title                 string
	RecordCreatedAt       string
	Messages              []neoMessage
	ReferenceSummary      string
	ReferenceSummaryBytes int
	SourceBytes           int
	CorrectionSignals     int
	Anchors               []string
}

var (
	neoCompactionUUIDThreadPattern       = regexp.MustCompile(`^T-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	neoCompactionCodeAnchorPattern       = regexp.MustCompile("`([^`\\n]{3,180})`")
	neoCompactionPathAnchorPattern       = regexp.MustCompile(`(?:/[A-Za-z0-9._+~-]+){2,}|(?:[A-Za-z0-9._+-]+/){1,}[A-Za-z0-9._+-]+`)
	neoCompactionPRAnchorPattern         = regexp.MustCompile(`(?i)\bPR\s*#?[0-9]+\b|#[0-9]+\b`)
	neoCompactionCommitAnchorPattern     = regexp.MustCompile(`\b[0-9a-f]{7,40}\b`)
	neoCompactionModelAnchorPattern      = regexp.MustCompile(`(?i)\b(?:gpt|claude|gemini|glm|amp)-[A-Za-z0-9._-]+\b`)
	neoCompactionCredentialURLPattern    = regexp.MustCompile("(?i)[a-z][a-z0-9+.-]*://[^\\s`]+@[^\\s`]+")
	neoCompactionCorrectionSignalPattern = regexp.MustCompile(`(?i)forgot|missing|missed|wrong|not what|didn.t|did not|what about|you said|supposed to|still not|still does not|still is not`)
)

func TestNeoCompactionSyntheticBenchmarkFixtures(t *testing.T) {
	cases := neoCompactionSyntheticBenchmarkCases()
	if len(cases) < 10 {
		t.Fatalf("synthetic compaction cases = %d, want at least 10", len(cases))
	}
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			if strings.TrimSpace(tc.ThreadID) == "" || len(tc.Messages) == 0 {
				t.Fatalf("case has no thread/messages: %#v", tc)
			}
			if len(tc.MustInclude)+len(tc.MustIncludeOneOf)+len(tc.MustNotInclude) == 0 {
				t.Fatalf("case rubric is empty: %#v", tc)
			}
			history := neoCompactionHistory(tc.Messages)
			if len(history) == 0 {
				t.Fatalf("compaction history empty for %s", tc.Name)
			}
			passed, missing, forbidden := neoCompactionBenchmarkRubric(tc.GoldenSummary, tc)
			if !passed {
				t.Fatalf("golden summary missed rubric: missing=%#v forbidden=%#v\n%s", missing, forbidden, tc.GoldenSummary)
			}
		})
	}
}

func TestNeoCompactionDefaultBenchmarkCandidates(t *testing.T) {
	t.Setenv("AMP_COMPACTION_MODEL_BENCHMARK_CANDIDATES", "")
	candidates := neoCompactionBenchmarkCandidates(t)
	got := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		got = append(got, candidate.Route.Provider+"/"+candidate.Route.Model+"@"+candidate.Effort)
	}
	want := []string{"openai/gpt-5.6-sol@medium"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("default compaction benchmark candidates = %#v, want %#v", got, want)
	}
}

func TestNeoCompactionBenchmarkRubricRejectsGenericSummary(t *testing.T) {
	tc := neoCompactionSyntheticBenchmarkCases()[0]
	passed, missing, forbidden := neoCompactionBenchmarkRubric("Made progress. Continue with the work.", tc)
	if passed || len(missing) == 0 || len(forbidden) != 0 {
		t.Fatalf("generic summary rubric = passed:%v missing:%#v forbidden:%#v", passed, missing, forbidden)
	}
}

func TestNeoCompactionHistoryHonorsSummaryBeforeCutRecordShape(t *testing.T) {
	threadID := "T-019f3c6b-b575-7185-a670-2a14e003dd04"
	summary := "Latest summary: keep post_launch_15m paper-only work, preserve live safety, and finish collector wiring."
	messages := []neoMessage{
		neoCompactionBenchmarkTextMessage(threadID, "user", "M-old", "old compacted prefix should not replay"),
		neoCompactionBenchmarkSummaryMessage(threadID, "M-summary", summary),
		neoCompactionBenchmarkTextMessage(threadID, "user", "M-cut", "cut boundary request: finish under_2h cron wiring"),
		neoCompactionBenchmarkTextMessage(threadID, "assistant", "M-after", "after cut: tests still need to run"),
	}
	records := []map[string]any{{"cutMessageId": "M-cut", "createdAt": "2026-07-09T14:54:17.036309Z"}}

	history := neoHistoryFromStoredMessages(messages, records)
	rendered := fmt.Sprint(history)
	if len(history) != 3 || history[0].Role != "user" || history[0].Text != summary {
		t.Fatalf("history = %#v, want summary plus cut/tail messages", history)
	}
	if strings.Contains(rendered, "old compacted prefix") {
		t.Fatalf("history replayed compacted prefix: %#v", history)
	}
	for _, want := range []string{"cut boundary request", "tests still need to run"} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("history missing %q: %#v", want, history)
		}
	}
}

func TestNeoCompactionRecentReplayReconstructsPriorSummaryAndRootTail(t *testing.T) {
	threadID := "T-019f0000-0000-7000-8000-000000000001"
	thread := map[string]any{
		"title": "replay fixture",
		"messages": []any{
			neoCompactionBenchmarkTextMessage(threadID, "user", "M-0000000000000000000000", "old prefix must not replay").protocol(),
			map[string]any{"role": "info", "messageId": "M-0000000000000000000001", "createdAt": "2026-07-09T09:29:00Z", "content": []any{map[string]any{"type": "summary", "summary": map[string]any{"type": "message", "summary": "prior compacted decision MODE_BETA"}}}},
			neoCompactionBenchmarkTextMessage(threadID, "user", "M-0000000000000000000002", "continue beta work").protocol(),
			map[string]any{"role": "user", "messageId": "M-0000000000000000000003", "parentToolUseId": "TU-read", "createdAt": "2026-07-09T09:45:00Z", "content": []any{map[string]any{"type": "text", "text": "nested child noise"}}},
			map[string]any{"role": "info", "messageId": "M-0000000000000000000004", "createdAt": "2026-07-09T09:59:59Z", "content": []any{map[string]any{"type": "summary", "summary": map[string]any{"type": "message", "summary": "latest generated summary"}}}},
			neoCompactionBenchmarkTextMessage(threadID, "user", "M-0000000000000000000005", "retained root tail").protocol(),
			map[string]any{"role": "user", "messageId": "M-0000000000000000000006", "createdAt": "2026-07-09T10:01:00Z", "content": []any{map[string]any{"type": "text", "text": "post compaction request"}}},
		},
		"compactionRecords": []any{
			map[string]any{"cutMessageId": "M-0000000000000000000002", "createdAt": "2026-07-09T09:30:00Z"},
			map[string]any{"cutMessageId": "M-0000000000000000000005", "createdAt": "2026-07-09T10:00:00Z"},
		},
	}
	rawMessages := arrayValue(thread["messages"])
	for index := range rawMessages {
		message := mapValue(rawMessages[index])
		if message["createdAt"] == nil {
			message["createdAt"] = fmt.Sprintf("2026-07-09T09:%02d:00Z", 10+index)
		}
	}
	tc, ok := neoCompactionRecentReplayCaseFromThread(threadID, thread)
	if !ok {
		t.Fatal("recent replay fixture was not reconstructed")
	}
	rendered := fmt.Sprint(neoCompactionHistory(tc.Messages))
	for _, want := range []string{"prior compacted decision MODE_BETA", "continue beta work", "retained root tail"} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("replay history missing %q: %s", want, rendered)
		}
	}
	for _, unwanted := range []string{"old prefix", "nested child noise", "latest generated summary", "post compaction request"} {
		if strings.Contains(rendered, unwanted) {
			t.Fatalf("replay history retained %q: %s", unwanted, rendered)
		}
	}
}

func TestNeoCompactionRecentCorrectionSignalsIgnoreToolResults(t *testing.T) {
	messages := []neoMessage{
		neoCompactionBenchmarkToolResultMessage("T-signals", "M-tool", "TU-1", "error", "missing file"),
		neoCompactionBenchmarkTextMessage("T-signals", "user", "M-neutral", "continue with the current task"),
	}
	messages[0].CreatedAt = "2026-07-09T10:01:00Z"
	messages[1].CreatedAt = "2026-07-09T10:02:00Z"
	if got := neoCompactionRecentCorrectionSignals(messages, neoTimeStringMillis("2026-07-09T10:00:00Z")); got != 0 {
		t.Fatalf("tool-result correction signals = %d, want 0", got)
	}
	messages = append(messages, neoCompactionBenchmarkTextMessage("T-signals", "user", "M-correction", "you missed the required check"))
	messages[2].CreatedAt = "2026-07-09T10:03:00Z"
	if got := neoCompactionRecentCorrectionSignals(messages, neoTimeStringMillis("2026-07-09T10:00:00Z")); got != 1 {
		t.Fatalf("text correction signals = %d, want 1", got)
	}
}

func TestNeoCompactionTechnicalAnchorsExcludeSecrets(t *testing.T) {
	text := "PR #1014 commit 0727ce92747b798e780d3e95943e2c4d9f36611f uses `worker_data.py`, `go test ./internal/api/modules/amp`, and gpt-5.5. Never emit `postgres://user:secret-password@db/app`."
	anchors := neoCompactionTechnicalAnchors(text)
	rendered := strings.Join(anchors, "\n")
	for _, want := range []string{"PR #1014", "0727ce92747b798e780d3e95943e2c4d9f36611f", "worker_data.py", "go test ./internal/api/modules/amp", "gpt-5.5"} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("technical anchors missing %q: %#v", want, anchors)
		}
	}
	if strings.Contains(rendered, "secret-password") || strings.Contains(rendered, "postgres://") {
		t.Fatalf("technical anchors retained a secret: %#v", anchors)
	}
}

func TestNeoCompactionEfficiencyTargetRequiresAbsoluteAndRelativeExcess(t *testing.T) {
	if got := neoCompactionEfficiencyTargetBytes(100_000); got != 12_000 {
		t.Fatalf("small source efficiency target = %d, want 12000", got)
	}
	if got := neoCompactionEfficiencyTargetBytes(900_000); got != 18_000 {
		t.Fatalf("large source efficiency target = %d, want 18000", got)
	}
}

func TestNeoCompactionSyntheticModelBenchmark(t *testing.T) {
	if !neoCompactionTruthyEnv("AMP_COMPACTION_MODEL_BENCHMARK") {
		t.Skip("set AMP_COMPACTION_MODEL_BENCHMARK=1 to run live compaction model benchmark")
	}
	candidates := neoCompactionBenchmarkCandidates(t)
	cases := neoCompactionSyntheticBenchmarkCases()
	repetitions := neoCompactionBenchmarkRepetitions(t)
	strict := neoCompactionTruthyEnv("AMP_COMPACTION_MODEL_BENCHMARK_STRICT")
	for _, candidate := range candidates {
		for _, tc := range cases {
			for rep := 1; rep <= repetitions; rep++ {
				candidate := candidate
				tc := tc
				rep := rep
				t.Run(neoCompactionBenchmarkSubtestName(candidate, tc, rep), func(t *testing.T) {
					result := neoCompactionRunSyntheticModelBenchmark(t, candidate, tc, rep)
					raw, _ := json.Marshal(result)
					t.Log(string(raw))
					if strict && !boolValue(result["passed"]) {
						t.Fatalf("benchmark miss: %s", string(raw))
					}
				})
			}
		}
	}
}

func TestNeoCompactionRecentThreadAudit(t *testing.T) {
	if !neoCompactionTruthyEnv("AMP_COMPACTION_RECENT_THREAD_AUDIT") {
		t.Skip("set AMP_COMPACTION_RECENT_THREAD_AUDIT=1 to audit recent local compactions")
	}
	cases := neoCompactionRecentReplayCases(t)
	if len(cases) == 0 {
		t.Fatal("no recent compacted local threads found")
	}
	for _, tc := range cases {
		result := map[string]any{
			"threadId":               tc.ThreadID,
			"title":                  tc.Title,
			"recordCreatedAt":        tc.RecordCreatedAt,
			"sourceMessages":         len(tc.Messages),
			"sourceBytes":            tc.SourceBytes,
			"summaryBytes":           tc.ReferenceSummaryBytes,
			"summaryToSourcePercent": neoCompactionPercent(tc.ReferenceSummaryBytes, tc.SourceBytes),
			"efficiencyTargetBytes":  neoCompactionEfficiencyTargetBytes(tc.SourceBytes),
			"overRetention":          tc.ReferenceSummaryBytes > neoCompactionEfficiencyTargetBytes(tc.SourceBytes),
			"technicalAnchors":       len(tc.Anchors),
			"correctionSignals":      tc.CorrectionSignals,
		}
		raw, _ := json.Marshal(result)
		t.Log(string(raw))
		if strings.TrimSpace(tc.ReferenceSummary) == "" || len(tc.Messages) == 0 {
			t.Fatalf("invalid replay case: %s", string(raw))
		}
	}
}

func TestNeoCompactionRecentThreadModelBenchmark(t *testing.T) {
	if !neoCompactionTruthyEnv("AMP_COMPACTION_RECENT_THREAD_MODEL_BENCHMARK") {
		t.Skip("set AMP_COMPACTION_RECENT_THREAD_MODEL_BENCHMARK=1 to replay recent local compactions")
	}
	candidates := neoCompactionBenchmarkCandidates(t)
	cases := neoCompactionRecentReplayCases(t)
	strict := neoCompactionTruthyEnv("AMP_COMPACTION_RECENT_THREAD_MODEL_BENCHMARK_STRICT")
	minimumCoverage := neoCompactionRecentMinimumAnchorCoverage(t)
	for _, candidate := range candidates {
		for _, tc := range cases {
			candidate := candidate
			tc := tc
			t.Run(candidate.Name+"/"+tc.ThreadID, func(t *testing.T) {
				result := neoCompactionRunRecentThreadModelBenchmark(t, candidate, tc, minimumCoverage)
				raw, _ := json.Marshal(result)
				t.Log(string(raw))
				if strict && !boolValue(result["passed"]) {
					t.Fatalf("recent thread benchmark miss: %s", string(raw))
				}
			})
		}
	}
}

func BenchmarkNeoCompactionHistoryFromRealThreadScaleShape(b *testing.B) {
	threadID := "T-scale-compaction"
	messages := make([]neoMessage, 0, 4000)
	for i := 0; i < 3910; i++ {
		role := "user"
		if i%2 == 1 {
			role = "assistant"
		}
		messages = append(messages, neoCompactionBenchmarkTextMessage(threadID, role, fmt.Sprintf("M-%04d", i), strings.Repeat(fmt.Sprintf("message-%04d ", i), 12)))
	}
	messages = append(messages,
		neoCompactionBenchmarkSummaryMessage(threadID, "M-summary", "latest summary for scaled thread"),
		neoCompactionBenchmarkTextMessage(threadID, "user", "M-cut", "cut message retained"),
		neoCompactionBenchmarkTextMessage(threadID, "assistant", "M-after", "post cut assistant retained"),
	)
	records := []map[string]any{{"cutMessageId": "M-cut", "createdAt": "2026-07-09T14:54:17.036309Z"}}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		history := neoHistoryFromStoredMessages(messages, records)
		if len(history) == 0 {
			b.Fatal("empty history")
		}
	}
}

func neoCompactionRunSyntheticModelBenchmark(t *testing.T, candidate neoCompactionBenchmarkCandidate, tc neoCompactionSyntheticBenchmarkCase, rep int) map[string]any {
	t.Helper()
	rt := newNeoRuntime(neoCompactionBenchmarkConfig(t))
	route := candidate.Route
	route.ThinkingSuffix = candidate.Effort
	promptName, prompt := neoCompactionBenchmarkPrompt(t)
	started := time.Now()
	summary, err := inferNeoCompactionLocal(rt, tc.ThreadID, route, tc.Messages, prompt)
	duration := time.Since(started)
	normalizedSummary := neoNormalizeCompactionSummary(summary)
	passed, missing, forbidden := neoCompactionBenchmarkRubric(normalizedSummary, tc)
	result := map[string]any{
		"candidate":      candidate.Name,
		"provider":       route.Provider,
		"model":          route.Model,
		"effort":         candidate.Effort,
		"prompt":         promptName,
		"case":           tc.Name,
		"rep":            rep,
		"passed":         passed && err == nil,
		"durationMillis": duration.Milliseconds(),
		"missing":        missing,
		"forbidden":      forbidden,
		"output":         neoClipRunes(normalizedSummary, 1800),
	}
	if err != nil {
		result["error"] = err.Error()
	}
	return result
}

func neoCompactionRunRecentThreadModelBenchmark(t *testing.T, candidate neoCompactionBenchmarkCandidate, tc neoCompactionRecentReplayCase, minimumCoverage float64) map[string]any {
	t.Helper()
	rt := newNeoRuntime(neoCompactionBenchmarkConfig(t))
	route := candidate.Route
	route.ThinkingSuffix = candidate.Effort
	promptName, prompt := neoCompactionBenchmarkPrompt(t)
	messages := tc.Messages
	bounded := false
	estimatedTokens, maxInputTokens, tooLarge := neoCompactionRequestExceedsInputBudget("deep", route, messages, prompt)
	if tooLarge {
		messages = neoCompactionBoundedTranscriptMessages(tc.ThreadID, messages)
		bounded = true
		estimatedTokens, maxInputTokens, tooLarge = neoCompactionRequestExceedsInputBudget("deep", route, messages, prompt)
	}
	started := time.Now()
	summary := ""
	var err error
	if tooLarge {
		err = fmt.Errorf("bounded replay input still exceeds compaction budget: estimated_input_tokens=%d max_input_tokens=%d", estimatedTokens, maxInputTokens)
	} else {
		summary, err = inferNeoCompactionLocal(rt, tc.ThreadID, route, messages, prompt)
	}
	duration := time.Since(started)
	summary = neoNormalizeCompactionSummary(summary)
	matched, missing := neoCompactionAnchorCoverage(summary, tc.Anchors)
	coverage := 1.0
	if len(tc.Anchors) > 0 {
		coverage = float64(matched) / float64(len(tc.Anchors))
	}
	efficiencyTarget := neoCompactionEfficiencyTargetBytes(tc.SourceBytes)
	passed := err == nil && strings.TrimSpace(summary) != "" && coverage >= minimumCoverage && len([]byte(summary)) <= efficiencyTarget
	result := map[string]any{
		"candidate":              candidate.Name,
		"provider":               route.Provider,
		"model":                  route.Model,
		"effort":                 candidate.Effort,
		"prompt":                 promptName,
		"threadId":               tc.ThreadID,
		"recordCreatedAt":        tc.RecordCreatedAt,
		"passed":                 passed,
		"durationMillis":         duration.Milliseconds(),
		"boundedTranscript":      bounded,
		"sourceMessages":         len(messages),
		"sourceBytes":            tc.SourceBytes,
		"referenceSummaryBytes":  tc.ReferenceSummaryBytes,
		"summaryBytes":           len([]byte(summary)),
		"summaryToSourcePercent": neoCompactionPercent(len([]byte(summary)), tc.SourceBytes),
		"efficiencyTargetBytes":  efficiencyTarget,
		"overRetention":          len([]byte(summary)) > efficiencyTarget,
		"technicalAnchors":       len(tc.Anchors),
		"matchedAnchors":         matched,
		"anchorCoverage":         coverage,
		"minimumAnchorCoverage":  minimumCoverage,
		"missingAnchorHashes":    neoCompactionAnchorHashes(missing),
	}
	if err != nil {
		result["error"] = err.Error()
	}
	if neoCompactionTruthyEnv("AMP_COMPACTION_RECENT_THREAD_MODEL_BENCHMARK_LOG_OUTPUT") {
		result["output"] = neoClipRunes(summary, 1800)
	}
	return result
}

func neoCompactionRecentReplayCases(t *testing.T) []neoCompactionRecentReplayCase {
	t.Helper()
	dir := strings.TrimSpace(os.Getenv("AMP_COMPACTION_RECENT_THREAD_DIR"))
	if dir == "" {
		dir = neoAmpThreadStoreDir()
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read recent compaction thread dir: %v", err)
	}
	cases := make([]neoCompactionRecentReplayCase, 0)
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		threadID := strings.TrimSuffix(entry.Name(), filepath.Ext(entry.Name()))
		if !neoCompactionUUIDThreadPattern.MatchString(threadID) {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			continue
		}
		var thread map[string]any
		if err := json.Unmarshal(raw, &thread); err != nil {
			continue
		}
		if tc, ok := neoCompactionRecentReplayCaseFromThread(threadID, thread); ok {
			cases = append(cases, tc)
		}
	}
	sort.Slice(cases, func(i, j int) bool {
		return neoTimeStringMillis(cases[i].RecordCreatedAt) > neoTimeStringMillis(cases[j].RecordCreatedAt)
	})
	limit := neoCompactionRecentThreadLimit(t)
	if len(cases) > limit {
		cases = cases[:limit]
	}
	return cases
}

func neoCompactionRecentReplayCaseFromThread(threadID string, thread map[string]any) (neoCompactionRecentReplayCase, bool) {
	records := normalizeNeoCompactionRecords(thread["compactionRecords"])
	if len(records) == 0 {
		return neoCompactionRecentReplayCase{}, false
	}
	latestRecord := records[0]
	latestMillis := neoTimeStringMillis(stringValue(latestRecord["createdAt"]))
	for _, record := range records[1:] {
		if created := neoTimeStringMillis(stringValue(record["createdAt"])); created > latestMillis {
			latestRecord = record
			latestMillis = created
		}
	}
	if latestMillis == 0 {
		return neoCompactionRecentReplayCase{}, false
	}
	rawMessages := arrayValue(thread["messages"])
	messages := make([]neoMessage, 0, len(rawMessages))
	for index, raw := range rawMessages {
		if message := neoMessageFromImportedThread(threadID, raw, index); message.Role != "" {
			messages = append(messages, message)
		}
	}
	summaryIndex, summaryText := neoCompactionRecentSummary(messages, latestMillis)
	if summaryIndex < 0 || strings.TrimSpace(summaryText) == "" {
		return neoCompactionRecentReplayCase{}, false
	}
	source := make([]neoMessage, 0, len(messages)-1)
	for index, message := range messages {
		if index == summaryIndex {
			continue
		}
		created := neoTimeStringMillis(message.CreatedAt)
		if created > 0 && created > latestMillis {
			continue
		}
		source = append(source, message)
	}
	source, _ = neoCompactionSourceMessages(source, "")
	previousRecords := make([]map[string]any, 0, len(records)-1)
	for _, record := range records {
		created := neoTimeStringMillis(stringValue(record["createdAt"]))
		if created > 0 && created < latestMillis {
			previousRecords = append(previousRecords, record)
		}
	}
	_, offset := neoCompactionWindow(source, previousRecords)
	input := neoCompactionInputMessages(source, offset)
	history := neoCompactionHistory(input)
	if len(history) == 0 {
		return neoCompactionRecentReplayCase{}, false
	}
	rendered, _ := json.Marshal(history)
	return neoCompactionRecentReplayCase{
		ThreadID:              threadID,
		Title:                 strings.TrimSpace(stringValue(thread["title"])),
		RecordCreatedAt:       stringValue(latestRecord["createdAt"]),
		Messages:              input,
		ReferenceSummary:      summaryText,
		ReferenceSummaryBytes: len([]byte(summaryText)),
		SourceBytes:           len(rendered),
		CorrectionSignals:     neoCompactionRecentCorrectionSignals(messages, latestMillis),
		Anchors:               neoCompactionTechnicalAnchors(summaryText),
	}, true
}

func neoCompactionRecentSummary(messages []neoMessage, recordMillis int) (int, string) {
	bestIndex := -1
	bestMillis := 0
	bestText := ""
	for index, message := range messages {
		created := neoTimeStringMillis(message.CreatedAt)
		if created == 0 || created > recordMillis+1000 || created < bestMillis {
			continue
		}
		for _, raw := range message.Content {
			block := mapValue(raw)
			if stringValue(block["type"]) != "summary" {
				continue
			}
			text := neoCompactionSummaryText(mapValue(block["summary"]))
			if text == "" {
				continue
			}
			bestIndex = index
			bestMillis = created
			bestText = text
		}
	}
	return bestIndex, bestText
}

func neoCompactionRecentCorrectionSignals(messages []neoMessage, recordMillis int) int {
	count := 0
	seen := 0
	for _, message := range messages {
		if message.Role != "user" || neoTimeStringMillis(message.CreatedAt) <= recordMillis {
			continue
		}
		parts := make([]string, 0)
		for _, raw := range message.Content {
			block := mapValue(raw)
			if stringValue(block["type"]) == "text" {
				parts = append(parts, stringValue(block["text"]))
			}
		}
		text := strings.TrimSpace(strings.Join(parts, "\n"))
		if text == "" {
			continue
		}
		seen++
		if neoCompactionCorrectionSignalPattern.MatchString(text) {
			count++
		}
		if seen >= 5 {
			break
		}
	}
	return count
}

func neoCompactionTechnicalAnchors(text string) []string {
	anchors := make([]string, 0)
	scrubbed := neoCompactionCredentialURLPattern.ReplaceAllString(text, "")
	for _, pattern := range []*regexp.Regexp{
		neoCompactionPRAnchorPattern,
		neoCompactionCommitAnchorPattern,
		neoCompactionModelAnchorPattern,
		neoCompactionPathAnchorPattern,
	} {
		anchors = append(anchors, pattern.FindAllString(scrubbed, -1)...)
	}
	for _, match := range neoCompactionCodeAnchorPattern.FindAllStringSubmatch(scrubbed, -1) {
		if neoCompactionUsefulCodeAnchor(match[1]) {
			anchors = append(anchors, match[1])
		}
	}
	seen := map[string]struct{}{}
	filtered := make([]string, 0, len(anchors))
	for _, anchor := range anchors {
		anchor = strings.Trim(strings.TrimSpace(anchor), ".,;:()[]{}\"'")
		normalized := neoCompactionBenchmarkRubricText(anchor)
		if len(normalized) < 3 || len(normalized) > 180 || neoCompactionSensitiveAnchor(normalized) {
			continue
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		filtered = append(filtered, anchor)
		if len(filtered) >= 40 {
			break
		}
	}
	return filtered
}

func neoCompactionUsefulCodeAnchor(anchor string) bool {
	anchor = strings.TrimSpace(anchor)
	if len(anchor) > 120 {
		return false
	}
	return strings.ContainsAny(anchor, "/_.#=-") ||
		strings.HasPrefix(anchor, "go ") ||
		strings.HasPrefix(anchor, "git ") ||
		strings.HasPrefix(anchor, "pnpm ") ||
		strings.HasPrefix(anchor, "amp ")
}

func neoCompactionSensitiveAnchor(anchor string) bool {
	return strings.Contains(anchor, "://") && strings.Contains(anchor, "@") ||
		strings.Contains(anchor, "password") ||
		strings.Contains(anchor, "secret") ||
		strings.Contains(anchor, "sk_live_") ||
		strings.Contains(anchor, "api-key:")
}

func neoCompactionAnchorCoverage(summary string, anchors []string) (int, []string) {
	normalized := neoCompactionBenchmarkRubricText(summary)
	matched := 0
	missing := make([]string, 0)
	for _, anchor := range anchors {
		if strings.Contains(normalized, neoCompactionBenchmarkRubricText(anchor)) {
			matched++
		} else {
			missing = append(missing, anchor)
		}
	}
	return matched, missing
}

func neoCompactionAnchorHashes(anchors []string) []string {
	hashes := make([]string, 0, len(anchors))
	for _, anchor := range anchors {
		sum := sha256.Sum256([]byte(neoCompactionBenchmarkRubricText(anchor)))
		hashes = append(hashes, fmt.Sprintf("%x", sum[:6]))
	}
	return hashes
}

func neoCompactionRecentThreadLimit(t *testing.T) int {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv("AMP_COMPACTION_RECENT_THREAD_LIMIT"))
	if raw == "" {
		return 10
	}
	limit, err := strconv.Atoi(raw)
	if err != nil || limit <= 0 || limit > 50 {
		t.Fatalf("AMP_COMPACTION_RECENT_THREAD_LIMIT = %q, want integer from 1 to 50", raw)
	}
	return limit
}

func neoCompactionRecentMinimumAnchorCoverage(t *testing.T) float64 {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv("AMP_COMPACTION_RECENT_THREAD_MIN_ANCHOR_COVERAGE"))
	if raw == "" {
		return 0.5
	}
	coverage, err := strconv.ParseFloat(raw, 64)
	if err != nil || coverage < 0 || coverage > 1 {
		t.Fatalf("AMP_COMPACTION_RECENT_THREAD_MIN_ANCHOR_COVERAGE = %q, want decimal from 0 to 1", raw)
	}
	return coverage
}

func neoCompactionPercent(numerator, denominator int) float64 {
	if denominator <= 0 {
		return 0
	}
	return float64(numerator) * 100 / float64(denominator)
}

func neoCompactionEfficiencyTargetBytes(sourceBytes int) int {
	target := sourceBytes * 2 / 100
	if target < 12_000 {
		return 12_000
	}
	return target
}

func neoCompactionBenchmarkCandidates(t *testing.T) []neoCompactionBenchmarkCandidate {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv("AMP_COMPACTION_MODEL_BENCHMARK_CANDIDATES"))
	if raw == "" {
		return []neoCompactionBenchmarkCandidate{
			{Name: "gpt-5.6-sol-medium", Route: neoModelRoute{Provider: "openai", Model: "gpt-5.6-sol"}, Effort: defaultNeoCompactionReasoning},
		}
	}
	candidates := make([]neoCompactionBenchmarkCandidate, 0)
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		routeText := item
		effort := defaultNeoCompactionReasoning
		if left, right, ok := strings.Cut(item, "@"); ok {
			routeText = strings.TrimSpace(left)
			effort = strings.TrimSpace(right)
		}
		route := parseNeoModelRoute(routeText)
		if route.Model == "" {
			t.Fatalf("invalid compaction benchmark candidate %q", item)
		}
		if route.Provider == "" {
			route.Provider = providerForNeoModel(route.Model)
		}
		name := neoCompactionBenchmarkSafeName(route.Provider + "-" + route.Model + "-" + effort)
		candidates = append(candidates, neoCompactionBenchmarkCandidate{Name: name, Route: route, Effort: effort})
	}
	if len(candidates) == 0 {
		t.Fatal("AMP_COMPACTION_MODEL_BENCHMARK_CANDIDATES did not contain any candidates")
	}
	return candidates
}

func neoCompactionBenchmarkConfig(t *testing.T) *config.Config {
	t.Helper()
	rawURL := strings.TrimSpace(os.Getenv("AMP_COMPACTION_MODEL_BENCHMARK_URL"))
	if rawURL == "" {
		rawURL = "http://127.0.0.1:8317"
	}
	parsed, err := url.Parse(rawURL)
	if err != nil {
		t.Fatalf("parse AMP_COMPACTION_MODEL_BENCHMARK_URL: %v", err)
	}
	host := parsed.Hostname()
	portText := parsed.Port()
	if host == "" {
		t.Fatalf("AMP_COMPACTION_MODEL_BENCHMARK_URL missing host: %q", rawURL)
	}
	if portText == "" {
		if parsed.Scheme == "https" {
			portText = "443"
		} else {
			portText = "80"
		}
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("parse benchmark URL port %q: %v", portText, err)
	}
	cfg := &config.Config{Host: host, Port: port}
	if parsed.Scheme == "https" {
		cfg.TLS.Enable = true
	}
	if key := strings.TrimSpace(os.Getenv("AMP_COMPACTION_MODEL_BENCHMARK_API_KEY")); key != "" {
		cfg.APIKeys = []string{key}
	}
	return cfg
}

func neoCompactionBenchmarkPrompt(t *testing.T) (string, string) {
	t.Helper()
	switch name := strings.ToLower(strings.TrimSpace(os.Getenv("AMP_COMPACTION_MODEL_BENCHMARK_PROMPT"))); name {
	case "", "current":
		return "current", ""
	case "lean-v56":
		return name, neoCompactionPrompt()
	default:
		t.Fatalf("unknown AMP_COMPACTION_MODEL_BENCHMARK_PROMPT %q", name)
		return "", ""
	}
}

func neoCompactionBenchmarkRepetitions(t *testing.T) int {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv("AMP_COMPACTION_MODEL_BENCHMARK_REPS"))
	if raw == "" {
		return 1
	}
	reps, err := strconv.Atoi(raw)
	if err != nil || reps <= 0 {
		t.Fatalf("AMP_COMPACTION_MODEL_BENCHMARK_REPS = %q, want positive integer", raw)
	}
	return reps
}

func neoCompactionBenchmarkRubric(text string, tc neoCompactionSyntheticBenchmarkCase) (bool, []string, []string) {
	normalized := neoCompactionBenchmarkRubricText(text)
	missing := make([]string, 0)
	for _, want := range tc.MustInclude {
		if !strings.Contains(normalized, neoCompactionBenchmarkRubricText(want)) {
			missing = append(missing, want)
		}
	}
	for _, group := range tc.MustIncludeOneOf {
		matched := false
		for _, want := range group {
			if strings.Contains(normalized, neoCompactionBenchmarkRubricText(want)) {
				matched = true
				break
			}
		}
		if !matched {
			missing = append(missing, "one of: "+strings.Join(group, " | "))
		}
	}
	forbidden := make([]string, 0)
	for _, bad := range tc.MustNotInclude {
		if strings.Contains(normalized, neoCompactionBenchmarkRubricText(bad)) {
			forbidden = append(forbidden, bad)
		}
	}
	if tc.MaxSummaryBytes > 0 && len([]byte(text)) > tc.MaxSummaryBytes {
		forbidden = append(forbidden, fmt.Sprintf("summary bytes %d > %d", len([]byte(text)), tc.MaxSummaryBytes))
	}
	return len(missing) == 0 && len(forbidden) == 0, missing, forbidden
}

func neoCompactionBenchmarkRubricText(text string) string {
	text = strings.ToLower(text)
	text = strings.NewReplacer("*", "", "`", "", "<summary>", "", "</summary>", "").Replace(text)
	return strings.Join(strings.Fields(text), " ")
}

func neoCompactionBenchmarkSubtestName(candidate neoCompactionBenchmarkCandidate, tc neoCompactionSyntheticBenchmarkCase, rep int) string {
	return fmt.Sprintf("%s/%s/%d", candidate.Name, neoCompactionBenchmarkSafeName(tc.Name), rep)
}

func neoCompactionBenchmarkSafeName(name string) string {
	replacer := strings.NewReplacer("/", "_", " ", "_", ":", "_", "@", "_", "\\", "_", "|", "_")
	return replacer.Replace(strings.TrimSpace(name))
}

func neoCompactionTruthyEnv(key string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(key))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func neoCompactionSyntheticBenchmarkCases() []neoCompactionSyntheticBenchmarkCase {
	return []neoCompactionSyntheticBenchmarkCase{
		{
			Name:     "post launch 15m paper handoff",
			ThreadID: "T-019f3c6b-b575-7185-a670-2a14e003dd04",
			Messages: []neoMessage{
				neoCompactionBenchmarkTextMessage("T-019f3c6b-b575-7185-a670-2a14e003dd04", "user", "M-post-1", "We are in /Users/aikins01/Developer/vela/on-chain. Paper-only post_launch_15m signals are stale and arriving after the market moved."),
				neoCompactionBenchmarkTextMessage("T-019f3c6b-b575-7185-a670-2a14e003dd04", "assistant", "M-post-2", "Inspected worker_data.py and data_collection/workers/token_leaderboard_collector.py. Found 222 signals expired because under_2h rows are not refreshed with 15m candles."),
				neoCompactionBenchmarkTextMessage("T-019f3c6b-b575-7185-a670-2a14e003dd04", "user", "M-post-3", "Oracle recommended a dedicated under_2h cron using 15m candles. Preserve 2h/live safety, leave readiness null, and do not loosen evaluator or trader safety."),
				neoCompactionBenchmarkTextMessage("T-019f3c6b-b575-7185-a670-2a14e003dd04", "assistant", "M-post-4", "Applied initial changes in worker_data.py. Collector wiring and focused pytest still remain."),
				neoCompactionBenchmarkTextMessage("T-019f3c6b-b575-7185-a670-2a14e003dd04", "user", "M-post-5", "Get to work. Finish the collector wiring, run tests, run amp review, and keep PR workflow."),
			},
			GoldenSummary: "Task Overview: latest request is to finish the stale paper-only post_launch_15m data-latency fix in /Users/aikins01/Developer/vela/on-chain. Success means fresh under_2h rows from 15m candles without loosening evaluator/trader safety, preserve 2h/live behavior, leave readiness null, running focused pytest and amp review, then following PR workflow.\nCurrent State: worker_data.py was analyzed and partially changed; data_collection/workers/token_leaderboard_collector.py still needs collector wiring. 222 signals were expired because under_2h metrics were not scheduled from 15m candles.\nNext Steps: finish the dedicated under_2h cron/collector wiring, run venv/bin/python -m pytest tests/test_worker_data.py tests/test_token_leaderboard_collector.py, run amp review, and prepare the PR. Preserve live safety and no live buys.",
			MustInclude: []string{
				"post_launch_15m",
				"under_2h",
				"/Users/aikins01/Developer/vela/on-chain",
				"worker_data.py",
				"data_collection/workers/token_leaderboard_collector.py",
				"paper-only",
				"amp review",
			},
			MustIncludeOneOf: [][]string{
				{"15m candles", "15-minute candles"},
				{"leave readiness null", "readiness fields/null semantics unchanged", "keep readiness null"},
				{"venv/bin/python -m pytest", "pytest"},
				{"preserve live safety", "preserve all existing 2h/live-trading safety behavior", "no live buys", "2h/live safety"},
			},
			MustNotInclude:  []string{"evaluator safety was loosened", "trader safety was loosened"},
			MaxSummaryBytes: 9000,
		},
		{
			Name:     "superseded sidebar implementation",
			ThreadID: "T-compaction-sidebar",
			Messages: []neoMessage{
				neoCompactionBenchmarkTextMessage("T-compaction-sidebar", "user", "M-side-1", "Initial thought: rewrite the web listThreadListSidebar route so progress shows in the sidebar."),
				neoCompactionBenchmarkTextMessage("T-compaction-sidebar", "assistant", "M-side-2", "I inspected the sidebar source and was about to edit the web route."),
				neoCompactionBenchmarkTextMessage("T-compaction-sidebar", "user", "M-side-3", "Do not edit the web sidebar or rewrite its requests. The proxy should emit the data the web already expects."),
				neoCompactionBenchmarkTextMessage("T-compaction-sidebar", "assistant", "M-side-4", "Current implementation touches internal/api/modules/amp/neo_thread_sync.go and internal/api/modules/amp/neo_runtime.go to emit actorStatus and progressSummary for connected local actors."),
				neoCompactionBenchmarkTextMessage("T-compaction-sidebar", "assistant", "M-side-5", "Focused verification: go test ./internal/api/modules/amp -run TestNeoSidebar passed. Full go test ./... is blocked by unrelated PostgreSQL setup."),
			},
			GoldenSummary: "Latest objective supersedes the old route rewrite idea: do not edit the web sidebar or rewrite its requests. Keep the proxy-side behavior that emits the data the web already expects. Current files are internal/api/modules/amp/neo_thread_sync.go and internal/api/modules/amp/neo_runtime.go, emitting actorStatus/progressSummary for connected local actors. Verification passed with go test ./internal/api/modules/amp -run TestNeoSidebar; full go test ./... is blocked by unrelated PostgreSQL setup. Next step is to preserve the proxy-side emitted data and avoid touching listThreadListSidebar.",
			MustInclude: []string{
				"do not edit the web sidebar",
				"internal/api/modules/amp/neo_thread_sync.go",
				"internal/api/modules/amp/neo_runtime.go",
				"actorStatus",
				"progressSummary",
				"go test ./internal/api/modules/amp -run TestNeoSidebar",
				"PostgreSQL",
			},
			MustIncludeOneOf: [][]string{{"proxy-side", "proxy/API", "proxy/backend response", "proxy/backend output", "proxy/backend must emit", "backend/proxy", "backend/proxy-related", "backend/proxy code", "proxy/server data"}},
			MaxSummaryBytes:  7000,
		},
		{
			Name:     "prior summary carried through repeated compaction",
			ThreadID: "T-compaction-repeated",
			Messages: []neoMessage{
				neoCompactionBenchmarkTextMessage("T-compaction-repeated", "user", "M-repeat-old", "Old transcript before compaction that should not be replayed."),
				neoCompactionBenchmarkSummaryMessage("T-compaction-repeated", "M-repeat-summary", "Prior compacted state: final mode MODE_BETA survived. File internal/config/beta.go uses AMP_BETA_ROLLOUT=1. Verification command is go test ./internal/config -run TestBetaRollout."),
				neoCompactionBenchmarkTextMessage("T-compaction-repeated", "user", "M-repeat-cut", "Continue from the compacted beta rollout work and add the missing config test."),
				neoCompactionBenchmarkTextMessage("T-compaction-repeated", "assistant", "M-repeat-after", "Added internal/config/beta_test.go. Test still needs to run after resolving a local env issue."),
			},
			GoldenSummary: "Carry forward prior compaction details: MODE_BETA is the final surviving mode, internal/config/beta.go reads AMP_BETA_ROLLOUT=1, and go test ./internal/config -run TestBetaRollout is the verification command. Latest work after the cut added internal/config/beta_test.go for the missing config test. Next step is to rerun go test ./internal/config -run TestBetaRollout after resolving the local env issue. Do not revive old pre-compaction transcript details.",
			MustInclude: []string{
				"MODE_BETA",
				"internal/config/beta.go",
				"internal/config/beta_test.go",
				"AMP_BETA_ROLLOUT=1",
				"go test ./internal/config -run TestBetaRollout",
			},
			MustIncludeOneOf: [][]string{{"prior compaction", "prior compacted state", "compacted beta rollout work"}},
			MustNotInclude:   []string{"old transcript before compaction"},
			MaxSummaryBytes:  6000,
		},
		{
			Name:     "failed tool result is not success",
			ThreadID: "T-compaction-tool-result",
			Messages: []neoMessage{
				neoCompactionBenchmarkTextMessage("T-compaction-tool-result", "user", "M-tool-1", "Apply the server flag patch in cmd/server/main.go."),
				neoCompactionBenchmarkToolUseMessage("T-compaction-tool-result", "M-tool-2", "TU-edit-main", "edit_file", map[string]any{"path": "cmd/server/main.go", "old_str": "flag.Bool", "new_str": "flag.String"}),
				neoCompactionBenchmarkToolResultMessage("T-compaction-tool-result", "M-tool-3", "TU-edit-main", "error", "patch failed: old_str not found in cmd/server/main.go"),
				neoCompactionBenchmarkTextMessage("T-compaction-tool-result", "assistant", "M-tool-4", "The patch did not land. cmd/server/main.go needs manual follow-up because edit_file returned an error."),
			},
			GoldenSummary: "Latest task is to apply the server flag patch in cmd/server/main.go, but it has not landed. The edit_file tool call TU-edit-main failed with error: patch failed: old_str not found in cmd/server/main.go. Current state is no successful patch; next step is manual follow-up in cmd/server/main.go and then rerun focused verification.",
			MustInclude: []string{
				"cmd/server/main.go",
				"old_str",
				"not found",
			},
			MustIncludeOneOf: [][]string{
				{"patch failed", "result: failed", "failed with", "attempt failed"},
				{"not landed", "did not land", "no confirmed code change", "no successful file changes", "no files were successfully modified", "no successful changes", "no files were created or modified", "task remains incomplete"},
				{"manual follow-up", "inspect the current file", "inspect the relevant file", "inspect cmd/server/main.go", "open cmd/server/main.go", "apply the correct patch"},
			},
			MustNotInclude:  []string{"landed successfully", "patch applied successfully"},
			MaxSummaryBytes: 5000,
		},
		{
			Name:     "deployment details redact secrets",
			ThreadID: "T-compaction-redaction",
			Messages: []neoMessage{
				neoCompactionBenchmarkTextMessage("T-compaction-redaction", "user", "M-secret-1", "Dokploy production deployment is failing on server.vela.partners for app-l4l5-web. POSTGRES_URL was pasted as postgres://l4l5:super-secret-password@db/l4l5 and sk_live_redacted_but_should_not_emit appeared in logs."),
				neoCompactionBenchmarkTextMessage("T-compaction-redaction", "assistant", "M-secret-2", "Actionable commands discussed: docker logs l4l5-web, dokploy compose pull, and check DATABASE_URL/POSTGRES_URL wiring. Do not repeat secret values."),
				neoCompactionBenchmarkTextMessage("T-compaction-redaction", "user", "M-secret-3", "Summarize enough to continue but don't include secrets."),
			},
			GoldenSummary: "Continue Dokploy production deployment debugging for server.vela.partners / app-l4l5-web. Preserve env var names POSTGRES_URL and DATABASE_URL only; do not include pasted credential values. Next checks are docker logs l4l5-web, dokploy compose pull, and verifying the deployment wiring for the database env vars. Blocker is production still failing with secret-bearing logs that must stay redacted.",
			MustInclude: []string{
				"server.vela.partners",
				"app-l4l5-web",
				"POSTGRES_URL",
				"DATABASE_URL",
				"docker logs l4l5-web",
				"dokploy compose pull",
				"redacted",
			},
			MustNotInclude: []string{
				"super-secret-password",
				"sk_live_redacted_but_should_not_emit",
				"postgres://l4l5:super-secret-password",
			},
			MaxSummaryBytes: 5500,
		},
		{
			Name:     "rejected demo becomes actual implementation",
			ThreadID: "T-compaction-rejected-demo",
			Messages: []neoMessage{
				neoCompactionBenchmarkTextMessage("T-compaction-rejected-demo", "user", "M-demo-1", "Add a connected local-agent experience for the Cairn CLI."),
				neoCompactionBenchmarkTextMessage("T-compaction-rejected-demo", "assistant", "M-demo-2", "I started a local-agent-demo page to illustrate the flow."),
				neoCompactionBenchmarkTextMessage("T-compaction-rejected-demo", "user", "M-demo-3", "Do not build a demo page. I mean the actual implementation: session registration, device linking, and heartbeat plumbing. Keep offline mode as an explicit fallback."),
				neoCompactionBenchmarkTextMessage("T-compaction-rejected-demo", "assistant", "M-demo-4", "The cancelled demo patch may have left apps/web/app/local-agent-demo/page.tsx; verify and remove it before implementing the real session flow."),
			},
			GoldenSummary: "The latest direction supersedes the demo-page idea: do not build a demo page. Implement the actual connected local-agent flow with session registration, device linking, and heartbeat plumbing, while preserving offline mode as an explicit fallback. First verify that apps/web/app/local-agent-demo/page.tsx was not left by the cancelled patch and remove it if present. Then implement the real API/session path.",
			MustInclude: []string{
				"actual",
				"session registration",
				"device linking",
				"heartbeat",
				"offline mode",
				"apps/web/app/local-agent-demo/page.tsx",
			},
			MustIncludeOneOf: [][]string{
				{"do not build a demo page", "do not build or continue a demo page", "not a demo page", "do not substitute mock UI or a demonstration page", "explicitly rejected building a demo page", "demo-page approach is superseded"},
				{"supersedes", "rejected", "cancelled demo", "not a demo page"},
			},
			MaxSummaryBytes: 5500,
		},
		{
			Name:     "conditional review action remains conditional",
			ThreadID: "T-compaction-conditional-review",
			Messages: []neoMessage{
				neoCompactionBenchmarkTextMessage("T-compaction-conditional-review", "user", "M-review-1", "Review PR #85 at commit cc2ea350ff4f0bbae2b8f75d6714973fb3937aa2."),
				neoCompactionBenchmarkTextMessage("T-compaction-conditional-review", "assistant", "M-review-2", "The reviewed line still matches that commit and the review text is ready."),
				neoCompactionBenchmarkTextMessage("T-compaction-conditional-review", "user", "M-review-3", "Only post it if PR #85 is still open and the head has not changed."),
			},
			GoldenSummary: "Review text for PR #85 is ready for commit cc2ea350ff4f0bbae2b8f75d6714973fb3937aa2, but posting remains conditional. Before any write, verify that PR #85 is still open and its head is unchanged. Do not post if it is merged, closed, or moved to another commit.",
			MustInclude: []string{
				"PR #85",
				"cc2ea350ff4f0bbae2b8f75d6714973fb3937aa2",
				"still open",
				"head",
			},
			MustIncludeOneOf: [][]string{{"only post", "posting remains conditional", "posting is conditionally authorized", "at posting time", "before any write", "if and only if"}},
			MustNotInclude:   []string{"review was posted successfully", "review has already been posted", "posted successfully"},
			MaxSummaryBytes:  4500,
		},
		{
			Name:     "destructive cleanup preserves retained threads",
			ThreadID: "T-compaction-cleanup",
			Messages: []neoMessage{
				neoCompactionBenchmarkTextMessage("T-compaction-cleanup", "user", "M-clean-1", "Clean up the obvious smoke threads created during the web test."),
				neoCompactionBenchmarkTextMessage("T-compaction-cleanup", "assistant", "M-clean-2", "Candidate smoke records are in the local thread store and some also exist remotely."),
				neoCompactionBenchmarkTextMessage("T-compaction-cleanup", "user", "M-clean-3", "Keep the active task T-019f37d9-current and the real repro T-019f35f9-repro. Back up local JSON before removal. If remote delete fails, archive only confirmed smoke artifacts."),
			},
			GoldenSummary: "Clean up only confirmed smoke/test threads. Preserve active task T-019f37d9-current and real repro T-019f35f9-repro. Back up local thread JSON before removing it. Attempt remote deletion for confirmed smoke artifacts; if deletion fails, archive those smoke artifacts instead. Do not delete or archive the retained active/repro threads.",
			MustInclude: []string{
				"T-019f37d9-current",
				"T-019f35f9-repro",
				"back up",
				"archive",
			},
			MustIncludeOneOf: [][]string{
				{"preserve", "keep the active"},
				{"only confirmed smoke", "confirmed smoke/test"},
				{"local thread JSON", "local thread/task JSON", "local JSON"},
				{"remote deletion", "remote delete", "remote smoke artifacts are deleted"},
			},
			MustNotInclude:  []string{"delete all threads", "archive all threads"},
			MaxSummaryBytes: 5000,
		},
		{
			Name:     "deployment recovery preserves secret boundary",
			ThreadID: "T-compaction-deployment-recovery",
			Messages: []neoMessage{
				neoCompactionBenchmarkTextMessage("T-compaction-deployment-recovery", "user", "M-recovery-1", "Coolify shows The payload is invalid while opening application owk0k4gsso8ksw048wcg4o4k."),
				neoCompactionBenchmarkTextMessage("T-compaction-deployment-recovery", "assistant", "M-recovery-2", "The stack points to EnvironmentVariable decryption while rendering the pending configuration diff."),
				neoCompactionBenchmarkTextMessage("T-compaction-deployment-recovery", "user", "M-recovery-3", "Find the corrupt row without printing values. Do not regenerate APP_KEY. Back up the row before any repair and ask before deleting or replacing it."),
			},
			GoldenSummary: "Investigate the Coolify EnvironmentVariable decryption failure for application owk0k4gsso8ksw048wcg4o4k. Identify the corrupt row by id/key/decrypt status without printing secret values. Never regenerate APP_KEY. Back up the affected row before repair, and obtain approval before deleting or replacing it. The next step is a Laravel-bootstrapped per-row decryption probe that emits metadata only.",
			MustInclude: []string{
				"owk0k4gsso8ksw048wcg4o4k",
				"without printing",
				"APP_KEY",
				"back up",
				"decryption",
			},
			MustIncludeOneOf: [][]string{
				{"never regenerate", "do not regenerate"},
				{"before deleting", "before repair"},
				{"EnvironmentVariable", "environment_variables", "environment variable"},
				{"approval", "confirmation", "explicit confirmation"},
			},
			MaxSummaryBytes: 6500,
		},
		{
			Name:     "completed pull request waits for merge authorization",
			ThreadID: "T-compaction-merge-authorization",
			Messages: []neoMessage{
				neoCompactionBenchmarkTextMessage("T-compaction-merge-authorization", "user", "M-merge-1", "Add the by-user usage panel to the existing PR #1011."),
				neoCompactionBenchmarkTextMessage("T-compaction-merge-authorization", "assistant", "M-merge-2", "Committed and pushed 9308ec2. pnpm check, Prettier, git diff --check, and amp review passed. Unrelated worker files remain unstaged."),
				neoCompactionBenchmarkTextMessage("T-compaction-merge-authorization", "user", "M-merge-3", "Do not merge unless I explicitly ask."),
			},
			GoldenSummary: "The requested UI work for PR #1011 is complete and pushed in commit 9308ec2. pnpm check, Prettier, git diff --check, and amp review passed; unrelated worker files remain unstaged. Do not merge unless the user explicitly asks. Until then, report the completed PR state and stop.",
			MustInclude: []string{
				"PR #1011",
				"9308ec2",
				"pnpm check",
				"Prettier",
				"git diff --check",
				"amp review",
				"unrelated worker files",
				"explicitly asks",
			},
			MustIncludeOneOf: [][]string{{"do not merge", "merge unless"}},
			MustNotInclude:   []string{"merged PR #1011", "merge completed"},
			MaxSummaryBytes:  5000,
		},
	}
}

func neoCompactionBenchmarkTextMessage(threadID, role, messageID, text string) neoMessage {
	return neoMessage{
		ThreadID:  threadID,
		MessageID: messageID,
		Role:      role,
		Content:   []any{map[string]any{"type": "text", "text": text}},
		CreatedAt: "2026-07-09T00:00:00Z",
	}
}

func neoCompactionBenchmarkSummaryMessage(threadID, messageID, summary string) neoMessage {
	return neoMessage{
		ThreadID:  threadID,
		MessageID: messageID,
		Role:      "info",
		Content: []any{map[string]any{
			"type": "summary",
			"summary": map[string]any{
				"type":    "message",
				"summary": summary,
			},
		}},
		CreatedAt: "2026-07-09T00:00:00Z",
	}
}

func neoCompactionBenchmarkToolUseMessage(threadID, messageID, toolUseID, name string, input map[string]any) neoMessage {
	return neoMessage{
		ThreadID:  threadID,
		MessageID: messageID,
		Role:      "assistant",
		Content: []any{map[string]any{
			"type":     "tool_use",
			"id":       toolUseID,
			"name":     name,
			"input":    input,
			"complete": true,
		}},
		CreatedAt: "2026-07-09T00:00:00Z",
	}
}

func neoCompactionBenchmarkToolResultMessage(threadID, messageID, toolUseID, status, text string) neoMessage {
	return neoMessage{
		ThreadID:  threadID,
		MessageID: messageID,
		Role:      "user",
		Content: []any{map[string]any{
			"type":      "tool_result",
			"toolUseID": toolUseID,
			"content":   text,
			"run": map[string]any{
				"status": status,
				"error":  map[string]any{"message": text},
			},
		}},
		CreatedAt: "2026-07-09T00:00:00Z",
	}
}
