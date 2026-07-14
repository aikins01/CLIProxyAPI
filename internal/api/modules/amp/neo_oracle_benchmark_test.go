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

type neoOracleBenchmarkCase struct {
	Name            string
	Task            string
	RequiredGroups  [][]string
	ForbiddenClaims []string
}

type neoOracleBenchmarkCandidate struct {
	Name   string
	Route  neoModelRoute
	Effort string
}

func TestNeoOracleBenchmarkFixtures(t *testing.T) {
	cases := neoOracleBenchmarkCases()
	if len(cases) < 5 {
		t.Fatalf("oracle benchmark cases = %d, want at least 5", len(cases))
	}
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			if strings.TrimSpace(tc.Task) == "" || len(tc.RequiredGroups) < 2 {
				t.Fatalf("incomplete oracle benchmark fixture: %#v", tc)
			}
			for _, group := range tc.RequiredGroups {
				if len(group) == 0 {
					t.Fatalf("empty required group: %#v", tc)
				}
			}
		})
	}
}

func TestNeoOracleDefaultBenchmarkCandidates(t *testing.T) {
	t.Setenv("AMP_ORACLE_MODEL_BENCHMARK_CANDIDATES", "")
	candidates := neoOracleBenchmarkCandidates(t)
	got := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		got = append(got, candidate.Route.Provider+"/"+candidate.Route.Model+"@"+candidate.Effort)
	}
	want := []string{
		"anthropic/claude-fable-5@high",
		"openai/gpt-5.6-sol@high",
		"openai/gpt-5.6-sol@xhigh",
		"openai/gpt-5.6-sol@max",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("default oracle benchmark candidates = %#v, want %#v", got, want)
	}
}

func TestNeoOracleBenchmarkScore(t *testing.T) {
	tc := neoOracleBenchmarkCase{
		RequiredGroups:  [][]string{{"state-only", "without broadcasts"}, {"final snapshot", "durable flush"}},
		ForbiddenClaims: []string{"release the runtime lock"},
	}
	passed, missing, forbidden := neoOracleBenchmarkScore("Use state-only cleanup without broadcasts, then write the final snapshot.", tc)
	if !passed || len(missing) != 0 || len(forbidden) != 0 {
		t.Fatalf("score = passed:%v missing:%#v forbidden:%#v", passed, missing, forbidden)
	}
	passed, _, forbidden = neoOracleBenchmarkScore("Release the runtime lock and write the final snapshot.", tc)
	if passed || len(forbidden) == 0 {
		t.Fatal("score accepted a forbidden claim")
	}
	passed, missing, forbidden = neoOracleBenchmarkScore("Do not release the runtime lock before cleanup; use state-only cleanup and a final snapshot.", tc)
	if !passed || len(missing) != 0 || len(forbidden) != 0 {
		t.Fatalf("score rejected a negated forbidden claim: passed:%v missing:%#v forbidden:%#v", passed, missing, forbidden)
	}
}

func TestNeoOracleSyntheticModelBenchmark(t *testing.T) {
	if !neoReadThreadTruthyEnv("AMP_ORACLE_MODEL_BENCHMARK") {
		t.Skip("set AMP_ORACLE_MODEL_BENCHMARK=1 to run live Oracle model benchmark")
	}
	candidates := neoOracleBenchmarkCandidates(t)
	cases := neoOracleSelectedBenchmarkCases(t)
	repetitions := neoOracleBenchmarkRepetitions(t)
	strict := neoReadThreadTruthyEnv("AMP_ORACLE_MODEL_BENCHMARK_STRICT")
	for caseIndex, tc := range cases {
		for rep := 1; rep <= repetitions; rep++ {
			candidateOffset := (caseIndex + rep - 1) % len(candidates)
			for candidateIndex := range candidates {
				candidate := candidates[(candidateIndex+candidateOffset)%len(candidates)]
				tc := tc
				rep := rep
				t.Run(fmt.Sprintf("%s/%s/%d", candidate.Name, neoReadThreadBenchmarkSafeName(tc.Name), rep), func(t *testing.T) {
					result := neoOracleRunSyntheticModelBenchmark(t, candidate, tc, rep)
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

func neoOracleRunSyntheticModelBenchmark(t *testing.T, candidate neoOracleBenchmarkCandidate, tc neoOracleBenchmarkCase, rep int) map[string]any {
	t.Helper()
	rt := newNeoRuntime(neoOracleBenchmarkConfig(t))
	systemPrompt := strings.NewReplacer(
		"{{WORKING_DIR}}", "/benchmark/repository",
		"{{WORKSPACE_ROOT}}", "/benchmark/repository",
	).Replace(neoOracleSubagentPrompt) + "\n\nAll evidence needed for this benchmark is embedded in the user request. Do not call tools. Distinguish facts, inferences, and unknowns, then give a concrete recommendation."
	settings := map[string]any{"reasoning.effort": candidate.Effort}
	route := candidate.Route
	request := neoInferenceRequest{
		ActorID:              "actor-oracle-benchmark",
		ThreadID:             "T-019f5000-0000-7000-8000-000000000002",
		MessageID:            newNeoMessageID(),
		AgentMode:            "high",
		ReasoningEffort:      candidate.Effort,
		Settings:             settings,
		History:              []neoHistoryMessage{{Role: "user", Text: tc.Task}},
		Environment:          map[string]any{"workingDirectory": "/benchmark/repository", "workspaceRoot": "/benchmark/repository"},
		ModelRouteOverride:   &route,
		SystemPromptOverride: systemPrompt,
	}
	started := time.Now()
	inference, err := inferNeoLocalStream(rt, request, func(neoInferenceDelta) {})
	duration := time.Since(started)
	passed, missing, forbidden := neoOracleBenchmarkScore(inference.Text, tc)
	result := map[string]any{
		"candidate":       candidate.Name,
		"provider":        candidate.Route.Provider,
		"model":           candidate.Route.Model,
		"effort":          candidate.Effort,
		"case":            tc.Name,
		"rep":             rep,
		"passed":          passed && err == nil,
		"durationMillis":  duration.Milliseconds(),
		"inputTokens":     neoReadThreadBenchmarkUsageInt(inference.Usage, "inputTokens", "input_tokens", "prompt_tokens", "promptTokenCount"),
		"outputTokens":    neoReadThreadBenchmarkUsageInt(inference.Usage, "outputTokens", "output_tokens", "completion_tokens", "candidatesTokenCount"),
		"cacheReadTokens": neoReadThreadBenchmarkUsageInt(inference.Usage, "cacheReadInputTokens", "cache_read_input_tokens", "cachedContentTokenCount"),
		"totalInputTokens": neoReadThreadBenchmarkUsageInt(inference.Usage,
			"totalInputTokens", "total_input_tokens", "prompt_tokens", "promptTokenCount"),
		"missing":   missing,
		"forbidden": forbidden,
		"output":    neoClipRunes(inference.Text, 1800),
	}
	if err != nil {
		result["error"] = err.Error()
	}
	return result
}

func neoOracleBenchmarkScore(text string, tc neoOracleBenchmarkCase) (bool, []string, []string) {
	normalized := neoReadThreadBenchmarkRubricText(text)
	missing := make([]string, 0)
	for _, group := range tc.RequiredGroups {
		matched := false
		for _, phrase := range group {
			if strings.Contains(normalized, neoReadThreadBenchmarkRubricText(phrase)) {
				matched = true
				break
			}
		}
		if !matched {
			missing = append(missing, "one of: "+strings.Join(group, " | "))
		}
	}
	forbidden := make([]string, 0)
	for _, claim := range tc.ForbiddenClaims {
		if neoOracleBenchmarkContainsAssertedClaim(normalized, neoReadThreadBenchmarkRubricText(claim)) {
			forbidden = append(forbidden, claim)
		}
	}
	return len(missing) == 0 && len(forbidden) == 0, missing, forbidden
}

func neoOracleBenchmarkContainsAssertedClaim(text, claim string) bool {
	for offset := 0; ; {
		index := strings.Index(text[offset:], claim)
		if index < 0 {
			return false
		}
		index += offset
		prefixStart := max(0, index-32)
		prefix := strings.TrimSpace(text[prefixStart:index])
		negated := false
		for _, suffix := range []string{"do not", "don't", "must not", "should not", "cannot", "can't", "never", "not to"} {
			if strings.HasSuffix(prefix, suffix) {
				negated = true
				break
			}
		}
		if !negated {
			return true
		}
		offset = index + len(claim)
	}
}

func neoOracleBenchmarkCandidates(t *testing.T) []neoOracleBenchmarkCandidate {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv("AMP_ORACLE_MODEL_BENCHMARK_CANDIDATES"))
	if raw == "" {
		return []neoOracleBenchmarkCandidate{
			{Name: "claude-fable-5-high", Route: neoModelRoute{Provider: "anthropic", Model: "claude-fable-5"}, Effort: "high"},
			{Name: "gpt-5.6-sol-high", Route: neoModelRoute{Provider: "openai", Model: "gpt-5.6-sol"}, Effort: "high"},
			{Name: "gpt-5.6-sol-xhigh", Route: neoModelRoute{Provider: "openai", Model: "gpt-5.6-sol"}, Effort: "xhigh"},
			{Name: "gpt-5.6-sol-max", Route: neoModelRoute{Provider: "openai", Model: "gpt-5.6-sol"}, Effort: "max"},
		}
	}
	candidates := make([]neoOracleBenchmarkCandidate, 0)
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		routeText, effort, ok := strings.Cut(item, "@")
		if !ok || strings.TrimSpace(effort) == "" {
			t.Fatalf("oracle benchmark candidate %q must include @effort", item)
		}
		route := parseNeoModelRoute(strings.TrimSpace(routeText))
		if route.Model == "" {
			t.Fatalf("invalid oracle benchmark candidate %q", item)
		}
		if route.Provider == "" {
			route.Provider = providerForNeoModel(route.Model)
		}
		effort = strings.TrimSpace(effort)
		candidates = append(candidates, neoOracleBenchmarkCandidate{
			Name:   neoReadThreadBenchmarkSafeName(route.Provider + "-" + route.Model + "-" + effort),
			Route:  route,
			Effort: effort,
		})
	}
	if len(candidates) == 0 {
		t.Fatal("AMP_ORACLE_MODEL_BENCHMARK_CANDIDATES did not contain any candidates")
	}
	return candidates
}

func neoOracleSelectedBenchmarkCases(t *testing.T) []neoOracleBenchmarkCase {
	t.Helper()
	cases := neoOracleBenchmarkCases()
	raw := strings.TrimSpace(os.Getenv("AMP_ORACLE_MODEL_BENCHMARK_CASES"))
	if raw == "" {
		return cases
	}
	wanted := map[string]bool{}
	for _, name := range strings.Split(raw, ",") {
		if name = strings.TrimSpace(name); name != "" {
			wanted[name] = true
		}
	}
	selected := make([]neoOracleBenchmarkCase, 0, len(wanted))
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
		t.Fatalf("unknown oracle benchmark cases: %v", unknown)
	}
	return selected
}

func neoOracleBenchmarkConfig(t *testing.T) *config.Config {
	t.Helper()
	rawURL := strings.TrimSpace(os.Getenv("AMP_ORACLE_MODEL_BENCHMARK_URL"))
	if rawURL == "" {
		rawURL = "http://127.0.0.1:8317"
	}
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Hostname() == "" {
		t.Fatalf("invalid AMP_ORACLE_MODEL_BENCHMARK_URL %q", rawURL)
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
	if err != nil {
		t.Fatalf("parse oracle benchmark URL port %q: %v", portText, err)
	}
	cfg := &config.Config{Host: parsed.Hostname(), Port: port}
	if parsed.Scheme == "https" {
		cfg.TLS.Enable = true
	}
	if key := strings.TrimSpace(os.Getenv("AMP_ORACLE_MODEL_BENCHMARK_API_KEY")); key != "" {
		cfg.APIKeys = []string{key}
	}
	return cfg
}

func neoOracleBenchmarkRepetitions(t *testing.T) int {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv("AMP_ORACLE_MODEL_BENCHMARK_REPS"))
	if raw == "" {
		return 1
	}
	repetitions, err := strconv.Atoi(raw)
	if err != nil || repetitions <= 0 {
		t.Fatalf("AMP_ORACLE_MODEL_BENCHMARK_REPS = %q, want positive integer", raw)
	}
	return repetitions
}

func neoOracleBenchmarkCases() []neoOracleBenchmarkCase {
	return []neoOracleBenchmarkCase{
		{
			Name: "shutdown lock inversion",
			Task: `Diagnose this shutdown bug and recommend the smallest robust fix.

Facts:
- stopWithOptions holds neoRuntime.mu for the full stop lifecycle.
- executorDisconnectedForSocket ends by calling syncCloudAsync.
- syncCloudAsync calls configSnapshot, which takes neoRuntime.mu.RLock.
- Shutdown must still persist a final local thread snapshot and must not start more work.

What is the failure mechanism and what should shutdown do?`,
			RequiredGroups: [][]string{
				{"lock inversion", "self-deadlock", "deadlock"},
				{"state-only cleanup", "cleanup without broadcasts", "skip post-disconnect work", "suppress", "shutdown flag", "no new work", "skip syncCloudAsync", "do not call syncCloudAsync", "syncCloudAsync no-op", "early-return in syncCloudAsync"},
				{"final snapshot", "final local snapshot", "final local thread snapshot", "final synchronous snapshot", "persist the cleaned state"},
			},
			ForbiddenClaims: []string{"release the runtime lock before cleanup"},
		},
		{
			Name: "disable versus rebind durability",
			Task: `Design the lifecycle policy for two operations.

Facts:
- Disable is a deliberate stop: spawned executors must stop and pending leases must not remain shown as connected.
- Rebind changes only the local runtime host or port, then immediately creates a replacement runtime.
- In-flight tool work can be resumed only if the persisted snapshot retains the executor identity and pending inference/tool state.
- Both operations must flush local state before disposal.

State the distinct policy for disable and rebind, including what the replacement runtime needs.`,
			RequiredGroups: [][]string{
				{"disable", "deliberate stop"},
				{"rebind must preserve", "preserve executor work on rebind", "rebind preserves", "resumable snapshot", "continuity-preserving"},
				{"resume executor", "executor identity", "resumeExecutorID"},
				{"pending inference", "pending tool", "in-flight work"},
			},
			ForbiddenClaims: []string{"treat disable and rebind identically"},
		},
		{
			Name: "review false positive restraint",
			Task: `Review only the changed behavior and decide whether there is a blocker.

Diff summary:
- A request handler now returns context.Canceled when its caller cancels the request.
- The caller already treats errors.Is(err, context.Canceled) as an expected client disconnect and does not report success.
- Existing tests assert cancellation reaches the caller and no output artifact is committed.
- No error is swallowed and no success response is emitted.

Should a code-review finding be filed? Explain why.`,
			RequiredGroups: [][]string{
				{"no blocker", "no finding", "no code-review finding", "no actionable finding", "do not file"},
				{"context cancellation", "context.Canceled", "client disconnect"},
				{"not silent success", "does not report success", "no false success", "error is propagated", "error is not swallowed", "no error swallowing"},
			},
			ForbiddenClaims: []string{"swallows the error", "silent success bug"},
		},
		{
			Name: "bounded snapshot architecture",
			Task: `Choose an event-persistence architecture for a 49 MB thread receiving rapid progress events.

Option A clones and synchronously writes the entire thread for every event before checking whether a cloud upload is already running.
Option B allows one local write in flight and remembers only that a newer snapshot is pending; when the write completes it writes the latest state once. Shutdown waits for the writer and performs one final synchronous snapshot.

State the choice, why it controls memory/IO amplification, and why it remains durable.`,
			RequiredGroups: [][]string{
				{"option b", "choose b"},
				{"coalesce", "one in flight", "latest pending"},
				{"bounded", "bounds", "memory control", "memory amplification", "allocation", "GC churn", "peak RSS"},
				{"final synchronous snapshot", "shutdown flush", "durable"},
			},
			ForbiddenClaims: []string{"choose option a"},
		},
		{
			Name: "uncertain 503 root cause",
			Task: `Assess the root cause of repeated 503s without overstating the evidence.

Known:
- The recorded immediate error is auth_unavailable: no auth available for codex/gpt-5.5.
- Compaction repeatedly reduced model input from about 220k tokens to about 20k tokens.
- Some historical streams closed before response.completed, and that class can temporarily cool an auth/model.
- Runtime logs containing the original trigger rotated before inspection.
- Current auth state and requests are healthy.

Was compaction failure the cause, and can the original trigger be named conclusively? Give the next observability step.`,
			RequiredGroups: [][]string{
				{"not compaction", "compaction was working", "compaction is not the direct cause", "compaction failure was not the cause", "compaction succeeded", "successful compaction"},
				{"auth_unavailable", "no usable auth"},
				{"cannot be proven", "not conclusive", "unknown"},
				{"runtime auth-state logging", "structured logging", "availability transition", "selected auth", "auth identity", "cooldown reason", "next_retry_after", "candidate rejection"},
			},
			ForbiddenClaims: []string{"stream closure was definitively the root cause", "compaction failure definitely caused the 503"},
		},
	}
}
