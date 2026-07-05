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

type neoReadThreadSyntheticCase struct {
	Name             string
	ThreadID         string
	Title            string
	Goal             string
	Messages         []any
	SearchQueries    []string
	MustInclude      []string
	MustIncludeOneOf []string
	MustNotInclude   []string
}

type neoReadThreadBenchmarkCandidate struct {
	Name   string
	Route  neoModelRoute
	Effort string
}

type neoReadThreadBenchmarkMetrics struct {
	Turns            int
	ToolCalls        int
	ForcedFinals     int
	InputTokens      int
	OutputTokens     int
	TotalInputTokens int
	ModelMillis      int64
}

func TestNeoReadThreadSyntheticBenchmarkFixtures(t *testing.T) {
	cases := neoReadThreadSyntheticBenchmarkCases()
	if len(cases) < 15 {
		t.Fatalf("synthetic cases = %d, want at least 15", len(cases))
	}
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			corpus := tc.corpus()
			if corpus.ThreadID != tc.ThreadID || corpus.Title != tc.Title || len(corpus.Messages) == 0 {
				t.Fatalf("corpus = %#v", corpus)
			}
			if strings.TrimSpace(tc.Goal) == "" || len(tc.MustInclude)+len(tc.MustIncludeOneOf)+len(tc.MustNotInclude) == 0 {
				t.Fatalf("case rubric is empty: %#v", tc)
			}
			for _, query := range tc.SearchQueries {
				result, err := neoReadThreadSearch(corpus, map[string]any{"query": query, "limit": 5})
				if err != nil {
					t.Fatalf("search %q error: %v", query, err)
				}
				if len(arrayValue(result["hits"])) == 0 {
					t.Fatalf("search %q returned no hits", query)
				}
			}
			latest, end, err := neoReadThreadRead(corpus, map[string]any{"latest": true, "count": 2})
			if err != nil {
				t.Fatalf("latest read error: %v", err)
			}
			if end != len(corpus.Messages)-1 || len(arrayValue(latest["messages"])) == 0 {
				t.Fatalf("latest read = %#v end=%d", latest, end)
			}
		})
	}
}

func TestNeoReadThreadDefaultBenchmarkCandidates(t *testing.T) {
	t.Setenv("AMP_READ_THREAD_MODEL_BENCHMARK_CANDIDATES", "")
	candidates := neoReadThreadBenchmarkCandidates(t)
	got := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		got = append(got, candidate.Route.Provider+"/"+candidate.Route.Model+"@"+candidate.Effort)
	}
	want := []string{"google/gemini-3.5-flash@high", "openai/gpt-5.5@medium"}
	if !slices.Equal(got, want) {
		t.Fatalf("default benchmark candidates = %#v, want %#v", got, want)
	}
}

func TestNeoReadThreadSyntheticModelBenchmark(t *testing.T) {
	if !neoReadThreadTruthyEnv("AMP_READ_THREAD_MODEL_BENCHMARK") {
		t.Skip("set AMP_READ_THREAD_MODEL_BENCHMARK=1 to run live read_thread model benchmark")
	}
	candidates := neoReadThreadBenchmarkCandidates(t)
	cases := neoReadThreadSyntheticBenchmarkCases()
	repetitions := neoReadThreadBenchmarkRepetitions(t)
	strict := neoReadThreadTruthyEnv("AMP_READ_THREAD_MODEL_BENCHMARK_STRICT")
	for _, candidate := range candidates {
		for _, tc := range cases {
			for rep := 1; rep <= repetitions; rep++ {
				candidate := candidate
				tc := tc
				rep := rep
				t.Run(neoReadThreadBenchmarkSubtestName(candidate, tc, rep), func(t *testing.T) {
					result := neoReadThreadRunSyntheticModelBenchmark(t, candidate, tc, rep)
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

func neoReadThreadRunSyntheticModelBenchmark(t *testing.T, candidate neoReadThreadBenchmarkCandidate, tc neoReadThreadSyntheticCase, rep int) map[string]any {
	t.Helper()
	metrics := &neoReadThreadBenchmarkMetrics{}
	rt := newNeoRuntime(neoReadThreadBenchmarkConfig(t))
	rt.inferStream = func(rt *neoRuntime, request neoInferenceRequest, onDelta neoStreamCallback) (neoInferenceResult, error) {
		started := time.Now()
		result, err := inferNeoLocalStream(rt, request, onDelta)
		metrics.Turns++
		metrics.ModelMillis += time.Since(started).Milliseconds()
		if len(request.Tools) == 0 {
			metrics.ForcedFinals++
		}
		metrics.ToolCalls += len(result.ToolCalls)
		metrics.InputTokens += neoReadThreadBenchmarkUsageInt(result.Usage, "inputTokens", "input_tokens", "prompt_tokens", "promptTokenCount")
		metrics.OutputTokens += neoReadThreadBenchmarkUsageInt(result.Usage, "outputTokens", "output_tokens", "completion_tokens", "candidatesTokenCount")
		metrics.TotalInputTokens += neoReadThreadBenchmarkUsageInt(result.Usage, "totalInputTokens", "total_input_tokens", "prompt_tokens", "promptTokenCount")
		return result, err
	}
	actor := newNeoActor(rt, "actor-read-thread-benchmark", "thread-actor", "T-019e65c0-0310-77a8-b233-4b84d9c06199", "T-019e65c0-0310-77a8-b233-4b84d9c06199", neoActorRecord("actor-read-thread-benchmark", "thread-actor", "T-019e65c0-0310-77a8-b233-4b84d9c06199"), nil)
	started := time.Now()
	text, err := actor.executeLocalReadThreadAgentWithRoute(neoPendingTool{ID: "TU-read-benchmark", Name: "read_thread", Input: map[string]any{"threadID": tc.ThreadID, "question": tc.Goal}, AgentMode: "deep"}, actor.generation, tc.corpus(), tc.Goal, candidate.Route, candidate.Effort)
	duration := time.Since(started)
	passed, missing, forbidden := neoReadThreadBenchmarkRubric(text, tc)
	result := map[string]any{
		"candidate":        candidate.Name,
		"provider":         candidate.Route.Provider,
		"model":            candidate.Route.Model,
		"effort":           candidate.Effort,
		"case":             tc.Name,
		"rep":              rep,
		"passed":           passed && err == nil,
		"durationMillis":   duration.Milliseconds(),
		"modelMillis":      metrics.ModelMillis,
		"turns":            metrics.Turns,
		"toolCalls":        metrics.ToolCalls,
		"forcedFinals":     metrics.ForcedFinals,
		"inputTokens":      metrics.InputTokens,
		"outputTokens":     metrics.OutputTokens,
		"totalInputTokens": metrics.TotalInputTokens,
		"missing":          missing,
		"forbidden":        forbidden,
		"output":           neoClipRunes(text, 1600),
	}
	if err != nil {
		result["error"] = err.Error()
	}
	return result
}

func neoReadThreadBenchmarkCandidates(t *testing.T) []neoReadThreadBenchmarkCandidate {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv("AMP_READ_THREAD_MODEL_BENCHMARK_CANDIDATES"))
	if raw == "" {
		return []neoReadThreadBenchmarkCandidate{
			{Name: "gemini-3.5-flash-high", Route: neoModelRoute{Provider: "google", Model: "gemini-3.5-flash"}, Effort: "high"},
			{Name: "gpt-5.5-medium", Route: neoModelRoute{Provider: "openai", Model: "gpt-5.5"}, Effort: "medium"},
		}
	}
	candidates := make([]neoReadThreadBenchmarkCandidate, 0)
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		routeText := item
		effort := neoReadThreadAgentEffort
		if left, right, ok := strings.Cut(item, "@"); ok {
			routeText = strings.TrimSpace(left)
			effort = strings.TrimSpace(right)
		}
		route := parseNeoModelRoute(routeText)
		if route.Model == "" {
			t.Fatalf("invalid benchmark candidate %q", item)
		}
		if route.Provider == "" {
			route.Provider = providerForNeoModel(route.Model)
		}
		candidates = append(candidates, neoReadThreadBenchmarkCandidate{Name: neoReadThreadBenchmarkSafeName(route.Provider + "-" + route.Model + "-" + effort), Route: route, Effort: effort})
	}
	if len(candidates) == 0 {
		t.Fatal("AMP_READ_THREAD_MODEL_BENCHMARK_CANDIDATES did not contain any candidates")
	}
	return candidates
}

func neoReadThreadBenchmarkConfig(t *testing.T) *config.Config {
	t.Helper()
	rawURL := strings.TrimSpace(os.Getenv("AMP_READ_THREAD_MODEL_BENCHMARK_URL"))
	if rawURL == "" {
		rawURL = "http://127.0.0.1:8317"
	}
	parsed, err := url.Parse(rawURL)
	if err != nil {
		t.Fatalf("parse AMP_READ_THREAD_MODEL_BENCHMARK_URL: %v", err)
	}
	host := parsed.Hostname()
	portText := parsed.Port()
	if host == "" {
		t.Fatalf("AMP_READ_THREAD_MODEL_BENCHMARK_URL missing host: %q", rawURL)
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
	if key := strings.TrimSpace(os.Getenv("AMP_READ_THREAD_MODEL_BENCHMARK_API_KEY")); key != "" {
		cfg.APIKeys = []string{key}
	}
	return cfg
}

func neoReadThreadBenchmarkRepetitions(t *testing.T) int {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv("AMP_READ_THREAD_MODEL_BENCHMARK_REPS"))
	if raw == "" {
		return 1
	}
	reps, err := strconv.Atoi(raw)
	if err != nil || reps <= 0 {
		t.Fatalf("AMP_READ_THREAD_MODEL_BENCHMARK_REPS = %q, want positive integer", raw)
	}
	return reps
}

func neoReadThreadBenchmarkRubric(text string, tc neoReadThreadSyntheticCase) (bool, []string, []string) {
	lower := strings.ToLower(text)
	missing := make([]string, 0)
	for _, want := range tc.MustInclude {
		if !strings.Contains(lower, strings.ToLower(want)) {
			missing = append(missing, want)
		}
	}
	if len(tc.MustIncludeOneOf) > 0 {
		matched := false
		for _, want := range tc.MustIncludeOneOf {
			if strings.Contains(lower, strings.ToLower(want)) {
				matched = true
				break
			}
		}
		if !matched {
			missing = append(missing, "one of: "+strings.Join(tc.MustIncludeOneOf, " | "))
		}
	}
	forbidden := make([]string, 0)
	for _, bad := range tc.MustNotInclude {
		if strings.Contains(lower, strings.ToLower(bad)) {
			forbidden = append(forbidden, bad)
		}
	}
	return len(missing) == 0 && len(forbidden) == 0, missing, forbidden
}

func neoReadThreadBenchmarkUsageInt(usage map[string]any, keys ...string) int {
	for _, key := range keys {
		if value := numberFrom(usage[key]); value > 0 {
			return value
		}
	}
	return 0
}

func neoReadThreadBenchmarkSubtestName(candidate neoReadThreadBenchmarkCandidate, tc neoReadThreadSyntheticCase, rep int) string {
	return fmt.Sprintf("%s/%s/%d", candidate.Name, neoReadThreadBenchmarkSafeName(tc.Name), rep)
}

func neoReadThreadBenchmarkSafeName(name string) string {
	replacer := strings.NewReplacer("/", "_", " ", "_", ":", "_", "@", "_", "\\", "_", "|", "_")
	return replacer.Replace(strings.TrimSpace(name))
}

func neoReadThreadTruthyEnv(key string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(key))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func (tc neoReadThreadSyntheticCase) corpus() neoReadThreadCorpus {
	return neoReadThreadCorpusFromThreadMap(tc.ThreadID, map[string]any{"id": tc.ThreadID, "title": tc.Title, "messages": tc.Messages}, "synthetic-benchmark")
}

func neoReadThreadSyntheticBenchmarkCases() []neoReadThreadSyntheticCase {
	return []neoReadThreadSyntheticCase{
		{
			Name:          "superseded implementation decision",
			ThreadID:      "T-019e65c0-0310-77a8-b233-4b84d9c06201",
			Title:         "Router migration strategy",
			Goal:          "Which router migration approach survived, and what files were involved?",
			Messages:      neoReadThreadSupersededDecisionMessages(),
			SearchQueries: []string{"router migration", "MIGRATION_ENGINE_BATCHED"},
			MustInclude:   []string{"MIGRATION_ENGINE_BATCHED", "internal/router/migrate.go", "routes_test.go"},
		},
		{
			Name:           "failed edit tool result",
			ThreadID:       "T-019e65c0-0310-77a8-b233-4b84d9c06202",
			Title:          "Server flag patch failure",
			Goal:           "Did the server flag patch land? Include the exact failing file and error outcome.",
			Messages:       neoReadThreadFailedToolMessages(),
			SearchQueries:  []string{"server flag patch", "cmd/server/main.go"},
			MustInclude:    []string{"cmd/server/main.go", "error", "patch failed"},
			MustNotInclude: []string{"landed successfully", "merged successfully"},
		},
		{
			Name:          "exact verification command and model",
			ThreadID:      "T-019e65c0-0310-77a8-b233-4b84d9c06203",
			Title:         "Review model verification",
			Goal:          "What model and verification command did we settle on for code review mode?",
			Messages:      neoReadThreadExactDetailMessages(),
			SearchQueries: []string{"TestNeoReviewModeRouteAndPrompt", "gpt-5.5"},
			MustInclude:   []string{"openai/gpt-5.5", "go test ./internal/api/modules/amp -run TestNeoReviewModeRouteAndPrompt", "neo_runtime_test.go"},
		},
		{
			Name:          "large thread exact marker",
			ThreadID:      "T-019e65c0-0310-77a8-b233-4b84d9c06204",
			Title:         "Long debugging session",
			Goal:          "Find the final marker and state the surviving timeout policy.",
			Messages:      neoReadThreadLargeThreadMessages(),
			SearchQueries: []string{"FINAL_TIMEOUT_POLICY"},
			MustInclude:   []string{"FINAL_TIMEOUT_POLICY=no-post-connect-timeouts", "internal/runtime/executor/codex_websockets_executor.go"},
		},
		{
			Name:             "absent topic negative case",
			ThreadID:         "T-019e65c0-0310-77a8-b233-4b84d9c06205",
			Title:            "Authentication cleanup",
			Goal:             "Which PostgreSQL migration file did this thread settle on?",
			Messages:         neoReadThreadAbsentTopicMessages(),
			SearchQueries:    []string{"authentication cleanup"},
			MustIncludeOneOf: []string{"not found", "no evidence", "not mentioned", "not present", "no postgresql migration", "no database migration", "no migration file"},
			MustNotInclude:   []string{"pg_migrate_20260705", "db/migrations/20260705"},
		},
		{
			Name:          "continuation handoff with blocker",
			ThreadID:      "T-019e65c0-0310-77a8-b233-4b84d9c06206",
			Title:         "Sidebar progress handoff",
			Goal:          "Extract the current implementation state, exact files, verification, blocker, and next steps to continue this work.",
			Messages:      neoReadThreadContinuationHandoffMessages(),
			SearchQueries: []string{"sidebar progress", "PostgreSQL blocker"},
			MustInclude:   []string{"internal/api/modules/amp/neo_thread_sync.go", "internal/api/modules/amp/neo_runtime.go", "go test ./internal/api/modules/amp -run TestNeoSidebar", "PostgreSQL", "web sidebar"},
		},
		{
			Name:           "latest continuation hidden by tool tail",
			ThreadID:       "T-019e65c0-0310-77a8-b233-4b84d9c06216",
			Title:          "Late signal paper fixes",
			Goal:           "Continue from the latest task in this thread. Extract what should be done next and ignore older superseded storage-validation context.",
			Messages:       neoReadThreadHiddenContinuationTailMessages(),
			SearchQueries:  []string{"lets do the fixes", "late_actionable"},
			MustInclude:    []string{"frontend/src/routes/post-launch/strategy-lab/+page.svelte", "data_collection/workers/strategy_paper_trader.py", "late_actionable", "paper-only"},
			MustNotInclude: []string{"position mark storage/query path", "signal-status ranking"},
		},
		{
			Name:          "review findings after partial fixes",
			ThreadID:      "T-019e65c0-0310-77a8-b233-4b84d9c06207",
			Title:         "Review follow-up",
			Goal:          "Which review finding is still unresolved, and which one was fixed?",
			Messages:      neoReadThreadReviewFindingsMessages(),
			SearchQueries: []string{"thread.Actor.ID", "P2 fixed"},
			MustInclude:   []string{"P1", "thread.Actor.ID", "internal/api/modules/amp/sidebar.go", "P2", "fixed"},
		},
		{
			Name:             "failed then passing verification",
			ThreadID:         "T-019e65c0-0310-77a8-b233-4b84d9c06208",
			Title:            "Cache invalidation verification",
			Goal:             "What changed, what is the current verification status, and what command should be run next before shipping? Include exact files.",
			Messages:         neoReadThreadVerificationRecoveryMessages(),
			SearchQueries:    []string{"TestCacheInvalidatesOnConfigChange", "full cache package"},
			MustInclude:      []string{"internal/cache/signature.go", "internal/cache/signature_test.go", "go test ./internal/cache -run TestCacheInvalidatesOnConfigChange", "go test ./internal/cache"},
			MustIncludeOneOf: []string{"passed", "pass"},
		},
		{
			Name:          "stale compaction needs original messages",
			ThreadID:      "T-019e65c0-0310-77a8-b233-4b84d9c06209",
			Title:         "Compacted rollout decision",
			Goal:          "The summary may be stale. What exact final mode, file, env var, and verification command survived?",
			Messages:      neoReadThreadStaleSummaryMessages(),
			SearchQueries: []string{"MODE_BETA", "AMP_BETA_ROLLOUT"},
			MustInclude:   []string{"MODE_BETA", "internal/config/beta.go", "AMP_BETA_ROLLOUT=1", "go test ./internal/config -run TestBetaRollout"},
		},
		{
			Name:          "late user scope change overrides shipping",
			ThreadID:      "T-019e65c0-0310-77a8-b233-4b84d9c06210",
			Title:         "Shipping instructions changed",
			Goal:          "What are the latest shipping instructions from the user?",
			Messages:      neoReadThreadLateScopeChangeMessages(),
			SearchQueries: []string{"do not push", "leave patch uncommitted"},
			MustInclude:   []string{"do not push", "do not restart brew", "leave the patch uncommitted", "report the tests"},
		},
		{
			Name:          "cliproxyapi binary shipping workflow",
			ThreadID:      "T-019e65c0-0310-77a8-b233-4b84d9c06211",
			Title:         "Build runtime and replace homebrew",
			Goal:          "Extract the exact CLIProxyAPI shipping workflow: branch/commit/push, build ldflags, replacement paths, restart, and validation commands.",
			Messages:      neoReadThreadCLIProxyShippingMessages(),
			SearchQueries: []string{"DefaultConfigPath", "cliproxyapi.real.new"},
			MustInclude:   []string{"amp/subagent-model-routing", "private remote", "go build -ldflags", "main.DefaultConfigPath=/opt/homebrew/etc/cliproxyapi.conf", "/tmp/cliproxyapi.real.new", "/opt/homebrew/opt/cliproxyapi/bin/cliproxyapi.real", "brew services restart cliproxyapi", "curl -fsS http://127.0.0.1:8317/healthz"},
		},
		{
			Name:           "deployment details with secret redaction",
			ThreadID:       "T-019e65c0-0310-77a8-b233-4b84d9c06212",
			Title:          "Dokploy production deployment failing",
			Goal:           "Extract actionable deployment details, commands, env var names, app identifiers, and blockers. Do not include secrets.",
			Messages:       neoReadThreadDeploymentRedactionMessages(),
			SearchQueries:  []string{"Dokploy", "POSTGRES_URL"},
			MustInclude:    []string{"server.vela.partners", "dokploy-l4l5-prod", "app-l4l5-web", "POSTGRES_URL", "DATABASE_URL", "docker logs l4l5-web", "dokploy compose pull"},
			MustNotInclude: []string{"super-secret-password", "sk_live_redacted_but_should_not_emit", "postgres://l4l5:super-secret-password"},
		},
		{
			Name:           "same thread code review tool invocation",
			ThreadID:       "T-019e65c0-0310-77a8-b233-4b84d9c06213",
			Title:          "Code review tool exposure",
			Goal:           "In this same thread, did code_review become available after loading the skill, and what exact invocation was used?",
			Messages:       neoReadThreadSameThreadCodeReviewMessages(),
			SearchQueries:  []string{"code_review", "applying-review-checks"},
			MustInclude:    []string{"code_review", "target", "HEAD", "checks", "applying-review-checks"},
			MustNotInclude: []string{"not available", "unavailable after loading"},
		},
		{
			Name:           "repository switch recommendation",
			ThreadID:       "T-019e65c0-0310-77a8-b233-4b84d9c06214",
			Title:          "Text icons question",
			Goal:           "Should future work continue in the existing telemetry.dev repo or switch to Duncan's new telemetry2 repo? Include the reason and latest recommendation.",
			Messages:       neoReadThreadRepositorySwitchMessages(),
			SearchQueries:  []string{"telemetry2", "traceloop/openllmetry"},
			MustInclude:    []string{"switch", "https://github.com/telemetry-dev/telemetry2", "traceloop/openllmetry", "OpenTelemetry", "telemetry.dev"},
			MustNotInclude: []string{"continue in the existing telemetry.dev repo as the final recommendation"},
		},
		{
			Name:          "mobile continuation cross platform handoff",
			ThreadID:      "T-019e65c0-0310-77a8-b233-4b84d9c06215",
			Title:         "Completing Sentry integration and data model fixes",
			Goal:          "Extract the continuation task, cross-platform files, completed fixes, remaining shipping work, and notable validation context.",
			Messages:      neoReadThreadMobileContinuationMessages(),
			SearchQueries: []string{"ComplaintData.age", "Sentry SDK"},
			MustInclude:   []string{"ComplaintData.age", "String?", "Sentry", "8.32.0", "9.4.1", "HealthlineViewModel", "MATCH_GIT_BASIC_AUTHORIZATION", "commit and merge"},
		},
	}
}

func neoReadThreadSupersededDecisionMessages() []any {
	return []any{
		neoReadThreadTextMessage("user", "M-router-1", "We need a router migration. Initial proposal: MIGRATION_ENGINE_STREAMING in internal/router/streaming.go."),
		neoReadThreadTextMessage("assistant", "M-router-2", "I started wiring MIGRATION_ENGINE_STREAMING and added a draft test."),
		neoReadThreadTextMessage("user", "M-router-3", "Review found the streaming path conflicts with rollback ordering."),
		neoReadThreadTextMessage("assistant", "M-router-4", "Final decision: use MIGRATION_ENGINE_BATCHED. Files: internal/router/migrate.go and internal/api/modules/amp/routes_test.go. The streaming draft was abandoned."),
		neoReadThreadTextMessage("user", "M-router-5", "Confirmed latest: MIGRATION_ENGINE_BATCHED survived; routes_test.go is the required regression coverage."),
	}
}

func neoReadThreadFailedToolMessages() []any {
	return []any{
		neoReadThreadTextMessage("user", "M-flag-1", "Please apply the server flag patch in cmd/server/main.go."),
		map[string]any{"role": "assistant", "messageId": "M-flag-2", "content": []any{map[string]any{"type": "tool_use", "id": "TU-edit-main", "name": "edit_file", "input": map[string]any{"path": "cmd/server/main.go", "old_str": "flag.Bool", "new_str": "flag.String"}, "complete": true}}},
		map[string]any{"role": "user", "messageId": "M-flag-3", "content": []any{map[string]any{"type": "tool_result", "toolUseID": "TU-edit-main", "run": map[string]any{"status": "error", "error": map[string]any{"message": "patch failed: old_str not found in cmd/server/main.go"}}}}},
		neoReadThreadTextMessage("assistant", "M-flag-4", "The server flag patch did not land. cmd/server/main.go still needs a manual follow-up because the edit_file result was an error."),
	}
}

func neoReadThreadExactDetailMessages() []any {
	return []any{
		neoReadThreadTextMessage("user", "M-review-1", "Check the code review mode route."),
		neoReadThreadTextMessage("assistant", "M-review-2", "TestNeoReviewModeRouteAndPrompt in internal/api/modules/amp/neo_runtime_test.go asserts review mode resolves to openai/gpt-5.5."),
		neoReadThreadTextMessage("assistant", "M-review-3", "Verification command: go test ./internal/api/modules/amp -run TestNeoReviewModeRouteAndPrompt"),
		neoReadThreadTextMessage("user", "M-review-4", "Keep that command and model name in the handoff."),
	}
}

func neoReadThreadLargeThreadMessages() []any {
	messages := make([]any, 0, 34)
	messages = append(messages, neoReadThreadTextMessage("user", "M-long-0", "We are debugging executor timeout policy and several unrelated flakes."))
	for i := 1; i <= 30; i++ {
		messages = append(messages, neoReadThreadTextMessage("assistant", fmt.Sprintf("M-long-%d", i), strings.Repeat(fmt.Sprintf("noise-%02d ", i), 80)))
	}
	messages = append(messages, neoReadThreadTextMessage("assistant", "M-long-31", "FINAL_TIMEOUT_POLICY=no-post-connect-timeouts. Only keep websocket liveness exceptions in internal/runtime/executor/codex_websockets_executor.go."))
	messages = append(messages, neoReadThreadTextMessage("user", "M-long-32", "Confirmed latest policy: no post-connect timeouts; codex websocket liveness remains the exception."))
	return messages
}

func neoReadThreadAbsentTopicMessages() []any {
	return []any{
		neoReadThreadTextMessage("user", "M-auth-1", "Authentication cleanup: remove stale OAuth comments and keep existing auth store behavior."),
		neoReadThreadTextMessage("assistant", "M-auth-2", "Touched internal/store/oauth.go and internal/api/modules/amp/secret.go. No database migration work was discussed."),
		neoReadThreadTextMessage("user", "M-auth-3", "Latest: authentication cleanup only; no PostgreSQL migration file exists in this thread."),
	}
}

func neoReadThreadContinuationHandoffMessages() []any {
	return []any{
		neoReadThreadTextMessage("user", "M-handoff-1", "We need sidebar progress to show while a thread is working. Current idea: edit the web sidebar directly."),
		neoReadThreadTextMessage("assistant", "M-handoff-2", "I inspected the web route and was about to change the sidebar request."),
		neoReadThreadTextMessage("user", "M-handoff-3", "Do not edit the web sidebar. The proxy should only emit the data the web already expects."),
		neoReadThreadTextMessage("assistant", "M-handoff-4", "Implementation state: changed internal/api/modules/amp/neo_thread_sync.go and internal/api/modules/amp/neo_runtime.go so synced threads include actorStatus=running and progressSummary when a local actor is connected."),
		neoReadThreadToolResultMessage("M-handoff-5", "TU-sidebar-test", "done", "ok github.com/router-for-me/CLIProxyAPI/v7/internal/api/modules/amp 1.12s\ncommand: go test ./internal/api/modules/amp -run TestNeoSidebar"),
		neoReadThreadTextMessage("assistant", "M-handoff-6", "Focused verification passed: go test ./internal/api/modules/amp -run TestNeoSidebar. Full go test ./... is blocked by unrelated PostgreSQL integration setup in internal/store/pg_test.go."),
		neoReadThreadTextMessage("user", "M-handoff-7", "Next steps: keep the web sidebar untouched, preserve the proxy-side emitted data, note the unrelated PostgreSQL blocker, and do not restart brew in this handoff."),
	}
}

func neoReadThreadHiddenContinuationTailMessages() []any {
	messages := make([]any, 0, 254)
	for i := 0; i < 230; i++ {
		text := fmt.Sprintf("Background implementation note %03d.", i)
		if i == 84 {
			text = "Earlier superseded context: add regression coverage for the position mark storage/query path and signal-status ranking behavior."
		}
		messages = append(messages, neoReadThreadTextMessage("assistant", fmt.Sprintf("M-hidden-%03d", i), text))
	}
	messages = append(messages,
		neoReadThreadTextMessage("user", "M-hidden-230", "why are expired or late signals showing up after the market already moved?"),
		neoReadThreadTextMessage("assistant", "M-hidden-231", "The issue is discovery-bound lateness. Dashboard labels in frontend/src/routes/post-launch/strategy-lab/+page.svelte collapse arrived-late and expired states, and the paper trader lacks a late_actionable variant."),
		neoReadThreadTextMessage("user", "M-hidden-232", "so what are going to be the fixes to resolve this"),
		neoReadThreadTextMessage("assistant", "M-hidden-233", "Fix the dashboard label first in frontend/src/routes/post-launch/strategy-lab/+page.svelte, then add a paper-only late observation variant in data_collection/workers/strategy_paper_trader.py with a 45m window that allows late_actionable."),
		neoReadThreadTextMessage("user", "M-hidden-234", "lets do the fixes"),
	)
	for i := 235; i < 254; i++ {
		messages = append(messages, neoReadThreadToolResultMessage(fmt.Sprintf("M-hidden-tool-%03d", i), fmt.Sprintf("TU-hidden-%03d", i), "done", fmt.Sprintf("tool result after latest user task %03d", i)))
	}
	return messages
}

func neoReadThreadReviewFindingsMessages() []any {
	return []any{
		neoReadThreadTextMessage("assistant", "M-review-finding-1", "Review findings: P1 unresolved nil pointer in internal/api/modules/amp/sidebar.go when thread.Actor is nil and code reads thread.Actor.ID. P2 timestamp sorting in internal/api/modules/amp/sidebar_test.go uses string order."),
		neoReadThreadTextMessage("assistant", "M-review-finding-2", "Patch applied for P2 only: internal/api/modules/amp/sidebar_test.go now parses timestamps before sorting."),
		neoReadThreadToolResultMessage("M-review-finding-3", "TU-review-test", "done", "ok github.com/router-for-me/CLIProxyAPI/v7/internal/api/modules/amp 0.74s\ncommand: go test ./internal/api/modules/amp -run TestNeoSidebarSort"),
		neoReadThreadTextMessage("user", "M-review-finding-4", "Which review finding remains open after that fix?"),
		neoReadThreadTextMessage("assistant", "M-review-finding-5", "Remaining unresolved finding: P1 in internal/api/modules/amp/sidebar.go, guard thread.Actor before reading thread.Actor.ID. P2 is fixed and verified."),
	}
}

func neoReadThreadVerificationRecoveryMessages() []any {
	return []any{
		neoReadThreadTextMessage("user", "M-cache-1", "Add cache invalidation when config generation changes."),
		neoReadThreadTextMessage("assistant", "M-cache-2", "Touched internal/cache/signature.go to include configGeneration in the signature key."),
		neoReadThreadToolResultMessage("M-cache-3", "TU-cache-test-1", "error", "FAIL TestCacheInvalidatesOnConfigChange: old key reused after config generation changed"),
		neoReadThreadTextMessage("assistant", "M-cache-4", "Fixed the test fixture in internal/cache/signature_test.go and updated expected keys."),
		neoReadThreadToolResultMessage("M-cache-5", "TU-cache-test-2", "done", "PASS\nok github.com/router-for-me/CLIProxyAPI/v7/internal/cache 0.39s\ncommand: go test ./internal/cache -run TestCacheInvalidatesOnConfigChange"),
		neoReadThreadTextMessage("user", "M-cache-6", "Latest: targeted verification passed. Next command before shipping is the full cache package: go test ./internal/cache."),
	}
}

func neoReadThreadStaleSummaryMessages() []any {
	return []any{
		neoReadThreadSummaryMessage("M-summary-1", "Old compaction summary: rollout uses MODE_ALPHA in config/alpha.yaml. No verification command has been chosen."),
		neoReadThreadTextMessage("user", "M-summary-2", "The compaction summary is stale. Original final requirement changed to MODE_BETA in internal/config/beta.go with env AMP_BETA_ROLLOUT=1."),
		neoReadThreadTextMessage("assistant", "M-summary-3", "Implemented MODE_BETA handling in internal/config/beta.go and removed the alpha path from the patch."),
		neoReadThreadToolResultMessage("M-summary-4", "TU-summary-test", "done", "PASS\nok github.com/router-for-me/CLIProxyAPI/v7/internal/config 0.44s\ncommand: go test ./internal/config -run TestBetaRollout"),
		neoReadThreadTextMessage("user", "M-summary-5", "Latest confirmation: final mode MODE_BETA, env AMP_BETA_ROLLOUT=1, verification command go test ./internal/config -run TestBetaRollout."),
	}
}

func neoReadThreadLateScopeChangeMessages() []any {
	return []any{
		neoReadThreadTextMessage("user", "M-scope-1", "After tests pass, push the branch to private remote and restart brew."),
		neoReadThreadToolResultMessage("M-scope-2", "TU-scope-build", "done", "PASS\nok github.com/router-for-me/CLIProxyAPI/v7/internal/api/modules/amp 0.88s\nbuild succeeded"),
		neoReadThreadTextMessage("assistant", "M-scope-3", "Ready to push and restart brew based on the earlier instruction."),
		neoReadThreadTextMessage("user", "M-scope-4", "Stop. Latest instruction overrides the earlier one: do not push, do not restart brew, leave the patch uncommitted, and report the tests only."),
	}
}

func neoReadThreadCLIProxyShippingMessages() []any {
	return []any{
		neoReadThreadTextMessage("user", "M-ship-1", "Ship the CLIProxyAPI fix like before: review, commit, push to private remote, build and replace the Homebrew binary."),
		neoReadThreadTextMessage("assistant", "M-ship-2", "Branch is amp/subagent-model-routing. Commit was created and pushed to the private remote as 0a3608db25e3."),
		neoReadThreadTextMessage("assistant", "M-ship-3", "Build command:\nVERSION=\"$(git describe --tags --always --dirty)\"\nCOMMIT=\"$(git rev-parse --short=12 HEAD)\"\nBUILD_DATE=\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"\ngo build -ldflags \"-X main.Version=${VERSION} -X main.Commit=${COMMIT} -X main.BuildDate=${BUILD_DATE} -X main.DefaultConfigPath=/opt/homebrew/etc/cliproxyapi.conf\" -o /tmp/cliproxyapi.real.new ./cmd/server"),
		neoReadThreadTextMessage("assistant", "M-ship-4", "Replacement steps: TARGET=/opt/homebrew/opt/cliproxyapi/bin/cliproxyapi.real; TS=$(date -u +%Y%m%dT%H%M%SZ); BACKUP=${TARGET}.backup-${TS}; cp \"$TARGET\" \"$BACKUP\"; cp /tmp/cliproxyapi.real.new \"$TARGET\"; chmod +x \"$TARGET\"."),
		neoReadThreadTextMessage("assistant", "M-ship-5", "Validation and restart: brew services restart cliproxyapi; brew services list | rg 'cliproxyapi|Name'; pgrep -fl cliproxyapi; /opt/homebrew/bin/cliproxyapi -version; curl -fsS http://127.0.0.1:8317/healthz."),
	}
}

func neoReadThreadDeploymentRedactionMessages() []any {
	return []any{
		neoReadThreadTextMessage("user", "M-deploy-1", "Extract L4L5/Reconn production deployment details from this thread. Do not include secrets."),
		neoReadThreadTextMessage("assistant", "M-deploy-2", "Production host is server.vela.partners. Dokploy app id is dokploy-l4l5-prod, web service is app-l4l5-web, container is l4l5-web, domain is https://reconn.vela.partners."),
		neoReadThreadTextMessage("assistant", "M-deploy-3", "Required env var names: POSTGRES_URL, DATABASE_URL, REDIS_URL, NEXT_PUBLIC_APP_URL. The old Coolify DB proxy should not be used after the migration."),
		neoReadThreadToolResultMessage("M-deploy-4", "TU-deploy-env", "done", "POSTGRES_URL=postgres://l4l5:super-secret-password@db.internal:5432/l4l5\nSTRIPE_SECRET_KEY=sk_live_redacted_but_should_not_emit\nNEXT_PUBLIC_APP_URL=https://reconn.vela.partners"),
		neoReadThreadTextMessage("assistant", "M-deploy-5", "Commands used: ssh root@server.vela.partners, dokploy compose pull dokploy-l4l5-prod, dokploy compose up -d dokploy-l4l5-prod, docker logs l4l5-web --tail=200, curl -fsS https://reconn.vela.partners/healthz."),
		neoReadThreadTextMessage("user", "M-deploy-6", "Latest blocker: production still points at old POSTGRES_URL in Dokploy. Report env var names and commands only; redact actual values."),
	}
}

func neoReadThreadSameThreadCodeReviewMessages() []any {
	return []any{
		neoReadThreadTextMessage("user", "M-code-review-1", "Load the code-review skill and tell me if code_review becomes available."),
		neoReadThreadTextMessage("assistant", "M-code-review-2", "Before loading the skill, I do not see a callable code_review surface."),
		neoReadThreadTextMessage("assistant", "M-code-review-3", "Skill loaded: applying-review-checks. code_review is now exposed as a tool in this thread."),
		map[string]any{"role": "assistant", "messageId": "M-code-review-4", "content": []any{map[string]any{"type": "tool_use", "id": "TU-code-review", "name": "code_review", "input": map[string]any{"target": "HEAD", "checks": []any{"applying-review-checks"}, "scope": "uncommitted read_thread changes"}, "complete": true}}},
		neoReadThreadToolResultMessage("M-code-review-5", "TU-code-review", "done", "review completed with no blocking findings"),
		neoReadThreadTextMessage("user", "M-code-review-6", "Find the exact invocation and input fields. Did it appear as name code_review in the content array or as a function namespace?"),
	}
}

func neoReadThreadRepositorySwitchMessages() []any {
	return []any{
		neoReadThreadTextMessage("user", "M-repo-1", "Should we keep working on the existing telemetry.dev repo?"),
		neoReadThreadTextMessage("assistant", "M-repo-2", "Initial recommendation: continue in the existing telemetry.dev repo because current patches and project context already live there."),
		neoReadThreadTextMessage("user", "M-repo-3", "Duncan shared a new repo: https://github.com/telemetry-dev/telemetry2 based on https://github.com/traceloop/openllmetry."),
		neoReadThreadTextMessage("assistant", "M-repo-4", "Revised recommendation: switch future work to https://github.com/telemetry-dev/telemetry2. Reason: telemetry2 inherits traceloop/openllmetry and aligns with OpenTelemetry conventions, while the existing telemetry.dev repo should only receive critical carry-over patches."),
		neoReadThreadTextMessage("user", "M-repo-5", "Latest: make the handoff clear that the final recommendation is switching to telemetry2, not continuing the old repo."),
	}
}

func neoReadThreadMobileContinuationMessages() []any {
	return []any{
		neoReadThreadTextMessage("user", "M-mobile-1", "Continuing work from another thread. When you lack specific information you can use read_thread to get it."),
		neoReadThreadTextMessage("assistant", "M-mobile-2", "Completed fixes: changed ComplaintData.age from Int? to String? on Android and iOS because the API returns values like \"28.0\"."),
		neoReadThreadTextMessage("assistant", "M-mobile-3", "Files involved include android/app/src/main/java/org/pywe/pharst/data/model/ComplaintData.kt, ios/PharstCare/PharstCare/Data/Models/ComplaintData.swift, android/app/src/main/java/org/pywe/pharst/ui/healthline/HealthlineViewModel.kt, and ios/PharstCare/PharstCare/Features/Healthline/HealthlineViewModel.swift."),
		neoReadThreadTextMessage("assistant", "M-mobile-4", "Integrated Sentry SDK versions Android 8.32.0 and iOS 9.4.1. Added captureException/capture calls across Healthline, Chat, Auth, Shop, Victoria, and PowerGirl ViewModels."),
		neoReadThreadTextMessage("assistant", "M-mobile-5", "Validation context: fixed CI credentials with MATCH_GIT_BASIC_AUTHORIZATION. Camera permission and chat animateScrollToItem crashes were already handled in the previous session."),
		neoReadThreadTextMessage("user", "M-mobile-6", "Current task is to commit and merge the remaining Sentry integration, ComplaintData fix, and broad error capture changes into the dev branch."),
	}
}

func neoReadThreadSummaryMessage(id, text string) map[string]any {
	return map[string]any{"role": "info", "messageId": id, "content": []any{map[string]any{"type": "summary", "summary": map[string]any{"type": "message", "summary": text}}}}
}

func neoReadThreadToolResultMessage(id, toolUseID, status, text string) map[string]any {
	run := map[string]any{"status": status}
	if status == "error" {
		run["error"] = map[string]any{"message": text}
	} else {
		run["output"] = text
	}
	return map[string]any{"role": "user", "messageId": id, "content": []any{map[string]any{"type": "tool_result", "toolUseID": toolUseID, "run": run}}}
}

func neoReadThreadTextMessage(role, id, text string) map[string]any {
	return map[string]any{"role": role, "messageId": id, "content": []any{map[string]any{"type": "text", "text": text}}}
}
