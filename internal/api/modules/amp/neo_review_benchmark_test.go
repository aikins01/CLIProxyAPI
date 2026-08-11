package amp

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
)

type neoReviewBenchmarkLane string

const (
	neoReviewBenchmarkCheckOnly  neoReviewBenchmarkLane = "check-only"
	neoReviewBenchmarkMainOnly   neoReviewBenchmarkLane = "main-review-only"
	neoReviewBenchmarkIntegrated neoReviewBenchmarkLane = "integrated"
)

type neoReviewBenchmarkCheck struct {
	Name     string
	Content  string
	Severity string
}

type neoReviewBenchmarkLocation struct {
	File      string
	StartLine int
	EndLine   int
}

type neoReviewBenchmarkExpectedFinding struct {
	ID                    string
	CheckName             string
	Locations             []neoReviewBenchmarkLocation
	Severity              string
	RequiredKeywordGroups [][]string
}

func TestNeoRunCheckExportStatusPredicateAcceptsDirectOwnershipChecks(t *testing.T) {
	accessPath, err := neoParseDependencyAccessPath("@example/sdk/subpath.js#Owner.call")
	if err != nil {
		t.Fatal(err)
	}
	for name, source := range map[string]string{
		"available":             `console.log(Object.hasOwn(pkg.exports, "./subpath.js") ? "AVAILABLE" : "MISSING")`,
		"missing":               `console.log(!Object.hasOwn(pkg.exports, "./subpath.js") ? "MISSING" : "AVAILABLE")`,
		"parenthesized missing": `console.log(!(Object.hasOwn(pkg.exports, "./subpath.js")) ? "MISSING" : "AVAILABLE")`,
		"assigned available":    `const available = Object.hasOwn(pkg.exports, "./subpath.js"); console.log(available ? "AVAILABLE" : "MISSING")`,
		"assigned missing":      `const missing = !Object.hasOwn(pkg.exports, "./subpath.js"); console.log(missing ? "MISSING" : "AVAILABLE")`,
	} {
		t.Run(name, func(t *testing.T) {
			executable := neoRunCheckASCIIFold(neoRunCheckDeclarationLexicalMask(source))
			for _, status := range []string{"AVAILABLE", "MISSING"} {
				if !neoRunCheckExportStatusPredicate(source, executable, status, accessPath) {
					t.Fatalf("direct ownership predicate rejected status %s", status)
				}
			}
		})
	}
	unrelated := `console.log(Object.hasOwn(cache, "./subpath.js") ? "AVAILABLE" : "MISSING")`
	if neoRunCheckExportStatusPredicate(unrelated, neoRunCheckASCIIFold(neoRunCheckDeclarationLexicalMask(unrelated)), "AVAILABLE", accessPath) {
		t.Fatal("unrelated ownership predicate was accepted as package export evidence")
	}
	unrelated = `console.log(Object.hasOwn(otherPkg.exports, "./subpath.js") ? "AVAILABLE" : "MISSING")`
	if neoRunCheckExportStatusPredicate(unrelated, neoRunCheckASCIIFold(neoRunCheckDeclarationLexicalMask(unrelated)), "AVAILABLE", accessPath) {
		t.Fatal("unrelated package ownership predicate was accepted as exact-floor export evidence")
	}
}

func TestNeoRunCheckExactExportStatusRequiresFloorBoundProvenance(t *testing.T) {
	accessPath, err := neoParseDependencyAccessPath("@example/sdk/subpath.js#Owner.call")
	if err != nil {
		t.Fatal(err)
	}
	command := `set -e; load-exact-floor @example/sdk@1.0.0 @example/sdk/subpath.js#Owner.call; console.log(Object.hasOwn(pkg.exports, "./subpath.js") ? "AVAILABLE" : "MISSING")`
	evidence := map[string]any{"tool": "shell_command", "input": `{"command":` + strconv.Quote(command) + `}`}
	if reason := neoRunCheckUnsafeStatusAssertion(evidence, "@example/sdk/subpath.js#Owner.call=AVAILABLE", accessPath, "AVAILABLE", "exact-export-inspection"); !strings.Contains(reason, "bound to the exact-floor package export map") {
		t.Fatalf("unbound exact-export assertion reason = %q", reason)
	}
	command = `set -e; load-exact-floor @example/sdk@1.0.0 @example/sdk/subpath.js#Owner.call; console.log('CLIPROXY_PACKAGE_EXPORTS=' + JSON.stringify(pkg.exports)); console.log(Object.hasOwn(pkg.exports, "./subpath.js") ? "AVAILABLE" : "MISSING")`
	evidence["input"] = `{"command":` + strconv.Quote(command) + `}`
	output := "CLIPROXY_PACKAGE_EXPORTS={\"./subpath.js\":\"./subpath.js\"}\n@example/sdk/subpath.js#Owner.call=AVAILABLE"
	if reason := neoRunCheckUnsafeStatusAssertion(evidence, output, accessPath, "AVAILABLE", "exact-export-inspection"); reason != "" {
		t.Fatalf("bound exact-export assertion reason = %q", reason)
	}
}

type neoReviewBenchmarkCase struct {
	Name            string
	Family          string
	Control         bool
	Diff            string
	Files           []string
	Evidence        string
	Check           neoReviewBenchmarkCheck
	Checks          []neoReviewBenchmarkCheck
	Expected        []neoReviewBenchmarkExpectedFinding
	ForbiddenClaims []string
}

type neoReviewBenchmarkObservedFinding struct {
	Owner       string `json:"owner"`
	CheckName   string `json:"checkName,omitempty"`
	File        string `json:"file"`
	StartLine   int    `json:"startLine"`
	EndLine     int    `json:"endLine"`
	Severity    string `json:"severity,omitempty"`
	CommentType string `json:"commentType,omitempty"`
	Text        string `json:"text"`
	Why         string `json:"why,omitempty"`
	Fix         string `json:"fix,omitempty"`
}

type neoReviewBenchmarkProtocol struct {
	ExpectedRunCheckCalls int      `json:"expectedRunCheckCalls"`
	ExpectedSubmitCalls   int      `json:"expectedSubmitCalls"`
	ExpectedMainTurns     int      `json:"expectedMainTurns"`
	ExpectedCheckTurns    int      `json:"expectedCheckTurns"`
	EmittedRunCheckCalls  int      `json:"emittedRunCheckCalls"`
	PersistedRunCheckUses int      `json:"persistedRunCheckUses"`
	RunCheckResults       int      `json:"runCheckResults"`
	EmittedSubmitCalls    int      `json:"emittedSubmitCalls"`
	PersistedSubmitUses   int      `json:"persistedSubmitUses"`
	SubmitResults         int      `json:"submitResults"`
	MainTurns             int      `json:"mainTurns"`
	CheckTurns            int      `json:"checkTurns"`
	UnknownToolCalls      int      `json:"unknownToolCalls"`
	DefinitionMismatches  int      `json:"definitionMismatches"`
	MainPromptErrors      int      `json:"mainPromptErrors"`
	CheckPromptErrors     int      `json:"checkPromptErrors"`
	CheckToolErrors       int      `json:"checkToolErrors"`
	ResultOrderingErrors  int      `json:"resultOrderingErrors"`
	CheckErrors           int      `json:"checkErrors"`
	SubmitErrors          int      `json:"submitErrors"`
	SubmitAfterChecks     bool     `json:"submitAfterChecks"`
	Failures              []string `json:"failures,omitempty"`
}

type neoReviewBenchmarkScore struct {
	Expected           int      `json:"expected"`
	Matched            int      `json:"matched"`
	Missed             int      `json:"missed"`
	FalsePositives     int      `json:"falsePositives"`
	Duplicates         int      `json:"duplicates"`
	OwnershipErrors    int      `json:"ownershipErrors"`
	SeverityErrors     int      `json:"severityErrors"`
	SeverityPromotions int      `json:"severityPromotions"`
	SeverityDemotions  int      `json:"severityDemotions"`
	UnsupportedClaims  int      `json:"unsupportedClaims"`
	NonActionable      int      `json:"nonActionable"`
	Recall             float64  `json:"recall"`
	Precision          float64  `json:"precision"`
	Passed             bool     `json:"passed"`
	Failures           []string `json:"failures,omitempty"`
}

type neoReviewBenchmarkRunResult struct {
	Candidate      string                              `json:"candidate"`
	Provider       string                              `json:"provider"`
	Model          string                              `json:"model"`
	Effort         string                              `json:"effort,omitempty"`
	MainEffort     string                              `json:"mainEffort,omitempty"`
	CheckEffort    string                              `json:"checkEffort,omitempty"`
	Lane           neoReviewBenchmarkLane              `json:"lane"`
	Case           string                              `json:"case"`
	Family         string                              `json:"family"`
	Control        bool                                `json:"control"`
	Rep            int                                 `json:"rep"`
	Passed         bool                                `json:"passed"`
	DurationMillis int64                               `json:"durationMillis"`
	InputTokens    int                                 `json:"inputTokens"`
	OutputTokens   int                                 `json:"outputTokens"`
	MainScore      *neoReviewBenchmarkScore            `json:"mainScore,omitempty"`
	CheckScore     *neoReviewBenchmarkScore            `json:"checkScore,omitempty"`
	FinalScore     neoReviewBenchmarkScore             `json:"finalScore"`
	Protocol       neoReviewBenchmarkProtocol          `json:"protocol"`
	Findings       []neoReviewBenchmarkObservedFinding `json:"findings"`
	Error          string                              `json:"error,omitempty"`
}

type neoReviewBenchmarkObserver struct {
	mu                   sync.Mutex
	mainTurns            int
	checkTurns           int
	emittedRunCheckCalls int
	emittedSubmitCalls   int
	unknownToolCalls     int
	definitionMismatches int
	mainPromptErrors     int
	checkPromptErrors    int
	checkToolErrors      int
	inputTokens          int
	outputTokens         int
	failures             []string
}

type neoReviewBenchmarkActorResults struct {
	mainFindings  []neoReviewBenchmarkObservedFinding
	checkFindings []neoReviewBenchmarkObservedFinding
	protocol      neoReviewBenchmarkProtocol
}

func TestNeoReviewBenchmarkFixtures(t *testing.T) {
	cases := neoReviewBenchmarkCases()
	if len(cases) < 10 {
		t.Fatalf("review benchmark cases = %d, want at least 10", len(cases))
	}
	required := map[string]map[bool]bool{
		"stale-retained-classification": {},
		"external-variant-completeness": {},
		"replacement-budget-accounting": {},
		"published-capability-floor":    {},
	}
	reflectivePositive := false
	privateLockControl := false
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			if strings.TrimSpace(tc.Name) == "" || strings.TrimSpace(tc.Family) == "" || strings.TrimSpace(tc.Diff) == "" || len(tc.Files) == 0 {
				t.Fatalf("incomplete fixture: %#v", tc)
			}
			checks := neoReviewBenchmarkChecks(tc)
			if len(checks) == 0 {
				t.Fatal("fixture has no checks")
			}
			for _, check := range checks {
				if check.Name == "" || strings.TrimSpace(check.Content) == "" || !neoReviewSeverityValid(check.Severity) {
					t.Fatalf("invalid check fixture: %#v", check)
				}
			}
			if tc.Control && len(tc.Expected) != 0 {
				t.Fatalf("control fixture has expected findings: %#v", tc.Expected)
			}
			if !tc.Control && len(tc.Expected) == 0 {
				t.Fatal("positive fixture has no expected findings")
			}
			snapshot := neoReviewBenchmarkSnapshot(tc)
			if !neoReviewSameStringSet(snapshot.Files, tc.Files) {
				t.Fatalf("snapshot files = %v, want %v", snapshot.Files, tc.Files)
			}
			changedLines := make([]string, 0)
			deletedLines := make([]string, 0)
			for _, hunk := range snapshot.Hunks {
				changedLines = append(changedLines, hunk.Changed...)
				deletedLines = append(deletedLines, hunk.Deleted...)
			}
			for _, expected := range tc.Expected {
				if expected.ID == "" || len(expected.Locations) == 0 || !neoReviewSeverityValid(expected.Severity) || len(expected.RequiredKeywordGroups) == 0 {
					t.Fatalf("invalid expected finding: %#v", expected)
				}
				for _, location := range expected.Locations {
					if !slices.Contains(tc.Files, location.File) || location.StartLine < 0 || location.EndLine < location.StartLine {
						t.Fatalf("invalid expected location: %#v", location)
					}
					changedLocation := false
					for line := location.StartLine; line <= location.EndLine; line++ {
						if neoReviewRangeWithinSnapshotHunk(location.File, line, line, changedLines) || neoReviewRangeWithinSnapshotHunk(location.File, line, line, deletedLines) {
							changedLocation = true
							break
						}
					}
					if !changedLocation {
						t.Fatalf("expected location has no changed line: %#v", location)
					}
				}
			}
		})
		if byControl, ok := required[tc.Family]; ok {
			byControl[tc.Control] = true
		}
		if tc.Name == "published capability floor reflective call" {
			reflectivePositive = !tc.Control
		}
		if tc.Name == "private lock update has no published floor" {
			privateLockControl = tc.Control
			for _, evidence := range []string{
				"+        specifier: 1.8.0\n+        version: 1.8.0",
				"packages:\n\n-  stream-core@1.7.0:\n-    resolution: {integrity: sha512-",
				"+  stream-core@1.8.0:\n+    resolution: {integrity: sha512-",
				"snapshots:\n\n-  stream-core@1.7.0: {}\n+  stream-core@1.8.0: {}",
			} {
				if !strings.Contains(tc.Diff, evidence) {
					t.Fatalf("private lock control is missing complete lockfile evidence %q", evidence)
				}
			}
		}
	}
	for family, byControl := range required {
		if !byControl[false] || !byControl[true] {
			t.Fatalf("fixture family %q positive=%v control=%v", family, byControl[false], byControl[true])
		}
	}
	if !reflectivePositive || !privateLockControl {
		t.Fatalf("capability fixtures reflectivePositive=%v privateLockControl=%v", reflectivePositive, privateLockControl)
	}
}

func TestNeoReviewBenchmarkScoreSeparatesFailureModes(t *testing.T) {
	tc := neoReviewBenchmarkCases()[0]
	checkOwner := "check:" + tc.Check.Name
	correct := neoReviewBenchmarkObservedFinding{
		Owner:     checkOwner,
		CheckName: tc.Check.Name,
		File:      "internal/client/client.go",
		StartLine: 11,
		EndLine:   11,
		Severity:  "medium",
		Text:      "Changing the endpoint leaves the retained sandbox classification stale.",
		Why:       "Requests can keep using test credentials after the source endpoint changes.",
		Fix:       "Recompute the derived classification when SetEndpoint changes its source.",
	}
	protocol := neoReviewBenchmarkProtocol{ExpectedRunCheckCalls: 1, ExpectedSubmitCalls: 1, EmittedRunCheckCalls: 1, PersistedRunCheckUses: 1, RunCheckResults: 1, EmittedSubmitCalls: 1, PersistedSubmitUses: 1, SubmitResults: 1, SubmitAfterChecks: true}
	protocol = neoReviewBenchmarkFinalizeProtocol(protocol)
	score := neoReviewBenchmarkScoreCase(tc, neoReviewBenchmarkIntegrated, []neoReviewBenchmarkObservedFinding{correct}, protocol.Failures)
	if !score.Passed || score.Recall != 1 || score.Precision != 1 {
		t.Fatalf("correct score = %#v", score)
	}

	duplicate := correct
	duplicate.Owner = "main"
	duplicate.CheckName = ""
	score = neoReviewBenchmarkScoreCase(tc, neoReviewBenchmarkIntegrated, []neoReviewBenchmarkObservedFinding{correct, duplicate}, protocol.Failures)
	if score.Passed || score.Duplicates != 1 || score.OwnershipErrors != 1 || score.Matched != 1 {
		t.Fatalf("duplicate score = %#v", score)
	}

	wrongSeverity := correct
	wrongSeverity.Severity = "high"
	score = neoReviewBenchmarkScoreCase(tc, neoReviewBenchmarkIntegrated, []neoReviewBenchmarkObservedFinding{wrongSeverity}, protocol.Failures)
	if score.Passed || score.SeverityErrors != 1 || score.SeverityPromotions != 1 || score.Matched != 1 {
		t.Fatalf("severity score = %#v", score)
	}

	control := neoReviewBenchmarkCases()[1]
	score = neoReviewBenchmarkScoreCase(control, neoReviewBenchmarkIntegrated, []neoReviewBenchmarkObservedFinding{correct}, protocol.Failures)
	if score.Passed || score.FalsePositives != 1 || score.Recall != 1 {
		t.Fatalf("control score = %#v", score)
	}

	reflective := neoReviewBenchmarkCaseByName(t, "published capability floor reflective call")
	unsupported := neoReviewBenchmarkObservedFinding{
		Owner:     "check:" + reflective.Check.Name,
		CheckName: reflective.Check.Name,
		File:      "packages/bridge/src/resume.ts",
		StartLine: 6,
		EndLine:   6,
		Severity:  "high",
		Text:      "This compile-time failure comes from calling resumeStream, introduced in 1.8, through a cast while the published floor remains 1.4.",
	}
	score = neoReviewBenchmarkScoreCase(reflective, neoReviewBenchmarkCheckOnly, []neoReviewBenchmarkObservedFinding{unsupported}, nil)
	if score.Passed || score.UnsupportedClaims != 1 || score.Matched != 1 {
		t.Fatalf("unsupported claim score = %#v", score)
	}
	unsupported.Text = "The resumeStream method was introduced in 1.8, so the 1.4 peer floor causes a runtime failure through this cast, not a compile-time failure."
	score = neoReviewBenchmarkScoreCase(reflective, neoReviewBenchmarkCheckOnly, []neoReviewBenchmarkObservedFinding{unsupported}, nil)
	if !score.Passed || score.UnsupportedClaims != 0 || score.Matched != 1 {
		t.Fatalf("negated unsupported claim score = %#v", score)
	}

	nonActionable := correct
	nonActionable.Owner = "main"
	nonActionable.CheckName = ""
	nonActionable.CommentType = "compliment"
	score = neoReviewBenchmarkScoreCase(tc, neoReviewBenchmarkMainOnly, []neoReviewBenchmarkObservedFinding{nonActionable}, nil)
	if score.Passed || score.Matched != 0 || score.NonActionable != 1 {
		t.Fatalf("non-actionable score = %#v", score)
	}
}

func TestNeoReviewBenchmarkProtocolRequiresExactLifecycle(t *testing.T) {
	protocol := neoReviewBenchmarkFinalizeProtocol(neoReviewBenchmarkProtocol{
		ExpectedRunCheckCalls: 1,
		ExpectedSubmitCalls:   1,
		ExpectedMainTurns:     3,
		ExpectedCheckTurns:    1,
		EmittedRunCheckCalls:  2,
		PersistedRunCheckUses: 2,
		RunCheckResults:       2,
		EmittedSubmitCalls:    1,
		PersistedSubmitUses:   1,
		SubmitResults:         1,
		MainTurns:             3,
		CheckTurns:            1,
		SubmitAfterChecks:     false,
	})
	if len(protocol.Failures) < 2 {
		t.Fatalf("protocol failures = %#v", protocol.Failures)
	}
	protocol = neoReviewBenchmarkFinalizeProtocol(neoReviewBenchmarkProtocol{
		ExpectedRunCheckCalls: 0,
		ExpectedSubmitCalls:   1,
		ExpectedMainTurns:     2,
		EmittedSubmitCalls:    1,
		PersistedSubmitUses:   1,
		SubmitResults:         1,
		MainTurns:             2,
		SubmitAfterChecks:     true,
	})
	if len(protocol.Failures) != 0 {
		t.Fatalf("clean protocol = %#v", protocol)
	}
}

func TestNeoReviewBenchmarkCheckAllowlist(t *testing.T) {
	tc := neoReviewBenchmarkCases()[0]
	want := neoReviewBenchmarkCheckInput(tc)
	raw, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]any{}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if err := neoReviewBenchmarkValidateCheckInput(got, want); err != nil {
		t.Fatalf("exact benchmark check rejected: %v", err)
	}
	delete(got, "checkContent")
	if err := neoReviewBenchmarkValidateCheckInput(got, want); err == nil {
		t.Fatal("missing embedded check content was accepted")
	}
	got = map[string]any{}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	mapValue(got["frontmatter"])["tools"] = []any{"shell_command"}
	if err := neoReviewBenchmarkValidateCheckInput(got, want); err == nil {
		t.Fatal("filesystem-capable benchmark check was accepted")
	}
}

func TestNeoReviewBenchmarkIntegratedDoesNotMaskCheckMiss(t *testing.T) {
	tc := neoReviewBenchmarkCases()[0]
	mainFinding := neoReviewBenchmarkObservedFinding{
		Owner:     "main",
		File:      "internal/client/client.go",
		StartLine: 11,
		EndLine:   11,
		Severity:  "medium",
		Text:      "Changing the endpoint leaves the retained sandbox classification stale.",
		Why:       "Requests can keep using test credentials after the source endpoint changes.",
		Fix:       "Recompute the derived classification when SetEndpoint changes its source.",
	}
	checkScore := neoReviewBenchmarkScoreCase(tc, neoReviewBenchmarkCheckOnly, nil, nil)
	finalScore := neoReviewBenchmarkScoreCase(tc, neoReviewBenchmarkIntegrated, []neoReviewBenchmarkObservedFinding{mainFinding}, nil)
	if checkScore.Recall != 0 || finalScore.Recall != 1 || finalScore.OwnershipErrors != 1 || finalScore.Passed {
		t.Fatalf("check score = %#v, final score = %#v", checkScore, finalScore)
	}
}

func TestNeoReviewBenchmarkProductionPromptSource(t *testing.T) {
	request := neoInferenceRequest{AgentMode: "review", Settings: map[string]any{}}
	route := neoModelRoute{Provider: "openai", Model: "gpt-5.5"}
	if prompt := neoSystemPrompt(request, route); !strings.Contains(prompt, neoReviewPrompt()) {
		t.Fatal("review request did not source its prompt from neoReviewPrompt")
	}
	if strings.TrimSpace(neoRunCheckSubagentPrompt) == "" {
		t.Fatal("production run_check prompt is empty")
	}
}

func TestNeoReviewDurabilityModelBenchmark(t *testing.T) {
	if !neoReadThreadTruthyEnv("AMP_REVIEW_MODEL_BENCHMARK") {
		t.Skip("set AMP_REVIEW_MODEL_BENCHMARK=1 to run the live three-lane review benchmark")
	}
	useTempNeoThreadStore(t)
	candidates := neoRunCheckBenchmarkCandidates(t)
	cases := neoReviewBenchmarkSelectedCases(t)
	lanes := neoReviewBenchmarkSelectedLanes(t)
	repetitions := neoRunCheckBenchmarkRepetitions(t)
	strict := neoReadThreadTruthyEnv("AMP_REVIEW_MODEL_BENCHMARK_STRICT")
	runs := make([]neoReviewBenchmarkRunResult, 0, len(candidates)*len(cases)*len(lanes)*repetitions)
	failedRuns := 0
	for caseIndex, tc := range cases {
		for rep := 1; rep <= repetitions; rep++ {
			candidateOffset := (caseIndex + rep - 1) % len(candidates)
			for candidateIndex := range candidates {
				candidate := candidates[(candidateIndex+candidateOffset)%len(candidates)]
				laneOffset := (caseIndex + rep + candidateIndex) % len(lanes)
				for laneIndex := range lanes {
					lane := lanes[(laneIndex+laneOffset)%len(lanes)]
					result := neoReviewBenchmarkRunLane(t, candidate, tc, lane, rep)
					runs = append(runs, result)
					raw, _ := json.Marshal(result)
					t.Log(string(raw))
					if !result.Passed {
						failedRuns++
						if strict {
							t.Fatalf("benchmark miss: %s", string(raw))
						}
					}
				}
			}
		}
	}
	summary, _ := json.Marshal(neoReviewBenchmarkSummary(runs))
	t.Log(string(summary))
	if failedRuns != 0 {
		t.Fatalf("%d live review benchmark runs failed", failedRuns)
	}
}

func TestNeoReviewStrictIntegratedBenchmark(t *testing.T) {
	if !neoReadThreadTruthyEnv("AMP_REVIEW_STRICT_INTEGRATED_BENCHMARK") {
		t.Skip("set AMP_REVIEW_STRICT_INTEGRATED_BENCHMARK=1 to run the strict integrated durability benchmark")
	}
	useTempNeoThreadStore(t)
	names := []string{
		"OpenRouter floor uses beta Responses resource",
		"OpenRouter floor exposes direct Responses resource",
		"terminal output reuses delta budget",
		"terminal output receives replacement budget",
		"partial Responses stream omits non-text events",
		"partial Responses stream preserves non-text events",
		"wrapper captures mutable provider at wrap time",
		"wrapper resolves mutable provider per call",
	}
	cases := make([]neoReviewBenchmarkCase, 0, len(names))
	positives := 0
	controls := 0
	for _, name := range names {
		tc := neoReviewBenchmarkCaseByName(t, name)
		cases = append(cases, tc)
		if tc.Control {
			controls++
		} else {
			positives++
		}
	}
	if positives != 4 || controls != 4 {
		t.Fatalf("strict fixture positives=%d controls=%d, want 4 and 4", positives, controls)
	}

	const repetitions = 2
	failedRuns := 0
	runs := make([]neoReviewBenchmarkRunResult, 0, len(cases)*repetitions)
	for _, candidate := range neoRunCheckBenchmarkCandidates(t) {
		for _, tc := range cases {
			for rep := 1; rep <= repetitions; rep++ {
				result := neoReviewBenchmarkRunActor(t, candidate, tc, neoReviewBenchmarkIntegrated, rep)
				runs = append(runs, result)
				raw, _ := json.Marshal(result)
				t.Log(string(raw))
				if !result.Passed {
					failedRuns++
				}
			}
		}
	}
	summary, _ := json.Marshal(neoReviewBenchmarkSummary(runs))
	t.Log(string(summary))
	if failedRuns != 0 {
		t.Fatalf("%d strict integrated durability benchmark runs failed", failedRuns)
	}
}

func neoReviewBenchmarkRunLane(t *testing.T, candidate neoRunCheckBenchmarkCandidate, tc neoReviewBenchmarkCase, lane neoReviewBenchmarkLane, rep int) neoReviewBenchmarkRunResult {
	t.Helper()
	if lane == neoReviewBenchmarkCheckOnly {
		return neoReviewBenchmarkRunCheckOnly(t, candidate, tc, rep)
	}
	return neoReviewBenchmarkRunActor(t, candidate, tc, lane, rep)
}

func neoReviewBenchmarkRunCheckOnly(t *testing.T, candidate neoRunCheckBenchmarkCandidate, tc neoReviewBenchmarkCase, rep int) neoReviewBenchmarkRunResult {
	t.Helper()
	started := time.Now()
	rt := newNeoRuntime(neoRunCheckBenchmarkConfig(t))
	input := neoReviewBenchmarkCheckInput(tc)
	input, err := neoPrepareRunCheckSnapshotInput(input, neoReviewBenchmarkSnapshot(tc))
	if err != nil {
		t.Fatalf("prepare immutable benchmark snapshot: %v", err)
	}
	systemPrompt := strings.NewReplacer(
		"{{WORKING_DIR}}", "/benchmark/repository",
		"{{WORKSPACE_ROOT}}", "/benchmark/repository",
	).Replace(neoRunCheckSubagentPrompt) + "\n\nThe immutable review snapshot is embedded in the request. Do not call tools."
	route := candidate.Route
	effectiveEffort := neoSubagentEffectiveReasoningEffort("run_check", route, candidate.Effort, "")
	settings := map[string]any{}
	if effectiveEffort != "" {
		settings["reasoning.effort"] = effectiveEffort
	}
	ctx, cancel := context.WithTimeout(context.Background(), neoReviewBenchmarkTimeout(t))
	defer cancel()
	threadID := "T-" + randomUUIDLike()
	inference, inferErr := inferNeoLocalStream(rt, neoInferenceRequest{
		Context:              ctx,
		ActorID:              "actor-" + randomBase62(22),
		ThreadID:             threadID,
		MessageID:            newNeoMessageID(),
		AgentMode:            "review",
		ReasoningEffort:      effectiveEffort,
		Settings:             settings,
		History:              []neoHistoryMessage{{Role: "user", Text: neoSubagentInputText("run_check", input)}},
		Environment:          map[string]any{"workingDirectory": "/benchmark/repository", "workspaceRoot": "/benchmark/repository"},
		ModelRouteOverride:   &route,
		SystemPromptOverride: systemPrompt,
	}, func(neoInferenceDelta) {})
	structured := neoRunCheckResultFromText(input, inference.Text)
	findings := neoReviewBenchmarkFindingsFromCheck(tc.Check.Name, structured)
	protocol := neoReviewBenchmarkFinalizeProtocol(neoReviewBenchmarkProtocol{SubmitAfterChecks: true})
	executionFailures := append([]string(nil), protocol.Failures...)
	if inferErr != nil {
		executionFailures = append(executionFailures, "check-only inference failed")
	}
	if stringValue(structured["status"]) != "completed" {
		executionFailures = append(executionFailures, "check-only result was not completed")
	}
	score := neoReviewBenchmarkScoreCase(tc, neoReviewBenchmarkCheckOnly, findings, executionFailures)
	result := neoReviewBenchmarkRunResult{
		Candidate:      candidate.Name,
		Provider:       candidate.Route.Provider,
		Model:          candidate.Route.Model,
		Effort:         candidate.Effort,
		CheckEffort:    effectiveEffort,
		Lane:           neoReviewBenchmarkCheckOnly,
		Case:           tc.Name,
		Family:         tc.Family,
		Control:        tc.Control,
		Rep:            rep,
		Passed:         score.Passed,
		DurationMillis: time.Since(started).Milliseconds(),
		InputTokens:    neoRunCheckBenchmarkUsageInt(inference.Usage, "inputTokens", "input_tokens", "prompt_tokens", "promptTokenCount"),
		OutputTokens:   neoRunCheckBenchmarkUsageInt(inference.Usage, "outputTokens", "output_tokens", "completion_tokens", "candidatesTokenCount"),
		CheckScore:     &score,
		FinalScore:     score,
		Protocol:       protocol,
		Findings:       findings,
	}
	if inferErr != nil {
		result.Error = inferErr.Error()
	}
	if stringValue(structured["status"]) != "completed" {
		result.Passed = false
		if result.Error == "" {
			result.Error = stringValue(structured["errorMessage"])
		}
	}
	return result
}

func neoReviewBenchmarkRunActor(t *testing.T, candidate neoRunCheckBenchmarkCandidate, tc neoReviewBenchmarkCase, lane neoReviewBenchmarkLane, rep int) neoReviewBenchmarkRunResult {
	t.Helper()
	started := time.Now()
	cfg := neoRunCheckBenchmarkConfig(t)
	routeText := neoReviewBenchmarkRouteText(candidate.Route)
	cfg.AmpCode.NeoLocalRuntime.ModeModelRoutes = map[string]config.ModelRouteList{"review": {routeText}}
	cfg.AmpCode.NeoLocalRuntime.SubagentModels = map[string][]string{"run_check": {routeText}}
	rt := newNeoRuntime(cfg)
	observer := &neoReviewBenchmarkObserver{}
	rt.inferStream = neoReviewBenchmarkInferObserver(observer, tc, lane)

	threadID := "T-" + randomUUIDLike()
	actorID := "actor-" + randomBase62(22)
	actor := newNeoActor(rt, actorID, "thread-actor", threadID, threadID, neoActorRecord(actorID, "thread-actor", threadID), nil)
	actor.executorReady = true
	actor.executorBootstrapComplete = true
	actor.settings = map[string]any{
		"tools.disable": []any{"shell_command", "create_thread", "list_agent_modes", "list_runners"},
	}
	actor.environment = map[string]any{"workingDirectory": "/benchmark/repository", "workspaceRoot": "/benchmark/repository"}
	rootMessageID := "M-review-benchmark-root"
	actor.messages = []neoMessage{{
		ThreadID:  threadID,
		MessageID: rootMessageID,
		Role:      "user",
		Content:   []any{map[string]any{"type": "text", "text": neoReviewBenchmarkUserPrompt(tc, lane)}},
	}}
	actor.reviewSnapshot = neoReviewBenchmarkSnapshot(tc)
	actor.reviewSnapshotDescription = neoReviewBenchmarkDiffDescription(tc)
	actor.reviewSnapshotRootMessageID = rootMessageID
	actor.reviewSnapshotScope = append([]string(nil), tc.Files...)
	actor.rebuildHistoryLocked()
	t.Cleanup(func() { actor.cancel() })

	go actor.runInferenceWithOptions("review", candidate.Effort, neoInferenceRunOptions{skipPreflightCompaction: true})
	completed := neoWaitFor(neoReviewBenchmarkTimeout(t), func() bool {
		observer.mu.Lock()
		startedInference := observer.mainTurns > 0
		observer.mu.Unlock()
		actor.mu.Lock()
		defer actor.mu.Unlock()
		return startedInference && actor.currentInference == nil && len(actor.pendingTools) == 0 && len(actor.subagentRuns) == 0 && actor.agentState == "idle"
	})
	if !completed {
		actor.cancel()
	}

	actorResults := neoReviewBenchmarkCollectActorResults(actor, lane, tc)
	observer.mu.Lock()
	actorResults.protocol.EmittedRunCheckCalls = observer.emittedRunCheckCalls
	actorResults.protocol.EmittedSubmitCalls = observer.emittedSubmitCalls
	actorResults.protocol.MainTurns = observer.mainTurns
	actorResults.protocol.CheckTurns = observer.checkTurns
	actorResults.protocol.UnknownToolCalls += observer.unknownToolCalls
	actorResults.protocol.DefinitionMismatches += observer.definitionMismatches
	actorResults.protocol.MainPromptErrors += observer.mainPromptErrors
	actorResults.protocol.CheckPromptErrors += observer.checkPromptErrors
	actorResults.protocol.CheckToolErrors += observer.checkToolErrors
	actorResults.protocol.Failures = append(actorResults.protocol.Failures, observer.failures...)
	inputTokens := observer.inputTokens
	outputTokens := observer.outputTokens
	observer.mu.Unlock()
	if !completed {
		actorResults.protocol.Failures = append(actorResults.protocol.Failures, "review actor did not reach idle before the benchmark deadline")
	}
	actorResults.protocol = neoReviewBenchmarkFinalizeProtocol(actorResults.protocol)

	mainProtocolFailures := neoReviewBenchmarkMainProtocolFailures(actorResults.protocol)
	checkProtocolFailures := neoReviewBenchmarkCheckProtocolFailures(actorResults.protocol)
	mainScore := neoReviewBenchmarkScoreCase(tc, neoReviewBenchmarkMainOnly, actorResults.mainFindings, mainProtocolFailures)
	checkScore := neoReviewBenchmarkScoreCase(tc, neoReviewBenchmarkCheckOnly, actorResults.checkFindings, checkProtocolFailures)
	findings := actorResults.mainFindings
	if lane == neoReviewBenchmarkIntegrated {
		findings = append(append([]neoReviewBenchmarkObservedFinding(nil), actorResults.checkFindings...), actorResults.mainFindings...)
	}
	finalScore := neoReviewBenchmarkScoreCase(tc, lane, findings, actorResults.protocol.Failures)
	passed := finalScore.Passed
	if lane == neoReviewBenchmarkIntegrated {
		passed = passed && checkScore.Passed
	}
	result := neoReviewBenchmarkRunResult{
		Candidate:      candidate.Name,
		Provider:       candidate.Route.Provider,
		Model:          candidate.Route.Model,
		Effort:         candidate.Effort,
		MainEffort:     strings.ToLower(strings.TrimSpace(neoEffectiveThinkingLevel(candidate.Route, candidate.Effort))),
		Lane:           lane,
		Case:           tc.Name,
		Family:         tc.Family,
		Control:        tc.Control,
		Rep:            rep,
		Passed:         passed,
		DurationMillis: time.Since(started).Milliseconds(),
		InputTokens:    inputTokens,
		OutputTokens:   outputTokens,
		MainScore:      &mainScore,
		FinalScore:     finalScore,
		Protocol:       actorResults.protocol,
		Findings:       findings,
	}
	if lane == neoReviewBenchmarkIntegrated {
		result.CheckEffort = neoSubagentEffectiveReasoningEffort("run_check", candidate.Route, candidate.Effort, "")
		result.CheckScore = &checkScore
	}
	actor.mu.Lock()
	if len(actor.activeError) != 0 {
		result.Error = stringValue(actor.activeError["message"])
		result.Passed = false
	}
	actor.mu.Unlock()
	if !completed && result.Error == "" {
		result.Error = "review actor timed out"
		result.Passed = false
	}
	return result
}

func neoReviewBenchmarkInferObserver(observer *neoReviewBenchmarkObserver, tc neoReviewBenchmarkCase, lane neoReviewBenchmarkLane) func(*neoRuntime, neoInferenceRequest, neoStreamCallback) (neoInferenceResult, error) {
	expectedInputs := map[string]map[string]any{}
	for _, input := range neoReviewBenchmarkCheckInputs(tc) {
		expectedInputs[stringValue(input["checkName"])] = input
	}
	return func(rt *neoRuntime, request neoInferenceRequest, callback neoStreamCallback) (neoInferenceResult, error) {
		main := request.ParentToolCallID == ""
		observer.mu.Lock()
		if main {
			observer.mainTurns++
			if request.SystemPromptOverride != "" {
				observer.mainPromptErrors++
				observer.failures = append(observer.failures, "main review used a system prompt override")
			}
			routes := neoInferenceModelRoutes(rt, request)
			if len(routes) == 0 || !strings.Contains(neoSystemPrompt(request, routes[0]), neoReviewPrompt()) {
				observer.mainPromptErrors++
				observer.failures = append(observer.failures, "main review did not use neoReviewPrompt")
			}
			toolNames := make([]string, 0, len(request.Tools))
			for _, tool := range request.Tools {
				toolNames = append(toolNames, tool.Name)
			}
			if !slices.Equal(toolNames, []string{"run_check", "submit_review"}) {
				observer.mainPromptErrors++
				observer.failures = append(observer.failures, fmt.Sprintf("main review tools = %v, want [run_check submit_review]", toolNames))
			}
		} else {
			observer.checkTurns++
			expectedPrompt := strings.NewReplacer(
				"{{WORKING_DIR}}", firstNonEmptyString(stringValue(request.Environment["workingDirectory"]), "unknown"),
				"{{WORKSPACE_ROOT}}", firstNonEmptyString(stringValue(request.Environment["workspaceRoot"]), "unknown"),
			).Replace(neoRunCheckSubagentPrompt)
			if expectedPrompt == "" || !strings.Contains(request.SystemPromptOverride, expectedPrompt) {
				observer.checkPromptErrors++
				observer.failures = append(observer.failures, "run_check did not use the production subagent prompt")
			}
			if len(request.Tools) != 0 {
				observer.checkToolErrors++
				observer.failures = append(observer.failures, fmt.Sprintf("run_check tools = %d, want 0", len(request.Tools)))
			}
		}
		observer.mu.Unlock()

		result, err := inferNeoLocalStream(rt, request, callback)
		observer.mu.Lock()
		observer.inputTokens += neoRunCheckBenchmarkUsageInt(result.Usage, "inputTokens", "input_tokens", "prompt_tokens", "promptTokenCount")
		observer.outputTokens += neoRunCheckBenchmarkUsageInt(result.Usage, "outputTokens", "output_tokens", "completion_tokens", "candidatesTokenCount")
		unsafeCall := false
		if main {
			for _, call := range result.ToolCalls {
				switch call.Name {
				case "run_check":
					observer.emittedRunCheckCalls++
					if lane != neoReviewBenchmarkIntegrated {
						observer.definitionMismatches++
						observer.failures = append(observer.failures, "main-only review emitted an unexpected run_check")
						unsafeCall = true
					} else if expectedInput := expectedInputs[stringValue(call.Input["checkName"])]; expectedInput == nil {
						observer.definitionMismatches++
						observer.failures = append(observer.failures, "main review emitted an unknown run_check definition")
						unsafeCall = true
					} else if validationErr := neoReviewBenchmarkValidateCheckInput(call.Input, expectedInput); validationErr != nil {
						observer.definitionMismatches++
						observer.failures = append(observer.failures, validationErr.Error())
						unsafeCall = true
					}
				case "submit_review":
					observer.emittedSubmitCalls++
				default:
					observer.unknownToolCalls++
					observer.failures = append(observer.failures, "main review emitted unavailable tool "+call.Name)
					unsafeCall = true
				}
			}
		}
		observer.mu.Unlock()
		if err != nil {
			return result, err
		}
		if unsafeCall {
			return result, fmt.Errorf("benchmark rejected an unsafe or mismatched review tool call")
		}
		return result, nil
	}
}

func neoReviewBenchmarkValidateCheckInput(got, want map[string]any) error {
	for _, key := range []string{"checkName", "checkURI", "checkContent", "diffDescription", "instructions"} {
		if stringValue(got[key]) != stringValue(want[key]) {
			return fmt.Errorf("run_check %s = %q, want %q", key, stringValue(got[key]), stringValue(want[key]))
		}
	}
	gotFiles, gotFilesOK := neoRunCheckStringSlice(got["files"])
	wantFiles, wantFilesOK := neoRunCheckStringSlice(want["files"])
	if !gotFilesOK || !wantFilesOK || !neoReviewSameStringSet(gotFiles, wantFiles) {
		return fmt.Errorf("run_check files = %#v, want %#v", got["files"], want["files"])
	}
	gotFrontmatter := mapValue(got["frontmatter"])
	wantFrontmatter := mapValue(want["frontmatter"])
	for _, key := range []string{"name", "severity-default"} {
		if stringValue(gotFrontmatter[key]) != stringValue(wantFrontmatter[key]) {
			return fmt.Errorf("run_check frontmatter %s = %q, want %q", key, stringValue(gotFrontmatter[key]), stringValue(wantFrontmatter[key]))
		}
	}
	if tools, exists := gotFrontmatter["tools"]; !exists || arrayValue(tools) == nil || len(arrayValue(tools)) != 0 {
		return fmt.Errorf("run_check frontmatter tools = %#v, want explicit empty array", gotFrontmatter["tools"])
	}
	return nil
}

func neoReviewBenchmarkCollectActorResults(actor *neoActor, lane neoReviewBenchmarkLane, tc neoReviewBenchmarkCase) neoReviewBenchmarkActorResults {
	actor.mu.Lock()
	messages := append([]neoMessage(nil), actor.messages...)
	actor.mu.Unlock()
	protocol := neoReviewBenchmarkProtocol{ExpectedSubmitCalls: 1, ExpectedMainTurns: 2, SubmitAfterChecks: true}
	if lane == neoReviewBenchmarkIntegrated {
		protocol.ExpectedRunCheckCalls = len(neoReviewBenchmarkChecks(tc))
		protocol.ExpectedMainTurns = 3
		protocol.ExpectedCheckTurns = protocol.ExpectedRunCheckCalls
	}
	type toolUse struct {
		name  string
		input map[string]any
		seq   int
	}
	uses := map[string]toolUse{}
	results := map[string]map[string]any{}
	resultSeq := map[string]int{}
	for _, message := range messages {
		if message.ParentToolUseID != "" {
			continue
		}
		for _, raw := range message.Content {
			block := mapValue(raw)
			switch stringValue(block["type"]) {
			case "tool_use":
				name := stringValue(block["name"])
				id := stringValue(block["id"])
				uses[id] = toolUse{name: name, input: mapValue(block["input"]), seq: message.Seq}
				switch name {
				case "run_check":
					protocol.PersistedRunCheckUses++
				case "submit_review":
					protocol.PersistedSubmitUses++
				default:
					protocol.UnknownToolCalls++
				}
			case "tool_result":
				id := stringValue(block["toolUseID"])
				results[id] = mapValue(block["run"])
				resultSeq[id] = message.Seq
			}
		}
	}
	mainFindings := make([]neoReviewBenchmarkObservedFinding, 0)
	checkFindings := make([]neoReviewBenchmarkObservedFinding, 0)
	maxCheckResultSeq := 0
	firstSubmitUseSeq := 0
	useIDs := make([]string, 0, len(uses))
	for id := range uses {
		useIDs = append(useIDs, id)
	}
	sort.Slice(useIDs, func(i, j int) bool {
		left := uses[useIDs[i]]
		right := uses[useIDs[j]]
		if left.seq != right.seq {
			return left.seq < right.seq
		}
		return useIDs[i] < useIDs[j]
	})
	for _, id := range useIDs {
		use := uses[id]
		run, hasResult := results[id]
		if hasResult && resultSeq[id] <= use.seq {
			protocol.ResultOrderingErrors++
		}
		switch use.name {
		case "run_check":
			if !hasResult {
				continue
			}
			protocol.RunCheckResults++
			maxCheckResultSeq = max(maxCheckResultSeq, resultSeq[id])
			structured := mapValue(run["result"])
			if stringValue(run["status"]) != "done" || stringValue(structured["status"]) != "completed" {
				protocol.CheckErrors++
				if message := firstNonEmptyString(stringValue(run["error"]), stringValue(structured["errorMessage"])); message != "" {
					protocol.Failures = append(protocol.Failures, "run_check failed: "+message)
				}
			}
			checkName := stringValue(use.input["checkName"])
			checkFindings = append(checkFindings, neoReviewBenchmarkFindingsFromCheck(checkName, structured)...)
		case "submit_review":
			if firstSubmitUseSeq == 0 || use.seq < firstSubmitUseSeq {
				firstSubmitUseSeq = use.seq
			}
			if !hasResult {
				continue
			}
			protocol.SubmitResults++
			if stringValue(run["status"]) != "done" {
				protocol.SubmitErrors++
				if message := firstNonEmptyString(stringValue(run["error"]), stringValue(mapValue(run["result"])["errorMessage"])); message != "" {
					protocol.Failures = append(protocol.Failures, "submit_review failed: "+message)
				}
				continue
			}
			mainFindings = append(mainFindings, neoReviewBenchmarkFindingsFromSubmit(mapValue(run["result"]))...)
		}
	}
	if protocol.ExpectedRunCheckCalls > 0 && (firstSubmitUseSeq == 0 || maxCheckResultSeq == 0 || firstSubmitUseSeq <= maxCheckResultSeq) {
		protocol.SubmitAfterChecks = false
	}
	return neoReviewBenchmarkActorResults{mainFindings: mainFindings, checkFindings: checkFindings, protocol: protocol}
}

func neoReviewBenchmarkFinalizeProtocol(protocol neoReviewBenchmarkProtocol) neoReviewBenchmarkProtocol {
	failures := append([]string(nil), protocol.Failures...)
	if protocol.EmittedRunCheckCalls != protocol.ExpectedRunCheckCalls {
		failures = append(failures, fmt.Sprintf("emitted run_check calls = %d, want %d", protocol.EmittedRunCheckCalls, protocol.ExpectedRunCheckCalls))
	}
	if protocol.PersistedRunCheckUses != protocol.ExpectedRunCheckCalls {
		failures = append(failures, fmt.Sprintf("persisted run_check uses = %d, want %d", protocol.PersistedRunCheckUses, protocol.ExpectedRunCheckCalls))
	}
	if protocol.RunCheckResults != protocol.ExpectedRunCheckCalls {
		failures = append(failures, fmt.Sprintf("run_check results = %d, want %d", protocol.RunCheckResults, protocol.ExpectedRunCheckCalls))
	}
	if protocol.EmittedSubmitCalls != protocol.ExpectedSubmitCalls {
		failures = append(failures, fmt.Sprintf("emitted submit_review calls = %d, want %d", protocol.EmittedSubmitCalls, protocol.ExpectedSubmitCalls))
	}
	if protocol.PersistedSubmitUses != protocol.ExpectedSubmitCalls {
		failures = append(failures, fmt.Sprintf("persisted submit_review uses = %d, want %d", protocol.PersistedSubmitUses, protocol.ExpectedSubmitCalls))
	}
	if protocol.SubmitResults != protocol.ExpectedSubmitCalls {
		failures = append(failures, fmt.Sprintf("submit_review results = %d, want %d", protocol.SubmitResults, protocol.ExpectedSubmitCalls))
	}
	if protocol.ExpectedSubmitCalls > 0 && !protocol.SubmitAfterChecks {
		failures = append(failures, "submit_review did not run after all checks completed")
	}
	if protocol.UnknownToolCalls != 0 {
		failures = append(failures, fmt.Sprintf("unknown tool calls = %d", protocol.UnknownToolCalls))
	}
	if protocol.DefinitionMismatches != 0 {
		failures = append(failures, fmt.Sprintf("run_check definition mismatches = %d", protocol.DefinitionMismatches))
	}
	if protocol.CheckErrors != 0 {
		failures = append(failures, fmt.Sprintf("run_check errors = %d", protocol.CheckErrors))
	}
	if protocol.SubmitErrors != 0 {
		failures = append(failures, fmt.Sprintf("submit_review errors = %d", protocol.SubmitErrors))
	}
	if protocol.MainTurns != protocol.ExpectedMainTurns {
		failures = append(failures, fmt.Sprintf("main review turns = %d, want %d", protocol.MainTurns, protocol.ExpectedMainTurns))
	}
	if protocol.CheckTurns != protocol.ExpectedCheckTurns {
		failures = append(failures, fmt.Sprintf("run_check turns = %d, want %d", protocol.CheckTurns, protocol.ExpectedCheckTurns))
	}
	if protocol.MainPromptErrors != 0 {
		failures = append(failures, fmt.Sprintf("main prompt errors = %d", protocol.MainPromptErrors))
	}
	if protocol.CheckPromptErrors != 0 {
		failures = append(failures, fmt.Sprintf("run_check prompt errors = %d", protocol.CheckPromptErrors))
	}
	if protocol.CheckToolErrors != 0 {
		failures = append(failures, fmt.Sprintf("run_check tool exposure errors = %d", protocol.CheckToolErrors))
	}
	if protocol.ResultOrderingErrors != 0 {
		failures = append(failures, fmt.Sprintf("tool result ordering errors = %d", protocol.ResultOrderingErrors))
	}
	protocol.Failures = neoReviewBenchmarkUniqueStrings(failures)
	return protocol
}

func neoReviewBenchmarkMainProtocolFailures(protocol neoReviewBenchmarkProtocol) []string {
	component := protocol
	component.ExpectedCheckTurns = 0
	component.CheckTurns = 0
	component.CheckPromptErrors = 0
	component.CheckToolErrors = 0
	component.CheckErrors = 0
	component.Failures = nil
	return neoReviewBenchmarkFinalizeProtocol(component).Failures
}

func neoReviewBenchmarkCheckProtocolFailures(protocol neoReviewBenchmarkProtocol) []string {
	component := neoReviewBenchmarkProtocol{
		ExpectedRunCheckCalls: protocol.ExpectedRunCheckCalls,
		ExpectedCheckTurns:    protocol.ExpectedCheckTurns,
		EmittedRunCheckCalls:  protocol.EmittedRunCheckCalls,
		PersistedRunCheckUses: protocol.PersistedRunCheckUses,
		RunCheckResults:       protocol.RunCheckResults,
		CheckTurns:            protocol.CheckTurns,
		DefinitionMismatches:  protocol.DefinitionMismatches,
		CheckPromptErrors:     protocol.CheckPromptErrors,
		CheckToolErrors:       protocol.CheckToolErrors,
		ResultOrderingErrors:  protocol.ResultOrderingErrors,
		CheckErrors:           protocol.CheckErrors,
		SubmitAfterChecks:     true,
	}
	return neoReviewBenchmarkFinalizeProtocol(component).Failures
}

func neoReviewBenchmarkScoreCase(tc neoReviewBenchmarkCase, lane neoReviewBenchmarkLane, observed []neoReviewBenchmarkObservedFinding, protocolFailures []string) neoReviewBenchmarkScore {
	score := neoReviewBenchmarkScore{Expected: len(tc.Expected), Recall: 1, Precision: 1}
	matchedObserved := make([]bool, len(observed))
	for _, expected := range tc.Expected {
		matchedIndex := -1
		for index, finding := range observed {
			if !matchedObserved[index] && neoReviewBenchmarkFindingMatches(expected, finding) {
				matchedIndex = index
				break
			}
		}
		if matchedIndex < 0 {
			score.Missed++
			continue
		}
		matchedObserved[matchedIndex] = true
		score.Matched++
		finding := observed[matchedIndex]
		if finding.Severity != expected.Severity {
			score.SeverityErrors++
			gotRank := neoReviewBenchmarkSeverityRank(finding.Severity)
			wantRank := neoReviewBenchmarkSeverityRank(expected.Severity)
			if gotRank > wantRank {
				score.SeverityPromotions++
			} else if gotRank < wantRank {
				score.SeverityDemotions++
			}
		}
	}
	for index, finding := range observed {
		matchesExpected := false
		for _, expected := range tc.Expected {
			contentMatches := neoReviewBenchmarkFindingContentMatches(expected, finding)
			if contentMatches && !neoReviewBenchmarkFindingActionable(finding) {
				score.NonActionable++
				break
			}
			if contentMatches {
				matchesExpected = true
				checkName := firstNonEmptyString(expected.CheckName, tc.Check.Name)
				if !neoReviewBenchmarkOwnerAllowed(lane, checkName, finding.Owner) {
					score.OwnershipErrors++
				}
				break
			}
		}
		if !matchedObserved[index] {
			if matchesExpected {
				score.Duplicates++
			} else {
				score.FalsePositives++
			}
		}
		text := strings.ToLower(strings.Join([]string{finding.Text, finding.Why, finding.Fix}, " "))
		for _, claim := range tc.ForbiddenClaims {
			if neoReviewBenchmarkContainsUnsupportedClaim(text, claim) {
				score.UnsupportedClaims++
				break
			}
		}
	}
	if score.Expected > 0 {
		score.Recall = float64(score.Matched) / float64(score.Expected)
	}
	denominator := score.Matched + score.FalsePositives + score.Duplicates
	if denominator > 0 {
		score.Precision = float64(score.Matched) / float64(denominator)
	} else if score.Expected > 0 {
		score.Precision = 0
	}
	failures := append([]string(nil), protocolFailures...)
	failureCounts := []struct {
		count   int
		message string
	}{
		{count: score.Missed, message: "missing expected root causes"},
		{count: score.FalsePositives, message: "false-positive root causes"},
		{count: score.Duplicates, message: "duplicate root causes"},
		{count: score.OwnershipErrors, message: "incorrect finding owners"},
		{count: score.SeverityErrors, message: "severity mismatches"},
		{count: score.UnsupportedClaims, message: "unsupported claims"},
		{count: score.NonActionable, message: "non-actionable expected root causes"},
	}
	for _, failure := range failureCounts {
		if failure.count > 0 {
			failures = append(failures, fmt.Sprintf("%d %s", failure.count, failure.message))
		}
	}
	score.Failures = neoReviewBenchmarkUniqueStrings(failures)
	score.Passed = len(score.Failures) == 0
	return score
}

func neoReviewBenchmarkFindingMatches(expected neoReviewBenchmarkExpectedFinding, observed neoReviewBenchmarkObservedFinding) bool {
	return neoReviewBenchmarkFindingActionable(observed) && neoReviewBenchmarkFindingContentMatches(expected, observed)
}

func neoReviewBenchmarkFindingContentMatches(expected neoReviewBenchmarkExpectedFinding, observed neoReviewBenchmarkObservedFinding) bool {
	locationMatches := false
	for _, location := range expected.Locations {
		if location.File != observed.File {
			continue
		}
		observedEnd := observed.EndLine
		if observedEnd == 0 {
			observedEnd = observed.StartLine
		}
		if observed.StartLine <= location.EndLine && observedEnd >= location.StartLine {
			locationMatches = true
			break
		}
	}
	if !locationMatches {
		return false
	}
	normalized := strings.ToLower(strings.Join([]string{observed.Text, observed.Why, observed.Fix}, " "))
	for _, group := range expected.RequiredKeywordGroups {
		matched := false
		for _, keyword := range group {
			if strings.Contains(normalized, strings.ToLower(keyword)) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

func neoReviewBenchmarkFindingActionable(observed neoReviewBenchmarkObservedFinding) bool {
	return observed.Owner != "main" || (observed.CommentType != "compliment" && observed.CommentType != "non_actionable")
}

func neoReviewBenchmarkContainsUnsupportedClaim(text, claim string) bool {
	claim = strings.ToLower(claim)
	for start := 0; ; {
		index := strings.Index(text[start:], claim)
		if index < 0 {
			return false
		}
		index += start
		prefixStart := max(0, index-48)
		prefix := text[prefixStart:index]
		negated := strings.Contains(prefix, "not ") || strings.Contains(prefix, "isn't ") || strings.Contains(prefix, "is not ") || strings.Contains(prefix, "doesn't ") || strings.Contains(prefix, "does not ") || strings.Contains(prefix, "rather than ")
		if !negated {
			return true
		}
		start = index + len(claim)
	}
}

func neoReviewBenchmarkOwnerAllowed(lane neoReviewBenchmarkLane, checkName, owner string) bool {
	switch lane {
	case neoReviewBenchmarkMainOnly:
		return owner == "main"
	case neoReviewBenchmarkCheckOnly, neoReviewBenchmarkIntegrated:
		return owner == "check:"+checkName
	default:
		return false
	}
}

func neoReviewBenchmarkSeverityRank(severity string) int {
	switch severity {
	case "low":
		return 1
	case "medium":
		return 2
	case "high":
		return 3
	case "critical":
		return 4
	default:
		return 0
	}
}

func neoReviewBenchmarkFindingsFromCheck(checkName string, result map[string]any) []neoReviewBenchmarkObservedFinding {
	findings := make([]neoReviewBenchmarkObservedFinding, 0)
	for _, raw := range arrayValue(result["issues"]) {
		issue := mapValue(raw)
		startLine := int(numberFrom(issue["line"]))
		endLine := int(numberFrom(issue["endLine"]))
		if endLine == 0 {
			endLine = startLine
		}
		findings = append(findings, neoReviewBenchmarkObservedFinding{
			Owner:     "check:" + checkName,
			CheckName: checkName,
			File:      stringValue(issue["file"]),
			StartLine: startLine,
			EndLine:   endLine,
			Severity:  stringValue(issue["severity"]),
			Text:      stringValue(issue["problem"]),
			Why:       stringValue(issue["why"]),
			Fix:       stringValue(issue["fix"]),
		})
	}
	return findings
}

func neoReviewBenchmarkFindingsFromSubmit(result map[string]any) []neoReviewBenchmarkObservedFinding {
	findings := make([]neoReviewBenchmarkObservedFinding, 0)
	for _, raw := range arrayValue(result["comments"]) {
		comment := mapValue(raw)
		findings = append(findings, neoReviewBenchmarkObservedFinding{
			Owner:       "main",
			File:        stringValue(comment["filename"]),
			StartLine:   int(numberFrom(comment["startLine"])),
			EndLine:     int(numberFrom(comment["endLine"])),
			Severity:    stringValue(comment["severity"]),
			CommentType: stringValue(comment["commentType"]),
			Text:        stringValue(comment["text"]),
			Why:         stringValue(comment["why"]),
			Fix:         stringValue(comment["fix"]),
		})
	}
	return findings
}

func neoReviewBenchmarkCheckInput(tc neoReviewBenchmarkCase) map[string]any {
	checks := neoReviewBenchmarkChecks(tc)
	if len(checks) == 0 {
		return nil
	}
	return neoReviewBenchmarkCheckInputFor(tc, checks[0])
}

func neoReviewBenchmarkCheckInputs(tc neoReviewBenchmarkCase) []map[string]any {
	checks := neoReviewBenchmarkChecks(tc)
	inputs := make([]map[string]any, 0, len(checks))
	for _, check := range checks {
		inputs = append(inputs, neoReviewBenchmarkCheckInputFor(tc, check))
	}
	return inputs
}

func neoReviewBenchmarkCheckInputFor(tc neoReviewBenchmarkCase, check neoReviewBenchmarkCheck) map[string]any {
	instructions := "Evaluate only the immutable embedded diff. Report only concrete issues introduced by changed lines."
	if strings.TrimSpace(tc.Evidence) != "" {
		instructions += "\n\nAuthoritative external evidence for this fixture:\n" + strings.TrimSpace(tc.Evidence)
	}
	return map[string]any{
		"checkName":       check.Name,
		"checkURI":        "file:///benchmark/checks/" + check.Name + ".md",
		"checkContent":    check.Content,
		"frontmatter":     map[string]any{"name": check.Name, "severity-default": check.Severity, "tools": []any{}},
		"diffDescription": neoReviewBenchmarkDiffDescription(tc),
		"files":           stringArrayValue(tc.Files),
		"instructions":    instructions,
	}
}

func neoReviewBenchmarkChecks(tc neoReviewBenchmarkCase) []neoReviewBenchmarkCheck {
	if len(tc.Checks) != 0 {
		return tc.Checks
	}
	if tc.Check.Name == "" {
		return nil
	}
	return []neoReviewBenchmarkCheck{tc.Check}
}

func neoReviewBenchmarkDiffDescription(tc neoReviewBenchmarkCase) string {
	return "embedded synthetic diff for " + tc.Name
}

func neoReviewBenchmarkSnapshot(tc neoReviewBenchmarkCase) *neoReviewDiffSnapshot {
	return neoRunCheckBenchmarkSnapshot(neoRunCheckBenchmarkCase{Diff: tc.Diff, Files: tc.Files})
}

func neoReviewBenchmarkUserPrompt(tc neoReviewBenchmarkCase, lane neoReviewBenchmarkLane) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Review this diff: %s\n", neoReviewBenchmarkDiffDescription(tc))
	b.WriteString("Focus on these files:\n")
	for _, file := range tc.Files {
		b.WriteString(file)
		b.WriteByte('\n')
	}
	b.WriteString("Additional instructions from the user:\n")
	b.WriteString("The immutable diff below is the complete review scope. Do not regenerate it or read the working tree.\n")
	if lane == neoReviewBenchmarkIntegrated {
		b.WriteString("Pre-discovered review checks are listed below.\n")
		for _, input := range neoReviewBenchmarkCheckInputs(tc) {
			raw, _ := json.Marshal(input)
			b.WriteString("Call run_check exactly once with this exact JSON object:\n")
			b.WriteString("<review_check_arguments>")
			b.Write(raw)
			b.WriteString("</review_check_arguments>\n")
		}
	} else {
		b.WriteString("No review checks were pre-discovered by the CLI. Do not call run_check.\n")
	}
	b.WriteString("Remember: call submit_review exactly once.\n")
	b.WriteString("<review_diff_snapshot>\n")
	b.WriteString(tc.Diff)
	b.WriteString("\n</review_diff_snapshot>")
	return b.String()
}

func neoReviewBenchmarkRouteText(route neoModelRoute) string {
	model := route.Model
	if suffix := strings.TrimSpace(route.ThinkingSuffix); suffix != "" {
		model += "(" + suffix + ")"
	}
	if route.Provider == "" {
		return model
	}
	return route.Provider + "/" + model
}

func neoReviewBenchmarkTimeout(t *testing.T) time.Duration {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv("AMP_REVIEW_MODEL_BENCHMARK_TIMEOUT"))
	if raw == "" {
		return 5 * time.Minute
	}
	timeout, err := time.ParseDuration(raw)
	if err != nil || timeout <= 0 {
		t.Fatalf("AMP_REVIEW_MODEL_BENCHMARK_TIMEOUT = %q, want positive duration", raw)
	}
	return timeout
}

func neoReviewBenchmarkSelectedLanes(t *testing.T) []neoReviewBenchmarkLane {
	t.Helper()
	all := []neoReviewBenchmarkLane{neoReviewBenchmarkCheckOnly, neoReviewBenchmarkMainOnly, neoReviewBenchmarkIntegrated}
	raw := strings.TrimSpace(os.Getenv("AMP_REVIEW_MODEL_BENCHMARK_LANES"))
	if raw == "" {
		return all
	}
	wanted := map[neoReviewBenchmarkLane]bool{}
	for _, item := range strings.Split(raw, ",") {
		lane := neoReviewBenchmarkLane(strings.TrimSpace(item))
		if lane != "" {
			wanted[lane] = true
		}
	}
	selected := make([]neoReviewBenchmarkLane, 0, len(wanted))
	for _, lane := range all {
		if wanted[lane] {
			selected = append(selected, lane)
			delete(wanted, lane)
		}
	}
	if len(wanted) != 0 {
		unknown := make([]string, 0, len(wanted))
		for lane := range wanted {
			unknown = append(unknown, string(lane))
		}
		sort.Strings(unknown)
		t.Fatalf("unknown review benchmark lanes: %v", unknown)
	}
	if len(selected) == 0 {
		t.Fatal("AMP_REVIEW_MODEL_BENCHMARK_LANES selected no lanes")
	}
	return selected
}

func neoReviewBenchmarkSelectedCases(t *testing.T) []neoReviewBenchmarkCase {
	t.Helper()
	cases := neoReviewBenchmarkCases()
	raw := strings.TrimSpace(os.Getenv("AMP_REVIEW_MODEL_BENCHMARK_CASES"))
	if raw == "" {
		return cases
	}
	wanted := map[string]bool{}
	for _, item := range strings.Split(raw, ",") {
		if name := strings.TrimSpace(item); name != "" {
			wanted[name] = true
		}
	}
	selected := make([]neoReviewBenchmarkCase, 0, len(wanted))
	for _, tc := range cases {
		if wanted[tc.Name] {
			selected = append(selected, tc)
			delete(wanted, tc.Name)
		}
	}
	if len(wanted) != 0 {
		unknown := make([]string, 0, len(wanted))
		for name := range wanted {
			unknown = append(unknown, name)
		}
		sort.Strings(unknown)
		t.Fatalf("unknown review benchmark cases: %v", unknown)
	}
	if len(selected) == 0 {
		t.Fatal("AMP_REVIEW_MODEL_BENCHMARK_CASES selected no cases")
	}
	return selected
}

func neoReviewBenchmarkCaseByName(t *testing.T, name string) neoReviewBenchmarkCase {
	t.Helper()
	for _, tc := range neoReviewBenchmarkCases() {
		if tc.Name == name {
			return tc
		}
	}
	t.Fatalf("review benchmark case %q not found", name)
	return neoReviewBenchmarkCase{}
}

func neoReviewBenchmarkSummary(runs []neoReviewBenchmarkRunResult) map[string]any {
	type aggregate struct {
		Runs                  int       `json:"runs"`
		Passed                int       `json:"passed"`
		PassRate              float64   `json:"passRate"`
		RunsWithErrors        int       `json:"runsWithErrors"`
		ControlRuns           int       `json:"controlRuns"`
		ControlFalsePositives int       `json:"controlFalsePositives"`
		MainRecall            []float64 `json:"mainRecallDistribution,omitempty"`
		CheckRecall           []float64 `json:"checkRecallDistribution,omitempty"`
		FinalRecall           []float64 `json:"finalRecallDistribution"`
		FalsePositives        int       `json:"falsePositives"`
		Duplicates            int       `json:"duplicates"`
		OwnershipErrors       int       `json:"ownershipErrors"`
		SeverityErrors        int       `json:"severityErrors"`
		SeverityPromotions    int       `json:"severityPromotions"`
		SeverityDemotions     int       `json:"severityDemotions"`
		UnsupportedClaims     int       `json:"unsupportedClaims"`
		NonActionable         int       `json:"nonActionable"`
		ProtocolFailures      int       `json:"protocolFailures"`
	}
	groups := map[string]*aggregate{}
	for _, run := range runs {
		key := run.Candidate + "/" + string(run.Lane)
		group := groups[key]
		if group == nil {
			group = &aggregate{}
			groups[key] = group
		}
		group.Runs++
		if run.Passed {
			group.Passed++
		}
		if run.Error != "" {
			group.RunsWithErrors++
		}
		if run.Control {
			group.ControlRuns++
			group.ControlFalsePositives += run.FinalScore.FalsePositives
		}
		if run.MainScore != nil {
			group.MainRecall = append(group.MainRecall, run.MainScore.Recall)
		}
		if run.CheckScore != nil {
			group.CheckRecall = append(group.CheckRecall, run.CheckScore.Recall)
		}
		group.FinalRecall = append(group.FinalRecall, run.FinalScore.Recall)
		group.FalsePositives += run.FinalScore.FalsePositives
		group.Duplicates += run.FinalScore.Duplicates
		group.OwnershipErrors += run.FinalScore.OwnershipErrors
		group.SeverityErrors += run.FinalScore.SeverityErrors
		group.SeverityPromotions += run.FinalScore.SeverityPromotions
		group.SeverityDemotions += run.FinalScore.SeverityDemotions
		group.UnsupportedClaims += run.FinalScore.UnsupportedClaims
		group.NonActionable += run.FinalScore.NonActionable
		group.ProtocolFailures += len(run.Protocol.Failures)
	}
	out := map[string]any{"runs": len(runs), "groups": map[string]any{}}
	groupMap := out["groups"].(map[string]any)
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		group := groups[key]
		if group.Runs > 0 {
			group.PassRate = float64(group.Passed) / float64(group.Runs)
		}
		groupMap[key] = group
	}
	return out
}

func neoReviewBenchmarkUniqueStrings(values []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	return out
}

func TestNeoReviewOpenRouterCapabilityFloorArchives(t *testing.T) {
	if !neoReadThreadTruthyEnv("AMP_REVIEW_PACKAGE_ARCHIVE_TEST") {
		t.Skip("set AMP_REVIEW_PACKAGE_ARCHIVE_TEST=1 to inspect exact OpenRouter package archives")
	}

	npmFiles := map[string]string{
		"package/esm/sdk/sdk.d.ts":  "",
		"package/esm/sdk/beta.d.ts": "",
	}
	npmFloor := neoReviewBenchmarkNPMArchiveFiles(t, "@openrouter/sdk", "1.0.0", npmFiles)
	if !strings.Contains(npmFloor["package/esm/sdk/sdk.d.ts"], "get beta(): Beta") || strings.Contains(npmFloor["package/esm/sdk/sdk.d.ts"], "get responses(): Responses") {
		t.Fatalf("@openrouter/sdk@1.0.0 root client does not expose only the beta owner:\n%s", npmFloor["package/esm/sdk/sdk.d.ts"])
	}
	if !strings.Contains(npmFloor["package/esm/sdk/beta.d.ts"], "get responses(): Responses") {
		t.Fatalf("@openrouter/sdk@1.0.0 beta owner does not expose responses:\n%s", npmFloor["package/esm/sdk/beta.d.ts"])
	}
	npmControl := neoReviewBenchmarkNPMArchiveFiles(t, "@openrouter/sdk", "1.2.11", map[string]string{"package/esm/sdk/sdk.d.ts": ""})
	if !strings.Contains(npmControl["package/esm/sdk/sdk.d.ts"], "get responses(): Responses") {
		t.Fatalf("@openrouter/sdk@1.2.11 control lacks direct root responses:\n%s", npmControl["package/esm/sdk/sdk.d.ts"])
	}

	pythonFloorPaths := map[string]string{
		"openrouter-1.0.0/src/openrouter/sdk.py":  "",
		"openrouter-1.0.0/src/openrouter/beta.py": "",
	}
	pythonFloor := neoReviewBenchmarkPyPIArchiveFiles(t, "openrouter", "1.0.0", pythonFloorPaths)
	if !strings.Contains(pythonFloor["openrouter-1.0.0/src/openrouter/sdk.py"], `"beta": ("openrouter.beta", "Beta")`) || strings.Contains(pythonFloor["openrouter-1.0.0/src/openrouter/sdk.py"], `"responses": ("openrouter.responses", "Responses")`) {
		t.Fatalf("openrouter==1.0.0 root client does not expose only the beta owner:\n%s", pythonFloor["openrouter-1.0.0/src/openrouter/sdk.py"])
	}
	if !strings.Contains(pythonFloor["openrouter-1.0.0/src/openrouter/beta.py"], "self.responses = Responses") {
		t.Fatalf("openrouter==1.0.0 beta owner does not attach responses:\n%s", pythonFloor["openrouter-1.0.0/src/openrouter/beta.py"])
	}
	pythonControl := neoReviewBenchmarkPyPIArchiveFiles(t, "openrouter", "1.1.31", map[string]string{"openrouter-1.1.31/src/openrouter/sdk.py": ""})
	if !strings.Contains(pythonControl["openrouter-1.1.31/src/openrouter/sdk.py"], `"responses": ("openrouter.responses", "Responses")`) {
		t.Fatalf("openrouter==1.1.31 control lacks direct root responses:\n%s", pythonControl["openrouter-1.1.31/src/openrouter/sdk.py"])
	}
}

func neoReviewBenchmarkNPMArchiveFiles(t *testing.T, packageName, version string, wanted map[string]string) map[string]string {
	t.Helper()
	metadataURL := "https://registry.npmjs.org/" + strings.ReplaceAll(packageName, "/", "%2F") + "/" + version
	metadata := map[string]any{}
	if err := json.Unmarshal(neoReviewBenchmarkFetch(t, metadataURL, 2<<20), &metadata); err != nil {
		t.Fatalf("decode npm metadata for %s@%s: %v", packageName, version, err)
	}
	tarballURL := stringValue(mapValue(metadata["dist"])["tarball"])
	if tarballURL == "" {
		t.Fatalf("npm metadata for %s@%s omitted dist.tarball", packageName, version)
	}
	return neoReviewBenchmarkTarGzipFiles(t, neoReviewBenchmarkFetch(t, tarballURL, 32<<20), wanted)
}

func neoReviewBenchmarkPyPIArchiveFiles(t *testing.T, packageName, version string, wanted map[string]string) map[string]string {
	t.Helper()
	metadata := map[string]any{}
	metadataURL := fmt.Sprintf("https://pypi.org/pypi/%s/%s/json", packageName, version)
	if err := json.Unmarshal(neoReviewBenchmarkFetch(t, metadataURL, 2<<20), &metadata); err != nil {
		t.Fatalf("decode PyPI metadata for %s==%s: %v", packageName, version, err)
	}
	tarballURL := ""
	for _, raw := range arrayValue(metadata["urls"]) {
		file := mapValue(raw)
		if stringValue(file["packagetype"]) == "sdist" {
			tarballURL = stringValue(file["url"])
			break
		}
	}
	if tarballURL == "" {
		t.Fatalf("PyPI metadata for %s==%s omitted the source archive", packageName, version)
	}
	return neoReviewBenchmarkTarGzipFiles(t, neoReviewBenchmarkFetch(t, tarballURL, 32<<20), wanted)
}

func neoReviewBenchmarkFetch(t *testing.T, rawURL string, limit int64) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		t.Fatalf("create bounded archive request: %v", err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("fetch %s: %v", rawURL, err)
	}
	defer func() {
		if err := response.Body.Close(); err != nil {
			t.Errorf("close %s response: %v", rawURL, err)
		}
	}()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("fetch %s: status %s", rawURL, response.Status)
	}
	if response.ContentLength > limit {
		t.Fatalf("fetch %s: content length %d exceeds %d-byte limit", rawURL, response.ContentLength, limit)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
	if err != nil {
		t.Fatalf("read %s: %v", rawURL, err)
	}
	if int64(len(body)) > limit {
		t.Fatalf("fetch %s exceeded %d-byte limit", rawURL, limit)
	}
	return body
}

func neoReviewBenchmarkTarGzipFiles(t *testing.T, archive []byte, wanted map[string]string) map[string]string {
	t.Helper()
	reader, err := gzip.NewReader(bytes.NewReader(archive))
	if err != nil {
		t.Fatalf("open package archive: %v", err)
	}
	defer func() {
		if err := reader.Close(); err != nil {
			t.Errorf("close package archive: %v", err)
		}
	}()
	tarReader := tar.NewReader(reader)
	result := make(map[string]string, len(wanted))
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read package archive: %v", err)
		}
		if _, ok := wanted[header.Name]; !ok {
			continue
		}
		content, err := io.ReadAll(io.LimitReader(tarReader, 2<<20+1))
		if err != nil {
			t.Fatalf("read %s from package archive: %v", header.Name, err)
		}
		if len(content) > 2<<20 {
			t.Fatalf("package archive entry %s exceeded 2 MiB", header.Name)
		}
		result[header.Name] = string(content)
	}
	for name := range wanted {
		if _, ok := result[name]; !ok {
			t.Fatalf("package archive omitted %s", name)
		}
	}
	return result
}

func TestNeoReviewPR148FixtureScoring(t *testing.T) {
	tc := neoReviewPR148BenchmarkDefinition()
	if len(tc.Expected) != 7 || len(neoReviewBenchmarkChecks(tc)) != 4 {
		t.Fatalf("PR 148 fixture expected=%d checks=%d, want seven findings and four checks", len(tc.Expected), len(neoReviewBenchmarkChecks(tc)))
	}
	high := 0
	medium := 0
	for _, expected := range tc.Expected {
		switch expected.Severity {
		case "high":
			high++
		case "medium":
			medium++
		}
	}
	if high != 2 || medium != 5 {
		t.Fatalf("PR 148 severities high=%d medium=%d, want 2 and 5", high, medium)
	}
	golden := neoReviewPR148GoldenFindings()
	score := neoReviewBenchmarkScoreCase(tc, neoReviewBenchmarkIntegrated, golden, nil)
	if !score.Passed || score.Matched != 7 || score.Missed != 0 || score.FalsePositives != 0 || score.Duplicates != 0 || score.OwnershipErrors != 0 || score.SeverityErrors != 0 {
		t.Fatalf("PR 148 golden score = %#v", score)
	}

	wrongOwner := append([]neoReviewBenchmarkObservedFinding(nil), golden...)
	wrongOwner[0].Owner = "main"
	if score := neoReviewBenchmarkScoreCase(tc, neoReviewBenchmarkIntegrated, wrongOwner, nil); score.Passed || score.OwnershipErrors != 1 {
		t.Fatalf("PR 148 ownership mismatch score = %#v", score)
	}
	wrongSeverity := append([]neoReviewBenchmarkObservedFinding(nil), golden...)
	wrongSeverity[2].Severity = "high"
	if score := neoReviewBenchmarkScoreCase(tc, neoReviewBenchmarkIntegrated, wrongSeverity, nil); score.Passed || score.SeverityErrors != 1 || score.SeverityPromotions != 1 {
		t.Fatalf("PR 148 severity mismatch score = %#v", score)
	}
	duplicate := append(append([]neoReviewBenchmarkObservedFinding(nil), golden...), golden[0])
	if score := neoReviewBenchmarkScoreCase(tc, neoReviewBenchmarkIntegrated, duplicate, nil); score.Passed || score.Duplicates != 1 {
		t.Fatalf("PR 148 duplicate score = %#v", score)
	}
}

func TestNeoReviewPR148LargeSnapshotSurvivesRunCheckProviderRequest(t *testing.T) {
	tc := neoReviewPR148BenchmarkDefinition()
	snapshot := neoReviewBenchmarkSnapshot(tc)
	filler := strings.Repeat("+snapshot delivery filler for a large immutable review packet\n", 300)
	snapshot.Hunks = nil
	for _, filename := range snapshot.Files {
		snapshot.Diffs[filename] += "\n" + filler
		snapshot.Hunks = append(snapshot.Hunks, neoReviewDiffHunks(filename, snapshot.Diffs[filename])...)
	}

	var prepared map[string]any
	providerCalls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		providerCalls++
		if request.URL.Path != "/api/provider/openai/v1/responses" {
			t.Fatalf("provider path = %q", request.URL.Path)
		}
		payload := readNeoJSON(request.Body)
		userMessages := make([]map[string]any, 0, 1)
		for _, raw := range arrayValue(payload["input"]) {
			item := mapValue(raw)
			if stringValue(item["type"]) == "message" && stringValue(item["role"]) == "user" {
				userMessages = append(userMessages, item)
			}
		}
		if len(userMessages) != 1 {
			t.Fatalf("provider user messages = %d, want one authoritative request", len(userMessages))
		}
		content := arrayValue(userMessages[0]["content"])
		if len(content) != 1 || stringValue(mapValue(content[0])["type"]) != "input_text" {
			t.Fatalf("provider user content = %#v", content)
		}
		text := stringValue(mapValue(content[0])["text"])
		if len(text) < 64*1024 {
			t.Fatalf("provider snapshot request = %d bytes, want at least 64 KiB", len(text))
		}
		for _, want := range []string{
			"<check name=\"classifier-detector-matrix\"",
			"<review_diff_snapshot>",
			"client.responses",
			"budget.accept(response.output)",
			"self._budget.accept(output)",
			"provider_name = _provider_for_client",
		} {
			if !strings.Contains(text, want) {
				t.Fatalf("provider snapshot request missing %q", want)
			}
		}
		snapshotIndex := strings.Index(text, "<review_diff_snapshot>")
		checkIndex := strings.Index(text, "<check name=\"classifier-detector-matrix\"")
		finalDirectiveIndex := strings.Index(text, "Evaluate the check against the immutable snapshot above.")
		if snapshotIndex < 0 || checkIndex <= snapshotIndex || finalDirectiveIndex <= checkIndex {
			t.Fatalf("provider snapshot/check ordering = snapshot:%d check:%d final:%d", snapshotIndex, checkIndex, finalDirectiveIndex)
		}

		result := map[string]any{
			"checkName":       "classifier-detector-matrix",
			"status":          "completed",
			"filesAnalyzed":   len(neoStringSlice(prepared[neoReviewSnapshotFilesKey])),
			"linesAnalyzed":   0,
			"coveredFiles":    stringArrayValue(neoStringSlice(prepared[neoReviewSnapshotFilesKey])),
			"coveredHunks":    stringArrayValue(neoStringSlice(prepared[neoReviewSnapshotHunksKey])),
			"patternsChecked": []any{"large immutable snapshot delivery"},
			"evidence": []any{map[string]any{
				"patternIndex":     0,
				"observation":      "The provider request retained the complete immutable snapshot.",
				"sources":          []any{"embedded review snapshot"},
				"outcome":          "no-finding",
				"issueIndexes":     []any{},
				"sourceLifetime":   "immutable",
				"decisionLifetime": "per-use",
				"mutationPath":     "none: snapshot text is not reassignable",
				"usePath":          "check evaluation consumes the snapshot",
			}},
			"issues": []any{},
		}
		resultJSON, err := json.Marshal(result)
		if err != nil {
			t.Fatal(err)
		}
		delta, _ := json.Marshal(map[string]any{"type": "response.output_text.delta", "output_index": 0, "content_index": 0, "delta": string(resultJSON)})
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintf(w, "data: %s\n\n", delta)
		fmt.Fprint(w, "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":1,\"output_tokens\":1},\"output\":[]}}\n\ndata: [DONE]\n\n")
	}))
	defer upstream.Close()

	rt := testNeoRuntimeForServer(t, upstream)
	actor := newNeoActor(rt, "actor-pr148-snapshot", "thread-actor", "T-pr148-snapshot", "T-pr148-snapshot", neoActorRecord("actor-pr148-snapshot", "thread-actor", "T-pr148-snapshot"), nil)
	actor.currentAgentMode = "review"
	actor.environment = map[string]any{"workingDirectory": "/benchmark/repository", "workspaceRoot": "/benchmark/repository"}
	rootMessageID := "M-pr148-snapshot-root"
	actor.messages = []neoMessage{{MessageID: rootMessageID, Role: "user"}}
	actor.reviewSnapshot = snapshot
	actor.reviewSnapshotDescription = neoReviewBenchmarkDiffDescription(tc)
	actor.reviewSnapshotRootMessageID = rootMessageID
	actor.reviewSnapshotScope = append([]string(nil), tc.Files...)
	t.Cleanup(actor.cancel)

	for _, input := range neoReviewBenchmarkCheckInputs(tc) {
		if stringValue(input["checkName"]) == "classifier-detector-matrix" {
			var err error
			prepared, err = actor.prepareRunCheckInputContext(context.Background(), input)
			if err != nil {
				t.Fatalf("prepare large PR 148 snapshot: %v", err)
			}
			break
		}
	}
	if prepared == nil {
		t.Fatal("classifier benchmark input missing")
	}
	text, err := actor.executeSubagentRun("run_check", prepared, "TU-pr148-snapshot", rootMessageID, actor.generation, 0, "")
	if err != nil {
		t.Fatalf("execute large PR 148 run_check: %v", err)
	}
	if providerCalls != 1 {
		t.Fatalf("provider calls = %d, want one", providerCalls)
	}
	if result := neoRunCheckResultFromText(prepared, text); stringValue(result["status"]) != "completed" {
		t.Fatalf("large PR 148 run_check result = %#v", result)
	}
}

func TestNeoReviewPR148LargeSnapshotSurvivesActorRunCheckLifecycle(t *testing.T) {
	tc := neoReviewPR148BenchmarkDefinition()
	snapshot := neoReviewBenchmarkSnapshot(tc)
	filler := strings.Repeat("+snapshot delivery filler for the actor lifecycle\n", 400)
	snapshot.Hunks = nil
	for _, filename := range snapshot.Files {
		snapshot.Diffs[filename] += "\n" + filler
		snapshot.Hunks = append(snapshot.Hunks, neoReviewDiffHunks(filename, snapshot.Diffs[filename])...)
	}

	inputs := neoReviewBenchmarkCheckInputs(tc)
	responses := make(map[string]string, len(inputs))
	toolInputs := make([]neoToolCall, 0, len(inputs))
	expectedHash := ""
	for index, input := range inputs {
		prepared, err := neoPrepareRunCheckSnapshotInput(input, snapshot)
		if err != nil {
			t.Fatalf("prepare expected lifecycle input: %v", err)
		}
		if expectedHash == "" {
			expectedHash = stringValue(prepared[neoReviewSnapshotHashKey])
		}
		checkName := stringValue(input["checkName"])
		evidence := map[string]any{
			"patternIndex": 0,
			"observation":  "The immutable actor snapshot was evaluated without a finding.",
			"sources":      []any{"embedded review snapshot"},
			"outcome":      "no-finding",
			"issueIndexes": []any{},
		}
		if checkName == "published-dependency-capability-floor" {
			evidence["dependency"] = "@openrouter/sdk"
			evidence["floorVersion"] = "1.0.0"
			evidence["accessPath"] = "client.beta.responses"
			evidence["floorStatus"] = "compatible"
			evidence["verification"] = "root-type-declaration"
			evidence["rootEvidence"] = []any{"root client type declares beta.responses"}
		}
		if checkName == "bounded-artifact-state-transitions" {
			evidence["phaseRelationship"] = "addition"
			evidence["budgetOrigin"] = "original"
			evidence["decisiveSequence"] = "before-state, candidate, accept decision, final state"
		}
		if checkName == "classifier-detector-matrix" {
			evidence["sourceLifetime"] = "immutable"
			evidence["decisionLifetime"] = "per-use"
			evidence["mutationPath"] = "none: snapshot text is not reassignable"
			evidence["usePath"] = "check evaluation consumes the snapshot"
		}
		patternsChecked := []any{"actor snapshot delivery"}
		if checkName == "published-dependency-capability-floor" {
			patternsChecked = []any{neoDependencyPatternKey("@openrouter/sdk", "1.0.0", "client.beta.responses")}
		}
		result := map[string]any{
			"checkName":       checkName,
			"status":          "completed",
			"filesAnalyzed":   len(neoStringSlice(prepared[neoReviewSnapshotFilesKey])),
			"linesAnalyzed":   0,
			"coveredFiles":    stringArrayValue(neoStringSlice(prepared[neoReviewSnapshotFilesKey])),
			"coveredHunks":    stringArrayValue(neoStringSlice(prepared[neoReviewSnapshotHunksKey])),
			"patternsChecked": patternsChecked,
			"evidence":        []any{evidence},
			"issues":          []any{},
		}
		if checkName == "published-dependency-capability-floor" {
			result["status"] = "error"
			result["patternsChecked"] = []any{}
			result["evidence"] = []any{}
			result["errorMessage"] = "required validation tool evidence is unavailable in the snapshot lifecycle fixture"
		}
		resultJSON, err := json.Marshal(result)
		if err != nil {
			t.Fatal(err)
		}
		toolCallID := fmt.Sprintf("TU-pr148-lifecycle-%d", index)
		responses[toolCallID] = string(resultJSON)
		toolInputs = append(toolInputs, neoToolCall{ID: toolCallID, Name: "run_check", Input: input})
	}

	type preparedCapture struct {
		hash       string
		textBytes  int
		files      int
		hunks      int
		anchorSeen bool
	}
	var captureMu sync.Mutex
	preparedByTool := make(map[string]preparedCapture, len(inputs))
	historyByTool := make(map[string][]neoHistoryMessage, len(inputs))
	observer := &neoSubagentRunObserver{prepared: func(toolCallID string, input map[string]any) {
		text := stringValue(input[neoReviewSnapshotTextKey])
		captureMu.Lock()
		preparedByTool[toolCallID] = preparedCapture{
			hash:       stringValue(input[neoReviewSnapshotHashKey]),
			textBytes:  len(text),
			files:      len(neoStringSlice(input[neoReviewSnapshotFilesKey])),
			hunks:      len(neoStringSlice(input[neoReviewSnapshotHunksKey])),
			anchorSeen: strings.Contains(text, "client.responses") && strings.Contains(text, "self._budget.accept(output)") && strings.Contains(text, "provider_name = _provider_for_client"),
		}
		captureMu.Unlock()
	}}
	if !neoSubagentRunObserverForTest.CompareAndSwap(nil, observer) {
		t.Fatal("subagent run observer already installed")
	}
	t.Cleanup(func() { neoSubagentRunObserverForTest.CompareAndSwap(observer, nil) })

	rt := newNeoRuntime(&config.Config{})
	mainTurns := 0
	rt.inferStream = func(_ *neoRuntime, request neoInferenceRequest, _ neoStreamCallback) (neoInferenceResult, error) {
		if response, ok := responses[request.ParentToolCallID]; ok {
			captureMu.Lock()
			historyByTool[request.ParentToolCallID] = append([]neoHistoryMessage(nil), request.History...)
			captureMu.Unlock()
			return neoInferenceResult{Text: response}, nil
		}
		captureMu.Lock()
		mainTurns++
		turn := mainTurns
		captureMu.Unlock()
		switch turn {
		case 1:
			return neoInferenceResult{ToolCalls: toolInputs}, nil
		case 2:
			return neoInferenceResult{ToolCalls: []neoToolCall{{ID: "TU-pr148-lifecycle-submit", Name: "submit_review", Input: map[string]any{"comments": []any{}}}}}, nil
		case 3:
			return neoInferenceResult{Text: "review submitted"}, nil
		default:
			return neoInferenceResult{}, fmt.Errorf("unexpected lifecycle main turn %d", turn)
		}
	}

	threadID := "T-pr148-lifecycle"
	actor := newNeoActor(rt, "actor-pr148-lifecycle", "thread-actor", threadID, threadID, neoActorRecord("actor-pr148-lifecycle", "thread-actor", threadID), nil)
	actor.executorReady = true
	actor.executorBootstrapComplete = true
	actor.currentAgentMode = "review"
	actor.environment = map[string]any{"workingDirectory": "/benchmark/repository", "workspaceRoot": "/benchmark/repository"}
	rootMessageID := "M-pr148-lifecycle-root"
	actor.messages = []neoMessage{{
		ThreadID:  threadID,
		MessageID: rootMessageID,
		Role:      "user",
		Content:   []any{map[string]any{"type": "text", "text": neoReviewBenchmarkUserPrompt(tc, neoReviewBenchmarkIntegrated)}},
	}}
	actor.reviewSnapshot = snapshot
	actor.reviewSnapshotDescription = neoReviewBenchmarkDiffDescription(tc)
	actor.reviewSnapshotRootMessageID = rootMessageID
	actor.reviewSnapshotScope = append([]string(nil), tc.Files...)
	actor.rebuildHistoryLocked()
	t.Cleanup(actor.cancel)

	go actor.runInferenceWithOptions("review", "medium", neoInferenceRunOptions{skipPreflightCompaction: true})
	if !neoWaitFor(5*time.Second, func() bool {
		captureMu.Lock()
		turns := mainTurns
		captureMu.Unlock()
		actor.mu.Lock()
		defer actor.mu.Unlock()
		return turns == 3 && actor.currentInference == nil && len(actor.pendingTools) == 0 && len(actor.subagentRuns) == 0 && actor.agentState == "idle"
	}) {
		actor.mu.Lock()
		defer actor.mu.Unlock()
		t.Fatalf("actor lifecycle did not finish: pending=%d subagents=%d state=%q", len(actor.pendingTools), len(actor.subagentRuns), actor.agentState)
	}

	expectedHunk := snapshot.Hunks[0].ID
	captureMu.Lock()
	defer captureMu.Unlock()
	if len(preparedByTool) != len(inputs) || len(historyByTool) != len(inputs) {
		t.Fatalf("lifecycle captures prepared=%d history=%d, want %d", len(preparedByTool), len(historyByTool), len(inputs))
	}
	for toolCallID := range responses {
		prepared := preparedByTool[toolCallID]
		if prepared.hash != expectedHash || prepared.textBytes < 64*1024 || prepared.files != len(snapshot.Files) || prepared.hunks == 0 || !prepared.anchorSeen {
			t.Errorf("prepared %s = %#v, want complete immutable snapshot", toolCallID, prepared)
		}
		history := historyByTool[toolCallID]
		if len(history) != 1 || history[0].Role != "user" {
			t.Errorf("provider history %s = %#v, want one user message", toolCallID, history)
			continue
		}
		text := history[0].Text
		for _, want := range []string{"sha256:" + expectedHash, "Changed lines (JSON objects with exact new-side locations):", "<review_diff_snapshot>", expectedHunk, `"line":629`, `"line":680`, `"line":722`, `"line":752`, `"line":894`, `"line":1058`, "client.responses", "self._budget.accept(output)", "provider_name = _provider_for_client", "Evaluate the check against the immutable snapshot above."} {
			if !strings.Contains(text, want) {
				t.Errorf("provider history %s missing %q", toolCallID, want)
			}
		}
		if len(text) < 64*1024 {
			t.Errorf("provider history %s = %d bytes, want at least 64 KiB", toolCallID, len(text))
		}
	}
}

func TestNeoReviewPR148ModelBenchmark(t *testing.T) {
	if !neoReadThreadTruthyEnv("AMP_REVIEW_PR148_MODEL_BENCHMARK") {
		t.Skip("set AMP_REVIEW_PR148_MODEL_BENCHMARK=1 to run the exact PR 148 integrated benchmark")
	}
	useTempNeoThreadStore(t)
	tc := neoReviewPR148BenchmarkDefinition()
	rawDiff := string(neoReviewPR148BenchmarkDiff(t))
	tc.Diff = neoReviewBenchmarkFilterDiff(rawDiff, tc.Files)
	snapshot := neoReviewBenchmarkSnapshot(tc)
	if !neoReviewSameStringSet(snapshot.Files, tc.Files) || len(snapshot.Diffs) != len(tc.Files) {
		t.Fatalf("PR 148 snapshot files = %v diffs=%d, want %v", snapshot.Files, len(snapshot.Diffs), tc.Files)
	}

	const repetitions = 2
	failedRuns := 0
	runs := make([]neoReviewBenchmarkRunResult, 0)
	for _, candidate := range neoRunCheckBenchmarkCandidates(t) {
		for rep := 1; rep <= repetitions; rep++ {
			result := neoReviewBenchmarkRunActor(t, candidate, tc, neoReviewBenchmarkIntegrated, rep)
			runs = append(runs, result)
			raw, _ := json.Marshal(result)
			t.Log(string(raw))
			if !result.Passed {
				failedRuns++
			}
		}
	}
	summary, _ := json.Marshal(neoReviewBenchmarkSummary(runs))
	t.Log(string(summary))
	if failedRuns != 0 {
		t.Fatalf("%d exact PR 148 benchmark runs failed", failedRuns)
	}
}

func neoReviewPR148BenchmarkDiff(t *testing.T) []byte {
	t.Helper()
	const limit = 4 << 20
	if path := strings.TrimSpace(os.Getenv("AMP_REVIEW_PR148_DIFF_FILE")); path != "" {
		diff, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read PR 148 benchmark diff: %v", err)
		}
		if len(diff) > limit {
			t.Fatalf("PR 148 benchmark diff exceeded %d-byte limit", limit)
		}
		return diff
	}
	return neoReviewBenchmarkFetch(t, "https://github.com/telemetry-dev/telemetry.dev/compare/e3b02d48bda1ef6f04e15bcd907648470b1778b4...63698523ec1005f83df931eb59b238b60f8cf2bc.diff", limit)
}

func neoReviewBenchmarkFilterDiff(diff string, files []string) string {
	wanted := make(map[string]bool, len(files))
	for _, file := range files {
		wanted[file] = true
	}
	sections := strings.Split(strings.TrimSpace(diff), "\ndiff --git ")
	filtered := make([]string, 0, len(files))
	for index, section := range sections {
		if index != 0 {
			section = "diff --git " + section
		}
		header, _, _ := strings.Cut(section, "\n")
		for file := range wanted {
			if header == "diff --git a/"+file+" b/"+file {
				filtered = append(filtered, strings.TrimSpace(section))
				delete(wanted, file)
				break
			}
		}
	}
	if len(filtered) == 0 {
		return ""
	}
	return strings.Join(filtered, "\n") + "\n"
}

func neoReviewPR148BenchmarkDefinition() neoReviewBenchmarkCase {
	const dependencyCheck = `---
name: published-dependency-capability-floor
severity-default: high
---
Apply when changed published package code requires a dependency capability newer than the declared minimum. Verify the exact root client access path at the lowest selectable version. An adjacent class or module does not prove that the root client owns that path. Report the runtime incompatibility and the corrected floor or fallback.`
	const budgetCheck = `---
name: bounded-artifact-state-transitions
severity-default: medium
---
Apply when partial and terminal representations are bounded. Determine whether terminal state replaces the partial capture. Report a terminal replacement that reuses an exhausted partial counter instead of receiving an independent budget. Do not combine additive state that correctly shares one counter.`
	const variantCheck = `---
name: generated-artifact-consumer-contract
severity-default: medium
---
Trace every payload-bearing external stream variant accepted by a changed partial-state adapter. Report omitted output-item or function-call argument events when cancellation before a terminal response loses their payload. Do not report variants preserved by another partial-state path.`
	const classifierCheck = `---
name: classifier-detector-matrix
severity-default: medium
---
Apply when a wrapper retains a derived provider classification while its source client endpoint remains mutable. Report a classification captured at wrap time when requests later use the current endpoint. Require per-call resolution or an intentionally immutable source.`

	files := []string{
		"packages/openrouter/package.json",
		"packages/openrouter/src/index.ts",
		"sdks/python-openrouter/pyproject.toml",
		"sdks/python-openrouter/src/telemetry_dev_openrouter/__init__.py",
		"sdks/python-openai/src/telemetry_dev_openai/__init__.py",
	}
	return neoReviewBenchmarkCase{
		Name:   "telemetry.dev PR 148 exact review",
		Family: "runtime-durability-pr-148",
		Diff: `diff --git a/packages/openrouter/package.json b/packages/openrouter/package.json
new file mode 100644
--- /dev/null
+++ b/packages/openrouter/package.json
@@ -0,0 +1,4 @@
+{
+  "name": "@telemetry-dev/openrouter",
+  "devDependencies": { "@openrouter/sdk": "1.2.11" },
+  "peerDependencies": { "@openrouter/sdk": ">=1 <2" }
diff --git a/packages/openrouter/src/index.ts b/packages/openrouter/src/index.ts
new file mode 100644
--- /dev/null
+++ b/packages/openrouter/src/index.ts
@@ -0,0 +625,8 @@
+  if (
+    type !== "response.output_text.delta" &&
+    type !== "response.reasoning_text.delta" &&
+    type !== "response.reasoning_summary_text.delta" &&
+    type !== "response.refusal.delta"
+  ) {
+    return false;
+  }
@@ -0,0 +718,10 @@
+          const terminal =
+            e.type === "response.completed" ||
+            e.type === "response.failed" ||
+            e.type === "response.incomplete";
+          const output =
+            terminal && response.output !== undefined && budget.accept(response.output)
+              ? response.output
+              : retainedOutput;
+          partial = { ...responsesResponse(response, false), output };
+        }
@@ -0,0 +890,10 @@
+    chatRequest,
+    chatResponse,
+  );
+  patchInstanceMethod(
+    client.responses as unknown as UnknownRecord,
+    "send",
+    "responses",
+    responsesRequest,
+    responsesResponse,
+  );
diff --git a/sdks/python-openrouter/pyproject.toml b/sdks/python-openrouter/pyproject.toml
new file mode 100644
--- /dev/null
+++ b/sdks/python-openrouter/pyproject.toml
@@ -0,0 +1,5 @@
+[project]
+name = "telemetry-dev-openrouter"
+dependencies = [
+  "openrouter>=1,<2",
+]
diff --git a/sdks/python-openrouter/src/telemetry_dev_openrouter/__init__.py b/sdks/python-openrouter/src/telemetry_dev_openrouter/__init__.py
new file mode 100644
--- /dev/null
+++ b/sdks/python-openrouter/src/telemetry_dev_openrouter/__init__.py
@@ -0,0 +675,9 @@
+        if event_type not in {
+            "response.output_text.delta",
+            "response.reasoning_text.delta",
+            "response.reasoning_summary_text.delta",
+            "response.refusal.delta",
+        }:
+            return
+        delta = _string(_field(event, "delta"))
+        if delta is None or not self._budget.accept(delta):
@@ -0,0 +748,10 @@
+        if response is not None:
+            fields = _responses_response(response, include_error=False, include_output=False)
+            if event_type in {"response.completed", "response.failed", "response.incomplete"}:
+                output = _native(_field(response, "output"))
+                if output is not None and self._budget.accept(output):
+                    self.retained_output = output
+            self.partial = fields
+            if self.retained_output is not None:
+                self.partial["output"] = self.retained_output
+        self._record_delta(event)
@@ -0,0 +1054,11 @@
+        _chat_request,
+        _chat_response,
+    )
+    _patch_instance(
+        cast(Any, client).responses,
+        "send",
+        _wrap_sync,
+        "responses",
+        _responses_request,
+        _responses_response,
+    )
diff --git a/sdks/python-openai/src/telemetry_dev_openai/__init__.py b/sdks/python-openai/src/telemetry_dev_openai/__init__.py
--- a/sdks/python-openai/src/telemetry_dev_openai/__init__.py
+++ b/sdks/python-openai/src/telemetry_dev_openai/__init__.py
@@ -225,4 +225,9 @@ def _provider_for_client(client: object | None) -> str:
+    if isinstance(client, openai.AzureOpenAI | openai.AsyncAzureOpenAI):
+        return "azure.ai.openai"
+    host = _base_url_host(getattr(client, "base_url", None))
+    if host is not None:
+        host = host.lower().removesuffix(".")
+    if host == "openrouter.ai" or (host is not None and host.endswith(".openrouter.ai")):
+        return "openrouter"
+    return "openai"
+
@@ -1058,3 +1063,14 @@ def wrap_openai(client: _T, *, inject_stream_usage: bool = False) -> _T:
+    provider_name = _provider_for_client(cast(object, client))
+    async_client = isinstance(client, openai.AsyncOpenAI)
+    wrapper_factory = _wrap_async if async_client else _wrap_sync
+    _patch_instance(
+        client.chat.completions,
+        "create",
+        wrapper_factory,
+        "chat",
+        _chat_request,
+        _chat_response,
+        provider_name,
+        inject_stream_usage,
+    )
`,
		Files:    files,
		Evidence: "At @openrouter/sdk 1.0.0 and openrouter 1.0.0, the root client exposes beta.responses but not responses; later development versions expose root responses. A terminal Responses output replaces partial deltas and needs an independent capture budget. The protocol also emits output_item and function_call_arguments events before terminal completion. OpenAI client base_url remains mutable after wrapping.",
		Checks: []neoReviewBenchmarkCheck{
			{Name: "published-dependency-capability-floor", Content: dependencyCheck, Severity: "high"},
			{Name: "bounded-artifact-state-transitions", Content: budgetCheck, Severity: "medium"},
			{Name: "generated-artifact-consumer-contract", Content: variantCheck, Severity: "medium"},
			{Name: "classifier-detector-matrix", Content: classifierCheck, Severity: "medium"},
		},
		Expected: []neoReviewBenchmarkExpectedFinding{
			{
				ID:        "typescript-openrouter-floor",
				CheckName: "published-dependency-capability-floor",
				Locations: []neoReviewBenchmarkLocation{{File: "packages/openrouter/src/index.ts", StartLine: 894, EndLine: 894}},
				Severity:  "high",
				RequiredKeywordGroups: [][]string{
					{"client.responses", ".responses", "responses resource"},
					{"client.beta.responses", "beta.responses", "beta"},
					{"1.0.0", "1.0", "minimum", "floor"},
					{"absent", "missing", "undefined", "runtime"},
				},
			},
			{
				ID:        "python-openrouter-floor",
				CheckName: "published-dependency-capability-floor",
				Locations: []neoReviewBenchmarkLocation{{File: "sdks/python-openrouter/src/telemetry_dev_openrouter/__init__.py", StartLine: 1058, EndLine: 1058}},
				Severity:  "high",
				RequiredKeywordGroups: [][]string{
					{"client.responses", ".responses", "responses resource"},
					{"client.beta.responses", "beta.responses", "beta"},
					{"1.0.0", "1.0", "minimum", "floor"},
					{"absent", "missing", "attributeerror", "runtime"},
				},
			},
			{
				ID:        "typescript-terminal-budget",
				CheckName: "bounded-artifact-state-transitions",
				Locations: []neoReviewBenchmarkLocation{{File: "packages/openrouter/src/index.ts", StartLine: 722, EndLine: 722}},
				Severity:  "medium",
				RequiredKeywordGroups: [][]string{
					{"budget", "counter", "capture"},
					{"terminal", "completed", "response.output"},
					{"delta", "partial"},
					{"fresh", "independent", "replacement", "reset"},
				},
			},
			{
				ID:        "python-terminal-budget",
				CheckName: "bounded-artifact-state-transitions",
				Locations: []neoReviewBenchmarkLocation{{File: "sdks/python-openrouter/src/telemetry_dev_openrouter/__init__.py", StartLine: 752, EndLine: 752}},
				Severity:  "medium",
				RequiredKeywordGroups: [][]string{
					{"budget", "_budget", "counter", "capture"},
					{"terminal", "completed", "response output"},
					{"delta", "partial"},
					{"fresh", "independent", "replacement", "reset"},
				},
			},
			{
				ID:        "typescript-partial-variants",
				CheckName: "generated-artifact-consumer-contract",
				Locations: []neoReviewBenchmarkLocation{{File: "packages/openrouter/src/index.ts", StartLine: 629, EndLine: 629}},
				Severity:  "medium",
				RequiredKeywordGroups: [][]string{
					{"output_item", "output item", "output-item"},
					{"function_call_arguments", "function call", "function-call"},
					{"partial", "cancel", "cancellation"},
					{"omit", "drop", "lose", "lost", "missing", "not record"},
				},
			},
			{
				ID:        "python-partial-variants",
				CheckName: "generated-artifact-consumer-contract",
				Locations: []neoReviewBenchmarkLocation{{File: "sdks/python-openrouter/src/telemetry_dev_openrouter/__init__.py", StartLine: 680, EndLine: 680}},
				Severity:  "medium",
				RequiredKeywordGroups: [][]string{
					{"output_item", "output item", "output-item"},
					{"function_call_arguments", "function call", "function-call"},
					{"partial", "cancel", "cancellation"},
					{"omit", "drop", "lose", "lost", "missing", "not record"},
				},
			},
			{
				ID:        "python-openai-stale-provider",
				CheckName: "classifier-detector-matrix",
				Locations: []neoReviewBenchmarkLocation{{File: "sdks/python-openai/src/telemetry_dev_openai/__init__.py", StartLine: 228, EndLine: 228}},
				Severity:  "medium",
				RequiredKeywordGroups: [][]string{
					{"provider"},
					{"base_url", "base url", "endpoint"},
					{"mutable", "change", "updated"},
					{"capture", "wrap", "stale", "per call", "each call"},
				},
			},
		},
		ForbiddenClaims: []string{"compile-time failure", "will not compile"},
	}
}

func neoReviewPR148GoldenFindings() []neoReviewBenchmarkObservedFinding {
	return []neoReviewBenchmarkObservedFinding{
		{
			Owner:     "check:published-dependency-capability-floor",
			CheckName: "published-dependency-capability-floor",
			File:      "packages/openrouter/src/index.ts",
			StartLine: 894,
			EndLine:   894,
			Severity:  "high",
			Text:      "client.responses is absent at the accepted @openrouter/sdk 1.0.0 floor.",
			Why:       "The floor exposes client.beta.responses, so wrapping the root responses resource fails at runtime.",
			Fix:       "Raise the minimum dependency or fall back to client.beta.responses at the floor.",
		},
		{
			Owner:     "check:published-dependency-capability-floor",
			CheckName: "published-dependency-capability-floor",
			File:      "sdks/python-openrouter/src/telemetry_dev_openrouter/__init__.py",
			StartLine: 1058,
			EndLine:   1058,
			Severity:  "high",
			Text:      "client.responses is absent at the accepted openrouter 1.0.0 floor.",
			Why:       "The floor exposes client.beta.responses, so this root access raises at runtime.",
			Fix:       "Raise the minimum dependency or use client.beta.responses for the supported floor.",
		},
		{
			Owner:     "check:bounded-artifact-state-transitions",
			CheckName: "bounded-artifact-state-transitions",
			File:      "packages/openrouter/src/index.ts",
			StartLine: 722,
			EndLine:   722,
			Severity:  "medium",
			Text:      "The terminal response.output replacement reuses the delta capture budget.",
			Why:       "A partial stream can exhaust the counter and then discard a valid completed output.",
			Fix:       "Give the terminal replacement a fresh independent budget instead of the partial delta counter.",
		},
		{
			Owner:     "check:bounded-artifact-state-transitions",
			CheckName: "bounded-artifact-state-transitions",
			File:      "sdks/python-openrouter/src/telemetry_dev_openrouter/__init__.py",
			StartLine: 752,
			EndLine:   752,
			Severity:  "medium",
			Text:      "The terminal response output replacement reuses _budget after partial deltas.",
			Why:       "An exhausted partial counter can reject the completed output.",
			Fix:       "Use a fresh independent replacement budget for terminal output.",
		},
		{
			Owner:     "check:generated-artifact-consumer-contract",
			CheckName: "generated-artifact-consumer-contract",
			File:      "packages/openrouter/src/index.ts",
			StartLine: 629,
			EndLine:   629,
			Severity:  "medium",
			Text:      "The partial stream omits response.output_item and response.function_call_arguments events.",
			Why:       "Cancellation before terminal completion can drop both output-item and function-call payloads.",
			Fix:       "Record both event families in partial state.",
		},
		{
			Owner:     "check:generated-artifact-consumer-contract",
			CheckName: "generated-artifact-consumer-contract",
			File:      "sdks/python-openrouter/src/telemetry_dev_openrouter/__init__.py",
			StartLine: 680,
			EndLine:   680,
			Severity:  "medium",
			Text:      "The partial stream omits response.output_item and response.function_call_arguments events.",
			Why:       "Cancellation before terminal completion can drop both output-item and function-call payloads.",
			Fix:       "Record both event families in partial state.",
		},
		{
			Owner:     "check:classifier-detector-matrix",
			CheckName: "classifier-detector-matrix",
			File:      "sdks/python-openai/src/telemetry_dev_openai/__init__.py",
			StartLine: 228,
			EndLine:   228,
			Severity:  "medium",
			Text:      "Provider classification becomes stale because wrap_openai captures it before later calls.",
			Why:       "base_url is mutable, so each call can use an updated endpoint after wrapping.",
			Fix:       "Resolve provider from the current base_url per call instead of capturing it at wrap time.",
		},
	}
}

func neoReviewBenchmarkCases() []neoReviewBenchmarkCase {
	classificationCheck := `---
name: classifier-detector-matrix
severity-default: medium
---
Apply when changed code retains a derived classification while making its source mutable or replaceable, including wrappers that capture a provider from a mutable client endpoint. Require the classification to be recomputed, invalidated, or intentionally tied to an immutable snapshot. Report only when a concrete consumer can observe a stale classification after the source changes. Do not report immutable replacement objects or per-call resolvers that derive fresh state.`
	variantCheck := `---
name: generated-artifact-consumer-contract
severity-default: medium
---
Apply when a changed external or generated protocol adds a variant or a changed stream adapter captures partial state. Trace every adapter that accepts the broadened type and report an omitted variant when it can be dropped, misclassified, or given the wrong fallback. For partial streams, include payload-bearing output-item and function-call events that must survive cancellation before a terminal event. Do not report intentionally narrower adapters whose input type excludes the new variant and whose caller handles it separately.`
	budgetCheck := `---
name: bounded-artifact-state-transitions
severity-default: high
---
Apply when changed multi-phase or iterative code enforces a configured replacement or artifact budget. Determine whether later phases add to prior capture or authoritatively replace it. Report resets that let additive work exceed a declared total cap, and report exhausted partial counters reused to reject an independently bounded terminal replacement. Do not combine independent outputs into a synthetic global budget, and do not report additive phases that correctly share one remaining counter.`
	capabilityCheck := `---
name: published-dependency-capability-floor
severity-default: high
---
Apply when changed published package code directly, reflectively, or through casts requires a dependency capability newer than the package's declared minimum supported version. A lockfile or development install does not raise the published compatibility floor. Verify the exact changed import, export, root resource path, API shape, and behavior at the lowest selectable version; existence of an adjacent class or module is not evidence for the accessed path. Report the concrete runtime or load-time incompatibility and the minimum floor or fallback needed. Do not report private packages with no published consumer contract.`
	return []neoReviewBenchmarkCase{
		{
			Name:   "mutable source leaves retained classification stale",
			Family: "stale-retained-classification",
			Diff: `diff --git a/internal/client/client.go b/internal/client/client.go
index 1111111..2222222 100644
--- a/internal/client/client.go
+++ b/internal/client/client.go
@@ -1,12 +1,18 @@
 package client
 
 import "strings"
 
 type Client struct {
     endpoint string
     sandbox  bool
 }
 
 func New(endpoint string) *Client {
     return &Client{endpoint: endpoint, sandbox: strings.Contains(endpoint, ".sandbox.")}
 }
+
+func (c *Client) SetEndpoint(endpoint string) {
+    c.endpoint = endpoint
+}
 
 func (c *Client) UseTestCredentials() bool { return c.sandbox }`,
			Files: []string{"internal/client/client.go"},
			Check: neoReviewBenchmarkCheck{Name: "classifier-detector-matrix", Content: classificationCheck, Severity: "medium"},
			Expected: []neoReviewBenchmarkExpectedFinding{{
				ID:        "stale-sandbox-classification",
				Severity:  "medium",
				Locations: []neoReviewBenchmarkLocation{{File: "internal/client/client.go", StartLine: 11, EndLine: 15}},
				RequiredKeywordGroups: [][]string{
					{"sandbox", "classification", "derived"},
					{"stale", "recompute", "update", "refresh", "invalidate"},
					{"endpoint", "source"},
				},
			}},
		},
		{
			Name:    "immutable replacement refreshes classification",
			Family:  "stale-retained-classification",
			Control: true,
			Diff: `diff --git a/internal/client/request.go b/internal/client/request.go
index 1111111..2222222 100644
--- a/internal/client/request.go
+++ b/internal/client/request.go
@@ -1,10 +1,15 @@
 package client
 
 import "strings"
 
 type Request struct {
     endpoint string
     sandbox  bool
 }
 
 func NewRequest(endpoint string) Request {
     return Request{endpoint: endpoint, sandbox: strings.Contains(endpoint, ".sandbox.")}
 }
+
+func (r Request) WithEndpoint(endpoint string) Request {
+    return NewRequest(endpoint)
+}`,
			Files: []string{"internal/client/request.go"},
			Check: neoReviewBenchmarkCheck{Name: "classifier-detector-matrix", Content: classificationCheck, Severity: "medium"},
		},
		{
			Name:   "external variant omitted by broad adapter",
			Family: "external-variant-completeness",
			Diff: `diff --git a/sdk/protocol.ts b/sdk/protocol.ts
index 1111111..2222222 100644
--- a/sdk/protocol.ts
+++ b/sdk/protocol.ts
@@ -1,14 +1,19 @@
 export interface TextPart { kind: "text"; text: string }
 export interface ActionPart { kind: "action"; name: string }
+export interface TracePart { kind: "trace"; traceId: string }
 
-export type WirePart = TextPart | ActionPart
+export type WirePart = TextPart | ActionPart | TracePart
 
 export function toEvent(part: WirePart): Event | undefined {
   switch (part.kind) {
     case "text":
       return { type: "message", value: part.text }
     case "action":
       return { type: "command", value: part.name }
   }
 }
 
 export interface Event { type: string; value: string }`,
			Files: []string{"sdk/protocol.ts"},
			Check: neoReviewBenchmarkCheck{Name: "generated-artifact-consumer-contract", Content: variantCheck, Severity: "medium"},
			Expected: []neoReviewBenchmarkExpectedFinding{{
				ID:        "trace-variant-dropped",
				Severity:  "medium",
				Locations: []neoReviewBenchmarkLocation{{File: "sdk/protocol.ts", StartLine: 1, EndLine: 6}},
				RequiredKeywordGroups: [][]string{
					{"trace", "TracePart"},
					{"adapter", "toEvent", "switch"},
					{"omit", "unhandled", "drop", "undefined"},
				},
			}},
			ForbiddenClaims: []string{"will crash"},
		},
		{
			Name:    "intentionally narrow adapter routes new variant separately",
			Family:  "external-variant-completeness",
			Control: true,
			Diff: `diff --git a/sdk/protocol.ts b/sdk/protocol.ts
index 1111111..2222222 100644
--- a/sdk/protocol.ts
+++ b/sdk/protocol.ts
@@ -1,14 +1,23 @@
 export interface TextPart { kind: "text"; text: string }
 export interface ActionPart { kind: "action"; name: string }
+export interface TracePart { kind: "trace"; traceId: string }
 
-export type WirePart = TextPart | ActionPart
+export type WirePart = TextPart | ActionPart | TracePart
+type RenderablePart = TextPart | ActionPart
 
-export function toEvent(part: WirePart): Event {
+export function toEvent(part: RenderablePart): Event {
   if (part.kind === "text") return { type: "message", value: part.text }
   return { type: "command", value: part.name }
 }
+
+export function consume(part: WirePart): Event {
+  if (part.kind === "trace") return { type: "trace", value: part.traceId }
+  return toEvent(part)
+}
 
 export interface Event { type: string; value: string }`,
			Files: []string{"sdk/protocol.ts"},
			Check: neoReviewBenchmarkCheck{Name: "generated-artifact-consumer-contract", Content: variantCheck, Severity: "medium"},
		},
		{
			Name:   "replacement budget resets inside additive phases",
			Family: "replacement-budget-accounting",
			Diff: `diff --git a/internal/redact/report.go b/internal/redact/report.go
index 1111111..2222222 100644
--- a/internal/redact/report.go
+++ b/internal/redact/report.go
@@ -8,10 +8,10 @@ func Redact(report *Report, rules []Rule, maxReplacements int) {
-    remaining := maxReplacements
     for _, rule := range rules {
+        remaining := maxReplacements
         for remaining > 0 {
             if !rule.ReplaceNext(report) {
                 break
             }
             remaining--
         }
     }
 }`,
			Files: []string{"internal/redact/report.go"},
			Check: neoReviewBenchmarkCheck{Name: "bounded-artifact-state-transitions", Content: budgetCheck, Severity: "high"},
			Expected: []neoReviewBenchmarkExpectedFinding{{
				ID:        "replacement-budget-reset",
				Severity:  "high",
				Locations: []neoReviewBenchmarkLocation{{File: "internal/redact/report.go", StartLine: 8, EndLine: 16}},
				RequiredKeywordGroups: [][]string{
					{"budget", "maxReplacements", "remaining", "cap", "limit"},
					{"reset", "each rule", "per rule", "inside"},
					{"exceed", "aggregate", "total", "multiple"},
				},
			}},
		},
		{
			Name:    "independent reports each receive their own replacement budget",
			Family:  "replacement-budget-accounting",
			Control: true,
			Diff: `diff --git a/internal/redact/reports.go b/internal/redact/reports.go
index 1111111..2222222 100644
--- a/internal/redact/reports.go
+++ b/internal/redact/reports.go
@@ -8,8 +8,16 @@ func RedactAll(inputs []Report, rule Rule, maxReplacements int) []Report {
+    outputs := make([]Report, 0, len(inputs))
+    for _, input := range inputs {
+        output := input.Clone()
+        remaining := maxReplacements
+        for remaining > 0 && rule.ReplaceNext(&output) {
+            remaining--
+        }
+        outputs = append(outputs, output)
+    }
+    return outputs
 }`,
			Files: []string{"internal/redact/reports.go"},
			Check: neoReviewBenchmarkCheck{Name: "bounded-artifact-state-transitions", Content: budgetCheck, Severity: "high"},
		},
		{
			Name:    "additive phases share one remaining replacement budget",
			Family:  "replacement-budget-accounting",
			Control: true,
			Diff: `diff --git a/internal/redact/report.go b/internal/redact/report.go
index 1111111..2222222 100644
--- a/internal/redact/report.go
+++ b/internal/redact/report.go
@@ -8,6 +8,13 @@ func Redact(report *Report, rules []Rule, maxReplacements int) {
+    remaining := maxReplacements
+    for _, rule := range rules {
+        for remaining > 0 && rule.ReplaceNext(report) {
+            remaining--
+        }
+    }
 }`,
			Files: []string{"internal/redact/report.go"},
			Check: neoReviewBenchmarkCheck{Name: "bounded-artifact-state-transitions", Content: budgetCheck, Severity: "high"},
		},
		{
			Name:   "OpenRouter floor uses beta Responses resource",
			Family: "published-capability-floor",
			Diff: `diff --git a/packages/router/package.json b/packages/router/package.json
index 1111111..2222222 100644
--- a/packages/router/package.json
+++ b/packages/router/package.json
@@ -1,6 +1,7 @@
 {
   "name": "@example/router",
   "version": "1.0.0",
   "peerDependencies": { "@router/sdk": ">=1 <2" },
+  "sideEffects": false
 }
diff --git a/packages/router/src/wrap.ts b/packages/router/src/wrap.ts
index 3333333..4444444 100644
--- a/packages/router/src/wrap.ts
+++ b/packages/router/src/wrap.ts
@@ -1,3 +1,8 @@
 import type { RouterClient } from "@router/sdk"
 
 export function wrap(client: RouterClient) {
+  const original = client.responses.send.bind(client.responses)
+  client.responses.send = async (request) => trace(await original(request))
+  return client
 }
+
+function trace<T>(value: T): T { return value }`,
			Files:    []string{"packages/router/package.json", "packages/router/src/wrap.ts"},
			Evidence: "The lowest selectable @router/sdk 1.0.0 client creates `client.beta.responses` and has no `client.responses` property. The development-selected 1.2.11 client creates `client.responses`.",
			Check:    neoReviewBenchmarkCheck{Name: "published-dependency-capability-floor", Content: capabilityCheck, Severity: "high"},
			Expected: []neoReviewBenchmarkExpectedFinding{{
				ID:        "direct-responses-resource-floor",
				Severity:  "high",
				Locations: []neoReviewBenchmarkLocation{{File: "packages/router/src/wrap.ts", StartLine: 3, EndLine: 8}},
				RequiredKeywordGroups: [][]string{
					{"responses"},
					{"beta.responses", "beta"},
					{"1.0", "floor", "lowest"},
					{"undefined", "throw", "crash", "runtime"},
				},
			}},
		},
		{
			Name:    "OpenRouter floor exposes direct Responses resource",
			Family:  "published-capability-floor",
			Control: true,
			Diff: `diff --git a/packages/router/package.json b/packages/router/package.json
index 1111111..2222222 100644
--- a/packages/router/package.json
+++ b/packages/router/package.json
@@ -1,6 +1,7 @@
 {
   "name": "@example/router",
   "version": "1.0.0",
   "peerDependencies": { "@router/sdk": ">=1 <2" },
+  "sideEffects": false
 }
diff --git a/packages/router/src/wrap.ts b/packages/router/src/wrap.ts
index 3333333..4444444 100644
--- a/packages/router/src/wrap.ts
+++ b/packages/router/src/wrap.ts
@@ -1,3 +1,8 @@
 import type { RouterClient } from "@router/sdk"
 
 export function wrap(client: RouterClient) {
+  const original = client.responses.send.bind(client.responses)
+  client.responses.send = async (request) => trace(await original(request))
+  return client
 }
+
+function trace<T>(value: T): T { return value }`,
			Files:    []string{"packages/router/package.json", "packages/router/src/wrap.ts"},
			Evidence: "The lowest selectable @router/sdk 1.0.0 client and the development-selected 1.2.11 client both create `client.responses` with a compatible `send` method.",
			Check:    neoReviewBenchmarkCheck{Name: "published-dependency-capability-floor", Content: capabilityCheck, Severity: "high"},
		},
		{
			Name:   "terminal output reuses delta budget",
			Family: "replacement-budget-accounting",
			Diff: `diff --git a/packages/router/src/stream.ts b/packages/router/src/stream.ts
index 1111111..2222222 100644
--- a/packages/router/src/stream.ts
+++ b/packages/router/src/stream.ts
@@ -1,3 +1,13 @@
 export function captureEvent(event: Event, partial: Output, budgets: { delta: Budget; replacement: Budget }) {
+  if (event.type === "response.output_text.delta") {
+    partial.text += budgets.delta.capture(event.delta)
+    return partial
+  }
+  if (event.type === "response.completed") {
+    const output = budgets.delta.capture(event.response.output)
+    return { status: "completed", text: output, output }
+  }
   return partial
 }
+
+type Event = { type: "response.output_text.delta"; delta: string } | { type: "response.completed"; response: { output: string } }
+interface Output { status?: string; text: string; output?: string }
+interface Budget { capture(value: string): string }`,
			Files:    []string{"packages/router/src/stream.ts"},
			Evidence: "A completed response output authoritatively replaces delta-derived partial output. The capture limit applies independently to each alternative representation; a terminal output that fits the original limit must not be rejected because earlier deltas exhausted their counter.",
			Check:    neoReviewBenchmarkCheck{Name: "bounded-artifact-state-transitions", Content: budgetCheck, Severity: "high"},
			Expected: []neoReviewBenchmarkExpectedFinding{{
				ID:        "terminal-reuses-delta-budget",
				Severity:  "high",
				Locations: []neoReviewBenchmarkLocation{{File: "packages/router/src/stream.ts", StartLine: 1, EndLine: 13}},
				RequiredKeywordGroups: [][]string{
					{"budget", "counter", "limit"},
					{"terminal", "completed", "final"},
					{"delta", "partial"},
					{"fresh", "reset", "independent", "replacement"},
				},
			}},
		},
		{
			Name:    "terminal output receives replacement budget",
			Family:  "replacement-budget-accounting",
			Control: true,
			Diff: `diff --git a/packages/router/src/stream.ts b/packages/router/src/stream.ts
index 1111111..2222222 100644
--- a/packages/router/src/stream.ts
+++ b/packages/router/src/stream.ts
@@ -1,3 +1,13 @@
 export function captureEvent(event: Event, partial: Output, budgets: { delta: Budget; replacement: Budget }) {
+  if (event.type === "response.output_text.delta") {
+    partial.text += budgets.delta.capture(event.delta)
+    return partial
+  }
+  if (event.type === "response.completed") {
+    const output = budgets.replacement.capture(event.response.output)
+    return { status: "completed", text: output, output }
+  }
   return partial
 }
+
+type Event = { type: "response.output_text.delta"; delta: string } | { type: "response.completed"; response: { output: string } }
+interface Output { status?: string; text: string; output?: string }
+interface Budget { capture(value: string): string }`,
			Files:    []string{"packages/router/src/stream.ts"},
			Evidence: "A completed response output authoritatively replaces delta-derived partial output, and the changed terminal branch gives that replacement a fresh budget with the same configured limit.",
			Check:    neoReviewBenchmarkCheck{Name: "bounded-artifact-state-transitions", Content: budgetCheck, Severity: "high"},
		},
		{
			Name:   "partial Responses stream omits non-text events",
			Family: "external-variant-completeness",
			Diff: `diff --git a/packages/router/src/partial.ts b/packages/router/src/partial.ts
index 1111111..2222222 100644
--- a/packages/router/src/partial.ts
+++ b/packages/router/src/partial.ts
@@ -1,3 +1,12 @@
 export function remember(event: ResponseEvent, partial: PartialOutput) {
+  if (event.type === "response.output_text.delta") partial.text += event.delta
+  if (event.type === "response.reasoning_text.delta") partial.reasoning += event.delta
+  if (event.type === "response.completed") partial.output = event.response.output
+  return partial
 }
+
+type ResponseEvent =
+  | { type: "response.output_text.delta" | "response.reasoning_text.delta" | "response.function_call_arguments.delta"; delta: string }
+  | { type: "response.function_call_arguments.done"; arguments: string }
+  | { type: "response.output_item.added" | "response.output_item.done"; output_index: number; item: unknown }
+  | { type: "response.completed"; response: { output: unknown } }
+interface PartialOutput { text: string; reasoning: string; output?: unknown }`,
			Files:    []string{"packages/router/src/partial.ts"},
			Evidence: "The supported protocol emits payload-bearing `response.output_item.added`, `response.output_item.done`, `response.function_call_arguments.delta`, and `response.function_call_arguments.done` events. If iteration is cancelled before a terminal event, only the adapter's remembered partial state is recorded.",
			Check:    neoReviewBenchmarkCheck{Name: "generated-artifact-consumer-contract", Content: variantCheck, Severity: "medium"},
			Expected: []neoReviewBenchmarkExpectedFinding{{
				ID:        "partial-non-text-events-dropped",
				Severity:  "medium",
				Locations: []neoReviewBenchmarkLocation{{File: "packages/router/src/partial.ts", StartLine: 1, EndLine: 12}},
				RequiredKeywordGroups: [][]string{
					{"output_item", "output item", "output-item"},
					{"function_call", "function call", "function-call"},
					{"cancel", "partial"},
					{"drop", "omit", "lose", "lost", "missing", "not persist"},
				},
			}},
		},
		{
			Name:    "partial Responses stream preserves non-text events",
			Family:  "external-variant-completeness",
			Control: true,
			Diff: `diff --git a/packages/router/src/partial.ts b/packages/router/src/partial.ts
index 1111111..2222222 100644
--- a/packages/router/src/partial.ts
+++ b/packages/router/src/partial.ts
@@ -1,3 +1,10 @@
 export function remember(event: ResponseEvent, partial: PartialOutput) {
+  if (event.type === "response.output_text.delta") partial.text += event.delta
+  if (event.type === "response.reasoning_text.delta") partial.reasoning += event.delta
+  if (event.type === "response.output_item.added" || event.type === "response.output_item.done") partial.items[event.output_index] = event.item
+  if (event.type === "response.function_call_arguments.delta") partial.arguments += event.delta
+  if (event.type === "response.function_call_arguments.done") partial.arguments = event.arguments
+  if (event.type === "response.completed") partial.output = event.response.output
+  return partial
 }
+
+type ResponseEvent =
+  | { type: "response.output_text.delta" | "response.reasoning_text.delta" | "response.function_call_arguments.delta"; delta: string }
+  | { type: "response.function_call_arguments.done"; arguments: string }
+  | { type: "response.output_item.added" | "response.output_item.done"; output_index: number; item: unknown }
+  | { type: "response.completed"; response: { output: unknown } }
+interface PartialOutput { text: string; reasoning: string; items: unknown[]; arguments: string; output?: unknown }`,
			Files:    []string{"packages/router/src/partial.ts"},
			Evidence: "The supported protocol emits payload-bearing output-item and function-call argument events. The changed adapter records both event families before cancellation and replaces them with terminal output when available.",
			Check:    neoReviewBenchmarkCheck{Name: "generated-artifact-consumer-contract", Content: variantCheck, Severity: "medium"},
		},
		{
			Name:   "wrapper captures mutable provider at wrap time",
			Family: "stale-retained-classification",
			Diff: `diff --git a/sdk/wrap.py b/sdk/wrap.py
index 1111111..2222222 100644
--- a/sdk/wrap.py
+++ b/sdk/wrap.py
@@ -1,2 +1,9 @@
 def wrap(client):
+    provider = provider_for(client.base_url)
+    original = client.responses.create
+
+    def create(**kwargs):
+        return trace(original(**kwargs), provider=provider)
+
+    client.responses.create = create
     return client`,
			Files:    []string{"sdk/wrap.py"},
			Evidence: "The client contract permits `base_url` to change after construction and wrapping. Patched instance methods send each request to the client's current base URL, so provider attribution must reflect the value when each call starts.",
			Check:    neoReviewBenchmarkCheck{Name: "classifier-detector-matrix", Content: classificationCheck, Severity: "medium"},
			Expected: []neoReviewBenchmarkExpectedFinding{{
				ID:        "wrap-time-provider-capture",
				Severity:  "medium",
				Locations: []neoReviewBenchmarkLocation{{File: "sdk/wrap.py", StartLine: 1, EndLine: 10}},
				RequiredKeywordGroups: [][]string{
					{"provider"},
					{"base_url", "base url", "endpoint"},
					{"stale", "capture", "resolve", "recompute"},
					{"call", "invocation", "request"},
				},
			}},
		},
		{
			Name:    "wrapper resolves mutable provider per call",
			Family:  "stale-retained-classification",
			Control: true,
			Diff: `diff --git a/sdk/wrap.py b/sdk/wrap.py
index 1111111..2222222 100644
--- a/sdk/wrap.py
+++ b/sdk/wrap.py
@@ -1,2 +1,8 @@
 def wrap(client):
+    original = client.responses.create
+
+    def create(**kwargs):
+        return trace(original(**kwargs), provider=provider_for(client.base_url))
+
+    client.responses.create = create
     return client`,
			Files:    []string{"sdk/wrap.py"},
			Evidence: "The client contract permits `base_url` to change after construction and wrapping. The changed wrapper derives provider attribution from the current client value when each patched call starts.",
			Check:    neoReviewBenchmarkCheck{Name: "classifier-detector-matrix", Content: classificationCheck, Severity: "medium"},
		},
		{
			Name:   "published capability floor direct call",
			Family: "published-capability-floor",
			Diff: `diff --git a/packages/bridge/package.json b/packages/bridge/package.json
index 1111111..2222222 100644
--- a/packages/bridge/package.json
+++ b/packages/bridge/package.json
@@ -1,7 +1,8 @@
 {
   "name": "@example/bridge",
   "version": "2.0.0",
   "peerDependencies": { "stream-core": ">=1.4.0" },
+  "sideEffects": false,
   "dependencies": {}
 }
diff --git a/packages/bridge/src/resume.ts b/packages/bridge/src/resume.ts
index 3333333..4444444 100644
--- a/packages/bridge/src/resume.ts
+++ b/packages/bridge/src/resume.ts
@@ -1,5 +1,5 @@
 import type { Session } from "stream-core"
 
 export function resume(session: Session) {
-  return session.start()
+  return session.resumeStream()
 }
diff --git a/vendor/stream-core-capabilities.json b/vendor/stream-core-capabilities.json
new file mode 100644
index 0000000..5555555
--- /dev/null
+++ b/vendor/stream-core-capabilities.json
@@ -0,0 +1,6 @@
+{
+  "package": "stream-core",
+  "capabilities": {
+    "resumeStream": { "introduced": "1.8.0" }
+  }
+}`,
			Files: []string{"packages/bridge/package.json", "packages/bridge/src/resume.ts", "vendor/stream-core-capabilities.json"},
			Check: neoReviewBenchmarkCheck{Name: "published-dependency-capability-floor", Content: capabilityCheck, Severity: "high"},
			Expected: []neoReviewBenchmarkExpectedFinding{{
				ID:       "direct-capability-floor",
				Severity: "high",
				Locations: []neoReviewBenchmarkLocation{
					{File: "packages/bridge/src/resume.ts", StartLine: 3, EndLine: 5},
					{File: "packages/bridge/package.json", StartLine: 1, EndLine: 7},
				},
				RequiredKeywordGroups: [][]string{
					{"resumeStream"},
					{"1.4", "published minimum", "peer", "floor"},
					{"1.8"},
				},
			}},
		},
		{
			Name:   "published capability floor reflective call",
			Family: "published-capability-floor",
			Diff: `diff --git a/packages/bridge/package.json b/packages/bridge/package.json
index 1111111..2222222 100644
--- a/packages/bridge/package.json
+++ b/packages/bridge/package.json
@@ -1,6 +1,7 @@
 {
   "name": "@example/bridge",
   "version": "2.0.0",
   "peerDependencies": { "stream-core": ">=1.4.0" },
+  "sideEffects": false
 }
diff --git a/packages/bridge/src/resume.ts b/packages/bridge/src/resume.ts
index 3333333..4444444 100644
--- a/packages/bridge/src/resume.ts
+++ b/packages/bridge/src/resume.ts
@@ -1,5 +1,6 @@
 import type { Session, Stream } from "stream-core"
 
-export function resume(session: Session): Stream {
-  return session.start()
-}
+export function resume(session: Session): Stream {
+  const modern = session as unknown as { resumeStream(): Stream }
+  return modern.resumeStream()
+}
diff --git a/vendor/stream-core-capabilities.json b/vendor/stream-core-capabilities.json
new file mode 100644
index 0000000..5555555
--- /dev/null
+++ b/vendor/stream-core-capabilities.json
@@ -0,0 +1,6 @@
+{
+  "package": "stream-core",
+  "capabilities": {
+    "resumeStream": { "introduced": "1.8.0" }
+  }
+}`,
			Files: []string{"packages/bridge/package.json", "packages/bridge/src/resume.ts", "vendor/stream-core-capabilities.json"},
			Check: neoReviewBenchmarkCheck{Name: "published-dependency-capability-floor", Content: capabilityCheck, Severity: "high"},
			Expected: []neoReviewBenchmarkExpectedFinding{{
				ID:       "reflective-capability-floor",
				Severity: "high",
				Locations: []neoReviewBenchmarkLocation{
					{File: "packages/bridge/src/resume.ts", StartLine: 3, EndLine: 7},
					{File: "packages/bridge/package.json", StartLine: 1, EndLine: 6},
				},
				RequiredKeywordGroups: [][]string{
					{"resumeStream"},
					{"1.4", "published minimum", "peer", "floor"},
					{"runtime", "cast", "reflect"},
					{"1.8"},
				},
			}},
			ForbiddenClaims: []string{"compile-time failure", "will not compile", "type-check failure", "type error"},
		},
		{
			Name:    "private lock update has no published floor",
			Family:  "published-capability-floor",
			Control: true,
			Diff: `diff --git a/apps/worker/package.json b/apps/worker/package.json
index 1111111..2222222 100644
--- a/apps/worker/package.json
+++ b/apps/worker/package.json
@@ -1,7 +1,7 @@
 {
   "name": "worker-app",
   "private": true,
   "dependencies": {
-    "stream-core": "1.7.0"
+    "stream-core": "1.8.0"
   }
 }
diff --git a/pnpm-lock.yaml b/pnpm-lock.yaml
index 3333333..4444444 100644
--- a/pnpm-lock.yaml
+++ b/pnpm-lock.yaml
@@ -1,22 +1,22 @@
 lockfileVersion: '9.0'

 settings:
   autoInstallPeers: true
   excludeLinksFromLockfile: false

 importers:

   'apps/worker':
     dependencies:
       stream-core:
-        specifier: 1.7.0
-        version: 1.7.0
+        specifier: 1.8.0
+        version: 1.8.0

 packages:

-  stream-core@1.7.0:
-    resolution: {integrity: sha512-c3RyZWFtLWNvcmUtMS43LjA=}
+  stream-core@1.8.0:
+    resolution: {integrity: sha512-c3RyZWFtLWNvcmUtMS44LjA=}

 snapshots:

-  stream-core@1.7.0: {}
+  stream-core@1.8.0: {}`,
			Files:           []string{"apps/worker/package.json", "pnpm-lock.yaml"},
			Check:           neoReviewBenchmarkCheck{Name: "published-dependency-capability-floor", Content: capabilityCheck, Severity: "high"},
			ForbiddenClaims: []string{"published consumers", "peer dependency floor"},
		},
	}
}
