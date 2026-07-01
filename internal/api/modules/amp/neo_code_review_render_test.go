package amp

import (
	"strconv"
	"strings"
	"testing"
)

func sampleCodeReviewResult() map[string]any {
	return map[string]any{
		"main": map[string]any{
			"status": "done",
			"review": map[string]any{
				"comments": []any{
					map[string]any{
						"filename": "apps/web/src/orpc/router/traces.ts", "startLine": float64(118),
						"severity": "high", "commentType": "bug",
						"text": "orderBy sorts on spans.startedAt while where filters on trace",
						"why":  "inconsistent ordering", "fix": "align the columns",
					},
				},
			},
			"toolUses": []any{map[string]any{
				"tool_name": "Bash",
				"input":     map[string]any{"cmd": "git show origin/pr/38:traces.ts"},
				"result":    map[string]any{"output": "BASH_TRANSCRIPT_NOISE"},
			}},
		},
		"checks": map[string]any{
			"file:///x/api-and-observability-polish.md": map[string]any{
				"result": map[string]any{
					"check":  map[string]any{"content": "CHECK_INSTRUCTION_RUBRIC_NOISE"},
					"result": map[string]any{"name": "api-and-observability-polish", "status": "completed", "issuesFound": float64(1)},
					"issues": []any{
						map[string]any{"severity": "medium", "file": "traces.ts", "line": float64(78),
							"problem": "no pagination", "why": "truncation", "fix": "add a cursor"},
					},
				},
			},
			"file:///x/implementation-simplicity-and-cost.md": map[string]any{
				"result": map[string]any{
					"check":  map[string]any{"content": "MORE_RUBRIC_NOISE"},
					"result": map[string]any{"name": "implementation-simplicity-and-cost", "status": "completed", "issuesFound": float64(0)},
					"issues": []any{},
				},
			},
		},
	}
}

func TestNeoCodeReviewResultXML(t *testing.T) {
	xml, ok := neoCodeReviewResultXML(sampleCodeReviewResult())
	if !ok {
		t.Fatal("expected a code_review result to render as XML")
	}

	for _, want := range []string{
		"<codeReview>", "<comment>", "<filename>apps/web/src/orpc/router/traces.ts</filename>",
		"<severity>high</severity>", "orderBy sorts on spans.startedAt", "<fix>align the columns</fix>",
		"<checkResult>", "<checkName>api-and-observability-polish</checkName>",
		"<status>completed</status>", "<issuesFound>1</issuesFound>",
		`<issue severity="medium" file="traces.ts" line="78">`, "<problem>no pagination</problem>",
	} {
		if !strings.Contains(xml, want) {
			t.Errorf("rendered XML missing %q\n---\n%s", want, xml)
		}
	}

	if !strings.Contains(xml, "<checkName>implementation-simplicity-and-cost</checkName>") ||
		!strings.Contains(xml, "<issuesFound>0</issuesFound>") {
		t.Errorf("clean-check coverage should be retained:\n%s", xml)
	}

	if !strings.Contains(xml, "<toolsRun>") ||
		!strings.Contains(xml, `<tool name="Bash">git show origin/pr/38:traces.ts</tool>`) {
		t.Errorf("toolsRun command summary should be present:\n%s", xml)
	}

	for _, noise := range []string{"CHECK_INSTRUCTION_RUBRIC_NOISE", "MORE_RUBRIC_NOISE", "BASH_TRANSCRIPT_NOISE", "toolUses"} {
		if strings.Contains(xml, noise) {
			t.Errorf("rendered XML must drop noise %q\n---\n%s", noise, xml)
		}
	}
}

func TestNeoCodeReviewResultXMLNotAReview(t *testing.T) {
	if _, ok := neoCodeReviewResultXML(map[string]any{"output": "hello"}); ok {
		t.Error("a non-review result must not render as <codeReview> XML")
	}
	if _, ok := neoCodeReviewResultXML(map[string]any{}); ok {
		t.Error("empty result must not render")
	}
}

func TestRunToTextForToolRendersCodeReviewAsXML(t *testing.T) {
	run := map[string]any{
		"status":   "done",
		"result":   sampleCodeReviewResult(),
		"progress": map[string]any{"output": "Check api-and-observability-polish: 1 issues found\nMain review complete"},
	}
	got := runToTextForTool("code_review", run)
	if !strings.Contains(got, "<codeReview>") || !strings.Contains(got, "no pagination") {
		t.Fatalf("runToTextForTool should render code_review as native XML:\n%s", got)
	}
	if strings.Contains(got, `"toolUses"`) || strings.Contains(got, "BASH_TRANSCRIPT_NOISE") {
		t.Errorf("runToTextForTool must not raw-JSON-dump the review result:\n%s", got)
	}

	generic := runToText(run)
	if !strings.Contains(generic, `"toolUses"`) || strings.Contains(generic, "<codeReview>") {
		t.Errorf("plain runToText should keep generic structured serialization:\n%s", generic)
	}
}

func TestNeoXMLEscaping(t *testing.T) {
	result := map[string]any{
		"main": map[string]any{"review": map[string]any{"comments": []any{
			map[string]any{"filename": "a.ts", "severity": "low", "text": "use a < b && c > d for the guard"},
		}}},
	}
	xml, ok := neoCodeReviewResultXML(result)
	if !ok {
		t.Fatal("expected render")
	}
	if !strings.Contains(xml, "a &lt; b &amp;&amp; c &gt; d") {
		t.Errorf("special chars in finding text must be XML-escaped:\n%s", xml)
	}
}

func TestNeoCodeReviewToolsRunXMLCapsCommands(t *testing.T) {
	tools := make([]any, 0, neoCodeReviewToolRunLimit+2)
	for i := 0; i < neoCodeReviewToolRunLimit+2; i++ {
		tools = append(tools, map[string]any{
			"tool_name": "Bash",
			"input":     map[string]any{"cmd": "cmd-" + strconv.Itoa(i)},
			"result":    map[string]any{"output": "output-" + strconv.Itoa(i)},
		})
	}
	result := map[string]any{
		"main": map[string]any{
			"review":   map[string]any{"comments": []any{}},
			"toolUses": tools,
		},
	}
	xml, ok := neoCodeReviewResultXML(result)
	if !ok {
		t.Fatal("expected render")
	}
	if strings.Count(xml, "<tool name=") != neoCodeReviewToolRunLimit {
		t.Fatalf("tool count = %d, want %d:\n%s", strings.Count(xml, "<tool name="), neoCodeReviewToolRunLimit, xml)
	}
	if !strings.Contains(xml, "<omittedTools>2</omittedTools>") {
		t.Fatalf("missing omitted tool count:\n%s", xml)
	}
	if strings.Contains(xml, "output-") {
		t.Fatalf("toolsRun leaked command output:\n%s", xml)
	}
}

func TestNeoCodeReviewResultXMLFallsBackOnMissingIssueBodies(t *testing.T) {
	result := map[string]any{
		"checks": map[string]any{
			"file:///x/repo-convention-fit.md": map[string]any{
				"result": map[string]any{
					"result": map[string]any{"name": "repo-convention-fit", "status": "completed", "issuesFound": float64(3)},
					"issues": []any{},
				},
			},
		},
	}
	if _, ok := neoCodeReviewResultXML(result); ok {
		t.Fatal("counts > extracted bodies must fall back to JSON (ok=false)")
	}
	run := map[string]any{"status": "done", "result": result}
	got := runToTextForTool("code_review", run)
	if !strings.Contains(got, `"issuesFound":3`) || strings.Contains(got, "<checkResult>") {
		t.Fatalf("expected JSON fallback preserving the result, got:\n%s", got)
	}
}

func TestNeoCodeReviewResultXMLFallsBackOnEmptyIssueBodies(t *testing.T) {
	result := map[string]any{
		"checks": map[string]any{
			"file:///x/repo-convention-fit.md": map[string]any{
				"result": map[string]any{
					"result": map[string]any{"name": "repo-convention-fit", "status": "completed", "issuesFound": float64(2)},
					"issues": []any{
						map[string]any{"severity": "medium", "file": "a.ts"},
						map[string]any{"severity": "low", "file": "b.ts"},
					},
				},
			},
		},
	}
	if _, ok := neoCodeReviewResultXML(result); ok {
		t.Fatal("empty issue bodies must fall back to JSON (ok=false)")
	}
}

func TestNeoCodeReviewResultXMLFallsBackOnContentlessComments(t *testing.T) {
	result := map[string]any{
		"main": map[string]any{"review": map[string]any{"comments": []any{
			map[string]any{"severity": "high"},
		}}},
	}
	if _, ok := neoCodeReviewResultXML(result); ok {
		t.Fatal("contentless comments must fall back to JSON (ok=false)")
	}
}

func TestNeoCodeReviewResultXMLCleanReviewRenders(t *testing.T) {
	result := map[string]any{
		"main": map[string]any{"review": map[string]any{"comments": []any{}}},
		"checks": map[string]any{
			"file:///x/repo-convention-fit.md": map[string]any{
				"result": map[string]any{
					"result": map[string]any{"name": "repo-convention-fit", "status": "completed", "issuesFound": float64(0)},
					"issues": []any{},
				},
			},
		},
	}
	xml, ok := neoCodeReviewResultXML(result)
	if !ok {
		t.Fatal("a clean review must still render coverage, not fall back")
	}
	if !strings.Contains(xml, "<checkName>repo-convention-fit</checkName>") || !strings.Contains(xml, "<issuesFound>0</issuesFound>") {
		t.Fatalf("clean review should render check coverage:\n%s", xml)
	}
}
