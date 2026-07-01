package amp

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
)

// Amp gaac893 replaced the in-thread code_review deferred tool with a hidden
// "review" agent mode driven by the `amp review` CLI: the CLI discovers checks
// client-side, embeds them in the thread's first user message, and expects the
// agent to call run_check once per check plus submit_review exactly once. Both
// tools are server-owned (the binary only renders their results), so the local
// runtime implements them here: submit_review echoes its validated input as the
// structured result the CLI parses, and run_check runs as a local subagent.

// neoReviewPrompt is the review-mode system prompt. The body is the main
// code-review agent prompt recovered verbatim from the amp-classic binary
// (g596c49), which ran reviews client-side; the trailing section bridges its
// prose-output flow to the gaac893 run_check/submit_review tool protocol,
// since the current server-side review prompt is not extractable.
func neoReviewPrompt() string {
	return "You are an expert senior engineer with deep knowledge of software engineering best practices, security, performance, and maintainability.\n\nYour task is to perform a code review of the provided diff description. The diff description might be a git or bash command that generates the diff or a description of the diff which can then be used to generate the git or bash command to generate the full diff.\n\nAfter reading the diff, do the following:\n1. Write a high-level summary of the changes in the diff.\n2. Go file-by-file and review each changed hunk.\n3. Comment on what changed in that hunk (including the line range) and how it relates to other\n   changed hunks and code, reading any other relevant files. Also call out bugs, hackiness,\n   unnecessary code, or too much shared mutable state.\n4. Evaluate abstraction fit in both directions: flag unnecessary indirection (over-abstraction)\n   and missing abstractions (duplication or branching complexity). For each finding, cite concrete\n   locations and recommend exactly one action—simplify/inline or introduce/extract a shared\n   concept—only when it improves current code (avoid speculative refactors).\n\nStrongly prefer to restrict your use of git commands to these when getting the diff or determining which files were added/changed/removed:\n<referenceCommands>\n  <command>\n    <description>committed changes on my branch since diverging from the upstream default branch</description>\n    <bash>git diff --merge-base origin/HEAD HEAD</bash>\n  </command>\n  <command>\n    <description>all current checkout changes since diverging from upstream (commits + staged + unstaged tracked)</description>\n    <bash>git diff --merge-base origin/HEAD</bash>\n  </command>\n  <command>\n    <description>changes since diverging from upstream up to and including staged changes</description>\n    <bash>git diff --cached --merge-base origin/HEAD</bash>\n  </command>\n  <command>\n    <description>current checkout tracked changes since divergence, plus a list of newly added untracked files</description>\n    <bash>git diff --merge-base origin/HEAD</bash>\n    <bash>git ls-files --others --exclude-standard</bash>\n  </command>\n  <command>\n    <description>changes on branch foo since divergence from upstream</description>\n    <bash>git diff --merge-base origin/HEAD foo</bash>\n  </command>\n  <command>\n    <description>only filenames changed by this branch since divergence</description>\n    <bash>git diff --name-only --merge-base origin/HEAD HEAD</bash>\n  </command>\n  <command>\n    <description>scope diff to a specific path since diverging from upstream</description>\n    <bash>git diff --merge-base origin/HEAD <ref-or-empty> -- &lt;pathspec&gt;</bash>\n</command>\n</referenceCommands>\n\nAvoid commands in this format, unless explicitly asked for:\n<avoidCommands>\n  <avoidCommand>git diff <base-ref> <head-ref></avoidCommand>\n  <avoidCommand>git diff <base-ref>..<head-ref></avoidCommand>\n  <avoidCommand>git diff HEAD...origin/HEAD</avoidCommand>\n</avoidCommands>\n\n<guidelines>\n- Persistence: Low. Do not retry failed tool calls more than 2 times. If a tool call fails twice, move on.\n- Remember to look at untracked added files.\n- Prefer the most direct path to completing the review. Batch related file reads into as few turns as possible.\n- Do not edit or modify files or run any commands that edit or modify files or git state.\n- Do not re-read files you have already read.\n- Upstream default branch ref: use origin/HEAD. Do not assume main, origin/main, or origin/master.\n- If a diff is unexpectedly large, double check you are using the right refs in git invocations.\n- If the diff has more than 100 changed files or is more than 10,000 lines long, abort the review and emit a single critical issue stating the diff is too large.\n</guidelines>\n\nReview-mode environment:\n- shell_command is your only inspection tool; use it for every file read and search (cat, rg, git) as well as for generating the diff.\n- The review client generates the high-level diff summary separately and discards assistant prose, so do not spend turns writing a narrative summary; convert your per-hunk findings directly into submit_review comments.\n\nSubmitting the review:\n- If review checks are provided in the request, call run_check exactly once per check, passing that check's checkName, checkURI, optional content, frontmatter, diffDescription, files, and any user instructions. Do not evaluate check criteria yourself and do not repeat check findings elsewhere.\n- Deliver every review finding as a structured comment through the submit_review tool; call it exactly once at the end. Prose review text is ignored by the review client.\n- Every comment needs filename (the EXACT repository-relative path from the diff header), startLine and endLine (1-based, on the new side of the diff), and text describing the problem. Set severity (critical/high/medium/low) and commentType when known; add why and fix when they help.\n- If the request says checks only, or the diff is clean, call submit_review with an empty comments array."
}

func neoRunCheckToolSpec() neoToolSpec {
	return neoToolSpec{
		Name:        "run_check",
		Description: "Run a single discovered review check against the changes under review. Call this once per check provided in the review request, passing the check's name, URI, optional embedded content, frontmatter, diff description, files, and any additional instructions. If content is not embedded, load criteria from the check URI before evaluating it. Returns the structured result of evaluating that check.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"checkName":       map[string]any{"type": "string", "description": "The name of the check, exactly as provided in the review request."},
				"checkURI":        map[string]any{"type": "string", "description": "The URI of the check, exactly as provided in the review request."},
				"checkContent":    map[string]any{"type": "string", "description": "Optional full markdown content of the check when already supplied by a legacy caller."},
				"frontmatter":     map[string]any{"type": "object", "description": "The check's frontmatter object, verbatim."},
				"diffDescription": map[string]any{"type": "string", "description": "The description of the diff under review."},
				"files":           map[string]any{"type": "array", "items": map[string]any{"type": "string"}, "description": "The files under review."},
				"instructions":    map[string]any{"type": "string", "description": "Additional user directives for review focus, severity filtering, or scope narrowing that must be honored while evaluating this check."},
			},
			"required": []any{"checkName", "checkURI"},
		},
		Meta: map[string]any{"source": "server"},
	}
}

func neoSubmitReviewToolSpec() neoToolSpec {
	return neoToolSpec{
		Name:        "submit_review",
		Description: "Submit the final code review. Call exactly once, after every provided check has been run with run_check, passing all review comments. Do not include run_check findings; they are appended mechanically. Pass an empty comments array when the diff is clean or the request asked for checks only.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"comments": map[string]any{
					"type": "array",
					"items": map[string]any{
						"type": "object",
						"properties": map[string]any{
							"filename":    map[string]any{"type": "string", "description": "Repository-relative path of the file the comment applies to."},
							"startLine":   map[string]any{"type": "number", "description": "First line of the commented range (1-based, new side of the diff)."},
							"endLine":     map[string]any{"type": "number", "description": "Last line of the commented range (1-based, new side of the diff)."},
							"text":        map[string]any{"type": "string", "description": "The review comment describing the problem."},
							"commentType": map[string]any{"type": "string", "enum": []any{"bug", "suggested_edit", "compliment", "non_actionable", "unknown"}, "description": "The kind of comment."},
							"severity":    map[string]any{"type": "string", "enum": []any{"critical", "high", "medium", "low"}, "description": "How severe the problem is."},
							"source":      map[string]any{"type": "string", "description": "What surfaced the comment."},
							"why":         map[string]any{"type": "string", "description": "Why the problem matters."},
							"fix":         map[string]any{"type": "string", "description": "Suggested fix."},
						},
						"required": []any{"filename", "startLine", "endLine", "text"},
					},
					"description": "All review comments. Empty when the diff is clean.",
				},
			},
			"required": []any{"comments"},
		},
		Meta: map[string]any{"source": "server"},
	}
}

// executeLocalSubmitReview validates a submit_review call and echoes the
// comments as the structured run result; the amp review CLI parses the tool
// result (not the input) for the final review payload.
func executeLocalSubmitReview(input map[string]any) (map[string]any, error) {
	rawComments, ok := input["comments"]
	if !ok {
		return nil, fmt.Errorf("submit_review requires a comments array")
	}
	comments := arrayValue(rawComments)
	if comments == nil {
		return nil, fmt.Errorf("submit_review comments must be an array")
	}
	out := make([]any, 0, len(comments))
	for i, raw := range comments {
		comment, ok := asMap(raw)
		if !ok {
			return nil, fmt.Errorf("submit_review comment %d must be an object", i)
		}
		filename := stringValue(comment["filename"])
		text := stringValue(comment["text"])
		if filename == "" || text == "" {
			return nil, fmt.Errorf("submit_review comment %d requires filename and text", i)
		}
		startLine, ok := neoReviewNumber(comment["startLine"])
		if !ok {
			return nil, fmt.Errorf("submit_review comment %d requires numeric startLine", i)
		}
		endLine, ok := neoReviewNumber(comment["endLine"])
		if !ok {
			return nil, fmt.Errorf("submit_review comment %d requires numeric endLine", i)
		}
		normalized := map[string]any{
			"filename":  filename,
			"startLine": startLine,
			"endLine":   endLine,
			"text":      text,
		}
		if commentType := stringValue(comment["commentType"]); commentType != "" {
			if !neoReviewCommentTypeValid(commentType) {
				return nil, fmt.Errorf("submit_review comment %d has invalid commentType %q", i, commentType)
			}
			normalized["commentType"] = commentType
		}
		if severity := stringValue(comment["severity"]); severity != "" {
			if !neoReviewSeverityValid(severity) {
				return nil, fmt.Errorf("submit_review comment %d has invalid severity %q", i, severity)
			}
			normalized["severity"] = severity
		}
		for _, key := range []string{"source", "why", "fix"} {
			if value := stringValue(comment[key]); value != "" {
				normalized[key] = value
			}
		}
		out = append(out, normalized)
	}
	return map[string]any{"comments": out}, nil
}

func neoReviewNumber(value any) (int, bool) {
	if number, ok := value.(float64); ok && (math.IsNaN(number) || math.IsInf(number, 0) || math.Trunc(number) != number) {
		return 0, false
	}
	number, ok := neoClientNumber(value)
	return number, ok
}

func neoReviewSeverityValid(value string) bool {
	switch value {
	case "critical", "high", "medium", "low":
		return true
	default:
		return false
	}
}

func neoReviewCommentTypeValid(value string) bool {
	switch value {
	case "bug", "suggested_edit", "compliment", "non_actionable", "unknown":
		return true
	default:
		return false
	}
}

func neoNormalizeRunCheckResult(input map[string]any, parsed map[string]any) (map[string]any, error) {
	checkName := firstNonEmptyString(parsed["checkName"], input["checkName"])
	if checkName == "" {
		return nil, fmt.Errorf("missing checkName")
	}
	status := stringValue(parsed["status"])
	if status == "" {
		status = "completed"
	}
	if status != "completed" && status != "error" {
		return nil, fmt.Errorf("invalid status %q", status)
	}
	rawIssues := arrayValue(parsed["issues"])
	if rawIssues == nil {
		return nil, fmt.Errorf("missing issues array")
	}
	out := map[string]any{
		"checkName": checkName,
		"status":    status,
		"issues":    []any{},
	}
	if value, ok := neoReviewNumber(parsed["filesAnalyzed"]); ok {
		out["filesAnalyzed"] = value
	}
	if value, ok := neoReviewNumber(parsed["linesAnalyzed"]); ok {
		out["linesAnalyzed"] = value
	}
	if patterns, ok := neoRunCheckStringArray(parsed["patternsChecked"]); ok {
		out["patternsChecked"] = patterns
	} else if parsed["patternsChecked"] != nil {
		return nil, fmt.Errorf("patternsChecked must be a string array")
	}
	if errorMessage := stringValue(parsed["errorMessage"]); errorMessage != "" {
		out["errorMessage"] = errorMessage
	}
	issues := make([]any, 0, len(rawIssues))
	for i, raw := range rawIssues {
		issue, ok := asMap(raw)
		if !ok {
			return nil, fmt.Errorf("issue %d must be an object", i)
		}
		severity := stringValue(issue["severity"])
		file := stringValue(issue["file"])
		problem := stringValue(issue["problem"])
		if !neoReviewSeverityValid(severity) || file == "" || problem == "" {
			return nil, fmt.Errorf("issue %d requires severity, file, and problem", i)
		}
		normalized := map[string]any{"severity": severity, "file": file, "problem": problem}
		if line, ok := neoReviewNumber(issue["line"]); ok {
			normalized["line"] = line
		} else if issue["line"] != nil {
			return nil, fmt.Errorf("issue %d line must be numeric", i)
		}
		if endLine, ok := neoReviewNumber(issue["endLine"]); ok {
			normalized["endLine"] = endLine
		} else if issue["endLine"] != nil {
			return nil, fmt.Errorf("issue %d endLine must be numeric", i)
		}
		for _, key := range []string{"why", "fix"} {
			if value := stringValue(issue[key]); value != "" {
				normalized[key] = value
			}
		}
		issues = append(issues, normalized)
	}
	out["issues"] = issues
	return out, nil
}

func neoRunCheckStringArray(value any) ([]any, bool) {
	if value == nil {
		return nil, false
	}
	switch typed := value.(type) {
	case []string:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, item)
		}
		return out, true
	case []any:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			text, ok := item.(string)
			if !ok {
				return nil, false
			}
			out = append(out, text)
		}
		return out, true
	default:
		return nil, false
	}
}

func neoRunCheckErrorResult(input map[string]any, message string) map[string]any {
	checkName := stringValue(input["checkName"])
	if checkName == "" {
		checkName = "unknown"
	}
	return map[string]any{
		"checkName":    checkName,
		"status":       "error",
		"issues":       []any{},
		"errorMessage": message,
	}
}

// neoRunCheckResultFromText parses the run_check subagent's final message
// into the structured check result the amp review CLI expects. A reply that
// is not the required JSON object, or that fails normalization, degrades to
// an error-status result instead of failing the tool call, mirroring how
// check failures surface upstream.
func neoRunCheckResultFromText(input map[string]any, text string) map[string]any {
	checkName := stringValue(input["checkName"])
	trimmed := strings.TrimSpace(text)
	if fenced := neoStripJSONFence(trimmed); fenced != "" {
		trimmed = fenced
	}
	var parsed map[string]any
	if start := strings.Index(trimmed, "{"); start >= 0 {
		if end := strings.LastIndex(trimmed, "}"); end > start {
			_ = json.Unmarshal([]byte(trimmed[start:end+1]), &parsed)
		}
	}
	if parsed == nil {
		return neoRunCheckErrorResult(input, "check agent did not return a structured result")
	}
	if stringValue(parsed["checkName"]) == "" {
		parsed["checkName"] = checkName
	}
	normalized, err := neoNormalizeRunCheckResult(input, parsed)
	if err != nil {
		return neoRunCheckErrorResult(input, err.Error())
	}
	return normalized
}

func neoStripJSONFence(text string) string {
	if !strings.HasPrefix(text, "```") {
		return ""
	}
	body := strings.TrimPrefix(text, "```")
	if idx := strings.Index(body, "\n"); idx >= 0 {
		body = body[idx+1:]
	}
	if idx := strings.LastIndex(body, "```"); idx >= 0 {
		body = body[:idx]
	}
	return strings.TrimSpace(body)
}
