package amp

import (
	"encoding/json"
	"sort"
	"strconv"
	"strings"
)

const neoCodeReviewToolRunLimit = 20

func runToTextForTool(toolName string, run any) string {
	switch normalizedNeoToolName(toolName) {
	case "codereview":
		if text, ok := neoCodeReviewRunText(run); ok {
			return text
		}
	case "shellcommand", "bash":
		if text, ok := neoStructuredToolRunResultText(run); ok {
			return text
		}
	}
	return runToText(run)
}

func neoStructuredToolRunResultText(run any) (string, bool) {
	m, ok := asMap(run)
	if !ok {
		return "", false
	}
	result, ok := m["result"]
	if !ok || !neoStructuredToolResult(result) {
		return "", false
	}
	raw, err := json.Marshal(result)
	if err != nil {
		return "", false
	}
	return string(raw), true
}

func neoCodeReviewRunText(run any) (string, bool) {
	m, ok := asMap(run)
	if !ok || !strings.EqualFold(strings.TrimSpace(stringValue(m["status"])), "done") {
		return "", false
	}
	result, ok := m["result"]
	if !ok {
		return "", false
	}
	if text := neoToolRunTextResult(result); text != "" {
		return text, true
	}
	return neoCodeReviewResultXML(mapValue(result))
}

func neoCodeReviewResultXML(result map[string]any) (string, bool) {
	if len(result) == 0 {
		return "", false
	}
	main := mapValue(result["main"])
	checks := mapValue(result["checks"])
	if _, hasReview := main["review"]; !hasReview && len(checks) == 0 {
		return "", false
	}

	var b strings.Builder
	var expectedIssues, renderedIssueBodies int
	var expectedComments, renderedCommentBodies int

	if comments := arrayValue(mapValue(main["review"])["comments"]); len(comments) > 0 {
		expectedComments = len(comments)
		b.WriteString("<codeReview>\n")
		for _, raw := range comments {
			c := mapValue(raw)
			text := neoCodeReviewMainCommentText(c)
			if text != "" {
				renderedCommentBodies++
			}
			b.WriteString("<comment>\n")
			neoXMLText(&b, "filename", firstNonEmptyString(stringValue(c["filename"]), stringValue(c["file"])))
			neoXMLText(&b, "startLine", neoCodeReviewLineStr(c["startLine"]))
			neoXMLText(&b, "endLine", neoCodeReviewLineStr(c["endLine"]))
			neoXMLText(&b, "severity", stringValue(c["severity"]))
			neoXMLText(&b, "commentType", stringValue(c["commentType"]))
			neoXMLText(&b, "text", text)
			neoXMLText(&b, "why", stringValue(c["why"]))
			neoXMLText(&b, "fix", stringValue(c["fix"]))
			b.WriteString("</comment>\n")
		}
		b.WriteString("</codeReview>\n")
	}

	neoCodeReviewToolsRunXML(&b, arrayValue(main["toolUses"]))

	keys := make([]string, 0, len(checks))
	for k := range checks {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		ck := mapValue(checks[k])
		res := mapValue(ck["result"])
		summary := mapValue(res["result"])
		issues := arrayValue(res["issues"])
		if issues == nil {
			issues = arrayValue(ck["issues"])
		}
		count := len(issues)
		if c, ok := summary["issuesFound"]; ok && c != nil {
			count = int(numberFrom(c))
		} else if c, ok := res["issuesFound"]; ok && c != nil {
			count = int(numberFrom(c))
		}
		expectedIssues += count
		b.WriteString("<checkResult>\n")
		neoXMLText(&b, "checkName", firstNonEmptyString(
			stringValue(summary["name"]),
			stringValue(res["name"]),
			stringValue(mapValue(res["check"])["name"]),
			stringValue(mapValue(ck["check"])["name"]),
			neoCheckNameFromKey(k),
		))
		neoXMLText(&b, "status", firstNonEmptyString(stringValue(summary["status"]), stringValue(res["status"]), stringValue(ck["status"])))
		neoXMLText(&b, "issuesFound", strconv.Itoa(count))
		if len(issues) > 0 {
			b.WriteString("<issues>\n")
			for _, raw := range issues {
				is := mapValue(raw)
				problem := neoCodeReviewIssueProblem(is)
				if problem != "" {
					renderedIssueBodies++
				}
				b.WriteString(`<issue severity="`)
				b.WriteString(neoXMLAttr(stringValue(is["severity"])))
				b.WriteString(`" file="`)
				b.WriteString(neoXMLAttr(firstNonEmptyString(stringValue(is["file"]), stringValue(is["filename"]))))
				b.WriteString(`" line="`)
				b.WriteString(neoXMLAttr(neoCodeReviewLineStr(is["line"])))
				b.WriteString("\">\n")
				neoXMLText(&b, "problem", problem)
				neoXMLText(&b, "why", stringValue(is["why"]))
				neoXMLText(&b, "fix", stringValue(is["fix"]))
				b.WriteString("</issue>\n")
			}
			b.WriteString("</issues>\n")
		}
		b.WriteString("</checkResult>\n")
	}

	if expectedIssues > renderedIssueBodies {
		return "", false
	}
	if expectedComments > renderedCommentBodies {
		return "", false
	}

	if b.Len() == 0 {
		return "<codeReview>No review findings.</codeReview>", true
	}
	return strings.TrimRight(b.String(), "\n"), true
}

func neoCodeReviewMainCommentText(comment map[string]any) string {
	return firstNonEmptyString(
		stringValue(comment["text"]),
		stringValue(comment["problem"]),
		stringValue(comment["message"]),
		stringValue(comment["title"]),
	)
}

func neoCodeReviewIssueProblem(issue map[string]any) string {
	return firstNonEmptyString(
		stringValue(issue["problem"]),
		stringValue(issue["text"]),
		stringValue(issue["message"]),
		stringValue(issue["title"]),
	)
}

func neoCheckNameFromKey(uri string) string {
	name := uri
	if i := strings.LastIndexByte(name, '/'); i >= 0 {
		name = name[i+1:]
	}
	return strings.TrimSuffix(name, ".md")
}

func neoCodeReviewLineStr(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(t)
	case float64:
		if t == 0 {
			return ""
		}
		return strconv.FormatInt(int64(t), 10)
	case int:
		if t == 0 {
			return ""
		}
		return strconv.Itoa(t)
	case int64:
		if t == 0 {
			return ""
		}
		return strconv.FormatInt(t, 10)
	default:
		if n := numberFrom(v); n != 0 {
			return strconv.FormatInt(int64(n), 10)
		}
		return ""
	}
}

var neoXMLTextEscaper = strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;")
var neoXMLAttrEscaper = strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", `"`, "&quot;")

func neoXMLText(b *strings.Builder, tag, val string) {
	val = strings.TrimSpace(val)
	if val == "" {
		return
	}
	b.WriteString("<")
	b.WriteString(tag)
	b.WriteString(">")
	b.WriteString(neoXMLTextEscaper.Replace(val))
	b.WriteString("</")
	b.WriteString(tag)
	b.WriteString(">\n")
}

func neoXMLAttr(val string) string {
	return neoXMLAttrEscaper.Replace(strings.TrimSpace(val))
}

func neoCodeReviewToolsRunXML(b *strings.Builder, tools []any) {
	if len(tools) == 0 {
		return
	}
	var lines strings.Builder
	written := 0
	omitted := 0
	for _, raw := range tools {
		t := mapValue(raw)
		desc := neoCodeReviewToolDescriptor(mapValue(t["input"]))
		if desc == "" {
			continue
		}
		if written >= neoCodeReviewToolRunLimit {
			omitted++
			continue
		}
		name := firstNonEmptyString(stringValue(t["tool_name"]), stringValue(t["toolName"]), stringValue(t["name"]), "tool")
		lines.WriteString(`<tool name="`)
		lines.WriteString(neoXMLAttr(name))
		lines.WriteString(`">`)
		lines.WriteString(neoXMLTextEscaper.Replace(desc))
		lines.WriteString("</tool>\n")
		written++
	}
	if lines.Len() == 0 {
		return
	}
	b.WriteString("<toolsRun>\n")
	b.WriteString(lines.String())
	if omitted > 0 {
		neoXMLText(b, "omittedTools", strconv.Itoa(omitted))
	}
	b.WriteString("</toolsRun>\n")
}

func neoCodeReviewToolDescriptor(input map[string]any) string {
	for _, key := range []string{"cmd", "command", "path", "file", "filePath", "file_path", "pattern", "query", "url", "diff_description", "prompt"} {
		if v := strings.TrimSpace(stringValue(input[key])); v != "" {
			if len(v) > 300 {
				v = v[:300] + "..."
			}
			return v
		}
	}
	return ""
}
