package amp

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// neoModeSystemPromptKey maps our internal mode key to the systemPrompt value the Amp
// binary uses to identify that mode's profile (only agg-man differs: "aggman").
var neoModeSystemPromptKey = map[string]string{
	"smart":    "smart",
	"large":    "large",
	"rush":     "rush",
	"agg-man":  "aggman",
	"puck":     "puck",
	"deep":     "deep",
	"review":   "review",
	"nostromo": "nostromo",
	"low":      "low",
	"medium":   "deep",
	"high":     "deep",
	"ultra":    "ultra",
}

var neoModeOptionalInAmpBinary = map[string]bool{
	"agg-man": true,
}

var neoModeRuntimeOnlyTools = map[string]map[string]bool{
	"puck": toolSet("rename_thread", "set_thread_pinned", "add_thread_labels", "remove_thread_labels"),
}

func neoModeBinaryComparableTools(mode string, names []string) []string {
	runtimeOnly := neoModeRuntimeOnlyTools[mode]
	out := make([]string, 0, len(names))
	for _, name := range names {
		if !runtimeOnly[name] {
			out = append(out, name)
		}
	}
	return out
}

// TestNeoKnownModeToolsMatchesModeUnion is a pure runtime invariant (no binary
// needed): neoKnownModeTools must equal the union of every mode's included and
// deferred tools. It guards against the known-set and the per-mode lists drifting
// apart in either direction.
func TestNeoKnownModeToolsMatchesModeUnion(t *testing.T) {
	union := map[string]bool{}
	for _, names := range neoModeToolOrder {
		for _, n := range names {
			union[n] = true
		}
	}
	for _, set := range neoModeDeferredToolAllowlist {
		for n := range set {
			union[n] = true
		}
	}
	if missing := neoToolParityDiff(union, neoKnownModeTools); len(missing) > 0 {
		t.Errorf("neoKnownModeTools is missing tools that appear in a mode list: %v", missing)
	}
	if extra := neoToolParityDiff(neoKnownModeTools, union); len(extra) > 0 {
		t.Errorf("neoKnownModeTools contains tools that no mode includes or defers: %v", extra)
	}
}

func TestNeoModeToolOrderMatchesAuditBaseline(t *testing.T) {
	baseline := ampBinaryParityBaselineForTest(t)
	profiles := map[string][]string{}
	for _, profile := range baseline.Signals.AgentModeProfiles {
		if len(profile.ToolNames) > 0 {
			profiles[profile.Name] = profile.ToolNames
		}
	}
	if len(profiles) == 0 {
		t.Fatal("Amp binary parity baseline has no agent mode tool_names")
	}
	for mode, want := range neoModeToolOrder {
		want = neoModeBinaryComparableTools(mode, want)
		got, ok := profiles[mode]
		if !ok {
			if neoModeOptionalInAmpBinary[mode] {
				continue
			}
			t.Fatalf("mode %q has no tool_names in Amp binary parity baseline", mode)
		}
		if !neoToolParityEqualOrdered(got, want) {
			t.Errorf("mode %q includeTools differs from Amp binary baseline:\n  baseline: %v\n  runtime : %v\n  baseline-only: %v\n  runtime-only : %v",
				mode, got, want,
				neoToolParityDiff(neoToolParitySet(got), neoToolParitySet(want)),
				neoToolParityDiff(neoToolParitySet(want), neoToolParitySet(got)))
		}
	}
}

// TestNeoModeToolOrderMatchesAmpBinary resolves each mode's includeTools and
// deferredTools arrays from the installed Amp binary and asserts they match our
// runtime, in order for includeTools and as a set for deferredTools.
func TestNeoModeToolOrderMatchesAmpBinary(t *testing.T) {
	text, path := neoToolParityLoadBinary(t)

	for mode, want := range neoModeToolOrder {
		want = neoModeBinaryComparableTools(mode, want)
		sp, ok := neoModeSystemPromptKey[mode]
		if !ok {
			t.Fatalf("mode %q has no systemPrompt mapping; update neoModeSystemPromptKey", mode)
		}

		idents := neoToolParityProfileIdents(text, sp, "includeTools")
		if len(idents) == 0 {
			if neoModeOptionalInAmpBinary[mode] {
				continue
			}
			t.Fatalf("mode %q (systemPrompt %q): no includeTools reference found in %s", mode, sp, path)
		}
		resolved := make([][]string, 0, len(idents))
		matched := false
		for _, ident := range idents {
			got, ok := neoToolParityResolveArray(text, ident, 0)
			if !ok {
				continue
			}
			resolved = append(resolved, got)
			if neoToolParityEqualOrdered(got, want) {
				matched = true
				break
			}
		}
		if len(resolved) == 0 {
			t.Fatalf("mode %q: could not resolve any includeTools array from binary (idents %v)", mode, idents)
		}
		if !matched {
			t.Errorf("mode %q includeTools differs from all Amp binary candidates:\n  binary candidates: %v\n  runtime: %v", mode, resolved, want)
		}

		wantDeferred := neoModeDeferredToolAllowlist[mode]
		dIdents := neoToolParityProfileIdents(text, sp, "deferredTools")
		deferredResolved := make([][]string, 0, len(dIdents))
		deferredMatched := len(dIdents) == 0
		for _, ident := range dIdents {
			got, ok := neoToolParityResolveArray(text, ident, 0)
			if !ok {
				continue
			}
			deferredResolved = append(deferredResolved, got)
			gotSet := neoToolParitySet(got)
			if len(neoToolParityDiff(gotSet, wantDeferred)) == 0 && len(neoToolParityDiff(wantDeferred, gotSet)) == 0 {
				deferredMatched = true
				break
			}
		}
		if !deferredMatched {
			t.Errorf("mode %q deferredTools differs from all Amp binary candidates:\n  binary candidates: %v\n  runtime: %v", mode, deferredResolved, wantDeferred)
		}
	}
}

func TestNeoResumeHighWaterEventMatchesAmpDecoder(t *testing.T) {
	text, path := neoToolParityLoadBinary(t)
	if !strings.Contains(text, `literal("error_cleared"),seq:`) {
		t.Fatalf("Amp binary %s does not decode a sequenced error_cleared event", path)
	}
	cursorStart := -1
	for searchStart := 0; searchStart < len(text); {
		offset := strings.Index(text[searchStart:], "advanceResumeCursor(")
		if offset < 0 {
			break
		}
		candidate := searchStart + offset
		closeOffset := strings.IndexByte(text[candidate:], ')')
		if closeOffset >= 0 && candidate+closeOffset+1 < len(text) && text[candidate+closeOffset+1] == '{' {
			cursorStart = candidate
			break
		}
		searchStart = candidate + len("advanceResumeCursor(")
	}
	if cursorStart < 0 {
		t.Fatalf("Amp binary %s has no advanceResumeCursor implementation", path)
	}
	cursor := text[cursorStart:min(cursorStart+500, len(text))]
	if !strings.Contains(cursor, `"seq"in`) || !strings.Contains(cursor, ".advanceFromSeq(") {
		t.Fatalf("Amp binary %s resume cursor no longer advances from event seq: %s", path, cursor)
	}
	agentStateStart := strings.Index(text, `literal("agent_state")`)
	if agentStateStart < 0 {
		t.Fatalf("Amp binary %s has no agent_state decoder", path)
	}
	agentStateSchema := text[agentStateStart:min(agentStateStart+220, len(text))]
	if strings.Contains(agentStateSchema, "seq:") {
		t.Fatalf("Amp binary %s agent_state decoder unexpectedly accepts seq: %s", path, agentStateSchema)
	}
}

func neoToolParityLoadBinary(t *testing.T) (string, string) {
	t.Helper()
	path := strings.TrimSpace(os.Getenv("AMP_BINARY"))
	if path == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			t.Skipf("cannot resolve home dir and AMP_BINARY unset; skipping: %v", err)
		}
		path = filepath.Join(home, ".amp", "bin", "amp")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Skipf("Amp binary not found at %s (set AMP_BINARY to override); skipping per-mode tool parity check", path)
	}
	return string(raw), path
}

// neoToolParityProfileIdents finds the ident referenced by field (includeTools or
// deferredTools) within each profile whose systemPrompt equals sp. There are two
// bundles in the binary, so a mode yields more than one ident.
func neoToolParityProfileIdents(text, sp, field string) []string {
	anchor := `systemPrompt:"` + sp + `"`
	fieldKey := field + ":"
	var out []string
	from := 0
	for {
		i := strings.Index(text[from:], anchor)
		if i < 0 {
			break
		}
		abs := from + i
		// Bound the search to this profile only. deferredTools is optional and sits
		// near the profile tail, so an unbounded window would bleed into the next
		// profile (which may defer tools this one does not). The next profile's
		// systemPrompt key is a clean terminator.
		winEnd := min(abs+600, len(text))
		if next := strings.Index(text[abs+len(anchor):winEnd], `systemPrompt:"`); next >= 0 {
			winEnd = abs + len(anchor) + next
		}
		win := text[abs:winEnd]
		if j := strings.Index(win, fieldKey); j >= 0 {
			if id := neoToolParityReadIdent(win[j+len(fieldKey):]); id != "" {
				out = append(out, id)
			}
		}
		from = abs + len(anchor)
	}
	return out
}

// neoToolParityResolveArray resolves a minified array variable to its tool-name list.
// It handles direct array literals (ident=["a","b",...]) and the nostromo-style
// derivation ident=Array.from(new Set([...a,...b])).
func neoToolParityResolveArray(text, ident string, depth int) ([]string, bool) {
	if depth > 5 || ident == "" {
		return nil, false
	}
	if open := neoToolParityIndexUnbound(text, ident+"=["); open >= 0 {
		bracket := open + len(ident) + 1
		if body, ok := neoToolParityScanBracket(text, bracket); ok {
			return neoToolParityArrayLiteralTools(text, body, depth)
		}
	}
	for _, setNeedle := range []string{ident + "=Array.from(new Set([", ident + "=new Set(["} {
		if at := neoToolParityIndexUnbound(text, setNeedle); at >= 0 {
			bracket := at + len(setNeedle) - 1
			body, ok := neoToolParityScanBracket(text, bracket)
			if !ok {
				return nil, false
			}
			items, ok := neoToolParityArrayLiteralTools(text, body, depth)
			if !ok {
				return nil, false
			}
			out := make([]string, 0, len(items))
			seen := map[string]bool{}
			for _, name := range items {
				if !seen[name] {
					seen[name] = true
					out = append(out, name)
				}
			}
			if len(out) > 0 {
				return out, true
			}
		}
	}
	assignNeedle := ident + "="
	if at := neoToolParityIndexUnbound(text, assignNeedle); at >= 0 {
		rest := text[at+len(assignNeedle):]
		alias := neoToolParityReadIdent(rest)
		if alias != "" && alias != ident {
			return neoToolParityResolveArray(text, alias, depth+1)
		}
	}
	return nil, false
}

func neoToolParityArrayLiteralTools(text, body string, depth int) ([]string, bool) {
	var out []string
	for _, part := range strings.Split(body, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if strings.HasPrefix(part, "...") {
			id := neoToolParityReadIdent(part[3:])
			if id == "" {
				return nil, false
			}
			sub, ok := neoToolParityResolveArray(text, id, depth+1)
			if !ok {
				return nil, false
			}
			out = append(out, sub...)
			continue
		}
		out = append(out, neoToolParityQuoted(part)...)
	}
	return out, true
}

// neoToolParityIndexUnbound returns the index of needle in text where the character
// immediately before it is not an identifier byte (so a search for "Ab=[" does not
// match inside "XAb=["). Returns -1 if absent.
func neoToolParityIndexUnbound(text, needle string) int {
	from := 0
	for {
		i := strings.Index(text[from:], needle)
		if i < 0 {
			return -1
		}
		abs := from + i
		if abs == 0 || !neoToolParityIsIdentByte(text[abs-1]) {
			return abs
		}
		from = abs + 1
	}
}

// neoToolParityScanBracket returns the contents between the '[' at open and its
// matching ']', honoring nesting.
func neoToolParityScanBracket(text string, open int) (string, bool) {
	if open >= len(text) || text[open] != '[' {
		return "", false
	}
	depth := 0
	for i := open; i < len(text); i++ {
		switch text[i] {
		case '[':
			depth++
		case ']':
			depth--
			if depth == 0 {
				return text[open+1 : i], true
			}
		}
	}
	return "", false
}

func neoToolParityQuoted(body string) []string {
	var out []string
	i := 0
	for i < len(body) {
		c := body[i]
		if c == '"' || c == '\'' {
			j := strings.IndexByte(body[i+1:], c)
			if j < 0 {
				break
			}
			out = append(out, body[i+1:i+1+j])
			i = i + 1 + j + 1
			continue
		}
		i++
	}
	return out
}

func neoToolParitySpreads(inner string) []string {
	var out []string
	for _, part := range strings.Split(inner, ",") {
		part = strings.TrimSpace(part)
		if strings.HasPrefix(part, "...") {
			if id := neoToolParityReadIdent(part[3:]); id != "" {
				out = append(out, id)
			}
		}
	}
	return out
}

func neoToolParityReadIdent(s string) string {
	end := 0
	for end < len(s) && neoToolParityIsIdentByte(s[end]) {
		end++
	}
	return s[:end]
}

func neoToolParityIsIdentByte(b byte) bool {
	return b >= 'A' && b <= 'Z' || b >= 'a' && b <= 'z' || b >= '0' && b <= '9' || b == '_' || b == '$'
}

func neoToolParityEqualOrdered(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func neoToolParitySet(names []string) map[string]bool {
	out := make(map[string]bool, len(names))
	for _, n := range names {
		out[n] = true
	}
	return out
}

func neoToolParityWithout(names []string, drop string) []string {
	out := make([]string, 0, len(names))
	for _, n := range names {
		if n != drop {
			out = append(out, n)
		}
	}
	return out
}

// neoToolParityDiff returns the names present in a but not in b, sorted.
func neoToolParityDiff(a, b map[string]bool) []string {
	var out []string
	for n := range a {
		if !b[n] {
			out = append(out, n)
		}
	}
	sort.Strings(out)
	return out
}
