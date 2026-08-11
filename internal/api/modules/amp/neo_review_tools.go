package amp

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

const (
	neoReviewMaxChangedFiles         = 100
	neoReviewMaxChangedLines         = 10000
	neoReviewMaxChangedBytes         = 4 * 1024 * 1024
	neoReviewSnapshotHashKey         = "internal.reviewSnapshotHash"
	neoReviewSnapshotFilesKey        = "internal.reviewSnapshotFiles"
	neoReviewSnapshotHunksKey        = "internal.reviewSnapshotHunks"
	neoReviewSnapshotLinesKey        = "internal.reviewSnapshotChangedLines"
	neoReviewSnapshotDeletedLinesKey = "internal.reviewSnapshotDeletedLines"
	neoReviewSnapshotDeletedKey      = "internal.reviewSnapshotDeletedFiles"
	neoReviewSnapshotZeroLineKey     = "internal.reviewSnapshotZeroLineFiles"
	neoReviewSnapshotTextKey         = "internal.reviewSnapshotText"
	neoReviewSnapshotStateMetaKey    = "internal.reviewSnapshot"
	neoRunCheckToolEvidenceKey       = "internal.runCheckToolEvidence"
	neoRunCheckToolEvidenceRequired  = "internal.runCheckToolEvidenceRequired"
)

var (
	neoReviewHunkHeaderPattern        = regexp.MustCompile(`^@@ -[0-9]+(?:,[0-9]+)? \+([0-9]+)(?:,([0-9]+))? @@`)
	neoReviewEmptyRetainedHunkPattern = regexp.MustCompile(`^@@ -1(?:,[0-9]+)? \+(?:0|1),0 @@`)
	neoRunCheckExportIndexPattern     = regexp.MustCompile(`(?is)(?:\bexports\b|\bexportsmap\b|\.exports)\s*(?:\?\.)?\s*\[[^]]+\]`)
	neoRunCheckWildcardExportPattern  = regexp.MustCompile(`(?m)["'][^"'\r\n]*\*[^"'\r\n]*["']\s*:`)
	neoRunCheckExportPacketPattern    = regexp.MustCompile(`(?is)["']CLIPROXY_PACKAGE_EXPORTS=["']\s*\+\s*JSON\.stringify\(\s*pkg\.exports\s*\)`)
)

type neoDependencyAccessPath struct {
	raw     string
	module  string
	members []string
}

type neoReviewDiffHunk struct {
	ID        string
	File      string
	StartLine int
	EndLine   int
	Changed   []string
	Deleted   []string
}

type neoReviewDiffSnapshot struct {
	Hash           string
	RepositoryRoot string
	Files          []string
	Diffs          map[string]string
	Hunks          []neoReviewDiffHunk
}

func neoReviewPromptBase() string {
	return "You are an expert senior engineer with deep knowledge of software engineering best practices, security, performance, and maintainability.\n\nYour task is to perform a code review of the provided diff description. The diff description might be a git or bash command that generates the diff or a description of the diff which can then be used to generate the git or bash command to generate the full diff.\n\nReview adversarially: actively try to disprove correctness, safety, compatibility, performance, and maintainability assumptions in the changed code. Only report concrete, actionable issues tied to the current diff; do not invent speculative problems or style nits.\n\nAfter reading the diff, do the following:\n1. Write a high-level summary of the changes in the diff.\n2. Go file-by-file and review each changed hunk.\n3. Comment on what changed in that hunk (including the line range) and how it relates to other\n   changed hunks and code, reading any other relevant files. Also call out bugs, hackiness,\n   unnecessary code, or too much shared mutable state.\n4. Evaluate abstraction fit in both directions: flag unnecessary indirection (over-abstraction)\n   and missing abstractions (duplication or branching complexity). For each finding, cite concrete\n   locations and recommend exactly one action—simplify/inline or introduce/extract a shared\n   concept—only when it improves current code (avoid speculative refactors).\n\nStrongly prefer to restrict your use of git commands to these when getting the diff or determining which files were added/changed/removed:\n<referenceCommands>\n  <command>\n    <description>committed changes on my branch since diverging from the upstream default branch</description>\n    <bash>git diff --merge-base origin/HEAD HEAD</bash>\n  </command>\n  <command>\n    <description>all current checkout changes since diverging from upstream (commits + staged + unstaged tracked)</description>\n    <bash>git diff --merge-base origin/HEAD</bash>\n  </command>\n  <command>\n    <description>changes since diverging from upstream up to and including staged changes</description>\n    <bash>git diff --cached --merge-base origin/HEAD</bash>\n  </command>\n  <command>\n    <description>current checkout tracked changes since divergence, plus a list of newly added untracked files</description>\n    <bash>git diff --merge-base origin/HEAD</bash>\n    <bash>git ls-files --others --exclude-standard</bash>\n  </command>\n  <command>\n    <description>changes on branch foo since divergence from upstream</description>\n    <bash>git diff --merge-base origin/HEAD foo</bash>\n  </command>\n  <command>\n    <description>only filenames changed by this branch since divergence</description>\n    <bash>git diff --name-only --merge-base origin/HEAD HEAD</bash>\n  </command>\n  <command>\n    <description>scope diff to a specific path since diverging from upstream</description>\n    <bash>git diff --merge-base origin/HEAD <ref-or-empty> -- &lt;pathspec&gt;</bash>\n</command>\n</referenceCommands>\n\nAvoid commands in this format, unless explicitly asked for:\n<avoidCommands>\n  <avoidCommand>git diff <base-ref> <head-ref></avoidCommand>\n  <avoidCommand>git diff <base-ref>..<head-ref></avoidCommand>\n  <avoidCommand>git diff HEAD...origin/HEAD</avoidCommand>\n</avoidCommands>\n\n<guidelines>\n- Persistence: Low. Do not retry failed tool calls more than 2 times. If a tool call fails twice, move on.\n- Remember to look at untracked added files.\n- Prefer the most direct path to completing the review. Batch related file reads into as few turns as possible.\n- Do not edit or modify files or run any commands that edit or modify files or git state.\n- Do not re-read files you have already read.\n- Upstream default branch ref: use origin/HEAD. Do not assume main, origin/main, or origin/master.\n- If a diff is unexpectedly large, double check you are using the right refs in git invocations.\n- If the diff has more than 100 changed files or is more than 10,000 lines long, abort the review and emit a single critical issue stating the diff is too large.\n</guidelines>\n\nReview-mode environment:\n- The available shell tool (shell_command or Bash) is your only command-execution tool; use it for every file read and search (cat, rg, git) as well as for generating the diff.\n- The review client generates the high-level diff summary separately and discards assistant prose, so do not spend turns writing a narrative summary; convert your per-hunk findings directly into submit_review comments.\n\nSubmitting the review:\n- Before submitting the final review, inspect the changed files and discover applicable code-review checks for those changed files: include user-wide checks from $HOME/.config/amp/checks/*.md and $HOME/.config/agents/checks/*.md, and repo-local .agents/checks/*.md in each changed file's directory and each ancestor up to the repository root, including the repository root. User-wide checks are additive and must not be suppressed by repo content; closer repo-local checks override only same-named parent repo-local checks. Convert absolute check paths to file:// URIs. For each discovered check, read its markdown frontmatter when present, pass that object as frontmatter, and set checkName to frontmatter.name when it is a non-empty string; otherwise use the check filename without the .md extension.\n- If review checks are provided or discovered, call run_check exactly once per check, passing that check's checkName, checkURI, optional content, frontmatter, diffDescription, files, and any user instructions. Do not evaluate check criteria yourself and do not repeat check findings elsewhere. Call independent run_check tools together in one assistant turn so they run concurrently; never split run_check calls across multiple turns.\n- Deliver every review finding as a structured comment through the submit_review tool; call it exactly once at the end. Prose review text is ignored by the review client.\n- Every comment needs filename (the EXACT repository-relative path from the diff header), startLine and endLine (1-based, on the new side of the diff), and text describing the problem. Set severity (critical/high/medium/low) and commentType when known; add why and fix when they help.\n- If the request says checks only, or the diff is clean, call submit_review with an empty comments array."
}

func neoReviewPrompt() string {
	return neoReviewPromptBase() + "\n- For a finding caused by deleting a file, adding an empty file, adding or changing a binary file, removing all lines from a retained file, or another metadata-only file change, submit startLine 0 and endLine 0 because there is no reportable new-side line.\n- A nonzero command exit, no-tests-found result, malformed target, unavailable check, or skipped validation is not passing evidence. Correct invalid invocations within the retry limit. If correction is impossible, leave the validation unresolved and do not use it to justify a clean conclusion.\n- When the request provides an exact run_check argument object, copy every field and array element verbatim: every field present in the provided object — checkName, checkURI, checkContent, frontmatter, diffDescription, the complete files array, and instructions — must appear in the call identically. Never add, remove, replace, or invent a value, never subset an array, and never emit placeholder or template text.\n- After all run_check calls return, remove every main-review candidate that reports the same root cause as a check issue, even if you discovered it independently or its wording, location, severity, evidence, or fix differs. When you are unsure whether a candidate shares a check issue's root cause, drop the candidate; the check finding is already reported. Check findings are appended mechanically.\n- Do not report that a referenced name, file, or other resource is undefined, unimported, missing, or unresolved when the review scope is a diff, hunk, or file-scoped excerpt; unchanged or unlisted context may provide it. Report an unresolved reference only after verifying the complete changed file, and only when the name is genuinely absent there.\n- Report a main-review finding only when the complete failing sequence is established by the changed code and the review scope in front of you. If the failure additionally requires any assumed caller, later patch, shared or mutated external state, concurrent or in-flight modification, future evolution of the code, or any other behavior outside the diff, it is speculative: do not report it, even when the vulnerable-looking code path is visible.\n- Your first assistant turn must contain the run_check tool calls for every provided or discovered check; do not emit a preamble, plan, or narration turn before calling tools."
}

func neoCaptureWorkingTreeReviewSnapshot(cwd, diffDescription string, files ...string) (*neoReviewDiffSnapshot, error) {
	return neoCaptureWorkingTreeReviewSnapshotContext(context.Background(), cwd, diffDescription, files...)
}

func neoCaptureWorkingTreeReviewSnapshotContext(ctx context.Context, cwd, diffDescription string, files ...string) (*neoReviewDiffSnapshot, error) {
	if !neoReviewFileScopedWorkingTreeDescription(diffDescription) {
		return nil, nil
	}
	if pathspec, exact := neoReviewGitDiffPathspec(diffDescription); exact {
		revisions, _ := neoReviewGitDiffRevisions(diffDescription)
		return neoCaptureGitDiffReviewSnapshotContext(ctx, cwd, pathspec, revisions...)
	}
	return neoCaptureReviewWorkingTreeSnapshotForFilesContext(ctx, cwd, neoNormalizedReviewSnapshotScope(files))
}

func neoCaptureReviewWorkingTreeSnapshot(cwd string) (*neoReviewDiffSnapshot, error) {
	return neoCaptureReviewWorkingTreeSnapshotContext(context.Background(), cwd)
}

func neoCaptureReviewWorkingTreeSnapshotContext(ctx context.Context, cwd string) (*neoReviewDiffSnapshot, error) {
	return neoCaptureReviewWorkingTreeSnapshotForFilesContext(ctx, cwd, nil)
}

func neoCaptureReviewWorkingTreeSnapshotForFiles(cwd string, pathspec []string) (*neoReviewDiffSnapshot, error) {
	return neoCaptureReviewWorkingTreeSnapshotForFilesContext(context.Background(), cwd, pathspec)
}

func neoCaptureReviewWorkingTreeSnapshotForFilesContext(ctx context.Context, cwd string, pathspec []string) (*neoReviewDiffSnapshot, error) {
	first, firstState, err := neoCaptureReviewWorkingTreeSnapshotOnce(ctx, cwd, pathspec)
	if err != nil {
		return nil, err
	}
	second, secondState, err := neoCaptureReviewWorkingTreeSnapshotOnce(ctx, cwd, pathspec)
	if err != nil {
		return nil, err
	}
	if first.Hash != second.Hash || firstState != secondState {
		return nil, fmt.Errorf("capture review diff: index or working tree changed during capture")
	}
	return second, nil
}

func neoCaptureReviewWorkingTreeSnapshotOnce(ctx context.Context, cwd string, pathspec []string) (*neoReviewDiffSnapshot, string, error) {
	rootOutput, err := neoReviewGit(ctx, cwd, []string{"rev-parse", "--show-toplevel"}, 64*1024, nil, nil)
	if err != nil {
		return nil, "", fmt.Errorf("capture review diff: %w", err)
	}
	root := neoReviewGitLine(rootOutput)
	statusArgs := []string{"--literal-pathspecs", "status", "--porcelain=v1", "--untracked-files=all", "-z"}
	if len(pathspec) > 0 {
		statusArgs = append(statusArgs, "--")
		for _, filename := range pathspec {
			if filename == "" || strings.ContainsRune(filename, '\x00') {
				return nil, "", fmt.Errorf("capture review diff: invalid empty path")
			}
			if filepath.IsAbs(filename) {
				relative, err := filepath.Rel(root, filename)
				if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
					return nil, "", fmt.Errorf("capture review diff: path %q is outside the repository", filename)
				}
				filename = relative
			}
			filename = filepath.Clean(filename)
			if filename == "." || filename == ".." || strings.HasPrefix(filename, ".."+string(filepath.Separator)) {
				return nil, "", fmt.Errorf("capture review diff: invalid path %q", filename)
			}
			statusArgs = append(statusArgs, filepath.ToSlash(filename))
		}
	}
	status, err := neoReviewGit(ctx, root, statusArgs, neoReviewMaxChangedBytes+1, nil, nil)
	if err != nil {
		return nil, "", fmt.Errorf("capture review diff: %w", err)
	}
	if len(status) > neoReviewMaxChangedBytes {
		return nil, "", fmt.Errorf("capture review diff: changed-file metadata exceeds %d bytes", neoReviewMaxChangedBytes)
	}
	entries := neoParseGitPorcelainZ(status)
	for _, entry := range entries {
		if !utf8.ValidString(entry.path) || !utf8.ValidString(entry.previousPath) {
			return nil, "", fmt.Errorf("capture review diff: Git filename is not valid UTF-8")
		}
	}
	if len(pathspec) > 0 {
		fullStatus, err := neoReviewGit(ctx, root, []string{"status", "--porcelain=v1", "--untracked-files=all", "-z"}, neoReviewMaxChangedBytes+1, nil, nil)
		if err != nil {
			return nil, "", fmt.Errorf("capture review diff: %w", err)
		}
		if len(fullStatus) > neoReviewMaxChangedBytes {
			return nil, "", fmt.Errorf("capture review diff: changed-file metadata exceeds %d bytes", neoReviewMaxChangedBytes)
		}
		selected := make(map[string]struct{}, len(entries)*2)
		for _, entry := range entries {
			selected[entry.path] = struct{}{}
			if entry.previousPath != "" {
				selected[entry.previousPath] = struct{}{}
			}
		}
		entries = entries[:0]
		fullEntries := neoParseGitPorcelainZ(fullStatus)
		for _, entry := range fullEntries {
			if !utf8.ValidString(entry.path) || !utf8.ValidString(entry.previousPath) {
				return nil, "", fmt.Errorf("capture review diff: Git filename is not valid UTF-8")
			}
		}
		for _, entry := range fullEntries {
			_, pathSelected := selected[entry.path]
			_, previousPathSelected := selected[entry.previousPath]
			if pathSelected || entry.previousPath != "" && previousPathSelected {
				entries = append(entries, entry)
			}
		}
	}
	if len(entries) > neoReviewMaxChangedFiles {
		return nil, "", fmt.Errorf("capture review diff: more than %d changed files", neoReviewMaxChangedFiles)
	}
	files := make([]string, 0, len(entries))
	diffs := make(map[string]string, len(entries))
	hunks := make([]neoReviewDiffHunk, 0)
	lineCount := 0
	byteCount := 0
	untracked := make([]neoGitStatusEntry, 0)
	tracked := make([]neoGitStatusEntry, 0)
	for _, entry := range entries {
		if entry.changeType == "untracked" {
			untracked = append(untracked, entry)
		} else {
			tracked = append(tracked, entry)
		}
	}
	untrackedDiffs, err := neoCaptureReviewUntrackedDiffs(ctx, root, untracked, neoReviewMaxChangedBytes+1)
	if err != nil {
		return nil, "", err
	}
	trackedDiffs, err := neoCaptureReviewTrackedDiffs(ctx, root, tracked, neoReviewMaxChangedBytes+1)
	if err != nil {
		return nil, "", err
	}
	for _, entry := range entries {
		if entry.path == "" {
			continue
		}
		remaining := neoReviewMaxChangedBytes - byteCount
		diff := untrackedDiffs[entry.path]
		if entry.changeType != "untracked" {
			var ok bool
			diff, ok = trackedDiffs[entry.path]
			if !ok {
				return nil, "", fmt.Errorf("capture review diff: incomplete tracked file capture for %q", entry.path)
			}
		}
		diff = strings.TrimRight(diff, "\n")
		if len(diff) > remaining {
			return nil, "", fmt.Errorf("capture review diff: more than %d diff bytes", neoReviewMaxChangedBytes)
		}
		byteCount += len(diff)
		lineCount += neoReviewDiffLineCount(diff)
		if lineCount > neoReviewMaxChangedLines {
			return nil, "", fmt.Errorf("capture review diff: more than %d diff lines", neoReviewMaxChangedLines)
		}
		files = append(files, entry.path)
		diffs[entry.path] = diff
		hunks = append(hunks, neoReviewDiffHunks(entry.path, diff)...)
	}
	snapshot := &neoReviewDiffSnapshot{
		Hash:           neoReviewSnapshotHash(files, diffs),
		RepositoryRoot: root,
		Files:          files,
		Diffs:          diffs,
		Hunks:          hunks,
	}
	return snapshot, status + "\x00" + snapshot.Hash, nil
}

func neoCaptureGitDiffReviewSnapshot(cwd string, pathspec []string, revisions ...string) (*neoReviewDiffSnapshot, error) {
	return neoCaptureGitDiffReviewSnapshotContext(context.Background(), cwd, pathspec, revisions...)
}

func neoCaptureGitDiffReviewSnapshotContext(ctx context.Context, cwd string, pathspec []string, revisions ...string) (*neoReviewDiffSnapshot, error) {
	first, firstState, err := neoCaptureGitDiffReviewSnapshotOnce(ctx, cwd, pathspec, revisions...)
	if err != nil {
		return nil, err
	}
	second, secondState, err := neoCaptureGitDiffReviewSnapshotOnce(ctx, cwd, pathspec, revisions...)
	if err != nil {
		return nil, err
	}
	if first.Hash != second.Hash || firstState != secondState {
		return nil, fmt.Errorf("capture review diff: index or working tree changed during capture")
	}
	return second, nil
}

func neoCaptureGitDiffReviewSnapshotOnce(ctx context.Context, cwd string, pathspec []string, revisions ...string) (*neoReviewDiffSnapshot, string, error) {
	rootOutput, err := neoReviewGit(ctx, cwd, []string{"rev-parse", "--show-toplevel"}, 64*1024, nil, nil)
	if err != nil {
		return nil, "", fmt.Errorf("capture review diff: %w", err)
	}
	root := neoReviewGitLine(rootOutput)
	resolvedCWD := cwd
	if resolved, resolveErr := filepath.EvalSymlinks(cwd); resolveErr == nil {
		resolvedCWD = resolved
	}
	rootPathspec := make([]string, 0, len(pathspec))
	for _, filename := range pathspec {
		if filename == "" || !utf8.ValidString(filename) || strings.ContainsRune(filename, '\x00') {
			return nil, "", fmt.Errorf("capture review diff: invalid path")
		}
		absolute := filename
		if !filepath.IsAbs(absolute) {
			absolute = filepath.Join(resolvedCWD, absolute)
		}
		relative, err := filepath.Rel(root, absolute)
		if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return nil, "", fmt.Errorf("capture review diff: path %q is outside the repository", filename)
		}
		rootPathspec = append(rootPathspec, filepath.ToSlash(relative))
	}
	if slices.Equal(revisions, []string{"HEAD"}) {
		if _, err := neoReviewGit(ctx, root, []string{"rev-parse", "--verify", "HEAD"}, 128, nil, nil); err != nil {
			if ctx.Err() != nil {
				return nil, "", ctx.Err()
			}
			emptyTreeOutput, err := neoReviewGit(ctx, root, []string{"hash-object", "-t", "tree", "--stdin"}, 128, strings.NewReader(""), nil)
			if err != nil {
				return nil, "", fmt.Errorf("capture review diff: resolve empty tree: %w", err)
			}
			emptyTree := neoReviewGitLine(emptyTreeOutput)
			if emptyTree == "" {
				return nil, "", fmt.Errorf("capture review diff: resolve empty tree: empty object ID")
			}
			revisions = []string{emptyTree}
		}
	}
	baseArgs := []string{"--literal-pathspecs", "-c", "core.quotepath=false", "diff", "--no-color", "--no-ext-diff"}
	nameArgs := append(append([]string{}, baseArgs...), "--name-only", "-z")
	nameArgs = append(nameArgs, revisions...)
	nameArgs = append(nameArgs, "--")
	nameArgs = append(nameArgs, rootPathspec...)
	namesOutput, err := neoReviewGit(ctx, root, nameArgs, neoReviewMaxChangedBytes+1, nil, nil)
	if err != nil {
		return nil, "", fmt.Errorf("capture review diff: %w", err)
	}
	if len(namesOutput) > neoReviewMaxChangedBytes {
		return nil, "", fmt.Errorf("capture review diff: changed-file metadata exceeds %d bytes", neoReviewMaxChangedBytes)
	}
	names := make([]string, 0)
	for _, filename := range strings.Split(namesOutput, "\x00") {
		if filename != "" {
			if !utf8.ValidString(filename) {
				return nil, "", fmt.Errorf("capture review diff: Git filename is not valid UTF-8")
			}
			names = append(names, filename)
		}
	}
	if len(names) > neoReviewMaxChangedFiles {
		return nil, "", fmt.Errorf("capture review diff: more than %d changed files", neoReviewMaxChangedFiles)
	}
	diffArgs := append([]string{}, baseArgs...)
	diffArgs = append(diffArgs, revisions...)
	diffArgs = append(diffArgs, "--")
	diffArgs = append(diffArgs, rootPathspec...)
	combined, err := neoReviewGit(ctx, root, diffArgs, neoReviewMaxChangedBytes+1, nil, nil)
	if err != nil {
		return nil, "", fmt.Errorf("capture review diff: %w", err)
	}
	combined = strings.TrimRight(combined, "\n")
	if len(combined) > neoReviewMaxChangedBytes {
		return nil, "", fmt.Errorf("capture review diff: more than %d diff bytes", neoReviewMaxChangedBytes)
	}
	patches := neoReviewSplitGitDiffPatches(combined)
	if len(patches) != len(names) {
		return nil, "", fmt.Errorf("capture review diff: found %d patches for %d changed files", len(patches), len(names))
	}
	if lineCount := neoReviewDiffLineCount(combined); lineCount > neoReviewMaxChangedLines {
		return nil, "", fmt.Errorf("capture review diff: more than %d diff lines", neoReviewMaxChangedLines)
	}
	files := make([]string, 0, len(names))
	diffs := map[string]string{}
	hunks := make([]neoReviewDiffHunk, 0)
	for index, filename := range names {
		diff := patches[index]
		files = append(files, filename)
		diffs[filename] = diff
		hunks = append(hunks, neoReviewDiffHunks(filename, diff)...)
	}
	sort.Strings(files)
	snapshot := &neoReviewDiffSnapshot{
		Hash:           neoReviewSnapshotHash(files, diffs),
		RepositoryRoot: root,
		Files:          files,
		Diffs:          diffs,
		Hunks:          hunks,
	}
	return snapshot, namesOutput + "\x00" + combined, nil
}

func neoCaptureReviewTrackedDiffs(ctx context.Context, root string, entries []neoGitStatusEntry, maxOutputBytes int) (map[string]string, error) {
	if len(entries) == 0 {
		return map[string]string{}, nil
	}
	paths := make([]string, 0, len(entries)*2)
	seen := make(map[string]bool, len(entries)*2)
	for _, entry := range entries {
		for _, filename := range []string{entry.previousPath, entry.path} {
			if filename == "" || seen[filename] {
				continue
			}
			seen[filename] = true
			paths = append(paths, filename)
		}
	}
	if _, err := neoReviewGit(ctx, root, []string{"rev-parse", "--verify", "HEAD"}, 128, nil, nil); err == nil {
		return neoCaptureReviewTrackedDiffSet(ctx, root, []string{"HEAD"}, paths, maxOutputBytes)
	} else if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	emptyTreeOutput, err := neoReviewGit(ctx, root, []string{"hash-object", "-t", "tree", "--stdin"}, 128, strings.NewReader(""), nil)
	if err != nil {
		return nil, fmt.Errorf("capture review diff: resolve empty tree: %w", err)
	}
	emptyTree := neoReviewGitLine(emptyTreeOutput)
	if emptyTree == "" {
		return nil, fmt.Errorf("capture review diff: resolve empty tree: empty object ID")
	}
	return neoCaptureReviewTrackedDiffSet(ctx, root, []string{emptyTree}, paths, maxOutputBytes)
}

func neoCaptureReviewTrackedDiffSet(ctx context.Context, root string, revisions, paths []string, maxOutputBytes int) (map[string]string, error) {
	baseArgs := []string{"--literal-pathspecs", "-c", "core.quotepath=false", "diff", "--no-color", "--no-ext-diff"}
	nameArgs := append(append([]string{}, baseArgs...), "--name-only", "-z")
	nameArgs = append(nameArgs, revisions...)
	nameArgs = append(nameArgs, "--")
	nameArgs = append(nameArgs, paths...)
	namesOutput, err := neoReviewGit(ctx, root, nameArgs, neoReviewMaxChangedBytes+1, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("capture review diff: %w", err)
	}
	names := make([]string, 0, len(paths))
	for _, filename := range strings.Split(namesOutput, "\x00") {
		if filename == "" {
			continue
		}
		if !utf8.ValidString(filename) {
			return nil, fmt.Errorf("capture review diff: Git filename is not valid UTF-8")
		}
		names = append(names, filename)
	}
	diffArgs := append(append([]string{}, baseArgs...), revisions...)
	diffArgs = append(diffArgs, "--")
	diffArgs = append(diffArgs, paths...)
	combined, err := neoReviewGit(ctx, root, diffArgs, maxOutputBytes, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("capture review diff: %w", err)
	}
	if maxOutputBytes > 0 && len(combined) >= maxOutputBytes {
		return nil, fmt.Errorf("capture review diff: more than %d diff bytes", maxOutputBytes-1)
	}
	patches := neoReviewSplitGitDiffPatches(strings.TrimRight(combined, "\n"))
	if len(patches) != len(names) {
		return nil, fmt.Errorf("capture review diff: found %d tracked patches for %d changed files", len(patches), len(names))
	}
	diffs := make(map[string]string, len(names))
	for index, filename := range names {
		diffs[filename] = patches[index]
	}
	return diffs, nil
}

func neoCaptureReviewUntrackedDiffs(ctx context.Context, root string, entries []neoGitStatusEntry, maxOutputBytes int) (map[string]string, error) {
	diffs := make(map[string]string, len(entries))
	if len(entries) == 0 {
		return diffs, nil
	}
	index, err := os.CreateTemp("", "amp-neo-review-index-*")
	if err != nil {
		return nil, fmt.Errorf("capture review diff: %w", err)
	}
	indexPath := index.Name()
	if err := index.Close(); err != nil {
		_ = os.Remove(indexPath)
		return nil, fmt.Errorf("capture review diff: %w", err)
	}
	if err := os.Remove(indexPath); err != nil {
		return nil, fmt.Errorf("capture review diff: %w", err)
	}
	defer func() { _ = os.Remove(indexPath) }()
	env := make([]string, 0, len(neoGitCommandEnv())+1)
	for _, item := range neoGitCommandEnv() {
		if !strings.HasPrefix(item, "GIT_INDEX_FILE=") {
			env = append(env, item)
		}
	}
	env = append(env, "GIT_INDEX_FILE="+indexPath)
	if _, err := neoReviewGit(ctx, root, []string{"read-tree", "--empty"}, 128, nil, env); err != nil {
		return nil, fmt.Errorf("capture review diff: %w", err)
	}
	var pathspec bytes.Buffer
	for _, entry := range entries {
		pathspec.WriteString(entry.path)
		pathspec.WriteByte(0)
	}
	if _, err := neoReviewGit(ctx, root, []string{"--literal-pathspecs", "add", "-N", "--pathspec-from-file=-", "--pathspec-file-nul"}, 128, &pathspec, env); err != nil {
		return nil, fmt.Errorf("capture review diff: %w", err)
	}
	baseArgs := []string{"-c", "core.quotepath=false", "diff", "--no-color", "--no-ext-diff"}
	nameArgs := append(append([]string{}, baseArgs...), "--name-only", "-z")
	namesOutput, err := neoReviewGit(ctx, root, nameArgs, neoReviewMaxChangedBytes+1, nil, env)
	if err != nil {
		return nil, fmt.Errorf("capture review diff: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, filename := range strings.Split(namesOutput, "\x00") {
		if filename != "" {
			if !utf8.ValidString(filename) {
				return nil, fmt.Errorf("capture review diff: Git filename is not valid UTF-8")
			}
			names = append(names, filename)
		}
	}
	combined, err := neoReviewGit(ctx, root, baseArgs, maxOutputBytes, nil, env)
	if err != nil {
		return nil, fmt.Errorf("capture review diff: %w", err)
	}
	if maxOutputBytes > 0 && len(combined) >= maxOutputBytes {
		return nil, fmt.Errorf("capture review diff: more than %d diff bytes", maxOutputBytes-1)
	}
	patches := neoReviewSplitGitDiffPatches(strings.TrimRight(combined, "\n"))
	if len(patches) != len(names) {
		return nil, fmt.Errorf("capture review diff: found %d untracked patches for %d changed files", len(patches), len(names))
	}
	for index, filename := range names {
		diffs[filename] = patches[index]
	}
	for _, entry := range entries {
		if _, ok := diffs[entry.path]; ok {
			continue
		}
		info, err := os.Stat(filepath.Join(root, filepath.FromSlash(entry.path)))
		if err != nil || !info.Mode().IsRegular() || info.Size() != 0 {
			return nil, fmt.Errorf("capture review diff: incomplete untracked file capture")
		}
		diffs[entry.path] = ""
	}
	if len(diffs) != len(entries) {
		return nil, fmt.Errorf("capture review diff: incomplete untracked file capture")
	}
	return diffs, nil
}

func neoReviewGit(ctx context.Context, cwd string, args []string, maxOutputBytes int, stdin io.Reader, env []string) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if len(env) == 0 {
		env = neoGitCommandEnv()
	}
	stdout := neoGitOutputBuffer{limit: maxOutputBytes}
	if err := neoChangesFileOrderGitWrite(ctx, cwd, args, false, &stdout, stdin, env); err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return stdout.String(), nil
}

func neoReviewGitLine(output string) string {
	output = strings.TrimSuffix(output, "\n")
	return strings.TrimSuffix(output, "\r")
}

func neoReviewSplitGitDiffPatches(diff string) []string {
	if strings.TrimSpace(diff) == "" {
		return nil
	}
	starts := []int{0}
	for offset := 0; ; {
		found := strings.Index(diff[offset:], "\ndiff --git ")
		if found < 0 {
			break
		}
		offset += found + 1
		starts = append(starts, offset)
	}
	patches := make([]string, 0, len(starts))
	for index, start := range starts {
		end := len(diff)
		if index+1 < len(starts) {
			end = starts[index+1] - 1
		}
		patch := strings.TrimRight(diff[start:end], "\n")
		if !strings.HasPrefix(patch, "diff --git ") {
			return nil
		}
		patches = append(patches, patch)
	}
	return patches
}

func neoReviewDiffLineCount(diff string) int {
	if diff == "" {
		return 0
	}
	return strings.Count(diff, "\n") + 1
}

func neoReviewSnapshotHash(files []string, diffs map[string]string) string {
	hash := sha256.New()
	for _, filename := range files {
		hash.Write([]byte(filename))
		hash.Write([]byte{0})
		hash.Write([]byte(diffs[filename]))
		hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func neoReviewSnapshotState(snapshot *neoReviewDiffSnapshot, description, rootMessageID string, scope []string, snapshotErr error) map[string]any {
	state := map[string]any{
		"description":   description,
		"rootMessageID": rootMessageID,
		"scope":         append([]string{}, neoNormalizedReviewSnapshotScope(scope)...),
	}
	if snapshotErr != nil {
		state["error"] = snapshotErr.Error()
		return state
	}
	if snapshot == nil {
		return state
	}
	state["hash"] = snapshot.Hash
	state["repositoryRoot"] = snapshot.RepositoryRoot
	state["files"] = append([]string(nil), snapshot.Files...)
	diffs := make(map[string]any, len(snapshot.Diffs))
	for filename, diff := range snapshot.Diffs {
		diffs[filename] = diff
	}
	state["diffs"] = diffs
	return state
}

func neoReviewSnapshotFromState(state map[string]any, description, rootMessageID string, scope []string) (*neoReviewDiffSnapshot, error) {
	if len(state) == 0 {
		return nil, nil
	}
	if description != "" && stringValue(state["description"]) != description || rootMessageID != "" && stringValue(state["rootMessageID"]) != rootMessageID {
		return nil, nil
	}
	if scope != nil && !slices.Equal(neoReviewSnapshotStateScope(state), neoNormalizedReviewSnapshotScope(scope)) {
		return nil, nil
	}
	if !neoReviewSnapshotStateHasPayload(state) {
		return nil, nil
	}
	if message := strings.TrimSpace(stringValue(state["error"])); message != "" {
		return nil, fmt.Errorf("%s", message)
	}
	files := neoStringSlice(state["files"])
	if len(files) > neoReviewMaxChangedFiles {
		return nil, fmt.Errorf("persisted review snapshot exceeds the file limit")
	}
	rawDiffs := mapValue(state["diffs"])
	diffs := make(map[string]string, len(files))
	hunks := make([]neoReviewDiffHunk, 0)
	lineCount := 0
	byteCount := 0
	for _, filename := range files {
		diff, ok := rawDiffs[filename].(string)
		if !ok {
			return nil, fmt.Errorf("persisted review snapshot is missing %q", filename)
		}
		byteCount += len(diff)
		if byteCount > neoReviewMaxChangedBytes {
			return nil, fmt.Errorf("persisted review snapshot exceeds the byte limit")
		}
		lineCount += neoReviewDiffLineCount(diff)
		if lineCount > neoReviewMaxChangedLines {
			return nil, fmt.Errorf("persisted review snapshot exceeds the line limit")
		}
		diffs[filename] = diff
		hunks = append(hunks, neoReviewDiffHunks(filename, diff)...)
	}
	hash := neoReviewSnapshotHash(files, diffs)
	if expected := strings.TrimSpace(stringValue(state["hash"])); expected == "" || expected != hash {
		return nil, fmt.Errorf("persisted review snapshot hash mismatch")
	}
	return &neoReviewDiffSnapshot{
		Hash:           hash,
		RepositoryRoot: stringValue(state["repositoryRoot"]),
		Files:          files,
		Diffs:          diffs,
		Hunks:          hunks,
	}, nil
}

func neoReviewSnapshotStateScope(state map[string]any) []string {
	if _, exists := state["scope"]; exists {
		return neoNormalizedReviewSnapshotScope(neoStringSlice(state["scope"]))
	}
	return neoNormalizedReviewSnapshotScope(neoStringSlice(state["files"]))
}

func neoReviewSnapshotScope(description string, files []string) []string {
	if pathspec, exact := neoReviewGitDiffPathspec(description); exact {
		return neoNormalizedReviewSnapshotScope(pathspec)
	}
	return neoNormalizedReviewSnapshotScope(files)
}

func neoNormalizedReviewSnapshotScope(files []string) []string {
	normalized := append([]string{}, files...)
	sort.Strings(normalized)
	return slices.Compact(normalized)
}

func neoReviewSnapshotStateHasPayload(state map[string]any) bool {
	for _, key := range []string{"hash", "files", "diffs", "error"} {
		if _, ok := state[key]; ok {
			return true
		}
	}
	return false
}

func neoReviewWorkingTreeDescription(description string) bool {
	switch strings.ToLower(strings.TrimSpace(description)) {
	case "uncommitted changes", "all uncommitted changes", "review all uncommitted changes", "working tree", "working tree changes", "current working tree", "current working tree changes", "worktree", "worktree changes", "current worktree", "current worktree changes", "review the worktree":
		return true
	}
	_, exact := neoReviewGitDiffPathspec(description)
	return exact
}

func neoReviewFileScopedWorkingTreeDescription(description string) bool {
	if neoReviewWorkingTreeDescription(description) {
		return true
	}
	candidate := strings.ToLower(strings.TrimSpace(description))
	for _, prefix := range []string{"could you please ", "would you please ", "could you ", "would you ", "please ", "review these ", "review the ", "review my ", "review ", "all ", "current "} {
		candidate = strings.TrimPrefix(candidate, prefix)
	}
	for _, marker := range []string{"uncommitted", "working tree", "worktree", "local edits", "local changes", "checkout changes"} {
		if candidate == marker || strings.HasPrefix(candidate, marker+" ") {
			return true
		}
	}
	return false
}

func neoReviewGitDiffPathspec(description string) ([]string, bool) {
	revisions, ok := neoReviewGitDiffRevisions(description)
	if !ok {
		return nil, false
	}
	fields, ok := neoReviewCommandFields(strings.TrimLeft(description, " \t\r\n"))
	if !ok {
		return nil, false
	}
	if len(fields) == 2 {
		return []string{}, true
	}
	separator := 2 + len(revisions)
	pathspec := append([]string(nil), fields[separator+1:]...)
	for _, filename := range pathspec {
		if filename == "" || strings.ContainsRune(filename, '\x00') {
			return nil, false
		}
	}
	return pathspec, true
}

func neoReviewGitDiffRevisions(description string) ([]string, bool) {
	fields, ok := neoReviewCommandFields(strings.TrimLeft(description, " \t\r\n"))
	if !ok || len(fields) < 2 || fields[0] != "git" || fields[1] != "diff" {
		return nil, false
	}
	if len(fields) == 2 {
		return nil, true
	}
	if fields[2] == "--" {
		return nil, len(fields) > 3
	}
	if fields[2] == "HEAD" && len(fields) >= 4 && fields[3] == "--" {
		return []string{"HEAD"}, len(fields) > 4
	}
	return nil, false
}

func neoReviewCommandFields(input string) ([]string, bool) {
	fields := make([]string, 0)
	var field strings.Builder
	var quote byte
	escaped := false
	started := false
	flush := func() {
		if started {
			fields = append(fields, field.String())
			field.Reset()
			started = false
		}
	}
	for index := 0; index < len(input); index++ {
		char := input[index]
		if escaped {
			field.WriteByte(char)
			started = true
			escaped = false
			continue
		}
		if quote != 0 {
			if char == quote {
				quote = 0
			} else if char == '\\' && quote == '"' {
				if index+1 >= len(input) {
					return nil, false
				}
				next := input[index+1]
				switch next {
				case '$', '`', '"', '\\':
					field.WriteByte(next)
					index++
				case '\n':
					index++
				default:
					field.WriteByte(char)
				}
			} else {
				field.WriteByte(char)
			}
			started = true
			continue
		}
		switch char {
		case '\'', '"':
			quote = char
			started = true
		case '\\':
			escaped = true
			started = true
		case ';', '|', '&', '<', '>':
			return nil, false
		case ' ', '\t', '\r', '\n':
			flush()
		default:
			field.WriteByte(char)
			started = true
		}
	}
	if quote != 0 || escaped {
		return nil, false
	}
	flush()
	return fields, true
}

func neoReviewDiffDescriptionFromHistory(history []neoHistoryMessage) string {
	const marker = "review this diff:"
	for i := len(history) - 1; i >= 0; i-- {
		role := strings.TrimSpace(history[i].Role)
		if !strings.EqualFold(role, "user") {
			continue
		}
		for _, line := range strings.Split(history[i].Text, "\n") {
			line = strings.TrimLeft(strings.TrimSuffix(line, "\r"), " \t")
			if len(line) >= len(marker) && strings.EqualFold(line[:len(marker)], marker) {
				return strings.TrimLeft(line[len(marker):], " \t")
			}
		}
	}
	return ""
}

func neoReviewRequestFromHistory(history []neoHistoryMessage) (string, []string) {
	for index := len(history) - 1; index >= 0; index-- {
		if !strings.EqualFold(strings.TrimSpace(history[index].Role), "user") {
			continue
		}
		message := history[index : index+1]
		if description := neoReviewDiffDescriptionFromHistory(message); description != "" {
			return description, neoReviewFilesFromHistory(message)
		}
	}
	return "", nil
}

func neoReviewFilesFromHistory(history []neoHistoryMessage) []string {
	const marker = "Focus on these files:"
	for index := len(history) - 1; index >= 0; index-- {
		if !strings.EqualFold(strings.TrimSpace(history[index].Role), "user") {
			continue
		}
		lines := strings.Split(history[index].Text, "\n")
		start := -1
		for lineIndex, line := range lines {
			if strings.EqualFold(strings.TrimSpace(line), marker) {
				start = lineIndex + 1
				break
			}
		}
		if start < 0 {
			continue
		}
		files := make([]string, 0)
		for _, line := range lines[start:] {
			line = strings.TrimSuffix(line, "\r")
			normalized := strings.TrimSpace(line)
			if normalized == "" || neoReviewFilesSectionBoundary(normalized) {
				break
			}
			files = append(files, line)
		}
		return files
	}
	return nil
}

func neoReviewFilesSectionBoundary(line string) bool {
	for _, prefix := range []string{
		"Additional instructions from the user:",
		"Only run review checks with these names:",
		"Only report issues found by checks;",
		"Review depth requested by user:",
		"Pre-discovered review checks are listed below.",
		"No review checks were pre-discovered by the CLI.",
		"Remember: call submit_review exactly once.",
	} {
		if strings.HasPrefix(line, prefix) {
			return true
		}
	}
	return false
}

func neoReviewDiffHunks(filename, diff string) []neoReviewDiffHunk {
	hunks := make([]neoReviewDiffHunk, 0)
	hunkIDCounts := map[string]int{}
	emptyRetained := neoReviewDiffEmptiesRetainedFile(diff)
	current := -1
	newLine := 0
	changedStart := 0
	deletionLine := 0
	deletionPending := false
	flushChanged := func() {
		if current < 0 || changedStart == 0 {
			return
		}
		count := newLine - changedStart
		hunks[current].Changed = append(hunks[current].Changed, fmt.Sprintf("%s@@+%d,%d", filename, changedStart, count))
		changedStart = 0
	}
	flushDeletion := func() {
		if current < 0 || !deletionPending {
			return
		}
		anchor := 0
		if emptyRetained {
			anchor = 0
		} else if hunks[current].EndLine > 0 {
			anchor = min(max(1, deletionLine), hunks[current].EndLine)
		} else if hunks[current].StartLine > 0 {
			anchor = max(1, hunks[current].StartLine-1)
		}
		count := 0
		if anchor > 0 {
			count = 1
		}
		hunks[current].Deleted = append(hunks[current].Deleted, fmt.Sprintf("%s@@+%d,%d", filename, anchor, count))
		deletionPending = false
	}
	for _, line := range strings.Split(diff, "\n") {
		match := neoReviewHunkHeaderPattern.FindStringSubmatch(line)
		if len(match) != 0 {
			flushChanged()
			flushDeletion()
			startLine, _ := strconv.Atoi(match[1])
			lineCount := 1
			if len(match) > 2 && match[2] != "" {
				lineCount, _ = strconv.Atoi(match[2])
			}
			endLine := max(0, startLine-1)
			if lineCount > 0 {
				endLine = startLine + lineCount - 1
			}
			hunkID := fmt.Sprintf("%s@@+%d,%d", filename, startLine, lineCount)
			hunkIDCounts[hunkID]++
			if hunkIDCounts[hunkID] > 1 {
				hunkID = fmt.Sprintf("%s#%d", hunkID, hunkIDCounts[hunkID])
			}
			hunks = append(hunks, neoReviewDiffHunk{
				ID:        hunkID,
				File:      filename,
				StartLine: startLine,
				EndLine:   endLine,
			})
			current = len(hunks) - 1
			newLine = startLine
			continue
		}
		if current < 0 || line == "" || strings.HasPrefix(line, "\\") {
			continue
		}
		switch line[0] {
		case '+':
			flushDeletion()
			if changedStart == 0 {
				changedStart = newLine
			}
			newLine++
		case '-':
			flushChanged()
			if !deletionPending {
				deletionLine = newLine
				deletionPending = true
			}
		case ' ':
			flushChanged()
			flushDeletion()
			newLine++
		default:
			flushChanged()
			flushDeletion()
		}
	}
	flushChanged()
	flushDeletion()
	return hunks
}

func neoPrepareRunCheckSnapshotInput(input map[string]any, snapshot *neoReviewDiffSnapshot) (map[string]any, error) {
	prepared := cloneMap(input)
	if snapshot == nil {
		return prepared, nil
	}
	requested := neoStringSlice(input["files"])
	if len(requested) == 0 {
		requested = append([]string(nil), snapshot.Files...)
	}
	requestedSet := make(map[string]bool, len(requested))
	for _, filename := range requested {
		requestedSet[filename] = true
	}
	missing := make([]string, 0)
	for _, filename := range requested {
		if _, ok := snapshot.Diffs[filename]; !ok {
			missing = append(missing, filename)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return nil, fmt.Errorf("run_check files are outside the immutable snapshot: %s", strings.Join(missing, ", "))
	}
	files := make([]string, 0, len(requested))
	for _, filename := range snapshot.Files {
		if !requestedSet[filename] {
			continue
		}
		files = append(files, filename)
	}
	sort.Strings(files)
	diffs := make([]string, 0, len(files))
	for _, filename := range files {
		diffs = append(diffs, snapshot.Diffs[filename])
	}
	hunks := make([]string, 0)
	changedLines := make([]string, 0)
	deletedLines := make([]string, 0)
	deletedFiles := make([]string, 0)
	zeroLineFiles := make([]string, 0)
	for _, hunk := range snapshot.Hunks {
		if requestedSet[hunk.File] {
			hunks = append(hunks, hunk.ID)
			changedLines = append(changedLines, hunk.Changed...)
			deletedLines = append(deletedLines, hunk.Deleted...)
		}
	}
	for _, filename := range files {
		if neoReviewDiffDeletesFile(snapshot.Diffs[filename]) {
			deletedFiles = append(deletedFiles, filename)
		} else if neoReviewDiffHasNoNewTextLine(snapshot.Diffs[filename]) || neoReviewStringSetContains(deletedLines, filename+"@@+0,0") {
			zeroLineFiles = append(zeroLineFiles, filename)
		}
	}
	sort.Strings(hunks)
	sort.Strings(changedLines)
	sort.Strings(deletedLines)
	sort.Strings(zeroLineFiles)
	hash := sha256.New()
	for i, filename := range files {
		hash.Write([]byte(filename))
		hash.Write([]byte{0})
		hash.Write([]byte(diffs[i]))
		hash.Write([]byte{0})
	}
	snapshotHash := hex.EncodeToString(hash.Sum(nil))
	var packet strings.Builder
	fmt.Fprintf(&packet, "Immutable review diff snapshot (sha256:%s). Evaluate this exact patch; do not regenerate or replace it with the current working tree.\n", snapshotHash)
	if snapshot.RepositoryRoot != "" {
		fmt.Fprintf(&packet, "Repository root: %s\n", snapshot.RepositoryRoot)
	}
	packet.WriteString("Files (JSON strings):\n")
	for _, filename := range files {
		encoded, _ := json.Marshal(filename)
		fmt.Fprintf(&packet, "- %s\n", encoded)
	}
	packet.WriteString("Hunks (JSON strings):\n")
	for _, hunk := range hunks {
		encoded, _ := json.Marshal(hunk)
		fmt.Fprintf(&packet, "- %s\n", encoded)
	}
	packet.WriteString("Changed lines (JSON objects with exact new-side locations):\n")
	for i, filename := range files {
		newLine := 0
		inHunk := false
		for _, line := range strings.Split(diffs[i], "\n") {
			if match := neoReviewHunkHeaderPattern.FindStringSubmatch(line); len(match) != 0 {
				newLine, _ = strconv.Atoi(match[1])
				inHunk = true
				continue
			}
			if !inHunk || line == "" || strings.HasPrefix(line, "\\") {
				continue
			}
			switch line[0] {
			case '+':
				encoded, _ := json.Marshal(map[string]any{"file": filename, "line": newLine, "text": line[1:]})
				packet.Write(encoded)
				packet.WriteByte('\n')
				newLine++
			case ' ':
				newLine++
			}
		}
	}
	packet.WriteString("\n<review_diff_snapshot>\n")
	packet.WriteString(strings.Join(diffs, "\n"))
	packet.WriteString("\n</review_diff_snapshot>")
	prepared[neoReviewSnapshotHashKey] = snapshotHash
	prepared[neoReviewSnapshotFilesKey] = files
	prepared[neoReviewSnapshotHunksKey] = hunks
	prepared[neoReviewSnapshotLinesKey] = changedLines
	prepared[neoReviewSnapshotDeletedLinesKey] = deletedLines
	prepared[neoReviewSnapshotDeletedKey] = deletedFiles
	prepared[neoReviewSnapshotZeroLineKey] = zeroLineFiles
	prepared[neoReviewSnapshotTextKey] = packet.String()
	return prepared, nil
}

func neoRunCheckToolSpec() neoToolSpec {
	return neoToolSpec{
		Name:        "run_check",
		Description: "Run a single discovered review check against the changes under review. Call this once per check provided in the review request, passing the check's name, URI, optional embedded content, frontmatter, diff description, files, and any additional instructions. Copy any exact argument object from the request verbatim; never add, remove, replace, or invent values or placeholders. If content is not embedded, load criteria from the check URI before evaluating it. Returns the structured result of evaluating that check.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"checkName":       map[string]any{"type": "string", "description": "The name of the check, exactly as provided in the review request."},
				"checkURI":        map[string]any{"type": "string", "description": "The URI of the check, exactly as provided in the review request."},
				"checkContent":    map[string]any{"type": []any{"string", "null"}, "description": "Optional full markdown content of the check when already supplied by a legacy caller."},
				"frontmatter":     neoRunCheckFrontmatterSchema(),
				"diffDescription": map[string]any{"type": "string", "description": "The description of the diff under review."},
				"files":           map[string]any{"type": "array", "items": map[string]any{"type": "string"}, "description": "The files under review."},
				"instructions":    map[string]any{"type": "string", "description": "Additional user directives for review focus, severity filtering, or scope narrowing that must be honored while evaluating this check."},
			},
			"required":             []any{"checkName", "checkURI", "checkContent", "frontmatter", "diffDescription", "files", "instructions"},
			"additionalProperties": false,
		},
		Meta:   map[string]any{"source": "server"},
		Strict: true,
	}
}

func neoRunCheckFrontmatterSchema() map[string]any {
	return map[string]any{
		"type": []any{"object", "null"},
		"properties": map[string]any{
			"name":             map[string]any{"type": []any{"string", "null"}},
			"description":      map[string]any{"type": []any{"string", "null"}},
			"severity-default": map[string]any{"type": []any{"string", "null"}},
			"tools":            map[string]any{"type": []any{"array", "null"}, "items": map[string]any{"type": "string"}},
		},
		"required":             []any{"name", "description", "severity-default", "tools"},
		"additionalProperties": false,
	}
}

func neoReviewEmbeddedRunCheckInputs(history []neoHistoryMessage) map[string]map[string]any {
	var registry map[string]map[string]any
	const openTag = "<review_check_arguments>"
	const closeTag = "</review_check_arguments>"
	for _, message := range history {
		if !strings.EqualFold(strings.TrimSpace(message.Role), "user") {
			continue
		}
		text := message.Text
		for {
			start := strings.Index(text, openTag)
			if start < 0 {
				break
			}
			text = text[start+len(openTag):]
			end := strings.Index(text, closeTag)
			if end < 0 {
				break
			}
			raw := strings.TrimSpace(text[:end])
			text = text[end+len(closeTag):]
			var parsed map[string]any
			if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
				continue
			}
			name := stringValue(parsed["checkName"])
			if name == "" {
				continue
			}
			if registry == nil {
				registry = make(map[string]map[string]any)
			}
			registry[name] = parsed
		}
	}
	return registry
}

func neoRepairRunCheckInput(registry map[string]map[string]any, name string, input map[string]any) (map[string]any, bool) {
	if len(registry) == 0 || name != "run_check" || input == nil {
		return input, false
	}
	canonical := registry[stringValue(input["checkName"])]
	if canonical == nil {
		return input, false
	}
	if reflect.DeepEqual(input, canonical) {
		return input, false
	}
	return cloneMap(canonical), true
}

func neoSubmitReviewToolSpec() neoToolSpec {
	nullableString := func(description string) map[string]any {
		return map[string]any{"type": []any{"string", "null"}, "description": description}
	}
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
							"startLine":   map[string]any{"type": "number", "description": "First line of the commented range (1-based, new side of the diff; use 0 when the changed file has no new-side line)."},
							"endLine":     map[string]any{"type": "number", "description": "Last line of the commented range (1-based, new side of the diff; use 0 when the changed file has no new-side line)."},
							"text":        map[string]any{"type": "string", "description": "The review comment describing the problem."},
							"commentType": map[string]any{"type": []any{"string", "null"}, "enum": []any{"bug", "suggested_edit", "compliment", "non_actionable", "unknown", nil}, "description": "The kind of comment."},
							"severity":    map[string]any{"type": []any{"string", "null"}, "enum": []any{"critical", "high", "medium", "low", nil}, "description": "How severe the problem is."},
							"source":      nullableString("What surfaced the comment."),
							"why":         nullableString("Why the problem matters."),
							"fix":         nullableString("Suggested fix."),
						},
						"required":             []any{"filename", "startLine", "endLine", "text", "commentType", "severity", "source", "why", "fix"},
						"additionalProperties": false,
					},
					"description": "All review comments. Empty when the diff is clean.",
				},
			},
			"required":             []any{"comments"},
			"additionalProperties": false,
		},
		Meta:   map[string]any{"source": "server"},
		Strict: true,
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
		if !neoReviewPathValid(filename) {
			return nil, fmt.Errorf("submit_review comment %d has invalid repository-relative filename %q", i, filename)
		}
		startLine, ok := neoReviewNumber(comment["startLine"])
		if !ok {
			return nil, fmt.Errorf("submit_review comment %d requires numeric startLine", i)
		}
		endLine, ok := neoReviewNumber(comment["endLine"])
		if !ok {
			return nil, fmt.Errorf("submit_review comment %d requires numeric endLine", i)
		}
		if !neoReviewLineRangeValid(startLine, endLine) {
			return nil, fmt.Errorf("submit_review comment %d has invalid line range %d-%d", i, startLine, endLine)
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

func neoValidateSubmittedReviewSnapshot(result map[string]any, snapshot *neoReviewDiffSnapshot) error {
	if snapshot == nil {
		return nil
	}
	files := snapshot.Files
	changedLines := make([]string, 0)
	deletedLines := make([]string, 0)
	for _, hunk := range snapshot.Hunks {
		changedLines = append(changedLines, hunk.Changed...)
		deletedLines = append(deletedLines, hunk.Deleted...)
	}
	for i, raw := range arrayValue(result["comments"]) {
		comment := mapValue(raw)
		filename := stringValue(comment["filename"])
		if !neoReviewStringSetContains(files, filename) {
			return fmt.Errorf("submit_review comment %d file %q is outside the immutable snapshot", i, filename)
		}
		startLine := numberFrom(comment["startLine"])
		endLine := numberFrom(comment["endLine"])
		if startLine == 0 && endLine == 0 {
			if !neoReviewZeroRangeAllowed(filename, snapshot.Diffs[filename], deletedLines) {
				return fmt.Errorf("submit_review comment %d uses the deleted-file range for non-deleted file %q", i, filename)
			}
			continue
		}
		if !neoReviewRangeWithinSnapshotHunk(filename, startLine, endLine, changedLines) && !neoReviewRangeWithinSnapshotHunk(filename, startLine, endLine, deletedLines) {
			return fmt.Errorf("submit_review comment %d range %s:%d-%d is outside changed hunks", i, filename, startLine, endLine)
		}
	}
	return nil
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

func neoReviewPathValid(value string) bool {
	if value == "" || !utf8.ValidString(value) || strings.Contains(value, "\x00") || strings.HasPrefix(value, "/") {
		return false
	}
	platformPath := strings.ReplaceAll(value, `\`, "/")
	if strings.HasPrefix(platformPath, "/") || len(platformPath) >= 2 && platformPath[1] == ':' && (platformPath[0] >= 'A' && platformPath[0] <= 'Z' || platformPath[0] >= 'a' && platformPath[0] <= 'z') {
		return false
	}
	platformCleaned := path.Clean(platformPath)
	if platformCleaned != platformPath || platformCleaned == ".." || strings.HasPrefix(platformCleaned, "../") {
		return false
	}
	cleaned := path.Clean(value)
	return cleaned == value && cleaned != "." && cleaned != ".." && !strings.HasPrefix(cleaned, "../")
}

func neoRunCheckResponseJSONSchema() map[string]any {
	nullableString := func() map[string]any {
		return map[string]any{"type": []any{"string", "null"}}
	}
	nullableStringArray := func() map[string]any {
		return map[string]any{"type": []any{"array", "null"}, "items": map[string]any{"type": "string"}}
	}
	nullableEnum := func(values ...string) map[string]any {
		enum := make([]any, 0, len(values)+1)
		for _, value := range values {
			enum = append(enum, value)
		}
		enum = append(enum, nil)
		return map[string]any{"type": []any{"string", "null"}, "enum": enum}
	}
	evidenceProperties := map[string]any{
		"patternIndex":        map[string]any{"type": "integer"},
		"observation":         map[string]any{"type": "string"},
		"sources":             map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
		"outcome":             map[string]any{"type": "string", "enum": []any{"finding", "no-finding", "not-applicable"}},
		"issueIndexes":        map[string]any{"type": "array", "items": map[string]any{"type": "integer"}},
		"dependency":          nullableString(),
		"floorVersion":        nullableString(),
		"accessPath":          nullableString(),
		"floorStatus":         nullableEnum("compatible", "incompatible", "unverified"),
		"verification":        nullableEnum("root-runtime-traversal", "root-source-construction", "root-type-declaration", "exact-export-inspection", "exact-behavior-test"),
		"rootEvidence":        nullableStringArray(),
		"phaseRelationship":   nullableString(),
		"budgetOrigin":        nullableEnum("original", "remaining", "independent"),
		"decisiveSequence":    nullableString(),
		"implementationOwner": nullableString(),
		"sourceLifetime":      nullableEnum("mutable", "immutable", "unknown"),
		"decisionLifetime":    nullableEnum("per-use", "retained", "unknown"),
		"mutationPath":        nullableString(),
		"usePath":             nullableString(),
	}
	evidenceRequired := make([]any, 0, len(evidenceProperties))
	for key := range evidenceProperties {
		evidenceRequired = append(evidenceRequired, key)
	}
	issueProperties := map[string]any{
		"severity": map[string]any{"type": "string", "enum": []any{"low", "medium", "high", "critical"}},
		"file":     map[string]any{"type": "string"},
		"line":     map[string]any{"type": "integer"},
		"endLine":  map[string]any{"type": []any{"integer", "null"}},
		"problem":  map[string]any{"type": "string"},
		"why":      map[string]any{"type": "string"},
		"fix":      map[string]any{"type": "string"},
	}
	issueRequired := make([]any, 0, len(issueProperties))
	for key := range issueProperties {
		issueRequired = append(issueRequired, key)
	}
	properties := map[string]any{
		"checkName":       map[string]any{"type": "string"},
		"status":          map[string]any{"type": "string", "enum": []any{"completed", "error"}},
		"filesAnalyzed":   map[string]any{"type": "integer"},
		"linesAnalyzed":   map[string]any{"type": "integer"},
		"patternsChecked": map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
		"evidence": map[string]any{"type": "array", "items": map[string]any{
			"type":                 "object",
			"properties":           evidenceProperties,
			"required":             evidenceRequired,
			"additionalProperties": false,
		}},
		"issues": map[string]any{"type": "array", "items": map[string]any{
			"type":                 "object",
			"properties":           issueProperties,
			"required":             issueRequired,
			"additionalProperties": false,
		}},
		"errorMessage": nullableString(),
		"coveredFiles": map[string]any{"type": []any{"array", "null"}, "items": map[string]any{"type": "string"}},
		"coveredHunks": map[string]any{"type": []any{"array", "null"}, "items": map[string]any{"type": "string"}},
	}
	required := make([]any, 0, len(properties))
	for key := range properties {
		required = append(required, key)
	}
	return map[string]any{
		"type":                 "object",
		"properties":           properties,
		"required":             required,
		"additionalProperties": false,
	}
}

func neoNormalizeRunCheckResult(input map[string]any, parsed map[string]any) (map[string]any, error) {
	checkName := stringValue(input["checkName"])
	if checkName == "" {
		return nil, fmt.Errorf("missing checkName")
	}
	if reported := stringValue(parsed["checkName"]); reported != "" && reported != checkName {
		return nil, fmt.Errorf("checkName %q does not match requested check %q", reported, checkName)
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
	if status == "error" && len(rawIssues) != 0 {
		return nil, fmt.Errorf("error status cannot include issues")
	}
	errorMessage := stringValue(parsed["errorMessage"])
	if status == "error" && errorMessage == "" {
		return nil, fmt.Errorf("error status requires errorMessage")
	}
	out := map[string]any{
		"checkName": checkName,
		"status":    status,
		"issues":    []any{},
	}
	expectedFiles := neoStringSlice(input[neoReviewSnapshotFilesKey])
	expectedHunks := neoStringSlice(input[neoReviewSnapshotHunksKey])
	expectedChangedLines := neoStringSlice(input[neoReviewSnapshotLinesKey])
	expectedDeletedLines := neoStringSlice(input[neoReviewSnapshotDeletedLinesKey])
	expectedDeletedFiles := neoStringSlice(input[neoReviewSnapshotDeletedKey])
	expectedZeroLineFiles := neoStringSlice(input[neoReviewSnapshotZeroLineKey])
	snapshotHash := stringValue(input[neoReviewSnapshotHashKey])
	if value, ok := neoReviewNumber(parsed["filesAnalyzed"]); ok {
		out["filesAnalyzed"] = value
		if snapshotHash != "" && value != len(expectedFiles) {
			return nil, fmt.Errorf("filesAnalyzed = %d, want %d files from immutable snapshot", value, len(expectedFiles))
		}
	} else if snapshotHash != "" && status == "completed" {
		return nil, fmt.Errorf("missing filesAnalyzed for immutable snapshot")
	}
	if value, ok := neoReviewNumber(parsed["linesAnalyzed"]); ok {
		out["linesAnalyzed"] = value
	}
	patterns, patternsOK := neoRunCheckStringArray(parsed["patternsChecked"])
	if patternsOK {
		for i, raw := range patterns {
			if strings.TrimSpace(raw.(string)) == "" {
				return nil, fmt.Errorf("patternsChecked entry %d must not be empty", i)
			}
		}
		out["patternsChecked"] = patterns
	} else if parsed["patternsChecked"] != nil {
		return nil, fmt.Errorf("patternsChecked must be a string array")
	}
	if errorMessage != "" {
		out["errorMessage"] = errorMessage
	}
	if snapshotHash != "" && status == "completed" {
		coveredFiles, ok := neoRunCheckStringSlice(parsed["coveredFiles"])
		if !ok || !neoReviewSameStringSet(coveredFiles, expectedFiles) {
			return nil, fmt.Errorf("coveredFiles must exactly match the immutable snapshot files")
		}
		coveredHunks, ok := neoRunCheckStringSlice(parsed["coveredHunks"])
		if !ok || !neoReviewSameStringSet(coveredHunks, expectedHunks) {
			return nil, fmt.Errorf("coveredHunks must exactly match the immutable snapshot hunks")
		}
		out["coveredFiles"] = stringArrayValue(coveredFiles)
		out["coveredHunks"] = stringArrayValue(coveredHunks)
		out["snapshotHash"] = snapshotHash
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
		if !neoReviewSeverityValid(severity) || problem == "" || file == "" {
			return nil, fmt.Errorf("issue %d requires severity, file, and problem", i)
		}
		if !neoReviewPathValid(file) {
			return nil, fmt.Errorf("issue %d has invalid repository-relative file %q", i, file)
		}
		if snapshotHash != "" && !neoReviewStringSetContains(expectedFiles, file) {
			return nil, fmt.Errorf("issue %d file %q is outside the immutable snapshot", i, file)
		}
		normalized := map[string]any{"severity": severity, "file": file, "problem": problem}
		line := 0
		hasLine := false
		if value, ok := neoReviewNumber(issue["line"]); ok {
			line = value
			hasLine = true
			normalized["line"] = line
		} else if issue["line"] != nil {
			return nil, fmt.Errorf("issue %d line must be numeric", i)
		} else if snapshotHash != "" {
			return nil, fmt.Errorf("issue %d requires a line from the immutable snapshot", i)
		}
		endLine := line
		if value, ok := neoReviewNumber(issue["endLine"]); ok {
			if !hasLine {
				return nil, fmt.Errorf("issue %d endLine requires line", i)
			}
			endLine = value
			normalized["endLine"] = endLine
		} else if issue["endLine"] != nil {
			return nil, fmt.Errorf("issue %d endLine must be numeric", i)
		}
		if hasLine && !neoReviewLineRangeValid(line, endLine) {
			return nil, fmt.Errorf("issue %d has invalid line range %d-%d", i, line, endLine)
		}
		if snapshotHash != "" && line == 0 && endLine == 0 {
			if !neoReviewStringSetContains(expectedDeletedFiles, file) && !neoReviewStringSetContains(expectedZeroLineFiles, file) && !neoReviewStringSetContains(expectedDeletedLines, file+"@@+0,0") {
				return nil, fmt.Errorf("issue %d uses the deleted-file range for non-deleted file %q", i, file)
			}
		} else if snapshotHash != "" && !neoReviewRangeWithinSnapshotHunk(file, line, endLine, expectedChangedLines) && !neoReviewRangeWithinSnapshotHunk(file, line, endLine, expectedDeletedLines) {
			return nil, fmt.Errorf("issue %d range %s:%d-%d is outside changed or deleted lines", i, file, line, endLine)
		}
		for _, key := range []string{"why", "fix"} {
			if value := stringValue(issue[key]); value != "" {
				normalized[key] = value
			}
		}
		issues = append(issues, normalized)
	}
	out["issues"] = issues
	if status == "completed" {
		if !patternsOK || len(patterns) == 0 {
			return nil, fmt.Errorf("completed result requires non-empty patternsChecked")
		}
		rawEvidence := arrayValue(parsed["evidence"])
		if rawEvidence == nil || len(rawEvidence) != len(patterns) {
			return nil, fmt.Errorf("evidence must contain exactly one entry for each checked pattern")
		}
		evidence := make([]any, 0, len(rawEvidence))
		seen := make(map[int]bool, len(rawEvidence))
		referencedIssues := make([]bool, len(issues))
		seenCapabilities := make(map[string]bool, len(rawEvidence))
		for i, raw := range rawEvidence {
			entry, ok := asMap(raw)
			if !ok {
				return nil, fmt.Errorf("evidence entry %d must be an object", i)
			}
			patternIndex, ok := neoReviewNumber(entry["patternIndex"])
			if !ok || patternIndex < 0 || patternIndex >= len(patterns) || seen[patternIndex] {
				return nil, fmt.Errorf("evidence entry %d has invalid or duplicate patternIndex", i)
			}
			seen[patternIndex] = true
			observation := strings.TrimSpace(stringValue(entry["observation"]))
			if observation == "" {
				return nil, fmt.Errorf("evidence entry %d requires an observation", i)
			}
			sources, ok := neoRunCheckStringSlice(entry["sources"])
			if !ok || len(sources) == 0 {
				return nil, fmt.Errorf("evidence entry %d requires sources", i)
			}
			for _, source := range sources {
				if strings.TrimSpace(source) == "" {
					return nil, fmt.Errorf("evidence entry %d contains an empty source", i)
				}
			}
			rawIssueIndexes := arrayValue(entry["issueIndexes"])
			if entry["issueIndexes"] != nil && rawIssueIndexes == nil {
				return nil, fmt.Errorf("evidence entry %d issueIndexes must be an array", i)
			}
			issueIndexes := make([]any, 0, len(rawIssueIndexes))
			seenIssueIndexes := make(map[int]bool, len(rawIssueIndexes))
			for _, rawIssueIndex := range rawIssueIndexes {
				issueIndex, ok := neoReviewNumber(rawIssueIndex)
				if !ok || issueIndex < 0 || issueIndex >= len(issues) || seenIssueIndexes[issueIndex] {
					return nil, fmt.Errorf("evidence entry %d has invalid or duplicate issue index", i)
				}
				seenIssueIndexes[issueIndex] = true
				referencedIssues[issueIndex] = true
				issueIndexes = append(issueIndexes, issueIndex)
			}
			outcome := strings.TrimSpace(stringValue(entry["outcome"]))
			switch outcome {
			case "finding":
				if len(issueIndexes) == 0 {
					return nil, fmt.Errorf("finding evidence entry %d requires issueIndexes", i)
				}
			case "no-finding", "not-applicable":
				if len(issueIndexes) != 0 {
					return nil, fmt.Errorf("non-finding evidence entry %d cannot reference issues", i)
				}
			default:
				return nil, fmt.Errorf("evidence entry %d has invalid outcome %q", i, outcome)
			}
			normalizedEvidence := map[string]any{
				"patternIndex": patternIndex,
				"observation":  observation,
				"sources":      stringArrayValue(sources),
				"outcome":      outcome,
				"issueIndexes": issueIndexes,
			}
			if checkName == "published-dependency-capability-floor" && outcome != "not-applicable" {
				dependency := strings.TrimSpace(stringValue(entry["dependency"]))
				floorVersion := strings.TrimSpace(stringValue(entry["floorVersion"]))
				accessPath := strings.TrimSpace(stringValue(entry["accessPath"]))
				floorStatus := strings.TrimSpace(stringValue(entry["floorStatus"]))
				verification := strings.TrimSpace(stringValue(entry["verification"]))
				rootEvidence, rootEvidenceOK := neoRunCheckStringSlice(entry["rootEvidence"])
				if dependency == "" || floorVersion == "" || accessPath == "" {
					return nil, fmt.Errorf("dependency-floor evidence entry %d requires dependency, floorVersion, and accessPath", i)
				}
				parsedAccessPath, accessPathErr := neoParseDependencyAccessPath(accessPath)
				if accessPathErr != nil {
					return nil, fmt.Errorf("dependency-floor evidence entry %d has invalid accessPath: %w", i, accessPathErr)
				}
				expectedPattern := neoDependencyPatternKey(dependency, floorVersion, accessPath)
				if pattern := patterns[patternIndex].(string); pattern != expectedPattern {
					return nil, fmt.Errorf("dependency-floor evidence entry %d pattern must be %q for accessPath %q", i, expectedPattern, accessPath)
				}
				if !rootEvidenceOK || len(rootEvidence) == 0 {
					return nil, fmt.Errorf("dependency-floor evidence entry %d requires rootEvidence as a non-empty array of exact excerpts", i)
				}
				switch floorStatus {
				case "compatible":
					if outcome != "no-finding" {
						return nil, fmt.Errorf("dependency-floor evidence entry %d compatible floorStatus requires no-finding outcome", i)
					}
				case "incompatible", "unverified":
					if outcome != "finding" {
						return nil, fmt.Errorf("dependency-floor evidence entry %d %s floorStatus requires finding outcome", i, floorStatus)
					}
				default:
					return nil, fmt.Errorf("dependency-floor evidence entry %d has invalid floorStatus %q", i, floorStatus)
				}
				switch verification {
				case "root-runtime-traversal", "root-source-construction", "root-type-declaration", "exact-export-inspection", "exact-behavior-test":
				default:
					return nil, fmt.Errorf("dependency-floor evidence entry %d has invalid verification %q", i, verification)
				}
				capabilityKey := dependency + "\x00" + floorVersion + "\x00" + accessPath
				if seenCapabilities[capabilityKey] {
					return nil, fmt.Errorf("dependency-floor evidence entry %d duplicates capability path %q at %s@%s", i, accessPath, dependency, floorVersion)
				}
				rootEvidence = neoNonEmptyRunCheckEvidence(rootEvidence)
				if len(rootEvidence) == 0 {
					return nil, fmt.Errorf("dependency-floor evidence entry %d requires non-empty rootEvidence excerpts", i)
				}
				if boolValue(input[neoRunCheckToolEvidenceRequired]) {
					if err := neoValidateDependencyToolEvidence(input, i, dependency, floorVersion, floorStatus, verification, parsedAccessPath, rootEvidence); err != nil {
						return nil, err
					}
				}
				seenCapabilities[capabilityKey] = true
				normalizedEvidence["dependency"] = dependency
				normalizedEvidence["floorVersion"] = floorVersion
				normalizedEvidence["accessPath"] = accessPath
				normalizedEvidence["floorStatus"] = floorStatus
				normalizedEvidence["verification"] = verification
				normalizedEvidence["rootEvidence"] = stringArrayValue(rootEvidence)
			}
			if checkName == "bounded-artifact-state-transitions" && outcome != "not-applicable" {
				phaseRelationship := strings.TrimSpace(stringValue(entry["phaseRelationship"]))
				budgetOrigin := strings.TrimSpace(stringValue(entry["budgetOrigin"]))
				decisiveSequence := strings.TrimSpace(stringValue(entry["decisiveSequence"]))
				if phaseRelationship == "" || decisiveSequence == "" {
					return nil, fmt.Errorf("bounded-artifact evidence entry %d requires phaseRelationship and decisiveSequence", i)
				}
				switch budgetOrigin {
				case "original", "remaining", "independent":
				default:
					return nil, fmt.Errorf("bounded-artifact evidence entry %d has invalid budgetOrigin %q", i, budgetOrigin)
				}
				normalizedEvidence["phaseRelationship"] = phaseRelationship
				normalizedEvidence["budgetOrigin"] = budgetOrigin
				normalizedEvidence["decisiveSequence"] = decisiveSequence
			}
			if checkName == "generated-artifact-consumer-contract" && outcome == "finding" {
				implementationOwner := strings.TrimSpace(stringValue(entry["implementationOwner"]))
				if implementationOwner == "" {
					return nil, fmt.Errorf("generated-artifact finding evidence entry %d requires implementationOwner", i)
				}
				for _, rawIssueIndex := range issueIndexes {
					issueIndex, _ := neoReviewNumber(rawIssueIndex)
					issue, _ := asMap(issues[issueIndex])
					if stringValue(issue["file"]) != implementationOwner {
						return nil, fmt.Errorf("generated-artifact finding evidence entry %d implementationOwner %q must equal the file of referenced issue %d", i, implementationOwner, issueIndex)
					}
				}
				normalizedEvidence["implementationOwner"] = implementationOwner
			}
			if checkName == "classifier-detector-matrix" && outcome != "not-applicable" {
				sourceLifetime := strings.TrimSpace(stringValue(entry["sourceLifetime"]))
				decisionLifetime := strings.TrimSpace(stringValue(entry["decisionLifetime"]))
				mutationPath := strings.TrimSpace(stringValue(entry["mutationPath"]))
				usePath := strings.TrimSpace(stringValue(entry["usePath"]))
				switch sourceLifetime {
				case "mutable", "immutable", "unknown":
				default:
					return nil, fmt.Errorf("classifier evidence entry %d has invalid sourceLifetime %q", i, sourceLifetime)
				}
				switch decisionLifetime {
				case "per-use", "retained", "unknown":
				default:
					return nil, fmt.Errorf("classifier evidence entry %d has invalid decisionLifetime %q", i, decisionLifetime)
				}
				if mutationPath == "" || usePath == "" {
					return nil, fmt.Errorf("classifier evidence entry %d requires mutationPath and usePath", i)
				}
				normalizedEvidence["sourceLifetime"] = sourceLifetime
				normalizedEvidence["decisionLifetime"] = decisionLifetime
				normalizedEvidence["mutationPath"] = mutationPath
				normalizedEvidence["usePath"] = usePath
			}
			evidence = append(evidence, normalizedEvidence)
		}
		for issueIndex, referenced := range referencedIssues {
			if !referenced {
				return nil, fmt.Errorf("reported issue %d is not referenced by finding evidence", issueIndex)
			}
		}
		out["evidence"] = evidence
	}
	return out, nil
}

func neoNonEmptyRunCheckEvidence(fragments []string) []string {
	nonEmpty := make([]string, 0, len(fragments))
	for _, fragment := range fragments {
		if strings.TrimSpace(fragment) != "" {
			nonEmpty = append(nonEmpty, fragment)
		}
	}
	return nonEmpty
}

func neoValidateDependencyToolEvidence(input map[string]any, entryIndex int, dependency, floorVersion, floorStatus, verification string, accessPath neoDependencyAccessPath, fragments []string) error {
	toolEvidence := arrayValue(input[neoRunCheckToolEvidenceKey])
	if len(toolEvidence) == 0 {
		return fmt.Errorf("dependency-floor evidence entry %d requires exact rootEvidence excerpts from successful tool results", entryIndex)
	}
	edgeOwners := append([]string(nil), accessPath.members[:len(accessPath.members)-1]...)
	for edgeIndex := range edgeOwners {
		for _, fragment := range fragments {
			if declaredOwner := neoRunCheckDeclaredMemberOwner(fragment, edgeOwners[edgeIndex], accessPath.members[edgeIndex+1]); declaredOwner != "" {
				if edgeIndex+1 < len(edgeOwners) {
					edgeOwners[edgeIndex+1] = declaredOwner
				}
				break
			}
		}
	}
	coveredEdges := make([]bool, len(accessPath.members)-1)
	moduleEdgeCovered := accessPath.module == ""
	assertionStatus := map[string]string{
		"compatible":   "AVAILABLE",
		"incompatible": "MISSING",
		"unverified":   "UNVERIFIED",
	}[floorStatus]
	assertion := accessPath.raw + "=" + assertionStatus
	if latestStatus, ok := neoRunCheckLatestDependencyStatus(toolEvidence, dependency, floorVersion, verification, accessPath); ok && latestStatus != assertionStatus {
		return fmt.Errorf("dependency-floor evidence entry %d status assertion %q was superseded by later successful exact-floor output %q", entryIndex, assertion, accessPath.raw+"="+latestStatus)
	}
	assertionFound := false
	absentEdge := -1
	moduleMissing := false
	fragmentPresentEdges := make([][]int, len(fragments))
	for fragmentIndex, fragment := range fragments {
		if !utf8.ValidString(fragment) || len(fragment) > 2048 {
			return fmt.Errorf("dependency-floor evidence entry %d rootEvidence excerpt %d must contain at most 2048 valid UTF-8 bytes", entryIndex, fragmentIndex)
		}
		matched := false
		matchedOutput := ""
		matchedCallText := ""
		unsafeReason := ""
		for _, raw := range toolEvidence {
			evidence := mapValue(raw)
			callText := neoRunCheckToolEvidenceCallText(evidence)
			output, outputMatched := neoRunCheckToolEvidenceOutput(evidence, fragment)
			if outputMatched && neoRunCheckExactFloorEvidenceTarget(callText, dependency, floorVersion, verification) {
				if neoRunCheckEvidenceLine(fragment, assertion) {
					if reason := neoRunCheckUnsafeStatusAssertion(evidence, output, accessPath, assertionStatus, verification); reason != "" {
						unsafeReason = reason
						continue
					}
				}
				if strings.Contains(callText, fragment) {
					continue
				}
				matched = true
				matchedOutput = output
				matchedCallText = callText
				break
			}
		}
		if !matched {
			if unsafeReason != "" {
				return fmt.Errorf("dependency-floor evidence entry %d rootEvidence excerpt %d uses an unsafe status assertion: %s", entryIndex, fragmentIndex, unsafeReason)
			}
			return fmt.Errorf("dependency-floor evidence entry %d rootEvidence excerpt %d was not found verbatim in a successful tool result whose call identifies %s@%s", entryIndex, fragmentIndex, dependency, floorVersion)
		}
		meaningful := false
		for _, status := range []string{"AVAILABLE", "MISSING", "UNVERIFIED"} {
			candidate := accessPath.raw + "=" + status
			if status != assertionStatus && neoRunCheckEvidenceLine(fragment, candidate) {
				return fmt.Errorf("dependency-floor evidence entry %d rootEvidence excerpt %d contradicts floorStatus %q with %q", entryIndex, fragmentIndex, floorStatus, candidate)
			}
		}
		if neoRunCheckEvidenceLine(fragment, assertion) {
			assertionFound = true
			meaningful = true
			if verification == "root-runtime-traversal" || verification == "exact-behavior-test" {
				moduleEdgeCovered = true
				for edgeIndex := range coveredEdges {
					coveredEdges[edgeIndex] = true
				}
			} else if verification == "exact-export-inspection" {
				moduleEdgeCovered = true
				moduleMissing = assertionStatus == "MISSING"
			}
		}
		if accessPath.module != "" && (neoRunCheckModuleExportBinding(fragment, accessPath.module, accessPath.members[0]) ||
			neoRunCheckExactModuleTarget(matchedCallText, accessPath.module) && neoRunCheckDirectModuleExport(fragment, accessPath.members[0])) {
			moduleEdgeCovered = true
			meaningful = true
		}
		for edgeIndex := 0; edgeIndex < len(accessPath.members)-1; edgeIndex++ {
			allowMethod := edgeIndex == len(accessPath.members)-2
			memberPresent, memberAbsent := neoRunCheckScopedMemberDeclaration(fragment, edgeOwners[edgeIndex], accessPath.members[edgeIndex+1], allowMethod)
			if verification == "root-source-construction" {
				if scopedPresent, scopedAbsent, scoped := neoRunCheckMatchedPythonConstruction(matchedOutput, fragment, edgeOwners[edgeIndex], accessPath.members[edgeIndex+1]); scoped {
					memberPresent, memberAbsent = scopedPresent, scopedAbsent
				}
			}
			if memberPresent {
				coveredEdges[edgeIndex] = true
				fragmentPresentEdges[fragmentIndex] = append(fragmentPresentEdges[fragmentIndex], edgeIndex)
				meaningful = true
			}
			if floorStatus == "incompatible" && memberAbsent && (absentEdge < 0 || edgeIndex < absentEdge) {
				absentEdge = edgeIndex
				meaningful = true
			}
		}
		if !meaningful {
			expected := make([]string, 0, len(coveredEdges)+2)
			if accessPath.module != "" {
				expected = append(expected, fmt.Sprintf("complete owner scope for module %q -> export %q", accessPath.module, accessPath.members[0]))
			}
			for edgeIndex := 0; edgeIndex < len(accessPath.members)-1; edgeIndex++ {
				expected = append(expected, fmt.Sprintf("complete owner scope for %q -> %q", accessPath.members[edgeIndex], accessPath.members[edgeIndex+1]))
			}
			expected = append(expected, fmt.Sprintf("exact assertion %q", assertion))
			return fmt.Errorf("dependency-floor evidence entry %d rootEvidence excerpt %d does not prove any edge or exact status for accessPath %q; replace it with %s", entryIndex, fragmentIndex, accessPath.raw, strings.Join(expected, ", "))
		}
	}
	if floorStatus != "compatible" && !assertionFound {
		return fmt.Errorf("dependency-floor evidence entry %d with %s floorStatus requires exact successful-tool assertion %q", entryIndex, floorStatus, assertion)
	}
	if floorStatus == "incompatible" && (verification == "root-source-construction" || verification == "root-type-declaration") && absentEdge < 0 {
		return fmt.Errorf("dependency-floor evidence entry %d requires a complete exact owner declaration or construction excerpt proving which accessPath edge is missing", entryIndex)
	}
	if absentEdge >= 0 {
		for fragmentIndex, presentEdges := range fragmentPresentEdges {
			for _, edgeIndex := range presentEdges {
				if edgeIndex >= absentEdge {
					return fmt.Errorf("dependency-floor evidence entry %d rootEvidence excerpt %d proves an irrelevant later edge after accessPath edge %q -> %q is already missing", entryIndex, fragmentIndex, accessPath.members[absentEdge], accessPath.members[absentEdge+1])
				}
			}
		}
	}
	if !moduleEdgeCovered {
		return fmt.Errorf("dependency-floor evidence entry %d rootEvidence does not connect module %q to export %q", entryIndex, accessPath.module, accessPath.members[0])
	}
	if moduleMissing {
		return nil
	}
	for edgeIndex, covered := range coveredEdges {
		if absentEdge >= 0 && edgeIndex >= absentEdge {
			break
		}
		if !covered {
			return fmt.Errorf("dependency-floor evidence entry %d rootEvidence does not connect accessPath segments %q and %q in one exact excerpt", entryIndex, accessPath.members[edgeIndex], accessPath.members[edgeIndex+1])
		}
	}
	return nil
}

func neoRunCheckLatestDependencyStatus(toolEvidence []any, dependency, floorVersion, verification string, accessPath neoDependencyAccessPath) (string, bool) {
	latest := ""
	for _, raw := range toolEvidence {
		evidence := mapValue(raw)
		callText := neoRunCheckToolEvidenceCallText(evidence)
		if !neoRunCheckExactFloorEvidenceTarget(callText, dependency, floorVersion, verification) {
			continue
		}
		for _, output := range neoStringSlice(evidence["outputs"]) {
			for _, line := range strings.Split(strings.ReplaceAll(output, "\r\n", "\n"), "\n") {
				line = strings.TrimSpace(line)
				if strings.Contains(callText, line) {
					continue
				}
				for _, status := range []string{"AVAILABLE", "MISSING", "UNVERIFIED"} {
					if line != accessPath.raw+"="+status || neoRunCheckUnsafeStatusAssertion(evidence, output, accessPath, status, verification) != "" {
						continue
					}
					latest = status
					break
				}
			}
		}
	}
	return latest, latest != ""
}

func neoRunCheckToolEvidenceOutput(evidence map[string]any, fragment string) (string, bool) {
	outputs := neoStringSlice(evidence["outputs"])
	for index := len(outputs) - 1; index >= 0; index-- {
		output := outputs[index]
		if strings.Contains(output, fragment) {
			return output, true
		}
	}
	return "", false
}

func neoParseDependencyAccessPath(value string) (neoDependencyAccessPath, error) {
	if value == "" || len(value) > 512 || !utf8.ValidString(value) || strings.IndexFunc(value, func(r rune) bool { return r <= ' ' || r == 0x7f }) >= 0 {
		return neoDependencyAccessPath{}, fmt.Errorf("%q must be valid UTF-8 without whitespace or control characters and at most 512 bytes", value)
	}
	parsed := neoDependencyAccessPath{raw: value}
	memberChain := value
	if hash := strings.IndexByte(value, '#'); hash >= 0 {
		if hash == 0 || hash == len(value)-1 || strings.Contains(value[hash+1:], "#") {
			return neoDependencyAccessPath{}, fmt.Errorf("%q must use module-specifier#Export.member for an imported owner", value)
		}
		parsed.module = value[:hash]
		memberChain = value[hash+1:]
	}
	parsed.members = strings.Split(memberChain, ".")
	if len(parsed.members) < 2 {
		return neoDependencyAccessPath{}, fmt.Errorf("%q must include an owner and terminal capability", value)
	}
	for _, member := range parsed.members {
		if !neoDependencyIdentifier(member) {
			return neoDependencyAccessPath{}, fmt.Errorf("%q contains invalid owner or capability segment %q", value, member)
		}
	}
	return parsed, nil
}

func neoDependencyPatternKey(dependency, floorVersion, accessPath string) string {
	return dependency + "@" + floorVersion + " " + accessPath
}

func neoDependencyIdentifier(value string) bool {
	if value == "" || !neoDependencyIdentifierStart(value[0]) {
		return false
	}
	for index := 1; index < len(value); index++ {
		if !neoDependencyIdentifierStart(value[index]) && (value[index] < '0' || value[index] > '9') {
			return false
		}
	}
	return true
}

func neoDependencyIdentifierStart(value byte) bool {
	return value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value == '_' || value == '$'
}

func neoRunCheckEvidenceTerm(text, term string) bool {
	text = strings.ToLower(text)
	term = strings.ToLower(strings.TrimSpace(term))
	if text == "" || term == "" {
		return false
	}
	for offset := 0; offset <= len(text)-len(term); {
		index := strings.Index(text[offset:], term)
		if index < 0 {
			return false
		}
		index += offset
		beforeOK := index == 0 || !neoRunCheckEvidenceIdentifierByte(text[index-1])
		after := index + len(term)
		afterOK := after == len(text) || !neoRunCheckEvidenceIdentifierByte(text[after])
		if beforeOK && afterOK {
			return true
		}
		offset = index + 1
	}
	return false
}

func neoRunCheckExactIdentifierTerm(text, term string) bool {
	term = strings.TrimSpace(term)
	if text == "" || term == "" {
		return false
	}
	for offset := 0; offset <= len(text)-len(term); {
		index := strings.Index(text[offset:], term)
		if index < 0 {
			return false
		}
		index += offset
		beforeOK := index == 0 || !neoRunCheckEvidenceIdentifierByte(text[index-1])
		after := index + len(term)
		afterOK := after == len(text) || !neoRunCheckEvidenceIdentifierByte(text[after])
		if beforeOK && afterOK {
			return true
		}
		offset = index + 1
	}
	return false
}

func neoRunCheckExactModuleTarget(text, module string) bool {
	module = strings.TrimSpace(module)
	if text == "" || module == "" {
		return false
	}
	for offset := 0; offset <= len(text)-len(module); {
		index := strings.Index(text[offset:], module)
		if index < 0 {
			return false
		}
		index += offset
		beforeOK := index == 0 || !neoDependencyEvidenceByte(text[index-1])
		after := index + len(module)
		afterOK := after == len(text) || !neoDependencyEvidenceByte(text[after])
		if beforeOK && afterOK {
			return true
		}
		offset = index + 1
	}
	return false
}

func neoRunCheckEvidenceIdentifierByte(value byte) bool {
	return value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value >= '0' && value <= '9' || value == '_' || value == '$'
}

func neoRunCheckEvidenceDelimitedTerm(text, term string, tokenByte func(byte) bool) bool {
	text = strings.ToLower(text)
	term = strings.ToLower(strings.TrimSpace(term))
	for offset := 0; term != "" && offset <= len(text)-len(term); {
		index := strings.Index(text[offset:], term)
		if index < 0 {
			return false
		}
		index += offset
		beforeOK := index == 0 || !tokenByte(text[index-1])
		after := index + len(term)
		if beforeOK && (after == len(text) || !tokenByte(text[after])) {
			return true
		}
		offset = index + 1
	}
	return false
}

func neoRunCheckDependencyTerm(text, term string) bool {
	text = strings.ToLower(text)
	term = strings.ToLower(strings.TrimSpace(term))
	for offset := 0; term != "" && offset <= len(text)-len(term); {
		index := strings.Index(text[offset:], term)
		if index < 0 {
			return false
		}
		index += offset
		beforeOK := index == 0 || !neoDependencyEvidenceByte(text[index-1])
		after := index + len(term)
		afterOK := after == len(text) || !neoDependencyEvidenceByte(text[after]) || text[after] == '@'
		if beforeOK && afterOK {
			return true
		}
		offset = index + 1
	}
	return false
}

func neoRunCheckExactFloorTarget(text, dependency, floorVersion string) bool {
	return neoRunCheckExactFloorTargetWithCommands(text, dependency, floorVersion, [][]string{
		{"npm", "pack"}, {"npm", "view"}, {"pnpm", "pack"}, {"pip", "download"}, {"python", "-m", "pip", "download"},
		{"python3", "-m", "pip", "download"}, {"inspect"}, {"inspect-literal"}, {"load-exact-floor"}, {"resolve.exports"}, {"resolve-exports"},
	})
}

func neoRunCheckExactFloorTargetWithCommands(text, dependency, floorVersion string, commands [][]string) bool {
	dependency = strings.ToLower(strings.TrimSpace(dependency))
	floorVersion = strings.ToLower(strings.TrimSpace(floorVersion))
	for _, source := range neoRunCheckExecutableSources(text) {
		for _, separator := range []string{"@", "=="} {
			target := dependency + separator + floorVersion
			for _, command := range neoRunCheckShellStatements(source) {
				if neoRunCheckCommandTargetsFloorWithCommands(command, target, commands) {
					return true
				}
			}
		}
	}
	return false
}

func neoRunCheckExactFloorEvidenceTarget(text, dependency, floorVersion, verification string) bool {
	switch verification {
	case "root-runtime-traversal", "exact-behavior-test":
		return neoRunCheckExactFloorTargetWithCommands(text, dependency, floorVersion, [][]string{{"load-exact-floor"}})
	case "exact-export-inspection":
		return neoRunCheckExactFloorTargetWithCommands(text, dependency, floorVersion, [][]string{{"resolve.exports"}, {"resolve-exports"}, {"load-exact-floor"}})
	}
	executableSources := neoRunCheckExecutableSources(text)
	shellText := ""
	if len(executableSources) > 0 {
		shellText = executableSources[0]
	}
	safeArchiveWorkflow := neoRunCheckShellExitsOnError(shellText) && !neoRunCheckShellDisablesErrexit(shellText) && !neoRunCheckShellHasOrOperator(shellText) && !neoRunCheckShellHasBackgroundOperator(shellText) &&
		(!neoRunCheckShellHasPipeline(shellText) || neoRunCheckShellEnablesPipefail(shellText))
	dependency = strings.ToLower(strings.TrimSpace(dependency))
	floorVersion = strings.ToLower(strings.TrimSpace(floorVersion))
	statements := neoRunCheckShellStatements(shellText)
	for _, separator := range []string{"@", "=="} {
		target := dependency + separator + floorVersion
		if neoRunCheckBoundInspectTarget(shellText, target) {
			return true
		}
		var artifact neoRunCheckFloorArtifact
		for _, words := range statements {
			if neoRunCheckCommandTargetsFloor(words, target) {
				artifact = neoRunCheckExpectedFloorArtifact(words, dependency, floorVersion)
				continue
			}
			if artifact.kind == "" || len(words) == 0 {
				continue
			}
			switch strings.ToLower(words[0]) {
			case "tar", "bsdtar", "unzip":
				artifact.root = neoRunCheckExtractedFloorRoot(words, artifact)
			case "cat", "sed", "awk", "grep", "rg", "head", "tail":
				if safeArchiveWorkflow && artifact.root != "" && neoRunCheckCommandReadsFloorRoot(words, artifact.root) {
					return true
				}
			}
		}
	}
	return false
}

func neoRunCheckBoundInspectTarget(text, target string) bool {
	var statement []string
	for _, source := range neoRunCheckExecutableSources(text) {
		for _, words := range neoRunCheckShellStatements(source) {
			if statement != nil {
				return false
			}
			statement = words
		}
	}
	return len(statement) > 1 && (strings.EqualFold(statement[0], "inspect") || strings.EqualFold(statement[0], "inspect-literal")) && strings.EqualFold(statement[1], target)
}

type neoRunCheckFloorArtifact struct {
	kind       string
	dependency string
	version    string
	root       string
}

func neoRunCheckExpectedFloorArtifact(words []string, dependency, floorVersion string) neoRunCheckFloorArtifact {
	if len(words) >= 3 && (strings.EqualFold(words[0], "npm") || strings.EqualFold(words[0], "pnpm")) && strings.EqualFold(words[1], "pack") {
		return neoRunCheckFloorArtifact{kind: "npm", dependency: dependency, version: floorVersion}
	}
	for _, prefix := range [][]string{{"pip", "download"}, {"python", "-m", "pip", "download"}, {"python3", "-m", "pip", "download"}} {
		if len(words) <= len(prefix) {
			continue
		}
		matched := true
		for index, part := range prefix {
			if !strings.EqualFold(words[index], part) {
				matched = false
				break
			}
		}
		if matched {
			return neoRunCheckFloorArtifact{kind: "python", dependency: dependency, version: floorVersion}
		}
	}
	return neoRunCheckFloorArtifact{}
}

func neoRunCheckExtractedFloorRoot(words []string, artifact neoRunCheckFloorArtifact) string {
	archive := ""
	destination := "."
	for index := 1; index < len(words); index++ {
		word := strings.TrimSpace(words[index])
		if neoRunCheckFloorArchive(artifact, filepath.Base(word)) {
			archive = filepath.Base(word)
			continue
		}
		unzip := strings.EqualFold(words[0], "unzip")
		if (unzip && word == "-d" || !unzip && word == "-C") && index+1 < len(words) {
			destination = filepath.Clean(words[index+1])
			index++
		}
	}
	if archive == "" {
		return ""
	}
	if artifact.kind == "npm" {
		return filepath.Join(destination, "package")
	}
	if strings.HasSuffix(strings.ToLower(archive), ".whl") {
		return destination
	}
	return filepath.Join(destination, neoRunCheckArchiveStem(archive))
}

func neoRunCheckFloorArchive(artifact neoRunCheckFloorArtifact, archive string) bool {
	archive = strings.ToLower(strings.TrimSpace(archive))
	name := strings.ToLower(strings.TrimPrefix(artifact.dependency, "@"))
	name = strings.ReplaceAll(name, "/", "-")
	if artifact.kind == "npm" {
		name = strings.ReplaceAll(name, "_", "-")
		return archive == name+"-"+artifact.version+".tgz"
	}
	for _, candidate := range []string{
		strings.NewReplacer("-", "_", ".", "_").Replace(name),
		strings.NewReplacer("_", "-", ".", "-").Replace(name),
	} {
		prefix := candidate + "-" + artifact.version
		if archive == prefix+".tar.gz" || archive == prefix+".tgz" || archive == prefix+".zip" || strings.HasPrefix(archive, prefix+"-") && strings.HasSuffix(archive, ".whl") {
			return true
		}
	}
	return false
}

func neoRunCheckArchiveStem(archive string) string {
	lower := strings.ToLower(archive)
	for _, suffix := range []string{".tar.gz", ".tgz", ".zip"} {
		if strings.HasSuffix(lower, suffix) {
			return archive[:len(archive)-len(suffix)]
		}
	}
	return archive
}

func neoRunCheckCommandReadsFloorRoot(words []string, root string) bool {
	root = filepath.Clean(root)
	for _, word := range words[1:] {
		candidate := filepath.Clean(strings.TrimSpace(word))
		if candidate == root || strings.HasPrefix(candidate, root+string(filepath.Separator)) {
			return true
		}
	}
	return false
}

func neoRunCheckCommandTargetsFloor(words []string, target string) bool {
	return neoRunCheckCommandTargetsFloorWithCommands(words, target, [][]string{
		{"npm", "pack"}, {"npm", "view"}, {"pnpm", "pack"}, {"pip", "download"}, {"python", "-m", "pip", "download"},
		{"python3", "-m", "pip", "download"}, {"inspect"}, {"inspect-literal"}, {"load-exact-floor"}, {"resolve.exports"}, {"resolve-exports"},
	})
}

func neoRunCheckCommandTargetsFloorWithCommands(words []string, target string, commands [][]string) bool {
	for _, command := range commands {
		if len(words) <= len(command) {
			continue
		}
		matched := true
		for index, commandWord := range command {
			if !strings.EqualFold(words[index], commandWord) {
				matched = false
				break
			}
		}
		if !matched {
			continue
		}
		for index := len(command); index < len(words); index++ {
			word := strings.ToLower(words[index])
			if word == target {
				return true
			}
			if !strings.HasPrefix(word, "-") {
				continue
			}
			if strings.Contains(word, "=") || neoRunCheckFloorBooleanOption(word) {
				continue
			}
			if !neoRunCheckFloorValueOption(word) || index+1 >= len(words) {
				break
			}
			index++
		}
	}
	return false
}

func neoRunCheckFloorBooleanOption(option string) bool {
	switch option {
	case "--json", "--dry-run", "--ignore-scripts", "--no-deps", "--no-binary", "--only-binary", "--pre", "--no-build-isolation":
		return true
	default:
		return false
	}
}

func neoRunCheckFloorValueOption(option string) bool {
	switch option {
	case "--pack-destination", "-d", "--dest", "--platform", "--python-version", "--implementation", "--abi", "--index-url", "--extra-index-url", "--find-links", "--proxy", "--timeout", "--retries", "--cache-dir", "--cert", "--client-cert", "--progress-bar", "--config-settings":
		return true
	default:
		return false
	}
}

func neoRunCheckShellStatements(text string) [][]string {
	statements := make([][]string, 0)
	words := make([]string, 0)
	var word strings.Builder
	flushWord := func() {
		if word.Len() > 0 {
			words = append(words, word.String())
			word.Reset()
		}
	}
	flushStatement := func() {
		flushWord()
		if len(words) > 0 {
			statements = append(statements, words)
			words = nil
		}
	}
	var quote byte
	escaped := false
	comment := false
	for index := 0; index < len(text); index++ {
		current := text[index]
		if comment {
			if current == '\n' {
				comment = false
				flushStatement()
			}
			continue
		}
		if escaped {
			word.WriteByte(current)
			escaped = false
			continue
		}
		if quote != 0 {
			if current == '\\' && quote == '"' {
				escaped = true
				continue
			}
			if current == quote {
				quote = 0
				continue
			}
			word.WriteByte(current)
			continue
		}
		switch current {
		case '\\':
			escaped = true
		case '\'', '"':
			quote = current
		case ' ', '\t', '\r':
			flushWord()
		case '\n', ';', '|', '&', '(', ')':
			flushStatement()
			if (current == '|' || current == '&') && index+1 < len(text) && text[index+1] == current {
				index++
			}
		case '#':
			if word.Len() == 0 {
				comment = true
			} else {
				word.WriteByte(current)
			}
		default:
			word.WriteByte(current)
		}
	}
	flushStatement()
	return statements
}

func neoDependencyEvidenceByte(value byte) bool {
	return neoRunCheckEvidenceIdentifierByte(value) || strings.ContainsRune("@/.-+", rune(value))
}

func neoVersionEvidenceByte(value byte) bool {
	return neoRunCheckEvidenceIdentifierByte(value) || strings.ContainsRune(".-+", rune(value))
}

func neoRunCheckEvidenceLine(text, line string) bool {
	for _, candidate := range strings.Split(strings.ReplaceAll(text, "\r\n", "\n"), "\n") {
		if strings.TrimSpace(candidate) == line {
			return true
		}
	}
	return false
}

func neoRunCheckModuleExportBinding(fragment, module, exportName string) bool {
	mask := neoRunCheckDeclarationLexicalMask(fragment)
	pattern := regexp.MustCompile(`(?s)\bexport\s+(?:type\s+)?\{([^}]*)\}\s*from\s*["']` + regexp.QuoteMeta(module) + `["']`)
	for _, match := range pattern.FindAllStringSubmatchIndex(fragment, -1) {
		if match[0] < 0 || match[1] > len(mask) || !neoRunCheckEvidenceTerm(mask[match[0]:match[1]], "export") {
			continue
		}
		for _, binding := range strings.Split(fragment[match[2]:match[3]], ",") {
			fields := strings.Fields(strings.TrimSpace(binding))
			if len(fields) > 0 && fields[0] == "type" {
				fields = fields[1:]
			}
			boundName := ""
			if len(fields) == 1 {
				boundName = fields[0]
			} else if len(fields) == 3 && fields[1] == "as" {
				boundName = fields[2]
			}
			if boundName == exportName {
				return true
			}
		}
	}
	return false
}

func neoRunCheckDirectModuleExport(fragment, exportName string) bool {
	mask := neoRunCheckDeclarationLexicalMask(fragment)
	declaration := regexp.MustCompile(`\bexport\s+(?:declare\s+)?(?:abstract\s+)?(?:class|interface|type|enum|function|namespace|const|let|var)\s+` + regexp.QuoteMeta(exportName) + `\b`)
	if declaration.MatchString(mask) {
		return true
	}
	bindings := regexp.MustCompile(`(?s)\bexport\s+(?:type\s+)?\{([^}]*)\}\s*;?`).FindAllStringSubmatch(mask, -1)
	for _, bindingList := range bindings {
		if len(bindingList) != 2 {
			continue
		}
		for _, binding := range strings.Split(bindingList[1], ",") {
			fields := strings.Fields(strings.TrimSpace(binding))
			if len(fields) > 0 && fields[0] == "type" {
				fields = fields[1:]
			}
			if len(fields) == 1 && fields[0] == exportName || len(fields) == 3 && fields[1] == "as" && fields[2] == exportName {
				return true
			}
		}
	}
	return false
}

func neoRunCheckScopedMember(fragment, owner, member string) (bool, bool) {
	return neoRunCheckScopedMemberDeclaration(fragment, owner, member, true)
}

func neoRunCheckDeclaredMemberOwner(fragment, owner, member string) string {
	masked := neoRunCheckDeclarationLexicalMask(fragment)
	for _, scope := range neoRunCheckOwnerScopes(masked, owner) {
		body := scope
		if open := strings.IndexByte(scope, '{'); open >= 0 {
			body = scope[open+1:]
		} else if lineEnd := strings.IndexByte(scope, '\n'); lineEnd >= 0 {
			body = scope[lineEnd+1:]
		}
		declaration := regexp.MustCompile(`(?:^|[;\n{}])\s*(?:(?:public|private|protected|readonly|static|abstract|declare|override|get)\s+)*` + regexp.QuoteMeta(member) + `\s*[?!]?\s*(?:\(\s*\))?\s*:\s*([A-Za-z_$][A-Za-z0-9_$]*)`)
		if match := declaration.FindStringSubmatch(body); len(match) == 2 {
			return match[1]
		}
	}
	for _, scope := range neoRunCheckPythonOwnerScopes(fragment, owner) {
		if mapBody, ok := neoRunCheckPythonSubSDKMap(scope); ok {
			mapping := regexp.MustCompile(`["']` + regexp.QuoteMeta(member) + `["']\s*:\s*([A-Za-z_$][A-Za-z0-9_$]*)`)
			if match := mapping.FindStringSubmatch(mapBody); len(match) == 2 {
				return match[1]
			}
		}
	}
	return ""
}

func neoRunCheckScopedMemberDeclaration(fragment, owner, member string, allowMethod bool) (bool, bool) {
	scopes := neoRunCheckOwnerScopes(neoRunCheckDeclarationLexicalMask(fragment), owner)
	if len(scopes) == 0 {
		return false, false
	}
	ambiguous := false
	for _, scope := range scopes {
		body := scope
		if open := strings.IndexByte(scope, '{'); open >= 0 {
			body = scope[open+1:]
		} else if lineEnd := strings.IndexByte(scope, '\n'); lineEnd >= 0 {
			body = scope[lineEnd+1:]
		}
		if neoRunCheckDirectMemberDeclaration(body, member, allowMethod) {
			return true, false
		}
		ambiguous = ambiguous || neoRunCheckExactIdentifierTerm(body, member)
	}
	for _, scope := range scopes {
		if neoRunCheckScopeHasInheritance(scope, owner) {
			return false, false
		}
	}
	if ambiguous {
		return false, false
	}
	return false, true
}

func neoRunCheckDirectMemberDeclaration(body, member string, allowMethod bool) bool {
	folded := body
	member = strings.TrimSpace(member)
	braceDepth, parenDepth, bracketDepth := 0, 0, 0
	for offset := 0; member != "" && offset <= len(folded)-len(member); offset++ {
		switch folded[offset] {
		case '{':
			braceDepth++
			continue
		case '}':
			if braceDepth > 0 {
				braceDepth--
			}
			continue
		case '(':
			parenDepth++
			continue
		case ')':
			if parenDepth > 0 {
				parenDepth--
			}
			continue
		case '[':
			bracketDepth++
			continue
		case ']':
			if bracketDepth > 0 {
				bracketDepth--
			}
			continue
		}
		if braceDepth != 0 || parenDepth != 0 || bracketDepth != 0 || !strings.HasPrefix(folded[offset:], member) {
			continue
		}
		beforeOK := offset == 0 || !neoRunCheckEvidenceIdentifierByte(folded[offset-1])
		after := offset + len(member)
		if !beforeOK || after < len(folded) && neoRunCheckEvidenceIdentifierByte(folded[after]) {
			continue
		}
		prefixStart := strings.LastIndexAny(folded[:offset], "\n;{}") + 1
		prefix := strings.TrimSpace(folded[prefixStart:offset])
		if !neoRunCheckMemberDeclarationPrefix(prefix) {
			continue
		}
		for after < len(folded) && (folded[after] == ' ' || folded[after] == '\t') {
			after++
		}
		if after < len(folded) && (folded[after] == '?' || folded[after] == '!') {
			after++
			for after < len(folded) && (folded[after] == ' ' || folded[after] == '\t') {
				after++
			}
		}
		if after == len(folded) || strings.ContainsRune(":=;", rune(folded[after])) || (allowMethod || neoRunCheckGetterDeclarationPrefix(prefix)) && folded[after] == '(' {
			return true
		}
	}
	return false
}

func neoRunCheckGetterDeclarationPrefix(prefix string) bool {
	for _, field := range strings.Fields(prefix) {
		if field == "get" {
			return true
		}
	}
	return false
}

func neoRunCheckMemberDeclarationPrefix(prefix string) bool {
	if prefix == "" {
		return true
	}
	allowed := map[string]bool{
		"abstract": true, "async": true, "declare": true, "def": true, "get": true, "override": true,
		"private": true, "protected": true, "public": true, "readonly": true, "set": true, "static": true,
	}
	for _, field := range strings.Fields(prefix) {
		if !allowed[field] {
			return false
		}
	}
	return true
}

func neoRunCheckDeclarationLexicalMask(fragment string) string {
	masked := []byte(fragment)
	for offset := 0; offset < len(masked); {
		switch {
		case offset+1 < len(masked) && masked[offset] == '/' && masked[offset+1] == '/':
			for offset < len(masked) && masked[offset] != '\n' {
				masked[offset] = ' '
				offset++
			}
		case offset+1 < len(masked) && masked[offset] == '/' && masked[offset+1] == '*':
			masked[offset], masked[offset+1] = ' ', ' '
			offset += 2
			for offset < len(masked) {
				if offset+1 < len(masked) && masked[offset] == '*' && masked[offset+1] == '/' {
					masked[offset], masked[offset+1] = ' ', ' '
					offset += 2
					break
				}
				if masked[offset] != '\n' {
					masked[offset] = ' '
				}
				offset++
			}
		case masked[offset] == '#' && (offset == 0 || masked[offset-1] == ' ' || masked[offset-1] == '\t' || masked[offset-1] == '\n'):
			for offset < len(masked) && masked[offset] != '\n' {
				masked[offset] = ' '
				offset++
			}
		case masked[offset] == '\'' || masked[offset] == '"' || masked[offset] == '`':
			quote := masked[offset]
			width := 1
			if quote != '`' && offset+2 < len(masked) && masked[offset+1] == quote && masked[offset+2] == quote {
				width = 3
			}
			for index := 0; index < width; index++ {
				masked[offset+index] = ' '
			}
			offset += width
			escaped := false
			for offset < len(masked) {
				if !escaped && width == 3 && offset+2 < len(masked) && masked[offset] == quote && masked[offset+1] == quote && masked[offset+2] == quote {
					masked[offset], masked[offset+1], masked[offset+2] = ' ', ' ', ' '
					offset += 3
					break
				}
				current := masked[offset]
				if !escaped && width == 1 && current == quote {
					masked[offset] = ' '
					offset++
					break
				}
				if current != '\n' {
					masked[offset] = ' '
				}
				if escaped {
					escaped = false
				} else if current == '\\' && quote != '\'' {
					escaped = true
				}
				offset++
			}
		default:
			offset++
		}
	}
	return string(masked)
}

func neoRunCheckScopeHasInheritance(scope, owner string) bool {
	header := scope
	if lineEnd := strings.IndexByte(header, '\n'); lineEnd >= 0 {
		header = header[:lineEnd]
	}
	if brace := strings.IndexByte(header, '{'); brace >= 0 {
		header = header[:brace]
	}
	folded := neoRunCheckASCIIFold(header)
	if neoRunCheckEvidenceTerm(folded, "extends") {
		return true
	}
	classPrefix := "class "
	trimmed := strings.TrimSpace(header)
	foldedTrimmed := neoRunCheckASCIIFold(trimmed)
	if !strings.HasPrefix(foldedTrimmed, classPrefix) {
		return false
	}
	declaration := strings.TrimSpace(trimmed[len(classPrefix):])
	if len(declaration) < len(owner) || !strings.EqualFold(declaration[:len(owner)], owner) {
		return false
	}
	remainder := strings.TrimSpace(declaration[len(owner):])
	if !strings.HasPrefix(remainder, "(") {
		return false
	}
	close := strings.IndexByte(remainder, ')')
	return close > 1 && strings.TrimSpace(remainder[1:close]) != ""
}

func neoRunCheckMatchedPythonConstruction(_ string, fragment, owner, member string) (bool, bool, bool) {
	if !strings.Contains(fragment, "_sub_sdk_map") {
		return false, false, false
	}
	for _, scope := range neoRunCheckPythonOwnerScopes(fragment, owner) {
		if mapBody, ok := neoRunCheckPythonSubSDKMap(scope); ok {
			present := neoRunCheckExactIdentifierTerm(mapBody, member)
			return present, !present, true
		}
	}
	return false, false, false
}

func neoRunCheckPythonSubSDKMap(scope string) (string, bool) {
	masked := neoRunCheckDeclarationLexicalMask(scope)
	mapIndex := strings.Index(masked, "_sub_sdk_map")
	if mapIndex < 0 {
		return "", false
	}
	openOffset := strings.IndexByte(masked[mapIndex:], '{')
	if openOffset < 0 {
		return "", false
	}
	open := mapIndex + openOffset
	close := neoRunCheckClosingBrace(masked, open)
	if close <= open {
		return "", false
	}
	return scope[open : close+1], true
}

func neoRunCheckBalancedBraces(text string) bool {
	open := strings.IndexByte(text, '{')
	return open >= 0 && neoRunCheckClosingBrace(text, open) == len(strings.TrimSpace(text))-1
}

func neoRunCheckOwnerScopes(fragment, owner string) []string {
	scopes := make([]string, 0)
	lower := neoRunCheckASCIIFold(fragment)
	for _, keyword := range []string{"class", "interface"} {
		for offset := 0; offset < len(lower); {
			index := strings.Index(lower[offset:], keyword)
			if index < 0 {
				break
			}
			index += offset
			nameStart := index + len(keyword)
			if index > 0 && neoRunCheckEvidenceIdentifierByte(lower[index-1]) || nameStart >= len(fragment) || fragment[nameStart] != ' ' && fragment[nameStart] != '\t' && fragment[nameStart] != '\n' && fragment[nameStart] != '\r' {
				offset = index + 1
				continue
			}
			for nameStart < len(fragment) && (fragment[nameStart] == ' ' || fragment[nameStart] == '\t') {
				nameStart++
			}
			nameEnd := nameStart
			for nameEnd < len(fragment) && neoRunCheckEvidenceIdentifierByte(lower[nameEnd]) {
				nameEnd++
			}
			if fragment[nameStart:nameEnd] != owner {
				offset = index + 1
				continue
			}
			braceOffset := strings.IndexByte(fragment[nameEnd:], '{')
			if braceOffset < 0 {
				offset = nameEnd
				continue
			}
			open := nameEnd + braceOffset
			if close := neoRunCheckClosingBrace(fragment, open); close > open {
				scopes = append(scopes, fragment[index:close+1])
				offset = close + 1
				continue
			}
			offset = open + 1
		}
	}
	lines := strings.Split(strings.ReplaceAll(fragment, "\r\n", "\n"), "\n")
	for index, line := range lines {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(strings.ToLower(trimmed), "class ") {
			continue
		}
		name := strings.TrimSpace(trimmed[len("class "):])
		if stop := strings.IndexAny(name, "(: \t"); stop >= 0 {
			name = name[:stop]
		}
		if name != owner {
			continue
		}
		indent := len(line) - len(strings.TrimLeft(line, " \t"))
		end := index + 1
		for end < len(lines) {
			if strings.TrimSpace(lines[end]) == "" {
				end++
				continue
			}
			bodyIndent := len(lines[end]) - len(strings.TrimLeft(lines[end], " \t"))
			if bodyIndent <= indent {
				break
			}
			end++
		}
		scopes = append(scopes, strings.Join(lines[index:end], "\n"))
	}
	return scopes
}

func neoRunCheckPythonOwnerScopes(fragment, owner string) []string {
	fragment = strings.ReplaceAll(strings.ReplaceAll(fragment, "\r\n", "\n"), "\r", "\n")
	originalLines := strings.Split(fragment, "\n")
	maskedLines := strings.Split(neoRunCheckDeclarationLexicalMask(fragment), "\n")
	scopes := make([]string, 0)
	for index, maskedLine := range maskedLines {
		trimmed := strings.TrimSpace(maskedLine)
		if !strings.HasPrefix(strings.ToLower(trimmed), "class ") {
			continue
		}
		name := strings.TrimSpace(trimmed[len("class "):])
		if stop := strings.IndexAny(name, "(: \t"); stop >= 0 {
			name = name[:stop]
		}
		if name != owner {
			continue
		}
		indent := len(maskedLine) - len(strings.TrimLeft(maskedLine, " \t"))
		end := index + 1
		for end < len(maskedLines) {
			if strings.TrimSpace(maskedLines[end]) == "" {
				end++
				continue
			}
			bodyIndent := len(maskedLines[end]) - len(strings.TrimLeft(maskedLines[end], " \t"))
			if bodyIndent <= indent {
				break
			}
			end++
		}
		scopes = append(scopes, strings.Join(originalLines[index:end], "\n"))
	}
	return scopes
}

func neoRunCheckASCIIFold(value string) string {
	folded := []byte(value)
	for index, current := range folded {
		if current >= 'A' && current <= 'Z' {
			folded[index] = current + ('a' - 'A')
		}
	}
	return string(folded)
}

func neoRunCheckUnsafeStatusAssertion(evidence map[string]any, output string, accessPath neoDependencyAccessPath, status, verification string) string {
	callText := neoRunCheckToolEvidenceCallText(evidence)
	if status != "UNVERIFIED" && neoRunCheckHasCaughtFailure(callText, accessPath) {
		return "caught load, import, dependency, extraction, or harness failures must produce UNVERIFIED or a nonzero result, never AVAILABLE or MISSING"
	}
	lower := strings.ToLower(neoRunCheckExecutableText(callText))
	if status != "UNVERIFIED" && neoRunCheckShellTool(stringValue(evidence["tool"])) {
		if neoRunCheckShellDisablesErrexit(callText) || neoRunCheckShellHasOrOperator(callText) {
			return "shell failure suppression cannot support an AVAILABLE or MISSING assertion"
		}
		if neoRunCheckShellHasBackgroundOperator(callText) {
			return "background shell commands cannot support an AVAILABLE or MISSING assertion"
		}
		if neoRunCheckShellHasAndOperator(callText) && !neoRunCheckShellExitsOnError(callText) {
			return "shell && chains must enable exit-on-error before emitting AVAILABLE or MISSING"
		}
		if neoRunCheckShellHasPipeline(callText) && !neoRunCheckShellEnablesPipefail(callText) {
			return "shell pipelines must enable pipefail before emitting AVAILABLE or MISSING"
		}
		if (strings.Contains(callText, "\n") || strings.Contains(callText, ";")) && !neoRunCheckShellExitsOnError(callText) {
			return "multi-stage shell measurement must enable exit-on-error before emitting AVAILABLE or MISSING"
		}
	}
	exactExportLookup := strings.Contains(lower, "exports") &&
		(strings.Contains(lower, "hasownproperty") || strings.Contains(lower, "object.hasown") || neoRunCheckExportIndexPattern.MatchString(lower) ||
			neoRunCheckExactExportInExpression(callText)) || neoRunCheckStringExportIndex(callText)
	standardsAwareExportResolver := neoRunCheckStandardsAwareExportResolver(callText)
	if exactExportLookup && !standardsAwareExportResolver {
		available, missing, bound := neoRunCheckExportMapStatus(callText, output, accessPath)
		if status == "AVAILABLE" && (!bound || !available) {
			return "exact-key package export availability must be bound to the exact-floor package export map or a standards-aware resolver"
		}
		if status == "MISSING" && (!bound || !missing) {
			return "exact-key package export lookup does not establish absence unless wildcard export patterns are resolved"
		}
	}
	if status == "MISSING" && accessPath.module != "" && neoRunCheckWildcardExportPattern.MatchString(output) && !standardsAwareExportResolver {
		return "package export output contains wildcard export patterns that must be resolved with a standards-aware resolver before declaring a module path missing"
	}
	if verification == "root-runtime-traversal" || verification == "exact-behavior-test" || verification == "exact-export-inspection" {
		if !strings.Contains(strings.ToLower(callText), strings.ToLower(accessPath.raw)) {
			return "runtime, behavior, and export status assertions must measure the exact accessPath"
		}
		if !neoRunCheckHasStatusPredicate(callText, accessPath, verification, status) {
			return status + " status must be derived from a capability predicate, not assembled unconditionally"
		}
	}
	return ""
}

func neoRunCheckHasStatusPredicate(callText string, accessPath neoDependencyAccessPath, verification, status string) bool {
	sources := neoRunCheckExecutableSources(callText)
	for _, source := range sources {
		executable := neoRunCheckDeclarationLexicalMask(source)
		if verification != "exact-export-inspection" {
			for _, pathPattern := range neoRunCheckRuntimePathPatterns(source, accessPath) {
				predicate := regexp.MustCompile(`\b(?:typeof\s+` + pathPattern + `\s*(?:===?|!==?)\s*["'](?:function|undefined|object|string|number|boolean|symbol|bigint)["']|callable\(\s*` + pathPattern + `\b)`)
				for _, match := range predicate.FindAllStringIndex(source, -1) {
					if !neoRunCheckEvidenceTerm(executable[match[0]:match[1]], "typeof") && !neoRunCheckEvidenceTerm(executable[match[0]:match[1]], "callable") {
						continue
					}
					if neoRunCheckConditionalStatusStatement(source, match[0], match[1], status) {
						return true
					}
				}
			}
			continue
		}
		if neoRunCheckExportStatusPredicate(source, neoRunCheckASCIIFold(executable), status, accessPath) {
			return true
		}
	}
	return false
}

func neoRunCheckRuntimePathPatterns(source string, accessPath neoDependencyAccessPath) []string {
	parts := make([]string, 0, len(accessPath.members))
	for _, member := range accessPath.members {
		parts = append(parts, regexp.QuoteMeta(member))
	}
	patterns := []string{strings.Join(parts, `(?:\.|\?\.)`)}
	if len(accessPath.members) < 2 {
		return patterns
	}
	owner := regexp.QuoteMeta(accessPath.members[0])
	aliases := make([]string, 0)
	executable := neoRunCheckDeclarationLexicalMask(source)
	javascript := regexp.MustCompile(`\b(?:const|let|var)\s+([A-Za-z_$][A-Za-z0-9_$]*)\s*=\s*new\s+` + owner + `\b`)
	for _, match := range javascript.FindAllStringSubmatch(executable, -1) {
		if len(match) == 2 {
			aliases = append(aliases, match[1])
		}
	}
	python := regexp.MustCompile(`(?m)^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*` + owner + `\s*\(`)
	for _, match := range python.FindAllStringSubmatch(executable, -1) {
		if len(match) == 2 {
			aliases = append(aliases, match[1])
		}
	}
	suffix := make([]string, 0, len(accessPath.members)-1)
	for _, member := range accessPath.members[1:] {
		suffix = append(suffix, regexp.QuoteMeta(member))
	}
	for _, alias := range aliases {
		patterns = append(patterns, regexp.QuoteMeta(alias)+`(?:\.|\?\.)`+strings.Join(suffix, `(?:\.|\?\.)`))
	}
	return patterns
}

func neoRunCheckConditionalStatusStatement(source string, start, end int, status string) bool {
	statementStart := start
	for statementStart > 0 && source[statementStart-1] != ';' && source[statementStart-1] != '\n' {
		statementStart--
	}
	statementEnd := end
	for statementEnd < len(source) && source[statementEnd] != ';' && source[statementEnd] != '\n' {
		statementEnd++
	}
	statement := strings.ToLower(source[statementStart:statementEnd])
	prefix := strings.TrimSpace(source[statementStart:start])
	predicate := strings.ToLower(source[start:end])
	trueStatus, falseStatus, validPredicate := neoRunCheckPredicateStatuses(predicate)
	if !validPredicate {
		return false
	}
	jsBranches := regexp.MustCompile(`\?\s*["']?` + trueStatus + `["']?\s*:\s*["']?` + falseStatus + `["']?`).FindStringIndex(statement)
	pythonBranches := regexp.MustCompile(`["']?` + trueStatus + `["']?\s+if\s+[^\n;]+\s+else\s+["']?` + falseStatus + `["']?`).FindStringIndex(statement)
	if jsBranches == nil && pythonBranches == nil {
		return false
	}
	emitsStatus := neoRunCheckConditionalDirectlyEmitted(source[statementStart:statementEnd], start-statementStart, end-statementStart)
	if !emitsStatus {
		assignment := regexp.MustCompile(`(?i)(?:const|let|var)\s+([a-z_$][a-z0-9_$]*)\s*=\s*$`).FindStringSubmatch(prefix)
		if len(assignment) == 2 {
			remainder := source[statementEnd:]
			emitter := regexp.MustCompile(`(?i)(?:console\.log|print|printf|emit)\s*\([^;\n]*\b` + regexp.QuoteMeta(assignment[1]) + `\b`)
			emitsStatus = emitter.MatchString(remainder)
		}
	}
	return strings.Contains(statement, strings.ToLower(status)) && emitsStatus
}

func neoRunCheckPredicateStatuses(predicate string) (string, string, bool) {
	if neoRunCheckEvidenceTerm(predicate, "callable") {
		return "available", "missing", true
	}
	lower := strings.ToLower(strings.TrimSpace(predicate))
	for _, marker := range []string{"hasownproperty", "object.hasown", " in ", "resolve.exports", "resolve-exports", "import.meta.resolve", "require.resolve"} {
		index := strings.Index(lower, marker)
		if index < 0 {
			continue
		}
		prefix := strings.TrimSpace(lower[:index])
		for strings.HasSuffix(prefix, "(") {
			prefix = strings.TrimSpace(strings.TrimSuffix(prefix, "("))
		}
		if strings.HasSuffix(prefix, "!") {
			return "missing", "available", true
		}
		return "available", "missing", true
	}
	comparison := regexp.MustCompile(`(===|==|!==|!=)\s*["'](function|undefined|object|string|number|boolean|symbol|bigint)["']`).FindStringSubmatch(predicate)
	if len(comparison) != 3 {
		return "", "", false
	}
	equality := comparison[1] == "==" || comparison[1] == "==="
	switch comparison[2] {
	case "undefined":
		if equality {
			return "missing", "available", true
		}
		return "available", "missing", true
	case "function":
		if equality {
			return "available", "missing", true
		}
		return "missing", "available", true
	default:
		if equality {
			return "available", "missing", true
		}
		return "", "", false
	}
}

func neoRunCheckConditionalDirectlyEmitted(statement string, predicateStart, predicateEnd int) bool {
	masked := strings.ToLower(neoRunCheckDeclarationLexicalMask(statement))
	emitter := regexp.MustCompile(`(?:console\.log|print|printf|emit)\s*\(`)
	for _, match := range emitter.FindAllStringIndex(masked, -1) {
		open := match[1] - 1
		if open >= predicateStart {
			continue
		}
		depth := 0
		for index := open; index < len(masked); index++ {
			switch masked[index] {
			case '(':
				depth++
			case ')':
				depth--
				if depth == 0 {
					if predicateEnd <= index {
						return true
					}
					index = len(masked)
				}
			}
		}
	}
	return false
}

func neoRunCheckExportStatusPredicate(source, executable, status string, accessPath neoDependencyAccessPath) bool {
	directPredicates := []string{"hasownproperty", "object.hasown", " in "}
	for _, predicate := range directPredicates {
		for offset := 0; ; {
			index := strings.Index(executable[offset:], predicate)
			if index < 0 {
				break
			}
			index += offset
			predicateStart := index
			for previous := index - 1; previous >= 0; previous-- {
				if source[previous] == ' ' || source[previous] == '\t' || source[previous] == '(' {
					continue
				}
				if source[previous] == '!' {
					predicateStart = previous
				}
				break
			}
			predicateEnd := index + len(predicate)
			for predicateEnd < len(source) && source[predicateEnd] != '?' && source[predicateEnd] != ';' && source[predicateEnd] != '\n' {
				predicateEnd++
			}
			if neoRunCheckExportPredicateTargetsModule(source, index, index+len(predicate), accessPath) && neoRunCheckConditionalStatusStatement(source, predicateStart, predicateEnd, status) {
				return true
			}
			offset = index + 1
		}
	}
	assignment := regexp.MustCompile(`\b(?:const|let|var)\s+([a-z_$][a-z0-9_$]*)\s*=\s*[^;\n]*(?:hasownproperty|object\.hasown|\sin\s+[^;\n]*exports|exports(?:\?\.)?\[[^]]+\]|resolve\.exports|resolve-exports|import\.meta\.resolve|require\.resolve)`)
	for _, match := range assignment.FindAllStringSubmatchIndex(executable, -1) {
		if len(match) < 4 {
			continue
		}
		name := executable[match[2]:match[3]]
		if !neoRunCheckExportPredicateTargetsModule(source, match[0], match[1], accessPath) {
			continue
		}
		trueStatus, falseStatus, validPredicate := neoRunCheckPredicateStatuses(source[match[0]:match[1]])
		if !validPredicate {
			continue
		}
		conditional := regexp.MustCompile(`(?:console\.log|print|printf|emit)\s*\([^;\n]*\b` + regexp.QuoteMeta(name) + `\b[^;\n]*\?[^;\n]*` + trueStatus + `[^;\n]*:[^;\n]*` + falseStatus)
		if conditional.MatchString(strings.ToLower(source[match[1]:])) {
			return true
		}
	}
	return false
}

func neoRunCheckExportPredicateTargetsModule(source string, start, end int, accessPath neoDependencyAccessPath) bool {
	if accessPath.module == "" {
		return true
	}
	exportKey := neoRunCheckModuleExportKey(accessPath.module)
	if exportKey == "" {
		return false
	}
	statementStart := start
	for statementStart > 0 && source[statementStart-1] != ';' && source[statementStart-1] != '\n' {
		statementStart--
	}
	statementEnd := end
	for statementEnd < len(source) && source[statementEnd] != ';' && source[statementEnd] != '\n' {
		statementEnd++
	}
	statement := source[statementStart:statementEnd]
	statementMask := strings.ToLower(neoRunCheckDeclarationLexicalMask(statement))
	exportTarget := regexp.MustCompile(`(?:\bpkg(?:\?\.|\.)exports\b|resolve\.exports|resolve-exports|import\.meta\.resolve|require\.resolve)`)
	if !exportTarget.MatchString(statementMask) {
		return false
	}
	quotedKey := regexp.MustCompile(`["']` + regexp.QuoteMeta(exportKey) + `["']`)
	if quotedKey.MatchString(statement) {
		return true
	}
	assignment := regexp.MustCompile(`(?m)\b(?:const|let|var)\s+([A-Za-z_$][A-Za-z0-9_$]*)\s*=\s*["']` + regexp.QuoteMeta(exportKey) + `["']`)
	for _, match := range assignment.FindAllStringSubmatch(source[:statementEnd], -1) {
		if len(match) == 2 && neoRunCheckEvidenceTerm(statement, match[1]) {
			return true
		}
	}
	return false
}

func neoRunCheckStandardsAwareExportResolver(callInput string) bool {
	executable := strings.ToLower(neoRunCheckExecutableText(callInput))
	for _, term := range []string{"resolve.exports", "resolve-exports", "import.meta.resolve(", "require.resolve("} {
		if neoRunCheckExecutableToken(executable, term) {
			return true
		}
	}
	return false
}

func neoRunCheckExecutableToken(text, term string) bool {
	for offset := 0; offset <= len(text)-len(term); {
		index := strings.Index(text[offset:], term)
		if index < 0 {
			return false
		}
		index += offset
		beforeOK := index == 0 || !neoDependencyEvidenceByte(text[index-1])
		after := index + len(term)
		afterOK := strings.HasSuffix(term, "(") || after == len(text) || !neoDependencyEvidenceByte(text[after])
		if beforeOK && afterOK {
			return true
		}
		offset = index + 1
	}
	return false
}

func neoRunCheckStringExportIndex(callInput string) bool {
	sources := neoRunCheckExecutableSources(callInput)
	for _, source := range sources {
		masked := neoRunCheckDeclarationLexicalMask(source)
		for open := 0; open < len(masked); open++ {
			if masked[open] != '[' {
				continue
			}
			closeOffset := strings.IndexByte(masked[open+1:], ']')
			if closeOffset < 0 {
				break
			}
			close := open + 1 + closeOffset
			property := strings.TrimSpace(source[open+1 : close])
			if property != `"exports"` && property != `'exports'` {
				continue
			}
			after := strings.TrimSpace(masked[close+1:])
			if strings.HasPrefix(after, "?.") {
				after = strings.TrimSpace(after[2:])
			}
			if strings.HasPrefix(after, "[") {
				return true
			}
		}
	}
	return false
}

func neoRunCheckCompleteExportMap(callInput, output string, accessPath neoDependencyAccessPath) bool {
	_, missing, ok := neoRunCheckExportMapStatus(callInput, output, accessPath)
	return ok && missing
}

func neoRunCheckExportMapStatus(callInput, output string, accessPath neoDependencyAccessPath) (bool, bool, bool) {
	const marker = "CLIPROXY_PACKAGE_EXPORTS="
	exportKey := neoRunCheckModuleExportKey(accessPath.module)
	if exportKey == "" || !neoRunCheckExportPacketPattern.MatchString(callInput) {
		return false, false, false
	}
	for _, line := range strings.Split(strings.ReplaceAll(output, "\r\n", "\n"), "\n") {
		packet := strings.TrimSpace(line)
		if !strings.HasPrefix(packet, marker) {
			continue
		}
		var exports any
		if json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(packet, marker))), &exports) != nil || !neoRunCheckValidExportTarget(exports) || neoRunCheckExportValueHasWildcard(exports) {
			continue
		}
		switch typed := exports.(type) {
		case nil, string, []any:
			if exportKey == "." {
				return exports != nil, exports == nil, true
			}
			return false, true, true
		case map[string]any:
			subpathMap := false
			conditionMap := false
			for key := range typed {
				if strings.HasPrefix(key, ".") {
					subpathMap = true
				} else {
					conditionMap = true
				}
			}
			if subpathMap && conditionMap {
				continue
			}
			if !subpathMap {
				if exportKey == "." {
					return true, false, true
				}
				return false, true, true
			}
			target, exported := typed[exportKey]
			if !exported || target == nil {
				return false, true, true
			}
			return true, false, true
		}
	}
	return false, false, false
}

func neoRunCheckModuleExportKey(module string) string {
	parts := strings.Split(strings.TrimSpace(module), "/")
	prefix := 1
	if len(parts) > 0 && strings.HasPrefix(parts[0], "@") {
		prefix = 2
	}
	if len(parts) <= prefix {
		return "."
	}
	return "./" + strings.Join(parts[prefix:], "/")
}

func neoRunCheckExportValueHasWildcard(value any) bool {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			if strings.Contains(key, "*") || neoRunCheckExportValueHasWildcard(child) {
				return true
			}
		}
	case []any:
		for _, child := range typed {
			if neoRunCheckExportValueHasWildcard(child) {
				return true
			}
		}
	case string:
		return strings.Contains(typed, "*")
	}
	return false
}

func neoRunCheckValidExportTarget(value any) bool {
	switch typed := value.(type) {
	case nil, string:
		return true
	case []any:
		for _, child := range typed {
			if !neoRunCheckValidExportTarget(child) {
				return false
			}
		}
		return true
	case map[string]any:
		for _, child := range typed {
			if !neoRunCheckValidExportTarget(child) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func neoRunCheckHasCaughtFailure(callInput string, accessPath neoDependencyAccessPath) bool {
	for _, source := range neoRunCheckExecutableSources(callInput) {
		executable := strings.ToLower(neoRunCheckDeclarationLexicalMask(source))
		for offset := 0; ; {
			index := strings.Index(executable[offset:], ".catch(")
			if index < 0 {
				break
			}
			index += offset
			start := strings.LastIndexAny(executable[:index], ";\n") + 1
			end := index + len(".catch(")
			if tail := strings.IndexAny(executable[end:], ";\n"); tail >= 0 {
				end += tail
			} else {
				end = len(executable)
			}
			if neoRunCheckCaughtFailureRelevant(source[start:end], accessPath) {
				return true
			}
			offset = index + len(".catch(")
		}
		for offset := 0; ; {
			index := strings.Index(executable[offset:], "catch")
			if index < 0 {
				break
			}
			index += offset
			beforeOK := index == 0 || !neoRunCheckEvidenceIdentifierByte(executable[index-1])
			after := index + len("catch")
			if beforeOK && (after == len(executable) || !neoRunCheckEvidenceIdentifierByte(executable[after])) {
				tryIndex := strings.LastIndex(executable[:index], "try")
				if tryIndex >= 0 && neoRunCheckCaughtFailureRelevant(source[tryIndex:after], accessPath) {
					return true
				}
			}
			offset = index + len("catch")
		}
		lines := strings.Split(strings.ReplaceAll(executable, "\r\n", "\n"), "\n")
		rawLines := strings.Split(strings.ReplaceAll(source, "\r\n", "\n"), "\n")
		for index, line := range lines {
			trimmed := strings.TrimSpace(line)
			if !strings.HasPrefix(trimmed, "except:") && !strings.HasPrefix(trimmed, "except ") {
				continue
			}
			for tryIndex := index - 1; tryIndex >= 0; tryIndex-- {
				if strings.TrimSpace(lines[tryIndex]) != "try:" {
					continue
				}
				if neoRunCheckCaughtFailureRelevant(strings.Join(rawLines[tryIndex:index+1], "\n"), accessPath) {
					return true
				}
				break
			}
		}
	}
	return false
}

func neoRunCheckCaughtFailureRelevant(scope string, accessPath neoDependencyAccessPath) bool {
	lower := strings.ToLower(scope)
	if accessPath.raw != "" && strings.Contains(lower, strings.ToLower(accessPath.raw)) {
		return true
	}
	for _, term := range []string{"loadfloor", "load_floor", "loadexactfloor", "load_exact_floor", "import", "require(", "resolve.exports", "resolve-exports", "npm pack", "pnpm pack", "pip download", "tar ", "unzip ", "traverse", "=available", "=missing", "=unverified"} {
		if strings.Contains(lower, term) {
			return true
		}
	}
	return false
}

func neoRunCheckExecutableText(callInput string) string {
	sources := neoRunCheckExecutableSources(callInput)
	for index := range sources {
		sources[index] = neoRunCheckDeclarationLexicalMask(sources[index])
	}
	return strings.Join(sources, "\n")
}

type neoRunCheckHeredoc struct {
	delimiter  string
	stripTabs  bool
	executable bool
	body       strings.Builder
}

func neoRunCheckExecutableSources(callInput string) []string {
	input := strings.ReplaceAll(strings.ReplaceAll(callInput, "\r\n", "\n"), "\r", "\n")
	outer := []byte(input)
	scripts := make([]string, 0)
	pending := make([]*neoRunCheckHeredoc, 0)
	for offset := 0; offset <= len(input); {
		end := strings.IndexByte(input[offset:], '\n')
		if end < 0 {
			end = len(input)
		} else {
			end += offset
		}
		line := input[offset:end]
		if len(pending) > 0 {
			candidate := line
			if pending[0].stripTabs {
				candidate = strings.TrimLeft(candidate, "\t")
			}
			if candidate == pending[0].delimiter {
				if pending[0].executable {
					scripts = append(scripts, pending[0].body.String())
				}
				pending = pending[1:]
			} else if pending[0].executable {
				pending[0].body.WriteString(line)
				pending[0].body.WriteByte('\n')
			}
			for index := offset; index < end; index++ {
				outer[index] = ' '
			}
		} else {
			pending = append(pending, neoRunCheckLineHeredocs(line)...)
		}
		if end == len(input) {
			break
		}
		offset = end + 1
	}
	outerText := string(outer)
	sources := []string{outerText}
	sources = append(sources, neoRunCheckInlineScriptBodies(callInput, neoRunCheckDeclarationLexicalMask(outerText))...)
	sources = append(sources, scripts...)
	return sources
}

func neoRunCheckLineHeredocs(line string) []*neoRunCheckHeredoc {
	heredocs := make([]*neoRunCheckHeredoc, 0)
	var quote byte
	escaped := false
	for index := 0; index+1 < len(line); index++ {
		current := line[index]
		if escaped {
			escaped = false
			continue
		}
		if quote != 0 {
			if current == '\\' && quote == '"' {
				escaped = true
			} else if current == quote {
				quote = 0
			}
			continue
		}
		if current == '\\' {
			escaped = true
			continue
		}
		if current == '\'' || current == '"' {
			quote = current
			continue
		}
		if current == '#' && (index == 0 || line[index-1] == ' ' || line[index-1] == '\t') {
			break
		}
		if current != '<' || line[index+1] != '<' {
			continue
		}
		cursor := index + 2
		stripTabs := false
		if cursor < len(line) && line[cursor] == '-' {
			stripTabs = true
			cursor++
		}
		for cursor < len(line) && (line[cursor] == ' ' || line[cursor] == '\t') {
			cursor++
		}
		delimiterQuote := byte(0)
		if cursor < len(line) && (line[cursor] == '\'' || line[cursor] == '"') {
			delimiterQuote = line[cursor]
			cursor++
		}
		start := cursor
		if delimiterQuote != 0 {
			for cursor < len(line) && line[cursor] != delimiterQuote {
				cursor++
			}
		} else {
			for cursor < len(line) && !strings.ContainsRune(" \t;|&()<>#", rune(line[cursor])) {
				cursor++
			}
		}
		if cursor == start || delimiterQuote != 0 && cursor == len(line) {
			continue
		}
		statements := neoRunCheckShellStatements(line[:index])
		heredocs = append(heredocs, &neoRunCheckHeredoc{
			delimiter:  line[start:cursor],
			stripTabs:  stripTabs,
			executable: len(statements) > 0 && neoRunCheckScriptInterpreter(statements[len(statements)-1]),
		})
		index = cursor
	}
	return heredocs
}

func neoRunCheckScriptInterpreter(words []string) bool {
	for len(words) > 0 && strings.Contains(words[0], "=") {
		words = words[1:]
	}
	if len(words) == 0 {
		return false
	}
	command := strings.ToLower(filepath.Base(words[0]))
	return command == "node" || command == "nodejs" || command == "python" || strings.HasPrefix(command, "python3")
}

func neoRunCheckInlineScriptBodies(callInput, lexicalMask string) []string {
	pattern := regexp.MustCompile(`\b(?:node|python(?:3(?:\.\d+)?)?)\s+(?:-e|-c|--eval)\b`)
	bodies := make([]string, 0)
	for _, match := range pattern.FindAllStringIndex(lexicalMask, -1) {
		offset := match[1]
		for offset < len(callInput) && (callInput[offset] == ' ' || callInput[offset] == '\t') {
			offset++
		}
		if offset >= len(callInput) || callInput[offset] != '\'' && callInput[offset] != '"' {
			continue
		}
		quote := callInput[offset]
		start := offset + 1
		escaped := false
		for offset = start; offset < len(callInput); offset++ {
			if escaped {
				escaped = false
				continue
			}
			if callInput[offset] == '\\' && quote == '"' {
				escaped = true
				continue
			}
			if callInput[offset] == quote {
				bodies = append(bodies, callInput[start:offset])
				break
			}
		}
	}
	return bodies
}

func neoRunCheckExactExportInExpression(callInput string) bool {
	executable := strings.ToLower(neoRunCheckExecutableText(callInput))
	for _, line := range strings.Split(executable, "\n") {
		if strings.Contains(line, " in ") && strings.Contains(line, "exports") {
			return true
		}
	}
	return false
}

func neoRunCheckShellTool(name string) bool {
	switch normalizedNeoToolName(name) {
	case "bash", "shellcommand", "shellcommandstatus", "runterminalcommand":
		return true
	default:
		return false
	}
}

func neoRunCheckShellExitsOnError(callInput string) bool {
	statement := neoRunCheckFirstShellStatement(callInput)
	fields := strings.Fields(statement)
	if len(fields) < 2 || fields[0] != "set" {
		return false
	}
	if fields[1] == "-o" {
		return len(fields) >= 3 && fields[2] == "errexit"
	}
	return strings.HasPrefix(fields[1], "-") && strings.Contains(fields[1][1:], "e")
}

func neoRunCheckShellDisablesErrexit(callInput string) bool {
	for _, statement := range neoRunCheckShellStatements(callInput) {
		if len(statement) < 2 || statement[0] != "set" {
			continue
		}
		if statement[1] == "+o" && len(statement) >= 3 && statement[2] == "errexit" || strings.HasPrefix(statement[1], "+") && strings.Contains(statement[1][1:], "e") {
			return true
		}
	}
	return false
}

func neoRunCheckShellHasOrOperator(callInput string) bool {
	hasOr, _, _, _ := neoRunCheckShellOperators(callInput)
	return hasOr
}

func neoRunCheckShellHasAndOperator(callInput string) bool {
	_, hasAnd, _, _ := neoRunCheckShellOperators(callInput)
	return hasAnd
}

func neoRunCheckShellHasPipeline(callInput string) bool {
	_, _, hasPipeline, _ := neoRunCheckShellOperators(callInput)
	return hasPipeline
}

func neoRunCheckShellHasBackgroundOperator(callInput string) bool {
	_, _, _, hasBackground := neoRunCheckShellOperators(callInput)
	return hasBackground
}

func neoRunCheckShellOperators(callInput string) (bool, bool, bool, bool) {
	input := strings.ReplaceAll(strings.ReplaceAll(callInput, "\r\n", "\n"), "\r", "\n")
	heredocs := make([]string, 0)
	var quote byte
	hasOr := false
	hasAnd := false
	hasPipeline := false
	hasBackground := false
	for _, line := range strings.Split(input, "\n") {
		if len(heredocs) > 0 {
			if strings.TrimSpace(line) == heredocs[0] {
				heredocs = heredocs[1:]
			}
			continue
		}
		lineHasOr, lineHasAnd, lineHasPipeline, lineHasBackground, lineHeredocs, nextQuote := neoRunCheckShellLineOperators(line, quote)
		hasOr = hasOr || lineHasOr
		hasAnd = hasAnd || lineHasAnd
		hasPipeline = hasPipeline || lineHasPipeline
		hasBackground = hasBackground || lineHasBackground
		quote = nextQuote
		heredocs = append(heredocs, lineHeredocs...)
	}
	return hasOr, hasAnd, hasPipeline, hasBackground
}

func neoRunCheckShellLineOperators(line string, quote byte) (bool, bool, bool, bool, []string, byte) {
	escaped := false
	comment := false
	hasOr := false
	hasAnd := false
	hasPipeline := false
	hasBackground := false
	heredocs := make([]string, 0)
	for index := 0; index < len(line); index++ {
		current := line[index]
		if comment {
			continue
		}
		if escaped {
			escaped = false
			continue
		}
		if quote != 0 {
			if current == '\\' && quote == '"' {
				escaped = true
			} else if current == quote {
				quote = 0
			}
			continue
		}
		switch current {
		case '\\':
			escaped = true
		case '\'', '"':
			quote = current
		case '#':
			if index == 0 || line[index-1] == ' ' || line[index-1] == '\t' {
				comment = true
			}
		case '<':
			if index+1 >= len(line) || line[index+1] != '<' {
				continue
			}
			cursor := index + 2
			if cursor < len(line) && line[cursor] == '-' {
				cursor++
			}
			for cursor < len(line) && (line[cursor] == ' ' || line[cursor] == '\t') {
				cursor++
			}
			start := cursor
			if cursor < len(line) && (line[cursor] == '\'' || line[cursor] == '"') {
				delimiterQuote := line[cursor]
				start = cursor + 1
				cursor = start
				for cursor < len(line) && line[cursor] != delimiterQuote {
					cursor++
				}
				if cursor < len(line) && cursor > start {
					heredocs = append(heredocs, line[start:cursor])
					index = cursor
				}
				continue
			}
			for cursor < len(line) && !strings.ContainsRune(" \t;|&()<>", rune(line[cursor])) {
				cursor++
			}
			if cursor > start {
				heredocs = append(heredocs, line[start:cursor])
				index = cursor - 1
			}
		case '|':
			if index+1 < len(line) && line[index+1] == '|' {
				hasOr = true
				index++
				continue
			}
			hasPipeline = true
		case '&':
			if index+1 < len(line) && line[index+1] == '&' {
				hasAnd = true
				index++
			} else if (index == 0 || line[index-1] != '>' && line[index-1] != '<') && (index+1 == len(line) || line[index+1] != '>') {
				hasBackground = true
			}
		}
	}
	return hasOr, hasAnd, hasPipeline, hasBackground, heredocs, quote
}

func neoRunCheckShellEnablesPipefail(callInput string) bool {
	fields := strings.Fields(neoRunCheckFirstShellStatement(callInput))
	if len(fields) < 3 || fields[0] != "set" {
		return false
	}
	for index := 1; index+1 < len(fields); index++ {
		option := fields[index]
		if (option == "-o" || strings.HasPrefix(option, "-") && strings.Contains(option[1:], "o")) && fields[index+1] == "pipefail" {
			return true
		}
	}
	return false
}

func neoRunCheckFirstShellStatement(callInput string) string {
	input := strings.ReplaceAll(strings.ReplaceAll(callInput, "\r\n", "\n"), "\r", "\n")
	for offset := 0; offset < len(input); {
		for offset < len(input) && (input[offset] == ' ' || input[offset] == '\t' || input[offset] == '\n') {
			offset++
		}
		if offset >= len(input) {
			return ""
		}
		if input[offset] == '#' {
			if newline := strings.IndexByte(input[offset:], '\n'); newline >= 0 {
				offset += newline + 1
				continue
			}
			return ""
		}
		start := offset
		var quote byte
		escaped := false
		for offset < len(input) {
			current := input[offset]
			if escaped {
				escaped = false
				offset++
				continue
			}
			if quote != 0 {
				if current == '\\' && quote == '"' {
					escaped = true
				} else if current == quote {
					quote = 0
				}
				offset++
				continue
			}
			switch current {
			case '\\':
				escaped = true
			case '\'', '"':
				quote = current
			case ';', '\n':
				return strings.TrimSpace(input[start:offset])
			case '#':
				if offset == start || input[offset-1] == ' ' || input[offset-1] == '\t' {
					return strings.TrimSpace(input[start:offset])
				}
			}
			offset++
		}
		return strings.TrimSpace(input[start:])
	}
	return ""
}

func neoRunCheckToolCallText(callInput string) string {
	var value any
	if json.Unmarshal([]byte(callInput), &value) != nil {
		return callInput
	}
	values := make([]string, 0)
	var collect func(any)
	collect = func(value any) {
		switch typed := value.(type) {
		case string:
			values = append(values, typed)
		case []any:
			for _, item := range typed {
				collect(item)
			}
		case map[string]any:
			keys := make([]string, 0, len(typed))
			for key := range typed {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			for _, key := range keys {
				collect(typed[key])
			}
		}
	}
	collect(value)
	return strings.Join(values, "\n")
}

func neoRunCheckToolCommandText(callInput string) string {
	var input map[string]any
	if json.Unmarshal([]byte(callInput), &input) == nil {
		return stringValue(input["command"])
	}
	return neoRunCheckToolCallText(callInput)
}

func neoRunCheckToolEvidenceCallText(evidence map[string]any) string {
	callInput := stringValue(evidence["input"])
	if neoRunCheckShellTool(stringValue(evidence["tool"])) {
		return neoRunCheckToolCommandText(callInput)
	}
	return neoRunCheckToolCallText(callInput)
}

func neoRunCheckClosingBrace(text string, open int) int {
	depth := 0
	quote := byte(0)
	escaped := false
	for index := open; index < len(text); index++ {
		value := text[index]
		if quote != 0 {
			if escaped {
				escaped = false
				continue
			}
			if value == '\\' {
				escaped = true
				continue
			}
			if value == quote {
				quote = 0
			}
			continue
		}
		if value == '\'' || value == '"' || value == '`' {
			quote = value
			continue
		}
		switch value {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return index
			}
		}
	}
	return -1
}

func neoRunCheckGroundedStatusAssertions(input map[string]any) []string {
	type assertionCandidate struct {
		line       string
		accessPath string
	}
	candidates := make([]assertionCandidate, 0)
	statusesByAccessPath := map[string]map[string]bool{}
	for _, raw := range arrayValue(input[neoRunCheckToolEvidenceKey]) {
		evidence := mapValue(raw)
		callInput := stringValue(evidence["input"])
		for _, output := range neoStringSlice(evidence["outputs"]) {
			for _, line := range strings.Split(strings.ReplaceAll(output, "\r\n", "\n"), "\n") {
				line = strings.TrimSpace(line)
				separator := strings.LastIndexByte(line, '=')
				if separator <= 0 {
					continue
				}
				status := line[separator+1:]
				if status != "AVAILABLE" && status != "MISSING" && status != "UNVERIFIED" {
					continue
				}
				accessPath, err := neoParseDependencyAccessPath(line[:separator])
				if err != nil || strings.Contains(callInput, line) {
					continue
				}
				runtimeReason := neoRunCheckUnsafeStatusAssertion(evidence, output, accessPath, status, "root-runtime-traversal")
				exportReason := neoRunCheckUnsafeStatusAssertion(evidence, output, accessPath, status, "exact-export-inspection")
				if runtimeReason != "" && exportReason != "" {
					continue
				}
				statuses := statusesByAccessPath[accessPath.raw]
				if statuses == nil {
					statuses = map[string]bool{}
					statusesByAccessPath[accessPath.raw] = statuses
				}
				statuses[status] = true
				candidates = append(candidates, assertionCandidate{line: line, accessPath: accessPath.raw})
			}
		}
	}
	seen := map[string]bool{}
	assertions := make([]string, 0)
	for _, candidate := range candidates {
		if len(statusesByAccessPath[candidate.accessPath]) != 1 || seen[candidate.line] {
			continue
		}
		seen[candidate.line] = true
		assertions = append(assertions, candidate.line)
		if len(assertions) == 64 {
			break
		}
	}
	return assertions
}

func neoRunCheckStringSlice(value any) ([]string, bool) {
	raw, ok := neoRunCheckStringArray(value)
	if !ok {
		return nil, false
	}
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		out = append(out, item.(string))
	}
	return out, true
}

func neoReviewSameStringSet(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	seen := make(map[string]bool, len(got))
	for _, value := range got {
		if seen[value] {
			return false
		}
		seen[value] = true
	}
	for _, value := range want {
		if !seen[value] {
			return false
		}
	}
	return true
}

func neoReviewStringSetContains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func neoReviewLineRangeValid(startLine, endLine int) bool {
	return startLine == 0 && endLine == 0 || startLine >= 1 && endLine >= startLine
}

func neoReviewDiffDeletesFile(diff string) bool {
	oldHeader := false
	for _, line := range strings.Split(diff, "\n") {
		if neoReviewHunkHeaderPattern.MatchString(line) {
			return false
		}
		if strings.HasPrefix(line, "--- ") {
			oldHeader = true
			continue
		}
		if oldHeader && line == "+++ /dev/null" {
			return true
		}
		if strings.HasPrefix(line, "+++ ") {
			oldHeader = false
		}
	}
	return false
}

func neoReviewDiffHasHunk(diff string) bool {
	for _, line := range strings.Split(diff, "\n") {
		if neoReviewHunkHeaderPattern.MatchString(line) {
			return true
		}
	}
	return false
}

func neoReviewDiffHasNoNewTextLine(diff string) bool {
	return !neoReviewDiffHasHunk(diff) || neoReviewDiffEmptiesRetainedFile(diff)
}

func neoReviewDiffEmptiesRetainedFile(diff string) bool {
	if neoReviewDiffDeletesFile(diff) {
		return false
	}
	hunks := 0
	emptyHunks := 0
	for _, line := range strings.Split(diff, "\n") {
		if !neoReviewHunkHeaderPattern.MatchString(line) {
			continue
		}
		hunks++
		if neoReviewEmptyRetainedHunkPattern.MatchString(line) {
			emptyHunks++
		}
	}
	return hunks == 1 && emptyHunks == 1
}

func neoReviewZeroRangeAllowed(filename, diff string, deletedLines []string) bool {
	return neoReviewDiffHasNoNewTextLine(diff) || neoReviewDiffDeletesFile(diff) || neoReviewStringSetContains(deletedLines, filename+"@@+0,0")
}

func neoReviewRangeWithinSnapshotHunk(filename string, startLine, endLine int, hunkIDs []string) bool {
	prefix := filename + "@@+"
	for _, hunkID := range hunkIDs {
		if !strings.HasPrefix(hunkID, prefix) {
			continue
		}
		rangeText := strings.TrimPrefix(hunkID, prefix)
		parts := strings.SplitN(rangeText, ",", 2)
		if len(parts) != 2 {
			continue
		}
		hunkStart, startErr := strconv.Atoi(parts[0])
		hunkCount, countErr := strconv.Atoi(parts[1])
		if startErr != nil || countErr != nil || hunkCount <= 0 {
			continue
		}
		hunkEnd := hunkStart + hunkCount - 1
		if startLine >= hunkStart && endLine <= hunkEnd {
			return true
		}
	}
	return false
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

func neoParseRunCheckResult(input map[string]any, text string) (map[string]any, error) {
	checkName := stringValue(input["checkName"])
	trimmed := strings.TrimSpace(text)
	var parsed map[string]any
	decoder := json.NewDecoder(strings.NewReader(trimmed))
	if err := decoder.Decode(&parsed); err != nil {
		return nil, fmt.Errorf("decode the final JSON object: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF || parsed == nil {
		return nil, fmt.Errorf("the final message must contain exactly one JSON object and no other content")
	}
	if stringValue(parsed["checkName"]) == "" {
		parsed["checkName"] = checkName
	}
	normalized, err := neoNormalizeRunCheckResult(input, parsed)
	if err != nil {
		return nil, fmt.Errorf("validate the final result: %w", err)
	}
	return normalized, nil
}

func neoRunCheckResultFromText(input map[string]any, text string) map[string]any {
	normalized, err := neoParseRunCheckResult(input, text)
	if err != nil {
		return neoRunCheckErrorResult(input, "check agent did not return a structured result: "+err.Error())
	}
	return normalized
}

func neoRunCheckRepairPrompt(input map[string]any, parseErr error) string {
	checkName := stringValue(input["checkName"])
	files := neoStringSlice(input[neoReviewSnapshotFilesKey])
	hunks := neoStringSlice(input[neoReviewSnapshotHunksKey])
	completed := map[string]any{
		"checkName":       checkName,
		"status":          "completed",
		"filesAnalyzed":   len(files),
		"linesAnalyzed":   0,
		"patternsChecked": []any{"one distinct concrete pattern or capability chain"},
		"evidence": []any{map[string]any{
			"patternIndex": 0,
			"observation":  "concrete fact established by the cited source",
			"sources":      []any{"exact source"},
			"outcome":      "no-finding",
			"issueIndexes": []any{},
		}},
		"issues": []any{},
	}
	if stringValue(input[neoReviewSnapshotHashKey]) != "" {
		completed["coveredFiles"] = stringArrayValue(files)
		completed["coveredHunks"] = stringArrayValue(hunks)
	}
	if checkName == "published-dependency-capability-floor" {
		evidence := mapValue(arrayValue(completed["evidence"])[0])
		dependency := "example-sdk"
		floorVersion := "1.0.0"
		accessPath := "RootClient.requiredCapability"
		completed["patternsChecked"] = []any{neoDependencyPatternKey(dependency, floorVersion, accessPath)}
		if boolValue(input[neoRunCheckToolEvidenceRequired]) {
			evidence["observation"] = "the capability pattern is not applicable after inspecting the relevant files"
			evidence["outcome"] = "not-applicable"
		} else {
			evidence["dependency"] = dependency
			evidence["floorVersion"] = floorVersion
			evidence["accessPath"] = accessPath
			evidence["floorStatus"] = "compatible"
			evidence["verification"] = "root-type-declaration"
			evidence["rootEvidence"] = []any{"export declare class RootClient { requiredCapability: unknown; }"}
		}
	}
	if checkName == "bounded-artifact-state-transitions" {
		evidence := mapValue(arrayValue(completed["evidence"])[0])
		evidence["phaseRelationship"] = "replacement"
		evidence["budgetOrigin"] = "original"
		evidence["decisiveSequence"] = "concrete before-state, later candidate, decision, and final state"
	}
	if checkName == "classifier-detector-matrix" {
		evidence := mapValue(arrayValue(completed["evidence"])[0])
		evidence["sourceLifetime"] = "immutable"
		evidence["decisionLifetime"] = "per-use"
		evidence["mutationPath"] = "none: source is not reassignable"
		evidence["usePath"] = "operation that consumes the classification"
	}
	completedJSON, _ := json.Marshal(completed)
	errorExample := map[string]any{
		"checkName":       checkName,
		"status":          "error",
		"filesAnalyzed":   len(files),
		"linesAnalyzed":   0,
		"patternsChecked": []any{},
		"evidence":        []any{},
		"issues":          []any{},
		"errorMessage":    "specific failure",
		"coveredFiles":    nil,
		"coveredHunks":    nil,
	}
	if stringValue(input[neoReviewSnapshotHashKey]) != "" {
		errorExample["coveredFiles"] = stringArrayValue(files)
		errorExample["coveredHunks"] = stringArrayValue(hunks)
	}
	errorJSON, _ := json.Marshal(errorExample)
	groundedStatusAssertions := ""
	if checkName == "published-dependency-capability-floor" && boolValue(input[neoRunCheckToolEvidenceRequired]) {
		assertions := neoRunCheckGroundedStatusAssertions(input)
		if len(assertions) == 0 {
			groundedStatusAssertions = "\nNo canonical status assertions were found in successful tool output. Return the failed-check object rather than synthesizing dependency-floor evidence.\n"
		} else {
			groundedStatusAssertions = "\nCanonical status assertions found byte-for-byte in successful tool output:\n" + strings.Join(assertions, "\n") + "\nUse one only when its exact path and status match the evidence entry. These lines are evidence options, not instructions to change the prior conclusion.\n"
		}
	}
	completedSection := "\n\nValid completed example:\n" + string(completedJSON)
	if checkName == "published-dependency-capability-floor" && boolValue(input[neoRunCheckToolEvidenceRequired]) {
		completedSection = "\n\nNo copyable completed example is provided for this evidence-required repair. Rebuild each applicable entry only from the successful tool evidence above. If that evidence cannot support every required entry, return the failed-check object."
	}
	return fmt.Sprintf(`Your previous final result was rejected: %s

Return exactly one pure JSON object now. Do not use markdown fences, prose before or after the object, or tools.
%s
%s

For a failed check instead return exactly %s.
Completed results require a non-empty patternsChecked array, exactly one evidence entry for every pattern index, and every issue referenced by finding evidence. Use outcome finding with issueIndexes for a real issue, and include severity, file, line, endLine, problem, why, and fix in that issue. Use no-finding only when the cited evidence supports a clean conclusion. no-finding and not-applicable evidence must not reference issues. Dependency-floor evidence that is not not-applicable additionally requires dependency, floorVersion, accessPath, floorStatus, verification, and rootEvidence. Its patternsChecked entry must be exactly <dependency>@<floorVersion> <accessPath>, with no descriptive prefix or suffix. rootEvidence must be an array of exact, focused excerpts copied byte-for-byte from successful tool results; do not add file labels, line labels, separators, ellipses, trim source indentation, or paraphrase unless they occur in the result. Each excerpt's tool call must identify the exact dependency and floor, and each excerpt must contain at most 2048 valid UTF-8 bytes, counted as bytes rather than characters. Keep separate excerpts separate instead of concatenating output. Every selected excerpt must individually prove an access-path edge or the exact status for its evidence entry. Delete every excerpt named as rejected above before rebuilding the failed entry; occurrence in successful output does not make an irrelevant excerpt valid. For a source-proven missing edge, select either the complete owner declaration when it fits or a complete balanced owner-construction subsection such as the root-owned resource map; do not copy a whole package or source dump. Do not crop a class or interface before its matching closing brace, and do not crop a construction map needed to prove absence. A Python root-owned map is valid only when the successful producer output includes the matched enclosing root class; a standalone _sub_sdk_map output is ownerless even when the selected excerpt is balanced. For RootClient.resource.method, the root-owner excerpt must be scoped to RootClient and expose or omit resource; when compatibility is source/type-proven, a separate excerpt scoped to resource's owner must expose method. Map every selected excerpt to one exact adjacent edge or the exact full-path status assertion, and omit it when it maps to neither. Use accessPath RootClient.resource.method for a root-owned chain or module-specifier#Export.member for an imported owner, including the terminal member. Each sibling sync or async method requires its own pattern, evidence entry, full accessPath, and full-path status assertion, even when those entries reference one consolidated issue caused by the same missing root edge. Include only changed owning-client chains required by the check, not capabilities that merely appeared in broad tool output. Use floorStatus compatible with no-finding, or incompatible/unverified with finding. For a compatible traversal or exact test, quote <accessPath>=AVAILABLE rather than an informal status such as OK; otherwise quote focused owner and terminal-member excerpts that connect every path edge. A compatible source-construction or type-declaration entry still needs focused excerpts connecting every edge; its status line alone is insufficient, and a source/type predicate must not be labeled as a runtime traversal or exact behavior test. For incompatible or unverified, quote an exact traversal/test line <accessPath>=MISSING or <accessPath>=UNVERIFIED that was derived by the test, not unconditionally echoed. When source or type inspection proves the first root edge missing, select only the complete exact root-owner declaration or construction excerpt that omits that edge and the derived full-path MISSING assertion for each affected entry. Do not add a terminal class, method, imported-owner, sibling-owner, or other adjacent excerpt after the first root edge is proven absent: no later edge needs evidence, and every selected excerpt must remain relevant to that exact path. Do not select an earlier status assertion contradicted by a later successful correction; use the correction only when its producer is sound and explicitly resolves the earlier detector error, and otherwise return the error object. verification must be one of root-runtime-traversal, root-source-construction, root-type-declaration, exact-export-inspection, or exact-behavior-test. If the available successful output cannot support every required entry under these rules, return the error object instead of guessing or fabricating evidence. Bounded-artifact evidence that is not not-applicable requires phaseRelationship, budgetOrigin (original, remaining, or independent), and decisiveSequence. Generated-artifact finding evidence requires implementationOwner equal to the file of every referenced issue. Classifier evidence that is not not-applicable requires sourceLifetime (mutable, immutable, or unknown), decisionLifetime (per-use, retained, or unknown), mutationPath, and usePath.`, parseErr, groundedStatusAssertions, completedSection, errorJSON)
}
