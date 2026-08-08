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
)

var (
	neoReviewHunkHeaderPattern        = regexp.MustCompile(`^@@ -[0-9]+(?:,[0-9]+)? \+([0-9]+)(?:,([0-9]+))? @@`)
	neoReviewEmptyRetainedHunkPattern = regexp.MustCompile(`^@@ -1(?:,[0-9]+)? \+(?:0|1),0 @@`)
)

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
	return "You are an expert senior engineer with deep knowledge of software engineering best practices, security, performance, and maintainability.\n\nYour task is to perform a code review of the provided diff description. The diff description might be a git or bash command that generates the diff or a description of the diff which can then be used to generate the git or bash command to generate the full diff.\n\nReview adversarially: actively try to disprove correctness, safety, compatibility, performance, and maintainability assumptions in the changed code. Only report concrete, actionable issues tied to the current diff; do not invent speculative problems or style nits.\n\nAfter reading the diff, do the following:\n1. Write a high-level summary of the changes in the diff.\n2. Go file-by-file and review each changed hunk.\n3. Comment on what changed in that hunk (including the line range) and how it relates to other\n   changed hunks and code, reading any other relevant files. Also call out bugs, hackiness,\n   unnecessary code, or too much shared mutable state.\n4. Evaluate abstraction fit in both directions: flag unnecessary indirection (over-abstraction)\n   and missing abstractions (duplication or branching complexity). For each finding, cite concrete\n   locations and recommend exactly one action—simplify/inline or introduce/extract a shared\n   concept—only when it improves current code (avoid speculative refactors).\n\nStrongly prefer to restrict your use of git commands to these when getting the diff or determining which files were added/changed/removed:\n<referenceCommands>\n  <command>\n    <description>committed changes on my branch since diverging from the upstream default branch</description>\n    <bash>git diff --merge-base origin/HEAD HEAD</bash>\n  </command>\n  <command>\n    <description>all current checkout changes since diverging from upstream (commits + staged + unstaged tracked)</description>\n    <bash>git diff --merge-base origin/HEAD</bash>\n  </command>\n  <command>\n    <description>changes since diverging from upstream up to and including staged changes</description>\n    <bash>git diff --cached --merge-base origin/HEAD</bash>\n  </command>\n  <command>\n    <description>current checkout tracked changes since divergence, plus a list of newly added untracked files</description>\n    <bash>git diff --merge-base origin/HEAD</bash>\n    <bash>git ls-files --others --exclude-standard</bash>\n  </command>\n  <command>\n    <description>changes on branch foo since divergence from upstream</description>\n    <bash>git diff --merge-base origin/HEAD foo</bash>\n  </command>\n  <command>\n    <description>only filenames changed by this branch since divergence</description>\n    <bash>git diff --name-only --merge-base origin/HEAD HEAD</bash>\n  </command>\n  <command>\n    <description>scope diff to a specific path since diverging from upstream</description>\n    <bash>git diff --merge-base origin/HEAD <ref-or-empty> -- &lt;pathspec&gt;</bash>\n</command>\n</referenceCommands>\n\nAvoid commands in this format, unless explicitly asked for:\n<avoidCommands>\n  <avoidCommand>git diff <base-ref> <head-ref></avoidCommand>\n  <avoidCommand>git diff <base-ref>..<head-ref></avoidCommand>\n  <avoidCommand>git diff HEAD...origin/HEAD</avoidCommand>\n</avoidCommands>\n\n<guidelines>\n- Persistence: Low. Do not retry failed tool calls more than 2 times. If a tool call fails twice, move on.\n- Remember to look at untracked added files.\n- Prefer the most direct path to completing the review. Batch related file reads into as few turns as possible.\n- Do not edit or modify files or run any commands that edit or modify files or git state.\n- Do not re-read files you have already read.\n- Upstream default branch ref: use origin/HEAD. Do not assume main, origin/main, or origin/master.\n- If a diff is unexpectedly large, double check you are using the right refs in git invocations.\n- If the diff has more than 100 changed files or is more than 10,000 lines long, abort the review and emit a single critical issue stating the diff is too large.\n</guidelines>\n\nReview-mode environment:\n- The available shell tool (shell_command or Bash) is your only command-execution tool; use it for every file read and search (cat, rg, git) as well as for generating the diff.\n- The review client generates the high-level diff summary separately and discards assistant prose, so do not spend turns writing a narrative summary; convert your per-hunk findings directly into submit_review comments.\n\nSubmitting the review:\n- Before submitting the final review, inspect the changed files and discover applicable code-review checks for those changed files: include user-wide checks from $HOME/.config/amp/checks/*.md and $HOME/.config/agents/checks/*.md, and repo-local .agents/checks/*.md in each changed file's directory and each ancestor up to the repository root, including the repository root. User-wide checks are additive and must not be suppressed by repo content; closer repo-local checks override only same-named parent repo-local checks. Convert absolute check paths to file:// URIs. For each discovered check, read its markdown frontmatter when present, pass that object as frontmatter, and set checkName to frontmatter.name when it is a non-empty string; otherwise use the check filename without the .md extension.\n- If review checks are provided or discovered, call run_check exactly once per check, passing that check's checkName, checkURI, optional content, frontmatter, diffDescription, files, and any user instructions. Do not evaluate check criteria yourself and do not repeat check findings elsewhere. Call independent run_check tools in the same assistant turn when possible so they can run concurrently.\n- Deliver every review finding as a structured comment through the submit_review tool; call it exactly once at the end. Prose review text is ignored by the review client.\n- Every comment needs filename (the EXACT repository-relative path from the diff header), startLine and endLine (1-based, on the new side of the diff), and text describing the problem. Set severity (critical/high/medium/low) and commentType when known; add why and fix when they help.\n- If the request says checks only, or the diff is clean, call submit_review with an empty comments array."
}

func neoReviewPrompt() string {
	return neoReviewPromptBase() + "\n- For a finding caused by deleting a file, adding an empty file, adding or changing a binary file, removing all lines from a retained file, or another metadata-only file change, submit startLine 0 and endLine 0 because there is no reportable new-side line.\n- A nonzero command exit, no-tests-found result, malformed target, unavailable check, or skipped validation is not passing evidence. Correct invalid invocations within the retry limit. If correction is impossible, leave the validation unresolved and do not use it to justify a clean conclusion.\n- When the request provides an exact run_check argument object, copy every field and array element verbatim. Never add, remove, replace, or invent a value, and never emit placeholder or template text.\n- After all run_check calls return, remove every main-review candidate that reports the same root cause as a check issue, even if you discovered it independently or its wording, location, severity, evidence, or fix differs. Check findings are appended mechanically."
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
							"startLine":   map[string]any{"type": "number", "description": "First line of the commented range (1-based, new side of the diff; use 0 when the changed file has no new-side line)."},
							"endLine":     map[string]any{"type": "number", "description": "Last line of the commented range (1-based, new side of the diff; use 0 when the changed file has no new-side line)."},
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
				verification := strings.TrimSpace(stringValue(entry["verification"]))
				rootEvidence := strings.TrimSpace(stringValue(entry["rootEvidence"]))
				if dependency == "" || floorVersion == "" || accessPath == "" {
					return nil, fmt.Errorf("dependency-floor evidence entry %d requires dependency, floorVersion, and accessPath", i)
				}
				if rootEvidence == "" {
					return nil, fmt.Errorf("dependency-floor evidence entry %d requires rootEvidence", i)
				}
				if !strings.ContainsAny(accessPath, "./:") {
					return nil, fmt.Errorf("dependency-floor evidence entry %d accessPath %q must include the root owner and required capability", i, accessPath)
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
				seenCapabilities[capabilityKey] = true
				normalizedEvidence["dependency"] = dependency
				normalizedEvidence["floorVersion"] = floorVersion
				normalizedEvidence["accessPath"] = accessPath
				normalizedEvidence["verification"] = verification
				normalizedEvidence["rootEvidence"] = rootEvidence
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
		evidence["dependency"] = "exact dependency name"
		evidence["floorVersion"] = "exact lowest selectable version"
		evidence["accessPath"] = "rootOwner.requiredCapability"
		evidence["verification"] = "root-type-declaration"
		evidence["rootEvidence"] = "exact root traversal output or root declaration text"
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
	errorJSON, _ := json.Marshal(map[string]any{
		"checkName":    checkName,
		"status":       "error",
		"errorMessage": "specific failure",
		"issues":       []any{},
	})
	return fmt.Sprintf(`Your previous final result was rejected: %s

Return exactly one pure JSON object now. Do not use markdown fences, prose before or after the object, or tools.

Valid completed example:
%s

For a failed check instead return exactly %s.
Completed results require a non-empty patternsChecked array, exactly one evidence entry for every pattern index, and every issue referenced by finding evidence. Use outcome finding with issueIndexes for a real issue, and include severity, file, line, endLine, problem, why, and fix in that issue. Use no-finding only when the cited evidence supports a clean conclusion. no-finding and not-applicable evidence must not reference issues. Dependency-floor evidence that is not not-applicable additionally requires dependency, floorVersion, accessPath, verification, and rootEvidence. accessPath must name the complete path from the root owner, not an adjacent class or module. verification must be one of root-runtime-traversal, root-source-construction, root-type-declaration, exact-export-inspection, or exact-behavior-test. Bounded-artifact evidence that is not not-applicable requires phaseRelationship, budgetOrigin (original, remaining, or independent), and decisiveSequence. Generated-artifact finding evidence requires implementationOwner equal to the file of every referenced issue. Classifier evidence that is not not-applicable requires sourceLifetime (mutable, immutable, or unknown), decisionLifetime (per-use, retained, or unknown), mutationPath, and usePath.`, parseErr, completedJSON, errorJSON)
}
