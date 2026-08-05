package amp

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

const (
	defaultNeoGitHubAPIBaseURL = "https://api.github.com"
	defaultNeoGitHubRawBaseURL = "https://raw.githubusercontent.com"
	neoGitHubToolOutputLimit   = 120000
)

type neoGitHubTreeEntry struct {
	Path string `json:"path"`
	Type string `json:"type"`
	Size int64  `json:"size"`
	URL  string `json:"url"`
}

func isNeoGitHubTool(name string) bool {
	switch strings.TrimSpace(name) {
	case "read_github", "search_github", "commit_search", "diff", "list_directory_github", "list_repositories", "glob_github", "github_repo_ci_status":
		return true
	default:
		return false
	}
}

func neoGitHubToolSpec(toolName string) (neoToolSpec, bool) {
	name := strings.TrimSpace(toolName)
	commonRepo := map[string]any{"type": "string", "description": "GitHub repository as owner/repo or https://github.com/owner/repo."}
	commonRevision := map[string]any{"type": "string", "description": "Branch, tag, or commit SHA. Omit to use the repository default branch."}
	switch name {
	case "read_github":
		return neoToolSpec{
			Name:        name,
			Description: "Read a file from a GitHub repository.",
			InputSchema: neoGitHubToolSchema(map[string]any{
				"repository": commonRepo,
				"path":       map[string]any{"type": "string", "description": "File path inside the repository."},
				"revision":   commonRevision,
				"url":        map[string]any{"type": "string", "description": "GitHub blob URL to read."},
				"read_range": map[string]any{"type": "array", "items": map[string]any{"type": "integer"}, "description": "Optional [start, end] 1-based line range."},
				"startLine":  map[string]any{"type": "integer", "description": "Optional 1-based start line."},
				"endLine":    map[string]any{"type": "integer", "description": "Optional 1-based end line."},
			}, nil),
			Meta: map[string]any{"source": "server"},
		}, true
	case "list_directory_github":
		return neoToolSpec{
			Name:        name,
			Description: "List files and directories in a GitHub repository directory.",
			InputSchema: neoGitHubToolSchema(map[string]any{
				"repository": commonRepo,
				"path":       map[string]any{"type": "string", "description": "Directory path inside the repository."},
				"revision":   commonRevision,
				"url":        map[string]any{"type": "string", "description": "GitHub tree URL to list."},
				"limit":      map[string]any{"type": "integer", "description": "Maximum entries to return."},
			}, nil),
			Meta: map[string]any{"source": "server"},
		}, true
	case "search_github":
		return neoToolSpec{
			Name:        name,
			Description: "Search code in a GitHub repository.",
			InputSchema: neoGitHubToolSchema(map[string]any{
				"repository": commonRepo,
				"query":      map[string]any{"type": "string", "description": "Code search query."},
				"revision":   commonRevision,
				"path":       map[string]any{"type": "string", "description": "Optional path prefix to focus the search."},
				"limit":      map[string]any{"type": "integer", "description": "Maximum results to return."},
				"offset":     map[string]any{"type": "integer", "description": "Result offset. Must be divisible by limit."},
			}, []any{"query"}),
			Meta: map[string]any{"source": "server"},
		}, true
	case "glob_github":
		return neoToolSpec{
			Name:        name,
			Description: "Find paths in a GitHub repository matching a glob pattern.",
			InputSchema: neoGitHubToolSchema(map[string]any{
				"repository":  commonRepo,
				"pattern":     map[string]any{"type": "string", "description": "Glob pattern such as **/*.go."},
				"filePattern": map[string]any{"type": "string", "description": "Alias for pattern."},
				"revision":    commonRevision,
				"limit":       map[string]any{"type": "integer", "description": "Maximum paths to return."},
				"offset":      map[string]any{"type": "integer", "description": "Result offset. Must be divisible by limit."},
			}, []any{"pattern"}),
			Meta: map[string]any{"source": "server"},
		}, true
	case "commit_search":
		return neoToolSpec{
			Name:        name,
			Description: "Search commits in a GitHub repository.",
			InputSchema: neoGitHubToolSchema(map[string]any{
				"repository": commonRepo,
				"query":      map[string]any{"type": "string", "description": "Commit search query."},
				"author":     map[string]any{"type": "string", "description": "Optional commit author filter."},
				"since":      map[string]any{"type": "string", "description": "Optional start date filter."},
				"until":      map[string]any{"type": "string", "description": "Optional end date filter."},
				"path":       map[string]any{"type": "string", "description": "Optional path filter."},
				"limit":      map[string]any{"type": "integer", "description": "Maximum commits to return."},
				"offset":     map[string]any{"type": "integer", "description": "Result offset. Must be divisible by limit."},
			}, nil),
			Meta: map[string]any{"source": "server"},
		}, true
	case "diff":
		return neoToolSpec{
			Name:        name,
			Description: "Read a commit or compare diff from a GitHub repository.",
			InputSchema: neoGitHubToolSchema(map[string]any{
				"repository": commonRepo,
				"revision":   map[string]any{"type": "string", "description": "Commit SHA to diff."},
				"base":       map[string]any{"type": "string", "description": "Base branch, tag, or SHA for compare diffs."},
				"head":       map[string]any{"type": "string", "description": "Head branch, tag, or SHA for compare diffs."},
				"url":        map[string]any{"type": "string", "description": "GitHub commit, pull request, or compare URL."},
			}, nil),
			Meta: map[string]any{"source": "server"},
		}, true
	case "list_repositories":
		return neoToolSpec{
			Name:        name,
			Description: "List or search GitHub repositories.",
			InputSchema: neoGitHubToolSchema(map[string]any{
				"query": map[string]any{"type": "string", "description": "Repository search query."},
				"owner": map[string]any{"type": "string", "description": "GitHub user or organization."},
				"org":   map[string]any{"type": "string", "description": "GitHub organization."},
				"user":  map[string]any{"type": "string", "description": "GitHub user."},
			}, nil),
			Meta: map[string]any{"source": "server"},
		}, true
	case "github_repo_ci_status":
		return neoToolSpec{
			Name:        name,
			Description: "Read GitHub CI status for a repository branch, commit, or pull request.",
			InputSchema: neoGitHubToolSchema(map[string]any{
				"repository": commonRepo,
				"revision":   commonRevision,
				"ref":        commonRevision,
				"branch":     commonRevision,
				"commit":     map[string]any{"type": "string", "description": "Commit SHA to inspect."},
				"sha":        map[string]any{"type": "string", "description": "Commit SHA to inspect."},
				"url":        map[string]any{"type": "string", "description": "GitHub commit, branch, or pull request URL."},
			}, nil),
			Meta: map[string]any{"source": "server"},
		}, true
	default:
		return neoToolSpec{}, false
	}
}

func neoGitHubToolSchema(properties map[string]any, required []any) map[string]any {
	if required == nil {
		required = []any{}
	}
	return map[string]any{"type": "object", "properties": properties, "required": required}
}

func (a *neoActor) execSubagentLocalGitHubTool(ctx context.Context, call neoToolCall, parentToolCallID, parentMessageID string, generation int, clientAPIKey string) map[string]any {
	pending := neoPendingTool{
		ID:               call.ID,
		Name:             call.Name,
		Input:            call.Input,
		MessageID:        parentMessageID,
		ParentToolCallID: parentToolCallID,
		ClientAPIKey:     clientAPIKey,
	}
	a.mu.Lock()
	if generation != a.generation {
		a.mu.Unlock()
		return map[string]any{"status": "cancelled", "reason": "user:cancelled"}
	}
	if a.subagentTools == nil {
		a.subagentTools = map[string]neoPendingTool{}
	}
	a.subagentTools[call.ID] = pending
	a.mu.Unlock()

	if !a.storeSubagentToolResultMessageForGeneration(call.ID, map[string]any{"status": "in-progress", "progress": map[string]any{"output": neoGitHubProgressText(call.Name, call.Input)}}, parentToolCallID, "tool_progress", generation) {
		return map[string]any{"status": "cancelled", "reason": "user:cancelled"}
	}

	ctx = neoContextWithClientAPIKey(ctx, clientAPIKey)
	run := a.runLocalGitHubTool(ctx, call.Name, call.Input)
	a.mu.Lock()
	current, owned := a.subagentTools[call.ID]
	if generation == a.generation && owned && current.MessageID == parentMessageID && current.ParentToolCallID == parentToolCallID {
		delete(a.subagentTools, call.ID)
	}
	a.mu.Unlock()

	a.storeSubagentToolResultMessageForGeneration(call.ID, run, parentToolCallID, "", generation)
	return run
}

func (a *neoActor) runLocalGitHubActorTool(pending neoPendingTool, generation int) {
	if a.subagentGenerationStale(generation) {
		return
	}
	a.receiveToolResult(map[string]any{
		"type":       "executor_tool_result",
		"toolCallId": pending.ID,
		"run": map[string]any{
			"status":   "in-progress",
			"progress": map[string]any{"output": neoGitHubProgressText(pending.Name, pending.Input)},
		},
	})
	run := a.runLocalGitHubTool(neoContextWithClientAPIKey(context.Background(), pending.ClientAPIKey), pending.Name, pending.Input)
	if a.subagentGenerationStale(generation) {
		return
	}
	a.receiveToolResult(map[string]any{"type": "executor_tool_result", "toolCallId": pending.ID, "run": run})
}

func neoContextWithClientAPIKey(ctx context.Context, clientAPIKey string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if clientAPIKey = strings.TrimSpace(clientAPIKey); clientAPIKey != "" {
		return context.WithValue(ctx, clientAPIKeyContextKey{}, clientAPIKey)
	}
	return ctx
}

func neoGitHubProgressText(name string, input map[string]any) string {
	target := firstNonEmptyString(input["query"], input["pattern"], input["filePattern"], input["path"], input["repository"], input["url"])
	switch name {
	case "search_github", "commit_search", "glob_github":
		if target != "" {
			return "Searching GitHub: " + target
		}
		return "Searching GitHub"
	case "list_directory_github", "list_repositories":
		if target != "" {
			return "Listing GitHub content: " + target
		}
		return "Listing GitHub content"
	case "github_repo_ci_status":
		if target != "" {
			return "Reading CI status: " + target
		}
		return "Reading CI status"
	default:
		if target != "" {
			return "Reading GitHub content: " + target
		}
		return "Reading GitHub content"
	}
}

func (a *neoActor) runLocalGitHubTool(ctx context.Context, name string, input map[string]any) map[string]any {
	text, err := a.runtime.runGitHubTool(ctx, name, input)
	if err != nil {
		return map[string]any{"status": "error", "error": map[string]any{"message": err.Error()}}
	}
	return map[string]any{"status": "done", "output": strings.TrimSpace(text)}
}

func (rt *neoRuntime) runGitHubTool(ctx context.Context, name string, input map[string]any) (string, error) {
	switch name {
	case "read_github":
		return rt.runGitHubRead(ctx, input)
	case "list_directory_github":
		return rt.runGitHubListDirectory(ctx, input)
	case "search_github":
		return rt.runGitHubSearch(ctx, input)
	case "glob_github":
		return rt.runGitHubGlob(ctx, input)
	case "commit_search":
		return rt.runGitHubCommitSearch(ctx, input)
	case "diff":
		return rt.runGitHubDiff(ctx, input)
	case "list_repositories":
		return rt.runGitHubListRepositories(ctx, input)
	case "github_repo_ci_status":
		return rt.runGitHubRepoCIStatus(ctx, input)
	default:
		return "", fmt.Errorf("unsupported GitHub tool %q", name)
	}
}

func (rt *neoRuntime) runGitHubRead(ctx context.Context, input map[string]any) (string, error) {
	repo, filePath, revision := neoGitHubRepoPathRevision(input)
	if repo == "" || filePath == "" {
		return "", errors.New("read_github requires repository and path, or a GitHub blob URL")
	}
	body, err := rt.githubAPIGet(ctx, "/repos/"+repo+"/contents/"+neoGitHubEscapePath(filePath), neoGitHubRefQuery(revision), "application/vnd.github+json")
	if err != nil {
		return "", err
	}
	var decoded any
	if err := json.Unmarshal(body, &decoded); err != nil {
		return "", err
	}
	if entries, ok := decoded.([]any); ok {
		return neoGitHubFormatDirectory(entries), nil
	}
	m := mapValue(decoded)
	content := stringValue(m["content"])
	if content == "" && stringValue(m["download_url"]) != "" {
		raw, rawErr := rt.githubHTTPGet(ctx, stringValue(m["download_url"]), "text/plain")
		if rawErr != nil {
			return "", rawErr
		}
		content = string(raw)
	} else if strings.EqualFold(stringValue(m["encoding"]), "base64") {
		raw, decodeErr := base64.StdEncoding.DecodeString(strings.ReplaceAll(content, "\n", ""))
		if decodeErr != nil {
			return "", decodeErr
		}
		content = string(raw)
	}
	content = neoGitHubApplyLineRange(content, input)
	return neoLimitGitHubOutput(content), nil
}

func (rt *neoRuntime) runGitHubListDirectory(ctx context.Context, input map[string]any) (string, error) {
	repo, dirPath, revision := neoGitHubRepoPathRevision(input)
	if repo == "" {
		return "", errors.New("list_directory_github requires repository, or a GitHub tree URL")
	}
	body, err := rt.githubAPIGet(ctx, "/repos/"+repo+"/contents/"+neoGitHubEscapePath(dirPath), neoGitHubRefQuery(revision), "application/vnd.github+json")
	if err != nil {
		return "", err
	}
	var decoded any
	if err := json.Unmarshal(body, &decoded); err != nil {
		return "", err
	}
	if entries, ok := decoded.([]any); ok {
		return neoGitHubFormatDirectory(neoGitHubApplyLimit(entries, neoGitHubIntArg(input, "limit", 100))), nil
	}
	m := mapValue(decoded)
	if stringValue(m["type"]) == "file" {
		return fmt.Sprintf("file %s %s bytes", stringValue(m["path"]), neoGitHubNumber(m["size"])), nil
	}
	return string(body), nil
}

func (rt *neoRuntime) runGitHubGlob(ctx context.Context, input map[string]any) (string, error) {
	repo, _, revision := neoGitHubRepoPathRevision(input)
	pattern := firstNonEmptyString(input["pattern"], input["filePattern"], input["query"])
	if repo == "" || pattern == "" {
		return "", errors.New("glob_github requires repository and pattern")
	}
	limit, offset, err := neoGitHubLimitOffset(input, 100, 100)
	if err != nil {
		return "", err
	}
	entries, truncated, err := rt.githubTree(ctx, repo, revision)
	if err != nil {
		return "", err
	}
	matches := make([]string, 0)
	for _, entry := range entries {
		if entry.Type != "blob" {
			continue
		}
		if neoGitHubGlobMatches(pattern, entry.Path) {
			matches = append(matches, entry.Path)
		}
	}
	matches = neoGitHubSliceStrings(matches, limit, offset)
	if len(matches) == 0 {
		return "No matching paths found.", nil
	}
	out := strings.Join(matches, "\n")
	if truncated {
		out += "\n\n[GitHub returned a truncated tree; results may be incomplete.]"
	}
	return neoLimitGitHubOutput(out), nil
}

func (rt *neoRuntime) runGitHubSearch(ctx context.Context, input map[string]any) (string, error) {
	repo, _, revision := neoGitHubRepoPathRevision(input)
	query := strings.TrimSpace(firstNonEmptyString(input["query"], input["pattern"], input["objective"]))
	if query == "" {
		return "", errors.New("search_github requires query")
	}
	if repo == "" {
		repo = neoGitHubRepoFromSearchQuery(query)
	}
	if repo != "" {
		out, err := rt.githubCodeSearch(ctx, repo, query, input)
		if err == nil && strings.TrimSpace(out) != "" {
			return out, nil
		}
		if errors.Is(err, errNeoGitHubInvalidPagination) {
			return "", err
		}
	}
	if repo == "" {
		return "", errors.New("search_github requires repository when GitHub authenticated code search is unavailable")
	}
	return rt.githubTreeContentSearch(ctx, repo, revision, query, firstNonEmptyString(input["path"], input["directory"]), input)
}

func (rt *neoRuntime) runGitHubCommitSearch(ctx context.Context, input map[string]any) (string, error) {
	repo, _, _ := neoGitHubRepoPathRevision(input)
	query := strings.TrimSpace(firstNonEmptyString(input["query"], input["pattern"], input["objective"]))
	pathFilter := strings.Trim(strings.TrimSpace(stringValue(input["path"])), "/")
	if query == "" && pathFilter == "" {
		return "", errors.New("commit_search requires query")
	}
	if repo == "" {
		return "", errors.New("commit_search requires repository")
	}
	limit, offset, err := neoGitHubLimitOffset(input, 50, 100)
	if err != nil {
		return "", err
	}
	if pathFilter != "" {
		commits, err := rt.githubCommitsForPath(ctx, repo, pathFilter, query, input, limit, offset)
		if err != nil {
			return "", err
		}
		if len(commits) == 0 {
			return "No matching commits found.", nil
		}
		return neoGitHubFormatCommits(commits), nil
	}
	searchQuery := query
	if repo != "" && !strings.Contains(searchQuery, "repo:") {
		searchQuery = "repo:" + repo + " " + searchQuery
	}
	if author := strings.TrimSpace(stringValue(input["author"])); author != "" && !strings.Contains(searchQuery, "author:") {
		searchQuery += " author:" + author
	}
	if since := strings.TrimSpace(stringValue(input["since"])); since != "" && !strings.Contains(searchQuery, "author-date:") {
		searchQuery += " author-date:>=" + since
	}
	if until := strings.TrimSpace(stringValue(input["until"])); until != "" {
		searchQuery += " author-date:<=" + until
	}
	body, err := rt.githubAPIGet(ctx, "/search/commits", url.Values{"q": {searchQuery}, "per_page": {strconv.Itoa(limit)}, "page": {strconv.Itoa(offset/limit + 1)}, "sort": {"author-date"}, "order": {"desc"}}, "application/vnd.github.cloak-preview+json")
	if err != nil {
		return "", err
	}
	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		return "", err
	}
	items := arrayValue(decoded["items"])
	if len(items) == 0 {
		return "No matching commits found.", nil
	}
	return neoGitHubFormatCommits(items), nil
}

func (rt *neoRuntime) githubCommitsForPath(ctx context.Context, repo, pathFilter, query string, input map[string]any, limit, offset int) ([]any, error) {
	const perPage = 100
	wanted := offset + limit
	matches := make([]any, 0, wanted)
	for page := 1; page <= 10; page++ {
		params := url.Values{"per_page": {strconv.Itoa(perPage)}, "page": {strconv.Itoa(page)}, "path": {pathFilter}}
		if author := strings.TrimSpace(stringValue(input["author"])); author != "" {
			params.Set("author", author)
		}
		if since := strings.TrimSpace(stringValue(input["since"])); since != "" {
			params.Set("since", since)
		}
		if until := strings.TrimSpace(stringValue(input["until"])); until != "" {
			params.Set("until", until)
		}
		body, err := rt.githubAPIGet(ctx, "/repos/"+repo+"/commits", params, "application/vnd.github+json")
		if err != nil {
			return nil, err
		}
		var pageCommits []any
		if err := json.Unmarshal(body, &pageCommits); err != nil {
			return nil, err
		}
		filtered := pageCommits
		if query != "" {
			filtered = neoGitHubFilterCommits(pageCommits, query)
		}
		matches = append(matches, filtered...)
		if len(matches) >= wanted || len(pageCommits) < perPage {
			break
		}
	}
	return neoGitHubSliceAny(matches, limit, offset), nil
}

func (rt *neoRuntime) runGitHubDiff(ctx context.Context, input map[string]any) (string, error) {
	repo, _, revision := neoGitHubRepoPathRevision(input)
	base := strings.TrimSpace(stringValue(input["base"]))
	head := strings.TrimSpace(firstNonEmptyString(input["head"], input["to"]))
	pull := ""
	if urlRepo, urlBase, urlHead, urlRevision, urlPull := neoGitHubParseDiffURL(stringValue(input["url"])); urlRepo != "" {
		if repo == "" {
			repo = urlRepo
		}
		if base == "" {
			base = urlBase
		}
		if head == "" {
			head = urlHead
		}
		if revision == "" {
			revision = urlRevision
		}
		pull = urlPull
	}
	if repo == "" {
		return "", errors.New("diff requires repository")
	}
	if pull != "" {
		body, err := rt.githubAPIGet(ctx, "/repos/"+repo+"/pulls/"+url.PathEscape(pull), nil, "application/vnd.github.diff")
		if err != nil {
			return "", err
		}
		return neoLimitGitHubOutput(string(body)), nil
	}
	if base == "" && head == "" && strings.Contains(revision, "...") {
		parts := strings.SplitN(revision, "...", 2)
		base = parts[0]
		head = parts[1]
		revision = ""
	}
	if base != "" && head != "" {
		body, err := rt.githubAPIGet(ctx, "/repos/"+repo+"/compare/"+url.PathEscape(base)+"..."+url.PathEscape(head), nil, "application/vnd.github.diff")
		if err != nil {
			return "", err
		}
		return neoLimitGitHubOutput(string(body)), nil
	}
	if revision == "" {
		revision = firstNonEmptyString(input["commit"], input["sha"])
	}
	if revision == "" {
		return "", errors.New("diff requires revision, commit, or base/head")
	}
	body, err := rt.githubAPIGet(ctx, "/repos/"+repo+"/commits/"+url.PathEscape(revision), nil, "application/vnd.github.diff")
	if err != nil {
		return "", err
	}
	return neoLimitGitHubOutput(string(body)), nil
}

func (rt *neoRuntime) runGitHubListRepositories(ctx context.Context, input map[string]any) (string, error) {
	query := strings.TrimSpace(firstNonEmptyString(input["query"], input["q"]))
	owner := strings.TrimSpace(firstNonEmptyString(input["org"], input["owner"], input["user"]))
	if query != "" {
		body, err := rt.githubAPIGet(ctx, "/search/repositories", url.Values{"q": {query}, "per_page": {"20"}}, "application/vnd.github+json")
		if err != nil {
			return "", err
		}
		var decoded map[string]any
		if err := json.Unmarshal(body, &decoded); err != nil {
			return "", err
		}
		return neoGitHubFormatRepositories(arrayValue(decoded["items"])), nil
	}
	if owner == "" {
		return "", errors.New("list_repositories requires query or owner")
	}
	body, err := rt.githubAPIGet(ctx, "/orgs/"+url.PathEscape(owner)+"/repos", url.Values{"per_page": {"100"}}, "application/vnd.github+json")
	if err != nil {
		body, err = rt.githubAPIGet(ctx, "/users/"+url.PathEscape(owner)+"/repos", url.Values{"per_page": {"100"}}, "application/vnd.github+json")
	}
	if err != nil {
		return "", err
	}
	var decoded []any
	if err := json.Unmarshal(body, &decoded); err != nil {
		return "", err
	}
	return neoGitHubFormatRepositories(decoded), nil
}

func (rt *neoRuntime) runGitHubRepoCIStatus(ctx context.Context, input map[string]any) (string, error) {
	repo, _, revision := neoGitHubRepoPathRevision(input)
	if urlRepo, _, _, urlRevision, urlPull := neoGitHubParseDiffURL(stringValue(input["url"])); urlRepo != "" {
		if repo == "" {
			repo = urlRepo
		}
		if revision == "" {
			revision = urlRevision
		}
		if revision == "" && urlPull != "" {
			pullBody, err := rt.githubAPIGet(ctx, "/repos/"+repo+"/pulls/"+url.PathEscape(urlPull), nil, "application/vnd.github+json")
			if err != nil {
				return "", err
			}
			var pull map[string]any
			if err := json.Unmarshal(pullBody, &pull); err != nil {
				return "", err
			}
			revision = strings.TrimSpace(stringValue(mapValue(pull["head"])["sha"]))
		}
	}
	if repo == "" {
		return "", errors.New("github_repo_ci_status requires repository")
	}
	if revision == "" {
		revision = firstNonEmptyString(input["ref"], input["branch"], input["commit"], input["sha"], input["head"])
	}
	if revision == "" {
		var err error
		revision, err = rt.githubDefaultBranch(ctx, repo)
		if err != nil {
			return "", err
		}
	}

	var checkRuns []any
	checksBody, checksErr := rt.githubAPIGet(ctx, "/repos/"+repo+"/commits/"+url.PathEscape(revision)+"/check-runs", url.Values{"per_page": {"100"}}, "application/vnd.github+json")
	if checksErr == nil {
		var checks map[string]any
		if err := json.Unmarshal(checksBody, &checks); err != nil {
			return "", err
		}
		checkRuns = arrayValue(checks["check_runs"])
	}

	var statuses []any
	combinedState := ""
	statusBody, statusErr := rt.githubAPIGet(ctx, "/repos/"+repo+"/commits/"+url.PathEscape(revision)+"/status", nil, "application/vnd.github+json")
	if statusErr == nil {
		var status map[string]any
		if err := json.Unmarshal(statusBody, &status); err != nil {
			return "", err
		}
		combinedState = stringValue(status["state"])
		statuses = arrayValue(status["statuses"])
	}
	if checksErr != nil && statusErr != nil {
		return "", fmt.Errorf("failed to read GitHub CI status: checks: %v; statuses: %v", checksErr, statusErr)
	}
	return neoGitHubFormatCIStatus(repo, revision, combinedState, checkRuns, statuses), nil
}

func (rt *neoRuntime) githubCodeSearch(ctx context.Context, repo, query string, input map[string]any) (string, error) {
	limit, offset, err := neoGitHubLimitOffset(input, 30, 100)
	if err != nil {
		return "", err
	}
	searchQuery := query
	if repo != "" && !strings.Contains(searchQuery, "repo:") {
		searchQuery = "repo:" + repo + " " + searchQuery
	}
	if pathFilter := strings.Trim(strings.TrimSpace(firstNonEmptyString(input["path"], input["directory"])), "/"); pathFilter != "" && pathFilter != "." && !strings.Contains(searchQuery, "path:") {
		searchQuery += " path:" + pathFilter
	}
	body, err := rt.githubAPIGet(ctx, "/search/code", url.Values{"q": {searchQuery}, "per_page": {strconv.Itoa(limit)}, "page": {strconv.Itoa(offset/limit + 1)}}, "application/vnd.github.text-match+json")
	if err != nil {
		return "", err
	}
	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		return "", err
	}
	items := arrayValue(decoded["items"])
	if len(items) == 0 {
		return "No matching code found.", nil
	}
	lines := make([]string, 0, len(items))
	for _, item := range items {
		m := mapValue(item)
		repository := mapValue(m["repository"])
		lines = append(lines, fmt.Sprintf("%s %s", firstNonEmptyString(repository["full_name"], repo), stringValue(m["path"])))
		for _, match := range arrayValue(m["text_matches"]) {
			fragment := strings.TrimSpace(stringValue(mapValue(match)["fragment"]))
			if fragment != "" {
				lines = append(lines, "  "+strings.ReplaceAll(fragment, "\n", "\n  "))
			}
		}
	}
	return neoLimitGitHubOutput(strings.Join(lines, "\n")), nil
}

func (rt *neoRuntime) githubTreeContentSearch(ctx context.Context, repo, revision, query, pathPrefix string, input map[string]any) (string, error) {
	limit, offset, err := neoGitHubLimitOffset(input, 40, 100)
	if err != nil {
		return "", err
	}
	entries, truncated, err := rt.githubTree(ctx, repo, revision)
	if err != nil {
		return "", err
	}
	if revision == "" {
		revision, _ = rt.githubDefaultBranch(ctx, repo)
	}
	tokens := neoGitHubSearchTokens(query)
	candidates := neoGitHubRankTreeEntries(entries, tokens, pathPrefix)
	matches := make([]string, 0)
	pathMatches := make([]string, 0)
	maxWanted := offset + limit
	for _, entry := range candidates {
		if len(pathMatches) < maxWanted {
			pathMatches = append(pathMatches, entry.Path)
		}
		if len(matches) >= maxWanted {
			break
		}
		if entry.Size > 300000 || !neoGitHubLikelyTextPath(entry.Path) {
			continue
		}
		content, readErr := rt.githubRawFile(ctx, repo, revision, entry.Path)
		if readErr != nil {
			continue
		}
		for _, line := range neoGitHubMatchingLines(content, tokens, 4) {
			matches = append(matches, entry.Path+":"+line)
			if len(matches) >= maxWanted {
				break
			}
		}
	}
	matches = neoGitHubSliceStrings(matches, limit, offset)
	pathMatches = neoGitHubSliceStrings(pathMatches, limit, offset)
	if len(matches) == 0 && len(pathMatches) > 0 {
		out := "Path matches:\n" + strings.Join(pathMatches, "\n")
		if truncated {
			out += "\n\n[GitHub returned a truncated tree; results may be incomplete.]"
		}
		return neoLimitGitHubOutput(out), nil
	}
	if len(matches) == 0 {
		return "No matching code found.", nil
	}
	out := strings.Join(matches, "\n")
	if truncated {
		out += "\n\n[GitHub returned a truncated tree; results may be incomplete.]"
	}
	return neoLimitGitHubOutput(out), nil
}

func (rt *neoRuntime) githubTree(ctx context.Context, repo, revision string) ([]neoGitHubTreeEntry, bool, error) {
	ref := strings.TrimSpace(revision)
	if ref == "" {
		var err error
		ref, err = rt.githubDefaultBranch(ctx, repo)
		if err != nil {
			return nil, false, err
		}
	}
	body, err := rt.githubAPIGet(ctx, "/repos/"+repo+"/git/trees/"+url.PathEscape(ref), url.Values{"recursive": {"1"}}, "application/vnd.github+json")
	if err != nil {
		return nil, false, err
	}
	var decoded struct {
		Tree      []neoGitHubTreeEntry `json:"tree"`
		Truncated bool                 `json:"truncated"`
	}
	if err := json.Unmarshal(body, &decoded); err != nil {
		return nil, false, err
	}
	return decoded.Tree, decoded.Truncated, nil
}

func (rt *neoRuntime) githubDefaultBranch(ctx context.Context, repo string) (string, error) {
	body, err := rt.githubAPIGet(ctx, "/repos/"+repo, nil, "application/vnd.github+json")
	if err != nil {
		return "", err
	}
	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		return "", err
	}
	branch := strings.TrimSpace(stringValue(decoded["default_branch"]))
	if branch == "" {
		return "main", nil
	}
	return branch, nil
}

func (rt *neoRuntime) githubRawFile(ctx context.Context, repo, revision, filePath string) (string, error) {
	if strings.TrimSpace(revision) == "" {
		var err error
		revision, err = rt.githubDefaultBranch(ctx, repo)
		if err != nil {
			return "", err
		}
	}
	if content, err := rt.githubContentFile(ctx, repo, revision, filePath); err == nil {
		return content, nil
	}
	u := strings.TrimRight(rt.githubRawBaseURL(), "/") + "/" + repo + "/" + neoGitHubEscapePath(revision) + "/" + neoGitHubEscapePath(filePath)
	body, err := rt.githubHTTPGet(ctx, u, "text/plain")
	if err != nil {
		return "", err
	}
	return string(body), nil
}

func (rt *neoRuntime) githubContentFile(ctx context.Context, repo, revision, filePath string) (string, error) {
	body, err := rt.githubAPIGet(ctx, "/repos/"+repo+"/contents/"+neoGitHubEscapePath(filePath), neoGitHubRefQuery(revision), "application/vnd.github+json")
	if err != nil {
		return "", err
	}
	var decoded any
	if err := json.Unmarshal(body, &decoded); err != nil {
		return "", err
	}
	if _, ok := decoded.([]any); ok {
		return "", errors.New("GitHub path is a directory")
	}
	m := mapValue(decoded)
	content := stringValue(m["content"])
	if content != "" && strings.EqualFold(stringValue(m["encoding"]), "base64") {
		raw, decodeErr := base64.StdEncoding.DecodeString(strings.ReplaceAll(content, "\n", ""))
		if decodeErr != nil {
			return "", decodeErr
		}
		return string(raw), nil
	}
	if content != "" {
		return content, nil
	}
	return "", errors.New("GitHub content response did not include file content")
}

func (rt *neoRuntime) githubAPIGet(ctx context.Context, path string, query url.Values, accept string) ([]byte, error) {
	body, err := rt.githubProxyAPIGet(ctx, path, query, accept)
	if err == nil {
		return body, nil
	}
	if !errors.Is(err, errNeoGitHubProxyUnavailable) {
		return nil, err
	}
	u := strings.TrimRight(rt.githubAPIBaseURL(), "/") + "/" + strings.TrimLeft(path, "/")
	if query != nil {
		if strings.Contains(u, "?") {
			u += "&" + query.Encode()
		} else {
			u += "?" + query.Encode()
		}
	}
	return rt.githubHTTPGet(ctx, u, accept)
}

func (rt *neoRuntime) githubProxyAPIGet(ctx context.Context, path string, query url.Values, accept string) ([]byte, error) {
	if rt == nil || strings.TrimSpace(rt.githubAPIBase) != "" {
		return nil, errNeoGitHubProxyUnavailable
	}
	cfg := rt.configSnapshot()
	if cfg == nil || strings.TrimSpace(cfg.AmpCode.UpstreamURL) == "" {
		return nil, errNeoGitHubProxyUnavailable
	}
	apiKey, err := rt.githubProxyAPIKey(ctx, cfg.AmpCode.UpstreamAPIKey)
	if err != nil {
		return nil, err
	}
	if apiKey == "" {
		return nil, errNeoGitHubProxyUnavailable
	}
	clientVersion := ampUpstreamClientVersion(&cfg.AmpCode)
	authURL := strings.TrimRight(strings.TrimSpace(cfg.AmpCode.UpstreamURL), "/") + "/api/internal/github-auth-status"
	authBody, err := rt.githubProxyHTTPGet(ctx, authURL, "application/json", apiKey, clientVersion)
	if err != nil {
		return nil, err
	}
	var authStatus struct {
		Authenticated bool `json:"authenticated"`
	}
	if err := json.Unmarshal(authBody, &authStatus); err != nil {
		return nil, err
	}
	if !authStatus.Authenticated {
		return nil, errors.New("GitHub proxy is not authenticated")
	}
	u := strings.TrimRight(strings.TrimSpace(cfg.AmpCode.UpstreamURL), "/") + "/api/internal/github-proxy/" + strings.TrimLeft(path, "/")
	if query != nil {
		if strings.Contains(u, "?") {
			u += "&" + query.Encode()
		} else {
			u += "?" + query.Encode()
		}
	}
	return rt.githubProxyHTTPGet(ctx, u, accept, apiKey, clientVersion)
}

func (rt *neoRuntime) githubProxyAPIKey(ctx context.Context, fallback string) (string, error) {
	if rt != nil {
		if source := rt.getSecretSource(); source != nil {
			key, err := source.Get(ctx)
			if err != nil {
				return "", err
			}
			if strings.TrimSpace(key) != "" {
				return strings.TrimSpace(key), nil
			}
		}
	}
	return strings.TrimSpace(fallback), nil
}

func (rt *neoRuntime) githubProxyHTTPGet(ctx context.Context, rawURL, accept, apiKey, clientVersion string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "CLIProxyAPI-Amp-Neo")
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("X-Api-Key", apiKey)
	if accept != "" {
		req.Header.Set("Accept", accept)
	}
	setAmpInternalClientHeaders(req, clientVersion)
	client := rt.githubHTTPClient()
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 10*1024*1024))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("GitHub proxy returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return body, nil
}

func (rt *neoRuntime) githubHTTPGet(ctx context.Context, rawURL, accept string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "CLIProxyAPI-Amp-Neo")
	if accept != "" {
		req.Header.Set("Accept", accept)
	}
	if token := neoGitHubToken(); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	client := rt.githubHTTPClient()
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 10*1024*1024))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("GitHub returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return body, nil
}

func (rt *neoRuntime) githubHTTPClient() *http.Client {
	if rt != nil && rt.githubClient != nil {
		return rt.githubClient
	}
	return http.DefaultClient
}

func (rt *neoRuntime) githubAPIBaseURL() string {
	if rt != nil && strings.TrimSpace(rt.githubAPIBase) != "" {
		return strings.TrimSpace(rt.githubAPIBase)
	}
	return defaultNeoGitHubAPIBaseURL
}

func (rt *neoRuntime) githubRawBaseURL() string {
	if rt != nil && strings.TrimSpace(rt.githubRawBase) != "" {
		return strings.TrimSpace(rt.githubRawBase)
	}
	return defaultNeoGitHubRawBaseURL
}

func neoGitHubToken() string {
	return strings.TrimSpace(firstNonEmptyString(os.Getenv("GITHUB_TOKEN"), os.Getenv("GH_TOKEN")))
}

func neoGitHubRepoPathRevision(input map[string]any) (string, string, string) {
	repo := strings.TrimSpace(firstNonEmptyString(input["repository"], input["repo"], input["repositoryURL"], input["repositoryUrl"]))
	filePath := strings.TrimSpace(firstNonEmptyString(input["path"], input["filePath"], input["filepath"]))
	revision := strings.TrimSpace(firstNonEmptyString(input["revision"], input["ref"], input["branch"], input["commit"], input["sha"]))
	if u := strings.TrimSpace(stringValue(input["url"])); u != "" {
		urlRepo, urlPath, urlRev := neoGitHubParseURL(u)
		if repo == "" {
			repo = urlRepo
		}
		if filePath == "" {
			filePath = urlPath
		}
		if revision == "" {
			revision = urlRev
		}
	}
	repo = neoGitHubNormalizeRepo(repo)
	return repo, strings.TrimPrefix(filePath, "/"), revision
}

func neoGitHubParseURL(raw string) (string, string, string) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", "", ""
	}
	host := strings.TrimPrefix(strings.ToLower(u.Host), "www.")
	if host != "github.com" {
		return "", "", ""
	}
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(parts) < 2 {
		return "", "", ""
	}
	repo := parts[0] + "/" + strings.TrimSuffix(parts[1], ".git")
	if len(parts) >= 5 && (parts[2] == "blob" || parts[2] == "tree") {
		return repo, strings.Join(parts[4:], "/"), parts[3]
	}
	if len(parts) >= 4 && parts[2] == "commit" {
		return repo, "", parts[3]
	}
	if len(parts) >= 4 && parts[2] == "compare" {
		return repo, "", parts[3]
	}
	return repo, "", ""
}

func neoGitHubParseDiffURL(raw string) (repo, base, head, revision, pull string) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", "", "", "", ""
	}
	host := strings.TrimPrefix(strings.ToLower(u.Host), "www.")
	if host != "github.com" {
		return "", "", "", "", ""
	}
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(parts) < 3 {
		return "", "", "", "", ""
	}
	repo = parts[0] + "/" + strings.TrimSuffix(parts[1], ".git")
	switch parts[2] {
	case "compare":
		if len(parts) >= 4 {
			compare := strings.Join(parts[3:], "/")
			if compareParts := strings.SplitN(compare, "...", 2); len(compareParts) == 2 {
				return repo, compareParts[0], compareParts[1], "", ""
			}
			return repo, "", "", compare, ""
		}
	case "commit":
		if len(parts) >= 4 {
			return repo, "", "", parts[3], ""
		}
	case "pull":
		if len(parts) >= 4 {
			return repo, "", "", "", parts[3]
		}
	}
	return repo, "", "", "", ""
}

func neoGitHubNormalizeRepo(repo string) string {
	repo = strings.TrimSpace(repo)
	repo = strings.TrimSuffix(repo, ".git")
	repo = strings.TrimPrefix(repo, "git@github.com:")
	if strings.HasPrefix(repo, "http://") || strings.HasPrefix(repo, "https://") {
		parsed, err := url.Parse(repo)
		if err == nil && strings.TrimPrefix(strings.ToLower(parsed.Host), "www.") == "github.com" {
			parts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
			if len(parts) >= 2 {
				return parts[0] + "/" + strings.TrimSuffix(parts[1], ".git")
			}
		}
	}
	parts := strings.Split(strings.Trim(repo, "/"), "/")
	if len(parts) >= 2 {
		return parts[0] + "/" + parts[1]
	}
	return ""
}

func neoGitHubRepoFromSearchQuery(query string) string {
	re := regexp.MustCompile(`(?:^|\s)repo:([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)`)
	match := re.FindStringSubmatch(query)
	if len(match) == 2 {
		return match[1]
	}
	return ""
}

func neoGitHubRefQuery(revision string) url.Values {
	if strings.TrimSpace(revision) == "" {
		return nil
	}
	return url.Values{"ref": {strings.TrimSpace(revision)}}
}

func neoGitHubEscapePath(value string) string {
	parts := strings.Split(strings.Trim(value, "/"), "/")
	for i, part := range parts {
		parts[i] = url.PathEscape(part)
	}
	return strings.Join(parts, "/")
}

func neoGitHubFormatDirectory(entries []any) string {
	lines := make([]string, 0, len(entries))
	for _, entry := range entries {
		m := mapValue(entry)
		line := strings.TrimSpace(strings.Join([]string{stringValue(m["type"]), stringValue(m["path"]), neoGitHubNumber(m["size"])}, " "))
		lines = append(lines, line)
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}

var (
	errNeoGitHubInvalidPagination = errors.New("invalid GitHub pagination")
	errNeoGitHubProxyUnavailable  = errors.New("GitHub proxy unavailable")
)

func neoGitHubIntArg(input map[string]any, key string, fallback int) int {
	value := numberFrom(input[key])
	if value <= 0 {
		return fallback
	}
	return value
}

func neoGitHubLimitOffset(input map[string]any, fallbackLimit, maxLimit int) (int, int, error) {
	limit := neoGitHubIntArg(input, "limit", fallbackLimit)
	if maxLimit > 0 && limit > maxLimit {
		limit = maxLimit
	}
	offset := numberFrom(input["offset"])
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 {
		limit = fallbackLimit
	}
	if offset%limit != 0 {
		return 0, 0, fmt.Errorf("%w: offset (%d) must be divisible by limit (%d)", errNeoGitHubInvalidPagination, offset, limit)
	}
	return limit, offset, nil
}

func neoGitHubApplyLimit(items []any, limit int) []any {
	if limit <= 0 || len(items) <= limit {
		return items
	}
	return items[:limit]
}

func neoGitHubSliceStrings(items []string, limit, offset int) []string {
	if offset >= len(items) {
		return nil
	}
	end := offset + limit
	if limit <= 0 || end > len(items) {
		end = len(items)
	}
	return items[offset:end]
}

func neoGitHubSliceAny(items []any, limit, offset int) []any {
	if offset >= len(items) {
		return nil
	}
	end := offset + limit
	if limit <= 0 || end > len(items) {
		end = len(items)
	}
	return items[offset:end]
}

func neoGitHubFilterCommits(items []any, query string) []any {
	query = strings.ToLower(strings.TrimSpace(query))
	if query == "" {
		return items
	}
	filtered := make([]any, 0, len(items))
	for _, item := range items {
		commit := mapValue(mapValue(item)["commit"])
		message := strings.ToLower(stringValue(commit["message"]))
		author := mapValue(commit["author"])
		if strings.Contains(message, query) || strings.Contains(strings.ToLower(stringValue(author["name"])), query) || strings.Contains(strings.ToLower(stringValue(author["email"])), query) {
			filtered = append(filtered, item)
		}
	}
	return filtered
}

func neoGitHubFormatCommits(items []any) string {
	lines := make([]string, 0, len(items))
	for _, item := range items {
		m := mapValue(item)
		commit := mapValue(m["commit"])
		message := strings.Split(strings.TrimSpace(stringValue(commit["message"])), "\n")[0]
		sha := stringValue(m["sha"])
		if len(sha) > 12 {
			sha = sha[:12]
		}
		lines = append(lines, strings.TrimSpace(strings.Join([]string{sha, message, stringValue(m["html_url"])}, " ")))
	}
	return neoLimitGitHubOutput(strings.Join(lines, "\n"))
}

func neoGitHubFormatRepositories(items []any) string {
	if len(items) == 0 {
		return "No repositories found."
	}
	lines := make([]string, 0, len(items))
	for _, item := range items {
		m := mapValue(item)
		lines = append(lines, strings.TrimSpace(strings.Join([]string{stringValue(m["full_name"]), stringValue(m["html_url"]), stringValue(m["description"])}, " - ")))
	}
	return neoLimitGitHubOutput(strings.Join(lines, "\n"))
}

func neoGitHubFormatCIStatus(repo, revision, combinedState string, checkRuns, statuses []any) string {
	lines := []string{"Repository: " + repo, "Ref: " + revision}
	if combinedState != "" {
		lines = append(lines, "Overall status: "+combinedState)
	}
	if len(checkRuns) > 0 {
		lines = append(lines, "", "Check runs:")
		for _, item := range checkRuns {
			m := mapValue(item)
			name := firstNonEmptyString(m["name"], m["app"], "check")
			status := stringValue(m["status"])
			conclusion := stringValue(m["conclusion"])
			state := strings.Trim(strings.TrimSpace(status+"/"+conclusion), "/")
			line := "- " + name
			if state != "" {
				line += ": " + state
			}
			if link := firstNonEmptyString(m["html_url"], m["details_url"]); link != "" {
				line += " " + link
			}
			lines = append(lines, line)
		}
	}
	if len(statuses) > 0 {
		lines = append(lines, "", "Commit statuses:")
		for _, item := range statuses {
			m := mapValue(item)
			contextName := firstNonEmptyString(m["context"], m["name"], "status")
			line := "- " + contextName
			if state := stringValue(m["state"]); state != "" {
				line += ": " + state
			}
			if description := stringValue(m["description"]); description != "" {
				line += " - " + description
			}
			if link := firstNonEmptyString(m["target_url"], m["url"]); link != "" {
				line += " " + link
			}
			lines = append(lines, line)
		}
	}
	if len(checkRuns) == 0 && len(statuses) == 0 {
		lines = append(lines, "No CI status found.")
	}
	return neoLimitGitHubOutput(strings.Join(lines, "\n"))
}

func neoGitHubNumber(value any) string {
	switch v := value.(type) {
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case float64:
		return strconv.FormatInt(int64(v), 10)
	case json.Number:
		return v.String()
	default:
		return ""
	}
}

func neoGitHubApplyLineRange(content string, input map[string]any) string {
	start := numberFrom(input["startLine"], input["lineStart"], input["start_line"])
	end := numberFrom(input["endLine"], input["lineEnd"], input["end_line"])
	if readRange := arrayValue(input["read_range"]); len(readRange) >= 2 {
		start = numberFrom(readRange[0])
		end = numberFrom(readRange[1])
	}
	if start <= 0 && end <= 0 {
		return content
	}
	lines := strings.Split(content, "\n")
	if start <= 0 {
		start = 1
	}
	if end <= 0 || end > len(lines) {
		end = len(lines)
	}
	if start > end || start > len(lines) {
		return ""
	}
	return strings.Join(lines[start-1:end], "\n")
}

func neoGitHubSearchTokens(query string) []string {
	re := regexp.MustCompile(`[A-Za-z0-9]+`)
	raw := re.FindAllString(query, -1)
	seen := map[string]bool{}
	out := make([]string, 0, len(raw))
	for _, token := range raw {
		for _, part := range neoGitHubSplitToken(token) {
			part = strings.ToLower(part)
			if len(part) < 3 || seen[part] || part == "repo" {
				continue
			}
			seen[part] = true
			out = append(out, part)
		}
	}
	return out
}

func neoGitHubSplitToken(token string) []string {
	withSpaces := regexp.MustCompile(`([a-z0-9])([A-Z])`).ReplaceAllString(token, `$1 $2`)
	return strings.Fields(withSpaces)
}

func neoGitHubRankTreeEntries(entries []neoGitHubTreeEntry, tokens []string, pathPrefix string) []neoGitHubTreeEntry {
	type scored struct {
		entry neoGitHubTreeEntry
		score int
	}
	scoredEntries := make([]scored, 0, len(entries))
	prefix := strings.Trim(strings.ToLower(pathPrefix), "/")
	for _, entry := range entries {
		if entry.Type != "blob" {
			continue
		}
		lowerPath := strings.ToLower(entry.Path)
		if prefix != "" && !strings.HasPrefix(lowerPath, prefix) {
			continue
		}
		score := 0
		for _, token := range tokens {
			if strings.Contains(lowerPath, token) {
				score += 4
			}
		}
		if neoGitHubLikelyTextPath(entry.Path) {
			score++
		}
		if score > 0 {
			scoredEntries = append(scoredEntries, scored{entry: entry, score: score})
		}
	}
	sort.Slice(scoredEntries, func(i, j int) bool {
		if scoredEntries[i].score != scoredEntries[j].score {
			return scoredEntries[i].score > scoredEntries[j].score
		}
		return scoredEntries[i].entry.Path < scoredEntries[j].entry.Path
	})
	limit := len(scoredEntries)
	if limit > 160 {
		limit = 160
	}
	out := make([]neoGitHubTreeEntry, 0, limit)
	for i := 0; i < limit; i++ {
		out = append(out, scoredEntries[i].entry)
	}
	return out
}

func neoGitHubMatchingLines(content string, tokens []string, maxPerFile int) []string {
	if len(tokens) == 0 {
		return nil
	}
	lines := strings.Split(content, "\n")
	out := make([]string, 0, maxPerFile)
	for i, line := range lines {
		lower := strings.ToLower(line)
		score := 0
		for _, token := range tokens {
			if strings.Contains(lower, token) {
				score++
			}
		}
		if score == 0 {
			continue
		}
		out = append(out, fmt.Sprintf("L%d: %s", i+1, strings.TrimSpace(line)))
		if len(out) >= maxPerFile {
			break
		}
	}
	return out
}

func neoGitHubLikelyTextPath(pathValue string) bool {
	lower := strings.ToLower(pathValue)
	for _, suffix := range []string{".go", ".ts", ".tsx", ".js", ".jsx", ".py", ".rs", ".java", ".kt", ".rb", ".php", ".c", ".cc", ".cpp", ".h", ".hpp", ".md", ".txt", ".yaml", ".yml", ".json", ".toml", ".sh"} {
		if strings.HasSuffix(lower, suffix) {
			return true
		}
	}
	return !strings.Contains(pathValue, ".")
}

func neoGitHubGlobMatches(pattern, value string) bool {
	re := regexp.QuoteMeta(strings.TrimSpace(pattern))
	re = strings.ReplaceAll(re, `\*\*`, `.*`)
	re = strings.ReplaceAll(re, `\*`, `[^/]*`)
	re = strings.ReplaceAll(re, `\?`, `[^/]`)
	ok, err := regexp.MatchString("^"+re+"$", value)
	return err == nil && ok
}

func neoLimitGitHubOutput(text string) string {
	if len(text) <= neoGitHubToolOutputLimit {
		return text
	}
	return text[:neoGitHubToolOutputLimit] + "\n\n[truncated]"
}
