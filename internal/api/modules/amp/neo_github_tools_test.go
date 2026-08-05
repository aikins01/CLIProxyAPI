package amp

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
)

func TestNeoSubagentResolvesLibrarianGitHubTools(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.executorBootstrapComplete = true

	def, ok := neoSubagentDefFor("librarian")
	if !ok {
		t.Fatal("librarian subagent definition missing")
	}
	tools := actor.resolveSubagentToolsLocked(def.IncludeTools)
	got := map[string]bool{}
	for _, tool := range tools {
		got[tool.Name] = true
	}
	for _, name := range def.IncludeTools {
		if !got[name] {
			t.Fatalf("librarian tool %s was not resolved; got %#v", name, got)
		}
	}
}

func TestNeoSubagentLocalGitHubToolStoresChildResult(t *testing.T) {
	content := "package main\n\nfunc main() {}\n"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/owner/repo/contents/cmd/server/main.go" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"type":"file","encoding":"base64","content":"` + base64.StdEncoding.EncodeToString([]byte(content)) + `"}`))
	}))
	defer server.Close()

	rt := newNeoRuntime(&config.Config{})
	rt.githubAPIBase = server.URL
	rt.githubClient = server.Client()
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)

	call := neoToolCall{
		ID:    "leaf-1",
		Name:  "read_github",
		Input: map[string]any{"repository": "owner/repo", "path": "cmd/server/main.go"},
	}
	childMessageID := actor.storeSubagentToolUseMessage(call, "TU-librarian")
	run := actor.execSubagentLocalGitHubTool(context.Background(), call, "TU-librarian", childMessageID, actor.generation, "")

	if stringValue(run["status"]) != "done" || !strings.Contains(stringValue(run["output"]), "func main") {
		t.Fatalf("run = %#v", run)
	}
	actor.mu.Lock()
	if len(actor.messages) != 2 {
		t.Fatalf("messages = %#v, want child tool_use plus child tool_result", actor.messages)
	}
	toolUseMessage := actor.messages[0]
	toolUse := mapValue(toolUseMessage.Content[0])
	if toolUseMessage.Role != "assistant" || toolUseMessage.MessageID != childMessageID || toolUseMessage.ParentToolUseID != "TU-librarian" {
		t.Fatalf("child tool_use message = %#v", toolUseMessage)
	}
	if stringValue(toolUse["type"]) != "tool_use" || stringValue(toolUse["id"]) != "leaf-1" || stringValue(toolUse["name"]) != "read_github" {
		t.Fatalf("child tool_use block = %#v", toolUse)
	}
	foundResult := false
	for _, message := range actor.messages {
		if message.MessageID != toolResultMessageID("leaf-1") {
			continue
		}
		foundResult = true
		if message.ParentToolUseID != "TU-librarian" || message.CompletionStatus != "" {
			t.Fatalf("child message parent/status = %q/%q", message.ParentToolUseID, message.CompletionStatus)
		}
		if !strings.Contains(runToText(mapValue(mapValue(message.Content[0])["run"])), "func main") {
			t.Fatalf("child message content = %#v", message.Content)
		}
	}
	actor.rebuildHistoryLocked()
	topHistory := scopedNeoHistory(actor.history, "")
	nestedHistory := scopedNeoHistory(actor.history, "TU-librarian")
	actor.mu.Unlock()
	if !foundResult {
		t.Fatal("child tool result message not stored")
	}
	if len(topHistory) != 0 {
		t.Fatalf("top-level history included nested GitHub call: %#v", topHistory)
	}
	if len(nestedHistory) != 2 || len(nestedHistory[0].ToolCalls) != 1 || nestedHistory[0].ToolCalls[0].Name != "read_github" || nestedHistory[1].ToolCallID != "leaf-1" {
		t.Fatalf("nested history = %#v", nestedHistory)
	}
}

func TestNeoSubagentLocalGitHubToolHonorsRunCancellation(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	rt := newNeoRuntime(&config.Config{})
	rt.githubAPIBase = server.URL
	rt.githubClient = server.Client()
	actor := newNeoActor(rt, "actor-cancel", "thread-actor", "T-cancel", "T-cancel", neoActorRecord("actor-cancel", "thread-actor", "T-cancel"), nil)
	call := neoToolCall{ID: "leaf-cancel", Name: "read_github", Input: map[string]any{"repository": "owner/repo", "path": "README.md"}}
	childMessageID := actor.storeSubagentToolUseMessage(call, "TU-librarian")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	run := actor.execSubagentLocalGitHubTool(ctx, call, "TU-librarian", childMessageID, actor.generation, "")
	if stringValue(run["status"]) != "error" || requests != 0 {
		t.Fatalf("cancelled GitHub run = %#v requests=%d", run, requests)
	}
}

func TestNeoSubagentLocalGitHubToolDoesNotDeleteReplacementOwner(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"type":"file","encoding":"base64","content":"cGFja2FnZSBtYWluCg=="}`))
	}))
	defer server.Close()

	rt := newNeoRuntime(&config.Config{})
	rt.githubAPIBase = server.URL
	rt.githubClient = server.Client()
	actor := newNeoActor(rt, "actor-owner", "thread-actor", "T-owner", "T-owner", neoActorRecord("actor-owner", "thread-actor", "T-owner"), nil)
	call := neoToolCall{ID: "leaf-owner", Name: "read_github", Input: map[string]any{"repository": "owner/repo", "path": "main.go"}}
	childMessageID := actor.storeSubagentToolUseMessage(call, "TU-librarian")
	generation := actor.generation
	done := make(chan map[string]any, 1)
	go func() {
		done <- actor.execSubagentLocalGitHubTool(context.Background(), call, "TU-librarian", childMessageID, generation, "")
	}()
	<-entered
	actor.mu.Lock()
	actor.advanceGenerationLocked()
	replacement := neoPendingTool{ID: call.ID, Name: call.Name, MessageID: "M-replacement", ParentToolCallID: "TU-replacement"}
	actor.subagentTools[call.ID] = replacement
	actor.mu.Unlock()
	close(release)
	<-done
	actor.mu.Lock()
	defer actor.mu.Unlock()
	if current := actor.subagentTools[call.ID]; current.MessageID != replacement.MessageID || current.ParentToolCallID != replacement.ParentToolCallID {
		t.Fatalf("replacement GitHub tool owner was removed: %#v", current)
	}
}

func TestNeoGitHubReadPrefersAmpProxy(t *testing.T) {
	t.Setenv("GITHUB_TOKEN", "")
	t.Setenv("GH_TOKEN", "")

	content := "package main\n\nfunc main() {}\n"
	var sawAuthStatus bool
	var sawProxyRead bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer amp-secret" {
			t.Errorf("authorization = %q", got)
		}
		switch r.URL.Path {
		case "/api/internal/github-auth-status":
			sawAuthStatus = true
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"authenticated":true}`))
		case "/api/internal/github-proxy/repos/owner/repo/contents/cmd/server/main.go":
			sawProxyRead = true
			if got := r.URL.Query().Get("ref"); got != "main" {
				t.Errorf("ref query = %q", got)
			}
			if got := r.Header.Get("Accept"); got != "application/vnd.github+json" {
				t.Errorf("accept = %q", got)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"type":"file","encoding":"base64","content":"` + base64.StdEncoding.EncodeToString([]byte(content)) + `"}`))
		default:
			t.Errorf("unexpected proxy path %s", r.URL.String())
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{UpstreamURL: server.URL, UpstreamAPIKey: "amp-secret"}})
	rt.githubClient = server.Client()
	out, err := rt.runGitHubRead(context.Background(), map[string]any{
		"repository": "owner/repo",
		"path":       "cmd/server/main.go",
		"revision":   "main",
	})
	if err != nil {
		t.Fatalf("runGitHubRead error: %v", err)
	}
	if !strings.Contains(out, "func main") {
		t.Fatalf("output = %q", out)
	}
	if !sawAuthStatus || !sawProxyRead {
		t.Fatalf("saw auth/read = %v/%v", sawAuthStatus, sawProxyRead)
	}
}

func TestNeoGitHubSearchUsesAmpProxyWithoutEnvToken(t *testing.T) {
	t.Setenv("GITHUB_TOKEN", "")
	t.Setenv("GH_TOKEN", "")

	var sawCodeSearch bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/internal/github-auth-status":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"authenticated":true}`))
		case "/api/internal/github-proxy/search/code":
			sawCodeSearch = true
			if got := r.URL.Query().Get("q"); got != "repo:owner/repo main path:cmd" {
				t.Errorf("query = %q", got)
			}
			if got := r.URL.Query().Get("per_page"); got != "5" {
				t.Errorf("per_page = %q", got)
			}
			if got := r.URL.Query().Get("page"); got != "3" {
				t.Errorf("page = %q", got)
			}
			if got := r.Header.Get("Accept"); got != "application/vnd.github.text-match+json" {
				t.Errorf("accept = %q", got)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"items":[{"path":"cmd/server/main.go","repository":{"full_name":"owner/repo"},"text_matches":[{"fragment":"func main() {}"}]}]}`))
		default:
			t.Errorf("unexpected proxy path %s", r.URL.String())
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{UpstreamURL: server.URL, UpstreamAPIKey: "amp-secret"}})
	rt.githubClient = server.Client()
	out, err := rt.runGitHubSearch(context.Background(), map[string]any{"repository": "owner/repo", "query": "main", "path": "cmd", "limit": 5, "offset": 10})
	if err != nil {
		t.Fatalf("runGitHubSearch error: %v", err)
	}
	if !sawCodeSearch {
		t.Fatal("code search proxy was not called")
	}
	if !strings.Contains(out, "owner/repo cmd/server/main.go") || !strings.Contains(out, "func main") {
		t.Fatalf("output = %q", out)
	}
}

func TestNeoGitHubProxyAuthFailureDoesNotFallbackToEnvToken(t *testing.T) {
	t.Setenv("GITHUB_TOKEN", "server-global-token")
	t.Setenv("GH_TOKEN", "")

	var sawAuthStatus bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/internal/github-auth-status":
			sawAuthStatus = true
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"authenticated":false}`))
		default:
			t.Errorf("unexpected fallback/proxy path %s", r.URL.String())
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{UpstreamURL: server.URL}})
	rt.setSecretSource(NewStaticSecretSource("amp-secret"))
	rt.githubClient = server.Client()
	_, err := rt.runGitHubRead(context.Background(), map[string]any{"repository": "owner/repo", "path": "cmd/server/main.go"})
	if err == nil || !strings.Contains(err.Error(), "GitHub proxy is not authenticated") {
		t.Fatalf("error = %v", err)
	}
	if !sawAuthStatus {
		t.Fatal("GitHub auth status was not checked")
	}
}

func TestNeoGitHubSearchFallbackHonorsLimitOffset(t *testing.T) {
	content := "package main\nvar needle = true\n"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/search/code":
			http.Error(w, "code search unavailable", http.StatusForbidden)
		case "/repos/owner/repo/git/trees/main":
			if r.URL.Query().Get("recursive") != "1" {
				t.Errorf("recursive = %q", r.URL.Query().Get("recursive"))
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"tree":[
				{"type":"blob","path":"a.go"},
				{"type":"blob","path":"b.go"},
				{"type":"blob","path":"c.go"}
			]}`))
		case "/repos/owner/repo/contents/a.go", "/repos/owner/repo/contents/b.go", "/repos/owner/repo/contents/c.go":
			if r.URL.Query().Get("ref") != "main" {
				t.Errorf("ref = %q", r.URL.Query().Get("ref"))
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"type":"file","encoding":"base64","content":"` + base64.StdEncoding.EncodeToString([]byte(content)) + `"}`))
		default:
			t.Errorf("unexpected path %s", r.URL.String())
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	rt := newNeoRuntime(&config.Config{})
	rt.githubAPIBase = server.URL
	rt.githubClient = server.Client()
	out, err := rt.runGitHubSearch(context.Background(), map[string]any{"repository": "owner/repo", "revision": "main", "query": "needle", "limit": 1, "offset": 1})
	if err != nil {
		t.Fatalf("runGitHubSearch error: %v", err)
	}
	if !strings.Contains(out, "b.go") || strings.Contains(out, "a.go") || strings.Contains(out, "c.go") {
		t.Fatalf("output = %q", out)
	}
}

func TestNeoGitHubReadAcceptsClassicReadRange(t *testing.T) {
	content := "one\ntwo\nthree\nfour"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/owner/repo/contents/file.txt" {
			t.Errorf("unexpected path %s", r.URL.String())
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"type":"file","encoding":"base64","content":"` + base64.StdEncoding.EncodeToString([]byte(content)) + `"}`))
	}))
	defer server.Close()

	rt := newNeoRuntime(&config.Config{})
	rt.githubAPIBase = server.URL
	rt.githubClient = server.Client()
	out, err := rt.runGitHubRead(context.Background(), map[string]any{"repository": "owner/repo", "path": "file.txt", "read_range": []any{2, 3}})
	if err != nil {
		t.Fatalf("runGitHubRead error: %v", err)
	}
	if out != "two\nthree" {
		t.Fatalf("output = %q", out)
	}
}

func TestNeoGitHubListDirectoryHonorsLimit(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/owner/repo/contents/dir" {
			t.Errorf("unexpected path %s", r.URL.String())
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[
			{"type":"file","path":"dir/a.go","size":1},
			{"type":"file","path":"dir/b.go","size":2},
			{"type":"file","path":"dir/c.go","size":3}
		]`))
	}))
	defer server.Close()

	rt := newNeoRuntime(&config.Config{})
	rt.githubAPIBase = server.URL
	rt.githubClient = server.Client()
	out, err := rt.runGitHubListDirectory(context.Background(), map[string]any{"repository": "owner/repo", "path": "dir", "limit": 2})
	if err != nil {
		t.Fatalf("runGitHubListDirectory error: %v", err)
	}
	if !strings.Contains(out, "dir/a.go") || !strings.Contains(out, "dir/b.go") || strings.Contains(out, "dir/c.go") {
		t.Fatalf("output = %q", out)
	}
}

func TestNeoGitHubGlobHonorsLimitOffset(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/owner/repo/git/trees/main" || r.URL.Query().Get("recursive") != "1" {
			t.Errorf("unexpected path %s", r.URL.String())
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"tree":[
			{"type":"blob","path":"a.go"},
			{"type":"blob","path":"b.go"},
			{"type":"blob","path":"c.go"}
		]}`))
	}))
	defer server.Close()

	rt := newNeoRuntime(&config.Config{})
	rt.githubAPIBase = server.URL
	rt.githubClient = server.Client()
	out, err := rt.runGitHubGlob(context.Background(), map[string]any{"repository": "owner/repo", "revision": "main", "pattern": "*.go", "limit": 1, "offset": 1})
	if err != nil {
		t.Fatalf("runGitHubGlob error: %v", err)
	}
	if strings.TrimSpace(out) != "b.go" {
		t.Fatalf("output = %q", out)
	}
	if _, err := rt.runGitHubGlob(context.Background(), map[string]any{"repository": "owner/repo", "revision": "main", "pattern": "*.go", "limit": 2, "offset": 1}); err == nil || !strings.Contains(err.Error(), "offset (1) must be divisible by limit (2)") {
		t.Fatalf("invalid offset error = %v", err)
	}
}

func TestNeoGitHubCommitSearchUsesPathFilters(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/owner/repo/commits" {
			t.Errorf("unexpected path %s", r.URL.String())
			http.NotFound(w, r)
			return
		}
		queries := map[string]string{"path": "cmd/server", "author": "alice", "since": "2024-01-01", "until": "2024-02-01", "per_page": "100", "page": "1"}
		for key, want := range queries {
			if got := r.URL.Query().Get(key); got != want {
				t.Errorf("%s = %q", key, got)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[
			{"sha":"0123456789abcdef","html_url":"https://github.com/owner/repo/commit/0123456789abcdef","commit":{"message":"fix router\n\nbody","author":{"name":"Alice","email":"alice@example.com","date":"2024-01-02T00:00:00Z"}}},
			{"sha":"abcdef0123456789","html_url":"https://github.com/owner/repo/commit/abcdef0123456789","commit":{"message":"docs only","author":{"name":"Alice","email":"alice@example.com","date":"2024-01-03T00:00:00Z"}}}
		]`))
	}))
	defer server.Close()

	rt := newNeoRuntime(&config.Config{})
	rt.githubAPIBase = server.URL
	rt.githubClient = server.Client()
	out, err := rt.runGitHubCommitSearch(context.Background(), map[string]any{"repository": "owner/repo", "query": "fix", "path": "cmd/server", "author": "alice", "since": "2024-01-01", "until": "2024-02-01", "limit": 2})
	if err != nil {
		t.Fatalf("runGitHubCommitSearch error: %v", err)
	}
	if !strings.Contains(out, "0123456789ab fix router") || strings.Contains(out, "docs only") {
		t.Fatalf("output = %q", out)
	}
}

func TestNeoGitHubCommitSearchPathQueryScansBeforeFiltering(t *testing.T) {
	pageOne := strings.Builder{}
	pageOne.WriteString("[")
	for i := 0; i < 100; i++ {
		if i > 0 {
			pageOne.WriteString(",")
		}
		pageOne.WriteString(`{"sha":"abcdef0123456789","html_url":"https://github.com/owner/repo/commit/abcdef0123456789","commit":{"message":"docs only","author":{"name":"Alice","email":"alice@example.com","date":"2024-01-03T00:00:00Z"}}}`)
	}
	pageOne.WriteString("]")

	pagesSeen := map[string]bool{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/owner/repo/commits" {
			t.Errorf("unexpected path %s", r.URL.String())
			http.NotFound(w, r)
			return
		}
		if got := r.URL.Query().Get("path"); got != "cmd/server" {
			t.Errorf("path = %q", got)
		}
		if got := r.URL.Query().Get("per_page"); got != "100" {
			t.Errorf("per_page = %q", got)
		}
		page := r.URL.Query().Get("page")
		pagesSeen[page] = true
		w.Header().Set("Content-Type", "application/json")
		switch page {
		case "1":
			_, _ = w.Write([]byte(pageOne.String()))
		case "2":
			_, _ = w.Write([]byte(`[
				{"sha":"0123456789abcdef","html_url":"https://github.com/owner/repo/commit/0123456789abcdef","commit":{"message":"fix router after docs","author":{"name":"Alice","email":"alice@example.com","date":"2024-01-02T00:00:00Z"}}}
			]`))
		default:
			_, _ = w.Write([]byte(`[]`))
		}
	}))
	defer server.Close()

	rt := newNeoRuntime(&config.Config{})
	rt.githubAPIBase = server.URL
	rt.githubClient = server.Client()
	out, err := rt.runGitHubCommitSearch(context.Background(), map[string]any{"repository": "owner/repo", "query": "fix", "path": "cmd/server", "limit": 1})
	if err != nil {
		t.Fatalf("runGitHubCommitSearch error: %v", err)
	}
	if !pagesSeen["1"] || !pagesSeen["2"] {
		t.Fatalf("pages seen = %#v", pagesSeen)
	}
	if !strings.Contains(out, "0123456789ab fix router after docs") || strings.Contains(out, "docs only") {
		t.Fatalf("output = %q", out)
	}
}

func TestNeoGitHubDiffSupportsCompareAndPullURLs(t *testing.T) {
	var sawCompare, sawPull bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/repos/owner/repo/compare/base...head":
			sawCompare = true
			if got := r.Header.Get("Accept"); got != "application/vnd.github.diff" {
				t.Errorf("compare accept = %q", got)
			}
			_, _ = w.Write([]byte("compare diff"))
		case "/repos/owner/repo/pulls/123":
			sawPull = true
			if got := r.Header.Get("Accept"); got != "application/vnd.github.diff" {
				t.Errorf("pull accept = %q", got)
			}
			_, _ = w.Write([]byte("pull diff"))
		default:
			t.Errorf("unexpected path %s", r.URL.String())
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	rt := newNeoRuntime(&config.Config{})
	rt.githubAPIBase = server.URL
	rt.githubClient = server.Client()
	compare, err := rt.runGitHubDiff(context.Background(), map[string]any{"url": "https://github.com/owner/repo/compare/base...head"})
	if err != nil {
		t.Fatalf("compare diff error: %v", err)
	}
	pull, err := rt.runGitHubDiff(context.Background(), map[string]any{"url": "https://github.com/owner/repo/pull/123"})
	if err != nil {
		t.Fatalf("pull diff error: %v", err)
	}
	if compare != "compare diff" || pull != "pull diff" || !sawCompare || !sawPull {
		t.Fatalf("diff outputs compare=%q pull=%q saw=%v/%v", compare, pull, sawCompare, sawPull)
	}
}

func TestNeoGitHubRepoCIStatusUsesChecksAndStatuses(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/repos/owner/repo/commits/main/check-runs":
			if got := r.URL.Query().Get("per_page"); got != "100" {
				t.Errorf("check-runs per_page = %q", got)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"check_runs":[{"name":"build","status":"completed","conclusion":"success","html_url":"https://github.com/owner/repo/actions/runs/1"}]}`))
		case "/repos/owner/repo/commits/main/status":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"state":"pending","statuses":[{"context":"legacy-ci","state":"pending","description":"waiting","target_url":"https://ci.example/build/1"}]}`))
		default:
			t.Errorf("unexpected path %s", r.URL.String())
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	rt := newNeoRuntime(&config.Config{})
	rt.githubAPIBase = server.URL
	rt.githubClient = server.Client()
	out, err := rt.runGitHubRepoCIStatus(context.Background(), map[string]any{"repository": "owner/repo", "revision": "main"})
	if err != nil {
		t.Fatalf("runGitHubRepoCIStatus error: %v", err)
	}
	for _, want := range []string{"Repository: owner/repo", "Ref: main", "Overall status: pending", "build: completed/success", "legacy-ci: pending - waiting"} {
		if !strings.Contains(out, want) {
			t.Fatalf("output missing %q:\n%s", want, out)
		}
	}
}

func TestNeoActorRunsTopLevelGitHubCIStatusLocallyWhenExecutorOmitsIt(t *testing.T) {
	t.Setenv("GITHUB_TOKEN", "")
	t.Setenv("GH_TOKEN", "")
	dir := t.TempDir()
	oldStoreDir := neoAmpDataDir
	neoAmpDataDir = func() string { return dir }
	t.Cleanup(func() { neoAmpDataDir = oldStoreDir })

	var sawCIStatus bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/internal/github-auth-status":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"authenticated":true}`))
		case "/api/internal/github-proxy/repos/owner/repo/commits/main/check-runs":
			sawCIStatus = true
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"check_runs":[{"name":"test","status":"completed","conclusion":"success"}]}`))
		case "/api/internal/github-proxy/repos/owner/repo/commits/main/status":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"state":"success","statuses":[]}`))
		default:
			t.Errorf("unexpected proxy path %s", r.URL.String())
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{UpstreamURL: server.URL}})
	rt.setSecretSource(NewStaticSecretSource("amp-secret"))
	rt.githubClient = server.Client()
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.executorBootstrapComplete = true
	if !actor.shouldRunLocalActorTool("github_repo_ci_status") {
		t.Fatal("github_repo_ci_status should run locally when executor omitted it")
	}
	tools := actor.inferenceRequestLocked("agg-man", "", "").Tools
	foundTool := false
	for _, tool := range tools {
		if tool.Name == "github_repo_ci_status" {
			foundTool = true
		}
	}
	if !foundTool {
		t.Fatalf("agg-man tools missing synthetic github_repo_ci_status: %#v", tools)
	}

	pending := neoPendingTool{ID: "TU-ci", Name: "github_repo_ci_status", Input: map[string]any{"repository": "owner/repo", "revision": "main"}, AgentMode: "agg-man", MessageID: "M-assistant"}
	actor.pendingTools[pending.ID] = pending
	actor.agentState = "running_tools"
	actor.runLocalActorTool(pending, actor.generation)

	actor.mu.Lock()
	_, stillPending := actor.pendingTools[pending.ID]
	var resultMessage neoMessage
	for _, message := range actor.messages {
		if message.MessageID == toolResultMessageID(pending.ID) {
			resultMessage = message
		}
	}
	actor.mu.Unlock()
	if stillPending {
		t.Fatal("top-level GitHub CI tool remained pending")
	}
	if !sawCIStatus {
		t.Fatal("GitHub CI status proxy was not called")
	}
	if len(resultMessage.Content) == 0 {
		t.Fatalf("missing tool result message content: %#v", resultMessage)
	}
	run := mapValue(mapValue(resultMessage.Content[0])["run"])
	if stringValue(run["status"]) != "done" || !strings.Contains(runToText(run), "test: completed/success") {
		t.Fatalf("tool result run = %#v", run)
	}
}

func TestNeoActorRunsTopLevelGitHubToolLocallyWhenExecutorOmitsIt(t *testing.T) {
	t.Setenv("GITHUB_TOKEN", "")
	t.Setenv("GH_TOKEN", "")
	dir := t.TempDir()
	oldStoreDir := neoAmpDataDir
	neoAmpDataDir = func() string { return dir }
	t.Cleanup(func() { neoAmpDataDir = oldStoreDir })

	content := "package main\n\nfunc main() {}\n"
	var sawProxyRead bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/internal/github-auth-status":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"authenticated":true}`))
		case "/api/internal/github-proxy/repos/owner/repo/contents/cmd/server/main.go":
			sawProxyRead = true
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"type":"file","encoding":"base64","content":"` + base64.StdEncoding.EncodeToString([]byte(content)) + `"}`))
		default:
			t.Errorf("unexpected proxy path %s", r.URL.String())
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{UpstreamURL: server.URL}})
	rt.setSecretSource(NewStaticSecretSource("amp-secret"))
	rt.githubClient = server.Client()
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.executorBootstrapComplete = true
	if !actor.shouldRunLocalActorTool("read_github") {
		t.Fatal("read_github should run locally when executor omitted it")
	}
	actor.tools["read_github"] = neoToolSpec{Name: "read_github"}
	if actor.shouldRunLocalActorTool("read_github") {
		t.Fatal("read_github should not run locally when executor registered it")
	}
	delete(actor.tools, "read_github")

	pending := neoPendingTool{
		ID:        "TU-github",
		Name:      "read_github",
		Input:     map[string]any{"repository": "owner/repo", "path": "cmd/server/main.go"},
		AgentMode: "agg-man",
		MessageID: "M-assistant",
	}
	actor.pendingTools[pending.ID] = pending
	actor.agentState = "running_tools"

	actor.runLocalActorTool(pending, actor.generation)

	actor.mu.Lock()
	_, stillPending := actor.pendingTools[pending.ID]
	var resultMessage neoMessage
	for _, message := range actor.messages {
		if message.MessageID == toolResultMessageID(pending.ID) {
			resultMessage = message
		}
	}
	actor.mu.Unlock()
	if stillPending {
		t.Fatal("top-level GitHub tool remained pending")
	}
	if !sawProxyRead {
		t.Fatal("GitHub proxy read was not called")
	}
	if resultMessage.MessageID == "" {
		t.Fatal("tool result message was not stored")
	}
	if resultMessage.CompletionStatus != "" {
		t.Fatalf("final completionStatus = %q, want complete", resultMessage.CompletionStatus)
	}
	run := mapValue(mapValue(resultMessage.Content[0])["run"])
	if stringValue(run["status"]) != "done" || !strings.Contains(runToText(run), "func main") {
		t.Fatalf("tool result run = %#v", run)
	}
}
