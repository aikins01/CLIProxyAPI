package amp

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
)

func TestNeoActorRunsTopLevelThreadToolsLocallyWhenExecutorOmitsThem(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	sourceID := "T-019e1046-656d-7132-879f-390ded941c40"
	targetID := "T-019e1046-656d-7132-879f-390ded941c41"
	source := rt.store.ensureThreadActor(sourceID)
	target := rt.store.ensureThreadActor(targetID)
	source.executorBootstrapComplete = true

	for _, name := range []string{"find_thread", "list_agent_modes", "list_runners", "list_workspace_members", "find_shared_plugins_and_skills", "create_thread", "thread_interact", "rename_thread", "set_thread_pinned", "add_thread_labels", "remove_thread_labels", "archive_current_thread", "archive_thread", "archive_threads", "unarchive_thread", "send_message_to_thread", "download_thread_file", "upload_thread_file"} {
		if !source.shouldRunLocalActorTool(name) {
			t.Fatalf("%s should run locally when executor omitted it", name)
		}
	}
	tools := source.inferenceRequestLocked("high", "", "").Tools
	got := map[string]bool{}
	for _, tool := range tools {
		got[tool.Name] = true
	}
	for _, name := range []string{"list_agent_modes", "list_runners", "list_workspace_members", "find_shared_plugins_and_skills", "create_thread", "thread_interact"} {
		if !got[name] {
			t.Fatalf("high tools missing synthetic %s: %#v", name, tools)
		}
	}
	source.tools["archive_thread"] = neoToolSpec{Name: "archive_thread"}
	if source.shouldRunLocalActorTool("archive_thread") {
		t.Fatal("archive_thread should not run locally when executor registered it")
	}
	delete(source.tools, "archive_thread")

	pending := neoPendingTool{ID: "TU-archive", Name: "archive_thread", Input: map[string]any{"targetThreadId": targetID}, AgentMode: "agg-man", MessageID: "M-assistant"}
	source.pendingTools[pending.ID] = pending
	source.agentState = "running_tools"
	source.runLocalActorTool(pending, source.generation)

	source.mu.Lock()
	_, stillPending := source.pendingTools[pending.ID]
	var result *neoMessage
	for i := range source.messages {
		if source.messages[i].MessageID == toolResultMessageID(pending.ID) {
			message := source.messages[i]
			result = &message
		}
	}
	sourceArchived := source.archived
	source.mu.Unlock()
	target.mu.Lock()
	targetArchived := target.archived
	target.mu.Unlock()
	if stillPending || sourceArchived || !targetArchived {
		t.Fatalf("archive state pending=%v source=%v target=%v", stillPending, sourceArchived, targetArchived)
	}
	if result == nil || len(result.Content) == 0 {
		t.Fatalf("missing archive tool result: %#v", result)
	}
	run := mapValue(mapValue(result.Content[0])["run"])
	if stringValue(run["status"]) != "done" || !strings.Contains(runToText(run), targetID) {
		t.Fatalf("archive run = %#v", run)
	}
}

func TestNeoPuckWithoutExecutorExposesOnlyRunnableServerTools(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	actor := rt.store.ensureThreadActor("T-019f7000-0000-7000-8000-000000000080")
	actor.updateSettings(map[string]any{"agentMode": "puck"})

	names := map[string]bool{}
	for _, tool := range actor.inferenceRequestLocked("puck", "", "").Tools {
		names[tool.Name] = true
	}
	for _, name := range []string{"find_thread", "read_thread", "list_agent_modes", "list_runners", "list_workspace_members", "find_shared_plugins_and_skills", "create_thread", "thread_interact", "update_thread", "archive_threads", "get_schedule", "set_schedule", "update_schedule", "clear_schedule", "github_repo_ci_status", "read_github", "search_github", "commit_search", "list_directory_github", "list_repositories", "glob_github", "diff"} {
		if !names[name] {
			t.Fatalf("executor-less Puck missing runnable tool %s: %#v", name, names)
		}
	}
	for _, name := range []string{"web_search", "read_web_page", "docs_list", "docs_read", "docs_write", "create_project", "publish_thread_artifacts", "slack_write", "slack_read", "get_thread_metadata", "archive_thread", "unarchive_thread", "send_message_to_thread", "rename_thread", "set_thread_pinned", "add_thread_labels", "remove_thread_labels"} {
		if names[name] {
			t.Fatalf("executor-less Puck exposed unavailable tool %s: %#v", name, names)
		}
	}
}

func TestNeoWorkspaceToolsUseAuthoritativeInternalRPCs(t *testing.T) {
	type capturedRequest struct {
		method      string
		path        string
		rawQuery    string
		body        string
		authorize   string
		application string
		clientType  string
		version     string
	}
	requests := make(chan capturedRequest, 2)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		requests <- capturedRequest{
			method:      r.Method,
			path:        r.URL.Path,
			rawQuery:    r.URL.RawQuery,
			body:        string(body),
			authorize:   r.Header.Get("Authorization"),
			application: r.Header.Get("X-Amp-Client-Application"),
			clientType:  r.Header.Get("X-Amp-Client-Type"),
			version:     r.Header.Get("X-Amp-Client-Version"),
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.RawQuery {
		case "listWorkspaceMembers":
			_, _ = io.WriteString(w, `{"ok":true,"result":{"workspace":{"id":"workspace-1","name":"engineering","displayName":null,"ignored":"value"},"members":[{"userID":"user-2","username":null,"displayName":"Second"},{"userID":"user-1","username":"first","displayName":null}],"ignored":true}}`)
		case "findSharedPluginsAndSkills":
			_, _ = io.WriteString(w, `{"ok":true,"result":{"workspace":{"id":"workspace-1","name":"engineering","displayName":"Engineering"},"items":[{"kind":"skill","name":"reviewing","id":"skill-2","description":null,"owner":{"userID":"user-2","username":null,"displayName":"Second"}},{"kind":"plugin","name":"browser","id":"plugin-1","description":"Browser tools","owner":{"userID":"user-1","username":"first","displayName":null}}],"hasMore":true}}`)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(upstream.Close)

	mapped := NewMappedSecretSource(NewStaticSecretSource(""))
	mapped.UpdateMappings([]config.AmpUpstreamAPIKeyEntry{{UpstreamAPIKey: "request-upstream-key", APIKeys: []string{"local-client-key"}}})
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
		UpstreamURL:                   upstream.URL,
		UpstreamAPIKey:                "wrong-default-key",
		UpstreamClientVersionOverride: "test-client-version",
	}})
	rt.setSecretSource(mapped)
	actor := rt.store.ensureThreadActor("T-019f7000-0000-7000-8000-0000000000c1")

	listed, err := actor.executeLocalThreadTool(neoPendingTool{
		Name:         "list_workspace_members",
		Input:        map[string]any{"ignored": true},
		ClientAPIKey: "local-client-key",
	})
	if err != nil {
		t.Fatal(err)
	}
	listRequest := <-requests
	if listRequest.method != http.MethodPost || listRequest.path != "/api/internal" || listRequest.rawQuery != "listWorkspaceMembers" || listRequest.body != `{"method":"listWorkspaceMembers","params":{}}` {
		t.Fatalf("list request = %#v", listRequest)
	}
	if listRequest.authorize != "Bearer request-upstream-key" || listRequest.application != "CLI" || listRequest.clientType != "cli" || listRequest.version != "test-client-version" {
		t.Fatalf("list headers = %#v", listRequest)
	}
	wantListed := map[string]any{
		"workspace": map[string]any{"id": "workspace-1", "name": "engineering", "displayName": nil},
		"members": []any{
			map[string]any{"userID": "user-2", "username": nil, "displayName": "Second"},
			map[string]any{"userID": "user-1", "username": "first", "displayName": nil},
		},
	}
	if !reflect.DeepEqual(listed, wantListed) {
		t.Fatalf("list result = %#v, want %#v", listed, wantListed)
	}

	found, err := actor.executeLocalThreadTool(neoPendingTool{
		Name: "find_shared_plugins_and_skills",
		Input: map[string]any{
			"query":       "review",
			"kind":        "skill",
			"ownerUserID": "user-2",
			"limit":       float64(50),
		},
		ClientAPIKey: "local-client-key",
	})
	if err != nil {
		t.Fatal(err)
	}
	findRequest := <-requests
	if findRequest.method != http.MethodPost || findRequest.path != "/api/internal" || findRequest.rawQuery != "findSharedPluginsAndSkills" || findRequest.body != `{"method":"findSharedPluginsAndSkills","params":{"kind":"skill","limit":50,"ownerUserID":"user-2","query":"review"}}` {
		t.Fatalf("find request = %#v", findRequest)
	}
	if findRequest.authorize != "Bearer request-upstream-key" || findRequest.application != "CLI" || findRequest.clientType != "cli" || findRequest.version != "test-client-version" {
		t.Fatalf("find headers = %#v", findRequest)
	}
	wantFound := map[string]any{
		"workspace": map[string]any{"id": "workspace-1", "name": "engineering", "displayName": "Engineering"},
		"items": []any{
			map[string]any{"kind": "skill", "name": "reviewing", "id": "skill-2", "description": nil, "owner": map[string]any{"userID": "user-2", "username": nil, "displayName": "Second"}},
			map[string]any{"kind": "plugin", "name": "browser", "id": "plugin-1", "description": "Browser tools", "owner": map[string]any{"userID": "user-1", "username": "first", "displayName": nil}},
		},
		"hasMore": true,
	}
	if !reflect.DeepEqual(found, wantFound) {
		t.Fatalf("find result = %#v, want %#v", found, wantFound)
	}
}

func TestNeoWorkspaceToolsPreserveNullWorkspaceAndEmptyArrays(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.RawQuery == "listWorkspaceMembers" {
			_, _ = io.WriteString(w, `{"ok":true,"result":{"workspace":null,"members":[]}}`)
			return
		}
		_, _ = io.WriteString(w, `{"ok":true,"result":{"workspace":null,"items":[],"hasMore":false}}`)
	}))
	t.Cleanup(upstream.Close)
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{UpstreamURL: upstream.URL, UpstreamAPIKey: "upstream-key"}})
	actor := rt.store.ensureThreadActor("T-019f7000-0000-7000-8000-0000000000c2")

	listed, err := actor.executeLocalThreadTool(neoPendingTool{Name: "list_workspace_members"})
	if err != nil || listed["workspace"] != nil || !reflect.DeepEqual(listed["members"], []any{}) {
		t.Fatalf("list result=%#v err=%v", listed, err)
	}
	found, err := actor.executeLocalThreadTool(neoPendingTool{Name: "find_shared_plugins_and_skills"})
	if err != nil || found["workspace"] != nil || !reflect.DeepEqual(found["items"], []any{}) || found["hasMore"] != false {
		t.Fatalf("find result=%#v err=%v", found, err)
	}
}

func TestNeoFindSharedPluginsAndSkillsValidatesOptionalInput(t *testing.T) {
	valid := []map[string]any{
		{"query": strings.Repeat("界", 200)},
		{"kind": "plugin"},
		{"kind": "skill"},
		{"ownerUserID": "user-1"},
		{"limit": 1},
		{"limit": float64(50)},
	}
	for _, input := range valid {
		if _, err := neoSharedPluginsAndSkillsParams(input); err != nil {
			t.Errorf("valid input %#v: %v", input, err)
		}
	}
	invalid := []map[string]any{
		{"query": strings.Repeat("界", 201)},
		{"query": nil},
		{"kind": "other"},
		{"kind": 1},
		{"ownerUserID": ""},
		{"ownerUserID": "   "},
		{"ownerUserID": nil},
		{"limit": 0},
		{"limit": 51},
		{"limit": 1.5},
		{"limit": "1"},
	}
	for _, input := range invalid {
		if _, err := neoSharedPluginsAndSkillsParams(input); err == nil {
			t.Errorf("invalid input %#v was accepted", input)
		}
	}
}

func TestNeoWorkspaceToolSpecsAndExposure(t *testing.T) {
	listSpec, ok := neoSyntheticLocalToolSpec("list_workspace_members")
	if !ok || !strings.Contains(listSpec.Description, "authoritative") || !reflect.DeepEqual(mapValue(listSpec.InputSchema["properties"]), map[string]any{}) || !reflect.DeepEqual(arrayValue(listSpec.InputSchema["required"]), []any{}) {
		t.Fatalf("list spec = %#v, ok=%v", listSpec, ok)
	}
	findSpec, ok := neoSyntheticLocalToolSpec("find_shared_plugins_and_skills")
	properties := mapValue(findSpec.InputSchema["properties"])
	limit := mapValue(properties["limit"])
	if !ok || !strings.Contains(findSpec.Description, "hasMore=true") || !strings.Contains(findSpec.Description, "up to 50") || numberFrom(mapValue(properties["query"])["maxLength"]) != 200 || !reflect.DeepEqual(arrayValue(mapValue(properties["kind"])["enum"]), []any{"plugin", "skill"}) || numberFrom(mapValue(properties["ownerUserID"])["minLength"]) != 1 || numberFrom(limit["minimum"]) != 1 || numberFrom(limit["maximum"]) != 50 || !strings.Contains(stringValue(limit["description"]), "upstream default") {
		t.Fatalf("find spec = %#v, ok=%v", findSpec, ok)
	}

	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	actor := rt.store.ensureThreadActor("T-019f7000-0000-7000-8000-0000000000c3")
	actor.mu.Lock()
	actor.executorBootstrapComplete = true
	builtinTools := actor.inferenceRequestLocked("high", "", "").Tools
	customMode := "custom-workspace-tools"
	actor.settings[neoCustomAgentModeSetting] = customMode
	actor.settings[neoCustomAgentToolsSetting] = "all"
	actor.currentAgentMode = customMode
	customTools := actor.inferenceRequestLocked(customMode, "medium", "").Tools
	actor.mu.Unlock()
	for label, tools := range map[string][]neoToolSpec{"builtin": builtinTools, "custom all": customTools} {
		names := map[string]bool{}
		for _, tool := range tools {
			names[tool.Name] = true
		}
		for _, name := range []string{"list_workspace_members", "find_shared_plugins_and_skills"} {
			if !names[name] {
				t.Errorf("%s tools missing %s", label, name)
			}
		}
	}
	for _, name := range []string{"list_workspace_members", "find_shared_plugins_and_skills"} {
		if !actor.shouldRunLocalActorTool(name) {
			t.Errorf("%s is not owned by local actor dispatch", name)
		}
	}
}

func TestNeoWorkspaceToolsFailExplicitlyWhenUpstreamOrAuthUnavailable(t *testing.T) {
	noUpstream := newNeoRuntime(&config.Config{}).store.ensureThreadActor("T-019f7000-0000-7000-8000-0000000000c4")
	if result, err := noUpstream.executeLocalThreadTool(neoPendingTool{Name: "list_workspace_members"}); err == nil || result != nil || !strings.Contains(err.Error(), "upstream is unavailable") {
		t.Fatalf("unavailable result=%#v err=%v", result, err)
	}

	noAuthRuntime := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{UpstreamURL: "http://127.0.0.1:1"}})
	noAuthRuntime.setSecretSource(NewStaticSecretSource(""))
	noAuth := noAuthRuntime.store.ensureThreadActor("T-019f7000-0000-7000-8000-0000000000c5")
	if result, err := noAuth.executeLocalThreadTool(neoPendingTool{Name: "find_shared_plugins_and_skills"}); err == nil || result != nil || !strings.Contains(err.Error(), "authentication is unavailable") {
		t.Fatalf("auth result=%#v err=%v", result, err)
	}

	for _, test := range []struct {
		name       string
		statusCode int
		body       string
		tool       string
		want       string
	}{
		{name: "non-2xx", statusCode: http.StatusBadGateway, body: `{"error":"offline"}`, tool: "list_workspace_members", want: "HTTP 502"},
		{name: "ok false", statusCode: http.StatusOK, body: `{"ok":false,"error":"denied"}`, tool: "find_shared_plugins_and_skills", want: "failed"},
		{name: "missing ok", statusCode: http.StatusOK, body: `{"result":{"workspace":null,"members":[]}}`, tool: "list_workspace_members", want: "missing ok"},
		{name: "trailing response data", statusCode: http.StatusOK, body: `{"ok":true,"result":{"workspace":null,"members":[]}} trailing`, tool: "list_workspace_members", want: "decode"},
		{name: "oversized response", statusCode: http.StatusOK, body: strings.Repeat("x", neoAmpInternalRPCMaxResponseBytes+1), tool: "list_workspace_members", want: "response exceeded"},
	} {
		t.Run(test.name, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(test.statusCode)
				_, _ = io.WriteString(w, test.body)
			}))
			defer upstream.Close()
			rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{UpstreamURL: upstream.URL, UpstreamAPIKey: "key"}})
			actor := rt.store.ensureThreadActor("T-019f7000-0000-7000-8000-0000000000c6")
			result, err := actor.executeLocalThreadTool(neoPendingTool{Name: test.tool})
			if err == nil || result != nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("result=%#v err=%v, want %q", result, err, test.want)
			}
		})
	}
}

func TestNeoWorkspaceToolRequestIsCancelledWhenGenerationAdvances(t *testing.T) {
	useTempNeoThreadStore(t)
	requestStarted := make(chan struct{})
	requestCancelled := make(chan struct{})
	releaseRequest := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		flusher, ok := w.(http.Flusher)
		if !ok {
			return
		}
		flusher.Flush()
		close(requestStarted)
		select {
		case <-r.Context().Done():
			close(requestCancelled)
		case <-releaseRequest:
		}
	}))
	t.Cleanup(func() {
		close(releaseRequest)
		upstream.Close()
	})
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{UpstreamURL: upstream.URL, UpstreamAPIKey: "key"}})
	actor := rt.store.ensureThreadActor("T-019f7000-0000-7000-8000-0000000000c8")
	t.Cleanup(actor.cancel)
	pending := neoPendingTool{ID: "TU-workspace-cancel", Name: "list_workspace_members", AgentMode: "high", MessageID: "M-assistant"}
	actor.mu.Lock()
	actor.pendingTools[pending.ID] = pending
	actor.agentState = "running_tools"
	generation := actor.generation
	actor.mu.Unlock()

	done := make(chan struct{})
	go func() {
		actor.runLocalThreadActorTool(pending, generation)
		close(done)
	}()
	select {
	case <-requestStarted:
	case <-time.After(time.Second):
		t.Fatal("workspace RPC did not start")
	}
	actor.cancel()
	select {
	case <-requestCancelled:
	case <-time.After(time.Second):
		t.Fatal("generation advance did not cancel workspace RPC")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("workspace tool did not stop after cancellation")
	}
	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.subagentRuns) != 0 {
		t.Fatalf("cancelled workspace tool retained %d run contexts", len(actor.subagentRuns))
	}
	if _, exists := actor.pendingTools[pending.ID]; exists {
		t.Fatal("cancelled workspace tool remained pending")
	}
}

func TestNeoWorkspaceToolsRejectMalformedUpstreamContracts(t *testing.T) {
	responses := map[string]string{
		"listWorkspaceMembers":       `{"ok":true,"result":{"workspace":{"id":"workspace-1","name":"engineering"},"members":[]}}`,
		"findSharedPluginsAndSkills": `{"ok":true,"result":{"workspace":null,"items":[],"hasMore":"false"}}`,
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, responses[r.URL.RawQuery])
	}))
	t.Cleanup(upstream.Close)
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{UpstreamURL: upstream.URL, UpstreamAPIKey: "key"}})
	actor := rt.store.ensureThreadActor("T-019f7000-0000-7000-8000-0000000000c7")
	for _, name := range []string{"list_workspace_members", "find_shared_plugins_and_skills"} {
		if result, err := actor.executeLocalThreadTool(neoPendingTool{Name: name}); err == nil || result != nil {
			t.Errorf("%s accepted malformed result %#v, err=%v", name, result, err)
		}
	}
}

func TestNeoThreadToolsFindOwnedThreads(t *testing.T) {
	useTempNeoThreadStore(t)
	enabled := true
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled}}})
	source := rt.store.ensureThreadActor("T-019f7000-0000-7000-8000-000000000085")
	target := rt.store.ensureThreadActor("T-019f7000-0000-7000-8000-000000000086")
	target.mu.Lock()
	target.title = "Needle callback investigation"
	target.messages = []neoMessage{{
		ThreadID:  target.threadID,
		MessageID: "M-0000000000000000000001",
		Role:      "user",
		Content:   []any{map[string]any{"type": "text", "text": "Trace the callback lifecycle."}},
		CreatedAt: "2026-07-21T12:00:00Z",
	}}
	target.rebuildHistoryLocked()
	target.mu.Unlock()
	target.syncCloudAsync()
	waitForNeoActorSyncIdle(t, target)

	result, err := source.executeLocalThreadTool(neoPendingTool{Name: "find_thread", Input: map[string]any{"query": "Needle callback", "limit": 5}})
	if err != nil {
		t.Fatal(err)
	}
	threads := arrayValue(result["threads"])
	if len(threads) != 1 || stringValue(mapValue(threads[0])["id"]) != target.threadID {
		t.Fatalf("find_thread result = %#v", result)
	}
}

func TestNeoThreadToolsListAgentModesAndArchiveCurrentThread(t *testing.T) {
	useTempNeoThreadStore(t)
	oldPluginsDir := neoAmpUserPluginsDir
	neoAmpUserPluginsDir = func() string { return t.TempDir() }
	t.Cleanup(func() { neoAmpUserPluginsDir = oldPluginsDir })

	rt := newNeoRuntime(&config.Config{})
	actor := rt.store.ensureThreadActor("T-019f7000-0000-7000-8000-000000000081")
	result, err := actor.executeLocalThreadTool(neoPendingTool{Name: "list_agent_modes"})
	if err != nil {
		t.Fatal(err)
	}
	modes := arrayValue(result["modes"])
	if len(modes) < 12 || stringValue(mapValue(modes[0])["key"]) != "low" || stringValue(mapValue(modes[0])["type"]) != "builtin" {
		t.Fatalf("agent modes = %#v", modes)
	}
	foundPlugin := false
	for _, item := range modes {
		mode := mapValue(item)
		if stringValue(mode["key"]) == neoDeepOneAgentModeKey && stringValue(mode["type"]) == "plugin" && stringValue(mode["pluginScope"]) == neoPluginAgentModeUserScope {
			foundPlugin = true
		}
	}
	if !foundPlugin {
		t.Fatalf("agent modes missing local plugin modes: %#v", modes)
	}
	actor.meta["ownerUserId"] = "user-owner"
	identity, err := actor.executeLocalThreadTool(neoPendingTool{Name: "get_current_user_identity"})
	if err != nil {
		t.Fatal(err)
	}
	if stringValue(identity["id"]) != "user-owner" || stringValue(identity["displayName"]) != "user-owner" {
		t.Fatalf("current user identity = %#v", identity)
	}

	archived, err := actor.executeLocalThreadTool(neoPendingTool{Name: "archive_current_thread"})
	if err != nil {
		t.Fatal(err)
	}
	actor.mu.Lock()
	isArchived := actor.archived
	actor.mu.Unlock()
	if !isArchived || stringValue(archived["threadId"]) != actor.threadID || archived["archived"] != true {
		t.Fatalf("archive current result=%#v archived=%v", archived, isArchived)
	}
}

func TestNeoThreadToolsMutateOwnedThreadMetadata(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	source := rt.store.ensureThreadActor("T-019f7000-0000-7000-8000-000000000083")
	targetID := "T-019f7000-0000-7000-8000-000000000084"
	target := rt.store.ensureThreadActor(targetID)
	target.setThreadLabels([]string{"triage", "Keep"})

	renamed, err := source.executeLocalThreadTool(neoPendingTool{Name: "rename_thread", Input: map[string]any{
		"url":   "https://ampcode.com/threads/" + targetID,
		"title": "  Investigate callbacks  ",
	}})
	if err != nil || stringValue(renamed["title"]) != "Investigate callbacks" {
		t.Fatalf("rename result=%#v err=%v", renamed, err)
	}
	pinned, err := source.executeLocalThreadTool(neoPendingTool{Name: "set_thread_pinned", Input: map[string]any{
		"threadId": targetID,
		"pinned":   true,
	}})
	if err != nil || pinned["pinned"] != true {
		t.Fatalf("pin result=%#v err=%v", pinned, err)
	}
	added, err := source.executeLocalThreadTool(neoPendingTool{Name: "add_thread_labels", Input: map[string]any{
		"threadId": targetID,
		"labels":   []any{"review", "keep"},
	}})
	if err != nil || !reflect.DeepEqual(stringArrayValue(added["labels"]), []any{"triage", "Keep", "review"}) {
		t.Fatalf("add labels result=%#v err=%v", added, err)
	}
	removed, err := source.executeLocalThreadTool(neoPendingTool{Name: "remove_thread_labels", Input: map[string]any{
		"threadId": targetID,
		"labels":   []any{"TRIAGE", "missing"},
	}})
	if err != nil || !reflect.DeepEqual(stringArrayValue(removed["labels"]), []any{"Keep", "review"}) {
		t.Fatalf("remove labels result=%#v err=%v", removed, err)
	}

	target.mu.Lock()
	title := target.title
	pinnedState := target.pinned
	target.mu.Unlock()
	if title != "Investigate callbacks" || !pinnedState || !reflect.DeepEqual(target.threadLabels(), []string{"Keep", "review"}) {
		t.Fatalf("target metadata title=%q pinned=%v labels=%#v", title, pinnedState, target.threadLabels())
	}
	if _, err := source.executeLocalThreadTool(neoPendingTool{Name: "set_thread_pinned", Input: map[string]any{"threadId": targetID}}); err == nil {
		t.Fatal("set_thread_pinned accepted a missing pinned value")
	}
	if _, err := source.executeLocalThreadTool(neoPendingTool{Name: "rename_thread", Input: map[string]any{"title": strings.Repeat("x", 257)}}); err == nil {
		t.Fatal("rename_thread accepted an oversized title")
	}
	if _, err := source.executeLocalThreadTool(neoPendingTool{Name: "remove_thread_labels", Input: map[string]any{"labels": []any{}}}); err == nil {
		t.Fatal("remove_thread_labels accepted empty labels")
	}
	updated, err := source.executeLocalThreadTool(neoPendingTool{Name: "update_thread", Input: map[string]any{
		"threadId": targetID,
		"title":    "Ready for review",
		"pinned":   false,
		"labels":   []any{"reviewed"},
	}})
	if err != nil || stringValue(updated["title"]) != "Ready for review" || updated["pinned"] != false || !reflect.DeepEqual(stringArrayValue(updated["labels"]), []any{"reviewed"}) {
		t.Fatalf("update_thread result=%#v err=%v", updated, err)
	}
	target.mu.Lock()
	target.settings["reasoning.effort"] = "max"
	target.mu.Unlock()
	metadata, err := source.executeLocalThreadTool(neoPendingTool{Name: "get_thread_metadata", Input: map[string]any{"threadId": targetID}})
	if err != nil || stringValue(metadata["threadId"]) != targetID || stringValue(metadata["title"]) != "Ready for review" || metadata["pinned"] != false || stringValue(metadata["reasoningEffort"]) != "max" || !reflect.DeepEqual(stringArrayValue(metadata["labels"]), []any{"reviewed"}) {
		t.Fatalf("get_thread_metadata result=%#v err=%v", metadata, err)
	}
	if _, err := source.executeLocalThreadTool(neoPendingTool{Name: "update_thread", Input: map[string]any{"threadId": targetID}}); err == nil {
		t.Fatal("update_thread accepted no updates")
	}
	if _, err := source.executeLocalThreadTool(neoPendingTool{Name: "update_thread", Input: map[string]any{"threadId": targetID, "labels": []any{"one"}, "addLabels": []any{"two"}}}); err == nil {
		t.Fatal("update_thread accepted conflicting label updates")
	}
	if _, err := source.executeLocalThreadTool(neoPendingTool{Name: "update_thread", Input: map[string]any{"threadId": targetID, "labels": []any{42}}}); err == nil {
		t.Fatal("update_thread accepted a non-string label")
	}
	interacted, err := source.executeLocalThreadTool(neoPendingTool{Name: "thread_interact", Input: map[string]any{"action": "get", "thread": targetID}})
	if err != nil || stringValue(interacted["threadID"]) != targetID || stringValue(interacted["url"]) != "https://ampcode.com/threads/"+targetID {
		t.Fatalf("thread_interact get result=%#v err=%v", interacted, err)
	}
	interacted, err = source.executeLocalThreadTool(neoPendingTool{Name: "thread_interact", Input: map[string]any{"action": "archive", "thread": targetID}})
	if err != nil || interacted["archived"] != true {
		t.Fatalf("thread_interact archive result=%#v err=%v", interacted, err)
	}
	interacted, err = source.executeLocalThreadTool(neoPendingTool{Name: "thread_interact", Input: map[string]any{"action": "unarchive", "thread": targetID}})
	if err != nil || interacted["archived"] != false {
		t.Fatalf("thread_interact unarchive result=%#v err=%v", interacted, err)
	}
	if _, err := source.executeLocalThreadTool(neoPendingTool{Name: "thread_interact", Input: map[string]any{"action": "unknown"}}); err == nil {
		t.Fatal("thread_interact accepted an unknown action")
	}
}

func TestNeoUpdateThreadAppliesMultiFieldMutationsAtomically(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	source := rt.store.ensureThreadActor("T-019f7000-0000-7000-8000-000000000085")
	targetID := "T-019f7000-0000-7000-8000-000000000086"
	target := rt.store.ensureThreadActor(targetID)
	updates := []map[string]any{
		{"threadId": targetID, "title": "Atomic alpha", "pinned": true, "labels": []any{"alpha"}},
		{"threadId": targetID, "title": "Atomic beta", "pinned": false, "labels": []any{"beta"}},
	}

	for range 50 {
		start := make(chan struct{})
		results := make([]map[string]any, len(updates))
		errs := make([]error, len(updates))
		var group sync.WaitGroup
		for index, update := range updates {
			group.Add(1)
			go func() {
				defer group.Done()
				<-start
				results[index], errs[index] = source.executeLocalUpdateThreadTool(update)
			}()
		}
		close(start)
		group.Wait()
		for index, result := range results {
			if errs[index] != nil || result["title"] != updates[index]["title"] || result["pinned"] != updates[index]["pinned"] || !reflect.DeepEqual(neoThreadLabelsFromAny(result["labels"]), neoThreadLabelsFromAny(updates[index]["labels"])) {
				t.Fatalf("update %d result=%#v err=%v want=%#v", index, result, errs[index], updates[index])
			}
		}
		target.mu.Lock()
		final := neoLocalThreadMetadataLocked(target, targetID)
		target.mu.Unlock()
		matches := false
		for _, update := range updates {
			if final["title"] == update["title"] && final["pinned"] == update["pinned"] && reflect.DeepEqual(neoThreadLabelsFromAny(final["labels"]), neoThreadLabelsFromAny(update["labels"])) {
				matches = true
			}
		}
		if !matches {
			t.Fatalf("interleaved final metadata = %#v", final)
		}
	}
}

func TestNeoThreadToolsHydratePersistedMetadata(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	rt.threadDir = t.TempDir()
	source := rt.store.ensureThreadActor("T-019f7000-0000-7000-8000-000000000091")
	ownerUserID := "user-primary"
	source.mu.Lock()
	source.meta["creatorUserID"] = ownerUserID
	source.meta["ownerUserId"] = ownerUserID
	source.mu.Unlock()
	rt.legacyOwnerMigrationMu.Lock()
	rt.legacyOwnerMigrationUser = ownerUserID
	rt.legacyOwnerMigrationMu.Unlock()
	targetID := "T-019f7000-0000-7000-8000-000000000092"
	workingDirectory := t.TempDir()
	_, err := writeNeoLocalThreadFileInDir(rt.threadDir, targetID, map[string]any{
		"id":        targetID,
		"title":     "Persisted target",
		"archived":  true,
		"agentMode": "deep",
		"meta": map[string]any{
			"ownerUserId":         neoLocalOwnerUserID,
			"labels":              []any{"reviewed"},
			"projectID":           "project-persisted",
			"cliProxyAPILocalNeo": true,
		},
		"settings": map[string]any{"agentMode": "deep", "reasoning.effort": "medium"},
		"env":      map[string]any{"workingDirectory": workingDirectory, "workspaceRoot": workingDirectory},
		"messages": []any{},
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := source.executeLocalThreadTool(neoPendingTool{Name: "thread_interact", Input: map[string]any{"action": "get", "thread": targetID}})
	if err != nil {
		t.Fatal(err)
	}
	if result["title"] != "Persisted target" || result["archived"] != true || result["projectID"] != "project-persisted" || result["agentMode"] != "deep" || result["reasoningEffort"] != "medium" {
		t.Fatalf("persisted metadata = %#v", result)
	}
	if labels := neoThreadLabelsFromAny(result["labels"]); !reflect.DeepEqual(labels, []string{"reviewed"}) {
		t.Fatalf("persisted labels = %#v", labels)
	}
	if result["workingDirectory"] != neoExistingDirectory(workingDirectory) {
		t.Fatalf("persisted working directory = %#v", result["workingDirectory"])
	}
	target := rt.store.lookupThreadActor(targetID)
	if target == nil {
		t.Fatal("persisted target actor was not hydrated")
	}
	if got := target.threadToolOwnerID(); got != ownerUserID {
		t.Fatalf("persisted target owner = %q, want %q", got, ownerUserID)
	}
	persisted, ok := loadNeoThreadFromDir(targetID, rt.threadDir)
	if !ok || neoThreadOwnerUserID(persisted) != ownerUserID {
		t.Fatalf("persisted target owner was not durably claimed: %#v", persisted)
	}
	target.mu.Lock()
	target.title = "Current in-memory target"
	target.mu.Unlock()
	if _, err := writeNeoLocalThreadFileInDir(rt.threadDir, targetID, map[string]any{
		"id":        targetID,
		"title":     "Stale persisted target",
		"agentMode": "deep",
		"meta": map[string]any{
			"ownerUserId":         ownerUserID,
			"cliProxyAPILocalNeo": true,
		},
	}); err != nil {
		t.Fatal(err)
	}
	result, err = source.executeLocalThreadTool(neoPendingTool{Name: "thread_interact", Input: map[string]any{"action": "get", "thread": targetID}})
	if err != nil {
		t.Fatal(err)
	}
	if result["title"] != "Current in-memory target" {
		t.Fatalf("warm target metadata = %#v", result)
	}
}

func TestNeoThreadLabelRelativeUpdatesPreserveConcurrentAdditions(t *testing.T) {
	for range 20 {
		actor := &neoActor{meta: map[string]any{"labels": []string{"drop"}}}
		start := make(chan struct{})
		var group sync.WaitGroup
		for index := range 16 {
			label := "keep-" + strings.Repeat("x", index+1)
			group.Add(2)
			go func() {
				defer group.Done()
				<-start
				actor.addThreadLabels([]string{label})
			}()
			go func() {
				defer group.Done()
				<-start
				actor.removeThreadLabels([]string{"drop"})
			}()
		}
		close(start)
		group.Wait()
		labels := actor.threadLabels()
		if len(labels) != 16 {
			t.Fatalf("concurrent labels = %#v, want every unmentioned addition", labels)
		}
	}
}

func TestNeoActorRunsThreadInteractMessageLocallyWhenExecutorOmitsIt(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	sourceID := "T-019e1046-656d-7132-879f-390ded941c42"
	targetID := "T-019e1046-656d-7132-879f-390ded941c43"
	source := rt.store.ensureThreadActor(sourceID)
	target := rt.store.ensureThreadActor(targetID)
	source.executorBootstrapComplete = true

	pending := neoPendingTool{ID: "TU-send", Name: "thread_interact", Input: map[string]any{"action": "message", "thread": targetID, "workflow": "code_review"}, AgentMode: "puck", MessageID: "M-assistant"}
	source.pendingTools[pending.ID] = pending
	source.agentState = "running_tools"
	source.runLocalActorTool(pending, source.generation)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		target.mu.Lock()
		if len(target.queue) > 0 {
			queued := target.queue[0]
			target.mu.Unlock()
			if got := textFromBlocks(queued.Content); got != "Review the changes with the code review tool." {
				t.Fatalf("queued content = %q", got)
			}
			if got := stringValue(mapValue(queued.queueProtocol()["queuedMessage"])["parentToolUseId"]); got != pending.ID {
				t.Fatalf("queued protocol parent tool id = %q, want %s", got, pending.ID)
			}
			target.mu.Lock()
			stored, _, _ := target.storeQueuedUserMessageLocked(queued, false)
			target.mu.Unlock()
			if stored.ParentToolUseID != pending.ID {
				t.Fatalf("stored parent tool id = %q, want %s", stored.ParentToolUseID, pending.ID)
			}
			if queued.AgentMode != "review" || queued.ReasoningEffort != "medium" {
				t.Fatalf("queued mode = %q/%q", queued.AgentMode, queued.ReasoningEffort)
			}
			break
		}
		target.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	target.mu.Lock()
	queueLen := len(target.queue)
	target.mu.Unlock()
	if queueLen == 0 {
		t.Fatal("timed out waiting for target thread queue")
	}

	source.mu.Lock()
	_, stillPending := source.pendingTools[pending.ID]
	var result *neoMessage
	for i := range source.messages {
		if source.messages[i].MessageID == toolResultMessageID(pending.ID) {
			message := source.messages[i]
			result = &message
		}
	}
	source.mu.Unlock()
	if stillPending || result == nil || len(result.Content) == 0 {
		t.Fatalf("send tool did not complete: pending=%v result=%#v", stillPending, result)
	}
	run := mapValue(mapValue(result.Content[0])["run"])
	if stringValue(run["status"]) != "done" || !strings.Contains(runToText(run), "code_review") {
		t.Fatalf("send run = %#v", run)
	}
}

func TestNeoThreadToolSendMessageFailsClosedWithoutRuntime(t *testing.T) {
	actor := &neoActor{threadID: "T-019e1046-656d-7132-879f-390ded941c47"}
	_, err := actor.executeLocalSendMessageToThreadTool(neoPendingTool{
		ID:   "TU-send-missing-runtime",
		Name: "send_message_to_thread",
		Input: map[string]any{
			"targetThreadId": "T-019e1046-656d-7132-879f-390ded941c48",
			"message":        "hello",
		},
	})
	if err == nil || !strings.Contains(err.Error(), "missing local runtime") {
		t.Fatalf("error = %v, want missing local runtime", err)
	}
}

func TestNeoThreadToolSendMessageRejectsCurrentThread(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	threadID := "T-019e1046-656d-7132-879f-390ded941c49"
	actor := rt.store.ensureThreadActor(threadID)

	_, err := actor.executeLocalSendMessageToThreadTool(neoPendingTool{
		ID:   "TU-send-current",
		Name: "send_message_to_thread",
		Input: map[string]any{
			"targetThreadId": threadID,
			"workflow":       "code_review",
		},
	})
	if err == nil || !strings.Contains(err.Error(), "cannot target the current thread") {
		t.Fatalf("error = %v, want current-thread rejection", err)
	}
	actor.mu.Lock()
	queueLen := len(actor.queue)
	relationships := append([]map[string]any(nil), actor.relationships...)
	actor.mu.Unlock()
	if queueLen != 0 || len(relationships) != 0 {
		t.Fatalf("self-target mutated actor: queue=%d relationships=%#v", queueLen, relationships)
	}
}

func TestNeoThreadToolCreatesThreadAndArchivesMultiple(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	sourceID := "T-019e1046-656d-7132-879f-390ded941c44"
	createdID := "T-019e1046-656d-7132-879f-390ded941c45"
	otherID := "T-019e1046-656d-7132-879f-390ded941c46"
	source := rt.store.ensureThreadActor(sourceID)

	created, err := source.executeLocalCreateThreadTool(neoPendingTool{Input: map[string]any{"threadId": createdID, "spawnExecutor": false}})
	if err != nil {
		t.Fatalf("create_thread error: %v", err)
	}
	if stringValue(created["threadId"]) != createdID || stringValue(created["threadID"]) != createdID || stringValue(created["url"]) != "https://ampcode.com/threads/"+createdID {
		t.Fatalf("created result = %#v", created)
	}
	createdActor := rt.store.ensureThreadActor(createdID)
	otherActor := rt.store.ensureThreadActor(otherID)
	archived, err := source.executeLocalArchiveThreadsTool(map[string]any{"threadIds": []any{createdID, "https://ampcode.com/threads/" + otherID}})
	if err != nil {
		t.Fatalf("archive_threads error: %v", err)
	}
	if len(arrayValue(archived["archived"])) != 2 {
		t.Fatalf("archive_threads result = %#v", archived)
	}
	createdActor.mu.Lock()
	createdArchived := createdActor.archived
	createdActor.mu.Unlock()
	otherActor.mu.Lock()
	otherArchived := otherActor.archived
	otherActor.mu.Unlock()
	if !createdArchived || !otherArchived {
		t.Fatalf("archived flags created=%v other=%v", createdArchived, otherArchived)
	}
}

func TestNeoCreateThreadInheritsContextAndChildReportsBack(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	parentID := "T-019f7000-0000-7000-8000-000000000001"
	childID := "T-019f7000-0000-7000-8000-000000000002"
	projectID := "019f7000-0000-7000-8000-000000000003"
	workDir := neoExistingDirectory(t.TempDir())
	parent := rt.store.ensureThreadActor(parentID)
	parent.updateEnvironment(map[string]any{"workingDirectory": workDir})
	parent.updateSettings(map[string]any{"agentMode": "high", "reasoning.effort": "high"})
	parent.mu.Lock()
	parent.meta["projectID"] = projectID
	parent.meta["repositoryURL"] = "https://github.com/aikins01/example"
	parent.mu.Unlock()

	created, err := parent.executeLocalCreateThreadTool(neoPendingTool{
		ID: "TU-create-child",
		Input: map[string]any{
			"threadId":        childID,
			"agentMode":       "low",
			"reasoningEffort": "low",
			"prompt":          "Investigate the parser and report back.",
			"spawnExecutor":   false,
		},
	})
	if err != nil {
		t.Fatalf("create_thread error: %v", err)
	}
	if created["threadId"] != childID || created["projectID"] != projectID || created["workingDirectory"] != workDir {
		t.Fatalf("create_thread result = %#v", created)
	}
	child := rt.store.lookupThreadActor(childID)
	if child == nil {
		t.Fatal("child actor was not created")
	}
	child.mu.Lock()
	childDirectory := neoWorkingDirectoryFromEnvironment(child.environment)
	childMode := child.currentAgentMode
	childEffort := child.currentReasoningEffort
	childProjectID := stringValue(child.meta["projectID"])
	childParentID := stringValue(child.meta["parentThreadID"])
	childExecutorType := child.bootstrapExecutorType
	childQueue := append([]neoQueuedMessage(nil), child.queue...)
	childRelationships := append([]map[string]any(nil), child.relationships...)
	child.mu.Unlock()
	if childDirectory != workDir || childMode != "low" || childEffort != "medium" || childProjectID != projectID || childParentID != parentID || childExecutorType != "" {
		t.Fatalf("child context dir=%q mode=%q effort=%q project=%q parent=%q executor=%q", childDirectory, childMode, childEffort, childProjectID, childParentID, childExecutorType)
	}
	if _, _, pending := child.pendingWebLocalExecutorRequest(); pending {
		t.Fatal("spawnExecutor false left the child eligible for a later executor spawn")
	}
	if len(childQueue) != 1 || textFromBlocks(childQueue[0].Content) != "Investigate the parser and report back." {
		t.Fatalf("child queue = %#v", childQueue)
	}
	if len(childRelationships) != 1 || stringValue(childRelationships[0]["threadID"]) != parentID || stringValue(childRelationships[0]["role"]) != "parent" {
		t.Fatalf("child relationships = %#v", childRelationships)
	}

	_, err = child.executeLocalSendMessageToThreadTool(neoPendingTool{
		ID: "TU-report-back",
		Input: map[string]any{
			"thread":  parentID,
			"message": "Parser investigation complete.",
		},
	})
	if err != nil {
		t.Fatalf("child report error: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		parent.mu.Lock()
		queue := append([]neoQueuedMessage(nil), parent.queue...)
		parent.mu.Unlock()
		if len(queue) > 0 {
			if got := textFromBlocks(queue[0].Content); got != "Parser investigation complete." {
				t.Fatalf("parent report = %q", got)
			}
			if queue[0].ParentToolUseID != "TU-report-back" {
				t.Fatalf("parent report tool linkage = %q", queue[0].ParentToolUseID)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for child report")
}

func TestNeoCreateThreadPreservesDistinctParentWorkspaceRoot(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	workspaceRoot := neoExistingDirectory(t.TempDir())
	workingDirectory := filepath.Join(workspaceRoot, "internal", "parser")
	if err := os.MkdirAll(workingDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	workingDirectory = neoExistingDirectory(workingDirectory)
	parent := rt.store.ensureThreadActor("T-019f7000-0000-7000-8000-0000000000a4")
	parent.updateEnvironment(map[string]any{
		"workingDirectory": workingDirectory,
		"workspaceRoot":    workspaceRoot,
	})
	created, err := parent.executeLocalCreateThreadTool(neoPendingTool{Input: map[string]any{
		"threadId":      "T-019f7000-0000-7000-8000-0000000000a5",
		"spawnExecutor": false,
	}})
	if err != nil {
		t.Fatal(err)
	}
	child := rt.store.lookupThreadActor(stringValue(created["threadId"]))
	if child == nil {
		t.Fatal("child actor was not created")
	}
	child.mu.Lock()
	childWorkingDirectory, childWorkspaceRoot := neoResolvedEnvironmentWorkspacePaths(child.environment)
	child.mu.Unlock()
	if childWorkingDirectory != workingDirectory || childWorkspaceRoot != workspaceRoot {
		t.Fatalf("child workspace = %q root=%q, want %q root=%q", childWorkingDirectory, childWorkspaceRoot, workingDirectory, workspaceRoot)
	}
}

func TestNeoCreateThreadDispatchesToSelectedRunner(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	parentID := "T-019f7000-0000-7000-8000-000000000011"
	childID := "T-019f7000-0000-7000-8000-000000000012"
	workDir := filepath.Join(t.TempDir(), "remote-runner-only", "project")
	if _, err := os.Stat(workDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("remote runner path unexpectedly exists on proxy host: %v", err)
	}
	userActor, _ := rt.store.upsert(map[string]any{"name": "userActor", "key": "user-local"}, true)
	runnerSocket := &neoSocket{runnerID: "build-runner"}
	registered := mapValue(userActor.handleForSocket(runnerSocket, map[string]any{
		"type": "registerRunner",
		"args": []any{map[string]any{
			"sessionId":        "session-build-runner",
			"hostname":         "Build Host",
			"workingDirectory": workDir,
			"runningThreads":   []any{},
		}},
	}))
	if registered["ok"] != true {
		t.Fatalf("runner registration = %#v", registered)
	}
	parent := rt.store.ensureThreadActor(parentID)
	parentDirectory := neoExistingDirectory(t.TempDir())
	parent.updateEnvironment(map[string]any{"workingDirectory": parentDirectory})
	parent.mu.Lock()
	parent.meta["ownerUserId"] = "user-local"
	parent.meta["projectID"] = "019f7000-0000-7000-8000-000000000010"
	parent.meta["repositoryURL"] = "https://github.com/aikins01/parent.git"
	parent.mu.Unlock()
	created, err := parent.executeLocalCreateThreadTool(neoPendingTool{Input: map[string]any{
		"threadId":  childID,
		"runnerId":  "build-runner",
		"agentMode": "low",
	}})
	if err != nil {
		t.Fatalf("create_thread runner error: %v", err)
	}
	if created["runnerId"] != "build-runner" || created["workingDirectory"] != workDir {
		t.Fatalf("create_thread runner result = %#v", created)
	}
	child := rt.store.lookupThreadActor(childID)
	if child == nil {
		t.Fatal("runner child was not created")
	}
	child.mu.Lock()
	childDirectory := stringValue(child.environment["workingDirectory"])
	childProjectID := stringValue(child.meta["projectID"])
	childRepositoryURL := stringValue(child.meta["repositoryURL"])
	child.mu.Unlock()
	if childDirectory != workDir || child.localThreadToolWorkingDirectory() != "" {
		t.Fatalf("runner child directory = %q, proxy-local directory = %q", childDirectory, child.localThreadToolWorkingDirectory())
	}
	if childProjectID == "019f7000-0000-7000-8000-000000000010" || childRepositoryURL == "https://github.com/aikins01/parent.git" {
		t.Fatalf("runner child retained parent project metadata: project=%q repository=%q", childProjectID, childRepositoryURL)
	}
	inheritedBody, inheritedRunnerID, _, err := child.localCreateThreadBody(map[string]any{
		"workingDirectory": workDir,
	}, "T-019f7000-0000-7000-8000-000000000017")
	if err != nil {
		t.Fatalf("create_thread inherited runner error: %v", err)
	}
	if inheritedRunnerID != "build-runner" || stringValue(inheritedBody["workingDirectory"]) != workDir {
		t.Fatalf("create_thread inherited runner body = %#v, runner = %q", inheritedBody, inheritedRunnerID)
	}
	child.maybeSpawnWebLocalExecutorForPendingWork()
	heartbeat := mapValue(userActor.handleForSocket(runnerSocket, map[string]any{
		"type": "runnerHeartbeat",
		"args": []any{map[string]any{"sessionId": "session-build-runner", "runningThreads": []any{}}},
	}))
	intents := arrayValue(heartbeat["intents"])
	if heartbeat["ok"] != true || len(intents) != 1 {
		t.Fatalf("runner should receive child intent: %#v", heartbeat)
	}
	intent := mapValue(intents[0])
	if intent["threadId"] != childID || intent["desired"] != "running" || intent["agentMode"] != "low" || intent["reasoningEffort"] != "medium" {
		t.Fatalf("runner child intent = %#v", intent)
	}
	if _, err := parent.executeLocalSendMessageToThreadTool(neoPendingTool{ID: "TU-start-runner-child", Input: map[string]any{
		"thread":  childID,
		"message": "Run the test matrix.",
	}}); err != nil {
		t.Fatalf("send child task error: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		heartbeat = mapValue(userActor.handleForSocket(runnerSocket, map[string]any{
			"type": "runnerHeartbeat",
			"args": []any{map[string]any{"sessionId": "session-build-runner", "runningThreads": []any{}}},
		}))
		if len(arrayValue(heartbeat["intents"])) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	intents = arrayValue(heartbeat["intents"])
	if heartbeat["ok"] != true || len(intents) != 1 || stringValue(mapValue(intents[0])["threadId"]) != childID || stringValue(mapValue(intents[0])["desired"]) != "running" {
		t.Fatalf("runner heartbeat = %#v", heartbeat)
	}
}

func TestNeoCreateThreadReportsRunnerDispatchAsRequested(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	userActor, _ := rt.store.upsert(map[string]any{"name": "userActor", "key": "user-local"}, true)
	runnerSocket := &neoSocket{runnerID: "build-runner"}
	registered := mapValue(userActor.handleForSocket(runnerSocket, map[string]any{
		"type": "registerRunner",
		"args": []any{map[string]any{
			"sessionId":        "session-build-runner",
			"workingDirectory": "/remote/runner/project",
			"runningThreads":   []any{},
		}},
	}))
	if registered["ok"] != true {
		t.Fatalf("runner registration = %#v", registered)
	}
	parent := rt.store.ensureThreadActor("T-019f7000-0000-7000-8000-000000000018")
	parent.mu.Lock()
	parent.meta["ownerUserId"] = "user-local"
	parent.mu.Unlock()
	created, err := parent.executeLocalCreateThreadTool(neoPendingTool{Input: map[string]any{
		"threadId": "T-019f7000-0000-7000-8000-000000000019",
		"runnerId": "build-runner",
		"prompt":   "Run the test matrix.",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if created["executorRequested"] != true || created["executorStarted"] != false || created["executorError"] != nil {
		t.Fatalf("create runner status = %#v", created)
	}
}

func TestNeoCreateThreadUsesExplicitProjectRepository(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	parentID := "T-019f7000-0000-7000-8000-000000000031"
	childID := "T-019f7000-0000-7000-8000-000000000032"
	parentProjectID := "019f7000-0000-7000-8000-000000000033"
	targetProjectID := "019f7000-0000-7000-8000-000000000034"
	targetDirectory := neoExistingDirectory(t.TempDir())
	rt.projectIndexMu.Lock()
	rt.projectIndexLoaded = true
	rt.projectIndexCache = []any{map[string]any{
		"id":               targetProjectID,
		"workingDirectory": targetDirectory,
		"repositoryURL":    "https://github.com/aikins01/target.git",
	}}
	rt.projectIndexMu.Unlock()
	target := rt.store.ensureThreadActor("T-019f7000-0000-7000-8000-00000000003a")
	target.updateEnvironment(map[string]any{"workingDirectory": targetDirectory})
	target.mu.Lock()
	target.meta["projectID"] = targetProjectID
	target.mu.Unlock()
	parent := rt.store.ensureThreadActor(parentID)
	parent.mu.Lock()
	parent.meta["projectID"] = parentProjectID
	parent.meta["repositoryURL"] = "https://github.com/aikins01/parent.git"
	parent.mu.Unlock()

	body, _, _, err := parent.localCreateThreadBody(map[string]any{
		"threadId":      childID,
		"projectID":     targetProjectID,
		"spawnExecutor": false,
	}, childID)
	if err != nil {
		t.Fatalf("create_thread body error: %v", err)
	}
	meta := mapValue(body["threadMeta"])
	if stringValue(body["workingDirectory"]) != targetDirectory || stringValue(meta["repositoryURL"]) != "https://github.com/aikins01/target.git" {
		t.Fatalf("explicit project body = %#v", body)
	}
	_, _, _, err = parent.localCreateThreadBody(map[string]any{
		"projectID":        targetProjectID,
		"workingDirectory": neoExistingDirectory(t.TempDir()),
	}, "T-019f7000-0000-7000-8000-000000000035")
	if err == nil || !strings.Contains(err.Error(), "does not match projectID") {
		t.Fatalf("mismatched project working directory error = %v", err)
	}
}

func TestNeoCreateThreadRejectsCloudProjectWithoutOwnerCheckout(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	projectID := "019f7000-0000-7000-8000-00000000003b"
	rt.projectIndexMu.Lock()
	rt.projectIndexLoaded = true
	rt.projectIndexCache = []any{map[string]any{
		"id":            projectID,
		"repositoryURL": "https://github.com/aikins01/cloud-only.git",
	}}
	rt.projectIndexMu.Unlock()
	parent := rt.store.ensureThreadActor("T-019f7000-0000-7000-8000-00000000003c")
	parent.updateEnvironment(map[string]any{"workingDirectory": neoExistingDirectory(t.TempDir())})

	body, _, _, err := parent.localCreateThreadBody(map[string]any{
		"projectID":     projectID,
		"spawnExecutor": false,
	}, "T-019f7000-0000-7000-8000-00000000003d")
	if err == nil || !strings.Contains(err.Error(), "does not have a local checkout available to the thread owner") {
		t.Fatalf("body=%#v error=%v", body, err)
	}
}

func TestNeoCreateThreadRejectsForeignOwnerIndexedWorkspace(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	projectID := "019f7000-0000-7000-8000-00000000003e"
	foreignDirectory := neoExistingDirectory(t.TempDir())
	rt.projectIndexMu.Lock()
	rt.projectIndexLoaded = true
	rt.projectIndexCache = []any{map[string]any{
		"id":               projectID,
		"workingDirectory": foreignDirectory,
		"repositoryURL":    neoFileURLForDirectory(foreignDirectory),
	}}
	rt.projectIndexMu.Unlock()
	foreign := rt.store.ensureThreadActor("T-019f7000-0000-7000-8000-00000000003f")
	foreign.updateEnvironment(map[string]any{"workingDirectory": foreignDirectory})
	foreign.mu.Lock()
	foreign.meta["ownerUserId"] = "user-b"
	foreign.meta["projectID"] = projectID
	foreign.mu.Unlock()
	parent := rt.store.ensureThreadActor("T-019f7000-0000-7000-8000-000000000041")
	parent.updateEnvironment(map[string]any{"workingDirectory": neoExistingDirectory(t.TempDir())})
	parent.mu.Lock()
	parent.meta["ownerUserId"] = "user-a"
	parent.mu.Unlock()

	for _, input := range []map[string]any{
		{"workingDirectory": foreignDirectory, "spawnExecutor": false},
		{"projectID": projectID, "spawnExecutor": false},
	} {
		if _, _, _, err := parent.localCreateThreadBody(input, "T-019f7000-0000-7000-8000-000000000042"); err == nil {
			t.Fatalf("foreign workspace input %#v was accepted", input)
		}
	}
}

func TestNeoCreateThreadReResolvesProjectForExplicitWorkspace(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	parent := rt.store.ensureThreadActor("T-019f7000-0000-7000-8000-000000000036")
	parentDirectory := neoExistingDirectory(t.TempDir())
	targetDirectory := neoExistingDirectory(t.TempDir())
	parent.updateEnvironment(map[string]any{"workingDirectory": parentDirectory})
	parent.mu.Lock()
	parent.meta["projectID"] = "019f7000-0000-7000-8000-000000000037"
	parent.meta["repositoryURL"] = "https://github.com/aikins01/parent.git"
	parent.mu.Unlock()
	rt.projectIndexMu.Lock()
	rt.projectIndexLoaded = true
	rt.projectIndexCache = []any{map[string]any{
		"id":               "019f7000-0000-7000-8000-000000000039",
		"workingDirectory": targetDirectory,
		"repositoryURL":    neoFileURLForDirectory(targetDirectory),
	}}
	rt.projectIndexMu.Unlock()
	target := rt.store.ensureThreadActor("T-019f7000-0000-7000-8000-000000000043")
	target.updateEnvironment(map[string]any{"workingDirectory": targetDirectory})
	target.mu.Lock()
	target.meta["projectID"] = "019f7000-0000-7000-8000-000000000039"
	target.mu.Unlock()

	body, _, _, err := parent.localCreateThreadBody(map[string]any{
		"workingDirectory": targetDirectory,
	}, "T-019f7000-0000-7000-8000-000000000038")
	if err != nil {
		t.Fatal(err)
	}
	meta := mapValue(body["threadMeta"])
	if stringValue(body["workingDirectory"]) != targetDirectory || stringValue(body["projectID"]) == "019f7000-0000-7000-8000-000000000037" || stringValue(meta["repositoryURL"]) == "https://github.com/aikins01/parent.git" {
		t.Fatalf("explicit workspace retained parent project metadata: %#v", body)
	}
}

func TestNeoCreateThreadConfinesExplicitLocalWorkspace(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	baseDirectory := t.TempDir()
	parentDirectory := filepath.Join(baseDirectory, "project")
	childDirectory := filepath.Join(parentDirectory, "child")
	if err := os.MkdirAll(childDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	parent := rt.store.ensureThreadActor("T-019f7000-0000-7000-8000-000000000040")
	parent.updateEnvironment(map[string]any{"workingDirectory": parentDirectory})
	parentProjectID := "019f7000-0000-7000-8000-000000000044"
	parent.mu.Lock()
	parent.meta["projectID"] = parentProjectID
	parent.meta["repositoryURL"] = neoFileURLForDirectory(parentDirectory)
	parent.mu.Unlock()
	rt.projectIndexMu.Lock()
	rt.projectIndexLoaded = true
	rt.projectIndexCache = nil
	rt.projectIndexMu.Unlock()

	body, _, _, err := parent.localCreateThreadBody(map[string]any{
		"workingDirectory": childDirectory,
		"spawnExecutor":    false,
	}, "T-019f7000-0000-7000-8000-00000000004a")
	meta := mapValue(body["threadMeta"])
	if err != nil || stringValue(body["workingDirectory"]) != neoExistingDirectory(childDirectory) || stringValue(body["workspaceRoot"]) != neoExistingDirectory(parentDirectory) || stringValue(body["projectID"]) != parentProjectID || stringValue(meta["repositoryURL"]) != neoFileURLForDirectory(parentDirectory) {
		t.Fatalf("child workspace body=%#v err=%v", body, err)
	}
	for _, directory := range []string{baseDirectory, string(filepath.Separator)} {
		_, _, _, err := parent.localCreateThreadBody(map[string]any{
			"workingDirectory": directory,
			"spawnExecutor":    false,
		}, "T-019f7000-0000-7000-8000-00000000004b")
		if err == nil || !strings.Contains(err.Error(), "current or an indexed project workspace") {
			t.Fatalf("workspace %q error = %v", directory, err)
		}
	}
}

func TestNeoCreateThreadRequiresRunnerIDForRunnerExecutor(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	parent := rt.store.ensureThreadActor("T-019f7000-0000-7000-8000-000000000013")
	_, _, _, err := parent.localCreateThreadBody(map[string]any{
		"executor": "runner",
	}, "T-019f7000-0000-7000-8000-000000000014")
	if err == nil || !strings.Contains(err.Error(), "requires runnerId") {
		t.Fatalf("error = %v, want missing runnerId rejection", err)
	}
}

func TestNeoCreateThreadExplicitLocalDoesNotInheritParentRunner(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	parent := rt.store.ensureThreadActor("T-019f7000-0000-7000-8000-000000000015")
	parent.mu.Lock()
	parent.meta["runnerId"] = "parent-runner"
	parent.environment = map[string]any{"workingDirectory": "/remote/runner/worktree"}
	parent.mu.Unlock()

	body, runnerID, _, err := parent.localCreateThreadBody(map[string]any{
		"executor":      "local",
		"spawnExecutor": false,
	}, "T-019f7000-0000-7000-8000-000000000016")
	if err != nil {
		t.Fatalf("create_thread body error: %v", err)
	}
	if runnerID != "" || stringValue(body["workingDirectory"]) != "" || stringValue(body["executorType"]) != "" || stringValue(mapValue(body["threadMeta"])["runnerId"]) != "" {
		t.Fatalf("explicit local body inherited parent runner state: %#v", body)
	}
	_, _, _, err = parent.localCreateThreadBody(map[string]any{
		"executor": "local",
	}, "T-019f7000-0000-7000-8000-000000000017")
	if err == nil || !strings.Contains(err.Error(), "requires an existing workingDirectory") {
		t.Fatalf("explicit local missing workspace error = %v", err)
	}
	_, _, _, err = parent.localCreateThreadBody(map[string]any{
		"executor": "local",
		"runnerId": "parent-runner",
	}, "T-019f7000-0000-7000-8000-000000000018")
	if err == nil || !strings.Contains(err.Error(), "conflicts with runnerId") {
		t.Fatalf("explicit local runner conflict error = %v", err)
	}
}

func TestNeoCreateThreadExplicitLocalUsesOwnedProjectCheckout(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	projectID := "019f7000-0000-7000-8000-0000000000b1"
	parent := rt.store.ensureThreadActor("T-019f7000-0000-7000-8000-0000000000b2")
	parent.mu.Lock()
	parent.meta["ownerUserId"] = "user-a"
	parent.meta["projectID"] = projectID
	parent.meta["runnerId"] = "remote-runner"
	parent.environment = map[string]any{"workingDirectory": neoExistingDirectory(t.TempDir())}
	parent.mu.Unlock()
	_, _, _, err := parent.localCreateThreadBody(map[string]any{
		"executor": "local",
	}, "T-019f7000-0000-7000-8000-0000000000b4")
	if err == nil || !strings.Contains(err.Error(), "requires an existing workingDirectory") {
		t.Fatalf("runner workspace was accepted as a local checkout: %v", err)
	}
	checkoutDirectory := neoExistingDirectory(t.TempDir())
	checkout := rt.store.ensureThreadActor("T-019f7000-0000-7000-8000-0000000000b3")
	checkout.mu.Lock()
	checkout.meta["ownerUserId"] = "user-a"
	checkout.meta["projectID"] = projectID
	checkout.environment = map[string]any{"workingDirectory": checkoutDirectory}
	checkout.mu.Unlock()

	body, runnerID, _, err := parent.localCreateThreadBody(map[string]any{
		"executor": "local",
	}, "T-019f7000-0000-7000-8000-0000000000b5")
	if err != nil {
		t.Fatal(err)
	}
	if runnerID != "" || stringValue(body["workingDirectory"]) != checkoutDirectory || stringValue(mapValue(body["threadMeta"])["runnerId"]) != "" {
		t.Fatalf("explicit local body = %#v, runnerID = %q", body, runnerID)
	}
	secondCheckout := rt.store.ensureThreadActor("T-019f7000-0000-7000-8000-0000000000b6")
	secondCheckout.mu.Lock()
	secondCheckout.meta["ownerUserId"] = "user-a"
	secondCheckout.meta["projectID"] = projectID
	secondCheckout.environment = map[string]any{"workingDirectory": neoExistingDirectory(t.TempDir())}
	secondCheckout.mu.Unlock()
	_, _, _, err = parent.localCreateThreadBody(map[string]any{
		"executor": "local",
	}, "T-019f7000-0000-7000-8000-0000000000b7")
	if err == nil || !strings.Contains(err.Error(), "multiple local project checkouts") {
		t.Fatalf("ambiguous local checkout error = %v", err)
	}
	body, runnerID, _, err = parent.localCreateThreadBody(map[string]any{
		"executor":         "local",
		"workingDirectory": checkoutDirectory,
		"spawnExecutor":    false,
	}, "T-019f7000-0000-7000-8000-0000000000b8")
	if err != nil {
		t.Fatal(err)
	}
	if runnerID != "" || stringValue(body["workingDirectory"]) != checkoutDirectory || stringValue(body["projectID"]) != projectID {
		t.Fatalf("selected local checkout body = %#v, runnerID = %q", body, runnerID)
	}
}

func TestNeoCreateThreadFromPuckDefaultsToMediumAgent(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	parent := rt.store.ensureThreadActor("T-019f7000-0000-7000-8000-00000000009c")
	parent.updateSettings(map[string]any{"agentMode": "puck", "reasoning.effort": "none"})

	body, _, _, err := parent.localCreateThreadBody(map[string]any{
		"spawnExecutor": false,
	}, "T-019f7000-0000-7000-8000-00000000009d")
	if err != nil {
		t.Fatal(err)
	}
	if stringValue(body["agentMode"]) != "medium" || stringValue(body["reasoningEffort"]) != "medium" || len(mapValue(body["agent"])) != 0 {
		t.Fatalf("Puck child body = %#v", body)
	}
}

func TestNeoCreateThreadPreservesInitialImageContent(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	parent := rt.store.ensureThreadActor("T-019f7000-0000-7000-8000-00000000009e")
	parent.updateSettings(map[string]any{"agentMode": "puck", "reasoning.effort": "none"})
	imageData := testNeoPNGBase64(t, 1, 1)
	content := []any{
		map[string]any{"type": "text", "text": "Use this reference image."},
		map[string]any{"type": "image", "source": map[string]any{"type": "base64", "mediaType": "image/png", "data": imageData}, "sourcePath": "reference.png"},
	}

	body, _, _, err := parent.localCreateThreadBody(map[string]any{
		"content":       content,
		"spawnExecutor": false,
	}, "T-019f7000-0000-7000-8000-00000000009f")
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := body["prompt"]; exists {
		t.Fatalf("image child body also contained prompt: %#v", body)
	}
	initialContent := arrayValue(body["content"])
	imageSource := mapValue(mapValue(initialContent[1])["source"])
	if len(initialContent) != 2 || textFromBlocks(initialContent) != "Use this reference image." || stringValue(imageSource["mediaType"]) != "image/png" || stringValue(imageSource["data"]) != imageData {
		t.Fatalf("image child content = %#v", initialContent)
	}
}

func TestNeoCreateThreadRejectsStaleInheritedRunner(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	parent := rt.store.ensureThreadActor("T-019f7000-0000-7000-8000-000000000019")
	parent.mu.Lock()
	parent.meta["runnerId"] = "disconnected-runner"
	parent.environment = map[string]any{"workingDirectory": "/remote/runner/project"}
	parent.mu.Unlock()

	_, _, _, err := parent.localCreateThreadBody(nil, "T-019f7000-0000-7000-8000-000000000020")
	if err == nil || !strings.Contains(err.Error(), "runner is not available") {
		t.Fatalf("stale inherited runner error = %v", err)
	}
}

func TestNeoThreadFileToolsCopyWithinLocalWorkspaces(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	sourceID := "T-019f7000-0000-7000-8000-000000000021"
	targetID := "T-019f7000-0000-7000-8000-000000000022"
	sourceDir := t.TempDir()
	targetDir := t.TempDir()
	if err := os.Mkdir(filepath.Join(targetDir, "incoming"), 0o700); err != nil {
		t.Fatalf("mkdir incoming: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sourceDir, "matrix.txt"), []byte("linux\nmacos\n"), 0o600); err != nil {
		t.Fatalf("write source matrix: %v", err)
	}
	if err := os.Symlink("matrix.txt", filepath.Join(sourceDir, "matrix-link.txt")); err != nil {
		t.Fatalf("symlink source matrix: %v", err)
	}
	source := rt.store.ensureThreadActor(sourceID)
	target := rt.store.ensureThreadActor(targetID)
	source.updateEnvironment(map[string]any{"workingDirectory": sourceDir})
	target.updateEnvironment(map[string]any{"workingDirectory": targetDir})
	if _, err := source.executeLocalUploadThreadFileTool(map[string]any{"thread": sourceID, "path": "matrix.txt"}); err == nil || !strings.Contains(err.Error(), "current thread") {
		t.Fatalf("self upload error = %v, want current thread rejection", err)
	}
	if _, err := source.executeLocalDownloadThreadFileTool(map[string]any{"thread": sourceID, "path": "matrix.txt"}); err == nil || !strings.Contains(err.Error(), "current thread") {
		t.Fatalf("self download error = %v, want current thread rejection", err)
	}

	uploaded, err := source.executeLocalUploadThreadFileTool(map[string]any{
		"thread":      targetID,
		"path":        "matrix.txt",
		"destination": "incoming/matrix.txt",
	})
	if err != nil {
		t.Fatalf("upload_thread_file error: %v", err)
	}
	if uploaded["bytes"] != 12 || uploaded["localPath"] != filepath.Join(neoExistingDirectory(sourceDir), "matrix.txt") || uploaded["remotePath"] != "incoming/matrix.txt" {
		t.Fatalf("upload result = %#v", uploaded)
	}
	remoteRaw, err := os.ReadFile(filepath.Join(targetDir, "incoming", "matrix.txt"))
	if err != nil || string(remoteRaw) != "linux\nmacos\n" {
		t.Fatalf("uploaded file raw=%q err=%v", remoteRaw, err)
	}
	linked, err := source.executeLocalUploadThreadFileTool(map[string]any{
		"thread": targetID,
		"path":   "matrix-link.txt",
	})
	if err != nil {
		t.Fatalf("upload symlink error: %v", err)
	}
	if linked["remotePath"] != "matrix-link.txt" {
		t.Fatalf("upload symlink result = %#v", linked)
	}
	linkedRaw, err := os.ReadFile(filepath.Join(targetDir, "matrix-link.txt"))
	if err != nil || string(linkedRaw) != "linux\nmacos\n" {
		t.Fatalf("uploaded symlink raw=%q err=%v", linkedRaw, err)
	}
	if _, err := source.executeLocalUploadThreadFileTool(map[string]any{
		"thread":      targetID,
		"path":        "matrix.txt",
		"destination": "incoming/matrix.txt",
	}); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("second upload error = %v, want already exists", err)
	}
	if err := os.Chmod(filepath.Join(sourceDir, "matrix.txt"), 0o644); err != nil {
		t.Fatalf("chmod source matrix: %v", err)
	}
	if _, err := source.executeLocalUploadThreadFileTool(map[string]any{
		"thread":      targetID,
		"path":        "matrix.txt",
		"destination": "incoming/matrix.txt",
		"overwrite":   true,
	}); err != nil {
		t.Fatalf("overwrite upload error: %v", err)
	}
	overwrittenInfo, err := os.Stat(filepath.Join(targetDir, "incoming", "matrix.txt"))
	if err != nil || overwrittenInfo.Mode().Perm() != 0o600 {
		t.Fatalf("overwritten mode = %#o err=%v, want 0600", overwrittenInfo.Mode().Perm(), err)
	}
	if _, err := source.executeLocalUploadThreadFileTool(map[string]any{
		"thread":      targetID,
		"path":        "matrix.txt",
		"destination": "../escape.txt",
	}); err == nil || !strings.Contains(err.Error(), "outside the workspace") {
		t.Fatalf("traversal upload error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(targetDir, "result.txt"), []byte("all green"), 0o600); err != nil {
		t.Fatalf("write target result: %v", err)
	}
	downloaded, err := source.executeLocalDownloadThreadFileTool(map[string]any{
		"thread":      targetID,
		"path":        "result.txt",
		"destination": "artifacts/downloaded.txt",
	})
	if err != nil {
		t.Fatalf("download_thread_file error: %v", err)
	}
	if downloaded["bytes"] != 9 || downloaded["localPath"] != filepath.Join(neoExistingDirectory(sourceDir), "artifacts", "downloaded.txt") || downloaded["remotePath"] != "result.txt" {
		t.Fatalf("download result = %#v", downloaded)
	}
	localRaw, err := os.ReadFile(filepath.Join(sourceDir, "artifacts", "downloaded.txt"))
	if err != nil || string(localRaw) != "all green" {
		t.Fatalf("downloaded file raw=%q err=%v", localRaw, err)
	}
}

func TestNeoThreadFileToolsUseWorkspaceRoot(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	sourceRoot := t.TempDir()
	targetRoot := t.TempDir()
	sourceWorkingDirectory := filepath.Join(sourceRoot, "internal", "parser")
	targetWorkingDirectory := filepath.Join(targetRoot, "cmd", "server")
	if err := os.MkdirAll(sourceWorkingDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(targetWorkingDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(sourceRoot, "artifacts"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(targetRoot, "incoming"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sourceRoot, "artifacts", "matrix.txt"), []byte("green\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(targetRoot, "result.txt"), []byte("done\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	source := rt.store.ensureThreadActor("T-019f7000-0000-7000-8000-000000000023")
	target := rt.store.ensureThreadActor("T-019f7000-0000-7000-8000-000000000024")
	source.updateEnvironment(map[string]any{"workingDirectory": sourceWorkingDirectory, "workspaceRoot": sourceRoot})
	target.updateEnvironment(map[string]any{"workingDirectory": targetWorkingDirectory, "workspaceRoot": targetRoot})
	result, err := source.executeLocalUploadThreadFileTool(map[string]any{
		"thread":      target.threadID,
		"path":        "artifacts/matrix.txt",
		"destination": "incoming/matrix.txt",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result["localPath"] != filepath.Join(neoExistingDirectory(sourceRoot), "artifacts", "matrix.txt") {
		t.Fatalf("upload result = %#v", result)
	}
	raw, err := os.ReadFile(filepath.Join(targetRoot, "incoming", "matrix.txt"))
	if err != nil || string(raw) != "green\n" {
		t.Fatalf("uploaded file raw=%q err=%v", raw, err)
	}
	downloaded, err := source.executeLocalDownloadThreadFileTool(map[string]any{
		"thread": target.threadID,
		"path":   "result.txt",
	})
	if err != nil {
		t.Fatal(err)
	}
	wantDownloadPath := filepath.Join(neoExistingDirectory(sourceWorkingDirectory), "result.txt")
	if downloaded["localPath"] != wantDownloadPath {
		t.Fatalf("download result = %#v, want local path %q", downloaded, wantDownloadPath)
	}
	if raw, err := os.ReadFile(wantDownloadPath); err != nil || string(raw) != "done\n" {
		t.Fatalf("downloaded file raw=%q err=%v", raw, err)
	}
	if _, err := os.Stat(filepath.Join(sourceRoot, "result.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("download unexpectedly used workspace root: %v", err)
	}
}

func TestNeoThreadFileToolsIgnoreMismatchedWorkspaceRoot(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	staleRoot := t.TempDir()
	sourceWorkingDirectory := t.TempDir()
	targetRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(sourceWorkingDirectory, "matrix.txt"), []byte("green\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(targetRoot, "incoming"), 0o700); err != nil {
		t.Fatal(err)
	}
	source := rt.store.ensureThreadActor("T-019f7000-0000-7000-8000-000000000025")
	target := rt.store.ensureThreadActor("T-019f7000-0000-7000-8000-000000000026")
	source.updateEnvironment(map[string]any{"workingDirectory": sourceWorkingDirectory, "workspaceRoot": staleRoot})
	target.updateEnvironment(map[string]any{"workingDirectory": targetRoot})
	result, err := source.executeLocalUploadThreadFileTool(map[string]any{
		"thread":      target.threadID,
		"path":        "matrix.txt",
		"destination": "incoming/matrix.txt",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result["localPath"] != filepath.Join(neoExistingDirectory(sourceWorkingDirectory), "matrix.txt") {
		t.Fatalf("upload result = %#v", result)
	}
}

func TestNeoCreateThreadRejectsExistingThreadIDWithoutMutation(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	parent := rt.store.ensureThreadActor("T-019f7000-0000-7000-8000-000000000041")
	existingID := "T-019f7000-0000-7000-8000-000000000042"
	existing := rt.store.ensureThreadActor(existingID)
	existing.mu.Lock()
	existing.title = "Existing thread"
	existing.mu.Unlock()

	_, err := parent.executeLocalCreateThreadTool(neoPendingTool{Input: map[string]any{
		"threadId": existingID,
		"prompt":   "replace the existing thread",
	}})
	if err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("error = %v, want existing thread rejection", err)
	}
	existing.mu.Lock()
	title := existing.title
	queueLen := len(existing.queue)
	existing.mu.Unlock()
	if title != "Existing thread" || queueLen != 0 {
		t.Fatalf("existing actor mutated: title=%q queue=%d", title, queueLen)
	}
}

func TestNeoCreateThreadSerializesExplicitThreadIDCreation(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	parent := rt.store.ensureThreadActor("T-019f7000-0000-7000-8000-000000000043")
	threadID := "T-019f7000-0000-7000-8000-000000000044"
	directBody, _, _, err := parent.localCreateThreadBody(map[string]any{
		"spawnExecutor": false,
	}, threadID)
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	errs := make([]error, 2)
	var wait sync.WaitGroup
	for index := range errs {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			if index == 0 {
				_, errs[index] = parent.executeLocalCreateThreadTool(neoPendingTool{Input: map[string]any{
					"threadId":      threadID,
					"spawnExecutor": false,
				}})
				return
			}
			ctx := context.WithValue(context.Background(), neoExclusiveThreadCreationContextKey{}, true)
			response, status := rt.localThreadActorManagementResponseForOwner(ctx, cloneMap(directBody), "", parent.threadToolOwnerID())
			if status < 200 || status >= 300 {
				errs[index] = errors.New(firstNonEmptyString(response["message"], response["error"]))
			}
		}()
	}
	close(start)
	wait.Wait()
	successes := 0
	duplicateErrors := 0
	for _, err := range errs {
		if err == nil {
			successes++
		} else if strings.Contains(err.Error(), "already exists") {
			duplicateErrors++
		}
	}
	if successes != 1 || duplicateErrors != 1 {
		t.Fatalf("concurrent create errors = %#v", errs)
	}
}

func TestNeoCreateThreadReportsLocalExecutorLaunchFailure(t *testing.T) {
	useTempNeoThreadStore(t)
	directory := t.TempDir()
	t.Cleanup(replaceNeoHeadlessPIDDir(func() string { return filepath.Join(directory, "owned-pids") }))
	t.Cleanup(replaceNeoAmpHeadlessPIDDir(func() string { return filepath.Join(directory, "amp-pids") }))
	enabled := true
	missingCommand := filepath.Join(directory, "missing-amp")
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{
		Enabled:         &enabled,
		ExecutorCommand: missingCommand,
	}}})
	parent := rt.store.ensureThreadActor("T-019f7000-0000-7000-8000-000000000045")
	parent.updateEnvironment(map[string]any{"workingDirectory": directory})
	created, err := parent.executeLocalCreateThreadTool(neoPendingTool{Input: map[string]any{
		"threadId":         "T-019f7000-0000-7000-8000-000000000046",
		"executor":         "local",
		"workingDirectory": directory,
		"prompt":           "Run the local checks.",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if created["created"] != true || created["executorStarted"] != false || !strings.Contains(stringValue(created["executorError"]), "Failed to start") {
		t.Fatalf("create result = %#v", created)
	}
}

func TestNeoCreateThreadRejectsUnknownExplicitProject(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	parent := rt.store.ensureThreadActor("T-019f7000-0000-7000-8000-000000000051")
	parent.updateEnvironment(map[string]any{"workingDirectory": t.TempDir()})

	_, _, _, err := parent.localCreateThreadBody(map[string]any{
		"projectID": "019f7000-0000-7000-8000-000000000052",
	}, "T-019f7000-0000-7000-8000-000000000053")
	if err == nil || !strings.Contains(err.Error(), "projectID is not available") {
		t.Fatalf("error = %v, want unavailable project rejection", err)
	}
}

func TestNeoThreadToolsScopeRunnersToThreadOwner(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	register := func(ownerID, runnerID, directory string) {
		userActor, _ := rt.store.upsert(map[string]any{"name": "userActor", "key": ownerID}, true)
		result := mapValue(userActor.handleForSocket(&neoSocket{runnerID: runnerID}, map[string]any{
			"type": "registerRunner",
			"args": []any{map[string]any{
				"sessionId":        "session-" + runnerID,
				"hostname":         ownerID,
				"workingDirectory": directory,
				"runningThreads":   []any{},
			}},
		}))
		if result["ok"] != true {
			t.Fatalf("register %s = %#v", runnerID, result)
		}
	}
	register("user-a", "runner-a", filepath.Join(t.TempDir(), "runner-a"))
	register("user-b", "runner-b", filepath.Join(t.TempDir(), "runner-b"))
	parent := rt.store.ensureThreadActor("T-019f7000-0000-7000-8000-000000000061")
	parent.mu.Lock()
	parent.meta["ownerUserId"] = "user-a"
	parent.mu.Unlock()

	listed, err := parent.executeLocalListRunnersTool()
	if err != nil {
		t.Fatal(err)
	}
	runners := arrayValue(listed["runners"])
	if len(runners) != 1 || stringValue(mapValue(runners[0])["runnerId"]) != "runner-a" {
		t.Fatalf("owner-scoped runners = %#v", runners)
	}
	_, err = parent.executeLocalCreateThreadTool(neoPendingTool{Input: map[string]any{
		"threadId": "T-019f7000-0000-7000-8000-000000000062",
		"runnerId": "runner-b",
	}})
	if err == nil || !strings.Contains(err.Error(), "runner is not available") {
		t.Fatalf("foreign runner error = %v", err)
	}
}

func TestNeoThreadToolsRejectForeignOwnedTargets(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	sourceID := "T-019f7000-0000-7000-8000-000000000071"
	targetID := "T-019f7000-0000-7000-8000-000000000072"
	source := rt.store.ensureThreadActor(sourceID)
	target := rt.store.ensureThreadActor(targetID)
	sourceDir := t.TempDir()
	targetDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(sourceDir, "source.txt"), []byte("source"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(targetDir, "target.txt"), []byte("target"), 0o600); err != nil {
		t.Fatal(err)
	}
	source.updateEnvironment(map[string]any{"workingDirectory": sourceDir})
	target.updateEnvironment(map[string]any{"workingDirectory": targetDir})
	source.mu.Lock()
	source.meta["ownerUserId"] = "user-a"
	source.mu.Unlock()
	target.mu.Lock()
	target.meta["ownerUserId"] = "user-b"
	target.mu.Unlock()

	checks := []func() error{
		func() error {
			_, err := source.executeLocalArchiveThreadTool(map[string]any{"threadId": targetID}, true)
			return err
		},
		func() error {
			_, err := source.executeLocalArchiveThreadsTool(map[string]any{"threadIds": []any{targetID}})
			return err
		},
		func() error {
			_, err := source.executeLocalSendMessageToThreadTool(neoPendingTool{Input: map[string]any{"threadId": targetID, "message": "foreign"}})
			return err
		},
		func() error {
			_, err := source.executeLocalUploadThreadFileTool(map[string]any{"thread": targetID, "path": "source.txt"})
			return err
		},
		func() error {
			_, err := source.executeLocalDownloadThreadFileTool(map[string]any{"thread": targetID, "path": "target.txt"})
			return err
		},
	}
	for index, check := range checks {
		if err := check(); err == nil || !strings.Contains(err.Error(), "not owned") {
			t.Fatalf("check %d error = %v, want ownership rejection", index, err)
		}
	}
	target.mu.Lock()
	archived := target.archived
	queueLen := len(target.queue)
	target.mu.Unlock()
	if archived || queueLen != 0 {
		t.Fatalf("foreign target mutated: archived=%v queue=%d", archived, queueLen)
	}
}

func TestNeoArchiveThreadsValidatesBatchBeforeMutation(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	source := rt.store.ensureThreadActor("T-019f7000-0000-7000-8000-000000000081")
	owned := rt.store.ensureThreadActor("T-019f7000-0000-7000-8000-000000000082")
	foreign := rt.store.ensureThreadActor("T-019f7000-0000-7000-8000-000000000083")
	source.mu.Lock()
	source.meta["ownerUserId"] = "user-a"
	source.mu.Unlock()
	owned.mu.Lock()
	owned.meta["ownerUserId"] = "user-a"
	owned.mu.Unlock()
	foreign.mu.Lock()
	foreign.meta["ownerUserId"] = "user-b"
	foreign.mu.Unlock()

	_, err := source.executeLocalArchiveThreadsTool(map[string]any{"threadIds": []any{owned.threadID, foreign.threadID}})
	if err == nil || !strings.Contains(err.Error(), "not owned") {
		t.Fatalf("error = %v, want ownership rejection", err)
	}
	owned.mu.Lock()
	archived := owned.archived
	owned.mu.Unlock()
	if archived {
		t.Fatal("owned thread was archived before the full batch was validated")
	}
}

func TestNeoCreateThreadAcceptsListedPluginAgentMode(t *testing.T) {
	useTempNeoThreadStore(t)
	dir := t.TempDir()
	oldPluginsDir := neoAmpUserPluginsDir
	neoAmpUserPluginsDir = func() string { return dir }
	t.Cleanup(func() { neoAmpUserPluginsDir = oldPluginsDir })
	source := `// @amp-agent-mode {"key":"audit-local","label":"Audit Local"}
const agent = amp.createAgent({ name: "audit", model: "anthropic/claude-opus-4-7", instructions: "Audit carefully", reasoningEffort: "high" })
amp.registerAgentMode({ key: "audit-local", label: "Audit Local", agent })`
	if err := os.WriteFile(filepath.Join(dir, "audit.ts"), []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
	rt := newNeoRuntime(&config.Config{})
	parent := rt.store.ensureThreadActor("T-019f7000-0000-7000-8000-000000000091")
	created, err := parent.executeLocalCreateThreadTool(neoPendingTool{Input: map[string]any{
		"threadId":      "T-019f7000-0000-7000-8000-000000000092",
		"agentMode":     "audit-local",
		"spawnExecutor": false,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if created["agentMode"] != "audit-local" || created["reasoningEffort"] != "high" {
		t.Fatalf("plugin create result = %#v", created)
	}
}

func TestNeoCreateThreadOrbExecutor(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	parent := rt.store.ensureThreadActor("T-019f7000-0000-7000-8000-000000000040")
	parent.updateEnvironment(map[string]any{"workingDirectory": neoExistingDirectory(t.TempDir())})
	input := map[string]any{"executor": map[string]any{"type": "orb"}}
	if _, _, _, err := parent.localCreateThreadBody(input, "T-019f7000-0000-7000-8000-000000000041"); err == nil || !strings.Contains(err.Error(), "orb executors are not enabled") {
		t.Fatalf("disabled orbs create_thread error = %v", err)
	}

	enabled := true
	rtOrbs := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{Orbs: config.AmpOrbs{Enabled: &enabled, Provider: "docker"}}})
	orbParent := rtOrbs.store.ensureThreadActor("T-019f7000-0000-7000-8000-000000000042")
	orbParent.mu.Lock()
	orbParent.environment = map[string]any{"workingDirectory": "/Users/runner/Developer/project", "workspaceRoot": "/Users/runner/Developer/project"}
	orbParent.meta["projectID"] = "019f7000-0000-7000-8000-000000000044"
	orbParent.meta["repositoryURL"] = "https://github.com/example/project.git"
	orbParent.mu.Unlock()
	body, _, _, err := orbParent.localCreateThreadBody(input, "T-019f7000-0000-7000-8000-000000000043")
	if err != nil {
		t.Fatalf("orb create_thread body error: %v", err)
	}
	meta := mapValue(body["threadMeta"])
	if stringValue(body["executorType"]) != "sandbox" || stringValue(meta["executorType"]) != "sandbox" {
		t.Fatalf("orb executorType = %#v", body)
	}
	if stringValue(body["workingDirectory"]) != "" || stringValue(meta["repositoryURL"]) != "https://github.com/example/project.git" {
		t.Fatalf("orb inherited host directory instead of repository metadata: %#v", body)
	}
}
