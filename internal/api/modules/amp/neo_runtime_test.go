package amp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
)

func TestNeoRuntimeEnabledIsOptIn(t *testing.T) {
	if neoRuntimeEnabled(&config.Config{}) {
		t.Fatal("neo runtime should be disabled unless explicitly enabled")
	}

	enabled := true
	if !neoRuntimeEnabled(&config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled}}}) {
		t.Fatal("neo runtime should be enabled when configured")
	}
}

func TestNeoRuntimeActorLifecycleHTTP(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})

	req := httptest.NewRequest(http.MethodPut, "/actors", strings.NewReader(`{"name":"thread-actor","key":"T-test"}`))
	rec := httptest.NewRecorder()
	rt.handleHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("PUT /actors status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"created":true`) {
		t.Fatalf("expected actor creation response, got %s", rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/actors?name=thread-actor&key=T-test", nil)
	rec = httptest.NewRecorder()
	rt.handleHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /actors status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"key":"T-test"`) {
		t.Fatalf("expected actor lookup by name/key, got %s", rec.Body.String())
	}
}

func TestNeoRuntimeActorKVKeyHTTP(t *testing.T) {
	dir := t.TempDir()
	oldStoreDir := neoAmpThreadStoreDir
	neoAmpThreadStoreDir = func() string { return dir }
	t.Cleanup(func() { neoAmpThreadStoreDir = oldStoreDir })

	rt := newNeoRuntime(&config.Config{})
	actor, _ := rt.store.upsert(map[string]any{"name": "thread-actor", "key": "T-kv"}, true)
	path := "/actors/" + url.PathEscape(actor.id) + "/kv/keys/skills%2Fstate?namespace=default"

	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	rt.handleHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET actor kv status = %d, body=%s", rec.Code, rec.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode kv response: %v", err)
	}
	value, exists := body["value"]
	if !exists || value != nil {
		t.Fatalf("kv response = %#v, want explicit null value", body)
	}

	req = httptest.NewRequest(http.MethodPut, path, strings.NewReader(`{"value":{"enabled":true,"count":2}}`))
	rec = httptest.NewRecorder()
	rt.handleHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("PUT actor kv status = %d, body=%s", rec.Code, rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, path, nil)
	rec = httptest.NewRecorder()
	rt.handleHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET actor kv after set status = %d, body=%s", rec.Code, rec.Body.String())
	}
	body = map[string]any{}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode kv response after set: %v", err)
	}
	stored := mapValue(body["value"])
	if stored["enabled"] != true || numberFrom(stored["count"]) != 2 {
		t.Fatalf("stored kv response = %#v", body)
	}
	waitForNeoActorSyncIdle(t, actor)
	thread, ok := loadNeoLocalThread("T-kv")
	if !ok {
		t.Fatal("local thread snapshot missing after kv set")
	}
	persisted := mapValue(mapValue(thread["actorKV"])["skills/state"])
	if persisted["enabled"] != true || numberFrom(persisted["count"]) != 2 {
		t.Fatalf("persisted actor kv = %#v", thread["actorKV"])
	}

	req = httptest.NewRequest(http.MethodDelete, path, nil)
	rec = httptest.NewRecorder()
	rt.handleHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("DELETE actor kv status = %d, body=%s", rec.Code, rec.Body.String())
	}
	req = httptest.NewRequest(http.MethodGet, path, nil)
	rec = httptest.NewRecorder()
	rt.handleHTTP(rec, req)
	body = map[string]any{}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode kv response after delete: %v", err)
	}
	if value, exists := body["value"]; !exists || value != nil {
		t.Fatalf("kv response after delete = %#v, want explicit null value", body)
	}
	waitForNeoActorSyncIdle(t, actor)
	thread, ok = loadNeoLocalThread("T-kv")
	if !ok {
		t.Fatal("local thread snapshot missing after kv delete")
	}
	if got := mapValue(thread["actorKV"]); len(got) != 0 {
		t.Fatalf("persisted actor kv after delete = %#v, want empty", got)
	}
}

func TestNeoRuntimeStateAndMessagesHTTPAreReadOnly(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor, _ := rt.store.upsert(map[string]any{"name": "threadActor", "key": "T-read-only"}, true)

	for _, path := range []string{
		"/gateway/" + actor.id + "/request/state",
		"/gateway/" + actor.id + "/request/messages",
	} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		rt.handleHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("GET %s status = %d, body=%s", path, rec.Code, rec.Body.String())
		}

		req = httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`))
		rec = httptest.NewRecorder()
		rt.handleHTTP(rec, req)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("POST %s status = %d, want 404 not local compatibility handling", path, rec.Code)
		}
	}
}

func TestLoadNeoLocalThreadRewritesOwnershipMetadata(t *testing.T) {
	dir := t.TempDir()
	oldStoreDir := neoAmpThreadStoreDir
	neoAmpThreadStoreDir = func() string { return dir }
	t.Cleanup(func() { neoAmpThreadStoreDir = oldStoreDir })

	threadID := "T-019e5ada-9aa4-73c9-afd7-d3619a89e2ae"
	raw := []byte(`{
		"id": "` + threadID + `",
		"title": "cached cloud thread",
		"creatorUserID": "user_123",
		"data": {
			"creatorUserID": "user_123",
			"ownerUserId": "user_123"
		}
	}`)
	if err := os.WriteFile(filepath.Join(dir, threadID+".json"), raw, 0o600); err != nil {
		t.Fatalf("write local thread: %v", err)
	}

	thread, ok := loadNeoLocalThread(threadID)
	if !ok {
		t.Fatal("loadNeoLocalThread returned false")
	}
	if got := stringValue(thread["creatorUserID"]); got != neoLocalOwnerUserID {
		t.Fatalf("creatorUserID = %q, want %q", got, neoLocalOwnerUserID)
	}
	if got := stringValue(thread["ownerUserId"]); got != neoLocalOwnerUserID {
		t.Fatalf("ownerUserId = %q, want %q", got, neoLocalOwnerUserID)
	}
	data := mapValue(thread["data"])
	if got := stringValue(data["creatorUserID"]); got != neoLocalOwnerUserID {
		t.Fatalf("data.creatorUserID = %q, want %q", got, neoLocalOwnerUserID)
	}
	if got := stringValue(data["ownerUserId"]); got != neoLocalOwnerUserID {
		t.Fatalf("data.ownerUserId = %q, want %q", got, neoLocalOwnerUserID)
	}

	persistedRaw, err := os.ReadFile(filepath.Join(dir, threadID+".json"))
	if err != nil {
		t.Fatalf("read rewritten local thread: %v", err)
	}
	var persisted map[string]any
	if err := json.Unmarshal(persistedRaw, &persisted); err != nil {
		t.Fatalf("decode rewritten local thread: %v", err)
	}
	if got := stringValue(persisted["creatorUserID"]); got != neoLocalOwnerUserID {
		t.Fatalf("persisted creatorUserID = %q, want %q", got, neoLocalOwnerUserID)
	}
	if got := stringValue(persisted["ownerUserId"]); got != neoLocalOwnerUserID {
		t.Fatalf("persisted ownerUserId = %q, want %q", got, neoLocalOwnerUserID)
	}
	persistedData := mapValue(persisted["data"])
	if got := stringValue(persistedData["creatorUserID"]); got != neoLocalOwnerUserID {
		t.Fatalf("persisted data.creatorUserID = %q, want %q", got, neoLocalOwnerUserID)
	}
	if got := stringValue(persistedData["ownerUserId"]); got != neoLocalOwnerUserID {
		t.Fatalf("persisted data.ownerUserId = %q, want %q", got, neoLocalOwnerUserID)
	}
}

func TestLoadNeoLocalThreadRewritesAgentModeForBinaryLoader(t *testing.T) {
	dir := t.TempDir()
	oldStoreDir := neoAmpThreadStoreDir
	neoAmpThreadStoreDir = func() string { return dir }
	t.Cleanup(func() { neoAmpThreadStoreDir = oldStoreDir })

	threadID := "T-019e65c0-0310-77a8-b233-4b84d9c0612b"
	raw := []byte(`{
		"id": "` + threadID + `",
		"title": "missing top-level mode",
		"messages": [
			{"role": "assistant", "messageId": "M-assistant"},
			{"role": "user", "messageId": "M-user", "agentMode": "deep", "reasoningEffort": "xhigh"}
		]
	}`)
	if err := os.WriteFile(filepath.Join(dir, threadID+".json"), raw, 0o600); err != nil {
		t.Fatalf("write local thread: %v", err)
	}

	thread, ok := loadNeoLocalThread(threadID)
	if !ok {
		t.Fatal("loadNeoLocalThread returned false")
	}
	if got := stringValue(thread["agentMode"]); got != "deep" {
		t.Fatalf("agentMode = %q, want deep", got)
	}

	persistedRaw, err := os.ReadFile(filepath.Join(dir, threadID+".json"))
	if err != nil {
		t.Fatalf("read rewritten local thread: %v", err)
	}
	var persisted map[string]any
	if err := json.Unmarshal(persistedRaw, &persisted); err != nil {
		t.Fatalf("decode rewritten local thread: %v", err)
	}
	if got := stringValue(persisted["agentMode"]); got != "deep" {
		t.Fatalf("persisted agentMode = %q, want deep", got)
	}
}

func TestLoadNeoLocalThreadDoesNotGuessAgentModeForUnmarkedThread(t *testing.T) {
	dir := t.TempDir()
	oldStoreDir := neoAmpThreadStoreDir
	neoAmpThreadStoreDir = func() string { return dir }
	t.Cleanup(func() { neoAmpThreadStoreDir = oldStoreDir })

	threadID := "T-019e65c0-0310-77a8-b233-4b84d9c0612c"
	if err := os.WriteFile(filepath.Join(dir, threadID+".json"), []byte(`{"id":"`+threadID+`","title":"legacy thread","messages":[{"role":"user","messageId":"M-user"}]}`), 0o600); err != nil {
		t.Fatalf("write local thread: %v", err)
	}

	thread, ok := loadNeoLocalThread(threadID)
	if !ok {
		t.Fatal("loadNeoLocalThread returned false")
	}
	if _, exists := thread["agentMode"]; exists {
		t.Fatalf("agentMode was guessed for unmarked thread: %#v", thread["agentMode"])
	}

	persistedRaw, err := os.ReadFile(filepath.Join(dir, threadID+".json"))
	if err != nil {
		t.Fatalf("read rewritten local thread: %v", err)
	}
	var persisted map[string]any
	if err := json.Unmarshal(persistedRaw, &persisted); err != nil {
		t.Fatalf("decode rewritten local thread: %v", err)
	}
	if _, exists := persisted["agentMode"]; exists {
		t.Fatalf("persisted agentMode was guessed for unmarked thread: %#v", persisted["agentMode"])
	}
}

func TestLoadNeoLocalThreadNormalizesBinaryIterableFields(t *testing.T) {
	dir := t.TempDir()
	oldStoreDir := neoAmpThreadStoreDir
	neoAmpThreadStoreDir = func() string { return dir }
	t.Cleanup(func() { neoAmpThreadStoreDir = oldStoreDir })

	threadID := "T-019e6541-06ae-75d7-b10e-d893170fa62c"
	raw := []byte(`{
		"id": "` + threadID + `",
		"title": "legacy null fields",
		"messages": [
			{
				"role": "user",
				"messageId": "M-user",
				"content": null,
				"userState": {
					"cwd": "/tmp/work",
					"currentlyVisibleFiles": null,
					"runningTerminalCommands": null,
					"aggmanContext": {
						"availableProjects": null,
						"recentUnreadThreads": null
					}
				}
			},
			{"role": "assistant", "messageId": "M-assistant", "content": null}
		],
		"data": {
			"messages": [
				{"role": "user", "messageId": "M-data-user", "userState": {"currentlyVisibleFiles": null}}
			]
		}
	}`)
	if err := os.WriteFile(filepath.Join(dir, threadID+".json"), raw, 0o600); err != nil {
		t.Fatalf("write local thread: %v", err)
	}

	thread, ok := loadNeoLocalThread(threadID)
	if !ok {
		t.Fatal("loadNeoLocalThread returned false")
	}
	assertBinarySafeMessagesForTest(t, thread)
	assertBinarySafeMessagesForTest(t, mapValue(thread["data"]))

	persistedRaw, err := os.ReadFile(filepath.Join(dir, threadID+".json"))
	if err != nil {
		t.Fatalf("read rewritten local thread: %v", err)
	}
	var persisted map[string]any
	if err := json.Unmarshal(persistedRaw, &persisted); err != nil {
		t.Fatalf("decode rewritten local thread: %v", err)
	}
	assertBinarySafeMessagesForTest(t, persisted)
	assertBinarySafeMessagesForTest(t, mapValue(persisted["data"]))
}

func assertBinarySafeMessagesForTest(t *testing.T, thread map[string]any) {
	t.Helper()
	messages := arrayValue(thread["messages"])
	if messages == nil {
		t.Fatalf("messages = %#v, want binary-safe array", thread["messages"])
	}
	for _, raw := range messages {
		message := mapValue(raw)
		if len(message) == 0 {
			continue
		}
		if content := arrayValue(message["content"]); content == nil {
			t.Fatalf("message content = %#v, want binary-safe array for %#v", message["content"], message)
		}
		if stringValue(message["role"]) != "user" {
			continue
		}
		userState := mapValue(message["userState"])
		if userState == nil {
			t.Fatalf("userState = %#v, want binary-safe object", message["userState"])
		}
		if files := arrayValue(userState["currentlyVisibleFiles"]); files == nil {
			t.Fatalf("currentlyVisibleFiles = %#v, want binary-safe array", userState["currentlyVisibleFiles"])
		}
		if _, exists := userState["runningTerminalCommands"]; exists {
			t.Fatalf("runningTerminalCommands = %#v, want omitted when not an array", userState["runningTerminalCommands"])
		}
		if aggman := mapValue(userState["aggmanContext"]); len(aggman) > 0 {
			if _, exists := aggman["availableProjects"]; exists {
				t.Fatalf("aggmanContext.availableProjects = %#v, want omitted when not an array", aggman["availableProjects"])
			}
			if _, exists := aggman["recentUnreadThreads"]; exists {
				t.Fatalf("aggmanContext.recentUnreadThreads = %#v, want omitted when not an array", aggman["recentUnreadThreads"])
			}
		}
	}
}

func TestNeoRuntimeAutoCompactsLargeLocalHistory(t *testing.T) {
	dir := t.TempDir()
	oldStoreDir := neoAmpThreadStoreDir
	neoAmpThreadStoreDir = func() string { return dir }
	t.Cleanup(func() { neoAmpThreadStoreDir = oldStoreDir })

	calls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/provider/openai/v1/chat/completions" {
			t.Fatalf("unexpected compaction path %s", r.URL.Path)
		}
		calls++
		payload := readNeoJSON(r.Body)
		wantKeys := map[string]bool{
			"max_completion_tokens": true,
			"messages":              true,
			"model":                 true,
			"reasoning_effort":      true,
			"stream":                true,
		}
		if len(payload) != len(wantKeys) {
			t.Fatalf("compaction payload keys = %v, want only [max_completion_tokens messages model reasoning_effort stream]", sortedMapKeys(payload))
		}
		for key := range wantKeys {
			if _, ok := payload[key]; !ok {
				t.Fatalf("compaction payload missing key %q: %v", key, sortedMapKeys(payload))
			}
		}
		if payload["model"] != "gpt-5.4" {
			t.Fatalf("compaction model = %#v, want gpt-5.4", payload["model"])
		}
		if payload["stream"] != false {
			t.Fatalf("compaction stream = %#v, want false", payload["stream"])
		}
		if numberFrom(payload["max_completion_tokens"]) != 2048 {
			t.Fatalf("compaction max_completion_tokens = %#v, want 2048", payload["max_completion_tokens"])
		}
		if payload["reasoning_effort"] != "xhigh" {
			t.Fatalf("compaction reasoning_effort = %#v, want xhigh", payload["reasoning_effort"])
		}
		if _, ok := payload["tools"]; ok {
			t.Fatalf("compaction payload should omit tools: %#v", payload["tools"])
		}
		if _, ok := payload["tool_choice"]; ok {
			t.Fatalf("compaction payload should omit tool_choice: %#v", payload["tool_choice"])
		}
		if _, ok := payload["temperature"]; ok {
			t.Fatalf("compaction payload should omit temperature: %#v", payload["temperature"])
		}
		messages := arrayValue(payload["messages"])
		renderedMessages := fmt.Sprint(messages)
		if len(messages) < 31 || !strings.Contains(renderedMessages, "continuation summary") || !strings.Contains(renderedMessages, "message 00") || !strings.Contains(renderedMessages, "message 29") {
			t.Fatalf("compaction prompt missing provider-shaped history: %#v", payload)
		}
		firstMessage := mapValue(messages[0])
		if firstMessage["role"] == "system" {
			t.Fatalf("first compaction message = %#v, want history without compaction-only system prompt", firstMessage)
		}
		for _, raw := range messages[:len(messages)-1] {
			if mapValue(raw)["role"] == "system" {
				t.Fatalf("compaction history should not include a compaction-only system prompt: %#v", messages)
			}
		}
		lastMessage := mapValue(messages[len(messages)-1])
		if lastMessage["role"] != "user" || lastMessage["content"] != neoCompactionPrompt() {
			t.Fatalf("last compaction message = %#v, want Amp continuation summary prompt", lastMessage)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"- preserved project goal\n- tests passed"}}]}`))
	}))
	t.Cleanup(upstream.Close)
	parsed, err := url.Parse(upstream.URL)
	if err != nil {
		t.Fatalf("parse upstream url: %v", err)
	}
	_, portString, err := net.SplitHostPort(parsed.Host)
	if err != nil {
		t.Fatalf("parse upstream host: %v", err)
	}
	port, err := strconv.Atoi(portString)
	if err != nil {
		t.Fatalf("parse upstream port: %v", err)
	}

	enabled := true
	rt := newNeoRuntime(&config.Config{
		Host: "127.0.0.1",
		Port: port,
		AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{
			Enabled:         &enabled,
			CompactionModel: "openai/gpt-5.4",
		}},
	})
	threadID := "T-auto-compact"
	actor := newNeoActor(rt, "actor-test", "threadActor", threadID, threadID, neoActorRecord("actor-test", "threadActor", threadID), nil)
	actor.settings["internal.model"] = "openai/local-small"
	longText := strings.Repeat("important context ", 300)
	actor.mu.Lock()
	for i := 0; i < 30; i++ {
		role := "user"
		if i%2 == 1 {
			role = "assistant"
		}
		actor.storeMessageLocked(neoMessage{ThreadID: threadID, MessageID: fmt.Sprintf("M-%022d", i), Role: role, Content: []any{map[string]any{"type": "text", "text": fmt.Sprintf("message %02d %s", i, longText)}}})
	}
	actor.rebuildHistoryLocked()
	actor.mu.Unlock()

	actor.mu.Lock()
	actor.syncRunning = true
	actor.mu.Unlock()
	actor.maybeCompactBeforeInference("smart", "medium", "", actor.generation)
	rawStored, err := os.ReadFile(filepath.Join(dir, threadID+".json"))
	if err != nil {
		t.Fatalf("compaction should synchronously persist local thread before async sync: %v", err)
	}
	var storedThread map[string]any
	if err := json.Unmarshal(rawStored, &storedThread); err != nil {
		t.Fatalf("stored compacted thread JSON: %v", err)
	}
	storedMessages := arrayValue(storedThread["messages"])
	summaryIndex := 30 - neoCompactionTailMessages
	if len(storedMessages) <= summaryIndex+1 {
		t.Fatalf("stored messages after compaction = %d, want at least %d", len(storedMessages), summaryIndex+2)
	}
	if got := stringValue(mapValue(storedMessages[summaryIndex])["role"]); got != "info" {
		t.Fatalf("stored summary role at cut boundary = %q, want info", got)
	}
	if got := stringValue(mapValue(storedMessages[summaryIndex+1])["protocolMessageID"]); got != "M-0000000000000000000022" {
		t.Fatalf("stored cut message after summary = %q, want M-0000000000000000000022", got)
	}
	actor.mu.Lock()
	actor.syncRunning = false
	actor.syncPending = false
	actor.mu.Unlock()
	waitForNeoActorSyncIdle(t, actor)

	actor.mu.Lock()
	if calls != 1 {
		actor.mu.Unlock()
		t.Fatalf("compaction calls = %d, want 1", calls)
	}
	if len(actor.messages) != 31 {
		actor.mu.Unlock()
		t.Fatalf("message count after compaction = %d, want preserved 30 messages plus summary", len(actor.messages))
	}
	if actor.messages[0].MessageID != "M-0000000000000000000000" {
		actor.mu.Unlock()
		t.Fatalf("old transcript prefix was removed: first message = %#v", actor.messages[0])
	}
	summaryMessage := actor.messages[summaryIndex]
	if summaryMessage.Role != "info" || stringValue(mapValue(summaryMessage.Content[0])["type"]) != "summary" {
		actor.mu.Unlock()
		t.Fatalf("summary message = %#v, want summary info at cut boundary", summaryMessage)
	}
	retained := actor.messages[summaryIndex+1]
	if retained.Role != "user" || retained.MessageID != "M-0000000000000000000022" {
		actor.mu.Unlock()
		t.Fatalf("retained cut message = %#v, want original cut message after summary", retained)
	}
	if !strings.Contains(stringValue(mapValue(mapValue(summaryMessage.Content[0])["summary"])["summary"]), "preserved project goal") {
		actor.mu.Unlock()
		t.Fatalf("summary message content = %#v", summaryMessage.Content)
	}
	if len(actor.compactionRecords) != 1 || stringValue(actor.compactionRecords[0]["cutMessageId"]) != "M-0000000000000000000022" {
		actor.mu.Unlock()
		t.Fatalf("compaction records = %#v", actor.compactionRecords)
	}
	if len(actor.history) == 0 || actor.history[0].Role != "assistant" || !strings.Contains(actor.history[0].Text, "preserved project goal") {
		actor.mu.Unlock()
		t.Fatalf("history after compaction = %#v", actor.history)
	}
	if !strings.Contains(fmt.Sprint(actor.history), "message 29") {
		actor.mu.Unlock()
		t.Fatalf("history after compaction lost retained tail messages: %#v", actor.history)
	}
	if strings.Contains(fmt.Sprint(actor.history), "message 00") {
		actor.mu.Unlock()
		t.Fatalf("history after compaction leaked pre-summary messages: %#v", actor.history)
	}
	actor.mu.Unlock()

	actor.maybeCompactBeforeInference("smart", "medium", "", actor.generation)
	waitForNeoActorSyncIdle(t, actor)

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if calls != 1 {
		t.Fatalf("compaction calls after unchanged compacted window = %d, want 1", calls)
	}
}

func TestNeoCompactionThresholdPercentSettingMatchesBinary(t *testing.T) {
	shortHistory := []neoMessage{
		{ThreadID: "T-threshold", MessageID: "M-0000000000000000000000", Role: "user", Content: []any{map[string]any{"type": "text", "text": "short"}}},
	}
	if neoCompactionShouldRun(shortHistory, 1000, 0) {
		t.Fatal("threshold percent 0 should still honor the minimum message count gate")
	}

	longHistory := make([]neoMessage, 0, neoCompactionMinMessages)
	longText := strings.Repeat("threshold parity ", 100)
	for i := 0; i < neoCompactionMinMessages; i++ {
		role := "user"
		if i%2 == 1 {
			role = "assistant"
		}
		longHistory = append(longHistory, neoMessage{ThreadID: "T-threshold", MessageID: fmt.Sprintf("M-%022d", i), Role: role, Content: []any{map[string]any{"type": "text", "text": longText}}})
	}

	if !neoCompactionShouldRun(longHistory, 1000, 0) {
		t.Fatal("threshold percent 0 should compact once the minimum message count gate is satisfied")
	}
	if neoCompactionShouldRun(longHistory, 1000000, 100) {
		t.Fatal("threshold percent 100 should defer compaction until history reaches the full input budget")
	}

	if got := neoCompactionThresholdPercent(map[string]any{}); got != 65 {
		t.Fatalf("default threshold percent = %v, want 65", got)
	}
	if got := neoCompactionThresholdPercent(map[string]any{"internal.compactionThresholdPercent": 0}); got != 0 {
		t.Fatalf("explicit zero threshold percent = %v, want 0", got)
	}
	if got := neoCompactionThresholdPercent(map[string]any{"internal.compactionThresholdPercent": 120}); got != 100 {
		t.Fatalf("clamped threshold percent = %v, want 100", got)
	}
	if got := neoCompactionThresholdPercent(map[string]any{"internal.compactionThresholdPercent": json.Number("75.5")}); got != 75.5 {
		t.Fatalf("decimal threshold percent = %v, want 75.5", got)
	}
}

func TestNeoCompactionPromptMatchesBinaryContinuationStyle(t *testing.T) {
	prompt := neoCompactionPrompt()

	for _, want := range []string{
		"continuation summary",
		"1. Task Overview",
		"2. Current State",
		"3. Important Discoveries",
		"4. Next Steps",
		"5. Context to Preserve",
		"Wrap your summary in <summary></summary> tags.",
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("compaction prompt missing %q:\n%s", want, prompt)
		}
	}

	history := neoCompactionHistory([]neoMessage{{
		ThreadID:  "T-compaction-prompt",
		MessageID: "M-0000000000000000000001",
		Role:      "user",
		Content:   []any{map[string]any{"type": "text", "text": "finish exact parity with Amp binary"}},
	}})
	if len(history) != 1 || history[0].Role != "user" || !strings.Contains(history[0].Text, "finish exact parity with Amp binary") {
		t.Fatalf("compaction history = %#v", history)
	}

	wrapped := "before\n<summary>\n- Task Overview\n- Current State\n</summary>\nafter"
	if got := neoNormalizeCompactionSummary(wrapped); got != "- Task Overview\n- Current State" {
		t.Fatalf("normalized wrapped summary = %q", got)
	}
	upper := "<SUMMARY>kept content</SUMMARY>"
	if got := neoNormalizeCompactionSummary(upper); got != "kept content" {
		t.Fatalf("normalized upper summary = %q", got)
	}
}

func TestNeoCompactionHistoryHonorsLatestSummaryBoundary(t *testing.T) {
	messages := []neoMessage{
		{ThreadID: "T-compaction-history", MessageID: "M-old-tail", Role: "user", Content: []any{map[string]any{"type": "text", "text": "old retained transcript tail"}}},
		neoCompactionSummaryMessage("T-compaction-history", "prior summary text"),
		{ThreadID: "T-compaction-history", MessageID: "M-new", Role: "user", Content: []any{map[string]any{"type": "text", "text": "new request after summary"}}},
	}

	history := neoCompactionHistory(messages)
	if len(history) != 2 {
		t.Fatalf("history len = %d, want summary plus messages after it: %#v", len(history), history)
	}
	if history[0].Role != "assistant" || history[0].Text != "prior summary text" {
		t.Fatalf("history[0] = %#v, want assistant summary", history[0])
	}
	if history[1].Role != "user" || !strings.Contains(history[1].Text, "new request after summary") {
		t.Fatalf("history[1] = %#v, want message after summary", history[1])
	}
	if strings.Contains(fmt.Sprint(history), "old retained transcript tail") {
		t.Fatalf("compaction history leaked transcript before latest summary: %#v", history)
	}
}

func TestNeoCompactionHistorySupportsThreadSummaryBlocks(t *testing.T) {
	messages := []neoMessage{
		{ThreadID: "T-compaction-thread", MessageID: "M-old", Role: "user", Content: []any{map[string]any{"type": "text", "text": "old context"}}},
		{ThreadID: "T-compaction-thread", MessageID: "M-summary", Role: "info", Content: []any{map[string]any{
			"type": "summary",
			"summary": map[string]any{
				"type":   "thread",
				"thread": "T-summary-source",
			},
		}}},
		{ThreadID: "T-compaction-thread", MessageID: "M-new", Role: "user", Content: []any{map[string]any{"type": "text", "text": "new context"}}},
	}

	history := neoCompactionHistory(messages)
	if len(history) != 2 {
		t.Fatalf("compaction history length = %d, want 2: %#v", len(history), history)
	}
	if history[0].Role != "assistant" || history[0].Text != "Summary thread: T-summary-source" {
		t.Fatalf("thread summary history prefix = %#v", history[0])
	}
	if strings.Contains(fmt.Sprint(history), "old context") {
		t.Fatalf("thread summary compaction leaked transcript before summary: %#v", history)
	}
}

func TestNeoRuntimeEditRerunAnnouncesInferenceBeforeCompaction(t *testing.T) {
	dir := t.TempDir()
	oldStoreDir := neoAmpThreadStoreDir
	neoAmpThreadStoreDir = func() string { return dir }
	t.Cleanup(func() { neoAmpThreadStoreDir = oldStoreDir })

	compactionStarted := make(chan struct{})
	allowCompaction := make(chan struct{})
	var releaseCompaction sync.Once
	t.Cleanup(func() { releaseCompaction.Do(func() { close(allowCompaction) }) })

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload := readNeoJSON(r.Body)
		if boolValue(payload["stream"]) {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"rerun complete\"}}]}\n\n"))
			_, _ = w.Write([]byte("data: [DONE]\n\n"))
			return
		}
		select {
		case <-compactionStarted:
		default:
			close(compactionStarted)
		}
		<-allowCompaction
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"compact summary"}}]}`))
	}))
	t.Cleanup(upstream.Close)
	parsed, err := url.Parse(upstream.URL)
	if err != nil {
		t.Fatalf("parse upstream url: %v", err)
	}
	_, portString, err := net.SplitHostPort(parsed.Host)
	if err != nil {
		t.Fatalf("parse upstream host: %v", err)
	}
	port, err := strconv.Atoi(portString)
	if err != nil {
		t.Fatalf("parse upstream port: %v", err)
	}

	enabled := true
	rt := newNeoRuntime(&config.Config{
		Host: "127.0.0.1",
		Port: port,
		AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{
			Enabled:         &enabled,
			CompactionModel: "openai/gpt-5.4",
		}},
	})
	threadID := "T-edit-rerun-compaction"
	actor, _ := rt.store.upsert(map[string]any{"name": "threadActor", "key": threadID, "input": map[string]any{"threadId": threadID}}, true)
	actor.mu.Lock()
	actor.executorReady = true
	actor.settings["internal.model"] = "openai/local-small"
	longText := strings.Repeat("important context ", 300)
	for i := 0; i < 30; i++ {
		role := "user"
		if i%2 == 1 {
			role = "assistant"
		}
		actor.storeMessageLocked(neoMessage{ThreadID: threadID, MessageID: fmt.Sprintf("M-%022d", i), Role: role, Content: []any{map[string]any{"type": "text", "text": fmt.Sprintf("message %02d %s", i, longText)}}})
	}
	actor.rebuildHistoryLocked()
	actor.mu.Unlock()

	server := httptest.NewServer(http.HandlerFunc(rt.handleHTTP))
	t.Cleanup(server.Close)
	conn := dialNeoActorWebSocket(t, server.URL, threadID)
	defer conn.Close()
	waitForNeoMessageType(t, conn, "agent_state", 2*time.Second)

	if err := conn.WriteJSON(map[string]any{
		"type":      "client_edit_message",
		"messageId": "M-0000000000000000000028",
		"editId":    "edit-rerun",
		"content":   []any{map[string]any{"type": "text", "text": "edited rerun prompt"}},
	}); err != nil {
		t.Fatalf("write client_edit_message: %v", err)
	}

	select {
	case <-compactionStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("compaction did not start")
	}

	seen := map[string]bool{}
	assistantID := ""
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !(seen["message_updated"] && seen["thread_truncated"] && seen["agent_working"] && seen["inference_tools"] && seen["delta_start"]) {
		msg, ok := readNeoMessage(t, conn, time.Until(deadline))
		if !ok {
			break
		}
		switch msg["type"] {
		case "message_updated":
			seen["message_updated"] = true
			content := arrayValue(mapValue(msg["message"])["content"])
			if len(content) != 1 || stringValue(mapValue(content[0])["text"]) != "edited rerun prompt" {
				t.Fatalf("message_updated content = %#v", msg)
			}
		case "thread_truncated":
			seen["thread_truncated"] = true
			if got := stringValue(msg["truncateFromMessage"]); got != "M-0000000000000000000029" {
				t.Fatalf("truncateFromMessage = %q, want edited tail assistant: %#v", got, msg)
			}
		case "agent_state":
			if stringValue(msg["state"]) == "working" && stringValue(msg["messageId"]) != "" {
				seen["agent_working"] = true
				assistantID = stringValue(msg["messageId"])
			}
		case "inference_tools":
			seen["inference_tools"] = true
			if assistantID != "" && stringValue(msg["messageId"]) != assistantID {
				t.Fatalf("inference_tools messageId = %#v, want %s", msg, assistantID)
			}
		case "delta":
			if stringValue(msg["state"]) == "start" {
				seen["delta_start"] = true
				if assistantID != "" && stringValue(msg["messageId"]) != assistantID {
					t.Fatalf("delta start messageId = %#v, want %s", msg, assistantID)
				}
			}
		}
	}
	for _, key := range []string{"message_updated", "thread_truncated", "agent_working", "inference_tools", "delta_start"} {
		if !seen[key] {
			t.Fatalf("missing %s before compaction was released; seen=%#v", key, seen)
		}
	}

	releaseCompaction.Do(func() { close(allowCompaction) })
}

func TestNeoActorStorePrunesIdleActors(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	stale, _ := rt.store.upsert(map[string]any{"name": "thread-actor", "key": "T-prune-stale", "input": map[string]any{"threadId": "T-prune-stale"}}, true)
	protected, _ := rt.store.upsert(map[string]any{"name": "thread-actor", "key": "T-prune-active", "input": map[string]any{"threadId": "T-prune-active"}}, true)

	now := time.Now()
	stale.mu.Lock()
	stale.lastUsed = now.Add(-time.Hour)
	stale.mu.Unlock()
	protected.mu.Lock()
	protected.lastUsed = now.Add(-time.Hour)
	protected.approvalQueue = []map[string]any{{"toolCallId": "TU-ask"}}
	protected.agentState = "awaiting_approval"
	protected.mu.Unlock()

	if pruned := rt.store.pruneIdle(now, 30*time.Minute); pruned != 1 {
		t.Fatalf("pruned = %d, want 1", pruned)
	}
	if got := rt.store.findActors(url.Values{"name": []string{"thread-actor"}, "key": []string{"T-prune-stale"}}); len(got) != 0 {
		t.Fatalf("stale actor still indexed: %#v", got)
	}
	if got := rt.store.findActors(url.Values{"name": []string{"thread-actor"}, "key": []string{"T-prune-active"}}); len(got) != 1 {
		t.Fatalf("protected actor missing: %#v", got)
	}
}

func TestNeoActorSyncCloudAsyncCoalescesWhileRunning(t *testing.T) {
	dir := t.TempDir()
	oldStoreDir := neoAmpThreadStoreDir
	neoAmpThreadStoreDir = func() string { return dir }
	t.Cleanup(func() { neoAmpThreadStoreDir = oldStoreDir })

	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-sync-coalesce", "T-sync-coalesce", neoActorRecord("actor-test", "thread-actor", "T-sync-coalesce"), nil)
	actor.mu.Lock()
	actor.syncRunning = true
	actor.mu.Unlock()

	for i := 0; i < 20; i++ {
		actor.syncCloudAsync()
	}
	actor.mu.Lock()
	if !actor.syncRunning || !actor.syncPending {
		t.Fatalf("sync state = running:%v pending:%v, want running and pending", actor.syncRunning, actor.syncPending)
	}
	actor.syncRunning = false
	actor.syncPending = false
	actor.mu.Unlock()

	actor.syncCloudAsync()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		actor.mu.Lock()
		running := actor.syncRunning
		actor.mu.Unlock()
		if !running {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	actor.mu.Lock()
	running := actor.syncRunning
	pending := actor.syncPending
	actor.mu.Unlock()
	if running || pending {
		t.Fatalf("sync state after worker = running:%v pending:%v, want idle", running, pending)
	}
	if _, ok := loadNeoLocalThread("T-sync-coalesce"); !ok {
		t.Fatalf("local thread snapshot was not written")
	}
}

func TestNeoRuntimeGatewayWebSocketGetOrCreate(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	server := httptest.NewServer(http.HandlerFunc(rt.handleHTTP))
	t.Cleanup(server.Close)

	dialer := websocket.Dialer{Subprotocols: []string{"rivet", "rivet_encoding.4", "rivet_skip_ready_wait"}}
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/gateway/threadActor/?rvt-method=getOrCreate&rvt-key=T-gateway"
	conn, resp, err := dialer.Dial(wsURL, nil)
	if err != nil {
		status := 0
		if resp != nil {
			status = resp.StatusCode
		}
		t.Fatalf("gateway websocket dial failed status=%d err=%v", status, err)
	}
	defer conn.Close()

	if got := conn.Subprotocol(); got != "rivet" {
		t.Fatalf("subprotocol = %q, want rivet", got)
	}
	actors := rt.store.findActors(url.Values{"name": []string{"threadActor"}, "key": []string{"T-gateway"}})
	if len(actors) != 1 {
		t.Fatalf("gateway actor count = %d, actors=%#v", len(actors), actors)
	}
}

func TestNeoRuntimeGatewayWebSocketGetRehydratesPersistedLocalThread(t *testing.T) {
	useTempNeoThreadStore(t)
	threadID := "T-gateway-get-rehydrate"
	if err := os.WriteFile(filepath.Join(neoAmpThreadStoreDir(), threadID+".json"), []byte(`{
		"id":"`+threadID+`",
		"agentMode":"deep",
		"meta":{"cliProxyAPILocalNeo":true},
		"messages":[{"role":"user","messageId":"M-0000000000000000000001","agentMode":"deep","content":[{"type":"text","text":"resume after restart"}]}]
	}`), 0o600); err != nil {
		t.Fatalf("write local thread: %v", err)
	}

	rt := newNeoRuntime(&config.Config{})
	server := httptest.NewServer(http.HandlerFunc(rt.handleHTTP))
	t.Cleanup(server.Close)

	dialer := websocket.Dialer{Subprotocols: []string{"rivet", "rivet_encoding.4", "rivet_skip_ready_wait"}}
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/gateway/threadActor/?rvt-method=get&rvt-key=" + url.QueryEscape(threadID)
	conn, resp, err := dialer.Dial(wsURL, nil)
	if err != nil {
		status := 0
		if resp != nil {
			status = resp.StatusCode
		}
		t.Fatalf("gateway websocket get dial failed status=%d err=%v", status, err)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))

	for {
		_, payload, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read rehydrated snapshot: %v", err)
		}
		var msg map[string]any
		if err := json.Unmarshal(payload, &msg); err != nil {
			t.Fatalf("snapshot JSON error: %v", err)
		}
		if msg["type"] != "message_added" {
			continue
		}
		message := mapValue(msg["message"])
		if stringValue(message["messageId"]) != "M-0000000000000000000001" || !strings.Contains(fmt.Sprint(message["content"]), "resume after restart") {
			t.Fatalf("rehydrated message = %#v", message)
		}
		break
	}

	actors := rt.store.findActors(url.Values{"name": []string{"threadActor"}, "key": []string{threadID}})
	if len(actors) != 1 {
		t.Fatalf("gateway actor count = %d, actors=%#v", len(actors), actors)
	}
}

func TestNeoRuntimeGatewayWebSocketGetRehydratesEmptyLocalNeoThread(t *testing.T) {
	useTempNeoThreadStore(t)
	threadID := "T-gateway-get-rehydrate-empty"
	if err := os.WriteFile(filepath.Join(neoAmpThreadStoreDir(), threadID+".json"), []byte(`{
		"id":"`+threadID+`",
		"agentMode":"rush",
		"meta":{"cliProxyAPILocalNeo":true},
		"messages":[]
	}`), 0o600); err != nil {
		t.Fatalf("write local thread: %v", err)
	}

	rt := newNeoRuntime(&config.Config{})
	server := httptest.NewServer(http.HandlerFunc(rt.handleHTTP))
	t.Cleanup(server.Close)

	dialer := websocket.Dialer{Subprotocols: []string{"rivet", "rivet_encoding.4", "rivet_skip_ready_wait"}}
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/gateway/threadActor/?rvt-method=get&rvt-key=" + url.QueryEscape(threadID)
	conn, resp, err := dialer.Dial(wsURL, nil)
	if err != nil {
		status := 0
		if resp != nil {
			status = resp.StatusCode
		}
		t.Fatalf("gateway websocket get dial failed status=%d err=%v", status, err)
	}
	defer conn.Close()

	actors := rt.store.findActors(url.Values{"name": []string{"threadActor"}, "key": []string{threadID}})
	if len(actors) != 1 {
		t.Fatalf("gateway actor count = %d, actors=%#v", len(actors), actors)
	}
	actor := rt.store.get(firstNonEmptyString(actors[0]["actor_id"], actors[0]["id"]))
	if actor == nil {
		t.Fatal("gateway actor not stored")
	}
	actor.mu.Lock()
	mode := actor.currentAgentMode
	actor.mu.Unlock()
	if mode != "rush" {
		t.Fatalf("rehydrated mode = %q, want rush", mode)
	}
}

func TestNeoRuntimeGatewayGetOrCreateRehydratesPersistedLocalThreadBeforeOpen(t *testing.T) {
	useTempNeoThreadStore(t)
	threadID := "T-gateway-get-or-create-rehydrate"
	if err := os.WriteFile(filepath.Join(neoAmpThreadStoreDir(), threadID+".json"), []byte(`{
		"id":"`+threadID+`",
		"agentMode":"deep",
		"meta":{"cliProxyAPILocalNeo":true},
		"messages":[{"role":"user","messageId":"M-0000000000000000000001","agentMode":"deep","content":[{"type":"text","text":"resume getOrCreate after restart"}]}]
	}`), 0o600); err != nil {
		t.Fatalf("write local thread: %v", err)
	}

	rt := newNeoRuntime(&config.Config{})
	req := httptest.NewRequest(http.MethodGet, "/gateway/threadActor/?rvt-method=getOrCreate&rvt-key="+url.QueryEscape(threadID), nil)
	actor := rt.store.actorForGatewayRequest(req)
	if actor == nil {
		t.Fatal("gateway getOrCreate did not rehydrate persisted local thread actor")
	}
	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.messages) != 1 {
		t.Fatalf("rehydrated message count = %d, want 1", len(actor.messages))
	}
	if actor.currentAgentMode != "deep" {
		t.Fatalf("agent mode = %q, want deep", actor.currentAgentMode)
	}
	if !strings.Contains(fmt.Sprint(actor.messages[0].Content), "resume getOrCreate after restart") {
		t.Fatalf("rehydrated message = %#v", actor.messages[0])
	}
}

func TestNeoRuntimeGatewayWebSocketGetDoesNotCreateMissingThreadActor(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	server := httptest.NewServer(http.HandlerFunc(rt.handleHTTP))
	t.Cleanup(server.Close)

	dialer := websocket.Dialer{Subprotocols: []string{"rivet", "rivet_encoding.4", "rivet_skip_ready_wait"}}
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/gateway/threadActor/?rvt-method=get&rvt-key=T-missing-gateway-get"
	conn, resp, err := dialer.Dial(wsURL, nil)
	if conn != nil {
		_ = conn.Close()
	}
	if err == nil {
		t.Fatal("gateway websocket get unexpectedly created a missing thread actor")
	}
	if resp == nil || resp.StatusCode != http.StatusNotFound {
		status := 0
		if resp != nil {
			status = resp.StatusCode
		}
		t.Fatalf("gateway websocket get missing status = %d err=%v, want 404", status, err)
	}
}

func TestNeoRuntimeGatewayWebSocketGetAllowsExecutorReconnectForPersistedThread(t *testing.T) {
	useTempNeoThreadStore(t)
	threadID := "T-gateway-get-executor"
	if err := os.WriteFile(filepath.Join(neoAmpThreadStoreDir(), threadID+".json"), []byte(`{
		"id":"`+threadID+`",
		"agentMode":"smart",
		"meta":{"cliProxyAPILocalNeo":true},
		"messages":[{"role":"user","messageId":"M-0000000000000000000001","agentMode":"smart","content":[{"type":"text","text":"executor reconnect"}]}]
	}`), 0o600); err != nil {
		t.Fatalf("write local thread: %v", err)
	}

	rt := newNeoRuntime(&config.Config{})
	server := httptest.NewServer(http.HandlerFunc(rt.handleHTTP))
	t.Cleanup(server.Close)

	dialer := websocket.Dialer{Subprotocols: []string{"rivet", "rivet_conn_params.%7B%22transport%22%3A%22json-rpc%22%7D", "rivet_encoding.4", "rivet_skip_ready_wait"}}
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/gateway/threadActor/?rvt-method=get&rvt-key=" + url.QueryEscape(threadID)
	conn, resp, err := dialer.Dial(wsURL, nil)
	if err != nil {
		status := 0
		if resp != nil {
			status = resp.StatusCode
		}
		t.Fatalf("gateway websocket get executor dial failed status=%d err=%v", status, err)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))

	if err := conn.WriteJSON(map[string]any{
		"jsonrpc": "2.0",
		"id":      "req-1",
		"method":  "executor_connect",
		"params": map[string]any{
			"clientId":     "executor-after-restart",
			"executorType": "local-client",
		},
	}); err != nil {
		t.Fatalf("write executor_connect: %v", err)
	}

	for {
		var frame map[string]any
		if err := conn.ReadJSON(&frame); err != nil {
			t.Fatalf("read executor reconnect frame: %v", err)
		}
		if frame["method"] != "executor_connected" {
			continue
		}
		params := mapValue(frame["params"])
		if params["executorId"] != "executor-after-restart" {
			t.Fatalf("executor_connected params = %#v", params)
		}
		return
	}
}

func TestNeoRuntimeGatewayWebSocketJSONRPCTransport(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	server := httptest.NewServer(http.HandlerFunc(rt.handleHTTP))
	t.Cleanup(server.Close)

	dialer := websocket.Dialer{Subprotocols: []string{"rivet", "rivet_conn_params.%7B%22transport%22%3A%22json-rpc%22%7D", "rivet_encoding.4", "rivet_skip_ready_wait"}}
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/gateway/threadActor/?rvt-method=getOrCreate&rvt-key=T-jsonrpc"
	conn, resp, err := dialer.Dial(wsURL, nil)
	if err != nil {
		status := 0
		if resp != nil {
			status = resp.StatusCode
		}
		t.Fatalf("gateway websocket dial failed status=%d err=%v", status, err)
	}
	defer conn.Close()

	if err := conn.WriteJSON(map[string]any{
		"jsonrpc": "2.0",
		"id":      "req-1",
		"method":  "executor_connect",
		"params": map[string]any{
			"clientId":     "executor-jsonrpc",
			"executorType": "local-client",
			"handshakeSeq": 1,
			"capabilities": map[string]any{"workspaceId": "/tmp/workspace"},
		},
	}); err != nil {
		t.Fatalf("write jsonrpc executor_connect: %v", err)
	}

	var sawConnected bool
	var sawResponse bool
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && (!sawConnected || !sawResponse) {
		_ = conn.SetReadDeadline(time.Now().Add(time.Until(deadline)))
		_, payload, err := conn.ReadMessage()
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				break
			}
			t.Fatalf("read jsonrpc websocket message: %v", err)
		}
		var frame map[string]any
		if err := json.Unmarshal(payload, &frame); err != nil {
			t.Fatalf("jsonrpc websocket JSON error: %v", err)
		}
		if _, hasRawType := frame["type"]; hasRawType {
			t.Fatalf("jsonrpc websocket received raw protocol frame: %#v", frame)
		}
		if frame["id"] == "req-1" {
			sawResponse = true
			continue
		}
		if frame["method"] == "executor_connected" {
			params := mapValue(frame["params"])
			if params["executorId"] != "executor-jsonrpc" {
				t.Fatalf("executor_connected params = %#v", params)
			}
			sawConnected = true
		}
	}
	if !sawConnected || !sawResponse {
		t.Fatalf("jsonrpc websocket sawConnected=%v sawResponse=%v", sawConnected, sawResponse)
	}
}

func TestNeoRuntimeWebSocketFiltersLocalExtensionEventsForAmpClients(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	server := httptest.NewServer(http.HandlerFunc(rt.handleHTTP))
	t.Cleanup(server.Close)

	threadID := "T-local-extension-filter"
	seedNeoLocalExtensionStateForTest(t, rt, threadID)

	conn := dialNeoActorWebSocketWithoutResume(t, server.URL, threadID, "")
	defer conn.Close()
	if err := conn.WriteJSON(map[string]any{"type": "client_resume", "version": 0}); err != nil {
		t.Fatalf("write client_resume: %v", err)
	}

	forbidden := map[string]bool{
		"artifact_deleted":       true,
		"artifact_upserted":      true,
		"artifacts_snapshot":     true,
		"clearPendingNavigation": true,
		"draft":                  true,
		"main-thread":            true,
		"max-tokens":             true,
		"setPendingNavigation":   true,
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		msg, ok := readNeoMessage(t, conn, time.Until(deadline))
		if !ok {
			break
		}
		msgType := stringValue(msg["type"])
		if forbidden[msgType] {
			t.Fatalf("Amp client received local extension event %q: %#v", msgType, msg)
		}
		if msgType == "agent_state" {
			return
		}
	}
	t.Fatal("timed out waiting for agent_state after filtered snapshot")
}

func TestNeoRuntimeWebSocketAllowsLocalExtensionEventsForRemoteUI(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	server := httptest.NewServer(http.HandlerFunc(rt.handleHTTP))
	t.Cleanup(server.Close)

	threadID := "T-local-extension-remote-ui"
	seedNeoLocalExtensionStateForTest(t, rt, threadID)

	conn := dialNeoActorWebSocketWithoutResume(t, server.URL, threadID, "cliproxy-client=neo-remote-ui")
	defer conn.Close()
	if err := conn.WriteJSON(map[string]any{"type": "client_resume", "version": 0}); err != nil {
		t.Fatalf("write client_resume: %v", err)
	}

	seen := map[string]bool{}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		msg, ok := readNeoMessage(t, conn, time.Until(deadline))
		if !ok {
			break
		}
		msgType := stringValue(msg["type"])
		seen[msgType] = true
		if msgType == "agent_state" {
			break
		}
	}
	for _, msgType := range []string{"artifacts_snapshot", "draft", "main-thread", "max-tokens", "setPendingNavigation"} {
		if !seen[msgType] {
			t.Fatalf("remote UI did not receive local extension event %q; seen=%v", msgType, seen)
		}
	}
}

func TestNeoRuntimeStopClosesActorWebSockets(t *testing.T) {
	port := freeTCPPortForTest(t)
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{
		Host: "127.0.0.1",
		Port: port,
	}}})
	if err := rt.start(); err != nil {
		t.Fatalf("start runtime: %v", err)
	}

	threadID := "T-stop-closes-websocket"
	dialer := websocket.Dialer{Subprotocols: []string{"rivet", "rivet_encoding.4", "rivet_skip_ready_wait"}}
	wsURL := fmt.Sprintf("ws://127.0.0.1:%d/gateway/threadActor/?rvt-method=getOrCreate&rvt-key=%s", port, url.QueryEscape(threadID))
	var conn *websocket.Conn
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
		nextConn, _, err := dialer.Dial(wsURL, nil)
		if err == nil {
			conn = nextConn
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if conn == nil {
		t.Fatal("gateway websocket did not connect")
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rt.stop(ctx); err != nil {
		t.Fatalf("stop runtime: %v", err)
	}

	deadline := time.Now().Add(500 * time.Millisecond)
	for {
		_ = conn.SetReadDeadline(deadline)
		if _, _, err := conn.ReadMessage(); err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				t.Fatalf("websocket was not closed by runtime stop: %v", err)
			}
			return
		}
	}
}

func TestNeoRuntimeShutdownClosesActorWebSocketsAsTransportFailure(t *testing.T) {
	port := freeTCPPortForTest(t)
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{
		Host: "127.0.0.1",
		Port: port,
	}}})
	if err := rt.start(); err != nil {
		t.Fatalf("start runtime: %v", err)
	}

	threadID := "T-shutdown-transport-failure"
	conn := dialNeoActorWebSocket(t, fmt.Sprintf("http://127.0.0.1:%d", port), threadID)
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rt.shutdown(ctx); err != nil {
		t.Fatalf("shutdown runtime: %v", err)
	}

	var closeErr *websocket.CloseError
	deadline := time.Now().Add(500 * time.Millisecond)
	for {
		_ = conn.SetReadDeadline(deadline)
		_, _, err := conn.ReadMessage()
		if err == nil {
			if time.Now().After(deadline) {
				t.Fatal("websocket stayed open after runtime shutdown")
			}
			continue
		}
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			t.Fatalf("websocket stayed open after runtime shutdown: %v", err)
		}
		if errors.As(err, &closeErr) {
			break
		}
		return
	}
	if closeErr != nil && closeErr.Code == websocket.CloseGoingAway {
		t.Fatalf("shutdown close code = %d, want transport failure so Amp treats restart as reconnectable", closeErr.Code)
	}
}

func TestNeoRuntimeBridgeShutdownClosesWebSocketAsTransportFailure(t *testing.T) {
	port := freeTCPPortForTest(t)
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{
		Host: "127.0.0.1",
		Port: port,
	}}})
	if err := rt.start(); err != nil {
		t.Fatalf("start runtime: %v", err)
	}

	gin.SetMode(gin.TestMode)
	r := gin.New()
	m := &AmpModule{neoRuntime: rt}
	r.Any("/gateway/*path", func(c *gin.Context) { m.serveNeoRuntimeBridge(c) })
	server := httptest.NewServer(r)
	defer server.Close()

	threadID := "T-bridge-shutdown-transport-failure"
	dialer := websocket.Dialer{Subprotocols: []string{"rivet", "rivet_encoding.4", "rivet_skip_ready_wait"}}
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/gateway/threadActor/?rvt-method=getOrCreate&rvt-key=" + url.QueryEscape(threadID)
	conn, resp, err := dialer.Dial(wsURL, nil)
	if err != nil {
		status := 0
		if resp != nil {
			status = resp.StatusCode
		}
		t.Fatalf("bridge websocket dial failed status=%d err=%v", status, err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rt.shutdown(ctx); err != nil {
		t.Fatalf("shutdown runtime: %v", err)
	}

	var closeErr *websocket.CloseError
	deadline := time.Now().Add(500 * time.Millisecond)
	for {
		_ = conn.SetReadDeadline(deadline)
		_, _, err := conn.ReadMessage()
		if err == nil {
			if time.Now().After(deadline) {
				t.Fatal("bridge websocket stayed open after runtime shutdown")
			}
			continue
		}
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			t.Fatalf("bridge websocket stayed open after runtime shutdown: %v", err)
		}
		if errors.As(err, &closeErr) {
			break
		}
		return
	}
	if closeErr != nil && closeErr.Code == websocket.CloseGoingAway {
		t.Fatalf("bridge shutdown close code = %d, want transport failure so Amp treats restart as reconnectable", closeErr.Code)
	}
}

func TestNeoRuntimeShutdownFlushesLocalThreadSnapshot(t *testing.T) {
	dir := t.TempDir()
	oldStoreDir := neoAmpThreadStoreDir
	neoAmpThreadStoreDir = func() string { return dir }
	t.Cleanup(func() { neoAmpThreadStoreDir = oldStoreDir })

	port := freeTCPPortForTest(t)
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{
		Host: "127.0.0.1",
		Port: port,
	}}})
	if err := rt.start(); err != nil {
		t.Fatalf("start runtime: %v", err)
	}

	threadID := "T-shutdown-flush"
	actor := rt.store.ensureThreadActor(threadID)
	actor.mu.Lock()
	actor.currentAgentMode = "deep"
	actor.currentReasoningEffort = "xhigh"
	actor.settings = map[string]any{"agentMode": "deep", "reasoning.effort": "xhigh"}
	actor.messages = []neoMessage{{
		ThreadID:        threadID,
		MessageID:       "M-0000000000000000000001",
		Role:            "user",
		Content:         []any{map[string]any{"type": "text", "text": "persist me before restart"}},
		AgentMode:       "deep",
		ReasoningEffort: "xhigh",
		Seq:             1,
	}, {
		ThreadID:  threadID,
		MessageID: "M-0000000000000000000002",
		Role:      "assistant",
		Content:   []any{map[string]any{"type": "text", "text": "partial answer"}},
		State:     map[string]any{"type": "streaming"},
		Seq:       2,
	}}
	actor.currentInference = &neoInferenceInflight{messageID: "M-0000000000000000000002", agentMode: "deep", reasoningEffort: "xhigh", tools: []string{"shell_command"}}
	actor.seq = 2
	actor.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rt.shutdown(ctx); err != nil {
		t.Fatalf("shutdown runtime: %v", err)
	}

	thread, ok := loadNeoLocalThread(threadID)
	if !ok {
		t.Fatal("shutdown did not write local thread snapshot")
	}
	if got := stringValue(thread["agentMode"]); got != "deep" {
		t.Fatalf("agentMode = %q, want deep", got)
	}
	messages := arrayValue(thread["messages"])
	if len(messages) != 2 || !strings.Contains(fmt.Sprint(messages[0]), "persist me before restart") {
		t.Fatalf("messages = %#v, want persisted user message", messages)
	}
	if got := stringValue(mapValue(mapValue(messages[1])["state"])["type"]); got != "cancelled" {
		t.Fatalf("interrupted assistant state = %q, want cancelled", got)
	}
	pending := mapValue(thread["pendingInference"])
	if stringValue(pending["agentMode"]) != "deep" || stringValue(pending["reasoningEffort"]) != "xhigh" {
		t.Fatalf("pendingInference = %#v, want deep/xhigh resume marker", pending)
	}
	if _, err := os.Stat(filepath.Join(dir, threadID+".json")); err != nil {
		t.Fatalf("snapshot stat: %v", err)
	}
}

func TestNeoRuntimeShutdownPreservesSpawnedExecutors(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("sleep command and signal 0 are Unix-specific")
	}
	port := freeTCPPortForTest(t)
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{
		Host: "127.0.0.1",
		Port: port,
	}}})
	if err := rt.start(); err != nil {
		t.Fatalf("start runtime: %v", err)
	}

	cmd := exec.Command("sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Skipf("sleep command unavailable: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})

	threadID := "T-shutdown-preserve-executor"
	actor := rt.store.ensureThreadActor(threadID)
	actor.mu.Lock()
	actor.spawnedExecutors["spawn-preserve"] = &neoSpawnedExecutor{
		spawnID:  "spawn-preserve",
		threadID: threadID,
		cmd:      cmd,
	}
	actor.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rt.shutdown(ctx); err != nil {
		t.Fatalf("shutdown runtime: %v", err)
	}
	if err := cmd.Process.Signal(syscall.Signal(0)); err != nil {
		t.Fatalf("spawned executor was not preserved across shutdown: %v", err)
	}
}

func TestNeoConfigureSpawnedExecutorProcessDetachesFromServiceGroup(t *testing.T) {
	cmd := exec.Command("amp", "--headless", "T-detach")
	neoConfigureSpawnedExecutorProcess(cmd)
	if !neoSpawnedExecutorDetachedForTest(cmd) {
		t.Fatal("spawned executor was not configured to survive service process group shutdown")
	}
}

func TestNeoRuntimeEnsureThreadActorIndexesGatewayThreadActorName(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	threadID := "T-index-gateway"
	created := rt.store.ensureThreadActor(threadID)

	server := httptest.NewServer(http.HandlerFunc(rt.handleHTTP))
	t.Cleanup(server.Close)
	conn := dialNeoActorWebSocket(t, server.URL, threadID)
	defer conn.Close()

	actors := rt.store.findActors(url.Values{"name": []string{"threadActor"}, "key": []string{threadID}})
	if len(actors) != 1 {
		t.Fatalf("gateway-indexed actor count = %d, actors=%#v", len(actors), actors)
	}
	if stringValue(actors[0]["actor_id"]) != created.id && stringValue(actors[0]["id"]) != created.id {
		t.Fatalf("gateway lookup returned wrong actor: got %#v want id %q", actors[0], created.id)
	}
}

func TestNeoRuntimeGatewayThreadActorImportRoute(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	server := httptest.NewServer(http.HandlerFunc(rt.handleHTTP))
	t.Cleanup(server.Close)

	threadID := "T-019e1046-656d-7132-879f-390ded941c16"
	body := bytes.NewBufferString(`{"thread":{"id":"` + threadID + `","agentMode":"smart","messages":[{"role":"user","content":[{"type":"text","text":"hi"}]}]}}`)
	req, err := http.NewRequest(http.MethodPost, server.URL+"/gateway/threadActor/request/import?rvt-method=getOrCreate&rvt-key="+url.QueryEscape(threadID), body)
	if err != nil {
		t.Fatalf("new import request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post import request: %v", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("import status=%d body=%s", resp.StatusCode, respBody)
	}
	if !strings.Contains(string(respBody), threadID) {
		t.Fatalf("import response missing thread id: %s", respBody)
	}

	actors := rt.store.findActors(url.Values{"name": []string{"threadActor"}, "key": []string{threadID}})
	if len(actors) != 1 {
		t.Fatalf("gateway actor count = %d, actors=%#v", len(actors), actors)
	}
	rt.store.mu.RLock()
	actorID := rt.store.byNameKey["threadActor\x00"+threadID]
	actor := rt.store.actors[actorID]
	rt.store.mu.RUnlock()
	if actor == nil {
		t.Fatalf("missing imported actor id=%q", actorID)
	}
	actor.mu.Lock()
	messageCount := len(actor.messages)
	actor.mu.Unlock()
	if messageCount != 1 {
		t.Fatalf("imported message count = %d, want 1", messageCount)
	}
}

func TestNeoRuntimeGatewayThreadActorImportRejectsMissingAgentModeLikeBinary(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	server := httptest.NewServer(http.HandlerFunc(rt.handleHTTP))
	t.Cleanup(server.Close)

	threadID := "T-019e1046-656d-7132-879f-390ded941c17"
	body := bytes.NewBufferString(`{"thread":{"id":"` + threadID + `","messages":[{"role":"user","content":[{"type":"text","text":"hi"}]}]}}`)
	req, err := http.NewRequest(http.MethodPost, server.URL+"/gateway/threadActor/request/import?rvt-method=getOrCreate&rvt-key="+url.QueryEscape(threadID), body)
	if err != nil {
		t.Fatalf("new import request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post import request: %v", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("import status=%d body=%s", resp.StatusCode, respBody)
	}
	if !strings.Contains(string(respBody), "agent mode could not be determined from thread") {
		t.Fatalf("import response missing binary-style error: %s", respBody)
	}
}

func TestNeoRuntimeGatewayThreadActorContextAnalysisRoute(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	server := httptest.NewServer(http.HandlerFunc(rt.handleHTTP))
	t.Cleanup(server.Close)

	threadID := "T-019e1046-656d-7132-879f-390ded941c16"
	actor, _ := rt.store.upsert(map[string]any{"name": "threadActor", "key": threadID, "input": map[string]any{"threadId": threadID}}, true)
	if err := actor.importThreadLocalOnly(map[string]any{
		"id":        threadID,
		"agentMode": "smart",
		"messages": []any{
			map[string]any{"role": "user", "messageId": "M-user", "content": []any{map[string]any{"type": "text", "text": "please summarize the local runtime routes"}}, "agentMode": "smart"},
			map[string]any{"role": "assistant", "messageId": "M-assistant", "content": []any{map[string]any{"type": "text", "text": "The runtime serves gateway request paths."}}, "state": map[string]any{"type": "complete"}},
		},
	}); err != nil {
		t.Fatalf("import thread: %v", err)
	}

	req, err := http.NewRequest(http.MethodGet, server.URL+"/gateway/threadActor/request/context-analysis?rvt-method=get&rvt-key="+url.QueryEscape(threadID)+"&rvt-skip-ready-wait=true", nil)
	if err != nil {
		t.Fatalf("new context analysis request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get context analysis: %v", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("context analysis status=%d body=%s", resp.StatusCode, respBody)
	}
	var payload map[string]any
	if err := json.Unmarshal(respBody, &payload); err != nil {
		t.Fatalf("decode context analysis response: %v body=%s", err, respBody)
	}
	analysis := mapValue(payload["analysis"])
	if !boolValue(payload["ok"]) || stringValue(analysis["modelDisplayName"]) == "" || intValue(analysis["maxContextTokens"]) <= 0 || intValue(analysis["totalTokens"]) <= 0 {
		t.Fatalf("unexpected context analysis payload: %#v", payload)
	}
	sections := arrayValue(analysis["sections"])
	if len(sections) < 2 {
		t.Fatalf("context analysis sections = %#v, want system and conversation sections", sections)
	}
	foundConversation := false
	for _, section := range sections {
		if stringValue(mapValue(section)["name"]) == "Conversation" {
			foundConversation = true
			break
		}
	}
	if !foundConversation {
		t.Fatalf("context analysis missing conversation section: %#v", sections)
	}
}

func TestNeoRuntimeGatewayThreadActorDynamicReloadRoute(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	server := httptest.NewServer(http.HandlerFunc(rt.handleHTTP))
	t.Cleanup(server.Close)

	req, err := http.NewRequest(http.MethodPut, server.URL+"/gateway/threadActor/dynamic/reload?rvt-method=get&rvt-key=T-reload", nil)
	if err != nil {
		t.Fatalf("new dynamic reload request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("put dynamic reload: %v", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(respBody), `"ok":true`) {
		t.Fatalf("dynamic reload status=%d body=%s", resp.StatusCode, respBody)
	}
}

func TestNeoRuntimeSnapshotThreadRelationshipsIncludesSeq(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	server := httptest.NewServer(http.HandlerFunc(rt.handleHTTP))
	t.Cleanup(server.Close)

	dialer := websocket.Dialer{Subprotocols: []string{"rivet", "rivet_encoding.4", "rivet_skip_ready_wait"}}
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/gateway/threadActor/?rvt-method=getOrCreate&rvt-key=T-seq"
	conn, resp, err := dialer.Dial(wsURL, nil)
	if err != nil {
		status := 0
		if resp != nil {
			status = resp.StatusCode
		}
		t.Fatalf("gateway websocket dial failed status=%d err=%v", status, err)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))

	for {
		_, payload, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read snapshot message: %v", err)
		}
		var msg map[string]any
		if err := json.Unmarshal(payload, &msg); err != nil {
			t.Fatalf("snapshot JSON error: %v", err)
		}
		if msg["type"] != "thread_relationships" {
			continue
		}
		if numberFrom(msg["seq"]) == 0 {
			t.Fatalf("thread_relationships missing seq: %#v", msg)
		}
		return
	}
}

func TestNeoRuntimeSnapshotReplaysOfficialMessageAddedFrames(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	threadID := "T-019e0e6e-f3f1-7077-b5dd-748f66f8c25d"
	actor, _ := rt.store.upsert(map[string]any{"name": "threadActor", "key": threadID, "input": map[string]any{"threadId": threadID}}, true)
	userMessageID := newNeoMessageID()
	assistantMessageID := newNeoMessageID()
	actor.mu.Lock()
	actor.messages = []neoMessage{
		{ThreadID: threadID, MessageID: userMessageID, Role: "user", Content: []any{map[string]any{"type": "text", "text": "hi"}}, Seq: 1},
		{ThreadID: threadID, MessageID: assistantMessageID, Role: "assistant", Content: []any{map[string]any{"type": "text", "text": "hello"}}, Seq: 2},
	}
	actor.seq = 3
	actor.mu.Unlock()

	server := httptest.NewServer(http.HandlerFunc(rt.handleHTTP))
	t.Cleanup(server.Close)

	conn := dialNeoActorWebSocket(t, server.URL, threadID)
	defer conn.Close()

	messageAdded := 0
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		msg, ok := readNeoMessage(t, conn, time.Until(deadline))
		if !ok {
			break
		}
		switch msg["type"] {
		case "messages":
			t.Fatalf("snapshot emitted non-official bulk messages frame: %#v", msg)
		case "message_added":
			messageAdded++
		case "agent_state":
			if messageAdded != 2 {
				t.Fatalf("message_added count = %d, want 2", messageAdded)
			}
			return
		}
	}
	t.Fatalf("timed out waiting for snapshot agent_state, saw %d message_added frame(s)", messageAdded)
}

func TestNeoThreadRelationshipsUseOfficialSchema(t *testing.T) {
	validThreadID := "T-019e1046-656d-7132-879f-390ded941c16"
	relationships := neoThreadRelationships([]neoMessage{{
		ThreadID:  "T-current",
		MessageID: "M-assistant",
		Role:      "assistant",
		Content: []any{map[string]any{
			"type":  "tool_use",
			"name":  "read_thread",
			"input": map[string]any{"threadID": validThreadID},
		}},
		CreatedAt: "2026-05-07T21:00:00Z",
	}})
	if len(relationships) != 1 {
		t.Fatalf("relationships = %#v, want one valid cloud thread relationship", relationships)
	}
	relationship := mapValue(relationships[0])
	if stringValue(relationship["threadID"]) != validThreadID || stringValue(relationship["type"]) != "mention" || stringValue(relationship["role"]) != "parent" {
		t.Fatalf("relationship = %#v", relationship)
	}
	if numberFrom(relationship["createdAt"]) == 0 || numberFrom(relationship["messageIndex"]) != 0 {
		t.Fatalf("relationship missing createdAt/messageIndex: %#v", relationship)
	}
}

func TestNeoThreadRelationshipsIgnoreBareUserThreadID(t *testing.T) {
	validThreadID := "T-019e1046-656d-7132-879f-390ded941c16"
	relationships := neoThreadRelationships([]neoMessage{{
		ThreadID:  "T-current",
		MessageID: "M-user",
		Role:      "user",
		Content: []any{map[string]any{
			"type": "text",
			"text": "following: @" + validThreadID,
		}},
		CreatedAt: "2026-05-07T21:00:00Z",
	}})
	if len(relationships) != 0 {
		t.Fatalf("relationships = %#v, want no relationship until read_thread is used", relationships)
	}
}

func TestNeoThreadRelationshipsIgnoreIncompleteReadThreadLikeBinary(t *testing.T) {
	validThreadID := "T-019e1046-656d-7132-879f-390ded941c16"
	relationships := neoThreadRelationships([]neoMessage{{
		ThreadID:  "T-current",
		MessageID: "M-assistant",
		Role:      "assistant",
		Content: []any{map[string]any{
			"type":     "tool_use",
			"name":     "read_thread",
			"complete": false,
			"input":    map[string]any{"threadID": validThreadID},
		}},
		CreatedAt: "2026-05-07T21:00:00Z",
	}})
	if len(relationships) != 0 {
		t.Fatalf("relationships = %#v, want no incomplete read_thread relationship", relationships)
	}
}

func TestNeoActorThreadRelationshipEventsUseOfficialSchema(t *testing.T) {
	dir := t.TempDir()
	oldStoreDir := neoAmpThreadStoreDir
	neoAmpThreadStoreDir = func() string { return dir }
	t.Cleanup(func() { neoAmpThreadStoreDir = oldStoreDir })

	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "threadActor", "T-current", "T-current", neoActorRecord("actor-test", "threadActor", "T-current"), nil)
	threadID := "T-019e1046-656d-7132-879f-390ded941c16"

	actor.handleCreateThread(map[string]any{"type": "client_create_thread", "threadId": threadID})
	actor.mu.Lock()
	if len(actor.replayEvents) == 0 {
		actor.mu.Unlock()
		t.Fatal("missing relationship replay event")
	}
	created := actor.replayEvents[len(actor.replayEvents)-1].Payload
	actor.mu.Unlock()
	if created["type"] != "thread_relationships" || numberFrom(created["seq"]) == 0 {
		t.Fatalf("created relationship payload = %#v", created)
	}
	relationship := mapValue(arrayValue(created["relationships"])[0])
	if stringValue(relationship["threadID"]) != threadID || stringValue(relationship["type"]) != "mention" || stringValue(relationship["role"]) != "child" || numberFrom(relationship["createdAt"]) == 0 {
		t.Fatalf("created relationship = %#v", relationship)
	}
	if _, exists := relationship["sourceThreadId"]; exists {
		t.Fatalf("relationship should not include legacy sourceThreadId: %#v", relationship)
	}
	unknownRelationship, ok := neoProtocolThreadRelationship(threadID, "client_create_thread", "child", 123, "")
	if !ok || stringValue(unknownRelationship["type"]) != "mention" {
		t.Fatalf("unknown relationship should default to mention: %#v ok=%v", unknownRelationship, ok)
	}
	legacyRelationship, ok := neoProtocolThreadRelationship(threadID, "handoff", "child", 123, "")
	if !ok || stringValue(legacyRelationship["type"]) != "handoff" {
		t.Fatalf("explicit legacy relationship should be preserved: %#v ok=%v", legacyRelationship, ok)
	}

	forkID := "T-019e1046-656d-7132-879f-390ded941c17"
	actor.handleForkThread(map[string]any{"type": "client_fork_thread", "threadId": forkID, "forkMessageId": "M-user"})
	actor.mu.Lock()
	forked := actor.replayEvents[len(actor.replayEvents)-1].Payload
	actor.mu.Unlock()
	forkRelationships := arrayValue(forked["relationships"])
	if len(forkRelationships) != 2 {
		t.Fatalf("fork relationship payload = %#v, want full relationship list", forked)
	}
	var forkRelationship map[string]any
	for _, raw := range forkRelationships {
		relationship := mapValue(raw)
		if stringValue(relationship["threadID"]) == forkID {
			forkRelationship = relationship
			break
		}
	}
	if stringValue(forkRelationship["threadID"]) != forkID || stringValue(forkRelationship["type"]) != "fork" || stringValue(forkRelationship["role"]) != "child" || stringValue(forkRelationship["comment"]) == "" {
		t.Fatalf("fork relationship = %#v", forkRelationship)
	}
	snapshot, ok := actor.threadSnapshot()
	if !ok {
		t.Fatal("threadSnapshot returned false")
	}
	cloudRelationships := arrayValue(neoCloudThread(snapshot)["relationships"])
	if len(cloudRelationships) != 2 {
		t.Fatalf("persisted relationships = %#v, want created and fork relationships", cloudRelationships)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		actor.mu.Lock()
		running := actor.syncRunning
		actor.mu.Unlock()
		if !running {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("relationship sync did not finish")
}

func TestNeoActorBinaryRelationshipDeltasAreSetLikeAndEmitFullList(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	firstID := "T-019e1046-656d-7132-879f-390ded941c16"
	secondID := "T-019e1046-656d-7132-879f-390ded941c17"

	actor.handle(map[string]any{
		"type": "relationship",
		"relationship": map[string]any{
			"threadID":  firstID,
			"type":      "mention",
			"role":      "parent",
			"createdAt": float64(1),
		},
	})
	actor.mu.Lock()
	firstReplayLen := len(actor.replayEvents)
	actor.mu.Unlock()

	actor.handle(map[string]any{
		"type": "relationship",
		"relationship": map[string]any{
			"threadID":  firstID,
			"type":      "mention",
			"role":      "parent",
			"createdAt": float64(2),
			"comment":   "duplicate should not replace",
		},
	})
	actor.mu.Lock()
	if len(actor.relationships) != 1 || len(actor.replayEvents) != firstReplayLen {
		t.Fatalf("duplicate relationship mutated state relationships=%#v replay=%d want replay=%d", actor.relationships, len(actor.replayEvents), firstReplayLen)
	}
	actor.mu.Unlock()

	actor.handle(map[string]any{
		"type": "relationship",
		"relationship": map[string]any{
			"threadID":  secondID,
			"type":      "mention",
			"role":      "child",
			"createdAt": float64(3),
		},
	})
	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.relationships) != 2 {
		t.Fatalf("relationships = %#v, want two", actor.relationships)
	}
	last := actor.replayEvents[len(actor.replayEvents)-1].Payload
	relationships := arrayValue(last["relationships"])
	if last["type"] != "thread_relationships" || len(relationships) != 2 {
		t.Fatalf("last relationship event = %#v, want full list", last)
	}
}

func TestNeoActorBinaryRelationshipPreservesRawShapeLikeBinary(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	relationship := map[string]any{
		"threadID": "not-a-cloud-thread-id",
		"type":     "custom-type",
		"role":     "custom-role",
		"note":     "preserve me",
	}

	actor.handle(map[string]any{"type": "relationship", "relationship": relationship})

	actor.mu.Lock()
	if len(actor.relationships) != 1 {
		actor.mu.Unlock()
		t.Fatalf("relationships = %#v, want one raw relationship", actor.relationships)
	}
	stored := actor.relationships[0]
	if stored["threadID"] != "not-a-cloud-thread-id" || stored["type"] != "custom-type" || stored["role"] != "custom-role" || stored["note"] != "preserve me" {
		actor.mu.Unlock()
		t.Fatalf("stored relationship = %#v, want raw reducer shape", stored)
	}
	if _, exists := stored["createdAt"]; exists {
		actor.mu.Unlock()
		t.Fatalf("stored relationship gained createdAt: %#v", stored)
	}
	last := actor.replayEvents[len(actor.replayEvents)-1].Payload
	actor.mu.Unlock()

	replayed := arrayValue(last["relationships"])
	if len(replayed) != 0 {
		t.Fatalf("replayed relationships = %#v, want invalid raw shape filtered from protocol event", replayed)
	}
	stateRelationship := mapValue(arrayValue(actor.stateSnapshotResponse()["relationships"])[0])
	if stateRelationship["threadID"] != "not-a-cloud-thread-id" || stateRelationship["type"] != "custom-type" || stateRelationship["role"] != "custom-role" || stateRelationship["note"] != "preserve me" {
		t.Fatalf("state relationship = %#v, want raw reducer shape", stateRelationship)
	}
	snapshot, ok := actor.threadSnapshot()
	if !ok {
		t.Fatal("threadSnapshot returned false")
	}
	cloudRelationship := mapValue(arrayValue(neoCloudThread(snapshot)["relationships"])[0])
	if cloudRelationship["threadID"] != "not-a-cloud-thread-id" || cloudRelationship["type"] != "custom-type" || cloudRelationship["role"] != "custom-role" || cloudRelationship["note"] != "preserve me" {
		t.Fatalf("cloud relationship = %#v, want raw reducer shape", cloudRelationship)
	}
}

func TestNeoActorBinaryDraftMissingContentClearsLikeBinary(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.draft = []any{map[string]any{"type": "text", "text": "stale draft"}}

	actor.handle(map[string]any{"type": "draft"})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if actor.draft != nil {
		t.Fatalf("draft = %#v, want nil when binary draft content is missing", actor.draft)
	}
	last := actor.replayEvents[len(actor.replayEvents)-1].Payload
	if content, exists := last["content"]; !exists || content != nil {
		t.Fatalf("draft replay content = %#v exists=%v, want explicit nil", content, exists)
	}
}

func TestNeoActorArchiveUsesTopLevelArchivedFlag(t *testing.T) {
	dir := t.TempDir()
	oldStoreDir := neoAmpThreadStoreDir
	neoAmpThreadStoreDir = func() string { return dir }
	t.Cleanup(func() { neoAmpThreadStoreDir = oldStoreDir })

	rt := newNeoRuntime(&config.Config{})
	threadID := "T-019e1046-656d-7132-879f-390ded941c18"
	actor := newNeoActor(rt, "actor-test", "threadActor", threadID, threadID, neoActorRecord("actor-test", "threadActor", threadID), nil)

	actor.archiveThread(true, nil)
	waitForNeoActorSyncIdle(t, actor)

	actor.mu.Lock()
	archived := actor.archived
	threadStatus := actor.threadStatus
	actor.mu.Unlock()
	if !archived || threadStatus != "" {
		t.Fatalf("archive state = archived:%v threadStatus:%q, want archived true and empty official thread status", archived, threadStatus)
	}
	snapshot, ok := actor.threadSnapshot()
	if !ok {
		t.Fatal("threadSnapshot returned false")
	}
	thread := marshalNeoThreadForTest(t, neoCloudThread(snapshot))
	if thread["archived"] != true || thread["threadStatus"] != nil {
		t.Fatalf("cloud archive fields = archived:%#v threadStatus:%#v", thread["archived"], thread["threadStatus"])
	}

	actor.archiveThread(false, nil)
	waitForNeoActorSyncIdle(t, actor)
	snapshot, ok = actor.threadSnapshot()
	if !ok {
		t.Fatal("threadSnapshot returned false after unarchive")
	}
	thread = marshalNeoThreadForTest(t, neoCloudThread(snapshot))
	if thread["archived"] != false {
		t.Fatalf("cloud archived after unarchive = %#v, want false", thread["archived"])
	}
}

func TestNeoRuntimeSnapshotIncludesThreadStatusAndCompactionRecords(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	threadID := "T-019e0e6e-f3f1-7078-b5dd-748f66f8c25d"
	actor, _ := rt.store.upsert(map[string]any{"name": "threadActor", "key": threadID, "input": map[string]any{"threadId": threadID}}, true)
	actor.mu.Lock()
	actor.compactionRecords = []map[string]any{{"cutMessageId": "M-00000000000000000000aa", "createdAt": "2026-01-01T00:00:00Z"}}
	actor.mu.Unlock()

	server := httptest.NewServer(http.HandlerFunc(rt.handleHTTP))
	t.Cleanup(server.Close)

	dialer := websocket.Dialer{Subprotocols: []string{"rivet", "rivet_encoding.4", "rivet_skip_ready_wait"}}
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/gateway/threadActor/?rvt-method=getOrCreate&rvt-key=" + url.QueryEscape(threadID)
	conn, resp, err := dialer.Dial(wsURL, nil)
	if err != nil {
		status := 0
		if resp != nil {
			status = resp.StatusCode
		}
		t.Fatalf("gateway websocket dial failed status=%d err=%v", status, err)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))

	sawThreadStatus := false
	sawCompactionRecords := false
	for !sawThreadStatus || !sawCompactionRecords {
		_, payload, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read snapshot message: %v", err)
		}
		var msg map[string]any
		if err := json.Unmarshal(payload, &msg); err != nil {
			t.Fatalf("snapshot JSON error: %v", err)
		}
		switch msg["type"] {
		case "thread_status":
			status, ok := msg["status"]
			if !ok {
				t.Fatalf("thread_status missing nullable status: %#v", msg)
			}
			if status != nil {
				t.Fatalf("thread_status status = %#v, want null", status)
			}
			sawThreadStatus = true
		case "compaction_records":
			records := arrayValue(msg["records"])
			if len(records) != 1 || stringValue(mapValue(records[0])["cutMessageId"]) != "M-00000000000000000000aa" {
				t.Fatalf("compaction_records = %#v", msg)
			}
			sawCompactionRecords = true
		}
	}
}

func TestNeoRuntimeSnapshotIncludesToolApprovalQueue(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	threadID := "T-019e0e6e-f3f1-7079-b5dd-748f66f8c25d"
	actor, _ := rt.store.upsert(map[string]any{"name": "threadActor", "key": threadID, "input": map[string]any{"threadId": threadID}}, true)
	actor.mu.Lock()
	actor.approvalQueue = []map[string]any{{
		"id":               "TU-child",
		"toolCallId":       "TU-child",
		"toolUseId":        "TU-child",
		"toolName":         "Bash",
		"args":             map[string]any{"cmd": "pwd"},
		"context":          "subagent",
		"parentToolCallId": "TU-parent",
		"subagentToolName": "researcher",
		"timestamp":        123,
		"reason":           "approval needed",
		"toAllow":          []any{"pwd"},
		"ruleSource":       "built-in",
		"matchedRule":      map[string]any{"tool": "Bash", "action": "ask"},
	}}
	actor.mu.Unlock()

	server := httptest.NewServer(http.HandlerFunc(rt.handleHTTP))
	t.Cleanup(server.Close)

	conn := dialNeoActorWebSocket(t, server.URL, threadID)
	defer conn.Close()
	queue := waitForNeoMessageType(t, conn, "tool_approval_queue", 2*time.Second)
	approvals := arrayValue(queue["approvals"])
	if len(approvals) != 1 {
		t.Fatalf("approval queue snapshot = %#v", queue)
	}
	approval := mapValue(approvals[0])
	if stringValue(approval["toolCallId"]) != "TU-child" || stringValue(approval["parentToolCallId"]) != "TU-parent" || stringValue(approval["subagentToolName"]) != "researcher" {
		t.Fatalf("approval queue snapshot approval = %#v", approval)
	}
	if _, exists := approval["toolUseId"]; exists {
		t.Fatalf("approval queue snapshot leaked toolUseId alias: %#v", approval)
	}
}

func TestNeoRuntimeClientResumeIsSocketScoped(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	threadID := "T-019e0e6e-f3f1-7080-b5dd-748f66f8c25d"
	actor, _ := rt.store.upsert(map[string]any{"name": "threadActor", "key": threadID, "input": map[string]any{"threadId": threadID}}, true)
	actor.mu.Lock()
	actor.messages = []neoMessage{
		{ThreadID: threadID, MessageID: "M-old", Role: "user", Content: []any{map[string]any{"type": "text", "text": "old"}}, Seq: 1},
		{ThreadID: threadID, MessageID: "M-new", Role: "assistant", Content: []any{map[string]any{"type": "text", "text": "new"}}, Seq: 2},
	}
	actor.seq = 3
	actor.mu.Unlock()

	server := httptest.NewServer(http.HandlerFunc(rt.handleHTTP))
	t.Cleanup(server.Close)

	conn1 := dialNeoActorWebSocket(t, server.URL, threadID)
	defer conn1.Close()
	waitForNeoMessageType(t, conn1, "agent_state", 2*time.Second)
	conn2 := dialNeoActorWebSocket(t, server.URL, threadID)
	defer conn2.Close()
	waitForNeoMessageType(t, conn2, "agent_state", 2*time.Second)
	waitForNeoMessageType(t, conn1, "observers", 2*time.Second)
	waitForNeoMessageType(t, conn2, "observers", 2*time.Second)

	if err := conn1.WriteJSON(map[string]any{"type": "client_resume", "version": 1}); err != nil {
		t.Fatalf("write client_resume: %v", err)
	}

	sawNew := false
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !sawNew {
		msg, ok := readNeoMessage(t, conn1, time.Until(deadline))
		if !ok {
			break
		}
		if msg["type"] != "message_added" {
			continue
		}
		messageID := stringValue(mapValue(msg["message"])["messageId"])
		if messageID == "M-old" {
			t.Fatalf("resume replayed message at or before requested version: %#v", msg)
		}
		if messageID == "M-new" {
			sawNew = true
		}
	}
	if !sawNew {
		t.Fatal("resume did not replay message newer than requested version")
	}
	if msg, ok := readNeoMessage(t, conn2, 150*time.Millisecond); ok {
		t.Fatalf("client_resume snapshot leaked to another socket: %#v", msg)
	}
}

func TestNeoRuntimeSkipReadyWaitDefersSnapshotUntilClientResume(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	threadID := "T-019e0e6e-f3f1-7081-b5dd-748f66f8c25d"
	actor, _ := rt.store.upsert(map[string]any{"name": "threadActor", "key": threadID, "input": map[string]any{"threadId": threadID}}, true)
	actor.mu.Lock()
	actor.messages = []neoMessage{
		{ThreadID: threadID, MessageID: "M-old", Role: "user", Content: []any{map[string]any{"type": "text", "text": "old"}}, Seq: 1},
		{ThreadID: threadID, MessageID: "M-seen", Role: "assistant", Content: []any{map[string]any{"type": "text", "text": "seen"}}, Seq: 2},
	}
	actor.seq = 3
	actor.mu.Unlock()

	server := httptest.NewServer(http.HandlerFunc(rt.handleHTTP))
	t.Cleanup(server.Close)

	dialer := websocket.Dialer{Subprotocols: []string{"rivet", "rivet_encoding.4", "rivet_skip_ready_wait"}}
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/gateway/threadActor/?rvt-method=getOrCreate&rvt-key=" + url.QueryEscape(threadID) + "&rvt-skip-ready-wait=true"
	conn, resp, err := dialer.Dial(wsURL, nil)
	if err != nil {
		status := 0
		if resp != nil {
			status = resp.StatusCode
		}
		t.Fatalf("gateway websocket dial failed status=%d err=%v", status, err)
	}
	defer conn.Close()

	if err := conn.WriteJSON(map[string]any{"type": "client_resume", "version": 2}); err != nil {
		t.Fatalf("write client_resume: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		msg, ok := readNeoMessage(t, conn, time.Until(deadline))
		if !ok {
			break
		}
		if msg["type"] == "message_added" {
			t.Fatalf("resume replayed message at or before requested version: %#v", msg)
		}
		if msg["type"] == "agent_state" {
			return
		}
	}
	t.Fatal("timed out waiting for deferred resume snapshot")
}

func TestNeoRuntimeAcceptsBatchedClientFrames(t *testing.T) {
	dir := t.TempDir()
	oldStoreDir := neoAmpThreadStoreDir
	neoAmpThreadStoreDir = func() string { return dir }
	t.Cleanup(func() { neoAmpThreadStoreDir = oldStoreDir })

	rt := newNeoRuntime(&config.Config{})
	server := httptest.NewServer(http.HandlerFunc(rt.handleHTTP))
	t.Cleanup(server.Close)

	threadID := "T-019e0e6e-f3f1-7081-b5dd-748f66f8c25d"
	conn := dialNeoActorWebSocket(t, server.URL, threadID)
	defer conn.Close()
	waitForNeoMessageType(t, conn, "agent_state", 2*time.Second)
	waitForNeoMessageType(t, conn, "observers", 2*time.Second)

	batch := []byte(`[{"type":"client_set_thread_title","title":"Batch Title"},{"type":"client_update_thread_settings","settings":{"agentMode":"rush"}}]`)
	if err := conn.WriteMessage(websocket.TextMessage, batch); err != nil {
		t.Fatalf("write batched frame: %v", err)
	}

	sawTitle := false
	sawSettings := false
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && (!sawTitle || !sawSettings) {
		msg, ok := readNeoMessage(t, conn, time.Until(deadline))
		if !ok {
			break
		}
		switch msg["type"] {
		case "thread_title":
			sawTitle = msg["title"] == "Batch Title"
		case "thread_settings":
			sawSettings = stringValue(mapValue(msg["settings"])["agentMode"]) == "rush"
		case "error":
			t.Fatalf("batched frame was rejected: %#v", msg)
		}
	}
	if !sawTitle || !sawSettings {
		t.Fatalf("batched frame was not fully applied: title=%v settings=%v", sawTitle, sawSettings)
	}
	waitForNeoActorSyncIdle(t, rt.store.ensureThreadActor(threadID))
}

func TestNeoRuntimeFilesystemBridgeNormalizesBothDirections(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	server := httptest.NewServer(http.HandlerFunc(rt.handleHTTP))
	t.Cleanup(server.Close)

	threadID := "T-019e0e6e-f3f1-7081-b5dd-748f66f8c25e"
	client := dialNeoActorWebSocket(t, server.URL, threadID)
	defer client.Close()
	executor := dialNeoActorWebSocket(t, server.URL, threadID)
	defer executor.Close()
	waitForNeoMessageType(t, client, "agent_state", 2*time.Second)
	waitForNeoMessageType(t, executor, "agent_state", 2*time.Second)

	if err := executor.WriteJSON(map[string]any{"type": "executor_filesystem_read_file", "requestID": "fs-1", "path": "file:///tmp/a.txt", "max_bytes": 42}); err != nil {
		t.Fatalf("write executor filesystem request: %v", err)
	}
	request := waitForNeoMessageType(t, client, "client_filesystem_read_file", 2*time.Second)
	if request["requestId"] != "fs-1" || request["uri"] != "file:///tmp/a.txt" || numberFrom(request["maxBytes"]) != 42 {
		t.Fatalf("filesystem request = %#v", request)
	}

	if err := client.WriteJSON(map[string]any{"type": "client_filesystem_read_file_result", "requestId": "fs-1", "content": "hello"}); err != nil {
		t.Fatalf("write client filesystem result: %v", err)
	}
	result := waitForNeoMessageType(t, executor, "executor_filesystem_read_file_result", 2*time.Second)
	if result["requestId"] != "fs-1" || result["content"] != "hello" {
		t.Fatalf("filesystem result = %#v", result)
	}
}

func TestNeoRuntimeGitCommandMatchesCurrentBinaryProtocol(t *testing.T) {
	repo := t.TempDir()
	if out, err := exec.Command("git", "init", repo).CombinedOutput(); err != nil {
		t.Fatalf("git init: %v\n%s", err, out)
	}
	if err := os.WriteFile(filepath.Join(repo, "untracked.txt"), []byte("hello\n"), 0o600); err != nil {
		t.Fatalf("write repo file: %v", err)
	}

	rt := newNeoRuntime(&config.Config{})
	threadID := "T-019e0e6e-f3f1-7081-b5dd-748f66f8c25f"
	actor, _ := rt.store.upsert(map[string]any{"name": "threadActor", "key": threadID, "input": map[string]any{"threadId": threadID}}, true)
	actor.mu.Lock()
	actor.environment = map[string]any{"workingDirectory": repo, "workspaceRoot": repo}
	actor.mu.Unlock()

	server := httptest.NewServer(http.HandlerFunc(rt.handleHTTP))
	t.Cleanup(server.Close)

	conn := dialNeoActorWebSocket(t, server.URL, threadID)
	defer conn.Close()
	waitForNeoMessageType(t, conn, "agent_state", 2*time.Second)

	if err := conn.WriteJSON(map[string]any{"type": "client_git_command", "requestId": "git-1", "operation": map[string]any{"type": "status"}}); err != nil {
		t.Fatalf("write client_git_command: %v", err)
	}
	result := waitForNeoMessageType(t, conn, "client_git_command_result", 2*time.Second)
	if result["requestId"] != "git-1" || result["ok"] != true || numberFrom(result["exitCode"]) != 0 || !strings.Contains(stringValue(result["stdout"]), "untracked.txt") {
		t.Fatalf("git status result = %#v", result)
	}

	if err := conn.WriteJSON(map[string]any{"type": "client_git_command", "requestId": "git-2", "operation": map[string]any{"type": "status_snapshot"}}); err != nil {
		t.Fatalf("write status_snapshot command: %v", err)
	}
	snapshotResult := waitForNeoMessageType(t, conn, "client_git_command_result", 2*time.Second)
	if snapshotResult["requestId"] != "git-2" || snapshotResult["ok"] != true {
		t.Fatalf("status snapshot result = %#v", snapshotResult)
	}
	var snapshot map[string]any
	if err := json.Unmarshal([]byte(stringValue(snapshotResult["stdout"])), &snapshot); err != nil {
		t.Fatalf("status snapshot JSON: %v payload=%q", err, stringValue(snapshotResult["stdout"]))
	}
	if snapshot["available"] != true || stringValue(snapshot["repositoryRoot"]) != repo {
		realRepo, err := filepath.EvalSymlinks(repo)
		if err != nil || stringValue(snapshot["repositoryRoot"]) != realRepo {
			t.Fatalf("status snapshot = %#v", snapshot)
		}
	}
}

func TestNeoRuntimeGitBridgeAndWorkspaceMessageTypes(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	server := httptest.NewServer(http.HandlerFunc(rt.handleHTTP))
	t.Cleanup(server.Close)

	threadID := "T-019e0e6e-f3f1-7081-b5dd-748f66f8c260"
	conn := dialNeoActorWebSocket(t, server.URL, threadID)
	defer conn.Close()
	waitForNeoMessageType(t, conn, "agent_state", 2*time.Second)

	if err := conn.WriteJSON(map[string]any{"type": "executor_git_command", "requestID": "git-raw", "args": []any{"status"}, "max_output_bytes": 128}); err != nil {
		t.Fatalf("write executor_git_command: %v", err)
	}
	rawRequest := waitForNeoMessageType(t, conn, "executor_git_command", 2*time.Second)
	if rawRequest["requestId"] != "git-raw" || numberFrom(rawRequest["maxOutputBytes"]) != 128 {
		t.Fatalf("executor git command request = %#v", rawRequest)
	}

	if err := conn.WriteJSON(map[string]any{"type": "executor_git_command_result", "requestId": "git-raw", "ok": true, "exitCode": 0, "stdout": "ok", "stderr": ""}); err != nil {
		t.Fatalf("write executor_git_command_result: %v", err)
	}
	clientResult := waitForNeoMessageType(t, conn, "client_git_command_result", 2*time.Second)
	if clientResult["requestId"] != "git-raw" || clientResult["stdout"] != "ok" {
		t.Fatalf("client git command result = %#v", clientResult)
	}

	if err := conn.WriteJSON(map[string]any{"type": "client_git_command_result", "requestId": "git-client", "ok": false, "error": map[string]any{"code": "INTERNAL_ERROR", "message": "failed"}}); err != nil {
		t.Fatalf("write client_git_command_result: %v", err)
	}
	executorResult := waitForNeoMessageType(t, conn, "executor_git_command_result", 2*time.Second)
	if executorResult["requestId"] != "git-client" || executorResult["ok"] != false {
		t.Fatalf("executor git command result = %#v", executorResult)
	}

	if err := conn.WriteJSON(map[string]any{"type": "executor_workspace_maybe_changed", "toolCallId": "TU-git", "toolName": "apply_patch"}); err != nil {
		t.Fatalf("write executor_workspace_maybe_changed: %v", err)
	}
	workspace := waitForNeoMessageType(t, conn, "executor_workspace_maybe_changed", 2*time.Second)
	if workspace["toolCallId"] != "TU-git" || workspace["toolName"] != "apply_patch" {
		t.Fatalf("workspace maybe changed = %#v", workspace)
	}
}

func TestNeoRuntimeExecutorConnectAlwaysRequiresFreshBootstrap(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	server := httptest.NewServer(http.HandlerFunc(rt.handleHTTP))
	t.Cleanup(server.Close)

	conn := dialNeoActorWebSocket(t, server.URL, "T-019e0e6e-f3f1-7082-b5dd-748f66f8c25d")
	defer conn.Close()
	waitForNeoMessageType(t, conn, "agent_state", 2*time.Second)

	connect := map[string]any{
		"type":     "executor_connect",
		"clientId": "executor-test",
		"capabilities": map[string]any{
			"workspaceId":      "/tmp/workspace",
			"workingDirectory": "/tmp/workspace",
			"environment":      map[string]any{"os": "darwin"},
			"tags":             []any{},
		},
	}
	if err := conn.WriteJSON(connect); err != nil {
		t.Fatalf("write executor_connect: %v", err)
	}
	first := waitForNeoMessageType(t, conn, "executor_connected", 2*time.Second)
	if boolValue(first["resumeBootstrap"]) {
		t.Fatalf("initial executor_connect resumeBootstrap = true: %#v", first)
	}

	if err := conn.WriteJSON(map[string]any{
		"type":  "executor_tools_register",
		"tools": []any{map[string]any{"name": "shell_command", "description": "run shell command", "inputSchema": map[string]any{"type": "object"}}},
	}); err != nil {
		t.Fatalf("write executor_tools_register: %v", err)
	}
	if err := conn.WriteJSON(map[string]any{"type": "executor_tools_bootstrap_complete", "ok": true}); err != nil {
		t.Fatalf("write executor_tools_bootstrap_complete: %v", err)
	}
	completed := waitForNeoMessageType(t, conn, "executor_connected", 2*time.Second)
	if boolValue(completed["resumeBootstrap"]) {
		t.Fatalf("initial bootstrap completion resumeBootstrap = true: %#v", completed)
	}
	if got := numberFrom(completed["registeredToolCount"]); got != 1 {
		t.Fatalf("registeredToolCount = %d, want 1: %#v", got, completed)
	}

	if err := conn.WriteJSON(connect); err != nil {
		t.Fatalf("write resumed executor_connect: %v", err)
	}
	resumed := waitForNeoMessageType(t, conn, "executor_connected", 2*time.Second)
	if boolValue(resumed["resumeBootstrap"]) {
		t.Fatalf("resumed executor_connect resumeBootstrap = true; expected false because each executor_connect is a fresh headless process: %#v", resumed)
	}
}

func TestNeoRuntimeSnapshotReplaysActiveErrorAndSeqDismissal(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	threadID := "T-019e0e6e-f3f1-7083-b5dd-748f66f8c25d"
	actor, _ := rt.store.upsert(map[string]any{"name": "threadActor", "key": threadID, "input": map[string]any{"threadId": threadID}}, true)
	actor.fail(io.ErrUnexpectedEOF)

	server := httptest.NewServer(http.HandlerFunc(rt.handleHTTP))
	t.Cleanup(server.Close)

	conn := dialNeoActorWebSocket(t, server.URL, threadID)
	defer conn.Close()
	errorSet := waitForNeoMessageType(t, conn, "error_set", 2*time.Second)
	seq := numberFrom(errorSet["seq"])
	if seq == 0 {
		t.Fatalf("error_set missing seq: %#v", errorSet)
	}
	if code := stringValue(mapValue(errorSet["error"])["code"]); code != "INTERNAL_ERROR" {
		t.Fatalf("error_set code = %q, want INTERNAL_ERROR: %#v", code, errorSet)
	}

	actor.clearActiveError(map[string]any{"seq": seq + 1})
	actor.mu.Lock()
	stillActive := len(actor.activeError) > 0
	actor.mu.Unlock()
	if !stillActive {
		t.Fatal("stale active error dismissal unexpectedly cleared the error")
	}

	actor.clearActiveError(map[string]any{"seq": seq})
	cleared := waitForNeoMessageType(t, conn, "error_cleared", 2*time.Second)
	if got := numberFrom(cleared["seq"]); got <= seq {
		t.Fatalf("error_cleared seq = %d, want > %d: %#v", got, seq, cleared)
	}
}

func TestNeoRuntimeResumeReplaysEditAndTruncationEvents(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	threadID := "T-019e0e6e-f3f1-7084-b5dd-748f66f8c25d"
	actor, _ := rt.store.upsert(map[string]any{"name": "threadActor", "key": threadID, "input": map[string]any{"threadId": threadID}}, true)
	actor.mu.Lock()
	actor.messages = []neoMessage{
		{ThreadID: threadID, MessageID: "M-user", Role: "user", Content: []any{map[string]any{"type": "text", "text": "old"}}, Seq: 1},
		{ThreadID: threadID, MessageID: "M-assistant", Role: "assistant", Content: []any{map[string]any{"type": "text", "text": "stale"}}, Seq: 2},
	}
	actor.seq = 3
	actor.rebuildHistoryLocked()
	actor.mu.Unlock()

	actor.editMessage(map[string]any{
		"type":      "client_edit_message",
		"messageId": "M-user",
		"editId":    "edit-1",
		"content":   []any{map[string]any{"type": "text", "text": "edited"}},
	})

	server := httptest.NewServer(http.HandlerFunc(rt.handleHTTP))
	t.Cleanup(server.Close)

	conn := dialNeoActorWebSocket(t, server.URL, threadID)
	defer conn.Close()
	waitForNeoMessageType(t, conn, "agent_state", 2*time.Second)
	if err := conn.WriteJSON(map[string]any{"type": "client_resume", "version": 2}); err != nil {
		t.Fatalf("write client_resume: %v", err)
	}

	updated := waitForNeoMessageType(t, conn, "message_updated", 2*time.Second)
	if got := numberFrom(updated["seq"]); got <= 2 {
		t.Fatalf("message_updated seq = %d, want > 2: %#v", got, updated)
	}
	content := arrayValue(mapValue(updated["message"])["content"])
	if len(content) != 1 || stringValue(mapValue(content[0])["text"]) != "edited" {
		t.Fatalf("message_updated content = %#v", updated)
	}

	truncated := waitForNeoMessageType(t, conn, "thread_truncated", 2*time.Second)
	if got := stringValue(truncated["truncateFromMessage"]); got != "M-assistant" {
		t.Fatalf("truncateFromMessage = %q, want M-assistant: %#v", got, truncated)
	}
	if numberFrom(truncated["seq"]) <= numberFrom(updated["seq"]) {
		t.Fatalf("thread_truncated seq should follow message_updated: update=%#v truncate=%#v", updated, truncated)
	}
}

func TestNeoRuntimeResumeReplaysReadStateUpdate(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	threadID := "T-019e0e6e-f3f1-7085-b5dd-748f66f8c25d"
	actor, _ := rt.store.upsert(map[string]any{"name": "threadActor", "key": threadID, "input": map[string]any{"threadId": threadID}}, true)
	actor.mu.Lock()
	actor.messages = []neoMessage{{ThreadID: threadID, MessageID: "M-user", Role: "user", Content: []any{map[string]any{"type": "text", "text": "read me"}}, Seq: 1}}
	actor.seq = 2
	actor.mu.Unlock()
	actor.markMessageRead("M-user", true)

	server := httptest.NewServer(http.HandlerFunc(rt.handleHTTP))
	t.Cleanup(server.Close)

	conn := dialNeoActorWebSocket(t, server.URL, threadID)
	defer conn.Close()
	waitForNeoMessageType(t, conn, "agent_state", 2*time.Second)
	if err := conn.WriteJSON(map[string]any{"type": "client_resume", "version": 1}); err != nil {
		t.Fatalf("write client_resume: %v", err)
	}

	updated := waitForNeoMessageType(t, conn, "message_updated", 2*time.Second)
	if got := numberFrom(updated["seq"]); got <= 1 {
		t.Fatalf("message_updated seq = %d, want > 1: %#v", got, updated)
	}
	if readAt := stringValue(mapValue(updated["message"])["readAt"]); readAt == "" {
		t.Fatalf("message_updated missing readAt: %#v", updated)
	}
}

func TestNeoRuntimeResumeReplaysCancelledEvent(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	threadID := "T-019e0e6e-f3f1-7086-b5dd-748f66f8c25d"
	actor, _ := rt.store.upsert(map[string]any{"name": "threadActor", "key": threadID, "input": map[string]any{"threadId": threadID}}, true)
	actor.mu.Lock()
	actor.messages = []neoMessage{{ThreadID: threadID, MessageID: "M-assistant", Role: "assistant", Content: []any{map[string]any{"type": "text", "text": "running"}}, Seq: 1}}
	actor.seq = 2
	actor.agentState = "working"
	actor.mu.Unlock()
	actor.cancel()

	server := httptest.NewServer(http.HandlerFunc(rt.handleHTTP))
	t.Cleanup(server.Close)

	conn := dialNeoActorWebSocket(t, server.URL, threadID)
	defer conn.Close()
	waitForNeoMessageType(t, conn, "agent_state", 2*time.Second)
	if err := conn.WriteJSON(map[string]any{"type": "client_resume", "version": 1}); err != nil {
		t.Fatalf("write client_resume: %v", err)
	}

	cancelled := waitForNeoMessageType(t, conn, "cancelled", 2*time.Second)
	if got := stringValue(cancelled["messageId"]); got != "M-assistant" {
		t.Fatalf("cancelled messageId = %q, want M-assistant: %#v", got, cancelled)
	}
	if got := numberFrom(cancelled["seq"]); got <= 1 {
		t.Fatalf("cancelled seq = %d, want > 1: %#v", got, cancelled)
	}
}

func dialNeoActorWebSocket(t *testing.T, serverURL, threadID string) *websocket.Conn {
	t.Helper()
	dialer := websocket.Dialer{Subprotocols: []string{"rivet", "rivet_encoding.4", "rivet_skip_ready_wait"}}
	wsURL := "ws" + strings.TrimPrefix(serverURL, "http") + "/gateway/threadActor/?rvt-method=getOrCreate&rvt-key=" + url.QueryEscape(threadID)
	var conn *websocket.Conn
	var resp *http.Response
	var err error
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
		conn, resp, err = dialer.Dial(wsURL, nil)
		if err == nil {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if err != nil {
		status := 0
		if resp != nil {
			status = resp.StatusCode
		}
		t.Fatalf("gateway websocket dial failed status=%d err=%v", status, err)
	}
	if err := conn.WriteJSON(map[string]any{"type": "client_resume", "version": 0}); err != nil {
		_ = conn.Close()
		t.Fatalf("write initial client_resume: %v", err)
	}
	return conn
}

func dialNeoActorWebSocketWithoutResume(t *testing.T, serverURL, threadID, extraQuery string) *websocket.Conn {
	t.Helper()
	dialer := websocket.Dialer{Subprotocols: []string{"rivet", "rivet_encoding.4", "rivet_skip_ready_wait"}}
	wsURL := "ws" + strings.TrimPrefix(serverURL, "http") + "/gateway/threadActor/?rvt-method=getOrCreate&rvt-key=" + url.QueryEscape(threadID)
	if extraQuery != "" {
		wsURL += "&" + extraQuery
	}
	conn, resp, err := dialer.Dial(wsURL, nil)
	if err != nil {
		status := 0
		if resp != nil {
			status = resp.StatusCode
		}
		t.Fatalf("gateway websocket dial failed status=%d err=%v", status, err)
	}
	return conn
}

func seedNeoLocalExtensionStateForTest(t *testing.T, rt *neoRuntime, threadID string) {
	t.Helper()
	actor, _ := rt.store.upsert(map[string]any{
		"name": "threadActor",
		"key":  threadID,
		"input": map[string]any{
			"threadId": threadID,
		},
	}, true)
	actor.mu.Lock()
	defer actor.mu.Unlock()
	actor.maxTokens = 32000
	actor.mainThreadID = "T-019e1046-656d-7132-879f-390ded941c16"
	actor.draft = []any{map[string]any{"type": "text", "text": "draft text"}}
	actor.pendingNavigation = "T-019e1046-656d-7132-879f-390ded941c16"
	if actor.artifacts == nil {
		actor.artifacts = map[string]any{}
	}
	actor.artifacts["artifact-1"] = map[string]any{
		"key":           "artifact-1",
		"dataType":      "text/plain",
		"contentBase64": "ZGlmZg==",
	}
}

func waitForNeoMessageType(t *testing.T, conn *websocket.Conn, msgType string, timeout time.Duration) map[string]any {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		msg, ok := readNeoMessage(t, conn, time.Until(deadline))
		if !ok {
			break
		}
		if msg["type"] == msgType {
			return msg
		}
	}
	t.Fatalf("timed out waiting for websocket message type %q", msgType)
	return nil
}

func readNeoMessage(t *testing.T, conn *websocket.Conn, timeout time.Duration) (map[string]any, bool) {
	t.Helper()
	if timeout <= 0 {
		timeout = time.Millisecond
	}
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	_, payload, err := conn.ReadMessage()
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return nil, false
		}
		t.Fatalf("read websocket message: %v", err)
	}
	var msg map[string]any
	if err := json.Unmarshal(payload, &msg); err != nil {
		t.Fatalf("websocket JSON error: %v", err)
	}
	return msg, true
}

func waitForNeoActorSyncIdle(t *testing.T, actor *neoActor) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		actor.mu.Lock()
		running := actor.syncRunning
		pending := actor.syncPending
		actor.mu.Unlock()
		if !running && !pending {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for actor sync to become idle")
}

func marshalNeoThreadForTest(t *testing.T, thread map[string]any) map[string]any {
	t.Helper()
	raw, err := json.Marshal(thread)
	if err != nil {
		t.Fatalf("marshal thread: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal thread: %v", err)
	}
	return out
}

func TestNeoActorHandlesThreadStatusCompactionAndRetryEvents(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "threadActor", "", "", neoActorRecord("actor-test", "threadActor", ""), nil)

	actor.handle(map[string]any{"type": "thread_status", "status": "merged"})
	actor.handle(map[string]any{"type": "compaction_started"})
	actor.handle(map[string]any{"type": "compaction_complete", "cutMessageId": "M-00000000000000000000aa", "createdAt": "2026-01-01T00:00:00Z"})
	actor.handle(map[string]any{"type": "retry_scheduled", "retryAt": 123, "attempt": 2, "maxAttempts": 3, "reason": "rate_limit"})

	actor.mu.Lock()
	if actor.threadStatus != "merged" {
		t.Fatalf("threadStatus = %q, want merged", actor.threadStatus)
	}
	if actor.compacting {
		t.Fatal("compacting should be false after compaction_complete")
	}
	if len(actor.compactionRecords) != 1 || stringValue(actor.compactionRecords[0]["cutMessageId"]) != "M-00000000000000000000aa" {
		t.Fatalf("compactionRecords = %#v", actor.compactionRecords)
	}
	if !actor.retryScheduled {
		t.Fatal("retryScheduled = false, want true")
	}
	actor.mu.Unlock()

	actor.handle(map[string]any{"type": "retry_cancelled"})
	actor.mu.Lock()
	defer actor.mu.Unlock()
	if actor.retryScheduled {
		t.Fatal("retryScheduled = true after retry_cancelled")
	}
}

func TestNeoActorClientRetryWaitsForExecutorReady(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "threadActor", "T-test", "T-test", neoActorRecord("actor-test", "threadActor", "T-test"), nil)
	actor.agentState = "idle"
	actor.executorReady = false
	actor.currentAgentMode = "deep"
	actor.currentReasoningEffort = "xhigh"

	actor.handle(map[string]any{"type": "client_retry"})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if !actor.retryScheduled {
		t.Fatal("retryScheduled = false, want true until executor bootstrap completes")
	}
	if len(actor.messages) != 0 || actor.agentState != "idle" {
		t.Fatalf("retry started before executor ready: messages=%#v state=%q", actor.messages, actor.agentState)
	}
}

func TestNeoActorHandlesProtocolLifecycleEvents(t *testing.T) {
	dir := t.TempDir()
	oldStoreDir := neoAmpThreadStoreDir
	neoAmpThreadStoreDir = func() string { return dir }
	t.Cleanup(func() { neoAmpThreadStoreDir = oldStoreDir })

	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "threadActor", "T-test", "T-test", neoActorRecord("actor-test", "threadActor", "T-test"), nil)

	actor.handle(map[string]any{"type": "agent_state", "state": "working", "messageId": "M-assistant", "agentMode": "deep", "reasoningEffort": "xhigh"})
	actor.handle(map[string]any{"type": "inference_tools", "messageId": "M-assistant", "agentMode": "deep", "reasoningEffort": "xhigh", "tools": []any{"shell_command"}})
	actor.handle(map[string]any{"type": "delta", "messageId": "M-assistant", "role": "assistant", "state": "generating", "blockIndex": 0, "blocks": []any{map[string]any{"type": "text", "text": "hel"}}})
	actor.handle(map[string]any{"type": "delta", "messageId": "M-assistant", "role": "assistant", "state": "generating", "blockIndex": 0, "blocks": []any{map[string]any{"type": "text", "text": "lo"}}})
	actor.handle(map[string]any{"type": "delta", "messageId": "M-assistant", "role": "assistant", "state": "tool_use", "blockIndex": 1, "blocks": []any{map[string]any{"type": "tool_use", "id": "TU-shell", "name": "shell_command", "input": map[string]any{"cmd": "pwd"}, "complete": true}}})
	actor.handle(map[string]any{"type": "message_added", "seq": 10, "message": map[string]any{"threadId": "T-test", "messageId": "M-assistant", "role": "assistant", "content": []any{map[string]any{"type": "text", "text": "hello"}, map[string]any{"type": "tool_use", "id": "TU-shell", "name": "shell_command", "input": map[string]any{"cmd": "pwd"}, "complete": true}}, "state": map[string]any{"type": "complete", "stopReason": "tool_use"}}})
	actor.handle(map[string]any{"type": "tool_lease", "toolCallId": "TU-shell", "toolName": "shell_command", "args": map[string]any{"cmd": "pwd"}, "messageId": "M-assistant"})

	actor.mu.Lock()
	if actor.currentInference != nil {
		t.Fatalf("currentInference = %#v, want nil after complete message", actor.currentInference)
	}
	if len(actor.messages) != 1 || actor.messages[0].Seq != 10 {
		t.Fatalf("messages = %#v, want assistant seq 10", actor.messages)
	}
	content := actor.messages[0].Content
	if len(content) != 2 || stringValue(mapValue(content[0])["text"]) != "hello" || stringValue(mapValue(content[1])["id"]) != "TU-shell" {
		t.Fatalf("assistant content = %#v", content)
	}
	if pending := actor.pendingTools["TU-shell"]; pending.Name != "shell_command" || stringValue(pending.Input["cmd"]) != "pwd" {
		t.Fatalf("pending tool = %#v", pending)
	}
	actor.mu.Unlock()

	actor.handle(map[string]any{"type": "thread_truncated", "seq": 11, "truncateFromMessage": "M-assistant"})
	actor.mu.Lock()
	if len(actor.messages) != 0 {
		actor.mu.Unlock()
		t.Fatalf("messages after truncation = %#v, want empty", actor.messages)
	}
	if len(actor.pendingTools) != 0 {
		actor.mu.Unlock()
		t.Fatalf("pending tools after truncation = %#v, want empty", actor.pendingTools)
	}
	if len(actor.replayEvents) == 0 || actor.replayEvents[len(actor.replayEvents)-1].Payload["type"] != "thread_truncated" {
		actor.mu.Unlock()
		t.Fatalf("last replay event = %#v, want thread_truncated", actor.replayEvents)
	}
	actor.mu.Unlock()
	waitForNeoActorSyncIdle(t, actor)
}

func TestNeoActorHandlesExactProtocolStateEvents(t *testing.T) {
	dir := t.TempDir()
	oldStoreDir := neoAmpThreadStoreDir
	neoAmpThreadStoreDir = func() string { return dir }
	t.Cleanup(func() { neoAmpThreadStoreDir = oldStoreDir })

	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "threadActor", "T-test", "T-test", neoActorRecord("actor-test", "threadActor", "T-test"), nil)

	actor.handle(map[string]any{"type": "thread_settings", "settings": map[string]any{"reasoning.effort": "high", "openai.speed": "turbo"}})
	actor.handle(map[string]any{"type": "environment_update", "environment": map[string]any{"workingDirectory": "/tmp/project"}})
	actor.handle(map[string]any{"type": "tool_approval_queue", "approvals": []any{
		map[string]any{"id": "approval-1", "toolCallId": "TU-0000000000000000000001", "toolName": "shell_command", "args": map[string]any{"cmd": "pwd"}, "context": "invalid", "timestamp": float64(1), "ruleSource": "bad"},
	}})
	actor.handle(map[string]any{"type": "queued_messages", "messages": []any{
		map[string]any{"steer": true, "queuedMessage": map[string]any{"role": "user", "messageId": "M-0000000000000000000001", "content": []any{map[string]any{"type": "text", "text": "queued"}}}},
		map[string]any{"queuedMessage": map[string]any{"role": "user", "messageId": "M-0000000000000000000002", "content": []any{map[string]any{"type": "text", "text": "drop missing steer"}}}},
	}})
	actor.handle(map[string]any{"type": "queued_message_added", "seq": 9, "message": map[string]any{"steer": false, "queuedMessage": map[string]any{"role": "user", "messageId": "M-0000000000000000000003", "content": []any{map[string]any{"type": "text", "text": "added"}}}}})
	actor.handle(map[string]any{"type": "queued_message_removed", "queuedMessageId": "M-0000000000000000000001", "seq": 10})
	actor.handle(map[string]any{"type": "artifacts_snapshot", "artifacts": []any{map[string]any{"key": "diff", "dataType": "text/plain", "contentBase64": "ZGlmZg=="}}})
	actor.handle(map[string]any{"type": "artifact_upserted", "artifact": map[string]any{"key": "summary", "content": "done"}})
	actor.handle(map[string]any{"type": "artifact_deleted", "key": "diff"})
	actor.handle(map[string]any{"type": "thread_title", "title": "Exact title"})
	actor.handle(map[string]any{"type": "thread_relationships", "seq": 11, "relationships": []any{
		map[string]any{"threadID": "T-019e65c0-0310-77a8-b233-4b84d9c0612b", "type": "mention", "role": "parent", "createdAt": float64(1), "comment": "related"},
		map[string]any{"threadID": "bad", "type": "mention", "role": "parent", "createdAt": float64(1)},
	}})

	actor.mu.Lock()
	if _, exists := actor.settings["openai.speed"]; exists {
		actor.mu.Unlock()
		t.Fatalf("invalid thread setting was preserved: %#v", actor.settings)
	}
	if actor.settings["reasoning.effort"] != "high" {
		actor.mu.Unlock()
		t.Fatalf("settings = %#v, want reasoning.effort high", actor.settings)
	}
	if got := stringValue(actor.environment["workingDirectory"]); got != "/tmp/project" {
		actor.mu.Unlock()
		t.Fatalf("environment = %#v, want workingDirectory", actor.environment)
	}
	if actor.agentState != "awaiting_approval" || len(actor.approvalQueue) != 1 {
		actor.mu.Unlock()
		t.Fatalf("approval state/queue = %q/%#v, want awaiting_approval with one approval", actor.agentState, actor.approvalQueue)
	}
	approval := actor.approvalQueue[0]
	if approval["context"] != "thread" {
		actor.mu.Unlock()
		t.Fatalf("approval context = %#v, want thread default", approval)
	}
	if _, exists := approval["ruleSource"]; exists {
		actor.mu.Unlock()
		t.Fatalf("invalid ruleSource was preserved: %#v", approval)
	}
	if len(actor.queue) != 1 || actor.queue[0].MessageID != "M-0000000000000000000003" || actor.queue[0].Steer {
		actor.mu.Unlock()
		t.Fatalf("queue = %#v, want only added non-steer message", actor.queue)
	}
	if _, exists := actor.artifacts["diff"]; exists {
		actor.mu.Unlock()
		t.Fatalf("deleted artifact remained: %#v", actor.artifacts)
	}
	if _, exists := actor.artifacts["summary"]; !exists {
		actor.mu.Unlock()
		t.Fatalf("summary artifact missing: %#v", actor.artifacts)
	}
	if actor.title != "Exact title" {
		actor.mu.Unlock()
		t.Fatalf("title = %q, want Exact title", actor.title)
	}
	if len(actor.relationships) != 1 || actor.relationships[0]["threadID"] != "T-019e65c0-0310-77a8-b233-4b84d9c0612b" {
		actor.mu.Unlock()
		t.Fatalf("relationships = %#v, want one normalized relationship", actor.relationships)
	}
	if last := actor.replayEvents[len(actor.replayEvents)-1].Payload; last["type"] != "thread_relationships" || numberFrom(last["seq"]) != 11 {
		actor.mu.Unlock()
		t.Fatalf("last replay event = %#v, want thread_relationships seq 11", last)
	}
	actor.mu.Unlock()
	waitForNeoActorSyncIdle(t, actor)
}

func TestNeoBinaryThreadTruncateBroadcastsProtocolShape(t *testing.T) {
	dir := t.TempDir()
	oldStoreDir := neoAmpThreadStoreDir
	neoAmpThreadStoreDir = func() string { return dir }
	t.Cleanup(func() { neoAmpThreadStoreDir = oldStoreDir })

	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "threadActor", "T-test", "T-test", neoActorRecord("actor-test", "threadActor", "T-test"), nil)
	actor.messages = []neoMessage{
		{ThreadID: "T-test", MessageID: "M-user", Role: "user", Content: []any{map[string]any{"type": "text", "text": "keep"}}, Seq: 1},
		{ThreadID: "T-test", MessageID: "M-assistant", Role: "assistant", Content: []any{map[string]any{"type": "text", "text": "drop"}}, Seq: 2},
	}

	actor.handle(map[string]any{"type": "thread:truncate", "fromIndex": 1, "seq": 7})

	actor.mu.Lock()
	if len(actor.messages) != 1 || actor.messages[0].MessageID != "M-user" {
		actor.mu.Unlock()
		t.Fatalf("messages = %#v, want only first message", actor.messages)
	}
	if len(actor.replayEvents) == 0 {
		actor.mu.Unlock()
		t.Fatal("missing replay event")
	}
	event := actor.replayEvents[len(actor.replayEvents)-1].Payload
	if event["type"] != "thread_truncated" || stringValue(event["truncateFromMessage"]) != "M-assistant" || numberFrom(event["seq"]) != 7 {
		actor.mu.Unlock()
		t.Fatalf("truncate event = %#v, want protocol truncate shape", event)
	}
	if _, exists := event["fromIndex"]; exists {
		actor.mu.Unlock()
		t.Fatalf("truncate event kept binary-only fromIndex: %#v", event)
	}
	actor.mu.Unlock()
	waitForNeoActorSyncIdle(t, actor)
}

func TestNeoAgentStateNormalizesLikeBinary(t *testing.T) {
	if got := normalizeNeoAgentState("finishing"); got != "working" {
		t.Fatalf("finishing normalized to %q, want working", got)
	}
	if got := normalizeNeoAgentState("not-a-state"); got != "working" {
		t.Fatalf("unknown state normalized to %q, want working", got)
	}
	if got := normalizeNeoAgentState(""); got != "working" {
		t.Fatalf("empty state normalized to %q, want working fallback", got)
	}

	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "threadActor", "T-test", "T-test", neoActorRecord("actor-test", "threadActor", "T-test"), nil)
	actor.setAgentState("finishing", "M-assistant", "smart", "")
	actor.mu.Lock()
	defer actor.mu.Unlock()
	if actor.agentState != "working" {
		t.Fatalf("actor state = %q, want working", actor.agentState)
	}
}

func TestNeoActorProtocolAgentStateUsesOfficialPayload(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	threadID := "T-019e0e6e-f3f1-7090-b5dd-748f66f8c25d"
	actor, _ := rt.store.upsert(map[string]any{"name": "threadActor", "key": threadID, "input": map[string]any{"threadId": threadID}}, true)
	messageID := newNeoMessageID()

	server := httptest.NewServer(http.HandlerFunc(rt.handleHTTP))
	t.Cleanup(server.Close)

	conn := dialNeoActorWebSocket(t, server.URL, threadID)
	defer conn.Close()
	waitForNeoMessageType(t, conn, "agent_state", 2*time.Second)

	actor.handle(map[string]any{
		"type":            "agent_state",
		"state":           "finishing",
		"messageId":       messageID,
		"agentMode":       "deep",
		"reasoningEffort": "not-valid",
		"nonSchemaField":  true,
	})
	state := waitForNeoMessageType(t, conn, "agent_state", 2*time.Second)
	if stringValue(state["state"]) != "working" || stringValue(state["messageId"]) != messageID || stringValue(state["agentMode"]) != "deep" {
		t.Fatalf("agent_state payload = %#v", state)
	}
	if _, exists := state["reasoningEffort"]; exists {
		t.Fatalf("agent_state retained invalid reasoningEffort: %#v", state)
	}
	if _, exists := state["nonSchemaField"]; exists {
		t.Fatalf("agent_state retained extra field: %#v", state)
	}
}

func TestNeoActorProtocolInferenceToolsUsesOfficialPayload(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	threadID := "T-019e0e6e-f3f1-7088-b5dd-748f66f8c25d"
	actor, _ := rt.store.upsert(map[string]any{"name": "threadActor", "key": threadID, "input": map[string]any{"threadId": threadID}}, true)
	messageID := newNeoMessageID()
	parentToolCallID := newNeoToolCallID()

	server := httptest.NewServer(http.HandlerFunc(rt.handleHTTP))
	t.Cleanup(server.Close)

	conn := dialNeoActorWebSocket(t, server.URL, threadID)
	defer conn.Close()
	waitForNeoMessageType(t, conn, "agent_state", 2*time.Second)

	actor.handle(map[string]any{
		"type":            "inference_tools",
		"messageId":       messageID,
		"agentMode":       "deep",
		"reasoningEffort": "xhigh",
		"parentToolUseId": parentToolCallID,
		"tools":           []any{"shell_command"},
		"nonSchemaField":  true,
	})
	tools := waitForNeoMessageType(t, conn, "inference_tools", 2*time.Second)
	if stringValue(tools["messageId"]) != messageID || stringValue(tools["agentMode"]) != "deep" || stringValue(tools["parentToolCallId"]) != parentToolCallID {
		t.Fatalf("inference_tools payload = %#v", tools)
	}
	if _, exists := tools["reasoningEffort"]; exists {
		t.Fatalf("inference_tools retained non-schema reasoningEffort: %#v", tools)
	}
	if _, exists := tools["parentToolUseId"]; exists {
		t.Fatalf("inference_tools retained legacy parentToolUseId: %#v", tools)
	}
	if _, exists := tools["nonSchemaField"]; exists {
		t.Fatalf("inference_tools retained extra field: %#v", tools)
	}
}

func TestNeoActorProtocolThreadTruncatedUsesArrayOrderLikeBinary(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "threadActor", "T-test", "T-test", neoActorRecord("actor-test", "threadActor", "T-test"), nil)
	actor.mu.Lock()
	actor.messages = []neoMessage{
		{ThreadID: "T-test", MessageID: "M-late", Role: "assistant", Content: []any{map[string]any{"type": "text", "text": "late"}}, Seq: 3},
		{ThreadID: "T-test", MessageID: "M-00000000000000000000aa", Role: "assistant", Content: []any{map[string]any{"type": "text", "text": "cut"}}, Seq: 2},
		{ThreadID: "T-test", MessageID: "M-early", Role: "user", Content: []any{map[string]any{"type": "text", "text": "early"}}, Seq: 1},
	}
	actor.relationships = []map[string]any{
		{"threadID": "T-019e1046-656d-7132-879f-390ded941c16", "type": "mention", "role": "parent", "createdAt": 1, "messageIndex": 0},
		{"threadID": "T-019e1046-656d-7132-879f-390ded941c17", "type": "mention", "role": "parent", "createdAt": 1, "messageIndex": 1},
	}
	actor.seq = 4
	actor.mu.Unlock()

	actor.handle(map[string]any{"type": "thread_truncated", "seq": 5, "truncateFromMessage": "M-00000000000000000000aa"})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.messages) != 1 || actor.messages[0].MessageID != "M-late" {
		t.Fatalf("messages after protocol truncation = %#v, want current array prefix", actor.messages)
	}
	if len(actor.relationships) != 1 || numberFrom(actor.relationships[0]["messageIndex"]) != 0 {
		t.Fatalf("relationships after truncation = %#v, want only pre-cut relationship", actor.relationships)
	}
}

func TestNeoActorProtocolToolLeaseNormalizesOfficialPayload(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	threadID := "T-019e0e6e-f3f1-7087-b5dd-748f66f8c25d"
	actor, _ := rt.store.upsert(map[string]any{"name": "threadActor", "key": threadID, "input": map[string]any{"threadId": threadID}}, true)
	assistantID := newNeoMessageID()
	toolCallID := newNeoToolCallID()
	actor.mu.Lock()
	actor.messages = []neoMessage{{ThreadID: threadID, MessageID: assistantID, Role: "assistant", Seq: 1, Content: []any{map[string]any{"type": "text", "text": ""}}, State: map[string]any{"type": "streaming"}}}
	actor.currentInference = &neoInferenceInflight{messageID: assistantID, agentMode: "smart"}
	actor.mu.Unlock()

	server := httptest.NewServer(http.HandlerFunc(rt.handleHTTP))
	t.Cleanup(server.Close)

	conn := dialNeoActorWebSocket(t, server.URL, threadID)
	defer conn.Close()
	waitForNeoMessageType(t, conn, "agent_state", 2*time.Second)

	actor.handle(map[string]any{"type": "tool_lease", "toolCallId": toolCallID, "name": "shell_command", "args": map[string]any{"cmd": "pwd"}})
	lease := waitForNeoMessageType(t, conn, "tool_lease", 2*time.Second)
	if got := stringValue(lease["toolName"]); got != "shell_command" {
		t.Fatalf("toolName = %q, want shell_command: %#v", got, lease)
	}
	if got := stringValue(lease["messageId"]); got != assistantID {
		t.Fatalf("messageId = %q, want %s: %#v", got, assistantID, lease)
	}
	if _, exists := lease["name"]; exists {
		t.Fatalf("tool_lease retained non-schema name field: %#v", lease)
	}
}

func TestNeoActorProtocolAbortedDeltaDropsPartialAssistant(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "threadActor", "T-test", "T-test", neoActorRecord("actor-test", "threadActor", "T-test"), nil)

	actor.handle(map[string]any{"type": "inference_tools", "messageId": "M-assistant", "agentMode": "smart", "tools": []any{"Bash"}})
	actor.handle(map[string]any{"type": "delta", "messageId": "M-assistant", "role": "assistant", "state": "generating", "blockIndex": 0, "blocks": []any{map[string]any{"type": "text", "text": "partial"}}})
	actor.handle(map[string]any{"type": "delta", "messageId": "M-assistant", "role": "assistant", "state": "aborted"})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.messages) != 0 {
		t.Fatalf("messages after aborted delta = %#v, want empty", actor.messages)
	}
	if len(actor.history) != 0 {
		t.Fatalf("history after aborted delta = %#v, want empty", actor.history)
	}
	if actor.currentInference != nil {
		t.Fatalf("currentInference = %#v, want nil", actor.currentInference)
	}
}

func TestNeoActorProtocolEmptyDeltasDoNotCreateMessages(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "threadActor", "T-test", "T-test", neoActorRecord("actor-test", "threadActor", "T-test"), nil)

	actor.handle(map[string]any{"type": "delta", "messageId": "M-user", "role": "user", "state": "complete"})
	actor.handle(map[string]any{"type": "delta", "messageId": "M-assistant", "role": "assistant", "state": "generating"})

	actor.mu.Lock()
	if len(actor.messages) != 0 || len(actor.history) != 0 {
		actor.mu.Unlock()
		t.Fatalf("empty non-terminal deltas created state messages=%#v history=%#v", actor.messages, actor.history)
	}
	actor.mu.Unlock()

	actor.handle(map[string]any{"type": "delta", "messageId": "M-assistant", "role": "assistant", "state": "complete"})
	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.messages) != 1 || actor.messages[0].MessageID != "M-assistant" || stringValue(mapValue(actor.messages[0].State)["type"]) != "complete" {
		t.Fatalf("terminal empty assistant delta should create complete message: %#v", actor.messages)
	}
}

func TestNeoActorProtocolDeltaSequencesAndPersistsStreamingAssistant(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "threadActor", "T-test", "T-test", neoActorRecord("actor-test", "threadActor", "T-test"), nil)
	defer waitForNeoActorSyncIdle(t, actor)
	actor.currentInference = &neoInferenceInflight{messageID: "M-assistant", agentMode: "smart"}

	first := map[string]any{"type": "delta", "messageId": "M-assistant", "role": "assistant", "state": "generating", "blockIndex": 0, "blocks": []any{map[string]any{"type": "text", "text": "hel"}}}
	actor.handle(first)

	actor.mu.Lock()
	firstSeq := actor.messages[0].Seq
	if firstSeq <= 0 || numberFrom(first["seq"]) != firstSeq {
		t.Fatalf("first delta seq message=%d payload=%#v", firstSeq, first)
	}
	if textFromBlocks(actor.messages[0].Content) != "hel" || stringValue(mapValue(actor.messages[0].State)["type"]) != "streaming" {
		t.Fatalf("first delta did not persist streaming assistant: %#v", actor.messages)
	}
	foundDelta := false
	for _, event := range actor.replayEvents {
		if event.Seq != firstSeq {
			continue
		}
		switch event.Payload["type"] {
		case "delta":
			if stringValue(event.Payload["messageId"]) == "M-assistant" {
				foundDelta = true
			}
		case "message_updated":
			t.Fatalf("streaming delta should replay as delta only, got message_updated: %#v", event.Payload)
		}
	}
	if !foundDelta {
		t.Fatalf("first delta replay event missing: %#v", actor.replayEvents)
	}
	actor.mu.Unlock()

	actor.handle(map[string]any{"type": "delta", "messageId": "M-assistant", "role": "assistant", "state": "generating", "blockIndex": 0, "blocks": []any{map[string]any{"type": "text", "text": "lo"}}})
	actor.mu.Lock()
	secondSeq := actor.messages[0].Seq
	if secondSeq <= firstSeq || textFromBlocks(actor.messages[0].Content) != "hello" {
		t.Fatalf("second delta seq/content = seq:%d content:%#v", secondSeq, actor.messages[0].Content)
	}
	actor.mu.Unlock()

	actor.handle(map[string]any{
		"type":       "delta",
		"messageId":  "M-assistant",
		"role":       "assistant",
		"state":      "tool_use",
		"blockIndex": 1,
		"blocks": []any{map[string]any{
			"type":             "tool_use",
			"id":               "TU-shell",
			"name":             "shell_command",
			"input":            map[string]any{},
			"complete":         false,
			"inputIncomplete":  map[string]any{},
			"inputPartialJSON": map[string]any{"json": "{\"cmd\""},
			"blockState":       "streaming",
		}},
	})
	actor.handle(map[string]any{
		"type":       "delta",
		"messageId":  "M-assistant",
		"role":       "assistant",
		"state":      "tool_use",
		"blockIndex": 1,
		"blocks": []any{map[string]any{
			"type":                  "tool_use",
			"id":                    "TU-shell",
			"complete":              false,
			"inputPartialJSONDelta": map[string]any{"json": ":\"pwd\"}"},
			"blockState":            "streaming",
		}},
	})

	actor.mu.Lock()
	toolPartialSeq := actor.messages[0].Seq
	toolBlock := mapValue(actor.messages[0].Content[1])
	if toolPartialSeq <= secondSeq || stringValue(mapValue(actor.messages[0].State)["type"]) != "streaming" {
		t.Fatalf("partial tool delta state/seq = seq:%d state:%#v", toolPartialSeq, actor.messages[0].State)
	}
	if stringValue(mapValue(toolBlock["inputPartialJSON"])["json"]) != "{\"cmd\":\"pwd\"}" || stringValue(mapValue(toolBlock["inputIncomplete"])["cmd"]) != "pwd" {
		t.Fatalf("partial tool block = %#v", toolBlock)
	}
	actor.mu.Unlock()

	actor.handle(map[string]any{
		"type":       "delta",
		"messageId":  "M-assistant",
		"role":       "assistant",
		"state":      "tool_use",
		"blockIndex": 1,
		"blocks": []any{map[string]any{
			"type":       "tool_use",
			"id":         "TU-shell",
			"name":       "shell_command",
			"input":      map[string]any{"cmd": "pwd"},
			"complete":   true,
			"blockState": "complete",
		}},
	})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	finalSeq := actor.messages[0].Seq
	finalToolBlock := mapValue(actor.messages[0].Content[1])
	if finalSeq <= toolPartialSeq || stringValue(mapValue(actor.messages[0].State)["stopReason"]) != "tool_use" {
		t.Fatalf("final tool delta state/seq = seq:%d state:%#v", finalSeq, actor.messages[0].State)
	}
	if _, exists := finalToolBlock["inputPartialJSON"]; exists {
		t.Fatalf("complete tool block retained partial json: %#v", finalToolBlock)
	}
	if stringValue(finalToolBlock["blockState"]) != "complete" || !boolValue(finalToolBlock["complete"]) {
		t.Fatalf("complete tool block = %#v", finalToolBlock)
	}
	if actor.currentInference != nil {
		t.Fatalf("currentInference = %#v, want nil after complete tool_use delta", actor.currentInference)
	}
}

func TestNeoActorProtocolDeltaAddsInputIncompleteForPartialToolJSON(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "threadActor", "T-test", "T-test", neoActorRecord("actor-test", "threadActor", "T-test"), nil)
	defer waitForNeoActorSyncIdle(t, actor)

	actor.handle(map[string]any{
		"type":       "delta",
		"messageId":  "M-assistant",
		"role":       "assistant",
		"state":      "tool_use",
		"blockIndex": 0,
		"blocks": []any{map[string]any{
			"type":             "tool_use",
			"id":               "TU-create",
			"name":             "create_file",
			"complete":         false,
			"inputIncomplete":  map[string]any{},
			"inputPartialJSON": map[string]any{"json": `{"path":"/tmp/app.go","content":"hel`},
			"blockState":       "streaming",
		}},
	})

	actor.mu.Lock()
	block := mapValue(actor.messages[0].Content[0])
	incomplete := mapValue(block["inputIncomplete"])
	input := mapValue(block["input"])
	actor.mu.Unlock()
	if stringValue(incomplete["path"]) != "/tmp/app.go" || stringValue(incomplete["content"]) != "hel" {
		t.Fatalf("initial inputIncomplete = %#v", incomplete)
	}
	if stringValue(input["path"]) != "/tmp/app.go" || stringValue(input["content"]) != "hel" {
		t.Fatalf("initial input = %#v", input)
	}

	actor.handle(map[string]any{
		"type":       "delta",
		"messageId":  "M-assistant",
		"role":       "assistant",
		"state":      "tool_use",
		"blockIndex": 0,
		"blocks": []any{map[string]any{
			"type":                  "tool_use",
			"id":                    "TU-create",
			"complete":              false,
			"inputPartialJSONDelta": map[string]any{"json": `lo"}`},
			"blockState":            "streaming",
		}},
	})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	block = mapValue(actor.messages[0].Content[0])
	if stringValue(mapValue(block["inputPartialJSON"])["json"]) != `{"path":"/tmp/app.go","content":"hello"}` {
		t.Fatalf("merged partial json = %#v", block)
	}
	incomplete = mapValue(block["inputIncomplete"])
	if stringValue(incomplete["path"]) != "/tmp/app.go" || stringValue(incomplete["content"]) != "hello" {
		t.Fatalf("merged inputIncomplete = %#v", incomplete)
	}
	input = mapValue(block["input"])
	if stringValue(input["path"]) != "/tmp/app.go" || stringValue(input["content"]) != "hello" {
		t.Fatalf("merged input = %#v", input)
	}
}

func TestParseNeoPartialJSONObjectToleratesStreamingFileEditArgs(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want map[string]string
	}{
		{
			name: "create file content",
			raw:  `{"path":"/tmp/app.go","content":"package main\nfunc ma`,
			want: map[string]string{"path": "/tmp/app.go", "content": "package main\nfunc ma"},
		},
		{
			name: "edit file old and new strings",
			raw:  `{"path":"/tmp/app.go","old_str":"func old() {}\n","new_str":"func ne`,
			want: map[string]string{"path": "/tmp/app.go", "old_str": "func old() {}\n", "new_str": "func ne"},
		},
		{
			name: "apply patch text",
			raw:  `{"patchText":"*** Begin Patch\n*** Update File: app.go\n@@\n-old`,
			want: map[string]string{"patchText": "*** Begin Patch\n*** Update File: app.go\n@@\n-old"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := parseNeoPartialJSONObject(tc.raw)
			for key, want := range tc.want {
				if stringValue(got[key]) != want {
					t.Fatalf("%s = %q, want %q; got %#v", key, stringValue(got[key]), want, got)
				}
			}
		})
	}

	if got := parseNeoPartialJSONObject(`{"cmd"`); len(got) != 0 {
		t.Fatalf("unterminated key parsed as %#v, want empty", got)
	}
}

func TestNeoActorFinishAdvancesSeqAfterStreamedPartial(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "threadActor", "T-test", "T-test", neoActorRecord("actor-test", "threadActor", "T-test"), nil)
	defer waitForNeoActorSyncIdle(t, actor)

	actor.handle(map[string]any{
		"type":       "delta",
		"messageId":  "M-assistant",
		"role":       "assistant",
		"state":      "generating",
		"blockIndex": 0,
		"blocks": []any{map[string]any{
			"type":       "text",
			"text":       "hel",
			"startTime":  1770000000000,
			"blockState": "streaming",
		}},
	})
	actor.mu.Lock()
	partialSeq := actor.messages[0].Seq
	actor.mu.Unlock()

	actor.finishAssistantMessageWithOptions("M-assistant", neoInferenceResult{
		Provider: "openai",
		Model:    "gpt-test",
		Text:     "hello",
		Usage:    map[string]any{},
	}, "smart", "", true, "")

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.messages) != 1 {
		t.Fatalf("messages = %#v", actor.messages)
	}
	finalMessage := actor.messages[0]
	if finalMessage.Seq <= partialSeq {
		t.Fatalf("final seq = %d, want > partial seq %d", finalMessage.Seq, partialSeq)
	}
	if textFromBlocks(finalMessage.Content) != "hello" || stringValue(mapValue(finalMessage.State)["type"]) != "complete" {
		t.Fatalf("final message = %#v", finalMessage)
	}
	block := mapValue(finalMessage.Content[0])
	if stringValue(block["blockState"]) != "complete" || numberFrom(block["startTime"]) != 1770000000000 || numberFrom(block["finalTime"]) <= 0 {
		t.Fatalf("final block timing/state = %#v", block)
	}
}

func TestNeoMessageProtocolUsesOfficialAssistantStateSchema(t *testing.T) {
	message := neoMessage{
		ThreadID:        "T-019e0e6e-f3f1-7089-b5dd-748f66f8c25d",
		MessageID:       newNeoMessageID(),
		Role:            "assistant",
		Content:         []any{map[string]any{"type": "text", "text": "partial"}},
		State:           map[string]any{"type": "streaming"},
		AgentMode:       "deep",
		ReasoningEffort: "xhigh",
	}
	if protocol := message.protocol(); protocol["state"] != nil {
		t.Fatalf("streaming assistant state should be omitted from message protocol: %#v", protocol)
	}
	if protocol := message.protocol(); protocol["agentMode"] != nil || protocol["reasoningEffort"] != nil {
		t.Fatalf("assistant protocol retained non-schema mode fields: %#v", protocol)
	}

	message.State = map[string]any{"type": "complete", "stopReason": "tool_use"}
	state := mapValue(message.protocol()["state"])
	if stringValue(state["type"]) != "complete" {
		t.Fatalf("complete state = %#v", state)
	}
	if _, exists := state["stopReason"]; exists {
		t.Fatalf("complete state retained non-schema stopReason: %#v", state)
	}

	user := neoMessage{
		ThreadID:        message.ThreadID,
		MessageID:       newNeoMessageID(),
		Role:            "user",
		Content:         []any{map[string]any{"type": "text", "text": "hi"}},
		AgentMode:       "deep",
		ReasoningEffort: "xhigh",
		FileMentions:    map[string]any{"files": []any{"main.go"}},
	}
	userProtocol := user.protocol()
	if stringValue(userProtocol["agentMode"]) != "deep" {
		t.Fatalf("user protocol missing agentMode: %#v", userProtocol)
	}
	if _, exists := userProtocol["reasoningEffort"]; exists {
		t.Fatalf("user protocol retained non-schema reasoningEffort: %#v", userProtocol)
	}
	if _, exists := userProtocol["fileMentions"]; exists {
		t.Fatalf("user protocol retained non-schema fileMentions: %#v", userProtocol)
	}

	queued := neoQueuedMessage{
		MessageID:       newNeoMessageID(),
		Content:         []any{map[string]any{"type": "text", "text": "queued"}},
		AgentMode:       "deep",
		ReasoningEffort: "xhigh",
		FileMentions:    map[string]any{"files": []any{"main.go"}},
	}
	queuedProtocol := queued.protocol()
	if queuedProtocol["agentMode"] != nil || queuedProtocol["reasoningEffort"] != nil || queuedProtocol["fileMentions"] != nil {
		t.Fatalf("queued message protocol retained non-schema mode fields: %#v", queuedProtocol)
	}

	info := neoMessage{
		ThreadID:  message.ThreadID,
		MessageID: newNeoMessageID(),
		Role:      "info",
		Content: []any{
			map[string]any{"type": "text", "text": "local-only info text"},
			map[string]any{"type": "summary", "summary": map[string]any{"type": "message", "summary": "compacted"}},
			map[string]any{"type": "manual_bash_invocation", "args": map[string]any{"cmd": "git", "args": []any{"status"}}, "toolRun": map[string]any{"status": "done", "result": "clean"}},
		},
	}
	infoContent := arrayValue(info.protocol()["content"])
	if len(infoContent) != 1 || stringValue(mapValue(infoContent[0])["type"]) != "manual_bash_invocation" {
		t.Fatalf("info protocol content = %#v, want only manual bash invocation", infoContent)
	}
}

func TestNeoToolLeaseRevokedDefaultsInvalidReasonToReassigned(t *testing.T) {
	payload := normalizeNeoToolLeaseRevoked(map[string]any{"toolCallId": newNeoToolCallID(), "reason": "not-valid"})
	if got := stringValue(payload["reason"]); got != "reassigned" {
		t.Fatalf("reason = %q, want reassigned: %#v", got, payload)
	}
}

func TestNeoToolProtocolNormalizersAcceptToolUseAliases(t *testing.T) {
	revoked := normalizeNeoToolLeaseRevoked(map[string]any{"toolUseID": "TU-alias", "reason": "user_canceled"})
	if revoked["toolCallId"] != "TU-alias" || revoked["reason"] != "user_canceled" {
		t.Fatalf("revoked = %#v", revoked)
	}
	ack := normalizeNeoToolResultAck(map[string]any{"toolUseId": "TU-alias"})
	if ack["toolCallId"] != "TU-alias" {
		t.Fatalf("ack = %#v", ack)
	}
	approval := normalizeNeoToolApprovalResponse(map[string]any{"toolUseID": "TU-alias", "accepted": true})
	if approval["toolCallId"] != "TU-alias" || approval["accepted"] != true {
		t.Fatalf("approval = %#v", approval)
	}
}

func TestNeoToolApprovalQueueNormalizesLikeBinary(t *testing.T) {
	payload := toolApprovalQueuePayload([]any{
		"not-an-approval",
		map[string]any{
			"id":         "TU-approval",
			"toolUseId":  "TU-approval",
			"toolName":   "Bash",
			"context":    "workspace",
			"ruleSource": "workspace",
		},
		map[string]any{
			"toolCallId": "TU-valid",
			"toolName":   "Read",
			"context":    "subagent",
			"ruleSource": "user",
		},
	})
	approvals := arrayValue(payload["approvals"])
	if len(approvals) != 2 {
		t.Fatalf("approvals = %#v, want 2 object approvals", approvals)
	}
	first := mapValue(approvals[0])
	if first["toolCallId"] != "TU-approval" || first["context"] != "thread" {
		t.Fatalf("first approval = %#v, want alias plus thread context", first)
	}
	if _, exists := first["toolUseId"]; exists {
		t.Fatalf("first approval leaked toolUseId: %#v", first)
	}
	if _, exists := first["ruleSource"]; exists {
		t.Fatalf("first approval kept invalid ruleSource: %#v", first)
	}
	second := mapValue(approvals[1])
	if second["context"] != "subagent" || second["ruleSource"] != "user" {
		t.Fatalf("second approval = %#v, want valid context/ruleSource preserved", second)
	}
}

func TestNeoActorProtocolToolResultMessageDropsSyntheticProgress(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "threadActor", "T-test", "T-test", neoActorRecord("actor-test", "threadActor", "T-test"), nil)
	actor.pendingTools["TU-shell"] = neoPendingTool{ID: "TU-shell", Name: "Bash", ParentToolCallID: "TU-parent"}

	actor.handleToolProgress(map[string]any{"type": "tool_progress", "toolCallId": "TU-shell", "progress": map[string]any{"type": "snapshot", "value": map[string]any{"phase": "start"}}})
	actor.handle(map[string]any{"type": "message_added", "seq": 5, "message": map[string]any{
		"threadId":  "T-test",
		"messageId": "M-real-tool-result",
		"role":      "user",
		"content": []any{map[string]any{
			"type":      "tool_result",
			"toolUseID": "TU-shell",
			"run":       map[string]any{"status": "done", "result": "workspace"},
		}},
		"parentToolUseId": "TU-parent",
	}})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.messages) != 1 || actor.messages[0].MessageID != "M-real-tool-result" {
		t.Fatalf("messages = %#v, want only real tool result", actor.messages)
	}
	if len(actor.history) != 1 || actor.history[0].ToolCallID != "TU-shell" || actor.history[0].Text != "workspace" || actor.history[0].ParentToolUseID != "TU-parent" {
		t.Fatalf("history = %#v, want one real tool result", actor.history)
	}
}

func TestNeoActorProtocolMessageUpdatedPreservesParentToolUse(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "threadActor", "T-test", "T-test", neoActorRecord("actor-test", "threadActor", "T-test"), nil)
	actor.messages = []neoMessage{{
		ThreadID:        "T-test",
		MessageID:       "M-nested",
		Role:            "assistant",
		ParentToolUseID: "TU-parent",
		Content:         []any{map[string]any{"type": "text", "text": "old"}},
		Seq:             1,
	}}
	actor.rebuildHistoryLocked()

	actor.handle(map[string]any{"type": "message_updated", "seq": 2, "message": map[string]any{
		"threadId":  "T-test",
		"messageId": "M-nested",
		"role":      "assistant",
		"content":   []any{map[string]any{"type": "text", "text": "new"}},
		"state":     map[string]any{"type": "complete"},
	}})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.messages) != 1 || actor.messages[0].ParentToolUseID != "TU-parent" || textFromBlocks(actor.messages[0].Content) != "new" {
		t.Fatalf("updated nested message = %#v", actor.messages)
	}
	if len(actor.history) != 1 || actor.history[0].ParentToolUseID != "TU-parent" || actor.history[0].Text != "new" {
		t.Fatalf("history = %#v", actor.history)
	}
}

func TestNeoActorToolProgressTerminalSnapshotCompletesMessage(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "threadActor", "T-test", "T-test", neoActorRecord("actor-test", "threadActor", "T-test"), nil)
	actor.pendingTools["TU-shell"] = neoPendingTool{ID: "TU-shell", Name: "Bash", ParentToolCallID: "TU-parent"}

	actor.handleToolProgress(map[string]any{"type": "tool_progress", "toolCallId": "TU-shell", "progress": map[string]any{"type": "snapshot", "value": map[string]any{"status": "done", "result": "workspace"}}})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.messages) != 1 {
		t.Fatalf("messages = %#v, want one completed tool result", actor.messages)
	}
	message := actor.messages[0]
	if message.CompletionStatus != "" {
		t.Fatalf("completion status = %q, want empty final result", message.CompletionStatus)
	}
	run := mapValue(mapValue(message.Content[0])["run"])
	if stringValue(run["status"]) != "done" || stringValue(run["result"]) != "workspace" {
		t.Fatalf("run = %#v, want terminal snapshot value", run)
	}
	if len(actor.history) != 1 || actor.history[0].ToolCallID != "TU-shell" || actor.history[0].Text != "workspace" {
		t.Fatalf("history = %#v, want terminal tool result", actor.history)
	}
}

func TestNeoActorProtocolErrorAndCancelClearRuntimeState(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "threadActor", "T-test", "T-test", neoActorRecord("actor-test", "threadActor", "T-test"), nil)
	actor.compacting = true

	actor.handle(map[string]any{"type": "error_set", "seq": 2, "error": map[string]any{"message": "boom", "code": "INTERNAL_ERROR"}})
	actor.mu.Lock()
	if actor.compacting {
		actor.mu.Unlock()
		t.Fatal("compacting should be false after error_set")
	}
	if actor.activeErrorSeq != 2 || stringValue(actor.activeError["message"]) != "boom" {
		actor.mu.Unlock()
		t.Fatalf("active error = seq:%d payload:%#v", actor.activeErrorSeq, actor.activeError)
	}
	actor.retryScheduled = true
	actor.mu.Unlock()

	actor.handle(map[string]any{"type": "cancelled", "seq": 3, "messageId": "M-assistant"})
	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.activeError) != 0 || actor.activeErrorSeq != 0 {
		t.Fatalf("active error after cancel = seq:%d payload:%#v", actor.activeErrorSeq, actor.activeError)
	}
	if actor.retryScheduled || actor.agentState != "idle" {
		t.Fatalf("runtime state after cancel = retry:%v agent:%q", actor.retryScheduled, actor.agentState)
	}
}

func TestNeoActorClientCancelUsesCurrentInferenceMessageID(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "threadActor", "T-test", "T-test", neoActorRecord("actor-test", "threadActor", "T-test"), nil)
	userMessageID := newNeoMessageID()
	assistantMessageID := newNeoMessageID()
	actor.mu.Lock()
	actor.messages = []neoMessage{{ThreadID: "T-test", MessageID: userMessageID, Role: "user", Content: []any{map[string]any{"type": "text", "text": "hi"}}, Seq: 1}}
	actor.currentInference = &neoInferenceInflight{messageID: assistantMessageID, agentMode: "smart"}
	actor.agentState = "working"
	actor.seq = 2
	actor.mu.Unlock()

	actor.cancel()

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if actor.currentInference != nil || actor.agentState != "idle" {
		t.Fatalf("cancel state = inference:%#v agent:%q", actor.currentInference, actor.agentState)
	}
	if len(actor.replayEvents) == 0 {
		t.Fatal("missing cancel replay event")
	}
	event := actor.replayEvents[len(actor.replayEvents)-1].Payload
	if event["type"] != "cancelled" || stringValue(event["messageId"]) != assistantMessageID {
		t.Fatalf("cancel replay event = %#v, want assistant message id %s", event, assistantMessageID)
	}
}

func TestNeoActorClientCancelPreservesEmptyStreamingInferenceMessageID(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "threadActor", "T-test", "T-test", neoActorRecord("actor-test", "threadActor", "T-test"), nil)
	actor.mu.Lock()
	actor.messages = []neoMessage{
		{ThreadID: "T-test", MessageID: "M-user", Role: "user", Content: []any{map[string]any{"type": "text", "text": "hi"}}, Seq: 1},
		{ThreadID: "T-test", MessageID: "M-old-assistant", Role: "assistant", Content: []any{map[string]any{"type": "text", "text": "old"}}, State: map[string]any{"type": "complete"}, Seq: 2},
		{ThreadID: "T-test", MessageID: "M-empty-stream", Role: "assistant", State: map[string]any{"type": "streaming"}, Seq: 3},
	}
	actor.currentInference = &neoInferenceInflight{messageID: "M-empty-stream", agentMode: "smart"}
	actor.agentState = "working"
	actor.rebuildHistoryLocked()
	actor.mu.Unlock()

	actor.cancel()

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if actor.messageIndexLocked("M-empty-stream") >= 0 {
		t.Fatalf("empty streaming assistant was not removed: %#v", actor.messages)
	}
	if len(actor.replayEvents) == 0 {
		t.Fatal("missing cancel replay event")
	}
	event := actor.replayEvents[len(actor.replayEvents)-1].Payload
	if event["type"] != "cancelled" || stringValue(event["messageId"]) != "M-empty-stream" {
		t.Fatalf("cancel replay event = %#v, want removed streaming message id", event)
	}
}

func TestNeoActorClientCancelDoesNotAbortCompletedToolUseWhileToolRuns(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	threadID := "T-019e0e6e-f3f1-7088-b5dd-748f66f8c25d"
	actor, _ := rt.store.upsert(map[string]any{"name": "threadActor", "key": threadID, "input": map[string]any{"threadId": threadID}}, true)
	actor.mu.Lock()
	actor.messages = []neoMessage{
		{ThreadID: threadID, MessageID: "M-user", Role: "user", Content: []any{map[string]any{"type": "text", "text": "run sleep"}}, Seq: 1},
		{ThreadID: threadID, MessageID: "M-assistant", Role: "assistant", Content: []any{map[string]any{"type": "tool_use", "id": "TU-sleep", "name": "Bash", "input": map[string]any{"cmd": "sleep 60"}}}, State: map[string]any{"type": "complete", "stopReason": "tool_use"}, Seq: 2},
		{ThreadID: threadID, MessageID: "M-sleep", Role: "user", Content: []any{map[string]any{"type": "tool_result", "toolUseID": "TU-sleep", "run": map[string]any{"status": "in-progress", "progress": map[string]any{"phase": "running"}}}}, CompletionStatus: "tool_progress", Seq: 3},
	}
	actor.pendingTools["TU-sleep"] = neoPendingTool{ID: "TU-sleep", Name: "Bash", MessageID: "M-assistant"}
	actor.agentState = "running_tools"
	actor.seq = 4
	actor.rebuildHistoryLocked()
	actor.mu.Unlock()

	server := httptest.NewServer(http.HandlerFunc(rt.handleHTTP))
	t.Cleanup(server.Close)
	conn := dialNeoActorWebSocket(t, server.URL, threadID)
	defer conn.Close()
	waitForNeoMessageType(t, conn, "agent_state", 2*time.Second)

	actor.cancel()

	sawCancelled := false
	sawUpdate := false
	sawIdle := false
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !sawIdle {
		msg, ok := readNeoMessage(t, conn, time.Until(deadline))
		if !ok {
			break
		}
		switch msg["type"] {
		case "delta":
			if stringValue(msg["messageId"]) == "M-assistant" && stringValue(msg["state"]) == "aborted" {
				t.Fatalf("cancel aborted completed tool_use assistant while tool was running: %#v", msg)
			}
		case "message_updated":
			message := mapValue(msg["message"])
			if stringValue(message["messageId"]) == "M-sleep" {
				run := mapValue(mapValue(arrayValue(message["content"])[0])["run"])
				if stringValue(run["status"]) == "cancelled" && stringValue(run["reason"]) == "user:cancelled" {
					sawUpdate = true
				}
			}
		case "cancelled":
			sawCancelled = true
		case "agent_state":
			if stringValue(msg["state"]) == "idle" && sawCancelled && sawUpdate {
				sawIdle = true
			}
		}
	}
	if !sawCancelled || !sawUpdate || !sawIdle {
		t.Fatalf("cancel events missing: cancelled=%v update=%v idle=%v", sawCancelled, sawUpdate, sawIdle)
	}
}

func TestNeoActorClientCancelCleansIncompleteAssistantLikeBinary(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "threadActor", "T-test", "T-test", neoActorRecord("actor-test", "threadActor", "T-test"), nil)
	actor.messages = []neoMessage{
		{
			ThreadID:  "T-test",
			MessageID: "M-user",
			Role:      "user",
			Content:   []any{map[string]any{"type": "text", "text": "run pwd"}},
			Seq:       1,
		},
		{
			ThreadID:  "T-test",
			MessageID: "M-assistant",
			Role:      "assistant",
			Content: []any{map[string]any{
				"type":             "tool_use",
				"id":               "TU-incomplete",
				"name":             "Bash",
				"input":            map[string]any{},
				"inputPartialJSON": map[string]any{"json": `{"cmd":"pwd"`},
				"inputIncomplete":  map[string]any{"cmd": "pwd"},
				"complete":         false,
			}},
			State: map[string]any{"type": "streaming"},
			Seq:   2,
		},
	}
	actor.currentInference = &neoInferenceInflight{messageID: "M-assistant", agentMode: "smart"}
	actor.pendingTools["TU-incomplete"] = neoPendingTool{ID: "TU-incomplete", Name: "Bash", MessageID: "M-assistant"}
	actor.agentState = "running_tools"
	actor.rebuildHistoryLocked()

	actor.cancel()

	actor.mu.Lock()
	defer actor.mu.Unlock()
	assistant := actor.messages[1]
	if stringValue(mapValue(assistant.State)["type"]) != "cancelled" {
		t.Fatalf("assistant state = %#v, want cancelled", assistant.State)
	}
	result := mapValue(actor.messages[2].Content[0])
	run := mapValue(result["run"])
	if firstNonEmptyString(result["toolUseID"], result["toolUseId"]) != "TU-incomplete" || stringValue(run["status"]) != "cancelled" || stringValue(run["reason"]) != "user:cancelled" {
		t.Fatalf("tool result = %#v", result)
	}
	if len(actor.pendingTools) != 0 || actor.currentInference != nil || actor.agentState != "idle" {
		t.Fatalf("cancel state = pending:%#v inference:%#v agent:%q", actor.pendingTools, actor.currentInference, actor.agentState)
	}
}

func TestNeoActorSpawnExecutorStartsHeadlessAmp(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fake Amp executor script uses /bin/sh")
	}

	dir := t.TempDir()
	t.Setenv("HOME", dir)
	logPath := filepath.Join(dir, "spawn.log")
	workDir := filepath.Join(dir, "workspace")
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		t.Fatalf("MkdirAll workDir error: %v", err)
	}
	realWorkDir, err := filepath.EvalSymlinks(workDir)
	if err != nil {
		t.Fatalf("EvalSymlinks workDir error: %v", err)
	}

	script := filepath.Join(dir, "amp")
	if err := os.WriteFile(script, []byte(`#!/bin/sh
{
  printf 'args:'
  for arg in "$@"; do printf ' [%s]' "$arg"; done
  printf '\nAMP_URL=%s\nAMP_API_KEY=%s\nAMP_EXECUTOR=%s\nAMP_PWD=%s\nAMP_THREAD_ID=%s\nRIVET_ENDPOINT=%s\nRIVET_PUBLIC_ENDPOINT=%s\n' "$AMP_URL" "$AMP_API_KEY" "$AMP_EXECUTOR" "$AMP_PWD" "$AMP_THREAD_ID" "$RIVET_ENDPOINT" "$RIVET_PUBLIC_ENDPOINT"
} > "$TEST_SPAWN_LOG"
sleep 5
`), 0o755); err != nil {
		t.Fatalf("WriteFile script error: %v", err)
	}
	t.Setenv("TEST_SPAWN_LOG", logPath)

	rt := newNeoRuntime(&config.Config{
		SDKConfig: config.SDKConfig{APIKeys: []string{"local-key"}},
		Host:      "127.0.0.1",
		Port:      8317,
		AmpCode: config.AmpCode{
			NeoLocalRuntime: config.AmpNeoLocalRuntime{ExecutorCommand: script},
		},
	})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test-thread", "T-test-thread", neoActorRecord("actor-test", "thread-actor", "T-test-thread"), nil)
	actor.environment = map[string]any{"workingDirectory": workDir}
	actor.settings = map[string]any{"agentMode": "deep", "reasoning.effort": "xhigh"}
	t.Cleanup(actor.dispose)

	actor.handle(map[string]any{"type": "client_spawn_executor", "requestId": "spawn-test"})

	var content string
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(logPath)
		if err == nil && len(raw) > 0 {
			content = string(raw)
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if content == "" {
		t.Fatal("spawn script did not run")
	}
	for _, want := range []string{
		"args: [--mode] [deep] [--effort] [xhigh] [--headless] [T-test-thread]",
		"AMP_URL=http://127.0.0.1:8317",
		"AMP_API_KEY=local-key",
		"AMP_EXECUTOR=1",
		"AMP_PWD=" + realWorkDir,
		"AMP_THREAD_ID=T-test-thread",
		"RIVET_ENDPOINT=http://127.0.0.1:6420",
		"RIVET_PUBLIC_ENDPOINT=http://127.0.0.1:6420",
	} {
		if !strings.Contains(content, want) {
			t.Fatalf("spawn log missing %q:\n%s", want, content)
		}
	}

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.spawnedExecutors) != 1 {
		t.Fatalf("spawnedExecutors len = %d, want 1", len(actor.spawnedExecutors))
	}
}

func TestNeoAmpBinaryHeadlessBootstrapSmoke(t *testing.T) {
	command := strings.TrimSpace(os.Getenv("AMP_BINARY_E2E"))
	if command == "" {
		t.Skip("set AMP_BINARY_E2E to an amp binary path, or 1 to use ~/.amp/bin/amp")
	}
	if command == "1" {
		home, err := os.UserHomeDir()
		if err != nil || home == "" {
			t.Fatalf("UserHomeDir error: %v", err)
		}
		command = filepath.Join(home, ".amp", "bin", "amp")
	}
	if info, err := os.Stat(command); err != nil || info.IsDir() {
		t.Fatalf("amp binary %q is not executable: %v", command, err)
	}

	rt := newNeoRuntime(&config.Config{SDKConfig: config.SDKConfig{APIKeys: []string{"local-key"}}})
	recorder := installNeoInboundRecorderForTest(t)
	threadID := "T-019e1cd4-bde0-778f-84f9-61259c2603d6"
	storeDir := t.TempDir()
	oldStoreDir := neoAmpThreadStoreDir
	neoAmpThreadStoreDir = func() string { return storeDir }
	t.Cleanup(func() { neoAmpThreadStoreDir = oldStoreDir })
	actor := rt.store.ensureThreadActor(threadID)
	actor.updateSettings(map[string]any{"agentMode": "rush"})
	testHome := t.TempDir()
	workDir := t.TempDir()
	settingsPath := filepath.Join(testHome, "settings.json")
	if err := os.WriteFile(settingsPath, []byte(`{"amp.url":"http://127.0.0.1:1","amp.permissions":[],"amp.mcpServers":{},"amp.skills.path":""}`), 0o600); err != nil {
		t.Fatalf("write settings file: %v", err)
	}
	logPath := filepath.Join(testHome, "amp-headless.log")
	var (
		requestsMu    sync.Mutex
		requests      []string
		providerMu    sync.Mutex
		providerCalls int
	)
	nextProviderCall := func() int {
		providerMu.Lock()
		defer providerMu.Unlock()
		providerCalls++
		return providerCalls
	}
	writeOpenAIResponsesSmokeStream := func(w http.ResponseWriter, callIndex int) {
		w.Header().Set("Content-Type", "text/event-stream")
		chunks := []string{
			`{"type":"response.created","response":{"id":"resp_binary_tool","status":"in_progress","model":"gpt-5.5","output":[]}}`,
			`{"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","id":"fc_binary_pwd","call_id":"call_binary_pwd","name":"Bash","arguments":"","status":"in_progress"}}`,
			`{"type":"response.function_call_arguments.delta","output_index":0,"delta":"{\"cmd\":\"pwd\"}"}`,
			`{"type":"response.function_call_arguments.done","output_index":0,"arguments":"{\"cmd\":\"pwd\"}"}`,
			`{"type":"response.output_item.done","output_index":0,"item":{"type":"function_call","id":"fc_binary_pwd","call_id":"call_binary_pwd","name":"Bash","arguments":"{\"cmd\":\"pwd\"}","status":"completed"}}`,
			`{"type":"response.completed","response":{"id":"resp_binary_tool","status":"completed","usage":{"input_tokens":3,"output_tokens":1},"output":[{"type":"function_call","id":"fc_binary_pwd","call_id":"call_binary_pwd","name":"Bash","arguments":"{\"cmd\":\"pwd\"}","status":"completed"}]}}`,
		}
		if callIndex > 1 {
			chunks = []string{
				`{"type":"response.created","response":{"id":"resp_binary_text","status":"in_progress","model":"gpt-5.5","output":[]}}`,
				`{"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"binary cycle ok"}`,
				`{"type":"response.output_text.done","output_index":0,"content_index":0,"text":"binary cycle ok"}`,
				`{"type":"response.completed","response":{"id":"resp_binary_text","status":"completed","usage":{"input_tokens":3,"output_tokens":3},"output":[{"type":"message","id":"msg_binary_text","status":"completed","role":"assistant","content":[{"type":"output_text","text":"binary cycle ok"}]}]}}`,
			}
		}
		for _, chunk := range chunks {
			_, _ = w.Write([]byte("data: " + chunk + "\n\n"))
		}
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}
	writeOpenAIChatSmokeStream := func(w http.ResponseWriter, callIndex int) {
		w.Header().Set("Content-Type", "text/event-stream")
		if callIndex == 1 {
			_, _ = w.Write([]byte(`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_binary_pwd","type":"function","function":{"name":"Bash","arguments":"{\"cmd\":\"pwd\"}"}}]}}],"usage":{"prompt_tokens":3,"completion_tokens":1}}` + "\n\n"))
			_, _ = w.Write([]byte("data: [DONE]\n\n"))
			return
		}
		_, _ = w.Write([]byte(`data: {"choices":[{"delta":{"content":"binary cycle ok"}}],"usage":{"prompt_tokens":3,"completion_tokens":3}}` + "\n\n"))
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestsMu.Lock()
		requests = append(requests, r.Method+" "+r.URL.RequestURI())
		requestsMu.Unlock()
		if r.URL.Path == "/api/provider/openai/v1/responses" && r.Method == http.MethodPost {
			callIndex := nextProviderCall()
			payload := readNeoJSON(r.Body)
			if payload["stream"] != true {
				t.Fatalf("binary smoke provider stream = %#v, want true", payload["stream"])
			}
			writeOpenAIResponsesSmokeStream(w, callIndex)
			return
		}
		if r.URL.Path == "/api/provider/openai/v1/chat/completions" && r.Method == http.MethodPost {
			callIndex := nextProviderCall()
			payload := readNeoJSON(r.Body)
			if payload["stream"] != true {
				t.Fatalf("binary smoke chat stream = %#v, want true", payload["stream"])
			}
			writeOpenAIChatSmokeStream(w, callIndex)
			return
		}
		if r.URL.Path == "/api/provider/anthropic/v1/messages" && r.Method == http.MethodPost {
			callIndex := nextProviderCall()
			payload := readNeoJSON(r.Body)
			if payload["stream"] != true {
				t.Fatalf("binary smoke provider stream = %#v, want true", payload["stream"])
			}
			w.Header().Set("Content-Type", "text/event-stream")
			if callIndex == 1 {
				_, _ = w.Write([]byte("event: message_start\n" +
					`data: {"type":"message_start","message":{"usage":{"input_tokens":3}}}` + "\n\n" +
					"event: content_block_start\n" +
					`data: {"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_binary_pwd","name":"Bash","input":{}}}` + "\n\n" +
					"event: content_block_delta\n" +
					`data: {"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"cmd\":\"pwd\"}"}}` + "\n\n" +
					"event: message_stop\n" +
					`data: {"type":"message_stop"}` + "\n\n"))
				return
			}
			_, _ = w.Write([]byte("event: message_start\n" +
				`data: {"type":"message_start","message":{"usage":{"input_tokens":3}}}` + "\n\n" +
				"event: content_block_start\n" +
				`data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}` + "\n\n" +
				"event: content_block_delta\n" +
				`data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"binary cycle ok"}}` + "\n\n" +
				"event: message_stop\n" +
				`data: {"type":"message_stop"}` + "\n\n"))
			return
		}
		if r.URL.Path == "/api/thread-actors" && r.Method == http.MethodPost {
			response, status := rt.localThreadActorManagementResponse(r.Context(), readNeoJSON(r.Body), "")
			writeNeoJSON(w, status, response)
			return
		}
		if r.URL.Path == "/api/internal" && r.URL.RawQuery == "loadPlugins" {
			writeNeoJSON(w, http.StatusOK, map[string]any{"ok": true, "result": []any{}})
			return
		}
		if r.URL.Path == "/api/internal" && r.URL.RawQuery == "getUserInfo" {
			writeNeoJSON(w, http.StatusOK, map[string]any{
				"ok": true,
				"result": map[string]any{
					"id":          "U-local-test",
					"email":       "local@example.com",
					"features":    []any{},
					"name":        "Local Test",
					"team":        map[string]any{"id": "W-local-test", "name": "Local Workspace"},
					"username":    "local-test",
					"workspaceID": "W-local-test",
					"workspaceId": "W-local-test",
				},
			})
			return
		}
		rt.handleHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	parsedServerURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("parse server URL: %v", err)
	}
	serverHost, serverPortText, err := net.SplitHostPort(parsedServerURL.Host)
	if err != nil {
		t.Fatalf("split server URL host: %v", err)
	}
	serverPort, err := strconv.Atoi(serverPortText)
	if err != nil {
		t.Fatalf("parse server URL port: %v", err)
	}
	if err := rt.updateConfig(&config.Config{SDKConfig: config.SDKConfig{APIKeys: []string{"local-key"}}, Host: serverHost, Port: serverPort}); err != nil {
		t.Fatalf("update runtime config: %v", err)
	}

	cmd := exec.Command(command, "--mode", "rush", "--headless", threadID)
	cmd.Dir = workDir
	var cmdOutput bytes.Buffer
	cmd.Stdout = &cmdOutput
	cmd.Stderr = &cmdOutput
	ampEnv := appendEnvOverrides(os.Environ(), map[string]string{
		"AMP_API_KEY":           "local-key",
		"AMP_CURRENT_THREAD_ID": threadID,
		"AMP_EXECUTOR":          "1",
		"AMP_GATEWAY_URL":       server.URL,
		"AMP_HEADLESS_OAUTH":    "1",
		"AMP_LOG_FILE":          logPath,
		"AMP_LOG_LEVEL":         "debug",
		"AMP_RUNTIME_URL":       server.URL,
		"AMP_SETTINGS_FILE":     settingsPath,
		"AMP_SKIP_UPDATE_CHECK": "1",
		"AMP_THREAD_ID":         threadID,
		"AMP_URL":               server.URL,
		"HOME":                  testHome,
		"RIVET_ENDPOINT":        server.URL,
		"RIVET_GATEWAY_URL":     server.URL,
		"RIVET_PUBLIC_ENDPOINT": server.URL,
		"RIVET_THREAD_ID":       threadID,
		"RIVETKIT_ENGINE_URL":   server.URL,
	})
	cmd.Env = ampEnv
	if err := cmd.Start(); err != nil {
		t.Fatalf("start amp headless: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
	})

	bootstrapped := false
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-done:
			t.Fatalf("amp headless exited before bootstrap: %v\n%s%s%s%s", err, cmdOutput.String(), readFileForTest(logPath), requestLogForTest(&requestsMu, requests), recorder.report())
		default:
		}
		actor.mu.Lock()
		ready := actor.executorReady
		bootstrapComplete := actor.executorBootstrapComplete
		mode := actor.currentAgentMode
		actor.mu.Unlock()
		if ready && bootstrapComplete {
			if mode != "rush" {
				t.Fatalf("agent mode after amp binary bootstrap = %q, want rush", mode)
			}
			bootstrapped = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !bootstrapped {
		actor.mu.Lock()
		defer actor.mu.Unlock()
		t.Fatalf("amp binary did not bootstrap executor: ready=%v bootstrap=%v executorID=%q activeError=%#v\n%s%s%s%s", actor.executorReady, actor.executorBootstrapComplete, actor.executorID, actor.activeError, cmdOutput.String(), readFileForTest(logPath), requestLogForTest(&requestsMu, requests), recorder.report())
	}

	client := dialNeoActorWebSocket(t, server.URL, threadID)
	defer client.Close()
	if err := client.WriteJSON(map[string]any{
		"type":      "client_append_user_msg",
		"messageId": newNeoMessageID(),
		"agentMode": "rush",
		"content":   []any{map[string]any{"type": "text", "text": "exercise binary executor cycle"}},
	}); err != nil {
		t.Fatalf("write binary smoke user message: %v", err)
	}
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-done:
			t.Fatalf("amp headless exited during cycle: %v\n%s%s%s%s", err, cmdOutput.String(), readFileForTest(logPath), requestLogForTest(&requestsMu, requests), recorder.report())
		default:
		}
		msg, ok := readNeoMessage(t, client, time.Until(deadline))
		if !ok {
			break
		}
		if msg["type"] != "message_added" {
			continue
		}
		message := mapValue(msg["message"])
		if stringValue(message["role"]) != "assistant" {
			continue
		}
		if textFromBlocks(arrayValue(message["content"])) != "binary cycle ok" {
			if calls := len(arrayValue(message["content"])); calls > 0 {
				continue
			}
			t.Fatalf("binary smoke assistant message = %#v", message)
		}
		providerMu.Lock()
		calls := providerCalls
		providerMu.Unlock()
		if calls != 2 {
			t.Fatalf("providerCalls = %d, want 2", calls)
		}
		actor.mu.Lock()
		firstExecutorID := actor.executorID
		actor.mu.Unlock()
		if firstExecutorID == "" {
			t.Fatal("first executor ID is empty")
		}
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for first amp headless process to exit")
		}
		deadline = time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			actor.mu.Lock()
			ready := actor.executorReady
			actor.mu.Unlock()
			if !ready {
				break
			}
			time.Sleep(25 * time.Millisecond)
		}

		cmd = exec.Command(command, "--mode", "rush", "--headless", threadID)
		cmd.Dir = workDir
		cmd.Stdout = &cmdOutput
		cmd.Stderr = &cmdOutput
		cmd.Env = ampEnv
		if err := cmd.Start(); err != nil {
			t.Fatalf("restart amp headless: %v", err)
		}
		done = make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		deadline = time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			select {
			case err := <-done:
				t.Fatalf("restarted amp headless exited before bootstrap: %v\n%s%s%s%s", err, cmdOutput.String(), readFileForTest(logPath), requestLogForTest(&requestsMu, requests), recorder.report())
			default:
			}
			actor.mu.Lock()
			ready := actor.executorReady
			bootstrapComplete := actor.executorBootstrapComplete
			executorID := actor.executorID
			actor.mu.Unlock()
			if ready && bootstrapComplete && executorID != "" && executorID != firstExecutorID {
				if unknown := recorder.unknownTypes(); len(unknown) > 0 {
					t.Fatalf("amp binary sent unhandled inbound message types: %v%s", unknown, recorder.report())
				}
				t.Log(recorder.report())
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		actor.mu.Lock()
		defer actor.mu.Unlock()
		if actor.executorID == firstExecutorID {
			t.Fatalf("restarted executor reused executor ID %q", actor.executorID)
		}
		t.Fatalf("restarted amp binary did not bootstrap: ready=%v bootstrap=%v executorID=%q activeError=%#v\n%s%s%s%s", actor.executorReady, actor.executorBootstrapComplete, actor.executorID, actor.activeError, cmdOutput.String(), readFileForTest(logPath), requestLogForTest(&requestsMu, requests), recorder.report())
		return
	}
	t.Fatalf("amp binary did not complete user-message cycle\n%s%s%s%s", cmdOutput.String(), readFileForTest(logPath), requestLogForTest(&requestsMu, requests), recorder.report())
}

func TestNeoAmpBinarySpawnPathBootstrapSmoke(t *testing.T) {
	command := strings.TrimSpace(os.Getenv("AMP_BINARY_E2E"))
	if command == "" {
		t.Skip("set AMP_BINARY_E2E to an amp binary path, or 1 to use ~/.amp/bin/amp")
	}
	if command == "1" {
		home, err := os.UserHomeDir()
		if err != nil || home == "" {
			t.Fatalf("UserHomeDir error: %v", err)
		}
		command = filepath.Join(home, ".amp", "bin", "amp")
	}
	if info, err := os.Stat(command); err != nil || info.IsDir() {
		t.Fatalf("amp binary %q is not executable: %v", command, err)
	}

	runtimePort := freeTCPPortForTest(t)
	testHome := t.TempDir()
	workDir := t.TempDir()
	settingsPath := filepath.Join(testHome, "settings.json")
	if err := os.WriteFile(settingsPath, []byte(`{"amp.url":"http://127.0.0.1:1","amp.permissions":[],"amp.mcpServers":{},"amp.skills.path":""}`), 0o600); err != nil {
		t.Fatalf("write settings file: %v", err)
	}
	t.Setenv("HOME", testHome)
	t.Setenv("AMP_SETTINGS_FILE", settingsPath)
	t.Setenv("AMP_LOG_LEVEL", "debug")

	threadID := "T-019e1cd4-bde0-778f-84f9-61259c2604d7"
	storeDir := t.TempDir()
	oldStoreDir := neoAmpThreadStoreDir
	neoAmpThreadStoreDir = func() string { return storeDir }
	t.Cleanup(func() { neoAmpThreadStoreDir = oldStoreDir })

	rt := newNeoRuntime(&config.Config{
		SDKConfig: config.SDKConfig{APIKeys: []string{"local-key"}},
		AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{
			Host:            "127.0.0.1",
			Port:            runtimePort,
			ExecutorCommand: command,
		}},
	})
	recorder := installNeoInboundRecorderForTest(t)
	actor := rt.store.ensureThreadActor(threadID)
	actor.updateSettings(map[string]any{"agentMode": "rush"})
	actor.updateEnvironment(map[string]any{"workingDirectory": workDir})

	var (
		requestsMu sync.Mutex
		requests   []string
	)
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestsMu.Lock()
		requests = append(requests, r.Method+" "+r.URL.RequestURI())
		requestsMu.Unlock()
		if r.URL.Path == "/api/thread-actors" && r.Method == http.MethodPost {
			response, status := rt.localThreadActorManagementResponse(r.Context(), readNeoJSON(r.Body), "")
			writeNeoJSON(w, status, response)
			return
		}
		if r.URL.Path == "/api/internal" && r.URL.RawQuery == "loadPlugins" {
			writeNeoJSON(w, http.StatusOK, map[string]any{"ok": true, "result": []any{}})
			return
		}
		if r.URL.Path == "/api/internal" && r.URL.RawQuery == "getUserInfo" {
			writeNeoJSON(w, http.StatusOK, map[string]any{"ok": true, "result": map[string]any{"id": "U-local-test", "email": "local@example.com", "workspaceID": "W-local-test"}})
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(proxy.Close)
	parsedProxyURL, err := url.Parse(proxy.URL)
	if err != nil {
		t.Fatalf("parse proxy URL: %v", err)
	}
	proxyHost, proxyPortText, err := net.SplitHostPort(parsedProxyURL.Host)
	if err != nil {
		t.Fatalf("split proxy URL host: %v", err)
	}
	proxyPort, err := strconv.Atoi(proxyPortText)
	if err != nil {
		t.Fatalf("parse proxy URL port: %v", err)
	}
	if err := rt.updateConfig(&config.Config{
		SDKConfig: config.SDKConfig{APIKeys: []string{"local-key"}},
		Host:      proxyHost,
		Port:      proxyPort,
		AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{
			Host:            "127.0.0.1",
			Port:            runtimePort,
			ExecutorCommand: command,
		}},
	}); err != nil {
		t.Fatalf("update runtime config: %v", err)
	}
	if err := rt.start(); err != nil {
		t.Fatalf("start runtime: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := rt.stop(ctx); err != nil {
			t.Fatalf("stop runtime: %v", err)
		}
	})

	runtimeURL := fmt.Sprintf("http://127.0.0.1:%d", runtimePort)
	var client *websocket.Conn
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
		dialer := websocket.Dialer{Subprotocols: []string{"rivet", "rivet_encoding.4", "rivet_skip_ready_wait"}}
		conn, _, err := dialer.Dial("ws"+strings.TrimPrefix(runtimeURL, "http")+"/gateway/threadActor/?rvt-method=getOrCreate&rvt-key="+url.QueryEscape(threadID), nil)
		if err == nil {
			client = conn
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if client == nil {
		client = dialNeoActorWebSocket(t, runtimeURL, threadID)
	}
	defer client.Close()
	if err := client.WriteJSON(map[string]any{"type": "client_spawn_executor", "requestId": "spawn-binary"}); err != nil {
		t.Fatalf("write client_spawn_executor: %v", err)
	}

	deadline := time.Now().Add(10 * time.Second)
	_ = client.SetReadDeadline(deadline)
	for time.Now().Before(deadline) {
		_, payload, err := client.ReadMessage()
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				break
			}
			t.Fatalf("read spawn-path websocket: %v", err)
		}
		var msg map[string]any
		if err := json.Unmarshal(payload, &msg); err != nil {
			t.Fatalf("spawn-path websocket JSON error: %v", err)
		}
		if msg["type"] == "executor_connected" && numberFrom(msg["registeredToolCount"]) > 0 {
			if executorID := stringValue(msg["executorId"]); executorID == "" {
				t.Fatalf("spawn-path executor_connected missing executorId: %#v", msg)
			}
			actor.mu.Lock()
			workingDirectory := firstNonEmptyString(actor.environment["workingDirectory"], nestedString(actor.environment["workingDirectory"], "path"), nestedString(actor.environment["workingDirectory"], "uri"))
			actor.mu.Unlock()
			if !strings.Contains(workingDirectory, filepath.Base(workDir)) {
				t.Fatalf("spawn-path workingDirectory = %q, want temp workdir %q", workingDirectory, workDir)
			}
			if unknown := recorder.unknownTypes(); len(unknown) > 0 {
				t.Fatalf("spawned amp binary sent unhandled inbound message types: %v%s", unknown, recorder.report())
			}
			t.Log(recorder.report())
			return
		}
		actor.mu.Lock()
		ready := actor.executorReady
		bootstrapComplete := actor.executorBootstrapComplete
		mode := actor.currentAgentMode
		executorID := actor.executorID
		actor.mu.Unlock()
		if ready && bootstrapComplete {
			if mode != "rush" {
				t.Fatalf("spawn-path mode = %q, want rush", mode)
			}
			if executorID == "" {
				t.Fatal("spawn-path executor ID is empty after bootstrap")
			}
			if unknown := recorder.unknownTypes(); len(unknown) > 0 {
				t.Fatalf("spawned amp binary sent unhandled inbound message types: %v%s", unknown, recorder.report())
			}
			t.Log(recorder.report())
			return
		}
	}
	actor.mu.Lock()
	defer actor.mu.Unlock()
	t.Fatalf("spawned amp binary did not bootstrap: ready=%v bootstrap=%v executorID=%q activeError=%#v\n%s%s%s", actor.executorReady, actor.executorBootstrapComplete, actor.executorID, actor.activeError, requestLogForTest(&requestsMu, requests), readSpawnLogsForTest(testHome), recorder.report())
}

func TestNeoAmpBinarySpawnedExecutorReconnectsAfterRuntimeRestart(t *testing.T) {
	command := strings.TrimSpace(os.Getenv("AMP_BINARY_E2E"))
	if command == "" {
		t.Skip("set AMP_BINARY_E2E to an amp binary path, or 1 to use ~/.amp/bin/amp")
	}
	if command == "1" {
		home, err := os.UserHomeDir()
		if err != nil || home == "" {
			t.Fatalf("UserHomeDir error: %v", err)
		}
		command = filepath.Join(home, ".amp", "bin", "amp")
	}
	if info, err := os.Stat(command); err != nil || info.IsDir() {
		t.Fatalf("amp binary %q is not executable: %v", command, err)
	}

	runtimePort := freeTCPPortForTest(t)
	testHome := t.TempDir()
	workDir := t.TempDir()
	settingsPath := filepath.Join(testHome, "settings.json")
	if err := os.WriteFile(settingsPath, []byte(`{"amp.url":"http://127.0.0.1:1","amp.permissions":[],"amp.mcpServers":{},"amp.skills.path":""}`), 0o600); err != nil {
		t.Fatalf("write settings file: %v", err)
	}
	t.Setenv("HOME", testHome)
	t.Setenv("AMP_SETTINGS_FILE", settingsPath)
	t.Setenv("AMP_LOG_LEVEL", "debug")

	threadID := "T-019e1cd4-bde0-778f-84f9-61259c2605e8"
	storeDir := t.TempDir()
	oldStoreDir := neoAmpThreadStoreDir
	neoAmpThreadStoreDir = func() string { return storeDir }
	t.Cleanup(func() { neoAmpThreadStoreDir = oldStoreDir })

	var (
		currentRTMu sync.Mutex
		currentRT   *neoRuntime
		requestsMu  sync.Mutex
		requests    []string
	)
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestsMu.Lock()
		requests = append(requests, r.Method+" "+r.URL.RequestURI())
		requestsMu.Unlock()
		currentRTMu.Lock()
		rt := currentRT
		currentRTMu.Unlock()
		if r.URL.Path == "/api/thread-actors" && r.Method == http.MethodPost && rt != nil {
			response, status := rt.localThreadActorManagementResponse(r.Context(), readNeoJSON(r.Body), "")
			writeNeoJSON(w, status, response)
			return
		}
		if r.URL.Path == "/api/internal" && r.URL.RawQuery == "loadPlugins" {
			writeNeoJSON(w, http.StatusOK, map[string]any{"ok": true, "result": []any{}})
			return
		}
		if r.URL.Path == "/api/internal" && r.URL.RawQuery == "getUserInfo" {
			writeNeoJSON(w, http.StatusOK, map[string]any{"ok": true, "result": map[string]any{"id": "U-local-test", "email": "local@example.com", "workspaceID": "W-local-test"}})
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(proxy.Close)
	parsedProxyURL, err := url.Parse(proxy.URL)
	if err != nil {
		t.Fatalf("parse proxy URL: %v", err)
	}
	proxyHost, proxyPortText, err := net.SplitHostPort(parsedProxyURL.Host)
	if err != nil {
		t.Fatalf("split proxy URL host: %v", err)
	}
	proxyPort, err := strconv.Atoi(proxyPortText)
	if err != nil {
		t.Fatalf("parse proxy URL port: %v", err)
	}
	newRuntime := func() *neoRuntime {
		rt := newNeoRuntime(&config.Config{
			SDKConfig: config.SDKConfig{APIKeys: []string{"local-key"}},
			Host:      proxyHost,
			Port:      proxyPort,
			AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{
				Host:            "127.0.0.1",
				Port:            runtimePort,
				ExecutorCommand: command,
			}},
		})
		currentRTMu.Lock()
		currentRT = rt
		currentRTMu.Unlock()
		if err := rt.start(); err != nil {
			t.Fatalf("start runtime: %v", err)
		}
		return rt
	}

	rt := newRuntime()
	actor := rt.store.ensureThreadActor(threadID)
	actor.updateSettings(map[string]any{"agentMode": "rush"})
	actor.updateEnvironment(map[string]any{"workingDirectory": workDir})
	client := dialNeoActorWebSocket(t, fmt.Sprintf("http://127.0.0.1:%d", runtimePort), threadID)
	if err := client.WriteJSON(map[string]any{"type": "client_spawn_executor", "requestId": "spawn-binary-restart"}); err != nil {
		t.Fatalf("write client_spawn_executor: %v", err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		actor.mu.Lock()
		ready := actor.executorReady
		bootstrapComplete := actor.executorBootstrapComplete
		actor.mu.Unlock()
		if ready && bootstrapComplete {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	actor.mu.Lock()
	ready := actor.executorReady
	bootstrapComplete := actor.executorBootstrapComplete
	var spawned *neoSpawnedExecutor
	for _, candidate := range actor.spawnedExecutors {
		spawned = candidate
		break
	}
	actor.mu.Unlock()
	if !ready || !bootstrapComplete || spawned == nil || spawned.cmd == nil || spawned.cmd.Process == nil {
		t.Fatalf("spawned amp binary did not bootstrap before restart: ready=%v bootstrap=%v spawned=%v\n%s%s", ready, bootstrapComplete, spawned != nil, requestLogForTest(&requestsMu, requests), readSpawnLogsForTest(testHome))
	}
	t.Cleanup(func() {
		spawned.stop()
		if spawned.cmd != nil {
			_ = spawned.cmd.Wait()
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	if err := rt.shutdown(ctx); err != nil {
		cancel()
		t.Fatalf("shutdown runtime: %v", err)
	}
	cancel()
	_ = client.Close()
	if err := spawned.cmd.Process.Signal(syscall.Signal(0)); err != nil {
		t.Fatalf("spawned executor died during runtime shutdown: %v\n%s", err, readSpawnLogsForTest(testHome))
	}

	rt = newRuntime()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = rt.stop(ctx)
	}()

	deadline = time.Now().Add(12 * time.Second)
	for time.Now().Before(deadline) {
		actors := rt.store.findActors(url.Values{"name": []string{"threadActor"}, "key": []string{threadID}})
		for _, record := range actors {
			id := firstNonEmptyString(record["actor_id"], record["id"])
			reconnected := rt.store.get(id)
			if reconnected == nil {
				continue
			}
			reconnected.mu.Lock()
			ready := reconnected.executorReady
			bootstrapComplete := reconnected.executorBootstrapComplete
			executorID := reconnected.executorID
			reconnected.mu.Unlock()
			if ready && bootstrapComplete && executorID != "" {
				return
			}
		}
		if err := spawned.cmd.Process.Signal(syscall.Signal(0)); err != nil {
			t.Fatalf("spawned executor exited before reconnecting: %v\n%s%s", err, requestLogForTest(&requestsMu, requests), readSpawnLogsForTest(testHome))
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("spawned executor did not reconnect after runtime restart\n%s%s", requestLogForTest(&requestsMu, requests), readSpawnLogsForTest(testHome))
}

func freeTCPPortForTest(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen on free port: %v", err)
	}
	defer listener.Close()
	_, portText, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("split free port addr: %v", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("parse free port: %v", err)
	}
	return port
}

func readFileForTest(path string) string {
	raw, err := os.ReadFile(path)
	if err != nil || len(raw) == 0 {
		return ""
	}
	return "\n--- amp log ---\n" + string(raw)
}

func readSpawnLogsForTest(home string) string {
	paths, err := filepath.Glob(filepath.Join(home, ".cli-proxy-api", "logs", "*.log"))
	if err != nil || len(paths) == 0 {
		return ""
	}
	var out strings.Builder
	for _, path := range paths {
		raw, err := os.ReadFile(path)
		if err != nil || len(raw) == 0 {
			continue
		}
		out.WriteString("\n--- spawn amp log: ")
		out.WriteString(filepath.Base(path))
		out.WriteString(" ---\n")
		out.Write(raw)
	}
	return out.String()
}

func requestLogForTest(mu *sync.Mutex, requests []string) string {
	mu.Lock()
	defer mu.Unlock()
	if len(requests) == 0 {
		return ""
	}
	return "\n--- requests ---\n" + strings.Join(requests, "\n")
}

type neoInboundRecordForTest struct {
	ActorID  string
	ThreadID string
	Type     string
	Keys     []string
	Snippet  string
}

type neoInboundRecorderForTest struct {
	mu      sync.Mutex
	records []neoInboundRecordForTest
}

func installNeoInboundRecorderForTest(t *testing.T) *neoInboundRecorderForTest {
	t.Helper()
	recorder := &neoInboundRecorderForTest{}
	neoInboundMessageHookMu.Lock()
	oldHook := neoInboundMessageHook
	neoInboundMessageHook = recorder.record
	neoInboundMessageHookMu.Unlock()
	t.Cleanup(func() {
		neoInboundMessageHookMu.Lock()
		neoInboundMessageHook = oldHook
		neoInboundMessageHookMu.Unlock()
	})
	return recorder
}

func (r *neoInboundRecorderForTest) record(actor *neoActor, msg map[string]any) {
	if r == nil {
		return
	}
	keys := make([]string, 0, len(msg))
	for key := range msg {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	actorID := ""
	threadID := ""
	if actor != nil {
		actorID = actor.id
		threadID = actor.threadID
	}
	r.mu.Lock()
	r.records = append(r.records, neoInboundRecordForTest{
		ActorID:  actorID,
		ThreadID: threadID,
		Type:     stringValue(msg["type"]),
		Keys:     keys,
		Snippet:  neoInboundSnippetForTest(msg),
	})
	r.mu.Unlock()
}

func (r *neoInboundRecorderForTest) unknownTypes() []string {
	if r == nil {
		return nil
	}
	handled := handledNeoInboundTypesForTest()
	seen := map[string]struct{}{}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, record := range r.records {
		if record.Type == "" {
			seen["<empty>"] = struct{}{}
			continue
		}
		if !handled[record.Type] {
			seen[record.Type] = struct{}{}
		}
	}
	unknown := make([]string, 0, len(seen))
	for msgType := range seen {
		unknown = append(unknown, msgType)
	}
	sort.Strings(unknown)
	return unknown
}

func (r *neoInboundRecorderForTest) report() string {
	if r == nil {
		return ""
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.records) == 0 {
		return "\n--- inbound ws messages ---\n(none)"
	}
	type summary struct {
		count int
		first neoInboundRecordForTest
	}
	summaries := map[string]*summary{}
	order := []string{}
	for _, record := range r.records {
		msgType := record.Type
		if msgType == "" {
			msgType = "<empty>"
		}
		entry := summaries[msgType]
		if entry == nil {
			entry = &summary{first: record}
			summaries[msgType] = entry
			order = append(order, msgType)
		}
		entry.count++
	}
	sort.Strings(order)
	var out strings.Builder
	out.WriteString("\n--- inbound ws messages ---\n")
	for _, msgType := range order {
		entry := summaries[msgType]
		out.WriteString(fmt.Sprintf("%s count=%d actor=%s thread=%s keys=%s\n  first=%s\n", msgType, entry.count, entry.first.ActorID, entry.first.ThreadID, strings.Join(entry.first.Keys, ","), entry.first.Snippet))
	}
	return out.String()
}

func neoInboundSnippetForTest(msg map[string]any) string {
	sanitized := sanitizeNeoInboundValueForTest(msg)
	raw, err := json.Marshal(sanitized)
	if err != nil {
		return fmt.Sprintf("%#v", sanitized)
	}
	if len(raw) > 1200 {
		return string(raw[:1200]) + "..."
	}
	return string(raw)
}

func sanitizeNeoInboundValueForTest(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, nested := range typed {
			lowerKey := strings.ToLower(key)
			if strings.Contains(lowerKey, "key") || strings.Contains(lowerKey, "token") || strings.Contains(lowerKey, "secret") || strings.Contains(lowerKey, "password") || strings.Contains(lowerKey, "auth") {
				out[key] = "[redacted]"
				continue
			}
			out[key] = sanitizeNeoInboundValueForTest(nested)
		}
		return out
	case []any:
		out := make([]any, len(typed))
		for i, nested := range typed {
			out[i] = sanitizeNeoInboundValueForTest(nested)
		}
		return out
	case string:
		if len(typed) > 200 {
			return typed[:200] + "..."
		}
		return typed
	default:
		return typed
	}
}

func handledNeoInboundTypesForTest() map[string]bool {
	return toolSet(
		"agent_state",
		"archive_thread",
		"agent-mode",
		"assistant:message",
		"assistant:message-update",
		"cancelled",
		"client_append_manual_bash_invocation",
		"client_append_user_msg",
		"client_archive_thread",
		"client_cancel",
		"client_create_thread",
		"client_dismiss_active_error",
		"client_edit_message",
		"client_filesystem_read_directory",
		"client_filesystem_read_directory_result",
		"client_filesystem_read_file",
		"client_filesystem_read_file_result",
		"client_git_command",
		"client_git_command_result",
		"client_fork_thread",
		"client_mark_message_read",
		"client_mark_message_unread",
		"client_remove_queued_msg",
		"client_resume",
		"client_retry",
		"client_send_message_to_aggman",
		"client_send_message_to_thread",
		"client_set_thread_title",
		"client_spawn_executor",
		"client_steer_queued_msg",
		"client_tool_approval_response",
		"client_unarchive_thread",
		"client_update_thread_settings",
		"client_upsert_notification_subscription",
		"compaction_complete",
		"compaction_records",
		"compaction_started",
		"create_thread",
		"delta",
		"draft",
		"edit_rejected",
		"environment_update",
		"error",
		"error_cleared",
		"error_set",
		"executor_artifact_delete",
		"executor_artifact_upsert",
		"executor_connect",
		"executor_connect_rejected",
		"executor_connected",
		"executor_disconnected",
		"executor_environment_snapshot",
		"executor_environment_update",
		"executor_error",
		"executor_filesystem_read_directory",
		"executor_filesystem_read_directory_result",
		"executor_filesystem_read_file",
		"executor_filesystem_read_file_result",
		"executor_git_command",
		"executor_git_command_result",
		"executor_guidance_discovery",
		"executor_guidance_snapshot",
		"executor_guidance_update",
		"executor_plugin_message",
		"executor_skill_snapshot",
		"executor_status",
		"executor_tool_approval_request",
		"executor_tool_approval_response",
		"executor_tool_lease_ack",
		"executor_tool_lease_revoked",
		"executor_tool_result",
		"executor_tool_result_ack",
		"executor_tools_bootstrap_complete",
		"executor_tools_register",
		"executor_tools_unregister",
		"executor_workspace_maybe_changed",
		"fork",
		"fork_thread",
		"info:manual-bash-invocation",
		"inference_tools",
		"inference:completed",
		"environment",
		"clearPendingNavigation",
		"main-thread",
		"max-tokens",
		"message_added",
		"message_updated",
		"observers",
		"plugin_message",
		"queued_message_added",
		"queued_message_dequeued",
		"queued_message_removed",
		"queued_messages",
		"reasoning-effort",
		"relationship",
		"retry_cancelled",
		"retry_scheduled",
		"retry_started",
		"send_message_to_aggman",
		"send_message_to_thread",
		"thread_status",
		"thread_relationships",
		"thread_settings",
		"thread_title",
		"thread_truncated",
		"thread:truncate",
		"title",
		"tool:data",
		"tool_approval_queue",
		"tool_lease",
		"tool:processed",
		"tool_progress",
		"trace:attributes",
		"trace:end",
		"trace:event",
		"trace:start",
		"setPendingNavigation",
		"unarchive_thread",
		"user:message",
		"user:message:append-content",
		"user:message:interrupt",
		"user:message-queue:dequeue",
		"user:message-queue:discard",
		"user:message-queue:enqueue",
		"user:tool-input",
	)
}

func TestHandledNeoInboundTypesCoverCurrentBinaryProtocolSwitch(t *testing.T) {
	currentBinaryTypes := []string{
		"agent_state",
		"cancelled",
		"client_append_manual_bash_invocation",
		"client_append_user_msg",
		"client_cancel",
		"client_dismiss_active_error",
		"client_edit_message",
		"client_filesystem_read_directory",
		"client_filesystem_read_directory_result",
		"client_filesystem_read_file",
		"client_filesystem_read_file_result",
		"client_git_command",
		"client_git_command_result",
		"client_mark_message_read",
		"client_mark_message_unread",
		"client_remove_queued_msg",
		"client_resume",
		"client_retry",
		"client_set_thread_title",
		"client_spawn_executor",
		"client_steer_queued_msg",
		"client_tool_approval_response",
		"client_update_thread_settings",
		"client_upsert_notification_subscription",
		"compaction_complete",
		"compaction_records",
		"compaction_started",
		"delta",
		"edit_rejected",
		"environment_update",
		"error",
		"error_cleared",
		"error_set",
		"executor_artifact_delete",
		"executor_artifact_upsert",
		"executor_connect",
		"executor_connected",
		"executor_environment_snapshot",
		"executor_environment_update",
		"executor_error",
		"executor_filesystem_read_directory",
		"executor_filesystem_read_directory_result",
		"executor_filesystem_read_file",
		"executor_filesystem_read_file_result",
		"executor_git_command",
		"executor_git_command_result",
		"executor_guidance_discovery",
		"executor_guidance_snapshot",
		"executor_plugin_message",
		"executor_skill_snapshot",
		"executor_status",
		"executor_tool_approval_request",
		"executor_tool_approval_response",
		"executor_tool_lease_ack",
		"executor_tool_lease_revoked",
		"executor_tool_result",
		"executor_tool_result_ack",
		"executor_tools_bootstrap_complete",
		"executor_tools_register",
		"executor_tools_unregister",
		"executor_workspace_maybe_changed",
		"inference_tools",
		"message_added",
		"message_updated",
		"observers",
		"plugin_message",
		"queued_message_added",
		"queued_message_dequeued",
		"queued_message_removed",
		"queued_messages",
		"retry_cancelled",
		"retry_scheduled",
		"retry_started",
		"thread_relationships",
		"thread_settings",
		"thread_status",
		"thread_title",
		"thread_truncated",
		"tool_approval_queue",
		"tool_lease",
		"tool_progress",
	}
	handled := handledNeoInboundTypesForTest()
	var missing []string
	for _, msgType := range currentBinaryTypes {
		if !handled[msgType] {
			missing = append(missing, msgType)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("handledNeoInboundTypesForTest missing current binary protocol cases: %v", missing)
	}
}

func TestNeoExecutorStatusNormalizesLikeBinary(t *testing.T) {
	invalid := normalizeNeoExecutorStatus(map[string]any{"type": "executor_status", "status": "not-valid"})
	if invalid["status"] != "starting" {
		t.Fatalf("invalid status = %#v, want starting", invalid["status"])
	}
	missing := normalizeNeoExecutorStatus(map[string]any{"type": "executor_status"})
	if missing["status"] != "starting" {
		t.Fatalf("missing status = %#v, want starting", missing["status"])
	}
}

func TestNeoExecutorStatusDetailsSanitizeLikeBinary(t *testing.T) {
	payload := normalizeNeoExecutorStatus(map[string]any{
		"type":   "executor_status",
		"status": "running",
		"details": map[string]any{
			"reasonCode": "executor_exited",
			"executionEnvironment": map[string]any{
				"setupState":    "not-valid",
				"setupPhase":    "not-valid",
				"stage":         "not-valid",
				"operation":     "not-valid",
				"providerState": "not-valid",
				"extra":         "kept",
			},
		},
	})
	details := mapValue(payload["details"])
	if _, exists := details["reasonCode"]; exists {
		t.Fatalf("invalid reasonCode was kept: %#v", details)
	}
	environment := mapValue(details["executionEnvironment"])
	for _, key := range []string{"setupState", "setupPhase", "stage", "operation", "providerState"} {
		if _, exists := environment[key]; exists {
			t.Fatalf("invalid executionEnvironment %s was kept: %#v", key, environment)
		}
	}
	if environment["extra"] != "kept" {
		t.Fatalf("executionEnvironment extra = %#v, want kept", environment["extra"])
	}

	valid := normalizeNeoExecutorStatus(map[string]any{
		"type":   "executor_status",
		"status": "running",
		"details": map[string]any{
			"reasonCode": "executor_connected",
			"executionEnvironment": map[string]any{
				"setupState":    "ready",
				"setupPhase":    nil,
				"stage":         "headless_ready",
				"operation":     "recovering",
				"providerState": "running",
			},
		},
	})
	validDetails := mapValue(valid["details"])
	validEnvironment := mapValue(validDetails["executionEnvironment"])
	if validDetails["reasonCode"] != "executor_connected" || validEnvironment["setupState"] != "ready" || validEnvironment["stage"] != "headless_ready" || validEnvironment["operation"] != "recovering" || validEnvironment["providerState"] != "running" {
		t.Fatalf("valid executor status details changed: %#v", validDetails)
	}
	if _, exists := validEnvironment["setupPhase"]; !exists || validEnvironment["setupPhase"] != nil {
		t.Fatalf("nullable setupPhase was not preserved: %#v", validEnvironment)
	}
}

func TestNeoThreadSettingsPayloadSanitizesKnownValuesLikeBinary(t *testing.T) {
	payload := neoThreadSettingsPayload(map[string]any{
		"agentMode":                                  "deep",
		"anthropic.provider":                         "bedrock",
		"anthropic.speed":                            "turbo",
		"anthropic.temperature":                      "warm",
		"anthropic.thinking.enabled":                 "yes",
		"anthropic.interleavedThinking.enabled":      "yes",
		"openai.speed":                               "fast",
		"reasoning.effort":                           "extreme",
		"internal.oracleReasoningEffort":             "max",
		"gemini.thinkingLevel":                       "huge",
		"internal.compactionThresholdPercent":        120,
		"painter.model":                              "",
		"internal.model":                             []any{"bad"},
		"agent.skipTitleGenerationIfMessageContains": []any{"keep"},
		"tools.disable":                              []any{"Bash", 42},
		"tools.enable":                               "Read",
	})
	settings := mapValue(payload["settings"])
	for _, key := range []string{"anthropic.provider", "anthropic.speed", "anthropic.temperature", "anthropic.thinking.enabled", "anthropic.interleavedThinking.enabled", "reasoning.effort", "internal.oracleReasoningEffort", "gemini.thinkingLevel", "internal.compactionThresholdPercent", "painter.model", "internal.model", "tools.disable", "tools.enable"} {
		if _, exists := settings[key]; exists {
			t.Fatalf("invalid setting %s was kept: %#v", key, settings)
		}
	}
	if settings["openai.speed"] != "fast" || settings["agentMode"] != "deep" {
		t.Fatalf("valid settings changed: %#v", settings)
	}
	if len(arrayValue(settings["agent.skipTitleGenerationIfMessageContains"])) != 1 {
		t.Fatalf("unknown/schema setting was not preserved: %#v", settings)
	}

	valid := mapValue(neoThreadSettingsPayload(map[string]any{
		"anthropic.provider":                    "vertex",
		"anthropic.speed":                       "standard",
		"anthropic.temperature":                 json.Number("0.2"),
		"anthropic.thinking.enabled":            true,
		"anthropic.interleavedThinking.enabled": false,
		"openai.speed":                          "standard",
		"reasoning.effort":                      "max",
		"internal.oracleReasoningEffort":        "xhigh",
		"gemini.thinkingLevel":                  "medium",
		"internal.compactionThresholdPercent":   json.Number("75.5"),
		"painter.model":                         "gpt-image-2",
		"internal.model":                        map[string]any{"deep": "openai/gpt-5.5"},
		"tools.disable":                         []any{"Bash"},
		"tools.enable":                          []any{"Read"},
	})["settings"])
	if valid["anthropic.provider"] != "vertex" || valid["anthropic.speed"] != "standard" || valid["anthropic.temperature"] != json.Number("0.2") || valid["anthropic.thinking.enabled"] != true || valid["anthropic.interleavedThinking.enabled"] != false || valid["openai.speed"] != "standard" || valid["reasoning.effort"] != "max" || valid["internal.oracleReasoningEffort"] != "xhigh" || valid["gemini.thinkingLevel"] != "medium" || valid["internal.compactionThresholdPercent"] != json.Number("75.5") || valid["painter.model"] != "gpt-image-2" {
		t.Fatalf("valid settings were not preserved: %#v", valid)
	}
	if len(arrayValue(valid["tools.disable"])) != 1 || len(arrayValue(valid["tools.enable"])) != 1 {
		t.Fatalf("valid tool settings were not preserved: %#v", valid)
	}
}

func TestNeoActorUpdateSettingsStoresSanitizedKnownValues(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor, _ := rt.store.upsert(map[string]any{"name": "thread-actor", "key": "T-settings"}, true)
	actor.updateSettings(map[string]any{
		"agentMode":             "smart",
		"reasoning.effort":      "invalid",
		"anthropic.speed":       "turbo",
		"tools.enable":          []any{"Read", 123},
		"anthropic.temperature": 0.2,
	})

	if actor.settings["anthropic.speed"] != nil || actor.settings["tools.enable"] != nil {
		t.Fatalf("invalid known settings were stored: %#v", actor.settings)
	}
	if actor.settings["anthropic.temperature"] != 0.2 {
		t.Fatalf("valid known setting was not stored: %#v", actor.settings)
	}
	if actor.settings["reasoning.effort"] != "high" {
		t.Fatalf("smart mode effort = %#v, want high default after invalid input", actor.settings["reasoning.effort"])
	}
}

func TestNeoHeadlessExecutorArgsByMode(t *testing.T) {
	tests := []struct {
		name   string
		mode   string
		effort string
		want   []string
	}{
		{name: "smart", mode: "smart", effort: "high", want: []string{"--mode", "smart", "--effort", "high", "--headless", "T-test"}},
		{name: "deep", mode: "deep", effort: "xhigh", want: []string{"--mode", "deep", "--effort", "xhigh", "--headless", "T-test"}},
		{name: "rush", mode: "rush", effort: "", want: []string{"--mode", "rush", "--headless", "T-test"}},
		{name: "rush none", mode: "rush", effort: "none", want: []string{"--mode", "rush", "--headless", "T-test"}},
		{name: "large", mode: "large", effort: "", want: []string{"--mode", "large", "--headless", "T-test"}},
		{name: "default", mode: "", effort: "", want: []string{"--mode", "smart", "--headless", "T-test"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := neoHeadlessExecutorArgs("T-test", tt.mode, tt.effort)
			if strings.Join(got, "\x00") != strings.Join(tt.want, "\x00") {
				t.Fatalf("args = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestNeoExecutorConnectTimeoutConfig(t *testing.T) {
	got := neoExecutorConnectTimeout(&config.Config{
		AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{ExecutorConnectTimeoutSeconds: 3}},
	})
	if got != 3*time.Second {
		t.Fatalf("timeout = %s, want 3s", got)
	}
	if got := neoExecutorConnectTimeout(&config.Config{}); got != defaultNeoExecutorConnectTimeout {
		t.Fatalf("default timeout = %s, want %s", got, defaultNeoExecutorConnectTimeout)
	}
}

func TestSelectNeoModelRouteHonorsExplicitOpenAIModel(t *testing.T) {
	got := selectNeoModelRoute("deep", map[string]any{"internal.model": map[string]any{"deep": "openai:gpt-5.4(xhigh)"}})
	if got.Provider != "openai" || got.Model != "gpt-5.4" || got.ThinkingSuffix != "xhigh" {
		t.Fatalf("route = %+v, want openai/gpt-5.4 thinking=xhigh", got)
	}
}

func TestSelectNeoModelRouteDefaultsDeepToGPT55(t *testing.T) {
	got := selectNeoModelRoute("deep", nil)
	if got.Provider != "openai" || got.Model != "gpt-5.5" {
		t.Fatalf("route = %+v, want openai/gpt-5.5", got)
	}
}

func TestSelectNeoModelRouteDefaultsRushToGPT55(t *testing.T) {
	got := selectNeoModelRoute("rush", nil)
	if got.Provider != "openai" || got.Model != "gpt-5.5" {
		t.Fatalf("route = %+v, want openai/gpt-5.5", got)
	}
}

func TestSelectNeoModelRouteDefaultsAggManToOpus46(t *testing.T) {
	got := selectNeoModelRoute("agg-man", nil)
	if got.Provider != "anthropic" || got.Model != "claude-opus-4-6" {
		t.Fatalf("route = %+v, want anthropic/claude-opus-4-6", got)
	}
}

func TestSelectNeoModelRouteDefaultsSmartToOpus47(t *testing.T) {
	for _, mode := range []string{"", "smart", "SMART"} {
		got := selectNeoModelRoute(mode, nil)
		if got.Provider != "anthropic" || got.Model != "claude-opus-4-7" {
			t.Fatalf("mode %q route = %+v, want anthropic/claude-opus-4-7", mode, got)
		}
	}
}

func TestSelectNeoModelRouteDefaultsUnknownModeToBinaryFallback(t *testing.T) {
	got := selectNeoModelRoute("frontier", nil)
	if got.Provider != "anthropic" || got.Model != defaultNeoUnknownModeModel {
		t.Fatalf("route = %+v, want anthropic/%s", got, defaultNeoUnknownModeModel)
	}
}

func TestSelectNeoModelRouteDefaultsNostromoToAmpNostromo(t *testing.T) {
	got := selectNeoModelRoute("nostromo", nil)
	if got.Provider != "openai" || got.Model != "amp-nostromo-v1" {
		t.Fatalf("route = %+v, want openai/amp-nostromo-v1", got)
	}
}

func TestProviderForNeoModelMatchesBinaryProviderTable(t *testing.T) {
	for _, tc := range []struct {
		model string
		want  string
	}{
		{model: "gpt-5.5", want: "openai"},
		{model: "openai/gpt-oss-120b", want: "openai"},
		{model: "grok-code-fast-1", want: "xai"},
		{model: "gemini-3.5-flash", want: "google"},
		{model: "zai-glm-4.7", want: "cerebras"},
		{model: "accounts/fireworks/models/glm-5", want: "fireworks"},
		{model: "moonshotai/Kimi-K2.5", want: "baseten"},
		{model: "kimi-k2-instruct-0905", want: "moonshotai"},
		{model: "sonoma-sky-alpha", want: "openrouter"},
		{model: "z-ai/glm-4.6", want: "openrouter"},
		{model: "moonshotai/kimi-k2-0905", want: "openrouter"},
		{model: "qwen/qwen3-coder", want: "openrouter"},
		{model: "claude-opus-4-7", want: "anthropic"},
		{model: "claude-opus-4-8", want: "anthropic"},
	} {
		t.Run(tc.model, func(t *testing.T) {
			if got := providerForNeoModel(tc.model); got != tc.want {
				t.Fatalf("provider = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestParseNeoModelRoutePreservesBinarySlashModelNames(t *testing.T) {
	for _, tc := range []struct {
		raw      string
		provider string
		model    string
	}{
		{raw: "openai/gpt-oss-120b", provider: "openai", model: "openai/gpt-oss-120b"},
		{raw: "accounts/fireworks/models/glm-5", provider: "fireworks", model: "accounts/fireworks/models/glm-5"},
		{raw: "moonshotai/Kimi-K2.5", provider: "baseten", model: "moonshotai/Kimi-K2.5"},
		{raw: "z-ai/glm-4.6", provider: "openrouter", model: "z-ai/glm-4.6"},
		{raw: "qwen/qwen3-coder", provider: "openrouter", model: "qwen/qwen3-coder"},
		{raw: "openai:gpt-5.5", provider: "openai", model: "gpt-5.5"},
		{raw: "openrouter/anthropic/claude-sonnet-4-5", provider: "openrouter", model: "anthropic/claude-sonnet-4-5"},
		{raw: "fireworks/custom-model", provider: "fireworks", model: "custom-model"},
		{raw: "groq/openai/gpt-oss-120b", provider: "groq", model: "openai/gpt-oss-120b"},
		{raw: "vertexai/gemini-3.1-pro-preview", provider: "google", model: "gemini-3.1-pro-preview"},
	} {
		t.Run(tc.raw, func(t *testing.T) {
			got := parseNeoModelRoute(tc.raw)
			if got.Provider != tc.provider || got.Model != tc.model {
				t.Fatalf("route = %+v, want %s/%s", got, tc.provider, tc.model)
			}
		})
	}
}

func TestInferNeoLocalPreservesExplicitOpenAICompatibleProvider(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/provider/openrouter/v1/chat/completions" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		payload := readNeoJSON(r.Body)
		if payload["model"] != "anthropic/claude-sonnet-4-5" {
			t.Fatalf("model = %#v, want explicit OpenRouter model; payload=%#v", payload["model"], payload)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"ok"}}],"usage":{"prompt_tokens":2,"completion_tokens":1}}`))
	}))
	defer upstream.Close()

	result, err := inferNeoLocal(testNeoRuntimeForServer(t, upstream), neoInferenceRequest{
		AgentMode: "smart",
		Settings:  map[string]any{"internal.model": "openrouter/anthropic/claude-sonnet-4-5"},
		History:   []neoHistoryMessage{{Role: "user", Text: "hello"}},
	})
	if err != nil {
		t.Fatalf("inferNeoLocal error: %v", err)
	}
	if result.Provider != "openrouter" || result.Model != "anthropic/claude-sonnet-4-5" || result.Text != "ok" {
		t.Fatalf("result = %+v, want explicit OpenRouter route", result)
	}
}

func TestInferNeoLocalUsesBinaryOpenAICompatibleProviderRoute(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/provider/openrouter/v1/chat/completions" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		payload := readNeoJSON(r.Body)
		if payload["stream"] != false || payload["model"] != "sonoma-sky-alpha" {
			t.Fatalf("payload = %#v, want non-streaming sonoma chat completion", payload)
		}
		if _, exists := payload["reasoning_effort"]; exists {
			t.Fatalf("openai-compatible provider should not receive OpenAI reasoning_effort: %#v", payload)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"ok"}}],"usage":{"prompt_tokens":2,"completion_tokens":1}}`))
	}))
	defer upstream.Close()

	result, err := inferNeoLocal(testNeoRuntimeForServer(t, upstream), neoInferenceRequest{
		AgentMode: "smart",
		Settings:  map[string]any{"internal.model": map[string]any{"smart": "sonoma-sky-alpha"}},
		History:   []neoHistoryMessage{{Role: "user", Text: "hello"}},
	})
	if err != nil {
		t.Fatalf("inferNeoLocal error: %v", err)
	}
	if result.Provider != "openrouter" || result.Model != "sonoma-sky-alpha" || result.Text != "ok" {
		t.Fatalf("result = %+v, want openrouter sonoma text", result)
	}
}

func TestInferNeoLocalStreamUsesBinaryOpenAICompatibleProviderRoute(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/provider/openrouter/v1/chat/completions" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		payload := readNeoJSON(r.Body)
		if payload["stream"] != true || payload["model"] != "sonoma-sky-alpha" {
			t.Fatalf("payload = %#v, want streaming sonoma chat completion", payload)
		}
		if _, exists := payload["reasoning_effort"]; exists {
			t.Fatalf("openai-compatible provider should not receive OpenAI reasoning_effort: %#v", payload)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1}}\n\n"))
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer upstream.Close()

	var deltas []string
	result, err := inferNeoLocalStream(testNeoRuntimeForServer(t, upstream), neoInferenceRequest{
		AgentMode:       "smart",
		ReasoningEffort: "max",
		Settings:        map[string]any{"internal.model": map[string]any{"smart": "sonoma-sky-alpha"}},
		History:         []neoHistoryMessage{{Role: "user", Text: "hello"}},
	}, func(delta neoInferenceDelta) {
		deltas = append(deltas, delta.Text)
	})
	if err != nil {
		t.Fatalf("inferNeoLocalStream error: %v", err)
	}
	if result.Provider != "openrouter" || result.Model != "sonoma-sky-alpha" || result.Text != "hi" || strings.Join(deltas, "") != "hi" {
		t.Fatalf("result = %+v deltas=%#v, want streaming openrouter sonoma text", result, deltas)
	}
}

func TestInferNeoLocalFireworksAppliesBinaryProviderSettings(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprintf("stream_%v", stream), func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/api/provider/fireworks/v1/chat/completions" {
					t.Fatalf("unexpected path %s", r.URL.Path)
				}
				if got := r.Header.Get("x-fireworks-direct-routing"); got != "true" {
					t.Fatalf("x-fireworks-direct-routing = %q, want true", got)
				}
				payload := readNeoJSON(r.Body)
				if payload["stream"] != stream || payload["model"] != "accounts/fireworks/models/kimi-k2-instruct-0905" {
					t.Fatalf("payload = %#v, want fireworks kimi stream=%v", payload, stream)
				}
				if got := stringValue(payload["reasoning_effort"]); got != "none" {
					t.Fatalf("reasoning_effort = %q, want none; payload=%#v", got, payload)
				}
				if got := payload["temperature"]; got != 0.6 {
					t.Fatalf("temperature = %#v, want 0.6; payload=%#v", got, payload)
				}
				if got := payload["top_p"]; got != 0.95 {
					t.Fatalf("top_p = %#v, want 0.95; payload=%#v", got, payload)
				}
				if got := numberFrom(payload["max_tokens"]); got != 32000 {
					t.Fatalf("max_tokens = %d, want 32000; payload=%#v", got, payload)
				}

				if stream {
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1}}\n\n"))
					_, _ = w.Write([]byte("data: [DONE]\n\n"))
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"ok"}}],"usage":{"prompt_tokens":2,"completion_tokens":1}}`))
			}))
			defer upstream.Close()

			request := neoInferenceRequest{
				ThreadID:  "T-test",
				AgentMode: "smart",
				Settings: map[string]any{
					"internal.model":                   "accounts/fireworks/models/kimi-k2-instruct-0905",
					"internal.fireworks.directRouting": true,
					"internal.kimi.reasoning":          "none",
				},
				History: []neoHistoryMessage{{Role: "user", Text: "hello"}},
			}
			if stream {
				var deltas []string
				result, err := inferNeoLocalStream(testNeoRuntimeForServer(t, upstream), request, func(delta neoInferenceDelta) {
					deltas = append(deltas, delta.Text)
				})
				if err != nil {
					t.Fatalf("inferNeoLocalStream error: %v", err)
				}
				if result.Provider != "fireworks" || result.Model != "accounts/fireworks/models/kimi-k2-instruct-0905" || result.Text != "hi" || strings.Join(deltas, "") != "hi" {
					t.Fatalf("result = %+v deltas=%#v, want fireworks stream", result, deltas)
				}
				return
			}
			result, err := inferNeoLocal(testNeoRuntimeForServer(t, upstream), request)
			if err != nil {
				t.Fatalf("inferNeoLocal error: %v", err)
			}
			if result.Provider != "fireworks" || result.Model != "accounts/fireworks/models/kimi-k2-instruct-0905" || result.Text != "ok" {
				t.Fatalf("result = %+v, want fireworks text", result)
			}
		})
	}
}

func TestInferNeoLocalBasetenAppliesBinaryKimiReasoningSettings(t *testing.T) {
	for _, tc := range []struct {
		name         string
		reasoning    string
		wantTemplate bool
		wantTemp     float64
	}{
		{name: "default", wantTemplate: true, wantTemp: 1},
		{name: "none", reasoning: "none", wantTemp: 0.6},
	} {
		t.Run(tc.name, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/api/provider/baseten/v1/chat/completions" {
					t.Fatalf("unexpected path %s", r.URL.Path)
				}
				payload := readNeoJSON(r.Body)
				if payload["model"] != "moonshotai/Kimi-K2.5" {
					t.Fatalf("model = %#v, want moonshotai/Kimi-K2.5; payload=%#v", payload["model"], payload)
				}
				if _, exists := payload["reasoning_effort"]; exists {
					t.Fatalf("baseten payload should not include reasoning_effort: %#v", payload)
				}
				template := mapValue(payload["chat_template_args"])
				if tc.wantTemplate {
					if template["enable_thinking"] != true {
						t.Fatalf("chat_template_args = %#v, want enable_thinking true; payload=%#v", template, payload)
					}
				} else if len(template) > 0 {
					t.Fatalf("chat_template_args should be omitted for none reasoning: %#v", payload)
				}
				if got, ok := payload["temperature"].(float64); !ok || got != tc.wantTemp {
					t.Fatalf("temperature = %#v, want %#v; payload=%#v", payload["temperature"], tc.wantTemp, payload)
				}
				if got := payload["top_p"]; got != 0.95 {
					t.Fatalf("top_p = %#v, want 0.95; payload=%#v", got, payload)
				}
				if got := numberFrom(payload["max_tokens"]); got != 32000 {
					t.Fatalf("max_tokens = %d, want 32000; payload=%#v", got, payload)
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"ok"}}],"usage":{"prompt_tokens":2,"completion_tokens":1}}`))
			}))
			defer upstream.Close()

			settings := map[string]any{"internal.model": "moonshotai/Kimi-K2.5"}
			if tc.reasoning != "" {
				settings["internal.kimi.reasoning"] = tc.reasoning
			}
			result, err := inferNeoLocal(testNeoRuntimeForServer(t, upstream), neoInferenceRequest{
				ThreadID:  "T-test",
				AgentMode: "smart",
				Settings:  settings,
				History:   []neoHistoryMessage{{Role: "user", Text: "hello"}},
			})
			if err != nil {
				t.Fatalf("inferNeoLocal error: %v", err)
			}
			if result.Provider != "baseten" || result.Model != "moonshotai/Kimi-K2.5" || result.Text != "ok" {
				t.Fatalf("result = %+v, want baseten text", result)
			}
		})
	}
}

func TestNeoModelRegistryMatchesAmpBinaryValues(t *testing.T) {
	for _, tc := range []struct {
		model    string
		context  int
		maxOut   int
		maxInput int
	}{
		{model: "claude-sonnet-4-20250514", context: 1000000, maxOut: 32000, maxInput: 968000},
		{model: "claude-sonnet-4-6", context: 1000000, maxOut: 64000, maxInput: 936000},
		{model: "claude-opus-4-6-1m", context: 1000000, maxOut: 32000, maxInput: 968000},
		{model: "claude-opus-4-7", context: 332000, maxOut: 32000, maxInput: 300000},
		{model: "claude-opus-4-8", context: 332000, maxOut: 32000, maxInput: 300000},
		{model: "o3", context: 200000, maxOut: 100000, maxInput: 100000},
		{model: "o3-mini", context: 200000, maxOut: 100000, maxInput: 100000},
		{model: "openai/gpt-oss-120b", context: 128000, maxOut: 32000, maxInput: 96000},
		{model: "gemini-3-pro-image", context: 1048576, maxOut: 65535, maxInput: 983041},
		{model: "gemini-3.5-flash", context: 1048576, maxOut: 65535, maxInput: 983041},
		{model: "accounts/fireworks/models/qwen3-coder-480b-a35b-instruct", context: 230144, maxOut: 32000, maxInput: 198144},
		{model: "moonshotai/Kimi-K2.5", context: 262144, maxOut: 32000, maxInput: 230144},
		{model: "kimi-k2-instruct-0905", context: 1000000, maxOut: 32000, maxInput: 968000},
		{model: "moonshotai/kimi-k2-instruct-0905", context: 1000000, maxOut: 32000, maxInput: 968000},
		{model: "z-ai/glm-4.6", context: 131000, maxOut: 40000, maxInput: 91000},
	} {
		t.Run(tc.model, func(t *testing.T) {
			if got := neoModelContextWindow[tc.model]; got != tc.context {
				t.Fatalf("context window = %d, want %d", got, tc.context)
			}
			if got := neoModelMaxOutputTokens[tc.model]; got != tc.maxOut {
				t.Fatalf("max output = %d, want %d", got, tc.maxOut)
			}
			if got := neoModelMaxInputTokens(tc.model); got != tc.maxInput {
				t.Fatalf("max input = %d, want %d", got, tc.maxInput)
			}
		})
	}
}

func TestNeoEffectiveContextWindowMatchesAmpLargeContextRules(t *testing.T) {
	for _, tc := range []struct {
		name     string
		mode     string
		model    string
		context  int
		maxInput int
	}{
		{name: "smart opus 4.6 registry window", mode: "smart", model: "claude-opus-4-6", context: 332000, maxInput: 300000},
		{name: "large opus 4.6 expands", mode: "large", model: "claude-opus-4-6", context: 1000000, maxInput: 968000},
		{name: "large opus 4.6 alias stays 1m", mode: "large", model: "claude-opus-4-6-1m", context: 1000000, maxInput: 968000},
		{name: "large opus 4.7 stays registry window", mode: "large", model: "claude-opus-4-7", context: 332000, maxInput: 300000},
		{name: "large opus 4.8 stays registry window", mode: "large", model: "claude-opus-4-8", context: 332000, maxInput: 300000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := neoEffectiveContextWindow(tc.mode, tc.model); got != tc.context {
				t.Fatalf("effective context = %d, want %d", got, tc.context)
			}
			if got := neoEffectiveMaxInputTokens(tc.mode, tc.model); got != tc.maxInput {
				t.Fatalf("effective max input = %d, want %d", got, tc.maxInput)
			}
		})
	}
}

func TestSelectNeoTitleRouteDefaultsToCurrentMode(t *testing.T) {
	enabled := true
	cfg := &config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled}}}
	got := selectNeoTitleRoute(cfg, "deep", nil)
	if got.Provider != "anthropic" || got.Model != defaultNeoTitleModel {
		t.Fatalf("title route = %+v, want anthropic/%s", got, defaultNeoTitleModel)
	}
}

func TestSelectNeoTitleRouteHonorsTitleModelOverride(t *testing.T) {
	got := selectNeoTitleRoute(&config.Config{
		AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{TitleModel: "anthropic:claude-haiku-4-5-20251001"}},
	}, "deep", map[string]any{"internal.model": map[string]any{"deep": "openai:gpt-5.5"}})
	if got.Provider != "anthropic" || got.Model != "claude-haiku-4-5-20251001" {
		t.Fatalf("title route = %+v, want anthropic/claude-haiku-4-5-20251001", got)
	}
}

func TestNeoTitleHelpersSanitizeAndTrimTranscript(t *testing.T) {
	history := neoTitleHistory([]neoHistoryMessage{
		{Role: "user", Text: "please fix the local Neo bridge"},
		{Role: "tool", Text: "large tool output"},
		{Role: "assistant", Text: "I will inspect the runtime"},
	})
	if len(history) != 1 || history[0].Text != "please fix the local Neo bridge" {
		t.Fatalf("title history = %#v", history)
	}
	if got := sanitizeNeoGeneratedTitle("Title: \"Fix Local Neo Bridge\".\nextra"); got != "Fix Local Neo Bridge" {
		t.Fatalf("sanitized title = %q", got)
	}
}

func TestNeoSystemPromptIncludesSkillNames(t *testing.T) {
	prompt := neoSystemPrompt(neoInferenceRequest{
		Tools: []neoToolSpec{
			{Name: "skill", Meta: map[string]any{"skillNames": []any{"code-review", "craft-docs"}}},
		},
	}, neoModelRoute{Provider: "anthropic", Model: "claude-opus-4-7"})
	if !strings.Contains(prompt, "<available_skills>") || !strings.Contains(prompt, "<name>code-review</name>") || !strings.Contains(prompt, "<name>craft-docs</name>") {
		t.Fatalf("prompt missing skill names:\n%s", prompt)
	}
	if strings.Contains(prompt, "Available skills: code-review") {
		t.Fatalf("prompt should use Amp's structured skills block, not the old flat hint:\n%s", prompt)
	}
}

func TestNeoSystemPromptUsesCustomSystemPromptSettingAsBase(t *testing.T) {
	prompt := neoSystemPrompt(neoInferenceRequest{
		AgentMode: "deep",
		Settings:  map[string]any{"systemPrompt": "Custom base prompt for this SDK actor."},
		Environment: map[string]any{
			"isLocalClientActorThread": true,
		},
		Tools: []neoToolSpec{
			{Name: "skill", Meta: map[string]any{"skillNames": []any{"code-review"}}},
		},
	}, neoModelRoute{Provider: "openai", Model: "gpt-5.5"})

	if !strings.HasPrefix(prompt, "Custom base prompt for this SDK actor.") {
		t.Fatalf("custom prompt was not used as the base:\n%s", prompt)
	}
	if strings.Contains(prompt, "You are Amp, an autonomous coding agent") {
		t.Fatalf("built-in base prompt was not replaced:\n%s", prompt)
	}
	for _, want := range []string{"### Available skills", "- code-review:", "the user's Amp client went offline"} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("custom prompt assembly missing %q:\n%s", want, prompt)
		}
	}
}

func TestNeoSystemPromptAppliesScaffoldCustomizationReplaceBase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "scaffold.yaml")
	if err := os.WriteFile(path, []byte("systemPrompt:\n  type: replaceBase\n  value:\n    - Custom scaffold foundation.\n"), 0o600); err != nil {
		t.Fatalf("write scaffold customization: %v", err)
	}

	prompt := neoSystemPrompt(neoInferenceRequest{
		AgentMode: "deep",
		Settings:  map[string]any{"internal.scaffoldCustomizationFile": path},
		Environment: map[string]any{
			"isLocalClientActorThread": true,
		},
		Tools: []neoToolSpec{
			{Name: "skill", Meta: map[string]any{"skillNames": []any{"code-review"}}},
		},
	}, neoModelRoute{Provider: "openai", Model: "gpt-5.5"})

	if !strings.HasPrefix(prompt, "Custom scaffold foundation.") {
		t.Fatalf("scaffold prompt did not replace base:\n%s", prompt)
	}
	if strings.Contains(prompt, "You are Amp, an autonomous coding agent") {
		t.Fatalf("built-in base prompt was not replaced:\n%s", prompt)
	}
	for _, want := range []string{"### Available skills", "- code-review:", "the user's Amp client went offline"} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("scaffold replaceBase prompt missing %q:\n%s", want, prompt)
		}
	}
}

func TestNeoSystemPromptCreatesScaffoldCustomizationTemplateWithoutApplyingIt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "scaffold.yaml")
	prompt := neoSystemPrompt(neoInferenceRequest{
		AgentMode: "smart",
		Settings:  map[string]any{"internal.scaffoldCustomizationFile": path},
		Tools: []neoToolSpec{
			{Name: "Read", Description: "read files", InputSchema: map[string]any{"type": "object"}},
		},
	}, neoModelRoute{Provider: "openai", Model: "gpt-5"})

	if !strings.Contains(prompt, "You are Amp, a powerful AI coding agent") {
		t.Fatalf("missing scaffold file should not alter current prompt:\n%s", prompt)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("scaffold template was not created: %v", err)
	}
	template := string(raw)
	for _, want := range []string{"systemPrompt:", "type: replaceAll", "enableToolSpecs:", "name: Read"} {
		if !strings.Contains(template, want) {
			t.Fatalf("template missing %q:\n%s", want, template)
		}
	}
}

func TestNeoSystemPromptUsesRushModeInstructions(t *testing.T) {
	prompt := neoSystemPrompt(neoInferenceRequest{AgentMode: "rush"}, neoModelRoute{Provider: "openai", Model: "gpt-5.5"})
	for _, want := range []string{"fewest useful tool loops", "## Contract", "## Operating Mode", "## Discovery", "# File Links", "Speed and low token use are the priority"} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("rush prompt missing %q:\n%s", want, prompt)
		}
	}
}

func TestNeoSystemPromptUsesExpandedModeFamilies(t *testing.T) {
	headingGuidance := "Use a few information-dense H1-H3 headings for important updates and navigation; each should state a takeaway, not merely organize content."
	communicationGuidance := "Communicate so the user can tell whether the work makes sense. This applies to plans, in-progress decisions, blockers, and final summaries."
	mermaidGuidance := "Only write Mermaid syntax for diagrams if the user explicitly asks for Mermaid diagrams."
	closedDiagram := "╰────────╯\n```"

	deep := neoSystemPrompt(neoInferenceRequest{AgentMode: "deep"}, neoModelRoute{Provider: "openai", Model: "gpt-5.5"})
	for _, want := range []string{"## Discovery Discipline", "## Verification", "## Working with the user", communicationGuidance, headingGuidance, mermaidGuidance, closedDiagram} {
		if !strings.Contains(deep, want) {
			t.Fatalf("deep prompt missing %q:\n%s", want, deep)
		}
	}

	deep54 := neoSystemPrompt(neoInferenceRequest{AgentMode: "deep"}, neoModelRoute{Provider: "openai", Model: "gpt-5.4(xhigh)"})
	if !strings.Contains(deep54, "You are Amp. You and the user share the same workspace") || strings.Contains(deep54, "## Autonomy And Persistence") {
		t.Fatalf("deep gpt-5.4 fallback prompt not selected:\n%s", deep54)
	}
	for _, want := range []string{headingGuidance, closedDiagram} {
		if !strings.Contains(deep54, want) {
			t.Fatalf("deep gpt-5.4 prompt missing %q:\n%s", want, deep54)
		}
	}

	genericOpenAI := neoSystemPrompt(neoInferenceRequest{AgentMode: "smart"}, neoModelRoute{Provider: "openai", Model: "gpt-5"})
	for _, want := range []string{"# Fast Context Understanding", "# Parallel Execution Policy", "# Final Status Spec", "**Bad**", "> Bash", "GPT-5.5 reasoning model", headingGuidance, closedDiagram} {
		if !strings.Contains(genericOpenAI, want) {
			t.Fatalf("generic OpenAI prompt missing %q:\n%s", want, genericOpenAI)
		}
	}
}

func TestNeoPromptFamilyMatchesBinarySelector(t *testing.T) {
	for _, tc := range []struct {
		name      string
		agentMode string
		route     neoModelRoute
		want      string
	}{
		{name: "rush mode", agentMode: "rush", route: neoModelRoute{Provider: "openai", Model: "gpt-5.5"}, want: neoPromptFamilyRush},
		{name: "deep gpt55", agentMode: "deep", route: neoModelRoute{Provider: "openai", Model: "gpt-5.5"}, want: neoPromptFamilyDeep},
		{name: "deep gpt54", agentMode: "deep", route: neoModelRoute{Provider: "openai", Model: "gpt-5.4"}, want: neoPromptFamilyDeepGPT54},
		{name: "codex model", agentMode: "smart", route: neoModelRoute{Provider: "openai", Model: "gpt-5-codex"}, want: neoPromptFamilyGPT5Codex},
		{name: "kimi model", agentMode: "smart", route: neoModelRoute{Provider: "anthropic", Model: "kimi-k2-0905"}, want: neoPromptFamilyKimi},
		{name: "generic openai", agentMode: "smart", route: neoModelRoute{Provider: "openai", Model: "o3"}, want: neoPromptFamilyGPT},
		{name: "xai provider", agentMode: "smart", route: neoModelRoute{Provider: "xai", Model: "grok-code-fast-1"}, want: neoPromptFamilyXAI},
		{name: "google provider", agentMode: "smart", route: neoModelRoute{Provider: "google", Model: "gemini-3-pro"}, want: neoPromptFamilyGemini},
		{name: "default provider", agentMode: "smart", route: neoModelRoute{Provider: "anthropic", Model: "claude-opus-4-7"}, want: neoPromptFamilyDefault},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := neoPromptFamily(tc.agentMode, tc.route); got != tc.want {
				t.Fatalf("prompt family = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestNeoSystemPromptUsesBinaryPromptFamilies(t *testing.T) {
	headingGuidance := "Use a few information-dense H1-H3 headings for important updates and navigation; each should state a takeaway, not merely organize content."
	closedDiagram := "╰────────╯\n```"

	for _, tc := range []struct {
		name    string
		request neoInferenceRequest
		route   neoModelRoute
		want    []string
	}{
		{
			name:    "codex",
			request: neoInferenceRequest{AgentMode: "smart"},
			route:   neoModelRoute{Provider: "openai", Model: "gpt-5-codex"},
			want:    []string{"If the user asks you to do an edit or you can infer it, do edits.", "# Fast Context Understanding", "**Bad**", "> Bash", "GPT-5.5 reasoning model", headingGuidance, closedDiagram},
		},
		{
			name:    "xai",
			request: neoInferenceRequest{AgentMode: "smart"},
			route:   neoModelRoute{Provider: "xai", Model: "grok-code-fast-1"},
			want:    []string{"When invoking the Read tool, ALWAYS use absolute paths.", "# Diagrams", headingGuidance, closedDiagram},
		},
		{
			name:    "kimi",
			request: neoInferenceRequest{AgentMode: "smart"},
			route:   neoModelRoute{Provider: "anthropic", Model: "kimi-k2-0905"},
			want:    []string{"**SPEED FIRST**", "Prefer specialized tools over Bash", headingGuidance, closedDiagram},
		},
		{
			name:    "gemini with optional tool guidance",
			request: neoInferenceRequest{AgentMode: "smart", Tools: []neoToolSpec{{Name: "oracle"}, {Name: "get_diagnostics"}}},
			route:   neoModelRoute{Provider: "google", Model: "gemini-3-pro"},
			want:    []string{"oracle tool to get expert guidance", "get_diagnostics tool and  any lint", headingGuidance, closedDiagram},
		},
		{
			name:    "default",
			request: neoInferenceRequest{AgentMode: "smart"},
			route:   neoModelRoute{Provider: "anthropic", Model: "claude-opus-4-7"},
			want:    []string{"<autonomy_and_persistence>", "<using_subagents>", "fewer than 4 lines of text", headingGuidance, closedDiagram},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prompt := neoSystemPrompt(tc.request, tc.route)
			for _, want := range tc.want {
				if !strings.Contains(prompt, want) {
					t.Fatalf("prompt missing %q:\n%s", want, prompt)
				}
			}
		})
	}
}

func TestNeoSystemPromptIncludesLocalClientActorFailureGuidance(t *testing.T) {
	prompt := neoSystemPrompt(neoInferenceRequest{
		AgentMode:   "smart",
		Environment: map[string]any{"isLocalClientActorThread": true},
	}, neoModelRoute{Provider: "openai", Model: "gpt-5.5"})

	for _, want := range []string{
		"Executor did not acknowledge tool lease",
		"the user's Amp client went offline",
		"without repeating the internal error message",
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("prompt missing local client actor guidance %q:\n%s", want, prompt)
		}
	}
}

func TestNeoSystemPromptIncludesSendMessageWorkflowGuidance(t *testing.T) {
	prompt := neoSystemPrompt(neoInferenceRequest{
		AgentMode: "agg-man",
		Tools:     []neoToolSpec{{Name: "send_message_to_thread"}},
	}, neoModelRoute{Provider: "openai", Model: "gpt-5.5"})

	for _, want := range []string{
		`workflow: "merge_changes"`,
		`workflow: "code_review"`,
		`The canonical merge prompt sent by workflow: "merge_changes" is: "Commit and merge the changes to a single commit on origin/main.`,
		`The canonical code review prompt sent by workflow: "code_review" is: "Review the changes with the code review tool."`,
		`Phrases like "make that change", "do it", "go ahead", or "sounds good" are instructions to implement or continue work -- they are not merge requests.`,
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("prompt missing send_message_to_thread workflow guidance %q:\n%s", want, prompt)
		}
	}

	withoutTool := neoSystemPrompt(neoInferenceRequest{AgentMode: "agg-man"}, neoModelRoute{Provider: "openai", Model: "gpt-5.5"})
	if strings.Contains(withoutTool, `workflow: "merge_changes"`) {
		t.Fatalf("prompt without send_message_to_thread tool should not include workflow guidance:\n%s", withoutTool)
	}
}

func TestNeoSystemPromptMatchesBinaryEnvironmentThreadContext(t *testing.T) {
	prompt := neoSystemPrompt(neoInferenceRequest{
		ThreadID:  "T-019e1055-055c-705b-ae36-73d934ec6a89",
		AgentMode: "deep",
		Environment: map[string]any{
			"ampURL":           "http://127.0.0.1:8317/",
			"workingDirectory": "/Users/test/project",
			"workspaceRoot":    "/Users/test/project",
			"platform":         map[string]any{"os": "darwin", "osVersion": "26.4.1", "cpuArchitecture": "arm64"},
			"trees": []any{map[string]any{
				"repository": map[string]any{"url": "github.com/router-for-me/CLIProxyAPI"},
			}},
		},
	}, neoModelRoute{Provider: "openai", Model: "gpt-5.5"})

	for _, want := range []string{
		"# Environment",
		"Working directory: /Users/test/project",
		"Operating system: darwin (26.4.1) on arm64",
		"Repository: github.com/router-for-me/CLIProxyAPI",
		"Amp Thread URL: http://127.0.0.1:8317/threads/T-019e1055-055c-705b-ae36-73d934ec6a89",
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("environment prompt missing %q:\n%s", want, prompt)
		}
	}
}

func TestNeoUserStateIsInjectedIntoProviderHistory(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)

	actor.mu.Lock()
	actor.messages = []neoMessage{{
		ThreadID:  "T-test",
		MessageID: "M-user",
		Role:      "user",
		Content:   []any{map[string]any{"type": "text", "text": "continue"}},
		UserState: map[string]any{
			"currentlyVisibleFiles": []any{"/Users/test/project/main.go"},
			"activeEditor":          "/Users/test/project/main.go",
			"cursorLocation":        map[string]any{"line": 12, "column": 4},
		},
	}}
	actor.rebuildHistoryLocked()
	history := append([]neoHistoryMessage(nil), actor.history...)
	actor.mu.Unlock()

	if len(history) != 1 {
		t.Fatalf("history = %#v", history)
	}
	if !strings.Contains(history[0].Text, "# User State") || !strings.Contains(history[0].Text, "Currently active editor: /Users/test/project/main.go") || !strings.Contains(history[0].Text, "continue") {
		t.Fatalf("history text missing user state: %q", history[0].Text)
	}
	if len(history[0].Content) != 2 || !strings.Contains(stringValue(mapValue(history[0].Content[0])["text"]), "# User State") || stringValue(mapValue(history[0].Content[1])["text"]) != "continue" {
		t.Fatalf("history content = %#v", history[0].Content)
	}
}

func TestNeoActorManualBashInvocationUsesBinarySchema(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)

	actor.handle(map[string]any{
		"type":   "client_append_manual_bash_invocation",
		"args":   map[string]any{"cmd": "git", "args": []any{"status"}},
		"run":    map[string]any{"status": "done", "result": "clean"},
		"hidden": true,
	})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.messages) != 1 {
		t.Fatalf("messages = %#v", actor.messages)
	}
	content := actor.messages[0].Content
	if len(content) != 1 {
		t.Fatalf("content = %#v", content)
	}
	block := mapValue(content[0])
	if stringValue(block["type"]) != "manual_bash_invocation" || stringValue(mapValue(block["args"])["cmd"]) != "git" || stringValue(mapValue(block["toolRun"])["result"]) != "clean" {
		t.Fatalf("manual bash block = %#v", block)
	}
	if _, exists := block["run"]; exists {
		t.Fatalf("manual bash block should use toolRun, not run: %#v", block)
	}
}

func TestNeoActorBinaryManualBashInvocationPreservesRawFieldsLikeBinary(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)

	actor.handle(map[string]any{
		"type":    "info:manual-bash-invocation",
		"args":    []any{"git", "status"},
		"run":     map[string]any{"status": "done", "result": "ignored client fallback"},
		"toolRun": "raw tool run",
		"hidden":  "yes",
	})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.messages) != 1 {
		t.Fatalf("messages = %#v", actor.messages)
	}
	block := mapValue(actor.messages[0].Content[0])
	if stringValue(block["type"]) != "manual_bash_invocation" {
		t.Fatalf("manual bash block = %#v", block)
	}
	if args := arrayValue(block["args"]); len(args) != 2 || args[0] != "git" || args[1] != "status" {
		t.Fatalf("manual bash args = %#v", block["args"])
	}
	if block["toolRun"] != "raw tool run" || block["hidden"] != "yes" {
		t.Fatalf("manual bash raw fields = %#v", block)
	}
}

func TestNeoActorRebuildHistoryIncludesManualBashInvocation(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)

	actor.mu.Lock()
	actor.messages = []neoMessage{{
		ThreadID:  "T-test",
		MessageID: "M-manual",
		Role:      "info",
		Content: []any{map[string]any{
			"type":    "manual_bash_invocation",
			"args":    map[string]any{"cmd": "git status --short", "cwd": "/tmp/work"},
			"toolRun": map[string]any{"status": "done", "result": map[string]any{"output": " M internal/api/modules/amp/neo_runtime.go", "exitCode": 0}},
		}},
	}}
	actor.rebuildHistoryLocked()
	history := append([]neoHistoryMessage(nil), actor.history...)
	actor.mu.Unlock()

	if len(history) != 1 {
		t.Fatalf("history = %#v", history)
	}
	if history[0].Role != "user" || !strings.Contains(history[0].Text, neoManualBashHistoryReminder) || !strings.Contains(history[0].Text, "<command>git status --short</command>") || !strings.Contains(history[0].Text, "<working_directory>/tmp/work</working_directory>") || !strings.Contains(history[0].Text, "<output> M internal/api/modules/amp/neo_runtime.go</output>") || !strings.Contains(history[0].Text, "<exit_code>0</exit_code>") {
		t.Fatalf("manual bash history = %#v", history[0])
	}
	if len(history[0].Content) != 2 || stringValue(mapValue(history[0].Content[0])["text"]) != neoManualBashHistoryReminder || !strings.Contains(stringValue(mapValue(history[0].Content[1])["text"]), "<command>git status --short</command>") {
		t.Fatalf("manual bash history content = %#v", history[0].Content)
	}
}

func TestNeoActorRebuildHistoryIncludesInfoTextLikeBinary(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)

	actor.mu.Lock()
	actor.messages = []neoMessage{{
		ThreadID:  "T-test",
		MessageID: "M-info",
		Role:      "info",
		Content: []any{
			map[string]any{"type": "text", "text": "You MUST call the skill tool before using this skill."},
			map[string]any{"type": "summary", "summary": map[string]any{"type": "transcript", "summary": "skip transcript marker"}},
		},
	}}
	actor.rebuildHistoryLocked()
	history := append([]neoHistoryMessage(nil), actor.history...)
	actor.mu.Unlock()

	if len(history) != 1 {
		t.Fatalf("history = %#v", history)
	}
	if history[0].Role != "user" || history[0].Text != "You MUST call the skill tool before using this skill." {
		t.Fatalf("info text history = %#v", history[0])
	}
	if len(history[0].Content) != 1 || stringValue(mapValue(history[0].Content[0])["text"]) != "You MUST call the skill tool before using this skill." {
		t.Fatalf("info text content = %#v", history[0].Content)
	}
}

func TestNeoActorRebuildHistoryCombinesInfoTextAndManualBashLikeBinary(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)

	actor.mu.Lock()
	actor.messages = []neoMessage{{
		ThreadID:  "T-test",
		MessageID: "M-info",
		Role:      "info",
		Content: []any{
			map[string]any{"type": "text", "text": "Loaded skill: code-review"},
			map[string]any{
				"type":    "manual_bash_invocation",
				"args":    map[string]any{"cmd": "git status --short", "cwd": "/tmp/work"},
				"toolRun": map[string]any{"status": "done", "result": map[string]any{"output": "clean", "exitCode": 0}},
			},
		},
	}}
	actor.rebuildHistoryLocked()
	history := append([]neoHistoryMessage(nil), actor.history...)
	actor.mu.Unlock()

	if len(history) != 1 {
		t.Fatalf("history = %#v", history)
	}
	if history[0].Role != "user" || !strings.Contains(history[0].Text, "Loaded skill: code-review") || !strings.Contains(history[0].Text, neoManualBashHistoryReminder) || !strings.Contains(history[0].Text, "<command>git status --short</command>") {
		t.Fatalf("combined info history = %#v", history[0])
	}
	if len(history[0].Content) != 3 || stringValue(mapValue(history[0].Content[0])["text"]) != "Loaded skill: code-review" || stringValue(mapValue(history[0].Content[1])["text"]) != neoManualBashHistoryReminder || !strings.Contains(stringValue(mapValue(history[0].Content[2])["text"]), "<output>clean</output>") {
		t.Fatalf("combined info content = %#v", history[0].Content)
	}
}

func TestNeoActorRebuildHistorySkipsHiddenManualBashInvocationLikeBinary(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)

	actor.mu.Lock()
	actor.messages = []neoMessage{{
		ThreadID:  "T-test",
		MessageID: "M-hidden",
		Role:      "info",
		Content: []any{map[string]any{
			"type":    "manual_bash_invocation",
			"args":    map[string]any{"cmd": "git status"},
			"toolRun": map[string]any{"status": "done", "result": map[string]any{"output": "clean", "exitCode": 0}},
			"hidden":  true,
		}},
	}}
	actor.rebuildHistoryLocked()
	history := append([]neoHistoryMessage(nil), actor.history...)
	actor.mu.Unlock()

	if len(history) != 0 {
		t.Fatalf("history = %#v, want hidden manual bash skipped", history)
	}
}

func TestNeoSystemPromptIncludesSkillSnapshotNames(t *testing.T) {
	prompt := neoSystemPrompt(neoInferenceRequest{
		Capabilities: map[string]any{
			"skills": []any{
				map[string]any{"name": "code-review", "description": "Review code", "baseDir": "/skills/code-review"},
				map[string]any{"displayName": "shipping-prs", "description": "Ship PRs", "location": "/skills/shipping-prs/SKILL.md"},
			},
		},
	}, neoModelRoute{Provider: "anthropic", Model: "claude-opus-4-7"})
	if !strings.Contains(prompt, "<description>Review code</description>") || !strings.Contains(prompt, "<location>/skills/shipping-prs/SKILL.md</location>") {
		t.Fatalf("prompt missing snapshot skill names:\n%s", prompt)
	}
}

func TestNeoActorSkillSnapshotAccumulatesBinaryChunksAndResetsBySnapshotID(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)

	actor.updateSkillSnapshot(map[string]any{
		"type":       "executor_skill_snapshot",
		"snapshotId": "snapshot-1",
		"skills":     []any{map[string]any{"name": "alpha"}},
		"isLast":     false,
	})
	actor.updateSkillSnapshot(map[string]any{
		"type":       "executor_skill_snapshot",
		"snapshotId": "snapshot-1",
		"skills":     []any{map[string]any{"name": "beta"}},
		"errors":     []any{map[string]any{"message": "warning"}},
		"isLast":     true,
	})

	actor.mu.Lock()
	skills := firstArray(actor.skillSnapshot["skills"], actor.skillSnapshot["skillInventory"])
	errors := arrayValue(actor.skillSnapshot["errors"])
	skillNames := neoSkillNamesFromAny(actor.skillSnapshot["skills"])
	actor.mu.Unlock()
	if len(skills) != 2 || stringValue(mapValue(skills[0])["name"]) != "alpha" || stringValue(mapValue(skills[1])["name"]) != "beta" {
		t.Fatalf("chunked skills = %#v", skills)
	}
	if len(errors) != 1 || len(skillNames) != 2 {
		t.Fatalf("errors/skillNames = %#v/%#v", errors, skillNames)
	}

	actor.updateSkillSnapshot(map[string]any{
		"type":       "executor_skill_snapshot",
		"snapshotId": "snapshot-2",
		"skills":     []any{map[string]any{"name": "gamma"}},
		"isLast":     true,
	})
	actor.mu.Lock()
	skills = firstArray(actor.skillSnapshot["skills"], actor.skillSnapshot["skillInventory"])
	actor.mu.Unlock()
	if len(skills) != 1 || stringValue(mapValue(skills[0])["name"]) != "gamma" {
		t.Fatalf("new snapshot skills = %#v, want reset to gamma only", skills)
	}
}

func TestNeoSystemPromptUsesExecutorGuidanceSnapshot(t *testing.T) {
	prompt := neoSystemPrompt(neoInferenceRequest{
		AgentMode: "deep",
		Guidance: map[string]any{
			"files": []any{map[string]any{
				"content": "Amp upstream executor guidance",
				"uri":     "file:///Users/test/.config/AGENTS.md",
			}},
		},
		Capabilities: map[string]any{
			"skills": []any{map[string]any{"name": "code-review", "description": "Review code", "baseDir": "/skills/code-review"}},
		},
	}, neoModelRoute{Provider: "openai", Model: "gpt-5.5"})

	if !strings.Contains(prompt, "You are Amp") {
		t.Fatalf("prompt missing base actor prompt:\n%s", prompt)
	}
	if !strings.Contains(prompt, "# AGENTS.md instructions for /Users/test/.config") || !strings.Contains(prompt, "<INSTRUCTIONS>\nAmp upstream executor guidance\n</INSTRUCTIONS>") {
		t.Fatalf("prompt missing executor guidance:\n%s", prompt)
	}
	for _, want := range []string{
		"you don't have to read or search for them",
		"The contents of AGENTS.md files at the root and directories up to the CWD are included automatically.",
		"When working in subdirectories, check for any additional AGENTS.md files that may apply.",
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("prompt missing binary guidance overview %q:\n%s", want, prompt)
		}
	}
	if !strings.Contains(prompt, "### Available skills") || !strings.Contains(prompt, "- code-review: Review code (file: /skills/code-review/SKILL.md)") {
		t.Fatalf("prompt missing fallback skill names:\n%s", prompt)
	}
}

func TestNeoGuidanceBlocksUseBinaryGuidanceLabels(t *testing.T) {
	blocks := strings.Join(neoGuidanceBlocks(neoInferenceRequest{
		Guidance: map[string]any{
			"files": []any{
				map[string]any{
					"content": "Project guidance",
					"uri":     "file:///Users/test/project/AGENTS.md",
				},
				map[string]any{
					"content": "Global guidance",
					"uri":     "file:///Users/test/.config/AGENTS.md",
				},
				map[string]any{
					"content": "Local guidance",
					"uri":     "file:///Users/test/project/AGENTS.local.md",
				},
				map[string]any{
					"content": "System guidance",
					"type":    "system",
					"uri":     "file:///Library/Application%20Support/Amp/AGENTS.md",
				},
				map[string]any{
					"content": "Subtree guidance",
					"type":    "subtree",
					"uri":     "file:///Users/test/project/src/AGENTS.md",
				},
			},
		},
	}, false), "\n\n")

	for _, want := range []string{
		"Contents of AGENTS.md (project instructions):\n<instructions>\nProject guidance\n</instructions>",
		"Contents of AGENTS.md (user's private global instructions for all projects):\n<instructions>\nGlobal guidance\n</instructions>",
		"Contents of AGENTS.local.md (user's private project instructions, not checked in):\n<instructions>\nLocal guidance\n</instructions>",
		"Contents of AGENTS.md (system-wide global instructions for all projects):\n<instructions>\nSystem guidance\n</instructions>",
		"Contents of AGENTS.md (directory-specific instructions for /Users/test/project/src):\n<instructions>\nSubtree guidance\n</instructions>",
	} {
		if !strings.Contains(blocks, want) {
			t.Fatalf("guidance blocks missing %q:\n%s", want, blocks)
		}
	}
}

func TestNeoEnvironmentPlatformTextUsesBinaryHints(t *testing.T) {
	got := neoPlatformText(map[string]any{
		"os":              "windows",
		"osVersion":       "11",
		"cpuArchitecture": "x64",
		"webBrowser":      true,
	})
	want := "windows (11) on x64 (use Windows file paths with backslashes) (running in web browser)"
	if got != want {
		t.Fatalf("platform text = %q, want %q", got, want)
	}
}

func TestNeoActorHandlesGuidanceDiscovery(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)

	actor.handle(map[string]any{
		"type": "executor_guidance_discovery",
		"files": []any{map[string]any{
			"content": "Discovered AGENTS guidance",
			"uri":     "file:///Users/test/project/AGENTS.md",
		}},
	})

	actor.mu.Lock()
	request := actor.inferenceRequestLocked("deep", "xhigh", "")
	actor.mu.Unlock()
	prompt := neoSystemPrompt(request, neoModelRoute{Provider: "openai", Model: "gpt-5.5"})

	if !strings.Contains(prompt, "# AGENTS.md instructions for /Users/test/project") || !strings.Contains(prompt, "Discovered AGENTS guidance") {
		t.Fatalf("prompt missing discovered guidance:\n%s", prompt)
	}
}

func TestNeoActorHandlesClientEditMessage(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.messages = []neoMessage{
		{ThreadID: "T-test", MessageID: "M-user", Role: "user", Content: []any{map[string]any{"type": "text", "text": "old"}}, AgentMode: "smart", Seq: 1},
		{ThreadID: "T-test", MessageID: "M-assistant", Role: "assistant", Content: []any{map[string]any{"type": "text", "text": "answer"}}, Seq: 2},
	}
	actor.history = []neoHistoryMessage{
		{Role: "user", Text: "old"},
		{Role: "assistant", Text: "answer"},
	}
	actor.seq = 3

	actor.handle(map[string]any{
		"type":            "client_edit_message",
		"messageId":       "M-user",
		"editId":          "E-edit",
		"content":         []any{map[string]any{"type": "text", "text": "edited"}},
		"agentMode":       "deep",
		"reasoningEffort": "xhigh",
	})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.messages) != 1 {
		t.Fatalf("messages len = %d, want edited message only", len(actor.messages))
	}
	if got := textFromBlocks(actor.messages[0].Content); got != "edited" {
		t.Fatalf("edited content = %q", got)
	}
	if actor.messages[0].AgentMode != "deep" || actor.messages[0].ReasoningEffort != "xhigh" {
		t.Fatalf("edited mode/effort = %q/%q", actor.messages[0].AgentMode, actor.messages[0].ReasoningEffort)
	}
	if len(actor.history) != 1 || actor.history[0].Text != "edited" {
		t.Fatalf("history = %#v", actor.history)
	}
	if actor.pendingInference == nil || actor.pendingInference.agentMode != "deep" || actor.pendingInference.reasoningEffort != "xhigh" {
		t.Fatalf("pending inference after offline edit = %#v, want deep/xhigh", actor.pendingInference)
	}
}

func TestNeoActorUpdateSettingsPreservesModeWhenPatchOmitsAgentMode(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.settings = map[string]any{"agentMode": "deep", "reasoning.effort": "xhigh"}
	actor.currentAgentMode = "deep"
	actor.currentReasoningEffort = "xhigh"

	actor.updateSettings(map[string]any{"tools.enable": []any{"read_thread"}})

	actor.mu.Lock()
	if actor.currentAgentMode != "deep" {
		t.Fatalf("currentAgentMode = %q, want deep", actor.currentAgentMode)
	}
	if actor.settings["agentMode"] != "deep" || actor.settings["reasoning.effort"] != "xhigh" {
		t.Fatalf("settings lost existing mode/effort: %#v", actor.settings)
	}
	actor.mu.Unlock()

	actor.updateSettings(map[string]any{"agentMode": "smart"})
	actor.mu.Lock()
	if actor.currentAgentMode != "smart" || actor.settings["agentMode"] != "smart" {
		t.Fatalf("explicit smart update was not applied: mode=%q settings=%#v", actor.currentAgentMode, actor.settings)
	}
	if actor.currentReasoningEffort != "high" || actor.settings["reasoning.effort"] != "high" {
		t.Fatalf("smart update did not reset effort to default: effort=%q settings=%#v", actor.currentReasoningEffort, actor.settings)
	}
	actor.mu.Unlock()

	actor.updateSettings(map[string]any{"agentMode": "rush"})
	actor.mu.Lock()
	defer actor.mu.Unlock()
	if actor.currentAgentMode != "rush" || actor.currentReasoningEffort != "none" {
		t.Fatalf("rush update should reset effort to binary default: mode=%q effort=%q", actor.currentAgentMode, actor.currentReasoningEffort)
	}
	if actor.settings["reasoning.effort"] != "none" {
		t.Fatalf("rush settings did not keep binary default effort: %#v", actor.settings)
	}
}

func TestNeoActorReasoningEffortDefaultsByMode(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.currentAgentMode = "smart"
	actor.currentReasoningEffort = "high"
	actor.messages = []neoMessage{{Role: "user", AgentMode: "smart", ReasoningEffort: "max"}}

	if got := actor.reasoningEffortForModeLocked("rush"); got != "none" {
		t.Fatalf("rush effort = %q, want none", got)
	}
	if got := actor.reasoningEffortForModeLocked("deep"); got != "medium" {
		t.Fatalf("deep effort = %q, want medium", got)
	}
	if got := actor.reasoningEffortForModeLocked("smart"); got != "high" {
		t.Fatalf("smart effort = %q, want high", got)
	}
	if got := actor.reasoningEffortForModeLocked("frontier"); got != "" {
		t.Fatalf("unknown mode effort = %q, want empty", got)
	}
	if got := actor.reasoningEffortForModeLocked("nostromo"); got != "low" {
		t.Fatalf("nostromo effort = %q, want low", got)
	}
}

func TestNeoActorFiltersAmpBuiltInToolsByMode(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.tools = map[string]neoToolSpec{
		"Read":                   {Name: "Read"},
		"Grep":                   {Name: "Grep"},
		"glob":                   {Name: "glob"},
		"Glob":                   {Name: "Glob"},
		"finder":                 {Name: "finder"},
		"file_tree":              {Name: "file_tree"},
		"Bash":                   {Name: "Bash"},
		"create_file":            {Name: "create_file"},
		"edit_file":              {Name: "edit_file"},
		"delete_file":            {Name: "delete_file"},
		"get_diagnostics":        {Name: "get_diagnostics"},
		"web_search":             {Name: "web_search"},
		"read_web_page":          {Name: "read_web_page"},
		"read_mcp_resource":      {Name: "read_mcp_resource"},
		"chart":                  {Name: "chart"},
		"read_thread":            {Name: "read_thread"},
		"find_thread":            {Name: "find_thread"},
		"skill":                  {Name: "skill"},
		"oracle":                 {Name: "oracle"},
		"librarian":              {Name: "librarian"},
		"Task":                   {Name: "Task"},
		"task_list":              {Name: "task_list"},
		"todo_write":             {Name: "todo_write"},
		"todo_read":              {Name: "todo_read"},
		"view_media":             {Name: "view_media"},
		"look_at":                {Name: "look_at"},
		"handoff":                {Name: "handoff"},
		"painter":                {Name: "painter"},
		"shell_command":          {Name: "shell_command"},
		"apply_patch":            {Name: "apply_patch"},
		"send_message_to_aggman": {Name: "send_message_to_aggman"},
		"search_documents":       {Name: "search_documents"},
		"get_document":           {Name: "get_document"},
		"docs_read":              {Name: "docs_read"},
		"render_agg_man":         {Name: "render_agg_man"},
		"diff":                   {Name: "diff"},
		"tb__gemini-oracle":      {Name: "tb__gemini-oracle"},
		"code_review":            {Name: "code_review", Meta: map[string]any{"deferred": true}},
		"deferred_custom":        {Name: "deferred_custom", Meta: map[string]any{"deferred": true}},
	}
	requestNames := func(mode string) map[string]bool {
		request := actor.inferenceRequestLocked(mode, "", "")
		names := map[string]bool{}
		for _, tool := range request.Tools {
			names[tool.Name] = true
		}
		return names
	}
	assertMode := func(label string, names map[string]bool, present, absent []string) {
		t.Helper()
		for _, name := range present {
			if !names[name] {
				t.Fatalf("%s tools missing %s: %#v", label, name, names)
			}
		}
		for _, name := range absent {
			if names[name] {
				t.Fatalf("%s tools unexpectedly included %s: %#v", label, name, names)
			}
		}
	}

	deepNames := requestNames("deep")
	assertMode("deep", deepNames,
		[]string{"Task", "read_thread", "shell_command", "apply_patch", "chart", "view_media", "tb__gemini-oracle", "code_review"},
		[]string{"Read", "Grep", "glob", "Glob", "Bash", "create_file", "edit_file", "get_diagnostics", "look_at", "handoff", "task_list", "todo_write", "file_tree", "deferred_custom", "docs_read"})

	smartNames := requestNames("smart")
	assertMode("smart", smartNames,
		[]string{"Read", "Bash", "create_file", "edit_file", "Task", "view_media", "tb__gemini-oracle", "code_review"},
		[]string{"Grep", "glob", "Glob", "delete_file", "get_diagnostics", "shell_command", "apply_patch", "chart", "look_at", "handoff", "task_list", "todo_write", "file_tree", "deferred_custom", "search_documents", "get_document", "docs_read"})

	smartPromptNames := map[string]bool{}
	for _, name := range actor.toolNamesLocked("smart") {
		smartPromptNames[name] = true
	}
	assertMode("smart prompt", smartPromptNames,
		[]string{"Task", "code_review"},
		[]string{"deferred_custom", "chart"})

	rushNames := requestNames("rush")
	assertMode("rush", rushNames,
		[]string{"Task", "shell_command", "apply_patch", "view_media", "read_mcp_resource", "tb__gemini-oracle"},
		[]string{"Read", "Grep", "glob", "Glob", "Bash", "create_file", "edit_file", "get_diagnostics", "chart", "look_at", "handoff", "task_list", "todo_write", "file_tree", "code_review", "deferred_custom", "docs_read"})

	largeNames := requestNames("large")
	assertMode("large", largeNames,
		[]string{"Read", "Bash", "create_file", "edit_file", "Task", "view_media", "tb__gemini-oracle", "code_review"},
		[]string{"Grep", "glob", "Glob", "get_diagnostics", "shell_command", "apply_patch", "chart", "look_at", "handoff", "task_list", "todo_write", "file_tree", "deferred_custom", "docs_read"})

	unknownModeNames := requestNames("frontier")
	assertMode("unknown mode", unknownModeNames,
		[]string{"Read", "Bash", "create_file", "edit_file", "Task", "view_media", "tb__gemini-oracle"},
		[]string{"shell_command", "apply_patch", "chart", "handoff", "code_review", "deferred_custom"})

	aggNames := requestNames("agg-man")
	assertMode("agg-man", aggNames,
		[]string{"read_thread", "web_search", "docs_read", "render_agg_man", "diff", "tb__gemini-oracle"},
		[]string{"Read", "Grep", "glob", "Glob", "Task", "shell_command", "chart", "view_media", "todo_write", "file_tree", "delete_file", "search_documents", "get_document", "code_review", "deferred_custom"})

	nostromoNames := requestNames("nostromo")
	assertMode("nostromo", nostromoNames,
		[]string{"Read", "Bash", "create_file", "edit_file", "Task", "shell_command", "apply_patch", "chart", "view_media", "send_message_to_aggman", "tb__gemini-oracle"},
		[]string{"Grep", "glob", "Glob", "get_diagnostics", "look_at", "handoff", "task_list", "todo_write", "file_tree", "code_review", "deferred_custom", "docs_read"})
}

func TestNeoCodeReviewIsDeferredOnlyLikeBinary(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.tools = map[string]neoToolSpec{
		"code_review": {Name: "code_review"},
	}
	if got := actor.inferenceRequestLocked("smart", "", ""); len(got.Tools) != 0 {
		t.Fatalf("non-deferred code_review tools = %#v, want omitted", got.Tools)
	}
	actor.tools["code_review"] = neoToolSpec{Name: "code_review", Meta: map[string]any{"deferred": true}}
	if got := actor.inferenceRequestLocked("smart", "", ""); len(got.Tools) != 1 || got.Tools[0].Name != "code_review" {
		t.Fatalf("deferred smart code_review tools = %#v, want included", got.Tools)
	}
	if got := actor.inferenceRequestLocked("rush", "", ""); len(got.Tools) != 0 {
		t.Fatalf("rush code_review tools = %#v, want omitted", got.Tools)
	}
}

func TestNeoActorAppliesScaffoldToolCustomization(t *testing.T) {
	useTempNeoThreadStore(t)
	path := filepath.Join(t.TempDir(), "scaffold.yaml")
	if err := os.WriteFile(path, []byte("enableToolSpecs:\n  - name: Task\n    description: custom task runner\n    inputSchema:\n      type: object\n      properties:\n        goal:\n          type: string\n  - name: view_media\ndisableTools:\n  - view_media\n"), 0o600); err != nil {
		t.Fatalf("write scaffold customization: %v", err)
	}

	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.settings["internal.scaffoldCustomizationFile"] = path
	actor.tools = map[string]neoToolSpec{
		"Read":       {Name: "Read", Description: "read files"},
		"Task":       {Name: "Task", Description: "original task"},
		"view_media": {Name: "view_media", Description: "view image"},
	}

	request := actor.inferenceRequestLocked("smart", "", "")
	if len(request.Tools) != 1 || request.Tools[0].Name != "Task" {
		t.Fatalf("tools = %#v, want only Task", request.Tools)
	}
	if request.Tools[0].Description != "custom task runner" {
		t.Fatalf("Task description = %q", request.Tools[0].Description)
	}
	properties := mapValue(request.Tools[0].InputSchema["properties"])
	goal := mapValue(properties["goal"])
	if stringValue(goal["type"]) != "string" {
		t.Fatalf("Task schema = %#v", request.Tools[0].InputSchema)
	}
}

func TestNormalizeNeoToolCallsOmitsEmptyCodeReviewDefaults(t *testing.T) {
	calls := normalizeNeoToolCalls([]neoToolCall{{
		ID:   "TU-codeReviewDefaults",
		Name: "code_review",
		Input: map[string]any{
			"diff_description": "Review the current diff.",
			"checkFilter":      []any{},
			"checkScope":       "",
			"checksOnly":       false,
			"thinking":         "low",
		},
	}})
	if len(calls) != 1 {
		t.Fatalf("normalized calls = %d, want 1", len(calls))
	}
	input := calls[0].Input
	for _, key := range []string{"checkFilter", "checkScope", "checksOnly"} {
		if _, ok := input[key]; ok {
			t.Fatalf("input unexpectedly kept %s: %#v", key, input)
		}
	}
	if got := input["diff_description"]; got != "Review the current diff." {
		t.Fatalf("diff_description = %#v", got)
	}
	if got := input["thinking"]; got != "low" {
		t.Fatalf("thinking = %#v", got)
	}
}

func TestNormalizeNeoToolCallsPreservesCodeReviewCheckSelection(t *testing.T) {
	calls := normalizeNeoToolCalls([]neoToolCall{{
		ID:   "TU-codeReviewFilter",
		Name: "code_review",
		Input: map[string]any{
			"checkFilter": []any{"repo-convention-fit"},
			"checkScope":  "/tmp/example",
			"checksOnly":  true,
		},
	}})
	if len(calls) != 1 {
		t.Fatalf("normalized calls = %d, want 1", len(calls))
	}
	input := calls[0].Input
	if got := input["checkFilter"]; len(arrayValue(got)) != 1 {
		t.Fatalf("checkFilter = %#v", got)
	}
	if got := input["checkScope"]; got != "/tmp/example" {
		t.Fatalf("checkScope = %#v", got)
	}
	if got := input["checksOnly"]; got != true {
		t.Fatalf("checksOnly = %#v", got)
	}
}

func TestNeoActorAppliesToolEnableDisableSettings(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.tools = map[string]neoToolSpec{
		"Read":                    {Name: "Read"},
		"read_thread":             {Name: "read_thread"},
		"shell_command":           {Name: "shell_command"},
		"mcp__git_server__search": {Name: "mcp__git_server__search", Meta: map[string]any{"source": map[string]any{"mcp": "git-server"}}},
		"tb__gemini-oracle":       {Name: "tb__gemini-oracle"},
	}
	namesFor := func(mode string) map[string]bool {
		request := actor.inferenceRequestLocked(mode, "", "")
		names := map[string]bool{}
		for _, tool := range request.Tools {
			names[tool.Name] = true
		}
		return names
	}

	actor.settings = map[string]any{"tools.disable": []any{"builtin:Read", "shell_*", "search"}}
	deepNames := namesFor("deep")
	if deepNames["Read"] || deepNames["shell_command"] || deepNames["mcp__git_server__search"] {
		t.Fatalf("disabled tools leaked through deep mode: %#v", deepNames)
	}
	if !deepNames["read_thread"] || !deepNames["tb__gemini-oracle"] {
		t.Fatalf("disable settings removed unrelated tools: %#v", deepNames)
	}

	actor.settings = map[string]any{"tools.enable": []any{"read_thread", "mcp__git-server__search"}}
	enabledNames := namesFor("deep")
	if !enabledNames["read_thread"] || !enabledNames["mcp__git_server__search"] {
		t.Fatalf("enable settings did not keep selected tools: %#v", enabledNames)
	}
	if enabledNames["shell_command"] || enabledNames["tb__gemini-oracle"] || enabledNames["Read"] {
		t.Fatalf("enable settings allowed unselected tools: %#v", enabledNames)
	}

	actor.settings = map[string]any{"tools.enable": `["read_thread"]`}
	promptNames := map[string]bool{}
	for _, name := range actor.toolNamesLocked("deep") {
		promptNames[name] = true
	}
	if !promptNames["read_thread"] || promptNames["shell_command"] || promptNames["mcp__git_server__search"] {
		t.Fatalf("toolNamesLocked did not apply JSON tools.enable settings: %#v", promptNames)
	}
}

func TestNeoProviderMessagesPreserveImageBlocks(t *testing.T) {
	msg := neoHistoryMessage{
		Role: "user",
		Text: "see this",
		Content: []any{
			map[string]any{"type": "text", "text": "see this"},
			map[string]any{"type": "image", "source": map[string]any{"type": "base64", "media_type": "image/png", "data": "aW1n"}},
		},
	}

	anthropic := anthropicNeoMessages([]neoHistoryMessage{msg})
	anthropicContent := arrayValue(mapValue(anthropic[0])["content"])
	if len(anthropicContent) != 3 || stringValue(mapValue(anthropicContent[2])["type"]) != "image" {
		t.Fatalf("anthropic content did not preserve image: %#v", anthropicContent)
	}
	if label := stringValue(mapValue(anthropicContent[1])["text"]); label != `<attached_image path="image">The following image is from the source above.</attached_image>` {
		t.Fatalf("anthropic image label = %q", label)
	}
	if data := stringValue(mapValue(mapValue(anthropicContent[2])["source"])["data"]); data != "aW1n" {
		t.Fatalf("anthropic image data = %q", data)
	}
	if mediaType := stringValue(mapValue(mapValue(anthropicContent[2])["source"])["media_type"]); mediaType != "image/png" {
		t.Fatalf("anthropic image media_type = %q", mediaType)
	}

	openai := openAINeoMessages([]neoHistoryMessage{msg}, "system")
	openAIContent := arrayValue(mapValue(openai[1])["content"])
	if len(openAIContent) != 3 || stringValue(mapValue(openAIContent[2])["type"]) != "image_url" {
		t.Fatalf("openai content did not preserve image: %#v", openAIContent)
	}
	if url := stringValue(mapValue(mapValue(openAIContent[2])["image_url"])["url"]); url != "data:image/png;base64,aW1n" {
		t.Fatalf("openai image url = %q", url)
	}

	openAIResponses := openAIResponsesNeoInput([]neoHistoryMessage{msg}, "system")
	openAIResponsesContent := arrayValue(mapValue(openAIResponses[1])["content"])
	if len(openAIResponsesContent) != 3 || stringValue(mapValue(openAIResponsesContent[2])["type"]) != "input_image" {
		t.Fatalf("openai responses content did not preserve image: %#v", openAIResponsesContent)
	}
	if url := stringValue(mapValue(openAIResponsesContent[2])["image_url"]); url != "data:image/png;base64,aW1n" {
		t.Fatalf("openai responses image_url = %q", url)
	}

	google := googleNeoContents([]neoHistoryMessage{msg}, "system")
	googleParts := arrayValue(mapValue(google[1])["parts"])
	if len(googleParts) != 3 {
		t.Fatalf("google parts = %#v", googleParts)
	}
	inlineData := mapValue(mapValue(googleParts[2])["inlineData"])
	if stringValue(inlineData["data"]) != "aW1n" || stringValue(inlineData["mimeType"]) != "image/png" {
		t.Fatalf("google inlineData = %#v", inlineData)
	}
}

func TestNeoProviderMessagesFilterThinkingByProviderLikeBinary(t *testing.T) {
	msg := neoHistoryMessage{
		Role: "assistant",
		Text: "final answer",
		ThinkingBlocks: []neoThinkingBlock{
			{Thinking: "openai plan", Signature: "openai-sig", Provider: "openai", ID: "rs-openai"},
			{Thinking: "anthropic plan", Signature: "anthropic-sig", Provider: "anthropic"},
		},
	}

	anthropic := anthropicNeoMessages([]neoHistoryMessage{msg})
	anthropicContent := arrayValue(mapValue(anthropic[0])["content"])
	if len(anthropicContent) != 2 {
		t.Fatalf("anthropic content = %#v", anthropicContent)
	}
	if thinking := mapValue(anthropicContent[0]); stringValue(thinking["type"]) != "thinking" || stringValue(thinking["thinking"]) != "anthropic plan" || stringValue(thinking["signature"]) != "anthropic-sig" {
		t.Fatalf("anthropic thinking = %#v", thinking)
	}
	if text := mapValue(anthropicContent[1]); stringValue(text["text"]) != "final answer" {
		t.Fatalf("anthropic text = %#v", text)
	}

	openAIChat := openAINeoMessages([]neoHistoryMessage{msg}, "")
	openAIContent := arrayValue(mapValue(openAIChat[0])["content"])
	if len(openAIContent) != 2 {
		t.Fatalf("openai chat content = %#v", openAIContent)
	}
	if got := stringValue(mapValue(openAIContent[0])["text"]); got != "Thoughts: openai plan" {
		t.Fatalf("openai thought = %q", got)
	}
	if got := stringValue(mapValue(openAIContent[1])["text"]); got != "final answer" {
		t.Fatalf("openai text = %q", got)
	}

	responsesInput := openAIResponsesNeoInput([]neoHistoryMessage{msg}, "")
	if len(responsesInput) != 2 {
		t.Fatalf("responses input = %#v", responsesInput)
	}
	reasoning := mapValue(responsesInput[0])
	if reasoning["type"] != "reasoning" || reasoning["id"] != "rs-openai" || reasoning["encrypted_content"] != "openai-sig" {
		t.Fatalf("responses reasoning = %#v", reasoning)
	}
	summary := arrayValue(reasoning["summary"])
	if len(summary) != 1 || stringValue(mapValue(summary[0])["text"]) != "openai plan" {
		t.Fatalf("responses summary = %#v", summary)
	}
	message := mapValue(responsesInput[1])
	if message["type"] != "message" || message["role"] != "assistant" || message["content"] != "final answer" {
		t.Fatalf("responses assistant message = %#v", message)
	}
}

func TestNeoProviderMessagesNormalizeInternalImageMediaType(t *testing.T) {
	msg := neoHistoryMessage{
		Role: "user",
		Text: "see this",
		Content: []any{
			map[string]any{"type": "image", "source": map[string]any{"type": "base64", "mediaType": "image/png", "data": "aW1n"}},
		},
	}

	anthropic := anthropicNeoMessages([]neoHistoryMessage{msg})
	anthropicContent := arrayValue(mapValue(anthropic[0])["content"])
	source := mapValue(mapValue(anthropicContent[1])["source"])
	if stringValue(source["media_type"]) != "image/png" {
		t.Fatalf("anthropic image source = %#v", source)
	}
	if _, exists := source["mediaType"]; exists {
		t.Fatalf("anthropic image leaked internal mediaType key: %#v", source)
	}
}

func TestNeoProviderMessagesNormalizeNestedImageBase64Envelope(t *testing.T) {
	msg := neoHistoryMessage{
		Role: "user",
		Content: []any{
			map[string]any{
				"type":       "image",
				"sourcePath": "shot.png",
				"source": map[string]any{
					"type": "base64",
					"base64": map[string]any{
						"media_type": "image/png",
						"data":       "aW1n",
					},
				},
			},
		},
	}

	anthropic := anthropicNeoMessages([]neoHistoryMessage{msg})
	anthropicContent := arrayValue(mapValue(anthropic[0])["content"])
	if len(anthropicContent) != 2 {
		t.Fatalf("anthropic content = %#v", anthropicContent)
	}
	if label := stringValue(mapValue(anthropicContent[0])["text"]); label != `<attached_image path="shot.png">The following image is from the source above.</attached_image>` {
		t.Fatalf("anthropic image label = %q", label)
	}
	source := mapValue(mapValue(anthropicContent[1])["source"])
	if stringValue(source["data"]) != "aW1n" || stringValue(source["media_type"]) != "image/png" {
		t.Fatalf("anthropic image source = %#v", source)
	}

	normalized := neoContentFromBinaryValue(msg.Content)
	image := mapValue(normalized[0])
	normalizedSource := mapValue(image["source"])
	if stringValue(normalizedSource["mediaType"]) != "image/png" || stringValue(normalizedSource["data"]) != "aW1n" {
		t.Fatalf("normalized binary image source = %#v", normalizedSource)
	}
}

func TestInferNeoOpenAIStreamsTextAndToolCalls(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/provider/openai/v1/responses" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		payload := readNeoJSON(r.Body)
		if payload["stream"] != true {
			t.Fatalf("stream = %#v, want true", payload["stream"])
		}
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, _ := w.(http.Flusher)
		for _, chunk := range []string{
			`data: {"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"hel"}`,
			`data: {"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"lo"}`,
			`data: {"type":"response.output_item.added","output_index":1,"item":{"type":"function_call","call_id":"call_1","name":"Bash","arguments":"{\"cmd\":\"pwd\"}"}}`,
			`data: {"type":"response.completed","response":{"usage":{"input_tokens":1,"output_tokens":1},"output":[]}}`,
			`data: [DONE]`,
		} {
			_, _ = w.Write([]byte(chunk + "\n\n"))
			if flusher != nil {
				flusher.Flush()
			}
		}
	}))
	defer upstream.Close()

	var deltas []string
	var toolDeltaID string
	result, err := inferNeoLocalStream(testNeoRuntimeForServer(t, upstream), neoInferenceRequest{
		ThreadID:        "T-test",
		AgentMode:       "deep",
		ReasoningEffort: "xhigh",
		Settings:        map[string]any{"internal.model": "openai/gpt-test"},
		History:         []neoHistoryMessage{{Role: "user", Text: "hi"}},
		Tools:           []neoToolSpec{{Name: "Bash", InputSchema: map[string]any{"type": "object"}}},
	}, func(delta neoInferenceDelta) {
		deltas = append(deltas, delta.Text)
		if delta.ToolCall != nil {
			toolDeltaID = delta.ToolCall.ID
		}
	})
	if err != nil {
		t.Fatalf("inferNeoLocalStream error: %v", err)
	}
	if result.Text != "hello" || strings.Join(deltas, "") != "hello" {
		t.Fatalf("text=%q deltas=%#v", result.Text, deltas)
	}
	if len(result.ToolCalls) != 1 || result.ToolCalls[0].Name != "Bash" || stringValue(result.ToolCalls[0].Input["cmd"]) != "pwd" {
		t.Fatalf("tool calls = %#v", result.ToolCalls)
	}
	if toolDeltaID == "" || result.ToolCalls[0].ID != toolDeltaID || !strings.HasPrefix(toolDeltaID, "TU-") {
		t.Fatalf("streamed tool id = %q final id = %q", toolDeltaID, result.ToolCalls[0].ID)
	}
}

func TestInferNeoOpenAIStreamEmitsDeltaBeforeUpstreamCompletes(t *testing.T) {
	firstSent := make(chan struct{})
	allowFinish := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/provider/openai/v1/responses" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		payload := readNeoJSON(r.Body)
		if payload["stream"] != true {
			t.Fatalf("stream = %#v, want true", payload["stream"])
		}
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, _ := w.(http.Flusher)
		_, _ = w.Write([]byte(`data: {"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"hel"}` + "\n\n"))
		if flusher != nil {
			flusher.Flush()
		}
		close(firstSent)
		select {
		case <-allowFinish:
		case <-time.After(2 * time.Second):
			return
		}
		_, _ = w.Write([]byte(`data: {"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"lo"}` + "\n\n"))
		_, _ = w.Write([]byte(`data: {"type":"response.completed","response":{"usage":{"input_tokens":1,"output_tokens":1},"output":[]}}` + "\n\n"))
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
		if flusher != nil {
			flusher.Flush()
		}
	}))
	defer upstream.Close()

	deltaCh := make(chan string, 2)
	resultCh := make(chan neoInferenceResult, 1)
	errCh := make(chan error, 1)
	go func() {
		result, err := inferNeoLocalStream(testNeoRuntimeForServer(t, upstream), neoInferenceRequest{
			ThreadID:        "T-test",
			AgentMode:       "deep",
			ReasoningEffort: "xhigh",
			Settings:        map[string]any{"internal.model": "openai/gpt-test"},
			History:         []neoHistoryMessage{{Role: "user", Text: "hi"}},
		}, func(delta neoInferenceDelta) {
			deltaCh <- delta.Text
		})
		if err != nil {
			errCh <- err
			return
		}
		resultCh <- result
	}()

	select {
	case <-firstSent:
	case err := <-errCh:
		t.Fatalf("inferNeoLocalStream error before first delta: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("upstream did not send first chunk")
	}

	select {
	case delta := <-deltaCh:
		if delta != "hel" {
			t.Fatalf("first delta = %q, want hel", delta)
		}
	case result := <-resultCh:
		t.Fatalf("stream completed before first delta: %#v", result)
	case err := <-errCh:
		t.Fatalf("inferNeoLocalStream error: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first streamed delta")
	}

	select {
	case result := <-resultCh:
		t.Fatalf("stream completed before upstream finished: %#v", result)
	default:
	}

	close(allowFinish)
	select {
	case result := <-resultCh:
		if result.Text != "hello" {
			t.Fatalf("text=%q, want hello", result.Text)
		}
	case err := <-errCh:
		t.Fatalf("inferNeoLocalStream error: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for completed stream")
	}
}

func TestInferNeoOpenAIStreamHandlesCRLFSSESeparatorsBeforeUpstreamCompletes(t *testing.T) {
	firstSent := make(chan struct{})
	allowFinish := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/provider/openai/v1/responses" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, _ := w.(http.Flusher)
		_, _ = w.Write([]byte("data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"content_index\":0,\"delta\":\"hel\"}\r\n\r\n"))
		if flusher != nil {
			flusher.Flush()
		}
		close(firstSent)
		select {
		case <-allowFinish:
		case <-time.After(2 * time.Second):
			return
		}
		_, _ = w.Write([]byte("data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"content_index\":0,\"delta\":\"lo\"}\r\n\r\n"))
		_, _ = w.Write([]byte("data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":1,\"output_tokens\":1},\"output\":[]}}\r\n\r\n"))
		_, _ = w.Write([]byte("data: [DONE]\r\n\r\n"))
		if flusher != nil {
			flusher.Flush()
		}
	}))
	defer upstream.Close()

	deltaCh := make(chan string, 2)
	resultCh := make(chan neoInferenceResult, 1)
	errCh := make(chan error, 1)
	go func() {
		result, err := inferNeoLocalStream(testNeoRuntimeForServer(t, upstream), neoInferenceRequest{
			ThreadID:        "T-test",
			AgentMode:       "deep",
			ReasoningEffort: "medium",
			Settings:        map[string]any{"internal.model": "openai/gpt-test"},
			History:         []neoHistoryMessage{{Role: "user", Text: "hi"}},
		}, func(delta neoInferenceDelta) {
			deltaCh <- delta.Text
		})
		if err != nil {
			errCh <- err
			return
		}
		resultCh <- result
	}()

	select {
	case <-firstSent:
	case err := <-errCh:
		t.Fatalf("inferNeoLocalStream error before first delta: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("upstream did not send first chunk")
	}

	select {
	case delta := <-deltaCh:
		if delta != "hel" {
			t.Fatalf("first delta = %q, want hel", delta)
		}
	case result := <-resultCh:
		t.Fatalf("stream completed before first delta: %#v", result)
	case err := <-errCh:
		t.Fatalf("inferNeoLocalStream error: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first CRLF streamed delta")
	}

	close(allowFinish)
	select {
	case result := <-resultCh:
		if result.Text != "hello" {
			t.Fatalf("text=%q, want hello", result.Text)
		}
	case err := <-errCh:
		t.Fatalf("inferNeoLocalStream error: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for completed CRLF stream")
	}
}

func TestInferNeoOpenAIResponsesSendsReasoningEffortWithTools(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprintf("stream_%t", stream), func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/api/provider/openai/v1/responses" {
					t.Fatalf("unexpected path %s", r.URL.Path)
				}
				payload := readNeoJSON(r.Body)
				if payload["stream"] != stream {
					t.Fatalf("stream = %#v, want %v", payload["stream"], stream)
				}
				reasoning := mapValue(payload["reasoning"])
				if reasoning["effort"] != "xhigh" || reasoning["summary"] != "auto" {
					t.Fatalf("reasoning = %#v, want xhigh summary auto", reasoning)
				}
				if len(arrayValue(payload["tools"])) == 0 {
					t.Fatalf("tools missing: %#v", payload)
				}
				if numberFrom(payload["max_output_tokens"]) != 128000 {
					t.Fatalf("max_output_tokens = %#v, want 128000", payload["max_output_tokens"])
				}
				if payload["service_tier"] != "priority" {
					t.Fatalf("service_tier = %#v, want priority", payload["service_tier"])
				}
				if stream {
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = w.Write([]byte("data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"content_index\":0,\"delta\":\"ok\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":1,\"output_tokens\":1},\"output\":[]}}\n\ndata: [DONE]\n\n"))
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"}]}]}`))
			}))
			defer upstream.Close()

			request := neoInferenceRequest{
				ThreadID:        "T-test",
				AgentMode:       "deep",
				ReasoningEffort: "xhigh",
				Settings:        map[string]any{"internal.model": "openai/gpt-5.5", "openai.speed": "fast"},
				History:         []neoHistoryMessage{{Role: "user", Text: "hi"}},
				Tools:           []neoToolSpec{{Name: "Bash", InputSchema: map[string]any{"type": "object"}}},
			}

			var (
				result neoInferenceResult
				err    error
			)
			if stream {
				result, err = inferNeoLocalStream(testNeoRuntimeForServer(t, upstream), request, nil)
			} else {
				result, err = inferNeoLocal(testNeoRuntimeForServer(t, upstream), request)
			}
			if err != nil {
				t.Fatalf("inferNeoLocal stream=%v error: %v", stream, err)
			}
			if result.Text != "ok" {
				t.Fatalf("text = %q, want ok", result.Text)
			}
		})
	}
}

func TestOpenAIResponsesServiceTierOnlyUsesFastSpeed(t *testing.T) {
	fastBody := openAIResponsesNeoBody(neoInferenceRequest{
		ThreadID:        "T-test",
		AgentMode:       "smart",
		ReasoningEffort: "high",
		Settings:        map[string]any{"openai.speed": "fast"},
	}, neoModelRoute{Provider: "openai", Model: "gpt-5.5"}, true)
	if fastBody["service_tier"] != "priority" {
		t.Fatalf("fast service_tier = %#v, want priority", fastBody["service_tier"])
	}

	standardBody := openAIResponsesNeoBody(neoInferenceRequest{
		ThreadID:        "T-test",
		AgentMode:       "smart",
		ReasoningEffort: "high",
		Settings:        map[string]any{"openai.speed": "standard"},
	}, neoModelRoute{Provider: "openai", Model: "gpt-5.5"}, true)
	if _, exists := standardBody["service_tier"]; exists {
		t.Fatalf("standard service_tier should be omitted: %#v", standardBody)
	}
}

func TestOpenAIResponsesNeoBodyMatchesBinaryBaseEnvelopeWithoutTools(t *testing.T) {
	body := openAIResponsesNeoBody(neoInferenceRequest{
		ThreadID: "T-test",
		History:  []neoHistoryMessage{{Role: "user", Text: "hi"}},
	}, neoModelRoute{Provider: "openai", Model: "gpt-5.5"}, true)

	tools, exists := body["tools"]
	if !exists {
		t.Fatalf("tools field missing: %#v", body)
	}
	if got := arrayValue(tools); len(got) != 0 {
		t.Fatalf("tools = %#v, want empty array", got)
	}
	if numberFrom(body["max_output_tokens"]) != 128000 {
		t.Fatalf("max_output_tokens = %#v, want 128000", body["max_output_tokens"])
	}
	if body["store"] != false || body["stream"] != true || body["prompt_cache_key"] != "T-test" || body["parallel_tool_calls"] != true {
		t.Fatalf("base envelope mismatch: %#v", body)
	}
	include := arrayValue(body["include"])
	if len(include) != 1 || include[0] != "reasoning.encrypted_content" {
		t.Fatalf("include = %#v, want encrypted reasoning content", include)
	}
	streamOptions := mapValue(body["stream_options"])
	if streamOptions["include_obfuscation"] != false {
		t.Fatalf("stream_options = %#v, want include_obfuscation false", streamOptions)
	}
	reasoning := mapValue(body["reasoning"])
	if reasoning["effort"] != "medium" || reasoning["summary"] != "auto" {
		t.Fatalf("reasoning = %#v, want medium summary auto", reasoning)
	}
}

func TestOpenAIResponsesMaxOutputUsesBinaryOpenAIFallback(t *testing.T) {
	body := openAIResponsesNeoBody(neoInferenceRequest{
		ThreadID: "T-test",
	}, neoModelRoute{Provider: "openai", Model: "gpt-test"}, true)

	if numberFrom(body["max_output_tokens"]) != 128000 {
		t.Fatalf("max_output_tokens = %#v, want 128000", body["max_output_tokens"])
	}
}

func TestOpenAIResponsesRushUsesBinaryNoneReasoningDefault(t *testing.T) {
	body := openAIResponsesNeoBody(neoInferenceRequest{
		ThreadID:        "T-test",
		AgentMode:       "rush",
		ReasoningEffort: defaultNeoReasoningEffort("rush"),
		History:         []neoHistoryMessage{{Role: "user", Text: "hi"}},
	}, neoModelRoute{Provider: "openai", Model: "gpt-5.5"}, true)

	reasoning := mapValue(body["reasoning"])
	if reasoning["effort"] != "none" || reasoning["summary"] != "auto" {
		t.Fatalf("reasoning = %#v, want none summary auto", reasoning)
	}
}

func TestOpenAIChatReasoningEffortPreservesBinaryNone(t *testing.T) {
	if got := openAIReasoningEffort("none"); got != "none" {
		t.Fatalf("openAIReasoningEffort(none) = %q, want none", got)
	}
}

func TestOpenAIResponsesNeoToolsPreserveCustomToolConfig(t *testing.T) {
	body := openAIResponsesNeoBody(neoInferenceRequest{
		ThreadID: "T-test",
		Tools: []neoToolSpec{{
			Name:        "apply_patch",
			Description: "apply a patch",
			OpenAICustomToolConfig: map[string]any{
				"type":       "custom",
				"inputField": "patchText",
				"format":     map[string]any{"type": "grammar", "syntax": "lark", "definition": "start: /.+/"},
			},
		}},
	}, neoModelRoute{Provider: "openai", Model: "gpt-5.5"}, true)

	tools := arrayValue(body["tools"])
	if len(tools) != 1 {
		t.Fatalf("tools = %#v, want one custom tool", tools)
	}
	tool := mapValue(tools[0])
	if tool["type"] != "custom" || tool["name"] != "apply_patch" {
		t.Fatalf("tool = %#v, want custom apply_patch", tool)
	}
	if _, hasParameters := tool["parameters"]; hasParameters {
		t.Fatalf("custom tool unexpectedly had parameters: %#v", tool)
	}
	if format := mapValue(tool["format"]); format["type"] != "grammar" {
		t.Fatalf("format = %#v, want grammar", format)
	}
}

func TestOpenAIResponsesNeoToolsNormalizeFunctionSchemaLikeBinary(t *testing.T) {
	body := openAIResponsesNeoBody(neoInferenceRequest{
		ThreadID: "T-test",
		Tools: []neoToolSpec{{
			Name:        "Bash",
			Description: "run shell",
			InputSchema: map[string]any{
				"type":                 "object",
				"properties":           map[string]any{"cmd": map[string]any{"type": "string"}},
				"additionalProperties": false,
				"$defs":                map[string]any{"unused": map[string]any{"type": "string"}},
			},
		}},
	}, neoModelRoute{Provider: "openai", Model: "gpt-5.5"}, true)

	tools := arrayValue(body["tools"])
	if len(tools) != 1 {
		t.Fatalf("tools = %#v, want one function tool", tools)
	}
	tool := mapValue(tools[0])
	if tool["type"] != "function" || tool["name"] != "Bash" || tool["strict"] != false {
		t.Fatalf("tool = %#v, want function Bash", tool)
	}
	parameters := mapValue(tool["parameters"])
	if parameters["type"] != "object" || parameters["additionalProperties"] != true {
		t.Fatalf("parameters = %#v, want object with additionalProperties true", parameters)
	}
	if _, hasDefs := parameters["$defs"]; hasDefs {
		t.Fatalf("parameters retained unsupported top-level schema key: %#v", parameters)
	}
	if required := arrayValue(parameters["required"]); len(required) != 0 {
		t.Fatalf("required = %#v, want empty array", required)
	}
	properties := mapValue(parameters["properties"])
	if cmd := mapValue(properties["cmd"]); cmd["type"] != "string" {
		t.Fatalf("properties = %#v, want cmd string schema", properties)
	}
}

func TestOpenAIResponsesNeoInputReplaysCustomToolCalls(t *testing.T) {
	input := openAIResponsesNeoInput([]neoHistoryMessage{
		{
			Role: "assistant",
			ToolCalls: []neoToolCall{{
				ID:               "TU-patch",
				Name:             "apply_patch",
				Input:            map[string]any{"patchText": "*** Begin Patch"},
				CustomInputField: "patchText",
			}},
		},
		{Role: "tool", ToolCallID: "TU-patch", Text: "applied"},
	}, "")

	if len(input) != 2 {
		t.Fatalf("input = %#v, want custom call and output", input)
	}
	call := mapValue(input[0])
	if call["type"] != "custom_tool_call" || call["name"] != "apply_patch" || call["input"] != "*** Begin Patch" {
		t.Fatalf("custom call = %#v", call)
	}
	output := mapValue(input[1])
	if output["type"] != "custom_tool_call_output" || output["call_id"] != "TU-patch" || output["output"] != "applied" {
		t.Fatalf("custom output = %#v", output)
	}
}

func TestParseNeoOpenAIResponsesResultCustomToolCall(t *testing.T) {
	result, err := parseNeoOpenAIResponsesResult(map[string]any{
		"output": []any{map[string]any{
			"type":    "custom_tool_call",
			"call_id": "TU-patch",
			"name":    "apply_patch",
			"input":   "*** Begin Patch",
		}},
	}, neoModelRoute{Provider: "openai", Model: "gpt-5.5"}, []neoToolSpec{{
		Name:                   "apply_patch",
		OpenAICustomToolConfig: map[string]any{"type": "custom", "inputField": "patchText"},
	}})
	if err != nil {
		t.Fatalf("parseNeoOpenAIResponsesResult error: %v", err)
	}

	if len(result.ToolCalls) != 1 {
		t.Fatalf("tool calls = %#v, want one", result.ToolCalls)
	}
	call := result.ToolCalls[0]
	if call.ID != "TU-patch" || call.Name != "apply_patch" || call.CustomInputField != "patchText" || call.Input["patchText"] != "*** Begin Patch" {
		t.Fatalf("custom tool call = %#v", call)
	}
}

func TestParseNeoOpenAIResponsesResultKeepsMalformedFunctionCallArgumentsIncomplete(t *testing.T) {
	result, err := parseNeoOpenAIResponsesResult(map[string]any{
		"output": []any{map[string]any{
			"type":      "function_call",
			"call_id":   "call_1",
			"name":      "Bash",
			"arguments": `{"cmd":"pwd"`,
		}},
	}, neoModelRoute{Provider: "openai", Model: "gpt-5.5"}, nil)
	if err != nil {
		t.Fatalf("parseNeoOpenAIResponsesResult error: %v", err)
	}
	if len(result.ToolCalls) != 1 {
		t.Fatalf("tool calls = %#v, want one", result.ToolCalls)
	}
	call := result.ToolCalls[0]
	if !call.Incomplete || call.PartialJSON != `{"cmd":"pwd"` || stringValue(call.InputIncomplete["cmd"]) != "pwd" {
		t.Fatalf("tool call = %#v, want incomplete partial JSON", call)
	}
	block := neoToolUseBlock(call, true)
	if boolValue(block["complete"]) {
		t.Fatalf("tool block complete = %#v, want false", block)
	}
	if stringValue(mapValue(block["inputPartialJSON"])["json"]) != `{"cmd":"pwd"` || stringValue(mapValue(block["inputIncomplete"])["cmd"]) != "pwd" {
		t.Fatalf("tool block partial fields = %#v", block)
	}
	emptyInput, emptyPartialJSON, emptyIncomplete, emptyBroken := parseOpenAIResponsesFunctionArguments("")
	if !emptyBroken || emptyPartialJSON != "" || len(emptyInput) != 0 || len(emptyIncomplete) != 0 {
		t.Fatalf("empty arguments parsed as input=%#v partial=%q incomplete=%#v broken=%v, want incomplete empty object", emptyInput, emptyPartialJSON, emptyIncomplete, emptyBroken)
	}
}

func TestFinishAssistantMessageDoesNotLeaseIncompleteToolCall(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := rt.store.ensureThreadActor("T-incomplete-tool")
	result := neoInferenceResult{
		Provider: "openai",
		Model:    "gpt-5.5",
		ToolCalls: []neoToolCall{{
			ID:              "TU-incomplete",
			Name:            "Bash",
			Input:           map[string]any{},
			PartialJSON:     `{"cmd":"pwd"`,
			InputIncomplete: map[string]any{"cmd": "pwd"},
			Incomplete:      true,
		}},
	}

	actor.finishAssistantMessageWithOptions("M-assistant", result, "deep", "xhigh", false, "")

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.pendingTools) != 0 {
		t.Fatalf("pending tools = %#v, want none for incomplete call", actor.pendingTools)
	}
	if actor.agentState != "idle" {
		t.Fatalf("agent state = %q, want idle", actor.agentState)
	}
	if len(actor.messages) != 1 {
		t.Fatalf("messages = %#v, want one assistant message", actor.messages)
	}
	var block map[string]any
	for _, raw := range actor.messages[0].Content {
		candidate := mapValue(raw)
		if stringValue(candidate["type"]) == "tool_use" {
			block = candidate
			break
		}
	}
	if len(block) == 0 {
		t.Fatalf("assistant content = %#v, want tool_use block", actor.messages[0].Content)
	}
	if boolValue(block["complete"]) || stringValue(mapValue(block["inputPartialJSON"])["json"]) != `{"cmd":"pwd"` {
		t.Fatalf("stored tool block = %#v, want incomplete partial JSON", block)
	}
}

func TestInferNeoOpenAIResponsesStreamCustomToolCall(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/provider/openai/v1/responses" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		payload := readNeoJSON(r.Body)
		tools := arrayValue(payload["tools"])
		if len(tools) != 1 || mapValue(tools[0])["type"] != "custom" {
			t.Fatalf("tools = %#v, want custom tool", tools)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"custom_tool_call\",\"call_id\":\"TU-patch\",\"name\":\"apply_patch\",\"input\":\"\"}}\n\n"))
		_, _ = w.Write([]byte("data: {\"type\":\"response.custom_tool_call_input.delta\",\"output_index\":0,\"delta\":\"*** Begin\"}\n\n"))
		_, _ = w.Write([]byte("data: {\"type\":\"response.custom_tool_call_input.delta\",\"output_index\":0,\"delta\":\" Patch\"}\n\n"))
		_, _ = w.Write([]byte("data: {\"type\":\"response.custom_tool_call_input.done\",\"output_index\":0,\"input\":\"*** Begin Patch\"}\n\n"))
		_, _ = w.Write([]byte("data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"custom_tool_call\",\"call_id\":\"TU-patch\",\"name\":\"apply_patch\",\"input\":\"*** Begin Patch\"}}\n\n"))
		_, _ = w.Write([]byte("data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":1,\"output_tokens\":1},\"output\":[]}}\n\ndata: [DONE]\n\n"))
	}))
	defer upstream.Close()

	var lastToolDelta *neoToolCallDelta
	result, err := inferNeoLocalStream(testNeoRuntimeForServer(t, upstream), neoInferenceRequest{
		ThreadID:        "T-test",
		AgentMode:       "deep",
		ReasoningEffort: "xhigh",
		Settings:        map[string]any{"internal.model": "openai/gpt-5.5"},
		History:         []neoHistoryMessage{{Role: "user", Text: "hi"}},
		Tools: []neoToolSpec{{
			Name:                   "apply_patch",
			Description:            "apply a patch",
			OpenAICustomToolConfig: map[string]any{"type": "custom", "inputField": "patchText"},
		}},
	}, func(delta neoInferenceDelta) {
		if delta.ToolCall != nil {
			copy := *delta.ToolCall
			lastToolDelta = &copy
		}
	})
	if err != nil {
		t.Fatalf("inferNeoLocalStream error: %v", err)
	}
	if len(result.ToolCalls) != 1 || result.ToolCalls[0].CustomInputField != "patchText" || result.ToolCalls[0].Input["patchText"] != "*** Begin Patch" {
		t.Fatalf("result tool calls = %#v", result.ToolCalls)
	}
	if lastToolDelta == nil || lastToolDelta.CustomInputField != "patchText" || lastToolDelta.Input["patchText"] != "*** Begin Patch" {
		t.Fatalf("last tool delta = %#v", lastToolDelta)
	}
}

func TestInferNeoOpenAIResponsesStreamHandlesReasoningTextEvents(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/provider/openai/v1/responses" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"reasoning\",\"id\":\"rs_1\",\"summary\":[],\"encrypted_content\":\"sig\"}}\n\n"))
		_, _ = w.Write([]byte("data: {\"type\":\"response.content_part.added\",\"output_index\":0,\"content_index\":0,\"part\":{\"type\":\"reasoning_text\",\"text\":\"plan \"}}\n\n"))
		_, _ = w.Write([]byte("data: {\"type\":\"response.reasoning_text.delta\",\"output_index\":0,\"content_index\":0,\"delta\":\"then act\"}\n\n"))
		_, _ = w.Write([]byte("data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"reasoning\",\"id\":\"rs_1\",\"summary\":[],\"content\":[{\"type\":\"reasoning_text\",\"text\":\"plan then act\"}],\"encrypted_content\":\"sig\"}}\n\n"))
		_, _ = w.Write([]byte("data: {\"type\":\"response.output_text.done\",\"output_index\":1,\"content_index\":0,\"text\":\"done\"}\n\n"))
		_, _ = w.Write([]byte("data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":1,\"output_tokens\":1},\"output\":[]}}\n\ndata: [DONE]\n\n"))
	}))
	defer upstream.Close()

	var thinking strings.Builder
	var text strings.Builder
	result, err := inferNeoLocalStream(testNeoRuntimeForServer(t, upstream), neoInferenceRequest{
		ThreadID:        "T-test",
		AgentMode:       "deep",
		ReasoningEffort: "xhigh",
		Settings:        map[string]any{"internal.model": "openai/gpt-5.5"},
		History:         []neoHistoryMessage{{Role: "user", Text: "hi"}},
	}, func(delta neoInferenceDelta) {
		thinking.WriteString(delta.Thinking)
		text.WriteString(delta.Text)
	})
	if err != nil {
		t.Fatalf("inferNeoLocalStream error: %v", err)
	}
	if result.Text != "done" || text.String() != "done" {
		t.Fatalf("text result=%q delta=%q, want done", result.Text, text.String())
	}
	if len(result.ThinkingBlocks) != 1 || result.ThinkingBlocks[0].Thinking != "plan then act" || result.ThinkingBlocks[0].Signature != "sig" || result.ThinkingBlocks[0].Provider != "openai" || result.ThinkingBlocks[0].ID != "rs_1" {
		t.Fatalf("thinking blocks = %#v", result.ThinkingBlocks)
	}
	if thinking.String() != "plan then act" {
		t.Fatalf("thinking delta = %q", thinking.String())
	}
}

func TestInferNeoOpenAIResponsesStreamHandlesContentPartDone(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/provider/openai/v1/responses" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"type\":\"response.content_part.added\",\"output_index\":0,\"content_index\":0,\"part\":{\"type\":\"output_text\",\"text\":\"\"}}\n\n"))
		_, _ = w.Write([]byte("data: {\"type\":\"response.content_part.done\",\"output_index\":0,\"content_index\":0,\"part\":{\"type\":\"output_text\",\"text\":\"done from part\"}}\n\n"))
		_, _ = w.Write([]byte("data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":1,\"output_tokens\":1},\"output\":[]}}\n\ndata: [DONE]\n\n"))
	}))
	defer upstream.Close()

	var text strings.Builder
	result, err := inferNeoLocalStream(testNeoRuntimeForServer(t, upstream), neoInferenceRequest{
		ThreadID:        "T-test",
		AgentMode:       "deep",
		ReasoningEffort: "xhigh",
		Settings:        map[string]any{"internal.model": "openai/gpt-5.5"},
		History:         []neoHistoryMessage{{Role: "user", Text: "hi"}},
	}, func(delta neoInferenceDelta) {
		text.WriteString(delta.Text)
	})
	if err != nil {
		t.Fatalf("inferNeoLocalStream error: %v", err)
	}
	if result.Text != "done from part" || text.String() != "done from part" {
		t.Fatalf("text result=%q delta=%q, want done from part", result.Text, text.String())
	}
}

func TestInferNeoOpenAIResponsesStreamUsesDoneTextForFinalResult(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/provider/openai/v1/responses" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"content_index\":0,\"delta\":\"hel\"}\n\n"))
		_, _ = w.Write([]byte("data: {\"type\":\"response.output_text.done\",\"output_index\":0,\"content_index\":0,\"text\":\"hello\"}\n\n"))
		_, _ = w.Write([]byte("data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":1,\"output_tokens\":1},\"output\":[]}}\n\ndata: [DONE]\n\n"))
	}))
	defer upstream.Close()

	var text strings.Builder
	result, err := inferNeoLocalStream(testNeoRuntimeForServer(t, upstream), neoInferenceRequest{
		ThreadID:        "T-test",
		AgentMode:       "deep",
		ReasoningEffort: "xhigh",
		Settings:        map[string]any{"internal.model": "openai/gpt-5.5"},
		History:         []neoHistoryMessage{{Role: "user", Text: "hi"}},
	}, func(delta neoInferenceDelta) {
		text.WriteString(delta.Text)
	})
	if err != nil {
		t.Fatalf("inferNeoLocalStream error: %v", err)
	}
	if text.String() != "hello" || result.Text != "hello" {
		t.Fatalf("text result=%q delta=%q, want hello", result.Text, text.String())
	}
}

func TestInferNeoOpenAIResponsesRejectsFailedStatus(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/provider/openai/v1/responses" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"failed","error":{"message":"model failed"},"output":[]}`))
	}))
	defer upstream.Close()

	_, err := inferNeoLocal(testNeoRuntimeForServer(t, upstream), neoInferenceRequest{
		ThreadID:        "T-test",
		AgentMode:       "deep",
		ReasoningEffort: "xhigh",
		Settings:        map[string]any{"internal.model": "openai/gpt-5.5"},
		History:         []neoHistoryMessage{{Role: "user", Text: "hi"}},
	})
	if err == nil || !strings.Contains(err.Error(), "model failed") {
		t.Fatalf("error = %v, want model failed", err)
	}
}

func TestInferNeoOpenAIResponsesStreamRejectsIncompleteStatus(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/provider/openai/v1/responses" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"content_index\":0,\"delta\":\"partial\"}\n\n"))
		_, _ = w.Write([]byte("data: {\"type\":\"response.completed\",\"response\":{\"status\":\"incomplete\",\"incomplete_details\":{\"reason\":\"max_output_tokens\"},\"usage\":{\"input_tokens\":1,\"output_tokens\":1},\"output\":[]}}\n\ndata: [DONE]\n\n"))
	}))
	defer upstream.Close()

	_, err := inferNeoLocalStream(testNeoRuntimeForServer(t, upstream), neoInferenceRequest{
		ThreadID:        "T-test",
		AgentMode:       "deep",
		ReasoningEffort: "xhigh",
		Settings:        map[string]any{"internal.model": "openai/gpt-5.5"},
		History:         []neoHistoryMessage{{Role: "user", Text: "hi"}},
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "max_output_tokens") {
		t.Fatalf("error = %v, want max_output_tokens", err)
	}
}

func TestInferNeoOpenAIResponsesErrorsOnUnsupportedOutputItem(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/provider/openai/v1/responses" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"output":[{"type":"image_generation_call","id":"ig_1"}]}`))
	}))
	defer upstream.Close()

	_, err := inferNeoLocal(testNeoRuntimeForServer(t, upstream), neoInferenceRequest{
		ThreadID:        "T-test",
		AgentMode:       "deep",
		ReasoningEffort: "xhigh",
		Settings:        map[string]any{"internal.model": "openai/gpt-test"},
		History:         []neoHistoryMessage{{Role: "user", Text: "hi"}},
	})
	if err == nil || !strings.Contains(err.Error(), "unsupported content block type image_generation_call") {
		t.Fatalf("error = %v, want unsupported image_generation_call", err)
	}
}

func TestInferNeoOpenAIResponsesStreamErrorsOnUnsupportedOutputItem(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/provider/openai/v1/responses" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"computer_call\",\"id\":\"comp_1\"}}\n\n"))
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer upstream.Close()

	_, err := inferNeoLocalStream(testNeoRuntimeForServer(t, upstream), neoInferenceRequest{
		ThreadID:        "T-test",
		AgentMode:       "deep",
		ReasoningEffort: "xhigh",
		Settings:        map[string]any{"internal.model": "openai/gpt-test"},
		History:         []neoHistoryMessage{{Role: "user", Text: "hi"}},
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "unsupported content block type computer_call") {
		t.Fatalf("error = %v, want unsupported computer_call", err)
	}
}

func TestParseNeoOpenAIResponsesResultReadsReasoningTextContent(t *testing.T) {
	result, err := parseNeoOpenAIResponsesResult(map[string]any{
		"output": []any{map[string]any{
			"type":              "reasoning",
			"id":                "rs_content",
			"encrypted_content": "sig",
			"content": []any{map[string]any{
				"type": "reasoning_text",
				"text": "reasoned from content",
			}},
		}},
	}, neoModelRoute{Provider: "openai", Model: "gpt-5.5"}, nil)
	if err != nil {
		t.Fatalf("parseNeoOpenAIResponsesResult error: %v", err)
	}

	if len(result.ThinkingBlocks) != 1 || result.ThinkingBlocks[0].Thinking != "reasoned from content" || result.ThinkingBlocks[0].Signature != "sig" || result.ThinkingBlocks[0].Provider != "openai" || result.ThinkingBlocks[0].ID != "rs_content" {
		t.Fatalf("thinking blocks = %#v", result.ThinkingBlocks)
	}
}

func TestInferNeoOpenAIResponsesFallsBackToChatCompletionsWhenUnsupported(t *testing.T) {
	responsesCalls := 0
	chatCalls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/provider/openai/v1/responses":
			responsesCalls++
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":{"message":"responses endpoint not found"}}`))
		case "/api/provider/openai/v1/chat/completions":
			chatCalls++
			payload := readNeoJSON(r.Body)
			if payload["stream"] != false {
				t.Fatalf("chat fallback stream = %#v, want false", payload["stream"])
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"chat fallback"}}]}`))
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer upstream.Close()

	result, err := inferNeoLocal(testNeoRuntimeForServer(t, upstream), neoInferenceRequest{
		ThreadID:        "T-test",
		AgentMode:       "deep",
		ReasoningEffort: "xhigh",
		Settings:        map[string]any{"internal.model": "openai/gpt-test"},
		History:         []neoHistoryMessage{{Role: "user", Text: "hi"}},
	})
	if err != nil {
		t.Fatalf("inferNeoLocal error: %v", err)
	}
	if result.Text != "chat fallback" || responsesCalls != 1 || chatCalls != 1 {
		t.Fatalf("result=%#v responsesCalls=%d chatCalls=%d", result, responsesCalls, chatCalls)
	}
}

func TestInferNeoOpenAIStreamFallsBackToNonStreamOnEmptyStream(t *testing.T) {
	streamCalls := 0
	nonStreamCalls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/provider/openai/v1/responses" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		payload := readNeoJSON(r.Body)
		if payload["stream"] == true {
			streamCalls++
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":{"message":"empty_stream: upstream stream closed before first payload","type":"server_error","code":"internal_server_error"}}`))
			return
		}

		nonStreamCalls++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"fallback text"}]},{"type":"function_call","call_id":"call_1","name":"Bash","arguments":"{\"cmd\":\"pwd\"}"}]}`))
	}))
	defer upstream.Close()

	deltaCalls := 0
	result, err := inferNeoLocalStream(testNeoRuntimeForServer(t, upstream), neoInferenceRequest{
		ThreadID:        "T-test",
		AgentMode:       "deep",
		ReasoningEffort: "xhigh",
		Settings:        map[string]any{"internal.model": "openai/gpt-test"},
		History:         []neoHistoryMessage{{Role: "user", Text: "hi"}},
		Tools:           []neoToolSpec{{Name: "Bash", InputSchema: map[string]any{"type": "object"}}},
	}, func(delta neoInferenceDelta) {
		deltaCalls++
	})
	if err != nil {
		t.Fatalf("inferNeoLocalStream error: %v", err)
	}
	if result.Text != "fallback text" {
		t.Fatalf("text=%q, want fallback text", result.Text)
	}
	if len(result.ToolCalls) != 1 || result.ToolCalls[0].Name != "Bash" || stringValue(result.ToolCalls[0].Input["cmd"]) != "pwd" {
		t.Fatalf("tool calls = %#v", result.ToolCalls)
	}
	if streamCalls != 1 || nonStreamCalls != 1 {
		t.Fatalf("streamCalls=%d nonStreamCalls=%d, want 1/1", streamCalls, nonStreamCalls)
	}
	if deltaCalls != 0 {
		t.Fatalf("deltaCalls=%d, want 0 for non-stream fallback", deltaCalls)
	}
}

func TestInferNeoOpenAIStreamFallsBackToNonStreamOnEmptyProviderVariants(t *testing.T) {
	variants := []struct {
		name        string
		contentType string
		statusCode  int
		streamBody  string
	}{
		{
			name:        "sse_event_error",
			contentType: "text/event-stream",
			streamBody:  "event: error\ndata: {\"message\":\"empty_stream: upstream stream closed before first payload\"}\n\n",
		},
		{
			name:        "empty_openai_delta",
			contentType: "text/event-stream",
			streamBody:  "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"content_index\":0,\"delta\":\"\"}\n\ndata: [DONE]\n\n",
		},
		{
			name:        "non_sse_empty_error_body",
			contentType: "application/json",
			streamBody:  `{"error":{"message":"empty_stream: upstream stream closed before first payload"}}`,
		},
		{
			name:        "provider_408_missing_response_completed",
			contentType: "application/json",
			statusCode:  http.StatusRequestTimeout,
			streamBody:  `{"error":{"message":"stream error: stream disconnected before completion: stream closed before response.completed","type":"invalid_request_error"}}`,
		},
	}

	for _, tt := range variants {
		t.Run(tt.name, func(t *testing.T) {
			streamCalls := 0
			nonStreamCalls := 0
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/api/provider/openai/v1/responses" {
					t.Fatalf("unexpected path %s", r.URL.Path)
				}
				payload := readNeoJSON(r.Body)
				if payload["stream"] == true {
					streamCalls++
					w.Header().Set("Content-Type", tt.contentType)
					if tt.statusCode != 0 {
						w.WriteHeader(tt.statusCode)
					}
					_, _ = w.Write([]byte(tt.streamBody))
					return
				}

				nonStreamCalls++
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"fallback text"}]}]}`))
			}))
			defer upstream.Close()

			deltaCalls := 0
			result, err := inferNeoLocalStream(testNeoRuntimeForServer(t, upstream), neoInferenceRequest{
				ThreadID:        "T-test",
				AgentMode:       "deep",
				ReasoningEffort: "xhigh",
				Settings:        map[string]any{"internal.model": "openai/gpt-test"},
				History:         []neoHistoryMessage{{Role: "user", Text: "hi"}},
			}, func(delta neoInferenceDelta) {
				deltaCalls++
			})
			if err != nil {
				t.Fatalf("inferNeoLocalStream error: %v", err)
			}
			if result.Text != "fallback text" {
				t.Fatalf("text=%q, want fallback text", result.Text)
			}
			if streamCalls != 1 || nonStreamCalls != 1 {
				t.Fatalf("streamCalls=%d nonStreamCalls=%d, want 1/1", streamCalls, nonStreamCalls)
			}
			if deltaCalls != 0 {
				t.Fatalf("deltaCalls=%d, want 0 for non-stream fallback", deltaCalls)
			}
		})
	}
}

func TestInferNeoOpenAIStreamDoesNotFallbackAfterPartialContent(t *testing.T) {
	streamCalls := 0
	nonStreamCalls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/provider/openai/v1/responses" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		payload := readNeoJSON(r.Body)
		if payload["stream"] == true {
			streamCalls++
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"content_index\":0,\"delta\":\"partial\"}\n\n"))
			_, _ = w.Write([]byte("event: error\ndata: {\"message\":\"stream error: stream disconnected before completion: stream closed before response.completed\"}\n\n"))
			return
		}

		nonStreamCalls++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"fallback text"}]}]}`))
	}))
	defer upstream.Close()

	deltaCalls := 0
	_, err := inferNeoLocalStream(testNeoRuntimeForServer(t, upstream), neoInferenceRequest{
		ThreadID:        "T-test",
		AgentMode:       "deep",
		ReasoningEffort: "xhigh",
		Settings:        map[string]any{"internal.model": "openai/gpt-test"},
		History:         []neoHistoryMessage{{Role: "user", Text: "hi"}},
	}, func(delta neoInferenceDelta) {
		if delta.Text == "partial" {
			deltaCalls++
		}
	})
	if err == nil {
		t.Fatal("expected partial stream error")
	}
	if streamCalls != 1 || nonStreamCalls != 0 {
		t.Fatalf("streamCalls=%d nonStreamCalls=%d, want 1/0", streamCalls, nonStreamCalls)
	}
	if deltaCalls != 1 {
		t.Fatalf("deltaCalls=%d, want 1 streamed delta before error", deltaCalls)
	}
}

func TestInferNeoAnthropicStreamFallsBackToNonStreamOnEmptyPayload(t *testing.T) {
	streamCalls := 0
	nonStreamCalls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/provider/anthropic/v1/messages" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		payload := readNeoJSON(r.Body)
		if payload["stream"] == true {
			streamCalls++
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("event: message_start\ndata: {\"type\":\"message_start\",\"message\":{}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"))
			return
		}

		nonStreamCalls++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"content":[{"type":"text","text":"anthropic fallback"}]}`))
	}))
	defer upstream.Close()

	deltaCalls := 0
	result, err := inferNeoLocalStream(testNeoRuntimeForServer(t, upstream), neoInferenceRequest{
		ThreadID:  "T-test",
		AgentMode: "smart",
		Settings:  map[string]any{"internal.model": "anthropic/claude-test"},
		History:   []neoHistoryMessage{{Role: "user", Text: "hi"}},
	}, func(delta neoInferenceDelta) {
		deltaCalls++
	})
	if err != nil {
		t.Fatalf("inferNeoLocalStream error: %v", err)
	}
	if result.Text != "anthropic fallback" {
		t.Fatalf("text=%q, want anthropic fallback", result.Text)
	}
	if streamCalls != 1 || nonStreamCalls != 1 {
		t.Fatalf("streamCalls=%d nonStreamCalls=%d, want 1/1", streamCalls, nonStreamCalls)
	}
	if deltaCalls != 0 {
		t.Fatalf("deltaCalls=%d, want 0 for non-stream fallback", deltaCalls)
	}
}

func TestInferNeoAnthropicStreamMatchesBinaryRequestEnvelope(t *testing.T) {
	calls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/provider/anthropic/v1/messages" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		payload := readNeoJSON(r.Body)
		if payload["stream"] != true {
			t.Fatalf("stream = %#v, want true", payload["stream"])
		}
		if got := numberFrom(payload["max_tokens"]); got != 64000 {
			t.Fatalf("max_tokens = %d, want binary thread maxTokens override 64000; payload=%#v", got, payload)
		}
		if _, ok := payload["context_management"]; ok {
			t.Fatalf("request included context_management, but current Amp binary omits it: %#v", payload)
		}
		if beta := r.Header.Get("Anthropic-Beta"); strings.Contains(beta, "context-management") {
			t.Fatalf("Anthropic-Beta = %q, want no context-management beta", beta)
		}

		calls++
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: message_start\n" +
			`data: {"type":"message_start","message":{"usage":{"input_tokens":3}}}` + "\n\n" +
			"event: content_block_start\n" +
			`data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}` + "\n\n" +
			"event: content_block_delta\n" +
			`data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"retried"}}` + "\n\n" +
			"event: message_stop\n" +
			`data: {"type":"message_stop"}` + "\n\n"))
	}))
	defer upstream.Close()

	var deltas []string
	result, err := inferNeoLocalStream(testNeoRuntimeForServer(t, upstream), neoInferenceRequest{
		ThreadID:  "T-test",
		AgentMode: "smart",
		MaxTokens: 64000,
		Settings:  map[string]any{"internal.model": "anthropic/claude-test"},
		History:   []neoHistoryMessage{{Role: "user", Text: "hi"}},
	}, func(delta neoInferenceDelta) {
		deltas = append(deltas, delta.Text)
	})
	if err != nil {
		t.Fatalf("inferNeoLocalStream error: %v", err)
	}
	if calls != 1 {
		t.Fatalf("calls=%d, want 1", calls)
	}
	if result.Text != "retried" || strings.Join(deltas, "") != "retried" {
		t.Fatalf("text=%q deltas=%#v", result.Text, deltas)
	}
}

func TestInferNeoAnthropicStreamPreservesCitationDeltas(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/provider/anthropic/v1/messages" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		payload := readNeoJSON(r.Body)
		if payload["stream"] != true {
			t.Fatalf("stream = %#v, want true", payload["stream"])
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: message_start\n" +
			`data: {"type":"message_start","message":{"usage":{"input_tokens":3}}}` + "\n\n" +
			"event: content_block_start\n" +
			`data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}` + "\n\n" +
			"event: content_block_delta\n" +
			`data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"cited answer"}}` + "\n\n" +
			"event: content_block_delta\n" +
			`data: {"type":"content_block_delta","index":0,"delta":{"type":"citations_delta","citation":{"type":"char_location","cited_text":"source","document_index":0,"document_title":"doc.md","start_char_index":0,"end_char_index":6}}}` + "\n\n" +
			"event: content_block_delta\n" +
			`data: {"type":"content_block_delta","index":0,"delta":{"type":"compaction_delta","text":"provider-side compaction progress"}}` + "\n\n" +
			"event: message_stop\n" +
			`data: {"type":"message_stop"}` + "\n\n"))
	}))
	defer upstream.Close()

	result, err := inferNeoLocalStream(testNeoRuntimeForServer(t, upstream), neoInferenceRequest{
		ThreadID:  "T-test",
		AgentMode: "smart",
		Settings:  map[string]any{"internal.model": "anthropic/claude-test"},
		History:   []neoHistoryMessage{{Role: "user", Text: "hi"}},
	}, nil)
	if err != nil {
		t.Fatalf("inferNeoLocalStream error: %v", err)
	}
	if result.Text != "cited answer" {
		t.Fatalf("text=%q, want cited answer", result.Text)
	}
	if len(result.TextCitations) != 1 {
		t.Fatalf("citations=%#v, want one citation", result.TextCitations)
	}
	citation := mapValue(result.TextCitations[0])
	if stringValue(citation["type"]) != "char_location" || stringValue(citation["document_title"]) != "doc.md" {
		t.Fatalf("citation=%#v", citation)
	}

	actor := newNeoActor(newNeoRuntime(&config.Config{}), "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.finishAssistantMessageWithOptions("M-assistant", result, "smart", "", false, "")
	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.messages) != 1 {
		t.Fatalf("messages=%#v", actor.messages)
	}
	block := mapValue(actor.messages[0].Content[0])
	citations := arrayValue(block["citations"])
	if len(citations) != 1 || stringValue(mapValue(citations[0])["document_title"]) != "doc.md" {
		t.Fatalf("text block citations=%#v", block["citations"])
	}
}

func TestInferNeoAnthropicUsesBinaryDefaultMaxTokens(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/provider/anthropic/v1/messages" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		payload := readNeoJSON(r.Body)
		if payload["stream"] != false {
			t.Fatalf("stream = %#v, want false", payload["stream"])
		}
		if got := numberFrom(payload["max_tokens"]); got != 32000 {
			t.Fatalf("max_tokens = %d, want Amp binary default 32000; payload=%#v", got, payload)
		}
		if _, ok := payload["context_management"]; ok {
			t.Fatalf("request included context_management, but current Amp binary omits it: %#v", payload)
		}
		if beta := r.Header.Get("Anthropic-Beta"); strings.Contains(beta, "context-management") {
			t.Fatalf("Anthropic-Beta = %q, want no context-management beta", beta)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"content":[{"type":"text","text":"ok"}]}`))
	}))
	defer upstream.Close()

	result, err := inferNeoAnthropic(testNeoRuntimeForServer(t, upstream), neoInferenceRequest{
		ThreadID:  "T-test",
		AgentMode: "smart",
		History:   []neoHistoryMessage{{Role: "user", Text: "hi"}},
	}, neoModelRoute{Provider: "anthropic", Model: "claude-opus-4-7"})
	if err != nil {
		t.Fatalf("inferNeoAnthropic error: %v", err)
	}
	if result.Text != "ok" {
		t.Fatalf("text=%q, want ok", result.Text)
	}
}

func TestInferNeoAnthropicAppliesTemperatureOnlyWhenThinkingDisabled(t *testing.T) {
	tests := []struct {
		name           string
		request        neoInferenceRequest
		wantTemp       bool
		wantNoThinking bool
	}{
		{
			name: "reasoning none",
			request: neoInferenceRequest{
				ThreadID:        "T-test",
				AgentMode:       "smart",
				ReasoningEffort: "none",
				Settings:        map[string]any{"anthropic.temperature": 0.7},
				History:         []neoHistoryMessage{{Role: "user", Text: "hi"}},
			},
			wantTemp:       true,
			wantNoThinking: true,
		},
		{
			name: "setting disabled",
			request: neoInferenceRequest{
				ThreadID:        "T-test",
				AgentMode:       "smart",
				ReasoningEffort: "high",
				Settings:        map[string]any{"anthropic.temperature": 0.7, "anthropic.thinking.enabled": false},
				History:         []neoHistoryMessage{{Role: "user", Text: "hi"}},
			},
			wantTemp:       true,
			wantNoThinking: true,
		},
		{
			name: "default disabled",
			request: neoInferenceRequest{
				ThreadID:        "T-test",
				AgentMode:       "smart",
				ReasoningEffort: "high",
				History:         []neoHistoryMessage{{Role: "user", Text: "hi"}},
			},
			wantNoThinking: true,
		},
		{
			name: "thinking enabled",
			request: neoInferenceRequest{
				ThreadID:        "T-test",
				AgentMode:       "smart",
				ReasoningEffort: "high",
				Settings:        map[string]any{"anthropic.temperature": 0.7, "anthropic.thinking.enabled": true},
				History:         []neoHistoryMessage{{Role: "user", Text: "hi"}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/api/provider/anthropic/v1/messages" {
					t.Fatalf("unexpected path %s", r.URL.Path)
				}
				payload := readNeoJSON(r.Body)
				if tt.wantTemp {
					if temp, ok := payload["temperature"].(float64); !ok || temp != 0.7 {
						t.Fatalf("temperature = %#v, want 0.7; payload=%#v", payload["temperature"], payload)
					}
				} else if _, exists := payload["temperature"]; exists {
					t.Fatalf("temperature should be omitted while thinking is enabled: %#v", payload)
				}
				if tt.wantNoThinking {
					if _, exists := payload["thinking"]; exists {
						t.Fatalf("thinking should be omitted when disabled for non-adaptive Anthropic models: %#v", payload)
					}
				} else {
					thinkingBody := mapValue(payload["thinking"])
					if stringValue(thinkingBody["type"]) != "enabled" {
						t.Fatalf("thinking = %#v, want enabled", thinkingBody)
					}
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"content":[{"type":"text","text":"ok"}]}`))
			}))
			defer upstream.Close()

			result, err := inferNeoAnthropic(testNeoRuntimeForServer(t, upstream), tt.request, neoModelRoute{Provider: "anthropic", Model: "claude-test"})
			if err != nil {
				t.Fatalf("inferNeoAnthropic error: %v", err)
			}
			if result.Text != "ok" {
				t.Fatalf("text=%q, want ok", result.Text)
			}
		})
	}
}

func TestInferNeoAnthropicStreamRetriesWithAdaptiveThinkingWhenEnabledUnsupported(t *testing.T) {
	calls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/provider/anthropic/v1/messages" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		payload := readNeoJSON(r.Body)
		if payload["stream"] != true {
			t.Fatalf("stream = %#v, want true", payload["stream"])
		}

		calls++
		switch calls {
		case 1:
			thinkingBody := mapValue(payload["thinking"])
			if stringValue(thinkingBody["type"]) != "enabled" || numberFrom(thinkingBody["budget_tokens"]) == 0 {
				t.Fatalf("first request thinking = %#v, want enabled with budget", thinkingBody)
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"type":"error","error":{"type":"invalid_request_error","message":"\"thinking.type.enabled\" is not supported for this model. Use \"thinking.type.adaptive\" and \"output_config.effort\" to control thinking behavior."}}`))
		case 2:
			thinkingBody := mapValue(payload["thinking"])
			if stringValue(thinkingBody["type"]) != "adaptive" {
				t.Fatalf("retry request thinking = %#v, want adaptive", thinkingBody)
			}
			if _, ok := thinkingBody["budget_tokens"]; ok {
				t.Fatalf("retry request retained thinking budget: %#v", thinkingBody)
			}
			if effort := stringValue(mapValue(payload["output_config"])["effort"]); effort != "high" {
				t.Fatalf("retry request effort = %q, want high; payload=%#v", effort, payload)
			}
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("event: message_start\n" +
				`data: {"type":"message_start","message":{"usage":{"input_tokens":3}}}` + "\n\n" +
				"event: content_block_delta\n" +
				`data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"adaptive"}}` + "\n\n" +
				"event: message_stop\n" +
				`data: {"type":"message_stop"}` + "\n\n"))
		default:
			t.Fatalf("unexpected retry call %d", calls)
		}
	}))
	defer upstream.Close()

	var deltas []string
	result, err := inferNeoLocalStream(testNeoRuntimeForServer(t, upstream), neoInferenceRequest{
		ThreadID:        "T-test",
		AgentMode:       "smart",
		ReasoningEffort: "high",
		Settings:        map[string]any{"internal.model": "anthropic/claude-test", "anthropic.thinking.enabled": true},
		History:         []neoHistoryMessage{{Role: "user", Text: "hi"}},
	}, func(delta neoInferenceDelta) {
		deltas = append(deltas, delta.Text)
	})
	if err != nil {
		t.Fatalf("inferNeoLocalStream error: %v", err)
	}
	if calls != 2 {
		t.Fatalf("calls=%d, want 2", calls)
	}
	if result.Text != "adaptive" || strings.Join(deltas, "") != "adaptive" {
		t.Fatalf("text=%q deltas=%#v", result.Text, deltas)
	}
}

func TestNeoApplyAnthropicThinkingUsesAdaptiveEffortForAmpOpusModels(t *testing.T) {
	tests := []struct {
		name     string
		model    string
		fallback string
		want     string
	}{
		{name: "smart opus 4.7 effort", model: "claude-opus-4-7", fallback: "xhigh", want: "xhigh"},
		{name: "smart opus 4.8 effort", model: "claude-opus-4-8", fallback: "xhigh", want: "xhigh"},
		{name: "opus 4.7 default", model: "claude-opus-4-7", fallback: "", want: "medium"},
		{name: "large opus 4.6 default", model: "claude-opus-4-6", fallback: "", want: "high"},
		{name: "large opus 4.6 1m default", model: "claude-opus-4-6-1m", fallback: "", want: "high"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := map[string]any{}
			neoApplyAnthropicThinking(body, neoModelRoute{Provider: "anthropic", Model: tt.model}, tt.fallback)
			thinkingBody := mapValue(body["thinking"])
			if stringValue(thinkingBody["type"]) != "adaptive" || stringValue(thinkingBody["display"]) != "summarized" {
				t.Fatalf("thinking = %#v, want adaptive summarized", thinkingBody)
			}
			if _, ok := thinkingBody["budget_tokens"]; ok {
				t.Fatalf("adaptive thinking retained budget_tokens: %#v", thinkingBody)
			}
			if effort := stringValue(mapValue(body["output_config"])["effort"]); effort != tt.want {
				t.Fatalf("output_config.effort = %q, want %q; body=%#v", effort, tt.want, body)
			}
		})
	}
}

func TestWithNeoAnthropicAdaptiveThinkingPreservesXHighEffort(t *testing.T) {
	body := map[string]any{
		"thinking": map[string]any{"type": "enabled", "budget_tokens": 32768},
	}
	out := withNeoAnthropicAdaptiveThinking(body)
	thinkingBody := mapValue(out["thinking"])
	if stringValue(thinkingBody["type"]) != "adaptive" {
		t.Fatalf("thinking = %#v, want adaptive", thinkingBody)
	}
	if stringValue(thinkingBody["display"]) != "summarized" {
		t.Fatalf("thinking display = %#v, want summarized", thinkingBody)
	}
	if _, ok := thinkingBody["budget_tokens"]; ok {
		t.Fatalf("adaptive thinking retained budget_tokens: %#v", thinkingBody)
	}
	if effort := stringValue(mapValue(out["output_config"])["effort"]); effort != "xhigh" {
		t.Fatalf("output_config.effort = %q, want xhigh", effort)
	}
}

func TestInferNeoGoogleStreamFallsBackToNonStreamOnEmptyPayload(t *testing.T) {
	streamCalls := 0
	nonStreamCalls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, ":streamGenerateContent"):
			streamCalls++
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("data: {\"candidates\":[{\"content\":{\"parts\":[{}]}}]}\n\n"))
		case strings.Contains(r.URL.Path, ":generateContent"):
			nonStreamCalls++
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"candidates":[{"content":{"parts":[{"text":"google fallback"}]}}]}`))
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer upstream.Close()

	deltaCalls := 0
	result, err := inferNeoLocalStream(testNeoRuntimeForServer(t, upstream), neoInferenceRequest{
		ThreadID:  "T-test",
		AgentMode: "smart",
		Settings:  map[string]any{"internal.model": "google/gemini-test"},
		History:   []neoHistoryMessage{{Role: "user", Text: "hi"}},
	}, func(delta neoInferenceDelta) {
		deltaCalls++
	})
	if err != nil {
		t.Fatalf("inferNeoLocalStream error: %v", err)
	}
	if result.Text != "google fallback" {
		t.Fatalf("text=%q, want google fallback", result.Text)
	}
	if streamCalls != 1 || nonStreamCalls != 1 {
		t.Fatalf("streamCalls=%d nonStreamCalls=%d, want 1/1", streamCalls, nonStreamCalls)
	}
	if deltaCalls != 0 {
		t.Fatalf("deltaCalls=%d, want 0 for non-stream fallback", deltaCalls)
	}
}

func TestNeoRuntimeWebSocketOffsetsDeepOpenAIBlocksAfterThinking(t *testing.T) {
	textIndexes, toolIndex := collectNeoOpenAIStreamBlockIndexes(t, "deep", "shell_command", nil)
	if len(textIndexes) == 0 {
		t.Fatal("expected streamed text deltas")
	}
	for _, got := range textIndexes {
		if got != 1 {
			t.Fatalf("deep OpenAI text blockIndex = %d, want 1", got)
		}
	}
	if toolIndex != 2 {
		t.Fatalf("deep OpenAI tool_use blockIndex = %d, want 2", toolIndex)
	}
}

func TestNeoRuntimeWebSocketKeepsNonThinkingOpenAIBlockIndexes(t *testing.T) {
	textIndexes, toolIndex := collectNeoOpenAIStreamBlockIndexes(t, "smart", "Bash", map[string]any{"internal.model": "openai/gpt-test"})
	if len(textIndexes) == 0 {
		t.Fatal("expected streamed text deltas")
	}
	for _, got := range textIndexes {
		if got != 0 {
			t.Fatalf("smart OpenAI text blockIndex = %d, want 0", got)
		}
	}
	if toolIndex != 1 {
		t.Fatalf("smart OpenAI tool_use blockIndex = %d, want 1", toolIndex)
	}
}

func TestNeoRuntimeWebSocketStreamingEventSequenceMatchesAmpActor(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/provider/openai/v1/responses" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		payload := readNeoJSON(r.Body)
		if payload["stream"] != true {
			t.Fatalf("stream = %#v, want true", payload["stream"])
		}
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, _ := w.(http.Flusher)
		for _, chunk := range []string{
			`data: {"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"hel"}`,
			`data: {"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"lo"}`,
			`data: {"type":"response.output_item.added","output_index":1,"item":{"type":"function_call","call_id":"call_1","name":"shell_command","arguments":"{\"cmd\":\"pwd\"}"}}`,
			`data: {"type":"response.completed","response":{"usage":{"input_tokens":1,"output_tokens":1},"output":[]}}`,
			`data: [DONE]`,
		} {
			_, _ = w.Write([]byte(chunk + "\n\n"))
			if flusher != nil {
				flusher.Flush()
			}
		}
	}))
	defer upstream.Close()

	rt := testNeoRuntimeForServer(t, upstream)
	server := httptest.NewServer(http.HandlerFunc(rt.handleHTTP))
	t.Cleanup(server.Close)

	threadID := "T-019e0e6e-f3f1-7078-b5dd-748f66f8c25f"
	conn := dialNeoActorWebSocket(t, server.URL, threadID)
	defer conn.Close()

	if err := conn.WriteJSON(map[string]any{
		"type":  "executor_tools_register",
		"tools": []any{map[string]any{"name": "shell_command", "description": "run shell command", "inputSchema": map[string]any{"type": "object"}}},
	}); err != nil {
		t.Fatalf("write tools register: %v", err)
	}
	if err := conn.WriteJSON(map[string]any{"type": "executor_connected", "executorId": "executor-test", "registeredToolCount": 1}); err != nil {
		t.Fatalf("write executor_connected: %v", err)
	}
	if err := conn.WriteJSON(map[string]any{
		"type":            "client_append_user_msg",
		"messageId":       "M-user",
		"agentMode":       "deep",
		"reasoningEffort": "xhigh",
		"content":         []any{map[string]any{"type": "text", "text": "hi"}},
	}); err != nil {
		t.Fatalf("write user message: %v", err)
	}

	events := make([]string, 0, 11)
	assistantMessageID := ""
	streamingStates := 0
	partialToolID := ""
	finalToolID := ""
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		msg, ok := readNeoMessage(t, conn, time.Until(deadline))
		if !ok {
			break
		}
		switch msg["type"] {
		case "agent_state":
			state := stringValue(msg["state"])
			switch state {
			case "working", "streaming", "running_tools":
				messageID := stringValue(msg["messageId"])
				if messageID == "" {
					continue
				}
				if assistantMessageID == "" {
					assistantMessageID = messageID
				} else if messageID != assistantMessageID {
					t.Fatalf("agent_state messageId = %q, want %q: %#v", messageID, assistantMessageID, msg)
				}
				if state == "streaming" {
					streamingStates++
				}
				events = append(events, "agent:"+state)
			}
		case "inference_tools":
			if messageID := stringValue(msg["messageId"]); messageID != "" {
				if assistantMessageID == "" {
					assistantMessageID = messageID
				} else if messageID != assistantMessageID {
					t.Fatalf("inference_tools messageId = %q, want %q: %#v", messageID, assistantMessageID, msg)
				}
			}
			events = append(events, "inference_tools")
		case "delta":
			if msg["role"] != "assistant" {
				continue
			}
			messageID := stringValue(msg["messageId"])
			if assistantMessageID == "" {
				assistantMessageID = messageID
			} else if messageID != assistantMessageID {
				t.Fatalf("delta messageId = %q, want %q: %#v", messageID, assistantMessageID, msg)
			}
			switch stringValue(msg["state"]) {
			case "start":
				events = append(events, "delta:start")
			case "generating":
				blocks := arrayValue(msg["blocks"])
				if len(blocks) == 0 {
					continue
				}
				block := mapValue(blocks[0])
				if stringValue(block["type"]) != "text" {
					continue
				}
				if streamingStates == 0 {
					t.Fatalf("text delta arrived before streaming state: %#v", msg)
				}
				events = append(events, "delta:text:"+stringValue(block["text"]))
			case "tool_use":
				blocks := arrayValue(msg["blocks"])
				if len(blocks) == 0 {
					continue
				}
				block := mapValue(blocks[0])
				if stringValue(block["type"]) != "tool_use" {
					continue
				}
				if stringValue(block["name"]) != "shell_command" || stringValue(mapValue(block["input"])["cmd"]) != "pwd" {
					t.Fatalf("tool_use block mismatch: %#v", block)
				}
				toolID := stringValue(block["id"])
				if toolID == "" || !strings.HasPrefix(toolID, "TU-") {
					t.Fatalf("tool_use id = %q, want TU-*", toolID)
				}
				if boolValue(block["complete"]) {
					if _, exists := block["inputPartialJSON"]; exists {
						t.Fatalf("complete tool_use should not carry inputPartialJSON: %#v", block)
					}
					finalToolID = toolID
					events = append(events, "delta:tool_use:complete")
				} else {
					partialJSON := mapValue(block["inputPartialJSON"])
					if stringValue(partialJSON["json"]) != "{\"cmd\":\"pwd\"}" {
						t.Fatalf("partial tool_use inputPartialJSON = %#v", partialJSON)
					}
					if stringValue(mapValue(block["inputIncomplete"])["cmd"]) != "pwd" {
						t.Fatalf("partial tool_use inputIncomplete = %#v", block["inputIncomplete"])
					}
					partialToolID = toolID
					events = append(events, "delta:tool_use:partial")
				}
			}
		case "message_added":
			message := mapValue(msg["message"])
			if stringValue(message["role"]) != "assistant" {
				continue
			}
			if messageID := stringValue(message["messageId"]); messageID != assistantMessageID {
				t.Fatalf("assistant messageId = %q, want %q: %#v", messageID, assistantMessageID, msg)
			}
			blocks := arrayValue(message["content"])
			if len(blocks) < 3 || stringValue(mapValue(blocks[1])["text"]) != "hello" || stringValue(mapValue(blocks[2])["id"]) != finalToolID {
				t.Fatalf("assistant message content mismatch: %#v", blocks)
			}
			events = append(events, "message_added:assistant")
		case "tool_lease":
			if stringValue(msg["toolCallId"]) != finalToolID {
				t.Fatalf("tool lease id = %q, want %q: %#v", stringValue(msg["toolCallId"]), finalToolID, msg)
			}
			events = append(events, "tool_lease")
			if partialToolID == "" || finalToolID == "" || partialToolID != finalToolID {
				t.Fatalf("tool ids partial=%q final=%q", partialToolID, finalToolID)
			}
			want := []string{
				"agent:working",
				"inference_tools",
				"delta:start",
				"agent:streaming",
				"delta:text:hel",
				"delta:text:lo",
				"delta:tool_use:partial",
				"delta:tool_use:complete",
				"message_added:assistant",
				"agent:running_tools",
				"tool_lease",
			}
			if strings.Join(events, "|") != strings.Join(want, "|") {
				t.Fatalf("events mismatch\n got: %#v\nwant: %#v", events, want)
			}
			if streamingStates != 1 {
				t.Fatalf("streaming state count = %d, want 1 events=%#v", streamingStates, events)
			}
			return
		}
	}
	t.Fatalf("timed out waiting for tool lease, events=%#v", events)
}

func TestNeoRuntimeWebSocketStreamingToolArgumentsUseAmpDeltaShape(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/provider/openai/v1/responses" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		payload := readNeoJSON(r.Body)
		if payload["stream"] != true {
			t.Fatalf("stream = %#v, want true", payload["stream"])
		}
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, _ := w.(http.Flusher)
		for _, chunk := range []string{
			`data: {"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","call_id":"call_1","name":"shell_command","arguments":"{\"cmd\""}}`,
			`data: {"type":"response.function_call_arguments.delta","output_index":0,"delta":":\"pwd\"}"}`,
			`data: {"type":"response.completed","response":{"usage":{"input_tokens":1,"output_tokens":1},"output":[]}}`,
			`data: [DONE]`,
		} {
			_, _ = w.Write([]byte(chunk + "\n\n"))
			if flusher != nil {
				flusher.Flush()
			}
		}
	}))
	defer upstream.Close()

	rt := testNeoRuntimeForServer(t, upstream)
	server := httptest.NewServer(http.HandlerFunc(rt.handleHTTP))
	t.Cleanup(server.Close)

	threadID := "T-019e0e6e-f3f1-7078-b5dd-748f66f8c25f"
	conn := dialNeoActorWebSocket(t, server.URL, threadID)
	defer conn.Close()

	if err := conn.WriteJSON(map[string]any{
		"type":  "executor_tools_register",
		"tools": []any{map[string]any{"name": "shell_command", "description": "run shell command", "inputSchema": map[string]any{"type": "object"}}},
	}); err != nil {
		t.Fatalf("write tools register: %v", err)
	}
	if err := conn.WriteJSON(map[string]any{"type": "executor_connected", "executorId": "executor-test", "registeredToolCount": 1}); err != nil {
		t.Fatalf("write executor_connected: %v", err)
	}
	if err := conn.WriteJSON(map[string]any{
		"type":      "client_append_user_msg",
		"messageId": "M-user",
		"agentMode": "deep",
		"content":   []any{map[string]any{"type": "text", "text": "hi"}},
	}); err != nil {
		t.Fatalf("write user message: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	toolDeltas := make([]map[string]any, 0, 2)
	for time.Now().Before(deadline) && len(toolDeltas) < 2 {
		msg, ok := readNeoMessage(t, conn, time.Until(deadline))
		if !ok {
			break
		}
		if msg["type"] != "delta" || msg["role"] != "assistant" || msg["state"] != "tool_use" {
			continue
		}
		blocks := arrayValue(msg["blocks"])
		if len(blocks) == 0 {
			continue
		}
		block := mapValue(blocks[0])
		if stringValue(block["type"]) == "tool_use" && !boolValue(block["complete"]) {
			toolDeltas = append(toolDeltas, block)
		}
	}
	if len(toolDeltas) != 2 {
		t.Fatalf("tool delta count = %d, want 2: %#v", len(toolDeltas), toolDeltas)
	}
	if stringValue(mapValue(toolDeltas[0]["inputPartialJSON"])["json"]) != "{\"cmd\"" {
		t.Fatalf("first tool delta = %#v, want initial inputPartialJSON", toolDeltas[0])
	}
	if _, exists := toolDeltas[0]["inputPartialJSONDelta"]; exists {
		t.Fatalf("first tool delta should not use inputPartialJSONDelta: %#v", toolDeltas[0])
	}
	if stringValue(mapValue(toolDeltas[1]["inputPartialJSONDelta"])["json"]) != ":\"pwd\"}" {
		t.Fatalf("second tool delta = %#v, want inputPartialJSONDelta", toolDeltas[1])
	}
	if _, exists := toolDeltas[1]["inputPartialJSON"]; exists {
		t.Fatalf("second tool delta should not repeat cumulative inputPartialJSON: %#v", toolDeltas[1])
	}
}

func TestNeoRuntimeWebSocketStreamsAnthropicThinkingAndTextIndexes(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/provider/anthropic/v1/messages" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		payload := readNeoJSON(r.Body)
		if payload["stream"] != true {
			t.Fatalf("stream = %#v, want true", payload["stream"])
		}
		if thinkingBody := mapValue(payload["thinking"]); stringValue(thinkingBody["type"]) != "adaptive" || stringValue(thinkingBody["display"]) != "summarized" {
			t.Fatalf("thinking = %#v, want adaptive summarized", thinkingBody)
		}
		if effort := stringValue(mapValue(payload["output_config"])["effort"]); effort != "max" {
			t.Fatalf("output_config.effort = %q, want max; payload=%#v", effort, payload)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, _ := w.(http.Flusher)
		for _, chunk := range []string{
			`event: message_start` + "\n" + `data: {"type":"message_start","message":{"id":"msg_1","model":"claude-opus-4-7","usage":{"input_tokens":2,"output_tokens":0}}}`,
			`event: content_block_start` + "\n" + `data: {"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":"","signature":""}}`,
			`event: content_block_delta` + "\n" + `data: {"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"considering"}}`,
			`event: content_block_delta` + "\n" + `data: {"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"sig_1"}}`,
			`event: content_block_start` + "\n" + `data: {"type":"content_block_start","index":1,"content_block":{"type":"text","text":""}}`,
			`event: content_block_delta` + "\n" + `data: {"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"hello"}}`,
			`event: message_delta` + "\n" + `data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"input_tokens":2,"output_tokens":1}}`,
			`data: [DONE]`,
		} {
			_, _ = w.Write([]byte(chunk + "\n\n"))
			if flusher != nil {
				flusher.Flush()
			}
		}
	}))
	defer upstream.Close()

	rt := testNeoRuntimeForServer(t, upstream)
	server := httptest.NewServer(http.HandlerFunc(rt.handleHTTP))
	t.Cleanup(server.Close)

	threadID := "T-019e0e6e-f3f1-7078-b5dd-748f66f8c25f"
	conn := dialNeoActorWebSocket(t, server.URL, threadID)
	defer conn.Close()

	if err := conn.WriteJSON(map[string]any{"type": "executor_connected", "executorId": "executor-test", "registeredToolCount": 0}); err != nil {
		t.Fatalf("write executor_connected: %v", err)
	}
	if err := conn.WriteJSON(map[string]any{
		"type":            "client_append_user_msg",
		"messageId":       "M-user",
		"agentMode":       "smart",
		"reasoningEffort": "max",
		"content":         []any{map[string]any{"type": "text", "text": "hi"}},
	}); err != nil {
		t.Fatalf("write user message: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	sawThinking := false
	sawText := false
	sawComplete := false
	for time.Now().Before(deadline) {
		msg, ok := readNeoMessage(t, conn, time.Until(deadline))
		if !ok {
			break
		}
		switch msg["type"] {
		case "delta":
			if msg["role"] != "assistant" || msg["state"] != "generating" {
				continue
			}
			blocks := arrayValue(msg["blocks"])
			if len(blocks) == 0 {
				continue
			}
			if usage := mapValue(msg["usage"]); len(usage) > 0 {
				if _, ok := usage["input_tokens"]; ok {
					t.Fatalf("streaming delta leaked provider usage shape: %#v", msg)
				}
				for _, key := range []string{"maxInputTokens", "inputTokens", "outputTokens", "cacheCreationInputTokens", "cacheReadInputTokens", "totalInputTokens"} {
					if _, ok := usage[key]; !ok {
						t.Fatalf("streaming delta usage missing %s: %#v", key, msg)
					}
				}
			}
			block := mapValue(blocks[0])
			switch stringValue(block["type"]) {
			case "thinking":
				if _, ok := block["signature"]; !ok {
					t.Fatalf("thinking delta missing required signature field: %#v", msg)
				}
				if numberFrom(msg["blockIndex"]) != 0 {
					t.Fatalf("thinking blockIndex = %d, want 0: %#v", numberFrom(msg["blockIndex"]), msg)
				}
				if strings.Contains(stringValue(block["thinking"]), "considering") {
					sawThinking = true
				}
			case "text":
				if numberFrom(msg["blockIndex"]) != 1 {
					t.Fatalf("text blockIndex = %d, want 1 after thinking: %#v", numberFrom(msg["blockIndex"]), msg)
				}
				if stringValue(block["text"]) == "hello" {
					sawText = true
				}
			}
		case "message_updated":
			message := mapValue(msg["message"])
			if stringValue(message["role"]) != "assistant" {
				continue
			}
			if stringValue(mapValue(message["state"])["type"]) == "streaming" {
				t.Fatalf("streaming assistant frame should be a delta, got message_updated: %#v", msg)
			}
			if stringValue(mapValue(message["state"])["type"]) == "complete" {
				content := arrayValue(message["content"])
				if len(content) >= 2 &&
					stringValue(mapValue(content[0])["type"]) == "thinking" &&
					stringValue(mapValue(content[1])["text"]) == "hello" {
					sawComplete = true
				}
			}
		case "message_added":
			message := mapValue(msg["message"])
			if stringValue(message["role"]) != "assistant" || stringValue(mapValue(message["state"])["type"]) != "complete" {
				continue
			}
			content := arrayValue(message["content"])
			if len(content) >= 2 &&
				stringValue(mapValue(content[0])["type"]) == "thinking" &&
				stringValue(mapValue(content[1])["text"]) == "hello" {
				sawComplete = true
			}
		case "agent_state":
			if msg["state"] == "idle" && sawThinking && sawText && sawComplete {
				return
			}
		}
	}
	t.Fatalf("timed out waiting for streamed thinking/text, sawThinking=%t sawText=%t sawComplete=%t", sawThinking, sawText, sawComplete)
}

func collectNeoOpenAIStreamBlockIndexes(t *testing.T, agentMode, toolName string, settings map[string]any) ([]int, int) {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/provider/openai/v1/responses" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, _ := w.(http.Flusher)
		for _, chunk := range []string{
			`data: {"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"hel"}`,
			`data: {"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"lo"}`,
			`data: {"type":"response.output_item.added","output_index":1,"item":{"type":"function_call","call_id":"call_1","name":"` + toolName + `","arguments":"{\"cmd\":\"pwd\"}"}}`,
			`data: {"type":"response.completed","response":{"usage":{"input_tokens":1,"output_tokens":1},"output":[]}}`,
			`data: [DONE]`,
		} {
			_, _ = w.Write([]byte(chunk + "\n\n"))
			if flusher != nil {
				flusher.Flush()
			}
		}
	}))
	defer upstream.Close()

	rt := testNeoRuntimeForServer(t, upstream)
	server := httptest.NewServer(http.HandlerFunc(rt.handleHTTP))
	t.Cleanup(server.Close)

	threadID := "T-019e0e6e-f3f1-7078-b5dd-748f66f8c25f"
	dialer := websocket.Dialer{Subprotocols: []string{"rivet", "rivet_encoding.4", "rivet_skip_ready_wait"}}
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/gateway/threadActor/?rvt-method=getOrCreate&rvt-key=" + url.QueryEscape(threadID)
	conn, resp, err := dialer.Dial(wsURL, nil)
	if err != nil {
		status := 0
		if resp != nil {
			status = resp.StatusCode
		}
		t.Fatalf("gateway websocket dial failed status=%d err=%v", status, err)
	}
	defer conn.Close()

	if err := conn.WriteJSON(map[string]any{
		"type":  "executor_tools_register",
		"tools": []any{map[string]any{"name": toolName, "description": "run shell command", "inputSchema": map[string]any{"type": "object"}}},
	}); err != nil {
		t.Fatalf("write tools register: %v", err)
	}
	if err := conn.WriteJSON(map[string]any{"type": "executor_connected", "executorId": "executor-test", "registeredToolCount": 1}); err != nil {
		t.Fatalf("write executor_connected: %v", err)
	}
	if len(settings) > 0 {
		if err := conn.WriteJSON(map[string]any{"type": "client_update_thread_settings", "settings": settings}); err != nil {
			t.Fatalf("write thread settings: %v", err)
		}
	}
	if err := conn.WriteJSON(map[string]any{
		"type":      "client_append_user_msg",
		"messageId": "M-user",
		"agentMode": agentMode,
		"content":   []any{map[string]any{"type": "text", "text": "hi"}},
	}); err != nil {
		t.Fatalf("write user message: %v", err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	sawStart := false
	textIndexes := make([]int, 0, 2)
	for {
		_, payload, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read websocket message: %v", err)
		}
		var msg map[string]any
		if err := json.Unmarshal(payload, &msg); err != nil {
			t.Fatalf("websocket JSON error: %v", err)
		}
		if msg["type"] != "delta" || msg["role"] != "assistant" {
			continue
		}
		switch msg["state"] {
		case "start":
			sawStart = true
		case "generating":
			blocks := arrayValue(msg["blocks"])
			if len(blocks) == 0 || stringValue(mapValue(blocks[0])["type"]) != "text" {
				continue
			}
			if !sawStart {
				t.Fatalf("text delta arrived before start delta: %#v", msg)
			}
			textIndexes = append(textIndexes, numberFrom(msg["blockIndex"]))
		case "tool_use":
			if !sawStart {
				t.Fatalf("tool_use delta arrived before start delta: %#v", msg)
			}
			return textIndexes, numberFrom(msg["blockIndex"])
		}
	}
}

func TestInferNeoAnthropicStreamsTextAndToolCalls(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/provider/anthropic/v1/messages" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		payload := readNeoJSON(r.Body)
		if payload["stream"] != true {
			t.Fatalf("stream = %#v, want true", payload["stream"])
		}
		w.Header().Set("Content-Type", "text/event-stream")
		for _, chunk := range []string{
			"event: message_start\n" +
				`data: {"type":"message_start","message":{"usage":{"input_tokens":3}}}`,
			"event: content_block_start\n" +
				`data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`,
			"event: content_block_delta\n" +
				`data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hi"}}`,
			"event: content_block_start\n" +
				`data: {"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"toolu_1","name":"shell_command","input":{}}}`,
			"event: content_block_delta\n" +
				`data: {"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"cmd\":\"pwd\"}"}}`,
			"event: message_stop\n" +
				`data: {"type":"message_stop"}`,
		} {
			_, _ = w.Write([]byte(chunk + "\n\n"))
		}
	}))
	defer upstream.Close()

	var deltas []string
	result, err := inferNeoLocalStream(testNeoRuntimeForServer(t, upstream), neoInferenceRequest{
		ThreadID:  "T-test",
		AgentMode: "deep",
		Settings:  map[string]any{"internal.model": "anthropic/claude-test"},
		History:   []neoHistoryMessage{{Role: "user", Text: "hi"}},
		Tools:     []neoToolSpec{{Name: "shell_command", InputSchema: map[string]any{"type": "object"}}},
	}, func(delta neoInferenceDelta) {
		deltas = append(deltas, delta.Text)
	})
	if err != nil {
		t.Fatalf("inferNeoLocalStream error: %v", err)
	}
	if result.Text != "hi" || strings.Join(deltas, "") != "hi" {
		t.Fatalf("text=%q deltas=%#v", result.Text, deltas)
	}
	if len(result.ToolCalls) != 1 || result.ToolCalls[0].Name != "shell_command" || stringValue(result.ToolCalls[0].Input["cmd"]) != "pwd" {
		t.Fatalf("tool calls = %#v", result.ToolCalls)
	}
}

func TestInferNeoGoogleUsesGeminiThinkingLevelSetting(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/provider/google/v1beta/models/gemini-3-pro:generateContent" {
			t.Fatalf("provider path = %s", r.URL.Path)
		}
		payload := readNeoJSON(r.Body)
		thinkingConfig := mapValue(mapValue(payload["generationConfig"])["thinkingConfig"])
		if got := numberFrom(thinkingConfig["thinkingBudget"]); got != 8192 {
			t.Fatalf("thinkingBudget = %d, want 8192; payload=%#v", got, payload)
		}
		writeNeoJSON(w, http.StatusOK, map[string]any{
			"candidates": []any{map[string]any{
				"content": map[string]any{"parts": []any{map[string]any{"text": "ok"}}},
			}},
		})
	}))
	defer upstream.Close()

	result, err := inferNeoGoogle(testNeoRuntimeForServer(t, upstream), neoInferenceRequest{
		ThreadID: "T-test",
		Settings: map[string]any{
			"gemini.thinkingLevel": "medium",
		},
		History: []neoHistoryMessage{{Role: "user", Text: "hi"}},
	}, neoModelRoute{Provider: "google", Model: "gemini-3-pro"})
	if err != nil {
		t.Fatalf("inferNeoGoogle error: %v", err)
	}
	if result.Text != "ok" {
		t.Fatalf("result text = %q", result.Text)
	}
}

func TestNeoGoogleThinkingFallbackPrefersValidReasoningEffort(t *testing.T) {
	if got := neoGoogleThinkingFallback(neoInferenceRequest{
		ReasoningEffort: "none",
		Settings:        map[string]any{"gemini.thinkingLevel": "medium"},
	}); got != "none" {
		t.Fatalf("fallback = %q, want none", got)
	}
	if got := neoGoogleThinkingFallback(neoInferenceRequest{
		ReasoningEffort: "ultra",
		Settings:        map[string]any{"gemini.thinkingLevel": "medium"},
	}); got != "medium" {
		t.Fatalf("fallback = %q, want medium", got)
	}
}

func testNeoRuntimeForServer(t *testing.T, upstream *httptest.Server) *neoRuntime {
	t.Helper()
	parsed, err := url.Parse(upstream.URL)
	if err != nil {
		t.Fatal(err)
	}
	host, portText, err := net.SplitHostPort(parsed.Host)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatal(err)
	}
	return newNeoRuntime(&config.Config{
		SDKConfig: config.SDKConfig{APIKeys: []string{"local-key"}},
		Host:      host,
		Port:      port,
	})
}

func TestNeoActorSteersQueuedMessageToFront(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.agentState = "running"
	actor.queue = []neoQueuedMessage{
		{MessageID: "M-first", Content: []any{map[string]any{"type": "text", "text": "first"}}},
		{MessageID: "M-second", Content: []any{map[string]any{"type": "text", "text": "second"}}},
	}

	actor.steerQueuedMessage("M-second")

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.queue) != 2 {
		t.Fatalf("queue len = %d", len(actor.queue))
	}
	if actor.queue[0].MessageID != "M-second" || !actor.queue[0].Steer {
		t.Fatalf("queue after steer = %#v", actor.queue)
	}
}

func TestNeoActorQueuesSteeredUserMessageAhead(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.agentState = "running"
	actor.executorReady = true
	actor.queue = []neoQueuedMessage{
		{MessageID: "M-first", Content: []any{map[string]any{"type": "text", "text": "first"}}},
	}

	actor.receiveUserMessage(map[string]any{
		"type":      "client_append_user_msg",
		"messageId": "M-steer",
		"content":   []any{map[string]any{"type": "text", "text": "now"}},
		"steer":     true,
	})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.queue) != 2 {
		t.Fatalf("queue len = %d", len(actor.queue))
	}
	if actor.queue[0].MessageID != "M-steer" || !actor.queue[0].Steer {
		t.Fatalf("queue after steered append = %#v", actor.queue)
	}
}

func TestNeoActorLeasesThreadToolsInsteadOfServingLocalSnapshots(t *testing.T) {
	dir := t.TempDir()
	oldStoreDir := neoAmpThreadStoreDir
	neoAmpThreadStoreDir = func() string { return dir }
	t.Cleanup(func() { neoAmpThreadStoreDir = oldStoreDir })

	currentThreadID := "T-019e65c0-0310-77a8-b233-4b84d9c0612f"
	targetThreadID := "T-019e65c0-0310-77a8-b233-4b84d9c06130"
	rawThread := []byte(`{"id":"` + targetThreadID + `","title":"local target","messages":[{"messageId":"M-local","role":"user","content":[{"type":"text","text":"local thread content must not be served"}]}]}`)
	if err := os.WriteFile(filepath.Join(dir, targetThreadID+".json"), rawThread, 0o600); err != nil {
		t.Fatalf("write local thread: %v", err)
	}

	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", currentThreadID, currentThreadID, neoActorRecord("actor-test", "thread-actor", currentThreadID), nil)
	actor.finishAssistantMessage("M-assistant", neoInferenceResult{
		ToolCalls: []neoToolCall{
			{ID: "TU-read", Name: "read_thread", Input: map[string]any{"threadID": targetThreadID}},
			{ID: "TU-find", Name: "find_thread", Input: map[string]any{"query": "local target"}},
		},
	}, "deep", "xhigh")

	func() {
		actor.mu.Lock()
		defer actor.mu.Unlock()
		if len(actor.pendingTools) != 2 {
			t.Fatalf("pending tools = %#v, want read_thread and find_thread leases", actor.pendingTools)
		}
		for _, toolID := range []string{"TU-read", "TU-find"} {
			if _, ok := actor.pendingTools[toolID]; !ok {
				t.Fatalf("missing pending thread tool lease %s: %#v", toolID, actor.pendingTools)
			}
		}
		for _, message := range actor.messages {
			if message.Role != "user" {
				continue
			}
			for _, raw := range message.Content {
				block := mapValue(raw)
				if stringValue(block["type"]) == "tool_result" {
					t.Fatalf("thread tool was served locally instead of leased: %#v", message)
				}
			}
		}
	}()
	waitForNeoActorSyncIdle(t, actor)
}

func TestNeoActorPassesReadThreadToolResultThrough(t *testing.T) {
	dir := t.TempDir()
	oldStoreDir := neoAmpThreadStoreDir
	neoAmpThreadStoreDir = func() string { return dir }
	t.Cleanup(func() { neoAmpThreadStoreDir = oldStoreDir })

	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-current-thread", "T-current-thread", neoActorRecord("actor-test", "thread-actor", "T-current-thread"), nil)
	actor.pendingTools["TU-read"] = neoPendingTool{
		ID:    "TU-read",
		Name:  "read_thread",
		Input: map[string]any{"threadID": "T-target-thread", "question": "extract context"},
	}

	actor.receiveToolResult(map[string]any{
		"type":       "executor_tool_result",
		"toolCallId": "TU-read",
		"run": map[string]any{
			"status": "done",
			"result": "binary subagent answer goes straight through unchanged",
		},
	})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.history) != 1 {
		t.Fatalf("history = %#v", actor.history)
	}
	if actor.history[0].Text != "binary subagent answer goes straight through unchanged" {
		t.Fatalf("history text = %q, expected the binary's subagent result to pass through without override", actor.history[0].Text)
	}
	if _, ok := actor.pendingTools["TU-read"]; ok {
		t.Fatal("read thread tool still pending")
	}
}

func TestNeoActorPassesFindThreadToolResultThrough(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-current-thread", "T-current-thread", neoActorRecord("actor-test", "thread-actor", "T-current-thread"), nil)
	actor.pendingTools["TU-find"] = neoPendingTool{
		ID:    "TU-find",
		Name:  "find_thread",
		Input: map[string]any{"query": "T-search-target", "limit": "5"},
	}

	actor.receiveToolResult(map[string]any{
		"type":       "executor_tool_result",
		"toolCallId": "TU-find",
		"run": map[string]any{
			"status": "done",
			"result": "binary find_thread result goes straight through unchanged",
		},
	})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.history) != 1 {
		t.Fatalf("history = %#v", actor.history)
	}
	if actor.history[0].Text != "binary find_thread result goes straight through unchanged" {
		t.Fatalf("history text = %q, expected the binary's find_thread result to pass through without override", actor.history[0].Text)
	}
	if _, ok := actor.pendingTools["TU-find"]; ok {
		t.Fatal("find thread tool still pending")
	}
}

func TestNeoFindThreadToolRunPassesThrough(t *testing.T) {
	run := normalizeNeoExecutorToolRun(context.Background(), nil, neoPendingTool{
		Name:  "find_thread",
		Input: map[string]any{"query": "T-search-target", "limit": "5"},
	}, map[string]any{
		"status": "done",
		"result": map[string]any{
			"hasMore": false,
			"threads": []any{map[string]any{
				"id":                "T-wrong-thread",
				"title":             "Wrong thread",
				"creatorUserID":     neoLocalOwnerUserID,
				"created":           float64(1778170000000),
				"updatedAt":         "2026-05-07T16:06:40Z",
				"messageCount":      float64(1),
				"matchedSearchText": "wrong result",
			}},
		},
	}, "T-current-thread")

	result := mapValue(run["result"])
	threads := arrayValue(result["threads"])
	if len(threads) != 1 {
		t.Fatalf("threads = %#v", result["threads"])
	}
	thread := mapValue(threads[0])
	if thread["id"] != "T-wrong-thread" || stringValue(thread["matchedSearchText"]) != "wrong result" {
		t.Fatalf("find_thread result was locally normalized: %#v", thread)
	}
}

func TestNeoActorHandlesMessageReadState(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.messages = []neoMessage{
		{ThreadID: "T-test", MessageID: "M-user", Role: "user", Content: []any{map[string]any{"type": "text", "text": "hello"}}, Seq: 1},
	}
	actor.seq = 2

	actor.handle(map[string]any{"type": "client_mark_message_read", "messageId": "M-user"})

	actor.mu.Lock()
	readAt := actor.messages[0].ReadAt
	actor.mu.Unlock()
	if readAt == "" {
		t.Fatal("readAt was not set")
	}
	if got := actor.messages[0].protocol()["readAt"]; got != readAt {
		t.Fatalf("protocol readAt = %#v, want %q", got, readAt)
	}

	actor.handle(map[string]any{"type": "client_mark_message_unread", "messageId": "M-user"})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if actor.messages[0].ReadAt != "" {
		t.Fatalf("readAt = %q, want cleared", actor.messages[0].ReadAt)
	}
	if _, ok := actor.messages[0].protocol()["readAt"]; ok {
		t.Fatalf("protocol should omit cleared readAt: %#v", actor.messages[0].protocol())
	}
}

func TestNeoActorHandlesToolLeaseRevoked(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.pendingTools["TU-test"] = neoPendingTool{ID: "TU-test", Name: "Bash", AgentMode: "smart", MessageID: "M-assistant"}
	actor.agentState = "running_tools"

	actor.handle(map[string]any{"type": "executor_tool_lease_revoked", "toolCallId": "TU-test", "reason": "reassigned"})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.pendingTools) != 0 {
		t.Fatalf("pending tools = %#v", actor.pendingTools)
	}
	if actor.agentState != "idle" {
		t.Fatalf("agentState = %q, want idle", actor.agentState)
	}
}

func TestNeoActorProcessQueueWaitsForExecutorReady(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.queue = []neoQueuedMessage{{MessageID: "M-queued", Content: []any{map[string]any{"type": "text", "text": "continue"}}, AgentMode: "deep", ReasoningEffort: "xhigh"}}
	actor.agentState = "idle"
	actor.executorReady = false

	actor.processQueue()

	actor.mu.Lock()
	if len(actor.queue) != 1 {
		actor.mu.Unlock()
		t.Fatalf("queue len after not-ready process = %d, want 1", len(actor.queue))
	}
	if len(actor.messages) != 0 {
		actor.mu.Unlock()
		t.Fatalf("messages after not-ready process = %#v, want none", actor.messages)
	}
	actor.executorReady = true
	actor.mu.Unlock()

	actor.processQueue()

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.queue) != 0 {
		t.Fatalf("queue len after ready process = %d, want 0", len(actor.queue))
	}
	if len(actor.messages) != 1 || actor.messages[0].MessageID != "M-queued" || actor.messages[0].AgentMode != "deep" {
		t.Fatalf("messages after ready process = %#v", actor.messages)
	}
}

func TestNeoActorHandlesBinaryUserThreadDeltas(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.messages = []neoMessage{
		{ThreadID: "T-test", MessageID: "M-user", Role: "user", Content: []any{map[string]any{"type": "text", "text": "hello"}}, Seq: 1},
		{ThreadID: "T-test", MessageID: "M-assistant", Role: "assistant", Content: []any{map[string]any{"type": "text", "text": "old"}}, Seq: 2},
	}
	actor.relationships = []map[string]any{{"threadID": "T-019e0e6e-f3f1-7081-b5dd-748f66f8c25d", "type": "mention", "role": "parent", "createdAt": 1, "messageIndex": 1}}
	actor.rebuildHistoryLocked()

	actor.handle(map[string]any{"type": "user:message:append-content", "messageId": "M-user", "content": []any{map[string]any{"type": "text", "text": " world"}}})
	actor.handle(map[string]any{"type": "user:message:interrupt", "messageIndex": 0})

	actor.mu.Lock()
	if textFromBlocks(actor.messages[0].Content) != "hello world" || !actor.messages[0].Interrupted {
		t.Fatalf("user delta message = %#v", actor.messages[0])
	}
	if !strings.Contains(actor.history[0].Text, "*(interrupted)*") {
		t.Fatalf("interrupted history marker missing: %#v", actor.history)
	}
	protocol := actor.messages[0].protocol()
	actor.mu.Unlock()
	if protocol["interrupted"] != true {
		t.Fatalf("protocol missing interrupted flag: %#v", protocol)
	}

	actor.handle(map[string]any{"type": "thread:truncate", "fromIndex": 1})
	actor.mu.Lock()
	if len(actor.messages) != 1 || actor.messages[0].MessageID != "M-user" {
		t.Fatalf("messages after truncate = %#v", actor.messages)
	}
	if len(actor.relationships) != 0 {
		t.Fatalf("relationships after truncate = %#v", actor.relationships)
	}
	actor.mu.Unlock()
	waitForNeoActorSyncIdle(t, actor)
}

func TestNeoBinaryReducerSanitizerMatchesBinaryPatternTable(t *testing.T) {
	if got := len(neoBinarySecretRedactionPatterns); got != 112 {
		t.Fatalf("binary redaction pattern count = %d, want 112", got)
	}
	herokuToken := "012345678-ABCD-ABCD-ABCD-ABCDEF123456"
	redacted := sanitizeNeoBinaryReducerString("é heroku=\"" + herokuToken + "\"")
	if strings.Contains(redacted, herokuToken) || !strings.Contains(redacted, "[REDACTED:heroku-api-key]") {
		t.Fatalf("heroku redaction = %q", redacted)
	}
	if got := sanitizeNeoBinaryReducerString("api_key=example"); got != "api_key=example" {
		t.Fatalf("negative-lookahead redaction = %q, want unchanged example value", got)
	}
}

func TestNeoActorBinaryUserMessageSanitizesReducerPayload(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)

	githubToken := "ghp_" + strings.Repeat("A", 36)
	imageContent := "sk-ant-api03-" + strings.Repeat("B", 32)
	actor.handle(map[string]any{
		"type": "user:message",
		"message": map[string]any{
			"content": []any{
				map[string]any{"type": "text", "text": "token " + githubToken + "\U000E0001"},
				map[string]any{"type": "image", "data": imageContent, "caption": githubToken},
			},
			"userState": map[string]any{
				"activeEditor": "api_key=z9YxWv77",
			},
			"fileMentions": map[string]any{
				"files": []any{
					map[string]any{"uri": "file:///image.png", "isImage": true, "content": githubToken, "imageInfo": map[string]any{"alt": githubToken}},
					map[string]any{"uri": "file:///plain.txt", "content": githubToken},
				},
			},
		},
	})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.messages) != 1 {
		t.Fatalf("messages = %#v", actor.messages)
	}
	text := stringValue(mapValue(actor.messages[0].Content[0])["text"])
	if strings.Contains(text, githubToken) || strings.Contains(text, "\U000E0001") || !strings.Contains(text, "[REDACTED:github-pat]") {
		t.Fatalf("sanitized text = %q", text)
	}
	imageBlock := mapValue(actor.messages[0].Content[1])
	if stringValue(imageBlock["data"]) != imageContent || stringValue(imageBlock["caption"]) != githubToken {
		t.Fatalf("image block should be preserved like binary uo(): %#v", imageBlock)
	}
	userState := mapValue(actor.messages[0].UserState)
	if got := stringValue(userState["activeEditor"]); got != "api_key=[REDACTED:api-key]" {
		t.Fatalf("user state = %#v", userState)
	}
	files := arrayValue(mapValue(actor.messages[0].FileMentions)["files"])
	imageFile := mapValue(files[0])
	if stringValue(imageFile["content"]) != githubToken || stringValue(mapValue(imageFile["imageInfo"])["alt"]) != "[REDACTED:github-pat]" {
		t.Fatalf("image file mention = %#v", imageFile)
	}
	plainFile := mapValue(files[1])
	if strings.Contains(stringValue(plainFile["content"]), githubToken) || stringValue(plainFile["content"]) != "[REDACTED:github-pat]" {
		t.Fatalf("plain file mention = %#v", plainFile)
	}
}

func TestNeoActorBinaryThreadTruncateUsesArrayOrderLikeBinary(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.messages = []neoMessage{
		{ThreadID: "T-test", MessageID: "M-array-first", Role: "assistant", Content: []any{map[string]any{"type": "text", "text": "array first"}}, Seq: 3},
		{ThreadID: "T-test", MessageID: "M-array-cut", Role: "user", Content: []any{map[string]any{"type": "text", "text": "array cut"}}, Seq: 1},
		{ThreadID: "T-test", MessageID: "M-array-last", Role: "assistant", Content: []any{map[string]any{"type": "text", "text": "array last"}}, Seq: 2},
	}
	actor.relationships = []map[string]any{
		{"threadID": "T-019e1046-656d-7132-879f-390ded941c16", "type": "mention", "role": "parent", "createdAt": 1, "messageIndex": 0},
		{"threadID": "T-019e1046-656d-7132-879f-390ded941c17", "type": "mention", "role": "parent", "createdAt": 1, "messageIndex": 1},
	}

	actor.handle(map[string]any{"type": "thread:truncate", "fromIndex": 1})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.messages) != 1 || actor.messages[0].MessageID != "M-array-first" {
		t.Fatalf("messages after binary truncate = %#v, want current array prefix", actor.messages)
	}
	if len(actor.relationships) != 1 || numberFrom(actor.relationships[0]["messageIndex"]) != 0 {
		t.Fatalf("relationships after binary truncate = %#v, want current array prefix relationship", actor.relationships)
	}
}

func TestNeoActorBinaryUserMessageIndexCanReplaceNonUserLikeBinary(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.messages = []neoMessage{
		{ThreadID: "T-test", MessageID: "M-user", Role: "user", Content: []any{map[string]any{"type": "text", "text": "keep"}}, Seq: 1},
		{ThreadID: "T-test", MessageID: "M-assistant", Role: "assistant", Content: []any{map[string]any{"type": "text", "text": "replace"}}, Seq: 2},
		{ThreadID: "T-test", MessageID: "M-tail", Role: "assistant", Content: []any{map[string]any{"type": "text", "text": "tail"}}, Seq: 3},
	}
	actor.rebuildHistoryLocked()

	actor.handle(map[string]any{
		"type":  "user:message",
		"index": 1,
		"message": map[string]any{
			"content": []any{map[string]any{"type": "text", "text": "replacement user"}},
		},
	})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.messages) != 2 {
		t.Fatalf("messages = %#v, want prefix plus replacement", actor.messages)
	}
	if actor.messages[0].MessageID != "M-user" || actor.messages[1].Role != "user" || textFromBlocks(actor.messages[1].Content) != "replacement user" {
		t.Fatalf("messages after indexed binary user message = %#v", actor.messages)
	}
}

func TestNeoActorBinaryUserMessageIndexModeUsesFirstRoleUserLikeBinary(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.messages = []neoMessage{
		{ThreadID: "T-test", MessageID: "M-tool-result", Role: "user", Content: []any{map[string]any{"type": "tool_result", "toolUseID": "TU-old", "run": map[string]any{"status": "done"}}}, Seq: 1},
		{ThreadID: "T-test", MessageID: "M-user", Role: "user", Content: []any{map[string]any{"type": "text", "text": "real user"}}, Seq: 2},
	}
	actor.rebuildHistoryLocked()

	actor.handle(map[string]any{
		"type":  "user:message",
		"index": 0,
		"message": map[string]any{
			"agentMode": "deep",
			"content":   []any{map[string]any{"type": "text", "text": "replacement user"}},
		},
	})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if actor.settings["agentMode"] != "deep" || actor.currentAgentMode != "deep" {
		t.Fatalf("mode after replacing first role=user message = current:%q settings:%#v", actor.currentAgentMode, actor.settings)
	}
	if len(actor.messages) != 1 || actor.messages[0].Role != "user" || textFromBlocks(actor.messages[0].Content) != "replacement user" {
		t.Fatalf("messages after indexed replacement = %#v", actor.messages)
	}
}

func TestNeoActorBinaryUserMessageUpdatesStateWithoutSubmitting(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.executorReady = false
	actor.agentState = "idle"
	actor.draft = []any{map[string]any{"type": "text", "text": "draft"}}

	actor.handle(map[string]any{
		"type":            "user:message",
		"reasoningEffort": "xhigh",
		"message": map[string]any{
			"messageId":       "ignored-binary-id",
			"agentMode":       "deep",
			"reasoningEffort": "xhigh",
			"content":         []any{map[string]any{"type": "text", "text": "binary user"}},
		},
	})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.queue) != 0 {
		t.Fatalf("queue after binary user message = %#v, want no queued submit", actor.queue)
	}
	if actor.pendingInference != nil {
		t.Fatalf("pending inference after binary user message = %#v, want nil", actor.pendingInference)
	}
	if len(actor.messages) != 1 || actor.messages[0].Role != "user" || textFromBlocks(actor.messages[0].Content) != "binary user" {
		t.Fatalf("messages after binary user message = %#v", actor.messages)
	}
	if actor.messages[0].MessageID == "ignored-binary-id" || actor.messages[0].MessageID == "" {
		t.Fatalf("binary user message id = %q, want generated runtime id", actor.messages[0].MessageID)
	}
	if actor.settings["agentMode"] != "deep" || actor.settings["reasoning.effort"] != "xhigh" {
		t.Fatalf("thread settings after first binary user message = %#v", actor.settings)
	}
	if actor.title != "" {
		t.Fatalf("title = %q, want binary user delta not to generate title", actor.title)
	}
	if actor.draft != nil {
		t.Fatalf("draft after binary user message = %#v, want cleared", actor.draft)
	}
}

func TestNeoActorBinaryUserMessageIndexReplacesWithoutSubmitting(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.executorReady = false
	actor.messages = []neoMessage{
		{ThreadID: "T-test", MessageID: "M-user", Role: "user", AgentMode: "smart", Content: []any{map[string]any{"type": "text", "text": "old"}}, Seq: 1},
		{ThreadID: "T-test", MessageID: "M-assistant", Role: "assistant", Content: []any{map[string]any{"type": "text", "text": "answer"}}, Seq: 2},
	}
	actor.pendingInference = &neoInferenceInflight{agentMode: "smart", reasoningEffort: "high"}
	actor.rebuildHistoryLocked()

	actor.handle(map[string]any{
		"type":  "user:message",
		"index": 0,
		"message": map[string]any{
			"agentMode":       "deep",
			"reasoningEffort": "xhigh",
			"content":         []any{map[string]any{"type": "text", "text": "edited"}},
		},
	})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.messages) != 1 || actor.messages[0].Role != "user" || textFromBlocks(actor.messages[0].Content) != "edited" {
		t.Fatalf("messages after indexed binary user message = %#v", actor.messages)
	}
	if actor.pendingInference != nil {
		t.Fatalf("pending inference after indexed binary user message = %#v, want nil", actor.pendingInference)
	}
	if actor.settings["agentMode"] != "deep" {
		t.Fatalf("settings after indexed binary user message = %#v", actor.settings)
	}
	if actor.title != "" {
		t.Fatalf("title = %q, want indexed binary user delta not to generate title", actor.title)
	}
}

func TestNeoActorThreadTruncateBeforeCompactionBoundaryPreservesPrefix(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.messages = []neoMessage{
		{ThreadID: "T-test", MessageID: "M-old", Role: "user", Content: []any{map[string]any{"type": "text", "text": "old context"}}, Seq: 1},
		neoCompactionSummaryMessage("T-test", "compacted summary"),
		{ThreadID: "T-test", MessageID: "M-00000000000000000000aa", Role: "user", Content: []any{map[string]any{"type": "text", "text": "cut message"}}, Seq: 3},
		{ThreadID: "T-test", MessageID: "M-after", Role: "assistant", Content: []any{map[string]any{"type": "text", "text": "after"}}, Seq: 4},
	}
	actor.compactionRecords = []map[string]any{{"cutMessageId": "M-00000000000000000000aa", "createdAt": "2026-01-01T00:00:00Z"}}
	actor.rebuildHistoryLocked()

	actor.handle(map[string]any{"type": "thread:truncate", "fromIndex": 1})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.messages) != 1 || actor.messages[0].MessageID != "M-old" {
		t.Fatalf("messages after truncate before compaction boundary = %#v, want old prefix preserved", actor.messages)
	}
	if len(actor.history) != 1 || actor.history[0].Role != "user" || !strings.Contains(actor.history[0].Text, "old context") {
		t.Fatalf("history after truncate before compaction boundary = %#v, want old context", actor.history)
	}
}

func TestNeoActorBinaryThreadTruncateDefaultsMissingIndexToZero(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.messages = []neoMessage{
		{ThreadID: "T-test", MessageID: "M-user", Role: "user", Content: []any{map[string]any{"type": "text", "text": "hello"}}, Seq: 1},
		{ThreadID: "T-test", MessageID: "M-assistant", Role: "assistant", Content: []any{map[string]any{"type": "text", "text": "old"}}, Seq: 2},
	}
	actor.relationships = []map[string]any{
		{"threadID": "T-019e0e6e-f3f1-7081-b5dd-748f66f8c25d", "type": "mention", "role": "parent", "createdAt": 1, "messageIndex": 0},
		{"threadID": "T-019e0e6e-f3f1-7081-b5dd-748f66f8c25e", "type": "mention", "role": "parent", "createdAt": 2},
	}
	actor.pendingTools["TU-test"] = neoPendingTool{ID: "TU-test", MessageID: "M-assistant"}
	actor.approvalQueue = []map[string]any{{"toolCallId": "TU-test"}}
	actor.currentInference = &neoInferenceInflight{messageID: "M-assistant", agentMode: "smart"}
	actor.rebuildHistoryLocked()

	actor.handle(map[string]any{"type": "thread:truncate"})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.messages) != 0 {
		t.Fatalf("messages after missing-index truncate = %#v, want empty", actor.messages)
	}
	if len(actor.history) != 0 {
		t.Fatalf("history after missing-index truncate = %#v, want empty", actor.history)
	}
	if len(actor.relationships) != 1 || stringValue(actor.relationships[0]["threadID"]) != "T-019e0e6e-f3f1-7081-b5dd-748f66f8c25e" {
		t.Fatalf("relationships after missing-index truncate = %#v", actor.relationships)
	}
	if len(actor.pendingTools) != 0 || len(actor.approvalQueue) != 0 || actor.currentInference != nil {
		t.Fatalf("tool/inference state after truncate = tools:%#v approvals:%#v inference:%#v", actor.pendingTools, actor.approvalQueue, actor.currentInference)
	}
}

func TestNeoActorHandlesBinaryQueueDeltas(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)

	actor.handle(map[string]any{
		"type": "user:message-queue:enqueue",
		"message": map[string]any{
			"content":         []any{map[string]any{"type": "text", "text": "queued"}},
			"agentMode":       "deep",
			"reasoningEffort": "xhigh",
		},
	})

	actor.mu.Lock()
	if len(actor.queue) != 1 {
		t.Fatalf("queue = %#v", actor.queue)
	}
	queued := actor.queue[0]
	if queued.ID != "queued-1" || queued.ID == queued.MessageID || textFromBlocks(queued.Content) != "queued" || queued.AgentMode != "deep" {
		t.Fatalf("queued item = %#v", queued)
	}
	queueProtocol := queued.queueProtocol()
	actor.mu.Unlock()
	if stringValue(queueProtocol["id"]) != queued.ID {
		t.Fatalf("queue protocol = %#v", queueProtocol)
	}

	actor.handle(map[string]any{"type": "user:message-queue:discard", "id": queued.ID})
	actor.mu.Lock()
	if len(actor.queue) != 0 {
		t.Fatalf("queue after discard = %#v", actor.queue)
	}
	actor.mu.Unlock()
	waitForNeoActorSyncIdle(t, actor)
}

func TestNeoActorBinaryQueueDiscardUsesWrapperIDAndFindIndexFallback(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.queue = []neoQueuedMessage{
		{ID: "queued-a", MessageID: "M-a", Content: []any{map[string]any{"type": "text", "text": "a"}}},
		{ID: "queued-b", MessageID: "M-b", Content: []any{map[string]any{"type": "text", "text": "b"}}},
	}

	actor.handle(map[string]any{"type": "user:message-queue:discard", "id": "M-a"})

	actor.mu.Lock()
	if len(actor.queue) != 1 || actor.queue[0].ID != "queued-a" {
		t.Fatalf("queue after message-id discard = %#v, want binary findIndex fallback to remove last wrapper", actor.queue)
	}
	actor.mu.Unlock()

	actor.handle(map[string]any{"type": "user:message-queue:discard", "id": "queued-a"})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.queue) != 0 {
		t.Fatalf("queue after wrapper-id discard = %#v", actor.queue)
	}
}

func TestNeoActorBinaryQueueDiscardTreatsEmptyIDAsPresentLikeBinary(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.queue = []neoQueuedMessage{
		{ID: "queued-a", MessageID: "M-a", Content: []any{map[string]any{"type": "text", "text": "a"}}},
		{ID: "queued-b", MessageID: "M-b", Content: []any{map[string]any{"type": "text", "text": "b"}}},
	}

	actor.handle(map[string]any{"type": "user:message-queue:discard", "id": ""})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.queue) != 1 || actor.queue[0].ID != "queued-a" {
		t.Fatalf("queue after empty-id discard = %#v, want binary splice(-1) behavior", actor.queue)
	}
}

func TestNeoActorBinaryQueueEnqueueCapsAndDoesNotAutoRun(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.executorReady = true
	actor.agentState = "idle"

	for i := 0; i < neoMaxQueuedMessages+1; i++ {
		actor.handle(map[string]any{
			"type": "user:message-queue:enqueue",
			"message": map[string]any{
				"content": []any{map[string]any{"type": "text", "text": fmt.Sprintf("queued %d", i)}},
			},
		})
	}

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.queue) != neoMaxQueuedMessages {
		t.Fatalf("queue len = %d, want binary cap %d: %#v", len(actor.queue), neoMaxQueuedMessages, actor.queue)
	}
	for i, queued := range actor.queue {
		wantID := fmt.Sprintf("queued-%d", i+1)
		if queued.ID != wantID {
			t.Fatalf("queue id at %d = %q, want %q: %#v", i, queued.ID, wantID, actor.queue)
		}
	}
	if len(actor.messages) != 0 {
		t.Fatalf("messages = %#v, want enqueue to leave queued messages pending", actor.messages)
	}
}

func TestNeoActorBinaryQueueEnqueueIgnoresInboundWrapperID(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)

	actor.handle(map[string]any{
		"type": "user:message-queue:enqueue",
		"id":   "queued-client",
		"message": map[string]any{
			"messageId": "M-client",
			"content":   []any{map[string]any{"type": "text", "text": "queued"}},
		},
	})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.queue) != 1 {
		t.Fatalf("queue = %#v", actor.queue)
	}
	queued := actor.queue[0]
	if queued.ID != "queued-1" || queued.MessageID == "M-client" {
		t.Fatalf("queued item = %#v, want binary-owned wrapper and message ids", queued)
	}
}

func TestNeoActorBinaryQueueDequeueDoesNotRequireExecutorReady(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.executorReady = false
	actor.agentState = "idle"
	actor.queue = []neoQueuedMessage{{
		ID:              "queued-1",
		MessageID:       "M-queued",
		Content:         []any{map[string]any{"type": "text", "text": "run later"}},
		AgentMode:       "deep",
		ReasoningEffort: "xhigh",
	}}

	actor.handle(map[string]any{"type": "user:message-queue:dequeue"})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.queue) != 0 {
		t.Fatalf("queue after dequeue = %#v", actor.queue)
	}
	if len(actor.messages) != 1 || actor.messages[0].MessageID != "M-queued" || textFromBlocks(actor.messages[0].Content) != "run later" {
		t.Fatalf("messages after dequeue = %#v", actor.messages)
	}
	if actor.pendingInference == nil || actor.pendingInference.agentMode != "deep" || actor.pendingInference.reasoningEffort != "xhigh" {
		t.Fatalf("pending inference = %#v, want queued message to run when executor connects", actor.pendingInference)
	}
}

func TestNeoActorBinaryQueueDequeuePreservesQueuedMessageModeFields(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.executorReady = false
	actor.agentState = "idle"
	actor.currentAgentMode = "deep"
	actor.currentReasoningEffort = "xhigh"
	actor.settings["agentMode"] = "deep"
	actor.settings["reasoning.effort"] = "xhigh"
	actor.queue = []neoQueuedMessage{{
		ID:        "queued-1",
		MessageID: "M-queued",
		Content:   []any{map[string]any{"type": "text", "text": "run later"}},
	}}

	actor.handle(map[string]any{"type": "user:message-queue:dequeue"})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.messages) != 1 {
		t.Fatalf("messages after dequeue = %#v", actor.messages)
	}
	if actor.messages[0].AgentMode != "" || actor.messages[0].ReasoningEffort != "" {
		t.Fatalf("queued message gained mode fields: %#v", actor.messages[0])
	}
	if actor.pendingInference == nil || actor.pendingInference.agentMode != "deep" || actor.pendingInference.reasoningEffort != "xhigh" {
		t.Fatalf("pending inference = %#v, want resolved runtime mode", actor.pendingInference)
	}
}

func TestNeoActorQueuedRemovalEventUsesQueuedMessageID(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	server := httptest.NewServer(http.HandlerFunc(rt.handleHTTP))
	t.Cleanup(server.Close)

	conn := dialNeoActorWebSocket(t, server.URL, "T-queued-remove")
	defer conn.Close()

	if err := conn.WriteJSON(map[string]any{
		"type": "user:message-queue:enqueue",
		"id":   "queued-wrapper",
		"message": map[string]any{
			"content": []any{map[string]any{"type": "text", "text": "queued"}},
		},
	}); err != nil {
		t.Fatalf("write queue enqueue: %v", err)
	}

	added := waitForNeoMessageType(t, conn, "queued_message_added", 2*time.Second)
	item := mapValue(added["message"])
	queuedMessage := mapValue(item["queuedMessage"])
	wrapperID := stringValue(item["id"])
	if wrapperID != "queued-1" {
		t.Fatalf("queued wrapper id = %q, want binary-owned queued-1: %#v", wrapperID, added)
	}
	messageID := stringValue(queuedMessage["messageId"])
	if messageID == "" || messageID == "queued-wrapper" {
		t.Fatalf("queued message id = %q, wrapper=%q: %#v", messageID, stringValue(item["id"]), added)
	}

	if err := conn.WriteJSON(map[string]any{"type": "client_remove_queued_msg", "queuedMessageId": wrapperID}); err != nil {
		t.Fatalf("write remove queued: %v", err)
	}
	removed := waitForNeoMessageType(t, conn, "queued_message_removed", 2*time.Second)
	if got := stringValue(removed["queuedMessageId"]); got != messageID {
		t.Fatalf("removed queuedMessageId = %q, want message id %q: %#v", got, messageID, removed)
	}
}

func TestNeoActorQueuedDequeueEventUsesQueuedMessageID(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	server := httptest.NewServer(http.HandlerFunc(rt.handleHTTP))
	t.Cleanup(server.Close)

	conn := dialNeoActorWebSocket(t, server.URL, "T-queued-dequeue")
	defer conn.Close()

	if err := conn.WriteJSON(map[string]any{"type": "agent_state", "state": "running_tools"}); err != nil {
		t.Fatalf("write running state: %v", err)
	}
	if err := conn.WriteJSON(map[string]any{"type": "executor_connected", "executorId": "executor-test", "registeredToolCount": 0}); err != nil {
		t.Fatalf("write executor_connected: %v", err)
	}
	if err := conn.WriteJSON(map[string]any{
		"type": "user:message-queue:enqueue",
		"id":   "queued-wrapper",
		"message": map[string]any{
			"content": []any{map[string]any{"type": "text", "text": "queued"}},
		},
	}); err != nil {
		t.Fatalf("write queue enqueue: %v", err)
	}

	added := waitForNeoMessageType(t, conn, "queued_message_added", 2*time.Second)
	item := mapValue(added["message"])
	if got := stringValue(item["id"]); got != "queued-1" {
		t.Fatalf("queued wrapper id = %q, want binary-owned queued-1: %#v", got, added)
	}
	messageID := stringValue(mapValue(mapValue(added["message"])["queuedMessage"])["messageId"])
	if messageID == "" || messageID == "queued-wrapper" {
		t.Fatalf("queued message id = %q: %#v", messageID, added)
	}

	if err := conn.WriteJSON(map[string]any{"type": "agent_state", "state": "idle"}); err != nil {
		t.Fatalf("write idle state: %v", err)
	}
	if err := conn.WriteJSON(map[string]any{"type": "user:message-queue:dequeue"}); err != nil {
		t.Fatalf("write queue dequeue: %v", err)
	}
	dequeued := waitForNeoMessageType(t, conn, "queued_message_dequeued", 2*time.Second)
	if got := stringValue(dequeued["queuedMessageId"]); got != messageID {
		t.Fatalf("dequeued queuedMessageId = %q, want message id %q: %#v", got, messageID, dequeued)
	}
	waitForNeoActorSyncIdle(t, rt.store.ensureThreadActor("T-queued-dequeue"))
}

func TestNeoSendMessageToThreadWorkflowPromptsMatchBinary(t *testing.T) {
	targetID := "T-target-thread"

	codeReviewContent := neoSendMessageToThreadContent(map[string]any{"workflow": "code_review"}, targetID)
	if got := textFromBlocks(arrayValue(codeReviewContent)); got != "Review the changes with the code review tool." {
		t.Fatalf("code review workflow content = %q", got)
	}

	mergeContent := neoSendMessageToThreadContent(map[string]any{"input": map[string]any{"workflow": "merge_changes"}}, targetID)
	wantMerge := "Commit and merge the changes to a single commit on origin/main. Run the full test suite before pushing. If there's a non-trivial merge conflict, resolve it and confirm with me before pushing. After resolving any merge conflict, ensure the changes are properly formatted before committing and pushing. If test failures are unrelated to this change (due to a commit upstream that introduced the failure), they can be ignored. After the merge succeeds, run `amp threads archive T-target-thread` to archive this thread."
	if got := textFromBlocks(arrayValue(mergeContent)); got != wantMerge {
		t.Fatalf("merge workflow content = %q, want %q", got, wantMerge)
	}

	explicitContent := []any{map[string]any{"type": "text", "text": "custom message"}}
	if got := textFromBlocks(arrayValue(neoSendMessageToThreadContent(map[string]any{"workflow": "code_review", "content": explicitContent}, targetID))); got != "custom message" {
		t.Fatalf("explicit content should win over workflow, got %q", got)
	}
}

func TestNeoActorSendMessageToThreadWorkflowQueuesCanonicalPrompt(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	sourceID := "T-019e1046-656d-7132-879f-390ded941c18"
	targetID := "T-019e1046-656d-7132-879f-390ded941c19"
	source := rt.store.ensureThreadActor(sourceID)
	target := rt.store.ensureThreadActor(targetID)

	source.handle(map[string]any{
		"type":           "send_message_to_thread",
		"targetThreadId": targetID,
		"workflow":       "code_review",
	})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		target.mu.Lock()
		if len(target.queue) > 0 {
			got := textFromBlocks(target.queue[0].Content)
			target.mu.Unlock()
			if got != "Review the changes with the code review tool." {
				t.Fatalf("queued workflow message = %q", got)
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
		t.Fatal("timed out waiting for workflow message to queue on target thread")
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	if len(source.relationships) != 1 {
		t.Fatalf("source relationships = %#v", source.relationships)
	}
	relationship := source.relationships[0]
	if stringValue(relationship["threadID"]) != targetID || stringValue(relationship["type"]) != "mention" || stringValue(relationship["role"]) != "child" {
		t.Fatalf("source relationship = %#v", relationship)
	}
}

func TestNeoActorHandlesBinaryToolDeltas(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.messages = []neoMessage{{
		ThreadID:  "T-test",
		MessageID: "M-assistant",
		Role:      "assistant",
		Content: []any{map[string]any{
			"type":  "tool_use",
			"id":    "TU-1",
			"name":  "Bash",
			"input": map[string]any{"cmd": "pwd"},
		}},
		State: map[string]any{"type": "complete", "stopReason": "tool_use"},
		Seq:   1,
	}}
	actor.rebuildHistoryLocked()

	actor.handle(map[string]any{"type": "tool:processed", "toolUse": "TU-1", "newArgs": map[string]any{"cmd": "echo redacted"}})
	actor.mu.Lock()
	toolBlock := mapValue(actor.messages[0].Content[0])
	original := mapValue(mapValue(actor.messages[0].OriginalToolUseInput)["TU-1"])
	actor.mu.Unlock()
	if stringValue(mapValue(toolBlock["input"])["cmd"]) != "echo redacted" || stringValue(original["cmd"]) != "pwd" {
		t.Fatalf("processed tool block = %#v original=%#v", toolBlock, original)
	}

	actor.handle(map[string]any{"type": "tool:processed", "toolUse": "TU-1", "newArgs": map[string]any{"cmd": "printf redacted"}})
	actor.mu.Lock()
	toolBlock = mapValue(actor.messages[0].Content[0])
	original = mapValue(mapValue(actor.messages[0].OriginalToolUseInput)["TU-1"])
	actor.mu.Unlock()
	if stringValue(mapValue(toolBlock["input"])["cmd"]) != "printf redacted" || stringValue(original["cmd"]) != "echo redacted" {
		t.Fatalf("second processed tool block = %#v original=%#v", toolBlock, original)
	}

	actor.handle(map[string]any{"type": "tool:processed", "toolUse": "TU-1", "newArgs": map[string]any{}, "args": map[string]any{"cmd": "ignored fallback"}})
	actor.mu.Lock()
	toolBlock = mapValue(actor.messages[0].Content[0])
	original = mapValue(mapValue(actor.messages[0].OriginalToolUseInput)["TU-1"])
	actor.mu.Unlock()
	if len(mapValue(toolBlock["input"])) != 0 || stringValue(original["cmd"]) != "printf redacted" {
		t.Fatalf("empty processed tool block = %#v original=%#v", toolBlock, original)
	}

	actor.handle(map[string]any{"type": "tool:processed", "toolUse": "TU-1", "args": map[string]any{"cmd": "not used by binary"}})
	actor.mu.Lock()
	toolBlock = mapValue(actor.messages[0].Content[0])
	original = mapValue(mapValue(actor.messages[0].OriginalToolUseInput)["TU-1"])
	actor.mu.Unlock()
	if len(mapValue(toolBlock["input"])) != 0 || len(original) != 0 {
		t.Fatalf("missing-newArgs processed tool block = %#v original=%#v", toolBlock, original)
	}

	actor.handle(map[string]any{"type": "tool:data", "toolUse": "TU-1", "data": map[string]any{}, "run": map[string]any{"status": "done", "result": "ignored fallback"}})
	actor.mu.Lock()
	if len(actor.messages) != 2 {
		t.Fatalf("messages after empty tool data = %#v", actor.messages)
	}
	resultBlock := mapValue(actor.messages[1].Content[0])
	if len(mapValue(resultBlock["run"])) != 0 {
		t.Fatalf("empty tool data run = %#v", resultBlock)
	}
	actor.mu.Unlock()

	actor.handle(map[string]any{"type": "tool:data", "toolUse": "TU-1", "data": map[string]any{"status": "in-progress", "progress": map[string]any{"phase": "run"}}})
	actor.handle(map[string]any{"type": "user:tool-input", "toolUse": "TU-1", "value": map[string]any{"accepted": true}})
	actor.handle(map[string]any{"type": "tool:data", "toolUse": "TU-1", "data": map[string]any{"status": "done", "result": "ok"}})

	actor.mu.Lock()
	if len(actor.messages) != 2 {
		t.Fatalf("messages = %#v", actor.messages)
	}
	resultBlock = mapValue(actor.messages[1].Content[0])
	if stringValue(mapValue(resultBlock["run"])["status"]) != "done" || boolValue(mapValue(resultBlock["userInput"])["accepted"]) != true {
		t.Fatalf("tool result block = %#v", resultBlock)
	}
	if len(actor.history) != 2 || actor.history[1].Role != "tool" || actor.history[1].ToolCallID != "TU-1" {
		t.Fatalf("history = %#v", actor.history)
	}
	actor.mu.Unlock()
	waitForNeoActorSyncIdle(t, actor)
}

func TestNeoActorBinaryUserToolInputRequiresToolUse(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.messages = []neoMessage{{
		ThreadID:  "T-test",
		MessageID: "M-tool-result",
		Role:      "user",
		Content: []any{map[string]any{
			"type":      "tool_result",
			"toolUseID": "TU-orphan",
			"run":       map[string]any{"status": "done"},
		}},
		Seq: 1,
	}}
	actor.rebuildHistoryLocked()

	actor.handle(map[string]any{"type": "user:tool-input", "toolUse": "TU-orphan", "value": map[string]any{"accepted": true}})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	block := mapValue(actor.messages[0].Content[0])
	if _, exists := block["userInput"]; exists {
		t.Fatalf("orphan tool result was updated: %#v", block)
	}
}

func TestNeoActorBinaryToolDataGroupsResultsAfterAssistant(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.messages = []neoMessage{{
		ThreadID:  "T-test",
		MessageID: "M-assistant",
		Role:      "assistant",
		Content: []any{
			map[string]any{"type": "tool_use", "id": "TU-one", "name": "Bash", "input": map[string]any{"cmd": "pwd"}},
			map[string]any{"type": "tool_use", "id": "TU-two", "name": "Read", "input": map[string]any{"path": "README.md"}},
		},
		State: map[string]any{"type": "complete", "stopReason": "tool_use"},
		Seq:   1,
	}}
	actor.seq = 2
	actor.rebuildHistoryLocked()

	actor.handle(map[string]any{"type": "tool:data", "toolUse": "TU-one", "data": map[string]any{"status": "done", "result": "workspace"}})
	actor.handle(map[string]any{"type": "tool:data", "toolUse": "TU-two", "data": map[string]any{"status": "done", "result": "readme"}})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.messages) != 2 {
		t.Fatalf("messages len = %d, want assistant plus grouped tool result: %#v", len(actor.messages), actor.messages)
	}
	resultMessage := actor.messages[1]
	if resultMessage.Role != "user" || len(resultMessage.Content) != 2 {
		t.Fatalf("grouped result message = %#v", resultMessage)
	}
	first := mapValue(resultMessage.Content[0])
	second := mapValue(resultMessage.Content[1])
	if firstNonEmptyString(first["toolUseID"], first["toolUseId"]) != "TU-one" || firstNonEmptyString(second["toolUseID"], second["toolUseId"]) != "TU-two" {
		t.Fatalf("tool result order = %#v", resultMessage.Content)
	}
	if len(actor.history) != 3 || actor.history[1].ToolCallID != "TU-one" || actor.history[2].ToolCallID != "TU-two" {
		t.Fatalf("history = %#v", actor.history)
	}
}

func TestNeoActorBinaryToolDataStoresRawFindThreadResult(t *testing.T) {
	useTempNeoThreadStore(t)
	snapshot := neoCloudThreadSnapshot{
		threadID:  "T-search-target",
		seq:       2,
		createdMs: 1778170000000,
		title:     "Search target",
		messages: []neoMessage{
			{ThreadID: "T-search-target", MessageID: "M-user", Role: "user", Content: []any{map[string]any{"type": "text", "text": "needle context"}}, Seq: 1},
		},
	}
	if err := writeNeoLocalThreadSnapshot(snapshot); err != nil {
		t.Fatalf("writeNeoLocalThreadSnapshot error: %v", err)
	}

	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.messages = []neoMessage{{
		ThreadID:  "T-test",
		MessageID: "M-assistant",
		Role:      "assistant",
		Content: []any{map[string]any{
			"type":  "tool_use",
			"id":    "TU-find",
			"name":  "find_thread",
			"input": map[string]any{"query": "T-search-target", "limit": "5"},
		}},
		State: map[string]any{"type": "complete", "stopReason": "tool_use"},
		Seq:   1,
	}}
	actor.rebuildHistoryLocked()

	actor.handle(map[string]any{
		"type":    "tool:data",
		"toolUse": "TU-find",
		"data": map[string]any{
			"status": "done",
			"result": map[string]any{
				"hasMore": false,
				"threads": []any{map[string]any{
					"id":                "T-wrong-thread",
					"title":             "Wrong thread",
					"creatorUserID":     neoLocalOwnerUserID,
					"created":           float64(1778170000000),
					"updatedAt":         "2026-05-07T16:06:40Z",
					"messageCount":      float64(1),
					"matchedSearchText": "wrong result",
				}},
			},
		},
	})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.messages) != 2 {
		t.Fatalf("messages = %#v", actor.messages)
	}
	run := mapValue(mapValue(actor.messages[1].Content[0])["run"])
	result := mapValue(run["result"])
	threads := arrayValue(result["threads"])
	if len(threads) != 1 {
		t.Fatalf("threads = %#v", result["threads"])
	}
	thread := mapValue(threads[0])
	if thread["id"] != "T-wrong-thread" || stringValue(thread["matchedSearchText"]) != "wrong result" {
		t.Fatalf("binary tool:data result was locally normalized: %#v", thread)
	}
}

func TestNeoActorBinaryToolDataStoresRawImageRun(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.messages = []neoMessage{{
		ThreadID:  "T-test",
		MessageID: "M-assistant",
		Role:      "assistant",
		Content: []any{map[string]any{
			"type":  "tool_use",
			"id":    "TU-painter",
			"name":  "painter",
			"input": map[string]any{"prompt": "draw"},
		}},
		State: map[string]any{"type": "complete", "stopReason": "tool_use"},
		Seq:   1,
	}}
	actor.rebuildHistoryLocked()

	actor.handle(map[string]any{
		"type":    "tool:data",
		"toolUse": "TU-painter",
		"data": map[string]any{
			"status": "done",
			"images": []any{map[string]any{"mimeType": "image/png", "data": "abc"}},
		},
	})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.messages) != 2 {
		t.Fatalf("messages = %#v", actor.messages)
	}
	run := mapValue(mapValue(actor.messages[1].Content[0])["run"])
	if stringValue(run["status"]) != "done" || len(arrayValue(run["images"])) != 1 {
		t.Fatalf("raw tool run = %#v", run)
	}
	for _, key := range []string{"imageCount", "displayMessage", "result", "prompt"} {
		if _, exists := run[key]; exists {
			t.Fatalf("binary tool:data run gained normalized key %q: %#v", key, run)
		}
	}
}

func TestNeoActorBinaryToolDataSanitizesReducerPayload(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.messages = []neoMessage{{
		ThreadID:  "T-test",
		MessageID: "M-assistant",
		Role:      "assistant",
		Content: []any{map[string]any{
			"type":  "tool_use",
			"id":    "TU-secret",
			"name":  "Bash",
			"input": map[string]any{"cmd": "pwd"},
		}},
		State: map[string]any{"type": "complete", "stopReason": "tool_use"},
		Seq:   1,
	}}
	actor.rebuildHistoryLocked()

	githubToken := "ghp_" + strings.Repeat("A", 36)
	imageData := "sk-ant-api03-" + strings.Repeat("B", 32)
	actor.handle(map[string]any{
		"type":    "tool:data",
		"toolUse": "TU-secret",
		"data": map[string]any{
			"status": "done",
			"result": map[string]any{
				"output": "token=" + githubToken + "\U000E0001",
				"image": map[string]any{
					"type": "image",
					"data": imageData,
					"note": githubToken,
				},
			},
		},
	})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	run := mapValue(mapValue(actor.messages[1].Content[0])["run"])
	result := mapValue(run["result"])
	output := stringValue(result["output"])
	if strings.Contains(output, githubToken) || strings.Contains(output, "\U000E0001") || !strings.Contains(output, "[REDACTED:github-pat]") {
		t.Fatalf("sanitized output = %q", output)
	}
	image := mapValue(result["image"])
	if stringValue(image["data"]) != imageData || stringValue(image["note"]) != githubToken {
		t.Fatalf("image payload should be preserved like binary uo(): %#v", image)
	}
}

func TestNeoActorNormalizesTerminalToolProgressResult(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.pendingTools["TU-image"] = neoPendingTool{
		ID:    "TU-image",
		Name:  "render_agg_man",
		Input: map[string]any{"prompt": "draw the mascot"},
	}

	actor.handleToolProgress(map[string]any{
		"type":       "tool_progress",
		"toolCallId": "TU-image",
		"progress": map[string]any{
			"status": "done",
			"result": map[string]any{
				"images": []any{map[string]any{"b64_json": "abc123", "mime_type": "image/png"}},
			},
		},
	})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.messages) != 1 {
		t.Fatalf("messages = %#v", actor.messages)
	}
	run := mapValue(mapValue(actor.messages[0].Content[0])["run"])
	results := arrayValue(run["result"])
	if len(results) != 1 {
		t.Fatalf("binary image result = %#v", run["result"])
	}
	image := mapValue(results[0])
	if stringValue(image["type"]) != "image" || stringValue(image["mimeType"]) != "image/png" || stringValue(image["data"]) != "abc123" {
		t.Fatalf("binary image = %#v", image)
	}
	if len(actor.history) != 1 || actor.history[0].Text != "rendered 1 image" {
		t.Fatalf("history = %#v", actor.history)
	}
}

func TestNeoActorHandlesBinaryAssistantAndSettingsDeltas(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)

	actor.handle(map[string]any{"type": "agent-mode", "mode": "deep"})
	actor.handle(map[string]any{"type": "reasoning-effort", "effort": "xhigh"})
	actor.handle(map[string]any{"type": "title", "value": "Binary Title"})
	actor.handle(map[string]any{"type": "environment", "env": map[string]any{"workspaceRoot": "/tmp/work"}})
	actor.handle(map[string]any{"type": "assistant:message", "message": map[string]any{"content": []any{map[string]any{"type": "text", "text": "draft"}}, "state": map[string]any{"type": "streaming"}}})
	actor.handle(map[string]any{"type": "assistant:message-update", "message": map[string]any{"content": []any{map[string]any{"type": "text", "text": "done"}}, "state": map[string]any{"type": "complete", "stopReason": "end_turn"}, "usage": map[string]any{"inputTokens": 3}}})
	actor.handle(map[string]any{"type": "inference:completed", "model": "gpt-5.5", "usage": map[string]any{"outputTokens": 4}})

	actor.mu.Lock()
	if actor.settings["agentMode"] != "deep" || actor.settings["reasoning.effort"] != "xhigh" {
		t.Fatalf("settings = %#v", actor.settings)
	}
	if actor.title != "Binary Title" || actor.environment["workspaceRoot"] != "/tmp/work" {
		t.Fatalf("title/env = %q %#v", actor.title, actor.environment)
	}
	if len(actor.messages) != 1 || textFromBlocks(actor.messages[0].Content) != "done" {
		t.Fatalf("assistant messages = %#v", actor.messages)
	}
	if numberFrom(actor.messages[0].Usage["inputTokens"]) != 3 || numberFrom(actor.messages[0].Usage["outputTokens"]) != 4 {
		t.Fatalf("assistant usage = %#v", actor.messages[0].Usage)
	}
	if _, exists := actor.messages[0].Usage["model"]; exists {
		t.Fatalf("inference model should not be copied into usage: %#v", actor.messages[0].Usage)
	}
	tags := stringArrayValue(mapValue(actor.environment["initial"])["tags"])
	if len(tags) != 1 || stringValue(tags[0]) != "model:gpt-5.5" {
		t.Fatalf("environment tags = %#v", tags)
	}
	debugUsage := mapValue(mapValue(actor.debug["lastInferenceUsage"]))
	if numberFrom(debugUsage["outputTokens"]) != 4 {
		t.Fatalf("debug usage = %#v", actor.debug)
	}
	actor.mu.Unlock()
	waitForNeoActorSyncIdle(t, actor)
}

func TestNeoActorBinaryAssistantMessageIgnoresSuppliedIDLikeBinary(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)

	actor.handle(map[string]any{"type": "assistant:message", "message": map[string]any{
		"messageId": "M-supplied",
		"content":   []any{map[string]any{"type": "text", "text": "hello"}},
		"state":     map[string]any{"type": "complete", "stopReason": "end_turn"},
	}})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.messages) != 1 {
		t.Fatalf("messages = %#v", actor.messages)
	}
	if got := actor.messages[0].MessageID; got == "" || got == "M-supplied" {
		t.Fatalf("assistant message id = %q, want generated id", got)
	}
}

func TestNeoActorBinaryInferenceCompletedMirrorsEnvAndDebugSemantics(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.environment = map[string]any{"initial": map[string]any{"tags": []any{"repo:cliproxy", "model:undefined"}}}
	actor.debug = map[string]any{"lastInferenceUsage": map[string]any{"inputTokens": 9, "outputTokens": 1}}
	actor.messages = []neoMessage{{
		ThreadID:  "T-test",
		MessageID: "M-assistant",
		Role:      "assistant",
		Content:   []any{map[string]any{"type": "text", "text": "done"}},
		State:     map[string]any{"type": "complete", "stopReason": "end_turn"},
		Usage:     map[string]any{"inputTokens": 8},
		Seq:       1,
	}}

	actor.handle(map[string]any{"type": "inference:completed", "model": "claude-test", "usage": map[string]any{"inputTokens": 3, "outputTokens": 7}})

	actor.mu.Lock()
	tags := stringArrayValue(mapValue(actor.environment["initial"])["tags"])
	if len(tags) != 2 || stringValue(tags[0]) != "repo:cliproxy" || stringValue(tags[1]) != "model:claude-test" {
		actor.mu.Unlock()
		t.Fatalf("environment tags = %#v", tags)
	}
	usage := actor.messages[0].Usage
	if numberFrom(usage["inputTokens"]) != 8 || numberFrom(usage["outputTokens"]) != 7 {
		actor.mu.Unlock()
		t.Fatalf("assistant usage = %#v", usage)
	}
	if _, exists := usage["model"]; exists {
		actor.mu.Unlock()
		t.Fatalf("assistant usage should not receive inference model: %#v", usage)
	}
	debugUsage := mapValue(actor.debug["lastInferenceUsage"])
	if numberFrom(debugUsage["inputTokens"]) != 9 || numberFrom(debugUsage["outputTokens"]) != 7 {
		actor.mu.Unlock()
		t.Fatalf("debug usage = %#v", actor.debug)
	}
	actor.mu.Unlock()

	snapshot, ok := actor.threadSnapshot()
	if !ok {
		t.Fatal("threadSnapshot returned false")
	}
	thread := neoCloudThread(snapshot)
	if len(mapValue(thread["~debug"])) == 0 {
		t.Fatalf("cloud thread missing ~debug: %#v", thread)
	}
	cloudTags := stringArrayValue(mapValue(mapValue(thread["env"])["initial"])["tags"])
	if len(cloudTags) != 2 || stringValue(cloudTags[0]) != "repo:cliproxy" || stringValue(cloudTags[1]) != "model:claude-test" {
		t.Fatalf("cloud env tags = %#v in thread %#v", cloudTags, thread["env"])
	}
	waitForNeoActorSyncIdle(t, actor)
}

func TestNeoActorBinaryInferenceCompletedCreatesInitialTagsWithoutUsage(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)

	actor.handle(map[string]any{"type": "inference:completed"})

	actor.mu.Lock()
	tags := stringArrayValue(mapValue(actor.environment["initial"])["tags"])
	if len(tags) != 0 {
		actor.mu.Unlock()
		t.Fatalf("environment tags = %#v, want empty list", tags)
	}
	if len(actor.debug) != 0 {
		actor.mu.Unlock()
		t.Fatalf("debug = %#v, want empty", actor.debug)
	}
	actor.mu.Unlock()
	waitForNeoActorSyncIdle(t, actor)
}

func TestMergeNeoUsageMirrorsBinaryMaxSemantics(t *testing.T) {
	merged := mergeNeoUsage(
		map[string]any{
			"model":                    "old-model",
			"maxInputTokens":           100,
			"inputTokens":              10,
			"outputTokens":             5,
			"cacheCreationInputTokens": nil,
			"cacheReadInputTokens":     7,
			"totalInputTokens":         17,
			"thinkingBudget":           1000,
			"timestamp":                "old",
		},
		map[string]any{
			"model":                    "new-model",
			"maxInputTokens":           80,
			"inputTokens":              3,
			"outputTokens":             8,
			"cacheCreationInputTokens": 4,
			"cacheReadInputTokens":     nil,
			"totalInputTokens":         14,
			"timestamp":                "new",
		},
	)

	if stringValue(merged["model"]) != "new-model" ||
		numberFrom(merged["maxInputTokens"]) != 100 ||
		numberFrom(merged["inputTokens"]) != 10 ||
		numberFrom(merged["outputTokens"]) != 8 ||
		numberFrom(merged["cacheCreationInputTokens"]) != 4 ||
		numberFrom(merged["cacheReadInputTokens"]) != 7 ||
		numberFrom(merged["totalInputTokens"]) != 17 ||
		numberFrom(merged["thinkingBudget"]) != 1000 ||
		stringValue(merged["timestamp"]) != "new" {
		t.Fatalf("merged usage = %#v", merged)
	}
}

func TestNeoActorBinaryEnvironmentDeltaClearsExistingEnvironment(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.environment = map[string]any{"workspaceRoot": "/tmp/work", "shell": "zsh"}

	actor.handle(map[string]any{"type": "environment", "env": map[string]any{}})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.environment) != 0 {
		t.Fatalf("environment = %#v, want cleared empty environment", actor.environment)
	}
}

func TestNeoActorBinaryEnvironmentDeltaIgnoresAliasOnlyPayload(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.environment = map[string]any{"workspaceRoot": "/tmp/work", "shell": "zsh"}

	actor.handle(map[string]any{"type": "environment", "environment": map[string]any{"workspaceRoot": "/tmp/other"}})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.environment) != 0 {
		t.Fatalf("environment = %#v, want binary env field only", actor.environment)
	}
}

func TestNeoActorBinaryModeDeltasUseUserTurnBoundary(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.messages = []neoMessage{{
		ThreadID:  "T-test",
		MessageID: "M-info",
		Role:      "info",
		Content:   []any{map[string]any{"type": "text", "text": "system note"}},
		Seq:       1,
	}}

	actor.handle(map[string]any{"type": "agent-mode", "mode": "deep"})
	actor.handle(map[string]any{"type": "reasoning-effort", "effort": "xhigh"})

	actor.mu.Lock()
	if actor.settings["agentMode"] != "deep" || actor.settings["reasoning.effort"] != "xhigh" {
		t.Fatalf("settings before user turn = %#v", actor.settings)
	}
	actor.messages = append(actor.messages, neoMessage{
		ThreadID:  "T-test",
		MessageID: "M-user",
		Role:      "user",
		Content:   []any{map[string]any{"type": "text", "text": "hello"}},
		Seq:       2,
	})
	actor.mu.Unlock()

	actor.handle(map[string]any{"type": "agent-mode", "mode": "smart"})
	actor.handle(map[string]any{"type": "reasoning-effort", "effort": "high"})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if actor.settings["agentMode"] != "deep" || actor.settings["reasoning.effort"] != "xhigh" {
		t.Fatalf("settings after user turn = %#v", actor.settings)
	}
}

func TestNeoActorBinaryAgentModeDoesNotMaterializeDefaultEffort(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)

	actor.handle(map[string]any{"type": "agent-mode", "agentMode": "deep", "value": "deep"})
	actor.handle(map[string]any{"type": "reasoning-effort", "reasoningEffort": "xhigh", "value": "xhigh"})
	actor.mu.Lock()
	if _, exists := actor.settings["agentMode"]; exists {
		actor.mu.Unlock()
		t.Fatalf("agent-mode alias materialized settings: %#v", actor.settings)
	}
	actor.mu.Unlock()

	actor.handle(map[string]any{"type": "agent-mode", "mode": "deep"})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if actor.currentAgentMode != "deep" || actor.settings["agentMode"] != "deep" {
		t.Fatalf("agent mode = current:%q settings:%#v", actor.currentAgentMode, actor.settings)
	}
	if _, exists := actor.settings["reasoning.effort"]; exists {
		t.Fatalf("agent-mode materialized reasoning effort: %#v", actor.settings)
	}
	if got := actor.reasoningEffortForModeLocked("deep"); got != "medium" {
		t.Fatalf("resolved default effort = %q, want medium without explicit setting", got)
	}
}

func TestNeoActorBinaryReasoningEffortAbsentClearsExplicitEffort(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)

	actor.handle(map[string]any{"type": "agent-mode", "mode": "deep"})
	actor.handle(map[string]any{"type": "reasoning-effort", "effort": "xhigh"})
	actor.handle(map[string]any{"type": "reasoning-effort"})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if actor.currentReasoningEffort != "" {
		t.Fatalf("current reasoning effort = %q, want cleared", actor.currentReasoningEffort)
	}
	if _, exists := actor.settings["reasoning.effort"]; exists {
		t.Fatalf("settings kept reasoning effort after absent effort delta: %#v", actor.settings)
	}
	if got := actor.reasoningEffortForModeLocked("deep"); got != "medium" {
		t.Fatalf("resolved default effort = %q, want medium after clearing explicit setting", got)
	}
}

func TestNeoActorBinaryReasoningEffortRequiresAgentModeLikeBinary(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)

	actor.handle(map[string]any{"type": "reasoning-effort", "effort": "max"})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if _, exists := actor.settings["reasoning.effort"]; exists {
		t.Fatalf("reasoning effort was set before agent-mode: %#v", actor.settings)
	}
	if actor.currentReasoningEffort != "high" {
		t.Fatalf("current reasoning effort = %q, want default high", actor.currentReasoningEffort)
	}
}

func TestNeoActorBinaryScalarDeltasUseFalseyClears(t *testing.T) {
	useTempNeoThreadStore(t)
	parentID := "T-019e1046-656d-7132-879f-390ded941c16"
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)

	actor.handle(map[string]any{"type": "title", "value": "Binary Title"})
	actor.handle(map[string]any{"type": "max-tokens", "value": 32000})
	actor.handle(map[string]any{"type": "main-thread", "value": parentID})
	actor.handle(map[string]any{"type": "setPendingNavigation", "threadID": parentID})

	actor.handle(map[string]any{"type": "title", "value": "  Binary Title  "})
	actor.mu.Lock()
	if actor.title != "  Binary Title  " {
		t.Fatalf("binary title = %q, want whitespace preserved", actor.title)
	}
	actor.mu.Unlock()

	actor.handle(map[string]any{"type": "title", "value": "", "title": "ignored fallback"})
	actor.handle(map[string]any{"type": "max-tokens", "value": "", "maxTokens": 64000})
	actor.handle(map[string]any{"type": "main-thread", "value": "", "threadID": parentID})
	actor.handle(map[string]any{"type": "setPendingNavigation", "threadID": ""})
	actor.handle(map[string]any{"type": "max-tokens", "maxTokens": 64000})
	actor.handle(map[string]any{"type": "main-thread", "threadID": parentID})

	actor.mu.Lock()
	if actor.title != "" {
		t.Fatalf("title = %q, want cleared empty value", actor.title)
	}
	if actor.maxTokens != nil {
		t.Fatalf("maxTokens = %#v, want cleared empty value", actor.maxTokens)
	}
	if _, ok := actor.settings["maxTokens"]; ok {
		t.Fatalf("settings maxTokens should be cleared: %#v", actor.settings)
	}
	if actor.mainThreadID != "" {
		t.Fatalf("mainThreadID = %q, want cleared empty value", actor.mainThreadID)
	}
	if _, ok := actor.settings["mainThreadID"]; ok {
		t.Fatalf("settings mainThreadID should be cleared: %#v", actor.settings)
	}
	if actor.pendingNavigation != "" {
		t.Fatalf("pendingNavigation = %q, want cleared empty threadID", actor.pendingNavigation)
	}
	actor.mu.Unlock()

	snapshot, ok := actor.threadSnapshot()
	if !ok {
		t.Fatal("threadSnapshot returned false")
	}
	thread := neoCloudThread(snapshot)
	for _, key := range []string{"maxTokens", "mainThreadID", "pendingNavigation"} {
		if _, exists := thread[key]; exists {
			t.Fatalf("%s should be omitted after binary falsey clear: %#v", key, thread[key])
		}
	}
}

func TestNeoNormalizeMaxTokensValueMirrorsBinaryTruthiness(t *testing.T) {
	for _, value := range []any{0, int64(0), float64(0), json.Number("0"), false, ""} {
		if got := neoNormalizeMaxTokensValue(value); got != nil {
			t.Fatalf("neoNormalizeMaxTokensValue(%#v) = %#v, want nil", value, got)
		}
	}
	if got := neoNormalizeMaxTokensValue(-1); got != -1 {
		t.Fatalf("neoNormalizeMaxTokensValue(-1) = %#v, want -1", got)
	}
	if got := neoNormalizeMaxTokensValue("0"); got != "0" {
		t.Fatalf("neoNormalizeMaxTokensValue(%q) = %#v, want %q", "0", got, "0")
	}
}

func TestNeoActorBinaryAssistantUpdateAppendsWhenLastIsNotAssistant(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.messages = []neoMessage{
		{
			ThreadID:  "T-test",
			MessageID: "M-assistant-old",
			Role:      "assistant",
			Content:   []any{map[string]any{"type": "text", "text": "old assistant"}},
			State:     map[string]any{"type": "complete", "stopReason": "tool_use"},
			Seq:       1,
		},
		{
			ThreadID:  "T-test",
			MessageID: "M-tool-result",
			Role:      "user",
			Content: []any{map[string]any{
				"type":      "tool_result",
				"toolUseID": "TU-old",
				"run":       map[string]any{"status": "done", "result": "ok"},
			}},
			Seq: 2,
		},
	}
	actor.rebuildHistoryLocked()

	actor.handle(map[string]any{
		"type": "assistant:message-update",
		"message": map[string]any{
			"messageId": "M-assistant-old",
			"content":   []any{map[string]any{"type": "text", "text": "new assistant"}},
			"state":     map[string]any{"type": "complete", "stopReason": "end_turn"},
		},
	})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.messages) != 3 {
		t.Fatalf("messages = %#v, want old assistant, tool result, appended assistant", actor.messages)
	}
	if actor.messages[0].MessageID != "M-assistant-old" || textFromBlocks(actor.messages[0].Content) != "old assistant" {
		t.Fatalf("old assistant was overwritten: %#v", actor.messages[0])
	}
	appended := actor.messages[2]
	if appended.Role != "assistant" || appended.MessageID == "" || appended.MessageID == "M-assistant-old" || textFromBlocks(appended.Content) != "new assistant" {
		t.Fatalf("appended assistant = %#v", appended)
	}
}

func TestNeoActorBinaryAssistantUpdateIgnoresSuppliedIDAndUpdatesLastAssistant(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.messages = []neoMessage{
		{
			ThreadID:  "T-test",
			MessageID: "M-other",
			Role:      "assistant",
			Content:   []any{map[string]any{"type": "text", "text": "other assistant"}},
			State:     map[string]any{"type": "complete", "stopReason": "end_turn"},
			Seq:       1,
		},
		{
			ThreadID:  "T-test",
			MessageID: "M-last",
			Role:      "assistant",
			Content:   []any{map[string]any{"type": "text", "text": "old last"}},
			State:     map[string]any{"type": "streaming"},
			Usage:     map[string]any{"inputTokens": 10},
			Seq:       2,
		},
	}
	actor.rebuildHistoryLocked()

	actor.handle(map[string]any{
		"type": "assistant:message-update",
		"message": map[string]any{
			"messageId": "M-other",
			"content":   []any{map[string]any{"type": "text", "text": "new last"}},
			"state":     map[string]any{"type": "complete", "stopReason": "end_turn"},
			"usage":     map[string]any{"inputTokens": 3, "outputTokens": 4},
		},
	})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.messages) != 2 {
		t.Fatalf("messages = %#v", actor.messages)
	}
	if textFromBlocks(actor.messages[0].Content) != "other assistant" {
		t.Fatalf("non-last assistant was overwritten: %#v", actor.messages[0])
	}
	last := actor.messages[1]
	if last.MessageID != "M-last" || textFromBlocks(last.Content) != "new last" {
		t.Fatalf("last assistant = %#v", last)
	}
	if numberFrom(last.Usage["inputTokens"]) != 10 || numberFrom(last.Usage["outputTokens"]) != 4 {
		t.Fatalf("last assistant usage = %#v", last.Usage)
	}
	if len(actor.replayEvents) == 0 || actor.replayEvents[len(actor.replayEvents)-1].Payload["type"] != "message_updated" {
		t.Fatalf("last replay event = %#v, want message_updated", actor.replayEvents)
	}
}

func TestNeoActorBinaryAssistantMessageCleansPriorIncompleteAssistant(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.messages = []neoMessage{{
		ThreadID:  "T-test",
		MessageID: "M-stale",
		Role:      "assistant",
		Content: []any{map[string]any{
			"type":            "tool_use",
			"id":              "TU-stale",
			"name":            "Bash",
			"complete":        false,
			"inputIncomplete": map[string]any{"cmd": "sleep 60"},
		}},
		State: map[string]any{"type": "streaming"},
		Seq:   1,
	}}
	actor.pendingTools["TU-stale"] = neoPendingTool{ID: "TU-stale", Name: "Bash", MessageID: "M-stale"}
	actor.seq = 2
	actor.rebuildHistoryLocked()

	actor.handle(map[string]any{"type": "assistant:message", "message": map[string]any{
		"content": []any{map[string]any{"type": "text", "text": "next"}},
		"state":   map[string]any{"type": "complete", "stopReason": "end_turn"},
	}})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.messages) != 2 {
		t.Fatalf("messages = %#v, want stale assistant and new assistant", actor.messages)
	}
	stale := actor.messages[0]
	if stringValue(mapValue(stale.State)["type"]) != "cancelled" {
		t.Fatalf("stale assistant state = %#v", stale.State)
	}
	toolUse := mapValue(stale.Content[0])
	if !boolValue(toolUse["complete"]) || stringValue(mapValue(toolUse["input"])["cmd"]) != "sleep 60" {
		t.Fatalf("stale tool use = %#v", toolUse)
	}
	if _, exists := toolUse["blockState"]; exists {
		t.Fatalf("stale tool use kept blockState: %#v", toolUse)
	}
	if _, exists := toolUse["inputIncomplete"]; exists {
		t.Fatalf("stale tool use kept inputIncomplete: %#v", toolUse)
	}
	if _, exists := toolUse["inputPartialJSON"]; exists {
		t.Fatalf("stale tool use kept inputPartialJSON: %#v", toolUse)
	}
	if _, ok := actor.pendingTools["TU-stale"]; ok {
		t.Fatalf("stale pending tool was not removed: %#v", actor.pendingTools)
	}
	if textFromBlocks(actor.messages[1].Content) != "next" {
		t.Fatalf("new assistant = %#v", actor.messages[1])
	}
}

func TestNeoActorBinaryUserMessageCleanupUsesBinaryToolResultShape(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.messages = []neoMessage{{
		ThreadID:  "T-test",
		MessageID: "M-stale",
		Role:      "assistant",
		Content: []any{map[string]any{
			"type":             "tool_use",
			"id":               "TU-stale",
			"name":             "Bash",
			"complete":         false,
			"blockState":       "streaming",
			"inputPartialJSON": map[string]any{"json": `{"cmd":"sleep 60"`},
			"inputIncomplete":  map[string]any{"cmd": "sleep 60"},
		}},
		State: map[string]any{"type": "streaming"},
		Seq:   1,
	}}
	actor.pendingTools["TU-stale"] = neoPendingTool{ID: "TU-stale", Name: "Bash", MessageID: "M-stale"}
	actor.seq = 2
	actor.rebuildHistoryLocked()

	actor.handle(map[string]any{
		"type": "user:message",
		"message": map[string]any{
			"content": []any{map[string]any{"type": "text", "text": "next"}},
		},
	})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.messages) != 3 {
		t.Fatalf("messages = %#v, want stale assistant, cancelled tool result, and new user", actor.messages)
	}
	toolUse := mapValue(actor.messages[0].Content[0])
	if len(toolUse) != 5 || toolUse["type"] != "tool_use" || toolUse["id"] != "TU-stale" || toolUse["name"] != "Bash" || !boolValue(toolUse["complete"]) || stringValue(mapValue(toolUse["input"])["cmd"]) != "sleep 60" {
		t.Fatalf("stale tool use = %#v, want exact binary completed shape", toolUse)
	}
	result := mapValue(actor.messages[1].Content[0])
	run := mapValue(result["run"])
	if stringValue(run["status"]) != "cancelled" || stringValue(run["reason"]) != "user:interrupted" {
		t.Fatalf("cancelled run = %#v", run)
	}
	if textFromBlocks(actor.messages[2].Content) != "next" {
		t.Fatalf("new user = %#v", actor.messages[2])
	}
}

func TestNeoActorBinaryUserMessageCancelsActiveToolProgress(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.messages = []neoMessage{
		{ThreadID: "T-test", MessageID: "M-user", Role: "user", Content: []any{map[string]any{"type": "text", "text": "run sleep"}}, Seq: 1},
		{ThreadID: "T-test", MessageID: "M-assistant", Role: "assistant", Content: []any{map[string]any{"type": "tool_use", "id": "TU-sleep", "name": "Bash", "input": map[string]any{"cmd": "sleep 60"}}}, State: map[string]any{"type": "complete", "stopReason": "tool_use"}, Seq: 2},
		{ThreadID: "T-test", MessageID: "M-sleep", Role: "user", Content: []any{map[string]any{"type": "tool_result", "toolUseID": "TU-sleep", "run": map[string]any{"status": "in-progress", "progress": map[string]any{"phase": "running"}}}}, CompletionStatus: "tool_progress", Seq: 3},
	}
	actor.pendingTools["TU-sleep"] = neoPendingTool{ID: "TU-sleep", Name: "Bash", MessageID: "M-assistant"}
	actor.currentInference = &neoInferenceInflight{messageID: "M-assistant", agentMode: "smart"}
	actor.agentState = "running_tools"
	actor.rebuildHistoryLocked()

	actor.handle(map[string]any{
		"type": "user:message",
		"message": map[string]any{
			"content": []any{map[string]any{"type": "text", "text": "stop and do this instead"}},
		},
	})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.pendingTools) != 0 || actor.currentInference != nil || actor.agentState != "idle" {
		t.Fatalf("interrupt state = pending:%#v inference:%#v agent:%q", actor.pendingTools, actor.currentInference, actor.agentState)
	}
	if len(actor.messages) != 4 {
		t.Fatalf("messages = %#v, want original user, assistant, cancelled progress, new user", actor.messages)
	}
	run := mapValue(mapValue(actor.messages[2].Content[0])["run"])
	if stringValue(run["status"]) != "cancelled" || stringValue(run["reason"]) != "user:interrupted" || len(mapValue(run["progress"])) == 0 {
		t.Fatalf("progress run after user interrupt = %#v", run)
	}
	if actor.messages[2].CompletionStatus != "" {
		t.Fatalf("progress message completion status = %q", actor.messages[2].CompletionStatus)
	}
	if textFromBlocks(actor.messages[3].Content) != "stop and do this instead" {
		t.Fatalf("new user = %#v", actor.messages[3])
	}
}

func TestNeoActorBinaryCleanupRemovesEmptyStreamingAssistant(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.messages = []neoMessage{
		{ThreadID: "T-test", MessageID: "M-user", Role: "user", Content: []any{map[string]any{"type": "text", "text": "hello"}}, Seq: 1},
		{ThreadID: "T-test", MessageID: "M-empty", Role: "assistant", Content: []any{}, State: map[string]any{"type": "streaming"}, Seq: 2},
	}
	actor.currentInference = &neoInferenceInflight{messageID: "M-empty", agentMode: "smart"}
	actor.rebuildHistoryLocked()

	actor.handle(map[string]any{
		"type": "user:message",
		"message": map[string]any{
			"content": []any{map[string]any{"type": "text", "text": "next"}},
		},
	})

	actor.mu.Lock()
	if len(actor.messages) != 2 {
		actor.mu.Unlock()
		t.Fatalf("messages = %#v, want original user and next user", actor.messages)
	}
	if actor.messages[0].MessageID != "M-user" || actor.messages[1].Role != "user" || textFromBlocks(actor.messages[1].Content) != "next" {
		actor.mu.Unlock()
		t.Fatalf("messages after cleanup = %#v", actor.messages)
	}
	if actor.currentInference != nil {
		actor.mu.Unlock()
		t.Fatalf("current inference = %#v, want cleared", actor.currentInference)
	}
	if len(actor.replayEvents) < 2 || actor.replayEvents[len(actor.replayEvents)-2].Payload["type"] != "thread_truncated" {
		actor.mu.Unlock()
		t.Fatalf("replay events = %#v, want truncation before user message", actor.replayEvents)
	}
	actor.mu.Unlock()
	waitForNeoActorSyncIdle(t, actor)
}

func TestNeoActorHandlesResidualBinaryThreadDeltas(t *testing.T) {
	useTempNeoThreadStore(t)
	threadID := "T-019e0e6e-f3f1-7081-b5dd-748f66f8c25d"
	parentID := "T-019e0e6e-f3f1-7081-b5dd-748f66f8c25e"
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", threadID, threadID, neoActorRecord("actor-test", "thread-actor", threadID), nil)

	actor.handle(map[string]any{
		"type":    "info:manual-bash-invocation",
		"args":    map[string]any{"cmd": "git", "args": []any{"status"}},
		"toolRun": map[string]any{"status": "done", "result": "clean"},
		"hidden":  true,
	})
	actor.handle(map[string]any{
		"type":         "relationship",
		"relationship": map[string]any{"threadID": parentID, "type": "mention", "role": "parent", "createdAt": 123},
	})
	actor.handle(map[string]any{"type": "draft", "content": []any{map[string]any{"type": "text", "text": "draft text"}}, "autoSubmit": true})
	actor.handle(map[string]any{"type": "setPendingNavigation", "threadID": parentID})
	actor.handle(map[string]any{"type": "setPendingNavigation", "threadID": "  " + parentID + "  "})

	actor.mu.Lock()
	if actor.pendingNavigation != "  "+parentID+"  " {
		t.Fatalf("pendingNavigation = %q, want binary whitespace preserved", actor.pendingNavigation)
	}
	actor.mu.Unlock()

	actor.handle(map[string]any{"type": "setPendingNavigation", "threadID": parentID})
	actor.handle(map[string]any{"type": "max-tokens", "value": 32000})
	actor.handle(map[string]any{"type": "main-thread", "value": parentID})
	actor.handle(map[string]any{"type": "trace:start", "span": map[string]any{"id": "trace-1", "name": "turn", "startTime": "2026-05-24T00:00:00Z"}})
	actor.handle(map[string]any{"type": "trace:event", "span": "trace-1", "event": map[string]any{"name": "tool-start"}})
	actor.handle(map[string]any{"type": "trace:attributes", "span": "trace-1", "attributes": map[string]any{"phase": "run"}})
	actor.handle(map[string]any{"type": "trace:end", "span": map[string]any{"id": "trace-1", "endTime": "2026-05-24T00:00:01Z"}})
	actor.handle(map[string]any{"type": "clearPendingNavigation"})

	actor.mu.Lock()
	if len(actor.messages) != 1 || actor.messages[0].Role != "info" {
		t.Fatalf("messages = %#v", actor.messages)
	}
	block := mapValue(actor.messages[0].Content[0])
	if stringValue(block["type"]) != "manual_bash_invocation" || stringValue(mapValue(block["toolRun"])["result"]) != "clean" || !boolValue(block["hidden"]) {
		t.Fatalf("manual bash block = %#v", block)
	}
	if len(actor.relationships) != 1 || stringValue(actor.relationships[0]["threadID"]) != parentID {
		t.Fatalf("relationships = %#v", actor.relationships)
	}
	if textFromBlocks(actor.draft) != "draft text" || !actor.autoSubmitDraft {
		t.Fatalf("draft = %#v auto=%v", actor.draft, actor.autoSubmitDraft)
	}
	if actor.pendingNavigation != "" || numberFrom(actor.maxTokens) != 32000 || actor.mainThreadID != parentID {
		t.Fatalf("navigation/max/main = %q %#v %q", actor.pendingNavigation, actor.maxTokens, actor.mainThreadID)
	}
	traces := arrayValue(actor.meta["traces"])
	if len(traces) != 1 {
		t.Fatalf("traces = %#v", actor.meta)
	}
	trace := mapValue(traces[0])
	if stringValue(trace["endTime"]) != "2026-05-24T00:00:01Z" || stringValue(mapValue(trace["attributes"])["phase"]) != "run" || len(arrayValue(trace["events"])) != 1 {
		t.Fatalf("trace = %#v", trace)
	}
	actor.mu.Unlock()

	snapshot, ok := actor.threadSnapshot()
	if !ok {
		t.Fatal("threadSnapshot returned false")
	}
	thread := neoCloudThread(snapshot)
	if numberFrom(thread["maxTokens"]) != 32000 || stringValue(thread["mainThreadID"]) != parentID {
		t.Fatalf("cloud max/main = %#v", thread)
	}
	if textFromBlocks(arrayValue(thread["draft"])) != "draft text" || thread["autoSubmitDraft"] != true {
		t.Fatalf("cloud draft = %#v auto=%#v", thread["draft"], thread["autoSubmitDraft"])
	}
	if _, exists := thread["pendingNavigation"]; exists {
		t.Fatalf("pendingNavigation should be cleared in cloud thread: %#v", thread["pendingNavigation"])
	}
	meta := mapValue(thread["meta"])
	if meta["cliProxyAPILocalNeo"] != true || len(arrayValue(meta["traces"])) != 1 {
		t.Fatalf("cloud meta = %#v", meta)
	}
	waitForNeoActorSyncIdle(t, actor)
}

func TestNeoActorBinaryTraceDeltasIgnoreMissingOrCompletedSpansLikeBinary(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)

	actor.handle(map[string]any{"type": "trace:event", "span": "missing", "event": map[string]any{"name": "ignored"}})
	actor.handle(map[string]any{"type": "trace:attributes", "span": "missing", "attributes": map[string]any{"ignored": true}})
	actor.handle(map[string]any{"type": "trace:end", "span": map[string]any{"id": "missing", "endTime": "2026-05-24T00:00:01Z"}})

	actor.mu.Lock()
	if len(actor.replayEvents) != 0 {
		actor.mu.Unlock()
		t.Fatalf("missing trace deltas replayed events: %#v", actor.replayEvents)
	}
	actor.mu.Unlock()

	actor.handle(map[string]any{"type": "trace:start", "span": map[string]any{"id": "trace-1", "startTime": "2026-05-24T00:00:00Z"}})
	actor.handle(map[string]any{"type": "trace:start", "span": map[string]any{"id": "trace-1", "startTime": "2026-05-24T00:00:02Z"}})
	actor.handle(map[string]any{"type": "trace:attributes", "span": "trace-1", "attributes": map[string]any{}})
	actor.handle(map[string]any{"type": "trace:end", "span": map[string]any{"id": "trace-1", "endTime": "2026-05-24T00:00:03Z"}})
	actor.handle(map[string]any{"type": "trace:end", "span": map[string]any{"id": "trace-1", "endTime": "2026-05-24T00:00:04Z"}})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.replayEvents) != 3 {
		t.Fatalf("trace replay events = %#v, want start, empty attributes, and end mutations", actor.replayEvents)
	}
	trace := mapValue(arrayValue(actor.meta["traces"])[0])
	if stringValue(trace["startTime"]) != "2026-05-24T00:00:00Z" || stringValue(trace["endTime"]) != "2026-05-24T00:00:03Z" {
		t.Fatalf("trace = %#v, want first start/end preserved", trace)
	}
	if attributes := mapValue(trace["attributes"]); len(attributes) != 0 {
		t.Fatalf("trace attributes = %#v, want empty object preserved", attributes)
	}
}

func TestNeoRuntimeImportRestoresResidualThreadState(t *testing.T) {
	threadID := "T-019e0e6e-f3f1-7081-b5dd-748f66f8c25d"
	parentID := "T-019e0e6e-f3f1-7081-b5dd-748f66f8c25e"
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", threadID, threadID, neoActorRecord("actor-test", "thread-actor", threadID), nil)

	err := actor.importThreadLocalOnly(map[string]any{
		"id":                threadID,
		"v":                 7,
		"agentMode":         "deep",
		"messages":          []any{},
		"meta":              map[string]any{"visibility": "private", "traces": []any{map[string]any{"id": "trace-1", "startTime": "2026-05-24T00:00:00Z"}}},
		"~debug":            map[string]any{"lastInferenceUsage": map[string]any{"inputTokens": 12}},
		"draft":             []any{map[string]any{"type": "text", "text": "restore draft"}},
		"autoSubmitDraft":   true,
		"pendingNavigation": parentID,
		"maxTokens":         64000,
		"mainThreadID":      parentID,
	})
	if err != nil {
		t.Fatalf("importThreadLocalOnly returned error: %v", err)
	}

	state := actor.stateSnapshotResponse()
	if got := numberFrom(state["seq"]); got != 7 {
		t.Fatalf("state seq = %d, want 7", got)
	}
	if textFromBlocks(arrayValue(state["draft"])) != "restore draft" || state["autoSubmitDraft"] != true {
		t.Fatalf("state draft = %#v auto=%#v", state["draft"], state["autoSubmitDraft"])
	}
	if stringValue(state["pendingNavigation"]) != parentID || numberFrom(state["maxTokens"]) != 64000 || stringValue(state["mainThreadID"]) != parentID {
		t.Fatalf("state residual fields = %#v", state)
	}
	meta := mapValue(state["meta"])
	if stringValue(meta["visibility"]) != "private" || len(arrayValue(meta["traces"])) != 1 {
		t.Fatalf("state meta = %#v", meta)
	}
	debug := mapValue(state["~debug"])
	if numberFrom(mapValue(debug["lastInferenceUsage"])["inputTokens"]) != 12 {
		t.Fatalf("state debug = %#v", debug)
	}
}

func TestNeoActorCancelledMarksLastToolResult(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.messages = []neoMessage{{
		ThreadID:  "T-test",
		MessageID: "M-tool-result",
		Role:      "user",
		Content: []any{map[string]any{
			"type":      "tool_result",
			"toolUseID": "TU-1",
			"run":       map[string]any{"status": "in-progress"},
		}},
		Seq: 1,
	}}
	actor.rebuildHistoryLocked()

	actor.handle(map[string]any{"type": "cancelled"})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	run := mapValue(mapValue(actor.messages[0].Content[0])["run"])
	if stringValue(run["status"]) != "cancelled" {
		t.Fatalf("tool result run = %#v", run)
	}
}

func TestNeoActorProtocolCancelledUsesCurrentInferenceMessageID(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.messages = []neoMessage{{
		ThreadID:  "T-test",
		MessageID: "M-assistant",
		Role:      "assistant",
		State:     map[string]any{"type": "streaming"},
		Seq:       1,
	}}
	actor.currentInference = &neoInferenceInflight{messageID: "M-assistant", agentMode: "smart"}

	actor.handle(map[string]any{"type": "cancelled"})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.replayEvents) == 0 {
		t.Fatal("missing replay events")
	}
	event := actor.replayEvents[len(actor.replayEvents)-1].Payload
	if event["type"] != "cancelled" || stringValue(event["messageId"]) != "M-assistant" {
		t.Fatalf("cancelled event = %#v, want current inference id", event)
	}
}

func TestNeoActorCancelledDoesNotRewriteTerminalToolResult(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.messages = []neoMessage{{
		ThreadID:  "T-test",
		MessageID: "M-tool-result",
		Role:      "user",
		Content: []any{map[string]any{
			"type":      "tool_result",
			"toolUseID": "TU-1",
			"run":       map[string]any{"status": "done", "result": "workspace"},
		}},
		Seq: 1,
	}}
	actor.rebuildHistoryLocked()

	actor.handle(map[string]any{"type": "cancelled"})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	run := mapValue(mapValue(actor.messages[0].Content[0])["run"])
	if stringValue(run["status"]) != "done" || stringValue(run["result"]) != "workspace" {
		t.Fatalf("terminal tool result was rewritten on cancel: %#v", run)
	}
}

func TestNeoActorCancelledMarksAllApprovalToolResults(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.messages = []neoMessage{{
		ThreadID:  "T-test",
		MessageID: "M-tool-result",
		Role:      "user",
		Content: []any{
			map[string]any{"type": "tool_result", "toolUseID": "TU-1", "run": map[string]any{"status": "blocked-on-user"}},
			map[string]any{"type": "tool_result", "toolUseID": "TU-2", "run": map[string]any{"status": "blocked-on-user"}},
		},
		CompletionStatus: "tool_progress",
		Seq:              1,
	}}
	actor.approvalQueue = []map[string]any{{"toolCallId": "TU-1"}, {"toolCallId": "TU-2"}}
	actor.agentState = "awaiting_approval"
	actor.rebuildHistoryLocked()

	actor.handle(map[string]any{"type": "cancelled"})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	for _, raw := range actor.messages[0].Content {
		run := mapValue(mapValue(raw)["run"])
		if stringValue(run["status"]) != "cancelled" || stringValue(run["reason"]) != "user:cancelled" {
			t.Fatalf("approval tool result was not cancelled: %#v", actor.messages[0].Content)
		}
	}
}

func TestNeoActorCancelledCleansIncompleteAssistantLikeBinary(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.messages = []neoMessage{
		{
			ThreadID:  "T-test",
			MessageID: "M-user",
			Role:      "user",
			Content:   []any{map[string]any{"type": "text", "text": "run pwd"}},
			Seq:       1,
		},
		{
			ThreadID:  "T-test",
			MessageID: "M-assistant",
			Role:      "assistant",
			Content: []any{map[string]any{
				"type":             "tool_use",
				"id":               "TU-incomplete",
				"name":             "Bash",
				"input":            map[string]any{},
				"inputPartialJSON": map[string]any{"json": `{"cmd":"pwd"`},
				"inputIncomplete":  map[string]any{"cmd": "pwd"},
				"complete":         false,
			}},
			State: map[string]any{"type": "streaming"},
			Seq:   2,
		},
	}
	actor.pendingTools["TU-incomplete"] = neoPendingTool{ID: "TU-incomplete", Name: "Bash", MessageID: "M-assistant"}
	actor.rebuildHistoryLocked()

	actor.handle(map[string]any{"type": "cancelled", "messageId": "M-assistant"})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.pendingTools) != 0 {
		t.Fatalf("pending tools = %#v, want none", actor.pendingTools)
	}
	if len(actor.messages) != 3 {
		t.Fatalf("messages = %#v, want user, cancelled assistant, cancelled tool result", actor.messages)
	}
	assistant := actor.messages[1]
	if stringValue(mapValue(assistant.State)["type"]) != "cancelled" {
		t.Fatalf("assistant state = %#v, want cancelled", assistant.State)
	}
	toolUse := mapValue(assistant.Content[0])
	if !boolValue(toolUse["complete"]) || stringValue(mapValue(toolUse["input"])["cmd"]) != "pwd" {
		t.Fatalf("tool use = %#v, want completed with incomplete input", toolUse)
	}
	if _, exists := toolUse["inputPartialJSON"]; exists {
		t.Fatalf("tool use still has inputPartialJSON: %#v", toolUse)
	}
	result := mapValue(actor.messages[2].Content[0])
	run := mapValue(result["run"])
	if firstNonEmptyString(result["toolUseID"], result["toolUseId"]) != "TU-incomplete" || stringValue(run["status"]) != "cancelled" {
		t.Fatalf("tool result = %#v", result)
	}
}

func useTempNeoThreadStore(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	oldStoreDir := neoAmpThreadStoreDir
	neoAmpThreadStoreDir = func() string { return dir }
	t.Cleanup(func() { neoAmpThreadStoreDir = oldStoreDir })
}

func TestNeoLocalThreadLoadDropsStaleCurrentInference(t *testing.T) {
	useTempNeoThreadStore(t)
	threadID := "T-019e6541-06ae-75d7-b10e-d893170fa62c"
	raw := []byte(`{
		"id": "` + threadID + `",
		"agentMode": "deep",
		"messages": [
			{"messageId": "M-existing", "role": "user", "content": [{"type": "text", "text": "hello"}]}
		],
		"currentInference": {"messageId": "M-missing", "agentMode": "deep", "tools": ["shell_command"]}
	}`)
	path := filepath.Join(neoAmpThreadStoreDir(), threadID+".json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write thread: %v", err)
	}

	thread, ok := loadNeoLocalThread(threadID)
	if !ok {
		t.Fatal("thread was not loaded")
	}
	if _, exists := thread["currentInference"]; exists {
		t.Fatalf("currentInference was not dropped: %#v", thread["currentInference"])
	}
	persistedRaw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read persisted thread: %v", err)
	}
	var persisted map[string]any
	if err := json.Unmarshal(persistedRaw, &persisted); err != nil {
		t.Fatalf("decode persisted thread: %v", err)
	}
	if _, exists := persisted["currentInference"]; exists {
		t.Fatalf("stale currentInference was re-cached: %#v", persisted["currentInference"])
	}
}

func TestNeoLocalThreadLoadCancelsStaleLocalCurrentInference(t *testing.T) {
	useTempNeoThreadStore(t)
	threadID := "T-019e6541-06ae-75d7-b10e-d893170fa62c"
	raw := []byte(`{
		"id": "` + threadID + `",
		"agentMode": "deep",
		"meta": {"cliProxyAPILocalNeo": true},
		"messages": [
			{"messageId": "M-user", "role": "user", "content": [{"type": "text", "text": "hello"}]},
			{"messageId": "M-assistant", "role": "assistant", "state": {"type": "streaming"}, "content": [{"type": "text", "text": "partial"}]}
		],
		"currentInference": {"messageId": "M-assistant", "agentMode": "deep", "tools": ["shell_command"]}
	}`)
	path := filepath.Join(neoAmpThreadStoreDir(), threadID+".json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write thread: %v", err)
	}

	thread, ok := loadNeoLocalThread(threadID)
	if !ok {
		t.Fatal("thread was not loaded")
	}
	if _, exists := thread["currentInference"]; exists {
		t.Fatalf("currentInference was not dropped: %#v", thread["currentInference"])
	}
	messages := arrayValue(thread["messages"])
	if len(messages) != 2 {
		t.Fatalf("messages = %#v, want stale assistant preserved as cancelled", messages)
	}
	assistant := mapValue(messages[1])
	if stringValue(mapValue(assistant["state"])["type"]) != "cancelled" {
		t.Fatalf("assistant state = %#v, want cancelled", assistant["state"])
	}

	persistedRaw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read persisted thread: %v", err)
	}
	var persisted map[string]any
	if err := json.Unmarshal(persistedRaw, &persisted); err != nil {
		t.Fatalf("decode persisted thread: %v", err)
	}
	if _, exists := persisted["currentInference"]; exists {
		t.Fatalf("stale currentInference was re-cached: %#v", persisted["currentInference"])
	}
	persistedMessages := arrayValue(persisted["messages"])
	persistedAssistant := mapValue(persistedMessages[1])
	if stringValue(mapValue(persistedAssistant["state"])["type"]) != "cancelled" {
		t.Fatalf("persisted assistant state = %#v, want cancelled", persistedAssistant["state"])
	}
}

func TestNeoLocalThreadLoadRemovesEmptyStaleLocalCurrentInference(t *testing.T) {
	useTempNeoThreadStore(t)
	threadID := "T-019e6541-06ae-75d7-b10e-d893170fa62c"
	raw := []byte(`{
		"id": "` + threadID + `",
		"agentMode": "deep",
		"meta": {"cliProxyAPILocalNeo": true},
		"messages": [
			{"messageId": "M-user", "role": "user", "content": [{"type": "text", "text": "hello"}]},
			{"messageId": "M-empty", "role": "assistant", "state": {"type": "streaming"}, "content": []}
		],
		"currentInference": {"messageId": "M-empty", "agentMode": "deep", "tools": ["shell_command"]}
	}`)
	path := filepath.Join(neoAmpThreadStoreDir(), threadID+".json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write thread: %v", err)
	}

	thread, ok := loadNeoLocalThread(threadID)
	if !ok {
		t.Fatal("thread was not loaded")
	}
	if _, exists := thread["currentInference"]; exists {
		t.Fatalf("currentInference was not dropped: %#v", thread["currentInference"])
	}
	messages := arrayValue(thread["messages"])
	if len(messages) != 1 || messageIDValue(mapValue(messages[0])["messageId"]) != "M-user" {
		t.Fatalf("messages = %#v, want empty stale assistant removed", messages)
	}
}

func TestNeoActorImportRestoresPendingInference(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	threadID := "T-019e6541-06ae-75d7-b10e-d893170fa62c"
	actor := newNeoActor(rt, "actor-pending", "thread-actor", threadID, threadID, neoActorRecord("actor-pending", "thread-actor", threadID), nil)
	thread := map[string]any{
		"id":        threadID,
		"agentMode": "deep",
		"messages": []any{
			map[string]any{"messageId": "M-user", "role": "user", "content": []any{map[string]any{"type": "text", "text": "continue after restart"}}},
			map[string]any{"messageId": "M-assistant", "role": "assistant", "state": map[string]any{"type": "cancelled"}, "content": []any{map[string]any{"type": "text", "text": "partial"}}},
		},
		"pendingInference": map[string]any{"messageId": "M-assistant", "agentMode": "deep", "reasoningEffort": "xhigh", "parentToolCallId": "TU-parent", "tools": []any{"shell_command"}},
	}
	if err := actor.importThreadLocalOnly(thread); err != nil {
		t.Fatalf("import thread: %v", err)
	}
	if actor.pendingInference == nil {
		t.Fatal("pendingInference was not restored")
	}
	if actor.pendingInference.agentMode != "deep" || actor.pendingInference.reasoningEffort != "xhigh" || actor.pendingInference.parentToolCallID != "TU-parent" {
		t.Fatalf("pendingInference = %#v, want deep/xhigh with parent", actor.pendingInference)
	}
	if actor.currentInference != nil {
		t.Fatalf("currentInference = %#v, want nil until retry starts", actor.currentInference)
	}
}

func TestNeoLocalThreadLoadRepairsLocalCompactionSummaryOrder(t *testing.T) {
	useTempNeoThreadStore(t)
	threadID := "T-019e6541-06ae-75d7-b10e-d893170fa62c"
	raw := []byte(`{
		"id": "` + threadID + `",
		"agentMode": "deep",
		"meta": {"cliProxyAPILocalNeo": true},
		"compactionRecords": [{"cutMessageId": "M-0000000000000000000002", "createdAt": "2026-05-30T00:00:00Z"}],
		"messages": [
			{"messageId": "M-0000000000000000000001", "role": "user", "content": [{"type": "text", "text": "old context"}]},
			{"messageId": "M-0000000000000000000002", "role": "user", "content": [{"type": "text", "text": "cut context"}]},
			{"messageId": "M-0000000000000000000003", "role": "assistant", "state": {"type": "complete", "stopReason": "end_turn"}, "content": [{"type": "text", "text": "tail context"}]},
			{"messageId": "M-0000000000000000000004", "role": "info", "content": [{"type": "summary", "summary": {"type": "message", "summary": "compacted context"}}]}
		]
	}`)
	path := filepath.Join(neoAmpThreadStoreDir(), threadID+".json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write thread: %v", err)
	}

	thread, ok := loadNeoLocalThread(threadID)
	if !ok {
		t.Fatal("thread was not loaded")
	}
	messages := arrayValue(thread["messages"])
	if got := firstNonEmptyString(mapValue(messages[1])["messageId"], mapValue(messages[1])["protocolMessageID"]); got != "M-0000000000000000000004" {
		t.Fatalf("summary message was not moved before cut: %q in %#v", got, messages)
	}
	if got := firstNonEmptyString(mapValue(messages[2])["messageId"], mapValue(messages[2])["protocolMessageID"]); got != "M-0000000000000000000002" {
		t.Fatalf("cut message moved incorrectly: %q in %#v", got, messages)
	}

	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", threadID, threadID, neoActorRecord("actor-test", "thread-actor", threadID), nil)
	if err := actor.importThreadLocalOnly(thread); err != nil {
		t.Fatalf("importThreadLocalOnly error: %v", err)
	}
	actor.mu.Lock()
	historyText := fmt.Sprint(actor.history)
	actor.mu.Unlock()
	if !strings.Contains(historyText, "compacted context") || !strings.Contains(historyText, "cut context") || !strings.Contains(historyText, "tail context") {
		t.Fatalf("history after repaired compaction lost retained context: %s", historyText)
	}
	if strings.Contains(historyText, "old context") {
		t.Fatalf("history after repaired compaction leaked old context: %s", historyText)
	}

	persistedRaw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read persisted thread: %v", err)
	}
	var persisted map[string]any
	if err := json.Unmarshal(persistedRaw, &persisted); err != nil {
		t.Fatalf("decode persisted thread: %v", err)
	}
	persistedMessages := arrayValue(persisted["messages"])
	if got := firstNonEmptyString(mapValue(persistedMessages[1])["messageId"], mapValue(persistedMessages[1])["protocolMessageID"]); got != "M-0000000000000000000004" {
		t.Fatalf("repaired compaction order was not persisted: %q", got)
	}
}

func TestNeoLocalThreadLoadLeavesUpstreamCompactionOrderUntouched(t *testing.T) {
	useTempNeoThreadStore(t)
	threadID := "T-019e6541-06ae-75d7-b10e-d893170fa62d"
	raw := []byte(`{
		"id": "` + threadID + `",
		"agentMode": "deep",
		"compactionRecords": [{"cutMessageId": "M-0000000000000000000002", "createdAt": "2026-05-30T00:00:00Z"}],
		"messages": [
			{"messageId": "M-0000000000000000000001", "role": "user", "content": [{"type": "text", "text": "old context"}]},
			{"messageId": "M-0000000000000000000002", "role": "user", "content": [{"type": "text", "text": "cut context"}]},
			{"messageId": "M-0000000000000000000003", "role": "assistant", "state": {"type": "complete", "stopReason": "end_turn"}, "content": [{"type": "text", "text": "tail context"}]},
			{"messageId": "M-0000000000000000000004", "role": "info", "content": [{"type": "summary", "summary": {"type": "message", "summary": "compacted context"}}]}
		]
	}`)
	path := filepath.Join(neoAmpThreadStoreDir(), threadID+".json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write thread: %v", err)
	}

	thread, ok := loadNeoLocalThread(threadID)
	if !ok {
		t.Fatal("thread was not loaded")
	}
	messages := arrayValue(thread["messages"])
	if got := firstNonEmptyString(mapValue(messages[3])["messageId"], mapValue(messages[3])["protocolMessageID"]); got != "M-0000000000000000000004" {
		t.Fatalf("upstream-shaped thread summary order changed: %q in %#v", got, messages)
	}
}

func TestNeoActorThreadSnapshotDropsStaleCurrentInference(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.messages = []neoMessage{{ThreadID: "T-test", MessageID: "M-existing", Role: "user", Content: []any{map[string]any{"type": "text", "text": "hello"}}, Seq: 1}}
	actor.currentInference = &neoInferenceInflight{messageID: "M-missing", agentMode: "deep", tools: []string{"shell_command"}}

	snapshot, ok := actor.threadSnapshot()
	if !ok {
		t.Fatal("snapshot failed")
	}
	if snapshot.currentInference != nil {
		t.Fatalf("snapshot currentInference = %#v, want nil", snapshot.currentInference)
	}
	if actor.currentInference != nil {
		t.Fatalf("actor currentInference = %#v, want nil", actor.currentInference)
	}
	if thread := neoCloudThread(snapshot); thread["currentInference"] != nil {
		t.Fatalf("cloud thread retained currentInference: %#v", thread["currentInference"])
	}
}

func TestNeoActorThreadSnapshotIncludesQueuedMessages(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.queue = []neoQueuedMessage{{
		MessageID:       "M-queued",
		Content:         []any{map[string]any{"type": "text", "text": "continue"}},
		UserState:       map[string]any{"cwd": "/tmp/work"},
		FileMentions:    map[string]any{"paths": []any{"README.md"}},
		Meta:            map[string]any{"source": "queue"},
		CreatedAt:       "2026-05-24T00:00:00Z",
		AgentMode:       "deep",
		ReasoningEffort: "xhigh",
	}}

	snapshot, ok := actor.threadSnapshot()
	if !ok {
		t.Fatal("threadSnapshot returned false")
	}
	thread := neoCloudThread(snapshot)
	queued := arrayValue(thread["queuedMessages"])
	if len(queued) != 1 {
		t.Fatalf("queuedMessages = %#v", thread["queuedMessages"])
	}
	item := mapValue(queued[0])
	message := mapValue(item["queuedMessage"])
	if stringValue(item["id"]) != "M-queued" || stringValue(message["messageId"]) != "M-queued" || textFromBlocks(arrayValue(message["content"])) != "continue" {
		t.Fatalf("queued message payload = %#v", item)
	}
	if stringValue(message["agentMode"]) != "deep" || stringValue(message["reasoningEffort"]) != "xhigh" || stringValue(mapValue(message["userState"])["cwd"]) != "/tmp/work" {
		t.Fatalf("queued message metadata = %#v", message)
	}
	paths := arrayValue(mapValue(message["fileMentions"])["paths"])
	if stringValue(mapValue(message["meta"])["source"]) != "queue" || len(paths) != 1 || stringValue(paths[0]) != "README.md" {
		t.Fatalf("queued message meta/fileMentions = %#v", message)
	}
}

func TestNeoRuntimeThreadImportRestoresQueuedMessages(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-import", "T-import", neoActorRecord("actor-test", "thread-actor", "T-import"), nil)

	if err := actor.importThreadLocalOnly(map[string]any{
		"id":        "T-import",
		"agentMode": "smart",
		"queuedMessages": []any{map[string]any{
			"id": "queued-1",
			"queuedMessage": map[string]any{
				"role":            "user",
				"messageId":       "M-queued",
				"content":         []any{map[string]any{"type": "text", "text": "continue later"}},
				"userState":       map[string]any{"cwd": "/tmp/work"},
				"meta":            map[string]any{"source": "persisted"},
				"createdAt":       "2026-05-24T00:00:00Z",
				"agentMode":       "deep",
				"reasoningEffort": "xhigh",
			},
		}},
	}); err != nil {
		t.Fatalf("importThreadLocalOnly error: %v", err)
	}

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.queue) != 1 {
		t.Fatalf("queue = %#v", actor.queue)
	}
	queued := actor.queue[0]
	if queued.MessageID != "M-queued" || textFromBlocks(queued.Content) != "continue later" || queued.AgentMode != "deep" || queued.ReasoningEffort != "xhigh" {
		t.Fatalf("queued message = %#v", queued)
	}
	if stringValue(mapValue(queued.UserState)["cwd"]) != "/tmp/work" || stringValue(queued.Meta["source"]) != "persisted" {
		t.Fatalf("queued metadata = userState:%#v meta:%#v", queued.UserState, queued.Meta)
	}
}

func TestNeoActorPropagatesParentToolCallToNestedAssistantAndLease(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)

	actor.finishAssistantMessageWithOptions("M-child", neoInferenceResult{
		Text: "child output",
		ToolCalls: []neoToolCall{{
			ID:    "TU-child",
			Name:  "Bash",
			Input: map[string]any{"cmd": "pwd"},
		}},
	}, "smart", "high", false, "TU-parent")

	actor.mu.Lock()
	if len(actor.messages) != 1 {
		actor.mu.Unlock()
		t.Fatalf("messages = %#v", actor.messages)
	}
	message := actor.messages[0]
	pending := actor.pendingTools["TU-child"]
	actor.mu.Unlock()

	if message.ParentToolUseID != "TU-parent" {
		t.Fatalf("message parent = %q, want TU-parent", message.ParentToolUseID)
	}
	if pending.ParentToolCallID != "TU-parent" {
		t.Fatalf("pending parent = %q, want TU-parent", pending.ParentToolCallID)
	}
	if got := stringValue(message.protocol()["parentToolUseId"]); got != "TU-parent" {
		t.Fatalf("protocol parent = %q, want TU-parent", got)
	}
	if got := stringValue(neoMessageAddedPayload(message)["parentToolUseId"]); got != "TU-parent" {
		t.Fatalf("message_added parent = %q, want TU-parent", got)
	}
	if got := stringValue(neoCloudMessage(message)["parentToolUseId"]); got != "TU-parent" {
		t.Fatalf("cloud parent = %q, want TU-parent", got)
	}

	imported := neoMessageFromImportedThread("T-test", map[string]any{
		"role":            "assistant",
		"messageId":       "M-imported",
		"content":         []any{map[string]any{"type": "text", "text": "nested"}},
		"parentToolUseId": "TU-parent",
	}, 0)
	if imported.ParentToolUseID != "TU-parent" {
		t.Fatalf("imported parent = %q, want TU-parent", imported.ParentToolUseID)
	}
}

func TestNeoCloudMessageSanitizesUserMetaLikeBinary(t *testing.T) {
	executorThreadID := "T-019e1046-656d-7132-879f-390ded941c16"
	cloud := neoCloudMessage(neoMessage{
		Role:      "user",
		MessageID: "M-user",
		Content:   []any{map[string]any{"type": "text", "text": "hello"}},
		Meta: map[string]any{
			"sentAt":               float64(1778170001000),
			"aggman":               "yes",
			"fromExecutorThreadID": executorThreadID,
			"source":               "persisted",
			"accountID":            "local-proxy-account",
		},
	})

	meta := mapValue(cloud["meta"])
	if numberFrom(meta["sentAt"]) != 1778170001000 {
		t.Fatalf("sentAt = %#v, want binary numeric sentAt", meta["sentAt"])
	}
	if meta["fromAggman"] != true {
		t.Fatalf("fromAggman = %#v, want true from aggman truthy alias", meta["fromAggman"])
	}
	if stringValue(meta["fromExecutorThreadID"]) != executorThreadID {
		t.Fatalf("fromExecutorThreadID = %#v, want %s", meta["fromExecutorThreadID"], executorThreadID)
	}
	for _, key := range []string{"source", "accountID", "aggman"} {
		if _, ok := meta[key]; ok {
			t.Fatalf("meta[%s] = %#v, want omitted like binary import", key, meta[key])
		}
	}

	invalid := neoCloudMessage(neoMessage{
		Role:      "user",
		MessageID: "M-invalid",
		Content:   []any{map[string]any{"type": "text", "text": "hello"}},
		Meta: map[string]any{
			"fromExecutorThreadID": "T-019E1046-656D-7132-879F-390DED941C16",
		},
	})
	if _, ok := invalid["meta"]; ok {
		t.Fatalf("invalid meta = %#v, want omitted for non-binary thread id", invalid["meta"])
	}
}

func TestNeoCloudMessageNormalizesUserStateForBinaryImport(t *testing.T) {
	cloud := neoCloudMessage(neoMessage{
		Role:      "user",
		MessageID: "M-user",
		Content:   []any{map[string]any{"type": "text", "text": "hello"}},
		UserState: map[string]any{
			"cwd":                     "/tmp/work",
			"runningTerminalCommands": "not-an-array",
			"aggmanContext": map[string]any{
				"availableProjects": []any{map[string]any{"name": "project"}},
			},
		},
	})

	userState := mapValue(cloud["userState"])
	if stringValue(userState["cwd"]) != "/tmp/work" {
		t.Fatalf("userState cwd = %#v, want preserved cwd", userState)
	}
	if files := arrayValue(userState["currentlyVisibleFiles"]); len(files) != 0 {
		t.Fatalf("currentlyVisibleFiles = %#v, want empty binary-safe array", files)
	}
	if _, exists := userState["runningTerminalCommands"]; exists {
		t.Fatalf("runningTerminalCommands = %#v, want omitted when not an array", userState["runningTerminalCommands"])
	}
	projects := arrayValue(mapValue(userState["aggmanContext"])["availableProjects"])
	if len(projects) != 1 || stringValue(mapValue(projects[0])["name"]) != "project" {
		t.Fatalf("aggmanContext = %#v, want cloned available projects", userState["aggmanContext"])
	}
}

func TestNeoMessageFromImportedThreadSanitizesUserMetaLikeBinary(t *testing.T) {
	executorThreadID := "T-019e1046-656d-7132-879f-390ded941c16"
	imported := neoMessageFromImportedThread("T-test", map[string]any{
		"role":      "user",
		"messageId": "M-user",
		"content":   []any{map[string]any{"type": "text", "text": "hello"}},
		"userState": map[string]any{"cwd": "/tmp/work"},
		"meta": map[string]any{
			"sentAt":               1778170001000,
			"fromAggman":           false,
			"aggman":               true,
			"fromExecutorThreadID": executorThreadID,
			"source":               "persisted",
		},
	}, 0)

	if numberFrom(imported.Meta["sentAt"]) != 1778170001000 {
		t.Fatalf("sentAt = %#v, want binary numeric sentAt", imported.Meta["sentAt"])
	}
	if imported.Meta["fromAggman"] != true {
		t.Fatalf("fromAggman = %#v, want true from aggman alias", imported.Meta["fromAggman"])
	}
	if stringValue(imported.Meta["fromExecutorThreadID"]) != executorThreadID {
		t.Fatalf("fromExecutorThreadID = %#v, want %s", imported.Meta["fromExecutorThreadID"], executorThreadID)
	}
	if _, ok := imported.Meta["source"]; ok {
		t.Fatalf("source = %#v, want omitted like binary import", imported.Meta["source"])
	}
	if !neoMessageIDPattern.MatchString(imported.MessageID) || imported.MessageID == "M-user" {
		t.Fatalf("messageID = %q, want generated binary-valid id for invalid import id", imported.MessageID)
	}
	userState := mapValue(imported.UserState)
	if stringValue(userState["cwd"]) != "/tmp/work" || arrayValue(userState["currentlyVisibleFiles"]) == nil {
		t.Fatalf("userState = %#v, want binary-safe user state", userState)
	}

	assistant := neoMessageFromImportedThread("T-test", map[string]any{
		"role":      "assistant",
		"messageId": "M-assistant",
		"content":   []any{map[string]any{"type": "text", "text": "answer"}},
		"meta":      map[string]any{"sentAt": 1778170001000},
	}, 1)
	if len(assistant.Meta) != 0 {
		t.Fatalf("assistant meta = %#v, want omitted like binary import", assistant.Meta)
	}
}

func TestNeoMessageFromImportedThreadMatchesBinaryAssistantAndInfoImport(t *testing.T) {
	complete := neoMessageFromImportedThread("T-test", map[string]any{
		"role":      "assistant",
		"messageId": "M-complete",
		"content":   []any{map[string]any{"type": "text", "text": "done"}},
		"state":     map[string]any{"type": "complete", "stopReason": "end_turn"},
	}, 0)
	if len(complete.State) != 0 {
		t.Fatalf("complete assistant state = %#v, want omitted like binary import", complete.State)
	}

	cancelled := neoMessageFromImportedThread("T-test", map[string]any{
		"role":      "assistant",
		"messageId": "M-cancelled",
		"content":   []any{map[string]any{"type": "text", "text": "stopped"}},
		"state":     map[string]any{"type": "cancelled", "reason": "interrupt"},
	}, 1)
	if stringValue(cancelled.State["type"]) != "cancelled" || len(cancelled.State) != 1 {
		t.Fatalf("cancelled assistant state = %#v, want only type=cancelled", cancelled.State)
	}

	info := neoMessageFromImportedThread("T-test", map[string]any{
		"role":      "info",
		"messageId": "M-info",
		"content": []any{
			map[string]any{"type": "summary", "summary": map[string]any{"type": "message", "summary": "kept for local history"}},
			map[string]any{"type": "manual_bash_invocation", "args": map[string]any{"cmd": "git status"}, "toolRun": map[string]any{"status": "done"}},
		},
	}, 2)
	if len(info.Content) != 2 || stringValue(mapValue(info.Content[0])["type"]) != "summary" || stringValue(mapValue(info.Content[1])["type"]) != "manual_bash_invocation" {
		t.Fatalf("info content = %#v, want summary retained internally plus manual bash invocation", info.Content)
	}
	protocolContent := arrayValue(info.protocol()["content"])
	if len(protocolContent) != 1 || stringValue(mapValue(protocolContent[0])["type"]) != "manual_bash_invocation" {
		t.Fatalf("protocol info content = %#v, want only manual bash invocation", protocolContent)
	}
}

func TestNeoActorPropagatesParentToolCallToNestedToolResultAndProgress(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.pendingTools["TU-child"] = neoPendingTool{ID: "TU-child", Name: "Bash", ParentToolCallID: "TU-parent"}
	actor.pendingTools["TU-other"] = neoPendingTool{ID: "TU-other", Name: "Bash"}

	progress := actor.toolProgressPayload(map[string]any{"type": "tool_progress", "toolCallId": "TU-child", "progress": map[string]any{"status": "running"}})
	if got := stringValue(progress["parentToolCallId"]); got != "TU-parent" {
		t.Fatalf("progress parent = %q, want TU-parent", got)
	}
	explicit := actor.toolProgressPayload(map[string]any{"type": "tool_progress", "toolCallId": "TU-child", "parentToolCallId": "TU-explicit"})
	if got := stringValue(explicit["parentToolCallId"]); got != "TU-explicit" {
		t.Fatalf("explicit progress parent = %q, want TU-explicit", got)
	}

	actor.receiveToolResult(map[string]any{
		"type":       "executor_tool_result",
		"toolCallId": "TU-child",
		"run":        map[string]any{"status": "done", "result": "workspace"},
	})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.messages) != 1 {
		t.Fatalf("messages = %#v", actor.messages)
	}
	if got := actor.messages[0].ParentToolUseID; got != "TU-parent" {
		t.Fatalf("tool result parent = %q, want TU-parent", got)
	}
	if got := stringValue(actor.messages[0].protocol()["parentToolUseId"]); got != "TU-parent" {
		t.Fatalf("tool result protocol parent = %q, want TU-parent", got)
	}
}

func TestNeoActorFinalToolResultWaitsForExecutorReady(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.pendingTools["TU-child"] = neoPendingTool{ID: "TU-child", Name: "Bash", AgentMode: "deep", ReasoningEffort: "xhigh", ParentToolCallID: "TU-parent"}
	actor.agentState = "running_tools"
	actor.executorReady = false

	actor.receiveToolResult(map[string]any{"type": "executor_tool_result", "toolCallId": "TU-child", "run": map[string]any{"status": "done", "result": "workspace"}})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if actor.agentState != "idle" {
		t.Fatalf("agentState = %q, want idle while waiting for executor reconnect", actor.agentState)
	}
	if actor.pendingInference == nil || actor.pendingInference.agentMode != "deep" || actor.pendingInference.reasoningEffort != "xhigh" || actor.pendingInference.parentToolCallID != "TU-parent" {
		t.Fatalf("pending inference = %#v, want deep/xhigh with parent", actor.pendingInference)
	}
}

func TestNeoActorToolResultAcceptsToolRunAlias(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.pendingTools["TU-test"] = neoPendingTool{ID: "TU-test", Name: "Bash", AgentMode: "rush", MessageID: "M-assistant"}
	actor.agentState = "running_tools"
	actor.executorReady = false

	actor.receiveToolResult(map[string]any{"type": "executor_tool_result", "toolCallId": "TU-test", "toolRun": map[string]any{"status": "done", "result": "alias output"}})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.history) != 1 || actor.history[0].ToolCallID != "TU-test" || actor.history[0].Text != "alias output" {
		t.Fatalf("history = %#v", actor.history)
	}
	if len(actor.messages) != 1 || runToText(mapValue(actor.messages[0].Content[0])["run"]) != "alias output" {
		t.Fatalf("tool result message = %#v", actor.messages)
	}
}

func TestNeoToolResultPreservesDiscoveredGuidanceFiles(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.pendingTools["TU-guidance"] = neoPendingTool{ID: "TU-guidance", Name: "Read", AgentMode: "rush", MessageID: "M-assistant"}

	actor.receiveToolResult(map[string]any{
		"type":       "executor_tool_result",
		"toolCallId": "TU-guidance",
		"run": map[string]any{
			"status": "done",
			"result": map[string]any{
				"output": "read complete",
				"discoveredGuidanceFiles": []any{map[string]any{
					"uri":     "file:///tmp/project/AGENTS.md",
					"content": "guidance",
				}},
			},
		},
	})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.messages) != 1 {
		t.Fatalf("messages = %#v", actor.messages)
	}
	run := mapValue(mapValue(actor.messages[0].Content[0])["run"])
	result := mapValue(run["result"])
	if files := arrayValue(result["discoveredGuidanceFiles"]); len(files) != 1 {
		t.Fatalf("discoveredGuidanceFiles = %#v, want preserved", result["discoveredGuidanceFiles"])
	}
}

func TestNeoImageToolResultPreservesImagesAndCompactsHistoryText(t *testing.T) {
	run := normalizeNeoExecutorToolRun(context.Background(), nil, neoPendingTool{
		Name:  "render_agg_man",
		Input: map[string]any{"prompt": "draw the mascot"},
	}, map[string]any{
		"status": "done",
		"result": map[string]any{
			"images": []any{map[string]any{"b64_json": "abc123", "mime_type": "image/png", "filename": "agg.png"}},
		},
	}, "T-current")

	images := arrayValue(run["images"])
	if len(images) != 1 {
		t.Fatalf("images = %#v", run["images"])
	}
	image := mapValue(images[0])
	if got := stringValue(image["data"]); got != "abc123" {
		t.Fatalf("image data = %q, want abc123", got)
	}
	if got := stringValue(image["mediaType"]); got != "image/png" {
		t.Fatalf("image mediaType = %q, want image/png", got)
	}
	if got := stringValue(run["prompt"]); got != "draw the mascot" {
		t.Fatalf("prompt = %q", got)
	}
	binaryResults := arrayValue(run["result"])
	if len(binaryResults) != 1 {
		t.Fatalf("binary image result = %#v", run["result"])
	}
	binaryImage := mapValue(binaryResults[0])
	if stringValue(binaryImage["type"]) != "image" || stringValue(binaryImage["mimeType"]) != "image/png" || stringValue(binaryImage["data"]) != "abc123" {
		t.Fatalf("binary image = %#v", binaryImage)
	}
	if got := runToText(run); got != "rendered 1 image" {
		t.Fatalf("runToText = %q, want compact image text", got)
	}

	viewRun := normalizeNeoExecutorToolRun(context.Background(), nil, neoPendingTool{Name: "view_media"}, map[string]any{
		"status": "done",
		"image":  map[string]any{"url": "https://example.test/image.png"},
	}, "T-current")
	if got := runToText(viewRun); got != "viewed 1 image" {
		t.Fatalf("view runToText = %q, want viewed image text", got)
	}
}

func TestNeoImageToolResultAcceptsBinaryResultArray(t *testing.T) {
	run := normalizeNeoExecutorToolRun(context.Background(), nil, neoPendingTool{
		Name:  "painter",
		Input: map[string]any{"prompt": "draw a ship"},
	}, map[string]any{
		"status": "done",
		"result": []any{
			map[string]any{"type": "image", "mimeType": "image/png", "url": "https://example.test/image.png"},
		},
	}, "T-current")

	images := arrayValue(run["images"])
	if len(images) != 1 {
		t.Fatalf("images = %#v", run["images"])
	}
	image := mapValue(images[0])
	if stringValue(image["url"]) != "https://example.test/image.png" || stringValue(image["mimeType"]) != "image/png" {
		t.Fatalf("image = %#v", image)
	}
	result := arrayValue(run["result"])
	if len(result) != 1 || stringValue(mapValue(result[0])["url"]) != "https://example.test/image.png" {
		t.Fatalf("result = %#v", run["result"])
	}
	if got := runToText(run); got != "generated 1 image" {
		t.Fatalf("runToText = %q, want generated image text", got)
	}
}

func TestNeoImageToolResultReplaysImageContentToProviders(t *testing.T) {
	run := normalizeNeoExecutorToolRun(context.Background(), nil, neoPendingTool{Name: "view_media"}, map[string]any{
		"status": "done",
		"result": []any{
			map[string]any{"type": "text", "text": "Viewed image: /tmp/chart.png"},
			map[string]any{"type": "image", "mimeType": "image/png", "data": "abc123"},
		},
	}, "T-current")
	tool := neoHistoryMessage{Role: "tool", ToolCallID: "TU-view", ToolName: "view_media", Text: runToText(run), Content: neoToolRunHistoryContent(run)}
	history := []neoHistoryMessage{
		{Role: "assistant", ToolCalls: []neoToolCall{{ID: "TU-view", Name: "view_media"}}},
		tool,
	}

	anthropic := anthropicNeoMessages(history)
	anthropicTool := mapValue(arrayValue(mapValue(anthropic[1])["content"])[0])
	anthropicContent := arrayValue(anthropicTool["content"])
	if len(anthropicContent) != 2 || stringValue(mapValue(anthropicContent[0])["text"]) != "Viewed image: /tmp/chart.png" {
		t.Fatalf("anthropic tool content = %#v", anthropicContent)
	}
	anthropicImageSource := mapValue(mapValue(anthropicContent[1])["source"])
	if stringValue(anthropicImageSource["media_type"]) != "image/png" || stringValue(anthropicImageSource["data"]) != "abc123" {
		t.Fatalf("anthropic image source = %#v", anthropicImageSource)
	}

	responsesInput := openAIResponsesNeoInput(history, "")
	responsesOutput := arrayValue(mapValue(responsesInput[1])["output"])
	if len(responsesOutput) != 2 || stringValue(mapValue(responsesOutput[0])["text"]) != "Viewed image: /tmp/chart.png" {
		t.Fatalf("responses output = %#v", responsesOutput)
	}
	if imageURL := stringValue(mapValue(responsesOutput[1])["image_url"]); imageURL != "data:image/png;base64,abc123" {
		t.Fatalf("responses image_url = %q", imageURL)
	}

	openAIChat := openAINeoMessages(history, "")
	if len(openAIChat) != 3 {
		t.Fatalf("openAI chat messages = %#v", openAIChat)
	}
	openAITool := mapValue(openAIChat[1])
	if stringValue(openAITool["role"]) != "tool" || stringValue(openAITool["content"]) != "Viewed image: /tmp/chart.png\nImage:" {
		t.Fatalf("openAI tool message = %#v", openAITool)
	}
	openAIImageContent := arrayValue(mapValue(openAIChat[2])["content"])
	if len(openAIImageContent) != 3 || stringValue(mapValue(openAIImageContent[0])["text"]) != "Viewed image: /tmp/chart.png" || stringValue(mapValue(openAIImageContent[1])["text"]) != "Image:" {
		t.Fatalf("openAI image content = %#v", openAIImageContent)
	}
	if imageURL := stringValue(mapValue(mapValue(openAIImageContent[2])["image_url"])["url"]); imageURL != "data:image/png;base64,abc123" {
		t.Fatalf("openAI image_url = %q", imageURL)
	}

	google := googleNeoContents(history, "")
	googleParts := arrayValue(mapValue(google[1])["parts"])
	if len(googleParts) != 4 {
		t.Fatalf("google parts = %#v", googleParts)
	}
	response := mapValue(mapValue(googleParts[0])["functionResponse"])
	if stringValue(mapValue(response["response"])["content"]) != "Viewed image: /tmp/chart.png\nImage:" {
		t.Fatalf("google function response = %#v", response)
	}
	if stringValue(mapValue(googleParts[1])["text"]) != "Viewed image: /tmp/chart.png" || stringValue(mapValue(googleParts[2])["text"]) != "Image:" {
		t.Fatalf("google text parts = %#v", googleParts)
	}
	inlineData := mapValue(mapValue(googleParts[3])["inlineData"])
	if stringValue(inlineData["mimeType"]) != "image/png" || stringValue(inlineData["data"]) != "abc123" {
		t.Fatalf("google inline data = %#v", inlineData)
	}
}

func TestNeoReadImageToolResultReplaysImageContentToProviders(t *testing.T) {
	run := map[string]any{
		"status": "done",
		"result": map[string]any{
			"absolutePath": "/tmp/chart.png",
			"content":      "abc123",
			"isImage":      true,
			"imageInfo":    map[string]any{"mimeType": "image/png", "size": 1234},
		},
	}
	tool := neoHistoryMessage{Role: "tool", ToolCallID: "TU-read", ToolName: "Read", Text: runToText(run), Content: neoToolRunHistoryContent(run)}
	history := []neoHistoryMessage{
		{Role: "assistant", ToolCalls: []neoToolCall{{ID: "TU-read", Name: "Read"}}},
		tool,
	}

	if tool.Text != "Image: /tmp/chart.png" {
		t.Fatalf("tool text = %q", tool.Text)
	}
	if len(tool.Content) != 2 {
		t.Fatalf("tool content = %#v", tool.Content)
	}

	anthropic := anthropicNeoMessages(history)
	anthropicTool := mapValue(arrayValue(mapValue(anthropic[1])["content"])[0])
	anthropicContent := arrayValue(anthropicTool["content"])
	if len(anthropicContent) != 2 || stringValue(mapValue(anthropicContent[0])["text"]) != "Image: /tmp/chart.png" {
		t.Fatalf("anthropic tool content = %#v", anthropicContent)
	}
	anthropicImageSource := mapValue(mapValue(anthropicContent[1])["source"])
	if stringValue(anthropicImageSource["media_type"]) != "image/png" || stringValue(anthropicImageSource["data"]) != "abc123" {
		t.Fatalf("anthropic image source = %#v", anthropicImageSource)
	}

	responsesInput := openAIResponsesNeoInput(history, "")
	responsesOutput := arrayValue(mapValue(responsesInput[1])["output"])
	if len(responsesOutput) != 2 || stringValue(mapValue(responsesOutput[0])["text"]) != "Image: /tmp/chart.png" {
		t.Fatalf("responses output = %#v", responsesOutput)
	}
	if imageURL := stringValue(mapValue(responsesOutput[1])["image_url"]); imageURL != "data:image/png;base64,abc123" {
		t.Fatalf("responses image_url = %q", imageURL)
	}

	openAIChat := openAINeoMessages(history, "")
	if len(openAIChat) != 3 {
		t.Fatalf("openAI chat messages = %#v", openAIChat)
	}
	openAITool := mapValue(openAIChat[1])
	if stringValue(openAITool["content"]) != "Image: /tmp/chart.png" {
		t.Fatalf("openAI tool message = %#v", openAITool)
	}
	openAIImageContent := arrayValue(mapValue(openAIChat[2])["content"])
	if len(openAIImageContent) != 2 || stringValue(mapValue(openAIImageContent[0])["text"]) != "Image: /tmp/chart.png" {
		t.Fatalf("openAI image content = %#v", openAIImageContent)
	}
	if imageURL := stringValue(mapValue(mapValue(openAIImageContent[1])["image_url"])["url"]); imageURL != "data:image/png;base64,abc123" {
		t.Fatalf("openAI image_url = %q", imageURL)
	}

	google := googleNeoContents(history, "")
	googleParts := arrayValue(mapValue(google[1])["parts"])
	if len(googleParts) != 3 {
		t.Fatalf("google parts = %#v", googleParts)
	}
	response := mapValue(mapValue(googleParts[0])["functionResponse"])
	if stringValue(mapValue(response["response"])["content"]) != "Image: /tmp/chart.png" {
		t.Fatalf("google function response = %#v", response)
	}
	if stringValue(mapValue(googleParts[1])["text"]) != "Image: /tmp/chart.png" {
		t.Fatalf("google text part = %#v", googleParts[1])
	}
	inlineData := mapValue(mapValue(googleParts[2])["inlineData"])
	if stringValue(inlineData["mimeType"]) != "image/png" || stringValue(inlineData["data"]) != "abc123" {
		t.Fatalf("google inline data = %#v", inlineData)
	}
}

func TestNeoToolRunTextResultMatchesBinaryTypedTextBlocks(t *testing.T) {
	run := map[string]any{
		"status": "done",
		"result": []any{
			map[string]any{"type": "text", "text": "first excerpt"},
			map[string]any{"type": "text", "text": "second excerpt"},
		},
	}

	if got := runToText(run); got != "first excerpt\nsecond excerpt" {
		t.Fatalf("runToText = %q", got)
	}
}

func TestNeoActorPersistsToolApprovalQueueAndPreservesNestedMetadata(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.pendingTools["TU-child"] = neoPendingTool{ID: "TU-child", Name: "Bash", Input: map[string]any{"cmd": "pwd"}, ParentToolCallID: "TU-parent"}

	actor.handleToolApprovalRequest(map[string]any{
		"type": "executor_tool_approval_request",
		"approval": map[string]any{
			"toolCallId":       "TU-child",
			"toolName":         "Bash",
			"args":             map[string]any{"cmd": "pwd"},
			"reason":           "command requires approval",
			"toAllow":          []any{"pwd"},
			"subagentToolName": "researcher",
			"matchedRule":      map[string]any{"tool": "Bash", "action": "ask", "matches": map[string]any{"cmd": "pwd"}},
			"ruleSource":       "user",
		},
	})

	actor.mu.Lock()
	approvals := actor.approvalQueueListLocked()
	state := actor.agentState
	actor.mu.Unlock()
	if state != "awaiting_approval" {
		t.Fatalf("agentState = %q, want awaiting_approval", state)
	}
	if len(approvals) != 1 {
		t.Fatalf("approval queue = %#v", approvals)
	}
	approval := mapValue(approvals[0])
	if stringValue(approval["toolCallId"]) != "TU-child" {
		t.Fatalf("approval ids = %#v", approval)
	}
	if _, exists := approval["toolUseId"]; exists {
		t.Fatalf("approval leaked toolUseId alias: %#v", approval)
	}
	if stringValue(approval["parentToolCallId"]) != "TU-parent" || stringValue(approval["context"]) != "subagent" || stringValue(approval["subagentToolName"]) != "researcher" {
		t.Fatalf("approval nested metadata = %#v", approval)
	}
	if stringValue(mapValue(approval["args"])["cmd"]) != "pwd" || stringValue(approval["reason"]) != "command requires approval" {
		t.Fatalf("approval payload = %#v", approval)
	}
	toAllow := arrayValue(approval["toAllow"])
	if len(toAllow) != 1 || stringValue(toAllow[0]) != "pwd" {
		t.Fatalf("approval toAllow = %#v", approval)
	}
	matchedRule := mapValue(approval["matchedRule"])
	if stringValue(matchedRule["tool"]) != "Bash" || stringValue(approval["ruleSource"]) != "user" || numberFrom(approval["timestamp"]) == 0 {
		t.Fatalf("approval rule metadata = %#v", approval)
	}

	actor.handleToolApprovalRequest(map[string]any{
		"type":      "executor_tool_approval_request",
		"toolUseId": "TU-child",
		"toolName":  "Bash",
		"input":     map[string]any{"cmd": "echo updated"},
	})
	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.approvalQueue) != 1 || stringValue(mapValue(actor.approvalQueue[0]["args"])["cmd"]) != "echo updated" {
		t.Fatalf("approval queue was not upserted: %#v", actor.approvalQueue)
	}
}

func TestNeoActorToolApprovalResponseClearsQueueAndPreservesFeedback(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.pendingTools["TU-child"] = neoPendingTool{ID: "TU-child", Name: "Bash"}

	actor.handleToolApprovalRequest(map[string]any{
		"type": "executor_tool_approval_request",
		"approval": map[string]any{
			"toolCallId": "TU-child",
			"toolName":   "Bash",
			"args":       map[string]any{"cmd": "pwd"},
			"context":    "thread",
		},
	})
	actor.handleToolApprovalResponse(map[string]any{"type": "client_tool_approval_response", "toolCallId": "TU-child", "accepted": false, "input": "needs a safer command"})

	actor.mu.Lock()
	queueLen := len(actor.approvalQueue)
	state := actor.agentState
	actor.mu.Unlock()
	if queueLen != 0 {
		t.Fatalf("approval queue len = %d, want 0", queueLen)
	}
	if state != "running_tools" {
		t.Fatalf("agentState = %q, want running_tools", state)
	}

	payload := normalizeNeoToolApprovalResponse(map[string]any{"toolUseId": "TU-child", "accepted": false, "input": "needs a safer command"})
	if stringValue(payload["toolCallId"]) != "TU-child" || boolValue(payload["accepted"]) {
		t.Fatalf("approval response payload = %#v", payload)
	}
	if got := stringValue(mapValue(payload["input"])["denyFeedback"]); got != "needs a safer command" {
		t.Fatalf("denyFeedback = %q, want preserved: %#v", got, payload)
	}

	withAnswers := normalizeNeoToolApprovalResponse(map[string]any{"toolCallId": "TU-child", "accepted": true, "askAnswers": map[string]any{"choice": "yes"}})
	if got := stringValue(mapValue(mapValue(withAnswers["input"])["askAnswers"])["choice"]); got != "yes" {
		t.Fatalf("askAnswers = %q, want yes: %#v", got, withAnswers)
	}
}

func TestNeoActorRestoresApprovalQueueFromBlockedToolResultOnImport(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)

	if err := actor.importThreadLocalOnly(map[string]any{
		"id":        "T-test",
		"agentMode": "smart",
		"messages": []any{
			map[string]any{
				"role":            "assistant",
				"messageId":       "M-assistant",
				"parentToolUseId": "TU-parent",
				"content": []any{map[string]any{
					"type":  "tool_use",
					"id":    "TU-child",
					"name":  "Bash",
					"input": map[string]any{"cmd": "pwd"},
				}},
			},
			map[string]any{
				"role":            "user",
				"messageId":       "M-child",
				"parentToolUseId": "TU-parent",
				"content": []any{map[string]any{
					"type":      "tool_result",
					"toolUseID": "TU-child",
					"run": map[string]any{
						"status":           "blocked-on-user",
						"reason":           "approval needed",
						"toAllow":          []any{"pwd"},
						"subagentToolName": "researcher",
						"matchedRule":      map[string]any{"tool": "Bash", "action": "ask"},
						"ruleSource":       "user",
					},
				}},
			},
		},
	}); err != nil {
		t.Fatalf("importThreadLocalOnly error: %v", err)
	}

	actor.mu.Lock()
	approvals := actor.approvalQueueListLocked()
	state := actor.agentState
	actor.mu.Unlock()
	if state != "awaiting_approval" {
		t.Fatalf("agentState = %q, want awaiting_approval", state)
	}
	if len(approvals) != 1 {
		t.Fatalf("approval queue = %#v", approvals)
	}
	approval := mapValue(approvals[0])
	if stringValue(approval["toolCallId"]) != "TU-child" || stringValue(approval["toolName"]) != "Bash" || stringValue(mapValue(approval["args"])["cmd"]) != "pwd" {
		t.Fatalf("approval restored wrong tool payload: %#v", approval)
	}
	if _, exists := approval["toolUseId"]; exists {
		t.Fatalf("restored approval leaked toolUseId alias: %#v", approval)
	}
	if stringValue(approval["context"]) != "subagent" || stringValue(approval["parentToolCallId"]) != "TU-parent" || stringValue(approval["subagentToolName"]) != "researcher" {
		t.Fatalf("approval restored wrong nested metadata: %#v", approval)
	}
	if stringValue(approval["reason"]) != "approval needed" || stringValue(approval["ruleSource"]) != "user" || stringValue(mapValue(approval["matchedRule"])["tool"]) != "Bash" {
		t.Fatalf("approval restored wrong rule metadata: %#v", approval)
	}
}

func TestNeoActorCancelMarksPendingToolProgressMessagesCancelled(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.pendingTools["TU-child"] = neoPendingTool{ID: "TU-child", Name: "Bash", ParentToolCallID: "TU-parent"}
	actor.agentState = "running_tools"

	actor.handleToolProgress(map[string]any{"type": "tool_progress", "toolCallId": "TU-child", "progress": map[string]any{"phase": "start"}})
	actor.cancel()

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.pendingTools) != 0 {
		t.Fatalf("pending tools = %#v", actor.pendingTools)
	}
	if len(actor.messages) != 1 {
		t.Fatalf("messages = %#v", actor.messages)
	}
	run := mapValue(mapValue(actor.messages[0].Content[0])["run"])
	if stringValue(run["status"]) != "cancelled" || stringValue(run["reason"]) != "user:cancelled" {
		t.Fatalf("cancelled run = %#v", run)
	}
	if len(actor.history) != 1 || actor.history[0].ToolCallID != "TU-child" || actor.history[0].ParentToolUseID != "TU-parent" {
		t.Fatalf("history after cancellation = %#v", actor.history)
	}
	lastReplay := actor.replayEvents[len(actor.replayEvents)-1]
	if lastReplay.Payload["type"] != "cancelled" {
		t.Fatalf("last replay event = %#v, want cancelled", lastReplay)
	}
	foundUpdate := false
	for _, event := range actor.replayEvents {
		if event.Payload["type"] == "message_updated" {
			foundUpdate = true
			break
		}
	}
	if !foundUpdate {
		t.Fatalf("message_updated replay event missing: %#v", actor.replayEvents)
	}
}

func TestNeoActorCancelMarksRestoredApprovalToolResultCancelled(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.messages = []neoMessage{{
		ThreadID:        "T-test",
		MessageID:       "M-child",
		Role:            "user",
		ParentToolUseID: "TU-parent",
		Content: []any{map[string]any{
			"type":      "tool_result",
			"toolUseID": "TU-child",
			"run": map[string]any{
				"status": "blocked-on-user",
				"reason": "approval needed",
			},
		}},
		CompletionStatus: "tool_progress",
		Seq:              1,
	}}
	actor.approvalQueue = []map[string]any{{"toolCallId": "TU-child", "toolName": "Bash", "context": "subagent", "parentToolCallId": "TU-parent"}}
	actor.agentState = "awaiting_approval"

	actor.cancel()

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.approvalQueue) != 0 || actor.agentState != "idle" {
		t.Fatalf("approval state after cancel = queue:%#v agent:%q", actor.approvalQueue, actor.agentState)
	}
	run := mapValue(mapValue(actor.messages[0].Content[0])["run"])
	if stringValue(run["status"]) != "cancelled" || stringValue(run["reason"]) != "user:cancelled" {
		t.Fatalf("restored approval run after cancel = %#v", run)
	}
	if actor.messages[0].CompletionStatus != "" {
		t.Fatalf("completion status after cancel = %q", actor.messages[0].CompletionStatus)
	}
}

func TestNeoActorPersistsToolProgressAndFinalResultUpdatesSameMessage(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.pendingTools["TU-child"] = neoPendingTool{ID: "TU-child", Name: "Bash", ParentToolCallID: "TU-parent"}
	actor.pendingTools["TU-other"] = neoPendingTool{ID: "TU-other", Name: "Bash"}

	actor.handleToolProgress(map[string]any{"type": "tool_progress", "toolCallId": "TU-child", "progress": map[string]any{"phase": "start"}})
	actor.handleToolProgress(map[string]any{"type": "tool_progress", "toolCallId": "TU-child", "progress": map[string]any{"detail": "half"}})

	actor.mu.Lock()
	if len(actor.messages) != 1 {
		actor.mu.Unlock()
		t.Fatalf("messages after progress = %#v", actor.messages)
	}
	progressMessage := actor.messages[0]
	progressSeq := progressMessage.Seq
	progressBlock := mapValue(progressMessage.Content[0])
	progressRun := mapValue(progressBlock["run"])
	progress := mapValue(progressRun["progress"])
	actor.rebuildHistoryLocked()
	historyLen := len(actor.history)
	actor.mu.Unlock()

	if progressMessage.MessageID != "M-child" || progressMessage.ParentToolUseID != "TU-parent" || progressMessage.CompletionStatus != "tool_progress" {
		t.Fatalf("progress message = %#v", progressMessage)
	}
	if stringValue(progressRun["status"]) != "in-progress" || progress["phase"] != "start" || progress["detail"] != "half" {
		t.Fatalf("progress run = %#v", progressRun)
	}
	if historyLen != 0 {
		t.Fatalf("progress-only tool result leaked into history, len=%d", historyLen)
	}

	actor.receiveToolResult(map[string]any{"type": "executor_tool_result", "toolCallId": "TU-child", "run": map[string]any{"status": "done", "result": "workspace"}})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.messages) != 1 {
		t.Fatalf("messages after final result = %#v", actor.messages)
	}
	finalMessage := actor.messages[0]
	if finalMessage.MessageID != "M-child" || finalMessage.Seq != progressSeq || finalMessage.CompletionStatus != "" {
		t.Fatalf("final message did not update progress message: %#v", finalMessage)
	}
	finalRun := mapValue(mapValue(finalMessage.Content[0])["run"])
	if stringValue(finalRun["status"]) != "done" || stringValue(finalRun["result"]) != "workspace" {
		t.Fatalf("final run = %#v", finalRun)
	}
	if len(actor.history) != 1 || actor.history[0].ToolCallID != "TU-child" || actor.history[0].Text != "workspace" {
		t.Fatalf("history after final result = %#v", actor.history)
	}
	if len(actor.replayEvents) == 0 || actor.replayEvents[len(actor.replayEvents)-1].Seq <= progressSeq || actor.replayEvents[len(actor.replayEvents)-1].Payload["type"] != "message_updated" {
		t.Fatalf("final update was not logged for replay: seq=%d replay=%#v", progressSeq, actor.replayEvents)
	}
}

func TestNeoRuntimeReplaysProgressMessageAndFinalProgressUpdate(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	threadID := "T-019e0e6e-f3f1-7087-b5dd-748f66f8c25d"
	actor, _ := rt.store.upsert(map[string]any{"name": "threadActor", "key": threadID, "input": map[string]any{"threadId": threadID}}, true)
	actor.pendingTools["TU-child"] = neoPendingTool{ID: "TU-child", Name: "Bash", ParentToolCallID: "TU-parent"}
	actor.pendingTools["TU-other"] = neoPendingTool{ID: "TU-other", Name: "Bash"}
	actor.handleToolProgress(map[string]any{"type": "tool_progress", "toolCallId": "TU-child", "progress": map[string]any{"phase": "start"}})
	actor.mu.Lock()
	progressSeq := actor.messages[0].Seq
	actor.mu.Unlock()

	server := httptest.NewServer(http.HandlerFunc(rt.handleHTTP))
	t.Cleanup(server.Close)

	conn := dialNeoActorWebSocket(t, server.URL, threadID)
	defer conn.Close()
	added := waitForNeoMessageType(t, conn, "message_added", 2*time.Second)
	addedMessage := mapValue(added["message"])
	if stringValue(addedMessage["messageId"]) != "M-child" || stringValue(added["parentToolUseId"]) != "TU-parent" {
		t.Fatalf("progress snapshot message = %#v", added)
	}
	addedRun := mapValue(mapValue(arrayValue(addedMessage["content"])[0])["run"])
	if stringValue(addedRun["status"]) != "in-progress" {
		t.Fatalf("progress snapshot run = %#v", addedRun)
	}

	actor.receiveToolResult(map[string]any{"type": "executor_tool_result", "toolCallId": "TU-child", "run": map[string]any{"status": "done", "result": "workspace"}})
	if err := conn.WriteJSON(map[string]any{"type": "client_resume", "version": progressSeq}); err != nil {
		t.Fatalf("write client_resume: %v", err)
	}
	updated := waitForNeoMessageType(t, conn, "message_updated", 2*time.Second)
	if numberFrom(updated["seq"]) <= progressSeq {
		t.Fatalf("message_updated seq = %d, want > %d: %#v", numberFrom(updated["seq"]), progressSeq, updated)
	}
	updatedRun := mapValue(mapValue(arrayValue(mapValue(updated["message"])["content"])[0])["run"])
	if stringValue(updatedRun["status"]) != "done" || stringValue(updatedRun["result"]) != "workspace" {
		t.Fatalf("updated run = %#v", updatedRun)
	}
}

func TestNeoActorScopesInferenceHistoryByParentToolUse(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.messages = []neoMessage{
		{ThreadID: "T-test", MessageID: "M-top-user", Role: "user", Content: []any{map[string]any{"type": "text", "text": "top user"}}, Seq: 1},
		{ThreadID: "T-test", MessageID: "M-top-assistant", Role: "assistant", Content: []any{
			map[string]any{"type": "text", "text": "top answer"},
			map[string]any{"type": "tool_use", "id": "TU-parent", "name": "Task", "input": map[string]any{"prompt": "delegate"}},
		}, Seq: 2},
		{ThreadID: "T-test", MessageID: "M-nested-assistant", Role: "assistant", ParentToolUseID: "TU-parent", Content: []any{
			map[string]any{"type": "text", "text": "nested parent answer"},
			map[string]any{"type": "tool_use", "id": "TU-child", "name": "Bash", "input": map[string]any{"cmd": "pwd"}},
		}, Seq: 3},
		{ThreadID: "T-test", MessageID: "M-nested-tool", Role: "user", ParentToolUseID: "TU-parent", Content: []any{map[string]any{"type": "tool_result", "toolUseID": "TU-child", "run": map[string]any{"status": "done", "result": "nested tool result"}}}, Seq: 4},
		{ThreadID: "T-test", MessageID: "M-sibling", Role: "assistant", ParentToolUseID: "TU-other", Content: []any{map[string]any{"type": "text", "text": "sibling nested answer"}}, Seq: 5},
	}

	actor.mu.Lock()
	actor.rebuildHistoryLocked()
	top := actor.inferenceRequestLocked("smart", "high", "")
	nested := actor.inferenceRequestLocked("smart", "high", "TU-parent")
	actor.mu.Unlock()

	topText := neoHistoryTestText(top.History)
	if !strings.Contains(topText, "top user") || !strings.Contains(topText, "top answer") {
		t.Fatalf("top history missing top-level context: %s", topText)
	}
	if strings.Contains(topText, "nested parent answer") || strings.Contains(topText, "nested tool result") || strings.Contains(topText, "sibling nested answer") {
		t.Fatalf("top history leaked nested messages: %s", topText)
	}

	nestedText := neoHistoryTestText(nested.History)
	if !strings.Contains(nestedText, "top user") || !strings.Contains(nestedText, "top answer") || !strings.Contains(nestedText, "nested parent answer") || !strings.Contains(nestedText, "nested tool result") {
		t.Fatalf("nested history missing expected scoped context: %s", nestedText)
	}
	if strings.Contains(nestedText, "sibling nested answer") {
		t.Fatalf("nested history leaked sibling parent messages: %s", nestedText)
	}
	if nested.ParentToolCallID != "TU-parent" {
		t.Fatalf("nested request parent = %q, want TU-parent", nested.ParentToolCallID)
	}
}

func neoHistoryTestText(history []neoHistoryMessage) string {
	parts := make([]string, 0, len(history))
	for _, message := range history {
		parts = append(parts, message.Text)
		for _, call := range message.ToolCalls {
			parts = append(parts, call.Name)
		}
	}
	return strings.Join(parts, "\n")
}

func TestNeoActorHandlesExecutorDisconnected(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.executorID = "executor-test"
	actor.executorReady = true
	actor.pendingTools["TU-test"] = neoPendingTool{ID: "TU-test", Name: "Bash"}

	actor.handle(map[string]any{"type": "executor_disconnected", "message": "lost"})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if actor.executorReady || actor.executorID != "" {
		t.Fatalf("executor state = ready:%v id:%q", actor.executorReady, actor.executorID)
	}
	if len(actor.pendingTools) != 0 {
		t.Fatalf("pending tools = %#v", actor.pendingTools)
	}
}

func TestNeoRuntimeDoesNotServeThreadReadSearchHTTP(t *testing.T) {
	dir := t.TempDir()
	oldStoreDir := neoAmpThreadStoreDir
	neoAmpThreadStoreDir = func() string { return dir }
	t.Cleanup(func() { neoAmpThreadStoreDir = oldStoreDir })

	threadID := "T-019e0379-5ef4-72e9-9ce6-dc9408a838f5"
	upstreamRequests := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests++
		writeNeoJSON(w, http.StatusOK, map[string]any{"unexpected": true})
	}))
	defer upstream.Close()

	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{UpstreamURL: upstream.URL, UpstreamAPIKey: "secret"}})
	for _, path := range []string{
		"/threads/" + threadID,
		"/threads/" + threadID + ".md",
		"/api/threads/find?q=remote+needle&limit=5",
	} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		rt.handleHTTP(rec, req)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("GET %s status = %d body=%s", path, rec.Code, rec.Body.String())
		}
	}
	if upstreamRequests != 0 {
		t.Fatalf("runtime thread read/search HTTP should not call upstream; requests=%d", upstreamRequests)
	}
}

func TestNeoRuntimeThreadActorBootstrapDoesNotFetchCloudThread(t *testing.T) {
	dir := t.TempDir()
	oldStoreDir := neoAmpThreadStoreDir
	neoAmpThreadStoreDir = func() string { return dir }
	t.Cleanup(func() { neoAmpThreadStoreDir = oldStoreDir })

	threadID := "T-019e65c0-0310-77a8-b233-4b84d9c0612b"
	upstreamRequests := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests++
		writeNeoJSON(w, http.StatusOK, map[string]any{"unexpected": true})
	}))
	defer upstream.Close()

	rt := newNeoRuntime(&config.Config{
		AmpCode: config.AmpCode{UpstreamURL: upstream.URL, UpstreamAPIKey: "secret"},
	})
	response, status := rt.localThreadActorManagementResponse(context.Background(), map[string]any{"threadId": threadID}, "")
	if status != http.StatusOK {
		t.Fatalf("status = %d response=%#v", status, response)
	}

	actor := rt.store.ensureThreadActor(threadID)
	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.messages) != 0 {
		t.Fatalf("actor imported cloud messages locally: %#v", actor.messages)
	}
	if upstreamRequests != 0 {
		t.Fatalf("runtime fetched upstream thread %d time(s)", upstreamRequests)
	}
}

func TestUploadNeoCloudThreadUsesAmpInternalClientHeaders(t *testing.T) {
	threadID := "T-019e65c0-0310-77a8-b233-4b84d9c0612b"
	var sawRequest bool
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawRequest = true
		if r.URL.Path != "/api/internal" || r.URL.RawQuery != "uploadThread" {
			t.Fatalf("request target = %s?%s", r.URL.Path, r.URL.RawQuery)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer secret" {
			t.Fatalf("Authorization = %q", got)
		}
		if got := r.Header.Get("X-Amp-Client-Application"); got != "CLI" {
			t.Fatalf("X-Amp-Client-Application = %q", got)
		}
		if got := r.Header.Get("X-Amp-Client-Type"); got != "cli" {
			t.Fatalf("X-Amp-Client-Type = %q", got)
		}
		if got := r.Header.Get("X-Amp-Client-Version"); strings.TrimSpace(got) == "" {
			t.Fatalf("X-Amp-Client-Version missing")
		}
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode payload: %v", err)
		}
		if payload["method"] != "uploadThread" {
			t.Fatalf("method = %#v", payload["method"])
		}
		params := mapValue(payload["params"])
		if params["createdOnServer"] != false {
			t.Fatalf("createdOnServer = %#v", params["createdOnServer"])
		}
		thread := mapValue(params["thread"])
		if thread["id"] != threadID {
			t.Fatalf("thread id = %#v", thread["id"])
		}
		writeNeoJSON(w, http.StatusOK, map[string]any{"ok": true})
	}))
	defer upstream.Close()

	err := uploadNeoCloudThread(neoCloudThreadSnapshot{
		upstreamURL: upstream.URL,
		apiKey:      "secret",
		threadID:    threadID,
		seq:         1,
		createdMs:   1778170000000,
		title:       "Header parity",
		messages: []neoMessage{
			{ThreadID: threadID, MessageID: "M-user", Role: "user", Content: []any{map[string]any{"type": "text", "text": "hello"}}, Seq: 1},
		},
	})
	if err != nil {
		t.Fatalf("uploadNeoCloudThread error: %v", err)
	}
	if !sawRequest {
		t.Fatal("upstream did not receive uploadThread request")
	}
}

func TestNeoRuntimeIgnoresCloudCachedLocalThreadOnBootstrap(t *testing.T) {
	dir := t.TempDir()
	oldStoreDir := neoAmpThreadStoreDir
	neoAmpThreadStoreDir = func() string { return dir }
	t.Cleanup(func() { neoAmpThreadStoreDir = oldStoreDir })

	threadID := "T-019e65c0-0310-77a8-b233-4b84d9c0612b"
	localThread := map[string]any{
		"id":        threadID,
		"title":     "stale cloud cache",
		"agentMode": "deep",
		"meta":      map[string]any{"cliProxyAPICloudCache": true},
		"messages": []any{
			map[string]any{"role": "user", "messageId": "M-local", "content": []any{map[string]any{"type": "text", "text": "stale local message"}}},
		},
	}
	rawLocal, err := json.Marshal(localThread)
	if err != nil {
		t.Fatalf("marshal local thread: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, threadID+".json"), rawLocal, 0o600); err != nil {
		t.Fatalf("write local thread: %v", err)
	}

	if thread, ok := loadNeoThread(threadID); ok {
		t.Fatalf("loadNeoThread returned cloud cache: %#v", thread)
	}
}

func TestNeoSystemPromptFiltersDisabledSkills(t *testing.T) {
	prompt := neoSystemPrompt(neoInferenceRequest{
		Capabilities: map[string]any{
			"skills": []any{
				map[string]any{"name": "code-review", "description": "Review code"},
				map[string]any{"name": "hidden-skill", "description": "Hidden", "frontmatter": map[string]any{"disable-model-invocation": true}},
			},
		},
	}, neoModelRoute{Provider: "anthropic", Model: "claude-opus-4-7"})

	if !strings.Contains(prompt, "<name>code-review</name>") || strings.Contains(prompt, "hidden-skill") {
		t.Fatalf("prompt did not filter skills correctly:\n%s", prompt)
	}
}

func TestNeoRuntimeActorSkillsHTTP(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor, _ := rt.store.upsert(map[string]any{"name": "thread-actor", "key": "T-test"}, true)
	actor.updateSkillSnapshot(map[string]any{
		"type": "executor_skill_snapshot",
		"skills": []any{
			map[string]any{"name": "code-review"},
		},
		"errors": []any{},
	})
	decoy, _ := rt.store.upsert(map[string]any{"name": "threadActor", "key": "T-decoy"}, true)
	decoy.updateSkillSnapshot(map[string]any{
		"type":   "executor_skill_snapshot",
		"skills": []any{map[string]any{"name": "wrong-skill"}},
	})

	for _, tc := range []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/actors/" + actor.id + "/skills"},
		{http.MethodGet, "/gateway/" + actor.id + "/request/skills"},
		{http.MethodGet, "/gateway/" + actor.id + "@default/request/skills"},
		{http.MethodGet, "/gateway/threadActor/request/skills?rvt-key=T-test"},
		{http.MethodPost, "/gateway/threadActor/request/list-skills?rvt-key=T-test"},
	} {
		req := httptest.NewRequest(tc.method, tc.path, nil)
		rec := httptest.NewRecorder()
		rt.handleHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s %s status = %d, body=%s", tc.method, tc.path, rec.Code, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), `"ok":true`) || !strings.Contains(rec.Body.String(), "code-review") {
			t.Fatalf("unexpected skills response for %s: %s", tc.path, rec.Body.String())
		}
	}
}

func TestNeoSkillsPathMatchesOnlyKnownEndpoints(t *testing.T) {
	for _, path := range []string{
		"/skills",
		"/request/skills",
		"/request/list-skills",
		"/actors/A-123/skills",
		"/gateway/A-123/request/skills",
		"/gateway/threadActor/request/list-skills",
	} {
		if !isNeoSkillsPath(path) {
			t.Fatalf("isNeoSkillsPath(%q) = false, want true", path)
		}
	}

	for _, path := range []string{
		"/gateway/threadActor/request/skillsets",
		"/gateway/threadActor/request/user-skills-summary",
		"/actors/A-123/kv/keys/skills%2Fstate",
		"/api/threads/find?q=skills",
	} {
		if isNeoSkillsPath(path) {
			t.Fatalf("isNeoSkillsPath(%q) = true, want false", path)
		}
	}
}

func TestNeoCloudThreadIncludesProtocolMessageIDAndCompleteState(t *testing.T) {
	thread := neoCloudThread(neoCloudThreadSnapshot{
		threadID:  "T-test",
		seq:       3,
		createdMs: 1778170000000,
		settings:  map[string]any{"agentMode": "deep"},
		messages: []neoMessage{
			{MessageID: "M-user", Role: "user", Content: []any{map[string]any{"type": "text", "text": "hi"}}, AgentMode: "deep", ReadAt: "2026-05-07T21:00:00Z", Seq: 1},
			{MessageID: "M-assistant", Role: "assistant", Content: []any{map[string]any{"type": "text", "text": "ok"}}, State: map[string]any{"type": "complete", "stopReason": "end_turn"}, Seq: 2},
		},
	})

	messages, ok := thread["messages"].([]any)
	if !ok || len(messages) != 2 {
		t.Fatalf("messages = %#v", thread["messages"])
	}
	assistant := mapValue(messages[1])
	if assistant["protocolMessageID"] != "M-assistant" {
		t.Fatalf("protocolMessageID = %#v", assistant["protocolMessageID"])
	}
	user := mapValue(messages[0])
	if user["readAt"] != "2026-05-07T21:00:00Z" {
		t.Fatalf("readAt = %#v", user["readAt"])
	}
	if got := stringValue(mapValue(assistant["state"])["type"]); got != "complete" {
		t.Fatalf("assistant state type = %q", got)
	}
	if thread["agentMode"] != "deep" {
		t.Fatalf("agentMode = %#v", thread["agentMode"])
	}
	meta := mapValue(thread["meta"])
	if meta["ampcodeConnectorLocalNeo"] != true {
		t.Fatalf("missing connector local Neo marker in meta: %#v", meta)
	}
	if thread["title"] != "hi" {
		t.Fatalf("title = %#v", thread["title"])
	}
	if meta["usesThreadActors"] != true {
		t.Fatalf("missing thread actor marker in meta: %#v", meta)
	}
}

func TestNeoCloudThreadDefaultsAgentModeForBinarySwitch(t *testing.T) {
	thread := neoCloudThread(neoCloudThreadSnapshot{
		threadID:  "T-test",
		seq:       1,
		createdMs: 1778170000000,
		messages: []neoMessage{
			{MessageID: "M-user", Role: "user", Content: []any{map[string]any{"type": "text", "text": "hi"}}, Seq: 1},
		},
	})

	if thread["agentMode"] != "smart" {
		t.Fatalf("agentMode = %#v, want smart fallback for Amp binary thread switch", thread["agentMode"])
	}
}

func TestNeoLocalThreadStoreWritesAmpThreadFile(t *testing.T) {
	dir := t.TempDir()
	oldStoreDir := neoAmpThreadStoreDir
	neoAmpThreadStoreDir = func() string { return dir }
	t.Cleanup(func() { neoAmpThreadStoreDir = oldStoreDir })

	snapshot := neoCloudThreadSnapshot{
		threadID:  "T-local-store",
		seq:       2,
		createdMs: 1778170000000,
		messages: []neoMessage{
			{ThreadID: "T-local-store", MessageID: "M-user", Role: "user", Content: []any{map[string]any{"type": "text", "text": "hello"}}, AgentMode: "smart", Seq: 1},
		},
	}
	if err := writeNeoLocalThreadSnapshot(snapshot); err != nil {
		t.Fatalf("writeNeoLocalThreadSnapshot error: %v", err)
	}

	raw, err := os.ReadFile(filepath.Join(dir, "T-local-store.json"))
	if err != nil {
		t.Fatalf("read stored thread: %v", err)
	}
	var thread map[string]any
	if err := json.Unmarshal(raw, &thread); err != nil {
		t.Fatalf("stored thread JSON: %v", err)
	}
	if thread["id"] != "T-local-store" {
		t.Fatalf("thread id = %#v", thread["id"])
	}
	meta := mapValue(thread["meta"])
	if meta["usesThreadActors"] != true || meta["cliProxyAPILocalNeo"] != true {
		t.Fatalf("stored meta = %#v", meta)
	}
	if markdown := neoThreadMarkdown(thread); !strings.Contains(markdown, "threadId: T-local-store") || !strings.Contains(markdown, "hello") {
		t.Fatalf("markdown missing thread content:\n%s", markdown)
	}
}

func TestNeoRuntimeThreadImportHTTP(t *testing.T) {
	dir := t.TempDir()
	oldStoreDir := neoAmpThreadStoreDir
	neoAmpThreadStoreDir = func() string { return dir }
	t.Cleanup(func() { neoAmpThreadStoreDir = oldStoreDir })

	rt := newNeoRuntime(&config.Config{})
	actor := rt.store.ensureThreadActor("T-import")

	req := httptest.NewRequest(http.MethodPost, "/request/import", strings.NewReader(`{"thread":{"id":"T-import","v":7,"title":"Imported","agentMode":"deep","artifacts":[{"key":"artifact-1","type":"markdown","content":"notes"}],"relationships":[{"threadID":"T-019e1046-656d-7132-879f-390ded941c16","type":"mention","role":"parent","createdAt":1,"messageIndex":0}],"messages":[{"role":"user","messageId":0,"content":[{"type":"text","text":"carry this"}]},{"role":"assistant","messageId":1,"content":[{"type":"text","text":"done"}],"state":{"type":"complete","stopReason":"end_turn"}}]}}`))
	rec := httptest.NewRecorder()
	rt.handleHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /request/import status = %d body=%s", rec.Code, rec.Body.String())
	}

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if actor.title != "Imported" || actor.currentAgentMode != "deep" {
		t.Fatalf("imported title/mode = %q/%q", actor.title, actor.currentAgentMode)
	}
	if len(actor.messages) != 2 || !neoMessageIDPattern.MatchString(actor.messages[0].MessageID) || !neoMessageIDPattern.MatchString(actor.messages[1].MessageID) || actor.messages[0].MessageID == "0" || actor.messages[1].MessageID == "1" || textFromBlocks(actor.messages[0].Content) != "carry this" {
		t.Fatalf("imported messages = %#v", actor.messages)
	}
	if len(actor.history) != 2 || actor.history[0].Text != "carry this" || actor.history[1].Text != "done" {
		t.Fatalf("history = %#v", actor.history)
	}
	if len(actor.relationships) != 1 || stringValue(actor.relationships[0]["threadID"]) != "T-019e1046-656d-7132-879f-390ded941c16" {
		t.Fatalf("relationships = %#v", actor.relationships)
	}
	if actor.meta["usesThreadActors"] != true || actor.meta["usesDtw"] != true {
		t.Fatalf("imported thread actor meta = %#v", actor.meta)
	}
	if got := stringValue(mapValue(actor.artifacts["artifact-1"])["content"]); got != "notes" {
		t.Fatalf("imported artifact content = %q", got)
	}
}

func TestNeoRuntimeThreadImportGatewayCreatesActor(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	threadID := "T-import-gateway"

	req := httptest.NewRequest(http.MethodPost, "/gateway/threadActor/request/import?rvt-method=getOrCreate&rvt-key="+threadID+"&rvt-skip-ready-wait=true", strings.NewReader(`{"thread":{"id":"`+threadID+`","v":3,"title":"Gateway import","agentMode":"deep","messages":[{"role":"user","messageId":"M-user","agentMode":"deep","content":[{"type":"text","text":"from upstream"}]}]}}`))
	rec := httptest.NewRecorder()
	rt.handleHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST gateway import status = %d body=%s", rec.Code, rec.Body.String())
	}

	actor := rt.store.ensureThreadActor(threadID)
	actor.mu.Lock()
	defer actor.mu.Unlock()
	if actor.title != "Gateway import" || actor.currentAgentMode != "deep" {
		t.Fatalf("imported actor title/mode = %q/%q", actor.title, actor.currentAgentMode)
	}
	if len(actor.messages) != 1 || textFromBlocks(actor.messages[0].Content) != "from upstream" {
		t.Fatalf("imported messages = %#v", actor.messages)
	}
}

func TestNeoRuntimeThreadActorMarkPersistsImportedMeta(t *testing.T) {
	dir := t.TempDir()
	oldStoreDir := neoAmpThreadStoreDir
	neoAmpThreadStoreDir = func() string { return dir }
	t.Cleanup(func() { neoAmpThreadStoreDir = oldStoreDir })

	threadID := "T-import-mark"
	rawThread := []byte(`{"id":"` + threadID + `","v":4,"title":"Legacy","agentMode":"deep","messages":[{"role":"user","messageId":"M-user","agentMode":"deep","content":[{"type":"text","text":"legacy thread"}]}]}`)
	if err := os.WriteFile(filepath.Join(dir, threadID+".json"), rawThread, 0o600); err != nil {
		t.Fatalf("write local thread: %v", err)
	}

	rt := newNeoRuntime(&config.Config{})
	response, status := rt.localThreadActorManagementResponse(context.Background(), map[string]any{"executorType": "local-client"}, threadID)
	if status != http.StatusOK {
		t.Fatalf("status = %d response=%#v", status, response)
	}
	if response["usesThreadActors"] != true || response["executorType"] != "local-client" {
		t.Fatalf("thread actor mark response = %#v", response)
	}

	actor := rt.store.ensureThreadActor(threadID)
	actor.mu.Lock()
	actorMeta := cloneMap(actor.meta)
	actor.mu.Unlock()
	if actorMeta["usesThreadActors"] != true || actorMeta["usesDtw"] != true {
		t.Fatalf("actor meta = %#v", actorMeta)
	}
	reloaded, ok := loadNeoLocalThread(threadID)
	if !ok {
		t.Fatal("local thread missing after mark")
	}
	meta := mapValue(reloaded["meta"])
	if meta["usesThreadActors"] != true || meta["usesDtw"] != true {
		t.Fatalf("persisted meta = %#v", meta)
	}
}

func TestNeoRuntimeThreadImportVersionAllocatesFutureSeq(t *testing.T) {
	dir := t.TempDir()
	oldStoreDir := neoAmpThreadStoreDir
	neoAmpThreadStoreDir = func() string { return dir }
	t.Cleanup(func() { neoAmpThreadStoreDir = oldStoreDir })

	rt := newNeoRuntime(&config.Config{})
	actor := rt.store.ensureThreadActor("T-import-version")
	err := actor.importThreadLocalOnly(map[string]any{
		"id":        "T-import-version",
		"v":         7,
		"agentMode": "deep",
		"messages": []any{
			map[string]any{"role": "user", "messageId": 0, "content": []any{map[string]any{"type": "text", "text": "carry this"}}},
			map[string]any{"role": "assistant", "messageId": 1, "content": []any{map[string]any{"type": "text", "text": "done"}}, "state": map[string]any{"type": "complete", "stopReason": "end_turn"}},
		},
	})
	if err != nil {
		t.Fatalf("importThreadLocalOnly error: %v", err)
	}

	actor.mu.Lock()
	if actor.seq != 8 {
		t.Fatalf("next seq after import = %d, want 8", actor.seq)
	}
	actor.mu.Unlock()

	actor.handle(map[string]any{"type": "message_added", "message": map[string]any{
		"role":      "assistant",
		"messageId": "M-next",
		"content":   []any{map[string]any{"type": "text", "text": "new"}},
		"state":     map[string]any{"type": "complete", "stopReason": "end_turn"},
	}})

	actor.mu.Lock()
	nextSeq := actor.seq
	seq := 0
	for _, message := range actor.messages {
		if message.MessageID == "M-next" {
			seq = message.Seq
			break
		}
	}
	actor.mu.Unlock()
	if seq != 8 || nextSeq != 9 {
		t.Fatalf("new message seq/next = %d/%d, want 8/9", seq, nextSeq)
	}
	snapshot, ok := actor.threadSnapshot()
	if !ok || snapshot.seq != 8 {
		t.Fatalf("snapshot seq = %#v/%v, want 8", snapshot, ok)
	}
}

func TestNeoRuntimeProtocolDeltaNormalizesLikeBinary(t *testing.T) {
	dir := t.TempDir()
	oldStoreDir := neoAmpThreadStoreDir
	neoAmpThreadStoreDir = func() string { return dir }
	t.Cleanup(func() { neoAmpThreadStoreDir = oldStoreDir })

	rt := newNeoRuntime(&config.Config{})
	actor := rt.store.ensureThreadActor("T-protocol-delta-normalize")

	actor.handle(map[string]any{
		"type":      "delta",
		"messageId": "M-ignored",
		"role":      "system",
		"blocks":    []any{map[string]any{"type": "text", "text": "ignored"}},
	})
	actor.mu.Lock()
	if len(actor.messages) != 0 || len(actor.replayEvents) != 0 {
		actor.mu.Unlock()
		t.Fatalf("invalid role was not dropped: messages=%#v replay=%#v", actor.messages, actor.replayEvents)
	}
	actor.mu.Unlock()

	actor.handle(map[string]any{
		"type":       "delta",
		"messageId":  "M-assistant",
		"role":       "assistant",
		"state":      "not-a-state",
		"blockIndex": "bad",
		"blocks": []any{
			map[string]any{"type": "unknown"},
			map[string]any{"type": "text", "text": "hello", "blockState": "bad"},
		},
	})
	actor.mu.Lock()
	if len(actor.messages) != 1 {
		actor.mu.Unlock()
		t.Fatalf("messages = %#v, want assistant message", actor.messages)
	}
	assistant := actor.messages[0]
	replay := actor.replayEvents[len(actor.replayEvents)-1].Payload
	actor.mu.Unlock()
	if got := stringValue(mapValue(assistant.State)["type"]); got != "streaming" {
		t.Fatalf("assistant state = %q, want streaming", got)
	}
	if len(assistant.Content) != 2 {
		t.Fatalf("assistant content = %#v, want 2 blocks", assistant.Content)
	}
	if hidden := mapValue(assistant.Content[0]); stringValue(hidden["type"]) != "text" || stringValue(hidden["text"]) != "" || !boolValue(hidden["hidden"]) {
		t.Fatalf("hidden replacement block = %#v", hidden)
	}
	if text := mapValue(assistant.Content[1]); stringValue(text["text"]) != "hello" {
		t.Fatalf("text block = %#v", text)
	} else if _, exists := text["blockState"]; exists {
		t.Fatalf("invalid blockState was preserved: %#v", text)
	}
	if got := stringValue(replay["state"]); got != "generating" {
		t.Fatalf("replay state = %q, want generating", got)
	}
	if got := numberFrom(replay["blockIndex"]); got != 0 {
		t.Fatalf("replay blockIndex = %d, want 0", got)
	}

	actor.handle(map[string]any{
		"type":       "delta",
		"messageId":  "M-user",
		"role":       "user",
		"state":      "generating",
		"blockIndex": 3,
		"blocks": []any{
			map[string]any{"type": "tool_use", "id": "TU-invalid"},
			map[string]any{"type": "text", "text": "user text"},
		},
	})
	actor.mu.Lock()
	user := actor.messages[1]
	userReplay := actor.replayEvents[len(actor.replayEvents)-1].Payload
	actor.mu.Unlock()
	if got := stringValue(userReplay["state"]); got != "complete" {
		t.Fatalf("user replay state = %q, want complete", got)
	}
	if _, exists := userReplay["blockIndex"]; exists {
		t.Fatalf("user replay kept blockIndex: %#v", userReplay)
	}
	if len(user.Content) != 1 || stringValue(mapValue(user.Content[0])["text"]) != "user text" {
		t.Fatalf("user content = %#v, want filtered text", user.Content)
	}
	waitForNeoActorSyncIdle(t, actor)
}

func TestNeoRuntimeProtocolMessagesNormalizeContentLikeBinary(t *testing.T) {
	dir := t.TempDir()
	oldStoreDir := neoAmpThreadStoreDir
	neoAmpThreadStoreDir = func() string { return dir }
	t.Cleanup(func() { neoAmpThreadStoreDir = oldStoreDir })

	rt := newNeoRuntime(&config.Config{})
	actor := rt.store.ensureThreadActor("T-protocol-message-normalize")

	actor.handle(map[string]any{"type": "message_added", "message": map[string]any{
		"messageId": "M-assistant",
		"role":      "assistant",
		"content": []any{
			map[string]any{"type": "text", "text": "kept"},
			map[string]any{"type": "thinking", "thinking": "missing signature"},
		},
		"state": map[string]any{"type": "streaming"},
	}})
	actor.mu.Lock()
	assistant := actor.messages[0]
	actor.mu.Unlock()
	if len(assistant.Content) != 1 || stringValue(mapValue(assistant.Content[0])["text"]) != "kept" {
		t.Fatalf("assistant content = %#v, want only valid text", assistant.Content)
	}
	if len(assistant.State) != 0 {
		t.Fatalf("assistant invalid message state was preserved: %#v", assistant.State)
	}

	actor.handle(map[string]any{"type": "message_added", "message": map[string]any{
		"messageId": "M-user",
		"role":      "user",
		"content": []any{
			map[string]any{
				"type":       "image",
				"name":       "shot.png",
				"media_type": "image/png",
				"source":     map[string]any{"type": "base64", "media_type": "image/png", "data": "abcd"},
			},
		},
	}})
	actor.mu.Lock()
	user := actor.messages[1]
	actor.mu.Unlock()
	image := mapValue(user.Content[0])
	source := mapValue(image["source"])
	if got := stringValue(source["mediaType"]); got != "image/png" {
		t.Fatalf("image source mediaType = %q, want image/png; block=%#v", got, image)
	}
	if got := stringValue(image["sourcePath"]); got != "shot.png" {
		t.Fatalf("image sourcePath = %q, want shot.png", got)
	}

	actor.handle(map[string]any{"type": "message_added", "message": map[string]any{
		"messageId": "M-info",
		"role":      "info",
		"content": []any{
			map[string]any{"type": "text", "text": "drop"},
			map[string]any{"type": "manual_bash_invocation", "args": map[string]any{"cmd": "pwd"}, "toolRun": map[string]any{"status": "success"}},
		},
	}})
	actor.mu.Lock()
	info := actor.messages[2]
	beforeInvalid := len(actor.messages)
	actor.mu.Unlock()
	if len(info.Content) != 1 || stringValue(mapValue(info.Content[0])["type"]) != "manual_bash_invocation" {
		t.Fatalf("info content = %#v, want only manual bash invocation", info.Content)
	}

	actor.handle(map[string]any{"type": "message_added", "message": map[string]any{
		"messageId": "M-invalid",
		"role":      "assistant",
	}})
	actor.mu.Lock()
	afterInvalid := len(actor.messages)
	actor.mu.Unlock()
	if afterInvalid != beforeInvalid {
		t.Fatalf("invalid message without content was stored: before=%d after=%d", beforeInvalid, afterInvalid)
	}
	waitForNeoActorSyncIdle(t, actor)
}

func TestNeoRuntimeThreadImportDerivesModeFromMessages(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := rt.store.ensureThreadActor("T-import-mode")
	actor.pendingInference = &neoInferenceInflight{agentMode: "smart", reasoningEffort: "high"}

	thread := map[string]any{
		"id": "T-import-mode",
		"messages": []any{
			map[string]any{"role": "user", "messageId": "M-user", "agentMode": "deep", "reasoningEffort": "xhigh", "content": []any{map[string]any{"type": "text", "text": "keep deep"}}},
		},
	}
	if err := actor.importThreadLocalOnly(thread); err != nil {
		t.Fatalf("importThreadLocalOnly error: %v", err)
	}

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if actor.currentAgentMode != "deep" || actor.settings["agentMode"] != "deep" || actor.currentReasoningEffort != "xhigh" || actor.settings["reasoning.effort"] != "xhigh" {
		t.Fatalf("imported mode/effort = current:%q/%q settings:%#v", actor.currentAgentMode, actor.currentReasoningEffort, actor.settings)
	}
	if actor.pendingInference != nil {
		t.Fatalf("pending inference after import = %#v, want nil", actor.pendingInference)
	}
}

func TestNeoRuntimeThreadImportDoesNotDeriveModeFromMetaLikeBinary(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := rt.store.ensureThreadActor("T-import-meta-mode")
	actor.currentAgentMode = "smart"
	actor.currentReasoningEffort = "high"
	actor.settings = map[string]any{"agentMode": "smart", "reasoning.effort": "high"}

	thread := map[string]any{
		"id":   "T-import-meta-mode",
		"meta": map[string]any{"agentMode": "deep"},
		"messages": []any{
			map[string]any{"role": "user", "messageId": "M-user", "content": []any{map[string]any{"type": "text", "text": "meta carries mode"}}},
		},
	}
	if err := actor.importThreadLocalOnly(thread); err == nil || !strings.Contains(err.Error(), "agent mode could not be determined from thread") {
		t.Fatalf("importThreadLocalOnly error = %v, want binary-style missing mode error", err)
	}
}

func TestNeoRuntimeThreadImportResetsSmartEffortForDeepThread(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := rt.store.ensureThreadActor("T-import-effort")
	actor.currentAgentMode = "smart"
	actor.currentReasoningEffort = "high"
	actor.settings = map[string]any{"agentMode": "smart", "reasoning.effort": "high"}

	thread := map[string]any{
		"id":        "T-import-effort",
		"agentMode": "deep",
		"messages": []any{
			map[string]any{"role": "user", "messageId": "M-user", "agentMode": "deep", "content": []any{map[string]any{"type": "text", "text": "keep deep effort"}}},
		},
	}
	if err := actor.importThreadLocalOnly(thread); err != nil {
		t.Fatalf("importThreadLocalOnly error: %v", err)
	}

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if actor.currentAgentMode != "deep" || actor.currentReasoningEffort != "medium" || actor.settings["reasoning.effort"] != "medium" {
		t.Fatalf("imported mode/effort = current:%q/%q settings:%#v", actor.currentAgentMode, actor.currentReasoningEffort, actor.settings)
	}
}

func TestNeoRuntimeThreadActorResumeKeepsImportedMode(t *testing.T) {
	dir := t.TempDir()
	oldStoreDir := neoAmpThreadStoreDir
	neoAmpThreadStoreDir = func() string { return dir }
	t.Cleanup(func() { neoAmpThreadStoreDir = oldStoreDir })

	threadID := "T-resume-mode"
	if err := os.WriteFile(filepath.Join(dir, threadID+".json"), []byte(`{"id":"`+threadID+`","agentMode":"deep","meta":{"usesThreadActors":true,"cliProxyAPILocalNeo":true},"messages":[{"role":"user","messageId":"M-user","agentMode":"smart","reasoningEffort":"high","content":[{"type":"text","text":"keep top-level deep"}]}]}`), 0o600); err != nil {
		t.Fatalf("write local thread: %v", err)
	}

	rt := newNeoRuntime(&config.Config{})
	response, status := rt.localThreadActorManagementResponse(context.Background(), map[string]any{"agentMode": "smart"}, threadID)
	if status != http.StatusOK {
		t.Fatalf("status = %d response=%#v", status, response)
	}
	if response["agentMode"] != "deep" {
		t.Fatalf("resume response mode = %#v, want deep", response["agentMode"])
	}
	actor := rt.store.ensureThreadActor(threadID)
	actor.mu.Lock()
	defer actor.mu.Unlock()
	if actor.currentAgentMode != "deep" || actor.currentReasoningEffort != "medium" || actor.settings["agentMode"] != "deep" || actor.settings["reasoning.effort"] != "medium" {
		t.Fatalf("actor mode/effort = current:%q/%q settings:%#v", actor.currentAgentMode, actor.currentReasoningEffort, actor.settings)
	}
}

func TestNeoRuntimeThreadActorManagementUsesExistingModeWhenRequestOmitsMode(t *testing.T) {
	dir := t.TempDir()
	oldStoreDir := neoAmpThreadStoreDir
	neoAmpThreadStoreDir = func() string { return dir }
	t.Cleanup(func() { neoAmpThreadStoreDir = oldStoreDir })

	threadID := "T-existing-mode"
	rt := newNeoRuntime(&config.Config{})
	actor := rt.store.ensureThreadActor(threadID)
	actor.updateSettings(map[string]any{"agentMode": "rush"})

	response, status := rt.localThreadActorManagementResponse(context.Background(), map[string]any{}, threadID)
	if status != http.StatusOK {
		t.Fatalf("status = %d response=%#v", status, response)
	}
	if response["agentMode"] != "rush" {
		t.Fatalf("resume response mode = %#v, want rush", response["agentMode"])
	}
	actor.mu.Lock()
	defer actor.mu.Unlock()
	if actor.currentAgentMode != "rush" || actor.currentReasoningEffort != "none" {
		t.Fatalf("actor mode/effort = %q/%q, want rush/none", actor.currentAgentMode, actor.currentReasoningEffort)
	}
}

func TestNeoActorThreadSnapshotIncludesArtifacts(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.artifacts = map[string]any{
		"artifact-b": map[string]any{"key": "artifact-b", "type": "markdown", "content": "notes"},
	}

	snapshot, ok := actor.threadSnapshot()
	if !ok {
		t.Fatal("threadSnapshot returned false")
	}
	if len(snapshot.artifacts) != 1 {
		t.Fatalf("snapshot artifacts = %#v", snapshot.artifacts)
	}
	thread := neoCloudThread(snapshot)
	artifacts := arrayValue(thread["artifacts"])
	if len(artifacts) != 1 || stringValue(mapValue(artifacts[0])["key"]) != "artifact-b" {
		t.Fatalf("cloud artifacts = %#v", thread["artifacts"])
	}
	artifact := mapValue(artifacts[0])
	if stringValue(artifact["dataType"]) != "text/markdown" || stringValue(artifact["contentBase64"]) != "bm90ZXM=" || stringValue(artifact["updatedAt"]) == "" {
		t.Fatalf("cloud artifact was not protocol-normalized: %#v", artifact)
	}
	if _, exists := artifact["content"]; exists {
		t.Fatalf("cloud artifact retained non-schema content field: %#v", artifact)
	}
	if _, exists := artifact["type"]; exists {
		t.Fatalf("cloud artifact retained non-schema type field: %#v", artifact)
	}
}

func TestNeoActorArtifactUpsertAddsProtocolMetadata(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)

	actor.handle(map[string]any{
		"type":       "executor_artifact_upsert",
		"toolCallId": "TU-artifact",
		"artifact": map[string]any{
			"key":           "artifact-1",
			"dataType":      "text/plain",
			"contentBase64": "aGk=",
		},
	})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	artifact := mapValue(actor.artifacts["artifact-1"])
	if stringValue(artifact["toolCallId"]) != "TU-artifact" || stringValue(artifact["updatedAt"]) == "" {
		t.Fatalf("artifact metadata = %#v", artifact)
	}
}

func TestNeoActorArtifactUpsertAcceptsBinaryTopLevelWorkspaceSnapshot(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)

	actor.handle(map[string]any{
		"type":      "executor_artifact_upsert",
		"available": true,
		"fileCount": 13,
		"branch":    "dev",
		"head":      "abc123",
	})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.artifacts) != 1 {
		t.Fatalf("artifacts = %#v, want one workspace artifact", actor.artifacts)
	}
	artifact := mapValue(actor.artifacts["workspace-artifacts"])
	if artifact["available"] != true || numberFrom(artifact["fileCount"]) != 13 || stringValue(artifact["branch"]) != "dev" || stringValue(artifact["head"]) != "abc123" {
		t.Fatalf("workspace artifact metadata = %#v", artifact)
	}
	if stringValue(artifact["dataType"]) != "application/json" || stringValue(artifact["contentBase64"]) == "" {
		t.Fatalf("workspace artifact payload = %#v", artifact)
	}
}

func TestNeoThreadMarkdownTruncatesToolResultsLikeAmpBinary(t *testing.T) {
	longResult := strings.Repeat("x", neoThreadMarkdownToolTextLimit+500) + "tail"
	thread := map[string]any{
		"id":      "T-local-store",
		"created": 1778170000000,
		"messages": []any{
			map[string]any{
				"role": "assistant",
				"content": []any{map[string]any{
					"type": "tool_use",
					"id":   "TU-edit",
					"name": "edit_file",
					"input": map[string]any{
						"path":    "app.go",
						"old_str": "old body",
						"new_str": "new body",
					},
				}},
			},
			map[string]any{
				"role": "user",
				"content": []any{map[string]any{
					"type":      "tool_result",
					"toolUseID": "TU-read",
					"run":       map[string]any{"status": "done", "result": longResult},
				}},
			},
		},
	}

	full := neoThreadMarkdown(thread)
	if !strings.Contains(full, "tail") {
		t.Fatalf("full markdown should include untruncated result:\n%s", full)
	}
	truncated := neoThreadMarkdown(thread, neoThreadMarkdownOptions{TruncateToolResults: true})
	for _, want := range []string{
		neoThreadMarkdownOmittedText,
		"[... old_str omitted in markdown version ...]",
		"[... new_str omitted in markdown version ...]",
	} {
		if !strings.Contains(truncated, want) {
			t.Fatalf("truncated markdown missing %q:\n%s", want, truncated)
		}
	}
	if strings.Contains(truncated, "tail") {
		t.Fatalf("truncated markdown leaked tail content:\n%s", truncated)
	}
}

func TestNeoRuntimeDoesNotServeLocalThreadHTTP(t *testing.T) {
	dir := t.TempDir()
	oldStoreDir := neoAmpThreadStoreDir
	neoAmpThreadStoreDir = func() string { return dir }
	t.Cleanup(func() { neoAmpThreadStoreDir = oldStoreDir })

	snapshot := neoCloudThreadSnapshot{
		threadID:  "T-local-store",
		seq:       2,
		createdMs: 1778170000000,
		messages: []neoMessage{
			{ThreadID: "T-local-store", MessageID: "M-user", Role: "user", Content: []any{map[string]any{"type": "text", "text": "hello from stored thread"}}, AgentMode: "deep", Seq: 1},
		},
	}
	if err := writeNeoLocalThreadSnapshot(snapshot); err != nil {
		t.Fatalf("writeNeoLocalThreadSnapshot error: %v", err)
	}

	rt := newNeoRuntime(&config.Config{})
	req := httptest.NewRequest(http.MethodGet, "/threads/T-local-store.md", nil)
	rec := httptest.NewRecorder()
	rt.handleHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("GET /threads/T-local-store.md status = %d body=%s", rec.Code, rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/api/threads/T-local-store", nil)
	rec = httptest.NewRecorder()
	rt.handleHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("GET /api/threads/T-local-store status = %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestNeoThreadMarkdownTruncatesLocalToolResults(t *testing.T) {
	dir := t.TempDir()
	oldStoreDir := neoAmpThreadStoreDir
	neoAmpThreadStoreDir = func() string { return dir }
	t.Cleanup(func() { neoAmpThreadStoreDir = oldStoreDir })

	threadID := "T-local-truncated"
	snapshot := neoCloudThreadSnapshot{
		threadID:  threadID,
		seq:       2,
		createdMs: 1778170000000,
		messages: []neoMessage{
			{
				ThreadID:  threadID,
				MessageID: "M-tool-result",
				Role:      "user",
				Content: []any{map[string]any{
					"type":      "tool_result",
					"toolUseID": "TU-read",
					"run":       map[string]any{"status": "done", "result": strings.Repeat("x", neoThreadMarkdownToolTextLimit+500) + "tail"},
				}},
				Seq: 1,
			},
		},
	}
	if err := writeNeoLocalThreadSnapshot(snapshot); err != nil {
		t.Fatalf("writeNeoLocalThreadSnapshot error: %v", err)
	}
	thread, ok := loadNeoLocalThread(threadID)
	if !ok {
		t.Fatal("loadNeoLocalThread returned not ok")
	}
	body := neoThreadMarkdown(thread, neoThreadMarkdownOptions{TruncateToolResults: true})
	if !strings.Contains(body, neoThreadMarkdownOmittedText) {
		t.Fatalf("markdown was not truncated:\n%s", body)
	}
	if strings.Contains(body, "tail") {
		t.Fatalf("markdown leaked truncated tail:\n%s", body)
	}
}

func TestNeoRuntimeDoesNotServeLocalThreadSearchHTTP(t *testing.T) {
	dir := t.TempDir()
	oldStoreDir := neoAmpThreadStoreDir
	neoAmpThreadStoreDir = func() string { return dir }
	t.Cleanup(func() { neoAmpThreadStoreDir = oldStoreDir })

	snapshot := neoCloudThreadSnapshot{
		threadID:  "T-local-search",
		seq:       2,
		createdMs: 1778170000000,
		title:     "Local strategy migration",
		messages: []neoMessage{
			{ThreadID: "T-local-search", MessageID: "M-user", Role: "user", Content: []any{map[string]any{"type": "text", "text": "arena strategy context"}}, AgentMode: "deep", Seq: 1},
			{ThreadID: "T-local-search", MessageID: "M-assistant", Role: "assistant", Content: []any{map[string]any{"type": "text", "text": "assistant response"}}, Seq: 2},
			{ThreadID: "T-local-search", MessageID: "M-tool-result", Role: "user", Content: []any{map[string]any{"type": "tool_result", "toolUseID": "TU-read"}}, Seq: 3},
		},
	}
	if err := writeNeoLocalThreadSnapshot(snapshot); err != nil {
		t.Fatalf("writeNeoLocalThreadSnapshot error: %v", err)
	}
	decoy := neoCloudThreadSnapshot{
		threadID:  "T-local-decoy",
		seq:       2,
		createdMs: 1778180000000,
		title:     "Recent thread mentioning target",
		messages: []neoMessage{
			{ThreadID: "T-local-decoy", MessageID: "M-user", Role: "user", Content: []any{map[string]any{"type": "text", "text": "this mentions T-local-search but is not the target"}}, AgentMode: "deep", Seq: 1},
		},
	}
	if err := writeNeoLocalThreadSnapshot(decoy); err != nil {
		t.Fatalf("writeNeoLocalThreadSnapshot decoy error: %v", err)
	}

	rt := newNeoRuntime(&config.Config{})
	req := httptest.NewRequest(http.MethodGet, "/api/threads/find?q=task:T-local-search&limit=5", nil)
	rec := httptest.NewRecorder()
	rt.handleHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("GET /api/threads/find status = %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestFinishAssistantMessageUsesStableNormalizedToolCallID(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)

	actor.finishAssistantMessage("M-assistant", neoInferenceResult{
		Text: "running",
		ToolCalls: []neoToolCall{
			{ID: "call-provider-id", Name: "Bash", Input: map[string]any{"cmd": "true"}},
		},
	}, "smart", "")

	actor.mu.Lock()
	defer actor.mu.Unlock()

	if len(actor.messages) != 1 {
		t.Fatalf("messages len = %d", len(actor.messages))
	}
	content := actor.messages[0].Content
	if len(content) != 2 {
		t.Fatalf("assistant content = %#v", content)
	}
	toolUseID := stringValue(mapValue(content[1])["id"])
	if !strings.HasPrefix(toolUseID, "TU-") {
		t.Fatalf("tool_use id = %q, want TU-*", toolUseID)
	}
	if _, ok := actor.pendingTools[toolUseID]; !ok {
		t.Fatalf("pending tool missing normalized id %q: %#v", toolUseID, actor.pendingTools)
	}
	if len(actor.history) != 1 || len(actor.history[0].ToolCalls) != 1 {
		t.Fatalf("history = %#v", actor.history)
	}
	if actor.history[0].ToolCalls[0].ID != toolUseID {
		t.Fatalf("history tool id = %q, want %q", actor.history[0].ToolCalls[0].ID, toolUseID)
	}
}

func TestOpenAINeoMessagesDropsUnansweredToolCalls(t *testing.T) {
	messages := openAINeoMessages([]neoHistoryMessage{
		{Role: "user", Text: "hi"},
		{Role: "assistant", ToolCalls: []neoToolCall{
			{ID: "TU-done", Name: "Bash", Input: map[string]any{"cmd": "pwd"}},
			{ID: "TU-interrupted", Name: "Bash", Input: map[string]any{"cmd": "sleep 60"}},
		}},
		{Role: "tool", ToolCallID: "TU-done", ToolName: "Bash", Text: "workspace"},
		{Role: "user", Text: "continue"},
	}, "system")

	raw, err := json.Marshal(messages)
	if err != nil {
		t.Fatalf("marshal messages: %v", err)
	}
	text := string(raw)
	if !strings.Contains(text, "TU-done") || !strings.Contains(text, `"role":"tool"`) {
		t.Fatalf("answered tool call was dropped: %s", text)
	}
	if strings.Contains(text, "TU-interrupted") {
		t.Fatalf("unanswered tool call leaked into OpenAI messages: %s", text)
	}
}

func TestNeoHistoryConvertsNonTerminalToolResultLikeBinary(t *testing.T) {
	toolNames := map[string]string{}
	history := make([]neoHistoryMessage, 0, 2)
	history = append(history, neoHistoryMessageFromStored(neoMessage{
		ThreadID:  "T-test",
		MessageID: "M-assistant",
		Role:      "assistant",
		Content: []any{map[string]any{
			"type":     "tool_use",
			"id":       "TU-running",
			"name":     "Bash",
			"input":    map[string]any{"cmd": "sleep 60"},
			"complete": true,
		}},
		State: map[string]any{"type": "complete", "stopReason": "tool_use"},
	}, toolNames)...)
	history = append(history, neoHistoryMessageFromStored(neoMessage{
		ThreadID:  "T-test",
		MessageID: "M-tool-result",
		Role:      "user",
		Content: []any{map[string]any{
			"type":      "tool_result",
			"toolUseID": "TU-running",
			"run": map[string]any{
				"status":   "in-progress",
				"progress": map[string]any{"output": "still running"},
			},
		}},
	}, toolNames)...)

	if len(history) != 2 || history[1].Role != "tool" || history[1].ToolCallID != "TU-running" || history[1].ToolName != "Bash" {
		t.Fatalf("history = %#v", history)
	}
	if !strings.Contains(history[1].Text, "Progress until cancellation:\nstill running") || !strings.Contains(history[1].Text, "still running when Amp restored") {
		t.Fatalf("non-terminal tool result text = %q", history[1].Text)
	}
	if strings.Contains(history[1].Text, "system:non-terminal-tool-result") {
		t.Fatalf("raw cancellation reason leaked into history text: %q", history[1].Text)
	}

	openAI := openAINeoMessages(history, "")
	raw, err := json.Marshal(openAI)
	if err != nil {
		t.Fatalf("marshal messages: %v", err)
	}
	text := string(raw)
	if !strings.Contains(text, "TU-running") || !strings.Contains(text, `"role":"tool"`) || !strings.Contains(text, "still running when Amp restored") {
		t.Fatalf("non-terminal tool result was not preserved as cancelled provider history: %s", text)
	}
}

func TestAnthropicNeoMessagesDedupesDuplicateToolResults(t *testing.T) {
	messages := anthropicNeoMessages([]neoHistoryMessage{
		{Role: "user", Text: "hi"},
		{Role: "assistant", ToolCalls: []neoToolCall{{ID: "TU-done", Name: "Bash", Input: map[string]any{"cmd": "pwd"}}}},
		{Role: "tool", ToolCallID: "TU-done", ToolName: "Bash", Text: "stale"},
		{Role: "tool", ToolCallID: "TU-done", ToolName: "Bash", Text: "workspace"},
	})

	if len(messages) != 3 {
		t.Fatalf("messages len = %d, want 3: %#v", len(messages), messages)
	}
	toolResult := mapValue(arrayValue(mapValue(messages[2])["content"])[0])
	if got := stringValue(toolResult["tool_use_id"]); got != "TU-done" {
		t.Fatalf("tool_use_id = %q, want TU-done", got)
	}
	if got := stringValue(toolResult["content"]); got != "stale" {
		t.Fatalf("tool_result content = %q, want first result to match binary pairing", got)
	}
}

func TestOpenAINeoMessagesSkipsFullyInterruptedToolOnlyAssistant(t *testing.T) {
	messages := openAINeoMessages([]neoHistoryMessage{
		{Role: "user", Text: "hi"},
		{Role: "assistant", ToolCalls: []neoToolCall{
			{ID: "TU-interrupted", Name: "Bash", Input: map[string]any{"cmd": "sleep 60"}},
		}},
		{Role: "user", Text: "continue"},
	}, "system")

	raw, err := json.Marshal(messages)
	if err != nil {
		t.Fatalf("marshal messages: %v", err)
	}
	text := string(raw)
	if strings.Contains(text, "TU-interrupted") || strings.Contains(text, "tool_calls") {
		t.Fatalf("interrupted tool-only assistant leaked into OpenAI messages: %s", text)
	}
	if !strings.Contains(text, "continue") {
		t.Fatalf("subsequent user message missing: %s", text)
	}
}

func TestFinishAssistantMessageAddsOpenAIThinkingBlockForDeepMode(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)

	actor.finishAssistantMessage("M-assistant", neoInferenceResult{
		Provider: "openai",
		Model:    "gpt-5.5",
		Text:     "hello",
	}, "deep", "xhigh")

	actor.mu.Lock()
	defer actor.mu.Unlock()

	if len(actor.messages) != 1 {
		t.Fatalf("messages len = %d", len(actor.messages))
	}
	content := actor.messages[0].Content
	if len(content) != 2 {
		t.Fatalf("assistant content = %#v", content)
	}
	thinking := mapValue(content[0])
	if thinking["type"] != "thinking" || thinking["signature"] != "" || thinking["provider"] != "openai" {
		t.Fatalf("thinking block = %#v", thinking)
	}
	text := mapValue(content[1])
	if text["type"] != "text" || text["text"] != "hello" {
		t.Fatalf("text block = %#v", text)
	}
}

func TestFinishAssistantMessageStoresOpenAIReasoningMetadataLikeBinary(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)

	actor.finishAssistantMessage("M-assistant", neoInferenceResult{
		Provider: "openai",
		Model:    "gpt-5.5",
		Text:     "done",
		ThinkingBlocks: []neoThinkingBlock{{
			Thinking:  "plan then act",
			Signature: "encrypted",
			ID:        "rs_1",
		}},
	}, "deep", "xhigh")

	actor.mu.Lock()
	content := cloneArray(actor.messages[0].Content)
	history := append([]neoHistoryMessage(nil), actor.history...)
	actor.mu.Unlock()

	thinking := mapValue(content[0])
	reasoning := mapValue(thinking["openAIReasoning"])
	if thinking["type"] != "thinking" || thinking["provider"] != "openai" || reasoning["id"] != "rs_1" || reasoning["encryptedContent"] != "encrypted" {
		t.Fatalf("stored thinking block = %#v", thinking)
	}
	if len(history) != 1 || len(history[0].ThinkingBlocks) != 1 {
		t.Fatalf("history = %#v", history)
	}
	historyThinking := history[0].ThinkingBlocks[0]
	if historyThinking.Provider != "openai" || historyThinking.ID != "rs_1" || historyThinking.Signature != "encrypted" || historyThinking.Thinking != "plan then act" {
		t.Fatalf("history thinking = %#v", historyThinking)
	}
}

func TestNormalizeNeoUsageKeepsRequiredCacheFields(t *testing.T) {
	got := normalizeNeoUsage(map[string]any{
		"prompt_tokens":     10,
		"completion_tokens": 2,
		"total_tokens":      12,
		"timestamp":         "2026-05-29T12:00:00Z",
		"serviceTier":       "priority",
	})

	if got["timestamp"] != "2026-05-29T12:00:00Z" {
		t.Fatalf("timestamp = %#v, want binary value", got["timestamp"])
	}
	if got["serviceTier"] != "priority" {
		t.Fatalf("serviceTier = %#v, want binary value", got["serviceTier"])
	}
	if _, ok := got["total_tokens"]; ok {
		t.Fatalf("provider total_tokens key leaked into payload: %#v", got)
	}

	if got["cacheCreationInputTokens"] != 0 {
		t.Fatalf("cacheCreationInputTokens = %#v, want 0", got["cacheCreationInputTokens"])
	}
	if got["cacheReadInputTokens"] != 0 {
		t.Fatalf("cacheReadInputTokens = %#v, want 0", got["cacheReadInputTokens"])
	}
	pruned := pruneNilJSON(map[string]any{"usage": got}).(map[string]any)
	usage := mapValue(pruned["usage"])
	if _, ok := usage["cacheCreationInputTokens"]; !ok {
		t.Fatalf("cacheCreationInputTokens was pruned: %#v", usage)
	}
	if _, ok := usage["cacheReadInputTokens"]; !ok {
		t.Fatalf("cacheReadInputTokens was pruned: %#v", usage)
	}
}

func TestNeoAssistantDeltaPayloadNormalizesUsageForAmpSchema(t *testing.T) {
	payload := neoAssistantDeltaPayload("M-assistant", []any{map[string]any{"type": "text", "text": "hi"}}, 0, "generating", map[string]any{
		"input_tokens":  2,
		"output_tokens": 1,
	})
	usage := mapValue(payload["usage"])
	if len(usage) == 0 {
		t.Fatalf("usage missing from payload: %#v", payload)
	}
	if _, ok := usage["input_tokens"]; ok {
		t.Fatalf("provider usage key leaked into payload: %#v", payload)
	}
	for _, key := range []string{"maxInputTokens", "inputTokens", "outputTokens", "cacheCreationInputTokens", "cacheReadInputTokens", "totalInputTokens"} {
		if _, ok := usage[key]; !ok {
			t.Fatalf("usage missing %s: %#v", key, usage)
		}
	}

	withoutUsage := neoAssistantDeltaPayload("M-assistant", []any{}, 0, "start", nil)
	if _, ok := withoutUsage["usage"]; ok {
		t.Fatalf("nil usage should be omitted: %#v", withoutUsage)
	}
}

func TestPruneNilJSONOmitsOptionalNullFields(t *testing.T) {
	got := pruneNilJSON(map[string]any{
		"type":            "agent_state",
		"state":           "idle",
		"messageId":       nil,
		"reasoningEffort": nil,
		"nested": map[string]any{
			"keep": "yes",
			"drop": nil,
		},
	}).(map[string]any)

	if _, ok := got["messageId"]; ok {
		t.Fatalf("messageId should be omitted: %#v", got)
	}
	if _, ok := got["reasoningEffort"]; ok {
		t.Fatalf("reasoningEffort should be omitted: %#v", got)
	}
	if _, ok := mapValue(got["nested"])["drop"]; ok {
		t.Fatalf("nested nil should be omitted: %#v", got["nested"])
	}
}

func TestFallbackHandlerLocalNeoInferenceFailsClosedWithoutAmpProxy(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	fh := NewFallbackHandlerWithMapper(func() *httputil.ReverseProxy { return nil }, nil, nil)
	router.POST("/api/provider/openai/v1/chat/completions", fh.WrapHandler(func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"unexpected": true})
	}))

	body := bytes.NewBufferString(`{"model":"definitely-not-local","messages":[{"role":"user","content":"hi"}]}`)
	req := httptest.NewRequest(http.MethodPost, "/api/provider/openai/v1/chat/completions", body)
	req.Header.Set(localNeoInferenceHeader, "1")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		data, _ := io.ReadAll(rec.Body)
		t.Fatalf("status=%d body=%s", rec.Code, data)
	}
	if !strings.Contains(rec.Body.String(), "local_neo_provider_unavailable") {
		t.Fatalf("expected fail-closed local Neo error, got %s", rec.Body.String())
	}
}
