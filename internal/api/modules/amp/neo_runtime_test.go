package amp

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
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

func TestNeoRuntimeSnapshotIncludesThreadStatusAndCompactionRecords(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	threadID := "T-019e0e6e-f3f1-7078-b5dd-748f66f8c25d"
	actor, _ := rt.store.upsert(map[string]any{"name": "threadActor", "key": threadID, "input": map[string]any{"threadId": threadID}}, true)
	actor.mu.Lock()
	actor.compactionRecords = []map[string]any{{"cutMessageId": "M-cut", "createdAt": "2026-01-01T00:00:00Z"}}
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
			if len(records) != 1 || stringValue(mapValue(records[0])["cutMessageId"]) != "M-cut" {
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

func TestNeoRuntimeAcceptsBatchedClientFrames(t *testing.T) {
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
}

func TestNeoRuntimeExecutorResumeBootstrapAfterCompletedBootstrap(t *testing.T) {
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
	if !boolValue(resumed["resumeBootstrap"]) {
		t.Fatalf("resumed executor_connect resumeBootstrap = false: %#v", resumed)
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

func TestNeoActorHandlesThreadStatusCompactionAndRetryEvents(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "threadActor", "", "", neoActorRecord("actor-test", "threadActor", ""), nil)

	actor.handle(map[string]any{"type": "thread_status", "status": "merged"})
	actor.handle(map[string]any{"type": "compaction_started"})
	actor.handle(map[string]any{"type": "compaction_complete", "cutMessageId": "M-cut", "createdAt": "2026-01-01T00:00:00Z"})
	actor.handle(map[string]any{"type": "retry_scheduled", "retryAt": 123, "attempt": 2, "maxAttempts": 3, "reason": "rate_limit"})

	actor.mu.Lock()
	if actor.threadStatus != "merged" {
		t.Fatalf("threadStatus = %q, want merged", actor.threadStatus)
	}
	if actor.compacting {
		t.Fatal("compacting should be false after compaction_complete")
	}
	if len(actor.compactionRecords) != 1 || stringValue(actor.compactionRecords[0]["cutMessageId"]) != "M-cut" {
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
  printf '\nAMP_URL=%s\nAMP_API_KEY=%s\nAMP_EXECUTOR=%s\nAMP_PWD=%s\nAMP_THREAD_ID=%s\n' "$AMP_URL" "$AMP_API_KEY" "$AMP_EXECUTOR" "$AMP_PWD" "$AMP_THREAD_ID"
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
		"args: [--headless] [T-test-thread]",
		"AMP_URL=http://127.0.0.1:8317",
		"AMP_API_KEY=local-key",
		"AMP_EXECUTOR=1",
		"AMP_PWD=" + realWorkDir,
		"AMP_THREAD_ID=T-test-thread",
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
	if got.Provider != "openai" || got.Model != "gpt-5.4(xhigh)" {
		t.Fatalf("route = %+v, want openai/gpt-5.4(xhigh)", got)
	}
}

func TestSelectNeoModelRouteDefaultsDeepToGPT55(t *testing.T) {
	got := selectNeoModelRoute("deep", nil)
	if got.Provider != "openai" || got.Model != "gpt-5.5" {
		t.Fatalf("route = %+v, want openai/gpt-5.5", got)
	}
}

func TestSelectNeoModelRouteDefaultsRushToHaiku45(t *testing.T) {
	got := selectNeoModelRoute("rush", nil)
	if got.Provider != "anthropic" || got.Model != "claude-haiku-4-5-20251001" {
		t.Fatalf("route = %+v, want anthropic/claude-haiku-4-5-20251001", got)
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

func TestNeoSystemPromptUsesRushModeInstructions(t *testing.T) {
	prompt := neoSystemPrompt(neoInferenceRequest{AgentMode: "rush"}, neoModelRoute{Provider: "anthropic", Model: "claude-haiku-4-5-20251001"})
	for _, want := range []string{"Amp (Rush Mode)", "SPEED FIRST", "ULTRA CONCISE", "<example>", "# File Links", "Speed is the priority"} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("rush prompt missing %q:\n%s", want, prompt)
		}
	}
}

func TestNeoSystemPromptUsesExpandedModeFamilies(t *testing.T) {
	deep := neoSystemPrompt(neoInferenceRequest{AgentMode: "deep"}, neoModelRoute{Provider: "openai", Model: "gpt-5.5"})
	for _, want := range []string{"## Discovery Discipline", "## Verification", "## Working with the user"} {
		if !strings.Contains(deep, want) {
			t.Fatalf("deep prompt missing %q:\n%s", want, deep)
		}
	}

	deep54 := neoSystemPrompt(neoInferenceRequest{AgentMode: "deep"}, neoModelRoute{Provider: "openai", Model: "gpt-5.4(xhigh)"})
	if !strings.Contains(deep54, "You are Amp. You and the user share the same workspace") || strings.Contains(deep54, "## Autonomy And Persistence") {
		t.Fatalf("deep gpt-5.4 fallback prompt not selected:\n%s", deep54)
	}

	genericOpenAI := neoSystemPrompt(neoInferenceRequest{AgentMode: "smart"}, neoModelRoute{Provider: "openai", Model: "gpt-5"})
	for _, want := range []string{"# Fast Context Understanding", "# Parallel Execution Policy", "# Final Status Spec"} {
		if !strings.Contains(genericOpenAI, want) {
			t.Fatalf("generic OpenAI prompt missing %q:\n%s", want, genericOpenAI)
		}
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
	if !strings.Contains(prompt, "### Available skills") || !strings.Contains(prompt, "- code-review: Review code (file: /skills/code-review/SKILL.md)") {
		t.Fatalf("prompt missing fallback skill names:\n%s", prompt)
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
	if actor.currentAgentMode != "rush" || actor.currentReasoningEffort != "" {
		t.Fatalf("rush update should clear reasoning effort: mode=%q effort=%q", actor.currentAgentMode, actor.currentReasoningEffort)
	}
	if _, ok := actor.settings["reasoning.effort"]; ok {
		t.Fatalf("rush settings should clear reasoning.effort: %#v", actor.settings)
	}
}

func TestNeoActorReasoningEffortDefaultsByMode(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.currentAgentMode = "smart"
	actor.currentReasoningEffort = "high"
	actor.messages = []neoMessage{{Role: "user", AgentMode: "smart", ReasoningEffort: "max"}}

	if got := actor.reasoningEffortForModeLocked("rush"); got != "" {
		t.Fatalf("rush effort = %q, want empty", got)
	}
	if got := actor.reasoningEffortForModeLocked("deep"); got != "medium" {
		t.Fatalf("deep effort = %q, want medium", got)
	}
	if got := actor.reasoningEffortForModeLocked("smart"); got != "high" {
		t.Fatalf("smart effort = %q, want high", got)
	}
}

func TestNeoActorFiltersAmpBuiltInToolsByMode(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-test", "T-test", neoActorRecord("actor-test", "thread-actor", "T-test"), nil)
	actor.tools = map[string]neoToolSpec{
		"Task":              {Name: "Task"},
		"task_list":         {Name: "task_list"},
		"read_thread":       {Name: "read_thread"},
		"shell_command":     {Name: "shell_command"},
		"tb__gemini-oracle": {Name: "tb__gemini-oracle"},
	}

	deep := actor.inferenceRequestLocked("deep", "xhigh", "")
	deepNames := map[string]bool{}
	for _, tool := range deep.Tools {
		deepNames[tool.Name] = true
	}
	if deepNames["Task"] || deepNames["task_list"] {
		t.Fatalf("deep tools should not include Task/task_list: %#v", deepNames)
	}
	if !deepNames["read_thread"] || !deepNames["shell_command"] || !deepNames["tb__gemini-oracle"] {
		t.Fatalf("deep tools lost allowed/custom tools: %#v", deepNames)
	}

	smart := actor.inferenceRequestLocked("smart", "high", "")
	smartNames := map[string]bool{}
	for _, tool := range smart.Tools {
		smartNames[tool.Name] = true
	}
	if !smartNames["Task"] || smartNames["task_list"] {
		t.Fatalf("smart tools should include Task but not task_list per current Amp mode table: %#v", smartNames)
	}

	rush := actor.inferenceRequestLocked("rush", "", "")
	rushNames := map[string]bool{}
	for _, tool := range rush.Tools {
		rushNames[tool.Name] = true
	}
	if !rushNames["Task"] || !rushNames["task_list"] || rushNames["shell_command"] {
		t.Fatalf("rush tools should include Task/task_list but not shell_command: %#v", rushNames)
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
	if len(anthropicContent) != 2 || stringValue(mapValue(anthropicContent[1])["type"]) != "image" {
		t.Fatalf("anthropic content did not preserve image: %#v", anthropicContent)
	}
	if data := stringValue(mapValue(mapValue(anthropicContent[1])["source"])["data"]); data != "aW1n" {
		t.Fatalf("anthropic image data = %q", data)
	}

	openai := openAINeoMessages([]neoHistoryMessage{msg}, "system")
	openAIContent := arrayValue(mapValue(openai[1])["content"])
	if len(openAIContent) != 2 || stringValue(mapValue(openAIContent[1])["type"]) != "image_url" {
		t.Fatalf("openai content did not preserve image: %#v", openAIContent)
	}
	if url := stringValue(mapValue(mapValue(openAIContent[1])["image_url"])["url"]); url != "data:image/png;base64,aW1n" {
		t.Fatalf("openai image url = %q", url)
	}

	google := googleNeoContents([]neoHistoryMessage{msg}, "system")
	googleParts := arrayValue(mapValue(google[1])["parts"])
	if len(googleParts) != 2 {
		t.Fatalf("google parts = %#v", googleParts)
	}
	inlineData := mapValue(mapValue(googleParts[1])["inlineData"])
	if stringValue(inlineData["data"]) != "aW1n" || stringValue(inlineData["mimeType"]) != "image/png" {
		t.Fatalf("google inlineData = %#v", inlineData)
	}
}

func TestInferNeoOpenAIStreamsTextAndToolCalls(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/provider/openai/v1/chat/completions" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		payload := readNeoJSON(r.Body)
		if payload["stream"] != true {
			t.Fatalf("stream = %#v, want true", payload["stream"])
		}
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, _ := w.(http.Flusher)
		for _, chunk := range []string{
			`data: {"choices":[{"delta":{"content":"hel"}}]}`,
			`data: {"choices":[{"delta":{"content":"lo"}}]}`,
			`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","function":{"name":"Bash","arguments":"{\"cmd\":\"pwd\"}"}}]}}]}`,
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
	result, err := inferNeoLocalStream(testNeoRuntimeForServer(t, upstream), neoInferenceRequest{
		ThreadID:        "T-test",
		AgentMode:       "deep",
		ReasoningEffort: "xhigh",
		Settings:        map[string]any{"internal.model": "openai/gpt-test"},
		History:         []neoHistoryMessage{{Role: "user", Text: "hi"}},
		Tools:           []neoToolSpec{{Name: "Bash", InputSchema: map[string]any{"type": "object"}}},
	}, func(delta neoInferenceDelta) {
		deltas = append(deltas, delta.Text)
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

func collectNeoOpenAIStreamBlockIndexes(t *testing.T, agentMode, toolName string, settings map[string]any) ([]int, int) {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/provider/openai/v1/chat/completions" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, _ := w.(http.Flusher)
		for _, chunk := range []string{
			`data: {"choices":[{"delta":{"content":"hel"}}]}`,
			`data: {"choices":[{"delta":{"content":"lo"}}]}`,
			`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","function":{"name":"` + toolName + `","arguments":"{\"cmd\":\"pwd\"}"}}]}}]}`,
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

func TestNeoActorRewritesWrongReadThreadToolResultFromLocalStore(t *testing.T) {
	dir := t.TempDir()
	oldStoreDir := neoAmpThreadStoreDir
	neoAmpThreadStoreDir = func() string { return dir }
	t.Cleanup(func() { neoAmpThreadStoreDir = oldStoreDir })

	snapshot := neoCloudThreadSnapshot{
		threadID:  "T-target-thread",
		seq:       2,
		createdMs: 1778170000000,
		messages: []neoMessage{
			{ThreadID: "T-target-thread", MessageID: "M-user", Role: "user", Content: []any{map[string]any{"type": "text", "text": "target thread details"}}, Seq: 1},
		},
	}
	if err := writeNeoLocalThreadSnapshot(snapshot); err != nil {
		t.Fatalf("writeNeoLocalThreadSnapshot error: %v", err)
	}

	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-current-thread", "T-current-thread", neoActorRecord("actor-test", "thread-actor", "T-current-thread"), nil)
	actor.pendingTools["TU-read"] = neoPendingTool{
		ID:    "TU-read",
		Name:  "read_thread",
		Input: map[string]any{"threadID": "T-target-thread", "goal": "extract context"},
	}
	actor.pendingTools["TU-other"] = neoPendingTool{ID: "TU-other", Name: "Bash"}

	actor.receiveToolResult(map[string]any{
		"type":       "executor_tool_result",
		"toolCallId": "TU-read",
		"run": map[string]any{
			"status": "done",
			"result": "The provided thread (T-current-thread) contains no message content.",
		},
	})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.history) != 1 {
		t.Fatalf("history = %#v", actor.history)
	}
	if !strings.Contains(actor.history[0].Text, "target thread details") || !strings.Contains(actor.history[0].Text, "Local thread fallback for T-target-thread") {
		t.Fatalf("history text = %q", actor.history[0].Text)
	}
	if _, ok := actor.pendingTools["TU-read"]; ok {
		t.Fatal("read thread tool still pending")
	}
}

func TestNeoActorRewritesEmptyFindThreadToolResultFromLocalStore(t *testing.T) {
	dir := t.TempDir()
	oldStoreDir := neoAmpThreadStoreDir
	neoAmpThreadStoreDir = func() string { return dir }
	t.Cleanup(func() { neoAmpThreadStoreDir = oldStoreDir })

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
	actor := newNeoActor(rt, "actor-test", "thread-actor", "T-current-thread", "T-current-thread", neoActorRecord("actor-test", "thread-actor", "T-current-thread"), nil)
	actor.pendingTools["TU-find"] = neoPendingTool{
		ID:    "TU-find",
		Name:  "find_thread",
		Input: map[string]any{"query": "T-search-target", "limit": "5"},
	}
	actor.pendingTools["TU-other"] = neoPendingTool{ID: "TU-other", Name: "Bash"}

	actor.receiveToolResult(map[string]any{
		"type":       "executor_tool_result",
		"toolCallId": "TU-find",
		"run":        map[string]any{"status": "done", "result": "map[hasMore:false threads:[]]"},
	})

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.history) != 1 {
		t.Fatalf("history = %#v", actor.history)
	}
	if !strings.Contains(actor.history[0].Text, "T-search-target") {
		t.Fatalf("history text = %q", actor.history[0].Text)
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
	if stringValue(approval["toolCallId"]) != "TU-child" || stringValue(approval["toolUseId"]) != "TU-child" {
		t.Fatalf("approval ids = %#v", approval)
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
		"id": "T-test",
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

func TestNeoRemoteControlGetThreadDecodesCloudData(t *testing.T) {
	threadID := "T-019e06a8-13c9-708d-8090-783005818ea7"
	var gotAuth string
	requests := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.URL.Path != "/api/internal" || r.URL.RawQuery != "getThread" {
			t.Fatalf("request path = %s?%s", r.URL.Path, r.URL.RawQuery)
		}
		gotAuth = r.Header.Get("Authorization")
		writeNeoJSON(w, http.StatusOK, map[string]any{
			"ok": true,
			"result": map[string]any{
				"thread": map[string]any{
					"id": threadID,
					"data": map[string]any{
						"id":       threadID,
						"title":    "Cloud thread",
						"messages": []any{},
					},
				},
			},
		})
	}))
	defer upstream.Close()

	thread, ok, err := getNeoCloudThread(context.Background(), &config.Config{
		AmpCode: config.AmpCode{UpstreamURL: upstream.URL, UpstreamAPIKey: "secret"},
	}, threadID)
	if err != nil {
		t.Fatalf("getNeoCloudThread error: %v", err)
	}
	if !ok || thread["id"] != threadID || thread["title"] != "Cloud thread" {
		t.Fatalf("thread = %#v ok=%v", thread, ok)
	}
	if gotAuth != "Bearer secret" {
		t.Fatalf("Authorization = %q", gotAuth)
	}

	if _, ok, err := getNeoCloudThread(context.Background(), &config.Config{
		AmpCode: config.AmpCode{UpstreamURL: upstream.URL, UpstreamAPIKey: "secret"},
	}, "T-test"); err != nil || ok {
		t.Fatalf("invalid cloud thread id returned ok=%v err=%v", ok, err)
	}
	if requests != 1 {
		t.Fatalf("invalid cloud thread id should not be requested; requests=%d", requests)
	}
}

func TestNeoRuntimeServesCloudThreadWhenLocalMissing(t *testing.T) {
	dir := t.TempDir()
	oldStoreDir := neoAmpThreadStoreDir
	neoAmpThreadStoreDir = func() string { return dir }
	t.Cleanup(func() { neoAmpThreadStoreDir = oldStoreDir })

	threadID := "T-019e0379-5ef4-72e9-9ce6-dc9408a838f5"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/threads/find" {
			writeNeoJSON(w, http.StatusOK, map[string]any{
				"hasMore": false,
				"threads": []any{map[string]any{
					"id":                threadID,
					"title":             "Cloud reference",
					"matchedSearchText": "cloud thread details",
				}},
			})
			return
		}
		if r.URL.Path != "/api/internal" || r.URL.RawQuery != "getThread" {
			t.Fatalf("request path = %s?%s", r.URL.Path, r.URL.RawQuery)
		}
		writeNeoJSON(w, http.StatusOK, map[string]any{
			"ok": true,
			"result": map[string]any{
				"thread": map[string]any{
					"id":    threadID,
					"title": "Cloud reference",
					"data": map[string]any{
						"messages": []any{
							map[string]any{"role": "user", "messageId": "M-cloud", "content": []any{map[string]any{"type": "text", "text": "cloud thread details"}}},
						},
					},
				},
			},
		})
	}))
	defer upstream.Close()

	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{UpstreamURL: upstream.URL, UpstreamAPIKey: "secret"}})
	req := httptest.NewRequest(http.MethodGet, "/threads/"+threadID+".md", nil)
	rec := httptest.NewRecorder()
	rt.handleHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /threads/:id.md status = %d body=%s", rec.Code, rec.Body.String())
	}
	if body := rec.Body.String(); !strings.Contains(body, "threadId: "+threadID) || !strings.Contains(body, "cloud thread details") {
		t.Fatalf("markdown missing cloud thread content:\n%s", body)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/threads/find?q=@"+threadID+"&limit=5", nil)
	rec = httptest.NewRecorder()
	rt.handleHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/threads/find status = %d body=%s", rec.Code, rec.Body.String())
	}
	var response map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("search JSON: %v", err)
	}
	threads := arrayValue(response["threads"])
	if len(threads) != 1 || mapValue(threads[0])["id"] != threadID {
		t.Fatalf("threads = %#v", response["threads"])
	}
}

func TestNeoRuntimeSearchesCloudThreadsHTTP(t *testing.T) {
	dir := t.TempDir()
	oldStoreDir := neoAmpThreadStoreDir
	neoAmpThreadStoreDir = func() string { return dir }
	t.Cleanup(func() { neoAmpThreadStoreDir = oldStoreDir })

	threadID := "T-019e06ba-3f8b-72ee-8607-0cd5575b1a7e"
	var gotAuth string
	var gotQuery string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/threads/find" {
			t.Fatalf("request path = %s?%s", r.URL.Path, r.URL.RawQuery)
		}
		gotAuth = r.Header.Get("Authorization")
		gotQuery = r.URL.Query().Get("q")
		writeNeoJSON(w, http.StatusOK, map[string]any{
			"hasMore": false,
			"threads": []any{map[string]any{
				"id":                threadID,
				"title":             "Cloud search result",
				"created":           1778229329803,
				"updatedAt":         "2026-05-08T08:35:29.803Z",
				"messageCount":      5,
				"matchedSearchText": "remote needle context",
			}},
		})
	}))
	defer upstream.Close()

	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{UpstreamURL: upstream.URL, UpstreamAPIKey: "secret"}})
	req := httptest.NewRequest(http.MethodGet, "/api/threads/find?q=remote+needle&limit=5", nil)
	rec := httptest.NewRecorder()
	rt.handleHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/threads/find status = %d body=%s", rec.Code, rec.Body.String())
	}
	if gotAuth != "Bearer secret" {
		t.Fatalf("Authorization = %q", gotAuth)
	}
	if gotQuery != "remote needle" {
		t.Fatalf("cloud q = %q", gotQuery)
	}
	var response map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("search JSON: %v", err)
	}
	threads := arrayValue(response["threads"])
	if len(threads) != 1 || mapValue(threads[0])["id"] != threadID {
		t.Fatalf("threads = %#v", response["threads"])
	}
	if stringValue(mapValue(threads[0])["matchedSearchText"]) != "remote needle context" {
		t.Fatalf("matchedSearchText = %#v", mapValue(threads[0])["matchedSearchText"])
	}
}

func TestNeoRemoteControlActorsIncludesCloudListThreads(t *testing.T) {
	dir := t.TempDir()
	oldStoreDir := neoAmpThreadStoreDir
	neoAmpThreadStoreDir = func() string { return dir }
	t.Cleanup(func() { neoAmpThreadStoreDir = oldStoreDir })

	threadID := "T-019e072a-de6c-7410-89dc-3da62916ace9"
	var gotAuth string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/internal" || r.URL.RawQuery != "listThreads" {
			t.Fatalf("request path = %s?%s", r.URL.Path, r.URL.RawQuery)
		}
		gotAuth = r.Header.Get("Authorization")
		var request map[string]any
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if request["method"] != "listThreads" {
			t.Fatalf("method = %#v", request["method"])
		}
		writeNeoJSON(w, http.StatusOK, map[string]any{
			"ok": true,
			"result": map[string]any{
				"threads": []any{
					map[string]any{"id": threadID, "title": "Recent cloud thread"},
					map[string]any{"id": "T-local-not-cloud", "title": "ignored"},
				},
			},
		})
	}))
	defer upstream.Close()

	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{UpstreamURL: upstream.URL, UpstreamAPIKey: "secret"}})
	actors := rt.remoteControlActors(context.Background(), &config.Config{AmpCode: config.AmpCode{UpstreamURL: upstream.URL, UpstreamAPIKey: "secret"}})
	if gotAuth != "Bearer secret" {
		t.Fatalf("Authorization = %q", gotAuth)
	}
	if len(actors) != 1 || actors[0].threadID != threadID {
		t.Fatalf("actors = %#v", actors)
	}
}

func TestNeoReadThreadToolFallbackUsesCloudThread(t *testing.T) {
	dir := t.TempDir()
	oldStoreDir := neoAmpThreadStoreDir
	neoAmpThreadStoreDir = func() string { return dir }
	t.Cleanup(func() { neoAmpThreadStoreDir = oldStoreDir })

	threadID := "T-019e0379-5ef4-72e9-9ce6-dc9408a838f5"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeNeoJSON(w, http.StatusOK, map[string]any{
			"ok": true,
			"result": map[string]any{
				"thread": map[string]any{
					"id": threadID,
					"data": map[string]any{
						"id": threadID,
						"messages": []any{
							map[string]any{"role": "user", "messageId": "M-cloud", "content": []any{map[string]any{"type": "text", "text": "cloud fallback details"}}},
						},
					},
				},
			},
		})
	}))
	defer upstream.Close()

	run := normalizeNeoLocalThreadToolRun(context.Background(), &config.Config{
		AmpCode: config.AmpCode{UpstreamURL: upstream.URL, UpstreamAPIKey: "secret"},
	}, neoPendingTool{
		Name:  "read_thread",
		Input: map[string]any{"threadID": threadID, "goal": "extract task"},
	}, map[string]any{
		"status": "done",
		"result": "thread lookup returned no useful content-only metadata",
	}, "T-current")

	if got := stringValue(run["result"]); !strings.Contains(got, "cloud fallback details") || !strings.Contains(got, "Local thread fallback for "+threadID) {
		t.Fatalf("fallback result = %q", got)
	}
}

func TestNeoRemoteControlAppliesOnlyNewUserMessages(t *testing.T) {
	threadID := "T-019e06a8-13c9-708d-8090-783005818ea7"
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", threadID, threadID, neoActorRecord("actor-test", "thread-actor", threadID), nil)
	actor.executorReady = true
	actor.agentState = "working"
	actor.messages = []neoMessage{
		{ThreadID: threadID, MessageID: "M-existing", Role: "user", Content: []any{map[string]any{"type": "text", "text": "old"}}, Seq: 1},
	}
	actor.seq = 2

	actor.applyRemoteControlThread(map[string]any{
		"id": threadID,
		"messages": []any{
			map[string]any{"role": "user", "messageId": "M-existing", "content": []any{map[string]any{"type": "text", "text": "old"}}},
			map[string]any{"role": "assistant", "messageId": "M-cloud-assistant", "content": []any{map[string]any{"type": "text", "text": "cloud output"}}},
			map[string]any{"role": "user", "messageId": "M-tool-result", "content": []any{map[string]any{"type": "tool_result", "toolUseID": "TU-test"}}},
			map[string]any{"role": "user", "messageId": "M-new", "content": []any{map[string]any{"type": "text", "text": "remote work"}}, "agentMode": "deep", "reasoningEffort": "xhigh"},
		},
	})

	actor.mu.Lock()
	if len(actor.queue) != 0 {
		t.Fatalf("first remote-control poll should baseline existing cloud history: %#v", actor.queue)
	}
	actor.mu.Unlock()

	actor.applyRemoteControlThread(map[string]any{
		"id": threadID,
		"messages": []any{
			map[string]any{"role": "user", "messageId": "M-existing", "content": []any{map[string]any{"type": "text", "text": "old"}}},
			map[string]any{"role": "assistant", "messageId": "M-cloud-assistant", "content": []any{map[string]any{"type": "text", "text": "cloud output"}}},
			map[string]any{"role": "user", "messageId": "M-tool-result", "content": []any{map[string]any{"type": "tool_result", "toolUseID": "TU-test"}}},
			map[string]any{"role": "user", "messageId": "M-new", "content": []any{map[string]any{"type": "text", "text": "remote work"}}, "agentMode": "deep", "reasoningEffort": "xhigh"},
			map[string]any{"role": "user", "messageId": "M-new-2", "content": []any{map[string]any{"type": "text", "text": "remote work 2"}}, "agentMode": "deep", "reasoningEffort": "xhigh"},
		},
	})

	actor.mu.Lock()
	if len(actor.queue) != 1 {
		t.Fatalf("queue = %#v", actor.queue)
	}
	if actor.queue[0].MessageID != "M-new-2" || actor.queue[0].AgentMode != "deep" || actor.queue[0].ReasoningEffort != "xhigh" {
		t.Fatalf("queued remote message = %#v", actor.queue[0])
	}
	if len(actor.messages) != 1 {
		t.Fatalf("cloud assistant/tool messages should not be imported: %#v", actor.messages)
	}
	actor.mu.Unlock()

	actor.applyRemoteControlThread(map[string]any{
		"id": threadID,
		"messages": []any{
			map[string]any{"role": "user", "messageId": "M-new-2", "content": []any{map[string]any{"type": "text", "text": "remote work 2"}}},
		},
	})
	actor.mu.Lock()
	if len(actor.queue) != 1 {
		t.Fatalf("duplicate remote message was queued: %#v", actor.queue)
	}
	actor.mu.Unlock()
}

func TestNeoRemoteControlAppliesThreadBodyOnlyNewUserMessages(t *testing.T) {
	threadID := "T-019e06a8-13c9-708d-8090-783005818ea7"
	rt := newNeoRuntime(&config.Config{})
	actor := newNeoActor(rt, "actor-test", "thread-actor", threadID, threadID, neoActorRecord("actor-test", "thread-actor", threadID), nil)
	actor.executorReady = true
	actor.agentState = "working"
	actor.messages = []neoMessage{
		{ThreadID: threadID, MessageID: "M-existing", Role: "user", Content: []any{map[string]any{"type": "text", "text": "old"}}, Seq: 1},
	}
	actor.seq = 2

	actor.applyRemoteControlThreadBody([]byte(`{"ok":true,"result":{"thread":{"id":"` + threadID + `","data":{"messages":[` +
		`{"role":"user","messageId":"M-existing","content":[{"type":"text","text":"old"}]},` +
		`{"role":"assistant","messageId":"M-cloud-assistant","content":[{"type":"text","text":"cloud output"}]},` +
		`{"role":"user","messageId":"M-tool-result","content":[{"type":"tool_result","toolUseID":"TU-test"}]}` +
		`]}}}}`))
	actor.mu.Lock()
	if len(actor.queue) != 0 {
		t.Fatalf("first remote-control body poll should baseline: %#v", actor.queue)
	}
	actor.mu.Unlock()

	actor.applyRemoteControlThreadBody([]byte(`{"ok":true,"result":{"thread":{"id":"` + threadID + `","data":{"messages":[` +
		`{"role":"user","messageId":"M-existing","content":[{"type":"text","text":"old"}]},` +
		`{"role":"user","messageId":"M-new","content":[{"type":"text","text":"remote work"}],"userState":{"cwd":"/tmp/work"},"meta":{"source":"web"},"agentMode":"deep","reasoningEffort":"xhigh"}` +
		`]}}}}`))

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.queue) != 1 {
		t.Fatalf("queue = %#v", actor.queue)
	}
	queued := actor.queue[0]
	if queued.MessageID != "M-new" || queued.AgentMode != "deep" || queued.ReasoningEffort != "xhigh" {
		t.Fatalf("queued remote message = %#v", queued)
	}
	if textFromBlocks(queued.Content) != "remote work" {
		t.Fatalf("queued content = %#v", queued.Content)
	}
	userState := mapValue(queued.UserState)
	if stringValue(userState["cwd"]) != "/tmp/work" || stringValue(queued.Meta["source"]) != "web" {
		t.Fatalf("queued metadata = userState:%#v meta:%#v", queued.UserState, queued.Meta)
	}
}

func TestRecentNeoLocalThreadsReturnsMetadataOnly(t *testing.T) {
	dir := t.TempDir()
	oldStoreDir := neoAmpThreadStoreDir
	neoAmpThreadStoreDir = func() string { return dir }
	t.Cleanup(func() { neoAmpThreadStoreDir = oldStoreDir })

	threadID := "T-019e06a8-13c9-708d-8090-783005818ea7"
	if err := os.WriteFile(filepath.Join(dir, threadID+".json"), []byte(`{"id":"`+threadID+`","title":"local","messages":[{"messageId":"M-large","role":"user","created":1778229329803,"content":[{"type":"text","text":"`+strings.Repeat("x", 4096)+`"}]}]}`), 0o600); err != nil {
		t.Fatalf("write local thread: %v", err)
	}

	threads := recentNeoLocalThreads(10)
	if len(threads) != 1 {
		t.Fatalf("threads = %#v", threads)
	}
	if threads[0]["id"] != threadID || threads[0]["title"] != "local" {
		t.Fatalf("thread metadata = %#v", threads[0])
	}
	if _, exists := threads[0]["messages"]; exists {
		t.Fatalf("recent thread should not include full messages: %#v", threads[0])
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

	req := httptest.NewRequest(http.MethodPost, "/request/import", strings.NewReader(`{"thread":{"id":"T-import","v":7,"title":"Imported","agentMode":"deep","artifacts":[{"key":"artifact-1","type":"markdown","content":"notes"}],"messages":[{"role":"user","messageId":0,"content":[{"type":"text","text":"carry this"}]},{"role":"assistant","messageId":1,"content":[{"type":"text","text":"done"}],"state":{"type":"complete","stopReason":"end_turn"}}]}}`))
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
	if len(actor.messages) != 2 || actor.messages[0].MessageID != "0" || textFromBlocks(actor.messages[0].Content) != "carry this" {
		t.Fatalf("imported messages = %#v", actor.messages)
	}
	if len(actor.history) != 2 || actor.history[0].Text != "carry this" || actor.history[1].Text != "done" {
		t.Fatalf("history = %#v", actor.history)
	}
	if got := stringValue(mapValue(actor.artifacts["artifact-1"])["content"]); got != "notes" {
		t.Fatalf("imported artifact content = %q", got)
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
}

func TestNeoRuntimeServesLocalThreadHTTP(t *testing.T) {
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
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /threads/T-local-store.md status = %d body=%s", rec.Code, rec.Body.String())
	}
	if body := rec.Body.String(); !strings.Contains(body, "threadId: T-local-store") || !strings.Contains(body, "hello from stored thread") {
		t.Fatalf("markdown missing stored thread content:\n%s", body)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/threads/T-local-store", nil)
	rec = httptest.NewRecorder()
	rt.handleHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/threads/T-local-store status = %d body=%s", rec.Code, rec.Body.String())
	}
	var thread map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &thread); err != nil {
		t.Fatalf("thread JSON: %v", err)
	}
	if thread["id"] != "T-local-store" {
		t.Fatalf("thread id = %#v", thread["id"])
	}
}

func TestNeoRuntimeSearchesLocalThreadsHTTP(t *testing.T) {
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
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/threads/find status = %d body=%s", rec.Code, rec.Body.String())
	}
	var response map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("search JSON: %v", err)
	}
	threads := arrayValue(response["threads"])
	if len(threads) < 2 {
		t.Fatalf("threads = %#v", response["threads"])
	}
	thread := mapValue(threads[0])
	if thread["id"] != "T-local-search" || thread["messageCount"] != float64(1) {
		t.Fatalf("thread result = %#v", thread)
	}
	if !strings.Contains(stringValue(thread["matchedSearchText"]), "T-local-search") {
		t.Fatalf("matchedSearchText = %#v", thread["matchedSearchText"])
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
	if thinking["type"] != "thinking" || thinking["signature"] != "" {
		t.Fatalf("thinking block = %#v", thinking)
	}
	text := mapValue(content[1])
	if text["type"] != "text" || text["text"] != "hello" {
		t.Fatalf("text block = %#v", text)
	}
}

func TestNormalizeNeoUsageKeepsRequiredCacheFields(t *testing.T) {
	got := normalizeNeoUsage(map[string]any{
		"prompt_tokens":     10,
		"completion_tokens": 2,
		"total_tokens":      12,
	})

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
