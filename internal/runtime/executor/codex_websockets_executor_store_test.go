package executor

import (
	"net/http"
	"testing"
	"time"

	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
)

func TestCodexWebsocketsExecutor_SessionStoreSurvivesExecutorReplacement(t *testing.T) {
	sessionID := "test-session-store-survives-replace"

	globalCodexWebsocketSessionStore.mu.Lock()
	delete(globalCodexWebsocketSessionStore.sessions, sessionID)
	globalCodexWebsocketSessionStore.mu.Unlock()

	exec1 := NewCodexWebsocketsExecutor(nil)
	sess1 := exec1.getOrCreateSession(sessionID)
	if sess1 == nil {
		t.Fatalf("expected session to be created")
	}

	exec2 := NewCodexWebsocketsExecutor(nil)
	sess2 := exec2.getOrCreateSession(sessionID)
	if sess2 == nil {
		t.Fatalf("expected session to be available across executors")
	}
	if sess1 != sess2 {
		t.Fatalf("expected the same session instance across executors")
	}

	exec1.CloseExecutionSession(cliproxyauth.CloseAllExecutionSessionsID)

	globalCodexWebsocketSessionStore.mu.Lock()
	_, stillPresent := globalCodexWebsocketSessionStore.sessions[sessionID]
	globalCodexWebsocketSessionStore.mu.Unlock()
	if !stillPresent {
		t.Fatalf("expected session to remain after executor replacement close marker")
	}

	exec2.CloseExecutionSession(sessionID)

	globalCodexWebsocketSessionStore.mu.Lock()
	_, presentAfterClose := globalCodexWebsocketSessionStore.sessions[sessionID]
	globalCodexWebsocketSessionStore.mu.Unlock()
	if presentAfterClose {
		t.Fatalf("expected session to be removed after explicit close")
	}
}

func TestCodexWebsocketsExecutor_PruneIdleSessions(t *testing.T) {
	exec := NewCodexWebsocketsExecutor(nil)
	exec.store = &codexWebsocketSessionStore{
		sessions:    make(map[string]*codexWebsocketSession),
		idleTimeout: time.Minute,
	}
	now := time.Now()

	idle := exec.getOrCreateSession("idle")
	idle.idleMu.Lock()
	idle.lastUsed = now.Add(-2 * time.Minute)
	idle.idleMu.Unlock()

	active := exec.getOrCreateSession("active")
	active.idleMu.Lock()
	active.lastUsed = now.Add(-2 * time.Minute)
	active.idleMu.Unlock()
	active.reqMu.Lock()

	subscribed := exec.getOrCreateSession("subscribed")
	_ = exec.UpstreamDisconnectChan("subscribed")
	subscribed.idleMu.Lock()
	subscribed.lastUsed = now.Add(-2 * time.Minute)
	subscribed.idleMu.Unlock()

	if got := exec.pruneIdleSessions(now); got != 1 {
		active.reqMu.Unlock()
		t.Fatalf("pruneIdleSessions() = %d, want 1", got)
	}
	exec.store.mu.Lock()
	_, idlePresent := exec.store.sessions["idle"]
	_, activePresent := exec.store.sessions["active"]
	_, subscribedPresent := exec.store.sessions["subscribed"]
	exec.store.mu.Unlock()
	if idlePresent {
		active.reqMu.Unlock()
		t.Fatal("idle session was not removed")
	}
	if !activePresent {
		active.reqMu.Unlock()
		t.Fatal("active session was removed")
	}
	if !subscribedPresent {
		active.reqMu.Unlock()
		t.Fatal("subscribed downstream session was removed")
	}
	active.reqMu.Unlock()

	exec.CloseExecutionSession("active")
	exec.CloseExecutionSession("subscribed")
}

func TestCodexWebsocketsExecutor_AutomaticallyPrunesReleasedIdleSession(t *testing.T) {
	exec := NewCodexWebsocketsExecutor(nil)
	exec.store = &codexWebsocketSessionStore{
		sessions:    make(map[string]*codexWebsocketSession),
		idleTimeout: 10 * time.Millisecond,
	}

	sess := exec.acquireSession("automatic-idle")
	exec.releaseSession(sess)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		exec.store.mu.Lock()
		_, present := exec.store.sessions["automatic-idle"]
		exec.store.mu.Unlock()
		if !present {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("released idle session was not automatically removed")
}

func TestCodexWebsocketHandshakeFingerprintIncludesPromptCacheIdentity(t *testing.T) {
	newHeaders := func(sessionID string) http.Header {
		headers := http.Header{}
		headers.Set("session_id", sessionID)
		headers.Set("Conversation_id", sessionID)
		headers.Set("OpenAI-Beta", "responses_websockets=1")
		return headers
	}

	base := codexWebsocketHandshakeFingerprint(newHeaders("cache-a"), "", "cache-a")
	if got := codexWebsocketHandshakeFingerprint(newHeaders("cache-a"), "", "cache-a"); got != base {
		t.Fatalf("identical prompt-cache identity changed the fingerprint")
	}
	if got := codexWebsocketHandshakeFingerprint(newHeaders("cache-b"), "", "cache-b"); got == base {
		t.Fatalf("changed prompt-cache identity kept the retained-session fingerprint")
	}

	turnNoise := newHeaders("cache-b")
	turnNoise.Set("x-client-request-id", "request-1")
	cacheBFingerprint := codexWebsocketHandshakeFingerprint(turnNoise, "", "cache-b")
	if cacheBFingerprint == base {
		t.Fatalf("per-turn headers without a prompt-cache identity collapsed to the cache-a fingerprint")
	}
	turnNoise.Set("x-client-request-id", "request-2")
	if again := codexWebsocketHandshakeFingerprint(turnNoise, "", "cache-b"); again != cacheBFingerprint {
		t.Fatalf("rotating per-turn request header changed the fingerprint")
	}
}
