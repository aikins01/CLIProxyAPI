package executor

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/util"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v7/sdk/config"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestEncodeCodexWebsocketAsSSEFramesCompleteEvent(t *testing.T) {
	payload := []byte(`{"type":"response.created"}`)
	want := []byte("data: {\"type\":\"response.created\"}\n\n")
	if got := encodeCodexWebsocketAsSSE(payload); !bytes.Equal(got, want) {
		t.Fatalf("encoded event = %q, want %q", got, want)
	}
}

func TestBuildCodexWebsocketRequestBodyPreservesPreviousResponseID(t *testing.T) {
	body := []byte(`{"model":"gpt-5-codex","previous_response_id":"resp-1","input":[{"type":"message","id":"msg-1"}]}`)

	wsReqBody := buildCodexWebsocketRequestBody(body)

	if got := gjson.GetBytes(wsReqBody, "type").String(); got != "response.create" {
		t.Fatalf("type = %s, want response.create", got)
	}
	if got := gjson.GetBytes(wsReqBody, "previous_response_id").String(); got != "resp-1" {
		t.Fatalf("previous_response_id = %s, want resp-1", got)
	}
	if gjson.GetBytes(wsReqBody, "input.0.id").String() != "msg-1" {
		t.Fatalf("input item id mismatch")
	}
	if got := gjson.GetBytes(wsReqBody, "type").String(); got == "response.append" {
		t.Fatalf("unexpected websocket request type: %s", got)
	}
}

func TestCodexWebsocketsExecutePreservesPreviousResponseIDUpstream(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	capturedPayload := make(chan []byte, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/responses" {
			t.Fatalf("request path = %s, want /responses", r.URL.Path)
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade websocket: %v", err)
		}
		defer func() { _ = conn.Close() }()

		msgType, payload, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read upstream websocket message: %v", err)
		}
		if msgType != websocket.TextMessage {
			t.Fatalf("message type = %d, want text", msgType)
		}
		capturedPayload <- bytes.Clone(payload)

		completed := []byte(`{"type":"response.completed","response":{"id":"resp-2","output":[],"usage":{"input_tokens":0,"output_tokens":0,"total_tokens":0}}}`)
		if errWrite := conn.WriteMessage(websocket.TextMessage, completed); errWrite != nil {
			t.Fatalf("write completed websocket message: %v", errWrite)
		}
	}))
	defer server.Close()

	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{"api_key": "sk-test", "base_url": server.URL}}
	req := cliproxyexecutor.Request{
		Model:   "gpt-5-codex",
		Payload: []byte(`{"model":"gpt-5-codex","previous_response_id":"resp-1","input":[{"type":"message","id":"msg-1"}]}`),
	}
	opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("codex")}

	if _, err := exec.Execute(context.Background(), auth, req, opts); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	select {
	case payload := <-capturedPayload:
		if got := gjson.GetBytes(payload, "type").String(); got != "response.create" {
			t.Fatalf("upstream type = %s, want response.create; payload=%s", got, payload)
		}
		if got := gjson.GetBytes(payload, "previous_response_id").String(); got != "resp-1" {
			t.Fatalf("upstream previous_response_id = %s, want resp-1; payload=%s", got, payload)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for upstream websocket payload")
	}
}

func TestCodexWebsocketsLocalNeoRequestDoesNotInjectImageGeneration(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	capturedPayload := make(chan []byte, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade websocket: %v", err)
		}
		defer func() { _ = conn.Close() }()

		_, payload, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read upstream websocket message: %v", err)
		}
		capturedPayload <- bytes.Clone(payload)
		completed := []byte(`{"type":"response.completed","response":{"id":"resp-2","output":[],"usage":{"input_tokens":0,"output_tokens":0,"total_tokens":0}}}`)
		if errWrite := conn.WriteMessage(websocket.TextMessage, completed); errWrite != nil {
			t.Fatalf("write completed websocket message: %v", errWrite)
		}
	}))
	defer server.Close()

	exec := NewCodexWebsocketsExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{"api_key": "sk-test", "base_url": server.URL}}
	ctx := util.WithTrustedLocalNeoInference(context.Background())
	_, err := exec.Execute(ctx, auth, cliproxyexecutor.Request{
		Model:   "gpt-5.6-sol",
		Payload: []byte(`{"model":"gpt-5.6-sol","input":"summarize","tools":[]}`),
	}, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FromString("codex"),
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	select {
	case payload := <-capturedPayload:
		for _, tool := range gjson.GetBytes(payload, "tools").Array() {
			if tool.Get("type").String() == "image_generation" {
				t.Fatalf("local Neo websocket request exposed image_generation: %s", payload)
			}
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for upstream websocket payload")
	}
}

func TestCodexWebsocketsUpstreamDisconnectChanSignalsOnInvalidate(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade websocket: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()
		for {
			if _, _, errRead := conn.ReadMessage(); errRead != nil {
				return
			}
		}
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() { _ = conn.Close() }()

	exec := NewCodexWebsocketsExecutor(&config.Config{})
	sessionID := "sess-1"
	disconnectCh := exec.UpstreamDisconnectChan(sessionID)
	if disconnectCh == nil {
		t.Fatal("expected disconnect channel")
	}

	sess := exec.getOrCreateSession(sessionID)
	if sess == nil {
		t.Fatal("expected session")
	}
	sess.connMu.Lock()
	sess.conn = conn
	sess.authID = "auth-1"
	sess.wsURL = "ws://example.test/responses"
	sess.readerConn = conn
	sess.connMu.Unlock()

	upstreamErr := errors.New("upstream gone")
	exec.invalidateUpstreamConn(sess, conn, "test_invalidate", upstreamErr)

	select {
	case errRead, ok := <-disconnectCh:
		if !ok {
			t.Fatal("expected disconnect channel to deliver error before closing")
		}
		if errRead == nil || errRead.Error() != upstreamErr.Error() {
			t.Fatalf("disconnect error = %v, want %v", errRead, upstreamErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for disconnect signal")
	}
}

func TestCodexWebsocketsExecuteStreamReleasesSessionLockAfterHandshakeFailure(t *testing.T) {
	tests := []struct {
		name         string
		rejectStatus int
	}{
		{name: "upgrade fallback", rejectStatus: http.StatusUpgradeRequired},
		{name: "status error", rejectStatus: http.StatusInternalServerError},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
			var websocketAttempts atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !websocket.IsWebSocketUpgrade(r) {
					http.Error(w, "fallback failed", http.StatusInternalServerError)
					return
				}
				if websocketAttempts.Add(1) == 1 {
					http.Error(w, "handshake failed", tc.rejectStatus)
					return
				}
				conn, errUpgrade := upgrader.Upgrade(w, r, nil)
				if errUpgrade != nil {
					return
				}
				defer func() { _ = conn.Close() }()
				if _, _, errRead := conn.ReadMessage(); errRead != nil {
					return
				}
				completed := []byte(`{"type":"response.completed","response":{"id":"resp-2","output":[],"usage":{"input_tokens":0,"output_tokens":0,"total_tokens":0}}}`)
				_ = conn.WriteMessage(websocket.TextMessage, completed)
			}))
			defer server.Close()

			exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
			exec.store = &codexWebsocketSessionStore{sessions: make(map[string]*codexWebsocketSession)}
			sessionID := "handshake-failure-session"
			defer exec.CloseExecutionSession(sessionID)
			auth := &cliproxyauth.Auth{ID: "auth-1", Attributes: map[string]string{"api_key": "sk-test", "base_url": server.URL}}
			req := cliproxyexecutor.Request{Model: "gpt-5-codex", Payload: []byte(`{"model":"gpt-5-codex","input":[]}`)}
			opts := cliproxyexecutor.Options{
				SourceFormat: sdktranslator.FromString("codex"),
				Metadata:     map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: sessionID},
			}

			if _, err := exec.ExecuteStream(context.Background(), auth, req, opts); err == nil {
				t.Fatal("first ExecuteStream() error = nil, want handshake failure")
			}

			type streamCall struct {
				result *cliproxyexecutor.StreamResult
				err    error
			}
			callCh := make(chan streamCall, 1)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			go func() {
				result, err := exec.ExecuteStream(ctx, auth, req, opts)
				callCh <- streamCall{result: result, err: err}
			}()

			var call streamCall
			select {
			case call = <-callCh:
			case <-ctx.Done():
				t.Fatal("second ExecuteStream() blocked on the session request lock")
			}
			if call.err != nil {
				t.Fatalf("second ExecuteStream() error = %v", call.err)
			}
			if call.result == nil {
				t.Fatal("second ExecuteStream() result = nil")
			}
			for chunk := range call.result.Chunks {
				if chunk.Err != nil {
					t.Fatalf("second ExecuteStream() chunk error = %v", chunk.Err)
				}
			}
		})
	}
}

func TestCodexWebsocketsExecuteSendRetryUsesFreshReadChannel(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	var connections atomic.Int32
	retryRequest := make(chan struct{}, 1)
	allowCompletion := make(chan struct{})
	serverErr := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		connection := connections.Add(1)
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			select {
			case serverErr <- errUpgrade:
			default:
			}
			return
		}
		defer func() { _ = conn.Close() }()
		if _, _, errRead := conn.ReadMessage(); errRead != nil {
			if connection != 1 {
				select {
				case serverErr <- errRead:
				default:
				}
			}
			return
		}
		if connection != 2 {
			return
		}
		retryRequest <- struct{}{}
		<-allowCompletion
		completed := []byte(`{"type":"response.completed","response":{"id":"resp-2","output":[],"usage":{"input_tokens":0,"output_tokens":0,"total_tokens":0}}}`)
		if errWrite := conn.WriteMessage(websocket.TextMessage, completed); errWrite != nil {
			select {
			case serverErr <- errWrite:
			default:
			}
		}
	}))
	defer server.Close()
	completionAllowed := false
	defer func() {
		if !completionAllowed {
			close(allowCompletion)
		}
	}()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/responses"
	staleConn, _, errDial := websocket.DefaultDialer.Dial(wsURL, nil)
	if errDial != nil {
		t.Fatalf("dial stale websocket: %v", errDial)
	}
	defer func() { _ = staleConn.Close() }()
	if errDeadline := staleConn.SetWriteDeadline(time.Now().Add(-time.Second)); errDeadline != nil {
		t.Fatalf("set stale websocket write deadline: %v", errDeadline)
	}

	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	exec.store = &codexWebsocketSessionStore{sessions: make(map[string]*codexWebsocketSession)}
	sessionID := "send-retry-session"
	defer exec.CloseExecutionSession(sessionID)
	auth := &cliproxyauth.Auth{ID: "auth-1", Attributes: map[string]string{"api_key": "sk-test", "base_url": server.URL}}
	sess := exec.getOrCreateSession(sessionID)
	sess.connMu.Lock()
	sess.conn = staleConn
	sess.wsURL = wsURL
	sess.authID = "auth-1"
	sess.handshakeFingerprint = codexWebsocketHandshakeFingerprint(applyCodexWebsocketHeaders(context.Background(), http.Header{}, auth, "sk-test", exec.cfg), "", "")
	sess.connMu.Unlock()

	req := cliproxyexecutor.Request{Model: "gpt-5-codex", Payload: []byte(`{"model":"gpt-5-codex","input":[]}`)}
	opts := cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FromString("codex"),
		Metadata:     map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: sessionID},
	}
	type executeCall struct {
		resp cliproxyexecutor.Response
		err  error
	}
	executeCh := make(chan executeCall, 1)
	sess.writeMu.Lock()
	writeLocked := true
	defer func() {
		if writeLocked {
			sess.writeMu.Unlock()
		}
	}()
	go func() {
		resp, err := exec.Execute(context.Background(), auth, req, opts)
		executeCh <- executeCall{resp: resp, err: err}
	}()

	var initialReadCh chan codexWebsocketRead
	deadline := time.Now().Add(5 * time.Second)
	for initialReadCh == nil && time.Now().Before(deadline) {
		sess.activeMu.Lock()
		initialReadCh = sess.activeCh
		sess.activeMu.Unlock()
		if initialReadCh == nil {
			time.Sleep(time.Millisecond)
		}
	}
	if initialReadCh == nil {
		t.Fatal("initial session read channel was not activated")
	}
	sess.writeMu.Unlock()
	writeLocked = false

	select {
	case <-retryRequest:
	case errServer := <-serverErr:
		t.Fatalf("retry websocket server error: %v", errServer)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for retried websocket request")
	}
	sess.activeMu.Lock()
	retryReadCh := sess.activeCh
	sess.activeMu.Unlock()
	if retryReadCh == nil {
		t.Fatal("retry session read channel was not activated")
	}
	if retryReadCh == initialReadCh {
		t.Fatal("retry reused the failed connection's read channel")
	}
	close(allowCompletion)
	completionAllowed = true

	select {
	case call := <-executeCh:
		if call.err != nil {
			t.Fatalf("Execute() error = %v", call.err)
		}
		if len(call.resp.Payload) == 0 {
			t.Fatal("Execute() response payload is empty")
		}
	case errServer := <-serverErr:
		t.Fatalf("retry websocket server error: %v", errServer)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Execute() after websocket send retry")
	}
}

func TestApplyCodexWebsocketHeadersDefaultsToCurrentResponsesBeta(t *testing.T) {
	headers := applyCodexWebsocketHeaders(context.Background(), http.Header{}, nil, "", nil)

	if got := headers.Get("OpenAI-Beta"); got != codexResponsesWebsocketBetaHeaderValue {
		t.Fatalf("OpenAI-Beta = %s, want %s", got, codexResponsesWebsocketBetaHeaderValue)
	}
	if got := headers.Get("User-Agent"); got != codexUserAgent {
		t.Fatalf("User-Agent = %s, want %s", got, codexUserAgent)
	}
	if !strings.HasPrefix(codexUserAgent, codexOriginator+"/") {
		t.Fatalf("default Codex User-Agent = %s, want prefix %s/", codexUserAgent, codexOriginator)
	}
	if strings.HasPrefix(codexUserAgent, "codex-tui/") {
		t.Fatalf("default Codex User-Agent = %s, must not use stale codex-tui prefix", codexUserAgent)
	}
	if strings.Contains(codexUserAgent, "(codex-tui;") {
		t.Fatalf("default Codex User-Agent = %s, must not include stale codex-tui suffix", codexUserAgent)
	}
	if got := headers.Get("Originator"); got != codexOriginator {
		t.Fatalf("Originator = %s, want %s", got, codexOriginator)
	}
	if got := headers.Get("Version"); got != "" {
		t.Fatalf("Version = %q, want empty", got)
	}
	if got := headers.Get("x-codex-beta-features"); got != "" {
		t.Fatalf("x-codex-beta-features = %q, want empty", got)
	}
	if got := headers.Get("X-Codex-Turn-Metadata"); got != "" {
		t.Fatalf("X-Codex-Turn-Metadata = %q, want empty", got)
	}
	if got := headers.Get("X-Client-Request-Id"); got != "" {
		t.Fatalf("X-Client-Request-Id = %q, want empty", got)
	}
}

func TestApplyCodexWebsocketHeadersPassesThroughClientIdentityHeaders(t *testing.T) {
	auth := &cliproxyauth.Auth{
		Provider: "codex",
		Metadata: map[string]any{"email": "user@example.com"},
	}
	ctx := contextWithGinHeaders(map[string]string{
		"Originator":            "Codex Desktop",
		"User-Agent":            "codex_cli_rs/0.1.0",
		"Version":               "0.115.0-alpha.27",
		"X-Codex-Turn-Metadata": `{"turn_id":"turn-1"}`,
		"X-Client-Request-Id":   "019d2233-e240-7162-992d-38df0a2a0e0d",
		"session_id":            "sess-client",
	})

	headers := applyCodexWebsocketHeaders(ctx, http.Header{}, auth, "", nil)

	if got := headers.Get("Originator"); got != "Codex Desktop" {
		t.Fatalf("Originator = %s, want %s", got, "Codex Desktop")
	}
	if got := headers.Get("User-Agent"); got != "codex_cli_rs/0.1.0" {
		t.Fatalf("User-Agent = %s, want %s", got, "codex_cli_rs/0.1.0")
	}
	if got := headers.Get("Version"); got != "0.115.0-alpha.27" {
		t.Fatalf("Version = %s, want %s", got, "0.115.0-alpha.27")
	}
	if got := headers.Get("X-Codex-Turn-Metadata"); got != `{"turn_id":"turn-1"}` {
		t.Fatalf("X-Codex-Turn-Metadata = %s, want %s", got, `{"turn_id":"turn-1"}`)
	}
	if got := headers.Get("X-Client-Request-Id"); got != "019d2233-e240-7162-992d-38df0a2a0e0d" {
		t.Fatalf("X-Client-Request-Id = %s, want %s", got, "019d2233-e240-7162-992d-38df0a2a0e0d")
	}
	if got := headerValueCaseInsensitive(headers, "session_id"); got != "sess-client" {
		t.Fatalf("session_id = %s, want sess-client", got)
	}
	if _, ok := headers["session_id"]; !ok {
		t.Fatalf("expected lowercase session_id header key, got %#v", headers)
	}
}

func TestApplyCodexWebsocketHeadersUsesConfigDefaultsForOAuth(t *testing.T) {
	cfg := &config.Config{
		CodexHeaderDefaults: config.CodexHeaderDefaults{
			UserAgent:    "my-codex-client/1.0",
			BetaFeatures: "feature-a,feature-b",
		},
	}
	auth := &cliproxyauth.Auth{
		Provider: "codex",
		Metadata: map[string]any{"email": "user@example.com"},
	}

	headers := applyCodexWebsocketHeaders(context.Background(), http.Header{}, auth, "", cfg)

	if got := headers.Get("User-Agent"); got != "my-codex-client/1.0" {
		t.Fatalf("User-Agent = %s, want %s", got, "my-codex-client/1.0")
	}
	if got := headers.Get("x-codex-beta-features"); got != "feature-a,feature-b" {
		t.Fatalf("x-codex-beta-features = %s, want %s", got, "feature-a,feature-b")
	}
	if got := headers.Get("OpenAI-Beta"); got != codexResponsesWebsocketBetaHeaderValue {
		t.Fatalf("OpenAI-Beta = %s, want %s", got, codexResponsesWebsocketBetaHeaderValue)
	}
}

func TestApplyCodexWebsocketHeadersPrefersExistingHeadersOverClientAndConfig(t *testing.T) {
	cfg := &config.Config{
		CodexHeaderDefaults: config.CodexHeaderDefaults{
			UserAgent:    "config-ua",
			BetaFeatures: "config-beta",
		},
	}
	auth := &cliproxyauth.Auth{
		Provider: "codex",
		Metadata: map[string]any{"email": "user@example.com"},
	}
	ctx := contextWithGinHeaders(map[string]string{
		"User-Agent":            "client-ua",
		"X-Codex-Beta-Features": "client-beta",
	})
	headers := http.Header{}
	headers.Set("User-Agent", "existing-ua")
	headers.Set("X-Codex-Beta-Features", "existing-beta")

	got := applyCodexWebsocketHeaders(ctx, headers, auth, "", cfg)

	if gotVal := got.Get("User-Agent"); gotVal != "existing-ua" {
		t.Fatalf("User-Agent = %s, want %s", gotVal, "existing-ua")
	}
	if gotVal := got.Get("x-codex-beta-features"); gotVal != "existing-beta" {
		t.Fatalf("x-codex-beta-features = %s, want %s", gotVal, "existing-beta")
	}
}

func TestApplyCodexWebsocketHeadersConfigUserAgentOverridesClientHeader(t *testing.T) {
	cfg := &config.Config{
		CodexHeaderDefaults: config.CodexHeaderDefaults{
			UserAgent:    "config-ua",
			BetaFeatures: "config-beta",
		},
	}
	auth := &cliproxyauth.Auth{
		Provider: "codex",
		Metadata: map[string]any{"email": "user@example.com"},
	}
	ctx := contextWithGinHeaders(map[string]string{
		"User-Agent":            "client-ua",
		"X-Codex-Beta-Features": "client-beta",
	})

	headers := applyCodexWebsocketHeaders(ctx, http.Header{}, auth, "", cfg)

	if got := headers.Get("User-Agent"); got != "config-ua" {
		t.Fatalf("User-Agent = %s, want %s", got, "config-ua")
	}
	if got := headers.Get("x-codex-beta-features"); got != "client-beta" {
		t.Fatalf("x-codex-beta-features = %s, want %s", got, "client-beta")
	}
}

func TestApplyCodexWebsocketHeadersIgnoresConfigForAPIKeyAuth(t *testing.T) {
	cfg := &config.Config{
		CodexHeaderDefaults: config.CodexHeaderDefaults{
			UserAgent:    "config-ua",
			BetaFeatures: "config-beta",
		},
	}
	auth := &cliproxyauth.Auth{
		Provider:   "codex",
		Attributes: map[string]string{"api_key": "sk-test"},
	}

	headers := applyCodexWebsocketHeaders(context.Background(), http.Header{}, auth, "sk-test", cfg)

	if got := headers.Get("User-Agent"); got != "" {
		t.Fatalf("User-Agent = %s, want empty", got)
	}
	if got := headers.Get("x-codex-beta-features"); got != "" {
		t.Fatalf("x-codex-beta-features = %q, want empty", got)
	}
	if got := headers.Get("Originator"); got != "" {
		t.Fatalf("Originator = %s, want empty", got)
	}
}

func TestApplyCodexWebsocketHeadersPreservesExplicitAPIKeyUserAgent(t *testing.T) {
	auth := &cliproxyauth.Auth{Provider: "codex", Attributes: map[string]string{"api_key": "sk-test"}}
	ctx := contextWithGinHeaders(map[string]string{"User-Agent": "api-key-client/1.0", "Originator": "explicit-origin"})

	headers := applyCodexWebsocketHeaders(ctx, http.Header{}, auth, "sk-test", nil)

	if got := headers.Get("User-Agent"); got != "api-key-client/1.0" {
		t.Fatalf("User-Agent = %s, want api-key-client/1.0", got)
	}
	if got := headers.Get("Originator"); got != "explicit-origin" {
		t.Fatalf("Originator = %s, want explicit-origin", got)
	}
}

func TestApplyCodexPromptCacheHeadersSetsLowercaseSessionAndLegacyConversation(t *testing.T) {
	req := cliproxyexecutor.Request{Model: "gpt-5-codex", Payload: []byte(`{"prompt_cache_key":"cache-1"}`)}

	_, headers, _ := applyCodexPromptCacheHeaders("openai-response", req, []byte(`{"model":"gpt-5-codex"}`))

	if got := headerValueCaseInsensitive(headers, "session_id"); got != "cache-1" {
		t.Fatalf("session_id = %s, want cache-1", got)
	}
	if _, ok := headers["session_id"]; !ok {
		t.Fatalf("expected lowercase session_id key, got %#v", headers)
	}
	if got := headers.Get("Conversation_id"); got != "cache-1" {
		t.Fatalf("Conversation_id = %s, want cache-1", got)
	}
}

func TestApplyCodexWebsocketHeadersUsesCanonicalAccountHeader(t *testing.T) {
	auth := &cliproxyauth.Auth{Provider: "codex", Metadata: map[string]any{"account_id": "acct-1"}}

	headers := applyCodexWebsocketHeaders(context.Background(), http.Header{}, auth, "", nil)

	if got := headerValueCaseInsensitive(headers, "ChatGPT-Account-ID"); got != "acct-1" {
		t.Fatalf("ChatGPT-Account-ID = %s, want acct-1", got)
	}
	values, ok := headers["ChatGPT-Account-ID"]
	if !ok {
		t.Fatalf("expected exact ChatGPT-Account-ID key, got %#v", headers)
	}
	if len(values) != 1 || values[0] != "acct-1" {
		t.Fatalf("ChatGPT-Account-ID values = %#v, want [acct-1]", values)
	}
}

func TestBuildCodexResponsesWebsocketURLRequiresHTTPURL(t *testing.T) {
	if got, err := buildCodexResponsesWebsocketURL("https://example.com/backend/responses"); err != nil || got != "wss://example.com/backend/responses" {
		t.Fatalf("https URL = %q, %v; want wss URL", got, err)
	}
	if _, err := buildCodexResponsesWebsocketURL("ftp://example.com/responses"); err == nil {
		t.Fatalf("expected unsupported scheme error")
	}
	if _, err := buildCodexResponsesWebsocketURL("https:///responses"); err == nil {
		t.Fatalf("expected empty host error")
	}
}

func TestNewCodexWebsocketTransportErrorClassifiesCloseCodes(t *testing.T) {
	transportErr := errors.New("connection reset")
	wrapped := newCodexWebsocketTransportError(transportErr)
	var neutral interface{ AuthStateNeutral() bool }
	if !errors.Is(wrapped, transportErr) || !errors.As(wrapped, &neutral) || !neutral.AuthStateNeutral() {
		t.Fatalf("transport error is not auth-state-neutral or lost its cause: %v", wrapped)
	}

	tests := []struct {
		name    string
		code    int
		neutral bool
	}{
		{name: "normal closure", code: websocket.CloseNormalClosure, neutral: true},
		{name: "going away", code: websocket.CloseGoingAway, neutral: true},
		{name: "abnormal closure", code: websocket.CloseAbnormalClosure, neutral: true},
		{name: "internal server error", code: websocket.CloseInternalServerErr, neutral: true},
		{name: "service restart", code: websocket.CloseServiceRestart, neutral: true},
		{name: "try again later", code: websocket.CloseTryAgainLater, neutral: true},
		{name: "protocol error", code: websocket.CloseProtocolError},
		{name: "unsupported data", code: websocket.CloseUnsupportedData},
		{name: "invalid frame payload", code: websocket.CloseInvalidFramePayloadData},
		{name: "policy violation", code: websocket.ClosePolicyViolation},
		{name: "provider 3xxx", code: 3001},
		{name: "provider 4xxx", code: 4001},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			closeErr := &websocket.CloseError{Code: tc.code, Text: tc.name}
			classified := newCodexWebsocketTransportError(closeErr)
			var marker interface{ AuthStateNeutral() bool }
			gotNeutral := errors.As(classified, &marker) && marker.AuthStateNeutral()
			if gotNeutral != tc.neutral {
				t.Fatalf("AuthStateNeutral() = %t, want %t for close code %d", gotNeutral, tc.neutral, tc.code)
			}
			if !errors.Is(classified, closeErr) {
				t.Fatalf("classified close lost its cause: %v", classified)
			}
			if !tc.neutral && classified != closeErr {
				t.Fatalf("non-neutral close = %v, want original %v", classified, closeErr)
			}
		})
	}
}

func TestParseCodexWebsocketErrorUsesCodexAuthStateClassification(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		neutral bool
	}{
		{name: "request timeout", payload: `{"type":"error","status":408,"error":{"type":"server_error","message":"request timed out"}}`, neutral: true},
		{name: "service unavailable", payload: `{"type":"error","status":503,"error":{"type":"server_error","message":"temporarily unavailable"}}`, neutral: true},
		{name: "authentication failure", payload: `{"type":"error","status":500,"error":{"type":"authentication_error","message":"invalid or expired token"}}`},
		{name: "quota failure", payload: `{"type":"error","status":429,"error":{"type":"usage_limit_reached","message":"usage limit reached"}}`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err, ok := parseCodexWebsocketError([]byte(tc.payload))
			if !ok {
				t.Fatal("expected websocket error")
			}
			neutral, ok := err.(interface{ AuthStateNeutral() bool })
			if !ok {
				t.Fatalf("websocket status error does not expose auth-state classification: %v", err)
			}
			if got := neutral.AuthStateNeutral(); got != tc.neutral {
				t.Fatalf("AuthStateNeutral() = %t, want %t", got, tc.neutral)
			}
		})
	}
}

func TestParseCodexWebsocketErrorMarksConnectionLimitRetryable(t *testing.T) {
	err, ok := parseCodexWebsocketError([]byte(`{"type":"error","status":429,"error":{"code":"websocket_connection_limit_reached","message":"too many websockets"},"headers":{"retry-after":"1"}}`))
	if !ok {
		t.Fatalf("expected websocket error")
	}
	status, ok := err.(interface{ StatusCode() int })
	if !ok || status.StatusCode() != http.StatusTooManyRequests {
		t.Fatalf("status = %#v, want 429", err)
	}
	retryable, ok := err.(interface{ RetryAfter() *time.Duration })
	if !ok || retryable.RetryAfter() == nil {
		t.Fatalf("expected retryable websocket connection limit error")
	}
	if got := *retryable.RetryAfter(); got != 0 {
		t.Fatalf("retryAfter = %v, want connection-limit fallback 0", got)
	}
	withHeaders, ok := err.(interface{ Headers() http.Header })
	if !ok || withHeaders.Headers().Get("retry-after") != "1" {
		t.Fatalf("headers = %#v, want retry-after", err)
	}
}

func TestParseCodexWebsocketErrorUsesUsageLimitRetryMetadata(t *testing.T) {
	err, ok := parseCodexWebsocketError([]byte(`{"type":"error","status":429,"body":{"error":{"type":"usage_limit_reached","message":"usage limit reached","resets_in_seconds":7}}}`))
	if !ok {
		t.Fatalf("expected websocket error")
	}

	retryable, ok := err.(interface{ RetryAfter() *time.Duration })
	if !ok || retryable.RetryAfter() == nil {
		t.Fatalf("expected retryable usage limit websocket error")
	}
	if got := *retryable.RetryAfter(); got != 7*time.Second {
		t.Fatalf("retryAfter = %v, want 7s", got)
	}
}

func TestParseCodexWebsocketErrorPreservesWrappedBodyAndHeaders(t *testing.T) {
	err, ok := parseCodexWebsocketError([]byte(`{"type":"error","status":429,"body":{"error":{"code":"websocket_connection_limit_reached","type":"server_error","message":"too many websocket connections"}},"headers":{"x-request-id":"req-1"}}`))
	if !ok {
		t.Fatalf("expected websocket error")
	}

	parsed := gjson.Parse(err.Error())
	if got := parsed.Get("status").Int(); got != http.StatusTooManyRequests {
		t.Fatalf("wrapped status = %d, want 429; payload=%s", got, err.Error())
	}
	if got := parsed.Get("body.error.code").String(); got != "websocket_connection_limit_reached" {
		t.Fatalf("wrapped body error code = %s, want websocket_connection_limit_reached; payload=%s", got, err.Error())
	}
	if got := parsed.Get("error.code").String(); got != "websocket_connection_limit_reached" {
		t.Fatalf("surface error code = %s, want websocket_connection_limit_reached; payload=%s", got, err.Error())
	}
	retryable, ok := err.(interface{ RetryAfter() *time.Duration })
	if !ok || retryable.RetryAfter() == nil {
		t.Fatalf("expected body.error.code websocket connection limit to be retryable")
	}
	withHeaders, ok := err.(interface{ Headers() http.Header })
	if !ok || withHeaders.Headers().Get("x-request-id") != "req-1" {
		t.Fatalf("headers = %#v, want x-request-id", err)
	}
}

func TestApplyCodexHeadersUsesConfigUserAgentForOAuth(t *testing.T) {
	req, err := http.NewRequest(http.MethodPost, "https://example.com/responses", nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}
	cfg := &config.Config{
		CodexHeaderDefaults: config.CodexHeaderDefaults{
			UserAgent:    "config-ua",
			BetaFeatures: "config-beta",
		},
	}
	auth := &cliproxyauth.Auth{
		Provider: "codex",
		Metadata: map[string]any{"email": "user@example.com"},
	}
	req = req.WithContext(contextWithGinHeaders(map[string]string{
		"User-Agent": "client-ua",
	}))

	applyCodexHeaders(req, auth, "oauth-token", true, cfg)

	if got := req.Header.Get("User-Agent"); got != "config-ua" {
		t.Fatalf("User-Agent = %s, want %s", got, "config-ua")
	}
	if got := req.Header.Get("x-codex-beta-features"); got != "" {
		t.Fatalf("x-codex-beta-features = %q, want empty", got)
	}
}

func TestApplyCodexHeadersPassesThroughClientIdentityHeaders(t *testing.T) {
	req, err := http.NewRequest(http.MethodPost, "https://example.com/responses", nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}
	auth := &cliproxyauth.Auth{
		Provider: "codex",
		Metadata: map[string]any{"email": "user@example.com"},
	}
	req = req.WithContext(contextWithGinHeaders(map[string]string{
		"Originator":            "Codex Desktop",
		"Version":               "0.115.0-alpha.27",
		"X-Codex-Turn-Metadata": `{"turn_id":"turn-1"}`,
		"X-Client-Request-Id":   "019d2233-e240-7162-992d-38df0a2a0e0d",
	}))

	applyCodexHeaders(req, auth, "oauth-token", true, nil)

	if got := req.Header.Get("Originator"); got != "Codex Desktop" {
		t.Fatalf("Originator = %s, want %s", got, "Codex Desktop")
	}
	if got := req.Header.Get("Version"); got != "0.115.0-alpha.27" {
		t.Fatalf("Version = %s, want %s", got, "0.115.0-alpha.27")
	}
	if got := req.Header.Get("X-Codex-Turn-Metadata"); got != `{"turn_id":"turn-1"}` {
		t.Fatalf("X-Codex-Turn-Metadata = %s, want %s", got, `{"turn_id":"turn-1"}`)
	}
	if got := req.Header.Get("X-Client-Request-Id"); got != "019d2233-e240-7162-992d-38df0a2a0e0d" {
		t.Fatalf("X-Client-Request-Id = %s, want %s", got, "019d2233-e240-7162-992d-38df0a2a0e0d")
	}
}

func TestApplyCodexHeadersDoesNotInjectClientOnlyHeadersByDefault(t *testing.T) {
	req, err := http.NewRequest(http.MethodPost, "https://example.com/responses", nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}

	applyCodexHeaders(req, nil, "oauth-token", true, nil)

	if got := req.Header.Get("Version"); got != "" {
		t.Fatalf("Version = %q, want empty", got)
	}
	if got := req.Header.Get("X-Codex-Turn-Metadata"); got != "" {
		t.Fatalf("X-Codex-Turn-Metadata = %q, want empty", got)
	}
	if got := req.Header.Get("X-Client-Request-Id"); got != "" {
		t.Fatalf("X-Client-Request-Id = %q, want empty", got)
	}
}

func contextWithGinHeaders(headers map[string]string) context.Context {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequest(http.MethodPost, "/", nil)
	ginCtx.Request.Header = make(http.Header, len(headers))
	for key, value := range headers {
		ginCtx.Request.Header.Set(key, value)
	}
	return context.WithValue(context.Background(), "gin", ginCtx)
}

func TestNewProxyAwareWebsocketDialerDirectDisablesProxy(t *testing.T) {
	t.Parallel()

	dialer := newProxyAwareWebsocketDialer(
		&config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "http://global-proxy.example.com:8080"}},
		&cliproxyauth.Auth{ProxyURL: "direct"},
	)

	if dialer.Proxy != nil {
		t.Fatal("expected websocket proxy function to be nil for direct mode")
	}
}

func TestCodexWebsocketsExecutionSessionReusesAndIsolatesConnections(t *testing.T) {
	server, handshakes, _ := newCodexWebsocketMockServer(t, func(int32, int) []byte {
		return codexWebsocketTestCompleted
	})
	defer server.Close()

	exec := newIsolatedCodexWebsocketsExecutor()
	auth := codexWebsocketTestAuth("auth-1", server.URL)
	for _, sessionID := range []string{"session-1", "session-1", "session-2"} {
		if err := executeCodexWebsocketTestStream(context.Background(), exec, auth, sessionID); err != nil {
			t.Fatalf("ExecuteStream(%s) error = %v", sessionID, err)
		}
	}
	if got := handshakes.Load(); got != 2 {
		t.Fatalf("websocket handshakes = %d, want 2", got)
	}

	exec.CloseExecutionSession("session-1")
	exec.CloseExecutionSession("session-2")
}

func TestCodexWebsocketsExecutionSessionRedialsOnAuthOrEndpointChangeWithoutDisconnect(t *testing.T) {
	server1, handshakes1, closed1 := newCodexWebsocketMockServer(t, func(int32, int) []byte {
		return codexWebsocketTestCompleted
	})
	defer server1.Close()
	server2, handshakes2, _ := newCodexWebsocketMockServer(t, func(int32, int) []byte {
		return codexWebsocketTestCompleted
	})
	defer server2.Close()

	exec := newIsolatedCodexWebsocketsExecutor()
	sessionID := "identity-session"
	disconnectCh := exec.UpstreamDisconnectChan(sessionID)
	requests := []*cliproxyauth.Auth{
		codexWebsocketTestAuth("auth-1", server1.URL),
		codexWebsocketTestAuth("auth-2", server1.URL),
		codexWebsocketTestAuth("auth-2", server2.URL),
	}
	for i := range requests {
		if err := executeCodexWebsocketTestStream(context.Background(), exec, requests[i], sessionID); err != nil {
			t.Fatalf("request %d error = %v", i+1, err)
		}
	}
	if got := handshakes1.Load(); got != 2 {
		t.Fatalf("first endpoint handshakes = %d, want 2", got)
	}
	if got := handshakes2.Load(); got != 1 {
		t.Fatalf("second endpoint handshakes = %d, want 1", got)
	}
	for i := 0; i < 2; i++ {
		select {
		case <-closed1:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for replaced connection to close")
		}
	}
	select {
	case err, ok := <-disconnectCh:
		t.Fatalf("expected identity replacement to remain connected, got err=%v open=%t", err, ok)
	default:
	}

	exec.CloseExecutionSession(sessionID)
}

func TestCodexWebsocketsExecutionSessionRedialsOnHandshakeCredentialChange(t *testing.T) {
	tests := []struct {
		name   string
		auth   func(string) *cliproxyauth.Auth
		mutate func(*cliproxyauth.Auth)
	}{
		{
			name: "OAuth access token",
			auth: func(baseURL string) *cliproxyauth.Auth {
				return &cliproxyauth.Auth{
					ID:         "auth-1",
					Provider:   "codex",
					Attributes: map[string]string{"base_url": baseURL},
					Metadata:   map[string]any{"access_token": "oauth-token-1"},
				}
			},
			mutate: func(auth *cliproxyauth.Auth) {
				auth.Metadata["access_token"] = "oauth-token-2"
			},
		},
		{
			name: "account binding",
			auth: func(baseURL string) *cliproxyauth.Auth {
				return &cliproxyauth.Auth{
					ID:         "auth-1",
					Provider:   "codex",
					Attributes: map[string]string{"base_url": baseURL},
					Metadata: map[string]any{
						"access_token": "oauth-token",
						"account_id":   "account-1",
					},
				}
			},
			mutate: func(auth *cliproxyauth.Auth) {
				auth.Metadata["account_id"] = "account-2"
			},
		},
		{
			name: "custom credential header",
			auth: func(baseURL string) *cliproxyauth.Auth {
				auth := codexWebsocketTestAuth("auth-1", baseURL)
				auth.Attributes["header:X-Credential-Scope"] = "scope-1"
				return auth
			},
			mutate: func(auth *cliproxyauth.Auth) {
				auth.Attributes["header:X-Credential-Scope"] = "scope-2"
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server, handshakes, _ := newCodexWebsocketMockServer(t, func(int32, int) []byte {
				return codexWebsocketTestCompleted
			})
			defer server.Close()

			exec := newIsolatedCodexWebsocketsExecutor()
			sessionID := "credential-change-session"
			auth := tc.auth(server.URL)
			if err := executeCodexWebsocketTestStream(context.Background(), exec, auth, sessionID); err != nil {
				t.Fatalf("first ExecuteStream() error = %v", err)
			}
			tc.mutate(auth)
			if err := executeCodexWebsocketTestStream(context.Background(), exec, auth, sessionID); err != nil {
				t.Fatalf("second ExecuteStream() error = %v", err)
			}
			if got := handshakes.Load(); got != 2 {
				t.Fatalf("websocket handshakes = %d, want 2", got)
			}

			exec.CloseExecutionSession(sessionID)
		})
	}
}

func TestCodexWebsocketsExecutionSessionIgnoresPerTurnHandshakeHeaders(t *testing.T) {
	server, handshakes, _ := newCodexWebsocketMockServer(t, func(int32, int) []byte {
		return codexWebsocketTestCompleted
	})
	defer server.Close()

	exec := newIsolatedCodexWebsocketsExecutor()
	auth := codexWebsocketTestAuth("auth-1", server.URL)
	sessionID := "per-turn-header-session"
	firstCtx := contextWithGinHeaders(map[string]string{
		"Session_Id":            "session-1",
		"X-Client-Request-Id":   "request-1",
		"X-Codex-Turn-Metadata": `{"turn_id":"turn-1"}`,
		"X-Codex-Turn-State":    "state-1",
	})
	secondCtx := contextWithGinHeaders(map[string]string{
		"Session_Id":            "session-2",
		"X-Client-Request-Id":   "request-2",
		"X-Codex-Turn-Metadata": `{"turn_id":"turn-2"}`,
		"X-Codex-Turn-State":    "state-2",
	})
	for _, ctx := range []context.Context{firstCtx, secondCtx} {
		if err := executeCodexWebsocketTestStream(ctx, exec, auth, sessionID); err != nil {
			t.Fatalf("ExecuteStream() error = %v", err)
		}
	}
	if got := handshakes.Load(); got != 1 {
		t.Fatalf("websocket handshakes = %d, want 1", got)
	}

	exec.CloseExecutionSession(sessionID)
}

func TestCodexWebsocketsExecutionSessionRedialsOnProxyChange(t *testing.T) {
	server, handshakes, _ := newCodexWebsocketMockServer(t, func(int32, int) []byte {
		return codexWebsocketTestCompleted
	})
	defer server.Close()

	exec := newIsolatedCodexWebsocketsExecutor()
	auth := codexWebsocketTestAuth("auth-1", server.URL)
	sessionID := "proxy-change-session"
	if err := executeCodexWebsocketTestStream(context.Background(), exec, auth, sessionID); err != nil {
		t.Fatalf("first ExecuteStream() error = %v", err)
	}
	auth.ProxyURL = "direct"
	if err := executeCodexWebsocketTestStream(context.Background(), exec, auth, sessionID); err != nil {
		t.Fatalf("second ExecuteStream() error = %v", err)
	}
	if got := handshakes.Load(); got != 2 {
		t.Fatalf("websocket handshakes = %d, want 2", got)
	}

	exec.CloseExecutionSession(sessionID)
}

func TestCodexWebsocketsReadLoopNotifiesBeforeFullTerminalQueueCanBlock(t *testing.T) {
	const queueSize = 4096
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		for i := 0; i < queueSize; i++ {
			if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.output_text.delta"}`)); errWrite != nil {
				return
			}
		}
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, errDial := websocket.DefaultDialer.Dial(wsURL, nil)
	if errDial != nil {
		t.Fatalf("dial websocket: %v", errDial)
	}
	defer func() { _ = conn.Close() }()

	exec := newIsolatedCodexWebsocketsExecutor()
	sessionID := "full-terminal-queue-session"
	disconnectCh := exec.UpstreamDisconnectChan(sessionID)
	sess := exec.getOrCreateSession(sessionID)
	readCh := make(chan codexWebsocketRead, queueSize)
	sess.setActive(readCh)
	sess.connMu.Lock()
	sess.conn = conn
	sess.readerConn = conn
	sess.connMu.Unlock()
	go exec.readUpstreamLoop(sess, conn)

	select {
	case <-disconnectCh:
	case <-time.After(5 * time.Second):
		t.Fatal("upstream disconnect notification blocked behind the full read queue")
	}

	sess.clearActive(readCh)
	exec.CloseExecutionSession(sessionID)
}

func TestCodexWebsocketsExecuteCancellationRedialsWithoutDisconnect(t *testing.T) {
	firstRequest := make(chan struct{}, 1)
	server, handshakes, _ := newCodexWebsocketMockServer(t, func(connection int32, request int) []byte {
		if connection == 1 && request == 1 {
			firstRequest <- struct{}{}
			return nil
		}
		return codexWebsocketTestCompleted
	})
	defer server.Close()

	exec := newIsolatedCodexWebsocketsExecutor()
	sessionID := "execute-cancel-session"
	disconnectCh := exec.UpstreamDisconnectChan(sessionID)
	auth := codexWebsocketTestAuth("auth-1", server.URL)
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := exec.Execute(ctx, auth, codexWebsocketTestRequest(), codexWebsocketTestOptions(sessionID))
		errCh <- err
	}()

	select {
	case <-firstRequest:
		cancel()
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("timed out waiting for canceled request")
	}
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Execute() error = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled Execute() did not return")
	}

	if _, err := exec.Execute(context.Background(), auth, codexWebsocketTestRequest(), codexWebsocketTestOptions(sessionID)); err != nil {
		t.Fatalf("Execute() after cancellation error = %v", err)
	}
	if got := handshakes.Load(); got != 2 {
		t.Fatalf("websocket handshakes = %d, want 2", got)
	}
	select {
	case err, ok := <-disconnectCh:
		t.Fatalf("expected cancellation to remain connected, got err=%v open=%t", err, ok)
	default:
	}

	exec.CloseExecutionSession(sessionID)
}

func TestCodexWebsocketsExecuteStreamCancellationRedialsWithoutDisconnect(t *testing.T) {
	firstRequest := make(chan struct{}, 1)
	server, handshakes, _ := newCodexWebsocketMockServer(t, func(connection int32, request int) []byte {
		if connection == 1 && request == 1 {
			firstRequest <- struct{}{}
			return nil
		}
		return codexWebsocketTestCompleted
	})
	defer server.Close()

	exec := newIsolatedCodexWebsocketsExecutor()
	sessionID := "stream-cancel-session"
	disconnectCh := exec.UpstreamDisconnectChan(sessionID)
	auth := codexWebsocketTestAuth("auth-1", server.URL)
	ctx, cancel := context.WithCancel(context.Background())
	result, err := exec.ExecuteStream(ctx, auth, codexWebsocketTestRequest(), codexWebsocketTestOptions(sessionID))
	if err != nil {
		cancel()
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	select {
	case <-firstRequest:
		cancel()
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("timed out waiting for canceled stream request")
	}
	for range result.Chunks {
	}

	if err := executeCodexWebsocketTestStream(context.Background(), exec, auth, sessionID); err != nil {
		t.Fatalf("ExecuteStream() after cancellation error = %v", err)
	}
	if got := handshakes.Load(); got != 2 {
		t.Fatalf("websocket handshakes = %d, want 2", got)
	}
	select {
	case err, ok := <-disconnectCh:
		t.Fatalf("expected cancellation to remain connected, got err=%v open=%t", err, ok)
	default:
	}

	exec.CloseExecutionSession(sessionID)
}

func TestCodexWebsocketsExecuteCancellationWithoutSessionClosesConnection(t *testing.T) {
	firstRequest := make(chan struct{}, 1)
	server, handshakes, _ := newCodexWebsocketMockServer(t, func(connection int32, request int) []byte {
		if connection == 1 && request == 1 {
			firstRequest <- struct{}{}
		}
		return nil
	})
	defer server.Close()

	exec := newIsolatedCodexWebsocketsExecutor()
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := exec.Execute(ctx, codexWebsocketTestAuth("auth-1", server.URL), codexWebsocketTestRequest(), codexWebsocketTestOptions(""))
		errCh <- err
	}()

	select {
	case <-firstRequest:
		cancel()
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("timed out waiting for no-session request")
	}
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Execute() error = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled no-session Execute() did not return")
	}
	if got := handshakes.Load(); got != 1 {
		t.Fatalf("websocket handshakes = %d, want 1", got)
	}
}

func TestCodexWebsocketsUpstreamErrorDoesNotNotifyDisconnect(t *testing.T) {
	server, _, closed := newCodexWebsocketMockServer(t, func(int32, int) []byte {
		return []byte(`{"type":"error","status":429,"error":{"type":"rate_limit_error","message":"rate limited"}}`)
	})
	defer server.Close()

	exec := newIsolatedCodexWebsocketsExecutor()
	sessionID := "upstream-error-session"
	disconnectCh := exec.UpstreamDisconnectChan(sessionID)
	result, err := exec.ExecuteStream(context.Background(), codexWebsocketTestAuth("auth-1", server.URL), codexWebsocketTestRequest(), codexWebsocketTestOptions(sessionID))
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	if err := drainCodexWebsocketTestStream(result); err == nil {
		t.Fatal("stream error = nil, want upstream status error")
	}
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for errored connection to close")
	}
	select {
	case err, ok := <-disconnectCh:
		t.Fatalf("expected upstream status error to remain in-band, got err=%v open=%t", err, ok)
	default:
	}

	exec.CloseExecutionSession(sessionID)
}

func TestCodexWebsocketsExecuteStreamBinaryMessageReportsError(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			t.Errorf("upgrade websocket: %v", errUpgrade)
			return
		}
		defer func() { _ = conn.Close() }()
		if _, _, errRead := conn.ReadMessage(); errRead != nil {
			return
		}
		_ = conn.WriteMessage(websocket.BinaryMessage, []byte{1})
	}))
	defer server.Close()

	exec := newIsolatedCodexWebsocketsExecutor()
	result, err := exec.ExecuteStream(context.Background(), codexWebsocketTestAuth("auth-1", server.URL), codexWebsocketTestRequest(), codexWebsocketTestOptions("binary-session"))
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	if err := drainCodexWebsocketTestStream(result); err == nil || !strings.Contains(err.Error(), "unexpected binary message") {
		t.Fatalf("stream error = %v, want unexpected binary message", err)
	}
	exec.CloseExecutionSession("binary-session")
}

func TestCodexAutoExecutorRequiresBothWebsocketGates(t *testing.T) {
	var httpRequests atomic.Int32
	var wsHandshakes atomic.Int32
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if websocket.IsWebSocketUpgrade(r) {
			wsHandshakes.Add(1)
			conn, errUpgrade := upgrader.Upgrade(w, r, nil)
			if errUpgrade != nil {
				t.Errorf("upgrade websocket: %v", errUpgrade)
				return
			}
			defer func() { _ = conn.Close() }()
			if _, _, errRead := conn.ReadMessage(); errRead != nil {
				return
			}
			_ = conn.WriteMessage(websocket.TextMessage, codexWebsocketTestCompleted)
			return
		}
		httpRequests.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: " + string(codexWebsocketTestCompleted) + "\n\n"))
	}))
	defer server.Close()

	tests := []struct {
		name           string
		markDownstream bool
		websocketAuth  bool
		wantHTTP       int32
		wantWS         int32
	}{
		{name: "credential alone stays HTTP", websocketAuth: true, wantHTTP: 1},
		{name: "downstream marker alone stays HTTP", markDownstream: true, wantHTTP: 1},
		{name: "both gates select websocket", markDownstream: true, websocketAuth: true, wantWS: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			httpBefore := httpRequests.Load()
			wsBefore := wsHandshakes.Load()
			exec := NewCodexAutoExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
			exec.wsExec.store = &codexWebsocketSessionStore{sessions: make(map[string]*codexWebsocketSession)}
			auth := codexWebsocketTestAuth("auth-1", server.URL)
			if tc.websocketAuth {
				auth.Attributes["websockets"] = "true"
			}
			ctx := context.Background()
			if tc.markDownstream {
				ctx = cliproxyexecutor.WithDownstreamWebsocket(ctx)
			}
			result, err := exec.ExecuteStream(ctx, auth, codexWebsocketTestRequest(), codexWebsocketTestOptions(""))
			if err != nil {
				t.Fatalf("ExecuteStream() error = %v", err)
			}
			if err := drainCodexWebsocketTestStream(result); err != nil {
				t.Fatalf("stream error = %v", err)
			}
			if got := httpRequests.Load() - httpBefore; got != tc.wantHTTP {
				t.Fatalf("HTTP requests = %d, want %d", got, tc.wantHTTP)
			}
			if got := wsHandshakes.Load() - wsBefore; got != tc.wantWS {
				t.Fatalf("websocket handshakes = %d, want %d", got, tc.wantWS)
			}
		})
	}
}

func TestCodexWebsocketsHandshakeFallbackIsLimitedTo426(t *testing.T) {
	tests := []struct {
		name             string
		status           int
		wantHTTPRequests int32
		wantError        bool
	}{
		{name: "upgrade required falls back", status: http.StatusUpgradeRequired, wantHTTPRequests: 1},
		{name: "server error propagates", status: http.StatusInternalServerError, wantError: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var httpRequests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if websocket.IsWebSocketUpgrade(r) {
					http.Error(w, http.StatusText(tc.status), tc.status)
					return
				}
				httpRequests.Add(1)
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = w.Write([]byte("data: " + string(codexWebsocketTestCompleted) + "\n\n"))
			}))
			defer server.Close()

			exec := newIsolatedCodexWebsocketsExecutor()
			result, err := exec.ExecuteStream(context.Background(), codexWebsocketTestAuth("auth-1", server.URL), codexWebsocketTestRequest(), codexWebsocketTestOptions(""))
			if tc.wantError {
				if err == nil {
					t.Fatal("ExecuteStream() error = nil, want handshake status error")
				}
			} else {
				if err != nil {
					t.Fatalf("ExecuteStream() error = %v", err)
				}
				if err := drainCodexWebsocketTestStream(result); err != nil {
					t.Fatalf("fallback stream error = %v", err)
				}
			}
			if got := httpRequests.Load(); got != tc.wantHTTPRequests {
				t.Fatalf("HTTP requests = %d, want %d", got, tc.wantHTTPRequests)
			}
		})
	}
}

func TestCodexWebsocketsSendRetryHandshake426FallsBackToHTTP(t *testing.T) {
	tests := []struct {
		name   string
		stream bool
	}{
		{name: "non-stream"},
		{name: "stream", stream: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var websocketAttempts atomic.Int32
			var httpRequests atomic.Int32
			upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if websocket.IsWebSocketUpgrade(r) {
					if websocketAttempts.Add(1) > 1 {
						http.Error(w, http.StatusText(http.StatusUpgradeRequired), http.StatusUpgradeRequired)
						return
					}
					conn, errUpgrade := upgrader.Upgrade(w, r, nil)
					if errUpgrade != nil {
						return
					}
					defer func() { _ = conn.Close() }()
					for {
						if _, _, errRead := conn.ReadMessage(); errRead != nil {
							return
						}
					}
				}

				httpRequests.Add(1)
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = w.Write([]byte("data: " + string(codexWebsocketTestCompleted) + "\n\n"))
			}))
			defer server.Close()

			wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/responses"
			staleConn, _, errDial := websocket.DefaultDialer.Dial(wsURL, nil)
			if errDial != nil {
				t.Fatalf("dial stale websocket: %v", errDial)
			}
			defer func() { _ = staleConn.Close() }()
			if errDeadline := staleConn.SetWriteDeadline(time.Now().Add(-time.Second)); errDeadline != nil {
				t.Fatalf("set stale websocket write deadline: %v", errDeadline)
			}

			exec := newIsolatedCodexWebsocketsExecutor()
			auth := codexWebsocketTestAuth("auth-1", server.URL)
			sessionID := "send-retry-fallback-session"
			sess := exec.getOrCreateSession(sessionID)
			sess.connMu.Lock()
			sess.conn = staleConn
			sess.wsURL = wsURL
			sess.authID = auth.ID
			sess.handshakeFingerprint = codexWebsocketHandshakeFingerprint(applyCodexWebsocketHeaders(context.Background(), http.Header{}, auth, "sk-test", exec.cfg), "", "")
			sess.connMu.Unlock()

			if tc.stream {
				result, err := exec.ExecuteStream(context.Background(), auth, codexWebsocketTestRequest(), codexWebsocketTestOptions(sessionID))
				if err != nil {
					t.Fatalf("ExecuteStream() error = %v", err)
				}
				if err := drainCodexWebsocketTestStream(result); err != nil {
					t.Fatalf("fallback stream error = %v", err)
				}
			} else {
				if _, err := exec.Execute(context.Background(), auth, codexWebsocketTestRequest(), codexWebsocketTestOptions(sessionID)); err != nil {
					t.Fatalf("Execute() error = %v", err)
				}
			}
			if got := websocketAttempts.Load(); got != 2 {
				t.Fatalf("websocket attempts = %d, want 2", got)
			}
			if got := httpRequests.Load(); got != 1 {
				t.Fatalf("HTTP requests = %d, want 1", got)
			}

			exec.CloseExecutionSession(sessionID)
		})
	}
}

var codexWebsocketTestCompleted = []byte(`{"type":"response.completed","response":{"id":"resp-test","status":"completed","output":[],"usage":{"input_tokens":0,"output_tokens":0,"total_tokens":0}}}`)

func newCodexWebsocketMockServer(t *testing.T, response func(int32, int) []byte) (*httptest.Server, *atomic.Int32, <-chan int32) {
	t.Helper()
	var handshakes atomic.Int32
	closed := make(chan int32, 32)
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		connection := handshakes.Add(1)
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			t.Errorf("upgrade websocket: %v", errUpgrade)
			return
		}
		defer func() {
			_ = conn.Close()
			closed <- connection
		}()
		for request := 1; ; request++ {
			if _, _, errRead := conn.ReadMessage(); errRead != nil {
				return
			}
			payload := response(connection, request)
			if payload == nil {
				continue
			}
			if errWrite := conn.WriteMessage(websocket.TextMessage, payload); errWrite != nil {
				return
			}
		}
	}))
	return server, &handshakes, closed
}

func newIsolatedCodexWebsocketsExecutor() *CodexWebsocketsExecutor {
	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	exec.store = &codexWebsocketSessionStore{sessions: make(map[string]*codexWebsocketSession)}
	return exec
}

func codexWebsocketTestAuth(authID string, baseURL string) *cliproxyauth.Auth {
	return &cliproxyauth.Auth{
		ID:       authID,
		Provider: "codex",
		Attributes: map[string]string{
			"api_key":  "sk-test",
			"base_url": baseURL,
		},
	}
}

func codexWebsocketTestRequest() cliproxyexecutor.Request {
	return cliproxyexecutor.Request{
		Model:   "gpt-5-codex",
		Payload: []byte(`{"model":"gpt-5-codex","input":[]}`),
	}
}

func codexWebsocketTestOptions(sessionID string) cliproxyexecutor.Options {
	opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("codex")}
	if sessionID != "" {
		opts.Metadata = map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: sessionID}
	}
	return opts
}

func executeCodexWebsocketTestStream(ctx context.Context, exec *CodexWebsocketsExecutor, auth *cliproxyauth.Auth, sessionID string) error {
	result, err := exec.ExecuteStream(ctx, auth, codexWebsocketTestRequest(), codexWebsocketTestOptions(sessionID))
	if err != nil {
		return err
	}
	return drainCodexWebsocketTestStream(result)
}

func drainCodexWebsocketTestStream(result *cliproxyexecutor.StreamResult) error {
	if result == nil {
		return errors.New("stream result is nil")
	}
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			return chunk.Err
		}
	}
	return nil
}
