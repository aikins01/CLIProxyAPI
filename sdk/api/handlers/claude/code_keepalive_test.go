package claude

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v7/sdk/config"
)

type claudeKeepAliveExecutor struct {
	calls           int
	delay           time.Duration
	streamDelay     time.Duration
	streamPayload   []byte
	streamKeepAlive time.Duration
	streamCommitted bool
}

func (e *claudeKeepAliveExecutor) Identifier() string { return "test-claude-provider" }

func (e *claudeKeepAliveExecutor) Execute(ctx context.Context, auth *coreauth.Auth, req coreexecutor.Request, opts coreexecutor.Options) (coreexecutor.Response, error) {
	e.calls++
	if e.delay > 0 {
		select {
		case <-time.After(e.delay):
		case <-ctx.Done():
			return coreexecutor.Response{}, ctx.Err()
		}
	}
	return coreexecutor.Response{Payload: []byte(`{"ok":true}`)}, nil
}

func (e *claudeKeepAliveExecutor) ExecuteStream(ctx context.Context, _ *coreauth.Auth, _ coreexecutor.Request, _ coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	chunks := make(chan coreexecutor.StreamChunk)
	go func() {
		defer close(chunks)
		select {
		case <-time.After(e.streamDelay):
			chunks <- coreexecutor.StreamChunk{Payload: e.streamPayload}
		case <-ctx.Done():
		}
	}()
	result := &coreexecutor.StreamResult{Chunks: chunks}
	if e.streamKeepAlive != 0 {
		result.SetKeepAliveInterval(e.streamKeepAlive)
	}
	if e.streamCommitted {
		result.SetBootstrapCommitted()
	}
	return result, nil
}

func (e *claudeKeepAliveExecutor) Refresh(ctx context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (e *claudeKeepAliveExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (e *claudeKeepAliveExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func TestClaudeMessagesNonStreamingSkipsKeepAliveForAmpJSONClient(t *testing.T) {
	gin.SetMode(gin.TestMode)
	executor := &claudeKeepAliveExecutor{delay: 1200 * time.Millisecond}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)

	auth := &coreauth.Auth{ID: "auth-claude-amp-json", Provider: executor.Identifier(), Status: coreauth.StatusActive}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("Register auth: %v", err)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "claude-amp-json-model"}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	})

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{NonStreamKeepAliveInterval: 1}, manager)
	h := NewClaudeCodeAPIHandler(base)
	router := gin.New()
	router.POST("/v1/messages", h.ClaudeMessages)

	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"claude-amp-json-model","messages":[{"role":"user","content":"hello"}]}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Amp-Feature", "amp.chat")
	req.Header.Set("X-Amp-Client-Application", "CLI")
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.Code, http.StatusOK)
	}
	if executor.calls != 1 {
		t.Fatalf("executor calls = %d, want 1", executor.calls)
	}
	if body := resp.Body.String(); body != `{"ok":true}` {
		t.Fatalf("body = %q, want strict JSON without keepalive bytes", body)
	}
}

func TestClaudeCommittedStreamUsesProviderKeepAlive(t *testing.T) {
	gin.SetMode(gin.TestMode)
	executor := &claudeKeepAliveExecutor{
		streamDelay:     40 * time.Millisecond,
		streamPayload:   []byte("event: content_block_delta\ndata: {\"type\":\"content_block_delta\"}\n\n"),
		streamKeepAlive: 5 * time.Millisecond,
		streamCommitted: true,
	}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	auth := &coreauth.Auth{ID: "auth-claude-stream", Provider: executor.Identifier(), Status: coreauth.StatusActive}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("Register auth: %v", err)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "claude-stream-model"}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	router := gin.New()
	router.POST("/v1/messages", NewClaudeCodeAPIHandler(base).ClaudeMessages)
	request := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"claude-stream-model","stream":true,"messages":[{"role":"user","content":"hello"}]}`))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	body := recorder.Body.String()
	heartbeatIndex := strings.Index(body, ": keep-alive\n\n")
	dataIndex := strings.Index(body, "data:")
	if heartbeatIndex < 0 || dataIndex < 0 || heartbeatIndex > dataIndex {
		t.Fatalf("expected heartbeat before first Claude event, got %q", body)
	}
}
