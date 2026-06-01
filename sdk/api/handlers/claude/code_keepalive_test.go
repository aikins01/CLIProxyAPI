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
	calls int
	delay time.Duration
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

func (e *claudeKeepAliveExecutor) ExecuteStream(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	return nil, errors.New("not implemented")
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
