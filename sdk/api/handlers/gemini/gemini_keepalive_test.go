package gemini

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

type geminiKeepAliveExecutor struct {
	calls    int
	delay    time.Duration
	features []string
}

func (e *geminiKeepAliveExecutor) Identifier() string { return "test-gemini-provider" }

func (e *geminiKeepAliveExecutor) Execute(ctx context.Context, auth *coreauth.Auth, req coreexecutor.Request, opts coreexecutor.Options) (coreexecutor.Response, error) {
	e.calls++
	if opts.Headers != nil {
		e.features = append(e.features, opts.Headers.Get("X-Amp-Feature"))
	} else {
		e.features = append(e.features, "")
	}
	if e.delay > 0 {
		select {
		case <-time.After(e.delay):
		case <-ctx.Done():
			return coreexecutor.Response{}, ctx.Err()
		}
	}
	return coreexecutor.Response{Payload: []byte(`{"ok":true}`)}, nil
}

func (e *geminiKeepAliveExecutor) ExecuteStream(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	return nil, errors.New("not implemented")
}

func (e *geminiKeepAliveExecutor) Refresh(ctx context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (e *geminiKeepAliveExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (e *geminiKeepAliveExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func TestGeminiGenerateContentNonStreamingSkipsKeepAliveForAmpReview(t *testing.T) {
	gin.SetMode(gin.TestMode)
	executor := &geminiKeepAliveExecutor{delay: 1200 * time.Millisecond}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)

	auth := &coreauth.Auth{ID: "auth-gemini-amp-review-json", Provider: executor.Identifier(), Status: coreauth.StatusActive}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("Register auth: %v", err)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "gemini-amp-review-model"}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	})

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{NonStreamKeepAliveInterval: 1}, manager)
	h := NewGeminiAPIHandler(base)
	router := gin.New()
	router.POST("/v1beta/models/*action", h.GeminiHandler)

	req := httptest.NewRequest(http.MethodPost, "/v1beta/models/gemini-amp-review-model:generateContent", strings.NewReader(`{"contents":[{"role":"user","parts":[{"text":"review"}]}]}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Amp-Feature", "amp.review")
	req.Header.Set("X-Amp-Client-Application", "CLI")
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.Code, http.StatusOK)
	}
	if executor.calls != 1 {
		t.Fatalf("executor calls = %d, want 1", executor.calls)
	}
	if len(executor.features) != 1 || executor.features[0] != "amp.review" {
		t.Fatalf("executor features = %#v, want [amp.review]", executor.features)
	}
	if body := resp.Body.String(); body != `{"ok":true}` {
		t.Fatalf("body = %q, want strict JSON without keepalive bytes", body)
	}
}

func TestGeminiGenerateContentPreservesAmpPainterFeatureHeader(t *testing.T) {
	gin.SetMode(gin.TestMode)
	executor := &geminiKeepAliveExecutor{}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)

	auth := &coreauth.Auth{ID: "auth-gemini-amp-painter", Provider: executor.Identifier(), Status: coreauth.StatusActive}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("Register auth: %v", err)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "gemini-3-pro-image"}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	})

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	h := NewGeminiAPIHandler(base)
	router := gin.New()
	router.POST("/v1beta/models/*action", h.GeminiHandler)

	req := httptest.NewRequest(http.MethodPost, "/v1beta/models/gemini-3-pro-image:generateContent", strings.NewReader(`{"contents":[{"role":"user","parts":[{"text":"paint"}]}]}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Amp-Feature", "amp.painter")
	req.Header.Set("X-Amp-Client-Application", "CLI")
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.Code, http.StatusOK)
	}
	if len(executor.features) != 1 || executor.features[0] != "amp.painter" {
		t.Fatalf("executor features = %#v, want [amp.painter]", executor.features)
	}
}
