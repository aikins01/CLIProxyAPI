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
	calls           int
	delay           time.Duration
	features        []string
	streamDelay     time.Duration
	streamPayload   []byte
	streamPayloads  [][]byte
	streamKeepAlive time.Duration
	streamCommitted bool
	streamAlts      []string
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

func (e *geminiKeepAliveExecutor) ExecuteStream(ctx context.Context, _ *coreauth.Auth, _ coreexecutor.Request, opts coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	e.streamAlts = append(e.streamAlts, opts.Alt)
	chunks := make(chan coreexecutor.StreamChunk)
	go func() {
		defer close(chunks)
		select {
		case <-time.After(e.streamDelay):
		case <-ctx.Done():
			return
		}
		payloads := e.streamPayloads
		if len(payloads) == 0 {
			payloads = [][]byte{e.streamPayload}
		}
		for _, payload := range payloads {
			select {
			case chunks <- coreexecutor.StreamChunk{Payload: payload}:
			case <-ctx.Done():
				return
			}
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

func TestGeminiCommittedSSEUsesProviderKeepAlive(t *testing.T) {
	gin.SetMode(gin.TestMode)
	executor := &geminiKeepAliveExecutor{
		streamDelay:     40 * time.Millisecond,
		streamPayload:   []byte(`{"candidates":[{"content":{"parts":[{"text":"ok"}]}}]}`),
		streamKeepAlive: 5 * time.Millisecond,
		streamCommitted: true,
	}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	auth := &coreauth.Auth{ID: "auth-gemini-stream", Provider: executor.Identifier(), Status: coreauth.StatusActive}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("Register auth: %v", err)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "gemini-stream-model"}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	router := gin.New()
	router.POST("/v1beta/models/*action", NewGeminiAPIHandler(base).GeminiHandler)
	request := httptest.NewRequest(http.MethodPost, "/v1beta/models/gemini-stream-model:streamGenerateContent", strings.NewReader(`{"contents":[{"role":"user","parts":[{"text":"hello"}]}]}`))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	body := recorder.Body.String()
	heartbeatIndex := strings.Index(body, ": keep-alive\n\n")
	dataIndex := strings.Index(body, "data:")
	if heartbeatIndex < 0 || dataIndex < 0 || heartbeatIndex > dataIndex {
		t.Fatalf("expected heartbeat before first Gemini event, got %q", body)
	}
}

func TestGeminiRawAltDisablesSSEKeepAlive(t *testing.T) {
	gin.SetMode(gin.TestMode)
	payload := []byte(`{"candidates":[{"content":{"parts":[{"text":"ok"}]}}]}`)
	executor := &geminiKeepAliveExecutor{
		streamDelay:     20 * time.Millisecond,
		streamPayload:   payload,
		streamKeepAlive: 5 * time.Millisecond,
		streamCommitted: true,
	}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	auth := &coreauth.Auth{ID: "auth-gemini-raw", Provider: executor.Identifier(), Status: coreauth.StatusActive}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("Register auth: %v", err)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "gemini-raw-model"}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	router := gin.New()
	router.POST("/v1beta/models/*action", NewGeminiAPIHandler(base).GeminiHandler)
	request := httptest.NewRequest(http.MethodPost, "/v1beta/models/gemini-raw-model:streamGenerateContent?alt=json", strings.NewReader(`{"contents":[{"role":"user","parts":[{"text":"hello"}]}]}`))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	if body := recorder.Body.String(); body != string(payload) {
		t.Fatalf("raw body = %q, want %q", body, payload)
	}
	if strings.HasPrefix(recorder.Header().Get("Content-Type"), "text/event-stream") {
		t.Fatalf("raw alt unexpectedly used SSE Content-Type: %q", recorder.Header().Get("Content-Type"))
	}
	if len(executor.streamAlts) != 1 || executor.streamAlts[0] != "json" {
		t.Fatalf("stream alts = %#v, want [json]", executor.streamAlts)
	}
}

func TestGeminiCLIStreamingFramingAndKeepAlive(t *testing.T) {
	tests := []struct {
		name          string
		query         string
		wantHeartbeat bool
		wantBody      string
		wantAlt       string
	}{
		{name: "sse", wantHeartbeat: true},
		{name: "raw alt", query: "?alt=json", wantBody: `{"candidates":[]}`, wantAlt: "json"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			executor := &geminiKeepAliveExecutor{
				streamDelay:     40 * time.Millisecond,
				streamPayloads:  [][]byte{[]byte(`{"candi`), []byte(`dates":[]}`)},
				streamKeepAlive: 5 * time.Millisecond,
				streamCommitted: true,
			}
			manager := coreauth.NewManager(nil, nil, nil)
			manager.RegisterExecutor(executor)
			auth := &coreauth.Auth{ID: "auth-gemini-cli-" + tt.name, Provider: executor.Identifier(), Status: coreauth.StatusActive}
			if _, err := manager.Register(context.Background(), auth); err != nil {
				t.Fatalf("Register auth: %v", err)
			}
			registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "gemini-cli-stream-model"}})
			t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })

			cfg := &sdkconfig.SDKConfig{EnableGeminiCLIEndpoint: true}
			router := gin.New()
			router.POST("/v1internal:streamGenerateContent", NewGeminiCLIAPIHandler(handlers.NewBaseAPIHandlers(cfg, manager)).CLIHandler)
			request := httptest.NewRequest(http.MethodPost, "/v1internal:streamGenerateContent"+tt.query, strings.NewReader(`{"model":"gemini-cli-stream-model"}`))
			request.Host = "127.0.0.1"
			request.RemoteAddr = "127.0.0.1:12345"
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, request)

			body := recorder.Body.String()
			if tt.wantHeartbeat {
				heartbeatIndex := strings.Index(body, ": keep-alive\n\n")
				dataIndex := strings.Index(body, "data:")
				if heartbeatIndex < 0 || dataIndex < 0 || heartbeatIndex > dataIndex {
					t.Fatalf("expected heartbeat before first Gemini CLI event, got %q", body)
				}
				if strings.Count(body, "data:") != 1 || !strings.Contains(body, "data: {\"candidates\":[]}\n\n") {
					t.Fatalf("expected one complete Gemini CLI event, got %q", body)
				}
			} else if body != tt.wantBody {
				t.Fatalf("raw body = %q, want %q", body, tt.wantBody)
			}
			if strings.Contains(body, ": keep-alive") != tt.wantHeartbeat {
				t.Fatalf("heartbeat presence in %q = %v, want %v", body, strings.Contains(body, ": keep-alive"), tt.wantHeartbeat)
			}
			if len(executor.streamAlts) != 1 || executor.streamAlts[0] != tt.wantAlt {
				t.Fatalf("stream alts = %#v, want [%s]", executor.streamAlts, tt.wantAlt)
			}
		})
	}
}
