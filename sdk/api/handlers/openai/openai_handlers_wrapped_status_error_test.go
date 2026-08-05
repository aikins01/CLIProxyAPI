package openai

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v7/sdk/config"
	"github.com/tidwall/gjson"
)

type wrappedStatusError struct {
	status  int
	headers http.Header
}

func (e *wrappedStatusError) Error() string {
	return "chatgpt-web: ChatGPT Turnstile challenge required"
}

func (e *wrappedStatusError) StatusCode() int { return e.status }

func (e *wrappedStatusError) Headers() http.Header { return e.headers }

type wrappedStatusExecutor struct{}

func (e *wrappedStatusExecutor) Identifier() string { return "chatgpt-web" }

func (e *wrappedStatusExecutor) Execute(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, fmt.Errorf("chatgpt-web: fetch requirements: %w", &wrappedStatusError{
		status:  http.StatusForbidden,
		headers: http.Header{"Retry-After": {"30"}},
	})
}

func (e *wrappedStatusExecutor) ExecuteStream(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	return nil, fmt.Errorf("chatgpt-web: fetch requirements: %w", &wrappedStatusError{
		status:  http.StatusForbidden,
		headers: http.Header{"Retry-After": {"30"}},
	})
}

func (e *wrappedStatusExecutor) Refresh(ctx context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (e *wrappedStatusExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, &coreauth.Error{Code: "not_implemented", Message: "CountTokens not implemented"}
}

func (e *wrappedStatusExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, &coreauth.Error{Code: "not_implemented", Message: "HttpRequest not implemented", HTTPStatus: http.StatusNotImplemented}
}

type committedStreamExecutor struct {
	payload   []byte
	payloads  [][]byte
	immediate bool
}

type retryCommittedStreamExecutor struct {
	mu      sync.Mutex
	calls   int
	payload []byte
}

func (e *committedStreamExecutor) Identifier() string { return "committed-stream" }

func (e *committedStreamExecutor) Execute(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (e *committedStreamExecutor) ExecuteStream(ctx context.Context, _ *coreauth.Auth, _ coreexecutor.Request, _ coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	payloads := e.payloads
	if len(payloads) == 0 {
		payloads = [][]byte{e.payload}
	}
	immediate := e.immediate
	chunks := make(chan coreexecutor.StreamChunk)
	go func() {
		defer close(chunks)
		if !immediate {
			select {
			case <-time.After(40 * time.Millisecond):
			case <-ctx.Done():
				return
			}
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
	if !immediate {
		result.SetKeepAliveInterval(5 * time.Millisecond)
		result.SetBootstrapCommitted()
	}
	return result, nil
}

func (e *committedStreamExecutor) Refresh(ctx context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (e *committedStreamExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (e *committedStreamExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func (e *retryCommittedStreamExecutor) Identifier() string { return "retry-committed-stream" }

func (e *retryCommittedStreamExecutor) Execute(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (e *retryCommittedStreamExecutor) ExecuteStream(ctx context.Context, _ *coreauth.Auth, _ coreexecutor.Request, _ coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	e.mu.Lock()
	e.calls++
	call := e.calls
	e.mu.Unlock()

	if call == 1 {
		chunks := make(chan coreexecutor.StreamChunk, 2)
		chunks <- coreexecutor.StreamChunk{Payload: []byte("data: {\"type\":\"stale")}
		chunks <- coreexecutor.StreamChunk{Err: &coreauth.Error{Code: "upstream_closed", Message: "upstream closed", HTTPStatus: http.StatusBadGateway}}
		close(chunks)
		return &coreexecutor.StreamResult{Headers: http.Header{"X-Upstream-Attempt": {"1"}}, Chunks: chunks}, nil
	}

	chunks := make(chan coreexecutor.StreamChunk)
	go func() {
		defer close(chunks)
		select {
		case <-time.After(40 * time.Millisecond):
			chunks <- coreexecutor.StreamChunk{Payload: e.payload}
		case <-ctx.Done():
		}
	}()
	result := &coreexecutor.StreamResult{Headers: http.Header{"X-Upstream-Attempt": {"2"}}, Chunks: chunks}
	result.SetKeepAliveInterval(5 * time.Millisecond)
	result.SetBootstrapCommitted()
	return result, nil
}

func (e *retryCommittedStreamExecutor) Refresh(ctx context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (e *retryCommittedStreamExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (e *retryCommittedStreamExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func (e *retryCommittedStreamExecutor) Calls() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.calls
}

func newWrappedStatusHandler(t *testing.T) *OpenAIAPIHandler {
	t.Helper()
	gin.SetMode(gin.TestMode)

	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(&wrappedStatusExecutor{})

	auth := &coreauth.Auth{
		ID:       "wrapped-status-auth",
		Provider: "chatgpt-web",
		Status:   coreauth.StatusActive,
	}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("manager.Register: %v", err)
	}

	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "wrapped-status-model"}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	})

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{PassthroughHeaders: true}, manager)
	return NewOpenAIAPIHandler(base)
}

func TestChatCompletionsWrappedStatusErrorMapsToHTTPStatus(t *testing.T) {
	h := newWrappedStatusHandler(t)

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"wrapped-status-model","messages":[{"role":"user","content":"hi"}]}`))

	h.ChatCompletions(c)

	if recorder.Code != http.StatusForbidden {
		t.Fatalf("expected HTTP 403 for wrapped status error, got %d (body: %s)", recorder.Code, recorder.Body.String())
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "ChatGPT Turnstile challenge required") {
		t.Fatalf("expected error message preserved through wrapping, got: %s", body)
	}
	if strings.Contains(body, "internal_server_error") {
		t.Fatalf("expected non-500 error mapping, got: %s", body)
	}
	if got := recorder.Header().Get("Retry-After"); got != "30" {
		t.Fatalf("Retry-After = %q, want %q", got, "30")
	}
}

func TestChatCompletionsStreamWrappedStatusErrorMapsToHTTPStatus(t *testing.T) {
	h := newWrappedStatusHandler(t)

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"wrapped-status-model","stream":true,"messages":[{"role":"user","content":"hi"}]}`))

	h.ChatCompletions(c)

	if recorder.Code != http.StatusForbidden {
		t.Fatalf("expected HTTP 403 for wrapped status error, got %d (body: %s)", recorder.Code, recorder.Body.String())
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "ChatGPT Turnstile challenge required") {
		t.Fatalf("expected error message preserved through wrapping, got: %s", body)
	}
	if strings.Contains(body, "internal_server_error") {
		t.Fatalf("expected non-500 error mapping, got: %s", body)
	}
	if got := recorder.Header().Get("Retry-After"); got != "30" {
		t.Fatalf("Retry-After = %q, want %q", got, "30")
	}
}

func TestCommittedOpenAIStreamsHeartbeatBeforeFirstPayload(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		body    string
		payload string
		handler func(*handlers.BaseAPIHandler) gin.HandlerFunc
	}{
		{
			name:    "chat completions",
			path:    "/v1/chat/completions",
			body:    `{"model":"committed-stream-model","stream":true,"messages":[{"role":"user","content":"hi"}]}`,
			payload: `{"id":"chatcmpl-1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"ok"}}]}`,
			handler: func(base *handlers.BaseAPIHandler) gin.HandlerFunc { return NewOpenAIAPIHandler(base).ChatCompletions },
		},
		{
			name:    "legacy completions",
			path:    "/v1/completions",
			body:    `{"model":"committed-stream-model","stream":true,"prompt":"hi"}`,
			payload: `{"id":"chatcmpl-1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"ok"}}]}`,
			handler: func(base *handlers.BaseAPIHandler) gin.HandlerFunc { return NewOpenAIAPIHandler(base).Completions },
		},
		{
			name:    "responses",
			path:    "/v1/responses",
			body:    `{"model":"committed-stream-model","stream":true,"input":"hi"}`,
			payload: "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp-1\"}}\n\n",
			handler: func(base *handlers.BaseAPIHandler) gin.HandlerFunc {
				return NewOpenAIResponsesAPIHandler(base).Responses
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			executor := &committedStreamExecutor{payload: []byte(tt.payload)}
			manager := coreauth.NewManager(nil, nil, nil)
			manager.RegisterExecutor(executor)
			auth := &coreauth.Auth{ID: "committed-stream-auth", Provider: executor.Identifier(), Status: coreauth.StatusActive}
			if _, err := manager.Register(context.Background(), auth); err != nil {
				t.Fatalf("manager.Register: %v", err)
			}
			registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "committed-stream-model"}})
			t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })

			router := gin.New()
			router.POST(tt.path, tt.handler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)))
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPost, tt.path, strings.NewReader(tt.body))
			request.Header.Set("Content-Type", "application/json")
			router.ServeHTTP(recorder, request)

			if recorder.Code != http.StatusOK {
				t.Fatalf("status = %d, want %d (body: %s)", recorder.Code, http.StatusOK, recorder.Body.String())
			}
			body := recorder.Body.String()
			heartbeatIndex := strings.Index(body, ": keep-alive\n\n")
			dataIndex := strings.Index(body, "data:")
			if heartbeatIndex < 0 || dataIndex < 0 || heartbeatIndex > dataIndex {
				t.Fatalf("expected heartbeat before first data event, got %q", body)
			}
			if got := recorder.Header().Get("Content-Type"); !strings.HasPrefix(got, "text/event-stream") {
				t.Fatalf("Content-Type = %q, want text/event-stream", got)
			}
		})
	}
}

func TestOpenAIStreamingEndpointsReassembleEveryByteSplit(t *testing.T) {
	payload := []byte(`{"id":"chatcmpl-1","object":"chat.completion.chunk","created":1770000000,"model":"split-model","choices":[{"index":0,"delta":{"content":"hel\"{}🙂lo"}}]}`)
	tests := []struct {
		name        string
		path        string
		requestBody string
		handler     func(*handlers.BaseAPIHandler) gin.HandlerFunc
		assertBody  func(*testing.T, string)
	}{
		{
			name:        "chat completions",
			path:        "/v1/chat/completions",
			requestBody: `{"model":"split-model","stream":true,"messages":[{"role":"user","content":"hi"}]}`,
			handler:     func(base *handlers.BaseAPIHandler) gin.HandlerFunc { return NewOpenAIAPIHandler(base).ChatCompletions },
			assertBody: func(t *testing.T, body string) {
				t.Helper()
				wantEvent := "data: " + string(payload) + "\n\n"
				if !strings.Contains(body, wantEvent) || strings.Count(body, "data: ") != 2 {
					t.Fatalf("chat stream = %q, want one payload event and DONE", body)
				}
			},
		},
		{
			name:        "legacy completions",
			path:        "/v1/completions",
			requestBody: `{"model":"split-model","stream":true,"prompt":"hi"}`,
			handler:     func(base *handlers.BaseAPIHandler) gin.HandlerFunc { return NewOpenAIAPIHandler(base).Completions },
			assertBody: func(t *testing.T, body string) {
				t.Helper()
				var payloadEvents []string
				for _, event := range strings.Split(body, "\n\n") {
					data := strings.TrimPrefix(event, "data: ")
					if data != event && data != "[DONE]" && strings.TrimSpace(data) != "" {
						payloadEvents = append(payloadEvents, data)
					}
				}
				if len(payloadEvents) != 1 {
					t.Fatalf("completion payload events = %d, want 1: %q", len(payloadEvents), body)
				}
				if got := gjson.Get(payloadEvents[0], "choices.0.text").String(); got != `hel"{}🙂lo` {
					t.Fatalf("completion text = %q, want %q: %q", got, `hel"{}🙂lo`, body)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			executor := &committedStreamExecutor{immediate: true}
			manager := coreauth.NewManager(nil, nil, nil)
			manager.RegisterExecutor(executor)
			auth := &coreauth.Auth{ID: "split-" + tt.name, Provider: executor.Identifier(), Status: coreauth.StatusActive}
			if _, err := manager.Register(t.Context(), auth); err != nil {
				t.Fatalf("manager.Register: %v", err)
			}
			registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "split-model"}})
			t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })
			router := gin.New()
			router.POST(tt.path, tt.handler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)))

			for split := 1; split < len(payload); split++ {
				executor.payloads = [][]byte{payload[:split], payload[split:]}
				recorder := httptest.NewRecorder()
				request := httptest.NewRequest(http.MethodPost, tt.path, strings.NewReader(tt.requestBody))
				request.Header.Set("Content-Type", "application/json")
				router.ServeHTTP(recorder, request)
				if recorder.Code != http.StatusOK {
					t.Fatalf("split %d status = %d, want %d: %s", split, recorder.Code, http.StatusOK, recorder.Body.String())
				}
				tt.assertBody(t, recorder.Body.String())
			}
		})
	}
}

func TestRetryCommittedResponsesStreamPublishesHeadersAndHeartbeatBeforePayload(t *testing.T) {
	gin.SetMode(gin.TestMode)
	executor := &retryCommittedStreamExecutor{payload: []byte("event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp-1\"}}\n\n")}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)

	for _, authID := range []string{"retry-committed-auth-1", "retry-committed-auth-2"} {
		auth := &coreauth.Auth{ID: authID, Provider: executor.Identifier(), Status: coreauth.StatusActive}
		if _, err := manager.Register(context.Background(), auth); err != nil {
			t.Fatalf("manager.Register(%s): %v", authID, err)
		}
		registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "retry-committed-model"}})
		t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })
	}

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{
		PassthroughHeaders: true,
		Streaming: sdkconfig.StreamingConfig{
			BootstrapRetries: 1,
		},
	}, manager)
	router := gin.New()
	router.POST("/v1/responses", NewOpenAIResponsesAPIHandler(base).Responses)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"retry-committed-model","stream":true,"input":"hi"}`))
	request.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d (body: %s)", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	if got := recorder.Header().Get("X-Upstream-Attempt"); got != "2" {
		t.Fatalf("X-Upstream-Attempt = %q, want 2", got)
	}
	body := recorder.Body.String()
	heartbeatIndex := strings.Index(body, ": keep-alive\n\n")
	dataIndex := strings.Index(body, "data:")
	if heartbeatIndex < 0 || dataIndex < 0 || heartbeatIndex > dataIndex {
		t.Fatalf("expected retry heartbeat before first data event, got %q", body)
	}
	if calls := executor.Calls(); calls != 2 {
		t.Fatalf("stream attempts = %d, want 2", calls)
	}
}
