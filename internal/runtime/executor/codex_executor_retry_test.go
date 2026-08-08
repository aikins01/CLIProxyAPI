package executor

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
)

func TestParseCodexRetryAfter(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)

	t.Run("resets_in_seconds", func(t *testing.T) {
		body := []byte(`{"error":{"type":"usage_limit_reached","resets_in_seconds":123}}`)
		retryAfter := parseCodexRetryAfter(http.StatusTooManyRequests, body, now)
		if retryAfter == nil {
			t.Fatalf("expected retryAfter, got nil")
		}
		if *retryAfter != 123*time.Second {
			t.Fatalf("retryAfter = %v, want %v", *retryAfter, 123*time.Second)
		}
	})

	t.Run("prefers resets_at", func(t *testing.T) {
		resetAt := now.Add(5 * time.Minute).Unix()
		body := []byte(`{"error":{"type":"usage_limit_reached","resets_at":` + itoa(resetAt) + `,"resets_in_seconds":1}}`)
		retryAfter := parseCodexRetryAfter(http.StatusTooManyRequests, body, now)
		if retryAfter == nil {
			t.Fatalf("expected retryAfter, got nil")
		}
		if *retryAfter != 5*time.Minute {
			t.Fatalf("retryAfter = %v, want %v", *retryAfter, 5*time.Minute)
		}
	})

	t.Run("fallback when resets_at is past", func(t *testing.T) {
		resetAt := now.Add(-1 * time.Minute).Unix()
		body := []byte(`{"error":{"type":"usage_limit_reached","resets_at":` + itoa(resetAt) + `,"resets_in_seconds":77}}`)
		retryAfter := parseCodexRetryAfter(http.StatusTooManyRequests, body, now)
		if retryAfter == nil {
			t.Fatalf("expected retryAfter, got nil")
		}
		if *retryAfter != 77*time.Second {
			t.Fatalf("retryAfter = %v, want %v", *retryAfter, 77*time.Second)
		}
	})

	t.Run("non-429 status code", func(t *testing.T) {
		body := []byte(`{"error":{"type":"usage_limit_reached","resets_in_seconds":30}}`)
		if got := parseCodexRetryAfter(http.StatusBadRequest, body, now); got != nil {
			t.Fatalf("expected nil for non-429, got %v", *got)
		}
	})

	t.Run("non usage_limit_reached error type", func(t *testing.T) {
		body := []byte(`{"error":{"type":"server_error","resets_in_seconds":30}}`)
		if got := parseCodexRetryAfter(http.StatusTooManyRequests, body, now); got != nil {
			t.Fatalf("expected nil for non-usage_limit_reached, got %v", *got)
		}
	})
}

func TestNewCodexStatusErrTreatsCapacityAsRetryableRateLimit(t *testing.T) {
	body := []byte(`{"error":{"message":"Selected model is at capacity. Please try a different model."}}`)

	err := newCodexStatusErr(http.StatusBadRequest, body)

	if got := err.StatusCode(); got != http.StatusTooManyRequests {
		t.Fatalf("status code = %d, want %d", got, http.StatusTooManyRequests)
	}
	if err.RetryAfter() != nil {
		t.Fatalf("expected nil explicit retryAfter for capacity fallback, got %v", *err.RetryAfter())
	}
}

func TestNewCodexStatusErrClassifiesKnownCodexFailures(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		body       []byte
		wantStatus int
		wantType   string
		wantCode   string
	}{
		{
			name:       "context length status",
			statusCode: http.StatusRequestEntityTooLarge,
			body:       []byte(`{"error":{"message":"context length exceeded","type":"invalid_request_error","code":"context_length_exceeded"}}`),
			wantStatus: http.StatusRequestEntityTooLarge,
			wantType:   "invalid_request_error",
			wantCode:   "context_too_large",
		},
		{
			name:       "thinking signature",
			statusCode: http.StatusBadRequest,
			body:       []byte(`{"error":{"message":"Invalid signature in thinking block","type":"invalid_request_error","code":"invalid_request_error"}}`),
			wantStatus: http.StatusBadRequest,
			wantType:   "invalid_request_error",
			wantCode:   "thinking_signature_invalid",
		},
		{
			name:       "previous response missing",
			statusCode: http.StatusBadRequest,
			body:       []byte(`{"error":{"message":"No response found for previous_response_id resp_123","type":"invalid_request_error","code":"previous_response_not_found"}}`),
			wantStatus: http.StatusBadRequest,
			wantType:   "invalid_request_error",
			wantCode:   "previous_response_not_found",
		},
		{
			name:       "auth unavailable",
			statusCode: http.StatusUnauthorized,
			body:       []byte(`{"error":{"message":"invalid or expired token","type":"authentication_error","code":"invalid_api_key"}}`),
			wantStatus: http.StatusUnauthorized,
			wantType:   "authentication_error",
			wantCode:   "auth_unavailable",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := newCodexStatusErr(tc.statusCode, tc.body)

			if got := err.StatusCode(); got != tc.wantStatus {
				t.Fatalf("status code = %d, want %d", got, tc.wantStatus)
			}
			assertCodexErrorCode(t, err.Error(), tc.wantType, tc.wantCode)
		})
	}
}

func TestNewCodexStatusErrPreservesUnclassifiedErrors(t *testing.T) {
	body := []byte(`{"error":{"message":"documentation mentions too many tokens, but this is a billing configuration failure","type":"server_error","code":"billing_config_error"}}`)

	err := newCodexStatusErr(http.StatusBadGateway, body)

	if got := err.StatusCode(); got != http.StatusBadGateway {
		t.Fatalf("status code = %d, want %d", got, http.StatusBadGateway)
	}
	if got := err.Error(); got != string(body) {
		t.Fatalf("error body = %s, want original %s", got, string(body))
	}
}

func TestNewCodexStatusErrMarksTransientFailuresAuthStateNeutral(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		body       []byte
		want       bool
	}{
		{name: "request timeout", statusCode: http.StatusRequestTimeout, body: []byte(`{"error":{"type":"server_error"}}`), want: true},
		{name: "bad gateway", statusCode: http.StatusBadGateway, body: []byte(`{"error":{"type":"server_error"}}`), want: true},
		{name: "service unavailable", statusCode: http.StatusServiceUnavailable, body: []byte(`{"error":{"message":"upstream connect error or disconnect/reset before headers","type":"server_error"}}`), want: true},
		{name: "authentication failure", statusCode: http.StatusInternalServerError, body: []byte(`{"error":{"message":"invalid or expired token","type":"authentication_error"}}`), want: false},
		{name: "rate limit", statusCode: http.StatusTooManyRequests, body: []byte(`{"error":{"type":"usage_limit_reached"}}`), want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := newCodexStatusErr(tc.statusCode, tc.body)
			if got := err.AuthStateNeutral(); got != tc.want {
				t.Fatalf("AuthStateNeutral() = %t, want %t", got, tc.want)
			}
		})
	}
}

func TestCodexExecutorTransportFailureIsAuthStateNeutral(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	baseURL := server.URL
	server.Close()

	executor := NewCodexExecutor(new(config.Config))
	auth := new(cliproxyauth.Auth)
	auth.Attributes = map[string]string{"api_key": "test", "base_url": baseURL}
	_, err := executor.Execute(t.Context(), auth, cliproxyexecutor.Request{
		Model:   "gpt-5.6-sol",
		Payload: []byte(`{"model":"gpt-5.6-sol","input":"test"}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("openai-response")})
	if err == nil {
		t.Fatal("Execute() error = nil, want transport failure")
	}
	neutral, ok := err.(interface{ AuthStateNeutral() bool })
	if !ok || !neutral.AuthStateNeutral() {
		t.Fatalf("transport error is not auth-state-neutral: %v", err)
	}
}

func TestCodexExecutorIncompleteStreamsAreAuthStateNeutral(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "empty"},
		{name: "partial", body: "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\"}}\n\n"},
		{name: "truncated completed JSON", body: "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\""},
		{name: "completed event without delimiter", body: "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\"}}\n"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				if tc.body != "" {
					_, _ = w.Write([]byte(tc.body))
				}
			}))
			defer server.Close()

			executor := NewCodexExecutor(new(config.Config))
			auth := new(cliproxyauth.Auth)
			auth.Attributes = map[string]string{"api_key": "test", "base_url": server.URL}
			result, err := executor.ExecuteStream(t.Context(), auth, cliproxyexecutor.Request{
				Model:   "gpt-5.6-sol",
				Payload: []byte(`{"model":"gpt-5.6-sol","input":"test"}`),
			}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("openai-response")})
			if err != nil {
				t.Fatalf("ExecuteStream() error: %v", err)
			}

			var streamErr error
			for chunk := range result.Chunks {
				if chunk.Err != nil {
					streamErr = chunk.Err
				}
			}
			if streamErr == nil {
				t.Fatal("stream error = nil, want incomplete-stream failure")
			}
			neutral, ok := streamErr.(interface{ AuthStateNeutral() bool })
			if !ok || !neutral.AuthStateNeutral() {
				t.Fatalf("incomplete stream error is not auth-state-neutral: %v", streamErr)
			}
			statusErr, ok := streamErr.(interface{ StatusCode() int })
			if !ok || statusErr.StatusCode() != http.StatusRequestTimeout {
				t.Fatalf("incomplete stream status = %v, want %d", streamErr, http.StatusRequestTimeout)
			}
		})
	}
}

func TestCodexExecutorTruncatedCompletedEventIsAuthStateNeutral(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\""))
	}))
	defer server.Close()

	executor := NewCodexExecutor(new(config.Config))
	auth := new(cliproxyauth.Auth)
	auth.Attributes = map[string]string{"api_key": "test", "base_url": server.URL}
	_, err := executor.Execute(t.Context(), auth, cliproxyexecutor.Request{
		Model:   "gpt-5.6-sol",
		Payload: []byte(`{"model":"gpt-5.6-sol","input":"test"}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("openai-response")})
	if err == nil {
		t.Fatal("Execute() error = nil, want incomplete-stream failure")
	}
	neutral, ok := err.(interface{ AuthStateNeutral() bool })
	if !ok || !neutral.AuthStateNeutral() {
		t.Fatalf("incomplete stream error is not auth-state-neutral: %v", err)
	}
	statusErr, ok := err.(interface{ StatusCode() int })
	if !ok || statusErr.StatusCode() != http.StatusRequestTimeout {
		t.Fatalf("incomplete stream status = %v, want %d", err, http.StatusRequestTimeout)
	}
}

func assertCodexErrorCode(t *testing.T, raw string, wantType string, wantCode string) {
	t.Helper()

	var payload struct {
		Error struct {
			Type string `json:"type"`
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		t.Fatalf("error body is not valid JSON: %v; body=%s", err, raw)
	}
	if payload.Error.Type != wantType {
		t.Fatalf("error.type = %q, want %q; body=%s", payload.Error.Type, wantType, raw)
	}
	if payload.Error.Code != wantCode {
		t.Fatalf("error.code = %q, want %q; body=%s", payload.Error.Code, wantCode, raw)
	}
}

func itoa(v int64) string {
	return strconv.FormatInt(v, 10)
}
