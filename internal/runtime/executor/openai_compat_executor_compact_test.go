package executor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestOpenAICompatExecutorCompactPassthrough(t *testing.T) {
	var gotPath string
	var gotBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		body, _ := io.ReadAll(r.Body)
		gotBody = body
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"resp_1","object":"response.compaction","usage":{"input_tokens":1,"output_tokens":2,"total_tokens":3}}`))
	}))
	defer server.Close()

	executor := NewOpenAICompatExecutor("openai-compatibility", &config.Config{})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{
		"base_url": server.URL + "/v1",
		"api_key":  "test",
	}}
	payload := []byte(`{"model":"gpt-5.1-codex-max","input":[{"role":"user","content":"hi"}]}`)
	resp, err := executor.Execute(context.Background(), auth, cliproxyexecutor.Request{
		Model:   "gpt-5.1-codex-max",
		Payload: payload,
	}, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FromString("openai-response"),
		Alt:          "responses/compact",
		Stream:       false,
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if gotPath != "/v1/responses/compact" {
		t.Fatalf("path = %q, want %q", gotPath, "/v1/responses/compact")
	}
	if !gjson.GetBytes(gotBody, "input").Exists() {
		t.Fatalf("expected input in body")
	}
	if gjson.GetBytes(gotBody, "messages").Exists() {
		t.Fatalf("unexpected messages in body")
	}
	if string(resp.Payload) != `{"id":"resp_1","object":"response.compaction","usage":{"input_tokens":1,"output_tokens":2,"total_tokens":3}}` {
		t.Fatalf("payload = %s", string(resp.Payload))
	}
}

func TestOpenAICompatExecutorPayloadOverrideWinsOverThinkingSuffix(t *testing.T) {
	var gotBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		gotBody = body
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"chatcmpl_1","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`))
	}))
	defer server.Close()

	executor := NewOpenAICompatExecutor("openai-compatibility", &config.Config{
		Payload: config.PayloadConfig{
			Override: []config.PayloadRule{
				{
					Models: []config.PayloadModelRule{
						{Name: "custom-openai", Protocol: "openai"},
					},
					Params: map[string]any{
						"reasoning_effort": "low",
					},
				},
			},
		},
	})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{
		"base_url": server.URL + "/v1",
		"api_key":  "test",
	}}
	payload := []byte(`{"model":"custom-openai(high)","messages":[{"role":"user","content":"hi"}]}`)
	_, err := executor.Execute(context.Background(), auth, cliproxyexecutor.Request{
		Model:   "custom-openai(high)",
		Payload: payload,
	}, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FromString("openai"),
		Stream:       false,
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if got := gjson.GetBytes(gotBody, "reasoning_effort").String(); got != "low" {
		t.Fatalf("reasoning_effort = %q, want %q; body=%s", got, "low", string(gotBody))
	}
}

func TestOpenAICompatExecutorForwardsFireworksDirectRoutingHeader(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprintf("stream_%v", stream), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if got := r.Header.Get("x-fireworks-direct-routing"); got != "true" {
					t.Fatalf("x-fireworks-direct-routing = %q, want true", got)
				}
				if stream {
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\n"))
					_, _ = w.Write([]byte("data: [DONE]\n\n"))
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"id":"chatcmpl_1","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`))
			}))
			defer server.Close()

			executor := NewOpenAICompatExecutor("fireworks", &config.Config{})
			auth := &cliproxyauth.Auth{Attributes: map[string]string{
				"base_url": server.URL + "/v1",
				"api_key":  "test",
			}}
			opts := cliproxyexecutor.Options{
				SourceFormat: sdktranslator.FromString("openai"),
				Stream:       stream,
				Headers:      http.Header{"X-Fireworks-Direct-Routing": []string{"true"}},
			}
			request := cliproxyexecutor.Request{
				Model:   "accounts/fireworks/models/glm-5",
				Payload: []byte(`{"model":"accounts/fireworks/models/glm-5","messages":[{"role":"user","content":"hi"}]}`),
			}
			if stream {
				result, err := executor.ExecuteStream(context.Background(), auth, request, opts)
				if err != nil {
					t.Fatalf("ExecuteStream error: %v", err)
				}
				for range result.Chunks {
				}
				return
			}
			if _, err := executor.Execute(context.Background(), auth, request, opts); err != nil {
				t.Fatalf("Execute error: %v", err)
			}
		})
	}
}

func TestOpenAICompatExecutorStreamRejectsPlainJSONAfterBlankLines(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("\n\n: openrouter processing\n\nevent: error\n"))
		_, _ = w.Write([]byte(`{"error":{"message":"upstream failed","type":"server_error"}}` + "\n"))
	}))
	defer server.Close()

	executor := NewOpenAICompatExecutor("openai-compatibility", &config.Config{})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{
		"base_url": server.URL + "/v1",
		"api_key":  "test",
	}}
	result, err := executor.ExecuteStream(context.Background(), auth, cliproxyexecutor.Request{
		Model:   "openrouter-model",
		Payload: []byte(`{"model":"openrouter-model","messages":[{"role":"user","content":"hi"}],"stream":true}`),
	}, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FromString("openai"),
		Stream:       true,
	})
	if err != nil {
		t.Fatalf("ExecuteStream error: %v", err)
	}

	var gotErr error
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			gotErr = chunk.Err
			break
		}
	}
	if gotErr == nil {
		t.Fatalf("expected plain JSON stream error")
	}
	if status, ok := gotErr.(interface{ StatusCode() int }); !ok || status.StatusCode() != http.StatusBadGateway {
		t.Fatalf("stream error status = %v, want %d", gotErr, http.StatusBadGateway)
	}
	if !strings.Contains(gotErr.Error(), "upstream failed") {
		t.Fatalf("stream error = %v", gotErr)
	}
}

func TestOpenAICompatExecutorStreamSkipsKeepAliveUntilDataLine(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("\n\n: openrouter processing\n\nevent: ping\nid: 1\nretry: 1000\n"))
		_, _ = w.Write([]byte(`data: {"id":"chatcmpl_1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"hello"},"finish_reason":null}]}` + "\n"))
	}))
	defer server.Close()

	executor := NewOpenAICompatExecutor("openai-compatibility", &config.Config{})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{
		"base_url": server.URL + "/v1",
		"api_key":  "test",
	}}
	result, err := executor.ExecuteStream(context.Background(), auth, cliproxyexecutor.Request{
		Model:   "openrouter-model",
		Payload: []byte(`{"model":"openrouter-model","messages":[{"role":"user","content":"hi"}],"stream":true}`),
	}, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FromString("openai"),
		Stream:       true,
	})
	if err != nil {
		t.Fatalf("ExecuteStream error: %v", err)
	}

	var got strings.Builder
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			t.Fatalf("unexpected stream error: %v", chunk.Err)
		}
		got.Write(chunk.Payload)
	}
	if gjson.Get(got.String(), "choices.0.delta.content").String() != "hello" {
		t.Fatalf("stream payload = %s", got.String())
	}
}

func TestNewOpenAIShapeStatusErrPreservesCredentialFailureClassification(t *testing.T) {
	tests := []struct {
		name        string
		statusCode  int
		body        string
		wantNeutral bool
	}{
		{name: "transient server error", statusCode: http.StatusBadGateway, body: `{"error":{"type":"server_error"}}`, wantNeutral: true},
		{name: "authentication error in 5xx", statusCode: http.StatusInternalServerError, body: `{"error":{"type":"authentication_error"}}`},
		{name: "string authentication error in 5xx", statusCode: http.StatusInternalServerError, body: `{"error":"invalid api key"}`},
		{name: "plain authentication error in 5xx", statusCode: http.StatusInternalServerError, body: `invalid api key`},
		{name: "permission error in 5xx", statusCode: http.StatusBadGateway, body: `{"error":{"type":"permission_error"}}`},
		{name: "payment error in 5xx", statusCode: http.StatusServiceUnavailable, body: `{"error":{"code":"payment_required"}}`},
		{name: "quota error in 5xx", statusCode: http.StatusInternalServerError, body: `{"error":{"code":"insufficient_quota"}}`},
		{name: "rate limit status", statusCode: http.StatusTooManyRequests, body: `{"error":{"type":"server_error"}}`},
		{name: "provider capacity in 5xx", statusCode: http.StatusServiceUnavailable, body: `{"error":{"type":"server_error","message":"no capacity available"}}`, wantNeutral: true},
		{name: "at capacity message in 5xx", statusCode: http.StatusBadGateway, body: `{"error":{"type":"server_error","message":"model is at capacity"}}`, wantNeutral: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := newOpenAIShapeStatusErr(tc.statusCode, []byte(tc.body))
			if got := err.StatusCode(); got != tc.statusCode {
				t.Fatalf("StatusCode() = %d, want %d", got, tc.statusCode)
			}
			if got := err.AuthStateNeutral(); got != tc.wantNeutral {
				t.Fatalf("AuthStateNeutral() = %t, want %t", got, tc.wantNeutral)
			}
		})
	}
}

func TestOpenAICompatExecutorTransportFailureIsAuthStateNeutral(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	baseURL := server.URL
	server.Close()

	executor := NewOpenAICompatExecutor("openai-compatibility", &config.Config{})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{
		"base_url": baseURL,
		"api_key":  "test",
	}}
	_, err := executor.Execute(t.Context(), auth, cliproxyexecutor.Request{
		Model:   "openai-model",
		Payload: []byte(`{"model":"openai-model","messages":[{"role":"user","content":"hi"}]}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("openai")})
	if err == nil {
		t.Fatal("Execute() error = nil, want transport failure")
	}
	var neutral interface{ AuthStateNeutral() bool }
	if !errors.As(err, &neutral) || !neutral.AuthStateNeutral() {
		t.Fatalf("transport error is not auth-state-neutral: %v", err)
	}
}

type aiStudioRelayStatusTestError struct {
	status  int
	message string
}

func (e aiStudioRelayStatusTestError) Error() string   { return e.message }
func (e aiStudioRelayStatusTestError) StatusCode() int { return e.status }

func TestNewAIStudioRelayErrorPreservesStatusClassification(t *testing.T) {
	tests := []struct {
		name        string
		err         error
		wantStatus  int
		wantNeutral bool
	}{
		{name: "transport", err: errors.New("relay disconnected"), wantNeutral: true},
		{name: "transient status", err: aiStudioRelayStatusTestError{status: http.StatusServiceUnavailable, message: "temporarily unavailable"}, wantStatus: http.StatusServiceUnavailable, wantNeutral: true},
		{name: "authentication status", err: aiStudioRelayStatusTestError{status: http.StatusUnauthorized, message: "invalid api key"}, wantStatus: http.StatusUnauthorized},
		{name: "mislabeled authentication status", err: aiStudioRelayStatusTestError{status: http.StatusInternalServerError, message: "invalid api key"}, wantStatus: http.StatusInternalServerError},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := newAIStudioRelayError(tc.err)
			var neutral interface{ AuthStateNeutral() bool }
			if !errors.As(err, &neutral) || neutral.AuthStateNeutral() != tc.wantNeutral {
				t.Fatalf("AuthStateNeutral() for %v = %v, want %t", err, neutral, tc.wantNeutral)
			}
			if tc.wantStatus > 0 {
				var status interface{ StatusCode() int }
				if !errors.As(err, &status) || status.StatusCode() != tc.wantStatus {
					t.Fatalf("StatusCode() for %v = %v, want %d", err, status, tc.wantStatus)
				}
			}
		})
	}
}

func TestNewGoogleStatusErrKeepsProviderCapacityAuthStateNeutral(t *testing.T) {
	for _, body := range []string{
		`{"error":{"code":503,"status":"UNAVAILABLE","message":"no capacity available"}}`,
		`{"error":{"code":502,"status":"UNAVAILABLE","message":"model is at capacity"}}`,
	} {
		err := newGoogleStatusErr(http.StatusServiceUnavailable, []byte(body))
		if !err.AuthStateNeutral() {
			t.Fatalf("AuthStateNeutral() for %s = false, want true", body)
		}
	}
	credential := newGoogleStatusErr(http.StatusInternalServerError, []byte(`{"error":{"code":500,"message":"api key not valid"}}`))
	if credential.AuthStateNeutral() {
		t.Fatal("credential failure was classified auth-state-neutral")
	}
}
