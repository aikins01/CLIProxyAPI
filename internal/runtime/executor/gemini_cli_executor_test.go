package executor

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/runtime/geminicli"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	"golang.org/x/oauth2"
)

type geminiRoundTripperFunc func(*http.Request) (*http.Response, error)

func (f geminiRoundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type geminiErrorReader struct {
	err error
}

func (r geminiErrorReader) Read([]byte) (int, error) {
	return 0, r.err
}

func TestGeminiCLITokenMetadataUsesManagerUpdater(t *testing.T) {
	auth := &cliproxyauth.Auth{
		ID:       "auth-1",
		Provider: "gemini-cli",
		Metadata: map[string]any{"access_token": "original"},
	}
	executor := NewGeminiCLIExecutor(nil)
	var calls int
	executor.SetAuthMetadataUpdater(func(_ context.Context, expected *cliproxyauth.Auth, updates map[string]any) (*cliproxyauth.Auth, error) {
		calls++
		if expected != auth {
			t.Fatalf("expected auth = %p, want %p", expected, auth)
		}
		updated := expected.Clone()
		for key, value := range updates {
			updated.Metadata[key] = value
		}
		return updated, nil
	})
	token := &oauth2.Token{
		AccessToken:  "rotated",
		RefreshToken: "refresh",
		TokenType:    "Bearer",
		Expiry:       time.Now().Add(time.Hour),
	}

	if err := executor.updateGeminiCLITokenMetadata(t.Context(), auth, nil, token); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("metadata updater calls = %d, want 1", calls)
	}
	if auth.Metadata["access_token"] != "original" {
		t.Fatalf("execution snapshot was mutated: %#v", auth.Metadata)
	}
}

func TestGeminiCLITokenMetadataDoesNotPublishRejectedSharedState(t *testing.T) {
	shared := geminicli.NewSharedCredential("auth-parent", "", map[string]any{"access_token": "original"}, []string{"project-1"})
	auth := &cliproxyauth.Auth{
		ID:       "auth-virtual",
		Provider: "gemini-cli",
		Runtime:  geminicli.NewVirtualCredential("project-1", shared),
	}
	executor := NewGeminiCLIExecutor(nil)
	wantErr := errors.New("persist failed")
	executor.SetAuthMetadataUpdater(func(context.Context, *cliproxyauth.Auth, map[string]any) (*cliproxyauth.Auth, error) {
		return nil, wantErr
	})
	token := &oauth2.Token{AccessToken: "rotated", RefreshToken: "refresh", TokenType: "Bearer", Expiry: time.Now().Add(time.Hour)}

	if err := executor.updateGeminiCLITokenMetadata(t.Context(), auth, nil, token); !errors.Is(err, wantErr) {
		t.Fatalf("metadata update error = %v, want %v", err, wantErr)
	}
	if snapshot := shared.MetadataSnapshot(); snapshot["access_token"] != "original" {
		t.Fatalf("rejected shared metadata was published: %#v", snapshot)
	}
}

func TestGeminiCLITokenMetadataRejectsMissingCommittedSharedState(t *testing.T) {
	shared := geminicli.NewSharedCredential("auth-parent", "", map[string]any{"access_token": "original"}, []string{"project-1"})
	auth := &cliproxyauth.Auth{
		ID:       "auth-virtual",
		Provider: "gemini-cli",
		Runtime:  geminicli.NewVirtualCredential("project-1", shared),
	}
	executor := NewGeminiCLIExecutor(nil)
	executor.SetAuthMetadataUpdater(func(context.Context, *cliproxyauth.Auth, map[string]any) (*cliproxyauth.Auth, error) {
		return nil, nil
	})
	token := &oauth2.Token{AccessToken: "rotated", RefreshToken: "refresh", TokenType: "Bearer", Expiry: time.Now().Add(time.Hour)}

	if err := executor.updateGeminiCLITokenMetadata(t.Context(), auth, nil, token); err == nil {
		t.Fatal("metadata update succeeded without committed state")
	}
	if snapshot := shared.MetadataSnapshot(); snapshot["access_token"] != "original" {
		t.Fatalf("missing committed metadata was published: %#v", snapshot)
	}
}

func TestNewGeminiStatusErrPreservesAuthAndRetryBehavior(t *testing.T) {
	tests := []struct {
		name        string
		statusCode  int
		body        []byte
		wantNeutral bool
	}{
		{name: "transient upstream", statusCode: http.StatusServiceUnavailable, body: []byte(`{"error":{"status":"UNAVAILABLE"}}`), wantNeutral: true},
		{name: "authentication", statusCode: http.StatusInternalServerError, body: []byte(`{"error":{"status":"UNAUTHENTICATED"}}`)},
		{name: "quota", statusCode: http.StatusTooManyRequests, body: []byte(`{"error":{"status":"RESOURCE_EXHAUSTED"}}`)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := newGeminiStatusErr(tc.statusCode, tc.body)
			if got := err.AuthStateNeutral(); got != tc.wantNeutral {
				t.Fatalf("AuthStateNeutral() = %t, want %t", got, tc.wantNeutral)
			}
			if err.StatusCode() != tc.statusCode {
				t.Fatalf("StatusCode() = %d, want %d", err.StatusCode(), tc.statusCode)
			}
		})
	}

	body := []byte(`{"error":{"status":"RESOURCE_EXHAUSTED","details":[{"@type":"type.googleapis.com/google.rpc.RetryInfo","retryDelay":"1.25s"}]}}`)
	err := newGeminiStatusErr(http.StatusTooManyRequests, body)
	if err.RetryAfter() == nil || *err.RetryAfter() != 1250*time.Millisecond {
		t.Fatalf("RetryAfter() = %v, want 1.25s", err.RetryAfter())
	}
}

func TestGeminiCLIExecutionTransportFailuresAreAuthStateNeutral(t *testing.T) {
	wantErr := errors.New("upstream transport failed")
	auth := &cliproxyauth.Auth{
		ID: "auth-transport",
		Metadata: map[string]any{
			"access_token": "token",
			"expiry":       time.Now().Add(time.Hour).Format(time.RFC3339),
		},
	}
	executor := NewGeminiCLIExecutor(new(config.Config))
	ctx := context.WithValue(t.Context(), "cliproxy.roundtripper", geminiRoundTripperFunc(func(*http.Request) (*http.Response, error) {
		return nil, wantErr
	}))

	_, err := executor.Execute(ctx, auth, cliproxyexecutor.Request{
		Model:   "gemini-2.5-flash",
		Payload: []byte(`{"model":"gemini-2.5-flash","request":{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatGeminiCLI})
	if !errors.Is(err, wantErr) {
		t.Fatalf("Execute() error = %v, want %v", err, wantErr)
	}
	var neutral interface{ AuthStateNeutral() bool }
	if !errors.As(err, &neutral) || !neutral.AuthStateNeutral() {
		t.Fatalf("Execute() error is not auth-state-neutral: %v", err)
	}
}

func TestGeminiCLINon2xxReadFailureIsAuthStateNeutral(t *testing.T) {
	wantErr := errors.New("response read failed")
	auth := &cliproxyauth.Auth{
		ID: "auth-read",
		Metadata: map[string]any{
			"access_token": "token",
			"expiry":       time.Now().Add(time.Hour).Format(time.RFC3339),
		},
	}
	executor := NewGeminiCLIExecutor(new(config.Config))
	ctx := context.WithValue(t.Context(), "cliproxy.roundtripper", geminiRoundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusServiceUnavailable,
			Header:     make(http.Header),
			Body:       io.NopCloser(io.MultiReader(strings.NewReader(`{"error":`), geminiErrorReader{err: wantErr})),
		}, nil
	}))

	_, err := executor.Execute(ctx, auth, cliproxyexecutor.Request{
		Model:   "gemini-2.5-flash",
		Payload: []byte(`{"model":"gemini-2.5-flash","request":{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatGeminiCLI})
	if !errors.Is(err, wantErr) {
		t.Fatalf("Execute() error = %v, want read failure %v", err, wantErr)
	}
	var neutral interface{ AuthStateNeutral() bool }
	if !errors.As(err, &neutral) || !neutral.AuthStateNeutral() {
		t.Fatalf("Execute() read error is not auth-state-neutral: %v", err)
	}
}

func TestGeminiCLIStreamReadFailureIsAuthStateNeutral(t *testing.T) {
	wantErr := errors.New("stream read failed")
	auth := &cliproxyauth.Auth{
		ID: "auth-stream-read",
		Metadata: map[string]any{
			"access_token": "token",
			"expiry":       time.Now().Add(time.Hour).Format(time.RFC3339),
		},
	}
	executor := NewGeminiCLIExecutor(new(config.Config))
	ctx := context.WithValue(t.Context(), "cliproxy.roundtripper", geminiRoundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(geminiErrorReader{err: wantErr}),
		}, nil
	}))

	result, err := executor.ExecuteStream(ctx, auth, cliproxyexecutor.Request{
		Model:   "gemini-2.5-flash",
		Payload: []byte(`{"model":"gemini-2.5-flash","request":{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatGeminiCLI})
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	var streamErr error
	for chunk := range result.Chunks {
		if len(chunk.Payload) > 0 {
			t.Fatalf("stream read failure emitted completion payload first: %q", chunk.Payload)
		}
		if chunk.Err != nil {
			streamErr = chunk.Err
		}
	}
	if !errors.Is(streamErr, wantErr) {
		t.Fatalf("stream error = %v, want %v", streamErr, wantErr)
	}
	var neutral interface{ AuthStateNeutral() bool }
	if !errors.As(streamErr, &neutral) || !neutral.AuthStateNeutral() {
		t.Fatalf("stream read error is not auth-state-neutral: %v", streamErr)
	}
}
