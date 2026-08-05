package auth

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

type antigravityCreditsFallbackExecutor struct {
	executeCreditsRequested []bool
	streamCreditsRequested  []bool
	creditsError            error
	streamCreditsError      error
}

func (e *antigravityCreditsFallbackExecutor) Identifier() string { return "antigravity" }

func (e *antigravityCreditsFallbackExecutor) Execute(ctx context.Context, _ *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	creditsRequested := AntigravityCreditsRequested(ctx)
	e.executeCreditsRequested = append(e.executeCreditsRequested, creditsRequested)
	if !creditsRequested {
		return cliproxyexecutor.Response{}, &Error{HTTPStatus: http.StatusServiceUnavailable, Message: "quota exhausted"}
	}
	return cliproxyexecutor.Response{}, e.creditsError
}

func (e *antigravityCreditsFallbackExecutor) ExecuteStream(ctx context.Context, _ *Auth, req cliproxyexecutor.Request, _ cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	creditsRequested := AntigravityCreditsRequested(ctx)
	e.streamCreditsRequested = append(e.streamCreditsRequested, creditsRequested)
	ch := make(chan cliproxyexecutor.StreamChunk, 1)
	if !creditsRequested {
		ch <- cliproxyexecutor.StreamChunk{Err: &Error{HTTPStatus: http.StatusTooManyRequests, Message: "quota exhausted"}}
		close(ch)
		return &cliproxyexecutor.StreamResult{Headers: http.Header{"X-Initial": {req.Model}}, Chunks: ch}, nil
	}
	if e.streamCreditsError != nil {
		ch <- cliproxyexecutor.StreamChunk{Err: e.streamCreditsError}
		close(ch)
		return &cliproxyexecutor.StreamResult{Chunks: ch}, nil
	}
	ch <- cliproxyexecutor.StreamChunk{Payload: []byte("credits fallback")}
	close(ch)
	return &cliproxyexecutor.StreamResult{Headers: http.Header{"X-Credits": {req.Model}}, Chunks: ch}, nil
}

func TestManagerAntigravityCreditsFallbackDoesNotReplayNonRetryableError(t *testing.T) {
	const model = "claude-opus-4-6-thinking"
	tests := []struct {
		name       string
		configure  func(*antigravityCreditsFallbackExecutor)
		invoke     func(*Manager) error
		creditRuns func(*antigravityCreditsFallbackExecutor) []bool
	}{
		{
			name: "execute",
			configure: func(executor *antigravityCreditsFallbackExecutor) {
				executor.creditsError = nonRetryableTestError{}
			},
			invoke: func(manager *Manager) error {
				_, err := manager.Execute(t.Context(), []string{"antigravity"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
				return err
			},
			creditRuns: func(executor *antigravityCreditsFallbackExecutor) []bool {
				return executor.executeCreditsRequested
			},
		},
		{
			name: "stream",
			configure: func(executor *antigravityCreditsFallbackExecutor) {
				executor.streamCreditsError = nonRetryableTestError{}
			},
			invoke: func(manager *Manager) error {
				_, err := manager.ExecuteStream(t.Context(), []string{"antigravity"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
				return err
			},
			creditRuns: func(executor *antigravityCreditsFallbackExecutor) []bool {
				return executor.streamCreditsRequested
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			executor := &antigravityCreditsFallbackExecutor{}
			tc.configure(executor)
			manager := NewManager(nil, nil, nil)
			manager.SetConfig(&internalconfig.Config{QuotaExceeded: internalconfig.QuotaExceeded{AntigravityCredits: true}})
			manager.SetRetryConfig(0, 0, 1)
			manager.RegisterExecutor(executor)
			reg := registry.GetGlobalRegistry()
			for _, id := range []string{"ag-a", "ag-b"} {
				reg.RegisterClient(id, "antigravity", []*registry.ModelInfo{{ID: model}})
				t.Cleanup(func() { reg.UnregisterClient(id) })
				if _, err := manager.Register(t.Context(), &Auth{ID: id, Provider: "antigravity"}); err != nil {
					t.Fatal(err)
				}
			}
			if err := tc.invoke(manager); !isNonRetryableExecutionError(err) {
				t.Fatalf("fallback error = %v, want non-retryable", err)
			}
			if got := tc.creditRuns(executor); len(got) != 2 || got[0] || !got[1] {
				t.Fatalf("credits calls = %v, want [false true]", got)
			}
			current, ok := manager.GetByID("ag-a")
			if !ok || current.Failed != 1 {
				t.Fatalf("first auth failures = %v, want one non-credits failure", current)
			}
		})
	}
}

func (e *antigravityCreditsFallbackExecutor) Refresh(_ context.Context, auth *Auth) (*Auth, error) {
	return auth, nil
}

func (e *antigravityCreditsFallbackExecutor) CountTokens(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, &Error{HTTPStatus: http.StatusNotImplemented, Message: "CountTokens not implemented"}
}

func (e *antigravityCreditsFallbackExecutor) HttpRequest(context.Context, *Auth, *http.Request) (*http.Response, error) {
	return nil, &Error{HTTPStatus: http.StatusNotImplemented, Message: "HttpRequest not implemented"}
}

func TestManagerExecuteStream_AntigravityCreditsFallbackAfterBootstrap429(t *testing.T) {
	const model = "claude-opus-4-6-thinking"
	executor := &antigravityCreditsFallbackExecutor{}
	manager := NewManager(nil, nil, nil)
	manager.SetConfig(&internalconfig.Config{
		QuotaExceeded: internalconfig.QuotaExceeded{AntigravityCredits: true},
	})
	manager.RegisterExecutor(executor)
	registry.GetGlobalRegistry().RegisterClient("ag-credits", "antigravity", []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient("ag-credits") })
	if _, errRegister := manager.Register(context.Background(), &Auth{ID: "ag-credits", Provider: "antigravity"}); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	streamResult, errExecute := manager.ExecuteStream(context.Background(), []string{"antigravity"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if errExecute != nil {
		t.Fatalf("execute stream: %v", errExecute)
	}

	var payload []byte
	for chunk := range streamResult.Chunks {
		if chunk.Err != nil {
			t.Fatalf("unexpected stream error: %v", chunk.Err)
		}
		payload = append(payload, chunk.Payload...)
	}
	if string(payload) != "credits fallback" {
		t.Fatalf("payload = %q, want %q", string(payload), "credits fallback")
	}
	if got := streamResult.Headers.Get("X-Credits"); got != model {
		t.Fatalf("X-Credits header = %q, want routed model", got)
	}
	if len(executor.streamCreditsRequested) != 2 {
		t.Fatalf("stream calls = %d, want 2", len(executor.streamCreditsRequested))
	}
	if executor.streamCreditsRequested[0] || !executor.streamCreditsRequested[1] {
		t.Fatalf("credits flags = %v, want [false true]", executor.streamCreditsRequested)
	}
}

func TestStatusCodeFromError_UnwrapsStreamBootstrap429(t *testing.T) {
	bootstrapErr := newStreamBootstrapError(&Error{HTTPStatus: http.StatusTooManyRequests, Message: "quota exhausted"}, nil)
	wrappedErr := fmt.Errorf("conductor stream failed: %w", bootstrapErr)

	if status := statusCodeFromError(wrappedErr); status != http.StatusTooManyRequests {
		t.Fatalf("statusCodeFromError() = %d, want %d", status, http.StatusTooManyRequests)
	}
}

func TestIsAuthBlockedForModel_ClaudeWithCreditsStillBlockedDuringCooldown(t *testing.T) {
	auth := &Auth{
		ID:       "ag-1",
		Provider: "antigravity",
		ModelStates: map[string]*ModelState{
			"claude-sonnet-4-6": {
				Unavailable:    true,
				NextRetryAfter: time.Now().Add(10 * time.Minute),
				Quota: QuotaState{
					Exceeded:      true,
					NextRecoverAt: time.Now().Add(10 * time.Minute),
				},
			},
		},
	}

	SetAntigravityCreditsHint(auth.ID, AntigravityCreditsHint{
		Known:     true,
		Available: true,
		UpdatedAt: time.Now(),
	})

	blocked, reason, _ := isAuthBlockedForModel(auth, "claude-sonnet-4-6", time.Now())
	if !blocked || reason != blockReasonCooldown {
		t.Fatalf("expected auth to be blocked during cooldown even with credits, got blocked=%v reason=%v", blocked, reason)
	}
}

func TestIsAuthBlockedForModel_KeepsGeminiBlockedWithoutCreditsBypass(t *testing.T) {
	auth := &Auth{
		ID:       "ag-2",
		Provider: "antigravity",
		ModelStates: map[string]*ModelState{
			"gemini-3-flash": {
				Unavailable:    true,
				NextRetryAfter: time.Now().Add(10 * time.Minute),
				Quota: QuotaState{
					Exceeded:      true,
					NextRecoverAt: time.Now().Add(10 * time.Minute),
				},
			},
		},
	}

	SetAntigravityCreditsHint(auth.ID, AntigravityCreditsHint{
		Known:     true,
		Available: true,
		UpdatedAt: time.Now(),
	})

	blocked, reason, _ := isAuthBlockedForModel(auth, "gemini-3-flash", time.Now())
	if !blocked || reason != blockReasonCooldown {
		t.Fatalf("expected gemini auth to remain blocked, got blocked=%v reason=%v", blocked, reason)
	}
}
