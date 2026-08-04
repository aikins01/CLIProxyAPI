package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v7/sdk/config"
)

type failOnceStreamExecutor struct {
	mu        sync.Mutex
	calls     int
	committed bool
	keepAlive time.Duration
	partial   []byte
}

func TestExecuteStreamWithAuthManagerCompatibilitySignature(t *testing.T) {
	handler := NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, nil)
	var execute func(context.Context, string, string, []byte, string) (<-chan []byte, http.Header, <-chan *interfaces.ErrorMessage) = handler.ExecuteStreamWithAuthManager
	if execute == nil {
		t.Fatal("compatibility method is nil")
	}
	_ = coreexecutor.StreamResult{Headers: nil, Chunks: nil}
}

func TestExecuteStreamWithAuthManagerMetaReturnsNonNilStreamMetaOnImmediateError(t *testing.T) {
	handler := NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, nil)
	_, _, streamMeta, errChan := handler.ExecuteStreamWithAuthManagerMeta(context.Background(), "openai", "unknown-model", []byte(`{"model":"unknown-model"}`), "")
	if streamMeta == nil {
		t.Fatal("expected non-nil StreamMeta on immediate model-resolution error")
	}
	if streamMeta.BootstrapCommitted() {
		t.Fatal("unexpected committed bootstrap on immediate error")
	}
	if streamMeta.BootstrapCommittedSignal() == nil {
		t.Fatal("expected non-nil committed signal")
	}
	if msg := <-errChan; msg == nil {
		t.Fatal("expected immediate error message")
	}
}

func TestStreamResultMetadataConsumption(t *testing.T) {
	result := &coreexecutor.StreamResult{Headers: http.Header{"X-Upstream": {"present"}}}
	result.SetKeepAliveInterval(5 * time.Second)
	result.SetBootstrapCommitted()
	if len(result.Headers) != 1 || result.Headers.Get("X-Upstream") != "present" {
		t.Fatalf("setting stream metadata changed headers: %#v", result.Headers)
	}

	if !result.BootstrapCommitted() {
		t.Fatal("bootstrap commitment was not recorded")
	}
	interval := result.TakeKeepAliveInterval()
	if interval == nil || *interval != 5*time.Second {
		t.Fatalf("keepalive interval = %v, want %v", interval, 5*time.Second)
	}
	if !result.BootstrapCommitted() {
		t.Fatal("taking keepalive metadata removed bootstrap commitment")
	}
	if !result.TakeBootstrapCommitted() {
		t.Fatal("bootstrap commitment was not returned")
	}
	if result.BootstrapCommitted() || result.TakeBootstrapCommitted() || result.TakeKeepAliveInterval() != nil {
		t.Fatal("stream metadata remained after consumption")
	}
	if len(result.Headers) != 1 || result.Headers.Get("X-Upstream") != "present" {
		t.Fatalf("headers after metadata consumption = %#v", result.Headers)
	}
}

func TestStreamResultMetadataConcurrentAccess(t *testing.T) {
	result := &coreexecutor.StreamResult{Headers: http.Header{"X-Upstream": {"present"}}}
	var wg sync.WaitGroup
	for worker := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for iteration := range 1000 {
				result.SetKeepAliveInterval(time.Duration(worker+iteration) * time.Millisecond)
				result.SetBootstrapCommitted()
				_ = result.BootstrapCommitted()
				_ = result.TakeKeepAliveInterval()
				_ = result.TakeBootstrapCommitted()
			}
		}()
	}
	wg.Wait()

	result.SetKeepAliveInterval(7 * time.Second)
	result.SetBootstrapCommitted()
	interval := result.TakeKeepAliveInterval()
	if interval == nil || *interval != 7*time.Second || !result.TakeBootstrapCommitted() {
		t.Fatalf("final metadata = %v, committed = %t", interval, result.BootstrapCommitted())
	}
	if len(result.Headers) != 1 || result.Headers.Get("X-Upstream") != "present" {
		t.Fatalf("concurrent stream metadata changed headers: %#v", result.Headers)
	}
}

func (e *failOnceStreamExecutor) Identifier() string { return "codex" }

func (e *failOnceStreamExecutor) Execute(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, &coreauth.Error{Code: "not_implemented", Message: "Execute not implemented"}
}

func (e *failOnceStreamExecutor) ExecuteStream(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	e.mu.Lock()
	e.calls++
	call := e.calls
	e.mu.Unlock()

	ch := make(chan coreexecutor.StreamChunk, 2)
	if call == 1 {
		if len(e.partial) > 0 {
			ch <- coreexecutor.StreamChunk{Payload: e.partial}
		}
		ch <- coreexecutor.StreamChunk{
			Err: &coreauth.Error{
				Code:       "unauthorized",
				Message:    "unauthorized",
				Retryable:  false,
				HTTPStatus: http.StatusUnauthorized,
			},
		}
		close(ch)
		result := &coreexecutor.StreamResult{
			Headers: http.Header{"X-Upstream-Attempt": {"1"}},
			Chunks:  ch,
		}
		if e.committed {
			result.SetBootstrapCommitted()
		}
		if e.keepAlive != 0 {
			result.SetKeepAliveInterval(e.keepAlive)
		}
		return result, nil
	}

	ch <- coreexecutor.StreamChunk{Payload: []byte(`{"ok":true}`)}
	close(ch)
	return &coreexecutor.StreamResult{
		Headers: http.Header{"X-Upstream-Attempt": {"2"}},
		Chunks:  ch,
	}, nil
}

func (e *failOnceStreamExecutor) Refresh(ctx context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (e *failOnceStreamExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, &coreauth.Error{Code: "not_implemented", Message: "CountTokens not implemented"}
}

func (e *failOnceStreamExecutor) HttpRequest(ctx context.Context, auth *coreauth.Auth, req *http.Request) (*http.Response, error) {
	return nil, &coreauth.Error{
		Code:       "not_implemented",
		Message:    "HttpRequest not implemented",
		HTTPStatus: http.StatusNotImplemented,
	}
}

func (e *failOnceStreamExecutor) Calls() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.calls
}

type payloadThenErrorStreamExecutor struct {
	mu    sync.Mutex
	calls int
}

func (e *payloadThenErrorStreamExecutor) Identifier() string { return "codex" }

func (e *payloadThenErrorStreamExecutor) Execute(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, &coreauth.Error{Code: "not_implemented", Message: "Execute not implemented"}
}

func (e *payloadThenErrorStreamExecutor) ExecuteStream(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	e.mu.Lock()
	e.calls++
	e.mu.Unlock()

	ch := make(chan coreexecutor.StreamChunk, 2)
	ch <- coreexecutor.StreamChunk{Payload: []byte(`{"partial":true}`)}
	ch <- coreexecutor.StreamChunk{
		Err: &coreauth.Error{
			Code:       "upstream_closed",
			Message:    "upstream closed",
			Retryable:  false,
			HTTPStatus: http.StatusBadGateway,
		},
	}
	close(ch)
	return &coreexecutor.StreamResult{Chunks: ch}, nil
}

func (e *payloadThenErrorStreamExecutor) Refresh(ctx context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (e *payloadThenErrorStreamExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, &coreauth.Error{Code: "not_implemented", Message: "CountTokens not implemented"}
}

func (e *payloadThenErrorStreamExecutor) HttpRequest(ctx context.Context, auth *coreauth.Auth, req *http.Request) (*http.Response, error) {
	return nil, &coreauth.Error{
		Code:       "not_implemented",
		Message:    "HttpRequest not implemented",
		HTTPStatus: http.StatusNotImplemented,
	}
}

func (e *payloadThenErrorStreamExecutor) Calls() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.calls
}

type authAwareStreamExecutor struct {
	mu      sync.Mutex
	calls   int
	authIDs []string
}

type invalidJSONStreamExecutor struct{}

type splitResponsesEventStreamExecutor struct {
	events        [][]byte
	releaseSecond <-chan struct{}
}

func (e *invalidJSONStreamExecutor) Identifier() string { return "codex" }

func (e *invalidJSONStreamExecutor) Execute(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, &coreauth.Error{Code: "not_implemented", Message: "Execute not implemented"}
}

func (e *invalidJSONStreamExecutor) ExecuteStream(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	ch := make(chan coreexecutor.StreamChunk, 1)
	ch <- coreexecutor.StreamChunk{Payload: []byte("event: response.completed\ndata: {\"type\"\n\n")}
	close(ch)
	return &coreexecutor.StreamResult{Chunks: ch}, nil
}

func (e *invalidJSONStreamExecutor) Refresh(ctx context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (e *invalidJSONStreamExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, &coreauth.Error{Code: "not_implemented", Message: "CountTokens not implemented"}
}

func (e *invalidJSONStreamExecutor) HttpRequest(ctx context.Context, auth *coreauth.Auth, req *http.Request) (*http.Response, error) {
	return nil, &coreauth.Error{
		Code:       "not_implemented",
		Message:    "HttpRequest not implemented",
		HTTPStatus: http.StatusNotImplemented,
	}
}

func (e *splitResponsesEventStreamExecutor) Identifier() string { return "split-sse" }

func (e *splitResponsesEventStreamExecutor) Execute(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, &coreauth.Error{Code: "not_implemented", Message: "Execute not implemented"}
}

func (e *splitResponsesEventStreamExecutor) ExecuteStream(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	if len(e.events) > 0 {
		ch := make(chan coreexecutor.StreamChunk, 1)
		ch <- coreexecutor.StreamChunk{Payload: e.events[0]}
		go func() {
			defer close(ch)
			if e.releaseSecond != nil {
				<-e.releaseSecond
			}
			for _, event := range e.events[1:] {
				ch <- coreexecutor.StreamChunk{Payload: event}
			}
		}()
		return &coreexecutor.StreamResult{Chunks: ch}, nil
	}
	ch := make(chan coreexecutor.StreamChunk, 3)
	ch <- coreexecutor.StreamChunk{Payload: []byte("event: response.completed\n")}
	ch <- coreexecutor.StreamChunk{Payload: []byte("data: {\"type\":\"response.com")}
	ch <- coreexecutor.StreamChunk{Payload: []byte("pleted\",\"response\":{\"id\":\"resp-1\",\"output\":[]}}\n\n")}
	close(ch)
	return &coreexecutor.StreamResult{Chunks: ch}, nil
}

func (e *splitResponsesEventStreamExecutor) Refresh(ctx context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (e *splitResponsesEventStreamExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, &coreauth.Error{Code: "not_implemented", Message: "CountTokens not implemented"}
}

func (e *splitResponsesEventStreamExecutor) HttpRequest(ctx context.Context, auth *coreauth.Auth, req *http.Request) (*http.Response, error) {
	return nil, &coreauth.Error{
		Code:       "not_implemented",
		Message:    "HttpRequest not implemented",
		HTTPStatus: http.StatusNotImplemented,
	}
}

func (e *authAwareStreamExecutor) Identifier() string { return "codex" }

func (e *authAwareStreamExecutor) Execute(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, &coreauth.Error{Code: "not_implemented", Message: "Execute not implemented"}
}

func (e *authAwareStreamExecutor) ExecuteStream(ctx context.Context, auth *coreauth.Auth, req coreexecutor.Request, opts coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	_ = ctx
	_ = req
	_ = opts
	ch := make(chan coreexecutor.StreamChunk, 1)

	authID := ""
	if auth != nil {
		authID = auth.ID
	}

	e.mu.Lock()
	e.calls++
	e.authIDs = append(e.authIDs, authID)
	e.mu.Unlock()

	if authID == "auth1" {
		ch <- coreexecutor.StreamChunk{
			Err: &coreauth.Error{
				Code:       "unauthorized",
				Message:    "unauthorized",
				Retryable:  false,
				HTTPStatus: http.StatusUnauthorized,
			},
		}
		close(ch)
		return &coreexecutor.StreamResult{Chunks: ch}, nil
	}

	ch <- coreexecutor.StreamChunk{Payload: []byte(`{"ok":true}`)}
	close(ch)
	return &coreexecutor.StreamResult{Chunks: ch}, nil
}

func (e *authAwareStreamExecutor) Refresh(ctx context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (e *authAwareStreamExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, &coreauth.Error{Code: "not_implemented", Message: "CountTokens not implemented"}
}

func (e *authAwareStreamExecutor) HttpRequest(ctx context.Context, auth *coreauth.Auth, req *http.Request) (*http.Response, error) {
	return nil, &coreauth.Error{
		Code:       "not_implemented",
		Message:    "HttpRequest not implemented",
		HTTPStatus: http.StatusNotImplemented,
	}
}

func (e *authAwareStreamExecutor) Calls() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.calls
}

func (e *authAwareStreamExecutor) AuthIDs() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]string, len(e.authIDs))
	copy(out, e.authIDs)
	return out
}

func TestExecuteStreamWithAuthManager_RetriesBeforeFirstCompleteValueAndResetsBufferedFragments(t *testing.T) {
	executor := &failOnceStreamExecutor{partial: []byte(`{"stale":`)}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)

	auth1 := &coreauth.Auth{
		ID:       "auth1",
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{"email": "test1@example.com"},
	}
	if _, err := manager.Register(context.Background(), auth1); err != nil {
		t.Fatalf("manager.Register(auth1): %v", err)
	}

	auth2 := &coreauth.Auth{
		ID:       "auth2",
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{"email": "test2@example.com"},
	}
	if _, err := manager.Register(context.Background(), auth2); err != nil {
		t.Fatalf("manager.Register(auth2): %v", err)
	}

	registry.GetGlobalRegistry().RegisterClient(auth1.ID, auth1.Provider, []*registry.ModelInfo{{ID: "test-model"}})
	registry.GetGlobalRegistry().RegisterClient(auth2.ID, auth2.Provider, []*registry.ModelInfo{{ID: "test-model"}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth1.ID)
		registry.GetGlobalRegistry().UnregisterClient(auth2.ID)
	})

	handler := NewBaseAPIHandlers(&sdkconfig.SDKConfig{
		PassthroughHeaders: true,
		Streaming: sdkconfig.StreamingConfig{
			BootstrapRetries: 1,
		},
	}, manager)
	dataChan, upstreamHeaders, _, errChan := handler.ExecuteStreamWithAuthManagerMeta(context.Background(), "openai", "test-model", []byte(`{"model":"test-model"}`), "")
	if dataChan == nil || errChan == nil {
		t.Fatalf("expected non-nil channels")
	}

	var got []byte
	for chunk := range dataChan {
		got = append(got, chunk...)
	}

	for msg := range errChan {
		if msg != nil {
			t.Fatalf("unexpected error: %+v", msg)
		}
	}

	if string(got) != `{"ok":true}` {
		t.Fatalf("expected JSON payload, got %q", string(got))
	}
	if executor.Calls() != 2 {
		t.Fatalf("expected 2 stream attempts, got %d", executor.Calls())
	}
	upstreamAttemptHeader := upstreamHeaders.Get("X-Upstream-Attempt")
	if upstreamAttemptHeader != "2" {
		t.Fatalf("expected upstream header from retry attempt, got %q", upstreamAttemptHeader)
	}
}

func TestExecuteStreamWithAuthManager_HeaderPassthroughDisabledByDefault(t *testing.T) {
	executor := &failOnceStreamExecutor{}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)

	auth1 := &coreauth.Auth{
		ID:       "auth1",
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{"email": "test1@example.com"},
	}
	if _, err := manager.Register(context.Background(), auth1); err != nil {
		t.Fatalf("manager.Register(auth1): %v", err)
	}

	auth2 := &coreauth.Auth{
		ID:       "auth2",
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{"email": "test2@example.com"},
	}
	if _, err := manager.Register(context.Background(), auth2); err != nil {
		t.Fatalf("manager.Register(auth2): %v", err)
	}

	registry.GetGlobalRegistry().RegisterClient(auth1.ID, auth1.Provider, []*registry.ModelInfo{{ID: "test-model"}})
	registry.GetGlobalRegistry().RegisterClient(auth2.ID, auth2.Provider, []*registry.ModelInfo{{ID: "test-model"}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth1.ID)
		registry.GetGlobalRegistry().UnregisterClient(auth2.ID)
	})

	handler := NewBaseAPIHandlers(&sdkconfig.SDKConfig{
		Streaming: sdkconfig.StreamingConfig{
			BootstrapRetries: 1,
		},
	}, manager)
	dataChan, upstreamHeaders, _, errChan := handler.ExecuteStreamWithAuthManagerMeta(context.Background(), "openai", "test-model", []byte(`{"model":"test-model"}`), "")
	if dataChan == nil || errChan == nil {
		t.Fatalf("expected non-nil channels")
	}

	var got []byte
	for chunk := range dataChan {
		got = append(got, chunk...)
	}
	for msg := range errChan {
		if msg != nil {
			t.Fatalf("unexpected error: %+v", msg)
		}
	}

	if string(got) != `{"ok":true}` {
		t.Fatalf("expected JSON payload, got %q", string(got))
	}
	if upstreamHeaders != nil {
		t.Fatalf("expected nil upstream headers when passthrough is disabled, got %#v", upstreamHeaders)
	}
}

func TestExecuteStreamWithAuthManager_DoesNotRetryAfterFirstByte(t *testing.T) {
	executor := &payloadThenErrorStreamExecutor{}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)

	auth1 := &coreauth.Auth{
		ID:       "auth1",
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{"email": "test1@example.com"},
	}
	if _, err := manager.Register(context.Background(), auth1); err != nil {
		t.Fatalf("manager.Register(auth1): %v", err)
	}

	auth2 := &coreauth.Auth{
		ID:       "auth2",
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{"email": "test2@example.com"},
	}
	if _, err := manager.Register(context.Background(), auth2); err != nil {
		t.Fatalf("manager.Register(auth2): %v", err)
	}

	registry.GetGlobalRegistry().RegisterClient(auth1.ID, auth1.Provider, []*registry.ModelInfo{{ID: "test-model"}})
	registry.GetGlobalRegistry().RegisterClient(auth2.ID, auth2.Provider, []*registry.ModelInfo{{ID: "test-model"}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth1.ID)
		registry.GetGlobalRegistry().UnregisterClient(auth2.ID)
	})

	handler := NewBaseAPIHandlers(&sdkconfig.SDKConfig{
		Streaming: sdkconfig.StreamingConfig{
			BootstrapRetries: 1,
		},
	}, manager)
	dataChan, _, _, errChan := handler.ExecuteStreamWithAuthManagerMeta(context.Background(), "openai", "test-model", []byte(`{"model":"test-model"}`), "")
	if dataChan == nil || errChan == nil {
		t.Fatalf("expected non-nil channels")
	}

	var got []byte
	for chunk := range dataChan {
		got = append(got, chunk...)
	}

	var gotErr error
	var gotStatus int
	for msg := range errChan {
		if msg != nil && msg.Error != nil {
			gotErr = msg.Error
			gotStatus = msg.StatusCode
		}
	}

	if string(got) != `{"partial":true}` {
		t.Fatalf("expected complete payload, got %q", string(got))
	}
	if gotErr == nil {
		t.Fatalf("expected terminal error, got nil")
	}
	if gotStatus != http.StatusBadGateway {
		t.Fatalf("expected status %d, got %d", http.StatusBadGateway, gotStatus)
	}
	if executor.Calls() != 1 {
		t.Fatalf("expected 1 stream attempt, got %d", executor.Calls())
	}
}

func TestExecuteStreamWithAuthManager_DoesNotRetryCommittedBootstrap(t *testing.T) {
	executor := &failOnceStreamExecutor{committed: true, keepAlive: 5 * time.Second}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)

	for _, authID := range []string{"auth1", "auth2"} {
		auth := &coreauth.Auth{
			ID:       authID,
			Provider: "codex",
			Status:   coreauth.StatusActive,
			Metadata: map[string]any{"email": authID + "@example.com"},
		}
		if _, err := manager.Register(context.Background(), auth); err != nil {
			t.Fatalf("manager.Register(%s): %v", authID, err)
		}
		registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "test-model"}})
		t.Cleanup(func() {
			registry.GetGlobalRegistry().UnregisterClient(auth.ID)
		})
	}

	handler := NewBaseAPIHandlers(&sdkconfig.SDKConfig{
		PassthroughHeaders: true,
		Streaming: sdkconfig.StreamingConfig{
			BootstrapRetries: 1,
		},
	}, manager)
	dataChan, upstreamHeaders, streamMeta, errChan := handler.ExecuteStreamWithAuthManagerMeta(context.Background(), "openai", "test-model", []byte(`{"model":"test-model"}`), "")
	if !streamMeta.BootstrapCommitted() {
		t.Fatal("committed stream metadata was not visible synchronously")
	}
	keepAlive := streamMeta.KeepAliveInterval()
	if keepAlive == nil || *keepAlive != 5*time.Second {
		t.Fatalf("keepalive = %v, want %v", keepAlive, 5*time.Second)
	}
	if len(upstreamHeaders) != 1 || upstreamHeaders.Get("X-Upstream-Attempt") != "1" {
		t.Fatalf("exposed upstream headers = %#v", upstreamHeaders)
	}

	for chunk := range dataChan {
		t.Fatalf("committed bootstrap emitted unexpected payload %q", chunk)
	}
	var gotErr error
	for msg := range errChan {
		if msg != nil {
			gotErr = msg.Error
		}
	}
	if gotErr == nil {
		t.Fatal("committed bootstrap error was not returned")
	}
	if executor.Calls() != 1 {
		t.Fatalf("committed bootstrap retried %d times", executor.Calls())
	}
}

func TestStreamMetaRetryUpdateIsSynchronized(t *testing.T) {
	initial := time.Second
	meta := newStreamMeta(&initial, false)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 1000; i++ {
			interval := time.Duration(i+1) * time.Millisecond
			meta.set(&interval, i%2 == 0)
		}
		final := 7 * time.Second
		meta.set(&final, true)
	}()

	for {
		select {
		case <-done:
			select {
			case <-meta.BootstrapCommittedSignal():
			default:
				t.Fatal("bootstrap committed signal was not published")
			}
			keepAlive := meta.KeepAliveInterval()
			if keepAlive == nil || *keepAlive != 7*time.Second {
				t.Fatalf("keepalive = %v, want %v", keepAlive, 7*time.Second)
			}
			if !meta.BootstrapCommitted() {
				t.Fatal("bootstrap committed metadata was lost")
			}
			var nilMeta *StreamMeta
			if nilMeta.KeepAliveInterval() != nil || nilMeta.BootstrapCommitted() || nilMeta.BootstrapCommittedSignal() != nil {
				t.Fatal("nil stream metadata getters returned non-zero values")
			}
			return
		default:
			_ = meta.KeepAliveInterval()
			_ = meta.BootstrapCommitted()
		}
	}
}

func TestExecuteStreamWithAuthManager_EnrichesBootstrapRetryAuthUnavailableError(t *testing.T) {
	executor := &failOnceStreamExecutor{}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)

	auth1 := &coreauth.Auth{
		ID:       "auth1",
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{"email": "test1@example.com"},
	}
	if _, err := manager.Register(context.Background(), auth1); err != nil {
		t.Fatalf("manager.Register(auth1): %v", err)
	}

	registry.GetGlobalRegistry().RegisterClient(auth1.ID, auth1.Provider, []*registry.ModelInfo{{ID: "test-model"}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth1.ID)
	})

	handler := NewBaseAPIHandlers(&sdkconfig.SDKConfig{
		Streaming: sdkconfig.StreamingConfig{
			BootstrapRetries: 1,
		},
	}, manager)
	dataChan, _, _, errChan := handler.ExecuteStreamWithAuthManagerMeta(context.Background(), "openai", "test-model", []byte(`{"model":"test-model"}`), "")
	if dataChan == nil || errChan == nil {
		t.Fatalf("expected non-nil channels")
	}

	var got []byte
	for chunk := range dataChan {
		got = append(got, chunk...)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty payload, got %q", string(got))
	}

	var gotErr *interfaces.ErrorMessage
	for msg := range errChan {
		if msg != nil {
			gotErr = msg
		}
	}
	if gotErr == nil {
		t.Fatalf("expected terminal error")
	}
	if gotErr.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", gotErr.StatusCode, http.StatusServiceUnavailable)
	}

	var authErr *coreauth.Error
	if !errors.As(gotErr.Error, &authErr) || authErr == nil {
		t.Fatalf("expected coreauth.Error, got %T", gotErr.Error)
	}
	if authErr.Code != "auth_unavailable" {
		t.Fatalf("code = %q, want %q", authErr.Code, "auth_unavailable")
	}
	if !strings.Contains(authErr.Message, "providers=codex") {
		t.Fatalf("message missing provider context: %q", authErr.Message)
	}
	if !strings.Contains(authErr.Message, "model=test-model") {
		t.Fatalf("message missing model context: %q", authErr.Message)
	}

	if executor.Calls() != 1 {
		t.Fatalf("expected exactly one upstream call before retry path selection failure, got %d", executor.Calls())
	}
}

func TestExecuteStreamWithAuthManager_PinnedAuthKeepsSameUpstream(t *testing.T) {
	executor := &authAwareStreamExecutor{}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)

	auth1 := &coreauth.Auth{
		ID:       "auth1",
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{"email": "test1@example.com"},
	}
	if _, err := manager.Register(context.Background(), auth1); err != nil {
		t.Fatalf("manager.Register(auth1): %v", err)
	}

	auth2 := &coreauth.Auth{
		ID:       "auth2",
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{"email": "test2@example.com"},
	}
	if _, err := manager.Register(context.Background(), auth2); err != nil {
		t.Fatalf("manager.Register(auth2): %v", err)
	}

	registry.GetGlobalRegistry().RegisterClient(auth1.ID, auth1.Provider, []*registry.ModelInfo{{ID: "test-model"}})
	registry.GetGlobalRegistry().RegisterClient(auth2.ID, auth2.Provider, []*registry.ModelInfo{{ID: "test-model"}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth1.ID)
		registry.GetGlobalRegistry().UnregisterClient(auth2.ID)
	})

	handler := NewBaseAPIHandlers(&sdkconfig.SDKConfig{
		Streaming: sdkconfig.StreamingConfig{
			BootstrapRetries: 1,
		},
	}, manager)
	ctx := WithPinnedAuthID(context.Background(), "auth1")
	dataChan, _, _, errChan := handler.ExecuteStreamWithAuthManagerMeta(ctx, "openai", "test-model", []byte(`{"model":"test-model"}`), "")
	if dataChan == nil || errChan == nil {
		t.Fatalf("expected non-nil channels")
	}

	var got []byte
	for chunk := range dataChan {
		got = append(got, chunk...)
	}

	var gotErr error
	for msg := range errChan {
		if msg != nil && msg.Error != nil {
			gotErr = msg.Error
		}
	}

	if len(got) != 0 {
		t.Fatalf("expected empty payload, got %q", string(got))
	}
	if gotErr == nil {
		t.Fatalf("expected terminal error, got nil")
	}
	authIDs := executor.AuthIDs()
	if len(authIDs) == 0 {
		t.Fatalf("expected at least one upstream attempt")
	}
	for _, authID := range authIDs {
		if authID != "auth1" {
			t.Fatalf("expected all attempts on auth1, got sequence %v", authIDs)
		}
	}
}

func TestExecuteStreamWithAuthManager_SelectedAuthCallbackReceivesAuthID(t *testing.T) {
	executor := &authAwareStreamExecutor{}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)

	auth2 := &coreauth.Auth{
		ID:       "auth2",
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{"email": "test2@example.com"},
	}
	if _, err := manager.Register(context.Background(), auth2); err != nil {
		t.Fatalf("manager.Register(auth2): %v", err)
	}

	registry.GetGlobalRegistry().RegisterClient(auth2.ID, auth2.Provider, []*registry.ModelInfo{{ID: "test-model"}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth2.ID)
	})

	handler := NewBaseAPIHandlers(&sdkconfig.SDKConfig{
		Streaming: sdkconfig.StreamingConfig{
			BootstrapRetries: 0,
		},
	}, manager)

	selectedAuthID := ""
	ctx := WithSelectedAuthIDCallback(context.Background(), func(authID string) {
		selectedAuthID = authID
	})
	dataChan, _, _, errChan := handler.ExecuteStreamWithAuthManagerMeta(ctx, "openai", "test-model", []byte(`{"model":"test-model"}`), "")
	if dataChan == nil || errChan == nil {
		t.Fatalf("expected non-nil channels")
	}

	var got []byte
	for chunk := range dataChan {
		got = append(got, chunk...)
	}
	for msg := range errChan {
		if msg != nil {
			t.Fatalf("unexpected error: %+v", msg)
		}
	}

	if string(got) != `{"ok":true}` {
		t.Fatalf("expected JSON payload, got %q", string(got))
	}
	if selectedAuthID != "auth2" {
		t.Fatalf("selectedAuthID = %q, want %q", selectedAuthID, "auth2")
	}
}

func TestExecuteStreamWithAuthManager_ValidatesOpenAIResponsesStreamDataJSON(t *testing.T) {
	executor := &invalidJSONStreamExecutor{}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)

	auth1 := &coreauth.Auth{
		ID:       "auth1",
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{"email": "test1@example.com"},
	}
	if _, err := manager.Register(context.Background(), auth1); err != nil {
		t.Fatalf("manager.Register(auth1): %v", err)
	}

	registry.GetGlobalRegistry().RegisterClient(auth1.ID, auth1.Provider, []*registry.ModelInfo{{ID: "test-model"}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth1.ID)
	})

	handler := NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	dataChan, _, _, errChan := handler.ExecuteStreamWithAuthManagerMeta(context.Background(), "openai-response", "test-model", []byte(`{"model":"test-model"}`), "")
	if dataChan == nil || errChan == nil {
		t.Fatalf("expected non-nil channels")
	}

	var got []byte
	for chunk := range dataChan {
		got = append(got, chunk...)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty payload, got %q", string(got))
	}

	gotErr := false
	for msg := range errChan {
		if msg == nil {
			continue
		}
		if msg.StatusCode != http.StatusBadGateway {
			t.Fatalf("expected status %d, got %d", http.StatusBadGateway, msg.StatusCode)
		}
		if msg.Error == nil {
			t.Fatalf("expected error")
		}
		gotErr = true
	}
	if !gotErr {
		t.Fatalf("expected terminal error")
	}
}

func TestExecuteStreamWithAuthManager_AllowsSplitOpenAIResponsesSSEEventAndDataJSON(t *testing.T) {
	executor := &splitResponsesEventStreamExecutor{}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)

	auth1 := &coreauth.Auth{
		ID:       "auth1",
		Provider: "split-sse",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{"email": "test1@example.com"},
	}
	if _, err := manager.Register(context.Background(), auth1); err != nil {
		t.Fatalf("manager.Register(auth1): %v", err)
	}

	registry.GetGlobalRegistry().RegisterClient(auth1.ID, auth1.Provider, []*registry.ModelInfo{{ID: "test-model"}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth1.ID)
	})

	handler := NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	dataChan, _, _, errChan := handler.ExecuteStreamWithAuthManagerMeta(context.Background(), "openai-response", "test-model", []byte(`{"model":"test-model"}`), "")
	if dataChan == nil || errChan == nil {
		t.Fatalf("expected non-nil channels")
	}

	var got []byte
	for chunk := range dataChan {
		got = append(got, chunk...)
	}

	for msg := range errChan {
		if msg != nil {
			t.Fatalf("unexpected error: %+v", msg)
		}
	}

	expected := "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-1\",\"output\":[]}}\n\n"
	if string(got) != expected {
		t.Fatalf("forwarded stream = %q, want %q", got, expected)
	}
}

func TestExecuteStreamWithAuthManager_ForwardsCompleteResponsesEventsLive(t *testing.T) {
	first := []byte("event: response.created\ndata: {\"type\":\"response.created\"}\n\n")
	second := []byte("event: response.completed\ndata: {\"type\":\"response.completed\"}\n\n")
	releaseSecond := make(chan struct{})
	executor := &splitResponsesEventStreamExecutor{events: [][]byte{first, second}, releaseSecond: releaseSecond}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)

	auth := &coreauth.Auth{ID: "auth1", Provider: "split-sse", Status: coreauth.StatusActive}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("manager.Register: %v", err)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "test-model"}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	})

	handler := NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	dataChan, _, _, errChan := handler.ExecuteStreamWithAuthManagerMeta(context.Background(), "openai-response", "test-model", []byte(`{"model":"test-model"}`), "")
	select {
	case got := <-dataChan:
		if !bytes.Equal(got, first) {
			t.Fatalf("first event = %q, want %q", got, first)
		}
	case <-time.After(time.Second):
		t.Fatal("first complete event was not forwarded before the next event")
	}

	close(releaseSecond)
	select {
	case got := <-dataChan:
		if !bytes.Equal(got, second) {
			t.Fatalf("second event = %q, want %q", got, second)
		}
	case <-time.After(time.Second):
		t.Fatal("second complete event was not forwarded")
	}
	if _, ok := <-dataChan; ok {
		t.Fatal("data channel remained open")
	}
	for msg := range errChan {
		if msg != nil {
			t.Fatalf("unexpected error: %+v", msg)
		}
	}
}

func TestSSEJSONStreamValidatorDoesNotReleaseValidPrefixBeforeBoundary(t *testing.T) {
	validator := &sseJSONStreamValidator{}
	payload, err := validator.Add([]byte(`data: {"type":"response.completed"}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(payload) != 0 {
		t.Fatalf("payload = %q", payload)
	}
	payload, err = validator.Add([]byte(` trailing`))
	if err != nil {
		t.Fatal(err)
	}
	if len(payload) != 0 {
		t.Fatalf("released ambiguous payload: %q", payload)
	}
	if _, err = validator.Finish(); err == nil {
		t.Fatal("expected continued valid prefix to fail validation")
	}
}

func TestSSEJSONStreamValidatorDoesNotInferFieldBoundaryFromChunk(t *testing.T) {
	validator := &sseJSONStreamValidator{}
	if payload, err := validator.Add([]byte(`data: {"text":"`)); err != nil || len(payload) != 0 {
		t.Fatalf("first add payload = %q, err = %v", payload, err)
	}
	if payload, err := validator.Add([]byte(`data:value"}`)); err != nil || len(payload) != 0 {
		t.Fatalf("second add payload = %q, err = %v", payload, err)
	}
	if payload, err := validator.Finish(); err == nil || len(payload) != 0 {
		t.Fatalf("unterminated finish payload = %q, err = %v", payload, err)
	}

	validator = &sseJSONStreamValidator{}
	if payload, err := validator.Add([]byte("data: {\"text\":\"data:value\"}\n\n")); err != nil || string(payload) != "data: {\"text\":\"data:value\"}\n\n" {
		t.Fatalf("delimited add payload = %q, err = %v", payload, err)
	}
	payload, err := validator.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if len(payload) != 0 {
		t.Fatalf("finish payload = %q", payload)
	}
}

func TestSSEJSONStreamValidatorProcessesManyFrames(t *testing.T) {
	validator := &sseJSONStreamValidator{}
	frames := bytes.Repeat([]byte("data: {}\n\n"), 1024)
	payload, err := validator.Add(frames)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(payload, frames) || len(validator.pending) != 0 {
		t.Fatalf("processed payload length = %d, pending = %d", len(payload), len(validator.pending))
	}
}

func TestSSEJSONStreamValidatorHandlesSplitDelimiters(t *testing.T) {
	tests := []struct {
		lineEnding string
		delimiter  string
	}{
		{lineEnding: "\n", delimiter: "\n\n"},
		{lineEnding: "\r\n", delimiter: "\r\n\r\n"},
		{lineEnding: "\r", delimiter: "\r\r"},
		{lineEnding: "\n", delimiter: "\n\r"},
		{lineEnding: "\r", delimiter: "\r\r\n"},
		{lineEnding: "\r\n", delimiter: "\r\n\n"},
	}
	for _, tc := range tests {
		for split := 1; split < len(tc.delimiter); split++ {
			name := fmt.Sprintf("%q_at_%d", tc.delimiter, split)
			t.Run(name, func(t *testing.T) {
				validator := &sseJSONStreamValidator{}
				frame := "event: response.completed" + tc.lineEnding + "data: {}" + tc.delimiter
				prefix := []byte(frame[:len(frame)-len(tc.delimiter)+split])
				if payload, err := validator.Add(prefix); err != nil || len(payload) != 0 {
					t.Fatalf("first add payload = %q, err = %v", payload, err)
				}
				payload, err := validator.Add([]byte(tc.delimiter[split:] + "x"))
				if err != nil {
					t.Fatal(err)
				}
				want := []byte(frame)
				if !bytes.Equal(payload, want) || !bytes.Equal(validator.pending, []byte("x")) {
					t.Fatalf("payload = %q, pending = %q, want %q and pending x", payload, validator.pending, want)
				}
			})
		}
	}
}

func TestSSEJSONStreamValidatorProcessesFramesBeforePartialSuffix(t *testing.T) {
	validator := &sseJSONStreamValidator{}
	frames := bytes.Repeat([]byte("data: {}\n\n"), 1024)
	partial := []byte(`data: {"complete":`)
	chunk := append(bytes.Clone(frames), partial...)
	payload, err := validator.Add(chunk)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(payload, frames) || !bytes.Equal(validator.pending, partial) {
		t.Fatalf("payload length = %d, pending = %q", len(payload), validator.pending)
	}
	payload, err = validator.Add([]byte("true}\r\n\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	want := append(bytes.Clone(partial), []byte("true}\r\n\r\n")...)
	if !bytes.Equal(payload, want) || len(validator.pending) != 0 {
		t.Fatalf("payload = %q, pending = %q, want %q", payload, validator.pending, want)
	}
}

func TestSSEJSONStreamValidatorPendingLimit(t *testing.T) {
	validator := &sseJSONStreamValidator{}
	chunk := make([]byte, maxStreamJSONPendingBytes)
	if payload, err := validator.Add(chunk); err != nil || len(payload) != 0 {
		t.Fatalf("exact-limit add payload length = %d, err = %v", len(payload), err)
	}
	if len(validator.pending) != maxStreamJSONPendingBytes {
		t.Fatalf("pending length = %d, want %d", len(validator.pending), maxStreamJSONPendingBytes)
	}
	if _, err := validator.Add([]byte{'x'}); err == nil {
		t.Fatal("over-limit add succeeded")
	}
	if len(validator.pending) != maxStreamJSONPendingBytes {
		t.Fatalf("over-limit add changed pending length to %d", len(validator.pending))
	}
}

func TestOpenAIJSONStreamValidatorReassemblesEveryByteSplit(t *testing.T) {
	payload := []byte(`{"id":"chatcmpl-1","choices":[{"delta":{"content":"hel\"{}🙂lo"}}]}`)
	inputs := [][]byte{
		payload,
		append([]byte("event: message\r\ndata: "), append(payload, []byte("\r\n\r\n")...)...),
	}

	for inputIndex, input := range inputs {
		for split := 1; split < len(input); split++ {
			validator := &openAIJSONStreamValidator{}
			first, err := validator.Add(input[:split])
			if err != nil {
				t.Fatalf("input %d split %d first add: %v", inputIndex, split, err)
			}
			if len(first) != 0 {
				t.Fatalf("input %d split %d released first payloads: %q", inputIndex, split, first)
			}
			second, err := validator.Add(input[split:])
			if err != nil {
				t.Fatalf("input %d split %d second add: %v", inputIndex, split, err)
			}
			final, err := validator.Finish()
			if err != nil {
				t.Fatalf("input %d split %d finish: %v", inputIndex, split, err)
			}
			got := append(second, final...)
			if len(got) != 1 || !bytes.Equal(got[0], payload) {
				t.Fatalf("input %d split %d payloads = %q, want %q", inputIndex, split, got, payload)
			}
		}
	}
}

func TestOpenAIJSONStreamValidatorFlattensMultiLineSSEData(t *testing.T) {
	validator := &openAIJSONStreamValidator{}
	payloads, err := validator.Add([]byte("data: {\"a\":\ndata: 1}\n\n"))
	if err != nil {
		t.Fatalf("add: %v", err)
	}
	if len(payloads) != 1 {
		t.Fatalf("payloads = %q, want one payload", payloads)
	}
	if !json.Valid(payloads[0]) {
		t.Fatalf("payload is not valid JSON: %q", payloads[0])
	}
	if bytes.IndexByte(payloads[0], '\n') >= 0 {
		t.Fatalf("payload still contains newline, breaking downstream SSE re-framing: %q", payloads[0])
	}
}

func TestOpenAIJSONStreamValidatorProcessesAdjacentValuesAndDoneMarkers(t *testing.T) {
	validator := &openAIJSONStreamValidator{}
	input := []byte("{\"first\":true}{\"second\":\"{}\"}[DONE]data: [DONE]\n\ndata: {\"third\":\"🙂\"}\n\n")
	payloads, err := validator.Add(input)
	if err != nil {
		t.Fatal(err)
	}
	final, err := validator.Finish()
	if err != nil {
		t.Fatal(err)
	}
	payloads = append(payloads, final...)
	want := [][]byte{[]byte(`{"first":true}`), []byte(`{"second":"{}"}`), []byte(`{"third":"🙂"}`)}
	if len(payloads) != len(want) {
		t.Fatalf("payload count = %d, want %d: %q", len(payloads), len(want), payloads)
	}
	for i := range want {
		if !bytes.Equal(payloads[i], want[i]) {
			t.Fatalf("payload %d = %q, want %q", i, payloads[i], want[i])
		}
	}
}

func TestOpenAIJSONStreamValidatorRequiresBoundaryAfterLiteral(t *testing.T) {
	validator := &openAIJSONStreamValidator{}
	payloads, err := validator.Add([]byte("true"))
	if err != nil {
		t.Fatalf("add partial literal: %v", err)
	}
	if len(payloads) != 0 {
		t.Fatalf("complete literal prefix at chunk end was released without a token boundary: %q", payloads)
	}
	if _, err = validator.Add([]byte("e")); err == nil {
		t.Fatal("expected invalid trailing byte after literal to be rejected")
	}

	validator = &openAIJSONStreamValidator{}
	payloads, err = validator.Add([]byte("truefalse"))
	if err != nil {
		t.Fatalf("add adjacent literals: %v", err)
	}
	final, err := validator.Finish()
	if err != nil {
		t.Fatalf("finish adjacent literals: %v", err)
	}
	payloads = append(payloads, final...)
	if len(payloads) != 2 || !bytes.Equal(payloads[0], []byte("true")) || !bytes.Equal(payloads[1], []byte("false")) {
		t.Fatalf("adjacent literal payloads = %q, want [true false]", payloads)
	}

	validator = &openAIJSONStreamValidator{}
	payloads, err = validator.Add([]byte("null-12"))
	if err != nil {
		t.Fatalf("add literal followed by number: %v", err)
	}
	final, err = validator.Finish()
	if err != nil {
		t.Fatalf("finish literal followed by number: %v", err)
	}
	payloads = append(payloads, final...)
	if len(payloads) != 2 || !bytes.Equal(payloads[0], []byte("null")) || !bytes.Equal(payloads[1], []byte("-12")) {
		t.Fatalf("literal+number payloads = %q, want [null -12]", payloads)
	}

	for _, stream := range []string{"true,", "false]", "null}"} {
		validator = &openAIJSONStreamValidator{}
		if _, err = validator.Add([]byte(stream)); err == nil {
			t.Fatalf("expected %q to be rejected as an invalid literal boundary", stream)
		}
	}
}

func TestOpenAIJSONStreamValidatorProcessesAllJSONValueKinds(t *testing.T) {
	values := [][]byte{
		[]byte(`[{"nested":{"text":"[]{}\\backslash\"quote\u263a"}}]`),
		[]byte(`"split \u263a 🙂"`),
		[]byte(`true`),
		[]byte(`false`),
		[]byte(`null`),
		[]byte(`-12.5e+3`),
	}
	for valueIndex, value := range values {
		for split := 1; split <= len(value); split++ {
			validator := &openAIJSONStreamValidator{}
			var got [][]byte
			for offset := 0; offset < len(value); offset += split {
				end := min(offset+split, len(value))
				payloads, err := validator.Add(value[offset:end])
				if err != nil {
					t.Fatalf("value %d split %d add: %v", valueIndex, split, err)
				}
				got = append(got, payloads...)
			}
			payloads, err := validator.Finish()
			if err != nil {
				t.Fatalf("value %d split %d finish: %v", valueIndex, split, err)
			}
			got = append(got, payloads...)
			if len(got) != 1 || !bytes.Equal(got[0], value) {
				t.Fatalf("value %d split %d payloads = %q, want %q", valueIndex, split, got, value)
			}
		}
	}
}

func TestOpenAIJSONStreamValidatorAdvancesIncrementallyAcrossOneByteChunks(t *testing.T) {
	value := append([]byte{'"'}, bytes.Repeat([]byte{'x'}, 8192)...)
	value = append(value, '"')
	validator := &openAIJSONStreamValidator{}
	for index, valueByte := range value {
		payloads, err := validator.Add([]byte{valueByte})
		if err != nil {
			t.Fatalf("byte %d add: %v", index, err)
		}
		if index < len(value)-1 {
			if len(payloads) != 0 || validator.rawScan.offset != len(validator.pending) {
				t.Fatalf("byte %d payloads = %q, scan offset = %d, pending = %d", index, payloads, validator.rawScan.offset, len(validator.pending))
			}
			continue
		}
		if len(payloads) != 1 || !bytes.Equal(payloads[0], value) {
			t.Fatalf("completed payloads = %q, want one %d-byte value", payloads, len(value))
		}
	}
}

func TestOpenAIJSONStreamValidatorRejectsUnterminatedSSEFrame(t *testing.T) {
	validator := &openAIJSONStreamValidator{}
	if payloads, err := validator.Add([]byte("event: message\ndata: {\"ok\":true}")); err != nil || len(payloads) != 0 {
		t.Fatalf("unterminated add payloads = %q, err = %v", payloads, err)
	}
	if payloads, err := validator.Finish(); err == nil || len(payloads) != 0 {
		t.Fatalf("unterminated finish payloads = %q, err = %v", payloads, err)
	}

	validator = &openAIJSONStreamValidator{}
	payloads, err := validator.Add([]byte("event: message\ndata: {\"ok\":true}\n\n"))
	if err != nil || len(payloads) != 1 || !bytes.Equal(payloads[0], []byte(`{"ok":true}`)) {
		t.Fatalf("delimited add payloads = %q, err = %v", payloads, err)
	}
}

func TestOpenAIJSONStreamValidatorRejectsIncompleteAndInvalidJSON(t *testing.T) {
	validator := &openAIJSONStreamValidator{}
	if payloads, err := validator.Add([]byte(`{"incomplete":`)); err != nil || len(payloads) != 0 {
		t.Fatalf("incomplete add payloads = %q, err = %v", payloads, err)
	}
	if _, err := validator.Finish(); err == nil {
		t.Fatal("incomplete JSON finished successfully")
	}

	validator = &openAIJSONStreamValidator{}
	if _, err := validator.Add([]byte(`{"invalid":]}`)); err == nil {
		t.Fatal("invalid JSON add succeeded")
	}

	for _, value := range []string{`"\u12"`, `1e`, `01`, `truX`, `{"mismatch":[1}}`} {
		validator = &openAIJSONStreamValidator{}
		if _, err := validator.Add([]byte(value)); err == nil {
			_, err = validator.Finish()
			if err == nil {
				t.Fatalf("invalid JSON %q succeeded", value)
			}
		}
	}
}

func TestOpenAIJSONStreamValidatorPendingLimit(t *testing.T) {
	complete := make([]byte, maxStreamJSONPendingBytes)
	complete[0] = '"'
	for i := 1; i < len(complete)-1; i++ {
		complete[i] = 'x'
	}
	complete[len(complete)-1] = '"'
	validator := &openAIJSONStreamValidator{}
	payloads, err := validator.Add(complete)
	if err != nil || len(payloads) != 1 || len(payloads[0]) != len(complete) {
		t.Fatalf("exact-limit complete value payloads = %d, err = %v", len(payloads), err)
	}

	validator = &openAIJSONStreamValidator{}
	incomplete := complete[:len(complete)-1]
	if payloads, err = validator.Add(incomplete); err != nil || len(payloads) != 0 {
		t.Fatalf("exact-limit incomplete value payloads = %d, err = %v", len(payloads), err)
	}
	remaining := maxStreamJSONPendingBytes - len(incomplete)
	if payloads, err = validator.Add(bytes.Repeat([]byte{'x'}, remaining)); err != nil || len(payloads) != 0 {
		t.Fatalf("filled pending value payloads = %d, err = %v", len(payloads), err)
	}
	if _, err = validator.Add([]byte{'x'}); err == nil {
		t.Fatal("over-limit incomplete value succeeded")
	}
}

func TestForwardStreamPrioritizesPendingErrorWhenDataCloses(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/stream", nil)
	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		t.Fatal("expected response writer to implement http.Flusher")
	}

	data := make(chan []byte)
	close(data)
	wantErr := errors.New("upstream failed")
	errs := make(chan *interfaces.ErrorMessage, 1)
	errs <- &interfaces.ErrorMessage{StatusCode: http.StatusBadGateway, Error: wantErr}
	close(errs)

	terminal := false
	done := false
	var canceled error
	handler := NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, nil)
	handler.ForwardStream(c, flusher, func(err error) { canceled = err }, data, errs, StreamForwardOptions{
		WriteTerminalError: func(*interfaces.ErrorMessage) { terminal = true },
		WriteDone:          func() { done = true },
	})

	if !terminal || done {
		t.Fatalf("terminal = %v, done = %v", terminal, done)
	}
	if !errors.Is(canceled, wantErr) {
		t.Fatalf("cancel error = %v, want %v", canceled, wantErr)
	}
}
