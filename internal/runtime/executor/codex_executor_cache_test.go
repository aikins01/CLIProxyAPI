package executor

import (
	"context"
	"io"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestCodexExecutorCacheHelper_OpenAIChatCompletions_StablePromptCacheKeyFromAPIKey(t *testing.T) {
	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Set("userApiKey", "test-api-key")

	ctx := context.WithValue(context.Background(), "gin", ginCtx)
	executor := &CodexExecutor{}
	rawJSON := []byte(`{"model":"gpt-5.3-codex","stream":true}`)
	req := cliproxyexecutor.Request{
		Model:   "gpt-5.3-codex",
		Payload: []byte(`{"model":"gpt-5.3-codex"}`),
	}
	url := "https://example.com/responses"

	httpReq, err := executor.cacheHelper(ctx, sdktranslator.FromString("openai"), url, req, rawJSON)
	if err != nil {
		t.Fatalf("cacheHelper error: %v", err)
	}

	body, errRead := io.ReadAll(httpReq.Body)
	if errRead != nil {
		t.Fatalf("read request body: %v", errRead)
	}

	expectedKey := uuid.NewSHA1(uuid.NameSpaceOID, []byte("cli-proxy-api:codex:prompt-cache:test-api-key")).String()
	gotKey := gjson.GetBytes(body, "prompt_cache_key").String()
	if gotKey != expectedKey {
		t.Fatalf("prompt_cache_key = %q, want %q", gotKey, expectedKey)
	}
	if gotConversation := httpReq.Header.Get("Conversation_id"); gotConversation != "" {
		t.Fatalf("Conversation_id = %q, want empty", gotConversation)
	}
	if gotSession := httpReq.Header.Get("Session_id"); gotSession != expectedKey {
		t.Fatalf("Session_id = %q, want %q", gotSession, expectedKey)
	}

	httpReq2, err := executor.cacheHelper(ctx, sdktranslator.FromString("openai"), url, req, rawJSON)
	if err != nil {
		t.Fatalf("cacheHelper error (second call): %v", err)
	}
	body2, errRead2 := io.ReadAll(httpReq2.Body)
	if errRead2 != nil {
		t.Fatalf("read request body (second call): %v", errRead2)
	}
	gotKey2 := gjson.GetBytes(body2, "prompt_cache_key").String()
	if gotKey2 != expectedKey {
		t.Fatalf("prompt_cache_key (second call) = %q, want %q", gotKey2, expectedKey)
	}
}

func TestApplyCodexPromptCacheHeaders_OpenAIChatCompletionsMatchesHTTP(t *testing.T) {
	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Set("userApiKey", "websocket-api-key")
	ctx := context.WithValue(context.Background(), "gin", ginCtx)
	req := cliproxyexecutor.Request{Model: "gpt-5.3-codex", Payload: []byte(`{"model":"gpt-5.3-codex"}`)}

	body, headers, cacheID := applyCodexPromptCacheHeaders(ctx, sdktranslator.FromString("openai"), req, []byte(`{"model":"gpt-5.3-codex"}`))

	expectedKey := uuid.NewSHA1(uuid.NameSpaceOID, []byte("cli-proxy-api:codex:prompt-cache:websocket-api-key")).String()
	if cacheID != expectedKey {
		t.Fatalf("cache ID = %q, want %q", cacheID, expectedKey)
	}
	if got := gjson.GetBytes(body, "prompt_cache_key").String(); got != expectedKey {
		t.Fatalf("prompt_cache_key = %q, want %q", got, expectedKey)
	}
	if got := headerValueCaseInsensitive(headers, "Session_id"); got != expectedKey {
		t.Fatalf("Session_id = %q, want %q", got, expectedKey)
	}
	if got := headers.Get("Conversation_id"); got != "" {
		t.Fatalf("Conversation_id = %q, want empty", got)
	}
}

func TestCodexPromptCacheID_ConcurrentClaudeRequestsReuseID(t *testing.T) {
	req := cliproxyexecutor.Request{
		Model:   "gpt-5.3-codex",
		Payload: []byte(`{"metadata":{"user_id":"concurrent-user"}}`),
	}
	const requestCount = 64
	ids := make(chan string, requestCount)
	var wg sync.WaitGroup
	for range requestCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ids <- codexPromptCacheID(context.Background(), sdktranslator.FromString("claude"), req)
		}()
	}
	wg.Wait()
	close(ids)

	var expected string
	for id := range ids {
		if id == "" {
			t.Fatal("expected a non-empty prompt cache ID")
		}
		if expected == "" {
			expected = id
			continue
		}
		if id != expected {
			t.Fatalf("concurrent prompt cache IDs differ: %q and %q", expected, id)
		}
	}
}
