package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	chatgptweb "github.com/router-for-me/CLIProxyAPI/v7/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/runtime/executor/helps"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	"github.com/tidwall/gjson"
)

const testChatGPTWebUserAgent = "Mozilla/5.0 AppleWebKit/537.36 Chrome/140.0.0.0 Safari/537.36"

type closeTrackingRoundTripper struct {
	closed atomic.Bool
	body   io.ReadCloser
}

func (t *closeTrackingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.body == nil {
		return nil, errors.New("not implemented")
	}
	return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: t.body, Request: req}, nil
}

func (t *closeTrackingRoundTripper) CloseIdleConnections() {
	t.closed.Store(true)
}

func testSession() *chatgptweb.Session {
	return &chatgptweb.Session{
		Cookie:      "__Secure-next-auth.session-token=tok",
		AccessToken: "at",
		DeviceID:    "dev-1",
		AccountID:   "acct-1",
		UserAgent:   testChatGPTWebUserAgent,
	}
}

func testSessionWithCookie(current *chatgptweb.Session, cookie string) *chatgptweb.Session {
	return &chatgptweb.Session{
		Cookie:       cookie,
		AccessToken:  current.AccessToken,
		AccountID:    current.AccountID,
		UserID:       current.UserID,
		DeviceID:     current.DeviceID,
		Email:        current.Email,
		UserAgent:    current.UserAgent,
		WebSessionID: current.WebSessionID,
	}
}

func useChatGPTWebTestURLs(t *testing.T, base, backend, conversation string) {
	t.Helper()
	currentBase, currentBackend, currentConversation := chatgptweb.CurrentBaseURLs()
	if base == "" {
		base = currentBase
	}
	if backend == "" {
		backend = currentBackend
	}
	if conversation == "" {
		conversation = currentConversation
	}
	restore := chatgptweb.SetBaseURLsForTesting(base, backend, conversation)
	t.Cleanup(restore)
}

func TestInputToTurnsPlainString(t *testing.T) {
	t.Parallel()
	body := []byte(`{"model":"gpt-5-6-pro","input":"hello there"}`)
	turns, err := inputToTurns(body)
	if err != nil {
		t.Fatal(err)
	}
	if len(turns) != 1 || turns[0].Role != "user" || turns[0].Content != "hello there" {
		t.Fatalf("turns = %+v", turns)
	}
}

func TestInputToTurnsArray(t *testing.T) {
	t.Parallel()
	body := []byte(`{"instructions":"be terse","input":[{"role":"user","content":[{"type":"input_text","text":"hi"}]},{"role":"assistant","content":[{"type":"output_text","text":"hey"}]}]}`)
	turns, err := inputToTurns(body)
	if err != nil {
		t.Fatal(err)
	}
	if len(turns) != 3 {
		t.Fatalf("turns = %+v", turns)
	}
	if turns[0].Role != "system" || turns[0].Content != "be terse" {
		t.Fatalf("system turn = %+v", turns[0])
	}
	if turns[1].Role != "user" || turns[1].Content != "hi" || turns[2].Role != "assistant" || turns[2].Content != "hey" {
		t.Fatalf("turns = %+v", turns)
	}
}

func TestInputToTurnsRejectsNonText(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"image part":      `{"input":[{"role":"user","content":[{"type":"input_image","image_url":"https://x"}]}]}`,
		"tool item":       `{"input":[{"type":"function_call","name":"f","arguments":"{}"}]}`,
		"tool role":       `{"input":[{"role":"tool","content":[{"type":"input_text","text":"result"}]}]}`,
		"tools":           `{"input":"hi","tools":[{"type":"function","name":"f"}]}`,
		"required tool":   `{"input":"hi","tool_choice":"required"}`,
		"mixed parts":     `{"input":[{"role":"user","content":[{"type":"input_text","text":"hi"},{"type":"input_file","file_data":"..."}]}]}`,
		"unknown part":    `{"input":[{"role":"user","content":[{"type":"refusal","refusal":"no"}]}]}`,
		"object content":  `{"input":[{"role":"user","content":{"type":"input_text","text":"hi"}}]}`,
		"missing text":    `{"input":[{"role":"user","content":[{"type":"input_text"}]}]}`,
		"number text":     `{"input":[{"role":"user","content":[{"type":"input_text","text":42}]}]}`,
		"object text":     `{"input":[{"role":"user","content":[{"type":"input_text","text":{"value":"hi"}}]}]}`,
		"boolean text":    `{"input":[{"role":"user","content":[{"type":"input_text","text":true}]}]}`,
		"null text":       `{"input":[{"role":"user","content":[{"type":"input_text","text":null}]}]}`,
		"missing role":    `{"input":[{"content":[{"type":"input_text","text":"hi"}]}]}`,
		"empty role":      `{"input":[{"role":"","content":[{"type":"input_text","text":"hi"}]}]}`,
		"null role":       `{"input":[{"role":null,"content":[{"type":"input_text","text":"hi"}]}]}`,
		"structured text": `{"input":"hi","text":{"format":{"type":"json_schema","name":"result","schema":{"type":"object"}}}}`,
	}
	for name, body := range cases {
		if _, err := inputToTurns([]byte(body)); err == nil {
			t.Errorf("%s: expected explicit rejection", name)
		}
	}
	executor := NewChatGPTWebExecutor(&config.Config{})
	_, err := executor.CountTokens(t.Context(), nil, cliproxyexecutor.Request{
		Model:   "gpt-5-6-pro",
		Payload: []byte(cases["image part"]),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("codex")})
	var statusErr cliproxyexecutor.StatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode() != http.StatusBadRequest {
		t.Fatalf("non-text request error = %v", err)
	}
	var neutral interface{ AuthStateNeutral() bool }
	if !errors.As(err, &neutral) || !neutral.AuthStateNeutral() {
		t.Fatalf("non-text request error changed auth state: %v", err)
	}
}

func TestChatGPTWebBuildUpstreamBodyRejectsStructuredResponseFormats(t *testing.T) {
	t.Parallel()
	for _, format := range []string{"json_object", "json_schema"} {
		t.Run(format, func(t *testing.T) {
			executor := NewChatGPTWebExecutor(&config.Config{})
			_, _, err := executor.buildUpstreamBody(t.Context(), nil, cliproxyexecutor.Request{
				Model:   "gpt-5-6-pro",
				Payload: []byte(fmt.Sprintf(`{"model":"gpt-5-6-pro","messages":[{"role":"user","content":"hi"}],"response_format":{"type":%q}}`, format)),
			}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAI})
			if err == nil || !strings.Contains(err.Error(), "structured output formats are not supported") {
				t.Fatalf("response format %q error = %v", format, err)
			}
			var statusErr cliproxyexecutor.StatusError
			if !errors.As(err, &statusErr) || statusErr.StatusCode() != http.StatusBadRequest {
				t.Fatalf("response format %q status error = %v", format, err)
			}
		})
	}
}

func TestNormalizeChatGPTWebThinkingEffortAcceptsNone(t *testing.T) {
	t.Parallel()
	got, err := helps.NormalizeChatGPTWebThinkingEffort("none")
	if err != nil || got != "standard" {
		t.Fatalf("none effort normalized to %q with error %v", got, err)
	}
}

func TestChatGPTWebDiscoverModels(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/auth/session":
			_, _ = io.WriteString(w, `{"accessToken":"access-token","user":{"id":"user-id"},"account":{"id":"account-id"}}`)
		case "/backend-api/models":
			if r.Header.Get("Authorization") != "Bearer access-token" {
				t.Fatal("model discovery omitted session authorization")
			}
			w.Header().Set("Set-Cookie", "__Secure-next-auth.session-token=rotated-model-token; Path=/; HttpOnly")
			_, _ = io.WriteString(w, `{"models":[{"slug":"gpt-5-5-thinking","title":"GPT-5.5 Thinking"},{"slug":"gpt-5-6-pro","title":"GPT-5.6 Pro"}]}`)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	useChatGPTWebTestURLs(t, srv.URL, srv.URL+"/backend-api", "")

	var persistedCookie string
	exec := NewChatGPTWebExecutor(&config.Config{}, func(_ context.Context, _ *cliproxyauth.Auth, updates map[string]any) (*cliproxyauth.Auth, error) {
		persistedCookie, _ = updates["cookie"].(string)
		return &cliproxyauth.Auth{}, nil
	})
	models, _, err := exec.DiscoverModels(t.Context(), &cliproxyauth.Auth{ID: "model-auth", Metadata: map[string]any{
		"cookie":     "__Secure-next-auth.session-token=session-token",
		"user_agent": testChatGPTWebUserAgent,
	}})
	if err != nil {
		t.Fatalf("DiscoverModels() error = %v", err)
	}
	if len(models) != 2 || models[0].ID != "chatgpt-web/gpt-5-5-thinking" || models[0].Type != "chatgpt-web" || models[1].ID != "chatgpt-web/gpt-5-6-pro" {
		t.Fatalf("discovered models = %#v", models)
	}
	if models[1].ContextLength == 0 || models[1].DisplayName != "GPT-5.6 Pro (Web)" {
		t.Fatalf("static web model metadata was not preserved: %#v", models[1])
	}
	if chatgptweb.SessionToken(persistedCookie) != "rotated-model-token" {
		t.Fatalf("persisted model-discovery cookie = %q", persistedCookie)
	}
}

func TestChatGPTWebDiscoverModelsDoesNotWaitForTurnLock(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/auth/session":
			_, _ = io.WriteString(w, `{"accessToken":"access-token","user":{"id":"user-id"},"account":{"id":"account-id"}}`)
		case "/backend-api/models":
			_, _ = io.WriteString(w, `{"models":[{"slug":"gpt-5-6-pro","title":"GPT-5.6 Pro"}]}`)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	useChatGPTWebTestURLs(t, srv.URL, srv.URL+"/backend-api", "")

	auth := &cliproxyauth.Auth{Metadata: map[string]any{
		"cookie":     "__Secure-next-auth.session-token=session-token",
		"user_agent": testChatGPTWebUserAgent,
	}}
	exec := NewChatGPTWebExecutor(&config.Config{})
	unlock, err := exec.turns.lock(context.Background(), turnKey(auth, nil))
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	models, _, err := exec.DiscoverModels(ctx, auth)
	if err != nil {
		t.Fatalf("DiscoverModels() error = %v", err)
	}
	if len(models) != 1 || models[0].ID != "chatgpt-web/gpt-5-6-pro" {
		t.Fatalf("discovered models = %#v", models)
	}
}

func TestPersistSessionTokenRotationSurvivesRequestCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	auth := &cliproxyauth.Auth{ID: "auth"}
	exec := NewChatGPTWebExecutor(&config.Config{}, func(ctx context.Context, _ *cliproxyauth.Auth, updates map[string]any) (*cliproxyauth.Auth, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return auth, nil
	})
	if _, err := exec.persistSessionTokenRotation(ctx, auth, "__Secure-next-auth.session-token=old", "__Secure-next-auth.session-token=new"); err != nil {
		t.Fatalf("persist session rotation error = %v", err)
	}
}

func TestPersistSessionTokenRotationDetachedPersistenceIsBounded(t *testing.T) {
	exec := NewChatGPTWebExecutor(&config.Config{}, func(ctx context.Context, _ *cliproxyauth.Auth, _ map[string]any) (*cliproxyauth.Auth, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})
	exec.credentialPersistTimeout = 10 * time.Millisecond
	_, err := exec.persistSessionTokenRotation(t.Context(), &cliproxyauth.Auth{ID: "auth"}, "__Secure-next-auth.session-token=old", "__Secure-next-auth.session-token=new")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("persist session rotation error = %v, want deadline exceeded", err)
	}
	var neutral interface{ AuthStateNeutral() bool }
	var retryable interface{ Retryable() bool }
	if !errors.As(err, &neutral) || !neutral.AuthStateNeutral() || !errors.As(err, &retryable) || retryable.Retryable() {
		t.Fatalf("persistence error was not auth-neutral and non-retryable: %v", err)
	}
}

func TestPersistSessionTokenRotationPersistsAntiAbuseCookie(t *testing.T) {
	var persistedCookie string
	exec := NewChatGPTWebExecutor(&config.Config{}, func(_ context.Context, _ *cliproxyauth.Auth, updates map[string]any) (*cliproxyauth.Auth, error) {
		persistedCookie, _ = updates["cookie"].(string)
		return &cliproxyauth.Auth{}, nil
	})
	previous := "__Secure-next-auth.session-token=token; __cf_bm=old"
	current := "__Secure-next-auth.session-token=token; __cf_bm=new"
	_, err := exec.persistSessionTokenRotation(t.Context(), &cliproxyauth.Auth{ID: "auth"}, previous, current)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(persistedCookie, "__cf_bm=new") {
		t.Fatalf("persisted cookie = %q", persistedCookie)
	}
}

func TestPersistSessionTokenDeletionClearsStoredCredential(t *testing.T) {
	var updates map[string]any
	exec := NewChatGPTWebExecutor(&config.Config{}, func(_ context.Context, _ *cliproxyauth.Auth, got map[string]any) (*cliproxyauth.Auth, error) {
		updates = got
		return &cliproxyauth.Auth{}, nil
	})
	_, err := exec.persistSessionTokenRotation(t.Context(), &cliproxyauth.Auth{ID: "auth"}, "__Secure-next-auth.session-token=old", "__cf_bm=remaining")
	if err != nil {
		t.Fatal(err)
	}
	if updates["cookie"] != "" || updates["session_token"] != "" {
		t.Fatalf("session-token deletion updates = %#v", updates)
	}
}

func TestAcquirePreservesRequirementsErrorAfterCookiePersistence(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/auth/session":
			fmt.Fprint(w, `{"accessToken":"at","user":{"id":"u"},"account":{"id":"a"}}`)
		case "/":
			fmt.Fprint(w, `<html><head data-build="test-build"></head><body></body></html>`)
		case "/backend-api/me", "/backend-api/conversations", "/backend-api/models":
			fmt.Fprint(w, `{}`)
		case "/backend-api/sentinel/chat-requirements/prepare":
			w.Header().Set("Set-Cookie", "__cf_bm=rotated; Path=/; Secure; HttpOnly")
			w.WriteHeader(http.StatusForbidden)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	useChatGPTWebTestURLs(t, srv.URL, srv.URL+"/backend-api", srv.URL+"/backend-api/f/conversation")

	var persisted string
	exec := NewChatGPTWebExecutor(&config.Config{}, func(_ context.Context, auth *cliproxyauth.Auth, updates map[string]any) (*cliproxyauth.Auth, error) {
		persisted, _ = updates["cookie"].(string)
		return auth, nil
	})
	auth := &cliproxyauth.Auth{ID: "auth", Metadata: map[string]any{"cookie": "__Secure-next-auth.session-token=tok"}}

	_, _, err := exec.acquire(t.Context(), auth)
	if err == nil || !strings.Contains(err.Error(), "sentinel prepare") {
		t.Fatalf("requirements error after cookie persistence = %v", err)
	}
	if !strings.Contains(persisted, "__cf_bm=rotated") {
		t.Fatalf("persisted cookie = %q", persisted)
	}
}

func TestChatGPTWebBuildUpstreamBodyStripsCatalogNamespace(t *testing.T) {
	exec := NewChatGPTWebExecutor(&config.Config{})
	body, model, err := exec.buildUpstreamBody(t.Context(), nil, cliproxyexecutor.Request{
		Model:   "chatgpt-web/gpt-5-5-thinking",
		Payload: []byte(`{"model":"chatgpt-web/gpt-5-5-thinking","input":"hello"}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("openai-response")})
	if err != nil {
		t.Fatalf("buildUpstreamBody() error = %v", err)
	}
	if model != "gpt-5-5-thinking" || gjson.GetBytes(body, "model").String() != "gpt-5-5-thinking" {
		t.Fatalf("upstream model/body = %q/%s", model, body)
	}
}

func TestChatGPTWebBuildUpstreamBodyPreservesTranslatedExplicitValue(t *testing.T) {
	exec := NewChatGPTWebExecutor(&config.Config{Payload: config.PayloadConfig{Default: []config.PayloadRule{{
		Models: []config.PayloadModelRule{{Name: "gpt-5-6-pro", Protocol: "codex"}},
		Params: map[string]any{"reasoning.effort": "low"},
	}}}})
	body, _, err := exec.buildUpstreamBody(t.Context(), nil, cliproxyexecutor.Request{
		Model:   "gpt-5-6-pro",
		Payload: []byte(`{"model":"gpt-5-6-pro","messages":[{"role":"user","content":"hello"}],"reasoning_effort":"high"}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("openai")})
	if err != nil {
		t.Fatal(err)
	}
	if got := gjson.GetBytes(body, "reasoning.effort").String(); got != "high" {
		t.Fatalf("translated explicit reasoning effort = %q, want high: %s", got, body)
	}
}

func TestChatGPTWebBuildUpstreamBodyRejectsBackgroundResponse(t *testing.T) {
	exec := NewChatGPTWebExecutor(&config.Config{})
	_, _, err := exec.buildUpstreamBody(t.Context(), nil, cliproxyexecutor.Request{
		Model:   "gpt-5-6-pro",
		Payload: []byte(`{"model":"gpt-5-6-pro","input":"hello","background":true}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse})
	if err == nil || !strings.Contains(err.Error(), "background responses are not supported") {
		t.Fatalf("buildUpstreamBody() error = %v", err)
	}
	var neutral interface{ AuthStateNeutral() bool }
	if !errors.As(err, &neutral) || !neutral.AuthStateNeutral() {
		t.Fatalf("background response error changed auth state: %v", err)
	}
}

func TestChatGPTWebBuildUpstreamBodyThinkingValidationIsAuthStateNeutral(t *testing.T) {
	exec := NewChatGPTWebExecutor(&config.Config{})
	_, _, err := exec.buildUpstreamBody(t.Context(), nil, cliproxyexecutor.Request{
		Model:   "chatgpt-web/gpt-5-6-pro(max)",
		Payload: []byte(`{"model":"chatgpt-web/gpt-5-6-pro","input":"hello"}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse})
	if err == nil {
		t.Fatal("unsupported thinking level succeeded")
	}
	var neutral interface{ AuthStateNeutral() bool }
	if !errors.As(err, &neutral) || !neutral.AuthStateNeutral() {
		t.Fatalf("thinking validation error changed auth state: %v", err)
	}
}

func TestConversationHeadersStableSessionID(t *testing.T) {
	t.Parallel()
	s := testSession()
	s.WebSessionID = "wsess-1"
	reqs := &chatgptweb.SentinelRequirements{Token: "sent", PrepareToken: "prep", ProofToken: "proof"}
	first, err := http.NewRequest(http.MethodPost, "https://chatgpt.com/backend-api/f/conversation", nil)
	if err != nil {
		t.Fatal(err)
	}
	second, err := http.NewRequest(http.MethodPost, "https://chatgpt.com/backend-api/f/conversation", nil)
	if err != nil {
		t.Fatal(err)
	}
	applyConversationHeaders(first, s, reqs, "/backend-api/f/conversation", nil)
	applyConversationHeaders(second, s, reqs, "/backend-api/f/conversation", nil)
	if got := first.Header.Get("OAI-Session-Id"); got != "wsess-1" {
		t.Fatalf("OAI-Session-Id = %q, want stored session id", got)
	}
	if got := second.Header.Get("OAI-Session-Id"); got != "wsess-1" {
		t.Fatalf("OAI-Session-Id = %q, want stored session id", got)
	}
	if first.Header.Get("x-oai-turn-trace-id") == "" || first.Header.Get("x-oai-turn-trace-id") == second.Header.Get("x-oai-turn-trace-id") {
		t.Fatal("x-oai-turn-trace-id was not unique per request")
	}
	if !strings.Contains(first.Header.Get("Sec-CH-UA"), `"Chromium"`) {
		t.Fatalf("Sec-CH-UA = %q, want chromium brand", first.Header.Get("Sec-CH-UA"))
	}
	if got := first.Header.Get("Sec-Fetch-Mode"); got != "cors" {
		t.Fatalf("Sec-Fetch-Mode = %q, want cors", got)
	}
	for header, want := range map[string]string{
		"OAI-Client-Build-Number": "9052945",
		"OAI-Client-Version":      "prod-e1d6f2820dd20c3bab36cc42e8668035bf87f7bc",
		"Priority":                "u=1, i",
	} {
		if got := first.Header.Get(header); got != want {
			t.Fatalf("%s = %q, want %q", header, got, want)
		}
	}
}

func TestConversationHeadersNoEcho(t *testing.T) {
	t.Parallel()
	req, err := http.NewRequest(http.MethodPost, "https://chatgpt.com/backend-api/f/conversation", nil)
	if err != nil {
		t.Fatal(err)
	}
	reqs := &chatgptweb.SentinelRequirements{Token: "sent", PrepareToken: "prep", ProofToken: "proof", Turnstile: "turn"}
	applyConversationHeaders(req, testSession(), reqs, "/backend-api/f/conversation", nil)
	if got := req.Header.Get("X-OpenAI-Target-Path"); got != "/backend-api/f/conversation" {
		t.Fatalf("target path = %q", got)
	}
	if got := req.Header.Get("X-OpenAI-Target-Route"); got != "/backend-api/f/conversation" {
		t.Fatalf("target route = %q", got)
	}
	if req.Header.Get("OAI-Session-Id") == "" || req.Header.Get("x-oai-turn-trace-id") == "" {
		t.Fatal("missing per-turn tracing headers")
	}
	if req.Header.Get("chatgpt-account-id") != "acct-1" {
		t.Fatal("missing account header")
	}
	if req.Header.Get("User-Agent") != testChatGPTWebUserAgent {
		t.Fatal("missing browser User-Agent")
	}
}

func TestStreamConversationOmitsUpstreamBodyFromError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Set-Cookie", "__Secure-next-auth.session-token=rotated-on-error; Path=/; HttpOnly")
		w.Header().Set("Retry-After", "30")
		w.WriteHeader(http.StatusForbidden)
		_, _ = fmt.Fprint(w, `{"detail":"cf challenge marker secret-body"}`)
	}))
	defer srv.Close()

	useChatGPTWebTestURLs(t, srv.URL, srv.URL, srv.URL)

	_, _, headers, err := streamConversation(context.Background(), srv.Client(), testSession(), nil, []byte(`{}`), "", nil, nil)
	if err == nil {
		t.Fatal("expected status error")
	}
	var se cliproxyexecutor.StatusError
	if !errors.As(err, &se) || se.StatusCode() != http.StatusForbidden {
		t.Fatalf("err = %v", err)
	}
	if strings.Contains(err.Error(), "secret-body") || strings.Contains(err.Error(), "cf challenge") {
		t.Fatalf("error leaked upstream body: %v", err)
	}
	if got := headers.Values("Set-Cookie"); len(got) != 1 || !strings.Contains(got[0], "rotated-on-error") {
		t.Fatalf("error response cookie headers = %v", got)
	}
	var provider interface{ Headers() http.Header }
	if !errors.As(err, &provider) {
		t.Fatalf("error omitted response headers: %v", err)
	}
	publicHeaders := provider.Headers()
	if publicHeaders.Get("Retry-After") != "30" || publicHeaders.Get("Set-Cookie") != "" {
		t.Fatalf("public error headers = %#v", publicHeaders)
	}
}

func TestStreamConversationReturnsTransportErrorWithoutResponse(t *testing.T) {
	want := errors.New("transport failed")
	client := &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return nil, want
	})}
	_, parser, headers, err := streamConversation(t.Context(), client, testSession(), nil, []byte(`{}`), "", nil, nil)
	if !errors.Is(err, want) {
		t.Fatalf("transport error = %v, want %v", err, want)
	}
	if parser != nil || headers != nil {
		t.Fatalf("transport failure returned parser %#v or headers %#v", parser, headers)
	}
	wrapped := wrapChatGPTWebResponseError(err)
	var neutral interface{ AuthStateNeutral() bool }
	var retryable interface{ Retryable() bool }
	if !errors.As(wrapped, &neutral) || !neutral.AuthStateNeutral() || errors.As(wrapped, &retryable) {
		t.Fatalf("pre-write transport failure was not neutral and retryable: %v", wrapped)
	}
}

func TestStreamConversationTreatsWroteRequestErrorAsAmbiguous(t *testing.T) {
	want := errors.New("write failed")
	client := &http.Client{Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		trace := httptrace.ContextClientTrace(req.Context())
		trace.WroteRequest(httptrace.WroteRequestInfo{Err: want})
		return nil, want
	})}
	_, _, _, err := streamConversation(t.Context(), client, testSession(), nil, []byte(`{}`), "", nil, nil)
	wrapped := wrapChatGPTWebResponseError(err)
	var neutral interface{ AuthStateNeutral() bool }
	var retryable interface{ Retryable() bool }
	if !errors.Is(wrapped, want) || !errors.As(wrapped, &neutral) || !neutral.AuthStateNeutral() || !errors.As(wrapped, &retryable) || retryable.Retryable() {
		t.Fatalf("ambiguous write error was not neutral and non-retryable: %v", wrapped)
	}
}

func TestStreamConversationClassifiesBadRequestWithoutLeakingBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = fmt.Fprint(w, `{"error":{"type":"invalid_request_error","code":"context_length_exceeded","message":"secret request content"}}`)
	}))
	t.Cleanup(srv.Close)
	useChatGPTWebTestURLs(t, srv.URL, srv.URL, srv.URL)

	_, _, _, err := streamConversation(t.Context(), srv.Client(), testSession(), nil, []byte(`{}`), "", nil, nil)
	var statusErr cliproxyexecutor.StatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode() != http.StatusBadRequest {
		t.Fatalf("bad request error = %v", err)
	}
	if !strings.Contains(err.Error(), "invalid_request_error") || !strings.Contains(err.Error(), "context_length_exceeded") {
		t.Fatalf("bad request classification = %v", err)
	}
	if strings.Contains(err.Error(), "secret request content") {
		t.Fatalf("bad request error leaked upstream message: %v", err)
	}
}

func TestStreamConversationCumulativeAndV1(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		switch r.URL.Query().Get("mode") {
		case "v1":
			fmt.Fprint(w, "event: delta_encoding\ndata: \"v1\"\n\n")
			fmt.Fprint(w, "data: {\"p\":\"/conversation_id\",\"o\":\"replace\",\"v\":\"conv-v1\"}\n\n")
			fmt.Fprint(w, "data: {\"p\":\"/message/author/role\",\"o\":\"replace\",\"v\":\"assistant\"}\n\n")
			fmt.Fprint(w, "data: {\"p\":\"/message/content/parts/0\",\"o\":\"append\",\"v\":\"po\"}\n\n")
			fmt.Fprint(w, "data: {\"p\":\"/message/content/parts/0\",\"o\":\"append\",\"v\":\"ng\"}\n\n")
			fmt.Fprint(w, "data: {\"p\":\"/message/metadata/finish_details\",\"o\":\"replace\",\"v\":{\"type\":\"stop\"}}\n\n")
			fmt.Fprint(w, "data: [DONE]\n\n")
		default:
			fmt.Fprint(w, "data: {\"conversation_id\":\"conv-1\",\"message\":{\"id\":\"m2\",\"author\":{\"role\":\"assistant\"},\"status\":\"in_progress\",\"content\":{\"parts\":[\"Hel\"]}}}\n\n")
			fmt.Fprint(w, "data: {\"conversation_id\":\"conv-1\",\"message\":{\"id\":\"m2\",\"author\":{\"role\":\"assistant\"},\"status\":\"finished_successfully\",\"content\":{\"parts\":[\"Hello world\"]},\"metadata\":{\"finish_details\":{\"type\":\"stop\"}}}}\n\n")
			fmt.Fprint(w, "data: [DONE]\n\n")
		}
	}))
	defer srv.Close()

	var deltas []string
	useChatGPTWebTestURLs(t, srv.URL, "", srv.URL+"/")
	text, parser, _, err := streamConversation(context.Background(), srv.Client(), testSession(), nil, []byte(`{}`), "", nil, func(d string) { deltas = append(deltas, d) })
	if err != nil {
		t.Fatal(err)
	}
	if text != "Hello world" || parser.ConversationID() != "conv-1" {
		t.Fatalf("text=%q conv=%q", text, parser.ConversationID())
	}
	if strings.Join(deltas, "") != "Hello world" {
		t.Fatalf("deltas = %q", deltas)
	}

	useChatGPTWebTestURLs(t, "", "", srv.URL+"/?mode=v1")
	deltas = nil
	text, parser, _, err = streamConversation(context.Background(), srv.Client(), testSession(), nil, []byte(`{}`), "", nil, func(d string) { deltas = append(deltas, d) })
	if err != nil {
		t.Fatal(err)
	}
	if text != "pong" || parser.ConversationID() != "conv-v1" {
		t.Fatalf("text=%q conv=%q", text, parser.ConversationID())
	}
}

func TestResumeConversationSuffixOnly(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/f/conversation/resume" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		if payload["offset"].(float64) == 0 {
			fmt.Fprint(w, "data: {\"conversation_id\":\"conv-9\",\"message\":{\"id\":\"m\",\"author\":{\"role\":\"assistant\"},\"status\":\"finished_successfully\",\"content\":{\"parts\":[\"Hello world, continued\"]}}}\n\n")
			fmt.Fprint(w, "data: [DONE]\n\n")
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	useChatGPTWebTestURLs(t, srv.URL, srv.URL, "")

	var deltas []string
	text, _, err := resumeConversation(context.Background(), srv.Client(), testSession(), nil, nil, "conv-9", "tok", "Hello", nil, func(d string) { deltas = append(deltas, d) }, nil)
	if err != nil {
		t.Fatal(err)
	}
	if text != "Hello world, continued" {
		t.Fatalf("text = %q", text)
	}
	if strings.Join(deltas, "") != " world, continued" {
		t.Fatalf("deltas = %q (must not replay)", deltas)
	}
}

func TestResumeConversationFollowsChainedHandoff(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload struct {
			Offset int `json:"offset"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		switch payload.Offset {
		case 0:
			w.Header().Add("Set-Cookie", "__cf_bm=resume-rotation; Path=/; HttpOnly")
			fmt.Fprint(w, "data: {\"conversation_id\":\"conv\",\"message\":{\"id\":\"m\",\"author\":{\"role\":\"assistant\"},\"status\":\"in_progress\",\"content\":{\"parts\":[\"Hello one\"]}}}\n\n")
			fmt.Fprint(w, "data: {\"type\":\"resume_conversation_token\",\"token\":\"tok-2\"}\n\n")
			fmt.Fprint(w, "data: {\"type\":\"stream_handoff\"}\n\n")
			fmt.Fprint(w, "data: [DONE]\n\n")
		case 1:
			if r.Header.Get("x-conduit-token") != "tok-2" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if !strings.Contains(r.Header.Get("Cookie"), "__cf_bm=resume-rotation") {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			fmt.Fprint(w, "data: {\"conversation_id\":\"conv\",\"message\":{\"id\":\"m\",\"author\":{\"role\":\"assistant\"},\"status\":\"finished_successfully\",\"content\":{\"parts\":[\"Hello one two\"]},\"metadata\":{\"finish_details\":{\"type\":\"stop\"}}}}\n\n")
			fmt.Fprint(w, "data: [DONE]\n\n")
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)

	useChatGPTWebTestURLs(t, srv.URL, srv.URL, "")

	var deltas []string
	text, _, err := resumeConversation(context.Background(), srv.Client(), testSession(), nil, nil, "conv", "tok-1", "Hello", nil, func(delta string) {
		deltas = append(deltas, delta)
	}, func(current *chatgptweb.Session, setCookies []string) (*chatgptweb.Session, error) {
		return testSessionWithCookie(current, chatgptweb.MergeRefreshedCookie(current.Cookie, setCookies)), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if text != "Hello one two" {
		t.Fatalf("text = %q", text)
	}
	if got := strings.Join(deltas, ""); got != " one two" {
		t.Fatalf("deltas = %q", got)
	}
}

func TestResumeConversationStopsWhenSessionTokenIsDeleted(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Set-Cookie", "__Secure-next-auth.session-token=; Max-Age=0; Path=/")
		fmt.Fprint(w, "data: {\"conversation_id\":\"conv\",\"message\":{\"id\":\"m\",\"author\":{\"role\":\"assistant\"},\"status\":\"finished_successfully\",\"content\":{\"parts\":[\"Hello world\"]}}}\n\ndata: [DONE]\n\n")
	}))
	t.Cleanup(srv.Close)
	useChatGPTWebTestURLs(t, srv.URL, srv.URL, "")

	_, _, err := resumeConversation(t.Context(), srv.Client(), testSession(), nil, nil, "conv", "tok", "Hello", nil, nil, func(current *chatgptweb.Session, setCookies []string) (*chatgptweb.Session, error) {
		return testSessionWithCookie(current, chatgptweb.MergeRefreshedCookie(current.Cookie, setCookies)), nil
	})
	var statusErr cliproxyexecutor.StatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode() != http.StatusUnauthorized {
		t.Fatalf("session-token deletion error = %v", err)
	}
	var neutral interface{ AuthStateNeutral() bool }
	var retryable interface{ Retryable() bool }
	if !errors.As(err, &neutral) || !neutral.AuthStateNeutral() || !errors.As(err, &retryable) || retryable.Retryable() {
		t.Fatalf("session-token deletion was not auth-neutral and non-retryable: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("session-token deletion made %d resume attempts", calls.Load())
	}
}

func TestResumeConversationPreservesV1StateAcrossHandoff(t *testing.T) {
	var polls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		switch {
		case r.URL.Path == "/initial":
			fmt.Fprint(w, "event: delta_encoding\ndata: \"v1\"\n\n")
			fmt.Fprint(w, "data: {\"p\":\"/message/author/role\",\"o\":\"replace\",\"v\":\"assistant\"}\n\n")
			fmt.Fprint(w, "data: {\"p\":\"\",\"o\":\"patch\",\"v\":[{\"p\":\"/conversation_id\",\"o\":\"replace\",\"v\":\"conv\"},{\"p\":\"/message/content/parts/0\",\"o\":\"append\",\"v\":\"Hel\"}]}\n\n")
			fmt.Fprint(w, "data: {\"type\":\"resume_conversation_token\",\"token\":\"tok\"}\n\n")
			fmt.Fprint(w, "data: {\"type\":\"stream_handoff\"}\n\n")
			fmt.Fprint(w, "data: [DONE]\n\n")
		case r.URL.Path == "/f/conversation/resume":
			fmt.Fprint(w, "event: delta_encoding\ndata: \"v1\"\n\n")
			fmt.Fprint(w, "data: {\"v\":[{\"v\":\"conv\"},{\"v\":\"lo\"}]}\n\n")
			fmt.Fprint(w, "data: {\"p\":\"/message/metadata/finish_details\",\"o\":\"replace\",\"v\":{\"type\":\"max_tokens\"}}\n\n")
			fmt.Fprint(w, "data: [DONE]\n\n")
		case strings.HasPrefix(r.URL.Path, "/conversation/"):
			polls.Add(1)
			w.WriteHeader(http.StatusNotFound)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)

	useChatGPTWebTestURLs(t, srv.URL, srv.URL, srv.URL+"/initial")

	initialText, parser, _, err := streamConversation(context.Background(), srv.Client(), testSession(), nil, []byte(`{}`), "", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if initialText != "Hel" || !parser.Handoff() {
		t.Fatalf("initial text=%q handoff=%v", initialText, parser.Handoff())
	}
	var deltas []string
	text, finalParser, err := resumeConversation(context.Background(), srv.Client(), testSession(), nil, parser, parser.ConversationID(), parser.ResumeToken(), initialText, nil, func(delta string) {
		deltas = append(deltas, delta)
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if text != "Hello" || strings.Join(deltas, "") != "lo" {
		t.Fatalf("text=%q deltas=%q", text, deltas)
	}
	if got := chatGPTWebFinishType(finalParser); got != "max_tokens" {
		t.Fatalf("resumed finish type = %q", got)
	}
	if polls.Load() != 0 {
		t.Fatalf("temporary chat attempted persisted-conversation polling %d times", polls.Load())
	}
}

func TestSSEControlEnvelopeRetainsConversationID(t *testing.T) {
	t.Parallel()
	parser := &helps.ChatGPTWebSSEParser{}
	_, _, err := parser.FeedLine([]byte(`data: {"type":"resume_conversation_token","conversation_id":"conv-control","token":"tok"}`))
	if err != nil {
		t.Fatal(err)
	}
	if parser.ConversationID() != "conv-control" || parser.ResumeToken() != "tok" {
		t.Fatalf("control envelope metadata: conversation=%q token=%q", parser.ConversationID(), parser.ResumeToken())
	}
}

func TestSSEJoinsDataFieldsAtEventBoundary(t *testing.T) {
	t.Parallel()
	parser := &helps.ChatGPTWebSSEParser{}
	var text strings.Builder
	events := "data: {\"conversation_id\":\"conv\",\n" +
		"data: \"message\":{\"id\":\"m\",\"author\":{\"role\":\"assistant\"},\"status\":\"finished_successfully\",\"content\":{\"parts\":[\"answer\"]},\"metadata\":{\"finish_details\":{\"type\":\"stop\"}}}}\n\n" +
		"data: [DONE]\n\n"
	if err := helps.DrainChatGPTWebSSE(strings.NewReader(events), parser, func(delta string) { text.WriteString(delta) }); err != nil {
		t.Fatal(err)
	}
	if text.String() != "answer" || parser.ConversationID() != "conv" || !parser.Terminal() || !parser.Finished() {
		t.Fatalf("joined SSE event = text %q conversation %q terminal %v finished %v", text.String(), parser.ConversationID(), parser.Terminal(), parser.Finished())
	}
}

func TestSSEWeightZeroReplayDoesNotReplaceLiveResponse(t *testing.T) {
	t.Parallel()
	parser := &helps.ChatGPTWebSSEParser{}
	for _, line := range []string{
		`data: {"conversation_id":"conv","message":{"id":"current","author":{"role":"assistant"},"status":"in_progress","content":{"parts":["current"]}}}`,
		`data: {"conversation_id":"conv","message":{"id":"old","author":{"role":"assistant"},"status":"finished_successfully","content":{"parts":["old replay"]},"metadata":{"weight":0,"finish_details":{"type":"stop"}}}}`,
	} {
		if _, _, err := parser.FeedLine([]byte(line)); err != nil {
			t.Fatal(err)
		}
	}
	if parser.Text() != "current" || parser.Finished() {
		t.Fatalf("replay changed live response: text=%q finished=%v", parser.Text(), parser.Finished())
	}
}

func TestSSERepeatedInstantMessageUpdatesText(t *testing.T) {
	t.Parallel()
	parser := &helps.ChatGPTWebSSEParser{}
	var text strings.Builder
	for _, line := range []string{
		`data: {"conversation_id":"conv","message":{"id":"m","author":{"role":"assistant"},"status":"finished_successfully","content":{"parts":["Hel"]}}}`,
		`data: {"conversation_id":"conv","message":{"id":"m","author":{"role":"assistant"},"status":"finished_successfully","content":{"parts":["Hello"]},"metadata":{"finish_details":{"type":"stop"}}}}`,
		`data: [DONE]`,
	} {
		delta, _, err := parser.FeedLine([]byte(line))
		if err != nil {
			t.Fatal(err)
		}
		text.WriteString(delta)
	}
	if text.String() != "Hello" || parser.Text() != "Hello" || !parser.Finished() {
		t.Fatalf("repeated instant response state: deltas=%q text=%q finished=%v", text.String(), parser.Text(), parser.Finished())
	}
}

func TestSSEInProgressSnapshotClearsCompletion(t *testing.T) {
	t.Parallel()
	parser := &helps.ChatGPTWebSSEParser{}
	for _, line := range []string{
		`data: {"conversation_id":"conv","message":{"id":"m","author":{"role":"assistant"},"status":"finished_successfully","content":{"parts":["answer"]},"metadata":{"finish_details":{"type":"stop"}}}}`,
		`data: {"conversation_id":"conv","message":{"id":"m","author":{"role":"assistant"},"status":"in_progress","content":{"parts":["answer"]}}}`,
	} {
		if _, _, err := parser.FeedLine([]byte(line)); err != nil {
			t.Fatal(err)
		}
	}
	if parser.Finished() || parser.FinishType() != "" {
		t.Fatalf("in-progress snapshot retained completion: finished=%v type=%q", parser.Finished(), parser.FinishType())
	}
}

func TestSSEFailedStatusOverridesFinishDetails(t *testing.T) {
	t.Parallel()
	for name, lines := range map[string][]string{
		"whole message": {
			`data: {"conversation_id":"conv","message":{"id":"m","author":{"role":"assistant"},"status":"failed","content":{"parts":["partial"]},"metadata":{"finish_details":{"type":"stop"}}}}`,
		},
		"v1 message": {
			`event: delta_encoding`,
			`data: "v1"`,
			`data: {"p":"/message","o":"replace","v":{"id":"m","author":{"role":"assistant"},"status":"failed","content":{"parts":["partial"]},"metadata":{"finish_details":{"type":"stop"}}}}`,
		},
		"v1 patches": {
			`event: delta_encoding`,
			`data: "v1"`,
			`data: {"p":"/message/author/role","o":"replace","v":"assistant"}`,
			`data: {"p":"/message/content/parts/0","o":"append","v":"partial"}`,
			`data: {"p":"/message/status","o":"replace","v":"failed"}`,
			`data: {"p":"/message/metadata/finish_details","o":"replace","v":{"type":"stop"}}`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			parser := &helps.ChatGPTWebSSEParser{}
			for _, line := range lines {
				if _, _, err := parser.FeedLine([]byte(line)); err != nil {
					t.Fatal(err)
				}
			}
			if parser.Finished() || parser.FinishType() != "" {
				t.Fatalf("failed assistant marked finished: finished=%v type=%q", parser.Finished(), parser.FinishType())
			}
		})
	}
}

func TestSSEFailedFirstSnapshotIsNotEmittedOnDone(t *testing.T) {
	t.Parallel()
	parser := &helps.ChatGPTWebSSEParser{}
	delta, terminal, err := parser.FeedLine([]byte(`data: {"conversation_id":"conv","message":{"id":"m","author":{"role":"assistant"},"status":"failed","content":{"parts":["failed partial"]},"metadata":{"finish_details":{"type":"stop"}}}}`))
	if err != nil || delta != "" || terminal {
		t.Fatalf("failed snapshot result: delta=%q terminal=%v error=%v", delta, terminal, err)
	}
	delta, terminal, err = parser.FeedLine([]byte(`data: [DONE]`))
	if err == nil || !strings.Contains(err.Error(), "without an assistant response") {
		t.Fatalf("failed snapshot DONE error = %v", err)
	}
	if delta != "" || terminal || parser.Text() != "" || parser.Finished() {
		t.Fatalf("failed snapshot state: delta=%q terminal=%v text=%q finished=%v", delta, terminal, parser.Text(), parser.Finished())
	}
}

func TestSSEFailedSnapshotClearsPreviousCompletion(t *testing.T) {
	t.Parallel()
	parser := &helps.ChatGPTWebSSEParser{}
	for _, line := range []string{
		`data: {"conversation_id":"conv","message":{"id":"m","author":{"role":"assistant"},"status":"finished_successfully","content":{"parts":["stale answer"]},"metadata":{"finish_details":{"type":"stop"}}}}`,
		`data: {"conversation_id":"conv","message":{"id":"m","author":{"role":"assistant"},"status":"failed","content":{"parts":["failed partial"]}}}`,
	} {
		if delta, terminal, err := parser.FeedLine([]byte(line)); err != nil || delta != "" || terminal {
			t.Fatalf("snapshot result: delta=%q terminal=%v error=%v", delta, terminal, err)
		}
	}
	if parser.Finished() || parser.FinishType() != "" {
		t.Fatalf("failed snapshot retained completion: finished=%v type=%q", parser.Finished(), parser.FinishType())
	}
	delta, terminal, err := parser.FeedLine([]byte(`data: [DONE]`))
	if err != nil || delta != "" || !terminal || parser.Text() != "" || parser.Finished() {
		t.Fatalf("DONE state: delta=%q terminal=%v text=%q finished=%v error=%v", delta, terminal, parser.Text(), parser.Finished(), err)
	}
}

func TestSSEV1IgnoresNonAssistantMessagePatches(t *testing.T) {
	t.Parallel()
	parser := &helps.ChatGPTWebSSEParser{}
	for _, line := range []string{
		`event: delta_encoding`,
		`data: "v1"`,
		`data: {"p":"/message/author/role","o":"replace","v":"tool"}`,
		`data: {"p":"/message","o":"replace","v":{"id":"tool","author":{"role":"tool"},"status":"in_progress","content":{"parts":["secret tool output"]}}}`,
		`data: {"p":"/message/content/parts/0","o":"append","v":" leaked"}`,
		`data: {"p":"","o":"","v":" root leaked"}`,
		`data: {"p":"/message/metadata/finish_details","o":"replace","v":{"type":"stop"}}`,
	} {
		if _, _, err := parser.FeedLine([]byte(line)); err != nil {
			t.Fatal(err)
		}
	}
	if parser.Text() != "" || parser.Finished() {
		t.Fatalf("non-assistant v1 message was exposed: text=%q finished=%v", parser.Text(), parser.Finished())
	}
	delta, _, err := parser.FeedLine([]byte(`data: {"p":"/message","o":"replace","v":{"id":"assistant","author":{"role":"assistant"},"status":"finished_successfully","content":{"parts":["answer"]},"metadata":{"finish_details":{"type":"stop"}}}}`))
	if err != nil {
		t.Fatal(err)
	}
	if delta != "answer" || parser.Text() != "answer" || !parser.Finished() {
		t.Fatalf("assistant v1 message was not restored: delta=%q text=%q finished=%v", delta, parser.Text(), parser.Finished())
	}
}

func TestSSEV1RolelessAppendDefaultsToAssistant(t *testing.T) {
	t.Parallel()
	parser := &helps.ChatGPTWebSSEParser{}
	var text strings.Builder
	for _, line := range []string{
		`event: delta_encoding`,
		`data: "v1"`,
		`data: {"p":"","o":"","v":{"conversation_id":"conv","message":{"id":"user","author":{"role":"user"},"content":{"parts":["question"]}}}}`,
		`data: {"p":"/message/content/parts/0","o":"append","v":"answer"}`,
		`data: {"p":"/message/metadata/finish_details","o":"replace","v":{"type":"stop"}}`,
		`data: [DONE]`,
	} {
		delta, _, err := parser.FeedLine([]byte(line))
		if err != nil {
			t.Fatal(err)
		}
		text.WriteString(delta)
	}
	if text.String() != "answer" || parser.Text() != "answer" || !parser.Finished() {
		t.Fatalf("roleless v1 append state: deltas=%q text=%q finished=%v", text.String(), parser.Text(), parser.Finished())
	}
}

func TestSSEV1ExplicitRoleClearsDefaultedAssistantContent(t *testing.T) {
	t.Parallel()
	parser := &helps.ChatGPTWebSSEParser{}
	for _, line := range []string{
		`event: delta_encoding`,
		`data: "v1"`,
	} {
		if _, _, err := parser.FeedLine([]byte(line)); err != nil {
			t.Fatal(err)
		}
	}
	if delta, _, err := parser.FeedLine([]byte(`data: {"p":"/message/content/parts/0","o":"append","v":"secret"}`)); err != nil || delta != "secret" {
		t.Fatalf("pre-role content result: delta=%q error=%v", delta, err)
	}
	if _, _, err := parser.FeedLine([]byte(`data: {"p":"/message/author/role","o":"replace","v":"tool"}`)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := parser.FeedLine([]byte(`data: [DONE]`)); err == nil || !strings.Contains(err.Error(), "without an assistant response") {
		t.Fatalf("non-assistant completion error = %v", err)
	}
	if parser.Text() != "" || parser.SeenAssistant() {
		t.Fatalf("non-assistant content was exposed: text=%q seen=%v", parser.Text(), parser.SeenAssistant())
	}
}

func TestSSEV1RolelessRootStringDefaultsToAssistant(t *testing.T) {
	t.Parallel()
	parser := &helps.ChatGPTWebSSEParser{}
	var text strings.Builder
	for _, line := range []string{
		`event: delta_encoding`,
		`data: "v1"`,
		`data: {"p":"","o":"","v":"answer"}`,
		`data: {"p":"/message/metadata/finish_details","o":"replace","v":{"type":"stop"}}`,
		`data: [DONE]`,
	} {
		delta, _, err := parser.FeedLine([]byte(line))
		if err != nil {
			t.Fatal(err)
		}
		text.WriteString(delta)
	}
	if text.String() != "answer" || parser.Text() != "answer" || !parser.Finished() {
		t.Fatalf("roleless v1 root state: deltas=%q text=%q finished=%v", text.String(), parser.Text(), parser.Finished())
	}
}

func TestSSEV1WeightZeroReplayDoesNotClaimRole(t *testing.T) {
	t.Parallel()
	parser := &helps.ChatGPTWebSSEParser{}
	var text strings.Builder
	for _, line := range []string{
		`event: delta_encoding`,
		`data: "v1"`,
		`data: {"p":"/message","o":"replace","v":{"id":"old","author":{"role":"assistant"},"status":"finished_successfully","content":{"parts":["old replay"]},"metadata":{"weight":0,"finish_details":{"type":"stop"}}}}`,
		`data: {"p":"/message/author/role","o":"replace","v":"assistant"}`,
		`data: {"p":"/message/content/parts/0","o":"append","v":"fresh"}`,
		`data: {"p":"/message/metadata/finish_details","o":"replace","v":{"type":"stop"}}`,
		`data: [DONE]`,
	} {
		delta, _, err := parser.FeedLine([]byte(line))
		if err != nil {
			t.Fatal(err)
		}
		text.WriteString(delta)
	}
	if text.String() != "fresh" || parser.Text() != "fresh" || !parser.Finished() {
		t.Fatalf("v1 replay changed current response: deltas=%q text=%q finished=%v", text.String(), parser.Text(), parser.Finished())
	}
}

func TestSSEV1NonAssistantReplacementClearsAssistantState(t *testing.T) {
	t.Parallel()
	parser := &helps.ChatGPTWebSSEParser{}
	for _, line := range []string{
		`event: delta_encoding`,
		`data: "v1"`,
		`data: {"p":"/message","o":"replace","v":{"id":"assistant","author":{"role":"assistant"},"status":"finished_successfully","content":{"parts":["answer"]},"metadata":{"finish_details":{"type":"stop"}}}}`,
		`data: {"p":"/message","o":"replace","v":{"id":"tool","author":{"role":"tool"},"status":"finished_successfully","content":{"parts":["secret"]}}}`,
	} {
		if _, _, err := parser.FeedLine([]byte(line)); err != nil {
			t.Fatal(err)
		}
	}
	if parser.Text() != "" || parser.SeenAssistant() || parser.Finished() || !parser.Diverged() {
		t.Fatalf("non-assistant replacement state: text=%q seen=%v finished=%v diverged=%v", parser.Text(), parser.SeenAssistant(), parser.Finished(), parser.Diverged())
	}
	if delta, _, err := parser.FeedLine([]byte(`data: {"p":"/message/content/parts/0","o":"append","v":" leaked"}`)); err != nil || delta != "" || parser.Text() != "" {
		t.Fatalf("non-assistant append state: delta=%q text=%q error=%v", delta, parser.Text(), err)
	}
}

func TestSSEV1NonAssistantRootClearsAssistantState(t *testing.T) {
	t.Parallel()
	parser := &helps.ChatGPTWebSSEParser{}
	for _, line := range []string{
		`event: delta_encoding`,
		`data: "v1"`,
		`data: {"p":"/message","o":"replace","v":{"id":"assistant","author":{"role":"assistant"},"status":"finished_successfully","content":{"parts":["answer"]},"metadata":{"finish_details":{"type":"stop"}}}}`,
		`data: {"p":"","o":"","v":{"conversation_id":"conv","message":{"id":"tool","author":{"role":"tool"},"status":"finished_successfully","content":{"parts":["secret"]}}}}`,
	} {
		if _, _, err := parser.FeedLine([]byte(line)); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := parser.FeedLine([]byte(`data: [DONE]`)); err == nil || !strings.Contains(err.Error(), "without an assistant response") {
		t.Fatalf("non-assistant root completion error = %v", err)
	}
	if parser.ConversationID() != "conv" || parser.Text() != "" || parser.SeenAssistant() || parser.Finished() || !parser.Diverged() {
		t.Fatalf("non-assistant root state: conversation=%q text=%q seen=%v finished=%v diverged=%v", parser.ConversationID(), parser.Text(), parser.SeenAssistant(), parser.Finished(), parser.Diverged())
	}
}

func TestSSEV1NonAssistantRolePatchClearsAssistantState(t *testing.T) {
	t.Parallel()
	parser := &helps.ChatGPTWebSSEParser{}
	for _, line := range []string{
		`event: delta_encoding`,
		`data: "v1"`,
		`data: {"p":"/message","o":"replace","v":{"id":"assistant","author":{"role":"assistant"},"status":"finished_successfully","content":{"parts":["answer"]},"metadata":{"finish_details":{"type":"stop"}}}}`,
		`data: {"p":"/message/author/role","o":"replace","v":"tool"}`,
	} {
		if _, _, err := parser.FeedLine([]byte(line)); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := parser.FeedLine([]byte(`data: [DONE]`)); err == nil || !strings.Contains(err.Error(), "without an assistant response") {
		t.Fatalf("non-assistant role completion error = %v", err)
	}
	if parser.Text() != "" || parser.SeenAssistant() || parser.Finished() || !parser.Diverged() {
		t.Fatalf("non-assistant role state: text=%q seen=%v finished=%v diverged=%v", parser.Text(), parser.SeenAssistant(), parser.Finished(), parser.Diverged())
	}
}

func TestSSEV1FinishDetailsRemovalDoesNotFinish(t *testing.T) {
	t.Parallel()
	parser := &helps.ChatGPTWebSSEParser{}
	for _, line := range []string{
		`event: delta_encoding`,
		`data: "v1"`,
		`data: {"p":"/message/author/role","o":"replace","v":"assistant"}`,
		`data: {"p":"/message/content/parts/0","o":"append","v":"answer"}`,
		`data: {"p":"/message/metadata/finish_details","o":"remove","v":{"type":"stop"}}`,
	} {
		if _, _, err := parser.FeedLine([]byte(line)); err != nil {
			t.Fatal(err)
		}
	}
	if parser.Finished() || parser.FinishType() != "" {
		t.Fatalf("finish-details removal marked completion: finished=%v type=%q", parser.Finished(), parser.FinishType())
	}
}

func TestSSEV1StatusReplacementClearsCompletion(t *testing.T) {
	t.Parallel()
	parser := &helps.ChatGPTWebSSEParser{}
	for _, line := range []string{
		`event: delta_encoding`,
		`data: "v1"`,
		`data: {"p":"/message/author/role","o":"replace","v":"assistant"}`,
		`data: {"p":"/message/content/parts/0","o":"append","v":"answer"}`,
		`data: {"p":"/message/status","o":"replace","v":"finished_successfully"}`,
		`data: {"p":"/message/status","o":"replace","v":"in_progress"}`,
	} {
		if _, _, err := parser.FeedLine([]byte(line)); err != nil {
			t.Fatal(err)
		}
	}
	if parser.Finished() {
		t.Fatal("unfinished replacement retained completed status")
	}
}

func TestSSEV1CompletionStatusBeforeContent(t *testing.T) {
	t.Parallel()
	parser := &helps.ChatGPTWebSSEParser{}
	var text strings.Builder
	for _, line := range []string{
		`event: delta_encoding`,
		`data: "v1"`,
		`data: {"p":"/message/author/role","o":"replace","v":"assistant"}`,
		`data: {"p":"/message/status","o":"replace","v":"finished_successfully"}`,
		`data: {"p":"/message/content/parts/0","o":"append","v":"answer"}`,
		`data: [DONE]`,
	} {
		delta, _, err := parser.FeedLine([]byte(line))
		if err != nil {
			t.Fatal(err)
		}
		text.WriteString(delta)
	}
	if text.String() != "answer" || parser.Text() != "answer" || !parser.Finished() {
		t.Fatalf("status-before-content state: delta=%q text=%q finished=%v", text.String(), parser.Text(), parser.Finished())
	}
}

func TestSSEV1MessageReplacementClearsCompletion(t *testing.T) {
	t.Parallel()
	parser := &helps.ChatGPTWebSSEParser{}
	for _, line := range []string{
		`event: delta_encoding`,
		`data: "v1"`,
		`data: {"p":"/message","o":"replace","v":{"id":"m","author":{"role":"assistant"},"status":"finished_successfully","content":{"parts":["answer"]},"metadata":{"finish_details":{"type":"stop"}}}}`,
		`data: {"p":"/message","o":"replace","v":{"id":"m","author":{"role":"assistant"},"status":"in_progress","content":{"parts":["answer"]}}}`,
	} {
		if _, _, err := parser.FeedLine([]byte(line)); err != nil {
			t.Fatal(err)
		}
	}
	if parser.Finished() || parser.FinishType() != "" {
		t.Fatalf("unfinished message replacement retained completion: finished=%v type=%q", parser.Finished(), parser.FinishType())
	}
}

func TestSSERejectsNonTextAssistantParts(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"whole message": `data: {"message":{"id":"m","author":{"role":"assistant"},"status":"finished_successfully","content":{"parts":[{"text":"answer"}]}}}`,
		"v1 message":    `data: {"p":"/message","o":"replace","v":{"id":"m","author":{"role":"assistant"},"status":"finished_successfully","content":{"parts":[{"text":"answer"}]}}}`,
	}
	for name, event := range cases {
		t.Run(name, func(t *testing.T) {
			parser := &helps.ChatGPTWebSSEParser{}
			if name == "v1 message" {
				if _, _, err := parser.FeedLine([]byte(`event: delta_encoding`)); err != nil {
					t.Fatal(err)
				}
				if _, _, err := parser.FeedLine([]byte(`data: "v1"`)); err != nil {
					t.Fatal(err)
				}
			}
			if _, _, err := parser.FeedLine([]byte(event)); err == nil || !strings.Contains(err.Error(), "assistant content part 0 is not text") {
				t.Fatalf("malformed assistant part error = %v", err)
			}
		})
	}
}

func TestSSEFinishDetailsRequiresType(t *testing.T) {
	t.Parallel()
	parser := &helps.ChatGPTWebSSEParser{}
	if _, _, err := parser.FeedLine([]byte(`data: {"message":{"id":"m","author":{"role":"assistant"},"status":"in_progress","content":{"parts":["answer"]},"metadata":{"finish_details":{}}}}`)); err != nil {
		t.Fatal(err)
	}
	if parser.Finished() {
		t.Fatal("empty whole-message finish details marked completion")
	}

	v1 := &helps.ChatGPTWebSSEParser{}
	for _, line := range []string{
		`event: delta_encoding`,
		`data: "v1"`,
		`data: {"p":"/message/author/role","o":"replace","v":"assistant"}`,
		`data: {"p":"/message/content/parts/0","o":"append","v":"answer"}`,
	} {
		if _, _, err := v1.FeedLine([]byte(line)); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := v1.FeedLine([]byte(`data: {"p":"/message/metadata/finish_details","o":"replace","v":{}}`)); err == nil {
		t.Fatal("empty v1 finish details were accepted")
	}
	if v1.Finished() {
		t.Fatal("empty v1 finish details marked completion")
	}
}

func TestResumeOffsetPrefixMismatchKeepsBaseline(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"conversation_id\":\"c\",\"message\":{\"id\":\"m\",\"author\":{\"role\":\"assistant\"},\"status\":\"in_progress\",\"content\":{\"parts\":[\"Different text entirely\"]}}}\n\n")
		fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	defer srv.Close()

	useChatGPTWebTestURLs(t, srv.URL, srv.URL, "")

	previous := &helps.ChatGPTWebSSEParser{}
	if _, _, err := previous.FeedLine([]byte(`data: {"conversation_id":"c","message":{"id":"m","author":{"role":"assistant"},"status":"in_progress","content":{"parts":["Hello"]}}}`)); err != nil {
		t.Fatal(err)
	}
	var deltas []string
	text, completed, returned, _, err := resumeOffset(context.Background(), srv.Client(), testSession(), nil, previous, "c", "tok", 0, "Hello", nil, func(d string) { deltas = append(deltas, d) })
	if err != nil {
		t.Fatal(err)
	}
	if text != "Hello" {
		t.Fatalf("mismatched cumulative text must not replace the baseline, got %q", text)
	}
	if len(deltas) != 0 {
		t.Fatalf("mismatched text must not emit deltas: %q", deltas)
	}
	if completed {
		t.Fatal("prefix mismatch must force another offset, not accept the offset")
	}
	if returned != previous {
		t.Fatal("prefix mismatch replaced the continuation parser state")
	}
}

func TestResumeOffsetMalformedStreamErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {not json}\n\n")
	}))
	defer srv.Close()

	useChatGPTWebTestURLs(t, srv.URL, srv.URL, "")

	text, completed, _, _, err := resumeOffset(context.Background(), srv.Client(), testSession(), nil, nil, "c", "tok", 0, "Hello", nil, nil)
	if err == nil {
		t.Fatal("malformed resume stream must error")
	}
	if completed || text != "Hello" {
		t.Fatalf("text=%q completed=%v", text, completed)
	}
}

func TestResumeConversationDoesNotPollTemporaryChat(t *testing.T) {
	var polls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/f/conversation/resume" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if !strings.HasPrefix(r.URL.Path, "/conversation/") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		var status string
		if polls.Add(1) == 1 {
			status = `"in_progress"`
		} else {
			status = `"finished_successfully"`
		}
		fmt.Fprintf(w, `{"current_node":"b","mapping":{"b":{"message":{"author":{"role":"assistant"},"status":%s,"content":{"parts":["Hello polled"]}}}}}`, status)
	}))
	defer srv.Close()

	useChatGPTWebTestURLs(t, srv.URL, srv.URL, "")

	_, _, err := resumeConversation(context.Background(), srv.Client(), testSession(), nil, nil, "conv-p", "tok", "Hello", nil, nil, nil)
	var statusErr cliproxyexecutor.StatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode() != http.StatusBadGateway {
		t.Fatalf("expected continuation failure status 502, got %v", err)
	}
	if polls.Load() != 0 {
		t.Fatalf("temporary chat attempted persisted-conversation polling %d times", polls.Load())
	}
}

func TestChatGPTWebContinuationErrorStatusCode(t *testing.T) {
	for _, test := range []struct {
		name  string
		cause error
		want  int
	}{
		{name: "unauthorized", cause: fmt.Errorf("resume: %w", &statusError{code: http.StatusUnauthorized}), want: http.StatusUnauthorized},
		{name: "forbidden", cause: fmt.Errorf("resume: %w", &statusError{code: http.StatusForbidden}), want: http.StatusForbidden},
		{name: "rate limited", cause: fmt.Errorf("resume: %w", &statusError{code: http.StatusTooManyRequests}), want: http.StatusTooManyRequests},
		{name: "internal not found", cause: fmt.Errorf("resume: %w", &statusError{code: http.StatusNotFound}), want: http.StatusBadGateway},
		{name: "generic", cause: errors.New("resume failed"), want: http.StatusBadGateway},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := &chatGPTWebContinuationError{cause: test.cause}
			if got := err.StatusCode(); got != test.want {
				t.Fatalf("status = %d, want %d", got, test.want)
			}
			if !err.AuthStateNeutral() || err.Retryable() {
				t.Fatal("accepted continuation failure was not auth-neutral and non-retryable")
			}
		})
	}
}

func TestWrapChatGPTWebResponseErrorMapsUpstreamNotFound(t *testing.T) {
	err := wrapChatGPTWebResponseError(&statusError{code: http.StatusNotFound})
	var statusErr cliproxyexecutor.StatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode() != http.StatusBadGateway {
		t.Fatalf("wrapped upstream not-found error = %v", err)
	}
}

func TestResumeConversationStopsOnTerminalStatus(t *testing.T) {
	for _, code := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusTooManyRequests} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			var calls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				w.WriteHeader(code)
			}))
			t.Cleanup(srv.Close)
			useChatGPTWebTestURLs(t, srv.URL, srv.URL, "")

			_, _, err := resumeConversation(t.Context(), srv.Client(), testSession(), nil, nil, "conv", "tok", "Hello", nil, nil, nil)
			var statusErr cliproxyexecutor.StatusError
			if !errors.As(err, &statusErr) || statusErr.StatusCode() != code {
				t.Fatalf("terminal status error = %v", err)
			}
			if calls.Load() != 1 {
				t.Fatalf("terminal status %d made %d resume attempts", code, calls.Load())
			}
		})
	}
}

func TestPollConversationRespectsContextCancel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/conversation/") {
			fmt.Fprint(w, `{"current_node":"b","mapping":{"b":{"message":{"author":{"role":"assistant"},"status":"in_progress","content":{"parts":["Hello partial"]}}}}}`)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	useChatGPTWebTestURLs(t, srv.URL, srv.URL, "")

	origPoll := pollInterval
	pollInterval = 10 * time.Millisecond
	defer func() { pollInterval = origPoll }()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_, err := pollConversation(ctx, srv.Client(), testSession(), "conv-p", "Hello", nil, nil)
	if err == nil {
		t.Fatal("polling must stop when the caller context ends")
	}
}

func TestCountTokensUsesModelTokenizer(t *testing.T) {
	t.Parallel()
	e := NewChatGPTWebExecutor(&config.Config{})
	count := func(input string) int {
		req := cliproxyexecutor.Request{
			Model:   "gpt-5-6-pro",
			Payload: []byte(fmt.Sprintf(`{"model":"gpt-5-6-pro","input":%s}`, input)),
		}
		resp, err := e.CountTokens(context.Background(), nil, req, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse})
		if err != nil {
			t.Fatal(err)
		}
		var out struct {
			Object      string `json:"object"`
			InputTokens int    `json:"input_tokens"`
		}
		if err := json.Unmarshal(resp.Payload, &out); err != nil {
			t.Fatal(err)
		}
		if out.Object != "response.input_tokens" {
			t.Fatalf("token count response does not use OpenAI Responses format: %s", resp.Payload)
		}
		return out.InputTokens
	}
	if got := count(`""`); got != 0 {
		t.Errorf("empty input = %d, want 0", got)
	}
	if got := count(`"a"`); got != 1 {
		t.Errorf("1-byte input = %d, want 1", got)
	}
	if got := count(`"abc"`); got != 1 {
		t.Errorf("3-byte input = %d, want 1", got)
	}
	if got := count(`"abcd"`); got != 1 {
		t.Errorf("4-byte input = %d, want 1", got)
	}
	if got := count(`"abcde"`); got != 2 {
		t.Errorf("5-byte input = %d, want 2", got)
	}
	if got := count(`"hello world"`); got != 2 {
		t.Errorf("hello world = %d, want 2", got)
	}
	direct := count(`"old answer new question"`)
	history := count(`[{"role":"assistant","content":[{"type":"output_text","text":"old answer"}]},{"role":"user","content":[{"type":"input_text","text":"new question"}]}]`)
	if history <= direct {
		t.Fatalf("transformed history overhead was not counted: direct=%d history=%d", direct, history)
	}
}

func TestExtractFinishedAssistantTextAncestry(t *testing.T) {
	t.Parallel()
	raw := []byte(`{
		"current_node": "b",
		"mapping": {
			"a": {"message": {"author": {"role": "assistant"}, "status": "finished_successfully", "content": {"parts": ["old reply"]}}},
			"b": {"parent": "a", "message": {"author": {"role": "assistant"}, "status": "finished_successfully", "content": {"parts": ["new reply"]}}}
		}
	}`)
	text, finished := extractFinishedAssistantText(raw)
	if !finished || text != "new reply" {
		t.Fatalf("text=%q finished=%v", text, finished)
	}
}

func TestExtractFinishedAssistantTextInProgress(t *testing.T) {
	t.Parallel()
	raw := []byte(`{
		"current_node": "b",
		"mapping": {
			"a": {"message": {"author": {"role": "assistant"}, "status": "finished_successfully", "content": {"parts": ["done"]}}},
			"b": {"parent": "a", "message": {"author": {"role": "assistant"}, "status": "in_progress", "content": {"parts": ["partial"]}}}
		}
	}`)
	text, finished := extractFinishedAssistantText(raw)
	if finished || text != "" {
		t.Fatalf("text=%q finished=%v", text, finished)
	}
}

func TestExtractFinishedAssistantTextThroughNonAssistantNode(t *testing.T) {
	t.Parallel()
	raw := []byte(`{
		"current_node": "c",
		"mapping": {
			"a": {"message": {"author": {"role": "user"}, "status": "finished_successfully", "content": {"parts": ["question"]}}},
			"b": {"parent": "a", "message": {"author": {"role": "assistant"}, "status": "finished_successfully", "content": {"parts": ["answer"]}}},
			"c": {"parent": "b"}
		}
	}`)
	text, finished := extractFinishedAssistantText(raw)
	if !finished || text != "answer" {
		t.Fatalf("text=%q finished=%v", text, finished)
	}
}

func TestExtractFinishedAssistantTextNothingFinished(t *testing.T) {
	t.Parallel()
	raw := []byte(`{
		"current_node": "b",
		"mapping": {
			"a": {"message": {"author": {"role": "user"}, "status": "finished_successfully", "content": {"parts": ["question"]}}},
			"b": {"parent": "a", "message": {"author": {"role": "assistant"}, "status": "in_progress", "content": {"parts": ["partial"]}}}
		}
	}`)
	text, finished := extractFinishedAssistantText(raw)
	if finished || text != "" {
		t.Fatalf("text=%q finished=%v", text, finished)
	}
}

func TestTurnKeyStableAndDistinct(t *testing.T) {
	t.Parallel()
	a := &cliproxyauth.Auth{ID: "auth-a", Metadata: map[string]any{"cookie": "__Secure-next-auth.session-token=cookie-a"}}
	b := &cliproxyauth.Auth{ID: "auth-b", Metadata: map[string]any{"cookie": "__Secure-next-auth.session-token=cookie-b"}}
	sessionA := &chatgptweb.Session{AccountID: "account-a", DeviceID: "device-a"}
	sessionB := &chatgptweb.Session{AccountID: "account-b", DeviceID: "device-b"}
	if turnKey(a, sessionA) != turnKey(a, sessionA) {
		t.Fatal("turn key unstable")
	}
	if turnKey(a, sessionA) == turnKey(b, sessionB) {
		t.Fatal("distinct accounts must not share a turn key")
	}
	if strings.Contains(turnKey(a, sessionA), "account-a") || strings.Contains(turnKey(a, sessionA), "device-a") {
		t.Fatal("turn key leaks cookie")
	}
	if turnKey(a, sessionA) != turnKey(b, sessionA) {
		t.Fatal("duplicate auth records for one account must share a turn key")
	}
	b.Metadata["cookie"] = "__Secure-next-auth.session-token=rotated"
	if turnKey(a, sessionA) != turnKey(b, sessionA) {
		t.Fatal("session-token rotation changed the auth turn key")
	}
	if turnKey(a, nil) != turnKey(a, nil) || turnKey(a, nil) == turnKey(b, nil) {
		t.Fatal("auth identity fallback is not stable and distinct")
	}
}

func TestKeyedMutexSerializes(t *testing.T) {
	t.Parallel()
	var k keyedMutex
	var inFlight atomic.Int32
	var maxSeen atomic.Int32
	done := make(chan struct{})
	for i := 0; i < 8; i++ {
		go func() {
			unlock, _ := k.lock(context.Background(), "acct")
			cur := inFlight.Add(1)
			for {
				m := maxSeen.Load()
				if cur <= m || maxSeen.CompareAndSwap(m, cur) {
					break
				}
			}
			time.Sleep(5 * time.Millisecond)
			inFlight.Add(-1)
			unlock()
			done <- struct{}{}
		}()
	}
	for i := 0; i < 8; i++ {
		<-done
	}
	if maxSeen.Load() != 1 {
		t.Fatalf("max in flight = %d", maxSeen.Load())
	}
}

func TestExecuteNonStreamChatCompletion(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/auth/session":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"accessToken":"at","user":{"id":"u","email":"e"},"account":{"id":"a"}}`)
		case r.URL.Path == "/backend-api/me" || r.URL.Path == "/backend-api/models":
			fmt.Fprint(w, `{}`)
		case r.URL.Path == "/":
			w.Header().Set("Content-Type", "text/html")
			fmt.Fprint(w, `<html><head data-build="test-build"></head><body></body></html>`)
		case r.URL.Path == "/backend-api/sentinel/chat-requirements/prepare":
			fmt.Fprint(w, `{"persona":"chatgpt-paid","prepare_token":"prep-tok","proofofwork":{"required":false},"turnstile":{"required":false}}`)
		case r.URL.Path == "/backend-api/sentinel/chat-requirements":
			fmt.Fprint(w, `{"persona":"chatgpt-paid","token":"final-tok"}`)
		case r.URL.Path == "/backend-api/f/conversation":
			raw, _ := io.ReadAll(r.Body)
			if !strings.Contains(string(raw), `"history_and_training_disabled":true`) {
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprint(w, "temporary chat required")
				return
			}
			if !strings.Contains(string(raw), `"thinking_effort":"standard"`) {
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprint(w, "thinking effort must default to standard")
				return
			}
			w.Header().Set("Content-Type", "text/event-stream")
			w.Header().Add("Set-Cookie", "__cf_bm=conversation-rotation; Path=/; HttpOnly")
			fmt.Fprint(w, "data: {\"conversation_id\":\"conv-1\",\"message\":{\"id\":\"m2\",\"author\":{\"role\":\"assistant\"},\"status\":\"in_progress\",\"content\":{\"parts\":[\"Hel\"]}}}\n\n")
			fmt.Fprint(w, "data: {\"conversation_id\":\"conv-1\",\"message\":{\"id\":\"m2\",\"author\":{\"role\":\"assistant\"},\"status\":\"finished_successfully\",\"content\":{\"parts\":[\"Hello world\"]},\"metadata\":{\"finish_details\":{\"type\":\"stop\"}}}}\n\n")
			fmt.Fprint(w, "data: [DONE]\n\n")
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	useChatGPTWebTestURLs(t, srv.URL, srv.URL+"/backend-api", srv.URL+"/backend-api/f/conversation")

	e := NewChatGPTWebExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Metadata: map[string]any{"cookie": "__Secure-next-auth.session-token=tok"}}
	req := cliproxyexecutor.Request{
		Model:   "gpt-5-6-pro",
		Payload: []byte(`{"model":"gpt-5-6-pro","messages":[{"role":"user","content":"hi"}]}`),
	}
	opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("openai")}

	resp, err := e.Execute(context.Background(), auth, req, opts)
	if err != nil {
		t.Fatal(err)
	}
	var parsed struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(resp.Payload, &parsed); err != nil {
		t.Fatalf("response payload: %v\n%s", err, resp.Payload)
	}
	if len(parsed.Choices) != 1 || parsed.Choices[0].Message.Content != "Hello world" {
		t.Fatalf("payload = %s", resp.Payload)
	}
	if len(resp.Headers.Values("Set-Cookie")) != 0 {
		t.Fatalf("response exposed upstream cookies: %v", resp.Headers.Values("Set-Cookie"))
	}
	if got := resp.Headers.Get("Content-Type"); got != "" {
		t.Fatalf("response exposed upstream content type %q", got)
	}
	if gjson.GetBytes(resp.Payload, "usage.total_tokens").Int() == 0 {
		t.Fatalf("response omitted estimated usage: %s", resp.Payload)
	}
	cached, err := e.sessions.Resolve(t.Context(), srv.Client(), chatGPTWebCookie(auth), chatgptweb.DefaultUserAgent)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(cached.Cookie, "__cf_bm=conversation-rotation") {
		t.Fatalf("conversation cookie rotation was not recorded: %q", cached.Cookie)
	}
}

func TestExecuteStreamSingleDoneMarker(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/auth/session":
			fmt.Fprint(w, `{"accessToken":"at","user":{"id":"u"},"account":{"id":"a"}}`)
		case r.URL.Path == "/backend-api/me" || r.URL.Path == "/backend-api/models":
			fmt.Fprint(w, `{}`)
		case r.URL.Path == "/":
			w.Header().Set("Content-Type", "text/html")
			fmt.Fprint(w, `<html><head data-build="test-build"></head><body></body></html>`)
		case r.URL.Path == "/backend-api/sentinel/chat-requirements/prepare":
			fmt.Fprint(w, `{"persona":"chatgpt-paid","prepare_token":"prep-tok","proofofwork":{"required":false},"turnstile":{"required":false}}`)
		case r.URL.Path == "/backend-api/sentinel/chat-requirements":
			fmt.Fprint(w, `{"persona":"chatgpt-paid","token":"final-tok"}`)
		case r.URL.Path == "/backend-api/f/conversation":
			w.Header().Set("Content-Type", "text/event-stream")
			w.Header().Set("Set-Cookie", "__cf_bm=stream-rotation; Path=/; HttpOnly")
			fmt.Fprint(w, "data: {\"conversation_id\":\"conv-1\",\"message\":{\"id\":\"m2\",\"author\":{\"role\":\"assistant\"},\"status\":\"in_progress\",\"content\":{\"parts\":[\"Hel\"]}}}\n\n")
			fmt.Fprint(w, "data: {\"conversation_id\":\"conv-1\",\"message\":{\"id\":\"m2\",\"author\":{\"role\":\"assistant\"},\"status\":\"finished_successfully\",\"content\":{\"parts\":[\"Hello world\"]},\"metadata\":{\"finish_details\":{\"type\":\"stop\"}}}}\n\n")
			fmt.Fprint(w, "data: [DONE]\n\n")
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	useChatGPTWebTestURLs(t, srv.URL, srv.URL+"/backend-api", srv.URL+"/backend-api/f/conversation")

	e := NewChatGPTWebExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Metadata: map[string]any{"cookie": "__Secure-next-auth.session-token=tok"}}
	req := cliproxyexecutor.Request{
		Model:   "gpt-5-6-pro",
		Payload: []byte(`{"model":"gpt-5-6-pro","messages":[{"role":"user","content":"hi"}],"stream":true}`),
	}
	opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("openai"), Stream: true}

	stream, err := e.ExecuteStream(context.Background(), auth, req, opts)
	if err != nil {
		t.Fatal(err)
	}
	if len(stream.Headers.Values("Set-Cookie")) != 0 {
		t.Fatalf("stream exposed upstream cookies: %v", stream.Headers.Values("Set-Cookie"))
	}
	var all strings.Builder
	var texts []string
	for chunk := range stream.Chunks {
		if chunk.Err != nil {
			t.Fatal(chunk.Err)
		}
		all.Write(chunk.Payload)
		var c struct {
			Choices []struct {
				Delta struct {
					Content string `json:"content"`
				} `json:"delta"`
			} `json:"choices"`
		}
		if json.Unmarshal(chunk.Payload, &c) == nil {
			for _, ch := range c.Choices {
				texts = append(texts, ch.Delta.Content)
			}
		}
	}
	if strings.Contains(all.String(), "[DONE]") {
		t.Fatalf("executor emitted the HTTP handler's terminal marker:\n%s", all.String())
	}
	if got := strings.Join(texts, ""); got != "Hello world" {
		t.Fatalf("stream text = %q\n%s", got, all.String())
	}
	cached, err := e.sessions.Resolve(t.Context(), srv.Client(), chatGPTWebCookie(auth), chatgptweb.DefaultUserAgent)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(cached.Cookie, "__cf_bm=stream-rotation") {
		t.Fatalf("stream cookie rotation was not recorded: %q", cached.Cookie)
	}
}

func TestResumeMismatchReturnsContinuationError(t *testing.T) {
	var polls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/f/conversation/resume" {
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprint(w, "data: {\"conversation_id\":\"c\",\"message\":{\"id\":\"m\",\"author\":{\"role\":\"assistant\"},\"status\":\"in_progress\",\"content\":{\"parts\":[\"Different text entirely\"]}}}\n\n")
			fmt.Fprint(w, "data: [DONE]\n\n")
			return
		}
		if strings.HasPrefix(r.URL.Path, "/conversation/") {
			polls.Add(1)
			fmt.Fprint(w, `{"current_node":"b","mapping":{"b":{"message":{"author":{"role":"assistant"},"status":"finished_successfully","content":{"parts":["Hello polled"]}}}}}`)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	useChatGPTWebTestURLs(t, srv.URL, srv.URL, "")

	_, _, err := resumeConversation(context.Background(), srv.Client(), testSession(), nil, nil, "c", "tok", "Hello", nil, nil, nil)
	var statusErr cliproxyexecutor.StatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode() != http.StatusBadGateway {
		t.Fatalf("expected continuation failure status 502, got %v", err)
	}
	if polls.Load() != 0 {
		t.Fatalf("temporary chat attempted persisted-conversation polling %d times", polls.Load())
	}
}

func TestPollConversationRejectsRevisionAfterEmission(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"current_node":"b","mapping":{"b":{"message":{"author":{"role":"assistant"},"status":"finished_successfully","content":{"parts":["Different answer"]}}}}}`)
	}))
	t.Cleanup(srv.Close)

	useChatGPTWebTestURLs(t, srv.URL, srv.URL, "")
	origPoll := pollInterval
	pollInterval = time.Millisecond
	t.Cleanup(func() { pollInterval = origPoll })

	var deltas []string
	text, err := pollConversation(context.Background(), srv.Client(), testSession(), "c", "Hello", nil, func(delta string) {
		deltas = append(deltas, delta)
	})
	if err == nil || !strings.Contains(err.Error(), "diverged from streamed text") {
		t.Fatalf("expected divergence error, got text=%q err=%v", text, err)
	}
	if len(deltas) != 0 {
		t.Fatalf("revision emitted deltas: %q", deltas)
	}
}

func TestPollConversationTerminalStatus(t *testing.T) {
	for _, code := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/f/conversation/resume" {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				w.WriteHeader(code)
			}))
			defer srv.Close()

			useChatGPTWebTestURLs(t, srv.URL, srv.URL, "")

			origPoll := pollInterval
			pollInterval = time.Millisecond
			defer func() { pollInterval = origPoll }()

			_, err := pollConversation(context.Background(), srv.Client(), testSession(), "c", "Hello", nil, nil)
			var se cliproxyexecutor.StatusError
			if !errors.As(err, &se) || se.StatusCode() != code {
				t.Fatalf("expected terminal statusError %d, got %v", code, err)
			}
		})
	}
}

func TestPollConversationBoundedFailures(t *testing.T) {
	var polls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/f/conversation/resume" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		polls.Add(1)
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()

	useChatGPTWebTestURLs(t, srv.URL, srv.URL, "")

	origPoll := pollInterval
	pollInterval = time.Millisecond
	defer func() { pollInterval = origPoll }()

	_, err := pollConversation(context.Background(), srv.Client(), testSession(), "c", "Hello", nil, nil)
	if err == nil {
		t.Fatal("polling must give up after bounded consecutive failures")
	}
	var se cliproxyexecutor.StatusError
	if errors.As(err, &se) {
		t.Fatalf("transient 502s must not surface as terminal statusError: %v", err)
	}
	if got := polls.Load(); got != 10 {
		t.Fatalf("expected exactly 10 bounded poll attempts, got %d", got)
	}
}

func TestHttpRequestRejectsNonChatGPTHost(t *testing.T) {
	e := NewChatGPTWebExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Metadata: map[string]any{"cookie": "k=v"}}
	for _, u := range []string{
		"http://chatgpt.com/backend-api/models",
		"https://example.com/",
		"https://chatgpt.com.evil.example/",
		"https://chatgpt.com:8443/backend-api/models",
	} {
		req, err := http.NewRequest(http.MethodGet, u, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := e.PrepareRequest(req, auth); err == nil {
			t.Errorf("%s: PrepareRequest accepted disallowed destination", u)
		}
		if got := req.Header.Get("Cookie"); got != "" {
			t.Errorf("%s: PrepareRequest attached cookie %q", u, got)
		}
		if _, err := e.HttpRequest(context.Background(), auth, req); err == nil {
			t.Errorf("%s: expected host rejection, got nil error", u)
		}
	}
	if got := auth.Metadata["cookie"]; got != "k=v" {
		t.Fatalf("auth metadata mutated: %v", got)
	}
}

func TestHttpRequestRejectsDisallowedRedirect(t *testing.T) {
	for _, location := range []string{
		"http://chatgpt.com/leak",
		"https://example.com/leak",
		"https://chatgpt.com:8443/leak",
	} {
		t.Run(location, func(t *testing.T) {
			var requests int
			e := NewChatGPTWebExecutor(&config.Config{})
			e.transports = map[string]http.RoundTripper{"": roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				requests++
				if requests > 1 {
					t.Fatalf("redirect destination was requested with cookie %q", req.Header.Get("Cookie"))
				}
				return &http.Response{
					StatusCode: http.StatusFound,
					Header:     http.Header{"Location": []string{location}},
					Body:       io.NopCloser(strings.NewReader("redirect")),
					Request:    req,
				}, nil
			})}
			auth := &cliproxyauth.Auth{Metadata: map[string]any{"cookie": "__Secure-next-auth.session-token=token"}}
			req, err := http.NewRequest(http.MethodGet, "https://chatgpt.com/backend-api/models", nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = e.HttpRequest(t.Context(), auth, req); err == nil || requests != 1 {
				t.Fatalf("redirect error = %v requests = %d", err, requests)
			}
		})
	}
}

func TestHttpRequestConsumesAndPersistsSetCookie(t *testing.T) {
	var persistedCookie string
	e := NewChatGPTWebExecutor(&config.Config{}, func(_ context.Context, _ *cliproxyauth.Auth, updates map[string]any) (*cliproxyauth.Auth, error) {
		persistedCookie, _ = updates["cookie"].(string)
		return &cliproxyauth.Auth{}, nil
	})
	e.transports = map[string]http.RoundTripper{"": roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if got := req.Header.Get("Cookie"); !strings.Contains(got, "session-token=token") {
			t.Fatalf("upstream cookie = %q", got)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header: http.Header{
				"Connection":          []string{"keep-alive, X-Connection-Scoped"},
				"Content-Encoding":    []string{"gzip"},
				"Content-Length":      []string{"2"},
				"Content-Type":        []string{"application/json"},
				"Keep-Alive":          []string{"timeout=5"},
				"Set-Cookie":          []string{"__cf_bm=new; Path=/; HttpOnly"},
				"X-Connection-Scoped": []string{"remove-me"},
				"X-Request-Id":        []string{"request-id"},
			},
			Body: io.NopCloser(strings.NewReader("ok")),
		}, nil
	})}
	auth := &cliproxyauth.Auth{ID: "auth", Metadata: map[string]any{"cookie": "__Secure-next-auth.session-token=token; __cf_bm=old"}}
	req, err := http.NewRequest(http.MethodGet, "https://chatgpt.com/backend-api/models", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := e.HttpRequest(t.Context(), auth, req)
	if err != nil {
		t.Fatal(err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"Connection", "Keep-Alive", "Set-Cookie", "X-Connection-Scoped"} {
		if got := resp.Header.Get(key); got != "" {
			t.Fatalf("%s leaked downstream: %q", key, got)
		}
	}
	if got := resp.Header.Get("X-Request-ID"); got != "request-id" {
		t.Fatalf("X-Request-ID = %q", got)
	}
	for key, want := range map[string]string{"Content-Encoding": "gzip", "Content-Length": "2", "Content-Type": "application/json"} {
		if got := resp.Header.Get(key); got != want {
			t.Fatalf("%s = %q, want %q", key, got, want)
		}
	}
	if !strings.Contains(persistedCookie, "__cf_bm=new") {
		t.Fatalf("persisted cookie = %q", persistedCookie)
	}
}

func TestPrepareRequestProtectsBrowserUserAgent(t *testing.T) {
	e := NewChatGPTWebExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{
		Metadata:   map[string]any{"cookie": "k=v", "user_agent": testChatGPTWebUserAgent},
		Attributes: map[string]string{"header:User-Agent": "wrong-agent"},
	}
	req, err := http.NewRequest(http.MethodGet, "https://chatgpt.com/backend-api/models", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.PrepareRequest(req, auth); err != nil {
		t.Fatal(err)
	}
	if got := req.Header.Get("User-Agent"); got != testChatGPTWebUserAgent {
		t.Fatalf("User-Agent = %q", got)
	}
	legacy, err := chatGPTWebUserAgent(&cliproxyauth.Auth{})
	if err != nil || legacy != chatgptweb.DefaultUserAgent {
		t.Fatalf("legacy User-Agent = %q, %v", legacy, err)
	}
}

func TestHTTPClientsShareTransport(t *testing.T) {
	e := NewChatGPTWebExecutor(&config.Config{})
	credential, stream := e.httpClients(nil)
	credentialAgain, _ := e.httpClients(nil)
	if credential.Transport != stream.Transport || credential.Transport != credentialAgain.Transport {
		t.Fatal("ChatGPT web clients did not reuse one transport pool")
	}
	if credential.Timeout == 0 || stream.Timeout != 0 {
		t.Fatal("shared transport did not preserve per-client timeouts")
	}
}

func TestHTTPClientsBoundTransportCache(t *testing.T) {
	e := NewChatGPTWebExecutor(&config.Config{})
	e.transports = make(map[string]http.RoundTripper, chatGPTWebTransportCacheLimit)
	cached := make([]*closeTrackingRoundTripper, chatGPTWebTransportCacheLimit)
	for i := range cached {
		cached[i] = &closeTrackingRoundTripper{}
		key := fmt.Sprintf("proxy-%d", i)
		e.transports[key] = cached[i]
		e.transportOrder = append(e.transportOrder, key)
	}
	e.httpClients(&cliproxyauth.Auth{ProxyURL: "socks5://127.0.0.1:10000"})
	if got := len(e.transports); got != chatGPTWebTransportCacheLimit {
		t.Fatalf("cached transports = %d, want %d", got, chatGPTWebTransportCacheLimit)
	}
	for i, transport := range cached {
		if got, want := transport.closed.Load(), i == 0; got != want {
			t.Fatalf("transport %d closed = %t, want %t", i, got, want)
		}
	}
}

func TestCachedTransportClosesAfterActiveResponse(t *testing.T) {
	underlying := &closeTrackingRoundTripper{body: io.NopCloser(strings.NewReader("ok"))}
	transport := &chatGPTWebCachedTransport{transport: underlying}
	req, err := http.NewRequest(http.MethodGet, "https://chatgpt.com/", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	transport.CloseIdleConnections()
	if underlying.closed.Load() {
		t.Fatal("active transport closed before response body")
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if !underlying.closed.Load() {
		t.Fatal("retired transport remained open after response body closed")
	}
}

func TestTurnstileRequiredDoesNotInvalidateSession(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = w.Write([]byte(`{"accessToken":"at","user":{"id":"u"},"account":{"id":"a"}}`))
	}))
	t.Cleanup(srv.Close)
	useChatGPTWebTestURLs(t, srv.URL, "", "")

	e := NewChatGPTWebExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Metadata: map[string]any{
		"cookie":     "__Secure-next-auth.session-token=tok",
		"user_agent": testChatGPTWebUserAgent,
	}}
	if _, err := e.sessions.Resolve(t.Context(), srv.Client(), chatGPTWebCookie(auth), testChatGPTWebUserAgent); err != nil {
		t.Fatal(err)
	}
	e.invalidateOnAuthError(auth, fmt.Errorf("sentinel: %w", chatgptweb.ErrTurnstileRequired))
	if _, err := e.sessions.Resolve(t.Context(), srv.Client(), chatGPTWebCookie(auth), testChatGPTWebUserAgent); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("session exchanges = %d", calls.Load())
	}
}

func TestContinuationAuthErrorInvalidatesSession(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = w.Write([]byte(`{"accessToken":"at","user":{"id":"u"},"account":{"id":"a"}}`))
	}))
	t.Cleanup(srv.Close)
	useChatGPTWebTestURLs(t, srv.URL, "", "")

	e := NewChatGPTWebExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Metadata: map[string]any{
		"cookie":     "__Secure-next-auth.session-token=tok",
		"user_agent": testChatGPTWebUserAgent,
	}}
	if _, err := e.sessions.Resolve(t.Context(), srv.Client(), chatGPTWebCookie(auth), testChatGPTWebUserAgent); err != nil {
		t.Fatal(err)
	}
	e.invalidateOnAuthError(auth, &chatGPTWebContinuationError{cause: &statusError{code: http.StatusUnauthorized}})
	if _, err := e.sessions.Resolve(t.Context(), srv.Client(), chatGPTWebCookie(auth), testChatGPTWebUserAgent); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatalf("session exchanges = %d", calls.Load())
	}
}

func TestChatGPTWebRequirementsOptionsParsing(t *testing.T) {
	t.Parallel()
	if got := chatGPTWebRequirementsOptions(nil); got.TurnstileToken != "" || got.SubmitWithoutTurnstile {
		t.Fatalf("nil auth = %+v", got)
	}
	auth := &cliproxyauth.Auth{Metadata: map[string]any{
		"turnstile_token":          "tok-user",
		"submit_without_turnstile": true,
	}}
	got := chatGPTWebRequirementsOptions(auth)
	if got.TurnstileToken != "tok-user" || !got.SubmitWithoutTurnstile {
		t.Fatalf("parsed = %+v", got)
	}
	auth.Metadata["submit_without_turnstile"] = "true"
	if got := chatGPTWebRequirementsOptions(auth); got.SubmitWithoutTurnstile {
		t.Fatal("non-bool submit_without_turnstile must be ignored")
	}
}

func newTurnstileChallengeServer(t *testing.T, conversationHandler http.HandlerFunc) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/auth/session":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"accessToken":"at","user":{"id":"u","email":"e"},"account":{"id":"a"}}`)
		case r.URL.Path == "/backend-api/me" || r.URL.Path == "/backend-api/models":
			fmt.Fprint(w, `{}`)
		case r.URL.Path == "/":
			w.Header().Set("Content-Type", "text/html")
			fmt.Fprint(w, `<html><head data-build="test-build"></head><body></body></html>`)
		case r.URL.Path == "/backend-api/sentinel/chat-requirements/prepare":
			fmt.Fprint(w, `{"persona":"chatgpt-paid","prepare_token":"prep-tok","proofofwork":{"required":false},"turnstile":{"required":false}}`)
		case r.URL.Path == "/backend-api/sentinel/chat-requirements":
			fmt.Fprint(w, `{"persona":"chatgpt-paid","token":"final-tok","turnstile":{"required":true}}`)
		case r.URL.Path == "/backend-api/f/conversation":
			conversationHandler(w, r)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	useChatGPTWebTestURLs(t, srv.URL, srv.URL+"/backend-api", srv.URL+"/backend-api/f/conversation")
}

func executeTestChatCompletion(e *ChatGPTWebExecutor, auth *cliproxyauth.Auth) error {
	req := cliproxyexecutor.Request{
		Model:   "gpt-5-6-pro",
		Payload: []byte(`{"model":"gpt-5-6-pro","messages":[{"role":"user","content":"hi"}]}`),
	}
	opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("openai")}
	_, err := e.Execute(context.Background(), auth, req, opts)
	return err
}

func TestExecuteTurnstileHardStopSkipsConversation(t *testing.T) {
	var conversationCalls atomic.Int32
	newTurnstileChallengeServer(t, func(w http.ResponseWriter, _ *http.Request) {
		conversationCalls.Add(1)
		w.WriteHeader(http.StatusForbidden)
	})
	e := NewChatGPTWebExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Metadata: map[string]any{"cookie": "__Secure-next-auth.session-token=tok"}}
	err := executeTestChatCompletion(e, auth)
	if !errors.Is(err, chatgptweb.ErrTurnstileRequired) {
		t.Fatalf("error = %v, want ErrTurnstileRequired", err)
	}
	if conversationCalls.Load() != 0 {
		t.Fatalf("conversation calls = %d, want 0 on hard stop", conversationCalls.Load())
	}
}

func TestExecuteTurnstileGambleSubmitsOnceWithoutToken(t *testing.T) {
	var conversationCalls atomic.Int32
	newTurnstileChallengeServer(t, func(w http.ResponseWriter, r *http.Request) {
		conversationCalls.Add(1)
		if got := r.Header.Get("openai-sentinel-turnstile-token"); got != "" {
			t.Errorf("turnstile header = %q, want absent on gamble", got)
		}
		if got := r.Header.Get("openai-sentinel-chat-requirements-token"); got != "final-tok" {
			t.Errorf("requirements token = %q", got)
		}
		w.WriteHeader(http.StatusForbidden)
		fmt.Fprint(w, `{"detail":"cf-chl-secret-body"}`)
	})
	e := NewChatGPTWebExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Metadata: map[string]any{
		"cookie":                   "__Secure-next-auth.session-token=tok",
		"submit_without_turnstile": true,
	}}
	err := executeTestChatCompletion(e, auth)
	if err == nil {
		t.Fatal("rejected gambled conversation must error")
	}
	if errors.Is(err, chatgptweb.ErrTurnstileRequired) {
		t.Fatalf("gamble rejection must not surface as pre-submit hard stop: %v", err)
	}
	if strings.Contains(err.Error(), "cf-chl-secret-body") {
		t.Fatalf("error leaks upstream body: %v", err)
	}
	if conversationCalls.Load() != 1 {
		t.Fatalf("conversation calls = %d, want exactly 1 (no retry)", conversationCalls.Load())
	}
}

func TestExecuteForwardsSuppliedTurnstileToken(t *testing.T) {
	var conversationCalls atomic.Int32
	newTurnstileChallengeServer(t, func(w http.ResponseWriter, r *http.Request) {
		conversationCalls.Add(1)
		if got := r.Header.Get("openai-sentinel-turnstile-token"); got != "user-captured-token" {
			t.Errorf("turnstile header = %q, want user-captured-token", got)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"conversation_id\":\"conv-1\",\"message\":{\"id\":\"m2\",\"author\":{\"role\":\"assistant\"},\"status\":\"in_progress\",\"content\":{\"parts\":[\"o\"]}}}\n\n")
		fmt.Fprint(w, "data: {\"conversation_id\":\"conv-1\",\"message\":{\"id\":\"m2\",\"author\":{\"role\":\"assistant\"},\"status\":\"finished_successfully\",\"content\":{\"parts\":[\"ok\"]},\"metadata\":{\"finish_details\":{\"type\":\"stop\"}}}}\n\n")
		fmt.Fprint(w, "data: [DONE]\n\n")
	})
	e := NewChatGPTWebExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Metadata: map[string]any{
		"cookie":          "__Secure-next-auth.session-token=tok",
		"turnstile_token": "user-captured-token",
	}}
	if err := executeTestChatCompletion(e, auth); err != nil {
		t.Fatalf("execute with supplied token: %v", err)
	}
	if conversationCalls.Load() != 1 {
		t.Fatalf("conversation calls = %d, want 1", conversationCalls.Load())
	}
}

func TestPrepareRequestRejectsStoredNonChromiumUserAgent(t *testing.T) {
	e := NewChatGPTWebExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Metadata: map[string]any{
		"cookie":     "k=v",
		"user_agent": "Mozilla/5.0 Firefox/140.0",
	}}
	req, err := http.NewRequest(http.MethodGet, "https://chatgpt.com/backend-api/models", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.PrepareRequest(req, auth); err == nil {
		t.Fatal("stored non-Chromium User-Agent was accepted")
	}
	if req.Header.Get("Cookie") != "" {
		t.Fatal("cookie was attached before User-Agent validation")
	}
	auth.Metadata["user_agent"] = 123
	if err := e.PrepareRequest(req, auth); err == nil {
		t.Fatal("non-string stored User-Agent was accepted")
	}
}

func TestWarmupUsesSessionIdentity(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call := calls.Add(1)
		cookieOK := strings.Contains(r.Header.Get("Cookie"), "__Secure-next-auth.session-token=fake")
		if call > 1 {
			cookieOK = cookieOK && strings.Contains(r.Header.Get("Cookie"), "__cf_bm=rotated-"+strconv.Itoa(int(call-1)))
		}
		if r.Header.Get("User-Agent") != testChatGPTWebUserAgent ||
			!cookieOK ||
			r.Header.Get("OAI-Device-Id") != "device-fake" ||
			r.Header.Get("Authorization") != "Bearer access-fake" ||
			r.Header.Get("OAI-Client-Version") == "" ||
			r.Header.Get("Sec-CH-UA") == "" ||
			r.Header.Get("OAI-Session-Id") != "wsess-warm" ||
			r.Header.Get("OAI-Language") != "en-US" {
			t.Error("warmup request did not preserve session identity")
		}
		if r.URL.Path == "/models" && r.URL.Query().Get("history_and_training_disabled") != "true" {
			t.Error("model warmup was not scoped to Temporary Chat")
		}
		w.Header().Add("Set-Cookie", "__cf_bm=rotated-"+strconv.Itoa(int(call))+"; Path=/")
		w.Header().Add("Set-Cookie", "theme=dark; Path=/")
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(srv.Close)
	useChatGPTWebTestURLs(t, srv.URL, srv.URL, "")

	setCookies := warmupChatGPTWeb(t.Context(), srv.Client(), &chatgptweb.Session{
		Cookie:       "__Secure-next-auth.session-token=fake",
		AccessToken:  "access-fake",
		DeviceID:     "device-fake",
		UserAgent:    testChatGPTWebUserAgent,
		WebSessionID: "wsess-warm",
	})
	if calls.Load() != int32(len(chatGPTWebWarmupSteps)) {
		t.Fatalf("warmup calls = %d", calls.Load())
	}
	if len(setCookies) != 2*len(chatGPTWebWarmupSteps) {
		t.Fatalf("Set-Cookie values = %d", len(setCookies))
	}
}

func TestCustomHeadersApplied(t *testing.T) {
	t.Parallel()
	req, err := http.NewRequest(http.MethodPost, "https://chatgpt.com/backend-api/f/conversation", nil)
	if err != nil {
		t.Fatal(err)
	}
	attrs := map[string]string{
		"header:X-Custom-Trace":                                  "trace-1",
		"header:Authorization":                                   "Bearer wrong",
		"header:Cookie":                                          "wrong=cookie",
		"header:User-Agent":                                      "wrong-agent",
		"header:X-OpenAI-Target-Path":                            "/wrong",
		"header:X-OpenAI-Target-Route":                           "/wrong",
		"header:OAI-Device-Id":                                   "wrong-device",
		"header:chatgpt-account-id":                              "wrong-account",
		"header:openai-sentinel-chat-requirements-token":         "wrong-sentinel",
		"header:openai-sentinel-chat-requirements-prepare-token": "wrong-prepare",
		"header:openai-sentinel-proof-token":                     "wrong-proof",
		"header:openai-sentinel-turnstile-token":                 "wrong-turnstile",
		"header:Host":                                            "example.com",
		"not-a-header":                                           "ignored",
	}
	reqs := &chatgptweb.SentinelRequirements{Token: "sent", PrepareToken: "prep", ProofToken: "proof", Turnstile: "turn"}
	applyConversationHeaders(req, testSession(), reqs, "/backend-api/f/conversation", attrs)
	if got := req.Header.Get("X-Custom-Trace"); got != "trace-1" {
		t.Fatalf("custom header = %q", got)
	}
	if got := req.Header.Get("not-a-header"); got != "" {
		t.Fatalf("non-header attribute leaked: %q", got)
	}
	if got := req.Header.Get("Authorization"); got != "Bearer at" {
		t.Fatalf("authorization = %q", got)
	}
	if got := req.Header.Get("Cookie"); got != "__Secure-next-auth.session-token=tok" {
		t.Fatalf("cookie = %q", got)
	}
	if got := req.Header.Get("User-Agent"); got != testChatGPTWebUserAgent {
		t.Fatalf("User-Agent = %q", got)
	}
	if got := req.Header.Get("X-OpenAI-Target-Path"); got != "/backend-api/f/conversation" {
		t.Fatalf("target path = %q", got)
	}
	if got := req.Header.Get("OAI-Device-Id"); got != "dev-1" {
		t.Fatalf("device id = %q", got)
	}
	if got := req.Header.Get("chatgpt-account-id"); got != "acct-1" {
		t.Fatalf("account id = %q", got)
	}
	if got := req.Header.Get("openai-sentinel-chat-requirements-token"); got != "sent" {
		t.Fatalf("sentinel token = %q", got)
	}
	if got := req.Header.Get("openai-sentinel-chat-requirements-prepare-token"); got != "prep" {
		t.Fatalf("sentinel prepare token = %q", got)
	}
	if got := req.Header.Get("openai-sentinel-proof-token"); got != "proof" {
		t.Fatalf("sentinel proof token = %q", got)
	}
	if got := req.Header.Get("openai-sentinel-turnstile-token"); got != "turn" {
		t.Fatalf("sentinel turnstile token = %q", got)
	}
	if req.Host != "" || req.Header.Get("Host") != "" {
		t.Fatalf("custom host override survived: req.Host=%q header=%q", req.Host, req.Header.Get("Host"))
	}
}

func TestKeyedMutexRemovesIdleEntries(t *testing.T) {
	t.Parallel()
	var k keyedMutex
	unlock, err := k.lock(context.Background(), "acct")
	if err != nil {
		t.Fatal(err)
	}
	unlock()
	k.mu.Lock()
	n := len(k.locks)
	k.mu.Unlock()
	if n != 0 {
		t.Fatalf("idle lock entries retained: %d", n)
	}
}

func TestKeyedMutexWaitHonorsCancellation(t *testing.T) {
	t.Parallel()
	var k keyedMutex
	unlock, err := k.lock(context.Background(), "acct")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := k.lock(ctx, "acct"); err == nil {
		t.Fatal("canceled lock wait returned no error")
	}
	unlock()
	k.mu.Lock()
	n := len(k.locks)
	k.mu.Unlock()
	if n != 0 {
		t.Fatalf("canceled waiter left %d lock entries", n)
	}
}

func TestKeyedMutexIdleAcquireHonorsCancellation(t *testing.T) {
	t.Parallel()
	var k keyedMutex
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := k.lock(ctx, "acct"); !errors.Is(err, context.Canceled) {
		t.Fatalf("idle canceled lock error = %v", err)
	}
	k.mu.Lock()
	n := len(k.locks)
	k.mu.Unlock()
	if n != 0 {
		t.Fatalf("idle canceled acquire left %d lock entries", n)
	}
}

func TestCodexStreamEmitterResponseContract(t *testing.T) {
	t.Parallel()
	emitter := newCodexStreamEmitter("gpt-5-6-pro", []byte(`{"background":true,"instructions":"be terse","metadata":{"request_id":"r1"},"parallel_tool_calls":false,"temperature":0.25,"tool_choice":{"type":"function","name":"lookup"},"tools":[{"type":"function","name":"lookup"}],"top_p":0.75}`))
	usage := map[string]any{"input_tokens": int64(2), "output_tokens": int64(1), "total_tokens": int64(3)}
	response := emitter.terminalResponse("done", "stop", usage)
	for _, field := range []string{
		"id", "object", "created_at", "model", "status", "background", "error",
		"incomplete_details", "instructions", "metadata", "output", "parallel_tool_calls",
		"temperature", "tool_choice", "tools", "top_p", "usage",
	} {
		if _, ok := response[field]; !ok {
			t.Errorf("completed response missing %q", field)
		}
	}
	if !reflect.DeepEqual(response["usage"], usage) {
		t.Fatalf("completed response usage = %#v, want %#v", response["usage"], usage)
	}
	toolChoice := response["tool_choice"].(map[string]any)
	if toolChoice["name"] != "lookup" || response["parallel_tool_calls"] != false {
		t.Fatalf("completed response request settings = tool_choice:%#v parallel_tool_calls:%#v", response["tool_choice"], response["parallel_tool_calls"])
	}
	metadata := response["metadata"].(map[string]any)
	tools := response["tools"].([]any)
	if response["background"] != false || response["instructions"] != "be terse" || metadata["request_id"] != "r1" || response["temperature"] != 0.25 || len(tools) != 1 || response["top_p"] != 0.75 {
		t.Fatalf("completed response did not preserve request fields: %#v", response)
	}
	item := response["output"].([]any)[0].(map[string]any)
	part := item["content"].([]any)[0].(map[string]any)
	if _, ok := part["annotations"].([]any); !ok {
		t.Fatalf("completed response annotations = %#v", part["annotations"])
	}
	if _, ok := part["logprobs"].([]any); !ok {
		t.Fatalf("completed response logprobs = %#v", part["logprobs"])
	}
	for i, event := range emitter.start() {
		if got := gjson.GetBytes(dataPayload(event), "sequence_number").Int(); got != int64(i) {
			t.Fatalf("start event %d sequence_number = %d", i, got)
		}
	}
}

func TestCodexStreamEmitterIncompleteOutputItemStatus(t *testing.T) {
	t.Parallel()
	emitter := newCodexStreamEmitter("gpt-5-6-pro", []byte(`{}`))
	usage := map[string]any{"input_tokens": int64(2), "output_tokens": int64(1), "total_tokens": int64(3)}

	response := emitter.terminalResponse("truncated", "max_tokens", usage)
	if got := response["status"]; got != "incomplete" {
		t.Fatalf("incomplete terminal response status = %#v", got)
	}
	item := response["output"].([]any)[0].(map[string]any)
	if got := item["status"]; got != "incomplete" {
		t.Fatalf("incomplete terminal output item status = %#v, want incomplete", got)
	}

	completed := emitter.terminalResponse("done", "stop", usage)
	completedItem := completed["output"].([]any)[0].(map[string]any)
	if got := completedItem["status"]; got != "completed" {
		t.Fatalf("completed terminal output item status = %#v, want completed", got)
	}

	var itemDoneStatus, terminalEvent string
	for _, event := range emitter.finish("truncated", "max_tokens", false, usage) {
		payload := dataPayload(event)
		switch gjson.GetBytes(payload, "type").String() {
		case "response.output_item.done":
			itemDoneStatus = gjson.GetBytes(payload, "item.status").String()
		case "response.incomplete":
			terminalEvent = gjson.GetBytes(payload, "response.output.0.status").String()
		}
	}
	if itemDoneStatus != "incomplete" {
		t.Fatalf("response.output_item.done item status = %q, want incomplete", itemDoneStatus)
	}
	if terminalEvent != "incomplete" {
		t.Fatalf("response.incomplete output item status = %q, want incomplete", terminalEvent)
	}
}

func TestSanitizeChatGPTWebResponseHeaders(t *testing.T) {
	responseHeaders := make(http.Header)
	for key, value := range map[string]string{
		"Connection":          "keep-alive, X-Connection-Scoped",
		"Content-Encoding":    "gzip",
		"Content-Length":      "42",
		"Content-Type":        "text/event-stream",
		"Keep-Alive":          "timeout=5",
		"Proxy-Authenticate":  "challenge",
		"Proxy-Authorization": "secret",
		"Set-Cookie":          "session=secret",
		"TE":                  "trailers",
		"Trailer":             "X-Checksum",
		"Transfer-Encoding":   "chunked",
		"Upgrade":             "websocket",
		"X-Connection-Scoped": "remove-me",
		"X-Request-ID":        "request-id",
	} {
		responseHeaders.Set(key, value)
	}
	sanitizeChatGPTWebResponseHeaders(responseHeaders)
	for _, key := range []string{
		"Connection",
		"Content-Encoding",
		"Content-Length",
		"Content-Type",
		"Keep-Alive",
		"Proxy-Authenticate",
		"Proxy-Authorization",
		"Set-Cookie",
		"TE",
		"Trailer",
		"Transfer-Encoding",
		"Upgrade",
		"X-Connection-Scoped",
	} {
		if got := responseHeaders.Get(key); got != "" {
			t.Errorf("%s = %q", key, got)
		}
	}
	if got := responseHeaders.Get("X-Request-ID"); got != "request-id" {
		t.Fatalf("X-Request-ID = %q", got)
	}
}

func TestExtractFinishedAssistantTextEmptyCurrentTurn(t *testing.T) {
	t.Parallel()
	raw := []byte(`{
		"current_node": "b",
		"mapping": {
			"a": {"message": {"author": {"role": "assistant"}, "status": "finished_successfully", "content": {"parts": ["old reply"]}}},
			"b": {"parent": "a", "message": {"author": {"role": "assistant"}, "status": "finished_successfully", "content": {"parts": [""]}}}
		}
	}`)
	text, finished := extractFinishedAssistantText(raw)
	if !finished || text != "" {
		t.Fatalf("text=%q finished=%v; empty finished current turn must win over the older ancestor", text, finished)
	}
}

func TestRefreshPersistsRotatedCookie(t *testing.T) {
	var calls atomic.Int32
	var cookies []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/auth/session" {
			cookies = append(cookies, r.Header.Get("Cookie"))
			w.Header().Set("Content-Type", "application/json")
			switch calls.Add(1) {
			case 1:
				w.Header().Set("Set-Cookie", "__Secure-next-auth.session-token=tok-rotated; Path=/; HttpOnly; Max-Age=86400")
			case 2:
				w.Header().Set("Set-Cookie", "__Secure-next-auth.session-token=tok-refreshed; Path=/; HttpOnly; Max-Age=86400")
			}
			w.Header().Add("Set-Cookie", "tracking-cookie=drop-me; Path=/")
			fmt.Fprint(w, `{"accessToken":"at","user":{"id":"u"},"account":{"id":"a"}}`)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	useChatGPTWebTestURLs(t, srv.URL, srv.URL+"/backend-api", srv.URL+"/backend-api/f/conversation")

	e := NewChatGPTWebExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Metadata: map[string]any{
		"cookie":       "__Secure-next-auth.session-token=tok-orig",
		"last_refresh": int64(1),
	}}
	if _, err := e.sessions.Resolve(context.Background(), srv.Client(), chatGPTWebCookie(auth), chatgptweb.DefaultUserAgent); err != nil {
		t.Fatal(err)
	}
	refreshed, err := e.Refresh(context.Background(), auth)
	if err != nil {
		t.Fatal(err)
	}
	want := "__Secure-next-auth.session-token=tok-refreshed; oai-did=" + chatgptweb.DeviceIDForCookie("__Secure-next-auth.session-token=tok-orig")
	if got := refreshed.Metadata["cookie"]; got != want {
		t.Fatalf("refreshed cookie = %v", got)
	}
	if strings.Contains(refreshed.Metadata["cookie"].(string), "tracking-cookie") {
		t.Fatal("refresh persisted an unrelated cookie")
	}
	lastRefresh, ok := refreshed.Metadata["last_refresh"].(int64)
	if !ok || lastRefresh <= 1 {
		t.Fatalf("last_refresh = %v", refreshed.Metadata["last_refresh"])
	}
	if _, err := e.sessions.Resolve(context.Background(), srv.Client(), chatGPTWebCookie(refreshed), chatgptweb.DefaultUserAgent); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatalf("minimized refreshed cookie missed the session cache: calls = %d", calls.Load())
	}
	if len(cookies) != 2 || !strings.Contains(cookies[1], "tok-rotated") {
		t.Fatalf("refresh cookies = %q", cookies)
	}
	if auth.Metadata["cookie"] != "__Secure-next-auth.session-token=tok-orig" {
		t.Fatal("original auth metadata mutated")
	}
	if auth.Metadata["last_refresh"] != int64(1) {
		t.Fatal("original refresh metadata mutated")
	}
}

func TestRefreshRejectsMissingCookie(t *testing.T) {
	e := NewChatGPTWebExecutor(&config.Config{})
	if _, err := e.Refresh(context.Background(), &cliproxyauth.Auth{}); err == nil {
		t.Fatal("missing cookie refresh must fail")
	}
}

func newChatGPTWebEventServer(t *testing.T, events string, responseHeaders ...http.Header) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/auth/session":
			fmt.Fprint(w, `{"accessToken":"at","user":{"id":"u"},"account":{"id":"a"}}`)
		case "/":
			fmt.Fprint(w, `<html><head data-build="test-build"></head><body></body></html>`)
		case "/backend-api/sentinel/chat-requirements/prepare":
			fmt.Fprint(w, `{"persona":"chatgpt-paid","prepare_token":"prep-tok","proofofwork":{"required":false},"turnstile":{"required":false}}`)
		case "/backend-api/sentinel/chat-requirements":
			fmt.Fprint(w, `{"persona":"chatgpt-paid","token":"final-tok"}`)
		case "/backend-api/f/conversation":
			for _, headers := range responseHeaders {
				for key, values := range headers {
					for _, value := range values {
						w.Header().Add(key, value)
					}
				}
			}
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprint(w, events)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)

	useChatGPTWebTestURLs(t, srv.URL, srv.URL+"/backend-api", srv.URL+"/backend-api/f/conversation")
	return srv
}

func newChatGPTWebTransportFailureServer(t *testing.T) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/auth/session":
			fmt.Fprint(w, `{"accessToken":"at","user":{"id":"u"},"account":{"id":"a"}}`)
		case "/":
			fmt.Fprint(w, `<html><head data-build="test-build"></head><body></body></html>`)
		case "/backend-api/sentinel/chat-requirements/prepare":
			fmt.Fprint(w, `{"persona":"chatgpt-paid","prepare_token":"prep-tok","proofofwork":{"required":false},"turnstile":{"required":false}}`)
		case "/backend-api/sentinel/chat-requirements":
			fmt.Fprint(w, `{"persona":"chatgpt-paid","token":"final-tok"}`)
		case "/backend-api/f/conversation":
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			_ = conn.Close()
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	useChatGPTWebTestURLs(t, srv.URL, srv.URL+"/backend-api", srv.URL+"/backend-api/f/conversation")
}

func TestExecuteTransportFailureIsAuthNeutralAndNonRetryable(t *testing.T) {
	newChatGPTWebTransportFailureServer(t)
	e := NewChatGPTWebExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Metadata: map[string]any{"cookie": "__Secure-next-auth.session-token=tok"}}
	_, err := e.Execute(t.Context(), auth, cliproxyexecutor.Request{
		Model:   "gpt-5-6-pro",
		Payload: []byte(`{"model":"gpt-5-6-pro","input":"hi"}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse})
	var neutral interface{ AuthStateNeutral() bool }
	var retryable interface{ Retryable() bool }
	if err == nil || !errors.As(err, &neutral) || !neutral.AuthStateNeutral() || !errors.As(err, &retryable) || retryable.Retryable() {
		t.Fatalf("transport error was not auth-neutral and non-retryable: %v", err)
	}
}

func TestExecuteStreamCommitsTransportFailureWithoutResponse(t *testing.T) {
	newChatGPTWebTransportFailureServer(t)
	e := NewChatGPTWebExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Metadata: map[string]any{"cookie": "__Secure-next-auth.session-token=tok"}}
	stream, err := e.ExecuteStream(t.Context(), auth, cliproxyexecutor.Request{
		Model:   "gpt-5-6-pro",
		Payload: []byte(`{"model":"gpt-5-6-pro","input":"hi","stream":true}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, Stream: true})
	if err != nil {
		t.Fatal(err)
	}
	if stream == nil || !stream.BootstrapCommitted() {
		t.Fatalf("transport failure returned uncommitted stream %#v", stream)
	}
	chunk, ok := <-stream.Chunks
	var retryable interface{ Retryable() bool }
	if !ok || chunk.Err == nil || !errors.As(chunk.Err, &retryable) || retryable.Retryable() {
		t.Fatalf("transport failure stream chunk: open=%v error=%v", ok, chunk.Err)
	}
}

func TestExecuteStreamCommitsFirstEventErrors(t *testing.T) {
	cases := map[string]struct {
		events string
		want   string
	}{
		"error event":                {events: "data: {\"type\":\"error\"}\n\n", want: "upstream stream returned an error event"},
		"named error event":          {events: "event: error\ndata: upstream failed\n\n", want: "upstream stream returned an error event"},
		"malformed event":            {events: "data: {not-json}\n\n", want: "decode SSE data event"},
		"empty assistant then error": {events: "data: {\"message\":{\"id\":\"m\",\"author\":{\"role\":\"assistant\"},\"status\":\"in_progress\",\"content\":{\"parts\":[\"\"]}}}\n\ndata: {not-json}\n\n", want: "decode SSE data event"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			newChatGPTWebEventServer(t, tc.events)
			e := NewChatGPTWebExecutor(&config.Config{})
			auth := &cliproxyauth.Auth{Metadata: map[string]any{"cookie": "__Secure-next-auth.session-token=tok"}}
			req := cliproxyexecutor.Request{Model: "gpt-5-6-pro", Payload: []byte(`{"model":"gpt-5-6-pro","input":"hi","stream":true}`)}
			opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, Stream: true}

			stream, err := e.ExecuteStream(t.Context(), auth, req, opts)
			if err != nil {
				t.Fatal(err)
			}
			if stream == nil || !stream.BootstrapCommitted() {
				t.Fatalf("first-event error returned uncommitted stream %#v", stream)
			}
			var streamErr error
			for chunk := range stream.Chunks {
				if len(chunk.Payload) != 0 {
					t.Fatalf("first-event error emitted payload %q", chunk.Payload)
				}
				streamErr = chunk.Err
			}
			if streamErr == nil || !strings.Contains(streamErr.Error(), tc.want) {
				t.Fatalf("first-event stream error = %v", streamErr)
			}
		})
	}
}

func TestExecuteStreamCommitsCookiePersistenceError(t *testing.T) {
	newChatGPTWebEventServer(t, "data: [DONE]\n\n", http.Header{
		"Set-Cookie": {"__Secure-next-auth.session-token=rotated; Path=/; HttpOnly"},
	})
	persistErr := errors.New("persist rotated session token")
	e := NewChatGPTWebExecutor(&config.Config{}, func(context.Context, *cliproxyauth.Auth, map[string]any) (*cliproxyauth.Auth, error) {
		return nil, persistErr
	})
	auth := &cliproxyauth.Auth{ID: "persist-auth", Metadata: map[string]any{"cookie": "__Secure-next-auth.session-token=tok"}}
	req := cliproxyexecutor.Request{Model: "gpt-5-6-pro", Payload: []byte(`{"model":"gpt-5-6-pro","input":"hi","stream":true}`)}
	opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, Stream: true}

	stream, err := e.ExecuteStream(t.Context(), auth, req, opts)
	if err != nil {
		t.Fatal(err)
	}
	if stream == nil || !stream.BootstrapCommitted() {
		t.Fatalf("cookie persistence error returned uncommitted stream %#v", stream)
	}
	chunk, ok := <-stream.Chunks
	if !ok || !errors.Is(chunk.Err, persistErr) || len(chunk.Payload) != 0 {
		t.Fatalf("cookie persistence stream chunk: open=%v payload=%q error=%v", ok, chunk.Payload, chunk.Err)
	}
}

func TestExecuteStreamReturnsBeforeSilentUpstreamBody(t *testing.T) {
	started := make(chan struct{})
	canceled := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/auth/session":
			fmt.Fprint(w, `{"accessToken":"at","user":{"id":"u"},"account":{"id":"a"}}`)
		case "/":
			fmt.Fprint(w, `<html><head data-build="test-build"></head><body></body></html>`)
		case "/backend-api/sentinel/chat-requirements/prepare":
			fmt.Fprint(w, `{"persona":"chatgpt-paid","prepare_token":"prep-tok","proofofwork":{"required":false},"turnstile":{"required":false}}`)
		case "/backend-api/sentinel/chat-requirements":
			fmt.Fprint(w, `{"persona":"chatgpt-paid","token":"final-tok"}`)
		case "/backend-api/f/conversation":
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			close(started)
			<-r.Context().Done()
			close(canceled)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	useChatGPTWebTestURLs(t, srv.URL, srv.URL+"/backend-api", srv.URL+"/backend-api/f/conversation")

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	e := NewChatGPTWebExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Metadata: map[string]any{"cookie": "__Secure-next-auth.session-token=tok"}}
	req := cliproxyexecutor.Request{Model: "gpt-5-6-pro", Payload: []byte(`{"model":"gpt-5-6-pro","input":"hi","stream":true}`)}
	opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, Stream: true}
	type result struct {
		stream *cliproxyexecutor.StreamResult
		err    error
	}
	done := make(chan result, 1)
	go func() {
		stream, err := e.ExecuteStream(ctx, auth, req, opts)
		done <- result{stream: stream, err: err}
	}()

	select {
	case got := <-done:
		if got.err != nil || got.stream == nil || !got.stream.BootstrapCommitted() {
			t.Fatalf("silent upstream result: stream=%v error=%v", got.stream, got.err)
		}
		if interval := got.stream.TakeKeepAliveInterval(); interval == nil || *interval != chatGPTWebStreamKeepAlive {
			t.Fatalf("silent upstream keepalive = %v", interval)
		}
		select {
		case chunk := <-got.stream.Chunks:
			t.Fatalf("silent upstream emitted before cancellation: payload=%q error=%v", chunk.Payload, chunk.Err)
		case <-time.After(100 * time.Millisecond):
		}
		cancel()
		for chunk := range got.stream.Chunks {
			if len(chunk.Payload) != 0 || !errors.Is(chunk.Err, context.Canceled) {
				t.Fatalf("canceled silent stream chunk: payload=%q error=%v", chunk.Payload, chunk.Err)
			}
		}
	case <-time.After(10 * time.Second):
		t.Fatal("ExecuteStream waited for a silent upstream body")
	}
	select {
	case <-canceled:
	case <-time.After(10 * time.Second):
		t.Fatal("canceled preflight left the upstream request open")
	}
}

func TestExecuteStreamOpenAIResponseHasNoDoneArtifact(t *testing.T) {
	events := "data: {\"conversation_id\":\"conv\",\"message\":{\"id\":\"m\",\"author\":{\"role\":\"assistant\"},\"status\":\"finished_successfully\",\"content\":{\"parts\":[\"answer\"]},\"metadata\":{\"finish_details\":{\"type\":\"stop\"}}}}\n\ndata: [DONE]\n\n"
	newChatGPTWebEventServer(t, events)
	e := NewChatGPTWebExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Metadata: map[string]any{"cookie": "__Secure-next-auth.session-token=tok"}}
	req := cliproxyexecutor.Request{Model: "gpt-5-6-pro", Payload: []byte(`{"model":"gpt-5-6-pro","input":"hi","stream":true}`)}
	opts := cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FormatOpenAIResponse,
		Stream:       true,
		Metadata: map[string]any{
			cliproxyexecutor.RequestedModelMetadataKey: "chatgpt-web/gpt-5-6-pro(high)",
		},
	}

	stream, err := e.ExecuteStream(t.Context(), auth, req, opts)
	if err != nil {
		t.Fatal(err)
	}
	var payload strings.Builder
	for chunk := range stream.Chunks {
		if chunk.Err != nil {
			t.Fatal(chunk.Err)
		}
		if !strings.HasPrefix(string(chunk.Payload), "event: ") || !strings.Contains(string(chunk.Payload), "\ndata: ") || !strings.HasSuffix(string(chunk.Payload), "\n\n") || strings.Count(string(chunk.Payload), "\n\n") != 1 {
			t.Fatalf("incomplete or combined OpenAI Responses event: %q", chunk.Payload)
		}
		payload.Write(chunk.Payload)
	}
	if !strings.Contains(payload.String(), `"type":"response.completed"`) {
		t.Fatalf("stream omitted response.completed: %s", payload.String())
	}
	if !strings.Contains(payload.String(), "answer") {
		t.Fatalf("stream omitted the preflight response delta: %s", payload.String())
	}
	if !strings.Contains(payload.String(), `"model":"chatgpt-web/gpt-5-6-pro(high)"`) {
		t.Fatalf("stream omitted the client-requested response model: %s", payload.String())
	}
	if strings.Contains(payload.String(), "[DONE]") {
		t.Fatalf("OpenAI Responses stream emitted a DONE artifact: %s", payload.String())
	}
}

func TestExecuteStreamNonOpenAIFormatsDoNotAppendDone(t *testing.T) {
	events := "data: {\"conversation_id\":\"conv\",\"message\":{\"id\":\"m\",\"author\":{\"role\":\"assistant\"},\"status\":\"finished_successfully\",\"content\":{\"parts\":[\"answer\"]},\"metadata\":{\"finish_details\":{\"type\":\"stop\"}}}}\n\ndata: [DONE]\n\n"
	for _, test := range []struct {
		name    string
		format  sdktranslator.Format
		payload string
		want    string
	}{
		{name: "claude", format: sdktranslator.FormatClaude, payload: `{"model":"gpt-5-6-pro","messages":[{"role":"user","content":"hi"}],"max_tokens":100,"stream":true}`, want: "message_stop"},
		{name: "gemini", format: sdktranslator.FormatGemini, payload: `{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`, want: "totalTokenCount"},
		{name: "gemini cli", format: sdktranslator.FormatGeminiCLI, payload: `{"request":{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}}`, want: "totalTokenCount"},
		{name: "codex", format: sdktranslator.FormatCodex, payload: `{"model":"gpt-5-6-pro","input":"hi","stream":true}`, want: "response.completed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			newChatGPTWebEventServer(t, events)
			e := NewChatGPTWebExecutor(&config.Config{})
			auth := &cliproxyauth.Auth{Metadata: map[string]any{"cookie": "__Secure-next-auth.session-token=tok"}}
			stream, err := e.ExecuteStream(t.Context(), auth, cliproxyexecutor.Request{
				Model:   "gpt-5-6-pro",
				Payload: []byte(test.payload),
			}, cliproxyexecutor.Options{SourceFormat: test.format, Stream: true})
			if err != nil {
				t.Fatal(err)
			}
			var payload strings.Builder
			for chunk := range stream.Chunks {
				if chunk.Err != nil {
					t.Fatal(chunk.Err)
				}
				payload.Write(chunk.Payload)
			}
			if strings.Contains(payload.String(), "[DONE]") || !strings.Contains(payload.String(), test.want) {
				t.Fatalf("terminal payload = %s", payload.String())
			}
		})
	}
}

func TestExecuteOpenAIResponseUsesRequestedModel(t *testing.T) {
	events := "data: {\"conversation_id\":\"conv\",\"message\":{\"id\":\"m\",\"author\":{\"role\":\"assistant\"},\"status\":\"finished_successfully\",\"content\":{\"parts\":[\"answer\"]},\"metadata\":{\"finish_details\":{\"type\":\"stop\"}}}}\n\ndata: [DONE]\n\n"
	newChatGPTWebEventServer(t, events)
	e := NewChatGPTWebExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Metadata: map[string]any{"cookie": "__Secure-next-auth.session-token=tok"}}
	req := cliproxyexecutor.Request{Model: "gpt-5-6-pro", Payload: []byte(`{"model":"gpt-5-6-pro","input":"hi"}`)}
	opts := cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FormatOpenAIResponse,
		Metadata: map[string]any{
			cliproxyexecutor.RequestedModelMetadataKey: "chatgpt-web/gpt-5-6-pro(high)",
		},
	}

	resp, err := e.Execute(t.Context(), auth, req, opts)
	if err != nil {
		t.Fatal(err)
	}
	if got := gjson.GetBytes(resp.Payload, "model").String(); got != "chatgpt-web/gpt-5-6-pro(high)" {
		t.Fatalf("response model = %q, payload = %s", got, resp.Payload)
	}
}

func TestExecuteStreamHandoffDoesNotEmitBeforeResumeError(t *testing.T) {
	events := "data: {\"conversation_id\":\"conv\"}\n\n" +
		"data: {\"type\":\"resume_conversation_token\",\"token\":\"tok\"}\n\n" +
		"data: {\"type\":\"stream_handoff\"}\n\n" +
		"data: [DONE]\n\n"
	newChatGPTWebEventServer(t, events)
	e := NewChatGPTWebExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Metadata: map[string]any{"cookie": "__Secure-next-auth.session-token=tok"}}
	req := cliproxyexecutor.Request{Model: "gpt-5-6-pro", Payload: []byte(`{"model":"gpt-5-6-pro","messages":[{"role":"user","content":"hi"}],"stream":true}`)}
	opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("openai"), Stream: true}

	stream, err := e.ExecuteStream(t.Context(), auth, req, opts)
	if err != nil {
		t.Fatal(err)
	}
	if !stream.BootstrapCommitted() {
		t.Fatal("accepted handoff stream was not marked committed")
	}
	var streamErr error
	for chunk := range stream.Chunks {
		if len(chunk.Payload) != 0 {
			t.Fatalf("handoff emitted response artifacts before continuation failed: %q", chunk.Payload)
		}
		if chunk.Err != nil {
			streamErr = chunk.Err
		}
	}
	if streamErr == nil {
		t.Fatal("resume failure was not reported")
	}
}

func TestExecuteRejectsIncompleteHandoffMetadata(t *testing.T) {
	cases := map[string]string{
		"missing conversation id": "data: {\"type\":\"resume_conversation_token\",\"token\":\"tok\"}\n\ndata: {\"type\":\"stream_handoff\"}\n\ndata: [DONE]\n\n",
		"missing resume token":    "data: {\"conversation_id\":\"conv\"}\n\ndata: {\"type\":\"stream_handoff\"}\n\ndata: [DONE]\n\n",
	}
	for name, events := range cases {
		t.Run(name, func(t *testing.T) {
			newChatGPTWebEventServer(t, events)
			e := NewChatGPTWebExecutor(&config.Config{})
			auth := &cliproxyauth.Auth{Metadata: map[string]any{"cookie": "__Secure-next-auth.session-token=tok"}}
			req := cliproxyexecutor.Request{Model: "gpt-5-6-pro", Payload: []byte(`{"model":"gpt-5-6-pro","messages":[{"role":"user","content":"hi"}]}`)}
			opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("openai")}
			if _, err := e.Execute(context.Background(), auth, req, opts); err == nil || !strings.Contains(err.Error(), "omitted continuation metadata") {
				t.Fatalf("expected incomplete handoff error, got %v", err)
			}
		})
	}
}

func TestExecuteBareDoneWithoutExplicitFinishErrors(t *testing.T) {
	events := "data: {\"conversation_id\":\"conv\",\"message\":{\"id\":\"m\",\"author\":{\"role\":\"assistant\"},\"status\":\"in_progress\",\"content\":{\"parts\":[\"partial\"]}}}\n\n" +
		"data: [DONE]\n\n"
	newChatGPTWebEventServer(t, events)
	e := NewChatGPTWebExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Metadata: map[string]any{"cookie": "__Secure-next-auth.session-token=tok"}}
	req := cliproxyexecutor.Request{Model: "gpt-5-6-pro", Payload: []byte(`{"model":"gpt-5-6-pro","messages":[{"role":"user","content":"hi"}]}`)}
	opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("openai")}

	_, err := e.Execute(context.Background(), auth, req, opts)
	var statusErr cliproxyexecutor.StatusError
	if err == nil || !strings.Contains(err.Error(), "ended before completion") || !errors.As(err, &statusErr) || statusErr.StatusCode() != http.StatusBadGateway {
		t.Fatalf("expected incomplete stream error, got %v", err)
	}
}

func TestExecuteStreamBareDoneWithoutExplicitFinishErrors(t *testing.T) {
	events := "data: {\"conversation_id\":\"conv\",\"message\":{\"id\":\"m\",\"author\":{\"role\":\"assistant\"},\"status\":\"in_progress\",\"content\":{\"parts\":[\"partial\"]}}}\n\n" +
		"data: [DONE]\n\n"
	newChatGPTWebEventServer(t, events)
	e := NewChatGPTWebExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Metadata: map[string]any{"cookie": "__Secure-next-auth.session-token=tok"}}
	req := cliproxyexecutor.Request{Model: "gpt-5-6-pro", Payload: []byte(`{"model":"gpt-5-6-pro","input":"hi","stream":true}`)}
	opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, Stream: true}

	stream, err := e.ExecuteStream(context.Background(), auth, req, opts)
	if err != nil {
		t.Fatal(err)
	}
	var payload strings.Builder
	var streamErr error
	for chunk := range stream.Chunks {
		payload.Write(chunk.Payload)
		if chunk.Err != nil {
			streamErr = chunk.Err
		}
	}
	if streamErr == nil || !strings.Contains(streamErr.Error(), "ended before completion") {
		t.Fatalf("expected incomplete stream error, got %v", streamErr)
	}
	if strings.Contains(payload.String(), "response.completed") {
		t.Fatalf("bare DONE emitted a completion artifact: %s", payload.String())
	}
}

func TestExecuteStreamRevisionEndsWithError(t *testing.T) {
	events := "data: {\"conversation_id\":\"conv\",\"message\":{\"id\":\"m\",\"author\":{\"role\":\"assistant\"},\"status\":\"in_progress\",\"content\":{\"parts\":[\"cat\"]}}}\n\n" +
		"data: {\"conversation_id\":\"conv\",\"message\":{\"id\":\"m\",\"author\":{\"role\":\"assistant\"},\"status\":\"finished_successfully\",\"content\":{\"parts\":[\"car\"]},\"metadata\":{\"finish_details\":{\"type\":\"stop\"}}}}\n\n" +
		"data: [DONE]\n\n"
	newChatGPTWebEventServer(t, events)
	e := NewChatGPTWebExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Metadata: map[string]any{"cookie": "__Secure-next-auth.session-token=tok"}}
	req := cliproxyexecutor.Request{Model: "gpt-5-6-pro", Payload: []byte(`{"model":"gpt-5-6-pro","input":"hi","stream":true}`)}
	opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, Stream: true}
	stream, err := e.ExecuteStream(context.Background(), auth, req, opts)
	if err != nil {
		t.Fatal(err)
	}
	var payload strings.Builder
	var streamErr error
	for chunk := range stream.Chunks {
		payload.Write(chunk.Payload)
		if chunk.Err != nil {
			streamErr = chunk.Err
		}
	}
	if streamErr == nil || !strings.Contains(streamErr.Error(), "revised after emission") {
		t.Fatalf("expected revision error, got %v", streamErr)
	}
	if strings.Contains(payload.String(), "response.completed") {
		t.Fatalf("revised stream emitted a completion artifact: %s", payload.String())
	}
}

func newTruncatedTurnServer(t *testing.T, gates ...chan struct{}) *httptest.Server {
	return newTurnCompletionServer(t, "", gates...)
}

func newTurnCompletionServer(t *testing.T, finishType string, gates ...chan struct{}) *httptest.Server {
	t.Helper()
	var convCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/auth/session":
			fmt.Fprint(w, `{"accessToken":"at","user":{"id":"u"},"account":{"id":"a"}}`)
		case r.URL.Path == "/backend-api/me" || r.URL.Path == "/backend-api/models":
			fmt.Fprint(w, `{}`)
		case r.URL.Path == "/":
			w.Header().Set("Content-Type", "text/html")
			fmt.Fprint(w, `<html><head data-build="test-build"></head><body></body></html>`)
		case r.URL.Path == "/backend-api/sentinel/chat-requirements/prepare":
			fmt.Fprint(w, `{"persona":"chatgpt-paid","prepare_token":"prep-tok","proofofwork":{"required":false},"turnstile":{"required":false}}`)
		case r.URL.Path == "/backend-api/sentinel/chat-requirements":
			fmt.Fprint(w, `{"persona":"chatgpt-paid","token":"final-tok"}`)
		case r.URL.Path == "/backend-api/f/conversation":
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			n := int(convCalls.Add(1))
			fmt.Fprint(w, "data: {\"conversation_id\":\"conv-1\",\"message\":{\"id\":\"m2\",\"author\":{\"role\":\"assistant\"},\"status\":\"in_progress\",\"content\":{\"parts\":[\"Hel\"]}}}\n\n")
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			if n <= len(gates) && gates[n-1] != nil {
				<-gates[n-1]
			}
			if finishType != "" {
				fmt.Fprintf(w, "data: {\"conversation_id\":\"conv-1\",\"message\":{\"id\":\"m2\",\"author\":{\"role\":\"assistant\"},\"status\":\"finished_successfully\",\"content\":{\"parts\":[\"Hello\"]},\"metadata\":{\"finish_details\":{\"type\":%q}}}}\n\ndata: [DONE]\n\n", finishType)
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)

	useChatGPTWebTestURLs(t, srv.URL, srv.URL+"/backend-api", srv.URL+"/backend-api/f/conversation")
	return srv
}

func TestExecuteNonStreamTruncatedErrors(t *testing.T) {
	newTruncatedTurnServer(t)

	e := NewChatGPTWebExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Metadata: map[string]any{"cookie": "__Secure-next-auth.session-token=tok"}}
	req := cliproxyexecutor.Request{
		Model:   "gpt-5-6-pro",
		Payload: []byte(`{"model":"gpt-5-6-pro","messages":[{"role":"user","content":"hi"}]}`),
	}
	opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("openai")}

	_, err := e.Execute(context.Background(), auth, req, opts)
	if err == nil || !strings.Contains(err.Error(), "ended before completion") {
		t.Fatalf("expected truncation error, got %v", err)
	}
}

func TestExecuteStreamTruncatedErrors(t *testing.T) {
	newTruncatedTurnServer(t)

	e := NewChatGPTWebExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Metadata: map[string]any{"cookie": "__Secure-next-auth.session-token=tok"}}
	req := cliproxyexecutor.Request{
		Model:   "gpt-5-6-pro",
		Payload: []byte(`{"model":"gpt-5-6-pro","messages":[{"role":"user","content":"hi"}],"stream":true}`),
	}
	opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("openai"), Stream: true}

	stream, err := e.ExecuteStream(context.Background(), auth, req, opts)
	if err != nil {
		t.Fatal(err)
	}
	var sawErr, sawTerminal error
	var doneCount int
	for chunk := range stream.Chunks {
		if chunk.Err != nil {
			sawErr = chunk.Err
			continue
		}
		if strings.Contains(string(chunk.Payload), "[DONE]") || strings.Contains(string(chunk.Payload), "response.completed") {
			doneCount++
		}
	}
	if sawErr == nil || !strings.Contains(sawErr.Error(), "ended before completion") {
		t.Fatalf("expected truncation error chunk, got %v", sawErr)
	}
	if sawTerminal != nil || doneCount != 0 {
		t.Fatalf("terminal marker emitted for truncated stream: done=%d", doneCount)
	}
}

func TestExecuteNonStreamAcceptsMaxTokensFinishType(t *testing.T) {
	newTurnCompletionServer(t, "max_tokens")

	e := NewChatGPTWebExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Metadata: map[string]any{"cookie": "__Secure-next-auth.session-token=tok"}}
	req := cliproxyexecutor.Request{
		Model:   "gpt-5-6-pro",
		Payload: []byte(`{"model":"gpt-5-6-pro","messages":[{"role":"user","content":"hi"}]}`),
	}
	opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("openai")}

	resp, err := e.Execute(context.Background(), auth, req, opts)
	if err != nil {
		t.Fatalf("max_tokens completion error = %v", err)
	}
	if got := gjson.GetBytes(resp.Payload, "choices.0.message.content").String(); got != "Hello" {
		t.Fatalf("max_tokens completion content = %q, payload = %s", got, resp.Payload)
	}
	if got := gjson.GetBytes(resp.Payload, "choices.0.finish_reason").String(); got != "length" {
		t.Fatalf("max_tokens completion finish reason = %q, payload = %s", got, resp.Payload)
	}
	if got := gjson.GetBytes(resp.Payload, "choices.0.native_finish_reason").String(); got != "max_tokens" {
		t.Fatalf("max_tokens completion native finish reason = %q, payload = %s", got, resp.Payload)
	}
}

func TestExecuteStreamAcceptsMaxTokensFinishType(t *testing.T) {
	newTurnCompletionServer(t, "max_tokens")

	e := NewChatGPTWebExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Metadata: map[string]any{"cookie": "__Secure-next-auth.session-token=tok"}}
	req := cliproxyexecutor.Request{
		Model:   "gpt-5-6-pro",
		Payload: []byte(`{"model":"gpt-5-6-pro","messages":[{"role":"user","content":"hi"}],"stream":true}`),
	}
	opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("openai"), Stream: true}

	stream, err := e.ExecuteStream(context.Background(), auth, req, opts)
	if err != nil {
		t.Fatal(err)
	}
	var payload strings.Builder
	var streamErr error
	for chunk := range stream.Chunks {
		payload.Write(chunk.Payload)
		if chunk.Err != nil {
			streamErr = chunk.Err
		}
	}
	if streamErr != nil {
		t.Fatalf("max_tokens stream error = %v", streamErr)
	}
	if !strings.Contains(payload.String(), `"finish_reason":"length"`) || !strings.Contains(payload.String(), `"native_finish_reason":"max_tokens"`) {
		t.Fatalf("max_tokens stream omitted terminal chunk: %s", payload.String())
	}
}

func TestExecuteNonStreamReportsMaxTokensAsIncompleteResponse(t *testing.T) {
	newTurnCompletionServer(t, "max_tokens")

	e := NewChatGPTWebExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Metadata: map[string]any{"cookie": "__Secure-next-auth.session-token=tok"}}
	req := cliproxyexecutor.Request{
		Model:   "gpt-5-6-pro",
		Payload: []byte(`{"model":"gpt-5-6-pro","input":"hi"}`),
	}
	opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse}

	resp, err := e.Execute(context.Background(), auth, req, opts)
	if err != nil {
		t.Fatalf("max_tokens response error = %v", err)
	}
	if got := gjson.GetBytes(resp.Payload, "status").String(); got != "incomplete" {
		t.Fatalf("max_tokens response status = %q, payload = %s", got, resp.Payload)
	}
	if got := gjson.GetBytes(resp.Payload, "incomplete_details.reason").String(); got != "max_output_tokens" {
		t.Fatalf("max_tokens incomplete reason = %q, payload = %s", got, resp.Payload)
	}
}

func TestExecuteStreamReportsMaxTokensAsIncompleteResponse(t *testing.T) {
	newTurnCompletionServer(t, "max_tokens")

	e := NewChatGPTWebExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Metadata: map[string]any{"cookie": "__Secure-next-auth.session-token=tok"}}
	req := cliproxyexecutor.Request{
		Model:   "gpt-5-6-pro",
		Payload: []byte(`{"model":"gpt-5-6-pro","input":"hi","stream":true}`),
	}
	opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, Stream: true}

	stream, err := e.ExecuteStream(context.Background(), auth, req, opts)
	if err != nil {
		t.Fatal(err)
	}
	var payload strings.Builder
	for chunk := range stream.Chunks {
		if chunk.Err != nil {
			t.Fatalf("max_tokens response stream error = %v", chunk.Err)
		}
		payload.Write(chunk.Payload)
	}
	if !strings.Contains(payload.String(), `"type":"response.incomplete"`) || !strings.Contains(payload.String(), `"reason":"max_output_tokens"`) {
		t.Fatalf("max_tokens response stream omitted incomplete terminal event: %s", payload.String())
	}
	if strings.Contains(payload.String(), `"type":"response.completed"`) {
		t.Fatalf("max_tokens response stream emitted completed terminal event: %s", payload.String())
	}
}

func TestExecuteStreamHoldsTurnLockUntilDrained(t *testing.T) {
	held := make(chan struct{})
	srv := newTruncatedTurnServer(t, held)
	defer srv.Close()

	e := NewChatGPTWebExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Metadata: map[string]any{"cookie": "__Secure-next-auth.session-token=tok"}}
	req := cliproxyexecutor.Request{
		Model:   "gpt-5-6-pro",
		Payload: []byte(`{"model":"gpt-5-6-pro","messages":[{"role":"user","content":"hi"}],"stream":true}`),
	}
	opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("openai"), Stream: true}

	stream, err := e.ExecuteStream(context.Background(), auth, req, opts)
	if err != nil {
		t.Fatal(err)
	}

	key := turnKey(auth, nil)
	acquired := make(chan struct{})
	go func() {
		unlock, _ := e.turns.lock(context.Background(), key)
		unlock()
		close(acquired)
	}()

	select {
	case <-acquired:
		t.Fatal("second lock acquired while the first stream was still held upstream")
	case <-time.After(100 * time.Millisecond):
	}

	close(held)
	for range stream.Chunks {
	}

	select {
	case <-acquired:
	case <-time.After(10 * time.Second):
		t.Fatal("turn lock not released after the first stream drained")
	}
}
