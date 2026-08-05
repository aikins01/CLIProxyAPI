package auth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	chatgptweb "github.com/router-for-me/CLIProxyAPI/v7/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
)

const chatGPTWebTestUserAgent = "Mozilla/5.0 AppleWebKit/537.36 Chrome/140.0.0.0 Safari/537.36"

func chatGPTWebLoginPrompt(cookie string) func(string) (string, error) {
	return func(prompt string) (string, error) {
		if strings.HasPrefix(prompt, "User-Agent") {
			return chatGPTWebTestUserAgent, nil
		}
		return cookie, nil
	}
}

func useChatGPTWebTestURLs(t *testing.T, base string) {
	t.Helper()
	restore := chatgptweb.SetBaseURLsForTesting(base, base+"/backend-api", base+"/backend-api/f/conversation")
	t.Cleanup(restore)
}

func chatGPTWebSessionServer(t *testing.T, status int) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/auth/session" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if got := r.Header.Get("User-Agent"); got != chatGPTWebTestUserAgent {
			t.Errorf("User-Agent = %q", got)
		}
		if status != http.StatusOK {
			w.WriteHeader(status)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"accessToken":"at","user":{"id":"u"},"account":{"id":"a"}}`))
	}))
	t.Cleanup(srv.Close)
	useChatGPTWebTestURLs(t, srv.URL)
	return srv
}

func TestChatGPTWebLoginStoresNormalizedCookieWithoutEcho(t *testing.T) {
	chatGPTWebSessionServer(t, http.StatusOK)
	a := &ChatGPTWebAuthenticator{}
	opts := &LoginOptions{
		Prompt: chatGPTWebLoginPrompt("cf_clearance=abc; __Secure-next-auth.session-token=tok123"),
	}
	auth, err := a.Login(context.Background(), &config.Config{}, opts)
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	cookie, _ := auth.Metadata["cookie"].(string)
	want := "__Secure-next-auth.session-token=tok123; cf_clearance=abc; oai-did=" + chatgptweb.DeviceIDForCookie("__Secure-next-auth.session-token=tok123")
	if cookie != want {
		t.Fatalf("cookie = %q", cookie)
	}
	if auth.Provider != "chatgpt-web" {
		t.Fatalf("provider = %q", auth.Provider)
	}
	if auth.Metadata["user_agent"] != chatGPTWebTestUserAgent {
		t.Fatalf("user agent = %q", auth.Metadata["user_agent"])
	}
	if auth.Metadata["label"] != auth.Label {
		t.Fatalf("stored label = %q", auth.Metadata["label"])
	}
	lastRefresh, ok := auth.Metadata["last_refresh"].(int64)
	if !ok || lastRefresh <= 0 || auth.Metadata["timestamp"] != lastRefresh {
		t.Fatalf("refresh metadata = %#v", auth.Metadata)
	}
	credentialID := strings.TrimSuffix(strings.TrimPrefix(auth.FileName, "chatgpt-web-"), ".json")
	if _, err := uuid.Parse(credentialID); err != nil || auth.ID != auth.FileName {
		t.Fatalf("credential filename = %q, ID = %q", auth.FileName, auth.ID)
	}
}

func TestChatGPTWebRefreshLead(t *testing.T) {
	lead := (ChatGPTWebAuthenticator{}).RefreshLead()
	if lead == nil || *lead != 4*time.Minute {
		t.Fatalf("refresh lead = %v, want %v", lead, 4*time.Minute)
	}
}

func TestChatGPTWebLoginHonorsCancellationBeforePrompt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	prompted := false
	_, err := (ChatGPTWebAuthenticator{}).Login(ctx, &config.Config{}, &LoginOptions{Prompt: func(string) (string, error) {
		prompted = true
		return "", nil
	}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Login error = %v, want context.Canceled", err)
	}
	if prompted {
		t.Fatal("Login prompted after cancellation")
	}
}

func TestChatGPTWebLoginRejectsInvalidSession(t *testing.T) {
	chatGPTWebSessionServer(t, http.StatusUnauthorized)
	a := &ChatGPTWebAuthenticator{}
	opts := &LoginOptions{
		Prompt: chatGPTWebLoginPrompt("__Secure-next-auth.session-token=expired"),
	}
	if _, err := a.Login(context.Background(), &config.Config{}, opts); err == nil {
		t.Fatal("expected login to reject an invalid session cookie")
	}
}

func TestChatGPTWebLoginAcceptsBareTokenWithPadding(t *testing.T) {
	chatGPTWebSessionServer(t, http.StatusOK)
	a := &ChatGPTWebAuthenticator{}
	opts := &LoginOptions{
		Prompt: chatGPTWebLoginPrompt("opaquebase64token=="),
	}
	auth, err := a.Login(context.Background(), &config.Config{}, opts)
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	cookie, _ := auth.Metadata["cookie"].(string)
	if !strings.Contains(cookie, "__Secure-next-auth.session-token=opaquebase64token==") {
		t.Fatalf("bare token must be normalized into the session cookie key, got %q", cookie)
	}
}

func TestChatGPTWebLoginRejectsMissingSessionToken(t *testing.T) {
	t.Parallel()
	a := &ChatGPTWebAuthenticator{}
	opts := &LoginOptions{
		Prompt: chatGPTWebLoginPrompt("cf_clearance=abc"),
	}
	if _, err := a.Login(context.Background(), &config.Config{}, opts); err == nil {
		t.Fatal("expected error for cookie without session token")
	}
}

func TestChatGPTWebLoginRejectsInvalidUserAgent(t *testing.T) {
	a := &ChatGPTWebAuthenticator{}
	opts := &LoginOptions{
		Prompt: func(prompt string) (string, error) {
			if strings.HasPrefix(prompt, "User-Agent") {
				return "curl/8.0", nil
			}
			return "__Secure-next-auth.session-token=tok123", nil
		},
	}
	if _, err := a.Login(context.Background(), &config.Config{}, opts); err == nil {
		t.Fatal("expected invalid browser User-Agent to be rejected")
	}
}

func TestChatGPTWebLoginErrorDoesNotEchoCookie(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = fmt.Fprint(w, `{"detail":"secret-token-value invalid"}`)
	}))
	t.Cleanup(srv.Close)
	useChatGPTWebTestURLs(t, srv.URL)

	a := &ChatGPTWebAuthenticator{}
	opts := &LoginOptions{
		Prompt: chatGPTWebLoginPrompt("__Secure-next-auth.session-token=secret-token-value"),
	}
	_, err := a.Login(context.Background(), &config.Config{}, opts)
	if err == nil {
		t.Fatal("expected error")
	}
	if strings.Contains(err.Error(), "secret-token-value") {
		t.Fatalf("error must not echo cookie or upstream body: %v", err)
	}
}

func TestChatGPTWebLoginDropsUnrelatedCookies(t *testing.T) {
	chatGPTWebSessionServer(t, http.StatusOK)
	a := &ChatGPTWebAuthenticator{}
	opts := &LoginOptions{
		Prompt: chatGPTWebLoginPrompt("Cookie: oai-did=track-me; __Host-next-auth.csrf-token=csrf; __Secure-next-auth.session-token=tok123; _dd_s=rum; cf_clearance=clear"),
	}
	auth, err := a.Login(context.Background(), &config.Config{}, opts)
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	cookie, _ := auth.Metadata["cookie"].(string)
	if cookie != "__Secure-next-auth.session-token=tok123; cf_clearance=clear; oai-did=track-me" {
		t.Fatalf("unrelated cookies must be dropped before persisting, got %q", cookie)
	}
}

func TestChatGPTWebLoginPersistsRotatedToken(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/auth/session" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if !strings.Contains(r.Header.Get("Cookie"), "__Secure-next-auth.session-token=old-tok") {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Add("Set-Cookie", "__Secure-next-auth.session-token=; Max-Age=0; Path=/")
		w.Header().Add("Set-Cookie", "__Secure-next-auth.session-token=new-tok; Path=/; HttpOnly")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"accessToken":"at","user":{"id":"u"},"account":{"id":"a"}}`))
	}))
	t.Cleanup(srv.Close)
	useChatGPTWebTestURLs(t, srv.URL)

	a := &ChatGPTWebAuthenticator{}
	opts := &LoginOptions{
		Prompt: chatGPTWebLoginPrompt("__Secure-next-auth.session-token=old-tok; cf_clearance=clear"),
	}
	auth, err := a.Login(context.Background(), &config.Config{}, opts)
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	cookie, _ := auth.Metadata["cookie"].(string)
	want := "__Secure-next-auth.session-token=new-tok; cf_clearance=clear; oai-did=" + chatgptweb.DeviceIDForCookie("__Secure-next-auth.session-token=old-tok")
	if cookie != want {
		t.Fatalf("rotated token must replace the original, got %q", cookie)
	}
}

func TestChatGPTWebLoginRotatedChunkedToken(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/auth/session" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Add("Set-Cookie", "__Secure-next-auth.session-token.0=partA; Path=/; HttpOnly")
		w.Header().Add("Set-Cookie", "__Secure-next-auth.session-token.1=partB; Path=/; HttpOnly")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"accessToken":"at","user":{"id":"u"},"account":{"id":"a"}}`))
	}))
	t.Cleanup(srv.Close)
	useChatGPTWebTestURLs(t, srv.URL)

	a := &ChatGPTWebAuthenticator{}
	opts := &LoginOptions{
		Prompt: chatGPTWebLoginPrompt("__Secure-next-auth.session-token=old"),
	}
	auth, err := a.Login(context.Background(), &config.Config{}, opts)
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	cookie, _ := auth.Metadata["cookie"].(string)
	want := "__Secure-next-auth.session-token.0=partA; __Secure-next-auth.session-token.1=partB; oai-did=" + chatgptweb.DeviceIDForCookie("__Secure-next-auth.session-token=old")
	if cookie != want {
		t.Fatalf("rotated chunks must be persisted contiguously, got %q", cookie)
	}
	if chatgptweb.SessionToken(cookie) != "partApartB" {
		t.Fatalf("persisted cookie must resolve to the rotated token, got %q", cookie)
	}
}

func TestChatGPTWebLoginMetadataHasNoAccountIDs(t *testing.T) {
	chatGPTWebSessionServer(t, http.StatusOK)
	a := &ChatGPTWebAuthenticator{}
	opts := &LoginOptions{
		Prompt: chatGPTWebLoginPrompt("__Secure-next-auth.session-token=tok123"),
	}
	auth, err := a.Login(context.Background(), &config.Config{}, opts)
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	for _, key := range []string{"access_token", "account_id", "user_id"} {
		if _, ok := auth.Metadata[key]; ok {
			t.Fatalf("metadata must not persist %q", key)
		}
	}
}

func TestChatGPTWebLoginFallsBackToSessionEmail(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"accessToken":"at","user":{"id":"u","email":"verified@example.com"},"account":{"id":"a"}}`))
	}))
	t.Cleanup(srv.Close)
	useChatGPTWebTestURLs(t, srv.URL)

	a := &ChatGPTWebAuthenticator{}
	opts := &LoginOptions{
		Prompt: chatGPTWebLoginPrompt("__Secure-next-auth.session-token=tok123"),
	}
	auth, err := a.Login(context.Background(), &config.Config{}, opts)
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	if auth.Label != "verified@example.com" {
		t.Fatalf("label = %q, want session email fallback", auth.Label)
	}
	if auth.Metadata["email"] != "verified@example.com" {
		t.Fatalf("metadata email = %v", auth.Metadata["email"])
	}

	// An explicit email override wins over the session email.
	opts.Metadata = map[string]string{"email": "override@example.com"}
	auth, err = a.Login(context.Background(), &config.Config{}, opts)
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	if auth.Label != "override@example.com" || auth.Metadata["email"] != "override@example.com" {
		t.Fatalf("explicit email must win: label=%q metadata=%v", auth.Label, auth.Metadata["email"])
	}
}
