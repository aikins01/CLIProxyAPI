package cliproxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	chatgptweb "github.com/router-for-me/CLIProxyAPI/v7/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/runtime/executor"
	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/config"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
)

type chatGPTWebSchedulerStore struct {
	saves chan *coreauth.Auth
}

func useChatGPTWebServiceTestURLs(t *testing.T, base, backend, conversation string) {
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

func (s *chatGPTWebSchedulerStore) List(context.Context) ([]*coreauth.Auth, error) {
	return nil, nil
}

func (s *chatGPTWebSchedulerStore) Save(_ context.Context, auth *coreauth.Auth) (string, error) {
	s.saves <- auth.Clone()
	return auth.ID, nil
}

func (s *chatGPTWebSchedulerStore) Delete(context.Context, string) error {
	return nil
}

func TestEnsureExecutorsForAuth_ChatGPTWebTrimsProvider(t *testing.T) {
	service := &Service{
		cfg:         &config.Config{},
		coreManager: coreauth.NewManager(nil, nil, nil),
	}
	auth := &coreauth.Auth{
		ID:       "chatgpt-web-auth-1",
		Provider: "  ChatGPT-Web ",
		Status:   coreauth.StatusActive,
	}

	service.ensureExecutorsForAuth(auth)
	exec, ok := service.coreManager.Executor("chatgpt-web")
	if !ok || exec == nil {
		t.Fatal("expected chatgpt-web executor despite surrounding whitespace/case")
	}
	if _, ok := exec.(*executor.ChatGPTWebExecutor); !ok {
		t.Fatalf("executor type = %T, want *executor.ChatGPTWebExecutor", exec)
	}
	service.ensureExecutorsForAuth(auth)
	reused, _ := service.coreManager.Executor("chatgpt-web")
	if reused != exec {
		t.Fatal("ordinary auth refresh replaced the stateful ChatGPT web executor")
	}
	service.ensureExecutorsForAuthWithMode(auth, true)
	rebound, _ := service.coreManager.Executor("chatgpt-web")
	if rebound == exec {
		t.Fatal("forced config rebind did not replace the ChatGPT web executor")
	}
}

func TestRegisterModelsForAuthDiscoversChatGPTWebCatalog(t *testing.T) {
	var failCatalog atomic.Bool
	var catalogCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/auth/session":
			_, _ = io.WriteString(w, `{"accessToken":"access-token","user":{"id":"user-id"},"account":{"id":"account-id"}}`)
		case "/backend-api/models":
			catalogCalls.Add(1)
			if failCatalog.Load() {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			_, _ = io.WriteString(w, `{"models":[{"slug":"gpt-5-5-thinking","title":"GPT-5.5 Thinking"},{"slug":"gpt-5-6-pro","title":"GPT-5.6 Pro"}]}`)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	useChatGPTWebServiceTestURLs(t, srv.URL, srv.URL+"/backend-api", "")

	auth := &coreauth.Auth{
		ID:       "chatgpt-web-catalog-auth",
		Provider: "chatgpt-web",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{
			"cookie":     "__Secure-next-auth.session-token=session-token",
			"user_agent": "Mozilla/5.0 AppleWebKit/537.36 Chrome/140.0.0.0 Safari/537.36",
		},
	}
	service := &Service{cfg: &config.Config{}, coreManager: coreauth.NewManager(nil, nil, nil)}
	service.ensureExecutorsForAuth(auth)
	if _, err := service.coreManager.Register(t.Context(), auth); err != nil {
		t.Fatal(err)
	}
	modelRegistry := registry.GetGlobalRegistry()
	modelRegistry.UnregisterClient(auth.ID)
	t.Cleanup(func() { modelRegistry.UnregisterClient(auth.ID) })

	service.registerModelsForAuth(auth)
	if calls := catalogCalls.Load(); calls != 0 {
		t.Fatalf("synchronous model registration made %d remote catalog calls", calls)
	}
	service.refreshChatGPTWebModelsForAuth(t.Context(), auth)
	service.registerModelsForAuth(auth)
	models := modelRegistry.GetModelsForClient(auth.ID)
	ids := make([]string, 0, len(models))
	for _, model := range models {
		ids = append(ids, model.ID)
	}
	if !slices.Equal(ids, []string{"chatgpt-web/gpt-5-5-thinking", "chatgpt-web/gpt-5-6-pro"}) {
		t.Fatalf("registered ChatGPT web models = %#v", ids)
	}

	service.chatGPTWebCatalogMu.Lock()
	entry := service.chatGPTWebCatalogs[auth.ID]
	entry.fetchedAt = time.Time{}
	service.chatGPTWebCatalogs[auth.ID] = entry
	service.chatGPTWebCatalogMu.Unlock()
	failCatalog.Store(true)
	service.refreshChatGPTWebModelsForAuth(t.Context(), auth)
	service.registerModelsForAuth(auth)
	models = modelRegistry.GetModelsForClient(auth.ID)
	ids = ids[:0]
	for _, model := range models {
		ids = append(ids, model.ID)
	}
	if !slices.Equal(ids, []string{"chatgpt-web/gpt-5-5-thinking", "chatgpt-web/gpt-5-6-pro"}) {
		t.Fatalf("failed refresh replaced the last successful catalog: %#v", ids)
	}
	service.refreshChatGPTWebModelsForAuth(t.Context(), auth)
	if calls := catalogCalls.Load(); calls != 2 {
		t.Fatalf("catalog calls after cached failure = %d, want 2", calls)
	}
	failCatalog.Store(false)
	service.chatGPTWebCatalogMu.Lock()
	entry = service.chatGPTWebCatalogs[auth.ID]
	entry.fetchedAt = time.Now().Add(-2 * chatGPTWebCatalogRetryInterval)
	service.chatGPTWebCatalogs[auth.ID] = entry
	service.chatGPTWebCatalogMu.Unlock()
	if refreshed := service.retryFailedChatGPTWebModelRegistrations(t.Context()); refreshed != 1 {
		t.Fatalf("retried failed catalogs = %d, want 1", refreshed)
	}
	if calls := catalogCalls.Load(); calls != 3 {
		t.Fatalf("catalog calls after retry = %d, want 3", calls)
	}
}

func TestChatGPTWebCatalogUpdaterSkipsStatusDisabledAuth(t *testing.T) {
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"models":[{"slug":"gpt-5-6-pro","title":"GPT-5.6 Pro"}]}`)
	}))
	t.Cleanup(srv.Close)
	useChatGPTWebServiceTestURLs(t, srv.URL, srv.URL+"/backend-api", "")

	auth := &coreauth.Auth{
		ID:       "chatgpt-web-disabled-status",
		Provider: "chatgpt-web",
		Status:   coreauth.StatusDisabled,
		Metadata: map[string]any{
			"cookie":     "__Secure-next-auth.session-token=session-token",
			"user_agent": chatgptweb.DefaultUserAgent,
		},
	}
	service := &Service{cfg: &config.Config{}, coreManager: coreauth.NewManager(nil, nil, nil)}
	service.ensureExecutorsForAuth(auth)
	if _, err := service.coreManager.Register(t.Context(), auth); err != nil {
		t.Fatal(err)
	}
	modelRegistry := registry.GetGlobalRegistry()
	modelRegistry.RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "chatgpt-web/stale"}})
	t.Cleanup(func() { modelRegistry.UnregisterClient(auth.ID) })
	service.registerModelsForAuth(auth)
	if models := modelRegistry.GetModelsForClient(auth.ID); len(models) != 0 {
		t.Fatalf("disabled registered models = %#v, want none", models)
	}
	service.chatGPTWebCatalogs = map[string]chatGPTWebCatalogEntry{
		auth.ID: {
			fingerprint:   service.chatGPTWebAuthFingerprint(auth),
			fetchedAt:     time.Now().Add(-2 * chatGPTWebCatalogRetryInterval),
			refreshFailed: true,
		},
	}

	if refreshed, _ := service.refreshChatGPTWebModelRegistrations(t.Context()); refreshed != 0 {
		t.Fatalf("refreshed disabled catalogs = %d, want 0", refreshed)
	}
	if retried := service.retryFailedChatGPTWebModelRegistrations(t.Context()); retried != 0 {
		t.Fatalf("retried disabled catalogs = %d, want 0", retried)
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("disabled catalog requests = %d, want 0", got)
	}
}

func TestRefreshChatGPTWebModelsRejectsOutOfOrderCredentialResult(t *testing.T) {
	oldStarted := make(chan struct{}, 1)
	releaseOld := make(chan struct{})
	var releaseOnce sync.Once
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		cookie := r.Header.Get("Cookie")
		switch r.URL.Path {
		case "/api/auth/session":
			_, _ = io.WriteString(w, `{"accessToken":"access-token","user":{"id":"user-id"},"account":{"id":"account-id"}}`)
		case "/backend-api/models":
			if strings.Contains(cookie, "old-token") {
				oldStarted <- struct{}{}
				<-releaseOld
				_, _ = io.WriteString(w, `{"models":[{"slug":"gpt-5-5-thinking","title":"Old Catalog"}]}`)
				return
			}
			_, _ = io.WriteString(w, `{"models":[{"slug":"gpt-5-6-pro","title":"New Catalog"}]}`)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseOld) }) })
	useChatGPTWebServiceTestURLs(t, srv.URL, srv.URL+"/backend-api", "")

	oldAuth := &coreauth.Auth{
		ID:       "chatgpt-web-catalog-race",
		Provider: "chatgpt-web",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{
			"cookie":     "__Secure-next-auth.session-token=old-token",
			"user_agent": chatgptweb.DefaultUserAgent,
		},
	}
	newAuth := oldAuth.Clone()
	newAuth.Metadata["cookie"] = "__Secure-next-auth.session-token=new-token"
	service := &Service{cfg: &config.Config{}, coreManager: coreauth.NewManager(nil, nil, nil)}
	service.ensureExecutorsForAuth(oldAuth)
	if _, err := service.coreManager.Register(t.Context(), oldAuth); err != nil {
		t.Fatal(err)
	}

	oldDone := make(chan []*ModelInfo, 1)
	go func() {
		oldDone <- service.refreshChatGPTWebModelsForAuth(t.Context(), oldAuth)
	}()
	select {
	case <-oldStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("old catalog request did not start")
	}
	if _, err := service.coreManager.Update(t.Context(), newAuth); err != nil {
		t.Fatal(err)
	}
	newModels := service.refreshChatGPTWebModelsForAuth(t.Context(), newAuth)
	if len(newModels) != 1 || newModels[0].ID != "chatgpt-web/gpt-5-6-pro" {
		t.Fatalf("new catalog = %#v", newModels)
	}
	releaseOnce.Do(func() { close(releaseOld) })
	select {
	case <-oldDone:
	case <-time.After(5 * time.Second):
		t.Fatal("old catalog request did not finish")
	}

	service.chatGPTWebCatalogMu.Lock()
	cached := service.chatGPTWebCatalogs[newAuth.ID]
	service.chatGPTWebCatalogMu.Unlock()
	if cached.fingerprint != service.chatGPTWebAuthFingerprint(newAuth) || len(cached.models) != 1 || cached.models[0].ID != "chatgpt-web/gpt-5-6-pro" {
		t.Fatalf("stale refresh replaced new catalog: %#v", cached)
	}
}

func TestRefreshChatGPTWebModelsRejectsStaleSessionRotationBeforeCatalogFetch(t *testing.T) {
	sessionStarted := make(chan struct{})
	releaseSession := make(chan struct{})
	var releaseOnce sync.Once
	var modelRequests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/auth/session":
			close(sessionStarted)
			<-releaseSession
			w.Header().Set("Set-Cookie", "__Secure-next-auth.session-token=old-rotated-on-session; Path=/; HttpOnly")
			_, _ = io.WriteString(w, `{"accessToken":"access-token","user":{"id":"user-id"},"account":{"id":"account-id"}}`)
		case "/backend-api/models":
			modelRequests.Add(1)
			w.Header().Set("Set-Cookie", "__Secure-next-auth.session-token=old-rotated-on-models; Path=/; HttpOnly")
			_, _ = io.WriteString(w, `{"models":[{"slug":"gpt-5-5-thinking","title":"Old Catalog"}]}`)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseSession) }) })
	useChatGPTWebServiceTestURLs(t, srv.URL, srv.URL+"/backend-api", "")

	oldAuth := &coreauth.Auth{
		ID:       "chatgpt-web-stale-rotation",
		Provider: "chatgpt-web",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{
			"cookie":     "__Secure-next-auth.session-token=old-token",
			"user_agent": chatgptweb.DefaultUserAgent,
		},
	}
	service := &Service{cfg: &config.Config{}, coreManager: coreauth.NewManager(nil, nil, nil)}
	service.ensureExecutorsForAuth(oldAuth)
	registered, err := service.coreManager.Register(t.Context(), oldAuth)
	if err != nil {
		t.Fatal(err)
	}

	refreshDone := make(chan struct{})
	go func() {
		service.refreshChatGPTWebModelsForAuth(t.Context(), registered)
		close(refreshDone)
	}()
	select {
	case <-sessionStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("session request did not start")
	}

	newAuth := registered.Clone()
	newAuth.Metadata["cookie"] = "__Secure-next-auth.session-token=new-token"
	if _, err = service.coreManager.Update(t.Context(), newAuth); err != nil {
		t.Fatal(err)
	}
	releaseOnce.Do(func() { close(releaseSession) })
	select {
	case <-refreshDone:
	case <-time.After(5 * time.Second):
		t.Fatal("stale refresh did not finish")
	}

	current, ok := service.coreManager.GetByID(oldAuth.ID)
	if !ok || chatgptweb.SessionToken(current.Metadata["cookie"].(string)) != "new-token" {
		t.Fatalf("stale rotation replaced current credential: %#v", current)
	}
	if got := modelRequests.Load(); got != 0 {
		t.Fatalf("stale session fetched model catalog %d times", got)
	}
}

func TestRefreshChatGPTWebModelsDoesNotReregisterRemovedAuth(t *testing.T) {
	refreshStarted := make(chan struct{})
	releaseRefresh := make(chan struct{})
	var releaseOnce sync.Once
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/auth/session":
			_, _ = io.WriteString(w, `{"accessToken":"access-token","user":{"id":"user-id"},"account":{"id":"account-id"}}`)
		case "/backend-api/models":
			close(refreshStarted)
			<-releaseRefresh
			_, _ = io.WriteString(w, `{"models":[{"slug":"gpt-5-6-pro","title":"GPT-5.6 Pro"}]}`)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseRefresh) }) })
	useChatGPTWebServiceTestURLs(t, srv.URL, srv.URL+"/backend-api", "")

	auth := &coreauth.Auth{
		ID:       "chatgpt-web-removal-race",
		Provider: "chatgpt-web",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{
			"cookie":     "__Secure-next-auth.session-token=session-token",
			"user_agent": chatgptweb.DefaultUserAgent,
		},
	}
	service := &Service{cfg: &config.Config{}, coreManager: coreauth.NewManager(nil, nil, nil)}
	service.ensureExecutorsForAuth(auth)
	if _, err := service.coreManager.Register(t.Context(), auth); err != nil {
		t.Fatal(err)
	}
	modelRegistry := registry.GetGlobalRegistry()
	t.Cleanup(func() { modelRegistry.UnregisterClient(auth.ID) })

	refreshDone := make(chan bool, 1)
	go func() {
		refreshDone <- service.refreshModelRegistrationForAuthUncoalesced(t.Context(), auth)
	}()
	select {
	case <-refreshStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("catalog refresh did not start")
	}
	service.applyCoreAuthRemoval(t.Context(), auth.ID)
	releaseOnce.Do(func() { close(releaseRefresh) })
	select {
	case refreshed := <-refreshDone:
		if refreshed {
			t.Fatal("removed auth refresh reported success")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("catalog refresh did not finish")
	}
	if models := modelRegistry.GetModelsForClient(auth.ID); len(models) != 0 {
		t.Fatalf("removed auth models = %#v, want none", models)
	}
	service.modelRegistrationMu.Lock()
	defer service.modelRegistrationMu.Unlock()
	if len(service.modelRegistrations) != 0 {
		t.Fatalf("idle model registration locks = %d, want 0", len(service.modelRegistrations))
	}
}

func TestChatGPTWebCatalogFingerprintUsesGlobalProxyFallback(t *testing.T) {
	cfg := &config.Config{}
	cfg.ProxyURL = " http://proxy-a.example:8080 "
	service := &Service{cfg: cfg, coreManager: coreauth.NewManager(nil, nil, nil)}
	auth := &coreauth.Auth{
		ID:       "chatgpt-web-global-proxy",
		Provider: "chatgpt-web",
		Metadata: map[string]any{
			"cookie":     "__Secure-next-auth.session-token=proxy-token",
			"user_agent": chatgptweb.DefaultUserAgent,
		},
	}
	fingerprintA := service.chatGPTWebAuthFingerprint(auth)
	service.chatGPTWebCatalogs = map[string]chatGPTWebCatalogEntry{
		auth.ID: {fingerprint: fingerprintA, models: []*ModelInfo{{ID: "proxy-a-only-model"}}},
	}

	cfg.ProxyURL = "http://proxy-b.example:8080"
	fingerprintB := service.chatGPTWebAuthFingerprint(auth)
	if fingerprintA == fingerprintB {
		t.Fatal("global proxy change did not change the catalog fingerprint")
	}
	for _, model := range service.chatGPTWebModelsForAuth(auth) {
		if model.ID == "proxy-a-only-model" {
			t.Fatal("catalog fetched through the old global proxy remained cached")
		}
	}
}

func TestApplyConfigUpdateRefreshesChatGPTWebCatalogAfterProxyChange(t *testing.T) {
	modelRequested := make(chan struct{})
	var modelRequestOnce sync.Once
	proxyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/auth/session":
			_, _ = io.WriteString(w, `{"accessToken":"access-token","user":{"id":"user-id"},"account":{"id":"account-id"}}`)
		case "/backend-api/models":
			modelRequestOnce.Do(func() { close(modelRequested) })
			_, _ = io.WriteString(w, `{"models":[{"slug":"gpt-5-6-pro","title":"Proxy Catalog"}]}`)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(proxyServer.Close)
	useChatGPTWebServiceTestURLs(t, "http://chatgpt.invalid", "http://chatgpt.invalid/backend-api", "")

	cfg := &config.Config{}
	manager := coreauth.NewManager(nil, nil, nil)
	service := &Service{cfg: cfg, coreManager: manager, runtimeContext: t.Context()}
	auth := &coreauth.Auth{
		ID:       "chatgpt-web-proxy-refresh",
		Provider: "chatgpt-web",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{
			"cookie":     "__Secure-next-auth.session-token=proxy-token",
			"user_agent": chatgptweb.DefaultUserAgent,
		},
	}
	registered, err := manager.Register(t.Context(), auth)
	if err != nil {
		t.Fatal(err)
	}
	service.chatGPTWebCatalogs = map[string]chatGPTWebCatalogEntry{
		auth.ID: {
			models:      []*ModelInfo{{ID: "chatgpt-web/old-proxy-model"}},
			fingerprint: service.chatGPTWebAuthFingerprint(registered),
			fetchedAt:   time.Now(),
			discovered:  true,
		},
	}
	modelRegistry := registry.GetGlobalRegistry()
	modelRegistry.RegisterClient(auth.ID, auth.Provider, service.chatGPTWebCatalogs[auth.ID].models)
	t.Cleanup(func() { modelRegistry.UnregisterClient(auth.ID) })

	newCfg := &config.Config{}
	newCfg.ProxyURL = proxyServer.URL
	service.applyConfigUpdate(newCfg)
	select {
	case <-modelRequested:
	case <-time.After(5 * time.Second):
		t.Fatal("proxy change did not trigger a ChatGPT web catalog refresh")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		models := modelRegistry.GetModelsForClient(auth.ID)
		if len(models) == 1 && models[0].ID == "chatgpt-web/gpt-5-6-pro" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("catalog after proxy change = %#v", models)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestChatGPTWebModelsForAuthKeepsSuccessfulEmptyCatalog(t *testing.T) {
	service := &Service{cfg: &config.Config{}, coreManager: coreauth.NewManager(nil, nil, nil)}
	auth := &coreauth.Auth{
		ID:       "chatgpt-web-empty-catalog",
		Provider: "chatgpt-web",
		Metadata: map[string]any{
			"cookie":     "__Secure-next-auth.session-token=empty-catalog-token",
			"user_agent": chatgptweb.DefaultUserAgent,
		},
	}
	fingerprint := service.chatGPTWebAuthFingerprint(auth)
	service.chatGPTWebCatalogs = map[string]chatGPTWebCatalogEntry{
		auth.ID: {fingerprint: fingerprint, models: []*ModelInfo{}, fetchedAt: time.Now(), discovered: true},
	}
	if models := service.chatGPTWebModelsForAuth(auth); len(models) != 0 {
		t.Fatalf("successful empty catalog was replaced with fallback models: %#v", models)
	}
}

func TestChatGPTWebAutoRefreshPersistsRotatedCookie(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/auth/session" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Set-Cookie", "__Secure-next-auth.session-token=scheduler-rotated; Path=/; HttpOnly")
		_, _ = io.WriteString(w, `{"accessToken":"access-token","user":{"id":"user-id"},"account":{"id":"account-id"}}`)
	}))
	t.Cleanup(srv.Close)
	useChatGPTWebServiceTestURLs(t, srv.URL, srv.URL+"/backend-api", "")

	store := &chatGPTWebSchedulerStore{saves: make(chan *coreauth.Auth, 8)}
	manager := coreauth.NewManager(store, nil, nil)
	manager.RegisterExecutor(executor.NewChatGPTWebExecutor(&config.Config{}))
	auth := &coreauth.Auth{
		ID:       "chatgpt-web-scheduled-refresh",
		Provider: "chatgpt-web",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{
			"cookie":     "__Secure-next-auth.session-token=scheduler-original",
			"user_agent": chatgptweb.DefaultUserAgent,
		},
	}
	if _, err := manager.Register(t.Context(), auth); err != nil {
		t.Fatal(err)
	}
	manager.StartAutoRefresh(t.Context(), time.Millisecond)
	t.Cleanup(manager.StopAutoRefresh)

	deadline := time.After(5 * time.Second)
	for {
		select {
		case saved := <-store.saves:
			cookie, _ := saved.Metadata["cookie"].(string)
			if strings.Contains(cookie, "scheduler-rotated") {
				lastRefresh, ok := saved.Metadata["last_refresh"].(int64)
				if !ok || lastRefresh <= 0 {
					t.Fatalf("persisted last_refresh = %v", saved.Metadata["last_refresh"])
				}
				current, ok := manager.GetByID(auth.ID)
				if !ok || current.Metadata["cookie"] != cookie {
					t.Fatalf("manager cookie = %v, persisted cookie = %q", current, cookie)
				}
				return
			}
		case <-deadline:
			t.Fatal("scheduled refresh did not persist the rotated cookie")
		}
	}
}

func TestChatGPTWebConversationRotationPersistsAcrossRebindAndRestart(t *testing.T) {
	var conversationCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/auth/session":
			_, _ = io.WriteString(w, `{"accessToken":"access-token","user":{"id":"user-id"},"account":{"id":"account-id"}}`)
		case "/", "/backend-api/me", "/backend-api/conversations", "/backend-api/models":
			if r.URL.Path == "/" {
				w.Header().Set("Content-Type", "text/html")
				_, _ = io.WriteString(w, `<html><head data-build="test-build"></head><body></body></html>`)
				return
			}
			_, _ = io.WriteString(w, `{}`)
		case "/backend-api/sentinel/chat-requirements/prepare":
			_, _ = io.WriteString(w, `{"persona":"chatgpt-paid","prepare_token":"prepare-token","proofofwork":{"required":false},"turnstile":{"required":false}}`)
		case "/backend-api/sentinel/chat-requirements":
			_, _ = io.WriteString(w, `{"persona":"chatgpt-paid","token":"requirements-token"}`)
		case "/backend-api/f/conversation":
			call := conversationCalls.Add(1)
			expectedToken := "conversation-rotated"
			if call == 1 {
				expectedToken = "conversation-original"
			} else if call == 5 {
				expectedToken = "error-rotated-4"
			}
			if chatgptweb.SessionToken(r.Header.Get("Cookie")) != expectedToken {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			if call == 1 {
				w.Header().Set("Set-Cookie", "__Secure-next-auth.session-token=conversation-rotated; Path=/; HttpOnly")
			}
			if call >= 4 {
				w.Header().Set("Set-Cookie", fmt.Sprintf("__Secure-next-auth.session-token=error-rotated-%d; Path=/; HttpOnly", call))
				w.WriteHeader(http.StatusForbidden)
				return
			}
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "data: {\"conversation_id\":\"conv\",\"message\":{\"id\":\"m\",\"author\":{\"role\":\"assistant\"},\"status\":\"finished_successfully\",\"content\":{\"parts\":[\"ok\"]},\"metadata\":{\"finish_details\":{\"type\":\"stop\"}}}}\n\ndata: [DONE]\n\n")
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	useChatGPTWebServiceTestURLs(t, srv.URL, srv.URL+"/backend-api", srv.URL+"/backend-api/f/conversation")

	store := &chatGPTWebSchedulerStore{saves: make(chan *coreauth.Auth, 8)}
	manager := coreauth.NewManager(store, nil, nil)
	service := &Service{cfg: &config.Config{}, coreManager: manager}
	auth := &coreauth.Auth{
		ID:       "chatgpt-web-conversation-rotation",
		Provider: "chatgpt-web",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{
			"cookie":     "__Secure-next-auth.session-token=conversation-original",
			"user_agent": chatgptweb.DefaultUserAgent,
		},
	}
	service.ensureExecutorsForAuth(auth)
	if _, err := manager.Register(t.Context(), auth); err != nil {
		t.Fatal(err)
	}
	<-store.saves
	exec, ok := manager.Executor("chatgpt-web")
	if !ok {
		t.Fatal("chatgpt-web executor was not registered")
	}
	req := cliproxyexecutor.Request{Model: "gpt-5-6-pro", Payload: []byte(`{"model":"gpt-5-6-pro","input":"hi"}`)}
	opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse}
	if _, err := exec.Execute(t.Context(), auth, req, opts); err != nil {
		t.Fatal(err)
	}
	var saved *coreauth.Auth
	select {
	case saved = <-store.saves:
	case <-time.After(5 * time.Second):
		t.Fatal("conversation token rotation was not persisted")
	}
	if chatgptweb.SessionToken(saved.Metadata["cookie"].(string)) != "conversation-rotated" {
		t.Fatalf("persisted cookie = %v", saved.Metadata["cookie"])
	}
	current, ok := manager.GetByID(auth.ID)
	if !ok || chatgptweb.SessionToken(current.Metadata["cookie"].(string)) != "conversation-rotated" {
		t.Fatalf("manager did not retain rotated cookie: %#v", current)
	}

	service.ensureExecutorsForAuthWithMode(current, true)
	rebound, _ := manager.Executor("chatgpt-web")
	if _, err := rebound.Execute(t.Context(), current, req, opts); err != nil {
		t.Fatalf("rebound executor did not use rotated cookie: %v", err)
	}

	restartedManager := coreauth.NewManager(nil, nil, nil)
	restartedService := &Service{cfg: &config.Config{}, coreManager: restartedManager}
	restartedService.ensureExecutorsForAuth(saved)
	if _, err := restartedManager.Register(t.Context(), saved); err != nil {
		t.Fatal(err)
	}
	restarted, _ := restartedManager.Executor("chatgpt-web")
	if _, err := restarted.Execute(t.Context(), saved, req, opts); err != nil {
		t.Fatalf("restarted executor did not use persisted cookie: %v", err)
	}
	_, err := restarted.Execute(t.Context(), saved, req, opts)
	var statusErr cliproxyexecutor.StatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode() != http.StatusForbidden {
		t.Fatalf("rotating error response = %v", err)
	}
	current, _ = restartedManager.GetByID(saved.ID)
	if chatgptweb.SessionToken(current.Metadata["cookie"].(string)) != "error-rotated-4" {
		t.Fatalf("non-stream error rotation was not persisted: %#v", current.Metadata)
	}
	if _, err = restarted.ExecuteStream(t.Context(), current, req, opts); !errors.As(err, &statusErr) || statusErr.StatusCode() != http.StatusForbidden {
		t.Fatalf("streaming rotating error response = %v", err)
	}
	current, _ = restartedManager.GetByID(saved.ID)
	if chatgptweb.SessionToken(current.Metadata["cookie"].(string)) != "error-rotated-5" {
		t.Fatalf("stream error rotation was not persisted: %#v", current.Metadata)
	}
	if conversationCalls.Load() != 5 {
		t.Fatalf("conversation calls = %d, want 5", conversationCalls.Load())
	}
}

func TestChatGPTWebTurnstileHardStopDoesNotSuspendModel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/auth/session":
			_, _ = io.WriteString(w, `{"accessToken":"access-token","user":{"id":"user-id"},"account":{"id":"account-id"}}`)
		case "/":
			w.Header().Set("Content-Type", "text/html")
			_, _ = io.WriteString(w, `<html><head data-build="test-build"></head><body></body></html>`)
		case "/backend-api/sentinel/chat-requirements/prepare":
			_, _ = io.WriteString(w, `{"prepare_token":"prepare-token"}`)
		case "/backend-api/sentinel/chat-requirements":
			_, _ = io.WriteString(w, `{"token":"requirements-token","proofofwork":{"required":false},"turnstile":{"required":true}}`)
		case "/backend-api/f/conversation":
			if got := r.Header.Get("openai-sentinel-turnstile-token"); got != "captured-turnstile-token" {
				t.Errorf("turnstile header = %q", got)
			}
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "data: {\"conversation_id\":\"conv\",\"message\":{\"id\":\"m\",\"author\":{\"role\":\"assistant\"},\"status\":\"finished_successfully\",\"content\":{\"parts\":[\"ok\"]},\"metadata\":{\"finish_details\":{\"type\":\"stop\"}}}}\n\ndata: [DONE]\n\n")
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	useChatGPTWebServiceTestURLs(t, srv.URL, srv.URL+"/backend-api", srv.URL+"/backend-api/f/conversation")

	model := "gpt-5-6-pro"
	auth := &coreauth.Auth{
		ID:       "chatgpt-web-turnstile-manager",
		Provider: "chatgpt-web",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{
			"cookie":     "__Secure-next-auth.session-token=turnstile-token",
			"user_agent": chatgptweb.DefaultUserAgent,
		},
	}
	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() { reg.UnregisterClient(auth.ID) })
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor.NewChatGPTWebExecutor(&config.Config{}))
	if _, err := manager.Register(t.Context(), auth); err != nil {
		t.Fatal(err)
	}
	req := cliproxyexecutor.Request{
		Model:   model,
		Payload: []byte(`{"model":"gpt-5-6-pro","input":"hi"}`),
	}
	opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse}
	_, err := manager.Execute(t.Context(), []string{auth.Provider}, req, opts)
	if !errors.Is(err, chatgptweb.ErrTurnstileRequired) {
		t.Fatalf("first request error = %v, want Turnstile requirement", err)
	}
	var statusErr interface{ StatusCode() int }
	if !errors.As(err, &statusErr) || statusErr.StatusCode() != http.StatusForbidden {
		t.Fatalf("first request status error = %v", err)
	}
	current, ok := manager.GetByID(auth.ID)
	if !ok || current.Unavailable || !current.NextRetryAfter.IsZero() {
		t.Fatalf("Turnstile hard stop changed auth availability: %#v", current)
	}
	if state := current.ModelStates[model]; state != nil && (state.Unavailable || !state.NextRetryAfter.IsZero()) {
		t.Fatalf("Turnstile hard stop suspended model: %#v", state)
	}
	current.Metadata["turnstile_token"] = "captured-turnstile-token"
	if _, err := manager.Update(t.Context(), current); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Execute(t.Context(), []string{auth.Provider}, req, opts); err != nil {
		t.Fatalf("corrected credential did not route immediately: %v", err)
	}
}

func TestChatGPTWebRefreshContextUsesServiceLifetime(t *testing.T) {
	serviceCtx, stopService := context.WithCancel(t.Context())
	service := &Service{runtimeContext: serviceCtx}
	requestCtx, cancelRequest := context.WithCancel(t.Context())
	refreshCtx := service.chatGPTWebRefreshContext(requestCtx)
	cancelRequest()
	if err := refreshCtx.Err(); err != nil {
		t.Fatalf("refresh context ended with request: %v", err)
	}
	stopService()
	if err := refreshCtx.Err(); !errors.Is(err, context.Canceled) {
		t.Fatalf("refresh context did not end with service: %v", err)
	}
}

func TestQueueModelRegistrationRefreshCoalesces(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	var catalogCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/auth/session":
			_, _ = io.WriteString(w, `{"accessToken":"access-token","user":{"id":"user-id"},"account":{"id":"account-id"}}`)
		case "/backend-api/models":
			catalogCalls.Add(1)
			started <- struct{}{}
			<-release
			_, _ = io.WriteString(w, `{"models":[{"slug":"gpt-5-6-pro","title":"GPT-5.6 Pro"}]}`)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	useChatGPTWebServiceTestURLs(t, srv.URL, srv.URL+"/backend-api", "")

	auth := &coreauth.Auth{
		ID:       "chatgpt-web-catalog-coalesced",
		Provider: "chatgpt-web",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{
			"cookie":     "__Secure-next-auth.session-token=coalesced-token",
			"user_agent": chatgptweb.DefaultUserAgent,
		},
	}
	serviceCtx, stopService := context.WithCancel(t.Context())
	t.Cleanup(stopService)
	service := &Service{cfg: &config.Config{}, coreManager: coreauth.NewManager(nil, nil, nil), runtimeContext: serviceCtx}
	service.ensureExecutorsForAuth(auth)
	if _, err := service.coreManager.Register(t.Context(), auth); err != nil {
		t.Fatal(err)
	}
	registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })

	for range 8 {
		service.queueModelRegistrationRefresh(serviceCtx, auth.Clone())
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("catalog refresh did not start")
	}
	if calls := catalogCalls.Load(); calls != 1 {
		t.Fatalf("coalesced catalog calls = %d, want 1", calls)
	}
	releaseOnce.Do(func() { close(release) })
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		models := service.chatGPTWebModelsForAuth(auth)
		if len(models) == 1 && models[0].ID == "chatgpt-web/gpt-5-6-pro" {
			if calls := catalogCalls.Load(); calls != 1 {
				t.Fatalf("coalesced catalog calls = %d, want 1", calls)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("coalesced catalog refresh did not publish its result")
}

func TestChatGPTWebAuthFingerprintFallsBackToSessionToken(t *testing.T) {
	service := &Service{}

	sessionOnly := &coreauth.Auth{
		Metadata: map[string]any{
			"session_token": "__Secure-next-auth.session-token=session-token",
		},
	}
	bothUsable := &coreauth.Auth{
		Metadata: map[string]any{
			"cookie":        "__Secure-next-auth.session-token=cookie-token",
			"session_token": "__Secure-next-auth.session-token=session-token",
		},
	}
	unusableCookie := &coreauth.Auth{
		Metadata: map[string]any{
			"cookie":        "irrelevant=1; other=2",
			"session_token": "__Secure-next-auth.session-token=session-token",
		},
	}

	if got := service.chatGPTWebAuthFingerprint(unusableCookie); got != service.chatGPTWebAuthFingerprint(sessionOnly) {
		t.Fatal("unusable cookie must not suppress the session_token fallback")
	}
	if got := service.chatGPTWebAuthFingerprint(bothUsable); got == service.chatGPTWebAuthFingerprint(sessionOnly) {
		t.Fatal("usable cookie must keep its own fingerprint")
	}
}

func TestChatGPTWebModelCandidates(t *testing.T) {
	discovered := &ModelInfo{ID: "chatgpt-web/gpt-5-5-thinking", Version: "gpt-5-5-thinking"}
	if ids := chatGPTWebModelCandidates(discovered); !slices.Contains(ids, "chatgpt-web/gpt-5-5-thinking") || !slices.Contains(ids, "gpt-5-5-thinking") {
		t.Fatalf("discovered model candidates = %v", ids)
	}

	static := &ModelInfo{ID: "chatgpt-web/gpt-5-6-pro"}
	if ids := chatGPTWebModelCandidates(static); !slices.Contains(ids, "gpt-5-6-pro") {
		t.Fatalf("static model candidates must derive the bare slug, got %v", ids)
	}

	if ids := chatGPTWebModelCandidates(nil); ids != nil {
		t.Fatalf("nil model candidates = %v, want nil", ids)
	}
}

func TestApplyExcludedModelsMatchingChatGPTWeb(t *testing.T) {
	models := []*ModelInfo{
		{ID: "chatgpt-web/gpt-5-6-pro"},
		{ID: "chatgpt-web/gpt-5-5-thinking", Version: "gpt-5-5-thinking"},
	}

	remaining := applyExcludedModelsMatching(models, []string{"gpt-5-6-pro"}, chatGPTWebModelCandidates)
	if len(remaining) != 1 || remaining[0].ID != "chatgpt-web/gpt-5-5-thinking" {
		t.Fatalf("bare slug exclusion left %v", modelIDs(remaining))
	}

	remaining = applyExcludedModelsMatching(models, []string{"chatgpt-web/gpt-5-5-thinking"}, chatGPTWebModelCandidates)
	if len(remaining) != 1 || remaining[0].ID != "chatgpt-web/gpt-5-6-pro" {
		t.Fatalf("namespaced exclusion left %v", modelIDs(remaining))
	}

	remaining = applyExcludedModelsMatching(models, []string{"gpt-5-*"}, chatGPTWebModelCandidates)
	if len(remaining) != 0 {
		t.Fatalf("wildcard exclusion left %v", modelIDs(remaining))
	}

	remaining = applyExcludedModelsMatching(models, []string{"claude/gpt-5-6-pro"}, chatGPTWebModelCandidates)
	if len(remaining) != 2 {
		t.Fatalf("unrelated namespaced pattern excluded %v", modelIDs(remaining))
	}
}

func TestApplyExcludedModelsPreservesIDOnlyBehavior(t *testing.T) {
	models := []*ModelInfo{
		{ID: "provider/model-a", Version: "model-a"},
		{ID: "provider/model-b"},
	}
	remaining := applyExcludedModels(models, []string{"model-a"})
	if len(remaining) != 2 {
		t.Fatalf("default matcher must not match Version, left %v", modelIDs(remaining))
	}
	remaining = applyExcludedModels(models, []string{"provider/model-a"})
	if len(remaining) != 1 || remaining[0].ID != "provider/model-b" {
		t.Fatalf("default matcher full-ID exclusion left %v", modelIDs(remaining))
	}
}

func TestRegisterModelsForAuthChatGPTWebExcludesBareSlug(t *testing.T) {
	service := &Service{
		cfg: &config.Config{
			OAuthExcludedModels: map[string][]string{
				"chatgpt-web": {"gpt-5-6-pro"},
			},
		},
		coreManager: coreauth.NewManager(nil, nil, nil),
	}
	auth := &coreauth.Auth{
		ID:       "chatgpt-web-exclusion-auth",
		Provider: "chatgpt-web",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{
			"cookie":     "__Secure-next-auth.session-token=session-token",
			"user_agent": chatgptweb.DefaultUserAgent,
		},
	}

	modelRegistry := registry.GetGlobalRegistry()
	modelRegistry.UnregisterClient(auth.ID)
	t.Cleanup(func() { modelRegistry.UnregisterClient(auth.ID) })

	service.registerModelsForAuth(auth)

	models := modelRegistry.GetModelsForClient(auth.ID)
	if len(models) != 0 {
		t.Fatalf("expected static fallback to be excluded by bare slug, got %v", modelIDs(models))
	}
}

func TestChatGPTWebCredentialFingerprintNormalizesDefaultUserAgent(t *testing.T) {
	cookie := "__Secure-next-auth.session-token=ua-token"
	missing := &coreauth.Auth{Metadata: map[string]any{"cookie": cookie}}
	explicit := &coreauth.Auth{Metadata: map[string]any{"cookie": cookie, "user_agent": chatgptweb.DefaultUserAgent}}
	blank := &coreauth.Auth{Metadata: map[string]any{"cookie": cookie, "user_agent": "  "}}
	nonString := &coreauth.Auth{Metadata: map[string]any{"cookie": cookie, "user_agent": 42}}

	if got := chatGPTWebCredentialFingerprint(missing); got != chatGPTWebCredentialFingerprint(explicit) {
		t.Fatal("missing user_agent must fingerprint like the executor default")
	}
	if got := chatGPTWebCredentialFingerprint(blank); got == chatGPTWebCredentialFingerprint(explicit) {
		t.Fatal("blank user_agent is rejected by the executor and must not fingerprint like the default")
	}
	if got := chatGPTWebCredentialFingerprint(nonString); got == chatGPTWebCredentialFingerprint(explicit) {
		t.Fatal("non-string user_agent is rejected by the executor and must not fingerprint like the default")
	}
}

func TestRunBoundedChatGPTWebRefreshesSharesRefreshSlot(t *testing.T) {
	blocked := make(chan struct{}, chatGPTWebRefreshConcurrency)
	release := make(chan struct{})
	var releaseOnce sync.Once
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/auth/session":
			_, _ = io.WriteString(w, `{"accessToken":"access-token","user":{"id":"user-id"},"account":{"id":"account-id"}}`)
		case "/backend-api/models":
			blocked <- struct{}{}
			<-release
			_, _ = io.WriteString(w, `{"models":[{"slug":"gpt-5-6-pro","title":"GPT-5.6 Pro"}]}`)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	useChatGPTWebServiceTestURLs(t, srv.URL, srv.URL+"/backend-api", "")

	service := &Service{cfg: &config.Config{}, coreManager: coreauth.NewManager(nil, nil, nil)}
	service.chatGPTWebCatalogs = make(map[string]chatGPTWebCatalogEntry)
	auths := make([]*coreauth.Auth, 0, chatGPTWebRefreshConcurrency+1)
	for i := range chatGPTWebRefreshConcurrency + 1 {
		auth := &coreauth.Auth{
			ID:       fmt.Sprintf("chatgpt-web-bounded-%d", i),
			Provider: "chatgpt-web",
			Status:   coreauth.StatusActive,
			Metadata: map[string]any{
				"cookie":     "__Secure-next-auth.session-token=session-token",
				"user_agent": chatgptweb.DefaultUserAgent,
			},
		}
		service.ensureExecutorsForAuth(auth)
		if _, err := service.coreManager.Register(t.Context(), auth); err != nil {
			t.Fatal(err)
		}
		service.chatGPTWebCatalogs[auth.ID] = chatGPTWebCatalogEntry{
			fingerprint: service.chatGPTWebAuthFingerprint(auth),
			fetchedAt:   time.Now().Add(-2 * chatGPTWebCatalogCacheTTL),
		}
		auths = append(auths, auth)
	}
	t.Cleanup(func() {
		for _, auth := range auths {
			registry.GetGlobalRegistry().UnregisterClient(auth.ID)
		}
	})

	done := make(chan int, 1)
	go func() {
		done <- service.runBoundedChatGPTWebRefreshes(t.Context(), auths)
	}()
	for i := range chatGPTWebRefreshConcurrency {
		select {
		case <-blocked:
		case <-time.After(5 * time.Second):
			t.Fatalf("discovery %d did not start", i)
		}
	}
	select {
	case <-blocked:
		t.Fatal("more discoveries ran than the refresh slot bound allows")
	case <-time.After(200 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case refreshed := <-done:
		if refreshed != len(auths) {
			t.Fatalf("refreshed = %d, want %d", refreshed, len(auths))
		}
	case <-time.After(10 * time.Second):
		t.Fatal("bounded refresh batch did not finish")
	}
}

func TestRunBoundedChatGPTWebRefreshesStopsOnCanceledContext(t *testing.T) {
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"models":[{"slug":"gpt-5-6-pro","title":"GPT-5.6 Pro"}]}`)
	}))
	t.Cleanup(srv.Close)
	useChatGPTWebServiceTestURLs(t, srv.URL, srv.URL+"/backend-api", "")

	service := &Service{cfg: &config.Config{}, coreManager: coreauth.NewManager(nil, nil, nil)}
	service.chatGPTWebCatalogs = make(map[string]chatGPTWebCatalogEntry)
	auths := make([]*coreauth.Auth, 0, 2)
	for i := range 2 {
		auth := &coreauth.Auth{
			ID:       fmt.Sprintf("chatgpt-web-canceled-%d", i),
			Provider: "chatgpt-web",
			Status:   coreauth.StatusActive,
			Metadata: map[string]any{
				"cookie":     "__Secure-next-auth.session-token=session-token",
				"user_agent": chatgptweb.DefaultUserAgent,
			},
		}
		service.ensureExecutorsForAuth(auth)
		if _, err := service.coreManager.Register(t.Context(), auth); err != nil {
			t.Fatal(err)
		}
		service.chatGPTWebCatalogs[auth.ID] = chatGPTWebCatalogEntry{
			fingerprint: service.chatGPTWebAuthFingerprint(auth),
			fetchedAt:   time.Now().Add(-2 * chatGPTWebCatalogCacheTTL),
		}
		auths = append(auths, auth)
	}
	t.Cleanup(func() {
		for _, auth := range auths {
			registry.GetGlobalRegistry().UnregisterClient(auth.ID)
		}
	})

	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if refreshed := service.runBoundedChatGPTWebRefreshes(canceled, auths); refreshed != 0 {
		t.Fatalf("refreshed with canceled context = %d, want 0", refreshed)
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("canceled batch issued %d catalog requests, want 0", got)
	}
}

func modelIDs(models []*ModelInfo) []string {
	ids := make([]string, 0, len(models))
	for _, model := range models {
		if model != nil {
			ids = append(ids, model.ID)
		}
	}
	return ids
}

func TestChatGPTWebStaticModelClassification(t *testing.T) {
	models := registry.GetStaticModelDefinitionsByChannel(" chatGPT-WEB ")
	if len(models) != 1 || models[0].ID != "chatgpt-web/gpt-5-6-pro" {
		t.Fatalf("channel classification = %v, want chatgpt-web/gpt-5-6-pro", models)
	}
	if got := registry.GetStaticModelDefinitionsByChannel("chatgpt-web-extra"); got != nil {
		t.Fatalf("near-miss channel returned %v, want nil", got)
	}

	info := registry.LookupStaticModelInfo("chatgpt-web/gpt-5-6-pro")
	if info == nil || info.Type != "chatgpt-web" {
		t.Fatalf("LookupStaticModelInfo = %+v, want type chatgpt-web", info)
	}
	if got := registry.LookupStaticModelInfo("chatgpt-web/gpt-5-6-promax"); got != nil {
		t.Fatalf("near-miss model ID returned %+v, want nil", got)
	}
}
