package executor

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	chatgptweb "github.com/router-for-me/CLIProxyAPI/v7/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/thinking"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/util"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	cliproxyusage "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/usage"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const (
	chatGPTWebModelDefault   = "gpt-5-6-pro"
	chatGPTWebProviderKey    = "chatgpt-web"
	chatGPTWebClientPrefix   = "chatgpt-web"
	chatGPTWebWarmupInterval = 4 * time.Minute
	resumeMaxOffsets         = 3
)

var chatGPTWebHosts = map[string]bool{
	"chatgpt.com":     true,
	"www.chatgpt.com": true,
}

var pollInterval = 4 * time.Second // var so tests can shorten it

const (
	chatGPTWebCredentialPersistTimeout = 10 * time.Second
	chatGPTWebTransportCacheLimit      = 32
)

// chatGPTWebStreamKeepAlive is the transport-level SSE keep-alive interval
// requested for chatgpt-web streams. It covers long silent resume phases
// and is emitted by the HTTP writer as `: keep-alive` comments, bypassing the
// emitter/translator so it never becomes assistant content.
const chatGPTWebStreamKeepAlive = 5 * time.Second

// ChatGPTWebExecutor relays requests to ChatGPT's web backend
// (chatgpt.com/backend-api/f/conversation) using a pasted browser session
// cookie. It exists to reach web-only models such as gpt-5-6-pro that the
// Codex OAuth API does not expose.
type ChatGPTWebExecutor struct {
	cfg                      *config.Config
	sessions                 *chatgptweb.SessionManager
	updateAuthMetadata       func(context.Context, *cliproxyauth.Auth, map[string]any) (*cliproxyauth.Auth, error)
	credentialPersistTimeout time.Duration
	turns                    keyedMutex
	transportMu              sync.Mutex
	transports               map[string]http.RoundTripper
	transportOrder           []string
}

// EffectiveProxyURL reports the proxy this executor would use for the given
// auth, resolved from the same config snapshot DiscoverModels reads.
func (e *ChatGPTWebExecutor) EffectiveProxyURL(auth *cliproxyauth.Auth) string {
	return e.proxyURL(auth)
}

type chatGPTWebCachedTransport struct {
	transport http.RoundTripper
	mu        sync.Mutex
	active    int
	retired   bool
}

type chatGPTWebTrackedBody struct {
	io.ReadCloser
	once    sync.Once
	release func()
}

func (t *chatGPTWebCachedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.mu.Lock()
	t.active++
	t.mu.Unlock()
	resp, err := t.transport.RoundTrip(req)
	if err != nil || resp == nil || resp.Body == nil {
		t.release()
		return resp, err
	}
	resp.Body = &chatGPTWebTrackedBody{ReadCloser: resp.Body, release: t.release}
	return resp, nil
}

func (t *chatGPTWebCachedTransport) CloseIdleConnections() {
	t.mu.Lock()
	t.retired = true
	closeNow := t.active == 0
	t.mu.Unlock()
	if closeNow {
		t.closeIdleConnections()
	}
}

func (t *chatGPTWebCachedTransport) release() {
	t.mu.Lock()
	t.active--
	closeNow := t.retired && t.active == 0
	t.mu.Unlock()
	if closeNow {
		t.closeIdleConnections()
	}
}

func (t *chatGPTWebCachedTransport) closeIdleConnections() {
	if closer, ok := t.transport.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
}

func (b *chatGPTWebTrackedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err != nil {
		b.once.Do(b.release)
	}
	return n, err
}

func (b *chatGPTWebTrackedBody) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(b.release)
	return err
}

// keyedMutex serializes whole upstream turns per auth account. ChatGPT's web
// backend rejects a second conversation turn while one is in flight. Entries
// are reference-counted and removed once no turn holds or waits on them.
type keyedMutex struct {
	mu    sync.Mutex
	locks map[string]*keyedLock
}

type keyedLock struct {
	token chan struct{}
	refs  int
}

func (k *keyedMutex) lock(ctx context.Context, key string) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	k.mu.Lock()
	if k.locks == nil {
		k.locks = make(map[string]*keyedLock)
	}
	l, ok := k.locks[key]
	if !ok {
		l = &keyedLock{token: make(chan struct{}, 1)}
		l.token <- struct{}{}
		k.locks[key] = l
	}
	l.refs++
	k.mu.Unlock()
	select {
	case <-l.token:
		if err := ctx.Err(); err != nil {
			l.token <- struct{}{}
			k.mu.Lock()
			l.refs--
			if l.refs == 0 {
				delete(k.locks, key)
			}
			k.mu.Unlock()
			return nil, err
		}
		return func() {
			l.token <- struct{}{}
			k.mu.Lock()
			l.refs--
			if l.refs == 0 {
				delete(k.locks, key)
			}
			k.mu.Unlock()
		}, nil
	case <-ctx.Done():
		k.mu.Lock()
		l.refs--
		if l.refs == 0 {
			delete(k.locks, key)
		}
		k.mu.Unlock()
		return nil, ctx.Err()
	}
}

func NewChatGPTWebExecutor(cfg *config.Config, updateAuthMetadata ...func(context.Context, *cliproxyauth.Auth, map[string]any) (*cliproxyauth.Auth, error)) *ChatGPTWebExecutor {
	executor := &ChatGPTWebExecutor{cfg: cfg, sessions: chatgptweb.NewSessionManager(), credentialPersistTimeout: chatGPTWebCredentialPersistTimeout}
	if len(updateAuthMetadata) > 0 {
		executor.updateAuthMetadata = updateAuthMetadata[0]
	}
	return executor
}

// DiscoverModels returns the current model catalog for one ChatGPT web credential.
func (e *ChatGPTWebExecutor) DiscoverModels(ctx context.Context, auth *cliproxyauth.Auth) ([]*registry.ModelInfo, *cliproxyauth.Auth, error) {
	cookie := chatGPTWebCookie(auth)
	if cookie == "" {
		return nil, auth, fmt.Errorf("chatgpt-web executor: no cookie configured for model discovery")
	}
	userAgent, err := chatGPTWebUserAgent(auth)
	if err != nil {
		return nil, auth, err
	}
	effectiveAuth := auth
	credentialClient, requestClient := e.httpClients(auth)
	session, err := e.sessions.ResolveForIdentity(ctx, credentialClient, cookie, userAgent, e.proxyURL(auth))
	if err != nil {
		return nil, effectiveAuth, err
	}
	if effectiveAuth, err = e.persistSessionTokenRotation(ctx, effectiveAuth, cookie, session.Cookie); err != nil {
		return nil, effectiveAuth, err
	}
	models, setCookies, err := chatgptweb.FetchModels(ctx, requestClient, session)
	if len(setCookies) > 0 {
		previousCookie := session.Cookie
		session = e.sessions.RecordCookieRotation(session, setCookies, cookie)
		if effectiveAuth, err = e.persistSessionTokenRotation(ctx, effectiveAuth, previousCookie, session.Cookie); err != nil {
			return nil, effectiveAuth, err
		}
	}
	if err != nil {
		return nil, effectiveAuth, err
	}
	result := make([]*registry.ModelInfo, 0, len(models))
	for _, model := range models {
		if info := registry.LookupStaticModelInfo(chatGPTWebProviderKey + "/" + model.Slug); info != nil && info.Type == chatGPTWebProviderKey {
			info.ID = chatGPTWebProviderKey + "/" + model.Slug
			if model.Title != "" {
				info.DisplayName = model.Title + " (Web)"
			}
			result = append(result, info)
			continue
		}
		displayName := model.Title
		if displayName == "" {
			displayName = model.Slug
		}
		result = append(result, &registry.ModelInfo{
			ID:          chatGPTWebProviderKey + "/" + model.Slug,
			Object:      "model",
			OwnedBy:     "openai",
			Type:        chatGPTWebProviderKey,
			DisplayName: displayName + " (Web)",
			Description: displayName + " via ChatGPT web backend",
			Version:     model.Slug,
			Thinking: &registry.ThinkingSupport{
				Levels: []string{"low", "medium", "high"},
			},
		})
	}
	return result, effectiveAuth, nil
}

func (e *ChatGPTWebExecutor) proxyURL(auth *cliproxyauth.Auth) string {
	return helps.EffectiveProxyURL(e.cfg, auth)
}

func (e *ChatGPTWebExecutor) httpClients(auth *cliproxyauth.Auth) (*http.Client, *http.Client) {
	proxyURL := e.proxyURL(auth)
	e.transportMu.Lock()
	if e.transports == nil {
		e.transports = make(map[string]http.RoundTripper)
	}
	transport := e.transports[proxyURL]
	if transport == nil {
		if len(e.transports) >= chatGPTWebTransportCacheLimit {
			var evictKey string
			found := false
			for len(e.transportOrder) > 0 && !found {
				evictKey = e.transportOrder[0]
				e.transportOrder = e.transportOrder[1:]
				_, found = e.transports[evictKey]
			}
			if !found {
				for key := range e.transports {
					evictKey = key
					found = true
					break
				}
			}
			if found {
				if closer, ok := e.transports[evictKey].(interface{ CloseIdleConnections() }); ok {
					closer.CloseIdleConnections()
				}
				delete(e.transports, evictKey)
			}
		}
		transport = &chatGPTWebCachedTransport{transport: helps.NewUtlsHTTPRoundTripper(e.cfg, auth)}
		e.transports[proxyURL] = transport
		e.transportOrder = append(e.transportOrder, proxyURL)
	}
	e.transportMu.Unlock()
	return &http.Client{Transport: transport, Timeout: 60 * time.Second}, &http.Client{Transport: transport}
}

func (e *ChatGPTWebExecutor) Identifier() string { return chatGPTWebProviderKey }

func chatGPTWebCookie(auth *cliproxyauth.Auth) string {
	if auth == nil {
		return ""
	}
	if v, ok := auth.Metadata["cookie"].(string); ok && v != "" {
		if cookie := chatgptweb.MinimizeCookie(v); cookie != "" {
			return cookie
		}
	}
	if v, ok := auth.Metadata["session_token"].(string); ok && v != "" {
		return chatgptweb.MinimizeCookie(v)
	}
	return ""
}

func (e *ChatGPTWebExecutor) persistSessionTokenRotation(ctx context.Context, auth *cliproxyauth.Auth, previousCookie, currentCookie string) (*cliproxyauth.Auth, error) {
	if e == nil || e.updateAuthMetadata == nil || auth == nil || auth.ID == "" {
		return auth, nil
	}
	if currentCookie == previousCookie {
		return auth, nil
	}
	currentToken := chatgptweb.SessionToken(currentCookie)
	updates := make(map[string]any, 2)
	if currentToken == "" {
		updates["cookie"] = ""
		updates["session_token"] = ""
	} else {
		minimized := chatgptweb.MinimizeCookie(currentCookie)
		if minimized == "" {
			return auth, nil
		}
		updates["cookie"] = minimized
	}
	persistBase := ctx
	if persistBase == nil {
		persistBase = context.Background()
	}
	persistCtx, cancelPersist := context.WithTimeout(context.WithoutCancel(persistBase), e.credentialPersistTimeout)
	defer cancelPersist()
	updated, err := e.updateAuthMetadata(persistCtx, auth, updates)
	if err != nil {
		return auth, &chatGPTWebPersistenceError{cause: fmt.Errorf("chatgpt-web executor: persist rotated session cookie: %w", err)}
	}
	if updated == nil {
		return auth, nil
	}
	return updated, nil
}

func chatGPTWebRequirementsOptions(auth *cliproxyauth.Auth) chatgptweb.RequirementsOptions {
	var opts chatgptweb.RequirementsOptions
	if auth == nil {
		return opts
	}
	if v, ok := auth.Metadata["turnstile_token"].(string); ok {
		opts.TurnstileToken = v
	}
	if v, ok := auth.Metadata["submit_without_turnstile"].(bool); ok {
		opts.SubmitWithoutTurnstile = v
	}
	return opts
}

func chatGPTWebUserAgent(auth *cliproxyauth.Auth) (string, error) {
	if auth != nil {
		if raw, exists := auth.Metadata["user_agent"]; exists {
			value, ok := raw.(string)
			if !ok {
				return "", fmt.Errorf("chatgpt-web executor: stored User-Agent is invalid")
			}
			userAgent, err := chatgptweb.NormalizeUserAgent(value)
			if err != nil {
				return "", err
			}
			return userAgent, nil
		}
	}
	return chatgptweb.DefaultUserAgent, nil
}

func (e *ChatGPTWebExecutor) PrepareRequest(req *http.Request, auth *cliproxyauth.Auth) error {
	if req == nil {
		return nil
	}
	if err := validateChatGPTWebDestination(req); err != nil {
		return err
	}
	var attrs map[string]string
	if auth != nil {
		attrs = auth.Attributes
	}
	userAgent, err := chatGPTWebUserAgent(auth)
	if err != nil {
		return err
	}
	util.ApplyCustomHeadersFromAttrs(req, attrs)
	req.Host = ""
	req.Header.Del("Host")
	req.Header.Set("User-Agent", userAgent)
	if cookie := chatGPTWebCookie(auth); cookie != "" {
		req.Header.Set("Cookie", cookie)
	}
	return nil
}

func validateChatGPTWebDestination(req *http.Request) error {
	if req.URL == nil || req.URL.Scheme != "https" || !chatGPTWebHosts[strings.ToLower(req.URL.Hostname())] || req.URL.Port() != "" && req.URL.Port() != "443" {
		return fmt.Errorf("chatgpt-web executor: request destination is not an allowed ChatGPT host")
	}
	return nil
}

// HttpRequest executes a caller-supplied request with the session cookie
// attached. The destination must be an HTTPS ChatGPT host; attaching the
// cookie to an arbitrary URL would leak the browser session credential.
func (e *ChatGPTWebExecutor) HttpRequest(ctx context.Context, auth *cliproxyauth.Auth, req *http.Request) (*http.Response, error) {
	if req == nil {
		return nil, fmt.Errorf("chatgpt-web executor: request is nil")
	}
	if err := validateChatGPTWebDestination(req); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = req.Context()
	}
	httpReq := req.WithContext(ctx)
	// PrepareRequest sets credentials on the header map; clone it so the
	// caller's original request is never mutated.
	httpReq.Header = req.Header.Clone()
	if err := e.PrepareRequest(httpReq, auth); err != nil {
		return nil, err
	}
	_, client := e.httpClients(auth)
	client.CheckRedirect = func(redirect *http.Request, via []*http.Request) error {
		if len(via) >= 10 {
			return fmt.Errorf("chatgpt-web executor: stopped after 10 redirects")
		}
		return validateChatGPTWebDestination(redirect)
	}
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	setCookies := resp.Header.Values("Set-Cookie")
	sanitizeChatGPTWebRawResponseHeaders(resp.Header)
	if len(setCookies) == 0 {
		return resp, nil
	}
	previousCookie := chatGPTWebCookie(auth)
	currentCookie := chatgptweb.MergeRefreshedCookie(previousCookie, setCookies)
	if _, err = e.persistSessionTokenRotation(ctx, auth, previousCookie, currentCookie); err != nil {
		if errClose := resp.Body.Close(); errClose != nil {
			log.WithError(errClose).Warn("chatgpt-web executor: close response after cookie persistence failure")
		}
		return nil, err
	}
	return resp, nil
}

func (e *ChatGPTWebExecutor) Refresh(ctx context.Context, auth *cliproxyauth.Auth) (*cliproxyauth.Auth, error) {
	cookie := chatGPTWebCookie(auth)
	if cookie == "" {
		return auth, fmt.Errorf("chatgpt-web executor: no usable cookie configured for refresh")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	userAgent, err := chatGPTWebUserAgent(auth)
	if err != nil {
		return auth, err
	}
	unlock, err := e.turns.lock(ctx, turnKey(auth, nil))
	if err != nil {
		return auth, err
	}
	defer unlock()
	client, _ := e.httpClients(auth)
	sess, err := e.sessions.RefreshForIdentity(ctx, client, cookie, userAgent, e.proxyURL(auth))
	if err != nil {
		return auth, err
	}
	refreshedCookie := chatgptweb.MinimizeCookie(sess.Cookie)
	if refreshedCookie == "" {
		return auth, fmt.Errorf("chatgpt-web executor: refreshed session has no usable cookie")
	}
	refreshed := *auth
	metadata := make(map[string]any, len(auth.Metadata)+2)
	for k, v := range auth.Metadata {
		metadata[k] = v
	}
	metadata["last_refresh"] = time.Now().UnixMilli()
	if refreshedCookie != cookie {
		metadata["cookie"] = refreshedCookie
	}
	refreshed.Metadata = metadata
	return &refreshed, nil
}

type chatGPTWebSession struct {
	session *chatgptweb.Session
	reqs    *chatgptweb.SentinelRequirements
	auth    *cliproxyauth.Auth
}

func (e *ChatGPTWebExecutor) acquire(ctx context.Context, auth *cliproxyauth.Auth) (*chatGPTWebSession, *http.Client, error) {
	cookie := chatGPTWebCookie(auth)
	if cookie == "" {
		return nil, nil, fmt.Errorf("chatgpt-web executor: no cookie configured for this auth")
	}
	userAgent, err := chatGPTWebUserAgent(auth)
	if err != nil {
		return nil, nil, err
	}

	credentialClient, streamClient := e.httpClients(auth)
	sess, err := e.sessions.ResolveForIdentity(ctx, credentialClient, cookie, userAgent, e.proxyURL(auth))
	if err != nil {
		return nil, nil, err
	}
	effectiveAuth, err := e.persistSessionTokenRotation(ctx, auth, cookie, sess.Cookie)
	if err != nil {
		return nil, nil, err
	}
	// Browsers issue traffic like GET /backend-api/me before a conversation;
	// skipping straight to the Sentinel/conversation calls raises the risk
	// score on sessions where anti-abuse challenges are borderline.
	if sess.ShouldWarmup(chatGPTWebWarmupInterval) {
		if sc := warmupChatGPTWeb(ctx, streamClient, sess); len(sc) > 0 {
			previousCookie := sess.Cookie
			sess = e.sessions.RecordCookieRotation(sess, sc, cookie)
			effectiveAuth, err = e.persistSessionTokenRotation(ctx, effectiveAuth, previousCookie, sess.Cookie)
			if err != nil {
				return nil, nil, err
			}
		}
	}
	reqs, cookieUpdates, errRequirements := chatgptweb.FetchRequirements(ctx, streamClient, sess, chatGPTWebRequirementsOptions(effectiveAuth))
	if len(cookieUpdates) > 0 {
		previousCookie := sess.Cookie
		sess = e.sessions.RecordCookieRotation(sess, cookieUpdates, cookie)
		effectiveAuth, err = e.persistSessionTokenRotation(ctx, effectiveAuth, previousCookie, sess.Cookie)
		if err != nil {
			return nil, nil, err
		}
	}
	if errRequirements != nil {
		return nil, nil, errRequirements
	}
	return &chatGPTWebSession{session: sess, reqs: reqs, auth: effectiveAuth}, streamClient, nil
}

// Browsers issue traffic like GET /backend-api/me before a conversation;
// skipping straight to the Sentinel/conversation calls raises the risk
// score on sessions where anti-abuse challenges are borderline.
type warmupStep struct {
	path   string
	accept string
}

var chatGPTWebWarmupSteps = []warmupStep{
	{"/me", "application/json"},
	{"/conversations?offset=0&limit=28&order=updated", "application/json"},
	{"/models?history_and_training_disabled=true", "application/json"},
}

func warmupChatGPTWeb(ctx context.Context, client *http.Client, s *chatgptweb.Session) []string {
	baseURL, backendURL, _ := chatgptweb.CurrentBaseURLs()
	userAgent := chatgptweb.DefaultUserAgent
	if normalized, err := chatgptweb.NormalizeUserAgent(s.UserAgent); err == nil {
		userAgent = normalized
	}
	var setCookies []string
	cookie := s.Cookie
	for _, step := range chatGPTWebWarmupSteps {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, backendURL+step.path, nil)
		if err != nil {
			log.WithError(err).Debug("chatgpt-web: build warm-up request failed; continuing")
			continue
		}
		req.Header.Set("Accept", step.accept)
		req.Header.Set("User-Agent", userAgent)
		req.Header.Set("Origin", baseURL)
		req.Header.Set("Referer", baseURL+"/")
		req.Header.Set("Authorization", "Bearer "+s.AccessToken)
		req.Header.Set("Cookie", cookie)
		req.Header.Set("OAI-Device-Id", s.DeviceID)
		req.Header.Set("OAI-Language", "en-US")
		req.Header.Set("OAI-Client-Version", chatgptweb.ClientVersion())
		req.Header.Set("OAI-Client-Build-Number", chatgptweb.ClientBuildNumber())
		chatgptweb.ApplyBrowserHeaders(req, userAgent)
		if s.WebSessionID != "" {
			req.Header.Set("OAI-Session-Id", s.WebSessionID)
		}
		if s.AccountID != "" {
			req.Header.Set("chatgpt-account-id", s.AccountID)
		}
		resp, err := client.Do(req)
		if err != nil {
			log.WithError(err).Debug("chatgpt-web: warm-up request failed; continuing")
			continue
		}
		updates := resp.Header.Values("Set-Cookie")
		setCookies = append(setCookies, updates...)
		if len(updates) != 0 {
			cookie = chatgptweb.MergeRefreshedCookie(cookie, updates)
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			log.Debugf("chatgpt-web: warm-up %s returned status %d; continuing", step.path, resp.StatusCode)
		}
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64*1024))
		if err := resp.Body.Close(); err != nil {
			log.WithError(err).Debug("chatgpt-web: warm-up response close failed")
		}
		if chatgptweb.SessionToken(cookie) == "" {
			break
		}
	}
	return setCookies
}

func (e *ChatGPTWebExecutor) buildUpstreamBody(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (body []byte, model string, err error) {
	baseModel := thinking.ParseSuffix(req.Model).ModelName
	baseModel = strings.TrimPrefix(baseModel, chatGPTWebProviderKey+"/")
	from := opts.SourceFormat
	to := sdktranslator.FromString("codex")

	originalPayloadSource := req.Payload
	if len(opts.OriginalRequest) > 0 {
		originalPayloadSource = opts.OriginalRequest
	}
	responseFormat := gjson.GetBytes(originalPayloadSource, "response_format")
	if responseFormat.Exists() && responseFormat.Type != gjson.Null {
		formatType := responseFormat.Get("type")
		if !responseFormat.IsObject() || formatType.Type != gjson.String || formatType.String() != "text" {
			return nil, "", &chatGPTWebRequestError{cause: fmt.Errorf("chatgpt-web executor: structured output formats are not supported")}
		}
	}
	originalTranslated := sdktranslator.TranslateRequest(from, to, baseModel, bytes.Clone(originalPayloadSource), false)
	body = sdktranslator.TranslateRequest(from, to, baseModel, bytes.Clone(req.Payload), true)
	body, err = thinking.ApplyThinking(body, req.Model, from.String(), to.String(), e.Identifier())
	if err != nil {
		return nil, "", &chatGPTWebRequestError{cause: err}
	}
	requestedModel := helps.PayloadRequestedModel(opts, req.Model)
	requestPath := helps.PayloadRequestPath(opts)
	body = helps.ApplyPayloadConfigWithRoot(e.cfg, baseModel, to.String(), "", body, originalTranslated, requestedModel, requestPath)
	if gjson.GetBytes(body, "background").Bool() {
		return nil, "", &chatGPTWebRequestError{cause: fmt.Errorf("chatgpt-web executor: background responses are not supported")}
	}

	model = gjson.GetBytes(body, "model").String()
	model = strings.TrimPrefix(model, chatGPTWebProviderKey+"/")
	if model == "" {
		model = chatGPTWebModelDefault
	}
	body, err = sjson.SetBytes(body, "model", model)
	if err != nil {
		return nil, "", &chatGPTWebRequestError{cause: fmt.Errorf("chatgpt-web executor: set upstream model: %w", err)}
	}
	return body, model, nil
}

// inputToTurns flattens Codex Responses input into plain text turns. This
// provider is text-only, so any structured part or item it cannot represent
// is an explicit error rather than a silent omission.
func inputToTurns(body []byte) ([]helps.ChatGPTWebTurn, error) {
	var turns []helps.ChatGPTWebTurn
	textFormat := gjson.GetBytes(body, "text.format")
	if textFormat.Exists() && textFormat.Type != gjson.Null {
		formatType := textFormat.Get("type")
		if !textFormat.IsObject() || formatType.Type != gjson.String || formatType.String() != "text" {
			return nil, fmt.Errorf("chatgpt-web executor: structured output formats are not supported")
		}
	}
	tools := gjson.GetBytes(body, "tools")
	if tools.Exists() && tools.Type != gjson.Null {
		if !tools.IsArray() {
			return nil, fmt.Errorf("chatgpt-web executor: tools must be an array")
		}
		if len(tools.Array()) != 0 {
			return nil, fmt.Errorf("chatgpt-web executor: tools are not supported")
		}
	}
	toolChoice := gjson.GetBytes(body, "tool_choice")
	if toolChoice.Exists() && toolChoice.Type != gjson.Null {
		if toolChoice.Type != gjson.String || toolChoice.String() != "auto" && toolChoice.String() != "none" {
			return nil, fmt.Errorf("chatgpt-web executor: tool_choice is not supported")
		}
	}
	instructionValue := gjson.GetBytes(body, "instructions")
	if instructionValue.Exists() && instructionValue.Type != gjson.Null && instructionValue.Type != gjson.String {
		return nil, fmt.Errorf("chatgpt-web executor: instructions must be text")
	}
	instructions := instructionValue.String()
	if instructions != "" {
		turns = append(turns, helps.ChatGPTWebTurn{Role: "system", Content: instructions})
	}
	input := gjson.GetBytes(body, "input")
	if input.Type == gjson.String {
		if text := input.String(); strings.TrimSpace(text) != "" {
			turns = append(turns, helps.ChatGPTWebTurn{Role: "user", Content: text})
		}
		return turns, nil
	}
	if !input.IsArray() {
		return nil, fmt.Errorf("chatgpt-web executor: unsupported input representation (text only)")
	}
	var convErr error
	input.ForEach(func(_, item gjson.Result) bool {
		if convErr != nil {
			return false
		}
		itemType := item.Get("type").String()
		if itemType != "" && itemType != "message" {
			convErr = fmt.Errorf("chatgpt-web executor: unsupported input item type %q (text only)", itemType)
			return false
		}
		role := item.Get("role").String()
		if role == "" {
			convErr = fmt.Errorf("chatgpt-web executor: message role is required")
			return false
		}
		// Codex translators demote source system messages to developer;
		// recognize both before the user fallback buries them in the
		// transcript.
		if role == "developer" {
			role = "system"
		}
		switch role {
		case "system", "user", "assistant":
		default:
			convErr = fmt.Errorf("chatgpt-web executor: unsupported message role %q", role)
			return false
		}
		var text strings.Builder
		content := item.Get("content")
		if content.IsArray() {
			content.ForEach(func(_, part gjson.Result) bool {
				t := part.Get("type").String()
				if t == "input_text" || t == "output_text" || t == "text" {
					partText := part.Get("text")
					if !partText.Exists() || partText.Type != gjson.String {
						convErr = fmt.Errorf("chatgpt-web executor: content part text must be a string")
						return false
					}
					text.WriteString(partText.String())
					return true
				}
				convErr = fmt.Errorf("chatgpt-web executor: unsupported content part type %q (text only)", t)
				return false
			})
		} else if content.Type == gjson.String {
			text.WriteString(content.String())
		} else {
			convErr = fmt.Errorf("chatgpt-web executor: unsupported message content representation (text only)")
			return false
		}
		if convErr != nil {
			return false
		}
		if text.Len() > 0 {
			turns = append(turns, helps.ChatGPTWebTurn{Role: role, Content: text.String()})
		}
		return true
	})
	if convErr != nil {
		return nil, convErr
	}
	return turns, nil
}

func estimateChatGPTWebUsage(model string, conversationBody []byte, output string) (cliproxyusage.Detail, error) {
	enc, err := helps.TokenizerForModel(model)
	if err != nil {
		return cliproxyusage.Detail{}, fmt.Errorf("chatgpt-web executor: tokenizer init failed: %w", err)
	}
	var detail cliproxyusage.Detail
	messages := gjson.GetBytes(conversationBody, "messages")
	if !messages.IsArray() {
		return cliproxyusage.Detail{}, fmt.Errorf("chatgpt-web executor: conversation body omitted messages")
	}
	var errInput error
	messages.ForEach(func(_, message gjson.Result) bool {
		message.Get("content.parts").ForEach(func(_, part gjson.Result) bool {
			if part.Type != gjson.String {
				errInput = fmt.Errorf("chatgpt-web executor: conversation body contains non-text content")
				return false
			}
			count, errCount := enc.Count(part.String())
			if errCount != nil {
				errInput = fmt.Errorf("chatgpt-web executor: input token counting failed: %w", errCount)
				return false
			}
			detail.InputTokens += int64(count)
			return true
		})
		return errInput == nil
	})
	if errInput != nil {
		return cliproxyusage.Detail{}, errInput
	}
	outputCount, err := enc.Count(output)
	if err != nil {
		return cliproxyusage.Detail{}, fmt.Errorf("chatgpt-web executor: output token counting failed: %w", err)
	}
	detail.OutputTokens = int64(outputCount)
	detail.TotalTokens = detail.InputTokens + detail.OutputTokens
	return detail, nil
}

func publishEstimatedChatGPTWebUsage(ctx context.Context, reporter *helps.UsageReporter, model string, conversationBody []byte, output string) map[string]any {
	detail, err := estimateChatGPTWebUsage(model, conversationBody, output)
	if err != nil {
		log.WithError(err).Warn("chatgpt-web executor: estimate usage failed")
		reporter.EnsurePublished(ctx)
		return nil
	}
	reporter.Publish(ctx, detail)
	return map[string]any{
		"input_tokens":  detail.InputTokens,
		"output_tokens": detail.OutputTokens,
		"total_tokens":  detail.TotalTokens,
	}
}

func applyConversationHeaders(httpReq *http.Request, s *chatgptweb.Session, reqs *chatgptweb.SentinelRequirements, targetPath string, attrs map[string]string) {
	baseURL, _, _ := chatgptweb.CurrentBaseURLs()
	util.ApplyCustomHeadersFromAttrs(httpReq, attrs)
	userAgent := chatgptweb.DefaultUserAgent
	if normalized, err := chatgptweb.NormalizeUserAgent(s.UserAgent); err == nil {
		userAgent = normalized
	}
	httpReq.Host = ""
	httpReq.Header.Del("Host")
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "text/event-stream")
	httpReq.Header.Set("User-Agent", userAgent)
	httpReq.Header.Set("Origin", baseURL)
	httpReq.Header.Set("Referer", baseURL+"/")
	httpReq.Header.Set("Authorization", "Bearer "+s.AccessToken)
	httpReq.Header.Set("Cookie", s.Cookie)
	httpReq.Header.Set("OAI-Device-Id", s.DeviceID)
	httpReq.Header.Set("OAI-Language", "en-US")
	httpReq.Header.Set("OAI-Client-Version", chatgptweb.ClientVersion())
	httpReq.Header.Set("OAI-Client-Build-Number", chatgptweb.ClientBuildNumber())
	chatgptweb.ApplyBrowserHeaders(httpReq, userAgent)
	if s.WebSessionID != "" {
		httpReq.Header.Set("OAI-Session-Id", s.WebSessionID)
	} else {
		httpReq.Header.Set("OAI-Session-Id", uuid.NewString())
	}
	httpReq.Header.Set("x-oai-turn-trace-id", uuid.NewString())
	httpReq.Header.Set("X-OpenAI-Target-Path", targetPath)
	httpReq.Header.Set("X-OpenAI-Target-Route", targetPath)
	if s.AccountID != "" {
		httpReq.Header.Set("chatgpt-account-id", s.AccountID)
	}
	if reqs != nil {
		if reqs.Token != "" {
			httpReq.Header.Set("openai-sentinel-chat-requirements-token", reqs.Token)
		}
		if reqs.PrepareToken != "" {
			httpReq.Header.Set("openai-sentinel-chat-requirements-prepare-token", reqs.PrepareToken)
		}
		if reqs.ProofToken != "" {
			httpReq.Header.Set("openai-sentinel-proof-token", reqs.ProofToken)
		}
		if reqs.Turnstile != "" {
			httpReq.Header.Set("openai-sentinel-turnstile-token", reqs.Turnstile)
		}
	}
}

// codexStreamEmitter produces Codex Responses SSE lines from plain text
// deltas so existing codex→openai/openai-response/claude translators can be
// reused unchanged.
type codexStreamEmitter struct {
	model             string
	requestFields     map[string]any
	toolChoice        any
	parallelToolCalls any
	respID            string
	itemID            string
	created           int64
	started           bool
	seq               int
}

func newCodexStreamEmitter(model string, requestBody []byte) *codexStreamEmitter {
	requestFields := make(map[string]any)
	_ = json.Unmarshal(requestBody, &requestFields)
	toolChoice, ok := requestFields["tool_choice"]
	if !ok {
		toolChoice = "auto"
	}
	parallelToolCalls, ok := requestFields["parallel_tool_calls"]
	if !ok {
		parallelToolCalls = true
	}
	return &codexStreamEmitter{
		model:             model,
		requestFields:     requestFields,
		toolChoice:        toolChoice,
		parallelToolCalls: parallelToolCalls,
		respID:            chatGPTWebClientPrefix + "-" + uuid.NewString(),
		itemID:            "item-" + uuid.NewString(),
		created:           time.Now().Unix(),
	}
}

func (em *codexStreamEmitter) line(event string, payload map[string]any) []byte {
	payload["sequence_number"] = em.seq
	em.seq++
	if payload["type"] == nil {
		payload["type"] = event
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil
	}
	var b bytes.Buffer
	b.WriteString("event: ")
	b.WriteString(event)
	b.WriteString("\ndata: ")
	b.Write(raw)
	b.WriteString("\n\n")
	return b.Bytes()
}

func (em *codexStreamEmitter) start() [][]byte {
	if em.started {
		return nil
	}
	em.started = true
	response := em.response("in_progress", []any{}, nil)
	item := map[string]any{
		"id":      em.itemID,
		"type":    "message",
		"status":  "in_progress",
		"role":    "assistant",
		"content": []any{},
	}
	part := map[string]any{"type": "output_text", "text": "", "annotations": []any{}, "logprobs": []any{}}
	return [][]byte{
		em.line("response.created", map[string]any{"response": response}),
		em.line("response.in_progress", map[string]any{"response": response}),
		em.line("response.output_item.added", map[string]any{"output_index": 0, "item": item}),
		em.line("response.content_part.added", map[string]any{"output_index": 0, "item_id": em.itemID, "content_index": 0, "part": part}),
	}
}

func (em *codexStreamEmitter) delta(text string) []byte {
	return em.line("response.output_text.delta", map[string]any{"output_index": 0, "item_id": em.itemID, "content_index": 0, "delta": text, "logprobs": []any{}})
}

func (em *codexStreamEmitter) finish(fullText, finishType string, completedEvent bool, usage map[string]any) [][]byte {
	part := map[string]any{"type": "output_text", "text": fullText, "annotations": []any{}, "logprobs": []any{}}
	event := "response.completed"
	if finishType == "max_tokens" && !completedEvent {
		event = "response.incomplete"
	}
	item := em.terminalItem(part, event)
	response := em.terminalResponse(fullText, finishType, usage)
	return [][]byte{
		em.line("response.output_text.done", map[string]any{"output_index": 0, "item_id": em.itemID, "content_index": 0, "text": fullText, "logprobs": []any{}}),
		em.line("response.content_part.done", map[string]any{"output_index": 0, "item_id": em.itemID, "content_index": 0, "part": part}),
		em.line("response.output_item.done", map[string]any{"output_index": 0, "item": item}),
		em.line(event, map[string]any{"response": response}),
	}
}

func (em *codexStreamEmitter) terminalItem(part map[string]any, event string) map[string]any {
	// The output item mirrors the terminal response status so an
	// incomplete response never claims a completed item.
	status := "completed"
	if event == "response.incomplete" {
		status = "incomplete"
	}
	return map[string]any{
		"id":      em.itemID,
		"type":    "message",
		"status":  status,
		"role":    "assistant",
		"content": []any{part},
	}
}

func (em *codexStreamEmitter) terminalResponse(fullText, finishType string, usage map[string]any) map[string]any {
	part := map[string]any{"type": "output_text", "text": fullText, "annotations": []any{}, "logprobs": []any{}}
	status := "completed"
	stopReason := "stop"
	var incompleteDetails any
	event := "response.completed"
	if finishType == "max_tokens" {
		status = "incomplete"
		stopReason = "max_tokens"
		incompleteDetails = map[string]any{"reason": "max_output_tokens"}
		event = "response.incomplete"
	}
	response := em.response(status, []any{em.terminalItem(part, event)}, usage)
	response["incomplete_details"] = incompleteDetails
	response["stop_reason"] = stopReason
	return response
}

func (em *codexStreamEmitter) response(status string, output []any, usage any) map[string]any {
	response := map[string]any{
		"id":                  em.respID,
		"object":              "response",
		"created_at":          em.created,
		"model":               em.model,
		"status":              status,
		"background":          false,
		"error":               nil,
		"incomplete_details":  nil,
		"instructions":        nil,
		"metadata":            map[string]any{},
		"output":              output,
		"parallel_tool_calls": em.parallelToolCalls,
		"temperature":         nil,
		"tool_choice":         em.toolChoice,
		"tools":               []any{},
		"top_p":               nil,
		"usage":               usage,
	}
	for _, field := range []string{"instructions", "metadata", "temperature", "tools", "top_p"} {
		if value, ok := em.requestFields[field]; ok {
			response[field] = value
		}
	}
	return response
}

func chatGPTWebNeedsCompletedTerminal(format sdktranslator.Format, stream bool) bool {
	switch format {
	case sdktranslator.FormatOpenAI, sdktranslator.FormatGemini, sdktranslator.FormatGeminiCLI:
		return true
	case sdktranslator.FormatOpenAIResponse:
		return !stream
	default:
		return false
	}
}

func chatGPTWebMarkMaxTokens(payload []byte, format sdktranslator.Format) []byte {
	var err error
	switch format {
	case sdktranslator.FormatOpenAI:
		payload, err = sjson.SetBytes(payload, "choices.0.finish_reason", "length")
		if err == nil {
			payload, err = sjson.SetBytes(payload, "choices.0.native_finish_reason", "max_tokens")
		}
	case sdktranslator.FormatGemini, sdktranslator.FormatGeminiCLI:
		payload, err = sjson.SetBytes(payload, "candidates.0.finishReason", "MAX_TOKENS")
	}
	if err != nil {
		return payload
	}
	return payload
}

func (em *codexStreamEmitter) failed(message string) []byte {
	response := map[string]any{
		"id":     em.respID,
		"object": "response",
		"model":  em.model,
		"status": "failed",
		"error":  map[string]any{"message": message},
	}
	return em.line("response.failed", map[string]any{"response": response})
}

func openConversation(ctx context.Context, client *http.Client, s *chatgptweb.Session, reqs *chatgptweb.SentinelRequirements, body []byte, conduitToken string, attrs map[string]string) (*http.Response, error) {
	_, _, conversationURL := chatgptweb.CurrentBaseURLs()
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, conversationURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	applyConversationHeaders(httpReq, s, reqs, "/backend-api/f/conversation", attrs)
	if conduitToken != "" {
		httpReq.Header.Set("x-conduit-token", conduitToken)
	}
	var requestWritten atomic.Bool
	httpReq = httpReq.WithContext(httptrace.WithClientTrace(httpReq.Context(), &httptrace.ClientTrace{
		WroteRequest: func(httptrace.WroteRequestInfo) {
			requestWritten.Store(true)
		},
	}))
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, &chatGPTWebConversationTransportError{cause: err, requestWritten: requestWritten.Load()}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		if cerr := resp.Body.Close(); cerr != nil {
			log.WithError(cerr).Warn("chatgpt-web executor: close error body")
		}
		return resp, newChatGPTWebStatusError(resp.StatusCode, raw, resp.Header)
	}
	return resp, nil
}

func streamConversation(ctx context.Context, client *http.Client, s *chatgptweb.Session, reqs *chatgptweb.SentinelRequirements, body []byte, conduitToken string, attrs map[string]string, onDelta func(string)) (string, *helps.ChatGPTWebSSEParser, http.Header, error) {
	resp, err := openConversation(ctx, client, s, reqs, body, conduitToken, attrs)
	if err != nil {
		if resp == nil {
			return "", nil, nil, err
		}
		return "", nil, resp.Header.Clone(), err
	}
	header := resp.Header.Clone()
	fullText, parser, err := drainConversation(resp, onDelta)
	return fullText, parser, header, err
}

func recordChatGPTWebCookieRotation(current *chatgptweb.Session, setCookies []string, record func(*chatgptweb.Session, []string) (*chatgptweb.Session, error)) (*chatgptweb.Session, error) {
	if len(setCookies) == 0 || record == nil {
		return current, nil
	}
	var err error
	current, err = record(current, setCookies)
	if err != nil {
		return current, err
	}
	if current == nil || chatgptweb.SessionToken(current.Cookie) == "" {
		return current, &chatGPTWebSessionRevokedError{}
	}
	return current, nil
}

func wrapChatGPTWebResponseError(err error) error {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var transportErr *chatGPTWebConversationTransportError
	if errors.As(err, &transportErr) {
		if transportErr.requestWritten {
			return &chatGPTWebResponseError{cause: err}
		}
		return err
	}
	var statusErr cliproxyexecutor.StatusError
	if errors.As(err, &statusErr) {
		if statusErr.StatusCode() == http.StatusNotFound {
			return &chatGPTWebResponseError{cause: err}
		}
		return err
	}
	return &chatGPTWebResponseError{cause: err}
}

func drainConversation(resp *http.Response, onDelta func(string)) (string, *helps.ChatGPTWebSSEParser, error) {
	parser := &helps.ChatGPTWebSSEParser{}
	return drainConversationFrom(resp, resp.Body, parser, onDelta)
}

func drainConversationFrom(resp *http.Response, reader io.Reader, parser *helps.ChatGPTWebSSEParser, onDelta func(string)) (string, *helps.ChatGPTWebSSEParser, error) {
	err := helps.DrainChatGPTWebSSE(reader, parser, onDelta)
	if cerr := resp.Body.Close(); cerr != nil {
		log.WithError(cerr).Warn("chatgpt-web executor: close stream body")
	}
	if err != nil {
		return parser.Text(), parser, err
	}
	return parser.Text(), parser, nil
}

// resumeConversation continues a long-running turn after a stream_handoff.
func resumeConversation(ctx context.Context, client *http.Client, s *chatgptweb.Session, reqs *chatgptweb.SentinelRequirements, parser *helps.ChatGPTWebSSEParser, convID, resumeToken, already string, attrs map[string]string, onDelta func(string), recordCookies func(*chatgptweb.Session, []string) (*chatgptweb.Session, error)) (string, *helps.ChatGPTWebSSEParser, error) {
	var lastErr error
	continuation := parser
	for offset := 0; offset < resumeMaxOffsets; offset++ {
		var chained *helps.ChatGPTWebSSEParser
		text, resumed, chained, setCookies, errResume := resumeOffset(ctx, client, s, reqs, continuation, convID, resumeToken, offset, already, attrs, onDelta)
		var errRotation error
		s, errRotation = recordChatGPTWebCookieRotation(s, setCookies, recordCookies)
		if errRotation != nil {
			return already, continuation, &chatGPTWebContinuationError{cause: errRotation}
		}
		if errResume != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return already, continuation, ctxErr
			}
			var statusErr cliproxyexecutor.StatusError
			if !errors.As(errResume, &statusErr) || statusErr.StatusCode() != http.StatusNotFound {
				return already, continuation, &chatGPTWebContinuationError{cause: errResume}
			}
			lastErr = errResume
			log.WithError(errResume).WithField("offset", offset).Debug("chatgpt-web executor: resume offset failed")
			continue
		}
		if chained != nil {
			if tok := chained.ResumeToken(); tok != "" {
				resumeToken = tok
			}
			if id := chained.ConversationID(); id != "" {
				convID = id
			}
		}
		if resumed {
			if errFinish := chatGPTWebCompletionError(chained); errFinish != nil {
				return text, chained, &chatGPTWebContinuationError{cause: errFinish}
			}
			return text, chained, nil
		}
		continuation = chained
		if len(text) > len(already) && strings.HasPrefix(text, already) {
			already = text
		}
	}
	if lastErr != nil {
		return already, continuation, &chatGPTWebContinuationError{cause: lastErr}
	}
	return already, continuation, &chatGPTWebContinuationError{cause: fmt.Errorf("resume stream ended before completion")}
}

func chatGPTWebCompletionError(parser *helps.ChatGPTWebSSEParser) error {
	if parser == nil || !parser.Finished() {
		return fmt.Errorf("chatgpt-web executor: conversation stream ended before completion")
	}
	finishType := chatGPTWebFinishType(parser)
	if finishType != "" && finishType != "stop" && finishType != "max_tokens" {
		return fmt.Errorf("chatgpt-web executor: conversation response was incomplete (finish type %q)", finishType)
	}
	return nil
}

func chatGPTWebFinishType(parser *helps.ChatGPTWebSSEParser) string {
	if parser == nil {
		return ""
	}
	return strings.ToLower(strings.TrimSpace(parser.FinishType()))
}

func resumeOffset(ctx context.Context, client *http.Client, s *chatgptweb.Session, reqs *chatgptweb.SentinelRequirements, previous *helps.ChatGPTWebSSEParser, convID, resumeToken string, offset int, already string, attrs map[string]string, onDelta func(string)) (string, bool, *helps.ChatGPTWebSSEParser, []string, error) {
	resumeBody, errMarshal := json.Marshal(map[string]any{"conversation_id": convID, "offset": offset})
	if errMarshal != nil {
		return already, false, nil, nil, errMarshal
	}
	_, backendURL, _ := chatgptweb.CurrentBaseURLs()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, backendURL+"/f/conversation/resume", bytes.NewReader(resumeBody))
	if err != nil {
		return already, false, nil, nil, err
	}
	applyConversationHeaders(req, s, reqs, "/backend-api/f/conversation/resume", attrs)
	req.Header.Set("x-conduit-token", resumeToken)
	resp, err := client.Do(req)
	if err != nil {
		return already, false, nil, nil, err
	}
	setCookies := resp.Header.Values("Set-Cookie")
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		if cerr := resp.Body.Close(); cerr != nil {
			log.WithError(cerr).Warn("chatgpt-web executor: close resume body")
		}
		return already, false, nil, setCookies, newChatGPTWebStatusError(resp.StatusCode, raw, resp.Header)
	}
	parser := previous.Continuation()
	errDrain := helps.DrainChatGPTWebSSE(resp.Body, parser, nil)
	if cerr := resp.Body.Close(); cerr != nil {
		log.WithError(cerr).Warn("chatgpt-web executor: close resume stream")
	}
	if errDrain != nil {
		return already, false, parser, setCookies, errDrain
	}
	completed := parser.Finished() && !parser.Handoff()
	text := parser.Text()
	if text == already {
		return already, completed, parser, setCookies, nil
	}
	// Resume streams replay cumulative text; forward only the suffix past
	// what the original stream already produced. A mismatch means this
	// offset serves a different turn slice; it cannot certify completion,
	// so keep the safer baseline and fall through to the next offset.
	if !strings.HasPrefix(text, already) || len(text) <= len(already) {
		return already, false, previous, setCookies, nil
	}
	if onDelta != nil {
		onDelta(text[len(already):])
	}
	return text, completed, parser, setCookies, nil
}

func pollConversation(ctx context.Context, client *http.Client, s *chatgptweb.Session, convID, already string, attrs map[string]string, onDelta func(string)) (string, error) {
	consecutiveFailures := 0
	const maxConsecutiveFailures = 10
	for {
		select {
		case <-ctx.Done():
			return already, ctx.Err()
		case <-time.After(pollInterval):
		}
		_, backendURL, _ := chatgptweb.CurrentBaseURLs()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, backendURL+"/conversation/"+convID, nil)
		if err != nil {
			return already, err
		}
		applyConversationHeaders(req, s, nil, "/backend-api/conversation/"+convID, attrs)
		resp, err := client.Do(req)
		if err != nil {
			consecutiveFailures++
			if consecutiveFailures >= maxConsecutiveFailures {
				return already, fmt.Errorf("chatgpt-web executor: polling failed %d consecutive times: %w", consecutiveFailures, err)
			}
			continue
		}
		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusNotFound {
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
			if cerr := resp.Body.Close(); cerr != nil {
				log.WithError(cerr).Warn("chatgpt-web executor: close poll body")
			}
			return already, &statusError{code: resp.StatusCode}
		}
		raw, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
		if cerr := resp.Body.Close(); cerr != nil {
			log.WithError(cerr).Warn("chatgpt-web executor: close poll body")
		}
		if err != nil || resp.StatusCode != http.StatusOK {
			consecutiveFailures++
			if consecutiveFailures >= maxConsecutiveFailures {
				return already, fmt.Errorf("chatgpt-web executor: polling failed %d consecutive times (status %d)", consecutiveFailures, resp.StatusCode)
			}
			continue
		}
		consecutiveFailures = 0
		text, finished := extractFinishedAssistantText(raw)
		if finished && onDelta == nil {
			return text, nil
		}
		if len(text) > len(already) && strings.HasPrefix(text, already) {
			if onDelta != nil {
				onDelta(text[len(already):])
			}
			already = text
		}
		if finished {
			if text != already {
				return already, fmt.Errorf("chatgpt-web executor: polled response diverged from streamed text")
			}
			return already, nil
		}
	}
}

// extractFinishedAssistantText follows the conversation's current-node
// ancestry so the returned message is the actual final assistant turn, not
// an arbitrary map-iteration winner.
func extractFinishedAssistantText(raw []byte) (string, bool) {
	mapping := gjson.GetBytes(raw, "mapping")
	if !mapping.Exists() {
		return "", false
	}
	var textAt func(node gjson.Result) string
	textAt = func(node gjson.Result) string {
		msg := node.Get("message")
		if !msg.Exists() || msg.Get("author.role").String() != "assistant" {
			return ""
		}
		var text strings.Builder
		msg.Get("content.parts").ForEach(func(_, part gjson.Result) bool {
			if part.Type == gjson.String {
				text.WriteString(part.String())
			}
			return true
		})
		return text.String()
	}
	currentID := gjson.GetBytes(raw, "current_node").String()
	for depth := 0; currentID != "" && depth < 64; depth++ {
		node := mapping.Get(currentID)
		if !node.Exists() {
			return "", false
		}
		msg := node.Get("message")
		if msg.Exists() && msg.Get("author.role").String() == "assistant" {
			if msg.Get("status").String() != "finished_successfully" {
				// The turn's current assistant message is unfinished; a
				// finished ancestor belongs to an older turn.
				return "", false
			}
			// The current turn's finished assistant message is the answer,
			// even when empty; older finished assistants are prior turns.
			return textAt(node), true
		}
		currentID = node.Get("parent").String()
	}
	return "", false
}

func (e *ChatGPTWebExecutor) Execute(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (resp cliproxyexecutor.Response, err error) {
	baseModel := thinking.ParseSuffix(req.Model).ModelName
	reporter := helps.NewUsageReporter(ctx, e.Identifier(), baseModel, auth)
	defer reporter.TrackFailure(ctx, &err)
	responseModel := helps.PayloadRequestedModel(opts, req.Model)

	body, model, err := e.buildUpstreamBody(ctx, auth, req, opts)
	if err != nil {
		return resp, err
	}
	effort, err := helps.NormalizeChatGPTWebThinkingEffort(gjson.GetBytes(body, "reasoning.effort").String())
	if err != nil {
		return resp, &chatGPTWebRequestError{cause: err}
	}
	turns, err := inputToTurns(body)
	if err != nil {
		return resp, &chatGPTWebRequestError{cause: err}
	}
	convBody, err := helps.BuildChatGPTWebConversationBody(model, turns, effort)
	if err != nil {
		return resp, &chatGPTWebRequestError{cause: err}
	}
	authKey := turnKey(auth, nil)
	unlockAuth, err := e.turns.lock(ctx, authKey)
	if err != nil {
		return resp, err
	}
	defer unlockAuth()
	cs, client, err := e.acquire(ctx, auth)
	if err != nil {
		e.invalidateOnAuthError(auth, err)
		return resp, err
	}
	accountKey := turnKey(auth, cs.session)
	if accountKey != authKey {
		unlockAccount, errLock := e.turns.lock(ctx, accountKey)
		if errLock != nil {
			return resp, errLock
		}
		defer unlockAccount()
	}
	configuredCookie := chatGPTWebCookie(auth)
	recordCookies := func(current *chatgptweb.Session, setCookies []string) (*chatgptweb.Session, error) {
		previousCookie := current.Cookie
		current = e.sessions.RecordCookieRotation(current, setCookies, configuredCookie)
		updatedAuth, errPersist := e.persistSessionTokenRotation(ctx, cs.auth, previousCookie, current.Cookie)
		if errPersist == nil {
			cs.auth = updatedAuth
		}
		return current, errPersist
	}

	fullText, parser, headers, errStream := streamConversation(ctx, client, cs.session, cs.reqs, convBody, "", authAttrs(auth), nil)
	if headers == nil {
		headers = make(http.Header)
	}
	cs.session, err = recordChatGPTWebCookieRotation(cs.session, headers.Values("Set-Cookie"), recordCookies)
	headers.Del("Set-Cookie")
	sanitizeChatGPTWebResponseHeaders(headers)
	if err != nil {
		e.invalidateOnAuthError(auth, err)
		return resp, err
	}
	if errStream != nil {
		errStream = wrapChatGPTWebResponseError(errStream)
		e.invalidateOnAuthError(auth, errStream)
		return resp, errStream
	}
	if parser.Handoff() {
		if parser.ConversationID() == "" || parser.ResumeToken() == "" {
			return resp, wrapChatGPTWebResponseError(fmt.Errorf("chatgpt-web executor: stream handoff omitted continuation metadata"))
		}
		fullText, parser, err = resumeConversation(ctx, client, cs.session, cs.reqs, parser, parser.ConversationID(), parser.ResumeToken(), fullText, authAttrs(auth), nil, recordCookies)
		if err != nil {
			e.invalidateOnAuthError(auth, err)
			return resp, err
		}
	} else if err = chatGPTWebCompletionError(parser); err != nil {
		return resp, wrapChatGPTWebResponseError(err)
	}
	finishType := chatGPTWebFinishType(parser)
	from := opts.SourceFormat
	event := "response.completed"
	if finishType == "max_tokens" && !chatGPTWebNeedsCompletedTerminal(from, false) {
		event = "response.incomplete"
	}
	emitter := newCodexStreamEmitter(responseModel, body)
	usage := publishEstimatedChatGPTWebUsage(ctx, reporter, model, convBody, fullText)
	completed, err := json.Marshal(map[string]any{
		"type":     event,
		"response": emitter.terminalResponse(fullText, finishType, usage),
	})
	if err != nil {
		return resp, err
	}

	to := sdktranslator.FromString("codex")
	var param any
	out := sdktranslator.TranslateNonStream(ctx, to, from, responseModel, opts.OriginalRequest, convBody, completed, &param)
	if finishType == "max_tokens" {
		out = chatGPTWebMarkMaxTokens(out, from)
	}
	resp = cliproxyexecutor.Response{Payload: out, Headers: headers}
	return resp, nil
}

func (e *ChatGPTWebExecutor) ExecuteStream(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (_ *cliproxyexecutor.StreamResult, err error) {
	baseModel := thinking.ParseSuffix(req.Model).ModelName
	reporter := helps.NewUsageReporter(ctx, e.Identifier(), baseModel, auth)
	defer reporter.TrackFailure(ctx, &err)
	responseModel := helps.PayloadRequestedModel(opts, req.Model)

	body, model, err := e.buildUpstreamBody(ctx, auth, req, opts)
	if err != nil {
		return nil, err
	}
	effort, err := helps.NormalizeChatGPTWebThinkingEffort(gjson.GetBytes(body, "reasoning.effort").String())
	if err != nil {
		return nil, &chatGPTWebRequestError{cause: err}
	}
	turns, err := inputToTurns(body)
	if err != nil {
		return nil, &chatGPTWebRequestError{cause: err}
	}
	convBody, err := helps.BuildChatGPTWebConversationBody(model, turns, effort)
	if err != nil {
		return nil, &chatGPTWebRequestError{cause: err}
	}
	authKey := turnKey(auth, nil)
	unlockAuth, err := e.turns.lock(ctx, authKey)
	if err != nil {
		return nil, err
	}
	cs, client, err := e.acquire(ctx, auth)
	if err != nil {
		unlockAuth()
		e.invalidateOnAuthError(auth, err)
		return nil, err
	}
	accountKey := turnKey(auth, cs.session)
	unlockAccount := func() {}
	if accountKey != authKey {
		unlockAccount, err = e.turns.lock(ctx, accountKey)
		if err != nil {
			unlockAuth()
			return nil, err
		}
	}
	unlock := func() {
		unlockAccount()
		unlockAuth()
	}
	handedOff := false
	defer func() {
		if !handedOff {
			unlock()
		}
	}()

	// The conversation must open before any synthetic chunk is emitted:
	// upstream headers are part of the result, and an initial 401/403 has to
	// surface as an error so auth rotation can retry instead of the client
	// receiving a started-but-failed stream.
	configuredCookie := chatGPTWebCookie(auth)
	recordCookies := func(current *chatgptweb.Session, setCookies []string) (*chatgptweb.Session, error) {
		previousCookie := current.Cookie
		current = e.sessions.RecordCookieRotation(current, setCookies, configuredCookie)
		updatedAuth, errPersist := e.persistSessionTokenRotation(ctx, cs.auth, previousCookie, current.Cookie)
		if errPersist == nil {
			cs.auth = updatedAuth
		}
		return current, errPersist
	}
	upstream, err := openConversation(ctx, client, cs.session, cs.reqs, convBody, "", authAttrs(auth))
	if err != nil {
		upstreamErr := err
		if upstream != nil {
			var errRotation error
			cs.session, errRotation = recordChatGPTWebCookieRotation(cs.session, upstream.Header.Values("Set-Cookie"), recordCookies)
			upstream.Header.Del("Set-Cookie")
			if errRotation != nil {
				err = errRotation
			} else {
				err = upstreamErr
			}
		}
		err = wrapChatGPTWebResponseError(err)
		e.invalidateOnAuthError(auth, err)
		reporter.PublishFailure(ctx, err)
		var retryable interface{ Retryable() bool }
		if upstream == nil && errors.As(err, &retryable) && !retryable.Retryable() {
			return chatGPTWebCommittedStreamError(nil, err), nil
		}
		return nil, err
	}
	headers := upstream.Header.Clone()
	cs.session, err = recordChatGPTWebCookieRotation(cs.session, headers.Values("Set-Cookie"), recordCookies)
	headers.Del("Set-Cookie")
	sanitizeChatGPTWebResponseHeaders(headers)
	if err != nil {
		if errClose := upstream.Body.Close(); errClose != nil {
			log.WithError(errClose).Warn("chatgpt-web executor: close revoked conversation body")
		}
		e.invalidateOnAuthError(auth, err)
		reporter.PublishFailure(ctx, err)
		return chatGPTWebCommittedStreamError(headers, err), nil
	}
	from := opts.SourceFormat
	to := sdktranslator.FromString("codex")

	handedOff = true
	out := make(chan cliproxyexecutor.StreamChunk)
	go func() {
		defer unlock()
		defer close(out)
		emitter := newCodexStreamEmitter(responseModel, body)
		terminalFinishType := ""
		var param any
		emit := func(raw []byte) bool {
			if len(raw) == 0 {
				return true
			}
			chunks := sdktranslator.TranslateStream(ctx, to, from, responseModel, opts.OriginalRequest, convBody, bytes.Clone(raw), &param)
			terminal := dataPayload(raw)
			markMaxTokens := terminalFinishType == "max_tokens" && gjson.GetBytes(terminal, "type").String() == "response.completed"
			for i := range chunks {
				if markMaxTokens {
					chunks[i] = chatGPTWebMarkMaxTokens(chunks[i], from)
				}
				select {
				case out <- cliproxyexecutor.StreamChunk{Payload: chunks[i]}:
				case <-ctx.Done():
					return false
				}
			}
			return true
		}
		emitDataLine := func(raw []byte) bool {
			var b bytes.Buffer
			b.WriteString("data: ")
			b.Write(raw)
			b.WriteString("\n\n")
			return emit(b.Bytes())
		}
		emitEmitterEvent := func(raw []byte) bool {
			if from == sdktranslator.FormatOpenAIResponse {
				return emit(raw)
			}
			return emitDataLine(dataPayload(raw))
		}
		emitErr := func(streamErr error) {
			streamErr = wrapChatGPTWebResponseError(streamErr)
			reporter.PublishFailure(ctx, streamErr)
			select {
			case out <- cliproxyexecutor.StreamChunk{Err: streamErr}:
			case <-ctx.Done():
			}
		}
		publishEmissionFailure := func() {
			emitErrContext := ctx.Err()
			if emitErrContext == nil {
				emitErrContext = errors.New("chatgpt-web executor: stream completion emission stopped")
			}
			reporter.PublishFailure(ctx, emitErrContext)
		}

		emissionStopped := false
		ensureStarted := func() bool {
			if emitter.started {
				return true
			}
			for _, chunk := range emitter.start() {
				if !emitEmitterEvent(chunk) {
					emissionStopped = true
					return false
				}
			}
			return true
		}
		onDelta := func(d string) {
			if emissionStopped || !ensureStarted() {
				return
			}
			if !emitEmitterEvent(emitter.delta(d)) {
				emissionStopped = true
			}
		}
		fullText, parser, streamErr := drainConversation(upstream, onDelta)
		if emissionStopped {
			publishEmissionFailure()
			return
		}
		if streamErr != nil {
			emitErr(streamErr)
			return
		}
		if parser.Diverged() {
			emitErr(fmt.Errorf("chatgpt-web executor: streamed response was revised after emission"))
			return
		}
		if parser.Handoff() {
			if parser.ConversationID() == "" || parser.ResumeToken() == "" {
				emitErr(fmt.Errorf("chatgpt-web executor: stream handoff omitted continuation metadata"))
				return
			}
			// resumeConversation tracks its own cumulative
			// baseline and only emit the incremental suffix via onDelta, so
			// emitter and translator state stay single-threaded on this goroutine.
			// Long silent resume phases are kept alive by the transport-level
			// SSE keep-alive emitted by the HTTP writer,
			// which bypasses the emitter/translator entirely.
			fullText, parser, streamErr = resumeConversation(ctx, client, cs.session, cs.reqs, parser, parser.ConversationID(), parser.ResumeToken(), fullText, authAttrs(auth), onDelta, recordCookies)
			if streamErr != nil {
				e.invalidateOnAuthError(auth, streamErr)
				emitErr(streamErr)
				return
			}
		} else if streamErr = chatGPTWebCompletionError(parser); streamErr != nil {
			emitErr(streamErr)
			return
		}
		if !ensureStarted() {
			publishEmissionFailure()
			return
		}
		terminalFinishType = chatGPTWebFinishType(parser)
		usage := publishEstimatedChatGPTWebUsage(ctx, reporter, model, convBody, fullText)
		for _, chunk := range emitter.finish(fullText, terminalFinishType, chatGPTWebNeedsCompletedTerminal(from, true), usage) {
			if !emitEmitterEvent(chunk) {
				publishEmissionFailure()
				return
			}
		}
	}()
	result := &cliproxyexecutor.StreamResult{Headers: headers, Chunks: out}
	result.SetKeepAliveInterval(chatGPTWebStreamKeepAlive)
	result.SetBootstrapCommitted()
	return result, nil
}

func chatGPTWebCommittedStreamError(headers http.Header, err error) *cliproxyexecutor.StreamResult {
	out := make(chan cliproxyexecutor.StreamChunk, 1)
	out <- cliproxyexecutor.StreamChunk{Err: err}
	close(out)
	result := &cliproxyexecutor.StreamResult{Headers: headers, Chunks: out}
	result.SetBootstrapCommitted()
	return result
}

func sanitizeChatGPTWebRawResponseHeaders(headers http.Header) {
	for _, value := range headers.Values("Connection") {
		for _, token := range strings.Split(value, ",") {
			if token = strings.TrimSpace(token); token != "" {
				headers.Del(token)
			}
		}
	}
	for _, key := range []string{
		"Connection",
		"Keep-Alive",
		"Proxy-Authenticate",
		"Proxy-Authorization",
		"Set-Cookie",
		"TE",
		"Trailer",
		"Transfer-Encoding",
		"Upgrade",
	} {
		headers.Del(key)
	}
}

func sanitizeChatGPTWebResponseHeaders(headers http.Header) {
	sanitizeChatGPTWebRawResponseHeaders(headers)
	for _, key := range []string{"Content-Encoding", "Content-Length", "Content-Type"} {
		headers.Del(key)
	}
}

func authAttrs(auth *cliproxyauth.Auth) map[string]string {
	if auth == nil {
		return nil
	}
	return auth.Attributes
}

func turnKey(auth *cliproxyauth.Auth, session *chatgptweb.Session) string {
	identity := ""
	if session != nil {
		switch {
		case session.AccountID != "":
			identity = "account:" + session.AccountID
		case session.UserID != "":
			identity = "user:" + session.UserID
		case session.DeviceID != "":
			identity = "device:" + session.DeviceID
		}
	}
	if identity == "" && auth != nil && auth.ID != "" {
		identity = "auth:" + auth.ID
	}
	if identity == "" && auth != nil {
		identity = fmt.Sprintf("auth-pointer:%p", auth)
	}
	if identity == "" {
		identity = "cookie:" + chatgptweb.DeviceIDForCookie(chatGPTWebCookie(auth))
	}
	sum := sha256.Sum256([]byte("chatgpt-web-turn:" + identity))
	return hex.EncodeToString(sum[:8])
}

func dataPayload(line []byte) []byte {
	idx := bytes.Index(line, []byte("data: "))
	if idx < 0 {
		return bytes.TrimSpace(line)
	}
	payload := line[idx+len("data: "):]
	return bytes.TrimRight(payload, "\n")
}

func (e *ChatGPTWebExecutor) CountTokens(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	from := opts.SourceFormat
	body, model, err := e.buildUpstreamBody(ctx, auth, req, opts)
	if err != nil {
		return cliproxyexecutor.Response{}, err
	}
	turns, err := inputToTurns(body)
	if err != nil {
		return cliproxyexecutor.Response{}, &chatGPTWebRequestError{cause: err}
	}
	var total int64
	if len(turns) != 0 {
		effort, errEffort := helps.NormalizeChatGPTWebThinkingEffort(gjson.GetBytes(body, "reasoning.effort").String())
		if errEffort != nil {
			return cliproxyexecutor.Response{}, &chatGPTWebRequestError{cause: errEffort}
		}
		conversationBody, errBody := helps.BuildChatGPTWebConversationBody(model, turns, effort)
		if errBody != nil {
			return cliproxyexecutor.Response{}, &chatGPTWebRequestError{cause: errBody}
		}
		detail, errUsage := estimateChatGPTWebUsage(model, conversationBody, "")
		if errUsage != nil {
			return cliproxyexecutor.Response{}, errUsage
		}
		total = detail.InputTokens
	}
	usage := helps.BuildOpenAIUsageJSON(total)
	out := sdktranslator.TranslateTokenCount(ctx, sdktranslator.FromString("openai"), from, total, usage)
	return cliproxyexecutor.Response{Payload: out}, nil
}

func (e *ChatGPTWebExecutor) invalidateOnAuthError(auth *cliproxyauth.Auth, err error) {
	if errors.Is(err, chatgptweb.ErrTurnstileRequired) {
		return
	}
	var continuationErr *chatGPTWebContinuationError
	if errors.As(err, &continuationErr) {
		err = continuationErr.cause
	}
	var se cliproxyexecutor.StatusError
	if errors.As(err, &se) && (se.StatusCode() == http.StatusUnauthorized || se.StatusCode() == http.StatusForbidden) {
		e.sessions.Invalidate(chatGPTWebCookie(auth))
	}
}

type statusError struct {
	code           int
	classification string
	headers        http.Header
}

func (e *statusError) Error() string {
	if e.classification != "" {
		return fmt.Sprintf("chatgpt-web executor: upstream status %d (%s)", e.code, e.classification)
	}
	return fmt.Sprintf("chatgpt-web executor: upstream status %d", e.code)
}
func (e *statusError) StatusCode() int { return e.code }
func (e *statusError) Headers() http.Header {
	if e == nil || e.headers == nil {
		return nil
	}
	return e.headers.Clone()
}

func newChatGPTWebStatusError(code int, body []byte, headers http.Header) *statusError {
	errType := safeChatGPTWebErrorLabel(gjson.GetBytes(body, "error.type").String())
	errCode := safeChatGPTWebErrorLabel(gjson.GetBytes(body, "error.code").String())
	if errType == "" && code == http.StatusBadRequest {
		errType = "invalid_request_error"
	}
	classification := errType
	if errCode != "" && errCode != errType {
		if classification != "" {
			classification += ":"
		}
		classification += errCode
	}
	headers = headers.Clone()
	headers.Del("Set-Cookie")
	sanitizeChatGPTWebResponseHeaders(headers)
	return &statusError{code: code, classification: classification, headers: headers}
}

func safeChatGPTWebErrorLabel(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" || len(value) > 64 {
		return ""
	}
	for _, ch := range value {
		if ch == '_' || ch == '-' || ch == '.' || ch >= 'a' && ch <= 'z' || ch >= '0' && ch <= '9' {
			continue
		}
		return ""
	}
	return value
}

type chatGPTWebContinuationError struct {
	cause error
}

type chatGPTWebResponseError struct {
	cause error
}

type chatGPTWebRequestError struct {
	cause error
}

type chatGPTWebPersistenceError struct {
	cause error
}

type chatGPTWebConversationTransportError struct {
	cause          error
	requestWritten bool
}

type chatGPTWebSessionRevokedError struct{}

func (e *chatGPTWebRequestError) Error() string   { return e.cause.Error() }
func (e *chatGPTWebRequestError) Unwrap() error   { return e.cause }
func (e *chatGPTWebRequestError) StatusCode() int { return http.StatusBadRequest }
func (e *chatGPTWebRequestError) AuthStateNeutral() bool {
	return true
}
func (e *chatGPTWebRequestError) Retryable() bool { return false }

func (e *chatGPTWebPersistenceError) Error() string   { return e.cause.Error() }
func (e *chatGPTWebPersistenceError) Unwrap() error   { return e.cause }
func (e *chatGPTWebPersistenceError) StatusCode() int { return http.StatusInternalServerError }
func (e *chatGPTWebPersistenceError) AuthStateNeutral() bool {
	return true
}
func (e *chatGPTWebPersistenceError) Retryable() bool { return false }

func (e *chatGPTWebConversationTransportError) Error() string { return e.cause.Error() }
func (e *chatGPTWebConversationTransportError) Unwrap() error { return e.cause }
func (e *chatGPTWebConversationTransportError) AuthStateNeutral() bool {
	return true
}

func (*chatGPTWebSessionRevokedError) Error() string {
	return "chatgpt-web executor: rotated session cookie removed the session token"
}
func (*chatGPTWebSessionRevokedError) StatusCode() int        { return http.StatusUnauthorized }
func (*chatGPTWebSessionRevokedError) AuthStateNeutral() bool { return true }
func (*chatGPTWebSessionRevokedError) Retryable() bool        { return false }

func (e *chatGPTWebResponseError) Error() string {
	return fmt.Sprintf("chatgpt-web executor: upstream response failed: %v", e.cause)
}

func (e *chatGPTWebResponseError) Unwrap() error { return e.cause }
func (e *chatGPTWebResponseError) StatusCode() int {
	return http.StatusBadGateway
}
func (e *chatGPTWebResponseError) AuthStateNeutral() bool { return true }
func (e *chatGPTWebResponseError) Retryable() bool        { return false }

func (e *chatGPTWebContinuationError) Error() string {
	return fmt.Sprintf("chatgpt-web executor: response continuation failed: %v", e.cause)
}

func (e *chatGPTWebContinuationError) Unwrap() error { return e.cause }
func (e *chatGPTWebContinuationError) AuthStateNeutral() bool {
	return true
}
func (e *chatGPTWebContinuationError) Retryable() bool { return false }
func (e *chatGPTWebContinuationError) StatusCode() int {
	var statusErr cliproxyexecutor.StatusError
	if errors.As(e.cause, &statusErr) {
		if code := statusErr.StatusCode(); code >= 100 && code <= 599 {
			if code == http.StatusNotFound {
				return http.StatusBadGateway
			}
			return code
		}
	}
	return http.StatusBadGateway
}
