// Package chatgptweb implements a session-based client for ChatGPT's web backend
// (https://chatgpt.com/backend-api), used to reach models that are not exposed
// through the Codex OAuth API (e.g. gpt-5-6-pro).
//
// Authentication is a pasted browser session cookie; the session JWT is then
// exchanged for a short-lived access token, and each conversation turn is
// gated by OpenAI's Sentinel proof-of-work requirements.
package chatgptweb

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	BaseURL         = "https://chatgpt.com"
	BackendBaseURL  = BaseURL + "/backend-api"
	ConversationURL = BackendBaseURL + "/f/conversation"

	DefaultUserAgent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"

	clientVersion      = "prod-e1d6f2820dd20c3bab36cc42e8668035bf87f7bc"
	clientBuildNumber  = "9052945"
	sessionTokenMaxAge = 4 * time.Minute
	modelsResponseMax  = 4 * 1024 * 1024
)

type endpointURLs struct {
	Base         string
	Backend      string
	Conversation string
}

var endpointURLValue atomic.Value

func init() {
	endpointURLValue.Store(endpointURLs{Base: BaseURL, Backend: BackendBaseURL, Conversation: ConversationURL})
}

func currentBaseURLs() endpointURLs {
	return endpointURLValue.Load().(endpointURLs)
}

func CurrentBaseURLs() (base, backend, conversation string) {
	urls := currentBaseURLs()
	return urls.Base, urls.Backend, urls.Conversation
}

func SetBaseURLsForTesting(base, backend, conversation string) func() {
	previous := currentBaseURLs()
	endpointURLValue.Store(endpointURLs{Base: base, Backend: backend, Conversation: conversation})
	var once sync.Once
	return func() {
		once.Do(func() { endpointURLValue.Store(previous) })
	}
}

// ClientVersion returns the mimicked ChatGPT web client version.
func ClientVersion() string { return clientVersion }

// ClientBuildNumber returns the mimicked ChatGPT web client build number.
func ClientBuildNumber() string { return clientBuildNumber }

type Session struct {
	Cookie      string
	AccessToken string
	AccountID   string
	UserID      string
	DeviceID    string
	Email       string
	UserAgent   string

	// WebSessionID is the oai-session-id the browser keeps stable for one
	// sitting; minting a fresh UUID per request is an automation tell.
	WebSessionID string

	expiresAt     time.Time
	warmupMu      sync.Mutex
	warmedAt      time.Time
	cacheIdentity string
}

// Model describes one model exposed to the authenticated ChatGPT web account.
type Model struct {
	Slug  string
	Title string
}

func newWebSessionID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("chatgpt-web: generate web session ID: %w", err)
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}

type upstreamStatusError struct {
	operation string
	code      int
}

func (e *upstreamStatusError) Error() string {
	return fmt.Sprintf("chatgpt-web: %s returned status %d", e.operation, e.code)
}

func (e *upstreamStatusError) StatusCode() int {
	return e.code
}

func (s *Session) Expired() bool {
	return s == nil || s.AccessToken == "" || time.Now().After(s.expiresAt)
}

func (s *Session) ShouldWarmup(interval time.Duration) bool {
	if s == nil {
		return false
	}
	s.warmupMu.Lock()
	defer s.warmupMu.Unlock()
	now := time.Now()
	if !s.warmedAt.IsZero() && now.Sub(s.warmedAt) < interval {
		return false
	}
	s.warmedAt = now
	return true
}

// FetchModels returns the current model catalog for an authenticated web session.
func FetchModels(ctx context.Context, client *http.Client, session *Session) ([]Model, []string, error) {
	if client == nil || session == nil || strings.TrimSpace(session.AccessToken) == "" {
		return nil, nil, fmt.Errorf("chatgpt-web: model catalog requires an authenticated session")
	}
	urls := currentBaseURLs()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, urls.Backend+"/models?history_and_training_disabled=false", nil)
	if err != nil {
		return nil, nil, fmt.Errorf("chatgpt-web: build model catalog request: %w", err)
	}
	userAgent := userAgentForSession(session)
	request.Header.Set("Accept", "application/json")
	request.Header.Set("User-Agent", userAgent)
	request.Header.Set("Origin", urls.Base)
	request.Header.Set("Referer", urls.Base+"/")
	request.Header.Set("Authorization", "Bearer "+session.AccessToken)
	request.Header.Set("Cookie", session.Cookie)
	request.Header.Set("OAI-Device-Id", session.DeviceID)
	request.Header.Set("OAI-Language", "en-US")
	request.Header.Set("OAI-Client-Version", ClientVersion())
	request.Header.Set("OAI-Client-Build-Number", ClientBuildNumber())
	ApplyBrowserHeaders(request, userAgent)
	if session.WebSessionID != "" {
		request.Header.Set("OAI-Session-Id", session.WebSessionID)
	}
	if session.AccountID != "" {
		request.Header.Set("chatgpt-account-id", session.AccountID)
	}

	response, err := client.Do(request)
	if err != nil {
		return nil, nil, fmt.Errorf("chatgpt-web: fetch model catalog: %w", err)
	}
	defer func() {
		if errClose := response.Body.Close(); errClose != nil {
			log.WithError(errClose).Debug("chatgpt-web: close model catalog response")
		}
	}()
	setCookies := response.Header.Values("Set-Cookie")
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 64*1024))
		return nil, setCookies, &upstreamStatusError{operation: "model catalog", code: response.StatusCode}
	}

	data, err := io.ReadAll(io.LimitReader(response.Body, modelsResponseMax+1))
	if err != nil {
		return nil, setCookies, fmt.Errorf("chatgpt-web: read model catalog: %w", err)
	}
	if len(data) > modelsResponseMax {
		return nil, setCookies, fmt.Errorf("chatgpt-web: model catalog exceeds %d bytes", modelsResponseMax)
	}
	var catalog struct {
		Models []struct {
			Slug  string `json:"slug"`
			Title string `json:"title"`
		} `json:"models"`
	}
	if err := json.Unmarshal(data, &catalog); err != nil {
		return nil, setCookies, fmt.Errorf("chatgpt-web: parse model catalog: %w", err)
	}
	models := make([]Model, 0, len(catalog.Models))
	seen := make(map[string]struct{}, len(catalog.Models))
	for _, entry := range catalog.Models {
		slug := strings.TrimSpace(entry.Slug)
		if slug == "" {
			continue
		}
		key := strings.ToLower(slug)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		models = append(models, Model{Slug: slug, Title: strings.TrimSpace(entry.Title)})
	}
	if len(models) == 0 {
		return nil, setCookies, fmt.Errorf("chatgpt-web: model catalog returned no models")
	}
	return models, setCookies, nil
}

func NormalizeUserAgent(raw string) (string, error) {
	userAgent := strings.TrimSpace(raw)
	if len(userAgent) >= len("User-Agent:") && strings.EqualFold(userAgent[:len("User-Agent:")], "User-Agent:") {
		userAgent = strings.TrimSpace(userAgent[len("User-Agent:"):])
	}
	if userAgent == "" {
		return "", fmt.Errorf("chatgpt-web: browser User-Agent is required")
	}
	if len(userAgent) > 512 || !strings.HasPrefix(userAgent, "Mozilla/5.0") || !strings.Contains(userAgent, "AppleWebKit/537.36") {
		return "", fmt.Errorf("chatgpt-web: invalid browser User-Agent")
	}
	for _, product := range strings.Fields(userAgent) {
		if strings.HasPrefix(product, "Firefox/") || strings.HasPrefix(product, "FxiOS/") || strings.HasPrefix(product, "CriOS/") || strings.HasPrefix(product, "Version/") {
			return "", fmt.Errorf("chatgpt-web: invalid browser User-Agent")
		}
	}
	chromium := false
	recognizedProductVersion := func(product, prefix string) bool {
		version, ok := strings.CutPrefix(product, prefix)
		if !ok {
			return true
		}
		if version == "" {
			return false
		}
		for _, part := range strings.Split(version, ".") {
			if part == "" {
				return false
			}
			for _, ch := range part {
				if ch < '0' || ch > '9' {
					return false
				}
			}
		}
		return true
	}
	for _, product := range strings.Fields(userAgent) {
		if !recognizedProductVersion(product, "Chrome/") ||
			!recognizedProductVersion(product, "Chromium/") ||
			!recognizedProductVersion(product, "Edg/") ||
			!recognizedProductVersion(product, "EdgA/") ||
			!recognizedProductVersion(product, "EdgiOS/") ||
			!recognizedProductVersion(product, "OPR/") {
			return "", fmt.Errorf("chatgpt-web: invalid browser User-Agent")
		}
		if _, ok := strings.CutPrefix(product, "Chrome/"); ok {
			chromium = true
		}
	}
	if !chromium {
		return "", fmt.Errorf("chatgpt-web: invalid browser User-Agent")
	}
	for _, r := range userAgent {
		if r < 0x20 || r == 0x7f {
			return "", fmt.Errorf("chatgpt-web: invalid browser User-Agent")
		}
	}
	return userAgent, nil
}

func userAgentOrDefault(userAgent string) string {
	if normalized, err := NormalizeUserAgent(userAgent); err == nil {
		return normalized
	}
	return DefaultUserAgent
}

func validateUserAgentOrDefault(userAgent string) (string, error) {
	if strings.TrimSpace(userAgent) == "" {
		return DefaultUserAgent, nil
	}
	return NormalizeUserAgent(userAgent)
}

func userAgentForSession(s *Session) string {
	if s == nil {
		return DefaultUserAgent
	}
	return userAgentOrDefault(s.UserAgent)
}

type SessionManager struct {
	mu               sync.Mutex
	byID             map[string]*Session
	deviceByID       map[string]string
	aliasesBySession map[*Session]map[string]struct{}
	exchanges        map[string]*sessionExchange
}

type sessionExchange struct {
	done      chan struct{}
	cancel    context.CancelFunc
	waiters   int
	completed bool
	session   *Session
	err       error
}

// Aliases accumulate as session tokens rotate: every exchange rekeys the
// session under its refreshed cookie while older keys stay as aliases.
// liveAliasCap bounds those aliases; expiredSessionAliasCap keeps expired
// entries only as a small re-exchange window before eviction.
const (
	liveAliasCap           = 64
	expiredSessionAliasCap = 16
)

func NewSessionManager() *SessionManager {
	return &SessionManager{
		byID:             make(map[string]*Session),
		deviceByID:       make(map[string]string),
		aliasesBySession: make(map[*Session]map[string]struct{}),
		exchanges:        make(map[string]*sessionExchange),
	}
}

func (m *SessionManager) setAliasLocked(key string, session *Session) {
	if previous := m.byID[key]; previous != nil && previous != session {
		if aliases := m.aliasesBySession[previous]; aliases != nil {
			delete(aliases, key)
			if len(aliases) == 0 {
				delete(m.aliasesBySession, previous)
			}
		}
	}
	m.byID[key] = session
	m.deviceByID[key] = session.DeviceID
	aliases := m.aliasesBySession[session]
	if aliases == nil {
		aliases = make(map[string]struct{})
		m.aliasesBySession[session] = aliases
	}
	aliases[key] = struct{}{}
}

func (m *SessionManager) deleteAliasLocked(key string) {
	if session := m.byID[key]; session != nil {
		if aliases := m.aliasesBySession[session]; aliases != nil {
			delete(aliases, key)
			if len(aliases) == 0 {
				delete(m.aliasesBySession, session)
			}
		}
	}
	delete(m.byID, key)
	delete(m.deviceByID, key)
}

func (m *SessionManager) replaceSessionAliasesLocked(current, replacement *Session) {
	aliases := m.aliasesBySession[current]
	if len(aliases) == 0 {
		return
	}
	delete(m.aliasesBySession, current)
	m.aliasesBySession[replacement] = aliases
	for key := range aliases {
		m.byID[key] = replacement
		m.deviceByID[key] = replacement.DeviceID
	}
}

func (m *SessionManager) deleteSessionAliasesLocked(session *Session) {
	for key := range m.aliasesBySession[session] {
		delete(m.byID, key)
		delete(m.deviceByID, key)
	}
	delete(m.aliasesBySession, session)
}

func cookieKey(cookie string) string {
	return sessionCacheKey(cookie, DefaultUserAgent, "")

}

func sessionCacheKey(cookie, userAgent, networkIdentity string) string {
	sum := sha256.Sum256([]byte(cookie + "\x00" + userAgentOrDefault(userAgent) + "\x00" + strings.TrimSpace(networkIdentity)))
	return hex.EncodeToString(sum[:16])
}

const sessionCookieName = "__Secure-next-auth.session-token"

// isSessionTokenName matches the unchunked session cookie or a numbered
// chunk like `...session-token.0`. A dotted suffix that is not a
// non-negative integer (e.g. `.metadata`) is an unrelated cookie.
func isSessionTokenName(name string) bool {
	if name == sessionCookieName {
		return true
	}
	return isSessionTokenChunk(name)
}

func isSessionTokenChunk(name string) bool {
	suffix, ok := strings.CutPrefix(name, sessionCookieName+".")
	if !ok || suffix == "" {
		return false
	}
	// NextAuth chunks are plain non-negative integers without leading
	// zeros, signs, or padding; anything else is a different cookie.
	for i, ch := range suffix {
		if ch < '0' || ch > '9' {
			return false
		}
		if i == 0 && ch == '0' && len(suffix) > 1 {
			return false
		}
	}
	_, err := strconv.Atoi(suffix)
	return err == nil
}

// setCookieDeletes reports whether a raw Set-Cookie header expires the
// cookie: a negative or explicitly present zero Max-Age, or an Expires date
// in the past. Go's http.Cookie uses MaxAge 0 both for "absent" and
// "Max-Age=0", so the raw header distinguishes them.
func setCookieDeletes(sc string) bool {
	if hasMaxAgeZero(sc) {
		return true
	}
	c, err := http.ParseSetCookie(sc)
	if err != nil {
		return false
	}
	if c.MaxAge < 0 {
		return true
	}
	if c.MaxAge > 0 {
		return false
	}
	return !c.Expires.IsZero() && c.Expires.Before(time.Now())
}

func hasMaxAgeZero(raw string) bool {
	for _, attr := range strings.Split(raw, ";")[1:] {
		kv := strings.SplitN(strings.TrimSpace(attr), "=", 2)
		if len(kv) == 2 && strings.EqualFold(kv[0], "Max-Age") && strings.TrimSpace(kv[1]) == "0" {
			return true
		}
	}
	return false
}

// ParseCookieInput accepts a full "Cookie: ..." header line, a bare
// __Secure-next-auth.session-token value, or an already-normalized cookie
// string, and returns a normalized Cookie header value. An explicitly
// labeled header is preserved verbatim; the bare-token heuristic (token
// values containing "=", like JWTs with base64 padding) applies only to
// unlabeled input.
func ParseCookieInput(raw string) string {
	s := strings.TrimSpace(raw)
	if strings.HasPrefix(strings.ToLower(s), "cookie:") {
		return strings.TrimSpace(s[len("cookie:"):])
	}
	if !strings.Contains(s, "=") {
		return sessionCookieName + "=" + s
	}
	if !strings.Contains(s, ";") {
		name, value, _ := strings.Cut(s, "=")
		name = strings.TrimSpace(name)
		if name != sessionCookieName && !strings.HasPrefix(name, sessionCookieName+".") && name != "cf_clearance" && name != "oai-did" {
			if strings.Trim(value, "=") == "" {
				return sessionCookieName + "=" + s
			}
			return s
		}
	}
	return s
}

// MinimizeCookie reduces a pasted Cookie header to the credentials the
// session exchange needs: the session token (unchunked or a contiguous
// numbered chunk set), the Cloudflare clearance/rotation cookies, and the
// browser device ID. Everything else is dropped so a pasted full browser
// header is not persisted wholesale.
// The result is empty when no usable session token is present.
func MinimizeCookie(cookie string) string {
	cookie = ParseCookieInput(cookie)
	unchunked := ""
	unchunkedSet := false
	chunks := map[int]string{}
	cf := ""
	cfBM := ""
	cfuvid := ""
	deviceID := ""
	for _, part := range strings.Split(cookie, ";") {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) != 2 {
			continue
		}
		name, value := kv[0], kv[1]
		switch {
		case name == sessionCookieName:
			if !unchunkedSet {
				unchunked = value
				unchunkedSet = true
			}
		case name == "cf_clearance" && cf == "":
			cf = value
		case name == "__cf_bm" && cfBM == "":
			cfBM = value
		case name == "_cfuvid" && cfuvid == "":
			cfuvid = value
		case name == "oai-did" && deviceID == "" && validDeviceID(value):
			deviceID = value
		default:
			if suffix, ok := strings.CutPrefix(name, sessionCookieName+"."); ok && isSessionTokenChunk(name) {
				if idx, err := strconv.Atoi(suffix); err == nil {
					if _, exists := chunks[idx]; !exists {
						chunks[idx] = value
					}
				}
			}
		}
	}
	var b strings.Builder
	if unchunkedSet {
		if unchunked == "" {
			return ""
		}
		b.WriteString(sessionCookieName + "=" + unchunked)
	} else if len(chunks) > 0 {
		for i := 0; i < len(chunks); i++ {
			v, ok := chunks[i]
			if !ok || v == "" {
				return ""
			}
			if i > 0 {
				b.WriteString("; ")
			}
			b.WriteString(sessionCookieName + "." + strconv.Itoa(i) + "=" + v)
		}
	} else {
		return ""
	}
	if cf != "" {
		b.WriteString("; cf_clearance=" + cf)
	}
	if cfBM != "" {
		b.WriteString("; __cf_bm=" + cfBM)
	}
	if cfuvid != "" {
		b.WriteString("; _cfuvid=" + cfuvid)
	}
	if deviceID == "" {
		deviceID = DeviceIDForCookie(b.String())
	}
	if deviceID != "" {
		b.WriteString("; oai-did=" + deviceID)
	}
	return b.String()
}

func SessionToken(cookie string) string {
	unchunked := ""
	unchunkedSet := false
	chunks := map[int]string{}
	for _, part := range strings.Split(cookie, ";") {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) != 2 {
			continue
		}
		if kv[0] == sessionCookieName {
			// Keep the first occurrence, even when empty, to match
			// MinimizeCookie's semantics; a later duplicate would classify an
			// unusable first cookie as valid.
			if !unchunkedSet {
				unchunked = kv[1]
				unchunkedSet = true
			}
			continue
		}
		if suffix, ok := strings.CutPrefix(kv[0], sessionCookieName+"."); ok && isSessionTokenChunk(kv[0]) {
			if idx, err := strconv.Atoi(suffix); err == nil {
				if _, exists := chunks[idx]; !exists {
					chunks[idx] = kv[1]
				}
			}
		}
	}
	if unchunkedSet {
		return unchunked
	}
	if len(chunks) == 0 {
		return ""
	}
	idxs := make([]int, 0, len(chunks))
	for idx := range chunks {
		idxs = append(idxs, idx)
	}
	sort.Ints(idxs)
	// A gap means part of the token is missing; concatenating anyway would
	// send a corrupted token and surface it as an opaque auth failure.
	for i, idx := range idxs {
		if idx != i {
			return ""
		}
	}
	var sb strings.Builder
	for _, idx := range idxs {
		if chunks[idx] == "" {
			return ""
		}
		sb.WriteString(chunks[idx])
	}
	return sb.String()
}

// sessionCookiePairs returns all name=value pairs carrying the session token,
// unchunked or chunked, in header order.
func sessionCookiePairs(cookie string) []string {
	var pairs []string
	for _, part := range strings.Split(cookie, ";") {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) != 2 {
			continue
		}
		if kv[0] == sessionCookieName || isSessionTokenChunk(kv[0]) {
			pairs = append(pairs, kv[0]+"="+kv[1])
		}
	}
	return pairs
}

// MergeRefreshedCookie folds Set-Cookie values back into the stored blob while
// preserving unrelated cookies such as cf_clearance, which Cloudflare pins to
// the session.
func MergeRefreshedCookie(stored string, setCookies []string) string {
	jar := map[string]string{}
	var order []string
	add := func(pair string) {
		kv := strings.SplitN(strings.TrimSpace(pair), "=", 2)
		if len(kv) != 2 || kv[0] == "" {
			return
		}
		if _, ok := jar[kv[0]]; !ok {
			order = append(order, kv[0])
		}
		jar[kv[0]] = kv[1]
	}
	remove := func(name string) {
		delete(jar, name)
		kept := order[:0]
		for _, k := range order {
			if k != name {
				kept = append(kept, k)
			}
		}
		order = kept
	}
	dropSessionToken := func() {
		for k := range jar {
			if isSessionTokenName(k) {
				delete(jar, k)
			}
		}
		kept := order[:0]
		for _, k := range order {
			if !isSessionTokenName(k) {
				kept = append(kept, k)
			}
		}
		order = kept
	}
	for _, part := range strings.Split(stored, ";") {
		add(part)
	}
	// Sequential responses flattened into one slice may change the token
	// form mid-batch. The last non-deletion token entry wins, so skip stale
	// entries of the other form before they are added back after the drop.
	lastSetToken := ""
	for _, sc := range setCookies {
		first := strings.SplitN(sc, ";", 2)[0]
		kv := strings.SplitN(strings.TrimSpace(first), "=", 2)
		if len(kv) != 2 || !isSessionTokenName(kv[0]) || setCookieDeletes(sc) {
			continue
		}
		lastSetToken = kv[0]
	}
	lastSetChunked := isSessionTokenChunk(lastSetToken)
	sessionTokenDropped := false
	for _, sc := range setCookies {
		first := strings.SplitN(sc, ";", 2)[0]
		kv := strings.SplitN(strings.TrimSpace(first), "=", 2)
		if len(kv) != 2 {
			continue
		}
		if isSessionTokenName(kv[0]) && !setCookieDeletes(sc) && isSessionTokenChunk(kv[0]) != lastSetChunked {
			continue
		}
		// Standard deletion forms: negative or zero Max-Age, or an Expires
		// date in the past. Deletions apply in header order, so a trailing
		// deletion still erases a token set earlier in this response.
		if setCookieDeletes(sc) {
			if isSessionTokenName(kv[0]) {
				if !sessionTokenDropped {
					dropSessionToken()
					sessionTokenDropped = true
				}
				remove(kv[0])
			} else {
				remove(kv[0])
			}
			continue
		}
		// A refresh ships either the unchunked token or a full chunk set; any
		// refreshed session entry invalidates every stale form exactly once.
		if isSessionTokenName(kv[0]) {
			if !sessionTokenDropped {
				dropSessionToken()
				sessionTokenDropped = true
			}
		}
		add(first)
	}
	parts := make([]string, 0, len(order))
	for _, k := range order {
		if v, ok := jar[k]; ok {
			parts = append(parts, k+"="+v)
		}
	}
	return strings.Join(parts, "; ")
}

func DeviceIDForCookie(cookie string) string {
	if deviceID := browserDeviceID(cookie); deviceID != "" {
		return deviceID
	}
	// Derive the device ID from the session-token material only; auxiliary
	// cookies like cf_clearance rotate. Token rotation still changes it, so
	// only Resolve's device-hint carryover keeps the ID stable across
	// refreshes.
	tok := SessionToken(cookie)
	if tok == "" {
		tok = strings.Join(sessionCookiePairs(cookie), ";")
	}
	if tok == "" {
		tok = cookie
	}
	sum := sha256.Sum256([]byte("chatgpt-web-device:" + tok))
	h := hex.EncodeToString(sum[:16])
	return fmt.Sprintf("%s-%s-%s-%s-%s", h[0:8], h[8:12], h[12:16], h[16:20], h[20:32])
}

func browserDeviceID(cookie string) string {
	for _, part := range strings.Split(cookie, ";") {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) == 2 && kv[0] == "oai-did" && validDeviceID(kv[1]) {
			return kv[1]
		}
	}
	return ""
}

func validDeviceID(value string) bool {
	if len(value) < 8 || len(value) > 128 {
		return false
	}
	for _, r := range value {
		if r == '-' || r == '_' || r == '.' || r >= '0' && r <= '9' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' {
			continue
		}
		return false
	}
	return true
}

type sessionAPIResponse struct {
	AccessToken string `json:"accessToken"`
	User        struct {
		ID    string `json:"id"`
		Email string `json:"email"`
	} `json:"user"`
	Account struct {
		ID string `json:"id"`
	} `json:"account"`
	Accounts map[string]struct {
		Account struct {
			ID string `json:"id"`
		} `json:"account"`
	} `json:"accounts"`
}

func (m *SessionManager) Resolve(ctx context.Context, client *http.Client, cookie, userAgent string) (*Session, error) {
	return m.ResolveForIdentity(ctx, client, cookie, userAgent, "")
}

func (m *SessionManager) ResolveForIdentity(ctx context.Context, client *http.Client, cookie, userAgent, networkIdentity string) (*Session, error) {
	var err error
	userAgent, err = validateUserAgentOrDefault(userAgent)
	if err != nil {
		return nil, err
	}
	networkIdentity = strings.TrimSpace(networkIdentity)
	key := sessionCacheKey(cookie, userAgent, networkIdentity)
	m.mu.Lock()
	priorDeviceID := m.deviceByID[key]
	if s, ok := m.byID[key]; ok {
		if priorDeviceID == "" {
			priorDeviceID = s.DeviceID
		}
		if !s.Expired() {
			m.mu.Unlock()
			return s, nil
		}
		// The entry's cookie carries any token rotation from the last
		// exchange; re-exchanging the original cookie would use a stale token.
		cookie = s.Cookie
	}
	m.mu.Unlock()

	flightKey := sessionCacheKey(cookie, userAgent, networkIdentity)
	exchange := m.acquireSessionExchange(ctx, client, key, flightKey, cookie, priorDeviceID, userAgent, networkIdentity)
	defer m.releaseSessionExchange(flightKey, exchange)
	select {
	case <-exchange.done:
		err = exchange.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if err != nil {
		return nil, err
	}
	s := exchange.session
	if s == nil {
		return nil, fmt.Errorf("chatgpt-web: session exchange returned no session")
	}
	// A waiter whose key differs from the closure's key still holds the
	// same entry; alias its own key so a later resolve is a cache hit.
	// If the entry was already rotated or re-resolved to a newer session by
	// a concurrent caller, keep that newer session instead of rebinding the
	// key to this possibly-stale one.
	m.mu.Lock()
	if existing := m.byID[key]; existing == nil {
		m.setAliasLocked(key, s)
	} else if existing != s {
		s = existing
	}
	m.mu.Unlock()
	return s, nil
}

func (m *SessionManager) acquireSessionExchange(ctx context.Context, client *http.Client, key, flightKey, cookie, priorDeviceID, userAgent, networkIdentity string) *sessionExchange {
	m.mu.Lock()
	if exchange := m.exchanges[flightKey]; exchange != nil {
		exchange.waiters++
		m.mu.Unlock()
		return exchange
	}
	exchangeCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	exchange := &sessionExchange{done: make(chan struct{}), cancel: cancel, waiters: 1}
	m.exchanges[flightKey] = exchange
	m.mu.Unlock()

	go func() {
		s, err := exchangeSessionWithDeviceHint(exchangeCtx, client, cookie, priorDeviceID, userAgent)
		if err != nil {
			m.finishSessionExchange(flightKey, exchange, nil, err)
			return
		}
		s.cacheIdentity = networkIdentity
		m.mu.Lock()
		if m.exchanges[flightKey] != exchange || exchange.waiters == 0 || exchange.completed {
			m.mu.Unlock()
			m.finishSessionExchange(flightKey, exchange, nil, context.Canceled)
			return
		}
		// A re-exchange refreshes every alias already pointing at the
		// exchanged cookie, so no key keeps resolving the superseded
		// session and re-exchanging an already-rotated token.
		for k, old := range m.byID {
			if old != nil && old != s && old.Cookie == cookie && old.UserAgent == userAgent && old.cacheIdentity == networkIdentity {
				m.setAliasLocked(k, s)
			}
		}
		m.setAliasLocked(key, s)
		// The refreshed cookie rekeys future resolves; alias it to this
		// entry so the original and rotated cookie hit the same session.
		if newKey := sessionCacheKey(s.Cookie, userAgent, networkIdentity); newKey != key {
			m.setAliasLocked(newKey, s)
		}
		if minimized := MinimizeCookie(s.Cookie); minimized != "" {
			minimizedKey := sessionCacheKey(minimized, userAgent, networkIdentity)
			m.setAliasLocked(minimizedKey, s)
		}
		m.pruneLocked(key)
		m.finishSessionExchangeLocked(flightKey, exchange, s, nil)
		m.mu.Unlock()
	}()
	return exchange
}

func (m *SessionManager) finishSessionExchange(flightKey string, exchange *sessionExchange, session *Session, err error) {
	m.mu.Lock()
	m.finishSessionExchangeLocked(flightKey, exchange, session, err)
	m.mu.Unlock()
}

func (m *SessionManager) finishSessionExchangeLocked(flightKey string, exchange *sessionExchange, session *Session, err error) {
	if m.exchanges[flightKey] == exchange {
		delete(m.exchanges, flightKey)
	}
	exchange.session = session
	exchange.err = err
	exchange.completed = true
	exchange.cancel()
	close(exchange.done)
}

func (m *SessionManager) releaseSessionExchange(flightKey string, exchange *sessionExchange) {
	m.mu.Lock()
	exchange.waiters--
	if exchange.waiters == 0 && !exchange.completed {
		if m.exchanges[flightKey] == exchange {
			delete(m.exchanges, flightKey)
		}
		exchange.cancel()
	}
	m.mu.Unlock()
}

func (m *SessionManager) Refresh(ctx context.Context, client *http.Client, cookie, userAgent string) (*Session, error) {
	return m.RefreshForIdentity(ctx, client, cookie, userAgent, "")
}

func (m *SessionManager) RefreshForIdentity(ctx context.Context, client *http.Client, cookie, userAgent, networkIdentity string) (*Session, error) {
	var err error
	userAgent, err = validateUserAgentOrDefault(userAgent)
	if err != nil {
		return nil, err
	}
	networkIdentity = strings.TrimSpace(networkIdentity)
	m.mu.Lock()
	if s := m.byID[sessionCacheKey(cookie, userAgent, networkIdentity)]; s != nil {
		s.expiresAt = time.Time{}
	}
	m.mu.Unlock()
	return m.ResolveForIdentity(ctx, client, cookie, userAgent, networkIdentity)
}

// pruneLocked bounds alias growth from token rotation. Each session
// keeps at most liveAliasCap aliases, always retaining its current
// cookie key so fresh resolves stay cache hits; older expired sessions
// survive only as a small re-exchange window. A dropped historical
// alias costs one re-exchange at worst, and only for cookies whose
// token was already rotated upstream.
func (m *SessionManager) pruneLocked(protected string) {
	bySession := make(map[*Session][]string)
	for k, s := range m.byID {
		if s == nil {
			m.deleteAliasLocked(k)
			continue
		}
		bySession[s] = append(bySession[s], k)
	}
	if len(bySession) == 0 {
		return
	}
	for s, keys := range bySession {
		if len(keys) <= liveAliasCap {
			continue
		}
		current := sessionCacheKey(s.Cookie, s.UserAgent, s.cacheIdentity)
		sort.Strings(keys)
		kept := make([]string, 0, liveAliasCap)
		if m.byID[current] == s {
			kept = append(kept, current)
		}
		if protected != current && m.byID[protected] == s {
			kept = append(kept, protected)
		}
		for _, k := range keys {
			if k == current || k == protected {
				continue
			}
			if len(kept) >= liveAliasCap {
				m.deleteAliasLocked(k)
				continue
			}
			kept = append(kept, k)
		}
		bySession[s] = kept
	}
	now := time.Now()
	sessions := make([]*Session, 0, len(bySession))
	for s := range bySession {
		sessions = append(sessions, s)
	}
	sort.Slice(sessions, func(i, j int) bool {
		return sessions[i].expiresAt.After(sessions[j].expiresAt)
	})
	expiredKept := 0
	for _, s := range sessions {
		if !now.After(s.expiresAt) {
			continue
		}
		limit := expiredSessionAliasCap - expiredKept
		if limit < 0 {
			limit = 0
		}
		keys := bySession[s]
		kept := make([]string, 0, limit)
		current := sessionCacheKey(s.Cookie, s.UserAgent, s.cacheIdentity)
		if limit > 0 && m.byID[current] == s {
			kept = append(kept, current)
		}
		if limit > len(kept) && protected != current && m.byID[protected] == s {
			kept = append(kept, protected)
		}
		for _, k := range keys {
			if k == current || k == protected || len(kept) >= limit {
				continue
			}
			kept = append(kept, k)
		}
		keptSet := make(map[string]struct{}, len(kept))
		for _, k := range kept {
			keptSet[k] = struct{}{}
		}
		for _, k := range keys {
			if _, ok := keptSet[k]; ok {
				continue
			}
			m.deleteAliasLocked(k)
		}
		bySession[s] = kept
		expiredKept += len(kept)
	}
}

// volatileCookieNames are the anti-abuse cookies Cloudflare rotates
// mid-session.
var volatileCookieNames = map[string]bool{
	"cf_clearance": true,
	"__cf_bm":      true,
	"_cfuvid":      true,
	"oai-did":      true,
}

// RecordCookieRotation folds session and anti-abuse cookie updates from an
// upstream response back into the session, so later requests present the
// freshest browser state. No-op when the session is unknown or unchanged.
func (m *SessionManager) RecordCookieRotation(s *Session, setCookies []string, protectedCookie string) *Session {
	if s == nil || len(setCookies) == 0 {
		return s
	}
	updates := make([]string, 0, len(setCookies))
	for _, sc := range setCookies {
		first := strings.SplitN(sc, ";", 2)[0]
		kv := strings.SplitN(strings.TrimSpace(first), "=", 2)
		if len(kv) != 2 || !volatileCookieNames[kv[0]] && !isSessionTokenName(kv[0]) {
			continue
		}
		updates = append(updates, sc)
	}
	if len(updates) == 0 {
		return s
	}
	oldKey := sessionCacheKey(s.Cookie, s.UserAgent, s.cacheIdentity)
	protectedKey := ""
	if protectedCookie != "" {
		protectedKey = sessionCacheKey(protectedCookie, s.UserAgent, s.cacheIdentity)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	current := m.byID[oldKey]
	if current == nil && protectedKey != "" {
		current = m.byID[protectedKey]
	}
	if current == nil {
		return s
	}
	if m.byID[oldKey] == current {
		m.setAliasLocked(oldKey, current)
	}
	if protectedKey != "" && m.byID[protectedKey] == current {
		m.setAliasLocked(protectedKey, current)
	}
	merged := MergeRefreshedCookie(current.Cookie, updates)
	if merged == current.Cookie {
		return current
	}
	if SessionToken(merged) == "" {
		m.deleteSessionAliasesLocked(current)
		return &Session{
			Cookie:        merged,
			AccountID:     current.AccountID,
			UserID:        current.UserID,
			DeviceID:      current.DeviceID,
			Email:         current.Email,
			UserAgent:     current.UserAgent,
			WebSessionID:  current.WebSessionID,
			cacheIdentity: current.cacheIdentity,
		}
	}
	current.warmupMu.Lock()
	warmedAt := current.warmedAt
	if s != current {
		// A concurrent warmup may have been claimed on the caller's copy
		// after it left the cache; carry the newest claim into the rotated
		// session so that warmup is not silently re-run.
		s.warmupMu.Lock()
		if s.warmedAt.After(warmedAt) {
			warmedAt = s.warmedAt
		}
		s.warmupMu.Unlock()
	}
	current.warmupMu.Unlock()
	rotated := &Session{
		Cookie:        merged,
		AccessToken:   current.AccessToken,
		AccountID:     current.AccountID,
		UserID:        current.UserID,
		DeviceID:      current.DeviceID,
		Email:         current.Email,
		UserAgent:     current.UserAgent,
		WebSessionID:  current.WebSessionID,
		expiresAt:     current.expiresAt,
		warmedAt:      warmedAt,
		cacheIdentity: current.cacheIdentity,
	}
	if deviceID := browserDeviceID(merged); deviceID != "" {
		rotated.DeviceID = deviceID
	}
	newKey := sessionCacheKey(merged, current.UserAgent, current.cacheIdentity)
	m.replaceSessionAliasesLocked(current, rotated)
	m.setAliasLocked(newKey, rotated)
	if oldKey != newKey && oldKey != protectedKey {
		m.deleteAliasLocked(oldKey)
	}
	return rotated
}

func (m *SessionManager) Invalidate(cookie string) {
	m.mu.Lock()
	// Entries aliased by earlier rotations carry a superseded token for
	// the same credential; drop every entry whose session token matches
	// the requested or the current rotated one so no stale alias survives.
	toks := map[string]struct{}{}
	if t := SessionToken(cookie); t != "" {
		toks[t] = struct{}{}
	}
	for key, s := range m.byID {
		if s != nil && key == sessionCacheKey(cookie, s.UserAgent, s.cacheIdentity) {
			if t := SessionToken(s.Cookie); t != "" {
				toks[t] = struct{}{}
			}
		}
	}
	for k, v := range m.byID {
		if _, hit := toks[SessionToken(v.Cookie)]; hit {
			m.deleteAliasLocked(k)
		}
	}
	m.mu.Unlock()
}

// ResolveLoginSession validates the cookie and returns the exchanged
// session, including any token rotation from the response Set-Cookie
// headers, so the caller persists the rotated credential.
func ResolveLoginSession(ctx context.Context, client *http.Client, cookie, userAgent string) (*Session, error) {
	return exchangeSession(ctx, client, cookie, userAgent)
}

func exchangeSession(ctx context.Context, client *http.Client, cookie, userAgent string) (*Session, error) {
	return exchangeSessionWithDeviceHint(ctx, client, cookie, "", userAgent)
}

func exchangeSessionWithDeviceHint(ctx context.Context, client *http.Client, cookie, priorDeviceID, userAgent string) (*Session, error) {
	var err error
	userAgent, err = validateUserAgentOrDefault(userAgent)
	if err != nil {
		return nil, err
	}
	urls := currentBaseURLs()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, urls.Base+"/api/auth/session", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Cookie", cookie)
	req.Header.Set("Referer", urls.Base+"/")
	ApplyBrowserHeaders(req, userAgent)

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("chatgpt-web: session exchange: %w", err)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if cerr := resp.Body.Close(); cerr != nil {
		log.WithError(cerr).Warn("chatgpt-web: close session response body")
	}
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, &upstreamStatusError{operation: "session exchange", code: resp.StatusCode}
	}
	var parsed sessionAPIResponse
	if errParse := json.Unmarshal(body, &parsed); errParse != nil {
		return nil, fmt.Errorf("chatgpt-web: session exchange parse: %w", errParse)
	}
	if parsed.AccessToken == "" {
		return nil, fmt.Errorf("chatgpt-web: session exchange returned no access token (cookie expired?)")
	}
	accountID := parsed.Account.ID
	merged := MergeRefreshedCookie(cookie, resp.Header.Values("Set-Cookie"))
	if SessionToken(merged) == "" {
		return nil, fmt.Errorf("chatgpt-web: session exchange returned no usable session token")
	}
	deviceID := DeviceIDForCookie(merged)
	if browserDeviceID(cookie) == "" && browserDeviceID(merged) == "" && DeviceIDForCookie(cookie) != deviceID && priorDeviceID != "" {
		// Session-token rotation must not change the device identity.
		deviceID = priorDeviceID
	}
	webSessionID, err := newWebSessionID()
	if err != nil {
		return nil, err
	}
	return &Session{
		Cookie:       merged,
		AccessToken:  parsed.AccessToken,
		AccountID:    accountID,
		UserID:       parsed.User.ID,
		Email:        parsed.User.Email,
		DeviceID:     deviceID,
		UserAgent:    userAgent,
		WebSessionID: webSessionID,
		expiresAt:    time.Now().Add(sessionTokenMaxAge),
	}, nil
}
