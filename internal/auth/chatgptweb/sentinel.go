package chatgptweb

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	xhtml "golang.org/x/net/html"
)

const (
	sentinelPreparePath      = "/sentinel/chat-requirements/prepare"
	sentinelRequirementsPath = "/sentinel/chat-requirements"

	prefixRequirements = "gAAAAAC"
	prefixProof        = "gAAAAAB"
	configSuffix       = "~S"

	powMaxIterations = 500000
	buildCacheTTL    = time.Hour

	defaultBuildID = clientVersion
)

type turnstileRequiredError struct{}

func (*turnstileRequiredError) Error() string {
	return "chatgpt-web: ChatGPT Turnstile challenge required"
}

func (*turnstileRequiredError) StatusCode() int {
	return http.StatusForbidden
}

func (*turnstileRequiredError) AuthStateNeutral() bool {
	return true
}

var ErrTurnstileRequired error = &turnstileRequiredError{}

func actionableTurnstileError() error {
	return fmt.Errorf("%w: cookie replay cannot solve this challenge; recopying a fresh full Cookie header from an active ChatGPT tab and using the same network may reduce challenge risk but is not guaranteed", ErrTurnstileRequired)
}

var scriptURLPool = []string{
	"https://chatgpt.com/backend-api/sentinel/sdk.js",
	"https://chatgpt.com/sentinel/20260219f9f6/sdk.js",
}

var navigatorProbes = []string{
	"windowControlsOverlay−[object WindowControlsOverlay]",
	"geolocation−[object Geolocation]",
	"clipboard−[object Clipboard]",
	"mediaDevices−[object MediaDevices]",
	"permissions−[object Permissions]",
	"bluetooth−[object Bluetooth]",
	"usb−[object USB]",
	"serial−[object Serial]",
	"hid−[object HID]",
	"presentation−[object Presentation]",
	"credentials−[object CredentialsContainer]",
}

var documentKeys = []string{
	"_reactListening8in7sfyhjvp", "_reactListeningo743lnnpvdg",
	"_reactContainer$5pyziap1brc", "__reactContainer$b63yiita51i",
	"location", "cookie", "referrer", "currentScript", "body", "head", "documentElement",
}

var windowKeys = []string{
	"onchange", "onclick", "onload", "onerror", "onresize",
	"onmouseover", "onmouseout", "onfocus", "onblur", "onscroll",
	"onkeydown", "onkeyup", "onkeypress",
	"requestIdleCallback", "requestAnimationFrame", "setTimeout",
	"fetch", "console", "Promise", "Map", "Set", "WeakMap", "WeakSet",
	"crypto", "performance", "navigator", "document", "location", "history",
	"localStorage", "sessionStorage", "indexedDB",
	"Image", "XMLHttpRequest", "FormData", "Headers", "Request", "Response",
	"alert", "confirm", "prompt", "close", "focus", "blur",
	"addEventListener", "removeEventListener", "dispatchEvent",
	"scrollTo", "scrollBy", "scroll", "matchMedia", "getComputedStyle",
	"getSelection", "find", "stop", "open", "print", "captureEvents",
	"releaseEvents", "queueMicrotask", "reportError", "structuredClone",
	"isSecureContext", "crossOriginIsolated", "originAgentCluster",
	"speechSynthesis", "MediaSource", "Blob", "File", "FileReader",
	"Atomics", "SharedArrayBuffer", "WebAssembly", "BigInt", "Symbol", "Proxy",
}

type SentinelRequirements struct {
	Token        string
	PrepareToken string
	ProofToken   string
	Turnstile    string
}

type sentinelPrepareResponse struct {
	Persona      string `json:"persona"`
	Token        string `json:"token"`
	PrepareToken string `json:"prepare_token"`
	ProofOfWork  struct {
		Required   bool   `json:"required"`
		Seed       string `json:"seed"`
		Difficulty string `json:"difficulty"`
	} `json:"proofofwork"`
	Turnstile struct {
		Required bool   `json:"required"`
		DX       string `json:"dx"`
	} `json:"turnstile"`
	ForceLogin bool `json:"force_login"`
}

type sentinelRequirementsResponse = sentinelPrepareResponse

type buildInfo struct {
	dpl        string
	scriptSrc  string
	fetchedAt  time.Time
	setCookies []string
	err        error
}

var buildCache struct {
	mu      sync.Mutex
	entries map[string]buildInfo
}

func fetchBuildInfo(ctx context.Context, client *http.Client, s *Session) (dpl, scriptSrc string, setCookies []string) {
	urls := currentBaseURLs()
	stableIdentity := ""
	networkIdentity := ""
	if s != nil {
		stableIdentity = strings.TrimSpace(s.AccountID) + "\x00" + strings.TrimSpace(s.DeviceID)
		networkIdentity = s.cacheIdentity
	}
	cacheKey := urls.Base + "\x00" + sessionCacheKey(stableIdentity, userAgentForSession(s), networkIdentity)
	now := time.Now()
	buildCache.mu.Lock()
	for key, cached := range buildCache.entries {
		if now.Sub(cached.fetchedAt) >= buildCacheTTL {
			delete(buildCache.entries, key)
		}
	}
	if cached, ok := buildCache.entries[cacheKey]; ok {
		buildCache.mu.Unlock()
		return cached.dpl, cached.scriptSrc, nil
	}
	buildCache.mu.Unlock()

	// Concurrent misses intentionally fetch independently so one caller's
	// cancellation cannot fail other same-identity callers; failures are not
	// cached, and successes converge on the same cached entry.
	dpl, scriptSrc, setCookies, err := scrapeBuildInfo(ctx, client, s)
	if err != nil {
		log.WithError(err).Debug("chatgpt-web: homepage build discovery failed")
		return "", "", setCookies
	}
	buildCache.mu.Lock()
	if buildCache.entries == nil {
		buildCache.entries = make(map[string]buildInfo)
	}
	buildCache.entries[cacheKey] = buildInfo{dpl: dpl, scriptSrc: scriptSrc, fetchedAt: time.Now()}
	buildCache.mu.Unlock()
	return dpl, scriptSrc, setCookies
}

func scrapeBuildInfo(ctx context.Context, client *http.Client, s *Session) (dpl, scriptSrc string, setCookies []string, err error) {
	urls := currentBaseURLs()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, urls.Base+"/", nil)
	if err != nil {
		return "", "", nil, err
	}
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	req.Header.Set("User-Agent", userAgentForSession(s))
	applyNavigationHeaders(req, userAgentForSession(s))
	if s != nil {
		req.Header.Set("Cookie", s.Cookie)
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", "", nil, err
	}
	setCookies = resp.Header.Values("Set-Cookie")
	body, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if cerr := resp.Body.Close(); cerr != nil {
		log.WithError(cerr).Warn("chatgpt-web: close homepage body")
	}
	if err != nil {
		return "", "", setCookies, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", "", setCookies, fmt.Errorf("homepage returned status %d", resp.StatusCode)
	}
	tokenizer := xhtml.NewTokenizer(strings.NewReader(string(body)))
	for {
		tokenType := tokenizer.Next()
		if tokenType == xhtml.ErrorToken {
			break
		}
		if tokenType != xhtml.StartTagToken && tokenType != xhtml.SelfClosingTagToken {
			continue
		}
		token := tokenizer.Token()
		for _, attr := range token.Attr {
			if dpl == "" && strings.EqualFold(attr.Key, "data-build") {
				dpl = attr.Val
			}
			if strings.EqualFold(token.Data, "script") && strings.EqualFold(attr.Key, "src") {
				if source := normalizeSentinelScriptSource(attr.Val); source != "" {
					scriptSrc = source
				}
			}
		}
		if dpl != "" && scriptSrc != "" {
			break
		}
	}
	return dpl, scriptSrc, setCookies, nil
}

func normalizeSentinelScriptSource(source string) string {
	if !isSentinelScriptSource(source) {
		return ""
	}
	u, err := url.Parse(strings.TrimSpace(source))
	if err != nil || u.IsAbs() {
		return strings.TrimSpace(source)
	}
	base, err := url.Parse(currentBaseURLs().Base)
	if err != nil {
		return ""
	}
	return base.ResolveReference(u).String()
}

func isSentinelScriptSource(source string) bool {
	u, err := url.Parse(strings.TrimSpace(source))
	if err != nil || (u.Scheme != "" && u.Scheme != "https") {
		return false
	}
	if u.Host != "" && !strings.EqualFold(u.Hostname(), "chatgpt.com") {
		return false
	}
	return u.Path == "/backend-api/sentinel/sdk.js" ||
		(strings.HasPrefix(u.Path, "/sentinel/") && strings.HasSuffix(u.Path, "/sdk.js"))
}

func browserConfig(deviceID, dpl, scriptSrc, userAgent string) []any {
	if scriptSrc == "" {
		scriptSrc = scriptURLPool[rand.Intn(len(scriptURLPool))]
	}
	if dpl == "" {
		dpl = defaultBuildID
	}
	return []any{
		3000,
		sentinelDateString(),
		int64(4294967296),
		0,
		userAgentOrDefault(userAgent),
		scriptSrc,
		dpl,
		"en-US",
		"en-US,en",
		rand.Float64(),
		navigatorProbes[rand.Intn(len(navigatorProbes))],
		documentKeys[rand.Intn(len(documentKeys))],
		windowKeys[rand.Intn(len(windowKeys))],
		float64(0),
		deviceID,
		"",
		8,
		float64(time.Now().UnixMilli()),
		0, 0, 0, 0, 0, 0, 0,
	}
}

func sentinelDateString() string {
	// A real browser reports its own local timezone here; forcing a fixed
	// zone would contradict the exit IP's geography.
	t := time.Now()
	head := t.Format("Mon Jan 02 2006 15:04:05")
	zoneName, offset := t.Zone()
	sign := "+"
	if offset < 0 {
		sign = "-"
		offset = -offset
	}
	gmt := fmt.Sprintf("GMT%s%02d%02d", sign, offset/3600, (offset%3600)/60)
	if full := fullTimezoneName(zoneName); full != "" {
		zoneName = full
	}
	return head + " " + gmt + " (" + zoneName + ")"
}

func fullTimezoneName(short string) string {
	names := map[string]string{
		"PDT":  "Pacific Daylight Time",
		"PST":  "Pacific Standard Time",
		"EDT":  "Eastern Daylight Time",
		"EST":  "Eastern Standard Time",
		"CDT":  "Central Daylight Time",
		"CST":  "Central Standard Time",
		"MDT":  "Mountain Daylight Time",
		"MST":  "Mountain Standard Time",
		"BST":  "British Summer Time",
		"GMT":  "Greenwich Mean Time",
		"UTC":  "Coordinated Universal Time",
		"JST":  "Japan Standard Time",
		"KST":  "Korea Standard Time",
		"AEST": "Australian Eastern Standard Time",
		"AEDT": "Australian Eastern Daylight Time",
		"NZST": "New Zealand Standard Time",
		"NZDT": "New Zealand Daylight Time",
	}
	return names[short]
}

func generateRequirementsToken(deviceID, dpl, scriptSrc, userAgent string) string {
	return generateRequirementsTokenWithConfig(browserConfig(deviceID, dpl, scriptSrc, userAgent))
}

func generateRequirementsTokenWithConfig(baseConfig []any) string {
	config := append([]any(nil), baseConfig...)
	config[3] = 1
	return prefixRequirements + encodeConfig(config) + configSuffix
}

func encodeConfig(config []any) string {
	raw, err := json.Marshal(config)
	if err != nil {
		return ""
	}
	return base64.StdEncoding.EncodeToString(raw)
}

func fnv1a32(text string) string {
	return fnv1a32Bytes([]byte(text))
}

func fnv1a32Bytes(text []byte) string {
	return fmt.Sprintf("%08x", fnv1a32Value(text))
}

func fnv1a32Value(text []byte) uint32 {
	h := uint32(2166136261)
	for _, ch := range text {
		h ^= uint32(ch)
		h *= 16777619
	}
	h ^= h >> 16
	h *= 2246822507
	h ^= h >> 13
	h *= 3266489909
	h ^= h >> 16
	return h
}

func solveProof(ctx context.Context, seed, difficulty, deviceID, dpl, scriptSrc, userAgent string) (string, error) {
	return solveProofWithConfig(ctx, seed, difficulty, browserConfig(deviceID, dpl, scriptSrc, userAgent))
}

func solveProofWithConfig(ctx context.Context, seed, difficulty string, baseConfig []any) (string, error) {
	if len(difficulty) > 8 {
		return "", fmt.Errorf("difficulty %q exceeds hash length", difficulty)
	}
	for _, ch := range difficulty {
		if (ch < '0' || ch > '9') && (ch < 'a' || ch > 'f') {
			return "", fmt.Errorf("difficulty %q is not lowercase hexadecimal", difficulty)
		}
	}
	difficultyValue := uint64(0)
	if difficulty != "" {
		var err error
		difficultyValue, err = strconv.ParseUint(difficulty, 16, 32)
		if err != nil {
			return "", fmt.Errorf("parse difficulty: %w", err)
		}
	}
	difficultyShift := uint(32 - len(difficulty)*4)
	// Only slots 3 (nonce) and 9 (elapsed ms) vary per iteration. Marshal
	// the invariant head and tail once so the loop only re-encodes the two
	// varying scalars.
	if len(baseConfig) <= 9 {
		return "", fmt.Errorf("unexpected config length %d", len(baseConfig))
	}
	headRaw := mustJSON(baseConfig[:3])
	midRaw := mustJSON(baseConfig[4:9])
	tailRaw := mustJSON(baseConfig[10:])
	head := string(headRaw)[:len(headRaw)-1] // strip "]"
	mid := string(midRaw)
	mid = mid[1 : len(mid)-1]   // strip "[" and "]"
	tail := string(tailRaw)[1:] // strip "["

	start := time.Now()
	jsonBuf := make([]byte, 0, 1024)
	encBuf := make([]byte, 0, base64.StdEncoding.EncodedLen(1024))
	hashIn := make([]byte, 0, len(seed)+cap(encBuf))
	for i := 0; i < powMaxIterations; i++ {
		if i&1023 == 0 {
			if err := ctx.Err(); err != nil {
				return "", err
			}
		}
		jsonBuf = jsonBuf[:0]
		jsonBuf = append(jsonBuf, head...)
		jsonBuf = append(jsonBuf, ',')
		jsonBuf = strconv.AppendInt(jsonBuf, int64(i), 10)
		jsonBuf = append(jsonBuf, ',')
		jsonBuf = append(jsonBuf, mid...)
		jsonBuf = append(jsonBuf, ',')
		jsonBuf = strconv.AppendInt(jsonBuf, time.Since(start).Milliseconds(), 10)
		jsonBuf = append(jsonBuf, ',')
		jsonBuf = append(jsonBuf, tail...)
		encLen := base64.StdEncoding.EncodedLen(len(jsonBuf))
		if cap(encBuf) < encLen {
			encBuf = make([]byte, encLen)
		}
		enc := encBuf[:encLen]
		base64.StdEncoding.Encode(enc, jsonBuf)
		hashIn = hashIn[:0]
		hashIn = append(hashIn, seed...)
		hashIn = append(hashIn, enc...)
		if uint64(fnv1a32Value(hashIn)>>difficultyShift) <= difficultyValue {
			return prefixProof + string(enc) + configSuffix, nil
		}
	}
	return "", fmt.Errorf("no solution within %d iterations", powMaxIterations)
}

// RequirementsOptions controls how FetchRequirements handles a Turnstile
// challenge reported by sentinel.
type RequirementsOptions struct {
	// TurnstileToken is a legitimate token captured by the user from an
	// active ChatGPT browser tab. It is forwarded verbatim when present.
	TurnstileToken string
	// SubmitWithoutTurnstile is an explicit opt-in compatibility gamble:
	// when sentinel reports turnstile.required and no token is available,
	// return the remaining requirement tokens anyway so the caller may make
	// exactly one ordinary conversation submission without a Turnstile
	// token. It is unreliable, never retried, and defaults to off.
	SubmitWithoutTurnstile bool
}

// FetchRequirements performs the two-stage sentinel flow and returns the
// tokens required on a conversation request.
func FetchRequirements(ctx context.Context, client *http.Client, s *Session, opts RequirementsOptions) (*SentinelRequirements, []string, error) {
	urls := currentBaseURLs()
	turnstileToken := strings.TrimSpace(opts.TurnstileToken)
	userAgent := userAgentForSession(s)
	dpl, scriptSrc, cookieUpdates := fetchBuildInfo(ctx, client, s)
	sentinelSession := s
	if s != nil {
		sentinelSession = &Session{
			Cookie:       s.Cookie,
			AccessToken:  s.AccessToken,
			AccountID:    s.AccountID,
			DeviceID:     s.DeviceID,
			WebSessionID: s.WebSessionID,
			UserAgent:    s.UserAgent,
		}
	}
	if sentinelSession != nil && len(cookieUpdates) > 0 {
		hadSessionToken := SessionToken(sentinelSession.Cookie) != ""
		sentinelSession.Cookie = MergeRefreshedCookie(sentinelSession.Cookie, cookieUpdates)
		if hadSessionToken && SessionToken(sentinelSession.Cookie) == "" {
			return nil, cookieUpdates, &upstreamStatusError{operation: "homepage session rotation", code: http.StatusUnauthorized}
		}
		if rotatedDeviceID := browserDeviceID(sentinelSession.Cookie); rotatedDeviceID != "" {
			sentinelSession.DeviceID = rotatedDeviceID
		}
	}
	deviceID := ""
	if sentinelSession != nil {
		deviceID = sentinelSession.DeviceID
	}
	config := browserConfig(deviceID, dpl, scriptSrc, userAgent)
	initial := generateRequirementsTokenWithConfig(config)

	prepareRaw, prepareCookies, err := postSentinel(ctx, client, urls.Backend+sentinelPreparePath, sentinelPreparePath, sentinelSession, mustJSON(map[string]any{"p": initial}))
	cookieUpdates = append(cookieUpdates, prepareCookies...)
	if err != nil {
		return nil, cookieUpdates, fmt.Errorf("chatgpt-web: sentinel prepare: %w", err)
	}
	if sentinelSession != nil && len(prepareCookies) > 0 {
		hadSessionToken := SessionToken(sentinelSession.Cookie) != ""
		flowCookies := make([]string, 0, len(prepareCookies))
		for _, setCookie := range prepareCookies {
			name, _, _ := strings.Cut(strings.SplitN(setCookie, ";", 2)[0], "=")
			if strings.TrimSpace(name) != "oai-did" {
				flowCookies = append(flowCookies, setCookie)
			}
		}
		sentinelSession.Cookie = MergeRefreshedCookie(sentinelSession.Cookie, flowCookies)
		if hadSessionToken && SessionToken(sentinelSession.Cookie) == "" {
			return nil, cookieUpdates, &upstreamStatusError{operation: "sentinel prepare session rotation", code: http.StatusUnauthorized}
		}
	}
	var prep sentinelPrepareResponse
	if errParse := json.Unmarshal(prepareRaw, &prep); errParse != nil {
		return nil, cookieUpdates, fmt.Errorf("chatgpt-web: sentinel prepare parse: %w", errParse)
	}
	if prep.ForceLogin {
		return nil, cookieUpdates, &upstreamStatusError{operation: "sentinel force_login", code: http.StatusUnauthorized}
	}
	if prep.Turnstile.Required && turnstileToken == "" && !opts.SubmitWithoutTurnstile {
		return nil, cookieUpdates, actionableTurnstileError()
	}
	if prep.PrepareToken == "" {
		return nil, cookieUpdates, fmt.Errorf("chatgpt-web: sentinel prepare returned no prepare_token")
	}

	payload := map[string]string{"p": initial, "prepare_token": prep.PrepareToken}
	requirementsRaw, requirementsCookies, err := postSentinel(ctx, client, urls.Backend+sentinelRequirementsPath, sentinelRequirementsPath, sentinelSession, mustJSON(payload))
	cookieUpdates = append(cookieUpdates, requirementsCookies...)
	if err != nil {
		return nil, cookieUpdates, fmt.Errorf("chatgpt-web: sentinel requirements: %w", err)
	}
	if sentinelSession != nil && len(requirementsCookies) > 0 {
		hadSessionToken := SessionToken(sentinelSession.Cookie) != ""
		mergedCookie := MergeRefreshedCookie(sentinelSession.Cookie, requirementsCookies)
		if hadSessionToken && SessionToken(mergedCookie) == "" {
			return nil, cookieUpdates, &upstreamStatusError{operation: "sentinel requirements session rotation", code: http.StatusUnauthorized}
		}
	}
	var requirements sentinelRequirementsResponse
	if errParse := json.Unmarshal(requirementsRaw, &requirements); errParse != nil {
		return nil, cookieUpdates, fmt.Errorf("chatgpt-web: sentinel requirements parse: %w", errParse)
	}
	if requirements.ForceLogin {
		return nil, cookieUpdates, &upstreamStatusError{operation: "sentinel force_login", code: http.StatusUnauthorized}
	}
	if requirements.Turnstile.Required && turnstileToken == "" && !opts.SubmitWithoutTurnstile {
		return nil, cookieUpdates, actionableTurnstileError()
	}
	if requirements.Token == "" {
		return nil, cookieUpdates, fmt.Errorf("chatgpt-web: sentinel requirements returned no token")
	}

	var proof string
	if requirements.ProofOfWork.Required {
		if requirements.ProofOfWork.Seed == "" {
			return nil, cookieUpdates, fmt.Errorf("chatgpt-web: sentinel requires proof of work but returned an empty seed")
		}
		proof, err = solveProofWithConfig(ctx, requirements.ProofOfWork.Seed, requirements.ProofOfWork.Difficulty, config)
		if err != nil {
			return nil, cookieUpdates, fmt.Errorf("chatgpt-web: sentinel proof of work: %w", err)
		}
	}

	return &SentinelRequirements{
		Token:        requirements.Token,
		PrepareToken: prep.PrepareToken,
		ProofToken:   proof,
		Turnstile:    turnstileToken,
	}, cookieUpdates, nil
}

func newSentinelRequest(ctx context.Context, url, targetPath string, body []byte, s *Session) (*http.Request, error) {
	urls := currentBaseURLs()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(string(body)))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "*/*")
	req.Header.Set("User-Agent", userAgentForSession(s))
	req.Header.Set("Origin", urls.Base)
	req.Header.Set("Referer", urls.Base+"/")
	if s != nil {
		req.Header.Set("Cookie", s.Cookie)
		if s.AccessToken != "" {
			req.Header.Set("Authorization", "Bearer "+s.AccessToken)
		}
		if s.AccountID != "" {
			req.Header.Set("chatgpt-account-id", s.AccountID)
		}
		req.Header.Set("OAI-Device-Id", s.DeviceID)
		if s.WebSessionID != "" {
			req.Header.Set("OAI-Session-Id", s.WebSessionID)
		}
	}
	req.Header.Set("OAI-Language", "en-US")
	req.Header.Set("OAI-Client-Version", clientVersion)
	req.Header.Set("OAI-Client-Build-Number", clientBuildNumber)
	req.Header.Set("X-OpenAI-Target-Path", "/backend-api"+targetPath)
	req.Header.Set("X-OpenAI-Target-Route", "/backend-api"+targetPath)
	ApplyBrowserHeaders(req, userAgentForSession(s))
	return req, nil
}

func postSentinel(ctx context.Context, client *http.Client, url, targetPath string, s *Session, body []byte) ([]byte, []string, error) {
	req, err := newSentinelRequest(ctx, url, targetPath, body, s)
	if err != nil {
		return nil, nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, nil, err
	}
	setCookies := resp.Header.Values("Set-Cookie")
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if cerr := resp.Body.Close(); cerr != nil {
		log.WithError(cerr).Warn("chatgpt-web: close sentinel body")
	}
	if err != nil {
		return nil, setCookies, err
	}
	if isSentinelTurnstileChallenge(resp, raw) {
		return nil, setCookies, actionableTurnstileError()
	}
	if resp.StatusCode != http.StatusOK {
		return nil, setCookies, &upstreamStatusError{operation: "sentinel", code: resp.StatusCode}
	}
	return raw, setCookies, nil
}

func isSentinelTurnstileChallenge(resp *http.Response, raw []byte) bool {
	if resp == nil || resp.StatusCode != http.StatusForbidden {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(resp.Header.Get("Cf-Mitigated")), "challenge") {
		return true
	}
	body := strings.ToLower(string(raw))
	var payload struct {
		Turnstile struct {
			Required bool `json:"required"`
		} `json:"turnstile"`
		Code    string          `json:"code"`
		Detail  string          `json:"detail"`
		Message string          `json:"message"`
		Error   json.RawMessage `json:"error"`
	}
	if json.Unmarshal(raw, &payload) == nil {
		if payload.Turnstile.Required {
			return true
		}
		var nestedError struct {
			Code    string `json:"code"`
			Type    string `json:"type"`
			Message string `json:"message"`
		}
		_ = json.Unmarshal(payload.Error, &nestedError)
		for _, value := range []string{payload.Code, nestedError.Code, nestedError.Type} {
			switch strings.ToLower(strings.TrimSpace(value)) {
			case "turnstile_required", "turnstile_token_required", "turnstile_challenge":
				return true
			}
		}
		var errorText string
		_ = json.Unmarshal(payload.Error, &errorText)
		if isTurnstileErrorText(errorText) || isTurnstileErrorText(nestedError.Message) || isTurnstileErrorText(payload.Detail) || isTurnstileErrorText(payload.Message) {
			return true
		}
	}
	contentType := strings.ToLower(resp.Header.Get("Content-Type"))
	return strings.Contains(contentType, "text/html") &&
		(strings.Contains(body, "/cdn-cgi/challenge-platform/") || strings.Contains(body, "cf-chl-"))
}

func isTurnstileErrorText(value string) bool {
	text := strings.ToLower(strings.TrimSpace(value))
	// A negated mention is informational, not a challenge requirement —
	// unless a positive required-form clause also appears in the text.
	positiveRequired := false
	for _, phrase := range []string{"turnstile token is required", "turnstile challenge is required", "turnstile is required", "turnstile token required", "turnstile challenge required", "turnstile required"} {
		if strings.Contains(text, phrase) {
			positiveRequired = true
			break
		}
	}
	if !positiveRequired && strings.Contains(text, "not required") {
		return false
	}
	for _, signal := range []string{
		"turnstile required",
		"turnstile is required",
		"turnstile challenge required",
		"turnstile challenge is required",
		"turnstile token required",
		"turnstile token is required",
		"a turnstile token is required",
	} {
		if text == signal {
			return true
		}
		// A recognized signal at the start of the message is a challenge even
		// when ordinary continuation text follows ("... to continue").
		if strings.HasPrefix(text, signal) {
			rest := strings.TrimLeft(text[len(signal):], " .,:;(-–—")
			if rest == "" {
				return true
			}
			// With a required-form clause already seen, later clauses are
			// unrelated context; without one, only accept ordinary
			// continuation wording ("... to continue").
			if positiveRequired {
				return true
			}
			for _, word := range []string{"to", "before", "please", "and", "or", "then", "in", "for", "you", "your"} {
				if rest == word || strings.HasPrefix(rest, word+" ") {
					return true
				}
			}
		}
	}
	// Imperative challenge wording directed at the user, e.g. "Please complete
	// the Turnstile challenge to continue". Require both a request verb and a
	// challenge/captcha object so an unrelated mention such as "Turnstile
	// provider unavailable" is not misclassified.
	if strings.Contains(text, "turnstile") {
		hasVerb := false
		hasObject := strings.Contains(text, "challenge") || strings.Contains(text, "captcha") || strings.Contains(text, "verification")
		for _, word := range strings.FieldsFunc(text, func(r rune) bool {
			return !('a' <= r && r <= 'z')
		}) {
			switch word {
			case "complete", "completing", "finish", "pass", "solve", "verify", "provide", "submit":
				hasVerb = true
			}
		}
		if hasVerb && hasObject {
			return true
		}
	}
	return false
}

func mustJSON(v any) []byte {
	raw, err := json.Marshal(v)
	if err != nil {
		return []byte("{}")
	}
	return raw
}
