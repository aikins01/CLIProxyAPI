package amp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/api/handlers"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/api/handlers/claude"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/api/handlers/gemini"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/api/handlers/openai"
	log "github.com/sirupsen/logrus"
)

// clientAPIKeyContextKey is the context key used to pass the client API key
// from gin.Context to the request context for SecretSource lookup.
type clientAPIKeyContextKey struct{}

const neoInternalClientAPIKeyHeader = "X-Cliproxy-Internal-Client-API-Key"

// clientAPIKeyMiddleware injects the authenticated client API key from gin.Context["userApiKey"]
// into the request context so that SecretSource can look it up for per-client upstream routing.
func clientAPIKeyMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Extract the client API key from gin context (set by AuthMiddleware)
		if apiKey, exists := c.Get("userApiKey"); exists {
			if keyStr, ok := apiKey.(string); ok && keyStr != "" {
				// Inject into request context for SecretSource.Get(ctx) to read
				ctx := context.WithValue(c.Request.Context(), clientAPIKeyContextKey{}, keyStr)
				c.Request = c.Request.WithContext(ctx)
			}
		}
		c.Next()
	}
}

// getClientAPIKeyFromContext retrieves the client API key from request context.
// Returns empty string if not present.
func getClientAPIKeyFromContext(ctx context.Context) string {
	if val := ctx.Value(clientAPIKeyContextKey{}); val != nil {
		if keyStr, ok := val.(string); ok {
			return keyStr
		}
	}
	return ""
}

// localhostOnlyMiddleware returns a middleware that dynamically checks the module's
// localhost restriction setting. This allows hot-reload of the restriction without restarting.
func (m *AmpModule) localhostOnlyMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Check current setting (hot-reloadable)
		if !m.IsRestrictedToLocalhost() {
			c.Next()
			return
		}

		// Use actual TCP connection address (RemoteAddr) to prevent header spoofing
		// This cannot be forged by X-Forwarded-For or other client-controlled headers
		remoteAddr := c.Request.RemoteAddr

		// RemoteAddr format is "IP:port" or "[IPv6]:port", extract just the IP
		host, _, err := net.SplitHostPort(remoteAddr)
		if err != nil {
			// Try parsing as raw IP (shouldn't happen with standard HTTP, but be defensive)
			host = remoteAddr
		}

		// Parse the IP to handle both IPv4 and IPv6
		ip := net.ParseIP(host)
		if ip == nil {
			log.Warnf("amp management: invalid RemoteAddr %s, denying access", remoteAddr)
			c.AbortWithStatusJSON(403, gin.H{
				"error": "Access denied: management routes restricted to localhost",
			})
			return
		}

		// Check if IP is loopback (127.0.0.1 or ::1)
		if !ip.IsLoopback() {
			log.Warnf("amp management: non-localhost connection from %s attempted access, denying", remoteAddr)
			c.AbortWithStatusJSON(403, gin.H{
				"error": "Access denied: management routes restricted to localhost",
			})
			return
		}

		c.Next()
	}
}

// noCORSMiddleware disables CORS for management routes to prevent browser-based attacks.
// This overwrites any global CORS headers set by the server.
func noCORSMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.GetBool(ampWebLocalInferenceCORSContextKey) {
			c.Next()
			return
		}

		// Remove CORS headers to prevent cross-origin access from browsers
		c.Header("Access-Control-Allow-Origin", "")
		c.Header("Access-Control-Allow-Methods", "")
		c.Header("Access-Control-Allow-Headers", "")
		c.Header("Access-Control-Allow-Credentials", "")

		// For OPTIONS preflight, deny with 403
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(403)
			return
		}

		c.Next()
	}
}

// managementAvailabilityMiddleware short-circuits management routes when the upstream
// proxy is disabled, preventing noisy localhost warnings and accidental exposure.
func (m *AmpModule) managementAvailabilityMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if m.getProxy() == nil {
			if m.canServeNeoLocalManagement(c) {
				c.Next()
				return
			}
			logging.SkipGinRequestLogging(c)
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{
				"error": "amp upstream proxy not available",
			})
			return
		}
		c.Next()
	}
}

// wrapManagementAuth skips auth for selected management paths while keeping authentication elsewhere.
func wrapManagementAuth(auth gin.HandlerFunc, prefixes ...string) gin.HandlerFunc {
	return func(c *gin.Context) {
		path := c.Request.URL.Path
		if !c.GetBool(ampWebLocalInferenceCORSContextKey) && actorEngineRequest(c.Request) {
			c.Next()
			return
		}
		if managementPathMatches(path, prefixes...) {
			c.Next()
			return
		}
		auth(c)
	}
}

func wrapLocalConnectionManagementAuth(auth gin.HandlerFunc, prefixes ...string) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !c.GetBool(ampWebLocalInferenceCORSContextKey) && strings.TrimSpace(c.GetHeader("Origin")) == "" && managementPathMatches(c.Request.URL.Path, prefixes...) && requestRemoteAddrIsLocalConnection(c.Request) {
			c.Next()
			return
		}
		auth(c)
	}
}

func managementPathMatches(path string, prefixes ...string) bool {
	for _, prefix := range prefixes {
		if strings.HasPrefix(path, prefix) && (len(path) == len(prefix) || path[len(prefix)] == '/') {
			return true
		}
	}
	return false
}

func requestRemoteAddrIsLocalConnection(r *http.Request) bool {
	ip := requestRemoteAddrIP(r)
	if ip == nil {
		return false
	}
	if ip.IsUnspecified() {
		return false
	}
	if ip.IsLoopback() {
		return true
	}
	localIP := requestLocalAddrIP(r)
	return localIP != nil && ip.Equal(localIP)
}

func requestRemoteAddrIP(r *http.Request) net.IP {
	if r == nil {
		return nil
	}
	return requestAddrIP(r.RemoteAddr)
}

func requestLocalAddrIP(r *http.Request) net.IP {
	if r == nil {
		return nil
	}
	addr, _ := r.Context().Value(http.LocalAddrContextKey).(net.Addr)
	if addr == nil {
		return nil
	}
	return requestAddrIP(addr.String())
}

func requestAddrIP(addr string) net.IP {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	host = strings.Trim(host, "[]")
	if zoneIndex := strings.LastIndex(host, "%"); zoneIndex >= 0 {
		host = host[:zoneIndex]
	}
	return net.ParseIP(host)
}

// registerManagementRoutes registers Amp management proxy routes
// Uses dynamic middleware and proxy getter for hot-reload support.
// The auth middleware validates Authorization header against configured API keys.
func (m *AmpModule) registerManagementRoutes(engine *gin.Engine, baseHandler *handlers.BaseAPIHandler, auth gin.HandlerFunc) {
	engine.GET("/ampcode/local-inference.user.js", m.serveWebLocalInferenceUserscript)

	ampAPI := engine.Group("/api")

	// Always disable CORS for management routes to prevent browser-based attacks
	ampAPI.Use(m.webLocalInferenceCORSMiddleware(), m.webLocalInferenceQueryAuthMiddleware(), m.managementAvailabilityMiddleware(), noCORSMiddleware())

	// Apply the configured management host restriction before API-key auth.
	ampAPI.Use(m.localhostOnlyMiddleware())

	// Apply API-key auth, bypassing /api/internal only for loopback or same-address local connections.
	var authWithBypass gin.HandlerFunc
	if auth != nil {
		ampAPI.Use(wrapLocalConnectionManagementAuth(auth, "/api/internal"))
		authWithBypass = wrapManagementAuth(auth, "/threads", "/auth", "/docs", "/settings")
	}

	// Inject client API key into request context for per-client upstream routing
	ampAPI.Use(clientAPIKeyMiddleware())

	// Dynamic proxy handler that uses m.getProxy() for hot-reload support
	proxyHandler := func(c *gin.Context) {
		if m.tryServeNeoLocalThreadActor(c) {
			return
		}
		if m.tryServeNeoLocalAttachment(c) {
			return
		}
		if m.tryServeNeoWebLocalRemote(c) {
			return
		}

		// Swallow ErrAbortHandler panics from ReverseProxy copyResponse to avoid noisy stack traces
		defer func() {
			if rec := recover(); rec != nil {
				if err, ok := rec.(error); ok && errors.Is(err, http.ErrAbortHandler) {
					// Upstream already wrote the status (often 404) before the client/stream ended.
					return
				}
				log.WithField("panic", rec).Error("amp upstream proxy panic")
				if !c.Writer.Written() {
					c.JSON(http.StatusBadGateway, gin.H{"error": "amp upstream proxy failed"})
				}
			}
		}()

		if m.tryServeNeoWebLocalInternalRPC(c) {
			return
		}
		if m.tryServeNeoLocalInternalRPC(c) {
			return
		}
		proxy := m.getProxy()
		if proxy == nil {
			c.JSON(503, gin.H{"error": "amp upstream proxy not available"})
			return
		}
		if m.tryServeNeoLocalThreadSearchFallback(c, proxy) {
			return
		}
		proxy.ServeHTTP(c.Writer, c.Request)
	}

	// Management routes - these are proxied directly to Amp upstream
	ampAPI.Any("/internal", proxyHandler)
	ampAPI.Any("/internal/*path", proxyHandler)
	ampAPI.Any("/user", proxyHandler)
	ampAPI.Any("/user/*path", proxyHandler)
	ampAPI.Any("/user-actor-credentials", proxyHandler)
	ampAPI.Any("/auth", proxyHandler)
	ampAPI.Any("/auth/*path", proxyHandler)
	ampAPI.Any("/meta", proxyHandler)
	ampAPI.Any("/meta/*path", proxyHandler)
	ampAPI.Any("/ads", proxyHandler)
	ampAPI.Any("/telemetry", proxyHandler)
	ampAPI.Any("/telemetry/*path", proxyHandler)
	ampAPI.Any("/1.0", proxyHandler)
	ampAPI.Any("/1.0/*path", proxyHandler)
	ampAPI.Any("/threads", proxyHandler)
	ampAPI.Any("/threads/*path", proxyHandler)
	ampAPI.Any("/thread-actors", proxyHandler)
	ampAPI.Any("/thread-actors/*path", proxyHandler)
	ampAPI.Any("/attachments", proxyHandler)
	ampAPI.Any("/attachments/*path", proxyHandler)
	ampAPI.Any("/otel", proxyHandler)
	ampAPI.Any("/otel/*path", proxyHandler)
	ampAPI.Any("/tab", proxyHandler)
	ampAPI.Any("/tab/*path", proxyHandler)
	ampAPI.Any("/durable-thread-workers", proxyHandler)
	ampAPI.Any("/durable-thread-workers/*path", proxyHandler)
	ampAPI.Any("/v2", proxyHandler)
	ampAPI.Any("/v2/*path", proxyHandler)

	// Root-level routes that AMP CLI expects without /api prefix
	// These need the same security middleware as the /api/* routes (dynamic for hot-reload)
	rootMiddleware := []gin.HandlerFunc{m.webLocalInferenceCORSMiddleware(), m.webLocalInferenceQueryAuthMiddleware(), m.managementAvailabilityMiddleware(), noCORSMiddleware(), m.localhostOnlyMiddleware()}
	if authWithBypass != nil {
		rootMiddleware = append(rootMiddleware, authWithBypass)
	}
	// Add clientAPIKeyMiddleware after auth for per-client upstream routing
	rootMiddleware = append(rootMiddleware, clientAPIKeyMiddleware())
	engine.Any("/threads", append(rootMiddleware, proxyHandler)...)
	engine.Any("/threads/*path", append(rootMiddleware, proxyHandler)...)
	engine.GET("/docs", append(rootMiddleware, proxyHandler)...)
	engine.GET("/docs/*path", append(rootMiddleware, proxyHandler)...)
	engine.GET("/settings", append(rootMiddleware, proxyHandler)...)
	engine.GET("/settings/*path", append(rootMiddleware, proxyHandler)...)
	engine.Any("/_app/remote/*path", append(rootMiddleware, proxyHandler)...)

	engine.GET("/threads.rss", append(rootMiddleware, proxyHandler)...)
	engine.GET("/news.rss", append(rootMiddleware, proxyHandler)...)

	neoRuntimeBridgeHandler := func(c *gin.Context) {
		if !m.shouldServeNeoRuntimeBridge(c.Request) {
			proxyHandler(c)
			return
		}
		m.serveNeoRuntimeBridge(c)
	}
	engine.Any("/gateway", append(rootMiddleware, neoRuntimeBridgeHandler)...)
	engine.Any("/gateway/*path", append(rootMiddleware, neoRuntimeBridgeHandler)...)
	engine.Any("/actors", append(rootMiddleware, neoRuntimeBridgeHandler)...)
	engine.Any("/actors/*path", append(rootMiddleware, neoRuntimeBridgeHandler)...)
	engine.OPTIONS("/metadata", append(rootMiddleware, neoRuntimeBridgeHandler)...)
	engine.GET("/metadata", append(rootMiddleware, neoRuntimeBridgeHandler)...)

	// Root-level auth routes for CLI login flow
	// Amp uses multiple auth routes: /auth/cli-login, /auth/callback, /auth/sign-in, /auth/logout
	// We proxy all /auth/* to support the complete OAuth flow
	engine.Any("/auth", append(rootMiddleware, proxyHandler)...)
	engine.Any("/auth/*path", append(rootMiddleware, proxyHandler)...)

	// Google v1beta1 passthrough with OAuth fallback
	// AMP CLI uses non-standard paths like /publishers/google/models/...
	// We bridge these to our standard Gemini handler to enable local OAuth.
	// If no local OAuth is available, falls back to ampcode.com proxy.
	geminiHandlers := gemini.NewGeminiAPIHandler(baseHandler)
	geminiBridge := createGeminiBridgeHandler(geminiHandlers.GeminiHandler)
	geminiV1Beta1Fallback := m.registerFallbackHandler(NewFallbackHandlerWithMapper(func() *httputil.ReverseProxy {
		return m.getProxy()
	}, m.modelMapper, m.forceModelMappings), m.currentCompactionCaptureDir())
	geminiV1Beta1Handler := geminiV1Beta1Fallback.WrapHandler(geminiBridge)

	// Route POST model calls through Gemini bridge with FallbackHandler.
	// FallbackHandler checks provider -> mapping -> proxy fallback automatically.
	// All other methods (e.g., GET model listing) always proxy to upstream to preserve Amp CLI behavior.
	ampAPI.Any("/provider/google/v1beta1/*path", func(c *gin.Context) {
		if c.Request.Method == "POST" {
			if path := c.Param("path"); strings.Contains(path, "/models/") {
				// POST with /models/ path -> use Gemini bridge with fallback handler
				// FallbackHandler will check provider/mapping and proxy if needed
				geminiV1Beta1Handler(c)
				return
			}
		}
		// Non-POST or no local provider available -> proxy upstream
		proxyHandler(c)
	})
}

func (m *AmpModule) tryServeNeoLocalThreadSearchFallback(c *gin.Context, proxy *httputil.ReverseProxy) bool {
	if m == nil || m.neoRuntime == nil || c == nil || c.Request == nil || c.Request.URL == nil || proxy == nil {
		return false
	}
	if c.Request.Method != http.MethodGet || "/"+strings.Trim(c.Request.URL.Path, "/") != "/api/threads/find" {
		return false
	}
	if !neoRuntimeEnabled(m.neoThreadConfigSnapshot()) {
		return false
	}

	searchQuery := c.Request.URL.Query()
	threadSearchProxy := *proxy
	originalModifyResponse := proxy.ModifyResponse
	threadSearchProxy.ModifyResponse = func(resp *http.Response) error {
		if originalModifyResponse != nil {
			if err := originalModifyResponse(resp); err != nil {
				return err
			}
		}
		if resp.StatusCode != http.StatusRequestTimeout || resp.Body == nil {
			return nil
		}
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		if closeErr := resp.Body.Close(); closeErr != nil {
			log.Debugf("amp thread search upstream timeout body close failed: %v", closeErr)
		}
		if neoThreadSearchTimeBudgetExceeded(body) {
			if response, ok := m.neoRuntime.localThreadSearchResponse(searchQuery); ok {
				fallbackBody, err := json.Marshal(response)
				if err != nil {
					return err
				}
				resp.StatusCode = http.StatusOK
				resp.Status = strconv.Itoa(http.StatusOK) + " " + http.StatusText(http.StatusOK)
				resp.Body = io.NopCloser(bytes.NewReader(fallbackBody))
				resp.ContentLength = int64(len(fallbackBody))
				resp.Header.Del("Content-Encoding")
				resp.Header.Set("Content-Length", strconv.FormatInt(resp.ContentLength, 10))
				resp.Header.Set("Content-Type", "application/json")
				return nil
			}
		}
		resp.Body = io.NopCloser(bytes.NewReader(body))
		resp.ContentLength = int64(len(body))
		resp.Header.Set("Content-Length", strconv.FormatInt(resp.ContentLength, 10))
		return nil
	}
	threadSearchProxy.ServeHTTP(c.Writer, c.Request)
	return true
}

func neoThreadSearchTimeBudgetExceeded(body []byte) bool {
	var response struct {
		Code  string `json:"code"`
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return false
	}
	return response.Code == "time-budget-exceeded" || response.Error.Code == "time-budget-exceeded"
}

func (m *AmpModule) tryServeNeoWebLocalInternalRPC(c *gin.Context) bool {
	if !c.GetBool(ampWebLocalInferenceCORSContextKey) {
		return false
	}
	method, params, matched, err := neoWebLocalInternalRPCRequestWithError(c.Request)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_json_body", "message": "invalid JSON body for local internal RPC", "detail": err.Error()})
		return true
	}
	if !matched || m == nil || m.neoRuntime == nil {
		return false
	}
	response, status, ok := m.neoRuntime.neoWebLocalInternalRPCResponse(method, params)
	if !ok {
		return false
	}
	writeNeoJSON(c.Writer, status, response)
	return true
}

func (m *AmpModule) canServeNeoWebLocalInternalRPC(c *gin.Context) bool {
	if c == nil || !c.GetBool(ampWebLocalInferenceCORSContextKey) {
		return false
	}
	method, params, matched, err := neoWebLocalInternalRPCRequestWithError(c.Request)
	if err != nil {
		return matched
	}
	if !matched || m == nil || m.neoRuntime == nil || m.neoRuntime.store == nil {
		return false
	}
	cfg := m.neoThreadConfigSnapshot()
	if cfg == nil || !neoRuntimeEnabled(cfg) {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(method)) {
	case "listthreads":
		return true
	case "loadthreads":
		return len(neoInternalRPCThreadIDs(params)) > 0
	default:
		threadID := neoInternalRPCThreadID(params)
		if !neoThreadIDExactPattern.MatchString(threadID) {
			return false
		}
		actor := m.neoRuntime.store.lookupThreadActor(threadID)
		return actor != nil && actor.hasLocalThreadBootstrapState()
	}
}

func neoWebLocalInternalRPCRequestWithError(r *http.Request) (string, map[string]any, bool, error) {
	if r == nil || r.URL == nil || r.Method != http.MethodPost {
		return "", nil, false, nil
	}
	if strings.TrimSpace(r.Header.Get(ampWebLocalInferenceHeader)) == "" {
		return "", nil, false, nil
	}
	if "/"+strings.Trim(r.URL.Path, "/") != "/api/internal" {
		return "", nil, false, nil
	}
	body, err := readAndRestoreNeoJSONBody(r)
	if err != nil {
		log.WithError(err).Debug("amp web-local internal RPC body decode failed")
		return "", nil, true, err
	}
	method := strings.TrimSpace(stringValue(body["method"]))
	if method == "" {
		method = neoInternalQueryMethod(r.URL.RawQuery)
	}
	if !neoWebLocalInternalRPCSupported(method) {
		return "", nil, false, nil
	}
	return method, mapValue(body["params"]), true, nil
}

func neoWebLocalInternalRPCSupported(method string) bool {
	if neoLocalInternalRPCSupported(method) {
		return true
	}
	switch strings.ToLower(strings.TrimSpace(method)) {
	case "listthreads", "loadthreads", "getthread", "readthread", "getthreadtail", "loadthreadtail", "getthreadmeta":
		return true
	default:
		return false
	}
}

func (m *AmpModule) tryServeNeoLocalInternalRPC(c *gin.Context) bool {
	method, params, matched, err := neoLocalInternalRPCRequestWithError(c.Request)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_json_body", "message": "invalid JSON body for local internal RPC", "detail": err.Error()})
		return true
	}
	if !matched {
		return false
	}
	threadID := neoInternalRPCThreadID(params)
	if !neoThreadIDExactPattern.MatchString(threadID) {
		return false
	}
	actor := m.neoLocalInternalRPCActor(threadID)
	if actor == nil {
		return false
	}
	response, status, ok := actor.neoLocalInternalRPCResponse(method, params)
	if !ok {
		return false
	}
	writeNeoJSON(c.Writer, status, response)
	return true
}

func neoLocalInternalRPCRequestWithError(r *http.Request) (string, map[string]any, bool, error) {
	if r == nil || r.URL == nil || r.Method != http.MethodPost {
		return "", nil, false, nil
	}
	if "/"+strings.Trim(r.URL.Path, "/") != "/api/internal" {
		return "", nil, false, nil
	}
	body, err := readAndRestoreNeoJSONBody(r)
	if err != nil {
		log.WithError(err).Debug("amp local internal RPC body decode failed")
		return "", nil, true, err
	}
	method := strings.TrimSpace(stringValue(body["method"]))
	if method == "" {
		method = neoInternalQueryMethod(r.URL.RawQuery)
	}
	if !neoLocalInternalRPCSupported(method) {
		return "", nil, false, nil
	}
	return method, mapValue(body["params"]), true, nil
}

func neoLocalInternalRPCSupported(method string) bool {
	switch strings.ToLower(strings.TrimSpace(method)) {
	case "getthreadlabels", "setthreadlabels", "addthreadlabels", "archivethread":
		return true
	default:
		return false
	}
}

func neoInternalRPCThreadID(params map[string]any) string {
	return firstNonEmptyString(
		params["thread"], params["threadID"], params["threadId"], params["thread_id"],
		findThreadID(params),
	)
}

func neoInternalRPCThreadIDs(params map[string]any) []string {
	if len(params) == 0 {
		return nil
	}
	rawThreads := firstNonNil(params["threads"], params["threadIDs"], params["threadIds"], params["thread_ids"])
	candidates := stringArrayValue(rawThreads)
	if len(candidates) == 0 {
		if threadID := neoInternalRPCThreadID(params); threadID != "" {
			candidates = []any{threadID}
		}
	}
	out := make([]string, 0, len(candidates))
	seen := map[string]struct{}{}
	for _, rawThreadID := range candidates {
		threadID := strings.TrimSpace(stringValue(rawThreadID))
		if !neoThreadIDExactPattern.MatchString(threadID) {
			continue
		}
		if _, exists := seen[threadID]; exists {
			continue
		}
		seen[threadID] = struct{}{}
		out = append(out, threadID)
	}
	return out
}

func (m *AmpModule) neoLocalInternalRPCActor(threadID string) *neoActor {
	if m == nil || m.neoRuntime == nil || m.neoRuntime.store == nil || !neoThreadIDExactPattern.MatchString(threadID) {
		return nil
	}
	cfg := m.neoThreadConfigSnapshot()
	if !neoRuntimeEnabled(cfg) {
		return nil
	}
	actor := m.neoRuntime.store.lookupThreadActor(threadID)
	if actor == nil || !actor.hasLocalThreadBootstrapState() {
		return nil
	}
	return actor
}

func neoInternalQueryMethod(rawQuery string) string {
	rawQuery = strings.TrimSpace(rawQuery)
	if rawQuery == "" {
		return ""
	}
	if !strings.ContainsAny(rawQuery, "=&") {
		if decoded, err := url.QueryUnescape(rawQuery); err == nil {
			rawQuery = decoded
		}
		return strings.TrimSpace(rawQuery)
	}
	values, err := url.ParseQuery(rawQuery)
	if err != nil {
		return ""
	}
	if method := strings.TrimSpace(values.Get("method")); method != "" {
		return method
	}
	if _, ok := values["getThread"]; ok {
		return "getThread"
	}
	return ""
}

func (m *AmpModule) shouldServeNeoRuntimeBridge(r *http.Request) bool {
	return m != nil && m.neoRuntime != nil && neoRuntimeBridgeRequest(r)
}

func (m *AmpModule) serveNeoRuntimeBridge(c *gin.Context) {
	if m == nil || m.neoRuntime == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "amp neo local runtime not available"})
		return
	}
	clientAPIKey := getClientAPIKeyFromContext(c.Request.Context())
	target := &url.URL{
		Scheme: "http",
		Host:   net.JoinHostPort(m.neoRuntime.host, strconv.Itoa(m.neoRuntime.port)),
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.Director = func(req *http.Request) {
		originalHost := strings.TrimSpace(req.Host)
		originalProto := "http"
		if forwardedProto := strings.TrimSpace(req.Header.Get("X-Forwarded-Proto")); forwardedProto != "" && !neoRequestHostIsLoopback(originalHost) {
			originalProto = strings.Split(forwardedProto, ",")[0]
		} else if req.TLS != nil {
			originalProto = "https"
		}
		req.URL.Scheme = target.Scheme
		req.URL.Host = target.Host
		req.Host = target.Host
		// Newer Amp binaries move the metadata probe and gateway transport under
		// /actors/; rewrite those back to the legacy paths the local engine serves.
		if req.URL != nil {
			if stripped := neoStripActorsRivetPrefix(req.URL.Path); stripped != req.URL.Path {
				req.URL.Path = stripped
				req.URL.RawPath = ""
			}
		}
		if originalHost != "" && strings.TrimSpace(req.Header.Get("X-Forwarded-Host")) == "" {
			req.Header.Set("X-Forwarded-Host", originalHost)
		}
		if strings.TrimSpace(req.Header.Get("X-Forwarded-Proto")) == "" {
			req.Header.Set("X-Forwarded-Proto", originalProto)
		}
		stripNeoRuntimeBridgeCredentials(req)
		if clientAPIKey != "" {
			req.Header.Set(neoInternalClientAPIKeyHeader, clientAPIKey)
		}
	}
	proxy.ErrorHandler = func(rw http.ResponseWriter, req *http.Request, err error) {
		log.WithError(err).Warn("amp neo local runtime bridge failed")
		if !strings.EqualFold(req.Header.Get("Upgrade"), "websocket") {
			rw.Header().Set("Content-Type", "application/json")
		}
		rw.WriteHeader(http.StatusBadGateway)
		_, _ = rw.Write([]byte(`{"error":"amp neo local runtime bridge failed"}`))
	}
	proxy.ServeHTTP(c.Writer, c.Request)
}

func neoThreadIDFromBridgeRequest(r *http.Request) string {
	if r == nil || r.URL == nil {
		return ""
	}
	q := r.URL.Query()
	for _, value := range []string{q.Get("rvt-key"), q.Get("key")} {
		if threadID := neoThreadIDFromGatewayKey(value); threadID != "" {
			return threadID
		}
	}
	if input := decodeNeoGatewayInput(q.Get("rvt-input")); input != nil {
		for _, candidate := range []any{input, mapValue(input)["input"], parseJSONString(mapValue(input)["input"])} {
			if threadID := findThreadID(candidate); neoThreadIDExactPattern.MatchString(threadID) {
				return threadID
			}
		}
	}
	return ""
}

func stripNeoRuntimeBridgeCredentials(req *http.Request) {
	if req == nil {
		return
	}
	req.Header.Del("Authorization")
	req.Header.Del("X-Api-Key")
	req.Header.Del("X-Goog-Api-Key")
	req.Header.Del("X-Rivet-Token")
	req.Header.Del(neoInternalClientAPIKeyHeader)
	if protocols := stripNeoCredentialSubprotocols(req.Header.Get("Sec-WebSocket-Protocol")); protocols != "" {
		req.Header.Set("Sec-WebSocket-Protocol", protocols)
	} else {
		req.Header.Del("Sec-WebSocket-Protocol")
	}
	if req.URL == nil {
		return
	}
	query := req.URL.Query()
	query.Del("auth_token")
	query.Del("access_token")
	query.Del("rvt-token")
	query.Del(ampWebLocalInferenceAPIKeyQuery)
	req.URL.RawQuery = query.Encode()
}

func stripNeoCredentialSubprotocols(header string) string {
	if strings.TrimSpace(header) == "" {
		return ""
	}
	parts := strings.Split(header, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" || strings.HasPrefix(trimmed, "rivet_token.") {
			continue
		}
		out = append(out, trimmed)
	}
	return strings.Join(out, ", ")
}

func requestHasAmpClientHeaders(r *http.Request) bool {
	if r == nil {
		return false
	}
	return strings.TrimSpace(r.Header.Get("X-Amp-Client-Application")) != "" ||
		strings.TrimSpace(r.Header.Get("X-Amp-Client-Type")) != "" ||
		strings.TrimSpace(r.Header.Get("X-Amp-Client-Version")) != ""
}

// registerProviderAliases registers /api/provider/{provider}/... routes
// These allow Amp CLI to route requests like:
//
//	/api/provider/openai/v1/chat/completions
//	/api/provider/anthropic/v1/messages
//	/api/provider/google/v1beta/models
func (m *AmpModule) registerProviderAliases(engine *gin.Engine, baseHandler *handlers.BaseAPIHandler, auth gin.HandlerFunc) {
	// Create handler instances for different providers
	openaiHandlers := openai.NewOpenAIAPIHandler(baseHandler)
	geminiHandlers := gemini.NewGeminiAPIHandler(baseHandler)
	claudeCodeHandlers := claude.NewClaudeCodeAPIHandler(baseHandler)
	openaiResponsesHandlers := openai.NewOpenAIResponsesAPIHandler(baseHandler)

	// Create fallback handler wrapper that forwards to ampcode.com when provider not found
	// Uses m.getProxy() for hot-reload support (proxy can be updated at runtime)
	// Also includes model mapping support for routing unavailable models to alternatives
	fallbackHandler := m.registerFallbackHandler(NewFallbackHandlerWithMapper(func() *httputil.ReverseProxy {
		return m.getProxy()
	}, m.modelMapper, m.forceModelMappings), m.currentCompactionCaptureDir())

	// Provider-specific routes under /api/provider/:provider
	ampProviders := engine.Group("/api/provider")
	if auth != nil {
		ampProviders.Use(auth)
	}
	// Inject client API key into request context for per-client upstream routing
	ampProviders.Use(clientAPIKeyMiddleware())

	provider := ampProviders.Group("/:provider")

	// Dynamic models handler - routes to appropriate provider based on path parameter
	ampModelsHandler := func(c *gin.Context) {
		providerName := strings.ToLower(c.Param("provider"))

		switch providerName {
		case "anthropic":
			claudeCodeHandlers.ClaudeModels(c)
		case "google":
			geminiHandlers.GeminiModels(c)
		default:
			// Default to OpenAI-compatible (works for openai, groq, cerebras, etc.)
			openaiHandlers.OpenAIModels(c)
		}
	}

	// Root-level routes (for providers that omit /v1, like groq/cerebras)
	// Wrap handlers with fallback logic to forward to ampcode.com when provider not found
	provider.GET("/models", ampModelsHandler) // Models endpoint doesn't need fallback (no body to check)
	provider.POST("/chat/completions", fallbackHandler.WrapHandler(openaiHandlers.ChatCompletions))
	provider.POST("/completions", fallbackHandler.WrapHandler(openaiHandlers.Completions))
	provider.POST("/responses", fallbackHandler.WrapHandler(openaiResponsesHandlers.Responses))
	provider.POST("/responses/compact", fallbackHandler.WrapHandler(openaiResponsesHandlers.Compact))
	provider.POST("/images/generations", fallbackHandler.WrapHandler(openaiHandlers.ImagesGenerations))
	provider.POST("/images/edits", fallbackHandler.WrapHandler(openaiHandlers.ImagesEdits))

	// /v1 routes (OpenAI/Claude-compatible endpoints)
	v1Amp := provider.Group("/v1")
	{
		v1Amp.GET("/models", ampModelsHandler) // Models endpoint doesn't need fallback

		// OpenAI-compatible endpoints with fallback
		v1Amp.POST("/chat/completions", fallbackHandler.WrapHandler(openaiHandlers.ChatCompletions))
		v1Amp.POST("/completions", fallbackHandler.WrapHandler(openaiHandlers.Completions))
		v1Amp.POST("/responses", fallbackHandler.WrapHandler(openaiResponsesHandlers.Responses))
		v1Amp.POST("/responses/compact", fallbackHandler.WrapHandler(openaiResponsesHandlers.Compact))
		v1Amp.POST("/images/generations", fallbackHandler.WrapHandler(openaiHandlers.ImagesGenerations))
		v1Amp.POST("/images/edits", fallbackHandler.WrapHandler(openaiHandlers.ImagesEdits))

		// Claude/Anthropic-compatible endpoints with fallback
		v1Amp.POST("/messages", fallbackHandler.WrapHandler(claudeCodeHandlers.ClaudeMessages))
		v1Amp.POST("/messages/count_tokens", fallbackHandler.WrapHandler(claudeCodeHandlers.ClaudeCountTokens))
	}

	// /v1beta routes (Gemini native API)
	// Note: Gemini handler extracts model from URL path, so fallback logic needs special handling
	v1betaAmp := provider.Group("/v1beta")
	{
		v1betaAmp.GET("/models", geminiHandlers.GeminiModels)
		v1betaAmp.POST("/models/*action", fallbackHandler.WrapHandler(withMappedGeminiAction(geminiHandlers.GeminiHandler)))
		v1betaAmp.GET("/models/*action", geminiHandlers.GeminiGetHandler)
	}
}
