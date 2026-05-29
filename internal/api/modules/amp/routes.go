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
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
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
			if m.canServeNeoLocalManagement(c.Request) {
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
		for _, prefix := range prefixes {
			if strings.HasPrefix(path, prefix) && (len(path) == len(prefix) || path[len(prefix)] == '/') {
				c.Next()
				return
			}
		}
		auth(c)
	}
}

// registerManagementRoutes registers Amp management proxy routes
// These routes proxy through to the Amp control plane for OAuth, user management, etc.
// Uses dynamic middleware and proxy getter for hot-reload support.
// The auth middleware validates Authorization header against configured API keys.
func (m *AmpModule) registerManagementRoutes(engine *gin.Engine, baseHandler *handlers.BaseAPIHandler, auth gin.HandlerFunc) {
	ampAPI := engine.Group("/api")

	// Always disable CORS for management routes to prevent browser-based attacks
	ampAPI.Use(m.managementAvailabilityMiddleware(), noCORSMiddleware())

	// Apply dynamic localhost-only restriction (hot-reloadable via m.IsRestrictedToLocalhost())
	ampAPI.Use(m.localhostOnlyMiddleware())

	// Apply authentication middleware - requires valid API key in Authorization header
	var authWithBypass gin.HandlerFunc
	if auth != nil {
		ampAPI.Use(auth)
		authWithBypass = wrapManagementAuth(auth, "/threads", "/auth", "/docs", "/settings")
	}

	// Inject client API key into request context for per-client upstream routing
	ampAPI.Use(clientAPIKeyMiddleware())

	// Dynamic proxy handler that uses m.getProxy() for hot-reload support
	proxyHandler := func(c *gin.Context) {
		if m.tryServeNeoLocalInternal(c) {
			return
		}
		if m.tryServeNeoLocalThreadActor(c) {
			return
		}
		if m.tryServeNeoLocalAttachment(c) {
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

		proxy := m.getProxy()
		if proxy == nil {
			c.JSON(503, gin.H{"error": "amp upstream proxy not available"})
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
	rootMiddleware := []gin.HandlerFunc{m.managementAvailabilityMiddleware(), noCORSMiddleware(), m.localhostOnlyMiddleware()}
	if authWithBypass != nil {
		rootMiddleware = append(rootMiddleware, authWithBypass)
	}
	// Add clientAPIKeyMiddleware after auth for per-client upstream routing
	rootMiddleware = append(rootMiddleware, clientAPIKeyMiddleware())
	engine.GET("/threads", append(rootMiddleware, proxyHandler)...)
	engine.GET("/threads/*path", append(rootMiddleware, proxyHandler)...)
	engine.GET("/docs", append(rootMiddleware, proxyHandler)...)
	engine.GET("/docs/*path", append(rootMiddleware, proxyHandler)...)
	engine.GET("/settings", append(rootMiddleware, proxyHandler)...)
	engine.GET("/settings/*path", append(rootMiddleware, proxyHandler)...)

	engine.GET("/threads.rss", append(rootMiddleware, proxyHandler)...)
	engine.GET("/news.rss", append(rootMiddleware, proxyHandler)...)

	neoRuntimeBridgeHandler := func(c *gin.Context) {
		m.serveNeoRuntimeBridge(c)
	}
	engine.Any("/gateway", append(rootMiddleware, neoRuntimeBridgeHandler)...)
	engine.Any("/gateway/*path", append(rootMiddleware, neoRuntimeBridgeHandler)...)
	engine.Any("/actors", append(rootMiddleware, neoRuntimeBridgeHandler)...)
	engine.Any("/actors/*path", append(rootMiddleware, neoRuntimeBridgeHandler)...)
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

func (m *AmpModule) serveNeoRuntimeBridge(c *gin.Context) {
	if m == nil || m.neoRuntime == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "amp neo local runtime not available"})
		return
	}
	target := &url.URL{
		Scheme: "http",
		Host:   net.JoinHostPort(m.neoRuntime.host, strconv.Itoa(m.neoRuntime.port)),
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.Director = func(req *http.Request) {
		req.URL.Scheme = target.Scheme
		req.URL.Host = target.Host
		req.Host = target.Host
		stripNeoRuntimeBridgeCredentials(req)
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

func stripNeoRuntimeBridgeCredentials(req *http.Request) {
	if req == nil {
		return
	}
	req.Header.Del("Authorization")
	req.Header.Del("X-Api-Key")
	req.Header.Del("X-Goog-Api-Key")
	req.Header.Del("X-Rivet-Token")
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

func (m *AmpModule) tryServeNeoLocalInternal(c *gin.Context) bool {
	if m == nil || c == nil || c.Request == nil || c.Request.URL == nil || m.neoThreadConfigSnapshot() == nil {
		return false
	}
	method := neoLocalInternalMethod(c.Request)
	if method == "" {
		return false
	}
	if m.getProxy() != nil {
		return false
	}
	c.JSON(http.StatusOK, neoLocalInternalResponse(c.Request.Context(), m.neoThreadConfigSnapshot(), c.Request, method))
	return true
}

func requestHasAmpClientHeaders(r *http.Request) bool {
	if r == nil {
		return false
	}
	return strings.TrimSpace(r.Header.Get("X-Amp-Client-Application")) != "" ||
		strings.TrimSpace(r.Header.Get("X-Amp-Client-Type")) != "" ||
		strings.TrimSpace(r.Header.Get("X-Amp-Client-Version")) != ""
}

func neoLocalInternalMethod(r *http.Request) string {
	if r == nil || r.URL == nil {
		return ""
	}
	path := strings.TrimPrefix(r.URL.Path, "/api")
	if "/"+strings.Trim(path, "/") != "/internal" {
		return ""
	}
	query := r.URL.Query()
	for _, method := range neoLocalInternalMethods {
		if _, ok := query[method]; ok {
			return method
		}
	}
	if method := strings.TrimSpace(query.Get("method")); neoLocalInternalMethodSupported(method) {
		return method
	}
	if method := strings.TrimSpace(stringValue(neoLocalInternalPayload(r)["method"])); neoLocalInternalMethodSupported(method) {
		return method
	}
	return ""
}

func neoLocalInternalMethodSupported(method string) bool {
	method = strings.TrimSpace(method)
	for _, supported := range neoLocalInternalMethods {
		if method == supported {
			return true
		}
	}
	return false
}

var neoLocalInternalMethods = []string{
	"loadPlugins",
	"getUserInfo",
}

func neoLocalInternalPayload(r *http.Request) map[string]any {
	if r == nil || r.Body == nil {
		return nil
	}
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		return nil
	}
	r.Body = io.NopCloser(bytes.NewReader(raw))
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil
	}
	return payload
}

func neoLocalInternalResponse(ctx context.Context, cfg *config.Config, r *http.Request, method string) gin.H {
	switch method {
	case "loadPlugins":
		return gin.H{"ok": true, "result": []any{}}
	case "getUserInfo":
		return gin.H{"ok": true, "result": gin.H{
			"id":                neoLocalOwnerUserID,
			"username":          neoLocalOwnerUserID,
			"githubLogin":       neoLocalOwnerUserID,
			"slackUserID":       nil,
			"email":             "local@example.com",
			"firstName":         "Local",
			"lastName":          "User",
			"emailVerified":     true,
			"profilePictureUrl": nil,
			"lastSignInAt":      nil,
			"createdAt":         nil,
			"updatedAt":         nil,
			"siteAdmin":         false,
			"features": []any{
				gin.H{"name": "accept-abuse-data-retention", "enabled": true},
				gin.H{"name": "thread-actors-tui", "enabled": true},
			},
			"name":              "Local User",
			"team":              gin.H{"id": "local-workspace", "name": "Local Workspace"},
			"workspaceID":       "local-workspace",
			"workspaceId":       "local-workspace",
			"mysteriousMessage": nil,
		}}
	default:
		return gin.H{"ok": true, "result": nil}
	}
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

	// /v1 routes (OpenAI/Claude-compatible endpoints)
	v1Amp := provider.Group("/v1")
	{
		v1Amp.GET("/models", ampModelsHandler) // Models endpoint doesn't need fallback

		// OpenAI-compatible endpoints with fallback
		v1Amp.POST("/chat/completions", fallbackHandler.WrapHandler(openaiHandlers.ChatCompletions))
		v1Amp.POST("/completions", fallbackHandler.WrapHandler(openaiHandlers.Completions))
		v1Amp.POST("/responses", fallbackHandler.WrapHandler(openaiResponsesHandlers.Responses))

		// Claude/Anthropic-compatible endpoints with fallback
		v1Amp.POST("/messages", fallbackHandler.WrapHandler(claudeCodeHandlers.ClaudeMessages))
		v1Amp.POST("/messages/count_tokens", fallbackHandler.WrapHandler(claudeCodeHandlers.ClaudeCountTokens))
	}

	// /v1beta routes (Gemini native API)
	// Note: Gemini handler extracts model from URL path, so fallback logic needs special handling
	v1betaAmp := provider.Group("/v1beta")
	{
		v1betaAmp.GET("/models", geminiHandlers.GeminiModels)
		v1betaAmp.POST("/models/*action", fallbackHandler.WrapHandler(geminiHandlers.GeminiHandler))
		v1betaAmp.GET("/models/*action", geminiHandlers.GeminiGetHandler)
	}
}
