package amp

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httputil"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/thinking"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/util"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// AmpRouteType represents the type of routing decision made for an Amp request
type AmpRouteType string

const (
	// RouteTypeLocalProvider indicates the request is handled by a local OAuth provider (free)
	RouteTypeLocalProvider AmpRouteType = "LOCAL_PROVIDER"
	// RouteTypeModelMapping indicates the request was remapped to another available model (free)
	RouteTypeModelMapping AmpRouteType = "MODEL_MAPPING"
	// RouteTypeAmpCredits indicates the request is forwarded to ampcode.com (uses Amp credits)
	RouteTypeAmpCredits AmpRouteType = "AMP_CREDITS"
	// RouteTypeNoProvider indicates no provider or fallback available
	RouteTypeNoProvider AmpRouteType = "NO_PROVIDER"
)

// MappedModelContextKey is the Gin context key for passing mapped model names.
const MappedModelContextKey = "mapped_model"
const localNeoInferenceHeader = "X-CLIProxyAPI-Local-Neo-Inference"

var (
	ampCompactionCaptureSafeNamePattern = regexp.MustCompile(`[^A-Za-z0-9._-]+`)
)

func logAmpIngressRequest(c *gin.Context, rawBody []byte) {
	if !log.IsLevelEnabled(log.DebugLevel) || c == nil || c.Request == nil {
		return
	}
	payload := map[string]any{
		"component":    "amp-ingress",
		"method":       c.Request.Method,
		"path":         c.Request.URL.Path,
		"raw_query":    util.MaskSensitiveQuery(c.Request.URL.RawQuery),
		"headers":      maskedHeaders(c.Request.Header),
		"body_len":     len(rawBody),
		"body":         string(rawBody),
		"action_param": c.Param("action"),
		"path_param":   c.Param("path"),
	}
	if clientModel := extractModelFromRequest(rawBody, c); strings.TrimSpace(clientModel) != "" {
		payload["detected_model"] = clientModel
	}
	if responsesEffort := gjson.GetBytes(rawBody, "reasoning.effort"); responsesEffort.Exists() {
		payload["reasoning.effort"] = responsesEffort.String()
	}
	if chatEffort := gjson.GetBytes(rawBody, "reasoning_effort"); chatEffort.Exists() {
		payload["reasoning_effort"] = chatEffort.String()
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		log.Debug("amp ingress request")
		return
	}
	log.Debugf("amp ingress request %s", encoded)
}

func (fh *FallbackHandler) maybeCaptureAmpCompactionRequest(c *gin.Context, rawBody []byte) {
	dir := fh.ampCompactionCaptureDir()
	if dir == "" || c == nil || c.Request == nil || len(rawBody) == 0 {
		return
	}
	if !looksLikeAmpCompactionRequest(rawBody) {
		return
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		log.WithError(err).Warn("amp compaction capture: failed to create capture directory")
		return
	}
	payload := map[string]any{
		"capturedAt": time.Now().UTC().Format(time.RFC3339Nano),
		"method":     c.Request.Method,
		"path":       c.Request.URL.Path,
		"rawQuery":   util.MaskSensitiveQuery(c.Request.URL.RawQuery),
		"headers":    maskedHeaders(c.Request.Header),
		"bodyLength": len(rawBody),
	}
	if model := extractModelFromRequest(rawBody, c); strings.TrimSpace(model) != "" {
		payload["detectedModel"] = model
	}
	if effort := firstNonEmptyString(gjson.GetBytes(rawBody, "reasoning_effort").String(), gjson.GetBytes(rawBody, "reasoning.effort").String()); effort != "" {
		payload["reasoningEffort"] = effort
	}
	if json.Valid(rawBody) {
		payload["body"] = json.RawMessage(rawBody)
	} else {
		payload["body"] = string(rawBody)
	}
	encoded, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		log.WithError(err).Warn("amp compaction capture: failed to encode capture")
		return
	}
	name := ampCompactionCaptureFilename(stringValue(payload["detectedModel"]))
	path := filepath.Join(dir, name)
	if err = os.WriteFile(path, encoded, 0o600); err != nil {
		log.WithError(err).Warn("amp compaction capture: failed to write capture")
		return
	}
	log.WithField("path", path).Warn("amp compaction capture: wrote candidate provider request")
}

func (fh *FallbackHandler) ampCompactionCaptureDir() string {
	if dir := strings.TrimSpace(os.Getenv("CLIPROXYAPI_AMP_COMPACTION_CAPTURE_DIR")); dir != "" {
		return dir
	}
	if fh == nil {
		return ""
	}
	fh.compactionCaptureDirMu.RLock()
	defer fh.compactionCaptureDirMu.RUnlock()
	return fh.compactionCaptureDir
}

func looksLikeAmpCompactionRequest(rawBody []byte) bool {
	if len(rawBody) == 0 {
		return false
	}
	if stream := gjson.GetBytes(rawBody, "stream"); stream.Exists() && stream.Bool() {
		return false
	}
	if !lastArrayFieldContainsCompactionPrompt(gjson.GetBytes(rawBody, "messages"), "content") && !lastArrayFieldContainsCompactionPrompt(gjson.GetBytes(rawBody, "contents"), "parts") {
		return false
	}
	for _, marker := range [][]byte{
		[]byte("continuation summary"),
		[]byte("Wrap your summary in <summary></summary> tags."),
		[]byte("You have been working on the task described above"),
		[]byte("Context to Preserve"),
	} {
		if bytes.Contains(rawBody, marker) {
			return true
		}
	}
	return false
}

func lastArrayFieldContainsCompactionPrompt(array gjson.Result, field string) bool {
	items := array.Array()
	if len(items) == 0 {
		return false
	}
	return messageContainsCompactionPrompt(items[len(items)-1].Get(field))
}

func messageContainsCompactionPrompt(value gjson.Result) bool {
	if !value.Exists() {
		return false
	}
	if strings.Contains(value.String(), "Wrap your summary in <summary></summary> tags.") {
		return true
	}
	return strings.Contains(value.String(), "You have been working on the task described above") && strings.Contains(value.String(), "Context to Preserve")
}

func ampCompactionCaptureFilename(model string) string {
	stamp := time.Now().UTC().Format("20060102T150405.000000000Z")
	model = strings.TrimSpace(model)
	if model == "" {
		model = "unknown-model"
	}
	model = ampCompactionCaptureSafeNamePattern.ReplaceAllString(model, "_")
	model = strings.Trim(model, "._-")
	if model == "" {
		model = "unknown-model"
	}
	return "amp-compaction-" + stamp + "-" + model + "-" + randomCaptureSuffix() + ".json"
}

func randomCaptureSuffix() string {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "nosuffix"
	}
	return hex.EncodeToString(b[:])
}

func maskedHeaders(headers http.Header) map[string][]string {
	if len(headers) == 0 {
		return nil
	}
	masked := make(map[string][]string, len(headers))
	for key, values := range headers {
		if len(values) == 0 {
			masked[key] = nil
			continue
		}
		maskedValues := make([]string, 0, len(values))
		for _, value := range values {
			maskedValues = append(maskedValues, util.MaskSensitiveHeaderValue(key, value))
		}
		masked[key] = maskedValues
	}
	return masked
}

// logAmpRouting logs the routing decision for an Amp request with structured fields
func logAmpRouting(routeType AmpRouteType, requestedModel, resolvedModel, provider, path string) {
	fields := log.Fields{
		"component":       "amp-routing",
		"route_type":      string(routeType),
		"requested_model": requestedModel,
		"path":            path,
		"timestamp":       time.Now().Format(time.RFC3339),
	}

	if resolvedModel != "" && resolvedModel != requestedModel {
		fields["resolved_model"] = resolvedModel
	}
	if provider != "" {
		fields["provider"] = provider
	}

	switch routeType {
	case RouteTypeLocalProvider:
		fields["cost"] = "free"
		fields["source"] = "local_oauth"
		log.WithFields(fields).Debugf("amp using local provider for model: %s", requestedModel)

	case RouteTypeModelMapping:
		fields["cost"] = "free"
		fields["source"] = "local_oauth"
		fields["mapping"] = requestedModel + " -> " + resolvedModel
		// model mapping already logged in mapper; avoid duplicate here

	case RouteTypeAmpCredits:
		fields["cost"] = "amp_credits"
		fields["source"] = "ampcode.com"
		fields["model_id"] = requestedModel // Explicit model_id for easy config reference
		log.WithFields(fields).Warnf("forwarding to ampcode.com (uses amp credits) - model_id: %s | To use local provider, add to config: ampcode.model-mappings: [{from: \"%s\", to: \"<your-local-model>\"}]", requestedModel, requestedModel)

	case RouteTypeNoProvider:
		fields["cost"] = "none"
		fields["source"] = "error"
		fields["model_id"] = requestedModel // Explicit model_id for easy config reference
		log.WithFields(fields).Warnf("no provider available for model_id: %s", requestedModel)
	}
}

// FallbackHandler wraps a standard handler with fallback logic to ampcode.com
// when the model's provider is not available in CLIProxyAPI
type FallbackHandler struct {
	getProxy               func() *httputil.ReverseProxy
	modelMapper            ModelMapper
	forceModelMappings     func() bool
	compactionCaptureDirMu sync.RWMutex
	compactionCaptureDir   string
}

// NewFallbackHandler creates a new fallback handler wrapper
// The getProxy function allows lazy evaluation of the proxy (useful when proxy is created after routes)
func NewFallbackHandler(getProxy func() *httputil.ReverseProxy) *FallbackHandler {
	return &FallbackHandler{
		getProxy:           getProxy,
		forceModelMappings: func() bool { return false },
	}
}

// NewFallbackHandlerWithMapper creates a new fallback handler with model mapping support
func NewFallbackHandlerWithMapper(getProxy func() *httputil.ReverseProxy, mapper ModelMapper, forceModelMappings func() bool) *FallbackHandler {
	if forceModelMappings == nil {
		forceModelMappings = func() bool { return false }
	}
	return &FallbackHandler{
		getProxy:           getProxy,
		modelMapper:        mapper,
		forceModelMappings: forceModelMappings,
	}
}

// SetModelMapper sets the model mapper for this handler (allows late binding)
func (fh *FallbackHandler) SetModelMapper(mapper ModelMapper) {
	fh.modelMapper = mapper
}

// SetCompactionCaptureDir updates the optional directory used to capture Amp
// compaction provider requests for parity debugging.
func (fh *FallbackHandler) SetCompactionCaptureDir(dir string) {
	if fh == nil {
		return
	}
	fh.compactionCaptureDirMu.Lock()
	fh.compactionCaptureDir = strings.TrimSpace(dir)
	fh.compactionCaptureDirMu.Unlock()
}

// WrapHandler wraps a gin.HandlerFunc with fallback logic
// If the model's provider is not configured in CLIProxyAPI, it forwards to ampcode.com
func (fh *FallbackHandler) WrapHandler(handler gin.HandlerFunc) gin.HandlerFunc {
	return func(c *gin.Context) {
		requestPath := c.Request.URL.Path

		// Read the request body to extract the model name
		bodyBytes, err := io.ReadAll(c.Request.Body)
		if err != nil {
			log.Errorf("amp fallback: failed to read request body: %v", err)
			handler(c)
			return
		}

		rawBodyBytes := bodyBytes
		logAmpIngressRequest(c, rawBodyBytes)
		fh.maybeCaptureAmpCompactionRequest(c, rawBodyBytes)
		rawModelName := extractModelFromRequest(rawBodyBytes, c)
		rawResponsesEffort := gjson.GetBytes(rawBodyBytes, "reasoning.effort")
		rawChatEffort := gjson.GetBytes(rawBodyBytes, "reasoning_effort")
		if rawModelName != "" || rawResponsesEffort.Exists() || rawChatEffort.Exists() {
			fields := log.Fields{
				"path": requestPath,
			}
			if rawModelName != "" {
				fields["model"] = rawModelName
			}
			if rawResponsesEffort.Exists() {
				fields["reasoning.effort"] = rawResponsesEffort.String()
			}
			if rawChatEffort.Exists() {
				fields["reasoning_effort"] = rawChatEffort.String()
			}
			log.WithFields(fields).Debug("amp raw request thinking config")
		}

		// Sanitize request body: remove thinking blocks with invalid signatures
		// to prevent upstream API 400 errors
		bodyBytes = SanitizeAmpRequestBody(bodyBytes)
		proxyBodyBytes := append([]byte(nil), bodyBytes...)

		// Restore the body for the handler to read
		c.Request.Body = io.NopCloser(bytes.NewReader(bodyBytes))

		// Try to extract model from request body or URL path (for Gemini)
		modelName := extractModelFromRequest(bodyBytes, c)
		if modelName == "" {
			// Can't determine model, proceed with normal handler
			handler(c)
			return
		}

		// Normalize model (handles dynamic thinking suffixes)
		suffixResult := thinking.ParseSuffix(modelName)
		normalizedModel := suffixResult.ModelName
		thinkingSuffix := ""
		if suffixResult.HasSuffix {
			thinkingSuffix = "(" + suffixResult.RawSuffix + ")"
		}

		resolveMappedModel := func() (string, []string) {
			if fh.modelMapper == nil {
				return "", nil
			}

			mappedModel := fh.modelMapper.MapModel(modelName)
			if mappedModel == "" {
				mappedModel = fh.modelMapper.MapModel(normalizedModel)
			}
			mappedModel = strings.TrimSpace(mappedModel)
			if mappedModel == "" {
				return "", nil
			}

			// Preserve dynamic thinking suffix (e.g. "(xhigh)") when mapping applies, unless the target
			// already specifies its own thinking suffix.
			if thinkingSuffix != "" {
				mappedSuffixResult := thinking.ParseSuffix(mappedModel)
				if !mappedSuffixResult.HasSuffix {
					mappedModel += thinkingSuffix
				}
			}

			mappedBaseModel := thinking.ParseSuffix(mappedModel).ModelName
			mappedProviders := util.GetProviderName(mappedBaseModel)
			if len(mappedProviders) == 0 {
				return "", nil
			}

			return mappedModel, mappedProviders
		}

		// Track resolved model for logging (may change if mapping is applied)
		resolvedModel := normalizedModel
		usedMapping := false
		var providers []string

		// Check if model mappings should be forced ahead of local API keys
		forceMappings := fh.forceModelMappings != nil && fh.forceModelMappings()

		if forceMappings {
			// FORCE MODE: Check model mappings FIRST (takes precedence over local API keys)
			// This allows users to route Amp requests to their preferred OAuth providers
			if mappedModel, mappedProviders := resolveMappedModel(); mappedModel != "" {
				// Mapping found and provider available - rewrite the model in request body
				bodyBytes = rewriteModelInRequest(bodyBytes, mappedModel)
				c.Request.Body = io.NopCloser(bytes.NewReader(bodyBytes))
				// Store mapped model in context for handlers that check it (like gemini bridge)
				c.Set(MappedModelContextKey, mappedModel)
				resolvedModel = mappedModel
				usedMapping = true
				providers = mappedProviders
			}

			// If no mapping applied, check for local providers
			if !usedMapping {
				providers = util.GetProviderName(normalizedModel)
			}
		} else {
			// DEFAULT MODE: Check local providers first, then mappings as fallback
			providers = util.GetProviderName(normalizedModel)

			if len(providers) == 0 {
				// No providers configured - check if we have a model mapping
				if mappedModel, mappedProviders := resolveMappedModel(); mappedModel != "" {
					// Mapping found and provider available - rewrite the model in request body
					bodyBytes = rewriteModelInRequest(bodyBytes, mappedModel)
					c.Request.Body = io.NopCloser(bytes.NewReader(bodyBytes))
					// Store mapped model in context for handlers that check it (like gemini bridge)
					c.Set(MappedModelContextKey, mappedModel)
					resolvedModel = mappedModel
					usedMapping = true
					providers = mappedProviders
				}
			}
		}

		proxyToAmp := func() bool {
			proxy := fh.getProxy()
			if proxy == nil {
				return false
			}

			// Log: Forwarding to ampcode.com (uses Amp credits)
			logAmpRouting(RouteTypeAmpCredits, modelName, "", "", requestPath)

			// Restore body for the proxy path.
			c.Request.Body = io.NopCloser(bytes.NewReader(proxyBodyBytes))

			// Forward to ampcode.com
			proxy.ServeHTTP(c.Writer, c.Request)
			return true
		}

		// If this request came from the local Neo runtime, keep inference local.
		// Normal Amp binary provider requests can still fall back to ampcode.com.
		if len(providers) == 0 {
			if c.GetHeader(localNeoInferenceHeader) == "1" {
				logAmpRouting(RouteTypeNoProvider, modelName, "", "", requestPath)
				c.AbortWithStatusJSON(http.StatusBadGateway, gin.H{
					"error":   "local_neo_provider_unavailable",
					"message": "Amp Neo local inference has no local provider for requested model",
					"model":   modelName,
				})
				return
			}
			if proxyToAmp() {
				return
			}

			// No proxy available, let the normal handler return the error
			logAmpRouting(RouteTypeNoProvider, modelName, "", "", requestPath)
		}

		// Log the routing decision
		providerName := ""
		if len(providers) > 0 {
			providerName = providers[0]
		}

		if usedMapping {
			// Log: Model was mapped to another model
			log.Debugf("amp model mapping: request %s -> %s", normalizedModel, resolvedModel)
			logAmpRouting(RouteTypeModelMapping, modelName, resolvedModel, providerName, requestPath)
			rewriter := NewResponseRewriter(c.Writer, modelName)
			rewriter.suppressThinking = true
			c.Writer = rewriter
			// Filter Anthropic-Beta header only for local handling paths
			filterAntropicBetaHeader(c)
			c.Request.Body = io.NopCloser(bytes.NewReader(bodyBytes))
			handler(c)
			rewriter.Flush()
			log.Debugf("amp model mapping: response %s -> %s", resolvedModel, modelName)
		} else if len(providers) > 0 {
			// Log: Using local provider (free)
			logAmpRouting(RouteTypeLocalProvider, modelName, resolvedModel, providerName, requestPath)
			// Wrap with ResponseRewriter for local providers too, because upstream
			// proxies (e.g. NewAPI) may return a different model name and lack
			// Amp-required fields like thinking.signature.
			rewriter := NewResponseRewriter(c.Writer, modelName)
			rewriter.suppressThinking = providerName != "claude"
			c.Writer = rewriter
			// Filter Anthropic-Beta header only for local handling paths
			filterAntropicBetaHeader(c)
			c.Request.Body = io.NopCloser(bytes.NewReader(bodyBytes))
			handler(c)
			rewriter.Flush()
		} else {
			// No provider, no mapping, no proxy: fall back to the wrapped handler so it can return an error response
			c.Request.Body = io.NopCloser(bytes.NewReader(bodyBytes))
			handler(c)
		}
	}
}

// filterAntropicBetaHeader filters Anthropic-Beta header to remove features requiring special subscription
// This is needed when using local providers (bypassing the Amp proxy)
func filterAntropicBetaHeader(c *gin.Context) {
	if betaHeader := c.Request.Header.Get("Anthropic-Beta"); betaHeader != "" {
		if filtered := filterBetaFeatures(betaHeader, "context-1m-2025-08-07"); filtered != "" {
			c.Request.Header.Set("Anthropic-Beta", filtered)
		} else {
			c.Request.Header.Del("Anthropic-Beta")
		}
	}
}

// rewriteModelInRequest replaces the model name in a JSON request body
func rewriteModelInRequest(body []byte, newModel string) []byte {
	if !gjson.GetBytes(body, "model").Exists() {
		return body
	}
	result, err := sjson.SetBytes(body, "model", newModel)
	if err != nil {
		log.Warnf("amp model mapping: failed to rewrite model in request body: %v", err)
		return body
	}
	return result
}

// extractModelFromRequest attempts to extract the model name from various request formats
func extractModelFromRequest(body []byte, c *gin.Context) string {
	// First try to parse from JSON body (OpenAI, Claude, etc.)
	// Check common model field names
	if result := gjson.GetBytes(body, "model"); result.Exists() && result.Type == gjson.String {
		return result.String()
	}
	if model := extractMultipartModelFromRequest(body, c); model != "" {
		return model
	}

	// For Gemini requests, model is in the URL path
	// Standard format: /models/{model}:generateContent -> :action parameter
	if action := c.Param("action"); action != "" {
		// Split by colon to get model name (e.g., "gemini-pro:generateContent" -> "gemini-pro")
		parts := strings.Split(action, ":")
		if len(parts) > 0 && parts[0] != "" {
			return parts[0]
		}
	}

	// AMP CLI format: /publishers/google/models/{model}:method -> *path parameter
	// Example: /publishers/google/models/gemini-3-pro-preview:streamGenerateContent
	if path := c.Param("path"); path != "" {
		// Look for /models/{model}:method pattern
		if idx := strings.Index(path, "/models/"); idx >= 0 {
			modelPart := path[idx+8:] // Skip "/models/"
			// Split by colon to get model name
			if colonIdx := strings.Index(modelPart, ":"); colonIdx > 0 {
				return modelPart[:colonIdx]
			}
		}
	}

	return ""
}

func extractMultipartModelFromRequest(body []byte, c *gin.Context) string {
	if c == nil || c.Request == nil {
		return ""
	}
	mediaType, params, err := mime.ParseMediaType(c.GetHeader("Content-Type"))
	if err != nil || !strings.HasPrefix(strings.ToLower(mediaType), "multipart/") {
		return ""
	}
	boundary := strings.TrimSpace(params["boundary"])
	if boundary == "" {
		return ""
	}
	reader := multipart.NewReader(bytes.NewReader(body), boundary)
	for {
		part, err := reader.NextPart()
		if errors.Is(err, io.EOF) {
			return ""
		}
		if err != nil {
			return ""
		}
		if part.FormName() != "model" {
			_ = part.Close()
			continue
		}
		raw, err := io.ReadAll(io.LimitReader(part, 4096))
		_ = part.Close()
		if err != nil {
			return ""
		}
		return strings.TrimSpace(string(raw))
	}
}
