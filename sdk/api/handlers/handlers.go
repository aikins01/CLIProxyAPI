// Package handlers provides core API handler functionality for the CLI Proxy API server.
// It includes common types, client management, load balancing, and error handling
// shared across all API endpoint handlers (OpenAI, Claude, Gemini).
package handlers

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/thinking"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/util"
	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/config"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"golang.org/x/net/context"
)

// ErrorResponse represents a standard error response format for the API.
// It contains a single ErrorDetail field.
type ErrorResponse struct {
	// Error contains detailed information about the error that occurred.
	Error ErrorDetail `json:"error"`
}

// ErrorDetail provides specific information about an error that occurred.
// It includes a human-readable message, an error type, and an optional error code.
type ErrorDetail struct {
	// Message is a human-readable message providing more details about the error.
	Message string `json:"message"`

	// Type is the category of error that occurred (e.g., "invalid_request_error").
	Type string `json:"type"`

	// Code is a short code identifying the error, if applicable.
	Code string `json:"code,omitempty"`
}

type StreamMeta struct {
	mu                   sync.RWMutex
	commitOnce           sync.Once
	committed            chan struct{}
	keepAliveInterval    time.Duration
	hasKeepAliveInterval bool
	bootstrapCommitted   bool
}

func newStreamMeta(keepAliveInterval *time.Duration, bootstrapCommitted bool) *StreamMeta {
	meta := &StreamMeta{committed: make(chan struct{})}
	meta.set(keepAliveInterval, bootstrapCommitted)
	return meta
}

func (m *StreamMeta) set(keepAliveInterval *time.Duration, bootstrapCommitted bool) {
	if m == nil {
		return
	}
	m.mu.Lock()
	if keepAliveInterval == nil {
		m.keepAliveInterval = 0
		m.hasKeepAliveInterval = false
	} else {
		m.keepAliveInterval = *keepAliveInterval
		m.hasKeepAliveInterval = true
	}
	publishCommitted := bootstrapCommitted && !m.bootstrapCommitted
	m.bootstrapCommitted = m.bootstrapCommitted || bootstrapCommitted
	m.mu.Unlock()
	if publishCommitted {
		m.commitOnce.Do(func() { close(m.committed) })
	}
}

func (m *StreamMeta) KeepAliveInterval() *time.Duration {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if !m.hasKeepAliveInterval {
		return nil
	}
	interval := m.keepAliveInterval
	return &interval
}

func (m *StreamMeta) BootstrapCommitted() bool {
	if m == nil {
		return false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.bootstrapCommitted
}

func (m *StreamMeta) BootstrapCommittedSignal() <-chan struct{} {
	if m == nil {
		return nil
	}
	return m.committed
}

const idempotencyKeyMetadataKey = "idempotency_key"

const (
	defaultStreamingKeepAliveSeconds = 0
	defaultStreamingBootstrapRetries = 0
)

type pinnedAuthContextKey struct{}
type selectedAuthCallbackContextKey struct{}
type executionSessionContextKey struct{}
type disallowFreeAuthContextKey struct{}

// WithPinnedAuthID returns a child context that requests execution on a specific auth ID.
func WithPinnedAuthID(ctx context.Context, authID string) context.Context {
	authID = strings.TrimSpace(authID)
	if authID == "" {
		return ctx
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, pinnedAuthContextKey{}, authID)
}

// WithSelectedAuthIDCallback returns a child context that receives the selected auth ID.
func WithSelectedAuthIDCallback(ctx context.Context, callback func(string)) context.Context {
	if callback == nil {
		return ctx
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, selectedAuthCallbackContextKey{}, callback)
}

// WithExecutionSessionID returns a child context tagged with a long-lived execution session ID.
func WithExecutionSessionID(ctx context.Context, sessionID string) context.Context {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return ctx
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, executionSessionContextKey{}, sessionID)
}

// WithDisallowFreeAuth returns a child context that requests skipping known free-tier credentials.
func WithDisallowFreeAuth(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, disallowFreeAuthContextKey{}, true)
}

// BuildErrorResponseBody builds an OpenAI-compatible JSON error response body.
// If errText is already valid JSON, it is returned as-is to preserve upstream error payloads.
func BuildErrorResponseBody(status int, errText string) []byte {
	if status <= 0 {
		status = http.StatusInternalServerError
	}
	if strings.TrimSpace(errText) == "" {
		errText = http.StatusText(status)
	}

	trimmed := strings.TrimSpace(errText)
	if trimmed != "" && json.Valid([]byte(trimmed)) {
		return []byte(trimmed)
	}

	errType := "invalid_request_error"
	var code string
	switch status {
	case http.StatusUnauthorized:
		errType = "authentication_error"
		code = "invalid_api_key"
	case http.StatusForbidden:
		errType = "permission_error"
		code = "insufficient_quota"
	case http.StatusTooManyRequests:
		errType = "rate_limit_error"
		code = "rate_limit_exceeded"
	case http.StatusNotFound:
		errType = "invalid_request_error"
		code = "model_not_found"
	default:
		if status >= http.StatusInternalServerError {
			errType = "server_error"
			code = "internal_server_error"
		}
	}

	payload, err := json.Marshal(ErrorResponse{
		Error: ErrorDetail{
			Message: errText,
			Type:    errType,
			Code:    code,
		},
	})
	if err != nil {
		return []byte(fmt.Sprintf(`{"error":{"message":%q,"type":"server_error","code":"internal_server_error"}}`, errText))
	}
	return payload
}

func statusCodeFromError(err error) int {
	if se, ok := errors.AsType[coreexecutor.StatusError](err); ok && se != nil {
		if code := se.StatusCode(); code > 0 {
			return code
		}
	}
	return http.StatusInternalServerError
}

func headersFromError(err error) http.Header {
	if err == nil {
		return nil
	}
	var provider interface{ Headers() http.Header }
	if !errors.As(err, &provider) || provider == nil {
		return nil
	}
	headers := provider.Headers()
	if headers == nil {
		return nil
	}
	return headers.Clone()
}

// StreamingKeepAliveInterval returns the SSE keep-alive interval for this server.
// Returning 0 disables keep-alives (default when unset).
func StreamingKeepAliveInterval(cfg *config.SDKConfig) time.Duration {
	seconds := defaultStreamingKeepAliveSeconds
	if cfg != nil {
		seconds = cfg.Streaming.KeepAliveSeconds
	}
	if seconds <= 0 {
		return 0
	}
	return time.Duration(seconds) * time.Second
}

// NonStreamingKeepAliveInterval returns the keep-alive interval for non-streaming responses.
// Returning 0 disables keep-alives (default when unset).
func NonStreamingKeepAliveInterval(cfg *config.SDKConfig) time.Duration {
	seconds := 0
	if cfg != nil {
		seconds = cfg.NonStreamKeepAliveInterval
	}
	if seconds <= 0 {
		return 0
	}
	return time.Duration(seconds) * time.Second
}

// StreamingBootstrapRetries returns how many times a streaming request may be retried before any bytes are sent.
func StreamingBootstrapRetries(cfg *config.SDKConfig) int {
	retries := defaultStreamingBootstrapRetries
	if cfg != nil {
		retries = cfg.Streaming.BootstrapRetries
	}
	if retries < 0 {
		retries = 0
	}
	return retries
}

// PassthroughHeadersEnabled returns whether upstream response headers should be forwarded to clients.
// Default is false.
func PassthroughHeadersEnabled(cfg *config.SDKConfig) bool {
	return cfg != nil && cfg.PassthroughHeaders
}

func requestExecutionMetadata(ctx context.Context) map[string]any {
	// Idempotency-Key is an optional client-supplied header used to correlate retries.
	// Only include it if the client explicitly provides it.
	key := ""
	requestPath := ""
	if ctx != nil {
		if ginCtx, ok := ctx.Value("gin").(*gin.Context); ok && ginCtx != nil && ginCtx.Request != nil {
			key = strings.TrimSpace(ginCtx.GetHeader("Idempotency-Key"))
			requestPath = strings.TrimSpace(ginCtx.FullPath())
			if requestPath == "" && ginCtx.Request.URL != nil {
				requestPath = strings.TrimSpace(ginCtx.Request.URL.Path)
			}
		}
	}

	meta := make(map[string]any)
	if key != "" {
		meta[idempotencyKeyMetadataKey] = key
	}
	if requestPath != "" {
		meta[coreexecutor.RequestPathMetadataKey] = requestPath
	}
	if pinnedAuthID := pinnedAuthIDFromContext(ctx); pinnedAuthID != "" {
		meta[coreexecutor.PinnedAuthMetadataKey] = pinnedAuthID
	}
	if selectedCallback := selectedAuthIDCallbackFromContext(ctx); selectedCallback != nil {
		meta[coreexecutor.SelectedAuthCallbackMetadataKey] = selectedCallback
	}
	if executionSessionID := executionSessionIDFromContext(ctx); executionSessionID != "" {
		meta[coreexecutor.ExecutionSessionMetadataKey] = executionSessionID
	}
	if disallowFreeAuthFromContext(ctx) {
		meta[coreexecutor.DisallowFreeAuthMetadataKey] = true
	}
	return meta
}

// headersFromContext extracts the original HTTP request headers from the gin context
// embedded in the provided context. This allows session affinity selectors to read
// client headers like X-Amp-Thread-Id.
func headersFromContext(ctx context.Context) http.Header {
	if ctx == nil {
		return nil
	}
	if ginCtx, ok := ctx.Value("gin").(*gin.Context); ok && ginCtx != nil && ginCtx.Request != nil {
		return ginCtx.Request.Header.Clone()
	}
	return nil
}

func pinnedAuthIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	raw := ctx.Value(pinnedAuthContextKey{})
	switch v := raw.(type) {
	case string:
		return strings.TrimSpace(v)
	case []byte:
		return strings.TrimSpace(string(v))
	default:
		return ""
	}
}

func selectedAuthIDCallbackFromContext(ctx context.Context) func(string) {
	if ctx == nil {
		return nil
	}
	raw := ctx.Value(selectedAuthCallbackContextKey{})
	if callback, ok := raw.(func(string)); ok && callback != nil {
		return callback
	}
	return nil
}

func executionSessionIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	raw := ctx.Value(executionSessionContextKey{})
	switch v := raw.(type) {
	case string:
		return strings.TrimSpace(v)
	case []byte:
		return strings.TrimSpace(string(v))
	default:
		return ""
	}
}

func disallowFreeAuthFromContext(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	raw, ok := ctx.Value(disallowFreeAuthContextKey{}).(bool)
	return ok && raw
}

// BaseAPIHandler contains the handlers for API endpoints.
// It holds a pool of clients to interact with the backend service and manages
// load balancing, client selection, and configuration.
type BaseAPIHandler struct {
	// AuthManager manages auth lifecycle and execution in the new architecture.
	AuthManager *coreauth.Manager

	// Cfg holds the current application configuration.
	Cfg *config.SDKConfig
}

// NewBaseAPIHandlers creates a new API handlers instance.
// It takes a slice of clients and configuration as input.
//
// Parameters:
//   - cliClients: A slice of AI service clients
//   - cfg: The application configuration
//
// Returns:
//   - *BaseAPIHandler: A new API handlers instance
func NewBaseAPIHandlers(cfg *config.SDKConfig, authManager *coreauth.Manager) *BaseAPIHandler {
	return &BaseAPIHandler{
		Cfg:         cfg,
		AuthManager: authManager,
	}
}

// UpdateClients updates the handlers' client list and configuration.
// This method is called when the configuration or authentication tokens change.
//
// Parameters:
//   - clients: The new slice of AI service clients
//   - cfg: The new application configuration
func (h *BaseAPIHandler) UpdateClients(cfg *config.SDKConfig) { h.Cfg = cfg }

// GetAlt extracts the 'alt' parameter from the request query string.
// It checks both 'alt' and '$alt' parameters and returns the appropriate value.
//
// Parameters:
//   - c: The Gin context containing the HTTP request
//
// Returns:
//   - string: The alt parameter value, or empty string if it's "sse"
func (h *BaseAPIHandler) GetAlt(c *gin.Context) string {
	var alt string
	var hasAlt bool
	alt, hasAlt = c.GetQuery("alt")
	if !hasAlt {
		alt, _ = c.GetQuery("$alt")
	}
	if alt == "sse" {
		return ""
	}
	return alt
}

// GetContextWithCancel creates a new context with cancellation capabilities.
// It embeds the Gin context and the API handler into the new context for later use.
// The returned cancel function also handles logging the API response if request logging is enabled.
//
// Parameters:
//   - handler: The API handler associated with the request.
//   - c: The Gin context of the current request.
//   - ctx: The parent context (caller values/deadlines are preserved; request context adds cancellation and request ID).
//
// Returns:
//   - context.Context: The new context with cancellation and embedded values.
//   - APIHandlerCancelFunc: A function to cancel the context and log the response.
func (h *BaseAPIHandler) GetContextWithCancel(handler interfaces.APIHandler, c *gin.Context, ctx context.Context) (context.Context, APIHandlerCancelFunc) {
	parentCtx := ctx
	if parentCtx == nil {
		parentCtx = context.Background()
	}

	var requestCtx context.Context
	if c != nil && c.Request != nil {
		requestCtx = c.Request.Context()
	}

	if requestCtx != nil && logging.GetRequestID(parentCtx) == "" {
		if requestID := logging.GetRequestID(requestCtx); requestID != "" {
			parentCtx = logging.WithRequestID(parentCtx, requestID)
		} else if requestID = logging.GetGinRequestID(c); requestID != "" {
			parentCtx = logging.WithRequestID(parentCtx, requestID)
		}
	}
	if util.IsTrustedLocalNeoInference(requestCtx) {
		parentCtx = util.WithTrustedLocalNeoInference(parentCtx)
	}
	newCtx, cancel := context.WithCancel(parentCtx)

	endpoint := ""
	if c != nil && c.Request != nil {
		path := strings.TrimSpace(c.FullPath())
		if path == "" && c.Request.URL != nil {
			path = strings.TrimSpace(c.Request.URL.Path)
		}
		if path != "" {
			method := strings.TrimSpace(c.Request.Method)
			if method != "" {
				endpoint = method + " " + path
			} else {
				endpoint = path
			}
		}
	}
	if endpoint != "" {
		newCtx = logging.WithEndpoint(newCtx, endpoint)
	}
	newCtx = logging.WithResponseStatusHolder(newCtx)

	cancelCtx := newCtx
	if requestCtx != nil && requestCtx != parentCtx {
		go func() {
			select {
			case <-requestCtx.Done():
				cancel()
			case <-cancelCtx.Done():
			}
		}()
	}
	newCtx = context.WithValue(newCtx, "gin", c)
	newCtx = context.WithValue(newCtx, "handler", handler)
	return newCtx, func(params ...interface{}) {
		if c != nil {
			logging.SetResponseStatus(cancelCtx, c.Writer.Status())
		}
		if h.Cfg.RequestLog && len(params) == 1 {
			if existing, exists := c.Get("API_RESPONSE"); exists {
				if existingBytes, ok := existing.([]byte); ok && len(bytes.TrimSpace(existingBytes)) > 0 {
					switch params[0].(type) {
					case error, string:
						cancel()
						return
					}
				}
			}

			var payload []byte
			switch data := params[0].(type) {
			case []byte:
				payload = data
			case error:
				if data != nil {
					payload = []byte(data.Error())
				}
			case string:
				payload = []byte(data)
			}
			if len(payload) > 0 {
				if existing, exists := c.Get("API_RESPONSE"); exists {
					if existingBytes, ok := existing.([]byte); ok && len(existingBytes) > 0 {
						trimmedPayload := bytes.TrimSpace(payload)
						if len(trimmedPayload) > 0 && bytes.Contains(existingBytes, trimmedPayload) {
							cancel()
							return
						}
					}
				}
				appendAPIResponse(c, payload)
			}
		}

		cancel()
	}
}

// StartNonStreamingKeepAlive emits blank lines while waiting for a non-streaming response.
// It returns a stop function that must be called before writing the final response.
func (h *BaseAPIHandler) StartNonStreamingKeepAlive(c *gin.Context, ctx context.Context) func() {
	if h == nil || c == nil {
		return func() {}
	}
	if isAmpStrictJSONRequest(c) {
		return func() {}
	}
	interval := NonStreamingKeepAliveInterval(h.Cfg)
	if interval <= 0 {
		return func() {}
	}
	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		return func() {}
	}
	if ctx == nil {
		ctx = context.Background()
	}

	stopChan := make(chan struct{})
	var stopOnce sync.Once
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stopChan:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				_, _ = c.Writer.Write([]byte("\n"))
				flusher.Flush()
			}
		}
	}()

	return func() {
		stopOnce.Do(func() {
			close(stopChan)
		})
		wg.Wait()
	}
}

func isAmpStrictJSONRequest(c *gin.Context) bool {
	if c == nil || c.Request == nil {
		return false
	}
	headers := c.Request.Header
	if util.IsTrustedLocalNeoInference(c.Request.Context()) {
		return true
	}
	if strings.TrimSpace(headers.Get("X-Amp-Client-Application")) != "" ||
		strings.TrimSpace(headers.Get("X-Amp-Client-Type")) != "" ||
		strings.TrimSpace(headers.Get("X-Amp-Client-Version")) != "" {
		return true
	}
	feature := strings.ToLower(strings.TrimSpace(headers.Get("X-Amp-Feature")))
	return strings.HasPrefix(feature, "amp.")
}

// appendAPIResponse preserves any previously captured API response and appends new data.
func appendAPIResponse(c *gin.Context, data []byte) {
	if c == nil || len(data) == 0 {
		return
	}

	// Capture timestamp on first API response
	if _, exists := c.Get("API_RESPONSE_TIMESTAMP"); !exists {
		c.Set("API_RESPONSE_TIMESTAMP", time.Now())
	}

	if existing, exists := c.Get("API_RESPONSE"); exists {
		if existingBytes, ok := existing.([]byte); ok && len(existingBytes) > 0 {
			combined := make([]byte, 0, len(existingBytes)+len(data)+1)
			combined = append(combined, existingBytes...)
			if existingBytes[len(existingBytes)-1] != '\n' {
				combined = append(combined, '\n')
			}
			combined = append(combined, data...)
			c.Set("API_RESPONSE", combined)
			return
		}
	}

	c.Set("API_RESPONSE", bytes.Clone(data))
}

// ExecuteWithAuthManager executes a non-streaming request via the core auth manager.
// This path is the only supported execution route.
func (h *BaseAPIHandler) ExecuteWithAuthManager(ctx context.Context, handlerType, modelName string, rawJSON []byte, alt string) ([]byte, http.Header, *interfaces.ErrorMessage) {
	providers, normalizedModel, errMsg := h.getRequestDetails(modelName)
	if errMsg != nil {
		return nil, nil, errMsg
	}
	logHandlerThinkingConfig("handler auth-manager request", handlerType, normalizedModel, rawJSON)
	reqMeta := requestExecutionMetadata(ctx)
	reqMeta[coreexecutor.RequestedModelMetadataKey] = modelName
	payload := rawJSON
	if len(payload) == 0 {
		payload = nil
	}
	req := coreexecutor.Request{
		Model:   normalizedModel,
		Payload: payload,
	}
	opts := coreexecutor.Options{
		Stream:          false,
		Alt:             alt,
		OriginalRequest: rawJSON,
		SourceFormat:    sdktranslator.FromString(handlerType),
		Headers:         headersFromContext(ctx),
	}
	opts.Metadata = reqMeta
	resp, err := h.AuthManager.Execute(ctx, providers, req, opts)
	if err != nil {
		err = enrichAuthSelectionError(err, providers, normalizedModel)
		status := statusCodeFromError(err)
		addon := headersFromError(err)
		return nil, nil, &interfaces.ErrorMessage{StatusCode: status, Error: err, Addon: addon}
	}
	if !PassthroughHeadersEnabled(h.Cfg) {
		return resp.Payload, nil, nil
	}
	return resp.Payload, FilterUpstreamHeaders(resp.Headers), nil
}

// ExecuteCountWithAuthManager executes a non-streaming request via the core auth manager.
// This path is the only supported execution route.
func (h *BaseAPIHandler) ExecuteCountWithAuthManager(ctx context.Context, handlerType, modelName string, rawJSON []byte, alt string) ([]byte, http.Header, *interfaces.ErrorMessage) {
	providers, normalizedModel, errMsg := h.getRequestDetails(modelName)
	if errMsg != nil {
		return nil, nil, errMsg
	}
	logHandlerThinkingConfig("handler auth-manager token count request", handlerType, normalizedModel, rawJSON)
	reqMeta := requestExecutionMetadata(ctx)
	reqMeta[coreexecutor.RequestedModelMetadataKey] = modelName
	payload := rawJSON
	if len(payload) == 0 {
		payload = nil
	}
	req := coreexecutor.Request{
		Model:   normalizedModel,
		Payload: payload,
	}
	opts := coreexecutor.Options{
		Stream:          false,
		Alt:             alt,
		OriginalRequest: rawJSON,
		SourceFormat:    sdktranslator.FromString(handlerType),
		Headers:         headersFromContext(ctx),
	}
	opts.Metadata = reqMeta
	resp, err := h.AuthManager.ExecuteCount(ctx, providers, req, opts)
	if err != nil {
		err = enrichAuthSelectionError(err, providers, normalizedModel)
		status := statusCodeFromError(err)
		addon := headersFromError(err)
		return nil, nil, &interfaces.ErrorMessage{StatusCode: status, Error: err, Addon: addon}
	}
	if !PassthroughHeadersEnabled(h.Cfg) {
		return resp.Payload, nil, nil
	}
	return resp.Payload, FilterUpstreamHeaders(resp.Headers), nil
}

// ExecuteStreamWithAuthManager executes a streaming request via the core auth manager.
// This path is the only supported execution route.
// The returned http.Header carries upstream response headers captured before streaming begins.
func (h *BaseAPIHandler) ExecuteStreamWithAuthManager(ctx context.Context, handlerType, modelName string, rawJSON []byte, alt string) (<-chan []byte, http.Header, <-chan *interfaces.ErrorMessage) {
	data, headers, _, errs := h.ExecuteStreamWithAuthManagerMeta(ctx, handlerType, modelName, rawJSON, alt)
	return data, headers, errs
}

// ExecuteStreamWithAuthManagerMeta executes a streaming request and exposes synchronized stream metadata.
func (h *BaseAPIHandler) ExecuteStreamWithAuthManagerMeta(ctx context.Context, handlerType, modelName string, rawJSON []byte, alt string) (<-chan []byte, http.Header, *StreamMeta, <-chan *interfaces.ErrorMessage) {
	providers, normalizedModel, errMsg := h.getRequestDetails(modelName)
	if errMsg != nil {
		errChan := make(chan *interfaces.ErrorMessage, 1)
		errChan <- errMsg
		close(errChan)
		return nil, nil, newStreamMeta(nil, false), errChan
	}
	logHandlerThinkingConfig("handler auth-manager stream request", handlerType, normalizedModel, rawJSON)
	reqMeta := requestExecutionMetadata(ctx)
	reqMeta[coreexecutor.RequestedModelMetadataKey] = modelName
	payload := rawJSON
	if len(payload) == 0 {
		payload = nil
	}
	req := coreexecutor.Request{
		Model:   normalizedModel,
		Payload: payload,
	}
	opts := coreexecutor.Options{
		Stream:          true,
		Alt:             alt,
		OriginalRequest: rawJSON,
		SourceFormat:    sdktranslator.FromString(handlerType),
		Headers:         headersFromContext(ctx),
	}
	opts.Metadata = reqMeta
	streamResult, err := h.AuthManager.ExecuteStream(ctx, providers, req, opts)
	if err != nil {
		err = enrichAuthSelectionError(err, providers, normalizedModel)
		errChan := make(chan *interfaces.ErrorMessage, 1)
		status := statusCodeFromError(err)
		addon := headersFromError(err)
		errChan <- &interfaces.ErrorMessage{StatusCode: status, Error: err, Addon: addon}
		close(errChan)
		return nil, nil, newStreamMeta(nil, false), errChan
	}
	streamMeta := newStreamMeta(streamResult.TakeKeepAliveInterval(), streamResult.TakeBootstrapCommitted())
	bootstrapCommitted := streamMeta.BootstrapCommitted()
	passthroughHeadersEnabled := PassthroughHeadersEnabled(h.Cfg)
	// Capture upstream headers from the initial connection synchronously before the goroutine starts.
	// Keep a mutable map so bootstrap retries can replace it before first payload is sent.
	var upstreamHeaders http.Header
	if passthroughHeadersEnabled {
		upstreamHeaders = cloneHeader(FilterUpstreamHeaders(streamResult.Headers))
		if upstreamHeaders == nil {
			upstreamHeaders = make(http.Header)
		}
	}
	chunks := streamResult.Chunks
	dataChan := make(chan []byte)
	errChan := make(chan *interfaces.ErrorMessage, 1)
	go func() {
		defer close(dataChan)
		defer close(errChan)
		sentPayload := false
		bootstrapRetries := 0
		maxBootstrapRetries := StreamingBootstrapRetries(h.Cfg)
		var responsesValidator *sseJSONStreamValidator
		if handlerType == "openai-response" {
			responsesValidator = &sseJSONStreamValidator{}
		}
		var openAIValidator *openAIJSONStreamValidator
		if handlerType == "openai" || (handlerType == "gemini-cli" && alt == "") {
			openAIValidator = &openAIJSONStreamValidator{}
		}

		sendErr := func(msg *interfaces.ErrorMessage) bool {
			if ctx == nil {
				errChan <- msg
				return true
			}
			select {
			case <-ctx.Done():
				return false
			case errChan <- msg:
				return true
			}
		}

		sendData := func(chunk []byte) bool {
			if ctx == nil {
				dataChan <- chunk
				return true
			}
			select {
			case <-ctx.Done():
				return false
			case dataChan <- chunk:
				return true
			}
		}

		bootstrapEligible := func(err error) bool {
			status := statusFromError(err)
			if status == 0 {
				return true
			}
			switch status {
			case http.StatusUnauthorized, http.StatusForbidden, http.StatusPaymentRequired,
				http.StatusRequestTimeout, http.StatusTooManyRequests:
				return true
			default:
				return status >= http.StatusInternalServerError
			}
		}

	outer:
		for {
			for {
				var chunk coreexecutor.StreamChunk
				var ok bool
				if ctx != nil {
					select {
					case <-ctx.Done():
						return
					case chunk, ok = <-chunks:
					}
				} else {
					chunk, ok = <-chunks
				}
				if !ok {
					if responsesValidator != nil {
						payload, errValidate := responsesValidator.Finish()
						if errValidate != nil {
							_ = sendErr(&interfaces.ErrorMessage{StatusCode: http.StatusBadGateway, Error: errValidate})
							return
						}
						if len(payload) > 0 && !sendData(payload) {
							return
						}
					}
					if openAIValidator != nil {
						payloads, errValidate := openAIValidator.Finish()
						if errValidate != nil {
							_ = sendErr(&interfaces.ErrorMessage{StatusCode: http.StatusBadGateway, Error: errValidate})
							return
						}
						for _, payload := range payloads {
							if !sendData(payload) {
								return
							}
						}
					}
					return
				}
				if chunk.Err != nil {
					streamErr := chunk.Err
					// Safe bootstrap recovery: if the upstream fails before any payload bytes are sent,
					// retry a few times (to allow auth rotation / transient recovery) and then attempt model fallback.
					if !sentPayload && !bootstrapCommitted {
						if bootstrapRetries < maxBootstrapRetries && bootstrapEligible(streamErr) {
							bootstrapRetries++
							retryResult, retryErr := h.AuthManager.ExecuteStream(ctx, providers, req, opts)
							if retryErr == nil {
								keepAliveInterval := retryResult.TakeKeepAliveInterval()
								bootstrapCommitted = retryResult.TakeBootstrapCommitted()
								if passthroughHeadersEnabled {
									replaceHeader(upstreamHeaders, FilterUpstreamHeaders(retryResult.Headers))
								}
								if responsesValidator != nil {
									responsesValidator = &sseJSONStreamValidator{}
								}
								if openAIValidator != nil {
									openAIValidator = &openAIJSONStreamValidator{}
								}
								streamMeta.set(keepAliveInterval, bootstrapCommitted)
								chunks = retryResult.Chunks
								continue outer
							}
							streamErr = enrichAuthSelectionError(retryErr, providers, normalizedModel)
						}
					}

					status := statusCodeFromError(streamErr)
					addon := headersFromError(streamErr)
					_ = sendErr(&interfaces.ErrorMessage{StatusCode: status, Error: streamErr, Addon: addon})
					return
				}
				if len(chunk.Payload) > 0 {
					payloads := [][]byte{chunk.Payload}
					if responsesValidator != nil {
						payload, errValidate := responsesValidator.Add(chunk.Payload)
						if errValidate != nil {
							_ = sendErr(&interfaces.ErrorMessage{StatusCode: http.StatusBadGateway, Error: errValidate})
							return
						}
						if len(payload) == 0 {
							continue
						}
						payloads = [][]byte{payload}
					} else if openAIValidator != nil {
						var errValidate error
						payloads, errValidate = openAIValidator.Add(chunk.Payload)
						if errValidate != nil {
							_ = sendErr(&interfaces.ErrorMessage{StatusCode: http.StatusBadGateway, Error: errValidate})
							return
						}
					}
					for _, payload := range payloads {
						sentPayload = true
						if !sendData(cloneBytes(payload)) {
							return
						}
					}
				}
			}
		}
	}()
	return dataChan, upstreamHeaders, streamMeta, errChan
}

type openAIJSONStreamValidator struct {
	pending []byte
	rawScan jsonValueStreamScanner
}

func (v *openAIJSONStreamValidator) Add(chunk []byte) ([][]byte, error) {
	if v == nil || len(chunk) == 0 {
		return nil, nil
	}
	var payloads [][]byte
	for len(chunk) > 0 {
		available := maxStreamJSONPendingBytes - len(v.pending)
		if available == 0 {
			return nil, fmt.Errorf("OpenAI stream value exceeds %d bytes", maxStreamJSONPendingBytes)
		}
		appendLen := min(len(chunk), available)
		v.pending = append(v.pending, chunk[:appendLen]...)
		chunk = chunk[appendLen:]
		complete, err := v.consume(false)
		if err != nil {
			return nil, err
		}
		payloads = append(payloads, complete...)
	}
	return payloads, nil
}

func (v *openAIJSONStreamValidator) Finish() ([][]byte, error) {
	if v == nil {
		return nil, nil
	}
	payloads, err := v.consume(true)
	if err != nil {
		return nil, err
	}
	if len(bytes.TrimSpace(v.pending)) > 0 {
		return nil, errors.New("incomplete OpenAI stream value")
	}
	v.pending = nil
	return payloads, nil
}

func (v *openAIJSONStreamValidator) consume(final bool) ([][]byte, error) {
	var payloads [][]byte
	for {
		trimmed := bytes.TrimLeft(v.pending, " \t\r\n")
		if len(trimmed) != len(v.pending) {
			v.rawScan.reset()
		}
		v.pending = trimmed
		if len(v.pending) == 0 {
			return payloads, nil
		}

		isSSE, partialSSEPrefix := openAIStreamSSEPrefix(v.pending)
		if partialSSEPrefix {
			if final {
				return nil, errors.New("incomplete OpenAI SSE field")
			}
			return payloads, nil
		}
		if isSSE {
			frameLen := sseJSONFrameLen(v.pending)
			if frameLen == 0 {
				if final {
					return nil, errors.New("incomplete OpenAI SSE frame")
				}
				return payloads, nil
			}
			frame := v.pending[:frameLen]
			payload, found := sseJSONDataPayload(frame)
			if found && len(payload) > 0 && !bytes.Equal(payload, []byte("[DONE]")) {
				if !json.Valid(payload) {
					return nil, errors.New("invalid OpenAI SSE data JSON")
				}
				// Joined multi-line data is valid JSON, so newlines are inter-token
				// whitespace; flatten them so downstream SSE re-framing stays intact.
				if bytes.IndexByte(payload, '\n') >= 0 {
					payload = bytes.ReplaceAll(payload, []byte("\n"), []byte(" "))
				}
				payloads = append(payloads, cloneBytes(payload))
			}
			v.pending = v.pending[frameLen:]
			v.rawScan.reset()
			continue
		}

		doneMarker := []byte("[DONE]")
		if len(v.pending) < len(doneMarker) && bytes.HasPrefix(doneMarker, v.pending) {
			if final {
				return nil, errors.New("incomplete OpenAI stream done marker")
			}
			return payloads, nil
		}
		if bytes.HasPrefix(v.pending, doneMarker) {
			v.pending = v.pending[len(doneMarker):]
			v.rawScan.reset()
			continue
		}

		end, complete, err := v.rawScan.valueEnd(v.pending, final)
		if err != nil {
			return nil, fmt.Errorf("invalid OpenAI stream JSON: %w", err)
		}
		if !complete {
			if final {
				return nil, errors.New("incomplete OpenAI stream value")
			}
			return payloads, nil
		}
		payload := v.pending[:end]
		if !json.Valid(payload) {
			return nil, errors.New("invalid OpenAI stream JSON")
		}
		payloads = append(payloads, cloneBytes(payload))
		v.pending = v.pending[end:]
		v.rawScan.reset()
	}
}

func openAIStreamSSEPrefix(data []byte) (bool, bool) {
	for _, prefix := range [][]byte{[]byte("data:"), []byte("event:"), []byte("id:"), []byte("retry:"), []byte(":")} {
		if bytes.HasPrefix(data, prefix) {
			return true, false
		}
		if bytes.HasPrefix(prefix, data) {
			return false, true
		}
	}
	return false, false
}

type jsonValueStreamScanner struct {
	offset   int
	depth    int
	kind     byte
	inString bool
	escaped  bool
}

func (s *jsonValueStreamScanner) reset() {
	*s = jsonValueStreamScanner{}
}

func (s *jsonValueStreamScanner) valueEnd(data []byte, final bool) (int, bool, error) {
	if len(data) == 0 {
		return 0, false, nil
	}
	if s.kind == 0 {
		switch data[0] {
		case '{', '[':
			s.kind = data[0]
			s.depth = 1
			s.offset = 1
		case '"':
			s.kind = '"'
			s.inString = true
			s.offset = 1
		case 't', 'f', 'n':
			s.kind = data[0]
		case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			s.kind = '#'
		default:
			return 0, false, fmt.Errorf("unexpected leading byte %q", data[0])
		}
	}

	switch s.kind {
	case '{', '[', '"':
		for index := s.offset; index < len(data); index++ {
			value := data[index]
			if s.inString {
				if s.escaped {
					s.escaped = false
					continue
				}
				switch value {
				case '\\':
					s.escaped = true
				case '"':
					s.inString = false
					if s.kind == '"' {
						return index + 1, true, nil
					}
				default:
					if value < 0x20 {
						return 0, false, fmt.Errorf("unescaped control byte %q", value)
					}
				}
				continue
			}
			switch value {
			case '"':
				s.inString = true
			case '{', '[':
				s.depth++
			case '}', ']':
				s.depth--
				if s.depth < 0 {
					return 0, false, errors.New("unexpected closing delimiter")
				}
				if s.depth == 0 {
					return index + 1, true, nil
				}
			}
		}
		s.offset = len(data)
		return 0, false, nil
	case 't', 'f', 'n':
		target := "true"
		if s.kind == 'f' {
			target = "false"
		} else if s.kind == 'n' {
			target = "null"
		}
		for s.offset < len(data) && s.offset < len(target) {
			if data[s.offset] != target[s.offset] {
				return 0, false, fmt.Errorf("invalid literal %q", data[:s.offset+1])
			}
			s.offset++
		}
		if s.offset == len(target) {
			// A literal is complete only when followed by a token boundary;
			// otherwise wait for more bytes so "truee" is not split as
			// "true" plus a dangling "e".
			if s.offset < len(data) {
				switch data[s.offset] {
				case ' ', '\t', '\r', '\n', '{', '[', '"', 't', 'f', 'n', '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
				default:
					return 0, false, fmt.Errorf("invalid byte %q after literal %q", data[s.offset], target)
				}
			} else if !final {
				return 0, false, nil
			}
			return s.offset, true, nil
		}
		return 0, false, nil
	case '#':
		index := s.offset
		for index < len(data) && jsonNumberStreamByte(data[index]) {
			index++
		}
		s.offset = index
		if index < len(data) {
			if !json.Valid(data[:index]) {
				return 0, false, fmt.Errorf("invalid number %q", data[:index])
			}
			return index, true, nil
		}
		if final {
			if !json.Valid(data) {
				return 0, false, fmt.Errorf("invalid number %q", data)
			}
			return len(data), true, nil
		}
		return 0, false, nil
	default:
		return 0, false, errors.New("invalid JSON scanner state")
	}
}

func jsonNumberStreamByte(value byte) bool {
	return value >= '0' && value <= '9' || value == '-' || value == '+' || value == '.' || value == 'e' || value == 'E'
}

type sseJSONStreamValidator struct {
	pending  []byte
	scanFrom int
}

const maxStreamJSONPendingBytes = 64 << 20

func (v *sseJSONStreamValidator) Add(chunk []byte) ([]byte, error) {
	if v == nil || len(chunk) == 0 {
		return nil, nil
	}

	var out []byte
	for len(chunk) > 0 {
		available := maxStreamJSONPendingBytes - len(v.pending)
		if available == 0 {
			return nil, fmt.Errorf("SSE frame exceeds %d bytes", maxStreamJSONPendingBytes)
		}
		appendLen := min(len(chunk), available)
		v.pending = append(v.pending, chunk[:appendLen]...)
		chunk = chunk[appendLen:]

		consumed := 0
		scanFrom := v.scanFrom
		for {
			frameLen := sseJSONFrameLen(v.pending[scanFrom:])
			if frameLen == 0 {
				v.scanFrom = max(0, len(v.pending)-3)
				break
			}
			frameEnd := scanFrom + frameLen
			frame := v.pending[consumed:frameEnd]
			if err := validateSSEDataJSON(frame); err != nil {
				return nil, err
			}
			out = append(out, frame...)
			consumed = frameEnd
			scanFrom = frameEnd
		}
		if consumed > 0 {
			copy(v.pending, v.pending[consumed:])
			v.pending = v.pending[:len(v.pending)-consumed]
			v.scanFrom -= consumed
			if v.scanFrom < 0 {
				v.scanFrom = 0
			}
		}
	}
	return out, nil
}

func (v *sseJSONStreamValidator) Finish() ([]byte, error) {
	if v == nil || len(bytes.TrimSpace(v.pending)) == 0 {
		return nil, nil
	}
	return nil, errors.New("incomplete SSE frame")
}

func sseJSONFrameLen(chunk []byte) int {
	lineStart := 0
	for i := 0; i < len(chunk); {
		switch chunk[i] {
		case '\n':
			i++
			if i-1 == lineStart {
				return i
			}
			lineStart = i
		case '\r':
			lineEnd := i
			i++
			if lineEnd == lineStart && i == len(chunk) {
				return 0
			}
			if i < len(chunk) && chunk[i] == '\n' {
				i++
			}
			if lineEnd == lineStart {
				return i
			}
			lineStart = i
		default:
			i++
		}
	}
	return 0
}

func sseJSONDataPayload(frame []byte) ([]byte, bool) {
	var payload []byte
	found := false
	for len(frame) > 0 {
		lineEnd := bytes.IndexAny(frame, "\r\n")
		line := frame
		if lineEnd >= 0 {
			line = frame[:lineEnd]
			separatorLen := 1
			if frame[lineEnd] == '\r' && lineEnd+1 < len(frame) && frame[lineEnd+1] == '\n' {
				separatorLen = 2
			}
			frame = frame[lineEnd+separatorLen:]
		} else {
			frame = nil
		}
		line = bytes.TrimSpace(line)
		if !bytes.HasPrefix(line, []byte("data:")) {
			continue
		}
		if found {
			payload = append(payload, '\n')
		}
		payload = append(payload, bytes.TrimSpace(line[len("data:"):])...)
		found = true
	}
	return payload, found
}

func sseJSONPayloadValid(data []byte) bool {
	data = bytes.TrimSpace(data)
	return len(data) == 0 || bytes.Equal(data, []byte("[DONE]")) || json.Valid(data)
}

func validateSSEDataJSON(frame []byte) error {
	data, found := sseJSONDataPayload(frame)
	if !found || sseJSONPayloadValid(data) {
		return nil
	}
	const max = 512
	preview := data
	if len(preview) > max {
		preview = preview[:max]
	}
	return fmt.Errorf("invalid SSE data JSON (len=%d): %q", len(data), preview)
}

func statusFromError(err error) int {
	if err == nil {
		return 0
	}
	if se, ok := errors.AsType[coreexecutor.StatusError](err); ok && se != nil {
		if code := se.StatusCode(); code > 0 {
			return code
		}
	}
	return 0
}

func (h *BaseAPIHandler) getRequestDetails(modelName string) (providers []string, normalizedModel string, err *interfaces.ErrorMessage) {
	resolvedModelName := modelName
	initialSuffix := thinking.ParseSuffix(modelName)
	if initialSuffix.ModelName == "auto" {
		if h != nil && h.AuthManager != nil && h.AuthManager.HomeEnabled() {
			resolvedModelName = modelName
		} else {
			resolvedBase := util.ResolveAutoModel(initialSuffix.ModelName)
			if initialSuffix.HasSuffix {
				resolvedModelName = fmt.Sprintf("%s(%s)", resolvedBase, initialSuffix.RawSuffix)
			} else {
				resolvedModelName = resolvedBase
			}
		}
	} else {
		if h != nil && h.AuthManager != nil && h.AuthManager.HomeEnabled() {
			resolvedModelName = modelName
		} else {
			resolvedModelName = util.ResolveAutoModel(modelName)
		}
	}

	parsed := thinking.ParseSuffix(resolvedModelName)
	baseModel := strings.TrimSpace(parsed.ModelName)

	if strings.EqualFold(baseModel, "gpt-image-2") {
		return nil, "", &interfaces.ErrorMessage{
			StatusCode: http.StatusServiceUnavailable,
			Error:      fmt.Errorf("model %s is only supported on /v1/images/generations and /v1/images/edits", baseModel),
		}
	}

	if h != nil && h.AuthManager != nil && h.AuthManager.HomeEnabled() {
		return []string{"home"}, resolvedModelName, nil
	}

	providers = util.GetProviderName(baseModel)
	// Fallback: if baseModel has no provider but differs from resolvedModelName,
	// try using the full model name. This handles edge cases where custom models
	// may be registered with their full suffixed name (e.g., "my-model(8192)").
	// Evaluated in Story 11.8: This fallback is intentionally preserved to support
	// custom model registrations that include thinking suffixes.
	if len(providers) == 0 && baseModel != resolvedModelName {
		providers = util.GetProviderName(resolvedModelName)
	}

	if len(providers) == 0 {
		return nil, "", &interfaces.ErrorMessage{StatusCode: http.StatusBadGateway, Error: fmt.Errorf("unknown provider for model %s", modelName)}
	}

	// The thinking suffix is preserved in the model name itself, so no
	// metadata-based configuration passing is needed.
	return providers, resolvedModelName, nil
}

func cloneBytes(src []byte) []byte {
	if len(src) == 0 {
		return nil
	}
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

func logHandlerThinkingConfig(component, handlerType, model string, body []byte) {
	if !log.IsLevelEnabled(log.DebugLevel) {
		return
	}
	responsesEffort := gjson.GetBytes(body, "reasoning.effort")
	chatEffort := gjson.GetBytes(body, "reasoning_effort")
	if strings.TrimSpace(model) == "" && !responsesEffort.Exists() && !chatEffort.Exists() {
		return
	}
	fields := log.Fields{
		"component":    component,
		"handler_type": handlerType,
		"model":        model,
	}
	if responsesEffort.Exists() {
		fields["reasoning.effort"] = responsesEffort.String()
	}
	if chatEffort.Exists() {
		fields["reasoning_effort"] = chatEffort.String()
	}
	log.WithFields(fields).Debug("handler request thinking config")
}

func cloneHeader(src http.Header) http.Header {
	if src == nil {
		return nil
	}
	dst := make(http.Header, len(src))
	for key, values := range src {
		dst[key] = append([]string(nil), values...)
	}
	return dst
}

func replaceHeader(dst http.Header, src http.Header) {
	for key := range dst {
		delete(dst, key)
	}
	for key, values := range src {
		dst[key] = append([]string(nil), values...)
	}
}

func enrichAuthSelectionError(err error, providers []string, model string) error {
	if err == nil {
		return nil
	}

	var authErr *coreauth.Error
	if !errors.As(err, &authErr) || authErr == nil {
		return err
	}

	code := strings.TrimSpace(authErr.Code)
	if code != "auth_not_found" && code != "auth_unavailable" {
		return err
	}

	providerText := strings.Join(providers, ",")
	if providerText == "" {
		providerText = "unknown"
	}
	modelText := strings.TrimSpace(model)
	if modelText == "" {
		modelText = "unknown"
	}

	baseMessage := strings.TrimSpace(authErr.Message)
	if baseMessage == "" {
		baseMessage = "no auth available"
	}
	detail := fmt.Sprintf("%s (providers=%s, model=%s)", baseMessage, providerText, modelText)

	// Clarify the most common alias confusion between Anthropic route names and internal provider keys.
	if strings.Contains(","+providerText+",", ",claude,") {
		detail += "; check Claude auth/key session and cooldown state via /v0/management/auth-files"
	}

	status := authErr.HTTPStatus
	if status <= 0 {
		status = http.StatusServiceUnavailable
	}

	return &coreauth.Error{
		Code:       authErr.Code,
		Message:    detail,
		Retryable:  authErr.Retryable,
		HTTPStatus: status,
	}
}

// WriteErrorResponse writes an error message to the response writer using the HTTP status embedded in the message.
func (h *BaseAPIHandler) WriteErrorResponse(c *gin.Context, msg *interfaces.ErrorMessage) {
	status := http.StatusInternalServerError
	if msg != nil && msg.StatusCode > 0 {
		status = msg.StatusCode
	}
	if msg != nil && msg.Addon != nil && PassthroughHeadersEnabled(h.Cfg) {
		for key, values := range msg.Addon {
			if len(values) == 0 {
				continue
			}
			c.Writer.Header().Del(key)
			for _, value := range values {
				c.Writer.Header().Add(key, value)
			}
		}
	}

	errText := http.StatusText(status)
	if msg != nil && msg.Error != nil {
		if v := strings.TrimSpace(msg.Error.Error()); v != "" {
			errText = v
		}
	}

	body := BuildErrorResponseBody(status, errText)
	// Append first to preserve upstream response logs, then drop duplicate payloads if already recorded.
	var previous []byte
	if existing, exists := c.Get("API_RESPONSE"); exists {
		if existingBytes, ok := existing.([]byte); ok && len(existingBytes) > 0 {
			previous = existingBytes
		}
	}
	appendAPIResponse(c, body)
	trimmedErrText := strings.TrimSpace(errText)
	trimmedBody := bytes.TrimSpace(body)
	if len(previous) > 0 {
		if (trimmedErrText != "" && bytes.Contains(previous, []byte(trimmedErrText))) ||
			(len(trimmedBody) > 0 && bytes.Contains(previous, trimmedBody)) {
			c.Set("API_RESPONSE", previous)
		}
	}

	if !c.Writer.Written() {
		c.Writer.Header().Set("Content-Type", "application/json")
	}
	c.Status(status)
	_, _ = c.Writer.Write(body)
}

func (h *BaseAPIHandler) LoggingAPIResponseError(ctx context.Context, err *interfaces.ErrorMessage) {
	if h.Cfg.RequestLog {
		if ginContext, ok := ctx.Value("gin").(*gin.Context); ok {
			if apiResponseErrors, isExist := ginContext.Get("API_RESPONSE_ERROR"); isExist {
				if slicesAPIResponseError, isOk := apiResponseErrors.([]*interfaces.ErrorMessage); isOk {
					slicesAPIResponseError = append(slicesAPIResponseError, err)
					ginContext.Set("API_RESPONSE_ERROR", slicesAPIResponseError)
				}
			} else {
				// Create new response data entry
				ginContext.Set("API_RESPONSE_ERROR", []*interfaces.ErrorMessage{err})
			}
		}
	}
}

// APIHandlerCancelFunc is a function type for canceling an API handler's context.
// It can optionally accept parameters, which are used for logging the response.
type APIHandlerCancelFunc func(params ...interface{})
