package middleware

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/orbconfig"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/orbcredentials"
)

func TestRequestLoggingMiddlewareNeverCapturesSensitiveBrokerPayloads(t *testing.T) {
	gin.SetMode(gin.TestMode)
	paths := []string{
		orbconfig.EndpointPath,
		orbconfig.EndpointPath + "/",
		"/ampcode/local-broker/./orb-config-bundle.json",
		"/ampcode//local-broker/orb-config-bundle.json",
		"/ampcode/local-broker/temporary/../orb-config-bundle.json",
		orbcredentials.EndpointPath,
		orbcredentials.EndpointPath + "/",
		"/ampcode/local-broker/./orb-credentials.json",
		"/ampcode//local-broker/orb-credentials.json",
		"/ampcode/local-broker/publish-image-result.json",
		"/ampcode/local-broker/./publish-image-result.json",
	}
	for _, enabled := range []bool{false, true} {
		for _, requestPath := range paths {
			if shouldLogRequest(requestPath) {
				t.Fatalf("enabled=%t path=%q: sensitive broker path is loggable", enabled, requestPath)
			}
			logger := &credentialRequestLogger{enabled: enabled}
			body := &credentialRequestBody{reader: strings.NewReader("sensitive-broker-payload")}
			bodyReplaced := false
			engine := gin.New()
			engine.RedirectFixedPath = false
			engine.RedirectTrailingSlash = false
			engine.Use(RequestLoggingMiddleware(logger))
			engine.NoRoute(func(c *gin.Context) {
				bodyReplaced = c.Request.Body != body
				c.JSON(http.StatusBadRequest, gin.H{"ok": false})
			})
			sentinel := "sensitive-broker-payload"
			request := httptest.NewRequest(http.MethodPut, requestPath, nil)
			request.Body = body
			request.ContentLength = int64(len(sentinel))
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			engine.ServeHTTP(response, request)
			if logger.calls != 0 || logger.enabledChecks != 0 {
				t.Fatalf("enabled=%t path=%q: sensitive broker request was logged", enabled, requestPath)
			}
			if bodyReplaced || body.reads != 0 || body.closes != 0 {
				t.Fatalf("enabled=%t path=%q: sensitive broker body was touched", enabled, requestPath)
			}
			if strings.Contains(string(logger.body), sentinel) {
				t.Fatalf("enabled=%t path=%q: sensitive broker body was captured", enabled, requestPath)
			}
		}
	}
}

type credentialRequestLogger struct {
	enabled       bool
	enabledChecks int
	calls         int
	body          []byte
}

type credentialRequestBody struct {
	reader *strings.Reader
	reads  int
	closes int
}

func (body *credentialRequestBody) Read(payload []byte) (int, error) {
	body.reads++
	return body.reader.Read(payload)
}

func (body *credentialRequestBody) Close() error {
	body.closes++
	return nil
}

func (logger *credentialRequestLogger) LogRequest(_ string, _ string, _ map[string][]string, body []byte, _ int, _ map[string][]string, _ []byte, _ []byte, _ []byte, _ []byte, _ []byte, _ []*interfaces.ErrorMessage, _ string, _ time.Time, _ time.Time) error {
	logger.calls++
	logger.body = append([]byte(nil), body...)
	return nil
}

func (logger *credentialRequestLogger) LogStreamingRequest(string, string, map[string][]string, []byte, string) (logging.StreamingLogWriter, error) {
	logger.calls++
	return nil, nil
}

func (logger *credentialRequestLogger) IsEnabled() bool {
	logger.enabledChecks++
	return logger.enabled
}

func TestShouldSkipMethodForRequestLogging(t *testing.T) {
	tests := []struct {
		name string
		req  *http.Request
		skip bool
	}{
		{
			name: "nil request",
			req:  nil,
			skip: true,
		},
		{
			name: "post request should not skip",
			req: &http.Request{
				Method: http.MethodPost,
				URL:    &url.URL{Path: "/v1/responses"},
			},
			skip: false,
		},
		{
			name: "plain get should skip",
			req: &http.Request{
				Method: http.MethodGet,
				URL:    &url.URL{Path: "/v1/models"},
				Header: http.Header{},
			},
			skip: true,
		},
		{
			name: "responses websocket upgrade should not skip",
			req: &http.Request{
				Method: http.MethodGet,
				URL:    &url.URL{Path: "/v1/responses"},
				Header: http.Header{"Upgrade": []string{"websocket"}},
			},
			skip: false,
		},
		{
			name: "responses get without upgrade should skip",
			req: &http.Request{
				Method: http.MethodGet,
				URL:    &url.URL{Path: "/v1/responses"},
				Header: http.Header{},
			},
			skip: true,
		},
	}

	for i := range tests {
		got := shouldSkipMethodForRequestLogging(tests[i].req)
		if got != tests[i].skip {
			t.Fatalf("%s: got skip=%t, want %t", tests[i].name, got, tests[i].skip)
		}
	}
}

func TestShouldCaptureRequestBody(t *testing.T) {
	tests := []struct {
		name          string
		loggerEnabled bool
		req           *http.Request
		want          bool
	}{
		{
			name:          "logger enabled always captures",
			loggerEnabled: true,
			req: &http.Request{
				Body:          io.NopCloser(strings.NewReader("{}")),
				ContentLength: -1,
				Header:        http.Header{"Content-Type": []string{"application/json"}},
			},
			want: true,
		},
		{
			name:          "nil request",
			loggerEnabled: false,
			req:           nil,
			want:          false,
		},
		{
			name:          "small known size json in error-only mode",
			loggerEnabled: false,
			req: &http.Request{
				Body:          io.NopCloser(strings.NewReader("{}")),
				ContentLength: 2,
				Header:        http.Header{"Content-Type": []string{"application/json"}},
			},
			want: true,
		},
		{
			name:          "large known size skipped in error-only mode",
			loggerEnabled: false,
			req: &http.Request{
				Body:          io.NopCloser(strings.NewReader("x")),
				ContentLength: maxErrorOnlyCapturedRequestBodyBytes + 1,
				Header:        http.Header{"Content-Type": []string{"application/json"}},
			},
			want: false,
		},
		{
			name:          "unknown size skipped in error-only mode",
			loggerEnabled: false,
			req: &http.Request{
				Body:          io.NopCloser(strings.NewReader("x")),
				ContentLength: -1,
				Header:        http.Header{"Content-Type": []string{"application/json"}},
			},
			want: false,
		},
		{
			name:          "multipart skipped in error-only mode",
			loggerEnabled: false,
			req: &http.Request{
				Body:          io.NopCloser(strings.NewReader("x")),
				ContentLength: 1,
				Header:        http.Header{"Content-Type": []string{"multipart/form-data; boundary=abc"}},
			},
			want: false,
		},
	}

	for i := range tests {
		got := shouldCaptureRequestBody(tests[i].loggerEnabled, tests[i].req)
		if got != tests[i].want {
			t.Fatalf("%s: got %t, want %t", tests[i].name, got, tests[i].want)
		}
	}
}
