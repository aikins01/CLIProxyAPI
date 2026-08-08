package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/util"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v7/sdk/config"
	"golang.org/x/net/context"
)

func TestRequestExecutionMetadataIncludesExecutionSessionWithoutIdempotencyKey(t *testing.T) {
	ctx := WithExecutionSessionID(context.Background(), "session-1")

	meta := requestExecutionMetadata(ctx)
	if got := meta[coreexecutor.ExecutionSessionMetadataKey]; got != "session-1" {
		t.Fatalf("ExecutionSessionMetadataKey = %v, want %q", got, "session-1")
	}
	if _, ok := meta[idempotencyKeyMetadataKey]; ok {
		t.Fatalf("unexpected idempotency key in metadata: %v", meta[idempotencyKeyMetadataKey])
	}
}

func TestLocalNeoTrustUsesRequestContext(t *testing.T) {
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	c.Request.Header.Set(util.LocalNeoInferenceHeaderName, "1")

	if isAmpStrictJSONRequest(c) {
		t.Fatal("public marker must not enable strict JSON behavior")
	}

	c.Request = c.Request.WithContext(util.WithTrustedLocalNeoInference(c.Request.Context()))
	if !isAmpStrictJSONRequest(c) {
		t.Fatal("trusted request context did not enable strict JSON behavior")
	}
}

func TestGetContextWithCancelPreservesLocalNeoTrust(t *testing.T) {
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	c.Request = c.Request.WithContext(util.WithTrustedLocalNeoInference(c.Request.Context()))
	handler := NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, nil)

	executorCtx, cancel := handler.GetContextWithCancel(nil, c, context.Background())
	defer cancel()
	if !util.IsTrustedLocalNeoInference(executorCtx) {
		t.Fatal("executor context did not preserve trusted local Neo state")
	}
}

func TestGetContextWithCancelPreservesWebsocketExecutionSession(t *testing.T) {
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	requestCtx := coreexecutor.WithDownstreamWebsocket(c.Request.Context())
	requestCtx = WithExecutionSessionID(requestCtx, "session-1")
	c.Request = c.Request.WithContext(requestCtx)
	handler := NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, nil)

	executorCtx, cancel := handler.GetContextWithCancel(nil, c, context.Background())
	defer cancel()
	if !coreexecutor.DownstreamWebsocket(executorCtx) {
		t.Fatal("executor context did not preserve downstream websocket state")
	}
	meta := requestExecutionMetadata(executorCtx)
	if got := meta[coreexecutor.ExecutionSessionMetadataKey]; got != "session-1" {
		t.Fatalf("ExecutionSessionMetadataKey = %v, want session-1", got)
	}
}
