package amp

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
)

func TestNeoRetryableProviderStatus(t *testing.T) {
	for _, s := range []int{http.StatusTooManyRequests, http.StatusServiceUnavailable} {
		if !neoRetryableProviderStatus(s) {
			t.Errorf("status %d should trigger fallback", s)
		}
	}
	for _, s := range []int{http.StatusOK, http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusInternalServerError} {
		if neoRetryableProviderStatus(s) {
			t.Errorf("status %d should not trigger fallback", s)
		}
	}
}

func TestNeoModelFailureBreaker(t *testing.T) {
	b := newNeoModelFailureBreaker(60 * time.Millisecond)

	if b.tripped("gemini-3.1-pro-preview") {
		t.Fatal("breaker should start un-tripped")
	}
	b.trip("gemini-3.1-pro-preview")
	if !b.tripped("gemini-3.1-pro-preview") {
		t.Fatal("breaker should be tripped after a failure")
	}
	// Suffix/case-insensitive: the same base model shares the cooldown.
	if !b.tripped("Gemini-3.1-Pro-Preview(xhigh)") {
		t.Fatal("breaker key should ignore thinking suffix and case")
	}
	// An unrelated model is independent.
	if b.tripped("gemini-3-flash-preview") {
		t.Fatal("unrelated model should not be tripped")
	}
	// Cooldown expires.
	time.Sleep(90 * time.Millisecond)
	if b.tripped("gemini-3.1-pro-preview") {
		t.Fatal("breaker should reset after cooldown")
	}
}

func TestNeoModelFailureBreakerNilSafe(t *testing.T) {
	var b *neoModelFailureBreaker
	if b.tripped("x") {
		t.Fatal("nil breaker must report not-tripped")
	}
	b.trip("x") // must not panic
}

func TestFallbackTargetForNoMapper(t *testing.T) {
	fh := &FallbackHandler{}
	if got := fh.fallbackTargetFor("gemini-3.1-pro-preview", "gemini-3.1-pro-preview"); got != "" {
		t.Fatalf("no fallback mapper should yield empty target, got %q", got)
	}
}

// stubFallbackMapper always returns a fixed fallback target, bypassing the registry
// availability check so the handler integration can be exercised in isolation.
type stubFallbackMapper struct{ target string }

func (s stubFallbackMapper) MapModel(string) string                  { return s.target }
func (s stubFallbackMapper) UpdateMappings([]config.AmpModelMapping) {}

func TestFallbackHandlerFailsOverOnRetryableStatus(t *testing.T) {
	gin.SetMode(gin.TestMode)
	reg := registry.GetGlobalRegistry()
	reg.RegisterClient("test-client-gemini-fallback-source", "gemini", []*registry.ModelInfo{
		{ID: "gemini-3-flash-preview", OwnedBy: "google", Type: "gemini"},
	})
	defer reg.UnregisterClient("test-client-gemini-fallback-source")

	fh := &FallbackHandler{
		getProxy:           func() *httputil.ReverseProxy { return nil },
		fallbackMapper:     stubFallbackMapper{target: "claude-sonnet-4-6"},
		failureBreaker:     newNeoModelFailureBreaker(time.Minute),
		forceModelMappings: func() bool { return false },
	}

	var seenModels []string
	calls := 0
	handler := func(c *gin.Context) {
		body, _ := io.ReadAll(c.Request.Body)
		seenModels = append(seenModels, gjson.GetBytes(body, "model").String())
		calls++
		if calls == 1 {
			c.JSON(http.StatusTooManyRequests, gin.H{"error": "quota exceeded"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"model": gjson.GetBytes(body, "model").String()})
	}
	wrapped := fh.WrapHandler(handler)

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost,
		"/api/provider/google/v1beta1/models/gemini-3-flash-preview:generateContent",
		bytes.NewReader([]byte(`{"model":"gemini-3-flash-preview","input":"x"}`)))
	wrapped(c)

	if !fh.failureBreaker.tripped("gemini-3-flash-preview") {
		t.Fatal("a provider quota failure should trip the breaker")
	}
	if len(seenModels) != 2 || seenModels[1] != "claude-sonnet-4-6" {
		t.Fatalf("same request should retry through the fallback model, saw %v", seenModels)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
}

// When the source model has no local provider at all (e.g. the Gemini Vertex auth was
// removed), a configured fallback must route to the fallback model rather than forwarding
// to ampcode.com (Amp credits).
func TestFallbackHandlerRoutesToFallbackWhenNoLocalProvider(t *testing.T) {
	gin.SetMode(gin.TestMode)
	fh := &FallbackHandler{
		getProxy:           func() *httputil.ReverseProxy { return nil },
		fallbackMapper:     stubFallbackMapper{target: "claude-opus-4-8(xhigh)"},
		failureBreaker:     newNeoModelFailureBreaker(time.Minute),
		forceModelMappings: func() bool { return true },
	}
	seen := ""
	handler := func(c *gin.Context) {
		body, _ := io.ReadAll(c.Request.Body)
		seen = gjson.GetBytes(body, "model").String()
		c.Status(http.StatusOK)
	}
	wrapped := fh.WrapHandler(handler)

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost,
		"/api/provider/google/v1beta1/models/gemini-3.1-pro-preview:generateContent",
		bytes.NewReader([]byte(`{"model":"gemini-3.1-pro-preview","input":"x"}`)))
	wrapped(c)

	if seen != "claude-opus-4-8(xhigh)" {
		t.Fatalf("no local provider should route to the fallback model (not amp credits), saw %q", seen)
	}
}
