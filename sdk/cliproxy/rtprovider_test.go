package cliproxy

import (
	"fmt"
	"net/http"
	"testing"

	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
)

func TestRoundTripperForDirectBypassesProxy(t *testing.T) {
	t.Parallel()

	provider := newDefaultRoundTripperProvider()
	rt := provider.RoundTripperFor(&coreauth.Auth{ProxyURL: "direct"})
	transport, ok := rt.(*http.Transport)
	if !ok {
		t.Fatalf("transport type = %T, want *http.Transport", rt)
	}
	if transport.Proxy != nil {
		t.Fatal("expected direct transport to disable proxy function")
	}
}

func TestRoundTripperForReusesAndBoundsProxyTransports(t *testing.T) {
	provider := newDefaultRoundTripperProvider()
	auth := &coreauth.Auth{ProxyURL: "http://proxy-0.example.com:8080"}
	first := provider.RoundTripperFor(auth)
	if reused := provider.RoundTripperFor(auth); reused != first {
		t.Fatal("expected transport to be reused for the same proxy")
	}

	for i := 1; i <= roundTripperCacheLimit; i++ {
		provider.RoundTripperFor(&coreauth.Auth{ProxyURL: fmt.Sprintf("http://proxy-%d.example.com:8080", i)})
	}

	provider.mu.Lock()
	defer provider.mu.Unlock()
	if got := len(provider.cache); got != roundTripperCacheLimit {
		t.Fatalf("transport cache size = %d, want %d", got, roundTripperCacheLimit)
	}
	if _, exists := provider.cache[auth.ProxyURL]; exists {
		t.Fatal("expected least recently used proxy transport to be evicted")
	}
}
