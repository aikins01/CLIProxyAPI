package helps

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v7/sdk/config"
)

type closeTrackingTransport struct {
	closeCalls int
}

func (t *closeTrackingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader("ok")),
	}, nil
}

func (t *closeTrackingTransport) CloseIdleConnections() {
	t.closeCalls++
}

func TestNewProxyAwareHTTPClientDirectBypassesGlobalProxy(t *testing.T) {
	t.Parallel()

	client := NewProxyAwareHTTPClient(
		context.Background(),
		&config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "http://global-proxy.example.com:8080"}},
		&cliproxyauth.Auth{ProxyURL: "direct"},
		0,
	)

	cached, ok := client.Transport.(*retiringRoundTripper)
	if !ok {
		t.Fatalf("transport type = %T, want *retiringRoundTripper", client.Transport)
	}
	transport, ok := cached.transport.(*http.Transport)
	if !ok {
		t.Fatalf("cached transport type = %T, want *http.Transport", cached.transport)
	}
	if transport.Proxy != nil {
		t.Fatal("expected direct transport to disable proxy function")
	}
}

func TestNewProxyAwareHTTPClientReusesConfiguredTransport(t *testing.T) {
	cfg := &config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "http://reuse-proxy.example.com:8080"}}

	first := NewProxyAwareHTTPClient(context.Background(), cfg, nil, 0)
	second := NewProxyAwareHTTPClient(context.Background(), cfg, nil, 0)

	if first.Transport != second.Transport {
		t.Fatal("expected configured proxy transport to be reused")
	}
}

func TestRoundTripperCacheRetiresTransportAfterActiveResponseCloses(t *testing.T) {
	cache := roundTripperCache{limit: 1}
	first := &closeTrackingTransport{}
	cached := cache.get("first", func() http.RoundTripper { return first })
	req, err := http.NewRequest(http.MethodGet, "https://example.com", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := cached.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}

	cache.get("second", func() http.RoundTripper { return &closeTrackingTransport{} })
	if first.closeCalls != 0 {
		t.Fatalf("close calls while response active = %d, want 0", first.closeCalls)
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if first.closeCalls != 1 {
		t.Fatalf("close calls after response close = %d, want 1", first.closeCalls)
	}
}

func TestNewAuthStateNeutralErrorPreservesCause(t *testing.T) {
	want := errors.New("transport failed")
	wrapped := NewAuthStateNeutralError(want)
	if !errors.Is(wrapped, want) {
		t.Fatalf("wrapped error does not preserve cause: %v", wrapped)
	}
	neutral, ok := wrapped.(interface{ AuthStateNeutral() bool })
	if !ok || !neutral.AuthStateNeutral() {
		t.Fatalf("wrapped error is not auth-state-neutral: %v", wrapped)
	}
	if NewAuthStateNeutralError(nil) != nil {
		t.Fatal("nil error was not preserved")
	}
}

func TestNewAuthStateNeutralStatusError(t *testing.T) {
	cause := errors.New("gzip: invalid header")
	err := NewAuthStateNeutralStatusError(cause, http.StatusBadGateway)
	if err == nil {
		t.Fatal("nil error")
	}
	if !errors.Is(err, cause) {
		t.Fatalf("wrapped cause lost: %v", err)
	}
	var neutral interface{ AuthStateNeutral() bool }
	if !errors.As(err, &neutral) || !neutral.AuthStateNeutral() {
		t.Fatalf("not auth-state-neutral: %v", err)
	}
	var withStatus interface{ StatusCode() int }
	if !errors.As(err, &withStatus) || withStatus.StatusCode() != http.StatusBadGateway {
		t.Fatalf("upstream status lost: %v", err)
	}
	if !strings.Contains(err.Error(), "502") || !strings.Contains(err.Error(), "invalid header") {
		t.Fatalf("diagnostic message = %q", err.Error())
	}
	if got := NewAuthStateNeutralStatusError(nil, http.StatusBadGateway); got != nil {
		t.Fatalf("nil cause = %v, want nil", got)
	}
}
