package helps

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v7/sdk/config"
)

func TestNewProxyAwareHTTPClientDirectBypassesGlobalProxy(t *testing.T) {
	t.Parallel()

	client := NewProxyAwareHTTPClient(
		context.Background(),
		&config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "http://global-proxy.example.com:8080"}},
		&cliproxyauth.Auth{ProxyURL: "direct"},
		0,
	)

	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport type = %T, want *http.Transport", client.Transport)
	}
	if transport.Proxy != nil {
		t.Fatal("expected direct transport to disable proxy function")
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
