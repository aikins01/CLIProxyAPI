package helps

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/proxyutil"
	log "github.com/sirupsen/logrus"
)

const httpTransportCacheLimit = 32

type roundTripperCache struct {
	mu      sync.Mutex
	entries map[string]*retiringRoundTripper
	order   []string
	limit   int
}

type retiringRoundTripper struct {
	transport http.RoundTripper
	mu        sync.Mutex
	active    int
	retired   bool
}

type trackedResponseBody struct {
	io.ReadCloser
	once    sync.Once
	release func()
}

func (c *roundTripperCache) get(key string, build func() http.RoundTripper) http.RoundTripper {
	c.mu.Lock()
	if cached := c.entries[key]; cached != nil {
		c.touch(key)
		c.mu.Unlock()
		return cached
	}

	transport := build()
	if transport == nil {
		c.mu.Unlock()
		return nil
	}
	if c.entries == nil {
		c.entries = make(map[string]*retiringRoundTripper)
	}
	limit := c.limit
	if limit <= 0 {
		limit = httpTransportCacheLimit
	}
	var evicted *retiringRoundTripper
	if len(c.entries) >= limit && len(c.order) > 0 {
		evictKey := c.order[0]
		c.order = c.order[1:]
		evicted = c.entries[evictKey]
		delete(c.entries, evictKey)
	}
	cached := &retiringRoundTripper{transport: transport}
	c.entries[key] = cached
	c.order = append(c.order, key)
	c.mu.Unlock()

	if evicted != nil {
		evicted.retire()
	}
	return cached
}

func (c *roundTripperCache) touch(key string) {
	for i, cachedKey := range c.order {
		if cachedKey != key {
			continue
		}
		copy(c.order[i:], c.order[i+1:])
		c.order[len(c.order)-1] = key
		return
	}
	c.order = append(c.order, key)
}

func (t *retiringRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	t.mu.Lock()
	t.active++
	t.mu.Unlock()

	resp, err := t.transport.RoundTrip(req)
	if err != nil || resp == nil || resp.Body == nil {
		t.release()
		return resp, err
	}
	resp.Body = &trackedResponseBody{ReadCloser: resp.Body, release: t.release}
	return resp, nil
}

func (t *retiringRoundTripper) CloseIdleConnections() {
	if closer, ok := t.transport.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
}

func (t *retiringRoundTripper) retire() {
	t.mu.Lock()
	t.retired = true
	closeNow := t.active == 0
	t.mu.Unlock()
	if closeNow {
		t.CloseIdleConnections()
	}
}

func (t *retiringRoundTripper) release() {
	t.mu.Lock()
	t.active--
	closeNow := t.retired && t.active == 0
	t.mu.Unlock()
	if closeNow {
		t.CloseIdleConnections()
	}
}

func (b *trackedResponseBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err != nil {
		b.once.Do(b.release)
	}
	return n, err
}

func (b *trackedResponseBody) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(b.release)
	return err
}

var proxyHTTPTransportCache = roundTripperCache{limit: httpTransportCacheLimit}

// UnwrapCachedRoundTripper returns the transport wrapped by the shared transport cache.
func UnwrapCachedRoundTripper(transport http.RoundTripper) http.RoundTripper {
	if cached, ok := transport.(*retiringRoundTripper); ok {
		return cached.transport
	}
	return transport
}

type authStateNeutralError struct {
	err error
}

func NewAuthStateNeutralError(err error) error {
	if err == nil {
		return nil
	}
	return authStateNeutralError{err: err}
}

func (e authStateNeutralError) Error() string {
	return e.err.Error()
}

func (e authStateNeutralError) Unwrap() error {
	return e.err
}

func (e authStateNeutralError) AuthStateNeutral() bool {
	return true
}

type authStateNeutralStatusError struct {
	err    error
	status int
}

func NewAuthStateNeutralStatusError(err error, status int) error {
	if err == nil {
		return nil
	}
	return authStateNeutralStatusError{err: err, status: status}
}

func (e authStateNeutralStatusError) Error() string {
	return fmt.Sprintf("%s (status=%d)", e.err.Error(), e.status)
}

func (e authStateNeutralStatusError) Unwrap() error {
	return e.err
}

func (e authStateNeutralStatusError) AuthStateNeutral() bool {
	return true
}

func (e authStateNeutralStatusError) StatusCode() int {
	return e.status
}

func EffectiveProxyURL(cfg *config.Config, auth *cliproxyauth.Auth) string {
	if auth != nil {
		if proxyURL := strings.TrimSpace(auth.ProxyURL); proxyURL != "" {
			return proxyURL
		}
	}
	if cfg != nil {
		return strings.TrimSpace(cfg.ProxyURL)
	}
	return ""
}

// NewProxyAwareHTTPClient creates an HTTP client with proper proxy configuration priority:
// 1. Use auth.ProxyURL if configured (highest priority)
// 2. Use cfg.ProxyURL if auth proxy is not configured
// 3. Use RoundTripper from context if neither are configured
//
// Parameters:
//   - ctx: The context containing optional RoundTripper
//   - cfg: The application configuration
//   - auth: The authentication information
//   - timeout: The client timeout (0 means no timeout)
//
// Returns:
//   - *http.Client: An HTTP client with configured proxy or transport
func NewProxyAwareHTTPClient(ctx context.Context, cfg *config.Config, auth *cliproxyauth.Auth, timeout time.Duration) *http.Client {
	httpClient := &http.Client{}
	if timeout > 0 {
		httpClient.Timeout = timeout
	}

	proxyURL := EffectiveProxyURL(cfg, auth)

	// If we have a proxy URL configured, set up the transport
	if proxyURL != "" {
		transport := proxyHTTPTransportCache.get(proxyURL, func() http.RoundTripper {
			return buildProxyTransport(proxyURL)
		})
		if transport != nil {
			httpClient.Transport = transport
			return httpClient
		}
		// If proxy setup failed, log and fall through to context RoundTripper
		log.Debugf("failed to setup proxy from URL: %s, falling back to context transport", proxyURL)
	}

	// Priority 3: Use RoundTripper from context (typically from RoundTripperFor)
	if rt, ok := ctx.Value("cliproxy.roundtripper").(http.RoundTripper); ok && rt != nil {
		httpClient.Transport = rt
	}

	return httpClient
}

// buildProxyTransport creates an HTTP transport configured for the given proxy URL.
// It supports SOCKS5, HTTP, and HTTPS proxy protocols.
//
// Parameters:
//   - proxyURL: The proxy URL string (e.g., "socks5://user:pass@host:port", "http://host:port")
//
// Returns:
//   - *http.Transport: A configured transport, or nil if the proxy URL is invalid
func buildProxyTransport(proxyURL string) *http.Transport {
	transport, _, errBuild := proxyutil.BuildHTTPTransport(proxyURL)
	if errBuild != nil {
		log.Errorf("%v", errBuild)
		return nil
	}
	return transport
}
