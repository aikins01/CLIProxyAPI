package helps

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	utls "github.com/refraction-networking/utls"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/proxyutil"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/http2"
	"golang.org/x/net/proxy"
)

type pendingConn struct {
	done chan struct{}
	conn *http2.ClientConn
	err  error
}

// utlsRoundTripper implements http.RoundTripper with a Chrome uTLS
// fingerprint for hosts guarded by Cloudflare TLS fingerprinting.
type utlsRoundTripper struct {
	mu          sync.Mutex
	connections map[string]*http2.ClientConn
	pending     map[string]*pendingConn
	leases      map[*http2.ClientConn]int
	dialer      proxy.Dialer
}

func newUtlsRoundTripper(proxyURL string) *utlsRoundTripper {
	var dialer proxy.Dialer = proxy.Direct
	if proxyURL != "" {
		proxyDialer, mode, errBuild := buildUtlsProxyDialer(proxyURL)
		if errBuild != nil {
			log.Errorf("utls: failed to configure proxy dialer for %q: %v", proxyURL, errBuild)
		} else if mode != proxyutil.ModeInherit && proxyDialer != nil {
			dialer = proxyDialer
		}
	}
	return &utlsRoundTripper{
		connections: make(map[string]*http2.ClientConn),
		pending:     make(map[string]*pendingConn),
		dialer:      dialer,
	}
}

// buildUtlsProxyDialer resolves the configured proxy for the uTLS path.
// x/net/proxy only understands SOCKS URLs, so HTTP(S) proxies need an
// explicit CONNECT dialer to keep both the proxy and the browser
// fingerprint: dropping to proxy.Direct here would silently bypass the
// configured proxy for fingerprinted hosts.
func buildUtlsProxyDialer(raw string) (proxy.Dialer, proxyutil.Mode, error) {
	setting, errParse := proxyutil.Parse(raw)
	if errParse != nil {
		return nil, setting.Mode, errParse
	}
	switch setting.Mode {
	case proxyutil.ModeProxy:
		if setting.URL.Scheme == "http" || setting.URL.Scheme == "https" {
			return &httpConnectDialer{proxyURL: setting.URL}, setting.Mode, nil
		}
		dialer, errDialer := proxy.FromURL(setting.URL, proxy.Direct)
		if errDialer != nil {
			return nil, setting.Mode, fmt.Errorf("create proxy dialer failed: %w", errDialer)
		}
		return dialer, setting.Mode, nil
	case proxyutil.ModeDirect:
		return proxy.Direct, setting.Mode, nil
	default:
		return nil, setting.Mode, nil
	}
}

// httpConnectDialer tunnels TCP connections through an HTTP(S) proxy with
// CONNECT so the uTLS ClientHello reaches the target unmodified.
type httpConnectDialer struct {
	proxyURL *url.URL
}

func (d *httpConnectDialer) Dial(network, addr string) (net.Conn, error) {
	return d.DialContext(context.Background(), network, addr)
}

func (d *httpConnectDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	if network != "tcp" && network != "tcp4" && network != "tcp6" {
		return nil, fmt.Errorf("utls: http proxy does not support network %q", network)
	}
	proxyHost := d.proxyURL.Hostname()
	proxyPort := d.proxyURL.Port()
	if proxyPort == "" {
		switch d.proxyURL.Scheme {
		case "https":
			proxyPort = "443"
		default:
			proxyPort = "80"
		}
	}
	proxyAddr := net.JoinHostPort(proxyHost, proxyPort)
	conn, errDial := (&net.Dialer{}).DialContext(ctx, "tcp", proxyAddr)
	if errDial != nil {
		return nil, fmt.Errorf("utls: connect to http proxy: %w", errDial)
	}
	closeOnError := func(err error) (net.Conn, error) {
		_ = conn.Close()
		return nil, err
	}
	if d.proxyURL.Scheme == "https" {
		serverName := d.proxyURL.Hostname()
		tlsConn := tls.Client(conn, &tls.Config{ServerName: serverName, MinVersion: tls.VersionTLS12})
		if ctx.Done() != nil {
			stop := context.AfterFunc(ctx, func() { _ = tlsConn.Close() })
			err := tlsConn.Handshake()
			if !stop() {
				_ = tlsConn.Close()
				return nil, ctx.Err()
			}
			// stop() == true only means the callback had not run yet; the
			// context may still have been canceled concurrently with the
			// handshake. Prefer the context error over any I/O error it
			// caused so callers and coalesced waiters classify it as
			// cancellation.
			if ctx.Err() != nil {
				_ = tlsConn.Close()
				return nil, ctx.Err()
			}
			if err != nil {
				return closeOnError(fmt.Errorf("utls: tls handshake with http proxy: %w", err))
			}
		} else if err := tlsConn.Handshake(); err != nil {
			return closeOnError(fmt.Errorf("utls: tls handshake with http proxy: %w", err))
		}
		conn = tlsConn
	}
	connectReq := &http.Request{
		Method: http.MethodConnect,
		URL:    &url.URL{Opaque: addr},
		Host:   addr,
		Header: http.Header{"User-Agent": {"CLIProxyAPI"}},
	}
	if d.proxyURL.User != nil {
		password, _ := d.proxyURL.User.Password()
		token := base64.StdEncoding.EncodeToString([]byte(d.proxyURL.User.Username() + ":" + password))
		connectReq.Header.Set("Proxy-Authorization", "Basic "+token)
	}
	if ctx.Done() != nil {
		stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
		err := connRoundTrip(conn, connectReq)
		if !stop() {
			return nil, ctx.Err()
		}
		// stop() == true only means the callback had not run yet; the
		// context may still have been canceled concurrently with the
		// exchange. Prefer the context error over any I/O error it
		// caused so callers classify it as cancellation.
		if ctx.Err() != nil {
			_ = conn.Close()
			return nil, ctx.Err()
		}
		if err != nil {
			return nil, err
		}
	} else if err := connRoundTrip(conn, connectReq); err != nil {
		return nil, err
	}
	return conn, nil
}

func connRoundTrip(conn net.Conn, req *http.Request) error {
	if err := req.Write(conn); err != nil {
		_ = conn.Close()
		return fmt.Errorf("utls: write CONNECT request: %w", err)
	}
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, req)
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("utls: read CONNECT response: %w", err)
	}
	// Any 2xx response establishes the tunnel (RFC 7231 §4.3.6). The
	// response body IS the tunnel from here on, so it must not be
	// closed; on failure the whole connection is closed instead.
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// Do not close the body first: an unframed rejection body ends
		// only at EOF, so draining it could block until the proxy
		// closes the socket. Closing the connection releases both.
		_ = conn.Close()
		return fmt.Errorf("utls: http proxy CONNECT failed: %s", resp.Status)
	}
	if br.Buffered() > 0 {
		// The proxy already sent tunnel data; the buffered bytes cannot
		// be spliced back onto conn, so the tunnel would be corrupted.
		_ = conn.Close()
		return fmt.Errorf("utls: http proxy sent %d bytes before CONNECT completed", br.Buffered())
	}
	return nil
}

func (t *utlsRoundTripper) getOrCreateConnection(ctx context.Context, host, addr string) (*http2.ClientConn, error) {
	t.mu.Lock()
	if h2Conn, ok := t.connections[addr]; ok && h2Conn.CanTakeNewRequest() {
		t.leaseLocked(h2Conn)
		t.mu.Unlock()
		return h2Conn, nil
	}

	if p, ok := t.pending[addr]; ok {
		t.mu.Unlock()
		// Wait without holding the mutex so a canceled coalesced request
		// returns on its own context instead of the creator's. When both
		// become ready concurrently, prefer the waiter's own
		// cancellation: the creator's result is irrelevant to a waiter
		// whose context is done.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-p.done:
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
		}
		if p.err == nil && p.conn != nil && p.conn.CanTakeNewRequest() {
			t.mu.Lock()
			// Re-validate under the mutex: CloseIdleConnections may have
			// removed and closed the connection since CanTakeNewRequest
			// was checked.
			if cached, ok := t.connections[addr]; ok && cached == p.conn {
				t.leaseLocked(p.conn)
				t.mu.Unlock()
				return p.conn, nil
			}
			t.mu.Unlock()
			return t.getOrCreateConnection(ctx, host, addr)
		}
		if p.err != nil {
			// A creator that was canceled or timed out proves nothing
			// about this waiter's context; retry under the waiter's own
			// context instead of inheriting the creator's failure.
			// Non-context errors (bad credentials, unreachable host)
			// are shared.
			if ctx.Err() == nil && (errors.Is(p.err, context.Canceled) || errors.Is(p.err, context.DeadlineExceeded)) {
				return t.getOrCreateConnection(ctx, host, addr)
			}
			return nil, p.err
		}
		return t.getOrCreateConnection(ctx, host, addr)
	}

	p := &pendingConn{done: make(chan struct{})}
	t.pending[addr] = p
	t.mu.Unlock()

	h2Conn, err := t.createConnection(ctx, host, addr)

	t.mu.Lock()
	delete(t.pending, addr)
	if err == nil {
		// A stale cached connection cannot serve new requests but may
		// still carry active streams or outstanding leases; drain it
		// once nothing uses it instead of dropping its only reference.
		if old, ok := t.connections[addr]; ok && old != h2Conn {
			go t.drainWhenIdle(old)
		}
		// Register the new connection with one lease held by the creator
		// so CloseIdleConnections cannot close it before the first
		// stream starts.
		t.connections[addr] = h2Conn
		t.leaseLocked(h2Conn)
	}
	t.mu.Unlock()

	p.conn = h2Conn
	p.err = err
	close(p.done)

	if err != nil {
		return nil, err
	}
	return h2Conn, nil
}

// leaseLocked marks an active stream on the connection so
// CloseIdleConnections skips it. Callers hold t.mu. The caller must
// release the lease with unlease once the stream is active (or the round
// trip has failed).
func (t *utlsRoundTripper) leaseLocked(conn *http2.ClientConn) {
	if t.leases == nil {
		t.leases = make(map[*http2.ClientConn]int)
	}
	t.leases[conn]++
}

func (t *utlsRoundTripper) unlease(conn *http2.ClientConn) {
	t.mu.Lock()
	if n, ok := t.leases[conn]; ok {
		if n <= 1 {
			delete(t.leases, conn)
		} else {
			t.leases[conn] = n - 1
		}
	}
	t.mu.Unlock()
}

// drainWhenIdle closes a connection removed from the cache once it has
// no active streams and no outstanding leases, so in-flight requests
// finish on it while new requests use a fresh connection.
func (t *utlsRoundTripper) drainWhenIdle(conn *http2.ClientConn) {
	for {
		t.mu.Lock()
		leased := t.leases[conn] > 0
		t.mu.Unlock()
		if !leased && conn.State().StreamsActive == 0 {
			_ = conn.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (t *utlsRoundTripper) createConnection(ctx context.Context, host, addr string) (*http2.ClientConn, error) {
	var conn net.Conn
	var err error
	// Connection setup must honor request cancellation; the credential and
	// stream clients intentionally carry no transport timeout, so a stalled
	// proxy or handshake would otherwise hang past the request's lifetime.
	if cd, ok := t.dialer.(proxy.ContextDialer); ok {
		conn, err = cd.DialContext(ctx, "tcp", addr)
	} else {
		// The dialer is not context-aware; dial in a background
		// goroutine that owns cleanup of any late connection. A
		// permanently stalled dial leaves that single goroutine blocked
		// on the dialer, matching any non-context-aware proxy.Dialer.
		type result struct {
			conn net.Conn
			err  error
		}
		// Unbuffered so a dial completing after the caller abandoned it
		// deterministically takes the cleanup branch.
		done := make(chan result)
		abandoned := make(chan struct{})
		go func() {
			c, e := t.dialer.Dial("tcp", addr)
			select {
			case done <- result{c, e}:
			case <-abandoned:
				if c != nil {
					_ = c.Close()
				}
			}
		}()
		select {
		case r := <-done:
			conn, err = r.conn, r.err
		case <-ctx.Done():
			close(abandoned)
			return nil, ctx.Err()
		}
	}
	if err != nil {
		return nil, err
	}

	tlsConfig := &utls.Config{ServerName: host}
	tlsConn := utls.UClient(conn, tlsConfig, utls.HelloChrome_Auto)

	if ctx.Done() != nil {
		stop := context.AfterFunc(ctx, func() { _ = tlsConn.Close() })
		err = tlsConn.Handshake()
		if !stop() {
			return nil, ctx.Err()
		}
		// stop() == true only means the callback had not run yet; the
		// context may still have been canceled concurrently with the
		// handshake. Prefer the context error over any I/O error it
		// caused so coalesced waiters retry instead of inheriting a
		// non-context error.
		if ctx.Err() != nil {
			_ = tlsConn.Close()
			return nil, ctx.Err()
		}
	} else {
		err = tlsConn.Handshake()
	}
	if err != nil {
		conn.Close()
		return nil, err
	}

	tr := &http2.Transport{}
	h2Conn, err := tr.NewClientConn(tlsConn)
	if err != nil {
		tlsConn.Close()
		return nil, err
	}

	return h2Conn, nil
}

func (t *utlsRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	hostname := req.URL.Hostname()
	port := req.URL.Port()
	if port == "" {
		port = "443"
	}
	addr := net.JoinHostPort(hostname, port)

	h2Conn, err := t.getOrCreateConnection(req.Context(), hostname, addr)
	if err != nil {
		return nil, err
	}
	defer t.unlease(h2Conn)

	resp, err := h2Conn.RoundTrip(req)
	if err != nil {
		// A per-request error such as context.Canceled does not make the
		// shared connection unusable. Only evict connections that can no
		// longer serve requests; otherwise the socket would leak, closed
		// neither here nor by CloseIdleConnections.
		if !h2Conn.CanTakeNewRequest() {
			// Drain instead of closing immediately: other streams or
			// outstanding leases may still be using the connection.
			t.mu.Lock()
			evicted := false
			if cached, ok := t.connections[addr]; ok && cached == h2Conn {
				delete(t.connections, addr)
				evicted = true
			}
			t.mu.Unlock()
			if evicted {
				go t.drainWhenIdle(h2Conn)
			}
		}
		return nil, err
	}

	return resp, nil
}

func (t *utlsRoundTripper) CloseIdleConnections() {
	t.mu.Lock()
	connections := make([]*http2.ClientConn, 0, len(t.connections))
	for addr, connection := range t.connections {
		if connection.State().StreamsActive != 0 || t.leases[connection] > 0 {
			continue
		}
		delete(t.connections, addr)
		connections = append(connections, connection)
	}
	t.mu.Unlock()
	for _, connection := range connections {
		_ = connection.Close()
	}
}

// browserFingerprintHosts are routed through the Chrome uTLS fingerprint.
// Cloudflare pins cf_clearance to the TLS/JA3 fingerprint; without a
// browser-matching fingerprint the session is challenged even with valid
// cookies.
var browserFingerprintHosts = map[string]struct{}{
	"api.anthropic.com": {},
	"chatgpt.com":       {},
	"www.chatgpt.com":   {},
}

var utlsHTTPTransportCache = roundTripperCache{limit: httpTransportCacheLimit}

// fallbackRoundTripper uses utls for fingerprinted HTTPS hosts and falls back
// to the standard transport for everything else.
type fallbackRoundTripper struct {
	utls     *utlsRoundTripper
	fallback http.RoundTripper
}

func (f *fallbackRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Scheme == "https" {
		hostname := strings.TrimSuffix(strings.ToLower(req.URL.Hostname()), ".")
		if _, ok := browserFingerprintHosts[hostname]; ok {
			return f.utls.RoundTrip(req)
		}
	}
	return f.fallback.RoundTrip(req)
}

func (f *fallbackRoundTripper) CloseIdleConnections() {
	f.utls.CloseIdleConnections()
	if closer, ok := f.fallback.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
}

// NewUtlsHTTPRoundTripper creates a shared Chrome-family uTLS transport.
func NewUtlsHTTPRoundTripper(cfg *config.Config, auth *cliproxyauth.Auth) http.RoundTripper {
	proxyURL := EffectiveProxyURL(cfg, auth)

	utlsRT := newUtlsRoundTripper(proxyURL)

	var standardTransport http.RoundTripper = &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
	}
	if proxyURL != "" {
		if transport := buildProxyTransport(proxyURL); transport != nil {
			standardTransport = transport
		}
	}

	return &fallbackRoundTripper{
		utls:     utlsRT,
		fallback: standardTransport,
	}
}

// NewUtlsHTTPClient creates an HTTP client that presents a Chrome uTLS
// fingerprint for HTTPS hosts in browserFingerprintHosts and falls back to
// the standard transport for non-HTTPS requests and for HTTPS hosts not in
// that list.
func NewUtlsHTTPClient(cfg *config.Config, auth *cliproxyauth.Auth, timeout time.Duration) *http.Client {
	proxyURL := EffectiveProxyURL(cfg, auth)
	transport := utlsHTTPTransportCache.get(proxyURL, func() http.RoundTripper {
		return NewUtlsHTTPRoundTripper(cfg, auth)
	})
	client := &http.Client{Transport: transport}
	if timeout > 0 {
		client.Timeout = timeout
	}
	return client
}
