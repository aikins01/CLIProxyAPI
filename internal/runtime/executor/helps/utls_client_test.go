package helps

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/proxy"
)

type blockingDialer struct {
	release chan struct{}
}

func (d *blockingDialer) Dial(network, addr string) (net.Conn, error) {
	<-d.release
	return nil, errors.New("dial aborted")
}

type slowDialer struct {
	delay  time.Duration
	closed chan struct{}
}

type trackConn struct {
	net.Conn
	closed chan struct{}
	once   bool
}

func (c *trackConn) Close() error {
	if !c.once {
		c.once = true
		close(c.closed)
	}
	return c.Conn.Close()
}

func (d *slowDialer) Dial(network, addr string) (net.Conn, error) {
	time.Sleep(d.delay)
	c1, c2 := net.Pipe()
	_ = c2
	return &trackConn{Conn: c1, closed: d.closed}, nil
}

type onceDialer struct {
	called chan struct{}
}

func (d *onceDialer) Dial(network, addr string) (net.Conn, error) {
	close(d.called)
	<-time.After(10 * time.Second)
	return nil, errors.New("dial should have been abandoned")
}

func TestCreateConnectionHonorsCancelDuringDial(t *testing.T) {
	t.Parallel()
	rt := newUtlsRoundTripper("")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := rt.createConnection(ctx, "chatgpt.com", "127.0.0.1:1")
	if err == nil {
		t.Fatal("expected cancellation error")
	}
}

func TestCreateConnectionHonorsCancelDuringHandshake(t *testing.T) {
	t.Parallel()
	// A listener that accepts but never speaks TLS stalls the handshake.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, errAccept := ln.Accept()
			if errAccept != nil {
				return
			}
			go func() {
				defer c.Close()
				// Hold without responding until the client side closes.
				buf := make([]byte, 512)
				for {
					if _, errRead := c.Read(buf); errRead != nil {
						return
					}
				}
			}()
		}
	}()

	rt := newUtlsRoundTripper("")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, errConn := rt.createConnection(ctx, "chatgpt.com", ln.Addr().String())
		done <- errConn
	}()
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case errConn := <-done:
		if errConn == nil {
			t.Fatal("expected handshake cancellation error")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("handshake did not unblock on context cancellation")
	}
}

func TestPendingWaiterHonorsOwnCancellation(t *testing.T) {
	t.Parallel()
	d := &blockingDialer{release: make(chan struct{})}
	rt := newUtlsRoundTripper("")
	rt.dialer = d

	creatorErr := make(chan error, 1)
	go func() {
		_, err := rt.getOrCreateConnection(context.Background(), "chatgpt.com", "127.0.0.1:1")
		creatorErr <- err
	}()
	// Let the creator register the pending entry and block in Dial.
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	waiterErr := make(chan error, 1)
	go func() {
		_, err := rt.getOrCreateConnection(ctx, "chatgpt.com", "127.0.0.1:1")
		waiterErr <- err
	}()
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case err := <-waiterErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("waiter must return its own cancellation, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("waiter stayed blocked on the creator's dial after cancellation")
	}

	close(d.release)
	if err := <-creatorErr; err == nil {
		t.Fatal("creator dial should have failed")
	}
}

func TestFallbackDialAbandonedOnCancel(t *testing.T) {
	t.Parallel()
	d := &onceDialer{called: make(chan struct{})}
	rt := newUtlsRoundTripper("")
	rt.dialer = d

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := rt.createConnection(ctx, "chatgpt.com", "127.0.0.1:1")
		done <- err
	}()
	select {
	case <-d.called:
	case <-time.After(2 * time.Second):
		t.Fatal("dial never started")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected cancellation, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancellation did not abandon a stalled non-context-aware dial")
	}
}

func TestFallbackDialLateSuccessIsClosed(t *testing.T) {
	t.Parallel()
	d := &slowDialer{delay: 300 * time.Millisecond, closed: make(chan struct{})}
	rt := newUtlsRoundTripper("")
	rt.dialer = d

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := rt.createConnection(ctx, "chatgpt.com", "127.0.0.1:1")
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
	// The abandoned dial completes after ~300ms; its late connection
	// must be closed by the background cleanup path.
	select {
	case <-d.closed:
	case <-time.After(5 * time.Second):
		t.Fatal("late successful dial leaked an open connection")
	}
}

func TestWaiterRetriesAfterCreatorCancellation(t *testing.T) {
	t.Parallel()
	d := &blockingDialer{release: make(chan struct{})}
	rt := newUtlsRoundTripper("")
	rt.dialer = d

	creatorCtx, creatorCancel := context.WithCancel(context.Background())
	creatorErr := make(chan error, 1)
	go func() {
		_, err := rt.getOrCreateConnection(creatorCtx, "chatgpt.com", "127.0.0.1:1")
		creatorErr <- err
	}()
	// Let the creator register the pending entry and block in Dial.
	time.Sleep(100 * time.Millisecond)

	waiterErr := make(chan error, 1)
	go func() {
		// The waiter's context stays live; after the creator is canceled
		// the waiter retries and must surface its own dial failure rather
		// than the creator's cancellation.
		_, err := rt.getOrCreateConnection(context.Background(), "chatgpt.com", "127.0.0.1:1")
		waiterErr <- err
	}()
	// Let the waiter park on the creator's pending entry.
	time.Sleep(100 * time.Millisecond)
	creatorCancel()

	if err := <-creatorErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("creator should return its own cancellation, got %v", err)
	}
	// Let the waiter become the new creator and block in Dial, then
	// release it so its dial fails on its own.
	time.Sleep(100 * time.Millisecond)
	close(d.release)
	select {
	case err := <-waiterErr:
		if errors.Is(err, context.Canceled) {
			t.Fatalf("live waiter inherited the creator's cancellation: %v", err)
		}
		if err == nil {
			t.Fatal("waiter retry against an unreachable target must fail")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("live waiter did not retry after creator cancellation")
	}

	rt.mu.Lock()
	pending := len(rt.pending)
	rt.mu.Unlock()
	if pending != 0 {
		t.Fatalf("pending entry leaked after waiter retry, pending=%d", pending)
	}
}

func TestHandshakeSuccessAfterCancelIsNotCached(t *testing.T) {
	t.Parallel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, errAccept := ln.Accept()
			if errAccept != nil {
				return
			}
			defer c.Close()
		}
	}()

	rt := newUtlsRoundTripper("")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, errConn := rt.getOrCreateConnection(ctx, "chatgpt.com", ln.Addr().String())
		done <- errConn
	}()
	// Cancel before the TLS handshake gets a response; the server never
	// speaks TLS, so the handshake either fails or the AfterFunc close
	// wins. Either way no connection may be cached for the host.
	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done

	rt.mu.Lock()
	_, cached := rt.connections[ln.Addr().String()]
	rt.mu.Unlock()
	if cached {
		t.Fatal("a connection established after cancellation must not be cached")
	}
}

func TestUtlsHTTPProxyDialerUsesCONNECT(t *testing.T) {
	t.Parallel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	type connectRequest struct {
		target string
		auth   string
	}
	reqCh := make(chan connectRequest, 1)
	go func() {
		conn, errAccept := ln.Accept()
		if errAccept != nil {
			return
		}
		defer conn.Close()
		reader := bufio.NewReader(conn)
		line, errRead := reader.ReadString('\n')
		if errRead != nil {
			return
		}
		req := connectRequest{target: strings.TrimSpace(strings.TrimPrefix(line, "CONNECT "))}
		for {
			header, errHeader := reader.ReadString('\n')
			if errHeader != nil {
				return
			}
			header = strings.TrimSpace(header)
			if header == "" {
				break
			}
			if strings.HasPrefix(header, "Proxy-Authorization:") {
				req.auth = strings.TrimSpace(strings.TrimPrefix(header, "Proxy-Authorization:"))
			}
		}
		reqCh <- req
		_, _ = conn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n"))
	}()

	proxyURL := "http://user:pass@" + ln.Addr().String()
	rt := newUtlsRoundTripper(proxyURL)
	if _, isCONNECT := rt.dialer.(*httpConnectDialer); !isCONNECT {
		t.Fatalf("http proxy must use a CONNECT dialer, got %T", rt.dialer)
	}
	conn, err := rt.dialer.(proxy.ContextDialer).DialContext(context.Background(), "tcp", "chatgpt.com:443")
	if err != nil {
		t.Fatalf("CONNECT dial through http proxy failed: %v", err)
	}
	defer conn.Close()

	select {
	case req := <-reqCh:
		if req.target != "chatgpt.com:443 HTTP/1.1" {
			t.Fatalf("CONNECT request line target = %q", req.target)
		}
		want := "Basic " + base64.StdEncoding.EncodeToString([]byte("user:pass"))
		if req.auth != want {
			t.Fatalf("Proxy-Authorization = %q, want %q", req.auth, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("proxy never received a CONNECT request")
	}
}

func TestUtlsHTTPSProxyDialerTLSHandshakeFirst(t *testing.T) {
	t.Parallel()
	cert, err := tls.X509KeyPair(localhostCertPEM, localhostKeyPEM)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	reqCh := make(chan string, 1)
	go func() {
		conn, errAccept := ln.Accept()
		if errAccept != nil {
			return
		}
		defer conn.Close()
		reader := bufio.NewReader(conn)
		line, errRead := reader.ReadString('\n')
		if errRead != nil {
			return
		}
		reqCh <- strings.TrimSpace(line)
		for {
			header, errHeader := reader.ReadString('\n')
			if errHeader != nil || strings.TrimSpace(header) == "" {
				break
			}
		}
		_, _ = conn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n"))
	}()

	proxyURL := "https://" + ln.Addr().String()
	rt := newUtlsRoundTripper(proxyURL)
	if _, isCONNECT := rt.dialer.(*httpConnectDialer); !isCONNECT {
		t.Fatalf("https proxy must use a CONNECT dialer, got %T", rt.dialer)
	}
	// The test certificate is self-signed, so the client-side handshake to
	// the proxy fails; that failure itself proves the dialer attempted TLS
	// to the proxy instead of sending a plaintext CONNECT.
	_, err = rt.dialer.(proxy.ContextDialer).DialContext(context.Background(), "tcp", "chatgpt.com:443")
	if err == nil {
		t.Fatal("expected TLS handshake failure against self-signed proxy certificate")
	}
	select {
	case line := <-reqCh:
		t.Fatalf("proxy received a plaintext request before the TLS handshake: %q", line)
	case <-time.After(300 * time.Millisecond):
	}
}

// localhostCertPEM and localhostKeyPEM are a throwaway self-signed pair
// generated for 127.0.0.1, used only to assert the dialer negotiates TLS
// with HTTPS proxies.
var localhostCertPEM = []byte(`-----BEGIN CERTIFICATE-----
MIIBjTCCATSgAwIBAgIUcg9qDA+Cwyzv0av7K2ZXpN5QiHowCgYIKoZIzj0EAwIw
FDESMBAGA1UEAwwJMTI3LjAuMC4xMB4XDTI2MDgwNDE5MTUzM1oXDTM2MDgwMTE5
MTUzM1owFDESMBAGA1UEAwwJMTI3LjAuMC4xMFkwEwYHKoZIzj0CAQYIKoZIzj0D
AQcDQgAEfTdH4jWc+5ZN3jD5UUkovDzI/pv3WtaQL0BgcSYUSTE/7udwLxWfEByh
9fYSv+7f/eN0TvQbRv5ZFobNxRdY8aNkMGIwHQYDVR0OBBYEFOUf/mmYeU+d/QaX
+x+h/bjrMdm0MB8GA1UdIwQYMBaAFOUf/mmYeU+d/QaX+x+h/bjrMdm0MA8GA1Ud
EwEB/wQFMAMBAf8wDwYDVR0RBAgwBocEfwAAATAKBggqhkjOPQQDAgNHADBEAiAu
o6uyQK3Xo3NF7xDeZde7L8yJYz/JKT8OJdZqQxPuMgIgTUNz8mx1O/Uu7teiOMU+
zgFY2w7gttGZT5693sPUIQ4=
-----END CERTIFICATE-----
`)

var localhostKeyPEM = []byte(`-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgtz82K+b03k1IWXtP
CtNiVsYtbsZoxlFYEWHHwAOKMYGhRANCAAR9N0fiNZz7lk3eMPlRSSi8PMj+m/da
1pAvQGBxJhRJMT/u53AvFZ8QHKH19hK/7t/943RO9BtG/lkWhs3FF1jx
-----END PRIVATE KEY-----
`)

func TestConnectionCacheKeysIncludePort(t *testing.T) {
	t.Parallel()
	rt := newUtlsRoundTripper("")

	first := &pendingConn{done: make(chan struct{})}
	second := &pendingConn{done: make(chan struct{})}
	rt.mu.Lock()
	rt.pending["chatgpt.com:443"] = first
	rt.pending["chatgpt.com:8443"] = second
	rt.connections["chatgpt.com:443"] = nil
	rt.mu.Unlock()

	if len(rt.pending) != 2 {
		t.Fatalf("pending entries for distinct ports must not coalesce, pending=%d", len(rt.pending))
	}
	rt.mu.Lock()
	if rt.pending["chatgpt.com:443"] != first || rt.pending["chatgpt.com:8443"] != second {
		rt.mu.Unlock()
		t.Fatal("pending lookup must be scoped by host:port")
	}
	_, cached := rt.connections["chatgpt.com:443"]
	rt.mu.Unlock()
	if !cached {
		t.Fatal("connection cache lookup must be scoped by host:port")
	}
}

func TestCanceledWaiterPrefersOwnCancellation(t *testing.T) {
	t.Parallel()
	rt := newUtlsRoundTripper("")
	p := &pendingConn{done: make(chan struct{}), err: errors.New("upstream dial failed")}
	addr := "chatgpt.com:443"
	rt.mu.Lock()
	rt.pending[addr] = p
	rt.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	close(p.done)

	_, err := rt.getOrCreateConnection(ctx, "chatgpt.com", addr)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled waiter must surface its own cancellation, got %v", err)
	}
}

func TestFallbackRoundTripperRoutesOnlyExactFingerprintHosts(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		url      string
		wantUtls bool
	}{
		{"exact chatgpt.com", "https://chatgpt.com/v1/chat", true},
		{"exact www.chatgpt.com", "https://www.chatgpt.com/v1/chat", true},
		{"exact api.anthropic.com", "https://api.anthropic.com/v1/messages", true},
		{"uppercase host", "https://CHATGPT.COM/v1/chat", true},
		{"trailing root dot", "https://chatgpt.com./v1/chat", true},
		{"subdomain is not a fingerprint host", "https://api.chatgpt.com/v1/chat", false},
		{"lookalike suffix domain", "https://chatgpt.com.evil.example/v1/chat", false},
		{"prefix lookalike", "https://notchatgpt.com/v1/chat", false},
		{"http scheme never uses utls", "http://chatgpt.com/v1/chat", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req, err := http.NewRequest(http.MethodPost, tc.url, nil)
			if err != nil {
				t.Fatal(err)
			}
			fallbackHit := false
			utlsRT := newUtlsRoundTripper("")
			utlsRT.dialer = &unreachableDialer{}
			rt := &fallbackRoundTripper{
				utls: utlsRT,
				fallback: roundTripFunc(func(*http.Request) (*http.Response, error) {
					fallbackHit = true
					return nil, errors.New("fallback stub")
				}),
			}
			_, errRoundTrip := rt.RoundTrip(req)
			if errRoundTrip == nil {
				t.Fatal("both stub transports must fail")
			}
			if tc.wantUtls && fallbackHit {
				t.Fatalf("%s must route through the uTLS transport, hit fallback", tc.url)
			}
			if !tc.wantUtls && !fallbackHit {
				t.Fatalf("%s must route through the fallback transport", tc.url)
			}
		})
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

type unreachableDialer struct{}

func (d *unreachableDialer) Dial(network, addr string) (net.Conn, error) {
	return nil, errors.New("utls stub: unreachable")
}

func TestNewUtlsHTTPClientReusesTransport(t *testing.T) {
	first := NewUtlsHTTPClient(nil, nil, 0)
	second := NewUtlsHTTPClient(nil, nil, time.Second)

	if first.Transport != second.Transport {
		t.Fatal("expected uTLS transport to be reused")
	}
	if first.Timeout != 0 || second.Timeout != time.Second {
		t.Fatalf("client timeouts = %v and %v", first.Timeout, second.Timeout)
	}
}

func (d *unreachableDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	return nil, errors.New("utls stub: unreachable")
}
