package cliproxy

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/config"
)

func TestAmpThreadActorProxySettingsDefaultsWhenAmpUpstreamConfigured(t *testing.T) {
	cfg := &config.Config{}
	cfg.AmpCode.UpstreamURL = "https://ampcode.com"

	enabled, addr, upstream := ampThreadActorProxySettings(cfg)
	if !enabled {
		t.Fatal("expected proxy to be enabled")
	}
	if addr != defaultAmpThreadActorProxyAddr {
		t.Fatalf("unexpected addr: %q", addr)
	}
	if !strings.Contains(upstream, "actors.ampcode.com") {
		t.Fatalf("unexpected upstream: %q", upstream)
	}
}

func TestAmpThreadActorProxySettingsCanDisable(t *testing.T) {
	disabled := false
	cfg := &config.Config{}
	cfg.AmpCode.UpstreamURL = "https://ampcode.com"
	cfg.AmpCode.ThreadActorProxyEnabled = &disabled

	enabled, _, _ := ampThreadActorProxySettings(cfg)
	if enabled {
		t.Fatal("expected proxy to be disabled")
	}
}

func TestAmpThreadActorProxySettingsYieldsToNeoLocalRuntime(t *testing.T) {
	neoEnabled := true
	cfg := &config.Config{}
	cfg.AmpCode.UpstreamURL = "https://ampcode.com"
	cfg.AmpCode.NeoLocalRuntime.Enabled = &neoEnabled

	enabled, _, _ := ampThreadActorProxySettings(cfg)
	if enabled {
		t.Fatal("expected proxy to be disabled when the Neo local runtime owns the actor endpoint")
	}
}

func TestAmpThreadActorProxySettingsRunsWhenNeoLocalRuntimeDisabled(t *testing.T) {
	neoEnabled := false
	cfg := &config.Config{}
	cfg.AmpCode.UpstreamURL = "https://ampcode.com"
	cfg.AmpCode.NeoLocalRuntime.Enabled = &neoEnabled

	enabled, _, _ := ampThreadActorProxySettings(cfg)
	if !enabled {
		t.Fatal("expected proxy to be enabled when the Neo local runtime is disabled")
	}
}

func TestAmpThreadActorProxyForwardsRequests(t *testing.T) {
	var gotPath string
	var gotQuery string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()

	proxy, err := newAmpThreadActorProxy("127.0.0.1:0", upstream.URL)
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := proxy.Start(ctx); err != nil {
		t.Fatalf("start proxy: %v", err)
	}
	defer func() {
		if err := proxy.Shutdown(context.Background()); err != nil {
			t.Fatalf("shutdown proxy: %v", err)
		}
	}()

	resp, err := http.Get("http://" + proxy.addr + "/metadata?x=1")
	if err != nil {
		t.Fatalf("get through proxy: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: %d body=%s", resp.StatusCode, body)
	}
	if string(body) != `{"ok":true}` {
		t.Fatalf("unexpected body: %s", body)
	}
	if gotPath != "/metadata" {
		t.Fatalf("unexpected upstream path: %q", gotPath)
	}
	if gotQuery != "x=1" {
		t.Fatalf("unexpected upstream query: %q", gotQuery)
	}
}

func TestAmpThreadActorProxySendsRivetTokenFromUpstreamURL(t *testing.T) {
	var gotAuth string
	var gotRivetToken string
	var gotQueryToken string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotRivetToken = r.Header.Get(rivetTokenHeader)
		gotQueryToken = r.URL.Query().Get("rvt-token")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer upstream.Close()

	upstreamURL, err := url.Parse(upstream.URL)
	if err != nil {
		t.Fatalf("parse upstream url: %v", err)
	}
	upstreamURL.User = url.UserPassword("default", "gateway-token")

	proxy, err := newAmpThreadActorProxy("127.0.0.1:0", upstreamURL.String())
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}
	if strings.Contains(proxy.upstream, "gateway-token") {
		t.Fatalf("proxy upstream leaked password: %q", proxy.upstream)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := proxy.Start(ctx); err != nil {
		t.Fatalf("start proxy: %v", err)
	}
	defer func() {
		if err := proxy.Shutdown(context.Background()); err != nil {
			t.Fatalf("shutdown proxy: %v", err)
		}
	}()

	resp, err := http.Get("http://" + proxy.addr + "/gateway/threadActor/?rvt-method=getOrCreate")
	if err != nil {
		t.Fatalf("get through proxy: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	}
	if gotAuth != "Bearer gateway-token" {
		t.Fatalf("unexpected auth header: %q", gotAuth)
	}
	if gotRivetToken != "gateway-token" {
		t.Fatalf("unexpected rivet token header: %q", gotRivetToken)
	}
	if gotQueryToken != "gateway-token" {
		t.Fatalf("unexpected rivet query token: %q", gotQueryToken)
	}
}

func TestAmpThreadActorProxyAddsRivetWebSocketProtocolToken(t *testing.T) {
	var gotProtocol string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotProtocol = r.Header.Get("Sec-WebSocket-Protocol")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer upstream.Close()

	upstreamURL, err := url.Parse(upstream.URL)
	if err != nil {
		t.Fatalf("parse upstream url: %v", err)
	}
	upstreamURL.User = url.UserPassword("default", "gateway-token")

	proxy, err := newAmpThreadActorProxy("127.0.0.1:0", upstreamURL.String())
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := proxy.Start(ctx); err != nil {
		t.Fatalf("start proxy: %v", err)
	}
	defer func() {
		if err := proxy.Shutdown(context.Background()); err != nil {
			t.Fatalf("shutdown proxy: %v", err)
		}
	}()

	req, err := http.NewRequest(http.MethodGet, "http://"+proxy.addr+"/connect", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Sec-WebSocket-Protocol", "rivet, rivet_encoding.bare")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get through proxy: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	}
	if gotProtocol != "rivet, rivet_encoding.bare, rivet_token.gateway-token" {
		t.Fatalf("unexpected websocket protocol: %q", gotProtocol)
	}
}

func TestAmpThreadActorProxyUpstreamFromBytes(t *testing.T) {
	data := `prefix,H99="https://default:token@actors.ampcode.com",M99="https://staging"`
	got := ampThreadActorProxyUpstreamFromReader(strings.NewReader(data))
	if got != "https://default:token@actors.ampcode.com" {
		t.Fatalf("unexpected upstream: %q", got)
	}
}

func TestAmpThreadActorProxyUpstreamFromBytesDoesNotRequireMinifiedVariableName(t *testing.T) {
	data := `prefix,Qx="https://default:token@actors.ampcode.com",M99="https://staging"`
	got := ampThreadActorProxyUpstreamFromReader(strings.NewReader(data))
	if got != "https://default:token@actors.ampcode.com" {
		t.Fatalf("unexpected upstream: %q", got)
	}
}

func TestAmpThreadActorProxyUpstreamRejectsHostSuffix(t *testing.T) {
	data := `x="https://default:token@actors.ampcode.com.evil.example"`
	got := ampThreadActorProxyUpstreamFromReader(strings.NewReader(data))
	if got != "" {
		t.Fatalf("expected no upstream for host with suffix, got %q", got)
	}
}

func TestAmpThreadActorProxyRejectsNonLoopbackAddr(t *testing.T) {
	for _, addr := range []string{"0.0.0.0:6420", ":6420", "[::]:6420", "192.168.1.10:6420", "example.com:6420"} {
		if _, err := newAmpThreadActorProxy(addr, "https://actors.ampcode.com"); err == nil {
			t.Fatalf("expected loopback rejection for addr %q", addr)
		}
	}
	for _, addr := range []string{"127.0.0.1:6420", "localhost:6420", "[::1]:6420"} {
		if _, err := newAmpThreadActorProxy(addr, "https://actors.ampcode.com"); err != nil {
			t.Fatalf("expected loopback addr %q to be accepted: %v", addr, err)
		}
	}
}

func TestAmpThreadActorProxyAddsGatewayTokenForExactGatewayPath(t *testing.T) {
	var gotQueryToken string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQueryToken = r.URL.Query().Get("rvt-token")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer upstream.Close()

	upstreamURL, err := url.Parse(upstream.URL)
	if err != nil {
		t.Fatalf("parse upstream url: %v", err)
	}
	upstreamURL.User = url.UserPassword("default", "gateway-token")

	proxy, err := newAmpThreadActorProxy("127.0.0.1:0", upstreamURL.String())
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := proxy.Start(ctx); err != nil {
		t.Fatalf("start proxy: %v", err)
	}
	defer func() {
		if err := proxy.Shutdown(context.Background()); err != nil {
			t.Fatalf("shutdown proxy: %v", err)
		}
	}()

	resp, err := http.Get("http://" + proxy.addr + "/gateway?rvt-method=listActors")
	if err != nil {
		t.Fatalf("get through proxy: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	}
	if gotQueryToken != "gateway-token" {
		t.Fatalf("unexpected rivet query token for exact /gateway path: %q", gotQueryToken)
	}
}

func TestAmpThreadActorProxyRewritesSameHostOrigin(t *testing.T) {
	var gotOrigin string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotOrigin = r.Header.Get("Origin")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer upstream.Close()

	proxy, err := newAmpThreadActorProxy("127.0.0.1:0", upstream.URL)
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := proxy.Start(ctx); err != nil {
		t.Fatalf("start proxy: %v", err)
	}
	defer func() {
		if err := proxy.Shutdown(context.Background()); err != nil {
			t.Fatalf("shutdown proxy: %v", err)
		}
	}()

	_, port, err := net.SplitHostPort(proxy.addr)
	if err != nil {
		t.Fatalf("split proxy addr: %v", err)
	}
	req, err := http.NewRequest(http.MethodGet, "http://"+proxy.addr+"/metadata", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Origin", "http://127.0.0.1:"+port)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get through proxy: %v", err)
	}
	defer resp.Body.Close()

	upstreamURL, _ := url.Parse(upstream.URL)
	if gotOrigin != upstreamURL.Scheme+"://"+upstreamURL.Host {
		t.Fatalf("expected origin rewritten to upstream, got %q", gotOrigin)
	}
}

func TestAmpThreadActorProxyDoesNotRewriteCrossAliasOrigin(t *testing.T) {
	var gotOrigin string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotOrigin = r.Header.Get("Origin")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer upstream.Close()

	proxy, err := newAmpThreadActorProxy("127.0.0.1:0", upstream.URL)
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := proxy.Start(ctx); err != nil {
		t.Fatalf("start proxy: %v", err)
	}
	defer func() {
		if err := proxy.Shutdown(context.Background()); err != nil {
			t.Fatalf("shutdown proxy: %v", err)
		}
	}()

	_, port, err := net.SplitHostPort(proxy.addr)
	if err != nil {
		t.Fatalf("split proxy addr: %v", err)
	}
	req, err := http.NewRequest(http.MethodGet, "http://"+proxy.addr+"/metadata", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Origin", "http://localhost:"+port)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get through proxy: %v", err)
	}
	defer resp.Body.Close()

	origin := "http://localhost:" + port
	if gotOrigin != origin {
		t.Fatalf("expected cross-alias origin preserved, got %q", gotOrigin)
	}
}

func TestAmpThreadActorProxyDoesNotRewriteForeignOrigin(t *testing.T) {
	var gotOrigin string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotOrigin = r.Header.Get("Origin")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer upstream.Close()

	proxy, err := newAmpThreadActorProxy("127.0.0.1:0", upstream.URL)
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := proxy.Start(ctx); err != nil {
		t.Fatalf("start proxy: %v", err)
	}
	defer func() {
		if err := proxy.Shutdown(context.Background()); err != nil {
			t.Fatalf("shutdown proxy: %v", err)
		}
	}()

	req, err := http.NewRequest(http.MethodGet, "http://"+proxy.addr+"/metadata", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Origin", "http://127.0.0.1:9999")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get through proxy: %v", err)
	}
	defer resp.Body.Close()

	if gotOrigin != "http://127.0.0.1:9999" {
		t.Fatalf("expected foreign origin preserved, got %q", gotOrigin)
	}
}

func TestAmpThreadActorProxyShutdownClosesHijackedConnections(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			http.Error(w, "no hijack", http.StatusInternalServerError)
			return
		}
		conn, _, err := hijacker.Hijack()
		if err != nil {
			return
		}
		_, _ = conn.Write([]byte("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n"))
		buf := make([]byte, 1)
		for {
			if _, err := conn.Read(buf); err != nil {
				_ = conn.Close()
				return
			}
		}
	}))
	defer upstream.Close()

	proxy, err := newAmpThreadActorProxy("127.0.0.1:0", upstream.URL)
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := proxy.Start(ctx); err != nil {
		t.Fatalf("start proxy: %v", err)
	}

	conn, err := net.Dial("tcp", proxy.addr)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer conn.Close()
	_, _ = fmt.Fprintf(conn, "GET /connect HTTP/1.1\r\nHost: %s\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n", proxy.addr)

	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("read upgrade response: %v", err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	}

	shutdownDone := make(chan error, 1)
	go func() {
		shutdownDone <- proxy.Shutdown(context.Background())
	}()
	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatalf("shutdown proxy: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("shutdown blocked on hijacked connection")
	}

	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1)
	if _, err := conn.Read(buf); err == nil {
		t.Fatal("expected client connection closed after proxy shutdown")
	}
}

func TestAmpThreadActorProxyRepeatedShutdown(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer upstream.Close()

	proxy, err := newAmpThreadActorProxy("127.0.0.1:0", upstream.URL)
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := proxy.Start(ctx); err != nil {
		t.Fatalf("start proxy: %v", err)
	}

	for i := 0; i < 3; i++ {
		done := make(chan error, 1)
		go func() {
			done <- proxy.Shutdown(context.Background())
		}()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("shutdown %d: %v", i, err)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("shutdown %d blocked", i)
		}
	}
}

func TestAmpThreadActorProxyContextCancelStopsProxy(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer upstream.Close()

	proxy, err := newAmpThreadActorProxy("127.0.0.1:0", upstream.URL)
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	if err := proxy.Start(ctx); err != nil {
		t.Fatalf("start proxy: %v", err)
	}
	cancel()

	select {
	case <-proxy.done:
	case <-time.After(10 * time.Second):
		t.Fatal("proxy did not stop after context cancel")
	}

	if _, err := net.DialTimeout("tcp", proxy.addr, time.Second); err == nil {
		t.Fatal("expected listener closed after context cancel")
	}
}
