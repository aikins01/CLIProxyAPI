package cliproxy

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

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
