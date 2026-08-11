package amp

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	"image/png"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/api/handlers"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

type failingWebLocalThreadResponseWriter struct {
	header     http.Header
	body       bytes.Buffer
	statusCode int
	limit      int
	writes     int
}

func (w *failingWebLocalThreadResponseWriter) Header() http.Header {
	return w.header
}

func (w *failingWebLocalThreadResponseWriter) WriteHeader(statusCode int) {
	w.statusCode = statusCode
}

func (w *failingWebLocalThreadResponseWriter) Write(data []byte) (int, error) {
	w.writes++
	remaining := max(0, w.limit-w.body.Len())
	if remaining == 0 {
		return 0, errors.New("response write failed")
	}
	written, _ := w.body.Write(data[:min(len(data), remaining)])
	if written != len(data) {
		return written, errors.New("response write failed")
	}
	return written, nil
}

func requireNeoBinaryV7ThreadID(t *testing.T, threadID string) {
	t.Helper()
	if !neoBinaryThreadIDExactPattern.MatchString(threadID) {
		t.Fatalf("threadId = %q, want binary UUID thread id", threadID)
	}
	parts := strings.Split(strings.TrimPrefix(threadID, "T-"), "-")
	if len(parts) != 5 || !strings.HasPrefix(parts[2], "7") {
		t.Fatalf("threadId = %q, want UUIDv7-shaped binary thread id", threadID)
	}
}

func firstNonLoopbackLocalIPForTest(t *testing.T) string {
	t.Helper()
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		t.Fatalf("interface addrs: %v", err)
	}
	for _, addr := range addrs {
		var ip net.IP
		switch typed := addr.(type) {
		case *net.IPNet:
			ip = typed.IP
		case *net.IPAddr:
			ip = typed.IP
		}
		if ip == nil || ip.IsLoopback() || ip.IsUnspecified() || ip.IsLinkLocalUnicast() {
			continue
		}
		if ip4 := ip.To4(); ip4 != nil {
			return ip4.String()
		}
		return ip.String()
	}
	return ""
}

func firstLinkLocalInterfaceAddrForTest(t *testing.T) (string, string) {
	t.Helper()
	interfaces, err := net.Interfaces()
	if err != nil {
		t.Fatalf("interfaces: %v", err)
	}
	for _, iface := range interfaces {
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			var ip net.IP
			switch typed := addr.(type) {
			case *net.IPNet:
				ip = typed.IP
			case *net.IPAddr:
				ip = typed.IP
			}
			if ip != nil && ip.To4() == nil && ip.IsLinkLocalUnicast() {
				return ip.String(), iface.Name
			}
		}
	}
	return "", ""
}

func withLocalTCPAddrForTest(req *http.Request, addr *net.TCPAddr) *http.Request {
	return req.WithContext(context.WithValue(req.Context(), http.LocalAddrContextKey, addr))
}

func TestRequestRemoteAddrIsLocalConnection(t *testing.T) {
	if requestRemoteAddrIsLocalConnection(nil) {
		t.Fatal("nil request was treated as local")
	}

	invalidReq := httptest.NewRequest(http.MethodGet, "/", nil)
	invalidReq.RemoteAddr = "not-an-ip"
	if requestRemoteAddrIsLocalConnection(invalidReq) {
		t.Fatal("invalid remote address was treated as local")
	}
	unspecifiedReq := httptest.NewRequest(http.MethodGet, "/", nil)
	unspecifiedReq.RemoteAddr = "0.0.0.0:12345"
	if requestRemoteAddrIsLocalConnection(unspecifiedReq) {
		t.Fatal("unspecified remote address was treated as local")
	}

	loopbackReq := httptest.NewRequest(http.MethodGet, "/", nil)
	loopbackReq.RemoteAddr = "127.0.0.1:12345"
	if !requestRemoteAddrIsLocalConnection(loopbackReq) {
		t.Fatal("loopback remote address was not treated as local")
	}
	ipv6LoopbackReq := httptest.NewRequest(http.MethodGet, "/", nil)
	ipv6LoopbackReq.RemoteAddr = "[::1]:12345"
	if !requestRemoteAddrIsLocalConnection(ipv6LoopbackReq) {
		t.Fatal("IPv6 loopback remote address was not treated as local")
	}
	mappedRemoteReq := httptest.NewRequest(http.MethodGet, "/", nil)
	mappedRemoteReq.RemoteAddr = "[::ffff:203.0.113.42]:12345"
	mappedRemoteReq = withLocalTCPAddrForTest(mappedRemoteReq, &net.TCPAddr{IP: net.ParseIP("192.0.2.10"), Port: 8317})
	if requestRemoteAddrIsLocalConnection(mappedRemoteReq) {
		t.Fatal("IPv4-mapped remote address was treated as local with a different local socket address")
	}

	scopedReq := httptest.NewRequest(http.MethodGet, "/", nil)
	scopedReq.RemoteAddr = "[fe80::1%en0]:12345"
	if ip := requestRemoteAddrIP(scopedReq); ip == nil || !ip.Equal(net.ParseIP("fe80::1")) {
		t.Fatalf("scoped IPv6 remote address parsed as %v", ip)
	}
	if linkLocalIP, ifaceName := firstLinkLocalInterfaceAddrForTest(t); linkLocalIP != "" && ifaceName != "" {
		linkLocalReq := httptest.NewRequest(http.MethodGet, "/", nil)
		linkLocalReq.RemoteAddr = net.JoinHostPort(linkLocalIP+"%"+ifaceName, "12345")
		linkLocalReq = withLocalTCPAddrForTest(linkLocalReq, &net.TCPAddr{IP: net.ParseIP(linkLocalIP), Port: 8317, Zone: ifaceName})
		if !requestRemoteAddrIsLocalConnection(linkLocalReq) {
			t.Fatalf("link-local interface address %s%%%s was not treated as local", linkLocalIP, ifaceName)
		}
	}

	remoteReq := httptest.NewRequest(http.MethodGet, "/", nil)
	remoteReq.RemoteAddr = "203.0.113.42:12345"
	if requestRemoteAddrIsLocalConnection(remoteReq) {
		t.Fatal("remote address was treated as local")
	}

	localIP := firstNonLoopbackLocalIPForTest(t)
	if localIP == "" {
		t.Skip("no non-loopback local IP available")
	}
	localReq := httptest.NewRequest(http.MethodGet, "/", nil)
	localReq.RemoteAddr = net.JoinHostPort(localIP, "12345")
	localReq = withLocalTCPAddrForTest(localReq, &net.TCPAddr{IP: net.ParseIP(localIP), Port: 8317})
	if !requestRemoteAddrIsLocalConnection(localReq) {
		t.Fatalf("local interface address %s was not treated as local", localIP)
	}
}

func TestRegisterManagementRoutes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	// Create module with proxy for testing
	m := &AmpModule{
		restrictToLocalhost: false, // disable localhost restriction for tests
	}

	// Create a mock proxy that tracks calls
	proxyCalled := false
	mockProxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxyCalled = true
		w.WriteHeader(200)
		w.Write([]byte("proxied"))
	}))
	defer mockProxy.Close()

	// Create real proxy to mock server
	proxy, _ := createReverseProxy(mockProxy.URL, NewStaticSecretSource(""))
	m.setProxy(proxy)

	base := &handlers.BaseAPIHandler{}
	m.registerManagementRoutes(r, base, nil)
	srv := httptest.NewServer(r)
	defer srv.Close()

	managementPaths := []struct {
		path   string
		method string
	}{
		{"/api/internal", http.MethodGet},
		{"/api/internal/some/path", http.MethodGet},
		{"/api/internal/github-proxy/repos/local", http.MethodGet},
		{"/api/user", http.MethodGet},
		{"/api/user/profile", http.MethodGet},
		{"/api/user-actor-credentials", http.MethodPost},
		{"/api/auth", http.MethodGet},
		{"/api/auth/login", http.MethodGet},
		{"/api/meta", http.MethodGet},
		{"/api/telemetry", http.MethodGet},
		{"/api/1.0/projects/local/project", http.MethodGet},
		{"/api/1.0/repos", http.MethodGet},
		{"/api/threads", http.MethodGet},
		{"/api/threads/T-019e65c0-0310-77a8-b233-4b84d9c0612b/messages/M-reader", http.MethodPost},
		{"/api/thread-actors", http.MethodPost},
		{"/threads/", http.MethodGet},
		{"/threads.rss", http.MethodGet}, // Root-level route (no /api prefix)
		{"/api/otel", http.MethodGet},
		{"/api/v2/spans", http.MethodPost},
		{"/api/tab", http.MethodGet},
		{"/api/tab/some/path", http.MethodGet},
		{"/api/durable-thread-workers/T-worker", http.MethodGet},
		{"/auth", http.MethodGet},           // Root-level auth route
		{"/auth/cli-login", http.MethodGet}, // CLI login flow
		{"/auth/callback", http.MethodGet},  // OAuth callback
		// Google v1beta1 bridge should still proxy non-model requests (GET) and allow POST
		{"/api/provider/google/v1beta1/models", http.MethodGet},
		{"/api/provider/google/v1beta1/models", http.MethodPost},
	}

	for _, path := range managementPaths {
		t.Run(path.path, func(t *testing.T) {
			proxyCalled = false
			req, err := http.NewRequest(path.method, srv.URL+path.path, nil)
			if err != nil {
				t.Fatalf("failed to build request: %v", err)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("request failed: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusNotFound {
				t.Fatalf("route %s not registered", path.path)
			}
			if !proxyCalled {
				t.Fatalf("proxy handler not called for %s", path.path)
			}
		})
	}
}

func TestLocalBrokerHeartbeatRouteOwnerIsolation(t *testing.T) {
	useTempNeoThreadStore(t)
	gin.SetMode(gin.TestMode)
	ownerByAuthorization := map[string]string{
		"Bearer upstream-a": "user_a",
		"Bearer upstream-b": "user_b",
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ownerUserID := ownerByAuthorization[r.Header.Get("Authorization")]
		if r.URL.Path != "/api/internal" || r.URL.RawQuery != "getUserInfo" || ownerUserID == "" {
			http.Error(w, "unexpected request", http.StatusUnauthorized)
			return
		}
		writeNeoJSON(w, http.StatusOK, map[string]any{"ok": true, "result": map[string]any{"id": ownerUserID}})
	}))
	t.Cleanup(upstream.Close)
	rt := newNeoRuntime(&config.Config{
		SDKConfig: config.SDKConfig{APIKeys: []string{"client-a", "client-b"}},
		AmpCode: config.AmpCode{
			UpstreamURL:    upstream.URL,
			UpstreamAPIKey: "upstream-a",
		},
	})
	mapped := NewMappedSecretSource(NewStaticSecretSource("upstream-a"))
	mapped.UpdateMappings([]config.AmpUpstreamAPIKeyEntry{{UpstreamAPIKey: "upstream-b", APIKeys: []string{"client-b"}}})
	rt.setSecretSource(mapped)
	m := &AmpModule{restrictToLocalhost: true, neoRuntime: rt}
	router := gin.New()
	auth := func(c *gin.Context) {
		token := strings.TrimSpace(strings.TrimPrefix(c.GetHeader("Authorization"), "Bearer "))
		if token != "client-a" && token != "client-b" {
			c.AbortWithStatus(http.StatusUnauthorized)
			return
		}
		c.Set("userApiKey", token)
		c.Next()
	}
	m.registerManagementRoutes(router, &handlers.BaseAPIHandler{}, auth)

	payload := func(brokerID, sessionID, runnerID, directory string) string {
		return fmt.Sprintf(`{"brokerId":%q,"sessionId":%q,"sessionGeneration":1,"hostname":"Mac","pid":1234,"runners":[{"runnerId":%q,"workingDirectory":%q,"repositoryURL":"","runningThreads":[]}]}`, brokerID, sessionID, runnerID, directory)
	}
	request := func(clientAPIKey, body, origin string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/ampcode/local-broker/heartbeat.json", strings.NewReader(body))
		req.RemoteAddr = "203.0.113.42:1234"
		req.Header.Set("Authorization", "Bearer "+clientAPIKey)
		req.Header.Set("Content-Type", "application/json; charset=utf-8")
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		return rec
	}
	if rec := request("client-a", payload("broker-a", "session-a", "local-runner-shared", "/Users/a/Developer/app"), ""); rec.Code != http.StatusOK {
		t.Fatalf("owner A heartbeat status = %d body=%s", rec.Code, rec.Body.String())
	}
	if rec := request("client-b", payload("broker-b", "session-b", "local-runner-shared", "/Users/b/Developer/app"), ""); rec.Code != http.StatusOK {
		t.Fatalf("owner B heartbeat status = %d body=%s", rec.Code, rec.Body.String())
	}
	runnersA := rt.store.userExecutorRunnersForOwner("user_a")
	runnersB := rt.store.userExecutorRunnersForOwner("user_b")
	if len(runnersA) != 1 || stringValue(mapValue(runnersA[0])["workingDirectory"]) != "/Users/a/Developer/app" {
		t.Fatalf("owner A runners = %#v", runnersA)
	}
	if len(runnersB) != 1 || stringValue(mapValue(runnersB[0])["workingDirectory"]) != "/Users/b/Developer/app" {
		t.Fatalf("owner B runners = %#v", runnersB)
	}
	if rec := request("client-a", payload("browser-broker", "browser-session", "browser-runner", "/Users/a/Developer/browser"), "https://ampcode.com"); rec.Code != http.StatusForbidden || rec.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatalf("browser-origin heartbeat status = %d CORS=%q body=%s", rec.Code, rec.Header().Get("Access-Control-Allow-Origin"), rec.Body.String())
	}
	unauthorizedReq := httptest.NewRequest(http.MethodPost, "/ampcode/local-broker/heartbeat.json", strings.NewReader(payload("broker-a", "session-a", "runner-a", "/Users/a/Developer/app")))
	unauthorizedReq.Header.Set("Content-Type", "application/json")
	unauthorizedRec := httptest.NewRecorder()
	router.ServeHTTP(unauthorizedRec, unauthorizedReq)
	if unauthorizedRec.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized heartbeat status = %d body=%s", unauthorizedRec.Code, unauthorizedRec.Body.String())
	}
}

func TestWebLocalInferenceCORSRequiresOptIn(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	m := &AmpModule{restrictToLocalhost: false}
	settings := config.AmpCode{}
	m.lastConfig = &settings
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	req := httptest.NewRequest(http.MethodOptions, "/api/thread-actors", nil)
	req.Header.Set("Origin", "https://ampcode.com")
	req.Header.Set("Access-Control-Request-Headers", "authorization, content-type, "+ampWebLocalInferenceHeader)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code == http.StatusNoContent {
		t.Fatal("disabled web-local-inference unexpectedly answered CORS preflight")
	}
	if allowOrigin := rec.Header().Get("Access-Control-Allow-Origin"); allowOrigin != "" {
		t.Fatalf("Access-Control-Allow-Origin = %q, want empty", allowOrigin)
	}
}

func TestWebLocalInferenceCORSAllowsOnlyConfiguredActorOriginsAndPaths(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	m := &AmpModule{restrictToLocalhost: false}
	settings := config.AmpCode{
		WebLocalInference: config.AmpWebLocalInference{
			Enabled:        true,
			AllowedOrigins: []string{"https://ampcode.com"},
		},
	}
	m.lastConfig = &settings
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	req := httptest.NewRequest(http.MethodOptions, "/api/thread-actors", nil)
	req.Header.Set("Origin", "https://ampcode.com")
	req.Header.Set("Access-Control-Request-Headers", "authorization, content-type, "+ampWebLocalInferenceHeader)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("enabled web-local-inference preflight status = %d, want %d", rec.Code, http.StatusNoContent)
	}
	if allowOrigin := rec.Header().Get("Access-Control-Allow-Origin"); allowOrigin != "https://ampcode.com" {
		t.Fatalf("Access-Control-Allow-Origin = %q, want https://ampcode.com", allowOrigin)
	}
	if allowHeaders := rec.Header().Get("Access-Control-Allow-Headers"); !strings.Contains(strings.ToLower(allowHeaders), strings.ToLower(ampWebLocalInferenceHeader)) {
		t.Fatalf("Access-Control-Allow-Headers = %q, want %s", allowHeaders, ampWebLocalInferenceHeader)
	}
	for _, header := range []string{"X-Rivet-Conn-Params", "X-Rivet-Encoding"} {
		if allowHeaders := rec.Header().Get("Access-Control-Allow-Headers"); !strings.Contains(strings.ToLower(allowHeaders), strings.ToLower(header)) {
			t.Fatalf("Access-Control-Allow-Headers = %q, want %s", allowHeaders, header)
		}
	}
	if allowCredentials := rec.Header().Get("Access-Control-Allow-Credentials"); allowCredentials != "true" {
		t.Fatalf("Access-Control-Allow-Credentials = %q, want true", allowCredentials)
	}
	if allowPrivateNetwork := rec.Header().Get("Access-Control-Allow-Private-Network"); allowPrivateNetwork != "true" {
		t.Fatalf("Access-Control-Allow-Private-Network = %q, want true", allowPrivateNetwork)
	}

	internalReq := httptest.NewRequest(http.MethodOptions, "/api/internal", nil)
	internalReq.Header.Set("Origin", "https://ampcode.com")
	internalReq.Header.Set("Access-Control-Request-Headers", "authorization, content-type, "+ampWebLocalInferenceHeader)
	internalRec := httptest.NewRecorder()
	r.ServeHTTP(internalRec, internalReq)
	if internalRec.Code != http.StatusNoContent {
		t.Fatalf("enabled web-local-inference internal preflight status = %d, want %d", internalRec.Code, http.StatusNoContent)
	}

	for _, path := range []string{"/metadata", "/actors/metadata", "/gateway/thread-actor/", "/_app/remote/3abror/createProjectThread", "/ampcode/local-projects.json", "/ampcode/local-project-details.json", "/ampcode/local-activity.json", "/ampcode/local-thread-search.json", "/ampcode/local-thread-data.json", "/api/threads/find"} {
		t.Run("root preflight "+path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodOptions, path, nil)
			req.Header.Set("Origin", "https://ampcode.com")
			req.Header.Set("Access-Control-Request-Headers", "authorization, content-type, "+ampWebLocalInferenceHeader)
			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, req)
			if rec.Code != http.StatusNoContent {
				t.Fatalf("enabled web-local-inference preflight status = %d, want %d", rec.Code, http.StatusNoContent)
			}
			if allowOrigin := rec.Header().Get("Access-Control-Allow-Origin"); allowOrigin != "https://ampcode.com" {
				t.Fatalf("Access-Control-Allow-Origin = %q, want https://ampcode.com", allowOrigin)
			}
		})
	}

	actualReq := httptest.NewRequest(http.MethodGet, "/actors?name=userActor&key=user_01JY3S65TF0JGK80KZ0NA9T18B&namespace=default", nil)
	actualReq.Header.Set("Origin", "https://ampcode.com")
	actualRec := httptest.NewRecorder()
	r.ServeHTTP(actualRec, actualReq)
	if allowOrigin := actualRec.Header().Get("Access-Control-Allow-Origin"); allowOrigin != "https://ampcode.com" {
		t.Fatalf("actual Access-Control-Allow-Origin = %q, want https://ampcode.com", allowOrigin)
	}
	if allowCredentials := actualRec.Header().Get("Access-Control-Allow-Credentials"); allowCredentials != "true" {
		t.Fatalf("actual Access-Control-Allow-Credentials = %q, want true", allowCredentials)
	}

	for _, tc := range []struct {
		name   string
		path   string
		origin string
	}{
		{name: "unowned path", path: "/api/user", origin: "https://ampcode.com"},
		{name: "nested remote near miss", path: "/_app/remote/3abror/nested/createProjectThread", origin: "https://ampcode.com"},
		{name: "unconfigured origin", path: "/api/thread-actors", origin: "https://example.com"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodOptions, tc.path, nil)
			req.Header.Set("Origin", tc.origin)
			req.Header.Set("Access-Control-Request-Headers", "authorization, content-type, "+ampWebLocalInferenceHeader)
			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, req)
			if rec.Code == http.StatusNoContent {
				t.Fatalf("%s unexpectedly answered CORS preflight", tc.name)
			}
			if allowOrigin := rec.Header().Get("Access-Control-Allow-Origin"); allowOrigin != "" {
				t.Fatalf("Access-Control-Allow-Origin = %q, want empty", allowOrigin)
			}
		})
	}
}

func TestWebLocalInferenceOriginAllowlistRequiresOriginOnly(t *testing.T) {
	if !ampWebLocalInferenceOriginAllowed("https://ampcode.com", nil) {
		t.Fatal("default ampcode.com origin was not allowed")
	}
	if ampWebLocalInferenceOriginAllowed("https://ampcode.com", []string{}) {
		t.Fatal("explicit empty allowed-origins should deny default origins")
	}
	if !ampWebLocalInferenceOriginAllowed("https://AMPCODE.com", []string{"https://ampcode.com"}) {
		t.Fatal("origin matching should be case-insensitive")
	}
	if ampWebLocalInferenceOriginAllowed("chrome-extension://abcdefghijklmnop", []string{"chrome-extension://abcdefghijklmnop"}) {
		t.Fatal("non-http origin was accepted")
	}
	for _, allowed := range []string{
		"https://ampcode.com/",
		"https://ampcode.com/path",
		"https://ampcode.com?env=dev",
		"https://ampcode.com#fragment",
	} {
		if ampWebLocalInferenceOriginAllowed("https://ampcode.com", []string{allowed}) {
			t.Fatalf("allowed origin %q was accepted despite path/query/fragment", allowed)
		}
	}
}

func TestWebLocalInferenceConfiguredBaseURLOriginIsAllowed(t *testing.T) {
	settings := config.AmpWebLocalInference{
		Enabled:        true,
		AllowedOrigins: []string{"https://ampcode.com"},
		BaseURL:        "https://aikinss-macbook-pro.taila39f5b.ts.net/",
	}
	if !ampWebLocalInferenceRequestOriginAllowed("https://aikinss-macbook-pro.taila39f5b.ts.net", settings) {
		t.Fatal("configured base URL origin was not allowed")
	}
	if ampWebLocalInferenceRequestOriginAllowed("https://example.com", settings) {
		t.Fatal("unconfigured origin was allowed")
	}
	settings.BaseURL = "https://aikinss-macbook-pro.taila39f5b.ts.net/?token=ignored#fragment"
	if !ampWebLocalInferenceRequestOriginAllowed("https://aikinss-macbook-pro.taila39f5b.ts.net", settings) {
		t.Fatal("sanitized configured base URL origin was not allowed")
	}
}

func TestWebLocalInferenceUserscriptRoute(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	m := &AmpModule{
		restrictToLocalhost: false,
		lastConfig: &config.AmpCode{
			WebLocalInference: config.AmpWebLocalInference{Enabled: true},
		},
	}
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	req := httptest.NewRequest(http.MethodGet, "/ampcode/local-inference.user.js", nil)
	req.Host = "127.0.0.1:8317"
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("userscript status = %d, want %d", rec.Code, http.StatusOK)
	}
	body := rec.Body.String()
	if strings.Contains(body, "%!") {
		t.Fatalf("userscript response contains fmt corruption marker:\n%s", body)
	}
	if got, want := rec.Header().Get("Content-Length"), strconv.Itoa(len(rec.Body.Bytes())); got != want {
		t.Fatalf("userscript Content-Length = %q, want %q", got, want)
	}
	if got := rec.Header().Get("Cache-Control"); got != "no-store, max-age=0" {
		t.Fatalf("userscript Cache-Control = %q, want no-store revalidation", got)
	}
	if got := rec.Header().Get("Pragma"); got != "no-cache" {
		t.Fatalf("userscript Pragma = %q, want no-cache", got)
	}
	for _, want := range []string{
		"// ==UserScript==",
		"@version 0.1.202",
		"@match https://ampcode.com/*",
		"@updateURL http://127.0.0.1:8317/ampcode/local-inference.user.js",
		"@downloadURL http://127.0.0.1:8317/ampcode/local-inference.user.js",
		"@sandbox raw",
		`"http://127.0.0.1:8317"`,
		`let defaultWorkingDirectory = "";`,
		"ensureDefaultWorkingDirectory",
		ampWebLocalInferenceHeader,
		"globalThis.JSON.parse",
		"decodedConfigPatchCount",
		"decodedGraphPassCount",
		"decodedGraphVisitCount",
		"menuIntegrationCount",
		"commandPaletteIntegrationCount",
		"localThreadPickerOpenCount",
		"removedLocalThreadControlCount",
		`const userscriptVersion = "0.1.202"`,
		"localThreadSearchEndpointPath",
		"fetchLocalThreadSearch",
		"mergeThreadSearchResponse",
		"localThreadSearchFetchCount",
		"localThreadSearchMergeCount",
		"captureAuthenticatedAmpUserFromBootstrap",
		"resolvedWorkingDirectory === defaultLocalWorkingDirectory()",
		`resolvedNoProject ? "No Project"`,
		"discoverLocalThreadID",
		"normalizeLocalThreadViewPath",
		`originalFetch(localBaseURLString() + "/api/thread-actors"`,
		`globalThis.document.body.appendChild(anchor)`,
		"shouldPatchSidebarResponseJSON",
		"mergeSidebarResponse",
		"appendDevalueSidebarValue",
		`url.searchParams.set("cliproxy-thread-id", threadID)`,
		"localSidebarThreadMergeCount",
		"localSidebarTitlePatchCount",
		"installLocalSidebarMetadataIntegration",
		"reconcileActiveLocalThreadTitle",
		"data-sidebar-thread-id",
		"userscriptVersion",
		"lastPatchedThreadActorBaseURL",
		"lastPatchedThreadID",
		`let pendingLocalBootstrapThreadID = "";`,
		"threadActorConfig",
		"plainThreadActorConfigHasBridgeFields",
		"devalueThreadActorConfigHasBridgeFields",
		"plainStandaloneThreadActorConfigLike",
		"devalueStandaloneThreadActorConfigLike",
		"plainThreadContainerLike",
		"devalueThreadContainerLike",
		"plainTranscriptOrToolValueLike",
		"plainThreadActorConfigValueNeedsLocality",
		"patchPlainThreadActorConfigValue",
		"patchDecodedLocalInferenceGraph",
		"for (const key in value)",
		"localPlainThreadActorConfig",
		"localDevalueThreadActorConfig",
		"plainContainerThreadID",
		"devalueContainerThreadID",
		"ensurePlainLocalThreadActorConfig",
		"ensureDevalueLocalThreadActorConfig",
		`wsToken: storedLocalAPIKey() || "local-neo"`,
		`threadActorTransport: "json-rpc"`,
		"local-client",
		"cliproxyapi.ampLocalInference.apiKey",
		"storedLocalAPIKey",
		"persistentLocalAPIKeyStorageKey",
		"globalThis.localStorage.getItem(persistentStorageKey)",
		`const promptedAPIKey = (globalThis.prompt("CLIProxyAPI API key") || "").trim()`,
		"rememberLocalAPIKey(promptedAPIKey)",
		"cliproxyapi.ampLocalInference.workingDirectory",
		"cliproxyapi.ampLocalInference.selectedLocalProject",
		"cliproxyapi.ampLocalInference.localThreadIDs",
		"cliproxyapi.ampLocalInference.threadWorkingDirectories",
		"cliproxyapi.ampLocalInference.threadSettings",
		"cliproxyapi.ampLocalInference.sidebarTitles.v3",
		"data-cliproxy-local-sidebar-hydrating",
		"data-cliproxy-local-inference-version",
		"scheduleLocalSidebarHydrationReveal",
		"requestLocalSidebarHydrationRefresh",
		"requestLocalSidebarProjectRegroup",
		"installHiddenPageSocketPause",
		"pauseLocalThreadSocketsForHiddenPage",
		"hiddenPageSocketPauseGraceMs",
		"localInferencePatchScanMaxChars",
		"scheduleIntegrateThreadMenus",
		"scheduleIntegrateCommandPalettes",
		"installHiddenPageSocketPause",
		"pauseLocalThreadSocketsForHiddenPage",
		"hiddenPageSocketPauseGraceMs",
		"localInferencePatchScanMaxChars",
		"scheduleIntegrateThreadMenus",
		"scheduleIntegrateCommandPalettes",
		"localSidebarProjectRegroupMismatchThreadIDs",
		"localSidebarProjectRegroupPendingThreadIDs",
		"localSidebarProjectRegroupStableSince",
		"localSidebarProjectRegroupStableTimer",
		"scheduleLocalSidebarProjectRegroupStability",
		"localSidebarProjectRegroupStabilityDelay",
		"newRegroupThreadIDs",
		"mergeReferencedSidebarProjects",
		"localSidebarPresentationThread",
		"sidebarPresentationThreadFields",
		"localSidebarProjectMergeCount",
		`globalThis.dispatchEvent(new Event("pageshow"))`,
		"/ampcode/local-projects.json",
		"localProjectsEndpointPath",
		"/ampcode/local-project-details.json",
		"localProjectDetailsEndpointPath",
		"openLocalProjectPage",
		"openMissingLocalProjectPage",
		"Commits to ",
		"Recent Threads",
		"/ampcode/local-activity.json",
		"localActivityEndpointPath",
		"/ampcode/local-thread-search.json",
		"fetchLocalActivity",
		"mergeActivityResponse",
		"mergeActivityFilterResponse",
		"activityFilterSearchQuery(response.url)",
		"rememberActivityFilterResponse",
		"activityFilterSourceURL",
		"installLocalActivityIntegration",
		"renderLocalActivityFilters",
		"localActivityDOMFilterIntegrationCount",
		"searchFeedRepositories",
		"searchFeedUsers",
		"searchThreads",
		"shouldPatchThreadSearchResponseJSON",
		"threadSearchResponseMissingMetadataIDs",
		"patchThreadSearchResponse",
		"localThreadSearchTitlePatchCount",
		"localThreadSearchProjectPatchCount",
		"threadSearchAPIPath",
		"/ampcode/local-thread-data.json",
		"localThreadDataEndpointPath",
		"localThreadResourceDataThreadID",
		"selectedLocalProjectWorkingDirectory",
		"currentLocalProjectWorkingDirectory",
		"rememberSelectedLocalProject",
		"clearSelectedLocalProject",
		"globalThis.localStorage.removeItem(workingDirectoryStorageKey)",
		"normalizeLocalProject",
		"fetchLocalProjects",
		"function fetchLocalProjects(promptForKey = false, additionalSidebarThreadIDs = [], force = false)",
		`const headers = localFetchHeaders("", false)`,
		"localProjectLookupAPIKey",
		"promptedProjectsAPIKey",
		"responseDefaultWorkingDirectory",
		"fetchLocalProjects(false)",
		"fetchLocalProjects(true)",
		"installLocalProjectPickerIntegration",
		"installLocalProjectPickerSearch",
		"filterLocalProjectPickerItems",
		"installLocalProjectNoProjectSelectionHandler",
		"refreshCreateThreadProjectActivatorsAfterSelection",
		"setLocalProjectPickerCurrentDirectory",
		"localProjectPickerLooksLikeProjectPicker",
		"if (!item || projectPickerItemSelected(item))",
		"closeLocalProjectPickerViaNoProject",
		"localProjectFetchCount",
		"localProjectFetchFailureCount",
		"lastLocalProjectFetchFailure",
		`localProjectsCache = { at: 0, projects: [], runners: [], threadID, thread: null, threads: [], threadTitles: Object.assign({}, localSidebarTitleCache), sidebarTitleKey: "", promise: null }`,
		"localProjectPickerIntegrationCount",
		"localProjectIntegrationGeneration",
		"projectMutationCandidateCount",
		"projectMutationIgnoredCount",
		"projectMutationCoalescedCount",
		"projectMutationFlushCount",
		"projectMutationPendingRootCount",
		"lastObservedThreadID",
		"observedThreadID",
		"threadID === pathThreadID()",
		"normalizeExplicitReasoningEffort",
		"lastInheritedWorkingDirectory",
		"remoteShellCreateCount",
		"executorType: localPayload.executorType",
		"lastLocalThreadAgentMode",
		"lastVisibleThreadModeBadge",
		"lastLocalThreadChoice",
		"cliproxy-api-key",
		"cliproxy-working-directory",
		"cliproxy-agent-mode",
		"cliproxy-reasoning-effort",
		"bridgeRequestBody",
		"bodyNeedsTextBridge",
		"responseThreadID",
		"reasoning.effort",
		"visibleThreadModeOptions",
		"modeOptionsFromBadgeText",
		"badgeLevelReasoningEffort",
		"localThreadModeChoices",
		"showLocalThreadPicker",
		"buildLocalThreadChoiceButton",
		"appendMenuItemChevron",
		"Deep 2",
		"deep-1",
		"deep-2",
		"deep-3",
		"removeStaleLocalThreadButton",
		"removeInjectedLocalThreadControls",
		"threadMenuLooksLikeThreadMenu",
		"commandPaletteLooksLikePalette",
		"stripCommandPaletteShortcut",
		`data-value", label`,
		"Generate Diagnostic Report",
		"plainThreadWorkingDirectory",
		"devalueThreadWorkingDirectory",
		"rememberPlainThreadRuntime",
		"rememberDevalueThreadRuntime",
		"rememberLoadedThreadBase",
		"loadedThreadBaseVersionLimit",
		"loadedThreadBaseVersionEntryCount",
		"rewriteClientResumePayload",
		"clientResumeRewriteCount",
		"TextDecoder",
		`.split(/[\\/]+/)`,
		"parsedTextLocalInferencePatchOptions",
		"localInferencePatchFieldPattern",
		"matches.size === 1",
		"internalAPIPath",
		"workingDirectory",
		`prompt("CLIProxyAPI API key")`,
		"function localAPIKey(rememberCancel = true)",
		`"Bearer " + apiKey`,
		"gatewayActorPath",
		"gatewayUserActorPath",
		"gatewayUserActorActionPath",
		`new Set(["registerRunner", "runnerHeartbeat", "unregisterRunner", "listRunners"])`,
		"localRunnerActionNames.has(parts[3])",
		`"user-actor"`,
		`.split("@")[0]`,
		"shouldBridgeUserActorWebSocket",
		"return !!storedLocalAPIKey();",
		"threadActorAPIPath",
		"threadActorCreatePath",
		"threadActorInstancePath",
		"svelteKitRemoteEndpoint",
		"svelteKitRemotePath",
		"localThreadMutationRemoteBody",
		"createProjectThreadRemotePath",
		"requestUsesLocalBridge",
		"localProjectCheckoutsByPath",
		"decorateLocalProjectList",
		"Amp Cloud",
		"Local checkout",
		"Local settings",
		"remoteCreateProjectThreadWorkingDirectory",
		"rememberRemoteCreateProjectThread",
		`if (!response || !response.ok || typeof response.clone !== "function")`,
		`if (workingDirectory)`,
		"/_app/remote/",
		"createProjectThread",
		"archiveThreadCommand",
		"deleteThreadCommand",
		"markThreadUnreadCommand",
		"pinThreadCommand",
		"prewarmProjectThread",
		"shouldBridgeHTTP",
		"pathThreadID",
		"request.clone().text()",
		`duplex = "half"`,
		"activeThreadID",
		"threadIDFromGatewayURL",
		"firstThreadIDFromValue",
		"firstThreadIDFromText",
		"rvt-input",
		"sameLocalHTTPBase",
		"samePageWebSocketBase",
		"sameLocalWebSocketBase",
		"diagnostics",
		"lastWebSocketBootstrapped",
		"cliproxy-client",
		"amp-web-local-inference",
		"cliproxy-bootstrap-executor",
		`const apiKey = local.searchParams.get("cliproxy-api-key") || (userActorSocket ? storedLocalAPIKey() : localAPIKey());`,
		"if (userActorSocket && !apiKey)",
		"const bootstrapExecutor = shouldBootstrapExecutor(source);",
		"const workingDirectory = normalizeWorkingDirectory(threadWorkingDirectories()[threadID]);",
		"const mode = normalizedThreadSettings(threadSettings()[threadID]);",
		"if (bootstrapExecutor && !userActorSocket && threadID && threadID === pathThreadID())",
		"pendingLocalBootstrapThreadID = threadID;",
		"let rememberLocalThreadIDOnOpen = \"\";",
		"rememberLocalThreadIDOnOpen = pendingLocalBootstrapThreadID;",
		"if (rememberLocalThreadIDOnOpen)",
		"/api/thread-actors",
		"/api/internal",
		"WebSocket",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("userscript missing %q:\n%s", want, body)
		}
	}
	if strings.Contains(body, "localSidebarProjectRegroupRequested") {
		t.Fatal("userscript still uses the page-wide sidebar regroup guard")
	}
	activityFilterValue := `key === "repo" ? option?.key : key === "user" ? option?.id : option?.[key]`
	if strings.Count(body, activityFilterValue) != 2 {
		t.Fatalf("userscript Activity filter values must map repository keys and user ids in both build and update paths")
	}
	visibleProjectIndex := strings.Index(body, "const fromVisibleProject = visibleProjectWorkingDirectory(createProjectName || visibleCreateThreadProjectName());")
	selectedLocalProjectIndex := strings.Index(body, "const fromSelectedLocalProject = selectedLocalProjectWorkingDirectory();")
	if selectedLocalProjectIndex < 0 || visibleProjectIndex < 0 || visibleProjectIndex > selectedLocalProjectIndex {
		t.Fatalf("userscript must prefer visible project before stale selected local project for create-thread working directory")
	}
	remoteCreateIndex := strings.Index(body, "function remoteCreateProjectThreadWorkingDirectory(body, createProjectName = \"\")")
	if remoteCreateIndex < 0 {
		t.Fatal("userscript missing remote create-thread working directory resolver")
	}
	remoteCreateBody := body[remoteCreateIndex:]
	visibleHomeSelectionIndex := strings.Index(remoteCreateBody, "const fromVisibleLocalSelection = visibleCreateLocalProjectWorkingDirectory();")
	explicitProjectIDIndex := strings.Index(remoteCreateBody, "if (projectID)")
	homeSelectionIndex := strings.Index(remoteCreateBody, `if (localProjectIsNoProject(selectedProject))`)
	if visibleHomeSelectionIndex < 0 || explicitProjectIDIndex < 0 || homeSelectionIndex < 0 || explicitProjectIDIndex > visibleHomeSelectionIndex || visibleHomeSelectionIndex > homeSelectionIndex {
		t.Fatal("userscript must resolve an explicit project ID before stale No Project state")
	}
	localWorkingDirectoryIndex := strings.Index(body, "function localWorkingDirectory()")
	selectedDirectoryIndex := strings.Index(body[localWorkingDirectoryIndex:], "const selectedDirectory = selectedLocalProjectWorkingDirectory();")
	activeDirectoryIndex := strings.Index(body[localWorkingDirectoryIndex:], "const activeDirectory = activeThreadWorkingDirectory();")
	if localWorkingDirectoryIndex < 0 || selectedDirectoryIndex < 0 || activeDirectoryIndex < 0 || activeDirectoryIndex > selectedDirectoryIndex {
		t.Fatalf("userscript must preserve the active thread working directory before a new-thread project selection")
	}
	newLocalWorkingDirectoryIndex := strings.Index(body, "function newLocalThreadWorkingDirectory()")
	if newLocalWorkingDirectoryIndex < 0 {
		t.Fatal("userscript missing new-thread working directory selection")
	}
	newLocalWorkingDirectoryBody := body[newLocalWorkingDirectoryIndex:]
	newVisibleDirectoryIndex := strings.Index(newLocalWorkingDirectoryBody, "visibleProjectWorkingDirectory(visibleCreateThreadProjectName())")
	newSelectedDirectoryIndex := strings.Index(newLocalWorkingDirectoryBody, "selectedDirectory ||")
	if newVisibleDirectoryIndex < 0 || newSelectedDirectoryIndex < 0 || newVisibleDirectoryIndex > newSelectedDirectoryIndex {
		t.Fatal("userscript must prefer the visible create-thread project before a stale selected project")
	}
	if !strings.Contains(newLocalWorkingDirectoryBody, `localProjectIsNoProject(selectedProject)`) {
		t.Fatal("userscript must preserve an explicit No Project home selection for local thread creation")
	}
	for _, unwanted := range []string{
		"rememberVisibleLocalSidebarTitles",
		"installLocalThreadKeyboardShortcut();",
		"installThreadMenuIntegration();",
		"installCommandPaletteIntegration();",
		"handleNewThreadIntent();",
		"seedLocalSidebarProjects",
		"seedLocalSidebarProjectForWorkingDirectory",
		"scheduleLocalSidebarProjectsRefresh",
		"refreshLocalSidebarProjects",
		"mergeDevalueSidebarProjects",
		"cachedLocalSidebarRecentThreads",
		"cachedLocalSidebarProjects",
		"appendDevalueSidebarDateValue",
		"devalueSidebarThreadIDs",
		"devalueSidebarThreadRef",
		"patchDevalueSidebarThread",
		"localProjectIDForWorkingDirectory",
		"globalThis.localStorage.setItem(apiKeyStorageKey, promptedAPIKey)",
		"cachedProjectWorkingDirectory",
		"patchPlainThreadRuntime",
		"patchDevalueThreadRuntime",
		`meta.executorType = "local-client"`,
		`source.includes("hasExecutor")`,
		`source.includes("executorConnected")`,
		"ensureDevalueValueIndex(values, true)",
		`if (!workingDirectory || !response || !response.ok || typeof response.clone !== "function")`,
		"!projectPickerItemSelected(noProject)",
		"/_app/remote/cliproxy/listThreadListSidebar",
		`case "listUserExecutorDaemons":`,
		"path.endsWith(\"/listThreadListSidebar\")",
		"path.endsWith(\"/listUserExecutorDaemons\")",
		"\n\t\tcreateLocalThread,\n",
		"\n\t\tpromptLocalThread,\n",
		"\n\t\topenLocalThreadFromMenu,\n",
		"patchNestedDevalueThreadActorConfigs",
		"patchPlainThreadActorConfigs",
		"data-cliproxy-plugin-agent-mode-dial",
		"visibleActivityFilterSearchQuery",
		"if (nativeHasLocalActivity) {",
	} {
		if strings.Contains(body, unwanted) {
			t.Fatalf("userscript still activates obsolete local thread control %q:\n%s", unwanted, body)
		}
	}
	projectIDIndex := strings.Index(body, `const projectID = isPlainObject(decoded) ? firstString(decoded.projectID, decoded.projectId, decoded.project_id) : "";`)
	projectIDLookupIndex := strings.Index(body, `const fromProjectID = normalizeWorkingDirectory(localProjectByID(localProjectsCache.projects, projectID)?.workingDirectory);`)
	capturedProjectFallbackIndex := strings.Index(body, `const fromCapturedProject = visibleProjectWorkingDirectory(createProjectName);`)
	visibleProjectFallbackIndex := strings.Index(body, `const fromVisibleProject = visibleProjectWorkingDirectory(createProjectName || visibleCreateThreadProjectName());`)
	if projectIDIndex < 0 || projectIDLookupIndex < 0 || capturedProjectFallbackIndex < 0 || visibleProjectFallbackIndex < 0 || projectIDIndex > projectIDLookupIndex || projectIDLookupIndex > capturedProjectFallbackIndex || capturedProjectFallbackIndex > visibleProjectFallbackIndex || !strings.Contains(body[projectIDLookupIndex:capturedProjectFallbackIndex], `return fromProjectID;`) || !strings.Contains(body[projectIDLookupIndex:capturedProjectFallbackIndex], `return "";`) {
		t.Fatalf("userscript should resolve projectID bodies before visible project fallback:\n%s", body)
	}

	headReq := httptest.NewRequest(http.MethodHead, "/ampcode/local-inference.user.js", nil)
	headReq.Host = "127.0.0.1:8317"
	headRec := httptest.NewRecorder()
	r.ServeHTTP(headRec, headReq)

	if headRec.Code != http.StatusOK {
		t.Fatalf("userscript HEAD status = %d, want %d", headRec.Code, http.StatusOK)
	}
	if got := headRec.Body.String(); got != "" {
		t.Fatalf("userscript HEAD body = %q, want empty", got)
	}
	if got := headRec.Header().Get("Content-Length"); got == "" {
		t.Fatalf("userscript HEAD missing Content-Length")
	}
	if got, want := headRec.Header().Get("Content-Length"), rec.Header().Get("Content-Length"); got != want {
		t.Fatalf("userscript HEAD Content-Length = %q, want GET length %q", got, want)
	}
	if got := headRec.Header().Get("Cache-Control"); got != "no-store, max-age=0" {
		t.Fatalf("userscript HEAD Cache-Control = %q, want no-store revalidation", got)
	}
	if got := headRec.Header().Get("Pragma"); got != "no-cache" {
		t.Fatalf("userscript HEAD Pragma = %q, want no-cache", got)
	}
}

func TestWebLocalInferenceUserscriptSyntax(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("node not installed")
	}
	path := filepath.Join(t.TempDir(), "local-inference.user.js")
	if err := os.WriteFile(path, []byte(ampWebLocalInferenceUserscript("http://127.0.0.1:8317", nil)), 0o600); err != nil {
		t.Fatalf("write userscript: %v", err)
	}
	cmd := exec.Command("node", "--check", path)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("userscript syntax check failed: %v\n%s", err, output)
	}
}

func TestWebLocalInferenceUserscriptShellPayloadPreservesExecutorType(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("node not installed")
	}
	script := ampWebLocalInferenceUserscript("http://127.0.0.1:8317", nil)
	start := strings.Index(script, "async function createRemoteThreadShell(localPayload)")
	if start < 0 {
		t.Fatal("userscript shell creation function was not found")
	}
	end := strings.Index(script[start:], "async function readJSONResponse")
	if end < 0 {
		t.Fatal("userscript shell creation function end was not found")
	}
	shellFunction := script[start : start+end]
	if !strings.Contains(shellFunction, "executorType: localPayload.executorType") {
		t.Fatalf("thread shell payload dropped executorType:\n%s", shellFunction)
	}
	runner := `
(async () => {
	const calls = [];
	const diagnostics = { remoteShellCreateCount: 0 };
	const localBaseURLString = () => "http://127.0.0.1:8317";
	const localFetchHeaders = () => ({});
	const originalFetch = async (_, options) => {
		calls.push(JSON.parse(options.body));
		return { text: async () => JSON.stringify({ threadId: "T-shell" }) };
	};
	const readJSONResponse = async (response) => JSON.parse(await response.text());
	const responseThreadID = (response) => response.threadId || "";
` + shellFunction + `
	for (const executorType of ["sandbox", "local-client"]) {
		await createRemoteThreadShell({
			agentMode: "smart",
			reasoningEffort: "high",
			executorType,
			runnerId: executorType === "local-client" ? "local-runner-a" : "",
			spawnExecutor: executorType === "local-client" ? false : undefined,
			settings: { agentMode: "smart" },
			threadMeta: { executorType, runnerId: executorType === "local-client" ? "local-runner-a" : undefined },
		});
	}
	if (calls.length !== 2 || calls[0].executorType !== "sandbox" || calls[1].executorType !== "local-client") {
		throw new Error("shell executor types were not preserved: " + JSON.stringify(calls));
	}
	for (const call of calls) {
		if (call.threadMeta.executorType !== call.executorType || call.threadMeta.cliProxyAPIWebLocalShell !== true) {
			throw new Error("shell thread metadata was not preserved: " + JSON.stringify(call));
		}
	}
	if (calls[0].runnerId || calls[0].spawnExecutor !== undefined || calls[1].runnerId || calls[1].spawnExecutor !== false || calls[1].threadMeta.runnerId) {
		throw new Error("runner intent leaked into shell payload: " + JSON.stringify(calls));
	}
})().catch((error) => { console.error(error && error.stack ? error.stack : error); process.exit(1); });
`
	if output, err := exec.Command("node", "-e", runner).CombinedOutput(); err != nil {
		t.Fatalf("userscript shell payload check failed: %v\n%s", err, output)
	}
}

func TestWebLocalInferenceUserscriptRequiresExactLiveRunner(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("node not installed")
	}
	script := ampWebLocalInferenceUserscript("https://amp.aikins.xyz", nil)
	pathStart := strings.Index(script, "function normalizeLocalRunnerWorkingDirectory(value)")
	if pathStart < 0 {
		t.Fatal("userscript local runner path normalizer was not found")
	}
	pathEnd := strings.Index(script[pathStart:], "function firstWorkingDirectory")
	start := strings.Index(script, "function normalizeLocalRunner(value)")
	if pathEnd < 0 || start < 0 {
		t.Fatal("userscript local runner normalizer was not found")
	}
	end := strings.Index(script[start:], "async function readJSONResponse")
	if end < 0 {
		t.Fatal("userscript local runner block end was not found")
	}
	localRunnerFunctions := script[pathStart:pathStart+pathEnd] + script[start:start+end]
	runner := `
(async () => {
	const assert = (condition, message) => { if (!condition) throw new Error(message); };
	const calls = [];
	let localProjectsCache = { projects: [], runners: [] };
	let fetched = 0;
	let fetchRunners = [];
	const diagnostics = { remoteShellCreateCount: 0 };
	const isPlainObject = (value) => value !== null && typeof value === "object" && !Array.isArray(value);
	const firstString = (...values) => values.find((value) => typeof value === "string" && value.trim())?.trim() || "";
	const normalizeWorkingDirectory = (value) => typeof value === "string" ? value.trim() : "";
	const localThreadModeOptions = () => ({ agentMode: "smart", reasoningEffort: "high" });
	const localProjectRepositoryURLForDirectory = () => "https://github.com/example/app.git";
	const localFetchHeaders = () => ({});
	const ensureDefaultWorkingDirectory = async () => "";
	const fetchLocalProjects = async () => { fetched += 1; localProjectsCache.runners = fetchRunners.map(normalizeLocalRunner).filter(Boolean); return []; };
	const localBaseURLString = () => "https://amp.aikins.xyz";
	const readJSONResponse = async (response) => JSON.parse(await response.text());
	const responseThreadID = (value) => value.threadId || "";
	const rememberLocalThreadID = () => {};
	const rememberThreadWorkingDirectory = () => {};
	const rememberThreadSettings = () => {};
	const localThreadModeLabel = () => "Smart";
	const navigateToThread = () => {};
	const originalFetch = async (url, options) => {
		calls.push({ url, body: JSON.parse(options.body) });
		return { ok: true, status: 200, text: async () => JSON.stringify({ threadId: "T-019f4000-0000-4000-8000-000000000031" }) };
	};
` + localRunnerFunctions + `

	const directory = "/Users/test/Developer/app";
	let missingCheckoutError = "";
	try { requireLocalRunner(""); } catch (error) { missingCheckoutError = error.message; }
	assert(missingCheckoutError === "Select a valid checkout before starting a Mac thread", "missing checkout error = " + missingCheckoutError);
	fetchRunners = [{ runnerId: "local-runner-a", workingDirectory: directory + "/", hostname: "Mac" }];
	await createLocalThread("hello", directory + "/nested/.././", {}, "local");
	assert(fetched === 1, "local creation did not force a runner refresh");
	assert(calls.length === 2, "local creation did not create shell and actor");
	assert(!calls[0].body.runnerId && !calls[0].body.threadMeta.runnerId, "runner intent leaked into shell request: " + JSON.stringify(calls[0]));
		assert(calls[0].body.spawnExecutor === false, "shell executor spawn not disabled in " + JSON.stringify(calls[0]));
		assert(calls[1].body.runnerId === "local-runner-a", "runnerId missing from actor request: " + JSON.stringify(calls[1]));
		assert(calls[1].body.spawnExecutor === false, "actor executor spawn not disabled in " + JSON.stringify(calls[1]));
		assert(calls[1].body.threadMeta.runnerId === "local-runner-a", "nested runnerId missing from actor request: " + JSON.stringify(calls[1]));
	assert(calls.every((call) => call.body.workingDirectory === directory), "catalog path did not replace browser path: " + JSON.stringify(calls));

	calls.length = 0;
	fetchRunners = [{ runnerId: "local-runner-a", workingDirectory: "/Users/test/Developer/other" }];
	let mismatchError = "";
	try { await createLocalThread("hello", directory, {}, "local"); } catch (error) { mismatchError = error.message; }
	assert(mismatchError === "No Mac broker runner is connected for this checkout", "mismatch error = " + mismatchError);
	assert(calls.length === 0, "mismatch sent a shell request");

	calls.length = 0;
	fetchRunners = [];
	let offlineError = "";
	try { await createLocalThread("hello", directory, {}, "local"); } catch (error) { offlineError = error.message; }
	assert(offlineError === "No Mac broker runner is connected for this checkout", "offline error = " + offlineError);
	assert(calls.length === 0, "offline broker sent a shell request");

		fetchRunners = [
			{ runnerId: "local-runner-a", workingDirectory: directory },
			{ runnerId: "local-runner-b", workingDirectory: directory },
		];
		let ambiguousError = "";
		try { await createLocalThread("hello", directory, {}, "local"); } catch (error) { ambiguousError = error.message; }
		assert(ambiguousError === "Multiple Mac broker runners match this checkout; keep exactly one runner active or choose a different checkout", "ambiguous error = " + ambiguousError);
		assert(calls.length === 0, "ambiguous broker sent a shell request");

	calls.length = 0;
	fetched = 0;
	await createLocalThread("hello", directory, {}, "orb");
	assert(fetched === 0, "Orb creation unexpectedly fetched a local runner");
	assert(calls.length === 2 && calls.every((call) => call.body.executorType === "sandbox"), "Orb classification changed: " + JSON.stringify(calls));
	assert(calls.every((call) => !call.body.runnerId && call.body.spawnExecutor === undefined), "Orb payload gained runner fields: " + JSON.stringify(calls));
})().catch((error) => { console.error(error && error.stack ? error.stack : error); process.exit(1); });
`
	if output, err := exec.Command("node", "-e", runner).CombinedOutput(); err != nil {
		t.Fatalf("userscript local runner check failed: %v\n%s", err, output)
	}
}

func TestWebLocalInferenceUserscriptScopesDelayedAPIKeyHydration(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("node not installed")
	}
	for _, test := range []struct {
		name     string
		baseURL  string
		scenario string
	}{
		{name: "after document start", baseURL: "http://127.0.0.1:8317", scenario: "delayed"},
		{name: "after visual timeout", baseURL: "http://127.0.0.1:8317", scenario: "timeout"},
		{name: "account scope", baseURL: "http://127.0.0.1:8317", scenario: "account"},
		{name: "local base scope", baseURL: "http://127.0.0.1:8318", scenario: "base"},
		{name: "pre-identity persistence consent", baseURL: "http://127.0.0.1:8317", scenario: "pending"},
		{name: "session key survives refresh before identity", baseURL: "http://127.0.0.1:8317", scenario: "refresh"},
		{name: "current bootstrap restores refresh identity", baseURL: "http://127.0.0.1:8317", scenario: "bootstrap-object"},
		{name: "quoted JSON bootstrap restores refresh identity", baseURL: "http://127.0.0.1:8317", scenario: "bootstrap-object-quoted"},
		{name: "cached sidebar reveals before metadata refresh", baseURL: "http://127.0.0.1:8317", scenario: "cached-sidebar"},
		{name: "current bootstrap confirms account switch", baseURL: "http://127.0.0.1:8317", scenario: "bootstrap-object-account"},
		{name: "conflicting bootstrap users cannot confirm identity", baseURL: "http://127.0.0.1:8317", scenario: "bootstrap-object-conflict"},
		{name: "unrelated object cannot confirm identity", baseURL: "http://127.0.0.1:8317", scenario: "bootstrap-unrelated"},
		{name: "decoy bootstrap keys cannot confirm identity", baseURL: "http://127.0.0.1:8317", scenario: "bootstrap-decoy-keys"},
		{name: "stale refresh identity hint cannot cross accounts", baseURL: "http://127.0.0.1:8317", scenario: "refresh-account"},
		{name: "legacy session key migrates after identity", baseURL: "http://127.0.0.1:8317", scenario: "legacy"},
		{name: "legacy key cannot overwrite scoped key", baseURL: "http://127.0.0.1:8317", scenario: "legacy-existing"},
		{name: "corrected credential retries hydration", baseURL: "http://127.0.0.1:8317", scenario: "retry"},
		{name: "forgotten credential invalidates hydration", baseURL: "http://127.0.0.1:8317", scenario: "forget"},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			scriptPath := filepath.Join(dir, "local-inference.user.js")
			script := ampWebLocalInferenceUserscript(test.baseURL, nil)
			hydrationTimeout := "5"
			if test.scenario == "cached-sidebar" {
				hydrationTimeout = "100"
			}
			script = strings.Replace(script, "const localSidebarHydrationTimeout = 2500;", "const localSidebarHydrationTimeout = "+hydrationTimeout+";", 1)
			exported := strings.Replace(script, "\tglobalThis.__cliproxyAmpLocalInference = {", "\tglobalThis.__cliproxyAmpLocalInferenceAPIKeyTest = { storedLocalAPIKey, localAPIKey, rememberLocalAPIKey, forgetLocalAPIKey, fetchLocalProjects, localSidebarArchivedThreadID, sessionLocalAPIKeyStorageKey, persistentLocalAPIKeyStorageKey, localAPIKeyPromptStorageKey, rememberAuthenticatedAmpUser, renderLocalSidebarMetadata };\n\tglobalThis.__cliproxyAmpLocalInference = {", 1)
			if exported == script {
				t.Fatal("userscript missing local inference bridge")
			}
			if err := os.WriteFile(scriptPath, []byte(exported), 0o600); err != nil {
				t.Fatalf("write userscript: %v", err)
			}
			runner := `
(async () => {
const assert = (condition, message) => { if (!condition) throw new Error(message); };
const scriptPath = ` + strconv.Quote(scriptPath) + `;
const scenario = ` + strconv.Quote(test.scenario) + `;
const configuredBaseURL = ` + strconv.Quote(test.baseURL) + `;
const viewerA = "viewer-a";
const viewerB = "viewer-b";
const viewerC = "viewer-c";
const legacyKey = "cliproxyapi.ampLocalInference.apiKey";
const authenticatedUserIDKey = legacyKey + ".authenticatedAmpUserID";
const normalizedBaseURL = (value) => new URL(value).href.replace(/\/+$/, "");
const scopeSuffix = (userID, baseURL) => encodeURIComponent(userID) + "." + encodeURIComponent(normalizedBaseURL(baseURL));
const scopedKey = (userID, baseURL) => "cliproxyapi.ampLocalInference.apiKey.user." + scopeSuffix(userID, baseURL);
const scopedPromptKey = (userID, baseURL) => "cliproxyapi.ampLocalInference.promptedAPIKey.user." + scopeSuffix(userID, baseURL);
class TestStorage {
	constructor() { this.values = new Map(); }
	getItem(key) { key = String(key); return this.values.has(key) ? this.values.get(key) : null; }
	setItem(key, value) { this.values.set(String(key), String(value)); }
	removeItem(key) { this.values.delete(String(key)); }
}
class FakeElement {
	constructor(tagName = "div", text = "") { this.attributes = new Map(); this.children = []; this.dataset = {}; this.style = {}; this.parentElement = null; this.tagName = String(tagName).toUpperCase(); this.textContent = String(text); }
	appendChild(child) { this.children.push(child); child.parentElement = this; return child; }
	remove() { if (!this.parentElement) return; this.parentElement.children = this.parentElement.children.filter((child) => child !== this); this.parentElement = null; }
	setAttribute(name, value) { this.attributes.set(String(name), String(value)); }
	getAttribute(name) { return this.attributes.has(String(name)) ? this.attributes.get(String(name)) : null; }
	removeAttribute(name) { this.attributes.delete(String(name)); }
	hasAttribute(name) { return this.attributes.has(String(name)); }
	querySelector() { return null; }
	querySelectorAll(selector) { return selector === "span" ? this.children.filter((child) => child.tagName === "SPAN") : []; }
	matches() { return false; }
	closest() { return null; }
	contains(target) { for (let node = target; node; node = node.parentElement) if (node === this) return true; return false; }
	addEventListener() {}
	removeEventListener() {}
}
const documentElement = new FakeElement();
const head = documentElement.appendChild(new FakeElement());
const body = documentElement.appendChild(new FakeElement());
const cachedSidebarThreadID = "T-019f324b-2802-7868-b1b1-000000000000";
const cachedSidebarRow = new FakeElement("a");
cachedSidebarRow.dataset.sidebarThreadId = cachedSidebarThreadID;
const cachedSidebarTitle = cachedSidebarRow.appendChild(new FakeElement("span", "Untitled"));
const bootstrapObjectUserID = scenario === "bootstrap-object-account" ? viewerB : viewerA;
const inlineScripts = scenario === "bootstrap-object" || scenario === "bootstrap-object-account" || scenario === "cached-sidebar" ? [{
	textContent: '__sveltekit.data={q:{user:{id:"' + bootstrapObjectUserID + '",email:"viewer@example.test",firstName:"Viewer",lastName:"One",username:"viewer",profilePictureUrl:"https://example.test/avatar"},initialProjects:{projects:[]},userFeatures:[]}};',
}] : scenario === "bootstrap-object-quoted" ? [{
	textContent: '__sveltekit.data={"q":{"user":{"id":"' + viewerA + '","email":"viewer@example.test","username":"viewer"},"initialProjects":{"projects":[]},"userFeatures":[]}};',
}] : scenario === "bootstrap-object-conflict" ? [{
	textContent: '__sveltekit.data={q:{user:{id:"' + viewerA + '",email:"viewer-a@example.test",username:"viewer-a"},nested:{user:{id:"' + viewerB + '",email:"viewer-b@example.test",username:"viewer-b"}},initialProjects:{projects:[]},userFeatures:[]}};',
}] : scenario === "bootstrap-unrelated" ? [{
	textContent: 'const unrelated={user:{id:"' + viewerA + '",email:"viewer@example.test",username:"viewer"},initialProjects:{projects:[]},userFeatures:[]};__sveltekit.data={q:{initialProjects:{projects:[]},userFeatures:[]}};',
}] : scenario === "bootstrap-decoy-keys" ? [{
	textContent: '__sveltekit.data={"q":{"user":{"id":"' + viewerA + '","email":"viewer@example.test","username":"viewer"},"notinitialProjects":{},"notuserFeatures":[]}};',
}] : [];
globalThis.Element = FakeElement;
globalThis.HTMLElement = FakeElement;
globalThis.NodeFilter = { SHOW_TEXT: 4, SHOW_ELEMENT: 1 };
globalThis.document = {
	readyState: scenario === "timeout" ? "loading" : "complete",
	visibilityState: "visible",
	title: "Amp",
	documentElement,
	head,
	body,
	createElement() { return new FakeElement(); },
	createTextNode() { return new FakeElement(); },
	createTreeWalker() { return { currentNode: null, nextNode() { return null; } }; },
	getElementById() { return null; },
	querySelector() { return null; },
	querySelectorAll(selector) {
		if (selector === "script:not([src])") return inlineScripts;
		if (selector === "[data-sidebar-thread-id]" && scenario === "cached-sidebar") return [cachedSidebarRow];
		return [];
	},
	contains(target) { return documentElement.contains(target); },
	addEventListener() {},
	removeEventListener() {},
	dispatchEvent() { return true; },
};
globalThis.window = globalThis;
globalThis.location = new URL("https://ampcode.com/");
globalThis.history = { state: null, replaceState() {}, pushState() {}, back() {} };
globalThis.localStorage = new TestStorage();
globalThis.sessionStorage = new TestStorage();
globalThis.MutationObserver = class { observe() {} disconnect() {} };
let pageShowCount = 0;
globalThis.dispatchEvent = (event) => { if (event?.type === "pageshow") pageShowCount += 1; return true; };
globalThis.requestAnimationFrame = (callback) => { if (scenario === "cached-sidebar") setTimeout(callback, 0); return 1; };
globalThis.cancelAnimationFrame = () => {};
globalThis.WebSocket = class { static CONNECTING = 0; static OPEN = 1; static CLOSING = 2; static CLOSED = 3; addEventListener() {} send() {} close() {} };
let promptCount = 0;
globalThis.prompt = () => { promptCount += 1; return scenario === "pending" ? "pending-key" : ""; };
globalThis.confirm = () => scenario === "pending";
let localProjectsFetchCount = 0;
let releaseLocalProjectsFetch;
const localProjectsFetchGate = scenario === "forget" ? new Promise((resolve) => { releaseLocalProjectsFetch = resolve; }) : null;
globalThis.fetch = async (input) => {
	const url = new URL(String(input), globalThis.location.href);
	if (url.pathname === "/ampcode/local-projects.json") {
		localProjectsFetchCount += 1;
		if (scenario === "cached-sidebar") {
			await new Promise((resolve) => setTimeout(resolve, 10));
		}
		if (scenario === "retry" && localProjectsFetchCount === 1) {
			return new Response(JSON.stringify({ error: "invalid api key" }), { status: 401, headers: { "Content-Type": "application/json" } });
		}
		if (scenario === "forget" && localProjectsFetchCount === 1) {
			await localProjectsFetchGate;
		}
		const projects = scenario === "forget" ? [{ id: "stale", name: "Stale", workingDirectory: "/tmp/stale" }] : [];
		const archivedThreadIDs = scenario === "account" ? ["T-019f324b-2802-7868-b1b1-5f0fa3e87e99"] : [];
		const threads = scenario === "cached-sidebar" ? Array.from({ length: 100 }, (_, index) => ({
			id: "T-019f324b-2802-7868-b1b1-" + index.toString(16).padStart(12, "0"),
			title: index === 0 ? "Cached local title" : "Cached local thread " + index,
		})) : [];
		return new Response(JSON.stringify({ ok: true, projects, threads, archivedThreadIDs }), { status: 200, headers: { "Content-Type": "application/json" } });
	}
	return new Response("", { status: 404 });
};
const previousBaseURL = "http://127.0.0.1:8317";
if (scenario === "refresh" || scenario === "refresh-account" || scenario === "bootstrap-object" || scenario === "bootstrap-object-quoted" || scenario === "bootstrap-object-account" || scenario === "bootstrap-object-conflict" || scenario === "bootstrap-unrelated" || scenario === "bootstrap-decoy-keys" || scenario === "cached-sidebar") {
	globalThis.sessionStorage.setItem(scopedKey(viewerA, previousBaseURL), "session-key");
	globalThis.sessionStorage.setItem(authenticatedUserIDKey, viewerA);
} else if (scenario === "legacy") {
	globalThis.sessionStorage.setItem(legacyKey, "legacy-session-key");
} else if (scenario === "legacy-existing") {
	globalThis.sessionStorage.setItem(legacyKey, "legacy-session-key");
	globalThis.sessionStorage.setItem(scopedKey(viewerA, previousBaseURL), "current-session-key");
} else if (scenario !== "pending") {
	globalThis.localStorage.setItem(scopedKey(viewerA, previousBaseURL), "persistent-key");
}
if (scenario === "cached-sidebar") {
	globalThis.localStorage.setItem("cliproxyapi.ampLocalInference.sidebarTitles.v3." + scopeSuffix(viewerA, previousBaseURL), JSON.stringify({ [cachedSidebarThreadID]: "Cached local title" }));
}
if (scenario === "base") {
	globalThis.sessionStorage.setItem(scopedPromptKey(viewerA, previousBaseURL), "1");
}
await import("file://" + scriptPath);
const bridge = globalThis.__cliproxyAmpLocalInferenceAPIKeyTest;
assert(bridge && typeof bridge.rememberAuthenticatedAmpUser === "function", "API-key test bridge was not exposed");
if (scenario === "timeout") {
	await new Promise((resolve) => setTimeout(resolve, 15));
	assert(!documentElement.hasAttribute("data-cliproxy-local-sidebar-hydrating"), "visual hydration gate remained after its timeout");
}
if (scenario === "pending") {
	assert(bridge.localAPIKey(false) === "pending-key", "pre-identity prompt did not return the entered key");
	assert(bridge.sessionLocalAPIKeyStorageKey() === "" && bridge.persistentLocalAPIKeyStorageKey() === "", "pre-identity key gained an anonymous storage scope");
	assert(!Array.from(globalThis.sessionStorage.values.values()).includes("pending-key") && !Array.from(globalThis.localStorage.values.values()).includes("pending-key"), "pre-identity key was written to browser storage");
	bridge.rememberAuthenticatedAmpUser({ id: viewerA });
	await new Promise((resolve) => setTimeout(resolve, 20));
	const sessionKey = bridge.sessionLocalAPIKeyStorageKey();
	const persistentKey = bridge.persistentLocalAPIKeyStorageKey();
	assert(globalThis.sessionStorage.getItem(sessionKey) === "pending-key", "pre-identity key was not promoted to scoped session storage");
	assert(globalThis.localStorage.getItem(persistentKey) === "pending-key", "explicit pre-identity persistence consent was not promoted");
	assert(localProjectsFetchCount === 1 && promptCount === 1, "promoted pre-identity key did not hydrate exactly once without another prompt");
	return;
}
if (scenario === "refresh") {
	assert(bridge.localAPIKey(false) === "", "refresh used a scoped key before confirming the current account");
	assert(promptCount === 0, "refresh prompted before delayed identity discovery");
	bridge.rememberAuthenticatedAmpUser({ id: viewerA });
	await new Promise((resolve) => setTimeout(resolve, 20));
	assert(bridge.storedLocalAPIKey() === "session-key", "refresh did not recover its account-scoped session key");
	assert(promptCount === 0 && localProjectsFetchCount === 1, "refresh did not hydrate exactly once without prompting");
	return;
}
if (scenario === "bootstrap-object" || scenario === "bootstrap-object-quoted") {
	await new Promise((resolve) => setTimeout(resolve, 20));
	assert(bridge.storedLocalAPIKey() === "session-key", "current Amp bootstrap did not restore the account-scoped key");
	assert(promptCount === 0 && localProjectsFetchCount === 1, "current Amp bootstrap did not hydrate exactly once without prompting");
	return;
}
if (scenario === "cached-sidebar") {
	await new Promise((resolve) => setTimeout(resolve, 20));
	assert(cachedSidebarTitle.textContent === "Cached local title", "scoped cached sidebar title was not rendered immediately");
	assert(!documentElement.hasAttribute("data-cliproxy-local-sidebar-hydrating"), "cached visible sidebar waited for the hydration timeout");
	assert(localProjectsFetchCount === 1, "cached sidebar hydration did not keep one bounded metadata refresh");
	assert(pageShowCount === 1, "cached sidebar hydration rebuild count = " + pageShowCount + ", want 1 after background metadata arrived");
	assert(globalThis.localStorage.getItem("cliproxyapi.ampLocalInference.sidebarTitles.v3." + scopeSuffix(viewerA, previousBaseURL)) !== null, "sidebar title cache was not scoped to the account and local server");
	cachedSidebarTitle.textContent = "Untitled";
	bridge.rememberAuthenticatedAmpUser({ id: viewerB });
	bridge.renderLocalSidebarMetadata();
	assert(cachedSidebarTitle.textContent === "Untitled", "cached sidebar title leaked across authenticated accounts");
	return;
}
if (scenario === "bootstrap-object-account") {
	await new Promise((resolve) => setTimeout(resolve, 20));
	assert(bridge.storedLocalAPIKey() === "", "current Amp bootstrap reused the hinted account's key after an account switch");
	assert(globalThis.sessionStorage.getItem(authenticatedUserIDKey) === viewerB, "current Amp bootstrap did not replace the stale identity hint");
	assert(promptCount === 0 && localProjectsFetchCount === 0, "current Amp bootstrap hydrated under the stale hinted account");
	return;
}
if (scenario === "bootstrap-object-conflict") {
	await new Promise((resolve) => setTimeout(resolve, 20));
	assert(bridge.storedLocalAPIKey() === "", "conflicting bootstrap users confirmed the stale identity hint");
	assert(promptCount === 0 && localProjectsFetchCount === 0, "conflicting bootstrap users triggered local hydration");
	return;
}
if (scenario === "bootstrap-unrelated") {
	await new Promise((resolve) => setTimeout(resolve, 20));
	assert(bridge.storedLocalAPIKey() === "", "unrelated user object confirmed the stale identity hint");
	assert(promptCount === 0 && localProjectsFetchCount === 0, "unrelated user object triggered local hydration");
	return;
}
if (scenario === "bootstrap-decoy-keys") {
	await new Promise((resolve) => setTimeout(resolve, 20));
	assert(bridge.storedLocalAPIKey() === "", "decoy bootstrap keys confirmed the stale identity hint");
	assert(promptCount === 0 && localProjectsFetchCount === 0, "decoy bootstrap keys triggered local hydration");
	return;
}
if (scenario === "refresh-account") {
	assert(bridge.localAPIKey(false) === "", "stale refresh hint used the prior account's scoped key");
	assert(promptCount === 0, "stale refresh hint prompted before delayed identity discovery");
	bridge.rememberAuthenticatedAmpUser({ id: viewerB });
	await new Promise((resolve) => setTimeout(resolve, 20));
	assert(bridge.storedLocalAPIKey() === "", "confirmed account reused the stale hinted account's key");
	assert(globalThis.sessionStorage.getItem(scopedKey(viewerA, previousBaseURL)) === "session-key", "account confirmation modified the stale hinted account's key");
	assert(globalThis.sessionStorage.getItem(authenticatedUserIDKey) === viewerB, "confirmed account did not replace the stale identity hint");
	assert(promptCount === 0 && localProjectsFetchCount === 0, "stale identity hint triggered local hydration under the wrong account");
	return;
}
if (scenario === "legacy") {
	assert(bridge.localAPIKey(false) === "", "legacy key was used before confirming the current account");
	assert(globalThis.sessionStorage.getItem(legacyKey) === "legacy-session-key", "legacy key was removed before identity discovery");
	assert(promptCount === 0, "legacy key migration prompted before identity discovery");
	bridge.rememberAuthenticatedAmpUser({ id: viewerA });
	await new Promise((resolve) => setTimeout(resolve, 20));
	assert(globalThis.sessionStorage.getItem(scopedKey(viewerA, previousBaseURL)) === "legacy-session-key", "legacy session key was not migrated into the authenticated scope");
	assert(globalThis.localStorage.getItem(scopedKey(viewerA, previousBaseURL)) === null, "legacy session key became persistent during migration");
	assert(globalThis.sessionStorage.getItem(legacyKey) === null, "legacy session key remained after migration");
	assert(bridge.storedLocalAPIKey() === "legacy-session-key" && promptCount === 0 && localProjectsFetchCount === 1, "migrated legacy key did not hydrate exactly once without prompting");
	return;
}
if (scenario === "legacy-existing") {
	assert(bridge.localAPIKey(false) === "", "legacy key was used before confirming the current account");
	bridge.rememberAuthenticatedAmpUser({ id: viewerA });
	await new Promise((resolve) => setTimeout(resolve, 20));
	assert(bridge.storedLocalAPIKey() === "current-session-key", "legacy migration overwrote the existing scoped key");
	assert(globalThis.sessionStorage.getItem(legacyKey) === null, "superseded legacy key remained after identity discovery");
	assert(promptCount === 0 && localProjectsFetchCount === 1, "existing scoped key did not hydrate exactly once after legacy cleanup");
	return;
}
bridge.rememberAuthenticatedAmpUser({ id: viewerA });
if (scenario === "forget") {
	await new Promise((resolve) => setTimeout(resolve, 0));
	assert(localProjectsFetchCount === 1, "passive hydration did not start before the scoped key was forgotten");
	bridge.forgetLocalAPIKey();
	releaseLocalProjectsFetch();
	await new Promise((resolve) => setTimeout(resolve, 20));
	assert(bridge.storedLocalAPIKey() === "", "forgotten scoped API key remained available");
	assert((await bridge.fetchLocalProjects(false)).length === 0, "response authorized by a forgotten key repopulated the project cache");
	return;
}
await new Promise((resolve) => setTimeout(resolve, 20));
if (scenario === "base") {
	assert(bridge.storedLocalAPIKey() === "", "local-base switch reused another server's key");
	assert(bridge.persistentLocalAPIKeyStorageKey() !== scopedKey(viewerA, previousBaseURL), "local-base switch retained the prior storage scope");
	assert(globalThis.sessionStorage.getItem(bridge.localAPIKeyPromptStorageKey("key")) === null, "local-base switch reused prior prompt suppression");
	bridge.localAPIKey();
	assert(promptCount === 1 && localProjectsFetchCount === 0, "local-base switch did not prompt independently without hydrating");
	return;
}
assert(bridge.storedLocalAPIKey() === "persistent-key", "delayed identity did not reuse its scoped persistent key");
assert(promptCount === 0 && localProjectsFetchCount === 1, "delayed identity did not hydrate exactly once without prompting");
if (scenario === "retry") {
	bridge.rememberLocalAPIKey("corrected-key");
	await new Promise((resolve) => setTimeout(resolve, 20));
	assert(bridge.storedLocalAPIKey() === "corrected-key", "corrected scoped API key was not retained");
	assert(localProjectsFetchCount === 2, "corrected scoped API key did not retry failed passive hydration");
	return;
}
if (scenario !== "account") return;
const viewerAArchivedThreadID = "T-019f324b-2802-7868-b1b1-5f0fa3e87e99";
assert(bridge.localSidebarArchivedThreadID(viewerAArchivedThreadID), "first account archive state was not hydrated");
const viewerAKey = bridge.sessionLocalAPIKeyStorageKey();
const viewerAPromptKey = bridge.localAPIKeyPromptStorageKey("key");
bridge.rememberAuthenticatedAmpUser({ id: viewerB });
assert(bridge.storedLocalAPIKey() === "", "account switch reused the prior account's key");
assert(!bridge.localSidebarArchivedThreadID(viewerAArchivedThreadID), "account switch retained the prior account's archived-thread state");
assert(bridge.sessionLocalAPIKeyStorageKey() !== viewerAKey && bridge.localAPIKeyPromptStorageKey("key") !== viewerAPromptKey, "account switch retained prior scoped state keys");
bridge.localAPIKey();
bridge.localAPIKey();
assert(promptCount === 1, "same-account prompt suppression did not prevent a repeat prompt");
bridge.rememberAuthenticatedAmpUser({ id: viewerC });
bridge.localAPIKey();
assert(promptCount === 2, "prompt suppression crossed authenticated accounts");
bridge.rememberAuthenticatedAmpUser({ id: viewerB });
bridge.localAPIKey();
assert(promptCount === 2, "returning account lost its scoped prompt suppression");
bridge.rememberAuthenticatedAmpUser({ id: viewerA });
await new Promise((resolve) => setTimeout(resolve, 20));
assert(bridge.storedLocalAPIKey() === "persistent-key" && localProjectsFetchCount === 2, "returning keyed account did not rehydrate its invalidated cache exactly once");
})().catch((error) => { console.error(error && error.stack ? error.stack : error); process.exit(1); });
`
			runnerPath := filepath.Join(dir, "runner.mjs")
			if err := os.WriteFile(runnerPath, []byte(runner), 0o600); err != nil {
				t.Fatalf("write runner: %v", err)
			}
			if output, err := exec.Command("node", runnerPath).CombinedOutput(); err != nil {
				t.Fatalf("scoped API-key behavior check failed: %v\n%s", err, output)
			}
		})
	}
}

func TestServeWebLocalThreadSearchScopesResultsToOwner(t *testing.T) {
	gin.SetMode(gin.TestMode)
	useTempNeoThreadStore(t)
	enabled := true
	rt := newNeoRuntime(&config.Config{
		SDKConfig: config.SDKConfig{APIKeys: []string{"client-key"}},
		AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{
			Enabled: &enabled,
		}},
	})
	ownedThreadID := "T-019f75f2-5bf4-736b-909c-000000000021"
	foreignThreadID := "T-019f75f2-5bf4-736b-909c-000000000022"
	for _, snapshot := range []neoCloudThreadSnapshot{
		{
			threadID: ownedThreadID,
			title:    "Owned local search result",
			meta: map[string]any{
				"ownerUserId": neoLocalOwnerUserID,
			},
			messages: []neoMessage{{Role: "user", Content: []any{map[string]any{"type": "text", "text": "owner scoped needle"}}}},
		},
		{
			threadID: foreignThreadID,
			title:    "Foreign local search result",
			meta: map[string]any{
				"ownerUserId": "user_foreign",
			},
			messages: []neoMessage{{Role: "user", Content: []any{map[string]any{"type": "text", "text": "owner scoped needle"}}}},
		},
	} {
		if err := writeNeoLocalThreadSnapshotToDir(snapshot, rt.threadDir); err != nil {
			t.Fatalf("write local search thread: %v", err)
		}
	}
	m := &AmpModule{neoRuntime: rt}
	request := func(method, query, bridgeHeader string) *httptest.ResponseRecorder {
		t.Helper()
		recorder := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(recorder)
		req := httptest.NewRequest(method, "/ampcode/local-thread-search.json?q="+url.QueryEscape(query)+"&limit=10", nil)
		req = req.WithContext(context.WithValue(req.Context(), clientAPIKeyContextKey{}, "client-key"))
		req.Header.Set(ampWebLocalInferenceHeader, bridgeHeader)
		c.Request = req
		c.Set(ampWebLocalInferenceCORSContextKey, true)
		m.serveWebLocalThreadSearch(c)
		return recorder
	}

	recorder := request(http.MethodGet, "owner scoped needle", "1")
	if recorder.Code != http.StatusOK {
		t.Fatalf("local thread search status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var response map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode local thread search: %v", err)
	}
	threads := arrayValue(response["threads"])
	if len(threads) != 1 || stringValue(mapValue(threads[0])["id"]) != ownedThreadID {
		t.Fatalf("owner-scoped local thread search = %#v", threads)
	}
	if matched := stringValue(mapValue(threads[0])["matchedSearchText"]); !strings.Contains(matched, "owner scoped needle") {
		t.Fatalf("local search matched text = %q", matched)
	}

	recorder = request(http.MethodGet, "label:review", "1")
	if recorder.Code != http.StatusOK {
		t.Fatalf("unsupported local thread search status = %d", recorder.Code)
	}
	response = nil
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode unsupported local thread search: %v", err)
	}
	if threads := arrayValue(response["threads"]); len(threads) != 0 || boolValue(response["hasMore"]) {
		t.Fatalf("unsupported local thread search response = %#v", response)
	}
	if recorder := request(http.MethodGet, "owner scoped needle", ""); recorder.Code != http.StatusNotFound {
		t.Fatalf("unauthenticated local thread search status = %d", recorder.Code)
	}
	if recorder := request(http.MethodPost, "owner scoped needle", "1"); recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("non-GET local thread search status = %d", recorder.Code)
	}
}

func TestWebLocalInferenceUserscriptPatchesLocalThreadActorConfig(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("node not installed")
	}
	dir := t.TempDir()
	scriptPath := filepath.Join(dir, "local-inference.user.js")
	script := ampWebLocalInferenceUserscript("http://127.0.0.1:8317", nil)
	testDelay := strings.Replace(script, "const localSidebarProjectRegroupStabilityDelay = 3000;", "const localSidebarProjectRegroupStabilityDelay = 5;", 1)
	if testDelay == script {
		t.Fatal("userscript missing sidebar regroup stability delay")
	}
	hiddenDelay := strings.Replace(testDelay, "const hiddenPageSocketPauseGraceMs = 1500;", "const hiddenPageSocketPauseGraceMs = 5;", 1)
	if hiddenDelay == testDelay {
		t.Fatal("userscript missing hidden page socket pause delay")
	}
	script = strings.Replace(hiddenDelay, "\tglobalThis.__cliproxyAmpLocalInference = {", "\tglobalThis.__cliproxyAmpLocalInferenceTest = { requestLocalSidebarProjectRegroup, localProjectCheckoutIdentity, localSidebarProjectMatches, localProjectNativeMissingPage, fetchLocalProjects, fetchLocalThreadSearch, diffCaptureReadThreadID, invalidateLocalSidebarAfterThreadMutation, localSidebarCachedThreadID, localSidebarCachedThreadMetadataComplete, localAPIKey, localProjectLookupAPIKey, storedLocalAPIKey, rememberLocalAPIKey, forgetLocalAPIKey, sessionLocalAPIKeyStorageKey, persistentLocalAPIKeyStorageKey, localAPIKeyPromptStorageKey, rememberAuthenticatedAmpUser, startPassiveLocalSidebarHydration, localSidebarArchivedThreadID, localSidebarMissingMetadataThreadIDs, scheduleLocalSidebarMetadataRefresh };\n\tglobalThis.__cliproxyAmpLocalInference = {", 1)
	if script == hiddenDelay {
		t.Fatal("userscript missing local inference bridge")
	}
	if err := os.WriteFile(scriptPath, []byte(script), 0o600); err != nil {
		t.Fatalf("write userscript: %v", err)
	}
	threadID := "T-019f324b-2802-7868-b1b1-5f0fa3e87ea5"
	secondThreadID := "T-019f324b-2802-7868-b1b1-5f0fa3e87ea6"
	runner := `
(async () => {
const assert = (condition, message) => {
	if (!condition) throw new Error(message);
};
const scriptPath = ` + strconv.Quote(scriptPath) + `;
const threadID = ` + strconv.Quote(threadID) + `;
const secondThreadID = ` + strconv.Quote(secondThreadID) + `;
	const pastThreadID = "T-019f324b-2802-7868-b1b1-5f0fa3e87ea7";
	const cloudThreadID = "T-019f324b-2802-7868-b1b1-5f0fa3e87ea8";
	const archivedThreadID = "T-019f324b-2802-7868-b1b1-5f0fa3e87ea9";
	const discoveredMutationThreadID = "T-019f324b-2802-7868-b1b1-5f0fa3e87eaa";
	const transientThreadID = "T-019f324b-2802-7868-b1b1-5f0fa3e87eab";
	const puckThreadID = "T-019f324b-2802-7868-b1b1-5f0fa3e87eac";
	const hoverSidebarThreadID = "T-019f324b-2802-7868-b1b1-5f0fa3e87ead";
	const collidingSidebarThreadID = "T-019f324b-2802-7868-b1b1-5f0fa3e87eae";
	const raceSidebarThreadID = "T-019f324b-2802-7868-b1b1-5f0fa3e87eaf";
	const incompleteSidebarThreadID = "T-019f324b-2802-7868-b1b1-5f0fa3e87eb3";
	const unknownSidebarThreadID = "T-019f324b-2802-7868-b1b1-5f0fa3e87eb4";
	const searchThreadID = "T-019f324b-2802-7868-b1b1-5f0fa3e87eb0";
	const searchProjectThreadID = "T-019f324b-2802-7868-b1b1-5f0fa3e87eb2";
	const cloudPuckSessionValues = () => [
		{ _: 1 },
		{ thread: 2, threadActorConfig: 4, workspaceProjects: 12 },
		{ id: 3 },
		puckThreadID,
		{ threadId: 3, wsToken: 5, ampURL: 6, baseURL: 6, capability: 7, poolName: 8, requiresSudoForWrite: 9, requiresSudoForTerminal: 9, threadActorTransport: 10 },
		"cloud-token",
		"https://ampcode.com",
		"write",
		"default",
		false,
		"json-rpc",
		true,
		[],
	];
const createdThreadID = "T-019f20b2-5e05-7501-8ddd-994e151ee951";
const failedThreadID = "T-019f20b2-5e05-7501-8ddd-994e151ee955";
const ampViewerUserID = "user_amp_viewer";
const createdThreadWorkDir = "/Users/aikins01/Developer/CLIProxyAPI";
const createdThreadProjectID = "e8122773-e08d-5426-b7cd-58b498e32c21";
const secondThreadProjectID = "44adc7ee-cb9d-5a2e-a83c-8da2de268ede";
class TestStorage {
	constructor() { this.values = new Map(); }
	getItem(key) {
		key = String(key);
		return this.values.has(key) ? this.values.get(key) : null;
	}
	setItem(key, value) { this.values.set(String(key), String(value)); }
	removeItem(key) { this.values.delete(String(key)); }
}
	class FakeElement {
		constructor(tagName = "div", text = "") {
			this.dataset = {};
			this.style = {};
			this.children = [];
			this.attributes = new Map();
			this.tagName = String(tagName).toUpperCase();
			this.textContent = String(text);
			this.parentElement = null;
			this.classList = { add() {}, remove() {}, contains() { return false; } };
		}
		appendChild(child) { this.children.push(child); child.parentElement = this; return child; }
		append(...children) { for (const child of children) this.appendChild(child); }
		addEventListener() {}
		removeEventListener() {}
		setAttribute(name, value) { this.attributes.set(String(name), String(value)); }
		getAttribute(name) { return this.attributes.has(String(name)) ? this.attributes.get(String(name)) : null; }
		hasAttribute(name) { return this.attributes.has(String(name)); }
		removeAttribute(name) { this.attributes.delete(String(name)); }
		remove() {
			if (!this.parentElement) return;
			this.parentElement.children = this.parentElement.children.filter((child) => child !== this);
			this.parentElement = null;
		}
		getBoundingClientRect() { return { width: 120, height: 24 }; }
		replaceChildren(...children) { this.children = []; this.textContent = ""; this.append(...children); }
		querySelector() { return null; }
		querySelectorAll(selector) {
			if (selector === "span") return this.children.filter((child) => child.tagName === "SPAN");
			if (selector === "h1") return this.children.filter((child) => child.tagName === "H1");
			return [];
		}
		closest() { return null; }
		matches() { return false; }
	}
class BrowserEventTarget {
	constructor() { this.eventTargetListeners = new Map(); }
	addEventListener(type, listener) {
		if (typeof listener !== "function") return;
		let listeners = this.eventTargetListeners.get(type);
		if (!listeners) {
			listeners = new Set();
			this.eventTargetListeners.set(type, listeners);
		}
		listeners.add(listener);
	}
	removeEventListener(type, listener) { this.eventTargetListeners.get(type)?.delete(listener); }
	dispatchEvent(event) {
		Object.defineProperties(event, {
			target: { value: this, configurable: true },
			currentTarget: { value: this, configurable: true },
		});
		for (const listener of Array.from(this.eventTargetListeners.get(event.type) || [])) {
			Object.defineProperty(event, "currentTarget", { value: this, configurable: true });
			listener.call(this, event);
		}
		Object.defineProperty(event, "currentTarget", { value: null, configurable: true });
		return !event.defaultPrevented;
	}
}
globalThis.EventTarget = BrowserEventTarget;
class NativeWebSocket extends EventTarget {
	static CONNECTING = 0;
	static OPEN = 1;
	static CLOSING = 2;
	static CLOSED = 3;
	static instances = [];
	constructor(url, protocols) {
		super();
		if (new.target !== NativeWebSocket) throw new TypeError("Illegal invocation");
		this.url = String(url);
		this.protocols = protocols;
		this.readyState = NativeWebSocket.CONNECTING;
		this.listeners = {};
		this.sent = [];
		this.closeDispatched = false;
		NativeWebSocket.instances.push(this);
	}
	addEventListener(name, callback) { (this.listeners[name] ||= []).push(callback); }
	send(payload) { this.sent.push(payload); }
	close(code = 1000, reason = "") {
		if (this.closeDispatched) return;
		this.closeDispatched = true;
		this.readyState = NativeWebSocket.CLOSED;
		queueMicrotask(() => {
			for (const callback of this.listeners.close || []) callback.call(this, { type: "close", target: this, currentTarget: this, code, reason, wasClean: true });
		});
	}
}
globalThis.location = new URL("https://ampcode.com/threads/" + threadID);
	let documentQueryElements = [];
	let documentQueryElement = null;
	const documentEventListeners = new Map();
	const globalEventListeners = new Map();
	const addTestEventListener = (listeners, name, callback) => {
		let callbacks = listeners.get(name);
		if (!callbacks) {
			callbacks = new Set();
			listeners.set(name, callbacks);
		}
		callbacks.add(callback);
	};
	const dispatchTestEvent = (listeners, event) => {
		for (const callback of Array.from(listeners.get(event.type) || [])) callback(event);
	};
	globalThis.document = {
		readyState: "loading",
		visibilityState: "visible",
		body: new FakeElement(),
		documentElement: new FakeElement(),
		addEventListener(name, callback) { addTestEventListener(documentEventListeners, name, callback); },
		querySelector() { return documentQueryElement; },
		querySelectorAll() { return documentQueryElements; },
		createElement(tagName) { return new FakeElement(tagName); },
		createTreeWalker() { return { nextNode() { return null; } }; },
	};
globalThis.Element = FakeElement;
globalThis.HTMLElement = FakeElement;
globalThis.NodeFilter = { SHOW_TEXT: 4, SHOW_ELEMENT: 1 };
globalThis.MutationObserver = class { observe() {} disconnect() {} };
globalThis.localStorage = new TestStorage();
globalThis.sessionStorage = new TestStorage();
Object.defineProperty(globalThis, "navigator", { value: { platform: "iPhone", maxTouchPoints: 5 }, configurable: true });
let pageShowEventCount = 0;
	globalThis.addEventListener = (name, callback) => addTestEventListener(globalEventListeners, name, callback);
globalThis.dispatchEvent = (event) => {
	if (event?.type === "pageshow") pageShowEventCount += 1;
	dispatchTestEvent(globalEventListeners, event);
	return true;
};
	const dispatchDocumentEvent = (type) => dispatchTestEvent(documentEventListeners, { type });
const encodeDevalue = (value) => Buffer.from(JSON.stringify(value)).toString("base64url");
const devalueTable = (value, cacheKey = "") => {
	const values = [null];
	const append = (item) => {
		const ref = values.length;
		values.push(null);
		if (Array.isArray(item)) {
			values[ref] = item.map(append);
		} else if (item !== null && typeof item === "object") {
			const encoded = {};
			values[ref] = encoded;
			for (const [key, child] of Object.entries(item)) encoded[key] = append(child);
		} else {
			values[ref] = item;
		}
		return ref;
	};
	const rootRef = append(value);
	values[0] = { _: rootRef };
	if (cacheKey) {
		const cacheValueRef = values.length;
		values.push({ v: rootRef });
		const queryRef = values.length;
		values.push({ [cacheKey]: cacheValueRef });
		values[0].q = queryRef;
	}
	return values;
};
const requestPayloadText = (body) => {
	try {
		return Buffer.from(JSON.parse(body).payload || "", "base64url").toString("utf8");
	} catch {
		return "";
	}
};
	const nativeJSONParse = JSON.parse.bind(JSON);
let createFetchURL = "";
			let localProjectsFetchURL = "";
			let localProjectsFetchCount = 0;
			let localProjectsResponseInvalid = false;
			let localProjectsResponseGate = null;
			let includeIncompleteCachedThread = false;
			let localActivityFetchURL = "";
			let localThreadSearchFetchURL = "";
let threadSearchRemoteData = "";
let threadSearchRemoteFetchURLs = [];
let localThreadDataFetchURL = "";
let localThreadDataFetchCount = 0;
let localThreadSummaryFetchCount = 0;
let transientThreadDataFailures = 0;
let lastFetchURL = "";
let lastFetchAuthorization = "";
let lastFetchBridgeHeader = "";
let threadMutationFetchURLs = [];
let threadMutationFetchRequests = [];
let cloudMutationResponseGate = null;
let cloudDeleteResult = true;
let projectWorkflowResponseSuccess = true;
let metadataFetchURL = "";
let metadataFetchAuthorization = "";
let metadataFetchBridgeHeader = "";
let metadataFetchRivetEncoding = "";
let metadataFetchUnsupportedHeader = "";
globalThis.fetch = async (url, init) => {
	const fetchURL = typeof url === "string" ? url : url?.url || String(url);
	lastFetchURL = fetchURL;
	const requestHeaders = new Headers(init?.headers || (url instanceof Request ? url.headers : undefined));
	lastFetchAuthorization = requestHeaders.get("Authorization") || "";
	lastFetchBridgeHeader = requestHeaders.get("X-CLIProxyAPI-Web-Local-Inference") || "";
	const parsedURL = new URL(fetchURL, globalThis.location.href);
	if (parsedURL.pathname.endsWith("/openPuckThread")) {
		const response = new Response(JSON.stringify({ data: JSON.stringify(cloudPuckSessionValues()) }), {
			status: 200,
			statusText: "Cloud Puck",
			headers: { "Content-Type": "application/json", "X-Puck-Metadata": "preserved" },
		});
		Object.defineProperties(response, {
			url: { value: parsedURL.href },
			type: { value: "basic" },
			redirected: { value: true },
		});
		return response;
	}
	if (/\/(archiveThreadCommand|deleteThreadCommand|markThreadUnreadCommand|pinThreadCommand)$/.test(parsedURL.pathname)) {
		threadMutationFetchURLs.push(fetchURL);
		threadMutationFetchRequests.push({
			url: fetchURL,
			accept: new Headers(init?.headers || (url instanceof Request ? url.headers : undefined)).get("Accept") || "",
			credentials: init?.credentials || (url instanceof Request ? url.credentials : ""),
			mode: init?.mode || (url instanceof Request ? url.mode : ""),
			referrerPolicy: init?.referrerPolicy || (url instanceof Request ? url.referrerPolicy : ""),
		});
		if (parsedURL.origin === "https://ampcode.com" && cloudMutationResponseGate) {
			const gate = cloudMutationResponseGate;
			cloudMutationResponseGate = null;
			await gate;
		}
		if (parsedURL.origin === "https://ampcode.com" && parsedURL.pathname.endsWith("/deleteThreadCommand") && !cloudDeleteResult) {
			const data = JSON.stringify([{ _: 1 }, { ok: 2, error: 3 }, false, { code: 4 }, "permission-denied"]);
			return new Response(JSON.stringify({ type: "result", data }), {
				status: 200,
				headers: { "Content-Type": "application/json" },
			});
		}
	}
	if (parsedURL.pathname === "/ampcode/local-activity.json") {
		localActivityFetchURL = fetchURL;
		return new Response(JSON.stringify({
			ok: true,
			threads: [{
				id: secondThreadID,
				threadId: secondThreadID,
				title: "Local Activity Result",
				created: 1784390000000,
				updatedAt: "2026-07-18T16:46:40Z",
				creatorUserID: ampViewerUserID,
				meta: { projectName: "CLIProxyAPI" },
			}],
			usersMap: { [ampViewerUserID]: { id: ampViewerUserID, username: "viewer" } },
			repositories: [{ key: "local:cliproxyapi", name: "CLIProxyAPI", ownerPrefix: "local/", count: 1 }],
			users: [{ id: ampViewerUserID, name: "viewer", count: 1 }],
			repositoryTotalThreadCount: 1,
			userTotalThreadCount: 1,
			hasMore: false,
		}), { status: 200, headers: { "Content-Type": "application/json" } });
	}
	if (parsedURL.pathname === "/ampcode/local-thread-search.json") {
		localThreadSearchFetchURL = fetchURL;
		const archivedOnly = parsedURL.searchParams.get("q") === "archived-only";
		return new Response(JSON.stringify({
			threads: archivedOnly ? [{
				id: firstCloudSearchThreadID,
				title: "Active local text match",
				archived: false,
			}, {
				id: secondCloudSearchThreadID,
				title: "Archived local text match",
				archived: true,
			}] : [{
				id: secondThreadID,
				title: "Persisted local text match",
				created: 1784390000000,
				creator: { id: ampViewerUserID, username: "viewer" },
				creatorUserID: ampViewerUserID,
				matchedSearchText: "persisted local needle transcript",
				meta: { agentMode: "smart", projectName: "CLIProxyAPI" },
				projectName: "CLIProxyAPI",
				summaryStats: { diffStats: { added: 7, deleted: 2 } },
			}],
			hasMore: false,
		}), { status: 200, headers: { "Content-Type": "application/json" } });
	}
	if (parsedURL.pathname.endsWith("/searchThreads") && threadSearchRemoteData) {
		threadSearchRemoteFetchURLs.push(fetchURL);
		const response = new Response(JSON.stringify({ type: "result", data: threadSearchRemoteData }), {
			status: 200,
			headers: { "Content-Type": "application/json" },
		});
		Object.defineProperty(response, "url", { value: fetchURL });
		return response;
	}
			if (parsedURL.pathname === "/ampcode/local-projects.json") {
				localProjectsFetchURL = fetchURL;
				localProjectsFetchCount += 1;
				if (localProjectsResponseGate) {
					const gate = localProjectsResponseGate;
					localProjectsResponseGate = null;
					await gate;
				}
				if (localProjectsResponseInvalid) {
					return new Response(JSON.stringify({ ok: true }), {
						status: 200,
						headers: { "Content-Type": "application/json" },
					});
				}
				const requestedSidebarThreadIDs = parsedURL.searchParams.getAll("cliproxy-sidebar-thread-id");
			return new Response(JSON.stringify({
			ok: true,
			defaultWorkingDirectory: "/Users/aikins01",
			projects: [
				{ id: createdThreadProjectID, name: "CLIProxyAPI", repositoryURL: "https://github.com/router-for-me/CLIProxyAPI.git", workingDirectory: createdThreadWorkDir, localOnly: false, changesWorkflow: "push-to-branch" },
				{ id: secondThreadProjectID, name: "Second Project", workingDirectory: "/Users/aikins01/Developer/second-project" },
				...(parsedURL.searchParams.getAll("cliproxy-sidebar-thread-id").includes(collidingSidebarThreadID) ? [{ id: createdThreadProjectID, name: "Collision Project", namespace: "different-owner", repositoryURL: "https://github.com/different-owner/collision-project.git", workingDirectory: "/Users/aikins01/Developer/collision-project" }] : []),
			],
			archivedThreadIDs: [
				archivedThreadID,
				...(parsedURL.searchParams.getAll("cliproxy-sidebar-thread-id").includes(hoverSidebarThreadID) ? [hoverSidebarThreadID] : []),
			],
			thread: {
				id: createdThreadID,
				threadId: createdThreadID,
				v: 1,
				title: "Use local project",
				pinned: false,
				state: "idle",
				agentState: "idle",
				meta: { executorType: "local-client", usesThreadActors: true, projectID: createdThreadProjectID },
			},
			threads: [
				{
					id: createdThreadID,
					threadId: createdThreadID,
					v: 1,
					title: "Use local project",
					pinned: false,
					state: "idle",
					agentState: "idle",
					meta: { executorType: "local-client", usesThreadActors: true, projectID: createdThreadProjectID },
					creator: { id: "local-user", name: "Local Amp" },
				},
				{
					id: secondThreadID,
					threadId: secondThreadID,
					v: 1,
					title: "Second local thread",
					state: "idle",
					agentState: "idle",
					meta: { executorType: "local-client", usesThreadActors: true, projectID: secondThreadProjectID, projectName: "Second Project", namespace: "aikins01", repositoryURL: "https://github.com/aikins01/second-project.git" },
					creator: { id: "local-user", name: "Local Amp" },
				},
				...(includeIncompleteCachedThread ? [{
					id: incompleteSidebarThreadID,
					threadId: incompleteSidebarThreadID,
					v: 1,
					title: requestedSidebarThreadIDs.includes(incompleteSidebarThreadID) ? "Hydrated incomplete local thread" : "Untitled",
					state: "idle",
					agentState: "idle",
					meta: { projectID: requestedSidebarThreadIDs.includes(incompleteSidebarThreadID) ? createdThreadProjectID : "unresolved-local-project" },
					creator: { id: "local-user", name: "Local Amp" },
				}] : []),
				...(parsedURL.searchParams.getAll("cliproxy-sidebar-thread-id").includes(collidingSidebarThreadID) ? [{
					id: collidingSidebarThreadID,
					threadId: collidingSidebarThreadID,
					v: 1,
					title: "Colliding local project thread",
					state: "idle",
					agentState: "idle",
					env: { initial: { workingDirectory: "/Users/aikins01/Developer/collision-project", workspaceRoot: "/Users/aikins01/Developer/collision-project" } },
					meta: { executorType: "local-client", usesThreadActors: true, projectID: createdThreadProjectID, projectName: "Collision Project", namespace: "different-owner", repositoryURL: "https://github.com/different-owner/collision-project.git" },
					creator: { id: "local-user", name: "Local Amp" },
				}] : []),
				...(parsedURL.searchParams.getAll("cliproxy-sidebar-thread-id").includes(hoverSidebarThreadID) ? [{
					id: hoverSidebarThreadID,
					threadId: hoverSidebarThreadID,
					v: 1,
					title: "Hover hydrated local thread",
					archived: true,
					state: "idle",
					agentState: "idle",
					env: { initial: { trees: [{ displayName: "CLIProxyAPI", uri: "file:///Users/aikins01/Developer/CLIProxyAPI" }] } },
					meta: { executorType: "local-client", usesThreadActors: true, projectID: createdThreadProjectID, projectName: "CLIProxyAPI" },
					creator: { id: "local-user", name: "Local Amp" },
				}] : []),
				...(parsedURL.searchParams.getAll("cliproxy-sidebar-thread-id").includes(searchThreadID) ? [{
					id: searchThreadID,
					threadId: searchThreadID,
					title: "Hydrated local search title",
					meta: { projectID: createdThreadProjectID },
				}] : []),
				...(parsedURL.searchParams.getAll("cliproxy-sidebar-thread-id").includes(searchProjectThreadID) ? [{
					id: searchProjectThreadID,
					threadId: searchProjectThreadID,
					title: "Existing local search title",
					meta: { projectID: createdThreadProjectID },
				}] : []),
			],
			threadTitles: Object.fromEntries(
				parsedURL.searchParams.getAll("cliproxy-sidebar-thread-id")
					.filter((threadID) => threadID === searchThreadID)
					.map((threadID) => [threadID, "Hydrated local search title"]),
			),
		}), {
			status: 200,
			headers: { "Content-Type": "application/json" },
		});
	}
	if (parsedURL.pathname === "/ampcode/local-thread-data.json") {
		localThreadDataFetchURL = fetchURL;
		localThreadDataFetchCount += 1;
		const localThreadID = parsedURL.searchParams.get("cliproxy-thread-id") || "";
		if (localThreadID === transientThreadID && parsedURL.searchParams.get("cliproxy-summary-only") !== "1" && transientThreadDataFailures === 0) {
			transientThreadDataFailures += 1;
			return new Response(JSON.stringify({ message: "thread not found" }), {
				status: 404,
				headers: { "Content-Type": "application/json" },
			});
		}
		if (localThreadID === cloudThreadID) {
			return new Response(JSON.stringify({ message: "thread not found" }), {
				status: 404,
				headers: { "Content-Type": "application/json" },
			});
		}
		if (parsedURL.searchParams.get("cliproxy-summary-only") === "1") {
			localThreadSummaryFetchCount += 1;
			return new Response(JSON.stringify({
				type: "result",
				thread: { id: localThreadID, threadId: localThreadID, v: 9, title: "Local summary" },
			}), {
				status: 200,
				headers: { "Content-Type": "application/json" },
			});
		}
		const response = new Response(JSON.stringify({
			type: "result",
			thread: {
				id: localThreadID,
				threadId: localThreadID,
				creatorUserID: "local-user",
				ownerUserId: "local-user",
				creator: { id: "local-user", name: "Local Amp" },
				v: 9,
				env: { initial: { workingDirectory: createdThreadWorkDir, workspaceRoot: createdThreadWorkDir } },
				messages: [{ messageId: "M-local-history", protocolMessageID: "M-local-history", role: "user", content: [{ type: "text", text: "local history" }] }],
				queuedMessages: [],
			},
			project: null,
			threadActorConfig: { threadId: localThreadID, wsToken: "local-neo", ampURL: "http://127.0.0.1:8317", baseURL: "http://127.0.0.1:8317" },
			creator: { id: "local-user", name: "Local Amp" },
			actorPermissions: { manageThread: true, manageBilling: false },
		}), {
			status: 200,
			headers: { "Content-Type": "application/json" },
		});
		Object.defineProperty(response, "url", { value: fetchURL });
		return response;
	}
	if (parsedURL.pathname === "/metadata" || parsedURL.pathname === "/actors/metadata") {
		metadataFetchURL = fetchURL;
		const headers = new Headers(init?.headers || {});
		metadataFetchAuthorization = headers.get("Authorization") || "";
		metadataFetchBridgeHeader = headers.get("X-CLIProxyAPI-Web-Local-Inference") || "";
		metadataFetchRivetEncoding = headers.get("X-Rivet-Encoding") || "";
		metadataFetchUnsupportedHeader = headers.get("X-Unlisted-Framework-Header") || "";
		return new Response(JSON.stringify({
			clientEndpoint: "http://127.0.0.1:8317",
			clientNamespace: parsedURL.searchParams.get("namespace") || "default",
			clientToken: "local-neo",
			runtime: "engine",
		}), {
			status: 200,
			headers: { "Content-Type": "application/json" },
		});
	}
	if (parsedURL.pathname.endsWith("/listThreadListSidebar")) {
		const data = JSON.stringify([
			{ q: 1 },
			{ "3abror/listThreadListSidebar/": 2 },
			{ v: 3 },
			{ projects: 4, recentThreads: 5 },
			[],
			[],
		]);
		return new Response(JSON.stringify({ type: "result", data }), {
			status: 200,
			headers: { "Content-Type": "application/json", "Content-Length": "1" },
		});
	}
	if (parsedURL.pathname.endsWith("/searchFeedRepositories")) {
		const response = new Response(JSON.stringify({ type: "result", data: JSON.stringify([{ _: 1 }, []]) }), {
			status: 200,
			headers: { "Content-Type": "application/json" },
		});
		Object.defineProperty(response, "url", { value: fetchURL });
		return response;
	}
	if (parsedURL.pathname.endsWith("/createProjectThread")) {
		createFetchURL = fetchURL;
	}
	const body = String(init?.body || "");
	if (parsedURL.pathname.endsWith("/updateOwnedProjectChangesWorkflow") && parsedURL.origin === "http://127.0.0.1:8317") {
		const data = JSON.stringify([
			{ _: 1 },
			{ success: 2, message: 3 },
			projectWorkflowResponseSuccess,
			projectWorkflowResponseSuccess ? "saved" : "rejected",
		]);
		return new Response(JSON.stringify({ type: "result", data }), {
			status: 200,
			headers: { "Content-Type": "application/json" },
		});
	}
	if (requestPayloadText(body).includes(failedThreadID)) {
		const data = JSON.stringify([
			{ _: 1 },
			{ ok: 2, error: 3 },
			false,
			{ message: 4 },
			"missing working directory",
		]);
		return new Response(JSON.stringify({ type: "result", data }), {
			status: 200,
			headers: { "Content-Type": "application/json" },
		});
	}
	const data = JSON.stringify([
		{ _: 1 },
		{ ok: 2, threadID: 3, threadId: 3, workingDirectory: 4, workspaceRoot: 4 },
		true,
		createdThreadID,
		createdThreadWorkDir,
	]);
	return new Response(JSON.stringify({ type: "result", data }), {
		status: 200,
		headers: { "Content-Type": "application/json" },
	});
};
globalThis.WebSocket = NativeWebSocket;
let localAPIKeyPromptCount = 0;
globalThis.prompt = () => {
	localAPIKeyPromptCount += 1;
	return "";
};
globalThis.history = {
	state: null,
	replaceState() {},
	pushState(_state, _title, path) { globalThis.location = new URL(path, globalThis.location.href); },
	back() {},
};
const nativeResponseJSON = Response.prototype.json;
let lastNativeResponseJSONPromise = null;
Response.prototype.json = function(...args) {
	const promise = nativeResponseJSON.apply(this, args);
	lastNativeResponseJSONPromise = promise;
	return promise;
};
if (typeof globalThis.atob !== "function") {
	globalThis.atob = (value) => Buffer.from(value, "base64").toString("binary");
}
if (typeof globalThis.btoa !== "function") {
	globalThis.btoa = (value) => Buffer.from(value, "binary").toString("base64");
}
	require(scriptPath);
	const bridge = globalThis.__cliproxyAmpLocalInference;
	const regroupTestBridge = globalThis.__cliproxyAmpLocalInferenceTest;
assert(bridge && bridge.userscriptVersion === "0.1.202", "bridge userscript version was not exposed");
	assert(typeof regroupTestBridge?.requestLocalSidebarProjectRegroup === "function", "sidebar regroup test bridge was not exposed");
	const projectPageTitle = globalThis.document.title;
	const validProjectHost = new FakeElement("main");
	validProjectHost.appendChild(new FakeElement("h1", "Symbol not found"));
	globalThis.document.title = "Local Project - Amp";
	assert(!regroupTestBridge.localProjectNativeMissingPage(validProjectHost), "unrelated project-page text was classified as a missing page");
	const missingProjectHost = new FakeElement("main");
	missingProjectHost.appendChild(new FakeElement("h1", "Project not found"));
	assert(regroupTestBridge.localProjectNativeMissingPage(missingProjectHost), "native project 404 was not detected");
	globalThis.document.title = projectPageTitle;
	const sharedRepositoryCheckout = { repositoryURL: "https://github.com/router-for-me/CLIProxyAPI.git", workingDirectory: "/tmp/CLIProxyAPI-main" };
	const sharedRepositoryWorktree = { repositoryURL: "https://github.com/router-for-me/CLIProxyAPI.git", workingDirectory: "/tmp/CLIProxyAPI-feature" };
	assert(regroupTestBridge.localProjectCheckoutIdentity(sharedRepositoryCheckout) !== regroupTestBridge.localProjectCheckoutIdentity(sharedRepositoryWorktree), "same-repository worktrees shared a checkout identity");
	assert(!regroupTestBridge.localSidebarProjectMatches(sharedRepositoryCheckout, sharedRepositoryWorktree), "same-repository worktrees matched as one sidebar project");
	assert(regroupTestBridge.diffCaptureReadThreadID("/api/threads/%E0%A4%A/diff-captures/latest") === "", "malformed diff-capture thread path was not rejected");
	assert(globalThis.document.documentElement.getAttribute("data-cliproxy-local-sidebar-hydrating") === "1", "sidebar hydration gate was not installed before rendering");
assert(globalThis.document.documentElement.getAttribute("data-cliproxy-local-inference-version") === "0.1.202", "userscript version was not exposed on the document root");
	class InstrumentedWebSocket extends WebSocket {}
	const instrumentedSocket = new InstrumentedWebSocket("wss://ampcode.com/gateway/userActor/?rvt-method=get&rvt-key=subclass-test");
	assert(instrumentedSocket instanceof InstrumentedWebSocket, "patched WebSocket discarded a derived constructor prototype");
	instrumentedSocket.close();
	globalThis.location = new URL("https://ampcode.com/feed");
	bridge.rememberLocalThreadID(puckThreadID);
	const cloudPuckSessionResponse = await fetch("https://ampcode.com/_app/remote/14dvguk/openPuckThread", {
		method: "POST",
		headers: { "Content-Type": "application/json" },
		body: JSON.stringify({ payload: "" }),
	});
	assert(cloudPuckSessionResponse.ok, "Puck session without a local key did not remain usable");
	const cloudPuckEnvelope = nativeJSONParse(await cloudPuckSessionResponse.clone().text());
	const cloudPuckValues = nativeJSONParse(cloudPuckEnvelope.data);
	const cloudPuckConfig = cloudPuckValues[cloudPuckValues[cloudPuckValues[0]._].threadActorConfig];
	assert(cloudPuckValues[cloudPuckConfig.baseURL] === "https://ampcode.com", "Puck response without a local key was rewritten");
	await fetch("https://ampcode.com/_app/remote/14dvguk/openPuckThread", {
		method: "POST",
		headers: { "Content-Type": "application/json" },
		body: JSON.stringify({ payload: "" }),
	});
	assert(localAPIKeyPromptCount === 1, "Puck session prompted repeatedly for a declined local key");
	assert(!JSON.parse(globalThis.localStorage.getItem(bridge.localThreadIDsStorageKey) || "[]").includes(puckThreadID), "Puck thread was claimed without local authentication");
	await fetch("https://ampcode.com/api/thread-actors", { method: "POST", body: "{}" });
	assert(new URL(lastFetchURL).origin === "https://ampcode.com", "Puck actor HTTP without a local key did not remain on Amp Cloud");
	lastFetchURL = "";
	await fetch("https://ampcode.com/api/threads/find?q=cloud");
	assert(new URL(lastFetchURL).origin === "https://ampcode.com", "cloud thread search without a local key did not remain on Amp Cloud");
	const cloudPuckSocket = new WebSocket("wss://ampcode.com/gateway/threadActor/?rvt-method=get&rvt-key=" + encodeURIComponent(puckThreadID));
	assert(new URL(cloudPuckSocket.url).origin === "wss://ampcode.com", "Puck socket without a local key did not remain on Amp Cloud");
	cloudPuckSocket.close();
	regroupTestBridge.rememberLocalAPIKey("local-key");
	assert(globalThis.sessionStorage.getItem(bridge.apiKeyStorageKey) === null, "pre-identity API key used the obsolete unscoped session key");
	lastFetchURL = "";
	const puckSessionResponse = await fetch("https://ampcode.com/_app/remote/14dvguk/openPuckThread", {
		method: "POST",
		headers: { "Content-Type": "application/json" },
		body: JSON.stringify({ payload: "" }),
	});
	assert(puckSessionResponse.ok && new URL(lastFetchURL).origin === "https://ampcode.com", "Puck session bootstrap did not remain on Amp");
	assert(puckSessionResponse.url === "https://ampcode.com/_app/remote/14dvguk/openPuckThread", "Puck response lost its URL");
	assert(puckSessionResponse.type === "basic" && puckSessionResponse.redirected === true, "Puck response lost immutable Fetch metadata");
	assert(puckSessionResponse.status === 200 && puckSessionResponse.statusText === "Cloud Puck", "Puck response lost its status metadata");
	assert(puckSessionResponse.headers.get("X-Puck-Metadata") === "preserved", "Puck response lost its headers");
	const localPuckClone = puckSessionResponse.clone();
	assert(localPuckClone.url === puckSessionResponse.url && localPuckClone.type === puckSessionResponse.type && localPuckClone.redirected === puckSessionResponse.redirected, "Puck response clone lost Fetch metadata");
	const localPuckEnvelope = nativeJSONParse(await localPuckClone.text());
	const localPuckValues = nativeJSONParse(localPuckEnvelope.data);
	const localPuckConfig = localPuckValues[localPuckValues[localPuckValues[0]._].threadActorConfig];
	assert(localPuckValues[localPuckConfig.baseURL] === "http://127.0.0.1:8317", "Puck response did not receive the local actor base URL");
	assert(localPuckValues[localPuckConfig.ampURL] === "http://127.0.0.1:8317", "Puck response did not receive the local Amp URL");
	assert(localPuckValues[localPuckConfig.wsToken] === "local-key", "Puck response did not receive local actor authentication");
	const localPuckJSON = await puckSessionResponse.clone().json();
	assert(typeof localPuckJSON.data === "string", "Puck response JSON reader did not receive the patched payload");
	assert(typeof await puckSessionResponse.text() === "string" && puckSessionResponse.bodyUsed, "Puck response text reader did not consume the patched body");
	assert(JSON.parse(globalThis.localStorage.getItem(bridge.localThreadIDsStorageKey)).includes(puckThreadID), "Puck thread was not claimed by the local runtime");
	const rememberedPuckSettings = JSON.parse(globalThis.localStorage.getItem(bridge.threadSettingsStorageKey))[puckThreadID];
	assert(rememberedPuckSettings?.agentMode === "puck" && rememberedPuckSettings?.reasoningEffort === "none", "Puck thread settings were not remembered");
	const captureSHA = "a".repeat(40);
	await fetch("https://ampcode.com/api/threads/" + puckThreadID + "/diff-captures/latest");
	assert(new URL(lastFetchURL).origin === "http://127.0.0.1:8317", "remembered local capture latest request was not bridged");
	assert(lastFetchAuthorization === "Bearer local-key" && lastFetchBridgeHeader === "1", "local capture latest request omitted bridge authentication");
	await fetch("https://ampcode.com/api/threads/" + puckThreadID + "/diff-captures/blob/" + captureSHA);
	assert(new URL(lastFetchURL).origin === "http://127.0.0.1:8317", "remembered local capture blob request was not bridged");
	await fetch("https://ampcode.com/api/threads/" + puckThreadID + "/diff-captures/diff?from=" + captureSHA + "&to=" + captureSHA);
	assert(new URL(lastFetchURL).origin === "http://127.0.0.1:8317", "remembered local capture diff request was not bridged");
	await fetch("https://ampcode.com/api/threads/" + puckThreadID + "/diff-captures", { method: "POST", body: "{}" });
	assert(new URL(lastFetchURL).origin === "https://ampcode.com" && lastFetchAuthorization === "" && lastFetchBridgeHeader === "", "capture writer request was exposed through the browser bridge");
	await fetch("https://ampcode.com/api/threads/" + cloudThreadID + "/diff-captures/latest");
	assert(new URL(lastFetchURL).origin === "https://ampcode.com" && lastFetchAuthorization === "" && lastFetchBridgeHeader === "", "unknown capture reader request did not remain on Amp Cloud");
	const puckSocket = new WebSocket("wss://ampcode.com/gateway/threadActor/?rvt-method=get&rvt-key=" + encodeURIComponent(puckThreadID));
	const puckSocketURL = new URL(puckSocket.url);
	assert(puckSocketURL.origin === "ws://127.0.0.1:8317", "Puck actor socket was not bridged locally");
	assert(puckSocketURL.searchParams.get("cliproxy-agent-mode") === "puck", "Puck actor socket lost its mode");
	assert(puckSocketURL.searchParams.get("cliproxy-reasoning-effort") === "none", "Puck actor socket lost its effort");
	puckSocket.close();
	globalThis.location = new URL("https://ampcode.com/threads/" + threadID);
		JSON.parse(JSON.stringify({ user: { id: "U-unrelated", email: "other@example.com" }, workspaces: [] }));
		assert(bridge.diagnostics.authenticatedAmpUserIDCaptureCount === 0, "unrelated nested user was accepted as the authenticated viewer");
	JSON.parse(JSON.stringify({
		user: { id: ampViewerUserID, email: "viewer@example.com", username: "aikins01", firstName: "Aikins", profilePictureUrl: "https://workoscdn.com/images/aikins" },
		userWorkspace: { id: "W-viewer" },
		userFeatures: [],
	}));
	assert(bridge.diagnostics.authenticatedAmpUserIDCaptureCount === 1, "authenticated Amp user id was not captured from initial page data parsing");
	assert(bridge.sidebarTitlesStorageKey.endsWith("." + encodeURIComponent(ampViewerUserID) + "." + encodeURIComponent("http://127.0.0.1:8317")), "sidebar title cache was not scoped to the authenticated Amp user and local server");
	const authenticatedPageDataResponse = new Response(JSON.stringify({
	type: "data",
	nodes: [{
		type: "data",
		data: JSON.stringify([
			{ user: 1, userWorkspace: 4, userFeatures: 5 },
			{ id: 2, email: 3, username: 6, firstName: 7, profilePictureUrl: 8 },
			ampViewerUserID,
			"viewer@example.com",
			null,
			[],
			"aikins01",
			"Aikins",
			"https://workoscdn.com/images/aikins",
		]),
	}],
}));
	Object.defineProperty(authenticatedPageDataResponse, "url", { value: "https://ampcode.com/__data" });
	await authenticatedPageDataResponse.json();
	assert(bridge.diagnostics.authenticatedAmpUserIDCaptureCount === 1, "authenticated Amp user id was not captured from SvelteKit page data");
	new WebSocket("wss://ampcode.com/gateway/userActor/?rvt-method=get&rvt-key=" + encodeURIComponent(ampViewerUserID));
	assert(bridge.diagnostics.authenticatedAmpUserIDCaptureCount === 1, "user actor socket changed the authenticated Amp user id");
	const activityPageDataResponse = new Response(JSON.stringify({
		type: "data",
		nodes: [{
			type: "data",
			data: [
				{ threads: 1, usersMap: 2, repositories: 3, users: 4, repositoryTotalThreadCount: 5, userTotalThreadCount: 6, hasMore: 7 },
				[8],
				{},
				[],
				[],
				75,
				75,
				true,
				{ id: 9, title: 10 },
				secondThreadID,
				"Cloud Activity Result",
			],
		}],
	}));
	Object.defineProperty(activityPageDataResponse, "url", { value: "https://ampcode.com/feed/__data.json?q=Local" });
	const activityPageData = await activityPageDataResponse.json();
	const activityValues = activityPageData.nodes[0].data;
	const localActivityThreadRef = activityValues[activityValues[0].threads][0];
	assert(activityValues[activityValues[localActivityThreadRef].id] === secondThreadID, "local Activity thread was not merged into SvelteKit route data");
	assert(activityValues[activityValues[localActivityThreadRef].title] === "Local Activity Result", "local Activity title was not preserved");
	assert(activityValues[activityValues[0].threads].length === 1, "local Activity merge changed the cloud page size");
	assert(activityValues[activityValues[0].repositoryTotalThreadCount] === 75 && activityValues[activityValues[0].userTotalThreadCount] === 75, "local Activity merge double-counted cloud totals");
assert(new URL(localActivityFetchURL).searchParams.get("q") === "Local", "Activity search query was not forwarded to the local runtime");
for (const key of ["cliproxy-user-id", "cliproxy-user-username", "cliproxy-user-first-name", "cliproxy-user-last-name", "cliproxy-user-email", "cliproxy-user-profile-picture-url"]) {
	assert(!new URL(localActivityFetchURL).searchParams.has(key), "authenticated Amp profile data leaked into the local Activity URL: " + key);
}
	assert(bridge.diagnostics.localActivityFetchCount === 1 && bridge.diagnostics.localActivityMergeCount > 0, "Activity merge diagnostics were not recorded");
	const repositoryFilterResponse = await fetch("https://ampcode.com/_app/remote/mnzyo0/searchFeedRepositories", {
		method: "POST",
		headers: { "Content-Type": "application/json" },
		body: JSON.stringify({
			payload: encodeDevalue([{ query: 1, repo: 2, time: 3, userID: 4 }, "clip", "", "7d", ampViewerUserID]),
			refreshes: [],
		}),
	});
	const repositoryFilterData = await repositoryFilterResponse.json();
	const repositoryFilterValues = JSON.parse(repositoryFilterData.data);
	assert(repositoryFilterValues[1].length === 1, "local Activity repository search result was not merged");
	assert(new URL(localActivityFetchURL).searchParams.get("time") === "7d", "Activity repository search time filter was not decoded from the remote action");
	assert(new URL(localActivityFetchURL).searchParams.get("user") === ampViewerUserID, "Activity repository search user filter was not decoded from the remote action");
const unrelatedGraphPasses = bridge.diagnostics.decodedGraphPassCount;
const unrelatedIDPayload = JSON.parse('{"id":"generic-object","payload":[{"value":1}]}');
assert(unrelatedIDPayload.id === "generic-object", "unrelated id payload changed");
assert(bridge.diagnostics.decodedGraphPassCount === unrelatedGraphPasses, "unrelated id payload triggered decoded graph traversal");
	JSON.parse(JSON.stringify({
		id: threadID,
		v: 6,
		messages: [],
		workingDirectory: "/Users/aikins01/Developer/vela/on-chain",
		env: { trees: [{ uri: "file:///Users/aikins01/Developer/bort" }] },
	}));
	const directOpenedThreadSocket = new WebSocket("wss://ampcode.com/gateway/threadActor/?rvt-method=get&rvt-key=" + encodeURIComponent(threadID));
	assert(directOpenedThreadSocket instanceof WebSocket, "wrapped websocket lost native instanceof behavior");
	assert(WebSocket.OPEN === NativeWebSocket.OPEN, "wrapped websocket lost native static constants");
	class DerivedWebSocket extends WebSocket {}
	const derivedSocket = new DerivedWebSocket("wss://ampcode.com/unrelated");
	assert(derivedSocket instanceof WebSocket, "derived websocket construction did not return a native socket");
	const directOpenedThreadURL = new URL(directOpenedThreadSocket.url);
	assert(directOpenedThreadURL.searchParams.get("cliproxy-working-directory") === "/Users/aikins01/Developer/bort", "stale thread directory overrode the task tree");
	JSON.parse(JSON.stringify({ id: threadID, v: 7, messages: [] }));
	const directOpenedThreadResume = '{"type":"client_resume","version":0}';
	directOpenedThreadSocket.send(directOpenedThreadResume);
	assert(JSON.parse(directOpenedThreadSocket.sent.at(-1)).version === 7, "active direct-open thread base did not rewrite zero resume before local id discovery");
	bridge.rememberLocalThreadID(threadID);
	globalThis.location = new URL("https://ampcode.com/threads/" + secondThreadID);
	const navigatedThreadSocket = new WebSocket("wss://ampcode.com/gateway/threadActor/?rvt-method=get&rvt-key=" + encodeURIComponent(secondThreadID));
	const navigatedThreadURL = new URL(navigatedThreadSocket.url);
	assert(!navigatedThreadURL.searchParams.has("cliproxy-working-directory"), "active task fallback directory leaked into a different task socket");
	assert(!navigatedThreadURL.searchParams.has("cliproxy-agent-mode"), "active task fallback mode leaked into a different task socket");
	assert(!navigatedThreadURL.searchParams.has("cliproxy-reasoning-effort"), "active task fallback effort leaked into a different task socket");
	assert(directOpenedThreadSocket.readyState === NativeWebSocket.CLOSED, "inactive local thread socket survived navigation");
	assert(navigatedThreadSocket.readyState === NativeWebSocket.CONNECTING, "active local thread socket was closed during navigation cleanup");
	assert(bridge.diagnostics.staleLocalThreadSocketCloseCount === 1, "inactive local thread socket close was not recorded");
	assert(bridge.diagnostics.trackedLocalThreadSocketCount === 1, "inactive local thread socket remained tracked");
	const staleReconnectSocket = new WebSocket("wss://ampcode.com/gateway/threadActor/?rvt-method=get&rvt-key=" + encodeURIComponent(threadID));
	assert(staleReconnectSocket.readyState === NativeWebSocket.CLOSED, "off-route local thread reconnect remained open");
	assert(navigatedThreadSocket.readyState === NativeWebSocket.CONNECTING, "stale thread reconnect closed the active thread socket");
	assert(bridge.diagnostics.staleLocalThreadSocketCloseCount === 2, "off-route local thread reconnect close was not recorded");
	assert(bridge.diagnostics.trackedLocalThreadSocketCount === 1, "off-route reconnect changed the tracked socket count");
	globalThis.location = new URL("https://ampcode.com/settings/plugins/manage");
	const offRouteReconnectSocket = new WebSocket("wss://ampcode.com/gateway/threadActor/?rvt-method=get&rvt-key=" + encodeURIComponent(secondThreadID));
	assert(offRouteReconnectSocket.readyState === NativeWebSocket.CONNECTING, "server-owned thread socket was treated as a stale local reconnect");
	assert(new URL(offRouteReconnectSocket.url).origin === "wss://ampcode.com", "server-owned thread socket was bridged after leaving the thread route");
	assert(bridge.diagnostics.staleLocalThreadSocketCloseCount === 2, "server-owned thread socket changed the stale local close count");
	globalThis.location = new URL("https://ampcode.com/threads/" + threadID);
	const activeThreadPath = globalThis.location.pathname;
	const boundedThreadIDs = [];
	for (let i = 1; i <= 80; i += 1) {
		const boundedThreadID = "T-00000000-0000-0000-0000-" + i.toString(16).padStart(12, "0");
		boundedThreadIDs.push(boundedThreadID);
	}
	globalThis.localStorage.setItem(bridge.localThreadIDsStorageKey, JSON.stringify(boundedThreadIDs));
	for (let i = 0; i < boundedThreadIDs.length; i += 1) {
		const boundedThreadID = boundedThreadIDs[i];
		JSON.parse(JSON.stringify({ id: boundedThreadID, v: i + 1, messages: [] }));
	}
	globalThis.location.pathname = activeThreadPath;
	assert(bridge.diagnostics.loadedThreadBaseVersionEntryCount === 64, "loaded thread base versions were not bounded");
	const storedThreadIDs = [];
	const storedDirectories = {};
	const storedSettings = {};
	for (let i = 1; i <= bridge.localThreadStorageLimit + 20; i += 1) {
		const storedThreadID = "T-10000000-0000-4000-8000-" + i.toString(16).padStart(12, "0");
		storedThreadIDs.push(storedThreadID);
		storedDirectories[storedThreadID] = "/tmp/project-" + i;
		storedSettings[storedThreadID] = { agentMode: "medium", reasoningEffort: "medium" };
	}
	storedThreadIDs.unshift(threadID);
	storedDirectories[threadID] = createdThreadWorkDir;
	storedSettings[threadID] = { agentMode: "low", reasoningEffort: "medium" };
	globalThis.localStorage.setItem(bridge.localThreadIDsStorageKey, JSON.stringify(storedThreadIDs));
	globalThis.localStorage.setItem(bridge.threadWorkingDirectoriesStorageKey, JSON.stringify(storedDirectories));
	globalThis.localStorage.setItem(bridge.threadSettingsStorageKey, JSON.stringify(storedSettings));
	bridge.rememberLocalThreadID(threadID);
	const boundedDirectories = JSON.parse(globalThis.localStorage.getItem(bridge.threadWorkingDirectoriesStorageKey));
	const boundedSettings = JSON.parse(globalThis.localStorage.getItem(bridge.threadSettingsStorageKey));
	assert(Object.keys(boundedDirectories).length <= bridge.localThreadStorageLimit, "thread working-directory storage was not bounded");
	assert(Object.keys(boundedSettings).length <= bridge.localThreadStorageLimit, "thread settings storage was not bounded");
	assert(boundedDirectories[threadID] === createdThreadWorkDir, "active thread working directory was pruned");
	assert(boundedSettings[threadID]?.agentMode === "low", "active thread settings were pruned");
	globalThis.localStorage.removeItem(bridge.threadWorkingDirectoriesStorageKey);
	globalThis.localStorage.removeItem(bridge.threadSettingsStorageKey);
const unrelatedResponse = new Response('{"ok":true}');
Object.defineProperty(unrelatedResponse, "url", { value: "https://ampcode.com/api/unrelated" });
const unrelatedResponseJSONPromise = unrelatedResponse.json();
assert(unrelatedResponseJSONPromise === lastNativeResponseJSONPromise, "unrelated response json did not use the native promise fast path");
assert((await unrelatedResponseJSONPromise).ok === true, "unrelated response json changed decoded data");
const throwingURLResponse = new Response('{"ok":true}');
Object.defineProperty(throwingURLResponse, "url", { get() { throw new Error("unexpected url access"); } });
assert((await throwingURLResponse.json()).ok === true, "response url inspection changed native json behavior");
		globalThis.localStorage.setItem(bridge.localThreadIDsStorageKey, JSON.stringify([threadID, secondThreadID]));
		globalThis.localStorage.setItem(bridge.workingDirectoryStorageKey, createdThreadWorkDir);
		bridge.rememberLocalThreadID(transientThreadID);
		const transientFetchCount = localThreadDataFetchCount;
		const transientThreadResponse = await fetch("https://ampcode.com/threads/" + transientThreadID + "/__data");
		const transientThreadData = await transientThreadResponse.json();
		assert(transientThreadResponse.ok && transientThreadData.thread.id === transientThreadID, "transient local thread reload did not recover");
		assert(transientThreadDataFailures === 1 && localThreadDataFetchCount === transientFetchCount + 2, "transient local thread reload did not retry exactly once");
		const destinationThreadResourceResponse = await fetch("https://ampcode.com/threads/" + secondThreadID + "/__data");
		await destinationThreadResourceResponse.json();
		assert(bridge.diagnostics.lastLoadedThreadBaseThreadID === secondThreadID && bridge.diagnostics.lastLoadedThreadBaseVersion === 9, "destination thread base was not captured before client navigation committed");
		const localThreadResourceResponse = await fetch("https://ampcode.com/threads/" + threadID + "/__data");
	const localThreadResource = await localThreadResourceResponse.json();
	assert(new URL(localThreadDataFetchURL).pathname === "/ampcode/local-thread-data.json", "local thread resource was not bridged");
	assert(new URL(localThreadDataFetchURL).searchParams.get("cliproxy-thread-id") === threadID, "local thread resource id was not forwarded");
	assert(localThreadResource.thread.id === threadID && localThreadResource.thread.v === 9, "local thread resource identity/version mismatch");
	assert(localThreadResource.thread.creatorUserID === ampViewerUserID && localThreadResource.thread.ownerUserId === ampViewerUserID, "local thread resource was not projected as owned by the authenticated Amp user");
	assert(localThreadResource.thread.creator.id === ampViewerUserID && localThreadResource.creator.id === ampViewerUserID, "local thread resource creator objects were not projected as the authenticated Amp user");
	assert(localThreadResource.project?.id === createdThreadProjectID, "local checkout project was not associated by working directory");
	assert(localThreadResource.project?.changesWorkflow === "push-to-branch", "hybrid local checkout did not use the Push to Branch workflow");
	assert(Array.isArray(localThreadResource.project?.additionalRepositories), "local checkout project omitted the additional repositories array");
		assert(Array.isArray(localThreadResource.thread.messages) && localThreadResource.thread.messages.length === 1, "local thread resource lost transcript messages");
		assert(localThreadResource.threadActorConfig.threadId === threadID, "local thread resource actor config mismatch");
		assert(localThreadResource.threadActorConfig.wsToken === "local-key", "local thread resource actor config did not carry the worker token");
	const localThreadViewResourceResponse = await fetch("https://ampcode.com/threads/" + threadID + "/view/__data");
	const localThreadViewResource = await localThreadViewResourceResponse.json();
	assert(new URL(localThreadDataFetchURL).pathname === "/ampcode/local-thread-data.json", "local thread view resource was not bridged");
	assert(new URL(localThreadDataFetchURL).searchParams.get("cliproxy-thread-id") === threadID, "local thread view resource id was not forwarded");
	assert(localThreadViewResource.thread.id === threadID && localThreadViewResource.threadActorConfig.threadId === threadID, "local thread view resource payload mismatch");
	localThreadDataFetchURL = "";
	await fetch("https://ampcode.com/threads/" + threadID + "/__data.json?x-sveltekit-invalidated=0010");
	assert(new URL(lastFetchURL).pathname === "/threads/" + threadID + "/__data.json", "SvelteKit thread data request was rewritten");
	assert(localThreadDataFetchURL === "", "SvelteKit thread data request used the plain thread resource bridge");
	await fetch("https://ampcode.com/threads/" + threadID + "/view/__data.json?x-sveltekit-invalidated=0010");
	assert(new URL(lastFetchURL).pathname === "/threads/" + threadID + "/view/__data.json", "SvelteKit thread view data request was rewritten");
	assert(localThreadDataFetchURL === "", "SvelteKit thread view data request used the plain thread resource bridge");
	let replacedLocationURL = "";
	globalThis.location = new URL("https://ampcode.com/threads/" + pastThreadID + "/view");
	globalThis.location.replace = (value) => { replacedLocationURL = String(value); };
	globalThis.document.readyState = "complete";
	globalThis.history.replaceState = () => { throw new Error("history fallback used when location.replace was available"); };
	const discoveryFetchCount = localThreadDataFetchCount;
	const discoverySummaryFetchCount = localThreadSummaryFetchCount;
	const discoveredPastThreadResponse = await fetch("https://ampcode.com/threads/" + pastThreadID + "/view/__data");
	const discoveredPastThread = await discoveredPastThreadResponse.json();
	await new Promise((resolve) => setTimeout(resolve, 0));
	assert(discoveredPastThread.thread.id === pastThreadID, "past local thread resource was not bridged after discovery");
	assert(localThreadDataFetchCount === discoveryFetchCount + 2, "past local thread discovery did not separate summary lookup from thread data");
	assert(localThreadSummaryFetchCount === discoverySummaryFetchCount + 1, "past local thread discovery did not use a summary-only lookup");
	assert(JSON.parse(globalThis.localStorage.getItem(bridge.localThreadIDsStorageKey)).includes(pastThreadID), "past local thread was not remembered after discovery");
	assert(replacedLocationURL === "/threads/" + pastThreadID, "past local thread view route was not normalized");
	globalThis.location = new URL("https://ampcode.com/threads/" + cloudThreadID);
	await fetch("https://ampcode.com/threads/" + cloudThreadID + "/__data");
	assert(new URL(lastFetchURL).pathname === "/threads/" + cloudThreadID + "/__data", "cloud thread resource was bridged after failed local discovery");
	assert(!JSON.parse(globalThis.localStorage.getItem(bridge.localThreadIDsStorageKey)).includes(cloudThreadID), "cloud thread was remembered as local");
	globalThis.location = new URL("https://ampcode.com/threads/" + threadID);
	const encodeGatewayInput = (value) => Buffer.from(JSON.stringify(value)).toString("base64url");
	new WebSocket("wss://ampcode.com/gateway/threadActor/?rvt-method=get&rvt-input=" + encodeURIComponent(encodeGatewayInput({ input: { threadID } })));
	const decodedGatewayInputURL = new URL(NativeWebSocket.instances.at(-1).url);
	assert(decodedGatewayInputURL.searchParams.get("cliproxy-bootstrap-executor") === "true", "decoded rvt-input thread id was not recognized");
	new WebSocket("wss://ampcode.com/gateway/threadActor/?rvt-method=get&rvt-input=" + encodeURIComponent(encodeGatewayInput({ input: JSON.stringify({ threadId: threadID }) })));
	const stringGatewayInputURL = new URL(NativeWebSocket.instances.at(-1).url);
	assert(stringGatewayInputURL.searchParams.get("cliproxy-bootstrap-executor") === "true", "string-wrapped rvt-input thread id was not recognized");
	new WebSocket("wss://ampcode.com/gateway/threadActor/?rvt-method=get&rvt-input=" + encodeURIComponent(encodeGatewayInput({ input: { payload: { id: secondThreadID }, message: "unrelated " + secondThreadID } })));
	const unrelatedGatewayInputURL = new URL(NativeWebSocket.instances.at(-1).url);
	assert(!unrelatedGatewayInputURL.searchParams.has("cliproxy-bootstrap-executor"), "unrelated rvt-input id was treated as a thread id");
	const strayHigh = new FakeElement("span", "High");
	const modeLow = new FakeElement("button", "Low");
	modeLow.setAttribute("aria-haspopup", "menu");
	documentQueryElements = [strayHigh, modeLow];
	globalThis.localStorage.setItem(bridge.threadSettingsStorageKey, JSON.stringify({ [threadID]: { agentMode: "low", reasoningEffort: "medium" } }));
		const inheritedModeSocket = new WebSocket("wss://ampcode.com/gateway?key=" + encodeURIComponent(threadID));
		const inheritedModeURL = new URL(NativeWebSocket.instances.at(-1).url);
		assert(inheritedModeURL.origin === "ws://127.0.0.1:8317", "gateway websocket was not bridged locally");
		assert(inheritedModeURL.searchParams.get("cliproxy-agent-mode") === "low", "unrelated High text overrode real mode control");
		assert(inheritedModeURL.searchParams.get("cliproxy-reasoning-effort") === "medium", "low mode reasoning effort was not inherited");
		const localHistoryResume = '{"type":"client_resume","version":0}';
		inheritedModeSocket.send(localHistoryResume);
		assert(JSON.parse(inheritedModeSocket.sent.at(-1)).version === 9, "local thread history base did not rewrite zero resume");
		const genericAriaModeHigh = new FakeElement("button", "High");
		genericAriaModeHigh.setAttribute("aria-label", "Agent mode");
		documentQueryElements = [genericAriaModeHigh];
		new WebSocket("wss://ampcode.com/gateway?key=" + encodeURIComponent(threadID));
		const genericAriaModeURL = new URL(NativeWebSocket.instances.at(-1).url);
		assert(genericAriaModeURL.searchParams.get("cliproxy-agent-mode") === "low", "visible mode leaked into exact task socket settings");
		assert(genericAriaModeURL.searchParams.get("cliproxy-reasoning-effort") === "medium", "visible reasoning effort leaked into exact task socket settings");
		documentQueryElements = [];
	const metadataResponse = await fetch("http://127.0.0.1:8317/metadata?namespace=default");
assert(metadataResponse.ok, "metadata fetch failed");
assert(metadataFetchURL === "http://127.0.0.1:8317/metadata?namespace=default", "local metadata URL changed");
assert(metadataFetchAuthorization === "Bearer local-key", "local metadata fetch missing API key");
assert(metadataFetchBridgeHeader === "1", "local metadata fetch missing bridge header");
const actorsMetadataResponse = await fetch("https://ampcode.com/actors/metadata?namespace=default", { headers: { "X-Rivet-Encoding": "bare", "X-Unlisted-Framework-Header": "blocked" } });
assert(actorsMetadataResponse.ok, "actors metadata fetch failed");
const bridgedActorsMetadataURL = new URL(metadataFetchURL);
assert(bridgedActorsMetadataURL.origin === "http://127.0.0.1:8317", "same-origin actors metadata was not bridged to local base");
assert(bridgedActorsMetadataURL.pathname === "/actors/metadata", "actors metadata path changed");
assert(metadataFetchAuthorization === "Bearer local-key", "actors metadata fetch missing API key");
assert(metadataFetchBridgeHeader === "1", "actors metadata fetch missing bridge header");
assert(metadataFetchRivetEncoding === "bare", "supported Rivet header was not forwarded");
assert(metadataFetchUnsupportedHeader === "", "unsupported source header was forwarded to local CORS request");
const assertPlainConfig = (config, label, expectedThreadID = label === "created plain" ? createdThreadID : threadID) => {
	assert(config && typeof config === "object", label + " config missing");
	assert(config.threadId === expectedThreadID, label + " threadId mismatch");
		assert(config.wsToken === "local-key", label + " wsToken mismatch");
	assert(config.ampURL === "http://127.0.0.1:8317", label + " ampURL mismatch");
	assert(config.baseURL === "http://127.0.0.1:8317", label + " baseURL mismatch");
	assert(config.capability === "write", label + " capability mismatch");
	assert(config.poolName === "default", label + " poolName mismatch");
	assert(config.requiresSudoForWrite === false, label + " requiresSudoForWrite mismatch");
	assert(config.requiresSudoForTerminal === false, label + " requiresSudoForTerminal mismatch");
	assert(config.threadActorTransport === "json-rpc", label + " threadActorTransport mismatch");
};
const plainThreadSource = {
	id: threadID,
	v: 23,
	title: "Local",
	messages: [
		{ messageId: "M-0000000000000000000001", role: "assistant", content: [{ type: "tool_use", id: "TU-plain", name: "shell_command", input: { command: "pwd", request: { id: threadID, threadId: threadID, baseURL: "https://service.example", ampURL: "https://amp.example", wsToken: "tool-token", capability: "write", poolName: "default", threadActorTransport: "json-rpc", hasExecutor: false, executorConnected: false, threadActorConfig: { threadId: threadID, baseURL: "https://service.example" } } } }] },
		{ messageId: "M-0000000000000000000002", role: "user", content: [{ type: "tool_result", toolUseID: "TU-plain", content: "/workspace" }] },
	],
	compactionRecords: [{ cutMessageId: "M-0000000000000000000002", createdAt: "2026-07-12T00:00:00Z" }],
};
const preservedPlainTranscript = JSON.stringify({ messages: plainThreadSource.messages, compactionRecords: plainThreadSource.compactionRecords });
const plain = JSON.parse(JSON.stringify({ current: { thread: plainThreadSource, threadActorConfig: null } }));
assertPlainConfig(plain.current.threadActorConfig, "plain");
assert(bridge.diagnostics.lastPatchedThreadID === threadID, "plain patch did not record thread ID");
assert(JSON.stringify({ messages: plain.current.thread.messages, compactionRecords: plain.current.thread.compactionRecords }) === preservedPlainTranscript, "plain route patch changed transcript IDs, tool blocks, or compaction records");
const referencedPlainConfig = JSON.parse(JSON.stringify({ threadActorConfig: { threadId: threadID, baseURL: "https://ampcode.com" } }));
assert(referencedPlainConfig.threadActorConfig.baseURL === "http://127.0.0.1:8317", "referenced minimal plain config baseURL was not patched");
assert(referencedPlainConfig.threadActorConfig.ampURL === "http://127.0.0.1:8317", "referenced minimal plain config ampURL was not patched");
const standalonePlainConfig = JSON.parse(JSON.stringify({
	threadId: threadID,
	baseURL: "https://ampcode.com",
	wsToken: "remote-token",
	capability: "write",
	poolName: "default",
}));
assert(standalonePlainConfig.baseURL === "http://127.0.0.1:8317", "standalone plain actor config baseURL was not patched");
assert(standalonePlainConfig.ampURL === "http://127.0.0.1:8317", "standalone plain actor config ampURL was not patched");
const devalueShapedToolInput = [
	{ request: 1 },
	{ id: 2, threadId: 2, baseURL: 3, ampURL: 4, wsToken: 5, hasExecutor: 6, executorConnected: 6, capability: 7, poolName: 8, threadActorConfig: 9 },
	threadID,
	"https://service.example",
	"https://amp.example",
	"tool-token",
	false,
	"write",
	"default",
	{ threadId: 2, baseURL: 3 },
];
const preservedDevalueShapedToolInput = JSON.stringify(devalueShapedToolInput);
const devalueShapedToolEnvelope = JSON.parse(JSON.stringify({ messages: [{ content: [{ type: "tool_use", input: devalueShapedToolInput }] }] }));
const patchedDevalueShapedToolInput = JSON.stringify(devalueShapedToolEnvelope.messages[0].content[0].input);
assert(patchedDevalueShapedToolInput === preservedDevalueShapedToolInput, "devalue-shaped tool input was patched as an actor config: " + patchedDevalueShapedToolInput);
assert(devalueShapedToolEnvelope.messages[0].content[0].input.length === devalueShapedToolInput.length, "devalue-shaped tool input table length changed");
assert(!patchedDevalueShapedToolInput.includes("http://127.0.0.1:8317"), "devalue-shaped tool input retained the local base URL");
assert(devalueShapedToolEnvelope.messages[0].content[0].input[1].threadActorConfig === 9, "devalue-shaped tool input threadActorConfig reference changed");
const devalueTranscriptTable = [
	{ messages: 1 },
	[2],
	{ role: 3, content: 4 },
	"assistant",
	[5],
	{ type: 6, input: 7 },
	"tool_use",
	{ threadActorConfig: 8 },
	{ threadId: 9, baseURL: 10, wsToken: 11, capability: 12, poolName: 13 },
	threadID,
	"https://service.example",
	"tool-token",
	"write",
	"default",
];
const preservedDevalueTranscriptTable = JSON.stringify(devalueTranscriptTable);
const patchedDevalueTranscriptTable = JSON.parse(JSON.stringify(devalueTranscriptTable));
assert(JSON.stringify(patchedDevalueTranscriptTable) === preservedDevalueTranscriptTable, "devalue transcript table actor payload was patched");
const sharedDevalueConfig = [
	{ threadActorConfig: 8, messages: 1 },
	[2],
	{ role: 3, content: 4 },
	"assistant",
	[5],
	{ type: 6, input: 7 },
	"tool_use",
	{ threadActorConfig: 8 },
	{ threadId: 9, baseURL: 10 },
	threadID,
	"https://service.example",
];
const preservedSharedTranscriptConfig = JSON.stringify(sharedDevalueConfig[8]);
const patchedSharedDevalueConfig = JSON.parse(JSON.stringify(sharedDevalueConfig));
assert(JSON.stringify(patchedSharedDevalueConfig[8]) === preservedSharedTranscriptConfig, "shared transcript actor config was mutated");
assert(patchedSharedDevalueConfig[7].threadActorConfig === 8, "shared transcript actor config reference changed");
const externalSharedConfigIndex = patchedSharedDevalueConfig[0].threadActorConfig;
assert(externalSharedConfigIndex !== 8, "external actor config reference was not isolated from transcript data");
const externalSharedConfig = patchedSharedDevalueConfig[externalSharedConfigIndex];
const externalSharedDeref = (value) => Number.isInteger(value) ? patchedSharedDevalueConfig[value] : value;
assert(externalSharedDeref(externalSharedConfig.baseURL) === "http://127.0.0.1:8317", "isolated external actor config baseURL was not patched");
assert(externalSharedDeref(externalSharedConfig.ampURL) === "http://127.0.0.1:8317", "isolated external actor config ampURL was not patched");
const referencedDevalueConfig = JSON.parse(JSON.stringify([
	{ threadActorConfig: 1 },
	{ threadId: 2, baseURL: 3 },
	threadID,
	"https://ampcode.com",
]));
const referencedDevalueDeref = (value) => Number.isInteger(value) ? referencedDevalueConfig[value] : value;
assert(referencedDevalueDeref(referencedDevalueConfig[1].baseURL) === "http://127.0.0.1:8317", "referenced minimal devalue config baseURL was not patched");
assert(referencedDevalueDeref(referencedDevalueConfig[1].ampURL) === "http://127.0.0.1:8317", "referenced minimal devalue config ampURL was not patched");
const standaloneDevalueConfig = JSON.parse(JSON.stringify([
	{ threadId: 1, baseURL: 2, wsToken: 3, capability: 4, poolName: 5 },
	threadID,
	"https://ampcode.com",
	"remote-token",
	"write",
	"default",
]));
const standaloneDevalueDeref = (value) => Number.isInteger(value) ? standaloneDevalueConfig[value] : value;
assert(standaloneDevalueDeref(standaloneDevalueConfig[0].baseURL) === "http://127.0.0.1:8317", "standalone devalue actor config baseURL was not patched");
assert(standaloneDevalueDeref(standaloneDevalueConfig[0].ampURL) === "http://127.0.0.1:8317", "standalone devalue actor config ampURL was not patched");
const rawZeroResume = '{ "type": "client_resume", "version": 0 }';
inheritedModeSocket.send(rawZeroResume);
const rewrittenRawResume = inheritedModeSocket.sent.at(-1);
assert(typeof rewrittenRawResume === "string", "raw resume payload type changed");
assert(JSON.parse(rewrittenRawResume).version === 23, "matching plain loaded base did not rewrite raw zero resume");
JSON.parse(JSON.stringify({ id: threadID, v: 17, data: { messages: [] } }));
inheritedModeSocket.send(rawZeroResume);
assert(JSON.parse(inheritedModeSocket.sent.at(-1)).version === 17, "plain envelope with outer identity and nested transcript did not rewrite zero resume");
const stalePositiveResume = '{ "type": "client_resume", "version": 11 }';
inheritedModeSocket.send(stalePositiveResume);
assert(JSON.parse(inheritedModeSocket.sent.at(-1)).version === 17, "stale positive resume did not advance to the loaded base");
const currentResume = '{ "type": "client_resume", "version": 17 }';
inheritedModeSocket.send(currentResume);
assert(inheritedModeSocket.sent.at(-1) === currentResume, "current resume was changed");
const newerResume = '{ "type": "client_resume", "version": 18 }';
inheritedModeSocket.send(newerResume);
assert(inheritedModeSocket.sent.at(-1) === newerResume, "newer resume was changed");
const unrelatedFrame = '{ "type": "client_set_thread_title", "title": "unchanged" }';
inheritedModeSocket.send(unrelatedFrame);
assert(inheritedModeSocket.sent.at(-1) === unrelatedFrame, "unrelated websocket frame was changed");
const binaryFrame = new Uint8Array([1, 2, 3]);
inheritedModeSocket.send(binaryFrame);
assert(inheritedModeSocket.sent.at(-1) === binaryFrame, "binary websocket frame was changed");
const userActorSocket = new WebSocket("wss://ampcode.com/gateway/userActor/?rvt-method=get&rvt-key=" + encodeURIComponent(ampViewerUserID));
userActorSocket.send(rawZeroResume);
assert(userActorSocket.sent.at(-1) === rawZeroResume, "user actor resume frame was changed");
	globalThis.location = new URL("https://ampcode.com/feed");
	const serverOnlyThreadSocket = new WebSocket("wss://ampcode.com/gateway/threadActor/?rvt-method=get&rvt-key=" + encodeURIComponent(cloudThreadID));
	assert(new URL(serverOnlyThreadSocket.url).origin === "wss://ampcode.com", "server-only thread actor socket was bridged locally");
	const feedLocalThreadSocket = new WebSocket("wss://ampcode.com/gateway/threadActor/?rvt-method=get&rvt-key=" + encodeURIComponent(threadID));
	assert(new URL(feedLocalThreadSocket.url).origin === "ws://127.0.0.1:8317", "remembered local thread actor socket was not bridged from a non-thread page");
	globalThis.location = new URL("https://ampcode.com/threads/" + threadID);
	const noBaseSocket = new WebSocket("wss://ampcode.com/gateway/threadActor/?rvt-method=get&rvt-key=" + encodeURIComponent(secondThreadID));
	const noBaseResume = '{"type":"client_resume","version":0}';
	const unknownBaseSocket = new WebSocket("wss://ampcode.com/gateway/threadActor/?rvt-method=get&rvt-key=" + encodeURIComponent(cloudThreadID));
	unknownBaseSocket.send(noBaseResume);
	assert(unknownBaseSocket.sent.at(-1) === noBaseResume, "mismatched or missing loaded base changed zero resume");
const values = JSON.parse(JSON.stringify([
	{ current: 1 },
	{ thread: 2, creator: 10 },
	{ id: 3, title: 4, v: 5, messages: 6, creator: 7, creatorUserID: 8, ownerUserId: 8 },
	threadID,
	"Local",
	37,
	[],
	{ id: 8, name: 9 },
	"local-user",
	"Local Amp",
	{ id: 8, name: 9 },
	{ threadCreator: 7, entryCreator: 10 },
]));
const configIndex = values[1].threadActorConfig;
assert(Number.isInteger(configIndex), "devalue threadActorConfig was not a reference");
const devalueConfig = values[configIndex];
const deref = (value) => Number.isInteger(value) ? values[value] : value;
assert(deref(devalueConfig.threadId) === threadID, "devalue threadId mismatch");
assert(deref(devalueConfig.wsToken) === "local-key", "devalue wsToken mismatch");
assert(deref(devalueConfig.ampURL) === "http://127.0.0.1:8317", "devalue ampURL mismatch");
assert(deref(devalueConfig.baseURL) === "http://127.0.0.1:8317", "devalue baseURL mismatch");
assert(deref(devalueConfig.capability) === "write", "devalue capability mismatch");
assert(deref(devalueConfig.poolName) === "default", "devalue poolName mismatch");
assert(deref(devalueConfig.requiresSudoForWrite) === false, "devalue requiresSudoForWrite mismatch");
assert(deref(devalueConfig.requiresSudoForTerminal) === false, "devalue requiresSudoForTerminal mismatch");
assert(deref(devalueConfig.threadActorTransport) === "json-rpc", "devalue threadActorTransport mismatch");
assert(deref(values[2].creatorUserID) === ampViewerUserID && deref(values[2].ownerUserId) === ampViewerUserID, "devalue local thread owner ids were not patched");
assert(deref(values[values[2].creator].id) === ampViewerUserID && deref(values[values[1].creator].id) === ampViewerUserID, "devalue local thread creator objects were not patched");
assert(deref(values[values[11].threadCreator].id) === "local-user" && deref(values[values[11].entryCreator].id) === "local-user", "shared devalue creator objects were mutated");
assert(bridge.diagnostics.lastPatchedThreadID === threadID, "devalue patch did not record thread ID");
const nullConfigValues = JSON.parse(JSON.stringify([{ current: 1 }, { thread: 2, threadActorConfig: 5 }, { id: 3, title: 4 }, threadID, "Local", null ]));
const nullConfigIndex = nullConfigValues[1].threadActorConfig;
assert(Number.isInteger(nullConfigIndex), "null-ref devalue threadActorConfig was not patched");
assert(nullConfigIndex !== 5, "null-ref devalue threadActorConfig still points at null");
const nullConfig = nullConfigValues[nullConfigIndex];
const nullDeref = (value) => Number.isInteger(value) ? nullConfigValues[value] : value;
assert(nullDeref(nullConfig.threadId) === threadID, "null-ref devalue threadId mismatch");
assert(nullDeref(nullConfig.wsToken) === "local-key", "null-ref devalue wsToken mismatch");
globalThis.location = new URL("https://ampcode.com/threads/" + secondThreadID);
const secondValues = JSON.parse(JSON.stringify([{ current: 1 }, { thread: 2 }, { id: 3, title: 4, v: 5, messages: 6 }, secondThreadID, "Second", 41, [] ]));
const secondConfigIndex = secondValues[1].threadActorConfig;
assert(Number.isInteger(secondConfigIndex), "second devalue threadActorConfig was not patched after local navigation");
const secondConfig = secondValues[secondConfigIndex];
const secondDeref = (value) => Number.isInteger(value) ? secondValues[value] : value;
assert(secondDeref(secondConfig.threadId) === secondThreadID, "second devalue threadId mismatch");
const jsonRPCZeroResume = '{"jsonrpc":"2.0","id":"resume-1","method":"client_resume","params":{"version":0}}';
noBaseSocket.send(jsonRPCZeroResume);
const rewrittenJSONRPCResume = noBaseSocket.sent.at(-1);
assert(typeof rewrittenJSONRPCResume === "string", "JSON-RPC resume payload type changed");
const parsedJSONRPCResume = JSON.parse(rewrittenJSONRPCResume);
assert(parsedJSONRPCResume.id === "resume-1", "JSON-RPC resume id changed");
assert(parsedJSONRPCResume.params.version === 41, "matching devalue loaded base did not rewrite JSON-RPC zero resume");
assert(bridge.diagnostics.clientResumeObservedCount === 8, "resume observation diagnostics count mismatch");
assert(bridge.diagnostics.lastClientResumeObservedVersion === 0, "resume observation diagnostics version mismatch");
assert(bridge.diagnostics.clientResumeRewriteCount === 6, "resume rewrite diagnostics count mismatch");
assert(bridge.diagnostics.lastClientResumeThreadID === secondThreadID, "resume rewrite diagnostics thread mismatch");
assert(bridge.diagnostics.lastClientResumeBaseVersion === 41, "resume rewrite diagnostics version mismatch");
JSON.parse(JSON.stringify([{ thread: 1 }, { id: 2, data: 3 }, secondThreadID, { v: 4, messages: 5 }, 29, []]));
noBaseSocket.send(jsonRPCZeroResume);
assert(JSON.parse(noBaseSocket.sent.at(-1)).params.version === 29, "devalue envelope with outer identity and nested transcript did not rewrite zero resume");
JSON.parse(JSON.stringify([{ thread: 1 }, { id: 2, v: 3, messages: 4 }, secondThreadID, 19, []]));
noBaseSocket.send(jsonRPCZeroResume);
assert(JSON.parse(noBaseSocket.sent.at(-1)).params.version === 19, "newly loaded lower thread version did not replace a stale higher base");
const failedRequestBody = JSON.stringify({
	payload: encodeDevalue([
		{ content: 1, agentMode: 5, threadID: 6, reasoningEffort: 7 },
		[2],
		{ type: 3, text: 4 },
		"text",
		"Missing local project",
		"deep",
		failedThreadID,
		"xhigh",
	]),
	refreshes: [],
});
await fetch("https://ampcode.com/_app/remote/3abror/createProjectThread", {
	method: "POST",
	headers: { "Content-Type": "application/json" },
	body: failedRequestBody,
});
assert(!JSON.parse(globalThis.localStorage.getItem(bridge.localThreadIDsStorageKey)).includes(failedThreadID), "failed create thread was remembered");
const createRequestBody = JSON.stringify({
	payload: encodeDevalue([
		{ content: 1, agentMode: 5, threadID: 6, reasoningEffort: 7 },
		[2],
		{ type: 3, text: 4 },
		"text",
		"Use local project",
		"deep",
		createdThreadID,
		"xhigh",
	]),
	refreshes: [],
});
globalThis.sessionStorage.setItem(bridge.selectedLocalProjectStorageKey, JSON.stringify({
	name: "local-project",
	workingDirectory: createdThreadWorkDir,
	selectedAt: Date.now(),
}));
await fetch("https://ampcode.com/_app/remote/3abror/createProjectThread", {
	method: "POST",
	headers: { "Content-Type": "application/json" },
	body: createRequestBody,
});
const bridgedCreateURL = new URL(createFetchURL);
assert(bridgedCreateURL.origin === "http://127.0.0.1:8317", "create fetch was not bridged to local base");
assert(bridgedCreateURL.searchParams.get("cliproxy-working-directory") === createdThreadWorkDir, "create fetch missing working directory");
assert(bridgedCreateURL.searchParams.get("cliproxy-local-project") === "1", "create fetch missing local project marker");
assert(JSON.parse(globalThis.localStorage.getItem(bridge.localThreadIDsStorageKey)).includes(createdThreadID), "created thread was not remembered before route-data parse");
	const projectWorkflowRequestBody = JSON.stringify({
		payload: encodeDevalue([{ projectID: 1, changesWorkflow: 2 }, createdThreadProjectID, "push-to-branch"]),
		refreshes: [],
	});
	await fetch("https://ampcode.com/_app/remote/127n0b0/updateOwnedProjectChangesWorkflow", {
		method: "POST",
		headers: { "Content-Type": "application/json" },
		body: projectWorkflowRequestBody,
	});
	assert(new URL(lastFetchURL).origin === "https://ampcode.com", "Amp Cloud project setting was intercepted by the local runtime");
	await fetch("https://ampcode.com/_app/remote/127n0b0/updateOwnedProjectChangesWorkflow", {
		method: "POST",
		headers: { "Content-Type": "application/json", "X-CLIProxyAPI-Web-Local-Inference": "1" },
		body: projectWorkflowRequestBody,
	});
	assert(new URL(lastFetchURL).origin === "http://127.0.0.1:8317", "explicit local project setting was not bridged to the local runtime");
	assert(bridge.diagnostics.localProjectChangesWorkflowCacheUpdateCount === 1, "successful local project setting did not update the cache");
	projectWorkflowResponseSuccess = false;
	await fetch("https://ampcode.com/_app/remote/127n0b0/updateOwnedProjectChangesWorkflow", {
		method: "POST",
		headers: { "Content-Type": "application/json", "X-CLIProxyAPI-Web-Local-Inference": "1" },
		body: projectWorkflowRequestBody,
	});
	assert(new URL(lastFetchURL).origin === "http://127.0.0.1:8317", "failed local project setting escaped to Amp Cloud");
	assert(bridge.diagnostics.localProjectChangesWorkflowCacheUpdateCount === 1, "failed local project setting updated the cache");
	projectWorkflowResponseSuccess = true;
	await fetch("https://ampcode.com/_app/remote/127n0b0/updateOwnedProjectChangesWorkflow", {
		method: "POST",
		headers: { "Content-Type": "application/json", "X-CLIProxyAPI-Web-Local-Inference": "1" },
		body: JSON.stringify({ malformed: true }),
	});
	assert(new URL(lastFetchURL).origin === "http://127.0.0.1:8317", "invalid bridge-marked project setting escaped to Amp Cloud");
globalThis.localStorage.setItem(bridge.localThreadIDsStorageKey, JSON.stringify([createdThreadID]));
	const localProjectsFetchCountBeforeMutation = localProjectsFetchCount;
	const archiveRequestBody = JSON.stringify({
		payload: encodeDevalue([{ threadID: 1, archived: 2 }, createdThreadID, true]),
		refreshes: [],
	});
	await fetch("https://ampcode.com/_app/remote/145jw2/archiveThreadCommand", {
		method: "POST",
		headers: { "Content-Type": "application/json" },
		body: archiveRequestBody,
	});
	assert(new URL(lastFetchURL).origin === "http://127.0.0.1:8317", "local archive command was not bridged");
	const addLabelRequestBody = JSON.stringify({
		payload: encodeDevalue([{ threadID: 1, label: 2 }, createdThreadID, "shipping"]),
		refreshes: [],
	});
	await fetch("https://ampcode.com/_app/remote/urffnu/addThreadLabel", {
		method: "POST",
		headers: { "Content-Type": "application/json" },
		body: addLabelRequestBody,
	});
	assert(new URL(lastFetchURL).origin === "http://127.0.0.1:8317", "local add-label command was not bridged");
	assert(threadMutationFetchURLs.slice(-2).some((url) => new URL(url).origin === "https://ampcode.com"), "local add-label command was not mirrored to Amp");
	const pinRequestBody = JSON.stringify({
		payload: encodeDevalue([{ threadID: 1, pinned: 2 }, createdThreadID, true]),
		refreshes: [],
	});
	let releaseCloudMutationResponse;
	cloudMutationResponseGate = new Promise((resolve) => {
		releaseCloudMutationResponse = resolve;
	});
	let requestPinResolved = false;
	const requestPinPromise = fetch(new Request("https://ampcode.com/_app/remote/145jw2/pinThreadCommand", {
		method: "POST",
		headers: { "Content-Type": "application/json", "Accept": "application/request-default" },
		body: pinRequestBody,
		credentials: "omit",
		mode: "cors",
		referrerPolicy: "no-referrer",
	}), {
		headers: { "Content-Type": "application/json", "Accept": "application/init-override" },
	}).then(() => {
		requestPinResolved = true;
	});
	await new Promise((resolve) => setTimeout(resolve, 0));
	assert(!requestPinResolved, "local pin command resolved before its Amp mirror");
	releaseCloudMutationResponse();
	await requestPinPromise;
	assert(new URL(lastFetchURL).origin === "http://127.0.0.1:8317", "local Request-object pin command was not bridged");
	assert(threadMutationFetchURLs.slice(-2).some((url) => new URL(url).origin === "https://ampcode.com"), "local Request-object pin command was not mirrored to Amp");
	assert(threadMutationFetchURLs.slice(-2).some((url) => new URL(url).origin === "http://127.0.0.1:8317"), "local Request-object pin command did not reach the local runtime");
	const localRequestPinReplay = threadMutationFetchRequests.slice(-2).find((request) => new URL(request.url).origin === "http://127.0.0.1:8317");
	assert(localRequestPinReplay?.accept === "application/init-override", "Request-object mutation replay ignored init header overrides");
	assert(localRequestPinReplay?.credentials === "omit" && localRequestPinReplay?.mode === "cors" && localRequestPinReplay?.referrerPolicy === "no-referrer", "Request-object mutation replay dropped Request fetch options");
	const markUnreadRequestBody = JSON.stringify({
		payload: encodeDevalue([{ threadID: 1 }, createdThreadID]),
		refreshes: [],
	});
	await fetch("https://ampcode.com/_app/remote/145jw2/markThreadUnreadCommand", {
		method: "POST",
		headers: { "Content-Type": "application/json" },
		body: markUnreadRequestBody,
	});
	assert(new URL(lastFetchURL).origin === "http://127.0.0.1:8317", "local mark-unread command was not bridged");
	assert(threadMutationFetchURLs.slice(-2).some((url) => new URL(url).origin === "https://ampcode.com"), "local mark-unread command was not mirrored to Amp");
	const archivedPinRequestBody = JSON.stringify({
		payload: encodeDevalue([{ threadID: 1, pinned: 2 }, archivedThreadID, true]),
		refreshes: [],
	});
	await fetch("https://ampcode.com/_app/remote/145jw2/pinThreadCommand", {
		method: "POST",
		headers: { "Content-Type": "application/json" },
		body: archivedPinRequestBody,
	});
	assert(new URL(lastFetchURL).origin === "http://127.0.0.1:8317", "archived local pin command was not bridged");
	const discoveredPinRequestBody = JSON.stringify({
		payload: encodeDevalue([{ threadID: 1, pinned: 2 }, discoveredMutationThreadID, true]),
		refreshes: [],
	});
	await fetch("https://ampcode.com/_app/remote/145jw2/pinThreadCommand", {
		method: "POST",
		headers: { "Content-Type": "application/json" },
		body: discoveredPinRequestBody,
	});
	assert(new URL(lastFetchURL).origin === "http://127.0.0.1:8317", "discovered local pin command was not bridged");
	assert(JSON.parse(globalThis.localStorage.getItem(bridge.localThreadIDsStorageKey)).includes(discoveredMutationThreadID), "mutation-discovered local thread was not remembered");
	globalThis.localStorage.setItem(bridge.threadWorkingDirectoriesStorageKey, JSON.stringify({ [discoveredMutationThreadID]: "/tmp/discovered" }));
	globalThis.localStorage.setItem(bridge.threadSettingsStorageKey, JSON.stringify({ [discoveredMutationThreadID]: { agentMode: "deep" } }));
	const deleteRequestBody = JSON.stringify({
		payload: encodeDevalue([{ threadID: 1 }, discoveredMutationThreadID]),
		refreshes: [],
	});
	cloudDeleteResult = false;
	const failedDeleteFetchCount = threadMutationFetchURLs.length;
	await fetch("https://ampcode.com/_app/remote/145jw2/deleteThreadCommand", {
		method: "POST",
		headers: { "Content-Type": "application/json" },
		body: deleteRequestBody,
	});
	assert(threadMutationFetchURLs.length === failedDeleteFetchCount + 1, "failed cloud delete reached the local runtime");
	assert(new URL(threadMutationFetchURLs.at(-1)).origin === "https://ampcode.com", "failed cloud delete did not remain authoritative");
	assert(JSON.parse(globalThis.localStorage.getItem(bridge.localThreadIDsStorageKey)).includes(discoveredMutationThreadID), "failed cloud delete forgot the local thread");
	assert(Object.hasOwn(JSON.parse(globalThis.localStorage.getItem(bridge.threadWorkingDirectoriesStorageKey)), discoveredMutationThreadID), "failed cloud delete removed the local thread working directory");
	assert(Object.hasOwn(JSON.parse(globalThis.localStorage.getItem(bridge.threadSettingsStorageKey)), discoveredMutationThreadID), "failed cloud delete removed the local thread settings");
	cloudDeleteResult = true;
	await fetch("https://ampcode.com/_app/remote/145jw2/deleteThreadCommand", {
		method: "POST",
		headers: { "Content-Type": "application/json" },
		body: deleteRequestBody,
	});
	assert(new URL(lastFetchURL).origin === "http://127.0.0.1:8317", "local delete command was not bridged");
	assert(!JSON.parse(globalThis.localStorage.getItem(bridge.localThreadIDsStorageKey)).includes(discoveredMutationThreadID), "deleted local thread was retained");
	assert(!Object.hasOwn(JSON.parse(globalThis.localStorage.getItem(bridge.threadWorkingDirectoriesStorageKey)), discoveredMutationThreadID), "deleted local thread working directory was retained");
	assert(!Object.hasOwn(JSON.parse(globalThis.localStorage.getItem(bridge.threadSettingsStorageKey)), discoveredMutationThreadID), "deleted local thread settings were retained");
	const cloudPinRequestBody = JSON.stringify({
		payload: encodeDevalue([{ threadID: 1, pinned: 2 }, cloudThreadID, true]),
		refreshes: [],
	});
	await fetch("https://ampcode.com/_app/remote/145jw2/pinThreadCommand", {
		method: "POST",
		headers: { "Content-Type": "application/json" },
		body: cloudPinRequestBody,
	});
	assert(new URL(lastFetchURL).origin === "https://ampcode.com", "cloud pin command was incorrectly bridged");
	assert(threadMutationFetchURLs.slice(-1)[0] === lastFetchURL, "cloud pin command was duplicated");
const sidebarData = JSON.stringify([
	{ q: 1 },
	{ "3abror/listThreadListSidebar/": 2 },
	{ v: 3 },
	{ projects: 4, recentThreads: 5 },
	[],
	[6],
	{ id: 7, meta: 8 },
	archivedThreadID,
	{ projectID: 9 },
	createdThreadProjectID,
]);
const sidebarResponse = new Response(JSON.stringify({ type: "result", data: sidebarData }), {
	status: 200,
	headers: { "Content-Type": "application/json" },
});
Object.defineProperty(sidebarResponse, "url", { value: "https://ampcode.com/_app/remote/3abror/listThreadListSidebar" });
	const sidebarMergeCountBefore = bridge.diagnostics.localSidebarThreadMergeCount;
	const untitledSidebarAnchor = new FakeElement("a");
	untitledSidebarAnchor.dataset.sidebarThreadId = createdThreadID;
	const untitledSidebarTitle = new FakeElement("span", "Untitled");
	untitledSidebarAnchor.appendChild(untitledSidebarTitle);
	const staleSidebarAnchor = new FakeElement("a");
	staleSidebarAnchor.dataset.sidebarThreadId = secondThreadID;
	const staleSidebarTitle = new FakeElement("span", "Use local project");
	staleSidebarAnchor.appendChild(staleSidebarTitle);
	documentQueryElements = [untitledSidebarAnchor, staleSidebarAnchor];
	const decodedSidebarResponse = await sidebarResponse.json();
	assert(bridge.diagnostics.localSidebarThreadMergeCount - sidebarMergeCountBefore === 2, "sidebar response did not merge exactly two threads");
	untitledSidebarAnchor.dataset.sidebarGroupId = "project:No project";
	staleSidebarAnchor.dataset.sidebarGroupId = "project:second-project";
	regroupTestBridge.requestLocalSidebarProjectRegroup();
	await new Promise((resolve) => setTimeout(resolve, 0));
	assert(pageShowEventCount === 1, "initial project mismatch did not request a sidebar regroup");
	regroupTestBridge.requestLocalSidebarProjectRegroup();
	await new Promise((resolve) => setTimeout(resolve, 0));
	assert(pageShowEventCount === 1, "persistent project mismatch caused a regroup loop");
	staleSidebarAnchor.dataset.sidebarGroupId = "project:No project";
	regroupTestBridge.requestLocalSidebarProjectRegroup();
	await new Promise((resolve) => setTimeout(resolve, 0));
	assert(pageShowEventCount === 2, "late project mismatch did not request its own regroup");
	untitledSidebarAnchor.dataset.sidebarGroupId = "project:CLIProxyAPI";
	staleSidebarAnchor.dataset.sidebarGroupId = "project:second-project";
	regroupTestBridge.requestLocalSidebarProjectRegroup();
	await new Promise((resolve) => setTimeout(resolve, 10));
	untitledSidebarAnchor.dataset.sidebarGroupId = "project:No project";
	regroupTestBridge.requestLocalSidebarProjectRegroup();
	await new Promise((resolve) => setTimeout(resolve, 0));
	assert(pageShowEventCount === 3, "stably corrected project mismatch was not eligible for a later regroup");
	regroupTestBridge.requestLocalSidebarProjectRegroup();
	await new Promise((resolve) => setTimeout(resolve, 0));
	assert(pageShowEventCount === 3, "reappearing project mismatch caused a regroup loop");
	untitledSidebarAnchor.dataset.sidebarGroupId = "project:CLIProxyAPI";
	const threadSearchValues = devalueTable({
		hasMore: true,
		threads: [
			{
				archived: false,
				created: 1784390000000,
				creator: { id: ampViewerUserID, username: "viewer" },
				creatorUserID: ampViewerUserID,
				href: "/threads/" + searchThreadID,
				id: searchThreadID,
				meta: { agentMode: "smart" },
				projectName: "No Project",
				summaryStats: { diffStats: { added: null, changed: null, deleted: null } },
			},
			{
				archived: false,
				created: 1784380000000,
				creator: { id: ampViewerUserID, username: "viewer" },
				creatorUserID: ampViewerUserID,
				href: "/threads/" + searchProjectThreadID,
				id: searchProjectThreadID,
				meta: { agentMode: "smart" },
				projectName: "No Project",
				summaryStats: { diffStats: { added: null, changed: null, deleted: null } },
				title: "Existing local search title",
			},
			{
				archived: false,
				created: 1784370000000,
				creator: { id: "cloud-user", username: "cloud" },
				creatorUserID: "cloud-user",
				href: "/threads/" + cloudThreadID,
				id: cloudThreadID,
				meta: { agentMode: "smart" },
				projectName: "Cloud Project",
				summaryStats: { diffStats: { added: 1, changed: 2, deleted: 3 } },
				title: "Cloud search title",
			},
		],
	}, "13cgrsc/searchThreads/payload");
	const threadSearchQueryBefore = JSON.stringify(threadSearchValues[threadSearchValues[0].q]);
	const threadSearchResponse = new Response(JSON.stringify({ type: "result", data: JSON.stringify(threadSearchValues) }), {
		status: 200,
		headers: { "Content-Type": "application/json" },
	});
	Object.defineProperty(threadSearchResponse, "url", { value: "https://ampcode.com/_app/remote/13cgrsc/searchThreads?payload=test" });
	const threadSearchFetchCount = localProjectsFetchCount;
	localProjectsFetchURL = "";
	const patchedThreadSearchEnvelope = await threadSearchResponse.json();
	const patchedThreadSearchValues = JSON.parse(patchedThreadSearchEnvelope.data);
	const patchedThreadSearchRoot = patchedThreadSearchValues[patchedThreadSearchValues[0]._];
	const patchedThreadSearchRefs = patchedThreadSearchValues[patchedThreadSearchRoot.threads];
	const patchedThreadSearchThreads = patchedThreadSearchRefs.map((ref) => patchedThreadSearchValues[ref]);
	const patchedThreadSearchIDs = patchedThreadSearchThreads.map((thread) => patchedThreadSearchValues[thread.id]);
	assert(patchedThreadSearchIDs.join(",") === [searchThreadID, searchProjectThreadID, cloudThreadID].join(","), "thread search result order changed");
	assert(patchedThreadSearchRefs.length === 3, "thread search result count changed");
	assert(patchedThreadSearchValues[patchedThreadSearchRoot.hasMore] === true, "thread search pagination changed");
	assert(patchedThreadSearchValues[patchedThreadSearchThreads[0].title] === "Hydrated local search title", "local thread search title was not hydrated");
	assert(patchedThreadSearchValues[patchedThreadSearchThreads[0].projectName] === "CLIProxyAPI", "local thread search project was not hydrated");
	assert(patchedThreadSearchValues[patchedThreadSearchThreads[1].title] === "Existing local search title", "existing local thread search title was overwritten");
	assert(patchedThreadSearchValues[patchedThreadSearchThreads[1].projectName] === "CLIProxyAPI", "local thread search project-only metadata was not hydrated");
	assert(patchedThreadSearchValues[patchedThreadSearchThreads[2].title] === "Cloud search title", "cloud thread search title was overwritten");
	assert(patchedThreadSearchValues[patchedThreadSearchThreads[2].projectName] === "Cloud Project", "cloud thread search project was overwritten");
	assert(JSON.stringify(patchedThreadSearchValues[patchedThreadSearchValues[0].q]) === threadSearchQueryBefore, "thread search query cache graph changed");
	assert(localProjectsFetchCount === threadSearchFetchCount + 1, "thread search metadata hydration did not make one local metadata request");
	assert(new URL(localProjectsFetchURL).searchParams.getAll("cliproxy-sidebar-thread-id").includes(searchThreadID), "missing thread search title id was not sent to the local metadata endpoint");
	assert(new URL(localProjectsFetchURL).searchParams.getAll("cliproxy-sidebar-thread-id").includes(searchProjectThreadID), "missing thread search project id was not sent to the local metadata endpoint");
	assert(bridge.diagnostics.localThreadSearchTitlePatchCount === 1, "local thread search title patch was not recorded");
	assert(bridge.diagnostics.localThreadSearchProjectPatchCount === 2, "local thread search project patches were not recorded");
	const firstCloudSearchThreadID = "T-019f75f2-5bf4-736b-909c-000000000011";
	const secondCloudSearchThreadID = "T-019f75f2-5bf4-736b-909c-000000000012";
	const threadSearchRemoteValues = devalueTable({
		hasMore: true,
		threads: [
			{
				archived: false,
				created: 1784385000000,
				creator: { id: ampViewerUserID, username: "viewer" },
				creatorUserID: ampViewerUserID,
				href: "/threads/" + secondThreadID,
				id: secondThreadID,
				meta: { agentMode: "smart" },
				projectName: "No Project",
				summaryStats: { diffStats: {} },
				title: "Cloud shell for local thread",
			},
			{
				archived: false,
				created: 1784384000000,
				creator: { id: "cloud-user", username: "cloud" },
				creatorUserID: "cloud-user",
				href: "/threads/" + firstCloudSearchThreadID,
				id: firstCloudSearchThreadID,
				meta: { agentMode: "smart" },
				projectName: "Preserved Cloud Project",
				summaryStats: { diffStats: { added: 4 } },
				title: "Preserved cloud result",
			},
			{
				archived: false,
				created: 1784383000000,
				creator: { id: "cloud-user", username: "cloud" },
				creatorUserID: "cloud-user",
				href: "/threads/" + secondCloudSearchThreadID,
				id: secondCloudSearchThreadID,
				meta: { agentMode: "smart" },
				projectName: "Second Cloud Project",
				summaryStats: { diffStats: { added: 2 } },
				title: "Second cloud result",
			},
		],
	}, "13cgrsc/searchThreads/local-text");
		const threadSearchRemoteRoot = threadSearchRemoteValues[threadSearchRemoteValues[0]._];
		const threadSearchRemoteRefs = threadSearchRemoteValues[threadSearchRemoteRoot.threads];
		threadSearchRemoteValues[threadSearchRemoteRefs[1]].optional = -1;
		const threadSearchTaggedRef = threadSearchRemoteValues.length;
		threadSearchRemoteValues.push(["Set", threadSearchRemoteValues[threadSearchRemoteRefs[1]].title]);
		threadSearchRemoteValues[threadSearchRemoteRefs[1]].tagged = threadSearchTaggedRef;
		threadSearchRemoteData = JSON.stringify(threadSearchRemoteValues);
	const searchLocalThreads = async (offset) => {
		const requestValues = [
			["__skrao", 1],
			{ limit: 2, offset: 3, query: 4 },
			2,
			offset,
			"local needle",
		];
		const url = new URL("https://ampcode.com/_app/remote/13cgrsc/searchThreads");
		url.searchParams.set("payload", encodeDevalue(requestValues));
		const envelope = await (await fetch(url.href)).json();
		const values = JSON.parse(envelope.data);
		const root = values[values[0]._];
		const refs = values[root.threads];
		return {
			ids: refs.map((ref) => values[values[ref].id]),
			root,
			threads: refs.map((ref) => values[ref]),
			values,
		};
	};
	const firstLocalTextPage = await searchLocalThreads(0);
	assert(firstLocalTextPage.ids.join(",") === [secondThreadID, firstCloudSearchThreadID].join(","), "local text match was not merged ahead of cloud results");
	assert(firstLocalTextPage.ids.filter((id) => id === secondThreadID).length === 1, "local text search result was not deduplicated");
	assert(firstLocalTextPage.values[firstLocalTextPage.threads[0].title] === "Persisted local text match", "local persisted title was not used for a text match");
	assert(firstLocalTextPage.values[firstLocalTextPage.threads[0].projectName] === "CLIProxyAPI", "local persisted project was not used for a text match");
	assert(firstLocalTextPage.values[firstLocalTextPage.threads[0].matchedSearchText] === "persisted local needle transcript", "local matched transcript preview was lost");
	assert(firstLocalTextPage.values[firstLocalTextPage.threads[1].title] === "Preserved cloud result", "cloud text search title was overwritten");
	assert(firstLocalTextPage.values[firstLocalTextPage.threads[1].projectName] === "Preserved Cloud Project", "cloud text search project was overwritten");
	assert(firstLocalTextPage.values[firstLocalTextPage.root.hasMore] === true, "merged text search lost hasMore");
	assert(JSON.stringify(firstLocalTextPage.values[firstLocalTextPage.values[0].q]) === JSON.stringify(JSON.parse(threadSearchRemoteData)[JSON.parse(threadSearchRemoteData)[0].q]), "merged text search changed the query cache graph");
	assert(new URL(localThreadSearchFetchURL).searchParams.get("q") === "local needle", "local text query was not sent to the local search endpoint");
	assert(new URL(localThreadSearchFetchURL).searchParams.get("offset") === "0", "local text search window did not start at zero");
	assert(new URL(localThreadSearchFetchURL).searchParams.get("limit") === "2", "local text search used the wrong first-page window");
	const secondLocalTextPage = await searchLocalThreads(1);
	assert(secondLocalTextPage.ids.join(",") === [firstCloudSearchThreadID, secondCloudSearchThreadID].join(","), "combined local/cloud offset pagination skipped results");
		assert(secondLocalTextPage.threads[0].optional === -1, "cloud thread search copy replaced a negative devalue sentinel");
		const copiedTaggedSet = secondLocalTextPage.values[secondLocalTextPage.threads[0].tagged];
		assert(copiedTaggedSet[0] === "Set" && secondLocalTextPage.values[copiedTaggedSet[1]] === "Preserved cloud result", "cloud thread search copy did not rebase a tagged devalue reference");
	const secondPageCloudWindows = threadSearchRemoteFetchURLs.slice(-2).map((fetchURL) => {
		const requestValues = JSON.parse(Buffer.from(new URL(fetchURL).searchParams.get("payload"), "base64url").toString("utf8"));
		const input = requestValues.find((value) => value && !Array.isArray(value) && typeof value === "object" && Object.prototype.hasOwnProperty.call(value, "query"));
		return { limit: requestValues[input.limit], offset: requestValues[input.offset] };
	});
	assert(secondPageCloudWindows.some((window) => window.offset === 1 && window.limit === 2), "primary cloud text search request contract changed");
	assert(secondPageCloudWindows.some((window) => window.offset === 0 && window.limit === 3), "cloud text search window was not overfetched for pagination");
	assert(new URL(localThreadSearchFetchURL).searchParams.get("limit") === "3", "local text search window did not cover the requested offset");
	assert(bridge.diagnostics.localThreadSearchFetchCount === 2, "local text search fetches were not recorded");
	assert(bridge.diagnostics.localThreadSearchMergeCount === 2, "local text search merges were not recorded");
		const deepPageRemoteFetchCount = threadSearchRemoteFetchURLs.length;
		const deepLocalTextPage = await searchLocalThreads(100);
		assert(deepLocalTextPage.ids.join(",") === [secondThreadID, firstCloudSearchThreadID, secondCloudSearchThreadID].join(","), "deep cloud thread search page was rebased from a truncated window");
		assert(threadSearchRemoteFetchURLs.length === deepPageRemoteFetchCount + 1, "deep cloud thread search issued a truncated overfetch request");
		const archivedLocalSearch = await regroupTestBridge.fetchLocalThreadSearch({ query: "archived-only", archived: true, offset: 0, limit: 20 });
		assert(archivedLocalSearch.threads.length === 1 && archivedLocalSearch.threads[0].archived === true, "archived-only thread search retained active local threads");
	const usageData = JSON.stringify([
		{ _: 1 },
		[2],
		{ kind: 3, threadID: 4, title: 5, createdAt: 6, cost: 7 },
		"thread",
		createdThreadID,
		"Untitled Thread",
		"2026-07-19T11:09:00Z",
		{ freeUSD: 8, paidUSD: 8 },
		0,
	]);
	const usageResponse = new Response(JSON.stringify({ type: "result", data: usageData }), {
		status: 200,
		headers: { "Content-Type": "application/json" },
	});
	Object.defineProperty(usageResponse, "url", { value: "https://ampcode.com/_app/remote/1dvu7om/getPersonalThreadsUsageTable" });
	const usageValues = JSON.parse((await usageResponse.json()).data);
	assert(usageValues[usageValues[2].title] === "Use local project", "local Usage row retained its placeholder title");
	assert(bridge.diagnostics.localUsageTitlePatchCount === 1, "local Usage title patch was not recorded");
	assert(untitledSidebarTitle.textContent === "Use local project", "local sidebar placeholder title was not hydrated");
	assert(staleSidebarTitle.textContent === "Second local thread", "stale non-placeholder sidebar title was not reconciled");
	assert(bridge.diagnostics.localSidebarTitlePatchCount === 2, "local sidebar title reconciliation was not recorded");
	assert(new URL(localProjectsFetchURL).searchParams.getAll("cliproxy-sidebar-thread-id").includes(createdThreadID), "unresolved sidebar title id was not sent to the local metadata endpoint");
	assert(JSON.parse(globalThis.localStorage.getItem(bridge.sidebarTitlesStorageKey))[createdThreadID] === "Use local project", "hydrated sidebar title was not cached");
	await new Promise((resolve) => setTimeout(resolve, 0));
	assert(globalThis.document.documentElement.getAttribute("data-cliproxy-local-sidebar-hydrating") === null, "sidebar hydration gate was not released after merging metadata");
	documentQueryElements = [];
	assert(localProjectsFetchCount > localProjectsFetchCountBeforeMutation, "local thread mutation did not invalidate the sidebar cache");
	const mergedSidebarValues = JSON.parse(decodedSidebarResponse.data);
	const mergedSidebarRefs = mergedSidebarValues[5].slice(0, 2);
	const mergedSidebarProjectRefs = mergedSidebarValues[4];
	const mergedSidebarProjectIDs = mergedSidebarProjectRefs.map((ref) => mergedSidebarValues[mergedSidebarValues[ref].id]);
	const mergedSidebarProjectNames = mergedSidebarProjectRefs.map((ref) => mergedSidebarValues[mergedSidebarValues[ref].name]);
	const mergedSidebarThreadRefs = mergedSidebarRefs;
		const mergedSidebarThreadIDs = mergedSidebarThreadRefs.map((ref) => mergedSidebarValues[mergedSidebarValues[ref].id]);
		assert(mergedSidebarValues[5].length === 2, "archived local thread remained in the sidebar");
	assert(!mergedSidebarValues[5].some((ref) => mergedSidebarValues[mergedSidebarValues[ref].id] === archivedThreadID), "archived local thread tombstone was ignored");
	assert(mergedSidebarProjectIDs.includes(createdThreadProjectID) && mergedSidebarProjectIDs.includes(secondThreadProjectID), "sidebar response omitted referenced local projects");
	assert(mergedSidebarProjectNames.includes("CLIProxyAPI") && mergedSidebarProjectNames.includes("Second Project"), "sidebar response local project names mismatch");
	assert(bridge.diagnostics.localSidebarProjectMergeCount === 2, "sidebar project merges were not recorded");
	const collidingSidebarData = JSON.stringify([
		{ projects: 1, recentThreads: 2 },
		[3],
		[4],
		{ id: 5, name: 6, repositoryURL: 7 },
		{ id: 8, meta: 9 },
		createdThreadProjectID,
		"CLIProxyAPI",
		"https://github.com/router-for-me/CLIProxyAPI.git",
		collidingSidebarThreadID,
		{ projectID: 5 },
	]);
	const collidingSidebarResponse = new Response(JSON.stringify({ type: "result", data: collidingSidebarData }), {
		status: 200,
		headers: { "Content-Type": "application/json" },
	});
	Object.defineProperty(collidingSidebarResponse, "url", { value: "https://ampcode.com/_app/remote/3abror/listThreadListSidebar" });
	const collidingSidebarValues = JSON.parse((await collidingSidebarResponse.json()).data);
	const collidingSidebarProjectRefs = collidingSidebarValues[collidingSidebarValues[0].projects];
	const collisionProjectRef = collidingSidebarProjectRefs.find((ref) => {
		const project = collidingSidebarValues[ref];
		return collidingSidebarValues[project.name] === "Collision Project";
	});
	assert(Number.isInteger(collisionProjectRef), "colliding sidebar checkout was not projected as its own project");
	const collisionPresentedProjectID = collidingSidebarValues[collidingSidebarValues[collisionProjectRef].id];
	assert(collisionPresentedProjectID !== createdThreadProjectID, "colliding sidebar checkout reused the cloud project's render id");
	const collidingSidebarThreadRefs = collidingSidebarValues[collidingSidebarValues[0].recentThreads];
	const collisionThreadRef = collidingSidebarThreadRefs.find((ref) => collidingSidebarValues[collidingSidebarValues[ref].id] === collidingSidebarThreadID);
	assert(Number.isInteger(collisionThreadRef), "colliding local project thread was not merged into the sidebar");
	const collisionThreadRecord = collidingSidebarValues[collisionThreadRef];
	const collisionThreadMeta = collidingSidebarValues[collisionThreadRecord.meta];
	const collisionThreadProjectID = Number.isInteger(collisionThreadRecord.projectID)
		? collidingSidebarValues[collisionThreadRecord.projectID]
		: collidingSidebarValues[collisionThreadMeta.projectID];
	assert(collisionThreadProjectID === collisionPresentedProjectID, "colliding sidebar thread was attached to the wrong project render id");
		const unarchiveRequestBody = JSON.stringify({
			payload: encodeDevalue([{ threadID: 1, archived: 2 }, archivedThreadID, false]),
			refreshes: [],
		});
		localProjectsResponseInvalid = true;
		await fetch("https://ampcode.com/_app/remote/145jw2/archiveThreadCommand", {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: unarchiveRequestBody,
		});
		const failedRefreshSidebarData = JSON.stringify([
			{ recentThreads: 1 },
			[2],
			{ id: 3, title: 4 },
			archivedThreadID,
			"Unarchived local thread",
		]);
		const failedRefreshSidebarResponse = new Response(JSON.stringify({ type: "result", data: failedRefreshSidebarData }), {
			status: 200,
			headers: { "Content-Type": "application/json" },
		});
		Object.defineProperty(failedRefreshSidebarResponse, "url", { value: "https://ampcode.com/_app/remote/3abror/listThreadListSidebar" });
			const failedRefreshSidebarValues = JSON.parse((await failedRefreshSidebarResponse.json()).data);
			assert(failedRefreshSidebarValues[1].length === 1, "failed project refresh retained an archived-thread tombstone");
			await fetch("https://ampcode.com/_app/remote/145jw2/archiveThreadCommand", {
				method: "POST",
				headers: { "Content-Type": "application/json" },
				body: archiveRequestBody,
			});
			const failedArchiveRefreshSidebarResponse = new Response(JSON.stringify({
				type: "result",
				data: JSON.stringify([{ recentThreads: 1 }, [2], { id: 3 }, createdThreadID]),
			}), {
				status: 200,
				headers: { "Content-Type": "application/json" },
			});
			Object.defineProperty(failedArchiveRefreshSidebarResponse, "url", { value: "https://ampcode.com/_app/remote/3abror/listThreadListSidebar" });
			const failedArchiveRefreshSidebarValues = JSON.parse((await failedArchiveRefreshSidebarResponse.json()).data);
			assert(failedArchiveRefreshSidebarValues[1].length === 0, "failed project refresh lost an optimistic archived-thread tombstone");
			localProjectsResponseInvalid = false;
		const recoveredSidebarData = JSON.stringify([{ projects: 1 }, []]);
		const recoveredSidebarResponse = new Response(JSON.stringify({ type: "result", data: recoveredSidebarData }), {
			status: 200,
			headers: { "Content-Type": "application/json" },
		});
		Object.defineProperty(recoveredSidebarResponse, "url", { value: "https://ampcode.com/_app/remote/3abror/listThreadListSidebar" });
		await recoveredSidebarResponse.json();
	const mergedSidebarCreatorIDs = mergedSidebarThreadRefs.map((ref) => {
		const creatorRef = mergedSidebarValues[ref].creator;
		return mergedSidebarValues[mergedSidebarValues[creatorRef].id];
	});
	assert(mergedSidebarRefs.every((ref) => Number.isInteger(ref) && !Object.hasOwn(mergedSidebarValues[ref], "thread") && Number.isInteger(mergedSidebarValues[ref].meta)), "local sidebar threads were not inserted with the current direct-row shape");
	assert(JSON.stringify(mergedSidebarThreadIDs) === JSON.stringify([createdThreadID, secondThreadID]), "merged sidebar thread order mismatch");
	assert(mergedSidebarCreatorIDs.every((id) => id === ampViewerUserID), "merged sidebar creator objects were not projected as the authenticated Amp user");
	assert(mergedSidebarValues[mergedSidebarValues[mergedSidebarThreadRefs[0]].title] === "Use local project", "merged sidebar thread title mismatch");
	assert(mergedSidebarValues[mergedSidebarValues[mergedSidebarThreadRefs[0]].projectName] === "CLIProxyAPI", "inserted direct sidebar thread project name mismatch");
	assert(mergedSidebarValues[mergedSidebarValues[mergedSidebarThreadRefs[0]].repositoryGroupName] === "CLIProxyAPI", "inserted direct sidebar thread repository group mismatch");
	assert(mergedSidebarValues[mergedSidebarValues[mergedSidebarThreadRefs[1]].repositoryGroupName] === "second-project", "inserted direct Git sidebar thread repository group mismatch");
	assert(mergedSidebarValues[mergedSidebarValues[mergedSidebarThreadRefs[0]].automation] === null, "inserted direct sidebar thread did not default automation to null");
	assert(mergedSidebarValues[mergedSidebarValues[mergedSidebarThreadRefs[1]].automation] === null, "inserted direct Git sidebar thread did not default automation to null");
	assert(!Object.hasOwn(mergedSidebarValues[mergedSidebarThreadRefs[1]], "hasExecutor"), "cached local sidebar thread gained hasExecutor before discovery");
	assert(!Object.hasOwn(mergedSidebarValues[mergedSidebarThreadRefs[1]], "executorConnected"), "cached local sidebar thread gained executorConnected before discovery");
assert(new URL(localProjectsFetchURL).searchParams.get("cliproxy-thread-id") === createdThreadID, "local summary request used the wrong thread id");
	globalThis.localStorage.setItem(bridge.localThreadIDsStorageKey, JSON.stringify([createdThreadID]));
	const cachedThreadPinRequestBody = JSON.stringify({
		payload: encodeDevalue([{ threadID: 1, pinned: 2 }, secondThreadID, true]),
		refreshes: [],
	});
	await fetch("https://ampcode.com/_app/remote/145jw2/pinThreadCommand", {
		method: "POST",
		headers: { "Content-Type": "application/json" },
		body: cachedThreadPinRequestBody,
	});
	assert(new URL(lastFetchURL).origin === "http://127.0.0.1:8317", "sidebar-cached local pin command was not bridged");
	let releaseLocalProjectsResponse;
	localProjectsResponseGate = new Promise((resolve) => {
		releaseLocalProjectsResponse = resolve;
	});
	const localProjectsFetchCountBeforeRace = localProjectsFetchCount;
	const raceSidebarResponse = new Response(JSON.stringify({ type: "result", data: JSON.stringify([{ recentThreads: 1 }, []]) }), {
		status: 200,
		headers: { "Content-Type": "application/json" },
	});
	Object.defineProperty(raceSidebarResponse, "url", { value: "https://ampcode.com/_app/remote/3abror/listThreadListSidebar" });
	const raceSidebarPromise = raceSidebarResponse.json();
	await new Promise((resolve) => setTimeout(resolve, 0));
	assert(localProjectsFetchCount === localProjectsFetchCountBeforeRace + 1, "sidebar race did not start a local project fetch");
	await fetch("https://ampcode.com/_app/remote/145jw2/pinThreadCommand", {
		method: "POST",
		headers: { "Content-Type": "application/json" },
		body: JSON.stringify({ payload: encodeDevalue([{ threadID: 1, pinned: 2 }, secondThreadID, false]), refreshes: [] }),
	});
	releaseLocalProjectsResponse();
	await raceSidebarPromise;
	assert(localProjectsFetchCount === localProjectsFetchCountBeforeRace + 2, "stale local project response was not followed by a fresh fetch");
	regroupTestBridge.invalidateLocalSidebarAfterThreadMutation(new URL("https://ampcode.com/_app/remote/145jw2/pinThreadCommand"), JSON.stringify({ payload: encodeDevalue([{ threadID: 1, pinned: 2 }, secondThreadID, true]), refreshes: [] }), { ok: true });
	let releaseHydrationProjectsResponse;
	localProjectsResponseGate = new Promise((resolve) => {
		releaseHydrationProjectsResponse = resolve;
	});
	const hydrationFetchCount = localProjectsFetchCount;
	const hydrationPromise = regroupTestBridge.fetchLocalProjects(false, [raceSidebarThreadID]);
	await Promise.resolve();
	assert(localProjectsFetchCount === hydrationFetchCount + 1, "sidebar hydration race did not start a local project fetch");
	assert(new URL(localProjectsFetchURL).searchParams.getAll("cliproxy-sidebar-thread-id").includes(raceSidebarThreadID), "sidebar hydration id was not sent on the initial fetch");
	regroupTestBridge.invalidateLocalSidebarAfterThreadMutation(new URL("https://ampcode.com/_app/remote/145jw2/pinThreadCommand"), JSON.stringify({ payload: encodeDevalue([{ threadID: 1, pinned: 2 }, secondThreadID, false]), refreshes: [] }), { ok: true });
	releaseHydrationProjectsResponse();
	await hydrationPromise;
	assert(localProjectsFetchCount === hydrationFetchCount + 2, "invalidated sidebar hydration fetch was not retried");
	assert(new URL(localProjectsFetchURL).searchParams.getAll("cliproxy-sidebar-thread-id").includes(raceSidebarThreadID), "sidebar hydration id was dropped from the invalidation retry");
	const alternatingSidebarThreadID = "T-019f324b-2802-7868-b1b1-5f0fa3e87eb0";
	const alternatingSidebarFetchCount = localProjectsFetchCount;
	await regroupTestBridge.fetchLocalProjects(false, [alternatingSidebarThreadID]);
	await regroupTestBridge.fetchLocalProjects(false, [raceSidebarThreadID]);
	await regroupTestBridge.fetchLocalProjects(false, [alternatingSidebarThreadID]);
	assert(localProjectsFetchCount === alternatingSidebarFetchCount + 1, "alternating sidebar title subsets thrashed the local project cache");
	const mutationOnlySidebarThread = new FakeElement("a");
	mutationOnlySidebarThread.dataset.sidebarThreadId = "T-019f324b-2802-7868-b1b1-5f0fa3e87eb1";
	documentQueryElements = [mutationOnlySidebarThread];
	const mutationOnlySidebarFetchCount = localProjectsFetchCount;
	await regroupTestBridge.fetchLocalProjects(false);
	assert(localProjectsFetchCount === mutationOnlySidebarFetchCount, "DOM-only sidebar changes bypassed the warm local project cache");
	documentQueryElements = [];
	assert(regroupTestBridge.localSidebarCachedThreadID(secondThreadID), "cached local thread was missing before archiving");
	regroupTestBridge.invalidateLocalSidebarAfterThreadMutation(new URL("https://ampcode.com/_app/remote/145jw2/archiveThreadCommand"), JSON.stringify({ payload: encodeDevalue([{ threadID: 1, archived: 2 }, secondThreadID, true]), refreshes: [] }), { ok: true });
	assert(!regroupTestBridge.localSidebarCachedThreadID(secondThreadID), "archived local thread remained in the sidebar cache");
	assert(regroupTestBridge.localSidebarArchivedThreadID(secondThreadID), "archived local thread was not tracked as archived");
	regroupTestBridge.invalidateLocalSidebarAfterThreadMutation(new URL("https://ampcode.com/_app/remote/145jw2/archiveThreadCommand"), JSON.stringify({ payload: encodeDevalue([{ threadID: 1, archived: 2 }, secondThreadID, false]), refreshes: [] }), { ok: true });
	assert(!regroupTestBridge.localSidebarArchivedThreadID(secondThreadID), "unarchived local thread was still tracked as archived");
	await regroupTestBridge.fetchLocalProjects(false);
	assert(regroupTestBridge.localSidebarCachedThreadID(secondThreadID), "sidebar cache did not recover the unarchived local thread");
	const previousPromptStub = globalThis.prompt;
	const previousConfirmStub = globalThis.confirm;
	let promptReplayValue = "";
	let confirmReplayValue = false;
	globalThis.prompt = () => promptReplayValue;
	globalThis.confirm = () => confirmReplayValue;
	const sessionAPIKeyStorageKey = regroupTestBridge.sessionLocalAPIKeyStorageKey();
	const persistentAPIKeyStorageKey = regroupTestBridge.persistentLocalAPIKeyStorageKey();
	assert(sessionAPIKeyStorageKey === persistentAPIKeyStorageKey, "session and persistent API key storage did not share one account/server scope");
	assert(persistentAPIKeyStorageKey.includes(encodeURIComponent(ampViewerUserID)), "persistent API key storage was not scoped to the authenticated Amp user");
	assert(persistentAPIKeyStorageKey.includes(encodeURIComponent("http://127.0.0.1:8317")), "persistent API key storage was not scoped to the local server");
	assert(globalThis.sessionStorage.getItem(sessionAPIKeyStorageKey) === "local-key", "pre-identity session key was not promoted after authenticated identity arrived");
	globalThis.sessionStorage.removeItem(sessionAPIKeyStorageKey);
	globalThis.localStorage.removeItem(bridge.apiKeyStorageKey);
	globalThis.localStorage.removeItem(persistentAPIKeyStorageKey);
	promptReplayValue = "test-key-declined";
	assert(regroupTestBridge.localAPIKey(false) === "test-key-declined", "declined API key prompt did not return the entered key");
	assert(globalThis.sessionStorage.getItem(sessionAPIKeyStorageKey) === "test-key-declined", "declined API key was not stored for the scoped session");
	assert(globalThis.localStorage.getItem(persistentAPIKeyStorageKey) === null, "declined API key consent persisted the key to local storage");
	globalThis.sessionStorage.removeItem(sessionAPIKeyStorageKey);
	globalThis.localStorage.removeItem(persistentAPIKeyStorageKey);
	promptReplayValue = "test-key-accepted";
	confirmReplayValue = true;
	assert(regroupTestBridge.localAPIKey(false) === "test-key-accepted", "accepted API key prompt did not return the entered key");
	assert(globalThis.sessionStorage.getItem(sessionAPIKeyStorageKey) === "test-key-accepted", "accepted API key was not stored for the scoped session");
	assert(globalThis.localStorage.getItem(persistentAPIKeyStorageKey) === "test-key-accepted", "accepted API key consent did not persist the key to local storage");
	globalThis.sessionStorage.setItem(sessionAPIKeyStorageKey, "stale-key");
	globalThis.localStorage.setItem(persistentAPIKeyStorageKey, "stale-key");
	regroupTestBridge.forgetLocalAPIKey();
	assert(globalThis.sessionStorage.getItem(sessionAPIKeyStorageKey) === null && globalThis.localStorage.getItem(persistentAPIKeyStorageKey) === null, "forgetLocalAPIKey left a scoped API key behind");
	assert(globalThis.sessionStorage.getItem(bridge.apiKeyStorageKey) === null && globalThis.localStorage.getItem(bridge.apiKeyStorageKey) === null, "obsolete generic API key storage was recreated");
	globalThis.localStorage.setItem(persistentAPIKeyStorageKey, "local-only-key");
	assert(regroupTestBridge.storedLocalAPIKey() === "local-only-key", "storedLocalAPIKey did not fall back to local storage");
	globalThis.localStorage.removeItem(persistentAPIKeyStorageKey);
	globalThis.prompt = previousPromptStub;
	globalThis.confirm = previousConfirmStub;
	globalThis.sessionStorage.setItem(sessionAPIKeyStorageKey, "local-key");
		await regroupTestBridge.fetchLocalProjects(false);
	const mergedSidebarMetas = mergedSidebarThreadRefs.map((ref) => mergedSidebarValues[mergedSidebarValues[ref].meta]);
	assert(mergedSidebarValues[mergedSidebarMetas[0].projectID] === createdThreadProjectID, "merged sidebar project identity mismatch");
	assert(mergedSidebarValues[mergedSidebarMetas[1].projectName] === "Second Project", "merged sidebar project name mismatch");
	globalThis.sessionStorage.setItem(bridge.selectedLocalProjectStorageKey, JSON.stringify({
		name: "~",
		workingDirectory: "/Users/aikins01",
		selectedAt: Date.now(),
	}));
	const explicitProjectRequestBody = JSON.stringify({
		payload: encodeDevalue([
			{ content: 1, agentMode: 5, threadID: 6, reasoningEffort: 7, projectID: 8 },
			[2],
			{ type: 3, text: 4 },
			"text",
			"Explicit project beats stale home selection",
			"deep",
			createdThreadID,
			"xhigh",
			createdThreadProjectID,
		]),
		refreshes: [],
	});
	await fetch("https://ampcode.com/_app/remote/3abror/createProjectThread", {
		method: "POST",
		headers: { "Content-Type": "application/json" },
		body: explicitProjectRequestBody,
	});
	const explicitProjectCreateURL = new URL(createFetchURL);
	assert(explicitProjectCreateURL.searchParams.get("cliproxy-working-directory") === createdThreadWorkDir, "stale home selection overrode explicit project working directory");
	assert(!explicitProjectCreateURL.searchParams.has("cliproxy-local-project"), "explicit remote project was mislabeled as a local project selection");
	const canonicalSidebarData = JSON.stringify([
		{ recentThreads: 1 },
		[2],
		{ thread: 3, lastActivityTimestamp: 6, projectName: 7, repositoryGroupName: 9 },
		{ id: 4, title: 5, hasExecutor: 8, executorConnected: 8, pinned: 12, meta: 10, automation: 13 },
		createdThreadID,
		"Remote canonical title",
		1,
		null,
		true,
		"Developer",
		{ projectID: 11 },
		"remote-project",
		false,
		{ enabled: 8 },
	]);
const canonicalSidebarResponse = new Response(JSON.stringify({ type: "result", data: canonicalSidebarData }), {
	status: 200,
	headers: { "Content-Type": "application/json" },
});
Object.defineProperty(canonicalSidebarResponse, "url", { value: "https://ampcode.com/_app/remote/3abror/listThreadListSidebar" });
	const decodedCanonicalSidebar = await canonicalSidebarResponse.json();
	const canonicalSidebarValues = JSON.parse(decodedCanonicalSidebar.data);
	const canonicalSidebarRefs = canonicalSidebarValues[1];
	const canonicalSidebarThreadRefs = canonicalSidebarRefs.map((ref) => Number.isInteger(canonicalSidebarValues[ref].thread) ? canonicalSidebarValues[ref].thread : ref);
	const canonicalSidebarThreadIDs = canonicalSidebarThreadRefs.map((ref) => canonicalSidebarValues[canonicalSidebarValues[ref].id]);
	const canonicalThreadRef = canonicalSidebarThreadRefs.find((ref) => canonicalSidebarValues[canonicalSidebarValues[ref].id] === createdThreadID);
	const canonicalWrapperRef = canonicalSidebarRefs.find((ref) => Number.isInteger(canonicalSidebarValues[ref].thread) && canonicalSidebarValues[canonicalSidebarValues[ref].thread].id === canonicalSidebarValues[canonicalThreadRef].id);
	assert(canonicalSidebarValues[canonicalSidebarValues[canonicalThreadRef].title] === "Use local project", "existing remote sidebar shell was not enriched with the local title");
	assert(canonicalSidebarValues[canonicalSidebarValues[canonicalWrapperRef].projectName] === "CLIProxyAPI", "existing remote sidebar shell project grouping was not enriched");
	assert(canonicalSidebarValues[canonicalSidebarValues[canonicalWrapperRef].repositoryGroupName] === "Developer", "existing canonical repository grouping was overwritten");
	assert(canonicalSidebarValues[canonicalSidebarValues[canonicalWrapperRef].lastActivityTimestamp] === 1, "existing canonical activity timestamp was overwritten");
	assert(canonicalSidebarValues[canonicalSidebarValues[canonicalThreadRef].meta].projectID === 11 && canonicalSidebarValues[11] === "remote-project", "existing canonical project metadata was overwritten");
	assert(canonicalSidebarValues[canonicalSidebarValues[canonicalSidebarValues[canonicalThreadRef].meta].projectName] === "CLIProxyAPI", "missing canonical project metadata was not enriched");
assert(canonicalSidebarValues[canonicalSidebarValues[canonicalThreadRef].hasExecutor] === true, "existing local sidebar thread lost hasExecutor");
assert(canonicalSidebarValues[canonicalSidebarValues[canonicalThreadRef].executorConnected] === true, "existing local sidebar thread lost executorConnected");
	assert(canonicalSidebarValues[canonicalSidebarValues[canonicalSidebarValues[canonicalThreadRef].automation].enabled] === true, "existing local sidebar thread lost its automation object");
	const canonicalInsertedThreadRef = canonicalSidebarThreadRefs.find((ref) => canonicalSidebarValues[canonicalSidebarValues[ref].id] === secondThreadID);
	assert(canonicalSidebarValues[canonicalSidebarValues[canonicalInsertedThreadRef].automation] === null, "inserted wrapped sidebar thread did not default automation to null");
	assert(canonicalSidebarValues[canonicalSidebarValues[canonicalThreadRef].pinned] === true, "explicit local pin did not override stale native state");
	assert(!Object.hasOwn(canonicalSidebarValues[canonicalThreadRef], "cliProxyAPILocalPinnedOverride"), "local pin override marker leaked into sidebar data");
assert(canonicalSidebarValues[canonicalSidebarValues[canonicalThreadRef].state] === "idle", "existing remote sidebar shell was not enriched with local state");
	await fetch("https://ampcode.com/_app/remote/145jw2/pinThreadCommand", {
		method: "POST",
		headers: { "Content-Type": "application/json" },
		body: JSON.stringify({ payload: encodeDevalue([{ threadID: 1, pinned: 2 }, createdThreadID, false]), refreshes: [] }),
	});
	const unpinnedSidebarResponse = new Response(JSON.stringify({
		type: "result",
		data: JSON.stringify([{ recentThreads: 1 }, [2], { thread: 3 }, { id: 4, pinned: 5 }, createdThreadID, true]),
	}), { status: 200, headers: { "Content-Type": "application/json" } });
	Object.defineProperty(unpinnedSidebarResponse, "url", { value: "https://ampcode.com/_app/remote/3abror/listThreadListSidebar" });
	const unpinnedSidebarValues = JSON.parse((await unpinnedSidebarResponse.json()).data);
	const unpinnedWrapper = unpinnedSidebarValues[unpinnedSidebarValues[0].recentThreads].find((ref) => {
		const row = unpinnedSidebarValues[ref];
		const threadRef = Number.isInteger(row.thread) ? row.thread : ref;
		return unpinnedSidebarValues[unpinnedSidebarValues[threadRef].id] === createdThreadID;
	});
	const unpinnedThread = unpinnedSidebarValues[unpinnedWrapper].thread;
	assert(unpinnedSidebarValues[unpinnedSidebarValues[unpinnedThread].pinned] === false, "explicit local unpin did not override stale native state");
	const verifiedGitSidebarData = JSON.stringify([
		{ recentThreads: 1 },
		[2],
		{ thread: 3, lastActivityTimestamp: 6, projectName: 7, repositoryGroupName: 7 },
		{ id: 4, title: 5, meta: 8 },
		secondThreadID,
		"Remote stale project title",
		2,
		"Wrong Project",
		{ projectID: 9, projectName: 7, namespace: 10, repositoryURL: 11 },
		"remote-wrong-project",
		"wrong-owner",
		"https://github.com/wrong-owner/wrong-project.git",
	]);
	const verifiedGitSidebarResponse = new Response(JSON.stringify({ type: "result", data: verifiedGitSidebarData }), {
		status: 200,
		headers: { "Content-Type": "application/json" },
	});
	Object.defineProperty(verifiedGitSidebarResponse, "url", { value: "https://ampcode.com/_app/remote/3abror/listThreadListSidebar" });
	const verifiedGitSidebarValues = JSON.parse((await verifiedGitSidebarResponse.json()).data);
	const verifiedGitWrapper = verifiedGitSidebarValues[verifiedGitSidebarValues[0].recentThreads].find((ref) => {
		const row = verifiedGitSidebarValues[ref];
		const threadRef = Number.isInteger(row.thread) ? row.thread : ref;
		return verifiedGitSidebarValues[verifiedGitSidebarValues[threadRef].id] === secondThreadID;
	});
	const verifiedGitThread = verifiedGitSidebarValues[verifiedGitWrapper].thread;
	const verifiedGitMeta = verifiedGitSidebarValues[verifiedGitThread].meta;
	assert(verifiedGitSidebarValues[verifiedGitSidebarValues[verifiedGitThread].title] === "Second local thread", "authoritative local title did not replace the stale native title");
	assert(verifiedGitSidebarValues[verifiedGitSidebarValues[verifiedGitWrapper].repositoryGroupName] === "second-project", "verified Git repository name did not replace stale repository grouping");
	assert(verifiedGitSidebarValues[verifiedGitSidebarValues[verifiedGitMeta].namespace] === "aikins01", "verified Git namespace did not replace stale canonical owner");
	assert(verifiedGitSidebarValues[verifiedGitSidebarValues[verifiedGitMeta].repositoryURL] === "https://github.com/aikins01/second-project.git", "verified Git remote did not replace stale canonical repository");
	assert(verifiedGitSidebarValues[verifiedGitSidebarValues[verifiedGitMeta].projectID] === secondThreadProjectID, "verified Git project id did not replace stale canonical project");
	const fallbackSidebarData = JSON.stringify([
		{ recentThreads: 1 },
		[2],
		{ thread: 3, lastActivityTimestamp: 6, projectName: 7, repositoryGroupName: 9 },
		{ id: 4, title: 5, hasExecutor: 8, executorConnected: 8 },
		createdThreadID,
		"Remote fallback title",
		1,
		null,
		true,
		"No project",
	]);
	const fallbackSidebarResponse = new Response(JSON.stringify({ type: "result", data: fallbackSidebarData }), {
		status: 200,
		headers: { "Content-Type": "application/json" },
	});
	Object.defineProperty(fallbackSidebarResponse, "url", { value: "https://ampcode.com/_app/remote/3abror/listThreadListSidebar" });
	const decodedFallbackSidebar = await fallbackSidebarResponse.json();
	const fallbackSidebarValues = JSON.parse(decodedFallbackSidebar.data);
	const fallbackWrapperRef = fallbackSidebarValues[fallbackSidebarValues[0].recentThreads].find((ref) => {
		const row = fallbackSidebarValues[ref];
		return Number.isInteger(row.thread) && fallbackSidebarValues[fallbackSidebarValues[row.thread].id] === createdThreadID;
	});
	assert(fallbackSidebarValues[fallbackSidebarValues[fallbackWrapperRef].repositoryGroupName] === "CLIProxyAPI", "No project fallback was not replaced by the local repository group");
	const directFallbackSidebarResponse = new Response(JSON.stringify({
		type: "result",
		data: JSON.stringify([
			{ recentThreads: 1 },
			[2],
			{ id: 3, title: 4, projectName: 5, repositoryGroupName: 5, meta: 6 },
			createdThreadID,
			"Remote direct fallback title",
			"No project",
			{ projectID: 7 },
			createdThreadProjectID,
		]),
	}), { status: 200, headers: { "Content-Type": "application/json" } });
	Object.defineProperty(directFallbackSidebarResponse, "url", { value: "https://ampcode.com/_app/remote/3abror/listThreadListSidebar" });
	const directFallbackSidebarValues = JSON.parse((await directFallbackSidebarResponse.json()).data);
	const directFallbackThreadRef = directFallbackSidebarValues[directFallbackSidebarValues[0].recentThreads].find((ref) =>
		directFallbackSidebarValues[directFallbackSidebarValues[ref].id] === createdThreadID
	);
	assert(directFallbackSidebarValues[directFallbackSidebarValues[directFallbackThreadRef].projectName] === "CLIProxyAPI", "direct sidebar row project name retained No project");
	assert(directFallbackSidebarValues[directFallbackSidebarValues[directFallbackThreadRef].repositoryGroupName] === "CLIProxyAPI", "direct sidebar row repository group retained No project");
	assert(directFallbackSidebarValues[directFallbackSidebarValues[directFallbackThreadRef].automation] === null, "enriched direct sidebar row did not default automation to null");
	const directAutomationSidebarResponse = new Response(JSON.stringify({
		type: "result",
		data: JSON.stringify([
			{ recentThreads: 1 },
			[2],
			{ id: 3, title: 4, automation: 5 },
			createdThreadID,
			"Remote automation title",
			{ enabled: 6 },
			true,
		]),
	}), { status: 200, headers: { "Content-Type": "application/json" } });
	Object.defineProperty(directAutomationSidebarResponse, "url", { value: "https://ampcode.com/_app/remote/3abror/listThreadListSidebar" });
	const directAutomationSidebarValues = JSON.parse((await directAutomationSidebarResponse.json()).data);
	const directAutomationThreadRef = directAutomationSidebarValues[directAutomationSidebarValues[0].recentThreads].find((ref) =>
		directAutomationSidebarValues[directAutomationSidebarValues[ref].id] === createdThreadID
	);
	assert(directAutomationSidebarValues[directAutomationSidebarValues[directAutomationSidebarValues[directAutomationThreadRef].automation].enabled] === true, "enriched direct sidebar row lost its automation object");
	includeIncompleteCachedThread = true;
	regroupTestBridge.invalidateLocalSidebarAfterThreadMutation(new URL("https://ampcode.com/_app/remote/145jw2/pinThreadCommand"), JSON.stringify({ payload: encodeDevalue([{ threadID: 1, pinned: 2 }, secondThreadID, false]), refreshes: [] }), { ok: true });
	await regroupTestBridge.fetchLocalProjects(false);
	assert(regroupTestBridge.localSidebarCachedThreadID(incompleteSidebarThreadID), "incomplete sidebar shell was not cached");
	assert(!regroupTestBridge.localSidebarCachedThreadMetadataComplete(incompleteSidebarThreadID), "Untitled sidebar shell was treated as complete metadata");
	const incompleteSidebarFetchCount = localProjectsFetchCount;
	const incompleteSidebarResponse = new Response(JSON.stringify({
		type: "result",
		data: JSON.stringify([
			{ recentThreads: 1 },
			[2],
			{ id: 3, title: 4 },
			incompleteSidebarThreadID,
			"Untitled",
		]),
	}), { status: 200, headers: { "Content-Type": "application/json" } });
	Object.defineProperty(incompleteSidebarResponse, "url", { value: "https://ampcode.com/_app/remote/3abror/listThreadListSidebar" });
	const incompleteSidebarValues = JSON.parse((await incompleteSidebarResponse.json()).data);
	assert(localProjectsFetchCount === incompleteSidebarFetchCount + 1, "incomplete cached sidebar shell did not trigger one targeted metadata lookup");
	assert(new URL(localProjectsFetchURL).searchParams.getAll("cliproxy-sidebar-thread-id").includes(incompleteSidebarThreadID), "incomplete cached sidebar id was not sent to the metadata endpoint");
	const incompleteSidebarThreadRef = incompleteSidebarValues[incompleteSidebarValues[0].recentThreads].find((ref) =>
		incompleteSidebarValues[incompleteSidebarValues[ref].id] === incompleteSidebarThreadID
	);
	assert(incompleteSidebarValues[incompleteSidebarValues[incompleteSidebarThreadRef].title] === "Hydrated incomplete local thread", "incomplete sidebar response resolved before local title hydration");
	assert(regroupTestBridge.localSidebarCachedThreadMetadataComplete(incompleteSidebarThreadID), "targeted sidebar metadata remained incomplete");
	const completeSidebarFetchCount = localProjectsFetchCount;
	await regroupTestBridge.fetchLocalProjects(false, [secondThreadID]);
	assert(localProjectsFetchCount === completeSidebarFetchCount, "complete cached sidebar metadata triggered a redundant request");
	const unknownSidebarFetchCount = localProjectsFetchCount;
	await regroupTestBridge.fetchLocalProjects(false, [unknownSidebarThreadID]);
	await regroupTestBridge.fetchLocalProjects(false, [unknownSidebarThreadID]);
	assert(localProjectsFetchCount === unknownSidebarFetchCount + 1, "unknown sidebar metadata retried after its completed targeted attempt");
	await new Promise((resolve) => setTimeout(resolve, 5));
	const completeMutationRow = new FakeElement("a");
	completeMutationRow.dataset.sidebarThreadId = secondThreadID;
	completeMutationRow.appendChild(new FakeElement("span", "Second local thread"));
	documentQueryElements = [completeMutationRow];
	assert(regroupTestBridge.localSidebarMissingMetadataThreadIDs().length === 0, "complete sidebar row was classified as missing metadata");
	const completeMutationFetchCount = localProjectsFetchCount;
	const nativeDateNow = Date.now;
	Date.now = () => nativeDateNow() + 20000;
	regroupTestBridge.scheduleLocalSidebarMetadataRefresh();
	await new Promise((resolve) => setTimeout(resolve, 5));
	Date.now = nativeDateNow;
	assert(localProjectsFetchCount === completeMutationFetchCount, "complete sidebar row mutation restarted TTL metadata polling");
	const insertedUnknownRow = new FakeElement("a");
	insertedUnknownRow.dataset.sidebarThreadId = unknownSidebarThreadID;
	insertedUnknownRow.appendChild(new FakeElement("span", "Remote sidebar title"));
	documentQueryElements = [insertedUnknownRow];
	const insertedUnknownFetchCount = localProjectsFetchCount;
	regroupTestBridge.scheduleLocalSidebarMetadataRefresh();
	await new Promise((resolve) => setTimeout(resolve, 5));
	assert(localProjectsFetchCount === insertedUnknownFetchCount, "previously covered unknown sidebar row triggered another metadata lookup");
	const newlyInsertedUnknownThreadID = "T-019f324b-2802-7868-b1b1-5f0fa3e87eb5";
	insertedUnknownRow.dataset.sidebarThreadId = newlyInsertedUnknownThreadID;
	regroupTestBridge.scheduleLocalSidebarMetadataRefresh();
	await new Promise((resolve) => setTimeout(resolve, 5));
	regroupTestBridge.scheduleLocalSidebarMetadataRefresh();
	await new Promise((resolve) => setTimeout(resolve, 5));
	assert(localProjectsFetchCount === insertedUnknownFetchCount + 1, "new sidebar row did not make exactly one targeted metadata lookup");
	assert(new URL(localProjectsFetchURL).searchParams.getAll("cliproxy-sidebar-thread-id").includes(newlyInsertedUnknownThreadID), "new sidebar row id was not sent to the metadata endpoint");
	documentQueryElements = [];
	const hoverSidebarFetchCount = localProjectsFetchCount;
	const hoverSidebarResponse = new Response(JSON.stringify({
		type: "result",
		data: JSON.stringify([
			{ recentThreads: 1, projects: 5 },
			[2],
			{ id: 3, title: 4 },
			hoverSidebarThreadID,
			"Remote hover placeholder",
			[],
		]),
	}), { status: 200, headers: { "Content-Type": "application/json" } });
	Object.defineProperty(hoverSidebarResponse, "url", { value: "https://ampcode.com/_app/remote/3abror/listThreadListSidebar" });
	const hoverSidebarValues = JSON.parse((await hoverSidebarResponse.json()).data);
	assert(localProjectsFetchCount === hoverSidebarFetchCount + 1, "hover sidebar response did not fetch its thread metadata before rendering");
	assert(new URL(localProjectsFetchURL).searchParams.getAll("cliproxy-sidebar-thread-id").includes(hoverSidebarThreadID), "hover sidebar thread id was not sent to the local metadata endpoint");
	assert(!hoverSidebarValues[hoverSidebarValues[0].recentThreads].some((ref) =>
		hoverSidebarValues[hoverSidebarValues[ref].id] === hoverSidebarThreadID
	), "archived hover sidebar thread was removed and then reinserted");
	assert(canonicalSidebarThreadIDs.includes(secondThreadID), "missing local sidebar thread was not added");
	assert(bridge.diagnostics.localSidebarThreadMergeCount >= 6, "sidebar additions and enrichments were not recorded");
globalThis.localStorage.setItem(bridge.localThreadIDsStorageKey, JSON.stringify([threadID, secondThreadID, createdThreadID]));
globalThis.location = new URL("https://ampcode.com/threads/" + createdThreadID);
const staleSidebarThreadID = "T-019f3586-fb79-7309-a219-4a279ef2900c";
const sideEffectThreadID = "T-019f3586-fb79-7309-a219-4a279ef2900d";
const stalePlainArchived = JSON.parse(JSON.stringify({ id: createdThreadID, title: "Created", archived: true, executorConnected: false }));
assert(stalePlainArchived.archived === false, "plain active local thread retained stale archived state");
	const activeTitleBar = new FakeElement("div");
	activeTitleBar.appendChild(new FakeElement("h2", "Created"));
	const staleArchivedBadge = activeTitleBar.appendChild(new FakeElement("span", "Archived"));
	documentQueryElement = activeTitleBar;
	await regroupTestBridge.fetchLocalProjects(false);
	assert(!activeTitleBar.children.includes(staleArchivedBadge), "active local stale archived badge survived authoritative project refresh");
const staleDevalueArchived = JSON.parse(JSON.stringify([
	{ _: 1 },
	{ id: 2, title: 3, archived: 4, executorConnected: 5 },
	createdThreadID,
	"Created",
	true,
	false,
]));
assert(staleDevalueArchived[staleDevalueArchived[1].archived] === false, "devalue active local thread retained stale archived state");
globalThis.location = new URL("https://ampcode.com/threads/" + archivedThreadID);
const activeArchivedLocal = JSON.parse(JSON.stringify({ id: archivedThreadID, title: "Archived", archived: false, executorConnected: false }));
assert(activeArchivedLocal.archived === true, "locally archived active thread was unarchived by stale cloud state");
	const archivedTitleBar = new FakeElement("div");
	archivedTitleBar.appendChild(new FakeElement("h2", "Archived local thread"));
	const authoritativeArchivedBadge = archivedTitleBar.appendChild(new FakeElement("span", "Archived"));
	documentQueryElement = archivedTitleBar;
	await regroupTestBridge.fetchLocalProjects(false);
	assert(archivedTitleBar.children.includes(authoritativeArchivedBadge), "authoritative local archived badge was removed");
	documentQueryElement = null;
globalThis.location = new URL("https://ampcode.com/threads/" + createdThreadID);
const stalePlainSidebar = JSON.parse(JSON.stringify({
	recentThreads: [
		{ id: createdThreadID, title: "Created", hasExecutor: false, executorConnected: false, meta: { executorType: "local-client", usesThreadActors: true } },
		{ id: staleSidebarThreadID, title: "Remote", hasExecutor: false, executorConnected: false, meta: { executorType: "local-client", usesThreadActors: true } },
	],
}));
assert(!Object.hasOwn(stalePlainSidebar.recentThreads[0], "hasExecutor"), "plain local stale hasExecutor was not cleared");
assert(!Object.hasOwn(stalePlainSidebar.recentThreads[0], "executorConnected"), "plain local stale executorConnected was not cleared");
assert(stalePlainSidebar.recentThreads[1].hasExecutor === false, "plain nonlocal hasExecutor was changed");
assert(stalePlainSidebar.recentThreads[1].executorConnected === false, "plain nonlocal executorConnected was changed");
const stalePlainAlias = JSON.parse(JSON.stringify({ id: "not-a-thread-id", threadId: createdThreadID, title: "Created", hasExecutor: false, executorConnected: false }));
assert(!Object.hasOwn(stalePlainAlias, "hasExecutor"), "plain alias stale hasExecutor was not cleared");
assert(!Object.hasOwn(stalePlainAlias, "executorConnected"), "plain alias stale executorConnected was not cleared");
const activePlainArray = JSON.parse(JSON.stringify([{ id: createdThreadID, title: "Created", hasExecutor: false, executorConnected: false, threadActorConfig: null }]));
assertPlainConfig(activePlainArray[0].threadActorConfig, "created plain");
assert(!Object.hasOwn(activePlainArray[0], "hasExecutor"), "active plain array stale hasExecutor was not cleared");
assert(!Object.hasOwn(activePlainArray[0], "executorConnected"), "active plain array stale executorConnected was not cleared");
const stalePlainContainer = JSON.parse(JSON.stringify({ thread: { id: createdThreadID, title: "Created" }, hasExecutor: false, executorConnected: false, threadActorConfig: null }));
assertPlainConfig(stalePlainContainer.threadActorConfig, "created plain");
assert(!Object.hasOwn(stalePlainContainer, "hasExecutor"), "plain container stale hasExecutor was not cleared");
assert(!Object.hasOwn(stalePlainContainer, "executorConnected"), "plain container stale executorConnected was not cleared");
const staleDevalueSidebar = JSON.parse(JSON.stringify([
	{ recentThreads: 1 },
	[2, 7],
	{ id: 3, title: 4, hasExecutor: 5, executorConnected: 5, meta: 6 },
	createdThreadID,
	"Created",
	false,
	{ executorType: 8, usesThreadActors: 9 },
	{ id: 10, title: 11, hasExecutor: 5, executorConnected: 5, meta: 6 },
	"local-client",
	true,
	staleSidebarThreadID,
	"Remote",
]));
assert(!Object.hasOwn(staleDevalueSidebar[2], "hasExecutor"), "devalue local sidebar retained stale hasExecutor");
assert(!Object.hasOwn(staleDevalueSidebar[2], "executorConnected"), "devalue local sidebar retained stale executorConnected");
assert(staleDevalueSidebar[7].hasExecutor === 5, "devalue nonlocal hasExecutor was changed");
assert(staleDevalueSidebar[7].executorConnected === 5, "devalue nonlocal executorConnected was changed");
const staleDevalueAlias = JSON.parse(JSON.stringify([
	{ id: 1, threadID: 2, hasExecutor: 3, executorConnected: 3, title: 4 },
	"not-a-thread-id",
	createdThreadID,
	false,
	"Created",
]));
assert(!Object.hasOwn(staleDevalueAlias[0], "hasExecutor"), "devalue alias stale hasExecutor was not cleared");
assert(!Object.hasOwn(staleDevalueAlias[0], "executorConnected"), "devalue alias stale executorConnected was not cleared");
const devalueThreadSettingsMessage = JSON.parse(JSON.stringify([
	{ type: 1, threadID: 2, settings: 3 },
	"thread_settings",
	createdThreadID,
	{ agentMode: 4 },
	"deep",
]));
assert(!Object.hasOwn(devalueThreadSettingsMessage[0], "threadActorConfig"), "devalue thread_settings message was given a threadActorConfig");
const staleDevalueContainer = JSON.parse(JSON.stringify([
	{ thread: 1, hasExecutor: 3, executorConnected: 3 },
	{ id: 2, title: 4 },
	createdThreadID,
	false,
	"Created",
]));
assert(!Object.hasOwn(staleDevalueContainer[0], "hasExecutor"), "devalue container stale hasExecutor was not cleared");
assert(!Object.hasOwn(staleDevalueContainer[0], "executorConnected"), "devalue container stale executorConnected was not cleared");
const localThreadIDs = JSON.parse(globalThis.localStorage.getItem(bridge.localThreadIDsStorageKey));
localThreadIDs.unshift(sideEffectThreadID);
globalThis.localStorage.setItem(bridge.localThreadIDsStorageKey, JSON.stringify(localThreadIDs));
const observedBeforeContainerClear = bridge.diagnostics.lastObservedThreadID;
globalThis.location = new URL("https://ampcode.com/");
const sideEffectDevalueContainer = JSON.parse(JSON.stringify([
	{ thread: 1, hasExecutor: 3, executorConnected: 3 },
	{ id: 2, title: 4 },
	sideEffectThreadID,
	false,
	"Container",
]));
assert(!Object.hasOwn(sideEffectDevalueContainer[0], "hasExecutor"), "side-effect devalue container stale hasExecutor was not cleared");
assert(!Object.hasOwn(sideEffectDevalueContainer[0], "executorConnected"), "side-effect devalue container stale executorConnected was not cleared");
assert(bridge.diagnostics.lastObservedThreadID === observedBeforeContainerClear, "stale devalue container changed the observed thread");
globalThis.location = new URL("https://ampcode.com/threads/" + createdThreadID);
assert(bridge.diagnostics.localThreadStatusPatchCount >= 2, "local thread status patches were not recorded");
const createdPlain = JSON.parse(JSON.stringify({ threadData: { thread: { id: createdThreadID, title: "Created" }, threadActorConfig: null } }));
assertPlainConfig(createdPlain.threadData.threadActorConfig, "created plain");
assert(bridge.diagnostics.lastPatchedThreadID === createdThreadID, "created route-data patch did not record thread ID");
const threadDataResponse = new Response(JSON.stringify({
	type: "result",
	thread: { id: createdThreadID, title: "Created", hasExecutor: false, executorConnected: false },
	project: null,
}));
Object.defineProperty(threadDataResponse, "url", { value: "https://ampcode.com/threads/" + createdThreadID + "/__data" });
const decodedThreadDataResponse = await threadDataResponse.json();
assertPlainConfig(decodedThreadDataResponse.threadActorConfig, "created plain");
assert(!Object.hasOwn(decodedThreadDataResponse.thread, "hasExecutor"), "thread data response stale hasExecutor was not cleared");
assert(!Object.hasOwn(decodedThreadDataResponse.thread, "executorConnected"), "thread data response stale executorConnected was not cleared");
assert(bridge.diagnostics.lastPatchedThreadID === createdThreadID, "thread data response patch did not record thread ID");
const localThreadDataResponse = new Response(JSON.stringify({
	type: "result",
	thread: { id: createdThreadID, title: "Created local", hasExecutor: false, executorConnected: false },
	project: null,
}));
Object.defineProperty(localThreadDataResponse, "url", { value: "http://127.0.0.1:8317/threads/" + createdThreadID + "/__data" });
const decodedLocalThreadDataResponse = await localThreadDataResponse.json();
assertPlainConfig(decodedLocalThreadDataResponse.threadActorConfig, "created plain");
assert(!Object.hasOwn(decodedLocalThreadDataResponse.thread, "hasExecutor"), "local thread data response stale hasExecutor was not cleared");
assert(!Object.hasOwn(decodedLocalThreadDataResponse.thread, "executorConnected"), "local thread data response stale executorConnected was not cleared");
const jsonThreadDataResponse = new Response(JSON.stringify({
	type: "data",
	nodes: [
		null,
		{
			type: "data",
			data: [
				{ threadData: 1 },
				{ thread: 2, threadActorConfig: 5 },
				{ id: 3, title: 4, hasExecutor: 6, executorConnected: 6 },
				createdThreadID,
				"Created JSON",
				null,
				false,
			],
		},
	],
}));
Object.defineProperty(jsonThreadDataResponse, "url", { value: "https://ampcode.com/threads/" + createdThreadID + "/__data.json" });
const decodedJSONThreadDataResponse = await jsonThreadDataResponse.json();
const jsonValues = decodedJSONThreadDataResponse.nodes[1].data;
const jsonConfigIndex = jsonValues[1].threadActorConfig;
assert(Number.isInteger(jsonConfigIndex), "json route-data threadActorConfig was not patched");
assert(jsonConfigIndex !== 5, "json route-data threadActorConfig still points at null");
const jsonConfig = jsonValues[jsonConfigIndex];
const jsonDeref = (value) => Number.isInteger(value) ? jsonValues[value] : value;
assert(jsonDeref(jsonConfig.threadId) === createdThreadID, "json route-data threadId mismatch");
assert(jsonDeref(jsonConfig.wsToken) === "local-key", "json route-data wsToken mismatch");
assert(!Object.hasOwn(jsonValues[2], "hasExecutor"), "json route-data stale hasExecutor was not cleared");
assert(!Object.hasOwn(jsonValues[2], "executorConnected"), "json route-data stale executorConnected was not cleared");
assert(bridge.diagnostics.responseJSONPatchCount >= 3, "response json patches were not recorded");
const nestedRouteData = JSON.parse(JSON.stringify({
	type: "data",
	nodes: [
		null,
		{
			type: "data",
			data: [
				{ threadData: 1 },
				{ thread: 2, threadActorConfig: 5 },
				{ id: 3, title: 4 },
				createdThreadID,
				"Created",
				null,
			],
		},
	],
}));
const nestedValues = nestedRouteData.nodes[1].data;
const nestedConfigIndex = nestedValues[1].threadActorConfig;
assert(Number.isInteger(nestedConfigIndex), "nested route-data threadActorConfig was not patched");
assert(nestedConfigIndex !== 5, "nested route-data threadActorConfig still points at null");
const nestedConfig = nestedValues[nestedConfigIndex];
const nestedDeref = (value) => Number.isInteger(value) ? nestedValues[value] : value;
assert(nestedDeref(nestedConfig.threadId) === createdThreadID, "nested route-data threadId mismatch");
assert(nestedDeref(nestedConfig.wsToken) === "local-key", "nested route-data wsToken mismatch");
assert(nestedDeref(nestedConfig.ampURL) === "http://127.0.0.1:8317", "nested route-data ampURL mismatch");
assert(bridge.diagnostics.lastPatchedThreadID === createdThreadID, "nested route-data patch did not record thread ID");
const discoveredThreadID = "T-019f3586-fb79-7309-a219-4a279ef2900e";
globalThis.location = new URL("https://ampcode.com/threads/" + discoveredThreadID);
const mixedLocality = JSON.parse(JSON.stringify({
	id: discoveredThreadID,
	hasExecutor: false,
	executorConnected: false,
	threadActorConfig: null,
	nested: [
		{ threadActorConfig: 1 },
		{ threadId: 2, baseURL: 3, wsToken: 4 },
		discoveredThreadID,
		"https://ampcode.com",
		"remote-token",
	],
}));
assertPlainConfig(mixedLocality.threadActorConfig, "nested-discovered plain", discoveredThreadID);
assert(!Object.hasOwn(mixedLocality, "hasExecutor"), "nested-discovered plain stale hasExecutor was not cleared");
assert(!Object.hasOwn(mixedLocality, "executorConnected"), "nested-discovered plain stale executorConnected was not cleared");
assert(JSON.parse(globalThis.localStorage.getItem(bridge.localThreadIDsStorageKey)).includes(discoveredThreadID), "nested devalue config did not establish local thread identity");
const pureDevalueThreadID = "T-019f3586-fb79-7309-a219-4a279ef2900f";
globalThis.location = new URL("https://ampcode.com/threads/" + pureDevalueThreadID);
const pureDevalueLocality = JSON.parse(JSON.stringify([
	{ current: 1, configSource: 6 },
	{ thread: 2, hasExecutor: 5, executorConnected: 5 },
	{ id: 3, title: 4 },
	pureDevalueThreadID,
	"Pure devalue",
	false,
	{ thread: 2, threadActorConfig: 7 },
	{ threadId: 3, baseURL: 8, wsToken: 9 },
	"https://ampcode.com",
	"remote-token",
]));
assert(!Object.hasOwn(pureDevalueLocality[1], "hasExecutor"), "later devalue locality did not clear stale hasExecutor");
assert(!Object.hasOwn(pureDevalueLocality[1], "executorConnected"), "later devalue locality did not clear stale executorConnected");
const pureDevalueConfigIndex = pureDevalueLocality[1].threadActorConfig;
assert(Number.isInteger(pureDevalueConfigIndex), "later devalue locality did not add a threadActorConfig");
const pureDevalueConfig = pureDevalueLocality[pureDevalueConfigIndex];
const pureDevalueDeref = (value) => Number.isInteger(value) ? pureDevalueLocality[value] : value;
assert(pureDevalueDeref(pureDevalueConfig.threadId) === pureDevalueThreadID, "later devalue locality config threadId mismatch");
assert(pureDevalueDeref(pureDevalueConfig.baseURL) === "http://127.0.0.1:8317", "later devalue locality config baseURL mismatch");
globalThis.location = new URL("https://ampcode.com/threads/" + createdThreadID);
const socket = new WebSocket("wss://ampcode.com/gateway/threadActor/?rvt-method=get&rvt-key=" + encodeURIComponent(secondThreadID));
const rewritten = new URL(socket.url);
assert(rewritten.protocol === "ws:", "websocket protocol was not rewritten");
assert(rewritten.host === "127.0.0.1:8317", "websocket host was not rewritten");
assert(rewritten.searchParams.get("cliproxy-api-key") === "local-key", "localStorage API key was not applied");
assert(rewritten.searchParams.get("cliproxy-bootstrap-executor") === "true", "bootstrap executor flag missing");
const relativeSocket = new WebSocket("/gateway/threadActor/?rvt-method=get&rvt-key=" + encodeURIComponent(secondThreadID));
const relativeRewritten = new URL(relativeSocket.url);
assert(relativeRewritten.protocol === "ws:", "relative websocket protocol was not rewritten");
assert(relativeRewritten.host === "127.0.0.1:8317", "relative websocket host was not rewritten");
assert(relativeRewritten.searchParams.get("cliproxy-api-key") === "local-key", "relative localStorage API key was not applied");
const localHTTPConfigSocket = new WebSocket("http://127.0.0.1:8317/gateway/threadActor/websocket/?rvt-method=get&rvt-key=" + encodeURIComponent(secondThreadID));
const localHTTPRewritten = new URL(localHTTPConfigSocket.url);
assert(localHTTPRewritten.protocol === "ws:", "local http websocket protocol was not rewritten");
assert(localHTTPRewritten.host === "127.0.0.1:8317", "local http websocket host changed");
assert(localHTTPRewritten.searchParams.get("cliproxy-api-key") === "local-key", "local http websocket localStorage API key was not applied");
assert(bridge.diagnostics.decodedConfigPatchCount >= 2, "decoded config patches were not recorded");
assert(bridge.diagnostics.decodedGraphPassCount > 0, "decoded graph passes were not recorded");
assert(bridge.diagnostics.decodedGraphVisitCount >= bridge.diagnostics.decodedGraphPassCount, "decoded graph visits were not recorded");
assert(bridge.diagnostics.webSocketBootstrapCount === 13, "websocket bootstrap was not recorded");
	globalThis.location = new URL("https://ampcode.com/threads/" + secondThreadID);
	const visibleLocalSocket = new WebSocket("wss://ampcode.com/gateway/threadActor/?rvt-method=get&rvt-key=" + encodeURIComponent(secondThreadID));
	let suppressedSocket = null;
	visibleLocalSocket.addEventListener("close", () => {
		suppressedSocket = new WebSocket("wss://ampcode.com/gateway/threadActor/?rvt-method=get&rvt-key=" + encodeURIComponent(secondThreadID));
	});
	let pagehideSuppressedSocket = null;
	const closeVisibleLocalSocket = visibleLocalSocket.close.bind(visibleLocalSocket);
	visibleLocalSocket.close = (code, reason) => {
		if (!pagehideSuppressedSocket) {
			pagehideSuppressedSocket = new WebSocket("wss://ampcode.com/gateway/threadActor/?rvt-method=get&rvt-key=" + encodeURIComponent(secondThreadID));
		}
		closeVisibleLocalSocket(code, reason);
	};
	const nativeSocketCountBeforePause = NativeWebSocket.instances.length;
	const hiddenSuppressionCountBeforePause = bridge.diagnostics.hiddenPageSocketConstructionSuppressionCount;
	const hiddenResumeCountBeforePause = bridge.diagnostics.hiddenPageSocketResumeCount;
	globalThis.document.visibilityState = "hidden";
	dispatchDocumentEvent("visibilitychange");
	globalThis.dispatchEvent({ type: "pagehide" });
	await new Promise((resolve) => setTimeout(resolve, 10));
	assert(visibleLocalSocket.readyState === NativeWebSocket.CLOSED, "hidden pause did not close the visible local thread socket");
	assert(pagehideSuppressedSocket?.readyState === NativeWebSocket.CONNECTING, "pagehide cleanup constructed a replacement before hidden pause state was set");
	assert(suppressedSocket?.readyState === NativeWebSocket.CONNECTING, "hidden close did not produce a deferred replacement socket");
	assert(NativeWebSocket.instances.length === nativeSocketCountBeforePause, "pagehide or hidden close constructed a native replacement socket");
	const repeatedSuppressedSocket = new WebSocket("wss://ampcode.com/gateway/threadActor/?rvt-method=get&rvt-key=" + encodeURIComponent(secondThreadID));
	assert(NativeWebSocket.instances.length === nativeSocketCountBeforePause, "repeated hidden local thread reconnect constructed a native socket");
	assert(pagehideSuppressedSocket.readyState === NativeWebSocket.CONNECTING && suppressedSocket.readyState === NativeWebSocket.CONNECTING && repeatedSuppressedSocket.readyState === NativeWebSocket.CONNECTING, "suppressed sockets did not remain safely connecting while hidden");
	assert(suppressedSocket instanceof WebSocket, "suppressed socket lost WebSocket prototype compatibility");
	assert(suppressedSocket.CONNECTING === NativeWebSocket.CONNECTING && suppressedSocket.OPEN === NativeWebSocket.OPEN && suppressedSocket.CLOSING === NativeWebSocket.CLOSING && suppressedSocket.CLOSED === NativeWebSocket.CLOSED, "suppressed socket lost WebSocket state constants");
	assert(Object.prototype.toString.call(suppressedSocket) === "[object WebSocket]", "suppressed socket lost WebSocket string tag");
	class DeferredSocketSubclass extends WebSocket {}
	class DeferredSocketLeaf extends DeferredSocketSubclass {}
	const explicitlyClosedSocket = new DeferredSocketLeaf("wss://ampcode.com/gateway/threadActor/?rvt-method=get&rvt-key=" + encodeURIComponent(secondThreadID));
	assert(explicitlyClosedSocket instanceof WebSocket && explicitlyClosedSocket instanceof DeferredSocketSubclass && explicitlyClosedSocket instanceof DeferredSocketLeaf, "suppressed socket lost subclass instanceof compatibility");
	let explicitCloseCount = 0;
	explicitlyClosedSocket.onclose = function(event) {
		assert(this === explicitlyClosedSocket && event.target === explicitlyClosedSocket && event.currentTarget === explicitlyClosedSocket, "explicit suppressed socket close lost EventTarget semantics");
		assert(event.code === 1000 && event.reason === "client close" && event.wasClean === true, "explicit suppressed socket close metadata was incorrect");
		explicitCloseCount += 1;
	};
	explicitlyClosedSocket.close(1000, "client close");
	await new Promise((resolve) => setTimeout(resolve, 0));
	assert(explicitCloseCount === 1 && explicitlyClosedSocket.readyState === NativeWebSocket.CLOSED, "explicit suppressed socket close was not dispatched exactly once");
	let resumedCloseCount = 0;
	let recoveredLocalSocket = null;
	suppressedSocket.addEventListener("close", function(event) {
		assert(this === suppressedSocket, "suppressed socket close lost listener this");
		assert(event.target === suppressedSocket, "suppressed socket close lost event target");
		assert(event.currentTarget === suppressedSocket, "suppressed socket close lost current event target");
		assert(event.code === 1006 && event.reason === "page visible" && event.wasClean === false, "suppressed socket close metadata was incorrect");
		resumedCloseCount += 1;
		recoveredLocalSocket = new WebSocket("wss://ampcode.com/gateway/threadActor/?rvt-method=get&rvt-key=" + encodeURIComponent(secondThreadID));
	});
	repeatedSuppressedSocket.onclose = function(event) {
		assert(this === repeatedSuppressedSocket, "suppressed socket onclose lost listener this");
		assert(event.target === repeatedSuppressedSocket, "suppressed socket onclose lost event target");
		assert(event.currentTarget === repeatedSuppressedSocket, "suppressed socket onclose lost current event target");
		resumedCloseCount += 1;
	};
	const hiddenUserActorSocket = new WebSocket("wss://ampcode.com/gateway/userActor/?rvt-method=get&rvt-key=" + encodeURIComponent(ampViewerUserID));
	const hiddenNonlocalSocket = new WebSocket("wss://example.com/unrelated");
	assert(NativeWebSocket.instances.length === nativeSocketCountBeforePause + 2, "hidden suppression affected user-actor or nonlocal sockets");
	assert(new URL(hiddenUserActorSocket.url).origin === "ws://127.0.0.1:8317", "hidden local user-actor socket was not bridged normally");
	assert(new URL(hiddenNonlocalSocket.url).origin === "wss://example.com", "hidden nonlocal socket was rewritten");
	assert(bridge.diagnostics.hiddenPageSocketConstructionSuppressionCount === hiddenSuppressionCountBeforePause + 4, "hidden socket suppressions were not recorded");
	assert(bridge.diagnostics.hiddenPageDeferredSocketCount === 3, "hidden deferred socket count was not recorded");
	globalThis.document.visibilityState = "visible";
	dispatchDocumentEvent("visibilitychange");
	await new Promise((resolve) => setTimeout(resolve, 0));
	assert(pagehideSuppressedSocket.readyState === NativeWebSocket.CLOSED && suppressedSocket.readyState === NativeWebSocket.CLOSED && repeatedSuppressedSocket.readyState === NativeWebSocket.CLOSED, "suppressed sockets were not released on visibility recovery");
	assert(resumedCloseCount === 2 && bridge.diagnostics.hiddenPageSocketResumeCount === hiddenResumeCountBeforePause + 3, "visibility recovery did not release each deferred reconnect exactly once");
	assert(bridge.diagnostics.hiddenPageDeferredSocketCount === 0, "visibility recovery retained deferred sockets");
	assert(NativeWebSocket.instances.length === nativeSocketCountBeforePause + 3, "visibility recovery did not construct exactly one native local thread socket");
	assert(new URL(recoveredLocalSocket.url).origin === "ws://127.0.0.1:8317", "visible recovery did not preserve the local thread bridge");
	let delayedHiddenReplacementSocket = null;
	recoveredLocalSocket.addEventListener("close", () => {
		globalThis.setTimeout(() => {
			delayedHiddenReplacementSocket = new WebSocket("ws://127.0.0.1:8317/actors/gateway/threadActor/websocket/?actorId=local-actor&cliproxy-api-key=local-key", ["rivet_actor.local-actor"]);
		}, 1);
	});
	const nativeSocketCountBeforeDelayedHiddenReconnect = NativeWebSocket.instances.length;
	globalThis.document.visibilityState = "hidden";
	dispatchDocumentEvent("visibilitychange");
	recoveredLocalSocket.close(1006, "network hidden");
	await new Promise((resolve) => setTimeout(resolve, 3));
	assert(delayedHiddenReplacementSocket?.readyState === NativeWebSocket.CONNECTING, "delayed hidden close did not produce a deferred actor-ID reconnect");
	assert(NativeWebSocket.instances.length === nativeSocketCountBeforeDelayedHiddenReconnect, "delayed hidden actor-ID reconnect constructed a native socket before the pause timer");
	await new Promise((resolve) => setTimeout(resolve, 10));
	assert(delayedHiddenReplacementSocket.readyState === NativeWebSocket.CONNECTING, "delayed hidden actor-ID reconnect did not remain deferred throughout the hidden grace period");
	assert(NativeWebSocket.instances.length === nativeSocketCountBeforeDelayedHiddenReconnect, "hidden grace timer constructed a native actor-ID reconnect");
	delayedHiddenReplacementSocket.close(1000, "test complete");
	await new Promise((resolve) => setTimeout(resolve, 0));
	globalThis.document.visibilityState = "visible";
	dispatchDocumentEvent("visibilitychange");
	await new Promise((resolve) => setTimeout(resolve, 0));
	const nativeSocketCountBeforePageShowRecovery = NativeWebSocket.instances.length;
	globalThis.document.visibilityState = "hidden";
	globalThis.dispatchEvent({ type: "pagehide" });
	const pageShowSuppressedSocket = new WebSocket("wss://ampcode.com/gateway/threadActor/?rvt-method=get&rvt-key=" + encodeURIComponent(secondThreadID));
	assert(NativeWebSocket.instances.length === nativeSocketCountBeforePageShowRecovery, "second hidden cycle constructed a native local thread socket");
	globalThis.document.visibilityState = "visible";
	globalThis.dispatchEvent({ type: "pageshow" });
	await new Promise((resolve) => setTimeout(resolve, 0));
	assert(pageShowSuppressedSocket.readyState === NativeWebSocket.CLOSED, "pageshow recovery did not release the deferred socket");
	assert(bridge.diagnostics.hiddenPageSocketResumeCount === hiddenResumeCountBeforePause + 4, "pageshow recovery did not release the deferred reconnect exactly once");
	assert(bridge.diagnostics.hiddenPageDeferredSocketCount === 0, "pageshow recovery retained deferred sockets");
	assert(NativeWebSocket.instances.length === nativeSocketCountBeforePageShowRecovery, "pageshow recovery constructed an unexpected local thread socket");
		globalThis.document.visibilityState = "hidden";
		globalThis.dispatchEvent({ type: "pagehide" });
		const boundedSuppressedSockets = Array.from({ length: 70 }, () => new WebSocket("wss://ampcode.com/gateway/threadActor/?rvt-method=get&rvt-key=" + encodeURIComponent(secondThreadID)));
		assert(bridge.diagnostics.hiddenPageDeferredSocketCount === 64, "hidden deferred socket retention exceeded its bound");
		assert(boundedSuppressedSockets.slice(0, 6).every((socket) => socket.readyState === NativeWebSocket.CLOSED), "oldest hidden deferred sockets were not evicted");
		for (const socket of boundedSuppressedSockets) socket.close(1000, "test complete");
		await new Promise((resolve) => setTimeout(resolve, 0));
		assert(bridge.diagnostics.hiddenPageDeferredSocketCount === 0, "explicitly closed bounded deferred sockets remained retained");
		globalThis.document.visibilityState = "visible";
		globalThis.dispatchEvent({ type: "pageshow" });
})().catch((error) => {
	console.error(error && error.stack ? error.stack : error);
	process.exit(1);
});
`
	runnerPath := filepath.Join(dir, "run-userscript-test.js")
	if err := os.WriteFile(runnerPath, []byte(runner), 0o600); err != nil {
		t.Fatalf("write runner: %v", err)
	}
	cmd := exec.Command("node", runnerPath)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("userscript behavior check failed: %v\n%s", err, output)
	}
}

func TestWebLocalInferenceUserscriptProjectPickerPrefersVisibleProject(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("node not installed")
	}
	dir := t.TempDir()
	scriptPath := filepath.Join(dir, "local-inference.user.js")
	script := ampWebLocalInferenceUserscript("http://127.0.0.1:8317", nil)
	exported := strings.Replace(script, "\tglobalThis.__cliproxyAmpLocalInference = {", "\tglobalThis.__cliproxyAmpLocalInferenceProjectListTest = { mergeProjectListResponse };\n\tglobalThis.__cliproxyAmpLocalInference = {", 1)
	if exported == script {
		t.Fatal("userscript missing local inference bridge")
	}
	if err := os.WriteFile(scriptPath, []byte(exported), 0o600); err != nil {
		t.Fatalf("write userscript: %v", err)
	}
	runner := `
(async () => {
const assert = (condition, message) => {
	if (!condition) throw new Error(message);
};
const scriptPath = ` + strconv.Quote(scriptPath) + `;
const nativeArraySome = Array.prototype.some;
class TestStorage {
	constructor() { this.values = new Map(); }
	getItem(key) { key = String(key); return this.values.has(key) ? this.values.get(key) : null; }
	setItem(key, value) { this.values.set(String(key), String(value)); }
	removeItem(key) { this.values.delete(String(key)); }
}
const datasetKey = (name) => String(name || "").replace(/^data-/, "").replace(/-([a-z])/g, (_, char) => char.toUpperCase());
class FakeEvent {
	constructor(type, init = {}) {
		this.type = type;
		this.key = init.key || "";
		this.bubbles = !!init.bubbles;
		this.cancelable = !!init.cancelable;
		this.defaultPrevented = false;
		this.propagationStopped = false;
		this.target = null;
	}
	preventDefault() { this.defaultPrevented = true; }
	stopPropagation() { this.propagationStopped = true; }
}
class FakeElement {
	constructor(tagName = "div") {
		this.tagName = String(tagName).toUpperCase();
		this.dataset = {};
		this.attributes = new Map();
		this.style = {};
		this.children = [];
		this.parentElement = null;
		this.eventListeners = {};
		this.className = "";
		this.disabled = false;
		this.hidden = false;
		this.value = "";
		this._text = "";
	}
	get firstElementChild() { return this.children[0] || null; }
	get href() {
		const value = this.getAttribute("href");
		return value ? new URL(value, globalThis.location.href).href : "";
	}
	get textContent() { return this._text + this.children.map((child) => child.textContent).join(""); }
	set textContent(value) { this._text = String(value || ""); this.children = []; }
	get innerText() {
		const childText = this.children.map((child) => child.innerText).filter(Boolean);
		if (!childText.length) return this._text;
		const separator = ["BUTTON", "DIV"].includes(this.tagName) ? "\n" : "";
		return [this._text, childText.join(separator)].filter(Boolean).join(separator);
	}
	set innerText(value) { this.textContent = value; }
	append(...nodes) { for (const node of nodes) this.appendChild(node); }
	appendChild(child) {
		if (!(child instanceof FakeElement)) return child;
		child.remove();
		child.parentElement = this;
		this.children.push(child);
		return child;
	}
	insertBefore(child, before) {
		if (!(child instanceof FakeElement)) return child;
		child.remove();
		child.parentElement = this;
		const index = before ? this.children.indexOf(before) : -1;
		if (index >= 0) this.children.splice(index, 0, child);
		else this.children.push(child);
		return child;
	}
	replaceChildren(...nodes) {
		for (const child of this.children) child.parentElement = null;
		this.children = [];
		this._text = "";
		this.append(...nodes);
	}
	remove() {
		if (!this.parentElement) return;
		const index = this.parentElement.children.indexOf(this);
		if (index >= 0) this.parentElement.children.splice(index, 1);
		this.parentElement = null;
	}
	setAttribute(name, value) {
		name = String(name);
		value = String(value);
		this.attributes.set(name, value);
		if (name.startsWith("data-")) this.dataset[datasetKey(name)] = value;
		if (name === "role") this.role = value;
	}
	getAttribute(name) {
		name = String(name);
		if (name.startsWith("data-") && this.dataset[datasetKey(name)] !== undefined) return this.dataset[datasetKey(name)];
		return this.attributes.has(name) ? this.attributes.get(name) : null;
	}
	removeAttribute(name) {
		name = String(name);
		this.attributes.delete(name);
		if (name.startsWith("data-")) delete this.dataset[datasetKey(name)];
	}
	matches(selector) {
		return String(selector).split(",").some((raw) => {
			const part = raw.trim();
			if (!part) return false;
			if (part === "*") return true;
			if (/^[a-z]+$/i.test(part)) return this.tagName.toLowerCase() === part.toLowerCase();
			const tagAttribute = part.match(/^([a-z]+)(\[.+\])$/i);
			if (tagAttribute) return this.tagName.toLowerCase() === tagAttribute[1].toLowerCase() && this.matches(tagAttribute[2]);
			const match = part.match(/^\[([^=\]]+)(?:=['"]?([^'"\]]+)['"]?)?\]$/);
			if (!match) return false;
			const name = match[1];
			const expected = match[2];
			const value = this.getAttribute(name);
			return expected === undefined ? value !== null || this.dataset[datasetKey(name)] !== undefined : value === expected;
		});
	}
	querySelectorAll(selector) {
		const out = [];
		const visit = (element) => {
			for (const child of element.children) {
				if (child.matches(selector)) out.push(child);
				visit(child);
			}
		};
		visit(this);
		return out;
	}
	querySelector(selector) { return this.querySelectorAll(selector)[0] || null; }
	closest(selector) {
		for (let node = this; node; node = node.parentElement) {
			if (node.matches(selector)) return node;
		}
		return null;
	}
	contains(target) {
		for (let node = target; node; node = node.parentElement) {
			if (node === this) return true;
		}
		return false;
	}
	addEventListener(type, callback) {
		(this.eventListeners[type] ||= []).push(callback);
	}
	removeEventListener() {}
	dispatchEvent(event) {
		event.target ||= this;
		for (const callback of this.eventListeners[event.type] || []) {
			callback.call(this, event);
			if (event.propagationStopped) return !event.defaultPrevented;
		}
		if (event.bubbles && this.parentElement) {
			return this.parentElement.dispatchEvent(event);
		}
		return !event.defaultPrevented;
	}
	getBoundingClientRect() {
		if (this.hidden || this.style.display === "none") return { width: 0, height: 0, left: 0, top: 0, right: 0, bottom: 0 };
		return { width: 320, height: 32, left: 0, top: 0, right: 320, bottom: 32 };
	}
	scrollIntoView() {}
}
const documentElement = new FakeElement("html");
const body = new FakeElement("body");
documentElement.appendChild(body);
globalThis.Element = FakeElement;
globalThis.HTMLElement = FakeElement;
globalThis.NodeFilter = { SHOW_TEXT: 4, SHOW_ELEMENT: 1 };
globalThis.KeyboardEvent = class extends FakeEvent { constructor(type, init) { super(type, init); } };
globalThis.MouseEvent = class extends FakeEvent { constructor(type, init) { super(type, init); } };
const mutationObservers = [];
globalThis.MutationObserver = class {
	constructor(callback) { this.callback = callback; mutationObservers.push(this); }
	observe() {}
	disconnect() { this.disconnected = true; }
};
const animationFrameCallbacks = new Map();
let nextAnimationFrameID = 1;
globalThis.requestAnimationFrame = (callback) => {
	const id = nextAnimationFrameID++;
	animationFrameCallbacks.set(id, callback);
	return id;
};
globalThis.cancelAnimationFrame = (id) => animationFrameCallbacks.delete(id);
const flushAnimationFrames = () => {
	for (const [id, callback] of Array.from(animationFrameCallbacks)) {
		animationFrameCallbacks.delete(id);
		callback(Date.now());
	}
};
const authenticatedBootstrapScript = {
	textContent: 'globalThis.data=(function(a){a.id="viewer-user";a.username="aikins01";return {initialProjects:[],userFeatures:[]};})({});',
};
globalThis.document = {
	readyState: "loading",
	body,
	documentElement,
	createElement(tag) { return new FakeElement(tag); },
	querySelector(selector) { return documentElement.querySelector(selector); },
	querySelectorAll(selector) { return selector === "script:not([src])" ? [authenticatedBootstrapScript] : documentElement.querySelectorAll(selector); },
	getElementById(id) { return documentElement.querySelectorAll("*").find((element) => element.getAttribute("id") === String(id)) || null; },
	contains(target) { return documentElement.contains(target); },
	addEventListener(type, callback) { documentElement.addEventListener(type, callback); },
	removeEventListener(type, callback) { documentElement.removeEventListener(type, callback); },
	dispatchEvent(event) { return documentElement.dispatchEvent(event); },
	createTreeWalker() { return { currentNode: null, nextNode() { return null; } }; },
};
globalThis.location = new URL("https://ampcode.com/threads/T-019f324b-2802-7868-b1b1-5f0fa3e87ea5");
globalThis.history = {
	state: null,
	replaceState() {},
	pushState(_state, _title, path) { globalThis.location = new URL(path, globalThis.location.href); },
	back() {},
};
globalThis.localStorage = new TestStorage();
globalThis.sessionStorage = new TestStorage();
globalThis.sessionStorage.setItem("cliproxyapi.ampLocalInference.apiKey.user." + encodeURIComponent("viewer-user") + "." + encodeURIComponent("http://127.0.0.1:8317"), "local-key");
globalThis.localStorage.setItem("cliproxyapi.ampLocalInference.workingDirectory", "/Users/aikins01/Developer/on-chain");
globalThis.sessionStorage.setItem("cliproxyapi.ampLocalInference.selectedLocalProject", JSON.stringify({
	name: "on-chain",
	workingDirectory: "/Users/aikins01/Developer/on-chain",
	selectedAt: Date.now(),
}));
const tempTelemetryDirectory = "/private/tmp/telemetry-pr81-review-6soxv4lc/telemetry.dev";
let localProjectsPayload = {
	defaultWorkingDirectory: "/Users/aikins01",
	projects: [
		{ name: "aikins01", workingDirectory: "/Users/aikins01" },
		{ id: "on-chain-local", name: "on-chain", namespace: "Vela-Engineering", repositoryURL: "https://github.com/Vela-Engineering/on-chain.git", workingDirectory: "/Users/aikins01/Developer/on-chain", localOnly: false },
		{ name: "telemetry.dev", workingDirectory: tempTelemetryDirectory },
		{ name: "telemetry.dev", workingDirectory: "/private/var/folders/63/_bz0gwdn0px0d4r7s9zhct8m0000gn/T/telemetry-pr78-review-XXXXXX.RGqjFliah5/telemetry.dev" },
		{ name: "telemetry.dev", namespace: "telemetry-dev", repositoryURL: "https://github.com/telemetry-dev/telemetry.dev.git", workingDirectory: "/Users/aikins01/Developer/telemetry.dev" },
	],
};
	let deferLocalProjectsFetch = false;
	const deferredLocalProjectsFetches = [];
	let lastFetchURL = "";
	let localProjectDetailsFetchCount = 0;
	globalThis.fetch = async (url) => {
		lastFetchURL = String(url);
		if (lastFetchURL.includes("/_app/remote/3abror/createProjectThread")) {
			return new Response(JSON.stringify({ data: "" }), { status: 200, headers: { "Content-Type": "application/json" } });
		}
		if (lastFetchURL.includes("/ampcode/local-project-details.json")) {
			localProjectDetailsFetchCount += 1;
			return new Response(JSON.stringify({
				ok: true,
				project: { id: "telemetry-local", name: "telemetry.dev", namespace: "telemetry-dev", repositoryURL: "https://github.com/telemetry-dev/telemetry.dev.git", workingDirectory: "/Users/aikins01/Developer/telemetry.dev" },
				defaultBranch: "main",
				currentBranch: "",
				detachedAt: "01234567",
				hasLocalChanges: false,
				commits: [{ sha: "0123456789abcdef", shortSha: "01234567", authorName: "Aikins", committedAt: "2026-07-21T10:00:00Z", message: "Local project page" }],
				files: ["README.md", "internal/runtime.go"],
				filesLimit: 2000,
				filesTruncated: true,
				threads: [{ id: "T-019f324b-2802-7868-b1b1-5f0fa3e87ea5", title: "Local project thread", updatedAt: "2026-07-21T10:00:00Z", messageCount: 3 }],
			}), { status: 200, headers: { "Content-Type": "application/json" } });
		}
		assert(lastFetchURL.includes("/ampcode/local-projects.json"), "unexpected fetch " + url);
		const responseBody = JSON.stringify(localProjectsPayload);
			if (deferLocalProjectsFetch) {
				return new Promise((resolve) => deferredLocalProjectsFetches.push(() => resolve(new Response(responseBody, { status: 200, headers: { "Content-Type": "application/json" } }))));
			}
			return new Response(responseBody, { status: 200, headers: { "Content-Type": "application/json" } });
	};
globalThis.prompt = () => "";
globalThis.WebSocket = class {};
const encodeDevalue = (value) => Buffer.from(JSON.stringify(value)).toString("base64url");
const headerProjectLink = new FakeElement("a");
headerProjectLink.setAttribute("href", "https://github.com/telemetry-dev/telemetry.dev/tree/6bcbce6911b604cfc8dd8255bc9215a930222dfb");
headerProjectLink.textContent = "telemetry.dev";
const message = new FakeElement("article");
const messageProjectLink = new FakeElement("a");
messageProjectLink.setAttribute("href", "https://github.com/example/wrong/tree/main");
messageProjectLink.textContent = "wrong";
message.appendChild(messageProjectLink);
const broadProjectLink = new FakeElement("a");
broadProjectLink.setAttribute("href", "https://github.com/example/body-wrong/tree/main");
broadProjectLink.textContent = "body-wrong";
const broadMoreActions = new FakeElement("button");
broadMoreActions.textContent = "More Actions";
const headerContainer = new FakeElement("div");
const moreActions = new FakeElement("button");
moreActions.setAttribute("aria-label", "More Actions");
moreActions.textContent = "More Actions";
headerContainer.append(moreActions, headerProjectLink);
const picker = new FakeElement("div");
picker.setAttribute("role", "dialog");
const nearMissPicker = new FakeElement("div");
	nearMissPicker.setAttribute("role", "dialog");
	const nearMissInput = new FakeElement("input");
	const nearMissTitle = new FakeElement("div");
	nearMissTitle.textContent = "Projects";
	const nearMissDescription = new FakeElement("div");
	nearMissDescription.textContent = "No Project is configured";
	const nearMissList = new FakeElement("div");
	nearMissList.setAttribute("role", "listbox");
	const nearMissOption = new FakeElement("button");
	nearMissOption.setAttribute("role", "option");
	nearMissOption.textContent = "Open Projects";
	nearMissList.appendChild(nearMissOption);
	nearMissPicker.append(nearMissInput, nearMissTitle, nearMissDescription, nearMissList);
	const input = new FakeElement("input");
input.textContent = "Choose a project…";
const title = new FakeElement("div");
title.textContent = "Projects";
const popupProjectButton = new FakeElement("button");
popupProjectButton.textContent = "Project: on-chain ⌘ K";
const list = new FakeElement("div");
list.setAttribute("role", "listbox");
const noProject = new FakeElement("button");
noProject.setAttribute("role", "option");
noProject.textContent = "No Project";
const noProjectRight = new FakeElement("div");
const noProjectCheck = new FakeElement("span");
noProjectCheck.dataset.slot = "project-check";
noProjectRight.appendChild(noProjectCheck);
noProject.appendChild(noProjectRight);
const nativeProject = new FakeElement("button");
nativeProject.setAttribute("role", "option");
nativeProject.className = "group flex w-full gap-3 rounded-xl px-3.5 py-2.5 text-left items-center svelte-native";
nativeProject.textContent = "cloud-project Amp aikins01/cloud-project";
const actions = new FakeElement("div");
actions.textContent = "Actions";
list.appendChild(noProject);
list.appendChild(nativeProject);
list.appendChild(actions);
	picker.append(input, title, popupProjectButton, list);
body.appendChild(picker);
body.appendChild(nearMissPicker);
body.appendChild(message);
	body.appendChild(broadProjectLink);
	body.appendChild(broadMoreActions);
	body.appendChild(headerContainer);
	require(scriptPath);
	const bridge = globalThis.__cliproxyAmpLocalInference;
	const captureViewerIdentity = () => JSON.parse(JSON.stringify({
		userWorkspace: {},
		userFeatures: {},
		user: { id: "viewer-user", username: "aikins01", firstName: "Aikins", profilePictureUrl: "https://workoscdn.com/images/aikins" },
	}));
	captureViewerIdentity();
	globalThis.document.readyState = "complete";
	globalThis.document.dispatchEvent(new FakeEvent("DOMContentLoaded"));
	await new Promise((resolve) => setTimeout(resolve, 25));
	assert(Array.prototype.some === nativeArraySome, "userscript replaced Array.prototype.some");
	assert(nearMissList.querySelectorAll("[data-cliproxy-local-project-item]").length === 0, "near-miss project text container was integrated as a picker");
	const observer = globalThis.__cliproxyAmpLocalInferenceProjectPickerObserver;
	const ignoredBefore = bridge.diagnostics.projectMutationIgnoredCount;
	const flushBefore = bridge.diagnostics.projectMutationFlushCount;
	const messageMutations = [];
	for (let i = 0; i < 40; i += 1) {
		const content = new FakeElement("span");
		message.appendChild(content);
		messageMutations.push({ addedNodes: [content] });
	}
	observer.callback(messageMutations);
	await new Promise((resolve) => setTimeout(resolve, 10));
	assert(bridge.diagnostics.projectMutationCandidateCount >= 40, "message mutation candidates were not recorded");
	assert(bridge.diagnostics.projectMutationIgnoredCount >= ignoredBefore + 40, "message mutations were not ignored");
	assert(bridge.diagnostics.projectMutationFlushCount === flushBefore, "message mutations scheduled project integration");
	const relevantFlushBefore = bridge.diagnostics.projectMutationFlushCount;
	const coalescedBefore = bridge.diagnostics.projectMutationCoalescedCount;
	observer.callback([{ addedNodes: [picker, popupProjectButton, picker] }]);
	assert(bridge.diagnostics.projectMutationPendingRootCount === 1, "coalesced project root count was not bounded");
	flushAnimationFrames();
	await new Promise((resolve) => setTimeout(resolve, 10));
	assert(bridge.diagnostics.projectMutationFlushCount === relevantFlushBefore + 1, "relevant project mutations were not flushed once");
	assert(bridge.diagnostics.projectMutationCoalescedCount >= coalescedBefore + 2, "nested project mutations were not coalesced");
	assert(bridge.diagnostics.projectMutationPendingRootCount === 0, "flushed project roots were retained");
	const incrementalPicker = new FakeElement("div");
	incrementalPicker.setAttribute("role", "dialog");
	body.appendChild(incrementalPicker);
	observer.callback([{ addedNodes: [incrementalPicker] }]);
	flushAnimationFrames();
	const incrementalInput = new FakeElement("input");
	incrementalInput.textContent = "Choose a project…";
	const incrementalTitle = new FakeElement("div");
	incrementalTitle.textContent = "Projects";
	const incrementalList = new FakeElement("div");
	incrementalList.setAttribute("role", "listbox");
	const incrementalNoProject = new FakeElement("button");
	incrementalNoProject.setAttribute("role", "option");
	incrementalNoProject.textContent = "No Project";
	incrementalList.appendChild(incrementalNoProject);
	incrementalPicker.append(incrementalInput, incrementalTitle, incrementalList);
	observer.callback([{ addedNodes: [incrementalInput, incrementalTitle, incrementalList] }]);
	flushAnimationFrames();
	await new Promise((resolve) => setTimeout(resolve, 25));
	assert(incrementalList.querySelectorAll("[data-cliproxy-local-project-item]").length === 4, "incrementally mounted project picker was not integrated");
	incrementalPicker.remove();
	globalThis.sessionStorage.setItem("cliproxyapi.ampLocalInference.selectedLocalProject", JSON.stringify({
		name: "on-chain",
		workingDirectory: "/Users/aikins01/Developer/on-chain",
		selectedAt: Date.now(),
	}));
	popupProjectButton.textContent = "Project: telemetry.dev";
	lastFetchURL = "";
	await globalThis.fetch("https://ampcode.com/_app/remote/3abror/createProjectThread", {
		method: "POST",
		headers: { "Content-Type": "application/json" },
		body: JSON.stringify({ payload: encodeDevalue([{ projectID: 1 }, "75616c3b-f4de-48b7-8b83-c1af6978a034"]) }),
	});
	const createURL = new URL(lastFetchURL);
	assert(createURL.origin === "http://127.0.0.1:8317", "create-thread request was not rewritten locally: " + lastFetchURL);
	assert(!createURL.searchParams.has("cliproxy-working-directory"), "cloud project ID was matched to a same-named local project: " + lastFetchURL);
	headerProjectLink.textContent = "missing";
	headerProjectLink.setAttribute("href", "https://github.com/example/missing/tree/main");
	popupProjectButton.textContent = "Project: cloud-only";
	lastFetchURL = "";
	await globalThis.fetch("https://ampcode.com/_app/remote/3abror/createProjectThread", {
		method: "POST",
		headers: { "Content-Type": "application/json" },
		body: JSON.stringify({ payload: encodeDevalue([{ projectID: 1 }, "75616c3b-f4de-48b7-8b83-c1af6978a034"]) }),
	});
	const unresolvedProjectURL = new URL(lastFetchURL);
	assert(!unresolvedProjectURL.searchParams.has("cliproxy-working-directory"), "unresolved projectID fell through to stale selected project: " + lastFetchURL);
	headerProjectLink.textContent = "telemetry.dev";
	headerProjectLink.setAttribute("href", "https://github.com/telemetry-dev/telemetry.dev/tree/6bcbce6911b604cfc8dd8255bc9215a930222dfb");
	popupProjectButton.textContent = "Project: telemetry.dev";
	await new Promise((resolve) => setTimeout(resolve, 25));
const localItems = list.querySelectorAll("[data-cliproxy-local-project-item]");
assert(localItems.length === 4, "expected four local project items, got " + localItems.length);
assert(localItems.every((item) => item.className === nativeProject.className), "local project items did not inherit the native project row frame");
assert(localItems.every((item) => item.style.minHeight === "44px" && item.style.padding === "10px 14px"), "local project item geometry did not match native rows");
assert(localItems.every((item) => item.firstElementChild?.firstElementChild?.style?.cssText?.includes("font-size:16px")), "local project titles did not match native row typography");
assert(localItems.every((item) => item.getAttribute("aria-label").includes(", Local, ")), "local project accessible labels did not include their source");
assert(localItems.some((item) => item.getAttribute("aria-label").includes("~/Developer/")), "local project accessible labels did not include displayed paths");
assert(new Set(localItems.map((item) => item.getAttribute("data-value"))).size === localItems.length, "local project values were not unique by displayed path");
assert(localItems.every((item) => item.innerText.includes("Local")), "local project rows did not identify their local source");
assert(localItems.every((item) => !item.hidden && item.style.display !== "none"), "empty project search treated placeholder text as a query");
const selected = localItems.find((item) => item.getAttribute("aria-selected") === "true" || item.dataset.selected === "true");
assert(selected, "no injected local project item was selected");
assert(selected.dataset.cliproxyLocalProjectWorkingDirectory === "/Users/aikins01/Developer/telemetry.dev", "visible project was not selected: " + selected.innerText);
assert(selected.dataset.cliproxyLocalProjectCurrent === "1", "visible project item was not marked current");
assert(popupProjectButton.textContent.includes("Project: telemetry.dev"), "popup project button stayed stale: " + popupProjectButton.textContent);
const unrelatedProjectButton = new FakeElement("button");
unrelatedProjectButton.textContent = "Project: unrelated";
body.appendChild(unrelatedProjectButton);
noProject.addEventListener("click", () => { popupProjectButton.textContent = "No Project"; });
noProject.dispatchEvent(new MouseEvent("pointerdown", { bubbles: true, cancelable: true }));
noProject.dispatchEvent(new MouseEvent("click", { bubbles: true, cancelable: true }));
await new Promise((resolve) => setTimeout(resolve, 75));
const homeSelection = JSON.parse(globalThis.sessionStorage.getItem("cliproxyapi.ampLocalInference.selectedLocalProject"));
assert(homeSelection.name === "No Project" && homeSelection.workingDirectory === "/Users/aikins01", "No Project did not select home: " + JSON.stringify(homeSelection));
assert(noProject.getAttribute("aria-selected") === "true" && noProject.dataset.selected === "true", "No Project was not the selected picker item");
assert(localItems.every((item) => item.dataset.cliproxyLocalProjectCurrent === "0" && !(item.querySelector('[data-slot="project-check"]')?.textContent || "")), "No Project left a local project check selected");
assert(noProject.querySelector("[data-cliproxy-no-project-path]")?.textContent === "~", "No Project did not show the home path detail");
assert(popupProjectButton.textContent.includes("No Project") && !popupProjectButton.textContent.includes("~"), "No Project did not preserve its visible label: " + popupProjectButton.textContent);
assert(unrelatedProjectButton.textContent === "Project: unrelated", "project refresh rewrote unrelated control: " + unrelatedProjectButton.textContent);
unrelatedProjectButton.remove();
lastFetchURL = "";
await globalThis.fetch("https://ampcode.com/_app/remote/3abror/createProjectThread", {
	method: "POST",
	headers: { "Content-Type": "application/json" },
	body: JSON.stringify({ payload: "" }),
});
const homeCreateURL = new URL(lastFetchURL);
assert(homeCreateURL.searchParams.get("cliproxy-working-directory") === "/Users/aikins01", "No Project create did not use home: " + lastFetchURL);
lastFetchURL = "";
await globalThis.fetch("https://ampcode.com/_app/remote/3abror/createProjectThread", {
	method: "POST",
	headers: { "Content-Type": "application/json" },
	body: JSON.stringify({ payload: encodeDevalue([{ projectID: 1 }, "75616c3b-f4de-48b7-8b83-c1af6978a034"]) }),
});
const homeProjectIDCreateURL = new URL(lastFetchURL);
assert(homeProjectIDCreateURL.searchParams.get("cliproxy-working-directory") === "/Users/aikins01", "No Project create with projectID did not use home: " + lastFetchURL);
lastFetchURL = "";
await globalThis.fetch("https://ampcode.com/_app/remote/3abror/createProjectThread", {
	method: "POST",
	headers: { "Content-Type": "application/json" },
	body: JSON.stringify({ payload: encodeDevalue([{ projectID: 1 }, "on-chain-local"]) }),
});
const explicitLocalProjectIDCreateURL = new URL(lastFetchURL);
assert(explicitLocalProjectIDCreateURL.searchParams.get("cliproxy-working-directory") === "/Users/aikins01/Developer/on-chain", "No Project state overrode an explicit local project ID: " + lastFetchURL);
globalThis.sessionStorage.removeItem("cliproxyapi.ampLocalInference.selectedLocalProject");
globalThis.location = new URL("https://ampcode.com/projects");
	const onChainProjectRow = new FakeElement("div");
	const appMain = new FakeElement("main");
	appMain.setAttribute("data-slot", "sidebar-inset");
	body.appendChild(appMain);
	const onChainProjectPrimary = new FakeElement("div");
	const onChainProjectTitle = new FakeElement("div");
	const onChainProjectLink = new FakeElement("a");
	onChainProjectLink.setAttribute("href", "/@aikins01/on-chain");
	onChainProjectLink.textContent = "on-chain";
	const onChainCloudSettings = new FakeElement("a");
	onChainCloudSettings.setAttribute("href", "/@aikins01/on-chain/settings");
	onChainCloudSettings.setAttribute("aria-label", "Project Settings on-chain");
	onChainProjectTitle.appendChild(onChainProjectLink);
	onChainProjectPrimary.appendChild(onChainProjectTitle);
	onChainProjectRow.append(onChainProjectPrimary, onChainCloudSettings);
	body.appendChild(onChainProjectRow);
	const telemetryProjectRow = new FakeElement("div");
	const telemetryProjectPrimary = new FakeElement("div");
	const telemetryProjectTitle = new FakeElement("div");
	const telemetryProjectLink = new FakeElement("a");
	telemetryProjectLink.setAttribute("href", "/@telemetry-dev/telemetry.dev");
	telemetryProjectLink.textContent = "telemetry.dev";
	const telemetryProjectSettings = new FakeElement("a");
	telemetryProjectSettings.setAttribute("href", "/@telemetry-dev/telemetry.dev/settings");
	telemetryProjectSettings.setAttribute("aria-label", "Project Settings telemetry.dev");
	telemetryProjectTitle.appendChild(telemetryProjectLink);
	telemetryProjectPrimary.appendChild(telemetryProjectTitle);
	telemetryProjectRow.append(telemetryProjectPrimary, telemetryProjectSettings);
	body.appendChild(telemetryProjectRow);
	const conflictingProjectListResponse = new Response(JSON.stringify({
		type: "result",
		data: JSON.stringify([
			{ projects: 1 },
			[2],
			{ id: 3, name: 4, namespace: 5, repositoryURL: 6 },
			"cloud-on-chain",
			"on-chain",
			"Vela-Engineering",
			"https://github.com/example/not-on-chain.git",
		]),
	}), { status: 200, headers: { "Content-Type": "application/json" } });
	Object.defineProperty(conflictingProjectListResponse, "url", { value: "https://ampcode.com/_app/remote/3abror/listProjects" });
	const conflictingProjectList = await conflictingProjectListResponse.json();
	const conflictingProjectValues = JSON.parse(conflictingProjectList.data);
	const conflictingProjectRefs = conflictingProjectValues[conflictingProjectValues[0].projects];
	const conflictingOnChainRefs = conflictingProjectRefs.filter((ref) => {
			const project = conflictingProjectValues[ref];
			return conflictingProjectValues[project.name] === "on-chain" && conflictingProjectValues[project.namespace] === "Vela-Engineering";
		});
		assert(conflictingOnChainRefs.length === 1, "same-path cloud and local projects produced indistinguishable duplicate routes");
		assert(!Object.hasOwn(conflictingProjectValues[conflictingOnChainRefs[0]], "cliProxyAPILocalProject"), "same-path local project replaced the addressable cloud project");
	const projectListResponse = new Response(JSON.stringify({
		type: "result",
		data: JSON.stringify([
			{ projects: 1 },
			[2],
			{ id: 3, name: 4, namespace: 5, repositoryURL: 6 },
			"75616c3b-f4de-48b7-8b83-c1af6978a034",
			"on-chain",
			"aikins01",
			"https://github.com/Vela-Engineering/on-chain.git",
		]),
	}), { status: 200, headers: { "Content-Type": "application/json" } });
	Object.defineProperty(projectListResponse, "url", { value: "https://ampcode.com/_app/remote/3abror/listProjects" });
	const mergedProjectList = await projectListResponse.json();
	const mergedProjectListValues = JSON.parse(mergedProjectList.data);
	const mergedOnChain = mergedProjectListValues[mergedProjectListValues[0].projects].find((ref) => mergedProjectListValues[mergedProjectListValues[ref].name] === "on-chain");
	const mergedOnChainProject = mergedProjectListValues[mergedOnChain];
	assert(mergedProjectListValues[mergedOnChainProject.cliProxyAPILocalCheckout] === true, "hybrid project response was not marked as a local checkout");
	flushAnimationFrames();
	const onChainScopeText = onChainProjectRow.querySelectorAll("[data-cliproxy-project-scope-decoration]").map((element) => element.textContent).join(" | ");
	assert(onChainScopeText.includes("Amp Cloud"), "hybrid project did not show its Amp Cloud scope: " + onChainScopeText);
	assert(onChainScopeText.includes("Local checkout"), "hybrid project did not show its local checkout scope: " + onChainScopeText);
	assert(onChainScopeText.includes("Local settings"), "hybrid project did not expose separate local settings: " + onChainScopeText);
	assert(onChainScopeText.includes("~/Developer/on-chain"), "hybrid project did not show its local path: " + onChainScopeText);
	assert(onChainCloudSettings.getAttribute("aria-label") === "Amp Cloud Project Settings on-chain", "native hybrid settings were not identified as Amp Cloud settings");
	assert(onChainProjectLink.getAttribute("data-sveltekit-preload-data") === null, "hybrid cloud project preload was disabled");
	assert(telemetryProjectLink.getAttribute("data-sveltekit-preload-data") === "off", "local project hover still preloads a missing cloud route");
	assert(telemetryProjectSettings.getAttribute("data-sveltekit-preload-data") === "off", "local project settings hover still preloads a missing cloud route");
	const telemetryProjectClick = new FakeEvent("click", { bubbles: true, cancelable: true });
	telemetryProjectLink.dispatchEvent(telemetryProjectClick);
	await new Promise((resolve) => setTimeout(resolve, 25));
	const localProjectPage = appMain.querySelector("[data-cliproxy-local-project-page]");
	assert(telemetryProjectClick.defaultPrevented, "local project click reached the missing cloud route");
	assert(globalThis.location.pathname === "/@telemetry-dev/telemetry.dev", "local project page URL was not activated: " + globalThis.location.href);
	assert(localProjectPage?.innerText.includes("Commits to main"), "local project page did not render commits: " + localProjectPage?.innerText);
	assert(localProjectPage?.innerText.includes("Detached at 01234567"), "local project page mislabeled detached HEAD: " + localProjectPage?.innerText);
	assert(localProjectPage?.innerText.includes("README.md") && localProjectPage?.innerText.includes("Recent Threads"), "local project page did not render files and threads: " + localProjectPage?.innerText);
	assert(localProjectPage?.innerText.includes("Files (first 2,000 tracked)"), "local project page hid the tracked-file limit: " + localProjectPage?.innerText);
	assert(localProjectPage?.innerText.includes("aikins01") && localProjectPage?.querySelector("img")?.src === "https://workoscdn.com/images/aikins", "local project threads did not reuse the authenticated Amp profile: " + localProjectPage?.innerText);
	const localProjectDetailsURL = new URL(lastFetchURL);
	assert(!localProjectDetailsURL.searchParams.has("projectID"), "ID-less local project gained a fabricated project identity");
	assert(localProjectDetailsURL.searchParams.get("repository") === "https://github.com/telemetry-dev/telemetry.dev.git", "local project details request omitted its repository identity");
	assert(localProjectDetailsURL.searchParams.get("workingDirectory") === "/Users/aikins01/Developer/telemetry.dev", "local project details request omitted its checkout identity");
	assert(localProjectDetailsURL.searchParams.get("includeThreads") === "1", "local project details request omitted threads");
	assert(localProjectDetailsFetchCount === 1, "local project page repeated full repository inspection: " + String(localProjectDetailsFetchCount));
	globalThis.history.pushState({}, "", "/threads/T-019f324b-2802-7868-b1b1-5f0fa3e87ea5");
	await Promise.resolve();
	assert(!appMain.querySelector("[data-cliproxy-local-project-page]"), "project overlay remained mounted after pushState navigation");
globalThis.location = new URL("https://ampcode.com/projects");
headerContainer.remove();
popupProjectButton.textContent = "Project: No Project";
delete popupProjectButton.dataset.cliproxyLocalProjectActivator;
delete popupProjectButton.dataset.cliproxyLocalProjectLabel;
delete popupProjectButton.dataset.cliproxyLocalProjectWorkingDirectory;
delete list.dataset.cliproxyCurrentProjectAutoSelected;
noProject.setAttribute("aria-selected", "false");
noProject.dataset.selected = "false";
nativeProject.setAttribute("aria-selected", "true");
nativeProject.dataset.selected = "true";
observer.callback([{ addedNodes: [popupProjectButton] }]);
flushAnimationFrames();
await new Promise((resolve) => setTimeout(resolve, 25));
assert(popupProjectButton.textContent.includes("No Project") && !popupProjectButton.textContent.includes("aikins01"), "fresh No Project state used the home basename: " + popupProjectButton.textContent);
assert(popupProjectButton.dataset.cliproxyLocalProjectLabel === "No Project", "fresh No Project state did not retain its semantic label");
assert(noProject.getAttribute("aria-selected") === "true" && noProject.dataset.selected === "true", "fresh No Project state did not select the No Project row");
assert(nativeProject.getAttribute("aria-selected") === "false" && nativeProject.dataset.selected === "false", "fresh No Project state left the cloud project selected");
body.appendChild(headerContainer);
globalThis.sessionStorage.setItem("cliproxyapi.ampLocalInference.selectedLocalProject", JSON.stringify({
	name: "telemetry.dev",
	workingDirectory: "/Users/aikins01/Developer/telemetry.dev",
	selectedAt: Date.now(),
}));
globalThis.localStorage.setItem("cliproxyapi.ampLocalInference.workingDirectory", "/Users/aikins01/Developer/telemetry.dev");
popupProjectButton.textContent = "Project: telemetry.dev";
input.value = "Developer/telemetry.dev";
input.dispatchEvent(new FakeEvent("input", { bubbles: true }));
await new Promise((resolve) => setTimeout(resolve, 25));
const filteredItems = localItems.filter((item) => !item.hidden && item.style.display !== "none");
assert(filteredItems.length === 1, "local project search did not filter to one item: " + filteredItems.map((item) => item.innerText).join(" | "));
assert(filteredItems[0].dataset.cliproxyLocalProjectWorkingDirectory === "/Users/aikins01/Developer/telemetry.dev", "local project search kept wrong duplicate");
assert(filteredItems[0].dataset.selected === "true", "local project search did not select its visible match");
input.value = "~/Developer/telemetry.dev";
input.dispatchEvent(new FakeEvent("input", { bubbles: true }));
await new Promise((resolve) => setTimeout(resolve, 25));
const displayedPathFilteredItems = localItems.filter((item) => !item.hidden && item.style.display !== "none");
assert(displayedPathFilteredItems.length === 1 && displayedPathFilteredItems[0].dataset.cliproxyLocalProjectWorkingDirectory === "/Users/aikins01/Developer/telemetry.dev", "displayed tilde path did not find the local project");
input.value = "";
input.dispatchEvent(new FakeEvent("input", { bubbles: true }));
await new Promise((resolve) => setTimeout(resolve, 25));
assert(localItems.every((item) => !item.hidden && item.style.display !== "none"), "clearing local project search did not restore all items");
popupProjectButton.textContent = "Project: on-chain";
delete popupProjectButton.dataset.cliproxyLocalProjectActivator;
delete popupProjectButton.dataset.cliproxyLocalProjectLabel;
delete popupProjectButton.dataset.cliproxyLocalProjectWorkingDirectory;
	const hiddenCreateDialog = new FakeElement("div");
	hiddenCreateDialog.setAttribute("role", "dialog");
	const hiddenProjectButton = new FakeElement("button");
	hiddenProjectButton.textContent = "Project: stale";
	hiddenProjectButton.hidden = true;
	hiddenCreateDialog.appendChild(hiddenProjectButton);
	body.insertBefore(hiddenCreateDialog, picker);
globalThis.sessionStorage.setItem("cliproxyapi.ampLocalInference.selectedLocalProject", JSON.stringify({
	name: "stale",
	workingDirectory: "/Users/aikins01/Developer/stale",
	selectedAt: Date.now(),
}));
lastFetchURL = "";
await globalThis.fetch("https://ampcode.com/_app/remote/3abror/createProjectThread", {
	method: "POST",
	headers: { "Content-Type": "application/json" },
	body: JSON.stringify({ payload: "" }),
});
const nativeDialogSelectedURL = new URL(lastFetchURL);
assert(nativeDialogSelectedURL.searchParams.get("cliproxy-working-directory") === "/Users/aikins01/Developer/on-chain", "native dialog-selected project did not win: " + lastFetchURL);
	hiddenCreateDialog.remove();
lastFetchURL = "";
await globalThis.fetch("https://ampcode.com/_app/remote/3abror/createProjectThread", {
	method: "POST",
	headers: { "Content-Type": "application/json" },
	body: JSON.stringify({ payload: encodeDevalue([{ projectID: 1 }, "75616c3b-f4de-48b7-8b83-c1af6978a034"]) }),
});
const nativeDialogProjectIDURL = new URL(lastFetchURL);
assert(!nativeDialogProjectIDURL.searchParams.has("cliproxy-working-directory"), "cloud project ID was matched to the dialog project by name: " + lastFetchURL);
popupProjectButton.textContent = "Project: cloud-only";
delete popupProjectButton.dataset.cliproxyLocalProjectActivator;
delete popupProjectButton.dataset.cliproxyLocalProjectLabel;
delete popupProjectButton.dataset.cliproxyLocalProjectWorkingDirectory;
lastFetchURL = "";
await globalThis.fetch("https://ampcode.com/_app/remote/3abror/createProjectThread", {
	method: "POST",
	headers: { "Content-Type": "application/json" },
	body: JSON.stringify({ payload: "" }),
});
const unresolvedDialogURL = new URL(lastFetchURL);
assert(!unresolvedDialogURL.searchParams.has("cliproxy-working-directory"), "unresolved dialog project inherited page project: " + lastFetchURL);
popupProjectButton.textContent = "Project: on-chain";
popupProjectButton.dataset.cliproxyLocalProjectActivator = "1";
popupProjectButton.dataset.cliproxyLocalProjectLabel = "on-chain";
popupProjectButton.dataset.cliproxyLocalProjectWorkingDirectory = "/Users/aikins01/Developer/on-chain";
globalThis.sessionStorage.setItem("cliproxyapi.ampLocalInference.selectedLocalProject", JSON.stringify({
	name: "on-chain",
	workingDirectory: "/Users/aikins01/Developer/on-chain",
	selectedAt: Date.now(),
}));
lastFetchURL = "";
await globalThis.fetch("https://ampcode.com/_app/remote/3abror/createProjectThread", {
	method: "POST",
	headers: { "Content-Type": "application/json" },
	body: JSON.stringify({ payload: "" }),
});
const dialogSelectedURL = new URL(lastFetchURL);
assert(dialogSelectedURL.searchParams.get("cliproxy-working-directory") === "/Users/aikins01/Developer/on-chain", "dialog-selected project did not win: " + lastFetchURL);
popupProjectButton.textContent = "Project: telemetry.dev";
popupProjectButton.dataset.cliproxyLocalProjectLabel = "telemetry.dev";
popupProjectButton.dataset.cliproxyLocalProjectWorkingDirectory = "/Users/aikins01/Developer/telemetry.dev";
globalThis.sessionStorage.setItem("cliproxyapi.ampLocalInference.selectedLocalProject", JSON.stringify({
	name: "telemetry.dev",
	workingDirectory: tempTelemetryDirectory,
	selectedAt: Date.now(),
}));
lastFetchURL = "";
await globalThis.fetch("https://ampcode.com/_app/remote/3abror/createProjectThread", {
	method: "POST",
	headers: { "Content-Type": "application/json" },
	body: JSON.stringify({ payload: "" }),
});
const explicitDuplicateURL = new URL(lastFetchURL);
assert(explicitDuplicateURL.searchParams.get("cliproxy-working-directory") === tempTelemetryDirectory, "explicit duplicate project was overridden: " + lastFetchURL);
	picker.dispatchEvent(new KeyboardEvent("keydown", { key: "Enter", bubbles: true, cancelable: true }));
	await new Promise((resolve) => setTimeout(resolve, 25));
	assert(globalThis.localStorage.getItem("cliproxyapi.ampLocalInference.workingDirectory") === "/Users/aikins01/Developer/telemetry.dev", "Enter did not activate the selected visible project");
	headerProjectLink.textContent = "api";
	headerProjectLink.setAttribute("href", "https://github.com/example/foo/tree/main");
	popupProjectButton.textContent = "Project: api";
	popupProjectButton.dataset.cliproxyLocalProjectLabel = "api";
	popupProjectButton.dataset.cliproxyLocalProjectWorkingDirectory = "/Users/aikins01/Developer/bar/api";
	globalThis.sessionStorage.setItem("cliproxyapi.ampLocalInference.selectedLocalProject", JSON.stringify({
		name: "api",
		workingDirectory: "/Users/aikins01/Developer/bar/api",
		selectedAt: Date.now(),
	}));
	globalThis.localStorage.setItem("cliproxyapi.ampLocalInference.workingDirectory", "/Users/aikins01/Developer/bar/api");
	globalThis.localStorage.setItem("cliproxyapi.ampLocalInference.threadWorkingDirectories", JSON.stringify({
		"T-019f324b-2802-7868-b1b1-5f0fa3e87ea6": "/Users/aikins01/Developer/foo/api",
	}));
	lastFetchURL = "";
	await globalThis.fetch("https://ampcode.com/_app/remote/3abror/createProjectThread", {
		method: "POST",
		headers: { "Content-Type": "application/json" },
		body: JSON.stringify({ payload: "" }),
	});
	const collisionURL = new URL(lastFetchURL);
	assert(collisionURL.searchParams.get("cliproxy-working-directory") === "/Users/aikins01/Developer/bar/api", "remembered basename match overrode selected project: " + lastFetchURL);
	headerProjectLink.textContent = "missing";
	headerProjectLink.setAttribute("href", "https://github.com/example/missing/tree/main");
	localProjectsPayload = {
		defaultWorkingDirectory: "/Users/aikins01",
		projects: [
			{ name: "api", workingDirectory: "/Users/aikins01/Developer/foo/api" },
			{ name: "bar service", workingDirectory: "/Users/aikins01/Developer/bar/api" },
		],
	};
	const collisionProjectsPayload = localProjectsPayload;
	for (const item of list.querySelectorAll("[data-cliproxy-local-project-item]")) item.remove();
	delete list.dataset.cliproxyCurrentProjectAutoSelected;
	delete list.dataset.cliproxyLocalProjectLoading;
	delete list.dataset.cliproxyLocalProjectAttempts;
	const cleanupFlushBefore = bridge.diagnostics.projectMutationFlushCount;
	observer.callback([{ addedNodes: [picker] }]);
		assert(bridge.diagnostics.projectMutationPendingRootCount === 1, "project root was not pending before observer cleanup");
		deferLocalProjectsFetch = true;
		localProjectsPayload = {
		defaultWorkingDirectory: "/Users/aikins01",
		projects: [{ name: "stale", workingDirectory: "/Users/aikins01/Developer/stale" }],
		};
		delete require.cache[require.resolve(scriptPath)];
		require(scriptPath);
		captureViewerIdentity();
		assert(observer.disconnected === true, "replaced project observer was not disconnected");
		assert(bridge.diagnostics.projectMutationPendingRootCount === 0, "observer cleanup retained pending project roots");
		flushAnimationFrames();
		assert(bridge.diagnostics.projectMutationFlushCount === cleanupFlushBefore, "stale project observer flushed after cleanup");
		assert(deferredLocalProjectsFetches.length === 1, "replacement integration did not start a deferred project fetch");
		const deferredObserver = mutationObservers.at(-1);
		localProjectsPayload = collisionProjectsPayload;
		delete require.cache[require.resolve(scriptPath)];
		require(scriptPath);
		captureViewerIdentity();
		assert(deferredObserver.disconnected === true, "observer with an in-flight project fetch was not disconnected");
		assert(deferredLocalProjectsFetches.length === 2, "current integration did not start its own project fetch");
		deferredLocalProjectsFetches[0]();
		await new Promise((resolve) => setTimeout(resolve, 10));
		assert(list.querySelectorAll("[data-cliproxy-local-project-item]").length === 0, "stale project fetch mutated the reinstalled picker");
		deferredLocalProjectsFetches[1]();
		deferLocalProjectsFetch = false;
		await new Promise((resolve) => setTimeout(resolve, 25));
	const collisionItems = list.querySelectorAll("[data-cliproxy-local-project-item]");
	const selectedCollision = collisionItems.find((item) => item.getAttribute("aria-selected") === "true" || item.dataset.selected === "true");
	const collisionSummary = collisionItems.map((item) => item.dataset.cliproxyLocalProjectWorkingDirectory + ":" + item.dataset.selected + ":" + item.getAttribute("aria-selected") + ":" + item.dataset.cliproxyLocalProjectCurrent).join("|");
	assert(selectedCollision, "no collision local project item was selected: " + collisionSummary);
	assert(selectedCollision.dataset.cliproxyLocalProjectWorkingDirectory === "/Users/aikins01/Developer/bar/api", "basename collision selected wrong local project: " + selectedCollision.innerText);
	for (const item of list.querySelectorAll("[data-cliproxy-local-project-item]")) item.remove();
	delete list.dataset.cliproxyLocalProjectLoading;
	delete list.dataset.cliproxyLocalProjectAttempts;
	popupProjectButton.textContent = "Project: on-chain";
	delete popupProjectButton.dataset.cliproxyLocalProjectActivator;
	delete popupProjectButton.dataset.cliproxyLocalProjectLabel;
	delete popupProjectButton.dataset.cliproxyLocalProjectWorkingDirectory;
	globalThis.sessionStorage.setItem("cliproxyapi.ampLocalInference.selectedLocalProject", JSON.stringify({
		name: "No Project",
		workingDirectory: "/Users/aikins01",
		selectedAt: Date.now(),
	}));
	localProjectsPayload = {
		defaultWorkingDirectory: "/Users/aikins01",
		projects: [{ id: "on-chain-local", name: "on-chain", workingDirectory: "/Users/aikins01/Developer/on-chain" }],
	};
	deferLocalProjectsFetch = true;
	delete require.cache[require.resolve(scriptPath)];
	require(scriptPath);
	captureViewerIdentity();
	assert(deferredLocalProjectsFetches.length === 3, "project-close race did not start a deferred project lookup");
	lastFetchURL = "";
	const projectCloseCreatePromise = globalThis.fetch("https://ampcode.com/_app/remote/3abror/createProjectThread", {
		method: "POST",
		headers: { "Content-Type": "application/json" },
		body: JSON.stringify({ payload: encodeDevalue([{ projectID: 1 }, "75616c3b-f4de-48b7-8b83-c1af6978a034"]) }),
	});
	picker.remove();
	deferredLocalProjectsFetches[2]();
	deferLocalProjectsFetch = false;
	await projectCloseCreatePromise;
	const projectCloseCreateURL = new URL(lastFetchURL);
	assert(!projectCloseCreateURL.searchParams.has("cliproxy-working-directory"), "unresolved cloud project ID was matched to a local project by name: " + lastFetchURL);
	body.appendChild(picker);
	for (const item of list.querySelectorAll("[data-cliproxy-local-project-item]")) item.remove();
	delete list.dataset.cliproxyLocalProjectLoading;
	delete list.dataset.cliproxyLocalProjectAttempts;
	deferLocalProjectsFetch = true;
	delete require.cache[require.resolve(scriptPath)];
	require(scriptPath);
	captureViewerIdentity();
	assert(deferredLocalProjectsFetches.length === 4, "empty-body project-close race did not start a deferred project lookup");
	lastFetchURL = "";
	const emptyBodyProjectClosePromise = globalThis.fetch("https://ampcode.com/_app/remote/3abror/createProjectThread", {
		method: "POST",
		headers: { "Content-Type": "application/json" },
		body: JSON.stringify({ payload: "" }),
	});
	picker.remove();
	deferredLocalProjectsFetches[3]();
	deferLocalProjectsFetch = false;
	await emptyBodyProjectClosePromise;
	const emptyBodyProjectCloseURL = new URL(lastFetchURL);
	assert(emptyBodyProjectCloseURL.searchParams.get("cliproxy-working-directory") === "/Users/aikins01/Developer/on-chain", "empty create body lost the project selected before popup close: " + lastFetchURL);
	body.appendChild(picker);
	for (const item of list.querySelectorAll("[data-cliproxy-local-project-item]")) item.remove();
	delete list.dataset.cliproxyLocalProjectLoading;
	delete list.dataset.cliproxyLocalProjectAttempts;
	deferLocalProjectsFetch = true;
	delete require.cache[require.resolve(scriptPath)];
	require(scriptPath);
	captureViewerIdentity();
	assert(deferredLocalProjectsFetches.length === 5, "Request-body project-close race did not start a deferred project lookup");
	lastFetchURL = "";
	const requestBodyProjectClosePromise = globalThis.fetch(new Request("https://ampcode.com/_app/remote/3abror/createProjectThread", {
		method: "POST",
		headers: { "Content-Type": "application/json" },
		body: JSON.stringify({ payload: encodeDevalue([{ projectID: 1 }, "on-chain-local"]) }),
	}));
	picker.remove();
	deferredLocalProjectsFetches[4]();
	deferLocalProjectsFetch = false;
	await requestBodyProjectClosePromise;
	const requestBodyProjectCloseURL = new URL(lastFetchURL);
	assert(requestBodyProjectCloseURL.searchParams.get("cliproxy-working-directory") === "/Users/aikins01/Developer/on-chain", "Request create body lost the project selected before popup close: " + lastFetchURL);
	body.appendChild(picker);
	for (const item of list.querySelectorAll("[data-cliproxy-local-project-item]")) item.remove();
		delete list.dataset.cliproxyLocalProjectLoading;
		delete list.dataset.cliproxyLocalProjectAttempts;
		localProjectsPayload = {
			defaultWorkingDirectory: "/Users/aikins01",
			projects: [
				{ id: "75616c3b-f4de-48b7-8b83-c1af6978a034", name: "on-chain", namespace: "Vela-Engineering", repositoryURL: "https://github.com/Vela-Engineering/on-chain.git", workingDirectory: "/Users/aikins01/Developer/vela/on-chain" },
				{ id: "75616c3b-f4de-48b7-8b83-c1af6978a034", name: "on-chain", namespace: "Vela-Engineering", repositoryURL: "https://github.com/Vela-Engineering/on-chain.git", workingDirectory: "/Users/aikins01/Developer/worktrees/on-chain-feature" },
				{ id: "75616c3b-f4de-48b7-8b83-c1af6978a034", name: "open-codex-computer-use", namespace: "iFurySt", repositoryURL: "https://github.com/iFurySt/open-codex-computer-use.git", workingDirectory: "/Users/aikins01/Developer/open-codex-computer-use" },
					{ name: "idless-local", namespace: "local", workingDirectory: "/Users/aikins01/Developer/idless-local" },
			],
		};
		delete require.cache[require.resolve(scriptPath)];
		require(scriptPath);
		captureViewerIdentity();
		await new Promise((resolve) => setTimeout(resolve, 25));
		globalThis.location = new URL("https://ampcode.com/projects");
		const collidingProjectListResponse = new Response(JSON.stringify({
			type: "result",
			data: JSON.stringify([
				{ projects: 1 },
				[2],
				{ id: 3, name: 4, namespace: 5, repositoryURL: 6 },
				"75616c3b-f4de-48b7-8b83-c1af6978a034",
				"on-chain",
				"aikins01",
				"https://github.com/Vela-Engineering/on-chain.git",
			]),
		}), { status: 200, headers: { "Content-Type": "application/json" } });
		Object.defineProperty(collidingProjectListResponse, "url", { value: "https://ampcode.com/_app/remote/3abror/listProjects" });
		const collidingProjectList = await collidingProjectListResponse.json();
		const collidingProjectValues = JSON.parse(collidingProjectList.data);
		const collidingProjectRefs = collidingProjectValues[collidingProjectValues[0].projects];
		const collidingProjectRecords = collidingProjectRefs.map((ref) => collidingProjectValues[ref]);
		const collidingProjectNames = collidingProjectRecords.map((project) => collidingProjectValues[project.name]);
		const collidingProjectIDs = collidingProjectRecords.map((project) => collidingProjectValues[project.id]);
		assert(collidingProjectNames.includes("on-chain"), "cloud project disappeared during colliding local merge");
		assert(collidingProjectNames.includes("open-codex-computer-use"), "different repository was swallowed by a colliding cloud project id");
		assert(new Set(collidingProjectIDs).size === collidingProjectIDs.length, "colliding cloud and local projects retained duplicate render keys");
		const localOnlyCollidingProjectList = {
			type: "result",
			data: JSON.stringify([{ projects: 1 }, []]),
		};
		globalThis.__cliproxyAmpLocalInferenceProjectListTest.mergeProjectListResponse(localOnlyCollidingProjectList, {
			url: "https://ampcode.com/_app/remote/3abror/listProjects",
		});
		const localOnlyCollidingValues = JSON.parse(localOnlyCollidingProjectList.data);
		const localOnlyCollidingRefs = localOnlyCollidingValues[localOnlyCollidingValues[0].projects];
		const localOnlyCollidingIDs = localOnlyCollidingRefs.map((ref) => localOnlyCollidingValues[localOnlyCollidingValues[ref].id]);
		assert(localOnlyCollidingRefs.length === 4, "distinct local project checkouts sharing a path or ID were collapsed: " + JSON.stringify(localOnlyCollidingIDs));
		assert(new Set(localOnlyCollidingIDs).size === 4 && localOnlyCollidingIDs.every(Boolean), "distinct local project checkouts retained missing or duplicate render keys");
		localProjectsPayload = null;
		delete require.cache[require.resolve(scriptPath)];
		require(scriptPath);
		captureViewerIdentity();
		const malformedProjectsBridge = globalThis.__cliproxyAmpLocalInference;
		await new Promise((resolve) => setTimeout(resolve, 25));
		assert(malformedProjectsBridge.diagnostics.localProjectFetchFailureCount === 1, "malformed project response failure was not recorded");
		assert(malformedProjectsBridge.diagnostics.lastLocalProjectFetchFailure === "invalid_payload", "malformed project response failure status mismatch");
		})().catch((error) => {
	console.error(error && error.stack ? error.stack : error);
	process.exit(1);
});
`
	runnerPath := filepath.Join(dir, "run-project-picker-test.js")
	if err := os.WriteFile(runnerPath, []byte(runner), 0o600); err != nil {
		t.Fatalf("write runner: %v", err)
	}
	cmd := exec.Command("node", runnerPath)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("userscript project picker check failed: %v\n%s", err, output)
	}
}

func TestWebLocalInferenceUserscriptScopesMenuObserversToAddedSubtrees(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("node not installed")
	}
	dir := t.TempDir()
	scriptPath := filepath.Join(dir, "local-inference.user.js")
	script := ampWebLocalInferenceUserscript("http://127.0.0.1:8317", nil)
	exported := strings.Replace(script, "\tglobalThis.__cliproxyAmpLocalInference = {", "\tglobalThis.__cliproxyAmpLocalInferenceTest = { mutationAddedRoots, threadMenuSelector, commandPaletteSelector };\n\tglobalThis.__cliproxyAmpLocalInference = {", 1)
	if exported == script {
		t.Fatal("userscript missing local inference bridge")
	}
	if err := os.WriteFile(scriptPath, []byte(exported), 0o600); err != nil {
		t.Fatalf("write userscript: %v", err)
	}
	runner := `
(async () => {
const assert = (condition, message) => {
	if (!condition) throw new Error(message);
};
class FakeElement {
	constructor(selector = "") {
		this.selector = selector;
		this.children = [];
		this.parentElement = null;
	}
	appendChild(child) { this.children.push(child); child.parentElement = this; return child; }
	matches(selector) { return this.selector !== "" && selector.includes(this.selector); }
	querySelector(selector) {
		for (const child of this.children) {
			if (child.matches(selector) || child.querySelector(selector)) return child;
		}
		return null;
	}
	contains(node) {
		if (node === this) return true;
		return this.children.some((child) => child.contains(node));
	}
}
globalThis.Element = FakeElement;
globalThis.document = {
	readyState: "complete",
	visibilityState: "visible",
	documentElement: new FakeElement(),
	body: new FakeElement(),
	head: new FakeElement(),
	createElement() { return new FakeElement(); },
	createTextNode() { return new FakeElement(); },
	createTreeWalker() { return { currentNode: null, nextNode() { return null; } }; },
	getElementById() { return null; },
	querySelector() { return null; },
	querySelectorAll() { return []; },
	contains() { return false; },
	addEventListener() {},
	removeEventListener() {},
	dispatchEvent() { return true; },
};
globalThis.window = globalThis;
globalThis.location = { href: "https://ampcode.com/", origin: "https://ampcode.com", pathname: "/" };
globalThis.localStorage = { getItem() { return null; }, setItem() {}, removeItem() {} };
globalThis.sessionStorage = globalThis.localStorage;
globalThis.MutationObserver = class { observe() {} disconnect() {} };
globalThis.requestAnimationFrame = () => 1;
globalThis.cancelAnimationFrame = () => {};
globalThis.WebSocket = class { constructor() {} send() {} close() {} addEventListener() {} };
globalThis.fetch = async () => ({ ok: false, status: 404, json: async () => ({}), text: async () => "" });
await import(` + strconv.Quote("file://"+scriptPath) + `);
const bridge = globalThis.__cliproxyAmpLocalInferenceTest;
assert(bridge && typeof bridge.mutationAddedRoots === "function", "test bridge was not exposed");
const menu = bridge.threadMenuSelector;

const unrelated = new FakeElement();
unrelated.appendChild(new FakeElement());
const noise = bridge.mutationAddedRoots([{ addedNodes: [unrelated] }], menu);
assert(Array.isArray(noise) && noise.length === 0, "unrelated DOM churn should schedule no menu work, got " + JSON.stringify(noise.length));

const menuNode = new FakeElement('[role="menu"]');
assert(bridge.mutationAddedRoots([{ addedNodes: [menuNode] }], menu)[0] === menuNode, "a directly added menu should be returned as a root");

const wrapper = new FakeElement();
wrapper.appendChild(new FakeElement('[data-radix-menu-content]'));
assert(bridge.mutationAddedRoots([{ addedNodes: [wrapper] }], menu)[0] === wrapper, "a wrapper containing a menu should be returned as a root");

const nested = bridge.mutationAddedRoots([{ addedNodes: [wrapper, wrapper.children[0]] }], menu);
assert(nested.length === 1 && nested[0] === wrapper, "nested roots should coalesce to the outermost, got " + nested.length);

assert(bridge.mutationAddedRoots(undefined, menu) === null, "missing mutation records should fall back to a full scan");

const palette = new FakeElement("[cmdk-root]");
const paletteRoots = bridge.mutationAddedRoots([{ addedNodes: [palette, unrelated] }], bridge.commandPaletteSelector);
assert(paletteRoots.length === 1 && paletteRoots[0] === palette, "command palette filtering should keep only palette roots");

console.log("OK");
})().catch((error) => { console.error(error); process.exit(1); });
`
	runnerPath := filepath.Join(dir, "runner.mjs")
	if err := os.WriteFile(runnerPath, []byte(runner), 0o600); err != nil {
		t.Fatalf("write runner: %v", err)
	}
	output, err := exec.Command("node", runnerPath).CombinedOutput()
	if err != nil {
		t.Fatalf("observer scoping check failed: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), "OK") {
		t.Fatalf("observer scoping check did not pass:\n%s", output)
	}
}

func TestWebLocalInferenceUserscriptRouteRequiresOptIn(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	m := &AmpModule{restrictToLocalhost: false}
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	req := httptest.NewRequest(http.MethodGet, "/ampcode/local-inference.user.js", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("userscript status = %d, want %d", rec.Code, http.StatusNotFound)
	}

	headReq := httptest.NewRequest(http.MethodHead, "/ampcode/local-inference.user.js", nil)
	headRec := httptest.NewRecorder()
	r.ServeHTTP(headRec, headReq)

	if headRec.Code != http.StatusNotFound {
		t.Fatalf("userscript HEAD status = %d, want %d", headRec.Code, http.StatusNotFound)
	}
}

func TestWebLocalInferenceGatewayWebSocketUsesQueryAPIKey(t *testing.T) {
	gin.SetMode(gin.TestMode)

	var sawInternalClientKey string
	var sawQueryAPIKey bool
	runtimeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawInternalClientKey = r.Header.Get(neoInternalClientAPIKeyHeader)
		sawQueryAPIKey = r.URL.Query().Get(ampWebLocalInferenceAPIKeyQuery) != ""
		writeNeoJSON(w, http.StatusOK, map[string]any{"ok": true})
	}))
	defer runtimeServer.Close()

	parsed, err := url.Parse(runtimeServer.URL)
	if err != nil {
		t.Fatalf("parse runtime URL: %v", err)
	}
	host, portText, err := net.SplitHostPort(parsed.Host)
	if err != nil {
		t.Fatalf("split runtime host: %v", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("parse runtime port: %v", err)
	}

	r := gin.New()
	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime:          &neoRuntime{host: host, port: port},
		lastConfig: &config.AmpCode{
			WebLocalInference: config.AmpWebLocalInference{
				Enabled:        true,
				AllowedOrigins: []string{"https://ampcode.com"},
			},
		},
	}
	auth := func(c *gin.Context) {
		token := strings.TrimSpace(c.GetHeader("Authorization"))
		token = strings.TrimSpace(strings.TrimPrefix(token, "Bearer "))
		if token != "local-key" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing auth"})
			return
		}
		c.Set("userApiKey", token)
		c.Next()
	}
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, auth)
	server := httptest.NewServer(r)
	defer server.Close()

	unauthReq, err := http.NewRequest(http.MethodGet, server.URL+"/gateway/threadActor/?rvt-method=get&rvt-key=T-web&"+ampWebLocalInferenceAPIKeyQuery+"=local-key", nil)
	if err != nil {
		t.Fatalf("unauth request build: %v", err)
	}
	unauthResp, err := http.DefaultClient.Do(unauthReq)
	if err != nil {
		t.Fatalf("unauth request: %v", err)
	}
	defer unauthResp.Body.Close()
	if unauthResp.StatusCode != http.StatusUnauthorized {
		body, _ := io.ReadAll(unauthResp.Body)
		t.Fatalf("unauth status = %d, body=%s", unauthResp.StatusCode, body)
	}
	if sawInternalClientKey != "" || sawQueryAPIKey {
		t.Fatal("non-CORS web local inference query API key reached runtime")
	}
	corsInvalidAuthReq, err := http.NewRequest(http.MethodGet, server.URL+"/gateway/threadActor/?rvt-method=get&rvt-key=T-web", nil)
	if err != nil {
		t.Fatalf("CORS invalid auth request build: %v", err)
	}
	corsInvalidAuthReq.Header.Set("Origin", "https://ampcode.com")
	corsInvalidAuthReq.Header.Set("Authorization", "Bearer wrong-key")
	corsInvalidAuthResp, err := http.DefaultClient.Do(corsInvalidAuthReq)
	if err != nil {
		t.Fatalf("CORS invalid auth request: %v", err)
	}
	defer corsInvalidAuthResp.Body.Close()
	if corsInvalidAuthResp.StatusCode != http.StatusUnauthorized {
		body, _ := io.ReadAll(corsInvalidAuthResp.Body)
		t.Fatalf("CORS invalid auth status = %d, body=%s", corsInvalidAuthResp.StatusCode, body)
	}
	if sawInternalClientKey != "" || sawQueryAPIKey {
		t.Fatal("CORS invalid auth request reached runtime")
	}

	req, err := http.NewRequest(http.MethodGet, server.URL+"/gateway/threadActor/?rvt-method=get&rvt-key=T-web&"+ampWebLocalInferenceAPIKeyQuery+"=local-key", nil)
	if err != nil {
		t.Fatalf("request build: %v", err)
	}
	req.Header.Set("Origin", "https://ampcode.com")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body=%s", resp.StatusCode, body)
	}
	if sawInternalClientKey != "local-key" {
		t.Fatalf("%s = %q, want local-key", neoInternalClientAPIKeyHeader, sawInternalClientKey)
	}
	if sawQueryAPIKey {
		t.Fatal("web local inference query API key leaked to runtime")
	}

	sawInternalClientKey = ""
	sawQueryAPIKey = false
	rivetReq, err := http.NewRequest(http.MethodGet, server.URL+"/gateway/threadActor/?rvt-method=get&rvt-key=T-web&rvt-token=local-neo&"+ampWebLocalInferenceAPIKeyQuery+"=local-key", nil)
	if err != nil {
		t.Fatalf("rivet request build: %v", err)
	}
	rivetReq.Header.Set("Origin", "https://ampcode.com")
	rivetResp, err := http.DefaultClient.Do(rivetReq)
	if err != nil {
		t.Fatalf("rivet request: %v", err)
	}
	defer rivetResp.Body.Close()
	if rivetResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(rivetResp.Body)
		t.Fatalf("rivet status = %d, body=%s", rivetResp.StatusCode, body)
	}
	if sawInternalClientKey != "local-key" {
		t.Fatalf("rivet %s = %q, want local-key", neoInternalClientAPIKeyHeader, sawInternalClientKey)
	}
	if sawQueryAPIKey {
		t.Fatal("rivet web local inference query API key leaked to runtime")
	}
}

func TestWebLocalInferenceAPIGroupUsesQueryAPIKey(t *testing.T) {
	gin.SetMode(gin.TestMode)

	var upstreamCalled bool
	var sawQueryAPIKey bool
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalled = true
		sawQueryAPIKey = r.URL.Query().Get(ampWebLocalInferenceAPIKeyQuery) != ""
		writeNeoJSON(w, http.StatusOK, map[string]any{"ok": true})
	}))
	defer upstream.Close()

	proxy, err := createReverseProxy(upstream.URL, NewStaticSecretSource("amp-secret"))
	if err != nil {
		t.Fatalf("create reverse proxy: %v", err)
	}

	r := gin.New()
	m := &AmpModule{
		restrictToLocalhost: false,
		lastConfig: &config.AmpCode{
			WebLocalInference: config.AmpWebLocalInference{
				Enabled:        true,
				AllowedOrigins: []string{"https://ampcode.com"},
			},
		},
	}
	m.setProxy(proxy)
	var sawLocalToken string
	auth := func(c *gin.Context) {
		token := strings.TrimSpace(c.GetHeader("Authorization"))
		token = strings.TrimSpace(strings.TrimPrefix(token, "Bearer "))
		sawLocalToken = token
		if token != "local-key" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing auth"})
			return
		}
		c.Set("userApiKey", token)
		c.Next()
	}
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, auth)
	server := httptest.NewServer(r)
	defer server.Close()

	req, err := http.NewRequest(http.MethodGet, server.URL+"/api/thread-actors?"+ampWebLocalInferenceAPIKeyQuery+"=local-key", nil)
	if err != nil {
		t.Fatalf("request build: %v", err)
	}
	req.Header.Set("Origin", "https://ampcode.com")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body=%s", resp.StatusCode, body)
	}
	if sawLocalToken != "local-key" {
		t.Fatalf("local auth token = %q, want local-key", sawLocalToken)
	}
	if !upstreamCalled {
		t.Fatal("request did not reach upstream after local query auth")
	}
	if sawQueryAPIKey {
		t.Fatal("web local inference query API key leaked to upstream")
	}
}

func TestWebLocalInferenceCanBootstrapRemoteShellThreadLocallyWithProxy(t *testing.T) {
	gin.SetMode(gin.TestMode)

	var upstreamCalled bool
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalled = true
		writeNeoJSON(w, http.StatusNotFound, map[string]any{"error": "unexpected upstream"})
	}))
	defer upstream.Close()

	proxy, err := createReverseProxy(upstream.URL, NewStaticSecretSource("amp-secret"))
	if err != nil {
		t.Fatalf("create reverse proxy: %v", err)
	}

	r := gin.New()
	enabled := true
	threadID := "T-019f03e0-bfa6-7595-9206-d5715ab49f2b"
	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime: newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
			NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled},
		}}),
		lastConfig: &config.AmpCode{
			WebLocalInference: config.AmpWebLocalInference{
				Enabled:        true,
				AllowedOrigins: []string{"https://ampcode.com"},
			},
		},
	}
	m.setProxy(proxy)
	auth := func(c *gin.Context) {
		token := strings.TrimSpace(c.GetHeader("Authorization"))
		token = strings.TrimSpace(strings.TrimPrefix(token, "Bearer "))
		if token != "local-key" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing auth"})
			return
		}
		c.Set("userApiKey", token)
		c.Next()
	}
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, auth)
	server := httptest.NewServer(r)
	defer server.Close()

	noHeaderThreadID := "T-019f03e0-bfa6-7595-9206-d5715ab49f2c"
	noHeaderReq, err := http.NewRequest(http.MethodPost, server.URL+"/api/thread-actors/"+noHeaderThreadID+"?"+ampWebLocalInferenceAPIKeyQuery+"=local-key", bytes.NewBufferString(`{"agentMode":"deep","executorType":"local-client","usesThreadActors":true}`))
	if err != nil {
		t.Fatalf("request build: %v", err)
	}
	noHeaderReq.Header.Set("Content-Type", "application/json")
	noHeaderReq.Header.Set("Origin", "https://ampcode.com")
	noHeaderResp, err := http.DefaultClient.Do(noHeaderReq)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer noHeaderResp.Body.Close()
	if noHeaderResp.StatusCode != http.StatusNotFound {
		body, _ := io.ReadAll(noHeaderResp.Body)
		t.Fatalf("bootstrap without bridge header status = %d, want %d; body=%s", noHeaderResp.StatusCode, http.StatusNotFound, body)
	}
	if actor := m.neoRuntime.store.lookupThreadActor(noHeaderThreadID); actor != nil {
		t.Fatal("local actor was created without bridge header")
	}
	upstreamCalled = false

	noOriginThreadID := "T-019f03e0-bfa6-7595-9206-d5715ab49f2e"
	noOriginReq, err := http.NewRequest(http.MethodPost, server.URL+"/api/thread-actors/"+noOriginThreadID+"?"+ampWebLocalInferenceAPIKeyQuery+"=local-key", bytes.NewBufferString(`{"agentMode":"deep","executorType":"local-client","usesThreadActors":true}`))
	if err != nil {
		t.Fatalf("request build: %v", err)
	}
	noOriginReq.Header.Set("Content-Type", "application/json")
	noOriginReq.Header.Set("Authorization", "Bearer local-key")
	noOriginReq.Header.Set(ampWebLocalInferenceHeader, "1")
	noOriginResp, err := http.DefaultClient.Do(noOriginReq)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer noOriginResp.Body.Close()
	if noOriginResp.StatusCode != http.StatusNotFound {
		body, _ := io.ReadAll(noOriginResp.Body)
		t.Fatalf("bootstrap without trusted Origin status = %d, want %d; body=%s", noOriginResp.StatusCode, http.StatusNotFound, body)
	}
	if actor := m.neoRuntime.store.lookupThreadActor(noOriginThreadID); actor != nil {
		t.Fatal("local actor was created without trusted Origin")
	}
	upstreamCalled = false

	req := httptest.NewRequest(http.MethodPost, "/api/thread-actors/"+threadID+"?"+ampWebLocalInferenceAPIKeyQuery+"=local-key", bytes.NewBufferString(`{"agentMode":"deep","reasoningEffort":"xhigh","executorType":"local-client","usesThreadActors":true,"settings":{"agentMode":"deep","reasoning.effort":"xhigh"}}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", "https://ampcode.com")
	req.Header.Set(ampWebLocalInferenceHeader, "1")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("bootstrap status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if upstreamCalled {
		t.Fatal("web local inference bootstrap was proxied upstream")
	}
	var response map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("response JSON error: %v", err)
	}
	if stringValue(response["threadId"]) != threadID || response["executorType"] != "local-client" || stringValue(response["wsToken"]) == "" || stringValue(response["ownerUserId"]) != neoLocalOwnerUserID {
		t.Fatalf("unexpected bootstrap response: %#v", response)
	}
	actor := m.neoRuntime.store.lookupThreadActor(threadID)
	if actor == nil {
		t.Fatal("local actor was not created")
	}
	actor.mu.Lock()
	defer actor.mu.Unlock()
	if actor.currentAgentMode != "deep" || actor.currentReasoningEffort != "xhigh" {
		t.Fatalf("actor mode/effort = %q/%q, want deep/xhigh", actor.currentAgentMode, actor.currentReasoningEffort)
	}

	invalidBodyThreadID := "T-019f03e0-bfa6-7595-9206-d5715ab49f2f"
	invalidBodyReq := httptest.NewRequest(http.MethodPost, "/api/thread-actors/"+invalidBodyThreadID+"?"+ampWebLocalInferenceAPIKeyQuery+"=local-key", bytes.NewReader([]byte{0x1f, 0x8b, '{', '}'}))
	invalidBodyReq.Header.Set("Content-Type", "application/json")
	invalidBodyReq.Header.Set("Origin", "https://ampcode.com")
	invalidBodyReq.Header.Set(ampWebLocalInferenceHeader, "1")
	invalidBodyRec := httptest.NewRecorder()
	r.ServeHTTP(invalidBodyRec, invalidBodyReq)
	if invalidBodyRec.Code != http.StatusBadRequest {
		t.Fatalf("invalid body bootstrap status = %d, want %d; body=%s", invalidBodyRec.Code, http.StatusBadRequest, invalidBodyRec.Body.String())
	}
	if actor := m.neoRuntime.store.lookupThreadActor(invalidBodyThreadID); actor != nil {
		t.Fatal("invalid body actor was created")
	}

	invalidModeThreadID := "T-019f03e0-bfa6-7595-9206-d5715ab49f2d"
	invalidModeReq := httptest.NewRequest(http.MethodPost, "/api/thread-actors/"+invalidModeThreadID+"?"+ampWebLocalInferenceAPIKeyQuery+"=local-key", bytes.NewBufferString(`{"executorType":"local-client","usesThreadActors":true,"settings":{"agentMode":"bogus","reasoning.effort":"xhigh"}}`))
	invalidModeReq.Header.Set("Content-Type", "application/json")
	invalidModeReq.Header.Set("Origin", "https://ampcode.com")
	invalidModeReq.Header.Set(ampWebLocalInferenceHeader, "1")
	invalidModeRec := httptest.NewRecorder()
	r.ServeHTTP(invalidModeRec, invalidModeReq)
	if invalidModeRec.Code != http.StatusOK {
		t.Fatalf("invalid settings mode bootstrap status = %d, want %d; body=%s", invalidModeRec.Code, http.StatusOK, invalidModeRec.Body.String())
	}
	invalidModeActor := m.neoRuntime.store.lookupThreadActor(invalidModeThreadID)
	if invalidModeActor == nil {
		t.Fatal("invalid settings mode actor was not created")
	}
	invalidModeActor.mu.Lock()
	defer invalidModeActor.mu.Unlock()
	if invalidModeActor.currentAgentMode == "bogus" {
		t.Fatal("invalid settings agentMode was accepted")
	}
	if invalidModeActor.currentReasoningEffort == "xhigh" {
		t.Fatal("reasoning effort from invalid settings agentMode was accepted")
	}
}

func TestWebLocalInferenceRemoteShellBootstrapRequiresNeoRuntime(t *testing.T) {
	gin.SetMode(gin.TestMode)

	var upstreamCalled bool
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalled = true
		writeNeoJSON(w, http.StatusOK, map[string]any{"threadId": "T-upstream"})
	}))
	defer upstream.Close()

	proxy, err := createReverseProxy(upstream.URL, NewStaticSecretSource("amp-secret"))
	if err != nil {
		t.Fatalf("create reverse proxy: %v", err)
	}

	r := gin.New()
	enabled := false
	threadID := "T-019f03e0-bfa6-7595-9206-d5715ab49f2b"
	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime: newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
			NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled},
		}}),
		lastConfig: &config.AmpCode{
			WebLocalInference: config.AmpWebLocalInference{
				Enabled:        true,
				AllowedOrigins: []string{"https://ampcode.com"},
			},
		},
	}
	m.setProxy(proxy)
	auth := func(c *gin.Context) {
		token := strings.TrimSpace(c.GetHeader("Authorization"))
		token = strings.TrimSpace(strings.TrimPrefix(token, "Bearer "))
		if token != "local-key" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing auth"})
			return
		}
		c.Set("userApiKey", token)
		c.Next()
	}
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, auth)
	server := httptest.NewServer(r)
	defer server.Close()

	req, err := http.NewRequest(http.MethodPost, server.URL+"/api/thread-actors/"+threadID+"?"+ampWebLocalInferenceAPIKeyQuery+"=local-key", bytes.NewBufferString(`{"agentMode":"deep","executorType":"local-client","usesThreadActors":true}`))
	if err != nil {
		t.Fatalf("request build: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", "https://ampcode.com")
	req.Header.Set(ampWebLocalInferenceHeader, "1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("bootstrap status = %d, want %d; body=%s", resp.StatusCode, http.StatusOK, body)
	}
	if !upstreamCalled {
		t.Fatal("web local inference bootstrap did not fall through to upstream when neo runtime was disabled")
	}
	if actor := m.neoRuntime.store.lookupThreadActor(threadID); actor != nil {
		t.Fatal("local actor was created while neo runtime was disabled")
	}
}

func TestWebLocalInferenceInternalRPCRequiresAPIKey(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	enabled := true
	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime: newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
			NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled},
		}}),
		lastConfig: &config.AmpCode{
			WebLocalInference: config.AmpWebLocalInference{Enabled: true},
		},
	}
	auth := func(c *gin.Context) {
		token := strings.TrimSpace(c.GetHeader("Authorization"))
		token = strings.TrimSpace(strings.TrimPrefix(token, "Bearer "))
		if token != "local-key" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing auth"})
			return
		}
		c.Set("userApiKey", token)
		c.Next()
	}
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, auth)

	threadID := "T-019e0e6e-f3f1-7078-b5dd-748f66f8c267"
	m.neoRuntime.store.ensureThreadActor(threadID)
	body := `{"method":"getThreadLabels","params":{"thread":"` + threadID + `"}}`

	unauthReq := httptest.NewRequest(http.MethodPost, "/api/internal?getThreadLabels", bytes.NewBufferString(body))
	unauthReq.Header.Set("Content-Type", "application/json")
	unauthReq.Header.Set("Origin", "https://ampcode.com")
	unauthReq.Header.Set(ampWebLocalInferenceHeader, "1")
	unauthRec := httptest.NewRecorder()
	r.ServeHTTP(unauthRec, unauthReq)
	if unauthRec.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated internal RPC status = %d, want %d; body=%s", unauthRec.Code, http.StatusUnauthorized, unauthRec.Body.String())
	}

	authReq := httptest.NewRequest(http.MethodPost, "/api/internal?getThreadLabels&"+ampWebLocalInferenceAPIKeyQuery+"=local-key", bytes.NewBufferString(body))
	authReq.Header.Set("Content-Type", "application/json")
	authReq.Header.Set("Origin", "https://ampcode.com")
	authReq.Header.Set(ampWebLocalInferenceHeader, "1")
	authRec := httptest.NewRecorder()
	r.ServeHTTP(authRec, authReq)
	if authRec.Code != http.StatusOK {
		t.Fatalf("authenticated internal RPC status = %d, want %d; body=%s", authRec.Code, http.StatusOK, authRec.Body.String())
	}
}

func TestInternalRPCRequestsTreatInvalidGzipAsMatchedError(t *testing.T) {
	raw := []byte{0x1f, 0x8b, '{', '}'}
	webReq := httptest.NewRequest(http.MethodPost, "/api/internal?readThread", bytes.NewReader(raw))
	webReq.Header.Set(ampWebLocalInferenceHeader, "1")
	_, _, matched, err := neoWebLocalInternalRPCRequestWithError(webReq)
	if !matched || err == nil || !strings.Contains(err.Error(), "decode gzip JSON body") {
		t.Fatalf("web-local matched=%v err=%v, want matched gzip error", matched, err)
	}
	webRestored, errRead := io.ReadAll(webReq.Body)
	if errRead != nil {
		t.Fatalf("read restored web-local body: %v", errRead)
	}
	if !bytes.Equal(webRestored, raw) {
		t.Fatalf("web-local body was not restored: %v", webRestored)
	}

	localReq := httptest.NewRequest(http.MethodPost, "/api/internal?getThreadLabels", bytes.NewReader(raw))
	_, _, matched, err = neoLocalInternalRPCRequestWithError(localReq)
	if !matched || err == nil || !strings.Contains(err.Error(), "decode gzip JSON body") {
		t.Fatalf("local matched=%v err=%v, want matched gzip error", matched, err)
	}
}

func TestWebLocalInferenceInternalRPCRequiresAcceptedCORS(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name     string
		settings config.AmpWebLocalInference
		origin   string
	}{
		{
			name:     "disabled",
			settings: config.AmpWebLocalInference{Enabled: false},
			origin:   "https://ampcode.com",
		},
		{
			name: "disallowed origin",
			settings: config.AmpWebLocalInference{
				Enabled:        true,
				AllowedOrigins: []string{"https://ampcode.com"},
			},
			origin: "https://example.com",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := gin.New()
			enabled := true
			m := &AmpModule{
				restrictToLocalhost: false,
				neoRuntime: newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
					NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled},
				}}),
				lastConfig: &config.AmpCode{WebLocalInference: tc.settings},
			}
			auth := func(c *gin.Context) {
				token := strings.TrimSpace(c.GetHeader("Authorization"))
				token = strings.TrimSpace(strings.TrimPrefix(token, "Bearer "))
				if token != "local-key" {
					c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing auth"})
					return
				}
				c.Set("userApiKey", token)
				c.Next()
			}
			m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, auth)

			req := httptest.NewRequest(http.MethodPost, "/api/internal?listThreads&"+ampWebLocalInferenceAPIKeyQuery+"=local-key", bytes.NewBufferString(`{"method":"listThreads","params":{"limit":20}}`))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Origin", tc.origin)
			req.Header.Set(ampWebLocalInferenceHeader, "1")
			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, req)
			if rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("internal RPC status = %d, want %d; body=%s", rec.Code, http.StatusServiceUnavailable, rec.Body.String())
			}
		})
	}
}

func TestWebLocalInferenceUserscriptSanitizesBaseURL(t *testing.T) {
	body := ampWebLocalInferenceUserscript("javascript:alert(1)", nil)
	if strings.Contains(body, "javascript:alert") {
		t.Fatalf("userscript used invalid base URL:\n%s", body)
	}
	if !strings.Contains(body, "@updateURL http://127.0.0.1:8317/ampcode/local-inference.user.js") {
		t.Fatalf("userscript did not fall back to loopback base URL:\n%s", body)
	}

	body = ampWebLocalInferenceUserscript("https://proxy.example.test/?token=secret#fragment", nil)
	if strings.Contains(body, "token=secret") || strings.Contains(body, "fragment") {
		t.Fatalf("userscript kept query or fragment in base URL:\n%s", body)
	}
	if !strings.Contains(body, "@updateURL https://proxy.example.test/ampcode/local-inference.user.js") {
		t.Fatalf("userscript did not preserve sanitized HTTPS base URL:\n%s", body)
	}

	body = ampWebLocalInferenceUserscript("https://proxy.example.test/base/", nil)
	if !strings.Contains(body, "@updateURL http://127.0.0.1:8317/ampcode/local-inference.user.js") {
		t.Fatalf("userscript did not reject path-prefixed base URL:\n%s", body)
	}
}

func TestWebLocalInferenceUserscriptMatchesAllowedOrigins(t *testing.T) {
	if matches := ampWebLocalInferenceUserscriptMatches([]string{}); matches != "" {
		t.Fatalf("explicit empty allowed origins generated matches: %q", matches)
	}

	body := ampWebLocalInferenceUserscript("http://127.0.0.1:8317", []string{
		"https://ampcode.example.test",
		"https://AMPCODE.example.test",
		"https://ignored.example.test/path",
	})
	if !strings.Contains(body, "// @match https://ampcode.example.test/*") {
		t.Fatalf("userscript missing configured origin match:\n%s", body)
	}
	if strings.Contains(body, "ignored.example.test") {
		t.Fatalf("userscript included invalid path-prefixed origin:\n%s", body)
	}
	if strings.Count(body, "// @match https://ampcode.example.test/*") != 1 {
		t.Fatalf("userscript did not deduplicate normalized origins:\n%s", body)
	}
	if !strings.Contains(body, "// @match http://127.0.0.1:8317/*") {
		t.Fatalf("userscript missing configured base URL origin match:\n%s", body)
	}
}

func TestWebLocalInferenceUserscriptRouteUsesConfiguredBaseURL(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	m := &AmpModule{restrictToLocalhost: false}
	settings := config.AmpCode{
		WebLocalInference: config.AmpWebLocalInference{
			Enabled: true,
			BaseURL: "https://aikinss-macbook-pro.taila39f5b.ts.net/",
		},
	}
	m.lastConfig = &settings
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	req := httptest.NewRequest(http.MethodGet, "/ampcode/local-inference.user.js", nil)
	req.Host = "127.0.0.1:8317"
	req.Header.Set("X-Forwarded-Proto", "https")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("userscript status = %d, want %d", rec.Code, http.StatusOK)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"https://aikinss-macbook-pro.taila39f5b.ts.net"`) {
		t.Fatalf("userscript missing configured base URL:\n%s", body)
	}
	if !strings.Contains(body, "@updateURL https://aikinss-macbook-pro.taila39f5b.ts.net/ampcode/local-inference.user.js") {
		t.Fatalf("userscript missing configured update URL:\n%s", body)
	}
}

func TestRegisterManagementRoutesProxiesAuditBaselineAmpOwnedMethods(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	enabled := true
	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime: newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
			NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled},
		}}),
	}
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	samples := map[string]string{
		"/api/telemetry":              "/api/telemetry",
		"/api/user-actor-credentials": "/api/user-actor-credentials",
		"/threads":                    "/threads",
		"/threads/runs":               "/threads/runs",
	}
	baseline := ampBinaryParityBaselineForTest(t)
	for _, route := range baseline.Signals.RouteMethods {
		if route.Scope != "amp-owned" {
			continue
		}
		path := samples[route.Name]
		if path == "" {
			t.Fatalf("Amp binary baseline exposes amp-owned route %q without an explicit proxy sample", route.Name)
		}
		for _, method := range route.Methods {
			t.Run(method+" "+route.Name, func(t *testing.T) {
				req := httptest.NewRequest(method, path, bytes.NewBufferString(`{}`))
				req.Header.Set("Content-Type", "application/json")
				rec := httptest.NewRecorder()
				r.ServeHTTP(rec, req)

				if rec.Code == http.StatusNotFound {
					t.Fatalf("%s %s was not registered for Amp-owned upstream proxying", method, path)
				}
				if rec.Code != http.StatusServiceUnavailable {
					t.Fatalf("%s %s status = %d, want 503 from missing upstream proxy; body=%s", method, path, rec.Code, rec.Body.String())
				}
			})
		}
	}
}

func TestRegisterManagementRoutesAmpOwnedRouteCoverageDoesNotServeLocally(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	enabled := true
	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime: newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
			NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled},
		}}),
	}
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	samples := map[string]struct {
		method string
		path   string
	}{
		"/api/telemetry":              {method: http.MethodPost, path: "/api/telemetry"},
		"/api/threads":                {method: http.MethodPost, path: "/api/threads"},
		"/api/threads/":               {method: http.MethodPost, path: "/api/threads/T-local/diff-captures"},
		"/api/threads/find?":          {method: http.MethodGet, path: "/api/threads/find?q=local"},
		"/api/user-actor-credentials": {method: http.MethodPost, path: "/api/user-actor-credentials"},
		"/auth/callback":              {method: http.MethodGet, path: "/auth/callback?code=local"},
		"/auth/cli-login?authToken=":  {method: http.MethodGet, path: "/auth/cli-login?authToken=local"},
		"/docs":                       {method: http.MethodGet, path: "/docs"},
		"/threads":                    {method: http.MethodGet, path: "/threads"},
		"/threads/":                   {method: http.MethodGet, path: "/threads/T-local"},
	}
	baseline := ampBinaryParityBaselineForTest(t)
	checked := 0
	for _, route := range baseline.Signals.RouteCoverage {
		if route.Scope != "amp-owned" {
			continue
		}
		sample, ok := samples[route.Name]
		if !ok {
			t.Fatalf("Amp binary baseline exposes amp-owned route %q without an explicit local ownership sample", route.Name)
		}
		checked++
		t.Run(route.Name, func(t *testing.T) {
			req := httptest.NewRequest(sample.method, sample.path, bytes.NewBufferString(`{}`))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, req)

			if rec.Code == http.StatusNotFound {
				t.Fatalf("%s %s was not registered for Amp-owned upstream proxying", sample.method, sample.path)
			}
			if rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("%s %s status = %d, want 503 from missing upstream proxy; body=%s", sample.method, sample.path, rec.Code, rec.Body.String())
			}
		})
	}
	if checked != len(samples) {
		t.Fatalf("checked %d amp-owned route coverage entries, want %d; update local/upstream ownership samples for the Amp binary baseline", checked, len(samples))
	}
}

func TestRegisterManagementRoutesServesAuditBaselineActorRuntimeRoutesLocally(t *testing.T) {
	gin.SetMode(gin.TestMode)
	runtimeRequests := 0
	runtimeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		runtimeRequests++
		writeNeoJSON(w, http.StatusOK, map[string]any{"source": "runtime", "path": r.URL.Path})
	}))
	defer runtimeServer.Close()
	runtimeURL, err := url.Parse(runtimeServer.URL)
	if err != nil {
		t.Fatalf("parse runtime URL: %v", err)
	}
	host, portText, err := net.SplitHostPort(runtimeURL.Host)
	if err != nil {
		t.Fatalf("split runtime host: %v", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("parse runtime port: %v", err)
	}

	existingThreadID := "T-019e65c0-0310-77a8-b233-4b84d9c0612d"
	enabled := true
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
		NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled, ForceThreadActors: true},
	}})
	rt.host = host
	rt.port = port

	r := gin.New()
	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime:          rt,
	}
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)
	localServer := httptest.NewServer(r)
	defer localServer.Close()

	type routeSample struct {
		method       string
		path         string
		body         string
		expectBridge bool
	}
	samples := map[string][]routeSample{
		"/actors": {
			{method: http.MethodPost, path: "/actors", body: `{}`, expectBridge: true},
			{method: http.MethodPut, path: "/actors", body: `{}`, expectBridge: true},
		},
		"/actors/": {
			{method: http.MethodGet, path: "/actors/A-audit/kv/keys/state", expectBridge: true},
			{method: http.MethodDelete, path: "/actors/A-audit", expectBridge: true},
		},
		"/actors?actor_ids=": {
			{method: http.MethodGet, path: "/actors?actor_ids=A-audit", expectBridge: true},
		},
		"/actors?name=": {
			{method: http.MethodGet, path: "/actors?name=threadActor&key=T-audit", expectBridge: true},
		},
		"/api/thread-actors": {
			{method: http.MethodPost, path: "/api/thread-actors", body: `{"agentMode":"deep","usesThreadActors":true}`},
		},
		"/api/thread-actors/": {
			{method: http.MethodPost, path: "/api/thread-actors/" + existingThreadID, body: `{}`},
		},
		"/gateway/": {
			{method: http.MethodGet, path: "/gateway/threadActor/?rvt-method=get&rvt-key=T-audit", expectBridge: true},
		},
		"/metadata": {
			{method: http.MethodGet, path: "/metadata", expectBridge: true},
		},
	}

	baseline := ampBinaryParityBaselineForTest(t)
	checked := 0
	for _, route := range baseline.Signals.RouteCoverage {
		if route.Scope != "local-runtime" {
			continue
		}
		routeSamples, ok := samples[route.Name]
		if !ok {
			continue
		}
		checked++
		for _, sample := range routeSamples {
			t.Run(sample.method+" "+route.Name, func(t *testing.T) {
				beforeRuntimeRequests := runtimeRequests
				req, err := http.NewRequest(sample.method, localServer.URL+sample.path, strings.NewReader(sample.body))
				if err != nil {
					t.Fatalf("build request: %v", err)
				}
				if sample.body != "" {
					req.Header.Set("Content-Type", "application/json")
				}
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					t.Fatalf("request: %v", err)
				}
				defer resp.Body.Close()
				body, err := io.ReadAll(resp.Body)
				if err != nil {
					t.Fatalf("read response: %v", err)
				}

				if resp.StatusCode < 200 || resp.StatusCode >= 300 {
					t.Fatalf("%s %s status = %d, body=%s", sample.method, sample.path, resp.StatusCode, string(body))
				}
				if sample.expectBridge && runtimeRequests != beforeRuntimeRequests+1 {
					t.Fatalf("%s %s runtimeRequests = %d, want %d", sample.method, sample.path, runtimeRequests, beforeRuntimeRequests+1)
				}
				if !sample.expectBridge && runtimeRequests != beforeRuntimeRequests {
					t.Fatalf("%s %s unexpectedly hit runtime bridge", sample.method, sample.path)
				}
			})
		}
	}
	if checked != len(samples) {
		t.Fatalf("checked %d actor runtime route coverage entries, want %d; update local/upstream ownership samples for the Amp binary baseline", checked, len(samples))
	}
}

func TestRegisterManagementRoutesRemoteWebRoutesDoNotSynthesizeLocalThreadData(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	enabled := true

	dir := t.TempDir()
	oldStoreDir := neoAmpDataDir
	neoAmpDataDir = func() string { return dir }
	t.Cleanup(func() { neoAmpDataDir = oldStoreDir })

	threadID := "T-019e65c0-0310-77a8-b233-4b84d9c06131"
	rawThread := []byte(`{"id":"` + threadID + `","title":"remote web must not read this local thread","agentMode":"deep","messages":[{"messageId":"M-local","role":"user","content":[{"type":"text","text":"remote-web-local-leak-marker"}]}]}`)
	if err := os.WriteFile(filepath.Join(dir, threadID+".json"), rawThread, 0o600); err != nil {
		t.Fatalf("write local thread: %v", err)
	}

	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime: newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
			UpstreamURL:    "https://ampcode.test",
			UpstreamAPIKey: "secret",
			NeoLocalRuntime: config.AmpNeoLocalRuntime{
				Enabled: &enabled,
			},
		}}),
	}
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	samples := map[string]string{
		"/api/internal":  "/api/internal",
		"/api/internal?": "/api/internal?listThreads",
	}
	baseline := ampBinaryParityBaselineForTest(t)
	checked := 0
	for _, route := range baseline.Signals.RouteMethods {
		if route.Scope != "remote-web" {
			continue
		}
		path := samples[route.Name]
		if path == "" {
			t.Fatalf("Amp binary baseline exposes remote-web route %q without an explicit local synthesis policy sample", route.Name)
		}
		checked++
		for _, method := range route.Methods {
			t.Run(method+" "+route.Name, func(t *testing.T) {
				req := httptest.NewRequest(method, path, bytes.NewBufferString(`{"method":"listThreads","params":{"limit":20}}`))
				req.Header.Set("Content-Type", "application/json")
				rec := httptest.NewRecorder()
				r.ServeHTTP(rec, req)

				if rec.Code != http.StatusServiceUnavailable {
					t.Fatalf("%s %s status = %d, want 503 without upstream proxy; body=%s", method, path, rec.Code, rec.Body.String())
				}
				if body := rec.Body.String(); strings.Contains(body, "remote-web-local-leak-marker") || strings.Contains(body, "remote web must not read this local thread") {
					t.Fatalf("remote-web route synthesized local thread data: %s", body)
				}
			})
		}
	}
	if checked == 0 {
		t.Fatal("Amp binary parity baseline has no remote-web route methods")
	}
}

func TestWebLocalInferenceInternalRPCDoesNotServeThreadDocumentsWithoutProxy(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	enabled := true
	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime: newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
			NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled},
		}}),
		lastConfig: &config.AmpCode{
			WebLocalInference: config.AmpWebLocalInference{Enabled: true},
		},
	}
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	createReq := httptest.NewRequest(http.MethodPost, "/api/thread-actors", bytes.NewBufferString(`{"agentMode":"deep","executorType":"local-client","usesThreadActors":true,"prompt":"hello local web","threadMeta":{"cliProxyAPILocalNeo":true}}`))
	createReq.Header.Set("Content-Type", "application/json")
	createRec := httptest.NewRecorder()
	r.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body=%s", createRec.Code, createRec.Body.String())
	}
	var createResponse map[string]any
	if err := json.Unmarshal(createRec.Body.Bytes(), &createResponse); err != nil {
		t.Fatalf("create response JSON error: %v", err)
	}
	threadID := stringValue(createResponse["threadId"])
	requireNeoBinaryV7ThreadID(t, threadID)

	tests := []struct {
		name    string
		method  string
		body    string
		headers map[string]string
		remote  string
	}{
		{
			name:   "web getThread",
			method: "getThread",
			body:   `{"method":"getThread","params":{"thread":"` + threadID + `"}}`,
			headers: map[string]string{
				"Origin":                   "https://ampcode.com",
				ampWebLocalInferenceHeader: "1",
			},
		},
		{
			name:   "web listThreads",
			method: "listThreads",
			body:   `{"method":"listThreads","params":{"limit":20}}`,
			headers: map[string]string{
				"Origin":                   "https://ampcode.com",
				ampWebLocalInferenceHeader: "1",
			},
		},
		{
			name:   "plain getThread",
			method: "getThread",
			body:   `{"method":"getThread","params":{"thread":"` + threadID + `"}}`,
		},
		{
			name:   "cli getThread",
			method: "getThread",
			body:   `{"method":"getThread","params":{"thread":"` + threadID + `"}}`,
			headers: map[string]string{
				"X-Amp-Client-Application": "CLI",
				"X-Amp-Client-Type":        "cli",
			},
			remote: "[::1]:54321",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/api/internal?"+tc.method, bytes.NewBufferString(tc.body))
			req.Header.Set("Content-Type", "application/json")
			for key, value := range tc.headers {
				req.Header.Set(key, value)
			}
			if tc.remote != "" {
				req.RemoteAddr = tc.remote
			}
			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, req)
			if rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusServiceUnavailable, rec.Body.String())
			}
		})
	}
}

func TestNeoDecodeSvelteKitCreateProjectThreadPayloadCaptured(t *testing.T) {
	decoded, err := neoDecodeSvelteKitRemotePayload("W3siY29udGVudCI6MSwiYWdlbnRNb2RlIjo1LCJzcGF3bkV4ZWN1dG9yIjo2LCJ0aHJlYWRJRCI6NywicHJvamVjdElEIjo4LCJyZWFzb25pbmdFZmZvcnQiOjl9LFsyXSx7InR5cGUiOjMsInRleHQiOjR9LCJ0ZXh0IiwiRm9sbG93aW5nIEBULTAxOWYxZjYyLTc5OTYtNzY4Ny1hYzMzLTg5MDZhMGQzZDU3MSIsImRlZXAiLHRydWUsIlQtMDE5ZjIwYjItNWUwNS03NTAxLThkZGQtOTk0ZTE1MWVlOTUxIiwiNzU2MTZjM2ItZjRkZS00OGI3LThiODMtYzFhZjY5NzhhMDM0IiwibWVkaXVtIl0")
	if err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	request := mapValue(decoded)
	if stringValue(request["agentMode"]) != "deep" || stringValue(request["reasoningEffort"]) != "medium" || request["spawnExecutor"] != true {
		t.Fatalf("decoded mode/effort/spawn = %#v", request)
	}
	if got := stringValue(request["threadID"]); got != "T-019f20b2-5e05-7501-8ddd-994e151ee951" {
		t.Fatalf("threadID = %q", got)
	}
	if got := stringValue(request["projectID"]); got != "75616c3b-f4de-48b7-8b83-c1af6978a034" {
		t.Fatalf("projectID = %q", got)
	}
	if got := textFromBlocks(arrayValue(request["content"])); got != "Following @T-019f1f62-7996-7687-ac33-8906a0d3d571" {
		t.Fatalf("content text = %q", got)
	}
}

func TestNeoWebLocalRemoteEndpointOnlyAllowsLocalWebRoutes(t *testing.T) {
	for _, path := range []string{"/_app/remote/urffnu/addThreadLabel", "/_app/remote/145jw2/archiveThreadCommand", "/_app/remote/3abror/createProjectThread", "/_app/remote/145jw2/deleteThreadCommand", "/_app/remote/3abror/listUserExecutorRunners", "/_app/remote/145jw2/markThreadUnreadCommand", "/_app/remote/145jw2/pinThreadCommand", "/_app/remote/3abror/prewarmProjectThread", "/_app/remote/urffnu/removeThreadLabel", "/_app/remote/127n0b0/updateOwnedProjectChangesWorkflow"} {
		if _, _, ok := neoWebLocalRemoteEndpoint(path); !ok {
			t.Fatalf("remote endpoint %s was not accepted", path)
		}
	}
	for _, path := range []string{"/_app/remote/cliproxy/listThreadListSidebar", "/_app/remote/3abror/listThreadListSidebar", "/_app/remote/3abror/listUserExecutorDaemons"} {
		if _, _, ok := neoWebLocalRemoteEndpoint(path); ok {
			t.Fatalf("remote endpoint %s should not be served locally", path)
		}
	}
}

func TestWebLocalInferenceProjectChangesWorkflowRemote(t *testing.T) {
	allowTestTemporaryProjectDirectories(t)
	gin.SetMode(gin.TestMode)
	enabled := true
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
		NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled},
	}})
	rt.threadDir = t.TempDir()
	workingDirectory := t.TempDir()
	repositoryURL := neoFileURLForDirectory(workingDirectory)
	projectName := filepath.Base(workingDirectory)
	projectID := neoDeterministicLocalProjectID(projectName, repositoryURL, neoExistingDirectory(workingDirectory))
	if err := writeNeoWebLocalProjectIndex(rt.threadDir, []any{map[string]any{
		"id":               projectID,
		"name":             projectName,
		"repositoryURL":    repositoryURL,
		"workingDirectory": workingDirectory,
	}}); err != nil {
		t.Fatalf("write project index: %v", err)
	}

	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime:          rt,
		lastConfig: &config.AmpCode{
			WebLocalInference: config.AmpWebLocalInference{Enabled: true},
			NeoLocalRuntime:   config.AmpNeoLocalRuntime{Enabled: &enabled},
		},
	}
	router := gin.New()
	auth := func(c *gin.Context) {
		if c.GetHeader("Authorization") != "Bearer local-key" {
			c.AbortWithStatus(http.StatusUnauthorized)
			return
		}
		c.Next()
	}
	m.registerManagementRoutes(router, &handlers.BaseAPIHandler{}, auth)
	body := neoSvelteKitRemoteCommandBodyForTest(t, map[string]any{
		"projectID":       projectID,
		"changesWorkflow": "merge-to-main",
	})
	req := httptest.NewRequest(http.MethodPost, "/_app/remote/127n0b0/updateOwnedProjectChangesWorkflow", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer local-key")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", "https://ampcode.com")
	req.Header.Set(ampWebLocalInferenceHeader, "1")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("update changes workflow status = %d, body=%s", rec.Code, rec.Body.String())
	}
	result := mapValue(decodeSvelteKitRemoteEnvelopeForTest(t, rec.Body.Bytes())["_"])
	project := mapValue(result["project"])
	if result["success"] != true || project["changesWorkflow"] != "merge-to-main" {
		t.Fatalf("update changes workflow result = %#v", result)
	}
	persisted := neoWebLocalProjectByID(readNeoWebLocalProjectIndex(rt.threadDir), projectID)
	if persisted["changesWorkflow"] != "merge-to-main" {
		t.Fatalf("persisted project = %#v", persisted)
	}
}

func TestWebLocalInferenceLocalProjectsRoute(t *testing.T) {
	allowTestTemporaryProjectDirectories(t)
	gin.SetMode(gin.TestMode)
	dataDir := t.TempDir()
	oldStoreDir := neoAmpDataDir
	neoAmpDataDir = func() string { return dataDir }
	t.Cleanup(func() { neoAmpDataDir = oldStoreDir })
	r := gin.New()
	enabled := true
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
		NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled},
	}})
	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime:          rt,
		lastConfig: &config.AmpCode{
			WebLocalInference: config.AmpWebLocalInference{Enabled: true},
		},
	}
	auth := func(c *gin.Context) {
		token := strings.TrimSpace(c.GetHeader("Authorization"))
		token = strings.TrimSpace(strings.TrimPrefix(token, "Bearer "))
		if token != "local-key" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing auth"})
			return
		}
		c.Set("userApiKey", token)
		c.Next()
	}
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, auth)

	workDir := neoExistingDirectory(t.TempDir())
	projectID := neoDeterministicLocalProjectID("local-app", neoFileURLForDirectory(workDir), workDir)
	persistedRepositoryURL := "https://github.com/example/persisted-app.git"
	persistedProjectID := neoDeterministicLocalProjectID("persisted-app", persistedRepositoryURL, "")
	if err := writeNeoWebLocalProjectIndex(rt.threadDir, []any{
		map[string]any{
			"id":               projectID,
			"name":             "local-app",
			"repositoryURL":    neoFileURLForDirectory(workDir),
			"workingDirectory": workDir,
		},
		map[string]any{
			"id":            persistedProjectID,
			"name":          "persisted-app",
			"repositoryURL": persistedRepositoryURL,
		},
	}); err != nil {
		t.Fatalf("write project index: %v", err)
	}
	persistedThreadID := "T-019f6551-9d97-73b2-ab56-d3967bce6e10"
	if _, err := writeNeoLocalThreadFileInDir(rt.threadDir, persistedThreadID, map[string]any{
		"id":    persistedThreadID,
		"title": "Persisted repository thread",
		"meta": map[string]any{
			"cliProxyAPILocalNeo": true,
			"repositoryURL":       persistedRepositoryURL,
		},
	}); err != nil {
		t.Fatalf("write persisted repository thread: %v", err)
	}
	archivedThreadID := "T-019f6551-9d97-73b2-ab56-d3967bce6e11"
	if _, err := writeNeoLocalThreadFileInDir(rt.threadDir, archivedThreadID, map[string]any{
		"id":       archivedThreadID,
		"title":    "Archived local thread",
		"archived": true,
		"meta":     map[string]any{"cliProxyAPILocalNeo": true},
	}); err != nil {
		t.Fatalf("write archived local thread: %v", err)
	}
	reviewThreadID := "T-019f6551-9d97-73b2-ab56-d3967bce6e12"
	if _, err := writeNeoLocalThreadFileInDir(rt.threadDir, reviewThreadID, map[string]any{
		"id":    reviewThreadID,
		"title": "Local review thread",
		"meta": map[string]any{
			"cliProxyAPILocalNeo": true,
			"labels":              []any{"review"},
		},
	}); err != nil {
		t.Fatalf("write review thread: %v", err)
	}
	threadID := "T-019f6551-9d97-73b2-ab56-d3967bce6e09"
	actor := rt.store.ensureThreadActor(threadID)
	actor.mu.Lock()
	actor.title = "New local web thread"
	actor.lastUsed = time.Now()
	actor.environment = map[string]any{"workingDirectory": workDir, "workspaceRoot": workDir}
	actor.meta = map[string]any{"executorType": "local-client", "usesThreadActors": true}
	actor.settings = map[string]any{"agentMode": "medium", "reasoning.effort": "medium"}
	actor.currentAgentMode = "medium"
	actor.currentReasoningEffort = "medium"
	actor.messages = []neoMessage{
		{ThreadID: threadID, MessageID: "M-local-user", Role: "user", Content: []any{map[string]any{"type": "text", "text": "inspect local history"}}, Seq: 1},
		{ThreadID: threadID, MessageID: "M-local-assistant", Role: "assistant", Content: []any{map[string]any{"type": "text", "text": "history ready"}}, State: map[string]any{"type": "complete", "stopReason": "end_turn"}, Seq: 2},
	}
	actor.seq = 3
	actor.mu.Unlock()
	puckThreadID := "T-019f6551-9d97-73b2-ab56-d3967bce6e13"
	puckActor := rt.store.ensureThreadActor(puckThreadID)
	puckActor.mu.Lock()
	puckActor.title = "Puck"
	puckActor.lastUsed = time.Now().Add(time.Second)
	puckActor.meta = map[string]any{"executorType": "local-client", "usesThreadActors": true}
	puckActor.settings = map[string]any{"agentMode": "puck", "reasoning.effort": "none"}
	puckActor.currentAgentMode = "puck"
	puckActor.currentReasoningEffort = "none"
	puckActor.messages = []neoMessage{{ThreadID: puckThreadID, MessageID: "M-puck-user", Role: "user", Content: []any{map[string]any{"type": "text", "text": "hello Puck"}}, Seq: 1}}
	puckActor.seq = 2
	puckActor.mu.Unlock()
	for index := 0; index < 50*neoWebLocalSidebarScanFactor+1; index++ {
		recentPuckThreadID := fmt.Sprintf("T-019f6552-9d97-73b2-ab56-%012x", index+1)
		recentPuckActor := rt.store.ensureThreadActor(recentPuckThreadID)
		recentPuckActor.mu.Lock()
		recentPuckActor.title = "Puck"
		recentPuckActor.lastUsed = time.Now().Add(time.Duration(index+2) * time.Second)
		recentPuckActor.settings = map[string]any{"agentMode": "puck", "reasoning.effort": "none"}
		recentPuckActor.currentAgentMode = "puck"
		recentPuckActor.currentReasoningEffort = "none"
		recentPuckActor.messages = []neoMessage{{ThreadID: recentPuckThreadID, MessageID: "M-puck-user", Role: "user", Content: []any{map[string]any{"type": "text", "text": "hello Puck"}}, Seq: 1}}
		recentPuckActor.seq = 2
		recentPuckActor.mu.Unlock()
	}

	req := httptest.NewRequest(http.MethodGet, "/ampcode/local-projects.json?"+ampWebLocalInferenceAPIKeyQuery+"=local-key&cliproxy-thread-id="+url.QueryEscape(threadID)+"&"+ampWebLocalSidebarThreadIDQuery+"="+url.QueryEscape(persistedThreadID)+"&"+ampWebLocalSidebarThreadIDQuery+"="+url.QueryEscape(archivedThreadID), nil)
	req.Header.Set("Origin", "https://ampcode.com")
	req.Header.Set(ampWebLocalInferenceHeader, "1")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("local projects status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if allowOrigin := rec.Header().Get("Access-Control-Allow-Origin"); allowOrigin != "https://ampcode.com" {
		t.Fatalf("Access-Control-Allow-Origin = %q, want https://ampcode.com", allowOrigin)
	}
	var response map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("local projects JSON error: %v", err)
	}
	projects := arrayValue(response["projects"])
	if response["ok"] != true || len(projects) != 2 {
		t.Fatalf("local projects response = %#v", response)
	}
	homeDirectory := neoDefaultWebLocalWorkingDirectory()
	if stringValue(response["defaultWorkingDirectory"]) != homeDirectory {
		t.Fatalf("defaultWorkingDirectory = %#v, want %q", response["defaultWorkingDirectory"], homeDirectory)
	}
	var project map[string]any
	for _, rawProject := range projects {
		candidate := mapValue(rawProject)
		if stringValue(candidate["id"]) == projectID {
			project = candidate
			break
		}
	}
	if stringValue(project["workingDirectory"]) != workDir || stringValue(project["name"]) != "local-app" {
		t.Fatalf("project = %#v, want id=%q workingDirectory=%q", project, projectID, workDir)
	}
	if repositories, ok := project["additionalRepositories"].([]any); !ok || len(repositories) != 0 {
		t.Fatalf("project additionalRepositories = %#v, want empty array", project["additionalRepositories"])
	}
	thread := mapValue(response["thread"])
	if stringValue(thread["id"]) != threadID || stringValue(thread["title"]) != "New local web thread" || numberFrom(thread["v"]) < 1 {
		t.Fatalf("thread summary = %#v", thread)
	}
	if _, exists := thread["messages"]; exists {
		t.Fatalf("thread summary included transcript messages: %#v", thread)
	}
	recentThreads := arrayValue(response["threads"])
	if len(recentThreads) != 3 {
		t.Fatalf("recent thread summaries = %#v, want live, persisted, and review threads", recentThreads)
	}
	foundLiveThread := false
	foundPersistedThread := false
	foundReviewThread := false
	for _, rawRecentThread := range recentThreads {
		recentThread := mapValue(rawRecentThread)
		if automation, exists := recentThread["automation"]; !exists || automation != nil {
			t.Fatalf("recent thread automation = %#v, exists=%v, want JSON null", automation, exists)
		}
		switch stringValue(recentThread["id"]) {
		case threadID:
			foundLiveThread = true
		case persistedThreadID:
			foundPersistedThread = stringValue(mapValue(recentThread["meta"])["projectID"]) == persistedProjectID
		case reviewThreadID:
			foundReviewThread = ampThreadListHasExcludedLabel(recentThread, map[string]bool{"review": true})
		}
	}
	if !foundLiveThread || !foundPersistedThread || !foundReviewThread {
		t.Fatalf("recent thread project association = %#v, want live=%v persisted project=%q review=%v", recentThreads, foundLiveThread, persistedProjectID, foundReviewThread)
	}
	for _, rawRecentThread := range recentThreads {
		if stringValue(mapValue(rawRecentThread)["id"]) == puckThreadID {
			t.Fatalf("Puck backing task leaked into ordinary sidebar threads: %#v", recentThreads)
		}
	}
	puckProjectsReq := httptest.NewRequest(http.MethodGet, "/ampcode/local-projects.json?"+ampWebLocalInferenceAPIKeyQuery+"=local-key&cliproxy-thread-id="+url.QueryEscape(puckThreadID), nil)
	puckProjectsReq.Header.Set("Origin", "https://ampcode.com")
	puckProjectsReq.Header.Set(ampWebLocalInferenceHeader, "1")
	puckProjectsRec := httptest.NewRecorder()
	r.ServeHTTP(puckProjectsRec, puckProjectsReq)
	if puckProjectsRec.Code != http.StatusOK {
		t.Fatalf("Puck local projects status = %d, body=%s", puckProjectsRec.Code, puckProjectsRec.Body.String())
	}
	var puckProjectsResponse map[string]any
	if err := json.Unmarshal(puckProjectsRec.Body.Bytes(), &puckProjectsResponse); err != nil {
		t.Fatalf("Puck local projects JSON error: %v", err)
	}
	if puckProjectsResponse["thread"] != nil {
		t.Fatalf("Puck backing task was returned as current sidebar thread: %#v", puckProjectsResponse["thread"])
	}
	puckSummaryReq := httptest.NewRequest(http.MethodGet, "/ampcode/local-thread-data.json?"+ampWebLocalInferenceAPIKeyQuery+"=local-key&"+ampWebLocalThreadSummaryQuery+"=1&cliproxy-thread-id="+url.QueryEscape(puckThreadID), nil)
	puckSummaryReq.Header.Set("Origin", "https://ampcode.com")
	puckSummaryReq.Header.Set(ampWebLocalInferenceHeader, "1")
	puckSummaryRec := httptest.NewRecorder()
	r.ServeHTTP(puckSummaryRec, puckSummaryReq)
	if puckSummaryRec.Code != http.StatusOK {
		t.Fatalf("Puck local thread summary status = %d, body=%s", puckSummaryRec.Code, puckSummaryRec.Body.String())
	}
	var puckSummaryResponse map[string]any
	if err := json.Unmarshal(puckSummaryRec.Body.Bytes(), &puckSummaryResponse); err != nil {
		t.Fatalf("Puck local thread summary JSON error: %v", err)
	}
	if stringValue(mapValue(puckSummaryResponse["thread"])["id"]) != puckThreadID {
		t.Fatalf("Puck backing task was not available through direct local thread data: %#v", puckSummaryResponse)
	}
	if got := stringValue(mapValue(response["threadTitles"])[persistedThreadID]); got != "Persisted repository thread" {
		t.Fatalf("sidebar thread title = %q, want Persisted repository thread", got)
	}
	if rt.store.lookupThreadActor(persistedThreadID) != nil {
		t.Fatal("sidebar title lookup hydrated a persisted thread actor")
	}
	archivedThreadIDs := arrayValue(response["archivedThreadIDs"])
	if len(archivedThreadIDs) != 1 || stringValue(archivedThreadIDs[0]) != archivedThreadID {
		t.Fatalf("archived thread IDs = %#v, want %q", archivedThreadIDs, archivedThreadID)
	}
	recentThreadMeta := mapValue(mapValue(recentThreads[0])["meta"])
	if stringValue(recentThreadMeta["projectID"]) != projectID || recentThreadMeta["usesThreadActors"] != true {
		t.Fatalf("recent thread summary meta = %#v, want projectID=%q", recentThreadMeta, projectID)
	}
	threadMeta := mapValue(thread["meta"])
	if threadMeta["cliProxyAPILocalNeo"] != true || threadMeta["usesThreadActors"] != true || stringValue(threadMeta["executorType"]) != "local-client" {
		t.Fatalf("thread summary meta = %#v", threadMeta)
	}

	threadSummaryReq := httptest.NewRequest(http.MethodGet, "/ampcode/local-thread-data.json?"+ampWebLocalInferenceAPIKeyQuery+"=local-key&"+ampWebLocalThreadSummaryQuery+"=1&cliproxy-thread-id="+url.QueryEscape(threadID), nil)
	threadSummaryReq.Header.Set("Origin", "https://ampcode.com")
	threadSummaryReq.Header.Set(ampWebLocalInferenceHeader, "1")
	threadSummaryRec := httptest.NewRecorder()
	r.ServeHTTP(threadSummaryRec, threadSummaryReq)
	if threadSummaryRec.Code != http.StatusOK {
		t.Fatalf("local thread summary status = %d, body=%s", threadSummaryRec.Code, threadSummaryRec.Body.String())
	}
	var threadSummaryData map[string]any
	if err := json.Unmarshal(threadSummaryRec.Body.Bytes(), &threadSummaryData); err != nil {
		t.Fatalf("local thread summary JSON error: %v", err)
	}
	threadSummary := mapValue(threadSummaryData["thread"])
	if numberFrom(threadSummary["v"]) != 2 || stringValue(threadSummary["id"]) != threadID {
		t.Fatalf("local thread summary data = %#v", threadSummaryData)
	}
	if _, exists := threadSummary["messages"]; exists {
		t.Fatalf("local thread summary included messages: %#v", threadSummary)
	}

	threadDataReq := httptest.NewRequest(http.MethodGet, "/ampcode/local-thread-data.json?"+ampWebLocalInferenceAPIKeyQuery+"=local-key&cliproxy-thread-id="+url.QueryEscape(threadID), nil)
	threadDataReq.Header.Set("Origin", "https://ampcode.com")
	threadDataReq.Header.Set(ampWebLocalInferenceHeader, "1")
	threadDataRec := httptest.NewRecorder()
	r.ServeHTTP(threadDataRec, threadDataReq)
	if threadDataRec.Code != http.StatusOK {
		t.Fatalf("local thread data status = %d, body=%s", threadDataRec.Code, threadDataRec.Body.String())
	}
	if got := threadDataRec.Header().Get("Content-Type"); got != "application/json; charset=utf-8" {
		t.Fatalf("local thread data content type = %q", got)
	}
	expectedThreadData := rt.neoWebLocalThreadData(threadID, neoWebLocalRequestBaseURL(threadDataReq), "local-key")
	expectedThreadDataJSON, err := json.Marshal(expectedThreadData)
	if err != nil {
		t.Fatalf("marshal expected local thread data: %v", err)
	}
	if got := threadDataRec.Body.Bytes(); !bytes.Equal(got, expectedThreadDataJSON) {
		t.Fatal("streamed local thread data differs from buffered JSON")
	}
	var threadData map[string]any
	if err := json.Unmarshal(threadDataRec.Body.Bytes(), &threadData); err != nil {
		t.Fatalf("local thread data JSON error: %v", err)
	}
	threadDataThread := mapValue(threadData["thread"])
	if stringValue(threadData["type"]) != "result" || stringValue(threadDataThread["id"]) != threadID || numberFrom(threadDataThread["v"]) != 2 {
		t.Fatalf("local thread data = %#v", threadData)
	}
	messages, ok := threadDataThread["messages"].([]any)
	if !ok || len(messages) != 2 {
		t.Fatalf("local thread data messages = %#v", threadDataThread["messages"])
	}
	if stringValue(mapValue(messages[0])["protocolMessageID"]) != "M-local-user" || stringValue(mapValue(messages[1])["protocolMessageID"]) != "M-local-assistant" {
		t.Fatalf("local thread data message IDs = %#v", messages)
	}
	threadDataMeta := mapValue(threadDataThread["meta"])
	if stringValue(threadDataMeta["workspaceID"]) != "W-cliproxy-local" || threadData["viewerInThreadWorkspace"] != true {
		t.Fatalf("local thread data app access = %#v", threadData)
	}
	threadDataEnvInitial := mapValue(mapValue(threadDataThread["env"])["initial"])
	if stringValue(threadDataEnvInitial["workingDirectory"]) != workDir || stringValue(threadDataEnvInitial["workspaceRoot"]) != workDir {
		t.Fatalf("local thread data environment = %#v", threadDataThread["env"])
	}
	threadDataConfig := mapValue(threadData["threadActorConfig"])
	if stringValue(threadDataConfig["threadId"]) != threadID || stringValue(threadDataConfig["wsToken"]) != "local-key" || stringValue(threadDataConfig["baseURL"]) != neoWebLocalRequestBaseURL(threadDataReq) {
		t.Fatalf("local thread actor config = %#v", threadDataConfig)
	}
	permissions := mapValue(threadData["actorPermissions"])
	if permissions["manageThread"] != true || permissions["manageBilling"] != false {
		t.Fatalf("local thread actor permissions = %#v", permissions)
	}

	if err := writeNeoWebLocalProjectIndex(rt.threadDir, []any{
		map[string]any{
			"id":               projectID,
			"name":             "renamed-local-app",
			"repositoryURL":    neoFileURLForDirectory(workDir),
			"workingDirectory": workDir,
		},
		map[string]any{
			"id":            persistedProjectID,
			"name":          "persisted-app",
			"repositoryURL": persistedRepositoryURL,
		},
	}); err != nil {
		t.Fatalf("rename project index entry: %v", err)
	}

	historyDir := neoExistingDirectory(t.TempDir())
	historyLine, err := json.Marshal(map[string]any{"text": "history project", "cwd": historyDir})
	if err != nil {
		t.Fatalf("marshal history: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "history.jsonl"), append(historyLine, '\n'), 0o600); err != nil {
		t.Fatalf("write history: %v", err)
	}
	refreshedReq := httptest.NewRequest(http.MethodGet, "/ampcode/local-projects.json?"+ampWebLocalInferenceAPIKeyQuery+"=local-key", nil)
	refreshedReq.Header.Set("Origin", "https://ampcode.com")
	refreshedReq.Header.Set(ampWebLocalInferenceHeader, "1")
	refreshedRec := httptest.NewRecorder()
	r.ServeHTTP(refreshedRec, refreshedReq)
	if refreshedRec.Code != http.StatusOK {
		t.Fatalf("refreshed local projects status = %d, body=%s", refreshedRec.Code, refreshedRec.Body.String())
	}
	var refreshedResponse map[string]any
	if err := json.Unmarshal(refreshedRec.Body.Bytes(), &refreshedResponse); err != nil {
		t.Fatalf("refreshed local projects JSON error: %v", err)
	}
	refreshedProjects := arrayValue(refreshedResponse["projects"])
	if len(refreshedProjects) != 3 {
		t.Fatalf("refreshed projects = %#v, want two index and one history projects", refreshedResponse)
	}
	historyID := neoDeterministicLocalProjectID(filepath.Base(historyDir), neoFileURLForDirectory(historyDir), historyDir)
	foundHistoryProject := false
	foundRenamedProject := false
	for _, rawProject := range refreshedProjects {
		project := mapValue(rawProject)
		if stringValue(project["id"]) == historyID && stringValue(project["workingDirectory"]) == historyDir {
			foundHistoryProject = true
		}
		if stringValue(project["id"]) == projectID && stringValue(project["name"]) == "renamed-local-app" {
			foundRenamedProject = true
		}
	}
	if !foundHistoryProject {
		t.Fatalf("refreshed projects missing history project id=%q dir=%q: %#v", historyID, historyDir, refreshedProjects)
	}
	if !foundRenamedProject {
		t.Fatalf("refreshed projects missing renamed project id=%q: %#v", projectID, refreshedProjects)
	}
	foundRefreshedLiveThread := false
	for _, rawThread := range arrayValue(refreshedResponse["threads"]) {
		thread := mapValue(rawThread)
		if stringValue(thread["id"]) == threadID {
			foundRefreshedLiveThread = true
			if stringValue(mapValue(thread["meta"])["projectName"]) != "renamed-local-app" {
				t.Fatalf("refreshed thread used stale project metadata: %#v", thread)
			}
		}
	}
	if !foundRefreshedLiveThread {
		t.Fatalf("refreshed threads missing live thread %q: %#v", threadID, refreshedResponse["threads"])
	}
	indexPath := neoWebLocalProjectIndexPath(rt.threadDir)
	indexInfo, err := os.Stat(indexPath)
	if err != nil {
		t.Fatalf("stat refreshed project index: %v", err)
	}
	time.Sleep(10 * time.Millisecond)
	repeatedReq := httptest.NewRequest(http.MethodGet, "/ampcode/local-projects.json?"+ampWebLocalInferenceAPIKeyQuery+"=local-key", nil)
	repeatedReq.Header.Set("Origin", "https://ampcode.com")
	repeatedReq.Header.Set(ampWebLocalInferenceHeader, "1")
	repeatedRec := httptest.NewRecorder()
	r.ServeHTTP(repeatedRec, repeatedReq)
	if repeatedRec.Code != http.StatusOK {
		t.Fatalf("repeated local projects status = %d, body=%s", repeatedRec.Code, repeatedRec.Body.String())
	}
	repeatedIndexInfo, err := os.Stat(indexPath)
	if err != nil {
		t.Fatalf("stat repeated project index: %v", err)
	}
	if !repeatedIndexInfo.ModTime().Equal(indexInfo.ModTime()) {
		t.Fatalf("unchanged local projects request rewrote index mtime=%s want=%s", repeatedIndexInfo.ModTime(), indexInfo.ModTime())
	}

	unmarkedReq := httptest.NewRequest(http.MethodGet, "/ampcode/local-projects.json?"+ampWebLocalInferenceAPIKeyQuery+"=local-key", nil)
	unmarkedReq.Header.Set("Origin", "https://ampcode.com")
	unmarkedRec := httptest.NewRecorder()
	r.ServeHTTP(unmarkedRec, unmarkedReq)
	if unmarkedRec.Code != http.StatusNotFound {
		t.Fatalf("unmarked local projects status = %d, want %d; body=%s", unmarkedRec.Code, http.StatusNotFound, unmarkedRec.Body.String())
	}

	unauthorizedReq := httptest.NewRequest(http.MethodGet, "/ampcode/local-projects.json", nil)
	unauthorizedReq.Header.Set("Origin", "https://ampcode.com")
	unauthorizedReq.Header.Set(ampWebLocalInferenceHeader, "1")
	unauthorizedRec := httptest.NewRecorder()
	r.ServeHTTP(unauthorizedRec, unauthorizedReq)
	if unauthorizedRec.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized local projects status = %d, want %d; body=%s", unauthorizedRec.Code, http.StatusUnauthorized, unauthorizedRec.Body.String())
	}

	methodReq := httptest.NewRequest(http.MethodPost, "/ampcode/local-project-details.json?"+ampWebLocalInferenceAPIKeyQuery+"=local-key", nil)
	methodReq.Header.Set("Origin", "https://ampcode.com")
	methodReq.Header.Set(ampWebLocalInferenceHeader, "1")
	methodRec := httptest.NewRecorder()
	r.ServeHTTP(methodRec, methodReq)
	if methodRec.Code != http.StatusMethodNotAllowed || methodRec.Header().Get("Allow") != http.MethodGet {
		t.Fatalf("local project details method response = status %d Allow %q, want 405 Allow GET", methodRec.Code, methodRec.Header().Get("Allow"))
	}

	time.Sleep(10 * time.Millisecond)
	ambiguousDirectories := []string{t.TempDir(), t.TempDir()}
	for _, directory := range ambiguousDirectories {
		for _, args := range [][]string{{"init"}, {"remote", "add", "origin", "https://github.com/example/duplicate.git"}} {
			command := exec.Command("git", args...)
			command.Dir = directory
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("git %v in ambiguous checkout: %v\n%s", args, err, output)
			}
		}
	}
	if err := writeNeoWebLocalProjectIndex(rt.threadDir, []any{
		map[string]any{"id": "duplicate-a", "name": "duplicate", "namespace": "example", "repositoryURL": "https://github.com/example/duplicate.git", "workingDirectory": ambiguousDirectories[0]},
		map[string]any{"id": "duplicate-b", "name": "duplicate", "namespace": "example", "repositoryURL": "https://github.com/example/duplicate.git", "workingDirectory": ambiguousDirectories[1]},
	}); err != nil {
		t.Fatalf("write ambiguous project index: %v", err)
	}
	rt.reloadNeoWebLocalProjectCache()
	ambiguousReq := httptest.NewRequest(http.MethodGet, "/ampcode/local-project-details.json?"+ampWebLocalInferenceAPIKeyQuery+"=local-key&namespace=example&project=duplicate", nil)
	ambiguousReq.Header.Set("Origin", "https://ampcode.com")
	ambiguousReq.Header.Set(ampWebLocalInferenceHeader, "1")
	ambiguousRec := httptest.NewRecorder()
	r.ServeHTTP(ambiguousRec, ambiguousReq)
	if ambiguousRec.Code != http.StatusConflict {
		t.Fatalf("ambiguous local project details status = %d, want 409; body=%s", ambiguousRec.Code, ambiguousRec.Body.String())
	}
	var ambiguousResponse map[string]any
	if err := json.Unmarshal(ambiguousRec.Body.Bytes(), &ambiguousResponse); err != nil {
		t.Fatalf("ambiguous local project details JSON error: %v", err)
	}
	if stringValue(ambiguousResponse["code"]) != "ambiguous_project" {
		t.Fatalf("ambiguous local project details response = %#v", ambiguousResponse)
	}
}

func TestWebLocalInferenceThreadDataRecordsCommittedWriterError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	enabled := true
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled}}})
	threadID := "T-019f6551-9d97-73b2-ab56-d3967bce6e20"
	actor := rt.store.ensureThreadActor(threadID)
	actor.mu.Lock()
	actor.title = "Writer failure thread"
	actor.messages = []neoMessage{{ThreadID: threadID, MessageID: "M-writer-error", Role: "user", Content: []any{map[string]any{"type": "text", "text": strings.Repeat("payload ", 64)}}, Seq: 1}}
	actor.seq = 2
	actor.mu.Unlock()
	m := &AmpModule{
		neoRuntime: rt,
		lastConfig: &config.AmpCode{WebLocalInference: config.AmpWebLocalInference{Enabled: true}},
	}
	router := gin.New()
	recordedErrors := 0
	router.GET("/ampcode/local-thread-data.json", func(c *gin.Context) {
		c.Set(ampWebLocalInferenceCORSContextKey, true)
		c.Set("userApiKey", "local-key")
		c.Next()
	}, func(c *gin.Context) {
		m.serveWebLocalThreadData(c)
		recordedErrors = len(c.Errors)
	})
	req := httptest.NewRequest(http.MethodGet, "/ampcode/local-thread-data.json?cliproxy-thread-id="+url.QueryEscape(threadID), nil)
	req.Header.Set(ampWebLocalInferenceHeader, "1")
	response := &failingWebLocalThreadResponseWriter{header: make(http.Header), limit: 64}

	router.ServeHTTP(response, req)

	if response.statusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.statusCode, http.StatusOK)
	}
	if response.body.Len() != response.limit {
		t.Fatalf("committed bytes = %d, want %d", response.body.Len(), response.limit)
	}
	if response.writes != 1 {
		t.Fatalf("response writes = %d, want one failed streaming write", response.writes)
	}
	if recordedErrors != 1 {
		t.Fatalf("recorded errors = %d, want 1", recordedErrors)
	}
}

func TestWebLocalInferenceRemoteCreateProjectThreadCreatesLocalActor(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("test uses POSIX shell script")
	}
	gin.SetMode(gin.TestMode)
	dataDir := t.TempDir()
	oldStoreDir := neoAmpDataDir
	neoAmpDataDir = func() string { return dataDir }
	t.Cleanup(func() { neoAmpDataDir = oldStoreDir })
	pidDir := filepath.Join(t.TempDir(), "pids")
	if err := os.MkdirAll(pidDir, 0o700); err != nil {
		t.Fatalf("mkdir pid dir: %v", err)
	}
	t.Cleanup(replaceNeoHeadlessPIDDir(func() string { return pidDir }))
	executorDir := t.TempDir()
	command := filepath.Join(executorDir, "amp")
	startedLog := filepath.Join(executorDir, "started.log")
	if err := os.WriteFile(command, []byte("#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$TEST_STARTED_LOG\"\nsleep 30\n"), 0o755); err != nil {
		t.Fatalf("write fake amp: %v", err)
	}
	t.Setenv("TEST_STARTED_LOG", startedLog)
	r := gin.New()
	enabled := true
	const (
		threadID              = "T-019f20b2-5e05-7501-8ddd-994e151ee951"
		localProjectThreadID  = "T-019f20b2-5e05-7501-8ddd-994e151ee959"
		projectOnlyThreadID   = "T-019f20b2-5e05-7501-8ddd-994e151ee952"
		runnerThreadID        = "T-019f20b2-5e05-7501-8ddd-994e151ee955"
		runnerOptOutThreadID  = "T-019f20b2-5e05-7501-8ddd-994e151ee956"
		explicitFalseThreadID = "T-019f20b2-5e05-7501-8ddd-994e151ee954"
		missingDirectoryID    = "T-019f20b2-5e05-7501-8ddd-994e151ee953"
	)
	cloudThreadIDs := []string{threadID, localProjectThreadID, projectOnlyThreadID, runnerThreadID, explicitFalseThreadID}
	cloudThreadCalls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/api/internal" && r.URL.RawQuery == "getUserInfo" {
			writeNeoJSON(w, http.StatusOK, map[string]any{"ok": true, "result": map[string]any{"id": "user_cloud"}})
			return
		}
		if r.Method != http.MethodPost || r.URL.Path != "/api/thread-actors" || cloudThreadCalls >= len(cloudThreadIDs) {
			t.Fatalf("unexpected cloud shell request %s %s", r.Method, r.URL.Path)
		}
		body := readNeoJSON(r.Body)
		for _, key := range []string{"prompt", "initialPrompt", "message", "content"} {
			if _, ok := body[key]; ok {
				t.Fatalf("cloud shell request included %s: %#v", key, body)
			}
		}
		response := map[string]any{
			"threadId":         cloudThreadIDs[cloudThreadCalls],
			"wsToken":          "cloud-token",
			"ownerUserId":      "user_cloud",
			"threadVersion":    0,
			"usesDtw":          true,
			"usesThreadActors": true,
		}
		if executorType := stringValue(body["executorType"]); executorType != "" {
			response["executorType"] = executorType
		}
		cloudThreadCalls++
		writeNeoJSON(w, http.StatusCreated, response)
	}))
	t.Cleanup(upstream.Close)
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
		UpstreamURL:     upstream.URL,
		UpstreamAPIKey:  "cloud-key",
		NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled, ExecutorCommand: command},
	}})
	t.Cleanup(func() { rt.store.disposeAll(true, "test done", false) })
	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime:          rt,
		lastConfig: &config.AmpCode{
			WebLocalInference: config.AmpWebLocalInference{Enabled: true},
		},
	}
	auth := func(c *gin.Context) {
		token := strings.TrimSpace(c.GetHeader("Authorization"))
		token = strings.TrimSpace(strings.TrimPrefix(token, "Bearer "))
		if token != "local-key" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing auth"})
			return
		}
		c.Set("userApiKey", token)
		c.Next()
	}
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, auth)

	workDir := t.TempDir()
	expectedWorkDir := neoExistingDirectory(workDir)
	projectID := "75616c3b-f4de-48b7-8b83-c1af6978a034"
	parentThreadID := "T-019f1f62-7996-7687-ac33-8906a0d3d571"
	emptyContentBody := neoSvelteKitRemoteCommandBodyForTest(t, map[string]any{
		"content":  []any{},
		"threadID": threadID,
	})
	emptyContentReq := httptest.NewRequest(http.MethodPost, "/_app/remote/3abror/createProjectThread?"+ampWebLocalInferenceAPIKeyQuery+"=local-key", strings.NewReader(emptyContentBody))
	emptyContentReq.Header.Set("Content-Type", "application/json")
	emptyContentReq.Header.Set("Origin", "https://ampcode.com")
	emptyContentReq.Header.Set(ampWebLocalInferenceHeader, "1")
	emptyContentRec := httptest.NewRecorder()
	r.ServeHTTP(emptyContentRec, emptyContentReq)
	if emptyContentRec.Code != http.StatusOK {
		t.Fatalf("empty-content create status = %d, body=%s", emptyContentRec.Code, emptyContentRec.Body.String())
	}
	emptyContentResult := mapValue(decodeSvelteKitRemoteEnvelopeForTest(t, emptyContentRec.Body.Bytes())["_"])
	if emptyContentResult["ok"] != false || !strings.Contains(stringValue(mapValue(emptyContentResult["error"])["message"]), "valid text or image blocks") {
		t.Fatalf("empty-content create result = %#v", emptyContentResult)
	}
	if cloudThreadCalls != 0 {
		t.Fatalf("empty-content create made %d cloud shell requests", cloudThreadCalls)
	}
	emptyTextContentBody := neoSvelteKitRemoteCommandBodyForTest(t, map[string]any{
		"content":  []any{map[string]any{"type": "text", "text": " \n\t "}},
		"threadID": threadID,
	})
	emptyTextContentReq := httptest.NewRequest(http.MethodPost, "/_app/remote/3abror/createProjectThread?"+ampWebLocalInferenceAPIKeyQuery+"=local-key", strings.NewReader(emptyTextContentBody))
	emptyTextContentReq.Header.Set("Content-Type", "application/json")
	emptyTextContentReq.Header.Set("Origin", "https://ampcode.com")
	emptyTextContentReq.Header.Set(ampWebLocalInferenceHeader, "1")
	emptyTextContentRec := httptest.NewRecorder()
	r.ServeHTTP(emptyTextContentRec, emptyTextContentReq)
	if emptyTextContentRec.Code != http.StatusOK {
		t.Fatalf("empty-text-content create status = %d, body=%s", emptyTextContentRec.Code, emptyTextContentRec.Body.String())
	}
	emptyTextContentResult := mapValue(decodeSvelteKitRemoteEnvelopeForTest(t, emptyTextContentRec.Body.Bytes())["_"])
	if emptyTextContentResult["ok"] != false || !strings.Contains(stringValue(mapValue(emptyTextContentResult["error"])["message"]), "valid text or image blocks") {
		t.Fatalf("empty-text-content create result = %#v", emptyTextContentResult)
	}
	if cloudThreadCalls != 0 {
		t.Fatalf("empty-text-content create made %d cloud shell requests", cloudThreadCalls)
	}
	malformedContentBody := neoSvelteKitRemoteCommandBodyForTest(t, map[string]any{
		"content": []any{map[string]any{
			"type":   "image",
			"source": map[string]any{"type": "base64", "mediaType": "image/png", "data": "not-base64"},
		}},
		"threadID": threadID,
	})
	malformedContentReq := httptest.NewRequest(http.MethodPost, "/_app/remote/3abror/createProjectThread?"+ampWebLocalInferenceAPIKeyQuery+"=local-key", strings.NewReader(malformedContentBody))
	malformedContentReq.Header.Set("Content-Type", "application/json")
	malformedContentReq.Header.Set("Origin", "https://ampcode.com")
	malformedContentReq.Header.Set(ampWebLocalInferenceHeader, "1")
	malformedContentRec := httptest.NewRecorder()
	r.ServeHTTP(malformedContentRec, malformedContentReq)
	if malformedContentRec.Code != http.StatusOK {
		t.Fatalf("malformed-content create status = %d, body=%s", malformedContentRec.Code, malformedContentRec.Body.String())
	}
	malformedContentResult := mapValue(decodeSvelteKitRemoteEnvelopeForTest(t, malformedContentRec.Body.Bytes())["_"])
	if malformedContentResult["ok"] != false || !strings.Contains(stringValue(mapValue(malformedContentResult["error"])["message"]), "valid text or image blocks") {
		t.Fatalf("malformed-content create result = %#v", malformedContentResult)
	}
	if cloudThreadCalls != 0 {
		t.Fatalf("malformed-content create made %d cloud shell requests", cloudThreadCalls)
	}
	if _, status := rt.localThreadActorManagementResponse(context.Background(), map[string]any{
		"threadId":         parentThreadID,
		"workingDirectory": workDir,
		"workspaceRoot":    workDir,
		"threadMeta":       map[string]any{"projectID": projectID},
	}, ""); status != http.StatusOK {
		t.Fatalf("seed parent status = %d", status)
	}

	imageData := testNeoPNGBase64(t, 1, 1)
	requestBody := neoSvelteKitRemoteCommandBodyForTest(t, map[string]any{
		"content": []any{
			map[string]any{"type": "image", "source": map[string]any{"type": "base64", "mediaType": "image/png", "data": imageData}, "sourcePath": "web-create.png"},
			map[string]any{"type": "text", "text": "Following @" + parentThreadID},
		},
		"agentMode":       "deep",
		"spawnExecutor":   true,
		"threadID":        threadID,
		"projectID":       projectID,
		"reasoningEffort": "medium",
	})
	req := httptest.NewRequest(http.MethodPost, "/_app/remote/3abror/createProjectThread?"+ampWebLocalInferenceAPIKeyQuery+"=local-key", strings.NewReader(requestBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", "https://ampcode.com")
	req.Header.Set(ampWebLocalInferenceHeader, "1")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("create remote status = %d, body=%s", rec.Code, rec.Body.String())
	}
	createEnvelope := decodeSvelteKitRemoteEnvelopeForTest(t, rec.Body.Bytes())
	createResult := mapValue(createEnvelope["_"])
	if createResult["ok"] != true || stringValue(createResult["threadID"]) != threadID {
		t.Fatalf("create result = %#v", createResult)
	}
	if createResult["usesThreadActors"] != true || createResult["usesDtw"] != true || stringValue(createResult["executorType"]) != "local-client" || stringValue(createResult["wsToken"]) == "" {
		t.Fatalf("create local actor response = %#v", createResult)
	}
	createActorConfig := mapValue(createResult["threadActorConfig"])
	if stringValue(createActorConfig["threadId"]) != threadID || stringValue(createActorConfig["wsToken"]) == "" || stringValue(createActorConfig["baseURL"]) == "" || stringValue(createActorConfig["ampURL"]) == "" {
		t.Fatalf("create actor config = %#v", createActorConfig)
	}
	createThreadData := mapValue(createResult["threadData"])
	createThreadDataConfig := mapValue(createThreadData["threadActorConfig"])
	if stringValue(createThreadDataConfig["threadId"]) != threadID || stringValue(createThreadDataConfig["wsToken"]) == "" {
		t.Fatalf("create threadData actor config = %#v", createThreadData)
	}
	createThreadDataThread := mapValue(createThreadData["thread"])
	if stringValue(createThreadDataThread["id"]) != threadID || stringValue(createThreadDataThread["creatorUserID"]) == "" {
		t.Fatalf("create threadData thread identity = %#v", createThreadDataThread)
	}
	if _, ok := createThreadDataThread["messages"].([]any); !ok {
		t.Fatalf("create threadData thread messages = %#v", createThreadDataThread["messages"])
	}
	createThreadDataMeta := mapValue(createThreadDataThread["meta"])
	if stringValue(createThreadDataMeta["executorType"]) != "local-client" || createThreadDataMeta["usesThreadActors"] != true {
		t.Fatalf("create threadData runtime identity = %#v", createThreadDataMeta)
	}
	if createThreadDataThread["hasExecutor"] == false || createThreadDataThread["executorConnected"] == false {
		t.Fatalf("create threadData leaked disconnected executor state: %#v", createThreadDataThread)
	}
	initialThread := mapValue(createResult["initialThread"])
	if stringValue(initialThread["id"]) != threadID {
		t.Fatalf("create initialThread = %#v", initialThread)
	}
	initialQueuedMessages := arrayValue(initialThread["queuedMessages"])
	if len(initialQueuedMessages) != 1 {
		t.Fatalf("create initialThread queued messages = %#v, thread = %#v", initialQueuedMessages, initialThread)
	}
	initialQueuedContent := arrayValue(mapValue(mapValue(initialQueuedMessages[0])["queuedMessage"])["content"])
	if len(initialQueuedContent) != 2 || stringValue(mapValue(mapValue(initialQueuedContent[0])["source"])["data"]) != imageData || stringValue(mapValue(initialQueuedContent[1])["text"]) != "Following @"+parentThreadID {
		t.Fatalf("create initialThread queued content = %#v", initialQueuedContent)
	}
	if stringValue(createResult["workingDirectory"]) != expectedWorkDir || stringValue(createResult["workspaceRoot"]) != expectedWorkDir {
		t.Fatalf("create working directory response = %#v, want %q", createResult, expectedWorkDir)
	}

	actor := rt.store.lookupThreadActor(threadID)
	if actor == nil {
		t.Fatal("created thread actor not found")
	}
	actor.mu.Lock()
	environment := cloneMap(actor.environment)
	queue := append([]neoQueuedMessage(nil), actor.queue...)
	agentMode := actor.currentAgentMode
	reasoningEffort := actor.currentReasoningEffort
	meta := cloneMap(actor.meta)
	bootstrapExecutorType := actor.bootstrapExecutorType
	spawnedCount := len(actor.spawnedExecutors)
	actor.mu.Unlock()
	if got := stringValue(environment["workingDirectory"]); got != expectedWorkDir {
		t.Fatalf("workingDirectory = %q, want %q", got, expectedWorkDir)
	}
	if len(queue) != 1 || textFromBlocks(queue[0].Content) != "Following @"+parentThreadID {
		t.Fatalf("queue = %#v", queue)
	}
	queuedContent := queue[0].Content
	if len(queuedContent) != 2 || stringValue(mapValue(mapValue(queuedContent[0])["source"])["data"]) != imageData || stringValue(mapValue(queuedContent[1])["text"]) != "Following @"+parentThreadID {
		t.Fatalf("queued content = %#v", queuedContent)
	}
	if agentMode != "deep" || reasoningEffort != "medium" {
		t.Fatalf("mode/effort = %q/%q", agentMode, reasoningEffort)
	}
	if stringValue(meta["projectID"]) != projectID || stringValue(meta["ampcodeConnectorMode"]) != "local-neo" {
		t.Fatalf("meta = %#v", meta)
	}
	if bootstrapExecutorType != "local-client" || spawnedCount != 1 {
		t.Fatalf("executor bootstrap = type:%q spawned:%d", bootstrapExecutorType, spawnedCount)
	}

	localProjectBody := neoSvelteKitRemoteCommandBodyForTest(t, map[string]any{
		"content":       []any{map[string]any{"type": "text", "text": "Use injected local project"}},
		"agentMode":     "medium",
		"spawnExecutor": true,
		"threadID":      localProjectThreadID,
		"projectID":     projectID,
	})
	expectedLocalProjectID := stringValue(rt.neoWebLocalProjectForWorkingDirectory(expectedWorkDir)["id"])
	if expectedLocalProjectID == "" {
		t.Fatal("expected inferred local project ID")
	}
	localProjectReq := httptest.NewRequest(http.MethodPost, "/_app/remote/3abror/createProjectThread?"+ampWebLocalInferenceAPIKeyQuery+"=local-key&cliproxy-working-directory="+url.QueryEscape(expectedWorkDir)+"&cliproxy-local-project=1", strings.NewReader(localProjectBody))
	localProjectReq.Header.Set("Content-Type", "application/json")
	localProjectReq.Header.Set("Origin", "https://ampcode.com")
	localProjectReq.Header.Set(ampWebLocalInferenceHeader, "1")
	localProjectRec := httptest.NewRecorder()
	r.ServeHTTP(localProjectRec, localProjectReq)
	if localProjectRec.Code != http.StatusOK {
		t.Fatalf("local-project create status = %d, body=%s", localProjectRec.Code, localProjectRec.Body.String())
	}
	localProjectActor := rt.store.lookupThreadActor(localProjectThreadID)
	if localProjectActor == nil {
		t.Fatal("local-project thread actor not found")
	}
	localProjectActor.mu.Lock()
	localProjectMeta := cloneMap(localProjectActor.meta)
	localProjectEnvironment := cloneMap(localProjectActor.environment)
	localProjectActor.mu.Unlock()
	if stringValue(localProjectMeta["projectID"]) != expectedLocalProjectID {
		t.Fatalf("local-project create project ID = %#v, want %q", localProjectMeta, expectedLocalProjectID)
	}
	workspace := neoRecentThreadWorkspace(localProjectEnvironment)
	if stringValue(workspace["uri"]) != (&url.URL{Scheme: "file", Path: expectedWorkDir}).String() {
		t.Fatalf("local-project workspace = %#v, want %q", workspace, expectedWorkDir)
	}

	projectOnlyBody := neoSvelteKitRemoteCommandBodyForTest(t, map[string]any{
		"content":         []any{map[string]any{"type": "text", "text": "Use selected project only"}},
		"agentMode":       "smart",
		"threadID":        projectOnlyThreadID,
		"projectID":       projectID,
		"reasoningEffort": "high",
	})
	projectOnlyReq := httptest.NewRequest(http.MethodPost, "/_app/remote/3abror/createProjectThread?"+ampWebLocalInferenceAPIKeyQuery+"=local-key", strings.NewReader(projectOnlyBody))
	projectOnlyReq.Header.Set("Content-Type", "application/json")
	projectOnlyReq.Header.Set("Origin", "https://ampcode.com")
	projectOnlyReq.Header.Set(ampWebLocalInferenceHeader, "1")
	projectOnlyRec := httptest.NewRecorder()
	r.ServeHTTP(projectOnlyRec, projectOnlyReq)
	if projectOnlyRec.Code != http.StatusOK {
		t.Fatalf("project-only create status = %d, body=%s", projectOnlyRec.Code, projectOnlyRec.Body.String())
	}
	projectOnlyEnvelope := decodeSvelteKitRemoteEnvelopeForTest(t, projectOnlyRec.Body.Bytes())
	projectOnlyResult := mapValue(projectOnlyEnvelope["_"])
	if projectOnlyResult["ok"] != true || stringValue(projectOnlyResult["threadID"]) != projectOnlyThreadID {
		t.Fatalf("project-only create result = %#v", projectOnlyResult)
	}
	if projectOnlyResult["usesThreadActors"] != true || stringValue(projectOnlyResult["executorType"]) != "local-client" {
		t.Fatalf("project-only local actor response = %#v", projectOnlyResult)
	}
	if stringValue(projectOnlyResult["workingDirectory"]) != expectedWorkDir || stringValue(projectOnlyResult["workspaceRoot"]) != expectedWorkDir {
		t.Fatalf("project-only working directory response = %#v, want %q", projectOnlyResult, expectedWorkDir)
	}
	projectOnlyActor := rt.store.lookupThreadActor(projectOnlyThreadID)
	if projectOnlyActor == nil {
		t.Fatal("project-only thread actor not found")
	}
	projectOnlyActor.mu.Lock()
	projectOnlyEnvironment := cloneMap(projectOnlyActor.environment)
	projectOnlyBootstrapExecutorType := projectOnlyActor.bootstrapExecutorType
	projectOnlySpawnedCount := len(projectOnlyActor.spawnedExecutors)
	projectOnlyActor.mu.Unlock()
	if got := stringValue(projectOnlyEnvironment["workingDirectory"]); got != expectedWorkDir {
		t.Fatalf("project-only workingDirectory = %q, want %q", got, expectedWorkDir)
	}
	if projectOnlyBootstrapExecutorType != "local-client" || projectOnlySpawnedCount != 1 {
		t.Fatalf("project-only executor bootstrap = type:%q spawned:%d", projectOnlyBootstrapExecutorType, projectOnlySpawnedCount)
	}

	userActor, _ := rt.store.upsert(map[string]any{"name": "userActor", "key": "user_cloud"}, true)
	runnerSocket := &neoSocket{runnerID: "runner-local-test"}
	runnerRegistration := mapValue(userActor.handleForSocket(runnerSocket, map[string]any{
		"type": "registerRunner",
		"args": []any{map[string]any{
			"sessionId":        "session-local-test",
			"hostname":         "Local Machine",
			"workingDirectory": expectedWorkDir,
			"runningThreads":   []any{},
		}},
	}))
	if runnerRegistration["ok"] != true {
		t.Fatalf("runner registration = %#v", runnerRegistration)
	}

	runnerBody := neoSvelteKitRemoteCommandBodyForTest(t, map[string]any{
		"content":         []any{map[string]any{"type": "text", "text": "Use selected runner"}},
		"agentMode":       "smart",
		"threadID":        runnerThreadID,
		"projectID":       projectID,
		"reasoningEffort": "high",
		"runnerId":        "runner-local-test",
	})
	runnerReq := httptest.NewRequest(http.MethodPost, "/_app/remote/3abror/createProjectThread?"+ampWebLocalInferenceAPIKeyQuery+"=local-key", strings.NewReader(runnerBody))
	runnerReq.Header.Set("Content-Type", "application/json")
	runnerReq.Header.Set("Origin", "https://ampcode.com")
	runnerReq.Header.Set(ampWebLocalInferenceHeader, "1")
	runnerRec := httptest.NewRecorder()
	r.ServeHTTP(runnerRec, runnerReq)
	if runnerRec.Code != http.StatusOK {
		t.Fatalf("runner create status = %d, body=%s", runnerRec.Code, runnerRec.Body.String())
	}
	runnerEnvelope := decodeSvelteKitRemoteEnvelopeForTest(t, runnerRec.Body.Bytes())
	runnerResult := mapValue(runnerEnvelope["_"])
	if runnerResult["ok"] != true || stringValue(runnerResult["threadID"]) != runnerThreadID {
		t.Fatalf("runner create result = %#v", runnerResult)
	}
	if runnerResult["usesThreadActors"] != true || runnerResult["usesDtw"] != true || stringValue(runnerResult["executorType"]) != "local-client" {
		t.Fatalf("runner local actor response = %#v", runnerResult)
	}
	if stringValue(mapValue(runnerResult["threadActorConfig"])["threadId"]) != runnerThreadID {
		t.Fatalf("runner actor config = %#v", runnerResult["threadActorConfig"])
	}
	runnerActor := rt.store.lookupThreadActor(runnerThreadID)
	if runnerActor == nil {
		t.Fatal("runner thread actor not found")
	}
	runnerActor.mu.Lock()
	runnerMeta := cloneMap(runnerActor.meta)
	runnerBootstrapExecutorType := runnerActor.bootstrapExecutorType
	runnerSpawnedCount := len(runnerActor.spawnedExecutors)
	runnerActor.mu.Unlock()
	if stringValue(runnerMeta["runnerId"]) != "runner-local-test" {
		t.Fatalf("runner meta = %#v", runnerMeta)
	}
	if runnerBootstrapExecutorType != "local-client" || runnerSpawnedCount != 0 {
		t.Fatalf("runner executor bootstrap = type:%q spawned:%d", runnerBootstrapExecutorType, runnerSpawnedCount)
	}
	runnerHeartbeat := mapValue(userActor.handleForSocket(runnerSocket, map[string]any{
		"type": "runnerHeartbeat",
		"args": []any{map[string]any{"sessionId": "session-local-test", "runningThreads": []any{}}},
	}))
	runnerIntents := arrayValue(runnerHeartbeat["intents"])
	if runnerHeartbeat["ok"] != true || len(runnerIntents) != 1 || stringValue(mapValue(runnerIntents[0])["threadId"]) != runnerThreadID || stringValue(mapValue(runnerIntents[0])["desired"]) != "running" {
		t.Fatalf("runner heartbeat = %#v", runnerHeartbeat)
	}

	runnerOptOutBody := neoSvelteKitRemoteCommandBodyForTest(t, map[string]any{
		"content":       []any{map[string]any{"type": "text", "text": "Use selected runner without spawning"}},
		"agentMode":     "smart",
		"spawnExecutor": false,
		"threadID":      runnerOptOutThreadID,
		"runnerId":      "cliproxy-local-optout",
	})
	runnerOptOutReq := httptest.NewRequest(http.MethodPost, "/_app/remote/3abror/createProjectThread?"+ampWebLocalInferenceAPIKeyQuery+"=local-key", strings.NewReader(runnerOptOutBody))
	runnerOptOutReq.Header.Set("Content-Type", "application/json")
	runnerOptOutReq.Header.Set("Origin", "https://ampcode.com")
	runnerOptOutReq.Header.Set(ampWebLocalInferenceHeader, "1")
	runnerOptOutRec := httptest.NewRecorder()
	r.ServeHTTP(runnerOptOutRec, runnerOptOutReq)
	if runnerOptOutRec.Code != http.StatusOK {
		t.Fatalf("runner opt-out create status = %d, body=%s", runnerOptOutRec.Code, runnerOptOutRec.Body.String())
	}
	runnerOptOutEnvelope := decodeSvelteKitRemoteEnvelopeForTest(t, runnerOptOutRec.Body.Bytes())
	runnerOptOutResult := mapValue(runnerOptOutEnvelope["_"])
	if runnerOptOutResult["ok"] != false || stringValue(mapValue(runnerOptOutResult["error"])["message"]) != "selected local runner is no longer available" {
		t.Fatalf("runner opt-out create result = %#v", runnerOptOutResult)
	}
	if actor := rt.store.lookupThreadActor(runnerOptOutThreadID); actor != nil {
		t.Fatalf("unavailable runner created actor %#v", actor)
	}

	explicitFalseBody := neoSvelteKitRemoteCommandBodyForTest(t, map[string]any{
		"content":         []any{map[string]any{"type": "text", "text": "Create without spawning"}},
		"agentMode":       "smart",
		"spawnExecutor":   false,
		"threadID":        explicitFalseThreadID,
		"projectID":       projectID,
		"reasoningEffort": "high",
	})
	explicitFalseReq := httptest.NewRequest(http.MethodPost, "/_app/remote/3abror/createProjectThread?"+ampWebLocalInferenceAPIKeyQuery+"=local-key", strings.NewReader(explicitFalseBody))
	explicitFalseReq.Header.Set("Content-Type", "application/json")
	explicitFalseReq.Header.Set("Origin", "https://ampcode.com")
	explicitFalseReq.Header.Set(ampWebLocalInferenceHeader, "1")
	explicitFalseRec := httptest.NewRecorder()
	r.ServeHTTP(explicitFalseRec, explicitFalseReq)
	if explicitFalseRec.Code != http.StatusOK {
		t.Fatalf("explicit-false create status = %d, body=%s", explicitFalseRec.Code, explicitFalseRec.Body.String())
	}
	explicitFalseEnvelope := decodeSvelteKitRemoteEnvelopeForTest(t, explicitFalseRec.Body.Bytes())
	explicitFalseResult := mapValue(explicitFalseEnvelope["_"])
	if explicitFalseResult["ok"] != true || stringValue(explicitFalseResult["threadID"]) != explicitFalseThreadID {
		t.Fatalf("explicit-false create result = %#v", explicitFalseResult)
	}
	if explicitFalseResult["usesThreadActors"] != true || explicitFalseResult["usesDtw"] != true || stringValue(explicitFalseResult["executorType"]) != "" {
		t.Fatalf("explicit-false local actor response = %#v", explicitFalseResult)
	}
	if stringValue(mapValue(explicitFalseResult["threadActorConfig"])["threadId"]) != explicitFalseThreadID {
		t.Fatalf("explicit-false actor config = %#v", explicitFalseResult)
	}
	if threadData := mapValue(explicitFalseResult["threadData"]); stringValue(mapValue(threadData["threadActorConfig"])["threadId"]) != explicitFalseThreadID {
		t.Fatalf("explicit-false threadData actor config = %#v", threadData)
	}
	if stringValue(explicitFalseResult["workingDirectory"]) != expectedWorkDir || stringValue(explicitFalseResult["workspaceRoot"]) != expectedWorkDir {
		t.Fatalf("explicit-false working directory response = %#v, want %q", explicitFalseResult, expectedWorkDir)
	}
	explicitFalseActor := rt.store.lookupThreadActor(explicitFalseThreadID)
	if explicitFalseActor == nil {
		t.Fatal("explicit-false thread actor not found")
	}
	explicitFalseActor.mu.Lock()
	explicitFalseBootstrapExecutorType := explicitFalseActor.bootstrapExecutorType
	explicitFalseSpawnedCount := len(explicitFalseActor.spawnedExecutors)
	explicitFalseActor.mu.Unlock()
	if explicitFalseBootstrapExecutorType != "" || explicitFalseSpawnedCount != 0 {
		t.Fatalf("explicit-false executor bootstrap = type:%q spawned:%d", explicitFalseBootstrapExecutorType, explicitFalseSpawnedCount)
	}

	missingDirectoryBody := neoSvelteKitRemoteCommandBodyForTest(t, map[string]any{
		"content":       []any{map[string]any{"type": "text", "text": "No directory source"}},
		"spawnExecutor": true,
		"threadID":      missingDirectoryID,
	})
	missingDirectoryReq := httptest.NewRequest(http.MethodPost, "/_app/remote/3abror/createProjectThread?"+ampWebLocalInferenceAPIKeyQuery+"=local-key", strings.NewReader(missingDirectoryBody))
	missingDirectoryReq.Header.Set("Content-Type", "application/json")
	missingDirectoryReq.Header.Set("Origin", "https://ampcode.com")
	missingDirectoryReq.Header.Set(ampWebLocalInferenceHeader, "1")
	missingDirectoryRec := httptest.NewRecorder()
	r.ServeHTTP(missingDirectoryRec, missingDirectoryReq)
	if missingDirectoryRec.Code != http.StatusOK {
		t.Fatalf("missing-directory create status = %d, body=%s", missingDirectoryRec.Code, missingDirectoryRec.Body.String())
	}
	missingDirectoryEnvelope := decodeSvelteKitRemoteEnvelopeForTest(t, missingDirectoryRec.Body.Bytes())
	missingDirectoryResult := mapValue(missingDirectoryEnvelope["_"])
	if missingDirectoryResult["ok"] != false {
		t.Fatalf("missing-directory result = %#v, want ok false", missingDirectoryResult)
	}
	if actor := rt.store.lookupThreadActor(missingDirectoryID); actor != nil {
		t.Fatal("missing-directory thread actor was created")
	}
	if cloudThreadCalls != len(cloudThreadIDs) {
		t.Fatalf("cloud shell requests = %d, want %d", cloudThreadCalls, len(cloudThreadIDs))
	}
}

func TestWebLocalInferenceListUserExecutorRunnersCreatesQueryResult(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	enabled := true
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
		NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled},
	}})
	userActor, _ := rt.store.upsert(map[string]any{"name": "userActor", "key": neoLocalOwnerUserID}, true)
	userActor.handleForSocket(&neoSocket{runnerID: "runner-local"}, map[string]any{
		"type": "registerRunner",
		"args": []any{map[string]any{
			"sessionId":        "session-local",
			"hostname":         "Local Machine",
			"workingDirectory": neoDefaultWebLocalWorkingDirectory(),
			"runningThreads":   []any{},
		}},
	})
	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime:          rt,
		lastConfig: &config.AmpCode{
			WebLocalInference: config.AmpWebLocalInference{Enabled: true},
		},
	}
	auth := func(c *gin.Context) {
		token := strings.TrimSpace(c.GetHeader("Authorization"))
		token = strings.TrimSpace(strings.TrimPrefix(token, "Bearer "))
		if token != "local-key" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing auth"})
			return
		}
		c.Set("userApiKey", token)
		c.Next()
	}
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, auth)

	req := httptest.NewRequest(http.MethodGet, "/_app/remote/3abror/listUserExecutorRunners?"+ampWebLocalInferenceAPIKeyQuery+"=local-key", nil)
	req.Header.Set("Origin", "https://ampcode.com")
	req.Header.Set(ampWebLocalInferenceHeader, "1")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("runner query status = %d, body=%s", rec.Code, rec.Body.String())
	}
	envelope := decodeSvelteKitRemoteEnvelopeForTest(t, rec.Body.Bytes())
	queryResults := mapValue(envelope["q"])
	node := mapValue(queryResults["3abror/listUserExecutorRunners/"])
	runners := arrayValue(node["v"])
	if len(runners) != 1 {
		t.Fatalf("runner query = %#v", envelope)
	}
	runner := mapValue(runners[0])
	if stringValue(runner["runnerId"]) == "" || stringValue(runner["hostname"]) == "" {
		t.Fatalf("runner identity = %#v", runner)
	}
	if got, want := stringValue(runner["workingDirectory"]), neoDefaultWebLocalWorkingDirectory(); got != want {
		t.Fatalf("runner workingDirectory = %q, want %q", got, want)
	}
}

func TestWebLocalInferenceRemotePrewarmRejectsMalformedPayload(t *testing.T) {
	gin.SetMode(gin.TestMode)
	dataDir := t.TempDir()
	oldStoreDir := neoAmpDataDir
	neoAmpDataDir = func() string { return dataDir }
	t.Cleanup(func() { neoAmpDataDir = oldStoreDir })
	r := gin.New()
	enabled := true
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
		NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled},
	}})
	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime:          rt,
		lastConfig: &config.AmpCode{
			WebLocalInference: config.AmpWebLocalInference{Enabled: true},
		},
	}
	auth := func(c *gin.Context) {
		token := strings.TrimSpace(c.GetHeader("Authorization"))
		token = strings.TrimSpace(strings.TrimPrefix(token, "Bearer "))
		if token != "local-key" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing auth"})
			return
		}
		c.Set("userApiKey", token)
		c.Next()
	}
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, auth)

	req := httptest.NewRequest(http.MethodPost, "/_app/remote/3abror/prewarmProjectThread?"+ampWebLocalInferenceAPIKeyQuery+"=local-key", strings.NewReader(`{"payload":"%%%","refreshes":[]}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", "https://ampcode.com")
	req.Header.Set(ampWebLocalInferenceHeader, "1")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("prewarm status = %d, body=%s", rec.Code, rec.Body.String())
	}

	envelope := decodeSvelteKitRemoteEnvelopeForTest(t, rec.Body.Bytes())
	result := mapValue(envelope["_"])
	if result["ok"] != false {
		t.Fatalf("prewarm result = %#v, want ok false", result)
	}
	errorBody := mapValue(result["error"])
	if !strings.Contains(stringValue(errorBody["message"]), "invalid SvelteKit remote payload encoding") {
		t.Fatalf("error = %#v", errorBody)
	}
}

func TestWebLocalInferenceRemoteThreadMutationsUseLocalActor(t *testing.T) {
	gin.SetMode(gin.TestMode)
	dataDir := t.TempDir()
	oldStoreDir := neoAmpDataDir
	neoAmpDataDir = func() string { return dataDir }
	t.Cleanup(func() { neoAmpDataDir = oldStoreDir })
	r := gin.New()
	enabled := true
	cloudThreadID := "T-019f6f35-9af2-7369-b23d-a4d3368960fd"
	cloudRequests := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/internal" && r.URL.RawQuery == "getUserInfo" {
			writeNeoJSON(w, http.StatusOK, map[string]any{"ok": true, "result": map[string]any{"id": "user_local_owner"}})
			return
		}
		if r.URL.Path != "/api/internal" || r.URL.RawQuery != "getThread" {
			t.Fatalf("unexpected upstream request path=%s query=%s", r.URL.Path, r.URL.RawQuery)
		}
		if r.Header.Get("Authorization") != "Bearer secret" {
			t.Fatalf("upstream Authorization = %q", r.Header.Get("Authorization"))
		}
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode getThread request: %v", err)
		}
		requestedThreadID := firstNonEmptyString(mapValue(payload["params"])["thread"])
		if requestedThreadID != cloudThreadID {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		cloudRequests++
		writeNeoJSON(w, http.StatusOK, map[string]any{"ok": true, "result": map[string]any{"thread": map[string]any{
			"id": cloudThreadID,
			"v":  3,
			"meta": map[string]any{
				"cliProxyAPILocalNeo": true,
				"ownerUserId":         "user_local_owner",
			},
			"data": map[string]any{
				"id":        cloudThreadID,
				"title":     "Cloud local thread",
				"agentMode": "smart",
				"messages": []any{map[string]any{
					"messageId": "M-cloud-local-user",
					"role":      "user",
					"content":   []any{map[string]any{"type": "text", "text": "restore before mutation"}},
				}},
			},
		}}})
	}))
	t.Cleanup(upstream.Close)
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
		UpstreamURL:     upstream.URL,
		UpstreamAPIKey:  "secret",
		NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled},
	}})
	t.Cleanup(func() { rt.store.disposeAll(true, "test done", false) })
	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime:          rt,
		lastConfig: &config.AmpCode{
			WebLocalInference: config.AmpWebLocalInference{Enabled: true},
		},
	}
	auth := func(c *gin.Context) {
		token := strings.TrimSpace(c.GetHeader("Authorization"))
		token = strings.TrimSpace(strings.TrimPrefix(token, "Bearer "))
		if token != "local-key" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing auth"})
			return
		}
		c.Set("userApiKey", token)
		c.Next()
	}
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, auth)

	threadID := "T-019f6f35-9af2-7369-b23d-a4d3368960fc"
	if _, status := rt.localThreadActorManagementResponse(context.Background(), map[string]any{
		"threadId":         threadID,
		"workingDirectory": t.TempDir(),
		"agentMode":        "smart",
		"threadMeta":       map[string]any{"cliProxyAPILocalNeo": true, "ownerUserId": "user_local_owner"},
	}, ""); status != http.StatusOK {
		t.Fatalf("create local actor status = %d", status)
	}

	mutateResponse := func(endpoint string, value map[string]any) *httptest.ResponseRecorder {
		t.Helper()
		body := neoSvelteKitRemoteCommandBodyForTest(t, value)
		req := httptest.NewRequest(http.MethodPost, "/_app/remote/145jw2/"+endpoint+"?"+ampWebLocalInferenceAPIKeyQuery+"=local-key", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Origin", "https://ampcode.com")
		req.Header.Set(ampWebLocalInferenceHeader, "1")
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)
		return rec
	}
	mutate := func(endpoint string, value map[string]any) map[string]any {
		t.Helper()
		rec := mutateResponse(endpoint, value)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s status = %d, body=%s", endpoint, rec.Code, rec.Body.String())
		}
		return mapValue(decodeSvelteKitRemoteEnvelopeForTest(t, rec.Body.Bytes())["_"])
	}

	if result := mutate("archiveThreadCommand", map[string]any{"threadID": threadID, "archived": true}); result["ok"] != true {
		t.Fatalf("archive result = %#v", result)
	}
	if result := mutate("pinThreadCommand", map[string]any{"threadID": threadID, "pinned": true}); result["ok"] != true {
		t.Fatalf("pin result = %#v", result)
	}
	addLabelRec := mutateResponse("addThreadLabel", map[string]any{"threadID": threadID, "label": "shipping"})
	if addLabelRec.Code != http.StatusOK {
		t.Fatalf("add label status = %d, body=%s", addLabelRec.Code, addLabelRec.Body.String())
	}
	addedLabels := arrayValue(decodeSvelteKitRemoteEnvelopeForTest(t, addLabelRec.Body.Bytes())["_"])
	if len(addedLabels) != 1 || stringValue(mapValue(addedLabels[0])["name"]) != "shipping" {
		t.Fatalf("added labels = %#v", addedLabels)
	}
	removeLabelRec := mutateResponse("removeThreadLabel", map[string]any{"threadID": threadID, "label": "shipping"})
	if removeLabelRec.Code != http.StatusOK {
		t.Fatalf("remove label status = %d, body=%s", removeLabelRec.Code, removeLabelRec.Body.String())
	}
	if removedLabels := arrayValue(decodeSvelteKitRemoteEnvelopeForTest(t, removeLabelRec.Body.Bytes())["_"]); len(removedLabels) != 0 {
		t.Fatalf("removed labels = %#v, want empty", removedLabels)
	}
	actor := rt.store.lookupThreadActor(threadID)
	if actor == nil {
		t.Fatal("local actor missing after remote mutations")
	}
	actor.mu.Lock()
	actor.messages = []neoMessage{{
		ThreadID:  threadID,
		MessageID: "M-0000000000000000000001",
		Role:      "assistant",
		CreatedAt: "2026-07-19T01:00:00Z",
		ReadAt:    "2026-07-19T01:01:00Z",
		Seq:       1,
	}}
	actor.seq = 2
	actor.replayEvents = nil
	actor.mu.Unlock()
	if result := mutate("markThreadUnreadCommand", map[string]any{"threadID": threadID}); result["ok"] != true {
		t.Fatalf("mark unread result = %#v", result)
	}
	state := actor.stateSnapshotResponse()
	if state["archived"] != true || state["pinned"] != true || state["hasUnreadMessages"] != true {
		t.Fatalf("local mutation state = archived:%#v pinned:%#v unread:%#v", state["archived"], state["pinned"], state["hasUnreadMessages"])
	}
	summary := rt.neoWebLocalThreadSummary(threadID)
	if summary["archived"] != true || summary["pinned"] != true || summary["hasUnreadMessages"] != true || summary["latestAssistantMessageID"] != "M-0000000000000000000001" {
		t.Fatalf("local mutation summary = archived:%#v pinned:%#v unread:%#v latest:%#v", summary["archived"], summary["pinned"], summary["hasUnreadMessages"], summary["latestAssistantMessageID"])
	}
	if summary[neoLocalPinnedOverrideKey] != true {
		t.Fatalf("local pin override = %#v, want true", summary[neoLocalPinnedOverrideKey])
	}
	actor.mu.Lock()
	if actor.messages[0].ReadAt != "" || actor.seq != 2 || len(actor.replayEvents) != 0 {
		t.Fatalf("mark unread command emitted a visible message update: readAt=%q seq=%d replay=%d", actor.messages[0].ReadAt, actor.seq, len(actor.replayEvents))
	}
	actor.mu.Unlock()
	actor.handle(map[string]any{"type": "client_mark_message_read", "messageId": "M-0000000000000000000001"})
	if state = actor.stateSnapshotResponse(); state["hasUnreadMessages"] != false {
		t.Fatalf("explicit message read did not clear command unread state: %#v", state["hasUnreadMessages"])
	}
	if result := mutate("pinThreadCommand", map[string]any{"threadID": threadID, "pinned": false}); result["ok"] != true {
		t.Fatalf("unpin result = %#v", result)
	}
	state = actor.stateSnapshotResponse()
	summary = rt.neoWebLocalThreadSummary(threadID)
	if state["pinned"] != false || summary["pinned"] != false {
		t.Fatalf("local unpin state = state:%#v summary:%#v", state["pinned"], summary["pinned"])
	}
	if summary[neoLocalPinnedOverrideKey] != false {
		t.Fatalf("local unpin override = %#v, want false", summary[neoLocalPinnedOverrideKey])
	}
	if result := mutate("pinThreadCommand", map[string]any{"threadID": cloudThreadID, "pinned": true}); result["ok"] != true {
		t.Fatalf("cloud local pin result = %#v", result)
	}
	cloudActor := rt.store.lookupThreadActor(cloudThreadID)
	if cloudRequests != 1 || cloudActor == nil || !cloudActor.hasLocalThreadState() || cloudActor.stateSnapshotResponse()["pinned"] != true {
		t.Fatalf("cloud local import = requests:%d actor:%#v", cloudRequests, cloudActor)
	}
	for _, test := range []struct {
		endpoint string
		request  map[string]any
		error    string
	}{
		{endpoint: "archiveThreadCommand", request: map[string]any{"threadID": threadID}, error: "invalid archived value"},
		{endpoint: "pinThreadCommand", request: map[string]any{"threadID": threadID, "pinned": "true"}, error: "invalid pinned value"},
	} {
		rec := mutateResponse(test.endpoint, test.request)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s invalid value status = %d, body=%s", test.endpoint, rec.Code, rec.Body.String())
		}
		var envelope map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &envelope); err != nil {
			t.Fatalf("decode %s invalid value error: %v", test.endpoint, err)
		}
		if envelope["type"] != "error" || stringValue(envelope["error"]) != test.error {
			t.Fatalf("%s invalid value error = %#v", test.endpoint, envelope)
		}
	}
	state = actor.stateSnapshotResponse()
	if state["archived"] != true || state["pinned"] != false {
		t.Fatalf("invalid local mutations changed state = archived:%#v pinned:%#v", state["archived"], state["pinned"])
	}

	missingBody := neoSvelteKitRemoteCommandBodyForTest(t, map[string]any{
		"threadID": "T-019f6551-9d97-73b2-ab56-d3967bce6e99",
		"archived": true,
	})
	missingReq := httptest.NewRequest(http.MethodPost, "/_app/remote/145jw2/archiveThreadCommand?"+ampWebLocalInferenceAPIKeyQuery+"=local-key", strings.NewReader(missingBody))
	missingReq.Header.Set("Content-Type", "application/json")
	missingReq.Header.Set("Origin", "https://ampcode.com")
	missingReq.Header.Set(ampWebLocalInferenceHeader, "1")
	missingRec := httptest.NewRecorder()
	r.ServeHTTP(missingRec, missingReq)
	if missingRec.Code != http.StatusNotFound {
		t.Fatalf("missing archive status = %d, body=%s", missingRec.Code, missingRec.Body.String())
	}
	var missingEnvelope map[string]any
	if err := json.Unmarshal(missingRec.Body.Bytes(), &missingEnvelope); err != nil {
		t.Fatalf("decode missing archive error: %v", err)
	}
	if missingEnvelope["type"] != "error" || numberFrom(missingEnvelope["status"]) != http.StatusNotFound || stringValue(missingEnvelope["error"]) != "local thread not found" {
		t.Fatalf("missing archive error = %#v", missingEnvelope)
	}

	if result := mutate("deleteThreadCommand", map[string]any{"threadID": threadID}); result["ok"] != true {
		t.Fatalf("delete result = %#v", result)
	}
	if rt.store.lookupThreadActor(threadID) != nil || !rt.neoThreadDeleted(threadID) || rt.neoWebLocalThreadSummary(threadID) != nil {
		t.Fatalf("deleted local thread remained available: actor=%#v tombstoned=%t summary=%#v", rt.store.lookupThreadActor(threadID), rt.neoThreadDeleted(threadID), rt.neoWebLocalThreadSummary(threadID))
	}
}

func TestWebLocalInferenceProjectIndexIncludesHistoryProjects(t *testing.T) {
	allowTestTemporaryProjectDirectories(t)
	dataDir := t.TempDir()
	oldStoreDir := neoAmpDataDir
	neoAmpDataDir = func() string { return dataDir }
	t.Cleanup(func() { neoAmpDataDir = oldStoreDir })
	enabled := true
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
		NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled},
	}})

	existingDir := neoExistingDirectory(t.TempDir())
	existingID := neoDeterministicLocalProjectID("existing", neoFileURLForDirectory(existingDir), existingDir)
	rootProjectID := "75616c3b-f4de-48b7-8b83-c1af6978a034"
	if err := writeNeoWebLocalProjectIndex(rt.threadDir, []any{map[string]any{
		"id":               existingID,
		"name":             "existing",
		"repositoryURL":    neoFileURLForDirectory(existingDir),
		"workingDirectory": existingDir,
	}, map[string]any{
		"id":               rootProjectID,
		"name":             "local",
		"repositoryURL":    "file:///",
		"workingDirectory": string(filepath.Separator),
	}}); err != nil {
		t.Fatalf("write project index: %v", err)
	}
	if projects := rt.neoWebLocalProjectCache(); len(projects) != 1 {
		t.Fatalf("initial projects = %#v, want existing project only", projects)
	}

	workDir := t.TempDir()
	historyLine, err := json.Marshal(map[string]any{"text": "new local thread", "cwd": workDir})
	if err != nil {
		t.Fatalf("marshal history: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "history.jsonl"), append(historyLine, '\n'), 0o600); err != nil {
		t.Fatalf("write history: %v", err)
	}
	rootHistoryLine, err := json.Marshal(map[string]any{"text": "root local thread", "cwd": string(filepath.Separator)})
	if err != nil {
		t.Fatalf("marshal root history: %v", err)
	}
	historyFile, err := os.OpenFile(filepath.Join(dataDir, "history.jsonl"), os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("open history: %v", err)
	}
	if _, err := historyFile.Write(append(rootHistoryLine, '\n')); err != nil {
		_ = historyFile.Close()
		t.Fatalf("append root history: %v", err)
	}
	if err := historyFile.Close(); err != nil {
		t.Fatalf("close history: %v", err)
	}

	expectedWorkDir := neoExistingDirectory(workDir)
	expectedID := neoDeterministicLocalProjectID(filepath.Base(expectedWorkDir), neoFileURLForDirectory(expectedWorkDir), expectedWorkDir)
	if got := rt.neoWebLocalProjectWorkingDirectory(expectedID); got != expectedWorkDir {
		t.Fatalf("project workingDirectory = %q, want %q", got, expectedWorkDir)
	}
	if got := rt.neoWebLocalProjectWorkingDirectory(rootProjectID); got != "" {
		t.Fatalf("root project workingDirectory = %q, want empty", got)
	}
	projects := rt.neoWebLocalProjectCache()
	if len(projects) != 2 {
		t.Fatalf("projects = %#v, want existing and history projects", projects)
	}
	projectsByID := map[string]map[string]any{}
	for _, rawProject := range projects {
		project := mapValue(rawProject)
		projectsByID[stringValue(project["id"])] = project
	}
	project := projectsByID[expectedID]
	if stringValue(project["id"]) != expectedID || stringValue(project["workingDirectory"]) != expectedWorkDir || stringValue(project["name"]) != filepath.Base(expectedWorkDir) {
		t.Fatalf("project = %#v, want id=%q workingDirectory=%q", project, expectedID, expectedWorkDir)
	}
	if got := rt.neoWebLocalProjectWorkingDirectory(existingID); got != existingDir {
		t.Fatalf("existing project workingDirectory = %q, want %q", got, existingDir)
	}
	if _, err := os.Stat(neoWebLocalProjectIndexPath(rt.threadDir)); err != nil {
		t.Fatalf("project index was not written: %v", err)
	}
}

func TestNeoWebLocalFilesystemRoot(t *testing.T) {
	root := filepath.VolumeName(os.TempDir()) + string(filepath.Separator)
	if !neoWebLocalFilesystemRoot(root) {
		t.Fatalf("neoWebLocalFilesystemRoot(%q) = false, want true", root)
	}
	if neoWebLocalFilesystemRoot(t.TempDir()) {
		t.Fatal("temporary directory was treated as filesystem root")
	}
	if got := neoWebLocalProjectDirectory(root); got != "" {
		t.Fatalf("root project directory = %q, want empty", got)
	}
}

func neoSvelteKitRemoteCommandBodyForTest(t *testing.T, value any) string {
	t.Helper()
	data, err := neoSvelteKitDevalueString(value)
	if err != nil {
		t.Fatalf("encode devalue: %v", err)
	}
	raw := base64.StdEncoding.EncodeToString([]byte(data))
	body, err := json.Marshal(map[string]any{"payload": raw, "refreshes": []any{}})
	if err != nil {
		t.Fatalf("marshal remote body: %v", err)
	}
	return string(body)
}

func decodeSvelteKitRemoteEnvelopeForTest(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var envelope map[string]any
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatalf("decode remote envelope JSON: %v", err)
	}
	if envelope["type"] != "result" {
		t.Fatalf("remote envelope = %#v", envelope)
	}
	var values []any
	if err := json.Unmarshal([]byte(stringValue(envelope["data"])), &values); err != nil {
		t.Fatalf("decode remote data devalue JSON: %v", err)
	}
	decoded, ok := neoDecodeSvelteKitDevalueIndex(values, 0, map[int]bool{})
	if !ok {
		t.Fatalf("decode remote data failed: %#v", values)
	}
	return mapValue(decoded)
}

func TestRegisterManagementRoutesLocalNeoThreadActorsWithoutProxy(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	enabled := true
	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime: newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
			NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled},
		}}),
	}

	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	body := bytes.NewBufferString(`{"agentMode":"deep","usesThreadActors":true}`)
	req := httptest.NewRequest(http.MethodPost, "/api/thread-actors", body)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	var response map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("response JSON error: %v", err)
	}
	threadID := stringValue(response["threadId"])
	requireNeoBinaryV7ThreadID(t, threadID)
	if stringValue(response["wsToken"]) == "" || stringValue(response["ownerUserId"]) == "" || numberFrom(response["threadVersion"]) == 0 {
		t.Fatalf("missing required thread actor fields: %#v", response)
	}
	if response["usesDtw"] != true || response["usesThreadActors"] != true || response["agentMode"] != "deep" {
		t.Fatalf("unexpected thread actor flags: %#v", response)
	}
}

func TestRegisterManagementRoutesDoesNotSynthesizeUnknownNeoThreadActorWithoutProxy(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	enabled := true
	dir := t.TempDir()
	oldStoreDir := neoAmpDataDir
	neoAmpDataDir = func() string { return dir }
	t.Cleanup(func() { neoAmpDataDir = oldStoreDir })

	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime: newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
			NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled},
		}}),
	}

	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	threadID := "T-019e65c0-0310-77a8-b233-4b84d9c0612b"
	req := httptest.NewRequest(http.MethodPost, "/api/thread-actors/"+threadID, bytes.NewBufferString(`{}`))
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	m.neoRuntime.store.mu.RLock()
	defer m.neoRuntime.store.mu.RUnlock()
	for _, actor := range m.neoRuntime.store.actors {
		if actor.threadID == threadID || actor.key == threadID {
			t.Fatalf("unknown thread actor was synthesized locally: %#v", actor.record)
		}
	}
}

func TestRegisterManagementRoutesServesExistingNeoThreadActorLocallyWithoutProxy(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	enabled := true

	threadID := "T-019e65c0-0310-77a8-b233-4b84d9c0612c"

	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime: newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
			NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled, ForceThreadActors: true},
		}}),
	}

	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	req := httptest.NewRequest(http.MethodPost, "/api/thread-actors/"+threadID, bytes.NewBufferString(`{"agentMode":"deep"}`))
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	var response map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("response JSON error: %v", err)
	}
	if response["agentMode"] != "deep" || stringValue(response["wsToken"]) == "" {
		t.Fatalf("unexpected local thread actor response: %#v", response)
	}
}

func TestRegisterManagementRoutesLocalNeoThreadActorsReturnAuthenticatedWsToken(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	enabled := true
	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime: newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
			NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled},
		}}),
	}
	auth := func(c *gin.Context) {
		token := strings.TrimSpace(c.GetHeader("Authorization"))
		token = strings.TrimSpace(strings.TrimPrefix(token, "Bearer "))
		if token != "amp-local-key" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing auth"})
			return
		}
		c.Set("userApiKey", token)
		c.Next()
	}

	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, auth)

	body := bytes.NewBufferString(`{"agentMode":"deep","usesThreadActors":true}`)
	req := httptest.NewRequest(http.MethodPost, "/api/thread-actors", body)
	req.Header.Set("Authorization", "Bearer amp-local-key")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	var response map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("response JSON error: %v", err)
	}
	if got := stringValue(response["wsToken"]); got != "amp-local-key" {
		t.Fatalf("wsToken = %q, want authenticated client key", got)
	}
}

func TestRegisterManagementRoutesNeoRuntimeBridgeUsesManagementAuth(t *testing.T) {
	gin.SetMode(gin.TestMode)

	var runtimeRequests int
	var sawAuthToken bool
	var sawAuthorization bool
	var sawRivetHeader bool
	var sawRivetToken bool
	var sawRivetSubprotocol bool
	var sawRivetEncoding bool
	var sawRivetKey bool
	var sawMetadataPath bool
	var sawForwardedHost bool
	var sawForwardedProto bool
	var sawInternalClientKey bool
	runtimeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		runtimeRequests++
		sawAuthToken = r.URL.Query().Get("auth_token") != ""
		sawAuthorization = r.Header.Get("Authorization") != ""
		sawRivetHeader = r.Header.Get("X-Rivet-Token") != ""
		sawRivetToken = r.URL.Query().Get("rvt-token") != ""
		sawInternalClientKey = sawInternalClientKey || r.Header.Get(neoInternalClientAPIKeyHeader) != ""
		if r.Header.Get("X-Forwarded-Host") == "127.0.0.1:8333" {
			sawForwardedHost = true
		}
		if r.Header.Get("X-Forwarded-Proto") == "http" {
			sawForwardedProto = true
		}
		for _, header := range r.Header.Values("Sec-WebSocket-Protocol") {
			if strings.Contains(header, "rivet_token.") {
				sawRivetSubprotocol = true
			}
			if strings.Contains(header, "rivet_encoding.json") {
				sawRivetEncoding = true
			}
		}
		sawRivetKey = r.URL.Query().Get("rvt-key") == "T-bridge"
		if r.URL.Path == "/metadata" {
			sawMetadataPath = true
		}
		writeNeoJSON(w, http.StatusOK, map[string]any{"ok": true})
	}))
	defer runtimeServer.Close()

	parsed, err := url.Parse(runtimeServer.URL)
	if err != nil {
		t.Fatalf("parse runtime URL: %v", err)
	}
	host, portText, err := net.SplitHostPort(parsed.Host)
	if err != nil {
		t.Fatalf("split runtime host: %v", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("parse runtime port: %v", err)
	}

	r := gin.New()
	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime:          &neoRuntime{host: host, port: port},
	}
	auth := func(c *gin.Context) {
		token := c.Query("auth_token")
		if token == "" {
			token = c.Query("rvt-token")
		}
		if token == "" {
			token = strings.TrimSpace(c.GetHeader("X-Rivet-Token"))
		}
		if token == "" {
			token = strings.TrimSpace(c.GetHeader("Authorization"))
			token = strings.TrimSpace(strings.TrimPrefix(token, "Bearer "))
		}
		if token != "local-key" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing auth"})
			return
		}
		c.Set("userApiKey", "local-key")
		c.Next()
	}
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, auth)
	server := httptest.NewServer(r)
	defer server.Close()

	unauthResp, err := http.Get(server.URL + "/gateway/threadActor/?rvt-method=getOrCreate&rvt-key=T-bridge")
	if err != nil {
		t.Fatalf("unauth request: %v", err)
	}
	defer unauthResp.Body.Close()
	if unauthResp.StatusCode != http.StatusUnauthorized {
		body, _ := io.ReadAll(unauthResp.Body)
		t.Fatalf("unauth status = %d, body=%s", unauthResp.StatusCode, body)
	}
	if runtimeRequests != 0 {
		t.Fatalf("runtime saw unauthenticated request")
	}

	authReq, err := http.NewRequest(http.MethodGet, server.URL+"/gateway/threadActor/?rvt-method=getOrCreate&rvt-key=T-bridge&rvt-token=local-key", nil)
	if err != nil {
		t.Fatalf("auth request build: %v", err)
	}
	authReq.Host = "127.0.0.1:8333"
	authReq.Header.Set("Authorization", "Bearer local-key")
	authReq.Header.Set("X-Rivet-Token", "local-key")
	authReq.Header.Set(neoInternalClientAPIKeyHeader, "spoofed-key")
	authReq.Header.Set("Sec-WebSocket-Protocol", "rivet, rivet_token.local-key, rivet_encoding.json")
	authResp, err := http.DefaultClient.Do(authReq)
	if err != nil {
		t.Fatalf("auth request: %v", err)
	}
	defer authResp.Body.Close()
	if authResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(authResp.Body)
		t.Fatalf("auth status = %d, body=%s", authResp.StatusCode, body)
	}
	if runtimeRequests != 1 {
		t.Fatalf("runtimeRequests = %d, want 1", runtimeRequests)
	}
	if sawAuthToken {
		t.Fatalf("auth_token leaked to runtime")
	}
	if sawAuthorization {
		t.Fatalf("Authorization leaked to runtime")
	}
	if sawRivetHeader {
		t.Fatalf("X-Rivet-Token leaked to runtime")
	}
	if sawRivetToken {
		t.Fatalf("rvt-token leaked to runtime")
	}
	if sawRivetSubprotocol {
		t.Fatalf("rivet_token subprotocol leaked to runtime")
	}
	if sawInternalClientKey {
		t.Fatalf("%s leaked to runtime", neoInternalClientAPIKeyHeader)
	}
	if !sawRivetEncoding {
		t.Fatalf("non-credential websocket subprotocol was stripped")
	}
	if !sawRivetKey {
		t.Fatalf("rvt-key did not reach runtime")
	}
	if !sawForwardedHost {
		t.Fatalf("X-Forwarded-Host did not preserve public bridge host")
	}
	if !sawForwardedProto {
		t.Fatalf("X-Forwarded-Proto did not preserve public bridge scheme")
	}

	metadataReq, err := http.NewRequest(http.MethodGet, server.URL+"/metadata", nil)
	if err != nil {
		t.Fatalf("metadata request build: %v", err)
	}
	metadataReq.Header.Set("Authorization", "Bearer local-key")
	metadataResp, err := http.DefaultClient.Do(metadataReq)
	if err != nil {
		t.Fatalf("metadata request: %v", err)
	}
	defer metadataResp.Body.Close()
	if metadataResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(metadataResp.Body)
		t.Fatalf("metadata status = %d, body=%s", metadataResp.StatusCode, body)
	}
	if !sawMetadataPath {
		t.Fatalf("metadata route did not reach runtime")
	}
}

func TestNeoThreadIDFromBridgeRequestDoesNotGenerateIDFromInput(t *testing.T) {
	encoded := func(value any) string {
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatalf("marshal gateway input: %v", err)
		}
		return base64.RawURLEncoding.EncodeToString(raw)
	}

	req := httptest.NewRequest(http.MethodGet, "/gateway/threadActor/?rvt-input="+url.QueryEscape(encoded(map[string]any{"input": map[string]any{"message": "no thread here"}})), nil)
	if got := neoThreadIDFromBridgeRequest(req); got != "" {
		t.Fatalf("thread id = %q, want none", got)
	}

	threadID := "T-019e65c0-0310-77a8-b233-4b84d9c0612f"
	req = httptest.NewRequest(http.MethodGet, "/gateway/threadActor/?rvt-input="+url.QueryEscape(encoded(map[string]any{"input": map[string]any{"threadID": threadID}})), nil)
	if got := neoThreadIDFromBridgeRequest(req); got != threadID {
		t.Fatalf("thread id = %q, want %q", got, threadID)
	}
}

func TestRegisterManagementRoutesProxiesNeoRuntimeBridgeWhenRuntimeDisabled(t *testing.T) {
	gin.SetMode(gin.TestMode)

	upstreamRequests := 0
	seenPaths := map[string]int{}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests++
		seenPaths[r.URL.Path]++
		writeNeoJSON(w, http.StatusOK, map[string]any{"source": "upstream"})
	}))
	defer upstream.Close()

	r := gin.New()
	m := &AmpModule{restrictToLocalhost: false}
	proxy, err := createReverseProxy(upstream.URL, NewStaticSecretSource(""))
	if err != nil {
		t.Fatalf("create proxy: %v", err)
	}
	m.setProxy(proxy)
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	localServer := httptest.NewServer(r)
	defer localServer.Close()

	paths := []string{
		"/metadata",
		"/gateway/threadActor/?rvt-method=get&rvt-key=T-upstream",
		"/gateway/actor-local/request/state",
		"/gateway/actor-local/request/messages",
		"/actors?name=threadActor&key=T-upstream",
		"/actors/actor-local/kv/keys/state",
		"/actors/actor-local/skills",
	}
	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			resp, err := http.Get(localServer.URL + path)
			if err != nil {
				t.Fatalf("bridge request: %v", err)
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("read bridge response: %v", err)
			}

			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, body=%s", resp.StatusCode, string(body))
			}
			var response map[string]any
			if err := json.Unmarshal(body, &response); err != nil {
				t.Fatalf("response JSON error: %v", err)
			}
			if response["source"] != "upstream" {
				t.Fatalf("unexpected response: %#v", response)
			}
		})
	}
	if upstreamRequests != len(paths) {
		t.Fatalf("upstreamRequests = %d, want %d", upstreamRequests, len(paths))
	}
	for _, path := range []string{"/metadata", "/gateway/threadActor/", "/actors"} {
		if seenPaths[path] != 1 {
			t.Fatalf("upstream path %s count = %d, want 1; seen=%#v", path, seenPaths[path], seenPaths)
		}
	}
}

func TestRegisterManagementRoutesServesAmpBinaryNeoRuntimeBridgeLocally(t *testing.T) {
	gin.SetMode(gin.TestMode)

	runtimeRequests := 0
	runtimeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		runtimeRequests++
		writeNeoJSON(w, http.StatusOK, map[string]any{"source": "runtime"})
	}))
	defer runtimeServer.Close()
	runtimeURL, err := url.Parse(runtimeServer.URL)
	if err != nil {
		t.Fatalf("parse runtime URL: %v", err)
	}
	host, portText, err := net.SplitHostPort(runtimeURL.Host)
	if err != nil {
		t.Fatalf("split runtime host: %v", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("parse runtime port: %v", err)
	}

	upstreamRequests := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests++
		writeNeoJSON(w, http.StatusOK, map[string]any{"source": "upstream"})
	}))
	defer upstream.Close()

	r := gin.New()
	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime:          &neoRuntime{host: host, port: port},
	}
	proxy, err := createReverseProxy(upstream.URL, NewStaticSecretSource(""))
	if err != nil {
		t.Fatalf("create proxy: %v", err)
	}
	m.setProxy(proxy)
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	localServer := httptest.NewServer(r)
	defer localServer.Close()

	paths := []string{
		"/metadata",
		"/gateway/threadActor/?rvt-method=get&rvt-key=T-upstream",
		"/actors?name=threadActor&key=T-upstream",
	}
	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, localServer.URL+path, nil)
			if err != nil {
				t.Fatalf("build amp binary bridge request: %v", err)
			}
			req.Header.Set("User-Agent", "RivetKit/2.3.0-rc.9 Bun/1.3.14")
			if strings.HasPrefix(path, "/gateway/") {
				req.Header.Set("User-Agent", "undici")
				req.Header.Set("Sec-WebSocket-Protocol", "rivet, rivet_encoding.bare, rivet_skip_ready_wait")
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("amp binary bridge request: %v", err)
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("read amp binary bridge response: %v", err)
			}

			if resp.StatusCode != http.StatusOK {
				t.Fatalf("amp binary bridge status = %d, body=%s", resp.StatusCode, string(body))
			}
			var response map[string]any
			if err := json.Unmarshal(body, &response); err != nil {
				t.Fatalf("amp binary bridge response JSON error: %v", err)
			}
			if response["source"] != "runtime" {
				t.Fatalf("unexpected amp binary bridge response: %#v", response)
			}
		})
	}
	if upstreamRequests != 0 {
		t.Fatalf("upstreamRequests = %d, want 0", upstreamRequests)
	}
	if runtimeRequests != len(paths) {
		t.Fatalf("runtimeRequests = %d, want %d", runtimeRequests, len(paths))
	}

	resp, err := http.Get(localServer.URL + "/metadata?cliproxy-client=neo-remote-ui")
	if err != nil {
		t.Fatalf("local bridge request: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read local bridge response: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("local bridge status = %d, body=%s", resp.StatusCode, string(body))
	}
	if upstreamRequests != 0 {
		t.Fatalf("local bridge should not call upstream; upstreamRequests = %d", upstreamRequests)
	}
	if runtimeRequests != len(paths)+1 {
		t.Fatalf("runtimeRequests = %d, want %d", runtimeRequests, len(paths)+1)
	}
	var response map[string]any
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatalf("local bridge response JSON error: %v", err)
	}
	if response["source"] != "runtime" {
		t.Fatalf("unexpected local bridge response: %#v", response)
	}
}

func TestRegisterManagementRoutesPassesUnknownNeoRuntimeBridgePathsUpstreamWithProxy(t *testing.T) {
	gin.SetMode(gin.TestMode)

	runtimeRequests := 0
	runtimeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		runtimeRequests++
		writeNeoJSON(w, http.StatusOK, map[string]any{"source": "runtime"})
	}))
	defer runtimeServer.Close()
	runtimeURL, err := url.Parse(runtimeServer.URL)
	if err != nil {
		t.Fatalf("parse runtime URL: %v", err)
	}
	host, portText, err := net.SplitHostPort(runtimeURL.Host)
	if err != nil {
		t.Fatalf("split runtime host: %v", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("parse runtime port: %v", err)
	}

	upstreamRequests := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests++
		writeNeoJSON(w, http.StatusOK, map[string]any{"source": "upstream"})
	}))
	defer upstream.Close()

	r := gin.New()
	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime:          &neoRuntime{host: host, port: port},
	}
	proxy, err := createReverseProxy(upstream.URL, NewStaticSecretSource(""))
	if err != nil {
		t.Fatalf("create proxy: %v", err)
	}
	m.setProxy(proxy)
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	localServer := httptest.NewServer(r)
	defer localServer.Close()

	for _, tc := range []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/gateway"},
		{http.MethodGet, "/gateway/static/asset.js"},
		{http.MethodGet, "/gateway/threadActor/unhandled"},
		{http.MethodPost, "/gateway/threadActor/request/state"},
		{http.MethodGet, "/actors/actor-local/unhandled"},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			req, err := http.NewRequest(tc.method, localServer.URL+tc.path, nil)
			if err != nil {
				t.Fatalf("build request: %v", err)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("request: %v", err)
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("read response: %v", err)
			}
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, body=%s", resp.StatusCode, string(body))
			}
			var response map[string]any
			if err := json.Unmarshal(body, &response); err != nil {
				t.Fatalf("response JSON error: %v", err)
			}
			if response["source"] != "upstream" {
				t.Fatalf("unexpected response: %#v", response)
			}
		})
	}
	if runtimeRequests != 0 {
		t.Fatalf("runtimeRequests = %d, want 0", runtimeRequests)
	}
	if upstreamRequests != 5 {
		t.Fatalf("upstreamRequests = %d, want 5", upstreamRequests)
	}
}

func TestRegisterManagementRoutesPassesRemoteStyleActorEnginePathsUpstreamWithProxy(t *testing.T) {
	gin.SetMode(gin.TestMode)

	runtimeRequests := 0
	runtimeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		runtimeRequests++
		writeNeoJSON(w, http.StatusOK, map[string]any{"source": "runtime"})
	}))
	defer runtimeServer.Close()
	runtimeURL, err := url.Parse(runtimeServer.URL)
	if err != nil {
		t.Fatalf("parse runtime URL: %v", err)
	}
	host, portText, err := net.SplitHostPort(runtimeURL.Host)
	if err != nil {
		t.Fatalf("split runtime host: %v", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("parse runtime port: %v", err)
	}

	var upstreamPaths []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamPaths = append(upstreamPaths, r.URL.RequestURI())
		writeNeoJSON(w, http.StatusOK, map[string]any{"source": "upstream"})
	}))
	defer upstream.Close()

	r := gin.New()
	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime:          &neoRuntime{host: host, port: port},
	}
	proxy, err := createReverseProxy(upstream.URL, NewStaticSecretSource(""))
	if err != nil {
		t.Fatalf("create proxy: %v", err)
	}
	m.setProxy(proxy)
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	localServer := httptest.NewServer(r)
	defer localServer.Close()

	// Newer rivetkit moved the metadata probe and the gateway transport under /actors/;
	// with local-neo enabled these must reach the local engine. The rivet manager CRUD
	// endpoint (/actors/actors) is not a local-neo transport path and still passes
	// through to the configured upstream.
	for _, tc := range []struct {
		path       string
		wantSource string
	}{
		{"/actors/metadata", "runtime"},
		{"/actors/gateway/threadActor/?rvt-method=get&rvt-key=T-local", "runtime"},
		{"/actors/actors?name=threadActor&key=T-upstream", "upstream"},
	} {
		t.Run(tc.path, func(t *testing.T) {
			resp, err := http.Get(localServer.URL + tc.path)
			if err != nil {
				t.Fatalf("request: %v", err)
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("read response: %v", err)
			}
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, body=%s", resp.StatusCode, string(body))
			}
			var response map[string]any
			if err := json.Unmarshal(body, &response); err != nil {
				t.Fatalf("response JSON error: %v", err)
			}
			if response["source"] != tc.wantSource {
				t.Fatalf("path %s routed to %v, want %s", tc.path, response["source"], tc.wantSource)
			}
		})
	}
	if runtimeRequests != 2 {
		t.Fatalf("runtimeRequests = %d, want 2 (metadata + gateway served locally)", runtimeRequests)
	}
	if len(upstreamPaths) != 1 || !slices.Contains(upstreamPaths, "/actors/actors?name=threadActor&key=T-upstream") {
		t.Fatalf("upstreamPaths = %#v, want only the rivet manager CRUD path upstream", upstreamPaths)
	}
}

func TestRegisterManagementRoutesBypassesManagementAuthForActorEngineAuthorization(t *testing.T) {
	gin.SetMode(gin.TestMode)

	gotHeaders := make(chan http.Header, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/actors/metadata" {
			t.Fatalf("unexpected upstream path %s", r.URL.Path)
		}
		gotHeaders <- r.Header.Clone()
		writeNeoJSON(w, http.StatusOK, map[string]any{"source": "upstream"})
	}))
	defer upstream.Close()

	r := gin.New()
	m := &AmpModule{restrictToLocalhost: false}
	proxy, err := createReverseProxy(upstream.URL, NewStaticSecretSource("secret"))
	if err != nil {
		t.Fatalf("create proxy: %v", err)
	}
	m.setProxy(proxy)
	auth := func(c *gin.Context) {
		token := strings.TrimSpace(c.GetHeader("Authorization"))
		token = strings.TrimSpace(strings.TrimPrefix(token, "Bearer "))
		if token != "amp-local-key" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing auth"})
			return
		}
		c.Set("userApiKey", token)
		c.Next()
	}
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, auth)

	server := httptest.NewServer(r)
	defer server.Close()

	req, err := http.NewRequest(http.MethodGet, server.URL+"/actors/metadata?namespace=default", nil)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer actor-token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body=%s", resp.StatusCode, body)
	}
	hdr := <-gotHeaders
	if hdr.Get("Authorization") != "Bearer actor-token" {
		t.Fatalf("Authorization = %q, want actor Bearer auth", hdr.Get("Authorization"))
	}
	if hdr.Get("X-Api-Key") != "" {
		t.Fatalf("X-Api-Key = %q, want stripped for actor engine", hdr.Get("X-Api-Key"))
	}
}

func TestRegisterManagementRoutesRequiresManagementAuthForActorEngineWithoutActorCredentials(t *testing.T) {
	gin.SetMode(gin.TestMode)

	upstreamRequests := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests++
		writeNeoJSON(w, http.StatusOK, map[string]any{"source": "upstream"})
	}))
	defer upstream.Close()

	r := gin.New()
	m := &AmpModule{restrictToLocalhost: false}
	proxy, err := createReverseProxy(upstream.URL, NewStaticSecretSource("secret"))
	if err != nil {
		t.Fatalf("create proxy: %v", err)
	}
	m.setProxy(proxy)
	auth := func(c *gin.Context) {
		token := strings.TrimSpace(c.GetHeader("Authorization"))
		token = strings.TrimSpace(strings.TrimPrefix(token, "Bearer "))
		if token != "amp-local-key" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing auth"})
			return
		}
		c.Set("userApiKey", token)
		c.Next()
	}
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, auth)

	server := httptest.NewServer(r)
	defer server.Close()

	// A per-actor engine request without rivet credentials must still be rejected.
	// (GET /actors/metadata is exempt — it is the token-less discovery probe — so this
	// uses a non-metadata actor path to assert the auth gate still holds.)
	resp, err := http.Get(server.URL + "/actors/example-actor")
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body=%s", resp.StatusCode, body)
	}
	if upstreamRequests != 0 {
		t.Fatalf("upstreamRequests = %d, want 0", upstreamRequests)
	}
}

// The RivetKit metadata discovery probe (GET /actors/metadata) carries no token before
// a connection exists, so it must bypass management auth and reach the engine upstream.
// Otherwise the client's retry-forever lookup loops on connect_failed and the thread
// transport never establishes (the symptom that broke connectivity on a binary update).
func TestRegisterManagementRoutesAllowsUnauthenticatedActorMetadataDiscovery(t *testing.T) {
	gin.SetMode(gin.TestMode)

	upstreamRequests := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests++
		writeNeoJSON(w, http.StatusOK, map[string]any{"clientEndpoint": "http://127.0.0.1:8317"})
	}))
	defer upstream.Close()

	r := gin.New()
	m := &AmpModule{restrictToLocalhost: false}
	proxy, err := createReverseProxy(upstream.URL, NewStaticSecretSource("secret"))
	if err != nil {
		t.Fatalf("create proxy: %v", err)
	}
	m.setProxy(proxy)
	auth := func(c *gin.Context) {
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing auth"})
	}
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, auth)

	server := httptest.NewServer(r)
	defer server.Close()

	resp, err := http.Get(server.URL + "/actors/metadata?namespace=default")
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200 (auth must be bypassed for the discovery probe); body=%s", resp.StatusCode, body)
	}
	if upstreamRequests != 1 {
		t.Fatalf("upstreamRequests = %d, want 1 (probe must reach the engine)", upstreamRequests)
	}
}

func TestRegisterManagementRoutesBypassesManagementAuthForActorEngineWebsocketToken(t *testing.T) {
	gin.SetMode(gin.TestMode)

	gotPath := make(chan string, 1)
	gotHeaders := make(chan http.Header, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath <- r.URL.RequestURI()
		gotHeaders <- r.Header.Clone()
		writeNeoJSON(w, http.StatusOK, map[string]any{"source": "upstream"})
	}))
	defer upstream.Close()

	r := gin.New()
	m := &AmpModule{restrictToLocalhost: false}
	proxy, err := createReverseProxy(upstream.URL, NewStaticSecretSource("secret"))
	if err != nil {
		t.Fatalf("create proxy: %v", err)
	}
	m.setProxy(proxy)
	auth := func(c *gin.Context) {
		token := strings.TrimSpace(c.GetHeader("Authorization"))
		token = strings.TrimSpace(strings.TrimPrefix(token, "Bearer "))
		if token != "amp-local-key" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing auth"})
			return
		}
		c.Set("userApiKey", token)
		c.Next()
	}
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, auth)

	server := httptest.NewServer(r)
	defer server.Close()

	req, err := http.NewRequest(http.MethodGet, server.URL+"/actors/gateway/threadActor/websocket/?rvt-token=actor-token&rvt-method=getOrCreate", nil)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	req.Header.Set("Sec-WebSocket-Protocol", "rivet, rivet_token.actor-token, rivet_encoding.json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body=%s", resp.StatusCode, body)
	}
	if path := <-gotPath; !strings.HasPrefix(path, "/actors/gateway/threadActor/websocket/") {
		t.Fatalf("upstream path = %q, want actor websocket path", path)
	}
	hdr := <-gotHeaders
	if hdr.Get("Authorization") != "" {
		t.Fatalf("Authorization = %q, want none for actor websocket token", hdr.Get("Authorization"))
	}
	if hdr.Get("X-Api-Key") != "" {
		t.Fatalf("X-Api-Key = %q, want stripped for actor websocket token", hdr.Get("X-Api-Key"))
	}
}

func TestRegisterManagementRoutesRequiresManagementAuthForWebLocalActorEngineWebsocketToken(t *testing.T) {
	gin.SetMode(gin.TestMode)

	gotPath := make(chan string, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath <- r.URL.RequestURI()
		writeNeoJSON(w, http.StatusOK, map[string]any{"source": "upstream"})
	}))
	defer upstream.Close()

	r := gin.New()
	m := &AmpModule{
		restrictToLocalhost: false,
		lastConfig: &config.AmpCode{
			WebLocalInference: config.AmpWebLocalInference{
				Enabled:        true,
				AllowedOrigins: []string{"https://ampcode.com"},
			},
		},
	}
	proxy, err := createReverseProxy(upstream.URL, NewStaticSecretSource("secret"))
	if err != nil {
		t.Fatalf("create proxy: %v", err)
	}
	m.setProxy(proxy)
	authCalled := false
	auth := func(c *gin.Context) {
		authCalled = true
		token := strings.TrimSpace(c.GetHeader("Authorization"))
		token = strings.TrimSpace(strings.TrimPrefix(token, "Bearer "))
		if token != "local-key" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing auth"})
			return
		}
		c.Set("userApiKey", token)
		c.Next()
	}
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, auth)

	server := httptest.NewServer(r)
	defer server.Close()

	req, err := http.NewRequest(http.MethodGet, server.URL+"/gateway/threadActor/websocket/?rvt-namespace=default&rvt-method=get&rvt-key=T-web&rvt-token=local-neo&rvt-skip-ready-wait=true", nil)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	req.Header.Set("Origin", "https://ampcode.com")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body=%s", resp.StatusCode, body)
	}
	if !authCalled {
		t.Fatal("actor websocket token request should call management auth for web-local CORS")
	}
	select {
	case path := <-gotPath:
		t.Fatalf("unauthorized request reached upstream path %q", path)
	default:
	}

	authCalled = false
	authorizedReq, err := http.NewRequest(http.MethodGet, server.URL+"/gateway/threadActor/websocket/?rvt-namespace=default&rvt-method=get&rvt-key=T-web&rvt-token=local-neo&rvt-skip-ready-wait=true&"+ampWebLocalInferenceAPIKeyQuery+"=local-key", nil)
	if err != nil {
		t.Fatalf("create authorized request: %v", err)
	}
	authorizedReq.Header.Set("Origin", "https://ampcode.com")
	authorizedResp, err := http.DefaultClient.Do(authorizedReq)
	if err != nil {
		t.Fatalf("authorized request: %v", err)
	}
	defer authorizedResp.Body.Close()
	if authorizedResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(authorizedResp.Body)
		t.Fatalf("authorized status = %d, body=%s", authorizedResp.StatusCode, body)
	}
	if !authCalled {
		t.Fatal("authorized actor websocket token request should call management auth")
	}
	if path := <-gotPath; !strings.HasPrefix(path, "/gateway/threadActor/websocket/") {
		t.Fatalf("upstream path = %q, want actor websocket path", path)
	} else if strings.Contains(path, ampWebLocalInferenceAPIKeyQuery) {
		t.Fatalf("upstream path leaked query API key: %q", path)
	}

	authCalled = false
	workerReq, err := http.NewRequest(http.MethodGet, server.URL+"/gateway/threadActor/websocket/?rvt-namespace=default&rvt-method=get&rvt-key=T-web&rvt-token=local-key&rvt-skip-ready-wait=true", nil)
	if err != nil {
		t.Fatalf("create worker request: %v", err)
	}
	workerReq.Header.Set("Origin", "https://ampcode.com")
	workerReq.Header.Set("Upgrade", "websocket")
	workerResp, err := http.DefaultClient.Do(workerReq)
	if err != nil {
		t.Fatalf("worker request: %v", err)
	}
	defer workerResp.Body.Close()
	if workerResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(workerResp.Body)
		t.Fatalf("worker status = %d, body=%s", workerResp.StatusCode, body)
	}
	if !authCalled {
		t.Fatal("worker actor websocket token request should call management auth")
	}
	if path := <-gotPath; strings.Contains(path, "rvt-token") {
		t.Fatalf("upstream path leaked worker token: %q", path)
	}

	metadataReq, err := http.NewRequest(http.MethodGet, server.URL+"/metadata?namespace=default", nil)
	if err != nil {
		t.Fatalf("metadata request build: %v", err)
	}
	metadataReq.Header.Set("Origin", "https://ampcode.com")
	metadataResp, err := http.DefaultClient.Do(metadataReq)
	if err != nil {
		t.Fatalf("metadata request: %v", err)
	}
	defer metadataResp.Body.Close()
	if metadataResp.StatusCode != http.StatusUnauthorized {
		body, _ := io.ReadAll(metadataResp.Body)
		t.Fatalf("metadata status = %d, body=%s", metadataResp.StatusCode, body)
	}
}

func TestRegisterManagementRoutesRejectsUnknownNeoRuntimeBridgePathsWithoutProxy(t *testing.T) {
	gin.SetMode(gin.TestMode)

	runtimeRequests := 0
	runtimeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		runtimeRequests++
		writeNeoJSON(w, http.StatusOK, map[string]any{"source": "runtime"})
	}))
	defer runtimeServer.Close()
	runtimeURL, err := url.Parse(runtimeServer.URL)
	if err != nil {
		t.Fatalf("parse runtime URL: %v", err)
	}
	host, portText, err := net.SplitHostPort(runtimeURL.Host)
	if err != nil {
		t.Fatalf("split runtime host: %v", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("parse runtime port: %v", err)
	}

	r := gin.New()
	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime:          &neoRuntime{host: host, port: port},
	}
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	localServer := httptest.NewServer(r)
	defer localServer.Close()

	resp, err := http.Get(localServer.URL + "/metadata")
	if err != nil {
		t.Fatalf("metadata request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("metadata status = %d, body=%s", resp.StatusCode, string(body))
	}

	for _, path := range []string{
		"/gateway/static/asset.js",
		"/actors/actor-local/unhandled",
	} {
		t.Run(path, func(t *testing.T) {
			resp, err := http.Get(localServer.URL + path)
			if err != nil {
				t.Fatalf("request: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusServiceUnavailable {
				body, _ := io.ReadAll(resp.Body)
				t.Fatalf("status = %d, want 503; body=%s", resp.StatusCode, string(body))
			}
		})
	}
	if runtimeRequests != 1 {
		t.Fatalf("runtimeRequests = %d, want only metadata request", runtimeRequests)
	}
}

func TestRegisterManagementRoutesPassesNewNeoThreadActorUpstreamWithProxy(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	enabled := true
	proxyCalled := false
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxyCalled = true
		w.WriteHeader(http.StatusTeapot)
	}))
	defer upstream.Close()

	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime: newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
			UpstreamURL: upstream.URL,
			NeoLocalRuntime: config.AmpNeoLocalRuntime{
				Enabled: &enabled,
			},
		}}),
	}
	proxy, _ := createReverseProxy(upstream.URL, NewStaticSecretSource(""))
	m.setProxy(proxy)
	auth := func(c *gin.Context) {
		token := strings.TrimSpace(strings.TrimPrefix(c.GetHeader("Authorization"), "Bearer "))
		if token != "local-key" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing auth"})
			return
		}
		c.Set("userApiKey", token)
		c.Next()
	}
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, auth)

	localServer := httptest.NewServer(r)
	t.Cleanup(localServer.Close)
	req, err := http.NewRequest(http.MethodPost, localServer.URL+"/api/thread-actors", bytes.NewBufferString(`{"agentMode":"deep","usesThreadActors":true}`))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer local-key")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()
	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}

	if resp.StatusCode != http.StatusTeapot {
		t.Fatalf("status = %d, body=%s", resp.StatusCode, string(responseBody))
	}
	if !proxyCalled {
		t.Fatal("new CLI thread actor request did not reach upstream")
	}
}

func TestRegisterManagementRoutesPassesNewCLIThreadActorToUpstream(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	enabled := true
	cloudThreadID := "T-019f6c11-91d7-72cf-ad68-f51a948c2401"
	requests := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.Method != http.MethodPost || r.URL.Path != "/api/thread-actors" || r.URL.RawQuery != "" {
			t.Fatalf("upstream request = %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
		}
		if r.Header.Get("Authorization") != "Bearer upstream-key" {
			t.Fatalf("upstream Authorization = %q", r.Header.Get("Authorization"))
		}
		body := readNeoJSON(r.Body)
		if body["usesThreadActors"] != true || stringValue(body["agentMode"]) != "deep" {
			t.Fatalf("upstream body = %#v", body)
		}
		writeNeoJSON(w, http.StatusCreated, map[string]any{
			"threadId":         cloudThreadID,
			"wsToken":          "cloud-token",
			"ownerUserId":      "cloud-user",
			"threadVersion":    0,
			"poolName":         "cloud-pool",
			"capability":       "write",
			"usesDtw":          true,
			"usesThreadActors": true,
			"agentMode":        "deep",
		})
	}))
	t.Cleanup(upstream.Close)

	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime: newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
			UpstreamURL:    upstream.URL,
			UpstreamAPIKey: "upstream-key",
			NeoLocalRuntime: config.AmpNeoLocalRuntime{
				Enabled: &enabled,
			},
		}}),
	}
	proxy, _ := createReverseProxy(upstream.URL, NewStaticSecretSource("upstream-key"))
	m.setProxy(proxy)
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	localServer := httptest.NewServer(r)
	t.Cleanup(localServer.Close)
	req, err := http.NewRequest(http.MethodPost, localServer.URL+"/api/thread-actors", bytes.NewBufferString(`{"agentMode":"deep","usesThreadActors":true}`))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()
	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, body=%s", resp.StatusCode, string(responseBody))
	}
	if requests != 1 {
		t.Fatalf("upstream requests = %d, want 1", requests)
	}
	var response map[string]any
	if err := json.Unmarshal(responseBody, &response); err != nil {
		t.Fatalf("response JSON error: %v", err)
	}
	if stringValue(response["threadId"]) != cloudThreadID || stringValue(response["wsToken"]) != "cloud-token" || stringValue(response["ownerUserId"]) != "cloud-user" || stringValue(response["poolName"]) != "cloud-pool" {
		t.Fatalf("thread actor response = %#v", response)
	}
	if m.neoRuntime.store.lookupThreadActor(cloudThreadID) != nil {
		t.Fatal("CLI thread actor was incorrectly imported into the local runtime")
	}
}

func TestRegisterManagementRoutesPassesMalformedThreadActorCreateUpstreamWithProxy(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	enabled := true
	proxyCalled := false
	var upstreamBody string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxyCalled = true
		data, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read upstream body: %v", err)
		}
		upstreamBody = string(data)
		w.WriteHeader(http.StatusTeapot)
	}))
	defer upstream.Close()

	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime: newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
			UpstreamURL: upstream.URL,
			NeoLocalRuntime: config.AmpNeoLocalRuntime{
				Enabled: &enabled,
			},
		}}),
	}
	proxy, _ := createReverseProxy(upstream.URL, NewStaticSecretSource(""))
	m.setProxy(proxy)
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	requestBody := `{"agentMode":`
	localServer := httptest.NewServer(r)
	defer localServer.Close()
	req, err := http.NewRequest(http.MethodPost, localServer.URL+"/api/thread-actors", bytes.NewBufferString(requestBody))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Fatalf("close response body: %v", err)
		}
	}()

	if resp.StatusCode != http.StatusTeapot {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body=%s", resp.StatusCode, string(body))
	}
	if !proxyCalled {
		t.Fatal("malformed unclassified thread-actor create should be passed upstream")
	}
	if upstreamBody != requestBody {
		t.Fatalf("upstream body = %q, want %q", upstreamBody, requestBody)
	}
}

func TestRegisterManagementRoutesCanForceLocalNeoThreadActorsWithProxy(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	enabled := true
	cloudThreadID := "T-019f6c11-91d7-72cf-ad68-f51a948c2402"
	upstreamRequests := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests++
		if r.Method != http.MethodPost || r.URL.Path != "/api/thread-actors" {
			t.Fatalf("upstream request = %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer upstream-key" {
			t.Fatalf("upstream Authorization = %q", r.Header.Get("Authorization"))
		}
		body := readNeoJSON(r.Body)
		if stringValue(body["agentMode"]) != "deep" {
			t.Fatalf("upstream agentMode = %q, want deep", stringValue(body["agentMode"]))
		}
		for _, key := range []string{"prompt", "initialPrompt", "message"} {
			if _, ok := body[key]; ok {
				t.Fatalf("cloud shell request included %s: %#v", key, body)
			}
		}
		threadMeta := mapValue(body["threadMeta"])
		if threadMeta["cliProxyAPILocalNeo"] != true || threadMeta["cliProxyAPIWebLocalShell"] != true || threadMeta["ampcodeConnectorLocalNeo"] != true {
			t.Fatalf("upstream thread meta = %#v", threadMeta)
		}
		writeNeoJSON(w, http.StatusCreated, map[string]any{
			"threadId":         cloudThreadID,
			"wsToken":          "cloud-token",
			"ownerUserId":      "cloud-user",
			"threadVersion":    0,
			"poolName":         "cloud-pool",
			"usesDtw":          true,
			"usesThreadActors": true,
			"agentMode":        "rush",
		})
	}))
	defer upstream.Close()

	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime: newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
			UpstreamURL:    upstream.URL,
			UpstreamAPIKey: "upstream-key",
			NeoLocalRuntime: config.AmpNeoLocalRuntime{
				Enabled:           &enabled,
				ForceThreadActors: true,
			},
		}}),
	}
	proxy, _ := createReverseProxy(upstream.URL, NewStaticSecretSource("upstream-key"))
	m.setProxy(proxy)

	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	body := bytes.NewBufferString(`{"prompt":"run this locally","settings":{"agentMode":"deep"}}`)
	req := httptest.NewRequest(http.MethodPost, "/api/thread-actors", body)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if upstreamRequests != 1 {
		t.Fatalf("upstream requests = %d, want one cloud shell creation", upstreamRequests)
	}
	var response map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("response JSON error: %v", err)
	}
	if response["threadId"] != cloudThreadID || response["agentMode"] != "deep" || response["wsToken"] != "cloud-token" {
		t.Fatalf("unexpected local thread actor response: %#v", response)
	}
	actor := m.neoRuntime.store.lookupThreadActor(cloudThreadID)
	if actor == nil {
		t.Fatal("cloud thread ID was not bound to a local actor")
	}
	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.queue) != 1 || textFromBlocks(actor.queue[0].Content) != "run this locally" {
		t.Fatalf("local actor prompt queue = %#v", actor.queue)
	}
}

func TestNeoThreadActorBootstrapBodyRecognizesSupportedCreateShapes(t *testing.T) {
	for name, body := range map[string]map[string]any{
		"top-level mode":   {"agentMode": "deep"},
		"thread meta mode": {"threadMeta": map[string]any{"agentMode": "deep"}},
		"settings mode":    {"settings": map[string]any{"agentMode": "deep"}},
		"executor":         {"executorType": "local-client"},
		"prompt":           {"prompt": "run locally"},
		"initial prompt":   {"initialPrompt": "run locally"},
		"message":          {"message": "run locally"},
		"thread actors":    {"usesThreadActors": true},
	} {
		t.Run(name, func(t *testing.T) {
			if !neoThreadActorBootstrapBody(body) {
				t.Fatalf("bootstrap body was not recognized: %#v", body)
			}
		})
	}
	if neoThreadActorBootstrapBody(map[string]any{"unrelated": true}) {
		t.Fatal("unrelated body was recognized as a thread actor bootstrap")
	}
}

func TestRegisterManagementRoutesServesExistingNeoThreadActorLocallyWithProxy(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	enabled := true
	proxyCalled := false
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxyCalled = true
		w.WriteHeader(http.StatusTeapot)
	}))
	defer upstream.Close()

	threadID := "T-019e65c0-0310-77a8-b233-4b84d9c0612b"

	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime: newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
			UpstreamURL: upstream.URL,
			NeoLocalRuntime: config.AmpNeoLocalRuntime{
				Enabled: &enabled,
			},
		}}),
	}
	proxy, _ := createReverseProxy(upstream.URL, NewStaticSecretSource(""))
	m.setProxy(proxy)
	m.neoRuntime.store.ensureThreadActor(threadID)
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	req := httptest.NewRequest(http.MethodPost, "/api/thread-actors/"+threadID, bytes.NewBufferString(`{"agentMode":"deep"}`))
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if proxyCalled {
		t.Fatal("existing thread-actors request should be served locally")
	}
	var response map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("response JSON error: %v", err)
	}
	if response["agentMode"] != "deep" || stringValue(response["wsToken"]) == "" || stringValue(response["ownerUserId"]) != neoLocalOwnerUserID {
		t.Fatalf("unexpected local thread actor response: %#v", response)
	}
}

func TestRegisterManagementRoutesServesExistingNeoThreadActorBodyThreadIDLocallyWithProxy(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	enabled := true
	proxyCalled := false
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxyCalled = true
		w.WriteHeader(http.StatusTeapot)
	}))
	defer upstream.Close()

	threadID := "T-019e65c0-0310-77a8-b233-4b84d9c0612d"

	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime: newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
			UpstreamURL: upstream.URL,
			NeoLocalRuntime: config.AmpNeoLocalRuntime{
				Enabled: &enabled,
			},
		}}),
	}
	proxy, _ := createReverseProxy(upstream.URL, NewStaticSecretSource(""))
	m.setProxy(proxy)
	m.neoRuntime.store.ensureThreadActor(threadID)
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	req := httptest.NewRequest(http.MethodPost, "/api/thread-actors", bytes.NewBufferString(`{"threadId":"`+threadID+`","agentMode":"deep"}`))
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if proxyCalled {
		t.Fatal("body threadId reconnect should be served locally for a local Neo thread")
	}
	var response map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("response JSON error: %v", err)
	}
	if response["threadId"] != threadID || response["agentMode"] != "deep" || stringValue(response["wsToken"]) == "" || stringValue(response["ownerUserId"]) != neoLocalOwnerUserID {
		t.Fatalf("unexpected local thread actor response: %#v", response)
	}
}

func TestRegisterManagementRoutesImportsMarkedCloudNeoThreadActorWithProxy(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	enabled := true

	threadID := "T-019e65c0-0310-77a8-b233-4b84d9c06131"
	getThreadRequests := 0
	threadActorRequests := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/internal" && r.URL.RawQuery == "getThread":
			getThreadRequests++
			writeNeoJSON(w, http.StatusOK, map[string]any{
				"ok": true,
				"result": map[string]any{
					"thread": map[string]any{
						"id":    threadID,
						"v":     5,
						"title": "cloud local thread",
						"meta":  map[string]any{"ampcodeConnectorLocalNeo": true},
						"data": map[string]any{
							"id":        threadID,
							"agentMode": "deep",
							"messages": []any{
								map[string]any{"messageId": "M-user", "role": "user", "agentMode": "deep", "content": []any{map[string]any{"type": "text", "text": "resume from cloud"}}},
							},
						},
					},
				},
			})
		case r.URL.Path == "/api/thread-actors/"+threadID:
			threadActorRequests++
			w.WriteHeader(http.StatusTeapot)
		default:
			t.Fatalf("unexpected upstream request path=%s query=%s", r.URL.Path, r.URL.RawQuery)
		}
	}))
	defer upstream.Close()

	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime: newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
			UpstreamURL:    upstream.URL,
			UpstreamAPIKey: "secret",
			NeoLocalRuntime: config.AmpNeoLocalRuntime{
				Enabled:           &enabled,
				ForceThreadActors: true,
			},
		}}),
	}
	proxy, _ := createReverseProxy(upstream.URL, NewStaticSecretSource("secret"))
	m.setProxy(proxy)
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	req := httptest.NewRequest(http.MethodPost, "/api/thread-actors/"+threadID, bytes.NewBufferString(`{}`))
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if getThreadRequests != 1 {
		t.Fatalf("getThreadRequests = %d, want 1", getThreadRequests)
	}
	if threadActorRequests != 0 {
		t.Fatalf("threadActorRequests = %d, want 0", threadActorRequests)
	}
	var response map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("response JSON error: %v", err)
	}
	if response["threadId"] != threadID || response["agentMode"] != "deep" || stringValue(response["wsToken"]) == "" || stringValue(response["ownerUserId"]) != neoLocalOwnerUserID {
		t.Fatalf("unexpected local thread actor response: %#v", response)
	}
	actor := m.neoRuntime.store.ensureThreadActor(threadID)
	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.messages) != 1 || textFromBlocks(actor.messages[0].Content) != "resume from cloud" {
		t.Fatalf("imported actor messages = %#v", actor.messages)
	}
	if actor.meta["ampcodeConnectorLocalNeo"] != true {
		t.Fatalf("imported actor meta = %#v", actor.meta)
	}
}
func TestRegisterManagementRoutesResumesPersistedLocalThreadActorWithProxy(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	enabled := true

	dir := t.TempDir()
	oldStoreDir := neoAmpDataDir
	neoAmpDataDir = func() string { return dir }
	t.Cleanup(func() { neoAmpDataDir = oldStoreDir })

	threadID := "T-019ef546-46bb-76d2-bfaa-f093660aaab8"
	if _, err := writeNeoLocalThreadFileInDir(neoAmpThreadStoreDir(), threadID, map[string]any{
		"id":        threadID,
		"title":     "persisted local actor",
		"agentMode": "deep",
		"meta":      map[string]any{"ampcodeConnectorLocalNeo": true},
		"messages": []any{
			map[string]any{"messageId": "M-local", "role": "user", "agentMode": "deep", "content": []any{map[string]any{"type": "text", "text": "resume locally"}}},
		},
	}); err != nil {
		t.Fatalf("write local thread: %v", err)
	}

	upstreamCalled := false
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalled = true
		w.WriteHeader(http.StatusTeapot)
	}))
	defer upstream.Close()

	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime: newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
			UpstreamURL:    upstream.URL,
			UpstreamAPIKey: "secret",
			NeoLocalRuntime: config.AmpNeoLocalRuntime{
				Enabled: &enabled,
			},
		}}),
	}
	proxy, _ := createReverseProxy(upstream.URL, NewStaticSecretSource("secret"))
	m.setProxy(proxy)
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	req := httptest.NewRequest(http.MethodPost, "/api/thread-actors/"+threadID, bytes.NewBufferString(`{}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if upstreamCalled {
		t.Fatal("persisted local thread actor resume should not proxy upstream")
	}
	var response map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("response JSON error: %v", err)
	}
	if response["threadId"] != threadID || response["ownerUserId"] != neoLocalOwnerUserID || response["agentMode"] != "deep" {
		t.Fatalf("unexpected local response: %#v", response)
	}
	actor := m.neoRuntime.store.lookupThreadActor(threadID)
	if actor == nil || !actor.hasLocalThreadState() {
		t.Fatal("persisted local actor was not hydrated")
	}
}

func TestRegisterManagementRoutesImportsMarkedCloudNeoThreadActorWhenEmptyActorExists(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	enabled := true

	threadID := "T-019e65c0-0310-77a8-b233-4b84d9c06132"
	getThreadRequests := 0
	threadActorRequests := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/internal" && r.URL.RawQuery == "getThread":
			getThreadRequests++
			writeNeoJSON(w, http.StatusOK, map[string]any{
				"ok": true,
				"result": map[string]any{
					"thread": map[string]any{
						"id":        threadID,
						"title":     "cloud local thread after sidebar",
						"agentMode": "deep",
						"meta":      map[string]any{"ampcodeConnectorLocalNeo": true},
						"messages": []any{
							map[string]any{"messageId": "M-user", "role": "user", "agentMode": "deep", "content": []any{map[string]any{"type": "text", "text": "resume after empty actor"}}},
						},
					},
				},
			})
		case r.URL.Path == "/api/thread-actors/"+threadID:
			threadActorRequests++
			w.WriteHeader(http.StatusTeapot)
		default:
			t.Fatalf("unexpected upstream request path=%s query=%s", r.URL.Path, r.URL.RawQuery)
		}
	}))
	defer upstream.Close()

	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime: newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
			UpstreamURL:    upstream.URL,
			UpstreamAPIKey: "secret",
			NeoLocalRuntime: config.AmpNeoLocalRuntime{
				Enabled: &enabled,
			},
		}}),
	}
	m.neoRuntime.store.ensureThreadActor(threadID)
	proxy, _ := createReverseProxy(upstream.URL, NewStaticSecretSource("secret"))
	m.setProxy(proxy)
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	req := httptest.NewRequest(http.MethodPost, "/api/thread-actors/"+threadID, bytes.NewBufferString(`{}`))
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if getThreadRequests != 1 {
		t.Fatalf("getThreadRequests = %d, want 1", getThreadRequests)
	}
	if threadActorRequests != 0 {
		t.Fatalf("threadActorRequests = %d, want 0", threadActorRequests)
	}
	var response map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("response JSON error: %v", err)
	}
	if response["threadId"] != threadID || response["agentMode"] != "deep" {
		t.Fatalf("unexpected local thread actor response: %#v", response)
	}
	actor := m.neoRuntime.store.lookupThreadActor(threadID)
	if actor == nil {
		t.Fatal("actor missing after cloud import")
	}
	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.messages) != 1 || textFromBlocks(actor.messages[0].Content) != "resume after empty actor" {
		t.Fatalf("imported actor messages = %#v", actor.messages)
	}
}

func TestRegisterManagementRoutesPassesUnmarkedCloudThreadActorUpstreamWithProxy(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	enabled := true
	useTempNeoThreadStore(t)

	threadID := "T-019e65c0-0310-77a8-b233-4b84d9c0612b"
	getThreadRequests := 0
	threadActorRequests := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/internal" && r.URL.RawQuery == "getThread":
			getThreadRequests++
			writeNeoJSON(w, http.StatusOK, map[string]any{
				"ok": true,
				"result": map[string]any{
					"thread": map[string]any{
						"id":        threadID,
						"title":     "cloud-only thread",
						"agentMode": "deep",
						"messages": []any{
							map[string]any{"messageId": "M-user", "role": "user", "agentMode": "deep", "content": []any{map[string]any{"type": "text", "text": "do not hijack"}}},
						},
					},
				},
			})
		case r.URL.Path == "/api/thread-actors/"+threadID:
			threadActorRequests++
			writeNeoJSON(w, http.StatusOK, map[string]any{
				"threadId":          threadID,
				"wsToken":           "upstream-token",
				"ownerUserId":       "upstream-user",
				"threadVersion":     3,
				"usesDtw":           true,
				"usesThreadActors":  true,
				"executorType":      "upstream",
				"agentMode":         "deep",
				"cloudOnlyResponse": true,
			})
		default:
			t.Fatalf("unexpected upstream request path=%s query=%s", r.URL.Path, r.URL.RawQuery)
		}
	}))
	defer upstream.Close()

	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime: newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
			UpstreamURL:    upstream.URL,
			UpstreamAPIKey: "secret",
			NeoLocalRuntime: config.AmpNeoLocalRuntime{
				Enabled: &enabled,
			},
		}}),
	}
	proxy, _ := createReverseProxy(upstream.URL, NewStaticSecretSource(""))
	m.setProxy(proxy)
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	localServer := httptest.NewServer(r)
	defer localServer.Close()

	req, err := http.NewRequest(http.MethodPost, localServer.URL+"/api/thread-actors/"+threadID, bytes.NewBufferString(`{}`))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Amp-Client-Application", "CLI")
	req.Header.Set("X-Amp-Client-Type", "cli")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Fatalf("close response body: %v", err)
		}
	}()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body=%s", resp.StatusCode, string(body))
	}
	if getThreadRequests != 1 {
		t.Fatalf("getThreadRequests = %d, want 1", getThreadRequests)
	}
	if threadActorRequests != 1 {
		t.Fatalf("threadActorRequests = %d, want 1", threadActorRequests)
	}
	var response map[string]any
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatalf("response JSON error: %v", err)
	}
	if response["cloudOnlyResponse"] != true || stringValue(response["wsToken"]) != "upstream-token" {
		t.Fatalf("unexpected upstream response: %#v", response)
	}
}

func TestRegisterManagementRoutesPassesUnmarkedCloudThreadActorUpstreamWhenEmptyActorExists(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	enabled := true
	useTempNeoThreadStore(t)

	threadID := "T-019e65c0-0310-77a8-b233-4b84d9c06133"
	getThreadRequests := 0
	threadActorRequests := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/internal" && r.URL.RawQuery == "getThread":
			getThreadRequests++
			writeNeoJSON(w, http.StatusOK, map[string]any{
				"ok": true,
				"result": map[string]any{
					"thread": map[string]any{
						"id":        threadID,
						"title":     "cloud-only thread",
						"agentMode": "deep",
						"messages": []any{
							map[string]any{"messageId": "M-user", "role": "user", "agentMode": "deep", "content": []any{map[string]any{"type": "text", "text": "do not hijack empty actor"}}},
						},
					},
				},
			})
		case r.URL.Path == "/api/thread-actors/"+threadID:
			threadActorRequests++
			writeNeoJSON(w, http.StatusOK, map[string]any{"threadId": threadID, "wsToken": "upstream-token", "cloudOnlyResponse": true})
		default:
			t.Fatalf("unexpected upstream request path=%s query=%s", r.URL.Path, r.URL.RawQuery)
		}
	}))
	defer upstream.Close()

	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime: newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
			UpstreamURL:    upstream.URL,
			UpstreamAPIKey: "secret",
			NeoLocalRuntime: config.AmpNeoLocalRuntime{
				Enabled: &enabled,
			},
		}}),
	}
	m.neoRuntime.store.ensureThreadActor(threadID)
	proxy, _ := createReverseProxy(upstream.URL, NewStaticSecretSource("secret"))
	m.setProxy(proxy)
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	localServer := httptest.NewServer(r)
	defer localServer.Close()
	req, err := http.NewRequest(http.MethodPost, localServer.URL+"/api/thread-actors/"+threadID, bytes.NewBufferString(`{}`))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Fatalf("close response body: %v", err)
		}
	}()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body=%s", resp.StatusCode, string(body))
	}
	if getThreadRequests != 1 {
		t.Fatalf("getThreadRequests = %d, want 1", getThreadRequests)
	}
	if threadActorRequests != 1 {
		t.Fatalf("threadActorRequests = %d, want 1", threadActorRequests)
	}
	var response map[string]any
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatalf("response JSON error: %v", err)
	}
	if response["cloudOnlyResponse"] != true || stringValue(response["wsToken"]) != "upstream-token" {
		t.Fatalf("unexpected upstream response: %#v", response)
	}
}

func TestRegisterManagementRoutesPassesUnimportableMarkedCloudThreadActorUpstreamWithProxy(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	enabled := true
	useTempNeoThreadStore(t)

	threadID := "T-019e65c0-0310-77a8-b233-4b84d9c0612f"
	getThreadRequests := 0
	threadActorRequests := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/internal" && r.URL.RawQuery == "getThread":
			getThreadRequests++
			writeNeoJSON(w, http.StatusOK, map[string]any{
				"ok": true,
				"result": map[string]any{
					"thread": map[string]any{
						"id":    threadID,
						"title": "stale local marker",
						"meta":  map[string]any{"cliProxyAPILocalNeo": true},
						"messages": []any{
							map[string]any{"messageId": "M-user", "role": "user", "content": []any{map[string]any{"type": "text", "text": "missing mode should not become smart"}}},
						},
					},
				},
			})
		case r.URL.Path == "/api/thread-actors/"+threadID:
			threadActorRequests++
			writeNeoJSON(w, http.StatusOK, map[string]any{
				"threadId":          threadID,
				"wsToken":           "upstream-token",
				"ownerUserId":       "upstream-user",
				"threadVersion":     7,
				"usesDtw":           true,
				"usesThreadActors":  true,
				"executorType":      "upstream",
				"agentMode":         "deep",
				"cloudOnlyResponse": true,
			})
		default:
			t.Fatalf("unexpected upstream request path=%s query=%s", r.URL.Path, r.URL.RawQuery)
		}
	}))
	defer upstream.Close()

	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime: newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
			UpstreamURL:    upstream.URL,
			UpstreamAPIKey: "secret",
			NeoLocalRuntime: config.AmpNeoLocalRuntime{
				Enabled: &enabled,
			},
		}}),
	}
	proxy, _ := createReverseProxy(upstream.URL, NewStaticSecretSource(""))
	m.setProxy(proxy)
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	localServer := httptest.NewServer(r)
	defer localServer.Close()

	req, err := http.NewRequest(http.MethodPost, localServer.URL+"/api/thread-actors/"+threadID, bytes.NewBufferString(`{}`))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Amp-Client-Application", "CLI")
	req.Header.Set("X-Amp-Client-Type", "cli")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Fatalf("close response body: %v", err)
		}
	}()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body=%s", resp.StatusCode, string(body))
	}
	if getThreadRequests != 1 {
		t.Fatalf("getThreadRequests = %d, want 1", getThreadRequests)
	}
	if threadActorRequests != 1 {
		t.Fatalf("threadActorRequests = %d, want 1", threadActorRequests)
	}
	var response map[string]any
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatalf("response JSON error: %v", err)
	}
	if response["cloudOnlyResponse"] != true || stringValue(response["agentMode"]) != "deep" {
		t.Fatalf("unexpected upstream response: %#v", response)
	}
}

func TestRegisterManagementRoutesRestoresBodyWhenBodyThreadActorPassesUpstream(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	enabled := true
	useTempNeoThreadStore(t)

	threadID := "T-019e65c0-0310-77a8-b233-4b84d9c0612e"
	getThreadRequests := 0
	var upstreamBody string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/internal" && r.URL.RawQuery == "getThread":
			getThreadRequests++
			writeNeoJSON(w, http.StatusOK, map[string]any{
				"ok": true,
				"result": map[string]any{
					"thread": map[string]any{
						"id":        threadID,
						"agentMode": "deep",
						"messages": []any{
							map[string]any{"messageId": "M-user", "role": "user", "agentMode": "deep", "content": []any{map[string]any{"type": "text", "text": "pass upstream"}}},
						},
					},
				},
			})
		case r.URL.Path == "/api/thread-actors":
			data, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read upstream body: %v", err)
			}
			upstreamBody = string(data)
			writeNeoJSON(w, http.StatusOK, map[string]any{
				"threadId":          threadID,
				"wsToken":           "upstream-token",
				"ownerUserId":       "upstream-user",
				"threadVersion":     3,
				"usesDtw":           true,
				"usesThreadActors":  true,
				"executorType":      "upstream",
				"agentMode":         "deep",
				"cloudOnlyResponse": true,
			})
		default:
			t.Fatalf("unexpected upstream request path=%s query=%s", r.URL.Path, r.URL.RawQuery)
		}
	}))
	defer upstream.Close()

	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime: newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
			UpstreamURL:    upstream.URL,
			UpstreamAPIKey: "secret",
			NeoLocalRuntime: config.AmpNeoLocalRuntime{
				Enabled: &enabled,
			},
		}}),
	}
	proxy, _ := createReverseProxy(upstream.URL, NewStaticSecretSource(""))
	m.setProxy(proxy)
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	requestBody := `{"threadId":"` + threadID + `","marker":"keep-body"}`
	localServer := httptest.NewServer(r)
	defer localServer.Close()
	req, err := http.NewRequest(http.MethodPost, localServer.URL+"/api/thread-actors", bytes.NewBufferString(requestBody))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Fatalf("close response body: %v", err)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body=%s", resp.StatusCode, string(body))
	}
	if getThreadRequests != 1 {
		t.Fatalf("getThreadRequests = %d, want 1", getThreadRequests)
	}
	if upstreamBody != requestBody {
		t.Fatalf("upstream body = %q, want %q", upstreamBody, requestBody)
	}
}

func TestRegisterManagementRoutesPassesCloudOnlyThreadActorsUpstream(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	enabled := true
	useTempNeoThreadStore(t)

	threadID := "T-019e65c0-0310-77a8-b233-4b84d9c0612c"
	threadActorRequests := 0
	getThreadRequests := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/internal" && r.URL.RawQuery == "getThread":
			getThreadRequests++
			writeNeoJSON(w, http.StatusOK, map[string]any{
				"ok": true,
				"result": map[string]any{
					"thread": map[string]any{
						"id":    threadID,
						"title": "cloud-only thread",
						"data": map[string]any{
							"id":    threadID,
							"title": "cloud-only thread",
							"messages": []any{
								map[string]any{"messageId": "M-cloud", "role": "user", "content": []any{map[string]any{"type": "text", "text": "cloud content"}}},
							},
						},
					},
				},
			})
		case r.URL.Path == "/api/thread-actors/"+threadID:
			threadActorRequests++
			writeNeoJSON(w, http.StatusOK, map[string]any{
				"threadId":          threadID,
				"wsToken":           "upstream-token",
				"ownerUserId":       "upstream-user",
				"threadVersion":     3,
				"usesDtw":           true,
				"usesThreadActors":  true,
				"executorType":      "upstream",
				"agentMode":         "deep",
				"cloudOnlyResponse": true,
			})
		default:
			t.Fatalf("unexpected upstream request path=%s query=%s", r.URL.Path, r.URL.RawQuery)
		}
	}))
	defer upstream.Close()

	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime: newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
			UpstreamURL:    upstream.URL,
			UpstreamAPIKey: "secret",
			NeoLocalRuntime: config.AmpNeoLocalRuntime{
				Enabled: &enabled,
			},
		}}),
	}
	proxy, _ := createReverseProxy(upstream.URL, NewStaticSecretSource(""))
	m.setProxy(proxy)
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	localServer := httptest.NewServer(r)
	defer localServer.Close()

	req, err := http.NewRequest(http.MethodPost, localServer.URL+"/api/thread-actors/"+threadID, bytes.NewBufferString(`{}`))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Amp-Client-Application", "CLI")
	req.Header.Set("X-Amp-Client-Type", "cli")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Fatalf("close response body: %v", err)
		}
	}()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body=%s", resp.StatusCode, string(body))
	}
	if getThreadRequests != 1 {
		t.Fatalf("getThreadRequests = %d, want 1", getThreadRequests)
	}
	if threadActorRequests != 1 {
		t.Fatalf("threadActorRequests = %d, want 1", threadActorRequests)
	}
	var response map[string]any
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatalf("response JSON error: %v", err)
	}
	if response["cloudOnlyResponse"] != true || stringValue(response["wsToken"]) != "upstream-token" {
		t.Fatalf("unexpected upstream thread-actor response: %#v", response)
	}
}

func TestRegisterManagementRoutesMimicsCloudThreadActorHandshake(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	enabled := true
	proxyCalled := false
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxyCalled = true
		w.WriteHeader(http.StatusTeapot)
	}))
	defer upstream.Close()

	threadID := "T-019e5b0b-ea5f-73a1-a4c1-7fb8517a0c00"
	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime: newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
			UpstreamURL: upstream.URL,
			NeoLocalRuntime: config.AmpNeoLocalRuntime{
				Enabled:           &enabled,
				ForceThreadActors: true,
			},
		}}),
	}
	proxy, _ := createReverseProxy(upstream.URL, NewStaticSecretSource(""))
	m.setProxy(proxy)
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	initialBody := bytes.NewBufferString(`{"threadId":"` + threadID + `","usesThreadActors":true}`)
	initialReq := httptest.NewRequest(http.MethodPost, "/api/thread-actors", initialBody)
	initialRec := httptest.NewRecorder()
	r.ServeHTTP(initialRec, initialReq)

	if initialRec.Code != http.StatusCreated {
		t.Fatalf("initial status = %d, body=%s", initialRec.Code, initialRec.Body.String())
	}
	if proxyCalled {
		t.Fatal("initial thread-actors request should be served locally")
	}
	var initialResponse map[string]any
	if err := json.Unmarshal(initialRec.Body.Bytes(), &initialResponse); err != nil {
		t.Fatalf("initial response JSON error: %v", err)
	}
	if initialResponse["usesDtw"] != false || initialResponse["usesThreadActors"] != false {
		t.Fatalf("initial handshake response = %#v", initialResponse)
	}

	patchBody := bytes.NewBufferString(`{"executorType":"local-client"}`)
	patchReq := httptest.NewRequest(http.MethodPost, "/api/thread-actors/"+threadID, patchBody)
	patchRec := httptest.NewRecorder()
	r.ServeHTTP(patchRec, patchReq)

	if patchRec.Code != http.StatusOK {
		t.Fatalf("patch status = %d, body=%s", patchRec.Code, patchRec.Body.String())
	}
	var patchResponse map[string]any
	if err := json.Unmarshal(patchRec.Body.Bytes(), &patchResponse); err != nil {
		t.Fatalf("patch response JSON error: %v", err)
	}
	if patchResponse["ok"] != true || patchResponse["usesThreadActors"] != true || patchResponse["executorType"] != "local-client" || patchResponse["threadId"] != threadID || stringValue(patchResponse["wsToken"]) == "" {
		t.Fatalf("patch handshake response = %#v", patchResponse)
	}

	finalBody := bytes.NewBufferString(`{"threadId":"` + threadID + `"}`)
	finalReq := httptest.NewRequest(http.MethodPost, "/api/thread-actors", finalBody)
	finalRec := httptest.NewRecorder()
	r.ServeHTTP(finalRec, finalReq)

	if finalRec.Code != http.StatusCreated {
		t.Fatalf("final status = %d, body=%s", finalRec.Code, finalRec.Body.String())
	}
	var finalResponse map[string]any
	if err := json.Unmarshal(finalRec.Body.Bytes(), &finalResponse); err != nil {
		t.Fatalf("final response JSON error: %v", err)
	}
	if finalResponse["usesDtw"] != true || finalResponse["usesThreadActors"] != true || finalResponse["executorType"] != "local-client" {
		t.Fatalf("final handshake response = %#v", finalResponse)
	}
}

func TestRegisterManagementRoutesServesLocalNeoReviewThreadInternalRPCs(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	enabled := true
	proxyCalled := false
	cloudShellRequests := 0
	cloudThreadID := "T-019f6c50-371f-7d88-8d80-81bb1a97a787"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/api/thread-actors" {
			cloudShellRequests++
			writeNeoJSON(w, http.StatusCreated, map[string]any{
				"threadId":         cloudThreadID,
				"wsToken":          "cloud-token",
				"ownerUserId":      "cloud-user",
				"threadVersion":    0,
				"usesDtw":          true,
				"usesThreadActors": true,
			})
			return
		}
		proxyCalled = true
		w.WriteHeader(http.StatusTeapot)
	}))
	defer upstream.Close()

	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime: newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
			UpstreamURL:    upstream.URL,
			UpstreamAPIKey: "secret",
			NeoLocalRuntime: config.AmpNeoLocalRuntime{
				Enabled:           &enabled,
				ForceThreadActors: true,
			},
		}}),
	}
	proxy, _ := createReverseProxy(upstream.URL, NewStaticSecretSource("secret"))
	m.setProxy(proxy)
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	createReq := httptest.NewRequest(http.MethodPost, "/api/thread-actors", bytes.NewBufferString(`{"agentMode":"review","usesThreadActors":true}`))
	createRec := httptest.NewRecorder()
	r.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusCreated {
		t.Fatalf("create status = %d, body=%s", createRec.Code, createRec.Body.String())
	}
	if cloudShellRequests != 1 {
		t.Fatalf("cloud shell requests = %d, want 1", cloudShellRequests)
	}
	if proxyCalled {
		t.Fatal("review thread actor create should be served locally")
	}
	var createResponse map[string]any
	if err := json.Unmarshal(createRec.Body.Bytes(), &createResponse); err != nil {
		t.Fatalf("create response JSON error: %v", err)
	}
	threadID := stringValue(createResponse["threadId"])
	requireNeoBinaryV7ThreadID(t, threadID)

	proxyCalled = false
	labelBody := `{"method":"addThreadLabels","params":{"thread":"` + threadID + `","labels":["review"]}}`
	labelReq := httptest.NewRequest(http.MethodPost, "/api/internal?addThreadLabels", bytes.NewBufferString(labelBody))
	labelRec := httptest.NewRecorder()
	r.ServeHTTP(labelRec, labelReq)
	if labelRec.Code != http.StatusOK {
		t.Fatalf("label status = %d, body=%s", labelRec.Code, labelRec.Body.String())
	}
	if proxyCalled {
		t.Fatal("local review addThreadLabels should not proxy upstream")
	}
	var labelResponse map[string]any
	if err := json.Unmarshal(labelRec.Body.Bytes(), &labelResponse); err != nil {
		t.Fatalf("label response JSON error: %v", err)
	}
	labels := arrayValue(labelResponse["result"])
	if labelResponse["ok"] != true || len(labels) != 1 || stringValue(mapValue(labels[0])["name"]) != "review" {
		t.Fatalf("label response = %#v", labelResponse)
	}

	proxyCalled = false
	archiveBody := `{"method":"archiveThread","params":{"thread":"` + threadID + `","archived":true}}`
	archiveReq := httptest.NewRequest(http.MethodPost, "/api/internal?archiveThread", bytes.NewBufferString(archiveBody))
	archiveRec := httptest.NewRecorder()
	r.ServeHTTP(archiveRec, archiveReq)
	if archiveRec.Code != http.StatusOK {
		t.Fatalf("archive status = %d, body=%s", archiveRec.Code, archiveRec.Body.String())
	}
	if proxyCalled {
		t.Fatal("local review archiveThread should not proxy upstream")
	}
	actor := m.neoRuntime.store.lookupThreadActor(threadID)
	if actor == nil {
		t.Fatalf("missing local actor for %s", threadID)
	}
	actor.mu.Lock()
	archived := actor.archived
	storedLabels := neoThreadLabelsFromAny(actor.meta["labels"])
	actor.mu.Unlock()
	if !archived || len(storedLabels) != 1 || storedLabels[0] != "review" {
		t.Fatalf("actor archived=%v labels=%#v", archived, storedLabels)
	}
}

func TestRegisterManagementRoutesServesLocalNeoInternalRPCsWithoutProxy(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	enabled := true
	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime: newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
			NeoLocalRuntime: config.AmpNeoLocalRuntime{
				Enabled: &enabled,
			},
		}}),
	}
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	createReq := httptest.NewRequest(http.MethodPost, "/api/thread-actors", bytes.NewBufferString(`{"agentMode":"review","usesThreadActors":true}`))
	createRec := httptest.NewRecorder()
	r.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body=%s", createRec.Code, createRec.Body.String())
	}
	var createResponse map[string]any
	if err := json.Unmarshal(createRec.Body.Bytes(), &createResponse); err != nil {
		t.Fatalf("create response JSON error: %v", err)
	}
	threadID := stringValue(createResponse["threadId"])
	requireNeoBinaryV7ThreadID(t, threadID)

	labelBody := `{"method":"addThreadLabels","params":{"thread":"` + threadID + `","labels":["review"]}}`
	labelReq := httptest.NewRequest(http.MethodPost, "/api/internal?addThreadLabels", bytes.NewBufferString(labelBody))
	labelRec := httptest.NewRecorder()
	r.ServeHTTP(labelRec, labelReq)
	if labelRec.Code != http.StatusOK {
		t.Fatalf("label status = %d, body=%s", labelRec.Code, labelRec.Body.String())
	}

	archiveBody := `{"method":"archiveThread","params":{"thread":"` + threadID + `","archived":true}}`
	archiveReq := httptest.NewRequest(http.MethodPost, "/api/internal?archiveThread", bytes.NewBufferString(archiveBody))
	archiveRec := httptest.NewRecorder()
	r.ServeHTTP(archiveRec, archiveReq)
	if archiveRec.Code != http.StatusOK {
		t.Fatalf("archive status = %d, body=%s", archiveRec.Code, archiveRec.Body.String())
	}

	actor := m.neoRuntime.store.lookupThreadActor(threadID)
	if actor == nil {
		t.Fatalf("missing local actor for %s", threadID)
	}
	actor.mu.Lock()
	archived := actor.archived
	storedLabels := neoThreadLabelsFromAny(actor.meta["labels"])
	actor.mu.Unlock()
	if !archived || len(storedLabels) != 1 || storedLabels[0] != "review" {
		t.Fatalf("actor archived=%v labels=%#v", archived, storedLabels)
	}
}

func TestRegisterManagementRoutesDoesNotServeNeoBootstrapInternalsLocally(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	enabled := true
	proxyCalled := false
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxyCalled = true
		w.WriteHeader(http.StatusTeapot)
	}))
	defer upstream.Close()

	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime: newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
			UpstreamURL:    upstream.URL,
			UpstreamAPIKey: "secret",
			NeoLocalRuntime: config.AmpNeoLocalRuntime{
				Enabled: &enabled,
			},
		}}),
	}
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	for _, tc := range []struct {
		name string
		path string
	}{
		{name: "loadPlugins", path: "/api/internal?loadPlugins"},
		{name: "getUserInfo", path: "/api/internal?getUserInfo"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proxyCalled = false
			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, req)

			if rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
			}
			if proxyCalled {
				t.Fatalf("%s should not call the test upstream without a configured proxy", tc.path)
			}
		})
	}
}

func TestRegisterManagementRoutesServesNeoAttachmentsLocally(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	dir := t.TempDir()
	oldStoreDir := neoAmpDataDir
	neoAmpDataDir = func() string { return dir }
	t.Cleanup(func() { neoAmpDataDir = oldStoreDir })

	enabled := true
	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime: newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
			NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled},
		}}),
	}
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	imageBase64 := testNeoPNGBase64(t, 1, 1)
	imageData, err := base64.StdEncoding.DecodeString(imageBase64)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/attachments", bytes.NewBufferString(fmt.Sprintf(`{"data":%q,"mediaType":"image/png"}`, imageBase64)))
	req.Host = "127.0.0.1:8317"
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("upload status = %d, body=%s", rec.Code, rec.Body.String())
	}
	var response map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("upload response JSON error: %v", err)
	}
	attachmentURL := stringValue(response["url"])
	parsed, err := url.Parse(attachmentURL)
	if err != nil {
		t.Fatalf("parse attachment URL %q: %v", attachmentURL, err)
	}
	if parsed.Scheme != "http" || parsed.Host != "127.0.0.1:8317" || !strings.HasPrefix(parsed.Path, "/attachments/") {
		t.Fatalf("attachment URL = %q", attachmentURL)
	}

	getReq := httptest.NewRequest(http.MethodGet, parsed.RequestURI(), nil)
	getRec := httptest.NewRecorder()
	r.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("get status = %d, body=%s", getRec.Code, getRec.Body.String())
	}
	if got := getRec.Body.Bytes(); !bytes.Equal(got, imageData) {
		t.Fatalf("attachment body length = %d, want %d", len(got), len(imageData))
	}
	if got := getRec.Header().Get("Content-Type"); got != "image/png" {
		t.Fatalf("attachment content-type = %q", got)
	}

	headReq := httptest.NewRequest(http.MethodHead, parsed.RequestURI(), nil)
	headRec := httptest.NewRecorder()
	r.ServeHTTP(headRec, headReq)
	if headRec.Code != http.StatusOK || headRec.Body.Len() != 0 {
		t.Fatalf("head response = status %d body %q", headRec.Code, headRec.Body.String())
	}
}

func TestRegisterManagementRoutesServesNeoAttachmentsViaBinaryURLForms(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	dir := t.TempDir()
	oldStoreDir := neoAmpDataDir
	neoAmpDataDir = func() string { return dir }
	t.Cleanup(func() { neoAmpDataDir = oldStoreDir })

	enabled := true
	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime: newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
			NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled},
		}}),
	}
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	imageBase64 := testNeoPNGBase64(t, 1, 1)
	imageData, err := base64.StdEncoding.DecodeString(imageBase64)
	if err != nil {
		t.Fatal(err)
	}

	// Upload with current binary fields (threadID, publicArtifact, temporaryFile)
	// must be tolerated by the local upload handler.
	uploadBody := fmt.Sprintf(`{"data":%q,"mediaType":"image/png","threadID":"T-019fd27e-8185-77c2-bd71-77a5adbb0d16","publicArtifact":false,"temporaryFile":true}`, imageBase64)
	req := httptest.NewRequest(http.MethodPost, "/api/attachments", bytes.NewBufferString(uploadBody))
	req.Host = "127.0.0.1:8317"
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("upload status = %d, body=%s", rec.Code, rec.Body.String())
	}
	var response map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("upload response JSON error: %v", err)
	}
	attachmentURL := stringValue(response["url"])
	parsed, err := url.Parse(attachmentURL)
	if err != nil {
		t.Fatalf("parse attachment URL %q: %v", attachmentURL, err)
	}
	// The generated URL must match the current binary's accepted forms
	// (root /attachments/<id>), not the legacy /api prefix.
	if !strings.HasPrefix(parsed.Path, "/attachments/") {
		t.Fatalf("generated attachment URL path = %q", parsed.Path)
	}
	id := strings.TrimPrefix(parsed.Path, "/attachments/")

	// GET through the generated root URL.
	getReq := httptest.NewRequest(http.MethodGet, parsed.RequestURI(), nil)
	getRec := httptest.NewRecorder()
	r.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK || !bytes.Equal(getRec.Body.Bytes(), imageData) {
		t.Fatalf("root get = status %d body length %d", getRec.Code, getRec.Body.Len())
	}

	// GET through the legacy /api alias still works.
	legacyReq := httptest.NewRequest(http.MethodGet, "/api/attachments/"+id, nil)
	legacyRec := httptest.NewRecorder()
	r.ServeHTTP(legacyRec, legacyReq)
	if legacyRec.Code != http.StatusOK || !bytes.Equal(legacyRec.Body.Bytes(), imageData) {
		t.Fatalf("legacy get = status %d body length %d", legacyRec.Code, legacyRec.Body.Len())
	}

	// GET through /user-content/attachments with the same local ID resolves
	// locally (a sha256-style ID with filename suffix maps to canonical ID).
	shaID := strings.Repeat("ab", 32)
	if _, err := writeNeoLocalAttachment(imageData, "image/png"); err != nil {
		t.Fatal(err)
	}
	// Write directly under a sha256-style canonical ID to exercise suffix parsing.
	if err := writeNeoLocalAttachmentWithID(t, shaID, imageData, "image/png"); err != nil {
		t.Fatal(err)
	}
	ucReq := httptest.NewRequest(http.MethodGet, "/user-content/attachments/"+shaID+"-file.png", nil)
	ucRec := httptest.NewRecorder()
	r.ServeHTTP(ucRec, ucReq)
	if ucRec.Code != http.StatusOK || !bytes.Equal(ucRec.Body.Bytes(), imageData) {
		t.Fatalf("user-content get = status %d body %q", ucRec.Code, ucRec.Body.String())
	}
}

func writeNeoLocalAttachmentWithID(t *testing.T, id string, raw []byte, mediaType string) error {
	t.Helper()
	dir := filepath.Join(neoAmpDataDir(), "attachments")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, id+".bin"), raw, 0o600); err != nil {
		return err
	}
	metaRaw, _ := json.Marshal(map[string]any{"mediaType": mediaType})
	return os.WriteFile(filepath.Join(dir, id+".json"), metaRaw, 0o600)
}

func TestRegisterManagementRoutesServesNeoAttachmentUploadAliasesLocally(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	dir := t.TempDir()
	oldStoreDir := neoAmpDataDir
	neoAmpDataDir = func() string { return dir }
	t.Cleanup(func() { neoAmpDataDir = oldStoreDir })

	enabled := true
	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime: newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
			NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled},
		}}),
	}
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	imageBase64 := testNeoPNGBase64(t, 1, 1)
	imageData, err := base64.StdEncoding.DecodeString(imageBase64)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/attachments", bytes.NewBufferString(fmt.Sprintf(`{"contentBase64":%q,"media_type":"image/png"}`, imageBase64)))
	req.Host = "127.0.0.1:8317"
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("upload status = %d, body=%s", rec.Code, rec.Body.String())
	}
	var response map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("upload response JSON error: %v", err)
	}
	attachmentURL := stringValue(response["url"])
	parsed, err := url.Parse(attachmentURL)
	if err != nil {
		t.Fatalf("parse attachment URL %q: %v", attachmentURL, err)
	}

	getReq := httptest.NewRequest(http.MethodGet, parsed.RequestURI(), nil)
	getRec := httptest.NewRecorder()
	r.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK || !bytes.Equal(getRec.Body.Bytes(), imageData) {
		t.Fatalf("get response = status %d body length %d", getRec.Code, getRec.Body.Len())
	}
	if got := getRec.Header().Get("Content-Type"); got != "image/png" {
		t.Fatalf("attachment content-type = %q", got)
	}
}

func TestRegisterManagementRoutesNeoAttachmentURLUsesForwardedPublicHost(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	dir := t.TempDir()
	oldStoreDir := neoAmpDataDir
	neoAmpDataDir = func() string { return dir }
	t.Cleanup(func() { neoAmpDataDir = oldStoreDir })

	enabled := true
	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime: newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
			NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled},
		}}),
	}
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	imageBase64 := testNeoPNGBase64(t, 1, 1)
	req := httptest.NewRequest(http.MethodPost, "/api/attachments", bytes.NewBufferString(fmt.Sprintf(`{"data":%q,"mediaType":"image/png"}`, imageBase64)))
	req.Host = "100.74.232.68:8317"
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Forwarded-Host", "neo.aikins.xyz")
	req.Header.Set("X-Forwarded-Proto", "https")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("upload status = %d, body=%s", rec.Code, rec.Body.String())
	}
	var response map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("upload response JSON error: %v", err)
	}
	attachmentURL := stringValue(response["url"])
	parsed, err := url.Parse(attachmentURL)
	if err != nil {
		t.Fatalf("parse attachment URL %q: %v", attachmentURL, err)
	}
	if parsed.Scheme != "https" || parsed.Host != "neo.aikins.xyz" || !strings.HasPrefix(parsed.Path, "/attachments/") {
		t.Fatalf("attachment URL = %q", attachmentURL)
	}
}

func TestRegisterManagementRoutesNeoAttachmentURLKeepsLoopbackHTTP(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/api/attachments", nil)
	req.Host = "127.0.0.1:8317"
	req.Header.Set("X-Forwarded-Proto", "https")

	attachmentURL := neoLocalAttachmentURL(req, "AbCdEfGhIjKlMnOp")
	parsed, err := url.Parse(attachmentURL)
	if err != nil {
		t.Fatalf("parse attachment URL %q: %v", attachmentURL, err)
	}
	if parsed.Scheme != "http" || parsed.Host != "127.0.0.1:8317" || parsed.Path != "/attachments/AbCdEfGhIjKlMnOp" {
		t.Fatalf("attachment URL = %q", attachmentURL)
	}
}

func TestRegisterManagementRoutesPassesAmpBinaryAttachmentsUpstream(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	enabled := true
	proxyCalled := false
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxyCalled = true
		if r.URL.Path != "/api/attachments" {
			t.Fatalf("unexpected upstream request path=%s", r.URL.Path)
		}
		if got := r.Header.Get("X-Amp-Client-Application"); got != "CLI" {
			t.Fatalf("X-Amp-Client-Application = %q", got)
		}
		writeNeoJSON(w, http.StatusOK, map[string]any{"url": "https://ampcode.com/api/attachments/upstream"})
	}))
	defer upstream.Close()

	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime: newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
			UpstreamURL:    upstream.URL,
			UpstreamAPIKey: "secret",
			NeoLocalRuntime: config.AmpNeoLocalRuntime{
				Enabled: &enabled,
			},
		}}),
	}
	proxy, _ := createReverseProxy(upstream.URL, NewStaticSecretSource(""))
	m.setProxy(proxy)
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	localServer := httptest.NewServer(r)
	defer localServer.Close()

	req, err := http.NewRequest(http.MethodPost, localServer.URL+"/api/attachments", bytes.NewBufferString(`{"data":"aGVsbG8=","mediaType":"image/png"}`))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Amp-Client-Application", "CLI")
	req.Header.Set("X-Amp-Client-Type", "cli")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Fatalf("close response body: %v", err)
		}
	}()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body=%s", resp.StatusCode, string(body))
	}
	if !proxyCalled {
		t.Fatal("Amp binary attachment upload should pass through to upstream")
	}
	var response map[string]any
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatalf("response JSON error: %v", err)
	}
	if stringValue(response["url"]) != "https://ampcode.com/api/attachments/upstream" {
		t.Fatalf("unexpected upstream response: %#v", response)
	}
}

func TestRegisterManagementRoutesDoesNotServeAmpBinaryAttachmentsLocally(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	enabled := true
	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime: newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
			NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled},
		}}),
	}
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	req := httptest.NewRequest(http.MethodPost, "/api/attachments", bytes.NewBufferString(`{"data":"aGVsbG8=","mediaType":"image/png"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Amp-Client-Application", "CLI")
	req.Header.Set("X-Amp-Client-Type", "cli")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
}

func TestRegisterManagementRoutesPassesMissingAttachmentGETUpstream(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	enabled := true

	dir := t.TempDir()
	oldStoreDir := neoAmpDataDir
	neoAmpDataDir = func() string { return dir }
	t.Cleanup(func() { neoAmpDataDir = oldStoreDir })

	upstreamRequests := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests++
		if r.URL.Path != "/api/attachments/AmpCloudAttachment123" {
			t.Fatalf("unexpected upstream request path=%s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write([]byte("upstream image"))
	}))
	defer upstream.Close()

	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime: newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
			UpstreamURL:    upstream.URL,
			UpstreamAPIKey: "secret",
			NeoLocalRuntime: config.AmpNeoLocalRuntime{
				Enabled: &enabled,
			},
		}}),
	}
	proxy, _ := createReverseProxy(upstream.URL, NewStaticSecretSource(""))
	m.setProxy(proxy)
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	localServer := httptest.NewServer(r)
	defer localServer.Close()

	resp, err := http.Get(localServer.URL + "/api/attachments/AmpCloudAttachment123")
	if err != nil {
		t.Fatalf("get upstream attachment: %v", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Fatalf("close upstream response body: %v", err)
		}
	}()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read upstream response body: %v", err)
	}
	if resp.StatusCode != http.StatusOK || string(body) != "upstream image" {
		t.Fatalf("upstream attachment response = status %d body %q", resp.StatusCode, string(body))
	}
	if upstreamRequests != 1 {
		t.Fatalf("upstreamRequests = %d, want 1", upstreamRequests)
	}

	localID, err := writeNeoLocalAttachment([]byte("local image"), "image/png")
	if err != nil {
		t.Fatalf("write local attachment: %v", err)
	}
	localResp, err := http.Get(localServer.URL + "/api/attachments/" + localID)
	if err != nil {
		t.Fatalf("get local attachment: %v", err)
	}
	defer func() {
		if err := localResp.Body.Close(); err != nil {
			t.Fatalf("close local response body: %v", err)
		}
	}()
	localBody, err := io.ReadAll(localResp.Body)
	if err != nil {
		t.Fatalf("read local response body: %v", err)
	}
	if localResp.StatusCode != http.StatusOK || string(localBody) != "local image" {
		t.Fatalf("local attachment response = status %d body %q", localResp.StatusCode, string(localBody))
	}
	if upstreamRequests != 1 {
		t.Fatalf("local attachment should not call upstream; upstreamRequests = %d", upstreamRequests)
	}
}

func TestDecodeNeoAttachmentPayloadMatchesBinaryImageLimits(t *testing.T) {
	data := testNeoPNGBase64(t, 1, 1)
	raw, mediaType, err := decodeNeoAttachmentPayload(data, "image/png")
	if err != nil {
		t.Fatalf("decode valid image: %v", err)
	}
	if len(raw) == 0 || mediaType != "image/png" {
		t.Fatalf("decoded image = len %d mediaType %q", len(raw), mediaType)
	}

	if _, _, err := decodeNeoAttachmentPayload(data, "image/avif"); err == nil || !strings.Contains(err.Error(), "Unsupported image media type") {
		t.Fatalf("unsupported media error = %v", err)
	}

	encoded := strings.Repeat("A", base64.StdEncoding.EncodedLen(neoAttachmentMaxImageBytes)+4)
	if _, _, err := decodeNeoAttachmentPayload(encoded, "image/png"); err == nil || !strings.Contains(err.Error(), "exceeds maximum allowed size") {
		t.Fatalf("oversized image error = %v", err)
	}

	oversizedDimensions := testNeoPNGBase64(t, neoAttachmentMaxImageDimension+1, 1)
	if _, _, err := decodeNeoAttachmentPayload(oversizedDimensions, "image/png"); err == nil || !strings.Contains(err.Error(), "Image dimensions too large") {
		t.Fatalf("oversized dimensions error = %v", err)
	}
}

func testNeoPNGBase64(t *testing.T, width, height int) string {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, width, height))
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		t.Fatalf("encode png: %v", err)
	}
	return base64.StdEncoding.EncodeToString(buf.Bytes())
}

func TestRegisterManagementRoutesDoesNotServeNeoThreadUsageLocally(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	threadID := "T-019e1046-656d-7132-879f-390ded941c16"

	enabled := true
	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime: newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
			NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled},
		}}),
	}
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/threads/"+threadID+"/usage", nil)
	req.Host = "127.0.0.1:8317"
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}

	htmlReq := httptest.NewRequest(http.MethodGet, "/threads/"+threadID+"/usage", nil)
	htmlReq.Header.Set("Accept", "text/html")
	htmlRec := httptest.NewRecorder()
	r.ServeHTTP(htmlRec, htmlReq)
	if htmlRec.Code != http.StatusServiceUnavailable {
		t.Fatalf("html response = status %d content-type %q body=%s", htmlRec.Code, htmlRec.Header().Get("Content-Type"), htmlRec.Body.String())
	}
}

func TestRegisterManagementRoutesPassesNeoThreadUsageUpstreamWhenProxyExists(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	enabled := true

	dir := t.TempDir()
	oldStoreDir := neoAmpDataDir
	neoAmpDataDir = func() string { return dir }
	t.Cleanup(func() { neoAmpDataDir = oldStoreDir })

	threadID := "T-019e1046-656d-7132-879f-390ded941c16"
	rawThread := []byte(`{"id":"` + threadID + `","title":"local usage test","messages":[{"messageId":"M-one","role":"assistant","usage":{"inputTokens":3,"outputTokens":5}}]}`)
	if err := os.WriteFile(filepath.Join(dir, threadID+".json"), rawThread, 0o600); err != nil {
		t.Fatalf("write local thread: %v", err)
	}

	upstreamRequests := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests++
		switch r.URL.Path {
		case "/api/threads/" + threadID + "/usage", "/threads/" + threadID + "/usage":
			writeNeoJSON(w, http.StatusOK, map[string]any{"threadID": threadID, "upstream": true})
		default:
			t.Fatalf("unexpected upstream request path=%s", r.URL.Path)
		}
	}))
	defer upstream.Close()

	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime: newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
			UpstreamURL:    upstream.URL,
			UpstreamAPIKey: "secret",
			NeoLocalRuntime: config.AmpNeoLocalRuntime{
				Enabled: &enabled,
			},
		}}),
	}
	proxy, _ := createReverseProxy(upstream.URL, NewStaticSecretSource(""))
	m.setProxy(proxy)
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	localServer := httptest.NewServer(r)
	defer localServer.Close()

	for _, path := range []string{"/api/threads/" + threadID + "/usage", "/threads/" + threadID + "/usage"} {
		t.Run(path, func(t *testing.T) {
			resp, err := http.Get(localServer.URL + path)
			if err != nil {
				t.Fatalf("get usage: %v", err)
			}
			defer func() {
				if err := resp.Body.Close(); err != nil {
					t.Fatalf("close response body: %v", err)
				}
			}()
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("read response body: %v", err)
			}
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, body=%s", resp.StatusCode, string(body))
			}
			var response map[string]any
			if err := json.Unmarshal(body, &response); err != nil {
				t.Fatalf("response JSON error: %v", err)
			}
			if response["upstream"] != true {
				t.Fatalf("expected upstream usage response, got %#v", response)
			}
		})
	}
	if upstreamRequests != 2 {
		t.Fatalf("upstreamRequests = %d, want 2", upstreamRequests)
	}
}

func TestRegisterManagementRoutesDoesNotServeNeoLegacyRunEndpointsLocally(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	enabled := true
	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime: newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
			NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled},
		}}),
	}
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	for _, path := range []string{
		"/api/threads",
		"/api/threads/runs",
		"/api/threads/T-legacy/runs",
		"/api/threads/T-legacy/runs/run_123",
		"/api/threads/T-legacy/runs/run_123/steps",
		"/api/threads/T-legacy/runs/run_123/cancel",
		"/api/threads/T-legacy/runs/run_123/submit_tool_outputs",
	} {
		t.Run(path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, path, bytes.NewBufferString(`{"assistant_id":"asst_test","stream":true}`))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, req)
			if rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestRegisterManagementRoutesPassesNeoLegacyRunEndpointsUpstreamWhenProxyExists(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	enabled := true
	upstreamRequests := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests++
		if r.URL.Path != "/api/threads/T-legacy/runs" {
			t.Fatalf("unexpected upstream path = %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusTeapot)
		_, _ = w.Write([]byte("proxied"))
	}))
	defer upstream.Close()

	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime: newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
			UpstreamURL: upstream.URL,
			NeoLocalRuntime: config.AmpNeoLocalRuntime{
				Enabled: &enabled,
			},
		}}),
	}
	proxy, _ := createReverseProxy(upstream.URL, NewStaticSecretSource(""))
	m.setProxy(proxy)
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	localServer := httptest.NewServer(r)
	defer localServer.Close()

	req, err := http.NewRequest(http.MethodPost, localServer.URL+"/api/threads/T-legacy/runs", bytes.NewBufferString(`{"assistant_id":"asst_test"}`))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}

	if resp.StatusCode != http.StatusTeapot {
		t.Fatalf("status = %d, body=%s", resp.StatusCode, string(body))
	}
	if upstreamRequests != 1 {
		t.Fatalf("expected one upstream request, got %d", upstreamRequests)
	}
	if string(body) != "proxied" {
		t.Fatalf("body = %q, want proxied", string(body))
	}
}

func TestRegisterManagementRoutesDoesNotSynthesizeAmpControlPlaneRPCs(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	enabled := true
	proxyCalled := false
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxyCalled = true
		w.WriteHeader(http.StatusTeapot)
	}))
	defer upstream.Close()

	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime: newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
			UpstreamURL:    upstream.URL,
			UpstreamAPIKey: "secret",
			NeoLocalRuntime: config.AmpNeoLocalRuntime{
				Enabled: &enabled,
			},
		}}),
	}
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	for _, method := range []string{
		"notices",
		"loadPlugins",
		"getUserInfo",
		"getUserFreeTierStatus",
		"logNoticeAction",
		"markAsReadMysteriousMessage",
		"userDisplayBalanceInfo",
		"ping",
		"listThreads",
		"loadThreads",
		"uploadThread",
		"getThread",
		"getThreadTail",
		"loadThreadTail",
		"readThread",
		"getThreadMeta",
		"setThreadMeta",
		"archiveThread",
		"deleteThread",
		"getThreadLabels",
		"setThreadLabels",
		"addThreadLabels",
		"getUserLabels",
		"listApps",
		"createApp",
		"updateApp",
		"deleteApp",
		"listEnvVarSecrets",
		"getEnvironmentVariable",
		"setEnvVarSecret",
		"deleteEnvVarSecret",
		"listWorkspaceDocs",
		"readWorkspaceDoc",
		"writeWorkspaceDoc",
		"createTask",
		"getTask",
		"listTasks",
		"updateTask",
		"deleteTask",
		"createRemoteExecutorThread",
		"extractWebPageContent",
		"getGitHubGitAccessToken",
		"sendReport",
		"shareThreadWithOperator",
		"signCommit",
		"webSearch2",
		"futureBinaryRPC",
	} {
		t.Run(method+" is not locally synthesized", func(t *testing.T) {
			proxyCalled = false
			body := bytes.NewBufferString(`{"method":"` + method + `","params":{"thread":"T-019e06a8-13c9-708d-8090-783005818ea7","taskID":"task-local"}}`)
			req := httptest.NewRequest(http.MethodPost, "/api/internal?"+method, body)
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, req)

			if rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
			}
			if proxyCalled {
				t.Fatalf("%s should not call the test upstream without a configured proxy", method)
			}
		})
	}
}

func TestRegisterManagementRoutesPassesInternalRPCsUpstreamWhenProxyExists(t *testing.T) {
	gin.SetMode(gin.TestMode)
	useTempNeoThreadStore(t)
	r := gin.New()
	enabled := true
	upstreamRequests := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests++
		if r.URL.Path != "/api/internal" {
			t.Fatalf("unexpected upstream request path=%s query=%s", r.URL.Path, r.URL.RawQuery)
		}
		if r.Header.Get("X-Test-Amp-Headers") == "required" && r.Header.Get("X-Amp-Client-Application") != "CLI" {
			t.Fatalf("X-Amp-Client-Application = %q", r.Header.Get("X-Amp-Client-Application"))
		}
		writeNeoJSON(w, http.StatusOK, map[string]any{"ok": true, "method": r.URL.RawQuery, "result": map[string]any{"threads": []any{}}})
	}))
	defer upstream.Close()

	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime: newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
			UpstreamURL:    upstream.URL,
			UpstreamAPIKey: "secret",
			NeoLocalRuntime: config.AmpNeoLocalRuntime{
				Enabled: &enabled,
			},
		}}),
	}
	proxy, _ := createReverseProxy(upstream.URL, NewStaticSecretSource(""))
	m.setProxy(proxy)
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	localServer := httptest.NewServer(r)
	defer localServer.Close()

	tests := []struct {
		name       string
		method     string
		body       string
		ampHeaders bool
	}{
		{name: "loadPlugins", method: "loadPlugins", body: `{"method":"loadPlugins","params":{}}`},
		{name: "getUserInfo", method: "getUserInfo", body: `{"method":"getUserInfo","params":{}}`},
		{name: "getThreadLinkInfo", method: "getThreadLinkInfo", body: `{"method":"getThreadLinkInfo","params":{"thread":"T-019e65c0-0310-77a8-b233-4b84d9c0612b"}}`},
		{name: "threadDisplayCostInfo", method: "threadDisplayCostInfo", body: `{"method":"threadDisplayCostInfo","params":{"threadID":"T-019e65c0-0310-77a8-b233-4b84d9c0612b"}}`},
		{name: "getUserFreeTierStatus", method: "getUserFreeTierStatus", body: `{"method":"getUserFreeTierStatus","params":{}}`},
		{name: "notices", method: "notices", body: `{"method":"notices","params":{}}`},
		{name: "logNoticeAction", method: "logNoticeAction", body: `{"method":"logNoticeAction","params":{"key":"local","action":"view"}}`},
		{name: "markAsReadMysteriousMessage", method: "markAsReadMysteriousMessage", body: `{"method":"markAsReadMysteriousMessage","params":{"messageId":"msg_local"}}`},
		{name: "userDisplayBalanceInfo", method: "userDisplayBalanceInfo", body: `{"method":"userDisplayBalanceInfo","params":{}}`},
		{name: "ping", method: "ping", body: `{"method":"ping","params":{}}`},
		{name: "listThreads web", method: "listThreads", body: `{"method":"listThreads","params":{"limit":10}}`},
		{name: "listThreads amp", method: "listThreads", body: `{"method":"listThreads","params":{"limit":10}}`, ampHeaders: true},
		{name: "loadThreads web", method: "loadThreads", body: `{"method":"loadThreads","params":{"threads":["T-019e65c0-0310-77a8-b233-4b84d9c0612b"]}}`},
		{name: "loadThreads amp", method: "loadThreads", body: `{"method":"loadThreads","params":{"threads":["T-019e65c0-0310-77a8-b233-4b84d9c0612b"]}}`, ampHeaders: true},
		{name: "getThread web", method: "getThread", body: `{"method":"getThread","params":{"thread":"T-019e65c0-0310-77a8-b233-4b84d9c0612b"}}`},
		{name: "getThread amp", method: "getThread", body: `{"method":"getThread","params":{"thread":"T-019e65c0-0310-77a8-b233-4b84d9c0612b"}}`, ampHeaders: true},
		{name: "getThreadTail", method: "getThreadTail", body: `{"method":"getThreadTail","params":{"thread":"T-019e65c0-0310-77a8-b233-4b84d9c0612b","limit":10}}`},
		{name: "loadThreadTail", method: "loadThreadTail", body: `{"method":"loadThreadTail","params":{"thread":"T-019e65c0-0310-77a8-b233-4b84d9c0612b","limit":10}}`},
		{name: "readThread", method: "readThread", body: `{"method":"readThread","params":{"thread":"T-019e65c0-0310-77a8-b233-4b84d9c0612b"}}`},
		{name: "uploadThread", method: "uploadThread", body: `{"method":"uploadThread","params":{"thread":{"id":"T-019e65c0-0310-77a8-b233-4b84d9c0612b"}}}`},
		{name: "getThreadMeta", method: "getThreadMeta", body: `{"method":"getThreadMeta","params":{"thread":"T-019e65c0-0310-77a8-b233-4b84d9c0612b"}}`},
		{name: "setThreadMeta", method: "setThreadMeta", body: `{"method":"setThreadMeta","params":{"thread":"T-019e65c0-0310-77a8-b233-4b84d9c0612b","meta":{}}}`},
		{name: "archiveThread", method: "archiveThread", body: `{"method":"archiveThread","params":{"thread":"T-019e65c0-0310-77a8-b233-4b84d9c0612b","archived":true}}`},
		{name: "deleteThread", method: "deleteThread", body: `{"method":"deleteThread","params":{"thread":"T-019e65c0-0310-77a8-b233-4b84d9c0612b"}}`},
		{name: "getThreadLabels", method: "getThreadLabels", body: `{"method":"getThreadLabels","params":{"thread":"T-019e65c0-0310-77a8-b233-4b84d9c0612b"}}`},
		{name: "setThreadLabels", method: "setThreadLabels", body: `{"method":"setThreadLabels","params":{"thread":"T-019e65c0-0310-77a8-b233-4b84d9c0612b","labels":[]}}`},
		{name: "addThreadLabels", method: "addThreadLabels", body: `{"method":"addThreadLabels","params":{"thread":"T-019e65c0-0310-77a8-b233-4b84d9c0612b","labels":[]}}`},
		{name: "getUserLabels", method: "getUserLabels", body: `{"method":"getUserLabels","params":{}}`},
		{name: "listApps", method: "listApps", body: `{"method":"listApps","params":{}}`},
		{name: "createApp", method: "createApp", body: `{"method":"createApp","params":{"threadID":"T-019e65c0-0310-77a8-b233-4b84d9c0612b"}}`},
		{name: "updateApp", method: "updateApp", body: `{"method":"updateApp","params":{"app":"local-app","name":"Local app"}}`},
		{name: "deleteApp", method: "deleteApp", body: `{"method":"deleteApp","params":{"app":"local-app"}}`},
		{name: "listEnvVarSecrets", method: "listEnvVarSecrets", body: `{"method":"listEnvVarSecrets","params":{"target":{"type":"user"}}}`},
		{name: "getEnvironmentVariable", method: "getEnvironmentVariable", body: `{"method":"getEnvironmentVariable","params":{"target":{"type":"user"},"name":"LOCAL_ENV"}}`},
		{name: "setEnvVarSecret", method: "setEnvVarSecret", body: `{"method":"setEnvVarSecret","params":{"target":{"type":"user"},"name":"LOCAL_SECRET","value":"value"}}`},
		{name: "deleteEnvVarSecret", method: "deleteEnvVarSecret", body: `{"method":"deleteEnvVarSecret","params":{"target":{"type":"user"},"name":"LOCAL_SECRET"}}`},
		{name: "listWorkspaceDocs", method: "listWorkspaceDocs", body: `{"method":"listWorkspaceDocs","params":{}}`},
		{name: "readWorkspaceDoc", method: "readWorkspaceDoc", body: `{"method":"readWorkspaceDoc","params":{"path":"docs/local.md"}}`},
		{name: "writeWorkspaceDoc", method: "writeWorkspaceDoc", body: `{"method":"writeWorkspaceDoc","params":{"path":"docs/local.md","content":"local"}}`},
		{name: "createTask", method: "createTask", body: `{"method":"createTask","params":{"title":"Run the build"}}`},
		{name: "getTask", method: "getTask", body: `{"method":"getTask","params":{"taskID":"task-local"}}`},
		{name: "listTasks", method: "listTasks", body: `{"method":"listTasks","params":{}}`},
		{name: "updateTask", method: "updateTask", body: `{"method":"updateTask","params":{"taskID":"task-local"}}`},
		{name: "deleteTask", method: "deleteTask", body: `{"method":"deleteTask","params":{"taskID":"task-local"}}`},
		{name: "createRemoteExecutorThread", method: "createRemoteExecutorThread", body: `{"method":"createRemoteExecutorThread","params":{"thread":"T-019e65c0-0310-77a8-b233-4b84d9c0612b"}}`},
		{name: "extractWebPageContent", method: "extractWebPageContent", body: `{"method":"extractWebPageContent","params":{"url":"https://example.test"}}`},
		{name: "getGitHubGitAccessToken", method: "getGitHubGitAccessToken", body: `{"method":"getGitHubGitAccessToken","params":{"repo":"owner/repo"}}`},
		{name: "sendReport", method: "sendReport", body: `{"method":"sendReport","params":{"message":"local"}}`},
		{name: "shareThreadWithOperator", method: "shareThreadWithOperator", body: `{"method":"shareThreadWithOperator","params":{"threadID":"T-019e65c0-0310-77a8-b233-4b84d9c0612b"}}`},
		{name: "signCommit", method: "signCommit", body: `{"method":"signCommit","params":{"repo":"owner/repo","commit":"abc123"}}`},
		{name: "webSearch2", method: "webSearch2", body: `{"method":"webSearch2","params":{"query":"amp parity"}}`},
		{name: "futureBinaryRPC", method: "futureBinaryRPC", body: `{"method":"futureBinaryRPC","params":{"probe":true}}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodPost, localServer.URL+"/api/internal?"+tc.method, bytes.NewBufferString(tc.body))
			if err != nil {
				t.Fatalf("new request: %v", err)
			}
			req.Header.Set("Content-Type", "application/json")
			if tc.ampHeaders {
				req.Header.Set("X-Amp-Client-Application", "CLI")
				req.Header.Set("X-Amp-Client-Type", "cli")
				req.Header.Set("X-Test-Amp-Headers", "required")
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("do request: %v", err)
			}
			defer func() {
				if err := resp.Body.Close(); err != nil {
					t.Fatalf("close response body: %v", err)
				}
			}()
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("read response body: %v", err)
			}

			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, body=%s", resp.StatusCode, string(body))
			}
			var response map[string]any
			if err := json.Unmarshal(body, &response); err != nil {
				t.Fatalf("response JSON error: %v", err)
			}
			if response["ok"] != true {
				t.Fatalf("unexpected upstream response: %#v", response)
			}
		})
	}
	if upstreamRequests != len(tests) {
		t.Fatalf("upstreamRequests = %d, want %d", upstreamRequests, len(tests))
	}
}

func TestRegisterManagementRoutesPurgesLocalThreadAfterSuccessfulUpstreamDelete(t *testing.T) {
	gin.SetMode(gin.TestMode)
	useTempNeoThreadStore(t)
	enabled := true
	successThreadID := "T-019e65c0-0310-77a8-b233-4b84d9c0612b"
	failureThreadID := "T-019e65c0-0310-77a8-b233-4b84d9c0612c"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := readAndRestoreNeoJSONBody(r)
		if err != nil {
			t.Fatalf("decode upstream request: %v", err)
		}
		threadID := neoInternalRPCThreadID(mapValue(body["params"]))
		code := "permission-denied"
		if threadID == successThreadID {
			code = "thread-not-found"
		}
		writeNeoJSON(w, http.StatusOK, map[string]any{"ok": false, "error": map[string]any{"code": code}})
	}))
	defer upstream.Close()

	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime: newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
			UpstreamURL:    upstream.URL,
			UpstreamAPIKey: "secret",
			NeoLocalRuntime: config.AmpNeoLocalRuntime{
				Enabled: &enabled,
			},
		}}),
	}
	for _, threadID := range []string{successThreadID, failureThreadID} {
		m.neoRuntime.store.ensureThreadActor(threadID)
		thread := map[string]any{
			"id":    threadID,
			"title": "Greeting",
			"messages": []any{
				map[string]any{"role": "user", "content": []any{map[string]any{"type": "text", "text": "hi"}}},
			},
		}
		if _, err := writeNeoLocalThreadFileInDir(m.neoRuntime.threadDir, threadID, thread); err != nil {
			t.Fatalf("write local thread %s: %v", threadID, err)
		}
	}
	proxy, err := createReverseProxy(upstream.URL, NewStaticSecretSource("secret"))
	if err != nil {
		t.Fatalf("create proxy: %v", err)
	}
	m.setProxy(proxy)
	r := gin.New()
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)
	server := httptest.NewServer(r)
	defer server.Close()

	for _, threadID := range []string{successThreadID, failureThreadID} {
		body := bytes.NewBufferString(`{"method":"deleteThread","params":{"thread":"` + threadID + `"}}`)
		resp, err := http.Post(server.URL+"/api/internal?deleteThread", "application/json", body)
		if err != nil {
			t.Fatalf("delete %s: %v", threadID, err)
		}
		if err := resp.Body.Close(); err != nil {
			t.Fatalf("close delete response: %v", err)
		}
	}

	if actor := m.neoRuntime.store.lookupThreadActor(successThreadID); actor != nil {
		t.Fatal("successfully deleted thread actor was retained")
	}
	if !m.neoRuntime.neoThreadDeleted(successThreadID) {
		t.Fatal("successfully deleted thread was not tombstoned")
	}
	for _, path := range []string{
		filepath.Join(m.neoRuntime.threadDir, successThreadID+".json"),
		neoWebLocalThreadSummaryPath(m.neoRuntime.threadDir, successThreadID),
	} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("deleted local path still exists %s: %v", path, err)
		}
	}
	if actor := m.neoRuntime.store.lookupThreadActor(failureThreadID); actor == nil {
		t.Fatal("failed upstream delete removed the local actor")
	}
	if m.neoRuntime.neoThreadDeleted(failureThreadID) {
		t.Fatal("failed upstream delete tombstoned the local thread")
	}
	if _, err := os.Stat(filepath.Join(m.neoRuntime.threadDir, failureThreadID+".json")); err != nil {
		t.Fatalf("failed upstream delete removed the local snapshot: %v", err)
	}
}

func TestRegisterManagementRoutesGetThreadKeepsUpstreamNotFoundForLocalNeoActor(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	enabled := true
	threadID := "T-019e65c0-0310-77a8-b233-4b84d9c06134"
	upstreamRequests := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests++
		if r.URL.Path != "/api/internal" || r.URL.RawQuery != "getThread" {
			t.Fatalf("unexpected upstream request path=%s query=%s", r.URL.Path, r.URL.RawQuery)
		}
		writeNeoJSON(w, http.StatusOK, map[string]any{"ok": false, "error": map[string]any{"code": "thread-not-found", "message": "Thread not found"}})
	}))
	defer upstream.Close()

	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime: newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
			UpstreamURL:    upstream.URL,
			UpstreamAPIKey: "secret",
			NeoLocalRuntime: config.AmpNeoLocalRuntime{
				Enabled: &enabled,
			},
		}}),
	}
	actor := m.neoRuntime.store.ensureThreadActor(threadID)
	actor.mu.Lock()
	actor.currentAgentMode = "review"
	actor.currentReasoningEffort = "medium"
	actor.settings["agentMode"] = "review"
	actor.settings["reasoning.effort"] = "medium"
	actor.meta = neoThreadActorImportedMeta(actor.meta)
	actor.mu.Unlock()
	proxy, _ := createReverseProxy(upstream.URL, NewStaticSecretSource("secret"))
	m.setProxy(proxy)
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	localServer := httptest.NewServer(r)
	defer localServer.Close()
	req, err := http.NewRequest(http.MethodPost, localServer.URL+"/api/internal?getThread", bytes.NewBufferString(`{"method":"getThread","params":{"thread":"`+threadID+`"}}`))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Fatalf("close response body: %v", err)
		}
	}()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body=%s", resp.StatusCode, string(body))
	}
	if upstreamRequests != 1 {
		t.Fatalf("upstreamRequests = %d, want 1", upstreamRequests)
	}
	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("response JSON error: %v", err)
	}
	if decoded["ok"] != false || stringValue(mapValue(decoded["error"])["code"]) != "thread-not-found" {
		t.Fatalf("unexpected missing-thread response: %#v", decoded)
	}
}

func TestRegisterManagementRoutesServesLocalThreadTailForThreadSwitchPreview(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	enabled := true
	threadID := "T-019f75e0-cb3d-7710-a5c9-4f419e25944d"
	threadDir := t.TempDir()
	messages := []any{
		map[string]any{"messageId": "M-preview-1", "role": "user", "createdAt": "2026-07-18T10:00:00Z", "content": []any{map[string]any{"type": "text", "text": "first preview message"}}},
		map[string]any{"messageId": "M-preview-2", "role": "assistant", "createdAt": "2026-07-18T10:00:01Z", "content": []any{map[string]any{"type": "text", "text": "second preview message"}}},
		map[string]any{"messageId": "M-preview-3", "role": "user", "createdAt": "2026-07-18T10:00:02Z", "content": []any{map[string]any{"type": "text", "text": "third preview message"}}},
	}
	if _, err := writeNeoLocalThreadFileInDir(threadDir, threadID, map[string]any{
		"id":        threadID,
		"title":     "Local thread switch preview",
		"created":   time.Date(2026, time.July, 18, 10, 0, 0, 0, time.UTC).UnixMilli(),
		"agentMode": "smart",
		"settings":  map[string]any{"agentMode": "smart"},
		"meta":      map[string]any{"cliProxyAPILocalNeo": true},
		"messages":  messages,
	}); err != nil {
		t.Fatalf("write local thread: %v", err)
	}

	upstreamRequests := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests++
		w.WriteHeader(http.StatusTeapot)
	}))
	defer upstream.Close()
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
		UpstreamURL:    upstream.URL,
		UpstreamAPIKey: "secret",
		NeoLocalRuntime: config.AmpNeoLocalRuntime{
			Enabled: &enabled,
		},
	}})
	rt.threadDir = threadDir
	t.Cleanup(func() { rt.store.disposeAll(true, "test done", false) })
	m := &AmpModule{restrictToLocalhost: false, neoRuntime: rt}
	proxy, err := createReverseProxy(upstream.URL, NewStaticSecretSource("secret"))
	if err != nil {
		t.Fatalf("create proxy: %v", err)
	}
	m.setProxy(proxy)
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	req := httptest.NewRequest(http.MethodPost, "/api/internal?getThreadTail", bytes.NewBufferString(`{"method":"getThreadTail","params":{"thread":"`+threadID+`","limit":2}}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if upstreamRequests != 0 {
		t.Fatalf("upstreamRequests = %d, want 0", upstreamRequests)
	}
	response := readNeoJSON(rec.Body)
	if response["ok"] != true {
		t.Fatalf("response = %#v, want ok", response)
	}
	result := mapValue(response["result"])
	thread := mapValue(result["thread"])
	if stringValue(thread["id"]) != threadID || stringValue(thread["title"]) != "Local thread switch preview" || stringValue(thread["creatorUserID"]) == "" || stringValue(thread["updatedAt"]) == "" {
		t.Fatalf("thread tail metadata = %#v", thread)
	}
	data := mapValue(thread["data"])
	if stringValue(data["id"]) != threadID || data["messages"] != nil {
		t.Fatalf("thread tail data = %#v", data)
	}
	tail := arrayValue(result["messages"])
	if len(tail) != 2 || stringValue(mapValue(tail[0])["role"]) != "assistant" || stringValue(mapValue(tail[1])["role"]) != "user" || neoWebLocalProjectThreadContentText(mapValue(tail[0])["content"]) != "second preview message" || neoWebLocalProjectThreadContentText(mapValue(tail[1])["content"]) != "third preview message" {
		t.Fatalf("thread tail messages = %#v", tail)
	}
	if result["hasMoreBefore"] != true {
		t.Fatalf("hasMoreBefore = %#v, want true", result["hasMoreBefore"])
	}
}

func TestRegisterManagementRoutesGetThreadUsesUpstreamSuccessWithLocalActor(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	enabled := true
	threadID := "T-019e65c0-0310-77a8-b233-4b84d9c06135"
	upstreamRequests := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests++
		if r.URL.Path != "/api/internal" || r.URL.RawQuery != "getThread" {
			t.Fatalf("unexpected upstream request path=%s query=%s", r.URL.Path, r.URL.RawQuery)
		}
		writeNeoJSON(w, http.StatusOK, map[string]any{
			"ok":     true,
			"result": map[string]any{"thread": map[string]any{"id": threadID, "title": "upstream thread", "agentMode": "deep"}},
		})
	}))
	defer upstream.Close()

	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime: newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
			UpstreamURL:    upstream.URL,
			UpstreamAPIKey: "secret",
			NeoLocalRuntime: config.AmpNeoLocalRuntime{
				Enabled: &enabled,
			},
		}}),
	}
	actor := m.neoRuntime.store.ensureThreadActor(threadID)
	actor.mu.Lock()
	actor.title = "local stale title"
	actor.settings["agentMode"] = "review"
	actor.mu.Unlock()
	proxy, _ := createReverseProxy(upstream.URL, NewStaticSecretSource("secret"))
	m.setProxy(proxy)
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	localServer := httptest.NewServer(r)
	defer localServer.Close()
	resp, err := http.Post(localServer.URL+"/api/internal?getThread", "application/json", bytes.NewBufferString(`{"method":"getThread","params":{"thread":"`+threadID+`"}}`))
	if err != nil {
		t.Fatalf("post getThread: %v", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Fatalf("close response body: %v", err)
		}
	}()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body=%s", resp.StatusCode, string(body))
	}
	if upstreamRequests != 1 {
		t.Fatalf("upstreamRequests = %d, want 1", upstreamRequests)
	}
	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("response JSON error: %v", err)
	}
	thread := mapValue(mapValue(decoded["result"])["thread"])
	if stringValue(thread["title"]) != "upstream thread" || stringValue(thread["agentMode"]) != "deep" {
		t.Fatalf("unexpected upstream-preserved response: %#v", decoded)
	}
}

func TestRegisterManagementRoutesGetThreadKeepsUpstreamNotFoundWithoutLocalActor(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	enabled := true
	threadID := "T-019e65c0-0310-77a8-b233-4b84d9c06136"
	upstreamRequests := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests++
		if r.URL.Path != "/api/internal" || r.URL.RawQuery != "getThread" {
			t.Fatalf("unexpected upstream request path=%s query=%s", r.URL.Path, r.URL.RawQuery)
		}
		writeNeoJSON(w, http.StatusOK, map[string]any{"ok": false, "error": map[string]any{"code": "thread-not-found", "message": "Thread not found"}})
	}))
	defer upstream.Close()

	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime: newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
			UpstreamURL:    upstream.URL,
			UpstreamAPIKey: "secret",
			NeoLocalRuntime: config.AmpNeoLocalRuntime{
				Enabled: &enabled,
			},
		}}),
	}
	proxy, _ := createReverseProxy(upstream.URL, NewStaticSecretSource("secret"))
	m.setProxy(proxy)
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	localServer := httptest.NewServer(r)
	defer localServer.Close()
	resp, err := http.Post(localServer.URL+"/api/internal?getThread", "application/json", bytes.NewBufferString(`{"method":"getThread","params":{"thread":"`+threadID+`"}}`))
	if err != nil {
		t.Fatalf("post getThread: %v", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Fatalf("close response body: %v", err)
		}
	}()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body=%s", resp.StatusCode, string(body))
	}
	if upstreamRequests != 1 {
		t.Fatalf("upstreamRequests = %d, want 1", upstreamRequests)
	}
	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("response JSON error: %v", err)
	}
	if decoded["ok"] != false || stringValue(mapValue(decoded["error"])["code"]) != "thread-not-found" {
		t.Fatalf("unexpected missing-thread response: %#v", decoded)
	}
}

func TestRegisterManagementRoutesBypassesInternalRPCAuthForLocalConnectionsOnly(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	upstreamRequests := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests++
		if r.URL.Path != "/api/internal" || r.URL.RawQuery != "listThreads" {
			t.Fatalf("unexpected upstream request path=%s query=%s", r.URL.Path, r.URL.RawQuery)
		}
		writeNeoJSON(w, http.StatusOK, map[string]any{"ok": true, "result": map[string]any{"threads": []any{}}})
	}))
	defer upstream.Close()

	m := &AmpModule{restrictToLocalhost: false}
	proxy, err := createReverseProxy(upstream.URL, NewStaticSecretSource("secret"))
	if err != nil {
		t.Fatalf("create proxy: %v", err)
	}
	m.setProxy(proxy)

	authCalls := 0
	auth := func(c *gin.Context) {
		authCalls++
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Missing API key"})
	}
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, auth)

	localServer := httptest.NewServer(r)
	defer localServer.Close()

	body := `{"method":"listThreads","params":{"limit":20}}`
	localReq, err := http.NewRequest(http.MethodPost, localServer.URL+"/api/internal?listThreads", strings.NewReader(body))
	if err != nil {
		t.Fatalf("new local request: %v", err)
	}
	localReq.Header.Set("Content-Type", "application/json")
	localResp, err := http.DefaultClient.Do(localReq)
	if err != nil {
		t.Fatalf("do local request: %v", err)
	}
	defer func() {
		if err := localResp.Body.Close(); err != nil {
			t.Fatalf("close local response: %v", err)
		}
	}()
	localBody, err := io.ReadAll(localResp.Body)
	if err != nil {
		t.Fatalf("read local response: %v", err)
	}
	if localResp.StatusCode != http.StatusOK {
		t.Fatalf("local status = %d, body=%s", localResp.StatusCode, string(localBody))
	}
	if authCalls != 0 {
		t.Fatalf("authCalls after local request = %d, want 0", authCalls)
	}
	if upstreamRequests != 1 {
		t.Fatalf("upstreamRequests after local request = %d, want 1", upstreamRequests)
	}

	localInterfaceServed := false
	if localIP := firstNonLoopbackLocalIPForTest(t); localIP != "" {
		ln, err := net.Listen("tcp", net.JoinHostPort(localIP, "0"))
		if err != nil {
			t.Logf("skipping local interface route coverage: %v", err)
		} else {
			srv := &http.Server{Handler: r}
			go func() { _ = srv.Serve(ln) }()
			t.Cleanup(func() { _ = srv.Close() })

			_, port, err := net.SplitHostPort(ln.Addr().String())
			if err != nil {
				t.Fatalf("split local listener addr: %v", err)
			}
			localInterfaceReq, err := http.NewRequest(http.MethodPost, "http://"+net.JoinHostPort(localIP, port)+"/api/internal?listThreads", strings.NewReader(body))
			if err != nil {
				t.Fatalf("new local interface request: %v", err)
			}
			localInterfaceReq.Header.Set("Content-Type", "application/json")
			localTransport := &http.Transport{Proxy: nil}
			defer localTransport.CloseIdleConnections()
			localClient := &http.Client{Transport: localTransport}
			localInterfaceResp, err := localClient.Do(localInterfaceReq)
			if err != nil {
				t.Fatalf("do local interface request: %v", err)
			}
			defer func() {
				if err := localInterfaceResp.Body.Close(); err != nil {
					t.Fatalf("close local interface response: %v", err)
				}
			}()
			localInterfaceBody, err := io.ReadAll(localInterfaceResp.Body)
			if err != nil {
				t.Fatalf("read local interface response: %v", err)
			}
			if localInterfaceResp.StatusCode != http.StatusOK {
				t.Fatalf("local interface status = %d, body=%s", localInterfaceResp.StatusCode, string(localInterfaceBody))
			}
			if authCalls != 0 {
				t.Fatalf("authCalls after local interface request = %d, want 0", authCalls)
			}
			if upstreamRequests != 2 {
				t.Fatalf("upstreamRequests after local interface request = %d, want 2", upstreamRequests)
			}
			localInterfaceServed = true
		}
	}

	remoteReq := httptest.NewRequest(http.MethodPost, "/api/internal?listThreads", strings.NewReader(body))
	remoteReq.RemoteAddr = "203.0.113.42:54321"
	remoteReq.Header.Set("Content-Type", "application/json")
	remoteRec := httptest.NewRecorder()
	r.ServeHTTP(remoteRec, remoteReq)
	if remoteRec.Code != http.StatusUnauthorized {
		t.Fatalf("remote status = %d, body=%s", remoteRec.Code, remoteRec.Body.String())
	}
	if authCalls != 1 {
		t.Fatalf("authCalls after remote request = %d, want 1", authCalls)
	}
	wantUpstreamRequests := 1
	if localInterfaceServed {
		wantUpstreamRequests = 2
	}
	if upstreamRequests != wantUpstreamRequests {
		t.Fatalf("upstreamRequests after remote request = %d, want %d", upstreamRequests, wantUpstreamRequests)
	}
}

func TestRegisterManagementRoutesPassesThreadGETsUpstreamWhenProxyExists(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	enabled := true

	dir := t.TempDir()
	oldStoreDir := neoAmpDataDir
	neoAmpDataDir = func() string { return dir }
	t.Cleanup(func() { neoAmpDataDir = oldStoreDir })

	threadID := "T-019e65c0-0310-77a8-b233-4b84d9c0612b"
	rawThread := []byte(`{"id":"` + threadID + `","title":"local thread","messages":[{"messageId":"M-local","role":"user","content":[{"type":"text","text":"local needle"}]}]}`)
	if err := os.WriteFile(filepath.Join(dir, threadID+".json"), rawThread, 0o600); err != nil {
		t.Fatalf("write local thread: %v", err)
	}

	upstreamRequests := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests++
		switch r.URL.Path {
		case "/api/threads/find":
			writeNeoJSON(w, http.StatusOK, map[string]any{
				"threads": []any{map[string]any{"id": threadID, "title": "upstream search"}},
			})
		case "/api/threads/" + threadID:
			writeNeoJSON(w, http.StatusOK, map[string]any{"id": threadID, "title": "upstream api thread"})
		case "/api/threads/" + threadID + "/diff-captures":
			if r.Method != http.MethodPost {
				t.Fatalf("diff capture method = %s, want POST", r.Method)
			}
			writeNeoJSON(w, http.StatusOK, map[string]any{"captureID": "capture-upstream"})
		case "/threads/" + threadID:
			writeNeoJSON(w, http.StatusOK, map[string]any{"id": threadID, "title": "upstream thread"})
		default:
			t.Fatalf("unexpected upstream request path=%s", r.URL.Path)
		}
	}))
	defer upstream.Close()

	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime: newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
			UpstreamURL:    upstream.URL,
			UpstreamAPIKey: "secret",
			NeoLocalRuntime: config.AmpNeoLocalRuntime{
				Enabled: &enabled,
			},
		}}),
	}
	proxy, _ := createReverseProxy(upstream.URL, NewStaticSecretSource(""))
	m.setProxy(proxy)
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	localServer := httptest.NewServer(r)
	defer localServer.Close()

	tests := []struct {
		name       string
		path       string
		ampHeaders bool
		wantTitle  string
	}{
		{name: "find web", path: "/api/threads/find?q=local+needle&limit=5", wantTitle: "upstream search"},
		{name: "api thread web", path: "/api/threads/" + threadID, wantTitle: "upstream api thread"},
		{name: "thread web", path: "/threads/" + threadID, wantTitle: "upstream thread"},
		{name: "find amp", path: "/api/threads/find?q=local+needle&limit=5", ampHeaders: true, wantTitle: "upstream search"},
		{name: "api thread amp", path: "/api/threads/" + threadID, ampHeaders: true, wantTitle: "upstream api thread"},
		{name: "thread amp", path: "/threads/" + threadID, ampHeaders: true, wantTitle: "upstream thread"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, localServer.URL+tc.path, nil)
			if err != nil {
				t.Fatalf("new request: %v", err)
			}
			if tc.ampHeaders {
				req.Header.Set("X-Amp-Client-Application", "CLI")
				req.Header.Set("X-Amp-Client-Type", "cli")
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("do request: %v", err)
			}
			defer func() {
				if err := resp.Body.Close(); err != nil {
					t.Fatalf("close response body: %v", err)
				}
			}()
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("read response body: %v", err)
			}
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, body=%s", resp.StatusCode, string(body))
			}
			var response map[string]any
			if err := json.Unmarshal(body, &response); err != nil {
				t.Fatalf("response JSON error: %v", err)
			}
			title := stringValue(response["title"])
			if title == "" {
				threads := arrayValue(response["threads"])
				if len(threads) > 0 {
					title = stringValue(mapValue(threads[0])["title"])
				}
			}
			if title != tc.wantTitle {
				t.Fatalf("title = %q, want %q; response=%#v", title, tc.wantTitle, response)
			}
		})
	}
	captureReq, err := http.NewRequest(http.MethodPost, localServer.URL+"/api/threads/"+threadID+"/diff-captures", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("new diff capture request: %v", err)
	}
	captureReq.Header.Set("Content-Type", "application/json")
	captureResp, err := http.DefaultClient.Do(captureReq)
	if err != nil {
		t.Fatalf("do diff capture request: %v", err)
	}
	defer func() {
		if err := captureResp.Body.Close(); err != nil {
			t.Fatalf("close diff capture response: %v", err)
		}
	}()
	var capture map[string]any
	if err := json.NewDecoder(captureResp.Body).Decode(&capture); err != nil {
		t.Fatalf("decode diff capture response: %v", err)
	}
	if captureResp.StatusCode != http.StatusOK || capture["captureID"] != "capture-upstream" {
		t.Fatalf("diff capture response status=%d body=%#v", captureResp.StatusCode, capture)
	}
	if upstreamRequests != len(tests)+1 {
		t.Fatalf("upstreamRequests = %d, want %d", upstreamRequests, len(tests)+1)
	}
}

func TestRegisterManagementRoutesFallsBackToLocalThreadSearchOnUpstreamTimeout(t *testing.T) {
	gin.SetMode(gin.TestMode)
	useTempNeoThreadStore(t)
	r := gin.New()
	enabled := true

	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
		UpstreamAPIKey: "secret",
		NeoLocalRuntime: config.AmpNeoLocalRuntime{
			Enabled: &enabled,
		},
	}})
	threadID := "T-019e65c0-0310-77a8-b233-4b84d9c0612c"
	if err := writeNeoLocalThreadSnapshotToDir(neoCloudThreadSnapshot{
		threadID:  threadID,
		seq:       1,
		createdMs: 1778170000000,
		title:     "Local amp-classic search",
		messages: []neoMessage{{
			ThreadID:  threadID,
			MessageID: "M-local",
			Role:      "user",
			Content:   []any{map[string]any{"type": "text", "text": "debug amp-classic timeout fallback"}},
			Seq:       1,
		}},
	}, rt.threadDir); err != nil {
		t.Fatalf("write local thread: %v", err)
	}

	upstreamRequests := 0
	identitySearchRequests := 0
	searchAcceptEncodings := []string{}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests++
		if r.URL.Path != "/api/threads/find" {
			t.Fatalf("unexpected upstream request path=%s", r.URL.Path)
		}
		if r.URL.Query().Get("q") == "author:me" {
			searchAcceptEncodings = append(searchAcceptEncodings, r.Header.Get("Accept-Encoding"))
			if r.URL.Query().Get("offset") != "0" {
				t.Fatalf("upstream search offset = %q, want 0", r.URL.Query().Get("offset"))
			}
			payload := map[string]any{"threads": []any{map[string]any{"id": "T-019f75f2-5bf4-736b-909c-a71b64a1d9c2", "title": "upstream search"}}, "hasMore": false}
			if r.Header.Get("Accept-Encoding") != "identity" {
				encoded, err := json.Marshal(payload)
				if err != nil {
					t.Fatal(err)
				}
				w.Header().Set("Content-Encoding", "gzip")
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				if _, err := w.Write(gzipBytes(encoded)); err != nil {
					t.Fatal(err)
				}
				return
			}
			identitySearchRequests++
			writeNeoJSON(w, http.StatusOK, payload)
			return
		}
		if r.URL.Query().Get("q") == "not-a-budget-timeout" {
			writeNeoJSON(w, http.StatusRequestTimeout, map[string]any{"error": map[string]any{"code": "other-timeout", "message": "time-budget-exceeded text is not enough"}})
			return
		}
		writeNeoJSON(w, http.StatusRequestTimeout, map[string]any{"error": map[string]any{"code": "time-budget-exceeded", "message": "too broad"}})
	}))
	defer upstream.Close()

	rt.cfg.AmpCode.UpstreamURL = upstream.URL
	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime:          rt,
	}
	proxy, _ := createReverseProxy(upstream.URL, NewStaticSecretSource(""))
	m.setProxy(proxy)
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	localServer := httptest.NewServer(r)
	defer localServer.Close()

	search := func(path string) map[string]any {
		t.Helper()
		resp, err := http.Get(localServer.URL + path)
		if err != nil {
			t.Fatalf("get search: %v", err)
		}
		defer func() {
			if err := resp.Body.Close(); err != nil {
				t.Fatalf("close response body: %v", err)
			}
		}()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("read response body: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, body=%s", resp.StatusCode, string(body))
		}
		var response map[string]any
		if err := json.Unmarshal(body, &response); err != nil {
			t.Fatalf("response JSON error: %v", err)
		}
		return response
	}

	response := search("/api/threads/find?q=author%3Ame+amp-classic&limit=5")
	threads := arrayValue(response["threads"])
	if len(threads) != 1 {
		t.Fatalf("threads = %#v", response["threads"])
	}
	thread := mapValue(threads[0])
	if thread["id"] != threadID || stringValue(thread["title"]) != "Local amp-classic search" {
		t.Fatalf("thread = %#v", thread)
	}
	if !strings.Contains(stringValue(thread["matchedSearchText"]), "amp-classic") {
		t.Fatalf("matchedSearchText = %q", stringValue(thread["matchedSearchText"]))
	}

	response = search("/api/threads/find?q=author%3Ame&limit=5")
	threads = arrayValue(response["threads"])
	if len(threads) != 2 || stringValue(mapValue(threads[0])["id"]) != threadID || stringValue(mapValue(threads[1])["title"]) != "upstream search" {
		t.Fatalf("merged search threads = %#v", threads)
	}

	response = search("/api/threads/find?q=author%3Ame&offset=1&limit=1")
	threads = arrayValue(response["threads"])
	if len(threads) != 1 || stringValue(mapValue(threads[0])["title"]) != "upstream search" {
		t.Fatalf("paged merged search threads = %#v", threads)
	}

	response = search("/api/threads/find?q=definitely-no-local-match&limit=5")
	if threads := arrayValue(response["threads"]); len(threads) != 0 || boolValue(response["hasMore"]) {
		t.Fatalf("empty fallback response = %#v", response)
	}

	resp, err := http.Get(localServer.URL + "/api/threads/find?q=not-a-budget-timeout&limit=5")
	if err != nil {
		t.Fatalf("get non-budget timeout search: %v", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Fatalf("close non-budget timeout response body: %v", err)
		}
	}()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read non-budget timeout body: %v", err)
	}
	if resp.StatusCode != http.StatusRequestTimeout || !strings.Contains(string(body), "other-timeout") {
		t.Fatalf("non-budget timeout status=%d body=%s", resp.StatusCode, string(body))
	}

	if upstreamRequests != 5 {
		t.Fatalf("upstreamRequests = %d, want 5", upstreamRequests)
	}
	if identitySearchRequests != 2 {
		t.Fatalf("identity search requests = %d, want 2; encodings=%#v", identitySearchRequests, searchAcceptEncodings)
	}
}

func TestMergeNeoThreadSearchResponsesPaginatesCombinedResults(t *testing.T) {
	local := map[string]any{"threads": []any{
		map[string]any{"id": "T-019f75f2-5bf4-736b-909c-000000000001"},
		map[string]any{"id": "T-019f75f2-5bf4-736b-909c-000000000002"},
	}}
	upstream := map[string]any{"threads": []any{
		map[string]any{"id": "T-019f75f2-5bf4-736b-909c-000000000003"},
		map[string]any{"id": "T-019f75f2-5bf4-736b-909c-000000000004"},
	}}

	first := arrayValue(mergeNeoThreadSearchResponses(upstream, local, 0, 2)["threads"])
	second := arrayValue(mergeNeoThreadSearchResponses(upstream, local, 2, 2)["threads"])
	if len(first) != 2 || stringValue(mapValue(first[0])["id"]) != "T-019f75f2-5bf4-736b-909c-000000000001" || stringValue(mapValue(first[1])["id"]) != "T-019f75f2-5bf4-736b-909c-000000000002" {
		t.Fatalf("first merged page = %#v", first)
	}
	if len(second) != 2 || stringValue(mapValue(second[0])["id"]) != "T-019f75f2-5bf4-736b-909c-000000000003" || stringValue(mapValue(second[1])["id"]) != "T-019f75f2-5bf4-736b-909c-000000000004" {
		t.Fatalf("second merged page = %#v", second)
	}
}

func TestRegisterManagementRoutesPassesThreadReaderToolsUpstreamWhenProxyExists(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	enabled := true

	dir := t.TempDir()
	oldStoreDir := neoAmpDataDir
	neoAmpDataDir = func() string { return dir }
	t.Cleanup(func() { neoAmpDataDir = oldStoreDir })

	threadID := "T-019e65c0-0310-77a8-b233-4b84d9c0612d"
	rawThread := []byte(`{"id":"` + threadID + `","title":"local thread","messages":[{"messageId":"M-local","role":"user","content":[{"type":"text","text":"local content"}]}]}`)
	if err := os.WriteFile(filepath.Join(dir, threadID+".json"), rawThread, 0o600); err != nil {
		t.Fatalf("write local thread: %v", err)
	}

	upstreamRequests := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests++
		switch r.URL.Path {
		case "/api/threads/" + threadID + "/messages/message_stats":
			writeNeoJSON(w, http.StatusOK, map[string]any{"messageCount": 99, "upstream": true})
		case "/api/threads/" + threadID + "/messages/read_messages":
			writeNeoJSON(w, http.StatusOK, map[string]any{"content": "upstream read_messages", "upstream": true})
		case "/api/threads/" + threadID + "/messages/search_messages":
			writeNeoJSON(w, http.StatusOK, map[string]any{"matches": []any{"upstream search_messages"}, "upstream": true})
		case "/api/threads/" + threadID + "/messages/M-reader":
			writeNeoJSON(w, http.StatusOK, map[string]any{"messages": []any{map[string]any{"messageId": "M-reader", "upstream": true}}})
		default:
			t.Fatalf("unexpected upstream request path=%s", r.URL.Path)
		}
	}))
	defer upstream.Close()

	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime: newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
			UpstreamURL:    upstream.URL,
			UpstreamAPIKey: "secret",
			NeoLocalRuntime: config.AmpNeoLocalRuntime{
				Enabled: &enabled,
			},
		}}),
	}
	proxy, _ := createReverseProxy(upstream.URL, NewStaticSecretSource(""))
	m.setProxy(proxy)
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	localServer := httptest.NewServer(r)
	defer localServer.Close()

	tests := []struct {
		name string
		path string
		body string
	}{
		{name: "message stats", path: "/api/threads/" + threadID + "/messages/message_stats", body: `{}`},
		{name: "binary read messages", path: "/api/threads/" + threadID + "/messages/read_messages", body: `{"startIndex":0,"limit":20}`},
		{name: "binary search messages", path: "/api/threads/" + threadID + "/messages/search_messages", body: `{"query":"local","startIndex":0,"endIndex":3}`},
		{name: "message reader", path: "/api/threads/" + threadID + "/messages/M-reader", body: `{"limit":20}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodPost, localServer.URL+tc.path, bytes.NewBufferString(tc.body))
			if err != nil {
				t.Fatalf("new request: %v", err)
			}
			req.Header.Set("Content-Type", "application/json")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("do request: %v", err)
			}
			defer func() {
				if err := resp.Body.Close(); err != nil {
					t.Fatalf("close response body: %v", err)
				}
			}()
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("read response body: %v", err)
			}

			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, body=%s", resp.StatusCode, string(body))
			}
			var response map[string]any
			if err := json.Unmarshal(body, &response); err != nil {
				t.Fatalf("response JSON error: %v", err)
			}
			if response["upstream"] != true && len(arrayValue(response["messages"])) == 0 {
				t.Fatalf("unexpected upstream reader response: %#v", response)
			}
		})
	}
	if upstreamRequests != len(tests) {
		t.Fatalf("upstreamRequests = %d, want %d", upstreamRequests, len(tests))
	}
}

func TestRegisterManagementRoutesDoesNotServeAmpOwnedInternalMethodsLocally(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	enabled := true
	proxyCalled := false
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxyCalled = true
		w.WriteHeader(http.StatusTeapot)
	}))
	defer upstream.Close()

	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime: newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
			UpstreamURL:    upstream.URL,
			UpstreamAPIKey: "secret",
			NeoLocalRuntime: config.AmpNeoLocalRuntime{
				Enabled: &enabled,
			},
		}}),
	}
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	for _, method := range []string{"createTask", "getTask", "listTasks", "updateTask", "deleteTask", "getThreadLinkInfo", "threadDisplayCostInfo"} {
		t.Run(method, func(t *testing.T) {
			proxyCalled = false
			req := httptest.NewRequest(http.MethodPost, "/api/internal?"+method, bytes.NewBufferString(`{"method":"`+method+`","params":{"taskID":"task-local","title":"Run the build"}}`))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, req)

			if rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("%s status = %d, body=%s", method, rec.Code, rec.Body.String())
			}
			if proxyCalled {
				t.Fatalf("%s should not be served locally or call upstream without a configured proxy", method)
			}
		})
	}
}

func TestRegisterManagementRoutesDoesNotServeThreadDiscoveryLocallyWithoutProxy(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	enabled := true

	dir := t.TempDir()
	oldStoreDir := neoAmpDataDir
	neoAmpDataDir = func() string { return dir }
	t.Cleanup(func() { neoAmpDataDir = oldStoreDir })

	threadID := "T-019e65c0-0310-77a8-b233-4b84d9c0612e"
	rawThread := []byte(`{"id":"` + threadID + `","title":"local-only thread","messages":[{"messageId":"M-local","role":"user","content":[{"type":"text","text":"local needle"}]}]}`)
	if err := os.WriteFile(filepath.Join(dir, threadID+".json"), rawThread, 0o600); err != nil {
		t.Fatalf("write local thread: %v", err)
	}

	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime: newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
			UpstreamURL:    "https://ampcode.test",
			UpstreamAPIKey: "secret",
			NeoLocalRuntime: config.AmpNeoLocalRuntime{
				Enabled: &enabled,
			},
		}}),
	}
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	tests := []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{name: "internal list threads", method: http.MethodPost, path: "/api/internal?listThreads", body: `{"method":"listThreads","params":{"limit":20}}`},
		{name: "internal load threads", method: http.MethodPost, path: "/api/internal?loadThreads", body: `{"method":"loadThreads","params":{"threads":["` + threadID + `"]}}`},
		{name: "internal get thread", method: http.MethodPost, path: "/api/internal?getThread", body: `{"method":"getThread","params":{"thread":"` + threadID + `"}}`},
		{name: "internal load thread tail", method: http.MethodPost, path: "/api/internal?loadThreadTail", body: `{"method":"loadThreadTail","params":{"thread":"` + threadID + `","limit":10}}`},
		{name: "internal read thread", method: http.MethodPost, path: "/api/internal?readThread", body: `{"method":"readThread","params":{"thread":"` + threadID + `"}}`},
		{name: "thread search", method: http.MethodGet, path: "/api/threads/find?q=local+needle&limit=5"},
		{name: "api thread read", method: http.MethodGet, path: "/api/threads/" + threadID},
		{name: "thread read", method: http.MethodGet, path: "/threads/" + threadID},
		{name: "thread reader stats", method: http.MethodPost, path: "/api/threads/" + threadID + "/messages/message_stats", body: `{}`},
		{name: "thread reader read messages", method: http.MethodPost, path: "/api/threads/" + threadID + "/messages/read_messages", body: `{"startIndex":0,"limit":20}`},
		{name: "thread reader search messages", method: http.MethodPost, path: "/api/threads/" + threadID + "/messages/search_messages", body: `{"query":"local needle","startIndex":0,"endIndex":3}`},
		{name: "thread reader message", method: http.MethodPost, path: "/api/threads/" + threadID + "/messages/M-local", body: `{}`},
		{name: "user actor credentials", method: http.MethodPost, path: "/api/user-actor-credentials", body: `{}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, bytes.NewBufferString(tc.body))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, req)

			if rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
			}
			if strings.Contains(rec.Body.String(), "local needle") || strings.Contains(rec.Body.String(), "local-only thread") {
				t.Fatalf("response leaked local thread data: %s", rec.Body.String())
			}
		})
	}
}

func TestRegisterManagementRoutesGetThreadLinkInfoProxiesWhenProxyExists(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	enabled := true
	upstreamRequests := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests++
		w.WriteHeader(http.StatusTeapot)
		_, _ = w.Write([]byte("proxied"))
	}))
	defer upstream.Close()

	dir := t.TempDir()
	oldStoreDir := neoAmpDataDir
	neoAmpDataDir = func() string { return dir }
	t.Cleanup(func() { neoAmpDataDir = oldStoreDir })

	threadID := "T-019e1046-656d-7132-879f-390ded941c16"
	raw := []byte(`{
		"id": "` + threadID + `",
		"title": "continued locally",
		"creatorUserID": "user_cloud",
		"ownerUserId": "user_cloud"
	}`)
	if err := os.WriteFile(filepath.Join(dir, threadID+".json"), raw, 0o600); err != nil {
		t.Fatalf("write local thread: %v", err)
	}

	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime: newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
			UpstreamURL: upstream.URL,
			NeoLocalRuntime: config.AmpNeoLocalRuntime{
				Enabled: &enabled,
			},
		}}),
	}
	proxy, _ := createReverseProxy(upstream.URL, NewStaticSecretSource(""))
	m.setProxy(proxy)
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	server := httptest.NewServer(r)
	defer server.Close()

	resp, err := http.Get(server.URL + "/api/internal?getThreadLinkInfo&thread=" + threadID)
	if err != nil {
		t.Fatalf("get thread link info: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}

	if resp.StatusCode != http.StatusTeapot {
		t.Fatalf("status = %d, body=%s", resp.StatusCode, body)
	}
	if upstreamRequests != 1 {
		t.Fatalf("expected upstream proxy, upstream saw %d request(s)", upstreamRequests)
	}
	if string(body) != "proxied" {
		t.Fatalf("body = %q, want proxied", string(body))
	}
}

func TestRegisterProviderAliases_AllProvidersRegistered(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	// Minimal base handler setup (no need to initialize, just check routing)
	base := &handlers.BaseAPIHandler{}

	// Track if auth middleware was called
	authCalled := false
	authMiddleware := func(c *gin.Context) {
		authCalled = true
		c.Header("X-Auth", "ok")
		// Abort with success to avoid calling the actual handler (which needs full setup)
		c.AbortWithStatus(http.StatusOK)
	}

	m := &AmpModule{authMiddleware_: authMiddleware}
	m.registerProviderAliases(r, base, authMiddleware)

	paths := []struct {
		path   string
		method string
	}{
		{"/api/provider/openai/models", http.MethodGet},
		{"/api/provider/anthropic/models", http.MethodGet},
		{"/api/provider/google/models", http.MethodGet},
		{"/api/provider/groq/models", http.MethodGet},
		{"/api/provider/openai/chat/completions", http.MethodPost},
		{"/api/provider/anthropic/v1/messages", http.MethodPost},
		{"/api/provider/google/v1beta/models", http.MethodGet},
	}

	for _, tc := range paths {
		t.Run(tc.path, func(t *testing.T) {
			authCalled = false
			req := httptest.NewRequest(tc.method, tc.path, nil)
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)

			if w.Code == http.StatusNotFound {
				t.Fatalf("route %s %s not registered", tc.method, tc.path)
			}
			if !authCalled {
				t.Fatalf("auth middleware not executed for %s", tc.path)
			}
			if w.Header().Get("X-Auth") != "ok" {
				t.Fatalf("auth middleware header not set for %s", tc.path)
			}
		})
	}
}

func TestCodexWebsocketsExperimentMiddlewareOnlyMarksResponsesWhenEnabled(t *testing.T) {
	tests := []struct {
		name              string
		enabled           bool
		path              string
		attachMiddleware  bool
		wantWebsocketMark bool
	}{
		{name: "disabled responses", path: "/responses", attachMiddleware: true},
		{name: "enabled responses", enabled: true, path: "/responses", attachMiddleware: true, wantWebsocketMark: true},
		{name: "enabled compact", enabled: true, path: "/responses/compact"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			m := &AmpModule{lastConfig: &config.AmpCode{CodexWebsocketsExperiment: tc.enabled}}
			router := gin.New()
			handler := func(c *gin.Context) {
				if got := cliproxyexecutor.DownstreamWebsocket(c.Request.Context()); got != tc.wantWebsocketMark {
					t.Fatalf("DownstreamWebsocket() = %t, want %t", got, tc.wantWebsocketMark)
				}
				c.Status(http.StatusNoContent)
			}
			if tc.attachMiddleware {
				router.POST(tc.path, m.codexWebsocketsExperimentMiddleware(), handler)
			} else {
				router.POST(tc.path, handler)
			}

			req := httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(`{"model":"gpt-5-codex"}`))
			req.Header.Set("X-Amp-Thread-Id", "T-thread")
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			if w.Code != http.StatusNoContent {
				t.Fatalf("status = %d, want %d", w.Code, http.StatusNoContent)
			}
		})
	}
}

func TestAmpCodexWebsocketSessionIDIsStableBoundedAndIsolated(t *testing.T) {
	newRequest := func(threadID string, model string, feature string) *http.Request {
		req := httptest.NewRequest(http.MethodPost, "/api/provider/openai/v1/responses", strings.NewReader(`{"model":"`+model+`","input":[]}`))
		req.Header.Set("X-Amp-Thread-Id", threadID)
		req.Header.Set("x-amp-feature", feature)
		return req
	}

	base := ampCodexWebsocketSessionID(newRequest("T-thread", "gpt-5-codex", "chat"))
	if base == "" {
		t.Fatal("session ID is empty")
	}
	if !strings.HasPrefix(base, "amp-codex-ws:") || len(base) != len("amp-codex-ws:")+64 {
		t.Fatalf("session ID = %q, want bounded SHA-256 identifier", base)
	}
	if got := ampCodexWebsocketSessionID(newRequest("T-thread", "gpt-5-codex", "chat")); got != base {
		t.Fatalf("stable session ID = %q, want %q", got, base)
	}
	otherClient := newRequest("T-thread", "gpt-5-codex", "chat")
	otherClient = otherClient.WithContext(context.WithValue(otherClient.Context(), clientAPIKeyContextKey{}, "client-key"))
	if got := ampCodexWebsocketSessionID(otherClient); got == base {
		t.Fatalf("different authenticated client reused session ID %q", got)
	}

	isolated := []*http.Request{
		newRequest("T-other", "gpt-5-codex", "chat"),
		newRequest("T-thread", "gpt-5-codex-mini", "chat"),
		newRequest("T-thread", "gpt-5-codex", "side-call"),
	}
	for i := range isolated {
		if got := ampCodexWebsocketSessionID(isolated[i]); got == base {
			t.Fatalf("isolated request %d reused session ID %q", i, got)
		}
	}

	fallback := newRequest("", "gpt-5-codex", "chat")
	fallback.Header.Set("X-Session-ID", "T-thread")
	if got := ampCodexWebsocketSessionID(fallback); got != base {
		t.Fatalf("X-Session-ID fallback = %q, want %q", got, base)
	}
	if got := ampCodexWebsocketSessionID(httptest.NewRequest(http.MethodPost, "/responses", strings.NewReader(`{"model":"gpt-5-codex"}`))); got != "" {
		t.Fatalf("session ID without stable thread metadata = %q, want empty", got)
	}
}

func TestAmpProviderRequestModelBoundedReadPreservesBody(t *testing.T) {
	filler := strings.Repeat("x", ampProviderRequestModelPrefixLimit/2)
	body := `{"input":[{"text":"` + filler + `"}],"model":"gpt-5-codex"}`
	req := httptest.NewRequest(http.MethodPost, "/api/provider/openai/v1/responses", strings.NewReader(body))
	if got := ampProviderRequestModel(req); got != "gpt-5-codex" {
		t.Fatalf("model = %q, want gpt-5-codex", got)
	}
	restored, err := io.ReadAll(req.Body)
	if err != nil {
		t.Fatalf("read restored body: %v", err)
	}
	if string(restored) != body {
		t.Fatalf("restored body = %d bytes, want the original %d bytes", len(restored), len(body))
	}
}

func TestAmpProviderRequestModelSkipsDerivationBeyondPrefix(t *testing.T) {
	filler := strings.Repeat("x", 2*ampProviderRequestModelPrefixLimit)
	body := `{"input":[{"text":"` + filler + `"}],"model":"gpt-5-codex"}`
	req := httptest.NewRequest(http.MethodPost, "/api/provider/openai/v1/responses", strings.NewReader(body))
	if got := ampProviderRequestModel(req); got != "" {
		t.Fatalf("model = %q, want empty when model is beyond the bounded prefix", got)
	}
	restored, err := io.ReadAll(req.Body)
	if err != nil {
		t.Fatalf("read restored body: %v", err)
	}
	if string(restored) != body {
		t.Fatalf("restored body = %d bytes, want the original %d bytes", len(restored), len(body))
	}
}

func TestAmpProviderRequestModelRejectsNonObjectPrefix(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/responses", strings.NewReader(`["model"]`))
	if got := ampProviderRequestModel(req); got != "" {
		t.Fatalf("model = %q, want empty for non-object payloads", got)
	}
	restored, err := io.ReadAll(req.Body)
	if err != nil || string(restored) != `["model"]` {
		t.Fatalf("restored body = %q, %v", restored, err)
	}
}

func TestRegisterProviderAliases_DynamicModelsHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	base := &handlers.BaseAPIHandler{}

	m := &AmpModule{authMiddleware_: func(c *gin.Context) { c.AbortWithStatus(http.StatusOK) }}
	m.registerProviderAliases(r, base, func(c *gin.Context) { c.AbortWithStatus(http.StatusOK) })

	providers := []string{"openai", "anthropic", "google", "groq", "cerebras"}

	for _, provider := range providers {
		t.Run(provider, func(t *testing.T) {
			path := "/api/provider/" + provider + "/models"
			req := httptest.NewRequest(http.MethodGet, path, nil)
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)

			// Should not 404
			if w.Code == http.StatusNotFound {
				t.Fatalf("models route not found for provider: %s", provider)
			}
		})
	}
}

func TestRegisterProviderAliases_V1Routes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	base := &handlers.BaseAPIHandler{}

	m := &AmpModule{authMiddleware_: func(c *gin.Context) { c.AbortWithStatus(http.StatusOK) }}
	m.registerProviderAliases(r, base, func(c *gin.Context) { c.AbortWithStatus(http.StatusOK) })

	v1Paths := []struct {
		path   string
		method string
	}{
		{"/api/provider/openai/v1/models", http.MethodGet},
		{"/api/provider/openai/v1/chat/completions", http.MethodPost},
		{"/api/provider/openai/v1/completions", http.MethodPost},
		{"/api/provider/openai/v1/responses", http.MethodPost},
		{"/api/provider/openai/v1/responses/compact", http.MethodPost},
		{"/api/provider/openai/v1/images/generations", http.MethodPost},
		{"/api/provider/openai/v1/images/edits", http.MethodPost},
		{"/api/provider/anthropic/v1/messages", http.MethodPost},
		{"/api/provider/anthropic/v1/messages/count_tokens", http.MethodPost},
	}

	for _, tc := range v1Paths {
		t.Run(tc.path, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, nil)
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)

			if w.Code == http.StatusNotFound {
				t.Fatalf("v1 route %s %s not registered", tc.method, tc.path)
			}
		})
	}
}

func TestRegisterProviderAliases_V1BetaRoutes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	base := &handlers.BaseAPIHandler{}

	m := &AmpModule{authMiddleware_: func(c *gin.Context) { c.AbortWithStatus(http.StatusOK) }}
	m.registerProviderAliases(r, base, func(c *gin.Context) { c.AbortWithStatus(http.StatusOK) })

	v1betaPaths := []struct {
		path   string
		method string
	}{
		{"/api/provider/google/v1beta/models", http.MethodGet},
		{"/api/provider/google/v1beta/models/generateContent", http.MethodPost},
	}

	for _, tc := range v1betaPaths {
		t.Run(tc.path, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, nil)
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)

			if w.Code == http.StatusNotFound {
				t.Fatalf("v1beta route %s %s not registered", tc.method, tc.path)
			}
		})
	}
}

func TestRegisterProviderAliases_NoAuthMiddleware(t *testing.T) {
	// Test that routes still register even if auth middleware is nil (fallback behavior)
	gin.SetMode(gin.TestMode)
	r := gin.New()

	base := &handlers.BaseAPIHandler{}

	m := &AmpModule{authMiddleware_: nil} // No auth middleware
	m.registerProviderAliases(r, base, func(c *gin.Context) { c.AbortWithStatus(http.StatusOK) })

	req := httptest.NewRequest(http.MethodGet, "/api/provider/openai/models", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	// Should still work (with fallback no-op auth)
	if w.Code == http.StatusNotFound {
		t.Fatal("routes should register even without auth middleware")
	}
}

func TestLocalhostOnlyMiddleware_PreventsSpoofing(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	// Create module with localhost restriction enabled
	m := &AmpModule{
		restrictToLocalhost: true,
	}

	// Apply dynamic localhost-only middleware
	r.Use(m.localhostOnlyMiddleware())
	r.GET("/test", func(c *gin.Context) {
		c.String(http.StatusOK, "ok")
	})

	tests := []struct {
		name           string
		remoteAddr     string
		forwardedFor   string
		expectedStatus int
		description    string
	}{
		{
			name:           "spoofed_header_remote_connection",
			remoteAddr:     "192.168.1.100:12345",
			forwardedFor:   "127.0.0.1",
			expectedStatus: http.StatusForbidden,
			description:    "Spoofed X-Forwarded-For header should be ignored",
		},
		{
			name:           "real_localhost_ipv4",
			remoteAddr:     "127.0.0.1:54321",
			forwardedFor:   "",
			expectedStatus: http.StatusOK,
			description:    "Real localhost IPv4 connection should work",
		},
		{
			name:           "real_localhost_ipv6",
			remoteAddr:     "[::1]:54321",
			forwardedFor:   "",
			expectedStatus: http.StatusOK,
			description:    "Real localhost IPv6 connection should work",
		},
		{
			name:           "remote_ipv4",
			remoteAddr:     "203.0.113.42:8080",
			forwardedFor:   "",
			expectedStatus: http.StatusForbidden,
			description:    "Remote IPv4 connection should be blocked",
		},
		{
			name:           "remote_ipv6",
			remoteAddr:     "[2001:db8::1]:9090",
			forwardedFor:   "",
			expectedStatus: http.StatusForbidden,
			description:    "Remote IPv6 connection should be blocked",
		},
		{
			name:           "spoofed_localhost_ipv6",
			remoteAddr:     "203.0.113.42:8080",
			forwardedFor:   "::1",
			expectedStatus: http.StatusForbidden,
			description:    "Spoofed X-Forwarded-For with IPv6 localhost should be ignored",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/test", nil)
			req.RemoteAddr = tt.remoteAddr
			if tt.forwardedFor != "" {
				req.Header.Set("X-Forwarded-For", tt.forwardedFor)
			}

			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)

			if w.Code != tt.expectedStatus {
				t.Errorf("%s: expected status %d, got %d", tt.description, tt.expectedStatus, w.Code)
			}
		})
	}
}

func TestLocalhostOnlyMiddleware_HotReload(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	// Create module with localhost restriction initially enabled
	m := &AmpModule{
		restrictToLocalhost: true,
	}

	// Apply dynamic localhost-only middleware
	r.Use(m.localhostOnlyMiddleware())
	r.GET("/test", func(c *gin.Context) {
		c.String(http.StatusOK, "ok")
	})

	// Test 1: Remote IP should be blocked when restriction is enabled
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.RemoteAddr = "192.168.1.100:12345"
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusForbidden {
		t.Errorf("Expected 403 when restriction enabled, got %d", w.Code)
	}

	// Test 2: Hot-reload - disable restriction
	m.setRestrictToLocalhost(false)

	req = httptest.NewRequest(http.MethodGet, "/test", nil)
	req.RemoteAddr = "192.168.1.100:12345"
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected 200 after disabling restriction, got %d", w.Code)
	}

	// Test 3: Hot-reload - re-enable restriction
	m.setRestrictToLocalhost(true)

	req = httptest.NewRequest(http.MethodGet, "/test", nil)
	req.RemoteAddr = "192.168.1.100:12345"
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusForbidden {
		t.Errorf("Expected 403 after re-enabling restriction, got %d", w.Code)
	}
}

func TestLocalBrokerHeartbeatRouteSecurityAndStateContract(t *testing.T) {
	useTempNeoThreadStore(t)
	gin.SetMode(gin.TestMode)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/internal" || r.URL.RawQuery != "getUserInfo" || r.Header.Get("Authorization") == "Bearer upstream-unresolved" {
			http.Error(w, "owner unavailable", http.StatusUnauthorized)
			return
		}
		writeNeoJSON(w, http.StatusOK, map[string]any{"ok": true, "result": map[string]any{"id": "user_route_owner"}})
	}))
	t.Cleanup(upstream.Close)
	rt := newNeoRuntime(&config.Config{
		SDKConfig: config.SDKConfig{APIKeys: []string{"client-resolved", "client-unresolved"}},
		AmpCode: config.AmpCode{
			UpstreamURL:    upstream.URL,
			UpstreamAPIKey: "upstream-resolved",
		},
	})
	mapped := NewMappedSecretSource(NewStaticSecretSource("upstream-resolved"))
	mapped.UpdateMappings([]config.AmpUpstreamAPIKeyEntry{{UpstreamAPIKey: "upstream-unresolved", APIKeys: []string{"client-unresolved"}}})
	rt.setSecretSource(mapped)
	m := &AmpModule{restrictToLocalhost: true, neoRuntime: rt}
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "https://example.com")
		c.Header("Access-Control-Allow-Methods", "POST, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Authorization, Content-Type")
		c.Header("Access-Control-Allow-Credentials", "true")
		c.Next()
	})
	auth := func(c *gin.Context) {
		token := strings.TrimSpace(strings.TrimPrefix(c.GetHeader("Authorization"), "Bearer "))
		if token != "client-resolved" && token != "client-unresolved" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing_key"})
			return
		}
		c.Set("userApiKey", token)
		c.Next()
	}
	m.registerManagementRoutes(router, &handlers.BaseAPIHandler{}, auth)

	workingDirectory := t.TempDir()
	payload := func(generation uint64, sessionID string) string {
		raw, err := json.Marshal(map[string]any{
			"brokerId":          "route-broker",
			"sessionId":         sessionID,
			"sessionGeneration": generation,
			"hostname":          "Route Host",
			"pid":               1234,
			"runners": []any{map[string]any{
				"runnerId":         "route-runner",
				"workingDirectory": workingDirectory,
				"repositoryURL":    "",
				"runningThreads":   []any{},
			}},
		})
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	request := func(method, apiKey, contentType, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, "/ampcode/local-broker/heartbeat.json", strings.NewReader(body))
		req.RemoteAddr = "203.0.113.42:4317"
		if apiKey != "" {
			req.Header.Set("Authorization", "Bearer "+apiKey)
		}
		if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		}
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		return rec
	}
	assertNoPhysicalCORSHeaders := func(rec *httptest.ResponseRecorder) {
		t.Helper()
		for name := range rec.Header() {
			if strings.HasPrefix(strings.ToLower(name), "access-control-") {
				t.Fatalf("response physically contains CORS header %q: %#v", name, rec.Header())
			}
		}
	}

	for _, tc := range []struct {
		name        string
		contentType string
	}{
		{name: "missing content type"},
		{name: "wrong content type", contentType: "text/plain"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := request(http.MethodPost, "client-resolved", tc.contentType, payload(1, "session-content-type"))
			if rec.Code != http.StatusUnsupportedMediaType {
				t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
			}
			assertNoPhysicalCORSHeaders(rec)
		})
	}
	oversized := request(http.MethodPost, "client-resolved", "application/json", strings.Repeat("x", neoLocalBrokerMaxBodyBytes+1))
	if oversized.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized status=%d body=%s", oversized.Code, oversized.Body.String())
	}
	assertNoPhysicalCORSHeaders(oversized)
	missingKey := request(http.MethodPost, "", "application/json", payload(1, "session-missing-key"))
	if missingKey.Code != http.StatusUnauthorized {
		t.Fatalf("missing-key status=%d body=%s", missingKey.Code, missingKey.Body.String())
	}
	assertNoPhysicalCORSHeaders(missingKey)
	unresolved := request(http.MethodPost, "client-unresolved", "application/json", payload(1, "session-unresolved"))
	var unresolvedBody map[string]any
	if unresolved.Code != http.StatusUnauthorized || json.Unmarshal(unresolved.Body.Bytes(), &unresolvedBody) != nil || unresolvedBody["error"] != "owner_unavailable" {
		t.Fatalf("unresolved-owner status=%d body=%s", unresolved.Code, unresolved.Body.String())
	}
	assertNoPhysicalCORSHeaders(unresolved)

	accepted := request(http.MethodPost, "client-resolved", "application/json; charset=utf-8", payload(2, "session-2"))
	if accepted.Code != http.StatusOK {
		t.Fatalf("remote heartbeat status=%d body=%s", accepted.Code, accepted.Body.String())
	}
	assertNoPhysicalCORSHeaders(accepted)
	userActor := rt.store.userActorForOwner("user_route_owner")
	if userActor == nil {
		t.Fatal("accepted route did not create owner user actor")
	}
	beforeRejected := neoLocalBrokerStateForTest(userActor)
	stale := request(http.MethodPost, "client-resolved", "application/json", payload(1, "session-1"))
	if stale.Code != http.StatusConflict {
		t.Fatalf("stale heartbeat status=%d body=%s", stale.Code, stale.Body.String())
	}
	if after := neoLocalBrokerStateForTest(userActor); !reflect.DeepEqual(after, beforeRejected) {
		t.Fatalf("stale route heartbeat mutated state:\nbefore=%#v\nafter=%#v", beforeRejected, after)
	}
	invalid := request(http.MethodPost, "client-resolved", "application/json", `{"brokerId":`)
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("invalid heartbeat status=%d body=%s", invalid.Code, invalid.Body.String())
	}
	if after := neoLocalBrokerStateForTest(userActor); !reflect.DeepEqual(after, beforeRejected) {
		t.Fatalf("invalid route heartbeat mutated state:\nbefore=%#v\nafter=%#v", beforeRejected, after)
	}
	assertNoPhysicalCORSHeaders(stale)
	assertNoPhysicalCORSHeaders(invalid)

	options := request(http.MethodOptions, "", "", "")
	if options.Code != http.StatusForbidden {
		t.Fatalf("OPTIONS status=%d body=%s", options.Code, options.Body.String())
	}
	assertNoPhysicalCORSHeaders(options)
	nonPost := request(http.MethodGet, "client-resolved", "", "")
	if nonPost.Code != http.StatusMethodNotAllowed {
		t.Fatalf("non-POST status=%d body=%s", nonPost.Code, nonPost.Body.String())
	}
	if allow := nonPost.Header().Get("Allow"); allow != http.MethodPost {
		t.Fatalf("non-POST Allow=%q, want POST", allow)
	}
	assertNoPhysicalCORSHeaders(nonPost)
}
