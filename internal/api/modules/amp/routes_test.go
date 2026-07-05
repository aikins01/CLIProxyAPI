package amp

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
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
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/api/handlers"
)

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

	for _, path := range []string{"/metadata", "/actors/metadata", "/gateway/thread-actor/", "/_app/remote/3abror/createProjectThread", "/ampcode/local-projects.json"} {
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
	for _, want := range []string{
		"// ==UserScript==",
		"@version 0.1.37",
		"@match https://ampcode.com/*",
		"@updateURL http://127.0.0.1:8317/ampcode/local-inference.user.js",
		"@downloadURL http://127.0.0.1:8317/ampcode/local-inference.user.js",
		"@sandbox raw",
		`"http://127.0.0.1:8317"`,
		ampWebLocalInferenceHeader,
		"globalThis.JSON.parse",
		"decodedConfigPatchCount",
		"menuIntegrationCount",
		"commandPaletteIntegrationCount",
		"localThreadPickerOpenCount",
		"removedLocalThreadControlCount",
		"lastPatchedThreadActorBaseURL",
		"lastPatchedThreadID",
		"threadActorConfig",
		"local-client",
		"cliproxyapi.ampLocalInference.apiKey",
		"storedLocalAPIKey",
		`return globalThis.sessionStorage.getItem(apiKeyStorageKey) || ""`,
		`const promptedAPIKey = (globalThis.prompt("CLIProxyAPI API key") || "").trim()`,
		"globalThis.sessionStorage.setItem(apiKeyStorageKey, promptedAPIKey)",
		"cliproxyapi.ampLocalInference.workingDirectory",
		"cliproxyapi.ampLocalInference.selectedLocalProject",
		"cliproxyapi.ampLocalInference.localThreadIDs",
		"cliproxyapi.ampLocalInference.threadWorkingDirectories",
		"cliproxyapi.ampLocalInference.threadSettings",
		"/ampcode/local-projects.json",
		"localProjectsEndpointPath",
		"selectedLocalProjectWorkingDirectory",
		"rememberSelectedLocalProject",
		"clearSelectedLocalProject",
		"normalizeLocalProject",
		"fetchLocalProjects",
		"installLocalProjectPickerIntegration",
		"localProjectPickerLooksLikeProjectPicker",
		"closeLocalProjectPickerViaNoProject",
		"localProjectFetchCount",
		"localProjectPickerIntegrationCount",
		"lastObservedThreadID",
		"observedThreadID",
		"normalizeExplicitReasoningEffort",
		"lastInheritedWorkingDirectory",
		"remoteShellCreateCount",
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
		"TextDecoder",
		`.split(/[\\/]+/)`,
		"parsedTextLocalInferencePatchOptions",
		`source.includes("\"id\"")`,
		`source.includes("workingDirectory")`,
		`source.includes("workspaceRoot")`,
		"matches.size === 1",
		"internalAPIPath",
		"workingDirectory",
		`prompt("CLIProxyAPI API key")`,
		`"Bearer " + apiKey`,
		"gatewayActorPath",
		"threadActorAPIPath",
		"threadActorCreatePath",
		"threadActorInstancePath",
		"svelteKitRemoteEndpoint",
		"svelteKitRemotePath",
		"createProjectThreadRemotePath",
		"remoteCreateProjectThreadWorkingDirectory",
		"rememberRemoteCreateProjectThread",
		`if (!response || !response.ok || typeof response.clone !== "function")`,
		`if (workingDirectory)`,
		"/_app/remote/",
		"createProjectThread",
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
		"sameLocalWebSocketBase",
		"diagnostics",
		"lastWebSocketBootstrapped",
		"cliproxy-client",
		"amp-web-local-inference",
		"cliproxy-bootstrap-executor",
		"/api/thread-actors",
		"/api/internal",
		"WebSocket",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("userscript missing %q:\n%s", want, body)
		}
	}
	selectedLocalProjectIndex := strings.Index(body, "const fromSelectedLocalProject = selectedLocalProjectWorkingDirectory();")
	visibleProjectIndex := strings.Index(body, "const fromVisibleProject = visibleProjectWorkingDirectory();")
	if selectedLocalProjectIndex < 0 || visibleProjectIndex < 0 || selectedLocalProjectIndex > visibleProjectIndex {
		t.Fatalf("userscript must prefer selected local project before visible project for create-thread working directory")
	}
	for _, unwanted := range []string{
		"installLocalThreadKeyboardShortcut();",
		"installThreadMenuIntegration();",
		"installCommandPaletteIntegration();",
		"handleNewThreadIntent();",
		"mergeSidebarResponse",
		"seedLocalSidebarProjects",
		"seedLocalSidebarProjectForWorkingDirectory",
		"scheduleLocalSidebarProjectsRefresh",
		"refreshLocalSidebarProjects",
		"mergeDevalueSidebarProjects",
		"cachedLocalSidebarRecentThreads",
		"cachedLocalSidebarProjects",
		"sidebarDateFields",
		"appendDevalueSidebarDateValue",
		"appendDevalueSidebarValue",
		"devalueSidebarThreadIDs",
		"devalueSidebarThreadRef",
		"patchDevalueSidebarThread",
		"localProjectIDForWorkingDirectory",
		"cachedProjectWorkingDirectory",
		"patchPlainThreadRuntime",
		"patchDevalueThreadRuntime",
		"thread.hasExecutor",
		"thread.executorConnected",
		`meta.executorType = "local-client"`,
		`source.includes("hasExecutor")`,
		`source.includes("executorConnected")`,
		"ensureDevalueValueIndex(values, true)",
		`if (!workingDirectory || !response || !response.ok || typeof response.clone !== "function")`,
		"/_app/remote/cliproxy/listThreadListSidebar",
		`case "listThreadListSidebar":`,
		`case "listUserExecutorDaemons":`,
		`svelteKitRemoteEndpoint(sourceURL.pathname) === "listThreadListSidebar"`,
		"path.endsWith(\"/listThreadListSidebar\")",
		"path.endsWith(\"/listUserExecutorDaemons\")",
		"\n\t\tcreateLocalThread,\n",
		"\n\t\tpromptLocalThread,\n",
		"\n\t\topenLocalThreadFromMenu,\n",
	} {
		if strings.Contains(body, unwanted) {
			t.Fatalf("userscript still activates obsolete local thread control %q:\n%s", unwanted, body)
		}
	}
	projectIDIndex := strings.Index(body, `const projectID = isPlainObject(decoded) ? firstString(decoded.projectID, decoded.projectId, decoded.project_id) : "";`)
	visibleProjectFallbackIndex := strings.Index(body, `const fromVisibleProject = visibleProjectWorkingDirectory();`)
	if projectIDIndex < 0 || visibleProjectFallbackIndex < 0 || projectIDIndex > visibleProjectFallbackIndex || !strings.Contains(body[projectIDIndex:visibleProjectFallbackIndex], `if (projectID)`) {
		t.Fatalf("userscript should reject projectID bodies before visible project fallback:\n%s", body)
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

	unauthReq := httptest.NewRequest(http.MethodPost, "/api/internal?listThreads", bytes.NewBufferString(`{"method":"listThreads","params":{"limit":20}}`))
	unauthReq.Header.Set("Content-Type", "application/json")
	unauthReq.Header.Set("Origin", "https://ampcode.com")
	unauthReq.Header.Set(ampWebLocalInferenceHeader, "1")
	unauthRec := httptest.NewRecorder()
	r.ServeHTTP(unauthRec, unauthReq)
	if unauthRec.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated internal RPC status = %d, want %d; body=%s", unauthRec.Code, http.StatusUnauthorized, unauthRec.Body.String())
	}

	authReq := httptest.NewRequest(http.MethodPost, "/api/internal?listThreads&"+ampWebLocalInferenceAPIKeyQuery+"=local-key", bytes.NewBufferString(`{"method":"listThreads","params":{"limit":20}}`))
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

func TestWebLocalInferenceInternalRPCServesLocalThreadWithoutProxy(t *testing.T) {
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

	getReq := httptest.NewRequest(http.MethodPost, "/api/internal?getThread", bytes.NewBufferString(`{"method":"getThread","params":{"thread":"`+threadID+`"}}`))
	getReq.Header.Set("Content-Type", "application/json")
	getReq.Header.Set("Origin", "https://ampcode.com")
	getReq.Header.Set(ampWebLocalInferenceHeader, "1")
	getRec := httptest.NewRecorder()
	r.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("getThread status = %d, body=%s", getRec.Code, getRec.Body.String())
	}
	var getResponse map[string]any
	if err := json.Unmarshal(getRec.Body.Bytes(), &getResponse); err != nil {
		t.Fatalf("getThread response JSON error: %v", err)
	}
	thread := mapValue(mapValue(getResponse["result"])["thread"])
	if stringValue(thread["id"]) != threadID || thread["executorConnected"] != false || thread["hasExecutor"] != false {
		t.Fatalf("unexpected local thread response: %#v", getResponse)
	}
	if thread["agentState"] != "idle" || thread["state"] != "idle" {
		t.Fatalf("unexpected local thread state: %#v", thread)
	}
	if stringValue(mapValue(thread["meta"])["ampcodeConnectorMode"]) != "local-neo" {
		t.Fatalf("thread meta missing local marker: %#v", thread["meta"])
	}
	queued := arrayValue(thread["queuedMessages"])
	if len(queued) != 1 || textFromBlocks(arrayValue(mapValue(mapValue(queued[0])["queuedMessage"])["content"])) != "hello local web" {
		t.Fatalf("queuedMessages = %#v", queued)
	}

	listReq := httptest.NewRequest(http.MethodPost, "/api/internal?listThreads", bytes.NewBufferString(`{"method":"listThreads","params":{"limit":20}}`))
	listReq.Header.Set("Content-Type", "application/json")
	listReq.Header.Set("Origin", "https://ampcode.com")
	listReq.Header.Set(ampWebLocalInferenceHeader, "1")
	listRec := httptest.NewRecorder()
	r.ServeHTTP(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("listThreads status = %d, body=%s", listRec.Code, listRec.Body.String())
	}
	var listResponse map[string]any
	if err := json.Unmarshal(listRec.Body.Bytes(), &listResponse); err != nil {
		t.Fatalf("listThreads response JSON error: %v", err)
	}
	statuses := arrayValue(mapValue(listResponse["result"])["threads"])
	if len(statuses) != 1 || stringValue(mapValue(statuses[0])["threadId"]) != threadID || mapValue(statuses[0])["executorConnected"] != false || mapValue(statuses[0])["hasExecutor"] != false {
		t.Fatalf("unexpected local thread statuses: %#v", listResponse)
	}
	if mapValue(statuses[0])["agentState"] != "idle" || mapValue(statuses[0])["state"] != "idle" {
		t.Fatalf("unexpected local thread status state: %#v", listResponse)
	}

	plainReq := httptest.NewRequest(http.MethodPost, "/api/internal?getThread", bytes.NewBufferString(`{"method":"getThread","params":{"thread":"`+threadID+`"}}`))
	plainReq.Header.Set("Content-Type", "application/json")
	plainRec := httptest.NewRecorder()
	r.ServeHTTP(plainRec, plainReq)
	if plainRec.Code != http.StatusServiceUnavailable {
		t.Fatalf("unmarked getThread status = %d, want %d; body=%s", plainRec.Code, http.StatusServiceUnavailable, plainRec.Body.String())
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

func TestNeoWebLocalRemoteEndpointOnlyAllowsCommandRoutes(t *testing.T) {
	for _, path := range []string{"/_app/remote/3abror/createProjectThread", "/_app/remote/3abror/prewarmProjectThread"} {
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

func TestWebLocalInferenceLocalProjectsRoute(t *testing.T) {
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
	if err := writeNeoWebLocalProjectIndex(rt.threadDir, []any{map[string]any{
		"id":               projectID,
		"name":             "local-app",
		"repositoryURL":    neoFileURLForDirectory(workDir),
		"workingDirectory": workDir,
	}}); err != nil {
		t.Fatalf("write project index: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/ampcode/local-projects.json?"+ampWebLocalInferenceAPIKeyQuery+"=local-key", nil)
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
	if response["ok"] != true || len(projects) != 1 {
		t.Fatalf("local projects response = %#v", response)
	}
	project := mapValue(projects[0])
	if stringValue(project["id"]) != projectID || stringValue(project["workingDirectory"]) != workDir || stringValue(project["name"]) != "local-app" {
		t.Fatalf("project = %#v, want id=%q workingDirectory=%q", project, projectID, workDir)
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
	if len(refreshedProjects) != 2 {
		t.Fatalf("refreshed projects = %#v, want index and history projects", refreshedResponse)
	}
	historyID := neoDeterministicLocalProjectID(filepath.Base(historyDir), neoFileURLForDirectory(historyDir), historyDir)
	foundHistoryProject := false
	for _, rawProject := range refreshedProjects {
		project := mapValue(rawProject)
		if stringValue(project["id"]) == historyID && stringValue(project["workingDirectory"]) == historyDir {
			foundHistoryProject = true
			break
		}
	}
	if !foundHistoryProject {
		t.Fatalf("refreshed projects missing history project id=%q dir=%q: %#v", historyID, historyDir, refreshedProjects)
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
}

func TestWebLocalInferenceRemoteCreateProjectThreadCreatesLocalActor(t *testing.T) {
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

	workDir := t.TempDir()
	expectedWorkDir := neoExistingDirectory(workDir)
	projectID := "75616c3b-f4de-48b7-8b83-c1af6978a034"
	parentThreadID := "T-019f1f62-7996-7687-ac33-8906a0d3d571"
	if _, status := rt.localThreadActorManagementResponse(context.Background(), map[string]any{
		"threadId":         parentThreadID,
		"workingDirectory": workDir,
		"workspaceRoot":    workDir,
		"threadMeta":       map[string]any{"projectID": projectID},
	}, ""); status != http.StatusOK {
		t.Fatalf("seed parent status = %d", status)
	}

	threadID := "T-019f20b2-5e05-7501-8ddd-994e151ee951"
	requestBody := neoSvelteKitRemoteCommandBodyForTest(t, map[string]any{
		"content":         []any{map[string]any{"type": "text", "text": "Following @" + parentThreadID}},
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
	actor.mu.Unlock()
	if got := stringValue(environment["workingDirectory"]); got != expectedWorkDir {
		t.Fatalf("workingDirectory = %q, want %q", got, expectedWorkDir)
	}
	if len(queue) != 1 || textFromBlocks(queue[0].Content) != "Following @"+parentThreadID {
		t.Fatalf("queue = %#v", queue)
	}
	if agentMode != "deep" || reasoningEffort != "medium" {
		t.Fatalf("mode/effort = %q/%q", agentMode, reasoningEffort)
	}
	if stringValue(meta["projectID"]) != projectID || stringValue(meta["ampcodeConnectorMode"]) != "local-neo" {
		t.Fatalf("meta = %#v", meta)
	}

	projectOnlyThreadID := "T-019f20b2-5e05-7501-8ddd-994e151ee952"
	projectOnlyBody := neoSvelteKitRemoteCommandBodyForTest(t, map[string]any{
		"content":         []any{map[string]any{"type": "text", "text": "Use selected project only"}},
		"agentMode":       "smart",
		"spawnExecutor":   true,
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
	if stringValue(projectOnlyResult["workingDirectory"]) != expectedWorkDir || stringValue(projectOnlyResult["workspaceRoot"]) != expectedWorkDir {
		t.Fatalf("project-only working directory response = %#v, want %q", projectOnlyResult, expectedWorkDir)
	}
	projectOnlyActor := rt.store.lookupThreadActor(projectOnlyThreadID)
	if projectOnlyActor == nil {
		t.Fatal("project-only thread actor not found")
	}
	projectOnlyActor.mu.Lock()
	projectOnlyEnvironment := cloneMap(projectOnlyActor.environment)
	projectOnlyActor.mu.Unlock()
	if got := stringValue(projectOnlyEnvironment["workingDirectory"]); got != expectedWorkDir {
		t.Fatalf("project-only workingDirectory = %q, want %q", got, expectedWorkDir)
	}

	missingDirectoryThreadID := "T-019f20b2-5e05-7501-8ddd-994e151ee953"
	missingDirectoryBody := neoSvelteKitRemoteCommandBodyForTest(t, map[string]any{
		"content":       []any{map[string]any{"type": "text", "text": "No directory source"}},
		"spawnExecutor": true,
		"threadID":      missingDirectoryThreadID,
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
	if actor := rt.store.lookupThreadActor(missingDirectoryThreadID); actor != nil {
		t.Fatal("missing-directory thread actor was created")
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

func TestWebLocalInferenceProjectIndexIncludesHistoryProjects(t *testing.T) {
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
	if err := writeNeoWebLocalProjectIndex(rt.threadDir, []any{map[string]any{
		"id":               existingID,
		"name":             "existing",
		"repositoryURL":    neoFileURLForDirectory(existingDir),
		"workingDirectory": existingDir,
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

	expectedWorkDir := neoExistingDirectory(workDir)
	expectedID := neoDeterministicLocalProjectID(filepath.Base(expectedWorkDir), neoFileURLForDirectory(expectedWorkDir), expectedWorkDir)
	if got := rt.neoWebLocalProjectWorkingDirectory(expectedID); got != expectedWorkDir {
		t.Fatalf("project workingDirectory = %q, want %q", got, expectedWorkDir)
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

func TestRegisterManagementRoutesServesNewNeoThreadActorLocallyWithProxy(t *testing.T) {
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

	body := bytes.NewBufferString(`{"agentMode":"deep","usesThreadActors":true}`)
	req := httptest.NewRequest(http.MethodPost, "/api/thread-actors", body)
	req.Header.Set("Authorization", "Bearer local-key")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if proxyCalled {
		t.Fatal("new thread actor request should be served locally")
	}
	var response map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("response JSON error: %v", err)
	}
	requireNeoBinaryV7ThreadID(t, stringValue(response["threadId"]))
	if response["agentMode"] != "deep" || response["wsToken"] != "local-key" {
		t.Fatalf("unexpected local thread actor response: %#v", response)
	}
	if response["usesDtw"] != true || response["usesThreadActors"] != true {
		t.Fatalf("thread actor flags = %#v", response)
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
	proxyCalled := false
	upstreamHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxyCalled = true
		w.WriteHeader(http.StatusTeapot)
	})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHandler(w, r)
	}))
	defer upstream.Close()

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

	body := bytes.NewBufferString(`{"agentMode":"rush","usesThreadActors":true}`)
	req := httptest.NewRequest(http.MethodPost, "/api/thread-actors", body)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if proxyCalled {
		t.Fatal("thread-actors request should be served locally when force-thread-actors is enabled")
	}
	var response map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("response JSON error: %v", err)
	}
	if response["agentMode"] != "rush" || stringValue(response["wsToken"]) == "" {
		t.Fatalf("unexpected local thread actor response: %#v", response)
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
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	createReq := httptest.NewRequest(http.MethodPost, "/api/thread-actors", bytes.NewBufferString(`{"agentMode":"review","usesThreadActors":true}`))
	createRec := httptest.NewRecorder()
	r.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body=%s", createRec.Code, createRec.Body.String())
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

	req := httptest.NewRequest(http.MethodPost, "/api/attachments", bytes.NewBufferString(`{"data":"aGVsbG8=","mediaType":"image/png"}`))
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
	if parsed.Scheme != "http" || parsed.Host != "127.0.0.1:8317" || !strings.HasPrefix(parsed.Path, "/api/attachments/") {
		t.Fatalf("attachment URL = %q", attachmentURL)
	}

	getReq := httptest.NewRequest(http.MethodGet, parsed.RequestURI(), nil)
	getRec := httptest.NewRecorder()
	r.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("get status = %d, body=%s", getRec.Code, getRec.Body.String())
	}
	if got := getRec.Body.String(); got != "hello" {
		t.Fatalf("attachment body = %q", got)
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

	req := httptest.NewRequest(http.MethodPost, "/api/attachments", bytes.NewBufferString(`{"contentBase64":"aGVsbG8=","media_type":"image/png"}`))
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
	if getRec.Code != http.StatusOK || getRec.Body.String() != "hello" {
		t.Fatalf("get response = status %d body %q", getRec.Code, getRec.Body.String())
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

	req := httptest.NewRequest(http.MethodPost, "/api/attachments", bytes.NewBufferString(`{"data":"aGVsbG8=","mediaType":"image/png"}`))
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
	if parsed.Scheme != "https" || parsed.Host != "neo.aikins.xyz" || !strings.HasPrefix(parsed.Path, "/api/attachments/") {
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
	if parsed.Scheme != "http" || parsed.Host != "127.0.0.1:8317" || parsed.Path != "/api/attachments/AbCdEfGhIjKlMnOp" {
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

	encoded := strings.Repeat("A", neoAttachmentMaxImageBytes+1)
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
	if upstreamRequests != len(tests) {
		t.Fatalf("upstreamRequests = %d, want %d", upstreamRequests, len(tests))
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
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests++
		if r.URL.Path != "/api/threads/find" {
			t.Fatalf("unexpected upstream request path=%s", r.URL.Path)
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

	if upstreamRequests != 3 {
		t.Fatalf("upstreamRequests = %d, want 3", upstreamRequests)
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
		{name: "internal get thread tail", method: http.MethodPost, path: "/api/internal?getThreadTail", body: `{"method":"getThreadTail","params":{"thread":"` + threadID + `","limit":10}}`},
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
