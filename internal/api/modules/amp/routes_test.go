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
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

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

	for _, path := range []string{"/metadata", "/actors/metadata", "/gateway/thread-actor/", "/_app/remote/3abror/createProjectThread", "/ampcode/local-projects.json", "/ampcode/local-thread-data.json"} {
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
	for _, want := range []string{
		"// ==UserScript==",
		"@version 0.1.87",
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
		`const userscriptVersion = "0.1.87"`,
		"discoverLocalThreadID",
		"normalizeLocalThreadViewPath",
		`originalFetch(localBaseURLString() + "/api/thread-actors"`,
		`globalThis.document.body.appendChild(anchor)`,
		"shouldPatchSidebarResponseJSON",
		"mergeSidebarResponse",
		"appendDevalueSidebarValue",
		`url.searchParams.set("cliproxy-thread-id", threadID)`,
		"localSidebarThreadMergeCount",
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
		"globalThis.localStorage.getItem(apiKeyStorageKey)",
		`const promptedAPIKey = (globalThis.prompt("CLIProxyAPI API key") || "").trim()`,
		"globalThis.sessionStorage.setItem(apiKeyStorageKey, promptedAPIKey)",
		"cliproxyapi.ampLocalInference.workingDirectory",
		"cliproxyapi.ampLocalInference.selectedLocalProject",
		"cliproxyapi.ampLocalInference.localThreadIDs",
		"cliproxyapi.ampLocalInference.threadWorkingDirectories",
		"cliproxyapi.ampLocalInference.threadSettings",
		"/ampcode/local-projects.json",
		"localProjectsEndpointPath",
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
		"function fetchLocalProjects(promptForKey = false)",
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
		`localProjectsCache = { at: 0, projects: [], threadID, thread: null, threads: [], promise: null }`,
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
	visibleProjectIndex := strings.Index(body, "const fromVisibleProject = visibleProjectWorkingDirectory(visibleCreateThreadProjectName());")
	selectedLocalProjectIndex := strings.Index(body, "const fromSelectedLocalProject = selectedLocalProjectWorkingDirectory();")
	if selectedLocalProjectIndex < 0 || visibleProjectIndex < 0 || visibleProjectIndex > selectedLocalProjectIndex {
		t.Fatalf("userscript must prefer visible project before stale selected local project for create-thread working directory")
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
	if !strings.Contains(newLocalWorkingDirectoryBody, `selectedProject?.name === "~" && selectedDirectory === defaultLocalWorkingDirectory()`) {
		t.Fatal("userscript must preserve an explicit No Project home selection for local thread creation")
	}
	for _, unwanted := range []string{
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
	} {
		if strings.Contains(body, unwanted) {
			t.Fatalf("userscript still activates obsolete local thread control %q:\n%s", unwanted, body)
		}
	}
	projectIDIndex := strings.Index(body, `const projectID = isPlainObject(decoded) ? firstString(decoded.projectID, decoded.projectId, decoded.project_id) : "";`)
	projectIDLookupIndex := strings.Index(body, `const fromProjectID = normalizeWorkingDirectory(localProjectByID(localProjectsCache.projects, projectID)?.workingDirectory);`)
	projectIDVisibleLookupIndex := strings.Index(body, `const fromVisibleLocalProject = visibleLocalProjectWorkingDirectory(visibleCreateThreadProjectName());`)
	visibleProjectFallbackIndex := strings.Index(body, `const fromVisibleProject = visibleProjectWorkingDirectory(visibleCreateThreadProjectName());`)
	if projectIDIndex < 0 || projectIDLookupIndex < 0 || projectIDVisibleLookupIndex < 0 || visibleProjectFallbackIndex < 0 || projectIDIndex > projectIDLookupIndex || projectIDLookupIndex > projectIDVisibleLookupIndex || projectIDVisibleLookupIndex > visibleProjectFallbackIndex || !strings.Contains(body[projectIDLookupIndex:visibleProjectFallbackIndex], `return fromProjectID;`) || !strings.Contains(body[projectIDVisibleLookupIndex:visibleProjectFallbackIndex], `return "";`) {
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

func TestWebLocalInferenceUserscriptPatchesLocalThreadActorConfig(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("node not installed")
	}
	dir := t.TempDir()
	scriptPath := filepath.Join(dir, "local-inference.user.js")
	if err := os.WriteFile(scriptPath, []byte(ampWebLocalInferenceUserscript("http://127.0.0.1:8317", nil)), 0o600); err != nil {
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
const createdThreadID = "T-019f20b2-5e05-7501-8ddd-994e151ee951";
const failedThreadID = "T-019f20b2-5e05-7501-8ddd-994e151ee955";
const ampViewerUserID = "user_amp_viewer";
const createdThreadWorkDir = "/Users/aikins01/Developer/CLIProxyAPI";
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
		getBoundingClientRect() { return { width: 120, height: 24 }; }
		replaceChildren(...children) { this.children = []; this.textContent = ""; this.append(...children); }
		querySelector() { return null; }
		querySelectorAll() { return []; }
		closest() { return null; }
		matches() { return false; }
	}
class NativeWebSocket {
	static CONNECTING = 0;
	static OPEN = 1;
	static CLOSING = 2;
	static CLOSED = 3;
	static instances = [];
	constructor(url, protocols) {
		this.url = String(url);
		this.protocols = protocols;
		this.readyState = NativeWebSocket.CONNECTING;
		this.listeners = {};
		this.sent = [];
		NativeWebSocket.instances.push(this);
	}
	addEventListener(name, callback) { this.listeners[name] = callback; }
	send(payload) { this.sent.push(payload); }
	close() { this.readyState = NativeWebSocket.CLOSED; }
}
globalThis.location = new URL("https://ampcode.com/threads/" + threadID);
	let documentQueryElements = [];
	globalThis.document = {
		readyState: "loading",
		body: new FakeElement(),
		documentElement: new FakeElement(),
		addEventListener() {},
		querySelector() { return null; },
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
const encodeDevalue = (value) => Buffer.from(JSON.stringify(value)).toString("base64url");
const requestPayloadText = (body) => {
	try {
		return Buffer.from(JSON.parse(body).payload || "", "base64url").toString("utf8");
	} catch {
		return "";
	}
};
let createFetchURL = "";
let localProjectsFetchURL = "";
let localThreadDataFetchURL = "";
let localThreadDataFetchCount = 0;
let localThreadSummaryFetchCount = 0;
let lastFetchURL = "";
let metadataFetchURL = "";
let metadataFetchAuthorization = "";
let metadataFetchBridgeHeader = "";
let metadataFetchRivetEncoding = "";
let metadataFetchUnsupportedHeader = "";
globalThis.fetch = async (url, init) => {
	const fetchURL = String(url);
	lastFetchURL = fetchURL;
	const parsedURL = new URL(fetchURL, globalThis.location.href);
	if (parsedURL.pathname === "/ampcode/local-projects.json") {
		localProjectsFetchURL = fetchURL;
		return new Response(JSON.stringify({
			ok: true,
			projects: [],
			thread: {
				id: createdThreadID,
				threadId: createdThreadID,
				v: 1,
				title: "Use local project",
				state: "idle",
				agentState: "idle",
				meta: { executorType: "local-client", usesThreadActors: true },
			},
			threads: [
				{
					id: createdThreadID,
					threadId: createdThreadID,
					v: 1,
					title: "Use local project",
					state: "idle",
					agentState: "idle",
					meta: { executorType: "local-client", usesThreadActors: true },
				},
				{
					id: secondThreadID,
					threadId: secondThreadID,
					v: 1,
					title: "Second local thread",
					state: "idle",
					agentState: "idle",
					meta: { executorType: "local-client", usesThreadActors: true },
				},
			],
		}), {
			status: 200,
			headers: { "Content-Type": "application/json" },
		});
	}
	if (parsedURL.pathname === "/ampcode/local-thread-data.json") {
		localThreadDataFetchURL = fetchURL;
		localThreadDataFetchCount += 1;
		const localThreadID = parsedURL.searchParams.get("cliproxy-thread-id") || "";
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
				v: 9,
				messages: [{ messageId: "M-local-history", protocolMessageID: "M-local-history", role: "user", content: [{ type: "text", text: "local history" }] }],
				queuedMessages: [],
			},
			threadActorConfig: { threadId: localThreadID, wsToken: "local-neo", ampURL: "http://127.0.0.1:8317", baseURL: "http://127.0.0.1:8317" },
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
	if (parsedURL.pathname.endsWith("/createProjectThread")) {
		createFetchURL = fetchURL;
	}
	const body = String(init?.body || "");
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
globalThis.prompt = () => "";
globalThis.history = { state: null, replaceState() {} };
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
	assert(bridge && bridge.userscriptVersion === "0.1.87", "bridge userscript version was not exposed");
		globalThis.localStorage.setItem(bridge.apiKeyStorageKey, "local-key");
		JSON.parse(JSON.stringify({ user: { id: "U-unrelated", email: "other@example.com" }, workspaces: [] }));
		assert(bridge.diagnostics.authenticatedAmpUserIDCaptureCount === 0, "unrelated nested user was accepted as the authenticated viewer");
		new WebSocket("wss://ampcode.com/gateway/userActor/?rvt-method=get&rvt-key=" + encodeURIComponent(ampViewerUserID));
	assert(bridge.diagnostics.authenticatedAmpUserIDCaptureCount === 1, "authenticated Amp user id was not captured from the user actor socket");
	const authenticatedPageDataResponse = new Response(JSON.stringify({
	type: "data",
	nodes: [{
		type: "data",
		data: [
			{ user: 1, userWorkspace: 4, userFeatures: 5 },
			{ id: 2, email: 3 },
			ampViewerUserID,
			"viewer@example.com",
			null,
			[],
		],
	}],
}));
	Object.defineProperty(authenticatedPageDataResponse, "url", { value: "https://ampcode.com/__data" });
	await authenticatedPageDataResponse.json();
	assert(bridge.diagnostics.authenticatedAmpUserIDCaptureCount === 1, "authenticated Amp user id was not captured from SvelteKit page data");
const unrelatedGraphPasses = bridge.diagnostics.decodedGraphPassCount;
const unrelatedIDPayload = JSON.parse('{"id":"generic-object","payload":[{"value":1}]}');
assert(unrelatedIDPayload.id === "generic-object", "unrelated id payload changed");
assert(bridge.diagnostics.decodedGraphPassCount === unrelatedGraphPasses, "unrelated id payload triggered decoded graph traversal");
	const directOpenedThreadSocket = new WebSocket("wss://ampcode.com/gateway/threadActor/?rvt-method=get&rvt-key=" + encodeURIComponent(threadID));
	JSON.parse(JSON.stringify({ id: threadID, v: 7, messages: [] }));
	const directOpenedThreadResume = '{"type":"client_resume","version":0}';
	directOpenedThreadSocket.send(directOpenedThreadResume);
	assert(JSON.parse(directOpenedThreadSocket.sent.at(-1)).version === 7, "active direct-open thread base did not rewrite zero resume before local id discovery");
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
		const destinationThreadResourceResponse = await fetch("https://ampcode.com/threads/" + secondThreadID + "/__data");
		await destinationThreadResourceResponse.json();
		assert(bridge.diagnostics.lastLoadedThreadBaseThreadID === secondThreadID && bridge.diagnostics.lastLoadedThreadBaseVersion === 9, "destination thread base was not captured before client navigation committed");
		const localThreadResourceResponse = await fetch("https://ampcode.com/threads/" + threadID + "/__data");
	const localThreadResource = await localThreadResourceResponse.json();
	assert(new URL(localThreadDataFetchURL).pathname === "/ampcode/local-thread-data.json", "local thread resource was not bridged");
	assert(new URL(localThreadDataFetchURL).searchParams.get("cliproxy-thread-id") === threadID, "local thread resource id was not forwarded");
	assert(localThreadResource.thread.id === threadID && localThreadResource.thread.v === 9, "local thread resource identity/version mismatch");
	assert(localThreadResource.thread.creatorUserID === ampViewerUserID && localThreadResource.thread.ownerUserId === ampViewerUserID, "local thread resource was not projected as owned by the authenticated Amp user");
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
		assert(genericAriaModeURL.searchParams.get("cliproxy-agent-mode") === "high", "generic aria label masked visible mode text");
		assert(genericAriaModeURL.searchParams.get("cliproxy-reasoning-effort") === "xhigh", "high mode reasoning effort was not inherited");
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
const userActorSocket = new WebSocket("wss://ampcode.com/gateway/userActor/?rvt-method=get&rvt-key=user-local");
userActorSocket.send(rawZeroResume);
assert(userActorSocket.sent.at(-1) === rawZeroResume, "user actor resume frame was changed");
	const noBaseSocket = new WebSocket("wss://ampcode.com/gateway/threadActor/?rvt-method=get&rvt-key=" + encodeURIComponent(secondThreadID));
	const noBaseResume = '{"type":"client_resume","version":0}';
	const unknownBaseSocket = new WebSocket("wss://ampcode.com/gateway/threadActor/?rvt-method=get&rvt-key=" + encodeURIComponent(cloudThreadID));
	unknownBaseSocket.send(noBaseResume);
	assert(unknownBaseSocket.sent.at(-1) === noBaseResume, "mismatched or missing loaded base changed zero resume");
const values = JSON.parse(JSON.stringify([{ current: 1 }, { thread: 2 }, { id: 3, title: 4, v: 5, messages: 6 }, threadID, "Local", 37, [] ]));
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
globalThis.localStorage.setItem(bridge.localThreadIDsStorageKey, JSON.stringify([createdThreadID]));
const sidebarData = JSON.stringify([
	{ q: 1 },
	{ "3abror/listThreadListSidebar/": 2 },
	{ v: 3 },
	{ projects: 4, recentThreads: 5 },
	[],
	[],
]);
const sidebarResponse = new Response(JSON.stringify({ type: "result", data: sidebarData }), {
	status: 200,
	headers: { "Content-Type": "application/json" },
});
Object.defineProperty(sidebarResponse, "url", { value: "https://ampcode.com/_app/remote/3abror/listThreadListSidebar" });
	const decodedSidebarResponse = await sidebarResponse.json();
	const mergedSidebarValues = JSON.parse(decodedSidebarResponse.data);
	const mergedSidebarRefs = mergedSidebarValues[5].slice(0, 2);
	const mergedSidebarThreadRefs = mergedSidebarRefs.map((ref) => mergedSidebarValues[ref].thread);
	const mergedSidebarThreadIDs = mergedSidebarThreadRefs.map((ref) => mergedSidebarValues[mergedSidebarValues[ref].id]);
	assert(mergedSidebarRefs.every((ref) => Number.isInteger(ref) && Number.isInteger(mergedSidebarValues[ref].thread)), "local sidebar wrappers were not inserted into sidebar response");
	assert(mergedSidebarRefs.every((ref) => Number.isInteger(mergedSidebarValues[ref].lastActivityTimestamp)), "local sidebar wrappers are missing activity timestamps");
	assert(JSON.stringify(mergedSidebarThreadIDs) === JSON.stringify([createdThreadID, secondThreadID]), "merged sidebar thread order mismatch");
	assert(mergedSidebarValues[mergedSidebarValues[mergedSidebarThreadRefs[0]].title] === "Use local project", "merged sidebar thread title mismatch");
	assert(!Object.hasOwn(mergedSidebarValues[mergedSidebarThreadRefs[1]], "hasExecutor"), "cached local sidebar thread gained hasExecutor before discovery");
	assert(!Object.hasOwn(mergedSidebarValues[mergedSidebarThreadRefs[1]], "executorConnected"), "cached local sidebar thread gained executorConnected before discovery");
assert(new URL(localProjectsFetchURL).searchParams.get("cliproxy-thread-id") === createdThreadID, "local summary request used the wrong thread id");
assert(bridge.diagnostics.localSidebarThreadMergeCount === 2, "sidebar thread merges were not recorded");
	const canonicalSidebarData = JSON.stringify([
		{ recentThreads: 1 },
		[2],
		{ thread: 3, lastActivityTimestamp: 6, projectName: 7 },
		{ id: 4, title: 5, hasExecutor: 8, executorConnected: 8 },
		createdThreadID,
		"Remote canonical title",
		1,
		null,
		true,
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
	assert(canonicalSidebarValues[canonicalSidebarValues[canonicalThreadRef].title] === "Use local project", "existing remote sidebar shell was not enriched with the local title");
	assert(canonicalSidebarValues[canonicalSidebarValues[canonicalThreadRef].hasExecutor] === true, "existing local sidebar thread lost hasExecutor");
	assert(canonicalSidebarValues[canonicalSidebarValues[canonicalThreadRef].executorConnected] === true, "existing local sidebar thread lost executorConnected");
	assert(canonicalSidebarValues[canonicalSidebarValues[canonicalThreadRef].state] === "idle", "existing remote sidebar shell was not enriched with local state");
assert(canonicalSidebarThreadIDs.includes(secondThreadID), "missing local sidebar thread was not added");
assert(bridge.diagnostics.localSidebarThreadMergeCount === 4, "sidebar additions and enrichments were not recorded");
globalThis.localStorage.setItem(bridge.localThreadIDsStorageKey, JSON.stringify([threadID, secondThreadID, createdThreadID]));
globalThis.location = new URL("https://ampcode.com/threads/" + createdThreadID);
const staleSidebarThreadID = "T-019f3586-fb79-7309-a219-4a279ef2900c";
const sideEffectThreadID = "T-019f3586-fb79-7309-a219-4a279ef2900d";
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
assert(bridge.diagnostics.webSocketBootstrapCount === 9, "websocket bootstrap was not recorded");
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
	if err := os.WriteFile(scriptPath, []byte(ampWebLocalInferenceUserscript("http://127.0.0.1:8317", nil)), 0o600); err != nil {
		t.Fatalf("write userscript: %v", err)
	}
	runner := `
(async () => {
const assert = (condition, message) => {
	if (!condition) throw new Error(message);
};
const scriptPath = ` + strconv.Quote(scriptPath) + `;
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
globalThis.document = {
	readyState: "complete",
	body,
	documentElement,
	createElement(tag) { return new FakeElement(tag); },
	querySelector(selector) { return documentElement.querySelector(selector); },
	querySelectorAll(selector) { return documentElement.querySelectorAll(selector); },
	getElementById(id) { return documentElement.querySelectorAll("*").find((element) => element.getAttribute("id") === String(id)) || null; },
	contains(target) { return documentElement.contains(target); },
	addEventListener() {},
	removeEventListener() {},
	dispatchEvent(event) { return documentElement.dispatchEvent(event); },
	createTreeWalker() { return { currentNode: null, nextNode() { return null; } }; },
};
globalThis.location = new URL("https://ampcode.com/threads/T-019f324b-2802-7868-b1b1-5f0fa3e87ea5");
globalThis.history = { state: null, replaceState() {} };
globalThis.localStorage = new TestStorage();
globalThis.sessionStorage = new TestStorage();
globalThis.localStorage.setItem("cliproxyapi.ampLocalInference.apiKey", "local-key");
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
		{ name: "on-chain", workingDirectory: "/Users/aikins01/Developer/on-chain" },
		{ name: "telemetry.dev", workingDirectory: tempTelemetryDirectory },
		{ name: "telemetry.dev", workingDirectory: "/private/var/folders/63/_bz0gwdn0px0d4r7s9zhct8m0000gn/T/telemetry-pr78-review-XXXXXX.RGqjFliah5/telemetry.dev" },
		{ name: "telemetry.dev", workingDirectory: "/Users/aikins01/Developer/telemetry.dev" },
	],
};
	let deferLocalProjectsFetch = false;
	const deferredLocalProjectsFetches = [];
		let lastFetchURL = "";
		globalThis.fetch = async (url) => {
		lastFetchURL = String(url);
		if (lastFetchURL.includes("/_app/remote/3abror/createProjectThread")) {
			return new Response(JSON.stringify({ data: "" }), { status: 200, headers: { "Content-Type": "application/json" } });
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
const actions = new FakeElement("div");
actions.textContent = "Actions";
list.appendChild(noProject);
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
	await new Promise((resolve) => setTimeout(resolve, 25));
	assert(nearMissList.querySelectorAll("[data-cliproxy-local-project-item]").length === 0, "near-miss project text container was integrated as a picker");
	const observer = mutationObservers.at(-1);
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
	assert(createURL.searchParams.get("cliproxy-working-directory") === "/Users/aikins01/Developer/telemetry.dev", "create-thread did not use visible project from local cache: " + lastFetchURL);
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
assert(homeSelection.name === "~" && homeSelection.workingDirectory === "/Users/aikins01", "No Project did not select home: " + JSON.stringify(homeSelection));
assert(noProject.getAttribute("aria-selected") === "true" && noProject.dataset.selected === "true", "No Project was not the selected picker item");
assert(localItems.every((item) => item.dataset.cliproxyLocalProjectCurrent === "0" && !(item.querySelector('[data-slot="project-check"]')?.textContent || "")), "No Project left a local project check selected");
assert(popupProjectButton.textContent.includes("~"), "No Project did not refresh project button: " + popupProjectButton.textContent);
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
input.value = "";
input.dispatchEvent(new FakeEvent("input", { bubbles: true }));
await new Promise((resolve) => setTimeout(resolve, 25));
assert(localItems.every((item) => !item.hidden && item.style.display !== "none"), "clearing local project search did not restore all items");
popupProjectButton.textContent = "Project: on-chain";
delete popupProjectButton.dataset.cliproxyLocalProjectActivator;
delete popupProjectButton.dataset.cliproxyLocalProjectLabel;
delete popupProjectButton.dataset.cliproxyLocalProjectWorkingDirectory;
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
lastFetchURL = "";
await globalThis.fetch("https://ampcode.com/_app/remote/3abror/createProjectThread", {
	method: "POST",
	headers: { "Content-Type": "application/json" },
	body: JSON.stringify({ payload: encodeDevalue([{ projectID: 1 }, "75616c3b-f4de-48b7-8b83-c1af6978a034"]) }),
});
const nativeDialogProjectIDURL = new URL(lastFetchURL);
assert(nativeDialogProjectIDURL.searchParams.get("cliproxy-working-directory") === "/Users/aikins01/Developer/on-chain", "projectID create did not use known dialog-selected project: " + lastFetchURL);
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
		assert(observer.disconnected === true, "replaced project observer was not disconnected");
		assert(bridge.diagnostics.projectMutationPendingRootCount === 0, "observer cleanup retained pending project roots");
		flushAnimationFrames();
		assert(bridge.diagnostics.projectMutationFlushCount === cleanupFlushBefore, "stale project observer flushed after cleanup");
		assert(deferredLocalProjectsFetches.length === 1, "replacement integration did not start a deferred project fetch");
		const deferredObserver = mutationObservers.at(-1);
		localProjectsPayload = collisionProjectsPayload;
		delete require.cache[require.resolve(scriptPath)];
		require(scriptPath);
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
		localProjectsPayload = null;
		delete require.cache[require.resolve(scriptPath)];
		require(scriptPath);
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
	for _, path := range []string{"/_app/remote/3abror/createProjectThread", "/_app/remote/3abror/listUserExecutorRunners", "/_app/remote/3abror/prewarmProjectThread"} {
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
	threadID := "T-019f6551-9d97-73b2-ab56-d3967bce6e09"
	actor := rt.store.ensureThreadActor(threadID)
	actor.mu.Lock()
	actor.title = "New local web thread"
	actor.lastUsed = time.Now()
	actor.environment = map[string]any{"workingDirectory": workDir, "workspaceRoot": workDir}
	actor.meta = map[string]any{"projectID": projectID, "executorType": "local-client", "usesThreadActors": true}
	actor.settings = map[string]any{"agentMode": "medium", "reasoning.effort": "medium"}
	actor.currentAgentMode = "medium"
	actor.currentReasoningEffort = "medium"
	actor.messages = []neoMessage{
		{ThreadID: threadID, MessageID: "M-local-user", Role: "user", Content: []any{map[string]any{"type": "text", "text": "inspect local history"}}, Seq: 1},
		{ThreadID: threadID, MessageID: "M-local-assistant", Role: "assistant", Content: []any{map[string]any{"type": "text", "text": "history ready"}}, State: map[string]any{"type": "complete", "stopReason": "end_turn"}, Seq: 2},
	}
	actor.seq = 3
	actor.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/ampcode/local-projects.json?"+ampWebLocalInferenceAPIKeyQuery+"=local-key&cliproxy-thread-id="+url.QueryEscape(threadID), nil)
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
	homeDirectory := neoDefaultWebLocalWorkingDirectory()
	if stringValue(response["defaultWorkingDirectory"]) != homeDirectory {
		t.Fatalf("defaultWorkingDirectory = %#v, want %q", response["defaultWorkingDirectory"], homeDirectory)
	}
	project := mapValue(projects[0])
	if stringValue(project["id"]) != projectID || stringValue(project["workingDirectory"]) != workDir || stringValue(project["name"]) != "local-app" {
		t.Fatalf("project = %#v, want id=%q workingDirectory=%q", project, projectID, workDir)
	}
	thread := mapValue(response["thread"])
	if stringValue(thread["id"]) != threadID || stringValue(thread["title"]) != "New local web thread" || numberFrom(thread["v"]) < 1 {
		t.Fatalf("thread summary = %#v", thread)
	}
	if _, exists := thread["messages"]; exists {
		t.Fatalf("thread summary included transcript messages: %#v", thread)
	}
	recentThreads := arrayValue(response["threads"])
	if len(recentThreads) != 1 || stringValue(mapValue(recentThreads[0])["id"]) != threadID {
		t.Fatalf("recent thread summaries = %#v, want %q", recentThreads, threadID)
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
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
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
	if agentMode != "deep" || reasoningEffort != "medium" {
		t.Fatalf("mode/effort = %q/%q", agentMode, reasoningEffort)
	}
	if stringValue(meta["projectID"]) != projectID || stringValue(meta["ampcodeConnectorMode"]) != "local-neo" {
		t.Fatalf("meta = %#v", meta)
	}
	if bootstrapExecutorType != "local-client" || spawnedCount != 1 {
		t.Fatalf("executor bootstrap = type:%q spawned:%d", bootstrapExecutorType, spawnedCount)
	}

	localProjectThreadID := "T-019f20b2-5e05-7501-8ddd-994e151ee959"
	localProjectBody := neoSvelteKitRemoteCommandBodyForTest(t, map[string]any{
		"content":       []any{map[string]any{"type": "text", "text": "Use injected local project"}},
		"agentMode":     "medium",
		"spawnExecutor": true,
		"threadID":      localProjectThreadID,
		"projectID":     projectID,
	})
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
	if stringValue(localProjectMeta["projectID"]) != "" {
		t.Fatalf("local-project create kept stale project ID: %#v", localProjectMeta)
	}
	workspace := neoRecentThreadWorkspace(localProjectEnvironment)
	if stringValue(workspace["uri"]) != (&url.URL{Scheme: "file", Path: expectedWorkDir}).String() {
		t.Fatalf("local-project workspace = %#v, want %q", workspace, expectedWorkDir)
	}

	projectOnlyThreadID := "T-019f20b2-5e05-7501-8ddd-994e151ee952"
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

	userActor, _ := rt.store.upsert(map[string]any{"name": "userActor", "key": "user-local"}, true)
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

	runnerThreadID := "T-019f20b2-5e05-7501-8ddd-994e151ee955"
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

	runnerOptOutThreadID := "T-019f20b2-5e05-7501-8ddd-994e151ee956"
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

	explicitFalseThreadID := "T-019f20b2-5e05-7501-8ddd-994e151ee954"
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
	if explicitFalseResult["usesThreadActors"] != false || explicitFalseResult["usesDtw"] != false || stringValue(explicitFalseResult["executorType"]) != "" {
		t.Fatalf("explicit-false local actor response = %#v", explicitFalseResult)
	}
	if _, ok := explicitFalseResult["threadActorConfig"]; ok {
		t.Fatalf("explicit-false returned threadActorConfig: %#v", explicitFalseResult)
	}
	if threadData := mapValue(explicitFalseResult["threadData"]); threadData["threadActorConfig"] != nil {
		t.Fatalf("explicit-false returned threadData actor config: %#v", threadData)
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

func TestWebLocalInferenceListUserExecutorRunnersCreatesQueryResult(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	enabled := true
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
		NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled},
	}})
	userActor, _ := rt.store.upsert(map[string]any{"name": "userActor", "key": "user-local"}, true)
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
