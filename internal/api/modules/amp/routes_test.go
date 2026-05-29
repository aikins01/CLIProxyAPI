package amp

import (
	"bytes"
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
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/api/handlers"
)

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
		{"/api/user", http.MethodGet},
		{"/api/user/profile", http.MethodGet},
		{"/api/auth", http.MethodGet},
		{"/api/auth/login", http.MethodGet},
		{"/api/meta", http.MethodGet},
		{"/api/telemetry", http.MethodGet},
		{"/api/1.0/projects/local/project", http.MethodGet},
		{"/api/1.0/repos", http.MethodGet},
		{"/api/threads", http.MethodGet},
		{"/api/thread-actors", http.MethodPost},
		{"/threads/", http.MethodGet},
		{"/threads.rss", http.MethodGet}, // Root-level route (no /api prefix)
		{"/api/otel", http.MethodGet},
		{"/api/tab", http.MethodGet},
		{"/api/tab/some/path", http.MethodGet},
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
	if !neoThreadIDExactPattern.MatchString(threadID) {
		t.Fatalf("threadId = %#v", response["threadId"])
	}
	if stringValue(response["wsToken"]) == "" || stringValue(response["ownerUserId"]) == "" || numberFrom(response["threadVersion"]) == 0 {
		t.Fatalf("missing required thread actor fields: %#v", response)
	}
	if response["usesDtw"] != true || response["usesThreadActors"] != true || response["agentMode"] != "deep" {
		t.Fatalf("unexpected thread actor flags: %#v", response)
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
	runtimeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		runtimeRequests++
		sawAuthToken = r.URL.Query().Get("auth_token") != ""
		sawAuthorization = r.Header.Get("Authorization") != ""
		sawRivetHeader = r.Header.Get("X-Rivet-Token") != ""
		sawRivetToken = r.URL.Query().Get("rvt-token") != ""
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
	authReq.Header.Set("Authorization", "Bearer local-key")
	authReq.Header.Set("X-Rivet-Token", "local-key")
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
	if !sawRivetEncoding {
		t.Fatalf("non-credential websocket subprotocol was stripped")
	}
	if !sawRivetKey {
		t.Fatalf("rvt-key did not reach runtime")
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

	dir := t.TempDir()
	oldStoreDir := neoAmpThreadStoreDir
	neoAmpThreadStoreDir = func() string { return dir }
	t.Cleanup(func() { neoAmpThreadStoreDir = oldStoreDir })

	threadID := "T-019e65c0-0310-77a8-b233-4b84d9c0612b"
	rawThread := []byte(`{"id":"` + threadID + `","title":"deep resume","agentMode":"deep","messages":[{"messageId":"M-user","role":"user","agentMode":"deep","reasoningEffort":"xhigh","content":[{"type":"text","text":"resume this thread"}]}]}`)
	if err := os.WriteFile(filepath.Join(dir, threadID+".json"), rawThread, 0o600); err != nil {
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

	req := httptest.NewRequest(http.MethodPost, "/api/thread-actors/"+threadID, bytes.NewBufferString(`{}`))
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

func TestRegisterManagementRoutesPassesCloudOnlyThreadActorsUpstream(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	enabled := true

	dir := t.TempDir()
	oldStoreDir := neoAmpThreadStoreDir
	neoAmpThreadStoreDir = func() string { return dir }
	t.Cleanup(func() { neoAmpThreadStoreDir = oldStoreDir })

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
	if getThreadRequests != 0 {
		t.Fatalf("cloud-only thread actor decision fetched cloud thread %d times", getThreadRequests)
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
	if patchResponse["ok"] != true || patchResponse["usesThreadActors"] != true || patchResponse["executorType"] != "local-client" {
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
	oldStoreDir := neoAmpThreadStoreDir
	neoAmpThreadStoreDir = func() string { return dir }
	t.Cleanup(func() { neoAmpThreadStoreDir = oldStoreDir })

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
	oldStoreDir := neoAmpThreadStoreDir
	neoAmpThreadStoreDir = func() string { return dir }
	t.Cleanup(func() { neoAmpThreadStoreDir = oldStoreDir })

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
	oldStoreDir := neoAmpThreadStoreDir
	neoAmpThreadStoreDir = func() string { return dir }
	t.Cleanup(func() { neoAmpThreadStoreDir = oldStoreDir })

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
		"listThreads",
		"uploadThread",
		"getThread",
		"getThreadMeta",
		"setThreadMeta",
		"archiveThread",
		"deleteThread",
		"getThreadLabels",
		"setThreadLabels",
		"addThreadLabels",
		"getUserLabels",
		"createTask",
		"getTask",
		"listTasks",
		"updateTask",
		"deleteTask",
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
		{name: "listThreads web", method: "listThreads", body: `{"method":"listThreads","params":{"limit":10}}`},
		{name: "listThreads amp", method: "listThreads", body: `{"method":"listThreads","params":{"limit":10}}`, ampHeaders: true},
		{name: "getThread web", method: "getThread", body: `{"method":"getThread","params":{"thread":"T-019e65c0-0310-77a8-b233-4b84d9c0612b"}}`},
		{name: "getThread amp", method: "getThread", body: `{"method":"getThread","params":{"thread":"T-019e65c0-0310-77a8-b233-4b84d9c0612b"}}`, ampHeaders: true},
		{name: "uploadThread", method: "uploadThread", body: `{"method":"uploadThread","params":{"thread":{"id":"T-019e65c0-0310-77a8-b233-4b84d9c0612b"}}}`},
		{name: "getThreadMeta", method: "getThreadMeta", body: `{"method":"getThreadMeta","params":{"thread":"T-019e65c0-0310-77a8-b233-4b84d9c0612b"}}`},
		{name: "setThreadMeta", method: "setThreadMeta", body: `{"method":"setThreadMeta","params":{"thread":"T-019e65c0-0310-77a8-b233-4b84d9c0612b","meta":{}}}`},
		{name: "archiveThread", method: "archiveThread", body: `{"method":"archiveThread","params":{"thread":"T-019e65c0-0310-77a8-b233-4b84d9c0612b","archived":true}}`},
		{name: "deleteThread", method: "deleteThread", body: `{"method":"deleteThread","params":{"thread":"T-019e65c0-0310-77a8-b233-4b84d9c0612b"}}`},
		{name: "getThreadLabels", method: "getThreadLabels", body: `{"method":"getThreadLabels","params":{"thread":"T-019e65c0-0310-77a8-b233-4b84d9c0612b"}}`},
		{name: "setThreadLabels", method: "setThreadLabels", body: `{"method":"setThreadLabels","params":{"thread":"T-019e65c0-0310-77a8-b233-4b84d9c0612b","labels":[]}}`},
		{name: "addThreadLabels", method: "addThreadLabels", body: `{"method":"addThreadLabels","params":{"thread":"T-019e65c0-0310-77a8-b233-4b84d9c0612b","labels":[]}}`},
		{name: "getUserLabels", method: "getUserLabels", body: `{"method":"getUserLabels","params":{}}`},
		{name: "createTask", method: "createTask", body: `{"method":"createTask","params":{"title":"Run the build"}}`},
		{name: "getTask", method: "getTask", body: `{"method":"getTask","params":{"taskID":"task-local"}}`},
		{name: "listTasks", method: "listTasks", body: `{"method":"listTasks","params":{}}`},
		{name: "updateTask", method: "updateTask", body: `{"method":"updateTask","params":{"taskID":"task-local"}}`},
		{name: "deleteTask", method: "deleteTask", body: `{"method":"deleteTask","params":{"taskID":"task-local"}}`},
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

func TestRegisterManagementRoutesPassesThreadGETsUpstreamWhenProxyExists(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	enabled := true

	dir := t.TempDir()
	oldStoreDir := neoAmpThreadStoreDir
	neoAmpThreadStoreDir = func() string { return dir }
	t.Cleanup(func() { neoAmpThreadStoreDir = oldStoreDir })

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
		{name: "thread web", path: "/threads/" + threadID, wantTitle: "upstream thread"},
		{name: "find amp", path: "/api/threads/find?q=local+needle&limit=5", ampHeaders: true, wantTitle: "upstream search"},
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

func TestRegisterManagementRoutesPassesThreadReaderToolsUpstreamWhenProxyExists(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	enabled := true

	dir := t.TempDir()
	oldStoreDir := neoAmpThreadStoreDir
	neoAmpThreadStoreDir = func() string { return dir }
	t.Cleanup(func() { neoAmpThreadStoreDir = oldStoreDir })

	threadID := "T-019e65c0-0310-77a8-b233-4b84d9c0612d"
	rawThread := []byte(`{"id":"` + threadID + `","title":"local thread","messages":[{"messageId":"M-local","role":"user","content":[{"type":"text","text":"local content"}]}]}`)
	if err := os.WriteFile(filepath.Join(dir, threadID+".json"), rawThread, 0o600); err != nil {
		t.Fatalf("write local thread: %v", err)
	}

	upstreamRequests := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests++
		if r.URL.Path != "/api/threads/"+threadID+"/messages/message_stats" {
			t.Fatalf("unexpected upstream request path=%s", r.URL.Path)
		}
		writeNeoJSON(w, http.StatusOK, map[string]any{"messageCount": 99, "upstream": true})
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

	req, err := http.NewRequest(http.MethodPost, localServer.URL+"/api/threads/"+threadID+"/messages/message_stats", bytes.NewBufferString(`{}`))
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
	var response map[string]any
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatalf("response JSON error: %v", err)
	}
	if response["upstream"] != true || numberFrom(response["messageCount"]) != 99 {
		t.Fatalf("unexpected upstream reader response: %#v", response)
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
	oldStoreDir := neoAmpThreadStoreDir
	neoAmpThreadStoreDir = func() string { return dir }
	t.Cleanup(func() { neoAmpThreadStoreDir = oldStoreDir })

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
