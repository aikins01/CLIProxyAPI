package amp

import (
	"bytes"
	"encoding/json"
	"errors"
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

func TestRegisterManagementRoutesServesNeoBootstrapInternalsLocally(t *testing.T) {
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
	proxy, _ := createReverseProxy(upstream.URL, NewStaticSecretSource(""))
	m.setProxy(proxy)
	m.registerManagementRoutes(r, &handlers.BaseAPIHandler{}, nil)

	for _, tc := range []struct {
		name  string
		path  string
		check func(t *testing.T, response map[string]any)
	}{
		{name: "loadPlugins", path: "/api/internal?loadPlugins"},
		{
			name: "getUserInfo",
			path: "/api/internal?getUserInfo",
			check: func(t *testing.T, response map[string]any) {
				t.Helper()
				result := mapValue(response["result"])
				if stringValue(result["id"]) != neoLocalOwnerUserID || stringValue(result["githubLogin"]) == "" || result["mysteriousMessage"] != nil {
					t.Fatalf("user info result = %#v", result)
				}
				features := arrayValue(result["features"])
				if len(features) == 0 {
					t.Fatalf("user info features missing: %#v", result)
				}
				foundRetentionFeature := false
				for _, raw := range features {
					feature := mapValue(raw)
					if stringValue(feature["name"]) == "accept-abuse-data-retention" && boolValue(feature["enabled"]) {
						foundRetentionFeature = true
					}
				}
				if !foundRetentionFeature {
					t.Fatalf("user info missing GPT-5.5 retention feature: %#v", features)
				}
			},
		},
		{name: "getThreadLinkInfo", path: "/api/internal?getThreadLinkInfo&thread=T-019e1046-656d-7132-879f-390ded941c16"},
		{
			name: "threadDisplayCostInfo",
			path: "/api/internal?threadDisplayCostInfo&threadID=T-019e1046-656d-7132-879f-390ded941c16",
			check: func(t *testing.T, response map[string]any) {
				t.Helper()
				result := mapValue(response["result"])
				if result["totalCostUSD"] != nil || stringValue(result["costBreakdownURL"]) != "http://example.com/threads/T-019e1046-656d-7132-879f-390ded941c16/usage" {
					t.Fatalf("threadDisplayCostInfo result = %#v", result)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proxyCalled = false
			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, req)

			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
			}
			if proxyCalled {
				t.Fatalf("%s should be served locally during Neo bootstrap", tc.path)
			}
			var response map[string]any
			if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
				t.Fatalf("response JSON error: %v", err)
			}
			if response["ok"] != true {
				t.Fatalf("unexpected local internal response: %#v", response)
			}
			if tc.check != nil {
				tc.check(t, response)
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

func TestRegisterManagementRoutesServesNeoThreadUsageLocally(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	dir := t.TempDir()
	oldStoreDir := neoAmpThreadStoreDir
	neoAmpThreadStoreDir = func() string { return dir }
	t.Cleanup(func() { neoAmpThreadStoreDir = oldStoreDir })

	threadID := "T-019e1046-656d-7132-879f-390ded941c16"
	rawThread := []byte(`{"id":"` + threadID + `","title":"usage test","messages":[{"messageId":"M-one","role":"assistant","usage":{"model":"gpt-5.5","inputTokens":3,"outputTokens":5,"cacheCreationInputTokens":7,"cacheReadInputTokens":11,"maxInputTokens":400000}}]}`)
	if err := os.WriteFile(filepath.Join(dir, threadID+".json"), rawThread, 0o600); err != nil {
		t.Fatalf("write local thread: %v", err)
	}

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

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	var response map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("response JSON error: %v", err)
	}
	usage := mapValue(response["usage"])
	if numberFrom(usage["inputTokens"]) != 3 || numberFrom(usage["outputTokens"]) != 5 || numberFrom(usage["totalTokens"]) != 26 {
		t.Fatalf("usage summary = %#v", usage)
	}
	if got := stringValue(response["costBreakdownURL"]); got != "http://127.0.0.1:8317/threads/"+threadID+"/usage" {
		t.Fatalf("costBreakdownURL = %q", got)
	}

	htmlReq := httptest.NewRequest(http.MethodGet, "/threads/"+threadID+"/usage", nil)
	htmlReq.Header.Set("Accept", "text/html")
	htmlRec := httptest.NewRecorder()
	r.ServeHTTP(htmlRec, htmlReq)
	if htmlRec.Code != http.StatusOK || !strings.Contains(htmlRec.Body.String(), "usage test") || !strings.Contains(htmlRec.Header().Get("Content-Type"), "text/html") {
		t.Fatalf("html response = status %d content-type %q body=%s", htmlRec.Code, htmlRec.Header().Get("Content-Type"), htmlRec.Body.String())
	}
}

func TestRegisterManagementRoutesServesNeoLegacyRunEndpointsLocally(t *testing.T) {
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

	threadReq := httptest.NewRequest(http.MethodPost, "/api/threads", bytes.NewBufferString(`{"metadata":{"source":"test"}}`))
	threadReq.Header.Set("Content-Type", "application/json")
	threadRec := httptest.NewRecorder()
	r.ServeHTTP(threadRec, threadReq)
	if threadRec.Code != http.StatusOK {
		t.Fatalf("thread create status = %d body=%s", threadRec.Code, threadRec.Body.String())
	}
	var threadResponse map[string]any
	if err := json.Unmarshal(threadRec.Body.Bytes(), &threadResponse); err != nil {
		t.Fatalf("thread create JSON error: %v", err)
	}
	if stringValue(threadResponse["object"]) != "thread" || !strings.HasPrefix(stringValue(threadResponse["id"]), "T-") {
		t.Fatalf("thread create response = %#v", threadResponse)
	}

	threadID := "T-legacy"
	runReq := httptest.NewRequest(http.MethodPost, "/api/threads/"+threadID+"/runs", bytes.NewBufferString(`{"assistant_id":"asst_test","model":"gpt-5.5"}`))
	runReq.Header.Set("Content-Type", "application/json")
	runRec := httptest.NewRecorder()
	r.ServeHTTP(runRec, runReq)
	if runRec.Code != http.StatusOK {
		t.Fatalf("run create status = %d body=%s", runRec.Code, runRec.Body.String())
	}
	var runResponse map[string]any
	if err := json.Unmarshal(runRec.Body.Bytes(), &runResponse); err != nil {
		t.Fatalf("run create JSON error: %v", err)
	}
	runID := stringValue(runResponse["id"])
	if stringValue(runResponse["object"]) != "thread.run" || stringValue(runResponse["status"]) != "completed" || stringValue(runResponse["thread_id"]) != threadID || runID == "" {
		t.Fatalf("run create response = %#v", runResponse)
	}

	listReq := httptest.NewRequest(http.MethodGet, "/api/threads/"+threadID+"/runs", nil)
	listRec := httptest.NewRecorder()
	r.ServeHTTP(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("run list status = %d body=%s", listRec.Code, listRec.Body.String())
	}
	var listResponse map[string]any
	if err := json.Unmarshal(listRec.Body.Bytes(), &listResponse); err != nil {
		t.Fatalf("run list JSON error: %v", err)
	}
	if stringValue(listResponse["object"]) != "list" || len(arrayValue(listResponse["data"])) != 0 {
		t.Fatalf("run list response = %#v", listResponse)
	}

	stepsReq := httptest.NewRequest(http.MethodGet, "/api/threads/"+threadID+"/runs/"+runID+"/steps", nil)
	stepsRec := httptest.NewRecorder()
	r.ServeHTTP(stepsRec, stepsReq)
	if stepsRec.Code != http.StatusOK {
		t.Fatalf("steps status = %d body=%s", stepsRec.Code, stepsRec.Body.String())
	}

	cancelReq := httptest.NewRequest(http.MethodPost, "/api/threads/"+threadID+"/runs/"+runID+"/cancel", nil)
	cancelRec := httptest.NewRecorder()
	r.ServeHTTP(cancelRec, cancelReq)
	if cancelRec.Code != http.StatusOK {
		t.Fatalf("cancel status = %d body=%s", cancelRec.Code, cancelRec.Body.String())
	}
	var cancelResponse map[string]any
	if err := json.Unmarshal(cancelRec.Body.Bytes(), &cancelResponse); err != nil {
		t.Fatalf("cancel JSON error: %v", err)
	}
	if stringValue(cancelResponse["status"]) != "cancelled" || cancelResponse["cancelled_at"] == nil {
		t.Fatalf("cancel response = %#v", cancelResponse)
	}

	streamReq := httptest.NewRequest(http.MethodPost, "/api/threads/"+threadID+"/runs/"+runID+"/submit_tool_outputs", bytes.NewBufferString(`{"stream":true,"tool_outputs":[]}`))
	streamReq.Header.Set("Content-Type", "application/json")
	streamRec := httptest.NewRecorder()
	r.ServeHTTP(streamRec, streamReq)
	if streamRec.Code != http.StatusOK || !strings.Contains(streamRec.Header().Get("Content-Type"), "text/event-stream") || !strings.Contains(streamRec.Body.String(), "thread.run.completed") || !strings.Contains(streamRec.Body.String(), "[DONE]") {
		t.Fatalf("stream response = status %d content-type %q body=%s", streamRec.Code, streamRec.Header().Get("Content-Type"), streamRec.Body.String())
	}
}

func TestRegisterManagementRoutesServesNeoStartupInternalRPCPostsLocally(t *testing.T) {
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

	dir := t.TempDir()
	oldStoreDir := neoAmpThreadStoreDir
	neoAmpThreadStoreDir = func() string { return dir }
	t.Cleanup(func() { neoAmpThreadStoreDir = oldStoreDir })

	threadID := "T-019e06a8-13c9-708d-8090-783005818ea7"
	rawThread := []byte(`{"id":"` + threadID + `","title":"local","creatorUserID":"user_cloud","v":7,"agentMode":"deep","originThreadID":"T-origin","mainThreadID":"T-main","env":{"initial":{"trees":[]}},"meta":{"usesDtw":true,"usesThreadActors":true},"relationships":[],"messages":[{"messageId":"M-large","role":"user","content":[{"type":"text","text":"hello"}]}]}`)
	if err := os.WriteFile(filepath.Join(dir, threadID+".json"), rawThread, 0o600); err != nil {
		t.Fatalf("write local thread: %v", err)
	}

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

	t.Run("listThreads", func(t *testing.T) {
		proxyCalled = false
		body := bytes.NewBufferString(`{"method":"listThreads","params":{"includeArchived":false,"limit":200}}`)
		req := httptest.NewRequest(http.MethodPost, "/api/internal?listThreads", body)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
		}
		var response map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
			t.Fatalf("response JSON error: %v", err)
		}
		result := arrayValue(mapValue(response["result"])["threads"])
		if len(result) != 1 {
			t.Fatalf("result = %#v", response["result"])
		}
		thread := mapValue(result[0])
		if stringValue(thread["id"]) != threadID || stringValue(thread["title"]) != "local" {
			t.Fatalf("thread metadata = %#v", thread)
		}
		if _, exists := thread["messages"]; exists {
			t.Fatalf("listThreads should return metadata-only thread entries: %#v", thread)
		}
		if stringValue(thread["creatorUserID"]) != neoLocalOwnerUserID {
			t.Fatalf("creatorUserID = %#v, want %q", thread["creatorUserID"], neoLocalOwnerUserID)
		}
		if relationships, ok := thread["relationships"].([]any); !ok || relationships == nil {
			t.Fatalf("relationships = %#v, want empty array", thread["relationships"])
		}
		meta := mapValue(thread["meta"])
		if stringValue(meta["visibility"]) != "private" {
			t.Fatalf("meta = %#v, want private visibility", thread["meta"])
		}
		if sharedGroupIDs, ok := meta["sharedGroupIDs"].([]any); !ok || sharedGroupIDs == nil {
			t.Fatalf("sharedGroupIDs = %#v, want empty array", meta["sharedGroupIDs"])
		}
		if numberFrom(mapValue(thread["summaryStats"])["messageCount"]) != 1 {
			t.Fatalf("summaryStats = %#v, want messageCount=1", thread["summaryStats"])
		}
		if stringValue(thread["agentMode"]) != "deep" || numberFrom(thread["v"]) != 7 {
			t.Fatalf("local list metadata = %#v", thread)
		}
		if stringValue(thread["originThreadID"]) != "T-origin" || stringValue(thread["mainThreadID"]) != "T-main" {
			t.Fatalf("local related thread metadata = origin:%#v main:%#v", thread["originThreadID"], thread["mainThreadID"])
		}
		if _, exists := thread["env"]; !exists {
			t.Fatalf("local env metadata missing: %#v", thread)
		}
		if thread["usesDtw"] != true || thread["usesThreadActors"] != true {
			t.Fatalf("local DTW/thread-actors flags = %#v", thread)
		}
	})

	t.Run("listThreads merges cloud threads", func(t *testing.T) {
		cloudThreadID := "T-019e1046-656d-7132-879f-390ded941c99"
		cloudOnlyThreadID := "T-019e1046-656d-7132-879f-390ded941c98"
		upstreamRequests := 0
		gotAuth := ""
		upstreamHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			upstreamRequests++
			if r.URL.Path != "/api/internal" || r.URL.RawQuery != "listThreads" {
				proxyCalled = true
				w.WriteHeader(http.StatusTeapot)
				_, _ = w.Write([]byte("unexpected upstream request"))
				return
			}
			gotAuth = r.Header.Get("Authorization")
			writeNeoJSON(w, http.StatusOK, map[string]any{
				"ok": true,
				"result": map[string]any{
					"threads": []any{
						map[string]any{
							"id":               cloudOnlyThreadID,
							"title":            "cloud only",
							"updatedAt":        3000,
							"messageCount":     4,
							"v":                7,
							"agentMode":        "deep",
							"env":              map[string]any{"initial": map[string]any{"trees": []any{}}},
							"summaryStats":     map[string]any{"messageCount": 4},
							"usesDtw":          false,
							"usesThreadActors": false,
							"relationships":    []any{},
						},
						map[string]any{"id": cloudThreadID, "title": "cloud newer", "updatedAt": 2000, "messageCount": 2},
					},
				},
			})
		})

		if err := os.WriteFile(filepath.Join(dir, cloudThreadID+".json"), []byte(`{"id":"`+cloudThreadID+`","title":"local older","updatedAt":1000,"messages":[{"messageId":"M-local","role":"user","created":1000,"content":[{"type":"text","text":"hello"}]}]}`), 0o600); err != nil {
			t.Fatalf("write local thread: %v", err)
		}

		body := bytes.NewBufferString(`{"method":"listThreads","params":{"includeArchived":false,"limit":10}}`)
		req := httptest.NewRequest(http.MethodPost, "/api/internal?listThreads", body)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
		}
		if upstreamRequests != 1 {
			t.Fatalf("upstreamRequests = %d, want 1", upstreamRequests)
		}
		if gotAuth != "Bearer secret" {
			t.Fatalf("Authorization = %q", gotAuth)
		}
		var response map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
			t.Fatalf("response JSON error: %v", err)
		}
		result := arrayValue(mapValue(response["result"])["threads"])
		if len(result) != 3 {
			t.Fatalf("result = %#v", response["result"])
		}
		first := mapValue(result[0])
		second := mapValue(result[1])
		third := mapValue(result[2])
		if stringValue(first["id"]) != threadID || stringValue(second["id"]) != cloudOnlyThreadID || stringValue(third["id"]) != cloudThreadID {
			t.Fatalf("thread order = %#v", result)
		}
		if relationships, ok := second["relationships"].([]any); !ok || relationships == nil {
			t.Fatalf("cloud relationships = %#v, want empty array", second["relationships"])
		}
		if stringValue(second["agentMode"]) != "deep" || numberFrom(second["v"]) != 7 {
			t.Fatalf("cloud metadata = %#v", second)
		}
		if _, exists := second["env"]; !exists {
			t.Fatalf("cloud env metadata missing: %#v", second)
		}
		if numberFrom(mapValue(second["summaryStats"])["messageCount"]) != 4 {
			t.Fatalf("cloud summaryStats = %#v, want messageCount=4", second["summaryStats"])
		}
		if stringValue(third["title"]) != "cloud newer" {
			t.Fatalf("merged thread title = %#v", third["title"])
		}
		if numberFrom(third["messageCount"]) != 2 {
			t.Fatalf("merged thread messageCount = %#v", third["messageCount"])
		}
		if _, exists := third["messages"]; exists {
			t.Fatalf("listThreads should stay metadata-only after cloud merge: %#v", third)
		}
		if relationships, ok := third["relationships"].([]any); !ok || relationships == nil {
			t.Fatalf("merged relationships = %#v, want empty array", third["relationships"])
		}

		limitedBody := bytes.NewBufferString(`{"method":"listThreads","params":{"includeArchived":false,"limit":2}}`)
		limitedReq := httptest.NewRequest(http.MethodPost, "/api/internal?listThreads", limitedBody)
		limitedReq.Header.Set("Content-Type", "application/json")
		limitedRec := httptest.NewRecorder()
		r.ServeHTTP(limitedRec, limitedReq)
		if limitedRec.Code != http.StatusOK {
			t.Fatalf("limited status = %d, body=%s", limitedRec.Code, limitedRec.Body.String())
		}
		var limitedResponse map[string]any
		if err := json.Unmarshal(limitedRec.Body.Bytes(), &limitedResponse); err != nil {
			t.Fatalf("limited response JSON error: %v", err)
		}
		if limited := arrayValue(mapValue(limitedResponse["result"])["threads"]); len(limited) != 2 {
			t.Fatalf("limited result length = %d, want 2: %#v", len(limited), limited)
		}
	})

	for _, tc := range []struct {
		name string
		path string
		body string
	}{
		{
			name: "notices",
			path: "/api/internal?notices",
			body: `{"method":"notices","params":{}}`,
		},
		{
			name: "getUserFreeTierStatus",
			path: "/api/internal?getUserFreeTierStatus",
			body: `{"method":"getUserFreeTierStatus","params":{}}`,
		},
		{
			name: "uploadThread",
			path: "/api/internal?uploadThread",
			body: `{"method":"uploadThread","params":{"thread":{"id":"` + threadID + `"},"createdOnServer":false}}`,
		},
		{
			name: "logNoticeAction",
			path: "/api/internal?logNoticeAction",
			body: `{"method":"logNoticeAction","params":{"key":"local","action":"view"}}`,
		},
		{
			name: "markAsReadMysteriousMessage",
			path: "/api/internal?markAsReadMysteriousMessage",
			body: `{"method":"markAsReadMysteriousMessage","params":{"messageId":"msg_local"}}`,
		},
		{
			name: "userDisplayBalanceInfo",
			path: "/api/internal?userDisplayBalanceInfo",
			body: `{"method":"userDisplayBalanceInfo","params":{}}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proxyCalled = false
			req := httptest.NewRequest(http.MethodPost, tc.path, bytes.NewBufferString(tc.body))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, req)

			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
			}
			if proxyCalled {
				t.Fatalf("%s should be served locally during Neo startup", tc.name)
			}
			var response map[string]any
			if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
				t.Fatalf("response JSON error: %v", err)
			}
			if response["ok"] != true {
				t.Fatalf("unexpected local internal response: %#v", response)
			}
		})
	}

	t.Run("getThread", func(t *testing.T) {
		proxyCalled = false
		body := bytes.NewBufferString(`{"method":"getThread","params":{"thread":"` + threadID + `"}}`)
		req := httptest.NewRequest(http.MethodPost, "/api/internal?getThread", body)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
		}
		if proxyCalled {
			t.Fatal("getThread should be served locally during Neo startup")
		}
		var response map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
			t.Fatalf("response JSON error: %v", err)
		}
		if response["ok"] != true {
			t.Fatalf("unexpected local internal response: %#v", response)
		}
		thread := mapValue(mapValue(mapValue(response["result"])["thread"])["data"])
		if stringValue(thread["id"]) != threadID || stringValue(thread["title"]) != "local" {
			t.Fatalf("thread data = %#v", thread)
		}
		if stringValue(thread["creatorUserID"]) != neoLocalOwnerUserID {
			t.Fatalf("creatorUserID = %#v, want %q", thread["creatorUserID"], neoLocalOwnerUserID)
		}
		envelope := mapValue(mapValue(response["result"])["thread"])
		if stringValue(envelope["agentMode"]) != "deep" || stringValue(thread["agentMode"]) != "deep" {
			t.Fatalf("agentMode envelope=%#v data=%#v, want deep", envelope["agentMode"], thread["agentMode"])
		}
	})

	t.Run("thread meta", func(t *testing.T) {
		proxyCalled = false
		body := bytes.NewBufferString(`{"method":"getThreadMeta","params":{"thread":"` + threadID + `"}}`)
		req := httptest.NewRequest(http.MethodPost, "/api/internal?getThreadMeta", body)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("getThreadMeta status = %d, body=%s", rec.Code, rec.Body.String())
		}
		if proxyCalled {
			t.Fatal("getThreadMeta should be served locally")
		}
		var response map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
			t.Fatalf("getThreadMeta response JSON error: %v", err)
		}
		meta := mapValue(mapValue(response["result"])["meta"])
		if stringValue(meta["visibility"]) != "private" {
			t.Fatalf("initial meta = %#v, want private visibility", meta)
		}

		setBody := bytes.NewBufferString(`{"method":"setThreadMeta","params":{"thread":"` + threadID + `","meta":{"visibility":"thread_workspace_shared","shareWithAllCreatorGroups":true,"sharedGroupIDs":["G-local"]}}}`)
		setReq := httptest.NewRequest(http.MethodPost, "/api/internal?setThreadMeta", setBody)
		setReq.Header.Set("Content-Type", "application/json")
		setRec := httptest.NewRecorder()
		r.ServeHTTP(setRec, setReq)

		if setRec.Code != http.StatusOK {
			t.Fatalf("setThreadMeta status = %d, body=%s", setRec.Code, setRec.Body.String())
		}
		if proxyCalled {
			t.Fatal("setThreadMeta should be served locally")
		}
		if err := json.Unmarshal(setRec.Body.Bytes(), &response); err != nil {
			t.Fatalf("setThreadMeta response JSON error: %v", err)
		}
		meta = mapValue(mapValue(response["result"])["meta"])
		if stringValue(meta["visibility"]) != "thread_workspace_shared" {
			t.Fatalf("updated meta = %#v, want workspace shared visibility", meta)
		}
		reloaded, ok := loadNeoLocalThread(threadID)
		if !ok {
			t.Fatal("thread should still be stored locally")
		}
		if stringValue(mapValue(reloaded["meta"])["visibility"]) != "thread_workspace_shared" {
			t.Fatalf("persisted meta = %#v", reloaded["meta"])
		}
	})

	t.Run("thread labels", func(t *testing.T) {
		proxyCalled = false
		addBody := bytes.NewBufferString(`{"method":"addThreadLabels","params":{"thread":"` + threadID + `","labels":["parity","runtime"]}}`)
		addReq := httptest.NewRequest(http.MethodPost, "/api/internal?addThreadLabels", addBody)
		addReq.Header.Set("Content-Type", "application/json")
		addRec := httptest.NewRecorder()
		r.ServeHTTP(addRec, addReq)

		if addRec.Code != http.StatusOK {
			t.Fatalf("addThreadLabels status = %d, body=%s", addRec.Code, addRec.Body.String())
		}
		if proxyCalled {
			t.Fatal("addThreadLabels should be served locally")
		}
		var response map[string]any
		if err := json.Unmarshal(addRec.Body.Bytes(), &response); err != nil {
			t.Fatalf("addThreadLabels response JSON error: %v", err)
		}
		labels := arrayValue(response["result"])
		if len(labels) != 2 {
			t.Fatalf("addThreadLabels result = %#v", response["result"])
		}

		getReq := httptest.NewRequest(http.MethodPost, "/api/internal?getThreadLabels", bytes.NewBufferString(`{"method":"getThreadLabels","params":{"thread":"`+threadID+`"}}`))
		getReq.Header.Set("Content-Type", "application/json")
		getRec := httptest.NewRecorder()
		r.ServeHTTP(getRec, getReq)
		if getRec.Code != http.StatusOK {
			t.Fatalf("getThreadLabels status = %d, body=%s", getRec.Code, getRec.Body.String())
		}
		if err := json.Unmarshal(getRec.Body.Bytes(), &response); err != nil {
			t.Fatalf("getThreadLabels response JSON error: %v", err)
		}
		labels = arrayValue(response["result"])
		if len(labels) != 2 || stringValue(mapValue(labels[0])["name"]) != "parity" || stringValue(mapValue(labels[1])["name"]) != "runtime" {
			t.Fatalf("getThreadLabels result = %#v", response["result"])
		}

		userReq := httptest.NewRequest(http.MethodPost, "/api/internal?getUserLabels", bytes.NewBufferString(`{"method":"getUserLabels","params":{"query":"run"}}`))
		userReq.Header.Set("Content-Type", "application/json")
		userRec := httptest.NewRecorder()
		r.ServeHTTP(userRec, userReq)
		if userRec.Code != http.StatusOK {
			t.Fatalf("getUserLabels status = %d, body=%s", userRec.Code, userRec.Body.String())
		}
		if err := json.Unmarshal(userRec.Body.Bytes(), &response); err != nil {
			t.Fatalf("getUserLabels response JSON error: %v", err)
		}
		labels = arrayValue(response["result"])
		if len(labels) != 1 || stringValue(mapValue(labels[0])["name"]) != "runtime" {
			t.Fatalf("getUserLabels result = %#v", response["result"])
		}

		listReq := httptest.NewRequest(http.MethodPost, "/api/internal?listThreads", bytes.NewBufferString(`{"method":"listThreads","params":{"includeArchived":false,"limit":10}}`))
		listReq.Header.Set("Content-Type", "application/json")
		listRec := httptest.NewRecorder()
		r.ServeHTTP(listRec, listReq)
		if listRec.Code != http.StatusOK {
			t.Fatalf("listThreads status = %d, body=%s", listRec.Code, listRec.Body.String())
		}
		if err := json.Unmarshal(listRec.Body.Bytes(), &response); err != nil {
			t.Fatalf("listThreads response JSON error: %v", err)
		}
		threads := arrayValue(mapValue(response["result"])["threads"])
		if len(threads) == 0 {
			t.Fatalf("listThreads result = %#v", response["result"])
		}
		var listedThread map[string]any
		for _, thread := range threads {
			candidate := mapValue(thread)
			if stringValue(candidate["id"]) == threadID {
				listedThread = candidate
				break
			}
		}
		if len(listedThread) == 0 {
			t.Fatalf("thread %s missing from listThreads result: %#v", threadID, response["result"])
		}
		listLabels := arrayValue(listedThread["labels"])
		if len(listLabels) != 2 || stringValue(mapValue(listLabels[0])["name"]) != "parity" || stringValue(mapValue(listLabels[1])["name"]) != "runtime" {
			t.Fatalf("list thread = %#v", listedThread)
		}
	})

	t.Run("archive and delete thread", func(t *testing.T) {
		proxyCalled = false
		archiveBody := bytes.NewBufferString(`{"method":"archiveThread","params":{"thread":"` + threadID + `","archived":true}}`)
		archiveReq := httptest.NewRequest(http.MethodPost, "/api/internal?archiveThread", archiveBody)
		archiveReq.Header.Set("Content-Type", "application/json")
		archiveRec := httptest.NewRecorder()
		r.ServeHTTP(archiveRec, archiveReq)

		if archiveRec.Code != http.StatusOK {
			t.Fatalf("archiveThread status = %d, body=%s", archiveRec.Code, archiveRec.Body.String())
		}
		if proxyCalled {
			t.Fatal("archiveThread should be served locally")
		}
		reloaded, ok := loadNeoLocalThread(threadID)
		if !ok || reloaded["archived"] != true {
			t.Fatalf("archived thread = %#v, ok=%v", reloaded, ok)
		}

		deleteID := "T-019e06a8-13c9-708d-8090-783005818ef0"
		if err := os.WriteFile(filepath.Join(dir, deleteID+".json"), []byte(`{"id":"`+deleteID+`","title":"delete me"}`), 0o600); err != nil {
			t.Fatalf("write delete thread: %v", err)
		}
		deleteBody := bytes.NewBufferString(`{"method":"deleteThread","params":{"thread":"` + deleteID + `"}}`)
		deleteReq := httptest.NewRequest(http.MethodPost, "/api/internal?deleteThread", deleteBody)
		deleteReq.Header.Set("Content-Type", "application/json")
		deleteRec := httptest.NewRecorder()
		r.ServeHTTP(deleteRec, deleteReq)

		if deleteRec.Code != http.StatusOK {
			t.Fatalf("deleteThread status = %d, body=%s", deleteRec.Code, deleteRec.Body.String())
		}
		if _, err := os.Stat(filepath.Join(dir, deleteID+".json")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("deleteThread stat err = %v, want not exist", err)
		}
	})
}

func TestRegisterManagementRoutesServesNeoTaskInternalMethodsLocally(t *testing.T) {
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

	postTask := func(method, body string) map[string]any {
		t.Helper()
		proxyCalled = false
		req := httptest.NewRequest(http.MethodPost, "/api/internal?"+method, bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s status = %d, body=%s", method, rec.Code, rec.Body.String())
		}
		if proxyCalled {
			t.Fatalf("%s should be served locally", method)
		}
		var response map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
			t.Fatalf("%s response JSON error: %v", method, err)
		}
		return response
	}

	createBuild := postTask("createTask", `{"method":"createTask","params":{"title":"Run the build","repoURL":"https://github.com/acme/repo","threadID":"T-local"}}`)
	if createBuild["ok"] != true {
		t.Fatalf("createTask build response = %#v", createBuild)
	}
	buildTask := mapValue(createBuild["result"])
	buildID := stringValue(buildTask["id"])
	if buildID == "" || stringValue(buildTask["status"]) != "open" || stringValue(buildTask["threadID"]) != "T-local" {
		t.Fatalf("created build task = %#v", buildTask)
	}

	createFix := postTask("createTask", `{"method":"createTask","params":{"title":"Fix type errors","repoURL":"https://github.com/acme/repo","dependsOn":["`+buildID+`"],"parentID":"`+buildID+`"}}`)
	if createFix["ok"] != true {
		t.Fatalf("createTask fix response = %#v", createFix)
	}
	fixTask := mapValue(createFix["result"])
	fixID := stringValue(fixTask["id"])
	if fixID == "" || stringValue(fixTask["parentID"]) != buildID {
		t.Fatalf("created fix task = %#v", fixTask)
	}

	notReady := postTask("listTasks", `{"method":"listTasks","params":{"dependsOn":"`+buildID+`","ready":true}}`)
	if tasks := arrayValue(mapValue(notReady["result"])["tasks"]); len(tasks) != 0 {
		t.Fatalf("ready tasks before dependency completion = %#v", tasks)
	}

	updateBuild := postTask("updateTask", `{"method":"updateTask","params":{"taskID":"`+buildID+`","status":"completed"}}`)
	if updateBuild["ok"] != true || stringValue(mapValue(updateBuild["result"])["status"]) != "completed" {
		t.Fatalf("updateTask build response = %#v", updateBuild)
	}

	ready := postTask("listTasks", `{"method":"listTasks","params":{"repoURL":"https://github.com/acme/repo","dependsOn":"`+buildID+`","ready":true,"limit":1}}`)
	readyTasks := arrayValue(mapValue(ready["result"])["tasks"])
	if len(readyTasks) != 1 || stringValue(mapValue(readyTasks[0])["id"]) != fixID {
		t.Fatalf("ready tasks after dependency completion = %#v", readyTasks)
	}

	getFix := postTask("getTask", `{"method":"getTask","params":{"taskID":"`+fixID+`"}}`)
	if getFix["ok"] != true || stringValue(mapValue(getFix["result"])["title"]) != "Fix type errors" {
		t.Fatalf("getTask response = %#v", getFix)
	}

	blockedDelete := postTask("deleteTask", `{"method":"deleteTask","params":{"taskID":"`+buildID+`","blockIfHasChildren":true}}`)
	if blockedDelete["ok"] != false || stringValue(mapValue(blockedDelete["error"])["code"]) != "has-children" {
		t.Fatalf("deleteTask blocked response = %#v", blockedDelete)
	}

	deleteFix := postTask("deleteTask", `{"method":"deleteTask","params":{"taskID":"`+fixID+`","blockIfHasChildren":true}}`)
	if deleteFix["ok"] != true {
		t.Fatalf("deleteTask fix response = %#v", deleteFix)
	}
	deleteBuild := postTask("deleteTask", `{"method":"deleteTask","params":{"taskID":"`+buildID+`","blockIfHasChildren":true}}`)
	if deleteBuild["ok"] != true {
		t.Fatalf("deleteTask build response = %#v", deleteBuild)
	}
	if _, err := os.Stat(filepath.Join(dir, "tasks", "tasks.json")); err != nil {
		t.Fatalf("task store stat error: %v", err)
	}
}

func TestRegisterManagementRoutesServesLocalThreadLinkInfoWithNormalizedOwnership(t *testing.T) {
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

	threadID := "T-019e1046-656d-7132-879f-390ded941c16"
	raw := []byte(`{
		"id": "` + threadID + `",
		"title": "resume me locally",
		"creatorUserID": "user_123",
		"ownerUserId": "user_123"
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

	req := httptest.NewRequest(http.MethodGet, "/api/internal?getThreadLinkInfo&thread="+threadID, nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if proxyCalled {
		t.Fatal("getThreadLinkInfo should be served locally")
	}
	var response map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("response JSON error: %v", err)
	}
	if response["ok"] != true {
		t.Fatalf("unexpected response: %#v", response)
	}
	result := mapValue(response["result"])
	if stringValue(result["id"]) != threadID {
		t.Fatalf("thread link info id = %#v, want %q", result["id"], threadID)
	}
	if stringValue(result["creatorUserID"]) != neoLocalOwnerUserID {
		t.Fatalf("creatorUserID = %#v, want %q", result["creatorUserID"], neoLocalOwnerUserID)
	}
	if stringValue(result["ownerUserId"]) != neoLocalOwnerUserID {
		t.Fatalf("ownerUserId = %#v, want %q", result["ownerUserId"], neoLocalOwnerUserID)
	}
	if stringValue(result["title"]) != "resume me locally" {
		t.Fatalf("title = %#v, want %q", result["title"], "resume me locally")
	}
	if _, ok := loadNeoLocalThread(threadID); !ok {
		t.Fatal("expected local thread to remain readable after normalization")
	}
}

func TestRegisterManagementRoutesGetThreadLinkInfoDoesNotProxyEndToEnd(t *testing.T) {
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

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body=%s", resp.StatusCode, body)
	}
	if upstreamRequests != 0 {
		t.Fatalf("expected local response without proxying, upstream saw %d request(s)", upstreamRequests)
	}
	var response map[string]any
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatalf("response JSON error: %v body=%s", err, body)
	}
	result := mapValue(response["result"])
	if stringValue(result["id"]) != threadID {
		t.Fatalf("thread link info id = %#v, want %q", result["id"], threadID)
	}
	if stringValue(result["creatorUserID"]) != neoLocalOwnerUserID {
		t.Fatalf("creatorUserID = %#v, want %q", result["creatorUserID"], neoLocalOwnerUserID)
	}
	if stringValue(result["ownerUserId"]) != neoLocalOwnerUserID {
		t.Fatalf("ownerUserId = %#v, want %q", result["ownerUserId"], neoLocalOwnerUserID)
	}
	if stringValue(result["title"]) != "continued locally" {
		t.Fatalf("title = %#v, want %q", result["title"], "continued locally")
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
