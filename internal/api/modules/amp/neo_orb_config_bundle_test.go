package amp

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/orbconfig"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/api/handlers"
)

func TestLocalBrokerOrbConfigBundleEndpointSecurityAndPersistence(t *testing.T) {
	useTempNeoThreadStore(t)
	gin.SetMode(gin.TestMode)
	upstream := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/api/internal" || request.URL.RawQuery != "getUserInfo" || request.Header.Get("Authorization") != "Bearer upstream" {
			http.Error(response, "owner unavailable", http.StatusUnauthorized)
			return
		}
		writeNeoJSON(response, http.StatusOK, map[string]any{"ok": true, "result": map[string]any{"id": "user_owner"}})
	}))
	t.Cleanup(upstream.Close)
	runtime := newNeoRuntime(&config.Config{
		SDKConfig: config.SDKConfig{APIKeys: []string{"client"}},
		AmpCode: config.AmpCode{
			UpstreamURL:    upstream.URL,
			UpstreamAPIKey: "upstream",
		},
	})
	runtime.setSecretSource(NewStaticSecretSource("upstream"))
	module := &AmpModule{restrictToLocalhost: true, neoRuntime: runtime}
	router := gin.New()
	auth := func(context *gin.Context) {
		if context.GetHeader("Authorization") != "Bearer client" {
			context.AbortWithStatus(http.StatusUnauthorized)
			return
		}
		context.Set("userApiKey", "client")
		context.Next()
	}
	module.registerManagementRoutes(router, &handlers.BaseAPIHandler{}, auth)

	heartbeatBody := `{"brokerId":"broker","sessionId":"session","sessionGeneration":3,"hostname":"Mac","pid":123,"runners":[],"orbConfigDigest":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`
	heartbeat := requestNeoOrbConfigEndpoint(router, http.MethodPost, heartbeatEndpointPathForTest, "client", "application/json", "", heartbeatBody)
	if heartbeat.Code != http.StatusOK || !strings.Contains(heartbeat.Body.String(), `"orbConfigDigest":""`) {
		t.Fatalf("heartbeat status=%d body=%s", heartbeat.Code, heartbeat.Body.String())
	}
	legacyHeartbeatBody := strings.Replace(heartbeatBody, `,"orbConfigDigest":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"`, "", 1)
	legacyHeartbeat := requestNeoOrbConfigEndpoint(router, http.MethodPost, heartbeatEndpointPathForTest, "client", "application/json", "", legacyHeartbeatBody)
	if legacyHeartbeat.Code != http.StatusOK || strings.Contains(legacyHeartbeat.Body.String(), "orbConfigDigest") || legacyHeartbeat.Header().Get("X-Cliproxy-Orb-Config") != "1" {
		t.Fatalf("legacy heartbeat status=%d headers=%v body=%s", legacyHeartbeat.Code, legacyHeartbeat.Header(), legacyHeartbeat.Body.String())
	}

	bundle, err := orbconfig.New([]orbconfig.DecodedFile{
		{Path: "checks/review.md", Content: []byte("review")},
		{Path: "skills/demo/SKILL.md", Content: []byte("---\nname: demo\n---\n")},
	}, []orbconfig.Extension{{Repo: "owner/gh-demo", Version: strings.Repeat("a", 40)}})
	if err != nil {
		t.Fatal(err)
	}
	upload := neoOwnerOrbConfigUploadRequest{BrokerID: "broker", SessionID: "session", SessionGeneration: 3, Bundle: &bundle}
	uploadRaw, err := json.Marshal(upload)
	if err != nil {
		t.Fatal(err)
	}
	accepted := requestNeoOrbConfigEndpoint(router, http.MethodPut, orbConfigEndpointPathForTest, "client", "application/json; charset=utf-8", "", string(uploadRaw))
	if accepted.Code != http.StatusOK || !strings.Contains(accepted.Body.String(), bundle.Digest) {
		t.Fatalf("upload status=%d body=%s", accepted.Code, accepted.Body.String())
	}
	stored, files, found, err := runtime.ownerOrbConfig("user_owner")
	if err != nil || !found || stored.Digest != bundle.Digest || string(files["checks/review.md"]) != "review" {
		t.Fatalf("stored bundle=%#v files=%#v found=%t err=%v", stored, files, found, err)
	}
	ownerDirectory, err := runtime.orbConfigStore.ownerDirectory("user_owner")
	if err != nil {
		t.Fatal(err)
	}
	for filePath, wantMode := range map[string]os.FileMode{
		runtime.orbConfigStore.rootDir:               0o700,
		ownerDirectory:                               0o700,
		filepath.Join(ownerDirectory, "bundle.json"): 0o600,
	} {
		info, err := os.Stat(filePath)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != wantMode {
			t.Fatalf("mode for %s = %o, want %o", filePath, info.Mode().Perm(), wantMode)
		}
	}
	before, err := os.Stat(filepath.Join(ownerDirectory, "bundle.json"))
	if err != nil {
		t.Fatal(err)
	}
	idempotent := requestNeoOrbConfigEndpoint(router, http.MethodPut, orbConfigEndpointPathForTest, "client", "application/json", "", string(uploadRaw))
	if idempotent.Code != http.StatusOK {
		t.Fatalf("idempotent upload status=%d body=%s", idempotent.Code, idempotent.Body.String())
	}
	after, err := os.Stat(filepath.Join(ownerDirectory, "bundle.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, after) {
		t.Fatal("digest-identical upload rewrote the current bundle")
	}
	if err := os.WriteFile(filepath.Join(ownerDirectory, "bundle.json"), []byte("invalid\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if digest, found, err := runtime.ownerOrbConfigDigest("user_owner"); err != nil || found || digest != "" {
		t.Fatalf("invalid stored digest = %q found=%t err=%v", digest, found, err)
	}
	repaired := requestNeoOrbConfigEndpoint(router, http.MethodPut, orbConfigEndpointPathForTest, "client", "application/json", "", string(uploadRaw))
	if repaired.Code != http.StatusOK {
		t.Fatalf("repair upload status=%d body=%s", repaired.Code, repaired.Body.String())
	}
	if stored, _, found, err := runtime.ownerOrbConfig("user_owner"); err != nil || !found || stored.Digest != bundle.Digest {
		t.Fatalf("repaired bundle=%#v found=%t err=%v", stored, found, err)
	}
	updatedBundle, err := orbconfig.New([]orbconfig.DecodedFile{{Path: "checks/review.md", Content: []byte("updated")}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	updatedUploadRaw, err := json.Marshal(neoOwnerOrbConfigUploadRequest{BrokerID: "broker", SessionID: "session", SessionGeneration: 3, Bundle: &updatedBundle})
	if err != nil {
		t.Fatal(err)
	}
	userActor := runtime.store.userActorForOwner("user_owner")
	if userActor == nil {
		t.Fatal("owner actor is unavailable")
	}
	runtime.orbConfigStore.mu.Lock()
	storeLocked := true
	defer func() {
		if storeLocked {
			runtime.orbConfigStore.mu.Unlock()
		}
	}()
	uploadDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		uploadDone <- requestNeoOrbConfigEndpoint(router, http.MethodPut, orbConfigEndpointPathForTest, "client", "application/json", "", string(updatedUploadRaw))
	}()
	lockDeadline := time.Now().Add(5 * time.Second)
	for {
		if !userActor.mu.TryLock() {
			break
		}
		userActor.mu.Unlock()
		if time.Now().After(lockDeadline) {
			t.Fatal("upload did not retain the owner session lock during publication")
		}
		time.Sleep(time.Millisecond)
	}
	supersedingHeartbeatBody := `{"brokerId":"broker","sessionId":"replacement","sessionGeneration":4,"hostname":"Mac","pid":124,"runners":[]}`
	heartbeatDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		heartbeatDone <- requestNeoOrbConfigEndpoint(router, http.MethodPost, heartbeatEndpointPathForTest, "client", "application/json", "", supersedingHeartbeatBody)
	}()
	runtime.orbConfigStore.mu.Unlock()
	storeLocked = false
	var serializedUpload *httptest.ResponseRecorder
	select {
	case serializedUpload = <-uploadDone:
	case <-time.After(5 * time.Second):
		t.Fatal("serialized upload did not complete")
	}
	if serializedUpload.Code != http.StatusOK {
		t.Fatalf("serialized upload status=%d body=%s", serializedUpload.Code, serializedUpload.Body.String())
	}
	var supersedingHeartbeat *httptest.ResponseRecorder
	select {
	case supersedingHeartbeat = <-heartbeatDone:
	case <-time.After(5 * time.Second):
		t.Fatal("superseding heartbeat did not complete")
	}
	if supersedingHeartbeat.Code != http.StatusOK {
		t.Fatalf("superseding heartbeat status=%d body=%s", supersedingHeartbeat.Code, supersedingHeartbeat.Body.String())
	}
	staleAfterSupersession := requestNeoOrbConfigEndpoint(router, http.MethodPut, orbConfigEndpointPathForTest, "client", "application/json", "", string(updatedUploadRaw))
	if staleAfterSupersession.Code != http.StatusConflict || !strings.Contains(staleAfterSupersession.Body.String(), "stale_session") {
		t.Fatalf("superseded upload status=%d body=%s", staleAfterSupersession.Code, staleAfterSupersession.Body.String())
	}
	if stored, _, found, err := runtime.ownerOrbConfig("user_owner"); err != nil || !found || stored.Digest != updatedBundle.Digest {
		t.Fatalf("serialized bundle=%#v found=%t err=%v", stored, found, err)
	}

	unsafeBodies := map[string]string{
		"unknown owner":   strings.TrimSuffix(string(uploadRaw), "}") + `,"ownerUserID":"other"}`,
		"unknown field":   strings.TrimSuffix(string(uploadRaw), "}") + `,"unknown":true}`,
		"duplicate field": strings.Replace(string(uploadRaw), `"brokerId":"broker"`, `"brokerId":"broker","brokerId":"broker"`, 1),
		"trailing JSON":   string(uploadRaw) + `{}`,
	}
	for name, body := range unsafeBodies {
		t.Run(name, func(t *testing.T) {
			response := requestNeoOrbConfigEndpoint(router, http.MethodPut, orbConfigEndpointPathForTest, "client", "application/json", "", body)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
		})
	}
	staleRaw := strings.Replace(string(uploadRaw), `"sessionGeneration":3`, `"sessionGeneration":2`, 1)
	stale := requestNeoOrbConfigEndpoint(router, http.MethodPut, orbConfigEndpointPathForTest, "client", "application/json", "", staleRaw)
	if stale.Code != http.StatusConflict || !strings.Contains(stale.Body.String(), "stale_session") {
		t.Fatalf("stale status=%d body=%s", stale.Code, stale.Body.String())
	}
	oversized := requestNeoOrbConfigEndpoint(router, http.MethodPut, orbConfigEndpointPathForTest, "client", "application/json", "", strings.Repeat("x", orbconfig.MaxBodyBytes+1))
	if oversized.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized status=%d body=%s", oversized.Code, oversized.Body.String())
	}
	for _, request := range []struct {
		name        string
		method      string
		apiKey      string
		contentType string
		origin      string
		want        int
	}{
		{name: "missing auth", method: http.MethodPut, contentType: "application/json", want: http.StatusUnauthorized},
		{name: "browser origin", method: http.MethodPut, apiKey: "client", contentType: "application/json", origin: "https://ampcode.com", want: http.StatusForbidden},
		{name: "wrong method", method: http.MethodPost, apiKey: "client", contentType: "application/json", want: http.StatusMethodNotAllowed},
		{name: "wrong content type", method: http.MethodPut, apiKey: "client", contentType: "text/plain", want: http.StatusUnsupportedMediaType},
	} {
		t.Run(request.name, func(t *testing.T) {
			response := requestNeoOrbConfigEndpoint(router, request.method, orbConfigEndpointPathForTest, request.apiKey, request.contentType, request.origin, string(uploadRaw))
			if response.Code != request.want {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			for header := range response.Header() {
				if strings.HasPrefix(strings.ToLower(header), "access-control-") {
					t.Fatalf("response contains CORS header %q", header)
				}
			}
		})
	}
}

const (
	heartbeatEndpointPathForTest = "/ampcode/local-broker/heartbeat.json"
	orbConfigEndpointPathForTest = orbconfig.EndpointPath
)

func requestNeoOrbConfigEndpoint(router http.Handler, method, requestPath, apiKey, contentType, origin, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, requestPath, strings.NewReader(body))
	request.RemoteAddr = "203.0.113.10:1234"
	if apiKey != "" {
		request.Header.Set("Authorization", "Bearer "+apiKey)
	}
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	if origin != "" {
		request.Header.Set("Origin", origin)
	}
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	return response
}

func TestDecodeNeoOwnerOrbConfigUploadRejectsInvalidBundle(t *testing.T) {
	bundle, err := orbconfig.New([]orbconfig.DecodedFile{{Path: "checks/review.md", Content: []byte("review")}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	bundle.Digest = strings.Repeat("0", 64)
	raw, err := json.Marshal(neoOwnerOrbConfigUploadRequest{BrokerID: "broker", SessionID: "session", SessionGeneration: 1, Bundle: &bundle})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeNeoOwnerOrbConfigUpload(raw); err == nil || !strings.Contains(err.Error(), "digest") {
		t.Fatalf("decode error = %v", err)
	}
	if _, err := decodeNeoOwnerOrbConfigUpload([]byte(fmt.Sprintf(`{"brokerId":"broker","sessionId":"session","sessionGeneration":1,"bundle":%s,"bundle":%s}`, mustNeoOrbConfigJSON(t, bundle), mustNeoOrbConfigJSON(t, bundle)))); err == nil {
		t.Fatal("duplicate nested bundle field was accepted")
	}
}

func mustNeoOrbConfigJSON(t *testing.T, value any) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}
