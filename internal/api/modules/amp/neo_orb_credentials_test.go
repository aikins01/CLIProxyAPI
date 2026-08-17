package amp

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/orbcredentials"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/api/handlers"
	log "github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

func TestNeoOwnerOrbCredentialStoreEncryptsAndFencesOwners(t *testing.T) {
	store := newTestNeoOwnerOrbCredentialStore(t)
	snapshot, err := orbcredentials.New("credential-sentinel-one", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	revision, err := store.put("owner-one", snapshot)
	if err != nil {
		t.Fatal(err)
	}
	ownerDirectory, err := store.ownerDirectory("owner-one")
	if err != nil {
		t.Fatal(err)
	}
	credentialPath := filepath.Join(ownerDirectory, "credentials.enc")
	raw, err := os.ReadFile(credentialPath)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte("credential-sentinel-one")) {
		t.Fatal("encrypted store contains plaintext credentials")
	}
	loaded, loadedRevision, found, revoked, err := store.load("owner-one")
	if err != nil || !found || revoked || loaded.GitHubToken != "credential-sentinel-one" || loadedRevision != revision {
		t.Fatalf("loaded=%#v revision=%q found=%t revoked=%t err=%v", loaded, loadedRevision, found, revoked, err)
	}
	sameRevision, err := store.put("owner-one", snapshot)
	if err != nil || sameRevision != revision {
		t.Fatalf("idempotent revision=%q err=%v", sameRevision, err)
	}
	idempotentRaw, err := os.ReadFile(credentialPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, idempotentRaw) {
		t.Fatal("idempotent credential write changed the ciphertext")
	}
	otherDirectory, err := store.ownerDirectory("owner-two")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(otherDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(otherDirectory, "credentials.enc"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, err := store.load("owner-two"); err == nil {
		t.Fatal("cross-owner ciphertext was accepted")
	}
	corrupt := append([]byte(nil), raw...)
	corrupt[len(corrupt)/2] ^= 1
	if err := os.WriteFile(credentialPath, corrupt, 0o600); err != nil {
		t.Fatal(err)
	}
	updated, err := orbcredentials.New("credential-sentinel-two", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.put("owner-one", updated); err == nil {
		t.Fatal("corrupt current ciphertext was overwritten")
	}
	afterCorruption, err := os.ReadFile(credentialPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(corrupt, afterCorruption) {
		t.Fatal("failed update changed corrupt current ciphertext")
	}
	corruptRevision, err := store.revision("owner-one")
	if err != nil || corruptRevision != neoOrbCredentialRepairRevision {
		t.Fatalf("corrupt revision=%q err=%v", corruptRevision, err)
	}
	repairedRevision, err := store.putRepairingCorrupt("owner-one", updated)
	if err != nil || repairedRevision == revision {
		t.Fatalf("repair revision=%q err=%v", repairedRevision, err)
	}
	repaired, loadedRepairRevision, found, revoked, err := store.load("owner-one")
	if err != nil || !found || revoked || repaired.GitHubToken != "credential-sentinel-two" || loadedRepairRevision != repairedRevision {
		t.Fatalf("repaired=%#v revision=%q found=%t revoked=%t err=%v", repaired, loadedRepairRevision, found, revoked, err)
	}
	for filePath, wantMode := range map[string]os.FileMode{
		store.rootDir:  0o700,
		ownerDirectory: 0o700,
		credentialPath: 0o600,
	} {
		info, err := os.Stat(filePath)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != wantMode {
			t.Fatalf("mode for %s = %o, want %o", filePath, info.Mode().Perm(), wantMode)
		}
	}
}

func TestNeoOwnerOrbCredentialStoreRevocationIsDurableAndCASFenced(t *testing.T) {
	store := newTestNeoOwnerOrbCredentialStore(t)
	snapshot, err := orbcredentials.New("credential-revocation-sentinel", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	activeRevision, err := store.put("owner-one", snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.revokeIfRevision("owner-one", neoOrbCredentialAbsentRevision); !errors.Is(err, errNeoOwnerOrbCredentialRevisionConflict) {
		t.Fatalf("mismatched revocation error = %v", err)
	}
	if revision, err := store.revision("owner-one"); err != nil || revision != activeRevision {
		t.Fatalf("revision after CAS mismatch = %q, %v", revision, err)
	}
	if revision, err := store.revokeIfRevision("owner-one", activeRevision); err != nil || revision != neoOrbCredentialRevokedRevision {
		t.Fatalf("revocation revision = %q, %v", revision, err)
	}
	if loaded, revision, found, revoked, err := store.load("owner-one"); err != nil || found || !revoked || revision != neoOrbCredentialRevokedRevision || loaded != (orbcredentials.Decoded{}) {
		t.Fatalf("revoked load = %#v, %q, found=%t, revoked=%t, err=%v", loaded, revision, found, revoked, err)
	}
	restarted := &neoOwnerOrbCredentialStore{rootDir: store.rootDir, aead: store.aead}
	if revision, err := restarted.revision("owner-one"); err != nil || revision != neoOrbCredentialRevokedRevision {
		t.Fatalf("restarted revision = %q, %v", revision, err)
	}
	if revision, err := restarted.revokeIfRevision("owner-one", activeRevision); err != nil || revision != neoOrbCredentialRevokedRevision {
		t.Fatalf("idempotent revocation = %q, %v", revision, err)
	}
	if revision, err := restarted.revision("owner-two"); err != nil || revision != neoOrbCredentialAbsentRevision {
		t.Fatalf("isolated owner revision = %q, %v", revision, err)
	}
	newRevision, err := restarted.put("owner-one", snapshot)
	if err != nil || !neoOrbCredentialRevisionPattern.MatchString(newRevision) || newRevision == activeRevision {
		t.Fatalf("active revision after revocation = %q, %v", newRevision, err)
	}
	if loaded, revision, found, revoked, err := restarted.load("owner-one"); err != nil || !found || revoked || revision != newRevision || loaded.GitHubToken != "credential-revocation-sentinel" {
		t.Fatalf("active load after revocation = %#v, %q, found=%t, revoked=%t, err=%v", loaded, revision, found, revoked, err)
	}
}

func TestNeoOwnerOrbCredentialStoreRevokesCorruptRecordWithRepairRevision(t *testing.T) {
	store := newTestNeoOwnerOrbCredentialStore(t)
	snapshot, err := orbcredentials.New("credential-corrupt-revocation-sentinel", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.put("owner", snapshot); err != nil {
		t.Fatal(err)
	}
	ownerDirectory, err := store.ownerDirectory("owner")
	if err != nil {
		t.Fatal(err)
	}
	credentialPath := filepath.Join(ownerDirectory, "credentials.enc")
	if err := os.WriteFile(credentialPath, []byte("{\"broken\":true}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if revision, err := store.revokeIfRevision("owner", neoOrbCredentialRepairRevision); err != nil || revision != neoOrbCredentialRevokedRevision {
		t.Fatalf("corrupt record revocation = %q, %v", revision, err)
	}
	if revision, err := store.revision("owner"); err != nil || revision != neoOrbCredentialRevokedRevision {
		t.Fatalf("revision after corrupt repair = %q, %v", revision, err)
	}
}

func TestNeoOwnerOrbCredentialStoreRejectsInvalidNonceLengthWithoutPanic(t *testing.T) {
	for _, nonce := range []string{"", base64.StdEncoding.EncodeToString([]byte{1})} {
		store := newTestNeoOwnerOrbCredentialStore(t)
		snapshot, err := orbcredentials.New("nonce-credential-sentinel", nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.put("owner", snapshot); err != nil {
			t.Fatal(err)
		}
		ownerDirectory, err := store.ownerDirectory("owner")
		if err != nil {
			t.Fatal(err)
		}
		credentialPath := filepath.Join(ownerDirectory, "credentials.enc")
		raw, err := os.ReadFile(credentialPath)
		if err != nil {
			t.Fatal(err)
		}
		var envelope neoOwnerOrbCredentialEnvelope
		if err := json.Unmarshal(raw, &envelope); err != nil {
			t.Fatal(err)
		}
		envelope.Nonce = nonce
		raw, err = json.Marshal(envelope)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(credentialPath, append(raw, '\n'), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, _, _, _, err := store.load("owner"); !errors.Is(err, errNeoStoredOrbCredentialsCorrupt) {
			t.Fatalf("nonce=%q load error=%v", nonce, err)
		}
	}
}

func TestNeoOwnerOrbCredentialStoreInitializationFailureIsLoggedWithoutSecrets(t *testing.T) {
	useTempNeoThreadStore(t)
	keyPath := filepath.Join(t.TempDir(), "credential-key-secret-path")
	if err := os.WriteFile(keyPath, []byte("invalid-key-secret-value\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(neoOrbCredentialKeyFileEnv, keyPath)
	hook := logtest.NewLocal(log.StandardLogger())
	defer hook.Reset()
	runtime := newNeoRuntime(&config.Config{})
	if runtime.orbCredentialStore != nil {
		t.Fatal("invalid credential key enabled the store")
	}
	found := false
	for _, entry := range hook.AllEntries() {
		if strings.HasPrefix(entry.Message, "amp neo owner credential store initialization failed:") {
			found = true
			if !strings.Contains(entry.Message, "owner credential key file is invalid") {
				t.Fatalf("credential initialization log omitted the safe failure reason: %s", entry.Message)
			}
		}
		if strings.Contains(entry.Message, keyPath) || strings.Contains(entry.Message, "invalid-key-secret-value") {
			t.Fatalf("credential initialization log exposed details: %s", entry.Message)
		}
	}
	if !found {
		t.Fatal("credential store initialization failure was not logged")
	}
}

func TestLocalBrokerOrbCredentialEndpointReportsDisabledOrInvalidKeyFileSafely(t *testing.T) {
	useTempNeoThreadStore(t)
	keyPath := filepath.Join(t.TempDir(), "credential-key-secret-path")
	keyValue := "invalid-key-secret-value"
	if err := os.WriteFile(keyPath, []byte(keyValue+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(neoOrbCredentialKeyFileEnv, keyPath)
	runtime := newNeoRuntime(&config.Config{})
	if runtime.store == nil || runtime.orbCredentialStore != nil {
		t.Fatalf("runtime store=%p credential store=%p", runtime.store, runtime.orbCredentialStore)
	}
	gin.SetMode(gin.TestMode)
	router := gin.New()
	module := &AmpModule{neoRuntime: runtime}
	router.PUT(orbcredentials.EndpointPath, module.serveLocalBrokerOrbCredentials)
	request := httptest.NewRequest(http.MethodPut, orbcredentials.EndpointPath, strings.NewReader(`{}`))
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable || !strings.Contains(response.Body.String(), "orb credential key-file configuration is disabled or invalid") {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if strings.Contains(response.Body.String(), keyPath) || strings.Contains(response.Body.String(), keyValue) {
		t.Fatalf("credential store response exposed configuration details: %s", response.Body.String())
	}
}

func TestNeoOwnerOrbCredentialDecodersReturnSafeFieldDiagnostics(t *testing.T) {
	secret := "credential-diagnostic-secret"
	snapshot, err := orbcredentials.New(secret, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	uploads := []struct {
		request neoOwnerOrbCredentialUploadRequest
		want    string
	}{
		{request: neoOwnerOrbCredentialUploadRequest{BrokerID: "!", SessionID: "session", SessionGeneration: 1, Credentials: snapshot}, want: "brokerId is invalid"},
		{request: neoOwnerOrbCredentialUploadRequest{BrokerID: "broker", SessionID: "!", SessionGeneration: 1, Credentials: snapshot}, want: "sessionId is invalid"},
		{request: neoOwnerOrbCredentialUploadRequest{BrokerID: "broker", SessionID: "session", Credentials: snapshot}, want: "sessionGeneration is invalid"},
		{request: neoOwnerOrbCredentialUploadRequest{BrokerID: "broker", SessionID: "session", SessionGeneration: 1, Credentials: orbcredentials.Snapshot{Schema: 2, GitHub: snapshot.GitHub}}, want: "credential snapshot is invalid"},
	}
	for _, test := range uploads {
		raw, marshalErr := json.Marshal(test.request)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		_, err := decodeNeoOwnerOrbCredentialUpload(raw)
		if err == nil || err.Error() != test.want || strings.Contains(err.Error(), secret) {
			t.Fatalf("upload error=%v, want %q", err, test.want)
		}
	}
	clears := []struct {
		request neoOwnerOrbCredentialClearRequest
		want    string
	}{
		{request: neoOwnerOrbCredentialClearRequest{BrokerID: "!", SessionID: "session", SessionGeneration: 1, ExpectedRevision: neoOrbCredentialAbsentRevision}, want: "brokerId is invalid"},
		{request: neoOwnerOrbCredentialClearRequest{BrokerID: "broker", SessionID: "!", SessionGeneration: 1, ExpectedRevision: neoOrbCredentialAbsentRevision}, want: "sessionId is invalid"},
		{request: neoOwnerOrbCredentialClearRequest{BrokerID: "broker", SessionID: "session", ExpectedRevision: neoOrbCredentialAbsentRevision}, want: "sessionGeneration is invalid"},
		{request: neoOwnerOrbCredentialClearRequest{BrokerID: "broker", SessionID: "session", SessionGeneration: 1, ExpectedRevision: "invalid"}, want: "expectedRevision is invalid"},
	}
	for _, test := range clears {
		raw, marshalErr := json.Marshal(test.request)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		_, err := decodeNeoOwnerOrbCredentialClear(raw)
		if err == nil || err.Error() != test.want {
			t.Fatalf("clear error=%v, want %q", err, test.want)
		}
	}
}

func TestLocalBrokerOrbCredentialEndpointUsesAuthenticatedOwnerAndFence(t *testing.T) {
	useTempNeoThreadStore(t)
	writeTestNeoOwnerOrbCredentialKey(t)
	gin.SetMode(gin.TestMode)
	upstream := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		writeNeoJSON(response, http.StatusOK, map[string]any{"ok": true, "result": map[string]any{"id": "user_credential_owner"}})
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
	ownerDirectory, err := runtime.orbCredentialStore.ownerDirectory("user_credential_owner")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(ownerDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ownerDirectory, "credentials.enc"), []byte("{\"broken\":true}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
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
	heartbeatBody := `{"brokerId":"broker","sessionId":"session","sessionGeneration":3,"hostname":"Mac","pid":123,"runners":[]}`
	heartbeat := requestNeoOrbConfigEndpoint(router, http.MethodPost, heartbeatEndpointPathForTest, "client", "application/json", "", heartbeatBody)
	if heartbeat.Code != http.StatusOK || heartbeat.Header().Get(orbcredentials.SupportHeader) != "1" || heartbeat.Header().Get(neoOrbCredentialRevisionHeader) != neoOrbCredentialRepairRevision {
		t.Fatalf("heartbeat status=%d headers=%v body=%s", heartbeat.Code, heartbeat.Header(), heartbeat.Body.String())
	}
	snapshot, err := orbcredentials.New("credential-endpoint-sentinel", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	uploadRaw, err := json.Marshal(neoOwnerOrbCredentialUploadRequest{
		BrokerID:          "broker",
		SessionID:         "session",
		SessionGeneration: 3,
		Credentials:       snapshot,
	})
	if err != nil {
		t.Fatal(err)
	}
	accepted := requestNeoOrbConfigEndpoint(router, http.MethodPut, orbcredentials.EndpointPath, "client", "application/json", "", string(uploadRaw))
	acceptedRevision := accepted.Header().Get(neoOrbCredentialRevisionHeader)
	if accepted.Code != http.StatusOK || accepted.Body.String() != `{"ok":true}` || !neoOrbCredentialRevisionPattern.MatchString(acceptedRevision) || strings.Contains(accepted.Body.String(), "credential-endpoint-sentinel") {
		t.Fatalf("upload status=%d body=%s", accepted.Code, accepted.Body.String())
	}
	loaded, _, found, revoked, err := runtime.ownerOrbCredentials("user_credential_owner")
	if err != nil || !found || revoked || loaded.GitHubToken != "credential-endpoint-sentinel" {
		t.Fatalf("loaded=%#v found=%t revoked=%t err=%v", loaded, found, revoked, err)
	}
	emptyRaw, err := json.Marshal(neoOwnerOrbCredentialUploadRequest{
		BrokerID:          "broker",
		SessionID:         "session",
		SessionGeneration: 3,
		Credentials:       orbcredentials.Snapshot{Schema: orbcredentials.Schema},
	})
	if err != nil {
		t.Fatal(err)
	}
	empty := requestNeoOrbConfigEndpoint(router, http.MethodPut, orbcredentials.EndpointPath, "client", "application/json", "", string(emptyRaw))
	if empty.Code != http.StatusBadRequest || !strings.Contains(empty.Body.String(), "credential snapshot is invalid") || strings.Contains(empty.Body.String(), "credential-endpoint-sentinel") {
		t.Fatalf("empty upload status=%d body=%s", empty.Code, empty.Body.String())
	}
	unsafeBrokerRaw := strings.Replace(string(uploadRaw), `"brokerId":"broker"`, `"brokerId":"!"`, 1)
	unsafeBroker := requestNeoOrbConfigEndpoint(router, http.MethodPut, orbcredentials.EndpointPath, "client", "application/json", "", unsafeBrokerRaw)
	if unsafeBroker.Code != http.StatusBadRequest || !strings.Contains(unsafeBroker.Body.String(), "brokerId is invalid") || strings.Contains(unsafeBroker.Body.String(), "credential-endpoint-sentinel") {
		t.Fatalf("unsafe broker status=%d body=%s", unsafeBroker.Code, unsafeBroker.Body.String())
	}
	loaded, retainedRevision, found, revoked, err := runtime.ownerOrbCredentials("user_credential_owner")
	if err != nil || !found || revoked || loaded.GitHubToken != "credential-endpoint-sentinel" || retainedRevision != acceptedRevision {
		t.Fatalf("retained=%#v revision=%q found=%t revoked=%t err=%v", loaded, retainedRevision, found, revoked, err)
	}
	reconciledHeartbeat := requestNeoOrbConfigEndpoint(router, http.MethodPost, heartbeatEndpointPathForTest, "client", "application/json", "", heartbeatBody)
	if reconciledHeartbeat.Code != http.StatusOK || reconciledHeartbeat.Header().Get(neoOrbCredentialRevisionHeader) != acceptedRevision {
		t.Fatalf("reconciled heartbeat status=%d headers=%v body=%s", reconciledHeartbeat.Code, reconciledHeartbeat.Header(), reconciledHeartbeat.Body.String())
	}
	clearRaw, err := json.Marshal(neoOwnerOrbCredentialClearRequest{
		BrokerID:          "broker",
		SessionID:         "session",
		SessionGeneration: 3,
		ExpectedRevision:  acceptedRevision,
	})
	if err != nil {
		t.Fatal(err)
	}
	cleared := requestNeoOrbConfigEndpoint(router, http.MethodDelete, orbcredentials.EndpointPath, "client", "application/json", "", string(clearRaw))
	if cleared.Code != http.StatusOK || cleared.Header().Get(neoOrbCredentialRevisionHeader) != neoOrbCredentialRevokedRevision || cleared.Body.String() != `{"ok":true}` {
		t.Fatalf("clear status=%d headers=%v body=%s", cleared.Code, cleared.Header(), cleared.Body.String())
	}
	if loaded, revision, found, revoked, err := runtime.ownerOrbCredentials("user_credential_owner"); err != nil || found || !revoked || revision != neoOrbCredentialRevokedRevision || loaded != (orbcredentials.Decoded{}) {
		t.Fatalf("cleared load=%#v revision=%q found=%t revoked=%t err=%v", loaded, revision, found, revoked, err)
	}
	revokedHeartbeat := requestNeoOrbConfigEndpoint(router, http.MethodPost, heartbeatEndpointPathForTest, "client", "application/json", "", heartbeatBody)
	if revokedHeartbeat.Code != http.StatusOK || revokedHeartbeat.Header().Get(neoOrbCredentialRevisionHeader) != neoOrbCredentialRevokedRevision {
		t.Fatalf("revoked heartbeat status=%d headers=%v body=%s", revokedHeartbeat.Code, revokedHeartbeat.Header(), revokedHeartbeat.Body.String())
	}
	staleClearRaw := strings.Replace(string(clearRaw), `"sessionGeneration":3`, `"sessionGeneration":2`, 1)
	staleClear := requestNeoOrbConfigEndpoint(router, http.MethodDelete, orbcredentials.EndpointPath, "client", "application/json", "", staleClearRaw)
	if staleClear.Code != http.StatusConflict || !strings.Contains(staleClear.Body.String(), "stale_session") {
		t.Fatalf("stale clear status=%d body=%s", staleClear.Code, staleClear.Body.String())
	}
	unsafeClearRaw := strings.TrimSuffix(string(clearRaw), "}") + `,"ownerUserID":"other"}`
	unsafeClear := requestNeoOrbConfigEndpoint(router, http.MethodDelete, orbcredentials.EndpointPath, "client", "application/json", "", unsafeClearRaw)
	if unsafeClear.Code != http.StatusBadRequest {
		t.Fatalf("unsafe clear status=%d body=%s", unsafeClear.Code, unsafeClear.Body.String())
	}
	restored := requestNeoOrbConfigEndpoint(router, http.MethodPut, orbcredentials.EndpointPath, "client", "application/json", "", string(uploadRaw))
	restoredRevision := restored.Header().Get(neoOrbCredentialRevisionHeader)
	if restored.Code != http.StatusOK || !neoOrbCredentialRevisionPattern.MatchString(restoredRevision) || restoredRevision == acceptedRevision {
		t.Fatalf("restored status=%d revision=%q body=%s", restored.Code, restoredRevision, restored.Body.String())
	}
	conflict := requestNeoOrbConfigEndpoint(router, http.MethodDelete, orbcredentials.EndpointPath, "client", "application/json", "", string(clearRaw))
	if conflict.Code != http.StatusConflict || !strings.Contains(conflict.Body.String(), "revision_conflict") {
		t.Fatalf("conflict status=%d body=%s", conflict.Code, conflict.Body.String())
	}
	if loaded, revision, found, revoked, err := runtime.ownerOrbCredentials("user_credential_owner"); err != nil || !found || revoked || revision != restoredRevision || loaded.GitHubToken != "credential-endpoint-sentinel" {
		t.Fatalf("retained after conflict=%#v revision=%q found=%t revoked=%t err=%v", loaded, revision, found, revoked, err)
	}
	staleRaw := strings.Replace(string(uploadRaw), `"sessionGeneration":3`, `"sessionGeneration":2`, 1)
	stale := requestNeoOrbConfigEndpoint(router, http.MethodPut, orbcredentials.EndpointPath, "client", "application/json", "", staleRaw)
	if stale.Code != http.StatusConflict || strings.Contains(stale.Body.String(), "credential-endpoint-sentinel") {
		t.Fatalf("stale status=%d body=%s", stale.Code, stale.Body.String())
	}
	unsafe := strings.TrimSuffix(string(uploadRaw), "}") + `,"ownerUserID":"other"}`
	rejected := requestNeoOrbConfigEndpoint(router, http.MethodPut, orbcredentials.EndpointPath, "client", "application/json", "", unsafe)
	if rejected.Code != http.StatusBadRequest || strings.Contains(rejected.Body.String(), "credential-endpoint-sentinel") {
		t.Fatalf("unsafe status=%d body=%s", rejected.Code, rejected.Body.String())
	}
}

func TestNeoOrbOwnerCredentialReconcilePrecedesConfigAndSuppressesSharedGitHubEnv(t *testing.T) {
	store := newTestNeoOwnerOrbCredentialStore(t)
	snapshot, err := orbcredentials.New("credential-reconcile-sentinel", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.put("owner", snapshot); err != nil {
		t.Fatal(err)
	}
	runtime := &neoRuntime{orbCredentialStore: store}
	manager := &neoOrbManager{runtime: runtime}
	fake := &neoOrbFakeProvider{execHandler: func(cmd []string) neoOrbExecResult {
		if len(cmd) == 2 && cmd[0] == "cat" && cmd[1] == neoOrbOwnerCredentialRevisionPath {
			return neoOrbExecResult{ExitCode: 1}
		}
		return neoOrbExecResult{ExitCode: 0}
	}}
	selection, err := manager.orbReconcileOwnerCredentials(t.Context(), fake, "container", "owner")
	if err != nil {
		t.Fatal(err)
	}
	if !selection.Found || selection.Revoked || !selection.HasGitHub || selection.HasUsableSSH || !selection.suppressSharedGitHub() {
		t.Fatalf("selection = %#v", selection)
	}
	for _, call := range fake.calls {
		if strings.Contains(call, "credential-reconcile-sentinel") {
			t.Fatalf("credential leaked into provider call record: %s", call)
		}
	}
	if fake.hasSensitiveCommandArg() {
		t.Fatalf("owner credential reached provider command args: %#v", fake.calls)
	}
	calls := strings.Join(fake.calls, "\n")
	if strings.Contains(calls, "mv /root/.ssh") || strings.Contains(calls, "copy-tar:"+neoOrbOwnerSSHStagePath) {
		t.Fatalf("GitHub-only snapshot replaced the SSH credential root:\n%s", calls)
	}
	if !strings.Contains(calls, "mv "+neoOrbOwnerGHStagePath+" /root/.config/gh") {
		t.Fatalf("GitHub-only snapshot did not replace the GitHub credential root:\n%s", calls)
	}
	if !strings.Contains(calls, "copy:"+neoOrbOwnerGHStagePath+"/"+neoOrbOwnerChannelRevisionFile+":33:384") {
		t.Fatalf("GitHub-only snapshot did not stage its ownership marker:\n%s", calls)
	}
	if _, retained := fake.copiedFiles[neoOrbOwnerGHStagePath+"/hosts.yml"]; retained {
		t.Fatal("staged GitHub credential body retained by fake provider")
	}
	cfg := &config.Config{AmpCode: config.AmpCode{Orbs: config.AmpOrbs{Env: map[string]string{
		"GH_TOKEN":     "shared-gh",
		"GITHUB_TOKEN": "shared-github",
		"EDITOR":       "vim",
	}}}}
	env := strings.Join(neoOrbExecutorEnvWithCredentials(cfg, "thread", "/work", "portal", true), "\n")
	if strings.Contains(env, "shared-gh") || strings.Contains(env, "shared-github") || !strings.Contains(env, "EDITOR=vim") {
		t.Fatalf("credential-aware executor env = %s", env)
	}
}

func TestNeoOrbOwnerCredentialStagingFailureCleansSensitiveArtifacts(t *testing.T) {
	store := newTestNeoOwnerOrbCredentialStore(t)
	snapshot, err := orbcredentials.New("credential-staging-sentinel", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.put("owner", snapshot); err != nil {
		t.Fatal(err)
	}
	fake := &neoOrbFakeProvider{
		copyFileErrorPath: neoOrbOwnerGHStagePath + "/" + neoOrbOwnerChannelRevisionFile,
		copyFileError:     errors.New("provider-copy-secret"),
		execHandler: func(cmd []string) neoOrbExecResult {
			if len(cmd) == 2 && cmd[0] == "cat" {
				return neoOrbExecResult{ExitCode: 1}
			}
			return neoOrbExecResult{}
		},
	}
	manager := &neoOrbManager{runtime: &neoRuntime{orbCredentialStore: store}}
	_, err = manager.orbReconcileOwnerCredentials(t.Context(), fake, "container", "owner")
	if err == nil || strings.Contains(err.Error(), "provider-copy-secret") {
		t.Fatalf("staging failure = %v", err)
	}
	copyIndex := fake.callIndex("copy:" + neoOrbOwnerGHStagePath + "/" + neoOrbOwnerChannelRevisionFile)
	cleanupIndex := fake.callIndex("exec:rm -rf " + neoOrbOwnerSSHStagePath + " " + neoOrbOwnerGHStagePath)
	if copyIndex < 0 || cleanupIndex <= copyIndex {
		t.Fatalf("staging cleanup order = %#v", fake.calls)
	}
	if strings.Contains(strings.Join(fake.calls, "\n"), "mv "+neoOrbOwnerGHStagePath+" /root/.config/gh") {
		t.Fatalf("failed staging reached replacement: %#v", fake.calls)
	}
}

func TestNeoOrbOwnerCredentialSSHOnlySnapshotPreservesGitHubRoot(t *testing.T) {
	snapshot := newTestNeoOwnerSSHCredentialSnapshot(t)
	store := newTestNeoOwnerOrbCredentialStore(t)
	if _, err := store.put("owner", snapshot); err != nil {
		t.Fatal(err)
	}
	manager := &neoOrbManager{runtime: &neoRuntime{orbCredentialStore: store}}
	for _, test := range []struct {
		name       string
		marker     string
		markerExit int
	}{
		{name: "missing marker", markerExit: 1},
		{name: "malformed marker", marker: "not-a-managed-revision"},
	} {
		t.Run(test.name, func(t *testing.T) {
			fake := &neoOrbFakeProvider{execHandler: func(cmd []string) neoOrbExecResult {
				if len(cmd) == 2 && cmd[0] == "cat" {
					switch cmd[1] {
					case neoOrbOwnerCredentialRevisionPath:
						return neoOrbExecResult{ExitCode: 1}
					case neoOrbOwnerGHRevisionPath:
						return neoOrbExecResult{ExitCode: test.markerExit, Stdout: test.marker}
					case neoOrbOwnerSSHRevisionPath:
						return neoOrbExecResult{ExitCode: 1}
					}
				}
				return neoOrbExecResult{ExitCode: 0}
			}}
			selection, err := manager.orbReconcileOwnerCredentials(t.Context(), fake, "container", "owner")
			if err != nil {
				t.Fatal(err)
			}
			if !selection.Found || selection.Revoked || selection.HasGitHub || !selection.HasUsableSSH || !selection.suppressSharedGitHub() {
				t.Fatalf("selection = %#v", selection)
			}
			calls := strings.Join(fake.calls, "\n")
			if !strings.Contains(calls, "mv "+neoOrbOwnerSSHStagePath+" /root/.ssh") {
				t.Fatalf("SSH-only snapshot did not replace the SSH credential root:\n%s", calls)
			}
			if strings.Contains(calls, "mv /root/.config/gh") || strings.Contains(calls, "mv "+neoOrbOwnerGHStagePath) || strings.Contains(calls, "git config --global") {
				t.Fatalf("SSH-only snapshot mutated unmanaged GitHub credential state:\n%s", calls)
			}
		})
	}
}

func TestNeoOrbOwnerCredentialGitHubOnlySnapshotPreservesUnmanagedSSHRoot(t *testing.T) {
	snapshot, err := orbcredentials.New("credential-reconcile-sentinel", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	store := newTestNeoOwnerOrbCredentialStore(t)
	if _, err := store.put("owner", snapshot); err != nil {
		t.Fatal(err)
	}
	manager := &neoOrbManager{runtime: &neoRuntime{orbCredentialStore: store}}
	for _, test := range []struct {
		name       string
		marker     string
		markerExit int
	}{
		{name: "missing marker", markerExit: 1},
		{name: "malformed marker", marker: "not-a-managed-revision"},
	} {
		t.Run(test.name, func(t *testing.T) {
			fake := &neoOrbFakeProvider{execHandler: func(cmd []string) neoOrbExecResult {
				if len(cmd) == 2 && cmd[0] == "cat" {
					switch cmd[1] {
					case neoOrbOwnerCredentialRevisionPath, neoOrbOwnerGHRevisionPath:
						return neoOrbExecResult{ExitCode: 1}
					case neoOrbOwnerSSHRevisionPath:
						return neoOrbExecResult{ExitCode: test.markerExit, Stdout: test.marker}
					}
				}
				return neoOrbExecResult{ExitCode: 0}
			}}
			if _, err := manager.orbReconcileOwnerCredentials(t.Context(), fake, "container", "owner"); err != nil {
				t.Fatal(err)
			}
			calls := strings.Join(fake.calls, "\n")
			if !strings.Contains(calls, "mv "+neoOrbOwnerGHStagePath+" /root/.config/gh") {
				t.Fatalf("GitHub-only snapshot did not replace the GitHub credential root:\n%s", calls)
			}
			if strings.Contains(calls, "mv /root/.ssh "+neoOrbOwnerSSHBackupPath) || strings.Contains(calls, "mv "+neoOrbOwnerSSHStagePath+" /root/.ssh") {
				t.Fatalf("GitHub-only snapshot mutated unmanaged SSH credential state:\n%s", calls)
			}
		})
	}
}

func TestNeoOrbOwnerCredentialGitHubOnlySnapshotRemovesManagedSSHRoot(t *testing.T) {
	snapshot, err := orbcredentials.New("credential-reconcile-sentinel", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	store := newTestNeoOwnerOrbCredentialStore(t)
	if _, err := store.put("owner", snapshot); err != nil {
		t.Fatal(err)
	}
	managedRevision := strings.Repeat("a", 32)
	fake := &neoOrbFakeProvider{execHandler: func(cmd []string) neoOrbExecResult {
		if len(cmd) == 2 && cmd[0] == "cat" {
			switch cmd[1] {
			case neoOrbOwnerCredentialRevisionPath, neoOrbOwnerGHRevisionPath:
				return neoOrbExecResult{ExitCode: 1}
			case neoOrbOwnerSSHRevisionPath:
				return neoOrbExecResult{ExitCode: 0, Stdout: managedRevision}
			}
		}
		return neoOrbExecResult{ExitCode: 0}
	}}
	manager := &neoOrbManager{runtime: &neoRuntime{orbCredentialStore: store}}
	if _, err := manager.orbReconcileOwnerCredentials(t.Context(), fake, "container", "owner"); err != nil {
		t.Fatal(err)
	}
	calls := strings.Join(fake.calls, "\n")
	if !strings.Contains(calls, "mv /root/.ssh "+neoOrbOwnerSSHBackupPath) || strings.Contains(calls, "mv "+neoOrbOwnerSSHStagePath+" /root/.ssh") {
		t.Fatalf("GitHub-only transition did not remove marker-managed SSH only:\n%s", calls)
	}
	if !strings.Contains(calls, "mv "+neoOrbOwnerGHStagePath+" /root/.config/gh") {
		t.Fatalf("GitHub-only transition did not install GitHub credentials:\n%s", calls)
	}
}

func TestNeoOrbOwnerCredentialSSHOnlySnapshotRemovesManagedGitHubAndScrubsGitConfig(t *testing.T) {
	store := newTestNeoOwnerOrbCredentialStore(t)
	if _, err := store.put("owner", newTestNeoOwnerSSHCredentialSnapshot(t)); err != nil {
		t.Fatal(err)
	}
	managedRevision := strings.Repeat("b", 32)
	fake := &neoOrbFakeProvider{execHandler: func(cmd []string) neoOrbExecResult {
		if len(cmd) == 2 && cmd[0] == "cat" {
			switch cmd[1] {
			case neoOrbOwnerCredentialRevisionPath, neoOrbOwnerSSHRevisionPath:
				return neoOrbExecResult{ExitCode: 1}
			case neoOrbOwnerGHRevisionPath:
				return neoOrbExecResult{ExitCode: 0, Stdout: managedRevision}
			}
		}
		return neoOrbExecResult{ExitCode: 0}
	}}
	manager := &neoOrbManager{runtime: &neoRuntime{orbCredentialStore: store}}
	if _, err := manager.orbReconcileOwnerCredentials(t.Context(), fake, "container", "owner"); err != nil {
		t.Fatal(err)
	}
	calls := strings.Join(fake.calls, "\n")
	if !strings.Contains(calls, "mv /root/.config/gh "+neoOrbOwnerGHBackupPath) || strings.Contains(calls, "mv "+neoOrbOwnerGHStagePath+" /root/.config/gh") {
		t.Fatalf("SSH-only transition did not remove marker-managed GitHub only:\n%s", calls)
	}
	for _, expected := range []string{
		"cp -a /root/.gitconfig " + neoOrbOwnerGitConfigBackupPath,
		"git config --global --unset-all url.https://github.com/.insteadOf",
		"mv " + neoOrbOwnerSSHStagePath + " /root/.ssh",
	} {
		if !strings.Contains(calls, expected) {
			t.Fatalf("SSH-only transition omitted %q:\n%s", expected, calls)
		}
	}
}

func TestNeoOrbOwnerCredentialExactGenericMarkerDoesNotHideManagedOmittedChannel(t *testing.T) {
	snapshot, err := orbcredentials.New("credential-reconcile-sentinel", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	store := newTestNeoOwnerOrbCredentialStore(t)
	revision, err := store.put("owner", snapshot)
	if err != nil {
		t.Fatal(err)
	}
	staleRevision := strings.Repeat("c", 32)
	if staleRevision == revision {
		staleRevision = strings.Repeat("d", 32)
	}
	fake := &neoOrbFakeProvider{execHandler: func(cmd []string) neoOrbExecResult {
		if len(cmd) == 2 && cmd[0] == "cat" {
			switch cmd[1] {
			case neoOrbOwnerCredentialRevisionPath, neoOrbOwnerGHRevisionPath:
				return neoOrbExecResult{ExitCode: 0, Stdout: revision}
			case neoOrbOwnerSSHRevisionPath:
				return neoOrbExecResult{ExitCode: 0, Stdout: staleRevision}
			}
		}
		return neoOrbExecResult{ExitCode: 0}
	}}
	manager := &neoOrbManager{runtime: &neoRuntime{orbCredentialStore: store}}
	if _, err := manager.orbReconcileOwnerCredentials(t.Context(), fake, "container", "owner"); err != nil {
		t.Fatal(err)
	}
	calls := strings.Join(fake.calls, "\n")
	if !strings.Contains(calls, "mv /root/.ssh "+neoOrbOwnerSSHBackupPath) {
		t.Fatalf("exact generic marker bypassed stale omitted SSH reconciliation:\n%s", calls)
	}
	if strings.Contains(calls, "mv "+neoOrbOwnerGHStagePath+" /root/.config/gh") {
		t.Fatalf("exact GitHub channel was unnecessarily replaced:\n%s", calls)
	}
	for _, markerPath := range []string{neoOrbOwnerSSHRevisionPath, neoOrbOwnerGHRevisionPath} {
		if !strings.Contains(calls, "cat "+markerPath) {
			t.Fatalf("fast-path check omitted %s:\n%s", markerPath, calls)
		}
	}
}

func TestNeoOrbOwnerCredentialReplacementRestoresAllAffectedRootsOnInstallFailure(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("test executes a POSIX shell script")
	}
	orbRoot := filepath.Join(t.TempDir(), "root")
	for path, content := range map[string]string{
		filepath.Join(orbRoot, ".ssh", "old-ssh"):               "old ssh",
		filepath.Join(orbRoot, ".config", "gh", "old-github"):   "old github",
		filepath.Join(orbRoot, ".gitconfig"):                    "old gitconfig",
		filepath.Join(orbRoot, ".cliproxyapi-ssh-stage", "new"): "new ssh",
	} {
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	script := neoOrbOwnerCredentialReplacementScript(neoOrbCredentialSelection{HasGitHub: true}, true, true, true, true, "", "")
	command := exec.Command("sh", "-c", strings.ReplaceAll(script, "/root", orbRoot))
	if output, err := command.CombinedOutput(); err == nil {
		t.Fatalf("replacement unexpectedly succeeded: %s", output)
	}
	for path, content := range map[string]string{
		filepath.Join(orbRoot, ".ssh", "old-ssh"):             "old ssh",
		filepath.Join(orbRoot, ".config", "gh", "old-github"): "old github",
		filepath.Join(orbRoot, ".gitconfig"):                  "old gitconfig",
	} {
		got, err := os.ReadFile(path)
		if err != nil || string(got) != content {
			t.Fatalf("restored %s = %q, %v", path, got, err)
		}
	}
	if _, err := os.Stat(filepath.Join(orbRoot, ".ssh", "new")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed replacement retained staged SSH root: %v", err)
	}
}

func TestNeoOrbOwnerCredentialReplacementRejectsChannelMarkerDriftBeforeMutation(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("test executes a POSIX shell script")
	}
	expectedRevision := strings.Repeat("a", 32)
	for _, test := range []struct {
		name                   string
		root                   string
		backup                 string
		removeSSH              bool
		removeGitHub           bool
		expectedSSHRevision    string
		expectedGitHubRevision string
	}{
		{name: "SSH", root: ".ssh", backup: ".cliproxyapi-ssh-backup", removeSSH: true, expectedSSHRevision: expectedRevision},
		{name: "GitHub", root: filepath.Join(".config", "gh"), backup: ".cliproxyapi-gh-backup", removeGitHub: true, expectedGitHubRevision: expectedRevision},
	} {
		t.Run(test.name, func(t *testing.T) {
			orbRoot := filepath.Join(t.TempDir(), "root")
			credentialRoot := filepath.Join(orbRoot, test.root)
			if err := os.MkdirAll(credentialRoot, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(credentialRoot, neoOrbOwnerChannelRevisionFile), []byte(strings.Repeat("b", 32)+"\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(credentialRoot, "retained"), []byte("retained"), 0o600); err != nil {
				t.Fatal(err)
			}
			script := neoOrbOwnerCredentialReplacementScript(neoOrbCredentialSelection{}, false, false, test.removeSSH, test.removeGitHub, test.expectedSSHRevision, test.expectedGitHubRevision)
			command := exec.Command("sh", "-c", strings.ReplaceAll(script, "/root", orbRoot))
			if output, err := command.CombinedOutput(); err == nil {
				t.Fatalf("marker drift replacement succeeded: %s", output)
			}
			if content, err := os.ReadFile(filepath.Join(credentialRoot, "retained")); err != nil || string(content) != "retained" {
				t.Fatalf("credential root changed after marker drift: %q, %v", content, err)
			}
			if _, err := os.Stat(filepath.Join(orbRoot, test.backup)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("marker drift created backup: %v", err)
			}
		})
	}
}

func TestNeoOrbOwnerCredentialManagedGitHubRemovalScrubsGitConfig(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("test executes a POSIX shell script")
	}
	orbRoot := filepath.Join(t.TempDir(), "root")
	binDir := filepath.Join(t.TempDir(), "bin")
	for path, content := range map[string]string{
		filepath.Join(orbRoot, ".ssh", "old"):                   "old ssh",
		filepath.Join(orbRoot, ".config", "gh", "hosts.yml"):    "old github",
		filepath.Join(orbRoot, ".cliproxyapi-ssh-stage", "new"): "new ssh",
		filepath.Join(orbRoot, ".gitconfig"):                    "[user]\n\tname = owner\n[url \"https://x-access-token:old@github.com/\"]\n\tinsteadOf = https://github.com/\n[url \"https://x-access-token:keep@github-com/\"]\n\tinsteadOf = https://github-com/\n[url \"https://github.com/\"]\n\tinsteadOf = git@github.com:\n",
		filepath.Join(binDir, "git"): `#!/bin/sh
set -eu
test "$*" = "config --global --unset-all url.https://github.com/.insteadOf"
temp="$(mktemp)"
awk '
  /^\[url "https:\/\/github.com\/"\]$/ { skip=1; next }
  /^\[/ { skip=0 }
  !skip { print }
' "$HOME/.gitconfig" > "$temp"
mv "$temp" "$HOME/.gitconfig"`,
	} {
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	script := neoOrbOwnerCredentialReplacementScript(neoOrbCredentialSelection{HasUsableSSH: true}, true, false, true, true, "", "")
	command := exec.Command("sh", "-c", strings.ReplaceAll(script, "/root", orbRoot))
	command.Env = []string{"PATH=" + binDir + ":/usr/bin:/bin"}
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("run managed GitHub removal: %v: %s", err, output)
	}
	if _, err := os.Stat(filepath.Join(orbRoot, ".config", "gh")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("managed GitHub root was retained: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(orbRoot, ".ssh", "new")); err != nil || string(got) != "new ssh" {
		t.Fatalf("desired SSH root = %q, %v", got, err)
	}
	gitConfig, err := os.ReadFile(filepath.Join(orbRoot, ".gitconfig"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(gitConfig), "old@github.com") || strings.Contains(string(gitConfig), "[url \"https://github.com/\"]") || !strings.Contains(string(gitConfig), "keep@github-com") || !strings.Contains(string(gitConfig), "name = owner") {
		t.Fatalf("GitHub gitconfig scrub result = %s", gitConfig)
	}
}

func TestNeoOrbOwnerCredentialSSHSelectionRequiresGitHubKnownHost(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	privateBlock, err := ssh.MarshalPrivateKey(privateKey, "")
	if err != nil {
		t.Fatal(err)
	}
	sshPublicKey, err := ssh.NewPublicKey(publicKey)
	if err != nil {
		t.Fatal(err)
	}
	publicLine := strings.TrimSpace(string(ssh.MarshalAuthorizedKey(sshPublicKey)))
	for _, test := range []struct {
		name       string
		knownHosts []byte
		wantUsable bool
	}{
		{name: "missing"},
		{name: "unrelated host", knownHosts: []byte("gitlab.com " + publicLine + "\n")},
		{name: "GitHub", knownHosts: []byte("github.com " + publicLine + "\n"), wantUsable: true},
		{name: "hashed GitHub", knownHosts: []byte(knownhosts.HashHostname("github.com") + " " + publicLine + "\n"), wantUsable: true},
		{name: "GitHub certificate authority", knownHosts: []byte("@cert-authority github.com " + publicLine + "\n")},
	} {
		t.Run(test.name, func(t *testing.T) {
			snapshot, err := orbcredentials.New("credential-reconcile-sentinel", []orbcredentials.DecodedSSHIdentity{{
				Name:       "id_ed25519",
				PrivateKey: pem.EncodeToMemory(privateBlock),
				PublicKey:  ssh.MarshalAuthorizedKey(sshPublicKey),
			}}, test.knownHosts)
			if err != nil {
				t.Fatal(err)
			}
			store := newTestNeoOwnerOrbCredentialStore(t)
			if _, err := store.put("owner", snapshot); err != nil {
				t.Fatal(err)
			}
			manager := &neoOrbManager{runtime: &neoRuntime{orbCredentialStore: store}}
			fake := &neoOrbFakeProvider{execHandler: func(cmd []string) neoOrbExecResult {
				if len(cmd) == 2 && cmd[0] == "cat" && cmd[1] == neoOrbOwnerCredentialRevisionPath {
					return neoOrbExecResult{ExitCode: 1}
				}
				return neoOrbExecResult{ExitCode: 0}
			}}
			selection, err := manager.orbReconcileOwnerCredentials(t.Context(), fake, "container", "owner")
			if err != nil {
				t.Fatal(err)
			}
			if !selection.Found || selection.Revoked || !selection.HasGitHub || selection.HasUsableSSH != test.wantUsable || !selection.suppressSharedGitHub() {
				t.Fatalf("selection = %#v", selection)
			}
			calls := strings.Join(fake.calls, "\n")
			wantStage := "copy-tar:" + neoOrbOwnerSSHStagePath + ":" + neoOrbOwnerChannelRevisionFile + ",config,id_ed25519,id_ed25519.pub"
			if len(test.knownHosts) > 0 {
				wantStage += ",known_hosts"
			}
			if !strings.Contains(calls, wantStage) {
				t.Fatalf("SSH snapshot was not staged as %q:\n%s", wantStage, calls)
			}
			fallback := strings.Contains(calls, "git config --global --add url.https://github.com/.insteadOf git@github.com:")
			if fallback == test.wantUsable {
				t.Fatalf("GitHub HTTPS fallback presence = %t, want %t:\n%s", fallback, !test.wantUsable, calls)
			}
		})
	}
}

func TestNeoOrbOwnerCredentialRevocationCleansOnlyChannelManagedRoots(t *testing.T) {
	for _, test := range []struct {
		name              string
		genericMarker     string
		genericExitCode   int
		sshMarker         string
		sshMarkerFound    bool
		githubMarker      string
		githubMarkerFound bool
		wantSSHCleanup    bool
		wantGitHubCleanup bool
		wantGitScrub      bool
	}{
		{
			name:              "GitHub-only owner marker preserves unmanaged SSH",
			genericMarker:     strings.Repeat("a", 32),
			githubMarker:      strings.Repeat("a", 32),
			githubMarkerFound: true,
			wantGitHubCleanup: true,
			wantGitScrub:      true,
		},
		{
			name:           "SSH-only owner marker preserves unmanaged GitHub",
			genericMarker:  strings.Repeat("b", 32),
			sshMarker:      strings.Repeat("b", 32),
			sshMarkerFound: true,
			wantSSHCleanup: true,
		},
		{name: "legacy generic marker proves neither channel", genericMarker: strings.Repeat("c", 32)},
		{name: "missing markers", genericExitCode: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := newTestNeoOwnerOrbCredentialStore(t)
			if revision, err := store.revokeIfRevision("owner", neoOrbCredentialAbsentRevision); err != nil || revision != neoOrbCredentialRevokedRevision {
				t.Fatalf("seed revocation = %q, %v", revision, err)
			}
			fake := &neoOrbFakeProvider{execHandler: func(cmd []string) neoOrbExecResult {
				if len(cmd) == 2 && cmd[0] == "cat" && cmd[1] == neoOrbOwnerCredentialRevisionPath {
					return neoOrbExecResult{ExitCode: test.genericExitCode, Stdout: test.genericMarker}
				}
				if len(cmd) == 2 && cmd[0] == "cat" && cmd[1] == neoOrbOwnerSSHRevisionPath {
					if test.sshMarkerFound {
						return neoOrbExecResult{ExitCode: 0, Stdout: test.sshMarker}
					}
					return neoOrbExecResult{ExitCode: 1}
				}
				if len(cmd) == 2 && cmd[0] == "cat" && cmd[1] == neoOrbOwnerGHRevisionPath {
					if test.githubMarkerFound {
						return neoOrbExecResult{ExitCode: 0, Stdout: test.githubMarker}
					}
					return neoOrbExecResult{ExitCode: 1}
				}
				return neoOrbExecResult{ExitCode: 0}
			}}
			manager := &neoOrbManager{runtime: &neoRuntime{orbCredentialStore: store}}
			selection, err := manager.orbReconcileOwnerCredentials(t.Context(), fake, "container", "owner")
			if err != nil {
				t.Fatal(err)
			}
			if selection.Found || !selection.Revoked || !selection.suppressSharedGitHub() {
				t.Fatalf("revoked selection = %#v", selection)
			}
			calls := strings.Join(fake.calls, "\n")
			sshCleanup := strings.Contains(calls, "mv /root/.ssh "+neoOrbOwnerSSHBackupPath)
			if sshCleanup != test.wantSSHCleanup {
				t.Fatalf("SSH cleanup mismatch:\n%s", calls)
			}
			githubCleanup := strings.Contains(calls, "mv /root/.config/gh "+neoOrbOwnerGHBackupPath)
			if githubCleanup != test.wantGitHubCleanup {
				t.Fatalf("GitHub cleanup mismatch:\n%s", calls)
			}
			gitScrub := strings.Contains(calls, "git config --global --unset-all url.https://github.com/.insteadOf")
			if gitScrub != test.wantGitScrub {
				t.Fatalf("Git scrub = %t, want %t:\n%s", gitScrub, test.wantGitScrub, calls)
			}
			for _, path := range []string{neoOrbOwnerSSHStagePath, neoOrbOwnerSSHBackupPath, neoOrbOwnerGHStagePath, neoOrbOwnerGHBackupPath} {
				if !strings.Contains(calls, path) {
					t.Fatalf("cleanup omitted %s:\n%s", path, calls)
				}
			}
			if !strings.Contains(calls, "copy:"+neoOrbOwnerCredentialRevisionPath+":8:384") {
				t.Fatalf("revoked marker was not persisted last:\n%s", calls)
			}
		})
	}
}

func TestNeoOrbOwnerCredentialRepeatedRevocationIsInert(t *testing.T) {
	store := newTestNeoOwnerOrbCredentialStore(t)
	if _, err := store.revokeIfRevision("owner", neoOrbCredentialAbsentRevision); err != nil {
		t.Fatal(err)
	}
	fake := &neoOrbFakeProvider{execHandler: func(cmd []string) neoOrbExecResult {
		if len(cmd) == 2 && cmd[0] == "cat" && cmd[1] == neoOrbOwnerCredentialRevisionPath {
			return neoOrbExecResult{ExitCode: 0, Stdout: neoOrbCredentialRevokedRevision + "\n"}
		}
		return neoOrbExecResult{ExitCode: 0}
	}}
	manager := &neoOrbManager{runtime: &neoRuntime{orbCredentialStore: store}}
	if selection, err := manager.orbReconcileOwnerCredentials(t.Context(), fake, "container", "owner"); err != nil || selection.Found || !selection.Revoked || !selection.suppressSharedGitHub() {
		t.Fatalf("selection=%#v err=%v", selection, err)
	}
	if len(fake.calls) != 3 || fake.callCount("exec:cat "+neoOrbOwnerCredentialRevisionPath) != 1 || fake.callCount("exec:cat "+neoOrbOwnerSSHRevisionPath) != 1 || fake.callCount("exec:cat "+neoOrbOwnerGHRevisionPath) != 1 {
		t.Fatalf("repeated revocation calls = %#v", fake.calls)
	}
}

func TestNeoOrbOwnerCredentialAbsenceAllowsSharedGitHub(t *testing.T) {
	manager := &neoOrbManager{runtime: &neoRuntime{orbCredentialStore: newTestNeoOwnerOrbCredentialStore(t)}}
	fake := &neoOrbFakeProvider{execHandler: func(cmd []string) neoOrbExecResult {
		if len(cmd) == 2 && cmd[0] == "cat" {
			return neoOrbExecResult{ExitCode: 1}
		}
		return neoOrbExecResult{}
	}}
	selection, err := manager.orbReconcileOwnerCredentials(t.Context(), fake, "container", "owner")
	if err != nil || selection.Found || selection.Revoked || selection.suppressSharedGitHub() {
		t.Fatalf("selection=%#v err=%v", selection, err)
	}
	if len(fake.calls) != 4 || fake.callCount("exec:cat "+neoOrbOwnerSSHRevisionPath) != 1 || fake.callCount("exec:cat "+neoOrbOwnerGHRevisionPath) != 1 {
		t.Fatalf("absent credential marker inspection calls = %#v", fake.calls)
	}
}

func TestNeoOrbOwnerCredentialUnreadableMarkerFailsClosed(t *testing.T) {
	manager := &neoOrbManager{runtime: &neoRuntime{orbCredentialStore: newTestNeoOwnerOrbCredentialStore(t)}}
	fake := &neoOrbFakeProvider{execHandler: func(cmd []string) neoOrbExecResult {
		if len(cmd) == 2 && cmd[0] == "cat" && cmd[1] == neoOrbOwnerSSHRevisionPath {
			return neoOrbExecResult{ExitCode: 1, Stderr: "credential-marker-secret"}
		}
		if len(cmd) == 5 && cmd[0] == "/bin/sh" && cmd[4] == neoOrbOwnerSSHRevisionPath {
			return neoOrbExecResult{ExitCode: 1}
		}
		return neoOrbExecResult{}
	}}
	selection, err := manager.orbReconcileOwnerCredentials(t.Context(), fake, "container", "owner")
	if err == nil || selection.Found || selection.Revoked || selection.suppressSharedGitHub() {
		t.Fatalf("selection=%#v err=%v", selection, err)
	}
	if strings.Contains(err.Error(), "credential-marker-secret") {
		t.Fatalf("marker inspection error disclosed provider output: %v", err)
	}
	if fake.callCount("copy:") != 0 || strings.Contains(strings.Join(fake.calls, "\n"), "mv /root/.ssh") {
		t.Fatalf("unreadable marker allowed credential mutation: %#v", fake.calls)
	}
}

func TestNeoOrbOwnerCredentialAbsenceCleansOnlyChannelManagedRoots(t *testing.T) {
	for _, test := range []struct {
		name              string
		sshMarker         string
		githubMarker      string
		wantSSHCleanup    bool
		wantGitHubCleanup bool
	}{
		{
			name:              "managed roots",
			sshMarker:         strings.Repeat("a", 32),
			githubMarker:      strings.Repeat("b", 32),
			wantSSHCleanup:    true,
			wantGitHubCleanup: true,
		},
		{name: "malformed markers preserve roots", sshMarker: "malformed-ssh", githubMarker: "malformed-github"},
	} {
		t.Run(test.name, func(t *testing.T) {
			fake := &neoOrbFakeProvider{execHandler: func(cmd []string) neoOrbExecResult {
				if len(cmd) == 2 && cmd[0] == "cat" {
					switch cmd[1] {
					case neoOrbOwnerSSHRevisionPath:
						return neoOrbExecResult{Stdout: test.sshMarker}
					case neoOrbOwnerGHRevisionPath:
						return neoOrbExecResult{Stdout: test.githubMarker}
					}
				}
				return neoOrbExecResult{}
			}}
			manager := &neoOrbManager{runtime: &neoRuntime{orbCredentialStore: newTestNeoOwnerOrbCredentialStore(t)}}
			selection, err := manager.orbReconcileOwnerCredentials(t.Context(), fake, "container", "owner")
			if err != nil || selection.Found || selection.Revoked || selection.suppressSharedGitHub() {
				t.Fatalf("selection=%#v err=%v", selection, err)
			}
			calls := strings.Join(fake.calls, "\n")
			sshCleanup := strings.Contains(calls, "mv /root/.ssh "+neoOrbOwnerSSHBackupPath)
			githubCleanup := strings.Contains(calls, "mv /root/.config/gh "+neoOrbOwnerGHBackupPath)
			if sshCleanup != test.wantSSHCleanup || githubCleanup != test.wantGitHubCleanup {
				t.Fatalf("managed cleanup SSH=%t GitHub=%t calls:\n%s", sshCleanup, githubCleanup, calls)
			}
			gitScrub := strings.Contains(calls, "git config --global --unset-all url.https://github.com/.insteadOf")
			if gitScrub != test.wantGitHubCleanup {
				t.Fatalf("GitHub scrub=%t calls:\n%s", gitScrub, calls)
			}
		})
	}
}

func TestNeoOrbOwnerGitHubAuthUsesRootConfigWithBookwormGH(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("test executes POSIX shell shims")
	}
	var script string
	fake := &neoOrbFakeProvider{execHandler: func(cmd []string) neoOrbExecResult {
		if len(cmd) == 3 && cmd[0] == "/bin/sh" && cmd[1] == "-lc" {
			script = cmd[2]
		}
		return neoOrbExecResult{ExitCode: 0}
	}}
	manager := &neoOrbManager{}
	if err := manager.orbConfigureOwnerGitHubAuth(t.Context(), fake, "container", neoOrbCredentialSelection{HasGitHub: true}); err != nil {
		t.Fatal(err)
	}
	if script == "" {
		t.Fatal("GitHub configuration script was not issued")
	}

	temp := t.TempDir()
	orbRoot := filepath.Join(temp, "root")
	userHome := filepath.Join(temp, "home", "user")
	binDir := filepath.Join(temp, "bin")
	callsPath := filepath.Join(temp, "calls")
	for _, directory := range []string{filepath.Join(orbRoot, ".config", "gh"), userHome, binDir} {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(orbRoot, ".config", "gh", "hosts.yml"), []byte("github.com:\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitStub := `#!/bin/sh
set -eu
test "$HOME" = "$EXPECTED_ROOT"
test "$GH_CONFIG_DIR" = "$EXPECTED_ROOT/.config/gh"
printf 'git:%s\n' "$*" >> "$CALLS_PATH"`
	ghStub := `#!/bin/sh
set -eu
test "$HOME" = "$EXPECTED_ROOT"
test "$GH_CONFIG_DIR" = "$EXPECTED_ROOT/.config/gh"
test -f "$GH_CONFIG_DIR/hosts.yml"
for argument in "$@"; do
  test "$argument" != --force
done
test "$*" = "auth setup-git --hostname github.com"
printf 'gh:%s\n' "$*" >> "$CALLS_PATH"`
	for name, body := range map[string]string{"git": gitStub, "gh": ghStub} {
		if err := os.WriteFile(filepath.Join(binDir, name), []byte(body), 0o700); err != nil {
			t.Fatal(err)
		}
	}

	command := exec.Command("sh", "-c", strings.ReplaceAll(script, "/root", orbRoot))
	command.Env = []string{
		"PATH=" + binDir + ":/usr/bin:/bin",
		"HOME=" + userHome,
		"EXPECTED_ROOT=" + orbRoot,
		"CALLS_PATH=" + callsPath,
	}
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("run GitHub configuration script: %v: %s", err, output)
	}
	calls, err := os.ReadFile(callsPath)
	if err != nil {
		t.Fatal(err)
	}
	callLog := string(calls)
	for _, expected := range []string{
		"git:config --global --unset-all url.https://github.com/.insteadOf",
		"gh:auth setup-git --hostname github.com",
		"git:config --global --add url.https://github.com/.insteadOf git@github.com:",
	} {
		if !strings.Contains(callLog, expected) {
			t.Fatalf("missing %q in calls:\n%s", expected, callLog)
		}
	}
}

func newTestNeoOwnerSSHCredentialSnapshot(t *testing.T) orbcredentials.Snapshot {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	privateBlock, err := ssh.MarshalPrivateKey(privateKey, "")
	if err != nil {
		t.Fatal(err)
	}
	sshPublicKey, err := ssh.NewPublicKey(publicKey)
	if err != nil {
		t.Fatal(err)
	}
	publicLine := strings.TrimSpace(string(ssh.MarshalAuthorizedKey(sshPublicKey)))
	snapshot, err := orbcredentials.New("", []orbcredentials.DecodedSSHIdentity{{
		Name:       "id_ed25519",
		PrivateKey: pem.EncodeToMemory(privateBlock),
		PublicKey:  ssh.MarshalAuthorizedKey(sshPublicKey),
	}}, []byte("github.com "+publicLine+"\n"))
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func newTestNeoOwnerOrbCredentialStore(t *testing.T) *neoOwnerOrbCredentialStore {
	t.Helper()
	root := t.TempDir()
	threadDir := filepath.Join(root, "data", "threads")
	if err := os.MkdirAll(threadDir, 0o700); err != nil {
		t.Fatal(err)
	}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	keyPath := filepath.Join(root, "credential-key")
	if err := os.WriteFile(keyPath, []byte(base64.StdEncoding.EncodeToString(key)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(neoOrbCredentialKeyFileEnv, keyPath)
	store, err := newNeoOwnerOrbCredentialStore(threadDir)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func writeTestNeoOwnerOrbCredentialKey(t *testing.T) {
	t.Helper()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	keyPath := filepath.Join(t.TempDir(), "credential-key")
	if err := os.WriteFile(keyPath, []byte(base64.StdEncoding.EncodeToString(key)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(neoOrbCredentialKeyFileEnv, keyPath)
}
