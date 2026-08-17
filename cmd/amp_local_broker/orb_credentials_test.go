//go:build darwin

package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/orbcredentials"
	"golang.org/x/crypto/ssh"
)

func TestCollectOrbCredentialsExplicitGitHubAndBroadSSH(t *testing.T) {
	home := t.TempDir()
	installOrbCredentialTestGH(t, home, "github-credential-sentinel", "")
	sshDirectory := filepath.Join(home, ".ssh")
	if err := os.MkdirAll(sshDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	privateBlock, err := ssh.MarshalPrivateKey(private, "")
	if err != nil {
		t.Fatal(err)
	}
	privateKey := pem.EncodeToMemory(privateBlock)
	sshPublic, err := ssh.NewPublicKey(public)
	if err != nil {
		t.Fatal(err)
	}
	publicKey := ssh.MarshalAuthorizedKey(sshPublic)
	knownHosts := []byte("github.com " + strings.TrimSpace(string(publicKey)) + "\n")
	writeOrbCredentialTestFile(t, filepath.Join(sshDirectory, "id_ed25519"), privateKey, 0o600)
	writeOrbCredentialTestFile(t, filepath.Join(sshDirectory, "id_ed25519.pub"), publicKey, 0o644)
	writeOrbCredentialTestFile(t, filepath.Join(sshDirectory, "known_hosts"), knownHosts, 0o644)
	writeOrbCredentialTestFile(t, filepath.Join(sshDirectory, "config"), []byte("Host *\n    ForwardAgent yes\n"), 0o600)
	writeOrbCredentialTestFile(t, filepath.Join(sshDirectory, "authorized_keys"), publicKey, 0o600)
	snapshot, err := collectOrbCredentials(t.Context(), home, orbCredentialSyncConfig{GitHub: true, AllowBroadSSHSync: true})
	if err != nil {
		t.Fatal(err)
	}
	_, decoded, err := orbcredentials.Validate(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.GitHubToken != "github-credential-sentinel" || decoded.SSH == nil || len(decoded.SSH.Identities) != 1 {
		t.Fatalf("decoded credentials = %#v", decoded)
	}
	if decoded.SSH.Identities[0].Name != "id_ed25519" || string(decoded.SSH.KnownHosts) != string(knownHosts) {
		t.Fatalf("decoded SSH credentials = %#v", decoded.SSH)
	}
	raw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "ForwardAgent") || strings.Contains(string(raw), "authorized_keys") {
		t.Fatalf("unapproved SSH content was collected: %s", raw)
	}
}

func TestCollectOrbCredentialsFailureDoesNotExposeCommandDiagnostic(t *testing.T) {
	home := t.TempDir()
	installOrbCredentialTestGH(t, home, "", "diagnostic-secret-sentinel")
	_, err := collectOrbCredentials(t.Context(), home, orbCredentialSyncConfig{GitHub: true})
	if err == nil {
		t.Fatal("expected GitHub credential collection failure")
	}
	if err.Error() != "GitHub credential collection failed" || strings.Contains(err.Error(), "diagnostic-secret-sentinel") {
		t.Fatalf("collection error exposed command diagnostic: %v", err)
	}
}

func TestCollectOrbCredentialsAcceptsMaximumGitHubTokenWithNewline(t *testing.T) {
	home := t.TempDir()
	token := strings.Repeat("a", orbcredentials.MaxGitHubTokenBytes)
	installOrbCredentialTestGH(t, home, token, "")
	snapshot, err := collectOrbCredentials(t.Context(), home, orbCredentialSyncConfig{GitHub: true})
	if err != nil {
		t.Fatal(err)
	}
	_, decoded, err := orbcredentials.Validate(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.GitHubToken != token {
		t.Fatalf("collected GitHub token length = %d, want %d", len(decoded.GitHubToken), len(token))
	}
}

func TestCollectOrbCredentialsRejectsOversizedGitHubToken(t *testing.T) {
	home := t.TempDir()
	installOrbCredentialTestGH(t, home, strings.Repeat("a", orbcredentials.MaxGitHubTokenBytes+1), "")
	if _, err := collectOrbCredentials(t.Context(), home, orbCredentialSyncConfig{GitHub: true}); !errors.Is(err, errOrbGitHubCredentialCollection) {
		t.Fatalf("oversized GitHub token error = %v", err)
	}
}

func TestOrbCredentialOutputRejectsChunkedOverflow(t *testing.T) {
	output := &orbCredentialOutput{}
	for _, chunk := range [][]byte{
		[]byte(strings.Repeat("a", orbcredentials.MaxGitHubTokenBytes)),
		[]byte("\n"),
		[]byte("x"),
	} {
		if written, err := output.Write(chunk); err != nil || written != len(chunk) {
			t.Fatalf("write = %d, %v", written, err)
		}
	}
	if !output.overflow || len(output.data) != orbcredentials.MaxGitHubTokenBytes+1 {
		t.Fatalf("overflow=%t captured=%d", output.overflow, len(output.data))
	}
}

func TestCollectOrbCredentialsSkipsUnusableSSHIdentities(t *testing.T) {
	home := t.TempDir()
	installOrbCredentialTestGH(t, home, "github-credential-sentinel", "")
	sshDirectory := filepath.Join(home, ".ssh")
	if err := os.MkdirAll(sshDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	privateBlock, err := ssh.MarshalPrivateKey(private, "")
	if err != nil {
		t.Fatal(err)
	}
	protectedBlock, err := ssh.MarshalPrivateKeyWithPassphrase(private, "", []byte("passphrase-sentinel"))
	if err != nil {
		t.Fatal(err)
	}
	sshPublic, err := ssh.NewPublicKey(public)
	if err != nil {
		t.Fatal(err)
	}
	writeOrbCredentialTestFile(t, filepath.Join(sshDirectory, "id_ed25519"), pem.EncodeToMemory(privateBlock), 0o600)
	writeOrbCredentialTestFile(t, filepath.Join(sshDirectory, "id_ed25519.pub"), ssh.MarshalAuthorizedKey(sshPublic), 0o644)
	writeOrbCredentialTestFile(t, filepath.Join(sshDirectory, "id_invalid"), []byte("not-a-private-key"), 0o600)
	writeOrbCredentialTestFile(t, filepath.Join(sshDirectory, "id_protected"), pem.EncodeToMemory(protectedBlock), 0o600)
	snapshot, err := collectOrbCredentials(t.Context(), home, orbCredentialSyncConfig{GitHub: true, AllowBroadSSHSync: true})
	if err != nil {
		t.Fatal(err)
	}
	_, decoded, err := orbcredentials.Validate(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.GitHubToken != "github-credential-sentinel" || decoded.SSH == nil || len(decoded.SSH.Identities) != 1 || decoded.SSH.Identities[0].Name != "id_ed25519" {
		t.Fatalf("decoded credentials = %#v", decoded)
	}
}

func TestCollectOrbCredentialsNoMaterialIsNoOp(t *testing.T) {
	home := t.TempDir()
	for _, cfg := range []orbCredentialSyncConfig{{}, {AllowBroadSSHSync: true}} {
		snapshot, err := collectOrbCredentials(t.Context(), home, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if snapshot != (orbcredentials.Snapshot{}) {
			t.Fatalf("empty collection = %#v", snapshot)
		}
	}
}

func TestReadOrbCredentialRegularFileRejectsPrivatePermissionDrift(t *testing.T) {
	path := filepath.Join(t.TempDir(), "id_ed25519")
	writeOrbCredentialTestFile(t, path, []byte("private-key"), 0o600)
	previousOpen := orbCredentialFileOpen
	orbCredentialFileOpen = func(path string) (*os.File, error) {
		file, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		if err := os.Chmod(path, 0o644); err != nil {
			return nil, errors.Join(err, file.Close())
		}
		return file, nil
	}
	t.Cleanup(func() { orbCredentialFileOpen = previousOpen })
	if _, found, err := readOrbCredentialRegularFile(path, orbcredentials.MaxPrivateKeyBytes, true); err == nil || found {
		t.Fatalf("permission-drift read = found %t, error %v", found, err)
	}
}

func TestBrokerOrbCredentialKnownHostsWithoutIdentityIsNoOp(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	sshDirectory := filepath.Join(home, ".ssh")
	if err := os.MkdirAll(sshDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	public, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	sshPublic, err := ssh.NewPublicKey(public)
	if err != nil {
		t.Fatal(err)
	}
	knownHosts := []byte("github.com " + strings.TrimSpace(string(ssh.MarshalAuthorizedKey(sshPublic))) + "\n")
	writeOrbCredentialTestFile(t, filepath.Join(sshDirectory, "known_hosts"), knownHosts, 0o644)
	uploads := 0
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case heartbeatEndpoint:
			response.Header().Set(orbCredentialSupportHeader, "1")
			response.Header().Set(orbCredentialRevisionHeader, orbCredentialAbsentRevision)
			_, _ = fmt.Fprint(response, `{"ok":true,"runners":[]}`)
		case orbCredentialEndpoint:
			uploads++
			response.WriteHeader(http.StatusBadRequest)
		default:
			response.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	localBroker := newOrbCredentialTestBroker(server, &orbCredentialSyncConfig{AllowBroadSSHSync: true})
	if err := localBroker.performHeartbeat(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := localBroker.performHeartbeat(t.Context()); err != nil {
		t.Fatal(err)
	}
	if uploads != 0 {
		t.Fatalf("credential uploads = %d, want 0", uploads)
	}
}

func TestBrokerOrbCredentialCollectionFailureRetainsServerSnapshot(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	sshDirectory := filepath.Join(home, ".ssh")
	if err := os.MkdirAll(sshDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	writeOrbCredentialTestFile(t, filepath.Join(sshDirectory, "id_insecure"), []byte("not-a-private-key"), 0o644)
	uploads := 0
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		if request.URL.Path == heartbeatEndpoint {
			response.Header().Set(orbCredentialSupportHeader, "1")
			response.Header().Set(orbCredentialRevisionHeader, orbCredentialAbsentRevision)
			_, _ = fmt.Fprint(response, `{"ok":true,"runners":[]}`)
			return
		}
		if request.URL.Path == orbCredentialEndpoint {
			uploads++
		}
		response.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(server.Close)
	localBroker := newOrbCredentialTestBroker(server, &orbCredentialSyncConfig{AllowBroadSSHSync: true})
	err := localBroker.performHeartbeat(t.Context())
	if err == nil || err.Error() != "SSH credential collection failed" || strings.Contains(err.Error(), "id_insecure") {
		t.Fatalf("heartbeat error = %v", err)
	}
	if uploads != 0 {
		t.Fatalf("collection failure uploaded %d snapshots", uploads)
	}
}

func TestOrbCredentialUploadResponseCannotInjectSecretIntoError(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		requests++
		response.Header().Set("Content-Type", "application/json")
		response.WriteHeader(http.StatusInternalServerError)
		_, _ = fmt.Fprint(response, `{"ok":false,"message":"response-secret-sentinel"}`)
	}))
	t.Cleanup(server.Close)
	localBroker := newOrbCredentialTestBroker(server, &orbCredentialSyncConfig{})
	if _, err := localBroker.putOrbCredentials(t.Context(), orbcredentials.Snapshot{Schema: orbcredentials.Schema}); err == nil {
		t.Fatal("empty credential upload unexpectedly succeeded")
	}
	if requests != 0 {
		t.Fatalf("empty credential upload sent %d requests", requests)
	}
	snapshot, err := orbcredentials.New("upload-secret-sentinel", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = localBroker.putOrbCredentials(t.Context(), snapshot)
	if err == nil || strings.Contains(err.Error(), "response-secret-sentinel") || strings.Contains(err.Error(), "upload-secret-sentinel") {
		t.Fatalf("upload error = %v", err)
	}
	if requests != 1 {
		t.Fatalf("credential upload sent %d requests", requests)
	}
}

func TestOrbCredentialUploadRejectsTrailingJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		response.Header().Set(orbCredentialRevisionHeader, strings.Repeat("a", 32))
		_, _ = fmt.Fprint(response, `{"ok":true}{"ok":true}`)
	}))
	t.Cleanup(server.Close)
	localBroker := newOrbCredentialTestBroker(server, &orbCredentialSyncConfig{})
	snapshot, err := orbcredentials.New("upload-secret-sentinel", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := localBroker.putOrbCredentials(t.Context(), snapshot); err == nil {
		t.Fatal("credential upload accepted trailing JSON")
	}
}

func TestBrokerOrbCredentialServerDataLossForcesReupload(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	installOrbCredentialTestGH(t, home, "github-credential-sentinel", "")
	uploads := 0
	serverRevision := orbCredentialAbsentRevision
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case heartbeatEndpoint:
			response.Header().Set(orbCredentialSupportHeader, "1")
			response.Header().Set(orbCredentialRevisionHeader, serverRevision)
			_, _ = fmt.Fprint(response, `{"ok":true,"runners":[]}`)
		case orbCredentialEndpoint:
			uploads++
			serverRevision = fmt.Sprintf("%032x", uploads)
			response.Header().Set(orbCredentialRevisionHeader, serverRevision)
			_, _ = fmt.Fprint(response, `{"ok":true}`)
		default:
			response.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	localBroker := newOrbCredentialTestBroker(server, &orbCredentialSyncConfig{GitHub: true})
	if err := localBroker.performHeartbeat(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := localBroker.performHeartbeat(t.Context()); err != nil {
		t.Fatal(err)
	}
	if uploads != 1 {
		t.Fatalf("credential uploads before data loss = %d, want 1", uploads)
	}
	serverRevision = orbCredentialAbsentRevision
	if err := localBroker.performHeartbeat(t.Context()); err != nil {
		t.Fatal(err)
	}
	if uploads != 2 {
		t.Fatalf("credential uploads after data loss = %d, want 2", uploads)
	}
}

func TestBrokerEnabledOrbCredentialsRevokeStaleRemoteSnapshot(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	sshDirectory := filepath.Join(home, ".ssh")
	if err := os.MkdirAll(sshDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	privateBlock, err := ssh.MarshalPrivateKey(private, "")
	if err != nil {
		t.Fatal(err)
	}
	privatePath := filepath.Join(sshDirectory, "id_ed25519")
	writeOrbCredentialTestFile(t, privatePath, pem.EncodeToMemory(privateBlock), 0o600)
	serverRevision := orbCredentialAbsentRevision
	uploads := 0
	clears := 0
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case heartbeatEndpoint:
			response.Header().Set(orbCredentialSupportHeader, "1")
			response.Header().Set(orbCredentialRevisionHeader, serverRevision)
			_, _ = fmt.Fprint(response, `{"ok":true,"runners":[]}`)
		case orbCredentialEndpoint:
			switch request.Method {
			case http.MethodPut:
				uploads++
				serverRevision = strings.Repeat("c", 32)
			case http.MethodDelete:
				var clear orbCredentialClearRequest
				if err := json.NewDecoder(request.Body).Decode(&clear); err != nil || clear.ExpectedRevision != serverRevision {
					response.WriteHeader(http.StatusBadRequest)
					return
				}
				clears++
				serverRevision = orbCredentialRevokedRevision
			default:
				response.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			response.Header().Set(orbCredentialRevisionHeader, serverRevision)
			_, _ = fmt.Fprint(response, `{"ok":true}`)
		default:
			response.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	localBroker := newOrbCredentialTestBroker(server, &orbCredentialSyncConfig{AllowBroadSSHSync: true})
	if err := localBroker.performHeartbeat(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(privatePath); err != nil {
		t.Fatal(err)
	}
	if err := localBroker.performHeartbeat(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := localBroker.performHeartbeat(t.Context()); err != nil {
		t.Fatal(err)
	}
	if uploads != 1 || clears != 1 {
		t.Fatalf("uploads=%d clears=%d", uploads, clears)
	}
	if localBroker.orbCredentials.snapshot != nil || localBroker.orbCredentials.revision != orbCredentialRevokedRevision || localBroker.orbCredentials.serverRevision != orbCredentialRevokedRevision {
		t.Fatalf("credential state = %#v", localBroker.orbCredentials)
	}
}

func TestBrokerDisabledOrbCredentialsRevokeOnce(t *testing.T) {
	for _, test := range []struct {
		name string
		cfg  *orbCredentialSyncConfig
	}{
		{name: "omitted"},
		{name: "all false", cfg: &orbCredentialSyncConfig{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			serverRevision := strings.Repeat("a", 32)
			clears := 0
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
				response.Header().Set("Content-Type", "application/json")
				switch request.URL.Path {
				case heartbeatEndpoint:
					response.Header().Set(orbCredentialSupportHeader, "1")
					response.Header().Set(orbCredentialRevisionHeader, serverRevision)
					_, _ = fmt.Fprint(response, `{"ok":true,"runners":[]}`)
				case orbCredentialEndpoint:
					clears++
					if request.Method != http.MethodDelete {
						response.WriteHeader(http.StatusMethodNotAllowed)
						return
					}
					var clear orbCredentialClearRequest
					decoder := json.NewDecoder(request.Body)
					decoder.DisallowUnknownFields()
					if err := decoder.Decode(&clear); err != nil || clear.ExpectedRevision != serverRevision {
						response.WriteHeader(http.StatusBadRequest)
						return
					}
					serverRevision = orbCredentialRevokedRevision
					response.Header().Set(orbCredentialRevisionHeader, serverRevision)
					_, _ = fmt.Fprint(response, `{"ok":true}`)
				default:
					response.WriteHeader(http.StatusNotFound)
				}
			}))
			t.Cleanup(server.Close)
			localBroker := newOrbCredentialTestBroker(server, test.cfg)
			if err := localBroker.performHeartbeat(t.Context()); err != nil {
				t.Fatal(err)
			}
			if err := localBroker.performHeartbeat(t.Context()); err != nil {
				t.Fatal(err)
			}
			if clears != 1 || localBroker.orbCredentials.serverRevision != orbCredentialRevokedRevision {
				t.Fatalf("clears=%d revision=%q", clears, localBroker.orbCredentials.serverRevision)
			}
		})
	}
}

func TestBrokerEnabledOrbCredentialsUploadAfterRevocation(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	installOrbCredentialTestGH(t, home, "github-credential-after-revocation", "")
	serverRevision := orbCredentialRevokedRevision
	uploads := 0
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case heartbeatEndpoint:
			response.Header().Set(orbCredentialSupportHeader, "1")
			response.Header().Set(orbCredentialRevisionHeader, serverRevision)
			_, _ = fmt.Fprint(response, `{"ok":true,"runners":[]}`)
		case orbCredentialEndpoint:
			if request.Method != http.MethodPut {
				response.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			uploads++
			serverRevision = strings.Repeat("b", 32)
			response.Header().Set(orbCredentialRevisionHeader, serverRevision)
			_, _ = fmt.Fprint(response, `{"ok":true}`)
		default:
			response.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	localBroker := newOrbCredentialTestBroker(server, &orbCredentialSyncConfig{GitHub: true})
	if err := localBroker.performHeartbeat(t.Context()); err != nil {
		t.Fatal(err)
	}
	if uploads != 1 || localBroker.orbCredentials.serverRevision != strings.Repeat("b", 32) {
		t.Fatalf("uploads=%d revision=%q", uploads, localBroker.orbCredentials.serverRevision)
	}
}

func TestOrbCredentialClearResponseValidation(t *testing.T) {
	for _, test := range []struct {
		name       string
		status     int
		body       string
		revision   string
		want       error
		wantAnyErr bool
	}{
		{name: "trailing JSON", status: http.StatusOK, body: `{"ok":true}{"ok":true}`, revision: orbCredentialRevokedRevision, wantAnyErr: true},
		{name: "stale session", status: http.StatusConflict, body: `{"ok":false,"error":"stale_session"}`, want: errStaleSession},
		{name: "revision conflict", status: http.StatusConflict, body: `{"ok":false,"error":"revision_conflict"}`, want: errOrbCredentialRevisionConflict},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
				response.Header().Set("Content-Type", "application/json")
				if test.revision != "" {
					response.Header().Set(orbCredentialRevisionHeader, test.revision)
				}
				response.WriteHeader(test.status)
				_, _ = fmt.Fprint(response, test.body)
			}))
			t.Cleanup(server.Close)
			localBroker := newOrbCredentialTestBroker(server, nil)
			_, err := localBroker.clearOrbCredentials(t.Context(), orbCredentialAbsentRevision)
			if test.want != nil && !errors.Is(err, test.want) {
				t.Fatalf("error=%v, want %v", err, test.want)
			}
			if test.wantAnyErr && err == nil {
				t.Fatal("invalid clear response was accepted")
			}
		})
	}
}

func TestOrbCredentialRequestsUseCallerContext(t *testing.T) {
	snapshot, err := orbcredentials.New("request-limit-token", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name     string
		method   string
		revision string
		call     func(*broker, context.Context) error
	}{
		{
			name:     "put",
			method:   http.MethodPut,
			revision: strings.Repeat("d", 32),
			call: func(localBroker *broker, ctx context.Context) error {
				_, err := localBroker.putOrbCredentials(ctx, snapshot)
				return err
			},
		},
		{
			name:     "clear",
			method:   http.MethodDelete,
			revision: orbCredentialRevokedRevision,
			call: func(localBroker *broker, ctx context.Context) error {
				_, err := localBroker.clearOrbCredentials(ctx, orbCredentialAbsentRevision)
				return err
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var requestContext context.Context
			client := &http.Client{Transport: brokerRoundTripper(func(request *http.Request) (*http.Response, error) {
				if request.Method != test.method {
					t.Errorf("request method = %s, want %s", request.Method, test.method)
				}
				requestContext = request.Context()
				header := make(http.Header)
				header.Set(orbCredentialRevisionHeader, test.revision)
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     header,
					Body:       io.NopCloser(strings.NewReader(`{"ok":true}`)),
				}, nil
			})}
			localBroker := &broker{
				config:            &brokerConfig{BrokerID: "broker", APIURL: "https://broker.example", apiKey: "key"},
				client:            client,
				sessionID:         "session",
				sessionGeneration: 1,
			}
			callerContext, cancel := context.WithCancel(context.Background())
			defer cancel()
			if err := test.call(localBroker, callerContext); err != nil {
				t.Fatal(err)
			}
			assertBrokerCallerContext(t, requestContext, cancel)
		})
	}
}

func newOrbCredentialTestBroker(server *httptest.Server, syncConfig *orbCredentialSyncConfig) *broker {
	return &broker{
		config: &brokerConfig{
			BrokerID:       "broker",
			APIURL:         server.URL,
			apiKey:         "key",
			OrbCredentials: syncConfig,
			Workspaces:     []workspaceConfig{},
		},
		client:             server.Client(),
		requestLimit:       time.Second,
		sessionID:          "session",
		sessionGeneration:  1,
		hostname:           "Mac",
		pid:                123,
		workspacesByRunner: map[string]*workspaceConfig{},
		children:           map[childKey]*childProcess{},
		launches:           map[childKey]*childLaunch{},
	}
}

func installOrbCredentialTestGH(t *testing.T, home, token, diagnostic string) {
	t.Helper()
	bin := filepath.Join(home, "bin")
	if err := os.MkdirAll(bin, 0o700); err != nil {
		t.Fatal(err)
	}
	script := "#!/bin/sh\n"
	if diagnostic != "" {
		script += "printf '%s\\n' " + diagnostic + " >&2\nexit 1\n"
	} else {
		script += "printf '%s\\n' " + token + "\n"
	}
	writeOrbCredentialTestFile(t, filepath.Join(bin, "gh"), []byte(script), 0o700)
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func writeOrbCredentialTestFile(t *testing.T, path string, content []byte, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, content, mode); err != nil {
		t.Fatal(err)
	}
}
