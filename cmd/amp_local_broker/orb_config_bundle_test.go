//go:build darwin

package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/orbconfig"
)

func TestCollectOrbConfigBundleDeterministicAndSafe(t *testing.T) {
	home := t.TempDir()
	commit := strings.Repeat("a", 40)
	writeOrbConfigTestFile(t, home, ".config/agents/checks/review.md", "review")
	writeOrbConfigTestFile(t, home, ".config/agents/checks/auth-token.json", "secret")
	writeOrbConfigTestFile(t, home, ".config/agents/checks/local-path.md", "read "+home+"/private")
	writeOrbConfigTestFile(t, home, ".config/agents/checks/leaked.md", "ghp_ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	writeOrbConfigTestFile(t, home, ".config/agents/skills/demo/SKILL.md", "---\nname: demo\n---\n")
	writeOrbConfigTestFile(t, home, ".config/agents/skills/demo/reference.md", "reference")
	writeOrbConfigTestFile(t, home, ".config/agents/skills/mcp/SKILL.md", "# mcp")
	writeOrbConfigTestFile(t, home, ".config/agents/skills/mcp/mcp.json", "{}")
	writeOrbConfigTestFile(t, home, ".config/agents/skills/malformed/SKILL.md", "---\nname: [\n---\n")
	writeOrbConfigTestFile(t, home, ".config/agents/skills/using-open-browser-use/SKILL.md", "# Mac browser")
	writeOrbConfigTestFile(t, home, ".local/share/gh/extensions/gh-demo/manifest.yml", fmt.Sprintf("owner: octo\nname: gh-demo\nhost: github.com\ntag: %s\nispinned: true\npath: %s/.local/share/gh/extensions/gh-demo/gh-demo\n", commit, home))
	writeOrbConfigTestFile(t, home, ".local/share/gh/extensions/gh-demo/gh-demo", string([]byte{0xcf, 0xfa, 0xed, 0xfe})+"MacBinary")
	if err := os.Symlink(filepath.Join(home, ".config", "agents", "checks", "review.md"), filepath.Join(home, ".config", "agents", "checks", "linked.md")); err != nil {
		t.Fatal(err)
	}

	first, err := collectOrbConfigBundle(home)
	if err != nil {
		t.Fatal(err)
	}
	second, err := collectOrbConfigBundle(home)
	if err != nil {
		t.Fatal(err)
	}
	if first.Digest != second.Digest || !reflect.DeepEqual(first, second) {
		t.Fatalf("collection is not deterministic:\n%#v\n%#v", first, second)
	}
	_, decoded, err := orbconfig.Validate(first)
	if err != nil {
		t.Fatal(err)
	}
	wantFiles := map[string]string{
		"checks/review.md":         "review",
		"skills/demo/SKILL.md":     "---\nname: demo\n---\n",
		"skills/demo/reference.md": "reference",
	}
	if len(decoded) != len(wantFiles) {
		t.Fatalf("decoded files = %#v", decoded)
	}
	for filePath, want := range wantFiles {
		if string(decoded[filePath]) != want {
			t.Fatalf("file %s = %q, want %q", filePath, decoded[filePath], want)
		}
	}
	if !reflect.DeepEqual(first.Extensions, []orbconfig.Extension{{Repo: "octo/gh-demo", Version: commit}}) {
		t.Fatalf("extensions = %#v", first.Extensions)
	}
	raw, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{home, "/Users/", "MacBinary", base64.StdEncoding.EncodeToString([]byte{0xcf, 0xfa, 0xed, 0xfe})} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatalf("bundle leaked %q: %s", forbidden, raw)
		}
	}
}

func TestCollectOrbConfigBundleExcludesWholeSkillForLateNonportableFile(t *testing.T) {
	home := t.TempDir()
	writeOrbConfigTestFile(t, home, ".config/agents/skills/portable/SKILL.md", "---\nname: portable\n---\n")
	writeOrbConfigTestFile(t, home, ".config/agents/skills/portable/reference.md", "portable")
	writeOrbConfigTestFile(t, home, ".config/agents/skills/rejected/SKILL.md", "---\nname: rejected\n---\n")
	writeOrbConfigTestFile(t, home, ".config/agents/skills/rejected/a-reference.md", "would otherwise be included")
	writeOrbConfigTestFile(t, home, ".config/agents/skills/rejected/z-reference.md", "-----BEGIN PRIVATE KEY-----\nnot-a-real-key\n-----END PRIVATE KEY-----")

	bundle, err := collectOrbConfigBundle(home)
	if err != nil {
		t.Fatal(err)
	}
	_, decoded, err := orbconfig.Validate(bundle)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded) != 2 || string(decoded["skills/portable/reference.md"]) != "portable" {
		t.Fatalf("portable skill files = %#v", decoded)
	}
	for filePath := range decoded {
		if strings.HasPrefix(filePath, "skills/rejected/") {
			t.Fatalf("late nonportable skill file %q was included", filePath)
		}
	}
}

func TestCollectGHGitHubExtensionsNormalizesManifests(t *testing.T) {
	root := t.TempDir()
	alphaCommit := strings.Repeat("a", 40)
	zedCommit := strings.Repeat("b", 40)
	writeOrbConfigTestFile(t, root, "z/manifest.yml", fmt.Sprintf("owner: Team\nname: gh-Zed\nhost: GITHUB.COM\ntag: %s\nispinned: true\npath: /Users/me/bin\n", zedCommit))
	writeOrbConfigTestFile(t, root, "a/manifest.yml", fmt.Sprintf("owner: team\nname: gh-alpha\nhost: github.com\ntag: %s\nispinned: true\n", alphaCommit))
	writeOrbConfigTestFile(t, root, "semver/manifest.yml", "owner: team\nname: gh-semver\nhost: github.com\ntag: v1.2.3\nispinned: true\n")
	writeOrbConfigTestFile(t, root, "mutable/manifest.yml", "owner: team\nname: gh-mutable\nhost: github.com\ntag: main\nispinned: true\n")
	writeOrbConfigTestFile(t, root, "invalid-name/manifest.yml", fmt.Sprintf("owner: team\nname: alpha\nhost: github.com\ntag: %s\nispinned: true\n", alphaCommit))
	writeOrbConfigTestFile(t, root, "unpinned/manifest.yml", fmt.Sprintf("owner: team\nname: gh-unpinned\nhost: github.com\ntag: %s\nispinned: false\n", alphaCommit))
	writeOrbConfigTestFile(t, root, "unmarked/manifest.yml", fmt.Sprintf("owner: team\nname: gh-unmarked\nhost: github.com\ntag: %s\n", alphaCommit))
	writeOrbConfigTestFile(t, root, "other/manifest.yml", fmt.Sprintf("owner: team\nname: gh-ignored\nhost: gitlab.com\ntag: %s\nispinned: true\n", alphaCommit))
	extensions, err := collectGHGitHubExtensions(root)
	if err != nil {
		t.Fatal(err)
	}
	want := []orbconfig.Extension{
		{Repo: "team/gh-alpha", Version: alphaCommit},
		{Repo: "team/gh-mutable", Version: "main"},
		{Repo: "team/gh-semver", Version: "v1.2.3"},
		{Repo: "Team/gh-Zed", Version: zedCommit},
	}
	if !reflect.DeepEqual(extensions, want) {
		t.Fatalf("extensions = %#v, want %#v", extensions, want)
	}
}

func TestBrokerUploadsOrbConfigOnlyOnDigestMismatch(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	writeOrbConfigTestFile(t, home, ".config/agents/checks/review.md", "review")
	var currentDigest string
	requests := make([]string, 0, 5)
	heartbeatDigestFields := make([]bool, 0, 3)
	heartbeatDigests := make([]string, 0, 3)
	uploadDigests := make([]string, 0, 2)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		requests = append(requests, request.Method+" "+request.URL.Path)
		response.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case heartbeatEndpoint:
			var heartbeat map[string]any
			if err := json.NewDecoder(request.Body).Decode(&heartbeat); err != nil {
				t.Errorf("decode heartbeat: %v", err)
			}
			heartbeatDigest, advertised := heartbeat["orbConfigDigest"].(string)
			heartbeatDigestFields = append(heartbeatDigestFields, advertised)
			heartbeatDigests = append(heartbeatDigests, heartbeatDigest)
			response.Header().Set(orbConfigSupportHeader, "1")
			response.Header().Set(orbConfigDigestHeader, currentDigest)
			if advertised {
				_, _ = fmt.Fprintf(response, `{"ok":true,"runners":[],"orbConfigDigest":%q}`, currentDigest)
			} else {
				_, _ = fmt.Fprint(response, `{"ok":true,"runners":[]}`)
			}
		case orbConfigBundleEndpoint:
			var upload orbConfigUploadRequest
			if err := json.NewDecoder(request.Body).Decode(&upload); err != nil {
				t.Errorf("decode upload: %v", err)
				response.WriteHeader(http.StatusBadRequest)
				return
			}
			currentDigest = upload.Bundle.Digest
			uploadDigests = append(uploadDigests, currentDigest)
			_, _ = fmt.Fprintf(response, `{"ok":true,"digest":%q}`, currentDigest)
		default:
			response.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	localBroker := &broker{
		config:             &brokerConfig{BrokerID: "broker", APIURL: server.URL, apiKey: "key", Workspaces: []workspaceConfig{}},
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
	if err := localBroker.performHeartbeat(t.Context()); err != nil {
		t.Fatal(err)
	}
	writeOrbConfigTestFile(t, home, ".config/agents/checks/review.md", "review-updated")
	if err := localBroker.performHeartbeat(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := localBroker.performHeartbeat(t.Context()); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"POST " + heartbeatEndpoint,
		"PUT " + orbConfigBundleEndpoint,
		"POST " + heartbeatEndpoint,
		"PUT " + orbConfigBundleEndpoint,
		"POST " + heartbeatEndpoint,
	}
	if !reflect.DeepEqual(requests, want) {
		t.Fatalf("requests = %#v, want %#v", requests, want)
	}
	if !reflect.DeepEqual(heartbeatDigestFields, []bool{false, true, true}) {
		t.Fatalf("heartbeat digest negotiation = %#v", heartbeatDigestFields)
	}
	if len(uploadDigests) != 2 || uploadDigests[0] == uploadDigests[1] {
		t.Fatalf("uploaded digests = %#v", uploadDigests)
	}
	if !reflect.DeepEqual(heartbeatDigests, []string{"", uploadDigests[1], uploadDigests[1]}) {
		t.Fatalf("heartbeat digests = %#v, uploads = %#v", heartbeatDigests, uploadDigests)
	}
}

func TestBrokerOrbConfigRemainsLegacyWithoutServerCapability(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	writeOrbConfigTestFile(t, home, ".config/agents/skills", "not-a-directory")
	heartbeatDigestFields := make([]bool, 0, 2)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != heartbeatEndpoint {
			t.Errorf("unexpected request: %s %s", request.Method, request.URL.Path)
			response.WriteHeader(http.StatusNotFound)
			return
		}
		var heartbeat map[string]any
		if err := json.NewDecoder(request.Body).Decode(&heartbeat); err != nil {
			t.Errorf("decode heartbeat: %v", err)
		}
		_, advertised := heartbeat["orbConfigDigest"]
		heartbeatDigestFields = append(heartbeatDigestFields, advertised)
		response.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(response, `{"ok":true,"runners":[]}`)
	}))
	t.Cleanup(server.Close)
	localBroker := &broker{
		config:             &brokerConfig{BrokerID: "broker", APIURL: server.URL, apiKey: "key", Workspaces: []workspaceConfig{}},
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
	if err := localBroker.performHeartbeat(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := localBroker.performHeartbeat(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(heartbeatDigestFields, []bool{false, false}) {
		t.Fatalf("legacy heartbeat digest negotiation = %#v", heartbeatDigestFields)
	}
}

func TestBrokerOrbConfigRecoversFromServerDowngrade(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	writeOrbConfigTestFile(t, home, ".config/agents/checks/review.md", "review")
	heartbeatDigestFields := make([]bool, 0, 3)
	heartbeatCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case heartbeatEndpoint:
			heartbeatCount++
			var heartbeat map[string]any
			if err := json.NewDecoder(request.Body).Decode(&heartbeat); err != nil {
				t.Errorf("decode heartbeat: %v", err)
			}
			_, advertised := heartbeat["orbConfigDigest"]
			heartbeatDigestFields = append(heartbeatDigestFields, advertised)
			if heartbeatCount == 1 {
				response.Header().Set(orbConfigSupportHeader, "1")
				response.Header().Set(orbConfigDigestHeader, "")
				_, _ = fmt.Fprint(response, `{"ok":true,"runners":[]}`)
				return
			}
			if advertised {
				response.WriteHeader(http.StatusBadRequest)
				_, _ = fmt.Fprint(response, `{"ok":false,"error":{"code":"invalid_request","message":"unsupported field","detail":{"field":"orbConfigDigest"}}}`)
				return
			}
			_, _ = fmt.Fprint(response, `{"ok":true,"runners":[]}`)
		case orbConfigBundleEndpoint:
			var upload orbConfigUploadRequest
			if err := json.NewDecoder(request.Body).Decode(&upload); err != nil {
				t.Errorf("decode upload: %v", err)
				response.WriteHeader(http.StatusBadRequest)
				return
			}
			_, _ = fmt.Fprintf(response, `{"ok":true,"digest":%q}`, upload.Bundle.Digest)
		default:
			response.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	localBroker := &broker{
		config:             &brokerConfig{BrokerID: "broker", APIURL: server.URL, apiKey: "key", Workspaces: []workspaceConfig{}},
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
	if err := localBroker.performHeartbeat(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := localBroker.performHeartbeat(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(heartbeatDigestFields, []bool{false, true, false}) {
		t.Fatalf("downgrade heartbeat digest negotiation = %#v", heartbeatDigestFields)
	}
	if localBroker.orbConfigSupport {
		t.Fatal("legacy retry retained orb configuration support")
	}
}

func TestBrokerOrbConfigDoesNotRetryUnrelatedHeartbeatFailure(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	writeOrbConfigTestFile(t, home, ".config/agents/checks/review.md", "review")
	heartbeats := 0
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != heartbeatEndpoint {
			response.WriteHeader(http.StatusNotFound)
			return
		}
		heartbeats++
		response.Header().Set("Content-Type", "application/json")
		response.WriteHeader(http.StatusInternalServerError)
		_, _ = fmt.Fprint(response, `{"ok":false,"error":"server_failure"}`)
	}))
	t.Cleanup(server.Close)
	localBroker := &broker{
		config:             &brokerConfig{BrokerID: "broker", APIURL: server.URL, apiKey: "key", Workspaces: []workspaceConfig{}},
		client:             server.Client(),
		requestLimit:       time.Second,
		sessionID:          "session",
		sessionGeneration:  1,
		hostname:           "Mac",
		pid:                123,
		orbConfigSupport:   true,
		workspacesByRunner: map[string]*workspaceConfig{},
		children:           map[childKey]*childProcess{},
		launches:           map[childKey]*childLaunch{},
	}
	if err := localBroker.performHeartbeat(t.Context()); err == nil || !strings.Contains(err.Error(), "server_failure") {
		t.Fatalf("heartbeat error = %v", err)
	}
	if heartbeats != 1 {
		t.Fatalf("heartbeats = %d, want 1", heartbeats)
	}
	if !localBroker.orbConfigSupport {
		t.Fatal("unrelated failure disabled orb configuration support")
	}
}

func TestOrbConfigDigestUnsupportedMatrix(t *testing.T) {
	for _, test := range []struct {
		name   string
		status int
		body   string
		want   bool
	}{
		{name: "dedicated top-level code", status: http.StatusBadRequest, body: `{"code":"orb_config_digest_unsupported"}`, want: true},
		{name: "dedicated string error", status: http.StatusBadRequest, body: `{"error":"unsupported_orb_config_digest"}`, want: true},
		{name: "dedicated object error", status: http.StatusBadRequest, body: `{"error":{"code":"orb_config_digest_not_supported"}}`, want: true},
		{name: "invalid request top-level message", status: http.StatusBadRequest, body: `{"code":"invalid_request","message":"unknown field orbConfigDigest"}`, want: true},
		{name: "invalid request nested message", status: http.StatusBadRequest, body: `{"error":{"code":"invalid_request","message":"orbConfigDigest is unsupported"}}`, want: true},
		{name: "invalid request nested structured detail", status: http.StatusBadRequest, body: `{"error":{"code":"invalid_request","detail":{"field":"orbConfigDigest"}}}`, want: true},
		{name: "unrelated invalid request", status: http.StatusBadRequest, body: `{"code":"invalid_request","message":"runner payload is invalid"}`},
		{name: "unrelated nested invalid request", status: http.StatusBadRequest, body: `{"error":{"code":"invalid_request","detail":"runner payload is invalid"}}`},
		{name: "digest evidence with unrelated code", status: http.StatusBadRequest, body: `{"code":"runner_invalid","message":"orbConfigDigest is unsupported"}`},
		{name: "wrong status", status: http.StatusConflict, body: `{"error":{"code":"invalid_request","message":"orbConfigDigest is unsupported"}}`},
		{name: "unrelated code", status: http.StatusBadRequest, body: `{"error":{"code":"invalid_digest"}}`},
		{name: "missing code", status: http.StatusBadRequest, body: `{"error":{"message":"orbConfigDigest is unsupported"}}`},
		{name: "invalid JSON", status: http.StatusBadRequest, body: `{"error":`},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := orbConfigDigestUnsupported(test.status, []byte(test.body)); got != test.want {
				t.Fatalf("orbConfigDigestUnsupported(%d, %s) = %t, want %t", test.status, test.body, got, test.want)
			}
		})
	}
}

func TestPutOrbConfigBundleUsesCallerContext(t *testing.T) {
	bundle, err := orbconfig.New(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	var requestContext context.Context
	client := &http.Client{Transport: brokerRoundTripper(func(request *http.Request) (*http.Response, error) {
		requestContext = request.Context()
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(fmt.Sprintf(`{"ok":true,"digest":%q}`, bundle.Digest))),
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
	if err := localBroker.putOrbConfigBundle(callerContext, bundle); err != nil {
		t.Fatal(err)
	}
	assertBrokerCallerContext(t, requestContext, cancel)
}

func writeOrbConfigTestFile(t *testing.T, root, relative, content string) {
	t.Helper()
	fullPath := filepath.Join(root, filepath.FromSlash(relative))
	if err := os.MkdirAll(filepath.Dir(fullPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fullPath, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}
