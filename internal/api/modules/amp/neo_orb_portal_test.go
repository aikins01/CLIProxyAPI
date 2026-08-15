package amp

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
)

func newNeoOrbPortalTestModule(t *testing.T) (*AmpModule, *neoOrbFakeProvider, string) {
	t.Helper()
	rt, fake := newNeoOrbTestRuntime(t)
	threadID := "T-019fdec9-b0cf-745d-8da4-f250184e870e"
	actor := rt.store.ensureThreadActor(threadID)
	t.Cleanup(actor.cancel)
	manager := rt.orbManagerFor()
	manager.orbs[threadID] = &neoOrbRecord{threadID: threadID, containerID: "container-fake", state: neoOrbStateRunning, workDir: neoOrbWorkDir, portalToken: strings.Repeat("a", neoOrbPortalTokenByteCount)}
	return &AmpModule{neoRuntime: rt}, fake, threadID
}

func neoOrbPortalRequest(t *testing.T, module *AmpModule, path string, headers map[string]string) *neoOrbPortalResponse {
	return neoOrbPortalRequestContext(t, context.Background(), module, path, headers)
}

func neoOrbPortalRequestContext(t *testing.T, ctx context.Context, module *AmpModule, path string, headers map[string]string) *neoOrbPortalResponse {
	t.Helper()
	router := gin.New()
	router.Use(func(c *gin.Context) {
		if key := c.GetHeader("X-Neo-Test-Client-API-Key"); key != "" {
			c.Request = c.Request.WithContext(context.WithValue(c.Request.Context(), clientAPIKeyContextKey{}, key))
		}
		c.Next()
	})
	router.Any("/orb/:threadID/p/:port/*path", module.serveOrbPortal)
	server := httptest.NewServer(router)
	t.Cleanup(server.Close)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+path, nil)
	if err != nil {
		t.Fatalf("portal request: %v", err)
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	if key := getClientAPIKeyFromContext(ctx); key != "" {
		req.Header.Set("X-Neo-Test-Client-API-Key", key)
	}
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatalf("portal round trip: %v", err)
	}
	defer func() {
		if errClose := resp.Body.Close(); errClose != nil {
			t.Logf("portal response body close: %v", errClose)
		}
	}()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("portal read body: %v", err)
	}
	return &neoOrbPortalResponse{status: resp.StatusCode, header: resp.Header, body: string(body)}
}

func TestNeoOrbPortalUsesAuthenticatedRequestOwner(t *testing.T) {
	module, fake, threadID := newNeoOrbPortalTestModule(t)
	fake.inspectState = neoOrbContainerState{Exists: true, Running: true, IPAddress: "172.17.0.11"}
	actor := module.neoRuntime.store.lookupThreadActor(threadID)
	actor.mu.Lock()
	actor.meta["creatorUserID"] = "user-test"
	actor.meta["ownerUserId"] = "user-test"
	actor.mu.Unlock()

	cfg := *module.neoRuntime.configSnapshot()
	cfg.AmpCode.UpstreamURL = "https://amp.invalid"
	cfg.AmpCode.UpstreamAPIKey = "upstream-key"
	if err := module.neoRuntime.updateConfig(&cfg); err != nil {
		t.Fatalf("update config: %v", err)
	}
	module.neoRuntime.setSecretSource(NewStaticSecretSource("upstream-key"))

	upstreamPortal := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("owned portal"))
	}))
	t.Cleanup(upstreamPortal.Close)
	original := neoOrbPortalTargetURL
	neoOrbPortalTargetURL = func(containerIP string, port int) (string, error) {
		return upstreamPortal.URL, nil
	}
	t.Cleanup(func() { neoOrbPortalTargetURL = original })

	ctx := context.WithValue(context.Background(), clientAPIKeyContextKey{}, "client-key")
	cacheKey, err := module.neoRuntime.recentThreadSeedKey(ctx)
	if err != nil {
		t.Fatalf("user profile cache key: %v", err)
	}
	module.neoRuntime.mu.Lock()
	module.neoRuntime.webLocalUserProfiles[cacheKey] = neoWebLocalUserProfile{fetchedAt: time.Now(), user: map[string]any{"id": "user-test"}}
	module.neoRuntime.mu.Unlock()
	if owner := module.neoRuntime.neoRequestOwnerUserID(ctx); owner != "user-test" {
		t.Fatalf("request owner = %q", owner)
	}
	recorder := neoOrbPortalRequestContext(t, ctx, module, "/orb/"+threadID+"/p/3000/", nil)
	if recorder.status != http.StatusOK || recorder.body != "owned portal" {
		t.Fatalf("authenticated portal response = %d %q", recorder.status, recorder.body)
	}
}

type neoOrbPortalResponse struct {
	status int
	header http.Header
	body   string
}

func TestNeoOrbPortalTokenMiddlewareAuthenticatesAndStripsToken(t *testing.T) {
	module, _, threadID := newNeoOrbPortalTestModule(t)
	token := strings.Repeat("a", neoOrbPortalTokenByteCount)
	router := gin.New()
	authCalls := 0
	auth := func(c *gin.Context) {
		authCalls++
		if c.GetHeader("Authorization") != "Bearer existing-key" {
			c.AbortWithStatus(http.StatusUnauthorized)
			return
		}
		c.Set("userApiKey", "existing-key")
		c.Next()
	}
	router.GET("/orb/:threadID/p/:port/*path", module.orbPortalTokenMiddleware(), wrapOrbPortalAuth(auth), clientAPIKeyMiddleware(), func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"authorization": c.GetHeader("Authorization"),
			"clientKey":     getClientAPIKeyFromContext(c.Request.Context()),
			"owner":         module.neoRuntime.neoRequestOwnerUserID(c.Request.Context()),
			"query":         c.Request.URL.RawQuery,
		})
	})
	req := httptest.NewRequest(http.MethodGet, "/orb/"+threadID+"/p/3000/app?x=1&"+neoOrbPortalTokenQuery+"="+token, nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusFound || rec.Header().Get("Location") != "/orb/"+threadID+"/p/3000/app?x=1" {
		t.Fatalf("token exchange response = %d location=%q", rec.Code, rec.Header().Get("Location"))
	}
	cookies := rec.Result().Cookies()
	if len(cookies) != 1 || cookies[0].Name != neoOrbPortalCookieName || !cookies[0].HttpOnly || cookies[0].Path != "/orb/"+threadID+"/p/3000/" {
		t.Fatalf("portal cookies = %#v", cookies)
	}
	req = httptest.NewRequest(http.MethodGet, "/orb/"+threadID+"/p/3000/app?x=1", nil)
	req.AddCookie(cookies[0])
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	var body map[string]any
	if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &body) != nil {
		t.Fatalf("token middleware response = %d %q", rec.Code, rec.Body.String())
	}
	if body["authorization"] != "" || body["clientKey"] != neoOrbPortalPrincipal || body["owner"] != neoLocalOwnerUserID || body["query"] != "x=1" || authCalls != 0 {
		t.Fatalf("token middleware body = %#v", body)
	}
	req = httptest.NewRequest(http.MethodGet, "/orb/"+threadID+"/p/3000/app?"+neoOrbPortalTokenQuery+"="+strings.Repeat("b", neoOrbPortalTokenByteCount), nil)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("invalid capability status = %d body=%s", rec.Code, rec.Body.String())
	}
	req = httptest.NewRequest(http.MethodGet, "/orb/"+threadID+"/p/3000/app?x=1&"+neoOrbPortalTokenQuery+"=ignored", nil)
	req.Header.Set("Authorization", "Bearer existing-key")
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &body) != nil || body["authorization"] != "Bearer existing-key" || body["clientKey"] != "existing-key" || body["query"] != "x=1" || authCalls != 1 {
		t.Fatalf("existing authorization response = %d %#v", rec.Code, body)
	}
}

func TestNeoOrbPortalCapabilityBypassesManagementLocalhostRestriction(t *testing.T) {
	module, _, threadID := newNeoOrbPortalTestModule(t)
	module.setRestrictToLocalhost(true)
	token := strings.Repeat("a", neoOrbPortalTokenByteCount)
	router := gin.New()
	router.GET("/orb/:threadID/p/:port/*path", module.orbPortalTokenMiddleware(), wrapOrbPortalLocalhost(module.localhostOnlyMiddleware()), func(c *gin.Context) {
		c.Status(http.StatusNoContent)
	})
	req := httptest.NewRequest(http.MethodGet, "/orb/"+threadID+"/p/3000/?"+neoOrbPortalTokenQuery+"="+token, nil)
	req.RemoteAddr = "192.0.2.10:12345"
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusFound {
		t.Fatalf("capability exchange status = %d body=%s", rec.Code, rec.Body.String())
	}
	cookies := rec.Result().Cookies()
	if len(cookies) != 1 {
		t.Fatalf("capability cookies = %#v", cookies)
	}
	req = httptest.NewRequest(http.MethodGet, "/orb/"+threadID+"/p/3000/", nil)
	req.RemoteAddr = "192.0.2.10:12345"
	req.AddCookie(cookies[0])
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("remote capability status = %d body=%s", rec.Code, rec.Body.String())
	}
	req = httptest.NewRequest(http.MethodGet, "/orb/"+threadID+"/p/3000/", nil)
	req.RemoteAddr = "192.0.2.10:12345"
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("remote unauthenticated status = %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestNeoOrbPortalTokenMiddlewareRejectsRecoveredLegacyCapability(t *testing.T) {
	module, fake, threadID := newNeoOrbPortalTestModule(t)
	token := strings.Repeat("r", neoOrbPortalTokenByteCount)
	writeNeoOrbPersistedThread(t, module.neoRuntime, threadID, "sandbox", "container-fake")
	actor := module.neoRuntime.store.lookupThreadActor(threadID)
	module.neoRuntime.store.delete(actor.id)
	manager := module.neoRuntime.orbManagerFor()
	manager.mu.Lock()
	manager.orbs = map[string]*neoOrbRecord{}
	manager.recovered = false
	manager.recoveredKey = ""
	manager.mu.Unlock()
	fake.containers = []neoOrbContainerSummary{{ID: "container-fake", Labels: map[string]string{"cliproxy.orb": threadID, neoOrbPortalTokenLabel: token}}}
	fake.inspectState = neoOrbContainerState{Exists: true, Running: true}

	router := gin.New()
	router.GET("/orb/:threadID/p/:port/*path", module.orbPortalTokenMiddleware(), func(c *gin.Context) {
		c.Status(http.StatusNoContent)
	})
	req := httptest.NewRequest(http.MethodGet, "/orb/"+threadID+"/p/3000/?"+neoOrbPortalTokenQuery+"="+token, nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("recovered portal capability status = %d body=%s", rec.Code, rec.Body.String())
	}
	loaded := module.neoRuntime.store.lookupThreadActor(threadID)
	if loaded != nil {
		t.Fatal("recovered portal capability imported the persisted actor")
	}
	if fake.callCount("inspect:") != 0 {
		t.Fatalf("recovered portal capability accessed container: %#v", fake.calls)
	}
}

func TestNeoOrbPortalHelperWritesLocalManifest(t *testing.T) {
	dir := t.TempDir()
	helper := filepath.Join(dir, "amp-orb-portal")
	if err := os.WriteFile(helper, []byte(neoOrbPortalHelperScript()), 0o755); err != nil {
		t.Fatalf("write helper: %v", err)
	}
	threadID := "T-019fdec9-b0cf-745d-8da4-f250184e870e"
	cmd := exec.Command(helper, "http://localhost:8765/app?q=1", "--name", "preview", "--title", "Preview", "--description", "Use test data")
	cmd.Dir = dir
	token := strings.Repeat("p", neoOrbPortalTokenByteCount)
	cmd.Env = append(os.Environ(), "AMP_THREAD_ID="+threadID, "AMP_ORB_PORTAL_BASE_URL=https://amp.example.test", "AMP_ORB_PORTAL_TOKEN="+token)
	output, err := cmd.Output()
	if err != nil {
		t.Fatalf("run helper: %v", err)
	}
	wantURL := "https://amp.example.test/orb/" + threadID + "/p/8765/app?q=1&" + neoOrbPortalTokenQuery + "=" + token
	if !strings.Contains(string(output), wantURL) {
		t.Fatalf("helper output = %q, want %q", output, wantURL)
	}
	manifest, err := os.ReadFile(filepath.Join(dir, ".amp", "portals", "preview.json"))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var decoded map[string]any
	if json.Unmarshal(manifest, &decoded) != nil {
		t.Fatalf("manifest = %q", manifest)
	}
	links := arrayValue(decoded["links"])
	if len(links) != 1 || stringValue(mapValue(links[0])["label"]) != "Preview" || stringValue(mapValue(links[0])["note"]) != "Use test data" || stringValue(mapValue(links[0])["url"]) != wantURL {
		t.Fatalf("manifest links = %#v", links)
	}
	manifestInfo, err := os.Stat(filepath.Join(dir, ".amp", "portals", "preview.json"))
	if err != nil {
		t.Fatalf("stat manifest: %v", err)
	}
	if manifestInfo.Mode().Perm() != 0o600 {
		t.Fatalf("manifest mode = %v", manifestInfo.Mode().Perm())
	}
	printOnly := exec.Command(helper, "8766", "--name", "print-only", "--no-manifest")
	printOnly.Dir = dir
	printOnly.Env = cmd.Env
	if output, err := printOnly.CombinedOutput(); err != nil || !strings.Contains(string(output), "/p/8766/") {
		t.Fatalf("print-only portal err=%v output=%q", err, output)
	}
	if _, err := os.Stat(filepath.Join(dir, ".amp", "portals", "print-only.json")); !os.IsNotExist(err) {
		t.Fatalf("print-only portal published a manifest: %v", err)
	}
	unsupported := exec.Command(helper, "https://localhost:8765/", "--name", "secure")
	unsupported.Dir = dir
	unsupported.Env = cmd.Env
	if output, err := unsupported.CombinedOutput(); err == nil || !strings.Contains(string(output), "target URL scheme must be http") {
		t.Fatalf("unsupported scheme err=%v output=%q", err, output)
	}
}

func TestNeoOrbServiceHelperSupportsSupervisedLifecycle(t *testing.T) {
	script := neoOrbServiceHelperScript()
	for _, want := range []string{"supervisord", "autorestart=true", "PUBLIC_URL", "amp-orb-portal", "restart", "status", "logs", "list"} {
		if !strings.Contains(script, want) {
			t.Fatalf("service helper missing %q", want)
		}
	}
	helper := filepath.Join(t.TempDir(), "amp-orb-service")
	if err := os.WriteFile(helper, []byte(script), 0o755); err != nil {
		t.Fatalf("write service helper: %v", err)
	}
	if output, err := exec.Command("python3", "-m", "py_compile", helper).CombinedOutput(); err != nil {
		t.Fatalf("compile service helper: %v: %s", err, output)
	}
	binDir := filepath.Join(filepath.Dir(helper), "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatalf("create fake bin: %v", err)
	}
	ctl := filepath.Join(binDir, "supervisorctl")
	if err := os.WriteFile(ctl, []byte("#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$HOME/ctl-calls\"\nfor arg in \"$@\"; do\n  case \"$arg\" in\n    pid) exit 0 ;;\n    status) printf 'multiline RUNNING pid 1, uptime 0:00:01\\n'; exit 0 ;;\n  esac\ndone\nexit 0\n"), 0o755); err != nil {
		t.Fatalf("write fake supervisorctl: %v", err)
	}
	home := filepath.Join(filepath.Dir(helper), "home")
	command := "printf 'first\\nsecond\\n'\nprintf 'third\\n'"
	start := exec.Command(helper, "start", "multiline", "--command", command, "--port", "8787")
	start.Env = append(os.Environ(), "HOME="+home, "PATH="+binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	if output, err := start.CombinedOutput(); err != nil {
		t.Fatalf("start multiline service: %v: %s", err, output)
	}
	serviceCommand := filepath.Join(home, ".cache", "amp", "services", "commands", "multiline")
	output, err := exec.Command(serviceCommand).CombinedOutput()
	if err != nil || string(output) != "first\nsecond\nthird\n" {
		t.Fatalf("multiline service command: err=%v output=%q", err, output)
	}
	conf, err := os.ReadFile(filepath.Join(home, ".cache", "amp", "services", "conf.d", "multiline.conf"))
	if err != nil {
		t.Fatalf("read multiline service config: %v", err)
	}
	if strings.Contains(string(conf), "second") || !strings.Contains(string(conf), "command="+serviceCommand) {
		t.Fatalf("multiline service config = %q", conf)
	}
	restart := exec.Command(helper, "start", "multiline", "--command", "printf 'changed\\n'", "--port", "8787")
	restart.Env = start.Env
	if output, err := restart.CombinedOutput(); err != nil {
		t.Fatalf("restart changed service: %v: %s", err, output)
	}
	ctlCalls, err := os.ReadFile(filepath.Join(home, "ctl-calls"))
	if err != nil || !strings.Contains(string(ctlCalls), "restart multiline") {
		t.Fatalf("changed command did not restart service: err=%v calls=%q", err, ctlCalls)
	}
	logPath := filepath.Join(home, ".cache", "amp", "services", "multiline.log")
	if err := os.WriteFile(logPath, []byte("secret log\n"), 0o600); err != nil {
		t.Fatalf("write service log: %v", err)
	}
	logs := exec.Command(helper, "logs", "multiline", "--lines", "0")
	logs.Env = start.Env
	if output, err := logs.CombinedOutput(); err != nil || len(output) != 0 {
		t.Fatalf("zero-line logs err=%v output=%q", err, output)
	}
}

func TestNeoOrbServiceHelperSerializesStateAndParsesStatus(t *testing.T) {
	dir := t.TempDir()
	helper := filepath.Join(dir, "amp-orb-service")
	if err := os.WriteFile(helper, []byte(neoOrbServiceHelperScript()), 0o755); err != nil {
		t.Fatalf("write service helper: %v", err)
	}
	binDir := filepath.Join(dir, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatalf("create fake bin: %v", err)
	}
	ctl := filepath.Join(binDir, "supervisorctl")
	ctlScript := `#!/bin/sh
printf '%s\n' "$*" >> "$HOME/ctl-calls"
last=""
has_status=false
for arg in "$@"; do
  [ "$arg" = pid ] && exit 0
  [ "$arg" = status ] && has_status=true
  last="$arg"
done
if "$has_status"; then
  if [ "$last" = RUNNING-app ]; then
    printf 'RUNNING-app FATAL exited too quickly\n'
  else
    printf '%s RUNNING pid 1, uptime 0:00:01\n' "$last"
  fi
fi
exit 0
`
	if err := os.WriteFile(ctl, []byte(ctlScript), 0o755); err != nil {
		t.Fatalf("write fake supervisorctl: %v", err)
	}
	portalHelper := filepath.Join(binDir, "amp-orb-portal")
	if err := os.WriteFile(portalHelper, []byte(neoOrbPortalHelperScript()), 0o755); err != nil {
		t.Fatalf("write portal helper: %v", err)
	}
	home := filepath.Join(dir, "home")
	threadID := "T-019fdec9-b0cf-745d-8da4-f250184e870e"
	portalToken := strings.Repeat("p", neoOrbPortalTokenByteCount)
	environment := append(os.Environ(), "HOME="+home, "PATH="+binDir+string(os.PathListSeparator)+os.Getenv("PATH"), "AMP_THREAD_ID="+threadID, "AMP_ORB_PORTAL_BASE_URL=https://amp.example.test", "AMP_ORB_PORTAL_TOKEN="+portalToken)
	commands := []*exec.Cmd{
		exec.Command(helper, "start", "alpha", "--command", "sleep 1", "--port", "8801"),
		exec.Command(helper, "start", "beta", "--command", "sleep 1", "--port", "8802"),
	}
	for _, command := range commands {
		command.Env = environment
		if err := command.Start(); err != nil {
			t.Fatalf("start concurrent helper: %v", err)
		}
	}
	for _, command := range commands {
		if err := command.Wait(); err != nil {
			t.Fatalf("wait concurrent helper: %v", err)
		}
	}
	stateRaw, err := os.ReadFile(filepath.Join(home, ".cache", "amp", "services", "state.json"))
	if err != nil {
		t.Fatalf("read service state: %v", err)
	}
	var state map[string]any
	if err := json.Unmarshal(stateRaw, &state); err != nil || len(state) != 2 || mapValue(state["alpha"])["port"] != float64(8801) || mapValue(state["beta"])["port"] != float64(8802) {
		t.Fatalf("serialized state err=%v state=%#v", err, state)
	}
	portalWorkDir := filepath.Join(dir, "workspace")
	if err := os.MkdirAll(portalWorkDir, 0o755); err != nil {
		t.Fatalf("create portal workspace: %v", err)
	}
	nearMatch := exec.Command(helper, "start", "RUNNING-app", "--command", "exit 1", "--port", "8803", "--portal")
	nearMatch.Env = environment
	nearMatch.Dir = portalWorkDir
	if output, err := nearMatch.CombinedOutput(); err == nil || !strings.Contains(string(output), "RUNNING-app FATAL") {
		t.Fatalf("near-match status err=%v output=%q", err, output)
	}
	if _, err := os.Stat(filepath.Join(portalWorkDir, ".amp", "portals", "RUNNING-app.json")); !os.IsNotExist(err) {
		t.Fatalf("failed service published a portal manifest: %v", err)
	}
	failingPortal := `#!/usr/bin/env python3
import os
import sys
if "--no-manifest" in sys.argv:
    os.rename(__file__, __file__ + ".unavailable")
    print("https://amp.example.test/orb/test/p/8805/")
    raise SystemExit(0)
raise SystemExit(42)
`
	if err := os.WriteFile(portalHelper, []byte(failingPortal), 0o755); err != nil {
		t.Fatalf("write failing portal helper: %v", err)
	}
	portalFailure := exec.Command(helper, "start", "portal-failure", "--command", "sleep 1", "--port", "8805", "--portal")
	portalFailure.Env = environment
	portalFailure.Dir = portalWorkDir
	if output, err := portalFailure.CombinedOutput(); err == nil {
		t.Fatalf("manifest publication unexpectedly succeeded: %q", output)
	}
	for _, path := range []string{
		filepath.Join(home, ".cache", "amp", "services", "commands", "portal-failure"),
		filepath.Join(home, ".cache", "amp", "services", "conf.d", "portal-failure.conf"),
	} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("failed service artifact remains at %s: %v", path, err)
		}
	}
	ctlCalls, err := os.ReadFile(filepath.Join(home, "ctl-calls"))
	if err != nil || !strings.Contains(string(ctlCalls), "stop portal-failure") {
		t.Fatalf("failed manifest publication did not stop service: err=%v calls=%q", err, ctlCalls)
	}
	if err := os.WriteFile(portalHelper, []byte(neoOrbPortalHelperScript()), 0o755); err != nil {
		t.Fatalf("restore portal helper: %v", err)
	}
	portalSuccess := exec.Command(helper, "start", "portal-success", "--command", "sleep 1", "--port", "8804", "--portal")
	portalSuccess.Env = environment
	portalSuccess.Dir = portalWorkDir
	if output, err := portalSuccess.CombinedOutput(); err != nil || !strings.Contains(string(output), "Wrote portal manifest") {
		t.Fatalf("running service portal err=%v output=%q", err, output)
	}
	if _, err := os.Stat(filepath.Join(portalWorkDir, ".amp", "portals", "portal-success.json")); err != nil {
		t.Fatalf("running service portal manifest: %v", err)
	}
}

func TestNeoOrbPortalRejectsInvalidThreadAndMissingOrb(t *testing.T) {
	module, fake, threadID := newNeoOrbPortalTestModule(t)
	recorder := neoOrbPortalRequest(t, module, "/orb/not-a-thread/p/3000/", nil)
	if recorder.status != http.StatusNotFound {
		t.Fatalf("invalid thread status = %d", recorder.status)
	}
	recorder = neoOrbPortalRequest(t, module, "/orb/T-00000000-0000-4000-8000-000000000000/p/3000/", nil)
	if recorder.status != http.StatusNotFound {
		t.Fatalf("missing orb status = %d", recorder.status)
	}
	manager := module.neoRuntime.orbManagerFor()
	manager.orbs[threadID].state = neoOrbStatePaused
	fake.inspectState = neoOrbContainerState{Exists: true, Running: true, IPAddress: "172.17.0.12"}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("resumed portal"))
	}))
	t.Cleanup(upstream.Close)
	original := neoOrbPortalTargetURL
	neoOrbPortalTargetURL = func(containerIP string, port int) (string, error) {
		return upstream.URL, nil
	}
	t.Cleanup(func() { neoOrbPortalTargetURL = original })
	recorder = neoOrbPortalRequest(t, module, "/orb/"+threadID+"/p/3000/", nil)
	if recorder.status != http.StatusOK || recorder.body != "resumed portal" {
		t.Fatalf("paused orb response = %d %q", recorder.status, recorder.body)
	}
	if fake.callCount("unpause:container-fake") != 1 {
		t.Fatalf("paused orb calls = %#v", fake.calls)
	}
	recorder = neoOrbPortalRequest(t, module, "/orb/"+threadID+"/p/0/", nil)
	if recorder.status != http.StatusBadRequest {
		t.Fatalf("invalid port status = %d", recorder.status)
	}
}

func TestNeoOrbPortalProxiesToContainer(t *testing.T) {
	module, fake, threadID := newNeoOrbPortalTestModule(t)
	fake.inspectState = neoOrbContainerState{Exists: true, Running: true, IPAddress: "172.17.0.9"}

	requestCount := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		wantCookie := ""
		if requestCount == 2 {
			wantCookie = "session=abc"
		}
		if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != wantCookie {
			w.WriteHeader(http.StatusTeapot)
			return
		}
		http.SetCookie(w, &http.Cookie{Name: "session", Value: "abc", Path: "/"})
		w.Header().Set("X-Upstream", "yes")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(r.Method + " " + r.URL.RequestURI()))
	}))
	defer upstream.Close()

	original := neoOrbPortalTargetURL
	neoOrbPortalTargetURL = func(containerIP string, port int) (string, error) {
		if containerIP != "172.17.0.9" {
			t.Fatalf("portal container IP = %q", containerIP)
		}
		return upstream.URL, nil
	}
	t.Cleanup(func() { neoOrbPortalTargetURL = original })

	recorder := neoOrbPortalRequest(t, module, "/orb/"+threadID+"/p/3000/app/index.html?q=1", map[string]string{
		"Authorization": "Bearer secret",
		"Cookie":        "session=abc; " + neoOrbPortalCookieName + "=portal-secret",
	})
	if recorder.status != http.StatusOK {
		t.Fatalf("portal status = %d body=%s", recorder.status, recorder.body)
	}
	if recorder.header.Get("X-Upstream") != "yes" {
		t.Fatalf("missing upstream header: %#v", recorder.header)
	}
	responseCookies := (&http.Response{Header: recorder.header}).Cookies()
	prefix := neoOrbPortalAppCookiePrefix(threadID, 3000)
	if len(responseCookies) != 1 || responseCookies[0].Name != prefix+"session" || responseCookies[0].Path != "/orb/"+threadID+"/p/3000/" {
		t.Fatalf("portal response cookies = %#v", responseCookies)
	}
	if recorder.body != "GET /app/index.html?q=1" {
		t.Fatalf("proxied path = %q", recorder.body)
	}
	second := neoOrbPortalRequest(t, module, "/orb/"+threadID+"/p/3000/app/index.html", map[string]string{
		"Cookie": prefix + "session=abc; unrelated=secret; " + neoOrbPortalCookieName + "=portal-secret",
	})
	if second.status != http.StatusOK || requestCount != 2 {
		t.Fatalf("namespaced cookie request status=%d count=%d body=%s", second.status, requestCount, second.body)
	}
}

func TestNeoOrbPortalQuarantinesRecoveredContainerOnFirstRequest(t *testing.T) {
	module, fake, threadID := newNeoOrbPortalTestModule(t)
	manager := module.neoRuntime.orbManagerFor()
	manager.mu.Lock()
	manager.orbs = map[string]*neoOrbRecord{}
	manager.recovered = false
	manager.recoveredKey = ""
	manager.mu.Unlock()
	writeNeoOrbPersistedThread(t, module.neoRuntime, threadID, "sandbox", "recovered-container")
	fake.containers = []neoOrbContainerSummary{{ID: "recovered-container", Labels: map[string]string{"cliproxy.orb": threadID}}}
	fake.inspectState = neoOrbContainerState{Exists: true, Running: true, IPAddress: "172.17.0.10"}

	recorder := neoOrbPortalRequest(t, module, "/orb/"+threadID+"/p/3000/", nil)
	if recorder.status != http.StatusConflict || !strings.Contains(recorder.body, "migration approval") {
		t.Fatalf("recovered portal response = %d %q", recorder.status, recorder.body)
	}
	if record, ok := manager.snapshot(threadID); !ok || record.containerID != "recovered-container" || record.state != neoOrbStateConflict || !record.recovered {
		t.Fatalf("recovered portal record = %#v, %t", record, ok)
	}
	if fake.callCount("list-orbs") != 1 || fake.callCount("inspect:") != 0 {
		t.Fatalf("portal recovery calls = %#v", fake.calls)
	}
}

func TestNeoOrbPortalDisabledOrbs(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	module := &AmpModule{neoRuntime: rt}
	recorder := neoOrbPortalRequest(t, module, "/orb/T-00000000-0000-4000-8000-000000000000/p/3000/", nil)
	if recorder.status != http.StatusNotFound {
		t.Fatalf("disabled orbs portal status = %d body=%s", recorder.status, recorder.body)
	}
	if !strings.Contains(recorder.body, "not found") {
		t.Fatalf("disabled orbs body = %s", recorder.body)
	}
}

func TestNeoOrbPortalRequiresActionableLifecycle(t *testing.T) {
	for _, activation := range []string{"missing", neoOrbLifecycleActivationBinding, neoOrbLifecycleActivationLaunching, neoOrbLifecycleActivationActionable} {
		t.Run(activation, func(t *testing.T) {
			fixture := newNeoOrbLifecycleAdmissionFixture(t, activation)
			module := &AmpModule{neoRuntime: fixture.runtime}
			proxied := 0
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				proxied++
				_, _ = w.Write([]byte("actionable portal"))
			}))
			t.Cleanup(upstream.Close)
			original := neoOrbPortalTargetURL
			neoOrbPortalTargetURL = func(string, int) (string, error) { return upstream.URL, nil }
			t.Cleanup(func() { neoOrbPortalTargetURL = original })

			response := neoOrbPortalRequest(t, module, "/orb/"+fixture.record.threadID+"/p/3000/", nil)
			if activation == neoOrbLifecycleActivationActionable {
				if response.status != http.StatusOK || response.body != "actionable portal" || proxied != 1 {
					t.Fatalf("actionable portal response = %d %q proxied=%d", response.status, response.body, proxied)
				}
			} else if response.status < 400 || proxied != 0 {
				t.Fatalf("inert portal response = %d %q proxied=%d", response.status, response.body, proxied)
			}
			assertNeoOrbNoProviderMutation(t, fixture.fake)
		})
	}
}

func TestNeoOrbPortalFinalAdmissionRejectsLifecycleTransition(t *testing.T) {
	fixture := newNeoOrbLifecycleAdmissionFixture(t, neoOrbLifecycleActivationActionable)
	module := &AmpModule{neoRuntime: fixture.runtime}
	proxied := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		proxied++
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(upstream.Close)
	original := neoOrbPortalTargetURL
	neoOrbPortalTargetURL = func(string, int) (string, error) {
		active := fixture.store.snapshot().Threads[fixture.record.threadID].Active
		if active == nil || !neoOrbLifecycleActionable(active) {
			t.Fatalf("portal race active generation = %#v", active)
		}
		launching, err := fixture.store.beginActivation(*active, "portal-final-recheck", neoOrbLifecycleOperationUnpause)
		if err != nil {
			t.Fatalf("begin portal race transition: %v", err)
		}
		fixture.manager.mu.Lock()
		fixture.record.activeGeneration = cloneNeoOrbLifecycleGeneration(&launching)
		fixture.manager.mu.Unlock()
		return upstream.URL, nil
	}
	t.Cleanup(func() { neoOrbPortalTargetURL = original })

	response := neoOrbPortalRequest(t, module, "/orb/"+fixture.record.threadID+"/p/3000/", nil)
	if response.status != http.StatusConflict || proxied != 0 {
		t.Fatalf("portal final recheck response = %d %q proxied=%d", response.status, response.body, proxied)
	}
	var body map[string]any
	if err := json.Unmarshal([]byte(response.body), &body); err != nil || body["error"] != "orb is not ready" || body["state"] != neoOrbStateRunning || len(body) != 2 {
		t.Fatalf("portal final recheck diagnostic err=%v body=%#v", err, body)
	}
	record, ok := fixture.manager.snapshot(fixture.record.threadID)
	if !ok || record.activePortals != 0 || record.activeGeneration == nil || record.activeGeneration.ActivationState != neoOrbLifecycleActivationLaunching || record.activeGeneration.OperationKind != neoOrbLifecycleOperationUnpause {
		t.Fatalf("portal race record = %#v, exists=%v", record, ok)
	}
	assertNeoOrbNoProviderMutation(t, fixture.fake)
}
