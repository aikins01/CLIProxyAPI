package amp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/api/handlers"
)

func TestNeoDiffCaptureRequestPath(t *testing.T) {
	threadID := "T-019f9ad5-01de-71e4-99ab-a144b7974b30"
	captureID := "0123456789abcdefghijklmn"
	sha := strings.Repeat("a", 40)
	tests := []struct {
		name    string
		path    string
		matched bool
		valid   bool
		action  string
	}{
		{name: "allocate", path: "/api/threads/" + threadID + "/diff-captures", matched: true, valid: true, action: "allocate"},
		{name: "allocate trailing slash", path: "/api/threads/" + threadID + "/diff-captures/", matched: true, valid: true, action: "allocate"},
		{name: "latest", path: "/api/threads/" + threadID + "/diff-captures/latest", matched: true, valid: true, action: "latest"},
		{name: "latest trailing slash", path: "/api/threads/" + threadID + "/diff-captures/latest/", matched: true, valid: true, action: "latest"},
		{name: "blob", path: "/api/threads/" + threadID + "/diff-captures/blob/" + sha, matched: true, valid: true, action: "blob"},
		{name: "diff", path: "/api/threads/" + threadID + "/diff-captures/diff", matched: true, valid: true, action: "diff"},
		{name: "publish", path: "/api/threads/" + threadID + "/diff-captures/" + captureID + "/publish", matched: true, valid: true, action: "publish"},
		{name: "short capture id", path: "/api/threads/" + threadID + "/diff-captures/short/publish", matched: true},
		{name: "invalid blob sha", path: "/api/threads/" + threadID + "/diff-captures/blob/not-a-sha", matched: true},
		{name: "traversal", path: "/api/threads/" + threadID + "/diff-captures/blob/../repository.git", matched: true},
		{name: "extra segment", path: "/api/threads/" + threadID + "/diff-captures/latest/extra", matched: true},
		{name: "invalid thread", path: "/api/threads/../diff-captures", matched: true},
		{name: "unrelated", path: "/api/threads/" + threadID, matched: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			route, matched := neoDiffCaptureRequestPath(test.path)
			if matched != test.matched || route.valid != test.valid || route.action != test.action {
				t.Fatalf("route = %#v, matched=%v; want valid=%v action=%q matched=%v", route, matched, test.valid, test.action, test.matched)
			}
		})
	}
}

func TestNeoGitDiffStatWriterMatchesManifestContract(t *testing.T) {
	for _, test := range []struct {
		name    string
		diff    string
		added   int
		deleted int
		changed int
	}{
		{name: "addition", diff: "diff --git a/a b/a\n--- a/a\n+++ b/a\n@@ -0,0 +1 @@\n+added\n", added: 1, changed: 1},
		{name: "deletion", diff: "diff --git a/a b/a\n--- a/a\n+++ b/a\n@@ -1 +0,0 @@\n-deleted\n", deleted: 1, changed: 1},
		{name: "binary", diff: "diff --git a/a b/a\nnew file mode 100644\nindex 0000000..1234567\nBinary files /dev/null and b/a differ\n", changed: 1},
		{name: "header-like lines", diff: "diff --git a/a b/a\n--- a/a\n+++ b/a\n@@ -1,2 +1,3 @@\n---literal deletion\n+++literal addition\n-old\n+new\n+extra\n", added: 3, deleted: 2, changed: 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			writer := &neoGitDiffStatWriter{}
			for _, chunk := range []string{test.diff[:len(test.diff)/2], test.diff[len(test.diff)/2:]} {
				if _, err := writer.Write([]byte(chunk)); err != nil {
					t.Fatal(err)
				}
			}
			result := writer.result()
			if numberFrom(result["added"]) != test.added || numberFrom(result["deleted"]) != test.deleted || numberFrom(result["changed"]) != test.changed {
				t.Fatalf("diff stat = %#v, want added=%d deleted=%d changed=%d", result, test.added, test.deleted, test.changed)
			}
		})
	}
}

func TestWebLocalInferenceDiffCaptureReadPaths(t *testing.T) {
	threadID := "T-019f9ad5-01de-71e4-99ab-a144b7974b30"
	captureID := "0123456789abcdefghijklmn"
	sha := strings.Repeat("a", 40)
	readerPaths := []string{
		"/api/threads/" + threadID + "/diff-captures/latest",
		"/api/threads/" + threadID + "/diff-captures/blob/" + sha,
		"/api/threads/" + threadID + "/diff-captures/diff",
	}
	for _, path := range readerPaths {
		if !neoDiffCaptureBrowserReadPath(path) || !ampWebLocalInferencePath(path) {
			t.Fatalf("reader path %q was not web-local enabled", path)
		}
	}
	writerPaths := []string{
		"/api/threads/" + threadID + "/diff-captures",
		"/api/threads/" + threadID + "/diff-captures/" + captureID + "/publish",
	}
	for _, path := range writerPaths {
		if neoDiffCaptureBrowserReadPath(path) || ampWebLocalInferencePath(path) {
			t.Fatalf("writer path %q was web-local enabled", path)
		}
	}

	gin.SetMode(gin.TestMode)
	m := &AmpModule{
		restrictToLocalhost: false,
		lastConfig: &config.AmpCode{WebLocalInference: config.AmpWebLocalInference{
			Enabled:        true,
			AllowedOrigins: []string{"https://ampcode.com"},
		}},
	}
	router := gin.New()
	m.registerManagementRoutes(router, &handlers.BaseAPIHandler{}, nil)
	for _, path := range readerPaths {
		request := httptest.NewRequest(http.MethodOptions, path, nil)
		request.Header.Set("Origin", "https://ampcode.com")
		request.Header.Set("Access-Control-Request-Headers", "authorization, "+ampWebLocalInferenceHeader)
		response := httptest.NewRecorder()
		router.ServeHTTP(response, request)
		if response.Code != http.StatusNoContent || response.Header().Get("Access-Control-Allow-Origin") != "https://ampcode.com" {
			t.Fatalf("reader preflight %q status=%d origin=%q", path, response.Code, response.Header().Get("Access-Control-Allow-Origin"))
		}
	}
	for _, path := range writerPaths {
		request := httptest.NewRequest(http.MethodOptions, path, nil)
		request.Header.Set("Origin", "https://ampcode.com")
		request.Header.Set("Access-Control-Request-Headers", "authorization, "+ampWebLocalInferenceHeader)
		response := httptest.NewRecorder()
		router.ServeHTTP(response, request)
		if response.Code == http.StatusNoContent || response.Header().Get("Access-Control-Allow-Origin") != "" {
			t.Fatalf("writer preflight %q status=%d origin=%q", path, response.Code, response.Header().Get("Access-Control-Allow-Origin"))
		}
	}
}

func TestNeoDiffCaptureFileURLNormalizesWindowsPaths(t *testing.T) {
	if got := neoDiffCaptureFileURL(`C:\Users\amp\capture.git`); got != "file:///C:/Users/amp/capture.git" {
		t.Fatalf("Windows diff capture URL = %q", got)
	}
	if got := neoDiffCaptureFileURL(`\\server\share\capture.git`); got != "file://server/share/capture.git" {
		t.Fatalf("UNC diff capture URL = %q", got)
	}
}

func TestWebLocalInferenceDiffCaptureQueryAuthReachesOwnedThread(t *testing.T) {
	gin.SetMode(gin.TestMode)
	useTempNeoThreadStore(t)
	enabled := true
	rt := newNeoRuntime(&config.Config{
		SDKConfig: config.SDKConfig{APIKeys: []string{"local-key"}},
		AmpCode:   config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled}},
	})
	threadID := "T-019f9ad5-01de-71e4-99ab-a144b7974b35"
	neoDiffCaptureTestActor(rt, threadID, neoLocalOwnerUserID)
	m := &AmpModule{
		restrictToLocalhost: false,
		neoRuntime:          rt,
		lastConfig: &config.AmpCode{WebLocalInference: config.AmpWebLocalInference{
			Enabled:        true,
			AllowedOrigins: []string{"https://ampcode.com"},
		}},
	}
	authenticated := false
	queryKeyRemoved := false
	auth := func(c *gin.Context) {
		if c.GetHeader("Authorization") != "Bearer local-key" {
			c.AbortWithStatus(http.StatusUnauthorized)
			return
		}
		authenticated = true
		queryKeyRemoved = !c.Request.URL.Query().Has(ampWebLocalInferenceAPIKeyQuery)
		c.Set("userApiKey", "local-key")
		c.Next()
	}
	router := gin.New()
	m.registerManagementRoutes(router, &handlers.BaseAPIHandler{}, auth)
	request := httptest.NewRequest(http.MethodGet, "/api/threads/"+threadID+"/diff-captures/latest?"+ampWebLocalInferenceAPIKeyQuery+"=local-key", nil)
	request.Header.Set("Origin", "https://ampcode.com")
	request.Header.Set(ampWebLocalInferenceHeader, "1")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("query-auth reader status = %d, body=%s", response.Code, response.Body.String())
	}
	if !authenticated || !queryKeyRemoved {
		t.Fatalf("query auth authenticated=%v keyRemoved=%v", authenticated, queryKeyRemoved)
	}
	if response.Header().Get("Access-Control-Allow-Origin") != "https://ampcode.com" {
		t.Fatalf("query-auth reader CORS origin = %q", response.Header().Get("Access-Control-Allow-Origin"))
	}
}

func TestNeoDiffCaptureLockLifecycle(t *testing.T) {
	threadID := "T-019f9ad5-01de-71e4-99ab-a144b7974b36"
	neoDiffCaptureLocks.mu.Lock()
	if neoDiffCaptureLocks.entries != nil {
		delete(neoDiffCaptureLocks.entries, threadID)
	}
	neoDiffCaptureLocks.mu.Unlock()

	unlockFirst, err := neoDiffCaptureLock(context.Background(), threadID)
	if err != nil {
		t.Fatal(err)
	}
	firstEntry := neoDiffCaptureTestWaitForLockRefs(t, threadID, 1)
	secondAcquired := make(chan neoDiffCaptureTestLockResult, 1)
	go func() {
		unlock, err := neoDiffCaptureLock(context.Background(), threadID)
		secondAcquired <- neoDiffCaptureTestLockResult{unlock: unlock, err: err}
	}()
	if secondEntry := neoDiffCaptureTestWaitForLockRefs(t, threadID, 2); secondEntry != firstEntry {
		t.Fatal("waiter used a distinct thread lock entry")
	}
	select {
	case result := <-secondAcquired:
		if result.unlock != nil {
			result.unlock()
		}
		t.Fatal("waiter acquired a held thread lock")
	default:
	}
	unlockFirst()

	var unlockSecond func()
	select {
	case result := <-secondAcquired:
		if result.err != nil {
			t.Fatal(result.err)
		}
		unlockSecond = result.unlock
	case <-time.After(time.Second):
		t.Fatal("waiter did not acquire released thread lock")
	}
	thirdAcquired := make(chan neoDiffCaptureTestLockResult, 1)
	go func() {
		unlock, err := neoDiffCaptureLock(context.Background(), threadID)
		thirdAcquired <- neoDiffCaptureTestLockResult{unlock: unlock, err: err}
	}()
	if thirdEntry := neoDiffCaptureTestWaitForLockRefs(t, threadID, 2); thirdEntry != firstEntry {
		t.Fatal("later waiter used a distinct thread lock entry")
	}
	select {
	case result := <-thirdAcquired:
		if result.unlock != nil {
			result.unlock()
		}
		t.Fatal("later waiter acquired a held thread lock")
	default:
	}
	unlockSecond()

	var unlockThird func()
	select {
	case result := <-thirdAcquired:
		if result.err != nil {
			t.Fatal(result.err)
		}
		unlockThird = result.unlock
	case <-time.After(time.Second):
		t.Fatal("later waiter did not acquire released thread lock")
	}
	unlockThird()
	neoDiffCaptureLocks.mu.Lock()
	_, retained := neoDiffCaptureLocks.entries[threadID]
	neoDiffCaptureLocks.mu.Unlock()
	if retained {
		t.Fatal("unused thread lock entry was retained")
	}
}

func TestNeoDiffCaptureLockCancellation(t *testing.T) {
	threadID := "T-019f9ad5-01de-71e4-99ab-a144b7974b40"
	unlock, err := neoDiffCaptureLock(context.Background(), threadID)
	if err != nil {
		t.Fatal(err)
	}
	entry := neoDiffCaptureTestWaitForLockRefs(t, threadID, 1)
	ctx, cancel := context.WithCancel(context.Background())
	resultChannel := make(chan neoDiffCaptureTestLockResult, 1)
	go func() {
		waiterUnlock, waiterErr := neoDiffCaptureLock(ctx, threadID)
		resultChannel <- neoDiffCaptureTestLockResult{unlock: waiterUnlock, err: waiterErr}
	}()
	if waiterEntry := neoDiffCaptureTestWaitForLockRefs(t, threadID, 2); waiterEntry != entry {
		t.Fatal("cancelable waiter used a distinct thread lock entry")
	}
	cancel()
	select {
	case result := <-resultChannel:
		if !errors.Is(result.err, context.Canceled) || result.unlock != nil {
			t.Fatalf("canceled lock result = %#v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled lock waiter did not return")
	}
	if remainingEntry := neoDiffCaptureTestWaitForLockRefs(t, threadID, 1); remainingEntry != entry {
		t.Fatal("canceled waiter changed the active lock entry")
	}
	unlock()
	neoDiffCaptureTestWaitForLockRefs(t, threadID, 0)
}

func TestNeoDiffCaptureGitCancellation(t *testing.T) {
	repositoryDir := t.TempDir()
	neoDiffCaptureTestGit(t, repositoryDir, "init", "--bare", "--quiet")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	reader, writer := io.Pipe()
	closed := make(chan struct{})
	go func() {
		<-ctx.Done()
		_ = writer.Close()
		close(closed)
	}()
	startedAt := time.Now()
	_, err := neoDiffCaptureGit(ctx, repositoryDir, []string{"cat-file", "--batch-check"}, 128, reader)
	_ = reader.Close()
	cancel()
	<-closed
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Git cancellation error = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(startedAt); elapsed > 2*time.Second {
		t.Fatalf("Git cancellation took %s", elapsed)
	}
}

func TestNeoDiffCaptureAllocateUsesRequestContext(t *testing.T) {
	gin.SetMode(gin.TestMode)
	useTempNeoThreadStore(t)
	threadID := "T-019f9ad5-01de-71e4-99ab-a144b7974b37"
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
	ctx, cancel := context.WithCancel(request.Context())
	cancel()
	request = request.WithContext(ctx)
	response := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(response)
	c.Request = request
	(&AmpModule{}).serveNeoDiffCaptureAllocate(c, threadID)
	if response.Body.Len() != 0 {
		t.Fatalf("canceled allocation body = %s", response.Body.String())
	}
	if _, err := os.Stat(neoDiffCaptureRepositoryDir(threadID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("canceled allocation repository stat error = %v", err)
	}
}

func TestNeoDiffCaptureAllocateUsesSourceObjectFormat(t *testing.T) {
	gin.SetMode(gin.TestMode)
	useTempNeoThreadStore(t)
	threadID := "T-019f9ad5-01de-71e4-99ab-a144b7974b51"
	workspace := t.TempDir()
	neoDiffCaptureTestGit(t, workspace, "init", "--quiet", "--object-format=sha256")
	rt := newNeoRuntime(&config.Config{})
	neoDiffCaptureTestActor(rt, threadID, neoLocalOwnerUserID)
	actor := rt.store.lookupThreadActor(threadID)
	actor.mu.Lock()
	actor.environment = map[string]any{"workingDirectory": workspace, "workspaceRoot": workspace}
	actor.mu.Unlock()
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
	response := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(response)
	c.Request = request
	(&AmpModule{neoRuntime: rt}).serveNeoDiffCaptureAllocate(c, threadID)
	if response.Code != http.StatusOK {
		t.Fatalf("SHA-256 allocation status = %d, body=%s", response.Code, response.Body.String())
	}
	objectFormat := strings.TrimSpace(neoDiffCaptureTestGit(t, neoDiffCaptureRepositoryDir(threadID), "rev-parse", "--show-object-format"))
	if objectFormat != "sha256" {
		t.Fatalf("capture repository object format = %q, want sha256", objectFormat)
	}
}

func TestNeoLocalDiffCaptureLifecycle(t *testing.T) {
	gin.SetMode(gin.TestMode)
	useTempNeoThreadStore(t)
	enabled := true
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled}}})
	threadID := "T-019f9ad5-01de-71e4-99ab-a144b7974b31"
	neoDiffCaptureTestActor(rt, threadID, neoLocalOwnerUserID)
	m := &AmpModule{restrictToLocalhost: false, neoRuntime: rt}
	router := gin.New()
	m.registerManagementRoutes(router, &handlers.BaseAPIHandler{}, nil)

	latestBefore := neoDiffCaptureTestRequest(t, router, http.MethodGet, "/api/threads/"+threadID+"/diff-captures/latest", nil)
	if latestBefore.Code != http.StatusNoContent {
		t.Fatalf("latest before publish status = %d, body=%s", latestBefore.Code, latestBefore.Body.String())
	}
	wrongMethod := neoDiffCaptureTestRequest(t, router, http.MethodGet, "/api/threads/"+threadID+"/diff-captures", nil)
	if wrongMethod.Code != http.StatusMethodNotAllowed || wrongMethod.Header().Get("Allow") != http.MethodPost {
		t.Fatalf("allocation method response = status %d Allow %q", wrongMethod.Code, wrongMethod.Header().Get("Allow"))
	}
	malformed := neoDiffCaptureTestRequest(t, router, http.MethodGet, "/api/threads/"+threadID+"/diff-captures/blob/not-a-sha", nil)
	if malformed.Code != http.StatusNotFound {
		t.Fatalf("malformed local capture status = %d, body=%s", malformed.Code, malformed.Body.String())
	}
	oversizedAllocation := neoDiffCaptureTestRequest(t, router, http.MethodPost, "/api/threads/"+threadID+"/diff-captures", []byte(`{"payload":"`+strings.Repeat("x", neoDiffCaptureAllocationMaxBytes)+`"}`))
	if oversizedAllocation.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized allocation status = %d, body=%s", oversizedAllocation.Code, oversizedAllocation.Body.String())
	}

	allocation := neoDiffCaptureTestAllocate(t, router, threadID)
	captureID := stringValue(allocation["captureID"])
	captureRef := stringValue(allocation["captureRef"])
	gitURL := stringValue(allocation["gitURL"])
	if !neoDiffCaptureIDPattern.MatchString(captureID) || captureRef != "refs/heads/amp/captures/"+captureID {
		t.Fatalf("allocation = %#v", allocation)
	}
	parsedGitURL, err := url.Parse(gitURL)
	if err != nil || parsedGitURL.Scheme != "file" {
		t.Fatalf("gitURL = %q, error=%v", gitURL, err)
	}
	repositoryDir := neoDiffCaptureRepositoryDir(threadID)
	if parsedGitURL.Path != repositoryDir {
		t.Fatalf("gitURL path = %q, want %q", parsedGitURL.Path, repositoryDir)
	}
	if mode := neoDiffCaptureTestMode(t, neoDiffCaptureThreadDir(threadID)); mode != 0o700 {
		t.Fatalf("thread capture directory mode = %o, want 700", mode)
	}
	if mode := neoDiffCaptureTestMode(t, neoDiffCaptureRecordPath(threadID, captureID)); mode != 0o600 {
		t.Fatalf("capture metadata mode = %o, want 600", mode)
	}
	if got := strings.TrimSpace(neoDiffCaptureTestGit(t, repositoryDir, "rev-parse", "--is-bare-repository")); got != "true" {
		t.Fatalf("bare repository check = %q", got)
	}
	for key, want := range map[string]string{
		"gc.auto":                     "0",
		"receive.hideRefs":            "refs/amp/diff-captures",
		"receive.maxInputSize":        strconv.Itoa(neoDiffCaptureReceiveMaxBytes),
		"receive.denyDeletes":         "true",
		"receive.denyNonFastForwards": "true",
	} {
		if got := strings.TrimSpace(neoDiffCaptureTestGit(t, repositoryDir, "config", "--get", key)); got != want {
			t.Fatalf("repository config %s = %q, want %q", key, got, want)
		}
	}
	for _, hook := range []string{"pre-receive", "reference-transaction", "post-receive"} {
		if mode := neoDiffCaptureTestMode(t, filepath.Join(repositoryDir, "hooks", hook)); mode != 0o700 {
			t.Fatalf("%s hook mode = %o, want 700", hook, mode)
		}
	}
	for _, hook := range []struct {
		name string
		args []string
	}{
		{name: "reference-transaction", args: []string{"aborted"}},
		{name: "post-receive"},
	} {
		ownerPath := filepath.Join(repositoryDir, "amp-receive-active-foreign")
		if err := os.WriteFile(ownerPath, []byte("1\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		command := exec.Command(filepath.Join(repositoryDir, "hooks", hook.name), hook.args...)
		command.Dir = repositoryDir
		command.Env = neoGitCommandEnv()
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("%s foreign-owner cleanup: %v: %s", hook.name, err, output)
		}
		if got, err := os.ReadFile(ownerPath); err != nil || string(got) != "1\n" {
			t.Fatalf("%s foreign receive owner = %q, err=%v", hook.name, got, err)
		}
		if err := os.Remove(ownerPath); err != nil {
			t.Fatal(err)
		}
	}
	abortedToken := fmt.Sprintf("%d-aborted", os.Getpid())
	abortedPaths := []string{
		filepath.Join(repositoryDir, "amp-receive-pending-"+abortedToken),
		filepath.Join(repositoryDir, "amp-receive-owner-"+abortedToken),
		filepath.Join(repositoryDir, "amp-receive-active-"+abortedToken),
	}
	for _, path := range abortedPaths {
		if err := os.WriteFile(path, []byte(abortedToken+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	abortedHook := exec.Command(filepath.Join(repositoryDir, "hooks", "reference-transaction"), "aborted")
	abortedHook.Dir = repositoryDir
	abortedHook.Env = neoGitCommandEnv()
	if output, err := abortedHook.CombinedOutput(); err != nil {
		t.Fatalf("aborted receive cleanup: %v: %s", err, output)
	}
	for _, path := range abortedPaths {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("aborted receive retained %s: %v", filepath.Base(path), err)
		}
	}
	fixture := neoDiffCaptureTestRepository(t)
	for _, ref := range []string{
		"refs/heads/untracked",
		"refs/heads/amp/captures/0000000000000000",
	} {
		command := exec.Command("git", "push", gitURL, fixture.headSHA+":"+ref)
		command.Dir = fixture.dir
		command.Env = neoGitCommandEnv()
		if output, err := command.CombinedOutput(); err == nil {
			t.Fatalf("unauthorized push to %s succeeded: %s", ref, output)
		}
	}
	operationRelease, err := neoAcquireDiffCaptureReceiveLock(threadID)
	if err != nil {
		t.Fatal(err)
	}
	if secondRelease, err := neoAcquireDiffCaptureReceiveLock(threadID); !errors.Is(err, errNeoDiffCaptureReceiveActive) {
		if secondRelease != nil {
			secondRelease()
		}
		t.Fatalf("concurrent API receive lock error = %v, want active", err)
	}
	blockedPush := exec.Command("git", "push", gitURL, fixture.headSHA+":"+captureRef)
	blockedPush.Dir = fixture.dir
	blockedPush.Env = neoGitCommandEnv()
	blockedOutput, blockedErr := blockedPush.CombinedOutput()
	operationRelease()
	if blockedErr == nil {
		t.Fatalf("push during diff capture operation succeeded: %s", blockedOutput)
	}
	if owners, err := filepath.Glob(filepath.Join(repositoryDir, "amp-api-active-*")); err != nil || len(owners) != 0 {
		t.Fatalf("released API receive owners = %v, err=%v", owners, err)
	}
	abandonedReceive := filepath.Join(repositoryDir, "amp-receive-active-malformed")
	if err := os.WriteFile(abandonedReceive, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := neoAcquireDiffCaptureReceiveLock(threadID); !errors.Is(err, errNeoDiffCaptureReceiveActive) {
		t.Fatalf("fresh malformed receive owner error = %v, want active", err)
	}
	staleAt := time.Now().Add(-2 * neoDiffCaptureLockStaleAfter)
	if err := os.Chtimes(abandonedReceive, staleAt, staleAt); err != nil {
		t.Fatal(err)
	}
	abandonedRelease, err := neoAcquireDiffCaptureReceiveLock(threadID)
	if err != nil {
		t.Fatalf("recover stale malformed receive owner: %v", err)
	}
	abandonedRelease()
	if _, err := os.Stat(abandonedReceive); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale malformed receive owner remained: %v", err)
	}
	stalePending := filepath.Join(repositoryDir, "amp-receive-pending-stale")
	staleOwner := filepath.Join(repositoryDir, "amp-receive-owner-stale")
	for _, path := range []string{stalePending, staleOwner} {
		if err := os.WriteFile(path, []byte("stale\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(path, staleAt, staleAt); err != nil {
			t.Fatal(err)
		}
	}
	staleMetadataRelease, err := neoAcquireDiffCaptureReceiveLock(threadID)
	if err != nil {
		t.Fatalf("recover stale receive metadata: %v", err)
	}
	staleMetadataRelease()
	for _, path := range []string{stalePending, staleOwner} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("stale receive metadata %s remained: %v", filepath.Base(path), err)
		}
	}
	liveStaleReceive := filepath.Join(repositoryDir, "amp-receive-active-live-stale")
	if err := os.WriteFile(liveStaleReceive, []byte(strconv.Itoa(os.Getpid())+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(liveStaleReceive, staleAt, staleAt); err != nil {
		t.Fatal(err)
	}
	liveStaleRelease, err := neoAcquireDiffCaptureReceiveLock(threadID)
	if err != nil {
		t.Fatalf("recover stale receive owner with live PID: %v", err)
	}
	liveStaleRelease()
	deadReceive := filepath.Join(repositoryDir, "amp-receive-active-dead")
	if err := os.WriteFile(deadReceive, []byte("999999999\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	deadRelease, err := neoAcquireDiffCaptureReceiveLock(threadID)
	if err != nil {
		t.Fatalf("recover dead receive owner: %v", err)
	}
	deadRelease()
	staleAPI := filepath.Join(repositoryDir, "amp-api-active-stale")
	if err := os.WriteFile(staleAPI, []byte("stale\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(staleAPI, staleAt, staleAt); err != nil {
		t.Fatal(err)
	}
	neoDiffCaptureTestGit(t, fixture.dir, "push", gitURL, fixture.headSHA+":"+captureRef)
	for _, pattern := range []string{"amp-receive-active-*", "amp-receive-owner-*", "amp-receive-pending-*"} {
		matches, err := filepath.Glob(filepath.Join(repositoryDir, pattern))
		if err != nil || len(matches) != 0 {
			t.Fatalf("completed push retained %s state: %v, err=%v", pattern, matches, err)
		}
	}
	if got := strings.TrimSpace(neoDiffCaptureTestGit(t, repositoryDir, "rev-parse", captureRef)); got != fixture.headSHA {
		t.Fatalf("pushed ref = %q, want %q", got, fixture.headSHA)
	}
	manifest := neoDiffCaptureTestManifest(captureID, captureRef, fixture)
	mismatchedManifest := cloneMap(manifest)
	mismatchedManifest["captureID"] = "0123456789ABCDEFGHIJKLMN"
	mismatchRaw, err := json.Marshal(mismatchedManifest)
	if err != nil {
		t.Fatal(err)
	}
	mismatch := neoDiffCaptureTestRequest(t, router, http.MethodPost, "/api/threads/"+threadID+"/diff-captures/"+captureID+"/publish", mismatchRaw)
	if mismatch.Code != http.StatusBadRequest {
		t.Fatalf("mismatched manifest status = %d, body=%s", mismatch.Code, mismatch.Body.String())
	}
	for _, test := range []struct {
		name   string
		sha    string
		status int
	}{
		{name: "invalid sha", sha: "not-a-sha", status: http.StatusBadRequest},
		{name: "missing object", sha: strings.Repeat("f", 40), status: http.StatusConflict},
		{name: "non-blob object", sha: fixture.headSHA, status: http.StatusConflict},
	} {
		t.Run(test.name, func(t *testing.T) {
			invalidManifest := neoDiffCaptureTestManifestWithOldBlob(t, manifest, test.sha)
			invalidRaw, err := json.Marshal(invalidManifest)
			if err != nil {
				t.Fatal(err)
			}
			response := neoDiffCaptureTestRequest(t, router, http.MethodPost, "/api/threads/"+threadID+"/diff-captures/"+captureID+"/publish", invalidRaw)
			if response.Code != test.status {
				t.Fatalf("invalid blob publish status = %d, want %d, body=%s", response.Code, test.status, response.Body.String())
			}
		})
	}
	unpublishedRecord, err := readNeoDiffCaptureRecord(threadID, captureID)
	if err != nil || unpublishedRecord.State != "allocated" || len(unpublishedRecord.Manifest) != 0 {
		t.Fatalf("rejected publish record = %#v, error=%v", unpublishedRecord, err)
	}
	manifestRaw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	publish := neoDiffCaptureTestRequest(t, router, http.MethodPost, "/api/threads/"+threadID+"/diff-captures/"+captureID+"/publish", manifestRaw)
	if publish.Code != http.StatusOK {
		t.Fatalf("publish status = %d, body=%s", publish.Code, publish.Body.String())
	}
	if _, err := os.Stat(neoDiffCapturePendingPublicationPath(threadID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("pending publication remained after publish: %v", err)
	}
	record, err := readNeoDiffCaptureRecord(threadID, captureID)
	if err != nil || record.State != "published" || record.PublishedRef != neoDiffCapturePublishedRef(captureID) || record.PublishedAt == "" || record.ManifestSHA256 != neoDiffCaptureManifestSHA256(manifestRaw) || len(record.Manifest) == 0 {
		t.Fatalf("published record = %#v, error=%v", record, err)
	}
	if got := strings.TrimSpace(neoDiffCaptureTestGit(t, repositoryDir, "rev-parse", record.PublishedRef)); got != fixture.headSHA {
		t.Fatalf("published ref = %q, want %q", got, fixture.headSHA)
	}
	publishedRefCheck := exec.Command("git", "show-ref", "--verify", "--quiet", captureRef)
	publishedRefCheck.Dir = repositoryDir
	publishedRefCheck.Env = neoGitCommandEnv()
	if err := publishedRefCheck.Run(); err == nil {
		t.Fatalf("mutable capture ref %s remained after publish", captureRef)
	}
	republishRef := exec.Command("git", "push", gitURL, fixture.headSHA+":"+captureRef)
	republishRef.Dir = fixture.dir
	republishRef.Env = neoGitCommandEnv()
	if output, err := republishRef.CombinedOutput(); err == nil {
		t.Fatalf("push to published capture ref succeeded: %s", output)
	}
	publishedAt := record.PublishedAt
	publishedManifest := append(json.RawMessage(nil), record.Manifest...)
	if err := os.Remove(neoDiffCaptureBlobIndexPath(threadID)); err != nil {
		t.Fatal(err)
	}
	neoDiffCaptureBlobIndexes.delete(threadID)
	idempotent := neoDiffCaptureTestRequest(t, router, http.MethodPost, "/api/threads/"+threadID+"/diff-captures/"+captureID+"/publish", manifestRaw)
	if idempotent.Code != http.StatusOK {
		t.Fatalf("idempotent republish status = %d, body=%s", idempotent.Code, idempotent.Body.String())
	}
	if _, err := os.Stat(neoDiffCapturePendingPublicationPath(threadID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("pending publication remained after republish: %v", err)
	}
	recoveredIndex, err := readNeoDiffCaptureBlobIndex(threadID)
	if err != nil || !recoveredIndex.allowsPair(fixture.oldBlobSHA, fixture.newBlobSHA) {
		t.Fatalf("recovered blob index = %#v, error=%v", recoveredIndex, err)
	}
	var reformatted bytes.Buffer
	if err := json.Indent(&reformatted, manifestRaw, "", "  "); err != nil {
		t.Fatal(err)
	}
	nonidentical := neoDiffCaptureTestRequest(t, router, http.MethodPost, "/api/threads/"+threadID+"/diff-captures/"+captureID+"/publish", reformatted.Bytes())
	if nonidentical.Code != http.StatusConflict {
		t.Fatalf("byte-different republish status = %d, body=%s", nonidentical.Code, nonidentical.Body.String())
	}
	differentManifest, ok := cloneNeoJSONValue(manifest).(map[string]any)
	if !ok {
		t.Fatal("cloned manifest is not an object")
	}
	mapValue(differentManifest["source"])["branch"] = "other"
	differentRaw, err := json.Marshal(differentManifest)
	if err != nil {
		t.Fatal(err)
	}
	conflict := neoDiffCaptureTestRequest(t, router, http.MethodPost, "/api/threads/"+threadID+"/diff-captures/"+captureID+"/publish", differentRaw)
	if conflict.Code != http.StatusConflict {
		t.Fatalf("different republish status = %d, body=%s", conflict.Code, conflict.Body.String())
	}
	unchangedRecord, err := readNeoDiffCaptureRecord(threadID, captureID)
	if err != nil || unchangedRecord.PublishedAt != publishedAt || unchangedRecord.ManifestSHA256 != record.ManifestSHA256 || !bytes.Equal(unchangedRecord.Manifest, publishedManifest) {
		t.Fatalf("republish changed record = %#v, error=%v", unchangedRecord, err)
	}

	indexed, ok := neoDiffCaptureBlobIndexes.get(threadID)
	if !ok || !indexed.allows(fixture.oldBlobSHA) || !indexed.allowsPair(fixture.oldBlobSHA, fixture.newBlobSHA) {
		t.Fatalf("published blob index = %#v, found=%v", indexed, ok)
	}
	indexedCapture, ok := indexed.captures[captureID]
	if !ok || indexedCapture.manifestSHA256 != record.ManifestSHA256 {
		t.Fatalf("published capture index entry = %#v, found=%v", indexedCapture, ok)
	}
	neoDiffCaptureBlobIndexes.delete(threadID)
	rebuilt, err := neoDiffCaptureBlobIndexForThread(threadID)
	if err != nil || !rebuilt.allows(fixture.binaryBlobSHA) || !rebuilt.allowsPair(fixture.oldBlobSHA, fixture.newBlobSHA) {
		t.Fatalf("rebuilt blob index = %#v, error=%v", rebuilt, err)
	}
	if mode := neoDiffCaptureTestMode(t, neoDiffCaptureBlobIndexPath(threadID)); mode != 0o600 {
		t.Fatalf("blob index mode = %o, want 600", mode)
	}
	poisonedSHA := strings.Repeat("f", 40)
	if poisonedSHA == fixture.oldBlobSHA {
		poisonedSHA = strings.Repeat("e", 40)
	}
	poisoned := rebuilt.withCapture(captureID, record.ManifestSHA256, []string{poisonedSHA})
	if authorized, err := neoDiffCaptureIndexAuthorizes(threadID, poisoned, poisonedSHA); authorized || err == nil {
		t.Fatal("blob index authorized a sha absent from its published manifest")
	}
	cached, err := neoDiffCaptureBlobIndexForThread(threadID)
	if err != nil || !cached.allows(fixture.binaryBlobSHA) || !cached.allowsPair(fixture.oldBlobSHA, fixture.newBlobSHA) {
		t.Fatalf("cached blob index = %#v, rebuilt=%#v, error=%v", cached, rebuilt, err)
	}

	latest := neoDiffCaptureTestRequest(t, router, http.MethodGet, "/api/threads/"+threadID+"/diff-captures/latest", nil)
	if latest.Code != http.StatusOK {
		t.Fatalf("latest status = %d, body=%s", latest.Code, latest.Body.String())
	}
	latestBody := neoDiffCaptureTestJSON(t, latest)
	if numberFrom(latestBody["version"]) != 3 || stringValue(latestBody["captureID"]) != captureID || numberFrom(latestBody["capturedAt"]) != 1785100693111 || stringValue(latestBody["branch"]) != "main" {
		t.Fatalf("latest body = %#v", latestBody)
	}
	ranges := arrayValue(latestBody["ranges"])
	if len(ranges) != 3 || stringValue(mapValue(ranges[0])["kind"]) != "head" || stringValue(mapValue(ranges[1])["kind"]) != "all" || stringValue(mapValue(ranges[2])["kind"]) != "commit" {
		t.Fatalf("latest ranges = %#v", ranges)
	}
	headSections := arrayValue(mapValue(ranges[0])["sections"])
	headFiles := arrayValue(mapValue(headSections[0])["files"])
	if stringValue(mapValue(headFiles[0])["oldBlobSHA"]) != fixture.oldBlobSHA || stringValue(mapValue(headFiles[0])["newBlobSHA"]) != fixture.newBlobSHA {
		t.Fatalf("head files = %#v", headFiles)
	}

	oldBlob := neoDiffCaptureTestRequest(t, router, http.MethodGet, "/api/threads/"+threadID+"/diff-captures/blob/"+fixture.oldBlobSHA, nil)
	oldBlobBody := neoDiffCaptureTestJSON(t, oldBlob)
	if oldBlob.Code != http.StatusOK || stringValue(oldBlobBody["kind"]) != "text" || stringValue(oldBlobBody["content"]) != "old\n" {
		t.Fatalf("old blob status=%d body=%#v", oldBlob.Code, oldBlobBody)
	}
	binaryBlob := neoDiffCaptureTestRequest(t, router, http.MethodGet, "/api/threads/"+threadID+"/diff-captures/blob/"+fixture.binaryBlobSHA, nil)
	binaryBody := neoDiffCaptureTestJSON(t, binaryBlob)
	if binaryBlob.Code != http.StatusOK || stringValue(binaryBody["kind"]) != "binary" || numberFrom(binaryBody["sizeBytes"]) != 4 {
		t.Fatalf("binary blob status=%d body=%#v", binaryBlob.Code, binaryBody)
	}
	omittedBlob := neoDiffCaptureTestRequest(t, router, http.MethodGet, "/api/threads/"+threadID+"/diff-captures/blob/"+fixture.largeBlobSHA, nil)
	omittedBody := neoDiffCaptureTestJSON(t, omittedBlob)
	if omittedBlob.Code != http.StatusOK || stringValue(omittedBody["kind"]) != "omitted" || numberFrom(omittedBody["sizeBytes"]) != neoDiffCaptureTextBlobMaxBytes+1 {
		t.Fatalf("omitted blob status=%d body=%#v", omittedBlob.Code, omittedBody)
	}
	unknownSHA := strings.Repeat("f", 40)
	if unknownSHA == fixture.oldBlobSHA {
		unknownSHA = strings.Repeat("e", 40)
	}
	unknownBlob := neoDiffCaptureTestRequest(t, router, http.MethodGet, "/api/threads/"+threadID+"/diff-captures/blob/"+unknownSHA, nil)
	if unknownBlob.Code != http.StatusNotFound {
		t.Fatalf("unknown blob status = %d, body=%s", unknownBlob.Code, unknownBlob.Body.String())
	}
	deltaPath := "/api/threads/" + threadID + "/diff-captures/diff?from=" + fixture.oldBlobSHA + "&to=" + fixture.newBlobSHA
	delta := neoDiffCaptureTestRequest(t, router, http.MethodGet, deltaPath, nil)
	deltaBody := neoDiffCaptureTestJSON(t, delta)
	diff := stringValue(deltaBody["diff"])
	if delta.Code != http.StatusOK || !strings.Contains(diff, "-old") || !strings.Contains(diff, "+new") {
		t.Fatalf("delta status=%d body=%#v", delta.Code, deltaBody)
	}

	oversizedPublish := neoDiffCaptureTestRequest(t, router, http.MethodPost, "/api/threads/"+threadID+"/diff-captures/"+captureID+"/publish", []byte(`{"payload":"`+strings.Repeat("x", neoDiffCapturePublishMaxBytes)+`"}`))
	if oversizedPublish.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized publish status = %d, body=%s", oversizedPublish.Code, oversizedPublish.Body.String())
	}

	rejectedAllocation := neoDiffCaptureTestAllocate(t, router, threadID)
	rejectedID := stringValue(rejectedAllocation["captureID"])
	rejectedRef := stringValue(rejectedAllocation["captureRef"])
	neoDiffCaptureTestGit(t, fixture.dir, "push", stringValue(rejectedAllocation["gitURL"]), fixture.baseSHA+":"+rejectedRef)
	neoDiffCaptureTestGit(t, neoDiffCaptureRepositoryDir(threadID), "fetch", "--quiet", fixture.dir, fixture.headSHA)
	rejectedManifestRaw, err := json.Marshal(neoDiffCaptureTestManifest(rejectedID, rejectedRef, fixture))
	if err != nil {
		t.Fatal(err)
	}
	rejectedPublish := neoDiffCaptureTestRequest(t, router, http.MethodPost, "/api/threads/"+threadID+"/diff-captures/"+rejectedID+"/publish", rejectedManifestRaw)
	if rejectedPublish.Code != http.StatusConflict {
		t.Fatalf("rejected promotion status = %d, body=%s", rejectedPublish.Code, rejectedPublish.Body.String())
	}
	if _, err := os.Stat(neoDiffCapturePendingPublicationPath(threadID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rejected promotion retained pending publication: %v", err)
	}
	rejectedCleanup := neoDiffCaptureTestRequest(t, router, http.MethodDelete, "/api/threads/"+threadID+"/diff-captures/"+rejectedID+"/publish", nil)
	if rejectedCleanup.Code != http.StatusNoContent {
		t.Fatalf("rejected promotion cleanup status = %d, body=%s", rejectedCleanup.Code, rejectedCleanup.Body.String())
	}

	cleanupAllocation := neoDiffCaptureTestAllocate(t, router, threadID)
	cleanupID := stringValue(cleanupAllocation["captureID"])
	cleanupRef := stringValue(cleanupAllocation["captureRef"])
	neoDiffCaptureTestGit(t, fixture.dir, "push", stringValue(cleanupAllocation["gitURL"]), fixture.headSHA+":"+cleanupRef)
	receiveRelease, err := neoAcquireDiffCaptureReceiveLock(threadID)
	if err != nil {
		t.Fatal(err)
	}
	blockedCleanup := neoDiffCaptureTestRequest(t, router, http.MethodDelete, "/api/threads/"+threadID+"/diff-captures/"+cleanupID+"/publish", nil)
	receiveRelease()
	if blockedCleanup.Code != http.StatusConflict {
		t.Fatalf("cleanup during receive status = %d, body=%s", blockedCleanup.Code, blockedCleanup.Body.String())
	}
	cleanup := neoDiffCaptureTestRequest(t, router, http.MethodDelete, "/api/threads/"+threadID+"/diff-captures/"+cleanupID+"/publish", nil)
	if cleanup.Code != http.StatusNoContent {
		t.Fatalf("cleanup status = %d, body=%s", cleanup.Code, cleanup.Body.String())
	}
	if _, err := os.Stat(neoDiffCaptureRecordPath(threadID, cleanupID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("cleanup metadata stat error = %v", err)
	}
	refCheck := exec.Command("git", "show-ref", "--verify", "--quiet", cleanupRef)
	refCheck.Dir = repositoryDir
	refCheck.Env = neoGitCommandEnv()
	if err := refCheck.Run(); err == nil {
		t.Fatalf("cleanup ref %s still exists", cleanupRef)
	}

	if err := rt.purgeNeoLocalThread(threadID); err != nil {
		t.Fatalf("purge local thread: %v", err)
	}
	if _, err := os.Stat(neoDiffCaptureThreadDir(threadID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("purged capture directory stat error = %v", err)
	}
	if _, ok := neoDiffCaptureBlobIndexes.get(threadID); ok {
		t.Fatal("purged capture retained blob index")
	}
}

func TestNeoLocalDiffCaptureOwnershipAndUnrelatedUpstreamPassThrough(t *testing.T) {
	gin.SetMode(gin.TestMode)
	useTempNeoThreadStore(t)
	enabled := true
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled}}})
	localThreadID := "T-019f9ad5-01de-71e4-99ab-a144b7974b32"
	foreignThreadID := "T-019f9ad5-01de-71e4-99ab-a144b7974b33"
	unknownThreadID := "T-019f9ad5-01de-71e4-99ab-a144b7974b34"
	neoDiffCaptureTestActor(rt, localThreadID, neoLocalOwnerUserID)
	neoDiffCaptureTestActor(rt, foreignThreadID, "user-foreign")
	upstreamRequests := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests++
		writeNeoJSON(w, http.StatusOK, map[string]any{"captureID": "capture-upstream", "path": r.URL.Path})
	}))
	t.Cleanup(upstream.Close)
	m := &AmpModule{restrictToLocalhost: false, neoRuntime: rt}
	proxy, err := createReverseProxy(upstream.URL, NewStaticSecretSource(""))
	if err != nil {
		t.Fatal(err)
	}
	m.setProxy(proxy)
	router := gin.New()
	auth := func(c *gin.Context) {
		c.Set("userApiKey", "local-client")
		c.Next()
	}
	m.registerManagementRoutes(router, &handlers.BaseAPIHandler{}, auth)

	local := neoDiffCaptureTestRequest(t, router, http.MethodPost, "/api/threads/"+localThreadID+"/diff-captures", []byte(`{}`))
	if local.Code != http.StatusOK || stringValue(neoDiffCaptureTestJSON(t, local)["captureRef"]) == "" || upstreamRequests != 0 {
		t.Fatalf("owned local response status=%d body=%s upstream=%d", local.Code, local.Body.String(), upstreamRequests)
	}
	foreign := neoDiffCaptureTestRequest(t, router, http.MethodPost, "/api/threads/"+foreignThreadID+"/diff-captures", []byte(`{}`))
	foreignBody := neoDiffCaptureTestJSON(t, foreign)
	if foreign.Code != http.StatusNotFound || stringValue(foreignBody["error"]) != "diff capture thread not found" || upstreamRequests != 0 {
		t.Fatalf("foreign response status=%d body=%#v upstream=%d", foreign.Code, foreignBody, upstreamRequests)
	}
	unknown := neoDiffCaptureTestRequest(t, router, http.MethodPost, "/api/threads/"+unknownThreadID+"/diff-captures", []byte(`{}`))
	unknownBody := neoDiffCaptureTestJSON(t, unknown)
	if unknown.Code != http.StatusOK || stringValue(unknownBody["captureID"]) != "capture-upstream" || upstreamRequests != 1 {
		t.Fatalf("unknown upstream response status=%d body=%#v upstream=%d", unknown.Code, unknownBody, upstreamRequests)
	}
	invalidThread := neoDiffCaptureTestRequest(t, router, http.MethodPost, "/api/threads/not-a-thread/diff-captures", []byte(`{}`))
	if invalidThread.Code != http.StatusNotFound || upstreamRequests != 1 {
		t.Fatalf("invalid thread response status=%d body=%s upstream=%d", invalidThread.Code, invalidThread.Body.String(), upstreamRequests)
	}
	unrelated := neoDiffCaptureTestRequest(t, router, http.MethodPost, "/api/threads/"+unknownThreadID+"/unrelated", []byte(`{}`))
	if unrelated.Code != http.StatusOK || upstreamRequests != 2 {
		t.Fatalf("unrelated upstream response status=%d body=%s upstream=%d", unrelated.Code, unrelated.Body.String(), upstreamRequests)
	}
}

func TestNeoDiffCaptureReceiveLockRejectsMultiRefPush(t *testing.T) {
	gin.SetMode(gin.TestMode)
	useTempNeoThreadStore(t)
	enabled := true
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled}}})
	threadID := "T-019f9ad5-01de-71e4-99ab-a144b7974b52"
	neoDiffCaptureTestActor(rt, threadID, neoLocalOwnerUserID)
	router := gin.New()
	(&AmpModule{restrictToLocalhost: false, neoRuntime: rt}).registerManagementRoutes(router, &handlers.BaseAPIHandler{}, nil)
	first := neoDiffCaptureTestAllocate(t, router, threadID)
	second := neoDiffCaptureTestAllocate(t, router, threadID)
	gitURL := stringValue(first["gitURL"])
	refs := []string{stringValue(first["captureRef"]), stringValue(second["captureRef"])}
	fixture := neoDiffCaptureTestRepository(t)
	release, err := neoAcquireDiffCaptureReceiveLock(threadID)
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command("git", "push", gitURL, fixture.headSHA+":"+refs[0], fixture.headSHA+":"+refs[1])
	command.Dir = fixture.dir
	command.Env = neoGitCommandEnv()
	output, err := command.CombinedOutput()
	release()
	if err == nil {
		t.Fatalf("multi-ref push during API operation succeeded: %s", output)
	}
	repositoryDir := neoDiffCaptureRepositoryDir(threadID)
	for _, ref := range refs {
		command = exec.Command("git", "show-ref", "--verify", "--quiet", ref)
		command.Dir = repositoryDir
		command.Env = neoGitCommandEnv()
		if err := command.Run(); err == nil {
			t.Fatalf("blocked multi-ref push updated %s", ref)
		}
	}
	for _, pattern := range []string{"amp-receive-active-*", "amp-receive-owner-*", "amp-receive-pending-*"} {
		matches, err := filepath.Glob(filepath.Join(repositoryDir, pattern))
		if err != nil || len(matches) != 0 {
			t.Fatalf("blocked multi-ref push retained %s state: %v, err=%v", pattern, matches, err)
		}
	}
}

func TestNeoDiffCaptureManifestCapturedAtCompatibility(t *testing.T) {
	record := neoDiffCaptureRecord{
		CaptureID:  "0123456789abcdefghijklmn",
		CaptureRef: "refs/heads/amp/captures/0123456789abcdefghijklmn",
	}
	base := map[string]any{
		"version":    1,
		"captureID":  record.CaptureID,
		"captureRef": record.CaptureRef,
		"source": map[string]any{
			"repositoryRoot": "/tmp/diff-capture-fixture",
			"branch":         "main",
			"headSHA":        strings.Repeat("a", 40),
			"mergeBaseSHA":   nil,
		},
		"revisions": map[string]any{
			"projectedWorktreeSHA": strings.Repeat("b", 40),
		},
		"sections":      []any{},
		"rangeSections": []any{},
	}
	for _, test := range []struct {
		name       string
		capturedAt any
		want       any
		valid      bool
	}{
		{name: "epoch milliseconds", capturedAt: int64(1785100693111), want: int64(1785100693111), valid: true},
		{name: "RFC3339 string", capturedAt: "2026-07-26T21:18:13.111Z", want: "2026-07-26T21:18:13.111Z", valid: true},
		{name: "null", capturedAt: nil},
		{name: "fractional", capturedAt: 1785100693111.5},
		{name: "unsafe integer", capturedAt: uint64(1 << 53)},
		{name: "invalid string", capturedAt: "not-a-timestamp"},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := cloneMap(base)
			input["capturedAt"] = test.capturedAt
			raw, err := json.Marshal(input)
			if err != nil {
				t.Fatal(err)
			}
			var manifest neoDiffCaptureManifest
			if err := json.Unmarshal(raw, &manifest); err != nil {
				t.Fatal(err)
			}
			err = validateNeoDiffCaptureManifest(record, manifest)
			if !test.valid {
				if err == nil {
					t.Fatal("invalid capturedAt was accepted")
				}
				return
			}
			if err != nil {
				t.Fatalf("valid capturedAt rejected: %v", err)
			}
			latest := neoDiffCaptureSafeSubset(record, manifest)
			if latest["capturedAt"] != test.want {
				t.Fatalf("safe capturedAt = %#v, want %#v", latest["capturedAt"], test.want)
			}
		})
	}
}

func TestNeoDiffCaptureManifestBlobBatchValidation(t *testing.T) {
	repositoryDir := t.TempDir()
	neoDiffCaptureTestGit(t, repositoryDir, "init", "--quiet")
	firstPath := filepath.Join(repositoryDir, "first.txt")
	secondPath := filepath.Join(repositoryDir, "second.txt")
	if err := os.WriteFile(firstPath, []byte("first\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(secondPath, []byte("second\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	firstSHA := strings.TrimSpace(neoDiffCaptureTestGit(t, repositoryDir, "hash-object", "-w", firstPath))
	secondSHA := strings.TrimSpace(neoDiffCaptureTestGit(t, repositoryDir, "hash-object", "-w", secondPath))
	manifest := neoDiffCaptureManifest{}
	manifest.Sections = []neoDiffCaptureManifestSection{{
		Files: []neoDiffCaptureManifestFile{{OldBlobSHA: firstSHA, NewBlobSHA: secondSHA}},
	}}
	shas, err := validateNeoDiffCaptureManifestBlobs(context.Background(), repositoryDir, manifest)
	if err != nil || len(shas) != 2 || shas[0] != firstSHA || shas[1] != secondSHA {
		t.Fatalf("batch validation shas = %#v, error=%v", shas, err)
	}
	manifest.Sections[0].Files[0].NewBlobSHA = strings.Repeat("f", 40)
	if _, err := validateNeoDiffCaptureManifestBlobs(context.Background(), repositoryDir, manifest); err == nil {
		t.Fatal("batch validation accepted a missing blob")
	}
}

func TestNeoDiffCaptureManifestRevisionLineage(t *testing.T) {
	fixture := neoDiffCaptureTestRevisionRepository(t)
	if err := validateNeoDiffCaptureManifestRevisions(context.Background(), fixture.dir, fixture.manifest); err != nil {
		t.Fatalf("valid revision lineage rejected: %v", err)
	}
	mismatchedTree := neoDiffCaptureTestCloneManifest(t, fixture.manifest)
	mismatchedTree.ProjectedCommits[0].ProjectedCommitSHA = fixture.mismatchedTreeSHA
	if err := validateNeoDiffCaptureManifestRevisions(context.Background(), fixture.dir, mismatchedTree); err == nil || !strings.Contains(err.Error(), "contents do not match") {
		t.Fatalf("projected tree mismatch error = %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*neoDiffCaptureManifest)
	}{
		{name: "unrelated source commit", mutate: func(manifest *neoDiffCaptureManifest) {
			manifest.ProjectedCommits[0].SourceCommitSHA = fixture.unrelatedSHA
		}},
		{name: "omitted source commit", mutate: func(manifest *neoDiffCaptureManifest) {
			manifest.ProjectedCommits = manifest.ProjectedCommits[1:]
		}},
		{name: "reordered source commits", mutate: func(manifest *neoDiffCaptureManifest) {
			manifest.ProjectedCommits[0].SourceCommitSHA, manifest.ProjectedCommits[1].SourceCommitSHA = manifest.ProjectedCommits[1].SourceCommitSHA, manifest.ProjectedCommits[0].SourceCommitSHA
		}},
		{name: "broken projected parent", mutate: func(manifest *neoDiffCaptureManifest) {
			manifest.ProjectedCommits[1].ProjectedCommitSHA = fixture.brokenProjectedSHA
			manifest.Revisions.ProjectedHeadSHA = fixture.brokenProjectedSHA
			manifest.Revisions.ProjectedIndexSHA = fixture.brokenProjectedSHA
			manifest.Revisions.ProjectedWorktreeSHA = fixture.brokenProjectedSHA
		}},
		{name: "broken projected head", mutate: func(manifest *neoDiffCaptureManifest) {
			manifest.Revisions.ProjectedHeadSHA = manifest.ProjectedCommits[0].ProjectedCommitSHA
			manifest.Revisions.ProjectedIndexSHA = manifest.Revisions.ProjectedHeadSHA
			manifest.Revisions.ProjectedWorktreeSHA = manifest.Revisions.ProjectedHeadSHA
		}},
		{name: "broken projected index", mutate: func(manifest *neoDiffCaptureManifest) {
			manifest.Revisions.ProjectedIndexSHA = fixture.brokenIndexSHA
			manifest.Revisions.ProjectedWorktreeSHA = fixture.brokenIndexSHA
		}},
		{name: "broken projected worktree", mutate: func(manifest *neoDiffCaptureManifest) {
			manifest.Revisions.ProjectedWorktreeSHA = fixture.brokenWorktreeSHA
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := neoDiffCaptureTestCloneManifest(t, fixture.manifest)
			test.mutate(&candidate)
			if err := validateNeoDiffCaptureManifestRevisions(context.Background(), fixture.dir, candidate); err == nil {
				t.Fatal("invalid revision lineage was accepted")
			}
		})
	}
}

func TestNeoDiffCaptureLatestPointer(t *testing.T) {
	useTempNeoThreadStore(t)
	threadID := "T-019f9ad5-01de-71e4-99ab-a144b7974b38"
	if err := os.MkdirAll(neoDiffCaptureCapturesDir(threadID), 0o700); err != nil {
		t.Fatal(err)
	}
	older := neoDiffCaptureTestPublishedRecord(t, "0000000000000001", "2026-07-31T10:00:00Z", strings.Repeat("a", 40), nil)
	newer := neoDiffCaptureTestPublishedRecord(t, "0000000000000002", "2026-07-31T11:00:00Z", strings.Repeat("a", 40), nil)
	for _, record := range []neoDiffCaptureRecord{older, newer} {
		if err := writeNeoDiffCaptureRecord(neoDiffCaptureRecordPath(threadID, record.CaptureID), record); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(neoDiffCaptureLatestPath(threadID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("latest pointer existed before compatibility read: %v", err)
	}
	record, _, ok, err := latestNeoDiffCapture(threadID)
	if err != nil || !ok || record.CaptureID != newer.CaptureID {
		t.Fatalf("compatibility latest record=%#v ok=%v err=%v", record, ok, err)
	}
	if mode := neoDiffCaptureTestMode(t, neoDiffCaptureLatestPath(threadID)); mode != 0o600 {
		t.Fatalf("latest pointer mode = %o, want 600", mode)
	}
	if err := os.WriteFile(neoDiffCaptureRecordPath(threadID, "0000000000000003"), []byte("not-json\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	record, _, ok, err = latestNeoDiffCapture(threadID)
	if err != nil || !ok || record.CaptureID != newer.CaptureID {
		t.Fatalf("normal latest record=%#v ok=%v err=%v", record, ok, err)
	}
	if err := os.WriteFile(neoDiffCaptureLatestPath(threadID), []byte("not-json\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := latestNeoDiffCapture(threadID); err == nil {
		t.Fatal("corrupt latest pointer was treated as absent")
	}
	gin.SetMode(gin.TestMode)
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	response := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(response)
	c.Request = request
	(&AmpModule{}).serveNeoDiffCaptureLatest(c, threadID)
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("corrupt latest status = %d, body=%s", response.Code, response.Body.String())
	}
}

func TestNeoDiffCapturePendingPublishedRecovery(t *testing.T) {
	useTempNeoThreadStore(t)
	fixture := neoDiffCaptureTestRepository(t)
	threadID := "T-019f9ad5-01de-71e4-99ab-a144b7974b46"
	repositoryDir, err := neoEnsureDiffCaptureRepository(context.Background(), threadID, "sha1")
	if err != nil {
		t.Fatal(err)
	}
	neoDiffCaptureTestGit(t, repositoryDir, "fetch", "--quiet", fixture.dir, fixture.headSHA)
	older := neoDiffCaptureTestPublishedRecord(t, "0000000000000001", "2026-07-31T10:00:00Z", fixture.baseSHA, []string{fixture.oldBlobSHA})
	newer := neoDiffCaptureTestPublishedRecord(t, "0000000000000002", "2026-07-31T11:00:00Z", fixture.headSHA, []string{fixture.newBlobSHA})
	for _, record := range []neoDiffCaptureRecord{older, newer} {
		if err := writeNeoDiffCaptureRecord(neoDiffCaptureRecordPath(threadID, record.CaptureID), record); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := neoDiffCaptureGit(context.Background(), repositoryDir, []string{"update-ref", older.PublishedRef, fixture.baseSHA}, 256, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := neoDiffCaptureGit(context.Background(), repositoryDir, []string{"update-ref", newer.PublishedRef, fixture.headSHA}, 256, nil); err != nil {
		t.Fatal(err)
	}
	if err := writeNeoDiffCaptureLatestRecord(threadID, neoDiffCaptureLatestRecord{
		StorageVersion: 1,
		CaptureID:      older.CaptureID,
		PublishedAt:    older.PublishedAt,
		ManifestSHA256: older.ManifestSHA256,
	}); err != nil {
		t.Fatal(err)
	}
	index := newNeoDiffCaptureBlobIndex(nil).withCapture(older.CaptureID, older.ManifestSHA256, []string{fixture.oldBlobSHA})
	indexRaw, err := marshalNeoDiffCaptureBlobIndex(index)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeNeoDiffCaptureBlobIndex(threadID, indexRaw); err != nil {
		t.Fatal(err)
	}
	neoDiffCaptureBlobIndexes.delete(threadID)
	t.Cleanup(func() { neoDiffCaptureBlobIndexes.delete(threadID) })
	if err := writeNeoDiffCapturePendingPublication(threadID, newer.CaptureID); err != nil {
		t.Fatal(err)
	}
	if mode := neoDiffCaptureTestMode(t, neoDiffCapturePendingPublicationPath(threadID)); mode != 0o600 {
		t.Fatalf("pending publication mode = %o, want 600", mode)
	}
	record, _, ok, err := latestNeoDiffCapture(threadID)
	if err != nil || !ok || record.CaptureID != newer.CaptureID {
		t.Fatalf("recovered latest record=%#v ok=%v err=%v", record, ok, err)
	}
	pointer, err := readNeoDiffCaptureLatestRecord(threadID)
	if err != nil || pointer.CaptureID != newer.CaptureID {
		t.Fatalf("recovered latest pointer=%#v err=%v", pointer, err)
	}
	recoveredIndex, err := readNeoDiffCaptureBlobIndex(threadID)
	if err != nil || !recoveredIndex.allows(fixture.newBlobSHA) {
		t.Fatalf("recovered blob index=%#v err=%v", recoveredIndex, err)
	}
	entry, found := recoveredIndex.captures[newer.CaptureID]
	if !found || entry.manifestSHA256 != newer.ManifestSHA256 {
		t.Fatalf("recovered blob index entry=%#v found=%v", entry, found)
	}
	if _, err := os.Stat(neoDiffCapturePendingPublicationPath(threadID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("recovered pending publication stat error = %v", err)
	}
}

func TestNeoDiffCapturePendingPublishedRecoveryRejectsInvalidRef(t *testing.T) {
	useTempNeoThreadStore(t)
	fixture := neoDiffCaptureTestRepository(t)
	tests := []struct {
		name      string
		threadID  string
		refTarget string
	}{
		{name: "missing", threadID: "T-019f9ad5-01de-71e4-99ab-a144b7974b59"},
		{name: "mismatched", threadID: "T-019f9ad5-01de-71e4-99ab-a144b7974b5a", refTarget: fixture.baseSHA},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			repositoryDir, err := neoEnsureDiffCaptureRepository(context.Background(), test.threadID, "sha1")
			if err != nil {
				t.Fatal(err)
			}
			record := neoDiffCaptureTestPublishedRecord(t, "0000000000000001", "2026-07-31T11:00:00Z", fixture.headSHA, []string{fixture.newBlobSHA})
			if err := writeNeoDiffCaptureRecord(neoDiffCaptureRecordPath(test.threadID, record.CaptureID), record); err != nil {
				t.Fatal(err)
			}
			if test.refTarget != "" {
				neoDiffCaptureTestGit(t, repositoryDir, "fetch", "--quiet", fixture.dir, test.refTarget)
				if _, err := neoDiffCaptureGit(context.Background(), repositoryDir, []string{"update-ref", record.PublishedRef, test.refTarget}, 256, nil); err != nil {
					t.Fatal(err)
				}
			}
			if err := writeNeoDiffCapturePendingPublication(test.threadID, record.CaptureID); err != nil {
				t.Fatal(err)
			}
			if _, _, ok, err := latestNeoDiffCapture(test.threadID); err == nil || ok {
				t.Fatalf("invalid published ref recovery ok=%v err=%v", ok, err)
			}
			if _, err := os.Stat(neoDiffCapturePendingPublicationPath(test.threadID)); err != nil {
				t.Fatalf("invalid published ref recovery removed pending publication: %v", err)
			}
			if _, err := os.Stat(neoDiffCaptureLatestPath(test.threadID)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("invalid published ref recovery wrote latest pointer: %v", err)
			}
			if _, err := os.Stat(neoDiffCaptureBlobIndexPath(test.threadID)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("invalid published ref recovery wrote blob index: %v", err)
			}
		})
	}
}

func TestNeoDiffCapturePendingAllocatedRecovery(t *testing.T) {
	useTempNeoThreadStore(t)
	threadID := "T-019f9ad5-01de-71e4-99ab-a144b7974b47"
	if _, err := neoEnsureDiffCaptureRepository(context.Background(), threadID, "sha1"); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(neoDiffCaptureCapturesDir(threadID), 0o700); err != nil {
		t.Fatal(err)
	}
	olderSHA := strings.Repeat("a", 40)
	older := neoDiffCaptureTestPublishedRecord(t, "0000000000000001", "2026-07-31T10:00:00Z", strings.Repeat("a", 40), []string{olderSHA})
	allocated := neoDiffCaptureRecord{
		StorageVersion: 1,
		CaptureID:      "0000000000000002",
		CaptureRef:     "refs/heads/amp/captures/0000000000000002",
		CreatedAt:      "2026-07-31T11:00:00Z",
		State:          "allocated",
	}
	for _, record := range []neoDiffCaptureRecord{older, allocated} {
		if err := writeNeoDiffCaptureRecord(neoDiffCaptureRecordPath(threadID, record.CaptureID), record); err != nil {
			t.Fatal(err)
		}
	}
	if err := writeNeoDiffCaptureLatestRecord(threadID, neoDiffCaptureLatestRecord{
		StorageVersion: 1,
		CaptureID:      older.CaptureID,
		PublishedAt:    older.PublishedAt,
		ManifestSHA256: older.ManifestSHA256,
	}); err != nil {
		t.Fatal(err)
	}
	index := newNeoDiffCaptureBlobIndex(nil).withCapture(older.CaptureID, older.ManifestSHA256, []string{olderSHA})
	indexRaw, err := marshalNeoDiffCaptureBlobIndex(index)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeNeoDiffCaptureBlobIndex(threadID, indexRaw); err != nil {
		t.Fatal(err)
	}
	if err := writeNeoDiffCapturePendingPublication(threadID, allocated.CaptureID); err != nil {
		t.Fatal(err)
	}
	record, _, ok, err := latestNeoDiffCapture(threadID)
	if err != nil || !ok || record.CaptureID != older.CaptureID {
		t.Fatalf("allocated recovery latest record=%#v ok=%v err=%v", record, ok, err)
	}
	recoveredIndex, err := readNeoDiffCaptureBlobIndex(threadID)
	if err != nil {
		t.Fatal(err)
	}
	if _, found := recoveredIndex.captures[allocated.CaptureID]; found {
		t.Fatal("allocated pending capture was added to the blob index")
	}
	if _, err := os.Stat(neoDiffCapturePendingPublicationPath(threadID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("allocated pending publication stat error = %v", err)
	}
}

func TestNeoDiffCapturePendingAllocatedRefPromotionRecovery(t *testing.T) {
	useTempNeoThreadStore(t)
	fixture := neoDiffCaptureTestRepository(t)
	threadID := "T-019f9ad5-01de-71e4-99ab-a144b7974b56"
	captureID := "0000000000000001"
	captureRef := "refs/heads/amp/captures/" + captureID
	publishedRef := neoDiffCapturePublishedRef(captureID)
	repositoryDir, err := neoEnsureDiffCaptureRepository(context.Background(), threadID, "sha1")
	if err != nil {
		t.Fatal(err)
	}
	allocated := neoDiffCaptureRecord{
		StorageVersion: 1,
		CaptureID:      captureID,
		CaptureRef:     captureRef,
		CreatedAt:      "2026-07-31T11:00:00Z",
		State:          "allocated",
	}
	if err := writeNeoDiffCaptureRecord(neoDiffCaptureRecordPath(threadID, captureID), allocated); err != nil {
		t.Fatal(err)
	}
	neoDiffCaptureTestGit(t, fixture.dir, "push", repositoryDir, fixture.headSHA+":"+captureRef)
	if promoted, err := neoPromoteDiffCaptureRef(context.Background(), repositoryDir, captureRef, publishedRef, fixture.headSHA); err != nil || !promoted {
		t.Fatalf("promote ref promoted=%v err=%v", promoted, err)
	}
	if err := writeNeoDiffCapturePendingPublication(threadID, captureID); err != nil {
		t.Fatal(err)
	}
	if _, _, ok, err := latestNeoDiffCapture(threadID); err != nil || ok {
		t.Fatalf("allocated ref promotion recovery ok=%v err=%v", ok, err)
	}
	if got := strings.TrimSpace(neoDiffCaptureTestGit(t, repositoryDir, "rev-parse", captureRef)); got != fixture.headSHA {
		t.Fatalf("restored capture ref = %q, want %q", got, fixture.headSHA)
	}
	if _, err := neoDiffCaptureGit(context.Background(), repositoryDir, []string{"show-ref", "--verify", publishedRef}, 256, nil); err == nil {
		t.Fatal("allocated ref promotion recovery retained immutable ref")
	}
	if _, err := os.Stat(neoDiffCapturePendingPublicationPath(threadID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("allocated ref promotion pending publication stat error = %v", err)
	}
	record, err := readNeoDiffCaptureRecord(threadID, captureID)
	if err != nil || record.State != "allocated" {
		t.Fatalf("allocated ref promotion record=%#v err=%v", record, err)
	}
}

func TestNeoDiffCapturePendingAllocatedRefPromotionConflict(t *testing.T) {
	useTempNeoThreadStore(t)
	fixture := neoDiffCaptureTestRepository(t)
	threadID := "T-019f9ad5-01de-71e4-99ab-a144b7974b57"
	captureID := "0000000000000001"
	captureRef := "refs/heads/amp/captures/" + captureID
	publishedRef := neoDiffCapturePublishedRef(captureID)
	repositoryDir, err := neoEnsureDiffCaptureRepository(context.Background(), threadID, "sha1")
	if err != nil {
		t.Fatal(err)
	}
	allocated := neoDiffCaptureRecord{
		StorageVersion: 1,
		CaptureID:      captureID,
		CaptureRef:     captureRef,
		CreatedAt:      "2026-07-31T11:00:00Z",
		State:          "allocated",
	}
	if err := writeNeoDiffCaptureRecord(neoDiffCaptureRecordPath(threadID, captureID), allocated); err != nil {
		t.Fatal(err)
	}
	neoDiffCaptureTestGit(t, fixture.dir, "push", repositoryDir, fixture.headSHA+":"+captureRef)
	if promoted, err := neoPromoteDiffCaptureRef(context.Background(), repositoryDir, captureRef, publishedRef, fixture.headSHA); err != nil || !promoted {
		t.Fatalf("promote ref promoted=%v err=%v", promoted, err)
	}
	if _, err := neoDiffCaptureGit(context.Background(), repositoryDir, []string{"update-ref", captureRef, fixture.baseSHA}, 256, nil); err != nil {
		t.Fatal(err)
	}
	if err := writeNeoDiffCapturePendingPublication(threadID, captureID); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := latestNeoDiffCapture(threadID); err == nil {
		t.Fatal("conflicting allocated ref promotion recovery succeeded")
	}
	if _, err := os.Stat(neoDiffCapturePendingPublicationPath(threadID)); err != nil {
		t.Fatalf("conflicting recovery removed pending publication: %v", err)
	}
	if got := strings.TrimSpace(neoDiffCaptureTestGit(t, repositoryDir, "rev-parse", captureRef)); got != fixture.baseSHA {
		t.Fatalf("conflicting capture ref = %q, want %q", got, fixture.baseSHA)
	}
	if got := strings.TrimSpace(neoDiffCaptureTestGit(t, repositoryDir, "rev-parse", publishedRef)); got != fixture.headSHA {
		t.Fatalf("conflicting published ref = %q, want %q", got, fixture.headSHA)
	}
}

func TestNeoDiffCaptureCorruptPendingPublication(t *testing.T) {
	useTempNeoThreadStore(t)
	threadID := "T-019f9ad5-01de-71e4-99ab-a144b7974b48"
	if err := os.MkdirAll(neoDiffCaptureThreadDir(threadID), 0o700); err != nil {
		t.Fatal(err)
	}
	path := neoDiffCapturePendingPublicationPath(threadID)
	if err := os.WriteFile(path, []byte("not-json\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := latestNeoDiffCapture(threadID); err == nil {
		t.Fatal("corrupt pending publication was ignored")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("corrupt pending publication was removed: %v", err)
	}
}

func TestNeoDiffCaptureLatestPointerEmptyAndMonotonic(t *testing.T) {
	useTempNeoThreadStore(t)
	emptyThreadID := "T-019f9ad5-01de-71e4-99ab-a144b7974b39"
	if err := os.MkdirAll(neoDiffCaptureCapturesDir(emptyThreadID), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, _, ok, err := latestNeoDiffCapture(emptyThreadID); err != nil || ok {
		t.Fatalf("empty latest ok=%v err=%v", ok, err)
	}
	if err := os.WriteFile(neoDiffCaptureRecordPath(emptyThreadID, "0000000000000001"), []byte("not-json\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, ok, err := latestNeoDiffCapture(emptyThreadID); err != nil || ok {
		t.Fatalf("sentinel latest rescanned captures: ok=%v err=%v", ok, err)
	}

	threadID := "T-019f9ad5-01de-71e4-99ab-a144b7974b3a"
	if err := os.MkdirAll(neoDiffCaptureCapturesDir(threadID), 0o700); err != nil {
		t.Fatal(err)
	}
	older := neoDiffCaptureTestPublishedRecord(t, "0000000000000001", "2026-07-31T10:00:00Z", strings.Repeat("a", 40), nil)
	newerLowID := neoDiffCaptureTestPublishedRecord(t, "0000000000000002", "2026-07-31T11:00:00Z", strings.Repeat("a", 40), nil)
	newerHighID := neoDiffCaptureTestPublishedRecord(t, "0000000000000003", "2026-07-31T11:00:00Z", strings.Repeat("a", 40), nil)
	for _, record := range []neoDiffCaptureRecord{older, newerLowID, newerHighID} {
		if err := writeNeoDiffCaptureRecord(neoDiffCaptureRecordPath(threadID, record.CaptureID), record); err != nil {
			t.Fatal(err)
		}
	}
	if err := updateNeoDiffCaptureLatest(threadID, newerHighID); err != nil {
		t.Fatal(err)
	}
	if err := updateNeoDiffCaptureLatest(threadID, older); err != nil {
		t.Fatal(err)
	}
	if err := updateNeoDiffCaptureLatest(threadID, newerLowID); err != nil {
		t.Fatal(err)
	}
	record, _, ok, err := latestNeoDiffCapture(threadID)
	if err != nil || !ok || record.CaptureID != newerHighID.CaptureID {
		t.Fatalf("monotonic latest record=%#v ok=%v err=%v", record, ok, err)
	}
}

func TestNeoDiffCaptureManifestFileValidation(t *testing.T) {
	repositoryDir, baseSHA, headSHA, files := neoDiffCaptureTestFileValidationRepository(t)
	manifest := neoDiffCaptureManifest{}
	manifest.Source.HeadSHA = headSHA
	manifest.Source.MergeBaseSHA = baseSHA
	manifest.Revisions.ProjectedHeadSHA = headSHA
	manifest.Revisions.ProjectedIndexSHA = headSHA
	manifest.Revisions.ProjectedWorktreeSHA = headSHA
	manifest.ProjectedCommits = []neoDiffCaptureProjectedCommit{{SourceCommitSHA: headSHA, ProjectedCommitSHA: headSHA}}
	manifest.Sections = []neoDiffCaptureManifestSection{{ID: "working-tree", BaseCommitSHA: baseSHA, HeadCommitSHA: headSHA, Files: files}}
	manifest.RangeSections = []neoDiffCaptureRangeSection{
		{RangeID: "merge-base:" + baseSHA, Files: files},
		{RangeID: "commit:" + headSHA, Files: files},
	}
	if err := validateNeoDiffCaptureManifestFiles(context.Background(), repositoryDir, manifest); err != nil {
		t.Fatalf("valid manifest files rejected: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*neoDiffCaptureManifest)
	}{
		{name: "path", mutate: func(value *neoDiffCaptureManifest) { value.Sections[0].Files[0].Path = "missing.txt" }},
		{name: "invalid utf8 path", mutate: func(value *neoDiffCaptureManifest) { value.Sections[0].Files[0].Path = string([]byte{'x', 0xff}) }},
		{name: "repository traversal", mutate: func(value *neoDiffCaptureManifest) { value.Sections[0].Files[0].Path = "../outside.txt" }},
		{name: "old blob", mutate: func(value *neoDiffCaptureManifest) { value.Sections[0].Files[0].Old.BlobSHA = strings.Repeat("f", 40) }},
		{name: "new blob", mutate: func(value *neoDiffCaptureManifest) { value.Sections[0].Files[0].New.BlobSHA = strings.Repeat("f", 40) }},
		{name: "change type", mutate: func(value *neoDiffCaptureManifest) { value.Sections[0].Files[0].ChangeType = "added" }},
		{name: "untracked modified", mutate: func(value *neoDiffCaptureManifest) { value.Sections[0].Files[0].ChangeType = "untracked" }},
		{name: "is binary", mutate: func(value *neoDiffCaptureManifest) { value.Sections[0].Files[0].IsBinary = true }},
		{name: "diff stat added", mutate: func(value *neoDiffCaptureManifest) { value.Sections[0].Files[0].DiffStat.Added++ }},
		{name: "diff stat deleted", mutate: func(value *neoDiffCaptureManifest) { value.Sections[0].Files[0].DiffStat.Deleted++ }},
		{name: "diff stat changed", mutate: func(value *neoDiffCaptureManifest) { value.Sections[0].Files[0].DiffStat.Changed++ }},
		{name: "negative diff stat", mutate: func(value *neoDiffCaptureManifest) { value.Sections[0].Files[0].DiffStat.Added = -1 }},
		{name: "previous path", mutate: func(value *neoDiffCaptureManifest) { value.Sections[0].Files[3].PreviousPath = "missing-source.txt" }},
		{name: "old side path", mutate: func(value *neoDiffCaptureManifest) {
			value.Sections[0].Files[3].Old.Path = value.Sections[0].Files[3].Path
		}},
		{name: "revision", mutate: func(value *neoDiffCaptureManifest) { value.Sections[0].HeadCommitSHA = baseSHA }},
		{name: "missing revision", mutate: func(value *neoDiffCaptureManifest) { value.Source.HeadSHA = strings.Repeat("f", 40) }},
		{name: "omitted changed path", mutate: func(value *neoDiffCaptureManifest) {
			value.Sections[0].Files = value.Sections[0].Files[:len(value.Sections[0].Files)-1]
		}},
		{name: "empty changed file list", mutate: func(value *neoDiffCaptureManifest) { value.Sections[0].Files = nil }},
		{name: "extra unchanged path", mutate: func(value *neoDiffCaptureManifest) {
			extra := value.Sections[0].Files[4]
			extra.ID = "extra-unchanged"
			extra.Path = extra.PreviousPath
			extra.PreviousPath = ""
			extra.ChangeType = "modified"
			extra.Old.Path = extra.Path
			extra.New.Path = extra.Path
			value.Sections[0].Files = append(value.Sections[0].Files, extra)
		}},
		{name: "duplicate file id", mutate: func(value *neoDiffCaptureManifest) { value.Sections[0].Files[1].ID = value.Sections[0].Files[0].ID }},
		{name: "duplicate file path", mutate: func(value *neoDiffCaptureManifest) { value.Sections[0].Files[1].Path = value.Sections[0].Files[0].Path }},
		{name: "duplicate section id", mutate: func(value *neoDiffCaptureManifest) { value.Sections = append(value.Sections, value.Sections[0]) }},
		{name: "duplicate range id", mutate: func(value *neoDiffCaptureManifest) {
			value.RangeSections = append(value.RangeSections, value.RangeSections[0])
		}},
		{name: "missing full range", mutate: func(value *neoDiffCaptureManifest) { value.RangeSections = nil }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := neoDiffCaptureTestCloneManifest(t, manifest)
			test.mutate(&candidate)
			if err := validateNeoDiffCaptureManifestFiles(context.Background(), repositoryDir, candidate); err == nil {
				t.Fatal("invalid manifest files were accepted")
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := validateNeoDiffCaptureManifestFiles(ctx, repositoryDir, manifest); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled manifest validation error = %v", err)
	}
	for _, index := range []int{3, 4} {
		if files[index].DiffStat != (neoDiffCaptureDiffStat{}) {
			t.Fatalf("unchanged move stats for %s = %#v", files[index].ID, files[index].DiffStat)
		}
	}
	if binary := files[len(files)-1]; !binary.IsBinary || binary.DiffStat != (neoDiffCaptureDiffStat{Changed: 1}) {
		t.Fatalf("binary file display metadata = %#v", binary)
	}
}

func TestNeoDiffCaptureRejectsCopyWithMutatedSource(t *testing.T) {
	oldSHA := strings.Repeat("a", 40)
	newSHA := strings.Repeat("b", 40)
	file := neoDiffCaptureManifestFile{
		ID:           "copy",
		Path:         "destination.txt",
		PreviousPath: "source.txt",
		ChangeType:   "copied",
		Old:          neoDiffCaptureFileSide{Kind: "file", Path: "source.txt", BlobSHA: oldSHA},
		New:          neoDiffCaptureFileSide{Kind: "file", Path: "destination.txt", BlobSHA: oldSHA},
	}
	base := map[string]neoDiffCaptureTreeFile{"source.txt": {mode: "100644", sha: oldSHA}}
	head := map[string]neoDiffCaptureTreeFile{
		"source.txt":      {mode: "100644", sha: newSHA},
		"destination.txt": {mode: "100644", sha: oldSHA},
	}
	if err := neoDiffCaptureValidateManifestFile(file, map[string]struct{}{}, base, head); err == nil {
		t.Fatal("copy with a mutated source was accepted")
	}
}

func TestNeoDiffCaptureRejectsCopyWithChangedDestination(t *testing.T) {
	oldSHA := strings.Repeat("a", 40)
	newSHA := strings.Repeat("b", 40)
	file := neoDiffCaptureManifestFile{
		ID:           "copy",
		Path:         "destination.txt",
		PreviousPath: "source.txt",
		ChangeType:   "copied",
		Old:          neoDiffCaptureFileSide{Kind: "file", Path: "source.txt", BlobSHA: oldSHA},
		New:          neoDiffCaptureFileSide{Kind: "file", Path: "destination.txt", BlobSHA: newSHA},
	}
	base := map[string]neoDiffCaptureTreeFile{"source.txt": {mode: "100644", sha: oldSHA}}
	for _, destination := range []neoDiffCaptureTreeFile{
		{mode: "100644", sha: newSHA},
		{mode: "100755", sha: oldSHA},
	} {
		head := map[string]neoDiffCaptureTreeFile{
			"source.txt":      {mode: "100644", sha: oldSHA},
			"destination.txt": destination,
		}
		file.New.BlobSHA = destination.sha
		if err := neoDiffCaptureValidateManifestFile(file, map[string]struct{}{file.Path: {}}, base, head); err == nil {
			t.Fatal("copy with a changed destination was accepted")
		}
	}
}

func TestNeoDiffCaptureAcceptsRenameWithReplacement(t *testing.T) {
	dir := t.TempDir()
	neoDiffCaptureTestGit(t, dir, "init", "--quiet")
	neoDiffCaptureTestGit(t, dir, "config", "user.email", "diff-capture@example.invalid")
	neoDiffCaptureTestGit(t, dir, "config", "user.name", "Diff Capture Test")
	if err := os.WriteFile(filepath.Join(dir, "source.txt"), []byte("original\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	neoDiffCaptureTestGit(t, dir, "add", "--all")
	neoDiffCaptureTestGit(t, dir, "commit", "--quiet", "-m", "base")
	baseSHA := strings.TrimSpace(neoDiffCaptureTestGit(t, dir, "rev-parse", "HEAD"))
	oldBlobSHA := strings.TrimSpace(neoDiffCaptureTestGit(t, dir, "rev-parse", "HEAD:source.txt"))
	if err := os.Rename(filepath.Join(dir, "source.txt"), filepath.Join(dir, "renamed.txt")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "source.txt"), []byte("replacement\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	neoDiffCaptureTestGit(t, dir, "add", "--all")
	neoDiffCaptureTestGit(t, dir, "commit", "--quiet", "-m", "rename and replace")
	headSHA := strings.TrimSpace(neoDiffCaptureTestGit(t, dir, "rev-parse", "HEAD"))
	replacementBlobSHA := strings.TrimSpace(neoDiffCaptureTestGit(t, dir, "rev-parse", "HEAD:source.txt"))
	files := []neoDiffCaptureManifestFile{
		{
			ID:           "rename",
			Path:         "renamed.txt",
			PreviousPath: "source.txt",
			ChangeType:   "renamed",
			Old:          neoDiffCaptureFileSide{Kind: "file", Path: "source.txt", BlobSHA: oldBlobSHA},
			New:          neoDiffCaptureFileSide{Kind: "file", Path: "renamed.txt", BlobSHA: oldBlobSHA},
			DiffStat:     neoDiffCaptureDiffStat{},
		},
		{
			ID:         "replacement",
			Path:       "source.txt",
			ChangeType: "modified",
			Old:        neoDiffCaptureFileSide{Kind: "file", Path: "source.txt", BlobSHA: oldBlobSHA},
			New:        neoDiffCaptureFileSide{Kind: "file", Path: "source.txt", BlobSHA: replacementBlobSHA},
			DiffStat:   neoDiffCaptureDiffStat{Added: 1, Deleted: 1, Changed: 1},
		},
	}
	manifest := neoDiffCaptureManifest{}
	manifest.Source.HeadSHA = headSHA
	manifest.Source.MergeBaseSHA = baseSHA
	manifest.Revisions.ProjectedHeadSHA = headSHA
	manifest.Revisions.ProjectedIndexSHA = headSHA
	manifest.Revisions.ProjectedWorktreeSHA = headSHA
	manifest.ProjectedCommits = []neoDiffCaptureProjectedCommit{{SourceCommitSHA: headSHA, ProjectedCommitSHA: headSHA}}
	manifest.Sections = []neoDiffCaptureManifestSection{{ID: "working-tree", BaseCommitSHA: baseSHA, HeadCommitSHA: headSHA, Files: files}}
	manifest.RangeSections = []neoDiffCaptureRangeSection{{RangeID: "merge-base:" + baseSHA, Files: files}}
	if err := validateNeoDiffCaptureManifestFiles(context.Background(), dir, manifest); err != nil {
		t.Fatalf("rename with replacement rejected: %v", err)
	}
	manifest.Sections[0].Files = manifest.Sections[0].Files[:1]
	manifest.RangeSections[0].Files = manifest.RangeSections[0].Files[:1]
	if err := validateNeoDiffCaptureManifestFiles(context.Background(), dir, manifest); err == nil {
		t.Fatal("rename with unrepresented replacement was accepted")
	}
}

func TestNeoDiffCaptureAcceptsStructuralRenameNumStats(t *testing.T) {
	dir := t.TempDir()
	neoDiffCaptureTestGit(t, dir, "init", "--quiet")
	neoDiffCaptureTestGit(t, dir, "config", "user.email", "diff-capture@example.invalid")
	neoDiffCaptureTestGit(t, dir, "config", "user.name", "Diff Capture Test")
	oldContent := strings.Repeat("old line\n", 10)
	if err := os.WriteFile(filepath.Join(dir, "source.txt"), []byte(oldContent), 0o600); err != nil {
		t.Fatal(err)
	}
	neoDiffCaptureTestGit(t, dir, "add", "--all")
	neoDiffCaptureTestGit(t, dir, "commit", "--quiet", "-m", "base")
	baseSHA := strings.TrimSpace(neoDiffCaptureTestGit(t, dir, "rev-parse", "HEAD"))
	oldBlobSHA := strings.TrimSpace(neoDiffCaptureTestGit(t, dir, "rev-parse", "HEAD:source.txt"))
	if err := os.Remove(filepath.Join(dir, "source.txt")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "destination.txt"), []byte("new one\nnew two\nnew three\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	neoDiffCaptureTestGit(t, dir, "add", "--all")
	neoDiffCaptureTestGit(t, dir, "commit", "--quiet", "-m", "structural rename")
	headSHA := strings.TrimSpace(neoDiffCaptureTestGit(t, dir, "rev-parse", "HEAD"))
	newBlobSHA := strings.TrimSpace(neoDiffCaptureTestGit(t, dir, "rev-parse", "HEAD:destination.txt"))
	file := neoDiffCaptureManifestFile{
		ID:           "rename",
		Path:         "destination.txt",
		PreviousPath: "source.txt",
		ChangeType:   "renamed",
		Old:          neoDiffCaptureFileSide{Kind: "file", Path: "source.txt", BlobSHA: oldBlobSHA},
		New:          neoDiffCaptureFileSide{Kind: "file", Path: "destination.txt", BlobSHA: newBlobSHA},
		DiffStat:     neoDiffCaptureDiffStat{Added: 3, Deleted: 10, Changed: 10},
	}
	manifest := neoDiffCaptureManifest{}
	manifest.Source.HeadSHA = headSHA
	manifest.Source.MergeBaseSHA = baseSHA
	manifest.Revisions.ProjectedHeadSHA = headSHA
	manifest.Revisions.ProjectedIndexSHA = headSHA
	manifest.Revisions.ProjectedWorktreeSHA = headSHA
	manifest.ProjectedCommits = []neoDiffCaptureProjectedCommit{{SourceCommitSHA: headSHA, ProjectedCommitSHA: headSHA}}
	manifest.Sections = []neoDiffCaptureManifestSection{{ID: "working-tree", BaseCommitSHA: baseSHA, HeadCommitSHA: headSHA, Files: []neoDiffCaptureManifestFile{file}}}
	manifest.RangeSections = []neoDiffCaptureRangeSection{{RangeID: "merge-base:" + baseSHA, Files: []neoDiffCaptureManifestFile{file}}}
	stats, err := neoDiffCaptureReadNumStats(context.Background(), dir, baseSHA, headSHA, neoDiffCaptureManifestMaxPathBytes)
	if err != nil {
		t.Fatal(err)
	}
	if len(stats.entries) != 2 {
		t.Fatalf("structural rename numstat entries = %#v", stats.entries)
	}
	if err := validateNeoDiffCaptureManifestFiles(context.Background(), dir, manifest); err != nil {
		t.Fatalf("structural rename rejected: %v", err)
	}
}

func TestNeoDiffCaptureCompatibilityScanRecordLimit(t *testing.T) {
	useTempNeoThreadStore(t)
	threadID := "T-019f9ad5-01de-71e4-99ab-a144b7974b41"
	if err := os.MkdirAll(neoDiffCaptureCapturesDir(threadID), 0o700); err != nil {
		t.Fatal(err)
	}
	for index := 0; index <= neoDiffCaptureScanMaxRecords; index++ {
		captureID := fmt.Sprintf("%016d", index)
		record := neoDiffCaptureRecord{
			StorageVersion: 1,
			CaptureID:      captureID,
			CaptureRef:     "refs/heads/amp/captures/" + captureID,
			CreatedAt:      "2026-07-31T10:00:00Z",
			State:          "allocated",
		}
		raw, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(neoDiffCaptureRecordPath(threadID, captureID), raw, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := publishedNeoDiffCaptures(threadID); err == nil || !strings.Contains(err.Error(), "record limit") {
		t.Fatalf("compatibility scan limit error = %v", err)
	}
}

func TestNeoDiffCaptureCompatibilityScanByteLimit(t *testing.T) {
	useTempNeoThreadStore(t)
	threadID := "T-019f9ad5-01de-71e4-99ab-a144b7974b42"
	if err := os.MkdirAll(neoDiffCaptureCapturesDir(threadID), 0o700); err != nil {
		t.Fatal(err)
	}
	path := neoDiffCaptureRecordPath(threadID, "0000000000000001")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, neoDiffCaptureScanMaxBytes+1); err != nil {
		t.Fatal(err)
	}
	if _, err := publishedNeoDiffCaptures(threadID); err == nil || !strings.Contains(err.Error(), "byte limit") {
		t.Fatalf("compatibility scan byte limit error = %v", err)
	}
}

func TestNeoDiffCaptureAllocationPrunesStaleRecords(t *testing.T) {
	gin.SetMode(gin.TestMode)
	useTempNeoThreadStore(t)
	threadID := "T-019f9ad5-01de-71e4-99ab-a144b7974b43"
	if err := os.MkdirAll(neoDiffCaptureCapturesDir(threadID), 0o700); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < neoDiffCaptureRetainedAllocations+2; index++ {
		captureID := fmt.Sprintf("%016d", index)
		record := neoDiffCaptureRecord{
			StorageVersion: 1,
			CaptureID:      captureID,
			CaptureRef:     "refs/heads/amp/captures/" + captureID,
			CreatedAt:      "2026-07-31T10:00:00Z",
			State:          "allocated",
		}
		raw, err := marshalNeoDiffCaptureRecord(record)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(neoDiffCaptureRecordPath(threadID, captureID), raw, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
	response := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(response)
	c.Request = request
	(&AmpModule{}).serveNeoDiffCaptureAllocate(c, threadID)
	if response.Code != http.StatusOK {
		t.Fatalf("allocation after stale record pruning status = %d, body=%s", response.Code, response.Body.String())
	}
	usage, err := neoDiffCaptureRecordStorageUsage(threadID)
	if err != nil {
		t.Fatal(err)
	}
	if usage.recordCount != 1 {
		t.Fatalf("record count after stale record pruning = %d, want 1", usage.recordCount)
	}
}

func TestNeoDiffCaptureStoragePrunesPublishedRecordsAndRebuildsIndexes(t *testing.T) {
	useTempNeoThreadStore(t)
	fixture := neoDiffCaptureTestRepository(t)
	threadID := "T-019f9ad5-01de-71e4-99ab-a144b7974b4f"
	repositoryDir, err := neoEnsureDiffCaptureRepository(context.Background(), threadID, "sha1")
	if err != nil {
		t.Fatal(err)
	}
	records := make([]neoDiffCaptureRecord, 0, neoDiffCaptureRetainedPublished+2)
	for index := 0; index < neoDiffCaptureRetainedPublished+2; index++ {
		captureID := fmt.Sprintf("%016d", index+1)
		publishedAt := time.Date(2026, 8, 1, 10, index, 0, 0, time.UTC).Format(time.RFC3339Nano)
		record := neoDiffCaptureTestPublishedRecord(t, captureID, publishedAt, fixture.headSHA, []string{fixture.oldBlobSHA})
		allocated := record
		allocated.PublishedRef = ""
		allocated.PublishedAt = ""
		allocated.State = "allocated"
		allocated.ManifestSHA256 = ""
		allocated.Manifest = nil
		if err := writeNeoDiffCaptureRecord(neoDiffCaptureRecordPath(threadID, captureID), allocated); err != nil {
			t.Fatal(err)
		}
		neoDiffCaptureTestGit(t, fixture.dir, "push", repositoryDir, fixture.headSHA+":"+record.CaptureRef)
		if _, err := neoDiffCaptureGit(context.Background(), repositoryDir, []string{"update-ref", record.PublishedRef, fixture.headSHA}, 4096, nil); err != nil {
			t.Fatal(err)
		}
		if err := writeNeoDiffCaptureRecord(neoDiffCaptureRecordPath(threadID, captureID), record); err != nil {
			t.Fatal(err)
		}
		records = append(records, record)
	}
	latest := records[len(records)-1]
	if err := updateNeoDiffCaptureLatest(threadID, latest); err != nil {
		t.Fatal(err)
	}
	index, err := rebuildNeoDiffCaptureBlobIndex(threadID)
	if err != nil {
		t.Fatal(err)
	}
	indexRaw, err := marshalNeoDiffCaptureBlobIndex(index)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeNeoDiffCaptureBlobIndex(threadID, indexRaw); err != nil {
		t.Fatal(err)
	}
	neoDiffCaptureBlobIndexes.put(threadID, index)
	t.Cleanup(func() { neoDiffCaptureBlobIndexes.delete(threadID) })
	if err := writeNeoDiffCapturePendingPrune(threadID, []string{records[0].CaptureID}); err != nil {
		t.Fatal(err)
	}
	if err := removeNeoDiffCaptureLatest(threadID); err != nil {
		t.Fatal(err)
	}
	if err := removeNeoDiffCaptureBlobIndex(threadID); err != nil {
		t.Fatal(err)
	}
	neoDiffCaptureBlobIndexes.delete(threadID)
	if err := os.Remove(neoDiffCaptureRecordPath(threadID, records[0].CaptureID)); err != nil {
		t.Fatal(err)
	}
	if err := neoPruneDiffCaptureStorage(context.Background(), threadID, latest.CaptureID, false); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(neoDiffCapturePendingPrunePath(threadID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("completed retention left pending prune: %v", err)
	}
	published, err := publishedNeoDiffCaptures(threadID)
	if err != nil {
		t.Fatal(err)
	}
	if len(published) != neoDiffCaptureRetainedPublished {
		t.Fatalf("retained published records = %d, want %d", len(published), neoDiffCaptureRetainedPublished)
	}
	retainedLatest, _, ok, err := latestNeoDiffCapture(threadID)
	if err != nil || !ok || retainedLatest.CaptureID != latest.CaptureID {
		t.Fatalf("latest after retention = %#v, ok=%v, err=%v", retainedLatest, ok, err)
	}
	rebuilt, err := readNeoDiffCaptureBlobIndex(threadID)
	if err != nil || len(rebuilt.captures) != neoDiffCaptureRetainedPublished {
		t.Fatalf("rebuilt index captures = %d, err=%v", len(rebuilt.captures), err)
	}
	for index, record := range records {
		_, refErr := neoDiffCaptureGit(context.Background(), repositoryDir, []string{"show-ref", "--verify", record.CaptureRef}, 256, nil)
		_, publishedRefErr := neoDiffCaptureGit(context.Background(), repositoryDir, []string{"show-ref", "--verify", record.PublishedRef}, 256, nil)
		_, recordErr := os.Stat(neoDiffCaptureRecordPath(threadID, record.CaptureID))
		retained := index >= len(records)-neoDiffCaptureRetainedPublished
		if retained && (refErr != nil || publishedRefErr != nil || recordErr != nil) {
			t.Fatalf("retained capture %s: ref=%v published-ref=%v record=%v", record.CaptureID, refErr, publishedRefErr, recordErr)
		}
		if !retained && (refErr == nil || publishedRefErr == nil || !errors.Is(recordErr, os.ErrNotExist)) {
			t.Fatalf("pruned capture %s: ref=%v published-ref=%v record=%v", record.CaptureID, refErr, publishedRefErr, recordErr)
		}
	}
}

func TestNeoDiffCaptureStoragePruneFailureInvalidatesBlobIndex(t *testing.T) {
	useTempNeoThreadStore(t)
	threadID := "T-019f9ad5-01de-71e4-99ab-a144b7974b59"
	repositoryDir, err := neoEnsureDiffCaptureRepository(context.Background(), threadID, "sha1")
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < neoDiffCaptureRetainedAllocations+1; index++ {
		captureID := fmt.Sprintf("%016d", index+1)
		record := neoDiffCaptureRecord{
			StorageVersion: 1,
			CaptureID:      captureID,
			CaptureRef:     "refs/heads/amp/captures/" + captureID,
			CreatedAt:      time.Now().Add(-time.Duration(index) * time.Minute).UTC().Format(time.RFC3339Nano),
			State:          "allocated",
		}
		if err := writeNeoDiffCaptureRecord(neoDiffCaptureRecordPath(threadID, captureID), record); err != nil {
			t.Fatal(err)
		}
	}
	staleSHA := strings.Repeat("a", 40)
	staleIndex := newNeoDiffCaptureBlobIndex(nil).withCapture("0000000000000001", strings.Repeat("b", 64), []string{staleSHA})
	staleRaw, err := marshalNeoDiffCaptureBlobIndex(staleIndex)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeNeoDiffCaptureBlobIndex(threadID, staleRaw); err != nil {
		t.Fatal(err)
	}
	if err := writeNeoDiffCaptureLatestRecord(threadID, neoDiffCaptureLatestRecord{
		StorageVersion: 1,
		CaptureID:      "0000000000000001",
		PublishedAt:    time.Now().UTC().Format(time.RFC3339Nano),
		ManifestSHA256: strings.Repeat("b", 64),
	}); err != nil {
		t.Fatal(err)
	}
	neoDiffCaptureBlobIndexes.put(threadID, staleIndex)
	t.Cleanup(func() { neoDiffCaptureBlobIndexes.delete(threadID) })
	if err := os.RemoveAll(repositoryDir); err != nil {
		t.Fatal(err)
	}
	if err := neoPruneDiffCaptureStorage(context.Background(), threadID, "", false); err == nil {
		t.Fatal("storage prune without repository succeeded")
	}
	if _, err := os.Stat(neoDiffCaptureBlobIndexPath(threadID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed prune retained stale blob index: %v", err)
	}
	if _, err := os.Stat(neoDiffCaptureLatestPath(threadID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed prune retained stale latest pointer: %v", err)
	}
	if _, ok := neoDiffCaptureBlobIndexes.get(threadID); ok {
		t.Fatal("failed prune retained cached blob index")
	}
	usage, err := neoDiffCaptureRecordStorageUsage(threadID)
	if err != nil || usage.recordCount != neoDiffCaptureRetainedAllocations {
		t.Fatalf("failed prune record count=%d err=%v", usage.recordCount, err)
	}
	if _, err := os.Stat(neoDiffCapturePendingPrunePath(threadID)); err != nil {
		t.Fatalf("failed prune did not retain recovery journal: %v", err)
	}
	rebuilt, err := neoDiffCaptureBlobIndexForThread(threadID)
	if err != nil {
		t.Fatal(err)
	}
	if rebuilt.allows(staleSHA) {
		t.Fatal("rebuilt blob index retained stale authorization")
	}
	if _, _, ok, err := latestNeoDiffCapture(threadID); err != nil || ok {
		t.Fatalf("rebuilt latest after failed prune ok=%v err=%v", ok, err)
	}
}

func TestNeoDiffCaptureStorageGCPrunesUnreachableObjects(t *testing.T) {
	useTempNeoThreadStore(t)
	threadID := "T-019f9ad5-01de-71e4-99ab-a144b7974b50"
	repositoryDir, err := neoEnsureDiffCaptureRepository(context.Background(), threadID, "sha1")
	if err != nil {
		t.Fatal(err)
	}
	sha, err := neoDiffCaptureGit(context.Background(), repositoryDir, []string{"hash-object", "-w", "--stdin"}, 128, strings.NewReader("unreachable\n"))
	if err != nil {
		t.Fatal(err)
	}
	sha = strings.TrimSpace(sha)
	if _, err := neoDiffCaptureGit(context.Background(), repositoryDir, []string{"cat-file", "-e", sha}, 128, nil); err != nil {
		t.Fatal(err)
	}
	if err := neoBoundDiffCaptureRepository(context.Background(), threadID, true); err != nil {
		t.Fatal(err)
	}
	if _, err := neoDiffCaptureGit(context.Background(), repositoryDir, []string{"cat-file", "-e", sha}, 128, nil); err == nil {
		t.Fatal("unreachable Git object survived explicit diff capture GC")
	}
}

func TestNeoDiffCaptureIndexAuthorizesLaterValidCandidate(t *testing.T) {
	useTempNeoThreadStore(t)
	threadID := "T-019f9ad5-01de-71e4-99ab-a144b7974b51"
	if err := os.MkdirAll(neoDiffCaptureCapturesDir(threadID), 0o700); err != nil {
		t.Fatal(err)
	}
	sha := strings.Repeat("a", 40)
	corruptID := "0000000000000001"
	valid := neoDiffCaptureTestPublishedRecord(t, "0000000000000002", "2026-08-01T10:00:00Z", strings.Repeat("a", 40), []string{sha})
	if err := os.WriteFile(neoDiffCaptureRecordPath(threadID, corruptID), []byte("not-json\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := writeNeoDiffCaptureRecord(neoDiffCaptureRecordPath(threadID, valid.CaptureID), valid); err != nil {
		t.Fatal(err)
	}
	index := newNeoDiffCaptureBlobIndex(nil).
		withCapture(corruptID, strings.Repeat("f", 64), []string{sha}).
		withCapture(valid.CaptureID, valid.ManifestSHA256, []string{sha})
	authorized, err := neoDiffCaptureIndexAuthorizes(threadID, index, sha)
	if err != nil || !authorized {
		t.Fatalf("later valid authorization = %v, err=%v", authorized, err)
	}
}

func TestNeoDiffCaptureRefPromotionRejectsConcurrentUpdate(t *testing.T) {
	useTempNeoThreadStore(t)
	fixture := neoDiffCaptureTestRepository(t)
	threadID := "T-019f9ad5-01de-71e4-99ab-a144b7974b52"
	captureID := "0000000000000001"
	captureRef := "refs/heads/amp/captures/" + captureID
	publishedRef := neoDiffCapturePublishedRef(captureID)
	repositoryDir, err := neoEnsureDiffCaptureRepository(context.Background(), threadID, "sha1")
	if err != nil {
		t.Fatal(err)
	}
	allocated := neoDiffCaptureRecord{
		StorageVersion: 1,
		CaptureID:      captureID,
		CaptureRef:     captureRef,
		CreatedAt:      "2026-08-01T10:00:00Z",
		State:          "allocated",
	}
	if err := writeNeoDiffCaptureRecord(neoDiffCaptureRecordPath(threadID, captureID), allocated); err != nil {
		t.Fatal(err)
	}
	neoDiffCaptureTestGit(t, fixture.dir, "push", repositoryDir, fixture.baseSHA+":"+captureRef)
	neoDiffCaptureTestGit(t, fixture.dir, "push", repositoryDir, fixture.headSHA+":"+captureRef)
	if promoted, err := neoPromoteDiffCaptureRef(context.Background(), repositoryDir, captureRef, publishedRef, fixture.baseSHA); err == nil || promoted {
		t.Fatalf("concurrently advanced capture ref promoted=%v err=%v", promoted, err)
	}
	if _, err := neoDiffCaptureGit(context.Background(), repositoryDir, []string{"show-ref", "--verify", publishedRef}, 256, nil); err == nil {
		t.Fatal("failed promotion created an immutable published ref")
	}
}

func TestNeoDiffCaptureDeleteRejectsPendingPublication(t *testing.T) {
	gin.SetMode(gin.TestMode)
	useTempNeoThreadStore(t)
	fixture := neoDiffCaptureTestRepository(t)
	threadID := "T-019f9ad5-01de-71e4-99ab-a144b7974b58"
	captureID := "0000000000000001"
	captureRef := "refs/heads/amp/captures/" + captureID
	publishedRef := neoDiffCapturePublishedRef(captureID)
	repositoryDir, err := neoEnsureDiffCaptureRepository(context.Background(), threadID, "sha1")
	if err != nil {
		t.Fatal(err)
	}
	record := neoDiffCaptureRecord{
		StorageVersion: 1,
		CaptureID:      captureID,
		CaptureRef:     captureRef,
		CreatedAt:      "2026-08-01T10:00:00Z",
		State:          "allocated",
	}
	if err := writeNeoDiffCaptureRecord(neoDiffCaptureRecordPath(threadID, captureID), record); err != nil {
		t.Fatal(err)
	}
	neoDiffCaptureTestGit(t, fixture.dir, "push", repositoryDir, fixture.headSHA+":"+captureRef)
	if promoted, err := neoPromoteDiffCaptureRef(context.Background(), repositoryDir, captureRef, publishedRef, fixture.headSHA); err != nil || !promoted {
		t.Fatalf("promote ref promoted=%v err=%v", promoted, err)
	}
	if err := writeNeoDiffCapturePendingPublication(threadID, captureID); err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(response)
	c.Request = httptest.NewRequest(http.MethodDelete, "/", nil)
	(&AmpModule{}).serveNeoDiffCaptureDelete(c, threadID, captureID)
	if response.Code != http.StatusConflict {
		t.Fatalf("pending-publication delete status = %d, body=%s", response.Code, response.Body.String())
	}
	if _, err := os.Stat(neoDiffCapturePendingPublicationPath(threadID)); err != nil {
		t.Fatalf("pending-publication delete removed marker: %v", err)
	}
	if _, err := os.Stat(neoDiffCaptureRecordPath(threadID, captureID)); err != nil {
		t.Fatalf("pending-publication delete removed record: %v", err)
	}
	if got := strings.TrimSpace(neoDiffCaptureTestGit(t, repositoryDir, "rev-parse", publishedRef)); got != fixture.headSHA {
		t.Fatalf("pending-publication delete changed published ref = %q, want %q", got, fixture.headSHA)
	}
}

func TestNeoDiffCaptureDeleteRejectsUnknownState(t *testing.T) {
	gin.SetMode(gin.TestMode)
	useTempNeoThreadStore(t)
	threadID := "T-019f9ad5-01de-71e4-99ab-a144b7974b53"
	captureID := "0000000000000001"
	if _, err := neoEnsureDiffCaptureRepository(context.Background(), threadID, "sha1"); err != nil {
		t.Fatal(err)
	}
	record := neoDiffCaptureRecord{
		StorageVersion: 1,
		CaptureID:      captureID,
		CaptureRef:     "refs/heads/amp/captures/" + captureID,
		CreatedAt:      "2026-08-01T10:00:00Z",
		State:          "publshed",
	}
	if err := writeNeoDiffCaptureRecord(neoDiffCaptureRecordPath(threadID, captureID), record); err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(response)
	c.Request = httptest.NewRequest(http.MethodDelete, "/", nil)
	(&AmpModule{}).serveNeoDiffCaptureDelete(c, threadID, captureID)
	if response.Code != http.StatusConflict {
		t.Fatalf("unknown-state delete status = %d, body=%s", response.Code, response.Body.String())
	}
	if _, err := os.Stat(neoDiffCaptureRecordPath(threadID, captureID)); err != nil {
		t.Fatalf("unknown-state record was removed: %v", err)
	}
}

func TestNeoDiffCaptureAllocationReportsCorruptMetadata(t *testing.T) {
	gin.SetMode(gin.TestMode)
	useTempNeoThreadStore(t)
	threadID := "T-019f9ad5-01de-71e4-99ab-a144b7974b44"
	if err := os.MkdirAll(neoDiffCaptureCapturesDir(threadID), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(neoDiffCaptureRecordPath(threadID, "0000000000000001"), []byte("not-json\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
	response := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(response)
	c.Request = request
	(&AmpModule{}).serveNeoDiffCaptureAllocate(c, threadID)
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("allocation with corrupt metadata status = %d, body=%s", response.Code, response.Body.String())
	}
}

func TestNeoDiffCapturePublishByteLimit(t *testing.T) {
	gin.SetMode(gin.TestMode)
	useTempNeoThreadStore(t)
	threadID := "T-019f9ad5-01de-71e4-99ab-a144b7974b45"
	captureID := "0123456789abcdefghijklmn"
	captureRef := "refs/heads/amp/captures/" + captureID
	repositoryDir, err := neoEnsureDiffCaptureRepository(context.Background(), threadID, "sha1")
	if err != nil {
		t.Fatal(err)
	}
	allocated := neoDiffCaptureRecord{
		StorageVersion: 1,
		CaptureID:      captureID,
		CaptureRef:     captureRef,
		CreatedAt:      "2026-07-31T10:00:00Z",
		State:          "allocated",
	}
	if err := writeNeoDiffCaptureRecord(neoDiffCaptureRecordPath(threadID, captureID), allocated); err != nil {
		t.Fatal(err)
	}
	fixture := neoDiffCaptureTestRepository(t)
	neoDiffCaptureTestGit(t, fixture.dir, "push", repositoryDir, fixture.headSHA+":"+captureRef)
	allocatedInfo, err := os.Stat(neoDiffCaptureRecordPath(threadID, captureID))
	if err != nil {
		t.Fatal(err)
	}
	fillerBytes := neoDiffCaptureScanMaxBytes - int(allocatedInfo.Size())
	for index := 0; index < 4; index++ {
		captureID := fmt.Sprintf("%016d", index)
		size := fillerBytes / (4 - index)
		fillerBytes -= size
		raw := neoDiffCaptureTestSizedRecord(t, captureID, size)
		if err := os.WriteFile(neoDiffCaptureRecordPath(threadID, captureID), raw, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	manifestRaw, err := json.Marshal(neoDiffCaptureTestManifest(captureID, captureRef, fixture))
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(manifestRaw))
	response := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(response)
	c.Request = request
	(&AmpModule{}).serveNeoDiffCapturePublish(c, threadID, captureID)
	if response.Code != http.StatusInsufficientStorage {
		t.Fatalf("publish beyond byte cap status = %d, body=%s", response.Code, response.Body.String())
	}
}

func TestNeoCaptureGitDiffReviewSnapshotUTF8Filename(t *testing.T) {
	dir := t.TempDir()
	neoDiffCaptureTestGit(t, dir, "init", "--quiet")
	neoDiffCaptureTestGit(t, dir, "config", "user.email", "review@example.invalid")
	neoDiffCaptureTestGit(t, dir, "config", "user.name", "Review Test")
	filename := "café.go"
	if err := os.WriteFile(filepath.Join(dir, filename), []byte("package cafe\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	neoDiffCaptureTestGit(t, dir, "add", "--", filename)
	neoDiffCaptureTestGit(t, dir, "commit", "--quiet", "-m", "base")
	if err := os.WriteFile(filepath.Join(dir, filename), []byte("package cafe\n\nconst Changed = true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	snapshot, err := neoCaptureGitDiffReviewSnapshot(dir, nil, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Files) != 1 || snapshot.Files[0] != filename {
		t.Fatalf("snapshot files = %#v", snapshot.Files)
	}
	if diff := snapshot.Diffs[filename]; !strings.Contains(diff, "diff --git a/"+filename+" b/"+filename) {
		t.Fatalf("UTF-8 filename patch did not map to metadata: %q", diff)
	}
}

func TestNeoDiffCaptureAuthorizedMissingBlobIsServerError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	useTempNeoThreadStore(t)
	threadID := "T-019f9ad5-01de-71e4-99ab-a144b7974b3b"
	if _, err := neoEnsureDiffCaptureRepository(context.Background(), threadID, "sha1"); err != nil {
		t.Fatal(err)
	}
	missingSHA := strings.Repeat("f", 40)
	record := neoDiffCaptureTestPublishedRecord(t, "0000000000000001", "2026-07-31T10:00:00Z", strings.Repeat("a", 40), []string{missingSHA})
	if err := writeNeoDiffCaptureRecord(neoDiffCaptureRecordPath(threadID, record.CaptureID), record); err != nil {
		t.Fatal(err)
	}
	index := newNeoDiffCaptureBlobIndex(nil).withCapture(record.CaptureID, record.ManifestSHA256, []string{missingSHA})
	neoDiffCaptureBlobIndexes.put(threadID, index)
	t.Cleanup(func() { neoDiffCaptureBlobIndexes.delete(threadID) })
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	response := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(response)
	c.Request = request
	(&AmpModule{}).serveNeoDiffCaptureBlob(c, threadID, missingSHA)
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("authorized missing blob status = %d, body=%s", response.Code, response.Body.String())
	}
	unknownSHA := strings.Repeat("e", 40)
	response = httptest.NewRecorder()
	c, _ = gin.CreateTestContext(response)
	c.Request = httptest.NewRequest(http.MethodGet, "/", nil)
	(&AmpModule{}).serveNeoDiffCaptureBlob(c, threadID, unknownSHA)
	if response.Code != http.StatusNotFound {
		t.Fatalf("unauthorized blob status = %d, body=%s", response.Code, response.Body.String())
	}
	if err := os.WriteFile(neoDiffCaptureRecordPath(threadID, record.CaptureID), []byte("not-json\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	response = httptest.NewRecorder()
	c, _ = gin.CreateTestContext(response)
	c.Request = httptest.NewRequest(http.MethodGet, "/", nil)
	(&AmpModule{}).serveNeoDiffCaptureBlob(c, threadID, missingSHA)
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("corrupt authorized capture status = %d, body=%s", response.Code, response.Body.String())
	}
}

func TestNeoDiffCaptureResponseWritingReleasesThreadLock(t *testing.T) {
	gin.SetMode(gin.TestMode)
	useTempNeoThreadStore(t)
	fixture := neoDiffCaptureTestRepository(t)
	tests := []struct {
		name       string
		threadID   string
		method     string
		path       string
		wantStatus int
		serve      func(*gin.Context, string, neoDiffCaptureRecord)
	}{
		{
			name:       "allocate",
			threadID:   "T-019f9ad5-01de-71e4-99ab-a144b7974b49",
			method:     http.MethodPost,
			path:       "/",
			wantStatus: http.StatusOK,
			serve: func(c *gin.Context, threadID string, _ neoDiffCaptureRecord) {
				(&AmpModule{}).serveNeoDiffCaptureAllocate(c, threadID)
			},
		},
		{
			name:       "publish",
			threadID:   "T-019f9ad5-01de-71e4-99ab-a144b7974b4a",
			method:     http.MethodPost,
			path:       "/",
			wantStatus: http.StatusConflict,
			serve: func(c *gin.Context, threadID string, record neoDiffCaptureRecord) {
				(&AmpModule{}).serveNeoDiffCapturePublish(c, threadID, record.CaptureID)
			},
		},
		{
			name:       "latest",
			threadID:   "T-019f9ad5-01de-71e4-99ab-a144b7974b4b",
			method:     http.MethodGet,
			path:       "/",
			wantStatus: http.StatusOK,
			serve: func(c *gin.Context, threadID string, _ neoDiffCaptureRecord) {
				(&AmpModule{}).serveNeoDiffCaptureLatest(c, threadID)
			},
		},
		{
			name:       "delete error",
			threadID:   "T-019f9ad5-01de-71e4-99ab-a144b7974b4c",
			method:     http.MethodDelete,
			path:       "/",
			wantStatus: http.StatusConflict,
			serve: func(c *gin.Context, threadID string, record neoDiffCaptureRecord) {
				(&AmpModule{}).serveNeoDiffCaptureDelete(c, threadID, record.CaptureID)
			},
		},
		{
			name:       "blob",
			threadID:   "T-019f9ad5-01de-71e4-99ab-a144b7974b4d",
			method:     http.MethodGet,
			path:       "/",
			wantStatus: http.StatusOK,
			serve: func(c *gin.Context, threadID string, _ neoDiffCaptureRecord) {
				(&AmpModule{}).serveNeoDiffCaptureBlob(c, threadID, fixture.oldBlobSHA)
			},
		},
		{
			name:       "delta",
			threadID:   "T-019f9ad5-01de-71e4-99ab-a144b7974b4e",
			method:     http.MethodGet,
			path:       "/?from=" + fixture.oldBlobSHA + "&to=" + fixture.newBlobSHA,
			wantStatus: http.StatusOK,
			serve: func(c *gin.Context, threadID string, _ neoDiffCaptureRecord) {
				(&AmpModule{}).serveNeoDiffCaptureDelta(c, threadID)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			captureID := "0000000000000001"
			repositoryDir, err := neoEnsureDiffCaptureRepository(context.Background(), test.threadID, "sha1")
			if err != nil {
				t.Fatal(err)
			}
			record := neoDiffCaptureTestPublishedRecord(t, captureID, "2026-07-31T10:00:00Z", fixture.headSHA, []string{fixture.oldBlobSHA, fixture.newBlobSHA})
			allocated := record
			allocated.PublishedRef = ""
			allocated.PublishedAt = ""
			allocated.State = "allocated"
			allocated.ManifestSHA256 = ""
			allocated.Manifest = nil
			if err := writeNeoDiffCaptureRecord(neoDiffCaptureRecordPath(test.threadID, captureID), allocated); err != nil {
				t.Fatal(err)
			}
			neoDiffCaptureTestGit(t, fixture.dir, "push", repositoryDir, fixture.headSHA+":"+record.CaptureRef)
			if err := writeNeoDiffCaptureRecord(neoDiffCaptureRecordPath(test.threadID, captureID), record); err != nil {
				t.Fatal(err)
			}
			index := newNeoDiffCaptureBlobIndex(nil).withCapture(captureID, record.ManifestSHA256, []string{fixture.oldBlobSHA, fixture.newBlobSHA})
			indexRaw, err := marshalNeoDiffCaptureBlobIndex(index)
			if err != nil {
				t.Fatal(err)
			}
			if err := writeNeoDiffCaptureBlobIndex(test.threadID, indexRaw); err != nil {
				t.Fatal(err)
			}
			neoDiffCaptureBlobIndexes.delete(test.threadID)
			t.Cleanup(func() { neoDiffCaptureBlobIndexes.delete(test.threadID) })
			writer := &neoDiffCaptureTestBlockingResponseWriter{
				ResponseRecorder: httptest.NewRecorder(),
				writeStarted:     make(chan struct{}),
				release:          make(chan struct{}),
			}
			c, _ := gin.CreateTestContext(writer)
			var body io.Reader
			if test.name == "publish" {
				body = bytes.NewReader(record.Manifest)
			}
			c.Request = httptest.NewRequest(test.method, test.path, body)
			done := make(chan struct{})
			go func() {
				test.serve(c, test.threadID, record)
				close(done)
			}()
			select {
			case <-writer.writeStarted:
			case <-time.After(2 * time.Second):
				close(writer.release)
				<-done
				t.Fatal("response writing did not start")
			}
			lockContext, cancel := context.WithTimeout(context.Background(), time.Second)
			secondUnlock, lockErr := neoDiffCaptureLock(lockContext, test.threadID)
			cancel()
			if secondUnlock != nil {
				secondUnlock()
			}
			receiveRelease, receiveErr := neoAcquireDiffCaptureReceiveLock(test.threadID)
			if receiveRelease != nil {
				receiveRelease()
			}
			close(writer.release)
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("response writing did not finish")
			}
			if lockErr != nil {
				t.Fatalf("same-thread lock remained held during response writing: %v; status=%d body=%s", lockErr, writer.Code, writer.Body.String())
			}
			if receiveErr != nil {
				t.Fatalf("receive lock remained held during response writing: %v; status=%d body=%s", receiveErr, writer.Code, writer.Body.String())
			}
			if writer.Code != test.wantStatus {
				t.Fatalf("response status = %d, want %d, body=%s", writer.Code, test.wantStatus, writer.Body.String())
			}
		})
	}
}

func TestNeoDiffCaptureBlobIndexCacheConcurrentAccounting(t *testing.T) {
	var cache neoDiffCaptureBlobIndexCache
	var wait sync.WaitGroup
	for index := 0; index < neoDiffCaptureBlobIndexMaxThreads*4; index++ {
		index := index
		wait.Add(1)
		go func() {
			defer wait.Done()
			captureID := fmt.Sprintf("%016d", index)
			sha := fmt.Sprintf("%040x", index+1)
			blobIndex := newNeoDiffCaptureBlobIndex(nil).withCapture(captureID, strings.Repeat("a", 64), []string{sha})
			cache.put(fmt.Sprintf("thread-%03d", index), blobIndex)
		}()
	}
	wait.Wait()
	cache.mu.Lock()
	defer cache.mu.Unlock()
	totalBytes := 0
	for _, entry := range cache.entries {
		totalBytes += entry.bytes
	}
	if cache.totalBytes != totalBytes || cache.totalBytes > neoDiffCaptureBlobIndexCacheBytes {
		t.Fatalf("cache bytes=%d summed=%d limit=%d", cache.totalBytes, totalBytes, neoDiffCaptureBlobIndexCacheBytes)
	}
	if len(cache.entries) > neoDiffCaptureBlobIndexMaxThreads {
		t.Fatalf("cache entries=%d limit=%d", len(cache.entries), neoDiffCaptureBlobIndexMaxThreads)
	}
}

func TestNeoDiffCaptureBlobIndexForPublishReloadsDiskState(t *testing.T) {
	useTempNeoThreadStore(t)
	threadID := "T-019f9ad5-01de-71e4-99ab-a144b7974b53"
	if err := os.MkdirAll(neoDiffCaptureThreadDir(threadID), 0o700); err != nil {
		t.Fatal(err)
	}
	staleSHA := strings.Repeat("a", 40)
	diskSHA := strings.Repeat("b", 40)
	stale := newNeoDiffCaptureBlobIndex(nil).withCapture("0000000000000001", strings.Repeat("c", 64), []string{staleSHA})
	disk := newNeoDiffCaptureBlobIndex(nil).withCapture("0000000000000002", strings.Repeat("d", 64), []string{diskSHA})
	raw, err := marshalNeoDiffCaptureBlobIndex(disk)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeNeoDiffCaptureBlobIndex(threadID, raw); err != nil {
		t.Fatal(err)
	}
	neoDiffCaptureBlobIndexes.put(threadID, stale)
	t.Cleanup(func() { neoDiffCaptureBlobIndexes.delete(threadID) })
	loaded, err := neoDiffCaptureBlobIndexForThread(threadID)
	if err != nil {
		t.Fatal(err)
	}
	if !loaded.allows(diskSHA) || loaded.allows(staleSHA) {
		t.Fatalf("thread blob index did not reload disk state: %#v", loaded)
	}
	neoDiffCaptureBlobIndexes.put(threadID, stale)
	loaded, err = neoDiffCaptureBlobIndexForPublish(threadID)
	if err != nil {
		t.Fatal(err)
	}
	if !loaded.allows(diskSHA) || loaded.allows(staleSHA) {
		t.Fatalf("publish blob index did not reload disk state: %#v", loaded)
	}
}

type neoDiffCaptureTestFixture struct {
	dir           string
	baseSHA       string
	headSHA       string
	oldBlobSHA    string
	newBlobSHA    string
	binaryBlobSHA string
	largeBlobSHA  string
}

type neoDiffCaptureTestRevisionFixture struct {
	dir                string
	manifest           neoDiffCaptureManifest
	unrelatedSHA       string
	mismatchedTreeSHA  string
	brokenProjectedSHA string
	brokenIndexSHA     string
	brokenWorktreeSHA  string
}

func neoDiffCaptureTestRevisionRepository(t *testing.T) neoDiffCaptureTestRevisionFixture {
	t.Helper()
	dir := t.TempDir()
	neoDiffCaptureTestGit(t, dir, "init", "--quiet")
	neoDiffCaptureTestGit(t, dir, "config", "user.email", "diff-capture@example.invalid")
	neoDiffCaptureTestGit(t, dir, "config", "user.name", "Diff Capture Test")
	path := filepath.Join(dir, "file.txt")
	if err := os.WriteFile(path, []byte("base\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	neoDiffCaptureTestGit(t, dir, "add", "--all")
	neoDiffCaptureTestGit(t, dir, "commit", "--quiet", "-m", "base")
	baseSHA := strings.TrimSpace(neoDiffCaptureTestGit(t, dir, "rev-parse", "HEAD"))
	if err := os.WriteFile(path, []byte("source one\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	neoDiffCaptureTestGit(t, dir, "add", "--all")
	neoDiffCaptureTestGit(t, dir, "commit", "--quiet", "-m", "source one")
	sourceOneSHA := strings.TrimSpace(neoDiffCaptureTestGit(t, dir, "rev-parse", "HEAD"))
	if err := os.WriteFile(path, []byte("source two\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	neoDiffCaptureTestGit(t, dir, "add", "--all")
	neoDiffCaptureTestGit(t, dir, "commit", "--quiet", "-m", "source two")
	sourceTwoSHA := strings.TrimSpace(neoDiffCaptureTestGit(t, dir, "rev-parse", "HEAD"))
	tree := func(revision string) string {
		return strings.TrimSpace(neoDiffCaptureTestGit(t, dir, "rev-parse", revision+"^{tree}"))
	}
	commit := func(treeSHA, parentSHA, subject string) string {
		args := []string{"commit-tree", treeSHA}
		if parentSHA != "" {
			args = append(args, "-p", parentSHA)
		}
		args = append(args, "-m", subject)
		return strings.TrimSpace(neoDiffCaptureTestGit(t, dir, args...))
	}
	projectedOneSHA := commit(tree(sourceOneSHA), baseSHA, "projected one")
	projectedTwoSHA := commit(tree(sourceTwoSHA), projectedOneSHA, "projected two")
	indexSHA := commit(tree(sourceTwoSHA), projectedTwoSHA, "projected index")
	worktreeSHA := commit(tree(sourceTwoSHA), indexSHA, "projected worktree")
	manifest := neoDiffCaptureManifest{}
	manifest.Source.MergeBaseSHA = baseSHA
	manifest.Source.HeadSHA = sourceTwoSHA
	manifest.Revisions.ProjectedHeadSHA = projectedTwoSHA
	manifest.Revisions.ProjectedIndexSHA = indexSHA
	manifest.Revisions.ProjectedWorktreeSHA = worktreeSHA
	manifest.ProjectedCommits = []neoDiffCaptureProjectedCommit{
		{SourceCommitSHA: sourceOneSHA, ProjectedCommitSHA: projectedOneSHA},
		{SourceCommitSHA: sourceTwoSHA, ProjectedCommitSHA: projectedTwoSHA},
	}
	return neoDiffCaptureTestRevisionFixture{
		dir:                dir,
		manifest:           manifest,
		unrelatedSHA:       commit(tree(sourceOneSHA), "", "unrelated"),
		mismatchedTreeSHA:  commit(tree(sourceTwoSHA), baseSHA, "mismatched projected tree"),
		brokenProjectedSHA: commit(tree(sourceTwoSHA), baseSHA, "broken projected"),
		brokenIndexSHA:     commit(tree(sourceTwoSHA), baseSHA, "broken index"),
		brokenWorktreeSHA:  commit(tree(sourceTwoSHA), baseSHA, "broken worktree"),
	}
}

func neoDiffCaptureTestRepository(t *testing.T) neoDiffCaptureTestFixture {
	t.Helper()
	dir := t.TempDir()
	neoDiffCaptureTestGit(t, dir, "init", "--quiet")
	neoDiffCaptureTestGit(t, dir, "config", "user.email", "diff-capture@example.invalid")
	neoDiffCaptureTestGit(t, dir, "config", "user.name", "Diff Capture Test")
	if err := os.WriteFile(filepath.Join(dir, "file.txt"), []byte("old\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	neoDiffCaptureTestGit(t, dir, "add", "file.txt")
	neoDiffCaptureTestGit(t, dir, "commit", "--quiet", "-m", "base")
	baseSHA := strings.TrimSpace(neoDiffCaptureTestGit(t, dir, "rev-parse", "HEAD"))
	oldBlobSHA := strings.TrimSpace(neoDiffCaptureTestGit(t, dir, "rev-parse", "HEAD:file.txt"))
	if err := os.WriteFile(filepath.Join(dir, "file.txt"), []byte("new\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "binary.bin"), []byte{0, 1, 2, 3}, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "large.txt"), bytes.Repeat([]byte("x"), neoDiffCaptureTextBlobMaxBytes+1), 0o600); err != nil {
		t.Fatal(err)
	}
	neoDiffCaptureTestGit(t, dir, "add", "file.txt", "binary.bin", "large.txt")
	neoDiffCaptureTestGit(t, dir, "commit", "--quiet", "-m", "projected")
	return neoDiffCaptureTestFixture{
		dir:           dir,
		baseSHA:       baseSHA,
		headSHA:       strings.TrimSpace(neoDiffCaptureTestGit(t, dir, "rev-parse", "HEAD")),
		oldBlobSHA:    oldBlobSHA,
		newBlobSHA:    strings.TrimSpace(neoDiffCaptureTestGit(t, dir, "rev-parse", "HEAD:file.txt")),
		binaryBlobSHA: strings.TrimSpace(neoDiffCaptureTestGit(t, dir, "rev-parse", "HEAD:binary.bin")),
		largeBlobSHA:  strings.TrimSpace(neoDiffCaptureTestGit(t, dir, "rev-parse", "HEAD:large.txt")),
	}
}

func neoDiffCaptureTestManifest(captureID, captureRef string, fixture neoDiffCaptureTestFixture) map[string]any {
	files := []any{
		map[string]any{
			"id": "file-edit", "path": "file.txt", "changeType": "modified", "isBinary": false,
			"old":      map[string]any{"kind": "file", "path": "file.txt", "blobSHA": fixture.oldBlobSHA, "sizeBytes": 4},
			"new":      map[string]any{"kind": "file", "path": "file.txt", "blobSHA": fixture.newBlobSHA, "sizeBytes": 4},
			"diffStat": map[string]any{"added": 1, "deleted": 1, "changed": 1},
		},
		map[string]any{
			"id": "binary-create", "path": "binary.bin", "changeType": "added", "isBinary": true,
			"old":      map[string]any{"kind": "absent", "path": "binary.bin"},
			"new":      map[string]any{"kind": "file", "path": "binary.bin", "blobSHA": fixture.binaryBlobSHA, "sizeBytes": 4},
			"diffStat": map[string]any{"added": 0, "deleted": 0, "changed": 1},
		},
		map[string]any{
			"id": "large-create", "path": "large.txt", "changeType": "added", "isBinary": false,
			"old":      map[string]any{"kind": "absent", "path": "large.txt"},
			"new":      map[string]any{"kind": "file", "path": "large.txt", "blobSHA": fixture.largeBlobSHA, "sizeBytes": neoDiffCaptureTextBlobMaxBytes + 1},
			"diffStat": map[string]any{"added": 1, "deleted": 0, "changed": 1},
		},
	}
	return map[string]any{
		"version":    1,
		"captureID":  captureID,
		"capturedAt": int64(1785100693111),
		"captureRef": captureRef,
		"source": map[string]any{
			"repositoryRoot": "/tmp/diff-capture-fixture",
			"branch":         "main",
			"headSHA":        fixture.headSHA,
			"mergeBaseSHA":   fixture.baseSHA,
		},
		"revisions": map[string]any{
			"projectedHeadSHA":     fixture.headSHA,
			"projectedIndexSHA":    fixture.headSHA,
			"projectedWorktreeSHA": fixture.headSHA,
		},
		"projectedCommits": []any{
			map[string]any{"sourceCommitSHA": fixture.headSHA, "projectedCommitSHA": fixture.headSHA, "subject": "projected"},
		},
		"sections": []any{
			map[string]any{"id": "working-tree", "label": "Working tree", "baseCommitSHA": fixture.baseSHA, "headCommitSHA": fixture.headSHA, "files": files},
		},
		"rangeSections": []any{
			map[string]any{"rangeID": "merge-base:" + fixture.baseSHA, "label": "All changes", "files": files},
			map[string]any{"rangeID": "commit:" + fixture.headSHA, "label": "Projected commit", "files": files},
		},
	}
}

func neoDiffCaptureTestManifestWithOldBlob(t *testing.T, manifest map[string]any, sha string) map[string]any {
	t.Helper()
	cloned, ok := cloneNeoJSONValue(manifest).(map[string]any)
	if !ok {
		t.Fatal("cloned manifest is not an object")
	}
	sections := arrayValue(cloned["sections"])
	files := arrayValue(mapValue(sections[0])["files"])
	mapValue(files[0])["oldBlobSHA"] = sha
	return cloned
}

func neoDiffCaptureTestPublishedRecord(t *testing.T, captureID, publishedAt, projectedWorktreeSHA string, shas []string) neoDiffCaptureRecord {
	t.Helper()
	manifest := neoDiffCaptureManifest{
		Version:    1,
		CaptureID:  captureID,
		CapturedAt: json.RawMessage(`1785100693111`),
		CaptureRef: "refs/heads/amp/captures/" + captureID,
	}
	manifest.Revisions.ProjectedWorktreeSHA = projectedWorktreeSHA
	for index, sha := range shas {
		manifest.Sections = append(manifest.Sections, neoDiffCaptureManifestSection{Files: []neoDiffCaptureManifestFile{{
			ID:         fmt.Sprintf("blob-%d", index),
			Path:       fmt.Sprintf("blob-%d.txt", index),
			ChangeType: "modified",
			Old:        neoDiffCaptureFileSide{Kind: "file", Path: fmt.Sprintf("blob-%d.txt", index), BlobSHA: sha},
		}}})
	}
	raw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	return neoDiffCaptureRecord{
		StorageVersion: 1,
		CaptureID:      captureID,
		CaptureRef:     manifest.CaptureRef,
		PublishedRef:   neoDiffCapturePublishedRef(captureID),
		CreatedAt:      publishedAt,
		PublishedAt:    publishedAt,
		State:          "published",
		ManifestSHA256: neoDiffCaptureManifestSHA256(raw),
		Manifest:       raw,
	}
}

func neoDiffCaptureTestSizedRecord(t *testing.T, captureID string, size int) []byte {
	t.Helper()
	record := neoDiffCaptureRecord{
		StorageVersion: 1,
		CaptureID:      captureID,
		CaptureRef:     "refs/heads/amp/captures/" + captureID,
		CreatedAt:      "2026-07-31T10:00:00Z",
		State:          "allocated",
		Manifest:       json.RawMessage(`""`),
	}
	base, err := marshalNeoDiffCaptureRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	if size < len(base) {
		t.Fatalf("sized record target %d is smaller than metadata %d", size, len(base))
	}
	record.Manifest = json.RawMessage(`"` + strings.Repeat("x", size-len(base)) + `"`)
	raw, err := marshalNeoDiffCaptureRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != size {
		t.Fatalf("sized record bytes = %d, want %d", len(raw), size)
	}
	return raw
}

func neoDiffCaptureTestFileValidationRepository(t *testing.T) (string, string, string, []neoDiffCaptureManifestFile) {
	t.Helper()
	dir := t.TempDir()
	neoDiffCaptureTestGit(t, dir, "init", "--quiet")
	neoDiffCaptureTestGit(t, dir, "config", "user.email", "diff-capture@example.invalid")
	neoDiffCaptureTestGit(t, dir, "config", "user.name", "Diff Capture Test")
	baseFiles := map[string]string{
		"modified.txt":      "old modified\n",
		"deleted.txt":       "deleted\n",
		"rename-source.txt": "rename\n",
		"copy-source.txt":   "copy\n",
	}
	for name, content := range baseFiles {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	neoDiffCaptureTestGit(t, dir, "add", "--all")
	neoDiffCaptureTestGit(t, dir, "commit", "--quiet", "-m", "base")
	baseSHA := strings.TrimSpace(neoDiffCaptureTestGit(t, dir, "rev-parse", "HEAD"))
	if err := os.WriteFile(filepath.Join(dir, "modified.txt"), []byte("new modified\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, "deleted.txt")); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(dir, "rename-source.txt"), filepath.Join(dir, "rename target.txt")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "copy-target.txt"), []byte(baseFiles["copy-source.txt"]), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, " whitespace name.txt"), []byte("whitespace\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ":(glob)magic[1].txt"), []byte("magic\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "binary.bin"), []byte{0, 1, 2, 3}, 0o600); err != nil {
		t.Fatal(err)
	}
	neoDiffCaptureTestGit(t, dir, "add", "--all")
	neoDiffCaptureTestGit(t, dir, "commit", "--quiet", "-m", "head")
	headSHA := strings.TrimSpace(neoDiffCaptureTestGit(t, dir, "rev-parse", "HEAD"))
	blob := func(revision, path string) string {
		return strings.TrimSpace(neoDiffCaptureTestGit(t, dir, "rev-parse", revision+":"+path))
	}
	fileSide := func(path, sha string) neoDiffCaptureFileSide {
		if sha == "" {
			return neoDiffCaptureFileSide{Kind: "absent", Path: path}
		}
		return neoDiffCaptureFileSide{Kind: "file", Path: path, BlobSHA: sha}
	}
	file := func(id, path, previousPath, changeType, oldSHA, newSHA string) neoDiffCaptureManifestFile {
		oldPath := path
		if previousPath != "" {
			oldPath = previousPath
		}
		result := neoDiffCaptureManifestFile{
			ID:           id,
			Path:         path,
			PreviousPath: previousPath,
			ChangeType:   changeType,
			Old:          fileSide(oldPath, oldSHA),
			New:          fileSide(path, newSHA),
		}
		switch changeType {
		case "modified":
			result.DiffStat = neoDiffCaptureDiffStat{Added: 1, Deleted: 1, Changed: 1}
		case "added", "untracked":
			result.DiffStat = neoDiffCaptureDiffStat{Added: 1, Changed: 1}
		case "deleted":
			result.DiffStat = neoDiffCaptureDiffStat{Deleted: 1, Changed: 1}
		}
		return result
	}
	files := []neoDiffCaptureManifestFile{
		file("modified", "modified.txt", "", "modified", blob(baseSHA, "modified.txt"), blob(headSHA, "modified.txt")),
		file("untracked", " whitespace name.txt", "", "untracked", "", blob(headSHA, " whitespace name.txt")),
		file("deleted", "deleted.txt", "", "deleted", blob(baseSHA, "deleted.txt"), ""),
		file("renamed", "rename target.txt", "rename-source.txt", "renamed", blob(baseSHA, "rename-source.txt"), blob(headSHA, "rename target.txt")),
		file("copied", "copy-target.txt", "copy-source.txt", "copied", blob(baseSHA, "copy-source.txt"), blob(headSHA, "copy-target.txt")),
		file("added", ":(glob)magic[1].txt", "", "added", "", blob(headSHA, ":(glob)magic[1].txt")),
		file("binary", "binary.bin", "", "added", "", blob(headSHA, "binary.bin")),
	}
	files[len(files)-1].IsBinary = true
	files[len(files)-1].DiffStat = neoDiffCaptureDiffStat{Changed: 1}
	return dir, baseSHA, headSHA, files
}

func neoDiffCaptureTestCloneManifest(t *testing.T, manifest neoDiffCaptureManifest) neoDiffCaptureManifest {
	t.Helper()
	raw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	var cloned neoDiffCaptureManifest
	if err := json.Unmarshal(raw, &cloned); err != nil {
		t.Fatal(err)
	}
	return cloned
}

func neoDiffCaptureTestActor(rt *neoRuntime, threadID, ownerUserID string) {
	actor := rt.store.ensureThreadActor(threadID)
	actor.mu.Lock()
	if actor.meta == nil {
		actor.meta = map[string]any{}
	}
	actor.meta["cliProxyAPILocalNeo"] = true
	actor.meta["ownerUserId"] = ownerUserID
	actor.title = "Diff capture test"
	actor.mu.Unlock()
}

func neoDiffCaptureTestAllocate(t *testing.T, router http.Handler, threadID string) map[string]any {
	t.Helper()
	response := neoDiffCaptureTestRequest(t, router, http.MethodPost, "/api/threads/"+threadID+"/diff-captures", []byte(`{}`))
	if response.Code != http.StatusOK {
		t.Fatalf("allocation status = %d, body=%s", response.Code, response.Body.String())
	}
	return neoDiffCaptureTestJSON(t, response)
}

func neoDiffCaptureTestRequest(t *testing.T, handler http.Handler, method, path string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, path, bytes.NewReader(body))
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response := &neoDiffCaptureTestResponseRecorder{ResponseRecorder: httptest.NewRecorder(), closed: make(chan bool)}
	handler.ServeHTTP(response, request)
	return response.ResponseRecorder
}

type neoDiffCaptureTestResponseRecorder struct {
	*httptest.ResponseRecorder
	closed chan bool
}

type neoDiffCaptureTestBlockingResponseWriter struct {
	*httptest.ResponseRecorder
	writeStarted chan struct{}
	release      chan struct{}
	once         sync.Once
}

type neoDiffCaptureTestLockResult struct {
	unlock func()
	err    error
}

func (r *neoDiffCaptureTestResponseRecorder) CloseNotify() <-chan bool {
	return r.closed
}

func (r *neoDiffCaptureTestBlockingResponseWriter) Write(data []byte) (int, error) {
	r.once.Do(func() { close(r.writeStarted) })
	<-r.release
	return r.ResponseRecorder.Write(data)
}

func neoDiffCaptureTestJSON(t *testing.T, response *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var body map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response JSON: %v; body=%s", err, response.Body.String())
	}
	return body
}

func neoDiffCaptureTestGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	command := exec.Command("git", args...)
	command.Dir = dir
	command.Env = neoGitCommandEnv()
	var output bytes.Buffer
	var errorOutput bytes.Buffer
	command.Stdout = &output
	command.Stderr = &errorOutput
	if err := command.Run(); err != nil {
		t.Fatalf("git %v in %s: %v\n%s", args, dir, err, errorOutput.Bytes())
	}
	return output.String()
}

func neoDiffCaptureTestMode(t *testing.T, path string) os.FileMode {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Mode().Perm()
}

func neoDiffCaptureTestWaitForLockRefs(t *testing.T, threadID string, want int) *neoDiffCaptureLockEntry {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		neoDiffCaptureLocks.mu.Lock()
		entry := neoDiffCaptureLocks.entries[threadID]
		refs := 0
		if entry != nil {
			refs = entry.refs
		}
		neoDiffCaptureLocks.mu.Unlock()
		if refs == want {
			return entry
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("thread lock refs did not reach %d", want)
	return nil
}
