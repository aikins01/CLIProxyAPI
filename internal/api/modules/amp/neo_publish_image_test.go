package amp

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
)

func TestNeoPublishImageToolAdvertisementAndDispatch(t *testing.T) {
	spec, ok := neoSyntheticLocalToolSpec("publish_image")
	if !ok || spec.Name != "publish_image" || stringValue(spec.Meta["source"]) != "server" {
		t.Fatalf("publish_image spec = %#v, %v", spec, ok)
	}
	properties := mapValue(spec.InputSchema["properties"])
	if len(properties) != 2 || properties["path"] == nil || properties["description"] == nil || spec.InputSchema["additionalProperties"] != false {
		t.Fatalf("publish_image schema = %#v", spec.InputSchema)
	}
	for _, mode := range []string{"smart", "large", "rush", "deep", "nostromo", "low", "medium", "high", "ultra"} {
		if !neoModeToolAllowlist[mode]["publish_image"] {
			t.Fatalf("mode %s does not advertise publish_image", mode)
		}
	}
	for _, mode := range []string{"puck", "review"} {
		if neoModeToolAllowlist[mode]["publish_image"] {
			t.Fatalf("mode %s unexpectedly advertises publish_image", mode)
		}
	}
	actor := &neoActor{executorBootstrapComplete: true, tools: map[string]neoToolSpec{}}
	if !neoKnownModeTools["publish_image"] || !isNeoThreadTool("publish_image") || !isNeoLocalActorTool("publish_image") || !actor.shouldRunLocalActorTool("publish_image") || !isNeoAmpWorkspaceTool("publish_image") || neoThreadToolProgressText("publish_image", nil) != "Publishing image" {
		t.Fatal("publish_image dispatch metadata is incomplete")
	}
}

func TestReadNeoWorkspaceImageSecurity(t *testing.T) {
	workspace := t.TempDir()
	valid := []byte("valid image bytes")
	if err := os.Mkdir(filepath.Join(workspace, "images"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "images", "valid.png"), valid, 0o600); err != nil {
		t.Fatal(err)
	}
	if got, err := readNeoWorkspaceImage(workspace, "images/valid.png"); err != nil || string(got) != string(valid) {
		t.Fatalf("valid image = %q, %v", got, err)
	}
	if err := os.Symlink("valid.png", filepath.Join(workspace, "images", "file-link.png")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("images", filepath.Join(workspace, "parent-link")); err != nil {
		t.Fatal(err)
	}
	oversize, err := os.Create(filepath.Join(workspace, "oversize.png"))
	if err != nil {
		t.Fatal(err)
	}
	if err := oversize.Truncate(neoAttachmentMaxImageBytes + 1); err != nil {
		t.Fatal(err)
	}
	if err := oversize.Close(); err != nil {
		t.Fatal(err)
	}
	for _, relative := range []string{"../outside.png", "/tmp/outside.png", "images/file-link.png", "parent-link/valid.png", "images", "oversize.png"} {
		if _, pathErr := validateNeoPublishImagePath(relative); pathErr != nil {
			if relative == "../outside.png" || relative == "/tmp/outside.png" {
				continue
			}
			t.Fatalf("path %q rejected before secure read: %v", relative, pathErr)
		}
		if _, readErr := readNeoWorkspaceImage(workspace, relative); readErr == nil {
			t.Fatalf("readNeoWorkspaceImage(%q) unexpectedly succeeded", relative)
		}
	}
}

func TestExecuteLocalPublishImageStoresURLBackedResult(t *testing.T) {
	workspace := t.TempDir()
	dataDir := t.TempDir()
	oldDataDir := neoAmpDataDir
	neoAmpDataDir = func() string { return dataDir }
	t.Cleanup(func() { neoAmpDataDir = oldDataDir })
	imageData, err := base64.StdEncoding.DecodeString(testNeoPNGBase64(t, 1, 1))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "image.png"), imageData, 0o600); err != nil {
		t.Fatal(err)
	}
	rt := &neoRuntime{}
	actor := &neoActor{runtime: rt, threadID: "T-00000000-0000-0000-0000-000000000001", environment: map[string]any{"workingDirectory": workspace, "ampURL": "https://neo.example.com"}, meta: map[string]any{}}
	result, err := actor.executeLocalThreadToolContext(context.Background(), neoPendingTool{Name: "publish_image", Input: map[string]any{"path": "image.png", "description": "preview"}})
	if err != nil {
		t.Fatalf("executeLocalPublishImageTool: %v", err)
	}
	image := mapValue(arrayValue(result["images"])[0])
	if stringValue(image["mimeType"]) != "image/png" || !strings.HasPrefix(stringValue(image["url"]), "https://neo.example.com/attachments/") {
		t.Fatalf("publish image result = %#v", result)
	}
	run := normalizeNeoImageToolRun(neoPendingTool{Name: "publish_image", Input: map[string]any{"description": "preview"}}, map[string]any{"status": "done", "result": result})
	binary := arrayValue(run["result"])
	if len(binary) != 1 || stringValue(mapValue(binary[0])["type"]) != "image" || stringValue(mapValue(binary[0])["url"]) == "" {
		t.Fatalf("normalized publish image run = %#v", run)
	}
	if displayMessage := stringValue(run["displayMessage"]); displayMessage != "published 1 image" {
		t.Fatalf("publish image display message = %q", displayMessage)
	}
	serialized, err := json.Marshal(run)
	if err != nil {
		t.Fatal(err)
	}
	text := string(serialized)
	if strings.Contains(text, base64.StdEncoding.EncodeToString(imageData)) || strings.Contains(text, "file://") || strings.Contains(text, workspace) {
		t.Fatalf("serialized result leaks image bytes or private path: %s", text)
	}
	imageURL := stringValue(image["url"])
	attachmentID := strings.TrimPrefix(imageURL, "https://neo.example.com/attachments/")
	if attachmentID == imageURL || attachmentID == "" {
		t.Fatalf("attachment URL = %q", imageURL)
	}
	actor.storeMessage(neoMessage{
		ThreadID:  actor.threadID,
		MessageID: "M-0000000000000000000001",
		Role:      "user",
		Content: []any{map[string]any{
			"type":      "tool_result",
			"toolUseID": "TU-000000000000000000001",
			"run":       run,
		}},
	})
	snapshot, ok := actor.threadSnapshot()
	if !ok {
		t.Fatal("thread snapshot unavailable")
	}
	threadDir := t.TempDir()
	if err := writeNeoLocalThreadSnapshotToDir(snapshot, threadDir); err != nil {
		t.Fatalf("write thread snapshot: %v", err)
	}
	persisted, ok := loadNeoThreadFromDir(actor.threadID, threadDir)
	if !ok {
		t.Fatal("load thread snapshot")
	}
	persistedMessages := arrayValue(persisted["messages"])
	if len(persistedMessages) != 1 {
		t.Fatalf("persisted messages = %#v", persistedMessages)
	}
	persistedContent := arrayValue(mapValue(persistedMessages[0])["content"])
	if len(persistedContent) != 1 {
		t.Fatalf("persisted tool result content = %#v", persistedContent)
	}
	persistedJSON, err := json.Marshal(mapValue(persistedContent[0])["run"])
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{base64.StdEncoding.EncodeToString(imageData), "file://", workspace, "broker-secret", "session-secret", "container-secret"} {
		if strings.Contains(string(persistedJSON), forbidden) {
			t.Fatalf("persisted publish_image result contains forbidden value %q: %s", forbidden, persistedJSON)
		}
	}
	if !strings.Contains(string(persistedJSON), imageURL) || !strings.Contains(string(persistedJSON), "image/png") {
		t.Fatalf("persisted publish_image result lost URL or MIME: %s", persistedJSON)
	}
	restored := &neoActor{runtime: rt, threadID: actor.threadID, meta: map[string]any{}}
	if err := restored.importThreadLocalOnly(persisted); err != nil {
		t.Fatalf("import persisted publish_image result: %v", err)
	}
	replayed, ok := restored.threadSnapshot()
	if !ok {
		t.Fatal("restored thread snapshot unavailable")
	}
	replayedJSON, err := json.Marshal(neoWebLocalThread(replayed))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(replayedJSON), imageURL) || !strings.Contains(string(replayedJSON), "image/png") {
		t.Fatalf("replayed publish_image result lost URL or MIME: %s", replayedJSON)
	}
	storedData, storedMediaType, err := readNeoLocalAttachment(attachmentID)
	if err != nil || !bytes.Equal(storedData, imageData) || storedMediaType != "image/png" {
		t.Fatalf("stored attachment = %d bytes, %q, %v", len(storedData), storedMediaType, err)
	}
	if storedID, ok := neoLocalAttachmentIDFromURL(imageURL, "https://neo.example.com"); !ok || storedID != attachmentID {
		t.Fatal("stored attachment retrieval contract changed")
	}
}

func TestExecuteOrbPublishImageUsesExactContainerAndPublicOrigin(t *testing.T) {
	dataDir := t.TempDir()
	oldDataDir := neoAmpDataDir
	neoAmpDataDir = func() string { return dataDir }
	t.Cleanup(func() { neoAmpDataDir = oldDataDir })
	imageData, err := base64.StdEncoding.DecodeString(testNeoPNGBase64(t, 1, 1))
	if err != nil {
		t.Fatal(err)
	}
	fake := &neoOrbFakeProvider{readFileHandler: func(ctx context.Context, id, workDir, relative string, maxBytes int) ([]byte, error) {
		if id != "container-exact" || workDir != neoOrbWorkDir || relative != ".amp/previews/orb.png" || maxBytes != neoAttachmentMaxImageBytes {
			t.Fatalf("ReadWorkspaceFile args = %q, %q, %q, %d", id, workDir, relative, maxBytes)
		}
		return imageData, nil
	}}
	cfg := &config.Config{Host: "0.0.0.0", AmpCode: config.AmpCode{Orbs: config.AmpOrbs{PublicURL: "https://orb.example.com"}}}
	rt := &neoRuntime{cfg: cfg}
	manager := newNeoOrbManager(rt)
	manager.newClient = func(string) (neoOrbProviderClient, error) { return fake, nil }
	manager.client = fake
	rt.orbManager = manager
	threadID := "T-00000000-0000-0000-0000-000000000003"
	manager.orbs[threadID] = &neoOrbRecord{threadID: threadID, containerID: "container-exact", workDir: neoOrbWorkDir, state: neoOrbStateRunning}
	actor := &neoActor{runtime: rt, threadID: threadID, bootstrapExecutorType: "sandbox", environment: map[string]any{}, meta: map[string]any{"executorType": "sandbox"}}
	result, err := actor.executeLocalPublishImageTool(context.Background(), map[string]any{"path": ".amp/previews/orb.png"})
	if err != nil {
		t.Fatalf("execute orb publish_image: %v", err)
	}
	image := mapValue(arrayValue(result["images"])[0])
	if stringValue(image["mimeType"]) != "image/png" || !strings.HasPrefix(stringValue(image["url"]), "https://orb.example.com/attachments/") {
		t.Fatalf("orb publish_image result = %#v", result)
	}
	cfg.AmpCode.Orbs.PublicURL = ""
	if _, err := actor.executeLocalPublishImageTool(context.Background(), map[string]any{"path": ".amp/previews/orb.png"}); err == nil || !strings.Contains(err.Error(), "attachment URL is unavailable") {
		t.Fatalf("unroutable orb origin error = %v", err)
	}
}

func TestNeoPublishImageHeartbeatBindingAndCancellation(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	ownerID := "publish-image-owner"
	userActor, _, allowed := rt.store.upsertForOwner(map[string]any{"name": "userActor", "key": ownerID}, true, ownerID)
	if !allowed || userActor == nil {
		t.Fatal("create owner actor")
	}
	threadID := "T-00000000-0000-0000-0000-000000000001"
	runnerID := "publish-image-runner"
	request := neoPublishImageRequest{
		RequestID:  "publish-image-request",
		ThreadID:   threadID,
		RunnerID:   runnerID,
		BrokerID:   "publish-image-broker",
		SessionID:  "publish-image-session",
		Generation: 3,
		Path:       "images/result.png",
	}
	pending := &neoPendingPublishImage{request: request, result: make(chan neoPublishImageDelivery, 1)}
	userActor.mu.Lock()
	userActor.pendingPublishImages[request.RequestID] = pending
	userActor.mu.Unlock()
	runningThreads := []string{threadID}
	runners := []neoLocalBrokerHeartbeatRunner{{RunnerID: runnerID, RunningThreads: &runningThreads}}
	heartbeat := neoLocalBrokerHeartbeatRequest{BrokerID: request.BrokerID, SessionID: request.SessionID, SessionGeneration: request.Generation, Runners: &runners}
	advertised := userActor.publishImageRequestsForHeartbeat(heartbeat)
	if len(advertised) != 1 || advertised[0] != request {
		t.Fatalf("advertised requests = %#v", advertised)
	}
	stoppedThreads := []string{}
	heartbeat.Runners = &[]neoLocalBrokerHeartbeatRunner{{RunnerID: runnerID, RunningThreads: &stoppedThreads}}
	if got := userActor.publishImageRequestsForHeartbeat(heartbeat); len(got) != 0 {
		t.Fatalf("stopped assignment advertised requests = %#v", got)
	}
	if delivery := <-pending.result; delivery.code != "stale_assignment" {
		t.Fatalf("stopped assignment delivery = %#v", delivery)
	}
	sessionRequest := request
	sessionRequest.RequestID = "publish-image-stale-session"
	sessionPending := &neoPendingPublishImage{request: sessionRequest, result: make(chan neoPublishImageDelivery, 1)}
	userActor.mu.Lock()
	userActor.pendingPublishImages[sessionRequest.RequestID] = sessionPending
	userActor.mu.Unlock()
	staleSessionHeartbeat := heartbeat
	staleSessionHeartbeat.SessionID = "publish-image-new-session"
	staleSessionHeartbeat.SessionGeneration++
	staleSessionHeartbeat.Runners = &runners
	if got := userActor.publishImageRequestsForHeartbeat(staleSessionHeartbeat); len(got) != 0 {
		t.Fatalf("stale-session request was advertised: %#v", got)
	}
	if delivery := <-sessionPending.result; delivery.code != "stale_session" {
		t.Fatalf("stale-session delivery = %#v", delivery)
	}

	userActor.mu.Lock()
	userActor.userRunners[runnerID] = neoUserExecutorRunner{
		runnerID: runnerID, brokerID: request.BrokerID, sessionID: request.SessionID, sessionGeneration: request.Generation,
		runningThreads: []string{threadID}, updatedAt: time.Now(),
	}
	userActor.mu.Unlock()
	threadActor := &neoActor{runtime: rt, threadID: threadID, meta: map[string]any{"ownerUserId": ownerID}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := threadActor.requestBrokerPublishImage(ctx, runnerID, threadID, "image.png")
		done <- err
	}()
	deadline := time.Now().Add(time.Second)
	for {
		userActor.mu.Lock()
		pendingCount := len(userActor.pendingPublishImages)
		userActor.mu.Unlock()
		if pendingCount == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("broker request was not queued")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled broker request error = %v", err)
	}
	userActor.mu.Lock()
	remaining := len(userActor.pendingPublishImages)
	userActor.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("cancelled broker request retained %d pending entries", remaining)
	}

	heartbeat.Runners = &runners
	userActor.mu.Lock()
	for index := 0; index < neoPublishImageRequestLimit+1; index++ {
		request := request
		request.RequestID = "publish-image-bounded-" + string(rune('a'+index))
		userActor.pendingPublishImages[request.RequestID] = &neoPendingPublishImage{request: request, result: make(chan neoPublishImageDelivery, 1)}
	}
	userActor.mu.Unlock()
	if got := userActor.publishImageRequestsForHeartbeat(heartbeat); len(got) != neoPublishImageRequestLimit {
		t.Fatalf("advertised request count = %d, want %d", len(got), neoPublishImageRequestLimit)
	}
	userActor.dispose()
	userActor.mu.Lock()
	remaining = len(userActor.pendingPublishImages)
	userActor.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("disposed actor retained %d pending requests", remaining)
	}
}

func TestServeLocalBrokerPublishImageResultBindingAndDuplicates(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rt := newNeoRuntime(&config.Config{})
	ownerID := "publish-image-route-owner"
	userActor, _, allowed := rt.store.upsertForOwner(map[string]any{"name": "userActor", "key": ownerID}, true, ownerID)
	if !allowed || userActor == nil {
		t.Fatal("create owner actor")
	}
	m := &AmpModule{neoRuntime: rt}
	router := gin.New()
	router.Use(func(c *gin.Context) {
		ctx := context.WithValue(c.Request.Context(), clientAPIKeyContextKey{}, "publish-image-key")
		ctx = context.WithValue(ctx, neoOrbPortalOwnerContextKey{}, ownerID)
		c.Request = c.Request.WithContext(ctx)
		c.Next()
	})
	router.Any("/ampcode/local-broker/publish-image-result.json", m.serveLocalBrokerPublishImageResult)

	threadID := "T-00000000-0000-0000-0000-000000000001"
	base := neoPublishImageResult{
		RequestID: "publish-image-result", ThreadID: threadID, RunnerID: "publish-image-runner",
		BrokerID: "publish-image-broker", SessionID: "publish-image-session", Generation: 7,
		Data: base64.StdEncoding.EncodeToString([]byte("image bytes")),
	}
	addPending := func(requestID string) *neoPendingPublishImage {
		t.Helper()
		request := neoPublishImageRequest{
			RequestID: requestID, ThreadID: base.ThreadID, RunnerID: base.RunnerID, BrokerID: base.BrokerID,
			SessionID: base.SessionID, Generation: base.Generation, Path: "image.png",
		}
		pending := &neoPendingPublishImage{request: request, result: make(chan neoPublishImageDelivery, 1)}
		userActor.mu.Lock()
		userActor.pendingPublishImages[requestID] = pending
		userActor.userRunners[base.RunnerID] = neoUserExecutorRunner{
			runnerID: base.RunnerID, brokerID: base.BrokerID, sessionID: base.SessionID, sessionGeneration: base.Generation,
			runningThreads: []string{base.ThreadID}, updatedAt: time.Now(),
		}
		userActor.mu.Unlock()
		return pending
	}
	post := func(payload []byte, origin string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, "/ampcode/local-broker/publish-image-result.json", bytes.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		return rec
	}
	encode := func(result neoPublishImageResult) []byte {
		raw, err := json.Marshal(result)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}

	pending := addPending(base.RequestID)
	accepted := post(encode(base), "")
	if accepted.Code != http.StatusOK {
		t.Fatalf("valid result status=%d body=%s", accepted.Code, accepted.Body.String())
	}
	if delivery := <-pending.result; string(delivery.data) != "image bytes" || delivery.code != "" {
		t.Fatalf("valid result delivery = %#v", delivery)
	}
	if duplicate := post(encode(base), ""); duplicate.Code != http.StatusConflict {
		t.Fatalf("duplicate result status=%d body=%s", duplicate.Code, duplicate.Body.String())
	}

	base.RequestID = "publish-image-concurrent"
	addPending(base.RequestID)
	statuses := make(chan int, 2)
	var wait sync.WaitGroup
	for range 2 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			statuses <- post(encode(base), "").Code
		}()
	}
	wait.Wait()
	close(statuses)
	got := make([]int, 0, 2)
	for status := range statuses {
		got = append(got, status)
	}
	sort.Ints(got)
	want := []int{http.StatusOK, http.StatusConflict}
	if !slices.Equal(got, want) {
		t.Fatalf("concurrent duplicate statuses = %v, want %v", got, want)
	}

	base.RequestID = "publish-image-invalid"
	addPending(base.RequestID)
	identityMutations := map[string]func(*neoPublishImageResult){
		"request":    func(result *neoPublishImageResult) { result.RequestID = "wrong-request" },
		"thread":     func(result *neoPublishImageResult) { result.ThreadID = "T-00000000-0000-0000-0000-000000000002" },
		"runner":     func(result *neoPublishImageResult) { result.RunnerID = "wrong-runner" },
		"broker":     func(result *neoPublishImageResult) { result.BrokerID = "wrong-broker" },
		"session":    func(result *neoPublishImageResult) { result.SessionID = "wrong-session" },
		"generation": func(result *neoPublishImageResult) { result.Generation++ },
	}
	for name, mutate := range identityMutations {
		invalid := base
		mutate(&invalid)
		if rec := post(encode(invalid), ""); rec.Code != http.StatusConflict {
			t.Fatalf("wrong %s status=%d body=%s", name, rec.Code, rec.Body.String())
		}
	}

	otherOwnerRouter := gin.New()
	otherOwnerRouter.Use(func(c *gin.Context) {
		ctx := context.WithValue(c.Request.Context(), clientAPIKeyContextKey{}, "publish-image-key")
		ctx = context.WithValue(ctx, neoOrbPortalOwnerContextKey{}, "another-owner")
		c.Request = c.Request.WithContext(ctx)
		c.Next()
	})
	otherOwnerRouter.Any("/ampcode/local-broker/publish-image-result.json", m.serveLocalBrokerPublishImageResult)
	wrongOwnerRequest := httptest.NewRequest(http.MethodPost, "/ampcode/local-broker/publish-image-result.json", bytes.NewReader(encode(base)))
	wrongOwnerRequest.Header.Set("Content-Type", "application/json")
	wrongOwnerResponse := httptest.NewRecorder()
	otherOwnerRouter.ServeHTTP(wrongOwnerResponse, wrongOwnerRequest)
	if wrongOwnerResponse.Code != http.StatusNotFound {
		t.Fatalf("wrong owner status=%d body=%s", wrongOwnerResponse.Code, wrongOwnerResponse.Body.String())
	}

	invalidResults := map[string]neoPublishImageResult{
		"invalid base64": func() neoPublishImageResult { result := base; result.Data = "!!!!"; return result }(),
		"empty data":     func() neoPublishImageResult { result := base; result.Data = ""; return result }(),
		"error and data": func() neoPublishImageResult { result := base; result.ErrorCode = "read_failed"; return result }(),
		"unknown error": func() neoPublishImageResult {
			result := base
			result.Data = ""
			result.ErrorCode = "broker_secret"
			return result
		}(),
	}
	for name, invalid := range invalidResults {
		addPending(base.RequestID)
		if rec := post(encode(invalid), ""); rec.Code != http.StatusBadRequest {
			t.Fatalf("%s status=%d body=%s", name, rec.Code, rec.Body.String())
		}
	}
	oversized := base
	oversized.Data = base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{'x'}, neoAttachmentMaxImageBytes+1))
	addPending(base.RequestID)
	if rec := post(encode(oversized), ""); rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("decoded oversize status=%d body=%s", rec.Code, rec.Body.String())
	}
	errorResult := base
	errorResult.Data = ""
	errorResult.ErrorCode = "read_failed"
	errorPending := addPending(base.RequestID)
	if rec := post(encode(errorResult), ""); rec.Code != http.StatusOK {
		t.Fatalf("bounded error status=%d body=%s", rec.Code, rec.Body.String())
	}
	if delivery := <-errorPending.result; delivery.code != "read_failed" || delivery.data != nil {
		t.Fatalf("bounded error delivery = %#v", delivery)
	}
	for _, code := range []string{"read_failed", "too_large", "stale_assignment", "stale_session"} {
		err := neoPublishImageDeliveryError(code)
		if err == nil || !strings.Contains(err.Error(), code) {
			t.Fatalf("delivery error for %q = %v", code, err)
		}
	}
	if err := neoPublishImageDeliveryError("broker-secret-payload"); err == nil || strings.Contains(err.Error(), "broker-secret-payload") {
		t.Fatalf("unexpected delivery code error = %v", err)
	}
	if rec := post([]byte(`{"requestId":"x","requestId":"y"}`), ""); rec.Code != http.StatusBadRequest {
		t.Fatalf("duplicate JSON status=%d body=%s", rec.Code, rec.Body.String())
	}
	if rec := post(encode(base), "https://browser.example"); rec.Code != http.StatusForbidden {
		t.Fatalf("browser origin status=%d body=%s", rec.Code, rec.Body.String())
	}
	if rec := post(bytes.Repeat([]byte{'x'}, neoPublishImageResultBody+1), ""); rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized result status=%d body=%s", rec.Code, rec.Body.String())
	}
	methodRequest := httptest.NewRequest(http.MethodGet, "/ampcode/local-broker/publish-image-result.json", nil)
	methodResponse := httptest.NewRecorder()
	router.ServeHTTP(methodResponse, methodRequest)
	if methodResponse.Code != http.StatusMethodNotAllowed || methodResponse.Header().Get("Allow") != http.MethodPost {
		t.Fatalf("GET status=%d allow=%q", methodResponse.Code, methodResponse.Header().Get("Allow"))
	}
}

func TestServeLocalBrokerPublishImageResultAvailabilityAndAuthentication(t *testing.T) {
	gin.SetMode(gin.TestMode)
	validRuntime := newNeoRuntime(&config.Config{})
	for _, test := range []struct {
		name       string
		module     *AmpModule
		apiKey     string
		wantStatus int
		wantError  string
	}{
		{name: "nil module", apiKey: "key", wantStatus: http.StatusServiceUnavailable, wantError: "runtime_unavailable"},
		{name: "nil runtime", module: &AmpModule{}, apiKey: "key", wantStatus: http.StatusServiceUnavailable, wantError: "runtime_unavailable"},
		{name: "nil store", module: &AmpModule{neoRuntime: &neoRuntime{}}, apiKey: "key", wantStatus: http.StatusServiceUnavailable, wantError: "runtime_unavailable"},
		{name: "missing API key", module: &AmpModule{neoRuntime: validRuntime}, wantStatus: http.StatusUnauthorized, wantError: "authentication_required"},
	} {
		t.Run(test.name, func(t *testing.T) {
			router := gin.New()
			router.POST("/ampcode/local-broker/publish-image-result.json", func(c *gin.Context) {
				if test.apiKey != "" {
					ctx := context.WithValue(c.Request.Context(), clientAPIKeyContextKey{}, test.apiKey)
					c.Request = c.Request.WithContext(ctx)
				}
				test.module.serveLocalBrokerPublishImageResult(c)
			})
			request := httptest.NewRequest(http.MethodPost, "/ampcode/local-broker/publish-image-result.json", strings.NewReader(`{}`))
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			router.ServeHTTP(response, request)
			if response.Code != test.wantStatus {
				t.Fatalf("status = %d, want %d", response.Code, test.wantStatus)
			}
			var payload struct {
				OK    bool   `json:"ok"`
				Error string `json:"error"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
				t.Fatal(err)
			}
			if payload.OK || payload.Error != test.wantError {
				t.Fatalf("response = %#v, want error %q", payload, test.wantError)
			}
		})
	}
}
