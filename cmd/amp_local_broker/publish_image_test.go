//go:build darwin

package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestPublishImageRequestValidationExactBinding(t *testing.T) {
	workspace := &workspaceConfig{RunnerID: "runner-1", Path: t.TempDir()}
	localBroker := &broker{
		config:             &brokerConfig{BrokerID: "broker-1"},
		sessionID:          "session-1",
		sessionGeneration:  4,
		workspacesByRunner: map[string]*workspaceConfig{"runner-1": workspace},
	}
	valid := publishImageRequest{RequestID: "request-1", ThreadID: "T-00000000-0000-0000-0000-000000000001", RunnerID: "runner-1", BrokerID: "broker-1", SessionID: "session-1", Generation: 4, Path: "images/a.png"}
	requests := []publishImageRequest{valid}
	if err := localBroker.validatePublishImageRequests(&requests); err != nil {
		t.Fatalf("valid request: %v", err)
	}
	mutations := []func(*publishImageRequest){
		func(r *publishImageRequest) { r.BrokerID = "broker-2" },
		func(r *publishImageRequest) { r.SessionID = "session-2" },
		func(r *publishImageRequest) { r.Generation++ },
		func(r *publishImageRequest) { r.RunnerID = "runner-2" },
		func(r *publishImageRequest) { r.ThreadID = "invalid" },
		func(r *publishImageRequest) { r.Path = "../outside.png" },
	}
	for index, mutate := range mutations {
		candidate := valid
		mutate(&candidate)
		items := []publishImageRequest{candidate}
		if err := localBroker.validatePublishImageRequests(&items); err == nil {
			t.Fatalf("mutation %d unexpectedly accepted: %#v", index, candidate)
		}
	}
	requests = []publishImageRequest{valid, valid}
	if err := localBroker.validatePublishImageRequests(&requests); err == nil {
		t.Fatal("duplicate request ID unexpectedly accepted")
	}
}

func TestReadPublishImageWorkspaceFileSecurity(t *testing.T) {
	workspace := t.TempDir()
	approvedWorkspace := approvedPublishImageWorkspace(t, workspace, "")
	if err := os.Mkdir(filepath.Join(workspace, "images"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "images", "valid.png"), []byte("image"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "empty.png"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if got, err := readPublishImageWorkspaceFile(approvedWorkspace, "images/valid.png"); err != nil || string(got) != "image" {
		t.Fatalf("valid file = %q, %v", got, err)
	}
	if err := os.Link(filepath.Join(workspace, "images", "valid.png"), filepath.Join(workspace, "images", "hard-linked.png")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("valid.png", filepath.Join(workspace, "images", "link.png")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("images", filepath.Join(workspace, "link-parent")); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mkfifo(filepath.Join(workspace, "pipe"), 0o600); err != nil {
		t.Fatal(err)
	}
	oversize, err := os.Create(filepath.Join(workspace, "large.png"))
	if err != nil {
		t.Fatal(err)
	}
	if err := oversize.Truncate(publishImageMaxBytes + 1); err != nil {
		t.Fatal(err)
	}
	if err := oversize.Close(); err != nil {
		t.Fatal(err)
	}
	for _, relative := range []string{"../outside", "/tmp/outside", "images/link.png", "images/hard-linked.png", "link-parent/valid.png", "images", "pipe", "large.png", "empty.png"} {
		if _, err := readPublishImageWorkspaceFile(approvedWorkspace, relative); err == nil {
			t.Fatalf("secure read of %q unexpectedly succeeded", relative)
		}
	}
}

func TestReadPublishImageWorkspaceFileRejectsReplacedWorkspace(t *testing.T) {
	parent := t.TempDir()
	workspacePath := filepath.Join(parent, "workspace")
	if err := os.Mkdir(workspacePath, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspacePath, "image.png"), []byte("approved"), 0o600); err != nil {
		t.Fatal(err)
	}
	workspace := approvedPublishImageWorkspace(t, workspacePath, "")
	replaced := false
	openRoot := func(path string) (*os.File, error) {
		if err := os.Rename(path, filepath.Join(parent, "approved-workspace")); err != nil {
			return nil, err
		}
		if err := os.Mkdir(path, 0o700); err != nil {
			return nil, err
		}
		if err := os.WriteFile(filepath.Join(path, "image.png"), []byte("replacement"), 0o600); err != nil {
			return nil, err
		}
		replaced = true
		return openPublishImageWorkspaceRoot(path)
	}
	if _, err := readPublishImageWorkspaceFileWithRootOpener(workspace, "image.png", openRoot); err == nil {
		t.Fatal("replaced workspace unexpectedly accepted")
	}
	if !replaced {
		t.Fatal("workspace replacement seam was not exercised")
	}
}

func TestFulfillPublishImageRequestPostsBoundResult(t *testing.T) {
	workspace := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspace, "image.png"), []byte("image"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "empty.png"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	var posted publishImageResult
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != publishImageEndpoint || request.Header.Get("Authorization") != "Bearer secret" {
			t.Errorf("request = %s, authorization=%q", request.URL.Path, request.Header.Get("Authorization"))
		}
		if err := json.NewDecoder(request.Body).Decode(&posted); err != nil {
			t.Errorf("decode result: %v", err)
		}
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()
	threadID := "T-00000000-0000-0000-0000-000000000001"
	localBroker := &broker{
		config:             &brokerConfig{BrokerID: "broker-1", APIURL: server.URL, apiKey: "secret"},
		client:             server.Client(),
		requestLimit:       time.Second,
		sessionID:          "session-1",
		sessionGeneration:  2,
		workspacesByRunner: map[string]*workspaceConfig{"runner-1": approvedPublishImageWorkspace(t, workspace, "runner-1")},
		children:           map[childKey]*childProcess{{runnerID: "runner-1", threadID: threadID}: {key: childKey{runnerID: "runner-1", threadID: threadID}}},
	}
	request := publishImageRequest{RequestID: "request-1", ThreadID: threadID, RunnerID: "runner-1", BrokerID: "broker-1", SessionID: "session-1", Generation: 2, Path: "image.png"}
	if err := localBroker.fulfillPublishImageRequest(context.Background(), request); err != nil {
		t.Fatalf("fulfillPublishImageRequest: %v", err)
	}
	if posted.RequestID != request.RequestID || posted.ThreadID != threadID || posted.RunnerID != "runner-1" || posted.SessionID != "session-1" || posted.Generation != 2 || posted.ErrorCode != "" {
		t.Fatalf("posted identity = %#v", posted)
	}
	data, err := base64.StdEncoding.DecodeString(posted.Data)
	if err != nil || string(data) != "image" {
		t.Fatalf("posted data = %q, %v", data, err)
	}
	request.RequestID = "request-empty"
	request.Path = "empty.png"
	posted = publishImageResult{}
	if err := localBroker.fulfillPublishImageRequest(context.Background(), request); err != nil {
		t.Fatalf("fulfill empty publish image: %v", err)
	}
	if posted.RequestID != request.RequestID || posted.ErrorCode != "read_failed" || posted.Data != "" {
		t.Fatalf("posted empty-file failure = %#v", posted)
	}
}

func TestPostPublishImageResultUsesCallerContext(t *testing.T) {
	var requestContext context.Context
	client := &http.Client{Transport: brokerRoundTripper(func(request *http.Request) (*http.Response, error) {
		requestContext = request.Context()
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(`{"ok":true}`)),
		}, nil
	})}
	localBroker := &broker{
		config: &brokerConfig{APIURL: "https://broker.example", apiKey: "key"},
		client: client,
	}
	callerContext, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := localBroker.postPublishImageResult(callerContext, publishImageResult{RequestID: "request"}); err != nil {
		t.Fatal(err)
	}
	assertBrokerCallerContext(t, requestContext, cancel)
}

func TestProcessPublishImageRequestsDoesNotBlockHeartbeat(t *testing.T) {
	workspace := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspace, "image.png"), []byte("image"), 0o600); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		started <- struct{}{}
		<-release
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()
	threadID := "T-00000000-0000-0000-0000-000000000001"
	request := publishImageRequest{
		RequestID: "request-async", ThreadID: threadID, RunnerID: "runner-1", BrokerID: "broker-1",
		SessionID: "session-1", Generation: 2, Path: "image.png",
	}
	requests := []publishImageRequest{request}
	localBroker := &broker{
		config:             &brokerConfig{BrokerID: "broker-1", APIURL: server.URL, apiKey: "secret"},
		client:             server.Client(),
		requestLimit:       time.Second,
		sessionID:          "session-1",
		sessionGeneration:  2,
		workspacesByRunner: map[string]*workspaceConfig{"runner-1": approvedPublishImageWorkspace(t, workspace, "runner-1")},
		children:           map[childKey]*childProcess{{runnerID: "runner-1", threadID: threadID}: {key: childKey{runnerID: "runner-1", threadID: threadID}}},
		publishImageActive: make(map[string]struct{}),
		publishImageErrors: make(chan error, publishImageMaxCount),
	}
	startedAt := time.Now()
	if err := localBroker.processPublishImageRequests(context.Background(), heartbeatResponse{PublishImageRequests: &requests}); err != nil {
		t.Fatalf("process publish image requests: %v", err)
	}
	if elapsed := time.Since(startedAt); elapsed > 100*time.Millisecond {
		t.Fatalf("heartbeat processing blocked for %s", elapsed)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("publish image upload did not start")
	}
	if err := localBroker.processPublishImageRequests(context.Background(), heartbeatResponse{PublishImageRequests: &requests}); err != nil {
		t.Fatalf("deduplicate active request: %v", err)
	}
	select {
	case <-started:
		t.Fatal("active publish image request was uploaded twice")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	localBroker.publishImageWG.Wait()
}

func TestProcessPublishImageRequestsCapsActiveWorkersAndReusesSlots(t *testing.T) {
	workspace := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspace, "image.png"), []byte("image"), 0o600); err != nil {
		t.Fatal(err)
	}
	started := make(chan string, publishImageMaxCount+2)
	release := make(chan struct{})
	client := &http.Client{Transport: brokerRoundTripper(func(request *http.Request) (*http.Response, error) {
		started <- request.Header.Get("X-Request-ID")
		<-release
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(`{"ok":true}`)),
		}, nil
	})}
	threadID := "T-00000000-0000-0000-0000-000000000001"
	requests := make([]publishImageRequest, 0, publishImageMaxCount)
	for index := 0; index < publishImageMaxCount; index++ {
		requests = append(requests, publishImageRequest{
			RequestID: "request-" + string(rune('a'+index)), ThreadID: threadID, RunnerID: "runner-1", BrokerID: "broker-1",
			SessionID: "session-1", Generation: 2, Path: "image.png",
		})
	}
	localBroker := &broker{
		config:             &brokerConfig{BrokerID: "broker-1", APIURL: "https://broker.example", apiKey: "secret"},
		client:             client,
		requestLimit:       time.Second,
		sessionID:          "session-1",
		sessionGeneration:  2,
		workspacesByRunner: map[string]*workspaceConfig{"runner-1": approvedPublishImageWorkspace(t, workspace, "runner-1")},
		children:           map[childKey]*childProcess{{runnerID: "runner-1", threadID: threadID}: {key: childKey{runnerID: "runner-1", threadID: threadID}}},
		publishImageActive: make(map[string]struct{}),
		publishImageErrors: make(chan error, publishImageMaxCount),
	}
	if err := localBroker.processPublishImageRequests(t.Context(), heartbeatResponse{PublishImageRequests: &requests}); err != nil {
		t.Fatalf("fill active publish image slots: %v", err)
	}
	for range publishImageMaxCount {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("publish image worker did not start")
		}
	}
	localBroker.mu.Lock()
	active := len(localBroker.publishImageActive)
	localBroker.mu.Unlock()
	if active != publishImageMaxCount {
		t.Fatalf("active publish image workers = %d, want %d", active, publishImageMaxCount)
	}
	extra := []publishImageRequest{{
		RequestID: "request-extra", ThreadID: threadID, RunnerID: "runner-1", BrokerID: "broker-1",
		SessionID: "session-1", Generation: 2, Path: "image.png",
	}}
	if err := localBroker.processPublishImageRequests(t.Context(), heartbeatResponse{PublishImageRequests: &extra}); err == nil || !strings.Contains(err.Error(), "too many active publish image requests") {
		t.Fatalf("full active set error = %v", err)
	}
	deduplicated := requests[:1]
	if err := localBroker.processPublishImageRequests(t.Context(), heartbeatResponse{PublishImageRequests: &deduplicated}); err != nil {
		t.Fatalf("deduplicate full active set: %v", err)
	}
	select {
	case requestID := <-started:
		t.Fatalf("unexpected extra publish image worker %q", requestID)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	localBroker.publishImageWG.Wait()
	localBroker.mu.Lock()
	active = len(localBroker.publishImageActive)
	localBroker.mu.Unlock()
	if active != 0 {
		t.Fatalf("active publish image workers after completion = %d", active)
	}
	if err := localBroker.processPublishImageRequests(t.Context(), heartbeatResponse{PublishImageRequests: &extra}); err != nil {
		t.Fatalf("reuse publish image slot: %v", err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("reused publish image slot did not start")
	}
	localBroker.publishImageWG.Wait()
}

func approvedPublishImageWorkspace(t *testing.T, path, runnerID string) *workspaceConfig {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return &workspaceConfig{RunnerID: runnerID, Path: path, info: info}
}

func TestStopAllCancelsBlockingPublishImageUpload(t *testing.T) {
	uploadStarted := make(chan struct{}, publishImageMaxCount)
	uploadCanceled := make(chan struct{}, publishImageMaxCount)
	client := &http.Client{Transport: brokerRoundTripper(func(request *http.Request) (*http.Response, error) {
		uploadStarted <- struct{}{}
		<-request.Context().Done()
		uploadCanceled <- struct{}{}
		return nil, request.Context().Err()
	})}

	requests := make([]publishImageRequest, 0, publishImageMaxCount)
	for index := 0; index < publishImageMaxCount; index++ {
		requests = append(requests, publishImageRequest{
			RequestID: "request-blocking-" + string(rune('a'+index)), ThreadID: "T-00000000-0000-0000-0000-000000000001", RunnerID: "runner-1", BrokerID: "broker-1",
			SessionID: "session-1", Generation: 2, Path: "image.png",
		})
	}
	localBroker := &broker{
		config:             &brokerConfig{BrokerID: "broker-1", APIURL: "https://broker.example", apiKey: "secret"},
		client:             client,
		requestLimit:       time.Minute,
		sessionID:          "session-1",
		sessionGeneration:  2,
		workspacesByRunner: map[string]*workspaceConfig{"runner-1": {RunnerID: "runner-1", Path: t.TempDir()}},
		children:           map[childKey]*childProcess{},
		launches:           map[childKey]*childLaunch{},
		publishImageActive: make(map[string]struct{}),
		publishImageErrors: make(chan error, publishImageMaxCount),
	}
	t.Cleanup(func() { _ = localBroker.stopAll() })
	if err := localBroker.processPublishImageRequests(t.Context(), heartbeatResponse{PublishImageRequests: &requests}); err != nil {
		t.Fatalf("process publish image requests: %v", err)
	}
	for range publishImageMaxCount {
		select {
		case <-uploadStarted:
		case <-time.After(time.Second):
			t.Fatal("publish image upload did not start")
		}
	}

	stopped := make(chan error, 1)
	go func() { stopped <- localBroker.stopAll() }()
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatalf("stopAll: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("stopAll waited on the blocking upload")
	}
	for range publishImageMaxCount {
		select {
		case <-uploadCanceled:
		case <-time.After(time.Second):
			t.Fatal("blocking upload request context was not canceled")
		}
	}
}
