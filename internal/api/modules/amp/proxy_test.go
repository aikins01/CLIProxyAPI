package amp

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
)

// Helper: compress data with gzip
func gzipBytes(b []byte) []byte {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	zw.Write(b)
	zw.Close()
	return buf.Bytes()
}

// Helper: create a mock http.Response
func mkResp(status int, hdr http.Header, body []byte) *http.Response {
	if hdr == nil {
		hdr = http.Header{}
	}
	return &http.Response{
		StatusCode:    status,
		Header:        hdr,
		Body:          io.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
	}
}

func TestAmpProxyThreadDeleteSucceededDecodesSvelteCommandResult(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "https://ampcode.com/_app/remote/145jw2/deleteThreadCommand", nil)
	tests := []struct {
		name   string
		result map[string]any
		want   bool
	}{
		{name: "success", result: map[string]any{"ok": true}, want: true},
		{name: "application failure", result: neoWebLocalRemoteCommandError("permission denied"), want: false},
		{name: "not found", result: map[string]any{"ok": false, "error": map[string]any{"code": "thread-not-found"}}, want: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			writeNeoSvelteKitRemoteCommand(recorder, http.StatusOK, tc.result)
			if got := ampProxyThreadDeleteSucceeded(req, recorder.Body.Bytes()); got != tc.want {
				t.Fatalf("delete success = %v, want %v; body=%s", got, tc.want, recorder.Body.String())
			}
		})
	}
}

func TestAmpProxyThreadDeleteSucceededRejectsAmbiguousInternalResponses(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "https://ampcode.com/api/internal?deleteThread", nil)
	for _, body := range []string{
		`{}`,
		`{"ok":"false"}`,
		`{"ok":false}`,
		`{"ok":false,"error":{"code":"permission-denied"}}`,
		`not-json`,
	} {
		if ampProxyThreadDeleteSucceeded(req, []byte(body)) {
			t.Fatalf("ambiguous delete response was accepted: %s", body)
		}
	}
	if !ampProxyThreadDeleteSucceeded(req, []byte(`{"ok":true}`)) {
		t.Fatal("explicit delete success was rejected")
	}
	if !ampProxyThreadDeleteSucceeded(req, []byte(`{"ok":false,"error":{"code":"thread-not-found"}}`)) {
		t.Fatal("thread-not-found delete response was rejected")
	}
}

type failingAmpProxyReadCloser struct{}

func (failingAmpProxyReadCloser) Read([]byte) (int, error) {
	return 0, errors.New("read failed")
}

func (failingAmpProxyReadCloser) Close() error {
	return nil
}

func TestApplyAmpProxyThreadDeletePropagatesBodyReadFailure(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "https://ampcode.com/_app/remote/145jw2/deleteThreadCommand", nil)
	req = req.WithContext(context.WithValue(req.Context(), ampProxyThreadDeleteContextKey{}, ampProxyThreadDelete{apply: func() error { return nil }}))
	response := &http.Response{StatusCode: http.StatusOK, Request: req, Body: failingAmpProxyReadCloser{}}

	err := applyAmpProxyThreadDelete(response)
	if err == nil || !strings.Contains(err.Error(), "read delete response") {
		t.Fatalf("error = %v, want response read failure", err)
	}
}

func TestApplyAmpProxyThreadDeletePropagatesLocalPurgeFailure(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "https://ampcode.com/api/internal?method=deleteThread", nil)
	req = req.WithContext(context.WithValue(req.Context(), ampProxyThreadDeleteContextKey{}, ampProxyThreadDelete{apply: func() error {
		return errors.New("snapshot remove failed")
	}}))
	response := mkResp(http.StatusOK, http.Header{"Content-Type": []string{"application/json"}}, []byte(`{"ok":true}`))
	response.Request = req

	err := applyAmpProxyThreadDelete(response)
	if err == nil || !strings.Contains(err.Error(), "purge local thread") || !strings.Contains(err.Error(), "snapshot remove failed") {
		t.Fatalf("error = %v, want local purge failure", err)
	}
}

func TestAttachNeoLocalThreadDeleteWaitsForCloudUpload(t *testing.T) {
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
		UpstreamURL:    "https://ampcode.com",
		UpstreamAPIKey: "secret",
	}})
	rt.threadDir = t.TempDir()
	threadID := "T-019f7000-0000-7000-8000-000000000081"
	actor := rt.store.ensureThreadActor(threadID)
	actor.mu.Lock()
	actor.title = "Delete upload race"
	actor.mu.Unlock()
	uploadStarted := make(chan struct{}, 2)
	releaseUpload := make(chan struct{})
	rt.uploadCloudSnapshot = func(neoCloudThreadSnapshot) (int64, error) {
		uploadStarted <- struct{}{}
		<-releaseUpload
		return 1, nil
	}
	actor.syncCloudAsync()
	select {
	case <-uploadStarted:
	case <-time.After(time.Second):
		t.Fatal("cloud upload did not start")
	}

	req := httptest.NewRequest(http.MethodPost, "https://ampcode.com/api/internal?deleteThread", bytes.NewBufferString(`{"params":{"thread":"`+threadID+`"}}`))
	module := &AmpModule{neoRuntime: rt}
	attached := make(chan struct{})
	go func() {
		module.attachNeoLocalThreadDelete(req)
		close(attached)
	}()
	select {
	case <-attached:
		t.Fatal("delete was forwarded before the cloud upload finished")
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseUpload)
	select {
	case <-attached:
	case <-time.After(time.Second):
		t.Fatal("delete did not resume after the cloud upload finished")
	}

	response := mkResp(http.StatusOK, http.Header{"Content-Type": []string{"application/json"}}, []byte(`{"ok":true}`))
	response.Request = req
	if err := applyAmpProxyThreadDelete(response); err != nil {
		t.Fatal(err)
	}
	if rt.store.lookupThreadActor(threadID) != nil || !rt.neoThreadDeleted(threadID) {
		t.Fatal("successful delete did not purge and tombstone the local thread")
	}
	actor.syncCloudAsync()
	select {
	case <-uploadStarted:
		t.Fatal("deleted thread scheduled another cloud upload")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestApplyAmpProxyThreadDeleteRestoresCloudSyncAfterFailure(t *testing.T) {
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{
		UpstreamURL:    "https://ampcode.com",
		UpstreamAPIKey: "secret",
	}})
	rt.threadDir = t.TempDir()
	threadID := "T-019f7000-0000-7000-8000-000000000082"
	actor := rt.store.ensureThreadActor(threadID)
	actor.mu.Lock()
	actor.title = "Failed cloud delete"
	actor.mu.Unlock()
	uploaded := make(chan struct{}, 1)
	rt.uploadCloudSnapshot = func(neoCloudThreadSnapshot) (int64, error) {
		uploaded <- struct{}{}
		return 1, nil
	}
	req := httptest.NewRequest(http.MethodPost, "https://ampcode.com/api/internal?deleteThread", bytes.NewBufferString(`{"params":{"thread":"`+threadID+`"}}`))
	module := &AmpModule{neoRuntime: rt}
	module.attachNeoLocalThreadDelete(req)
	response := mkResp(http.StatusOK, http.Header{"Content-Type": []string{"application/json"}}, []byte(`{"ok":false,"error":{"code":"permission-denied"}}`))
	response.Request = req
	if err := applyAmpProxyThreadDelete(response); err != nil {
		t.Fatal(err)
	}
	actor.syncCloudAsync()
	select {
	case <-uploaded:
	case <-time.After(time.Second):
		t.Fatal("failed delete did not restore cloud synchronization")
	}
	waitForNeoActorSyncIdle(t, actor)
}

func TestModifyResponseRestoresThreadDeleteWhenResponseIsSkipped(t *testing.T) {
	proxy, err := createReverseProxy("http://example.com", NewStaticSecretSource("k"))
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		header http.Header
	}{
		{name: "encoded", header: http.Header{"Content-Encoding": []string{"gzip"}}},
		{name: "streaming", header: http.Header{"Content-Type": []string{"text/event-stream"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			applied := 0
			restored := 0
			req := httptest.NewRequest(http.MethodPost, "https://ampcode.com/api/internal?deleteThread", nil)
			req = req.WithContext(context.WithValue(req.Context(), ampProxyThreadDeleteContextKey{}, ampProxyThreadDelete{
				apply: func() error {
					applied++
					return nil
				},
				restore: func() {
					restored++
				},
			}))
			response := mkResp(http.StatusOK, test.header, []byte(`{"ok":true}`))
			response.Request = req
			if err := proxy.ModifyResponse(response); err != nil {
				t.Fatal(err)
			}
			if applied != 0 || restored != 1 {
				t.Fatalf("apply calls = %d, restore calls = %d", applied, restored)
			}
		})
	}
}

func TestNeoCloudThreadDeleteStaysBlockedUntilAllAttemptsFinish(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	threadID := "T-019f7000-0000-7000-8000-000000000083"
	actor := rt.store.ensureThreadActor(threadID)
	first := rt.beginNeoCloudThreadDelete(threadID)
	second := rt.beginNeoCloudThreadDelete(threadID)

	rt.cancelNeoCloudThreadDelete(threadID, first)
	if !rt.neoCloudThreadSyncBlocked(threadID) {
		t.Fatal("first failed delete reopened cloud synchronization while another delete was pending")
	}
	actor.mu.Lock()
	closing := actor.cloudSyncClosing
	actor.mu.Unlock()
	if !closing {
		t.Fatal("first failed delete reopened actor cloud synchronization")
	}

	rt.cancelNeoCloudThreadDelete(threadID, second)
	if rt.neoCloudThreadSyncBlocked(threadID) {
		t.Fatal("final failed delete left cloud synchronization blocked")
	}
	actor.mu.Lock()
	closing = actor.cloudSyncClosing
	actor.mu.Unlock()
	if closing {
		t.Fatal("final failed delete did not reopen actor cloud synchronization")
	}
}

func TestCreateReverseProxy_ValidURL(t *testing.T) {
	proxy, err := createReverseProxy("http://example.com", NewStaticSecretSource("key"))
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if proxy == nil {
		t.Fatal("expected proxy to be created")
	}
}

func TestCreateReverseProxy_InvalidURL(t *testing.T) {
	_, err := createReverseProxy("://invalid", NewStaticSecretSource("key"))
	if err == nil {
		t.Fatal("expected error for invalid URL")
	}
}

func TestCreateReverseProxy_PreservesAmpClientVersionWithoutOverride(t *testing.T) {
	proxy, err := createReverseProxy("https://ampcode.test", NewStaticSecretSource("key"))
	if err != nil {
		t.Fatalf("create proxy: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "http://proxy.local/api/internal?webSearch2", nil)
	req.Header.Set("X-Amp-Client-Version", "0.0.1779896748-g596c49")

	proxy.Director(req)

	if got := req.Header.Get("X-Amp-Client-Version"); got != "0.0.1779896748-g596c49" {
		t.Fatalf("X-Amp-Client-Version = %q", got)
	}
}

func TestCreateReverseProxy_OverridesAmpClientVersionWhenConfigured(t *testing.T) {
	proxy, err := createReverseProxyWithClientVersionOverride("https://ampcode.test", NewStaticSecretSource("key"), " 0.0.1780359918-g778c3a ")
	if err != nil {
		t.Fatalf("create proxy: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "http://proxy.local/api/internal?webSearch2", nil)
	req.Header.Set("X-Amp-Client-Version", "0.0.1779896748-g596c49")

	proxy.Director(req)

	if got := req.Header.Get("X-Amp-Client-Version"); got != "0.0.1780359918-g778c3a" {
		t.Fatalf("X-Amp-Client-Version = %q", got)
	}
}

func TestModifyResponse_GzipScenarios(t *testing.T) {
	proxy, err := createReverseProxy("http://example.com", NewStaticSecretSource("k"))
	if err != nil {
		t.Fatal(err)
	}

	goodJSON := []byte(`{"ok":true}`)
	good := gzipBytes(goodJSON)
	truncated := good[:10]
	corrupted := append([]byte{0x1f, 0x8b}, []byte("notgzip")...)

	cases := []struct {
		name     string
		header   http.Header
		body     []byte
		status   int
		wantBody []byte
		wantCE   string
	}{
		{
			name:     "decompresses_valid_gzip_no_header",
			header:   http.Header{},
			body:     good,
			status:   200,
			wantBody: goodJSON,
			wantCE:   "",
		},
		{
			name:     "skips_when_ce_present",
			header:   http.Header{"Content-Encoding": []string{"gzip"}},
			body:     good,
			status:   200,
			wantBody: good,
			wantCE:   "gzip",
		},
		{
			name:     "passes_truncated_unchanged",
			header:   http.Header{},
			body:     truncated,
			status:   200,
			wantBody: truncated,
			wantCE:   "",
		},
		{
			name:     "passes_corrupted_unchanged",
			header:   http.Header{},
			body:     corrupted,
			status:   200,
			wantBody: corrupted,
			wantCE:   "",
		},
		{
			name:     "non_gzip_unchanged",
			header:   http.Header{},
			body:     []byte("plain"),
			status:   200,
			wantBody: []byte("plain"),
			wantCE:   "",
		},
		{
			name:     "empty_body",
			header:   http.Header{},
			body:     []byte{},
			status:   200,
			wantBody: []byte{},
			wantCE:   "",
		},
		{
			name:     "single_byte_body",
			header:   http.Header{},
			body:     []byte{0x1f},
			status:   200,
			wantBody: []byte{0x1f},
			wantCE:   "",
		},
		{
			name:     "decompresses_non_2xx_status_when_gzip_detected",
			header:   http.Header{},
			body:     good,
			status:   404,
			wantBody: goodJSON,
			wantCE:   "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := mkResp(tc.status, tc.header, tc.body)
			if err := proxy.ModifyResponse(resp); err != nil {
				t.Fatalf("ModifyResponse error: %v", err)
			}
			got, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("ReadAll error: %v", err)
			}
			if !bytes.Equal(got, tc.wantBody) {
				t.Fatalf("body mismatch:\nwant: %q\ngot:  %q", tc.wantBody, got)
			}
			if ce := resp.Header.Get("Content-Encoding"); ce != tc.wantCE {
				t.Fatalf("Content-Encoding: want %q, got %q", tc.wantCE, ce)
			}
		})
	}
}

func TestModifyResponse_NormalizesThreadListRelationships(t *testing.T) {
	proxy, err := createReverseProxy("http://example.com", NewStaticSecretSource("k"))
	if err != nil {
		t.Fatal(err)
	}

	body := []byte(`{"ok":true,"result":{"threads":[{"id":"T-1","title":"missing"},{"id":"T-2","title":"kept","relationships":[{"threadID":"T-1"}]}]}}`)
	resp := mkResp(http.StatusOK, http.Header{"Content-Type": []string{"application/json"}}, body)
	resp.Request = httptest.NewRequest(http.MethodPost, "http://proxy.local/api/internal?listThreads", nil)

	if err := proxy.ModifyResponse(resp); err != nil {
		t.Fatalf("ModifyResponse error: %v", err)
	}
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll error: %v", err)
	}
	var decoded any
	if err := json.Unmarshal(got, &decoded); err != nil {
		t.Fatalf("response JSON error: %v; body=%s", err, got)
	}
	assertThreadRelationshipsForTest(t, decoded)
}

func TestModifyResponse_NormalizesThreadListRelationshipsPreservesLargeNumbers(t *testing.T) {
	proxy, err := createReverseProxy("http://example.com", NewStaticSecretSource("k"))
	if err != nil {
		t.Fatal(err)
	}

	body := []byte(`{"ok":true,"result":{"threads":[{"id":"T-1","title":"missing","precise":9007199254740993}]}}`)
	resp := mkResp(http.StatusOK, http.Header{"Content-Type": []string{"application/json"}}, body)
	resp.Request = httptest.NewRequest(http.MethodPost, "http://proxy.local/api/internal?listThreads", nil)

	if err := proxy.ModifyResponse(resp); err != nil {
		t.Fatalf("ModifyResponse error: %v", err)
	}
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll error: %v", err)
	}
	if !bytes.Contains(got, []byte(`"precise":9007199254740993`)) {
		t.Fatalf("large numeric field changed: %s", got)
	}
	var decoded any
	if err := json.Unmarshal(got, &decoded); err != nil {
		t.Fatalf("response JSON error: %v; body=%s", err, got)
	}
	assertThreadRelationshipsForTest(t, decoded)
}

func TestModifyResponse_NormalizesBodyBasedThreadListRelationships(t *testing.T) {
	proxy, err := createReverseProxy("http://example.com", NewStaticSecretSource("k"))
	if err != nil {
		t.Fatal(err)
	}

	body := []byte(`{"ok":true,"result":{"threads":[{"id":"T-1","title":"missing"}]}}`)
	req := httptest.NewRequest(http.MethodPost, "http://proxy.local/api/internal", strings.NewReader(`{"method":"listThreads","params":{"limit":20}}`))
	proxy.Director(req)
	resp := mkResp(http.StatusOK, http.Header{"Content-Type": []string{"application/json"}}, body)
	resp.Request = req

	if err := proxy.ModifyResponse(resp); err != nil {
		t.Fatalf("ModifyResponse error: %v", err)
	}
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll error: %v", err)
	}
	var decoded any
	if err := json.Unmarshal(got, &decoded); err != nil {
		t.Fatalf("response JSON error: %v; body=%s", err, got)
	}
	assertThreadRelationshipsForTest(t, decoded)
}

func TestModifyResponse_MergesLocalRuntimeThreadsIntoUpstreamThreadList(t *testing.T) {
	allowTestTemporaryProjectDirectories(t)
	enabled := true
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled}}})
	rt.threadDir = t.TempDir()
	workingDirectory := t.TempDir()
	localThreadID := "T-019f70b9-5c65-73a7-8629-3b394bd51d01"
	if _, err := writeNeoLocalThreadFileInDir(rt.threadDir, localThreadID, map[string]any{
		"id":    localThreadID,
		"title": "Local unsynced thread",
		"meta":  map[string]any{"cliProxyAPILocalNeo": true},
		"env":   map[string]any{"workingDirectory": workingDirectory},
		"messages": []any{
			map[string]any{"messageId": "M-local", "role": "user", "content": []any{map[string]any{"type": "text", "text": "persist locally"}}},
			map[string]any{"messageId": "M-local-assistant", "role": "assistant", "content": []any{map[string]any{"type": "tool_use", "name": "apply_patch", "input": map[string]any{"patchText": "*** Begin Patch\n*** Update File: example.go\n@@\n-old\n+new\n+extra\n*** End Patch"}}}},
		},
	}); err != nil {
		t.Fatalf("write local thread: %v", err)
	}

	proxy, err := createReverseProxy("http://example.com", NewStaticSecretSource("k"))
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "http://proxy.local/api/internal?listThreads", strings.NewReader(`{"method":"listThreads","params":{"limit":20,"excludeLabelNames":["child-thread","review"]}}`))
	m := &AmpModule{neoRuntime: rt}
	m.attachNeoLocalThreadListAugmenter(req)
	proxy.Director(req)
	remoteThreadID := "T-019f70b9-5c65-73a7-8629-3b394bd51d02"
	resp := mkResp(http.StatusOK, http.Header{"Content-Type": []string{"application/json"}}, []byte(`{"ok":true,"result":{"threads":[{"id":"`+remoteThreadID+`","threadId":"`+remoteThreadID+`","title":"Upstream thread","lastUserMessageAt":"2020-01-01T00:00:00Z"}]}}`))
	resp.Request = req

	if err := proxy.ModifyResponse(resp); err != nil {
		t.Fatalf("ModifyResponse error: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	var decoded any
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("response JSON error: %v; body=%s", err, body)
	}
	threads := threadListItemsForTest(decoded)
	if len(threads) != 2 {
		t.Fatalf("thread list = %#v, want upstream and local threads", threads)
	}
	byID := map[string]map[string]any{}
	for _, thread := range threads {
		byID[ampThreadListItemID(thread)] = thread
	}
	local := byID[localThreadID]
	if stringValue(local["title"]) != "Local unsynced thread" {
		t.Fatalf("local thread = %#v", local)
	}
	if stringValue(local["projectName"]) != filepath.Base(workingDirectory) {
		t.Fatalf("local projectName = %#v, want %q", local["projectName"], filepath.Base(workingDirectory))
	}
	if _, ok := local["relationships"].([]any); !ok {
		t.Fatalf("local relationships = %#v, want array", local["relationships"])
	}
	if _, err := time.Parse(time.RFC3339Nano, stringValue(local["userLastInteractedAt"])); err != nil {
		t.Fatalf("local userLastInteractedAt = %#v: %v", local["userLastInteractedAt"], err)
	}
	if numberFrom(local["messageCount"]) != 2 {
		t.Fatalf("local messageCount = %#v, want 2", local["messageCount"])
	}
	summaryStats := mapValue(local["summaryStats"])
	if numberFrom(summaryStats["messageCount"]) != 2 {
		t.Fatalf("local summaryStats = %#v, want messageCount 2", local["summaryStats"])
	}
	diffStats := mapValue(summaryStats["diffStats"])
	if numberFrom(diffStats["added"]) != 2 || numberFrom(diffStats["changed"]) != 1 || numberFrom(diffStats["deleted"]) != 1 {
		t.Fatalf("local diffStats = %#v, want added 2 changed 1 deleted 1", diffStats)
	}
	if stringValue(local["executorType"]) != "local-client" {
		t.Fatalf("local executorType = %#v, want local-client", local["executorType"])
	}
	trees := arrayValue(nestedValue(nestedValue(local["env"], "initial"), "trees"))
	if len(trees) != 1 || stringValue(mapValue(trees[0])["uri"]) == "" {
		t.Fatalf("local env initial trees = %#v, want workspace", trees)
	}
	if numberFrom(local["created"]) <= 0 {
		t.Fatalf("local created = %#v, want timestamp", local["created"])
	}
	if byID[remoteThreadID] == nil {
		t.Fatalf("upstream thread missing from %#v", threads)
	}
}

func TestAttachNeoLocalThreadListAugmenterLoadsRequestedWindow(t *testing.T) {
	enabled := true
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled}}})
	rt.threadDir = t.TempDir()
	for index := range 3 {
		threadID := fmt.Sprintf("T-019f70b9-5c65-73a7-8629-%012x", index+1)
		if err := writeNeoLocalThreadSnapshotToDir(neoCloudThreadSnapshot{
			threadID:  threadID,
			createdMs: int64(1778170000000 + index),
			title:     fmt.Sprintf("Local thread %d", index+1),
			messages: []neoMessage{{
				MessageID: fmt.Sprintf("M-local-%d", index+1),
				Role:      "user",
				Content:   []any{map[string]any{"type": "text", "text": "persist locally"}},
			}},
		}, rt.threadDir); err != nil {
			t.Fatalf("write local thread %d: %v", index+1, err)
		}
	}
	if err := writeNeoLocalThreadSnapshotToDir(neoCloudThreadSnapshot{
		threadID:  "T-019f70b9-5c65-73a7-8629-000000000004",
		createdMs: 1778160000000,
		title:     "Archived local thread",
		archived:  true,
		messages: []neoMessage{{
			MessageID: "M-local-archived",
			Role:      "user",
			Content:   []any{map[string]any{"type": "text", "text": "archived locally"}},
		}},
	}, rt.threadDir); err != nil {
		t.Fatalf("write archived local thread: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "http://proxy.local/api/internal?listThreads", strings.NewReader(`{"method":"listThreads","params":{"offset":2,"limit":1}}`))
	m := &AmpModule{neoRuntime: rt}
	m.attachNeoLocalThreadListAugmenter(req)
	augmenter, ok := req.Context().Value(ampProxyThreadListAugmenterContextKey{}).(ampProxyThreadListAugmenter)
	if !ok || augmenter.load == nil {
		t.Fatal("thread list augmenter was not attached")
	}
	body, err := readAndRestoreNeoJSONBody(req)
	if err != nil {
		t.Fatal(err)
	}
	params := mapValue(body["params"])
	if numberFrom(params["offset"]) != 0 || numberFrom(params["limit"]) != 4 {
		t.Fatalf("rewritten persisted exclusion window = %#v, want offset 0 limit 4", params)
	}
	rt.localRecentMu.Lock()
	fullHistoryLoaded := rt.localRecentLoaded
	rt.localRecentMu.Unlock()
	if fullHistoryLoaded {
		t.Fatal("bounded CLI thread window populated the full-history cache")
	}
	if loaded := augmenter.load(3); len(loaded) != 3 {
		t.Fatalf("loaded local thread window = %d, want 3", len(loaded))
	}
}

func TestRewriteAmpThreadListRequestWindowAddsOmittedQueryOffset(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "http://proxy.local/api/internal?method=listThreads&limit=20", nil)
	if !rewriteAmpThreadListRequestWindow(req, ampProxyThreadListAugmenter{limit: 20, upstreamOverfetch: 1}) {
		t.Fatal("query-only listThreads window rewrite was rejected")
	}
	query := req.URL.Query()
	if query.Get("offset") != "0" || query.Get("limit") != "21" {
		t.Fatalf("rewritten query = %q", req.URL.RawQuery)
	}
}

func TestRewriteAmpThreadListRequestWindowCapsRefillToOnePage(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "http://proxy.local/api/internal?listThreads", strings.NewReader(`{"method":"listThreads","params":{"limit":50}}`))
	if !rewriteAmpThreadListRequestWindow(req, ampProxyThreadListAugmenter{limit: 50, upstreamOverfetch: 1000}) {
		t.Fatal("large thread list refill window was rejected")
	}
	body, err := readAndRestoreNeoJSONBody(req)
	if err != nil {
		t.Fatal(err)
	}
	if got := numberFrom(mapValue(body["params"])["limit"]); got != 100 {
		t.Fatalf("rewritten upstream limit = %d, want 100", got)
	}
}

func TestRewriteAmpThreadListRequestWindowRejectsWindowBeyondUpstreamLimit(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "http://proxy.local/api/internal?listThreads", strings.NewReader(`{"method":"listThreads","params":{"offset":1000,"limit":50}}`))
	if rewriteAmpThreadListRequestWindow(req, ampProxyThreadListAugmenter{offset: 1000, limit: 50, upstreamOverfetch: 1000}) {
		t.Fatal("thread list window beyond the upstream limit was rewritten")
	}
	body, err := readAndRestoreNeoJSONBody(req)
	if err != nil {
		t.Fatal(err)
	}
	params := mapValue(body["params"])
	if got := numberFrom(params["offset"]); got != 1000 {
		t.Fatalf("upstream offset = %d, want original 1000", got)
	}
	if got := numberFrom(params["limit"]); got != 50 {
		t.Fatalf("upstream limit = %d, want original 50", got)
	}

	maximumReq := httptest.NewRequest(http.MethodPost, "http://proxy.local/api/internal?listThreads", strings.NewReader(`{"method":"listThreads","params":{"offset":450,"limit":50}}`))
	if !rewriteAmpThreadListRequestWindow(maximumReq, ampProxyThreadListAugmenter{offset: 450, limit: 50}) {
		t.Fatal("thread list window ending at the upstream limit was rejected")
	}

	enabled := true
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled}}})
	rt.threadDir = t.TempDir()
	deepPageReq := httptest.NewRequest(http.MethodPost, "http://proxy.local/api/internal?listThreads", strings.NewReader(`{"method":"listThreads","params":{"offset":1000,"limit":50}}`))
	(&AmpModule{neoRuntime: rt}).attachNeoLocalThreadListAugmenter(deepPageReq)
	augmenter, ok := deepPageReq.Context().Value(ampProxyThreadListAugmenterContextKey{}).(ampProxyThreadListAugmenter)
	if !ok || augmenter.upstreamRebased {
		t.Fatalf("deep-page augmenter = %#v, want attached non-rebased augmentation", augmenter)
	}
	deepPageBody, err := readAndRestoreNeoJSONBody(deepPageReq)
	if err != nil {
		t.Fatal(err)
	}
	if params := mapValue(deepPageBody["params"]); numberFrom(params["offset"]) != 1000 || numberFrom(params["limit"]) != 50 {
		t.Fatalf("deep-page upstream request was changed: %#v", params)
	}
}

func TestAmpThreadListNonRebasedWindowKeepsSelectedAndInRangeLocalThreads(t *testing.T) {
	payload := map[string]any{"threads": []any{
		map[string]any{"id": "T-019f7000-0000-7000-8000-000000000101", "updatedAt": "2026-07-20T00:00:00Z"},
		map[string]any{"id": "T-019f7000-0000-7000-8000-000000000102", "updatedAt": "2026-07-10T00:00:00Z"},
	}}
	selectedID := "T-019f7000-0000-7000-8000-000000000101"
	inRangeID := "T-019f7000-0000-7000-8000-000000000103"
	filtered := ampThreadListNonRebasedWindow(payload, []any{
		map[string]any{"id": selectedID},
		map[string]any{"id": inRangeID, "updatedAt": "2026-07-15T00:00:00Z"},
		map[string]any{"id": "T-019f7000-0000-7000-8000-000000000104", "updatedAt": "2026-07-25T00:00:00Z"},
		map[string]any{"id": "T-019f7000-0000-7000-8000-000000000105", "updatedAt": "2026-07-05T00:00:00Z"},
	}, 2)
	if len(filtered) != 2 || ampThreadListItemID(mapValue(filtered[0])) != selectedID || ampThreadListItemID(mapValue(filtered[1])) != inRangeID {
		t.Fatalf("non-rebased local window = %#v", filtered)
	}
}

func TestModifyResponse_MergesArchivedLocalRuntimeThreadsWhenRequested(t *testing.T) {
	enabled := true
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled}}})
	rt.threadDir = t.TempDir()
	threadID := "T-019f70b9-5c65-73a7-8629-3b394bd51d03"
	if _, err := writeNeoLocalThreadFileInDir(rt.threadDir, threadID, map[string]any{
		"id":       threadID,
		"title":    "Archived local thread",
		"archived": true,
		"meta":     map[string]any{"cliProxyAPILocalNeo": true},
		"messages": []any{map[string]any{"messageId": "M-local", "role": "user", "content": []any{map[string]any{"type": "text", "text": "archived locally"}}}},
	}); err != nil {
		t.Fatalf("write local thread: %v", err)
	}

	proxy, err := createReverseProxy("http://example.com", NewStaticSecretSource("k"))
	if err != nil {
		t.Fatal(err)
	}
	defaultReq := httptest.NewRequest(http.MethodPost, "http://proxy.local/api/internal?listThreads", strings.NewReader(`{"method":"listThreads","params":{"limit":20}}`))
	m := &AmpModule{neoRuntime: rt}
	m.attachNeoLocalThreadListAugmenter(defaultReq)
	proxy.Director(defaultReq)
	defaultResp := mkResp(http.StatusOK, http.Header{"Content-Type": []string{"application/json"}}, []byte(`{"ok":true,"result":{"threads":[{"id":"`+threadID+`","title":"stale upstream copy"}]}}`))
	defaultResp.Request = defaultReq
	if err := proxy.ModifyResponse(defaultResp); err != nil {
		t.Fatalf("default ModifyResponse error: %v", err)
	}
	defaultBody, err := io.ReadAll(defaultResp.Body)
	if err != nil {
		t.Fatalf("read default response: %v", err)
	}
	if threads := threadListItemsForTest(readNeoJSON(bytes.NewReader(defaultBody))); len(threads) != 0 {
		t.Fatalf("default thread list retained archived upstream copy: %#v", threads)
	}

	req := httptest.NewRequest(http.MethodPost, "http://proxy.local/api/internal?listThreads", strings.NewReader(`{"method":"listThreads","params":{"includeArchived":true,"limit":20}}`))
	m.attachNeoLocalThreadListAugmenter(req)
	proxy.Director(req)
	resp := mkResp(http.StatusOK, http.Header{"Content-Type": []string{"application/json"}}, []byte(`{"ok":true,"result":{"threads":[]}}`))
	resp.Request = req

	if err := proxy.ModifyResponse(resp); err != nil {
		t.Fatalf("ModifyResponse error: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	threads := threadListItemsForTest(readNeoJSON(bytes.NewReader(body)))
	if len(threads) != 1 || ampThreadListItemID(threads[0]) != threadID || !boolValue(threads[0]["archived"]) {
		t.Fatalf("archived-inclusive thread list = %#v", threads)
	}
}

func TestModifyResponse_RemovesStaleUpstreamCopyOfExcludedLocalThread(t *testing.T) {
	threadID := "T-019f70b9-5c65-73a7-8629-3b394bd51d09"
	proxy, err := createReverseProxy("http://example.com", NewStaticSecretSource("k"))
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "http://proxy.local/api/internal?listThreads", nil)
	req = req.WithContext(context.WithValue(req.Context(), ampProxyThreadListAugmenterContextKey{}, ampProxyThreadListAugmenter{
		excludedLabelNames: map[string]bool{"review": true},
		load: func(int) []any {
			return []any{map[string]any{"id": threadID, "messageCount": 1, "labels": []any{"review"}}}
		},
	}))
	resp := mkResp(http.StatusOK, http.Header{"Content-Type": []string{"application/json"}}, []byte(`{"ok":true,"result":{"threads":[{"thread":{"data":{"id":"`+threadID+`","title":"stale upstream","labels":[]}}}]}}`))
	resp.Request = req

	if err := proxy.ModifyResponse(resp); err != nil {
		t.Fatalf("ModifyResponse error: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	var decoded any
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("response JSON error: %v; body=%s", err, body)
	}
	if threads := threadListItemsForTest(decoded); len(threads) != 0 {
		t.Fatalf("excluded local thread survived as upstream duplicate: %#v", threads)
	}
}

func TestNormalizeAmpThreadListResponseUsesSelectedLoaderWithoutFullScan(t *testing.T) {
	threadID := "T-019f70b9-5c65-73a7-8629-3b394bd51d19"
	fullLoadCalls := 0
	selectedLoadCalls := 0
	req := httptest.NewRequest(http.MethodPost, "http://proxy.local/api/internal?listThreads", nil)
	req = req.WithContext(context.WithValue(req.Context(), ampProxyThreadListAugmenterContextKey{}, ampProxyThreadListAugmenter{
		threadIDs: map[string]bool{threadID: true},
		load: func(int) []any {
			fullLoadCalls++
			return nil
		},
		selectedLoad: func(threadIDs map[string]bool) []any {
			selectedLoadCalls++
			if !threadIDs[threadID] {
				t.Fatalf("selected thread IDs = %#v", threadIDs)
			}
			return []any{map[string]any{"id": threadID, "messageCount": 1}}
		},
	}))
	resp := mkResp(http.StatusOK, http.Header{"Content-Type": []string{"application/json"}}, []byte(`{"ok":true,"result":{"threads":[]}}`))
	resp.Request = req

	if normalized := normalizeAmpThreadListResponse(resp, []byte(`{"ok":true,"result":{"threads":[]}}`)); normalized == nil {
		t.Fatal("selected thread was not merged")
	}
	if fullLoadCalls != 0 || selectedLoadCalls != 1 {
		t.Fatalf("loader calls = full:%d selected:%d, want 0/1", fullLoadCalls, selectedLoadCalls)
	}
}

func TestAttachNeoLocalThreadListAugmenterHydratesSelectedLocalThread(t *testing.T) {
	enabled := true
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled}}})
	rt.threadDir = t.TempDir()
	threadID := "T-019f70b9-5c65-73a7-8629-3b394bd51d20"
	if _, err := writeNeoLocalThreadFileInDir(rt.threadDir, threadID, map[string]any{
		"id":    threadID,
		"title": "Selected local thread",
		"meta":  map[string]any{"cliProxyAPILocalNeo": true},
		"messages": []any{map[string]any{
			"messageId": "M-selected-local",
			"role":      "user",
			"content":   []any{map[string]any{"type": "text", "text": "persist locally"}},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "http://proxy.local/api/internal?listThreads", strings.NewReader(`{"method":"listThreads","params":{"limit":20,"threadIDs":["`+threadID+`"]}}`))
	m := &AmpModule{neoRuntime: rt}
	m.attachNeoLocalThreadListAugmenter(req)
	augmenter, ok := req.Context().Value(ampProxyThreadListAugmenterContextKey{}).(ampProxyThreadListAugmenter)
	if !ok || augmenter.load == nil || augmenter.selectedLoad == nil {
		t.Fatalf("selected augmenter = %#v", augmenter)
	}
	resp := mkResp(http.StatusOK, http.Header{"Content-Type": []string{"application/json"}}, []byte(`{"ok":true,"result":{"threads":[]}}`))
	resp.Request = req
	normalized := normalizeAmpThreadListResponse(resp, []byte(`{"ok":true,"result":{"threads":[]}}`))
	if normalized == nil {
		t.Fatal("selected local thread was not merged")
	}
	threads := threadListItemsForTest(readNeoJSON(bytes.NewReader(normalized)))
	if len(threads) != 1 || ampThreadListItemID(threads[0]) != threadID {
		t.Fatalf("selected local threads = %#v", threads)
	}
}

func TestNormalizeAmpThreadListResponseBoundsFullLoadAndReconcilesUpstreamIDs(t *testing.T) {
	localThreadID := "T-019f70b9-5c65-73a7-8629-3b394bd51d33"
	archivedThreadID := "T-019f70b9-5c65-73a7-8629-3b394bd51d34"
	fullLoadLimit := 0
	selectedLoadCalls := 0
	req := httptest.NewRequest(http.MethodPost, "http://proxy.local/api/internal?listThreads", nil)
	req = req.WithContext(context.WithValue(req.Context(), ampProxyThreadListAugmenterContextKey{}, ampProxyThreadListAugmenter{
		limit: 20,
		load: func(limit int) []any {
			fullLoadLimit = limit
			return []any{map[string]any{"id": localThreadID, "threadId": localThreadID, "title": "Local", "messageCount": 1}}
		},
		selectedLoad: func(threadIDs map[string]bool) []any {
			selectedLoadCalls++
			if len(threadIDs) != 1 || !threadIDs[archivedThreadID] {
				t.Fatalf("upstream reconciliation IDs = %#v", threadIDs)
			}
			return []any{map[string]any{"id": archivedThreadID, "threadId": archivedThreadID, "title": "Archived", "messageCount": 1, "archived": true}}
		},
	}))
	resp := mkResp(http.StatusOK, http.Header{"Content-Type": []string{"application/json"}}, []byte(`{"ok":true,"result":{"threads":[{"id":"`+archivedThreadID+`","threadId":"`+archivedThreadID+`","title":"stale upstream"}]}}`))
	resp.Request = req

	got := normalizeAmpThreadListResponse(resp, []byte(`{"ok":true,"result":{"threads":[{"id":"`+archivedThreadID+`","threadId":"`+archivedThreadID+`","title":"stale upstream"}]}}`))
	if fullLoadLimit != 20 || selectedLoadCalls != 1 {
		t.Fatalf("loader calls = limit:%d selected:%d, want 20/1", fullLoadLimit, selectedLoadCalls)
	}
	threads := threadListItemsForTest(readNeoJSON(bytes.NewReader(got)))
	if len(threads) != 1 || ampThreadListItemID(threads[0]) != localThreadID {
		t.Fatalf("threads = %#v, want only local visible thread", threads)
	}
}

func TestModifyResponse_RemovesStaleUpstreamCopyOfEmptyLocalThread(t *testing.T) {
	threadID := "T-019f70b9-5c65-73a7-8629-3b394bd51d10"
	proxy, err := createReverseProxy("http://example.com", NewStaticSecretSource("k"))
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "http://proxy.local/api/internal?listThreads", nil)
	req = req.WithContext(context.WithValue(req.Context(), ampProxyThreadListAugmenterContextKey{}, ampProxyThreadListAugmenter{
		load: func(int) []any {
			return []any{map[string]any{"id": threadID, "messageCount": 0}}
		},
	}))
	resp := mkResp(http.StatusOK, http.Header{"Content-Type": []string{"application/json"}}, []byte(`{"ok":true,"result":{"threads":[{"id":"`+threadID+`","title":"stale upstream"}]}}`))
	resp.Request = req

	if err := proxy.ModifyResponse(resp); err != nil {
		t.Fatalf("ModifyResponse error: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if threads := threadListItemsForTest(readNeoJSON(bytes.NewReader(body))); len(threads) != 0 {
		t.Fatalf("empty local thread survived as upstream duplicate: %#v", threads)
	}
}

func TestMergeAmpThreadListItemsHonorsWindow(t *testing.T) {
	remote1 := "T-019f70b9-5c65-73a7-8629-3b394bd51d11"
	remote2 := "T-019f70b9-5c65-73a7-8629-3b394bd51d12"
	local := "T-019f70b9-5c65-73a7-8629-3b394bd51d13"
	merged, changed := mergeAmpThreadListItems([]any{
		map[string]any{"id": remote1, "lastUserMessageAt": "2026-07-16T00:00:00Z"},
		map[string]any{"id": remote2, "lastUserMessageAt": "2026-07-15T00:00:00Z"},
	}, []any{
		map[string]any{"id": local, "lastUserMessageAt": "2026-07-17T00:00:00Z"},
	}, 0, 2)
	if !changed || len(merged) != 2 {
		t.Fatalf("merged = %#v changed=%v, want first two rows", merged, changed)
	}
	for index, want := range []string{local, remote1} {
		if got := ampThreadListItemID(mapValue(merged[index])); got != want {
			t.Fatalf("merged[%d] = %q, want %q", index, got, want)
		}
	}
	secondPage, changed := mergeAmpThreadListItems([]any{
		map[string]any{"id": remote1, "lastUserMessageAt": "2026-07-16T00:00:00Z"},
		map[string]any{"id": remote2, "lastUserMessageAt": "2026-07-15T00:00:00Z"},
	}, []any{
		map[string]any{"id": local, "lastUserMessageAt": "2026-07-17T00:00:00Z"},
	}, 2, 2)
	if !changed || len(secondPage) != 1 || ampThreadListItemID(mapValue(secondPage[0])) != remote2 {
		t.Fatalf("second page = %#v changed=%v, want displaced upstream row", secondPage, changed)
	}
}

func TestMergeAmpThreadListItemsPreservesUpstreamRelationships(t *testing.T) {
	threadID := "T-019f70b9-5c65-73a7-8629-3b394bd51d21"
	parentID := "T-019f70b9-5c65-73a7-8629-3b394bd51d22"
	merged, changed := mergeAmpThreadListItems([]any{
		map[string]any{
			"id":                threadID,
			"lastUserMessageAt": "2026-07-16T00:00:00Z",
			"relationships":     []any{map[string]any{"threadID": parentID}},
		},
	}, []any{
		map[string]any{"id": threadID, "lastUserMessageAt": "2026-07-17T00:00:00Z", "relationships": []any{}},
	}, 0, 20)
	if !changed || len(merged) != 1 {
		t.Fatalf("merged = %#v changed=%v", merged, changed)
	}
	relationships := arrayValue(mapValue(merged[0])["relationships"])
	if len(relationships) != 1 || stringValue(mapValue(relationships[0])["threadID"]) != parentID {
		t.Fatalf("relationships = %#v, want upstream relationship", relationships)
	}
}

func TestMergeAmpThreadListItemsPreservesAuthoritativeUpstreamMetadata(t *testing.T) {
	threadID := "T-019f70b9-5c65-73a7-8629-3b394bd51d26"
	merged, changed := mergeAmpThreadListItems([]any{
		map[string]any{
			"id":                threadID,
			"lastUserMessageAt": "2026-07-18T00:00:00Z",
			"title":             "Cloud title",
			"archived":          true,
			"pinned":            true,
			"creatorUserID":     "aikins01",
			"ownerUserId":       "aikins01",
			"creator":           map[string]any{"id": "aikins01", "name": "Aikins"},
			"meta":              map[string]any{"repositoryURL": "https://github.com/aikins01/cloud.git"},
		},
	}, []any{
		map[string]any{
			"id":                threadID,
			"lastUserMessageAt": "2026-07-19T00:00:00Z",
			"title":             "Untitled",
			"archived":          false,
			"pinned":            false,
			"creatorUserID":     "local-user",
			"ownerUserId":       "local-user",
			"creator":           map[string]any{"id": "local-user", "name": "Local Amp"},
			"meta":              map[string]any{"repositoryURL": "file:///tmp/local", "ampcodeLocalRuntime": true},
		},
	}, 0, 20)
	if !changed || len(merged) != 1 {
		t.Fatalf("merged = %#v changed=%v", merged, changed)
	}
	thread, _ := ampThreadListItemThread(mapValue(merged[0]))
	if stringValue(thread["title"]) != "Cloud title" || stringValue(thread["creatorUserID"]) != "aikins01" || stringValue(thread["ownerUserId"]) != "aikins01" {
		t.Fatalf("cloud identity was replaced: %#v", thread)
	}
	if !boolValue(thread["archived"]) || !boolValue(thread["pinned"]) {
		t.Fatalf("cloud flags were replaced: %#v", thread)
	}
	meta := mapValue(thread["meta"])
	if stringValue(meta["repositoryURL"]) != "https://github.com/aikins01/cloud.git" || !boolValue(meta["ampcodeLocalRuntime"]) {
		t.Fatalf("merged metadata = %#v", meta)
	}
}

func TestMergeAmpThreadListItemsAppliesNewerLocalTitleAndExplicitPinOverride(t *testing.T) {
	threadID := "T-019f70b9-5c65-73a7-8629-3b394bd51d27"
	for _, test := range []struct {
		name        string
		cloudPinned bool
		localPinned bool
	}{
		{name: "pin", cloudPinned: false, localPinned: true},
		{name: "unpin", cloudPinned: true, localPinned: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			merged, changed := mergeAmpThreadListItems([]any{
				map[string]any{
					"id":                threadID,
					"lastUserMessageAt": "2026-07-18T00:00:00Z",
					"title":             "Stale cloud title",
					"pinned":            test.cloudPinned,
				},
			}, []any{
				map[string]any{
					"id":                      threadID,
					"lastUserMessageAt":       "2026-07-19T00:00:00Z",
					"title":                   "Current local title",
					"pinned":                  test.localPinned,
					neoLocalPinnedOverrideKey: test.localPinned,
				},
			}, 0, 20)
			if !changed || len(merged) != 1 {
				t.Fatalf("merged = %#v changed=%v", merged, changed)
			}
			thread, _ := ampThreadListItemThread(mapValue(merged[0]))
			if stringValue(thread["title"]) != "Current local title" || boolValue(thread["pinned"]) != test.localPinned {
				t.Fatalf("merged local authority = %#v", thread)
			}
			if _, exists := thread[neoLocalPinnedOverrideKey]; exists {
				t.Fatalf("local pin override leaked into response: %#v", thread)
			}
		})
	}
}

func TestMergeAmpThreadListItemsPreservesWrappedRowShape(t *testing.T) {
	existingID := "T-019f70b9-5c65-73a7-8629-3b394bd51d23"
	localID := "T-019f70b9-5c65-73a7-8629-3b394bd51d24"
	parentID := "T-019f70b9-5c65-73a7-8629-3b394bd51d25"
	merged, changed := mergeAmpThreadListItems([]any{
		map[string]any{"thread": map[string]any{"data": map[string]any{
			"id": existingID, "title": "remote", "lastUserMessageAt": "2026-07-16T00:00:00Z",
			"relationships": []any{map[string]any{"threadID": parentID}},
		}}},
	}, []any{
		map[string]any{"id": existingID, "title": "local update", "lastUserMessageAt": "2026-07-17T00:00:00Z"},
		map[string]any{"id": localID, "title": "local new", "lastUserMessageAt": "2026-07-18T00:00:00Z"},
	}, 0, 20)
	if !changed || len(merged) != 2 {
		t.Fatalf("merged = %#v changed=%v", merged, changed)
	}
	byID := map[string]map[string]any{}
	for _, raw := range merged {
		item := mapValue(raw)
		thread, shape := ampThreadListItemThread(item)
		if !reflect.DeepEqual(shape, []string{"thread", "data"}) {
			t.Fatalf("row shape = %#v, want wrapped thread/data", shape)
		}
		byID[ampThreadListItemID(item)] = thread
	}
	if stringValue(byID[existingID]["title"]) != "local update" {
		t.Fatalf("updated wrapped thread = %#v", byID[existingID])
	}
	relationships := arrayValue(byID[existingID]["relationships"])
	if len(relationships) != 1 || stringValue(mapValue(relationships[0])["threadID"]) != parentID {
		t.Fatalf("wrapped relationships = %#v", relationships)
	}
	if stringValue(byID[localID]["title"]) != "local new" {
		t.Fatalf("new wrapped thread = %#v", byID[localID])
	}
}

func TestNormalizeAmpThreadListResponseFiltersBeforeLocalLimit(t *testing.T) {
	targetID := "T-019f70b9-5c65-73a7-8629-3b394bd51d31"
	unrelatedID := "T-019f70b9-5c65-73a7-8629-3b394bd51d32"
	req := httptest.NewRequest(http.MethodPost, "http://proxy.local/api/internal?listThreads", nil)
	augmenter := ampProxyThreadListAugmenter{
		limit:     1,
		threadIDs: map[string]bool{targetID: true},
		load: func(limit int) []any {
			if limit != 0 {
				t.Fatalf("local summary load limit = %d, want 0 before filtering", limit)
			}
			return []any{map[string]any{"id": unrelatedID, "messageCount": 1}}
		},
		selectedLoad: func(threadIDs map[string]bool) []any {
			if !threadIDs[targetID] {
				t.Fatalf("selected thread IDs = %#v", threadIDs)
			}
			return []any{map[string]any{"id": targetID, "messageCount": 1}}
		},
	}
	req = req.WithContext(context.WithValue(req.Context(), ampProxyThreadListAugmenterContextKey{}, augmenter))
	resp := mkResp(http.StatusOK, http.Header{"Content-Type": []string{"application/json"}}, []byte(`{"ok":true,"result":{"threads":[]}}`))
	resp.Request = req
	got := normalizeAmpThreadListResponse(resp, []byte(`{"ok":true,"result":{"threads":[]}}`))
	threads := threadListItemsForTest(readNeoJSON(bytes.NewReader(got)))
	if len(threads) != 1 || ampThreadListItemID(threads[0]) != targetID {
		t.Fatalf("threads = %#v, want selected local thread", threads)
	}
}

func TestAmpProxyThreadListAugmenterRequestsGzipEncoding(t *testing.T) {
	proxy, err := createReverseProxy("http://example.com", NewStaticSecretSource("k"))
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "http://proxy.local/api/internal?listThreads", nil)
	req.Header.Set("Accept-Encoding", "gzip, br")
	req = req.WithContext(context.WithValue(req.Context(), ampProxyThreadListAugmenterContextKey{}, ampProxyThreadListAugmenter{}))
	proxy.Director(req)
	if got := req.Header.Get("Accept-Encoding"); got != "gzip" {
		t.Fatalf("Accept-Encoding = %q, want gzip", got)
	}
}

func TestAmpThreadListRequestAugmenterHonorsSelectors(t *testing.T) {
	queryPageReq := httptest.NewRequest(http.MethodPost, "http://proxy.local/api/internal?method=listThreads&offset=20&limit=10", nil)
	queryPageAugmenter, ok := ampThreadListRequestAugmenter(queryPageReq)
	if !ok || !rewriteAmpThreadListRequestWindow(queryPageReq, queryPageAugmenter) {
		t.Fatalf("query-only page augmenter = %#v ok=%v", queryPageAugmenter, ok)
	}
	if got := queryPageReq.URL.Query(); got.Get("offset") != "0" || got.Get("limit") != "30" {
		t.Fatalf("rewritten query-only page = %q", queryPageReq.URL.RawQuery)
	}

	pageReq := httptest.NewRequest(http.MethodPost, "http://proxy.local/api/internal?listThreads", strings.NewReader(`{"method":"listThreads","params":{"offset":20,"limit":10}}`))
	pageAugmenter, ok := ampThreadListRequestAugmenter(pageReq)
	if !ok || pageAugmenter.offset != 20 || pageAugmenter.limit != 10 {
		t.Fatalf("page augmenter = %#v ok=%v", pageAugmenter, ok)
	}
	if !rewriteAmpThreadListRequestWindow(pageReq, pageAugmenter) {
		t.Fatal("page request was not rewritten")
	}
	pageBody, err := readAndRestoreNeoJSONBody(pageReq)
	if err != nil {
		t.Fatalf("page body: %v", err)
	}
	pageParams := mapValue(pageBody["params"])
	if numberFrom(pageParams["offset"]) != 0 || numberFrom(pageParams["limit"]) != 30 {
		t.Fatalf("rewritten page params = %#v", pageParams)
	}

	installationReq := httptest.NewRequest(http.MethodPost, "http://proxy.local/api/internal?listThreads&installationID=install-other", strings.NewReader(`{"method":"listThreads","params":{"limit":20}}`))
	if _, ok = ampThreadListRequestAugmenter(installationReq); ok {
		t.Fatal("query installation ID unexpectedly enabled local augmentation")
	}

	req := httptest.NewRequest(http.MethodPost, "http://proxy.local/api/internal?listThreads", strings.NewReader(`{"method":"listThreads","params":{"limit":50,"includeEmpty":true,"threadIDs":["T-1"],"excludeLabelNames":["review"]}}`))
	augmenter, ok := ampThreadListRequestAugmenter(req)
	if !ok || augmenter.limit != 50 || !augmenter.includeEmpty || !augmenter.threadIDs["T-1"] || !augmenter.excludedLabelNames["review"] {
		t.Fatalf("augmenter = %#v ok=%v", augmenter, ok)
	}
	archivedReq := httptest.NewRequest(http.MethodPost, "http://proxy.local/api/internal?listThreads", strings.NewReader(`{"method":"listThreads","params":{"includeArchived":true,"limit":20}}`))
	archivedAugmenter, ok := ampThreadListRequestAugmenter(archivedReq)
	if !ok || !archivedAugmenter.includeArchived {
		t.Fatalf("archived augmenter = %#v ok=%v", archivedAugmenter, ok)
	}
	filtered := filterAmpThreadListLocalThreads([]any{
		map[string]any{"id": "T-1", "messageCount": 1, "labels": []any{}},
		map[string]any{"id": "T-2", "messageCount": 1, "labels": []any{}},
	}, augmenter)
	if len(filtered) != 1 || ampThreadListItemID(mapValue(filtered[0])) != "T-1" {
		t.Fatalf("filtered threads = %#v", filtered)
	}
	filtered = filterAmpThreadListLocalThreads([]any{
		map[string]any{"id": "T-1", "messageCount": 1, "labels": []any{"review"}},
		map[string]any{"id": "T-2", "messageCount": 0, "labels": []any{}},
		map[string]any{"id": "T-3", "messageCount": 1, "labels": []any{}},
	}, ampProxyThreadListAugmenter{excludedLabelNames: map[string]bool{"review": true}})
	if len(filtered) != 1 || ampThreadListItemID(mapValue(filtered[0])) != "T-3" {
		t.Fatalf("label/empty filtered threads = %#v", filtered)
	}
	filtered = filterAmpThreadListLocalThreads([]any{
		map[string]any{"id": "T-1", "messageCount": 1, "archived": true},
		map[string]any{"id": "T-2", "messageCount": 1},
	}, archivedAugmenter)
	if len(filtered) != 2 {
		t.Fatalf("archived-inclusive filtered threads = %#v", filtered)
	}
}

func TestAmpThreadListPaginationRefillsRowsExcludedByLocalState(t *testing.T) {
	excludedID := "T-019f70b9-5c65-73a7-8629-3b394bd51d40"
	visibleIDs := []string{
		"T-019f70b9-5c65-73a7-8629-3b394bd51d41",
		"T-019f70b9-5c65-73a7-8629-3b394bd51d42",
	}
	upstreamThreads := []any{
		map[string]any{"id": excludedID, "title": "archived upstream duplicate", "messageCount": 1},
		map[string]any{"id": visibleIDs[0], "title": "visible first", "messageCount": 1},
		map[string]any{"id": visibleIDs[1], "title": "visible second", "messageCount": 1},
	}
	localThreads := []any{map[string]any{"id": excludedID, "archived": true, "messageCount": 1}}

	for page, wantID := range visibleIDs {
		offset := page
		req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("http://proxy.local/api/internal?method=listThreads&offset=%d&limit=1", offset), nil)
		augmenter, ok := ampThreadListRequestAugmenter(req)
		if !ok {
			t.Fatalf("page %d request did not produce an augmenter", page)
		}
		augmenter.upstreamOverfetch = len(localThreads)
		augmenter.load = func(int) []any { return localThreads }
		if augmenter.upstreamRebased = rewriteAmpThreadListRequestWindow(req, augmenter); !augmenter.upstreamRebased {
			t.Fatalf("page %d request was not rewritten", page)
		}
		if got, want := req.URL.Query().Get("limit"), strconv.Itoa(offset+2); got != want {
			t.Fatalf("page %d upstream limit = %q, want %q", page, got, want)
		}
		req = req.WithContext(context.WithValue(req.Context(), ampProxyThreadListAugmenterContextKey{}, augmenter))
		payload := map[string]any{"ok": true, "result": map[string]any{"threads": upstreamThreads[:offset+2]}}
		body, err := json.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}
		resp := mkResp(http.StatusOK, http.Header{"Content-Type": []string{"application/json"}}, body)
		resp.Request = req
		normalized := normalizeAmpThreadListResponse(resp, body)
		threads := threadListItemsForTest(readNeoJSON(bytes.NewReader(normalized)))
		if len(threads) != 1 || ampThreadListItemID(threads[0]) != wantID {
			t.Fatalf("page %d threads = %#v, want %s", page, threads, wantID)
		}
	}
}

func TestAmpProxyInternalRPCMethodUsesQueryBeforeBody(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "http://proxy.local/api/internal?uploadThread", nil)
	body := &readCountingBody{}
	req.Body = body

	if got := ampProxyInternalRPCMethod(req); got != "uploadThread" {
		t.Fatalf("method = %q, want uploadThread", got)
	}
	if body.reads != 0 {
		t.Fatalf("body reads = %d, want 0", body.reads)
	}
}

func TestModifyResponse_DoesNotNormalizeThreadSearchRelationships(t *testing.T) {
	proxy, err := createReverseProxy("http://example.com", NewStaticSecretSource("k"))
	if err != nil {
		t.Fatal(err)
	}

	body := []byte(`{"threads":[{"id":"T-1","title":"missing"}]}`)
	resp := mkResp(http.StatusOK, http.Header{"Content-Type": []string{"application/json"}}, body)
	resp.Request = httptest.NewRequest(http.MethodGet, "http://proxy.local/api/threads/find?q=needle", nil)

	if err := proxy.ModifyResponse(resp); err != nil {
		t.Fatalf("ModifyResponse error: %v", err)
	}
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll error: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("thread search body changed:\nwant: %s\ngot:  %s", body, got)
	}
}

func TestModifyResponse_NormalizesGzippedThreadListRelationships(t *testing.T) {
	proxy, err := createReverseProxy("http://example.com", NewStaticSecretSource("k"))
	if err != nil {
		t.Fatal(err)
	}

	body := []byte(`{"ok":true,"result":{"threads":[{"id":"T-1","title":"missing"}]}}`)
	resp := mkResp(http.StatusOK, http.Header{"Content-Type": []string{"application/json"}}, gzipBytes(body))
	resp.Request = httptest.NewRequest(http.MethodPost, "http://proxy.local/api/internal?listThreads", nil)

	if err := proxy.ModifyResponse(resp); err != nil {
		t.Fatalf("ModifyResponse error: %v", err)
	}
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll error: %v", err)
	}
	var decoded any
	if err := json.Unmarshal(got, &decoded); err != nil {
		t.Fatalf("response JSON error: %v; body=%s", err, got)
	}
	assertThreadRelationshipsForTest(t, decoded)
}

func TestModifyResponse_NormalizesContentEncodedGzipThreadList(t *testing.T) {
	proxy, err := createReverseProxy("http://example.com", NewStaticSecretSource("k"))
	if err != nil {
		t.Fatal(err)
	}

	body := []byte(`{"ok":true,"result":{"threads":[{"id":"T-1","title":"missing"}]}}`)
	resp := mkResp(http.StatusOK, http.Header{
		"Content-Type":     []string{"application/json"},
		"Content-Encoding": []string{"gzip"},
	}, gzipBytes(body))
	resp.Request = httptest.NewRequest(http.MethodPost, "http://proxy.local/api/internal?listThreads", nil)

	if err := proxy.ModifyResponse(resp); err != nil {
		t.Fatalf("ModifyResponse error: %v", err)
	}
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Header.Get("Content-Encoding") != "" {
		t.Fatalf("Content-Encoding = %q, want decompressed response", resp.Header.Get("Content-Encoding"))
	}
	var decoded any
	if err := json.Unmarshal(got, &decoded); err != nil {
		t.Fatalf("response JSON error: %v; body=%s", err, got)
	}
	assertThreadRelationshipsForTest(t, decoded)
}

func assertThreadRelationshipsForTest(t *testing.T, value any) {
	t.Helper()
	threads := threadListItemsForTest(value)
	if len(threads) == 0 {
		t.Fatalf("no thread list items in %#v", value)
	}
	for _, thread := range threads {
		if _, ok := thread["relationships"].([]any); !ok {
			t.Fatalf("thread relationships = %#v, want array in %#v", thread["relationships"], thread)
		}
	}
}

func threadListItemsForTest(value any) []map[string]any {
	switch typed := value.(type) {
	case []any:
		return mapThreadItemsForTest(typed)
	case map[string]any:
		for _, key := range []string{"threads", "items", "data"} {
			if items := mapThreadItemsForTest(arrayValue(typed[key])); len(items) > 0 {
				return items
			}
		}
		if result := typed["result"]; result != nil {
			return threadListItemsForTest(result)
		}
	}
	return nil
}

func mapThreadItemsForTest(items []any) []map[string]any {
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		if thread, ok := item.(map[string]any); ok {
			out = append(out, thread)
		}
	}
	return out
}

type readCountingBody struct {
	reads int
}

func (b *readCountingBody) Read(_ []byte) (int, error) {
	b.reads++
	return 0, fmt.Errorf("unexpected body read")
}

func (b *readCountingBody) Close() error {
	return nil
}

func TestModifyResponse_UpdatesContentLengthHeader(t *testing.T) {
	proxy, err := createReverseProxy("http://example.com", NewStaticSecretSource("k"))
	if err != nil {
		t.Fatal(err)
	}

	goodJSON := []byte(`{"message":"test response"}`)
	gzipped := gzipBytes(goodJSON)

	// Simulate upstream response with gzip body AND Content-Length header
	// (this is the scenario the bot flagged - stale Content-Length after decompression)
	resp := mkResp(200, http.Header{
		"Content-Length": []string{fmt.Sprintf("%d", len(gzipped))}, // Compressed size
	}, gzipped)

	if err := proxy.ModifyResponse(resp); err != nil {
		t.Fatalf("ModifyResponse error: %v", err)
	}

	// Verify body is decompressed
	got, _ := io.ReadAll(resp.Body)
	if !bytes.Equal(got, goodJSON) {
		t.Fatalf("body should be decompressed, got: %q, want: %q", got, goodJSON)
	}

	// Verify Content-Length header is updated to decompressed size
	wantCL := fmt.Sprintf("%d", len(goodJSON))
	gotCL := resp.Header.Get("Content-Length")
	if gotCL != wantCL {
		t.Fatalf("Content-Length header mismatch: want %q (decompressed), got %q", wantCL, gotCL)
	}

	// Verify struct field also matches
	if resp.ContentLength != int64(len(goodJSON)) {
		t.Fatalf("resp.ContentLength mismatch: want %d, got %d", len(goodJSON), resp.ContentLength)
	}
}

func TestModifyResponse_SkipsStreamingResponses(t *testing.T) {
	proxy, err := createReverseProxy("http://example.com", NewStaticSecretSource("k"))
	if err != nil {
		t.Fatal(err)
	}

	goodJSON := []byte(`{"ok":true}`)
	gzipped := gzipBytes(goodJSON)

	t.Run("sse_skips_decompression", func(t *testing.T) {
		resp := mkResp(200, http.Header{"Content-Type": []string{"text/event-stream"}}, gzipped)
		if err := proxy.ModifyResponse(resp); err != nil {
			t.Fatalf("ModifyResponse error: %v", err)
		}
		// SSE should NOT be decompressed
		got, _ := io.ReadAll(resp.Body)
		if !bytes.Equal(got, gzipped) {
			t.Fatal("SSE response should not be decompressed")
		}
	})
}

func TestModifyResponse_DecompressesChunkedJSON(t *testing.T) {
	proxy, err := createReverseProxy("http://example.com", NewStaticSecretSource("k"))
	if err != nil {
		t.Fatal(err)
	}

	goodJSON := []byte(`{"ok":true}`)
	gzipped := gzipBytes(goodJSON)

	t.Run("chunked_json_decompresses", func(t *testing.T) {
		// Chunked JSON responses (like thread APIs) should be decompressed
		resp := mkResp(200, http.Header{"Transfer-Encoding": []string{"chunked"}}, gzipped)
		if err := proxy.ModifyResponse(resp); err != nil {
			t.Fatalf("ModifyResponse error: %v", err)
		}
		// Should decompress because it's not SSE
		got, _ := io.ReadAll(resp.Body)
		if !bytes.Equal(got, goodJSON) {
			t.Fatalf("chunked JSON should be decompressed, got: %q, want: %q", got, goodJSON)
		}
	})
}

func TestReverseProxy_InjectsHeaders(t *testing.T) {
	gotHeaders := make(chan http.Header, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeaders <- r.Header.Clone()
		w.WriteHeader(200)
		w.Write([]byte(`ok`))
	}))
	defer upstream.Close()

	proxy, err := createReverseProxy(upstream.URL, NewStaticSecretSource("secret"))
	if err != nil {
		t.Fatal(err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxy.ServeHTTP(w, r)
	}))
	defer srv.Close()

	res, err := http.Get(srv.URL + "/test")
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()

	hdr := <-gotHeaders
	if hdr.Get("X-Api-Key") != "secret" {
		t.Fatalf("X-Api-Key missing or wrong, got: %q", hdr.Get("X-Api-Key"))
	}
	if hdr.Get("Authorization") != "Bearer secret" {
		t.Fatalf("Authorization missing or wrong, got: %q", hdr.Get("Authorization"))
	}
}

func TestReverseProxy_PreservesActorEngineBasicAuth(t *testing.T) {
	gotHeaders := make(chan http.Header, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeaders <- r.Header.Clone()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`ok`))
	}))
	defer upstream.Close()

	proxy, err := createReverseProxy(upstream.URL, NewStaticSecretSource("secret"))
	if err != nil {
		t.Fatal(err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxy.ServeHTTP(w, r)
	}))
	defer srv.Close()

	req, err := http.NewRequest(http.MethodGet, srv.URL+"/actors/metadata", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Basic actor-token")
	req.Header.Set("X-Api-Key", "local-client-key")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()

	hdr := <-gotHeaders
	if hdr.Get("Authorization") != "Basic actor-token" {
		t.Fatalf("Authorization = %q, want actor Basic auth", hdr.Get("Authorization"))
	}
	if hdr.Get("X-Api-Key") != "" {
		t.Fatalf("X-Api-Key = %q, want stripped for actor engine", hdr.Get("X-Api-Key"))
	}
}

func TestReverseProxy_PreservesActorEngineBearerAuth(t *testing.T) {
	gotHeaders := make(chan http.Header, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeaders <- r.Header.Clone()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`ok`))
	}))
	defer upstream.Close()

	proxy, err := createReverseProxy(upstream.URL, NewStaticSecretSource("secret"))
	if err != nil {
		t.Fatal(err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxy.ServeHTTP(w, r)
	}))
	defer srv.Close()

	req, err := http.NewRequest(http.MethodGet, srv.URL+"/actors/metadata", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer actor-token")
	req.Header.Set("X-Api-Key", "local-client-key")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()

	hdr := <-gotHeaders
	if hdr.Get("Authorization") != "Bearer actor-token" {
		t.Fatalf("Authorization = %q, want actor Bearer auth", hdr.Get("Authorization"))
	}
	if hdr.Get("X-Api-Key") != "" {
		t.Fatalf("X-Api-Key = %q, want stripped for actor engine", hdr.Get("X-Api-Key"))
	}
}

func TestReverseProxy_DoesNotInjectAPIKeyForActorEngineWebsocketToken(t *testing.T) {
	gotHeaders := make(chan http.Header, 1)
	gotQuery := make(chan string, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeaders <- r.Header.Clone()
		gotQuery <- r.URL.RawQuery
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`ok`))
	}))
	defer upstream.Close()

	proxy, err := createReverseProxy(upstream.URL, NewStaticSecretSource("secret"))
	if err != nil {
		t.Fatal(err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxy.ServeHTTP(w, r)
	}))
	defer srv.Close()

	req, err := http.NewRequest(http.MethodGet, srv.URL+"/actors/gateway/threadActor/websocket/?rvt-token=actor-token&rvt-method=getOrCreate", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Sec-WebSocket-Protocol", "rivet, rivet_token.actor-token, rivet_encoding.json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()

	hdr := <-gotHeaders
	if hdr.Get("Authorization") != "" {
		t.Fatalf("Authorization = %q, want none for actor websocket token", hdr.Get("Authorization"))
	}
	if hdr.Get("X-Api-Key") != "" {
		t.Fatalf("X-Api-Key = %q, want stripped for actor websocket token", hdr.Get("X-Api-Key"))
	}
	if query := <-gotQuery; !strings.Contains(query, "rvt-token=actor-token") {
		t.Fatalf("query = %q, missing actor rvt-token", query)
	}
}

func TestReverseProxy_EmptySecret(t *testing.T) {
	gotHeaders := make(chan http.Header, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeaders <- r.Header.Clone()
		w.WriteHeader(200)
		w.Write([]byte(`ok`))
	}))
	defer upstream.Close()

	proxy, err := createReverseProxy(upstream.URL, NewStaticSecretSource(""))
	if err != nil {
		t.Fatal(err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxy.ServeHTTP(w, r)
	}))
	defer srv.Close()

	res, err := http.Get(srv.URL + "/test")
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()

	hdr := <-gotHeaders
	// Should NOT inject headers when secret is empty
	if hdr.Get("X-Api-Key") != "" {
		t.Fatalf("X-Api-Key should not be set, got: %q", hdr.Get("X-Api-Key"))
	}
	if authVal := hdr.Get("Authorization"); authVal != "" && authVal != "Bearer " {
		t.Fatalf("Authorization should not be set, got: %q", authVal)
	}
}

func TestReverseProxy_StripsClientCredentialsFromHeadersAndQuery(t *testing.T) {
	type captured struct {
		headers http.Header
		query   string
	}
	got := make(chan captured, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got <- captured{headers: r.Header.Clone(), query: r.URL.RawQuery}
		w.WriteHeader(200)
		w.Write([]byte(`ok`))
	}))
	defer upstream.Close()

	proxy, err := createReverseProxy(upstream.URL, NewStaticSecretSource("upstream"))
	if err != nil {
		t.Fatal(err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Simulate clientAPIKeyMiddleware injection (per-request)
		ctx := context.WithValue(r.Context(), clientAPIKeyContextKey{}, "client-key")
		proxy.ServeHTTP(w, r.WithContext(ctx))
	}))
	defer srv.Close()

	req, err := http.NewRequest(http.MethodGet, srv.URL+"/test?key=client-key&key=keep&auth_token=client-key&foo=bar", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer client-key")
	req.Header.Set("X-Api-Key", "client-key")
	req.Header.Set("X-Goog-Api-Key", "client-key")
	req.Header.Set(localNeoInferenceHeader, "1")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()

	c := <-got

	// These are client-provided credentials and must not reach the upstream.
	if v := c.headers.Get("X-Goog-Api-Key"); v != "" {
		t.Fatalf("X-Goog-Api-Key should be stripped, got: %q", v)
	}
	if v := c.headers.Get(localNeoInferenceHeader); v != "" {
		t.Fatalf("%s should be stripped, got: %q", localNeoInferenceHeader, v)
	}

	// We inject upstream Authorization/X-Api-Key, so the client auth must not survive.
	if v := c.headers.Get("Authorization"); v != "Bearer upstream" {
		t.Fatalf("Authorization should be upstream-injected, got: %q", v)
	}
	if v := c.headers.Get("X-Api-Key"); v != "upstream" {
		t.Fatalf("X-Api-Key should be upstream-injected, got: %q", v)
	}

	// Query-based credentials should be stripped only when they match the authenticated client key.
	// Should keep unrelated values and parameters.
	if strings.Contains(c.query, "auth_token=client-key") || strings.Contains(c.query, "key=client-key") {
		t.Fatalf("query credentials should be stripped, got raw query: %q", c.query)
	}
	if !strings.Contains(c.query, "key=keep") || !strings.Contains(c.query, "foo=bar") {
		t.Fatalf("expected query to keep non-credential params, got raw query: %q", c.query)
	}
}

func TestReverseProxy_InjectsMappedSecret_FromRequestContext(t *testing.T) {
	gotHeaders := make(chan http.Header, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeaders <- r.Header.Clone()
		w.WriteHeader(200)
		w.Write([]byte(`ok`))
	}))
	defer upstream.Close()

	defaultSource := NewStaticSecretSource("default")
	mapped := NewMappedSecretSource(defaultSource)
	mapped.UpdateMappings([]config.AmpUpstreamAPIKeyEntry{
		{
			UpstreamAPIKey: "u1",
			APIKeys:        []string{"k1"},
		},
	})

	proxy, err := createReverseProxy(upstream.URL, mapped)
	if err != nil {
		t.Fatal(err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Simulate clientAPIKeyMiddleware injection (per-request)
		ctx := context.WithValue(r.Context(), clientAPIKeyContextKey{}, "k1")
		proxy.ServeHTTP(w, r.WithContext(ctx))
	}))
	defer srv.Close()

	res, err := http.Get(srv.URL + "/test")
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()

	hdr := <-gotHeaders
	if hdr.Get("X-Api-Key") != "u1" {
		t.Fatalf("X-Api-Key missing or wrong, got: %q", hdr.Get("X-Api-Key"))
	}
	if hdr.Get("Authorization") != "Bearer u1" {
		t.Fatalf("Authorization missing or wrong, got: %q", hdr.Get("Authorization"))
	}
}

func TestReverseProxy_MappedSecret_FallsBackToDefault(t *testing.T) {
	gotHeaders := make(chan http.Header, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeaders <- r.Header.Clone()
		w.WriteHeader(200)
		w.Write([]byte(`ok`))
	}))
	defer upstream.Close()

	defaultSource := NewStaticSecretSource("default")
	mapped := NewMappedSecretSource(defaultSource)
	mapped.UpdateMappings([]config.AmpUpstreamAPIKeyEntry{
		{
			UpstreamAPIKey: "u1",
			APIKeys:        []string{"k1"},
		},
	})

	proxy, err := createReverseProxy(upstream.URL, mapped)
	if err != nil {
		t.Fatal(err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), clientAPIKeyContextKey{}, "k2")
		proxy.ServeHTTP(w, r.WithContext(ctx))
	}))
	defer srv.Close()

	res, err := http.Get(srv.URL + "/test")
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()

	hdr := <-gotHeaders
	if hdr.Get("X-Api-Key") != "default" {
		t.Fatalf("X-Api-Key fallback missing or wrong, got: %q", hdr.Get("X-Api-Key"))
	}
	if hdr.Get("Authorization") != "Bearer default" {
		t.Fatalf("Authorization fallback missing or wrong, got: %q", hdr.Get("Authorization"))
	}
}

func TestReverseProxy_ErrorHandler(t *testing.T) {
	// Point proxy to a non-routable address to trigger error
	proxy, err := createReverseProxy("http://127.0.0.1:1", NewStaticSecretSource(""))
	if err != nil {
		t.Fatal(err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxy.ServeHTTP(w, r)
	}))
	defer srv.Close()

	res, err := http.Get(srv.URL + "/any")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(res.Body)
	res.Body.Close()

	if res.StatusCode != http.StatusBadGateway {
		t.Fatalf("want 502, got %d", res.StatusCode)
	}
	if !bytes.Contains(body, []byte(`"amp_upstream_proxy_error"`)) {
		t.Fatalf("unexpected body: %s", body)
	}
	if ct := res.Header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content-type: want application/json, got %s", ct)
	}
}

func TestReverseProxy_ErrorHandler_ContextCanceled(t *testing.T) {
	// Test that context.Canceled errors return 499 without generic error response
	proxy, err := createReverseProxy("http://example.com", NewStaticSecretSource(""))
	if err != nil {
		t.Fatal(err)
	}

	// Create a canceled context to trigger the cancellation path
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	req := httptest.NewRequest(http.MethodGet, "/test", nil).WithContext(ctx)
	rr := httptest.NewRecorder()

	// Directly invoke the ErrorHandler with context.Canceled
	proxy.ErrorHandler(rr, req, context.Canceled)

	// Body should be empty for canceled requests (no JSON error response)
	body := rr.Body.Bytes()
	if len(body) > 0 {
		t.Fatalf("expected empty body for canceled context, got: %s", body)
	}
}

func TestReverseProxy_FullRoundTrip_Gzip(t *testing.T) {
	// Upstream returns gzipped JSON without Content-Encoding header
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write(gzipBytes([]byte(`{"upstream":"ok"}`)))
	}))
	defer upstream.Close()

	proxy, err := createReverseProxy(upstream.URL, NewStaticSecretSource("key"))
	if err != nil {
		t.Fatal(err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxy.ServeHTTP(w, r)
	}))
	defer srv.Close()

	res, err := http.Get(srv.URL + "/test")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(res.Body)
	res.Body.Close()

	expected := []byte(`{"upstream":"ok"}`)
	if !bytes.Equal(body, expected) {
		t.Fatalf("want decompressed JSON, got: %s", body)
	}
}

func TestReverseProxy_FullRoundTrip_PlainJSON(t *testing.T) {
	// Upstream returns plain JSON
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		w.Write([]byte(`{"plain":"json"}`))
	}))
	defer upstream.Close()

	proxy, err := createReverseProxy(upstream.URL, NewStaticSecretSource("key"))
	if err != nil {
		t.Fatal(err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxy.ServeHTTP(w, r)
	}))
	defer srv.Close()

	res, err := http.Get(srv.URL + "/test")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(res.Body)
	res.Body.Close()

	expected := []byte(`{"plain":"json"}`)
	if !bytes.Equal(body, expected) {
		t.Fatalf("want plain JSON unchanged, got: %s", body)
	}
}

func TestIsStreamingResponse(t *testing.T) {
	cases := []struct {
		name   string
		header http.Header
		want   bool
	}{
		{
			name:   "sse",
			header: http.Header{"Content-Type": []string{"text/event-stream"}},
			want:   true,
		},
		{
			name:   "chunked_not_streaming",
			header: http.Header{"Transfer-Encoding": []string{"chunked"}},
			want:   false, // Chunked is transport-level, not streaming
		},
		{
			name:   "normal_json",
			header: http.Header{"Content-Type": []string{"application/json"}},
			want:   false,
		},
		{
			name:   "empty",
			header: http.Header{},
			want:   false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := &http.Response{Header: tc.header}
			got := isStreamingResponse(resp)
			if got != tc.want {
				t.Fatalf("want %v, got %v", tc.want, got)
			}
		})
	}
}

func TestFilterBetaFeatures(t *testing.T) {
	tests := []struct {
		name            string
		header          string
		featureToRemove string
		expected        string
	}{
		{
			name:            "Remove context-1m from middle",
			header:          "fine-grained-tool-streaming-2025-05-14,context-1m-2025-08-07,oauth-2025-04-20",
			featureToRemove: "context-1m-2025-08-07",
			expected:        "fine-grained-tool-streaming-2025-05-14,oauth-2025-04-20",
		},
		{
			name:            "Remove context-1m from start",
			header:          "context-1m-2025-08-07,fine-grained-tool-streaming-2025-05-14",
			featureToRemove: "context-1m-2025-08-07",
			expected:        "fine-grained-tool-streaming-2025-05-14",
		},
		{
			name:            "Remove context-1m from end",
			header:          "fine-grained-tool-streaming-2025-05-14,context-1m-2025-08-07",
			featureToRemove: "context-1m-2025-08-07",
			expected:        "fine-grained-tool-streaming-2025-05-14",
		},
		{
			name:            "Feature not present",
			header:          "fine-grained-tool-streaming-2025-05-14,oauth-2025-04-20",
			featureToRemove: "context-1m-2025-08-07",
			expected:        "fine-grained-tool-streaming-2025-05-14,oauth-2025-04-20",
		},
		{
			name:            "Only feature to remove",
			header:          "context-1m-2025-08-07",
			featureToRemove: "context-1m-2025-08-07",
			expected:        "",
		},
		{
			name:            "Empty header",
			header:          "",
			featureToRemove: "context-1m-2025-08-07",
			expected:        "",
		},
		{
			name:            "Header with spaces",
			header:          "fine-grained-tool-streaming-2025-05-14, context-1m-2025-08-07 , oauth-2025-04-20",
			featureToRemove: "context-1m-2025-08-07",
			expected:        "fine-grained-tool-streaming-2025-05-14,oauth-2025-04-20",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := filterBetaFeatures(tt.header, tt.featureToRemove)
			if result != tt.expected {
				t.Errorf("filterBetaFeatures() = %q, want %q", result, tt.expected)
			}
		})
	}
}

// Newer Amp/RivetKit binaries perform a mandatory, token-less metadata discovery at
// GET /actors/metadata before connecting (the probe moved from /metadata). The local
// engine must answer it unauthenticated, otherwise the client's retry-forever lookup
// loops on connect_failed and the thread transport never establishes. The hole must
// stay scoped to exactly that GET so other actor paths still require a token.
func TestActorEngineRequest_RoutesUnauthenticatedMetadataDiscovery(t *testing.T) {
	meta := httptest.NewRequest(http.MethodGet, "http://localhost:8317/actors/metadata?namespace=default", nil)
	if !actorEngineMetadataRequest(meta) {
		t.Fatal("actorEngineMetadataRequest must match GET /actors/metadata")
	}
	if !actorEngineRequest(meta) {
		t.Fatal("unauthenticated GET /actors/metadata must route to the local engine")
	}

	for _, tc := range []struct {
		name string
		req  *http.Request
	}{
		{"non-metadata actor path", httptest.NewRequest(http.MethodGet, "http://localhost:8317/actors/T-123", nil)},
		{"metadata wrong method", httptest.NewRequest(http.MethodPost, "http://localhost:8317/actors/metadata", nil)},
		{"deeper metadata path", httptest.NewRequest(http.MethodGet, "http://localhost:8317/actors/x/metadata", nil)},
	} {
		if actorEngineMetadataRequest(tc.req) {
			t.Errorf("%s: must not be treated as metadata discovery", tc.name)
		}
		if actorEngineRequest(tc.req) {
			t.Errorf("%s: must still require engine auth/token", tc.name)
		}
	}
}

// The engine must serve the discovery descriptor on both the legacy /metadata path
// and the new /actors/metadata path, including the clientEndpoint the RivetKit client
// reads to resolve its connection endpoint.
func TestNeoRuntimeServesActorsMetadataDiscovery(t *testing.T) {
	rt := newNeoRuntime(&config.Config{})
	for _, path := range []string{"/metadata", "/actors/metadata"} {
		req := httptest.NewRequest(http.MethodGet, "http://localhost:8317"+path+"?namespace=default", nil)
		rec := httptest.NewRecorder()
		rt.handleHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: status = %d, want 200 (body=%s)", path, rec.Code, rec.Body.String())
		}
		// clientEndpoint lets the RivetKit client resolve its connection endpoint;
		// clientToken is what it then attaches (as rvt-token) to authenticate the WS.
		for _, field := range []string{"clientEndpoint", "clientToken"} {
			if !strings.Contains(rec.Body.String(), field) {
				t.Fatalf("%s: metadata response missing %s: %s", path, field, rec.Body.String())
			}
		}
	}
}

// Newer Amp binaries prefix the engine transport with /actors/. The bridge must map
// those back to the legacy paths the local engine speaks, while leaving actor CRUD
// paths untouched.
func TestNeoStripActorsRivetPrefix(t *testing.T) {
	cases := map[string]string{
		"/actors/metadata":                          "/metadata",
		"/actors/gateway":                           "/gateway",
		"/actors/gateway/threadActor/websocket/":    "/gateway/threadActor/websocket/",
		"/actors/gateway/threadActor/request/state": "/gateway/threadActor/request/state",
		"/metadata":                                 "/metadata",
		"/gateway/threadActor/":                     "/gateway/threadActor/",
		"/actors":                                   "/actors",
		"/actors/T-123":                             "/actors/T-123",
		"/actors/actors":                            "/actors/actors",
	}
	for in, want := range cases {
		if got := neoStripActorsRivetPrefix(in); got != want {
			t.Errorf("neoStripActorsRivetPrefix(%q) = %q, want %q", in, got, want)
		}
	}
}

// Once the client adopts the discovered engine endpoint it talks the rivetkit engine
// transport directly via /gateway/ (carrying its rvt-token). That path must bypass
// management auth like the /actors-prefixed manager transport, while the token-less
// metadata probe is allowed on both /metadata and /actors/metadata.
func TestActorEngineRequest_RoutesEngineModeGatewayTransport(t *testing.T) {
	bypass := []string{
		"http://localhost:8317/gateway/threadActor/websocket/?rvt-method=get&rvt-key=T-x&rvt-token=local-neo",
		"http://localhost:8317/metadata?namespace=default",
		"http://localhost:8317/actors/metadata?namespace=default",
	}
	for _, raw := range bypass {
		req := httptest.NewRequest(http.MethodGet, raw, nil)
		if !actorEngineRequest(req) {
			t.Errorf("expected engine routing (auth bypass) for %s", raw)
		}
	}

	// A gateway transport request with no rivet credential must still require auth — a
	// healthy client always carries its token once connected, so token-less probes are
	// not part of the local-neo flow.
	tokenless := httptest.NewRequest(http.MethodGet, "http://localhost:8317/gateway/threadActor/websocket/?rvt-method=get&rvt-key=T-x", nil)
	if actorEngineRequest(tokenless) {
		t.Error("token-less /gateway request must not bypass management auth")
	}
}
