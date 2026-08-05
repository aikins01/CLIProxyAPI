package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
)

type objectStoreTestRequest struct {
	method string
	body   []byte
	header http.Header
	query  string
}

type objectStoreTestBackend struct {
	mu               sync.Mutex
	requests         []objectStoreTestRequest
	visible          string
	seen             []byte
	currentETag      string
	currentData      []byte
	currentVersionID string
	nextVersionID    string
	conflictAt       int
	failAt           int
	putCount         int
	omitPutETag      bool
	omitGetETag      bool
}

func (b *objectStoreTestBackend) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	b.mu.Lock()
	b.requests = append(b.requests, objectStoreTestRequest{
		method: r.Method,
		body:   append([]byte(nil), body...),
		header: r.Header.Clone(),
		query:  r.URL.RawQuery,
	})
	requestIndex := len(b.requests)
	if b.visible != "" && r.Method == http.MethodPut {
		b.seen, _ = os.ReadFile(b.visible)
	}
	if b.conflictAt == requestIndex {
		b.currentETag = "concurrent"
		b.currentData = []byte(`{"value":"newer"}`)
		b.currentVersionID = "concurrent-version"
	}
	if b.failAt == requestIndex {
		b.mu.Unlock()
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, `<Error><Code>AccessDenied</Code><Message>rejected</Message></Error>`)
		return
	}
	if r.Method == http.MethodGet {
		if b.currentETag == "" {
			b.mu.Unlock()
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, `<Error><Code>NoSuchKey</Code><Message>missing</Message></Error>`)
			return
		}
		data := append([]byte(nil), b.currentData...)
		etag := b.currentETag
		versionID := b.currentVersionID
		omitETag := b.omitGetETag
		b.mu.Unlock()
		if !omitETag {
			w.Header().Set("ETag", `"`+etag+`"`)
		}
		if versionID != "" {
			w.Header().Set("X-Amz-Version-Id", versionID)
		}
		w.Header().Set("Last-Modified", "Mon, 02 Jan 2006 15:04:05 GMT")
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
		return
	}
	if r.Method == http.MethodHead {
		b.mu.Unlock()
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if match := strings.Trim(r.Header.Get("If-Match"), `"`); match != "" && match != b.currentETag {
		b.writePreconditionFailed(w)
		return
	}
	if r.Header.Get("If-None-Match") == "*" && b.currentETag != "" {
		b.writePreconditionFailed(w)
		return
	}
	if r.Method == http.MethodDelete {
		versionID := r.URL.Query().Get("versionId")
		if versionID == "" || versionID == b.currentVersionID {
			b.currentETag = ""
			b.currentData = nil
			b.currentVersionID = ""
		}
		b.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
		_, _ = io.WriteString(w, "deleted")
		return
	}
	b.putCount++
	b.currentETag = "etag-" + strconv.Itoa(b.putCount)
	b.currentData = decodeObjectStoreRequestBody(body)
	b.currentVersionID = b.nextVersionID
	etag := b.currentETag
	versionID := b.currentVersionID
	omitETag := b.omitPutETag
	b.mu.Unlock()
	if !omitETag {
		w.Header().Set("ETag", `"`+etag+`"`)
	}
	if versionID != "" {
		w.Header().Set("X-Amz-Version-Id", versionID)
	}
	w.WriteHeader(http.StatusOK)
}

func (b *objectStoreTestBackend) writePreconditionFailed(w http.ResponseWriter) {
	b.mu.Unlock()
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusPreconditionFailed)
	_, _ = io.WriteString(w, `<Error><Code>PreconditionFailed</Code><Message>changed</Message></Error>`)
}

func (b *objectStoreTestBackend) snapshot() ([]objectStoreTestRequest, []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	requests := make([]objectStoreTestRequest, len(b.requests))
	copy(requests, b.requests)
	return requests, append([]byte(nil), b.seen...)
}

func (b *objectStoreTestBackend) durableSnapshot() (string, []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.currentETag, append([]byte(nil), b.currentData...)
}

type storeTestTokenStorage struct {
	path string
	data []byte
	mode fs.FileMode
}

func (s *storeTestTokenStorage) SaveTokenToFile(path string) error {
	s.path = path
	mode := s.mode
	if mode == 0 {
		mode = 0o600
	}
	return os.WriteFile(path, s.data, mode)
}

type storeTestEmptyStorage struct{}

func (*storeTestEmptyStorage) SaveTokenToFile(string) error { return nil }

func TestObjectTokenStoreSavePersistsBeforeLocalPublication(t *testing.T) {
	previous := []byte(`{"value":"previous"}`)
	backend := &objectStoreTestBackend{
		currentETag:      "previous-etag",
		currentData:      previous,
		currentVersionID: "previous-version",
	}
	store := newObjectTokenStoreForTest(t, backend)
	path := filepath.Join(store.authDir, "credential.json")
	if err := os.WriteFile(path, previous, 0o600); err != nil {
		t.Fatalf("write previous auth: %v", err)
	}
	backend.visible = path

	_, err := store.Save(context.Background(), &cliproxyauth.Auth{
		ID:       "credential",
		Metadata: map[string]any{"value": "candidate"},
	})
	if err != nil {
		t.Fatalf("Save: %v", err)
	}

	requests, seen := backend.snapshot()
	if len(requests) != 2 || requests[0].method != http.MethodGet || requests[1].method != http.MethodPut {
		t.Fatalf("requests = %#v, want GET then PUT", requests)
	}
	if string(seen) != string(previous) {
		t.Fatalf("visible bytes during PUT = %q, want %q", seen, previous)
	}
	if got := requests[1].header.Get("If-Match"); got != `"previous-etag"` {
		t.Fatalf("replacement If-Match = %q, want %q", got, `"previous-etag"`)
	}
	assertJSONField(t, decodeObjectStoreBody(t, requests[1].body), "value", "candidate")
	published, errRead := os.ReadFile(path)
	if errRead != nil {
		t.Fatalf("read published auth: %v", errRead)
	}
	assertJSONField(t, published, "value", "candidate")
	assertNoStoreTempFile(t, path)
}

func TestObjectTokenStoreSaveUploadFailureDoesNotPublishLocalFile(t *testing.T) {
	for _, test := range []struct {
		name     string
		previous []byte
	}{
		{name: "existing", previous: []byte(`{"value":"previous"}`)},
		{name: "new"},
	} {
		t.Run(test.name, func(t *testing.T) {
			backend := &objectStoreTestBackend{failAt: 2}
			if test.previous != nil {
				backend.currentETag = "previous-etag"
				backend.currentData = append([]byte(nil), test.previous...)
			}
			store := newObjectTokenStoreForTest(t, backend)
			path := filepath.Join(store.authDir, "credential.json")
			if test.previous != nil {
				if err := os.WriteFile(path, test.previous, 0o600); err != nil {
					t.Fatalf("write previous auth: %v", err)
				}
			}

			_, err := store.Save(context.Background(), &cliproxyauth.Auth{
				ID:       "credential",
				Metadata: map[string]any{"value": "candidate"},
			})
			if err == nil {
				t.Fatal("Save succeeded, want upload error")
			}
			requests, _ := backend.snapshot()
			if len(requests) != 2 || requests[0].method != http.MethodGet || requests[1].method != http.MethodPut {
				t.Fatalf("requests = %#v, want GET then failed PUT", requests)
			}
			assertLocalStoreBytes(t, path, test.previous)
			assertNoStoreTempFile(t, path)
		})
	}
}

func TestObjectTokenStoreSavePublishFailureRestoresExistingRemoteConditionally(t *testing.T) {
	remotePrevious := []byte("{\n  \"value\": \"remote\",\n  \"opaque\": 7\n}\n")
	backend := &objectStoreTestBackend{
		currentETag:      "previous-etag",
		currentData:      remotePrevious,
		currentVersionID: "previous-version",
	}
	store := newObjectTokenStoreForTest(t, backend)
	path := filepath.Join(store.authDir, "credential.json")
	localPrevious := []byte(`{"value":"local"}`)
	if err := os.WriteFile(path, localPrevious, 0o600); err != nil {
		t.Fatalf("write previous auth: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	store.renameFile = func(string, string) error {
		cancel()
		return errors.New("publish rejected")
	}

	_, err := store.Save(ctx, &cliproxyauth.Auth{
		ID:       "credential",
		Metadata: map[string]any{"value": "candidate"},
	})
	if err == nil {
		t.Fatal("Save succeeded, want publish error")
	}
	requests, _ := backend.snapshot()
	if len(requests) != 3 || requests[0].method != http.MethodGet || requests[1].method != http.MethodPut || requests[2].method != http.MethodPut {
		t.Fatalf("requests = %#v, want GET then two PUTs", requests)
	}
	if got := requests[1].header.Get("If-Match"); got != `"previous-etag"` {
		t.Fatalf("candidate If-Match = %q, want %q", got, `"previous-etag"`)
	}
	if got := requests[2].header.Get("If-Match"); got != `"etag-1"` {
		t.Fatalf("rollback If-Match = %q, want %q", got, `"etag-1"`)
	}
	if got := decodeObjectStoreBody(t, requests[2].body); string(got) != string(remotePrevious) {
		t.Fatalf("rollback bytes = %q, want exact captured remote bytes %q", got, remotePrevious)
	}
	etag, data := backend.durableSnapshot()
	if etag != "etag-2" || string(data) != string(remotePrevious) {
		t.Fatalf("durable object = (%q, %q), want exact restored remote bytes", etag, data)
	}
	assertLocalStoreBytes(t, path, localPrevious)
	assertNoStoreTempFile(t, path)
}

func TestObjectTokenStoreSavePublishFailureDeletesExactUploadedVersion(t *testing.T) {
	backend := &objectStoreTestBackend{nextVersionID: "candidate-version"}
	store := newObjectTokenStoreForTest(t, backend)
	path := filepath.Join(store.authDir, "credential.json")
	store.renameFile = func(string, string) error { return errors.New("publish rejected") }

	_, err := store.Save(context.Background(), &cliproxyauth.Auth{
		ID:       "credential",
		Metadata: map[string]any{"value": "candidate"},
	})
	if err == nil {
		t.Fatal("Save succeeded, want publish error")
	}
	requests, _ := backend.snapshot()
	if len(requests) != 3 || requests[0].method != http.MethodGet || requests[1].method != http.MethodPut || requests[2].method != http.MethodDelete {
		t.Fatalf("requests = %#v, want GET, PUT, then DELETE", requests)
	}
	if got := requests[1].header.Get("If-None-Match"); got != "*" {
		t.Fatalf("candidate If-None-Match = %q, want *", got)
	}
	if !strings.Contains(requests[2].query, "versionId=candidate-version") {
		t.Fatalf("rollback query = %q, want uploaded version", requests[2].query)
	}
	assertLocalStoreBytes(t, path, nil)
	assertNoStoreTempFile(t, path)
}

func TestObjectTokenStoreSaveVersionRollbackPreservesConcurrentCurrentObject(t *testing.T) {
	backend := &objectStoreTestBackend{nextVersionID: "candidate-version", conflictAt: 3}
	store := newObjectTokenStoreForTest(t, backend)
	path := filepath.Join(store.authDir, "credential.json")
	store.renameFile = func(string, string) error { return errors.New("publish rejected") }

	_, err := store.Save(context.Background(), &cliproxyauth.Auth{
		ID:       "credential",
		Metadata: map[string]any{"value": "candidate"},
	})
	if err == nil || strings.Contains(err.Error(), "remote rollback failed") {
		t.Fatalf("Save error = %v, want publish failure with exact-version rollback", err)
	}
	requests, _ := backend.snapshot()
	if len(requests) != 3 || requests[2].method != http.MethodDelete || !strings.Contains(requests[2].query, "versionId=candidate-version") {
		t.Fatalf("requests = %#v, want rollback of candidate version", requests)
	}
	etag, data := backend.durableSnapshot()
	if etag != "concurrent" || string(data) != `{"value":"newer"}` {
		t.Fatalf("durable object = (%q, %q), want concurrent newer object", etag, data)
	}
	assertLocalStoreBytes(t, path, nil)
	assertNoStoreTempFile(t, path)
}

func TestObjectTokenStoreSavePublishFailureWithoutVersionConditionallyDeletesCandidate(t *testing.T) {
	backend := &objectStoreTestBackend{}
	store := newObjectTokenStoreForTest(t, backend)
	path := filepath.Join(store.authDir, "credential.json")
	store.renameFile = func(string, string) error { return errors.New("publish rejected") }

	_, err := store.Save(context.Background(), &cliproxyauth.Auth{
		ID:       "credential",
		Metadata: map[string]any{"value": "candidate"},
	})
	if err == nil || !strings.Contains(err.Error(), "publish rejected") || strings.Contains(err.Error(), "remote rollback failed") {
		t.Fatalf("Save error = %v, want publish failure with successful rollback", err)
	}
	requests, _ := backend.snapshot()
	if len(requests) != 3 || requests[0].method != http.MethodGet || requests[1].method != http.MethodPut || requests[2].method != http.MethodDelete {
		t.Fatalf("requests = %#v, want GET, PUT, then conditional DELETE", requests)
	}
	if got := requests[2].header.Get("If-Match"); got != `"etag-1"` {
		t.Fatalf("rollback If-Match = %q, want %q", got, `"etag-1"`)
	}
	if !strings.Contains(requests[2].query, "X-Amz-Signature=") {
		t.Fatalf("rollback query = %q, want presigned DELETE", requests[2].query)
	}
	etag, data := backend.durableSnapshot()
	if etag != "" || data != nil {
		t.Fatalf("durable object = (%q, %q), want absent", etag, data)
	}
	assertLocalStoreBytes(t, path, nil)
	assertNoStoreTempFile(t, path)
}

func TestObjectTokenStoreSaveConditionalDeleteRollbackConflictPreservesConcurrentRemoteObject(t *testing.T) {
	backend := &objectStoreTestBackend{conflictAt: 3}
	store := newObjectTokenStoreForTest(t, backend)
	path := filepath.Join(store.authDir, "credential.json")
	store.renameFile = func(string, string) error { return errors.New("publish rejected") }

	_, err := store.Save(context.Background(), &cliproxyauth.Auth{
		ID:       "credential",
		Metadata: map[string]any{"value": "candidate"},
	})
	if err == nil || !strings.Contains(err.Error(), "remote rollback failed") {
		t.Fatalf("Save error = %v, want rollback conflict", err)
	}
	requests, _ := backend.snapshot()
	if len(requests) != 3 || requests[2].method != http.MethodDelete {
		t.Fatalf("requests = %#v, want GET, PUT, then conditional DELETE", requests)
	}
	if got := requests[2].header.Get("If-Match"); got != `"etag-1"` {
		t.Fatalf("rollback If-Match = %q, want %q", got, `"etag-1"`)
	}
	etag, data := backend.durableSnapshot()
	if etag != "concurrent" || string(data) != `{"value":"newer"}` {
		t.Fatalf("durable object = (%q, %q), want concurrent newer object", etag, data)
	}
	assertLocalStoreBytes(t, path, nil)
	assertNoStoreTempFile(t, path)
}

func TestObjectTokenStoreSaveRollbackConflictPreservesNewerRemoteObject(t *testing.T) {
	previous := []byte(`{"value":"previous"}`)
	backend := &objectStoreTestBackend{currentETag: "previous", currentData: previous, conflictAt: 3}
	store := newObjectTokenStoreForTest(t, backend)
	path := filepath.Join(store.authDir, "credential.json")
	if err := os.WriteFile(path, previous, 0o600); err != nil {
		t.Fatalf("write previous auth: %v", err)
	}
	store.renameFile = func(string, string) error { return errors.New("publish rejected") }

	_, err := store.Save(context.Background(), &cliproxyauth.Auth{
		ID:       "credential",
		Metadata: map[string]any{"value": "candidate"},
	})
	if err == nil || !strings.Contains(err.Error(), "remote rollback failed") {
		t.Fatalf("Save error = %v, want rollback conflict", err)
	}
	etag, data := backend.durableSnapshot()
	if etag != "concurrent" || string(data) != `{"value":"newer"}` {
		t.Fatalf("durable object = (%q, %q), want concurrent newer object", etag, data)
	}
	assertLocalStoreBytes(t, path, previous)
	assertNoStoreTempFile(t, path)
}

func TestObjectTokenStoreSavePublishFailureWithoutETagRefusesUnsafeRestore(t *testing.T) {
	previous := []byte(`{"value":"previous"}`)
	backend := &objectStoreTestBackend{currentETag: "previous", currentData: previous, omitPutETag: true}
	store := newObjectTokenStoreForTest(t, backend)
	path := filepath.Join(store.authDir, "credential.json")
	if err := os.WriteFile(path, previous, 0o600); err != nil {
		t.Fatalf("write previous auth: %v", err)
	}
	store.renameFile = func(string, string) error { return errors.New("publish rejected") }

	_, err := store.Save(context.Background(), &cliproxyauth.Auth{
		ID:       "credential",
		Metadata: map[string]any{"value": "candidate"},
	})
	if err == nil || !strings.Contains(err.Error(), "without uploaded ETag") {
		t.Fatalf("Save error = %v, want unsafe restore refusal", err)
	}
	requests, _ := backend.snapshot()
	if len(requests) != 2 || requests[0].method != http.MethodGet || requests[1].method != http.MethodPut {
		t.Fatalf("requests = %#v, want GET then candidate PUT", requests)
	}
	assertLocalStoreBytes(t, path, previous)
	assertNoStoreTempFile(t, path)
}

func TestObjectTokenStoreSaveReplacementConflictPreservesConcurrentRemoteObject(t *testing.T) {
	previous := []byte(`{"value":"previous"}`)
	backend := &objectStoreTestBackend{currentETag: "previous-etag", currentData: previous, conflictAt: 2}
	store := newObjectTokenStoreForTest(t, backend)
	path := filepath.Join(store.authDir, "credential.json")
	if err := os.WriteFile(path, previous, 0o600); err != nil {
		t.Fatalf("write previous auth: %v", err)
	}

	_, err := store.Save(context.Background(), &cliproxyauth.Auth{
		ID:       "credential",
		Metadata: map[string]any{"value": "candidate"},
	})
	if err == nil {
		t.Fatal("Save succeeded, want replacement conflict")
	}
	requests, _ := backend.snapshot()
	if len(requests) != 2 || requests[0].method != http.MethodGet || requests[1].method != http.MethodPut {
		t.Fatalf("requests = %#v, want GET then conflicting PUT", requests)
	}
	if got := requests[1].header.Get("If-Match"); got != `"previous-etag"` {
		t.Fatalf("replacement If-Match = %q, want %q", got, `"previous-etag"`)
	}
	etag, data := backend.durableSnapshot()
	if etag != "concurrent" || string(data) != `{"value":"newer"}` {
		t.Fatalf("durable object = (%q, %q), want concurrent newer object", etag, data)
	}
	assertLocalStoreBytes(t, path, previous)
	assertNoStoreTempFile(t, path)
}

func TestObjectTokenStoreSaveCreateConflictPreservesConcurrentRemoteObject(t *testing.T) {
	backend := &objectStoreTestBackend{conflictAt: 2}
	store := newObjectTokenStoreForTest(t, backend)
	path := filepath.Join(store.authDir, "credential.json")

	_, err := store.Save(context.Background(), &cliproxyauth.Auth{
		ID:       "credential",
		Metadata: map[string]any{"value": "candidate"},
	})
	if err == nil {
		t.Fatal("Save succeeded, want create conflict")
	}
	requests, _ := backend.snapshot()
	if len(requests) != 2 || requests[0].method != http.MethodGet || requests[1].method != http.MethodPut {
		t.Fatalf("requests = %#v, want GET then conflicting PUT", requests)
	}
	if got := requests[1].header.Get("If-None-Match"); got != "*" {
		t.Fatalf("create If-None-Match = %q, want *", got)
	}
	etag, data := backend.durableSnapshot()
	if etag != "concurrent" || string(data) != `{"value":"newer"}` {
		t.Fatalf("durable object = (%q, %q), want concurrent newer object", etag, data)
	}
	assertLocalStoreBytes(t, path, nil)
	assertNoStoreTempFile(t, path)
}

func TestObjectTokenStoreSaveEmptyPayloadConditionallyDeletesRemoteObject(t *testing.T) {
	previous := []byte(`{"value":"previous"}`)
	backend := &objectStoreTestBackend{currentETag: "previous-etag", currentData: previous}
	store := newObjectTokenStoreForTest(t, backend)
	path := filepath.Join(store.authDir, "credential.json")
	if err := os.WriteFile(path, previous, 0o600); err != nil {
		t.Fatalf("write previous auth: %v", err)
	}

	_, err := store.Save(context.Background(), &cliproxyauth.Auth{
		ID:      "credential",
		Storage: &storeTestTokenStorage{data: []byte{}},
	})
	if err != nil {
		t.Fatalf("Save: %v", err)
	}
	requests, _ := backend.snapshot()
	if len(requests) != 2 || requests[0].method != http.MethodGet || requests[1].method != http.MethodDelete {
		t.Fatalf("requests = %#v, want GET then DELETE", requests)
	}
	if got := requests[1].header.Get("If-Match"); got != `"previous-etag"` {
		t.Fatalf("delete If-Match = %q, want %q", got, `"previous-etag"`)
	}
	if !strings.Contains(requests[1].query, "X-Amz-Signature=") || !strings.Contains(requests[1].query, "if-match") {
		t.Fatalf("delete query = %q, want If-Match-signed presigned URL", requests[1].query)
	}
	etag, data := backend.durableSnapshot()
	if etag != "" || data != nil {
		t.Fatalf("durable object = (%q, %q), want absent", etag, data)
	}
	assertLocalStoreBytes(t, path, []byte{})
	assertStoreFileMode(t, path, 0o600)
	assertNoStoreTempFile(t, path)
}

func TestObjectTokenStoreSaveEmptyPayloadDeleteConflictPreservesConcurrentRemoteObject(t *testing.T) {
	previous := []byte(`{"value":"previous"}`)
	backend := &objectStoreTestBackend{currentETag: "previous-etag", currentData: previous, conflictAt: 2}
	store := newObjectTokenStoreForTest(t, backend)
	path := filepath.Join(store.authDir, "credential.json")
	if err := os.WriteFile(path, previous, 0o600); err != nil {
		t.Fatalf("write previous auth: %v", err)
	}

	_, err := store.Save(context.Background(), &cliproxyauth.Auth{
		ID:      "credential",
		Storage: &storeTestTokenStorage{data: []byte{}},
	})
	if err == nil {
		t.Fatal("Save succeeded, want delete conflict")
	}
	etag, data := backend.durableSnapshot()
	if etag != "concurrent" || string(data) != `{"value":"newer"}` {
		t.Fatalf("durable object = (%q, %q), want concurrent newer object", etag, data)
	}
	assertLocalStoreBytes(t, path, previous)
	assertNoStoreTempFile(t, path)
}

func TestObjectTokenStoreSavePublishFailureAfterDeleteRecreatesExactRemoteBytesIfAbsent(t *testing.T) {
	remotePrevious := []byte("{\n  \"value\": \"remote\"\n}\n")
	localPrevious := []byte(`{"value":"local"}`)
	backend := &objectStoreTestBackend{currentETag: "previous-etag", currentData: remotePrevious}
	store := newObjectTokenStoreForTest(t, backend)
	path := filepath.Join(store.authDir, "credential.json")
	if err := os.WriteFile(path, localPrevious, 0o600); err != nil {
		t.Fatalf("write previous auth: %v", err)
	}
	store.renameFile = func(string, string) error { return errors.New("publish rejected") }

	_, err := store.Save(context.Background(), &cliproxyauth.Auth{
		ID:      "credential",
		Storage: &storeTestTokenStorage{data: []byte{}},
	})
	if err == nil || strings.Contains(err.Error(), "remote rollback failed") {
		t.Fatalf("Save error = %v, want publish failure with successful recreation", err)
	}
	requests, _ := backend.snapshot()
	if len(requests) != 3 || requests[0].method != http.MethodGet || requests[1].method != http.MethodDelete || requests[2].method != http.MethodPut {
		t.Fatalf("requests = %#v, want GET, DELETE, then PUT", requests)
	}
	if got := requests[2].header.Get("If-None-Match"); got != "*" {
		t.Fatalf("recreate If-None-Match = %q, want *", got)
	}
	if got := decodeObjectStoreBody(t, requests[2].body); string(got) != string(remotePrevious) {
		t.Fatalf("recreated bytes = %q, want exact captured remote bytes %q", got, remotePrevious)
	}
	etag, data := backend.durableSnapshot()
	if etag != "etag-1" || string(data) != string(remotePrevious) {
		t.Fatalf("durable object = (%q, %q), want exact recreated remote bytes", etag, data)
	}
	assertLocalStoreBytes(t, path, localPrevious)
	assertNoStoreTempFile(t, path)
}

func TestObjectTokenStoreSaveDeleteRollbackConflictPreservesConcurrentRemoteObject(t *testing.T) {
	previous := []byte(`{"value":"previous"}`)
	backend := &objectStoreTestBackend{currentETag: "previous-etag", currentData: previous, conflictAt: 3}
	store := newObjectTokenStoreForTest(t, backend)
	path := filepath.Join(store.authDir, "credential.json")
	if err := os.WriteFile(path, previous, 0o600); err != nil {
		t.Fatalf("write previous auth: %v", err)
	}
	store.renameFile = func(string, string) error { return errors.New("publish rejected") }

	_, err := store.Save(context.Background(), &cliproxyauth.Auth{
		ID:      "credential",
		Storage: &storeTestTokenStorage{data: []byte{}},
	})
	if err == nil || !strings.Contains(err.Error(), "remote rollback failed") {
		t.Fatalf("Save error = %v, want rollback conflict", err)
	}
	etag, data := backend.durableSnapshot()
	if etag != "concurrent" || string(data) != `{"value":"newer"}` {
		t.Fatalf("durable object = (%q, %q), want concurrent newer object", etag, data)
	}
	assertLocalStoreBytes(t, path, previous)
	assertNoStoreTempFile(t, path)
}

func TestObjectTokenStoreSaveUsesTemporaryPathForTokenStorage(t *testing.T) {
	backend := &objectStoreTestBackend{}
	store := newObjectTokenStoreForTest(t, backend)
	storage := &storeTestTokenStorage{data: []byte(`{"type":"gemini","token":"value"}`), mode: 0o644}
	store.renameFile = func(oldPath, newPath string) error {
		assertStoreFileMode(t, oldPath, 0o600)
		return os.Rename(oldPath, newPath)
	}
	path, err := store.Save(context.Background(), &cliproxyauth.Auth{
		ID:       "credential",
		Metadata: map[string]any{"type": "gemini"},
		Storage:  storage,
	})
	if err != nil {
		t.Fatalf("Save: %v", err)
	}
	if storage.path != path+".tmp" {
		t.Fatalf("storage path = %q, want %q", storage.path, path+".tmp")
	}
	assertLocalStoreBytes(t, path, storage.data)
	assertStoreFileMode(t, path, 0o600)
	assertNoStoreTempFile(t, path)
}

func TestObjectTokenStoreSavePreservesEmptyStorageNoOp(t *testing.T) {
	backend := &objectStoreTestBackend{}
	store := newObjectTokenStoreForTest(t, backend)
	auth := &cliproxyauth.Auth{
		ID:      "credential",
		Storage: &storeTestEmptyStorage{},
	}
	path, err := store.Save(context.Background(), auth)
	if err != nil {
		t.Fatalf("Save: %v", err)
	}
	if _, errStat := os.Stat(path); !errors.Is(errStat, os.ErrNotExist) {
		t.Fatalf("stat visible auth error = %v, want not exist", errStat)
	}
	requests, _ := backend.snapshot()
	if len(requests) != 0 {
		t.Fatalf("requests = %#v, want none", requests)
	}
	assertSavedAuthNormalized(t, auth, path)
}

func TestObjectTokenStoreSaveSameJSONNormalizesAuth(t *testing.T) {
	backend := &objectStoreTestBackend{}
	store := newObjectTokenStoreForTest(t, backend)
	path := filepath.Join(store.authDir, "credential.json")
	if err := os.WriteFile(path, []byte(`{"disabled":false,"value":"same"}`), 0o600); err != nil {
		t.Fatalf("write existing auth: %v", err)
	}
	auth := &cliproxyauth.Auth{ID: "credential", Metadata: map[string]any{"value": "same"}}
	gotPath, err := store.Save(context.Background(), auth)
	if err != nil {
		t.Fatalf("Save: %v", err)
	}
	if gotPath != path {
		t.Fatalf("Save path = %q, want %q", gotPath, path)
	}
	requests, _ := backend.snapshot()
	if len(requests) != 0 {
		t.Fatalf("requests = %#v, want none", requests)
	}
	assertSavedAuthNormalized(t, auth, path)
}

func newObjectTokenStoreForTest(t *testing.T, backend *objectStoreTestBackend) *ObjectTokenStore {
	t.Helper()
	server := httptest.NewServer(backend)
	t.Cleanup(server.Close)
	store, err := NewObjectTokenStore(ObjectStoreConfig{
		Endpoint:  strings.TrimPrefix(server.URL, "http://"),
		Bucket:    "test-bucket",
		AccessKey: "access",
		SecretKey: "secret",
		Region:    "us-east-1",
		LocalRoot: t.TempDir(),
		PathStyle: true,
	})
	if err != nil {
		t.Fatalf("NewObjectTokenStore: %v", err)
	}
	return store
}

func assertJSONField(t *testing.T, data []byte, key, want string) {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal(data, &payload); err != nil {
		t.Fatalf("unmarshal JSON %q: %v", data, err)
	}
	if got := payload[key]; got != want {
		t.Fatalf("JSON field %q = %#v, want %q", key, got, want)
	}
}

func assertLocalStoreBytes(t *testing.T, path string, want []byte) {
	t.Helper()
	got, err := os.ReadFile(path)
	if want == nil {
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("read local auth error = %v, want not exist", err)
		}
		return
	}
	if err != nil {
		t.Fatalf("read local auth: %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("local auth bytes = %q, want %q", got, want)
	}
}

func assertNoStoreTempFile(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path + ".tmp"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stat temp auth error = %v, want not exist", err)
	}
}

func assertStoreFileMode(t *testing.T, path string, want fs.FileMode) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	if got := info.Mode().Perm(); got != want {
		t.Fatalf("file mode for %s = %04o, want %04o", path, got, want)
	}
}

func assertSavedAuthNormalized(t *testing.T, auth *cliproxyauth.Auth, path string) {
	t.Helper()
	if got := auth.Attributes["path"]; got != path {
		t.Fatalf("auth path attribute = %q, want %q", got, path)
	}
	if got := auth.FileName; got != auth.ID {
		t.Fatalf("auth filename = %q, want %q", got, auth.ID)
	}
}

func decodeObjectStoreRequestBody(body []byte) []byte {
	original := append([]byte(nil), body...)
	decoded := make([]byte, 0, len(body))
	for len(body) > 0 {
		lineEnd := bytes.Index(body, []byte("\r\n"))
		if lineEnd < 0 {
			return original
		}
		sizeText := string(body[:lineEnd])
		if separator := strings.IndexByte(sizeText, ';'); separator >= 0 {
			sizeText = sizeText[:separator]
		}
		size, err := strconv.ParseInt(sizeText, 16, 64)
		if err != nil {
			return original
		}
		body = body[lineEnd+2:]
		if size == 0 {
			return decoded
		}
		if size > int64(len(body)) {
			return original
		}
		decoded = append(decoded, body[:size]...)
		body = body[size:]
		if bytes.HasPrefix(body, []byte("\r\n")) {
			body = body[2:]
		}
	}
	return decoded
}

func decodeObjectStoreBody(t *testing.T, body []byte) []byte {
	t.Helper()
	return decodeObjectStoreRequestBody(body)
}
