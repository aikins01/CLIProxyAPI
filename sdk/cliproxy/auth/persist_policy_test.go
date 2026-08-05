package auth

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

type countingStore struct {
	saveCount atomic.Int32
	saveErr   error
}

func (s *countingStore) List(context.Context) ([]*Auth, error) { return nil, nil }

func (s *countingStore) Save(context.Context, *Auth) (string, error) {
	s.saveCount.Add(1)
	return "", s.saveErr
}

func (s *countingStore) Delete(context.Context, string) error { return nil }

type normalizingStore struct {
	saveErr error
}

func (*normalizingStore) List(context.Context) ([]*Auth, error) { return nil, nil }

func (s *normalizingStore) Save(_ context.Context, auth *Auth) (string, error) {
	if auth.Metadata == nil {
		auth.Metadata = map[string]any{}
	}
	auth.Metadata["disabled"] = auth.Disabled
	return "", s.saveErr
}

func (*normalizingStore) Delete(context.Context, string) error { return nil }

type loadingStore struct {
	items []*Auth
}

func (s *loadingStore) List(context.Context) ([]*Auth, error) {
	items := make([]*Auth, 0, len(s.items))
	for _, auth := range s.items {
		items = append(items, auth.Clone())
	}
	return items, nil
}

func (*loadingStore) Save(context.Context, *Auth) (string, error) { return "", nil }
func (*loadingStore) Delete(context.Context, string) error        { return nil }

type blockingListStore struct {
	items   []*Auth
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockingListStore) List(context.Context) ([]*Auth, error) {
	s.once.Do(func() { close(s.started) })
	<-s.release
	return s.items, nil
}

func (*blockingListStore) Save(context.Context, *Auth) (string, error) { return "", nil }
func (*blockingListStore) Delete(context.Context, string) error        { return nil }

type authSnapshotHook struct {
	mu         sync.Mutex
	registered *Auth
	updated    *Auth
}

func (h *authSnapshotHook) OnAuthRegistered(_ context.Context, auth *Auth) {
	h.mu.Lock()
	h.registered = auth.Clone()
	h.mu.Unlock()
}

func (h *authSnapshotHook) OnAuthUpdated(_ context.Context, auth *Auth) {
	h.mu.Lock()
	h.updated = auth.Clone()
	h.mu.Unlock()
}

func (*authSnapshotHook) OnResult(context.Context, Result) {}

type reentrantMetadataHook struct {
	mgr                    *Manager
	once                   sync.Once
	mu                     sync.Mutex
	cookies                []string
	err                    error
	nestedCommitted        atomic.Bool
	nestedCallbackDeferred atomic.Bool
}

func (*reentrantMetadataHook) OnAuthRegistered(context.Context, *Auth) {}

func (h *reentrantMetadataHook) OnAuthUpdated(ctx context.Context, auth *Auth) {
	cookie := stringFromMetadata(auth.Metadata, "cookie")
	h.mu.Lock()
	h.cookies = append(h.cookies, cookie)
	h.mu.Unlock()
	if cookie == "rotated-2" {
		h.nestedCallbackDeferred.Store(h.nestedCommitted.Load())
	}
	h.once.Do(func() {
		expected, ok := h.mgr.GetByID(auth.ID)
		if !ok {
			h.err = errors.New("auth not found")
			return
		}
		updated, err := h.mgr.UpdateMetadata(ctx, expected, map[string]any{"cookie": "rotated-2"})
		h.err = err
		if err == nil && stringFromMetadata(updated.Metadata, "cookie") == "rotated-2" {
			h.nestedCommitted.Store(true)
		}
	})
}

func (*reentrantMetadataHook) OnResult(context.Context, Result) {}

type retainedContextHook struct {
	ctx context.Context
}

func (*retainedContextHook) OnAuthRegistered(context.Context, *Auth) {}

func (h *retainedContextHook) OnAuthUpdated(ctx context.Context, _ *Auth) {
	h.ctx = ctx
}

func (*retainedContextHook) OnResult(context.Context, Result) {}

type blockingPanickingMetadataHook struct {
	started      chan struct{}
	release      chan struct{}
	secondCalled chan struct{}
	calls        atomic.Int32
}

func (*blockingPanickingMetadataHook) OnAuthRegistered(context.Context, *Auth) {}

func (h *blockingPanickingMetadataHook) OnAuthUpdated(context.Context, *Auth) {
	switch h.calls.Add(1) {
	case 1:
		close(h.started)
		<-h.release
		panic("first hook failed")
	case 2:
		close(h.secondCalled)
	}
}

func (*blockingPanickingMetadataHook) OnResult(context.Context, Result) {}

type crossAuthReentrantHook struct {
	mgr     *Manager
	started chan struct{}
	release chan struct{}
	results chan error
	calls   atomic.Int32
}

func (*crossAuthReentrantHook) OnAuthRegistered(context.Context, *Auth) {}

func (h *crossAuthReentrantHook) OnAuthUpdated(ctx context.Context, auth *Auth) {
	if auth.Label != "outer" {
		return
	}
	if h.calls.Add(1) == 2 {
		close(h.started)
	}
	<-h.release
	otherID := "auth-1"
	if auth.ID == otherID {
		otherID = "auth-2"
	}
	other, _ := h.mgr.GetByID(otherID)
	if other == nil {
		return
	}
	other.Label = "nested"
	_, err := h.mgr.Update(ctx, other)
	h.results <- err
}

func (*crossAuthReentrantHook) OnResult(context.Context, Result) {}

type executionMetadataCommitExecutor struct {
	updateAuthMetadata func(context.Context, *Auth, map[string]any) (*Auth, error)
}

func (*executionMetadataCommitExecutor) Identifier() string { return "gemini-cli" }

func (e *executionMetadataCommitExecutor) SetAuthMetadataUpdater(update func(context.Context, *Auth, map[string]any) (*Auth, error)) {
	e.updateAuthMetadata = update
}

func (e *executionMetadataCommitExecutor) Execute(ctx context.Context, auth *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	if e.updateAuthMetadata == nil {
		return cliproxyexecutor.Response{}, errors.New("metadata updater not configured")
	}
	_, err := e.updateAuthMetadata(ctx, auth, map[string]any{"access_token": "rotated"})
	return cliproxyexecutor.Response{Payload: []byte("ok")}, err
}

func (*executionMetadataCommitExecutor) ExecuteStream(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	return nil, errors.New("not implemented")
}

func (*executionMetadataCommitExecutor) Refresh(_ context.Context, auth *Auth) (*Auth, error) {
	return auth, nil
}

func (*executionMetadataCommitExecutor) CountTokens(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, errors.New("not implemented")
}

func (*executionMetadataCommitExecutor) HttpRequest(context.Context, *Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

type sequentialCrossAuthReentrantHook struct {
	mgr *Manager
	err error
}

func (*sequentialCrossAuthReentrantHook) OnAuthRegistered(context.Context, *Auth) {}

func (h *sequentialCrossAuthReentrantHook) OnAuthUpdated(ctx context.Context, auth *Auth) {
	switch {
	case auth.ID == "auth-1" && auth.Label == "outer":
		other, _ := h.mgr.GetByID("auth-2")
		other.Label = "relay"
		_, h.err = h.mgr.Update(ctx, other)
	case auth.ID == "auth-2" && auth.Label == "relay":
		other, _ := h.mgr.GetByID("auth-1")
		other.Label = "nested"
		_, h.err = h.mgr.Update(ctx, other)
	}
}

func (*sequentialCrossAuthReentrantHook) OnResult(context.Context, Result) {}

type crossManagerBlockingHook struct {
	started      chan struct{}
	release      chan struct{}
	nestedCalled chan struct{}
}

func (*crossManagerBlockingHook) OnAuthRegistered(context.Context, *Auth) {}

func (h *crossManagerBlockingHook) OnAuthUpdated(_ context.Context, auth *Auth) {
	switch auth.Label {
	case "blocking":
		close(h.started)
		<-h.release
	case "nested":
		close(h.nestedCalled)
	}
}

func (*crossManagerBlockingHook) OnResult(context.Context, Result) {}

type crossManagerForwardingHook struct {
	target *Manager
	result chan error
}

func (*crossManagerForwardingHook) OnAuthRegistered(context.Context, *Auth) {}

func (h *crossManagerForwardingHook) OnAuthUpdated(ctx context.Context, auth *Auth) {
	if auth.Label != "outer" {
		return
	}
	target, _ := h.target.GetByID(auth.ID)
	target.Label = "nested"
	_, err := h.target.Update(ctx, target)
	h.result <- err
}

func (*crossManagerForwardingHook) OnResult(context.Context, Result) {}

func stringFromMetadata(metadata map[string]any, key string) string {
	value, _ := metadata[key].(string)
	return value
}

func TestUpdateMetadataHookReentrancyPreservesOrder(t *testing.T) {
	hook := &reentrantMetadataHook{}
	mgr := NewManager(nil, nil, hook)
	hook.mgr = mgr
	auth := &Auth{ID: "auth-1", Provider: "chatgpt-web", Metadata: map[string]any{"cookie": "original"}}
	registered, err := mgr.Register(t.Context(), auth)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		_, errUpdate := mgr.UpdateMetadata(t.Context(), registered, map[string]any{"cookie": "rotated-1"})
		done <- errUpdate
	}()
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("reentrant metadata hook deadlocked")
	}
	if hook.err != nil {
		t.Fatal(hook.err)
	}
	hook.mu.Lock()
	cookies := append([]string(nil), hook.cookies...)
	hook.mu.Unlock()
	if len(cookies) != 2 || cookies[0] != "rotated-1" || cookies[1] != "rotated-2" {
		t.Fatalf("hook cookies = %#v, want ordered rotations", cookies)
	}
	if !hook.nestedCommitted.Load() || !hook.nestedCallbackDeferred.Load() {
		t.Fatal("reentrant update did not commit before its deferred callback")
	}
}

func TestAuthHookContextExpiresAfterCallback(t *testing.T) {
	hook := &retainedContextHook{}
	mgr := NewManager(nil, nil, hook)
	auth, err := mgr.Register(t.Context(), &Auth{ID: "auth-1", Provider: "chatgpt-web"})
	if err != nil {
		t.Fatal(err)
	}
	auth.Label = "updated"
	if _, err = mgr.Update(t.Context(), auth); err != nil {
		t.Fatal(err)
	}
	if isReentrantAuthHookCall(hook.ctx, mgr, auth.ID) {
		t.Fatal("retained callback context remained reentrant")
	}
}

func TestUpdateMetadataPanickingHookDoesNotStrandConcurrentCallback(t *testing.T) {
	hook := &blockingPanickingMetadataHook{
		started:      make(chan struct{}),
		release:      make(chan struct{}),
		secondCalled: make(chan struct{}),
	}
	mgr := NewManager(nil, nil, hook)
	registered, err := mgr.Register(t.Context(), &Auth{ID: "auth-1", Provider: "chatgpt-web", Metadata: map[string]any{"cookie": "original"}})
	if err != nil {
		t.Fatal(err)
	}

	firstDone := make(chan any, 1)
	go func() {
		defer func() { firstDone <- recover() }()
		_, _ = mgr.UpdateMetadata(t.Context(), registered, map[string]any{"cookie": "rotated-1"})
	}()
	select {
	case <-hook.started:
	case <-time.After(5 * time.Second):
		t.Fatal("first hook did not start")
	}
	current, ok := mgr.GetByID(registered.ID)
	if !ok {
		t.Fatal("updated auth not found")
	}
	secondDone := make(chan error, 1)
	go func() {
		_, errUpdate := mgr.UpdateMetadata(t.Context(), current, map[string]any{"cookie": "rotated-2"})
		secondDone <- errUpdate
	}()
	select {
	case err = <-secondDone:
		t.Fatalf("second update returned before its hook completed: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(hook.release)
	select {
	case <-hook.secondCalled:
	case <-time.After(5 * time.Second):
		t.Fatal("queued hook callback was stranded")
	}
	select {
	case recovered := <-firstDone:
		if recovered != "first hook failed" {
			t.Fatalf("recovered panic = %v", recovered)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first update did not propagate hook panic")
	}
	select {
	case err = <-secondDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("second update did not return after its hook completed")
	}
	mgr.hookMu.Lock()
	queued := len(mgr.hookQueue)
	mgr.hookMu.Unlock()
	if queued != 0 {
		t.Fatalf("hook queues leaked: %d", queued)
	}
}

func TestCrossAuthReentrantHooksDoNotDeadlock(t *testing.T) {
	hook := &crossAuthReentrantHook{started: make(chan struct{}), release: make(chan struct{}), results: make(chan error, 2)}
	mgr := NewManager(nil, nil, hook)
	hook.mgr = mgr
	for _, id := range []string{"auth-1", "auth-2"} {
		if _, err := mgr.Register(t.Context(), &Auth{ID: id, Provider: "chatgpt-web", Metadata: map[string]any{"cookie": "original"}}); err != nil {
			t.Fatal(err)
		}
	}

	done := make(chan error, 2)
	for _, id := range []string{"auth-1", "auth-2"} {
		auth, ok := mgr.GetByID(id)
		if !ok {
			t.Fatalf("auth %q not found", id)
		}
		auth.Label = "outer"
		go func() {
			_, err := mgr.Update(t.Context(), auth)
			done <- err
		}()
	}
	select {
	case <-hook.started:
	case <-time.After(5 * time.Second):
		t.Fatal("cross-auth hooks did not start")
	}
	close(hook.release)
	for range 2 {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("cross-auth hook reentry deadlocked")
		}
	}
	for range 2 {
		select {
		case err := <-hook.results:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("nested cross-auth update did not return")
		}
	}
	mgr.hookMu.Lock()
	waits := len(mgr.hookWaits)
	mgr.hookMu.Unlock()
	if waits != 0 {
		t.Fatalf("hook wait edges leaked: %d", waits)
	}
}

func TestParallelHookWaitEdgesFromSameOriginPreserveCycleDetection(t *testing.T) {
	mgr := NewManager(nil, nil, nil)
	mgr.hookMu.Lock()
	for _, id := range []string{"auth-a", "auth-b", "auth-c"} {
		mgr.hookQueue[id] = &authHookQueue{dispatching: true}
	}
	mgr.hookMu.Unlock()
	stateA := &authHookContextState{}
	stateA.active.Store(true)
	ctxA := context.WithValue(t.Context(), authHookContextKey{}, authHookContext{manager: mgr, authID: "auth-a", state: stateA})
	tasks := make(chan *authHookTask, 2)
	for _, target := range []string{"auth-b", "auth-c"} {
		go func() {
			task, _ := mgr.enqueueAuthHook(ctxA, target, false, func(context.Context) {})
			tasks <- task
		}()
	}
	for range 2 {
		select {
		case task := <-tasks:
			if !task.wait || !task.waitEdge {
				t.Fatalf("parallel task did not retain its wait edge: %#v", task)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("parallel hook enqueue did not return")
		}
	}
	mgr.hookMu.Lock()
	targets := mgr.hookWaits["auth-a"]
	if targets["auth-b"] != 1 || targets["auth-c"] != 1 {
		mgr.hookMu.Unlock()
		t.Fatalf("parallel wait edges = %#v", targets)
	}
	mgr.hookMu.Unlock()

	stateB := &authHookContextState{}
	stateB.active.Store(true)
	ctxB := context.WithValue(t.Context(), authHookContextKey{}, authHookContext{manager: mgr, authID: "auth-b", state: stateB})
	cycleTask, _ := mgr.enqueueAuthHook(ctxB, "auth-a", false, func(context.Context) {})
	if cycleTask.wait || cycleTask.waitEdge {
		t.Fatal("parallel origin edge was ignored during cycle detection")
	}
}

func TestExecutionMetadataCommitSurvivesMarkResult(t *testing.T) {
	store := &countingStore{}
	mgr := NewManager(store, nil, nil)
	mgr.RegisterExecutor(&executionMetadataCommitExecutor{})
	const model = "gemini-test-model"
	auth := &Auth{ID: "auth-1", Provider: "gemini-cli", Metadata: map[string]any{"access_token": "original"}}
	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() { reg.UnregisterClient(auth.ID) })
	if _, err := mgr.Register(t.Context(), auth); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Execute(t.Context(), []string{auth.Provider}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{}); err != nil {
		t.Fatal(err)
	}
	current, ok := mgr.GetByID(auth.ID)
	if !ok || current.Metadata["access_token"] != "rotated" || current.Success != 1 {
		t.Fatalf("execution metadata commit was overwritten by result publication: %#v", current)
	}
	if saves := store.saveCount.Load(); saves != 3 {
		t.Fatalf("store saves = %d, want register, metadata, and result saves", saves)
	}
}

func TestVirtualExecutionMetadataCommitPersistsParent(t *testing.T) {
	store := &countingStore{}
	mgr := NewManager(store, nil, nil)
	mgr.RegisterExecutor(&executionMetadataCommitExecutor{})
	const model = "gemini-virtual-test-model"
	parent := &Auth{
		ID:       "auth-parent",
		Provider: "gemini-cli",
		Disabled: true,
		Status:   StatusDisabled,
		Metadata: map[string]any{"access_token": "original"},
	}
	virtual := &Auth{
		ID:       "auth-virtual",
		Provider: "gemini-cli",
		Attributes: map[string]string{
			"runtime_only":          "true",
			"gemini_virtual_parent": parent.ID,
		},
		Metadata: map[string]any{"access_token": "original"},
	}
	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(virtual.ID, virtual.Provider, []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() { reg.UnregisterClient(virtual.ID) })
	if _, err := mgr.Register(t.Context(), parent); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Register(t.Context(), virtual); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Execute(t.Context(), []string{virtual.Provider}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{}); err != nil {
		t.Fatal(err)
	}
	currentParent, ok := mgr.GetByID(parent.ID)
	if !ok || currentParent.Metadata["access_token"] != "rotated" {
		t.Fatalf("parent metadata was not updated: %#v", currentParent)
	}
	currentVirtual, ok := mgr.GetByID(virtual.ID)
	if !ok || currentVirtual.Success != 1 {
		t.Fatalf("virtual execution result was not published: %#v", currentVirtual)
	}
	if saves := store.saveCount.Load(); saves != 2 {
		t.Fatalf("store saves = %d, want parent register and metadata saves", saves)
	}
}

func TestSequentialCrossAuthReentrantHooksDoNotDeadlock(t *testing.T) {
	hook := &sequentialCrossAuthReentrantHook{}
	mgr := NewManager(nil, nil, hook)
	hook.mgr = mgr
	for _, id := range []string{"auth-1", "auth-2"} {
		if _, err := mgr.Register(t.Context(), &Auth{ID: id, Provider: "chatgpt-web", Metadata: map[string]any{"cookie": "original"}}); err != nil {
			t.Fatal(err)
		}
	}
	auth, _ := mgr.GetByID("auth-1")
	auth.Label = "outer"
	done := make(chan error, 1)
	go func() {
		_, err := mgr.Update(t.Context(), auth)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("sequential cross-auth hook reentry deadlocked")
	}
	if hook.err != nil {
		t.Fatal(hook.err)
	}
	mgr.hookMu.Lock()
	waits := len(mgr.hookWaits)
	mgr.hookMu.Unlock()
	if waits != 0 {
		t.Fatalf("hook wait edges leaked: %d", waits)
	}
}

func TestHookReentrancyDoesNotCrossManagers(t *testing.T) {
	targetHook := &crossManagerBlockingHook{
		started:      make(chan struct{}),
		release:      make(chan struct{}),
		nestedCalled: make(chan struct{}),
	}
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(targetHook.release) }) })
	targetManager := NewManager(nil, nil, targetHook)
	forwardingHook := &crossManagerForwardingHook{target: targetManager, result: make(chan error, 1)}
	sourceManager := NewManager(nil, nil, forwardingHook)
	for _, mgr := range []*Manager{sourceManager, targetManager} {
		if _, err := mgr.Register(t.Context(), &Auth{ID: "auth-1", Provider: "chatgpt-web"}); err != nil {
			t.Fatal(err)
		}
	}

	target, _ := targetManager.GetByID("auth-1")
	target.Label = "blocking"
	targetDone := make(chan error, 1)
	go func() {
		_, err := targetManager.Update(t.Context(), target)
		targetDone <- err
	}()
	select {
	case <-targetHook.started:
	case <-time.After(5 * time.Second):
		t.Fatal("target manager hook did not start")
	}
	source, _ := sourceManager.GetByID("auth-1")
	source.Label = "outer"
	sourceDone := make(chan error, 1)
	go func() {
		_, err := sourceManager.Update(t.Context(), source)
		sourceDone <- err
	}()
	select {
	case err := <-sourceDone:
		t.Fatalf("cross-manager update returned before its hook ran: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	releaseOnce.Do(func() { close(targetHook.release) })
	for name, done := range map[string]<-chan error{
		"source": sourceDone,
		"target": targetDone,
		"nested": forwardingHook.result,
	} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("%s update error: %v", name, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%s update did not finish", name)
		}
	}
	select {
	case <-targetHook.nestedCalled:
	case <-time.After(5 * time.Second):
		t.Fatal("cross-manager nested hook was not called")
	}
}

func TestRegisterAndUpdatePublishStoreNormalizedAuth(t *testing.T) {
	hook := &authSnapshotHook{}
	mgr := NewManager(&normalizingStore{}, nil, hook)
	registered, err := mgr.Register(t.Context(), &Auth{ID: "auth-1", Provider: "chatgpt-web", Metadata: map[string]any{"cookie": "original"}})
	if err != nil {
		t.Fatal(err)
	}
	hook.mu.Lock()
	registeredHook := hook.registered.Clone()
	hook.mu.Unlock()
	stored, ok := mgr.GetByID(registered.ID)
	if !ok || registered.Metadata["disabled"] != false || stored.Metadata["disabled"] != false || registeredHook.Metadata["disabled"] != false {
		t.Fatalf("normalized registered auth mismatch: returned=%#v stored=%#v hook=%#v", registered, stored, registeredHook)
	}

	registered.Disabled = true
	registered.Status = StatusDisabled
	updated, err := mgr.Update(t.Context(), registered)
	if err != nil {
		t.Fatal(err)
	}
	hook.mu.Lock()
	updatedHook := hook.updated.Clone()
	hook.mu.Unlock()
	stored, ok = mgr.GetByID(registered.ID)
	if !ok || updated.Metadata["disabled"] != true || stored.Metadata["disabled"] != true || updatedHook.Metadata["disabled"] != true {
		t.Fatalf("normalized updated auth mismatch: returned=%#v stored=%#v hook=%#v", updated, stored, updatedHook)
	}
}

func TestUpdateStoreNormalizationAdvancesMetadataGeneration(t *testing.T) {
	mgr := NewManager(&normalizingStore{}, nil, nil)
	registered, err := mgr.Register(t.Context(), &Auth{ID: "auth-1", Provider: "chatgpt-web", Metadata: map[string]any{"cookie": "original"}})
	if err != nil {
		t.Fatal(err)
	}
	stale := registered.Clone()
	registered.Disabled = true
	registered.Status = StatusDisabled
	updated, err := mgr.Update(t.Context(), registered)
	if err != nil {
		t.Fatal(err)
	}
	if updated.metadataVersion <= stale.metadataVersion {
		t.Fatalf("normalized update generation = %d, want greater than %d", updated.metadataVersion, stale.metadataVersion)
	}
	current, err := mgr.UpdateMetadata(t.Context(), stale, map[string]any{"cookie": "stale"})
	if !errors.Is(err, ErrStaleAuthMetadata) {
		t.Fatalf("stale update error = %v", err)
	}
	if current.Metadata["cookie"] != "original" || current.Metadata["disabled"] != true {
		t.Fatalf("stale update changed normalized auth: %#v", current.Metadata)
	}
}

func TestRegisterPersistsNonNilEmptyMetadata(t *testing.T) {
	store := &countingStore{}
	mgr := NewManager(store, nil, nil)
	registered, err := mgr.Register(t.Context(), &Auth{ID: "auth-1", Provider: "chatgpt-web", Metadata: map[string]any{}})
	if err != nil {
		t.Fatal(err)
	}
	if store.saveCount.Load() != 1 {
		t.Fatalf("Save count = %d, want 1", store.saveCount.Load())
	}
	if registered.Metadata == nil {
		t.Fatal("registered metadata became nil")
	}
	current, ok := mgr.GetByID(registered.ID)
	if !ok || current.Metadata == nil {
		t.Fatalf("published metadata = %#v", current)
	}
}

func TestRegisterAndUpdateDoNotPublishFailedStoreMutation(t *testing.T) {
	store := &normalizingStore{saveErr: errors.New("save failed")}
	hook := &authSnapshotHook{}
	mgr := NewManager(store, nil, hook)
	auth := &Auth{ID: "auth-1", Provider: "chatgpt-web", Metadata: map[string]any{"cookie": "original"}}
	if _, err := mgr.Register(t.Context(), auth); !errors.Is(err, store.saveErr) {
		t.Fatalf("Register error = %v", err)
	}
	if current, ok := mgr.GetByID(auth.ID); ok || current != nil {
		t.Fatalf("failed Register published auth: %#v", current)
	}
	hook.mu.Lock()
	registeredHook := hook.registered
	hook.mu.Unlock()
	if registeredHook != nil {
		t.Fatalf("failed Register invoked hook: %#v", registeredHook)
	}

	registered, err := mgr.Register(WithSkipPersist(t.Context()), auth)
	if err != nil {
		t.Fatal(err)
	}
	registered.Label = "changed"
	if _, err = mgr.Update(t.Context(), registered); !errors.Is(err, store.saveErr) {
		t.Fatalf("Update error = %v", err)
	}
	current, ok := mgr.GetByID(auth.ID)
	if !ok || current.Label != "" || current.Metadata["disabled"] != nil {
		t.Fatalf("failed Update changed manager auth: %#v", current)
	}
	hook.mu.Lock()
	updatedHook := hook.updated
	hook.mu.Unlock()
	if updatedHook != nil {
		t.Fatalf("failed Update invoked hook: %#v", updatedHook)
	}
}

func TestFailedStaleUpdateDoesNotAliasManagerMetadata(t *testing.T) {
	store := &normalizingStore{}
	mgr := NewManager(store, nil, nil)
	auth := &Auth{ID: "auth-1", Provider: "chatgpt-web", Metadata: map[string]any{"cookie": "original"}}
	registered, err := mgr.Register(t.Context(), auth)
	if err != nil {
		t.Fatal(err)
	}
	stale := registered.Clone()
	if _, err = mgr.UpdateMetadata(t.Context(), registered, map[string]any{"cookie": "rotated"}); err != nil {
		t.Fatal(err)
	}
	store.saveErr = errors.New("save failed")
	stale.Label = "replayed"
	if _, err = mgr.Update(t.Context(), stale); !errors.Is(err, store.saveErr) {
		t.Fatalf("stale Update error = %v", err)
	}
	stale.Metadata["cookie"] = "caller-mutated"
	current, ok := mgr.GetByID(auth.ID)
	if !ok || current.Metadata["cookie"] != "rotated" {
		t.Fatalf("failed stale Update aliased manager metadata: %#v", current)
	}
}

func TestUpdateMetadataQueuesAutoRefreshReschedule(t *testing.T) {
	mgr := NewManager(nil, nil, nil)
	registered, err := mgr.Register(t.Context(), &Auth{ID: "auth-1", Provider: "chatgpt-web", Metadata: map[string]any{"expires_at": time.Now().Add(time.Hour).Unix()}})
	if err != nil {
		t.Fatal(err)
	}
	loop := newAuthAutoRefreshLoop(mgr, time.Minute, 1)
	mgr.mu.Lock()
	mgr.refreshLoop = loop
	mgr.mu.Unlock()

	if _, err = mgr.UpdateMetadata(t.Context(), registered, map[string]any{"expires_at": time.Now().Add(30 * time.Minute).Unix()}); err != nil {
		t.Fatal(err)
	}
	loop.mu.Lock()
	_, queued := loop.dirty[registered.ID]
	loop.mu.Unlock()
	if !queued {
		t.Fatal("metadata update did not queue auto-refresh reschedule")
	}
}

func TestWithSkipPersist_DisablesUpdatePersistence(t *testing.T) {
	store := &countingStore{}
	mgr := NewManager(store, nil, nil)
	auth := &Auth{
		ID:       "auth-1",
		Provider: "antigravity",
		Metadata: map[string]any{"type": "antigravity"},
	}

	if _, err := mgr.Update(context.Background(), auth); err != nil {
		t.Fatalf("Update returned error: %v", err)
	}
	if got := store.saveCount.Load(); got != 1 {
		t.Fatalf("expected 1 Save call, got %d", got)
	}

	ctxSkip := WithSkipPersist(context.Background())
	if _, err := mgr.Update(ctxSkip, auth); err != nil {
		t.Fatalf("Update(skipPersist) returned error: %v", err)
	}
	if got := store.saveCount.Load(); got != 1 {
		t.Fatalf("expected Save call count to remain 1, got %d", got)
	}
}

func TestWithSkipPersist_DisablesRegisterPersistence(t *testing.T) {
	store := &countingStore{}
	mgr := NewManager(store, nil, nil)
	auth := &Auth{
		ID:       "auth-1",
		Provider: "antigravity",
		Metadata: map[string]any{"type": "antigravity"},
	}

	if _, err := mgr.Register(WithSkipPersist(context.Background()), auth); err != nil {
		t.Fatalf("Register(skipPersist) returned error: %v", err)
	}
	if got := store.saveCount.Load(); got != 0 {
		t.Fatalf("expected 0 Save calls, got %d", got)
	}
}

func TestWithSkipPersistDoesNotDisableUpdateMetadataPersistence(t *testing.T) {
	store := &countingStore{}
	mgr := NewManager(store, nil, nil)
	auth := &Auth{ID: "auth-1", Provider: "chatgpt-web", Metadata: map[string]any{"cookie": "original"}}
	registered, err := mgr.Register(WithSkipPersist(t.Context()), auth)
	if err != nil {
		t.Fatal(err)
	}
	updated, err := mgr.UpdateMetadata(WithSkipPersist(t.Context()), registered, map[string]any{"cookie": "rotated"})
	if err != nil {
		t.Fatal(err)
	}
	if got := store.saveCount.Load(); got != 1 {
		t.Fatalf("UpdateMetadata Save count = %d, want 1", got)
	}
	if updated.Metadata["cookie"] != "rotated" {
		t.Fatalf("UpdateMetadata result = %#v", updated.Metadata)
	}
}

func TestUpdateMetadataDoesNotPublishFailedPersistence(t *testing.T) {
	store := &countingStore{}
	mgr := NewManager(store, nil, nil)
	auth := &Auth{
		ID:       "auth-1",
		Provider: "chatgpt-web",
		Metadata: map[string]any{"cookie": "original"},
	}
	registered, err := mgr.Register(WithSkipPersist(t.Context()), auth)
	if err != nil {
		t.Fatal(err)
	}
	store.saveErr = errors.New("save failed")
	if _, err = mgr.UpdateMetadata(t.Context(), registered, map[string]any{"cookie": "rotated"}); !errors.Is(err, store.saveErr) {
		t.Fatalf("UpdateMetadata error = %v", err)
	}
	current, ok := mgr.GetByID(auth.ID)
	if !ok || current.Metadata["cookie"] != "original" {
		t.Fatalf("failed persistence changed manager auth: %#v", current)
	}
}

func TestUpdateMetadataDetachesNestedCallerValues(t *testing.T) {
	mgr := NewManager(nil, nil, nil)
	registered, err := mgr.Register(t.Context(), &Auth{ID: "auth-1", Provider: "chatgpt-web"})
	if err != nil {
		t.Fatal(err)
	}
	nested := map[string]any{"token": "rotated"}
	empty := map[string]any{}
	updated, err := mgr.UpdateMetadata(t.Context(), registered, map[string]any{"session": nested, "empty": empty})
	if err != nil {
		t.Fatal(err)
	}
	nested["token"] = "caller-mutated"
	empty["token"] = "caller-mutated"
	updated.Metadata["session"].(map[string]any)["token"] = "result-mutated"
	current, ok := mgr.GetByID(registered.ID)
	if !ok {
		t.Fatal("auth not found")
	}
	currentEmpty, emptyOK := current.Metadata["empty"].(map[string]any)
	if current.Metadata["session"].(map[string]any)["token"] != "rotated" || !emptyOK || len(currentEmpty) != 0 {
		t.Fatalf("nested caller metadata aliased manager state: %#v", current)
	}
}

func TestUpdateMetadataHonorsCallerCancellation(t *testing.T) {
	store := &countingStore{}
	mgr := NewManager(store, nil, nil)
	auth := &Auth{ID: "auth-1", Provider: "chatgpt-web", Metadata: map[string]any{"cookie": "original"}}
	registered, err := mgr.Register(WithSkipPersist(t.Context()), auth)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err = mgr.UpdateMetadata(ctx, registered, map[string]any{"cookie": "rotated"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("UpdateMetadata() error = %v, want canceled", err)
	}
	current, ok := mgr.GetByID(auth.ID)
	if !ok || current.Metadata["cookie"] != "original" || store.saveCount.Load() != 0 {
		t.Fatalf("canceled update changed credential state: auth=%#v saves=%d", current, store.saveCount.Load())
	}
}

func TestUpdateMetadataPersistenceWaitHonorsCallerCancellation(t *testing.T) {
	store := newBlockingStore()
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(store.release) }) })
	mgr := NewManager(store, nil, nil)
	auth := &Auth{ID: "auth-1", Provider: "chatgpt-web", Metadata: map[string]any{"cookie": "original"}}
	registered, err := mgr.Register(WithSkipPersist(t.Context()), auth)
	if err != nil {
		t.Fatal(err)
	}
	firstDone := make(chan error, 1)
	go func() {
		_, errUpdate := mgr.UpdateMetadata(t.Context(), registered, map[string]any{"cookie": "rotated-1"})
		firstDone <- errUpdate
	}()
	select {
	case <-store.started:
	case <-time.After(5 * time.Second):
		t.Fatal("first metadata persistence did not start")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	if _, err = mgr.UpdateMetadata(ctx, registered, map[string]any{"cookie": "rotated-2"}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked UpdateMetadata error = %v, want deadline exceeded", err)
	}
	releaseOnce.Do(func() { close(store.release) })
	select {
	case err = <-firstDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first metadata update did not finish")
	}
}

func TestPersistenceWaitDuringLoadHonorsCallerCancellation(t *testing.T) {
	store := &blockingListStore{
		items:   []*Auth{{ID: "auth-1", Provider: "chatgpt-web", Metadata: map[string]any{"cookie": "loaded"}}},
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(store.release) }) })
	mgr := NewManager(store, nil, nil)
	auth, err := mgr.Register(WithSkipPersist(t.Context()), &Auth{ID: "auth-1", Provider: "chatgpt-web", Metadata: map[string]any{"cookie": "original"}})
	if err != nil {
		t.Fatal(err)
	}
	loadDone := make(chan error, 1)
	go func() { loadDone <- mgr.Load(t.Context()) }()
	select {
	case <-store.started:
	case <-time.After(5 * time.Second):
		t.Fatal("load did not start")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	if _, err = mgr.UpdateMetadata(ctx, auth, map[string]any{"cookie": "blocked"}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked UpdateMetadata error = %v, want deadline exceeded", err)
	}
	mgr.persistByMu.Lock()
	locks := len(mgr.persistBy)
	mgr.persistByMu.Unlock()
	if locks != 0 {
		t.Fatalf("canceled persistence wait retained %d per-auth locks", locks)
	}
	releaseOnce.Do(func() { close(store.release) })
	select {
	case err = <-loadDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("load did not finish")
	}
}

func TestRegisterAndUpdatePersistenceWaitHonorCallerCancellation(t *testing.T) {
	store := newBlockingStore()
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(store.release) }) })
	mgr := NewManager(store, nil, nil)
	auth := &Auth{ID: "auth-1", Provider: "chatgpt-web", Metadata: map[string]any{"cookie": "original"}}
	registered, err := mgr.Register(WithSkipPersist(t.Context()), auth)
	if err != nil {
		t.Fatal(err)
	}
	metadataDone := make(chan error, 1)
	go func() {
		_, errUpdate := mgr.UpdateMetadata(t.Context(), registered, map[string]any{"cookie": "rotated"})
		metadataDone <- errUpdate
	}()
	select {
	case <-store.started:
	case <-time.After(5 * time.Second):
		t.Fatal("metadata persistence did not start")
	}

	tests := []struct {
		name   string
		invoke func(context.Context) error
	}{
		{
			name: "register",
			invoke: func(ctx context.Context) error {
				_, errRegister := mgr.Register(ctx, &Auth{ID: auth.ID, Provider: auth.Provider, Label: "replacement"})
				return errRegister
			},
		},
		{
			name: "update",
			invoke: func(ctx context.Context) error {
				candidate := registered.Clone()
				candidate.Label = "replacement"
				_, errUpdate := mgr.Update(ctx, candidate)
				return errUpdate
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
			defer cancel()
			if errInvoke := tc.invoke(ctx); !errors.Is(errInvoke, context.DeadlineExceeded) {
				t.Fatalf("persistence wait error = %v, want deadline exceeded", errInvoke)
			}
		})
	}

	releaseOnce.Do(func() { close(store.release) })
	select {
	case err = <-metadataDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("metadata update did not finish")
	}
	current, ok := mgr.GetByID(auth.ID)
	if !ok || current.Label != "" || current.Metadata["cookie"] != "rotated" {
		t.Fatalf("canceled persistence wait changed auth: %#v", current)
	}
}

func TestUpdateMetadataRejectsStaleCredentialRotation(t *testing.T) {
	mgr := NewManager(nil, nil, nil)
	auth := &Auth{ID: "auth-1", Provider: "chatgpt-web", Metadata: map[string]any{"cookie": "original"}}
	registered, err := mgr.Register(t.Context(), auth)
	if err != nil {
		t.Fatal(err)
	}
	stale := registered.Clone()
	current, err := mgr.UpdateMetadata(t.Context(), registered, map[string]any{"cookie": "newer"})
	if err != nil {
		t.Fatal(err)
	}
	current, err = mgr.UpdateMetadata(t.Context(), stale, map[string]any{"cookie": "stale"})
	if !errors.Is(err, ErrStaleAuthMetadata) {
		t.Fatalf("stale update error = %v", err)
	}
	if current.Metadata["cookie"] != "newer" {
		t.Fatalf("stale rotation replaced current credential: %#v", current.Metadata)
	}
}

func TestUpdateMetadataRejectsUnknownVersionStaleSnapshot(t *testing.T) {
	mgr := NewManager(nil, nil, nil)
	auth := &Auth{ID: "auth-1", Provider: "chatgpt-web", Metadata: map[string]any{"cookie": "current"}}
	if _, err := mgr.Register(t.Context(), auth); err != nil {
		t.Fatal(err)
	}
	stale := &Auth{ID: auth.ID, Provider: auth.Provider, Metadata: map[string]any{"cookie": "stale"}}
	current, err := mgr.UpdateMetadata(t.Context(), stale, map[string]any{"cookie": "rotated"})
	if !errors.Is(err, ErrStaleAuthMetadata) {
		t.Fatalf("unknown-version stale update error = %v", err)
	}
	if current.Metadata["cookie"] != "current" {
		t.Fatalf("unknown-version stale update changed credential: %#v", current.Metadata)
	}
}

func TestRegisterReplacementAdvancesMetadataGeneration(t *testing.T) {
	mgr := NewManager(nil, nil, nil)
	first, err := mgr.Register(t.Context(), &Auth{ID: "auth-1", Provider: "chatgpt-web", Metadata: map[string]any{"cookie": "first"}})
	if err != nil {
		t.Fatal(err)
	}
	second, err := mgr.Register(t.Context(), &Auth{ID: "auth-1", Provider: "chatgpt-web", Metadata: map[string]any{"cookie": "second"}})
	if err != nil {
		t.Fatal(err)
	}
	current, err := mgr.UpdateMetadata(t.Context(), first, map[string]any{"cookie": "stale"})
	if !errors.Is(err, ErrStaleAuthMetadata) {
		t.Fatalf("pre-replacement metadata update error = %v", err)
	}
	if current.Metadata["cookie"] != "second" || second.metadataVersion <= first.metadataVersion {
		t.Fatalf("replacement generation did not advance: first=%d second=%d current=%#v", first.metadataVersion, second.metadataVersion, current.Metadata)
	}
}

func TestRegisterRefreshesSuppliedMetadataGeneration(t *testing.T) {
	auth := &Auth{
		ID:                   "auth-1",
		Provider:             "chatgpt-web",
		Metadata:             map[string]any{"cookie": "persisted"},
		metadataVersion:      41,
		metadataVersionKnown: true,
		metadataSnapshot:     map[string]any{"cookie": "older"},
	}
	mgr := NewManager(nil, nil, nil)
	registered, err := mgr.Register(t.Context(), auth)
	if err != nil {
		t.Fatal(err)
	}
	if auth.metadataVersion != registered.metadataVersion || !auth.metadataVersionKnown || !reflect.DeepEqual(auth.metadataSnapshot, registered.Metadata) {
		t.Fatalf("supplied auth generation = %d/%t snapshot=%#v, registered = %d/%#v", auth.metadataVersion, auth.metadataVersionKnown, auth.metadataSnapshot, registered.metadataVersion, registered.Metadata)
	}
	updated, err := mgr.UpdateMetadata(t.Context(), auth, map[string]any{"cookie": "rotated"})
	if err != nil {
		t.Fatal(err)
	}
	if updated.Metadata["cookie"] != "rotated" {
		t.Fatalf("updated metadata = %#v", updated.Metadata)
	}
}

func TestUpdateTreatsPreLoadSnapshotAsStale(t *testing.T) {
	store := &loadingStore{}
	mgr := NewManager(store, nil, nil)
	preLoad, err := mgr.Register(WithSkipPersist(t.Context()), &Auth{ID: "auth-1", Provider: "chatgpt-web", Metadata: map[string]any{"cookie": "original"}})
	if err != nil {
		t.Fatal(err)
	}
	preLoadMetadata := preLoad.Clone()
	store.items = []*Auth{{ID: "auth-1", Provider: "chatgpt-web", Metadata: map[string]any{"cookie": "loaded"}}}
	if err = mgr.Load(t.Context()); err != nil {
		t.Fatal(err)
	}
	preLoad.Label = "management-edit"
	if _, err = mgr.Update(WithSkipPersist(t.Context()), preLoad); err != nil {
		t.Fatal(err)
	}
	current, ok := mgr.GetByID(preLoad.ID)
	if !ok || current.Label != "management-edit" || current.Metadata["cookie"] != "loaded" {
		t.Fatalf("pre-load snapshot replaced loaded metadata: %#v", current)
	}
	current, err = mgr.UpdateMetadata(t.Context(), preLoadMetadata, map[string]any{"cookie": "stale"})
	if !errors.Is(err, ErrStaleAuthMetadata) || current.Metadata["cookie"] != "loaded" {
		t.Fatalf("pre-load metadata update error = %v, current = %#v", err, current)
	}
}

type blockingStore struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
	saves   atomic.Int32
	mutate  func(*Auth)
	saveErr error
}

func newBlockingStore() *blockingStore {
	return &blockingStore{started: make(chan struct{}), release: make(chan struct{})}
}

func (s *blockingStore) List(context.Context) ([]*Auth, error) { return nil, nil }

func (s *blockingStore) Save(_ context.Context, auth *Auth) (string, error) {
	s.saves.Add(1)
	s.once.Do(func() {
		close(s.started)
		<-s.release
	})
	if s.mutate != nil {
		s.mutate(auth)
	}
	return "", s.saveErr
}

func (s *blockingStore) Delete(context.Context, string) error { return nil }

type authBlockingStore struct {
	blockedID string
	started   chan struct{}
	release   chan struct{}
	once      sync.Once
}

func (s *authBlockingStore) List(context.Context) ([]*Auth, error) { return nil, nil }

func (s *authBlockingStore) Save(_ context.Context, auth *Auth) (string, error) {
	if auth != nil && auth.ID == s.blockedID {
		s.once.Do(func() { close(s.started) })
		<-s.release
	}
	return auth.ID, nil
}

func (s *authBlockingStore) Delete(context.Context, string) error { return nil }

func TestMarkResultPersistenceDoesNotBlockOtherAuthRecords(t *testing.T) {
	store := &authBlockingStore{blockedID: "auth-1", started: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(store.release) }) })
	mgr := NewManager(store, nil, nil)
	for _, id := range []string{"auth-1", "auth-2"} {
		if _, err := mgr.Register(WithSkipPersist(t.Context()), &Auth{ID: id, Provider: "openai", Metadata: map[string]any{"type": "openai"}}); err != nil {
			t.Fatal(err)
		}
	}

	firstDone := make(chan struct{})
	go func() {
		mgr.MarkResult(t.Context(), Result{AuthID: "auth-1", Provider: "openai", Success: true})
		close(firstDone)
	}()
	select {
	case <-store.started:
	case <-time.After(5 * time.Second):
		t.Fatal("first auth persistence did not start")
	}
	secondDone := make(chan struct{})
	go func() {
		mgr.MarkResult(t.Context(), Result{AuthID: "auth-2", Provider: "openai", Success: true})
		close(secondDone)
	}()
	select {
	case <-secondDone:
	case <-time.After(time.Second):
		t.Fatal("first auth persistence blocked a different auth record")
	}
	releaseOnce.Do(func() { close(store.release) })
	select {
	case <-firstDone:
	case <-time.After(5 * time.Second):
		t.Fatal("first auth persistence did not finish")
	}
	mgr.persistByMu.Lock()
	defer mgr.persistByMu.Unlock()
	if len(mgr.persistBy) != 0 {
		t.Fatalf("idle persistence locks = %d, want 0", len(mgr.persistBy))
	}
}

func TestMarkResultPublishesOnlyPersistedStoreState(t *testing.T) {
	store := newBlockingStore()
	store.mutate = func(auth *Auth) {
		auth.Metadata["normalized"] = true
	}
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(store.release) }) })
	mgr := NewManager(store, nil, nil)
	if _, err := mgr.Register(WithSkipPersist(t.Context()), &Auth{ID: "auth-1", Provider: "openai", Metadata: map[string]any{"type": "openai"}}); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		mgr.MarkResult(t.Context(), Result{AuthID: "auth-1", Provider: "openai", Success: true})
		close(done)
	}()
	select {
	case <-store.started:
	case <-time.After(5 * time.Second):
		t.Fatal("result persistence did not start")
	}
	current, ok := mgr.GetByID("auth-1")
	if !ok || current.Success != 0 || current.Metadata["normalized"] != nil {
		t.Fatalf("unpersisted result was published: %#v", current)
	}
	releaseOnce.Do(func() { close(store.release) })
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("result persistence did not finish")
	}
	current, ok = mgr.GetByID("auth-1")
	if !ok || current.Success != 1 || current.Metadata["normalized"] != true {
		t.Fatalf("persisted normalized result was not published: %#v", current)
	}
}

func TestMarkResultAppliesInMemoryStateOnFailedPersistence(t *testing.T) {
	store := &normalizingStore{saveErr: errors.New("save failed")}
	mgr := NewManager(store, nil, nil)
	const model = "persist-failure-model"
	if _, err := mgr.Register(WithSkipPersist(t.Context()), &Auth{ID: "auth-1", Provider: "openai", Metadata: map[string]any{"type": "openai"}}); err != nil {
		t.Fatal(err)
	}
	reg := registry.GetGlobalRegistry()
	reg.RegisterClient("auth-1", "openai", []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() { reg.UnregisterClient("auth-1") })
	mgr.MarkResult(t.Context(), Result{AuthID: "auth-1", Provider: "openai", Model: model, Error: &Error{HTTPStatus: 401, Message: "unauthorized"}})
	current, ok := mgr.GetByID("auth-1")
	if !ok || current.Failed != 1 || current.ModelStates[model] == nil {
		t.Fatalf("failed result persistence dropped in-memory result state: %#v", current)
	}
	available := false
	for _, candidate := range reg.GetAvailableModels("openai") {
		if candidate["id"] == model {
			available = true
			break
		}
	}
	if available {
		t.Fatal("failed result persistence did not suspend the registry model")
	}
}

func TestReconcileRegistryModelStatesDoesNotPublishFailedPersistence(t *testing.T) {
	store := &normalizingStore{saveErr: errors.New("save failed")}
	mgr := NewManager(store, nil, nil)
	auth := &Auth{
		ID:       "auth-1",
		Provider: "openai",
		Metadata: map[string]any{"type": "openai"},
		Status:   StatusError,
		ModelStates: map[string]*ModelState{
			"model-1": {Status: StatusError, Unavailable: true},
		},
	}
	if _, err := mgr.Register(WithSkipPersist(t.Context()), auth); err != nil {
		t.Fatal(err)
	}
	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "model-1"}})
	t.Cleanup(func() { reg.UnregisterClient(auth.ID) })
	mgr.ReconcileRegistryModelStates(t.Context(), auth.ID)
	current, ok := mgr.GetByID(auth.ID)
	state := current.ModelStates["model-1"]
	if !ok || current.Status != StatusError || state == nil || state.Status != StatusError || !state.Unavailable {
		t.Fatalf("failed reconciliation persistence changed auth: %#v", current)
	}
}

func TestUpdateMetadataPersistenceDoesNotBlockManagerReads(t *testing.T) {
	store := newBlockingStore()
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(store.release) }) })
	mgr := NewManager(store, nil, nil)
	auth := &Auth{ID: "auth-1", Provider: "chatgpt-web", Metadata: map[string]any{"cookie": "original"}}
	registered, err := mgr.Register(WithSkipPersist(t.Context()), auth)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := mgr.UpdateMetadata(t.Context(), registered, map[string]any{"cookie": "rotated"})
		done <- err
	}()
	select {
	case <-store.started:
	case <-time.After(5 * time.Second):
		t.Fatal("metadata persistence did not start")
	}
	readDone := make(chan struct{})
	go func() {
		mgr.GetByID(auth.ID)
		close(readDone)
	}()
	select {
	case <-readDone:
	case <-time.After(time.Second):
		t.Fatal("metadata persistence blocked an unrelated manager read")
	}
	releaseOnce.Do(func() { close(store.release) })
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("UpdateMetadata() error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("metadata update did not finish")
	}
}

func TestUpdateMetadataPreservesRotationAcrossConcurrentManagerUpdate(t *testing.T) {
	store := newBlockingStore()
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(store.release) }) })
	mgr := NewManager(store, nil, nil)
	auth := &Auth{ID: "auth-1", Provider: "chatgpt-web", Metadata: map[string]any{"cookie": "original"}}
	if _, err := mgr.Register(WithSkipPersist(t.Context()), auth); err != nil {
		t.Fatal(err)
	}
	stale, ok := mgr.GetByID(auth.ID)
	if !ok {
		t.Fatal("registered auth not found")
	}
	rotationBase := stale.Clone()

	metadataDone := make(chan error, 1)
	go func() {
		_, err := mgr.UpdateMetadata(t.Context(), rotationBase, map[string]any{"cookie": "rotated"})
		metadataDone <- err
	}()
	select {
	case <-store.started:
	case <-time.After(5 * time.Second):
		t.Fatal("metadata persistence did not start")
	}
	stale.Label = "concurrent"
	stale.Metadata["note"] = "management-edit"
	updateDone := make(chan error, 1)
	go func() {
		_, err := mgr.Update(t.Context(), stale)
		updateDone <- err
	}()
	select {
	case <-updateDone:
		t.Fatal("concurrent manager update completed during metadata persistence")
	case <-time.After(100 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(store.release) })
	select {
	case err := <-metadataDone:
		if err != nil {
			t.Fatalf("metadata update error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("metadata update did not finish")
	}
	select {
	case err := <-updateDone:
		if err != nil {
			t.Fatalf("manager update error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("manager update did not finish")
	}
	current, ok := mgr.GetByID(auth.ID)
	if !ok || current.Label != "concurrent" || current.Metadata["cookie"] != "rotated" || current.Metadata["note"] != "management-edit" {
		t.Fatalf("concurrent manager update lost rotated metadata: %#v", current)
	}
	if got := store.saves.Load(); got != 2 {
		t.Fatalf("Save count = %d, want 2", got)
	}
}

func TestUpdatePreservesConcurrentMetadataChangeWhenStaleInputOmitsKey(t *testing.T) {
	mgr := NewManager(nil, nil, nil)
	registered, err := mgr.Register(t.Context(), &Auth{ID: "auth-1", Provider: "chatgpt-web", Metadata: map[string]any{"cookie": "original"}})
	if err != nil {
		t.Fatal(err)
	}
	stale := registered.Clone()
	delete(stale.Metadata, "cookie")
	if _, err = mgr.UpdateMetadata(t.Context(), registered, map[string]any{"cookie": "rotated"}); err != nil {
		t.Fatal(err)
	}
	stale.Label = "management-edit"
	if _, err = mgr.Update(t.Context(), stale); err != nil {
		t.Fatal(err)
	}
	current, ok := mgr.GetByID(registered.ID)
	if !ok || current.Label != "management-edit" || current.Metadata["cookie"] != "rotated" {
		t.Fatalf("stale omission removed concurrent metadata change: %#v", current)
	}
}

func TestUpdateAcceptsExternalMetadataAfterRuntimeRotation(t *testing.T) {
	mgr := NewManager(nil, nil, nil)
	auth := &Auth{ID: "auth-1", Provider: "chatgpt-web", Metadata: map[string]any{"cookie": "original"}}
	registered, err := mgr.Register(t.Context(), auth)
	if err != nil {
		t.Fatal(err)
	}
	rotated, err := mgr.UpdateMetadata(t.Context(), registered, map[string]any{"cookie": "rotated"})
	if err != nil {
		t.Fatal(err)
	}
	external := &Auth{
		ID:        auth.ID,
		Provider:  auth.Provider,
		Metadata:  map[string]any{"cookie": "operator-value"},
		UpdatedAt: rotated.UpdatedAt.Add(-time.Minute),
	}
	if _, err := mgr.Update(WithSkipPersist(t.Context()), external); err != nil {
		t.Fatal(err)
	}
	current, ok := mgr.GetByID(auth.ID)
	if !ok || current.Metadata["cookie"] != "operator-value" {
		t.Fatalf("external metadata update was not authoritative: %#v", current)
	}
}

func TestUpdateMetadataAcceptsNextRotationAfterWatcherReplay(t *testing.T) {
	mgr := NewManager(nil, nil, nil)
	auth := &Auth{ID: "auth-1", Provider: "chatgpt-web", Metadata: map[string]any{"cookie": "original"}}
	registered, err := mgr.Register(t.Context(), auth)
	if err != nil {
		t.Fatal(err)
	}
	rotated, err := mgr.UpdateMetadata(t.Context(), registered, map[string]any{"cookie": "rotated-1"})
	if err != nil {
		t.Fatal(err)
	}
	replayed := &Auth{ID: auth.ID, Provider: auth.Provider, Metadata: map[string]any{"cookie": "rotated-1"}}
	if _, err = mgr.Update(WithWatcherReplay(t.Context()), replayed); err != nil {
		t.Fatal(err)
	}
	current, err := mgr.UpdateMetadata(t.Context(), rotated, map[string]any{"cookie": "rotated-2"})
	if err != nil {
		t.Fatal(err)
	}
	if current.Metadata["cookie"] != "rotated-2" {
		t.Fatalf("watcher replay rejected next credential rotation: %#v", current.Metadata)
	}
	staleReplay := &Auth{
		ID:        auth.ID,
		Provider:  auth.Provider,
		Metadata:  map[string]any{"cookie": "rotated-1"},
		UpdatedAt: current.UpdatedAt.Add(-time.Nanosecond),
	}
	current, err = mgr.Update(WithWatcherReplay(t.Context()), staleReplay)
	if err != nil {
		t.Fatal(err)
	}
	if current.Metadata["cookie"] != "rotated-2" {
		t.Fatalf("stale watcher replay replaced newer credential rotation: %#v", current.Metadata)
	}
	sameTimestampEdit := &Auth{
		ID:        auth.ID,
		Provider:  auth.Provider,
		Metadata:  map[string]any{"cookie": "operator-same-time"},
		UpdatedAt: current.UpdatedAt,
	}
	current, err = mgr.Update(WithWatcherReplay(t.Context()), sameTimestampEdit)
	if err != nil {
		t.Fatal(err)
	}
	if current.Metadata["cookie"] != "operator-same-time" {
		t.Fatalf("same-time watcher edit was not authoritative: %#v", current.Metadata)
	}
	zeroTimestampReplay := &Auth{
		ID:       auth.ID,
		Provider: auth.Provider,
		Metadata: map[string]any{"cookie": "operator-zero-time"},
	}
	current, err = mgr.Update(WithWatcherReplay(t.Context()), zeroTimestampReplay)
	if err != nil {
		t.Fatal(err)
	}
	if current.Metadata["cookie"] != "operator-zero-time" {
		t.Fatalf("timestamp-less watcher edit was not authoritative: %#v", current.Metadata)
	}
}

func TestWatcherEditIsNotOrderedByResultTimestamp(t *testing.T) {
	mgr := NewManager(nil, nil, nil)
	registered, err := mgr.Register(t.Context(), &Auth{
		ID:        "auth-1",
		Provider:  "chatgpt-web",
		Metadata:  map[string]any{"cookie": "original"},
		UpdatedAt: time.Now().Add(-time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	rotated, err := mgr.UpdateMetadata(t.Context(), registered, map[string]any{"cookie": "rotated"})
	if err != nil {
		t.Fatal(err)
	}
	operatorEdit := &Auth{
		ID:        registered.ID,
		Provider:  registered.Provider,
		Metadata:  map[string]any{"cookie": "operator"},
		UpdatedAt: rotated.metadataUpdatedAt.Add(time.Nanosecond),
	}
	for !time.Now().After(operatorEdit.UpdatedAt) {
		time.Sleep(time.Nanosecond)
	}
	mgr.MarkResult(t.Context(), Result{AuthID: registered.ID, Provider: registered.Provider, Success: true})
	current, ok := mgr.GetByID(registered.ID)
	if !ok || !current.UpdatedAt.After(operatorEdit.UpdatedAt) {
		t.Fatalf("result timestamp = %v, operator timestamp = %v", current.UpdatedAt, operatorEdit.UpdatedAt)
	}
	current, err = mgr.Update(WithWatcherReplay(t.Context()), operatorEdit)
	if err != nil {
		t.Fatal(err)
	}
	if current.Metadata["cookie"] != "operator" {
		t.Fatalf("operator watcher edit was replaced: %#v", current.Metadata)
	}
}

func TestUpdateMetadataSerializesStoreReplacement(t *testing.T) {
	oldStore := newBlockingStore()
	newStore := &countingStore{}
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(oldStore.release) }) })
	mgr := NewManager(oldStore, nil, nil)
	auth := &Auth{ID: "auth-1", Provider: "chatgpt-web", Metadata: map[string]any{"cookie": "original"}}
	registered, err := mgr.Register(WithSkipPersist(t.Context()), auth)
	if err != nil {
		t.Fatal(err)
	}

	updateDone := make(chan error, 1)
	go func() {
		_, err := mgr.UpdateMetadata(t.Context(), registered, map[string]any{"cookie": "rotated"})
		updateDone <- err
	}()
	select {
	case <-oldStore.started:
	case <-time.After(5 * time.Second):
		t.Fatal("metadata persistence did not start")
	}
	storeDone := make(chan struct{})
	go func() {
		mgr.SetStore(newStore)
		close(storeDone)
	}()
	select {
	case <-storeDone:
		t.Fatal("store replacement completed during an in-flight save")
	case <-time.After(100 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(oldStore.release) })
	select {
	case err := <-updateDone:
		if err != nil {
			t.Fatalf("UpdateMetadata() error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("metadata update did not finish")
	}
	select {
	case <-storeDone:
	case <-time.After(5 * time.Second):
		t.Fatal("store replacement did not finish")
	}
	registered, ok := mgr.GetByID(auth.ID)
	if !ok {
		t.Fatal("registered auth not found")
	}
	if _, err = mgr.UpdateMetadata(t.Context(), registered, map[string]any{"cookie": "new-store"}); err != nil {
		t.Fatalf("UpdateMetadata() with replacement store error = %v", err)
	}
	if got := newStore.saveCount.Load(); got != 1 {
		t.Fatalf("replacement store Save count = %d, want 1", got)
	}
}
