package amp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
)

type neoOrbFakeProvider struct {
	mu             sync.Mutex
	calls          []string
	execHandler    func(cmd []string) neoOrbExecResult
	inspectState   neoOrbContainerState
	containers     []neoOrbContainerSummary
	copiedFiles    map[string][]byte
	pingErr        error
	listErr        error
	inspectErr     error
	removeErr      error
	removeStart    chan struct{}
	removeResume   chan struct{}
	startHandler   func(string) error
	pauseStart     chan struct{}
	pauseResume    chan struct{}
	detachedStart  chan struct{}
	detachedResume chan struct{}
	detachedErr    error
}

func (f *neoOrbFakeProvider) record(call string) {
	f.mu.Lock()
	f.calls = append(f.calls, call)
	f.mu.Unlock()
}

func (f *neoOrbFakeProvider) Ping(ctx context.Context) error {
	f.record("ping")
	return f.pingErr
}

func (f *neoOrbFakeProvider) EnsureImage(ctx context.Context, image string) error {
	f.record("ensure-image:" + image)
	return nil
}

func (f *neoOrbFakeProvider) CreateContainer(ctx context.Context, spec neoOrbContainerSpec) (string, error) {
	f.record("create:" + spec.Name + ":" + spec.Image)
	f.inspectState = neoOrbContainerState{Exists: true, Running: true, IPAddress: "172.17.0.8"}
	return "container-fake", nil
}

func (f *neoOrbFakeProvider) ListOrbContainers(ctx context.Context) ([]neoOrbContainerSummary, error) {
	f.record("list-orbs")
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]neoOrbContainerSummary(nil), f.containers...), f.listErr
}

func (f *neoOrbFakeProvider) StartContainer(ctx context.Context, id string) error {
	f.record("start:" + id)
	f.mu.Lock()
	handler := f.startHandler
	f.mu.Unlock()
	if handler != nil {
		return handler(id)
	}
	return nil
}

func (f *neoOrbFakeProvider) PauseContainer(ctx context.Context, id string) error {
	f.record("pause:" + id)
	f.mu.Lock()
	started := f.pauseStart
	resume := f.pauseResume
	f.mu.Unlock()
	if started != nil {
		select {
		case started <- struct{}{}:
		default:
		}
	}
	if resume != nil {
		select {
		case <-resume:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	f.mu.Lock()
	f.inspectState.Paused = true
	f.mu.Unlock()
	return nil
}

func (f *neoOrbFakeProvider) UnpauseContainer(ctx context.Context, id string) error {
	f.record("unpause:" + id)
	f.mu.Lock()
	f.inspectState.Paused = false
	f.mu.Unlock()
	return nil
}

func (f *neoOrbFakeProvider) RemoveContainer(ctx context.Context, id string, force bool) error {
	f.record("remove:" + id)
	f.mu.Lock()
	err := f.removeErr
	started := f.removeStart
	resume := f.removeResume
	f.mu.Unlock()
	if started != nil {
		select {
		case started <- struct{}{}:
		default:
		}
	}
	if resume != nil {
		select {
		case <-resume:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return err
}

func (f *neoOrbFakeProvider) InspectContainer(ctx context.Context, id string) (neoOrbContainerState, error) {
	f.record("inspect:" + id)
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.inspectState, f.inspectErr
}

func (f *neoOrbFakeProvider) Exec(ctx context.Context, id string, cmd []string, env []string, workDir string) (neoOrbExecResult, error) {
	joined := strings.Join(cmd, " ")
	f.record("exec:" + joined)
	if joined == "dpkg --print-architecture" {
		return neoOrbExecResult{ExitCode: 0, Stdout: "amd64\n"}, nil
	}
	if f.execHandler != nil {
		return f.execHandler(cmd), nil
	}
	return neoOrbExecResult{ExitCode: 0}, nil
}

func (f *neoOrbFakeProvider) ExecDetached(ctx context.Context, id string, cmd []string, env []string, workDir string) error {
	f.record("exec-detached:" + strings.Join(cmd, " ") + ":cwd=" + workDir)
	f.mu.Lock()
	started := f.detachedStart
	resume := f.detachedResume
	err := f.detachedErr
	f.mu.Unlock()
	if started != nil {
		select {
		case started <- struct{}{}:
		default:
		}
	}
	if resume != nil {
		select {
		case <-resume:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return err
}

func (f *neoOrbFakeProvider) CopyFileToContainer(ctx context.Context, id, destPath string, content []byte, mode int64) error {
	f.record(fmt.Sprintf("copy:%s:%d:%d", destPath, len(content), mode))
	f.mu.Lock()
	if f.copiedFiles == nil {
		f.copiedFiles = map[string][]byte{}
	}
	f.copiedFiles[destPath] = append([]byte(nil), content...)
	f.mu.Unlock()
	return nil
}

func (f *neoOrbFakeProvider) CopyTarToContainer(ctx context.Context, id, destDir string, files map[string][]byte, mode int64) error {
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	f.record(fmt.Sprintf("copy-tar:%s:%s", destDir, strings.Join(names, ",")))
	return nil
}

func (f *neoOrbFakeProvider) callCount(prefix string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	count := 0
	for _, call := range f.calls {
		if strings.HasPrefix(call, prefix) {
			count++
		}
	}
	return count
}

func (f *neoOrbFakeProvider) callIndex(prefix string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	for index, call := range f.calls {
		if strings.HasPrefix(call, prefix) {
			return index
		}
	}
	return -1
}

func newNeoOrbTestRuntime(t *testing.T) (*neoRuntime, *neoOrbFakeProvider) {
	t.Helper()
	useTempNeoThreadStore(t)
	enabled := true
	rt := newNeoRuntime(&config.Config{
		AmpCode: config.AmpCode{
			Orbs: config.AmpOrbs{
				Enabled:             &enabled,
				Provider:            "docker",
				AutoPauseSeconds:    1,
				SetupTimeoutSeconds: 30,
			},
			NeoLocalRuntime: config.AmpNeoLocalRuntime{
				ExecutorConnectTimeoutSeconds: 1,
			},
		},
	})
	fake := &neoOrbFakeProvider{}
	manager := newNeoOrbManager(rt)
	manager.newClient = func(host string) (neoOrbProviderClient, error) { return fake, nil }
	manager.client = fake
	rt.orbManager = manager
	return rt, fake
}

func neoOrbTestActor(rt *neoRuntime, threadID string) *neoActor {
	actor := newNeoActor(rt, "actor-"+threadID, "thread-actor", threadID, threadID, neoActorRecord("actor-"+threadID, "thread-actor", threadID), nil)
	actor.bootstrapExecutorType = "sandbox"
	rt.store.mu.Lock()
	rt.store.actors[threadID] = actor
	rt.store.mu.Unlock()
	return actor
}

func waitNeoOrbState(t *testing.T, manager *neoOrbManager, threadID, want string) neoOrbRecord {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		record, ok := manager.snapshot(threadID)
		if ok && record.state == want {
			return record
		}
		time.Sleep(10 * time.Millisecond)
	}
	record, _ := manager.snapshot(threadID)
	t.Fatalf("orb state = %v, want %s", record, want)
	return neoOrbRecord{}
}

func TestNeoOrbSpawnProvisionsContainerAndStartsExecutor(t *testing.T) {
	rt, fake := newNeoOrbTestRuntime(t)
	threadID := "T-019fdec9-b0cf-745d-8da4-f250184e870e"
	actor := neoOrbTestActor(rt, threadID)
	t.Cleanup(actor.cancel)
	fake.startHandler = func(id string) error {
		raw, err := os.ReadFile(filepath.Join(rt.threadDir, threadID+".json"))
		if err != nil {
			return err
		}
		var thread map[string]any
		if err := json.Unmarshal(raw, &thread); err != nil {
			return err
		}
		if binding := strings.TrimSpace(stringValue(mapValue(thread["meta"])[neoOrbContainerIDMetaKey])); binding != id {
			return fmt.Errorf("persisted binding = %q, want %q", binding, id)
		}
		return nil
	}

	actor.bootstrapExecutorType = "sandbox"
	result := actor.spawnExecutor(map[string]any{"requestId": "spawn-orb-1", "repositoryURL": "https://example.test/repo.git"})
	if stringValue(result["status"]) == "failed" {
		t.Fatalf("spawn failed: %#v", result)
	}
	manager := rt.orbManagerFor()
	record := waitNeoOrbState(t, manager, threadID, neoOrbStateRunning)
	if record.containerID != "container-fake" {
		t.Fatalf("containerID = %q", record.containerID)
	}
	for _, want := range []string{"ping", "ensure-image:debian:12-slim", "create:cliproxy-orb-t-019fdec9-b0cf-745d-8da4-f250184e870e:debian:12-slim", "start:container-fake"} {
		if fake.callCount(want) != 1 {
			t.Fatalf("missing provider call %q in %#v", want, fake.calls)
		}
	}
	if fake.callCount("exec-detached:/usr/bin/flock -n /run/cliproxy-amp-executor.lock /usr/local/bin/amp") != 1 {
		t.Fatalf("executor was not started detached: %#v", fake.calls)
	}
	if fake.callCount("exec:git clone --depth 1 https://example.test/repo.git") != 1 {
		t.Fatalf("workspace clone missing: %#v", fake.calls)
	}
	if binding := readNeoOrbPersistedBinding(t, rt, threadID); binding != "container-fake" {
		t.Fatalf("persisted binding = %q", binding)
	}
}

func TestNeoOrbSpawnRejectedWhenDisabled(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	threadID := "T-019fdec9-b0cf-745d-8da4-f250184e870e"
	actor := neoOrbTestActor(rt, threadID)
	t.Cleanup(actor.cancel)
	result := actor.spawnExecutor(map[string]any{"requestId": "spawn-orb-off"})
	if stringValue(result["status"]) != "failed" {
		t.Fatalf("disabled orbs spawn = %#v, want failed", result)
	}
}

func TestNeoOrbSpawnResumesPausedOrb(t *testing.T) {
	rt, fake := newNeoOrbTestRuntime(t)
	threadID := "T-019fdec9-b0cf-745d-8da4-f250184e870e"
	actor := neoOrbTestActor(rt, threadID)
	t.Cleanup(actor.cancel)
	manager := rt.orbManagerFor()

	actor.spawnExecutor(map[string]any{"requestId": "spawn-first"})
	waitNeoOrbState(t, manager, threadID, neoOrbStateRunning)

	manager.setState(manager.live(threadID), neoOrbStatePaused, "")
	fake.mu.Lock()
	fake.inspectState.Paused = true
	fake.mu.Unlock()

	actor.spawnExecutor(map[string]any{"requestId": "spawn-resume"})
	waitNeoOrbState(t, manager, threadID, neoOrbStateRunning)
	if fake.callCount("unpause:container-fake") != 1 {
		t.Fatalf("paused orb was not unpaused: %#v", fake.calls)
	}
	if fake.callCount("exec-detached:/usr/bin/flock -n /run/cliproxy-amp-executor.lock /usr/local/bin/amp") != 2 {
		t.Fatalf("resume did not restart the executor: %#v", fake.calls)
	}
	if fake.callCount("create:") != 1 {
		t.Fatalf("resume unexpectedly re-created the container: %#v", fake.calls)
	}
}

func TestNeoOrbSpawnReprovisionsWhenContainerMissing(t *testing.T) {
	rt, fake := newNeoOrbTestRuntime(t)
	threadID := "T-019fdec9-b0cf-745d-8da4-f250184e870e"
	actor := neoOrbTestActor(rt, threadID)
	t.Cleanup(actor.cancel)
	manager := rt.orbManagerFor()

	actor.spawnExecutor(map[string]any{"requestId": "spawn-first"})
	waitNeoOrbState(t, manager, threadID, neoOrbStateRunning)
	manager.mu.Lock()
	manager.orbs[threadID].state = neoOrbStatePaused
	manager.mu.Unlock()
	fake.mu.Lock()
	fake.inspectState = neoOrbContainerState{Exists: false}
	fake.mu.Unlock()

	actor.spawnExecutor(map[string]any{"requestId": "spawn-reprovision"})
	waitNeoOrbState(t, manager, threadID, neoOrbStateRunning)
	if fake.callCount("create:") != 2 {
		t.Fatalf("missing container was not re-provisioned: %#v", fake.calls)
	}
}

func writeNeoOrbPersistedThread(t *testing.T, rt *neoRuntime, threadID, executorType string, containerID ...string) {
	t.Helper()
	meta := map[string]any{
		"cliProxyAPILocalNeo": true,
		"executorType":        executorType,
	}
	if len(containerID) > 0 && strings.TrimSpace(containerID[0]) != "" {
		meta[neoOrbContainerIDMetaKey] = containerID[0]
	}
	if _, err := writeNeoLocalThreadFileInDir(rt.threadDir, threadID, map[string]any{
		"id":   threadID,
		"meta": meta,
	}); err != nil {
		t.Fatalf("write persisted orb thread: %v", err)
	}
}

func readNeoOrbPersistedBinding(t *testing.T, rt *neoRuntime, threadID string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(rt.threadDir, threadID+".json"))
	if err != nil {
		t.Fatalf("read persisted orb thread: %v", err)
	}
	var thread map[string]any
	if err := json.Unmarshal(raw, &thread); err != nil {
		t.Fatalf("decode persisted orb thread: %v", err)
	}
	return strings.TrimSpace(stringValue(mapValue(thread["meta"])[neoOrbContainerIDMetaKey]))
}

func TestNeoOrbRestartRecoversRunningAndPausedContainers(t *testing.T) {
	for _, test := range []struct {
		name      string
		state     neoOrbContainerState
		wantState string
	}{
		{name: "running", state: neoOrbContainerState{Exists: true, Running: true}, wantState: neoOrbStateRunning},
		{name: "paused", state: neoOrbContainerState{Exists: true, Running: true, Paused: true}, wantState: neoOrbStatePaused},
	} {
		t.Run(test.name, func(t *testing.T) {
			rt, fake := newNeoOrbTestRuntime(t)
			threadID := "T-019fdec9-b0cf-745d-8da4-f250184e870e"
			writeNeoOrbPersistedThread(t, rt, threadID, "sandbox", "recovered-container")
			fake.containers = []neoOrbContainerSummary{{ID: "recovered-container", Labels: map[string]string{"cliproxy.orb": threadID}}}
			fake.inspectState = test.state
			manager := newNeoOrbManager(rt)
			manager.newClient = func(host string) (neoOrbProviderClient, error) { return fake, nil }
			manager.client = fake
			rt.orbManager = manager

			if err := manager.ensureRecovered(rt.configSnapshot()); err != nil {
				t.Fatalf("ensureRecovered: %v", err)
			}
			record, ok := manager.snapshot(threadID)
			if !ok || record.containerID != "recovered-container" || record.state != test.wantState {
				t.Fatalf("recovered record = %#v, %t", record, ok)
			}
			for _, forbidden := range []string{"create:", "start:", "unpause:", "exec-detached:"} {
				if fake.callCount(forbidden) != 0 {
					t.Fatalf("recovery performed lifecycle action %q: %#v", forbidden, fake.calls)
				}
			}
			if err := manager.ensureRecovered(rt.configSnapshot()); err != nil || fake.callCount("list-orbs") != 1 {
				t.Fatalf("repeated recovery = %v, calls %#v", err, fake.calls)
			}
		})
	}
}

func TestNeoOrbRecoveryRequiresExactPersistedContainerBinding(t *testing.T) {
	for _, test := range []struct {
		name      string
		binding   string
		container string
	}{
		{name: "missing binding", container: "labelled-container"},
		{name: "mismatched binding", binding: "persisted-container", container: "labelled-container"},
	} {
		t.Run(test.name, func(t *testing.T) {
			rt, fake := newNeoOrbTestRuntime(t)
			threadID := "T-019fdec9-b0cf-745d-8da4-f250184e870e"
			writeNeoOrbPersistedThread(t, rt, threadID, "sandbox", test.binding)
			fake.containers = []neoOrbContainerSummary{{ID: test.container, Labels: map[string]string{"cliproxy.orb": threadID}}}
			manager := newNeoOrbManager(rt)
			manager.newClient = func(host string) (neoOrbProviderClient, error) { return fake, nil }
			manager.client = fake

			if err := manager.ensureRecovered(rt.configSnapshot()); err != nil {
				t.Fatalf("ensureRecovered: %v", err)
			}
			record, ok := manager.snapshot(threadID)
			if !ok || record.state != neoOrbStateConflict || record.containerID != "" {
				t.Fatalf("recovery conflict = %#v, %t", record, ok)
			}
			if fake.callCount("inspect:") != 0 {
				t.Fatalf("unbound container was inspected: %#v", fake.calls)
			}
		})
	}
}

func TestNeoOrbRecoveredExecutorCannotReconnectBeforeMigration(t *testing.T) {
	rt, fake := newNeoOrbTestRuntime(t)
	threadID := "T-019fdec9-b0cf-745d-8da4-f250184e870e"
	writeNeoOrbPersistedThread(t, rt, threadID, "sandbox", "recovered-container")
	fake.containers = []neoOrbContainerSummary{{ID: "recovered-container", Labels: map[string]string{"cliproxy.orb": threadID}}}
	fake.inspectState = neoOrbContainerState{Exists: true, Running: true}
	manager := newNeoOrbManager(rt)
	manager.newClient = func(host string) (neoOrbProviderClient, error) { return fake, nil }
	manager.client = fake
	rt.orbManager = manager
	actor := neoOrbTestActor(rt, threadID)
	t.Cleanup(actor.cancel)
	var messages []map[string]any
	socket := &neoSocket{writeMessage: func(_ int, data []byte) error {
		var message map[string]any
		if err := json.Unmarshal(data, &message); err != nil {
			return err
		}
		messages = append(messages, message)
		return nil
	}}
	socket.markExecutor("stale-orb-executor")

	actor.executorConnectedForSocket(socket, map[string]any{"executorId": "stale-orb-executor", "executorType": "sandbox"})
	actor.mu.Lock()
	executorID := actor.executorID
	ready := actor.executorReady
	actor.mu.Unlock()
	if executorID != "" || ready || socket.isExecutor() {
		t.Fatalf("unmigrated recovered executor was accepted: id=%q ready=%v", executorID, ready)
	}
	if len(messages) != 1 || stringValue(messages[0]["code"]) != "EXECUTOR_MIGRATION_REQUIRED" {
		t.Fatalf("migration rejection messages = %#v", messages)
	}

	manager.mu.Lock()
	manager.orbs[threadID].migrationReady = true
	manager.mu.Unlock()
	actor.executorConnectedForSocket(socket, map[string]any{"executorId": "migrated-orb-executor", "executorType": "sandbox"})
	actor.mu.Lock()
	executorID = actor.executorID
	ready = actor.executorReady
	actor.mu.Unlock()
	if executorID != "migrated-orb-executor" || !ready || !socket.isExecutor() {
		t.Fatalf("migrated recovered executor was rejected: id=%q ready=%v", executorID, ready)
	}
}

func TestNeoOrbRecoveryFailureRejectsExecutorAdmission(t *testing.T) {
	rt, fake := newNeoOrbTestRuntime(t)
	threadID := "T-019fdec9-b0cf-745d-8da4-f250184e870e"
	actor := neoOrbTestActor(rt, threadID)
	t.Cleanup(actor.cancel)
	fake.listErr = errors.New("docker unavailable")
	manager := newNeoOrbManager(rt)
	manager.newClient = func(host string) (neoOrbProviderClient, error) { return fake, nil }
	manager.client = fake
	rt.orbManager = manager
	var messages []map[string]any
	socket := &neoSocket{writeMessage: func(_ int, data []byte) error {
		var message map[string]any
		if err := json.Unmarshal(data, &message); err != nil {
			return err
		}
		messages = append(messages, message)
		return nil
	}}
	socket.markExecutor("unreconciled-orb-executor")

	actor.executorConnectedForSocket(socket, map[string]any{"executorId": "unreconciled-orb-executor", "executorType": "sandbox"})
	actor.mu.Lock()
	executorID := actor.executorID
	ready := actor.executorReady
	actor.mu.Unlock()
	if executorID != "" || ready || socket.isExecutor() {
		t.Fatalf("executor was accepted before Docker reconciliation: id=%q ready=%v", executorID, ready)
	}
	if len(messages) != 1 || stringValue(messages[0]["code"]) != "EXECUTOR_RECOVERY_UNAVAILABLE" {
		t.Fatalf("reconciliation rejection messages = %#v", messages)
	}
}

func TestNeoOrbRecoveryRejectsDockerHostChangesUntilRestart(t *testing.T) {
	useTempNeoThreadStore(t)
	enabled := true
	cfg := &config.Config{AmpCode: config.AmpCode{Orbs: config.AmpOrbs{
		Enabled:    &enabled,
		Provider:   "docker",
		DockerHost: "tcp://host-a:2375",
	}}}
	rt := newNeoRuntime(cfg)
	threadID := "T-019fdec9-b0cf-745d-8da4-f250184e870e"
	writeNeoOrbPersistedThread(t, rt, threadID, "sandbox", "container-host-a")
	hostA := &neoOrbFakeProvider{
		containers:   []neoOrbContainerSummary{{ID: "container-host-a", Labels: map[string]string{"cliproxy.orb": threadID}}},
		inspectState: neoOrbContainerState{Exists: true, Running: true},
	}
	manager := newNeoOrbManager(rt)
	manager.newClient = func(host string) (neoOrbProviderClient, error) {
		if host == "tcp://host-a:2375" {
			return hostA, nil
		}
		return nil, fmt.Errorf("unexpected Docker host %q", host)
	}
	rt.orbManager = manager

	if err := manager.ensureRecovered(rt.configSnapshot()); err != nil {
		t.Fatalf("recover host A: %v", err)
	}
	if record, ok := manager.snapshot(threadID); !ok || record.containerID != "container-host-a" || record.state != neoOrbStateRunning {
		t.Fatalf("host A record = %#v, %t", record, ok)
	}

	updated := *cfg
	updated.AmpCode.Orbs.DockerHost = "tcp://host-b:2375"
	if err := rt.updateConfig(&updated); err == nil || !strings.Contains(err.Error(), "requires restarting") {
		t.Fatalf("Docker host update error = %v", err)
	}
	if got := neoOrbsConfig(rt.configSnapshot()).DockerHost; got != "tcp://host-a:2375" {
		t.Fatalf("active Docker host = %q, want host A", got)
	}
	if record, ok := manager.snapshot(threadID); !ok || record.containerID != "container-host-a" || record.state != neoOrbStateRunning {
		t.Fatalf("host A record after rejected update = %#v, %t", record, ok)
	}
	if err := manager.ensureRecovered(&updated); err == nil || !strings.Contains(err.Error(), "requires restarting") {
		t.Fatalf("direct cross-host recovery error = %v", err)
	}
	if hostA.callCount("list-orbs") != 1 {
		t.Fatalf("host A recovery calls = %#v", hostA.calls)
	}
	if manager.clientKey != "tcp://host-a:2375" || manager.recoveredKey != "tcp://host-a:2375" {
		t.Fatalf("manager host keys client=%q recovered=%q", manager.clientKey, manager.recoveredKey)
	}
}

func TestNeoOrbPendingWorkRestartsRecoveredContainerWithLockGuard(t *testing.T) {
	for _, test := range []struct {
		name        string
		state       neoOrbContainerState
		wantUnpause int
	}{
		{name: "running", state: neoOrbContainerState{Exists: true, Running: true}},
		{name: "paused", state: neoOrbContainerState{Exists: true, Running: true, Paused: true}, wantUnpause: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			rt, fake := newNeoOrbTestRuntime(t)
			threadID := "T-019fdec9-b0cf-745d-8da4-f250184e870e"
			writeNeoOrbPersistedThread(t, rt, threadID, "sandbox", "recovered-container")
			fake.containers = []neoOrbContainerSummary{{ID: "recovered-container", Labels: map[string]string{"cliproxy.orb": threadID}}}
			fake.inspectState = test.state
			manager := newNeoOrbManager(rt)
			manager.newClient = func(host string) (neoOrbProviderClient, error) { return fake, nil }
			manager.client = fake
			rt.orbManager = manager
			actor := neoOrbTestActor(rt, threadID)
			t.Cleanup(actor.cancel)
			actor.agentState = "idle"
			actor.pendingInference = &neoInferenceInflight{messageID: "M-recovered"}

			if _, gotThread, pending := actor.pendingWebLocalExecutorRequest(); !pending || gotThread != threadID {
				t.Fatalf("recovered pending request = %v, %q", pending, gotThread)
			}
			result := actor.spawnExecutor(map[string]any{"requestId": "spawn-recovered"})
			if stringValue(result["status"]) != "starting" {
				t.Fatalf("recovered spawn = %#v", result)
			}
			waitNeoOrbState(t, manager, threadID, neoOrbStateRunning)
			if fake.callCount("create:") != 0 || fake.callCount("unpause:") != test.wantUnpause {
				t.Fatalf("recovered lifecycle calls = %#v", fake.calls)
			}
			if fake.callCount("exec-detached:/usr/bin/flock -n /run/cliproxy-amp-executor.lock /usr/local/bin/amp") != 1 {
				t.Fatalf("recovered executor did not use lock guard: %#v", fake.calls)
			}
			for _, prefix := range []string{
				"exec:dpkg --print-architecture",
				"exec:/bin/sh -lc set -eu\npkill -TERM -f '[a]mp .*--headless='",
				"copy:/usr/local/bin/agent-browser",
				"exec:/bin/sh -lc export HOME=/root",
			} {
				if index := fake.callIndex(prefix); index < 0 || index >= fake.callIndex("exec-detached:") {
					t.Fatalf("recovered migration call %q did not precede executor launch: %#v", prefix, fake.calls)
				}
			}
		})
	}
}

func TestNeoOrbRecoveredExecutorAdmissionWaitsForReplacementLaunch(t *testing.T) {
	rt, fake := newNeoOrbTestRuntime(t)
	threadID := "T-019fdec9-b0cf-745d-8da4-f250184e870e"
	writeNeoOrbPersistedThread(t, rt, threadID, "sandbox", "recovered-container")
	fake.containers = []neoOrbContainerSummary{{ID: "recovered-container", Labels: map[string]string{"cliproxy.orb": threadID}}}
	fake.inspectState = neoOrbContainerState{Exists: true, Running: true}
	fake.detachedStart = make(chan struct{}, 1)
	fake.detachedResume = make(chan struct{})
	manager := newNeoOrbManager(rt)
	manager.newClient = func(host string) (neoOrbProviderClient, error) { return fake, nil }
	manager.client = fake
	rt.orbManager = manager
	actor := neoOrbTestActor(rt, threadID)
	t.Cleanup(actor.cancel)
	actor.pendingInference = &neoInferenceInflight{messageID: "M-recovered"}
	actor.spawnExecutor(map[string]any{"requestId": "spawn-recovered"})

	select {
	case <-fake.detachedStart:
	case <-time.After(time.Second):
		t.Fatal("replacement executor launch did not start")
	}
	required, err := manager.orbExecutorMigrationRequired(threadID)
	if err != nil {
		t.Fatalf("orbExecutorMigrationRequired during launch: %v", err)
	}
	if !required {
		t.Fatal("executor admission opened before replacement launch completed")
	}
	close(fake.detachedResume)
	waitNeoOrbState(t, manager, threadID, neoOrbStateRunning)
	required, err = manager.orbExecutorMigrationRequired(threadID)
	if err != nil {
		t.Fatalf("orbExecutorMigrationRequired after launch: %v", err)
	}
	if required {
		t.Fatal("executor admission remained blocked after replacement launch")
	}
}

func TestNeoOrbRecoveredMigrationFailureDoesNotLaunchExecutor(t *testing.T) {
	rt, fake := newNeoOrbTestRuntime(t)
	threadID := "T-019fdec9-b0cf-745d-8da4-f250184e870e"
	writeNeoOrbPersistedThread(t, rt, threadID, "sandbox", "recovered-container")
	fake.containers = []neoOrbContainerSummary{{ID: "recovered-container", Labels: map[string]string{"cliproxy.orb": threadID}}}
	fake.inspectState = neoOrbContainerState{Exists: true, Running: true}
	fake.execHandler = func(cmd []string) neoOrbExecResult {
		if strings.Contains(strings.Join(cmd, " "), "pkill -TERM -f") {
			return neoOrbExecResult{ExitCode: 1, Stderr: "executor still holds lock"}
		}
		return neoOrbExecResult{ExitCode: 0}
	}
	manager := newNeoOrbManager(rt)
	manager.newClient = func(host string) (neoOrbProviderClient, error) { return fake, nil }
	manager.client = fake
	rt.orbManager = manager
	actor := neoOrbTestActor(rt, threadID)
	t.Cleanup(actor.cancel)

	actor.spawnExecutor(map[string]any{"requestId": "spawn-recovered-failure"})
	record := waitNeoOrbState(t, manager, threadID, neoOrbStateFailed)
	if record.containerID != "recovered-container" || fake.callCount("exec-detached:") != 0 {
		t.Fatalf("failed recovered migration record=%#v calls=%#v", record, fake.calls)
	}
}

func TestNeoOrbFailedRecordReplacesExistingContainer(t *testing.T) {
	rt, fake := newNeoOrbTestRuntime(t)
	threadID := "T-019fdec9-b0cf-745d-8da4-f250184e870e"
	actor := neoOrbTestActor(rt, threadID)
	t.Cleanup(actor.cancel)
	manager := rt.orbManagerFor()
	manager.orbs[threadID] = &neoOrbRecord{
		threadID:    threadID,
		containerID: "existing-container",
		state:       neoOrbStateFailed,
		workDir:     neoOrbWorkDir,
	}

	result := actor.spawnExecutor(map[string]any{"requestId": "retry-failed"})
	if stringValue(result["status"]) != "starting" {
		t.Fatalf("failed record retry = %#v", result)
	}
	waitNeoOrbState(t, manager, threadID, neoOrbStateRunning)
	if fake.callCount("remove:existing-container") != 1 || fake.callCount("create:") != 1 {
		t.Fatalf("failed record did not replace container: %#v", fake.calls)
	}
	if fake.callIndex("remove:existing-container") >= fake.callIndex("create:") {
		t.Fatalf("replacement container was created before retained container removal: %#v", fake.calls)
	}
}

func TestNeoOrbFailedRecordRemovalFailureDoesNotProvisionDuplicate(t *testing.T) {
	rt, fake := newNeoOrbTestRuntime(t)
	threadID := "T-019fdec9-b0cf-745d-8da4-f250184e870e"
	actor := neoOrbTestActor(rt, threadID)
	t.Cleanup(actor.cancel)
	manager := rt.orbManagerFor()
	manager.orbs[threadID] = &neoOrbRecord{
		threadID:    threadID,
		containerID: "existing-container",
		state:       neoOrbStateFailed,
		workDir:     neoOrbWorkDir,
	}
	fake.removeErr = errors.New("remove failed")

	actor.spawnExecutor(map[string]any{"requestId": "retry-failed"})
	record := waitNeoOrbState(t, manager, threadID, neoOrbStateFailed)
	if record.containerID != "existing-container" || fake.callCount("remove:existing-container") != 1 || fake.callCount("create:") != 0 {
		t.Fatalf("failed removal record=%#v calls=%#v", record, fake.calls)
	}
}

func TestNeoOrbResumeInspectFailureDoesNotProvisionDuplicate(t *testing.T) {
	rt, fake := newNeoOrbTestRuntime(t)
	threadID := "T-019fdec9-b0cf-745d-8da4-f250184e870e"
	actor := neoOrbTestActor(rt, threadID)
	t.Cleanup(actor.cancel)
	manager := rt.orbManagerFor()

	actor.spawnExecutor(map[string]any{"requestId": "spawn-first"})
	waitNeoOrbState(t, manager, threadID, neoOrbStateRunning)
	manager.mu.Lock()
	manager.orbs[threadID].state = neoOrbStatePaused
	manager.mu.Unlock()
	fake.mu.Lock()
	fake.inspectErr = errors.New("inspect failed")
	fake.mu.Unlock()

	actor.spawnExecutor(map[string]any{"requestId": "resume-inspect-failure"})
	record := waitNeoOrbState(t, manager, threadID, neoOrbStateFailed)
	if record.containerID != "container-fake" || fake.callCount("create:") != 1 || fake.callCount("remove:") != 0 {
		t.Fatalf("inspect failure record=%#v calls=%#v", record, fake.calls)
	}
}

func TestNeoOrbRestartConflictAndDiscoveryFailureBlockProvisioning(t *testing.T) {
	rt, fake := newNeoOrbTestRuntime(t)
	threadID := "T-019fdec9-b0cf-745d-8da4-f250184e870e"
	writeNeoOrbPersistedThread(t, rt, threadID, "sandbox", "container-one")
	fake.listErr = errors.New("docker unavailable")
	manager := newNeoOrbManager(rt)
	manager.newClient = func(host string) (neoOrbProviderClient, error) { return fake, nil }
	manager.client = fake
	rt.orbManager = manager
	actor := neoOrbTestActor(rt, threadID)
	t.Cleanup(actor.cancel)

	result := manager.spawnOrb(actor, map[string]any{"requestId": "spawn-discovery-failure"})
	if stringValue(result["status"]) != "failed" || fake.callCount("create:") != 0 {
		t.Fatalf("discovery failure spawn = %#v, calls %#v", result, fake.calls)
	}
	fake.mu.Lock()
	fake.listErr = nil
	fake.containers = []neoOrbContainerSummary{
		{ID: "container-one", Labels: map[string]string{"cliproxy.orb": threadID}},
		{ID: "container-two", Labels: map[string]string{"cliproxy.orb": threadID}},
	}
	fake.mu.Unlock()
	if err := manager.ensureRecovered(rt.configSnapshot()); err != nil {
		t.Fatalf("recovery retry: %v", err)
	}
	record, ok := manager.snapshot(threadID)
	if !ok || record.state != neoOrbStateConflict {
		t.Fatalf("duplicate recovery record = %#v, %t", record, ok)
	}
	result = manager.spawnOrb(actor, map[string]any{"requestId": "spawn-conflict"})
	if stringValue(result["status"]) != "failed" || fake.callCount("create:") != 0 {
		t.Fatalf("conflict spawn = %#v, calls %#v", result, fake.calls)
	}
}

func TestNeoOrbAutomaticPendingWorkReportsDiscoveryFailure(t *testing.T) {
	rt, fake := newNeoOrbTestRuntime(t)
	enabled := true
	rt.cfg.AmpCode.NeoLocalRuntime.Enabled = &enabled
	threadID := "T-019fdec9-b0cf-745d-8da4-f250184e870e"
	actor := neoOrbTestActor(rt, threadID)
	t.Cleanup(actor.cancel)
	actor.agentState = "idle"
	actor.pendingInference = &neoInferenceInflight{messageID: "M-pending"}
	fake.listErr = errors.New("docker unavailable")

	result := actor.maybeSpawnWebLocalExecutorForPendingWork()
	if stringValue(result["status"]) != "failed" || stringValue(mapValue(result["details"])["reasonCode"]) != "environment_recovering" {
		t.Fatalf("automatic discovery failure = %#v", result)
	}
	if fake.callCount("create:") != 0 {
		t.Fatalf("discovery failure created a container: %#v", fake.calls)
	}

	fake.mu.Lock()
	fake.listErr = nil
	fake.mu.Unlock()
	result = actor.maybeSpawnWebLocalExecutorForPendingWork()
	if stringValue(result["status"]) != "starting" {
		t.Fatalf("automatic recovery retry = %#v", result)
	}
	waitNeoOrbState(t, rt.orbManagerFor(), threadID, neoOrbStateRunning)
	if fake.callCount("create:") != 1 {
		t.Fatalf("recovery retry create calls = %#v", fake.calls)
	}
}

func TestNeoOrbReaperPausesIdleOrb(t *testing.T) {
	rt, fake := newNeoOrbTestRuntime(t)
	threadID := "T-019fdec9-b0cf-745d-8da4-f250184e870e"
	actor := neoOrbTestActor(rt, threadID)
	t.Cleanup(actor.cancel)
	manager := rt.orbManagerFor()

	actor.spawnExecutor(map[string]any{"requestId": "spawn-idle"})
	waitNeoOrbState(t, manager, threadID, neoOrbStateRunning)

	manager.mu.Lock()
	record := manager.orbs[threadID]
	record.idleSince = time.Now().Add(-time.Hour)
	manager.mu.Unlock()
	manager.reapRecord(record)
	if fake.callCount("pause:container-fake") != 1 {
		t.Fatalf("idle orb was not paused: %#v", fake.calls)
	}
	if state, _ := manager.snapshot(threadID); state.state != neoOrbStatePaused {
		t.Fatalf("record state = %q, want paused", state.state)
	}
}

func TestNeoOrbReaperKeepsActiveOrbRunning(t *testing.T) {
	rt, fake := newNeoOrbTestRuntime(t)
	threadID := "T-019fdec9-b0cf-745d-8da4-f250184e870e"
	actor := neoOrbTestActor(rt, threadID)
	t.Cleanup(actor.cancel)
	manager := rt.orbManagerFor()

	actor.spawnExecutor(map[string]any{"requestId": "spawn-active"})
	waitNeoOrbState(t, manager, threadID, neoOrbStateRunning)
	actor.mu.Lock()
	actor.agentState = "running"
	actor.mu.Unlock()

	manager.mu.Lock()
	record := manager.orbs[threadID]
	record.idleSince = time.Now().Add(-time.Hour)
	manager.mu.Unlock()
	manager.reapRecord(record)
	if fake.callCount("pause:") != 0 {
		t.Fatalf("active orb was paused: %#v", fake.calls)
	}
}

func TestNeoOrbReaperDoesNotPauseRecoveredOrbBeforeMigration(t *testing.T) {
	rt, fake := newNeoOrbTestRuntime(t)
	threadID := "T-019fdec9-b0cf-745d-8da4-f250184e870e"
	writeNeoOrbPersistedThread(t, rt, threadID, "sandbox", "recovered-container")
	fake.containers = []neoOrbContainerSummary{{ID: "recovered-container", Labels: map[string]string{"cliproxy.orb": threadID}}}
	fake.inspectState = neoOrbContainerState{Exists: true, Running: true}
	fake.pauseStart = make(chan struct{}, 1)
	fake.pauseResume = make(chan struct{})
	manager := newNeoOrbManager(rt)
	manager.newClient = func(host string) (neoOrbProviderClient, error) { return fake, nil }
	manager.client = fake
	rt.orbManager = manager
	actor := neoOrbTestActor(rt, threadID)
	t.Cleanup(actor.cancel)
	if err := manager.ensureRecovered(rt.configSnapshot()); err != nil {
		t.Fatalf("ensureRecovered: %v", err)
	}
	manager.mu.Lock()
	record := manager.orbs[threadID]
	record.idleSince = time.Now().Add(-time.Hour)
	manager.mu.Unlock()

	reaped := make(chan struct{})
	go func() {
		manager.reapRecord(record)
		close(reaped)
	}()
	select {
	case <-fake.pauseStart:
	case <-time.After(time.Second):
		t.Fatal("recovered idle orb was not considered for auto-pause")
	}
	actor.mu.Lock()
	actor.pendingInference = &neoInferenceInflight{messageID: "M-pending-during-pause"}
	actor.mu.Unlock()
	result := actor.spawnExecutor(map[string]any{"requestId": "spawn-during-recovered-pause"})
	if stringValue(result["status"]) != "starting" {
		t.Fatalf("resume during pause = %#v", result)
	}
	close(fake.pauseResume)
	select {
	case <-reaped:
	case <-time.After(time.Second):
		t.Fatal("recovered pause did not finish")
	}
	finalRecord := waitNeoOrbState(t, manager, threadID, neoOrbStateRunning)
	if finalRecord.recovered || fake.callCount("pause:recovered-container") != 1 || fake.callCount("unpause:recovered-container") != 1 {
		t.Fatalf("serialized recovered lifecycle record=%#v calls=%#v", finalRecord, fake.calls)
	}
}

func TestNeoOrbReachableURLRewritesLoopback(t *testing.T) {
	cases := map[string]string{
		"http://127.0.0.1:6420":          "http://host.docker.internal:6420",
		"http://localhost:8317":          "http://host.docker.internal:8317",
		"http://[::1]:6420":              "http://host.docker.internal:6420",
		"https://proxy.example.test":     "https://proxy.example.test",
		"https://proxy.example.test:443": "https://proxy.example.test:443",
		"http://127.0.0.1:6420/path":     "http://host.docker.internal:6420/path",
		"http://LOCALHOST:8317":          "http://host.docker.internal:8317",
		"http://127.0.0.2:6420":          "http://host.docker.internal:6420",
	}
	for input, want := range cases {
		if got := neoOrbReachableURL(input); got != want {
			t.Fatalf("neoOrbReachableURL(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestNeoOrbExecutorEnv(t *testing.T) {
	cfg := &config.Config{Host: "127.0.0.1", Port: 8317}
	env := neoOrbExecutorEnv(cfg, "T-019fdec9-b0cf-745d-8da4-f250184e870e", "/home/user/workspace/repo")
	joined := strings.Join(env, "\n")
	for _, want := range []string{
		"AMP_URL=http://host.docker.internal:8317",
		"AMP_THREAD_ID=T-019fdec9-b0cf-745d-8da4-f250184e870e",
		"AMP_EXECUTOR=1",
		"AMP_ORB=1",
		"RIVET_TOKEN=local-neo",
		"AMP_PWD=/home/user/workspace/repo",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("orb executor env missing %q:\n%s", want, joined)
		}
	}
}

func TestNeoOrbPendingExecutorRequestAllowsSandbox(t *testing.T) {
	rt, _ := newNeoOrbTestRuntime(t)
	threadID := "T-019fdec9-b0cf-745d-8da4-f250184e870e"
	actor := neoOrbTestActor(rt, threadID)
	t.Cleanup(actor.cancel)
	actor.agentState = "idle"
	actor.pendingInference = &neoInferenceInflight{messageID: "M-1"}

	_, gotThread, pending := actor.pendingWebLocalExecutorRequest()
	if !pending || gotThread != threadID {
		t.Fatalf("sandbox pending request = %v, %q", pending, gotThread)
	}

	manager := rt.orbManagerFor()
	manager.orbs[threadID] = &neoOrbRecord{threadID: threadID, state: neoOrbStateProvisioning}
	if _, _, pending = actor.pendingWebLocalExecutorRequest(); pending {
		t.Fatal("provisioning orb did not block a duplicate spawn")
	}
	manager.orbs[threadID].state = neoOrbStatePaused
	if _, _, pending = actor.pendingWebLocalExecutorRequest(); !pending {
		t.Fatal("paused orb blocked resume spawn")
	}
}

func TestNeoOrbExecutorEnvInjectsConfigEnv(t *testing.T) {
	cfg := &config.Config{
		Host: "127.0.0.1",
		Port: 8317,
		AmpCode: config.AmpCode{Orbs: config.AmpOrbs{Env: map[string]string{
			"GH_TOKEN":                      "ghp_test",
			"EDITOR":                        "vim",
			"AMP_URL":                       "https://evil.example",
			"RIVET_TOKEN":                   "override",
			"AGENT_BROWSER_EXECUTABLE_PATH": "/untrusted/browser",
			"HOME":                          "/untrusted/home",
			"9BAD":                          "nope",
		}}},
	}
	env := strings.Join(neoOrbExecutorEnv(cfg, "T-019fdec9-b0cf-745d-8da4-f250184e870e", "/work"), "\n")
	if !strings.Contains(env, "GH_TOKEN=ghp_test") || !strings.Contains(env, "EDITOR=vim") {
		t.Fatalf("config env not injected:\n%s", env)
	}
	if strings.Contains(env, "https://evil.example") || strings.Contains(env, "RIVET_TOKEN=override") || strings.Contains(env, "/untrusted/") || strings.Contains(env, "9BAD") {
		t.Fatalf("reserved or invalid env keys leaked:\n%s", env)
	}
	if !strings.Contains(env, "RIVET_TOKEN=local-neo") {
		t.Fatalf("managed RIVET_TOKEN missing:\n%s", env)
	}
}

func TestNeoOrbGitHubAuthUsesEnvNotArgs(t *testing.T) {
	fake := &neoOrbFakeProvider{}
	manager := &neoOrbManager{}
	cfg := &config.Config{AmpCode: config.AmpCode{Orbs: config.AmpOrbs{Env: map[string]string{"GH_TOKEN": "ghp_secret"}}}}
	if err := manager.orbConfigureGitHubAuth(context.Background(), cfg, fake, "c1"); err != nil {
		t.Fatalf("orbConfigureGitHubAuth: %v", err)
	}
	if fake.callCount("exec:/bin/sh -lc git config") != 1 {
		t.Fatalf("git config exec missing: %#v", fake.calls)
	}
	for _, call := range fake.calls {
		if strings.Contains(call, "ghp_secret") {
			t.Fatalf("token leaked into command args: %#v", call)
		}
	}
}

func TestNeoOrbSyncLocalConfig(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	write := func(rel, content string) {
		full := home + "/" + rel
		if err := os.MkdirAll(full[:strings.LastIndex(full, "/")], 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write(".config/amp/settings.json", `{"theme":"dark"}`)
	write(".config/AGENTS.md", "# global instructions")
	write(".config/agents/checks/orb-check.md", "check")
	write(".config/agents/skills/demo/SKILL.md", "skill")
	write(".config/agents/skills/mcp-file/SKILL.md", "mcp skill")
	write(".config/agents/skills/mcp-file/mcp.json", "{}")
	write(".config/agents/skills/mcp-inline/SKILL.md", "---\nname: mcp-inline\nmcpServers:\n  demo:\n    command: demo\n---\n")
	write(".config/agents/skills/mcp-quoted/SKILL.md", "---\nname: mcp-quoted\n\"mcpServers\":\n  demo:\n    command: demo\n---\n")
	write(".config/agents/skills/using-open-browser-use/SKILL.md", "laptop only")
	write(".config/agents/checks/auth-token.json", "secret")

	fake := &neoOrbFakeProvider{}
	manager := &neoOrbManager{}
	manager.orbSyncLocalConfig(context.Background(), fake, "c1")

	copies := []string{}
	for _, call := range fake.calls {
		if strings.HasPrefix(call, "copy:") || strings.HasPrefix(call, "copy-tar:") {
			copies = append(copies, call)
		}
	}
	joined := strings.Join(copies, "\n")
	for _, want := range []string{
		"copy:/root/.config/amp/settings.json",
		"copy:/root/.config/AGENTS.md",
		"copy-tar:/root/.config/agents/checks:orb-check.md",
		"copy-tar:/root/.config/agents/skills/demo:SKILL.md",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("missing synced file %q in:\n%s", want, joined)
		}
	}
	for _, excluded := range []string{"auth-token.json", "mcp-file", "mcp-inline", "mcp-quoted", "mcp.json", "using-open-browser-use"} {
		if strings.Contains(joined, excluded) {
			t.Fatalf("excluded file %q was synced:\n%s", excluded, joined)
		}
	}
}

func TestNeoOrbSkillSyncAllowedParsesFrontmatter(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    bool
	}{
		{name: "plain skill", content: "# Skill\n", want: true},
		{name: "normal quoted key", content: "---\n\"name\": demo\n---\n", want: true},
		{name: "unquoted MCP key", content: "---\nmcpServers: {}\n---\n"},
		{name: "single quoted MCP key", content: "---\n'mcpServers': {}\n---\n"},
		{name: "double quoted MCP key", content: "---\n\"mcpServers\": {}\n---\n"},
		{name: "flow map MCP key", content: "---\n{mcpServers: {demo: {command: demo}}}\n---\n"},
		{name: "malformed frontmatter", content: "---\nname: [\n---\n"},
		{name: "unterminated frontmatter", content: "---\nname: demo\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "SKILL.md"), []byte(test.content), 0o644); err != nil {
				t.Fatal(err)
			}
			if got := neoOrbSkillSyncAllowed(dir, "demo"); got != test.want {
				t.Fatalf("neoOrbSkillSyncAllowed() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestNeoOrbSafeSettingsAllowlist(t *testing.T) {
	raw := []byte(`{"amp.permissions":["read"],"amp.terminal.theme":"dark","amp.url":"https://secret.example","amp.mcpServers":{"secret":{"env":{"TOKEN":"value"}}},"amp.goalPlugin.thread.T-private":{"objective":"private"}}`)
	var safe map[string]any
	if err := json.Unmarshal(neoOrbSafeSettings(raw), &safe); err != nil {
		t.Fatalf("decode safe settings: %v", err)
	}
	if safe["amp.terminal.theme"] != "dark" {
		t.Fatalf("allowed settings missing: %#v", safe)
	}
	for _, denied := range []string{"amp.permissions", "amp.url", "amp.mcpServers", "amp.goalPlugin.thread.T-private"} {
		if safe[denied] != nil {
			t.Fatalf("unsafe setting %q retained: %#v", denied, safe)
		}
	}
}

func TestNeoOrbSecretLikeName(t *testing.T) {
	for _, name := range []string{"auth.json", "GH_TOKEN", "credentials.yaml", "server.pem", "my.key", "password.txt", "secrets"} {
		if !neoOrbSecretLikeName(name) {
			t.Fatalf("neoOrbSecretLikeName(%q) = false", name)
		}
	}
	for _, name := range []string{"settings.json", "SKILL.md", "checks", "keyboard.md", "authoring-guide.md", "authentication-guide.md"} {
		if neoOrbSecretLikeName(name) {
			t.Fatalf("neoOrbSecretLikeName(%q) = true", name)
		}
	}
	if !neoOrbSecretLikeName("GH_TOKEN") || !neoOrbSecretLikeName("my.key") || !neoOrbSecretLikeName("id_rsa.pem") {
		t.Fatal("secret-shaped names not detected")
	}
	for _, dotfile := range []string{".env", ".env.local", ".envrc", "id_rsa", "id_ed25519", "aws.credentials.json", "prod.secret.json"} {
		if !neoOrbSecretLikeName(dotfile) {
			t.Fatalf("neoOrbSecretLikeName(%q) = false", dotfile)
		}
	}
}

func TestNeoOrbSanitizeCloneURL(t *testing.T) {
	allowed := map[string]string{
		"https://github.com/aikins01/repo.git":     "https://github.com/aikins01/repo.git",
		"https://user:pass@github.com/a/b.git":     "https://github.com/a/b.git",
		"git@github.com:aikins01/repo.git":         "git@github.com:aikins01/repo.git",
		"https://gitea.example.test/team/repo.git": "https://gitea.example.test/team/repo.git",
		"https://192.168.1.20:8443/team/repo.git":  "https://192.168.1.20:8443/team/repo.git",
	}
	for input, want := range allowed {
		got, err := neoOrbSanitizeCloneURL(context.Background(), input)
		if err != nil || got != want {
			t.Fatalf("neoOrbSanitizeCloneURL(%q) = %q, %v; want %q", input, got, err, want)
		}
	}
	for _, blocked := range []string{
		"http://github.com/a/b.git",
		"https://169.254.169.254/latest/meta-data",
		"https://127.0.0.1:8317/secret",
		"https://localhost/repo.git",
		"https://LOCALHOST/repo.git",
		"https://host.docker.internal/repo.git",
		"git@127.0.0.1:repo.git",
		"git@[::1]:repo.git",
		"git@[fe80::1]:repo.git",
		"https://fe80::1%25eth0/repo.git",
		"git@[fe80::1%eth0]:repo.git",
		"file:///etc/passwd",
		"",
	} {
		if got, err := neoOrbSanitizeCloneURL(context.Background(), blocked); err == nil {
			t.Fatalf("neoOrbSanitizeCloneURL(%q) = %q, want error", blocked, got)
		}
	}
}

func TestNeoOrbBinaryIsELF(t *testing.T) {
	if !neoOrbBinaryIsELF([]byte{0x7f, 'E', 'L', 'F', 2, 1}) {
		t.Fatal("ELF binary not detected")
	}
	for _, other := range [][]byte{{0xfe, 0xed, 0xfa, 0xcf}, {0xca, 0xfe, 0xba, 0xbe}, {0x7f}, []byte("#!/bin/sh")} {
		if neoOrbBinaryIsELF(other) {
			t.Fatalf("non-ELF binary %x detected as ELF", other)
		}
	}
}

func TestNeoOrbToolchainPlanPinsVersionsAndArchitectures(t *testing.T) {
	amd64, err := neoOrbToolchainInstallScript("amd64")
	if err != nil {
		t.Fatalf("amd64 toolchain: %v", err)
	}
	for _, pin := range []string{neoOrbNodeVersion, neoOrbPNPMVersion, neoOrbBunVersion, neoOrbAgentBrowserVersion, neoOrbChromeVersion} {
		if !strings.Contains(amd64, pin) {
			t.Errorf("amd64 toolchain missing pin %q", pin)
		}
	}
	for _, forbidden := range []string{"@latest", "agent-browser install", "last-known-good-versions"} {
		if strings.Contains(amd64, forbidden) {
			t.Errorf("amd64 toolchain contains floating install %q", forbidden)
		}
	}
	readyGuard := strings.Index(amd64, "tools_ready=true")
	install := strings.Index(amd64, "apt-get update")
	if readyGuard < 0 || install < 0 || readyGuard >= install || !strings.Contains(amd64, "readlink /usr/local/libexec/cliproxy-browser") {
		t.Fatalf("amd64 toolchain does not validate pinned tools before installation")
	}
	if !strings.Contains(amd64, "github.com/vercel-labs/agent-browser/releases/download/v"+neoOrbAgentBrowserVersion+"/agent-browser-linux-x64") || strings.Contains(amd64, "registry.npmjs.org/agent-browser") {
		t.Fatalf("amd64 agent-browser is not installed from the pinned native release")
	}
	if !strings.Contains(amd64, "chrome-for-testing-public/"+neoOrbChromeVersion+"/linux64") || strings.Contains(amd64, "apt-get install -y -qq chromium") {
		t.Fatalf("amd64 browser selection is not pinned Chrome for Testing")
	}
	libexecDirectory := strings.Index(amd64, "mkdir -p /usr/local/libexec /home/user")
	browserSymlink := strings.LastIndex(amd64, "/usr/local/libexec/cliproxy-browser")
	if libexecDirectory < 0 || browserSymlink < 0 || libexecDirectory > browserSymlink {
		t.Fatalf("amd64 browser symlink is created before its parent directory")
	}
	arm64, err := neoOrbToolchainInstallScript("arm64")
	if err != nil {
		t.Fatalf("arm64 toolchain: %v", err)
	}
	if !strings.Contains(arm64, " chromium") || !strings.Contains(arm64, "/usr/bin/chromium") || strings.Contains(arm64, "chrome-for-testing-public") {
		t.Fatalf("arm64 browser selection is not distro Chromium")
	}
	if !strings.Contains(arm64, "agent-browser-linux-arm64") {
		t.Fatalf("arm64 agent-browser release selection is incorrect")
	}
	if _, err := neoOrbToolchainInstallScript("ppc64le"); err == nil {
		t.Fatal("unsupported architecture accepted")
	}
}

func TestNeoOrbAgentBrowserIsolationAndSmoke(t *testing.T) {
	fake := &neoOrbFakeProvider{}
	manager := &neoOrbManager{}
	firstThread := "T-019fdec9-b0cf-745d-8da4-f250184e870e"
	secondThread := "T-019fdec9-b0cf-745d-8da4-f250184e870f"
	if neoOrbBrowserNamespace(firstThread) == neoOrbBrowserNamespace(secondThread) || len(neoOrbBrowserNamespace(firstThread)) > 32 {
		t.Fatal("browser namespace is not stable, bounded, and thread-specific")
	}
	if err := manager.orbConfigureAgentBrowser(context.Background(), fake, "container", firstThread); err != nil {
		t.Fatalf("orbConfigureAgentBrowser: %v", err)
	}
	fake.mu.Lock()
	wrapper := string(fake.copiedFiles[neoOrbAgentBrowserPath])
	fake.mu.Unlock()
	for _, required := range []string{
		"HOME=/home/user",
		"AGENT_BROWSER_SOCKET_DIR=" + neoOrbAgentBrowserSocketDir,
		"AGENT_BROWSER_NAMESPACE=" + neoOrbBrowserNamespace(firstThread),
		"AGENT_BROWSER_EXECUTABLE_PATH=/usr/local/libexec/cliproxy-browser",
		"--no-sandbox,--disable-dev-shm-usage",
	} {
		if !strings.Contains(wrapper, required) {
			t.Fatalf("browser wrapper missing %q:\n%s", required, wrapper)
		}
	}
	joined := strings.Join(fake.calls, "\n")
	for _, required := range []string{"doctor --offline --quick", "data:text/html", "get title", "snapshot -i", "close --all"} {
		if !strings.Contains(joined, required) {
			t.Fatalf("browser smoke missing %q:\n%s", required, joined)
		}
	}
}

func TestNeoOrbHeadlessCommandUsesNonblockingContainerLock(t *testing.T) {
	args := []string{"--mode", "medium", "--headless=T-019fdec9-b0cf-745d-8da4-f250184e870e"}
	want := []string{"/usr/bin/flock", "-n", neoOrbExecutorLock, neoOrbBinaryPath, "--mode", "medium", "--headless=T-019fdec9-b0cf-745d-8da4-f250184e870e"}
	got := neoOrbHeadlessCommand(args)
	if strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("neoOrbHeadlessCommand() = %#v, want %#v", got, want)
	}
}

func TestNeoOrbInstallExecutorUsesInstallerBinaryPath(t *testing.T) {
	hostAmp := t.TempDir() + "/amp"
	if err := os.WriteFile(hostAmp, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatalf("write host Amp fixture: %v", err)
	}
	t.Setenv("AMP_EXECUTOR_COMMAND", hostAmp)
	var installCommand string
	fake := &neoOrbFakeProvider{
		execHandler: func(cmd []string) neoOrbExecResult {
			installCommand = strings.Join(cmd, " ")
			return neoOrbExecResult{ExitCode: 0}
		},
	}
	manager := &neoOrbManager{}
	if err := manager.orbInstallExecutor(context.Background(), &config.Config{}, fake, "container-fake"); err != nil {
		t.Fatalf("orbInstallExecutor: %v", err)
	}
	for _, required := range []string{
		"export HOME=/root",
		"${AMPBIN:-/root/.amp/bin/amp}",
		`cp "$AMPBIN" /usr/local/bin/amp`,
		"chmod 0755 /usr/local/bin/amp",
	} {
		if !strings.Contains(installCommand, required) {
			t.Fatalf("install command missing %q: %s", required, installCommand)
		}
	}
}

func TestNeoOrbConcurrentResumeClaimsOnce(t *testing.T) {
	rt, fake := newNeoOrbTestRuntime(t)
	threadID := "T-019fdec9-b0cf-745d-8da4-f250184e870e"
	actor := neoOrbTestActor(rt, threadID)
	t.Cleanup(actor.cancel)
	manager := rt.orbManagerFor()

	actor.spawnExecutor(map[string]any{"requestId": "spawn-first"})
	waitNeoOrbState(t, manager, threadID, neoOrbStateRunning)
	manager.mu.Lock()
	manager.orbs[threadID].state = neoOrbStatePaused
	manager.mu.Unlock()
	fake.mu.Lock()
	fake.inspectState.Paused = true
	fake.mu.Unlock()

	actor.spawnExecutor(map[string]any{"requestId": "resume-one"})
	second := actor.spawnExecutor(map[string]any{"requestId": "resume-two"})
	waitNeoOrbState(t, manager, threadID, neoOrbStateRunning)
	if fake.callCount("unpause:container-fake") != 1 {
		t.Fatalf("concurrent resumes unpause count = %d: %#v", fake.callCount("unpause:container-fake"), fake.calls)
	}
	if fake.callCount("exec-detached:/usr/bin/flock -n /run/cliproxy-amp-executor.lock /usr/local/bin/amp") != 2 {
		t.Fatalf("executor starts = %d, want exactly one resume restart: %#v", fake.callCount("exec-detached:/usr/bin/flock -n /run/cliproxy-amp-executor.lock /usr/local/bin/amp"), fake.calls)
	}
	if stringValue(second["status"]) != "starting" {
		t.Fatalf("second concurrent spawn = %#v, want already-starting status", second)
	}
}

func TestNeoOrbProvisionFailureRemovesContainer(t *testing.T) {
	rt, fake := newNeoOrbTestRuntime(t)
	threadID := "T-019fdec9-b0cf-745d-8da4-f250184e870e"
	actor := neoOrbTestActor(rt, threadID)
	t.Cleanup(actor.cancel)
	manager := rt.orbManagerFor()
	fake.removeStart = make(chan struct{}, 1)
	fake.removeResume = make(chan struct{})
	fake.execHandler = func(cmd []string) neoOrbExecResult {
		if strings.HasPrefix(strings.Join(cmd, " "), "git clone") {
			return neoOrbExecResult{ExitCode: 128, Stderr: "fatal: repository not found"}
		}
		return neoOrbExecResult{ExitCode: 0}
	}

	actor.spawnExecutor(map[string]any{"requestId": "spawn-fail", "repositoryURL": "https://example.test/gone.git"})
	select {
	case <-fake.removeStart:
	case <-time.After(time.Second):
		t.Fatal("failed provisioning did not start cleanup")
	}
	if record, ok := manager.snapshot(threadID); !ok || record.state != neoOrbStateProvisioning {
		t.Fatalf("cleanup released provisioning reservation early: %#v", record)
	}
	if binding := readNeoOrbPersistedBinding(t, rt, threadID); binding != "container-fake" {
		t.Fatalf("binding cleared before container removal: %q", binding)
	}
	actor.spawnExecutor(map[string]any{"requestId": "spawn-during-cleanup"})
	if fake.callCount("remove:container-fake") != 1 || fake.callCount("create:") != 1 {
		t.Fatalf("concurrent cleanup provisioned a duplicate: %#v", fake.calls)
	}
	close(fake.removeResume)
	waitNeoOrbState(t, manager, threadID, neoOrbStateFailed)
	if fake.callCount("remove:container-fake") != 1 {
		t.Fatalf("failed provisioning did not remove the container: %#v", fake.calls)
	}
	record, ok := manager.snapshot(threadID)
	if !ok || record.containerID != "" {
		t.Fatalf("failed provisioning retained removed container: %#v", record)
	}
	if binding := readNeoOrbPersistedBinding(t, rt, threadID); binding != "" {
		t.Fatalf("removed container binding = %q", binding)
	}
}

func TestNeoOrbBindingPersistenceFailureRemovesUnstartedContainer(t *testing.T) {
	rt, fake := newNeoOrbTestRuntime(t)
	threadID := "T-019fdec9-b0cf-745d-8da4-f250184e870e"
	actor := neoOrbTestActor(rt, threadID)
	t.Cleanup(actor.cancel)
	rt.writeLocalSnapshot = func(neoCloudThreadSnapshot, string) (int64, error) {
		return 0, errors.New("injected binding persistence failure")
	}

	actor.spawnExecutor(map[string]any{"requestId": "spawn-persist-fail"})
	record := waitNeoOrbState(t, rt.orbManagerFor(), threadID, neoOrbStateFailed)
	if record.containerID != "" || fake.callCount("create:") != 1 || fake.callCount("remove:container-fake") != 1 {
		t.Fatalf("persistence failure record=%#v calls=%#v", record, fake.calls)
	}
	if fake.callCount("start:") != 0 {
		t.Fatalf("container started before binding persistence: %#v", fake.calls)
	}
	actor.mu.Lock()
	binding := strings.TrimSpace(stringValue(actor.meta[neoOrbContainerIDMetaKey]))
	actor.mu.Unlock()
	if binding != "" {
		t.Fatalf("failed binding retained in actor metadata: %q", binding)
	}
}

func TestNeoOrbContainerBindingCleanupOnlyClearsMatchingBinding(t *testing.T) {
	rt, _ := newNeoOrbTestRuntime(t)
	threadID := "T-019fdec9-b0cf-745d-8da4-f250184e870e"
	actor := neoOrbTestActor(rt, threadID)
	t.Cleanup(actor.cancel)
	manager := rt.orbManagerFor()
	if err := manager.persistContainerBinding(actor, "replacement-container"); err != nil {
		t.Fatalf("persist replacement binding: %v", err)
	}

	manager.clearContainerBinding(actor, "removed-container")
	if binding := readNeoOrbPersistedBinding(t, rt, threadID); binding != "replacement-container" {
		t.Fatalf("nonmatching cleanup changed binding: %q", binding)
	}
	manager.clearContainerBinding(actor, "replacement-container")
	if binding := readNeoOrbPersistedBinding(t, rt, threadID); binding != "" {
		t.Fatalf("matching cleanup retained binding: %q", binding)
	}
}

func TestNeoOrbExecutorEnvPublicURLOverride(t *testing.T) {
	cfg := &config.Config{
		Host: "127.0.0.1",
		Port: 8317,
		AmpCode: config.AmpCode{Orbs: config.AmpOrbs{
			PublicURL:        "https://amp-proxy.example.test",
			RuntimePublicURL: "https://amp-runtime.example.test",
		}},
	}
	env := strings.Join(neoOrbExecutorEnv(cfg, "T-019fdec9-b0cf-745d-8da4-f250184e870e", "/work"), "\n")
	if !strings.Contains(env, "AMP_URL=https://amp-proxy.example.test") {
		t.Fatalf("public-url override missing:\n%s", env)
	}
	if !strings.Contains(env, "RIVET_ENDPOINT=https://amp-runtime.example.test") {
		t.Fatalf("runtime-public-url override missing:\n%s", env)
	}
	if strings.Contains(env, "host.docker.internal") {
		t.Fatalf("loopback rewrite leaked past the public-url override:\n%s", env)
	}
}

func TestNeoOrbContainerSpecNetwork(t *testing.T) {
	client, calls := newNeoOrbFakeDocker(t, func(call neoOrbFakeDockerCall) (int, any) {
		if strings.HasPrefix(call.Path, "/containers/create") {
			var payload map[string]any
			if err := json.Unmarshal(call.Body, &payload); err != nil {
				return 400, nil
			}
			if mapValue(payload["HostConfig"])["NetworkMode"] != "cliproxy-orbs" {
				return 400, map[string]any{"message": "missing network mode"}
			}
			return 201, map[string]any{"Id": "c1"}
		}
		return 500, nil
	})
	id, err := client.CreateContainer(context.Background(), neoOrbContainerSpec{Name: "n1", Image: "img", NetworkMode: "cliproxy-orbs"})
	if err != nil || id != "c1" {
		t.Fatalf("CreateContainer with network = %q, %v", id, err)
	}
	_ = calls
}

func TestNeoOrbNetworkNameValidation(t *testing.T) {
	for _, ok := range []string{"", "cliproxy-orbs", "dokploy-network", "orb_net.1", "cliproxy check net"} {
		if _, err := neoOrbNetworkName(ok); err != nil {
			t.Fatalf("neoOrbNetworkName(%q) rejected: %v", ok, err)
		}
	}
	for _, bad := range []string{"net\nwork", "net\trm", "net\x00"} {
		if _, err := neoOrbNetworkName(bad); err == nil {
			t.Fatalf("neoOrbNetworkName(%q) accepted", bad)
		}
	}
}
