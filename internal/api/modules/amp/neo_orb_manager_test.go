package amp

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/orbconfig"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/orbcredentials"
	"golang.org/x/crypto/ssh"
)

type neoOrbFakeProvider struct {
	mu                   sync.Mutex
	calls                []string
	execHandler          func(cmd []string) neoOrbExecResult
	execResultHandler    func(cmd []string) (neoOrbExecResult, error)
	readFileHandler      func(ctx context.Context, id, workDir, relative string, maxBytes int) ([]byte, error)
	inspectState         neoOrbContainerState
	containers           []neoOrbContainerSummary
	containerSpecs       []neoOrbContainerSpec
	createContainerID    string
	createContainerErr   error
	createContainerHook  func(context.Context, neoOrbContainerSpec)
	inspectContainerHook func(context.Context, string)
	copiedFiles          map[string][]byte
	pingErr              error
	listErr              error
	listStart            chan struct{}
	listResume           chan struct{}
	inspectErr           error
	stopErr              error
	pauseErr             error
	removeErr            error
	removeStart          chan struct{}
	removeResume         chan struct{}
	volumes              map[string]neoOrbVolumeState
	createVolumeErr      error
	createVolumeHook     func(context.Context, neoOrbVolumeSpec)
	inspectVolumeErr     error
	inspectVolumeHook    func(context.Context, string)
	listVolumesErr       error
	removeVolumeErr      error
	archiveFrom          func(context.Context, string, string) (io.ReadCloser, error)
	copyArchive          func(context.Context, string, string, io.Reader) error
	startHandler         func(string) error
	unpauseHandler       func(string) error
	containerTargetHook  func(string)
	pauseStart           chan struct{}
	pauseResume          chan struct{}
	detachedStart        chan struct{}
	detachedResume       chan struct{}
	detachedErr          error
	execDetachedEnvHook  func([]string)
	copyFileInspect      func(string)
	copyFileErrorPath    string
	copyFileError        error
	sensitiveCommandArg  bool
}

func (f *neoOrbFakeProvider) record(call string) {
	f.mu.Lock()
	f.calls = append(f.calls, call)
	f.mu.Unlock()
}

func (f *neoOrbFakeProvider) target(call string) {
	f.record(call)
	f.mu.Lock()
	hook := f.containerTargetHook
	f.mu.Unlock()
	if hook != nil {
		hook(call)
	}
}

func neoOrbFakeCommandCall(prefix string, cmd []string) (string, bool) {
	recorded := append([]string(nil), cmd...)
	sensitive := false
	for index, argument := range recorded {
		if strings.HasPrefix(argument, "https://") || strings.HasPrefix(argument, "http://") || strings.HasPrefix(argument, "git@") {
			if parsed, err := url.Parse(argument); err != nil || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || strings.HasPrefix(argument, "git@") && strings.ContainsAny(argument, "?#") {
				sensitive = true
			}
			recorded[index] = "<repository-url>"
			continue
		}
		if key, _, found := strings.Cut(argument, "="); found && neoOrbSecretLikeName(key) {
			sensitive = true
			recorded[index] = key + "=<redacted>"
			continue
		}
		if index > 0 && strings.HasPrefix(recorded[index-1], "-") && neoOrbSecretLikeName(strings.TrimLeft(recorded[index-1], "-")) {
			sensitive = true
			recorded[index] = "<redacted>"
		}
	}
	return prefix + strings.Join(recorded, " "), sensitive
}

func (f *neoOrbFakeProvider) commandCall(prefix string, cmd []string) string {
	call, sensitive := neoOrbFakeCommandCall(prefix, cmd)
	if sensitive {
		f.mu.Lock()
		f.sensitiveCommandArg = true
		f.mu.Unlock()
	}
	return call
}

func (f *neoOrbFakeProvider) hasSensitiveCommandArg() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sensitiveCommandArg
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
	f.mu.Lock()
	spec.Labels = maps.Clone(spec.Labels)
	delete(spec.Labels, neoOrbPortalTokenLabel)
	spec.Env = append([]string(nil), spec.Env...)
	spec.Cmd = append([]string(nil), spec.Cmd...)
	spec.ExtraHosts = append([]string(nil), spec.ExtraHosts...)
	spec.VolumeMounts = append([]neoOrbVolumeMount(nil), spec.VolumeMounts...)
	f.containerSpecs = append(f.containerSpecs, spec)
	hook := f.createContainerHook
	id := f.createContainerID
	err := f.createContainerErr
	f.mu.Unlock()
	if hook != nil {
		hook(ctx, spec)
	}
	if err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if id == "" {
		id = "container-fake"
	}
	f.mu.Lock()
	if spec.Labels["cliproxy.orb.lifecycle"] == "1" {
		mounts := make([]neoOrbContainerMount, 0, len(spec.VolumeMounts))
		for _, volumeMount := range spec.VolumeMounts {
			mounts = append(mounts, neoOrbContainerMount{Type: "volume", Source: f.volumes[volumeMount.Name].Mountpoint, Name: volumeMount.Name, Destination: volumeMount.Destination})
		}
		state := neoOrbContainerState{ID: id, Exists: true, Name: "/" + spec.Name, Labels: maps.Clone(spec.Labels), Mounts: mounts, RestartPolicy: spec.RestartPolicy}
		f.inspectState = state
	} else {
		f.inspectState = neoOrbContainerState{ID: id, Exists: true, Running: true, IPAddress: "172.17.0.8"}
	}
	f.mu.Unlock()
	return id, nil
}

func (f *neoOrbFakeProvider) ListOrbContainers(ctx context.Context) ([]neoOrbContainerSummary, error) {
	f.record("list-orbs")
	f.mu.Lock()
	containers := append([]neoOrbContainerSummary(nil), f.containers...)
	err := f.listErr
	started := f.listStart
	resume := f.listResume
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
			return nil, ctx.Err()
		}
	}
	return containers, err
}

func (f *neoOrbFakeProvider) StartContainer(ctx context.Context, id string) error {
	f.target("start:" + id)
	f.mu.Lock()
	handler := f.startHandler
	f.mu.Unlock()
	if handler != nil {
		if err := handler(id); err != nil {
			return err
		}
	}
	f.mu.Lock()
	f.inspectState.Running = true
	f.mu.Unlock()
	return nil
}

func (f *neoOrbFakeProvider) StopContainer(ctx context.Context, id string) error {
	f.target("stop:" + id)
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stopErr
}

func (f *neoOrbFakeProvider) PauseContainer(ctx context.Context, id string) error {
	f.target("pause:" + id)
	f.mu.Lock()
	started := f.pauseStart
	resume := f.pauseResume
	err := f.pauseErr
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
	if err != nil {
		return err
	}
	f.mu.Lock()
	f.inspectState.Paused = true
	f.mu.Unlock()
	return nil
}

func (f *neoOrbFakeProvider) UnpauseContainer(ctx context.Context, id string) error {
	f.target("unpause:" + id)
	f.mu.Lock()
	handler := f.unpauseHandler
	f.mu.Unlock()
	if handler != nil {
		if err := handler(id); err != nil {
			return err
		}
	}
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
	hook := f.inspectContainerHook
	f.mu.Unlock()
	if hook != nil {
		hook(ctx, id)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return neoOrbContainerState{}, err
	}
	state := f.inspectState
	err := f.inspectErr
	state.Labels = maps.Clone(state.Labels)
	state.Mounts = append([]neoOrbContainerMount(nil), state.Mounts...)
	return state, err
}

func (f *neoOrbFakeProvider) CreateVolume(ctx context.Context, spec neoOrbVolumeSpec) (neoOrbVolumeState, error) {
	f.record("create-volume:" + spec.Name)
	f.mu.Lock()
	hook := f.createVolumeHook
	f.mu.Unlock()
	if hook != nil {
		hook(ctx, spec)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.createVolumeErr != nil {
		return neoOrbVolumeState{}, f.createVolumeErr
	}
	if err := ctx.Err(); err != nil {
		return neoOrbVolumeState{}, err
	}
	if state, exists := f.volumes[spec.Name]; exists {
		state.Labels = maps.Clone(state.Labels)
		state.Options = maps.Clone(state.Options)
		return state, nil
	}
	if f.volumes == nil {
		f.volumes = map[string]neoOrbVolumeState{}
	}
	state := neoOrbVolumeState{Exists: true, Name: spec.Name, Driver: "local", Mountpoint: "/var/lib/docker/volumes/" + spec.Name + "/_data", Labels: maps.Clone(spec.Labels), Scope: "local"}
	f.volumes[spec.Name] = state
	return state, nil
}

func (f *neoOrbFakeProvider) InspectVolume(ctx context.Context, name string) (neoOrbVolumeState, error) {
	f.record("inspect-volume:" + name)
	f.mu.Lock()
	hook := f.inspectVolumeHook
	f.mu.Unlock()
	if hook != nil {
		hook(ctx, name)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.inspectVolumeErr != nil {
		return neoOrbVolumeState{}, f.inspectVolumeErr
	}
	if err := ctx.Err(); err != nil {
		return neoOrbVolumeState{}, err
	}
	state, exists := f.volumes[name]
	if !exists {
		return neoOrbVolumeState{}, nil
	}
	state.Labels = maps.Clone(state.Labels)
	state.Options = maps.Clone(state.Options)
	return state, nil
}

func (f *neoOrbFakeProvider) ListOrbVolumes(ctx context.Context) ([]neoOrbVolumeState, error) {
	f.record("list-orb-volumes")
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.listVolumesErr != nil {
		return nil, f.listVolumesErr
	}
	names := make([]string, 0, len(f.volumes))
	for name := range f.volumes {
		names = append(names, name)
	}
	sort.Strings(names)
	volumes := make([]neoOrbVolumeState, 0, len(names))
	for _, name := range names {
		state := f.volumes[name]
		if state.Labels["cliproxy.orb.lifecycle"] != "1" {
			continue
		}
		state.Labels = maps.Clone(state.Labels)
		state.Options = maps.Clone(state.Options)
		volumes = append(volumes, state)
	}
	return volumes, nil
}

func (f *neoOrbFakeProvider) RemoveVolume(ctx context.Context, name string) error {
	f.record("remove-volume:" + name)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.removeVolumeErr != nil {
		return f.removeVolumeErr
	}
	delete(f.volumes, name)
	return nil
}

func (f *neoOrbFakeProvider) Exec(ctx context.Context, id string, cmd []string, env []string, workDir string) (neoOrbExecResult, error) {
	joined := strings.Join(cmd, " ")
	f.target(f.commandCall("exec:", cmd))
	if f.execResultHandler != nil {
		return f.execResultHandler(cmd)
	}
	if joined == "dpkg --print-architecture" {
		return neoOrbExecResult{ExitCode: 0, Stdout: "amd64\n"}, nil
	}
	if f.execHandler != nil {
		return f.execHandler(cmd), nil
	}
	return neoOrbExecResult{ExitCode: 0}, nil
}

func (f *neoOrbFakeProvider) ReadWorkspaceFile(ctx context.Context, id, workDir, relative string, maxBytes int) ([]byte, error) {
	f.target("read-workspace-file:" + id + ":" + workDir + ":" + relative)
	if f.readFileHandler != nil {
		return f.readFileHandler(ctx, id, workDir, relative, maxBytes)
	}
	return nil, errors.New("read workspace file unavailable")
}

func (f *neoOrbFakeProvider) ExecDetached(ctx context.Context, id string, cmd []string, env []string, workDir string) error {
	f.target(f.commandCall("exec-detached:", cmd) + ":cwd=" + workDir)
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
	if hook := f.execDetachedEnvHook; hook != nil {
		hook(env)
	}
	return err
}

func (f *neoOrbFakeProvider) ArchiveFromContainer(ctx context.Context, id, sourcePath string) (io.ReadCloser, error) {
	f.target("archive-from:" + id + ":" + sourcePath)
	f.mu.Lock()
	handler := f.archiveFrom
	f.mu.Unlock()
	if handler != nil {
		return handler(ctx, id, sourcePath)
	}
	return io.NopCloser(strings.NewReader("")), nil
}

func (f *neoOrbFakeProvider) CopyArchiveToContainer(ctx context.Context, id, destinationPath string, archive io.Reader) error {
	f.target("copy-archive:" + id + ":" + destinationPath)
	f.mu.Lock()
	handler := f.copyArchive
	f.mu.Unlock()
	if handler != nil {
		return handler(ctx, id, destinationPath, archive)
	}
	return nil
}

func (f *neoOrbFakeProvider) CopyFileToContainer(ctx context.Context, id, destPath string, content []byte, mode int64) error {
	f.target(fmt.Sprintf("copy:%s:%d:%d", destPath, len(content), mode))
	f.mu.Lock()
	inspect := f.copyFileInspect
	copyErr := f.copyFileError
	copyErrPath := f.copyFileErrorPath
	if destPath == neoOrbAgentBrowserPath || destPath == neoOrbOwnerConfigDigestPath {
		if f.copiedFiles == nil {
			f.copiedFiles = map[string][]byte{}
		}
		f.copiedFiles[destPath] = append([]byte(nil), content...)
	}
	f.mu.Unlock()
	if inspect != nil {
		inspect(destPath)
	}
	if destPath == copyErrPath {
		return copyErr
	}
	return nil
}

func (f *neoOrbFakeProvider) CopyTarToContainer(ctx context.Context, id, destDir string, files map[string][]byte, mode int64) error {
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	f.target(fmt.Sprintf("copy-tar:%s:%s", destDir, strings.Join(names, ",")))
	return nil
}

func TestNeoOrbLifecycleFakeProviderListsOnlyLifecycleVolumes(t *testing.T) {
	fake := &neoOrbFakeProvider{volumes: map[string]neoOrbVolumeState{
		"lifecycle": {Exists: true, Name: "lifecycle", Labels: map[string]string{"cliproxy.orb.lifecycle": "1"}},
		"foreign":   {Exists: true, Name: "foreign"},
	}}
	volumes, err := fake.ListOrbVolumes(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(volumes) != 1 || volumes[0].Name != "lifecycle" {
		t.Fatalf("lifecycle volumes = %#v", volumes)
	}
}

func TestNeoOrbManagerReadWorkspaceFileExactRecord(t *testing.T) {
	fake := &neoOrbFakeProvider{readFileHandler: func(ctx context.Context, id, workDir, relative string, maxBytes int) ([]byte, error) {
		if id != "container-exact" || workDir != neoOrbWorkDir || relative != "images/result.png" || maxBytes != neoAttachmentMaxImageBytes {
			t.Fatalf("ReadWorkspaceFile args = %q, %q, %q, %d", id, workDir, relative, maxBytes)
		}
		return []byte("image"), nil
	}}
	rt := &neoRuntime{cfg: &config.Config{}}
	manager := newNeoOrbManager(rt)
	rt.orbManager = manager
	manager.newClient = func(string) (neoOrbProviderClient, error) { return fake, nil }
	threadID := "T-00000000-0000-0000-0000-000000000001"
	manager.orbs[threadID] = &neoOrbRecord{threadID: threadID, containerID: "container-exact", workDir: neoOrbWorkDir, state: neoOrbStateRunning}
	data, err := manager.readWorkspaceFile(context.Background(), threadID, "images/result.png")
	if err != nil || string(data) != "image" {
		t.Fatalf("readWorkspaceFile = %q, %v", data, err)
	}
	for _, state := range []string{neoOrbStatePaused, neoOrbStateConflict, neoOrbStateFailed} {
		manager.mu.Lock()
		manager.orbs[threadID].state = state
		manager.mu.Unlock()
		if _, err := manager.readWorkspaceFile(context.Background(), threadID, "images/result.png"); err == nil {
			t.Fatalf("state %s unexpectedly allowed workspace read", state)
		}
	}
	manager.mu.Lock()
	manager.orbs[threadID].state = neoOrbStateRunning
	manager.orbs[threadID].containerID = ""
	manager.mu.Unlock()
	if _, err := manager.readWorkspaceFile(context.Background(), threadID, "images/result.png"); err == nil {
		t.Fatal("missing container unexpectedly allowed workspace read")
	}
	if _, err := manager.readWorkspaceFile(context.Background(), "T-00000000-0000-0000-0000-000000000002", "images/result.png"); err == nil {
		t.Fatal("missing record unexpectedly allowed workspace read")
	}
}

func TestNeoOrbManagerReadWorkspaceFileCancellation(t *testing.T) {
	rt := &neoRuntime{cfg: &config.Config{}}
	manager := newNeoOrbManager(rt)
	threadID := "T-00000000-0000-0000-0000-000000000001"
	operationMu := &sync.Mutex{}
	operationMu.Lock()
	defer operationMu.Unlock()
	manager.orbs[threadID] = &neoOrbRecord{threadID: threadID, containerID: "container", workDir: neoOrbWorkDir, state: neoOrbStateRunning, operationMu: operationMu}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := manager.readWorkspaceFile(ctx, threadID, "image.png"); !errors.Is(err, context.Canceled) {
		t.Fatalf("readWorkspaceFile cancellation = %v", err)
	}
}

func TestNeoOrbManagerReadWorkspaceFileActiveLifecycle(t *testing.T) {
	fixture := newNeoOrbActiveLifecycleFixture(t)
	fixture.manager.mu.Lock()
	fixture.record.state = neoOrbStateRunning
	fixture.manager.mu.Unlock()

	data, err := fixture.manager.readWorkspaceFile(t.Context(), fixture.record.threadID, "images/result.png")
	if err != nil || string(data) != "workspace-data" {
		t.Fatalf("active lifecycle workspace read = %q, %v", data, err)
	}
	if fixture.fake.callCount("read-workspace-file:"+fixture.record.containerID+":"+neoOrbWorkDir+":images/result.png") != 1 {
		t.Fatalf("active lifecycle workspace calls = %#v", fixture.fake.calls)
	}
}

func TestNeoOrbManagerReadWorkspaceFileActiveLifecycleRejectsNonRunning(t *testing.T) {
	for _, state := range []string{neoOrbStateProvisioning, neoOrbStatePaused, neoOrbStateFailed, neoOrbStateConflict} {
		t.Run(state, func(t *testing.T) {
			fixture := newNeoOrbActiveLifecycleFixture(t)
			fixture.manager.mu.Lock()
			fixture.record.state = state
			fixture.manager.mu.Unlock()
			if _, err := fixture.manager.readWorkspaceFile(t.Context(), fixture.record.threadID, "image.png"); err == nil {
				t.Fatalf("state %s unexpectedly allowed lifecycle workspace read", state)
			}
			if len(fixture.fake.calls) != 0 {
				t.Fatalf("state %s reached provider: %#v", state, fixture.fake.calls)
			}
		})
	}
}

func TestNeoOrbManagerReadWorkspaceFileActiveLifecycleRejectsGenerationDrift(t *testing.T) {
	fixture := newNeoOrbActiveLifecycleFixture(t)
	fixture.manager.mu.Lock()
	fixture.record.state = neoOrbStateRunning
	fixture.manager.mu.Unlock()
	fixture.fake.mu.Lock()
	fixture.fake.readFileHandler = func(context.Context, string, string, string, int) ([]byte, error) {
		fixture.manager.mu.Lock()
		fixture.record.activeGeneration.ContainerID = "container-generation-drift"
		fixture.manager.mu.Unlock()
		return []byte("must-not-escape"), nil
	}
	fixture.fake.mu.Unlock()

	if data, err := fixture.manager.readWorkspaceFile(t.Context(), fixture.record.threadID, "image.png"); err == nil || data != nil {
		t.Fatalf("generation-drift workspace read = %q, %v", data, err)
	}
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

type neoOrbMaterializationFixture struct {
	runtime  *neoRuntime
	manager  *neoOrbManager
	fake     *neoOrbFakeProvider
	store    *neoOrbLifecycleStore
	record   *neoOrbRecord
	cfg      *config.Config
	provider string
}

func newNeoOrbMaterializationFixture(t *testing.T) *neoOrbMaterializationFixture {
	t.Helper()
	useTempNeoThreadStore(t)
	enabled := true
	cfg := &config.Config{AmpCode: config.AmpCode{Orbs: config.AmpOrbs{
		Enabled:    &enabled,
		Provider:   "docker",
		Image:      "orb-image:test",
		Network:    "orb-network",
		NanoCPUs:   2500000000,
		MemoryMB:   3072,
		DockerHost: "tcp://docker.materialization.test:2375",
	}}}
	rt := newNeoRuntime(cfg)
	fake := &neoOrbFakeProvider{createContainerID: "container-lifecycle-exact"}
	manager := newNeoOrbManager(rt)
	rt.orbManager = manager
	provider, err := resolveNeoOrbDockerEndpoint(neoOrbsConfig(cfg).DockerHost)
	if err != nil {
		t.Fatalf("resolve materialization provider: %v", err)
	}
	manager.client = fake
	manager.clientKey = provider
	manager.recovered = true
	manager.recoveredKey = provider
	store, err := rt.orbLifecycleStoreFor(provider)
	if err != nil {
		t.Fatalf("open materialization lifecycle store: %v", err)
	}
	t.Cleanup(func() { closeNeoOrbLifecycleTestStore(t, store) })
	record := &neoOrbRecord{
		threadID:    "T-019fdec9-b0cf-745d-8da4-f250184e870e",
		state:       neoOrbStateProvisioning,
		workDir:     neoOrbWorkDir,
		portalToken: strings.Repeat("q", neoOrbPortalTokenByteCount),
		operationMu: &sync.Mutex{},
	}
	manager.orbs[record.threadID] = record
	return &neoOrbMaterializationFixture{runtime: rt, manager: manager, fake: fake, store: store, record: record, cfg: cfg, provider: provider}
}

func assertNeoOrbMaterializationConflict(t *testing.T, fixture *neoOrbMaterializationFixture) neoOrbGenerationRecord {
	t.Helper()
	snapshot := fixture.store.snapshot()
	lifecycle, exists := snapshot.Threads[fixture.record.threadID]
	if !exists || lifecycle.Pending == nil || lifecycle.Active != nil {
		t.Fatalf("failed materialization lifecycle = %#v, exists=%v", lifecycle, exists)
	}
	record, exists := fixture.manager.snapshot(fixture.record.threadID)
	if !exists || record.state != neoOrbStateConflict || !record.lifecycleV1 || record.containerID != "" || record.activeGeneration != nil || record.pendingGeneration == nil || *record.pendingGeneration != *lifecycle.Pending || record.failReason != "lifecycle-v1 generation materialization requires reconciliation" {
		t.Fatalf("failed materialization record = %#v, exists=%v", record, exists)
	}
	for _, forbidden := range []string{"start:", "stop:", "pause:", "unpause:", "remove:", "remove-volume:", "exec:", "exec-detached:", "archive-from:", "copy-archive:", "copy:", "copy-tar:"} {
		if fixture.fake.callCount(forbidden) != 0 {
			t.Fatalf("failed materialization made cleanup or launch call %q: %#v", forbidden, fixture.fake.calls)
		}
	}
	return *lifecycle.Pending
}

func TestNeoOrbMaterializeLifecycleGenerationSuccess(t *testing.T) {
	fixture := newNeoOrbMaterializationFixture(t)
	assertRecordPending := func() {
		fixture.manager.mu.Lock()
		if fixture.record.containerID != "" || fixture.record.activeGeneration != nil || fixture.record.pendingGeneration == nil || !fixture.record.lifecycleV1 || fixture.record.state != neoOrbStateProvisioning {
			t.Errorf("record became actionable before promotion: %#v", fixture.record)
		}
		fixture.manager.mu.Unlock()
		if fixture.record.operationMu.TryLock() {
			fixture.record.operationMu.Unlock()
			t.Error("record operation lock was not held during provider call")
		}
	}
	assertPending := func() {
		assertRecordPending()
		if !fixture.store.mu.TryLock() {
			t.Error("lifecycle store lock was held during provider call")
		} else {
			fixture.store.mu.Unlock()
		}
		snapshot := fixture.store.snapshot()
		lifecycle := snapshot.Threads[fixture.record.threadID]
		if lifecycle.Pending == nil || lifecycle.Active != nil {
			t.Errorf("store was not pending during provider call: %#v", lifecycle)
		}
	}
	fixture.fake.createContainerHook = func(context.Context, neoOrbContainerSpec) { assertPending() }
	fixture.fake.inspectContainerHook = func(context.Context, string) { assertPending() }
	originalSync := fixture.store.syncDirectory
	syncCalls := 0
	fixture.store.syncDirectory = func(path string) error {
		syncCalls++
		if syncCalls == 2 {
			assertRecordPending()
		}
		return originalSync(path)
	}
	defer func() { fixture.store.syncDirectory = originalSync }()

	if err := fixture.manager.materializeLifecycleGeneration(t.Context(), fixture.record, fixture.fake, fixture.cfg); err != nil {
		t.Fatalf("materialize lifecycle generation: %v", err)
	}
	snapshot := fixture.store.snapshot()
	lifecycle, exists := snapshot.Threads[fixture.record.threadID]
	if !exists || lifecycle.Pending != nil || lifecycle.Active == nil || lifecycle.Active.Phase != neoOrbLifecyclePhaseActive || lifecycle.Active.ContainerID != "container-lifecycle-exact" {
		t.Fatalf("promoted lifecycle = %#v, exists=%v", lifecycle, exists)
	}
	record, exists := fixture.manager.snapshot(fixture.record.threadID)
	if !exists || record.state != neoOrbStateProvisioning || !record.lifecycleV1 || record.pendingGeneration != nil || record.activeGeneration == nil || *record.activeGeneration != *lifecycle.Active || record.containerID != lifecycle.Active.ContainerID || record.failReason != "" {
		t.Fatalf("promoted record = %#v, exists=%v", record, exists)
	}
	wantCalls := []string{
		"create-volume:" + lifecycle.Active.HomeVolumeName,
		"inspect-volume:" + lifecycle.Active.HomeVolumeName,
		"create-volume:" + lifecycle.Active.RootVolumeName,
		"inspect-volume:" + lifecycle.Active.RootVolumeName,
		"create:" + lifecycle.Active.ContainerName + ":orb-image:test",
		"inspect:container-lifecycle-exact",
		"inspect-volume:" + lifecycle.Active.HomeVolumeName,
		"inspect-volume:" + lifecycle.Active.RootVolumeName,
	}
	fixture.fake.mu.Lock()
	calls := append([]string(nil), fixture.fake.calls...)
	specs := append([]neoOrbContainerSpec(nil), fixture.fake.containerSpecs...)
	state := fixture.fake.inspectState
	fixture.fake.mu.Unlock()
	if !reflect.DeepEqual(calls, wantCalls) {
		t.Fatalf("provider order = %#v, want %#v", calls, wantCalls)
	}
	if len(specs) != 1 {
		t.Fatalf("container specs = %#v", specs)
	}
	wantSpec := neoOrbContainerSpec{
		Name:          lifecycle.Active.ContainerName,
		Image:         "orb-image:test",
		Cmd:           []string{"sleep", "infinity"},
		NanoCPUs:      2500000000,
		MemoryMB:      3072,
		ExtraHosts:    []string{"host.docker.internal:host-gateway"},
		Labels:        neoOrbLifecycleLabels(snapshot.OwnerID, *lifecycle.Active, neoOrbLifecycleRoleContainer),
		NetworkMode:   "orb-network",
		VolumeMounts:  []neoOrbVolumeMount{{Name: lifecycle.Active.HomeVolumeName, Destination: "/home/user", NoCopy: true}, {Name: lifecycle.Active.RootVolumeName, Destination: "/root", NoCopy: true}},
		RestartPolicy: "unless-stopped",
	}
	if !reflect.DeepEqual(specs[0], wantSpec) {
		t.Fatalf("container spec = %#v, want %#v", specs[0], wantSpec)
	}
	wantMounts := []neoOrbContainerMount{
		{Type: "volume", Source: "/var/lib/docker/volumes/" + lifecycle.Active.HomeVolumeName + "/_data", Name: lifecycle.Active.HomeVolumeName, Destination: "/home/user"},
		{Type: "volume", Source: "/var/lib/docker/volumes/" + lifecycle.Active.RootVolumeName + "/_data", Name: lifecycle.Active.RootVolumeName, Destination: "/root"},
	}
	if state.ID != lifecycle.Active.ContainerID || state.Name != "/"+lifecycle.Active.ContainerName || state.RestartPolicy != "unless-stopped" || !reflect.DeepEqual(state.Labels, wantSpec.Labels) || !reflect.DeepEqual(state.Mounts, wantMounts) {
		t.Fatalf("fake inspect state = %#v", state)
	}
	retained := fmt.Sprintf("%#v %#v", calls, specs)
	if strings.Contains(retained, fixture.record.portalToken) {
		t.Fatalf("provider fake retained portal token: %s", retained)
	}
	for _, forbidden := range []string{"start:", "stop:", "pause:", "unpause:", "remove:", "remove-volume:", "exec:", "exec-detached:", "archive-from:", "copy-archive:", "copy:", "copy-tar:"} {
		if fixture.fake.callCount(forbidden) != 0 {
			t.Fatalf("successful checkpoint made forbidden call %q: %#v", forbidden, calls)
		}
	}
}

func TestNeoOrbMaterializeLifecycleGenerationRejectsBeforeProviderMutation(t *testing.T) {
	tests := map[string]func(*testing.T, *neoOrbMaterializationFixture){
		"provider mismatch": func(_ *testing.T, fixture *neoOrbMaterializationFixture) {
			fixture.manager.clientKey = "http://foreign-provider.test:2375"
		},
		"record identity": func(_ *testing.T, fixture *neoOrbMaterializationFixture) {
			replacement := *fixture.record
			fixture.manager.orbs[fixture.record.threadID] = &replacement
		},
		"store unavailable": func(t *testing.T, fixture *neoOrbMaterializationFixture) {
			if err := fixture.store.Close(); err != nil {
				t.Fatalf("close lifecycle store: %v", err)
			}
			fixture.runtime.orbManagerMu.Lock()
			fixture.runtime.orbLifecycleStore = nil
			fixture.runtime.orbLifecycleStoreErr = errors.New("store unavailable")
			fixture.runtime.orbLifecycleStoreInitialized = true
			fixture.runtime.orbLifecycleStoreProvider = fixture.provider
			fixture.runtime.orbManagerMu.Unlock()
		},
		"reserve failure": func(_ *testing.T, fixture *neoOrbMaterializationFixture) {
			fixture.store.writeAtomic = func(string, []byte, os.FileMode) error { return errors.New("reserve unavailable") }
		},
		"store locked": func(t *testing.T, fixture *neoOrbMaterializationFixture) {
			if err := fixture.store.Close(); err != nil {
				t.Fatalf("close lifecycle store: %v", err)
			}
			fixture.runtime.orbManagerMu.Lock()
			fixture.runtime.orbLifecycleStore = nil
			fixture.runtime.orbLifecycleStoreErr = nil
			fixture.runtime.orbLifecycleStoreInitialized = false
			fixture.runtime.orbLifecycleStoreProvider = ""
			fixture.runtime.orbManagerMu.Unlock()
			locked, err := newNeoOrbLifecycleStore(fixture.runtime.threadDir, fixture.provider)
			if err != nil {
				t.Fatalf("lock lifecycle store: %v", err)
			}
			t.Cleanup(func() { closeNeoOrbLifecycleTestStore(t, locked) })
		},
	}
	for name, prepare := range tests {
		t.Run(name, func(t *testing.T) {
			fixture := newNeoOrbMaterializationFixture(t)
			prepare(t, fixture)
			if err := fixture.manager.materializeLifecycleGeneration(t.Context(), fixture.record, fixture.fake, fixture.cfg); err == nil {
				t.Fatal("materialization unexpectedly succeeded")
			}
			if len(fixture.fake.calls) != 0 {
				t.Fatalf("rejected materialization reached provider: %#v", fixture.fake.calls)
			}
		})
	}
}

func TestNeoOrbMaterializeLifecycleGenerationRejectsForeignVolumes(t *testing.T) {
	for _, role := range []string{neoOrbLifecycleRoleHome, neoOrbLifecycleRoleRoot} {
		t.Run(role, func(t *testing.T) {
			fixture := newNeoOrbMaterializationFixture(t)
			ownerID := fixture.store.ownerID()
			_, homeName, rootName := neoOrbLifecycleResourceNames(ownerID, fixture.record.threadID, 1)
			name := homeName
			if role == neoOrbLifecycleRoleRoot {
				name = rootName
			}
			fixture.fake.volumes = map[string]neoOrbVolumeState{name: {Exists: true, Name: name, Driver: "local", Mountpoint: "/foreign/" + name, Labels: map[string]string{"cliproxy.orb.owner": "foreign"}, Scope: "local"}}
			if err := fixture.manager.materializeLifecycleGeneration(t.Context(), fixture.record, fixture.fake, fixture.cfg); err == nil {
				t.Fatal("foreign same-name volume was accepted")
			}
			pending := assertNeoOrbMaterializationConflict(t, fixture)
			if pending.HomeVolumeName != homeName || pending.RootVolumeName != rootName {
				t.Fatalf("preserved pending generation = %#v", pending)
			}
			if fixture.fake.callCount("create-volume:"+name) != 1 || fixture.fake.callCount("inspect-volume:"+name) != 1 || fixture.fake.callCount("create:") != 0 {
				t.Fatalf("foreign volume calls = %#v", fixture.fake.calls)
			}
		})
	}
}

func TestNeoOrbMaterializeLifecycleGenerationRejectsInexactContainer(t *testing.T) {
	tests := map[string]func(*neoOrbMaterializationFixture){
		"returned id": func(fixture *neoOrbMaterializationFixture) {
			fixture.fake.createContainerID = "invalid/container/id"
		},
		"inspected id": func(fixture *neoOrbMaterializationFixture) {
			fixture.fake.inspectContainerHook = func(context.Context, string) {
				fixture.fake.mu.Lock()
				fixture.fake.inspectState.ID = "container-foreign"
				fixture.fake.mu.Unlock()
			}
		},
		"name": func(fixture *neoOrbMaterializationFixture) {
			fixture.fake.inspectContainerHook = func(context.Context, string) {
				fixture.fake.mu.Lock()
				fixture.fake.inspectState.Name += "-foreign"
				fixture.fake.mu.Unlock()
			}
		},
		"labels": func(fixture *neoOrbMaterializationFixture) {
			fixture.fake.inspectContainerHook = func(context.Context, string) {
				fixture.fake.mu.Lock()
				fixture.fake.inspectState.Labels["cliproxy.orb.owner"] = "foreign"
				fixture.fake.mu.Unlock()
			}
		},
		"mount source": func(fixture *neoOrbMaterializationFixture) {
			fixture.fake.inspectContainerHook = func(context.Context, string) {
				fixture.fake.mu.Lock()
				fixture.fake.inspectState.Mounts[0].Source = "/foreign/source"
				fixture.fake.mu.Unlock()
			}
		},
		"mount set": func(fixture *neoOrbMaterializationFixture) {
			fixture.fake.inspectContainerHook = func(context.Context, string) {
				fixture.fake.mu.Lock()
				fixture.fake.inspectState.Mounts = fixture.fake.inspectState.Mounts[:1]
				fixture.fake.mu.Unlock()
			}
		},
		"restart": func(fixture *neoOrbMaterializationFixture) {
			fixture.fake.inspectContainerHook = func(context.Context, string) {
				fixture.fake.mu.Lock()
				fixture.fake.inspectState.RestartPolicy = "no"
				fixture.fake.mu.Unlock()
			}
		},
	}
	for name, prepare := range tests {
		t.Run(name, func(t *testing.T) {
			fixture := newNeoOrbMaterializationFixture(t)
			prepare(fixture)
			if err := fixture.manager.materializeLifecycleGeneration(t.Context(), fixture.record, fixture.fake, fixture.cfg); err == nil {
				t.Fatal("inexact container was accepted")
			}
			assertNeoOrbMaterializationConflict(t, fixture)
			if fixture.fake.callCount("create:") != 1 {
				t.Fatalf("container creation retried: %#v", fixture.fake.calls)
			}
			if name == "returned id" && fixture.fake.callCount("inspect:") != 0 {
				t.Fatalf("invalid returned id was inspected: %#v", fixture.fake.calls)
			}
		})
	}
}

func TestNeoOrbMaterializeLifecycleGenerationProviderFailuresFailClosed(t *testing.T) {
	tests := map[string]struct {
		prepare  func(*neoOrbMaterializationFixture, context.CancelFunc)
		canceled bool
	}{
		"home volume create": {prepare: func(fixture *neoOrbMaterializationFixture, _ context.CancelFunc) {
			fixture.fake.createVolumeErr = errors.New("volume create failed")
		}},
		"root volume inspect": {prepare: func(fixture *neoOrbMaterializationFixture, _ context.CancelFunc) {
			fixture.fake.inspectVolumeHook = func(_ context.Context, name string) {
				if strings.HasSuffix(name, "-root") {
					fixture.fake.mu.Lock()
					fixture.fake.inspectVolumeErr = errors.New("volume inspect failed")
					fixture.fake.mu.Unlock()
				}
			}
		}},
		"container create": {prepare: func(fixture *neoOrbMaterializationFixture, _ context.CancelFunc) {
			fixture.fake.createContainerErr = errors.New("container create failed")
		}},
		"container inspect": {prepare: func(fixture *neoOrbMaterializationFixture, _ context.CancelFunc) {
			fixture.fake.inspectErr = errors.New("container inspect failed")
		}},
		"cancellation after container mutation starts": {canceled: true, prepare: func(fixture *neoOrbMaterializationFixture, cancel context.CancelFunc) {
			fixture.fake.createContainerHook = func(context.Context, neoOrbContainerSpec) { cancel() }
		}},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			fixture := newNeoOrbMaterializationFixture(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			test.prepare(fixture, cancel)
			err := fixture.manager.materializeLifecycleGeneration(ctx, fixture.record, fixture.fake, fixture.cfg)
			if err == nil {
				t.Fatal("failed provider stage unexpectedly succeeded")
			}
			if test.canceled && !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation error = %v", err)
			}
			assertNeoOrbMaterializationConflict(t, fixture)
			if fixture.fake.callCount("create-volume:") > 2 || fixture.fake.callCount("create:") > 1 || fixture.fake.callCount("inspect:") > 1 {
				t.Fatalf("provider mutation was retried: %#v", fixture.fake.calls)
			}
			if test.canceled && fixture.fake.callCount("inspect:") != 0 {
				t.Fatalf("canceled container creation made late calls: %#v", fixture.fake.calls)
			}
		})
	}
}

func TestNeoOrbMaterializeLifecycleGenerationPromotionDurabilityUncertainty(t *testing.T) {
	fixture := newNeoOrbMaterializationFixture(t)
	originalSync := fixture.store.syncDirectory
	rootInspects := 0
	fixture.fake.inspectVolumeHook = func(_ context.Context, name string) {
		if !strings.HasSuffix(name, "-root") {
			return
		}
		rootInspects++
		if rootInspects == 2 {
			fixture.store.syncDirectory = func(string) error { return errors.New("directory sync uncertain") }
		}
	}
	err := fixture.manager.materializeLifecycleGeneration(t.Context(), fixture.record, fixture.fake, fixture.cfg)
	fixture.store.syncDirectory = originalSync
	var durabilityErr *neoOrbLifecycleDurabilityError
	if !errors.As(err, &durabilityErr) {
		t.Fatalf("promotion error = %v, want durability uncertainty", err)
	}
	snapshot := fixture.store.snapshot()
	lifecycle, exists := snapshot.Threads[fixture.record.threadID]
	if !exists || lifecycle.Pending != nil || lifecycle.Active == nil || lifecycle.Active.Phase != neoOrbLifecyclePhaseActive {
		t.Fatalf("authoritative promoted snapshot = %#v, exists=%v", lifecycle, exists)
	}
	record, exists := fixture.manager.snapshot(fixture.record.threadID)
	if !exists || record.state != neoOrbStateConflict || record.containerID != "" || record.pendingGeneration != nil || record.activeGeneration == nil || *record.activeGeneration != *lifecycle.Active || record.failReason != "lifecycle-v1 generation materialization requires reconciliation" {
		t.Fatalf("uncertain promotion record = %#v, exists=%v", record, exists)
	}
	if fixture.fake.callCount("create:") != 1 || fixture.fake.callCount("remove:") != 0 || fixture.fake.callCount("remove-volume:") != 0 || fixture.fake.callCount("stop:") != 0 {
		t.Fatalf("uncertain promotion retried or cleaned up: %#v", fixture.fake.calls)
	}
}

func neoOrbTestActor(rt *neoRuntime, threadID string) *neoActor {
	actor := newNeoActor(rt, "actor-"+threadID, "thread-actor", threadID, threadID, neoActorRecord("actor-"+threadID, "thread-actor", threadID), nil)
	actor.bootstrapExecutorType = "sandbox"
	rt.store.mu.Lock()
	rt.store.actors[actor.id] = actor
	rt.store.indexActorLocked(actor)
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

func waitNeoOrbSpawnAfterDetachedInvocation(t *testing.T, manager *neoOrbManager, threadID string, invoked <-chan struct{}) *neoOrbRecord {
	t.Helper()
	select {
	case <-invoked:
	case <-time.After(5 * time.Second):
		t.Fatal("detached executor invocation did not occur")
	}
	record := manager.live(threadID)
	if record == nil || record.operationMu == nil {
		t.Fatalf("spawn record is unavailable: %#v", record)
	}
	record.operationMu.Lock()
	record.operationMu.Unlock()
	return record
}

func TestNeoOrbSpawnProvisionsContainerAndStartsExecutor(t *testing.T) {
	rt, fake := newNeoOrbTestRuntime(t)
	threadID := "T-019fdec9-b0cf-745d-8da4-f250184e870e"
	actor := neoOrbTestActor(rt, threadID)
	t.Cleanup(actor.cancel)
	manager := rt.orbManagerFor()
	cfg := rt.configSnapshot()
	cfg.AmpCode.Orbs.Env = map[string]string{"GH_TOKEN": "configured-spawn-token"}
	syncLocal := false
	cfg.AmpCode.Orbs.SyncLocalConfig = &syncLocal
	executorPath := filepath.Join(t.TempDir(), "amp-linux")
	if err := os.WriteFile(executorPath, []byte{0x7f, 'E', 'L', 'F', 1}, 0o700); err != nil {
		t.Fatalf("write test executor: %v", err)
	}
	cfg.AmpCode.Orbs.ExecutorCommand = executorPath
	bundle, err := orbconfig.New(nil, nil)
	if err != nil {
		t.Fatalf("create owner config bundle: %v", err)
	}
	if _, err := rt.orbConfigStore.put(neoLocalOwnerUserID, bundle); err != nil {
		t.Fatalf("store owner config bundle: %v", err)
	}
	fake.startHandler = func(id string) error {
		rt.orbManagerMu.Lock()
		store := rt.orbLifecycleStore
		rt.orbManagerMu.Unlock()
		if store == nil {
			return errors.New("lifecycle store is unavailable at start")
		}
		active := store.snapshot().Threads[threadID].Active
		if active == nil || active.ContainerID != id || active.ActivationState != neoOrbLifecycleActivationLaunching || active.OperationKind != neoOrbLifecycleOperationStart || active.OperationID == "" {
			return fmt.Errorf("start lifecycle activation = %#v", active)
		}
		if !manager.activeLifecycleActorBindingExact(threadID, active) {
			return errors.New("actor lifecycle binding is not durable at start")
		}
		raw, err := os.ReadFile(filepath.Join(rt.threadDir, threadID+".json"))
		if err != nil {
			return err
		}
		var thread map[string]any
		if err := json.Unmarshal(raw, &thread); err != nil {
			return err
		}
		ownerID, containerID, portalToken, revision, exact := neoOrbPersistedLifecycleBinding(thread)
		if !exact || ownerID != active.AuthenticatedOwnerID || containerID != active.ContainerID || portalToken != active.PortalToken || revision != active.Revision {
			return errors.New("persisted lifecycle binding is not exact at start")
		}
		return nil
	}
	detachedInvoked := make(chan struct{})
	var detachedOnce sync.Once
	var detachedLaunching bool
	var detachedBindingExact bool
	fake.execDetachedEnvHook = func([]string) {
		rt.orbManagerMu.Lock()
		store := rt.orbLifecycleStore
		rt.orbManagerMu.Unlock()
		if store != nil {
			active := store.snapshot().Threads[threadID].Active
			detachedLaunching = active != nil && active.ActivationState == neoOrbLifecycleActivationLaunching && active.OperationKind == neoOrbLifecycleOperationExecDetached && active.OperationID != ""
			detachedBindingExact = manager.activeLifecycleActorBindingExact(threadID, active)
		}
		detachedOnce.Do(func() { close(detachedInvoked) })
	}

	actor.bootstrapExecutorType = "sandbox"
	result := actor.spawnExecutor(map[string]any{"requestId": "spawn-orb-1", "repositoryURL": "https://example.test/repo.git"})
	if stringValue(result["status"]) == "failed" {
		t.Fatalf("spawn failed: %#v", result)
	}
	record := waitNeoOrbSpawnAfterDetachedInvocation(t, manager, threadID, detachedInvoked)
	if !detachedLaunching || !detachedBindingExact {
		t.Fatalf("detached invocation lifecycle launching=%v binding=%v", detachedLaunching, detachedBindingExact)
	}
	if record.state != neoOrbStateRunning || record.containerID != "container-fake" || !record.lifecycleV1 || record.activeGeneration == nil || !neoOrbLifecycleActionable(record.activeGeneration) {
		t.Fatalf("running lifecycle record = %#v", record)
	}
	rt.orbManagerMu.Lock()
	store := rt.orbLifecycleStore
	rt.orbManagerMu.Unlock()
	if store == nil {
		t.Fatal("spawn lifecycle store is unavailable")
	}
	snapshot := store.snapshot()
	lifecycle, exists := snapshot.Threads[threadID]
	active := lifecycle.Active
	if snapshot.Version != neoOrbLifecycleStoreVersion || !exists || lifecycle.Pending != nil || len(lifecycle.Retained) != 0 || active == nil || !neoOrbLifecycleActionable(active) || active.OperationID != "" || active.OperationKind != "" {
		t.Fatalf("durable active lifecycle = %#v, exists=%v version=%d", lifecycle, exists, snapshot.Version)
	}
	if active.AuthenticatedOwnerID != neoLocalOwnerUserID || active.ContainerID != "container-fake" || active.PortalToken != record.portalToken || active.Revision != active.Generation || *record.activeGeneration != *active {
		t.Fatalf("active lifecycle binding = %#v, record=%#v", active, record)
	}
	actor.mu.Lock()
	actorOwnerID, actorOwnerExact := neoOrbExactActorOwnerLocked(actor)
	actorContainerID := strings.TrimSpace(stringValue(actor.meta[neoOrbContainerIDMetaKey]))
	actorPortalToken := strings.TrimSpace(stringValue(actor.meta[neoOrbPortalTokenMetaKey]))
	actorRevision, actorRevisionExact := neoOrbLifecycleRevision(actor.meta[neoOrbLifecycleRevisionMetaKey])
	actor.mu.Unlock()
	if !actorOwnerExact || actorOwnerID != active.AuthenticatedOwnerID || actorContainerID != active.ContainerID || actorPortalToken != active.PortalToken || !actorRevisionExact || actorRevision != active.Revision || !manager.activeLifecycleActorBindingExact(threadID, active) {
		t.Fatalf("actor lifecycle binding owner=%q ownerExact=%v container=%q portalExact=%v revision=%d revisionExact=%v", actorOwnerID, actorOwnerExact, actorContainerID, actorPortalToken == active.PortalToken, actorRevision, actorRevisionExact)
	}
	persistedRaw, err := os.ReadFile(store.path)
	if err != nil {
		t.Fatalf("read durable lifecycle state: %v", err)
	}
	persisted, err := decodeNeoOrbLifecycleState(persistedRaw)
	if err != nil {
		t.Fatalf("decode durable lifecycle state: %v", err)
	}
	persistedActive := persisted.Threads[threadID].Active
	if persistedActive == nil || *persistedActive != *active || !neoOrbLifecycleActionable(persistedActive) {
		t.Fatalf("persisted active lifecycle = %#v, want %#v", persistedActive, active)
	}
	fake.mu.Lock()
	specs := append([]neoOrbContainerSpec(nil), fake.containerSpecs...)
	calls := append([]string(nil), fake.calls...)
	containerState := fake.inspectState
	homeVolume := fake.volumes[active.HomeVolumeName]
	rootVolume := fake.volumes[active.RootVolumeName]
	fake.mu.Unlock()
	if len(specs) != 1 {
		t.Fatalf("lifecycle container specs = %#v", specs)
	}
	wantLabels := neoOrbLifecycleLabels(snapshot.OwnerID, *active, neoOrbLifecycleRoleContainer)
	wantMounts := []neoOrbVolumeMount{{Name: active.HomeVolumeName, Destination: "/home/user", NoCopy: true}, {Name: active.RootVolumeName, Destination: "/root", NoCopy: true}}
	if specs[0].Name != active.ContainerName || specs[0].RestartPolicy != "unless-stopped" || !reflect.DeepEqual(specs[0].Labels, wantLabels) || !reflect.DeepEqual(specs[0].VolumeMounts, wantMounts) {
		t.Fatalf("lifecycle container spec = %#v", specs[0])
	}
	if !neoOrbLifecycleContainerMatches(active.ContainerID, containerState, homeVolume, rootVolume, snapshot.OwnerID, *active) {
		t.Fatalf("materialized lifecycle resources container=%#v home=%#v root=%#v", containerState, homeVolume, rootVolume)
	}
	if fake.callCount("create-volume:") != 2 || fake.callCount("create-volume:"+active.HomeVolumeName) != 1 || fake.callCount("create-volume:"+active.RootVolumeName) != 1 {
		t.Fatalf("lifecycle volume creation calls = %#v", calls)
	}
	if fake.callCount("start:"+active.ContainerID) != 1 || fake.callCount("exec-detached:/usr/bin/flock -n /run/cliproxy-amp-executor.lock /usr/local/bin/amp") != 1 {
		t.Fatalf("lifecycle activation calls = %#v", calls)
	}
	stagePrefixes := []string{
		"start:" + active.ContainerID,
		"exec:dpkg --print-architecture",
		"copy:/root/.config/gh/hosts.yml",
		"exec:cat " + neoOrbOwnerConfigDigestPath,
		"exec:git clone --depth 1 <repository-url>",
		"exec:/bin/sh -lc if [ -x .agents/setup ]",
		"exec-detached:/usr/bin/flock -n /run/cliproxy-amp-executor.lock /usr/local/bin/amp",
	}
	previous := -1
	for _, prefix := range stagePrefixes {
		index := fake.callIndex(prefix)
		if index <= previous {
			t.Fatalf("spawn stage %q index=%d after=%d calls=%#v", prefix, index, previous, calls)
		}
		previous = index
	}
	for _, forbidden := range []string{"stop:", "pause:", "unpause:", "remove:", "remove-volume:"} {
		if fake.callCount(forbidden) != 0 {
			t.Fatalf("successful lifecycle spawn performed cleanup %q: %#v", forbidden, calls)
		}
	}
}

func TestNeoOrbSpawnCredentialRevocationSuppressesConfiguredGlobalGitHub(t *testing.T) {
	rt, fake := newNeoOrbTestRuntime(t)
	threadID := "T-019fdec9-b0cf-745d-8da4-f250184e870e"
	actor := neoOrbTestActor(rt, threadID)
	t.Cleanup(actor.cancel)
	manager := rt.orbManagerFor()
	cfg := rt.configSnapshot()
	cfg.AmpCode.Orbs.Env = map[string]string{
		"GH_TOKEN":     "configured-global-token-a",
		"GITHUB_TOKEN": "configured-global-token-b",
	}
	syncLocal := false
	cfg.AmpCode.Orbs.SyncLocalConfig = &syncLocal
	executorPath := filepath.Join(t.TempDir(), "amp-linux")
	if err := os.WriteFile(executorPath, []byte{0x7f, 'E', 'L', 'F', 1}, 0o700); err != nil {
		t.Fatalf("write test executor: %v", err)
	}
	cfg.AmpCode.Orbs.ExecutorCommand = executorPath
	credentialStore := newTestNeoOwnerOrbCredentialStore(t)
	if revision, err := credentialStore.revokeIfRevision(neoLocalOwnerUserID, neoOrbCredentialAbsentRevision); err != nil || revision != neoOrbCredentialRevokedRevision {
		t.Fatalf("persist owner credential revocation: revision=%q err=%v", revision, err)
	}
	rt.orbCredentialStore = credentialStore
	globalHostsCopied := false
	fake.copyFileInspect = func(destination string) {
		if destination == "/root/.config/gh/hosts.yml" {
			globalHostsCopied = true
		}
	}
	detachedInvoked := make(chan struct{})
	var detachedOnce sync.Once
	var detachedHasGHToken bool
	var detachedHasGitHubToken bool
	fake.execDetachedEnvHook = func(env []string) {
		for _, entry := range env {
			key, _, _ := strings.Cut(entry, "=")
			detachedHasGHToken = detachedHasGHToken || strings.EqualFold(key, "GH_TOKEN")
			detachedHasGitHubToken = detachedHasGitHubToken || strings.EqualFold(key, "GITHUB_TOKEN")
		}
		detachedOnce.Do(func() { close(detachedInvoked) })
	}
	result := actor.spawnExecutor(map[string]any{"requestId": "spawn-orb-revoked"})
	if stringValue(result["status"]) == "failed" {
		t.Fatalf("revoked credential spawn failed: %#v", result)
	}
	record := waitNeoOrbSpawnAfterDetachedInvocation(t, manager, threadID, detachedInvoked)
	if record.state != neoOrbStateRunning || record.activeGeneration == nil || !neoOrbLifecycleActionable(record.activeGeneration) {
		t.Fatalf("revoked credential spawn record = %#v", record)
	}
	if globalHostsCopied || fake.callCount("copy:/root/.config/gh/hosts.yml") != 0 {
		t.Fatal("explicit owner credential revocation copied global GitHub hosts")
	}
	if detachedHasGHToken || detachedHasGitHubToken {
		t.Fatal("explicit owner credential revocation exposed shared GitHub env")
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

func TestNeoOrbSpawnDoesNotReprovisionMissingLifecycleContainer(t *testing.T) {
	rt, fake := newNeoOrbTestRuntime(t)
	threadID := "T-019fdec9-b0cf-745d-8da4-f250184e870e"
	actor := neoOrbTestActor(rt, threadID)
	t.Cleanup(actor.cancel)
	manager := rt.orbManagerFor()

	firstDetached := make(chan struct{})
	var firstDetachedOnce sync.Once
	fake.execDetachedEnvHook = func([]string) { firstDetachedOnce.Do(func() { close(firstDetached) }) }
	if result := actor.spawnExecutor(map[string]any{"requestId": "spawn-first"}); stringValue(result["status"]) == "failed" {
		t.Fatalf("initial spawn failed: %#v", result)
	}
	record := waitNeoOrbSpawnAfterDetachedInvocation(t, manager, threadID, firstDetached)
	if record.state != neoOrbStateRunning || record.activeGeneration == nil || !neoOrbLifecycleActionable(record.activeGeneration) {
		t.Fatalf("initial lifecycle record = %#v", record)
	}
	manager.mu.Lock()
	record.state = neoOrbStatePaused
	manager.mu.Unlock()
	fake.mu.Lock()
	fake.inspectState = neoOrbContainerState{Exists: false}
	fake.mu.Unlock()
	missingInspected := make(chan struct{})
	var missingInspectedOnce sync.Once
	fake.inspectContainerHook = func(context.Context, string) {
		missingInspectedOnce.Do(func() { close(missingInspected) })
	}

	if result := actor.spawnExecutor(map[string]any{"requestId": "spawn-missing"}); stringValue(result["status"]) == "failed" {
		t.Fatalf("missing-container resume was rejected before guarded inspection: %#v", result)
	}
	select {
	case <-missingInspected:
	case <-time.After(5 * time.Second):
		t.Fatal("missing lifecycle container was not inspected")
	}
	record.operationMu.Lock()
	record.operationMu.Unlock()
	state := manager.live(threadID)
	if state == nil || state.state != neoOrbStatePaused || state.containerID != "container-fake" || state.activeGeneration == nil || !neoOrbLifecycleActionable(state.activeGeneration) || state.failReason != "" {
		t.Fatalf("missing-container lifecycle record = %#v", state)
	}
	if fake.callCount("create:") != 1 || fake.callCount("unpause:") != 0 || fake.callCount("exec-detached:") != 1 {
		t.Fatalf("missing lifecycle container was invoked or re-provisioned: %#v", fake.calls)
	}
}

func writeNeoOrbPersistedThread(t *testing.T, rt *neoRuntime, threadID, executorType string, binding ...string) {
	t.Helper()
	meta := map[string]any{
		"cliProxyAPILocalNeo": true,
		"executorType":        executorType,
	}
	if len(binding) > 0 && strings.TrimSpace(binding[0]) != "" {
		meta[neoOrbContainerIDMetaKey] = binding[0]
	}
	if len(binding) > 1 && neoOrbPortalTokenValid(binding[1]) {
		meta[neoOrbPortalTokenMetaKey] = binding[1]
	}
	if len(binding) > 2 && strings.TrimSpace(binding[2]) != "" {
		meta["ownerUserId"] = binding[2]
	}
	if len(binding) > 3 && strings.TrimSpace(binding[3]) != "" {
		meta[neoOrbLifecycleRevisionMetaKey] = binding[3]
	}
	if _, err := writeNeoLocalThreadFileInDir(rt.threadDir, threadID, map[string]any{
		"id":   threadID,
		"meta": meta,
	}); err != nil {
		t.Fatalf("write persisted orb thread: %v", err)
	}
}

func readNeoOrbPersistedPortalToken(t *testing.T, rt *neoRuntime, threadID string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(rt.threadDir, threadID+".json"))
	if err != nil {
		t.Fatalf("read persisted orb thread: %v", err)
	}
	var thread map[string]any
	if err := json.Unmarshal(raw, &thread); err != nil {
		t.Fatalf("decode persisted orb thread: %v", err)
	}
	return strings.TrimSpace(stringValue(mapValue(thread["meta"])[neoOrbPortalTokenMetaKey]))
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

func TestNeoOrbRestartQuarantinesExactLegacyBindingWithoutAccess(t *testing.T) {
	rt, fake := newNeoOrbTestRuntime(t)
	threadID := "T-019fdec9-b0cf-745d-8da4-f250184e870e"
	writeNeoOrbPersistedThread(t, rt, threadID, "sandbox", "recovered-container")
	fake.containers = []neoOrbContainerSummary{{ID: "recovered-container", Labels: map[string]string{"cliproxy.orb": threadID, neoOrbPortalTokenLabel: strings.Repeat("p", neoOrbPortalTokenByteCount)}}}
	manager := newNeoOrbManager(rt)
	manager.newClient = func(host string) (neoOrbProviderClient, error) { return fake, nil }
	manager.client = fake
	rt.orbManager = manager

	if err := manager.ensureRecovered(rt.configSnapshot()); err != nil {
		t.Fatalf("ensureRecovered: %v", err)
	}
	record, ok := manager.snapshot(threadID)
	if !ok || record.containerID != "recovered-container" || record.state != neoOrbStateConflict || !record.recovered || record.lifecycleV1 {
		t.Fatalf("recovered legacy record = %#v, %t", record, ok)
	}
	if record.portalToken != "" || readNeoOrbPersistedPortalToken(t, rt, threadID) != "" {
		t.Fatalf("recovered legacy portal token changed: record=%q persisted=%q", record.portalToken, readNeoOrbPersistedPortalToken(t, rt, threadID))
	}
	if fake.callCount("inspect:") != 0 || manager.reaping {
		t.Fatalf("recovered legacy container was accessed: calls=%#v reaping=%v", fake.calls, manager.reaping)
	}
	if err := manager.ensureRecovered(rt.configSnapshot()); err != nil || fake.callCount("list-orbs") != 1 || fake.callCount("list-orb-volumes") != 1 {
		t.Fatalf("repeated recovery = %v, calls %#v", err, fake.calls)
	}
}

func seedNeoOrbLifecycleStore(t *testing.T, rt *neoRuntime, seed func(*neoOrbLifecycleStore)) {
	t.Helper()
	provider, err := resolveNeoOrbDockerEndpoint(neoOrbsConfig(rt.configSnapshot()).DockerHost)
	if err != nil {
		t.Fatalf("resolve lifecycle provider: %v", err)
	}
	store, err := newNeoOrbLifecycleStore(rt.threadDir, provider)
	if err != nil {
		t.Fatalf("open lifecycle store: %v", err)
	}
	seed(store)
	if err := store.Close(); err != nil {
		t.Fatalf("close lifecycle store: %v", err)
	}
}

func promoteNeoOrbLifecycleGeneration(t *testing.T, store *neoOrbLifecycleStore, threadID, containerID string) neoOrbGenerationRecord {
	t.Helper()
	generation, err := store.reserveGeneration(threadID, "portal-token-"+strings.ToLower(threadID))
	if err != nil {
		t.Fatalf("reserve lifecycle generation for %s: %v", threadID, err)
	}
	generation.ContainerID = containerID
	if err := store.promoteGeneration(generation); err != nil {
		t.Fatalf("promote lifecycle generation for %s: %v", threadID, err)
	}
	generation.Phase = neoOrbLifecyclePhaseActive
	return generation
}

func TestNeoOrbRecoveryQuarantinesLifecycleStateAndObservedResources(t *testing.T) {
	rt, fake := newNeoOrbTestRuntime(t)
	threads := map[string]string{
		"active":              "T-lifecycle-active",
		"pending":             "T-lifecycle-pending",
		"retained":            "T-lifecycle-retained",
		"tombstone":           "T-lifecycle-tombstone",
		"container":           "T-lifecycle-container",
		"empty-container":     "T-lifecycle-empty-container",
		"wrong-container":     "T-lifecycle-wrong-container",
		"volume":              "T-lifecycle-volume",
		"legacy-v1-collision": "T-lifecycle-collision",
	}
	activeGenerations := map[string]neoOrbGenerationRecord{}
	pendingGenerations := map[string]neoOrbGenerationRecord{}
	seedNeoOrbLifecycleStore(t, rt, func(store *neoOrbLifecycleStore) {
		activeGenerations[threads["active"]] = promoteNeoOrbLifecycleGeneration(t, store, threads["active"], "container-active")
		pending, err := store.reserveGeneration(threads["pending"], "portal-token-pending")
		if err != nil {
			t.Fatalf("reserve pending lifecycle generation: %v", err)
		}
		pendingGenerations[threads["pending"]] = pending
		promoteNeoOrbLifecycleGeneration(t, store, threads["retained"], "container-retained-1")
		activeGenerations[threads["retained"]] = promoteNeoOrbLifecycleGeneration(t, store, threads["retained"], "container-retained-2")
		activeGenerations[threads["tombstone"]] = promoteNeoOrbLifecycleGeneration(t, store, threads["tombstone"], "container-tombstone")
		if _, found, err := store.beginCleanup(threads["tombstone"]); err != nil || !found {
			t.Fatalf("begin lifecycle cleanup = %v, %v", found, err)
		}
	})
	writeNeoOrbPersistedThread(t, rt, threads["legacy-v1-collision"], "sandbox", "legacy-collision")
	fake.containers = []neoOrbContainerSummary{
		{ID: "container-v1", Labels: map[string]string{"cliproxy.orb": threads["container"], "cliproxy.orb.lifecycle": "1"}},
		{ID: "container-empty", Labels: map[string]string{"cliproxy.orb": threads["empty-container"], "cliproxy.orb.lifecycle": ""}},
		{ID: "container-wrong", Labels: map[string]string{"cliproxy.orb": threads["wrong-container"], "cliproxy.orb.lifecycle": "future"}},
		{ID: "legacy-collision", Labels: map[string]string{"cliproxy.orb": threads["legacy-v1-collision"]}},
		{ID: "v1-collision", Labels: map[string]string{"cliproxy.orb": threads["legacy-v1-collision"], "cliproxy.orb.lifecycle": "1"}},
	}
	fake.volumes = map[string]neoOrbVolumeState{
		"volume-v1": {Exists: true, Name: "volume-v1", Labels: map[string]string{"cliproxy.orb": threads["volume"], "cliproxy.orb.lifecycle": "1"}},
	}
	manager := newNeoOrbManager(rt)
	manager.newClient = func(string) (neoOrbProviderClient, error) { return fake, nil }
	rt.orbManager = manager

	if err := manager.ensureRecovered(rt.configSnapshot()); err != nil {
		t.Fatalf("ensureRecovered: %v", err)
	}
	for kind, threadID := range threads {
		record, ok := manager.snapshot(threadID)
		if !ok || record.state != neoOrbStateConflict || !record.recovered || !record.lifecycleV1 || record.containerID != "" || record.portalToken != "" || strings.TrimSpace(record.failReason) == "" {
			t.Fatalf("%s lifecycle quarantine = %#v, %v", kind, record, ok)
		}
		if want, hasActive := activeGenerations[threadID]; hasActive {
			if record.activeGeneration == nil || *record.activeGeneration != want {
				t.Fatalf("%s active generation = %#v, want %#v", kind, record.activeGeneration, want)
			}
		} else if record.activeGeneration != nil {
			t.Fatalf("%s unexpected active generation = %#v", kind, record.activeGeneration)
		}
		if want, hasPending := pendingGenerations[threadID]; hasPending {
			if record.pendingGeneration == nil || *record.pendingGeneration != want {
				t.Fatalf("%s pending generation = %#v, want %#v", kind, record.pendingGeneration, want)
			}
		} else if record.pendingGeneration != nil {
			t.Fatalf("%s unexpected pending generation = %#v", kind, record.pendingGeneration)
		}
	}
	for _, forbidden := range []string{"inspect:", "ping", "create:", "start:", "stop:", "pause:", "unpause:", "remove:", "exec:", "exec-detached:", "read-workspace-file:", "create-volume:", "inspect-volume:", "remove-volume:"} {
		if fake.callCount(forbidden) != 0 {
			t.Fatalf("lifecycle recovery accessed provider via %q: %#v", forbidden, fake.calls)
		}
	}
}

func setNeoOrbRecoveryLifecycleResources(fake *neoOrbFakeProvider, ownerID string, active neoOrbLifecycleGeneration, paused bool) {
	homeMountpoint := "/var/lib/docker/volumes/" + active.HomeVolumeName + "/_data"
	rootMountpoint := "/var/lib/docker/volumes/" + active.RootVolumeName + "/_data"
	fake.containers = []neoOrbContainerSummary{{ID: active.ContainerID, Labels: neoOrbLifecycleLabels(ownerID, active, neoOrbLifecycleRoleContainer)}}
	fake.volumes = map[string]neoOrbVolumeState{
		active.HomeVolumeName: {Exists: true, Name: active.HomeVolumeName, Driver: "local", Mountpoint: homeMountpoint, Labels: neoOrbLifecycleLabels(ownerID, active, neoOrbLifecycleRoleHome), Scope: "local"},
		active.RootVolumeName: {Exists: true, Name: active.RootVolumeName, Driver: "local", Mountpoint: rootMountpoint, Labels: neoOrbLifecycleLabels(ownerID, active, neoOrbLifecycleRoleRoot), Scope: "local"},
	}
	fake.inspectState = neoOrbContainerState{
		ID:            active.ContainerID,
		Exists:        true,
		Running:       true,
		Paused:        paused,
		Name:          "/" + active.ContainerName,
		Labels:        neoOrbLifecycleLabels(ownerID, active, neoOrbLifecycleRoleContainer),
		RestartPolicy: "unless-stopped",
		Mounts: []neoOrbContainerMount{
			{Type: "volume", Source: homeMountpoint, Name: active.HomeVolumeName, Destination: "/home/user"},
			{Type: "volume", Source: rootMountpoint, Name: active.RootVolumeName, Destination: "/root"},
		},
	}
}

func TestNeoOrbRecoveryQuarantinesVersion3BindingAndLaunchingWithoutResourceAccess(t *testing.T) {
	for _, activation := range []string{neoOrbLifecycleActivationBinding, neoOrbLifecycleActivationLaunching} {
		t.Run(activation, func(t *testing.T) {
			rt, fake := newNeoOrbTestRuntime(t)
			threadID := "T-recovery-v3-" + activation
			var active neoOrbLifecycleGeneration
			seedNeoOrbLifecycleStore(t, rt, func(store *neoOrbLifecycleStore) {
				active = activateNeoOrbBoundLifecycleTestGeneration(t, store, threadID, "container-"+activation, neoLocalOwnerUserID)
				if activation == neoOrbLifecycleActivationLaunching {
					var err error
					active, err = store.beginActivation(active, "operation-recovery-launching", neoOrbLifecycleOperationStart)
					if err != nil {
						t.Fatalf("begin launching recovery fixture: %v", err)
					}
				}
			})
			manager := newNeoOrbManager(rt)
			manager.newClient = func(string) (neoOrbProviderClient, error) { return fake, nil }
			rt.orbManager = manager

			if err := manager.ensureRecovered(rt.configSnapshot()); err != nil {
				t.Fatalf("recover %s lifecycle: %v", activation, err)
			}
			record, exists := manager.snapshot(threadID)
			if !exists || record.state != neoOrbStateConflict || !record.recovered || !record.lifecycleV1 || record.containerID != "" || record.activeGeneration == nil || *record.activeGeneration != active {
				t.Fatalf("%s lifecycle was not quarantined inertly", activation)
			}
			if fake.callCount("list-orbs") != 1 || fake.callCount("list-orb-volumes") != 1 || len(fake.calls) != 2 {
				t.Fatalf("%s lifecycle discovery calls = %#v", activation, fake.calls)
			}
			for _, forbidden := range []string{"inspect:", "inspect-volume:", "ping", "create:", "create-volume:", "start:", "stop:", "pause:", "unpause:", "remove:", "remove-volume:", "exec:", "exec-detached:"} {
				if fake.callCount(forbidden) != 0 {
					t.Fatalf("%s lifecycle reached provider via %q: %#v", activation, forbidden, fake.calls)
				}
			}
		})
	}
}

func TestNeoOrbRecoveryClassifiesExactVersion3ActionableResources(t *testing.T) {
	for _, test := range []struct {
		name      string
		paused    bool
		mismatch  bool
		wantState string
		wantInert bool
	}{
		{name: "running", wantState: neoOrbStateRunning},
		{name: "paused", paused: true, wantState: neoOrbStatePaused},
		{name: "restart policy mismatch", mismatch: true, wantState: neoOrbStateConflict, wantInert: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			rt, fake := newNeoOrbTestRuntime(t)
			threadID := "T-recovery-v3-actionable"
			var ownerID string
			var active neoOrbLifecycleGeneration
			seedNeoOrbLifecycleStore(t, rt, func(store *neoOrbLifecycleStore) {
				ownerID = store.ownerID()
				binding := activateNeoOrbBoundLifecycleTestGeneration(t, store, threadID, "container-actionable", neoLocalOwnerUserID)
				launching, err := store.beginActivation(binding, "operation-recovery-start", neoOrbLifecycleOperationStart)
				if err != nil {
					t.Fatalf("begin recovery start activation: %v", err)
				}
				launching, err = store.beginActivation(launching, "operation-recovery-exec", neoOrbLifecycleOperationExecDetached)
				if err != nil {
					t.Fatalf("begin recovery executor activation: %v", err)
				}
				active, err = store.finishActivation(launching)
				if err != nil {
					t.Fatalf("finish recovery activation: %v", err)
				}
			})
			writeNeoOrbPersistedThread(t, rt, threadID, "sandbox", active.ContainerID, active.PortalToken, active.AuthenticatedOwnerID, fmt.Sprint(active.Revision))
			setNeoOrbRecoveryLifecycleResources(fake, ownerID, active, test.paused)
			if test.mismatch {
				fake.inspectState.RestartPolicy = "no"
			}
			manager := newNeoOrbManager(rt)
			manager.newClient = func(string) (neoOrbProviderClient, error) { return fake, nil }
			rt.orbManager = manager

			if err := manager.ensureRecovered(rt.configSnapshot()); err != nil {
				t.Fatalf("recover actionable lifecycle: %v", err)
			}
			record, exists := manager.snapshot(threadID)
			if !exists || record.state != test.wantState || !record.lifecycleV1 || record.activeGeneration == nil || *record.activeGeneration != active {
				t.Fatalf("actionable recovery state=%q exists=%v lifecycle=%v activeExact=%v", record.state, exists, record.lifecycleV1, record.activeGeneration != nil && *record.activeGeneration == active)
			}
			if test.wantInert {
				if !record.recovered || record.containerID != "" || record.portalToken != "" {
					t.Fatalf("mismatched actionable lifecycle was not quarantined inertly")
				}
			} else if record.recovered || record.containerID != active.ContainerID || record.portalToken != active.PortalToken || !record.prelaunchReady || record.preparedOwnerID != active.AuthenticatedOwnerID {
				t.Fatalf("exact actionable lifecycle did not recover its authenticated binding")
			}
			if fake.callCount("list-orbs") != 1 || fake.callCount("list-orb-volumes") != 1 || fake.callCount("inspect:"+active.ContainerID) != 1 || fake.callCount("inspect-volume:") != 0 {
				t.Fatalf("actionable recovery reads = %#v", fake.calls)
			}
			for _, forbidden := range []string{"ping", "create:", "create-volume:", "start:", "stop:", "pause:", "unpause:", "remove:", "remove-volume:", "exec:", "exec-detached:"} {
				if fake.callCount(forbidden) != 0 {
					t.Fatalf("actionable recovery mutated provider via %q: %#v", forbidden, fake.calls)
				}
			}
			stored := rt.orbLifecycleStore.snapshot().Threads[threadID].Active
			if stored == nil || *stored != active || !neoOrbLifecycleActionable(stored) {
				t.Fatal("actionable recovery changed durable lifecycle state")
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

func TestNeoOrbLifecycleStoreCorruptionIsStickyBeforeProviderCalls(t *testing.T) {
	rt, fake := newNeoOrbTestRuntime(t)
	seedNeoOrbLifecycleStore(t, rt, func(*neoOrbLifecycleStore) {})
	storePath := filepath.Join(filepath.Dir(rt.threadDir), neoOrbLifecycleFileName)
	original, err := os.ReadFile(storePath)
	if err != nil {
		t.Fatalf("read lifecycle store: %v", err)
	}
	if err := os.WriteFile(storePath, []byte("{corrupt\n"), 0o600); err != nil {
		t.Fatalf("corrupt lifecycle store: %v", err)
	}
	manager := rt.orbManagerFor()
	firstErr := manager.ensureRecovered(rt.configSnapshot())
	if firstErr == nil || !strings.Contains(firstErr.Error(), "invalid JSON") {
		t.Fatalf("corrupt lifecycle recovery error = %v", firstErr)
	}
	if err := os.WriteFile(storePath, original, 0o600); err != nil {
		t.Fatalf("restore lifecycle store: %v", err)
	}
	secondErr := manager.ensureRecovered(rt.configSnapshot())
	if secondErr == nil || secondErr.Error() != firstErr.Error() {
		t.Fatalf("sticky lifecycle recovery error = %v, want %v", secondErr, firstErr)
	}
	if len(fake.calls) != 0 {
		t.Fatalf("corrupt lifecycle recovery reached provider: %#v", fake.calls)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := rt.stop(ctx); err != nil {
		t.Fatalf("stop runtime after sticky lifecycle error: %v", err)
	}
	fresh := rt.orbManagerFor()
	fresh.newClient = func(string) (neoOrbProviderClient, error) { return fake, nil }
	if err := fresh.ensureRecovered(rt.configSnapshot()); err != nil {
		t.Fatalf("fresh lifecycle recovery: %v", err)
	}
	if fake.callCount("list-orbs") != 1 || fake.callCount("list-orb-volumes") != 1 {
		t.Fatalf("fresh lifecycle recovery calls = %#v", fake.calls)
	}
}

func TestNeoOrbLifecycleStoreLockFailsBeforeProviderCalls(t *testing.T) {
	rt, fake := newNeoOrbTestRuntime(t)
	provider, err := resolveNeoOrbDockerEndpoint(neoOrbsConfig(rt.configSnapshot()).DockerHost)
	if err != nil {
		t.Fatalf("resolve lifecycle provider: %v", err)
	}
	locked, err := newNeoOrbLifecycleStore(rt.threadDir, provider)
	if err != nil {
		t.Fatalf("lock lifecycle store: %v", err)
	}
	manager := rt.orbManagerFor()
	if err := manager.ensureRecovered(rt.configSnapshot()); err == nil || !strings.Contains(err.Error(), "locked by another process") {
		t.Fatalf("locked lifecycle recovery error = %v", err)
	}
	if len(fake.calls) != 0 {
		t.Fatalf("locked lifecycle recovery reached provider: %#v", fake.calls)
	}
	if err := locked.Close(); err != nil {
		t.Fatalf("release lifecycle lock: %v", err)
	}
}

func TestNeoOrbRecoveredExecutorCannotReconnect(t *testing.T) {
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
		if host == "http://host-a:2375" {
			return hostA, nil
		}
		return nil, fmt.Errorf("unexpected Docker host %q", host)
	}
	rt.orbManager = manager

	if err := manager.ensureRecovered(rt.configSnapshot()); err != nil {
		t.Fatalf("recover host A: %v", err)
	}
	if record, ok := manager.snapshot(threadID); !ok || record.containerID != "container-host-a" || record.state != neoOrbStateConflict || !record.recovered {
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
	if record, ok := manager.snapshot(threadID); !ok || record.containerID != "container-host-a" || record.state != neoOrbStateConflict || !record.recovered {
		t.Fatalf("host A record after rejected update = %#v, %t", record, ok)
	}
	if err := manager.ensureRecovered(&updated); err == nil || !strings.Contains(err.Error(), "requires restarting") {
		t.Fatalf("direct cross-host recovery error = %v", err)
	}
	if hostA.callCount("list-orbs") != 1 {
		t.Fatalf("host A recovery calls = %#v", hostA.calls)
	}
	if manager.clientKey != "http://host-a:2375" || manager.recoveredKey != "http://host-a:2375" {
		t.Fatalf("manager host keys client=%q recovered=%q", manager.clientKey, manager.recoveredKey)
	}
}

func TestNeoOrbRecoveryRejectsEffectiveDockerHostEnvironmentChanges(t *testing.T) {
	useTempNeoThreadStore(t)
	t.Setenv("DOCKER_HOST", "tcp://host-a:2375")
	enabled := true
	cfg := &config.Config{AmpCode: config.AmpCode{Orbs: config.AmpOrbs{Enabled: &enabled, Provider: "docker"}}}
	rt := newNeoRuntime(cfg)
	manager := newNeoOrbManager(rt)
	manager.client = &neoOrbFakeProvider{}
	manager.clientKey = "http://host-a:2375"
	manager.recovered = true
	manager.recoveredKey = "http://host-a:2375"
	rt.orbManager = manager

	t.Setenv("DOCKER_HOST", "tcp://host-b:2375")
	updated := *cfg
	if err := rt.updateConfig(&updated); err == nil || !strings.Contains(err.Error(), "requires restarting") {
		t.Fatalf("effective Docker host update error = %v", err)
	}
	if err := manager.ensureRecovered(&updated); err == nil || !strings.Contains(err.Error(), "requires restarting") {
		t.Fatalf("effective Docker host recovery error = %v", err)
	}
	if manager.clientKey != "http://host-a:2375" || manager.recoveredKey != "http://host-a:2375" {
		t.Fatalf("manager host keys changed: client=%q recovered=%q", manager.clientKey, manager.recoveredKey)
	}
}

func TestNeoOrbManagerWorkersStopWithRuntime(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	manager := rt.orbManagerFor()
	started := make(chan struct{})
	stopped := make(chan struct{})
	if !manager.startWorker(func(ctx context.Context) {
		close(started)
		<-ctx.Done()
		close(stopped)
	}) {
		t.Fatal("worker did not start")
	}
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := rt.stop(ctx); err != nil {
		t.Fatalf("stop runtime: %v", err)
	}
	select {
	case <-stopped:
	default:
		t.Fatal("runtime stop returned before manager worker exited")
	}
	if manager.startWorker(func(context.Context) {}) {
		t.Fatal("manager accepted a worker after runtime stop")
	}
}

func TestNeoOrbRuntimeStopDoesNotCreateManagerAndAllowsFreshManager(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := rt.stop(ctx); err != nil {
		t.Fatalf("stop runtime without manager: %v", err)
	}
	if rt.orbManager != nil {
		t.Fatal("runtime stop created an orb manager")
	}
	previous := rt.orbManagerFor()
	if err := rt.stop(ctx); err != nil {
		t.Fatalf("stop runtime with manager: %v", err)
	}
	if rt.orbManager != nil {
		t.Fatal("runtime retained a successfully stopped orb manager")
	}
	if err := rt.stopOrbManager(ctx); err != nil {
		t.Fatalf("stop runtime orb manager again: %v", err)
	}
	fresh := rt.orbManagerFor()
	if fresh == previous {
		t.Fatal("runtime reused a stopped orb manager")
	}
	finished := make(chan struct{})
	if !fresh.startWorker(func(context.Context) { close(finished) }) {
		t.Fatal("fresh manager rejected a worker")
	}
	<-finished
	if err := fresh.stop(ctx); err != nil {
		t.Fatalf("stop fresh manager: %v", err)
	}
}

func TestNeoOrbRuntimeStopReleasesLifecycleStoreForFreshManager(t *testing.T) {
	rt, fake := newNeoOrbTestRuntime(t)
	manager := rt.orbManagerFor()
	if err := manager.ensureRecovered(rt.configSnapshot()); err != nil {
		t.Fatalf("initialize lifecycle store: %v", err)
	}
	provider, err := resolveNeoOrbDockerEndpoint(neoOrbsConfig(rt.configSnapshot()).DockerHost)
	if err != nil {
		t.Fatalf("resolve lifecycle provider: %v", err)
	}
	if _, err := newNeoOrbLifecycleStore(rt.threadDir, provider); err == nil || !strings.Contains(err.Error(), "locked by another process") {
		t.Fatalf("runtime lifecycle lock probe = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := rt.stop(ctx); err != nil {
		t.Fatalf("stop runtime: %v", err)
	}
	if rt.orbManager != nil || rt.orbLifecycleStore != nil || rt.orbLifecycleStoreInitialized || rt.orbLifecycleStoreErr != nil || rt.orbLifecycleStoreProvider != "" {
		t.Fatalf("runtime lifecycle state was not cleared: manager=%p store=%p initialized=%v err=%v provider=%q", rt.orbManager, rt.orbLifecycleStore, rt.orbLifecycleStoreInitialized, rt.orbLifecycleStoreErr, rt.orbLifecycleStoreProvider)
	}
	reopened, err := newNeoOrbLifecycleStore(rt.threadDir, provider)
	if err != nil {
		t.Fatalf("reopen lifecycle store after runtime stop: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("close reopened lifecycle store: %v", err)
	}
	fresh := rt.orbManagerFor()
	if fresh == manager {
		t.Fatal("runtime reused stopped manager")
	}
	fresh.newClient = func(string) (neoOrbProviderClient, error) { return fake, nil }
	if err := fresh.ensureRecovered(rt.configSnapshot()); err != nil {
		t.Fatalf("fresh manager recovery: %v", err)
	}
}

func TestNeoOrbRuntimeManagerStopTimeoutRetainsLifecycleStoreLock(t *testing.T) {
	rt, _ := newNeoOrbTestRuntime(t)
	manager := rt.orbManagerFor()
	if err := manager.ensureRecovered(rt.configSnapshot()); err != nil {
		t.Fatalf("initialize lifecycle store: %v", err)
	}
	store := rt.orbLifecycleStore
	workerStarted := make(chan struct{})
	releaseWorker := make(chan struct{})
	workerReleased := false
	defer func() {
		if !workerReleased {
			close(releaseWorker)
		}
	}()
	if !manager.startWorker(func(context.Context) {
		close(workerStarted)
		<-releaseWorker
	}) {
		t.Fatal("manager worker did not start")
	}
	<-workerStarted
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err := rt.stop(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("runtime stop timeout = %v", err)
	}
	if rt.orbManager != manager || rt.orbLifecycleStore != store || !rt.orbLifecycleStoreInitialized {
		t.Fatalf("timed out stop released lifecycle state: manager=%p store=%p initialized=%v", rt.orbManager, rt.orbLifecycleStore, rt.orbLifecycleStoreInitialized)
	}
	provider, err := resolveNeoOrbDockerEndpoint(neoOrbsConfig(rt.configSnapshot()).DockerHost)
	if err != nil {
		t.Fatalf("resolve lifecycle provider: %v", err)
	}
	if _, err := newNeoOrbLifecycleStore(rt.threadDir, provider); err == nil || !strings.Contains(err.Error(), "locked by another process") {
		t.Fatalf("timed out manager lifecycle lock probe = %v", err)
	}
	close(releaseWorker)
	workerReleased = true
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := rt.stop(ctx); err != nil {
		t.Fatalf("stop runtime after worker exit: %v", err)
	}
}

func TestNeoOrbManagerStopCancelsAndJoinsRecovery(t *testing.T) {
	useTempNeoThreadStore(t)
	enabled := true
	cfg := &config.Config{AmpCode: config.AmpCode{Orbs: config.AmpOrbs{Enabled: &enabled, Provider: "docker"}}}
	rt := newNeoRuntime(cfg)
	fake := &neoOrbFakeProvider{listStart: make(chan struct{}, 1), listResume: make(chan struct{})}
	manager := newNeoOrbManager(rt)
	manager.newClient = func(string) (neoOrbProviderClient, error) { return fake, nil }
	rt.orbManager = manager
	recovered := make(chan error, 1)
	go func() {
		recovered <- manager.ensureRecovered(cfg)
	}()
	select {
	case <-fake.listStart:
	case <-time.After(time.Second):
		t.Fatal("orb recovery did not reach provider list")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := rt.stop(ctx); err != nil {
		t.Fatalf("stop runtime during recovery: %v", err)
	}
	select {
	case err := <-recovered:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("recovery error = %v, want cancellation", err)
		}
	default:
		t.Fatal("runtime stop returned before recovery exited")
	}
	manager.mu.Lock()
	wasRecovered := manager.recovered
	manager.mu.Unlock()
	if wasRecovered {
		t.Fatal("canceled recovery published recovered state")
	}
}

func TestNeoOrbManagerStopCancelsRecoveryLockWait(t *testing.T) {
	useTempNeoThreadStore(t)
	enabled := true
	cfg := &config.Config{AmpCode: config.AmpCode{Orbs: config.AmpOrbs{Enabled: &enabled, Provider: "docker"}}}
	rt := newNeoRuntime(cfg)
	manager := newNeoOrbManager(rt)
	manager.recoverMu.Lock()
	defer manager.recoverMu.Unlock()
	rt.orbManager = manager
	recovered := make(chan error, 1)
	go func() {
		recovered <- manager.ensureRecovered(cfg)
	}()
	deadline := time.Now().Add(time.Second)
	for {
		manager.mu.Lock()
		workerCount := manager.workerCount
		manager.mu.Unlock()
		if workerCount == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("orb recovery did not wait for the recovery lock")
		}
		time.Sleep(time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := rt.stop(ctx); err != nil {
		t.Fatalf("stop runtime during recovery lock wait: %v", err)
	}
	select {
	case err := <-recovered:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("recovery lock wait error = %v, want cancellation", err)
		}
	default:
		t.Fatal("runtime stop returned before recovery lock waiter exited")
	}
}

func TestNeoOrbOperationLockWaitHonorsCancellation(t *testing.T) {
	operationMu := &sync.Mutex{}
	operationMu.Lock()
	defer operationMu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	waited := make(chan error, 1)
	go func() {
		waited <- neoOrbLockOperation(ctx, operationMu)
	}()
	select {
	case err := <-waited:
		t.Fatalf("operation lock returned before cancellation: %v", err)
	case <-time.After(2 * neoOrbOperationLockInterval):
	}
	cancel()
	select {
	case err := <-waited:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("operation lock error = %v, want cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("operation lock wait ignored cancellation")
	}
}

func TestNeoOrbManagerStopPreservesProvisionedContainerWithoutLateCleanup(t *testing.T) {
	rt, fake := newNeoOrbTestRuntime(t)
	threadID := "T-019fdec9-b0cf-745d-8da4-f250184e870e"
	actor := neoOrbTestActor(rt, threadID)
	t.Cleanup(actor.cancel)
	fake.detachedStart = make(chan struct{}, 1)
	fake.detachedResume = make(chan struct{})
	actor.spawnExecutor(map[string]any{"requestId": "spawn-stop-preserve"})
	select {
	case <-fake.detachedStart:
	case <-time.After(time.Second):
		t.Fatal("orb provisioning did not reach executor start")
	}
	manager := rt.orbManagerFor()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := rt.stop(ctx); err != nil {
		t.Fatalf("stop runtime during provisioning: %v", err)
	}
	if fake.callCount("remove:container-fake") != 0 {
		t.Fatalf("shutdown attempted late container cleanup: %#v", fake.calls)
	}
	record, ok := manager.snapshot(threadID)
	if !ok || record.state != neoOrbStateConflict || record.containerID != "" || record.activeGeneration == nil || record.activeGeneration.ContainerID != "container-fake" || record.activeGeneration.ActivationState != neoOrbLifecycleActivationLaunching || record.activeGeneration.OperationKind != neoOrbLifecycleOperationExecDetached {
		t.Fatalf("preserved provisioning record = %#v, %v", record, ok)
	}
	if binding := readNeoOrbPersistedBinding(t, rt, threadID); binding != "container-fake" {
		t.Fatalf("preserved container binding = %q", binding)
	}
}

func TestNeoOrbQuarantinedAndRecoveredRecordsRejectProviderAccess(t *testing.T) {
	for _, test := range []struct {
		name        string
		recovered   bool
		lifecycleV1 bool
	}{
		{name: "recovered legacy", recovered: true},
		{name: "lifecycle v1", recovered: true, lifecycleV1: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			rt, fake := newNeoOrbTestRuntime(t)
			threadID := "T-019fdec9-b0cf-745d-8da4-f250184e870e"
			actor := neoOrbTestActor(rt, threadID)
			t.Cleanup(actor.cancel)
			manager := rt.orbManagerFor()
			provider, err := resolveNeoOrbDockerEndpoint(neoOrbsConfig(rt.configSnapshot()).DockerHost)
			if err != nil {
				t.Fatalf("resolve lifecycle provider: %v", err)
			}
			manager.recovered = true
			manager.recoveredKey = provider
			manager.clientKey = provider
			record := &neoOrbRecord{
				threadID:    threadID,
				containerID: "inert-container",
				state:       neoOrbStateConflict,
				workDir:     neoOrbWorkDir,
				failReason:  "recovery conflict",
				recovered:   test.recovered,
				lifecycleV1: test.lifecycleV1,
				operationMu: &sync.Mutex{},
			}
			manager.orbs[threadID] = record

			result := manager.spawnOrb(actor, map[string]any{"requestId": "spawn-blocked"})
			if stringValue(result["status"]) != "failed" {
				t.Fatalf("blocked spawn = %#v", result)
			}
			manager.mu.Lock()
			record.state = neoOrbStateRunning
			manager.mu.Unlock()
			if _, err := manager.portalAddress(context.Background(), rt.configSnapshot(), threadID); err == nil {
				t.Fatal("blocked portal address succeeded")
			}
			if _, release, err := manager.acquirePortal(context.Background(), rt.configSnapshot(), threadID); err == nil || release != nil {
				t.Fatalf("blocked portal acquisition error=%v releaseNil=%v", err, release == nil)
			}
			if _, err := manager.readWorkspaceFile(context.Background(), threadID, "image.png"); err == nil {
				t.Fatal("blocked workspace read succeeded")
			}
			manager.reapRecordWithContext(context.Background(), record)
			manager.resumeOrb(context.Background(), actor, record, "resume-blocked")
			manager.replaceFailedOrb(context.Background(), actor, record, "replace-blocked")
			manager.reprovisionOrb(context.Background(), actor, record, "reprovision-blocked")
			required, err := manager.orbExecutorMigrationRequired(threadID)
			if err != nil || !required {
				t.Fatalf("blocked executor admission = %v, %v", required, err)
			}
			if len(fake.calls) != 0 {
				t.Fatalf("blocked record reached provider: %#v", fake.calls)
			}
			if current := manager.live(threadID); current != record {
				t.Fatalf("blocked record was replaced: current=%p record=%p", current, record)
			}
		})
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

type neoOrbRunningLifecycleTestFixture struct {
	runtime *neoRuntime
	manager *neoOrbManager
	fake    *neoOrbFakeProvider
	actor   *neoActor
	record  *neoOrbRecord
	store   *neoOrbLifecycleStore
}

func newNeoOrbRunningLifecycleTestFixture(t *testing.T) *neoOrbRunningLifecycleTestFixture {
	t.Helper()
	rt, fake := newNeoOrbTestRuntime(t)
	rt.cfg.AmpCode.NeoLocalRuntime.ExecutorConnectTimeoutSeconds = 30
	threadID := "T-019fdec9-b0cf-745d-8da4-f250184e870e"
	actor := neoOrbTestActor(rt, threadID)
	t.Cleanup(actor.cancel)
	detachedInvoked := make(chan struct{})
	var detachedOnce sync.Once
	fake.execDetachedEnvHook = func([]string) { detachedOnce.Do(func() { close(detachedInvoked) }) }
	result := actor.spawnExecutor(map[string]any{"requestId": "spawn-lifecycle-fixture"})
	if stringValue(result["status"]) == "failed" {
		t.Fatalf("spawn lifecycle fixture failed: %#v", result)
	}
	manager := rt.orbManagerFor()
	record := waitNeoOrbSpawnAfterDetachedInvocation(t, manager, threadID, detachedInvoked)
	rt.orbManagerMu.Lock()
	store := rt.orbLifecycleStore
	rt.orbManagerMu.Unlock()
	if record.state != neoOrbStateRunning || record.activeGeneration == nil || !neoOrbLifecycleActionable(record.activeGeneration) || store == nil {
		t.Fatal("running lifecycle fixture is not actionable")
	}
	fake.mu.Lock()
	fake.calls = nil
	fake.execDetachedEnvHook = nil
	fake.mu.Unlock()
	return &neoOrbRunningLifecycleTestFixture{runtime: rt, manager: manager, fake: fake, actor: actor, record: record, store: store}
}

func (fixture *neoOrbRunningLifecycleTestFixture) pause() {
	fixture.manager.mu.Lock()
	fixture.record.state = neoOrbStatePaused
	fixture.manager.mu.Unlock()
	fixture.fake.mu.Lock()
	fixture.fake.inspectState.Paused = true
	fixture.fake.mu.Unlock()
}

func assertNeoOrbResumeAmbiguity(t *testing.T, fixture *neoOrbRunningLifecycleTestFixture, operationKind string) {
	t.Helper()
	lifecycle := fixture.store.snapshot().Threads[fixture.record.threadID]
	active := lifecycle.Active
	if active == nil || lifecycle.Pending != nil || active.ActivationState != neoOrbLifecycleActivationLaunching || active.OperationID == "" || active.OperationKind != operationKind || neoOrbLifecycleActionable(active) {
		t.Fatalf("resume ambiguity did not preserve durable launching(%s)", operationKind)
	}
	if _, exists := fixture.store.snapshot().CleanupTombstones[fixture.record.threadID]; exists {
		t.Fatal("resume ambiguity created a cleanup tombstone")
	}
	record, exists := fixture.manager.snapshot(fixture.record.threadID)
	if !exists || record.state != neoOrbStateConflict || record.containerID != "" || record.recovered || !record.lifecycleV1 || record.activeGeneration == nil || *record.activeGeneration != *active || record.pendingGeneration != nil || record.prelaunchReady || record.preparedOwnerID != "" {
		t.Fatalf("resume ambiguity was not quarantined inertly: exists=%v state=%q container=%q lifecycle=%v", exists, record.state, record.containerID, record.lifecycleV1)
	}
	for _, forbidden := range []string{"list-orbs", "list-orb-volumes", "ping", "create:", "create-volume:", "start:", "stop:", "pause:", "remove:", "remove-volume:"} {
		if fixture.fake.callCount(forbidden) != 0 {
			t.Fatalf("resume ambiguity invoked forbidden provider call %q: %#v", forbidden, fixture.fake.calls)
		}
	}
}

func TestNeoOrbResumeInvokedUnpauseAmbiguityStaysDurableAndInert(t *testing.T) {
	for _, test := range []struct {
		name    string
		prepare func(*neoOrbRunningLifecycleTestFixture)
	}{
		{name: "provider error", prepare: func(fixture *neoOrbRunningLifecycleTestFixture) {
			fixture.fake.unpauseHandler = func(string) error { return errors.New("unpause failed") }
		}},
		{name: "postauthorization drift", prepare: func(fixture *neoOrbRunningLifecycleTestFixture) {
			fixture.fake.containerTargetHook = func(call string) {
				if strings.HasPrefix(call, "unpause:") {
					fixture.actor.mu.Lock()
					fixture.actor.meta["ownerUserId"] = "owner-after-unpause"
					fixture.actor.mu.Unlock()
				}
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newNeoOrbRunningLifecycleTestFixture(t)
			fixture.pause()
			test.prepare(fixture)
			invoked := make(chan struct{})
			var invokedOnce sync.Once
			originalHook := fixture.fake.containerTargetHook
			fixture.fake.containerTargetHook = func(call string) {
				if originalHook != nil {
					originalHook(call)
				}
				if strings.HasPrefix(call, "unpause:") {
					invokedOnce.Do(func() { close(invoked) })
				}
			}

			result := fixture.actor.spawnExecutor(map[string]any{"requestId": "resume-unpause-ambiguity"})
			if stringValue(result["status"]) != "starting" {
				t.Fatalf("resume ambiguity status = %#v", result)
			}
			select {
			case <-invoked:
			case <-time.After(5 * time.Second):
				t.Fatal("unpause ambiguity did not invoke the provider")
			}
			fixture.record.operationMu.Lock()
			fixture.record.operationMu.Unlock()

			assertNeoOrbResumeAmbiguity(t, fixture, neoOrbLifecycleOperationUnpause)
			if fixture.fake.callCount("unpause:"+fixture.record.activeGeneration.ContainerID) != 1 || fixture.fake.callCount("exec-detached:") != 0 {
				t.Fatalf("unpause ambiguity retried or advanced: %#v", fixture.fake.calls)
			}
		})
	}
}

func TestNeoOrbResumeInvokedExecDetachedErrorStaysDurableAndInert(t *testing.T) {
	fixture := newNeoOrbRunningLifecycleTestFixture(t)
	fixture.pause()
	fixture.fake.detachedErr = errors.New("detached executor failed")
	fixture.fake.detachedStart = make(chan struct{}, 1)

	result := fixture.actor.spawnExecutor(map[string]any{"requestId": "resume-detached-ambiguity"})
	if stringValue(result["status"]) != "starting" {
		t.Fatalf("detached ambiguity status = %#v", result)
	}
	select {
	case <-fixture.fake.detachedStart:
	case <-time.After(5 * time.Second):
		t.Fatal("detached ambiguity did not invoke the provider")
	}
	fixture.record.operationMu.Lock()
	fixture.record.operationMu.Unlock()

	assertNeoOrbResumeAmbiguity(t, fixture, neoOrbLifecycleOperationExecDetached)
	if fixture.fake.callCount("unpause:"+fixture.record.activeGeneration.ContainerID) != 1 || fixture.fake.callCount("exec-detached:/usr/bin/flock -n /run/cliproxy-amp-executor.lock /usr/local/bin/amp") != 1 {
		t.Fatalf("detached ambiguity retried or skipped a stage: %#v", fixture.fake.calls)
	}
}

func TestNeoOrbResumeInspectFailureStaysPausedWithoutDuplicate(t *testing.T) {
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
	record := waitNeoOrbState(t, manager, threadID, neoOrbStatePaused)
	if record.containerID != "container-fake" || record.activeGeneration == nil || !neoOrbLifecycleActionable(record.activeGeneration) || fake.callCount("create:") != 1 || fake.callCount("remove:") != 0 || fake.callCount("unpause:") != 0 {
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

func TestNeoOrbLifecycleReaperInvokedPauseErrorStaysActionableAndInert(t *testing.T) {
	fixture := newNeoOrbRunningLifecycleTestFixture(t)
	before := fixture.store.snapshot().Threads[fixture.record.threadID].Active
	if before == nil || !neoOrbLifecycleActionable(before) {
		t.Fatal("reaper fixture lifecycle is not actionable")
	}
	fixture.actor.mu.Lock()
	fixture.actor.agentState = "idle"
	fixture.actor.mu.Unlock()
	fixture.manager.mu.Lock()
	fixture.record.idleSince = time.Now().Add(-time.Hour)
	fixture.manager.mu.Unlock()
	fixture.fake.pauseErr = errors.New("pause failed")

	fixture.manager.reapRecord(fixture.record)

	after := fixture.store.snapshot()
	active := after.Threads[fixture.record.threadID].Active
	if active == nil || *active != *before || !neoOrbLifecycleActionable(active) {
		t.Fatal("invoked pause error changed durable actionable lifecycle state")
	}
	if _, exists := after.CleanupTombstones[fixture.record.threadID]; exists {
		t.Fatal("invoked pause error created a cleanup tombstone")
	}
	record, exists := fixture.manager.snapshot(fixture.record.threadID)
	if !exists || record.state != neoOrbStateConflict || record.containerID != "" || record.recovered || !record.lifecycleV1 || record.activeGeneration == nil || *record.activeGeneration != *active {
		t.Fatalf("invoked pause error was not quarantined inertly: exists=%v state=%q container=%q lifecycle=%v", exists, record.state, record.containerID, record.lifecycleV1)
	}
	if fixture.fake.callCount("pause:"+before.ContainerID) != 1 {
		t.Fatalf("pause error retried or skipped provider invocation: %#v", fixture.fake.calls)
	}
	for _, forbidden := range []string{"list-orbs", "list-orb-volumes", "ping", "create:", "create-volume:", "start:", "stop:", "unpause:", "remove:", "remove-volume:", "exec:", "exec-detached:"} {
		if fixture.fake.callCount(forbidden) != 0 {
			t.Fatalf("pause error invoked forbidden provider call %q: %#v", forbidden, fixture.fake.calls)
		}
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

func TestNeoOrbReaperKeepsLiveSessionsRunning(t *testing.T) {
	for _, test := range []struct {
		name    string
		prepare func(*neoActor, *neoOrbRecord)
	}{
		{
			name: "connected executor",
			prepare: func(actor *neoActor, _ *neoOrbRecord) {
				actor.executorID = "cli-headless-orb"
				actor.executorReady = true
			},
		},
		{
			name: "archived terminal relay",
			prepare: func(actor *neoActor, _ *neoOrbRecord) {
				actor.archived = true
				actor.terminalRelayChannels = map[string]struct{}{"terminal-channel": {}}
			},
		},
		{
			name: "archived inference",
			prepare: func(actor *neoActor, _ *neoOrbRecord) {
				actor.archived = true
				actor.currentInference = &neoInferenceInflight{messageID: "M-active"}
			},
		},
		{
			name: "archived portal",
			prepare: func(actor *neoActor, record *neoOrbRecord) {
				actor.archived = true
				record.activePortals = 1
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			rt, fake := newNeoOrbTestRuntime(t)
			threadID := "T-019fdec9-b0cf-745d-8da4-f250184e870e"
			actor := neoOrbTestActor(rt, threadID)
			t.Cleanup(actor.cancel)
			manager := rt.orbManagerFor()

			actor.spawnExecutor(map[string]any{"requestId": "spawn-session"})
			waitNeoOrbState(t, manager, threadID, neoOrbStateRunning)
			manager.mu.Lock()
			record := manager.orbs[threadID]
			record.idleSince = time.Now().Add(-time.Hour)
			manager.mu.Unlock()
			actor.mu.Lock()
			test.prepare(actor, record)
			actor.mu.Unlock()

			manager.reapRecord(record)
			if fake.callCount("pause:") != 0 {
				t.Fatalf("live session was paused: %#v", fake.calls)
			}
			if state, _ := manager.snapshot(threadID); state.state != neoOrbStateRunning {
				t.Fatalf("record state = %q, want running", state.state)
			}
		})
	}
}

func TestNeoOrbRecoveryDoesNotStartReaperOrPauseLegacyContainer(t *testing.T) {
	rt, fake := newNeoOrbTestRuntime(t)
	threadID := "T-019fdec9-b0cf-745d-8da4-f250184e870e"
	writeNeoOrbPersistedThread(t, rt, threadID, "sandbox", "recovered-container")
	fake.containers = []neoOrbContainerSummary{{ID: "recovered-container", Labels: map[string]string{"cliproxy.orb": threadID}}}
	manager := newNeoOrbManager(rt)
	manager.newClient = func(host string) (neoOrbProviderClient, error) { return fake, nil }
	manager.client = fake
	rt.orbManager = manager
	if err := manager.ensureRecovered(rt.configSnapshot()); err != nil {
		t.Fatalf("ensureRecovered: %v", err)
	}
	record, ok := manager.snapshot(threadID)
	if !ok || record.state != neoOrbStateConflict || !record.recovered || manager.reaping {
		t.Fatalf("recovered legacy reaper state = %#v, %v, reaping=%v", record, ok, manager.reaping)
	}
	manager.reapRecord(manager.live(threadID))
	if fake.callCount("pause:") != 0 || fake.callCount("inspect:") != 0 {
		t.Fatalf("recovered legacy container was accessed: %#v", fake.calls)
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
	env := neoOrbExecutorEnv(cfg, "T-019fdec9-b0cf-745d-8da4-f250184e870e", "/home/user/workspace/repo", "portal-token")
	joined := strings.Join(env, "\n")
	for _, want := range []string{
		"AMP_URL=http://host.docker.internal:8317",
		"AMP_ORB_PORTAL_BASE_URL=http://127.0.0.1:8317",
		"AMP_ORB_PORTAL_TOKEN=portal-token",
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
	env := strings.Join(neoOrbExecutorEnv(cfg, "T-019fdec9-b0cf-745d-8da4-f250184e870e", "/work", "portal-token"), "\n")
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

func TestNeoOrbGitHubAuthUsesPrivateHostsFileNotArgs(t *testing.T) {
	fake := &neoOrbFakeProvider{}
	manager := &neoOrbManager{}
	cfg := &config.Config{AmpCode: config.AmpCode{Orbs: config.AmpOrbs{Env: map[string]string{"GH_TOKEN": "ghp_secret"}}}}
	if err := manager.orbConfigureGitHubAuth(context.Background(), cfg, fake, "c1"); err != nil {
		t.Fatalf("orbConfigureGitHubAuth: %v", err)
	}
	if fake.callCount("copy:/root/.config/gh/hosts.yml") != 1 || fake.callCount("exec:/bin/sh -lc set -eu") != 1 {
		t.Fatalf("GitHub credential configuration missing: %#v", fake.calls)
	}
	for _, call := range fake.calls {
		if strings.Contains(call, "ghp_secret") {
			t.Fatalf("token leaked into command args: %#v", call)
		}
	}
	if fake.hasSensitiveCommandArg() {
		t.Fatal("GitHub credential reached provider command args")
	}
	if _, retained := fake.copiedFiles["/root/.config/gh/hosts.yml"]; retained {
		t.Fatal("GitHub credential body retained by fake provider")
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
		"http://user:invalid-scheme-secret@github.com/a/b.git",
		"https://user:userinfo-secret@github.com/a/b.git",
		"https://github.com/a/b.git?token=query-secret",
		"https://github.com/a/b.git#fragment-secret",
		"git@github.com:a/b.git?token=git-query-secret",
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
		got, err := neoOrbSanitizeCloneURL(context.Background(), blocked)
		if err == nil {
			t.Fatalf("neoOrbSanitizeCloneURL(%q) = %q, want error", blocked, got)
		}
		for _, secret := range []string{"invalid-scheme-secret", "userinfo-secret", "query-secret", "fragment-secret", "git-query-secret"} {
			if strings.Contains(err.Error(), secret) {
				t.Fatalf("neoOrbSanitizeCloneURL error retained credential data: %v", err)
			}
		}
	}
}

func TestNeoOrbConfigureWorkspaceRejectsCredentialBearingURLBeforeProvider(t *testing.T) {
	fake := &neoOrbFakeProvider{}
	manager := &neoOrbManager{}
	repositoryURL := "https://user:workspace-secret@github.com/a/b.git?token=query-secret"
	err := manager.orbConfigureWorkspace(t.Context(), fake, "container", repositoryURL, neoOrbWorkDir)
	if err == nil {
		t.Fatal("credential-bearing repository URL was accepted")
	}
	if strings.Contains(err.Error(), "workspace-secret") || strings.Contains(err.Error(), "query-secret") {
		t.Fatalf("workspace URL error retained credential data: %v", err)
	}
	if len(fake.calls) != 0 {
		t.Fatalf("credential-bearing repository URL reached provider: %#v", fake.calls)
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
	if !strings.Contains(amd64, "/usr/bin/google-chrome") {
		t.Fatal("amd64 Chrome is not exposed through a path agent-browser doctor discovers")
	}
	if !strings.Contains(amd64, "supervisor") {
		t.Fatal("orb toolchain does not install the unprivileged service supervisor")
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
	var installScript string
	fake := &neoOrbFakeProvider{
		execHandler: func(cmd []string) neoOrbExecResult {
			installCommand = strings.Join(cmd, " ")
			if len(cmd) == 3 && cmd[0] == "/bin/sh" && cmd[1] == "-lc" {
				installScript = cmd[2]
			}
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
		`if [ "$AMPBIN" != /usr/local/bin/amp ]; then cp "$AMPBIN" /usr/local/bin/amp; fi`,
		"chmod 0755 /usr/local/bin/amp",
	} {
		if !strings.Contains(installCommand, required) {
			t.Fatalf("install command missing %q: %s", required, installCommand)
		}
	}
	if installScript == "" {
		t.Fatalf("installer shell script not captured: %s", installCommand)
	}
	shell, err := exec.LookPath("sh")
	if err != nil {
		t.Skip("POSIX shell is unavailable")
	}
	t.Run("skips destination self-copy", func(t *testing.T) {
		testScript := `set -eu
curl() { :; }
bash() { :; }
command() { printf '%s\n' /usr/local/bin/amp; }
cp() { printf 'self-copy attempted\n' >&2; return 91; }
chmod() { [ "$1" = 0755 ] && [ "$2" = /usr/local/bin/amp ]; }
` + installScript
		if output, errRun := exec.Command(shell, "-c", testScript).CombinedOutput(); errRun != nil {
			t.Fatalf("installer script failed with destination already selected: %v\n%s", errRun, output)
		}
	})
	t.Run("copies distinct installer path", func(t *testing.T) {
		testScript := `set -eu
copied=
curl() { :; }
bash() { :; }
command() { printf '%s\n' /root/.amp/bin/amp; }
cp() {
  [ "$1" = /root/.amp/bin/amp ] && [ "$2" = /usr/local/bin/amp ] || return 92
  copied=1
}
chmod() { [ "$1" = 0755 ] && [ "$2" = /usr/local/bin/amp ]; }
` + installScript + `
[ "$copied" = 1 ]`
		if output, errRun := exec.Command(shell, "-c", testScript).CombinedOutput(); errRun != nil {
			t.Fatalf("installer script did not copy the distinct installer path: %v\n%s", errRun, output)
		}
	})
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

func TestNeoOrbProvisionFailurePreservesLaunchingContainer(t *testing.T) {
	rt, fake := newNeoOrbTestRuntime(t)
	threadID := "T-019fdec9-b0cf-745d-8da4-f250184e870e"
	actor := neoOrbTestActor(rt, threadID)
	t.Cleanup(actor.cancel)
	manager := rt.orbManagerFor()
	cloneInvoked := make(chan struct{})
	var cloneOnce sync.Once
	fake.execHandler = func(cmd []string) neoOrbExecResult {
		if strings.HasPrefix(strings.Join(cmd, " "), "git clone") {
			cloneOnce.Do(func() { close(cloneInvoked) })
			return neoOrbExecResult{ExitCode: 128, Stderr: "fatal: repository not found"}
		}
		return neoOrbExecResult{ExitCode: 0}
	}

	actor.spawnExecutor(map[string]any{"requestId": "spawn-fail", "repositoryURL": "https://example.test/gone.git"})
	select {
	case <-cloneInvoked:
	case <-time.After(time.Second):
		t.Fatal("failed provisioning did not invoke the workspace clone")
	}
	record := manager.live(threadID)
	if record == nil || record.operationMu == nil {
		t.Fatalf("failed provisioning record is unavailable: %#v", record)
	}
	record.operationMu.Lock()
	record.operationMu.Unlock()
	snapshot, ok := manager.snapshot(threadID)
	if !ok || snapshot.state != neoOrbStateConflict || snapshot.containerID != "" || !snapshot.lifecycleV1 || snapshot.activeGeneration == nil {
		t.Fatalf("failed provisioning record = %#v", snapshot)
	}
	active := snapshot.activeGeneration
	if active.ActivationState != neoOrbLifecycleActivationLaunching || active.OperationKind != neoOrbLifecycleOperationStart || active.OperationID == "" || neoOrbLifecycleActionable(active) {
		t.Fatalf("failed provisioning activation = %#v", active)
	}
	if binding := readNeoOrbPersistedBinding(t, rt, threadID); binding != active.ContainerID {
		t.Fatalf("durable launching container binding = %q, want %q", binding, active.ContainerID)
	}
	beforeRetry := append([]string(nil), fake.calls...)
	if result := actor.spawnExecutor(map[string]any{"requestId": "spawn-after-fail"}); stringValue(result["status"]) != "failed" {
		t.Fatalf("launching conflict retry = %#v", result)
	}
	if !reflect.DeepEqual(fake.calls, beforeRetry) {
		t.Fatalf("launching conflict retried provider work: before=%#v after=%#v", beforeRetry, fake.calls)
	}
	for _, forbidden := range []string{"stop:", "remove:", "remove-volume:"} {
		if fake.callCount(forbidden) != 0 {
			t.Fatalf("launching conflict performed cleanup via %q: %#v", forbidden, fake.calls)
		}
	}
	if fake.callCount("create:") != 1 || fake.callCount("create-volume:") != 2 {
		t.Fatalf("launching conflict created replacement resources: %#v", fake.calls)
	}
}

func TestNeoOrbBindingPersistenceFailureStaysInert(t *testing.T) {
	rt, fake := newNeoOrbTestRuntime(t)
	threadID := "T-019fdec9-b0cf-745d-8da4-f250184e870e"
	actor := neoOrbTestActor(rt, threadID)
	t.Cleanup(actor.cancel)
	rt.writeLocalSnapshot = func(neoCloudThreadSnapshot, string) (int64, error) {
		return 0, errors.New("injected binding persistence failure")
	}

	actor.spawnExecutor(map[string]any{"requestId": "spawn-persist-fail"})
	record := waitNeoOrbState(t, rt.orbManagerFor(), threadID, neoOrbStateConflict)
	if record.containerID != "" || !record.lifecycleV1 || record.activeGeneration == nil || record.activeGeneration.ActivationState != neoOrbLifecycleActivationBinding || neoOrbLifecycleActionable(record.activeGeneration) {
		t.Fatalf("persistence failure record=%#v calls=%#v", record, fake.calls)
	}
	if fake.callCount("create:") != 1 || fake.callCount("create-volume:") != 2 {
		t.Fatalf("binding uncertainty created replacement resources: %#v", fake.calls)
	}
	for _, forbidden := range []string{"start:", "stop:", "unpause:", "exec:", "exec-detached:", "remove:", "remove-volume:"} {
		if fake.callCount(forbidden) != 0 {
			t.Fatalf("binding uncertainty reached provider mutation %q: %#v", forbidden, fake.calls)
		}
	}
	actor.mu.Lock()
	binding := strings.TrimSpace(stringValue(actor.meta[neoOrbContainerIDMetaKey]))
	portalToken := strings.TrimSpace(stringValue(actor.meta[neoOrbPortalTokenMetaKey]))
	_, revisionExact := neoOrbLifecycleRevision(actor.meta[neoOrbLifecycleRevisionMetaKey])
	actor.mu.Unlock()
	if binding != "" || portalToken != "" || revisionExact {
		t.Fatalf("uncertain binding remained in actor metadata: container=%q portal=%t revision=%t", binding, portalToken != "", revisionExact)
	}
	beforeRetry := append([]string(nil), fake.calls...)
	if result := actor.spawnExecutor(map[string]any{"requestId": "spawn-persist-retry"}); stringValue(result["status"]) != "failed" {
		t.Fatalf("binding uncertainty retry = %#v", result)
	}
	if !reflect.DeepEqual(fake.calls, beforeRetry) {
		t.Fatalf("binding uncertainty retried provider work: before=%#v after=%#v", beforeRetry, fake.calls)
	}
}

func TestNeoOrbContainerBindingCleanupOnlyClearsMatchingBinding(t *testing.T) {
	rt, _ := newNeoOrbTestRuntime(t)
	threadID := "T-019fdec9-b0cf-745d-8da4-f250184e870e"
	actor := neoOrbTestActor(rt, threadID)
	t.Cleanup(actor.cancel)
	manager := rt.orbManagerFor()
	portalToken := strings.Repeat("p", neoOrbPortalTokenByteCount)
	if err := manager.persistContainerBinding(actor, "replacement-container", portalToken); err != nil {
		t.Fatalf("persist replacement binding: %v", err)
	}

	manager.clearContainerBinding(actor, "removed-container")
	if binding := readNeoOrbPersistedBinding(t, rt, threadID); binding != "replacement-container" {
		t.Fatalf("nonmatching cleanup changed binding: %q", binding)
	}
	if token := readNeoOrbPersistedPortalToken(t, rt, threadID); token != portalToken {
		t.Fatalf("nonmatching cleanup changed portal token: %q", token)
	}
	manager.clearContainerBinding(actor, "replacement-container")
	if binding := readNeoOrbPersistedBinding(t, rt, threadID); binding != "" {
		t.Fatalf("matching cleanup retained binding: %q", binding)
	}
	if token := readNeoOrbPersistedPortalToken(t, rt, threadID); token != "" {
		t.Fatalf("matching cleanup retained portal token: %q", token)
	}
}

func TestNeoOrbPortalTokenStaysLocal(t *testing.T) {
	portalToken := strings.Repeat("p", neoOrbPortalTokenByteCount)
	snapshot := neoCloudThreadSnapshot{
		threadID: "T-019fdec9-b0cf-745d-8da4-f250184e870e",
		meta: map[string]any{
			neoOrbContainerIDMetaKey: "container-fake",
			neoOrbPortalTokenMetaKey: portalToken,
		},
	}
	if token := stringValue(mapValue(neoLocalPersistedThread(snapshot, nil)["meta"])[neoOrbPortalTokenMetaKey]); token != portalToken {
		t.Fatalf("local snapshot portal token = %q", token)
	}
	for name, thread := range map[string]map[string]any{
		"cloud": neoCloudThread(snapshot),
		"web":   neoWebLocalThread(snapshot),
	} {
		if token := stringValue(mapValue(thread["meta"])[neoOrbPortalTokenMetaKey]); token != "" {
			t.Fatalf("%s thread exposed portal token %q", name, token)
		}
	}
	actor := &neoActor{threadID: snapshot.threadID, meta: cloneMap(snapshot.meta), settings: map[string]any{}, tools: map[string]neoToolSpec{}}
	if token := stringValue(mapValue(actor.stateSnapshotResponse()["meta"])[neoOrbPortalTokenMetaKey]); token != "" {
		t.Fatalf("actor state exposed portal token %q", token)
	}
	status, _ := neoRecentThreadStatusFromThreadMap(map[string]any{
		"id":   snapshot.threadID,
		"meta": cloneMap(snapshot.meta),
	})
	if token := stringValue(mapValue(status["meta"])[neoOrbPortalTokenMetaKey]); token != "" {
		t.Fatalf("recent thread status exposed portal token %q", token)
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
	env := strings.Join(neoOrbExecutorEnv(cfg, "T-019fdec9-b0cf-745d-8da4-f250184e870e", "/work", "portal-token"), "\n")
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

func TestNeoOrbExecutorEnvRequiresPublicURLForWildcardBind(t *testing.T) {
	cfg := &config.Config{Host: "0.0.0.0", Port: 8317}
	env := strings.Join(neoOrbExecutorEnv(cfg, "T-019fdec9-b0cf-745d-8da4-f250184e870e", "/work", "portal-token"), "\n")
	if !strings.Contains(env, "AMP_ORB_PORTAL_BASE_URL=\n") {
		t.Fatalf("wildcard bind exposed an unusable browser portal URL:\n%s", env)
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

func newNeoOrbActiveLifecycleFixture(t *testing.T) *neoOrbMaterializationFixture {
	t.Helper()
	fixture := newNeoOrbMaterializationFixture(t)
	if err := fixture.manager.materializeLifecycleGeneration(t.Context(), fixture.record, fixture.fake, fixture.cfg); err != nil {
		t.Fatalf("materialize active lifecycle fixture: %v", err)
	}
	fixture.fake.mu.Lock()
	fixture.fake.calls = nil
	fixture.fake.copiedFiles = nil
	fixture.fake.readFileHandler = func(context.Context, string, string, string, int) ([]byte, error) {
		return []byte("workspace-data"), nil
	}
	fixture.fake.mu.Unlock()
	return fixture
}

func beginNeoOrbActiveLifecycleTestOperation(t *testing.T, fixture *neoOrbMaterializationFixture) (*neoOrbActiveLifecycleOperation, neoOrbProviderClient) {
	t.Helper()
	operation, err := fixture.manager.beginActiveLifecycleOperation(t.Context(), fixture.record, fixture.fake, fixture.cfg)
	if err != nil {
		t.Fatalf("begin active lifecycle operation: %v", err)
	}
	return operation, operation.providerClient()
}

func invokeNeoOrbGuardedMethod(client neoOrbProviderClient, method, id string) error {
	switch method {
	case "start":
		return client.StartContainer(context.Background(), id)
	case "stop":
		return client.StopContainer(context.Background(), id)
	case "pause":
		return client.PauseContainer(context.Background(), id)
	case "unpause":
		return client.UnpauseContainer(context.Background(), id)
	case "inspect":
		_, err := client.InspectContainer(context.Background(), id)
		return err
	case "exec":
		_, err := client.Exec(context.Background(), id, []string{"true", "GH_TOKEN=guarded-argv-secret"}, []string{"SECRET_ENV=guarded"}, "/")
		return err
	case "exec-detached":
		return client.ExecDetached(context.Background(), id, []string{"true", "GH_TOKEN=guarded-argv-secret"}, []string{"SECRET_ENV=guarded"}, "/")
	case "read":
		_, err := client.ReadWorkspaceFile(context.Background(), id, neoOrbWorkDir, "result.txt", 1024)
		return err
	case "archive":
		archive, err := client.ArchiveFromContainer(context.Background(), id, "/tmp/source")
		if err != nil {
			return err
		}
		return archive.Close()
	case "copy-archive":
		return client.CopyArchiveToContainer(context.Background(), id, "/tmp", strings.NewReader("secret archive body"))
	case "copy-file":
		return client.CopyFileToContainer(context.Background(), id, "/tmp/guarded-secret", []byte("secret copied body"), 0o600)
	case "copy-tar":
		return client.CopyTarToContainer(context.Background(), id, "/tmp", map[string][]byte{"secret": []byte("secret tar body")}, 0o600)
	default:
		return fmt.Errorf("unknown guarded method %q", method)
	}
}

func neoOrbGuardedMethodTargetPrefix(method, id string) string {
	switch method {
	case "start", "stop", "pause", "unpause":
		return method + ":" + id
	case "inspect":
		return "inspect:" + id
	case "exec":
		return "exec:true"
	case "exec-detached":
		return "exec-detached:true"
	case "read":
		return "read-workspace-file:" + id
	case "archive":
		return "archive-from:" + id
	case "copy-archive":
		return "copy-archive:" + id
	case "copy-file":
		return "copy:/tmp/guarded-secret"
	case "copy-tar":
		return "copy-tar:/tmp:secret"
	default:
		return ""
	}
}

func TestNeoOrbGuardedProviderContainerMethodsAuthorizeExactResources(t *testing.T) {
	methods := []string{"start", "stop", "pause", "unpause", "inspect", "exec", "exec-detached", "read", "archive", "copy-archive", "copy-file", "copy-tar"}
	for _, method := range methods {
		t.Run(method, func(t *testing.T) {
			fixture := newNeoOrbActiveLifecycleFixture(t)
			operation, guarded := beginNeoOrbActiveLifecycleTestOperation(t, fixture)
			defer operation.close()
			id := fixture.record.activeGeneration.ContainerID
			if method == "inspect" {
				state, err := guarded.InspectContainer(t.Context(), id)
				if err != nil || state.ID != id {
					t.Fatalf("guarded inspection state=%#v err=%v", state, err)
				}
			} else if err := invokeNeoOrbGuardedMethod(guarded, method, id); err != nil {
				t.Fatalf("guarded %s: %v", method, err)
			}
			authorization := []string{
				"inspect-volume:" + fixture.record.activeGeneration.HomeVolumeName,
				"inspect-volume:" + fixture.record.activeGeneration.RootVolumeName,
				"inspect:" + id,
			}
			fixture.fake.mu.Lock()
			calls := append([]string(nil), fixture.fake.calls...)
			retained := fmt.Sprintf("%#v %#v", fixture.fake.calls, fixture.fake.copiedFiles)
			fixture.fake.mu.Unlock()
			targetPrefix := neoOrbGuardedMethodTargetPrefix(method, id)
			if method == "inspect" {
				if !reflect.DeepEqual(calls, authorization) {
					t.Fatalf("inspection authorization order = %#v, want %#v", calls, authorization)
				}
			} else if len(calls) != 7 || !reflect.DeepEqual(calls[:3], authorization) || !strings.HasPrefix(calls[3], targetPrefix) || !reflect.DeepEqual(calls[4:], authorization) {
				t.Fatalf("authorization order for %s = %#v", method, calls)
			}
			if fixture.fake.callCount(targetPrefix) != 1 {
				t.Fatalf("target count for %s = %d, calls=%#v", method, fixture.fake.callCount(targetPrefix), calls)
			}
			for _, secret := range []string{"SECRET_ENV=guarded", "guarded-argv-secret", "secret archive body", "secret copied body", "secret tar body"} {
				if strings.Contains(retained, secret) {
					t.Fatalf("guarded adapter input retained by fake: %s", secret)
				}
			}
		})

		t.Run(method+" wrong ID", func(t *testing.T) {
			fixture := newNeoOrbActiveLifecycleFixture(t)
			operation, guarded := beginNeoOrbActiveLifecycleTestOperation(t, fixture)
			defer operation.close()
			err := invokeNeoOrbGuardedMethod(guarded, method, "container-wrong")
			if !errors.Is(err, errNeoOrbActiveLifecycleGuardRejected) {
				t.Fatalf("wrong ID error = %v", err)
			}
			if len(fixture.fake.calls) != 0 {
				t.Fatalf("wrong ID reached raw provider: %#v", fixture.fake.calls)
			}
		})
	}
}

func TestNeoOrbGuardedProviderContainerMethodsPostCheck(t *testing.T) {
	methods := []string{"start", "stop", "pause", "unpause", "inspect", "exec", "exec-detached", "read", "archive", "copy-archive", "copy-file", "copy-tar"}
	for _, method := range methods {
		t.Run(method, func(t *testing.T) {
			fixture := newNeoOrbActiveLifecycleFixture(t)
			operation, guarded := beginNeoOrbActiveLifecycleTestOperation(t, fixture)
			defer operation.close()
			mutate := func() {
				fixture.manager.mu.Lock()
				fixture.record.failReason = "post-target drift"
				fixture.manager.mu.Unlock()
			}
			if method == "inspect" {
				fixture.fake.inspectContainerHook = func(context.Context, string) { mutate() }
			} else {
				fixture.fake.containerTargetHook = func(string) { mutate() }
			}
			id := fixture.record.activeGeneration.ContainerID
			err := invokeNeoOrbGuardedMethod(guarded, method, id)
			if !errors.Is(err, errNeoOrbActiveLifecycleGuardRejected) {
				t.Fatalf("post-check error = %v", err)
			}
			if fixture.fake.callCount(neoOrbGuardedMethodTargetPrefix(method, id)) != 1 {
				t.Fatalf("post-check target count calls=%#v", fixture.fake.calls)
			}
		})
	}
}

func TestNeoOrbGuardedProviderInvocationResults(t *testing.T) {
	t.Run("preauthorization rejection", func(t *testing.T) {
		fixture := newNeoOrbActiveLifecycleFixture(t)
		operation, guardedClient := beginNeoOrbActiveLifecycleTestOperation(t, fixture)
		defer operation.close()
		guarded := guardedClient.(*neoOrbGuardedProvider)
		fixture.fake.mu.Lock()
		fixture.fake.inspectState.RestartPolicy = "no"
		fixture.fake.mu.Unlock()
		result := guarded.StartContainerInvocation(t.Context(), fixture.record.containerID)
		if result.Invoked || result.ProviderOK || result.PostAuthorized || !errors.Is(result.Err, errNeoOrbActiveLifecycleGuardRejected) {
			t.Fatalf("preauthorization result = %#v", result)
		}
		if fixture.fake.callCount("start:") != 0 {
			t.Fatalf("preauthorization rejection reached target: %#v", fixture.fake.calls)
		}
	})

	t.Run("provider error", func(t *testing.T) {
		fixture := newNeoOrbActiveLifecycleFixture(t)
		operation, guardedClient := beginNeoOrbActiveLifecycleTestOperation(t, fixture)
		defer operation.close()
		guarded := guardedClient.(*neoOrbGuardedProvider)
		providerErr := errors.New("provider start failed")
		fixture.fake.startHandler = func(string) error { return providerErr }
		result := guarded.StartContainerInvocation(t.Context(), fixture.record.containerID)
		if !result.Invoked || result.ProviderOK || !result.PostAuthorized || !errors.Is(result.Err, providerErr) || result.confirmed() {
			t.Fatalf("provider error result = %#v", result)
		}
		if fixture.fake.callCount("start:") != 1 {
			t.Fatalf("provider error target calls = %#v", fixture.fake.calls)
		}
	})

	t.Run("postauthorization drift", func(t *testing.T) {
		fixture := newNeoOrbActiveLifecycleFixture(t)
		operation, guardedClient := beginNeoOrbActiveLifecycleTestOperation(t, fixture)
		defer operation.close()
		guarded := guardedClient.(*neoOrbGuardedProvider)
		fixture.fake.containerTargetHook = func(call string) {
			if strings.HasPrefix(call, "start:") {
				fixture.manager.mu.Lock()
				fixture.record.failReason = "postauthorization drift"
				fixture.manager.mu.Unlock()
			}
		}
		result := guarded.StartContainerInvocation(t.Context(), fixture.record.containerID)
		if !result.Invoked || !result.ProviderOK || result.PostAuthorized || !errors.Is(result.Err, errNeoOrbActiveLifecycleGuardRejected) || result.confirmed() {
			t.Fatalf("postauthorization result = %#v", result)
		}
		if fixture.fake.callCount("start:") != 1 {
			t.Fatalf("postauthorization target calls = %#v", fixture.fake.calls)
		}
	})

	t.Run("success", func(t *testing.T) {
		fixture := newNeoOrbActiveLifecycleFixture(t)
		operation, guardedClient := beginNeoOrbActiveLifecycleTestOperation(t, fixture)
		defer operation.close()
		guarded := guardedClient.(*neoOrbGuardedProvider)
		result := guarded.StartContainerInvocation(t.Context(), fixture.record.containerID)
		if !result.Invoked || !result.ProviderOK || !result.PostAuthorized || result.Err != nil || !result.confirmed() {
			t.Fatalf("successful invocation result = %#v", result)
		}
		if fixture.fake.callCount("start:") != 1 {
			t.Fatalf("successful target calls = %#v", fixture.fake.calls)
		}
	})
}

func TestNeoOrbGuardedProviderRejectsResourceDriftAfterTarget(t *testing.T) {
	methods := []string{"start", "stop", "pause", "unpause", "inspect", "exec", "exec-detached", "read", "archive", "copy-archive", "copy-file", "copy-tar"}
	for _, method := range methods {
		t.Run(method, func(t *testing.T) {
			fixture := newNeoOrbActiveLifecycleFixture(t)
			operation, guarded := beginNeoOrbActiveLifecycleTestOperation(t, fixture)
			defer operation.close()
			drift := func() {
				fixture.fake.mu.Lock()
				fixture.fake.inspectState.RestartPolicy = "no"
				fixture.fake.mu.Unlock()
			}
			if method == "inspect" {
				fixture.fake.inspectContainerHook = func(context.Context, string) {
					drift()
				}
			} else {
				fixture.fake.containerTargetHook = func(call string) {
					if strings.HasPrefix(call, neoOrbGuardedMethodTargetPrefix(method, fixture.record.containerID)) {
						drift()
					}
				}
			}
			err := invokeNeoOrbGuardedMethod(guarded, method, fixture.record.containerID)
			if !errors.Is(err, errNeoOrbActiveLifecycleGuardRejected) {
				t.Fatalf("post-target resource drift error = %v", err)
			}
			expectedAuthorizations := 2
			if method == "inspect" {
				expectedAuthorizations = 1
			}
			if fixture.fake.callCount("inspect-volume:"+fixture.record.activeGeneration.HomeVolumeName) != expectedAuthorizations || fixture.fake.callCount("inspect-volume:"+fixture.record.activeGeneration.RootVolumeName) != expectedAuthorizations {
				t.Fatalf("post-target resource authorization calls = %#v", fixture.fake.calls)
			}
		})
	}
}

func TestNeoOrbGuardedProviderRejectsUnscopedMethodsWithoutRawCalls(t *testing.T) {
	tests := map[string]func(neoOrbProviderClient) error{
		"ping": func(client neoOrbProviderClient) error { return client.Ping(context.Background()) },
		"ensure image": func(client neoOrbProviderClient) error {
			return client.EnsureImage(context.Background(), "secret-image")
		},
		"create container": func(client neoOrbProviderClient) error {
			_, err := client.CreateContainer(context.Background(), neoOrbContainerSpec{Name: "forbidden"})
			return err
		},
		"list containers": func(client neoOrbProviderClient) error {
			_, err := client.ListOrbContainers(context.Background())
			return err
		},
		"remove container": func(client neoOrbProviderClient) error {
			return client.RemoveContainer(context.Background(), "exact", true)
		},
		"create volume": func(client neoOrbProviderClient) error {
			_, err := client.CreateVolume(context.Background(), neoOrbVolumeSpec{Name: "forbidden"})
			return err
		},
		"inspect volume": func(client neoOrbProviderClient) error {
			_, err := client.InspectVolume(context.Background(), "exact")
			return err
		},
		"list volumes": func(client neoOrbProviderClient) error {
			_, err := client.ListOrbVolumes(context.Background())
			return err
		},
		"remove volume": func(client neoOrbProviderClient) error { return client.RemoveVolume(context.Background(), "exact") },
	}
	for name, invoke := range tests {
		t.Run(name, func(t *testing.T) {
			fixture := newNeoOrbActiveLifecycleFixture(t)
			operation, guarded := beginNeoOrbActiveLifecycleTestOperation(t, fixture)
			defer operation.close()
			if err := invoke(guarded); !errors.Is(err, errNeoOrbActiveLifecycleGuardRejected) {
				t.Fatalf("rejected method error = %v", err)
			}
			if len(fixture.fake.calls) != 0 {
				t.Fatalf("rejected method reached raw provider: %#v", fixture.fake.calls)
			}
		})
	}
}

type neoOrbGuardedTestReadCloser struct {
	reader *strings.Reader
	mu     sync.Mutex
	closed int
}

func (stream *neoOrbGuardedTestReadCloser) Read(buffer []byte) (int, error) {
	stream.mu.Lock()
	defer stream.mu.Unlock()
	return stream.reader.Read(buffer)
}

func (stream *neoOrbGuardedTestReadCloser) Close() error {
	stream.mu.Lock()
	stream.closed++
	stream.mu.Unlock()
	return nil
}

func (stream *neoOrbGuardedTestReadCloser) closeCount() int {
	stream.mu.Lock()
	defer stream.mu.Unlock()
	return stream.closed
}

type neoOrbGuardedBlockingReadCloser struct {
	started     chan struct{}
	unblock     chan struct{}
	startedOnce sync.Once
	closeOnce   sync.Once
	mu          sync.Mutex
	closed      int
}

func (stream *neoOrbGuardedBlockingReadCloser) Read([]byte) (int, error) {
	stream.startedOnce.Do(func() { close(stream.started) })
	<-stream.unblock
	return 0, io.ErrClosedPipe
}

func (stream *neoOrbGuardedBlockingReadCloser) Close() error {
	stream.closeOnce.Do(func() {
		stream.mu.Lock()
		stream.closed++
		stream.mu.Unlock()
		close(stream.unblock)
	})
	return nil
}

func (stream *neoOrbGuardedBlockingReadCloser) closeCount() int {
	stream.mu.Lock()
	defer stream.mu.Unlock()
	return stream.closed
}

func TestNeoOrbGuardedArchiveClosesOnStaleAndClosedLease(t *testing.T) {
	for _, test := range []struct {
		name  string
		stale bool
	}{
		{name: "stale", stale: true},
		{name: "closed lease"},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newNeoOrbActiveLifecycleFixture(t)
			rawStream := &neoOrbGuardedTestReadCloser{reader: strings.NewReader("archive")}
			fixture.fake.archiveFrom = func(context.Context, string, string) (io.ReadCloser, error) { return rawStream, nil }
			operation, guarded := beginNeoOrbActiveLifecycleTestOperation(t, fixture)
			archive, err := guarded.ArchiveFromContainer(t.Context(), fixture.record.containerID, "/tmp/source")
			if err != nil {
				t.Fatalf("open guarded archive: %v", err)
			}
			if test.stale {
				fixture.manager.mu.Lock()
				fixture.record.failReason = "stale archive"
				fixture.manager.mu.Unlock()
			} else {
				operation.close()
			}
			if _, err := archive.Read(make([]byte, 1)); !errors.Is(err, errNeoOrbActiveLifecycleGuardRejected) {
				t.Fatalf("stale archive read error = %v", err)
			}
			if rawStream.closeCount() != 1 {
				t.Fatalf("raw archive close count = %d", rawStream.closeCount())
			}
			_ = archive.Close()
			operation.close()
			if rawStream.closeCount() != 1 {
				t.Fatalf("raw archive closed repeatedly: %d", rawStream.closeCount())
			}
		})
	}
}

func TestNeoOrbGuardedArchiveCloseRevokesBlockedRead(t *testing.T) {
	fixture := newNeoOrbActiveLifecycleFixture(t)
	rawStream := &neoOrbGuardedBlockingReadCloser{started: make(chan struct{}), unblock: make(chan struct{})}
	fixture.fake.archiveFrom = func(context.Context, string, string) (io.ReadCloser, error) { return rawStream, nil }
	operation, guarded := beginNeoOrbActiveLifecycleTestOperation(t, fixture)
	archive, err := guarded.ArchiveFromContainer(t.Context(), fixture.record.containerID, "/tmp/source")
	if err != nil {
		operation.close()
		t.Fatalf("open guarded archive: %v", err)
	}
	readDone := make(chan error, 1)
	go func() {
		_, readErr := archive.Read(make([]byte, 1))
		readDone <- readErr
	}()
	select {
	case <-rawStream.started:
	case <-time.After(time.Second):
		operation.close()
		t.Fatal("guarded archive read did not block")
	}
	closeDone := make(chan struct{})
	go func() {
		operation.close()
		close(closeDone)
	}()
	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("operation close did not revoke blocked archive read")
	}
	select {
	case readErr := <-readDone:
		if !errors.Is(readErr, errNeoOrbActiveLifecycleGuardRejected) {
			t.Fatalf("revoked archive read error = %v", readErr)
		}
	case <-time.After(time.Second):
		t.Fatal("revoked archive read did not return")
	}
	if rawStream.closeCount() != 1 {
		t.Fatalf("raw archive close count = %d", rawStream.closeCount())
	}
	if !fixture.record.operationMu.TryLock() {
		t.Fatal("closed archive lease retained operation lock")
	}
	fixture.record.operationMu.Unlock()
	_ = archive.Close()
}

func TestNeoOrbActiveLifecycleOperationCancellationCloseAndLocks(t *testing.T) {
	t.Run("canceled before begin", func(t *testing.T) {
		fixture := newNeoOrbActiveLifecycleFixture(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := fixture.manager.beginActiveLifecycleOperation(ctx, fixture.record, fixture.fake, fixture.cfg); !errors.Is(err, context.Canceled) || !errors.Is(err, errNeoOrbActiveLifecycleGuardRejected) {
			t.Fatalf("canceled begin error = %v", err)
		}
		if len(fixture.fake.calls) != 0 {
			t.Fatalf("canceled begin reached provider: %#v", fixture.fake.calls)
		}
	})

	t.Run("canceled during inspection", func(t *testing.T) {
		fixture := newNeoOrbActiveLifecycleFixture(t)
		ctx, cancel := context.WithCancel(context.Background())
		operation, err := fixture.manager.beginActiveLifecycleOperation(ctx, fixture.record, fixture.fake, fixture.cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer operation.close()
		fixture.fake.inspectVolumeHook = func(context.Context, string) { cancel() }
		err = operation.providerClient().StartContainer(ctx, fixture.record.containerID)
		if !errors.Is(err, context.Canceled) || fixture.fake.callCount("start:") != 0 {
			t.Fatalf("inspection cancellation error=%v calls=%#v", err, fixture.fake.calls)
		}
	})

	t.Run("canceled during target", func(t *testing.T) {
		fixture := newNeoOrbActiveLifecycleFixture(t)
		ctx, cancel := context.WithCancel(context.Background())
		operation, err := fixture.manager.beginActiveLifecycleOperation(ctx, fixture.record, fixture.fake, fixture.cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer operation.close()
		fixture.fake.startHandler = func(string) error {
			cancel()
			return nil
		}
		err = operation.providerClient().StartContainer(ctx, fixture.record.containerID)
		if !errors.Is(err, context.Canceled) || fixture.fake.callCount("start:") != 1 {
			t.Fatalf("target cancellation error=%v calls=%#v", err, fixture.fake.calls)
		}
		for _, forbidden := range []string{"stop:", "remove:", "remove-volume:"} {
			if fixture.fake.callCount(forbidden) != 0 {
				t.Fatalf("cancellation cleaned up via %s: %#v", forbidden, fixture.fake.calls)
			}
		}
	})

	t.Run("closed lease", func(t *testing.T) {
		fixture := newNeoOrbActiveLifecycleFixture(t)
		operation, guarded := beginNeoOrbActiveLifecycleTestOperation(t, fixture)
		operation.close()
		operation.close()
		if err := guarded.StartContainer(context.Background(), fixture.record.containerID); !errors.Is(err, errNeoOrbActiveLifecycleGuardRejected) {
			t.Fatalf("closed lease error = %v", err)
		}
		if !fixture.record.operationMu.TryLock() {
			t.Fatal("closed lease did not release operation lock")
		}
		fixture.record.operationMu.Unlock()
		if len(fixture.fake.calls) != 0 {
			t.Fatalf("closed lease reached provider: %#v", fixture.fake.calls)
		}
	})

	t.Run("raw call lock boundary", func(t *testing.T) {
		fixture := newNeoOrbActiveLifecycleFixture(t)
		operation, guarded := beginNeoOrbActiveLifecycleTestOperation(t, fixture)
		defer operation.close()
		fixture.fake.containerTargetHook = func(string) {
			if !fixture.manager.mu.TryLock() {
				t.Error("manager mutex held across raw target")
			} else {
				fixture.manager.mu.Unlock()
			}
			if !fixture.runtime.orbManagerMu.TryLock() {
				t.Error("runtime orb manager mutex held across raw target")
			} else {
				fixture.runtime.orbManagerMu.Unlock()
			}
			if fixture.record.operationMu.TryLock() {
				fixture.record.operationMu.Unlock()
				t.Error("operation mutex was not held across raw target")
			}
		}
		if err := guarded.StartContainer(t.Context(), fixture.record.containerID); err != nil {
			t.Fatal(err)
		}
	})
}

func TestNeoOrbActiveLifecycleOperationRejectsOwnershipDriftBeforeProvider(t *testing.T) {
	tests := map[string]func(*neoOrbMaterializationFixture) (*config.Config, neoOrbProviderClient){
		"missing operation mutex": func(fixture *neoOrbMaterializationFixture) (*config.Config, neoOrbProviderClient) {
			fixture.record.operationMu = nil
			return fixture.cfg, fixture.fake
		},
		"runtime manager": func(fixture *neoOrbMaterializationFixture) (*config.Config, neoOrbProviderClient) {
			fixture.runtime.orbManagerMu.Lock()
			fixture.runtime.orbManager = newNeoOrbManager(fixture.runtime)
			fixture.runtime.orbManagerMu.Unlock()
			return fixture.cfg, fixture.fake
		},
		"runtime store error": func(fixture *neoOrbMaterializationFixture) (*config.Config, neoOrbProviderClient) {
			fixture.runtime.orbManagerMu.Lock()
			fixture.runtime.orbLifecycleStoreErr = errors.New("sticky")
			fixture.runtime.orbManagerMu.Unlock()
			return fixture.cfg, fixture.fake
		},
		"config": func(fixture *neoOrbMaterializationFixture) (*config.Config, neoOrbProviderClient) {
			copy := *fixture.cfg
			return &copy, fixture.fake
		},
		"manager client": func(fixture *neoOrbMaterializationFixture) (*config.Config, neoOrbProviderClient) {
			fixture.manager.client = &neoOrbFakeProvider{}
			return fixture.cfg, fixture.fake
		},
		"manager provider": func(fixture *neoOrbMaterializationFixture) (*config.Config, neoOrbProviderClient) {
			fixture.manager.clientKey = "tcp://foreign.test:2375"
			return fixture.cfg, fixture.fake
		},
		"manager recovery": func(fixture *neoOrbMaterializationFixture) (*config.Config, neoOrbProviderClient) {
			fixture.manager.recovered = false
			return fixture.cfg, fixture.fake
		},
		"record identity": func(fixture *neoOrbMaterializationFixture) (*config.Config, neoOrbProviderClient) {
			copy := *fixture.record
			fixture.manager.orbs[fixture.record.threadID] = &copy
			return fixture.cfg, fixture.fake
		},
		"record fields": func(fixture *neoOrbMaterializationFixture) (*config.Config, neoOrbProviderClient) {
			fixture.record.workDir = "/foreign"
			return fixture.cfg, fixture.fake
		},
		"record recovered": func(fixture *neoOrbMaterializationFixture) (*config.Config, neoOrbProviderClient) {
			fixture.record.recovered = true
			return fixture.cfg, fixture.fake
		},
		"record phase": func(fixture *neoOrbMaterializationFixture) (*config.Config, neoOrbProviderClient) {
			fixture.record.activeGeneration.Phase = neoOrbLifecyclePhaseRetained
			return fixture.cfg, fixture.fake
		},
		"record pending": func(fixture *neoOrbMaterializationFixture) (*config.Config, neoOrbProviderClient) {
			pending := *fixture.record.activeGeneration
			pending.Phase = neoOrbLifecyclePhasePending
			pending.ContainerID = ""
			fixture.record.pendingGeneration = &pending
			return fixture.cfg, fixture.fake
		},
		"record container": func(fixture *neoOrbMaterializationFixture) (*config.Config, neoOrbProviderClient) {
			fixture.record.containerID = "container-foreign"
			return fixture.cfg, fixture.fake
		},
		"store active": func(fixture *neoOrbMaterializationFixture) (*config.Config, neoOrbProviderClient) {
			if err := fixture.store.replaceActiveContainerID(*fixture.record.activeGeneration, "container-authoritative"); err != nil {
				t.Fatalf("replace active: %v", err)
			}
			return fixture.cfg, fixture.fake
		},
		"store pending": func(fixture *neoOrbMaterializationFixture) (*config.Config, neoOrbProviderClient) {
			if _, err := fixture.store.reserveGeneration(fixture.record.threadID, "pending-token"); err != nil {
				t.Fatalf("reserve pending: %v", err)
			}
			return fixture.cfg, fixture.fake
		},
		"store tombstone": func(fixture *neoOrbMaterializationFixture) (*config.Config, neoOrbProviderClient) {
			if _, found, err := fixture.store.beginCleanup(fixture.record.threadID); err != nil || !found {
				t.Fatalf("begin cleanup: found=%v err=%v", found, err)
			}
			return fixture.cfg, fixture.fake
		},
	}
	for name, prepare := range tests {
		t.Run(name, func(t *testing.T) {
			fixture := newNeoOrbActiveLifecycleFixture(t)
			cfg, raw := prepare(fixture)
			operation, err := fixture.manager.beginActiveLifecycleOperation(t.Context(), fixture.record, raw, cfg)
			if operation != nil {
				operation.close()
				t.Fatal("drifted operation returned a lease")
			}
			if !errors.Is(err, errNeoOrbActiveLifecycleGuardRejected) {
				t.Fatalf("drift rejection error = %v", err)
			}
			if len(fixture.fake.calls) != 0 {
				t.Fatalf("drift rejection reached provider: %#v", fixture.fake.calls)
			}
		})
	}
}

func TestNeoOrbGuardedProviderRejectsResourceDriftWithoutTarget(t *testing.T) {
	tests := map[string]func(*neoOrbMaterializationFixture){
		"home volume": func(fixture *neoOrbMaterializationFixture) {
			state := fixture.fake.volumes[fixture.record.activeGeneration.HomeVolumeName]
			state.Name += "-foreign"
			fixture.fake.volumes[fixture.record.activeGeneration.HomeVolumeName] = state
		},
		"root volume": func(fixture *neoOrbMaterializationFixture) {
			state := fixture.fake.volumes[fixture.record.activeGeneration.RootVolumeName]
			state.Labels["cliproxy.orb.owner"] = "foreign"
			fixture.fake.volumes[fixture.record.activeGeneration.RootVolumeName] = state
		},
		"container ID": func(fixture *neoOrbMaterializationFixture) {
			fixture.fake.inspectState.ID = "container-foreign"
		},
		"container name": func(fixture *neoOrbMaterializationFixture) {
			fixture.fake.inspectState.Name += "-foreign"
		},
		"container labels": func(fixture *neoOrbMaterializationFixture) {
			fixture.fake.inspectState.Labels["cliproxy.orb.role"] = "foreign"
		},
		"container mount": func(fixture *neoOrbMaterializationFixture) {
			fixture.fake.inspectState.Mounts[0].Name = "foreign"
		},
		"container mount source": func(fixture *neoOrbMaterializationFixture) {
			fixture.fake.inspectState.Mounts[0].Source = "/foreign"
		},
		"container restart": func(fixture *neoOrbMaterializationFixture) {
			fixture.fake.inspectState.RestartPolicy = "no"
		},
	}
	for name, drift := range tests {
		t.Run(name, func(t *testing.T) {
			fixture := newNeoOrbActiveLifecycleFixture(t)
			operation, guarded := beginNeoOrbActiveLifecycleTestOperation(t, fixture)
			defer operation.close()
			drift(fixture)
			err := guarded.StartContainer(t.Context(), fixture.record.containerID)
			if !errors.Is(err, errNeoOrbActiveLifecycleGuardRejected) {
				t.Fatalf("resource drift error = %v", err)
			}
			if fixture.fake.callCount("start:") != 0 {
				t.Fatalf("resource drift reached target: %#v", fixture.fake.calls)
			}
		})
	}
}

type neoOrbPrelaunchFixture struct {
	*neoOrbMaterializationFixture
	actor   *neoActor
	ownerID string
}

func newNeoOrbPrelaunchFixture(t *testing.T, ownerIDs ...string) *neoOrbPrelaunchFixture {
	t.Helper()
	fixture := newNeoOrbMaterializationFixture(t)
	fixture.cfg.AmpCode.Orbs.Env = map[string]string{}
	syncLocal := false
	fixture.cfg.AmpCode.Orbs.SyncLocalConfig = &syncLocal
	executorPath := filepath.Join(t.TempDir(), "amp-linux")
	if err := os.WriteFile(executorPath, []byte{0x7f, 'E', 'L', 'F', 1}, 0o700); err != nil {
		t.Fatal(err)
	}
	fixture.cfg.AmpCode.Orbs.ExecutorCommand = executorPath
	actor := fixture.runtime.store.ensureThreadActor(fixture.record.threadID)
	if actor == nil {
		t.Fatal("create prelaunch thread actor")
	}
	ownerID := "owner-prelaunch"
	if len(ownerIDs) == 1 {
		ownerID = ownerIDs[0]
	}
	actor.mu.Lock()
	actor.meta["ownerUserId"] = ownerID
	actor.mu.Unlock()
	if err := fixture.manager.materializeLifecycleGeneration(t.Context(), fixture.record, fixture.fake, fixture.cfg, ownerID); err != nil {
		t.Fatalf("materialize bound prelaunch lifecycle: %v", err)
	}
	fixture.manager.mu.Lock()
	active := cloneNeoOrbLifecycleGeneration(fixture.record.activeGeneration)
	fixture.manager.mu.Unlock()
	if active == nil {
		t.Fatal("bound prelaunch lifecycle is missing")
	}
	if err := fixture.manager.persistLifecycleContainerBinding(actor, *active); err != nil {
		t.Fatalf("persist bound prelaunch lifecycle: %v", err)
	}
	fixture.fake.mu.Lock()
	fixture.fake.calls = nil
	fixture.fake.copiedFiles = nil
	fixture.fake.readFileHandler = func(context.Context, string, string, string, int) ([]byte, error) {
		return []byte("workspace-data"), nil
	}
	fixture.fake.mu.Unlock()
	return &neoOrbPrelaunchFixture{neoOrbMaterializationFixture: fixture, actor: actor, ownerID: ownerID}
}

func (fixture *neoOrbPrelaunchFixture) putOwnerConfig(t *testing.T) {
	t.Helper()
	bundle, err := orbconfig.New(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.runtime.orbConfigStore.put(fixture.ownerID, bundle); err != nil {
		t.Fatal(err)
	}
}

func (fixture *neoOrbPrelaunchFixture) putOwnerCredentials(t *testing.T, githubToken string) {
	t.Helper()
	store := newTestNeoOwnerOrbCredentialStore(t)
	var identities []orbcredentials.DecodedSSHIdentity
	if githubToken == "" {
		publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		privateBlock, err := ssh.MarshalPrivateKey(privateKey, "")
		if err != nil {
			t.Fatal(err)
		}
		sshPublicKey, err := ssh.NewPublicKey(publicKey)
		if err != nil {
			t.Fatal(err)
		}
		identities = []orbcredentials.DecodedSSHIdentity{{
			Name:       "id_ed25519",
			PrivateKey: pem.EncodeToMemory(privateBlock),
			PublicKey:  ssh.MarshalAuthorizedKey(sshPublicKey),
		}}
	}
	snapshot, err := orbcredentials.New(githubToken, identities, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.put(fixture.ownerID, snapshot); err != nil {
		t.Fatal(err)
	}
	fixture.runtime.orbCredentialStore = store
}

func assertNeoOrbPrelaunchNoActivationOrCleanup(t *testing.T, fixture *neoOrbPrelaunchFixture) {
	t.Helper()
	for _, forbidden := range []string{"exec-detached:", "stop:", "remove:", "remove-volume:", "create:", "create-volume:", "list-orbs", "list-orb-volumes"} {
		if fixture.fake.callCount(forbidden) != 0 {
			t.Fatalf("prelaunch made forbidden call %q: %#v", forbidden, fixture.fake.calls)
		}
	}
	fixture.actor.mu.Lock()
	containerBinding := stringValue(fixture.actor.meta[neoOrbContainerIDMetaKey])
	portalBinding := stringValue(fixture.actor.meta[neoOrbPortalTokenMetaKey])
	revision, revisionExact := neoOrbLifecycleRevision(fixture.actor.meta[neoOrbLifecycleRevisionMetaKey])
	fixture.actor.mu.Unlock()
	active := fixture.store.snapshot().Threads[fixture.record.threadID].Active
	if active == nil || containerBinding != active.ContainerID || portalBinding != active.PortalToken || !revisionExact || revision != active.Revision {
		t.Fatalf("prelaunch changed actor binding container=%q portal=%q revision=%d active=%#v", containerBinding, portalBinding, revision, active)
	}
}

func assertNeoOrbPrelaunchFailureState(t *testing.T, fixture *neoOrbPrelaunchFixture, before neoOrbLifecycleState) {
	t.Helper()
	if after := fixture.store.snapshot(); !reflect.DeepEqual(after, before) {
		t.Fatalf("failed prelaunch changed lifecycle store:\nafter=%#v\nbefore=%#v", after, before)
	}
	record, exists := fixture.manager.snapshot(fixture.record.threadID)
	lifecycle := before.Threads[fixture.record.threadID]
	if !exists || record.state != neoOrbStateConflict || record.recovered || !record.lifecycleV1 || record.containerID != "" || record.prelaunchReady || record.preparedOwnerID != "" || record.failReason != "lifecycle-v1 prelaunch requires reconciliation" || lifecycle.Active == nil || record.activeGeneration == nil || *record.activeGeneration != *lifecycle.Active || !reflect.DeepEqual(record.pendingGeneration, lifecycle.Pending) {
		t.Fatalf("failed prelaunch record = %#v", record)
	}
	assertNeoOrbPrelaunchNoActivationOrCleanup(t, fixture)
}

func assertNeoOrbPrelaunchInvokedFailureState(t *testing.T, fixture *neoOrbPrelaunchFixture, before neoOrbLifecycleState) {
	t.Helper()
	after := fixture.store.snapshot()
	beforeLifecycle := before.Threads[fixture.record.threadID]
	afterLifecycle := after.Threads[fixture.record.threadID]
	if beforeLifecycle.Active == nil || afterLifecycle.Active == nil || afterLifecycle.Pending != nil {
		t.Fatalf("failed prelaunch lifecycle before=%#v after=%#v", beforeLifecycle, afterLifecycle)
	}
	want := *beforeLifecycle.Active
	want.ActivationState = neoOrbLifecycleActivationLaunching
	want.OperationID = afterLifecycle.Active.OperationID
	want.OperationKind = neoOrbLifecycleOperationStart
	if afterLifecycle.Active.OperationID == "" || *afterLifecycle.Active != want {
		t.Fatalf("failed prelaunch activation = %#v, want %#v", afterLifecycle.Active, want)
	}
	record, exists := fixture.manager.snapshot(fixture.record.threadID)
	if !exists || record.state != neoOrbStateConflict || record.recovered || !record.lifecycleV1 || record.containerID != "" || record.prelaunchReady || record.preparedOwnerID != "" || record.activeGeneration == nil || *record.activeGeneration != *afterLifecycle.Active || record.pendingGeneration != nil || neoOrbLifecycleActionable(record.activeGeneration) {
		t.Fatalf("failed prelaunch record = %#v", record)
	}
	assertNeoOrbPrelaunchNoActivationOrCleanup(t, fixture)
}

func TestNeoOrbCommitActiveLifecyclePrelaunchReadyRejectsStaleLifecycle(t *testing.T) {
	tests := map[string]func(*testing.T, *neoOrbPrelaunchFixture){
		"pending": func(t *testing.T, fixture *neoOrbPrelaunchFixture) {
			if _, err := fixture.store.reserveGeneration(fixture.record.threadID, "pending-token"); err != nil {
				t.Fatalf("reserve pending generation: %v", err)
			}
		},
		"tombstone": func(t *testing.T, fixture *neoOrbPrelaunchFixture) {
			if _, found, err := fixture.store.beginCleanup(fixture.record.threadID); err != nil || !found {
				t.Fatalf("begin cleanup: found=%v err=%v", found, err)
			}
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			fixture := newNeoOrbPrelaunchFixture(t)
			operation, _ := beginNeoOrbActiveLifecycleTestOperation(t, fixture.neoOrbMaterializationFixture)
			mutate(t, fixture)
			err := fixture.manager.commitActiveLifecyclePrelaunchReady(t.Context(), fixture.actor, fixture.record, operation, fixture.ownerID)
			operation.close()
			if !errors.Is(err, errNeoOrbActiveLifecycleGuardRejected) {
				t.Fatalf("stale readiness commit error = %v", err)
			}
			record, exists := fixture.manager.snapshot(fixture.record.threadID)
			if !exists || record.prelaunchReady || record.preparedOwnerID != "" || record.state != neoOrbStateProvisioning || record.containerID == "" {
				t.Fatalf("stale lifecycle committed readiness: %#v", record)
			}
			if !fixture.record.operationMu.TryLock() {
				t.Fatal("rejected readiness commit retained operation lock")
			}
			fixture.record.operationMu.Unlock()
		})
	}
}

func TestNeoOrbReconcileActiveLifecyclePrelaunchSuccessOrderAndQuarantine(t *testing.T) {
	fixture := newNeoOrbPrelaunchFixture(t)
	fixture.putOwnerConfig(t)
	fixture.cfg.AmpCode.Orbs.Env["GH_TOKEN"] = "global-fallback-secret"
	beforeStore := fixture.store.snapshot()
	assertProviderLocks := func() {
		if fixture.record.operationMu.TryLock() {
			fixture.record.operationMu.Unlock()
			t.Error("prelaunch operation mutex was not held")
		}
		if !fixture.manager.mu.TryLock() {
			t.Error("manager mutex held across prelaunch provider call")
		} else {
			fixture.manager.mu.Unlock()
		}
		if !fixture.runtime.orbManagerMu.TryLock() {
			t.Error("runtime orb manager mutex held across prelaunch provider call")
		} else {
			fixture.runtime.orbManagerMu.Unlock()
		}
	}
	fixture.fake.containerTargetHook = func(string) { assertProviderLocks() }
	fixture.fake.inspectContainerHook = func(context.Context, string) { assertProviderLocks() }
	fixture.fake.inspectVolumeHook = func(context.Context, string) { assertProviderLocks() }
	if err := fixture.manager.reconcileActiveLifecyclePrelaunch(t.Context(), fixture.actor, fixture.record, fixture.fake, fixture.cfg, ""); err != nil {
		t.Fatalf("prelaunch: %v", err)
	}
	afterStore := fixture.store.snapshot()
	beforeLifecycle := beforeStore.Threads[fixture.record.threadID]
	afterLifecycle := afterStore.Threads[fixture.record.threadID]
	if beforeLifecycle.Active == nil || afterLifecycle.Active == nil || afterLifecycle.Pending != nil {
		t.Fatalf("successful prelaunch lifecycle before=%#v after=%#v", beforeLifecycle, afterLifecycle)
	}
	wantActive := *beforeLifecycle.Active
	wantActive.ActivationState = neoOrbLifecycleActivationLaunching
	wantActive.OperationID = afterLifecycle.Active.OperationID
	wantActive.OperationKind = neoOrbLifecycleOperationStart
	if afterLifecycle.Active.OperationID == "" || *afterLifecycle.Active != wantActive || neoOrbLifecycleActionable(afterLifecycle.Active) {
		t.Fatalf("successful prelaunch activation = %#v, want %#v", afterLifecycle.Active, wantActive)
	}
	record, exists := fixture.manager.snapshot(fixture.record.threadID)
	if !exists || record.state != neoOrbStateProvisioning || record.recovered || !record.lifecycleV1 || record.containerID != afterLifecycle.Active.ContainerID || !record.prelaunchReady || record.preparedOwnerID != fixture.ownerID || record.pendingGeneration != nil || record.activeGeneration == nil || *record.activeGeneration != *afterLifecycle.Active || record.failReason != "" {
		t.Fatalf("successful prelaunch record = %#v", record)
	}
	if !fixture.record.operationMu.TryLock() {
		t.Fatal("successful prelaunch retained operation lease")
	}
	fixture.record.operationMu.Unlock()
	stagePrefixes := []string{
		"start:" + afterLifecycle.Active.ContainerID,
		"exec:dpkg --print-architecture",
		"copy:" + neoOrbAgentBrowserPath,
		"copy:" + neoOrbBinaryPath,
		"copy:" + neoOrbPortalHelperPath,
		"copy:/root/.config/gh/hosts.yml",
		"exec:cat " + neoOrbOwnerConfigDigestPath,
		"exec:mkdir -p " + neoOrbWorkDir,
	}
	previous := -1
	for _, prefix := range stagePrefixes {
		index := fixture.fake.callIndex(prefix)
		if index <= previous {
			t.Fatalf("prelaunch stage %q index=%d after=%d calls=%#v", prefix, index, previous, fixture.fake.calls)
		}
		previous = index
	}
	assertNeoOrbPrelaunchNoActivationOrCleanup(t, fixture)
	joined := strings.Join(fixture.fake.calls, "\n")
	if strings.Contains(joined, "global-fallback-secret") {
		t.Fatal("prelaunch provider calls retained global credential")
	}
	if fixture.fake.hasSensitiveCommandArg() {
		t.Fatal("prelaunch passed sensitive provider command args")
	}
}

func TestNeoOrbReconcileActiveLifecyclePrelaunchCredentialSelection(t *testing.T) {
	tests := map[string]struct {
		found              bool
		ownerGitHub        string
		wantOwnerGitHub    bool
		wantGlobalFallback bool
	}{
		"owner found with GitHub":    {found: true, ownerGitHub: "owner-github-secret", wantOwnerGitHub: true},
		"owner found without GitHub": {found: true},
		"owner absent":               {wantGlobalFallback: true},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			fixture := newNeoOrbPrelaunchFixture(t)
			fixture.putOwnerConfig(t)
			fixture.cfg.AmpCode.Orbs.Env["GH_TOKEN"] = "global-github-secret"
			if test.found {
				fixture.putOwnerCredentials(t, test.ownerGitHub)
			}
			if err := fixture.manager.reconcileActiveLifecyclePrelaunch(t.Context(), fixture.actor, fixture.record, fixture.fake, fixture.cfg, ""); err != nil {
				t.Fatalf("prelaunch credentials: %v", err)
			}
			ownerCopies := fixture.fake.callCount("copy:" + neoOrbOwnerGHStagePath + "/hosts.yml")
			globalCopies := fixture.fake.callCount("copy:/root/.config/gh/hosts.yml")
			if (ownerCopies == 1) != test.wantOwnerGitHub || (globalCopies == 1) != test.wantGlobalFallback {
				t.Fatalf("credential copies owner=%d global=%d calls=%#v", ownerCopies, globalCopies, fixture.fake.calls)
			}
			joined := strings.Join(fixture.fake.calls, "\n")
			if strings.Contains(joined, "owner-github-secret") || strings.Contains(joined, "global-github-secret") {
				t.Fatal("credential body leaked into provider calls")
			}
			if fixture.fake.hasSensitiveCommandArg() {
				t.Fatal("credential selection passed sensitive provider command args")
			}
			assertNeoOrbPrelaunchNoActivationOrCleanup(t, fixture)
		})
	}
}

func TestNeoOrbReconcileActiveLifecyclePrelaunchLocalConfigFallbackOnlyWhenOwnerMissing(t *testing.T) {
	for _, ownerConfig := range []bool{false, true} {
		t.Run(fmt.Sprintf("owner config %t", ownerConfig), func(t *testing.T) {
			fixture := newNeoOrbPrelaunchFixture(t)
			syncLocal := true
			fixture.cfg.AmpCode.Orbs.SyncLocalConfig = &syncLocal
			home := t.TempDir()
			t.Setenv("HOME", home)
			if err := os.MkdirAll(filepath.Join(home, ".config", "amp"), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(home, ".config", "amp", "settings.json"), []byte(`{"amp.showCosts":true}`), 0o600); err != nil {
				t.Fatal(err)
			}
			if ownerConfig {
				fixture.putOwnerConfig(t)
			}
			if err := fixture.manager.reconcileActiveLifecyclePrelaunch(t.Context(), fixture.actor, fixture.record, fixture.fake, fixture.cfg, ""); err != nil {
				t.Fatal(err)
			}
			fallbackCopies := fixture.fake.callCount("copy:/root/.config/amp/settings.json")
			if (fallbackCopies == 1) == ownerConfig {
				t.Fatalf("ownerConfig=%v local fallback copies=%d calls=%#v", ownerConfig, fallbackCopies, fixture.fake.calls)
			}
		})
	}
}

func TestNeoOrbReconcileActiveLifecyclePrelaunchFailuresStayInert(t *testing.T) {
	t.Run("ambiguous start error", func(t *testing.T) {
		fixture := newNeoOrbPrelaunchFixture(t)
		before := fixture.store.snapshot()
		fixture.fake.startHandler = func(string) error { return errors.New("start result unknown") }
		err := fixture.manager.reconcileActiveLifecyclePrelaunch(t.Context(), fixture.actor, fixture.record, fixture.fake, fixture.cfg, "")
		if err == nil || !strings.Contains(err.Error(), "at start") || fixture.fake.callCount("start:") != 1 || fixture.fake.callCount("exec:") != 0 {
			t.Fatalf("ambiguous start error=%v calls=%#v", err, fixture.fake.calls)
		}
		assertNeoOrbPrelaunchInvokedFailureState(t, fixture, before)
	})

	t.Run("cancellation after start begins", func(t *testing.T) {
		fixture := newNeoOrbPrelaunchFixture(t)
		before := fixture.store.snapshot()
		ctx, cancel := context.WithCancel(context.Background())
		fixture.fake.startHandler = func(string) error {
			cancel()
			return nil
		}
		err := fixture.manager.reconcileActiveLifecyclePrelaunch(ctx, fixture.actor, fixture.record, fixture.fake, fixture.cfg, "")
		if !errors.Is(err, context.Canceled) || fixture.fake.callCount("start:") != 1 || fixture.fake.callCount("exec:") != 0 {
			t.Fatalf("prelaunch cancellation error=%v calls=%#v", err, fixture.fake.calls)
		}
		assertNeoOrbPrelaunchInvokedFailureState(t, fixture, before)
	})

	t.Run("credential failure blocks config and workspace", func(t *testing.T) {
		fixture := newNeoOrbPrelaunchFixture(t)
		fixture.putOwnerCredentials(t, "owner-secret")
		fixture.putOwnerConfig(t)
		before := fixture.store.snapshot()
		fixture.fake.execResultHandler = func(cmd []string) (neoOrbExecResult, error) {
			joined := strings.Join(cmd, " ")
			if joined == "dpkg --print-architecture" {
				return neoOrbExecResult{Stdout: "amd64\n"}, nil
			}
			if strings.Contains(joined, "mkdir -m 0700 "+neoOrbOwnerGHStagePath) {
				return neoOrbExecResult{ExitCode: 1}, nil
			}
			return neoOrbExecResult{}, nil
		}
		err := fixture.manager.reconcileActiveLifecyclePrelaunch(t.Context(), fixture.actor, fixture.record, fixture.fake, fixture.cfg, "")
		if err == nil {
			t.Fatalf("credential failure error = %v", err)
		}
		if fixture.fake.callCount("exec:cat "+neoOrbOwnerConfigDigestPath) != 0 || fixture.fake.callCount("exec:mkdir -p "+neoOrbWorkDir) != 0 {
			t.Fatalf("credential failure reached config/workspace: %#v", fixture.fake.calls)
		}
		assertNeoOrbPrelaunchInvokedFailureState(t, fixture, before)
	})

	t.Run("owner changes before credentials", func(t *testing.T) {
		fixture := newNeoOrbPrelaunchFixture(t)
		before := fixture.store.snapshot()
		fixture.fake.containerTargetHook = func(call string) {
			if strings.HasPrefix(call, "copy:"+neoOrbServiceHelperPath) {
				fixture.actor.mu.Lock()
				fixture.actor.meta["ownerUserId"] = "owner-replacement"
				fixture.actor.mu.Unlock()
			}
		}
		err := fixture.manager.reconcileActiveLifecyclePrelaunch(t.Context(), fixture.actor, fixture.record, fixture.fake, fixture.cfg, "")
		if !errors.Is(err, errNeoOrbActiveLifecycleGuardRejected) {
			t.Fatalf("owner change error = %v", err)
		}
		if fixture.fake.callCount("exec:cat "+neoOrbOwnerCredentialRevisionPath) != 0 || fixture.fake.callCount("exec:mkdir -p "+neoOrbWorkDir) != 0 {
			t.Fatalf("owner change reached credentials/workspace: %#v", fixture.fake.calls)
		}
		assertNeoOrbPrelaunchInvokedFailureState(t, fixture, before)
	})

	t.Run("owner changes before config", func(t *testing.T) {
		fixture := newNeoOrbPrelaunchFixture(t)
		fixture.putOwnerCredentials(t, "")
		fixture.putOwnerConfig(t)
		before := fixture.store.snapshot()
		fixture.fake.containerTargetHook = func(call string) {
			if strings.HasPrefix(call, "copy:"+neoOrbOwnerCredentialRevisionPath) {
				fixture.actor.mu.Lock()
				fixture.actor.meta["ownerUserId"] = "owner-replacement"
				fixture.actor.mu.Unlock()
			}
		}
		err := fixture.manager.reconcileActiveLifecyclePrelaunch(t.Context(), fixture.actor, fixture.record, fixture.fake, fixture.cfg, "")
		if !errors.Is(err, errNeoOrbActiveLifecycleGuardRejected) {
			t.Fatalf("owner change error = %v", err)
		}
		if fixture.fake.callCount("exec:cat "+neoOrbOwnerConfigDigestPath) != 0 || fixture.fake.callCount("exec:mkdir -p "+neoOrbWorkDir) != 0 {
			t.Fatalf("owner change reached config/workspace: %#v", fixture.fake.calls)
		}
		assertNeoOrbPrelaunchInvokedFailureState(t, fixture, before)
	})

	t.Run("owner changes during config", func(t *testing.T) {
		fixture := newNeoOrbPrelaunchFixture(t)
		fixture.putOwnerConfig(t)
		before := fixture.store.snapshot()
		fixture.fake.containerTargetHook = func(call string) {
			if strings.HasPrefix(call, "copy:"+neoOrbOwnerConfigDigestPath) {
				fixture.actor.mu.Lock()
				fixture.actor.meta["ownerUserId"] = "owner-replacement"
				fixture.actor.mu.Unlock()
			}
		}
		err := fixture.manager.reconcileActiveLifecyclePrelaunch(t.Context(), fixture.actor, fixture.record, fixture.fake, fixture.cfg, "")
		if !errors.Is(err, errNeoOrbActiveLifecycleGuardRejected) {
			t.Fatalf("owner change error = %v", err)
		}
		if fixture.fake.callCount("exec:mkdir -p "+neoOrbWorkDir) != 0 {
			t.Fatalf("owner change reached workspace: %#v", fixture.fake.calls)
		}
		assertNeoOrbPrelaunchInvokedFailureState(t, fixture, before)
	})

	t.Run("owner changes during workspace", func(t *testing.T) {
		fixture := newNeoOrbPrelaunchFixture(t)
		fixture.putOwnerConfig(t)
		before := fixture.store.snapshot()
		fixture.fake.containerTargetHook = func(call string) {
			if strings.HasPrefix(call, "exec:mkdir -p "+neoOrbWorkDir) {
				fixture.actor.mu.Lock()
				fixture.actor.meta["ownerUserId"] = "owner-replacement"
				fixture.actor.mu.Unlock()
			}
		}
		err := fixture.manager.reconcileActiveLifecyclePrelaunch(t.Context(), fixture.actor, fixture.record, fixture.fake, fixture.cfg, "")
		if !errors.Is(err, errNeoOrbActiveLifecycleGuardRejected) {
			t.Fatalf("owner change error = %v", err)
		}
		assertNeoOrbPrelaunchInvokedFailureState(t, fixture, before)
	})

	t.Run("stale actor", func(t *testing.T) {
		fixture := newNeoOrbPrelaunchFixture(t)
		before := fixture.store.snapshot()
		replacement := newNeoActor(fixture.runtime, "replacement", "thread-actor", fixture.record.threadID, fixture.record.threadID, neoActorRecord("replacement", "thread-actor", fixture.record.threadID), nil)
		fixture.runtime.store.mu.Lock()
		fixture.runtime.store.actors[replacement.id] = replacement
		fixture.runtime.store.byNameKey["thread-actor\x00"+fixture.record.threadID] = replacement.id
		fixture.runtime.store.mu.Unlock()
		err := fixture.manager.reconcileActiveLifecyclePrelaunch(t.Context(), fixture.actor, fixture.record, fixture.fake, fixture.cfg, "")
		if !errors.Is(err, errNeoOrbActiveLifecycleGuardRejected) || len(fixture.fake.calls) != 0 {
			t.Fatalf("stale actor error=%v calls=%#v", err, fixture.fake.calls)
		}
		assertNeoOrbPrelaunchFailureState(t, fixture, before)
	})

	t.Run("foreign actor", func(t *testing.T) {
		fixture := newNeoOrbPrelaunchFixture(t)
		before := fixture.store.snapshot()
		foreign := newNeoActor(fixture.runtime, "foreign", "thread-actor", fixture.record.threadID, fixture.record.threadID, neoActorRecord("foreign", "thread-actor", fixture.record.threadID), nil)
		err := fixture.manager.reconcileActiveLifecyclePrelaunch(t.Context(), foreign, fixture.record, fixture.fake, fixture.cfg, "")
		if !errors.Is(err, errNeoOrbActiveLifecycleGuardRejected) || len(fixture.fake.calls) != 0 {
			t.Fatalf("foreign actor error=%v calls=%#v", err, fixture.fake.calls)
		}
		assertNeoOrbPrelaunchFailureState(t, fixture, before)
	})

	t.Run("recovered conflict", func(t *testing.T) {
		fixture := newNeoOrbPrelaunchFixture(t)
		fixture.manager.mu.Lock()
		fixture.record.state = neoOrbStateConflict
		fixture.record.recovered = true
		fixture.record.containerID = ""
		beforeRecord := *fixture.record
		beforeRecord.activeGeneration = cloneNeoOrbLifecycleGeneration(fixture.record.activeGeneration)
		fixture.manager.mu.Unlock()
		err := fixture.manager.reconcileActiveLifecyclePrelaunch(t.Context(), fixture.actor, fixture.record, fixture.fake, fixture.cfg, "")
		if err == nil || len(fixture.fake.calls) != 0 {
			t.Fatalf("recovered conflict error=%v calls=%#v", err, fixture.fake.calls)
		}
		after, _ := fixture.manager.snapshot(fixture.record.threadID)
		if !reflect.DeepEqual(after, beforeRecord) {
			t.Fatalf("recovered conflict was changed: %#v", after)
		}
	})
}

type neoOrbLifecycleAdmissionFixture struct {
	*neoOrbMaterializationFixture
	actor *neoActor
}

func newNeoOrbLifecycleAdmissionFixture(t *testing.T, activation string) *neoOrbLifecycleAdmissionFixture {
	t.Helper()
	var fixture *neoOrbMaterializationFixture
	var actor *neoActor
	if activation == "missing" {
		fixture = newNeoOrbActiveLifecycleFixture(t)
		actor = neoOrbTestActor(fixture.runtime, fixture.record.threadID)
	} else {
		prelaunch := newNeoOrbPrelaunchFixture(t, neoLocalOwnerUserID)
		fixture = prelaunch.neoOrbMaterializationFixture
		actor = prelaunch.actor
	}
	t.Cleanup(actor.cancel)
	active := fixture.store.snapshot().Threads[fixture.record.threadID].Active
	if active == nil {
		t.Fatal("admission fixture active generation is unavailable")
	}
	if activation == neoOrbLifecycleActivationLaunching || activation == neoOrbLifecycleActivationActionable {
		launching, err := fixture.store.beginActivation(*active, "admission-start", neoOrbLifecycleOperationStart)
		if err != nil {
			t.Fatalf("begin admission start activation: %v", err)
		}
		active = &launching
	}
	if activation == neoOrbLifecycleActivationActionable {
		launching, err := fixture.store.beginActivation(*active, "admission-exec", neoOrbLifecycleOperationExecDetached)
		if err != nil {
			t.Fatalf("begin admission executor activation: %v", err)
		}
		actionable, err := fixture.store.finishActivation(launching)
		if err != nil {
			t.Fatalf("finish admission activation: %v", err)
		}
		active = &actionable
	}
	fixture.manager.mu.Lock()
	fixture.record.state = neoOrbStateRunning
	fixture.record.containerID = active.ContainerID
	fixture.record.portalToken = active.PortalToken
	fixture.record.activeGeneration = cloneNeoOrbLifecycleGeneration(active)
	fixture.record.pendingGeneration = nil
	fixture.record.lifecycleV1 = true
	fixture.record.recovered = false
	fixture.record.prelaunchReady = activation != "missing"
	fixture.record.preparedOwnerID = active.AuthenticatedOwnerID
	fixture.record.failReason = ""
	fixture.manager.mu.Unlock()
	actor.bootstrapExecutorType = "sandbox"
	fixture.fake.mu.Lock()
	fixture.fake.calls = nil
	fixture.fake.inspectState.Running = true
	fixture.fake.inspectState.Paused = false
	fixture.fake.inspectState.IPAddress = "172.17.0.23"
	fixture.fake.mu.Unlock()
	return &neoOrbLifecycleAdmissionFixture{neoOrbMaterializationFixture: fixture, actor: actor}
}

func assertNeoOrbNoProviderMutation(t *testing.T, fake *neoOrbFakeProvider) {
	t.Helper()
	for _, forbidden := range []string{"start:", "stop:", "pause:", "unpause:", "create:", "create-volume:", "remove:", "remove-volume:", "exec:", "exec-detached:", "copy:", "copy-tar:", "copy-archive:"} {
		if fake.callCount(forbidden) != 0 {
			t.Fatalf("provider mutation %q was invoked: %#v", forbidden, fake.calls)
		}
	}
}

func TestNeoOrbExecutorAdmissionWaitStopsWithManager(t *testing.T) {
	fixture := newNeoOrbLifecycleAdmissionFixture(t, neoOrbLifecycleActivationActionable)
	fixture.record.operationMu.Lock()
	defer fixture.record.operationMu.Unlock()
	type admissionResult struct {
		release  func()
		required bool
		err      error
	}
	result := make(chan admissionResult, 1)
	go func() {
		release, required, err := fixture.manager.orbExecutorAdmission(fixture.actor, fixture.record.threadID)
		result <- admissionResult{release: release, required: required, err: err}
	}()
	select {
	case admission := <-result:
		if admission.release != nil {
			admission.release()
		}
		t.Fatalf("admission wait returned before manager stop: required=%v err=%v", admission.required, admission.err)
	case <-time.After(2 * neoOrbOperationLockInterval):
	}
	stopCtx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := fixture.manager.stop(stopCtx); err != nil {
		t.Fatalf("stop manager during admission wait: %v", err)
	}
	select {
	case admission := <-result:
		if admission.release != nil {
			admission.release()
		}
		if !admission.required || admission.err != nil {
			t.Fatalf("canceled admission result: required=%v err=%v", admission.required, admission.err)
		}
	case <-time.After(time.Second):
		t.Fatal("manager stop did not cancel admission wait")
	}
}

func TestNeoOrbExecutorAdmissionRequiresActionableLifecycleAtBothConnectionPaths(t *testing.T) {
	for _, connection := range []string{"direct", "socket"} {
		for _, activation := range []string{neoOrbLifecycleActivationBinding, neoOrbLifecycleActivationLaunching, neoOrbLifecycleActivationActionable} {
			t.Run(connection+"/"+activation, func(t *testing.T) {
				fixture := newNeoOrbLifecycleAdmissionFixture(t, activation)
				var messages []map[string]any
				var socket *neoSocket
				if connection == "socket" {
					socket = &neoSocket{writeMessage: func(_ int, data []byte) error {
						var message map[string]any
						if err := json.Unmarshal(data, &message); err != nil {
							return err
						}
						messages = append(messages, message)
						return nil
					}}
				}
				message := map[string]any{"executorId": "orb-admission-executor", "executorType": "sandbox"}
				if connection == "direct" {
					fixture.actor.executorConnected(message)
				} else {
					fixture.actor.executorConnectedForSocket(socket, message)
				}
				fixture.actor.mu.Lock()
				executorID := fixture.actor.executorID
				ready := fixture.actor.executorReady
				fixture.actor.mu.Unlock()
				if activation == neoOrbLifecycleActivationActionable {
					if executorID != "orb-admission-executor" || !ready || socket != nil && !socket.isExecutor() {
						t.Fatalf("actionable executor admission id=%q ready=%v socketExecutor=%v", executorID, ready, socket != nil && socket.isExecutor())
					}
				} else {
					if executorID != "" || ready || socket != nil && socket.isExecutor() {
						t.Fatalf("inert executor was admitted id=%q ready=%v socketExecutor=%v", executorID, ready, socket != nil && socket.isExecutor())
					}
					if socket != nil && (len(messages) != 1 || stringValue(messages[0]["code"]) != "EXECUTOR_MIGRATION_REQUIRED") {
						t.Fatalf("inert executor rejection messages = %#v", messages)
					}
				}
				assertNeoOrbNoProviderMutation(t, fixture.fake)
			})
		}
	}
}
