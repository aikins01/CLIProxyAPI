package amp

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

// neoOrbProviderClient abstracts the container backend an orb runs on. The
// Docker implementation is neoOrbDockerClient; tests substitute a fake.
type neoOrbProviderClient interface {
	Ping(ctx context.Context) error
	EnsureImage(ctx context.Context, image string) error
	CreateContainer(ctx context.Context, spec neoOrbContainerSpec) (string, error)
	ListOrbContainers(ctx context.Context) ([]neoOrbContainerSummary, error)
	StartContainer(ctx context.Context, id string) error
	StopContainer(ctx context.Context, id string) error
	PauseContainer(ctx context.Context, id string) error
	UnpauseContainer(ctx context.Context, id string) error
	RemoveContainer(ctx context.Context, id string, force bool) error
	InspectContainer(ctx context.Context, id string) (neoOrbContainerState, error)
	CreateVolume(ctx context.Context, spec neoOrbVolumeSpec) (neoOrbVolumeState, error)
	InspectVolume(ctx context.Context, name string) (neoOrbVolumeState, error)
	ListOrbVolumes(ctx context.Context) ([]neoOrbVolumeState, error)
	RemoveVolume(ctx context.Context, name string) error
	Exec(ctx context.Context, id string, cmd []string, env []string, workDir string) (neoOrbExecResult, error)
	ReadWorkspaceFile(ctx context.Context, id, workDir, relative string, maxBytes int) ([]byte, error)
	ExecDetached(ctx context.Context, id string, cmd []string, env []string, workDir string) error
	ArchiveFromContainer(ctx context.Context, id, sourcePath string) (io.ReadCloser, error)
	CopyArchiveToContainer(ctx context.Context, id, destinationPath string, archive io.Reader) error
	CopyFileToContainer(ctx context.Context, id, destPath string, content []byte, mode int64) error
	CopyTarToContainer(ctx context.Context, id, destDir string, files map[string][]byte, mode int64) error
}

const (
	neoOrbStateProvisioning = "provisioning"
	neoOrbStateRunning      = "running"
	neoOrbStatePaused       = "paused"
	neoOrbStateFailed       = "failed"
	neoOrbStateConflict     = "conflict"

	neoOrbWorkspaceDir             = "/home/user/workspace"
	neoOrbWorkDir                  = neoOrbWorkspaceDir + "/repo"
	neoOrbBinaryPath               = "/usr/local/bin/amp"
	neoOrbExecutorLog              = "/home/user/.cache/amp/logs/headless.log"
	neoOrbExecutorLock             = "/run/cliproxy-amp-executor.lock"
	neoOrbAgentBrowserPath         = "/usr/local/bin/agent-browser"
	neoOrbAgentBrowserSocketDir    = "/run/cliproxy-agent-browser"
	neoOrbContainerIDMetaKey       = "cliproxyOrbContainerID"
	neoOrbPortalTokenMetaKey       = "cliproxyOrbPortalToken"
	neoOrbReapInterval             = 30 * time.Second
	neoOrbOperationLockInterval    = 10 * time.Millisecond
	neoOrbLifecycleRevisionMetaKey = "cliproxyOrbLifecycleRevision"

	neoOrbNodeVersion         = "24.19.0"
	neoOrbPNPMVersion         = "11.20.0"
	neoOrbBunVersion          = "1.3.14"
	neoOrbAgentBrowserVersion = "0.33.2"
	neoOrbChromeVersion       = "151.0.7922.77"

	// neoOrbResumeHookSeconds bounds the .agents/resume hook on orb resume. The
	// hook only rewarms caches and services, so a stuck hook must not delay the
	// executor reconnect.
	neoOrbResumeHookSeconds = 10

	neoOrbSyncMaxBytes = 32 << 20
)

type neoOrbRecord struct {
	threadID                string
	runnerID                string
	spawnID                 string
	webLocal                bool
	multiplayerTTLSeconds   int
	runnerMigrationRequired bool
	runnerMigrationFencing  bool
	containerID             string
	state                   string
	workDir                 string
	repositoryURL           string
	portalToken             string
	activePortals           int
	idleSince               time.Time
	failReason              string
	portalIP                string
	portalIPAt              time.Time
	recovered               bool
	lifecycleV1             bool
	activeGeneration        *neoOrbGenerationRecord
	pendingGeneration       *neoOrbGenerationRecord
	prelaunchReady          bool
	preparedOwnerID         string
	suppressSharedGitHub    bool
	operationMu             *sync.Mutex
}

func neoOrbRunnerID(threadID, portalToken string) string {
	threadID = strings.TrimSpace(threadID)
	portalToken = strings.TrimSpace(portalToken)
	if !neoThreadIDExactPattern.MatchString(threadID) || !neoOrbPortalTokenValid(portalToken) {
		return ""
	}
	payload := strings.Join([]string{"cliproxy-orb-runner-v1", threadID, portalToken}, "\x00")
	digest := sha256.Sum256([]byte(payload))
	return fmt.Sprintf("orb-%x", digest[:16])
}

type neoOrbManager struct {
	runtime *neoRuntime

	mu           sync.Mutex
	orbs         map[string]*neoOrbRecord
	client       neoOrbProviderClient
	clientKey    string
	reaping      bool
	recovered    bool
	recoveredKey string
	recoverMu    sync.Mutex
	workerCtx    context.Context
	cancelWorker context.CancelFunc
	workers      sync.WaitGroup
	workerCount  int
	stopOnce     sync.Once
	workersDone  chan struct{}
	stopped      bool

	newClient func(host string) (neoOrbProviderClient, error)
	now       func() time.Time
}

func newNeoOrbManager(rt *neoRuntime) *neoOrbManager {
	workerCtx, cancelWorker := context.WithCancel(context.Background())
	return &neoOrbManager{
		runtime:      rt,
		orbs:         map[string]*neoOrbRecord{},
		workerCtx:    workerCtx,
		cancelWorker: cancelWorker,
		workersDone:  make(chan struct{}),
		newClient: func(host string) (neoOrbProviderClient, error) {
			return newNeoOrbDockerClient(host)
		},
		now: time.Now,
	}
}

func (m *neoOrbManager) startWorker(run func(context.Context)) bool {
	if m == nil || run == nil {
		return false
	}
	ctx, done, started := m.beginWorker()
	if !started {
		return false
	}
	go func() {
		defer done()
		run(ctx)
	}()
	return true
}

func (m *neoOrbManager) beginWorker() (context.Context, func(), bool) {
	if m == nil {
		return nil, nil, false
	}
	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		return nil, nil, false
	}
	ctx := m.workerCtx
	m.workers.Add(1)
	m.workerCount++
	m.mu.Unlock()
	done := func() {
		m.workers.Done()
		m.mu.Lock()
		m.workerCount--
		m.mu.Unlock()
	}
	return ctx, done, true
}

func (m *neoOrbManager) stop(ctx context.Context) error {
	if m == nil {
		return nil
	}
	m.stopOnce.Do(func() {
		m.mu.Lock()
		m.stopped = true
		m.cancelWorker()
		workerCount := m.workerCount
		if workerCount == 0 {
			close(m.workersDone)
		}
		m.mu.Unlock()
		if workerCount != 0 {
			go func() {
				m.workers.Wait()
				close(m.workersDone)
			}()
		}
	})
	select {
	case <-m.workersDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func neoOrbsConfig(cfg *config.Config) config.AmpOrbs {
	if cfg == nil {
		return config.AmpOrbs{}
	}
	return cfg.AmpCode.Orbs
}

func neoOrbsEnabled(cfg *config.Config) bool {
	return neoOrbsAvailable(cfg) == ""
}

func neoOrbsAvailable(cfg *config.Config) string {
	orbs := neoOrbsConfig(cfg)
	if orbs.Enabled == nil || !*orbs.Enabled {
		return "orb executors are not enabled on this server"
	}
	provider := strings.ToLower(strings.TrimSpace(orbs.Provider))
	if provider != "" && provider != "docker" {
		return fmt.Sprintf("orb provider %q is not supported (only \"docker\")", orbs.Provider)
	}
	return ""
}

func neoOrbImage(cfg *config.Config) string {
	if image := strings.TrimSpace(neoOrbsConfig(cfg).Image); image != "" {
		return image
	}
	return "debian:12-slim"
}

func neoOrbAutoPause(cfg *config.Config) time.Duration {
	seconds := neoOrbsConfig(cfg).AutoPauseSeconds
	if seconds <= 0 {
		seconds = 300
	}
	return time.Duration(seconds) * time.Second
}

func neoOrbSetupTimeout(cfg *config.Config) time.Duration {
	seconds := neoOrbsConfig(cfg).SetupTimeoutSeconds
	if seconds <= 0 {
		seconds = 600
	}
	return time.Duration(seconds) * time.Second
}

func neoOrbSyncLocalConfigEnabled(cfg *config.Config) bool {
	sync := neoOrbsConfig(cfg).SyncLocalConfig
	return sync == nil || *sync
}

func neoOrbGitHubToken(cfg *config.Config) string {
	env := neoOrbsConfig(cfg).Env
	return firstNonEmptyString(env["GH_TOKEN"], env["GITHUB_TOKEN"])
}

func (m *neoOrbManager) dockerClient(cfg *config.Config) (neoOrbProviderClient, error) {
	host, err := resolveNeoOrbDockerEndpoint(neoOrbsConfig(cfg).DockerHost)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped {
		return nil, errors.New("orb manager is stopped")
	}
	if m.client != nil && m.clientKey == host {
		return m.client, nil
	}
	client, err := m.newClient(host)
	if err != nil {
		return nil, err
	}
	m.client = client
	m.clientKey = host
	return client, nil
}

// live returns the raw record pointer; callers must only mutate fields while
// holding m.mu. Use snapshot for reads outside the lock.
func (m *neoOrbManager) live(threadID string) *neoOrbRecord {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.orbs[threadID]
}

func (m *neoOrbManager) snapshot(threadID string) (neoOrbRecord, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	record, ok := m.orbs[threadID]
	if !ok {
		return neoOrbRecord{}, false
	}
	return *record, true
}

func (m *neoOrbManager) hasLiveOrb(threadID string) bool {
	record, ok := m.snapshot(threadID)
	return ok && (record.state == neoOrbStateProvisioning || record.state == neoOrbStateRunning)
}

func (m *neoOrbManager) ensureRecovered(cfg *config.Config) error {
	host, err := resolveNeoOrbDockerEndpoint(neoOrbsConfig(cfg).DockerHost)
	if err != nil {
		return err
	}
	workerCtx, done, started := m.beginWorker()
	if !started {
		return errors.New("orb manager is stopped")
	}
	defer done()
	m.mu.Lock()
	if m.recovered && m.recoveredKey == host {
		m.mu.Unlock()
		return nil
	}
	m.mu.Unlock()
	if err := neoOrbLockOperation(workerCtx, &m.recoverMu); err != nil {
		return fmt.Errorf("wait for orb recovery: %w", err)
	}
	defer m.recoverMu.Unlock()
	m.mu.Lock()
	if m.recovered && m.recoveredKey == host {
		m.mu.Unlock()
		return nil
	}
	initialized := m.recovered || m.recoveredKey != "" || m.client != nil || len(m.orbs) != 0
	boundProvider := firstNonEmptyString(m.recoveredKey, m.clientKey)
	if initialized && boundProvider != "" && boundProvider != host {
		m.mu.Unlock()
		return fmt.Errorf("changing the effective orb Docker host (ampcode.orbs.docker-host or DOCKER_HOST) requires restarting CLIProxyAPI")
	}
	m.recoveredKey = host
	m.mu.Unlock()
	store, err := m.runtime.orbLifecycleStoreFor(host)
	if err != nil {
		return fmt.Errorf("open orb lifecycle store: %w", err)
	}
	if store == nil {
		return errors.New("orb lifecycle store is unavailable")
	}
	lifecycle := store.snapshot()
	client, err := m.dockerClient(cfg)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(workerCtx, 30*time.Second)
	defer cancel()
	containers, err := client.ListOrbContainers(ctx)
	if err != nil {
		return fmt.Errorf("list labelled containers: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("list labelled containers: %w", err)
	}
	volumes, err := client.ListOrbVolumes(ctx)
	if err != nil {
		return fmt.Errorf("list lifecycle volumes: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("list lifecycle volumes: %w", err)
	}
	quarantined := map[string]*neoOrbRecord{}
	quarantine := func(threadID, reason string, active, pending *neoOrbGenerationRecord) {
		if !neoThreadIDExactPattern.MatchString(threadID) {
			return
		}
		record := quarantined[threadID]
		if record == nil {
			record = &neoOrbRecord{
				threadID:    threadID,
				state:       neoOrbStateConflict,
				workDir:     neoOrbWorkDir,
				failReason:  reason,
				recovered:   true,
				lifecycleV1: true,
				operationMu: &sync.Mutex{},
			}
			quarantined[threadID] = record
		}
		if record.activeGeneration == nil && active != nil {
			generation := *active
			record.activeGeneration = &generation
			record.multiplayerTTLSeconds = generation.MultiplayerTTLSeconds
		}
		if record.pendingGeneration == nil && pending != nil {
			generation := *pending
			record.pendingGeneration = &generation
			if record.activeGeneration == nil {
				record.multiplayerTTLSeconds = generation.MultiplayerTTLSeconds
			}
		}
	}
	grouped := map[string][]neoOrbContainerSummary{}
	lifecycleContainers := map[string][]neoOrbContainerSummary{}
	for _, container := range containers {
		threadID := strings.TrimSpace(container.Labels["cliproxy.orb"])
		if !neoThreadIDExactPattern.MatchString(threadID) {
			log.Warnf("amp orbs: ignoring container=%s with invalid thread label", container.ID)
			continue
		}
		if lifecycleLabel, present := container.Labels["cliproxy.orb.lifecycle"]; present {
			if lifecycleLabel != "1" {
				quarantine(threadID, "ambiguous lifecycle container label", nil, nil)
			} else {
				lifecycleContainers[threadID] = append(lifecycleContainers[threadID], container)
			}
			continue
		}
		grouped[threadID] = append(grouped[threadID], container)
	}
	lifecycleVolumes := map[string][]neoOrbVolumeState{}
	volumesByName := map[string]neoOrbVolumeState{}
	for _, volume := range volumes {
		threadID := strings.TrimSpace(volume.Labels["cliproxy.orb"])
		if !neoThreadIDExactPattern.MatchString(threadID) {
			log.Warnf("amp orbs: ignoring volume=%s with invalid thread label", volume.Name)
			continue
		}
		lifecycleVolumes[threadID] = append(lifecycleVolumes[threadID], volume)
		volumesByName[volume.Name] = volume
	}
	for threadID, tombstone := range lifecycle.CleanupTombstones {
		quarantine(threadID, "lifecycle-v1 cleanup is pending", tombstone.Active, tombstone.Pending)
	}
	for threadID, lifecycleRecord := range lifecycle.Threads {
		active := lifecycleRecord.Active
		if lifecycleRecord.Pending != nil || active == nil {
			quarantine(threadID, "lifecycle-v1 generation is not actionable", active, lifecycleRecord.Pending)
			continue
		}
		if !neoOrbLifecycleActionable(active) {
			reason := "lifecycle-v1 activation is unavailable"
			if active.ActivationState == neoOrbLifecycleActivationBinding {
				reason = "lifecycle-v1 activation is binding"
			} else if active.ActivationState == neoOrbLifecycleActivationLaunching {
				reason = "lifecycle-v1 activation is launching"
			}
			quarantine(threadID, reason, active, nil)
			continue
		}
		thread, found := loadNeoThreadFromDir(threadID, m.runtime.threadDir)
		ownerID, containerID, portalToken, revision, bindingExact := neoOrbPersistedLifecycleBinding(thread)
		executorType := firstNonEmptyString(thread["executorType"], nestedValue(thread["meta"], "executorType"), nestedValue(thread["threadMeta"], "executorType"))
		if !found || !strings.EqualFold(executorType, "sandbox") || !bindingExact || ownerID != active.AuthenticatedOwnerID || containerID != active.ContainerID || portalToken != active.PortalToken || revision != active.Revision {
			quarantine(threadID, "lifecycle-v1 authenticated binding does not match", active, nil)
			continue
		}
		matches := lifecycleContainers[threadID]
		if len(matches) != 1 || matches[0].ID != active.ContainerID {
			quarantine(threadID, "lifecycle-v1 container does not match", active, nil)
			continue
		}
		homeVolume, homeFound := volumesByName[active.HomeVolumeName]
		rootVolume, rootFound := volumesByName[active.RootVolumeName]
		if !homeFound || !rootFound || !neoOrbLifecycleVolumeMatches(homeVolume, lifecycle.OwnerID, *active, neoOrbLifecycleRoleHome) || !neoOrbLifecycleVolumeMatches(rootVolume, lifecycle.OwnerID, *active, neoOrbLifecycleRoleRoot) {
			quarantine(threadID, "lifecycle-v1 volumes do not match", active, nil)
			continue
		}
		containerState, inspectErr := client.InspectContainer(ctx, active.ContainerID)
		if inspectErr != nil || !neoOrbLifecycleContainerMatches(active.ContainerID, containerState, homeVolume, rootVolume, lifecycle.OwnerID, *active) {
			quarantine(threadID, "lifecycle-v1 container ownership does not match", active, nil)
			continue
		}
		state := ""
		switch {
		case containerState.Running && containerState.Paused:
			state = neoOrbStatePaused
		case containerState.Running:
			state = neoOrbStateRunning
		default:
			quarantine(threadID, "lifecycle-v1 container is not running", active, nil)
			continue
		}
		quarantined[threadID] = &neoOrbRecord{
			threadID:                threadID,
			runnerID:                neoOrbRunnerID(threadID, active.PortalToken),
			multiplayerTTLSeconds:   active.MultiplayerTTLSeconds,
			runnerMigrationRequired: true,
			containerID:             active.ContainerID,
			state:                   state,
			workDir:                 neoOrbWorkDir,
			repositoryURL:           strings.TrimSpace(firstNonEmptyString(thread["repositoryURL"], nestedValue(thread["meta"], "repositoryURL"), nestedValue(thread["project"], "repositoryURL"))),
			portalToken:             active.PortalToken,
			lifecycleV1:             true,
			activeGeneration:        cloneNeoOrbLifecycleGeneration(active),
			prelaunchReady:          true,
			preparedOwnerID:         active.AuthenticatedOwnerID,
			suppressSharedGitHub:    true,
			operationMu:             &sync.Mutex{},
		}
	}
	for threadID := range lifecycleContainers {
		if _, exists := lifecycle.Threads[threadID]; !exists {
			quarantine(threadID, "untracked lifecycle-v1 container requires reconciliation", nil, nil)
		}
	}
	for threadID := range lifecycleVolumes {
		if _, exists := lifecycle.Threads[threadID]; !exists {
			quarantine(threadID, "untracked lifecycle-v1 volume requires reconciliation", nil, nil)
		}
	}
	repositories := map[string]string{}
	recoverable := map[string]bool{}
	containerBindings := map[string]string{}
	for threadID := range grouped {
		thread, ok := loadNeoThreadFromDir(threadID, m.runtime.threadDir)
		if ok {
			executorType := firstNonEmptyString(thread["executorType"], nestedValue(thread["meta"], "executorType"), nestedValue(thread["threadMeta"], "executorType"))
			if strings.EqualFold(executorType, "sandbox") {
				recoverable[threadID] = true
				repositories[threadID] = strings.TrimSpace(firstNonEmptyString(thread["repositoryURL"], nestedValue(thread["meta"], "repositoryURL"), nestedValue(thread["project"], "repositoryURL")))
				containerBindings[threadID] = strings.TrimSpace(firstNonEmptyString(nestedValue(thread["meta"], neoOrbContainerIDMetaKey), nestedValue(thread["threadMeta"], neoOrbContainerIDMetaKey)))
			}
		}
	}
	recovered := make(map[string]*neoOrbRecord, len(grouped)+len(quarantined))
	for threadID, record := range quarantined {
		recovered[threadID] = record
	}
	for threadID, matches := range grouped {
		if _, blocked := quarantined[threadID]; blocked {
			continue
		}
		record := &neoOrbRecord{threadID: threadID, state: neoOrbStateConflict, workDir: neoOrbWorkDir, repositoryURL: repositories[threadID], recovered: true, operationMu: &sync.Mutex{}}
		if !recoverable[threadID] {
			record.failReason = "labelled container has no persisted sandbox thread"
			recovered[threadID] = record
			log.Warnf("amp orbs: thread=%s has labelled containers without persisted sandbox state; automatic provisioning is blocked", threadID)
			continue
		}
		if containerBindings[threadID] == "" {
			record.failReason = "labelled container has no persisted container binding"
			recovered[threadID] = record
			log.Warnf("amp orbs: thread=%s has labelled containers without a persisted container binding; automatic provisioning is blocked", threadID)
			continue
		}
		if len(matches) != 1 {
			ids := make([]string, 0, len(matches))
			for _, match := range matches {
				ids = append(ids, match.ID)
			}
			record.failReason = fmt.Sprintf("found %d labelled containers (%s)", len(matches), strings.Join(ids, ", "))
			recovered[threadID] = record
			log.Warnf("amp orbs: thread=%s has %d labelled containers; automatic provisioning is blocked", threadID, len(matches))
			continue
		}
		if matches[0].ID != containerBindings[threadID] {
			record.failReason = "labelled container does not match persisted container binding"
			recovered[threadID] = record
			log.Warnf("amp orbs: thread=%s labelled container does not match the persisted container binding; automatic provisioning is blocked", threadID)
			continue
		}
		record.containerID = matches[0].ID
		record.failReason = "recovered legacy container requires migration approval"
		recovered[threadID] = record
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("reconcile labelled containers: %w", err)
	}
	m.mu.Lock()
	for threadID, record := range recovered {
		if _, exists := m.orbs[threadID]; !exists {
			m.orbs[threadID] = record
		}
	}
	m.recovered = true
	m.recoveredKey = host
	m.mu.Unlock()
	return nil
}

// orbSpawnStatus returns the manager-tracked spawn state for pending-work
// gating: provisioning and running orbs count as in-flight; paused or failed
// orbs are resumable and do not block a new spawn request.
func (m *neoOrbManager) orbInFlight(threadID string) bool {
	if err := m.ensureRecovered(m.runtime.configSnapshot()); err != nil {
		return false
	}
	record, ok := m.snapshot(threadID)
	return ok && (record.state == neoOrbStateProvisioning || record.state == neoOrbStateRunning && !record.recovered || record.state == neoOrbStateConflict)
}

func (m *neoOrbManager) orbExecutorAdmission(actor *neoActor, threadID string) (func(), bool, error) {
	if err := m.ensureRecovered(m.runtime.configSnapshot()); err != nil {
		return nil, true, err
	}
	m.mu.Lock()
	record := m.orbs[threadID]
	workerCtx := m.workerCtx
	if record == nil || !record.lifecycleV1 && !record.recovered {
		m.mu.Unlock()
		return func() {}, false, nil
	}
	legacyConflict := record.recovered && !record.lifecycleV1
	m.mu.Unlock()
	if legacyConflict {
		return nil, true, nil
	}
	operation, err := m.beginLifecycleAdmission(workerCtx, actor, threadID, neoOrbStateRunning)
	if err != nil {
		return nil, true, nil
	}
	return operation.close, false, nil
}

type neoOrbExecutorSocketAdmission struct {
	record   *neoOrbRecord
	spawnID  string
	runnerID string
	webLocal bool
	release  func()
}

func (m *neoOrbManager) orbExecutorSocketAdmission(actor *neoActor, threadID, incomingRunnerID string) (*neoOrbExecutorSocketAdmission, bool, error) {
	if err := m.ensureRecovered(m.runtime.configSnapshot()); err != nil {
		return nil, true, err
	}
	incomingRunnerID = strings.TrimSpace(incomingRunnerID)
	m.mu.Lock()
	record := m.orbs[threadID]
	if record == nil {
		m.mu.Unlock()
		return nil, false, nil
	}
	expectedRunnerID := neoOrbRunnerID(record.threadID, record.portalToken)
	if expectedRunnerID == "" || incomingRunnerID != expectedRunnerID {
		fenceCandidate := expectedRunnerID != "" && record.lifecycleV1 && record.state == neoOrbStateRunning && record.runnerMigrationRequired && !record.runnerMigrationFencing
		workerCtx := m.workerCtx
		m.mu.Unlock()
		startFence := false
		if fenceCandidate {
			operation, err := m.beginLifecycleAdmission(workerCtx, actor, threadID, neoOrbStateRunning)
			if err == nil {
				m.mu.Lock()
				startFence = !m.stopped && m.orbs[threadID] == record && record.lifecycleV1 && record.state == neoOrbStateRunning && record.runnerMigrationRequired && !record.runnerMigrationFencing && neoOrbRunnerID(record.threadID, record.portalToken) == expectedRunnerID
				if startFence {
					record.runnerMigrationFencing = true
				}
				m.mu.Unlock()
				operation.close()
			}
		}
		if startFence && !m.startWorker(func(ctx context.Context) {
			m.fenceLifecycleOrbRunnerMigration(ctx, actor, record)
		}) {
			m.mu.Lock()
			if m.orbs[threadID] == record && record.runnerMigrationRequired {
				record.runnerMigrationFencing = false
			}
			m.mu.Unlock()
		}
		return nil, true, nil
	}
	if record.runnerMigrationFencing {
		m.mu.Unlock()
		return nil, true, nil
	}
	if record.recovered && !record.lifecycleV1 {
		m.mu.Unlock()
		return nil, true, nil
	}
	lifecycleV1 := record.lifecycleV1
	spawnID := record.spawnID
	webLocal := record.webLocal
	workerCtx := m.workerCtx
	operationMu := record.operationMu
	m.mu.Unlock()

	admission := &neoOrbExecutorSocketAdmission{
		record:   record,
		spawnID:  spawnID,
		runnerID: expectedRunnerID,
		webLocal: webLocal,
	}
	if lifecycleV1 {
		operation, err := m.beginLifecycleAdmission(workerCtx, actor, threadID, neoOrbStateRunning)
		if err != nil {
			return nil, true, nil
		}
		m.mu.Lock()
		exact := !m.stopped && m.orbs[threadID] == record && record.state == neoOrbStateRunning &&
			!record.runnerMigrationFencing && record.spawnID == spawnID && record.webLocal == webLocal &&
			neoOrbRunnerID(record.threadID, record.portalToken) == expectedRunnerID
		if exact {
			record.runnerID = expectedRunnerID
			record.runnerMigrationRequired = false
		}
		m.mu.Unlock()
		if !exact {
			operation.close()
			return nil, true, nil
		}
		admission.release = operation.close
		return admission, false, nil
	}
	if operationMu == nil {
		return nil, true, nil
	}
	if err := neoOrbLockOperation(workerCtx, operationMu); err != nil {
		return nil, true, nil
	}
	m.mu.Lock()
	exact := !m.stopped && m.orbs[threadID] == record && record.state == neoOrbStateRunning &&
		!record.runnerMigrationFencing && record.spawnID == spawnID && record.webLocal == webLocal &&
		neoOrbRunnerID(record.threadID, record.portalToken) == expectedRunnerID
	if exact {
		record.runnerID = expectedRunnerID
		record.runnerMigrationRequired = false
	}
	m.mu.Unlock()
	if !exact {
		operationMu.Unlock()
		return nil, true, nil
	}
	var releaseOnce sync.Once
	admission.release = func() {
		releaseOnce.Do(operationMu.Unlock)
	}
	return admission, false, nil
}

func (m *neoOrbManager) orbExecutorMigrationRequired(threadID string) (bool, error) {
	if m == nil || m.runtime == nil || m.runtime.store == nil {
		return true, errors.New("orb manager is unavailable")
	}
	actor := m.runtime.store.lookupThreadActor(threadID)
	if actor == nil {
		actor = m.runtime.store.pendingThreadActor(threadID)
	}
	release, required, err := m.orbExecutorAdmission(actor, threadID)
	if release != nil {
		release()
	}
	return required, err
}

func (m *neoOrbManager) setState(record *neoOrbRecord, state, reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if record == nil || m.orbs[record.threadID] != record {
		return
	}
	record.state = state
	record.failReason = reason
	if state == neoOrbStateRunning {
		record.idleSince = time.Time{}
		record.recovered = false
	}
}

func (m *neoOrbManager) orbLaunchIdentity(record *neoOrbRecord, spawnID string) (string, string, bool) {
	if m == nil || record == nil || strings.TrimSpace(spawnID) == "" {
		return "", "", false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	threadID := record.threadID
	runnerID := record.runnerID
	exact := m.orbs[threadID] == record && record.spawnID == spawnID &&
		runnerID != "" && neoOrbRunnerID(threadID, record.portalToken) == runnerID
	return threadID, runnerID, exact
}

func (m *neoOrbManager) orbExecutorDisconnected(actor *neoActor, threadID, runnerID string) bool {
	threadID = strings.TrimSpace(threadID)
	runnerID = strings.TrimSpace(runnerID)
	if m == nil || actor == nil || !neoThreadIDExactPattern.MatchString(threadID) || runnerID == "" || !m.activeLifecycleActorExact(actor, threadID) {
		return false
	}
	m.mu.Lock()
	record := m.orbs[threadID]
	var active *neoOrbLifecycleGeneration
	if record != nil && record.activeGeneration != nil {
		active = cloneNeoOrbLifecycleGeneration(record.activeGeneration)
	}
	exact := record != nil && record.lifecycleV1 && !record.recovered && record.state == neoOrbStateRunning &&
		!record.runnerMigrationFencing && record.runnerID == runnerID && neoOrbRunnerID(threadID, record.portalToken) == runnerID &&
		record.pendingGeneration == nil && neoOrbLifecycleActionable(active)
	m.mu.Unlock()
	if !exact || !m.activeLifecycleActorBindingExact(threadID, active) {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	record = m.orbs[threadID]
	if record == nil || !record.lifecycleV1 || record.recovered || record.state != neoOrbStateRunning || record.runnerMigrationFencing ||
		record.runnerID != runnerID || neoOrbRunnerID(threadID, record.portalToken) != runnerID || record.pendingGeneration != nil ||
		record.activeGeneration == nil || neoOrbLifecycleGenerationDigest(record.activeGeneration) != neoOrbLifecycleGenerationDigest(active) {
		return false
	}
	record.state = neoOrbStateFailed
	record.failReason = "orb executor disconnected"
	record.runnerMigrationRequired = true
	record.portalIP = ""
	record.portalIPAt = time.Time{}
	return true
}

func (m *neoOrbManager) fenceLifecycleOrbRunnerMigration(ctx context.Context, actor *neoActor, record *neoOrbRecord) bool {
	if ctx == nil || actor == nil || record == nil {
		return false
	}
	fail := func(reason string) {
		m.mu.Lock()
		if m.orbs[record.threadID] == record && record.state == neoOrbStateRunning && record.runnerMigrationRequired && record.runnerMigrationFencing {
			record.runnerMigrationFencing = false
			if !m.stopped && ctx.Err() == nil {
				record.state = neoOrbStateConflict
				record.failReason = reason
			}
		}
		m.mu.Unlock()
	}
	m.mu.Lock()
	exact := !m.stopped && m.orbs[record.threadID] == record && record.lifecycleV1 && record.state == neoOrbStateRunning && record.runnerMigrationRequired && record.runnerMigrationFencing
	m.mu.Unlock()
	if !exact {
		return false
	}
	operation, err := m.beginLifecycleAdmission(ctx, actor, record.threadID, neoOrbStateRunning)
	if err != nil {
		fail("orb runner migration fence is unavailable")
		return false
	}
	guarded := operation.providerClient().(*neoOrbGuardedProvider)
	if err := m.orbStopRecoveredExecutor(ctx, guarded, operation.containerID); err != nil {
		operation.close()
		fail("stale orb executor could not be stopped")
		return false
	}
	result := guarded.PauseContainerInvocation(ctx, operation.containerID)
	operation.close()
	if !result.confirmed() {
		fail("stale orb executor container could not be fenced")
		return false
	}
	m.mu.Lock()
	fenced := !m.stopped && m.orbs[record.threadID] == record && record.state == neoOrbStateRunning && record.runnerMigrationRequired && record.runnerMigrationFencing
	if fenced {
		record.state = neoOrbStatePaused
		record.failReason = ""
		record.runnerMigrationRequired = false
		record.runnerMigrationFencing = false
		record.portalIP = ""
		record.portalIPAt = time.Time{}
	}
	m.mu.Unlock()
	return fenced
}

func (m *neoOrbManager) migrateLifecycleOrbRunner(ctx context.Context, actor *neoActor, record *neoOrbRecord, spawnID string, restart bool) {
	threadID, runnerID, exact := m.orbLaunchIdentity(record, spawnID)
	if !exact {
		return
	}
	if !m.fenceLifecycleOrbRunnerMigration(ctx, actor, record) {
		actor.clearWebLocalExecutorReservation(spawnID, runnerID)
		message := "Cannot safely migrate the recovered orb executor."
		if restart {
			message = "Cannot safely restart the disconnected orb executor."
		}
		actor.broadcastExecutorStatus(spawnID, "failed", message, map[string]any{"reasonCode": "spawn_failed", "threadId": threadID})
		return
	}
	m.mu.Lock()
	claimed := !m.stopped && ctx.Err() == nil && m.orbs[threadID] == record && record.state == neoOrbStatePaused &&
		record.spawnID == spawnID && record.runnerID == runnerID && !record.runnerMigrationRequired && !record.runnerMigrationFencing
	if claimed {
		record.state = neoOrbStateProvisioning
	}
	m.mu.Unlock()
	if !claimed {
		actor.clearWebLocalExecutorReservation(spawnID, runnerID)
		return
	}
	m.resumeLifecycleOrb(ctx, actor, record, spawnID, !restart)
}

func (m *neoOrbManager) wakeExistingLifecycleOrb(a *neoActor) bool {
	if a == nil {
		return false
	}
	a.mu.Lock()
	sandbox := strings.EqualFold(firstNonEmptyString(a.bootstrapExecutorType, a.meta["executorType"]), "sandbox")
	a.mu.Unlock()
	if !sandbox {
		return false
	}
	return m.spawnOrbWithOptions(a, map[string]any{
		"requestId":                "web-reopen-orb-" + randomBase62(12),
		"cliproxyWebLocalRunnerId": "orb",
	}, true) != nil
}

// spawnOrb provisions (or resumes) the orb backing a thread and starts the
// headless Amp executor inside it. The executor connects back to this proxy
// over the thread WebSocket exactly like a locally spawned headless executor.
func (m *neoOrbManager) spawnOrb(a *neoActor, msg map[string]any) map[string]any {
	return m.spawnOrbWithOptions(a, msg, false)
}

func (m *neoOrbManager) spawnOrbWithOptions(a *neoActor, msg map[string]any, existingLifecycleOnly bool) map[string]any {
	spawnID := firstNonEmptyString(msg["spawnId"], msg["requestId"])
	if spawnID == "" {
		spawnID = "spawn-orb-" + randomBase62(12)
	}
	webLocal := strings.TrimSpace(stringValue(msg["cliproxyWebLocalRunnerId"])) != ""
	a.mu.Lock()
	threadID := firstNonEmptyString(a.threadID, a.key)
	multiplayerTTLSeconds := a.multiplayerTTLSeconds
	a.mu.Unlock()
	if !neoThreadIDExactPattern.MatchString(threadID) {
		return a.broadcastExecutorStatus(spawnID, "failed", "Cannot provision an orb without a valid thread ID.", map[string]any{"reasonCode": "environment_missing"})
	}
	cfg := m.runtime.configSnapshot()
	if !existingLifecycleOnly {
		if reason := neoOrbsAvailable(cfg); reason != "" {
			return a.broadcastExecutorStatus(spawnID, "failed", "Cannot provision an orb: "+reason+".", map[string]any{"reasonCode": "spawn_rejected"})
		}
	}
	if err := m.ensureRecovered(cfg); err != nil {
		return a.broadcastExecutorStatus(spawnID, "failed", "Cannot safely reconcile existing orb containers: "+err.Error(), map[string]any{"reasonCode": "environment_recovering", "threadId": threadID})
	}
	if existingLifecycleOnly {
		m.mu.Lock()
		_, exists := m.orbs[threadID]
		m.mu.Unlock()
		if !exists {
			return nil
		}
		if reason := neoOrbsAvailable(cfg); reason != "" {
			return a.broadcastExecutorStatus(spawnID, "failed", "Cannot provision an orb: "+reason+".", map[string]any{"reasonCode": "spawn_rejected", "threadId": threadID})
		}
	}

	portalToken := neoOrbNewPortalToken()
	record := &neoOrbRecord{
		threadID:              threadID,
		runnerID:              neoOrbRunnerID(threadID, portalToken),
		multiplayerTTLSeconds: multiplayerTTLSeconds,
		state:                 neoOrbStateProvisioning,
		workDir:               neoOrbWorkDir,
		portalToken:           portalToken,
		operationMu:           &sync.Mutex{},
	}
	m.mu.Lock()
	existing := m.orbs[threadID]
	existingState := ""
	existingFailReason := ""
	action := "provision"
	if existingLifecycleOnly {
		action = "absent"
	}
	if existing != nil {
		existingState = existing.state
		existingFailReason = existing.failReason
		if existing.recovered {
			action = "conflict"
		} else if existing.lifecycleV1 {
			lifecycleActionable := existing.pendingGeneration == nil && neoOrbLifecycleActionable(existing.activeGeneration)
			switch existing.state {
			case neoOrbStateProvisioning:
				action = "inflight"
			case neoOrbStateRunning:
				if !lifecycleActionable {
					action = "conflict"
					existingFailReason = "lifecycle-v1 activation is not actionable"
				} else if existing.runnerMigrationRequired && !existing.runnerMigrationFencing {
					action = "migrate-lifecycle"
				} else {
					action = "inflight"
				}
			case neoOrbStatePaused:
				if lifecycleActionable {
					action = "resume-lifecycle"
				} else {
					action = "conflict"
					existingFailReason = "lifecycle-v1 activation is not actionable"
				}
			case neoOrbStateFailed:
				if lifecycleActionable && existing.runnerMigrationRequired && !existing.runnerMigrationFencing {
					action = "restart-lifecycle"
				} else {
					action = "conflict"
				}
			default:
				action = "conflict"
			}
		} else if existingLifecycleOnly {
			action = "conflict"
		} else {
			switch existing.state {
			case neoOrbStateProvisioning:
				action = "inflight"
			case neoOrbStateRunning:
				action = "inflight"
			case neoOrbStatePaused:
				action = "resume"
			case neoOrbStateFailed:
				if existing.containerID != "" {
					action = "replace"
				}
			case neoOrbStateConflict:
				action = "conflict"
			}
		}
	}
	launchRecord := record
	launchRunnerID := ""
	resumeLifecycleRecovery := false
	if action == "resume" || action == "resume-lifecycle" || action == "migrate-lifecycle" || action == "restart-lifecycle" || action == "replace" {
		launchRecord = existing
	}
	if action == "replace" {
		launchRecord.spawnID = spawnID
		launchRecord.webLocal = webLocal
		launchRecord.state = neoOrbStateProvisioning
	} else if action == "provision" || action == "resume" || action == "resume-lifecycle" || action == "migrate-lifecycle" || action == "restart-lifecycle" {
		launchRunnerID = neoOrbRunnerID(launchRecord.threadID, launchRecord.portalToken)
		if launchRunnerID == "" {
			action = "conflict"
			existingFailReason = "orb publication provenance is unavailable"
		} else {
			reserved := true
			if webLocal {
				a.mu.Lock()
				reserved = a.reserveWebLocalExecutorLocked(spawnID, launchRunnerID)
				if reserved && existingLifecycleOnly {
					a.webLocalExpectedObserverOnly = true
				}
				a.mu.Unlock()
			}
			if !reserved {
				action = "inflight"
			} else {
				launchRecord.runnerID = launchRunnerID
				launchRecord.spawnID = spawnID
				launchRecord.webLocal = webLocal
				if action == "migrate-lifecycle" || action == "restart-lifecycle" {
					if action == "restart-lifecycle" {
						launchRecord.state = neoOrbStateRunning
					}
					launchRecord.runnerMigrationFencing = true
				} else {
					launchRecord.state = neoOrbStateProvisioning
					if action == "resume-lifecycle" && launchRecord.runnerMigrationRequired {
						resumeLifecycleRecovery = true
						launchRecord.runnerMigrationFencing = true
					}
				}
				if action == "provision" {
					m.orbs[threadID] = launchRecord
				}
			}
		}
	}
	m.mu.Unlock()
	switch action {
	case "absent":
		return nil
	case "inflight":
		status := "starting"
		message := "Orb is already starting for this thread."
		if existingState == neoOrbStateRunning {
			status = "running"
			message = "Orb is already running for this thread."
		}
		if existingLifecycleOnly {
			return map[string]any{"type": "executor_status", "spawnId": existing.spawnID, "status": status, "message": message}
		}
		return a.broadcastExecutorStatus(spawnID, status, message, map[string]any{"reasonCode": "waiting_for_executor_connect", "threadId": threadID})
	case "resume":
		if !m.startWorker(func(ctx context.Context) { m.resumeOrb(ctx, a, existing, spawnID) }) {
			a.clearWebLocalExecutorReservation(spawnID, launchRunnerID)
			m.setState(existing, neoOrbStateFailed, "orb manager is stopping")
			return a.broadcastExecutorStatus(spawnID, "failed", "Cannot resume the orb while the runtime is stopping.", map[string]any{"reasonCode": "spawn_failed", "threadId": threadID})
		}
		m.ensureReaper()
		message := "Resuming paused orb."
		if existingState == neoOrbStateRunning {
			message = "Preparing recovered orb executor."
		}
		return a.broadcastExecutorStatus(spawnID, "starting", message, map[string]any{"reasonCode": "environment_recovering", "threadId": threadID})
	case "resume-lifecycle":
		if !m.startWorker(func(ctx context.Context) { m.resumeLifecycleOrb(ctx, a, existing, spawnID, resumeLifecycleRecovery) }) {
			a.clearWebLocalExecutorReservation(spawnID, launchRunnerID)
			m.setState(existing, neoOrbStateConflict, "orb manager is stopping")
			return a.broadcastExecutorStatus(spawnID, "failed", "Cannot resume the orb while the runtime is stopping.", map[string]any{"reasonCode": "spawn_failed", "threadId": threadID})
		}
		m.ensureReaper()
		return a.broadcastExecutorStatus(spawnID, "starting", "Resuming paused orb.", map[string]any{"reasonCode": "environment_recovering", "threadId": threadID})
	case "migrate-lifecycle", "restart-lifecycle":
		restart := action == "restart-lifecycle"
		if !m.startWorker(func(ctx context.Context) { m.migrateLifecycleOrbRunner(ctx, a, existing, spawnID, restart) }) {
			a.clearWebLocalExecutorReservation(spawnID, launchRunnerID)
			m.mu.Lock()
			if m.orbs[threadID] == existing && existing.runnerMigrationRequired {
				existing.runnerMigrationFencing = false
			}
			m.mu.Unlock()
			m.setState(existing, neoOrbStateConflict, "orb manager is stopping")
			message := "Cannot migrate the recovered orb while the runtime is stopping."
			if restart {
				message = "Cannot restart the disconnected orb executor while the runtime is stopping."
			}
			return a.broadcastExecutorStatus(spawnID, "failed", message, map[string]any{"reasonCode": "spawn_failed", "threadId": threadID})
		}
		m.ensureReaper()
		message := "Migrating the recovered orb executor."
		if restart {
			message = "Restarting the disconnected orb executor."
		}
		return a.broadcastExecutorStatus(spawnID, "starting", message, map[string]any{"reasonCode": "environment_recovering", "threadId": threadID})
	case "replace":
		if !m.startWorker(func(ctx context.Context) { m.replaceFailedOrb(ctx, a, existing, spawnID) }) {
			a.clearWebLocalExecutorReservation(spawnID, launchRunnerID)
			m.setState(existing, neoOrbStateFailed, "orb manager is stopping")
			return a.broadcastExecutorStatus(spawnID, "failed", "Cannot replace the orb while the runtime is stopping.", map[string]any{"reasonCode": "spawn_failed", "threadId": threadID})
		}
		m.ensureReaper()
		return a.broadcastExecutorStatus(spawnID, "starting", "Replacing failed orb container.", map[string]any{"reasonCode": "environment_recovering", "threadId": threadID})
	case "conflict":
		message := "Cannot provision an orb while existing container ownership is ambiguous."
		details := map[string]any{"reasonCode": "spawn_rejected", "threadId": threadID}
		if reason := strings.TrimSpace(existingFailReason); reason != "" {
			message = "Cannot provision an orb: " + reason + "."
			details["recoveryBlocker"] = reason
		}
		return a.broadcastExecutorStatus(spawnID, "failed", message, details)
	}

	a.mu.Lock()
	repositoryURL := strings.TrimSpace(firstNonEmptyString(stringValue(msg["repositoryURL"]), a.meta["repositoryURL"], nestedString(mapValue(a.meta["project"]), "repositoryURL")))
	agentMode := a.agentModeLocked()
	reasoningEffort := a.reasoningEffortForModeLocked(agentMode)
	a.mu.Unlock()

	a.broadcastExecutorStatus(spawnID, "starting", "Provisioning orb container.", map[string]any{"reasonCode": "spawn_requested", "threadId": threadID})
	if !m.startWorker(func(ctx context.Context) {
		m.provisionLifecycleOrb(ctx, a, record, spawnID, repositoryURL, agentMode, reasoningEffort)
	}) {
		a.clearWebLocalExecutorReservation(spawnID, launchRunnerID)
		m.setState(record, neoOrbStateFailed, "orb manager is stopping")
		return a.broadcastExecutorStatus(spawnID, "failed", "Cannot provision an orb while the runtime is stopping.", map[string]any{"reasonCode": "spawn_failed", "threadId": threadID})
	}
	m.ensureReaper()
	return map[string]any{"status": "starting", "message": "Provisioning orb container.", "spawnId": spawnID}
}

func neoOrbNetworkName(raw string) (string, error) {
	name := strings.TrimSpace(raw)
	if name == "" {
		return "", nil
	}
	for _, r := range name {
		if r < 0x20 || r == 0x7f {
			return "", fmt.Errorf("orbs.network %q is not a valid Docker network name", raw)
		}
	}
	return name, nil
}

func (m *neoOrbManager) materializeLifecycleGeneration(ctx context.Context, record *neoOrbRecord, client neoOrbProviderClient, cfg *config.Config, authenticatedOwnerID ...string) error {
	wrap := func(stage string, err error) error {
		return fmt.Errorf("materialize orb lifecycle generation at %s: %w", stage, err)
	}
	if ctx == nil {
		return wrap("validation", errors.New("context is unavailable"))
	}
	if m == nil || m.runtime == nil || record == nil || client == nil || cfg == nil {
		return wrap("validation", errors.New("orb lifecycle transaction is unavailable"))
	}
	operationMu := m.orbOperationMutex(record)
	if err := neoOrbLockOperation(ctx, operationMu); err != nil {
		return wrap("operation lock", err)
	}
	defer operationMu.Unlock()
	if err := ctx.Err(); err != nil {
		return wrap("validation", err)
	}
	if m.runtime.configSnapshot() != cfg {
		return wrap("validation", errors.New("orb configuration is not current"))
	}
	if reason := neoOrbsAvailable(cfg); reason != "" {
		return wrap("validation", errors.New(reason))
	}
	provider, err := resolveNeoOrbDockerEndpoint(neoOrbsConfig(cfg).DockerHost)
	if err != nil {
		return wrap("validation", err)
	}
	network, err := neoOrbNetworkName(neoOrbsConfig(cfg).Network)
	if err != nil {
		return wrap("validation", err)
	}
	m.runtime.orbManagerMu.Lock()
	runtimeManager := m.runtime.orbManager
	m.runtime.orbManagerMu.Unlock()
	if runtimeManager != m {
		return wrap("validation", errors.New("orb manager is not runtime-owned"))
	}
	m.mu.Lock()
	validTTL := record.multiplayerTTLSeconds == 0 || record.multiplayerTTLSeconds >= neoThreadOpenTTLMinSeconds && record.multiplayerTTLSeconds <= neoThreadOpenTTLMaxSeconds
	validRecord := !m.stopped && m.orbs[record.threadID] == record && record.operationMu == operationMu && record.state == neoOrbStateProvisioning && !record.recovered && !record.lifecycleV1 && record.containerID == "" && record.activeGeneration == nil && record.pendingGeneration == nil && record.workDir == neoOrbWorkDir && neoThreadIDExactPattern.MatchString(record.threadID) && len(record.threadID) <= 180 && neoOrbPortalTokenValid(record.portalToken) && validTTL
	validClient := m.client == client && m.clientKey == provider && m.recovered && m.recoveredKey == provider
	m.mu.Unlock()
	if !validRecord {
		return wrap("validation", errors.New("orb record is not the exact provisionable record"))
	}
	if !validClient {
		return wrap("validation", errors.New("orb provider is not the canonical recovered provider"))
	}
	store, err := m.runtime.orbLifecycleStoreFor(provider)
	if err != nil {
		return wrap("lifecycle store", err)
	}
	if store == nil {
		return wrap("lifecycle store", errors.New("orb lifecycle store is unavailable"))
	}
	initial := store.snapshot()
	if initial.Provider != provider {
		return wrap("lifecycle store", errors.New("orb lifecycle provider does not match"))
	}
	if _, exists := initial.Threads[record.threadID]; exists {
		return wrap("lifecycle store", errors.New("orb lifecycle thread already exists"))
	}
	if _, exists := initial.CleanupTombstones[record.threadID]; exists {
		return wrap("lifecycle store", errors.New("orb lifecycle cleanup is pending"))
	}

	reserved, err := store.reserveGeneration(record.threadID, record.portalToken, record.multiplayerTTLSeconds)
	fail := func(stage string, stageErr error) error {
		snapshot := store.snapshot()
		var active, pending *neoOrbLifecycleGeneration
		if lifecycle, exists := snapshot.Threads[record.threadID]; exists {
			active = cloneNeoOrbLifecycleGeneration(lifecycle.Active)
			pending = cloneNeoOrbLifecycleGeneration(lifecycle.Pending)
		} else if tombstone, exists := snapshot.CleanupTombstones[record.threadID]; exists {
			active = cloneNeoOrbLifecycleGeneration(tombstone.Active)
			pending = cloneNeoOrbLifecycleGeneration(tombstone.Pending)
		}
		m.mu.Lock()
		if m.orbs[record.threadID] == record {
			record.lifecycleV1 = true
			record.activeGeneration = active
			record.pendingGeneration = pending
			record.containerID = ""
			record.state = neoOrbStateConflict
			record.failReason = "lifecycle-v1 generation materialization requires reconciliation"
		}
		m.mu.Unlock()
		return wrap(stage, stageErr)
	}
	if err != nil {
		if reserved.Generation != 0 {
			return fail("reserve", err)
		}
		return wrap("reserve", err)
	}
	if err := ctx.Err(); err != nil {
		return fail("reserve", err)
	}
	reservedSnapshot := store.snapshot()
	reservedLifecycle, exists := reservedSnapshot.Threads[record.threadID]
	if !exists || reservedLifecycle.Pending == nil || *reservedLifecycle.Pending != reserved {
		return fail("reserve verification", errors.New("reserved orb lifecycle generation does not match"))
	}
	m.mu.Lock()
	if m.stopped || m.orbs[record.threadID] != record || record.operationMu != operationMu || record.state != neoOrbStateProvisioning || record.lifecycleV1 || record.containerID != "" || record.activeGeneration != nil || record.pendingGeneration != nil {
		m.mu.Unlock()
		return fail("reserve verification", errors.New("orb record changed during reservation"))
	}
	pending := reserved
	record.lifecycleV1 = true
	record.pendingGeneration = &pending
	record.failReason = ""
	m.mu.Unlock()

	checkPending := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		snapshot := store.snapshot()
		lifecycle, exists := snapshot.Threads[record.threadID]
		if !exists || lifecycle.Pending == nil || *lifecycle.Pending != reserved {
			return errors.New("orb lifecycle pending generation changed")
		}
		m.mu.Lock()
		valid := !m.stopped && m.orbs[record.threadID] == record && record.operationMu == operationMu && record.state == neoOrbStateProvisioning && record.lifecycleV1 && record.containerID == "" && record.activeGeneration == nil && record.pendingGeneration != nil && *record.pendingGeneration == reserved
		m.mu.Unlock()
		if !valid {
			return errors.New("orb record changed during lifecycle materialization")
		}
		return nil
	}
	if err := checkPending(); err != nil {
		return fail("home volume precondition", err)
	}
	ownerID := initial.OwnerID
	if _, err := neoOrbCreateLifecycleVolume(ctx, client, ownerID, reserved, neoOrbLifecycleRoleHome); err != nil {
		return fail("home volume", err)
	}
	if err := ctx.Err(); err != nil {
		return fail("home volume", err)
	}
	if err := checkPending(); err != nil {
		return fail("root volume precondition", err)
	}
	if _, err := neoOrbCreateLifecycleVolume(ctx, client, ownerID, reserved, neoOrbLifecycleRoleRoot); err != nil {
		return fail("root volume", err)
	}
	if err := ctx.Err(); err != nil {
		return fail("root volume", err)
	}
	if err := checkPending(); err != nil {
		return fail("container precondition", err)
	}
	orbs := neoOrbsConfig(cfg)
	containerID, err := client.CreateContainer(ctx, buildNeoOrbLifecycleContainerSpec(ownerID, reserved, neoOrbImage(cfg), network, orbs.NanoCPUs, orbs.MemoryMB))
	if err != nil {
		return fail("container creation", err)
	}
	if err := ctx.Err(); err != nil {
		return fail("container creation", err)
	}
	if !neoOrbDockerResourceNameValid(containerID) {
		return fail("container creation", errors.New("created orb container identifier is invalid"))
	}
	generation := reserved
	generation.ContainerID = containerID
	containerState, err := client.InspectContainer(ctx, containerID)
	if err != nil {
		return fail("container inspection", err)
	}
	if err := ctx.Err(); err != nil {
		return fail("container inspection", err)
	}
	homeVolume, err := client.InspectVolume(ctx, generation.HomeVolumeName)
	if err != nil {
		return fail("home volume verification", err)
	}
	if err := ctx.Err(); err != nil {
		return fail("home volume verification", err)
	}
	rootVolume, err := client.InspectVolume(ctx, generation.RootVolumeName)
	if err != nil {
		return fail("root volume verification", err)
	}
	if err := ctx.Err(); err != nil {
		return fail("root volume verification", err)
	}
	if !neoOrbLifecycleContainerMatches(containerID, containerState, homeVolume, rootVolume, ownerID, generation) {
		return fail("resource verification", errors.New("created orb lifecycle resources do not match"))
	}
	if err := checkPending(); err != nil {
		return fail("promotion precondition", err)
	}
	var promoted neoOrbLifecycleGeneration
	if len(authenticatedOwnerID) == 1 && strings.TrimSpace(authenticatedOwnerID[0]) != "" {
		promoted, err = store.promoteGenerationBound(generation, authenticatedOwnerID[0])
	} else {
		err = store.promoteGeneration(generation)
		promoted = generation
		promoted.Phase = neoOrbLifecyclePhaseActive
	}
	if err != nil {
		return fail("promotion", err)
	}
	promotedSnapshot := store.snapshot()
	promotedLifecycle, exists := promotedSnapshot.Threads[record.threadID]
	expectedActive := promoted
	if !exists || promotedLifecycle.Pending != nil || promotedLifecycle.Active == nil || *promotedLifecycle.Active != expectedActive {
		return fail("promotion verification", errors.New("promoted orb lifecycle generation does not match"))
	}
	m.mu.Lock()
	if m.stopped || m.orbs[record.threadID] != record || record.operationMu != operationMu || record.state != neoOrbStateProvisioning || !record.lifecycleV1 || record.containerID != "" || record.activeGeneration != nil || record.pendingGeneration == nil || *record.pendingGeneration != reserved {
		m.mu.Unlock()
		return fail("promotion verification", errors.New("orb record changed before promotion completed"))
	}
	active := expectedActive
	record.activeGeneration = &active
	record.pendingGeneration = nil
	record.containerID = active.ContainerID
	record.failReason = ""
	m.mu.Unlock()
	return nil
}

var errNeoOrbActiveLifecycleGuardRejected = errors.New("active orb lifecycle operation rejected")

type neoOrbActiveLifecycleRecordFence struct {
	state                  string
	workDir                string
	multiplayerTTLSeconds  int
	repositoryURLDigest    [32]byte
	portalTokenDigest      [32]byte
	activePortals          int
	idleSince              time.Time
	failReasonDigest       [32]byte
	portalIP               string
	portalIPAt             time.Time
	recovered              bool
	lifecycleV1            bool
	activeGenerationDigest [32]byte
	prelaunchReady         bool
	preparedOwnerID        string
	suppressSharedGitHub   bool
}

type neoOrbActiveLifecycleOperation struct {
	manager        *neoOrbManager
	record         *neoOrbRecord
	raw            neoOrbProviderClient
	store          *neoOrbLifecycleStore
	operationMu    *sync.Mutex
	leaseCtx       context.Context
	provider       string
	configIdentity uintptr
	threadID       string
	containerID    string
	ownerID        string
	active         neoOrbLifecycleGeneration
	fence          neoOrbActiveLifecycleRecordFence
	closed         atomic.Bool
	closeOnce      sync.Once
	callMu         sync.RWMutex
	streamsMu      sync.Mutex
	streams        map[*neoOrbGuardedArchive]struct{}
}

type neoOrbGuardedProvider struct {
	operation *neoOrbActiveLifecycleOperation
}

type neoOrbGuardedArchive struct {
	operation *neoOrbActiveLifecycleOperation
	ctx       context.Context
	id        string
	raw       io.ReadCloser
	mu        sync.Mutex
	closed    bool
	closeErr  error
}

var _ neoOrbProviderClient = (*neoOrbGuardedProvider)(nil)

func neoOrbSensitiveDigest(value string) [32]byte {
	return sha256.Sum256([]byte(value))
}

func neoOrbLifecycleGenerationDigest(generation *neoOrbLifecycleGeneration) [32]byte {
	if generation == nil {
		return [32]byte{}
	}
	raw, err := json.Marshal(generation)
	if err != nil {
		return [32]byte{}
	}
	return sha256.Sum256(raw)
}

func neoOrbConfigIdentity(cfg *config.Config) uintptr {
	if cfg == nil {
		return 0
	}
	return reflect.ValueOf(cfg).Pointer()
}

func neoOrbProviderClientExact(left, right neoOrbProviderClient) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	leftValue := reflect.ValueOf(left)
	rightValue := reflect.ValueOf(right)
	if leftValue.Type() != rightValue.Type() || !leftValue.Comparable() || !rightValue.Comparable() {
		return false
	}
	return leftValue.Interface() == rightValue.Interface()
}

func neoOrbCaptureActiveLifecycleRecordFence(record *neoOrbRecord) neoOrbActiveLifecycleRecordFence {
	return neoOrbActiveLifecycleRecordFence{
		state:                  record.state,
		workDir:                record.workDir,
		multiplayerTTLSeconds:  record.multiplayerTTLSeconds,
		repositoryURLDigest:    neoOrbSensitiveDigest(record.repositoryURL),
		portalTokenDigest:      neoOrbSensitiveDigest(record.portalToken),
		activePortals:          record.activePortals,
		idleSince:              record.idleSince,
		failReasonDigest:       neoOrbSensitiveDigest(record.failReason),
		portalIP:               record.portalIP,
		portalIPAt:             record.portalIPAt,
		recovered:              record.recovered,
		lifecycleV1:            record.lifecycleV1,
		activeGenerationDigest: neoOrbLifecycleGenerationDigest(record.activeGeneration),
		prelaunchReady:         record.prelaunchReady,
		preparedOwnerID:        record.preparedOwnerID,
		suppressSharedGitHub:   record.suppressSharedGitHub,
	}
}

func (operation *neoOrbActiveLifecycleOperation) recordExactLocked() bool {
	if operation == nil || operation.record == nil {
		return false
	}
	record := operation.record
	fence := operation.fence
	return record.threadID == operation.threadID &&
		record.containerID == operation.containerID &&
		record.state == fence.state &&
		record.workDir == fence.workDir &&
		record.multiplayerTTLSeconds == fence.multiplayerTTLSeconds &&
		neoOrbSensitiveDigest(record.repositoryURL) == fence.repositoryURLDigest &&
		neoOrbSensitiveDigest(record.portalToken) == fence.portalTokenDigest &&
		record.activePortals == fence.activePortals &&
		record.idleSince == fence.idleSince &&
		neoOrbSensitiveDigest(record.failReason) == fence.failReasonDigest &&
		record.portalIP == fence.portalIP &&
		record.portalIPAt == fence.portalIPAt &&
		record.recovered == fence.recovered &&
		record.lifecycleV1 == fence.lifecycleV1 &&
		neoOrbLifecycleGenerationDigest(record.activeGeneration) == fence.activeGenerationDigest &&
		record.pendingGeneration == nil &&
		record.prelaunchReady == fence.prelaunchReady &&
		record.preparedOwnerID == fence.preparedOwnerID &&
		record.suppressSharedGitHub == fence.suppressSharedGitHub &&
		record.operationMu == operation.operationMu
}

func neoOrbActiveLifecycleGuardError(ctx context.Context) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return errors.Join(errNeoOrbActiveLifecycleGuardRejected, err)
		}
	}
	return errNeoOrbActiveLifecycleGuardRejected
}

func (operation *neoOrbActiveLifecycleOperation) contextsExact(ctx context.Context) error {
	if operation == nil || operation.closed.Load() {
		return neoOrbActiveLifecycleGuardError(ctx)
	}
	if operation.leaseCtx == nil {
		return errNeoOrbActiveLifecycleGuardRejected
	}
	if err := operation.leaseCtx.Err(); err != nil {
		return errors.Join(errNeoOrbActiveLifecycleGuardRejected, err)
	}
	if ctx == nil {
		return errNeoOrbActiveLifecycleGuardRejected
	}
	if err := ctx.Err(); err != nil {
		return errors.Join(errNeoOrbActiveLifecycleGuardRejected, err)
	}
	return nil
}

func (operation *neoOrbActiveLifecycleOperation) checkExact(ctx context.Context) error {
	if err := operation.contextsExact(ctx); err != nil {
		return err
	}
	manager := operation.manager
	if manager == nil || manager.runtime == nil || neoOrbConfigIdentity(manager.runtime.configSnapshot()) != operation.configIdentity {
		return errNeoOrbActiveLifecycleGuardRejected
	}
	runtime := manager.runtime
	runtime.orbManagerMu.Lock()
	runtimeExact := runtime.orbManager == manager &&
		runtime.orbLifecycleStoreInitialized &&
		runtime.orbLifecycleStore == operation.store &&
		runtime.orbLifecycleStoreErr == nil &&
		runtime.orbLifecycleStoreProvider == operation.provider
	runtime.orbManagerMu.Unlock()
	if !runtimeExact {
		return errNeoOrbActiveLifecycleGuardRejected
	}
	manager.mu.Lock()
	managerExact := !manager.stopped &&
		manager.orbs[operation.threadID] == operation.record &&
		manager.clientKey == operation.provider &&
		manager.recovered &&
		manager.recoveredKey == operation.provider &&
		neoOrbProviderClientExact(manager.client, operation.raw) &&
		operation.recordExactLocked()
	manager.mu.Unlock()
	if !managerExact {
		return errNeoOrbActiveLifecycleGuardRejected
	}
	store := operation.store
	if store == nil {
		return errNeoOrbActiveLifecycleGuardRejected
	}
	store.mu.Lock()
	storeUsable := !store.closed && store.lockFile != nil && !store.durabilityPending
	snapshot := cloneNeoOrbLifecycleState(store.state)
	store.mu.Unlock()
	if !storeUsable || snapshot.Provider != operation.provider || snapshot.OwnerID != operation.ownerID || validateNeoOrbLifecycleState(snapshot) != nil {
		return errNeoOrbActiveLifecycleGuardRejected
	}
	if _, exists := snapshot.CleanupTombstones[operation.threadID]; exists {
		return errNeoOrbActiveLifecycleGuardRejected
	}
	lifecycle, exists := snapshot.Threads[operation.threadID]
	if !exists || lifecycle.Active == nil || lifecycle.Pending != nil || lifecycle.Active.Phase != neoOrbLifecyclePhaseActive || neoOrbLifecycleGenerationDigest(lifecycle.Active) != operation.fence.activeGenerationDigest {
		return errNeoOrbActiveLifecycleGuardRejected
	}
	if neoOrbLifecycleGenerationHasActivation(*lifecycle.Active) && !manager.activeLifecycleActorBindingExact(operation.threadID, lifecycle.Active) {
		return errNeoOrbActiveLifecycleGuardRejected
	}
	return operation.contextsExact(ctx)
}

func (m *neoOrbManager) beginActiveLifecycleOperation(ctx context.Context, record *neoOrbRecord, raw neoOrbProviderClient, cfg *config.Config) (*neoOrbActiveLifecycleOperation, error) {
	if ctx == nil || m == nil || m.runtime == nil || record == nil || raw == nil || cfg == nil {
		return nil, neoOrbActiveLifecycleGuardError(ctx)
	}
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(errNeoOrbActiveLifecycleGuardRejected, err)
	}
	if m.runtime.configSnapshot() != cfg {
		return nil, errNeoOrbActiveLifecycleGuardRejected
	}
	provider, err := resolveNeoOrbDockerEndpoint(neoOrbsConfig(cfg).DockerHost)
	if err != nil {
		return nil, errNeoOrbActiveLifecycleGuardRejected
	}
	m.mu.Lock()
	operationMu := record.operationMu
	m.mu.Unlock()
	if operationMu == nil {
		return nil, errNeoOrbActiveLifecycleGuardRejected
	}
	if err := neoOrbLockOperation(ctx, operationMu); err != nil {
		return nil, errors.Join(errNeoOrbActiveLifecycleGuardRejected, err)
	}
	operation := &neoOrbActiveLifecycleOperation{
		manager:        m,
		record:         record,
		raw:            raw,
		operationMu:    operationMu,
		leaseCtx:       ctx,
		provider:       provider,
		configIdentity: neoOrbConfigIdentity(cfg),
		streams:        map[*neoOrbGuardedArchive]struct{}{},
	}
	m.runtime.orbManagerMu.Lock()
	operation.store = m.runtime.orbLifecycleStore
	m.runtime.orbManagerMu.Unlock()
	m.mu.Lock()
	operation.threadID = record.threadID
	operation.containerID = record.containerID
	operation.fence = neoOrbCaptureActiveLifecycleRecordFence(record)
	activePortalTokenDigest := [32]byte{}
	if record.activeGeneration != nil {
		activePortalTokenDigest = neoOrbSensitiveDigest(record.activeGeneration.PortalToken)
		operation.active = *record.activeGeneration
		operation.active.PortalToken = ""
	}
	m.mu.Unlock()
	if operation.store != nil {
		operation.ownerID = operation.store.ownerID()
	}
	validState := operation.fence.state == neoOrbStateProvisioning || operation.fence.state == neoOrbStateRunning || operation.fence.state == neoOrbStatePaused
	validRecord := validState && neoThreadIDExactPattern.MatchString(operation.threadID) && len(operation.threadID) <= 180 && operation.fence.workDir == neoOrbWorkDir && operation.fence.lifecycleV1 && !operation.fence.recovered && operation.fence.multiplayerTTLSeconds == operation.active.MultiplayerTTLSeconds && operation.fence.portalTokenDigest == activePortalTokenDigest && operation.fence.activeGenerationDigest != [32]byte{} && operation.active.Phase == neoOrbLifecyclePhaseActive && operation.containerID != "" && operation.containerID == operation.active.ContainerID
	if !validRecord {
		operation.close()
		return nil, errNeoOrbActiveLifecycleGuardRejected
	}
	if err := operation.checkExact(ctx); err != nil {
		operation.close()
		return nil, err
	}
	return operation, nil
}

func (m *neoOrbManager) beginLifecycleAdmission(ctx context.Context, actor *neoActor, threadID string, allowedStates ...string) (*neoOrbActiveLifecycleOperation, error) {
	if ctx == nil || m == nil || m.runtime == nil || actor == nil {
		return nil, neoOrbActiveLifecycleGuardError(ctx)
	}
	cfg := m.runtime.configSnapshot()
	m.mu.Lock()
	record := m.orbs[threadID]
	raw := m.client
	m.mu.Unlock()
	operation, err := m.beginActiveLifecycleOperation(ctx, record, raw, cfg)
	if err != nil {
		return nil, err
	}
	allowed := false
	for _, state := range allowedStates {
		if operation.fence.state == state {
			allowed = true
			break
		}
	}
	m.mu.Lock()
	var active *neoOrbLifecycleGeneration
	if m.orbs[threadID] == record && record.activeGeneration != nil {
		active = cloneNeoOrbLifecycleGeneration(record.activeGeneration)
	}
	exact := allowed && active != nil && neoOrbLifecycleActionable(active) &&
		record.prelaunchReady && record.preparedOwnerID == active.AuthenticatedOwnerID &&
		record.portalToken == active.PortalToken && record.containerID == active.ContainerID &&
		neoOrbLifecycleGenerationDigest(active) == operation.fence.activeGenerationDigest
	m.mu.Unlock()
	if !exact || !m.activeLifecycleActorExact(actor, threadID) || !m.activeLifecycleActorBindingExact(threadID, active) {
		operation.close()
		return nil, errNeoOrbActiveLifecycleGuardRejected
	}
	if err := operation.checkExact(ctx); err != nil {
		operation.close()
		return nil, err
	}
	return operation, nil
}

func (operation *neoOrbActiveLifecycleOperation) providerClient() neoOrbProviderClient {
	return &neoOrbGuardedProvider{operation: operation}
}

func (operation *neoOrbActiveLifecycleOperation) beginCall(ctx context.Context) (func(), error) {
	if operation == nil {
		return nil, neoOrbActiveLifecycleGuardError(ctx)
	}
	operation.callMu.RLock()
	if err := operation.contextsExact(ctx); err != nil {
		operation.callMu.RUnlock()
		return nil, err
	}
	return operation.callMu.RUnlock, nil
}

func (operation *neoOrbActiveLifecycleOperation) close() {
	if operation == nil {
		return
	}
	operation.closeOnce.Do(func() {
		operation.closed.Store(true)
		operation.streamsMu.Lock()
		streams := make([]*neoOrbGuardedArchive, 0, len(operation.streams))
		for stream := range operation.streams {
			streams = append(streams, stream)
		}
		clear(operation.streams)
		operation.streamsMu.Unlock()
		for _, stream := range streams {
			_ = stream.closeRaw()
		}
		operation.callMu.Lock()
		defer operation.callMu.Unlock()
		operation.operationMu.Unlock()
	})
}

func (operation *neoOrbActiveLifecycleOperation) authorize(ctx context.Context, id string) (neoOrbContainerState, error) {
	if operation == nil || id != operation.containerID {
		return neoOrbContainerState{}, neoOrbActiveLifecycleGuardError(ctx)
	}
	if err := operation.checkExact(ctx); err != nil {
		return neoOrbContainerState{}, err
	}
	homeVolume, err := operation.raw.InspectVolume(ctx, operation.active.HomeVolumeName)
	if err != nil {
		return neoOrbContainerState{}, neoOrbActiveLifecycleGuardError(ctx)
	}
	if err := operation.contextsExact(ctx); err != nil || !neoOrbLifecycleVolumeMatches(homeVolume, operation.ownerID, operation.active, neoOrbLifecycleRoleHome) {
		if err != nil {
			return neoOrbContainerState{}, err
		}
		return neoOrbContainerState{}, errNeoOrbActiveLifecycleGuardRejected
	}
	if err := operation.checkExact(ctx); err != nil {
		return neoOrbContainerState{}, err
	}
	rootVolume, err := operation.raw.InspectVolume(ctx, operation.active.RootVolumeName)
	if err != nil {
		return neoOrbContainerState{}, neoOrbActiveLifecycleGuardError(ctx)
	}
	if err := operation.contextsExact(ctx); err != nil || !neoOrbLifecycleVolumeMatches(rootVolume, operation.ownerID, operation.active, neoOrbLifecycleRoleRoot) {
		if err != nil {
			return neoOrbContainerState{}, err
		}
		return neoOrbContainerState{}, errNeoOrbActiveLifecycleGuardRejected
	}
	if err := operation.checkExact(ctx); err != nil {
		return neoOrbContainerState{}, err
	}
	container, err := operation.raw.InspectContainer(ctx, operation.containerID)
	if err != nil {
		return neoOrbContainerState{}, neoOrbActiveLifecycleGuardError(ctx)
	}
	if err := operation.contextsExact(ctx); err != nil {
		return neoOrbContainerState{}, err
	}
	if !neoOrbLifecycleContainerMatches(operation.containerID, container, homeVolume, rootVolume, operation.ownerID, operation.active) {
		return neoOrbContainerState{}, errNeoOrbActiveLifecycleGuardRejected
	}
	if err := operation.checkExact(ctx); err != nil {
		return neoOrbContainerState{}, err
	}
	return container, nil
}

func (provider *neoOrbGuardedProvider) reject(ctx context.Context) error {
	if provider == nil || provider.operation == nil {
		return neoOrbActiveLifecycleGuardError(ctx)
	}
	return neoOrbActiveLifecycleGuardError(ctx)
}

func (provider *neoOrbGuardedProvider) target(ctx context.Context, id string, call func() error) error {
	return provider.targetInvocation(ctx, id, call).Err
}

type neoOrbGuardedInvocationResult struct {
	Invoked        bool
	ProviderOK     bool
	PostAuthorized bool
	Err            error
}

func (result neoOrbGuardedInvocationResult) confirmed() bool {
	return result.Invoked && result.ProviderOK && result.PostAuthorized && result.Err == nil
}

func (provider *neoOrbGuardedProvider) targetInvocation(ctx context.Context, id string, call func() error) neoOrbGuardedInvocationResult {
	if provider == nil || provider.operation == nil || call == nil {
		return neoOrbGuardedInvocationResult{Err: neoOrbActiveLifecycleGuardError(ctx)}
	}
	release, err := provider.operation.beginCall(ctx)
	if err != nil {
		return neoOrbGuardedInvocationResult{Err: err}
	}
	defer release()
	if _, err := provider.operation.authorize(ctx, id); err != nil {
		return neoOrbGuardedInvocationResult{Err: err}
	}
	result := neoOrbGuardedInvocationResult{Invoked: true}
	callErr := call()
	result.ProviderOK = callErr == nil
	if _, postErr := provider.operation.authorize(ctx, id); postErr != nil {
		result.Err = errors.Join(callErr, postErr)
		return result
	}
	result.PostAuthorized = true
	result.Err = callErr
	return result
}

func (provider *neoOrbGuardedProvider) Ping(ctx context.Context) error {
	return provider.reject(ctx)
}

func (provider *neoOrbGuardedProvider) EnsureImage(ctx context.Context, image string) error {
	return provider.reject(ctx)
}

func (provider *neoOrbGuardedProvider) CreateContainer(ctx context.Context, spec neoOrbContainerSpec) (string, error) {
	return "", provider.reject(ctx)
}

func (provider *neoOrbGuardedProvider) ListOrbContainers(ctx context.Context) ([]neoOrbContainerSummary, error) {
	return nil, provider.reject(ctx)
}

func (provider *neoOrbGuardedProvider) StartContainer(ctx context.Context, id string) error {
	return provider.StartContainerInvocation(ctx, id).Err
}

func (provider *neoOrbGuardedProvider) StartContainerInvocation(ctx context.Context, id string) neoOrbGuardedInvocationResult {
	return provider.targetInvocation(ctx, id, func() error { return provider.operation.raw.StartContainer(ctx, id) })
}

func (provider *neoOrbGuardedProvider) StopContainer(ctx context.Context, id string) error {
	return provider.target(ctx, id, func() error { return provider.operation.raw.StopContainer(ctx, id) })
}

func (provider *neoOrbGuardedProvider) PauseContainer(ctx context.Context, id string) error {
	return provider.PauseContainerInvocation(ctx, id).Err
}

func (provider *neoOrbGuardedProvider) PauseContainerInvocation(ctx context.Context, id string) neoOrbGuardedInvocationResult {
	return provider.targetInvocation(ctx, id, func() error { return provider.operation.raw.PauseContainer(ctx, id) })
}

func (provider *neoOrbGuardedProvider) UnpauseContainer(ctx context.Context, id string) error {
	return provider.UnpauseContainerInvocation(ctx, id).Err
}

func (provider *neoOrbGuardedProvider) UnpauseContainerInvocation(ctx context.Context, id string) neoOrbGuardedInvocationResult {
	return provider.targetInvocation(ctx, id, func() error { return provider.operation.raw.UnpauseContainer(ctx, id) })
}

func (provider *neoOrbGuardedProvider) RemoveContainer(ctx context.Context, id string, force bool) error {
	return provider.reject(ctx)
}

func (provider *neoOrbGuardedProvider) InspectContainer(ctx context.Context, id string) (neoOrbContainerState, error) {
	if provider == nil || provider.operation == nil {
		return neoOrbContainerState{}, neoOrbActiveLifecycleGuardError(ctx)
	}
	release, err := provider.operation.beginCall(ctx)
	if err != nil {
		return neoOrbContainerState{}, err
	}
	defer release()
	state, err := provider.operation.authorize(ctx, id)
	if err != nil {
		return neoOrbContainerState{}, err
	}
	return state, nil
}

func (provider *neoOrbGuardedProvider) CreateVolume(ctx context.Context, spec neoOrbVolumeSpec) (neoOrbVolumeState, error) {
	return neoOrbVolumeState{}, provider.reject(ctx)
}

func (provider *neoOrbGuardedProvider) InspectVolume(ctx context.Context, name string) (neoOrbVolumeState, error) {
	return neoOrbVolumeState{}, provider.reject(ctx)
}

func (provider *neoOrbGuardedProvider) ListOrbVolumes(ctx context.Context) ([]neoOrbVolumeState, error) {
	return nil, provider.reject(ctx)
}

func (provider *neoOrbGuardedProvider) RemoveVolume(ctx context.Context, name string) error {
	return provider.reject(ctx)
}

func (provider *neoOrbGuardedProvider) Exec(ctx context.Context, id string, cmd []string, env []string, workDir string) (neoOrbExecResult, error) {
	if provider == nil || provider.operation == nil {
		return neoOrbExecResult{}, neoOrbActiveLifecycleGuardError(ctx)
	}
	release, err := provider.operation.beginCall(ctx)
	if err != nil {
		return neoOrbExecResult{}, err
	}
	defer release()
	if _, err := provider.operation.authorize(ctx, id); err != nil {
		return neoOrbExecResult{}, err
	}
	result, callErr := provider.operation.raw.Exec(ctx, id, cmd, env, workDir)
	if _, err := provider.operation.authorize(ctx, id); err != nil {
		return neoOrbExecResult{}, err
	}
	if callErr != nil {
		return neoOrbExecResult{}, callErr
	}
	return result, nil
}

func (provider *neoOrbGuardedProvider) ReadWorkspaceFile(ctx context.Context, id, workDir, relative string, maxBytes int) ([]byte, error) {
	if provider == nil || provider.operation == nil {
		return nil, neoOrbActiveLifecycleGuardError(ctx)
	}
	release, err := provider.operation.beginCall(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	if _, err := provider.operation.authorize(ctx, id); err != nil {
		return nil, err
	}
	data, callErr := provider.operation.raw.ReadWorkspaceFile(ctx, id, workDir, relative, maxBytes)
	if _, err := provider.operation.authorize(ctx, id); err != nil {
		return nil, err
	}
	if callErr != nil {
		return nil, callErr
	}
	return data, nil
}

func (provider *neoOrbGuardedProvider) ExecDetached(ctx context.Context, id string, cmd []string, env []string, workDir string) error {
	return provider.ExecDetachedInvocation(ctx, id, cmd, env, workDir).Err
}

func (provider *neoOrbGuardedProvider) ExecDetachedInvocation(ctx context.Context, id string, cmd []string, env []string, workDir string) neoOrbGuardedInvocationResult {
	return provider.targetInvocation(ctx, id, func() error { return provider.operation.raw.ExecDetached(ctx, id, cmd, env, workDir) })
}

func (provider *neoOrbGuardedProvider) ArchiveFromContainer(ctx context.Context, id, sourcePath string) (io.ReadCloser, error) {
	if provider == nil || provider.operation == nil {
		return nil, neoOrbActiveLifecycleGuardError(ctx)
	}
	release, err := provider.operation.beginCall(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	if _, err := provider.operation.authorize(ctx, id); err != nil {
		return nil, err
	}
	raw, callErr := provider.operation.raw.ArchiveFromContainer(ctx, id, sourcePath)
	if _, err := provider.operation.authorize(ctx, id); err != nil {
		if raw != nil {
			_ = raw.Close()
		}
		return nil, err
	}
	if callErr != nil {
		if raw != nil {
			_ = raw.Close()
		}
		return nil, callErr
	}
	if raw == nil {
		return nil, errNeoOrbActiveLifecycleGuardRejected
	}
	stream := &neoOrbGuardedArchive{operation: provider.operation, ctx: ctx, id: id, raw: raw}
	provider.operation.streamsMu.Lock()
	if provider.operation.closed.Load() {
		provider.operation.streamsMu.Unlock()
		_ = stream.closeRaw()
		return nil, errNeoOrbActiveLifecycleGuardRejected
	}
	provider.operation.streams[stream] = struct{}{}
	provider.operation.streamsMu.Unlock()
	return stream, nil
}

func (provider *neoOrbGuardedProvider) CopyArchiveToContainer(ctx context.Context, id, destinationPath string, archive io.Reader) error {
	return provider.target(ctx, id, func() error { return provider.operation.raw.CopyArchiveToContainer(ctx, id, destinationPath, archive) })
}

func (provider *neoOrbGuardedProvider) CopyFileToContainer(ctx context.Context, id, destPath string, content []byte, mode int64) error {
	return provider.target(ctx, id, func() error { return provider.operation.raw.CopyFileToContainer(ctx, id, destPath, content, mode) })
}

func (provider *neoOrbGuardedProvider) CopyTarToContainer(ctx context.Context, id, destDir string, files map[string][]byte, mode int64) error {
	return provider.target(ctx, id, func() error { return provider.operation.raw.CopyTarToContainer(ctx, id, destDir, files, mode) })
}

func (stream *neoOrbGuardedArchive) Read(buffer []byte) (int, error) {
	if stream == nil || stream.operation == nil {
		return 0, errNeoOrbActiveLifecycleGuardRejected
	}
	release, err := stream.operation.beginCall(stream.ctx)
	if err != nil {
		_ = stream.closeRaw()
		return 0, err
	}
	defer release()
	stream.mu.Lock()
	closed := stream.closed || stream.raw == nil
	stream.mu.Unlock()
	if closed {
		return 0, errNeoOrbActiveLifecycleGuardRejected
	}
	if _, err := stream.operation.authorize(stream.ctx, stream.id); err != nil {
		_ = stream.closeRaw()
		return 0, err
	}
	stream.mu.Lock()
	if stream.closed || stream.raw == nil {
		stream.mu.Unlock()
		return 0, errNeoOrbActiveLifecycleGuardRejected
	}
	raw := stream.raw
	stream.mu.Unlock()
	read, readErr := raw.Read(buffer)
	if _, err := stream.operation.authorize(stream.ctx, stream.id); err != nil {
		_ = stream.closeRaw()
		return 0, err
	}
	return read, readErr
}

func (stream *neoOrbGuardedArchive) closeRaw() error {
	if stream == nil {
		return nil
	}
	stream.mu.Lock()
	defer stream.mu.Unlock()
	if stream.closed {
		return stream.closeErr
	}
	stream.closed = true
	if stream.raw != nil {
		stream.closeErr = stream.raw.Close()
		stream.raw = nil
	}
	return stream.closeErr
}

func (stream *neoOrbGuardedArchive) Close() error {
	if stream == nil {
		return nil
	}
	err := stream.closeRaw()
	if stream.operation != nil {
		stream.operation.streamsMu.Lock()
		delete(stream.operation.streams, stream)
		stream.operation.streamsMu.Unlock()
	}
	return err
}

func (m *neoOrbManager) activeLifecycleActorOwner(actor *neoActor, threadID string) (string, bool) {
	if !m.activeLifecycleActorExact(actor, threadID) {
		return "", false
	}
	actor.mu.Lock()
	ownerID, exact := neoOrbExactActorOwnerLocked(actor)
	actor.mu.Unlock()
	if !exact || ownerID == "" || !m.activeLifecycleActorExact(actor, threadID) {
		return "", false
	}
	return ownerID, true
}

func (m *neoOrbManager) activeLifecycleActorExact(actor *neoActor, threadID string) bool {
	if m == nil || m.runtime == nil || m.runtime.store == nil || actor == nil || actor.runtime != m.runtime || actor.name != "thread-actor" || actor.key != threadID || actor.threadID != threadID {
		return false
	}
	store := m.runtime.store
	store.mu.RLock()
	published := store.actors[store.byNameKey["thread-actor\x00"+threadID]]
	claim := store.threadClaims[threadID]
	exact := published == actor || claim != nil && claim.actor == actor
	store.mu.RUnlock()
	return exact
}

func neoOrbExactActorOwnerLocked(actor *neoActor) (string, bool) {
	if actor == nil {
		return "", false
	}
	ownerID := ""
	for _, key := range []string{"ownerUserId", "ownerUserID", "creatorUserID", "creatorUserId"} {
		value := strings.TrimSpace(stringValue(actor.meta[key]))
		if value == "" {
			continue
		}
		if ownerID != "" && ownerID != value {
			return "", false
		}
		ownerID = value
	}
	if ownerID == "" {
		ownerID = neoLocalOwnerUserID
	}
	return ownerID, true
}

func neoOrbLifecycleRevision(value any) (uint64, bool) {
	switch typed := value.(type) {
	case uint64:
		return typed, typed != 0
	case uint:
		return uint64(typed), typed != 0
	case int:
		return uint64(typed), typed > 0
	case int64:
		return uint64(typed), typed > 0
	case float64:
		if typed <= 0 || typed > 1<<53 || typed != float64(uint64(typed)) {
			return 0, false
		}
		return uint64(typed), true
	case json.Number:
		parsed, err := strconv.ParseUint(string(typed), 10, 64)
		return parsed, err == nil && parsed != 0
	case string:
		if typed == "" || strings.TrimSpace(typed) != typed {
			return 0, false
		}
		parsed, err := strconv.ParseUint(typed, 10, 64)
		return parsed, err == nil && parsed != 0 && strconv.FormatUint(parsed, 10) == typed
	default:
		return 0, false
	}
}

func neoOrbPersistedLifecycleBinding(thread map[string]any) (string, string, string, uint64, bool) {
	if len(thread) == 0 {
		return "", "", "", 0, false
	}
	meta := mapValue(thread["meta"])
	threadMeta := mapValue(thread["threadMeta"])
	data := mapValue(thread["data"])
	dataMeta := mapValue(data["meta"])
	exactString := func(values ...any) (string, bool) {
		selected := ""
		for _, value := range values {
			candidate := strings.TrimSpace(stringValue(value))
			if candidate == "" {
				continue
			}
			if selected != "" && selected != candidate {
				return "", false
			}
			selected = candidate
		}
		return selected, selected != ""
	}
	ownerID := ""
	ownerExact := true
	for _, value := range []any{
		thread["ownerUserId"], thread["ownerUserID"], thread["creatorUserID"], thread["creatorUserId"],
		meta["ownerUserId"], meta["ownerUserID"], meta["creatorUserID"], meta["creatorUserId"],
		threadMeta["ownerUserId"], threadMeta["ownerUserID"], threadMeta["creatorUserID"], threadMeta["creatorUserId"],
		data["ownerUserId"], data["ownerUserID"], data["creatorUserID"], data["creatorUserId"],
		dataMeta["ownerUserId"], dataMeta["ownerUserID"], dataMeta["creatorUserID"], dataMeta["creatorUserId"],
	} {
		candidate := strings.TrimSpace(stringValue(value))
		if candidate == "" {
			continue
		}
		if ownerID != "" && ownerID != candidate {
			ownerExact = false
			break
		}
		ownerID = candidate
	}
	if ownerID == "" {
		ownerID = neoLocalOwnerUserID
	}
	containerID, containerExact := exactString(meta[neoOrbContainerIDMetaKey], threadMeta[neoOrbContainerIDMetaKey], dataMeta[neoOrbContainerIDMetaKey])
	portalToken, portalExact := exactString(meta[neoOrbPortalTokenMetaKey], threadMeta[neoOrbPortalTokenMetaKey], dataMeta[neoOrbPortalTokenMetaKey])
	var revision uint64
	revisionExact := false
	for _, value := range []any{meta[neoOrbLifecycleRevisionMetaKey], threadMeta[neoOrbLifecycleRevisionMetaKey], dataMeta[neoOrbLifecycleRevisionMetaKey]} {
		candidate, found := neoOrbLifecycleRevision(value)
		if !found {
			if value != nil {
				return "", "", "", 0, false
			}
			continue
		}
		if revisionExact && revision != candidate {
			return "", "", "", 0, false
		}
		revision = candidate
		revisionExact = true
	}
	return ownerID, containerID, portalToken, revision, ownerExact && containerExact && portalExact && revisionExact && neoOrbPortalTokenValid(portalToken)
}

func (m *neoOrbManager) activeLifecycleActorBindingExact(threadID string, generation *neoOrbLifecycleGeneration) bool {
	if m == nil || m.runtime == nil || m.runtime.store == nil || generation == nil || generation.ThreadID != threadID || !neoOrbLifecycleGenerationHasActivation(*generation) {
		return false
	}
	actor := m.runtime.store.lookupThreadActor(threadID)
	if actor == nil {
		actor = m.runtime.store.pendingThreadActor(threadID)
	}
	if !m.activeLifecycleActorExact(actor, threadID) {
		return false
	}
	actor.mu.Lock()
	ownerID, ownerExact := neoOrbExactActorOwnerLocked(actor)
	containerID := strings.TrimSpace(stringValue(actor.meta[neoOrbContainerIDMetaKey]))
	portalToken := strings.TrimSpace(stringValue(actor.meta[neoOrbPortalTokenMetaKey]))
	revision, revisionExact := neoOrbLifecycleRevision(actor.meta[neoOrbLifecycleRevisionMetaKey])
	actor.mu.Unlock()
	return ownerExact && ownerID == generation.AuthenticatedOwnerID && containerID == generation.ContainerID && portalToken == generation.PortalToken && revisionExact && revision == generation.Revision && m.activeLifecycleActorExact(actor, threadID)
}

func (m *neoOrbManager) normalizeActiveLifecyclePrelaunchFailure(record *neoOrbRecord, store *neoOrbLifecycleStore) {
	if m == nil || record == nil {
		return
	}
	var active, pending *neoOrbLifecycleGeneration
	if store != nil {
		snapshot := store.snapshot()
		if lifecycle, exists := snapshot.Threads[record.threadID]; exists {
			active = cloneNeoOrbLifecycleGeneration(lifecycle.Active)
			pending = cloneNeoOrbLifecycleGeneration(lifecycle.Pending)
		} else if tombstone, exists := snapshot.CleanupTombstones[record.threadID]; exists {
			active = cloneNeoOrbLifecycleGeneration(tombstone.Active)
			pending = cloneNeoOrbLifecycleGeneration(tombstone.Pending)
		}
	}
	m.mu.Lock()
	if m.orbs[record.threadID] == record {
		record.state = neoOrbStateConflict
		record.containerID = ""
		record.recovered = false
		record.lifecycleV1 = true
		record.activeGeneration = active
		record.pendingGeneration = pending
		record.prelaunchReady = false
		record.preparedOwnerID = ""
		record.failReason = "lifecycle-v1 prelaunch requires reconciliation"
	}
	m.mu.Unlock()
}

func (m *neoOrbManager) beginLifecycleActivation(ctx context.Context, record *neoOrbRecord, operationKind string) (neoOrbLifecycleActivation, neoOrbLifecycleGeneration, error) {
	if ctx == nil || m == nil || m.runtime == nil || record == nil {
		return neoOrbLifecycleActivation{}, neoOrbLifecycleGeneration{}, neoOrbActiveLifecycleGuardError(ctx)
	}
	operationMu := m.orbOperationMutex(record)
	if err := neoOrbLockOperation(ctx, operationMu); err != nil {
		return neoOrbLifecycleActivation{}, neoOrbLifecycleGeneration{}, err
	}
	defer operationMu.Unlock()
	m.runtime.orbManagerMu.Lock()
	store := m.runtime.orbLifecycleStore
	storeExact := m.runtime.orbManager == m && m.runtime.orbLifecycleStoreInitialized && store != nil && m.runtime.orbLifecycleStoreErr == nil
	m.runtime.orbManagerMu.Unlock()
	m.mu.Lock()
	valid := !m.stopped && m.orbs[record.threadID] == record && record.operationMu == operationMu && record.lifecycleV1 && !record.recovered && record.pendingGeneration == nil && record.activeGeneration != nil && record.containerID == record.activeGeneration.ContainerID && record.portalToken == record.activeGeneration.PortalToken
	var expected neoOrbLifecycleGeneration
	if valid {
		expected = *record.activeGeneration
	}
	m.mu.Unlock()
	if !storeExact || !valid || !m.activeLifecycleActorBindingExact(record.threadID, &expected) {
		return neoOrbLifecycleActivation{}, neoOrbLifecycleGeneration{}, errNeoOrbActiveLifecycleGuardRejected
	}
	prior := neoOrbLifecycleActivation{State: expected.ActivationState, OperationID: expected.OperationID, OperationKind: expected.OperationKind}
	launching, err := store.beginActivation(expected, randomBase62(24), operationKind)
	if err != nil {
		return prior, launching, err
	}
	m.mu.Lock()
	if m.orbs[record.threadID] != record || record.operationMu != operationMu || record.activeGeneration == nil || *record.activeGeneration != expected {
		m.mu.Unlock()
		return prior, launching, errNeoOrbActiveLifecycleGuardRejected
	}
	record.activeGeneration = cloneNeoOrbLifecycleGeneration(&launching)
	m.mu.Unlock()
	return prior, launching, nil
}

func (m *neoOrbManager) abortLifecycleActivationLocked(record *neoOrbRecord, store *neoOrbLifecycleStore, expected neoOrbLifecycleGeneration, prior neoOrbLifecycleActivation) error {
	restored, err := store.abortUninvokedActivation(expected, prior)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.orbs[record.threadID] != record || record.activeGeneration == nil || *record.activeGeneration != expected {
		return errNeoOrbActiveLifecycleGuardRejected
	}
	record.activeGeneration = cloneNeoOrbLifecycleGeneration(&restored)
	return nil
}

func (m *neoOrbManager) finishLifecycleActivationLocked(record *neoOrbRecord, store *neoOrbLifecycleStore, expected neoOrbLifecycleGeneration) (neoOrbLifecycleGeneration, error) {
	actionable, err := store.finishActivation(expected)
	if err != nil {
		return actionable, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.orbs[record.threadID] != record || record.activeGeneration == nil || *record.activeGeneration != expected {
		return actionable, errNeoOrbActiveLifecycleGuardRejected
	}
	record.activeGeneration = cloneNeoOrbLifecycleGeneration(&actionable)
	return actionable, nil
}

func (m *neoOrbManager) commitActiveLifecyclePrelaunchReady(ctx context.Context, actor *neoActor, record *neoOrbRecord, operation *neoOrbActiveLifecycleOperation, ownerID string) error {
	return m.commitActiveLifecyclePrelaunchReadyWithSelection(ctx, actor, record, operation, ownerID, neoOrbCredentialSelection{})
}

func (m *neoOrbManager) commitActiveLifecyclePrelaunchReadyWithSelection(ctx context.Context, actor *neoActor, record *neoOrbRecord, operation *neoOrbActiveLifecycleOperation, ownerID string, credentialSelection neoOrbCredentialSelection) error {
	if ctx == nil || m == nil || m.runtime == nil || actor == nil || record == nil || operation == nil || operation.manager != m || operation.record != record || ownerID == "" {
		return neoOrbActiveLifecycleGuardError(ctx)
	}
	release, err := operation.beginCall(ctx)
	if err != nil {
		return err
	}
	defer release()
	if currentOwnerID, exact := m.activeLifecycleActorOwner(actor, operation.threadID); !exact || currentOwnerID != ownerID {
		return errNeoOrbActiveLifecycleGuardRejected
	}
	if err := operation.checkExact(ctx); err != nil {
		return err
	}
	runtime := m.runtime
	if neoOrbConfigIdentity(runtime.configSnapshot()) != operation.configIdentity {
		return errNeoOrbActiveLifecycleGuardRejected
	}
	runtime.orbManagerMu.Lock()
	defer runtime.orbManagerMu.Unlock()
	if runtime.orbManager != m || !runtime.orbLifecycleStoreInitialized || runtime.orbLifecycleStore != operation.store || runtime.orbLifecycleStoreErr != nil || runtime.orbLifecycleStoreProvider != operation.provider {
		return errNeoOrbActiveLifecycleGuardRejected
	}
	store := operation.store
	if store == nil {
		return errNeoOrbActiveLifecycleGuardRejected
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed || store.lockFile == nil || store.durabilityPending || store.provider != operation.provider || store.state.Provider != operation.provider || store.state.OwnerID != operation.ownerID || validateNeoOrbLifecycleState(store.state) != nil {
		return errNeoOrbActiveLifecycleGuardRejected
	}
	if _, exists := store.state.CleanupTombstones[operation.threadID]; exists {
		return errNeoOrbActiveLifecycleGuardRejected
	}
	lifecycle, exists := store.state.Threads[operation.threadID]
	if !exists || lifecycle.Active == nil || lifecycle.Active.Phase != neoOrbLifecyclePhaseActive || lifecycle.Active.ContainerID != operation.containerID || lifecycle.Pending != nil || neoOrbLifecycleGenerationDigest(lifecycle.Active) != operation.fence.activeGenerationDigest {
		return errNeoOrbActiveLifecycleGuardRejected
	}
	if err := operation.contextsExact(ctx); err != nil {
		return err
	}
	active := cloneNeoOrbLifecycleGeneration(lifecycle.Active)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped || m.orbs[operation.threadID] != record || !neoOrbProviderClientExact(m.client, operation.raw) || m.clientKey != operation.provider || !m.recovered || m.recoveredKey != operation.provider || !operation.recordExactLocked() {
		return errNeoOrbActiveLifecycleGuardRejected
	}
	record.state = neoOrbStateProvisioning
	record.containerID = active.ContainerID
	record.recovered = false
	record.lifecycleV1 = true
	record.activeGeneration = active
	record.pendingGeneration = nil
	record.prelaunchReady = true
	record.preparedOwnerID = ownerID
	record.suppressSharedGitHub = credentialSelection.suppressSharedGitHub()
	record.failReason = ""
	return nil
}

func (m *neoOrbManager) normalizeFreshActiveLifecyclePrelaunchFailure(record *neoOrbRecord) {
	if m == nil || m.runtime == nil || record == nil {
		return
	}
	m.mu.Lock()
	fresh := m.orbs[record.threadID] == record && record.state == neoOrbStateProvisioning && record.lifecycleV1 && !record.recovered && record.activeGeneration != nil && record.activeGeneration.Phase == neoOrbLifecyclePhaseActive && record.pendingGeneration == nil && record.containerID == record.activeGeneration.ContainerID
	m.mu.Unlock()
	if !fresh {
		return
	}
	m.runtime.orbManagerMu.Lock()
	store := m.runtime.orbLifecycleStore
	m.runtime.orbManagerMu.Unlock()
	m.normalizeActiveLifecyclePrelaunchFailure(record, store)
}

func (m *neoOrbManager) reconcileActiveLifecyclePrelaunch(ctx context.Context, actor *neoActor, record *neoOrbRecord, raw neoOrbProviderClient, cfg *config.Config, repositoryURL string) error {
	_, err := m.reconcileActiveLifecyclePrelaunchWithSelection(ctx, actor, record, raw, cfg, repositoryURL)
	return err
}

func (m *neoOrbManager) reconcileActiveLifecyclePrelaunchWithSelection(ctx context.Context, actor *neoActor, record *neoOrbRecord, raw neoOrbProviderClient, cfg *config.Config, repositoryURL string) (neoOrbCredentialSelection, error) {
	wrap := func(stage string, err error) error {
		return fmt.Errorf("reconcile active orb lifecycle prelaunch at %s: %w", stage, err)
	}
	if ctx == nil || m == nil || m.runtime == nil || record == nil || raw == nil || cfg == nil {
		return neoOrbCredentialSelection{}, wrap("validation", neoOrbActiveLifecycleGuardError(ctx))
	}
	m.mu.Lock()
	threadID := record.threadID
	prelaunchRecord := m.orbs[threadID] == record && record.state == neoOrbStateProvisioning && record.lifecycleV1 && !record.recovered && record.activeGeneration != nil && record.activeGeneration.Phase == neoOrbLifecyclePhaseActive && record.pendingGeneration == nil && record.containerID != "" && record.containerID == record.activeGeneration.ContainerID
	m.mu.Unlock()
	if !prelaunchRecord {
		return neoOrbCredentialSelection{}, wrap("validation", errNeoOrbActiveLifecycleGuardRejected)
	}
	ownerID, actorExact := m.activeLifecycleActorOwner(actor, threadID)
	if !actorExact {
		m.normalizeFreshActiveLifecyclePrelaunchFailure(record)
		return neoOrbCredentialSelection{}, wrap("actor validation", errNeoOrbActiveLifecycleGuardRejected)
	}
	priorActivation, _, err := m.beginLifecycleActivation(ctx, record, neoOrbLifecycleOperationStart)
	if err != nil {
		m.normalizeFreshActiveLifecyclePrelaunchFailure(record)
		return neoOrbCredentialSelection{}, wrap("activation checkpoint", err)
	}
	operation, err := m.beginActiveLifecycleOperation(ctx, record, raw, cfg)
	if err != nil {
		m.mu.Lock()
		expected := *record.activeGeneration
		m.mu.Unlock()
		operationMu := m.orbOperationMutex(record)
		if errLock := neoOrbLockOperation(ctx, operationMu); errLock == nil {
			_ = m.abortLifecycleActivationLocked(record, m.runtime.orbLifecycleStore, expected, priorActivation)
			operationMu.Unlock()
		}
		m.normalizeFreshActiveLifecyclePrelaunchFailure(record)
		return neoOrbCredentialSelection{}, wrap("operation lease", err)
	}
	guarded := operation.providerClient().(*neoOrbGuardedProvider)
	fail := func(stage string, stageErr error) (neoOrbCredentialSelection, error) {
		m.normalizeActiveLifecyclePrelaunchFailure(record, operation.store)
		operation.close()
		return neoOrbCredentialSelection{}, wrap(stage, stageErr)
	}
	if operation.fence.state != neoOrbStateProvisioning {
		return fail("validation", errNeoOrbActiveLifecycleGuardRejected)
	}
	containerID := operation.containerID
	startResult := guarded.StartContainerInvocation(ctx, containerID)
	if !startResult.confirmed() {
		if !startResult.Invoked {
			m.mu.Lock()
			expected := *record.activeGeneration
			m.mu.Unlock()
			_ = m.abortLifecycleActivationLocked(record, operation.store, expected, priorActivation)
		}
		return fail("start", startResult.Err)
	}
	if err := m.orbBootstrapTools(ctx, guarded, containerID); err != nil {
		return fail("tools", err)
	}
	if err := m.orbConfigureAgentBrowser(ctx, guarded, containerID, threadID, true); err != nil {
		return fail("browser", err)
	}
	if err := m.orbInstallExecutor(ctx, cfg, guarded, containerID); err != nil {
		return fail("executor", err)
	}
	if err := m.orbConfigurePortalHelper(ctx, guarded, containerID); err != nil {
		return fail("portal", err)
	}
	if currentOwnerID, exact := m.activeLifecycleActorOwner(actor, threadID); !exact || currentOwnerID != ownerID {
		return fail("credential actor validation", errNeoOrbActiveLifecycleGuardRejected)
	}
	credentialSelection, err := m.orbReconcileOwnerCredentials(ctx, guarded, containerID, ownerID)
	if err != nil {
		return fail("credentials", err)
	}
	if !credentialSelection.suppressSharedGitHub() && neoOrbGitHubToken(cfg) != "" {
		if err := m.orbConfigureGitHubAuth(ctx, cfg, guarded, containerID); err != nil {
			return fail("GitHub fallback", err)
		}
	}
	if currentOwnerID, exact := m.activeLifecycleActorOwner(actor, threadID); !exact || currentOwnerID != ownerID {
		return fail("config actor validation", errNeoOrbActiveLifecycleGuardRejected)
	}
	hasOwnerConfig, err := m.orbReconcileOwnerConfig(ctx, cfg, guarded, containerID, ownerID)
	if err != nil {
		return fail("config", err)
	}
	if currentOwnerID, exact := m.activeLifecycleActorOwner(actor, threadID); !exact || currentOwnerID != ownerID {
		return fail("post-config actor validation", errNeoOrbActiveLifecycleGuardRejected)
	}
	if !hasOwnerConfig && neoOrbSyncLocalConfigEnabled(cfg) {
		m.orbSyncLocalConfig(ctx, guarded, containerID)
	}
	if currentOwnerID, exact := m.activeLifecycleActorOwner(actor, threadID); !exact || currentOwnerID != ownerID {
		return fail("workspace actor validation", errNeoOrbActiveLifecycleGuardRejected)
	}
	if err := m.orbConfigureWorkspace(ctx, guarded, containerID, repositoryURL, operation.fence.workDir); err != nil {
		return fail("workspace", err)
	}
	if currentOwnerID, exact := m.activeLifecycleActorOwner(actor, threadID); !exact || currentOwnerID != ownerID {
		return fail("final actor validation", errNeoOrbActiveLifecycleGuardRejected)
	}
	if _, err := guarded.InspectContainer(ctx, containerID); err != nil {
		return fail("final verification", err)
	}
	if currentOwnerID, exact := m.activeLifecycleActorOwner(actor, threadID); !exact || currentOwnerID != ownerID {
		return fail("readiness actor validation", errNeoOrbActiveLifecycleGuardRejected)
	}
	if err := m.commitActiveLifecyclePrelaunchReadyWithSelection(ctx, actor, record, operation, ownerID, credentialSelection); err != nil {
		return fail("readiness commit", err)
	}
	operation.close()
	return credentialSelection, nil
}

func (m *neoOrbManager) provisionLifecycleOrb(ctx context.Context, actor *neoActor, record *neoOrbRecord, spawnID, repositoryURL, agentMode, reasoningEffort string) {
	threadID, runnerID, exact := m.orbLaunchIdentity(record, spawnID)
	if !exact {
		return
	}
	cfg := m.runtime.configSnapshot()
	watcherStarted := false
	defer func() {
		if !watcherStarted {
			actor.clearWebLocalExecutorReservation(spawnID, runnerID)
		}
	}()
	fail := func(stage, message string, err error) {
		m.runtime.orbManagerMu.Lock()
		store := m.runtime.orbLifecycleStore
		m.runtime.orbManagerMu.Unlock()
		m.normalizeActiveLifecyclePrelaunchFailure(record, store)
		m.mu.Lock()
		if m.orbs[threadID] == record && !record.lifecycleV1 {
			record.state = neoOrbStateFailed
			record.failReason = message
		}
		m.mu.Unlock()
		log.Warnf("amp orbs: lifecycle provision thread=%s stage=%s failed: %v", threadID, stage, err)
		actor.broadcastExecutorStatus(spawnID, "failed", message, map[string]any{"reasonCode": "spawn_failed", "threadId": threadID})
	}
	ownerID, actorExact := m.activeLifecycleActorOwner(actor, threadID)
	if !actorExact {
		fail("owner", "Cannot bind the orb to its authenticated owner.", errNeoOrbActiveLifecycleGuardRejected)
		return
	}
	client, err := m.dockerClient(cfg)
	if err != nil {
		fail("connect", "Cannot reach the orb provider.", err)
		return
	}
	pingCtx, pingCancel := context.WithTimeout(ctx, 15*time.Second)
	err = client.Ping(pingCtx)
	pingCancel()
	if err != nil {
		fail("connect", "Cannot reach the orb provider.", err)
		return
	}
	setupCtx, setupCancel := context.WithTimeout(ctx, neoOrbSetupTimeout(cfg))
	defer setupCancel()
	if err := client.EnsureImage(setupCtx, neoOrbImage(cfg)); err != nil {
		fail("image", "Cannot prepare the orb image.", err)
		return
	}
	m.mu.Lock()
	if m.orbs[threadID] == record {
		record.repositoryURL = repositoryURL
	}
	m.mu.Unlock()
	if err := m.materializeLifecycleGeneration(setupCtx, record, client, cfg, ownerID); err != nil {
		fail("materialize", "Cannot durably materialize the orb.", err)
		return
	}
	m.mu.Lock()
	if record.activeGeneration == nil {
		m.mu.Unlock()
		fail("binding", "Cannot durably bind the orb.", errNeoOrbActiveLifecycleGuardRejected)
		return
	}
	active := *record.activeGeneration
	m.mu.Unlock()
	if err := m.persistLifecycleContainerBinding(actor, active); err != nil {
		fail("binding", "Cannot durably bind the orb.", err)
		return
	}
	credentialSelection, err := m.reconcileActiveLifecyclePrelaunchWithSelection(setupCtx, actor, record, client, cfg, repositoryURL)
	if err != nil {
		fail("prelaunch", "Cannot safely prepare the orb.", err)
		return
	}
	priorActivation, _, err := m.beginLifecycleActivation(setupCtx, record, neoOrbLifecycleOperationExecDetached)
	if err != nil {
		fail("executor checkpoint", "Cannot durably prepare the orb executor.", err)
		return
	}
	operation, err := m.beginActiveLifecycleOperation(setupCtx, record, client, cfg)
	if err != nil {
		operationMu := m.orbOperationMutex(record)
		if errLock := neoOrbLockOperation(setupCtx, operationMu); errLock == nil {
			m.mu.Lock()
			expected := *record.activeGeneration
			m.mu.Unlock()
			_ = m.abortLifecycleActivationLocked(record, m.runtime.orbLifecycleStore, expected, priorActivation)
			operationMu.Unlock()
		}
		fail("executor guard", "Cannot safely start the orb executor.", err)
		return
	}
	guarded := operation.providerClient().(*neoOrbGuardedProvider)
	env := neoOrbExecutorEnvWithCredentials(cfg, threadID, record.workDir, record.portalToken, credentialSelection.suppressSharedGitHub())
	args := neoHeadlessExecutorArgs(threadID, agentMode, reasoningEffort, neoOrbExecutorLog, runnerID)
	actor.broadcastExecutorStatus(spawnID, "starting", "Starting the headless executor in the orb.", map[string]any{"reasonCode": "starting_headless", "threadId": threadID})
	result := guarded.ExecDetachedInvocation(setupCtx, operation.containerID, neoOrbHeadlessCommand(args), env, operation.fence.workDir)
	if !result.confirmed() {
		if !result.Invoked {
			m.mu.Lock()
			expected := *record.activeGeneration
			m.mu.Unlock()
			_ = m.abortLifecycleActivationLocked(record, operation.store, expected, priorActivation)
		}
		operation.close()
		fail("executor launch", "Cannot safely start the orb executor.", result.Err)
		return
	}
	if owner, exact := m.activeLifecycleActorOwner(actor, threadID); !exact || owner != ownerID {
		operation.close()
		fail("owner verification", "Cannot safely activate the orb.", errNeoOrbActiveLifecycleGuardRejected)
		return
	}
	m.mu.Lock()
	expected := *record.activeGeneration
	m.mu.Unlock()
	actionable, err := m.finishLifecycleActivationLocked(record, operation.store, expected)
	if err != nil {
		operation.close()
		fail("actionable checkpoint", "Cannot safely activate the orb.", err)
		return
	}
	bindingExact := m.activeLifecycleActorBindingExact(threadID, &actionable)
	m.mu.Lock()
	published := bindingExact && m.orbs[threadID] == record && record.activeGeneration != nil && *record.activeGeneration == actionable && neoOrbLifecycleActionable(record.activeGeneration) && record.prelaunchReady && record.preparedOwnerID == ownerID && record.suppressSharedGitHub == credentialSelection.suppressSharedGitHub()
	if published {
		record.state = neoOrbStateRunning
		record.failReason = ""
		record.idleSince = time.Time{}
	}
	m.mu.Unlock()
	operation.close()
	if !published {
		fail("publication", "Cannot safely activate the orb.", errNeoOrbActiveLifecycleGuardRejected)
		return
	}
	if actionable.MultiplayerTTLSeconds != 0 {
		actor.setThreadOpen(actionable.MultiplayerTTLSeconds)
	}
	actor.broadcastExecutorStatus(spawnID, "running", "Waiting for the orb executor to connect.", map[string]any{"reasonCode": "waiting_for_executor_connect", "threadId": threadID})
	watcherStarted = m.startWorker(func(ctx context.Context) {
		m.watchOrbConnect(ctx, actor, record, spawnID, neoExecutorConnectTimeout(cfg))
	})
	if !watcherStarted {
		m.setState(record, neoOrbStateConflict, "orb executor watcher could not start")
		actor.broadcastExecutorStatus(spawnID, "failed", "Cannot monitor the orb executor while the runtime is stopping.", map[string]any{"reasonCode": "spawn_failed", "threadId": threadID})
	}
}

func (m *neoOrbManager) provisionOrb(ctx context.Context, a *neoActor, record *neoOrbRecord, spawnID, repositoryURL, agentMode, reasoningEffort string) {
	threadID, runnerID, exact := m.orbLaunchIdentity(record, spawnID)
	if !exact {
		return
	}
	cfg := m.runtime.configSnapshot()
	watcherStarted := false
	defer func() {
		if !watcherStarted {
			a.clearWebLocalExecutorReservation(spawnID, runnerID)
		}
	}()
	client, clientErr := m.dockerClient(cfg)
	fail := func(stage, message string, err error) {
		m.mu.Lock()
		containerID := record.containerID
		m.mu.Unlock()
		removed := false
		if containerID != "" && ctx.Err() == nil {
			cleanupCtx, cleanupCancel := context.WithTimeout(ctx, 30*time.Second)
			cleanupErr := client.RemoveContainer(cleanupCtx, containerID, true)
			cleanupCancel()
			if cleanupErr != nil {
				log.Warnf("amp orbs: cleanup thread=%s container=%s failed: %v", threadID, containerID, cleanupErr)
			} else {
				removed = true
			}
		}
		m.mu.Lock()
		if removed && record.containerID == containerID {
			record.containerID = ""
		}
		record.state = neoOrbStateFailed
		record.failReason = message
		m.mu.Unlock()
		if removed {
			m.clearContainerBinding(a, containerID)
		}
		log.Warnf("amp orbs: provision thread=%s stage=%s failed: %v", threadID, stage, err)
		a.broadcastExecutorStatus(spawnID, "failed", message+": "+err.Error(), map[string]any{"reasonCode": "spawn_failed", "threadId": threadID})
	}

	if clientErr != nil {
		fail("connect", "Cannot reach the Docker host", clientErr)
		return
	}
	pingCtx, pingCancel := context.WithTimeout(ctx, 15*time.Second)
	if err := client.Ping(pingCtx); err != nil {
		pingCancel()
		fail("connect", "Cannot reach the Docker host", err)
		return
	}
	pingCancel()

	setupCtx, setupCancel := context.WithTimeout(ctx, neoOrbSetupTimeout(cfg))
	defer setupCancel()

	image := neoOrbImage(cfg)
	if err := client.EnsureImage(setupCtx, image); err != nil {
		fail("image", "Cannot prepare the orb image", err)
		return
	}

	orbs := neoOrbsConfig(cfg)
	network, err := neoOrbNetworkName(orbs.Network)
	if err != nil {
		fail("config", "Invalid orb configuration", err)
		return
	}
	containerID, err := client.CreateContainer(setupCtx, neoOrbContainerSpec{
		Name:        "cliproxy-orb-" + strings.ToLower(threadID),
		Image:       image,
		Cmd:         []string{"sleep", "infinity"},
		NanoCPUs:    orbs.NanoCPUs,
		MemoryMB:    orbs.MemoryMB,
		ExtraHosts:  []string{"host.docker.internal:host-gateway"},
		Labels:      map[string]string{"cliproxy.orb": threadID, neoOrbPortalTokenLabel: record.portalToken},
		NetworkMode: network,
	})
	if err != nil {
		fail("create", "Cannot create the orb container", err)
		return
	}
	m.mu.Lock()
	record.containerID = containerID
	record.repositoryURL = repositoryURL
	m.mu.Unlock()
	if err := m.persistContainerBinding(a, containerID, record.portalToken); err != nil {
		fail("persist", "Cannot persist the orb container binding", err)
		return
	}
	if err := client.StartContainer(setupCtx, containerID); err != nil {
		fail("start", "Cannot start the orb container", err)
		return
	}

	if err := m.orbBootstrapTools(setupCtx, client, containerID); err != nil {
		fail("tools", "Cannot prepare orb tooling", err)
		return
	}
	if err := m.orbConfigureAgentBrowser(setupCtx, client, containerID, threadID, true); err != nil {
		fail("browser", "Cannot prepare the isolated orb browser", err)
		return
	}
	if err := m.orbInstallExecutor(setupCtx, cfg, client, containerID); err != nil {
		fail("executor", "Cannot install the Amp executor in the orb", err)
		return
	}
	if err := m.orbConfigurePortalHelper(setupCtx, client, containerID); err != nil {
		fail("portal", "Cannot prepare orb portals", err)
		return
	}
	credentialSelection, err := m.orbReconcileOwnerCredentials(setupCtx, client, containerID, a.threadToolOwnerID())
	if err != nil {
		fail("credentials", "Cannot apply owner orb credentials", err)
		return
	}
	if !credentialSelection.suppressSharedGitHub() && neoOrbGitHubToken(cfg) != "" {
		if err := m.orbConfigureGitHubAuth(setupCtx, cfg, client, containerID); err != nil {
			fail("git-auth", "Cannot configure GitHub access in the orb", err)
			return
		}
	}
	hasOwnerConfig, err := m.orbReconcileOwnerConfig(setupCtx, cfg, client, containerID, a.threadToolOwnerID())
	if err != nil {
		fail("config", "Cannot apply owner orb configuration", err)
		return
	}
	if !hasOwnerConfig && neoOrbSyncLocalConfigEnabled(cfg) {
		m.orbSyncLocalConfig(setupCtx, client, containerID)
	}
	a.broadcastExecutorStatus(spawnID, "starting", "Configuring orb workspace.", map[string]any{"reasonCode": "configuring_workspace", "threadId": threadID})
	if err := m.orbConfigureWorkspace(setupCtx, client, containerID, repositoryURL, record.workDir); err != nil {
		fail("workspace", "Cannot configure the orb workspace", err)
		return
	}

	env := neoOrbExecutorEnvWithCredentials(cfg, threadID, record.workDir, record.portalToken, credentialSelection.suppressSharedGitHub())
	args := neoHeadlessExecutorArgs(threadID, agentMode, reasoningEffort, neoOrbExecutorLog, runnerID)
	cmd := neoOrbHeadlessCommand(args)
	a.broadcastExecutorStatus(spawnID, "starting", "Starting the headless executor in the orb.", map[string]any{"reasonCode": "starting_headless", "threadId": threadID})
	if err := client.ExecDetached(setupCtx, containerID, cmd, env, record.workDir); err != nil {
		fail("headless", "Cannot start the headless executor in the orb", err)
		return
	}
	m.setState(record, neoOrbStateRunning, "")
	a.broadcastExecutorStatus(spawnID, "running", "Waiting for the orb executor to connect.", map[string]any{"reasonCode": "waiting_for_executor_connect", "threadId": threadID})
	watcherStarted = m.startWorker(func(ctx context.Context) { m.watchOrbConnect(ctx, a, record, spawnID, neoExecutorConnectTimeout(cfg)) })
	if !watcherStarted {
		m.setState(record, neoOrbStateFailed, "orb executor watcher could not start")
		a.broadcastExecutorStatus(spawnID, "failed", "Cannot monitor the orb executor while the runtime is stopping.", map[string]any{"reasonCode": "spawn_failed", "threadId": threadID})
	}
}

func (m *neoOrbManager) persistContainerBinding(a *neoActor, containerID, portalToken string) error {
	if a == nil || strings.TrimSpace(containerID) == "" || !neoOrbPortalTokenValid(portalToken) {
		return fmt.Errorf("orb container binding is unavailable")
	}
	a.mu.Lock()
	if a.meta == nil {
		a.meta = map[string]any{}
	}
	previousBinding, hadPreviousBinding := a.meta[neoOrbContainerIDMetaKey]
	previousPortalToken, hadPreviousPortalToken := a.meta[neoOrbPortalTokenMetaKey]
	a.meta[neoOrbContainerIDMetaKey] = containerID
	a.meta[neoOrbPortalTokenMetaKey] = portalToken
	a.measurements.revision++
	if a.localThreadSnapshotsEnabled() {
		a.measurements.localRequestedRevision = a.measurements.revision
	}
	a.workerRevision = nil
	a.mu.Unlock()
	if a.syncLocalThreadSnapshotForShutdownNow() {
		return nil
	}
	a.mu.Lock()
	if strings.TrimSpace(stringValue(a.meta[neoOrbContainerIDMetaKey])) == containerID && strings.TrimSpace(stringValue(a.meta[neoOrbPortalTokenMetaKey])) == portalToken {
		if hadPreviousBinding {
			a.meta[neoOrbContainerIDMetaKey] = previousBinding
		} else {
			delete(a.meta, neoOrbContainerIDMetaKey)
		}
		if hadPreviousPortalToken {
			a.meta[neoOrbPortalTokenMetaKey] = previousPortalToken
		} else {
			delete(a.meta, neoOrbPortalTokenMetaKey)
		}
		a.measurements.revision++
		if a.localThreadSnapshotsEnabled() {
			a.measurements.localRequestedRevision = a.measurements.revision
		}
		a.workerRevision = nil
	}
	a.mu.Unlock()
	if !a.syncLocalThreadSnapshotForShutdownNow() {
		log.Warnf("amp orbs: thread=%s container binding rollback snapshot failed", a.threadID)
	}
	return fmt.Errorf("thread snapshot is unavailable")
}

func (m *neoOrbManager) persistLifecycleContainerBinding(a *neoActor, generation neoOrbLifecycleGeneration) error {
	if a == nil || !neoOrbLifecycleGenerationHasActivation(generation) || generation.AuthenticatedOwnerID == "" || generation.Revision == 0 || !neoOrbPortalTokenValid(generation.PortalToken) {
		return errors.New("orb lifecycle container binding is unavailable")
	}
	ownerID, exact := m.activeLifecycleActorOwner(a, generation.ThreadID)
	if !exact || ownerID != generation.AuthenticatedOwnerID {
		return errNeoOrbActiveLifecycleGuardRejected
	}
	a.mu.Lock()
	if a.meta == nil {
		a.meta = map[string]any{}
	}
	previousContainer, hadContainer := a.meta[neoOrbContainerIDMetaKey]
	previousPortal, hadPortal := a.meta[neoOrbPortalTokenMetaKey]
	previousRevision, hadRevision := a.meta[neoOrbLifecycleRevisionMetaKey]
	a.meta[neoOrbContainerIDMetaKey] = generation.ContainerID
	a.meta[neoOrbPortalTokenMetaKey] = generation.PortalToken
	a.meta[neoOrbLifecycleRevisionMetaKey] = generation.Revision
	a.measurements.revision++
	if a.localThreadSnapshotsEnabled() {
		a.measurements.localRequestedRevision = a.measurements.revision
	}
	a.workerRevision = nil
	a.mu.Unlock()
	if a.syncLocalThreadSnapshotForShutdownNow() && m.activeLifecycleActorBindingExact(generation.ThreadID, &generation) {
		return nil
	}
	a.mu.Lock()
	currentRevision, revisionExact := neoOrbLifecycleRevision(a.meta[neoOrbLifecycleRevisionMetaKey])
	if strings.TrimSpace(stringValue(a.meta[neoOrbContainerIDMetaKey])) == generation.ContainerID && strings.TrimSpace(stringValue(a.meta[neoOrbPortalTokenMetaKey])) == generation.PortalToken && revisionExact && currentRevision == generation.Revision {
		if hadContainer {
			a.meta[neoOrbContainerIDMetaKey] = previousContainer
		} else {
			delete(a.meta, neoOrbContainerIDMetaKey)
		}
		if hadPortal {
			a.meta[neoOrbPortalTokenMetaKey] = previousPortal
		} else {
			delete(a.meta, neoOrbPortalTokenMetaKey)
		}
		if hadRevision {
			a.meta[neoOrbLifecycleRevisionMetaKey] = previousRevision
		} else {
			delete(a.meta, neoOrbLifecycleRevisionMetaKey)
		}
		a.measurements.revision++
		if a.localThreadSnapshotsEnabled() {
			a.measurements.localRequestedRevision = a.measurements.revision
		}
		a.workerRevision = nil
	}
	a.mu.Unlock()
	if !a.syncLocalThreadSnapshotForShutdownNow() {
		log.Warnf("amp orbs: lifecycle container binding rollback snapshot failed")
	}
	return errors.New("thread snapshot is unavailable")
}

func (m *neoOrbManager) clearContainerBinding(a *neoActor, containerID string) {
	if a == nil || strings.TrimSpace(containerID) == "" {
		return
	}
	a.mu.Lock()
	if strings.TrimSpace(stringValue(a.meta[neoOrbContainerIDMetaKey])) != containerID {
		a.mu.Unlock()
		return
	}
	delete(a.meta, neoOrbContainerIDMetaKey)
	delete(a.meta, neoOrbPortalTokenMetaKey)
	delete(a.meta, neoOrbLifecycleRevisionMetaKey)
	a.measurements.revision++
	if a.localThreadSnapshotsEnabled() {
		a.measurements.localRequestedRevision = a.measurements.revision
	}
	a.workerRevision = nil
	a.mu.Unlock()
	if !a.syncLocalThreadSnapshotForShutdownNow() {
		log.Warnf("amp orbs: thread=%s cleared container binding snapshot failed", a.threadID)
	}
}

func (m *neoOrbManager) orbBootstrapTools(ctx context.Context, client neoOrbProviderClient, containerID string) error {
	architecture, err := client.Exec(ctx, containerID, []string{"dpkg", "--print-architecture"}, nil, "/")
	if err != nil {
		return err
	}
	if architecture.ExitCode != 0 {
		return fmt.Errorf("architecture probe exited %d: %s", architecture.ExitCode, clipNeoErrorBody([]byte(architecture.Stderr)))
	}
	arch := strings.TrimSpace(architecture.Stdout)
	installScript, err := neoOrbToolchainInstallScript(arch)
	if err != nil {
		return err
	}
	install, err := client.Exec(ctx, containerID, []string{"/bin/sh", "-lc", installScript}, nil, "/")
	if err != nil {
		return err
	}
	if install.ExitCode != 0 {
		return fmt.Errorf("orb toolchain install exited %d: %s", install.ExitCode, clipNeoErrorBody([]byte(install.Stderr)))
	}
	return nil
}

func neoOrbToolchainInstallScript(arch string) (string, error) {
	var nodeArch, nodeSHA, bunArch, bunSHA, agentBrowserSHA, browserReady, browserInstall string
	switch arch {
	case "amd64":
		nodeArch = "x64"
		nodeSHA = "14b342e71204f811bde6153be8e04b62aef63c236fef92b55f9c83154b409647"
		bunArch = "x64"
		bunSHA = "951ee2aee855f08595aeec6225226a298d3fea83a3dcd6465c09cbccdf7e848f"
		agentBrowserSHA = "b7bc3dfcf0a7326c1f5a60423163259ba2349eebfa5bd2e70e111af743da4a49"
		browserReady = fmt.Sprintf(`test -x "/opt/chrome-for-testing-%[1]s/chrome-linux64/chrome" &&
  test "$(readlink /usr/local/libexec/cliproxy-browser)" = "/opt/chrome-for-testing-%[1]s/chrome-linux64/chrome" &&
  test "$(readlink /usr/bin/google-chrome)" = "/opt/chrome-for-testing-%[1]s/chrome-linux64/chrome"`, neoOrbChromeVersion)
		browserInstall = fmt.Sprintf(`
mkdir -p /usr/local/libexec
download_verify "https://storage.googleapis.com/chrome-for-testing-public/%[1]s/linux64/chrome-linux64.zip" "60a324a6e1d27b20f2035a2cdaf71641a739fe1f5571f63794773225820bce8a" "$tmp/chrome.zip"
rm -rf "/opt/chrome-for-testing-%[1]s"
mkdir -p "/opt/chrome-for-testing-%[1]s"
unzip -q "$tmp/chrome.zip" -d "/opt/chrome-for-testing-%[1]s"
ln -sfn "/opt/chrome-for-testing-%[1]s/chrome-linux64/chrome" /usr/local/libexec/cliproxy-browser
ln -sfn "/opt/chrome-for-testing-%[1]s/chrome-linux64/chrome" /usr/bin/google-chrome
`, neoOrbChromeVersion)
	case "arm64":
		nodeArch = "arm64"
		nodeSHA = "01443c1e1a29e531ccad5a46fefa6df490d2189c49f7955904aecdbb0fe86fdc"
		bunArch = "aarch64"
		bunSHA = "a27ffb63a8310375836e0d6f668ae17fa8d8d18b88c37c821c65331973a19a3b"
		agentBrowserSHA = "6ccaba1eb26a0e6f5c23c59d2c63e6e0237fde82713cfdb543ba506490cac9c1"
		browserReady = `test -x /usr/bin/chromium &&
  test "$(readlink /usr/local/libexec/cliproxy-browser)" = /usr/bin/chromium`
		browserInstall = "mkdir -p /usr/local/libexec\nln -sfn /usr/bin/chromium /usr/local/libexec/cliproxy-browser\n"
	default:
		return "", fmt.Errorf("orb architecture %q is unsupported; expected amd64 or arm64", arch)
	}
	chromiumPackage := ""
	if arch == "arm64" {
		chromiumPackage = " chromium"
	}
	return fmt.Sprintf(`set -eu
umask 022
tools_ready=true
for tool in git git-lfs gh curl tmux supervisord supervisorctl rg fd jq unzip zip xz ssh rsync ps lsof nc dig ping sqlite3 python3 pip3; do
  command -v "$tool" >/dev/null || tools_ready=false
done
if "$tools_ready" &&
  test "$(node --version 2>/dev/null)" = "v%[2]s" &&
  test "$(pnpm --version 2>/dev/null)" = "%[5]s" &&
  test "$(bun --version 2>/dev/null)" = "%[6]s" &&
  test "$(/usr/local/libexec/agent-browser-native --version 2>/dev/null)" = "agent-browser %[9]s" &&
  test "$(readlink /usr/local/libexec/agent-browser-native)" = "/opt/agent-browser-%[9]s/agent-browser-linux-%[11]s" &&
  %[13]s; then
  exit 0
fi
rm -f %[14]s
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq git git-lfs gh curl ca-certificates tmux supervisor ripgrep fd-find jq unzip zip xz-utils openssh-client rsync procps lsof netcat-openbsd dnsutils iputils-ping sqlite3 less file util-linux build-essential python3 python3-pip python3-venv pkg-config fonts-liberation libasound2 libatk-bridge2.0-0 libatk1.0-0 libcups2 libdbus-1-3 libdrm2 libgbm1 libglib2.0-0 libgtk-3-0 libnspr4 libnss3 libpango-1.0-0 libx11-6 libx11-xcb1 libxcb1 libxcomposite1 libxdamage1 libxext6 libxfixes3 libxkbcommon0 libxrandr2 xdg-utils%[1]s >/dev/null
apt-get clean
rm -rf /var/lib/apt/lists/*
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
download_verify() {
  curl -fL --retry 3 --output "$3" "$1"
  printf '%%s  %%s\n' "$2" "$3" | sha256sum -c - >/dev/null
}
download_verify "https://nodejs.org/dist/v%[2]s/node-v%[2]s-linux-%[3]s.tar.xz" "%[4]s" "$tmp/node.tar.xz"
rm -rf "/opt/node-v%[2]s"
mkdir -p "/opt/node-v%[2]s"
tar -xJf "$tmp/node.tar.xz" --strip-components=1 -C "/opt/node-v%[2]s"
for tool in node npm npx corepack; do ln -sfn "/opt/node-v%[2]s/bin/$tool" "/usr/local/bin/$tool"; done
ln -sfn /usr/bin/fdfind /usr/local/bin/fd
download_verify "https://registry.npmjs.org/pnpm/-/pnpm-%[5]s.tgz" "34e198cb1e43237517ecedfd31f9ae26a6c0a3e5366ce58a2d05f4b21fb5f19a" "$tmp/pnpm.tgz"
rm -rf "/opt/pnpm-%[5]s"
mkdir -p "/opt/pnpm-%[5]s"
tar -xzf "$tmp/pnpm.tgz" --strip-components=1 -C "/opt/pnpm-%[5]s"
ln -sfn "/opt/pnpm-%[5]s/bin/pnpm.mjs" /usr/local/bin/pnpm
ln -sfn "/opt/pnpm-%[5]s/bin/pnpx.mjs" /usr/local/bin/pnpx
download_verify "https://github.com/oven-sh/bun/releases/download/bun-v%[6]s/bun-linux-%[7]s.zip" "%[8]s" "$tmp/bun.zip"
unzip -q "$tmp/bun.zip" -d "$tmp/bun"
install -m 0755 "$tmp/bun/bun-linux-%[7]s/bun" /usr/local/bin/bun
download_verify "https://github.com/vercel-labs/agent-browser/releases/download/v%[9]s/agent-browser-linux-%[11]s" "%[10]s" "$tmp/agent-browser"
rm -rf "/opt/agent-browser-%[9]s"
mkdir -p "/opt/agent-browser-%[9]s"
install -m 0755 "$tmp/agent-browser" "/opt/agent-browser-%[9]s/agent-browser-linux-%[11]s"
mkdir -p /usr/local/libexec /home/user
ln -sfn "/opt/agent-browser-%[9]s/agent-browser-linux-%[11]s" /usr/local/libexec/agent-browser-native
%[12]s
test "$(node --version)" = "v%[2]s"
test "$(pnpm --version)" = "%[5]s"
test "$(bun --version)" = "%[6]s"
test "$(/usr/local/libexec/agent-browser-native --version)" = "agent-browser %[9]s"
`, chromiumPackage, neoOrbNodeVersion, nodeArch, nodeSHA, neoOrbPNPMVersion, neoOrbBunVersion, bunArch, bunSHA, neoOrbAgentBrowserVersion, agentBrowserSHA, nodeArch, browserInstall, browserReady, shellQuoteNeoOrb(neoOrbAgentBrowserSmokeMarker())), nil
}

func neoOrbBrowserNamespace(threadID string) string {
	digest := sha256.Sum256([]byte(threadID))
	return fmt.Sprintf("orb-%x", digest[:12])
}

func neoOrbAgentBrowserSmokeMarker() string {
	return fmt.Sprintf("/opt/cliproxy-orb-browser-smoke/%s-%s", neoOrbAgentBrowserVersion, neoOrbChromeVersion)
}

func (m *neoOrbManager) orbConfigureAgentBrowser(ctx context.Context, client neoOrbProviderClient, containerID, threadID string, allowPrewarmAttestation bool) error {
	namespace := neoOrbBrowserNamespace(threadID)
	wrapper := fmt.Sprintf(`#!/bin/sh
set -eu
umask 077
export HOME=/home/user
export XDG_RUNTIME_DIR=%[1]s
export AGENT_BROWSER_SOCKET_DIR=%[1]s
export AGENT_BROWSER_NAMESPACE=%[2]s
export AGENT_BROWSER_EXECUTABLE_PATH=/usr/local/libexec/cliproxy-browser
export AGENT_BROWSER_ARGS=--no-sandbox,--disable-dev-shm-usage
mkdir -p "$XDG_RUNTIME_DIR" "$HOME/.agent-browser"
chmod 0700 "$XDG_RUNTIME_DIR" "$HOME/.agent-browser"
exec /usr/local/libexec/agent-browser-native "$@"
`, neoOrbAgentBrowserSocketDir, namespace)
	if err := client.CopyFileToContainer(ctx, containerID, neoOrbAgentBrowserPath, []byte(wrapper), 0o755); err != nil {
		return err
	}
	markerAction := "rm -f " + shellQuoteNeoOrb(neoOrbAgentBrowserSmokeMarker())
	if allowPrewarmAttestation {
		markerAction = fmt.Sprintf("if test -f %[1]s; then\n  rm -f %[1]s\n  exit 0\nfi", shellQuoteNeoOrb(neoOrbAgentBrowserSmokeMarker()))
	}
	smoke := fmt.Sprintf(`set -eu
fail() { printf 'agent-browser smoke failed at %%s\n' "$1" >&2; exit 1; }
output="$(agent-browser doctor --offline --quick 2>&1)" || { printf '%%s\n' "$output" >&2; fail doctor; }
%[1]s
trap 'agent-browser close --all >/dev/null 2>&1 || true' EXIT
output="$(agent-browser open 'data:text/html,<title>cliproxy-orb-browser-smoke</title><button>ready</button>' 2>&1)" || { printf '%%s\n' "$output" >&2; fail open; }
title="$(agent-browser get title 2>&1)" || { printf '%%s\n' "$title" >&2; fail title; }
test "$title" = cliproxy-orb-browser-smoke || { printf 'unexpected browser title: %%s\n' "$title" >&2; fail title; }
snapshot="$(agent-browser snapshot -i 2>&1)" || { printf '%%s\n' "$snapshot" >&2; fail snapshot; }
printf '%%s\n' "$snapshot" | grep -F button >/dev/null || { printf '%%s\n' "$snapshot" >&2; fail snapshot; }
agent-browser close --all >/dev/null 2>&1 || fail close
trap - EXIT`, markerAction)
	result, err := client.Exec(ctx, containerID, []string{"timeout", "90", "/bin/sh", "-lc", smoke}, nil, "/")
	if err != nil {
		return err
	}
	if result.ExitCode != 0 {
		return fmt.Errorf("agent-browser smoke test exited %d: %s", result.ExitCode, clipNeoErrorBody([]byte(result.Stderr)))
	}
	return nil
}

func (m *neoOrbManager) orbInstallExecutor(ctx context.Context, cfg *config.Config, client neoOrbProviderClient, containerID string) error {
	override := strings.TrimSpace(neoOrbsConfig(cfg).ExecutorCommand)
	if strings.HasPrefix(override, "http://") {
		return fmt.Errorf("executor-command download URLs must use https")
	}
	if strings.HasPrefix(override, "https://") {
		return m.orbDownloadExecutor(ctx, client, containerID, override)
	}
	if override != "" {
		binary, err := os.ReadFile(override)
		if err != nil {
			return fmt.Errorf("read executor binary %s: %w", override, err)
		}
		if !neoOrbBinaryIsELF(binary) {
			return fmt.Errorf("configured executor %s is not a Linux binary; orbs require a Linux ELF or an https download URL", override)
		}
		return client.CopyFileToContainer(ctx, containerID, neoOrbBinaryPath, binary, 0o755)
	}
	if resolved, err := neoAmpExecutorCommand(cfg); err == nil && runtime.GOOS == "linux" {
		if binary, errRead := os.ReadFile(resolved); errRead == nil && neoOrbBinaryIsELF(binary) {
			return client.CopyFileToContainer(ctx, containerID, neoOrbBinaryPath, binary, 0o755)
		}
	}
	installScript := "export HOME=/root; curl -fsSL https://ampcode.com/install.sh | bash && AMPBIN=\"$(command -v amp || true)\" && AMPBIN=\"${AMPBIN:-/root/.amp/bin/amp}\" && if [ \"$AMPBIN\" != " + neoOrbBinaryPath + " ]; then cp \"$AMPBIN\" " + neoOrbBinaryPath + "; fi && chmod 0755 " + neoOrbBinaryPath
	install, err := client.Exec(ctx, containerID, []string{"/bin/sh", "-lc", installScript}, nil, "/")
	if err != nil {
		return err
	}
	if install.ExitCode != 0 {
		return fmt.Errorf("executor install inside the orb exited %d: %s (set orbs.executor-command to a linux binary URL to override)", install.ExitCode, clipNeoErrorBody([]byte(install.Stderr)))
	}
	return nil
}

func (m *neoOrbManager) orbDownloadExecutor(ctx context.Context, client neoOrbProviderClient, containerID, url string) error {
	download, err := client.Exec(ctx, containerID, []string{"/bin/sh", "-lc", "curl -fsSL -o " + neoOrbBinaryPath + " " + shellQuoteNeoOrb(url) + " && chmod 0755 " + neoOrbBinaryPath}, nil, "/")
	if err != nil {
		return err
	}
	if download.ExitCode != 0 {
		return fmt.Errorf("executor download exited %d: %s", download.ExitCode, clipNeoErrorBody([]byte(download.Stderr)))
	}
	return nil
}

// neoOrbBinaryIsELF reports whether the binary is a Linux executable. Host
// binaries in other formats (e.g. macOS Mach-O) cannot run in the orb's Linux
// container and must be installed inside it instead.
func neoOrbBinaryIsELF(binary []byte) bool {
	return len(binary) > 4 && binary[0] == 0x7f && binary[1] == 'E' && binary[2] == 'L' && binary[3] == 'F'
}

func (m *neoOrbManager) orbConfigureGitHubAuth(ctx context.Context, cfg *config.Config, client neoOrbProviderClient, containerID string) error {
	token := neoOrbGitHubToken(cfg)
	if token == "" {
		return nil
	}
	prepared, err := client.Exec(ctx, containerID, []string{"/bin/sh", "-lc", "umask 077; mkdir -p /root/.config/gh; chmod 0700 /root/.config/gh"}, nil, "/")
	if err != nil {
		return err
	}
	if prepared.ExitCode != 0 {
		return fmt.Errorf("GitHub credential directory creation exited %d", prepared.ExitCode)
	}
	if err := client.CopyFileToContainer(ctx, containerID, "/root/.config/gh/hosts.yml", neoOrbGitHubHosts(token), 0o600); err != nil {
		return fmt.Errorf("GitHub credential staging failed")
	}
	return m.orbConfigureOwnerGitHubAuth(ctx, client, containerID, neoOrbCredentialSelection{HasGitHub: true})
}

// orbSyncLocalConfig copies the user's non-secret Amp configuration into the
// orb so the executor behaves like the local setup: settings, review checks,
// and skills. Auth material and credential-shaped files are never copied.
// Missing local files are fine; sync failures are logged, not fatal.
func (m *neoOrbManager) orbSyncLocalConfig(ctx context.Context, client neoOrbProviderClient, containerID string) {
	home, err := os.UserHomeDir()
	if err != nil || strings.TrimSpace(home) == "" {
		return
	}
	if _, err := client.Exec(ctx, containerID, []string{"mkdir", "-p", "/root/.config/amp", "/root/.config/agents"}, nil, "/"); err != nil {
		log.Warnf("amp orbs: config sync mkdir failed: %v", err)
		return
	}
	settingsPath := filepath.Join(home, ".config", "amp", "settings.json")
	if settings, errRead := os.ReadFile(settingsPath); errRead == nil {
		settings = neoOrbSafeSettings(settings)
		if errCopy := client.CopyFileToContainer(ctx, containerID, "/root/.config/amp/settings.json", settings, 0o644); errCopy != nil {
			log.Warnf("amp orbs: settings sync failed: %v", errCopy)
		}
	}
	agentsMD := filepath.Join(home, ".config", "AGENTS.md")
	if content, errRead := os.ReadFile(agentsMD); errRead == nil && len(content) <= 4<<20 {
		if errCopy := client.CopyFileToContainer(ctx, containerID, "/root/.config/AGENTS.md", content, 0o644); errCopy != nil {
			log.Warnf("amp orbs: AGENTS.md sync failed: %v", errCopy)
		}
	}
	budget := neoOrbSyncMaxBytes
	for _, dir := range []string{"checks", "skills"} {
		source := filepath.Join(home, ".config", "agents", dir)
		m.orbSyncDirectory(ctx, client, containerID, source, "/root/.config/agents/"+dir, &budget)
	}
}

func neoOrbSafeSettings(raw []byte) []byte {
	var settings map[string]any
	if len(raw) > 4<<20 || json.Unmarshal(raw, &settings) != nil {
		return []byte("{}\n")
	}
	allowed := map[string]bool{
		"amp.terminal.theme":               true,
		"amp.git.commit.coauthor.enabled":  true,
		"amp.git.commit.ampThread.enabled": true,
		"amp.showCosts":                    true,
		"amp.gauge":                        true,
	}
	safe := make(map[string]any, len(allowed))
	for key, value := range settings {
		if allowed[key] {
			safe[key] = value
		}
	}
	encoded, err := json.Marshal(safe)
	if err != nil {
		return []byte("{}\n")
	}
	return append(encoded, '\n')
}

func (m *neoOrbManager) orbSyncDirectory(ctx context.Context, client neoOrbProviderClient, containerID, sourceDir, destDir string, budget *int) {
	entries, err := os.ReadDir(sourceDir)
	if err != nil {
		return
	}
	files := make(map[string][]byte, len(entries))
	for _, entry := range entries {
		if ctx.Err() != nil {
			return
		}
		name := entry.Name()
		if (destDir == "/root/.config/agents/skills" || strings.HasPrefix(destDir, "/root/.config/agents/skills/")) && name == "mcp.json" {
			continue
		}
		if neoOrbSecretLikeName(name) {
			continue
		}
		sourcePath := filepath.Join(sourceDir, name)
		if entry.IsDir() {
			if destDir == "/root/.config/agents/skills" && !neoOrbSkillSyncAllowed(sourcePath, name) {
				continue
			}
			m.orbSyncDirectory(ctx, client, containerID, sourcePath, destDir+"/"+name, budget)
			continue
		}
		if !entry.Type().IsRegular() {
			continue
		}
		content, errRead := os.ReadFile(sourcePath)
		if errRead != nil || len(content) > 4<<20 {
			continue
		}
		if len(content) > *budget {
			*budget = 0
			log.Warnf("amp orbs: sync to %s truncated at the size limit", destDir)
			break
		}
		files[name] = content
		*budget -= len(content)
	}
	if len(files) == 0 {
		return
	}
	if _, errExec := client.Exec(ctx, containerID, []string{"mkdir", "-p", destDir}, nil, "/"); errExec != nil {
		return
	}
	if errCopy := client.CopyTarToContainer(ctx, containerID, destDir, files, 0o644); errCopy != nil {
		log.Warnf("amp orbs: sync to %s failed: %v", destDir, errCopy)
	}
}

func neoOrbSkillSyncAllowed(sourceDir, name string) bool {
	if name == "using-open-browser-use" {
		return false
	}
	// Lstat intentionally does not follow links: an mcp.json of any type,
	// including a symlink, marks the skill as MCP-backed and blocks the sync.
	if _, err := os.Lstat(filepath.Join(sourceDir, "mcp.json")); err == nil {
		return false
	}
	skillPath := filepath.Join(sourceDir, "SKILL.md")
	info, err := os.Lstat(skillPath)
	if err != nil || !info.Mode().IsRegular() || info.Size() > 4<<20 {
		return false
	}
	content, err := os.ReadFile(skillPath)
	if err != nil {
		return false
	}
	lines := strings.Split(strings.ReplaceAll(string(content), "\r\n", "\n"), "\n")
	if len(lines) == 0 || lines[0] != "---" {
		return true
	}
	frontmatterEnd := -1
	for index, line := range lines[1:] {
		if line == "---" {
			frontmatterEnd = index + 1
			break
		}
	}
	if frontmatterEnd < 0 {
		return false
	}
	frontmatter := map[string]any{}
	if err := yaml.Unmarshal([]byte(strings.Join(lines[1:frontmatterEnd], "\n")), &frontmatter); err != nil {
		return false
	}
	if _, hasMCPServers := frontmatter["mcpServers"]; hasMCPServers {
		return false
	}
	return true
}

func neoOrbSecretLikeName(name string) bool {
	lower := strings.ToLower(name)
	for _, suffix := range []string{".pem", ".key", ".token", ".keystore", ".p12", ".pfx"} {
		if strings.HasSuffix(lower, suffix) {
			return true
		}
	}
	base := strings.TrimLeft(lower, ".")
	switch base {
	case "id_rsa", "id_dsa", "id_ecdsa", "id_ed25519":
		return true
	}
	for _, segment := range strings.FieldsFunc(base, func(r rune) bool { return r == '.' || r == '_' || r == '-' }) {
		switch segment {
		case "auth", "token", "tokens", "secret", "secrets", "credential", "credentials", "password", "passwd", "apikey", "key", "env", "envrc":
			return true
		}
	}
	return false
}

func (m *neoOrbManager) orbConfigureWorkspace(ctx context.Context, client neoOrbProviderClient, containerID, repositoryURL, workDir string) error {
	if repositoryURL == "" {
		result, err := client.Exec(ctx, containerID, []string{"mkdir", "-p", workDir}, nil, "/")
		if err != nil {
			return err
		}
		if result.ExitCode != 0 {
			return fmt.Errorf("mkdir exited %d", result.ExitCode)
		}
		return nil
	}
	sanitizedURL, err := neoOrbSanitizeCloneURL(ctx, repositoryURL)
	if err != nil {
		return err
	}
	clone, err := client.Exec(ctx, containerID, []string{"git", "clone", "--depth", "1", sanitizedURL, workDir}, nil, "/")
	if err != nil {
		return err
	}
	if clone.ExitCode != 0 {
		return fmt.Errorf("git clone exited %d: %s", clone.ExitCode, clipNeoErrorBody([]byte(clone.Stderr)))
	}
	setup, err := client.Exec(ctx, containerID, []string{"/bin/sh", "-lc", "if [ -x .agents/setup ]; then mkdir -p /home/user/.cache/amp/logs && .agents/setup > /home/user/.cache/amp/logs/setup.log 2>&1; fi"}, nil, workDir)
	if err != nil {
		return err
	}
	if setup.ExitCode != 0 {
		return fmt.Errorf(".agents/setup exited %d (see setup.log in the orb)", setup.ExitCode)
	}
	return nil
}

func (m *neoOrbManager) resumeLifecycleOrb(ctx context.Context, actor *neoActor, record *neoOrbRecord, spawnID string, recoveryProbe bool) {
	if ctx == nil || m == nil || m.runtime == nil || actor == nil || record == nil {
		return
	}
	threadID, runnerID, exact := m.orbLaunchIdentity(record, spawnID)
	if !exact {
		return
	}
	watcherStarted := false
	defer func() {
		if !watcherStarted {
			actor.clearWebLocalExecutorReservation(spawnID, runnerID)
		}
	}()
	cfg := m.runtime.configSnapshot()
	quarantine := func(message string) {
		m.runtime.orbManagerMu.Lock()
		store := m.runtime.orbLifecycleStore
		m.runtime.orbManagerMu.Unlock()
		var active, pending *neoOrbLifecycleGeneration
		if store != nil {
			snapshot := store.snapshot()
			if lifecycle, exists := snapshot.Threads[threadID]; exists {
				active = cloneNeoOrbLifecycleGeneration(lifecycle.Active)
				pending = cloneNeoOrbLifecycleGeneration(lifecycle.Pending)
			} else if tombstone, exists := snapshot.CleanupTombstones[threadID]; exists {
				active = cloneNeoOrbLifecycleGeneration(tombstone.Active)
				pending = cloneNeoOrbLifecycleGeneration(tombstone.Pending)
			}
		}
		m.mu.Lock()
		if m.orbs[threadID] == record {
			record.state = neoOrbStateConflict
			record.containerID = ""
			record.lifecycleV1 = true
			record.recovered = false
			record.activeGeneration = active
			record.pendingGeneration = pending
			record.prelaunchReady = false
			record.preparedOwnerID = ""
			record.failReason = "lifecycle-v1 resume requires reconciliation"
		}
		m.mu.Unlock()
		actor.broadcastExecutorStatus(spawnID, "failed", message, map[string]any{"reasonCode": "spawn_failed", "threadId": threadID})
	}
	ownerID, actorExact := m.activeLifecycleActorOwner(actor, threadID)
	if !actorExact {
		quarantine("Cannot safely resume the orb.")
		return
	}
	client, err := m.dockerClient(cfg)
	if err != nil {
		quarantine("Cannot reach the orb provider.")
		return
	}
	resumeCtx, cancel := context.WithTimeout(ctx, neoOrbSetupTimeout(cfg))
	defer cancel()
	priorActivation, _, err := m.beginLifecycleActivation(resumeCtx, record, neoOrbLifecycleOperationUnpause)
	if err != nil {
		quarantine("Cannot durably resume the orb.")
		return
	}
	operation, err := m.beginActiveLifecycleOperation(resumeCtx, record, client, cfg)
	if err != nil {
		operationMu := m.orbOperationMutex(record)
		restored := false
		if errLock := neoOrbLockOperation(resumeCtx, operationMu); errLock == nil {
			m.mu.Lock()
			expected := *record.activeGeneration
			m.mu.Unlock()
			if abortErr := m.abortLifecycleActivationLocked(record, m.runtime.orbLifecycleStore, expected, priorActivation); abortErr == nil {
				m.mu.Lock()
				if m.orbs[threadID] == record {
					record.state = neoOrbStatePaused
					record.failReason = ""
					restored = true
				}
				m.mu.Unlock()
			}
			operationMu.Unlock()
		}
		if restored {
			actor.broadcastExecutorStatus(spawnID, "failed", "Cannot resume the orb.", map[string]any{"reasonCode": "spawn_failed", "threadId": threadID})
			return
		}
		quarantine("Cannot safely resume the orb.")
		return
	}
	guarded := operation.providerClient().(*neoOrbGuardedProvider)
	unpauseResult := guarded.UnpauseContainerInvocation(resumeCtx, operation.containerID)
	if !unpauseResult.confirmed() {
		if !unpauseResult.Invoked {
			m.mu.Lock()
			expected := *record.activeGeneration
			m.mu.Unlock()
			if abortErr := m.abortLifecycleActivationLocked(record, operation.store, expected, priorActivation); abortErr == nil {
				m.mu.Lock()
				if m.orbs[threadID] == record {
					record.state = neoOrbStatePaused
					record.failReason = ""
				}
				m.mu.Unlock()
				operation.close()
				actor.broadcastExecutorStatus(spawnID, "failed", "Cannot resume the orb.", map[string]any{"reasonCode": "spawn_failed", "threadId": threadID})
				return
			}
		}
		operation.close()
		quarantine("Cannot safely resume the orb.")
		return
	}
	if currentOwnerID, exact := m.activeLifecycleActorOwner(actor, threadID); !exact || currentOwnerID != ownerID {
		operation.close()
		quarantine("Cannot safely resume the orb.")
		return
	}
	m.mu.Lock()
	fenceRecoveredExecutor := !m.stopped && m.orbs[threadID] == record && record.state == neoOrbStateProvisioning &&
		record.spawnID == spawnID && record.runnerID == runnerID && record.runnerMigrationRequired && record.runnerMigrationFencing
	m.mu.Unlock()
	if fenceRecoveredExecutor {
		if err := m.orbStopRecoveredExecutor(resumeCtx, guarded, operation.containerID); err != nil {
			operation.close()
			quarantine("Cannot safely stop the recovered orb executor.")
			return
		}
		m.mu.Lock()
		fenced := !m.stopped && m.orbs[threadID] == record && record.state == neoOrbStateProvisioning &&
			record.spawnID == spawnID && record.runnerID == runnerID && record.runnerMigrationRequired && record.runnerMigrationFencing
		if fenced {
			record.runnerMigrationRequired = false
			record.runnerMigrationFencing = false
		}
		m.mu.Unlock()
		if !fenced {
			operation.close()
			quarantine("Cannot safely resume the orb.")
			return
		}
	}
	if recoveryProbe {
		if err := m.orbConfigureAgentBrowser(resumeCtx, guarded, operation.containerID, threadID, false); err != nil {
			operation.close()
			quarantine("Cannot prepare the recovered orb browser.")
			return
		}
	}
	credentialSelection, err := m.orbReconcileOwnerCredentials(resumeCtx, guarded, operation.containerID, ownerID)
	if err != nil {
		operation.close()
		quarantine("Cannot apply owner orb credentials.")
		return
	}
	if !credentialSelection.suppressSharedGitHub() && neoOrbGitHubToken(cfg) != "" {
		if err := m.orbConfigureGitHubAuth(resumeCtx, cfg, guarded, operation.containerID); err != nil {
			operation.close()
			quarantine("Cannot configure GitHub access in the orb.")
			return
		}
	}
	hasOwnerConfig, err := m.orbReconcileOwnerConfig(resumeCtx, cfg, guarded, operation.containerID, ownerID)
	if err != nil {
		operation.close()
		quarantine("Cannot apply owner orb configuration.")
		return
	}
	if !hasOwnerConfig && neoOrbSyncLocalConfigEnabled(cfg) {
		m.orbSyncLocalConfig(resumeCtx, guarded, operation.containerID)
	}
	if err := m.orbConfigurePortalHelper(resumeCtx, guarded, operation.containerID); err != nil {
		operation.close()
		quarantine("Cannot update orb portal services.")
		return
	}
	resumeScript := fmt.Sprintf("if [ -x .agents/resume ]; then mkdir -p /home/user/.cache/amp/logs && timeout %d .agents/resume > /home/user/.cache/amp/logs/resume.log 2>&1 || true; fi", neoOrbResumeHookSeconds)
	if _, err := guarded.Exec(resumeCtx, operation.containerID, []string{"/bin/sh", "-lc", resumeScript}, nil, operation.fence.workDir); err != nil {
		operation.close()
		quarantine("Cannot run the orb resume hook.")
		return
	}
	portalBase := neoOrbPortalBaseURL(cfg)
	if publicURL := strings.TrimRight(strings.TrimSpace(neoOrbsConfig(cfg).PublicURL), "/"); publicURL != "" {
		portalBase = publicURL
	}
	reconcileEnv := []string{
		"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
		"HOME=/home/user",
		"AMP_ORB=1",
		"AMP_THREAD_ID=" + threadID,
		"AMP_ORB_PORTAL_BASE_URL=" + portalBase,
		"AMP_ORB_PORTAL_TOKEN=" + record.portalToken,
	}
	reconcile, err := guarded.Exec(resumeCtx, operation.containerID, []string{neoOrbServiceHelperPath, "reconcile"}, reconcileEnv, operation.fence.workDir)
	if err != nil || reconcile.ExitCode != 0 {
		operation.close()
		quarantine("Cannot reconcile orb portal services.")
		return
	}
	if currentOwnerID, exact := m.activeLifecycleActorOwner(actor, threadID); !exact || currentOwnerID != ownerID {
		operation.close()
		quarantine("Cannot safely resume the orb.")
		return
	}
	operation.close()
	m.mu.Lock()
	if m.orbs[threadID] == record {
		record.suppressSharedGitHub = credentialSelection.suppressSharedGitHub()
		record.prelaunchReady = true
		record.preparedOwnerID = ownerID
	}
	m.mu.Unlock()
	if _, _, err := m.beginLifecycleActivation(resumeCtx, record, neoOrbLifecycleOperationExecDetached); err != nil {
		quarantine("Cannot durably restart the orb executor.")
		return
	}
	operation, err = m.beginActiveLifecycleOperation(resumeCtx, record, client, cfg)
	if err != nil {
		quarantine("Cannot safely restart the orb executor.")
		return
	}
	guarded = operation.providerClient().(*neoOrbGuardedProvider)
	actor.mu.Lock()
	agentMode := actor.agentModeLocked()
	reasoningEffort := actor.reasoningEffortForModeLocked(agentMode)
	actor.mu.Unlock()
	m.mu.Lock()
	portalToken := record.portalToken
	m.mu.Unlock()
	env := neoOrbExecutorEnvWithCredentials(cfg, threadID, operation.fence.workDir, portalToken, credentialSelection.suppressSharedGitHub())
	args := neoHeadlessExecutorArgs(threadID, agentMode, reasoningEffort, neoOrbExecutorLog, runnerID)
	result := guarded.ExecDetachedInvocation(resumeCtx, operation.containerID, neoOrbHeadlessCommand(args), env, operation.fence.workDir)
	if !result.confirmed() {
		operation.close()
		quarantine("Cannot safely restart the orb executor.")
		return
	}
	m.mu.Lock()
	expected := *record.activeGeneration
	m.mu.Unlock()
	actionable, err := m.finishLifecycleActivationLocked(record, operation.store, expected)
	if err != nil {
		operation.close()
		quarantine("Cannot safely activate the resumed orb.")
		return
	}
	bindingExact := m.activeLifecycleActorBindingExact(threadID, &actionable)
	m.mu.Lock()
	published := bindingExact && m.orbs[threadID] == record && record.activeGeneration != nil && *record.activeGeneration == actionable && neoOrbLifecycleActionable(record.activeGeneration) && record.prelaunchReady && record.preparedOwnerID == ownerID && record.suppressSharedGitHub == credentialSelection.suppressSharedGitHub()
	if published {
		record.state = neoOrbStateRunning
		record.containerID = actionable.ContainerID
		record.portalToken = actionable.PortalToken
		record.failReason = ""
		record.idleSince = time.Time{}
	}
	m.mu.Unlock()
	operation.close()
	if !published {
		quarantine("Cannot safely activate the resumed orb.")
		return
	}
	actor.broadcastExecutorStatus(spawnID, "running", "Waiting for the orb executor to reconnect.", map[string]any{"reasonCode": "waiting_for_executor_connect", "threadId": threadID})
	watcherStarted = m.startWorker(func(ctx context.Context) {
		m.watchOrbConnect(ctx, actor, record, spawnID, neoExecutorConnectTimeout(cfg))
	})
	if !watcherStarted {
		m.setState(record, neoOrbStateConflict, "orb executor watcher could not start")
		actor.broadcastExecutorStatus(spawnID, "failed", "Cannot monitor the orb executor while the runtime is stopping.", map[string]any{"reasonCode": "spawn_failed", "threadId": threadID})
	}
}

func (m *neoOrbManager) resumeOrb(ctx context.Context, a *neoActor, record *neoOrbRecord, spawnID string) {
	threadID, runnerID, exact := m.orbLaunchIdentity(record, spawnID)
	if !exact {
		return
	}
	watcherStarted := false
	defer func() {
		if !watcherStarted && a != nil {
			a.clearWebLocalExecutorReservation(spawnID, runnerID)
		}
	}()
	m.mu.Lock()
	blocked := record == nil || record.lifecycleV1 || record.recovered
	m.mu.Unlock()
	if blocked {
		return
	}
	operationMu := m.orbOperationMutex(record)
	if err := neoOrbLockOperation(ctx, operationMu); err != nil {
		return
	}
	defer operationMu.Unlock()
	cfg := m.runtime.configSnapshot()
	client, err := m.dockerClient(cfg)
	if err != nil {
		m.setState(record, neoOrbStateFailed, err.Error())
		a.broadcastExecutorStatus(spawnID, "failed", "Cannot reach the Docker host: "+err.Error(), map[string]any{"reasonCode": "spawn_failed", "threadId": threadID})
		return
	}
	resumeCtrl, resumeCtrlCancel := context.WithTimeout(ctx, 30*time.Second)
	state, err := client.InspectContainer(resumeCtrl, record.containerID)
	resumeCtrlCancel()
	if err != nil {
		m.setState(record, neoOrbStateFailed, err.Error())
		a.broadcastExecutorStatus(spawnID, "failed", "Cannot inspect the orb container: "+err.Error(), map[string]any{"reasonCode": "spawn_failed", "threadId": threadID})
		return
	}
	if !state.Exists {
		m.reprovisionOrb(ctx, a, record, spawnID)
		return
	}
	if state.Paused {
		unpauseCtx, unpauseCancel := context.WithTimeout(ctx, 30*time.Second)
		errUnpause := client.UnpauseContainer(unpauseCtx, record.containerID)
		unpauseCancel()
		if errUnpause != nil {
			m.setState(record, neoOrbStateFailed, errUnpause.Error())
			a.broadcastExecutorStatus(spawnID, "failed", "Cannot resume the orb: "+errUnpause.Error(), map[string]any{"reasonCode": "spawn_failed", "threadId": threadID})
			return
		}
	}
	resumeCtx, resumeCancel := context.WithTimeout(ctx, neoOrbSetupTimeout(cfg))
	defer resumeCancel()
	m.mu.Lock()
	recovered := record.recovered
	m.mu.Unlock()
	if recovered {
		if err := m.prepareRecoveredOrb(resumeCtx, cfg, client, record); err != nil {
			m.setState(record, neoOrbStateFailed, err.Error())
			a.broadcastExecutorStatus(spawnID, "failed", "Cannot update the recovered orb: "+err.Error(), map[string]any{"reasonCode": "spawn_failed", "threadId": threadID})
			return
		}
	}
	credentialSelection, err := m.orbReconcileOwnerCredentials(resumeCtx, client, record.containerID, a.threadToolOwnerID())
	if err != nil {
		m.setState(record, neoOrbStateFailed, "owner credential reconciliation failed")
		a.broadcastExecutorStatus(spawnID, "failed", "Cannot apply owner orb credentials", map[string]any{"reasonCode": "spawn_failed", "threadId": threadID})
		return
	}
	if !credentialSelection.suppressSharedGitHub() && neoOrbGitHubToken(cfg) != "" {
		if err := m.orbConfigureGitHubAuth(resumeCtx, cfg, client, record.containerID); err != nil {
			m.setState(record, neoOrbStateFailed, "GitHub credential configuration failed")
			a.broadcastExecutorStatus(spawnID, "failed", "Cannot configure GitHub access in the orb", map[string]any{"reasonCode": "spawn_failed", "threadId": threadID})
			return
		}
	}
	hasOwnerConfig, err := m.orbReconcileOwnerConfig(resumeCtx, cfg, client, record.containerID, a.threadToolOwnerID())
	if err != nil {
		m.setState(record, neoOrbStateFailed, err.Error())
		a.broadcastExecutorStatus(spawnID, "failed", "Cannot apply owner orb configuration: "+err.Error(), map[string]any{"reasonCode": "spawn_failed", "threadId": threadID})
		return
	}
	if !hasOwnerConfig && recovered && neoOrbSyncLocalConfigEnabled(cfg) {
		m.orbSyncLocalConfig(resumeCtx, client, record.containerID)
	}
	a.mu.Lock()
	agentMode := a.agentModeLocked()
	reasoningEffort := a.reasoningEffortForModeLocked(agentMode)
	a.mu.Unlock()
	env := neoOrbExecutorEnvWithCredentials(cfg, threadID, record.workDir, record.portalToken, credentialSelection.suppressSharedGitHub())
	args := neoHeadlessExecutorArgs(threadID, agentMode, reasoningEffort, neoOrbExecutorLog, runnerID)
	resumeScript := fmt.Sprintf("if [ -x .agents/resume ]; then mkdir -p /home/user/.cache/amp/logs && timeout %d .agents/resume > /home/user/.cache/amp/logs/resume.log 2>&1 || true; fi", neoOrbResumeHookSeconds)
	_, _ = client.Exec(resumeCtx, record.containerID, []string{"/bin/sh", "-lc", resumeScript}, nil, record.workDir)
	if err := client.ExecDetached(resumeCtx, record.containerID, neoOrbHeadlessCommand(args), env, record.workDir); err != nil {
		m.setState(record, neoOrbStateFailed, err.Error())
		a.broadcastExecutorStatus(spawnID, "failed", "Cannot restart the orb executor: "+err.Error(), map[string]any{"reasonCode": "spawn_failed", "threadId": threadID})
		return
	}
	m.setState(record, neoOrbStateRunning, "")
	a.broadcastExecutorStatus(spawnID, "running", "Waiting for the orb executor to reconnect.", map[string]any{"reasonCode": "waiting_for_executor_connect", "threadId": threadID})
	watcherStarted = m.startWorker(func(ctx context.Context) { m.watchOrbConnect(ctx, a, record, spawnID, neoExecutorConnectTimeout(cfg)) })
	if !watcherStarted {
		m.setState(record, neoOrbStateFailed, "orb executor watcher could not start")
		a.broadcastExecutorStatus(spawnID, "failed", "Cannot monitor the orb executor while the runtime is stopping.", map[string]any{"reasonCode": "spawn_failed", "threadId": threadID})
	}
}

func (m *neoOrbManager) prepareRecoveredOrb(ctx context.Context, cfg *config.Config, client neoOrbProviderClient, record *neoOrbRecord) error {
	if err := m.orbBootstrapTools(ctx, client, record.containerID); err != nil {
		return fmt.Errorf("prepare tooling: %w", err)
	}
	if err := m.orbStopRecoveredExecutor(ctx, client, record.containerID); err != nil {
		return fmt.Errorf("stop stale executor: %w", err)
	}
	if err := m.orbConfigureAgentBrowser(ctx, client, record.containerID, record.threadID, false); err != nil {
		return fmt.Errorf("prepare browser: %w", err)
	}
	if err := m.orbInstallExecutor(ctx, cfg, client, record.containerID); err != nil {
		return fmt.Errorf("install executor: %w", err)
	}
	if err := m.orbConfigurePortalHelper(ctx, client, record.containerID); err != nil {
		return fmt.Errorf("prepare portals: %w", err)
	}
	return nil
}

func (m *neoOrbManager) orbStopRecoveredExecutor(ctx context.Context, client neoOrbProviderClient, containerID string) error {
	script := `set -eu
pkill -TERM -f '[a]mp .*--headless=' 2>/dev/null || true
attempt=0
while pgrep -f '[a]mp .*--headless=' >/dev/null 2>&1; do
  attempt=$((attempt + 1))
  [ "$attempt" -lt 50 ] || exit 1
  sleep 0.1
done
/usr/bin/flock -n /run/cliproxy-amp-executor.lock true`
	result, err := client.Exec(ctx, containerID, []string{"/bin/sh", "-lc", script}, nil, "/")
	if err != nil {
		return err
	}
	if result.ExitCode != 0 {
		return fmt.Errorf("stale executor shutdown exited %d: %s", result.ExitCode, clipNeoErrorBody([]byte(result.Stderr)))
	}
	return nil
}

func (m *neoOrbManager) replaceFailedOrb(ctx context.Context, a *neoActor, record *neoOrbRecord, spawnID string) {
	m.mu.Lock()
	threadID := ""
	if record != nil {
		threadID = record.threadID
	}
	blocked := record == nil || m.stopped || record.lifecycleV1 || record.recovered || m.orbs[threadID] != record || record.state != neoOrbStateProvisioning || record.spawnID != spawnID
	m.mu.Unlock()
	if blocked {
		return
	}
	cfg := m.runtime.configSnapshot()
	client, err := m.dockerClient(cfg)
	if err != nil {
		m.setState(record, neoOrbStateFailed, err.Error())
		a.broadcastExecutorStatus(spawnID, "failed", "Cannot reach the Docker host: "+err.Error(), map[string]any{"reasonCode": "spawn_failed", "threadId": threadID})
		return
	}
	removeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	err = client.RemoveContainer(removeCtx, record.containerID, true)
	cancel()
	if err != nil {
		m.setState(record, neoOrbStateFailed, err.Error())
		a.broadcastExecutorStatus(spawnID, "failed", "Cannot replace the failed orb container: "+err.Error(), map[string]any{"reasonCode": "spawn_failed", "threadId": threadID})
		return
	}
	m.reprovisionOrb(ctx, a, record, spawnID)
}

func (m *neoOrbManager) reprovisionOrb(ctx context.Context, a *neoActor, record *neoOrbRecord, spawnID string) {
	m.mu.Lock()
	if record == nil || record.lifecycleV1 || record.recovered || m.orbs[record.threadID] != record {
		m.mu.Unlock()
		return
	}
	portalToken := neoOrbNewPortalToken()
	runnerID := neoOrbRunnerID(record.threadID, portalToken)
	operationMu := record.operationMu
	if operationMu == nil {
		operationMu = &sync.Mutex{}
	}
	replacement := &neoOrbRecord{
		threadID:      record.threadID,
		runnerID:      runnerID,
		spawnID:       spawnID,
		webLocal:      record.webLocal,
		state:         neoOrbStateProvisioning,
		workDir:       firstNonEmptyString(record.workDir, neoOrbWorkDir),
		repositoryURL: record.repositoryURL,
		portalToken:   portalToken,
		operationMu:   operationMu,
	}
	if replacement.webLocal {
		a.mu.Lock()
		a.clearWebLocalExecutorReservationExactLocked(record.spawnID, record.runnerID)
		reserved := a.reserveWebLocalExecutorLocked(spawnID, runnerID)
		a.mu.Unlock()
		if !reserved {
			record.state = neoOrbStateFailed
			record.failReason = "orb publication reservation changed"
			m.mu.Unlock()
			a.broadcastExecutorStatus(spawnID, "failed", "Cannot reserve the replacement orb executor publication.", map[string]any{"reasonCode": "spawn_failed", "threadId": record.threadID})
			return
		}
	}
	m.orbs[record.threadID] = replacement
	m.mu.Unlock()
	a.mu.Lock()
	agentMode := a.agentModeLocked()
	reasoningEffort := a.reasoningEffortForModeLocked(agentMode)
	a.mu.Unlock()
	a.broadcastExecutorStatus(spawnID, "starting", "Provisioning replacement orb container.", map[string]any{"reasonCode": "spawn_requested", "threadId": record.threadID})
	m.provisionOrb(ctx, a, replacement, spawnID, replacement.repositoryURL, agentMode, reasoningEffort)
}

func neoOrbHeadlessCommand(args []string) []string {
	command := []string{"/usr/bin/flock", "-n", neoOrbExecutorLock, neoOrbBinaryPath}
	return append(command, args...)
}

func (m *neoOrbManager) watchOrbConnect(ctx context.Context, a *neoActor, record *neoOrbRecord, spawnID string, timeout time.Duration) {
	if a == nil || record == nil {
		return
	}
	m.mu.Lock()
	runnerID := record.runnerID
	m.mu.Unlock()
	defer a.clearWebLocalExecutorReservation(spawnID, runnerID)
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		ready := a.executorConnectedForRunner(runnerID)
		m.mu.Lock()
		exact := !m.stopped && m.orbs[record.threadID] == record && record.spawnID == spawnID && record.runnerID == runnerID
		m.mu.Unlock()
		if !exact {
			return
		}
		if ready {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			goto timedOut
		case <-ticker.C:
		}
	}

timedOut:
	m.mu.Lock()
	failed := !m.stopped && m.orbs[record.threadID] == record && record.spawnID == spawnID && record.runnerID == runnerID && record.state == neoOrbStateRunning
	if failed {
		record.state = neoOrbStateFailed
		record.failReason = "orb executor did not connect in time"
		record.runnerMigrationRequired = true
		record.runnerMigrationFencing = false
		record.portalIP = ""
		record.portalIPAt = time.Time{}
	}
	m.mu.Unlock()
	if !failed {
		return
	}
	a.broadcastExecutorStatus(spawnID, "failed", "Cannot provision an orb: orb executor did not connect in time.", map[string]any{"reasonCode": "spawn_failed", "threadId": record.threadID})
}

func (m *neoOrbManager) portalAddress(ctx context.Context, cfg *config.Config, threadID string) (string, error) {
	if err := m.ensureRecovered(cfg); err != nil {
		return "", fmt.Errorf("orb recovery failed: %w", err)
	}
	m.mu.Lock()
	record, ok := m.orbs[threadID]
	if ok && record.recovered {
		m.mu.Unlock()
		return "", fmt.Errorf("orb recovery conflict")
	}
	lifecycleV1 := ok && record.lifecycleV1
	if lifecycleV1 && record.state != neoOrbStateRunning {
		m.mu.Unlock()
		return "", fmt.Errorf("orb is not ready")
	}
	if ok && record.portalIP != "" && time.Since(record.portalIPAt) < 5*time.Second {
		ip := record.portalIP
		m.mu.Unlock()
		if lifecycleV1 {
			actor := m.runtime.store.lookupThreadActor(threadID)
			operation, err := m.beginLifecycleAdmission(ctx, actor, threadID, neoOrbStateRunning)
			if err != nil {
				return "", fmt.Errorf("orb recovery conflict")
			}
			operation.close()
		}
		return ip, nil
	}
	if !ok || record.containerID == "" {
		m.mu.Unlock()
		return "", fmt.Errorf("no orb container for thread")
	}
	containerID := record.containerID
	m.mu.Unlock()

	if lifecycleV1 {
		actor := m.runtime.store.lookupThreadActor(threadID)
		operation, err := m.beginLifecycleAdmission(ctx, actor, threadID, neoOrbStateRunning)
		if err != nil {
			return "", fmt.Errorf("orb recovery conflict")
		}
		guarded := operation.providerClient()
		state, err := guarded.InspectContainer(ctx, containerID)
		if err != nil || !state.Exists || !state.Running || state.Paused || strings.TrimSpace(state.IPAddress) == "" {
			operation.close()
			if err != nil {
				return "", fmt.Errorf("orb provider inspect failed: %w", err)
			}
			return "", fmt.Errorf("orb container is not running")
		}
		m.mu.Lock()
		if current := m.orbs[threadID]; current == record && current.state == neoOrbStateRunning && current.containerID == containerID {
			current.portalIP = state.IPAddress
			current.portalIPAt = time.Now()
		}
		m.mu.Unlock()
		operation.close()
		return state.IPAddress, nil
	}

	client, err := m.dockerClient(cfg)
	if err != nil {
		return "", err
	}
	state, err := client.InspectContainer(ctx, containerID)
	if err != nil {
		return "", fmt.Errorf("orb provider inspect failed: %w", err)
	}
	if !state.Exists || !state.Running {
		return "", fmt.Errorf("orb container is not running")
	}
	m.mu.Lock()
	if current := m.orbs[threadID]; current != nil && current.containerID == containerID {
		current.portalIP = state.IPAddress
		current.portalIPAt = time.Now()
	}
	m.mu.Unlock()
	return state.IPAddress, nil
}

func (m *neoOrbManager) acquirePortal(ctx context.Context, cfg *config.Config, threadID string) (neoOrbRecord, func(), error) {
	record := m.live(threadID)
	if record == nil {
		return neoOrbRecord{}, nil, fmt.Errorf("no orb for thread")
	}
	m.mu.Lock()
	lifecycleV1 := m.orbs[threadID] == record && record.lifecycleV1
	recovered := record.recovered
	m.mu.Unlock()
	if recovered {
		return neoOrbRecord{}, nil, fmt.Errorf("orb recovery conflict")
	}
	if lifecycleV1 {
		actor := m.runtime.store.lookupThreadActor(threadID)
		operation, err := m.beginLifecycleAdmission(ctx, actor, threadID, neoOrbStateRunning)
		if err != nil {
			return neoOrbRecord{}, nil, fmt.Errorf("orb recovery conflict")
		}
		m.mu.Lock()
		if m.orbs[threadID] == record && record.state == neoOrbStateRunning {
			record.failReason = ""
			record.idleSince = time.Time{}
			record.activePortals++
		}
		snapshot := *record
		m.mu.Unlock()
		operation.close()
		if snapshot.state != neoOrbStateRunning || snapshot.activePortals == 0 {
			return neoOrbRecord{}, nil, fmt.Errorf("orb is not ready")
		}
		var once sync.Once
		release := func() {
			once.Do(func() {
				m.mu.Lock()
				if m.orbs[threadID] == record && record.activePortals > 0 {
					record.activePortals--
					if record.activePortals == 0 {
						record.idleSince = time.Time{}
					}
				}
				m.mu.Unlock()
			})
		}
		m.ensureReaper()
		return snapshot, release, nil
	}
	operationMu := m.orbOperationMutex(record)
	if err := neoOrbLockOperation(ctx, operationMu); err != nil {
		return neoOrbRecord{}, nil, err
	}
	defer operationMu.Unlock()
	m.mu.Lock()
	if m.orbs[threadID] != record {
		m.mu.Unlock()
		return neoOrbRecord{}, nil, fmt.Errorf("no orb for thread")
	}
	if record.lifecycleV1 || record.recovered {
		m.mu.Unlock()
		return neoOrbRecord{}, nil, fmt.Errorf("orb recovery conflict")
	}
	state := record.state
	containerID := record.containerID
	m.mu.Unlock()
	if state == neoOrbStatePaused {
		if containerID == "" {
			return neoOrbRecord{}, nil, fmt.Errorf("no orb container for thread")
		}
		client, err := m.dockerClient(cfg)
		if err != nil {
			return neoOrbRecord{}, nil, err
		}
		if err := client.UnpauseContainer(ctx, containerID); err != nil {
			return neoOrbRecord{}, nil, fmt.Errorf("orb portal resume failed: %w", err)
		}
	}
	m.mu.Lock()
	if m.orbs[threadID] == record && (record.state == neoOrbStatePaused || record.state == neoOrbStateRunning) {
		record.state = neoOrbStateRunning
		record.failReason = ""
		record.idleSince = time.Time{}
		record.portalIP = ""
		record.portalIPAt = time.Time{}
		record.activePortals++
	}
	snapshot := *record
	m.mu.Unlock()
	if snapshot.state != neoOrbStateRunning || snapshot.activePortals == 0 {
		return neoOrbRecord{}, nil, fmt.Errorf("orb is not ready")
	}
	var once sync.Once
	release := func() {
		once.Do(func() {
			m.mu.Lock()
			if m.orbs[threadID] == record && record.activePortals > 0 {
				record.activePortals--
				if record.activePortals == 0 {
					record.idleSince = time.Time{}
				}
			}
			m.mu.Unlock()
		})
	}
	m.ensureReaper()
	return snapshot, release, nil
}

// markOrbActivity resets the idle clock for a thread's orb. Called when the
// actor observes work or client activity.
func (m *neoOrbManager) markOrbActivity(threadID string) {
	m.mu.Lock()
	if record := m.orbs[threadID]; record != nil {
		record.idleSince = time.Time{}
	}
	m.mu.Unlock()
}

func (m *neoOrbManager) readWorkspaceFile(ctx context.Context, threadID, relative string) ([]byte, error) {
	if m == nil || m.runtime == nil {
		return nil, errors.New("publish_image orb is unavailable")
	}
	if _, err := validateNeoPublishImagePath(relative); err != nil {
		return nil, err
	}
	record := m.live(threadID)
	if record == nil {
		return nil, errors.New("publish_image orb is unavailable")
	}
	m.mu.Lock()
	validRecord := m.orbs[threadID] == record && !record.recovered && record.state == neoOrbStateRunning && record.containerID != "" && record.workDir == neoOrbWorkDir
	lifecycleV1 := record.lifecycleV1
	m.mu.Unlock()
	if !validRecord {
		return nil, errors.New("publish_image orb is not running")
	}
	cfg := m.runtime.configSnapshot()
	client, err := m.dockerClient(cfg)
	if err != nil {
		return nil, errors.New("publish_image orb is unavailable")
	}
	if lifecycleV1 {
		operation, err := m.beginActiveLifecycleOperation(ctx, record, client, cfg)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, errors.New("publish_image orb is unavailable")
		}
		defer operation.close()
		client = operation.providerClient()
		data, err := client.ReadWorkspaceFile(ctx, operation.containerID, operation.fence.workDir, relative, neoAttachmentMaxImageBytes)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, errors.New("publish_image could not read the orb image")
		}
		return data, nil
	}
	operationMu := m.orbOperationMutex(record)
	if err := neoOrbLockOperation(ctx, operationMu); err != nil {
		return nil, err
	}
	defer operationMu.Unlock()
	m.mu.Lock()
	if m.orbs[threadID] != record || record.lifecycleV1 || record.recovered || record.state != neoOrbStateRunning || record.containerID == "" || record.workDir != neoOrbWorkDir {
		m.mu.Unlock()
		return nil, errors.New("publish_image orb is not running")
	}
	containerID := record.containerID
	workDir := record.workDir
	m.mu.Unlock()
	data, err := client.ReadWorkspaceFile(ctx, containerID, workDir, relative, neoAttachmentMaxImageBytes)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errors.New("publish_image could not read the orb image")
	}
	return data, nil
}

func (m *neoOrbManager) orbOperationMutex(record *neoOrbRecord) *sync.Mutex {
	m.mu.Lock()
	defer m.mu.Unlock()
	if record.operationMu == nil {
		record.operationMu = &sync.Mutex{}
	}
	return record.operationMu
}

func neoOrbLockOperation(ctx context.Context, operationMu *sync.Mutex) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if operationMu.TryLock() {
		return nil
	}
	ticker := time.NewTicker(neoOrbOperationLockInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := ctx.Err(); err != nil {
				return err
			}
			if operationMu.TryLock() {
				return nil
			}
		}
	}
}

func (m *neoOrbManager) ensureReaper() {
	m.mu.Lock()
	if m.reaping {
		m.mu.Unlock()
		return
	}
	m.reaping = true
	m.mu.Unlock()
	if !m.startWorker(m.reapLoop) {
		m.mu.Lock()
		m.reaping = false
		m.mu.Unlock()
	}
}

func (m *neoOrbManager) reapLoop(ctx context.Context) {
	ticker := time.NewTicker(neoOrbReapInterval)
	defer ticker.Stop()
	defer func() {
		m.mu.Lock()
		m.reaping = false
		m.mu.Unlock()
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		m.mu.Lock()
		active := 0
		records := make([]*neoOrbRecord, 0, len(m.orbs))
		for _, record := range m.orbs {
			records = append(records, record)
			if record.state == neoOrbStateProvisioning || record.state == neoOrbStateRunning {
				active++
			}
		}
		if active == 0 {
			m.mu.Unlock()
			return
		}
		m.mu.Unlock()
		for _, record := range records {
			m.reapRecordWithContext(ctx, record)
		}
	}
}

func (m *neoOrbManager) reapRecord(record *neoOrbRecord) {
	m.reapRecordWithContext(context.Background(), record)
}

func (m *neoOrbManager) reapRecordWithContext(ctx context.Context, record *neoOrbRecord) {
	if record == nil {
		return
	}
	m.mu.Lock()
	lifecycleV1 := m.orbs[record.threadID] == record && record.lifecycleV1
	m.mu.Unlock()
	if lifecycleV1 {
		m.reapLifecycleRecord(ctx, record)
		return
	}
	operationMu := m.orbOperationMutex(record)
	if err := neoOrbLockOperation(ctx, operationMu); err != nil {
		return
	}
	defer operationMu.Unlock()
	m.mu.Lock()
	if record.lifecycleV1 || record.recovered || m.orbs[record.threadID] != record || record.state != neoOrbStateRunning {
		m.mu.Unlock()
		return
	}
	if record.activePortals > 0 {
		record.idleSince = time.Time{}
		m.mu.Unlock()
		return
	}
	m.mu.Unlock()
	cfg := m.runtime.configSnapshot()
	if !neoOrbsEnabled(cfg) {
		return
	}
	actor := m.runtime.store.get(record.threadID)
	if actor == nil {
		return
	}
	idle := m.orbActorIdle(actor)
	actor.mu.Lock()
	archived := actor.archived
	actor.mu.Unlock()

	m.mu.Lock()
	if !idle {
		record.idleSince = time.Time{}
		m.mu.Unlock()
		return
	}
	if record.idleSince.IsZero() {
		record.idleSince = m.now()
	}
	decisionIdleSince := record.idleSince
	idleFor := m.now().Sub(decisionIdleSince)
	m.mu.Unlock()
	if !archived && idleFor < neoOrbAutoPause(cfg) {
		return
	}
	if !m.orbActorIdle(actor) {
		m.mu.Lock()
		record.idleSince = time.Time{}
		m.mu.Unlock()
		return
	}
	m.mu.Lock()
	consistent := m.orbs[record.threadID] == record && record.state == neoOrbStateRunning && record.activePortals == 0 && record.idleSince.Equal(decisionIdleSince)
	m.mu.Unlock()
	if !consistent {
		return
	}
	client, err := m.dockerClient(cfg)
	if err != nil {
		log.Warnf("amp orbs: reaper cannot reach docker for thread=%s: %v", record.threadID, err)
		return
	}
	pauseCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := client.PauseContainer(pauseCtx, record.containerID); err != nil {
		log.Warnf("amp orbs: pause thread=%s failed: %v", record.threadID, err)
		return
	}
	m.mu.Lock()
	paused := m.orbs[record.threadID] == record && record.state == neoOrbStateRunning
	if paused {
		record.state = neoOrbStatePaused
		record.failReason = ""
	}
	m.mu.Unlock()
	if !paused {
		return
	}
	log.Infof("amp orbs: paused orb thread=%s idle=%s archived=%v", record.threadID, idleFor.Round(time.Second), archived)
}

func (m *neoOrbManager) reapLifecycleRecord(ctx context.Context, record *neoOrbRecord) {
	m.mu.Lock()
	if m.orbs[record.threadID] != record || !record.lifecycleV1 || record.recovered || record.state != neoOrbStateRunning {
		m.mu.Unlock()
		return
	}
	if record.activePortals > 0 {
		record.idleSince = time.Time{}
		m.mu.Unlock()
		return
	}
	m.mu.Unlock()
	cfg := m.runtime.configSnapshot()
	if !neoOrbsEnabled(cfg) {
		return
	}
	actor := m.runtime.store.lookupThreadActor(record.threadID)
	if actor == nil || !m.orbActorIdle(actor) {
		m.mu.Lock()
		if m.orbs[record.threadID] == record {
			record.idleSince = time.Time{}
		}
		m.mu.Unlock()
		return
	}
	actor.mu.Lock()
	archived := actor.archived
	actor.mu.Unlock()
	m.mu.Lock()
	if record.idleSince.IsZero() {
		record.idleSince = m.now()
	}
	decisionIdleSince := record.idleSince
	idleFor := m.now().Sub(decisionIdleSince)
	m.mu.Unlock()
	if !archived && idleFor < neoOrbAutoPause(cfg) {
		return
	}
	operation, err := m.beginLifecycleAdmission(ctx, actor, record.threadID, neoOrbStateRunning)
	if err != nil {
		return
	}
	if !m.orbActorIdle(actor) {
		operation.close()
		m.mu.Lock()
		if m.orbs[record.threadID] == record {
			record.idleSince = time.Time{}
		}
		m.mu.Unlock()
		return
	}
	m.mu.Lock()
	consistent := m.orbs[record.threadID] == record && record.state == neoOrbStateRunning && record.activePortals == 0 && record.idleSince.Equal(decisionIdleSince)
	m.mu.Unlock()
	if !consistent {
		operation.close()
		return
	}
	pauseCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	result := operation.providerClient().(*neoOrbGuardedProvider).PauseContainerInvocation(pauseCtx, operation.containerID)
	cancel()
	if !result.confirmed() {
		operation.close()
		if result.Invoked {
			m.mu.Lock()
			if m.orbs[record.threadID] == record {
				record.state = neoOrbStateConflict
				record.containerID = ""
				record.failReason = "lifecycle-v1 pause requires reconciliation"
				record.portalIP = ""
				record.portalIPAt = time.Time{}
			}
			m.mu.Unlock()
		}
		return
	}
	m.mu.Lock()
	paused := m.orbs[record.threadID] == record && record.state == neoOrbStateRunning && record.activePortals == 0 && record.idleSince.Equal(decisionIdleSince)
	if paused {
		record.state = neoOrbStatePaused
		record.failReason = ""
		record.portalIP = ""
		record.portalIPAt = time.Time{}
	}
	m.mu.Unlock()
	operation.close()
	if paused {
		log.Infof("amp orbs: paused orb thread=%s idle=%s archived=%v", record.threadID, idleFor.Round(time.Second), archived)
	}
}

func (m *neoOrbManager) orbActorIdle(actor *neoActor) bool {
	actor.mu.Lock()
	defer actor.mu.Unlock()
	return normalizeNeoAgentState(actor.agentState) == "idle" &&
		actor.executorID == "" &&
		len(actor.pendingTools) == 0 &&
		len(actor.subagentTools) == 0 &&
		len(actor.approvalQueue) == 0 &&
		len(actor.pluginUIRequests) == 0 &&
		len(actor.terminalRelayChannels) == 0 &&
		len(actor.queue) == 0 &&
		actor.currentInference == nil &&
		actor.pendingInference == nil &&
		!actor.retryScheduled &&
		!actor.compacting
}

// neoOrbExecutorEnv builds the executor environment for the in-orb headless
// Amp CLI. Loopback proxy/runtime addresses are rewritten to the Docker host
// gateway so the container can reach this proxy.
func neoOrbExecutorEnv(cfg *config.Config, threadID, workDir, portalToken string) []string {
	return neoOrbExecutorEnvWithCredentials(cfg, threadID, workDir, portalToken, false)
}

func neoOrbExecutorEnvWithCredentials(cfg *config.Config, threadID, workDir, portalToken string, suppressSharedGitHub bool) []string {
	proxyBase := neoOrbReachableURL(neoProxyBaseURL(cfg))
	portalBase := neoOrbPortalBaseURL(cfg)
	runtimeBase := neoOrbReachableURL(neoRuntimeBaseURL(cfg))
	orbs := neoOrbsConfig(cfg)
	if publicURL := strings.TrimRight(strings.TrimSpace(orbs.PublicURL), "/"); publicURL != "" {
		proxyBase = publicURL
		portalBase = publicURL
	}
	if runtimeURL := strings.TrimRight(strings.TrimSpace(orbs.RuntimePublicURL), "/"); runtimeURL != "" {
		runtimeBase = runtimeURL
	}
	env := []string{
		"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
		"HOME=/root",
		"AMP_EXECUTOR=1",
		"AMP_URL=" + proxyBase,
		"AMP_THREAD_ID=" + threadID,
		"AMP_CURRENT_THREAD_ID=" + threadID,
		"AMP_SKIP_UPDATE_CHECK=1",
		"AMP_HEADLESS_OAUTH=1",
		"AMP_REMOTE_CONTROL_TERMINAL=1",
		"AMP_ORB=1",
		"AMP_ORB_PORTAL_BASE_URL=" + portalBase,
		"AMP_ORB_PORTAL_TOKEN=" + portalToken,
		"AMP_GATEWAY_URL=" + runtimeBase,
		"AMP_RUNTIME_URL=" + runtimeBase,
		"RIVET_ENDPOINT=" + runtimeBase,
		"RIVET_GATEWAY_URL=" + runtimeBase,
		"RIVET_PUBLIC_ENDPOINT=" + runtimeBase,
		"RIVETKIT_ENGINE_URL=" + runtimeBase,
		"RIVET_THREAD_ID=" + threadID,
		"RIVET_TOKEN=" + neoLocalRuntimeClientToken,
		"RIVET_NAMESPACE=default",
		"RIVET_POOL=default",
		"AMP_LOG_FILE=" + neoOrbExecutorLog,
		"AMP_PWD=" + workDir,
		"AGENT_BROWSER_NAMESPACE=" + neoOrbBrowserNamespace(threadID),
		"AGENT_BROWSER_SOCKET_DIR=" + neoOrbAgentBrowserSocketDir,
		"AGENT_BROWSER_EXECUTABLE_PATH=/usr/local/libexec/cliproxy-browser",
		"AGENT_BROWSER_ARGS=--no-sandbox,--disable-dev-shm-usage",
	}
	if key := firstConfiguredAPIKey(cfg); key != "" {
		env = append(env, "AMP_API_KEY="+key)
	}
	for key, value := range neoOrbsConfig(cfg).Env {
		if suppressSharedGitHub && (strings.EqualFold(key, "GH_TOKEN") || strings.EqualFold(key, "GITHUB_TOKEN")) {
			continue
		}
		if !neoOrbEnvKeyValid(key) {
			log.Warnf("amp orbs: ignoring invalid or reserved env key %q", key)
			continue
		}
		env = append(env, key+"="+value)
	}
	return env
}

func neoOrbPortalBaseURL(cfg *config.Config) string {
	orbs := neoOrbsConfig(cfg)
	if publicURL := strings.TrimRight(strings.TrimSpace(orbs.PublicURL), "/"); publicURL != "" {
		return publicURL
	}
	if cfg != nil {
		switch strings.TrimSpace(cfg.Host) {
		case "0.0.0.0", "::", "[::]":
			return ""
		}
	}
	return strings.TrimRight(strings.TrimSpace(neoProxyBaseURL(cfg)), "/")
}

// neoOrbEnvKeyValid allows ordinary variable names while protecting the
// executor's connection contract (AMP_/RIVET_/RIVETKIT_ variables are managed
// by the runtime and cannot be overridden).
func neoOrbEnvKeyValid(key string) bool {
	if key == "" || key == "HOME" || key == "PATH" || strings.HasPrefix(key, "AMP_") || strings.HasPrefix(key, "RIVET_") || strings.HasPrefix(key, "RIVETKIT_") || strings.HasPrefix(key, "AGENT_BROWSER_") || key == "XDG_RUNTIME_DIR" {
		return false
	}
	for i, r := range key {
		if r == '_' || r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' || i > 0 && r >= '0' && r <= '9' {
			continue
		}
		return false
	}
	return true
}

func neoOrbReachableURL(rawURL string) string {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || parsed.Host == "" {
		return rawURL
	}
	host := strings.ToLower(parsed.Hostname())
	loopback := host == "localhost" || strings.HasSuffix(host, ".localhost")
	if ip := net.ParseIP(strings.Trim(host, "[]")); ip != nil {
		loopback = loopback || ip.IsLoopback() || ip.IsUnspecified()
	}
	if !loopback {
		return rawURL
	}
	port := parsed.Port()
	if port != "" {
		parsed.Host = net.JoinHostPort("host.docker.internal", port)
	} else {
		parsed.Host = "host.docker.internal"
	}
	return parsed.String()
}

func shellQuoteNeoOrb(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}

// neoOrbSanitizeCloneURL validates a repository URL before an orb clones it:
// only https and scp-style git@ URLs are accepted, embedded credentials are
// rejected (orb auth comes from the configured token instead), and loopback or
// link-local hosts are rejected so a thread cannot make the orb reach cloud
// metadata endpoints or the proxy host itself.
func neoOrbSanitizeCloneURL(ctx context.Context, raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("empty repository URL")
	}
	if strings.HasPrefix(raw, "git@") {
		if strings.ContainsAny(raw, "?#\r\n\t\x00") {
			return "", errors.New("orb git@ repository URLs cannot contain query, fragment, or control data")
		}
		rest := strings.TrimPrefix(raw, "git@")
		var host string
		var found bool
		if strings.HasPrefix(rest, "[") {
			end := strings.Index(rest, "]")
			if end > 0 && len(rest) > end+1 && rest[end+1] == ':' {
				host, found = rest[1:end], true
			}
		} else {
			host, _, found = strings.Cut(rest, ":")
		}
		if !found || neoOrbBlockedCloneHost(ctx, host) {
			return "", errors.New("repository URL host is not allowed for orb clones")
		}
		return raw, nil
	}
	parsed, err := url.Parse(raw)
	if err != nil || !strings.EqualFold(parsed.Scheme, "https") {
		return "", errors.New("orb clones require an https or git@ repository URL")
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" || parsed.RawFragment != "" {
		return "", errors.New("orb https repository URLs cannot contain credentials, query parameters, or fragments")
	}
	host := parsed.Hostname()
	if neoOrbBlockedCloneHost(ctx, host) {
		return "", errors.New("repository URL host is not allowed for orb clones")
	}
	return parsed.String(), nil
}

func neoOrbBlockedCloneHost(ctx context.Context, host string) bool {
	host = strings.Trim(strings.ToLower(strings.TrimSpace(host)), "[]")
	if zone := strings.Index(host, "%"); zone > 0 {
		host = host[:zone]
	}
	if host == "" || host == "localhost" || strings.HasSuffix(host, ".localhost") || strings.HasSuffix(host, ".internal") {
		return true
	}
	if ip := net.ParseIP(host); ip != nil {
		return neoOrbBlockedIP(ip)
	}
	// Best-effort DNS check: a hostname with any loopback or link-local answer
	// is rejected. Git re-resolves at clone time, so this cannot be a complete
	// DNS-rebinding defense; it exists to keep cloud metadata endpoints and the
	// proxy host out of orb clones.
	resolver := net.DefaultResolver
	if ips, err := resolver.LookupIP(ctx, "ip", host); err == nil && len(ips) > 0 {
		for _, ip := range ips {
			if neoOrbBlockedIP(ip) {
				return true
			}
		}
	}
	return false
}

func neoOrbBlockedIP(ip net.IP) bool {
	return ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified()
}
