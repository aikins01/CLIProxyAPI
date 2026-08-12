package amp

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
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
	PauseContainer(ctx context.Context, id string) error
	UnpauseContainer(ctx context.Context, id string) error
	RemoveContainer(ctx context.Context, id string, force bool) error
	InspectContainer(ctx context.Context, id string) (neoOrbContainerState, error)
	Exec(ctx context.Context, id string, cmd []string, env []string, workDir string) (neoOrbExecResult, error)
	ExecDetached(ctx context.Context, id string, cmd []string, env []string, workDir string) error
	CopyFileToContainer(ctx context.Context, id, destPath string, content []byte, mode int64) error
	CopyTarToContainer(ctx context.Context, id, destDir string, files map[string][]byte, mode int64) error
}

const (
	neoOrbStateProvisioning = "provisioning"
	neoOrbStateRunning      = "running"
	neoOrbStatePaused       = "paused"
	neoOrbStateFailed       = "failed"
	neoOrbStateConflict     = "conflict"

	neoOrbWorkDir               = "/home/user/workspace/repo"
	neoOrbBinaryPath            = "/usr/local/bin/amp"
	neoOrbExecutorLog           = "/home/user/.cache/amp/logs/headless.log"
	neoOrbExecutorLock          = "/run/cliproxy-amp-executor.lock"
	neoOrbAgentBrowserPath      = "/usr/local/bin/agent-browser"
	neoOrbAgentBrowserSocketDir = "/run/cliproxy-agent-browser"
	neoOrbContainerIDMetaKey    = "cliproxyOrbContainerID"
	neoOrbPortalTokenMetaKey    = "cliproxyOrbPortalToken"
	neoOrbReapInterval          = 30 * time.Second

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
	threadID       string
	containerID    string
	state          string
	workDir        string
	repositoryURL  string
	portalToken    string
	activePortals  int
	idleSince      time.Time
	failReason     string
	portalIP       string
	portalIPAt     time.Time
	recovered      bool
	migrationReady bool
	operationMu    *sync.Mutex
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

	newClient func(host string) (neoOrbProviderClient, error)
	now       func() time.Time
}

func newNeoOrbManager(rt *neoRuntime) *neoOrbManager {
	return &neoOrbManager{
		runtime: rt,
		orbs:    map[string]*neoOrbRecord{},
		newClient: func(host string) (neoOrbProviderClient, error) {
			return newNeoOrbDockerClient(host)
		},
		now: time.Now,
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
	host := strings.TrimSpace(neoOrbsConfig(cfg).DockerHost)
	m.mu.Lock()
	defer m.mu.Unlock()
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
	host := strings.TrimSpace(neoOrbsConfig(cfg).DockerHost)
	m.mu.Lock()
	if m.recovered && m.recoveredKey == host {
		m.mu.Unlock()
		return nil
	}
	m.mu.Unlock()
	m.recoverMu.Lock()
	defer m.recoverMu.Unlock()
	m.mu.Lock()
	if m.recovered && m.recoveredKey == host {
		m.mu.Unlock()
		return nil
	}
	initialized := m.recovered || m.client != nil || len(m.orbs) != 0
	if initialized && m.recoveredKey != host {
		m.mu.Unlock()
		return fmt.Errorf("changing orbs.docker-host requires restarting CLIProxyAPI")
	}
	m.recoveredKey = host
	m.mu.Unlock()
	client, err := m.dockerClient(cfg)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	containers, err := client.ListOrbContainers(ctx)
	if err != nil {
		return fmt.Errorf("list labelled containers: %w", err)
	}
	grouped := map[string][]neoOrbContainerSummary{}
	repositories := map[string]string{}
	recoverable := map[string]bool{}
	containerBindings := map[string]string{}
	portalTokens := map[string]string{}
	for _, container := range containers {
		threadID := strings.TrimSpace(container.Labels["cliproxy.orb"])
		if !neoThreadIDExactPattern.MatchString(threadID) {
			log.Warnf("amp orbs: ignoring container=%s with invalid thread label", container.ID)
			continue
		}
		grouped[threadID] = append(grouped[threadID], container)
		thread, ok := loadNeoThreadFromDir(threadID, m.runtime.threadDir)
		if ok {
			executorType := firstNonEmptyString(thread["executorType"], nestedValue(thread["meta"], "executorType"), nestedValue(thread["threadMeta"], "executorType"))
			if strings.EqualFold(executorType, "sandbox") {
				recoverable[threadID] = true
				repositories[threadID] = strings.TrimSpace(firstNonEmptyString(thread["repositoryURL"], nestedValue(thread["meta"], "repositoryURL"), nestedValue(thread["project"], "repositoryURL")))
				containerBindings[threadID] = strings.TrimSpace(firstNonEmptyString(nestedValue(thread["meta"], neoOrbContainerIDMetaKey), nestedValue(thread["threadMeta"], neoOrbContainerIDMetaKey)))
				portalTokens[threadID] = strings.TrimSpace(firstNonEmptyString(nestedValue(thread["meta"], neoOrbPortalTokenMetaKey), nestedValue(thread["threadMeta"], neoOrbPortalTokenMetaKey)))
			}
		}
	}
	recovered := make(map[string]*neoOrbRecord, len(grouped))
	for threadID, matches := range grouped {
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
		record.portalToken = strings.TrimSpace(matches[0].Labels[neoOrbPortalTokenLabel])
		if !neoOrbPortalTokenValid(record.portalToken) {
			record.portalToken = portalTokens[threadID]
		}
		persistPortalToken := false
		if !neoOrbPortalTokenValid(record.portalToken) {
			record.portalToken = neoOrbNewPortalToken()
			persistPortalToken = true
		}
		record.containerID = matches[0].ID
		state, errInspect := client.InspectContainer(ctx, record.containerID)
		if errInspect != nil {
			return fmt.Errorf("inspect labelled container %s: %w", record.containerID, errInspect)
		}
		switch {
		case state.Exists && state.Paused:
			record.state = neoOrbStatePaused
		case state.Exists && state.Running:
			record.state = neoOrbStateRunning
		default:
			record.failReason = "labelled container is not running or paused"
			log.Warnf("amp orbs: thread=%s container=%s is not safely recoverable; automatic provisioning is blocked", threadID, record.containerID)
		}
		if persistPortalToken && record.failReason == "" {
			actor, release := m.runtime.store.retainThreadActorWithoutReadyWork(threadID)
			if actor == nil {
				return fmt.Errorf("persist recovered orb portal token for thread %s: thread actor is unavailable", threadID)
			}
			errPersist := m.persistContainerBinding(actor, record.containerID, record.portalToken)
			release()
			if errPersist != nil {
				return fmt.Errorf("persist recovered orb portal token for thread %s: %w", threadID, errPersist)
			}
		}
		recovered[threadID] = record
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
	for _, record := range recovered {
		if record.state == neoOrbStateRunning {
			m.ensureReaper()
			break
		}
	}
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

func (m *neoOrbManager) orbExecutorMigrationRequired(threadID string) (bool, error) {
	if err := m.ensureRecovered(m.runtime.configSnapshot()); err != nil {
		return true, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	record := m.orbs[threadID]
	return record != nil && record.recovered && !record.migrationReady, nil
}

func (m *neoOrbManager) setState(record *neoOrbRecord, state, reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	record.state = state
	record.failReason = reason
	if state == neoOrbStateRunning {
		record.idleSince = time.Time{}
		record.recovered = false
	}
}

// spawnOrb provisions (or resumes) the orb backing a thread and starts the
// headless Amp executor inside it. The executor connects back to this proxy
// over the thread WebSocket exactly like a locally spawned headless executor.
func (m *neoOrbManager) spawnOrb(a *neoActor, msg map[string]any) map[string]any {
	spawnID := firstNonEmptyString(msg["spawnId"], msg["requestId"])
	if spawnID == "" {
		spawnID = "spawn-orb-" + randomBase62(12)
	}
	cfg := m.runtime.configSnapshot()
	if reason := neoOrbsAvailable(cfg); reason != "" {
		return a.broadcastExecutorStatus(spawnID, "failed", "Cannot provision an orb: "+reason+".", map[string]any{"reasonCode": "spawn_rejected"})
	}
	threadID := firstNonEmptyString(a.threadID, a.key)
	if !neoThreadIDExactPattern.MatchString(threadID) {
		return a.broadcastExecutorStatus(spawnID, "failed", "Cannot provision an orb without a valid thread ID.", map[string]any{"reasonCode": "environment_missing"})
	}
	if err := m.ensureRecovered(cfg); err != nil {
		return a.broadcastExecutorStatus(spawnID, "failed", "Cannot safely reconcile existing orb containers: "+err.Error(), map[string]any{"reasonCode": "environment_recovering", "threadId": threadID})
	}

	record := &neoOrbRecord{threadID: threadID, state: neoOrbStateProvisioning, workDir: neoOrbWorkDir, portalToken: neoOrbNewPortalToken(), operationMu: &sync.Mutex{}}
	m.mu.Lock()
	existing := m.orbs[threadID]
	existingState := ""
	existingFailReason := ""
	action := "provision"
	if existing != nil {
		existingState = existing.state
		existingFailReason = existing.failReason
		switch existing.state {
		case neoOrbStateProvisioning:
			action = "inflight"
		case neoOrbStateRunning:
			if existing.recovered {
				existing.state = neoOrbStateProvisioning
				action = "resume"
			} else {
				action = "inflight"
			}
		case neoOrbStatePaused:
			existing.state = neoOrbStateProvisioning
			action = "resume"
		case neoOrbStateFailed:
			if existing.containerID != "" {
				existing.state = neoOrbStateProvisioning
				action = "replace"
			}
		case neoOrbStateConflict:
			action = "conflict"
		}
	}
	if action == "provision" {
		m.orbs[threadID] = record
	}
	m.mu.Unlock()
	switch action {
	case "inflight":
		status := "starting"
		message := "Orb is already starting for this thread."
		if existingState == neoOrbStateRunning {
			status = "running"
			message = "Orb is already running for this thread."
		}
		return a.broadcastExecutorStatus(spawnID, status, message, map[string]any{"reasonCode": "waiting_for_executor_connect", "threadId": threadID})
	case "resume":
		go m.resumeOrb(a, existing, spawnID)
		m.ensureReaper()
		message := "Resuming paused orb."
		if existingState == neoOrbStateRunning {
			message = "Preparing recovered orb executor."
		}
		return a.broadcastExecutorStatus(spawnID, "starting", message, map[string]any{"reasonCode": "environment_recovering", "threadId": threadID})
	case "replace":
		go m.replaceFailedOrb(a, existing, spawnID)
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
	go m.provisionOrb(a, record, spawnID, repositoryURL, agentMode, reasoningEffort)
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

func (m *neoOrbManager) provisionOrb(a *neoActor, record *neoOrbRecord, spawnID, repositoryURL, agentMode, reasoningEffort string) {
	cfg := m.runtime.configSnapshot()
	threadID := record.threadID
	ctx := context.Background()
	client, clientErr := m.dockerClient(cfg)
	fail := func(stage, message string, err error) {
		m.mu.Lock()
		containerID := record.containerID
		m.mu.Unlock()
		removed := false
		if containerID != "" {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
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
	if neoOrbSyncLocalConfigEnabled(cfg) {
		m.orbSyncLocalConfig(setupCtx, client, containerID)
	}
	if neoOrbGitHubToken(cfg) != "" {
		if err := m.orbConfigureGitHubAuth(setupCtx, cfg, client, containerID); err != nil {
			fail("git-auth", "Cannot configure GitHub access in the orb", err)
			return
		}
	}

	a.broadcastExecutorStatus(spawnID, "starting", "Configuring orb workspace.", map[string]any{"reasonCode": "configuring_workspace", "threadId": threadID})
	if err := m.orbConfigureWorkspace(setupCtx, client, containerID, repositoryURL, record.workDir); err != nil {
		fail("workspace", "Cannot configure the orb workspace", err)
		return
	}

	env := neoOrbExecutorEnv(cfg, threadID, record.workDir, record.portalToken)
	args := neoHeadlessExecutorArgs(threadID, agentMode, reasoningEffort, neoOrbExecutorLog)
	cmd := neoOrbHeadlessCommand(args)
	a.broadcastExecutorStatus(spawnID, "starting", "Starting the headless executor in the orb.", map[string]any{"reasonCode": "starting_headless", "threadId": threadID})
	if err := client.ExecDetached(setupCtx, containerID, cmd, env, record.workDir); err != nil {
		fail("headless", "Cannot start the headless executor in the orb", err)
		return
	}
	m.setState(record, neoOrbStateRunning, "")
	a.broadcastExecutorStatus(spawnID, "running", "Waiting for the orb executor to connect.", map[string]any{"reasonCode": "waiting_for_executor_connect", "threadId": threadID})
	go m.watchOrbConnect(a, record, spawnID, neoExecutorConnectTimeout(cfg))
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
	installScript := "export HOME=/root; curl -fsSL https://ampcode.com/install.sh | bash && AMPBIN=\"$(command -v amp || true)\" && AMPBIN=\"${AMPBIN:-/root/.amp/bin/amp}\" && cp \"$AMPBIN\" " + neoOrbBinaryPath + " && chmod 0755 " + neoOrbBinaryPath
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
	tokenEnv := "GH_TOKEN=" + token
	script := `git config --global url."https://x-access-token:${GH_TOKEN}@github.com/".insteadOf "https://github.com/" && git config --global --add url."https://x-access-token:${GH_TOKEN}@github.com/".insteadOf "git@github.com:"`
	result, err := client.Exec(ctx, containerID, []string{"/bin/sh", "-lc", script}, []string{tokenEnv}, "/")
	if err != nil {
		return err
	}
	if result.ExitCode != 0 {
		return fmt.Errorf("git config exited %d", result.ExitCode)
	}
	return nil
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

func (m *neoOrbManager) resumeOrb(a *neoActor, record *neoOrbRecord, spawnID string) {
	operationMu := m.orbOperationMutex(record)
	operationMu.Lock()
	defer operationMu.Unlock()
	cfg := m.runtime.configSnapshot()
	threadID := record.threadID
	ctx := context.Background()
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
		m.reprovisionOrb(a, record, spawnID)
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
	a.mu.Lock()
	agentMode := a.agentModeLocked()
	reasoningEffort := a.reasoningEffortForModeLocked(agentMode)
	a.mu.Unlock()
	env := neoOrbExecutorEnv(cfg, threadID, record.workDir, record.portalToken)
	args := neoHeadlessExecutorArgs(threadID, agentMode, reasoningEffort, neoOrbExecutorLog)
	resumeScript := fmt.Sprintf("if [ -x .agents/resume ]; then mkdir -p /home/user/.cache/amp/logs && timeout %d .agents/resume > /home/user/.cache/amp/logs/resume.log 2>&1 || true; fi", neoOrbResumeHookSeconds)
	_, _ = client.Exec(resumeCtx, record.containerID, []string{"/bin/sh", "-lc", resumeScript}, nil, record.workDir)
	if err := client.ExecDetached(resumeCtx, record.containerID, neoOrbHeadlessCommand(args), env, record.workDir); err != nil {
		m.setState(record, neoOrbStateFailed, err.Error())
		a.broadcastExecutorStatus(spawnID, "failed", "Cannot restart the orb executor: "+err.Error(), map[string]any{"reasonCode": "spawn_failed", "threadId": threadID})
		return
	}
	m.mu.Lock()
	if m.orbs[threadID] == record {
		record.migrationReady = true
	}
	m.mu.Unlock()
	m.setState(record, neoOrbStateRunning, "")
	a.broadcastExecutorStatus(spawnID, "running", "Waiting for the orb executor to reconnect.", map[string]any{"reasonCode": "waiting_for_executor_connect", "threadId": threadID})
	go m.watchOrbConnect(a, record, spawnID, neoExecutorConnectTimeout(cfg))
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
	if neoOrbSyncLocalConfigEnabled(cfg) {
		m.orbSyncLocalConfig(ctx, client, record.containerID)
	}
	if neoOrbGitHubToken(cfg) != "" {
		if err := m.orbConfigureGitHubAuth(ctx, cfg, client, record.containerID); err != nil {
			return fmt.Errorf("configure GitHub access: %w", err)
		}
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

func (m *neoOrbManager) replaceFailedOrb(a *neoActor, record *neoOrbRecord, spawnID string) {
	cfg := m.runtime.configSnapshot()
	threadID := record.threadID
	client, err := m.dockerClient(cfg)
	if err != nil {
		m.setState(record, neoOrbStateFailed, err.Error())
		a.broadcastExecutorStatus(spawnID, "failed", "Cannot reach the Docker host: "+err.Error(), map[string]any{"reasonCode": "spawn_failed", "threadId": threadID})
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	err = client.RemoveContainer(ctx, record.containerID, true)
	cancel()
	if err != nil {
		m.setState(record, neoOrbStateFailed, err.Error())
		a.broadcastExecutorStatus(spawnID, "failed", "Cannot replace the failed orb container: "+err.Error(), map[string]any{"reasonCode": "spawn_failed", "threadId": threadID})
		return
	}
	m.reprovisionOrb(a, record, spawnID)
}

func (m *neoOrbManager) reprovisionOrb(a *neoActor, record *neoOrbRecord, spawnID string) {
	m.mu.Lock()
	if m.orbs[record.threadID] != record {
		m.mu.Unlock()
		return
	}
	replacement := &neoOrbRecord{
		threadID:      record.threadID,
		state:         neoOrbStateProvisioning,
		workDir:       firstNonEmptyString(record.workDir, neoOrbWorkDir),
		repositoryURL: record.repositoryURL,
		portalToken:   neoOrbNewPortalToken(),
		operationMu:   record.operationMu,
	}
	m.orbs[record.threadID] = replacement
	m.mu.Unlock()
	a.mu.Lock()
	agentMode := a.agentModeLocked()
	reasoningEffort := a.reasoningEffortForModeLocked(agentMode)
	a.mu.Unlock()
	a.broadcastExecutorStatus(spawnID, "starting", "Provisioning replacement orb container.", map[string]any{"reasonCode": "spawn_requested", "threadId": record.threadID})
	m.provisionOrb(a, replacement, spawnID, replacement.repositoryURL, agentMode, reasoningEffort)
}

func neoOrbHeadlessCommand(args []string) []string {
	command := []string{"/usr/bin/flock", "-n", neoOrbExecutorLock, neoOrbBinaryPath}
	return append(command, args...)
}

func (m *neoOrbManager) watchOrbConnect(a *neoActor, record *neoOrbRecord, spawnID string, timeout time.Duration) {
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		a.mu.Lock()
		ready := a.executorReady || a.executorBootstrapComplete || a.executorID != ""
		a.mu.Unlock()
		if ready {
			return
		}
		if m.live(record.threadID) != record {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	if m.live(record.threadID) != record {
		return
	}
	m.mu.Lock()
	stillRunning := record.state == neoOrbStateRunning
	m.mu.Unlock()
	if !stillRunning {
		return
	}
	m.setState(record, neoOrbStateFailed, "orb executor did not connect in time")
	a.broadcastExecutorStatus(spawnID, "failed", "Orb executor did not connect in time.", map[string]any{"reasonCode": "spawn_failed", "threadId": record.threadID})
}

func (m *neoOrbManager) portalAddress(ctx context.Context, cfg *config.Config, threadID string) (string, error) {
	if err := m.ensureRecovered(cfg); err != nil {
		return "", fmt.Errorf("orb recovery failed: %w", err)
	}
	m.mu.Lock()
	record, ok := m.orbs[threadID]
	if ok && record.portalIP != "" && time.Since(record.portalIPAt) < 5*time.Second {
		ip := record.portalIP
		m.mu.Unlock()
		return ip, nil
	}
	if !ok || record.containerID == "" {
		m.mu.Unlock()
		return "", fmt.Errorf("no orb container for thread")
	}
	containerID := record.containerID
	m.mu.Unlock()

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
	operationMu := m.orbOperationMutex(record)
	operationMu.Lock()
	defer operationMu.Unlock()
	m.mu.Lock()
	if m.orbs[threadID] != record {
		m.mu.Unlock()
		return neoOrbRecord{}, nil, fmt.Errorf("no orb for thread")
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

func (m *neoOrbManager) orbOperationMutex(record *neoOrbRecord) *sync.Mutex {
	m.mu.Lock()
	defer m.mu.Unlock()
	if record.operationMu == nil {
		record.operationMu = &sync.Mutex{}
	}
	return record.operationMu
}

func (m *neoOrbManager) ensureReaper() {
	m.mu.Lock()
	if m.reaping {
		m.mu.Unlock()
		return
	}
	m.reaping = true
	m.mu.Unlock()
	go m.reapLoop()
}

func (m *neoOrbManager) reapLoop() {
	ticker := time.NewTicker(neoOrbReapInterval)
	defer ticker.Stop()
	for range ticker.C {
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
			m.reaping = false
			m.mu.Unlock()
			return
		}
		m.mu.Unlock()
		for _, record := range records {
			m.reapRecord(record)
		}
	}
}

func (m *neoOrbManager) reapRecord(record *neoOrbRecord) {
	operationMu := m.orbOperationMutex(record)
	operationMu.Lock()
	defer operationMu.Unlock()
	m.mu.Lock()
	if m.orbs[record.threadID] != record || record.state != neoOrbStateRunning {
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
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := client.PauseContainer(ctx, record.containerID); err != nil {
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
// stripped (orb auth comes from the configured token instead), and loopback or
// link-local hosts are rejected so a thread cannot make the orb reach cloud
// metadata endpoints or the proxy host itself.
func neoOrbSanitizeCloneURL(ctx context.Context, raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("empty repository URL")
	}
	if strings.HasPrefix(raw, "git@") {
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
			return "", fmt.Errorf("repository URL host %q is not allowed for orb clones", host)
		}
		return raw, nil
	}
	parsed, err := url.Parse(raw)
	if err != nil || !strings.EqualFold(parsed.Scheme, "https") {
		return "", fmt.Errorf("orb clones require an https or git@ repository URL, got %q", raw)
	}
	host := parsed.Hostname()
	if neoOrbBlockedCloneHost(ctx, host) {
		return "", fmt.Errorf("repository URL host %q is not allowed for orb clones", host)
	}
	parsed.User = nil
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
