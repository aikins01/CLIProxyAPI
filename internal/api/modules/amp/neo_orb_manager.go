package amp

import (
	"context"
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
)

// neoOrbProviderClient abstracts the container backend an orb runs on. The
// Docker implementation is neoOrbDockerClient; tests substitute a fake.
type neoOrbProviderClient interface {
	Ping(ctx context.Context) error
	EnsureImage(ctx context.Context, image string) error
	CreateContainer(ctx context.Context, spec neoOrbContainerSpec) (string, error)
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

	neoOrbWorkDir      = "/home/user/workspace/repo"
	neoOrbBinaryPath   = "/usr/local/bin/amp"
	neoOrbExecutorLog  = "/home/user/.cache/amp/logs/headless.log"
	neoOrbReapInterval = 30 * time.Second

	// neoOrbResumeHookSeconds bounds the .agents/resume hook on orb resume. The
	// hook only rewarms caches and services, so a stuck hook must not delay the
	// executor reconnect.
	neoOrbResumeHookSeconds = 10

	// neoOrbSyncMaxBytes bounds the total local-config content uploaded into
	// one orb directory during provisioning.
	neoOrbSyncMaxBytes = 32 << 20
)

type neoOrbRecord struct {
	threadID      string
	containerID   string
	state         string
	workDir       string
	repositoryURL string
	idleSince     time.Time
	failReason    string
	portalIP      string
	portalIPAt    time.Time
}

type neoOrbManager struct {
	runtime *neoRuntime

	mu        sync.Mutex
	orbs      map[string]*neoOrbRecord
	client    neoOrbProviderClient
	clientKey string
	reaping   bool

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

// neoOrbsAvailable returns "" when orb executors can be provisioned, otherwise
// the reason they cannot.
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

// orbSpawnStatus returns the manager-tracked spawn state for pending-work
// gating: provisioning and running orbs count as in-flight; paused or failed
// orbs are resumable and do not block a new spawn request.
func (m *neoOrbManager) orbInFlight(threadID string) bool {
	return m.hasLiveOrb(threadID)
}

func (m *neoOrbManager) setState(record *neoOrbRecord, state, reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	record.state = state
	record.failReason = reason
	if state == neoOrbStateRunning {
		record.idleSince = time.Time{}
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

	record := &neoOrbRecord{threadID: threadID, state: neoOrbStateProvisioning, workDir: neoOrbWorkDir}
	m.mu.Lock()
	existing := m.orbs[threadID]
	existingState := ""
	action := "provision"
	if existing != nil {
		existingState = existing.state
		switch existing.state {
		case neoOrbStateProvisioning, neoOrbStateRunning:
			action = "inflight"
		case neoOrbStatePaused:
			existing.state = neoOrbStateProvisioning
			action = "resume"
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
		return a.broadcastExecutorStatus(spawnID, "starting", "Resuming paused orb.", map[string]any{"reasonCode": "environment_recovering", "threadId": threadID})
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

func (m *neoOrbManager) provisionOrb(a *neoActor, record *neoOrbRecord, spawnID, repositoryURL, agentMode, reasoningEffort string) {
	cfg := m.runtime.configSnapshot()
	threadID := record.threadID
	ctx := context.Background()
	client, clientErr := m.dockerClient(cfg)
	fail := func(stage, message string, err error) {
		m.mu.Lock()
		record.state = neoOrbStateFailed
		record.failReason = message
		containerID := record.containerID
		m.mu.Unlock()
		if containerID != "" {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			if cleanupErr := client.RemoveContainer(cleanupCtx, containerID, true); cleanupErr != nil {
				log.Warnf("amp orbs: cleanup thread=%s container=%s failed: %v", threadID, containerID, cleanupErr)
			}
			cleanupCancel()
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
	containerID, err := client.CreateContainer(setupCtx, neoOrbContainerSpec{
		Name:       "cliproxy-orb-" + strings.ToLower(threadID),
		Image:      image,
		Cmd:        []string{"sleep", "infinity"},
		NanoCPUs:   orbs.NanoCPUs,
		MemoryMB:   orbs.MemoryMB,
		ExtraHosts: []string{"host.docker.internal:host-gateway"},
		Labels:     map[string]string{"cliproxy.orb": threadID},
	})
	if err != nil {
		fail("create", "Cannot create the orb container", err)
		return
	}
	m.mu.Lock()
	record.containerID = containerID
	record.repositoryURL = repositoryURL
	m.mu.Unlock()
	if err := client.StartContainer(setupCtx, containerID); err != nil {
		fail("start", "Cannot start the orb container", err)
		return
	}

	if err := m.orbBootstrapTools(setupCtx, client, containerID); err != nil {
		fail("tools", "Cannot prepare orb tooling", err)
		return
	}
	if err := m.orbInstallExecutor(setupCtx, cfg, client, containerID); err != nil {
		fail("executor", "Cannot install the Amp executor in the orb", err)
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

	env := neoOrbExecutorEnv(cfg, threadID, record.workDir)
	args := neoHeadlessExecutorArgs(threadID, agentMode, reasoningEffort, neoOrbExecutorLog)
	cmd := append([]string{neoOrbBinaryPath}, args...)
	a.broadcastExecutorStatus(spawnID, "starting", "Starting the headless executor in the orb.", map[string]any{"reasonCode": "starting_headless", "threadId": threadID})
	if err := client.ExecDetached(setupCtx, containerID, cmd, env, record.workDir); err != nil {
		fail("headless", "Cannot start the headless executor in the orb", err)
		return
	}
	m.setState(record, neoOrbStateRunning, "")
	a.broadcastExecutorStatus(spawnID, "running", "Waiting for the orb executor to connect.", map[string]any{"reasonCode": "waiting_for_executor_connect", "threadId": threadID})
	go m.watchOrbConnect(a, record, spawnID, neoExecutorConnectTimeout(cfg))
}

func (m *neoOrbManager) orbBootstrapTools(ctx context.Context, client neoOrbProviderClient, containerID string) error {
	probe, err := client.Exec(ctx, containerID, []string{"/bin/sh", "-lc", "command -v git >/dev/null && command -v curl >/dev/null"}, nil, "/")
	if err != nil {
		return err
	}
	if probe.ExitCode == 0 {
		return nil
	}
	install, err := client.Exec(ctx, containerID, []string{"/bin/sh", "-lc", "apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq git curl ca-certificates tmux ripgrep >/dev/null"}, nil, "/")
	if err != nil {
		return err
	}
	if install.ExitCode != 0 {
		return fmt.Errorf("apt-get install exited %d: %s", install.ExitCode, clipNeoErrorBody([]byte(install.Stderr)))
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
	install, err := client.Exec(ctx, containerID, []string{"/bin/sh", "-lc", "curl -fsSL https://ampcode.com/install.sh | bash && cp \"$(command -v amp)\" " + neoOrbBinaryPath + " && chmod 0755 " + neoOrbBinaryPath}, nil, "/")
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
		if errCopy := client.CopyFileToContainer(ctx, containerID, "/root/.config/amp/settings.json", settings, 0o644); errCopy != nil {
			log.Warnf("amp orbs: settings sync failed: %v", errCopy)
		}
	}
	budget := neoOrbSyncMaxBytes
	for _, dir := range []string{"checks", "skills"} {
		source := filepath.Join(home, ".config", "agents", dir)
		m.orbSyncDirectory(ctx, client, containerID, source, "/root/.config/agents/"+dir, &budget)
	}
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
		if neoOrbSecretLikeName(name) {
			continue
		}
		sourcePath := filepath.Join(sourceDir, name)
		if entry.IsDir() {
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
	if err != nil || !state.Exists {
		m.mu.Lock()
		repositoryURL := record.repositoryURL
		delete(m.orbs, threadID)
		m.mu.Unlock()
		go m.spawnOrb(a, map[string]any{"spawnId": spawnID, "repositoryURL": repositoryURL})
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
	a.mu.Lock()
	agentMode := a.agentModeLocked()
	reasoningEffort := a.reasoningEffortForModeLocked(agentMode)
	a.mu.Unlock()
	env := neoOrbExecutorEnv(cfg, threadID, record.workDir)
	args := neoHeadlessExecutorArgs(threadID, agentMode, reasoningEffort, neoOrbExecutorLog)
	resumeCtx, resumeCancel := context.WithTimeout(ctx, neoOrbSetupTimeout(cfg))
	defer resumeCancel()
	resumeScript := fmt.Sprintf("if [ -x .agents/resume ]; then mkdir -p /home/user/.cache/amp/logs && timeout %d .agents/resume > /home/user/.cache/amp/logs/resume.log 2>&1 || true; fi", neoOrbResumeHookSeconds)
	_, _ = client.Exec(resumeCtx, record.containerID, []string{"/bin/sh", "-lc", resumeScript}, nil, record.workDir)
	if err := client.ExecDetached(resumeCtx, record.containerID, append([]string{neoOrbBinaryPath}, args...), env, record.workDir); err != nil {
		m.setState(record, neoOrbStateFailed, err.Error())
		a.broadcastExecutorStatus(spawnID, "failed", "Cannot restart the orb executor: "+err.Error(), map[string]any{"reasonCode": "spawn_failed", "threadId": threadID})
		return
	}
	m.setState(record, neoOrbStateRunning, "")
	a.broadcastExecutorStatus(spawnID, "running", "Waiting for the orb executor to reconnect.", map[string]any{"reasonCode": "waiting_for_executor_connect", "threadId": threadID})
	go m.watchOrbConnect(a, record, spawnID, neoExecutorConnectTimeout(cfg))
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

// portalAddress returns the orb container's current IP, cached briefly so
// portal requests do not each cost a Docker inspect.
func (m *neoOrbManager) portalAddress(ctx context.Context, cfg *config.Config, threadID string) (string, error) {
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

// markOrbActivity resets the idle clock for a thread's orb. Called when the
// actor observes work or client activity.
func (m *neoOrbManager) markOrbActivity(threadID string) {
	m.mu.Lock()
	if record := m.orbs[threadID]; record != nil {
		record.idleSince = time.Time{}
	}
	m.mu.Unlock()
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
	m.mu.Lock()
	if record.state != neoOrbStateRunning {
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
	if !idle && !archived {
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
	if !archived && !m.orbActorIdle(actor) {
		m.mu.Lock()
		record.idleSince = time.Time{}
		m.mu.Unlock()
		return
	}
	m.mu.Lock()
	consistent := record.state == neoOrbStateRunning && record.idleSince.Equal(decisionIdleSince)
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
	m.setState(record, neoOrbStatePaused, "")
	log.Infof("amp orbs: paused orb thread=%s idle=%s archived=%v", record.threadID, idleFor.Round(time.Second), archived)
}

// orbActorIdle reports whether the thread actor currently has no work in
// flight and no connected executor.
func (m *neoOrbManager) orbActorIdle(actor *neoActor) bool {
	actor.mu.Lock()
	defer actor.mu.Unlock()
	return normalizeNeoAgentState(actor.agentState) == "idle" &&
		len(actor.queue) == 0 &&
		actor.pendingInference == nil &&
		!actor.retryScheduled &&
		actor.executorID == ""
}

// neoOrbExecutorEnv builds the executor environment for the in-orb headless
// Amp CLI. Loopback proxy/runtime addresses are rewritten to the Docker host
// gateway so the container can reach this proxy.
func neoOrbExecutorEnv(cfg *config.Config, threadID, workDir string) []string {
	proxyBase := neoOrbReachableURL(neoProxyBaseURL(cfg))
	runtimeBase := neoOrbReachableURL(neoRuntimeBaseURL(cfg))
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

// neoOrbEnvKeyValid allows ordinary variable names while protecting the
// executor's connection contract (AMP_/RIVET_/RIVETKIT_ variables are managed
// by the runtime and cannot be overridden).
func neoOrbEnvKeyValid(key string) bool {
	if key == "" || strings.HasPrefix(key, "AMP_") || strings.HasPrefix(key, "RIVET_") || strings.HasPrefix(key, "RIVETKIT_") {
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
