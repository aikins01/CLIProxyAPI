package amp

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
)

type neoOrbFakeProvider struct {
	mu           sync.Mutex
	calls        []string
	execHandler  func(cmd []string) neoOrbExecResult
	inspectState neoOrbContainerState
	pingErr      error
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

func (f *neoOrbFakeProvider) StartContainer(ctx context.Context, id string) error {
	f.record("start:" + id)
	return nil
}

func (f *neoOrbFakeProvider) PauseContainer(ctx context.Context, id string) error {
	f.record("pause:" + id)
	f.inspectState.Paused = true
	return nil
}

func (f *neoOrbFakeProvider) UnpauseContainer(ctx context.Context, id string) error {
	f.record("unpause:" + id)
	f.inspectState.Paused = false
	return nil
}

func (f *neoOrbFakeProvider) RemoveContainer(ctx context.Context, id string, force bool) error {
	f.record("remove:" + id)
	return nil
}

func (f *neoOrbFakeProvider) InspectContainer(ctx context.Context, id string) (neoOrbContainerState, error) {
	f.record("inspect:" + id)
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.inspectState, nil
}

func (f *neoOrbFakeProvider) Exec(ctx context.Context, id string, cmd []string, env []string, workDir string) (neoOrbExecResult, error) {
	joined := strings.Join(cmd, " ")
	f.record("exec:" + joined)
	if f.execHandler != nil {
		return f.execHandler(cmd), nil
	}
	return neoOrbExecResult{ExitCode: 0}, nil
}

func (f *neoOrbFakeProvider) ExecDetached(ctx context.Context, id string, cmd []string, env []string, workDir string) error {
	f.record("exec-detached:" + strings.Join(cmd, " ") + ":cwd=" + workDir)
	return nil
}

func (f *neoOrbFakeProvider) CopyFileToContainer(ctx context.Context, id, destPath string, content []byte, mode int64) error {
	f.record(fmt.Sprintf("copy:%s:%d:%d", destPath, len(content), mode))
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
	if fake.callCount("exec-detached:/usr/local/bin/amp") != 1 {
		t.Fatalf("executor was not started detached: %#v", fake.calls)
	}
	if fake.callCount("exec:git clone --depth 1 https://example.test/repo.git") != 1 {
		t.Fatalf("workspace clone missing: %#v", fake.calls)
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
	if fake.callCount("exec-detached:/usr/local/bin/amp") != 2 {
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
			"GH_TOKEN":    "ghp_test",
			"EDITOR":      "vim",
			"AMP_URL":     "https://evil.example",
			"RIVET_TOKEN": "override",
			"9BAD":        "nope",
		}}},
	}
	env := strings.Join(neoOrbExecutorEnv(cfg, "T-019fdec9-b0cf-745d-8da4-f250184e870e", "/work"), "\n")
	if !strings.Contains(env, "GH_TOKEN=ghp_test") || !strings.Contains(env, "EDITOR=vim") {
		t.Fatalf("config env not injected:\n%s", env)
	}
	if strings.Contains(env, "https://evil.example") || strings.Contains(env, "RIVET_TOKEN=override") || strings.Contains(env, "9BAD") {
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
	if strings.Contains(joined, "auth-token.json") {
		t.Fatalf("secret-shaped file was synced:\n%s", joined)
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
	if fake.callCount("exec-detached:/usr/local/bin/amp") != 2 {
		t.Fatalf("executor starts = %d, want exactly one resume restart: %#v", fake.callCount("exec-detached:/usr/local/bin/amp"), fake.calls)
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
	fake.execHandler = func(cmd []string) neoOrbExecResult {
		if strings.HasPrefix(strings.Join(cmd, " "), "git clone") {
			return neoOrbExecResult{ExitCode: 128, Stderr: "fatal: repository not found"}
		}
		return neoOrbExecResult{ExitCode: 0}
	}

	actor.spawnExecutor(map[string]any{"requestId": "spawn-fail", "repositoryURL": "https://example.test/gone.git"})
	waitNeoOrbState(t, manager, threadID, neoOrbStateFailed)
	if fake.callCount("remove:container-fake") != 1 {
		t.Fatalf("failed provisioning did not remove the container: %#v", fake.calls)
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
