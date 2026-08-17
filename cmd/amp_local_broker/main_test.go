//go:build darwin

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

const testThreadID = "T-01234567-89ab-cdef-0123-456789abcdef"

type brokerRoundTripper func(*http.Request) (*http.Response, error)

func (roundTrip brokerRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return roundTrip(request)
}

func assertBrokerCallerContext(t *testing.T, requestContext context.Context, cancel context.CancelFunc) {
	t.Helper()
	if requestContext == nil {
		t.Fatal("request context was not captured")
	}
	if _, ok := requestContext.Deadline(); ok {
		t.Fatal("request context unexpectedly has a child deadline")
	}
	if err := requestContext.Err(); err != nil {
		t.Fatalf("request context error before caller cancellation = %v", err)
	}
	cancel()
	if err := requestContext.Err(); !errors.Is(err, context.Canceled) {
		t.Fatalf("request context error after caller cancellation = %v, want context canceled", err)
	}
}

type validationFixture struct {
	root       string
	home       string
	workspace  string
	apiKeyFile string
	ampBinary  string
	config     brokerConfig
}

func TestLoadConfigValidation(t *testing.T) {
	t.Run("valid check", func(t *testing.T) {
		fixture := newValidationFixture(t)
		path := fixture.writeConfig(t, fixture.config)
		var stdout strings.Builder
		var stderr strings.Builder
		if exitCode := run(context.Background(), []string{"-config", path, "-check"}, &stdout, &stderr); exitCode != 0 {
			t.Fatalf("run returned %d: %s", exitCode, stderr.String())
		}
		if stdout.String() != "configuration valid\n" {
			t.Fatalf("stdout = %q", stdout.String())
		}
		loaded, err := loadConfig(path)
		if err != nil {
			t.Fatalf("loadConfig: %v", err)
		}
		if loaded.HeartbeatSeconds != defaultHeartbeat {
			t.Fatalf("heartbeatSeconds = %d", loaded.HeartbeatSeconds)
		}
		for _, directory := range []string{loaded.StateDirectory, loaded.LogDirectory} {
			info, err := os.Stat(directory)
			if err != nil {
				t.Fatalf("stat %s: %v", directory, err)
			}
			if info.Mode().Perm() != 0o700 {
				t.Fatalf("mode for %s = %o", directory, info.Mode().Perm())
			}
		}
	})

	t.Run("unknown field", func(t *testing.T) {
		fixture := newValidationFixture(t)
		data := marshalConfig(t, fixture.config)
		var raw map[string]any
		if err := json.Unmarshal(data, &raw); err != nil {
			t.Fatal(err)
		}
		raw["browserPath"] = fixture.workspace
		path := fixture.writeJSON(t, raw)
		assertLoadConfigError(t, path, "unknown field")
	})

	t.Run("unsafe identifier", func(t *testing.T) {
		fixture := newValidationFixture(t)
		fixture.config.BrokerID = "../broker"
		assertLoadConfigError(t, fixture.writeConfig(t, fixture.config), "safe opaque identifier")
	})

	t.Run("heartbeat upper boundary", func(t *testing.T) {
		fixture := newValidationFixture(t)
		fixture.config.HeartbeatSeconds = maxHeartbeatSeconds
		if _, err := loadConfig(fixture.writeConfig(t, fixture.config)); err != nil {
			t.Fatalf("heartbeatSeconds %d: %v", maxHeartbeatSeconds, err)
		}

		fixture.config.HeartbeatSeconds = maxHeartbeatSeconds + 1
		assertLoadConfigError(t, fixture.writeConfig(t, fixture.config), "heartbeatSeconds must be between 1 and 60")
	})

	t.Run("config permissions", func(t *testing.T) {
		fixture := newValidationFixture(t)
		path := fixture.writeConfig(t, fixture.config)
		if err := os.Chmod(path, 0o640); err != nil {
			t.Fatal(err)
		}
		assertLoadConfigError(t, path, "permissions")
	})

	t.Run("api key permissions", func(t *testing.T) {
		fixture := newValidationFixture(t)
		if err := os.Chmod(fixture.apiKeyFile, 0o604); err != nil {
			t.Fatal(err)
		}
		assertLoadConfigError(t, fixture.writeConfig(t, fixture.config), "permissions")
	})

	t.Run("nested repository path", func(t *testing.T) {
		fixture := newValidationFixture(t)
		nested := filepath.Join(fixture.workspace, "nested")
		if err := os.Mkdir(nested, 0o700); err != nil {
			t.Fatal(err)
		}
		fixture.config.Workspaces[0].Path = nested
		assertLoadConfigError(t, fixture.writeConfig(t, fixture.config), "not the repository root")
	})

	t.Run("broad home rejected unless approved", func(t *testing.T) {
		fixture := newValidationFixture(t)
		initGitRepository(t, fixture.home)
		fixture.config.Workspaces[0].Path = fixture.home
		path := fixture.writeConfig(t, fixture.config)
		assertLoadConfigError(t, path, "broad root")

		fixture.config.Workspaces[0].AllowBroadRoot = true
		if _, err := loadConfig(fixture.writeConfig(t, fixture.config)); err != nil {
			t.Fatalf("explicit broad root approval failed: %v", err)
		}
	})

	t.Run("duplicate canonical workspace", func(t *testing.T) {
		fixture := newValidationFixture(t)
		duplicate := fixture.config.Workspaces[0]
		duplicate.ID = "workspace-two"
		fixture.config.Workspaces = append(fixture.config.Workspaces, duplicate)
		assertLoadConfigError(t, fixture.writeConfig(t, fixture.config), "duplicate workspace path")
	})
}

func TestBrokerHeartbeatIntervalCapsFallbackAtFifteenSeconds(t *testing.T) {
	tests := []struct {
		seconds int
		want    time.Duration
	}{
		{seconds: 1, want: time.Second},
		{seconds: defaultHeartbeat, want: 15 * time.Second},
		{seconds: maxHeartbeatSeconds, want: 15 * time.Second},
	}
	for _, test := range tests {
		if got := brokerHeartbeatInterval(test.seconds); got != test.want {
			t.Errorf("brokerHeartbeatInterval(%d) = %s, want %s", test.seconds, got, test.want)
		}
	}
}

func TestValidateBaseURL(t *testing.T) {
	tests := []struct {
		name     string
		raw      string
		insecure bool
		want     string
		wantErr  bool
		errText  string
	}{
		{name: "https", raw: "https://gateway.example/", want: "https://gateway.example"},
		{name: "loopback", raw: "http://127.0.0.1:8317/", insecure: true, want: "http://127.0.0.1:8317"},
		{name: "insecure disabled", raw: "http://localhost:8317", wantErr: true, errText: "explicitly allowed"},
		{name: "remote http", raw: "http://gateway.example", insecure: true, wantErr: true, errText: "use HTTPS for non-loopback hosts"},
		{name: "path", raw: "https://gateway.example/api", wantErr: true},
		{name: "userinfo", raw: "https://secret@gateway.example", wantErr: true},
		{name: "query", raw: "https://gateway.example/?workspace=/tmp", wantErr: true},
		{name: "fragment", raw: "https://gateway.example/#value", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := validateBaseURL("url", test.raw, test.insecure)
			if (err != nil) != test.wantErr {
				t.Fatalf("validateBaseURL error = %v", err)
			}
			if test.errText != "" && !strings.Contains(err.Error(), test.errText) {
				t.Fatalf("validateBaseURL error = %v, want %q", err, test.errText)
			}
			if got != test.want {
				t.Fatalf("validateBaseURL = %q, want %q", got, test.want)
			}
		})
	}
}

func TestStableRunnerID(t *testing.T) {
	brokerID := "broker-one"
	workspaceID := "workspace-one"
	path := "/approved/workspace"
	digest := sha256.Sum256([]byte(brokerID + "\x00" + workspaceID + "\x00" + path))
	want := runnerIDPrefix + hex.EncodeToString(digest[:runnerIDDigestBytes])
	got := stableRunnerID(brokerID, workspaceID, path)
	if got != want {
		t.Fatalf("stableRunnerID = %q, want %q", got, want)
	}
	if len(got) > 64 || !strings.HasPrefix(got, runnerIDPrefix) {
		t.Fatalf("runner ID is not readable and bounded: %q", got)
	}
	if got != stableRunnerID(brokerID, workspaceID, path) {
		t.Fatal("runner ID is not stable")
	}
	if got == stableRunnerID(brokerID, workspaceID, path+"-other") {
		t.Fatal("runner ID did not change with canonical path")
	}
}

func TestBrokerPublicationRunnerIDGoldenVector(t *testing.T) {
	got := brokerPublicationRunnerID(
		"broker-golden",
		"session-golden",
		7,
		"local-runner-golden",
		"T-019f9000-0000-7000-8000-000000000099",
	)
	if want := "broker-c41acab039a0cb7ba6f682b6027e312a"; got != want {
		t.Fatalf("publication runner ID = %q, want %q", got, want)
	}
	if got == brokerPublicationRunnerID("broker-golden", "session-other", 7, "local-runner-golden", "T-019f9000-0000-7000-8000-000000000099") {
		t.Fatal("publication runner ID did not change with the broker session")
	}
}

func TestMapAgentMode(t *testing.T) {
	tests := []struct {
		mode     string
		effort   string
		expected string
	}{
		{expected: "medium"},
		{mode: "smart", expected: "medium"},
		{mode: "rush", expected: "low"},
		{mode: "deep", expected: "medium"},
		{mode: "deep", effort: "xhigh", expected: "high"},
		{mode: "large", expected: "ultra"},
		{mode: "custom-mode", expected: "custom-mode"},
		{mode: "review", expected: "review"},
		{mode: "agg-man", expected: "agg-man"},
		{mode: "puck", expected: "puck"},
		{mode: "../unsafe", expected: ""},
	}
	for _, test := range tests {
		if got := mapAgentMode(test.mode, test.effort); got != test.expected {
			t.Errorf("mapAgentMode(%q, %q) = %q, want %q", test.mode, test.effort, got, test.expected)
		}
	}
}

func TestCanonicalWorkspaceIncludesGitDiagnostic(t *testing.T) {
	directory := t.TempDir()
	if _, err := canonicalWorkspace(directory); err == nil || !strings.Contains(err.Error(), "not a git repository") {
		t.Fatalf("canonicalWorkspace error = %v", err)
	}
}

func TestChildEnvironmentFiltersBase(t *testing.T) {
	base := []string{
		"HOME=/Users/test", "USER=test", "LOGNAME=test", "PATH=/usr/bin:/bin", "SHELL=/bin/zsh",
		"TMPDIR=/tmp/test", "TMP=/tmp", "TEMP=/tmp", "LANG=en_US.UTF-8", "TERM=xterm-256color",
		"COLORTERM=truecolor", "LC_ALL=en_US.UTF-8", "AWS_SECRET_ACCESS_KEY=secret", "BROKER_BASE_VALUE=secret",
		"AMP_URL=https://untrusted.invalid", "malformed",
	}
	cfg := &brokerConfig{APIURL: "https://api.example", RuntimeURL: "https://runtime.example", apiKey: "api-key"}
	environment := childEnvironment(base, cfg, "/workspace", testThreadID, "/logs/thread.log")
	values := make(map[string]string, len(environment))
	for _, entry := range environment {
		key, value, found := strings.Cut(entry, "=")
		if !found {
			t.Fatalf("malformed child environment entry %q", entry)
		}
		if _, exists := values[key]; exists {
			t.Fatalf("duplicate child environment key %q", key)
		}
		values[key] = value
	}
	for _, key := range []string{"HOME", "USER", "LOGNAME", "PATH", "SHELL", "TMPDIR", "TMP", "TEMP", "LANG", "TERM", "COLORTERM", "LC_ALL"} {
		if _, exists := values[key]; !exists {
			t.Errorf("allowed environment key %q was removed", key)
		}
	}
	for _, key := range []string{"AWS_SECRET_ACCESS_KEY", "BROKER_BASE_VALUE"} {
		if _, exists := values[key]; exists {
			t.Errorf("unapproved environment key %q was inherited", key)
		}
	}
	if values["AMP_URL"] != cfg.APIURL || values["AMP_API_KEY"] != cfg.apiKey {
		t.Fatalf("deliberate Amp environment updates were not preserved")
	}
	wantPath := strings.Join([]string{
		"/usr/bin",
		"/bin",
		"/Users/test/.local/bin",
		"/Users/test/.amp/bin",
		"/Users/test/.bun/bin",
		"/Users/test/.local/share/mise/shims",
		"/Users/test/.local/share/mise/installs/node/latest/bin",
		"/opt/homebrew/bin",
		"/opt/homebrew/sbin",
		"/usr/local/bin",
		"/usr/local/sbin",
		"/usr/sbin",
		"/sbin",
	}, string(os.PathListSeparator))
	if values["PATH"] != wantPath {
		t.Fatalf("PATH = %q, want %q", values["PATH"], wantPath)
	}
}

func TestChildEnvironmentPathIsAbsoluteAndDeduplicated(t *testing.T) {
	home := t.TempDir()
	base := []string{
		"HOME=" + home,
		"PATH=/custom/bin:relative:/usr/bin:/custom/bin:../other:/opt/homebrew/bin:",
	}
	environment := childEnvironment(base, &brokerConfig{}, "/workspace", testThreadID, "/logs/thread.log")
	values := make(map[string]string, len(environment))
	for _, entry := range environment {
		key, value, found := strings.Cut(entry, "=")
		if found {
			values[key] = value
		}
	}
	paths := filepath.SplitList(values["PATH"])
	want := []string{
		"/custom/bin",
		"/usr/bin",
		"/opt/homebrew/bin",
		filepath.Join(home, ".local", "bin"),
		filepath.Join(home, ".amp", "bin"),
		filepath.Join(home, ".bun", "bin"),
		filepath.Join(home, ".local", "share", "mise", "shims"),
		filepath.Join(home, ".local", "share", "mise", "installs", "node", "latest", "bin"),
		"/opt/homebrew/sbin",
		"/usr/local/bin",
		"/usr/local/sbin",
		"/bin",
		"/usr/sbin",
		"/sbin",
	}
	if !reflect.DeepEqual(paths, want) {
		t.Fatalf("PATH entries = %#v, want %#v", paths, want)
	}
	seen := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		if !filepath.IsAbs(path) {
			t.Errorf("PATH contains relative entry %q", path)
		}
		if _, exists := seen[path]; exists {
			t.Errorf("PATH contains duplicate entry %q", path)
		}
		seen[path] = struct{}{}
	}
}

func TestChildEnvironmentFindsHomeTools(t *testing.T) {
	home := t.TempDir()
	inheritedBin := filepath.Join(home, "inherited-bin")
	if err := os.Mkdir(inheritedBin, 0o700); err != nil {
		t.Fatal(err)
	}
	tools := map[string]string{
		"tmux":              filepath.Join(home, ".local", "bin"),
		"open-computer-use": filepath.Join(home, ".amp", "bin"),
		"bun":               filepath.Join(home, ".bun", "bin"),
		"open-browser-use":  filepath.Join(home, ".local", "share", "mise", "shims"),
		"node":              filepath.Join(home, ".local", "share", "mise", "installs", "node", "latest", "bin"),
	}
	for name, directory := range tools {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			t.Fatal(err)
		}
		writePrivateFile(t, filepath.Join(directory, name), []byte("#!/bin/sh\n"), 0o700)
	}
	environment := childEnvironment([]string{"HOME=" + home, "PATH=" + inheritedBin}, &brokerConfig{}, "/workspace", testThreadID, "/logs/thread.log")
	for _, entry := range environment {
		if path, found := strings.CutPrefix(entry, "PATH="); found {
			t.Setenv("PATH", path)
			break
		}
	}
	for name, directory := range tools {
		path, err := exec.LookPath(name)
		if err != nil {
			t.Errorf("locate %s: %v", name, err)
			continue
		}
		want := filepath.Join(directory, name)
		if path != want {
			t.Errorf("%s path = %q, want %q", name, path, want)
		}
	}
}

func TestProcessLock(t *testing.T) {
	stateDirectory := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(stateDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	first, err := acquireProcessLock(stateDirectory, "broker-one")
	if err != nil {
		t.Fatalf("first lock: %v", err)
	}
	t.Cleanup(func() {
		_ = first.Close()
	})
	if second, err := acquireProcessLock(stateDirectory, "broker-one"); err == nil {
		_ = second.Close()
		t.Fatal("second lock unexpectedly succeeded")
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close first lock: %v", err)
	}
	third, err := acquireProcessLock(stateDirectory, "broker-one")
	if err != nil {
		t.Fatalf("lock after release: %v", err)
	}
	if err := third.Close(); err != nil {
		t.Fatalf("close third lock: %v", err)
	}
}

func TestCanonicalWorkspaceUsesValidatedGitFromPath(t *testing.T) {
	fixture := newValidationFixture(t)
	gitPath, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	binDirectory := filepath.Join(fixture.root, "bin")
	if err := os.Mkdir(binDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(gitPath, filepath.Join(binDirectory, "git")); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDirectory)
	want, err := filepath.EvalSymlinks(fixture.workspace)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := canonicalWorkspace(fixture.workspace); err != nil || got != want {
		t.Fatalf("canonicalWorkspace = %q, %v", got, err)
	}
}

func TestProcessLockDirectoryReplacementUsesValidatedDescriptor(t *testing.T) {
	root := t.TempDir()
	stateDirectory := filepath.Join(root, "state")
	if err := os.Mkdir(stateDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	expected, err := os.Stat(stateDirectory)
	if err != nil {
		t.Fatal(err)
	}
	originalDirectory := filepath.Join(root, "original-state")
	previousOpenat := processLockOpenat
	processLockOpenat = func(directoryFD int, path string, flags int, mode uint32) (int, error) {
		if err := os.Rename(stateDirectory, originalDirectory); err != nil {
			return -1, err
		}
		if err := os.Mkdir(stateDirectory, 0o700); err != nil {
			return -1, err
		}
		return previousOpenat(directoryFD, path, flags, mode)
	}
	t.Cleanup(func() { processLockOpenat = previousOpenat })

	lock, err := acquireProcessLock(stateDirectory, "broker-one", expected)
	if err != nil {
		t.Fatalf("acquireProcessLock: %v", err)
	}
	t.Cleanup(func() { _ = lock.Close() })
	lockName := configurationLockLabel + "broker-one.lock"
	if _, err := os.Stat(filepath.Join(originalDirectory, lockName)); err != nil {
		t.Fatalf("stat descriptor-bound lock: %v", err)
	}
	if _, err := os.Stat(filepath.Join(stateDirectory, lockName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("replacement directory lock stat error = %v", err)
	}
}

func TestSessionGeneration(t *testing.T) {
	stateDirectory := t.TempDir()
	cfg := &brokerConfig{BrokerID: "broker-one", StateDirectory: stateDirectory}
	firstBroker, err := newBroker(cfg, http.DefaultClient)
	if err != nil {
		t.Fatalf("create first broker: %v", err)
	}
	secondBroker, err := newBroker(cfg, http.DefaultClient)
	if err != nil {
		t.Fatalf("create second broker: %v", err)
	}
	if firstBroker.sessionGeneration != 1 || secondBroker.sessionGeneration != 2 {
		t.Fatalf("generations = %d, %d", firstBroker.sessionGeneration, secondBroker.sessionGeneration)
	}
	path := filepath.Join(stateDirectory, sessionGenerationLabel+"broker-one")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read generation file: %v", err)
	}
	if string(data) != "2\n" {
		t.Fatalf("generation file = %q", data)
	}
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatalf("stat generation file: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("generation file mode = %o", info.Mode().Perm())
	}
}

func TestSessionGenerationRejectsInsecureFile(t *testing.T) {
	t.Run("permissions", func(t *testing.T) {
		stateDirectory := t.TempDir()
		path := filepath.Join(stateDirectory, sessionGenerationLabel+"broker-one")
		writePrivateFile(t, path, []byte("1\n"), 0o644)
		if _, err := allocateSessionGeneration(stateDirectory, "broker-one"); err == nil || !strings.Contains(err.Error(), "permissions") {
			t.Fatalf("allocateSessionGeneration error = %v", err)
		}
	})

	t.Run("symlink", func(t *testing.T) {
		stateDirectory := t.TempDir()
		target := filepath.Join(stateDirectory, "target")
		writePrivateFile(t, target, []byte("1\n"), 0o600)
		path := filepath.Join(stateDirectory, sessionGenerationLabel+"broker-one")
		if err := os.Symlink(target, path); err != nil {
			t.Fatal(err)
		}
		if _, err := allocateSessionGeneration(stateDirectory, "broker-one"); err == nil || !strings.Contains(err.Error(), "regular") {
			t.Fatalf("allocateSessionGeneration error = %v", err)
		}
	})
}

func TestBrokerRejectsReplacedStateDirectory(t *testing.T) {
	fixture := newValidationFixture(t)
	cfg, err := loadConfig(fixture.writeConfig(t, fixture.config))
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	original := cfg.StateDirectory + "-original"
	if err := os.Rename(cfg.StateDirectory, original); err != nil {
		t.Fatalf("rename state directory: %v", err)
	}
	if err := os.Mkdir(cfg.StateDirectory, 0o700); err != nil {
		t.Fatalf("replace state directory: %v", err)
	}
	if _, err := acquireProcessLock(cfg.StateDirectory, cfg.BrokerID, cfg.stateDirectoryInfo); err == nil || !strings.Contains(err.Error(), "changed after configuration validation") {
		t.Fatalf("acquireProcessLock replacement error = %v", err)
	}
	if _, err := newBroker(cfg, http.DefaultClient); err == nil || !strings.Contains(err.Error(), "changed after configuration validation") {
		t.Fatalf("newBroker replacement error = %v", err)
	}
}

func TestValidateHeartbeatResponseAcceptsStoppedUnknownMode(t *testing.T) {
	runnerID := "local-runner-approved"
	intents := []heartbeatIntent{{
		ThreadID:  testThreadID,
		Desired:   "stopped",
		AgentMode: "unknown",
	}}
	runners := []heartbeatRunnerIntents{{RunnerID: runnerID, Intents: &intents}}
	localBroker := &broker{workspacesByRunner: map[string]*workspaceConfig{runnerID: {}}}
	if err := localBroker.validateHeartbeatResponse(heartbeatResponse{OK: true, Runners: &runners}); err != nil {
		t.Fatalf("validateHeartbeatResponse error = %v", err)
	}
	intents[0].AgentMode = ""
	intents[0].ReasoningEffort = "unknown"
	if err := localBroker.validateHeartbeatResponse(heartbeatResponse{OK: true, Runners: &runners}); err != nil {
		t.Fatalf("validateHeartbeatResponse with empty mode error = %v", err)
	}
}

func TestValidateHeartbeatResponseAcceptsExplicitRunnerRejection(t *testing.T) {
	acceptedID := "local-runner-accepted"
	rejectedID := "local-runner-rejected"
	intents := []heartbeatIntent{}
	runners := []heartbeatRunnerIntents{{RunnerID: acceptedID, Intents: &intents}}
	rejected := []heartbeatRunnerRejection{{RunnerID: rejectedID, Code: "working_directory_conflict", Message: "working directory is already registered by another live runner"}}
	localBroker := &broker{workspacesByRunner: map[string]*workspaceConfig{acceptedID: {}, rejectedID: {}}}
	response := heartbeatResponse{OK: true, Runners: &runners, RejectedRunners: &rejected}
	if err := localBroker.validateHeartbeatResponse(response); err != nil {
		t.Fatalf("validateHeartbeatResponse error = %v", err)
	}
	rejected[0].RunnerID = acceptedID
	if err := localBroker.validateHeartbeatResponse(response); err == nil || !strings.Contains(err.Error(), "duplicate runner result") {
		t.Fatalf("duplicate runner rejection error = %v", err)
	}
}

func TestValidateHeartbeatResponseEnforcesCumulativeIntentLimit(t *testing.T) {
	runnerIDs := []string{"local-runner-cap-a", "local-runner-cap-b"}
	workspaceByRunner := map[string]*workspaceConfig{}
	runners := make([]heartbeatRunnerIntents, 0, len(runnerIDs))
	for runnerIndex, runnerID := range runnerIDs {
		workspaceByRunner[runnerID] = &workspaceConfig{}
		intents := make([]heartbeatIntent, intentLimit/len(runnerIDs))
		for intentIndex := range intents {
			intents[intentIndex] = heartbeatIntent{
				ThreadID:  fmt.Sprintf("T-019f9000-%04x-7000-8000-%012x", runnerIndex, intentIndex),
				Desired:   "running",
				AgentMode: "smart",
			}
		}
		runners = append(runners, heartbeatRunnerIntents{RunnerID: runnerID, Intents: &intents})
	}
	localBroker := &broker{workspacesByRunner: workspaceByRunner}
	response := heartbeatResponse{OK: true, Runners: &runners}
	if err := localBroker.validateHeartbeatResponse(response); err != nil {
		t.Fatalf("%d cumulative intents were rejected: %v", intentLimit, err)
	}
	*runners[1].Intents = append(*runners[1].Intents, heartbeatIntent{
		ThreadID:  "T-019f9000-0001-7000-8001-000000000000",
		Desired:   "running",
		AgentMode: "smart",
	})
	if err := localBroker.validateHeartbeatResponse(response); err == nil || !strings.Contains(err.Error(), "intents to exceed 4096 entries") {
		t.Fatalf("%d cumulative intents error = %v", intentLimit+1, err)
	}
}

func TestHeartbeatPayloadEmitsEmptyRunningThreadsArray(t *testing.T) {
	workspace := workspaceConfig{RunnerID: "local-runner-approved", Path: "/workspace"}
	localBroker := &broker{
		config:   &brokerConfig{BrokerID: "broker-one", Workspaces: []workspaceConfig{workspace}},
		children: map[childKey]*childProcess{},
	}
	payload, err := json.Marshal(localBroker.heartbeatPayload(false))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(payload, []byte(`"runningThreads":[]`)) || bytes.Contains(payload, []byte(`"runningThreads":null`)) {
		t.Fatalf("idle heartbeat payload = %s", payload)
	}
}

func TestValidateHeartbeatResponseRejectsUppercaseThreadID(t *testing.T) {
	runnerID := "local-runner-approved"
	intents := []heartbeatIntent{{
		ThreadID:  "T-01234567-89AB-CDEF-0123-456789ABCDEF",
		Desired:   "running",
		AgentMode: "smart",
	}}
	runners := []heartbeatRunnerIntents{{RunnerID: runnerID, Intents: &intents}}
	localBroker := &broker{workspacesByRunner: map[string]*workspaceConfig{runnerID: {}}}
	if err := localBroker.validateHeartbeatResponse(heartbeatResponse{OK: true, Runners: &runners}); err == nil || !strings.Contains(err.Error(), "invalid thread ID") || !strings.Contains(err.Error(), runnerID) || !strings.Contains(err.Error(), intents[0].ThreadID) {
		t.Fatalf("validateHeartbeatResponse error = %v", err)
	}
}

func TestCanceledReconcileDoesNotLaunch(t *testing.T) {
	fixture := newValidationFixture(t)
	launchPath := filepath.Join(fixture.root, "launched")
	fakeAmp := filepath.Join(fixture.root, "fake-amp")
	writePrivateFile(t, fakeAmp, []byte("#!/bin/sh\n: > \"$TMPDIR/launched\"\n"), 0o700)
	t.Setenv("TMPDIR", fixture.root)
	fixture.config.AmpBinary = fakeAmp
	cfg, err := loadConfig(fixture.writeConfig(t, fixture.config))
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	localBroker, err := newBroker(cfg, http.DefaultClient)
	if err != nil {
		t.Fatalf("newBroker: %v", err)
	}
	intents := []heartbeatIntent{{ThreadID: testThreadID, Desired: "running", AgentMode: "smart"}}
	runners := []heartbeatRunnerIntents{{RunnerID: cfg.Workspaces[0].RunnerID, Intents: &intents}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := localBroker.reconcile(ctx, heartbeatResponse{OK: true, Runners: &runners}); !strings.Contains(fmt.Sprint(err), "context canceled") {
		t.Fatalf("reconcile error = %v", err)
	}
	localBroker.mu.Lock()
	childCount := len(localBroker.children)
	shuttingDown := localBroker.shuttingDown
	localBroker.mu.Unlock()
	if childCount != 0 || !shuttingDown {
		t.Fatalf("children = %d, shuttingDown = %t", childCount, shuttingDown)
	}
	if _, err := os.Stat(launchPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("launch marker stat error = %v", err)
	}
}

func TestHeartbeatResponseBounds(t *testing.T) {
	tests := []struct {
		name         string
		requestLimit time.Duration
		handler      http.HandlerFunc
	}{
		{
			name:         "timeout",
			requestLimit: 20 * time.Millisecond,
			handler: func(response http.ResponseWriter, _ *http.Request) {
				time.Sleep(100 * time.Millisecond)
				_, _ = io.WriteString(response, `{"ok":true,"runners":[]}`)
			},
		},
		{
			name: "oversized response",
			handler: func(response http.ResponseWriter, _ *http.Request) {
				_, _ = response.Write(bytes.Repeat([]byte("x"), maxHeartbeatBody+1))
			},
		},
		{
			name: "unknown response field",
			handler: func(response http.ResponseWriter, _ *http.Request) {
				_, _ = io.WriteString(response, `{"ok":true,"runners":[],"unknown":true}`)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(test.handler)
			defer server.Close()
			requestLimit := test.requestLimit
			if requestLimit == 0 {
				requestLimit = time.Second
			}
			localBroker := &broker{
				config:             &brokerConfig{BrokerID: "broker-one", APIURL: server.URL, apiKey: "secret"},
				client:             server.Client(),
				requestLimit:       requestLimit,
				sessionID:          "session",
				sessionGeneration:  1,
				hostname:           "host",
				workspacesByRunner: map[string]*workspaceConfig{},
				children:           map[childKey]*childProcess{},
			}
			if _, err := localBroker.postHeartbeat(context.Background(), false); err == nil {
				t.Fatal("postHeartbeat unexpectedly succeeded")
			}
		})
	}
}

func TestParseIntentVersionHeaders(t *testing.T) {
	tests := []struct {
		name         string
		headers      http.Header
		wantEpoch    string
		wantRevision *uint64
		wantError    bool
	}{
		{name: "legacy", headers: http.Header{}},
		{name: "versioned", headers: http.Header{intentEpochHeader: {"epoch-1"}, intentRevisionHeader: {"7"}}, wantEpoch: "epoch-1", wantRevision: uint64Pointer(7)},
		{name: "missing revision", headers: http.Header{intentEpochHeader: {"epoch-1"}}, wantError: true},
		{name: "missing epoch", headers: http.Header{intentRevisionHeader: {"7"}}, wantError: true},
		{name: "invalid epoch", headers: http.Header{intentEpochHeader: {"bad epoch"}, intentRevisionHeader: {"7"}}, wantError: true},
		{name: "invalid revision", headers: http.Header{intentEpochHeader: {"epoch-1"}, intentRevisionHeader: {"07"}}, wantError: true},
		{name: "duplicate epoch", headers: http.Header{intentEpochHeader: {"epoch-1", "epoch-2"}, intentRevisionHeader: {"7"}}, wantError: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			epoch, revision, err := parseIntentVersionHeaders(test.headers)
			if (err != nil) != test.wantError {
				t.Fatalf("parseIntentVersionHeaders error = %v", err)
			}
			if err == nil && (epoch != test.wantEpoch || !reflect.DeepEqual(revision, test.wantRevision)) {
				t.Fatalf("parseIntentVersionHeaders = (%q, %v), want (%q, %v)", epoch, revision, test.wantEpoch, test.wantRevision)
			}
		})
	}
}

func TestDecodeControlMessageStrictValidation(t *testing.T) {
	runnerID := "local-runner-approved"
	localBroker := &broker{
		config:             &brokerConfig{BrokerID: "broker-one"},
		sessionID:          "session-one",
		sessionGeneration:  3,
		workspacesByRunner: map[string]*workspaceConfig{runnerID: {}},
	}
	valid := fmt.Sprintf(`{"type":"broker_intents","brokerId":"broker-one","sessionId":"session-one","sessionGeneration":3,"intentEpoch":"epoch-1","intentRevision":4,"runners":[{"runnerId":%q,"intents":[]}]}`, runnerID)
	response, err := localBroker.decodeControlMessage([]byte(valid))
	if err != nil {
		t.Fatalf("decodeControlMessage valid: %v", err)
	}
	if response.IntentEpoch != "epoch-1" || response.IntentRevision == nil || *response.IntentRevision != 4 || response.Runners == nil || len(*response.Runners) != 1 {
		t.Fatalf("decoded control response = %+v", response)
	}

	invalid := map[string][]byte{
		"duplicate":   []byte(strings.Replace(valid, `"type":"broker_intents"`, `"type":"broker_intents","type":"broker_intents"`, 1)),
		"trailing":    []byte(valid + `{}`),
		"unknown":     []byte(strings.Replace(valid, `"runners":`, `"unknown":true,"runners":`, 1)),
		"identity":    []byte(strings.Replace(valid, `"sessionGeneration":3`, `"sessionGeneration":2`, 1)),
		"epoch":       []byte(strings.Replace(valid, `"intentEpoch":"epoch-1"`, `"intentEpoch":"bad epoch"`, 1)),
		"binary size": bytes.Repeat([]byte("x"), maxHeartbeatBody+1),
	}
	for name, payload := range invalid {
		t.Run(name, func(t *testing.T) {
			if _, err := localBroker.decodeControlMessage(payload); err == nil {
				t.Fatal("decodeControlMessage unexpectedly succeeded")
			}
		})
	}
}

func TestApplyIntentResponseOrdering(t *testing.T) {
	localBroker := &broker{
		workspacesByRunner:  map[string]*workspaceConfig{},
		children:            map[childKey]*childProcess{},
		launches:            map[childKey]*childLaunch{},
		intentEpoch:         "epoch-a",
		intentRevision:      5,
		intentRevisionKnown: true,
	}
	runners := []heartbeatRunnerIntents{}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	response := func(epoch string, revision uint64) heartbeatResponse {
		return heartbeatResponse{OK: true, Runners: &runners, IntentEpoch: epoch, IntentRevision: &revision}
	}
	if err := localBroker.applyIntentResponse(canceled, response("epoch-a", 4), false); err != nil {
		t.Fatalf("lower revision was not ignored: %v", err)
	}
	if err := localBroker.applyIntentResponse(canceled, response("epoch-b", 6), false); err != nil {
		t.Fatalf("mismatched epoch was not ignored: %v", err)
	}
	if err := localBroker.applyIntentResponse(canceled, response("epoch-a", 5), false); !errors.Is(err, context.Canceled) {
		t.Fatalf("equal revision error = %v, want context canceled", err)
	}
	if err := localBroker.applyIntentResponse(canceled, response("epoch-b", 1), true); !errors.Is(err, context.Canceled) {
		t.Fatalf("authoritative heartbeat error = %v, want context canceled", err)
	}
	if localBroker.intentEpoch != "epoch-b" || localBroker.intentRevision != 1 || !localBroker.intentRevisionKnown {
		t.Fatalf("authoritative version = (%q, %d, %t)", localBroker.intentEpoch, localBroker.intentRevision, localBroker.intentRevisionKnown)
	}
	if err := localBroker.applyIntentResponse(canceled, heartbeatResponse{OK: true, Runners: &runners}, true); !errors.Is(err, context.Canceled) {
		t.Fatalf("legacy authoritative heartbeat error = %v, want context canceled", err)
	}
	if localBroker.intentEpoch != "" || localBroker.intentRevision != 0 || localBroker.intentRevisionKnown {
		t.Fatalf("legacy authoritative version = (%q, %d, %t)", localBroker.intentEpoch, localBroker.intentRevision, localBroker.intentRevisionKnown)
	}
}

func TestQueueControlMessagePreservesNewestCompatibleRevision(t *testing.T) {
	localBroker := &broker{controlMessages: make(chan heartbeatResponse, 1)}
	response := func(epoch string, revision uint64, ok bool) heartbeatResponse {
		return heartbeatResponse{OK: ok, IntentEpoch: epoch, IntentRevision: &revision}
	}

	localBroker.queueControlMessage(response("epoch-a", 2, true))
	localBroker.queueControlMessage(response("epoch-a", 1, false))
	queued := <-localBroker.controlMessages
	if queued.IntentRevision == nil || *queued.IntentRevision != 2 || !queued.OK {
		t.Fatalf("queued response = %+v, want revision 2", queued)
	}

	localBroker.queueControlMessage(response("epoch-a", 2, false))
	localBroker.queueControlMessage(response("epoch-a", 2, true))
	queued = <-localBroker.controlMessages
	if queued.IntentRevision == nil || *queued.IntentRevision != 2 || !queued.OK {
		t.Fatalf("equal-revision retry = %+v, want latest body", queued)
	}

	localBroker.queueControlMessage(response("epoch-a", 9, false))
	localBroker.queueControlMessage(response("epoch-b", 1, true))
	queued = <-localBroker.controlMessages
	if queued.IntentEpoch != "epoch-b" || queued.IntentRevision == nil || *queued.IntentRevision != 1 {
		t.Fatalf("new epoch response = %+v", queued)
	}
}

func TestApplyIntentResponseIgnoresStaleVersionedHeartbeat(t *testing.T) {
	runnerID := "runner-one"
	threadID := "T-019f5000-0000-4000-8000-000000000001"
	workspace := &workspaceConfig{RunnerID: runnerID}
	localBroker := &broker{
		workspacesByRunner: map[string]*workspaceConfig{runnerID: workspace},
		children:           map[childKey]*childProcess{},
		launches:           map[childKey]*childLaunch{},
	}
	stoppedIntents := []heartbeatIntent{{ThreadID: threadID, Desired: "stopped"}}
	runners := []heartbeatRunnerIntents{{RunnerID: runnerID, Intents: &stoppedIntents}}
	revision := uint64(2)
	if err := localBroker.applyIntentResponse(context.Background(), heartbeatResponse{OK: true, Runners: &runners, IntentEpoch: "epoch-a", IntentRevision: &revision}, false); err != nil {
		t.Fatalf("apply control response: %v", err)
	}
	runningIntents := []heartbeatIntent{{ThreadID: threadID, Desired: "running", AgentMode: "smart"}}
	runners[0].Intents = &runningIntents
	revision = 1
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := localBroker.applyIntentResponse(canceled, heartbeatResponse{OK: true, Runners: &runners, IntentEpoch: "epoch-a", IntentRevision: &revision}, true); err != nil {
		t.Fatalf("stale heartbeat was not ignored: %v", err)
	}
	if localBroker.intentEpoch != "epoch-a" || localBroker.intentRevision != 2 || !localBroker.intentRevisionKnown {
		t.Fatalf("intent version = (%q, %d, %t), want (epoch-a, 2, true)", localBroker.intentEpoch, localBroker.intentRevision, localBroker.intentRevisionKnown)
	}
}

func TestControlPushReconcilesRunningAndEmptyIntents(t *testing.T) {
	fixture := newValidationFixture(t)
	readyPath := filepath.Join(fixture.root, "control-ready")
	fakeAmp := filepath.Join(fixture.root, "fake-amp")
	writePrivateFile(t, fakeAmp, []byte(fmt.Sprintf("#!/bin/sh\n: > %q\ntrap 'exit 0' TERM INT\nwhile :; do sleep 1; done\n", readyPath)), 0o700)
	fixture.config.AmpBinary = fakeAmp
	cfg, err := loadConfig(fixture.writeConfig(t, fixture.config))
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	localBroker, err := newBroker(cfg, http.DefaultClient)
	if err != nil {
		t.Fatalf("newBroker: %v", err)
	}
	t.Cleanup(func() { _ = localBroker.stopAll() })
	runningIntents := []heartbeatIntent{{ThreadID: testThreadID, Desired: "running", AgentMode: "smart"}}
	runners := []heartbeatRunnerIntents{{RunnerID: cfg.Workspaces[0].RunnerID, Intents: &runningIntents}}
	revision := uint64(1)
	if err := localBroker.applyIntentResponse(context.Background(), heartbeatResponse{OK: true, Runners: &runners, IntentEpoch: "epoch-1", IntentRevision: &revision}, false); err != nil {
		t.Fatalf("apply running control response: %v", err)
	}
	waitForPath(t, readyPath)
	emptyIntents := []heartbeatIntent{}
	runners[0].Intents = &emptyIntents
	revision++
	if err := localBroker.applyIntentResponse(context.Background(), heartbeatResponse{OK: true, Runners: &runners, IntentEpoch: "epoch-1", IntentRevision: &revision}, false); err != nil {
		t.Fatalf("apply empty control response: %v", err)
	}
	waitForNoChildren(t, localBroker)
}

func TestRunControlConnectionCancellationClosesBlockedRead(t *testing.T) {
	connected := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer secret" || request.Header.Get(controlBrokerIDHeader) != "broker-one" || request.Header.Get(controlSessionIDHeader) != "session-one" || request.Header.Get(controlGenerationHeader) != "3" {
			http.Error(response, "invalid control identity", http.StatusUnauthorized)
			return
		}
		conn, err := (&websocket.Upgrader{}).Upgrade(response, request, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		close(connected)
		_, _, _ = conn.ReadMessage()
	}))
	defer server.Close()
	localBroker := &broker{
		config:             &brokerConfig{BrokerID: "broker-one", APIURL: server.URL, apiKey: "secret"},
		client:             server.Client(),
		requestLimit:       time.Second,
		sessionID:          "session-one",
		sessionGeneration:  3,
		workspacesByRunner: map[string]*workspaceConfig{},
		controlMessages:    make(chan heartbeatResponse, 1),
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		wasConnected, err := localBroker.runControlConnection(ctx)
		if !wasConnected && err == nil {
			err = errors.New("control connection was not established")
		}
		done <- err
	}()
	select {
	case <-connected:
	case <-time.After(time.Second):
		t.Fatal("control connection was not established")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("runControlConnection error = %v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("control connection did not stop after cancellation")
	}
}

func uint64Pointer(value uint64) *uint64 {
	return &value
}

func TestStaleSessionStopsChildrenAndTerminatesLoop(t *testing.T) {
	fixture := newValidationFixture(t)
	fakeAmp := filepath.Join(fixture.root, "fake-amp")
	writePrivateFile(t, fakeAmp, []byte("#!/bin/sh\ntrap 'exit 0' TERM INT\nwhile :; do sleep 1; done\n"), 0o700)
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		requestCount++
		response.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(response, `{"ok":false,"error":{"code":"stale_session","message":"response-secret"}}`)
	}))
	defer server.Close()
	fixture.config.APIURL = server.URL
	fixture.config.RuntimeURL = server.URL
	fixture.config.AllowInsecureLoopback = true
	fixture.config.AmpBinary = fakeAmp
	cfg, err := loadConfig(fixture.writeConfig(t, fixture.config))
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	localBroker, err := newBroker(cfg, server.Client())
	if err != nil {
		t.Fatalf("newBroker: %v", err)
	}
	t.Cleanup(func() {
		_ = localBroker.stopAll()
	})
	key := childKey{runnerID: cfg.Workspaces[0].RunnerID, threadID: testThreadID}
	if err := localBroker.launchChild(context.Background(), key, "medium"); err != nil {
		t.Fatalf("launchChild: %v", err)
	}
	localBroker.mu.Lock()
	child := localBroker.children[key]
	processGroupID := child.processGroupID
	localBroker.mu.Unlock()
	var stderr strings.Builder
	loopErr := localBroker.loop(context.Background(), &stderr)
	if !errors.Is(loopErr, errStaleSession) {
		t.Fatalf("loop error = %v", loopErr)
	}
	if requestCount != 1 {
		t.Fatalf("heartbeat count = %d", requestCount)
	}
	if strings.Contains(stderr.String(), "response-secret") || strings.Contains(fmt.Sprint(loopErr), "response-secret") {
		t.Fatal("heartbeat response details were exposed")
	}
	localBroker.mu.Lock()
	childCount := len(localBroker.children)
	shuttingDown := localBroker.shuttingDown
	localBroker.mu.Unlock()
	if childCount != 0 || !shuttingDown {
		child.lifecycleMu.Lock()
		cleanupErr := child.cleanupErr
		retired := child.retired
		child.lifecycleMu.Unlock()
		t.Fatalf("children = %d, shuttingDown = %t, retired = %t, cleanup = %v", childCount, shuttingDown, retired, cleanupErr)
	}
	if err := syscall.Kill(-processGroupID, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("child process group still exists: %v", err)
	}
}

func TestHeartbeatRejectionErrorRecognizesStaleSessionForms(t *testing.T) {
	for _, body := range []string{
		`{"code":"stale_session"}`,
		`{"error":"stale_session"}`,
		`{"error":{"code":"stale_session"}}`,
	} {
		if err := heartbeatRejectionError(http.StatusConflict, []byte(body)); !errors.Is(err, errStaleSession) {
			t.Errorf("heartbeatRejectionError(%s) = %v", body, err)
		}
	}
}

func TestHeartbeatRejectionErrorUsesSafeMessage(t *testing.T) {
	if err := heartbeatRejectionError(http.StatusConflict, []byte(`{"ok":false,"message":"broker requires reauthorization"}`)); err == nil || !strings.Contains(err.Error(), "broker requires reauthorization") {
		t.Fatalf("safe heartbeat message error = %v", err)
	}
	for _, message := range []string{"line one\nline two", strings.Repeat("x", heartbeatMessageLimit+1)} {
		body, err := json.Marshal(map[string]any{"ok": false, "message": message})
		if err != nil {
			t.Fatal(err)
		}
		if got := heartbeatRejectionError(http.StatusConflict, body).Error(); got != "heartbeat rejected with HTTP 409" {
			t.Fatalf("unsafe heartbeat message error = %q", got)
		}
	}
	if got := heartbeatRejectionError(http.StatusConflict, []byte(`{"ok":false,"error":{"message":"internal-secret"}}`)).Error(); strings.Contains(got, "internal-secret") {
		t.Fatalf("nested heartbeat error details were exposed: %q", got)
	}
}

func TestSuccessfulStatusStaleSessionIsTerminal(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(response, `{"ok":false,"error":"stale_session"}`)
	}))
	defer server.Close()
	localBroker := &broker{
		config:             &brokerConfig{BrokerID: "broker-one", APIURL: server.URL, apiKey: "secret"},
		client:             server.Client(),
		requestLimit:       time.Second,
		sessionID:          "session",
		sessionGeneration:  1,
		hostname:           "host",
		workspacesByRunner: map[string]*workspaceConfig{},
		children:           map[childKey]*childProcess{},
	}
	if _, err := localBroker.postHeartbeat(context.Background(), false); !errors.Is(err, errStaleSession) {
		t.Fatalf("postHeartbeat stale error = %v", err)
	}
}

func TestSuccessfulHeartbeatValidatesSchemaBeforeStaleSessionClassification(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(response, `{"ok":true,"runners":[],"code":"stale_session"}`)
	}))
	defer server.Close()
	localBroker := &broker{
		config:             &brokerConfig{BrokerID: "broker-one", APIURL: server.URL, apiKey: "secret"},
		client:             server.Client(),
		requestLimit:       time.Second,
		sessionID:          "session",
		sessionGeneration:  1,
		hostname:           "host",
		workspacesByRunner: map[string]*workspaceConfig{},
		children:           map[childKey]*childProcess{},
	}
	if _, err := localBroker.postHeartbeat(context.Background(), false); err == nil || errors.Is(err, errStaleSession) {
		t.Fatalf("postHeartbeat schema error = %v", err)
	}
}

func TestLaunchChildBlocksWhilePreviousCleanupIsIncomplete(t *testing.T) {
	key := childKey{runnerID: "runner-one", threadID: testThreadID}
	localBroker := &broker{
		workspacesByRunner: map[string]*workspaceConfig{key.runnerID: {}},
		children:           map[childKey]*childProcess{key: {key: key, mode: "medium", stopping: true}},
	}
	if err := localBroker.launchChild(context.Background(), key, "medium"); err == nil || !strings.Contains(err.Error(), "cleanup is incomplete") {
		t.Fatalf("launchChild cleanup error = %v", err)
	}
}

func TestLaunchChildRechecksTimedOutProcessGroupWithoutSignalingIt(t *testing.T) {
	fixture := newValidationFixture(t)
	cfg, err := loadConfig(fixture.writeConfig(t, fixture.config))
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	localBroker, err := newBroker(cfg, http.DefaultClient)
	if err != nil {
		t.Fatalf("newBroker: %v", err)
	}
	key := childKey{runnerID: cfg.Workspaces[0].RunnerID, threadID: testThreadID}
	previous := &childProcess{
		key:            key,
		stopping:       true,
		retired:        true,
		processGroupID: 4242,
		cleanupErr:     errors.New("process group did not exit after SIGKILL"),
		cleanupPending: true,
	}
	localBroker.children[key] = previous
	previousSignal := processGroupSignal
	var signals []syscall.Signal
	processGroupSignal = func(_ int, signal syscall.Signal) error {
		signals = append(signals, signal)
		if signal == 0 {
			return syscall.ESRCH
		}
		return errors.New("unexpected process group signal")
	}
	previousStart := processStart
	processStart = func(context.Context, *exec.Cmd) error { return errors.New("launch reached") }
	t.Cleanup(func() {
		processGroupSignal = previousSignal
		processStart = previousStart
	})
	if err := localBroker.launchChild(context.Background(), key, "medium"); err == nil || !strings.Contains(err.Error(), "launch reached") {
		t.Fatalf("launchChild error = %v", err)
	}
	if !reflect.DeepEqual(signals, []syscall.Signal{0}) {
		t.Fatalf("process group signals = %#v", signals)
	}
	localBroker.mu.Lock()
	current := localBroker.children[key]
	localBroker.mu.Unlock()
	if current == previous {
		t.Fatal("confirmed exited process group still blocks the child key")
	}
}

func TestLaunchChildFailureDoesNotRetainReservation(t *testing.T) {
	fixture := newValidationFixture(t)
	badAmp := filepath.Join(fixture.root, "bad-amp")
	writePrivateFile(t, badAmp, []byte("#!/missing/interpreter\n"), 0o700)
	fixture.config.AmpBinary = badAmp
	cfg, err := loadConfig(fixture.writeConfig(t, fixture.config))
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	localBroker, err := newBroker(cfg, http.DefaultClient)
	if err != nil {
		t.Fatalf("newBroker: %v", err)
	}
	key := childKey{runnerID: cfg.Workspaces[0].RunnerID, threadID: testThreadID}
	if err := localBroker.launchChild(context.Background(), key, "medium"); err == nil || !strings.Contains(err.Error(), "start amp") {
		t.Fatalf("launchChild error = %v", err)
	}
	localBroker.mu.Lock()
	launchCount := len(localBroker.launches)
	childCount := len(localBroker.children)
	localBroker.mu.Unlock()
	if launchCount != 0 || childCount != 0 {
		t.Fatalf("failed launch retained launches=%d children=%d", launchCount, childCount)
	}
}

func TestLaunchChildDoesNotBlockHeartbeatState(t *testing.T) {
	fixture := newValidationFixture(t)
	cfg, err := loadConfig(fixture.writeConfig(t, fixture.config))
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	localBroker, err := newBroker(cfg, http.DefaultClient)
	if err != nil {
		t.Fatalf("newBroker: %v", err)
	}
	previousStart := processStart
	startEntered := make(chan struct{})
	processStart = func(ctx context.Context, _ *exec.Cmd) error {
		close(startEntered)
		<-ctx.Done()
		return ctx.Err()
	}
	t.Cleanup(func() {
		processStart = previousStart
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	launchResult := make(chan error, 1)
	key := childKey{runnerID: cfg.Workspaces[0].RunnerID, threadID: testThreadID}
	go func() {
		launchResult <- localBroker.launchChild(ctx, key, "medium")
	}()
	select {
	case <-startEntered:
	case <-time.After(time.Second):
		t.Fatal("process start was not entered")
	}
	heartbeatState := make(chan map[string][]string, 1)
	go func() {
		heartbeatState <- localBroker.runningThreadsByRunner()
	}()
	select {
	case running := <-heartbeatState:
		if len(running) != 0 {
			t.Fatalf("launch reservation appeared as a running child: %#v", running)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("heartbeat state was blocked by child launch")
	}
	cancel()
	if err := <-launchResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("launchChild cancellation error = %v", err)
	}
	localBroker.mu.Lock()
	launchCount := len(localBroker.launches)
	localBroker.mu.Unlock()
	if launchCount != 0 {
		t.Fatalf("launch reservations = %d", launchCount)
	}
}

func TestShutdownDuringStartedLaunchReapsProcessGroup(t *testing.T) {
	fixture := newValidationFixture(t)
	fakeAmp := filepath.Join(fixture.root, "fake-amp")
	writePrivateFile(t, fakeAmp, []byte("#!/bin/sh\ntrap '' TERM INT HUP\nwhile :; do sleep 1; done\n"), 0o700)
	fixture.config.AmpBinary = fakeAmp
	cfg, err := loadConfig(fixture.writeConfig(t, fixture.config))
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	localBroker, err := newBroker(cfg, http.DefaultClient)
	if err != nil {
		t.Fatalf("newBroker: %v", err)
	}
	previousStart := processStart
	started := make(chan int, 1)
	processStart = func(ctx context.Context, command *exec.Cmd) error {
		if errStart := command.Start(); errStart != nil {
			return errStart
		}
		started <- command.Process.Pid
		<-ctx.Done()
		return nil
	}
	t.Cleanup(func() { processStart = previousStart })

	launchResult := make(chan error, 1)
	key := childKey{runnerID: cfg.Workspaces[0].RunnerID, threadID: testThreadID}
	go func() {
		launchResult <- localBroker.launchChild(context.Background(), key, "medium")
	}()
	var processGroupID int
	select {
	case processGroupID = <-started:
	case <-time.After(time.Second):
		t.Fatal("process was not started")
	}
	stopResult := make(chan error, 1)
	go func() {
		stopResult <- localBroker.stopAll()
	}()
	select {
	case errStop := <-stopResult:
		if errStop != nil {
			t.Fatalf("stopAll: %v", errStop)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("stopAll did not complete")
	}
	if errLaunch := <-launchResult; errLaunch == nil || strings.Contains(errLaunch.Error(), "process group did not exit after SIGKILL") {
		t.Fatalf("launchChild shutdown error = %v", errLaunch)
	}
	if err := syscall.Kill(-processGroupID, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("started launch process group still exists: %v", err)
	}
	localBroker.mu.Lock()
	launchCount := len(localBroker.launches)
	childCount := len(localBroker.children)
	localBroker.mu.Unlock()
	if launchCount != 0 || childCount != 0 {
		t.Fatalf("shutdown retained launches=%d children=%d", launchCount, childCount)
	}
}

func TestStopChildrenReportsCompletedCleanupFailure(t *testing.T) {
	previousSignal := processGroupSignal
	processGroupSignal = func(int, syscall.Signal) error { return nil }
	t.Cleanup(func() { processGroupSignal = previousSignal })
	done := make(chan struct{})
	close(done)
	want := errors.New("cleanup failed")
	child := &childProcess{done: done, cleanupErr: want}
	if err := (&broker{}).stopChildren([]*childProcess{child}); !errors.Is(err, want) {
		t.Fatalf("stopChildren error = %v", err)
	}
}

func TestRetiredChildDoesNotSignalProcessGroup(t *testing.T) {
	previousSignal := processGroupSignal
	signalCalls := 0
	processGroupSignal = func(int, syscall.Signal) error {
		signalCalls++
		return nil
	}
	t.Cleanup(func() { processGroupSignal = previousSignal })
	child := &childProcess{
		key:            childKey{runnerID: "runner-one", threadID: testThreadID},
		processGroupID: 4242,
		retired:        true,
	}
	if err := child.signalProcessGroup(syscall.SIGTERM); err != nil {
		t.Fatalf("signal retired child: %v", err)
	}
	if err := child.terminateProcessGroup(false); err != nil {
		t.Fatalf("retire completed child: %v", err)
	}
	if err := child.finishProcessGroupRetirement(); err != nil {
		t.Fatalf("finish completed child retirement: %v", err)
	}
	if signalCalls != 0 {
		t.Fatalf("retired child issued %d process group signals", signalCalls)
	}
}

func TestShutdownStopsChildrenBeforeEmptyHeartbeat(t *testing.T) {
	fixture := newValidationFixture(t)
	fakeAmp := filepath.Join(fixture.root, "fake-amp")
	writePrivateFile(t, fakeAmp, []byte("#!/bin/sh\ntrap 'exit 0' TERM INT\nwhile :; do sleep 1; done\n"), 0o700)
	type observation struct {
		emptyRunners bool
		childExited  bool
		childCount   int
		shuttingDown bool
	}
	observed := make(chan observation, 1)
	var localBroker *broker
	processGroupID := 0
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		var payload heartbeatRequest
		_ = json.NewDecoder(request.Body).Decode(&payload)
		localBroker.mu.Lock()
		childCount := len(localBroker.children)
		shuttingDown := localBroker.shuttingDown
		localBroker.mu.Unlock()
		processErr := syscall.Kill(-processGroupID, 0)
		observed <- observation{
			emptyRunners: len(payload.Runners) == 0,
			childExited:  errors.Is(processErr, syscall.ESRCH),
			childCount:   childCount,
			shuttingDown: shuttingDown,
		}
		response.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(response, `{"ok":true,"runners":[]}`)
	}))
	defer server.Close()
	fixture.config.APIURL = server.URL
	fixture.config.RuntimeURL = server.URL
	fixture.config.AllowInsecureLoopback = true
	fixture.config.AmpBinary = fakeAmp
	cfg, err := loadConfig(fixture.writeConfig(t, fixture.config))
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	localBroker, err = newBroker(cfg, server.Client())
	if err != nil {
		t.Fatalf("newBroker: %v", err)
	}
	t.Cleanup(func() {
		_ = localBroker.stopAll()
	})
	key := childKey{runnerID: cfg.Workspaces[0].RunnerID, threadID: testThreadID}
	ctx, cancel := context.WithCancel(context.Background())
	if err := localBroker.launchChild(ctx, key, "medium"); err != nil {
		t.Fatalf("launchChild: %v", err)
	}
	localBroker.mu.Lock()
	directChildPID := localBroker.children[key].cmd.Process.Pid
	processGroupID = localBroker.children[key].processGroupID
	localBroker.mu.Unlock()
	cancel()
	if err := syscall.Kill(directChildPID, 0); err != nil {
		t.Fatalf("child lifetime was tied to launch context: %v", err)
	}
	if err := localBroker.shutdown(); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	result := <-observed
	if !result.emptyRunners || !result.childExited || result.childCount != 0 || !result.shuttingDown {
		t.Fatalf("heartbeat observation = %+v", result)
	}
}

func TestShutdownReportsFinalHeartbeatFailure(t *testing.T) {
	fixture := newValidationFixture(t)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(response, `{"ok":false,"code":"temporarily_unavailable"}`)
	}))
	defer server.Close()
	fixture.config.APIURL = server.URL
	fixture.config.RuntimeURL = server.URL
	fixture.config.AllowInsecureLoopback = true
	cfg, err := loadConfig(fixture.writeConfig(t, fixture.config))
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	localBroker, err := newBroker(cfg, server.Client())
	if err != nil {
		t.Fatalf("newBroker: %v", err)
	}
	if err := localBroker.shutdown(); err == nil || !strings.Contains(err.Error(), "temporarily_unavailable") {
		t.Fatalf("shutdown error = %v", err)
	}
}

func TestLaunchChildRevalidatesWorkspaceForEveryLaunch(t *testing.T) {
	fixture := newValidationFixture(t)
	cfg, err := loadConfig(fixture.writeConfig(t, fixture.config))
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	localBroker, err := newBroker(cfg, http.DefaultClient)
	if err != nil {
		t.Fatalf("newBroker: %v", err)
	}
	originalWorkspace := cfg.Workspaces[0].Path + "-original"
	if err := os.Rename(cfg.Workspaces[0].Path, originalWorkspace); err != nil {
		t.Fatalf("rename workspace: %v", err)
	}
	if err := os.Mkdir(cfg.Workspaces[0].Path, 0o700); err != nil {
		t.Fatalf("replace workspace: %v", err)
	}
	key := childKey{runnerID: cfg.Workspaces[0].RunnerID, threadID: testThreadID}
	if err := localBroker.launchChild(context.Background(), key, "medium"); err == nil || !strings.Contains(err.Error(), "revalidate workspace") {
		t.Fatalf("launchChild workspace error = %v", err)
	}
	localBroker.mu.Lock()
	launchCount := len(localBroker.launches)
	childCount := len(localBroker.children)
	localBroker.mu.Unlock()
	if launchCount != 0 || childCount != 0 {
		t.Fatalf("failed launch retained launches=%d children=%d", launchCount, childCount)
	}
}

func TestDirectChildExitCleansProcessGroup(t *testing.T) {
	fixture := newValidationFixture(t)
	descendantPath := filepath.Join(fixture.root, "descendant-pid")
	releasePath := filepath.Join(fixture.root, "release")
	fakeAmp := filepath.Join(fixture.root, "fake-amp")
	writePrivateFile(t, fakeAmp, []byte(`#!/bin/sh
trap '' HUP
sleep 30 &
printf '%s\n' "$!" > "$TMPDIR/descendant-pid.tmp"
mv "$TMPDIR/descendant-pid.tmp" "$TMPDIR/descendant-pid"
while [ ! -e "$TMPDIR/release" ]; do sleep 0.01; done
exit 0
`), 0o700)
	t.Setenv("TMPDIR", fixture.root)
	fixture.config.AmpBinary = fakeAmp
	cfg, err := loadConfig(fixture.writeConfig(t, fixture.config))
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	localBroker, err := newBroker(cfg, http.DefaultClient)
	if err != nil {
		t.Fatalf("newBroker: %v", err)
	}
	t.Cleanup(func() {
		_ = localBroker.stopAll()
	})
	previousSignal := processGroupSignal
	var directChildPID atomic.Int64
	var leaderOwnedWhenSignaled atomic.Bool
	processGroupSignal = func(pid int, signal syscall.Signal) error {
		if signal == syscall.SIGKILL {
			leaderOwnedWhenSignaled.Store(syscall.Kill(int(directChildPID.Load()), 0) == nil)
		}
		return syscall.Kill(pid, signal)
	}
	t.Cleanup(func() { processGroupSignal = previousSignal })
	key := childKey{runnerID: cfg.Workspaces[0].RunnerID, threadID: testThreadID}
	if err := localBroker.launchChild(context.Background(), key, "medium"); err != nil {
		t.Fatalf("launchChild: %v", err)
	}
	localBroker.mu.Lock()
	child := localBroker.children[key]
	directChildPID.Store(int64(child.cmd.Process.Pid))
	processGroupID := child.processGroupID
	localBroker.mu.Unlock()
	if processGroupID != int(directChildPID.Load()) {
		t.Fatalf("process group = %d, direct child = %d", processGroupID, directChildPID.Load())
	}
	if directChildGroupID, err := syscall.Getpgid(int(directChildPID.Load())); err != nil || directChildGroupID != processGroupID {
		t.Fatalf("direct child process group = %d, err=%v, want %d", directChildGroupID, err, processGroupID)
	}
	waitForPath(t, descendantPath)
	data, err := os.ReadFile(descendantPath)
	if err != nil {
		t.Fatalf("read descendant PID: %v", err)
	}
	descendantPID, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		t.Fatalf("parse descendant PID: %v", err)
	}
	if err := syscall.Kill(descendantPID, 0); err != nil {
		t.Fatalf("descendant was not running before direct child exit: %v", err)
	}
	writePrivateFile(t, releasePath, nil, 0o600)
	waitForNoChildren(t, localBroker)
	waitForProcessExit(t, descendantPID)
	if !leaderOwnedWhenSignaled.Load() {
		t.Fatal("process group was signaled after its leader was reaped")
	}
}

func TestHeartbeatLaunchAndOmittedIntentTeardown(t *testing.T) {
	fixture := newValidationFixture(t)
	capturePath := filepath.Join(fixture.root, "capture.txt")
	readyPath := filepath.Join(fixture.root, "ready")
	fakeAmp := filepath.Join(fixture.root, "fake-amp")
	writePrivateFile(t, fakeAmp, []byte(`#!/bin/sh
{
  printf 'PWD=%s\n' "$PWD"
  printf 'ARG_COUNT=%s\n' "$#"
  index=0
  for argument do
    printf 'ARG%d=%s\n' "$index" "$argument"
    index=$((index + 1))
  done
  for name in AMP_EXECUTOR AMP_URL AMP_API_KEY AMP_THREAD_ID AMP_CURRENT_THREAD_ID AMP_SKIP_UPDATE_CHECK AMP_HEADLESS_OAUTH AMP_REMOTE_CONTROL_TERMINAL AMP_GATEWAY_URL AMP_RUNTIME_URL RIVET_ENDPOINT RIVET_GATEWAY_URL RIVET_PUBLIC_ENDPOINT RIVETKIT_ENGINE_URL RIVET_THREAD_ID RIVET_TOKEN RIVET_NAMESPACE RIVET_POOL AMP_LOG_FILE AMP_PWD BROKER_BASE_VALUE; do
    eval "value=\${$name}"
    printf 'ENV_%s=%s\n' "$name" "$value"
  done
} > "$TMPDIR/capture.txt"
: > "$TMPDIR/ready"
trap 'exit 0' TERM INT
while :; do sleep 1; done
`), 0o700)
	t.Setenv("TMPDIR", fixture.root)
	t.Setenv("BROKER_BASE_VALUE", "preserved")
	t.Setenv("AMP_URL", "https://must-be-overridden.invalid")

	var mu sync.Mutex
	var requests []heartbeatRequest
	heartbeatCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != heartbeatEndpoint {
			t.Errorf("request path = %q", request.URL.Path)
		}
		if request.Method != http.MethodPost {
			t.Errorf("request method = %q", request.Method)
		}
		if request.Header.Get("Authorization") != "Bearer top-secret-api-key" {
			t.Errorf("authorization header = %q", request.Header.Get("Authorization"))
		}
		if request.Header.Get("Content-Type") != "application/json" {
			t.Errorf("content type = %q", request.Header.Get("Content-Type"))
		}
		var payload heartbeatRequest
		if err := json.NewDecoder(io.LimitReader(request.Body, 1<<20)).Decode(&payload); err != nil {
			t.Errorf("decode request: %v", err)
			response.WriteHeader(http.StatusBadRequest)
			return
		}
		mu.Lock()
		requests = append(requests, payload)
		heartbeatCount++
		count := heartbeatCount
		mu.Unlock()
		response.Header().Set("Content-Type", "application/json")
		if count == 1 {
			_, _ = fmt.Fprintf(response, `{"ok":true,"runners":[{"runnerId":%q,"intents":[{"threadId":%q,"desired":"running","agentMode":"deep","reasoningEffort":"xhigh"}]}]}`, payload.Runners[0].RunnerID, testThreadID)
			return
		}
		if count == 2 {
			_, _ = io.WriteString(response, `{"ok":false,"error":"internal-secret-message"}`)
			return
		}
		_, _ = io.WriteString(response, `{"ok":true,"runners":[]}`)
	}))
	defer server.Close()

	fixture.config.APIURL = server.URL
	fixture.config.RuntimeURL = server.URL
	fixture.config.AllowInsecureLoopback = true
	fixture.config.AmpBinary = fakeAmp
	cfg, err := loadConfig(fixture.writeConfig(t, fixture.config))
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	localBroker, err := newBroker(cfg, server.Client())
	if err != nil {
		t.Fatalf("newBroker: %v", err)
	}
	t.Cleanup(func() {
		_ = localBroker.stopAll()
	})
	localBroker.sessionID = "test-session"
	localBroker.hostname = "test-host"
	localBroker.pid = 12345

	if err := localBroker.performHeartbeat(context.Background()); err != nil {
		t.Fatalf("first heartbeat: %v", err)
	}
	waitForPath(t, readyPath)
	captured := readKeyValues(t, capturePath)
	logPath := filepath.Join(cfg.LogDirectory, cfg.Workspaces[0].RunnerID+"-"+testThreadID+".log")
	wantValues := map[string]string{
		"PWD":                             cfg.Workspaces[0].Path,
		"ARG_COUNT":                       "7",
		"ARG0":                            "--mode",
		"ARG1":                            "high",
		"ARG2":                            "--headless=" + testThreadID,
		"ARG3":                            "--runner-id",
		"ARG4":                            brokerPublicationRunnerID(cfg.BrokerID, localBroker.sessionID, localBroker.sessionGeneration, cfg.Workspaces[0].RunnerID, testThreadID),
		"ARG5":                            "--log-file",
		"ARG6":                            logPath,
		"ENV_AMP_EXECUTOR":                "1",
		"ENV_AMP_URL":                     server.URL,
		"ENV_AMP_API_KEY":                 "top-secret-api-key",
		"ENV_AMP_THREAD_ID":               testThreadID,
		"ENV_AMP_CURRENT_THREAD_ID":       testThreadID,
		"ENV_AMP_SKIP_UPDATE_CHECK":       "1",
		"ENV_AMP_HEADLESS_OAUTH":          "1",
		"ENV_AMP_REMOTE_CONTROL_TERMINAL": "1",
		"ENV_AMP_GATEWAY_URL":             server.URL,
		"ENV_AMP_RUNTIME_URL":             server.URL,
		"ENV_RIVET_ENDPOINT":              server.URL,
		"ENV_RIVET_GATEWAY_URL":           server.URL,
		"ENV_RIVET_PUBLIC_ENDPOINT":       server.URL,
		"ENV_RIVETKIT_ENGINE_URL":         server.URL,
		"ENV_RIVET_THREAD_ID":             testThreadID,
		"ENV_RIVET_TOKEN":                 localRuntimeToken,
		"ENV_RIVET_NAMESPACE":             localRuntimeNamespace,
		"ENV_RIVET_POOL":                  localRuntimePool,
		"ENV_AMP_LOG_FILE":                logPath,
		"ENV_AMP_PWD":                     cfg.Workspaces[0].Path,
		"ENV_BROKER_BASE_VALUE":           "",
	}
	for key, want := range wantValues {
		if got := captured[key]; got != want {
			t.Errorf("%s = %q, want %q", key, got, want)
		}
	}
	for index := 0; index < 5; index++ {
		if strings.Contains(captured[fmt.Sprintf("ARG%d", index)], cfg.apiKey) {
			t.Fatal("API key appeared in child arguments")
		}
	}
	logInfo, err := os.Stat(logPath)
	if err != nil {
		t.Fatalf("stat log: %v", err)
	}
	if logInfo.Mode().Perm() != 0o600 {
		t.Errorf("log mode = %o", logInfo.Mode().Perm())
	}

	if err := localBroker.performHeartbeat(context.Background()); err == nil || strings.Contains(err.Error(), "internal-secret-message") {
		t.Fatalf("failed heartbeat error = %v", err)
	}
	localBroker.mu.Lock()
	runningAfterFailure := len(localBroker.children)
	localBroker.mu.Unlock()
	if runningAfterFailure != 1 {
		t.Fatalf("children after failed heartbeat = %d", runningAfterFailure)
	}
	if err := localBroker.performHeartbeat(context.Background()); err != nil {
		t.Fatalf("third heartbeat: %v", err)
	}
	waitForNoChildren(t, localBroker)
	mu.Lock()
	gotRequests := append([]heartbeatRequest(nil), requests...)
	mu.Unlock()
	if len(gotRequests) != 3 {
		t.Fatalf("heartbeat count = %d", len(gotRequests))
	}
	first := gotRequests[0]
	if first.BrokerID != cfg.BrokerID || first.SessionID != "test-session" || first.SessionGeneration != 1 || first.Hostname != "test-host" || first.PID != 12345 {
		t.Errorf("first heartbeat identity = %+v", first)
	}
	if len(first.Runners) != 1 {
		t.Fatalf("first heartbeat runners = %+v", first.Runners)
	}
	if first.Runners[0].RunnerID != cfg.Workspaces[0].RunnerID || first.Runners[0].WorkingDirectory != cfg.Workspaces[0].Path || first.Runners[0].RepositoryURL != cfg.Workspaces[0].RepositoryURL || len(first.Runners[0].RunningThreads) != 0 {
		t.Errorf("first runner payload = %+v", first.Runners[0])
	}
	if !reflect.DeepEqual(gotRequests[1].Runners[0].RunningThreads, []string{testThreadID}) || !reflect.DeepEqual(gotRequests[2].Runners[0].RunningThreads, []string{testThreadID}) {
		t.Errorf("second heartbeat running threads = %+v", gotRequests[1].Runners[0].RunningThreads)
	}
}

func newValidationFixture(t *testing.T) validationFixture {
	t.Helper()
	root := t.TempDir()
	home := filepath.Join(root, "home")
	workspace := filepath.Join(home, "Developer", "project")
	if err := os.MkdirAll(workspace, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(home, ".config"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", home)
	initGitRepository(t, workspace)
	apiKeyFile := filepath.Join(root, "api-key")
	writePrivateFile(t, apiKeyFile, []byte("top-secret-api-key\n"), 0o600)
	ampBinary := filepath.Join(root, "amp")
	writePrivateFile(t, ampBinary, []byte("#!/bin/sh\nexit 0\n"), 0o700)
	return validationFixture{
		root:       root,
		home:       home,
		workspace:  workspace,
		apiKeyFile: apiKeyFile,
		ampBinary:  ampBinary,
		config: brokerConfig{
			Version:          configVersion,
			BrokerID:         "broker-one",
			APIURL:           "https://gateway.example/",
			RuntimeURL:       "https://runtime.example/",
			APIKeyFile:       apiKeyFile,
			AmpBinary:        ampBinary,
			StateDirectory:   filepath.Join(root, "state"),
			LogDirectory:     filepath.Join(root, "logs"),
			HeartbeatSeconds: 0,
			Workspaces: []workspaceConfig{{
				ID:            "workspace-one",
				Path:          workspace,
				RepositoryURL: "https://example.com/repository.git",
			}},
		},
	}
}

func (fixture validationFixture) writeConfig(t *testing.T, cfg brokerConfig) string {
	t.Helper()
	path := filepath.Join(fixture.root, fmt.Sprintf("config-%d.json", time.Now().UnixNano()))
	writePrivateFile(t, path, marshalConfig(t, cfg), 0o600)
	return path
}

func (fixture validationFixture) writeJSON(t *testing.T, value any) string {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(fixture.root, fmt.Sprintf("config-%d.json", time.Now().UnixNano()))
	writePrivateFile(t, path, data, 0o600)
	return path
}

func marshalConfig(t *testing.T, cfg brokerConfig) []byte {
	t.Helper()
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func writePrivateFile(t *testing.T, path string, data []byte, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(path, data, mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
}

func initGitRepository(t *testing.T, path string) {
	t.Helper()
	command := exec.Command("git", "init", "-q", path)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("git init: %v: %s", err, output)
	}
}

func assertLoadConfigError(t *testing.T, path, contains string) {
	t.Helper()
	_, err := loadConfig(path)
	if err == nil || !strings.Contains(err.Error(), contains) {
		t.Fatalf("loadConfig error = %v, want containing %q", err, contains)
	}
}

func waitForPath(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", path)
}

func waitForNoChildren(t *testing.T, localBroker *broker) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		localBroker.mu.Lock()
		count := len(localBroker.children)
		localBroker.mu.Unlock()
		if count == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for child teardown")
}

func waitForProcessExit(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for process %d to exit", pid)
}

func readKeyValues(t *testing.T, path string) map[string]string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	values := make(map[string]string)
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	sort.Strings(lines)
	for _, line := range lines {
		key, value, found := strings.Cut(line, "=")
		if !found {
			t.Fatalf("invalid capture line %q", line)
		}
		values[key] = value
	}
	return values
}

func TestHeartbeatPayloadAdvertisesPluginAgentModesOnlyWhenSupported(t *testing.T) {
	pluginsHome := t.TempDir()
	pluginsDir := filepath.Join(pluginsHome, "amp", "plugins")
	if err := os.MkdirAll(pluginsDir, 0o700); err != nil {
		t.Fatal(err)
	}
	plugin := `// @amp-agent-mode {"key":"deep-blue","label":"Deep Blue","description":"synced mode"}
const agent = amp.createAgent({
  name: "deep-blue",
  model: "openai/gpt-5",
  instructions: "You are deep blue.",
})
amp.registerAgentMode({ key: "deep-blue", label: "Deep Blue", agent })
`
	if err := os.WriteFile(filepath.Join(pluginsDir, "deep-blue.ts"), []byte(plugin), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_CONFIG_HOME", pluginsHome)
	workspace := workspaceConfig{RunnerID: "local-runner-approved", Path: "/workspace"}
	localBroker := &broker{
		config:   &brokerConfig{BrokerID: "broker-one", Workspaces: []workspaceConfig{workspace}},
		children: map[childKey]*childProcess{},
	}
	withoutSupport := localBroker.heartbeatPayload(false)
	if withoutSupport.PluginAgentModes != nil {
		t.Fatalf("plugin agent modes advertised without server support: %#v", withoutSupport.PluginAgentModes)
	}
	localBroker.pluginModesSupport = true
	withSupport := localBroker.heartbeatPayload(false)
	if len(withSupport.PluginAgentModes) != 1 {
		t.Fatalf("advertised plugin agent modes = %#v", withSupport.PluginAgentModes)
	}
	mode := withSupport.PluginAgentModes[0]
	if mode.Key != "deep-blue" || mode.Label != "Deep Blue" || mode.AgentModel != "openai/gpt-5" || mode.AgentInstructions != "You are deep blue." {
		t.Fatalf("advertised plugin agent mode = %#v", mode)
	}
	if mode.PluginName != "deep-blue" || mode.PluginScope != "user" || mode.PluginRepositoryName != "amp-user-plugins" {
		t.Fatalf("advertised plugin metadata = %#v", mode)
	}
	if empty := localBroker.heartbeatPayload(true); empty.PluginAgentModes != nil {
		t.Fatalf("shutdown heartbeat advertised plugin agent modes: %#v", empty.PluginAgentModes)
	}
}

func TestReservedAgentModeKey(t *testing.T) {
	for _, key := range []string{"smart", "rush", "deep", "large", "review", "puck", "low", "medium", "high", "ultra", "deep-1", "deep-2", "deep-3"} {
		if !reservedAgentModeKey(key) {
			t.Fatalf("reservedAgentModeKey(%q) = false, want true", key)
		}
	}
	for _, key := range []string{"", "deep-blue", "kimi-k3", "custom"} {
		if reservedAgentModeKey(key) {
			t.Fatalf("reservedAgentModeKey(%q) = true, want false", key)
		}
	}
}

func TestDiscoverWorkspacesUnderRootsFindsGitRepositories(t *testing.T) {
	base := t.TempDir()
	root, err := canonicalExistingDirectory(base)
	if err != nil {
		t.Fatal(err)
	}
	alpha := filepath.Join(root, "alpha")
	nested := filepath.Join(root, "group", "beta")
	plain := filepath.Join(root, "notes")
	hidden := filepath.Join(root, ".cache", "gamma")
	pinned := filepath.Join(root, "pinned")
	invalid := filepath.Join(root, "invalid")
	for _, directory := range []string{alpha, nested, plain, hidden, pinned, invalid} {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	for _, repository := range []string{alpha, nested, hidden, pinned} {
		initGitRepository(t, repository)
	}
	if err := os.Mkdir(filepath.Join(invalid, ".git"), 0o700); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("git", "-C", alpha, "remote", "add", "origin", "https://example.com/alpha.git")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("add alpha remote: %v: %s", err, output)
	}
	cfg := &brokerConfig{
		BrokerID:       "broker-one",
		Workspaces:     []workspaceConfig{{ID: "pinned", Path: pinned}},
		WorkspaceRoots: []string{root},
	}
	discovered := discoverWorkspacesUnderRoots(cfg)
	if len(discovered) != 2 {
		t.Fatalf("discovered = %#v, want alpha and group/beta only", discovered)
	}
	if discovered[0].Path != alpha || discovered[1].Path != nested {
		t.Fatalf("discovered order = %#v", discovered)
	}
	if discovered[0].ID != "alpha" || discovered[1].ID != "group-beta" {
		t.Fatalf("discovered IDs = %q, %q", discovered[0].ID, discovered[1].ID)
	}
	if discovered[0].RepositoryURL != "https://example.com/alpha.git" || discovered[1].RepositoryURL != "" {
		t.Fatalf("discovered repositoryURLs = %q, %q", discovered[0].RepositoryURL, discovered[1].RepositoryURL)
	}
	if discovered[0].RunnerID != stableRunnerID("broker-one", "alpha", alpha) {
		t.Fatalf("alpha runner ID = %q", discovered[0].RunnerID)
	}
	for _, workspace := range discovered {
		if err := validateIdentifier("workspace id", workspace.ID); err != nil {
			t.Fatalf("derived ID %q invalid: %v", workspace.ID, err)
		}
		if workspace.info == nil {
			t.Fatalf("discovered workspace %q missing file info", workspace.ID)
		}
	}
	again := discoverWorkspacesUnderRoots(cfg)
	if len(again) != len(discovered) || again[0].RunnerID != discovered[0].RunnerID || again[1].RunnerID != discovered[1].RunnerID {
		t.Fatal("discovery is not stable across scans")
	}
}

func TestDiscoverWorkspacesUnderRootsIgnoresSymlinkEscapes(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "approved")
	external := filepath.Join(base, "external")
	for _, directory := range []string{root, external} {
		if err := os.Mkdir(directory, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	root, err := canonicalExistingDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	external, err = canonicalExistingDirectory(external)
	if err != nil {
		t.Fatal(err)
	}
	initGitRepository(t, external)
	if err := os.Symlink(external, filepath.Join(root, "external-repository")); err != nil {
		t.Fatal(err)
	}

	discovered := discoverWorkspacesUnderRoots(&brokerConfig{BrokerID: "broker-one", WorkspaceRoots: []string{root}})
	if len(discovered) != 0 {
		t.Fatalf("discovered workspace outside approved root = %#v", discovered)
	}
	if canonicalPathWithinRoot(root, external) {
		t.Fatalf("external repository %q classified beneath %q", external, root)
	}
}

func TestDiscoverWorkspacesUnderRootsIncludesRootAndHonorsDepth(t *testing.T) {
	t.Run("configured root", func(t *testing.T) {
		root, err := canonicalExistingDirectory(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		initGitRepository(t, root)
		discovered := discoverWorkspacesUnderRoots(&brokerConfig{BrokerID: "broker-one", WorkspaceRoots: []string{root}})
		if len(discovered) != 1 || discovered[0].Path != root {
			t.Fatalf("discovered root = %#v", discovered)
		}
	})

	t.Run("explicit configured root", func(t *testing.T) {
		root, err := canonicalExistingDirectory(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		initGitRepository(t, root)
		child := filepath.Join(root, "nested")
		if err := os.MkdirAll(child, 0o700); err != nil {
			t.Fatal(err)
		}
		initGitRepository(t, child)
		discovered := discoverWorkspacesUnderRoots(&brokerConfig{
			BrokerID:       "broker-one",
			Workspaces:     []workspaceConfig{{ID: "explicit-root", Path: root}},
			WorkspaceRoots: []string{root},
		})
		if len(discovered) != 1 || discovered[0].Path != child {
			t.Fatalf("explicit-root discovery = %#v, want only %s", discovered, child)
		}
	})

	t.Run("bounded depth", func(t *testing.T) {
		root, err := canonicalExistingDirectory(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		depthThree := filepath.Join(root, "one", "two", "three")
		depthFour := filepath.Join(root, "a", "b", "c", "four")
		for _, repository := range []string{depthThree, depthFour} {
			if err := os.MkdirAll(repository, 0o700); err != nil {
				t.Fatal(err)
			}
			initGitRepository(t, repository)
		}
		discovered := discoverWorkspacesUnderRoots(&brokerConfig{BrokerID: "broker-one", WorkspaceRoots: []string{root}})
		if len(discovered) != 1 || discovered[0].Path != depthThree {
			t.Fatalf("depth-bounded discovery = %#v, want only %s", discovered, depthThree)
		}
	})
}

func TestDiscoverWorkspacesUnderRootsHonorsTotalWorkspaceLimit(t *testing.T) {
	root, err := canonicalExistingDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"alpha", "beta"} {
		repository := filepath.Join(root, name)
		if err := os.MkdirAll(repository, 0o700); err != nil {
			t.Fatal(err)
		}
		initGitRepository(t, repository)
	}
	workspaces := make([]workspaceConfig, workspaceLimit-1)
	for index := range workspaces {
		workspaces[index] = workspaceConfig{ID: fmt.Sprintf("explicit-%d", index), Path: fmt.Sprintf("/explicit/%d", index)}
	}
	cfg := &brokerConfig{BrokerID: "broker-one", Workspaces: workspaces, WorkspaceRoots: []string{root}}
	if discovered := discoverWorkspacesUnderRoots(cfg); len(discovered) != 1 {
		t.Fatalf("discovered workspaces = %d, want 1 with %d explicit workspaces", len(discovered), len(workspaces))
	}
	cfg.Workspaces = append(cfg.Workspaces, workspaceConfig{ID: "explicit-last", Path: "/explicit/last"})
	if discovered := discoverWorkspacesUnderRoots(cfg); len(discovered) != 0 {
		t.Fatalf("discovered workspaces = %d, want 0 at total limit", len(discovered))
	}
}

func TestHeartbeatPayloadIncludesDiscoveredWorkspaces(t *testing.T) {
	base := t.TempDir()
	root, err := canonicalExistingDirectory(base)
	if err != nil {
		t.Fatal(err)
	}
	repository := filepath.Join(root, "laminar")
	if err := os.MkdirAll(repository, 0o700); err != nil {
		t.Fatal(err)
	}
	initGitRepository(t, repository)
	cfg := &brokerConfig{
		BrokerID:       "broker-one",
		Workspaces:     []workspaceConfig{{ID: "explicit", Path: "/workspace", RunnerID: "local-runner-explicit"}},
		WorkspaceRoots: []string{root},
	}
	localBroker := &broker{config: cfg, children: map[childKey]*childProcess{}}
	payload := localBroker.heartbeatPayload(false)
	if len(payload.Runners) != 2 {
		t.Fatalf("payload runners = %#v", payload.Runners)
	}
	var discoveredRunner *heartbeatRunner
	for index := range payload.Runners {
		if payload.Runners[index].WorkingDirectory == repository {
			discoveredRunner = &payload.Runners[index]
		}
	}
	if discoveredRunner == nil {
		t.Fatalf("payload missing discovered runner: %#v", payload.Runners)
	}
	if discoveredRunner.RunningThreads == nil {
		t.Fatal("discovered runner has nil runningThreads")
	}
	intents := []heartbeatIntent{}
	runners := []heartbeatRunnerIntents{{RunnerID: discoveredRunner.RunnerID, Intents: &intents}}
	if err := localBroker.validateHeartbeatResponse(heartbeatResponse{OK: true, Runners: &runners}); err != nil {
		t.Fatalf("discovered runner rejected by response validation: %v", err)
	}
	if localBroker.workspaceByRunnerID(discoveredRunner.RunnerID) == nil {
		t.Fatal("discovered runner not dispatchable")
	}
}

func TestLoadConfigWorkspaceRoots(t *testing.T) {
	fixture := newValidationFixture(t)
	developerRoot := filepath.Join(fixture.home, "Developer", "projects")
	if err := os.Mkdir(developerRoot, 0o700); err != nil {
		t.Fatal(err)
	}

	t.Run("roots only", func(t *testing.T) {
		cfg := fixture.config
		cfg.Workspaces = nil
		cfg.WorkspaceRoots = []string{developerRoot}
		loaded, err := loadConfig(fixture.writeConfig(t, cfg))
		if err != nil {
			t.Fatalf("loadConfig with roots only: %v", err)
		}
		if len(loaded.WorkspaceRoots) != 1 || loaded.WorkspaceRoots[0] == "" {
			t.Fatalf("workspaceRoots = %#v", loaded.WorkspaceRoots)
		}
	})

	t.Run("duplicate roots", func(t *testing.T) {
		cfg := fixture.config
		cfg.WorkspaceRoots = []string{developerRoot, developerRoot}
		assertLoadConfigError(t, fixture.writeConfig(t, cfg), "duplicate workspaceRoot")
	})

	t.Run("missing root", func(t *testing.T) {
		cfg := fixture.config
		cfg.WorkspaceRoots = []string{filepath.Join(fixture.root, "absent")}
		assertLoadConfigError(t, fixture.writeConfig(t, cfg), "workspaceRoot")
	})

	t.Run("broad root", func(t *testing.T) {
		cfg := fixture.config
		cfg.Workspaces = nil
		cfg.WorkspaceRoots = []string{fixture.home}
		path := fixture.writeConfig(t, cfg)
		assertLoadConfigError(t, path, "use an explicit workspace with allowBroadRoot")
	})

	t.Run("empty workspaces and roots", func(t *testing.T) {
		cfg := fixture.config
		cfg.Workspaces = nil
		cfg.WorkspaceRoots = nil
		assertLoadConfigError(t, fixture.writeConfig(t, cfg), "must not be empty")
	})
}
