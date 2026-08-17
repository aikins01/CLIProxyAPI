//go:build darwin

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/ampplugins"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/orbconfig"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
)

const (
	configVersion              = 1
	defaultHeartbeat           = 15
	maxHeartbeatSeconds        = 60
	heartbeatRequestLimit      = 30 * time.Second
	shutdownRequestLimit       = 5 * time.Second
	maxConfigBody              = 64 * 1024
	maxAPIKeyBody              = 16 * 1024
	maxHeartbeatBody           = 1 << 20
	maxGenerationBody          = 21
	maxJSONDepth               = 64
	identifierLimit            = 128
	heartbeatMessageLimit      = 512
	gitDiagnosticLimit         = 2048
	hostnameLimit              = 255
	URLLimit                   = 2048
	repositoryURLLimit         = 4096
	workspaceLimit             = 128
	workspaceRootLimit         = 8
	workspaceDiscoveryDepth    = 3
	workspaceDiscoveryInterval = time.Minute
	intentLimit                = 4096
	controlReconnectMin        = time.Second
	controlReconnectMax        = 15 * time.Second
	controlPingInterval        = 30 * time.Second
	maxHeartbeatInterval       = 15 * time.Second
	controlMessageType         = "broker_intents"
	stopGracePeriod            = 3 * time.Second
	killGracePeriod            = time.Second
	heartbeatEndpoint          = "/ampcode/local-broker/heartbeat.json"
	controlEndpoint            = "/ampcode/local-broker/control.ws"
	controlBrokerIDHeader      = "X-Cliproxy-Local-Broker-ID"
	controlSessionIDHeader     = "X-Cliproxy-Local-Broker-Session-ID"
	controlGenerationHeader    = "X-Cliproxy-Local-Broker-Session-Generation"
	intentEpochHeader          = "X-Cliproxy-Local-Broker-Intent-Epoch"
	intentRevisionHeader       = "X-Cliproxy-Local-Broker-Intent-Revision"
	runnerIDPrefix             = "local-runner-"
	runnerIDDigestBytes        = 16
	localRuntimeToken          = "local-neo"
	localRuntimeNamespace      = "default"
	localRuntimePool           = "default"
	configurationLockLabel     = "amp-local-broker-"
	sessionGenerationLabel     = "amp-local-broker-session-generation-"
)

var (
	errStaleSession               = errors.New("broker session is stale")
	errOrbConfigDigestUnsupported = errors.New("orb config digest is unsupported")
	threadIDPattern               = regexp.MustCompile(`^T-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	heartbeatCodePattern          = regexp.MustCompile(`^[a-z][a-z0-9_]{1,63}$`)
	orbConfigDigestPattern        = regexp.MustCompile(`^[0-9a-f]{64}$`)
	agentModePattern              = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,127}$`)
	processGroupSignal            = syscall.Kill
	processLockOpenat             = unix.Openat
	processStart                  = func(_ context.Context, command *exec.Cmd) error {
		return command.Start()
	}
)

type brokerConfig struct {
	Version               int                      `json:"version"`
	BrokerID              string                   `json:"brokerId"`
	APIURL                string                   `json:"apiURL"`
	RuntimeURL            string                   `json:"runtimeURL"`
	APIKeyFile            string                   `json:"apiKeyFile"`
	AmpBinary             string                   `json:"ampBinary"`
	StateDirectory        string                   `json:"stateDirectory"`
	LogDirectory          string                   `json:"logDirectory"`
	HeartbeatSeconds      int                      `json:"heartbeatSeconds"`
	AllowInsecureLoopback bool                     `json:"allowInsecureLoopback"`
	OrbCredentials        *orbCredentialSyncConfig `json:"orbCredentials,omitempty"`
	Workspaces            []workspaceConfig        `json:"workspaces"`
	WorkspaceRoots        []string                 `json:"workspaceRoots,omitempty"`
	apiKey                string
	ampBinaryInfo         os.FileInfo
	stateDirectoryInfo    os.FileInfo
	logDirectoryInfo      os.FileInfo
}

type workspaceConfig struct {
	ID             string `json:"id"`
	Path           string `json:"path"`
	RepositoryURL  string `json:"repositoryURL,omitempty"`
	AllowBroadRoot bool   `json:"allowBroadRoot,omitempty"`
	RunnerID       string `json:"-"`
	info           os.FileInfo
}

type heartbeatRequest struct {
	BrokerID          string                 `json:"brokerId"`
	SessionID         string                 `json:"sessionId"`
	SessionGeneration uint64                 `json:"sessionGeneration"`
	Hostname          string                 `json:"hostname"`
	PID               int                    `json:"pid"`
	Runners           []heartbeatRunner      `json:"runners"`
	OrbConfigDigest   *string                `json:"orbConfigDigest,omitempty"`
	PluginAgentModes  []ampplugins.AgentMode `json:"pluginAgentModes,omitempty"`
}

type heartbeatRunner struct {
	RunnerID         string   `json:"runnerId"`
	WorkingDirectory string   `json:"workingDirectory"`
	RepositoryURL    string   `json:"repositoryURL"`
	RunningThreads   []string `json:"runningThreads"`
}

type heartbeatResponse struct {
	OK                      bool                        `json:"ok"`
	Error                   json.RawMessage             `json:"error,omitempty"`
	Message                 string                      `json:"message,omitempty"`
	Runners                 *[]heartbeatRunnerIntents   `json:"runners"`
	RejectedRunners         *[]heartbeatRunnerRejection `json:"rejectedRunners,omitempty"`
	PublishImageRequests    *[]publishImageRequest      `json:"publishImageRequests,omitempty"`
	OrbConfigDigest         *string                     `json:"orbConfigDigest,omitempty"`
	OrbConfigSupport        bool                        `json:"-"`
	OrbCredentialSupport    bool                        `json:"-"`
	OrbCredentialRevision   string                      `json:"-"`
	PluginAgentModesSupport bool                        `json:"-"`
	IntentEpoch             string                      `json:"-"`
	IntentRevision          *uint64                     `json:"-"`
}

type controlMessage struct {
	Type              string                    `json:"type"`
	BrokerID          string                    `json:"brokerId"`
	SessionID         string                    `json:"sessionId"`
	SessionGeneration uint64                    `json:"sessionGeneration"`
	IntentEpoch       string                    `json:"intentEpoch"`
	IntentRevision    uint64                    `json:"intentRevision"`
	Runners           *[]heartbeatRunnerIntents `json:"runners"`
}

type heartbeatRunnerIntents struct {
	RunnerID string             `json:"runnerId"`
	Intents  *[]heartbeatIntent `json:"intents"`
}

type heartbeatRunnerRejection struct {
	RunnerID string `json:"runnerId"`
	Code     string `json:"code"`
	Message  string `json:"message"`
}

type heartbeatIntent struct {
	ThreadID        string `json:"threadId"`
	Desired         string `json:"desired"`
	AgentMode       string `json:"agentMode"`
	ReasoningEffort string `json:"reasoningEffort"`
}

type childKey struct {
	runnerID string
	threadID string
}

type childProcess struct {
	key            childKey
	mode           string
	cmd            *exec.Cmd
	exitWatcher    *processExitWatcher
	processGroupID int
	logFile        *os.File
	done           chan struct{}
	stopping       bool
	lifecycleMu    sync.Mutex
	retired        bool
	cleanupErr     error
	cleanupPending bool
}

type childLaunch struct {
	cancel context.CancelFunc
	done   chan struct{}
}

type broker struct {
	config              *brokerConfig
	client              *http.Client
	requestLimit        time.Duration
	sessionID           string
	sessionGeneration   uint64
	hostname            string
	pid                 int
	workspacesByRunner  map[string]*workspaceConfig
	discoveryMu         sync.RWMutex
	discovered          []workspaceConfig
	lastDiscovery       time.Time
	orbConfigDigest     *string
	orbConfigSupport    bool
	orbCredentials      orbCredentialState
	pluginModesSupport  bool
	mu                  sync.Mutex
	children            map[childKey]*childProcess
	launches            map[childKey]*childLaunch
	publishImageActive  map[string]struct{}
	publishImageErrors  chan error
	publishImageCtx     context.Context
	controlMessages     chan heartbeatResponse
	intentEpoch         string
	intentRevision      uint64
	intentRevisionKnown bool
	publishImageCancel  context.CancelFunc
	publishImageWG      sync.WaitGroup
	shuttingDown        bool
}

type processLock struct {
	file *os.File
}

type processExitWatcher struct {
	fileDescriptor int
}

type boundedDiagnostic struct {
	data []byte
}

func (diagnostic *boundedDiagnostic) Write(data []byte) (int, error) {
	remaining := gitDiagnosticLimit - len(diagnostic.data)
	if remaining > len(data) {
		remaining = len(data)
	}
	if remaining > 0 {
		diagnostic.data = append(diagnostic.data, data[:remaining]...)
	}
	return len(data), nil
}

func newProcessExitWatcher(pid int) (*processExitWatcher, error) {
	fileDescriptor, err := unix.Kqueue()
	if err != nil {
		return nil, err
	}
	watcher := &processExitWatcher{fileDescriptor: fileDescriptor}
	changes := []unix.Kevent_t{{
		Ident:  uint64(pid),
		Filter: unix.EVFILT_PROC,
		Flags:  unix.EV_ADD | unix.EV_ONESHOT,
		Fflags: unix.NOTE_EXIT,
	}}
	if _, err := unix.Kevent(fileDescriptor, changes, nil, nil); err != nil {
		_ = watcher.close()
		return nil, err
	}
	return watcher, nil
}

func (watcher *processExitWatcher) wait() error {
	events := make([]unix.Kevent_t, 1)
	for {
		eventCount, err := unix.Kevent(watcher.fileDescriptor, nil, events, nil)
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if err != nil {
			return err
		}
		if eventCount == 0 {
			continue
		}
		if events[0].Flags&unix.EV_ERROR != 0 && events[0].Data != 0 {
			return syscall.Errno(events[0].Data)
		}
		return nil
	}
}

func (watcher *processExitWatcher) close() error {
	if watcher == nil || watcher.fileDescriptor < 0 {
		return nil
	}
	err := unix.Close(watcher.fileDescriptor)
	watcher.fileDescriptor = -1
	return err
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("amp_local_broker", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", "", "path to the approved local broker JSON config")
	check := flags.Bool("check", false, "validate the config and approved workspaces, prepare the state and log directories, then exit")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintln(stderr, "unexpected positional arguments")
		return 2
	}
	if strings.TrimSpace(*configPath) == "" {
		fmt.Fprintln(stderr, "-config is required")
		return 2
	}

	cfg, err := loadConfig(*configPath)
	if err != nil {
		fmt.Fprintf(stderr, "validate config: %v\n", err)
		return 1
	}
	if *check {
		fmt.Fprintln(stdout, "configuration valid")
		return 0
	}

	lock, err := acquireProcessLock(cfg.StateDirectory, cfg.BrokerID, cfg.stateDirectoryInfo)
	if err != nil {
		fmt.Fprintf(stderr, "acquire broker lock: %v\n", err)
		return 1
	}
	defer func() {
		if errUnlock := lock.Close(); errUnlock != nil {
			fmt.Fprintf(stderr, "release broker lock: %v\n", errUnlock)
		}
	}()

	client := newHTTPClient()
	defer client.CloseIdleConnections()
	localBroker, err := newBroker(cfg, client)
	if err != nil {
		fmt.Fprintf(stderr, "initialize broker: %v\n", err)
		return 1
	}
	if err := localBroker.loop(ctx, stderr); err != nil {
		fmt.Fprintf(stderr, "run broker: %v\n", err)
		return 1
	}
	return 0
}

func loadConfig(path string) (*brokerConfig, error) {
	data, _, err := readSecureRegularFile(path, maxConfigBody)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	if err := rejectDuplicateJSONFields(data); err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}
	var cfg brokerConfig
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, errors.New("config contains trailing JSON")
	}
	if cfg.HeartbeatSeconds == 0 {
		cfg.HeartbeatSeconds = defaultHeartbeat
	}
	if err := validateConfig(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func rejectDuplicateJSONFields(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	var walk func(int) error
	walk = func(depth int) error {
		if depth > maxJSONDepth {
			return fmt.Errorf("JSON exceeds maximum depth %d", maxJSONDepth)
		}
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		delimiter, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		switch delimiter {
		case '{':
			seen := make(map[string]struct{})
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return err
				}
				key, ok := keyToken.(string)
				if !ok {
					return errors.New("object key is not a string")
				}
				// encoding/json binds struct fields case-insensitively, so
				// duplicates must be detected on the folded key as well.
				foldedKey := strings.ToLower(key)
				if _, exists := seen[foldedKey]; exists {
					return fmt.Errorf("duplicate field %q", key)
				}
				seen[foldedKey] = struct{}{}
				if err := walk(depth + 1); err != nil {
					return err
				}
			}
			_, err = decoder.Token()
			return err
		case '[':
			for decoder.More() {
				if err := walk(depth + 1); err != nil {
					return err
				}
			}
			_, err = decoder.Token()
			return err
		default:
			return errors.New("unexpected JSON delimiter")
		}
	}
	if err := walk(0); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return errors.New("contains trailing JSON")
	}
	return nil
}

func validateConfig(cfg *brokerConfig) error {
	if cfg.Version != configVersion {
		return fmt.Errorf("version must be %d", configVersion)
	}
	if err := validateIdentifier("brokerId", cfg.BrokerID); err != nil {
		return err
	}
	if cfg.HeartbeatSeconds < 1 || cfg.HeartbeatSeconds > maxHeartbeatSeconds {
		return fmt.Errorf("heartbeatSeconds must be between 1 and %d", maxHeartbeatSeconds)
	}

	apiURL, err := validateBaseURL("apiURL", cfg.APIURL, cfg.AllowInsecureLoopback)
	if err != nil {
		return err
	}
	runtimeURL, err := validateBaseURL("runtimeURL", cfg.RuntimeURL, cfg.AllowInsecureLoopback)
	if err != nil {
		return err
	}
	cfg.APIURL = apiURL
	cfg.RuntimeURL = runtimeURL

	if strings.TrimSpace(cfg.APIKeyFile) == "" {
		return errors.New("apiKeyFile is required")
	}
	keyData, keyPath, err := readSecureRegularFile(cfg.APIKeyFile, maxAPIKeyBody)
	if err != nil {
		return fmt.Errorf("read apiKeyFile: %w", err)
	}
	cfg.APIKeyFile = keyPath
	cfg.apiKey = strings.TrimSpace(string(keyData))
	if cfg.apiKey == "" {
		return errors.New("apiKeyFile contains an empty key")
	}
	if !validAPIKey(cfg.apiKey) {
		return errors.New("apiKeyFile contains an invalid key")
	}

	ampBinary, err := canonicalExecutable(cfg.AmpBinary)
	if err != nil {
		return fmt.Errorf("validate ampBinary: %w", err)
	}
	cfg.AmpBinary = ampBinary
	ampInfo, err := os.Stat(ampBinary)
	if err != nil {
		return fmt.Errorf("inspect ampBinary: %w", err)
	}
	cfg.ampBinaryInfo = ampInfo

	if len(cfg.Workspaces) == 0 && len(cfg.WorkspaceRoots) == 0 {
		return errors.New("workspaces or workspaceRoots must not be empty")
	}
	if len(cfg.WorkspaceRoots) > workspaceRootLimit {
		return fmt.Errorf("workspaceRoots exceeds %d entries", workspaceRootLimit)
	}
	seenRoots := make(map[string]struct{}, len(cfg.WorkspaceRoots))
	for index := range cfg.WorkspaceRoots {
		canonicalRoot, err := canonicalExistingDirectory(cfg.WorkspaceRoots[index])
		if err != nil {
			return fmt.Errorf("validate workspaceRoot: %w", err)
		}
		broad, err := isBroadWorkspaceRoot(canonicalRoot)
		if err != nil {
			return fmt.Errorf("validate workspaceRoot %q broad root: %w", canonicalRoot, err)
		}
		if broad {
			return fmt.Errorf("workspaceRoot %q is a broad root; use an explicit workspace with allowBroadRoot to approve it", canonicalRoot)
		}
		if _, exists := seenRoots[canonicalRoot]; exists {
			return fmt.Errorf("duplicate workspaceRoot %q", canonicalRoot)
		}
		seenRoots[canonicalRoot] = struct{}{}
		cfg.WorkspaceRoots[index] = canonicalRoot
	}
	if len(cfg.Workspaces) > workspaceLimit {
		return fmt.Errorf("workspaces exceeds %d entries", workspaceLimit)
	}
	seenIDs := make(map[string]struct{}, len(cfg.Workspaces))
	seenPaths := make(map[string]struct{}, len(cfg.Workspaces))
	for index := range cfg.Workspaces {
		workspace := &cfg.Workspaces[index]
		if err := validateIdentifier("workspace id", workspace.ID); err != nil {
			return err
		}
		if _, exists := seenIDs[workspace.ID]; exists {
			return fmt.Errorf("duplicate workspace id %q", workspace.ID)
		}
		seenIDs[workspace.ID] = struct{}{}
		canonicalPath, err := canonicalWorkspace(workspace.Path)
		if err != nil {
			return fmt.Errorf("validate workspace %q: %w", workspace.ID, err)
		}
		if _, exists := seenPaths[canonicalPath]; exists {
			return fmt.Errorf("duplicate workspace path %q", canonicalPath)
		}
		seenPaths[canonicalPath] = struct{}{}
		if !workspace.AllowBroadRoot {
			broad, err := isBroadWorkspaceRoot(canonicalPath)
			if err != nil {
				return fmt.Errorf("validate workspace %q broad root: %w", workspace.ID, err)
			}
			if broad {
				return fmt.Errorf("workspace %q is a broad root; set allowBroadRoot to approve it explicitly", workspace.ID)
			}
		}
		workspace.RepositoryURL = strings.TrimSpace(workspace.RepositoryURL)
		if len(workspace.RepositoryURL) > repositoryURLLimit {
			return fmt.Errorf("workspace %q repositoryURL exceeds %d bytes", workspace.ID, repositoryURLLimit)
		}
		if err := validateRepositoryURL(workspace.RepositoryURL); err != nil {
			return fmt.Errorf("workspace %q repositoryURL: %w", workspace.ID, err)
		}
		workspaceInfo, err := os.Stat(canonicalPath)
		if err != nil {
			return fmt.Errorf("inspect workspace %q: %w", workspace.ID, err)
		}
		workspace.Path = canonicalPath
		workspace.RunnerID = stableRunnerID(cfg.BrokerID, workspace.ID, canonicalPath)
		workspace.info = workspaceInfo
	}

	stateDirectory, err := createPrivateDirectory(cfg.StateDirectory)
	if err != nil {
		return fmt.Errorf("create stateDirectory: %w", err)
	}
	logDirectory, err := createPrivateDirectory(cfg.LogDirectory)
	if err != nil {
		return fmt.Errorf("create logDirectory: %w", err)
	}
	cfg.StateDirectory = stateDirectory
	cfg.LogDirectory = logDirectory
	stateDirectoryInfo, err := os.Stat(stateDirectory)
	if err != nil {
		return fmt.Errorf("inspect stateDirectory: %w", err)
	}
	logDirectoryInfo, err := os.Stat(logDirectory)
	if err != nil {
		return fmt.Errorf("inspect logDirectory: %w", err)
	}
	cfg.stateDirectoryInfo = stateDirectoryInfo
	cfg.logDirectoryInfo = logDirectoryInfo
	return nil
}

func validateIdentifier(name, value string) error {
	if value == "" {
		return fmt.Errorf("%s is required", name)
	}
	if len(value) > identifierLimit {
		return fmt.Errorf("%s exceeds %d bytes", name, identifierLimit)
	}
	for index, character := range []byte(value) {
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || index > 0 && (character == '.' || character == '_' || character == '-') {
			continue
		}
		return fmt.Errorf("%s is not a safe opaque identifier", name)
	}
	return nil
}

func validateRepositoryURL(raw string) error {
	if containsControl(raw) {
		return errors.New("contains control characters")
	}
	if raw == "" || !strings.Contains(raw, "://") {
		return nil
	}
	parsed, err := url.Parse(raw)
	if err != nil || !parsed.IsAbs() || parsed.Host == "" {
		return errors.New("must be a valid absolute URL")
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" {
		return errors.New("must not contain userinfo, query, or fragment")
	}
	return nil
}

func validAPIKey(value string) bool {
	for _, character := range []byte(value) {
		if character <= 0x20 || character >= 0x7f {
			return false
		}
	}
	return true
}

func validateBaseURL(name, raw string, allowInsecureLoopback bool) (string, error) {
	raw = strings.TrimSpace(raw)
	if len(raw) == 0 || len(raw) > URLLimit || containsControl(raw) {
		return "", fmt.Errorf("%s must be a non-empty URL of at most %d bytes", name, URLLimit)
	}
	parsed, err := url.Parse(raw)
	if err != nil || !parsed.IsAbs() || parsed.Host == "" || parsed.Opaque != "" {
		return "", fmt.Errorf("%s must be an absolute HTTP(S) URL", name)
	}
	scheme := strings.ToLower(parsed.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", fmt.Errorf("%s must use HTTP or HTTPS", name)
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" || strings.Contains(raw, "#") {
		return "", fmt.Errorf("%s must not contain userinfo, query, or fragment", name)
	}
	if parsed.Path != "" && parsed.Path != "/" || parsed.RawPath != "" {
		return "", fmt.Errorf("%s must not contain a path", name)
	}
	hostname := parsed.Hostname()
	if hostname == "" {
		return "", fmt.Errorf("%s must contain a hostname", name)
	}
	if parsed.Port() != "" {
		port, err := strconv.Atoi(parsed.Port())
		if err != nil || port < 1 || port > 65535 {
			return "", fmt.Errorf("%s contains an invalid port", name)
		}
	} else if strings.HasSuffix(parsed.Host, ":") {
		return "", fmt.Errorf("%s contains an invalid port", name)
	}
	if scheme == "http" && !allowInsecureLoopback {
		return "", fmt.Errorf("%s requires HTTPS unless insecure loopback is explicitly allowed", name)
	}
	if scheme == "http" && !isLoopbackHost(hostname) {
		return "", fmt.Errorf("%s may use HTTP only with localhost or a loopback address; use HTTPS for non-loopback hosts", name)
	}
	return scheme + "://" + parsed.Host, nil
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	address := net.ParseIP(host)
	return address != nil && address.IsLoopback()
}

func readSecureRegularFile(path string, limit int64) ([]byte, string, error) {
	if strings.TrimSpace(path) == "" {
		return nil, "", errors.New("path is required")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, "", err
	}
	info, err := os.Lstat(absolute)
	if err != nil {
		return nil, "", err
	}
	if err := validatePrivateFileInfo(info); err != nil {
		return nil, "", err
	}
	file, err := os.OpenFile(absolute, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, "", err
	}
	defer func() {
		if errClose := file.Close(); errClose != nil {
			logrus.WithError(errClose).WithField("path", absolute).Error("close secure file")
		}
	}()
	openedInfo, err := file.Stat()
	if err != nil {
		return nil, "", err
	}
	if err := validatePrivateFileInfo(openedInfo); err != nil || !os.SameFile(info, openedInfo) {
		return nil, "", errors.New("file changed while being opened")
	}
	if openedInfo.Size() > limit {
		return nil, "", fmt.Errorf("file exceeds %d bytes", limit)
	}
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, "", err
	}
	if int64(len(data)) > limit {
		return nil, "", fmt.Errorf("file exceeds %d bytes", limit)
	}
	finalInfo, err := file.Stat()
	if err != nil {
		return nil, "", err
	}
	if err := validatePrivateFileInfo(finalInfo); err != nil || !os.SameFile(openedInfo, finalInfo) || openedInfo.Size() != finalInfo.Size() || !openedInfo.ModTime().Equal(finalInfo.ModTime()) {
		return nil, "", errors.New("file changed while being read")
	}
	return data, absolute, nil
}

func validatePrivateFileInfo(info os.FileInfo) error {
	if !info.Mode().IsRegular() {
		return errors.New("file must be regular")
	}
	if info.Mode().Perm()&0o077 != 0 {
		return errors.New("file permissions must not grant group or world access")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Uid) != os.Geteuid() {
		return errors.New("file must be owned by the current user")
	}
	return nil
}

func canonicalExecutable(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", errors.New("path is required")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	canonical, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(canonical)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() {
		return "", errors.New("binary must be a regular file")
	}
	if info.Mode().Perm()&0o111 == 0 {
		return "", errors.New("binary must be executable")
	}
	if info.Mode().Perm()&0o022 != 0 {
		return "", errors.New("binary must not be group or world writable")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != 0 && int(stat.Uid) != os.Geteuid() {
		return "", errors.New("binary must be owned by root or the current user")
	}
	return canonical, nil
}

func canonicalWorkspace(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", errors.New("path is required")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	canonical, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(canonical)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", errors.New("path must be a directory")
	}
	gitPath, err := exec.LookPath("git")
	if err != nil {
		return "", fmt.Errorf("locate git: %w", err)
	}
	gitPath, err = canonicalExecutable(gitPath)
	if err != nil {
		return "", fmt.Errorf("validate git executable: %w", err)
	}
	command := exec.Command(gitPath, "-C", canonical, "rev-parse", "--show-toplevel")
	command.Env = gitValidationEnvironment(os.Environ())
	var diagnostic boundedDiagnostic
	command.Stderr = &diagnostic
	output, err := command.Output()
	if err != nil {
		detail := strings.Join(strings.Fields(strings.ToValidUTF8(string(diagnostic.data), "?")), " ")
		if detail != "" {
			return "", fmt.Errorf("git rev-parse --show-toplevel failed: %w: %s", err, strconv.QuoteToASCII(detail))
		}
		return "", fmt.Errorf("git rev-parse --show-toplevel failed: %w", err)
	}
	root := strings.TrimSpace(string(output))
	if root == "" {
		return "", errors.New("git returned an empty repository root")
	}
	rootAbsolute, err := filepath.Abs(root)
	if err != nil {
		return "", fmt.Errorf("canonicalize git repository root: %w", err)
	}
	rootCanonical, err := filepath.EvalSymlinks(rootAbsolute)
	if err != nil {
		return "", fmt.Errorf("canonicalize git repository root: %w", err)
	}
	if rootCanonical != canonical {
		return "", fmt.Errorf("path is not the repository root")
	}
	return canonical, nil
}

func gitValidationEnvironment(base []string) []string {
	environment := make([]string, 0, len(base)+2)
	for _, entry := range base {
		key, _, found := strings.Cut(entry, "=")
		if found && strings.HasPrefix(key, "GIT_") {
			continue
		}
		environment = append(environment, entry)
	}
	return append(environment, "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null")
}

func isBroadWorkspaceRoot(path string) (bool, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return false, err
	}
	home, err = canonicalExistingDirectory(home)
	if err != nil {
		return false, err
	}
	if path == filepath.VolumeName(path)+string(filepath.Separator) || path == home {
		return true, nil
	}
	relative, err := filepath.Rel(home, path)
	if err != nil {
		return false, err
	}
	if relative != "." && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) && !strings.Contains(relative, string(filepath.Separator)) {
		return true, nil
	}
	homeRelative, err := filepath.Rel(path, home)
	if err != nil {
		return false, err
	}
	return homeRelative == "." || homeRelative != ".." && !strings.HasPrefix(homeRelative, ".."+string(filepath.Separator)), nil
}

func canonicalExistingDirectory(path string) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	canonical, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(canonical)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", errors.New("path is not a directory")
	}
	return canonical, nil
}

func canonicalPathWithinRoot(root, path string) bool {
	relative, err := filepath.Rel(root, path)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func createPrivateDirectory(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", errors.New("path is required")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(absolute, 0o700); err != nil {
		return "", err
	}
	pathInfo, err := os.Lstat(absolute)
	if err != nil {
		return "", err
	}
	if !pathInfo.IsDir() || pathInfo.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("path must be a directory and not a symbolic link")
	}
	canonical, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(canonical)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", errors.New("path must be a directory")
	}
	if err := validatePrivateDirectoryInfo(info); err != nil {
		return "", err
	}
	if err := os.Chmod(canonical, 0o700); err != nil {
		return "", err
	}
	return canonical, nil
}

func validatePrivateDirectoryInfo(info os.FileInfo) error {
	if !info.IsDir() {
		return errors.New("path must be a directory")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Uid) != os.Geteuid() {
		return errors.New("directory must be owned by the current user")
	}
	if info.Mode().Perm()&0o077 != 0 {
		return errors.New("directory permissions must not grant group or world access")
	}
	return nil
}

func containsControl(value string) bool {
	for _, character := range value {
		if character < 0x20 || character == 0x7f {
			return true
		}
	}
	return false
}

func stableRunnerID(brokerID, workspaceID, canonicalPath string) string {
	digest := sha256.Sum256([]byte(brokerID + "\x00" + workspaceID + "\x00" + canonicalPath))
	return runnerIDPrefix + hex.EncodeToString(digest[:runnerIDDigestBytes])
}

func discoverWorkspacesUnderRoots(cfg *brokerConfig) []workspaceConfig {
	if cfg == nil || len(cfg.WorkspaceRoots) == 0 {
		return nil
	}
	discoveryLimit := workspaceLimit - len(cfg.Workspaces)
	if discoveryLimit <= 0 {
		return nil
	}
	explicitPaths := make(map[string]struct{}, len(cfg.Workspaces))
	takenPaths := make(map[string]struct{}, len(cfg.Workspaces))
	takenIDs := make(map[string]struct{}, len(cfg.Workspaces))
	for _, workspace := range cfg.Workspaces {
		explicitPaths[workspace.Path] = struct{}{}
		takenPaths[workspace.Path] = struct{}{}
		takenIDs[workspace.ID] = struct{}{}
	}
	discovered := make([]workspaceConfig, 0)
	for _, root := range cfg.WorkspaceRoots {
		scanWorkspaceRoot(root, root, 0, discoveryLimit, explicitPaths, takenPaths, takenIDs, &discovered)
		if len(discovered) >= discoveryLimit {
			break
		}
	}
	sort.Slice(discovered, func(i, j int) bool { return discovered[i].Path < discovered[j].Path })
	for index := range discovered {
		workspace := &discovered[index]
		workspace.RunnerID = stableRunnerID(cfg.BrokerID, workspace.ID, workspace.Path)
	}
	return discovered
}

func scanWorkspaceRoot(root, directory string, depth, limit int, explicitPaths, takenPaths, takenIDs map[string]struct{}, discovered *[]workspaceConfig) {
	if depth > workspaceDiscoveryDepth || len(*discovered) >= limit {
		return
	}
	_, explicit := explicitPaths[directory]
	if _, exists := takenPaths[directory]; exists && !explicit {
		return
	}
	if _, err := os.Lstat(filepath.Join(directory, ".git")); err == nil {
		if canonical, workspaceErr := canonicalWorkspace(directory); workspaceErr == nil {
			if _, exists := takenPaths[canonical]; !exists {
				takenPaths[canonical] = struct{}{}
				workspace := workspaceConfig{Path: canonical, RepositoryURL: readWorkspaceRepositoryURL(canonical)}
				workspace.ID = derivedWorkspaceID(root, canonical, takenIDs)
				takenIDs[workspace.ID] = struct{}{}
				if info, statErr := os.Stat(canonical); statErr == nil {
					workspace.info = info
				}
				*discovered = append(*discovered, workspace)
				return
			}
		}
	}
	if depth == workspaceDiscoveryDepth {
		return
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasPrefix(name, ".") || name == "node_modules" {
			continue
		}
		canonical, err := canonicalExistingDirectory(filepath.Join(directory, name))
		if err != nil {
			continue
		}
		if !canonicalPathWithinRoot(root, canonical) {
			continue
		}
		scanWorkspaceRoot(root, canonical, depth+1, limit, explicitPaths, takenPaths, takenIDs, discovered)
		if len(*discovered) >= limit {
			return
		}
	}
}

func derivedWorkspaceID(root, canonicalPath string, takenIDs map[string]struct{}) string {
	relative, err := filepath.Rel(root, canonicalPath)
	if err != nil || relative == "." || strings.HasPrefix(relative, "..") {
		relative = filepath.Base(canonicalPath)
	}
	var slug strings.Builder
	previousDash := true
	for _, character := range strings.ToLower(relative) {
		safe := character >= 'a' && character <= 'z' || character >= '0' && character <= '9' || character == '.' || character == '_'
		if !safe && character != '-' {
			character = '-'
		}
		if character == '-' || character == '.' || character == '_' {
			if previousDash {
				continue
			}
			previousDash = true
			slug.WriteRune(character)
			continue
		}
		previousDash = false
		slug.WriteRune(character)
	}
	identifier := strings.TrimRight(slug.String(), "-._")
	if identifier == "" {
		identifier = "repo"
	}
	if len(identifier) > identifierLimit-9 {
		identifier = strings.TrimRight(identifier[:identifierLimit-9], "-._")
	}
	if _, exists := takenIDs[identifier]; exists {
		digest := sha256.Sum256([]byte(canonicalPath))
		identifier = identifier + "-" + hex.EncodeToString(digest[:4])
	}
	return identifier
}

func readWorkspaceRepositoryURL(workspacePath string) string {
	data, err := os.ReadFile(filepath.Join(workspacePath, ".git", "config"))
	if err != nil || len(data) > 1<<20 {
		return ""
	}
	inOrigin := false
	for _, line := range strings.Split(string(data), "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "[") {
			inOrigin = trimmed == `[remote "origin"]`
			continue
		}
		if !inOrigin {
			continue
		}
		key, value, found := strings.Cut(trimmed, "=")
		if !found || strings.TrimSpace(key) != "url" {
			continue
		}
		candidate := strings.TrimSpace(value)
		if err := validateRepositoryURL(candidate); err == nil {
			return candidate
		}
		return ""
	}
	return ""
}

func (localBroker *broker) refreshDiscoveredWorkspaces() {
	if localBroker == nil || localBroker.config == nil || len(localBroker.config.WorkspaceRoots) == 0 {
		return
	}
	localBroker.discoveryMu.Lock()
	defer localBroker.discoveryMu.Unlock()
	if time.Since(localBroker.lastDiscovery) < workspaceDiscoveryInterval {
		return
	}
	localBroker.lastDiscovery = time.Now()
	localBroker.discovered = discoverWorkspacesUnderRoots(localBroker.config)
}

func (localBroker *broker) discoveredSnapshot() []workspaceConfig {
	localBroker.discoveryMu.RLock()
	defer localBroker.discoveryMu.RUnlock()
	return localBroker.discovered
}

func (localBroker *broker) workspaceByRunnerID(runnerID string) *workspaceConfig {
	if workspace := localBroker.workspacesByRunner[runnerID]; workspace != nil {
		return workspace
	}
	discovered := localBroker.discoveredSnapshot()
	for index := range discovered {
		if discovered[index].RunnerID == runnerID {
			return &discovered[index]
		}
	}
	return nil
}

func (localBroker *broker) approvedWorkspaceCount() int {
	return min(workspaceLimit, len(localBroker.workspacesByRunner)+len(localBroker.discoveredSnapshot()))
}

func acquireProcessLock(stateDirectory, brokerID string, expected ...os.FileInfo) (*processLock, error) {
	if err := validateIdentifier("brokerId", brokerID); err != nil {
		return nil, err
	}
	directoryFD, err := unix.Open(stateDirectory, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("open state directory: %w", err)
	}
	directory := os.NewFile(uintptr(directoryFD), stateDirectory)
	if directory == nil {
		_ = unix.Close(directoryFD)
		return nil, errors.New("open state directory: invalid file descriptor")
	}
	directoryInfo, err := directory.Stat()
	if err != nil {
		_ = directory.Close()
		return nil, fmt.Errorf("inspect state directory: %w", err)
	}
	if err := validatePrivateDirectoryInfo(directoryInfo); err != nil {
		_ = directory.Close()
		return nil, fmt.Errorf("validate state directory: %w", err)
	}
	if len(expected) > 0 && expected[0] != nil && !os.SameFile(expected[0], directoryInfo) {
		_ = directory.Close()
		return nil, errors.New("validate state directory: directory changed after configuration validation")
	}
	lockName := configurationLockLabel + brokerID + ".lock"
	lockFD, err := processLockOpenat(directoryFD, lockName, unix.O_CREAT|unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0o600)
	closeDirectoryErr := directory.Close()
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(lockFD), lockName)
	if file == nil {
		_ = unix.Close(lockFD)
		return nil, errors.New("open broker lock: invalid file descriptor")
	}
	if closeDirectoryErr != nil {
		_ = file.Close()
		return nil, fmt.Errorf("close state directory: %w", closeDirectoryErr)
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if err := validatePrivateFileInfo(info); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("validate lock file: %w", err)
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("another broker process holds the lock: %w", err)
	}
	return &processLock{file: file}, nil
}

func (lock *processLock) Close() error {
	if lock == nil || lock.file == nil {
		return nil
	}
	errUnlock := syscall.Flock(int(lock.file.Fd()), syscall.LOCK_UN)
	errClose := lock.file.Close()
	lock.file = nil
	return errors.Join(errUnlock, errClose)
}

func allocateSessionGeneration(stateDirectory, brokerID string, expected ...os.FileInfo) (uint64, error) {
	if err := validateIdentifier("brokerId", brokerID); err != nil {
		return 0, err
	}
	if err := revalidatePrivateDirectory(stateDirectory, expected...); err != nil {
		return 0, fmt.Errorf("validate state directory: %w", err)
	}
	path := filepath.Join(stateDirectory, sessionGenerationLabel+brokerID)
	current, err := readSessionGeneration(path)
	if err != nil {
		return 0, err
	}
	if current == ^uint64(0) {
		return 0, errors.New("session generation is exhausted")
	}
	next := current + 1
	if err := persistSessionGeneration(path, next); err != nil {
		return 0, err
	}
	return next, nil
}

func revalidatePrivateDirectory(path string, expected ...os.FileInfo) error {
	if len(expected) == 0 || expected[0] == nil {
		return nil
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return errors.New("directory must not be a symlink")
	}
	if err := validatePrivateDirectoryInfo(info); err != nil {
		return err
	}
	if !os.SameFile(expected[0], info) {
		return errors.New("directory changed after configuration validation")
	}
	return nil
}

func readSessionGeneration(path string) (uint64, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("inspect session generation: %w", err)
	}
	if err := validateSessionGenerationFileInfo(info); err != nil {
		return 0, fmt.Errorf("validate session generation: %w", err)
	}
	data, _, err := readSecureRegularFile(path, maxGenerationBody)
	if err != nil {
		return 0, fmt.Errorf("read session generation: %w", err)
	}
	text := strings.TrimSuffix(string(data), "\n")
	if text == "" || string(data) != text && string(data) != text+"\n" {
		return 0, errors.New("session generation is invalid")
	}
	generation, err := strconv.ParseUint(text, 10, 64)
	if err != nil || generation == 0 || strconv.FormatUint(generation, 10) != text {
		return 0, errors.New("session generation is invalid")
	}
	return generation, nil
}

func validateSessionGenerationFileInfo(info os.FileInfo) error {
	if err := validatePrivateFileInfo(info); err != nil {
		return err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Nlink != 1 {
		return errors.New("file must not be hard linked")
	}
	return nil
}

func persistSessionGeneration(path string, generation uint64) error {
	directory := filepath.Dir(path)
	file, err := os.CreateTemp(directory, ".session-generation-")
	if err != nil {
		return fmt.Errorf("create session generation: %w", err)
	}
	temporaryPath := file.Name()
	defer func() {
		_ = file.Close()
		_ = os.Remove(temporaryPath)
	}()
	if err := file.Chmod(0o600); err != nil {
		return fmt.Errorf("secure session generation: %w", err)
	}
	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("inspect session generation: %w", err)
	}
	if err := validateSessionGenerationFileInfo(info); err != nil {
		return fmt.Errorf("validate session generation: %w", err)
	}
	if _, err := io.WriteString(file, strconv.FormatUint(generation, 10)+"\n"); err != nil {
		return fmt.Errorf("write session generation: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync session generation: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close session generation: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("replace session generation: %w", err)
	}
	directoryFile, err := os.Open(directory)
	if err != nil {
		return fmt.Errorf("open state directory: %w", err)
	}
	if err := directoryFile.Sync(); err != nil {
		_ = directoryFile.Close()
		return fmt.Errorf("sync state directory: %w", err)
	}
	if err := directoryFile.Close(); err != nil {
		return fmt.Errorf("close state directory: %w", err)
	}
	return nil
}

func newHTTPClient() *http.Client {
	transport := &http.Transport{
		Proxy:                  http.ProxyFromEnvironment,
		MaxResponseHeaderBytes: maxHeartbeatBody,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout: 10 * time.Second,
	}
	return &http.Client{
		Transport: transport,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

func newBroker(cfg *brokerConfig, client *http.Client) (*broker, error) {
	sessionBytes := make([]byte, 16)
	if _, err := rand.Read(sessionBytes); err != nil {
		return nil, fmt.Errorf("create session ID: %w", err)
	}
	hostname, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("read hostname: %w", err)
	}
	hostname = strings.TrimSpace(hostname)
	if hostname == "" || len(hostname) > hostnameLimit || containsControl(hostname) {
		return nil, fmt.Errorf("hostname must be non-empty and at most %d bytes", hostnameLimit)
	}
	workspacesByRunner := make(map[string]*workspaceConfig, len(cfg.Workspaces))
	for index := range cfg.Workspaces {
		workspace := &cfg.Workspaces[index]
		workspacesByRunner[workspace.RunnerID] = workspace
	}
	discovered := discoverWorkspacesUnderRoots(cfg)
	sessionGeneration, err := allocateSessionGeneration(cfg.StateDirectory, cfg.BrokerID, cfg.stateDirectoryInfo)
	if err != nil {
		return nil, fmt.Errorf("allocate session generation: %w", err)
	}
	publishImageCtx, publishImageCancel := context.WithCancel(context.Background())
	return &broker{
		config:             cfg,
		client:             client,
		requestLimit:       heartbeatRequestLimit,
		sessionID:          hex.EncodeToString(sessionBytes),
		sessionGeneration:  sessionGeneration,
		hostname:           hostname,
		pid:                os.Getpid(),
		workspacesByRunner: workspacesByRunner,
		discovered:         discovered,
		lastDiscovery:      time.Now(),
		children:           make(map[childKey]*childProcess),
		launches:           make(map[childKey]*childLaunch),
		publishImageActive: make(map[string]struct{}),
		publishImageErrors: make(chan error, publishImageMaxCount),
		publishImageCtx:    publishImageCtx,
		controlMessages:    make(chan heartbeatResponse, 1),
		publishImageCancel: publishImageCancel,
	}, nil
}

func (localBroker *broker) loop(ctx context.Context, stderr io.Writer) error {
	if ctx.Err() == nil {
		if err := localBroker.performHeartbeat(ctx); err != nil {
			if errors.Is(err, errStaleSession) {
				return err
			}
			if ctx.Err() == nil {
				fmt.Fprintf(stderr, "heartbeat failed: %v\n", err)
			}
		}
	}
	controlCtx, cancelControl := context.WithCancel(ctx)
	controlDone := make(chan struct{})
	go func() {
		defer close(controlDone)
		localBroker.controlLoop(controlCtx)
	}()
	var stopControlOnce sync.Once
	stopControl := func() {
		stopControlOnce.Do(func() {
			cancelControl()
			<-controlDone
		})
	}
	defer stopControl()
	ticker := time.NewTicker(brokerHeartbeatInterval(localBroker.config.HeartbeatSeconds))
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			stopControl()
			return localBroker.shutdown()
		case response := <-localBroker.controlMessages:
			if err := localBroker.applyIntentResponse(ctx, response, false); err != nil && ctx.Err() == nil {
				fmt.Fprintf(stderr, "control update failed: %v\n", err)
			}
		case <-ticker.C:
			if err := localBroker.performHeartbeat(ctx); err != nil {
				if errors.Is(err, errStaleSession) {
					stopControl()
					return err
				}
				if ctx.Err() == nil {
					fmt.Fprintf(stderr, "heartbeat failed: %v\n", err)
				}
			}
		}
	}
}

func brokerHeartbeatInterval(seconds int) time.Duration {
	interval := time.Duration(seconds) * time.Second
	if interval > maxHeartbeatInterval {
		return maxHeartbeatInterval
	}
	return interval
}

func (localBroker *broker) controlLoop(ctx context.Context) {
	reconnectDelay := controlReconnectMin
	for ctx.Err() == nil {
		connected, err := localBroker.runControlConnection(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			logrus.WithError(err).Debug("local broker control channel disconnected")
		}
		if connected {
			reconnectDelay = controlReconnectMin
		}
		timer := time.NewTimer(reconnectDelay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
		}
		if !connected && reconnectDelay < controlReconnectMax {
			reconnectDelay *= 2
			if reconnectDelay > controlReconnectMax {
				reconnectDelay = controlReconnectMax
			}
		}
	}
}

func (localBroker *broker) runControlConnection(ctx context.Context) (bool, error) {
	controlURL, err := localBroker.controlURL()
	if err != nil {
		return false, err
	}
	headers := make(http.Header)
	headers.Set("Authorization", "Bearer "+localBroker.config.apiKey)
	headers.Set(controlBrokerIDHeader, localBroker.config.BrokerID)
	headers.Set(controlSessionIDHeader, localBroker.sessionID)
	headers.Set(controlGenerationHeader, strconv.FormatUint(localBroker.sessionGeneration, 10))
	conn, response, err := localBroker.controlDialer().DialContext(ctx, controlURL, headers)
	if response != nil {
		if closeErr := response.Body.Close(); closeErr != nil {
			logrus.WithError(closeErr).Debug("close local broker control response")
		}
	}
	if err != nil {
		if response != nil {
			return false, fmt.Errorf("connect local broker control channel: HTTP %d", response.StatusCode)
		}
		return false, fmt.Errorf("connect local broker control channel: %w", err)
	}

	conn.SetReadLimit(maxHeartbeatBody)
	var writeMu sync.Mutex
	conn.SetPingHandler(func(data string) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		return conn.WriteControl(websocket.PongMessage, []byte(data), time.Time{})
	})
	connectionDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-connectionDone:
		}
	}()
	pingCtx, cancelPing := context.WithCancel(ctx)
	pingDone := make(chan struct{})
	go func() {
		defer close(pingDone)
		ticker := time.NewTicker(controlPingInterval)
		defer ticker.Stop()
		for {
			select {
			case <-pingCtx.Done():
				return
			case <-ticker.C:
				writeMu.Lock()
				errPing := conn.WriteControl(websocket.PingMessage, nil, time.Time{})
				writeMu.Unlock()
				if errPing != nil {
					_ = conn.Close()
					return
				}
			}
		}
	}()

	readErr := localBroker.readControlMessages(conn)
	cancelPing()
	_ = conn.Close()
	<-pingDone
	close(connectionDone)
	if ctx.Err() != nil {
		return true, ctx.Err()
	}
	return true, readErr
}

func (localBroker *broker) controlURL() (string, error) {
	parsed, err := url.Parse(localBroker.config.APIURL)
	if err != nil {
		return "", fmt.Errorf("parse control URL: %w", err)
	}
	switch parsed.Scheme {
	case "http":
		parsed.Scheme = "ws"
	case "https":
		parsed.Scheme = "wss"
	default:
		return "", errors.New("control URL must use HTTP or HTTPS")
	}
	parsed.Path = controlEndpoint
	return parsed.String(), nil
}

func (localBroker *broker) controlDialer() *websocket.Dialer {
	dialer := &websocket.Dialer{
		Proxy:            http.ProxyFromEnvironment,
		HandshakeTimeout: localBroker.requestLimit,
	}
	transport, ok := localBroker.client.Transport.(*http.Transport)
	if !ok || transport == nil {
		return dialer
	}
	dialer.Proxy = transport.Proxy
	dialer.NetDialContext = transport.DialContext
	dialer.NetDialTLSContext = transport.DialTLSContext
	if transport.TLSClientConfig != nil {
		dialer.TLSClientConfig = transport.TLSClientConfig.Clone()
	}
	if transport.TLSHandshakeTimeout > 0 {
		dialer.HandshakeTimeout = transport.TLSHandshakeTimeout
	}
	return dialer
}

func (localBroker *broker) readControlMessages(conn *websocket.Conn) error {
	for {
		messageType, data, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		if messageType != websocket.TextMessage {
			return errors.New("local broker control message must be text")
		}
		response, err := localBroker.decodeControlMessage(data)
		if err != nil {
			return err
		}
		localBroker.queueControlMessage(response)
	}
}

func (localBroker *broker) queueControlMessage(response heartbeatResponse) {
	for {
		select {
		case localBroker.controlMessages <- response:
			return
		default:
		}
		select {
		case pending := <-localBroker.controlMessages:
			if pending.IntentEpoch == response.IntentEpoch && pending.IntentRevision != nil && response.IntentRevision != nil && *pending.IntentRevision > *response.IntentRevision {
				response = pending
			}
		default:
		}
	}
}

func (localBroker *broker) decodeControlMessage(data []byte) (heartbeatResponse, error) {
	if len(data) > maxHeartbeatBody {
		return heartbeatResponse{}, errors.New("local broker control message exceeds 1 MiB")
	}
	if err := rejectDuplicateJSONFields(data); err != nil {
		return heartbeatResponse{}, errors.New("local broker control message is invalid JSON")
	}
	var message controlMessage
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&message); err != nil {
		return heartbeatResponse{}, errors.New("local broker control message is invalid JSON")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return heartbeatResponse{}, errors.New("local broker control message contains trailing JSON")
	}
	if message.Type != controlMessageType {
		return heartbeatResponse{}, errors.New("local broker control message has an invalid type")
	}
	if message.BrokerID != localBroker.config.BrokerID || message.SessionID != localBroker.sessionID || message.SessionGeneration != localBroker.sessionGeneration {
		return heartbeatResponse{}, errors.New("local broker control message has stale identity")
	}
	if err := validateIdentifier("intent epoch", message.IntentEpoch); err != nil {
		return heartbeatResponse{}, errors.New("local broker control message has an invalid intent epoch")
	}
	revision := message.IntentRevision
	response := heartbeatResponse{
		OK:             true,
		Runners:        message.Runners,
		IntentEpoch:    message.IntentEpoch,
		IntentRevision: &revision,
	}
	if err := localBroker.validateHeartbeatResponse(response); err != nil {
		return heartbeatResponse{}, fmt.Errorf("local broker control message failed validation: %w", err)
	}
	return response, nil
}

func (localBroker *broker) applyIntentResponse(ctx context.Context, response heartbeatResponse, authoritative bool) error {
	if err := localBroker.validateHeartbeatResponse(response); err != nil {
		return err
	}
	if response.IntentEpoch != "" {
		if err := validateIdentifier("intent epoch", response.IntentEpoch); err != nil {
			return err
		}
	}
	if authoritative {
		if response.IntentEpoch == "" && response.IntentRevision == nil {
			localBroker.intentEpoch = ""
			localBroker.intentRevision = 0
			localBroker.intentRevisionKnown = false
		} else if response.IntentEpoch == "" || response.IntentRevision == nil {
			return errors.New("heartbeat response has incomplete intent version")
		} else {
			if localBroker.intentRevisionKnown && response.IntentEpoch == localBroker.intentEpoch && *response.IntentRevision < localBroker.intentRevision {
				return nil
			}
			localBroker.intentEpoch = response.IntentEpoch
			localBroker.intentRevision = *response.IntentRevision
			localBroker.intentRevisionKnown = true
		}
		return localBroker.reconcile(ctx, response)
	}
	if response.IntentEpoch == "" || response.IntentRevision == nil {
		return errors.New("local broker control message has incomplete intent version")
	}
	if localBroker.intentRevisionKnown {
		if response.IntentEpoch != localBroker.intentEpoch || *response.IntentRevision < localBroker.intentRevision {
			return nil
		}
	}
	localBroker.intentEpoch = response.IntentEpoch
	localBroker.intentRevision = *response.IntentRevision
	localBroker.intentRevisionKnown = true
	return localBroker.reconcile(ctx, response)
}

func (localBroker *broker) performHeartbeat(ctx context.Context) error {
	var bundle orbconfig.Bundle
	var collectionErr error
	collectedOrbConfig := false
	collectOrbConfig := func() {
		if collectedOrbConfig {
			return
		}
		collectedOrbConfig = true
		home, homeErr := os.UserHomeDir()
		if homeErr != nil {
			collectionErr = fmt.Errorf("find home directory for orb configuration: %w", homeErr)
			return
		}
		bundle, collectionErr = collectOrbConfigBundle(home)
	}
	if localBroker.orbConfigSupport {
		collectOrbConfig()
		if collectionErr == nil {
			localBroker.orbConfigDigest = &bundle.Digest
		}
	}
	advertisedOrbConfig := localBroker.orbConfigDigest != nil
	response, err := localBroker.postHeartbeat(ctx, false)
	localBroker.orbConfigDigest = nil
	if err != nil && advertisedOrbConfig && errors.Is(err, errOrbConfigDigestUnsupported) && ctx.Err() == nil {
		localBroker.orbConfigSupport = false
		response, err = localBroker.postHeartbeat(ctx, false)
	}
	if err != nil {
		if errors.Is(err, errStaleSession) {
			if errStop := localBroker.stopAll(); errStop != nil {
				return errors.Join(errStaleSession, errStop)
			}
			return errStaleSession
		}
		if ctx.Err() != nil {
			localBroker.markShuttingDown()
		}
		return err
	}
	localBroker.orbConfigSupport = response.OrbConfigSupport
	localBroker.orbCredentials.support = response.OrbCredentialSupport
	localBroker.orbCredentials.serverRevision = response.OrbCredentialRevision
	localBroker.pluginModesSupport = response.PluginAgentModesSupport
	if response.OrbConfigSupport {
		collectOrbConfig()
	}
	if err := ctx.Err(); err != nil {
		localBroker.markShuttingDown()
		return err
	}
	var uploadErr error
	if collectionErr == nil && response.OrbConfigSupport && response.OrbConfigDigest != nil && *response.OrbConfigDigest != bundle.Digest {
		uploadErr = localBroker.putOrbConfigBundle(ctx, bundle)
		if errors.Is(uploadErr, errStaleSession) {
			if errStop := localBroker.stopAll(); errStop != nil {
				return errors.Join(errStaleSession, errStop)
			}
			return errStaleSession
		}
	}
	credentialErr := localBroker.syncOrbCredentials(ctx)
	if errors.Is(credentialErr, errStaleSession) {
		if errStop := localBroker.stopAll(); errStop != nil {
			return errors.Join(errStaleSession, errStop)
		}
		return errStaleSession
	}
	publishImageErr := localBroker.processPublishImageRequests(ctx, response)
	reconcileErr := localBroker.applyIntentResponse(ctx, response, true)
	return errors.Join(collectionErr, uploadErr, credentialErr, publishImageErr, reconcileErr)
}

func (localBroker *broker) postHeartbeat(ctx context.Context, emptyRunners bool) (heartbeatResponse, error) {
	requestContext, cancel := context.WithTimeout(ctx, localBroker.requestLimit)
	defer cancel()
	payload := localBroker.heartbeatPayload(emptyRunners)
	body, err := json.Marshal(payload)
	if err != nil {
		return heartbeatResponse{}, fmt.Errorf("encode heartbeat: %w", err)
	}
	request, err := http.NewRequestWithContext(requestContext, http.MethodPost, localBroker.config.APIURL+heartbeatEndpoint, bytes.NewReader(body))
	if err != nil {
		return heartbeatResponse{}, fmt.Errorf("create heartbeat request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+localBroker.config.apiKey)
	request.Header.Set("Content-Type", "application/json")
	response, err := localBroker.client.Do(request)
	if err != nil {
		return heartbeatResponse{}, fmt.Errorf("send heartbeat: %w", err)
	}
	orbConfigSupport := response.Header.Get(orbConfigSupportHeader) == "1"
	orbConfigDigest := response.Header.Get(orbConfigDigestHeader)
	orbCredentialSupport := response.Header.Get(orbCredentialSupportHeader) == "1"
	orbCredentialRevision := response.Header.Get(orbCredentialRevisionHeader)
	pluginModesSupport := response.Header.Get(ampplugins.SupportHeader) == "1"
	responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, maxHeartbeatBody+1))
	closeErr := response.Body.Close()
	if readErr != nil {
		return heartbeatResponse{}, fmt.Errorf("read heartbeat response: %w", readErr)
	}
	if closeErr != nil {
		return heartbeatResponse{}, fmt.Errorf("close heartbeat response: %w", closeErr)
	}
	if len(responseBody) > maxHeartbeatBody {
		return heartbeatResponse{}, errors.New("heartbeat response exceeds 1 MiB")
	}
	if err := rejectDuplicateJSONFields(responseBody); err != nil {
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			return heartbeatResponse{}, fmt.Errorf("heartbeat rejected with HTTP %d", response.StatusCode)
		}
		return heartbeatResponse{}, errors.New("heartbeat response is invalid JSON")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		rejectionErr := heartbeatRejectionError(response.StatusCode, responseBody)
		if payload.OrbConfigDigest != nil && orbConfigDigestUnsupported(response.StatusCode, responseBody) {
			return heartbeatResponse{}, fmt.Errorf("%w: %v", errOrbConfigDigestUnsupported, rejectionErr)
		}
		return heartbeatResponse{}, rejectionErr
	}
	var decoded heartbeatResponse
	decoder := json.NewDecoder(bytes.NewReader(responseBody))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&decoded); err != nil {
		return heartbeatResponse{}, errors.New("heartbeat response is invalid JSON")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return heartbeatResponse{}, errors.New("heartbeat response contains trailing JSON")
	}
	if !decoded.OK {
		return heartbeatResponse{}, heartbeatRejectionError(response.StatusCode, responseBody)
	}
	intentEpoch, intentRevision, err := parseIntentVersionHeaders(response.Header)
	if err != nil {
		return heartbeatResponse{}, fmt.Errorf("heartbeat response failed validation: %w", err)
	}
	decoded.OrbConfigSupport = orbConfigSupport
	decoded.OrbCredentialSupport = orbCredentialSupport
	decoded.OrbCredentialRevision = orbCredentialRevision
	decoded.PluginAgentModesSupport = pluginModesSupport
	decoded.IntentEpoch = intentEpoch
	decoded.IntentRevision = intentRevision
	if decoded.OrbConfigDigest == nil && orbConfigSupport {
		decoded.OrbConfigDigest = &orbConfigDigest
	}
	if err := localBroker.validateHeartbeatResponse(decoded); err != nil {
		return heartbeatResponse{}, fmt.Errorf("heartbeat response failed validation: %w", err)
	}
	return decoded, nil
}

func parseIntentVersionHeaders(headers http.Header) (string, *uint64, error) {
	epochValues, epochPresent := matchingHeaderValues(headers, intentEpochHeader)
	revisionValues, revisionPresent := matchingHeaderValues(headers, intentRevisionHeader)
	if !epochPresent && !revisionPresent {
		return "", nil, nil
	}
	if !epochPresent || !revisionPresent || len(epochValues) != 1 || len(revisionValues) != 1 {
		return "", nil, errors.New("heartbeat response has incomplete intent version")
	}
	epoch := epochValues[0]
	if err := validateIdentifier("intent epoch", epoch); err != nil {
		return "", nil, errors.New("heartbeat response has an invalid intent epoch")
	}
	revision, err := strconv.ParseUint(revisionValues[0], 10, 64)
	if err != nil || strconv.FormatUint(revision, 10) != revisionValues[0] {
		return "", nil, errors.New("heartbeat response has an invalid intent revision")
	}
	return epoch, &revision, nil
}

func matchingHeaderValues(headers http.Header, name string) ([]string, bool) {
	var values []string
	for key, candidates := range headers {
		if strings.EqualFold(key, name) {
			values = append(values, candidates...)
		}
	}
	return values, values != nil
}

func heartbeatRejectionError(status int, body []byte) error {
	var response struct {
		Code    string          `json:"code"`
		Error   json.RawMessage `json:"error"`
		Message string          `json:"message"`
	}
	if err := json.Unmarshal(body, &response); err == nil {
		code := response.Code
		if code == "" {
			if err := json.Unmarshal(response.Error, &code); err != nil {
				var detail struct {
					Code string `json:"code"`
				}
				if err := json.Unmarshal(response.Error, &detail); err == nil {
					code = detail.Code
				}
			}
		}
		if code == "stale_session" {
			return errStaleSession
		}
		if heartbeatCodePattern.MatchString(code) {
			if status >= 200 && status < 300 {
				return fmt.Errorf("heartbeat rejected: %s", code)
			}
			return fmt.Errorf("heartbeat rejected with HTTP %d: %s", status, code)
		}
		message := strings.TrimSpace(response.Message)
		if message != "" && len(message) <= heartbeatMessageLimit && !containsControl(message) {
			if status >= 200 && status < 300 {
				return fmt.Errorf("heartbeat rejected: %s", message)
			}
			return fmt.Errorf("heartbeat rejected with HTTP %d: %s", status, message)
		}
	}
	if status >= 200 && status < 300 {
		return errors.New("heartbeat rejected")
	}
	return fmt.Errorf("heartbeat rejected with HTTP %d", status)
}

func orbConfigDigestUnsupported(status int, body []byte) bool {
	if status != http.StatusBadRequest {
		return false
	}
	var response map[string]any
	if err := json.Unmarshal(body, &response); err != nil {
		return false
	}
	code, _ := response["code"].(string)
	if code == "" {
		switch detail := response["error"].(type) {
		case string:
			code = detail
		case map[string]any:
			code, _ = detail["code"].(string)
		}
	}
	switch code {
	case "orb_config_digest_unsupported", "unsupported_orb_config_digest", "orb_config_digest_not_supported":
		return true
	case "invalid_request":
		return orbConfigDigestErrorEvidence(response)
	default:
		return false
	}
}

func orbConfigDigestErrorEvidence(value any) bool {
	switch typed := value.(type) {
	case string:
		return strings.Contains(typed, "orbConfigDigest")
	case map[string]any:
		for key, nested := range typed {
			switch strings.ToLower(key) {
			case "error", "message", "detail", "field", "parameter", "param":
				if orbConfigDigestErrorEvidence(nested) {
					return true
				}
			}
		}
	}
	return false
}

func (localBroker *broker) heartbeatPayload(emptyRunners bool) heartbeatRequest {
	localBroker.refreshDiscoveredWorkspaces()
	workspaces := make([]workspaceConfig, 0, len(localBroker.config.Workspaces)+len(localBroker.discoveredSnapshot()))
	workspaces = append(workspaces, localBroker.config.Workspaces...)
	workspaces = append(workspaces, localBroker.discoveredSnapshot()...)
	payload := heartbeatRequest{
		BrokerID:          localBroker.config.BrokerID,
		SessionID:         localBroker.sessionID,
		SessionGeneration: localBroker.sessionGeneration,
		Hostname:          localBroker.hostname,
		PID:               localBroker.pid,
		Runners:           make([]heartbeatRunner, 0, len(workspaces)),
		OrbConfigDigest:   localBroker.orbConfigDigest,
	}
	if emptyRunners {
		return payload
	}
	if localBroker.pluginModesSupport {
		if modes, err := ampplugins.Discover(ampplugins.DefaultPluginsDir(), reservedAgentModeKey); err != nil {
			logrus.WithError(err).Debug("discover amp plugin agent modes")
		} else if len(modes) > 0 {
			advertised := make([]ampplugins.AgentMode, 0, len(modes))
			for _, mode := range modes {
				if mode != nil {
					advertised = append(advertised, *mode)
				}
			}
			payload.PluginAgentModes = advertised
		}
	}
	running := localBroker.runningThreadsByRunner()
	for _, workspace := range workspaces {
		threads := append([]string{}, running[workspace.RunnerID]...)
		sort.Strings(threads)
		payload.Runners = append(payload.Runners, heartbeatRunner{
			RunnerID:         workspace.RunnerID,
			WorkingDirectory: workspace.Path,
			RepositoryURL:    workspace.RepositoryURL,
			RunningThreads:   threads,
		})
	}
	return payload
}

func (localBroker *broker) runningThreadsByRunner() map[string][]string {
	localBroker.mu.Lock()
	defer localBroker.mu.Unlock()
	running := make(map[string][]string, len(localBroker.config.Workspaces))
	for key, child := range localBroker.children {
		if !child.stopping {
			running[key.runnerID] = append(running[key.runnerID], key.threadID)
		}
	}
	return running
}

func (localBroker *broker) reconcile(ctx context.Context, response heartbeatResponse) error {
	if err := ctx.Err(); err != nil {
		localBroker.markShuttingDown()
		return err
	}
	targets := make(map[childKey]string)
	order := make([]childKey, 0)
	if response.RejectedRunners != nil {
		for _, rejected := range *response.RejectedRunners {
			logrus.WithFields(logrus.Fields{"runner_id": rejected.RunnerID, "code": rejected.Code}).Warn(rejected.Message)
		}
	}
	for _, runner := range *response.Runners {
		for _, intent := range *runner.Intents {
			key := childKey{runnerID: runner.RunnerID, threadID: intent.ThreadID}
			switch intent.Desired {
			case "running":
				order = append(order, key)
				targets[key] = mapAgentMode(intent.AgentMode, intent.ReasoningEffort)
			}
		}
	}

	localBroker.mu.Lock()
	toStop := make([]*childProcess, 0)
	for key, child := range localBroker.children {
		mode, keep := targets[key]
		if !keep || mode != child.mode {
			child.stopping = true
			toStop = append(toStop, child)
		}
	}
	localBroker.mu.Unlock()

	var reconcileErrors []error
	if err := localBroker.stopChildren(toStop); err != nil {
		return err
	}
	for _, key := range order {
		mode, wanted := targets[key]
		if !wanted {
			continue
		}
		if err := localBroker.launchChild(ctx, key, mode); err != nil {
			reconcileErrors = append(reconcileErrors, fmt.Errorf("launch thread %s: %w", key.threadID, err))
		}
	}
	return errors.Join(reconcileErrors...)
}

func (localBroker *broker) validateHeartbeatResponse(response heartbeatResponse) error {
	if !response.OK {
		return errors.New("response is not successful")
	}
	if response.Runners == nil {
		return errors.New("runners is required")
	}
	if response.OrbConfigDigest != nil && *response.OrbConfigDigest != "" && !orbConfigDigestPattern.MatchString(*response.OrbConfigDigest) {
		return errors.New("orbConfigDigest is invalid")
	}
	if response.OrbCredentialSupport {
		if response.OrbCredentialRevision != orbCredentialAbsentRevision && response.OrbCredentialRevision != orbCredentialRevokedRevision && response.OrbCredentialRevision != orbCredentialRepairRevision && !orbCredentialRevisionPattern.MatchString(response.OrbCredentialRevision) {
			return errors.New("orb credential revision is invalid")
		}
	} else if response.OrbCredentialRevision != "" {
		return errors.New("orb credential revision is unexpected")
	}
	if len(*response.Runners) > localBroker.approvedWorkspaceCount() {
		return errors.New("response contains too many runners")
	}
	seenRunners := make(map[string]struct{}, len(*response.Runners))
	seenThreads := make(map[string]struct{})
	intentCount := 0
	for _, runner := range *response.Runners {
		if localBroker.workspaceByRunnerID(runner.RunnerID) == nil {
			return fmt.Errorf("response contains unapproved runner %s", heartbeatDiagnosticIdentifier(runner.RunnerID))
		}
		if _, exists := seenRunners[runner.RunnerID]; exists {
			return fmt.Errorf("response contains duplicate runner %s", heartbeatDiagnosticIdentifier(runner.RunnerID))
		}
		seenRunners[runner.RunnerID] = struct{}{}
		if runner.Intents == nil {
			return fmt.Errorf("runner %s intents are required", heartbeatDiagnosticIdentifier(runner.RunnerID))
		}
		intentCount += len(*runner.Intents)
		if intentCount > intentLimit {
			return fmt.Errorf("runner %s causes intents to exceed %d entries", heartbeatDiagnosticIdentifier(runner.RunnerID), intentLimit)
		}
		for _, intent := range *runner.Intents {
			if !threadIDPattern.MatchString(intent.ThreadID) {
				return fmt.Errorf("response contains invalid thread ID %s for runner %s", heartbeatDiagnosticIdentifier(intent.ThreadID), heartbeatDiagnosticIdentifier(runner.RunnerID))
			}
			if _, exists := seenThreads[intent.ThreadID]; exists {
				return fmt.Errorf("response contains duplicate thread %s for runner %s", heartbeatDiagnosticIdentifier(intent.ThreadID), heartbeatDiagnosticIdentifier(runner.RunnerID))
			}
			seenThreads[intent.ThreadID] = struct{}{}
			switch intent.Desired {
			case "running":
				if mapAgentMode(intent.AgentMode, intent.ReasoningEffort) == "" {
					return fmt.Errorf("response contains invalid mode for runner %s thread %s", heartbeatDiagnosticIdentifier(runner.RunnerID), heartbeatDiagnosticIdentifier(intent.ThreadID))
				}
			case "stopped":
			default:
				return fmt.Errorf("response contains invalid desired state for runner %s thread %s", heartbeatDiagnosticIdentifier(runner.RunnerID), heartbeatDiagnosticIdentifier(intent.ThreadID))
			}
		}
	}
	if response.RejectedRunners != nil {
		if len(*response.Runners)+len(*response.RejectedRunners) > localBroker.approvedWorkspaceCount() {
			return errors.New("response contains too many runner results")
		}
		for _, rejected := range *response.RejectedRunners {
			if localBroker.workspaceByRunnerID(rejected.RunnerID) == nil {
				return fmt.Errorf("response rejects unapproved runner %s", heartbeatDiagnosticIdentifier(rejected.RunnerID))
			}
			if _, exists := seenRunners[rejected.RunnerID]; exists {
				return fmt.Errorf("response contains duplicate runner result %s", heartbeatDiagnosticIdentifier(rejected.RunnerID))
			}
			seenRunners[rejected.RunnerID] = struct{}{}
			if !heartbeatCodePattern.MatchString(rejected.Code) {
				return fmt.Errorf("response contains invalid rejection code for runner %s", heartbeatDiagnosticIdentifier(rejected.RunnerID))
			}
			if strings.TrimSpace(rejected.Message) == "" || len(rejected.Message) > heartbeatMessageLimit || containsControl(rejected.Message) {
				return fmt.Errorf("response contains invalid rejection message for runner %s", heartbeatDiagnosticIdentifier(rejected.RunnerID))
			}
		}
	}
	if err := localBroker.validatePublishImageRequests(response.PublishImageRequests); err != nil {
		return err
	}
	return nil
}

func heartbeatDiagnosticIdentifier(value string) string {
	if len(value) > identifierLimit {
		return strconv.QuoteToASCII(value[:identifierLimit]) + "..."
	}
	return strconv.QuoteToASCII(value)
}

func reservedAgentModeKey(key string) bool {
	switch key {
	case "smart", "rush", "deep", "large", "review", "puck", "low", "medium", "high", "ultra", "deep-1", "deep-2", "deep-3":
		return true
	default:
		return false
	}
}

func mapAgentMode(agentMode, reasoningEffort string) string {
	mode := strings.ToLower(strings.TrimSpace(agentMode))
	effort := strings.ToLower(strings.TrimSpace(reasoningEffort))
	switch effort {
	case "", "none", "low", "medium", "high", "xhigh", "max":
	default:
		return ""
	}
	switch mode {
	case "":
		return "medium"
	case "smart":
		return "medium"
	case "rush":
		return "low"
	case "deep":
		if effort == "xhigh" || effort == "max" {
			return "high"
		}
		return "medium"
	case "large":
		return "ultra"
	default:
		if agentModePattern.MatchString(mode) {
			return mode
		}
		return ""
	}
}

func brokerPublicationRunnerID(brokerID, sessionID string, sessionGeneration uint64, runnerID, threadID string) string {
	payload := strings.Join([]string{
		"cliproxy-local-broker-publication-v1",
		brokerID,
		sessionID,
		strconv.FormatUint(sessionGeneration, 10),
		runnerID,
		threadID,
	}, "\x00")
	digest := sha256.Sum256([]byte(payload))
	return "broker-" + hex.EncodeToString(digest[:16])
}

func (localBroker *broker) launchChild(ctx context.Context, key childKey, mode string) error {
	workspace := localBroker.workspaceByRunnerID(key.runnerID)
	if workspace == nil || !threadIDPattern.MatchString(key.threadID) {
		return errors.New("runner or thread is not approved")
	}
	localBroker.mu.Lock()
	if err := ctx.Err(); err != nil {
		localBroker.shuttingDown = true
		localBroker.mu.Unlock()
		return err
	}
	if localBroker.shuttingDown {
		localBroker.mu.Unlock()
		return errors.New("broker is shutting down")
	}
	for existingKey := range localBroker.children {
		if existingKey == key || existingKey.threadID == key.threadID {
			existing := localBroker.children[existingKey]
			if existing.stopping && existing.confirmProcessGroupExit() {
				delete(localBroker.children, existingKey)
				continue
			}
			if existing.stopping {
				localBroker.mu.Unlock()
				return errors.New("previous child process group cleanup is incomplete")
			}
			localBroker.mu.Unlock()
			return nil
		}
	}
	for existingKey := range localBroker.launches {
		if existingKey == key || existingKey.threadID == key.threadID {
			localBroker.mu.Unlock()
			return nil
		}
	}
	launchContext, cancelLaunch := context.WithCancel(ctx)
	launch := &childLaunch{cancel: cancelLaunch, done: make(chan struct{})}
	if localBroker.launches == nil {
		localBroker.launches = make(map[childKey]*childLaunch)
	}
	localBroker.launches[key] = launch
	localBroker.mu.Unlock()
	defer func() {
		cancelLaunch()
		localBroker.mu.Lock()
		if localBroker.launches[key] == launch {
			delete(localBroker.launches, key)
		}
		close(launch.done)
		localBroker.mu.Unlock()
	}()
	if err := localBroker.validateLaunchPaths(workspace); err != nil {
		return err
	}
	if err := launchContext.Err(); err != nil {
		localBroker.markShuttingDown()
		return err
	}

	logPath := filepath.Join(localBroker.config.LogDirectory, key.runnerID+"-"+key.threadID+".log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0o600)
	if err != nil {
		return fmt.Errorf("open child log: %w", err)
	}
	logInfo, err := logFile.Stat()
	if err != nil || !logInfo.Mode().IsRegular() {
		_ = logFile.Close()
		if err != nil {
			return fmt.Errorf("inspect child log: %w", err)
		}
		return errors.New("child log is not a regular file")
	}
	stat, ok := logInfo.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Uid) != os.Geteuid() || stat.Nlink != 1 {
		_ = logFile.Close()
		return errors.New("child log must be owned by the current user and not hard linked")
	}
	if err := logFile.Chmod(0o600); err != nil {
		_ = logFile.Close()
		return fmt.Errorf("secure child log: %w", err)
	}

	command := exec.Command(localBroker.config.AmpBinary,
		"--mode", mode,
		"--headless="+key.threadID,
		"--runner-id", brokerPublicationRunnerID(localBroker.config.BrokerID, localBroker.sessionID, localBroker.sessionGeneration, key.runnerID, key.threadID),
		"--log-file", logPath,
	)
	command.Dir = workspace.Path
	command.Env = childEnvironment(os.Environ(), localBroker.config, workspace.Path, key.threadID, logPath)
	command.Stdout = logFile
	command.Stderr = logFile
	if err := launchContext.Err(); err != nil {
		localBroker.markShuttingDown()
		_ = logFile.Close()
		return err
	}
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := processStart(launchContext, command); err != nil {
		_ = logFile.Close()
		return fmt.Errorf("start amp: %w", err)
	}
	if command.Process == nil || command.Process.Pid <= 0 {
		_ = logFile.Close()
		return errors.New("start amp succeeded without a valid process")
	}
	processGroupID := command.Process.Pid
	child := &childProcess{
		key:            key,
		mode:           mode,
		cmd:            command,
		processGroupID: processGroupID,
		logFile:        logFile,
		done:           make(chan struct{}),
	}
	exitWatcher, err := newProcessExitWatcher(processGroupID)
	if err != nil {
		cleanupErr := child.terminateProcessGroup(false)
		waitErr := command.Wait()
		cleanupErr = errors.Join(cleanupErr, child.finishProcessGroupRetirement())
		_ = logFile.Close()
		return errors.Join(fmt.Errorf("watch amp process exit: %w", err), waitErr, cleanupErr)
	}
	child.exitWatcher = exitWatcher
	localBroker.mu.Lock()
	if localBroker.shuttingDown || launchContext.Err() != nil {
		localBroker.mu.Unlock()
		killErr := child.terminateProcessGroup(false)
		if killErr != nil {
			leaderKillErr := command.Process.Kill()
			if errors.Is(leaderKillErr, os.ErrProcessDone) {
				leaderKillErr = nil
			}
			killErr = errors.Join(killErr, leaderKillErr)
		}
		waitErr := command.Wait()
		closeWatcherErr := child.exitWatcher.close()
		cleanupErr := child.finishProcessGroupRetirement()
		_ = logFile.Close()
		return errors.Join(errors.New("broker is shutting down"), launchContext.Err(), killErr, waitErr, closeWatcherErr, cleanupErr)
	}
	localBroker.children[key] = child
	localBroker.mu.Unlock()
	go localBroker.waitChild(child)
	return nil
}

func (localBroker *broker) validateLaunchPaths(workspace *workspaceConfig) error {
	ampBinary, err := canonicalExecutable(localBroker.config.AmpBinary)
	if err != nil {
		return fmt.Errorf("revalidate amp binary: %w", err)
	}
	if ampBinary != localBroker.config.AmpBinary {
		return errors.New("amp binary canonical path changed after configuration validation")
	}
	ampInfo, err := os.Stat(ampBinary)
	if err != nil {
		return fmt.Errorf("inspect amp binary: %w", err)
	}
	if !sameFileSnapshot(localBroker.config.ampBinaryInfo, ampInfo) {
		return errors.New("amp binary changed after configuration validation")
	}
	workspacePath, err := canonicalWorkspace(workspace.Path)
	if err != nil {
		return fmt.Errorf("revalidate workspace: %w", err)
	}
	if workspacePath != workspace.Path {
		return errors.New("workspace canonical path changed after configuration validation")
	}
	workspaceInfo, err := os.Stat(workspacePath)
	if err != nil {
		return fmt.Errorf("inspect workspace: %w", err)
	}
	if !os.SameFile(workspace.info, workspaceInfo) {
		return errors.New("workspace changed after configuration validation")
	}
	logInfo, err := os.Lstat(localBroker.config.LogDirectory)
	if err != nil {
		return fmt.Errorf("inspect log directory: %w", err)
	}
	if logInfo.Mode()&os.ModeSymlink != 0 || !os.SameFile(localBroker.config.logDirectoryInfo, logInfo) {
		return errors.New("log directory changed after configuration validation")
	}
	if err := validatePrivateDirectoryInfo(logInfo); err != nil {
		return fmt.Errorf("validate log directory: %w", err)
	}
	return nil
}

func sameFileSnapshot(first, second os.FileInfo) bool {
	return os.SameFile(first, second) && first.Size() == second.Size() && first.Mode() == second.Mode() && first.ModTime().Equal(second.ModTime())
}

func childEnvironment(base []string, cfg *brokerConfig, workingDirectory, threadID, logPath string) []string {
	updates := []struct {
		key   string
		value string
	}{
		{key: "AMP_EXECUTOR", value: "1"},
		{key: "AMP_URL", value: cfg.APIURL},
		{key: "AMP_API_KEY", value: cfg.apiKey},
		{key: "AMP_THREAD_ID", value: threadID},
		{key: "AMP_CURRENT_THREAD_ID", value: threadID},
		{key: "AMP_SKIP_UPDATE_CHECK", value: "1"},
		{key: "AMP_HEADLESS_OAUTH", value: "1"},
		{key: "AMP_REMOTE_CONTROL_TERMINAL", value: "1"},
		{key: "AMP_GATEWAY_URL", value: cfg.RuntimeURL},
		{key: "AMP_RUNTIME_URL", value: cfg.RuntimeURL},
		{key: "RIVET_ENDPOINT", value: cfg.RuntimeURL},
		{key: "RIVET_GATEWAY_URL", value: cfg.RuntimeURL},
		{key: "RIVET_PUBLIC_ENDPOINT", value: cfg.RuntimeURL},
		{key: "RIVETKIT_ENGINE_URL", value: cfg.RuntimeURL},
		{key: "RIVET_THREAD_ID", value: threadID},
		{key: "RIVET_TOKEN", value: localRuntimeToken},
		{key: "RIVET_NAMESPACE", value: localRuntimeNamespace},
		{key: "RIVET_POOL", value: localRuntimePool},
		{key: "AMP_LOG_FILE", value: logPath},
		{key: "AMP_PWD", value: workingDirectory},
	}
	allowed := map[string]struct{}{
		"HOME": {}, "USER": {}, "LOGNAME": {}, "SHELL": {},
		"TMPDIR": {}, "TMP": {}, "TEMP": {}, "LANG": {}, "TERM": {}, "COLORTERM": {},
	}
	home := ""
	inheritedPath := ""
	for _, entry := range base {
		key, value, found := strings.Cut(entry, "=")
		if !found {
			continue
		}
		switch key {
		case "HOME":
			home = value
		case "PATH":
			inheritedPath = value
		}
	}
	environment := make([]string, 0, len(allowed)+len(updates)+1)
	for _, entry := range base {
		key, _, found := strings.Cut(entry, "=")
		if !found {
			continue
		}
		if key == "PATH" {
			continue
		}
		if _, ok := allowed[key]; ok || strings.HasPrefix(key, "LC_") {
			environment = append(environment, entry)
		}
	}
	environment = append(environment, "PATH="+childPath(home, inheritedPath))
	for _, update := range updates {
		environment = append(environment, update.key+"="+update.value)
	}
	return environment
}

func childPath(home, inherited string) string {
	paths := make([]string, 0, len(filepath.SplitList(inherited))+13)
	seen := make(map[string]struct{}, cap(paths))
	add := func(path string) {
		path = filepath.Clean(path)
		if !filepath.IsAbs(path) {
			return
		}
		if _, exists := seen[path]; exists {
			return
		}
		seen[path] = struct{}{}
		paths = append(paths, path)
	}
	for _, path := range filepath.SplitList(inherited) {
		add(path)
	}
	if filepath.IsAbs(home) {
		home = filepath.Clean(home)
		for _, path := range []string{
			filepath.Join(home, ".local", "bin"),
			filepath.Join(home, ".amp", "bin"),
			filepath.Join(home, ".bun", "bin"),
			filepath.Join(home, ".local", "share", "mise", "shims"),
			filepath.Join(home, ".local", "share", "mise", "installs", "node", "latest", "bin"),
		} {
			add(path)
		}
	}
	for _, path := range []string{
		"/opt/homebrew/bin",
		"/opt/homebrew/sbin",
		"/usr/local/bin",
		"/usr/local/sbin",
		"/usr/bin",
		"/bin",
		"/usr/sbin",
		"/sbin",
	} {
		add(path)
	}
	return strings.Join(paths, string(os.PathListSeparator))
}

func (localBroker *broker) waitChild(child *childProcess) {
	watcherErr := child.exitWatcher.wait()
	cleanupErr := child.terminateProcessGroup(watcherErr == nil)
	if cleanupErr != nil {
		leaderKillErr := child.cmd.Process.Kill()
		if errors.Is(leaderKillErr, os.ErrProcessDone) {
			leaderKillErr = nil
		}
		cleanupErr = errors.Join(cleanupErr, leaderKillErr)
	}
	waitErr := child.cmd.Wait()
	closeWatcherErr := child.exitWatcher.close()
	cleanupErr = errors.Join(cleanupErr, child.finishProcessGroupRetirement())
	logFields := logrus.Fields{"runner_id": child.key.runnerID, "thread_id": child.key.threadID}
	if watcherErr != nil {
		logrus.WithError(watcherErr).WithFields(logFields).Warn("watch Amp child exit")
	}
	if waitErr != nil {
		logrus.WithError(waitErr).WithFields(logFields).Debug("Amp child exited")
	}
	if closeWatcherErr != nil {
		logrus.WithError(closeWatcherErr).WithFields(logFields).Warn("close Amp child exit watcher")
	}
	if errClose := child.logFile.Close(); errClose != nil {
		logrus.WithError(errClose).WithFields(logFields).Warn("close Amp child log")
	}
	localBroker.mu.Lock()
	if current := localBroker.children[child.key]; current == child && cleanupErr == nil {
		delete(localBroker.children, child.key)
	} else if current == child {
		child.stopping = true
	}
	close(child.done)
	localBroker.mu.Unlock()
}

func (child *childProcess) terminateProcessGroup(leaderExitObserved bool) error {
	child.lifecycleMu.Lock()
	defer child.lifecycleMu.Unlock()
	if child.retired {
		return child.cleanupErr
	}
	err := child.signalOwnedProcessGroup(syscall.SIGKILL, leaderExitObserved)
	child.cleanupErr = errors.Join(child.cleanupErr, err)
	return err
}

func (child *childProcess) finishProcessGroupRetirement() error {
	child.lifecycleMu.Lock()
	defer child.lifecycleMu.Unlock()
	if child.retired {
		return child.cleanupErr
	}
	err := child.cleanupErr
	if err == nil && !waitForProcessGroupExit(child.processGroupID, killGracePeriod) {
		err = errors.Join(err, errors.New("process group did not exit after SIGKILL"))
		child.cleanupPending = true
	}
	child.cleanupErr = err
	child.retired = true
	return err
}

func (child *childProcess) confirmProcessGroupExit() bool {
	child.lifecycleMu.Lock()
	defer child.lifecycleMu.Unlock()
	if !child.cleanupPending || !errors.Is(processGroupSignal(-child.processGroupID, 0), syscall.ESRCH) {
		return false
	}
	child.cleanupPending = false
	child.cleanupErr = nil
	return true
}

func (child *childProcess) cleanupError() error {
	child.lifecycleMu.Lock()
	defer child.lifecycleMu.Unlock()
	return child.cleanupErr
}

func appendChildCleanupErrors(destination []error, children []*childProcess) []error {
	for _, child := range children {
		if err := child.cleanupError(); err != nil {
			destination = append(destination, err)
		}
	}
	return destination
}

func (localBroker *broker) stopChildren(children []*childProcess) error {
	if len(children) == 0 {
		return nil
	}
	var stopErrors []error
	for _, child := range children {
		if err := child.signalProcessGroup(syscall.SIGTERM); err != nil {
			stopErrors = append(stopErrors, err)
		}
	}
	if waitForChildren(children, stopGracePeriod) {
		stopErrors = appendChildCleanupErrors(stopErrors, children)
		return errors.Join(stopErrors...)
	}
	for _, child := range children {
		if err := child.signalProcessGroup(syscall.SIGKILL); err != nil {
			stopErrors = append(stopErrors, err)
		}
	}
	if !waitForChildren(children, 2*killGracePeriod) {
		stopErrors = append(stopErrors, errors.New("child processes did not exit after SIGKILL"))
	} else {
		stopErrors = appendChildCleanupErrors(stopErrors, children)
	}
	return errors.Join(stopErrors...)
}

func (child *childProcess) signalProcessGroup(signal syscall.Signal) error {
	child.lifecycleMu.Lock()
	defer child.lifecycleMu.Unlock()
	if child.retired {
		return child.cleanupErr
	}
	return child.signalOwnedProcessGroup(signal, false)
}

func (child *childProcess) signalOwnedProcessGroup(signal syscall.Signal, leaderExitObserved bool) error {
	err := signalProcessGroupID(child.key.threadID, child.processGroupID, signal)
	if leaderExitObserved && errors.Is(err, syscall.EPERM) && child.cmd != nil && child.cmd.Process != nil && syscall.Kill(child.cmd.Process.Pid, 0) == nil {
		return nil
	}
	return err
}

func signalProcessGroupID(threadID string, processGroupID int, signal syscall.Signal) error {
	err := processGroupSignal(-processGroupID, signal)
	if errors.Is(err, syscall.ESRCH) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("signal thread %s process group: %w", threadID, err)
	}
	return nil
}

func waitForProcessGroupExit(processGroupID int, duration time.Duration) bool {
	deadline := time.Now().Add(duration)
	for {
		err := processGroupSignal(-processGroupID, 0)
		if errors.Is(err, syscall.ESRCH) {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func waitForChildren(children []*childProcess, duration time.Duration) bool {
	deadline := time.Now().Add(duration)
	for {
		allDone := true
		for _, child := range children {
			select {
			case <-child.done:
			default:
				allDone = false
			}
		}
		if allDone {
			return true
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return false
		}
		if remaining > 10*time.Millisecond {
			remaining = 10 * time.Millisecond
		}
		time.Sleep(remaining)
	}
}

func (localBroker *broker) markShuttingDown() {
	localBroker.mu.Lock()
	localBroker.shuttingDown = true
	localBroker.mu.Unlock()
}

func (localBroker *broker) beginShutdown() ([]*childProcess, []*childLaunch) {
	localBroker.mu.Lock()
	defer localBroker.mu.Unlock()
	localBroker.shuttingDown = true
	if localBroker.publishImageCancel != nil {
		localBroker.publishImageCancel()
	}
	children := make([]*childProcess, 0, len(localBroker.children))
	for _, child := range localBroker.children {
		child.stopping = true
		children = append(children, child)
	}
	launches := make([]*childLaunch, 0, len(localBroker.launches))
	for _, launch := range localBroker.launches {
		launch.cancel()
		launches = append(launches, launch)
	}
	return children, launches
}

func (localBroker *broker) stopAll() error {
	children, launches := localBroker.beginShutdown()
	err := localBroker.stopChildren(children)
	for _, launch := range launches {
		<-launch.done
	}
	localBroker.publishImageWG.Wait()
	return err
}

func (localBroker *broker) shutdown() error {
	if err := localBroker.stopAll(); err != nil {
		return err
	}
	finalContext, cancel := context.WithTimeout(context.Background(), shutdownRequestLimit)
	defer cancel()
	_, err := localBroker.postHeartbeat(finalContext, true)
	return err
}
