package amp

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	log "github.com/sirupsen/logrus"
)

const (
	ampClientVersionAuto          = "auto"
	ampClientVersionLatest        = "latest"
	ampClientVersionCacheDuration = time.Hour
	ampClientVersionProbeTimeout  = 5 * time.Second
	ampCLINPMLatestURL            = "https://registry.npmjs.org/@ampcode%2fcli/latest"
)

var (
	ampClientVersionPattern = regexp.MustCompile(`\b\d+\.\d+\.\d+(?:[-+][0-9A-Za-z][0-9A-Za-z._-]*)?\b`)
	ampClientVersions       = &ampClientVersionResolver{}
)

type ampClientVersionResolver struct {
	mu               sync.Mutex
	value            string
	expiresAt        time.Time
	refreshing       bool
	localProbeDone   chan struct{}
	installedVersion func(string) string
	latestVersion    func() string
}

func ampUpstreamClientVersionProvider(settings *config.AmpCode) func() string {
	if settings == nil {
		return nil
	}
	value := strings.TrimSpace(settings.UpstreamClientVersionOverride)
	if value == "" {
		return nil
	}
	if !strings.EqualFold(value, ampClientVersionAuto) && !strings.EqualFold(value, ampClientVersionLatest) {
		return func() string { return value }
	}
	executorCommand := strings.TrimSpace(settings.NeoLocalRuntime.ExecutorCommand)
	return func() string {
		return ampClientVersions.latest(executorCommand)
	}
}

func ampUpstreamClientVersion(settings *config.AmpCode) string {
	provider := ampUpstreamClientVersionProvider(settings)
	if provider == nil {
		return ""
	}
	return strings.TrimSpace(provider())
}

func (r *ampClientVersionResolver) latest(executorCommand string) string {
	if r == nil {
		return ""
	}
	now := time.Now()
	r.mu.Lock()
	if r.value != "" && now.Before(r.expiresAt) {
		value := r.value
		r.mu.Unlock()
		return value
	}
	if r.value != "" {
		if !r.refreshing {
			r.refreshing = true
			go r.refresh()
		}
		value := r.value
		r.mu.Unlock()
		return value
	}
	if !r.refreshing {
		r.refreshing = true
		r.localProbeDone = make(chan struct{})
		probeDone := r.localProbeDone
		r.mu.Unlock()
		local := r.resolveInstalledVersion(executorCommand)
		r.mu.Lock()
		if local != "" {
			r.value = local
			r.expiresAt = time.Now().Add(ampClientVersionCacheDuration / 4)
		}
		close(probeDone)
		r.localProbeDone = nil
		value := r.value
		r.mu.Unlock()
		go r.refresh()
		return value
	}
	value := r.value
	probeDone := r.localProbeDone
	r.mu.Unlock()
	if value == "" && probeDone != nil {
		<-probeDone
		r.mu.Lock()
		value = r.value
		r.mu.Unlock()
	}
	return value
}

func (r *ampClientVersionResolver) refresh() {
	version := r.resolveLatestVersion()
	r.mu.Lock()
	defer r.mu.Unlock()
	if version != "" {
		r.value = version
		r.expiresAt = time.Now().Add(ampClientVersionCacheDuration)
	}
	r.refreshing = false
}

func (r *ampClientVersionResolver) resolveInstalledVersion(executorCommand string) string {
	if r != nil && r.installedVersion != nil {
		return r.installedVersion(executorCommand)
	}
	return ampInstalledClientVersion(executorCommand)
}

func (r *ampClientVersionResolver) resolveLatestVersion() string {
	if r != nil && r.latestVersion != nil {
		return r.latestVersion()
	}
	return fetchAmpCLINPMLatestVersion()
}

func (r *ampClientVersionResolver) set(version string, expiresAt time.Time) {
	version = strings.TrimSpace(version)
	if r == nil || version == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.value = version
	r.expiresAt = expiresAt
}

func fetchAmpCLINPMLatestVersion() string {
	resp, err := http.Get(ampCLINPMLatestURL)
	if err != nil {
		log.Debugf("amp client version: npm latest lookup failed: %v", err)
		return ""
	}
	defer func() {
		if errClose := resp.Body.Close(); errClose != nil {
			log.Debugf("amp client version: close npm response failed: %v", errClose)
		}
	}()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		log.Debugf("amp client version: npm latest lookup status=%d", resp.StatusCode)
		return ""
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Debugf("amp client version: read npm response failed: %v", err)
		return ""
	}
	var payload struct {
		Version string `json:"version"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		log.Debugf("amp client version: parse npm response failed: %v", err)
		return ""
	}
	return strings.TrimSpace(payload.Version)
}

func ampInstalledClientVersion(executorCommand string) string {
	cfg := &config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{ExecutorCommand: strings.TrimSpace(executorCommand)}}}
	command, err := neoAmpExecutorCommand(cfg)
	if err != nil {
		log.Debugf("amp client version: installed amp lookup failed: %v", err)
		return ""
	}
	ctx, cancel := context.WithTimeout(context.Background(), ampClientVersionProbeTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, command, "--version")
	cmd.Env = ampClientVersionProbeEnv(os.Environ())
	out, err := cmd.Output()
	if err != nil {
		log.Debugf("amp client version: %s --version failed: %v", command, err)
		return ""
	}
	return ampClientVersionFromOutput(string(out))
}

func ampClientVersionProbeEnv(base []string) []string {
	out := make([]string, 0, len(base))
	for _, value := range base {
		key, _, _ := strings.Cut(value, "=")
		if key == "AMP_REMOTE_CONTROL_TERMINAL" {
			continue
		}
		out = append(out, value)
	}
	return out
}

func ampClientVersionFromOutput(output string) string {
	return strings.TrimSpace(ampClientVersionPattern.FindString(output))
}
