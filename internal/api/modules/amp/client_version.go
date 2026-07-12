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
	ampCLINPMLatestURL            = "https://registry.npmjs.org/@ampcode%2fcli/latest"
)

var (
	ampClientVersionPattern = regexp.MustCompile(`(?:^|[^0-9A-Za-z_.+-])(\d+\.\d+\.\d+(?:-[0-9A-Za-z_-]+(?:\.[0-9A-Za-z_-]+)*)?(?:\+[0-9A-Za-z_-]+(?:\.[0-9A-Za-z_-]+)*)?)(?:$|[^0-9A-Za-z_.+-])`)
	ampClientVersions       = &ampClientVersionResolver{}
)

type ampClientVersionResolver struct {
	mu               sync.Mutex
	value            string
	expiresAt        time.Time
	refreshing       bool
	localProbeDone   chan struct{}
	installedVersion func(context.Context, string) string
	latestVersion    func(context.Context) string
}

func ampUpstreamClientVersionProvider(settings *config.AmpCode, refreshCtx context.Context) func(context.Context) string {
	if settings == nil {
		return nil
	}
	value := strings.TrimSpace(settings.UpstreamClientVersionOverride)
	if value == "" {
		return nil
	}
	if !strings.EqualFold(value, ampClientVersionAuto) && !strings.EqualFold(value, ampClientVersionLatest) {
		return func(context.Context) string { return value }
	}
	executorCommand := strings.TrimSpace(settings.NeoLocalRuntime.ExecutorCommand)
	return func(ctx context.Context) string {
		return ampClientVersions.latest(ctx, refreshCtx, executorCommand)
	}
}

func ampUpstreamClientVersion(settings *config.AmpCode) string {
	provider := ampUpstreamClientVersionProvider(settings, nil)
	if provider == nil {
		return ""
	}
	return strings.TrimSpace(provider(context.Background()))
}

func (r *ampClientVersionResolver) latest(ctx, refreshCtx context.Context, executorCommand string) string {
	if r == nil {
		return ""
	}
	refreshAllowed := refreshCtx != nil && refreshCtx.Done() != nil
	if ctx == nil {
		ctx = context.Background()
	}
	now := time.Now()
	r.mu.Lock()
	if r.value != "" && now.Before(r.expiresAt) {
		value := r.value
		r.mu.Unlock()
		return value
	}
	if r.value != "" {
		if !r.refreshing && refreshAllowed {
			r.refreshing = true
			go r.refresh(refreshCtx)
		}
		value := r.value
		r.mu.Unlock()
		return value
	}
	if ctx.Done() == nil {
		r.mu.Unlock()
		return ""
	}
	if !r.refreshing {
		r.refreshing = true
		r.localProbeDone = make(chan struct{})
		probeDone := r.localProbeDone
		r.mu.Unlock()
		local := r.resolveInstalledVersion(ctx, executorCommand)
		r.mu.Lock()
		if local != "" {
			r.value = local
			r.expiresAt = time.Now().Add(ampClientVersionCacheDuration / 4)
		}
		close(probeDone)
		r.localProbeDone = nil
		value := r.value
		if !refreshAllowed {
			r.refreshing = false
		}
		r.mu.Unlock()
		if refreshAllowed {
			go r.refresh(refreshCtx)
		}
		return value
	}
	value := r.value
	probeDone := r.localProbeDone
	r.mu.Unlock()
	if value == "" && probeDone != nil {
		select {
		case <-probeDone:
		case <-ctx.Done():
			return ""
		}
		r.mu.Lock()
		value = r.value
		r.mu.Unlock()
	}
	return value
}

func (r *ampClientVersionResolver) refresh(ctx context.Context) {
	version := r.resolveLatestVersion(ctx)
	r.mu.Lock()
	defer r.mu.Unlock()
	if version != "" {
		r.value = version
		r.expiresAt = time.Now().Add(ampClientVersionCacheDuration)
	} else if ctx.Err() == nil && r.value != "" {
		r.expiresAt = time.Now().Add(ampClientVersionCacheDuration / 4)
	}
	r.refreshing = false
}

func (r *ampClientVersionResolver) resolveInstalledVersion(ctx context.Context, executorCommand string) string {
	if r != nil && r.installedVersion != nil {
		return r.installedVersion(ctx, executorCommand)
	}
	return ampInstalledClientVersion(ctx, executorCommand)
}

func (r *ampClientVersionResolver) resolveLatestVersion(ctx context.Context) string {
	if r != nil && r.latestVersion != nil {
		return r.latestVersion(ctx)
	}
	return fetchAmpCLINPMLatestVersion(ctx)
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

func fetchAmpCLINPMLatestVersion(ctx context.Context) string {
	if ctx == nil {
		ctx = context.Background()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ampCLINPMLatestURL, nil)
	if err != nil {
		log.Debugf("amp client version: create npm latest request failed: %v", err)
		return ""
	}
	resp, err := http.DefaultClient.Do(req)
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

func ampInstalledClientVersion(ctx context.Context, executorCommand string) string {
	cfg := &config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{ExecutorCommand: strings.TrimSpace(executorCommand)}}}
	command, err := neoAmpExecutorCommand(cfg)
	if err != nil {
		log.Debugf("amp client version: installed amp lookup failed: %v", err)
		return ""
	}
	if ctx == nil {
		ctx = context.Background()
	}
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
	match := ampClientVersionPattern.FindStringSubmatch(output)
	if len(match) < 2 {
		return ""
	}
	return strings.TrimSpace(match[1])
}
