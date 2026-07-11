package amp

import (
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
)

func TestAmpClientVersionFromOutput(t *testing.T) {
	output := "0.0.1780359918-g778c3a (released 2026-06-02T00:25:18.000Z, 34m ago)"
	if got := ampClientVersionFromOutput(output); got != "0.0.1780359918-g778c3a" {
		t.Fatalf("version = %q", got)
	}
}

func TestAmpUpstreamClientVersionProviderExplicit(t *testing.T) {
	provider := ampUpstreamClientVersionProvider(&config.AmpCode{
		UpstreamClientVersionOverride: " 0.0.1780359918-g778c3a ",
	})
	if provider == nil {
		t.Fatal("provider is nil")
	}
	if got := provider(); got != "0.0.1780359918-g778c3a" {
		t.Fatalf("version = %q", got)
	}
}

func TestAmpUpstreamClientVersionProviderEmpty(t *testing.T) {
	if provider := ampUpstreamClientVersionProvider(&config.AmpCode{}); provider != nil {
		t.Fatal("provider should be nil for empty override")
	}
}

func TestAmpClientVersionProbeEnvRemovesRemoteControlMode(t *testing.T) {
	got := ampClientVersionProbeEnv([]string{
		"PATH=/usr/bin:/bin",
		"AMP_REMOTE_CONTROL_TERMINAL=1",
		"AMP_SKIP_UPDATE_CHECK=1",
	})
	want := []string{"PATH=/usr/bin:/bin", "AMP_SKIP_UPDATE_CHECK=1"}
	if !slices.Equal(got, want) {
		t.Fatalf("probe environment = %#v, want %#v", got, want)
	}
}

func TestAmpClientVersionResolverCoalescesColdLocalProbe(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	resolver := &ampClientVersionResolver{
		installedVersion: func(string) string {
			if calls.Add(1) == 1 {
				close(started)
			}
			<-release
			return "0.0.1783758721-g462a93"
		},
		latestVersion: func() string { return "" },
	}

	results := make([]string, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		results[0] = resolver.latest("")
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("local version probe did not start")
	}
	go func() {
		defer wg.Done()
		results[1] = resolver.latest("")
	}()
	close(release)
	wg.Wait()

	if calls.Load() != 1 {
		t.Fatalf("local version probe calls = %d, want 1", calls.Load())
	}
	for i, result := range results {
		if result != "0.0.1783758721-g462a93" {
			t.Fatalf("result[%d] = %q", i, result)
		}
	}
}

func TestAmpClientVersionResolverReturnsStaleVersionWithoutLocalProbe(t *testing.T) {
	remoteStarted := make(chan struct{})
	releaseRemote := make(chan struct{})
	var localCalls atomic.Int32
	resolver := &ampClientVersionResolver{
		value:     "0.0.1783504389-g0a07a6",
		expiresAt: time.Now().Add(-time.Minute),
		installedVersion: func(string) string {
			localCalls.Add(1)
			return "0.0.1783758721-g462a93"
		},
		latestVersion: func() string {
			close(remoteStarted)
			<-releaseRemote
			return ""
		},
	}

	if got := resolver.latest(""); got != "0.0.1783504389-g0a07a6" {
		t.Fatalf("stale version = %q", got)
	}
	if localCalls.Load() != 0 {
		t.Fatalf("local version probe calls = %d, want 0", localCalls.Load())
	}
	select {
	case <-remoteStarted:
	case <-time.After(time.Second):
		t.Fatal("background version refresh did not start")
	}
	close(releaseRemote)
}
