package amp

import (
	"context"
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
	if got := ampClientVersionFromOutput("amp 1.2.3.4"); got != "" {
		t.Fatalf("four-component version = %q", got)
	}
	if got := ampClientVersionFromOutput("amp 1.2.3-rc.1+build.7"); got != "1.2.3-rc.1+build.7" {
		t.Fatalf("compound version = %q", got)
	}
	for _, output := range []string{"amp 1.2.3-", "amp 1.2.3+", "amp 1.2.3-rc.", "amp 1.2.3+build."} {
		if got := ampClientVersionFromOutput(output); got != "" {
			t.Fatalf("malformed version %q = %q", output, got)
		}
	}
}

func TestAmpUpstreamClientVersionProviderExplicit(t *testing.T) {
	provider := ampUpstreamClientVersionProvider(&config.AmpCode{
		UpstreamClientVersionOverride: " 0.0.1780359918-g778c3a ",
	}, nil)
	if provider == nil {
		t.Fatal("provider is nil")
	}
	if got := provider(context.Background()); got != "0.0.1780359918-g778c3a" {
		t.Fatalf("version = %q", got)
	}
}

func TestAmpUpstreamClientVersionProviderEmpty(t *testing.T) {
	if provider := ampUpstreamClientVersionProvider(&config.AmpCode{}, nil); provider != nil {
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
		installedVersion: func(context.Context, string) string {
			if calls.Add(1) == 1 {
				close(started)
			}
			<-release
			return "0.0.1783758721-g462a93"
		},
		latestVersion: func(context.Context) string { return "" },
	}

	results := make([]string, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		results[0] = resolver.latest(ctx, nil, "")
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("local version probe did not start")
	}
	go func() {
		defer wg.Done()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		results[1] = resolver.latest(ctx, nil, "")
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
		installedVersion: func(context.Context, string) string {
			localCalls.Add(1)
			return "0.0.1783758721-g462a93"
		},
		latestVersion: func(context.Context) string {
			close(remoteStarted)
			<-releaseRemote
			return ""
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if got := resolver.latest(context.Background(), ctx, ""); got != "0.0.1783504389-g0a07a6" {
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

func TestAmpClientVersionResolverDoesNotRefreshWithoutCancelableContext(t *testing.T) {
	var remoteCalls atomic.Int32
	resolver := &ampClientVersionResolver{
		value:            "0.0.1783758721-g462a93",
		expiresAt:        time.Now().Add(-time.Minute),
		installedVersion: func(context.Context, string) string { t.Fatal("installed version probe should not run"); return "" },
		latestVersion: func(context.Context) string {
			remoteCalls.Add(1)
			return ""
		},
	}

	if got := resolver.latest(context.Background(), nil, ""); got != "0.0.1783758721-g462a93" {
		t.Fatalf("local version = %q", got)
	}
	resolver.mu.Lock()
	refreshing := resolver.refreshing
	resolver.mu.Unlock()
	if refreshing {
		t.Fatal("remote version refresh remained active for contextless lookup")
	}
	if remoteCalls.Load() != 0 {
		t.Fatalf("remote version refresh calls = %d, want 0", remoteCalls.Load())
	}
}

func TestAmpClientVersionResolverCancelsLocalProbe(t *testing.T) {
	started := make(chan struct{})
	finished := make(chan struct{})
	resolver := &ampClientVersionResolver{
		installedVersion: func(ctx context.Context, _ string) string {
			close(started)
			<-ctx.Done()
			close(finished)
			return ""
		},
		latestVersion: func(context.Context) string { return "" },
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan string, 1)
	go func() {
		result <- resolver.latest(ctx, nil, "")
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("local version probe did not start")
	}
	cancel()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("local version probe did not observe cancellation")
	}
	select {
	case got := <-result:
		if got != "" {
			t.Fatalf("canceled local version probe = %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("local version lookup remained blocked after cancellation")
	}
}

func TestAmpClientVersionResolverBacksOffAfterRemoteFailure(t *testing.T) {
	finished := make(chan struct{}, 1)
	var calls atomic.Int32
	resolver := &ampClientVersionResolver{
		value:     "0.0.1783504389-g0a07a6",
		expiresAt: time.Now().Add(-time.Minute),
		latestVersion: func(context.Context) string {
			calls.Add(1)
			finished <- struct{}{}
			return ""
		},
	}
	refreshCtx, cancelRefresh := context.WithCancel(context.Background())
	defer cancelRefresh()
	if got := resolver.latest(context.Background(), refreshCtx, ""); got != "0.0.1783504389-g0a07a6" {
		t.Fatalf("stale version = %q", got)
	}
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("remote version refresh did not finish")
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		resolver.mu.Lock()
		refreshing := resolver.refreshing
		resolver.mu.Unlock()
		if !refreshing {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if got := resolver.latest(context.Background(), refreshCtx, ""); got != "0.0.1783504389-g0a07a6" {
		t.Fatalf("cached version after failed refresh = %q", got)
	}
	if calls.Load() != 1 {
		t.Fatalf("remote version refresh calls = %d, want 1", calls.Load())
	}
}

func TestAmpClientVersionResolverCancelsRemoteRefresh(t *testing.T) {
	started := make(chan int, 2)
	finished := make(chan int, 2)
	var calls atomic.Int32
	resolver := &ampClientVersionResolver{
		value:     "0.0.1783504389-g0a07a6",
		expiresAt: time.Now().Add(-time.Minute),
		latestVersion: func(ctx context.Context) string {
			call := int(calls.Add(1))
			started <- call
			<-ctx.Done()
			finished <- call
			return ""
		},
	}
	requestCtx, cancelRequest := context.WithCancel(context.Background())
	refreshCtxFirst, cancelRefreshFirst := context.WithCancel(context.Background())
	if got := resolver.latest(requestCtx, refreshCtxFirst, ""); got != "0.0.1783504389-g0a07a6" {
		t.Fatalf("stale version = %q", got)
	}
	select {
	case call := <-started:
		if call != 1 {
			t.Fatalf("first remote version refresh call = %d", call)
		}
	case <-time.After(time.Second):
		t.Fatal("remote version refresh did not start")
	}
	cancelRequest()
	select {
	case <-finished:
		t.Fatal("request cancellation stopped the shared remote version refresh")
	case <-time.After(20 * time.Millisecond):
	}
	cancelRefreshFirst()
	select {
	case call := <-finished:
		if call != 1 {
			t.Fatalf("first completed remote version refresh call = %d", call)
		}
	case <-time.After(time.Second):
		t.Fatal("remote version refresh did not observe cancellation")
	}
	deadline := time.Now().Add(time.Second)
	refreshing := true
	for time.Now().Before(deadline) {
		resolver.mu.Lock()
		refreshing = resolver.refreshing
		resolver.mu.Unlock()
		if !refreshing {
			break
		}
		time.Sleep(time.Millisecond)
	}
	resolver.mu.Lock()
	refreshing = resolver.refreshing
	resolver.mu.Unlock()
	if refreshing {
		t.Fatal("remote version refresh remained active after cancellation")
	}

	refreshCtxRetry, cancelRefreshRetry := context.WithCancel(context.Background())
	if got := resolver.latest(context.Background(), refreshCtxRetry, ""); got != "0.0.1783504389-g0a07a6" {
		t.Fatalf("stale version on retry = %q", got)
	}
	select {
	case call := <-started:
		if call != 2 {
			t.Fatalf("retried remote version refresh call = %d", call)
		}
	case <-time.After(time.Second):
		t.Fatal("remote version refresh did not retry")
	}
	cancelRefreshRetry()
	select {
	case call := <-finished:
		if call != 2 {
			t.Fatalf("completed retried remote version refresh call = %d", call)
		}
	case <-time.After(time.Second):
		t.Fatal("retried remote version refresh did not observe cancellation")
	}
}
