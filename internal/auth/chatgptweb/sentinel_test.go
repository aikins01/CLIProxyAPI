package chatgptweb

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func isolateBuildCache(t *testing.T) {
	t.Helper()
	buildCache.mu.Lock()
	original := buildCache.entries
	buildCache.entries = make(map[string]buildInfo)
	buildCache.mu.Unlock()
	t.Cleanup(func() {
		buildCache.mu.Lock()
		buildCache.entries = original
		buildCache.mu.Unlock()
	})
}

func TestScrapeBuildInfoExtractsQuotedAttributes(t *testing.T) {
	const userAgent = "Mozilla/5.0 AppleWebKit/537.36 Chrome/140.0.0.0 Safari/537.36"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("User-Agent") != userAgent || r.Header.Get("Cookie") != "a=b" {
			t.Error("homepage request did not preserve session headers")
		}
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(`<html><head DATA-BUILD="dpl-abc123"><script defer src="https://cdn.example/_next/static/app.js"></script><SCRIPT nonce="n" SRC = '/backend-api/sentinel/sdk.js'></SCRIPT></head><body></body></html>`))
	}))
	t.Cleanup(srv.Close)
	useTestBaseURLs(t, srv.URL)

	dpl, scriptSrc, _, err := scrapeBuildInfo(t.Context(), srv.Client(), &Session{Cookie: "a=b", UserAgent: userAgent})
	if err != nil {
		t.Fatal(err)
	}
	if dpl != "dpl-abc123" {
		t.Errorf("dpl = %q, want %q", dpl, "dpl-abc123")
	}
	if scriptSrc != srv.URL+"/backend-api/sentinel/sdk.js" {
		t.Errorf("scriptSrc = %q", scriptSrc)
	}
}

func TestFetchBuildInfoDoesNotCacheHomepageFailure(t *testing.T) {
	isolateBuildCache(t)

	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(`<html data-build="fresh"><script src="/backend-api/sentinel/sdk.js"></script></html>`))
	}))
	t.Cleanup(srv.Close)
	useTestBaseURLs(t, srv.URL)

	if dpl, script, _ := fetchBuildInfo(t.Context(), srv.Client(), nil); dpl != "" || script != "" {
		t.Fatal("failed homepage fetch returned build data")
	}
	if dpl, script, _ := fetchBuildInfo(t.Context(), srv.Client(), nil); dpl != "fresh" || script == "" {
		t.Fatalf("retry did not recover build info")
	}
	if calls.Load() != 2 {
		t.Fatalf("homepage calls = %d", calls.Load())
	}
}

func TestFetchBuildInfoCachesBySessionIdentity(t *testing.T) {
	isolateBuildCache(t)
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		build := "build-a"
		if strings.Contains(r.Header.Get("Cookie"), "session-b") {
			build = "build-b"
		}
		_, _ = fmt.Fprintf(w, `<html data-build="%s"><script src="/backend-api/sentinel/sdk.js"></script></html>`, build)
	}))
	t.Cleanup(srv.Close)
	useTestBaseURLs(t, srv.URL)

	sessionA := &Session{Cookie: "__Secure-next-auth.session-token=session-a", AccountID: "account-a", DeviceID: "device-a", UserAgent: DefaultUserAgent, cacheIdentity: "proxy"}
	sessionB := &Session{Cookie: "__Secure-next-auth.session-token=session-b", AccountID: "account-b", DeviceID: "device-b", UserAgent: DefaultUserAgent, cacheIdentity: "proxy"}
	if dpl, _, _ := fetchBuildInfo(t.Context(), srv.Client(), sessionA); dpl != "build-a" {
		t.Fatalf("session A build = %q", dpl)
	}
	if dpl, _, _ := fetchBuildInfo(t.Context(), srv.Client(), sessionB); dpl != "build-b" {
		t.Fatalf("session B build = %q", dpl)
	}
	if dpl, _, _ := fetchBuildInfo(t.Context(), srv.Client(), sessionA); dpl != "build-a" {
		t.Fatalf("cached session A build = %q", dpl)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("homepage calls = %d, want 2", got)
	}
}

func TestFetchBuildInfoCancellationDoesNotFailSameIdentity(t *testing.T) {
	isolateBuildCache(t)
	blockedStarted := make(chan struct{})
	blockedCanceled := make(chan struct{})
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			close(blockedStarted)
			<-r.Context().Done()
			close(blockedCanceled)
			return
		}
		_, _ = w.Write([]byte(`<html data-build="independent"><script src="/backend-api/sentinel/sdk.js"></script></html>`))
	}))
	t.Cleanup(srv.Close)
	useTestBaseURLs(t, srv.URL)

	session := &Session{Cookie: "__Secure-next-auth.session-token=session", AccountID: "account", DeviceID: "device"}
	blockedCtx, cancelBlocked := context.WithCancel(t.Context())
	blockedDone := make(chan struct{})
	go func() {
		defer close(blockedDone)
		fetchBuildInfo(blockedCtx, srv.Client(), session)
	}()
	<-blockedStarted
	independentCtx, cancelIndependent := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancelIndependent()
	if dpl, _, _ := fetchBuildInfo(independentCtx, srv.Client(), session); dpl != "independent" {
		t.Fatalf("healthy caller build = %q", dpl)
	}
	cancelBlocked()
	select {
	case <-blockedDone:
	case <-time.After(5 * time.Second):
		t.Fatal("canceled build caller remained blocked")
	}
	select {
	case <-blockedCanceled:
	case <-time.After(5 * time.Second):
		t.Fatal("canceled build fetch remained active")
	}
}

func TestFNV1a32GoldenVectors(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   string
		want string
	}{
		{"", "ab3e7c0b"},
		{"a", "1a80b1b3"},
		{"foobar", "0c0da6dc"},
		{"hello world", "b90456ec"},
	}
	for _, tc := range cases {
		if got := fnv1a32(tc.in); got != tc.want {
			t.Errorf("fnv1a32(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestBrowserConfigShape(t *testing.T) {
	t.Parallel()
	cfg := browserConfig("dev-1", "dpl-1", "https://example/sdk.js", DefaultUserAgent)
	if len(cfg) != 25 {
		t.Fatalf("config has %d slots, want 25", len(cfg))
	}
	if cfg[14] != "dev-1" {
		t.Errorf("slot 14 (device id) = %v", cfg[14])
	}
	if cfg[4] != DefaultUserAgent {
		t.Errorf("slot 4 (user agent) = %v", cfg[4])
	}
}

func TestSentinelUsesSessionUserAgent(t *testing.T) {
	t.Parallel()
	const userAgent = "Mozilla/5.0 AppleWebKit/537.36 Chrome/140.0.0.0 Safari/537.36"
	cfg := browserConfig("dev-1", "dpl-1", "https://example/sdk.js", userAgent)
	if cfg[4] != userAgent {
		t.Fatalf("token User-Agent = %v", cfg[4])
	}
	req, err := newSentinelRequest(t.Context(), "https://chatgpt.com/backend-api/sentinel", "/sentinel", []byte(`{}`), &Session{
		Cookie:       "a=b",
		AccountID:    "account-1",
		DeviceID:     "dev-1",
		WebSessionID: "session-1",
		UserAgent:    userAgent,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := req.Header.Get("User-Agent"); got != userAgent {
		t.Fatalf("request User-Agent = %q", got)
	}
	if got := req.Header.Get("chatgpt-account-id"); got != "account-1" {
		t.Fatalf("request account ID = %q", got)
	}
	if got := req.Header.Get("OAI-Session-Id"); got != "session-1" {
		t.Fatalf("request web session ID = %q", got)
	}
}

func TestRequirementsTokenFormat(t *testing.T) {
	t.Parallel()
	tok := generateRequirementsToken("dev-1", "dpl-1", "", DefaultUserAgent)
	if !strings.HasPrefix(tok, prefixRequirements) || !strings.HasSuffix(tok, configSuffix) {
		t.Fatalf("requirements token format wrong: %q", tok)
	}
	cfg := decodeTokenConfig(t, tok, prefixRequirements)
	if cfg[3] != float64(1) {
		t.Errorf("prepare token slot 3 = %v, want 1", cfg[3])
	}
}

func TestSolveProofZeroDifficulty(t *testing.T) {
	t.Parallel()
	tok, err := solveProof(context.Background(), "seed-string", "", "dev-1", "dpl-1", "", DefaultUserAgent)
	if err != nil {
		t.Fatalf("zero difficulty should solve immediately: %v", err)
	}
	if !strings.HasPrefix(tok, prefixProof) || !strings.HasSuffix(tok, configSuffix) {
		t.Fatalf("proof token format wrong: %q", tok)
	}
}

func TestSolveProofOversizedDifficulty(t *testing.T) {
	t.Parallel()
	if _, err := solveProof(context.Background(), "seed", "ffffffff0", "dev-1", "dpl-1", "", DefaultUserAgent); err == nil {
		t.Fatal("expected error for difficulty longer than the hash")
	}
	if _, err := solveProof(context.Background(), "seed", "0g", "dev-1", "dpl-1", "", DefaultUserAgent); err == nil {
		t.Fatal("expected error for non-hexadecimal difficulty")
	}
}

func TestSolveProofSolutionSatisfiesPrefix(t *testing.T) {
	t.Parallel()
	const difficulty = "00"
	tok, err := solveProof(context.Background(), "some-seed", difficulty, "dev-1", "dpl-1", "", DefaultUserAgent)
	if err != nil {
		t.Fatalf("solve: %v", err)
	}
	raw := strings.TrimSuffix(strings.TrimPrefix(tok, prefixProof), configSuffix)
	if got := fnv1a32("some-seed" + raw); !strings.HasPrefix(got, difficulty) {
		t.Fatalf("solution hash %q does not satisfy difficulty %q", got, difficulty)
	}
}

func TestSolveProofConfigShapePreserved(t *testing.T) {
	t.Parallel()
	tok, err := solveProof(context.Background(), "seed", "", "dev-1", "dpl-1", "", DefaultUserAgent)
	if err != nil {
		t.Fatalf("solve: %v", err)
	}
	cfg := decodeTokenConfig(t, tok, prefixProof)
	want := len(browserConfig("dev-1", "dpl-1", "", DefaultUserAgent))
	if len(cfg) != want {
		t.Fatalf("proof config has %d fields, want %d", len(cfg), want)
	}
	nonce, ok := cfg[3].(float64)
	if !ok || nonce != 0 {
		t.Fatalf("slot 3 must be the nonce, got %v", cfg[3])
	}
	if _, ok := cfg[9].(float64); !ok {
		t.Fatalf("slot 9 must be the elapsed time, got %v", cfg[9])
	}
}

func TestSentinelRequestHeaders(t *testing.T) {
	t.Parallel()
	s := &Session{Cookie: "a=b; oai-did=device-123", AccessToken: "tok", DeviceID: "device-123"}
	req, err := newSentinelRequest(t.Context(), "https://example.test/x", sentinelPreparePath, []byte(`{"p":"x"}`), s)
	if err != nil {
		t.Fatal(err)
	}
	wantPath := "/backend-api" + sentinelPreparePath
	if got := req.Header.Get("X-OpenAI-Target-Path"); got != wantPath {
		t.Errorf("target path = %q, want %q", got, wantPath)
	}
	if got := req.Header.Get("X-OpenAI-Target-Route"); got != wantPath {
		t.Errorf("target route = %q, want %q", got, wantPath)
	}
	if got := req.Header.Get("OAI-Device-Id"); got != "device-123" {
		t.Errorf("device id header = %q", got)
	}
	if got := req.Header.Get("Cookie"); !strings.Contains(got, "oai-did=device-123") {
		t.Errorf("device cookie missing: %q", got)
	}
}

func TestPostSentinelErrorOmitsBody(t *testing.T) {
	srv := newStatusServer(t, 403, `{"detail":"turnstile is not required: secret-response-body"}`)
	_, _, err := postSentinel(t.Context(), srv.Client(), srv.URL, sentinelPreparePath, nil, []byte(`{}`))
	if err == nil {
		t.Fatal("expected error for non-200")
	}
	if errors.Is(err, ErrTurnstileRequired) {
		t.Fatalf("generic 403 was classified as Turnstile: %v", err)
	}
	var statusErr interface{ StatusCode() int }
	if !errors.As(err, &statusErr) || statusErr.StatusCode() != http.StatusForbidden {
		t.Fatalf("status error = %v", err)
	}
	if strings.Contains(err.Error(), "secret-response-body") {
		t.Errorf("error leaks upstream body: %v", err)
	}

	stringErrorServer := newStatusServer(t, 403, `{"error":"turnstile is not required"}`)
	_, _, err = postSentinel(t.Context(), stringErrorServer.Client(), stringErrorServer.URL, sentinelPreparePath, nil, []byte(`{}`))
	if errors.Is(err, ErrTurnstileRequired) {
		t.Fatalf("negative string error was classified as Turnstile: %v", err)
	}
	messageServer := newStatusServer(t, 403, `{"message":"turnstile is not required"}`)
	_, _, err = postSentinel(t.Context(), messageServer.Client(), messageServer.URL, sentinelPreparePath, nil, []byte(`{}`))
	if errors.Is(err, ErrTurnstileRequired) {
		t.Fatalf("negative message was classified as Turnstile: %v", err)
	}
}

func TestPostSentinelClassifiesTurnstileChallenge(t *testing.T) {
	for name, test := range map[string]struct {
		mitigated   string
		contentType string
		body        string
	}{
		"mitigation header": {mitigated: "challenge"},
		"challenge html": {
			contentType: "text/html",
			body:        `<script src="/cdn-cgi/challenge-platform/h/b/orchestrate/chl_page/v1"></script>`,
		},
		"turnstile response": {
			contentType: "application/json",
			body:        `{"detail":"turnstile required"}`,
		},
		"turnstile object": {
			contentType: "application/json",
			body:        `{"turnstile":{"required":true}}`,
		},
		"nested error message": {
			contentType: "application/json",
			body:        `{"error":{"message":"A Turnstile token is required. Please retry."}}`,
		},
		"string error": {
			contentType: "application/json",
			body:        `{"error":"Turnstile token is required"}`,
		},
		"top-level message": {
			contentType: "application/json",
			body:        `{"message":"Turnstile token is required"}`,
		},
		"comma-delimited message": {
			contentType: "application/json",
			body:        `{"message":"Turnstile token is required, please retry."}`,
		},
		"imperative challenge message": {
			contentType: "application/json",
			body:        `{"message":"Please complete the Turnstile challenge to continue"}`,
		},
		"required clause beats unrelated negation": {
			contentType: "application/json",
			body:        `{"message":"Turnstile token is required; login is not required"}`,
		},
		"token-required code": {
			contentType: "application/json",
			body:        `{"code":"turnstile_token_required"}`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Cf-Mitigated", test.mitigated)
				w.Header().Set("Content-Type", test.contentType)
				w.WriteHeader(http.StatusForbidden)
				_, _ = w.Write([]byte(test.body))
			}))
			t.Cleanup(srv.Close)
			_, _, err := postSentinel(t.Context(), srv.Client(), srv.URL, sentinelPreparePath, nil, []byte(`{}`))
			if !errors.Is(err, ErrTurnstileRequired) {
				t.Fatalf("error = %v, want ErrTurnstileRequired", err)
			}
			var neutral interface{ AuthStateNeutral() bool }
			if !errors.As(err, &neutral) || !neutral.AuthStateNeutral() {
				t.Fatalf("HTTP Turnstile error is not auth-state neutral: %v", err)
			}
			if !strings.Contains(err.Error(), "recopying a fresh full Cookie header") {
				t.Fatalf("error is not actionable: %v", err)
			}
		})
	}
}

func TestFetchRequirementsCarriesCookieRotationsAcrossStages(t *testing.T) {
	isolateBuildCache(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/":
			w.Header().Add("Set-Cookie", "cf_clearance=homepage-rotation; Path=/; HttpOnly")
			_, _ = w.Write([]byte(`<html data-build="build"><script src="/backend-api/sentinel/sdk.js"></script></html>`))
		case "/backend-api" + sentinelPreparePath:
			if cookie := r.Header.Get("Cookie"); !strings.Contains(cookie, "cf_clearance=homepage-rotation") {
				t.Errorf("prepare cookie = %q", cookie)
			}
			w.Header().Add("Set-Cookie", "__cf_bm=prepare-rotation; Path=/; HttpOnly")
			w.Header().Add("Set-Cookie", "oai-did=rotated-device; Path=/")
			_, _ = w.Write([]byte(`{"prepare_token":"prep"}`))
		case "/backend-api" + sentinelRequirementsPath:
			cookie := r.Header.Get("Cookie")
			if !strings.Contains(cookie, "__cf_bm=prepare-rotation") || !strings.Contains(cookie, "oai-did=old-device") || strings.Contains(cookie, "oai-did=rotated-device") {
				t.Errorf("requirements cookie = %q", cookie)
			}
			if got := r.Header.Get("OAI-Device-Id"); got != "old-device" {
				t.Errorf("requirements device ID = %q", got)
			}
			w.Header().Add("Set-Cookie", "_cfuvid=requirements-rotation; Path=/; HttpOnly")
			_, _ = w.Write([]byte(`{"token":"final","proofofwork":{"required":false},"turnstile":{"required":false}}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	useTestBaseURLs(t, srv.URL)

	session := &Session{Cookie: "__Secure-next-auth.session-token=tok; __cf_bm=old; oai-did=old-device", DeviceID: "old-device"}
	_, cookieUpdates, err := FetchRequirements(t.Context(), srv.Client(), session, RequirementsOptions{})
	if err != nil {
		t.Fatal(err)
	}
	merged := MergeRefreshedCookie(session.Cookie, cookieUpdates)
	if !strings.Contains(merged, "cf_clearance=homepage-rotation") || !strings.Contains(merged, "__cf_bm=prepare-rotation") || !strings.Contains(merged, "_cfuvid=requirements-rotation") || !strings.Contains(merged, "oai-did=rotated-device") {
		t.Fatalf("merged Sentinel rotations = %q", merged)
	}
}

func TestFetchRequirementsRejectsHomepageSessionDeletionBeforePrepare(t *testing.T) {
	isolateBuildCache(t)
	var prepareCalled atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/":
			w.Header().Add("Set-Cookie", "__Secure-next-auth.session-token=; Max-Age=0; Path=/; HttpOnly")
			_, _ = w.Write([]byte(`<html data-build="build"><script src="/backend-api/sentinel/sdk.js"></script></html>`))
		case "/backend-api" + sentinelPreparePath:
			prepareCalled.Store(true)
			_, _ = w.Write([]byte(`{"prepare_token":"prep"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	useTestBaseURLs(t, srv.URL)

	_, cookieUpdates, err := FetchRequirements(t.Context(), srv.Client(), &Session{Cookie: "__Secure-next-auth.session-token=tok"}, RequirementsOptions{})
	if err == nil {
		t.Fatal("homepage session deletion returned no error")
	}
	var statusErr interface{ StatusCode() int }
	if !errors.As(err, &statusErr) || statusErr.StatusCode() != http.StatusUnauthorized {
		t.Fatalf("homepage session deletion error = %v", err)
	}
	if prepareCalled.Load() {
		t.Fatal("prepare called after homepage deleted the session token")
	}
	if merged := MergeRefreshedCookie("__Secure-next-auth.session-token=tok", cookieUpdates); SessionToken(merged) != "" {
		t.Fatalf("returned rotations retained session token: %q", merged)
	}
}

func TestFetchRequirementsReturnsHomepageRotationsWhenBuildDiscoveryFails(t *testing.T) {
	isolateBuildCache(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/":
			w.Header().Add("Set-Cookie", "cf_clearance=homepage-rotation; Path=/; HttpOnly")
			w.WriteHeader(http.StatusServiceUnavailable)
		case "/backend-api" + sentinelPreparePath:
			if cookie := r.Header.Get("Cookie"); !strings.Contains(cookie, "cf_clearance=homepage-rotation") {
				t.Errorf("prepare cookie = %q", cookie)
			}
			w.WriteHeader(http.StatusServiceUnavailable)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	useTestBaseURLs(t, srv.URL)

	_, cookieUpdates, err := FetchRequirements(t.Context(), srv.Client(), &Session{Cookie: "__Secure-next-auth.session-token=tok"}, RequirementsOptions{})
	if err == nil {
		t.Fatal("prepare failure returned no error")
	}
	if merged := MergeRefreshedCookie("__Secure-next-auth.session-token=tok", cookieUpdates); !strings.Contains(merged, "cf_clearance=homepage-rotation") {
		t.Fatalf("build failure dropped homepage rotation: %q", merged)
	}
}

func TestFetchRequirementsReturnsCookieRotationsOnFailure(t *testing.T) {
	isolateBuildCache(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/":
			_, _ = w.Write([]byte(`<html data-build="build"></html>`))
		case "/backend-api" + sentinelPreparePath:
			w.Header().Add("Set-Cookie", "__cf_bm=prepare-rotation; Path=/")
			_, _ = w.Write([]byte(`{"prepare_token":"prep"}`))
		case "/backend-api" + sentinelRequirementsPath:
			w.Header().Add("Set-Cookie", "_cfuvid=requirements-rotation; Path=/")
			w.WriteHeader(http.StatusServiceUnavailable)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	useTestBaseURLs(t, srv.URL)

	_, cookieUpdates, err := FetchRequirements(t.Context(), srv.Client(), &Session{Cookie: "__Secure-next-auth.session-token=tok"}, RequirementsOptions{})
	if err == nil {
		t.Fatal("requirements failure returned no error")
	}
	merged := MergeRefreshedCookie("__Secure-next-auth.session-token=tok", cookieUpdates)
	if !strings.Contains(merged, "__cf_bm=prepare-rotation") || !strings.Contains(merged, "_cfuvid=requirements-rotation") {
		t.Fatalf("failure dropped Sentinel rotations: %q", merged)
	}
}

func TestFetchRequirementsRejectsSessionDeletionAtSentinelStages(t *testing.T) {
	for _, stage := range []string{"prepare", "requirements"} {
		t.Run(stage, func(t *testing.T) {
			isolateBuildCache(t)
			var requirementsCalled atomic.Bool
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/":
					_, _ = w.Write([]byte(`<html data-build="build"></html>`))
				case "/backend-api" + sentinelPreparePath:
					if stage == "prepare" {
						w.Header().Add("Set-Cookie", "__Secure-next-auth.session-token=; Max-Age=0; Path=/; HttpOnly")
					}
					_, _ = w.Write([]byte(`{"prepare_token":"prep"}`))
				case "/backend-api" + sentinelRequirementsPath:
					requirementsCalled.Store(true)
					if stage == "requirements" {
						w.Header().Add("Set-Cookie", "__Secure-next-auth.session-token=; Max-Age=0; Path=/; HttpOnly")
					}
					_, _ = w.Write([]byte(`{"token":"final","proofofwork":{"required":false},"turnstile":{"required":false}}`))
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			t.Cleanup(srv.Close)
			useTestBaseURLs(t, srv.URL)

			_, cookieUpdates, err := FetchRequirements(t.Context(), srv.Client(), &Session{Cookie: "__Secure-next-auth.session-token=tok"}, RequirementsOptions{})
			var statusErr interface{ StatusCode() int }
			if !errors.As(err, &statusErr) || statusErr.StatusCode() != http.StatusUnauthorized {
				t.Fatalf("%s deletion error = %v", stage, err)
			}
			if stage == "prepare" && requirementsCalled.Load() {
				t.Fatal("requirements called after prepare deleted the session token")
			}
			if merged := MergeRefreshedCookie("__Secure-next-auth.session-token=tok", cookieUpdates); SessionToken(merged) != "" {
				t.Fatalf("%s deletion retained session token: %q", stage, merged)
			}
		})
	}
}

func newRequirementsGuardServer(t *testing.T, prepareBody, requirementsBody string) (*httptest.Server, *atomic.Bool) {
	t.Helper()
	requirementsCalled := &atomic.Bool{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/":
			w.Header().Set("Content-Type", "text/html")
			_, _ = w.Write([]byte(`<html><head data-build="b"></head><body></body></html>`))
		case "/backend-api" + sentinelPreparePath:
			_, _ = w.Write([]byte(prepareBody))
		case "/backend-api" + sentinelRequirementsPath:
			requirementsCalled.Store(true)
			_, _ = w.Write([]byte(requirementsBody))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	useTestBaseURLs(t, srv.URL)
	return srv, requirementsCalled
}

func TestFetchRequirementsRejectsEmptyProofSeed(t *testing.T) {
	srv, requirementsCalled := newRequirementsGuardServer(t,
		`{"prepare_token":"prep"}`,
		`{"token":"final","proofofwork":{"required":true,"seed":""},"turnstile":{"required":false}}`)
	_, _, err := FetchRequirements(t.Context(), srv.Client(), &Session{Cookie: "a=b"}, RequirementsOptions{})
	if err == nil {
		t.Fatal("required proof of work with empty seed must error")
	}
	if !requirementsCalled.Load() {
		t.Fatal("requirements stage was not called")
	}
}

func TestFetchRequirementsRejectsMissingTurnstile(t *testing.T) {
	srv, requirementsCalled := newRequirementsGuardServer(t,
		`{"prepare_token":"prep"}`,
		`{"token":"final","proofofwork":{"required":false},"turnstile":{"required":true}}`)
	_, _, err := FetchRequirements(t.Context(), srv.Client(), &Session{Cookie: "a=b"}, RequirementsOptions{})
	if err == nil {
		t.Fatal("required turnstile without a token must error")
	}
	if !errors.Is(err, ErrTurnstileRequired) {
		t.Fatalf("error = %v", err)
	}
	var statusErr interface{ StatusCode() int }
	if !errors.As(err, &statusErr) || statusErr.StatusCode() != http.StatusForbidden {
		t.Fatalf("status error = %v", err)
	}
	if !strings.Contains(err.Error(), "cookie replay cannot solve") || !strings.Contains(err.Error(), "not guaranteed") {
		t.Fatalf("error is not actionable: %v", err)
	}
	if !requirementsCalled.Load() {
		t.Fatal("requirements stage was not called")
	}
}

func TestFetchRequirementsRejectsTurnstileBeforeProofOfWork(t *testing.T) {
	srv, _ := newRequirementsGuardServer(t,
		`{"prepare_token":"prep"}`,
		`{"token":"final","proofofwork":{"required":true,"seed":""},"turnstile":{"required":true}}`)
	_, _, err := FetchRequirements(t.Context(), srv.Client(), &Session{Cookie: "a=b"}, RequirementsOptions{})
	if !errors.Is(err, ErrTurnstileRequired) {
		t.Fatalf("error = %v, want ErrTurnstileRequired", err)
	}
}

func TestFetchRequirementsRejectsPrepareTurnstile(t *testing.T) {
	srv, requirementsCalled := newRequirementsGuardServer(t,
		`{"prepare_token":"prep","turnstile":{"required":true}}`,
		`{"token":"final","turnstile":{"required":false}}`)
	_, _, err := FetchRequirements(t.Context(), srv.Client(), &Session{Cookie: "a=b"}, RequirementsOptions{})
	if !errors.Is(err, ErrTurnstileRequired) {
		t.Fatalf("error = %v, want ErrTurnstileRequired", err)
	}
	var neutral interface{ AuthStateNeutral() bool }
	if !errors.As(err, &neutral) || !neutral.AuthStateNeutral() {
		t.Fatalf("prepare Turnstile error is not auth-state neutral: %v", err)
	}
	if requirementsCalled.Load() {
		t.Fatal("requirements must not be called after prepare-stage Turnstile")
	}
}

func TestFetchRequirementsRejectsWhitespaceTurnstile(t *testing.T) {
	srv, _ := newRequirementsGuardServer(t,
		`{"prepare_token":"prep"}`,
		`{"token":"final","proofofwork":{"required":false},"turnstile":{"required":true}}`)
	_, _, err := FetchRequirements(t.Context(), srv.Client(), &Session{Cookie: "a=b"}, RequirementsOptions{TurnstileToken: " \t "})
	if !errors.Is(err, ErrTurnstileRequired) {
		t.Fatalf("error = %v, want ErrTurnstileRequired", err)
	}
}

func TestFetchRequirementsGambleReturnsTokensWithoutTurnstile(t *testing.T) {
	srv, requirementsCalled := newRequirementsGuardServer(t,
		`{"prepare_token":"prep"}`,
		`{"token":"final","proofofwork":{"required":false},"turnstile":{"required":true}}`)
	requirements, _, err := FetchRequirements(t.Context(), srv.Client(), &Session{Cookie: "a=b"}, RequirementsOptions{SubmitWithoutTurnstile: true})
	if err != nil {
		t.Fatalf("gamble returned error: %v", err)
	}
	if requirements.Token != "final" || requirements.PrepareToken != "prep" {
		t.Fatalf("requirements = %+v", requirements)
	}
	if requirements.Turnstile != "" {
		t.Fatalf("gamble must leave turnstile empty, got %q", requirements.Turnstile)
	}
	if !requirementsCalled.Load() {
		t.Fatal("requirements stage was not called")
	}
}

func TestFetchRequirementsForwardsSuppliedTurnstileToken(t *testing.T) {
	srv, _ := newRequirementsGuardServer(t,
		`{"prepare_token":"prep"}`,
		`{"token":"final","proofofwork":{"required":false},"turnstile":{"required":true}}`)
	requirements, _, err := FetchRequirements(t.Context(), srv.Client(), &Session{Cookie: "a=b"}, RequirementsOptions{TurnstileToken: "  user-captured-token \t"})
	if err != nil {
		t.Fatalf("supplied token returned error: %v", err)
	}
	if requirements.Turnstile != "user-captured-token" {
		t.Fatalf("turnstile = %q, want user-captured-token", requirements.Turnstile)
	}
}

func TestFetchRequirementsForceLoginIsUnauthorized(t *testing.T) {
	srv, requirementsCalled := newRequirementsGuardServer(t, `{"force_login":true,"prepare_token":"prep"}`, `{}`)
	_, _, err := FetchRequirements(t.Context(), srv.Client(), &Session{Cookie: "a=b"}, RequirementsOptions{})
	var statusErr interface{ StatusCode() int }
	if !errors.As(err, &statusErr) || statusErr.StatusCode() != http.StatusUnauthorized {
		t.Fatalf("error = %v", err)
	}
	if requirementsCalled.Load() {
		t.Fatal("requirements must not be called after force_login")
	}
}

func TestFetchRequirementsSendsPrekeyToSecondStage(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/":
			_, _ = w.Write([]byte(`<html data-build="build"></html>`))
		case "/backend-api" + sentinelPreparePath:
			if r.Header.Get("chatgpt-account-id") != "account-1" || r.Header.Get("OAI-Session-Id") != "session-1" {
				t.Errorf("prepare identity headers = account %q session %q", r.Header.Get("chatgpt-account-id"), r.Header.Get("OAI-Session-Id"))
			}
			_, _ = w.Write([]byte(`{"prepare_token":"prep","proofofwork":{"required":false},"turnstile":{"required":false}}`))
		case "/backend-api" + sentinelRequirementsPath:
			if r.Header.Get("chatgpt-account-id") != "account-1" || r.Header.Get("OAI-Session-Id") != "session-1" {
				t.Errorf("requirements identity headers = account %q session %q", r.Header.Get("chatgpt-account-id"), r.Header.Get("OAI-Session-Id"))
			}
			var payload map[string]string
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Errorf("decode payload: %v", err)
			}
			if len(payload) != 2 || !strings.HasPrefix(payload["p"], prefixRequirements) || payload["prepare_token"] != "prep" {
				t.Errorf("requirements payload missing prekey or prepare token")
			}
			_, _ = w.Write([]byte(`{"token":"final"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	useTestBaseURLs(t, srv.URL)

	requirements, _, err := FetchRequirements(t.Context(), srv.Client(), &Session{Cookie: "a=b", AccountID: "account-1", WebSessionID: "session-1", UserAgent: DefaultUserAgent}, RequirementsOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if requirements.Token != "final" || requirements.PrepareToken != "prep" {
		t.Fatalf("requirements = %+v", requirements)
	}
}

func TestSentinelDateStringUsesLocalZone(t *testing.T) {
	got := sentinelDateString()
	_, offset := time.Now().Zone()
	sign := "+"
	if offset < 0 {
		sign = "-"
		offset = -offset
	}
	want := fmt.Sprintf("GMT%s%02d%02d", sign, offset/3600, (offset%3600)/60)
	if !strings.Contains(got, want) {
		t.Fatalf("sentinelDateString = %q, want local offset %q", got, want)
	}
}

func decodeTokenConfig(t *testing.T, token, prefix string) []any {
	t.Helper()
	raw := strings.TrimSuffix(strings.TrimPrefix(token, prefix), configSuffix)
	data, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		t.Fatalf("decode token config: %v", err)
	}
	var cfg []any
	if err := json.Unmarshal(data, &cfg); err != nil {
		t.Fatalf("parse token config: %v", err)
	}
	return cfg
}
