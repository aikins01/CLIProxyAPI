package chatgptweb

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
)

func TestClientHintsForUA(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name         string
		ua           string
		wantMajor    string
		wantBrand    string
		wantMobile   string
		wantPlatform string
	}{
		{"empty falls back to default", "", "131", "Google Chrome", "?0", `"macOS"`},
		{"mac chrome", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36", "140", "Google Chrome", "?0", `"macOS"`},
		{"windows chrome", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36", "141", "Google Chrome", "?0", `"Windows"`},
		{"windows edge", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36 Edg/141.0.0.0", "141", "Microsoft Edge", "?0", `"Windows"`},
		{"windows opera", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36 OPR/121.0.0.0", "121", "Opera", "?0", `"Windows"`},
		{"android edge", "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Mobile Safari/537.36 EdgA/141.0.0.0", "141", "Microsoft Edge", "?1", `"Android"`},
		{"android chrome", "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Mobile Safari/537.36", "140", "Google Chrome", "?1", `"Android"`},
		{"android tablet", "Mozilla/5.0 (Linux; Android 14; Pixel Tablet) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36", "140", "Google Chrome", "?0", `"Android"`},
		{"single-component chrome", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140 Safari/537.36", "140", "Google Chrome", "?0", `"Linux"`},
		{"chromeos chrome", "Mozilla/5.0 (X11; CrOS x86_64 16093.59.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36", "140", "Google Chrome", "?0", `"Chrome OS"`},
		{"linux chrome", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36", "139", "Google Chrome", "?0", `"Linux"`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			hints := ClientHintsForUA(c.ua)
			chromiumMajor, ok := browserProductMajor(userAgentOrDefault(c.ua), "Chrome/")
			if !ok {
				t.Fatal("test User-Agent has no Chrome product")
			}
			if !strings.Contains(hints.Brands, fmt.Sprintf(`"Chromium";v="%s"`, chromiumMajor)) ||
				!strings.Contains(hints.Brands, fmt.Sprintf(`"%s";v="%s"`, c.wantBrand, c.wantMajor)) {
				t.Errorf("Brands = %q, want Chrome major %s", hints.Brands, c.wantMajor)
			}
			if hints.Mobile != c.wantMobile {
				t.Errorf("Mobile = %q, want %q", hints.Mobile, c.wantMobile)
			}
			if hints.Platform != c.wantPlatform {
				t.Errorf("Platform = %q, want %q", hints.Platform, c.wantPlatform)
			}
		})
	}
}

func TestClientHintsForChrome151(t *testing.T) {
	t.Parallel()
	const userAgent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36"
	const want = `"Not=A?Brand";v="99", "Google Chrome";v="151", "Chromium";v="151"`
	if got := ClientHintsForUA(userAgent).Brands; got != want {
		t.Fatalf("Sec-CH-UA = %q, want %q", got, want)
	}
}

func TestApplyBrowserHeadersXHRShape(t *testing.T) {
	t.Parallel()
	req, err := http.NewRequest(http.MethodPost, "https://chatgpt.com/backend-api/f/conversation", nil)
	if err != nil {
		t.Fatal(err)
	}
	ApplyBrowserHeaders(req, "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36")
	if got := req.Header.Get("Sec-CH-UA"); !strings.Contains(got, `v="140"`) {
		t.Errorf("Sec-CH-UA = %q", got)
	}
	if got := req.Header.Get("Sec-CH-UA-Mobile"); got != "?0" {
		t.Errorf("Sec-CH-UA-Mobile = %q", got)
	}
	if got := req.Header.Get("Sec-CH-UA-Platform"); got != `"macOS"` {
		t.Errorf("Sec-CH-UA-Platform = %q", got)
	}
	for header, want := range map[string]string{
		"Priority":       "u=1, i",
		"Sec-Fetch-Dest": "empty",
		"Sec-Fetch-Mode": "cors",
		"Sec-Fetch-Site": "same-origin",
	} {
		if got := req.Header.Get(header); got != want {
			t.Errorf("%s = %q, want %q", header, got, want)
		}
	}
}

func TestExchangeSessionSendsBrowserHeadersAndMintsWebSessionID(t *testing.T) {
	const userAgent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Sec-CH-UA"); !strings.Contains(got, `v="140"`) {
			t.Errorf("Sec-CH-UA = %q", got)
		}
		if got := r.Header.Get("Sec-Fetch-Mode"); got != "cors" {
			t.Errorf("Sec-Fetch-Mode = %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"accessToken":"at","user":{"id":"u"},"account":{"id":"a"}}`))
	}))
	t.Cleanup(srv.Close)
	useTestBaseURLs(t, srv.URL)

	sess, err := exchangeSession(t.Context(), srv.Client(), "__Secure-next-auth.session-token=tok", userAgent)
	if err != nil {
		t.Fatal(err)
	}
	if len(sess.WebSessionID) != 36 || strings.Count(sess.WebSessionID, "-") != 4 {
		t.Fatalf("WebSessionID = %q, want uuid-shaped", sess.WebSessionID)
	}
	id, err := newWebSessionID()
	if err != nil {
		t.Fatal(err)
	}
	if id == sess.WebSessionID {
		t.Fatal("WebSessionID was not unique per sitting")
	}
}

func TestRecordCookieRotationConcurrentPreservesWebSessionID(t *testing.T) {
	t.Parallel()
	cookie := "__Secure-next-auth.session-token=tok; __cf_bm=old"
	m := NewSessionManager()
	s := &Session{Cookie: cookie, AccessToken: "at", DeviceID: DeviceIDForCookie(cookie), WebSessionID: "wsess-race"}
	m.byID[cookieKey(cookie)] = s
	m.deviceByID[cookieKey(cookie)] = s.DeviceID

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cur := s
			for j := 0; j < 50; j++ {
				cur = m.RecordCookieRotation(cur, []string{"__cf_bm=" + strconv.Itoa(i*1000+j) + "; Path=/"}, cookie)
				if cur.WebSessionID != "wsess-race" {
					t.Errorf("WebSessionID = %q, want preserved under concurrent rotation", cur.WebSessionID)
					return
				}
			}
		}(i)
	}
	wg.Wait()
}

func TestRecordCookieRotationConcurrentMergesDistinctCookies(t *testing.T) {
	t.Parallel()
	cookie := "__Secure-next-auth.session-token=tok; __cf_bm=old; cf_clearance=old"
	m := NewSessionManager()
	s := &Session{Cookie: cookie, AccessToken: "at", DeviceID: DeviceIDForCookie(cookie), WebSessionID: "wsess-race"}
	m.byID[cookieKey(cookie)] = s
	m.deviceByID[cookieKey(cookie)] = s.DeviceID

	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, setCookie := range []string{"__cf_bm=new-bm; Path=/", "cf_clearance=new-clearance; Path=/"} {
		wg.Add(1)
		go func(setCookie string) {
			defer wg.Done()
			<-start
			m.RecordCookieRotation(s, []string{setCookie}, cookie)
		}(setCookie)
	}
	close(start)
	wg.Wait()

	m.mu.Lock()
	current := m.byID[cookieKey(cookie)]
	m.mu.Unlock()
	if current == nil || !strings.Contains(current.Cookie, "__cf_bm=new-bm") || !strings.Contains(current.Cookie, "cf_clearance=new-clearance") {
		t.Fatalf("concurrent rotations were not merged: %#v", current)
	}
}

func TestRecordCookieRotationMergesStaleSnapshotThroughConfiguredAlias(t *testing.T) {
	t.Parallel()
	configuredCookie := "__Secure-next-auth.session-token=configured"
	sessionCookie := "__Secure-next-auth.session-token=exchanged; __cf_bm=old; cf_clearance=old"
	m := NewSessionManager()
	s := &Session{Cookie: sessionCookie, AccessToken: "at", DeviceID: DeviceIDForCookie(sessionCookie)}
	for _, cookie := range []string{configuredCookie, sessionCookie} {
		m.byID[cookieKey(cookie)] = s
		m.deviceByID[cookieKey(cookie)] = s.DeviceID
	}

	first := m.RecordCookieRotation(s, []string{"__cf_bm=new-bm; Path=/"}, configuredCookie)
	second := m.RecordCookieRotation(s, []string{"cf_clearance=new-clearance; Path=/"}, configuredCookie)
	if second == s || second == first || !strings.Contains(second.Cookie, "__cf_bm=new-bm") || !strings.Contains(second.Cookie, "cf_clearance=new-clearance") {
		t.Fatalf("stale snapshot rotations were not merged: %#v", second)
	}
}

func TestRecordCookieRotationPreservesWebSessionID(t *testing.T) {
	t.Parallel()
	cookie := "__Secure-next-auth.session-token=tok; __cf_bm=old"
	m := NewSessionManager()
	s := &Session{Cookie: cookie, AccessToken: "at", DeviceID: DeviceIDForCookie(cookie), WebSessionID: "wsess-1"}
	m.byID[cookieKey(cookie)] = s
	m.deviceByID[cookieKey(cookie)] = s.DeviceID

	s = m.RecordCookieRotation(s, []string{"__cf_bm=new; Path=/"}, cookie)
	if s.WebSessionID != "wsess-1" {
		t.Fatalf("WebSessionID = %q, want preserved across rotation", s.WebSessionID)
	}
}

func TestClientHintsForUAOversizedChromeMajor(t *testing.T) {
	ua := "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99999999999999999999999999.0.0.0 Safari/537.36"
	hints := ClientHintsForUA(ua)
	if !strings.Contains(hints.Brands, `"Chromium";v="99999999999999999999999999"`) {
		t.Fatalf("brands = %q, want the oversized engine version preserved", hints.Brands)
	}
}
