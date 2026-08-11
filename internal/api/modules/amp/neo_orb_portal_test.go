package amp

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
)

func newNeoOrbPortalTestModule(t *testing.T) (*AmpModule, *neoOrbFakeProvider, string) {
	t.Helper()
	rt, fake := newNeoOrbTestRuntime(t)
	threadID := "T-019fdec9-b0cf-745d-8da4-f250184e870e"
	actor := rt.store.ensureThreadActor(threadID)
	t.Cleanup(actor.cancel)
	manager := rt.orbManagerFor()
	manager.orbs[threadID] = &neoOrbRecord{threadID: threadID, containerID: "container-fake", state: neoOrbStateRunning, workDir: neoOrbWorkDir}
	return &AmpModule{neoRuntime: rt}, fake, threadID
}

func neoOrbPortalRequest(t *testing.T, module *AmpModule, path string, headers map[string]string) *neoOrbPortalResponse {
	t.Helper()
	router := gin.New()
	router.Any("/orb/:threadID/p/:port/*path", module.serveOrbPortal)
	server := httptest.NewServer(router)
	t.Cleanup(server.Close)
	req, err := http.NewRequest(http.MethodGet, server.URL+path, nil)
	if err != nil {
		t.Fatalf("portal request: %v", err)
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatalf("portal round trip: %v", err)
	}
	defer func() {
		if errClose := resp.Body.Close(); errClose != nil {
			t.Logf("portal response body close: %v", errClose)
		}
	}()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("portal read body: %v", err)
	}
	return &neoOrbPortalResponse{status: resp.StatusCode, header: resp.Header, body: string(body)}
}

type neoOrbPortalResponse struct {
	status int
	header http.Header
	body   string
}

func TestNeoOrbPortalRejectsInvalidThreadAndMissingOrb(t *testing.T) {
	module, _, threadID := newNeoOrbPortalTestModule(t)
	recorder := neoOrbPortalRequest(t, module, "/orb/not-a-thread/p/3000/", nil)
	if recorder.status != http.StatusNotFound {
		t.Fatalf("invalid thread status = %d", recorder.status)
	}
	recorder = neoOrbPortalRequest(t, module, "/orb/T-00000000-0000-4000-8000-000000000000/p/3000/", nil)
	if recorder.status != http.StatusNotFound {
		t.Fatalf("missing orb status = %d", recorder.status)
	}
	manager := module.neoRuntime.orbManagerFor()
	manager.orbs[threadID].state = neoOrbStatePaused
	recorder = neoOrbPortalRequest(t, module, "/orb/"+threadID+"/p/3000/", nil)
	if recorder.status != http.StatusConflict {
		t.Fatalf("paused orb status = %d", recorder.status)
	}
	manager.orbs[threadID].state = neoOrbStateRunning
	recorder = neoOrbPortalRequest(t, module, "/orb/"+threadID+"/p/0/", nil)
	if recorder.status != http.StatusBadRequest {
		t.Fatalf("invalid port status = %d", recorder.status)
	}
}

func TestNeoOrbPortalProxiesToContainer(t *testing.T) {
	module, fake, threadID := newNeoOrbPortalTestModule(t)
	fake.inspectState = neoOrbContainerState{Exists: true, Running: true, IPAddress: "172.17.0.9"}

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" {
			w.WriteHeader(http.StatusTeapot)
			return
		}
		w.Header().Set("X-Upstream", "yes")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(r.Method + " " + r.URL.RequestURI()))
	}))
	defer upstream.Close()

	original := neoOrbPortalTargetURL
	neoOrbPortalTargetURL = func(containerIP string, port int) (string, error) {
		if containerIP != "172.17.0.9" {
			t.Fatalf("portal container IP = %q", containerIP)
		}
		return upstream.URL, nil
	}
	t.Cleanup(func() { neoOrbPortalTargetURL = original })

	recorder := neoOrbPortalRequest(t, module, "/orb/"+threadID+"/p/3000/app/index.html?q=1", map[string]string{
		"Authorization": "Bearer secret",
		"Cookie":        "session=abc",
	})
	if recorder.status != http.StatusOK {
		t.Fatalf("portal status = %d body=%s", recorder.status, recorder.body)
	}
	if recorder.header.Get("X-Upstream") != "yes" {
		t.Fatalf("missing upstream header: %#v", recorder.header)
	}
	if recorder.body != "GET /app/index.html?q=1" {
		t.Fatalf("proxied path = %q", recorder.body)
	}
}

func TestNeoOrbPortalRecoversContainerOnFirstRequest(t *testing.T) {
	module, fake, threadID := newNeoOrbPortalTestModule(t)
	manager := module.neoRuntime.orbManagerFor()
	manager.mu.Lock()
	manager.orbs = map[string]*neoOrbRecord{}
	manager.recovered = false
	manager.recoveredKey = ""
	manager.mu.Unlock()
	writeNeoOrbPersistedThread(t, module.neoRuntime, threadID, "sandbox", "recovered-container")
	fake.containers = []neoOrbContainerSummary{{ID: "recovered-container", Labels: map[string]string{"cliproxy.orb": threadID}}}
	fake.inspectState = neoOrbContainerState{Exists: true, Running: true, IPAddress: "172.17.0.10"}

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("recovered portal"))
	}))
	defer upstream.Close()
	original := neoOrbPortalTargetURL
	neoOrbPortalTargetURL = func(containerIP string, port int) (string, error) {
		if containerIP != "172.17.0.10" || port != 3000 {
			t.Fatalf("portal target = %q:%d", containerIP, port)
		}
		return upstream.URL, nil
	}
	t.Cleanup(func() { neoOrbPortalTargetURL = original })

	recorder := neoOrbPortalRequest(t, module, "/orb/"+threadID+"/p/3000/", nil)
	if recorder.status != http.StatusOK || recorder.body != "recovered portal" {
		t.Fatalf("recovered portal response = %d %q", recorder.status, recorder.body)
	}
	if record, ok := manager.snapshot(threadID); !ok || record.containerID != "recovered-container" {
		t.Fatalf("recovered portal record = %#v, %t", record, ok)
	}
	if fake.callCount("list-orbs") != 1 {
		t.Fatalf("portal recovery calls = %#v", fake.calls)
	}
}

func TestNeoOrbPortalDisabledOrbs(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	module := &AmpModule{neoRuntime: rt}
	recorder := neoOrbPortalRequest(t, module, "/orb/T-00000000-0000-4000-8000-000000000000/p/3000/", nil)
	if recorder.status != http.StatusNotFound {
		t.Fatalf("disabled orbs portal status = %d body=%s", recorder.status, recorder.body)
	}
	if !strings.Contains(recorder.body, "not found") {
		t.Fatalf("disabled orbs body = %s", recorder.body)
	}
}
