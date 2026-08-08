package amp

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type neoOrbFakeDockerCall struct {
	Method string
	Path   string
	Body   []byte
}

func newNeoOrbFakeDocker(t *testing.T, handler func(call neoOrbFakeDockerCall) (int, any)) (*neoOrbDockerClient, *[]neoOrbFakeDockerCall) {
	t.Helper()
	calls := make([]neoOrbFakeDockerCall, 0)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		call := neoOrbFakeDockerCall{Method: r.Method, Path: strings.TrimPrefix(r.URL.RequestURI(), "/v1.43"), Body: body}
		calls = append(calls, call)
		status, payload := handler(call)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		switch typed := payload.(type) {
		case nil:
		case string:
			fmt.Fprint(w, typed)
		case []byte:
			_, _ = w.Write(typed)
		default:
			_ = json.NewEncoder(w).Encode(typed)
		}
	}))
	t.Cleanup(server.Close)
	client, err := newNeoOrbDockerClient("tcp://" + strings.TrimPrefix(server.URL, "http://"))
	if err != nil {
		t.Fatalf("newNeoOrbDockerClient: %v", err)
	}
	return client, &calls
}

func TestNeoOrbDockerClientHostResolution(t *testing.T) {
	t.Setenv("DOCKER_HOST", "")
	client, err := newNeoOrbDockerClient("")
	if err != nil || client == nil {
		t.Fatalf("default unix client = %v, %v", client, err)
	}
	t.Setenv("DOCKER_HOST", "tcp://docker.internal:2375")
	client, err = newNeoOrbDockerClient("")
	if err != nil || !strings.HasPrefix(client.baseURL, "http://docker.internal:2375/") {
		t.Fatalf("DOCKER_HOST client base = %v, %v", client.baseURL, err)
	}
	if _, err = newNeoOrbDockerClient("npipe:////./pipe/docker"); err == nil {
		t.Fatal("unsupported scheme accepted")
	}
}

func TestNeoOrbDockerClientContainerLifecycle(t *testing.T) {
	client, calls := newNeoOrbFakeDocker(t, func(call neoOrbFakeDockerCall) (int, any) {
		switch {
		case call.Method == http.MethodPost && strings.HasPrefix(call.Path, "/containers/create"):
			if !strings.Contains(call.Path, "name=cliproxy-orb-T-1") {
				return 400, map[string]any{"message": "missing name"}
			}
			var payload map[string]any
			if err := json.Unmarshal(call.Body, &payload); err != nil {
				return 400, map[string]any{"message": "bad json"}
			}
			host := mapValue(payload["HostConfig"])
			if payload["Image"] != "debian:12-slim" || int64(mapValue(host)["NanoCPUs"].(float64)) != 2e9 || int64(mapValue(host)["Memory"].(float64)) != 2048*1024*1024 {
				return 400, map[string]any{"message": "bad spec"}
			}
			return 201, map[string]any{"Id": "container-1"}
		case call.Path == "/containers/container-1/start":
			return 204, nil
		case call.Path == "/containers/container-1/pause":
			return 204, nil
		case call.Path == "/containers/container-1/unpause":
			return 204, nil
		case strings.HasPrefix(call.Path, "/containers/container-1") && call.Method == http.MethodDelete:
			return 204, nil
		case strings.HasPrefix(call.Path, "/containers/missing") && call.Method == http.MethodDelete:
			return 404, map[string]any{"message": "no such container"}
		}
		return 500, map[string]any{"message": "unexpected " + call.Method + " " + call.Path}
	})
	ctx := context.Background()
	id, err := client.CreateContainer(ctx, neoOrbContainerSpec{
		Name:     "cliproxy-orb-T-1",
		Image:    "debian:12-slim",
		NanoCPUs: 2e9,
		MemoryMB: 2048,
	})
	if err != nil || id != "container-1" {
		t.Fatalf("CreateContainer = %q, %v", id, err)
	}
	if err := client.StartContainer(ctx, id); err != nil {
		t.Fatalf("StartContainer: %v", err)
	}
	if err := client.PauseContainer(ctx, id); err != nil {
		t.Fatalf("PauseContainer: %v", err)
	}
	if err := client.UnpauseContainer(ctx, id); err != nil {
		t.Fatalf("UnpauseContainer: %v", err)
	}
	if err := client.RemoveContainer(ctx, id, true); err != nil {
		t.Fatalf("RemoveContainer: %v", err)
	}
	if err := client.RemoveContainer(ctx, "missing", true); err != nil {
		t.Fatalf("RemoveContainer missing: %v", err)
	}
	if len(*calls) != 6 {
		t.Fatalf("calls = %d, want 6", len(*calls))
	}
}

func TestNeoOrbDockerClientInspect(t *testing.T) {
	client, _ := newNeoOrbFakeDocker(t, func(call neoOrbFakeDockerCall) (int, any) {
		switch {
		case strings.HasPrefix(call.Path, "/containers/running/json"):
			return 200, map[string]any{
				"Image":           "debian:12-slim",
				"State":           map[string]any{"Running": true, "Paused": false},
				"NetworkSettings": map[string]any{"Networks": map[string]any{"bridge": map[string]any{"IPAddress": "172.17.0.4"}}},
			}
		case strings.HasPrefix(call.Path, "/containers/gone/json"):
			return 404, map[string]any{"message": "no such container"}
		}
		return 500, map[string]any{"message": "unexpected"}
	})
	state, err := client.InspectContainer(context.Background(), "running")
	if err != nil || !state.Exists || !state.Running || state.Paused || state.IPAddress != "172.17.0.4" {
		t.Fatalf("InspectContainer = %#v, %v", state, err)
	}
	state, err = client.InspectContainer(context.Background(), "gone")
	if err != nil || state.Exists {
		t.Fatalf("InspectContainer missing = %#v, %v", state, err)
	}
}

func neoOrbExecFrame(stream byte, payload string) []byte {
	frame := make([]byte, 8+len(payload))
	frame[0] = stream
	binary.BigEndian.PutUint32(frame[4:], uint32(len(payload)))
	copy(frame[8:], payload)
	return frame
}

func TestNeoOrbDockerClientExecDemuxAndExitCode(t *testing.T) {
	client, calls := newNeoOrbFakeDocker(t, func(call neoOrbFakeDockerCall) (int, any) {
		switch {
		case strings.HasPrefix(call.Path, "/containers/container-1/exec"):
			var payload map[string]any
			if err := json.Unmarshal(call.Body, &payload); err != nil {
				return 400, nil
			}
			cmd, _ := payload["Cmd"].([]any)
			if len(cmd) != 3 || cmd[0] != "/bin/sh" || payload["WorkingDir"] != "/home/user/workspace" {
				return 400, map[string]any{"message": "bad exec spec"}
			}
			return 201, map[string]any{"Id": "exec-1"}
		case call.Path == "/exec/exec-1/start":
			stream := append(neoOrbExecFrame(1, "cloned\n"), neoOrbExecFrame(2, "warning\n")...)
			return 200, stream
		case call.Path == "/exec/exec-1/json":
			return 200, map[string]any{"ExitCode": 0}
		}
		return 500, map[string]any{"message": "unexpected " + call.Path}
	})
	result, err := client.Exec(context.Background(), "container-1", []string{"/bin/sh", "-lc", "git clone x"}, nil, "/home/user/workspace")
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if result.ExitCode != 0 || result.Stdout != "cloned\n" || result.Stderr != "warning\n" {
		t.Fatalf("Exec result = %#v", result)
	}
	var startCall *neoOrbFakeDockerCall
	for i := range *calls {
		if (*calls)[i].Path == "/exec/exec-1/start" {
			startCall = &(*calls)[i]
		}
	}
	if startCall == nil || !strings.Contains(string(startCall.Body), `"Detach":false`) {
		t.Fatalf("exec start body = %v", startCall)
	}
}

func TestNeoOrbDockerClientExecFailure(t *testing.T) {
	client, _ := newNeoOrbFakeDocker(t, func(call neoOrbFakeDockerCall) (int, any) {
		switch {
		case strings.HasPrefix(call.Path, "/containers/container-1/exec"):
			return 201, map[string]any{"Id": "exec-2"}
		case call.Path == "/exec/exec-2/start":
			return 200, neoOrbExecFrame(2, "boom\n")
		case call.Path == "/exec/exec-2/json":
			return 200, map[string]any{"ExitCode": 3}
		}
		return 500, map[string]any{"message": "unexpected"}
	})
	result, err := client.Exec(context.Background(), "container-1", []string{"false"}, nil, "")
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if result.ExitCode != 3 || result.Stderr != "boom\n" {
		t.Fatalf("Exec failure result = %#v", result)
	}
}

func TestNeoOrbDockerClientEnsureImage(t *testing.T) {
	pulled := false
	client, _ := newNeoOrbFakeDocker(t, func(call neoOrbFakeDockerCall) (int, any) {
		switch {
		case strings.HasPrefix(call.Path, "/images/debian"):
			return 404, map[string]any{"message": "no such image"}
		case strings.HasPrefix(call.Path, "/images/create"):
			pulled = true
			return 200, nil
		}
		return 500, nil
	})
	if err := client.EnsureImage(context.Background(), "debian:12-slim"); err != nil || !pulled {
		t.Fatalf("EnsureImage pull = %v, pulled=%v", err, pulled)
	}

	client, _ = newNeoOrbFakeDocker(t, func(call neoOrbFakeDockerCall) (int, any) {
		if strings.HasPrefix(call.Path, "/images/debian") {
			return 200, map[string]any{"Id": "sha256:1"}
		}
		if strings.HasPrefix(call.Path, "/images/create") {
			return 500, map[string]any{"message": "must not pull"}
		}
		return 500, nil
	})
	if err := client.EnsureImage(context.Background(), "debian:12-slim"); err != nil {
		t.Fatalf("EnsureImage present: %v", err)
	}
}

func TestNeoOrbDockerClientArchiveRoundTrip(t *testing.T) {
	var uploaded []byte
	client, _ := newNeoOrbFakeDocker(t, func(call neoOrbFakeDockerCall) (int, any) {
		if strings.HasPrefix(call.Path, "/containers/c1/archive") && call.Method == http.MethodPut {
			uploaded = call.Body
			if !strings.Contains(call.Path, "path=%2Fusr%2Flocal%2Fbin") {
				return 400, map[string]any{"message": "bad dest " + call.Path}
			}
			return 200, nil
		}
		return 500, nil
	})
	if err := client.CopyFileToContainer(context.Background(), "c1", "/usr/local/bin/amp", []byte("binary-bytes"), 0o755); err != nil {
		t.Fatalf("CopyFileToContainer: %v", err)
	}
	reader := tar.NewReader(bytes.NewReader(uploaded))
	header, err := reader.Next()
	if err != nil || header.Name != "amp" || header.Mode != 0o755 {
		t.Fatalf("uploaded tar header = %#v, %v", header, err)
	}
}

func TestNeoOrbDockerClientErrorClipping(t *testing.T) {
	client, _ := newNeoOrbFakeDocker(t, func(call neoOrbFakeDockerCall) (int, any) {
		return 500, map[string]any{"message": strings.Repeat("x", 4096)}
	})
	_, _, err := client.request(context.Background(), http.MethodGet, "/_ping", nil, "")
	if err == nil || !strings.Contains(err.Error(), "status 500") {
		t.Fatalf("error = %v", err)
	}
	if len(err.Error()) > 4096+256 {
		t.Fatalf("error body not clipped: %d bytes", len(err.Error()))
	}
}
