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
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
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
	if err != nil || client == nil || client.endpoint != "unix:///var/run/docker.sock" {
		t.Fatalf("default unix client = %v, %v", client, err)
	}
	t.Setenv("DOCKER_HOST", "tcp://DOCKER.internal:2375/")
	client, err = newNeoOrbDockerClient("")
	if err != nil || client.endpoint != "http://docker.internal:2375" || !strings.HasPrefix(client.baseURL, "http://docker.internal:2375/") {
		t.Fatalf("DOCKER_HOST client = %#v, %v", client, err)
	}
	if _, err = newNeoOrbDockerClient("npipe:////./pipe/docker"); err == nil {
		t.Fatal("unsupported scheme accepted")
	}
	client, err = newNeoOrbDockerClient("tcp://[FE80::1%25DOCKER0]:2375/")
	if err != nil || client.endpoint != "http://[fe80::1%25DOCKER0]:2375" {
		t.Fatalf("scoped IPv6 Docker host = %#v, %v", client, err)
	}
	if _, err = newNeoOrbDockerClient("tcp://docker.internal:2375?"); err == nil {
		t.Fatal("empty Docker host query accepted")
	}
	credentialHost := "tcp://docker-user:docker-password-sentinel@docker.internal:2375"
	if _, err = newNeoOrbDockerClient(credentialHost); err == nil || !strings.Contains(err.Error(), "unsupported userinfo") {
		t.Fatalf("credential-bearing Docker host error = %v", err)
	} else if strings.Contains(err.Error(), "docker-user") || strings.Contains(err.Error(), "docker-password-sentinel") {
		t.Fatalf("credential-bearing Docker host leaked userinfo: %v", err)
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
			if mapValue(host["RestartPolicy"])["Name"] != "unless-stopped" {
				return 400, map[string]any{"message": "bad restart policy"}
			}
			mounts, _ := host["Mounts"].([]any)
			if len(mounts) != 2 || mapValue(mounts[0])["Source"] != "orb-home" || mapValue(mounts[0])["Target"] != "/home/user" || mapValue(mapValue(mounts[0])["VolumeOptions"])["NoCopy"] != true || mapValue(mounts[1])["Source"] != "orb-root" || mapValue(mounts[1])["Target"] != "/root" || mapValue(mapValue(mounts[1])["VolumeOptions"])["NoCopy"] != true {
				return 400, map[string]any{"message": "bad mounts"}
			}
			return 201, map[string]any{"Id": "container-1"}
		case call.Path == "/containers/container-1/start":
			return 204, nil
		case call.Path == "/containers/container-1/stop":
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
		Name:          "cliproxy-orb-T-1",
		Image:         "debian:12-slim",
		NanoCPUs:      2e9,
		MemoryMB:      2048,
		VolumeMounts:  []neoOrbVolumeMount{{Name: "orb-home", Destination: "/home/user", NoCopy: true}, {Name: "orb-root", Destination: "/root", NoCopy: true}},
		RestartPolicy: "unless-stopped",
	})
	if err != nil || id != "container-1" {
		t.Fatalf("CreateContainer = %q, %v", id, err)
	}
	if err := client.StartContainer(ctx, id); err != nil {
		t.Fatalf("StartContainer: %v", err)
	}
	if err := client.StopContainer(ctx, id); err != nil {
		t.Fatalf("StopContainer: %v", err)
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
	if len(*calls) != 7 {
		t.Fatalf("calls = %d, want 7", len(*calls))
	}
}

func TestNeoOrbDockerClientInspect(t *testing.T) {
	client, _ := newNeoOrbFakeDocker(t, func(call neoOrbFakeDockerCall) (int, any) {
		switch {
		case strings.HasPrefix(call.Path, "/containers/running/json"):
			return 200, map[string]any{
				"Id":              "running",
				"Name":            "/cliproxy-orb-generated",
				"Image":           "debian:12-slim",
				"Config":          map[string]any{"Labels": map[string]string{"cliproxy.orb": "T-1"}},
				"HostConfig":      map[string]any{"RestartPolicy": map[string]any{"Name": "unless-stopped"}},
				"Mounts":          []any{map[string]any{"Type": "volume", "Source": "/var/lib/docker/volumes/orb-home/_data", "Name": "orb-home", "Destination": "/home/user"}},
				"State":           map[string]any{"Running": true, "Paused": false},
				"NetworkSettings": map[string]any{"Networks": map[string]any{"bridge": map[string]any{"IPAddress": "172.17.0.4"}}},
			}
		case strings.HasPrefix(call.Path, "/containers/stopped/json"):
			return 200, map[string]any{
				"Id":              "stopped",
				"Image":           "debian:12-slim",
				"State":           map[string]any{"Running": false, "Paused": false},
				"NetworkSettings": map[string]any{},
			}
		case strings.HasPrefix(call.Path, "/containers/gone/json"):
			return 404, map[string]any{"message": "no such container"}
		}
		return 500, map[string]any{"message": "unexpected"}
	})
	state, err := client.InspectContainer(context.Background(), "running")
	if err != nil || !state.Exists || state.ID != "running" || state.Name != "/cliproxy-orb-generated" || !state.Running || state.Paused || state.IPAddress != "172.17.0.4" || state.Labels["cliproxy.orb"] != "T-1" || state.RestartPolicy != "unless-stopped" || len(state.Mounts) != 1 || state.Mounts[0].Type != "volume" || state.Mounts[0].Source != "/var/lib/docker/volumes/orb-home/_data" || state.Mounts[0].Name != "orb-home" || state.Mounts[0].Destination != "/home/user" {
		t.Fatalf("InspectContainer = %#v, %v", state, err)
	}
	state, err = client.InspectContainer(context.Background(), "stopped")
	if err != nil || !state.Exists || state.Running || state.Paused {
		t.Fatalf("InspectContainer stopped = %#v, %v", state, err)
	}
	state, err = client.InspectContainer(context.Background(), "gone")
	if err != nil || state.Exists {
		t.Fatalf("InspectContainer missing = %#v, %v", state, err)
	}
}

func TestNeoOrbDockerClientListsLabelledContainers(t *testing.T) {
	client, _ := newNeoOrbFakeDocker(t, func(call neoOrbFakeDockerCall) (int, any) {
		parsed, err := url.Parse(call.Path)
		if err != nil || call.Method != http.MethodGet || parsed.Path != "/containers/json" || parsed.Query().Get("all") != "true" {
			return 400, map[string]any{"message": "bad list request"}
		}
		var filters map[string][]string
		if err := json.Unmarshal([]byte(parsed.Query().Get("filters")), &filters); err != nil || !reflect.DeepEqual(filters, map[string][]string{"label": {"cliproxy.orb"}}) {
			return 400, map[string]any{"message": "bad filters"}
		}
		return 200, []any{
			map[string]any{"Id": "container-one", "Names": []string{"/cliproxy-orb-one"}, "Labels": map[string]string{"cliproxy.orb": "T-019fdec9-b0cf-745d-8da4-f250184e870e"}},
			map[string]any{"Id": "container-two", "Labels": map[string]string{"other": "value"}},
		}
	})
	containers, err := client.ListOrbContainers(context.Background())
	if err != nil || len(containers) != 2 || containers[0].ID != "container-one" || !reflect.DeepEqual(containers[0].Names, []string{"/cliproxy-orb-one"}) || containers[0].Labels["cliproxy.orb"] == "" {
		t.Fatalf("ListOrbContainers = %#v, %v", containers, err)
	}
}

func TestNeoOrbDockerClientVolumeLifecycle(t *testing.T) {
	client, calls := newNeoOrbFakeDocker(t, func(call neoOrbFakeDockerCall) (int, any) {
		parsed, err := url.Parse(call.Path)
		if err != nil {
			return 400, map[string]any{"message": "bad path"}
		}
		switch {
		case call.Method == http.MethodPost && parsed.Path == "/volumes/create":
			var payload map[string]any
			if json.Unmarshal(call.Body, &payload) != nil || payload["Name"] != "orb-home" || mapValue(payload["Labels"])["cliproxy.orb.lifecycle"] != "1" {
				return 400, map[string]any{"message": "bad create"}
			}
			return 201, map[string]any{"Name": "orb-home", "Driver": "local", "Mountpoint": "/var/lib/docker/volumes/orb-home/_data", "Labels": payload["Labels"], "Scope": "local"}
		case call.Method == http.MethodGet && parsed.Path == "/volumes/orb-home":
			return 200, map[string]any{"Name": "orb-home", "Driver": "local", "Labels": map[string]string{"cliproxy.orb.lifecycle": "1"}}
		case call.Method == http.MethodGet && parsed.Path == "/volumes/missing":
			return 404, map[string]any{"message": "no such volume"}
		case call.Method == http.MethodGet && parsed.Path == "/volumes":
			var filters map[string][]string
			if json.Unmarshal([]byte(parsed.Query().Get("filters")), &filters) != nil || !reflect.DeepEqual(filters, map[string][]string{"label": {"cliproxy.orb.lifecycle=1"}}) {
				return 400, map[string]any{"message": "bad filters"}
			}
			return 200, map[string]any{"Volumes": []any{map[string]any{"Name": "orb-home", "Driver": "local", "Labels": map[string]string{"cliproxy.orb.lifecycle": "1"}}}}
		case call.Method == http.MethodDelete && parsed.Path == "/volumes/orb-home":
			if parsed.RawQuery != "" {
				return 400, map[string]any{"message": "forced removal"}
			}
			return 204, nil
		case call.Method == http.MethodDelete && parsed.Path == "/volumes/missing":
			if parsed.RawQuery != "" {
				return 400, map[string]any{"message": "forced removal"}
			}
			return 404, map[string]any{"message": "no such volume"}
		}
		return 500, map[string]any{"message": "unexpected " + call.Method + " " + call.Path}
	})
	ctx := context.Background()
	created, err := client.CreateVolume(ctx, neoOrbVolumeSpec{Name: "orb-home", Labels: map[string]string{"cliproxy.orb.lifecycle": "1"}})
	if err != nil || !created.Exists || created.Name != "orb-home" || created.Driver != "local" || created.Mountpoint == "" {
		t.Fatalf("CreateVolume = %#v, %v", created, err)
	}
	inspected, err := client.InspectVolume(ctx, "orb-home")
	if err != nil || !inspected.Exists || inspected.Name != "orb-home" {
		t.Fatalf("InspectVolume = %#v, %v", inspected, err)
	}
	missing, err := client.InspectVolume(ctx, "missing")
	if err != nil || missing.Exists {
		t.Fatalf("InspectVolume missing = %#v, %v", missing, err)
	}
	volumes, err := client.ListOrbVolumes(ctx)
	if err != nil || len(volumes) != 1 || !volumes[0].Exists || volumes[0].Name != "orb-home" {
		t.Fatalf("ListOrbVolumes = %#v, %v", volumes, err)
	}
	if err := client.RemoveVolume(ctx, "orb-home"); err != nil {
		t.Fatalf("RemoveVolume: %v", err)
	}
	if err := client.RemoveVolume(ctx, "missing"); err != nil {
		t.Fatalf("RemoveVolume missing: %v", err)
	}
	if len(*calls) != 6 {
		t.Fatalf("calls = %d, want 6", len(*calls))
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

func TestNeoOrbDockerClientReadWorkspaceFileSecureCommandAndBounds(t *testing.T) {
	output := string([]byte{0x00, 0x01, 0xfe, 0xff})
	relative := "images/final draft [1].png"
	client, calls := newNeoOrbFakeDocker(t, func(call neoOrbFakeDockerCall) (int, any) {
		switch {
		case strings.HasPrefix(call.Path, "/containers/container-1/exec"):
			var payload map[string]any
			if err := json.Unmarshal(call.Body, &payload); err != nil {
				return 400, nil
			}
			cmd := arrayValue(payload["Cmd"])
			if payload["WorkingDir"] != neoOrbWorkspaceDir || len(cmd) != 6 || cmd[0] != "python3" || cmd[1] != "-c" || cmd[3] != "repo" || cmd[4] != relative || cmd[5] != "16" {
				return 400, map[string]any{"message": "bad workspace read spec"}
			}
			program := stringValue(cmd[2])
			for _, required := range []string{"os.open(root, os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)", "dir_fd=fd", "os.O_DIRECTORY", "os.O_NOFOLLOW", "os.O_NONBLOCK", "stat.S_ISREG", "info.st_nlink != 1", "n+1-total"} {
				if !strings.Contains(program, required) {
					return 400, map[string]any{"message": "insecure workspace read program"}
				}
			}
			if strings.Contains(program, relative) || strings.Contains(program, "os.open('.')") {
				return 400, map[string]any{"message": "path interpolated into program"}
			}
			return 201, map[string]any{"Id": "exec-read"}
		case call.Path == "/exec/exec-read/start":
			return 200, neoOrbExecFrame(1, output)
		case call.Path == "/exec/exec-read/json":
			return 200, map[string]any{"ExitCode": 0}
		}
		return 500, map[string]any{"message": "unexpected"}
	})
	got, err := client.ReadWorkspaceFile(context.Background(), "container-1", neoOrbWorkDir, relative, 16)
	if err != nil || !bytes.Equal(got, []byte(output)) {
		t.Fatalf("ReadWorkspaceFile = %v, %v", got, err)
	}
	before := len(*calls)
	if _, err := client.ReadWorkspaceFile(context.Background(), "container-1", "/home/user/workspace/repo", "../secret", 16); err == nil {
		t.Fatal("traversal path unexpectedly reached Docker")
	}
	if len(*calls) != before {
		t.Fatalf("invalid path made %d Docker calls", len(*calls)-before)
	}
	if _, err := client.ReadWorkspaceFile(context.Background(), "container-1", neoOrbWorkspaceDir+"/swapped", "image.png", 16); err == nil {
		t.Fatal("untrusted workspace root unexpectedly reached Docker")
	}
	if len(*calls) != before {
		t.Fatalf("untrusted workspace made %d Docker calls", len(*calls)-before)
	}

	oversizeClient, _ := newNeoOrbFakeDocker(t, func(call neoOrbFakeDockerCall) (int, any) {
		switch {
		case strings.HasPrefix(call.Path, "/containers/container-1/exec"):
			return 201, map[string]any{"Id": "exec-oversize"}
		case call.Path == "/exec/exec-oversize/start":
			return 200, neoOrbExecFrame(1, "123456789")
		case call.Path == "/exec/exec-oversize/json":
			return 200, map[string]any{"ExitCode": 0}
		}
		return 500, nil
	})
	if _, err := oversizeClient.ReadWorkspaceFile(context.Background(), "container-1", "/home/user/workspace/repo", "image.png", 8); err == nil {
		t.Fatal("max+1 workspace output unexpectedly accepted")
	}
}

func TestNeoOrbReadWorkspaceFileProgramRejectsSymlinkedRoot(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 is unavailable")
	}
	parent := t.TempDir()
	workspace := filepath.Join(parent, "repo")
	if err := os.MkdirAll(filepath.Join(workspace, "images"), 0o700); err != nil {
		t.Fatal(err)
	}
	relative := "images/final draft [1].png"
	want := []byte("image-data")
	if err := os.WriteFile(filepath.Join(workspace, filepath.FromSlash(relative)), want, 0o600); err != nil {
		t.Fatal(err)
	}
	run := func() ([]byte, error) {
		command := exec.CommandContext(t.Context(), python, "-c", neoOrbReadWorkspaceFileProgram, "repo", relative, "64")
		command.Dir = parent
		return command.Output()
	}
	if got, err := run(); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("safe workspace read = %q, %v", got, err)
	}
	realWorkspace := filepath.Join(parent, "repo-real")
	if err := os.Rename(workspace, realWorkspace); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(realWorkspace, workspace); err != nil {
		t.Fatal(err)
	}
	if _, err := run(); err == nil {
		t.Fatal("symlink-swapped workspace root was accepted")
	}
}

func TestNeoOrbReadWorkspaceFileProgramRejectsHardLinkedFinalFile(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 is unavailable")
	}
	parent := t.TempDir()
	workspace := filepath.Join(parent, "repo")
	if err := os.MkdirAll(filepath.Join(workspace, "images"), 0o700); err != nil {
		t.Fatal(err)
	}
	relative := "images/result.png"
	imagePath := filepath.Join(workspace, filepath.FromSlash(relative))
	if err := os.WriteFile(imagePath, []byte("image-data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(imagePath, filepath.Join(workspace, "images", "alias.png")); err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(t.Context(), python, "-c", neoOrbReadWorkspaceFileProgram, "repo", relative, "64")
	command.Dir = parent
	if output, err := command.CombinedOutput(); err == nil {
		t.Fatalf("hard-linked workspace file was accepted: %q", output)
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

type neoOrbTestByteReader struct{}

func (neoOrbTestByteReader) Read(payload []byte) (int, error) {
	for index := range payload {
		payload[index] = byte(index)
	}
	return len(payload), nil
}

func TestNeoOrbDockerClientArchiveStreamsBeyondRequestLimit(t *testing.T) {
	const archiveSize = int64(9<<20 + 173)
	uploaded := make(chan int64, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.Method {
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/x-tar")
			_, _ = io.CopyN(w, neoOrbTestByteReader{}, archiveSize)
		case http.MethodPut:
			copied, _ := io.Copy(io.Discard, request.Body)
			uploaded <- copied
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	t.Cleanup(server.Close)
	client, err := newNeoOrbDockerClient("tcp://" + strings.TrimPrefix(server.URL, "http://"))
	if err != nil {
		t.Fatalf("newNeoOrbDockerClient: %v", err)
	}
	archive, err := client.ArchiveFromContainer(context.Background(), "container-1", "/home/user")
	if err != nil {
		t.Fatalf("ArchiveFromContainer: %v", err)
	}
	read, err := io.Copy(io.Discard, archive)
	if closeErr := archive.Close(); closeErr != nil {
		t.Fatalf("archive close: %v", closeErr)
	}
	if err != nil || read != archiveSize {
		t.Fatalf("archive read = %d, %v", read, err)
	}
	if err := client.CopyArchiveToContainer(context.Background(), "container-1", "/root", io.LimitReader(neoOrbTestByteReader{}, archiveSize)); err != nil {
		t.Fatalf("CopyArchiveToContainer: %v", err)
	}
	if copied := <-uploaded; copied != archiveSize {
		t.Fatalf("uploaded = %d, want %d", copied, archiveSize)
	}
}

func TestNeoOrbDockerClientArchiveErrorsDoNotIncludeContent(t *testing.T) {
	secret := "archive-secret-content"
	client, _ := newNeoOrbFakeDocker(t, func(call neoOrbFakeDockerCall) (int, any) {
		return http.StatusInternalServerError, secret
	})
	_, err := client.ArchiveFromContainer(context.Background(), "container-1", "/home/user")
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("ArchiveFromContainer error = %v", err)
	}
	if err := client.CopyArchiveToContainer(context.Background(), "container-1", "/root", strings.NewReader(secret)); err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("CopyArchiveToContainer error = %v", err)
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
