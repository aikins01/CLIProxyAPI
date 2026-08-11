package amp

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"

	log "github.com/sirupsen/logrus"
)

// neoOrbDockerClient is a minimal Docker Engine API client covering the
// container lifecycle operations orb provisioning needs. It speaks to the
// engine over a unix socket or TCP endpoint without external dependencies.
type neoOrbDockerClient struct {
	baseURL    string
	httpClient *http.Client
}

type neoOrbContainerSpec struct {
	Name        string
	Image       string
	Env         []string
	WorkingDir  string
	Cmd         []string
	NanoCPUs    int64
	MemoryMB    int64
	ExtraHosts  []string
	Labels      map[string]string
	NetworkMode string
}

type neoOrbContainerState struct {
	Running   bool
	Paused    bool
	IPAddress string
	Image     string
	Exists    bool
}

type neoOrbContainerSummary struct {
	ID     string
	Labels map[string]string
}

type neoOrbExecResult struct {
	ExitCode int
	Stdout   string
	Stderr   string
}

func newNeoOrbDockerClient(host string) (*neoOrbDockerClient, error) {
	host = strings.TrimSpace(host)
	if host == "" {
		host = strings.TrimSpace(os.Getenv("DOCKER_HOST"))
	}
	if host == "" {
		host = "unix:///var/run/docker.sock"
	}
	parsed, err := url.Parse(host)
	if err != nil {
		return nil, fmt.Errorf("docker host %q: %w", host, err)
	}
	transport := &http.Transport{}
	base := ""
	switch strings.ToLower(parsed.Scheme) {
	case "unix":
		socketPath := parsed.Path
		if socketPath == "" {
			return nil, fmt.Errorf("docker host %q has no socket path", host)
		}
		transport.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
			var dialer net.Dialer
			return dialer.DialContext(ctx, "unix", socketPath)
		}
		base = "http://docker.local"
	case "tcp", "http", "https":
		scheme := parsed.Scheme
		if scheme == "tcp" {
			scheme = "http"
		}
		base = scheme + "://" + parsed.Host
	default:
		return nil, fmt.Errorf("docker host %q uses unsupported scheme", host)
	}
	return &neoOrbDockerClient{baseURL: base + "/v1.43", httpClient: &http.Client{Transport: transport}}, nil
}

func (c *neoOrbDockerClient) request(ctx context.Context, method, path string, body io.Reader, contentType string) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return nil, 0, err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("docker %s %s: %w", method, path, err)
	}
	defer func() {
		if errClose := resp.Body.Close(); errClose != nil {
			log.Warnf("amp orbs: docker response close error: %v", errClose)
		}
	}()
	payload, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("docker %s %s: read body: %w", method, path, err)
	}
	if resp.StatusCode >= 400 {
		return payload, resp.StatusCode, fmt.Errorf("docker %s %s: status %d: %s", method, path, resp.StatusCode, clipNeoErrorBody(payload))
	}
	return payload, resp.StatusCode, nil
}

// Ping probes daemon connectivity. The caller bounds it with a provisioning
// timeout; the client itself sets no timeouts on established operations.
func (c *neoOrbDockerClient) Ping(ctx context.Context) error {
	_, _, err := c.request(ctx, http.MethodGet, "/_ping", nil, "")
	return err
}

func (c *neoOrbDockerClient) ImagePresent(ctx context.Context, image string) (bool, error) {
	_, status, err := c.request(ctx, http.MethodGet, "/images/"+url.PathEscape(image)+"/json", nil, "")
	if err != nil {
		if status == http.StatusNotFound {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (c *neoOrbDockerClient) EnsureImage(ctx context.Context, image string) error {
	present, err := c.ImagePresent(ctx, image)
	if err != nil || present {
		return err
	}
	// Image pulls stream progress frames and can exceed the generic request
	// body's 8 MiB cap; drain the stream to completion and keep only the tail
	// for error reporting.
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/images/create?fromImage="+url.QueryEscape(image), nil)
	if err != nil {
		return err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("pull image %s: %w", image, err)
	}
	defer func() {
		if errClose := resp.Body.Close(); errClose != nil {
			log.Warnf("amp orbs: docker pull stream close error: %v", errClose)
		}
	}()
	tail := make([]byte, 0, 4096)
	chunk := make([]byte, 32<<10)
	for {
		n, readErr := resp.Body.Read(chunk)
		if n > 0 {
			tail = append(tail, chunk[:n]...)
			if len(tail) > 4096 {
				tail = tail[len(tail)-4096:]
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return fmt.Errorf("pull image %s: stream: %w", image, readErr)
		}
	}
	if resp.StatusCode >= 400 {
		return fmt.Errorf("pull image %s: status %d: %s", image, resp.StatusCode, clipNeoErrorBody(tail))
	}
	return nil
}

func (c *neoOrbDockerClient) CreateContainer(ctx context.Context, spec neoOrbContainerSpec) (string, error) {
	type hostConfig struct {
		NanoCPUs    int64    `json:"NanoCPUs,omitempty"`
		Memory      int64    `json:"Memory,omitempty"`
		ExtraHosts  []string `json:"ExtraHosts,omitempty"`
		NetworkMode string   `json:"NetworkMode,omitempty"`
	}
	type createBody struct {
		Image      string            `json:"Image"`
		Env        []string          `json:"Env,omitempty"`
		Cmd        []string          `json:"Cmd,omitempty"`
		WorkingDir string            `json:"WorkingDir,omitempty"`
		Labels     map[string]string `json:"Labels,omitempty"`
		HostConfig hostConfig        `json:"HostConfig"`
	}
	payload := createBody{
		Image:      spec.Image,
		Env:        spec.Env,
		Cmd:        spec.Cmd,
		WorkingDir: spec.WorkingDir,
		Labels:     spec.Labels,
		HostConfig: hostConfig{
			NanoCPUs:    spec.NanoCPUs,
			Memory:      spec.MemoryMB * 1024 * 1024,
			ExtraHosts:  spec.ExtraHosts,
			NetworkMode: spec.NetworkMode,
		},
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	name := strings.TrimPrefix(spec.Name, "/")
	respBody, _, err := c.request(ctx, http.MethodPost, "/containers/create?name="+url.QueryEscape(name), bytes.NewReader(encoded), "application/json")
	if err != nil {
		return "", err
	}
	var created struct {
		ID string `json:"Id"`
	}
	if err := json.Unmarshal(respBody, &created); err != nil || strings.TrimSpace(created.ID) == "" {
		return "", fmt.Errorf("docker create container: unexpected response %s", clipNeoErrorBody(respBody))
	}
	return created.ID, nil
}

func (c *neoOrbDockerClient) ListOrbContainers(ctx context.Context) ([]neoOrbContainerSummary, error) {
	filters, err := json.Marshal(map[string][]string{"label": {"cliproxy.orb"}})
	if err != nil {
		return nil, err
	}
	respBody, _, err := c.request(ctx, http.MethodGet, "/containers/json?all=true&filters="+url.QueryEscape(string(filters)), nil, "")
	if err != nil {
		return nil, err
	}
	var listed []struct {
		ID     string            `json:"Id"`
		Labels map[string]string `json:"Labels"`
	}
	if err := json.Unmarshal(respBody, &listed); err != nil {
		return nil, fmt.Errorf("docker list orb containers: %w", err)
	}
	containers := make([]neoOrbContainerSummary, 0, len(listed))
	for _, container := range listed {
		if strings.TrimSpace(container.ID) == "" {
			continue
		}
		containers = append(containers, neoOrbContainerSummary{ID: container.ID, Labels: container.Labels})
	}
	return containers, nil
}

func (c *neoOrbDockerClient) StartContainer(ctx context.Context, id string) error {
	_, _, err := c.request(ctx, http.MethodPost, "/containers/"+url.PathEscape(id)+"/start", nil, "")
	return err
}

func (c *neoOrbDockerClient) PauseContainer(ctx context.Context, id string) error {
	_, _, err := c.request(ctx, http.MethodPost, "/containers/"+url.PathEscape(id)+"/pause", nil, "")
	return err
}

func (c *neoOrbDockerClient) UnpauseContainer(ctx context.Context, id string) error {
	_, _, err := c.request(ctx, http.MethodPost, "/containers/"+url.PathEscape(id)+"/unpause", nil, "")
	return err
}

func (c *neoOrbDockerClient) RemoveContainer(ctx context.Context, id string, force bool) error {
	query := ""
	if force {
		query = "?force=true"
	}
	_, status, err := c.request(ctx, http.MethodDelete, "/containers/"+url.PathEscape(id)+query, nil, "")
	if status == http.StatusNotFound {
		return nil
	}
	return err
}

func (c *neoOrbDockerClient) InspectContainer(ctx context.Context, id string) (neoOrbContainerState, error) {
	state := neoOrbContainerState{}
	respBody, status, err := c.request(ctx, http.MethodGet, "/containers/"+url.PathEscape(id)+"/json", nil, "")
	if err != nil {
		if status == http.StatusNotFound {
			return state, nil
		}
		return state, err
	}
	var inspected struct {
		Image string `json:"Image"`
		State struct {
			Running bool `json:"Running"`
			Paused  bool `json:"Paused"`
		} `json:"State"`
		NetworkSettings struct {
			IPAddress string `json:"IPAddress"`
			Networks  map[string]struct {
				IPAddress string `json:"IPAddress"`
			} `json:"Networks"`
		} `json:"NetworkSettings"`
	}
	if err := json.Unmarshal(respBody, &inspected); err != nil {
		return state, fmt.Errorf("docker inspect container: %w", err)
	}
	state.Exists = true
	state.Running = inspected.State.Running
	state.Paused = inspected.State.Paused
	state.Image = inspected.Image
	state.IPAddress = inspected.NetworkSettings.IPAddress
	if state.IPAddress == "" {
		for _, network := range inspected.NetworkSettings.Networks {
			if network.IPAddress != "" {
				state.IPAddress = network.IPAddress
				break
			}
		}
	}
	return state, nil
}

// Exec runs a command inside the container and waits for completion, bounded
// by the caller's context.
func (c *neoOrbDockerClient) Exec(ctx context.Context, id string, cmd []string, env []string, workDir string) (neoOrbExecResult, error) {
	result := neoOrbExecResult{ExitCode: -1}
	type execBody struct {
		Cmd          []string `json:"Cmd"`
		Env          []string `json:"Env,omitempty"`
		WorkingDir   string   `json:"WorkingDir,omitempty"`
		AttachStdout bool     `json:"AttachStdout"`
		AttachStderr bool     `json:"AttachStderr"`
	}
	encoded, err := json.Marshal(execBody{Cmd: cmd, Env: env, WorkingDir: workDir, AttachStdout: true, AttachStderr: true})
	if err != nil {
		return result, err
	}
	respBody, _, err := c.request(ctx, http.MethodPost, "/containers/"+url.PathEscape(id)+"/exec", bytes.NewReader(encoded), "application/json")
	if err != nil {
		return result, err
	}
	var created struct {
		ID string `json:"Id"`
	}
	if err := json.Unmarshal(respBody, &created); err != nil || created.ID == "" {
		return result, fmt.Errorf("docker exec create: unexpected response %s", clipNeoErrorBody(respBody))
	}

	startBody := bytes.NewReader([]byte(`{"Detach":false,"Tty":false}`))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/exec/"+created.ID+"/start", startBody)
	if err != nil {
		return result, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return result, fmt.Errorf("docker exec start: %w", err)
	}
	defer func() {
		if errClose := resp.Body.Close(); errClose != nil {
			log.Warnf("amp orbs: docker exec stream close error: %v", errClose)
		}
	}()
	if resp.StatusCode >= 400 {
		payload, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		return result, fmt.Errorf("docker exec start: status %d: %s", resp.StatusCode, clipNeoErrorBody(payload))
	}
	stdout, stderr, err := demuxNeoOrbExecStream(resp.Body)
	if err != nil {
		return result, fmt.Errorf("docker exec stream: %w", err)
	}
	result.Stdout = stdout
	result.Stderr = stderr

	inspectBody, _, err := c.request(ctx, http.MethodGet, "/exec/"+created.ID+"/json", nil, "")
	if err != nil {
		return result, err
	}
	var execInspect struct {
		ExitCode *int `json:"ExitCode"`
	}
	if err := json.Unmarshal(inspectBody, &execInspect); err == nil && execInspect.ExitCode != nil {
		result.ExitCode = *execInspect.ExitCode
	}
	return result, nil
}

// demuxNeoOrbExecStream decodes the multiplexed stdout/stderr stream Docker
// produces for non-TTY exec sessions (8-byte frame headers). Each stream is
// retained up to 4 MiB; output beyond that is drained but truncated.
func demuxNeoOrbExecStream(stream io.Reader) (string, string, error) {
	const retainLimit = 4 << 20
	var stdout, stderr bytes.Buffer
	header := make([]byte, 8)
	for {
		_, err := io.ReadFull(stream, header)
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", "", err
		}
		size := binary.BigEndian.Uint32(header[4:])
		if size == 0 {
			continue
		}
		if size > 32<<20 {
			return "", "", fmt.Errorf("frame size %d exceeds limit", size)
		}
		frame := make([]byte, size)
		if _, err := io.ReadFull(stream, frame); err != nil {
			return "", "", err
		}
		var target *bytes.Buffer
		switch header[0] {
		case 1:
			target = &stdout
		case 2:
			target = &stderr
		default:
			continue
		}
		if remaining := retainLimit - target.Len(); remaining > 0 {
			if len(frame) > remaining {
				target.Write(frame[:remaining])
			} else {
				target.Write(frame)
			}
		}
	}
	return stdout.String(), stderr.String(), nil
}

// ExecDetached starts a long-running command inside the container without
// waiting for completion.
func (c *neoOrbDockerClient) ExecDetached(ctx context.Context, id string, cmd []string, env []string, workDir string) error {
	type execBody struct {
		Cmd        []string `json:"Cmd"`
		Env        []string `json:"Env,omitempty"`
		WorkingDir string   `json:"WorkingDir,omitempty"`
	}
	encoded, err := json.Marshal(execBody{Cmd: cmd, Env: env, WorkingDir: workDir})
	if err != nil {
		return err
	}
	respBody, _, err := c.request(ctx, http.MethodPost, "/containers/"+url.PathEscape(id)+"/exec", bytes.NewReader(encoded), "application/json")
	if err != nil {
		return err
	}
	var created struct {
		ID string `json:"Id"`
	}
	if err := json.Unmarshal(respBody, &created); err != nil || created.ID == "" {
		return fmt.Errorf("docker exec create: unexpected response %s", clipNeoErrorBody(respBody))
	}
	_, _, err = c.request(ctx, http.MethodPost, "/exec/"+created.ID+"/start", bytes.NewReader([]byte(`{"Detach":true,"Tty":false}`)), "application/json")
	return err
}

// CopyTarToContainer uploads multiple files into a container directory as a
// single tar archive. Names must be plain basenames.
func (c *neoOrbDockerClient) CopyTarToContainer(ctx context.Context, id, destDir string, files map[string][]byte, mode int64) error {
	if len(files) == 0 {
		return nil
	}
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if name == "" || name == "." || name == ".." || strings.ContainsAny(name, "/\\") {
			return fmt.Errorf("invalid tar entry name %q", name)
		}
		content := files[name]
		if err := writer.WriteHeader(&tar.Header{Name: name, Mode: mode, Size: int64(len(content))}); err != nil {
			return err
		}
		if _, err := writer.Write(content); err != nil {
			return err
		}
	}
	if err := writer.Close(); err != nil {
		return err
	}
	_, _, err := c.request(ctx, http.MethodPut, "/containers/"+url.PathEscape(id)+"/archive?path="+url.QueryEscape(destDir), &archive, "application/x-tar")
	return err
}

// CopyFileToContainer uploads a single file into the container as a tar entry.
func (c *neoOrbDockerClient) CopyFileToContainer(ctx context.Context, id, destPath string, content []byte, mode int64) error {
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	name := destPath
	if idx := strings.LastIndex(destPath, "/"); idx >= 0 {
		name = destPath[idx+1:]
	}
	if err := writer.WriteHeader(&tar.Header{Name: name, Mode: mode, Size: int64(len(content))}); err != nil {
		return err
	}
	if _, err := writer.Write(content); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}
	dir := destPath
	if idx := strings.LastIndex(destPath, "/"); idx > 0 {
		dir = destPath[:idx]
	} else {
		dir = "/"
	}
	_, _, err := c.request(ctx, http.MethodPut, "/containers/"+url.PathEscape(id)+"/archive?path="+url.QueryEscape(dir), &archive, "application/x-tar")
	return err
}
