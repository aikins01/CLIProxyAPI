package amp

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
)

// neoOrbDockerClient is a minimal Docker Engine API client covering the
// container lifecycle operations orb provisioning needs. It speaks to the
// engine over a unix socket or TCP endpoint without external dependencies.
type neoOrbDockerClient struct {
	endpoint   string
	baseURL    string
	httpClient *http.Client
}

type neoOrbContainerSpec struct {
	Name          string
	Image         string
	Env           []string
	WorkingDir    string
	Cmd           []string
	NanoCPUs      int64
	MemoryMB      int64
	ExtraHosts    []string
	Labels        map[string]string
	NetworkMode   string
	VolumeMounts  []neoOrbVolumeMount
	RestartPolicy string
}

type neoOrbVolumeMount struct {
	Name        string
	Destination string
	NoCopy      bool
}

type neoOrbContainerMount struct {
	Type        string
	Source      string
	Name        string
	Destination string
}

type neoOrbContainerState struct {
	ID            string
	Name          string
	Running       bool
	Paused        bool
	IPAddress     string
	Image         string
	Exists        bool
	Labels        map[string]string
	Mounts        []neoOrbContainerMount
	RestartPolicy string
}

type neoOrbContainerSummary struct {
	ID     string
	Names  []string
	Labels map[string]string
}

type neoOrbVolumeSpec struct {
	Name   string
	Labels map[string]string
}

type neoOrbVolumeState struct {
	Exists     bool              `json:"-"`
	Name       string            `json:"Name"`
	Driver     string            `json:"Driver"`
	Mountpoint string            `json:"Mountpoint"`
	CreatedAt  string            `json:"CreatedAt"`
	Labels     map[string]string `json:"Labels"`
	Scope      string            `json:"Scope"`
	Options    map[string]string `json:"Options"`
}

type neoOrbExecResult struct {
	ExitCode int
	Stdout   string
	Stderr   string
}

func newNeoOrbDockerClient(host string) (*neoOrbDockerClient, error) {
	endpoint, err := resolveNeoOrbDockerEndpoint(host)
	if err != nil {
		return nil, err
	}
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("docker host %q: %w", endpoint, err)
	}
	transport := &http.Transport{}
	base := ""
	switch parsed.Scheme {
	case "unix":
		socketPath := parsed.Path
		transport.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
			var dialer net.Dialer
			return dialer.DialContext(ctx, "unix", socketPath)
		}
		base = "http://docker.local"
	case "http", "https":
		scheme := parsed.Scheme
		base = scheme + "://" + parsed.Host
	default:
		return nil, fmt.Errorf("docker host %q uses unsupported scheme", endpoint)
	}
	return &neoOrbDockerClient{endpoint: endpoint, baseURL: base + "/v1.43", httpClient: &http.Client{Transport: transport}}, nil
}

func resolveNeoOrbDockerEndpoint(configuredHost string) (string, error) {
	host := strings.TrimSpace(configuredHost)
	if host == "" {
		host = strings.TrimSpace(os.Getenv("DOCKER_HOST"))
	}
	if host == "" {
		host = "unix:///var/run/docker.sock"
	}
	if len(host) > 1024 || strings.ContainsAny(host, "\x00\r\n") {
		return "", errors.New("docker host is invalid")
	}
	parsed, err := url.Parse(host)
	if err != nil || parsed.Scheme == "" || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" {
		return "", errors.New("docker host is invalid")
	}
	if parsed.User != nil {
		return "", errors.New("docker host contains unsupported userinfo")
	}
	parsed.Scheme = strings.ToLower(parsed.Scheme)
	if parsed.Scheme == "tcp" {
		parsed.Scheme = "http"
	}
	switch parsed.Scheme {
	case "unix":
		if parsed.Host != "" || !filepath.IsAbs(parsed.Path) {
			return "", errors.New("docker host is invalid")
		}
		parsed.Path = filepath.Clean(parsed.Path)
	case "http", "https":
		if parsed.Host == "" || parsed.Path != "" && parsed.Path != "/" {
			return "", errors.New("docker host is invalid")
		}
		hostname := parsed.Hostname()
		if zoneIndex := strings.LastIndex(hostname, "%"); zoneIndex >= 0 {
			hostname = strings.ToLower(hostname[:zoneIndex]) + hostname[zoneIndex:]
		} else {
			hostname = strings.ToLower(hostname)
		}
		if port := parsed.Port(); port != "" {
			parsed.Host = net.JoinHostPort(hostname, port)
		} else if strings.Contains(hostname, ":") {
			parsed.Host = "[" + hostname + "]"
		} else {
			parsed.Host = hostname
		}
		parsed.Path = ""
	default:
		return "", errors.New("docker host uses unsupported scheme")
	}
	return parsed.String(), nil
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
	type volumeOptions struct {
		NoCopy bool `json:"NoCopy"`
	}
	type mount struct {
		Type          string        `json:"Type"`
		Source        string        `json:"Source"`
		Target        string        `json:"Target"`
		VolumeOptions volumeOptions `json:"VolumeOptions"`
	}
	type restartPolicy struct {
		Name string `json:"Name"`
	}
	type hostConfig struct {
		NanoCPUs      int64         `json:"NanoCPUs,omitempty"`
		Memory        int64         `json:"Memory,omitempty"`
		ExtraHosts    []string      `json:"ExtraHosts,omitempty"`
		NetworkMode   string        `json:"NetworkMode,omitempty"`
		Mounts        []mount       `json:"Mounts,omitempty"`
		RestartPolicy restartPolicy `json:"RestartPolicy"`
	}
	type createBody struct {
		Image      string            `json:"Image"`
		Env        []string          `json:"Env,omitempty"`
		Cmd        []string          `json:"Cmd,omitempty"`
		WorkingDir string            `json:"WorkingDir,omitempty"`
		Labels     map[string]string `json:"Labels,omitempty"`
		HostConfig hostConfig        `json:"HostConfig"`
	}
	mounts := make([]mount, 0, len(spec.VolumeMounts))
	for _, volumeMount := range spec.VolumeMounts {
		if !neoOrbDockerResourceNameValid(volumeMount.Name) {
			return "", fmt.Errorf("docker create container: invalid volume name")
		}
		if !neoOrbDockerAbsolutePathValid(volumeMount.Destination) {
			return "", fmt.Errorf("docker create container: invalid volume destination")
		}
		mounts = append(mounts, mount{
			Type:          "volume",
			Source:        volumeMount.Name,
			Target:        volumeMount.Destination,
			VolumeOptions: volumeOptions{NoCopy: volumeMount.NoCopy},
		})
	}
	payload := createBody{
		Image:      spec.Image,
		Env:        spec.Env,
		Cmd:        spec.Cmd,
		WorkingDir: spec.WorkingDir,
		Labels:     spec.Labels,
		HostConfig: hostConfig{
			NanoCPUs:      spec.NanoCPUs,
			Memory:        spec.MemoryMB * 1024 * 1024,
			ExtraHosts:    spec.ExtraHosts,
			NetworkMode:   spec.NetworkMode,
			Mounts:        mounts,
			RestartPolicy: restartPolicy{Name: spec.RestartPolicy},
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
		Names  []string          `json:"Names"`
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
		containers = append(containers, neoOrbContainerSummary{ID: container.ID, Names: append([]string(nil), container.Names...), Labels: container.Labels})
	}
	return containers, nil
}

func (c *neoOrbDockerClient) StartContainer(ctx context.Context, id string) error {
	_, _, err := c.request(ctx, http.MethodPost, "/containers/"+url.PathEscape(id)+"/start", nil, "")
	return err
}

func (c *neoOrbDockerClient) StopContainer(ctx context.Context, id string) error {
	if !neoOrbDockerResourceNameValid(id) {
		return fmt.Errorf("docker stop container: invalid container identifier")
	}
	_, _, err := c.request(ctx, http.MethodPost, "/containers/"+url.PathEscape(id)+"/stop", nil, "")
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
		ID     string `json:"Id"`
		Name   string `json:"Name"`
		Image  string `json:"Image"`
		Config struct {
			Labels map[string]string `json:"Labels"`
		} `json:"Config"`
		HostConfig struct {
			RestartPolicy struct {
				Name string `json:"Name"`
			} `json:"RestartPolicy"`
		} `json:"HostConfig"`
		Mounts []struct {
			Type        string `json:"Type"`
			Source      string `json:"Source"`
			Name        string `json:"Name"`
			Destination string `json:"Destination"`
		} `json:"Mounts"`
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
	state.ID = inspected.ID
	state.Name = inspected.Name
	state.Running = inspected.State.Running
	state.Paused = inspected.State.Paused
	state.Image = inspected.Image
	state.Labels = inspected.Config.Labels
	state.RestartPolicy = inspected.HostConfig.RestartPolicy.Name
	state.Mounts = make([]neoOrbContainerMount, 0, len(inspected.Mounts))
	for _, inspectedMount := range inspected.Mounts {
		state.Mounts = append(state.Mounts, neoOrbContainerMount{
			Type:        inspectedMount.Type,
			Source:      inspectedMount.Source,
			Name:        inspectedMount.Name,
			Destination: inspectedMount.Destination,
		})
	}
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

func (c *neoOrbDockerClient) CreateVolume(ctx context.Context, spec neoOrbVolumeSpec) (neoOrbVolumeState, error) {
	state := neoOrbVolumeState{}
	if !neoOrbDockerResourceNameValid(spec.Name) {
		return state, fmt.Errorf("docker create volume: invalid volume name")
	}
	encoded, err := json.Marshal(struct {
		Name   string            `json:"Name"`
		Labels map[string]string `json:"Labels,omitempty"`
	}{Name: spec.Name, Labels: spec.Labels})
	if err != nil {
		return state, err
	}
	respBody, _, err := c.request(ctx, http.MethodPost, "/volumes/create", bytes.NewReader(encoded), "application/json")
	if err != nil {
		return state, err
	}
	state, err = decodeNeoOrbVolume(respBody)
	if err != nil {
		return neoOrbVolumeState{}, fmt.Errorf("docker create volume: %w", err)
	}
	if state.Name != spec.Name {
		return neoOrbVolumeState{}, fmt.Errorf("docker create volume: unexpected response")
	}
	state.Exists = true
	return state, nil
}

func (c *neoOrbDockerClient) InspectVolume(ctx context.Context, name string) (neoOrbVolumeState, error) {
	state := neoOrbVolumeState{}
	if !neoOrbDockerResourceNameValid(name) {
		return state, fmt.Errorf("docker inspect volume: invalid volume name")
	}
	respBody, status, err := c.request(ctx, http.MethodGet, "/volumes/"+url.PathEscape(name), nil, "")
	if err != nil {
		if status == http.StatusNotFound {
			return state, nil
		}
		return state, err
	}
	state, err = decodeNeoOrbVolume(respBody)
	if err != nil {
		return neoOrbVolumeState{}, fmt.Errorf("docker inspect volume: %w", err)
	}
	state.Exists = true
	return state, nil
}

func (c *neoOrbDockerClient) ListOrbVolumes(ctx context.Context) ([]neoOrbVolumeState, error) {
	filters, err := json.Marshal(map[string][]string{"label": {"cliproxy.orb.lifecycle=1"}})
	if err != nil {
		return nil, err
	}
	respBody, _, err := c.request(ctx, http.MethodGet, "/volumes?filters="+url.QueryEscape(string(filters)), nil, "")
	if err != nil {
		return nil, err
	}
	var listed struct {
		Volumes []json.RawMessage `json:"Volumes"`
	}
	if err := json.Unmarshal(respBody, &listed); err != nil {
		return nil, fmt.Errorf("docker list orb volumes: %w", err)
	}
	volumes := make([]neoOrbVolumeState, 0, len(listed.Volumes))
	for _, raw := range listed.Volumes {
		volume, err := decodeNeoOrbVolume(raw)
		if err != nil {
			return nil, fmt.Errorf("docker list orb volumes: %w", err)
		}
		if strings.TrimSpace(volume.Name) == "" {
			continue
		}
		volume.Exists = true
		volumes = append(volumes, volume)
	}
	return volumes, nil
}

func (c *neoOrbDockerClient) RemoveVolume(ctx context.Context, name string) error {
	if !neoOrbDockerResourceNameValid(name) {
		return fmt.Errorf("docker remove volume: invalid volume name")
	}
	_, status, err := c.request(ctx, http.MethodDelete, "/volumes/"+url.PathEscape(name), nil, "")
	if status == http.StatusNotFound {
		return nil
	}
	return err
}

func decodeNeoOrbVolume(payload []byte) (neoOrbVolumeState, error) {
	var state neoOrbVolumeState
	if err := json.Unmarshal(payload, &state); err != nil {
		return neoOrbVolumeState{}, err
	}
	if strings.TrimSpace(state.Name) == "" {
		return neoOrbVolumeState{}, fmt.Errorf("volume name is empty")
	}
	return state, nil
}

// Exec runs a command inside the container and waits for completion, bounded
// by the caller's context.
func (c *neoOrbDockerClient) Exec(ctx context.Context, id string, cmd []string, env []string, workDir string) (neoOrbExecResult, error) {
	return c.execWithRetainLimits(ctx, id, cmd, env, workDir, 4<<20, 4<<20)
}

func (c *neoOrbDockerClient) execWithRetainLimits(ctx context.Context, id string, cmd []string, env []string, workDir string, stdoutLimit, stderrLimit int) (neoOrbExecResult, error) {
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
	stdout, stderr, err := demuxNeoOrbExecStreamLimits(resp.Body, stdoutLimit, stderrLimit)
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
	return demuxNeoOrbExecStreamLimits(stream, 4<<20, 4<<20)
}

func demuxNeoOrbExecStreamLimits(stream io.Reader, stdoutLimit, stderrLimit int) (string, string, error) {
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
		retainLimit := stdoutLimit
		if target == &stderr {
			retainLimit = stderrLimit
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

const neoOrbReadWorkspaceFileProgram = `import os, stat, sys
root=sys.argv[1]
p=sys.argv[2].split('/')
n=int(sys.argv[3])
fd=os.open(root, os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
try:
 for part in p[:-1]:
  nxt=os.open(part, os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW, dir_fd=fd)
  os.close(fd); fd=nxt
 out=os.open(p[-1], os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK, dir_fd=fd)
 try:
  info=os.fstat(out)
  if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1: sys.exit(3)
  chunks=[]; total=0
  while total<=n:
   chunk=os.read(out,min(65536,n+1-total))
   if not chunk: break
   chunks.append(chunk); total+=len(chunk)
  data=b''.join(chunks)
  if len(data)>n: sys.exit(4)
  sys.stdout.buffer.write(data)
 finally: os.close(out)
finally: os.close(fd)`

func (c *neoOrbDockerClient) ReadWorkspaceFile(ctx context.Context, id, workDir, relative string, maxBytes int) ([]byte, error) {
	if _, err := validateNeoPublishImagePath(relative); err != nil || workDir != neoOrbWorkDir || maxBytes <= 0 || maxBytes > neoAttachmentMaxImageBytes {
		return nil, errors.New("orb workspace file read failed")
	}
	workspaceRoot := strings.TrimPrefix(workDir, neoOrbWorkspaceDir+"/")
	result, err := c.execWithRetainLimits(ctx, id, []string{"python3", "-c", neoOrbReadWorkspaceFileProgram, workspaceRoot, relative, strconv.Itoa(maxBytes)}, nil, neoOrbWorkspaceDir, maxBytes+1, 64<<10)
	if err != nil {
		return nil, err
	}
	if result.ExitCode != 0 || len(result.Stdout) > maxBytes {
		return nil, errors.New("orb workspace file read failed")
	}
	return []byte(result.Stdout), nil
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
	return c.CopyArchiveToContainer(ctx, id, destDir, &archive)
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
	return c.CopyArchiveToContainer(ctx, id, dir, &archive)
}

func (c *neoOrbDockerClient) ArchiveFromContainer(ctx context.Context, id, sourcePath string) (io.ReadCloser, error) {
	if !neoOrbDockerResourceNameValid(id) {
		return nil, fmt.Errorf("docker read container archive: invalid container identifier")
	}
	if !neoOrbDockerAbsolutePathValid(sourcePath) {
		return nil, fmt.Errorf("docker read container archive: invalid source path")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/containers/"+url.PathEscape(id)+"/archive?path="+url.QueryEscape(sourcePath), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("docker read container archive: %w", err)
	}
	if resp.StatusCode >= 400 {
		if errClose := resp.Body.Close(); errClose != nil {
			log.Warnf("amp orbs: docker archive response close error: %v", errClose)
		}
		return nil, fmt.Errorf("docker read container archive: status %d", resp.StatusCode)
	}
	return resp.Body, nil
}

func (c *neoOrbDockerClient) CopyArchiveToContainer(ctx context.Context, id, destinationPath string, archive io.Reader) error {
	if !neoOrbDockerResourceNameValid(id) {
		return fmt.Errorf("docker copy container archive: invalid container identifier")
	}
	if !neoOrbDockerAbsolutePathValid(destinationPath) {
		return fmt.Errorf("docker copy container archive: invalid destination path")
	}
	if archive == nil {
		return fmt.Errorf("docker copy container archive: archive is unavailable")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.baseURL+"/containers/"+url.PathEscape(id)+"/archive?path="+url.QueryEscape(destinationPath), archive)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-tar")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("docker copy container archive: %w", err)
	}
	defer func() {
		if errClose := resp.Body.Close(); errClose != nil {
			log.Warnf("amp orbs: docker archive response close error: %v", errClose)
		}
	}()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("docker copy container archive: status %d", resp.StatusCode)
	}
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		return fmt.Errorf("docker copy container archive: read response: %w", err)
	}
	return nil
}

func neoOrbDockerResourceNameValid(name string) bool {
	if name == "" || len(name) > 255 || name[0] == '.' || name[0] == '-' {
		return false
	}
	for _, char := range name {
		if char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '_' || char == '.' || char == '-' {
			continue
		}
		return false
	}
	return true
}

func neoOrbDockerAbsolutePathValid(path string) bool {
	if path == "" || path[0] != '/' || strings.ContainsRune(path, 0) {
		return false
	}
	for _, part := range strings.Split(path, "/") {
		if part == ".." {
			return false
		}
	}
	return true
}
