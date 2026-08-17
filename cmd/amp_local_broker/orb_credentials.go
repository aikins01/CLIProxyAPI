//go:build darwin

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/orbcredentials"
)

const (
	orbCredentialEndpoint        = orbcredentials.EndpointPath
	orbCredentialSupportHeader   = orbcredentials.SupportHeader
	orbCredentialRevisionHeader  = orbcredentials.RevisionHeader
	orbCredentialCollectLimit    = 15 * time.Second
	orbCredentialAbsentRevision  = orbcredentials.AbsentRevision
	orbCredentialRevokedRevision = orbcredentials.RevokedRevision
	orbCredentialRepairRevision  = orbcredentials.RepairRevision
)

var (
	errOrbCredentialCapabilityUnavailable = errors.New("orb credential capability is unavailable")
	errOrbCredentialRevisionConflict      = errors.New("orb credential revision conflict")
	errOrbGitHubCredentialCollection      = errors.New("GitHub credential collection failed")
	errOrbSSHCredentialCollection         = errors.New("SSH credential collection failed")
	orbCredentialFileOpen                 = os.Open
)
var orbCredentialRevisionPattern = regexp.MustCompile(`^[0-9a-f]{32}$`)

type orbCredentialSyncConfig struct {
	GitHub            bool `json:"github"`
	AllowBroadSSHSync bool `json:"allowBroadSSHSync"`
}

type orbCredentialState struct {
	support        bool
	snapshot       *orbcredentials.Snapshot
	revision       string
	serverRevision string
}

type orbCredentialUploadRequest struct {
	BrokerID          string                  `json:"brokerId"`
	SessionID         string                  `json:"sessionId"`
	SessionGeneration uint64                  `json:"sessionGeneration"`
	Credentials       orbcredentials.Snapshot `json:"credentials"`
}

type orbCredentialClearRequest struct {
	BrokerID          string `json:"brokerId"`
	SessionID         string `json:"sessionId"`
	SessionGeneration uint64 `json:"sessionGeneration"`
	ExpectedRevision  string `json:"expectedRevision"`
}

type orbCredentialUploadResponse struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
	Code  string `json:"code,omitempty"`
}

type orbCredentialOutput struct {
	data     []byte
	overflow bool
}

func (output *orbCredentialOutput) Write(data []byte) (int, error) {
	remaining := orbcredentials.MaxGitHubTokenBytes + 1 - len(output.data)
	if remaining > len(data) {
		remaining = len(data)
	}
	if remaining > 0 {
		output.data = append(output.data, data[:remaining]...)
	}
	if remaining < len(data) {
		output.overflow = true
	}
	return len(data), nil
}

func (localBroker *broker) syncOrbCredentials(ctx context.Context) error {
	if localBroker == nil || localBroker.config == nil || !localBroker.orbCredentials.support {
		return nil
	}
	syncConfig := localBroker.config.OrbCredentials
	if syncConfig == nil || !syncConfig.GitHub && !syncConfig.AllowBroadSSHSync {
		if localBroker.orbCredentials.serverRevision == orbCredentialRevokedRevision {
			return nil
		}
		revision, err := localBroker.clearOrbCredentials(ctx, localBroker.orbCredentials.serverRevision)
		if err != nil {
			if errors.Is(err, errOrbCredentialCapabilityUnavailable) {
				localBroker.orbCredentials.support = false
				return nil
			}
			return err
		}
		localBroker.orbCredentials.snapshot = nil
		localBroker.orbCredentials.revision = revision
		localBroker.orbCredentials.serverRevision = revision
		return nil
	}
	home, err := os.UserHomeDir()
	if err != nil || strings.TrimSpace(home) == "" {
		return errors.New("orb credential collection failed")
	}
	snapshot, err := collectOrbCredentials(ctx, home, *syncConfig)
	if err != nil {
		if errors.Is(err, errOrbGitHubCredentialCollection) {
			return errOrbGitHubCredentialCollection
		}
		if errors.Is(err, errOrbSSHCredentialCollection) {
			return errOrbSSHCredentialCollection
		}
		return errors.New("orb credential collection failed")
	}
	if snapshot.GitHub == nil && snapshot.SSH == nil {
		if localBroker.orbCredentials.serverRevision == orbCredentialAbsentRevision || localBroker.orbCredentials.serverRevision == orbCredentialRevokedRevision {
			localBroker.orbCredentials.snapshot = nil
			localBroker.orbCredentials.revision = localBroker.orbCredentials.serverRevision
			return nil
		}
		revision, clearErr := localBroker.clearOrbCredentials(ctx, localBroker.orbCredentials.serverRevision)
		if clearErr != nil {
			if errors.Is(clearErr, errOrbCredentialCapabilityUnavailable) {
				localBroker.orbCredentials.support = false
				return nil
			}
			return clearErr
		}
		localBroker.orbCredentials.snapshot = nil
		localBroker.orbCredentials.revision = revision
		localBroker.orbCredentials.serverRevision = revision
		return nil
	}
	if localBroker.orbCredentials.snapshot != nil && reflect.DeepEqual(*localBroker.orbCredentials.snapshot, snapshot) && localBroker.orbCredentials.revision == localBroker.orbCredentials.serverRevision {
		return nil
	}
	revision, err := localBroker.putOrbCredentials(ctx, snapshot)
	if err != nil {
		if errors.Is(err, errOrbCredentialCapabilityUnavailable) {
			localBroker.orbCredentials.support = false
			return nil
		}
		return err
	}
	localBroker.orbCredentials.snapshot = &snapshot
	localBroker.orbCredentials.revision = revision
	localBroker.orbCredentials.serverRevision = revision
	return nil
}

func collectOrbCredentials(ctx context.Context, home string, cfg orbCredentialSyncConfig) (orbcredentials.Snapshot, error) {
	var githubToken string
	if cfg.GitHub {
		collectionContext, cancel := context.WithTimeout(ctx, orbCredentialCollectLimit)
		defer cancel()
		command := exec.CommandContext(collectionContext, "gh", "auth", "token", "--hostname", "github.com")
		output := &orbCredentialOutput{}
		command.Stdout = output
		command.Stderr = io.Discard
		if err := command.Run(); err != nil || output.overflow {
			return orbcredentials.Snapshot{}, errOrbGitHubCredentialCollection
		}
		githubToken = strings.TrimSpace(string(output.data))
		if githubToken == "" {
			return orbcredentials.Snapshot{}, errOrbGitHubCredentialCollection
		}
		if _, err := orbcredentials.New(githubToken, nil, nil); err != nil {
			return orbcredentials.Snapshot{}, errOrbGitHubCredentialCollection
		}
	}
	var identities []orbcredentials.DecodedSSHIdentity
	var knownHosts []byte
	if cfg.AllowBroadSSHSync {
		var err error
		identities, knownHosts, err = collectOrbSSHCredentials(filepath.Join(home, ".ssh"))
		if err != nil {
			return orbcredentials.Snapshot{}, errOrbSSHCredentialCollection
		}
		if len(identities) == 0 {
			identities = nil
			knownHosts = nil
		} else if len(knownHosts) == 0 {
			knownHosts = nil
		}
	}
	if githubToken == "" && len(identities) == 0 && len(knownHosts) == 0 {
		return orbcredentials.Snapshot{}, nil
	}
	snapshot, err := orbcredentials.New(githubToken, identities, knownHosts)
	if err != nil {
		if len(identities) > 0 || len(knownHosts) > 0 {
			return orbcredentials.Snapshot{}, errOrbSSHCredentialCollection
		}
		return orbcredentials.Snapshot{}, errOrbGitHubCredentialCollection
	}
	return snapshot, nil
}

func collectOrbSSHCredentials(root string) ([]orbcredentials.DecodedSSHIdentity, []byte, error) {
	entries, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, errors.New("SSH credential directory is unavailable")
	}
	names := make([]string, 0)
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, "id_") && !strings.HasSuffix(strings.ToLower(name), ".pub") {
			names = append(names, name)
		}
	}
	sort.Slice(names, func(i, j int) bool { return strings.ToLower(names[i]) < strings.ToLower(names[j]) })
	identities := make([]orbcredentials.DecodedSSHIdentity, 0, len(names))
	for _, name := range names {
		privateKey, found, err := readOrbCredentialRegularFile(filepath.Join(root, name), orbcredentials.MaxPrivateKeyBytes, true)
		if err != nil {
			return nil, nil, errors.New("SSH credential identity is unavailable")
		}
		if !found {
			continue
		}
		publicKey, publicFound, err := readOrbCredentialRegularFile(filepath.Join(root, name+".pub"), orbcredentials.MaxPublicKeyBytes, false)
		if err != nil {
			return nil, nil, errors.New("SSH public identity is unavailable")
		}
		if !publicFound {
			publicKey = nil
		}
		identity := orbcredentials.DecodedSSHIdentity{Name: name, PrivateKey: privateKey, PublicKey: publicKey}
		if _, err := orbcredentials.New("", []orbcredentials.DecodedSSHIdentity{identity}, nil); err != nil {
			continue
		}
		identities = append(identities, identity)
		if len(identities) > orbcredentials.MaxIdentities {
			return nil, nil, errors.New("SSH credential identity limit exceeded")
		}
	}
	if len(identities) == 0 {
		return nil, nil, nil
	}
	knownHosts, found, err := readOrbCredentialRegularFile(filepath.Join(root, "known_hosts"), orbcredentials.MaxKnownHostsBytes, false)
	if err != nil {
		return nil, nil, errors.New("SSH known_hosts is unavailable")
	}
	if !found {
		knownHosts = nil
	}
	if _, err := orbcredentials.New("", identities, knownHosts); err != nil && (len(identities) > 0 || len(knownHosts) > 0) {
		return nil, nil, errors.New("SSH credential material is invalid")
	}
	return identities, knownHosts, nil
}

func readOrbCredentialRegularFile(path string, limit int, private bool) ([]byte, bool, error) {
	before, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil || !before.Mode().IsRegular() || before.Size() <= 0 || before.Size() > int64(limit) {
		return nil, false, errors.New("credential file is invalid")
	}
	stat, ok := before.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Uid) != os.Getuid() || private && before.Mode().Perm()&0o077 != 0 {
		return nil, false, errors.New("credential file is invalid")
	}
	file, err := orbCredentialFileOpen(path)
	if err != nil {
		return nil, false, errors.New("credential file is unavailable")
	}
	content, readErr := io.ReadAll(io.LimitReader(file, int64(limit)+1))
	after, statErr := file.Stat()
	closeErr := file.Close()
	if readErr != nil || statErr != nil || closeErr != nil || !after.Mode().IsRegular() || !os.SameFile(before, after) || len(content) > limit {
		return nil, false, errors.New("credential file changed while reading")
	}
	afterStat, ok := after.Sys().(*syscall.Stat_t)
	if !ok || int(afterStat.Uid) != os.Getuid() || private && after.Mode().Perm()&0o077 != 0 {
		return nil, false, errors.New("credential file changed while reading")
	}
	return content, true, nil
}

func (localBroker *broker) putOrbCredentials(ctx context.Context, snapshot orbcredentials.Snapshot) (string, error) {
	normalized, _, err := orbcredentials.Validate(snapshot)
	if err != nil {
		return "", errors.New("orb credential upload failed")
	}
	body, err := json.Marshal(orbCredentialUploadRequest{
		BrokerID:          localBroker.config.BrokerID,
		SessionID:         localBroker.sessionID,
		SessionGeneration: localBroker.sessionGeneration,
		Credentials:       normalized,
	})
	if err != nil || len(body) > orbcredentials.MaxBodyBytes {
		return "", errors.New("orb credential upload failed")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, localBroker.config.APIURL+orbCredentialEndpoint, bytes.NewReader(body))
	if err != nil {
		return "", errors.New("orb credential upload failed")
	}
	request.Header.Set("Authorization", "Bearer "+localBroker.config.apiKey)
	request.Header.Set("Content-Type", "application/json")
	response, err := localBroker.client.Do(request)
	if err != nil {
		return "", errors.New("orb credential upload failed")
	}
	revision := response.Header.Get(orbCredentialRevisionHeader)
	responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, maxHeartbeatBody+1))
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil || len(responseBody) > maxHeartbeatBody {
		return "", errors.New("orb credential upload failed")
	}
	if response.StatusCode == http.StatusNotFound {
		return "", errOrbCredentialCapabilityUnavailable
	}
	var decoded orbCredentialUploadResponse
	if rejectDuplicateJSONFields(responseBody) == nil {
		decoder := json.NewDecoder(bytes.NewReader(responseBody))
		decoder.DisallowUnknownFields()
		decodeErr := decoder.Decode(&decoded)
		var trailing any
		trailingErr := decoder.Decode(&trailing)
		if decodeErr != nil || !errors.Is(trailingErr, io.EOF) {
			decoded = orbCredentialUploadResponse{}
		}
	}
	if response.StatusCode == http.StatusConflict && (decoded.Error == "stale_session" || decoded.Code == "stale_session") {
		return "", errStaleSession
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 || !decoded.OK || !orbCredentialRevisionPattern.MatchString(revision) {
		return "", errors.New("orb credential upload failed")
	}
	return revision, nil
}

func (localBroker *broker) clearOrbCredentials(ctx context.Context, expectedRevision string) (string, error) {
	body, err := json.Marshal(orbCredentialClearRequest{
		BrokerID:          localBroker.config.BrokerID,
		SessionID:         localBroker.sessionID,
		SessionGeneration: localBroker.sessionGeneration,
		ExpectedRevision:  expectedRevision,
	})
	if err != nil || len(body) > orbcredentials.MaxBodyBytes {
		return "", errors.New("orb credential revocation failed")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodDelete, localBroker.config.APIURL+orbCredentialEndpoint, bytes.NewReader(body))
	if err != nil {
		return "", errors.New("orb credential revocation failed")
	}
	request.Header.Set("Authorization", "Bearer "+localBroker.config.apiKey)
	request.Header.Set("Content-Type", "application/json")
	response, err := localBroker.client.Do(request)
	if err != nil {
		return "", errors.New("orb credential revocation failed")
	}
	revision := response.Header.Get(orbCredentialRevisionHeader)
	responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, maxHeartbeatBody+1))
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil || len(responseBody) > maxHeartbeatBody {
		return "", errors.New("orb credential revocation failed")
	}
	if response.StatusCode == http.StatusNotFound {
		return "", errOrbCredentialCapabilityUnavailable
	}
	var decoded orbCredentialUploadResponse
	if rejectDuplicateJSONFields(responseBody) == nil {
		decoder := json.NewDecoder(bytes.NewReader(responseBody))
		decoder.DisallowUnknownFields()
		decodeErr := decoder.Decode(&decoded)
		var trailing any
		trailingErr := decoder.Decode(&trailing)
		if decodeErr != nil || !errors.Is(trailingErr, io.EOF) {
			decoded = orbCredentialUploadResponse{}
		}
	}
	if response.StatusCode == http.StatusConflict {
		if decoded.Error == "stale_session" || decoded.Code == "stale_session" {
			return "", errStaleSession
		}
		if decoded.Error == "revision_conflict" || decoded.Code == "revision_conflict" {
			return "", errOrbCredentialRevisionConflict
		}
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 || !decoded.OK || revision != orbCredentialRevokedRevision {
		return "", errors.New("orb credential revocation failed")
	}
	return revision, nil
}
