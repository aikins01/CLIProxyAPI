//go:build darwin

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/orbconfig"
	"gopkg.in/yaml.v3"
)

const (
	orbConfigBundleEndpoint = orbconfig.EndpointPath
	orbConfigSupportHeader  = orbconfig.SupportHeader
	orbConfigDigestHeader   = orbconfig.DigestHeader
)

type orbConfigUploadRequest struct {
	BrokerID          string           `json:"brokerId"`
	SessionID         string           `json:"sessionId"`
	SessionGeneration uint64           `json:"sessionGeneration"`
	Bundle            orbconfig.Bundle `json:"bundle"`
}

type orbConfigUploadResponse struct {
	OK      bool            `json:"ok"`
	Digest  string          `json:"digest"`
	Error   json.RawMessage `json:"error,omitempty"`
	Message string          `json:"message,omitempty"`
}

type ghExtensionManifest struct {
	Owner    string `yaml:"owner"`
	Name     string `yaml:"name"`
	Host     string `yaml:"host"`
	Tag      string `yaml:"tag"`
	IsPinned *bool  `yaml:"ispinned"`
}

func collectOrbConfigBundle(home string) (orbconfig.Bundle, error) {
	home = strings.TrimSpace(home)
	if home == "" || !filepath.IsAbs(home) {
		return orbconfig.Bundle{}, errors.New("home directory is unavailable")
	}
	files := make([]orbconfig.DecodedFile, 0)
	totalBytes := 0
	checksRoot := filepath.Join(home, ".config", "agents", "checks")
	if err := collectOrbConfigTree(checksRoot, "checks", home, &files, &totalBytes); err != nil {
		return orbconfig.Bundle{}, err
	}
	if err := collectOrbConfigSkills(filepath.Join(home, ".config", "agents", "skills"), home, &files, &totalBytes); err != nil {
		return orbconfig.Bundle{}, err
	}
	extensions, err := collectGHGitHubExtensions(filepath.Join(home, ".local", "share", "gh", "extensions"))
	if err != nil {
		return orbconfig.Bundle{}, err
	}
	return orbconfig.New(files, extensions)
}

func collectOrbConfigSkills(root, home string, files *[]orbconfig.DecodedFile, totalBytes *int) error {
	entries, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read skills directory: %w", err)
	}
	for _, entry := range entries {
		name := entry.Name()
		skillPath := filepath.Join(root, name)
		info, err := os.Lstat(skillPath)
		if err != nil {
			return fmt.Errorf("inspect skill %q: %w", name, err)
		}
		if !info.IsDir() || strings.EqualFold(name, "using-open-browser-use") || orbconfig.SecretLikeName(name) {
			continue
		}
		skillFiles, portable, err := collectPortableOrbConfigSkill(skillPath, "skills/"+name, home)
		if err != nil {
			return err
		}
		if !portable {
			continue
		}
		if len(skillFiles) > orbconfig.MaxFiles-len(*files) {
			return fmt.Errorf("orb configuration exceeds %d files", orbconfig.MaxFiles)
		}
		skillBytes := 0
		for _, file := range skillFiles {
			if len(file.Content) > orbconfig.MaxDecodedBytes-*totalBytes-skillBytes {
				return fmt.Errorf("orb configuration exceeds %d decoded bytes", orbconfig.MaxDecodedBytes)
			}
			skillBytes += len(file.Content)
		}
		*files = append(*files, skillFiles...)
		*totalBytes += skillBytes
	}
	return nil
}

func collectPortableOrbConfigSkill(root, prefix, home string) ([]orbconfig.DecodedFile, bool, error) {
	files := make([]orbconfig.DecodedFile, 0)
	var skillContent []byte
	skillFound := false
	portable := true
	err := filepath.WalkDir(root, func(filePath string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if filePath != root && strings.EqualFold(entry.Name(), "mcp.json") {
			portable = false
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if filePath == root || entry.IsDir() {
			return nil
		}
		content, ok, err := readOrbConfigRegularFile(filePath)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		if !orbConfigContentPortable(content, home) {
			portable = false
			return nil
		}
		if filePath == filepath.Join(root, "SKILL.md") {
			skillContent = content
			skillFound = true
		}
		relative, err := filepath.Rel(root, filePath)
		if err != nil {
			return err
		}
		bundlePath := prefix + "/" + filepath.ToSlash(relative)
		if _, err := orbconfig.ValidatePath(bundlePath); err != nil {
			return nil
		}
		files = append(files, orbconfig.DecodedFile{Path: bundlePath, Content: content})
		return nil
	})
	if err != nil {
		return nil, false, fmt.Errorf("inspect skill %q: %w", filepath.Base(root), err)
	}
	if !skillFound || orbconfig.ValidateSkill(skillContent) != nil {
		portable = false
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	return files, portable, nil
}

func collectOrbConfigTree(root, prefix, home string, files *[]orbconfig.DecodedFile, totalBytes *int) error {
	rootInfo, err := os.Lstat(root)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect %s directory: %w", prefix, err)
	}
	if !rootInfo.IsDir() {
		return nil
	}
	return filepath.WalkDir(root, func(filePath string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if filePath == root {
			return nil
		}
		relative, err := filepath.Rel(root, filePath)
		if err != nil {
			return err
		}
		bundlePath := prefix + "/" + filepath.ToSlash(relative)
		if _, err := orbconfig.ValidatePath(bundlePath); err != nil {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		info, err := os.Lstat(filePath)
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		content, ok, err := readOrbConfigRegularFile(filePath)
		if err != nil {
			return err
		}
		if !ok || !orbConfigContentPortable(content, home) {
			return nil
		}
		if len(*files) >= orbconfig.MaxFiles {
			return fmt.Errorf("orb configuration exceeds %d files", orbconfig.MaxFiles)
		}
		if len(content) > orbconfig.MaxDecodedBytes-*totalBytes {
			return fmt.Errorf("orb configuration exceeds %d decoded bytes", orbconfig.MaxDecodedBytes)
		}
		*files = append(*files, orbconfig.DecodedFile{Path: bundlePath, Content: content})
		*totalBytes += len(content)
		return nil
	})
}

func orbConfigContentPortable(content []byte, home string) bool {
	if orbconfig.ContainsSecret(content) || orbconfig.ContainsMacAbsolutePath(content) {
		return false
	}
	home = filepath.Clean(strings.TrimSpace(home))
	return !filepath.IsAbs(home) || !orbconfig.ContainsAbsolutePathPrefix(content, home+string(os.PathSeparator))
}

func readOrbConfigRegularFile(path string) ([]byte, bool, error) {
	before, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if !before.Mode().IsRegular() || before.Size() > orbconfig.MaxFileBytes {
		return nil, false, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, false, err
	}
	content, readErr := io.ReadAll(io.LimitReader(file, orbconfig.MaxFileBytes+1))
	after, statErr := file.Stat()
	closeErr := file.Close()
	if readErr != nil || statErr != nil || closeErr != nil {
		return nil, false, errors.Join(readErr, statErr, closeErr)
	}
	if !after.Mode().IsRegular() || !os.SameFile(before, after) || len(content) > orbconfig.MaxFileBytes {
		return nil, false, nil
	}
	return content, true, nil
}

func collectGHGitHubExtensions(root string) ([]orbconfig.Extension, error) {
	entries, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return []orbconfig.Extension{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read GitHub extensions directory: %w", err)
	}
	extensions := make([]orbconfig.Extension, 0)
	for _, entry := range entries {
		extensionPath := filepath.Join(root, entry.Name())
		info, err := os.Lstat(extensionPath)
		if err != nil {
			return nil, fmt.Errorf("inspect GitHub extension %q: %w", entry.Name(), err)
		}
		if !info.IsDir() {
			continue
		}
		manifestPath := filepath.Join(extensionPath, "manifest.yml")
		manifestInfo, err := os.Lstat(manifestPath)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, fmt.Errorf("inspect GitHub extension manifest: %w", err)
		}
		if !manifestInfo.Mode().IsRegular() || manifestInfo.Size() > orbconfig.MaxManifestBytes {
			continue
		}
		raw, ok, err := readOrbConfigRegularFile(manifestPath)
		if err != nil {
			return nil, fmt.Errorf("read GitHub extension manifest: %w", err)
		}
		if !ok || len(raw) > orbconfig.MaxManifestBytes {
			continue
		}
		var manifest ghExtensionManifest
		if yaml.Unmarshal(raw, &manifest) != nil || !strings.EqualFold(strings.TrimSpace(manifest.Host), "github.com") || manifest.IsPinned == nil || !*manifest.IsPinned {
			continue
		}
		extension := orbconfig.Extension{Repo: strings.TrimSpace(manifest.Owner) + "/" + strings.TrimSpace(manifest.Name), Version: strings.TrimSpace(manifest.Tag)}
		if orbconfig.ValidateExtension(extension) != nil {
			continue
		}
		if len(extensions) >= orbconfig.MaxExtensions {
			return nil, fmt.Errorf("GitHub extension manifest exceeds %d entries", orbconfig.MaxExtensions)
		}
		extensions = append(extensions, extension)
	}
	sort.Slice(extensions, func(i, j int) bool {
		return strings.ToLower(extensions[i].Repo) < strings.ToLower(extensions[j].Repo)
	})
	return extensions, nil
}

func (localBroker *broker) putOrbConfigBundle(ctx context.Context, bundle orbconfig.Bundle) error {
	body, err := json.Marshal(orbConfigUploadRequest{
		BrokerID:          localBroker.config.BrokerID,
		SessionID:         localBroker.sessionID,
		SessionGeneration: localBroker.sessionGeneration,
		Bundle:            bundle,
	})
	if err != nil {
		return fmt.Errorf("encode orb configuration bundle: %w", err)
	}
	if len(body) > orbconfig.MaxBodyBytes {
		return fmt.Errorf("orb configuration bundle exceeds %d bytes", orbconfig.MaxBodyBytes)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, localBroker.config.APIURL+orbConfigBundleEndpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create orb configuration upload: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+localBroker.config.apiKey)
	request.Header.Set("Content-Type", "application/json")
	response, err := localBroker.client.Do(request)
	if err != nil {
		return fmt.Errorf("upload orb configuration bundle: %w", err)
	}
	responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, maxHeartbeatBody+1))
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil {
		return errors.Join(readErr, closeErr)
	}
	if len(responseBody) > maxHeartbeatBody {
		return errors.New("orb configuration upload response exceeds 1 MiB")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return heartbeatRejectionError(response.StatusCode, responseBody)
	}
	if err := rejectDuplicateJSONFields(responseBody); err != nil {
		return errors.New("orb configuration upload response is invalid JSON")
	}
	var decoded orbConfigUploadResponse
	decoder := json.NewDecoder(bytes.NewReader(responseBody))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&decoded); err != nil {
		return errors.New("orb configuration upload response is invalid JSON")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("orb configuration upload response contains trailing JSON")
	}
	if !decoded.OK || decoded.Digest != bundle.Digest {
		return errors.New("orb configuration upload response failed validation")
	}
	return nil
}
