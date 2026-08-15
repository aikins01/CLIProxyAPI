package amp

import (
	"context"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/orbconfig"
)

const (
	neoOrbOwnerConfigDigestPath = "/root/.config/.cliproxyapi-owner-orb-config-digest"
	neoOrbOwnerConfigStagePath  = "/root/.config/.cliproxyapi-agents-stage"
	neoOrbOwnerConfigBackupPath = "/root/.config/.cliproxyapi-agents-backup"
	neoOrbAgentsConfigPath      = "/root/.config/agents"
)

func (m *neoOrbManager) orbReconcileOwnerConfig(ctx context.Context, cfg *config.Config, client neoOrbProviderClient, containerID, ownerUserID string) (bool, error) {
	if m == nil || m.runtime == nil {
		return false, errors.New("owner orb configuration runtime is unavailable")
	}
	for attempt := 0; attempt < 2; attempt++ {
		beforeDigest, beforeFound, err := m.runtime.ownerOrbConfigDigest(ownerUserID)
		if err != nil {
			return false, err
		}
		found, err := m.orbReconcileOwnerConfigOnce(ctx, cfg, client, containerID, ownerUserID)
		if err != nil {
			return found, err
		}
		afterDigest, afterFound, err := m.runtime.ownerOrbConfigDigest(ownerUserID)
		if err != nil {
			return found, err
		}
		if beforeFound == afterFound && beforeDigest == afterDigest {
			return found, nil
		}
	}
	return false, errors.New("owner orb configuration changed during reconciliation")
}

func (m *neoOrbManager) orbReconcileOwnerConfigOnce(ctx context.Context, cfg *config.Config, client neoOrbProviderClient, containerID, ownerUserID string) (bool, error) {
	bundle, files, found, err := m.runtime.ownerOrbConfig(ownerUserID)
	if err != nil || !found {
		return found, err
	}
	applied, err := client.Exec(ctx, containerID, []string{"cat", neoOrbOwnerConfigDigestPath}, nil, "/")
	if err != nil {
		return true, err
	}
	if applied.ExitCode == 0 && strings.TrimSpace(applied.Stdout) == bundle.Digest {
		return true, nil
	}
	prepare := "set -eu\numask 077\nmkdir -p /root/.config\nrm -f " + neoOrbOwnerConfigDigestPath + "\nrm -rf " + neoOrbOwnerConfigStagePath + " " + neoOrbOwnerConfigBackupPath + "\nmkdir -m 0700 " + neoOrbOwnerConfigStagePath
	result, err := client.Exec(ctx, containerID, []string{"/bin/sh", "-lc", prepare}, nil, "/")
	if err != nil {
		return true, err
	}
	if result.ExitCode != 0 {
		return true, fmt.Errorf("owner config staging exited %d: %s", result.ExitCode, clipNeoErrorBody([]byte(result.Stderr)))
	}
	directories := map[string]bool{}
	filesByDirectory := map[string]map[string][]byte{}
	for filePath, content := range files {
		directory := path.Dir(filePath)
		directories[directory] = true
		if filesByDirectory[directory] == nil {
			filesByDirectory[directory] = map[string][]byte{}
		}
		filesByDirectory[directory][path.Base(filePath)] = content
	}
	directoryNames := make([]string, 0, len(directories))
	for directory := range directories {
		directoryNames = append(directoryNames, directory)
	}
	sort.Strings(directoryNames)
	if len(directoryNames) > 0 {
		arguments := []string{"mkdir", "-p"}
		for _, directory := range directoryNames {
			arguments = append(arguments, neoOrbOwnerConfigStagePath+"/"+directory)
		}
		mkdir, err := client.Exec(ctx, containerID, arguments, nil, "/")
		if err != nil {
			return true, err
		}
		if mkdir.ExitCode != 0 {
			return true, fmt.Errorf("owner config directory creation exited %d: %s", mkdir.ExitCode, clipNeoErrorBody([]byte(mkdir.Stderr)))
		}
	}
	for _, directory := range directoryNames {
		if err := client.CopyTarToContainer(ctx, containerID, neoOrbOwnerConfigStagePath+"/"+directory, filesByDirectory[directory], 0o600); err != nil {
			return true, fmt.Errorf("copy owner config %s: %w", directory, err)
		}
	}
	permissions, err := client.Exec(ctx, containerID, []string{"find", neoOrbOwnerConfigStagePath, "-type", "d", "-exec", "chmod", "0700", "{}", "+"}, nil, "/")
	if err != nil {
		return true, err
	}
	if permissions.ExitCode != 0 {
		return true, fmt.Errorf("owner config permissions exited %d: %s", permissions.ExitCode, clipNeoErrorBody([]byte(permissions.Stderr)))
	}
	replace := "set -eu\nrm -rf " + neoOrbOwnerConfigBackupPath + "\nif [ -e " + neoOrbAgentsConfigPath + " ] || [ -L " + neoOrbAgentsConfigPath + " ]; then mv " + neoOrbAgentsConfigPath + " " + neoOrbOwnerConfigBackupPath + "; fi\nif mv " + neoOrbOwnerConfigStagePath + " " + neoOrbAgentsConfigPath + "; then rm -rf " + neoOrbOwnerConfigBackupPath + "; else if [ -e " + neoOrbOwnerConfigBackupPath + " ] || [ -L " + neoOrbOwnerConfigBackupPath + " ]; then mv " + neoOrbOwnerConfigBackupPath + " " + neoOrbAgentsConfigPath + "; fi; exit 1; fi"
	result, err = client.Exec(ctx, containerID, []string{"/bin/sh", "-lc", replace}, nil, "/")
	if err != nil {
		return true, err
	}
	if result.ExitCode != 0 {
		return true, fmt.Errorf("owner config replacement exited %d: %s", result.ExitCode, clipNeoErrorBody([]byte(result.Stderr)))
	}
	if err := m.orbReconcileGitHubExtensions(ctx, cfg, client, containerID, bundle.Extensions); err != nil {
		return true, err
	}
	if err := client.CopyFileToContainer(ctx, containerID, neoOrbOwnerConfigDigestPath, []byte(bundle.Digest+"\n"), 0o600); err != nil {
		return true, fmt.Errorf("persist owner config digest: %w", err)
	}
	return true, nil
}

func (m *neoOrbManager) orbReconcileGitHubExtensions(ctx context.Context, cfg *config.Config, client neoOrbProviderClient, containerID string, extensions []orbconfig.Extension) error {
	env := []string(nil)
	desiredNames := make(map[string]bool, len(extensions))
	for _, extension := range extensions {
		if err := orbconfig.ValidateExtension(extension); err != nil {
			return err
		}
		desiredNames[strings.ToLower(path.Base(extension.Repo))] = true
		install, err := client.Exec(ctx, containerID, []string{"gh", "extension", "install", extension.Repo, "--pin", extension.Version, "--force"}, env, "/")
		if err != nil {
			return err
		}
		if install.ExitCode != 0 {
			return fmt.Errorf("install GitHub extension %s at %s exited %d: %s", extension.Repo, extension.Version, install.ExitCode, clipNeoErrorBody([]byte(install.Stderr)))
		}
	}
	listScript := "if [ -d /root/.local/share/gh/extensions ]; then find /root/.local/share/gh/extensions -mindepth 1 -maxdepth 1 -type d -printf '%f\\n'; fi"
	listed, err := client.Exec(ctx, containerID, []string{"/bin/sh", "-lc", listScript}, env, "/")
	if err != nil {
		return err
	}
	if listed.ExitCode != 0 {
		return fmt.Errorf("list GitHub extensions exited %d: %s", listed.ExitCode, clipNeoErrorBody([]byte(listed.Stderr)))
	}
	for _, name := range strings.Split(strings.TrimSpace(listed.Stdout), "\n") {
		name = strings.TrimSpace(name)
		if name == "" || desiredNames[strings.ToLower(name)] {
			continue
		}
		probe := orbconfig.Extension{Repo: "owner/" + name, Version: strings.Repeat("0", 40)}
		if orbconfig.ValidateExtension(probe) != nil {
			return fmt.Errorf("installed GitHub extension name %q is invalid", name)
		}
		remove, err := client.Exec(ctx, containerID, []string{"gh", "extension", "remove", name}, env, "/")
		if err != nil {
			return err
		}
		if remove.ExitCode != 0 {
			return fmt.Errorf("remove GitHub extension %s exited %d: %s", name, remove.ExitCode, clipNeoErrorBody([]byte(remove.Stderr)))
		}
	}
	return nil
}
