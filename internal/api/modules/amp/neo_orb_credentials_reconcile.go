package amp

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/orbcredentials"
	"golang.org/x/crypto/ssh"
)

const (
	neoOrbOwnerCredentialRevisionPath = "/root/.config/.cliproxyapi-owner-orb-credentials-revision"
	neoOrbOwnerChannelRevisionFile    = ".cliproxyapi-owner-orb-credentials-revision"
	neoOrbOwnerSSHRevisionPath        = "/root/.ssh/" + neoOrbOwnerChannelRevisionFile
	neoOrbOwnerGHRevisionPath         = "/root/.config/gh/" + neoOrbOwnerChannelRevisionFile
	neoOrbOwnerSSHStagePath           = "/root/.cliproxyapi-ssh-stage"
	neoOrbOwnerSSHBackupPath          = "/root/.cliproxyapi-ssh-backup"
	neoOrbOwnerGHStagePath            = "/root/.cliproxyapi-gh-stage"
	neoOrbOwnerGHBackupPath           = "/root/.cliproxyapi-gh-backup"
	neoOrbOwnerGitConfigBackupPath    = "/root/.cliproxyapi-gitconfig-backup"
)

type neoOrbCredentialSelection struct {
	Found        bool
	Revoked      bool
	HasGitHub    bool
	HasUsableSSH bool
}

func (selection neoOrbCredentialSelection) suppressSharedGitHub() bool {
	return selection.Found || selection.Revoked
}

func (m *neoOrbManager) orbReconcileOwnerCredentials(ctx context.Context, client neoOrbProviderClient, containerID, ownerUserID string) (neoOrbCredentialSelection, error) {
	if m == nil || m.runtime == nil || m.runtime.orbCredentialStore == nil {
		return neoOrbCredentialSelection{}, nil
	}
	for attempt := 0; attempt < 2; attempt++ {
		selection, appliedRevision, err := m.orbReconcileOwnerCredentialsAtRevision(ctx, client, containerID, ownerUserID)
		if err != nil {
			return selection, err
		}
		currentRevision, err := m.runtime.orbCredentialStore.revision(ownerUserID)
		if err != nil {
			return selection, err
		}
		if currentRevision == appliedRevision {
			return selection, nil
		}
	}
	return neoOrbCredentialSelection{}, errors.New("owner credentials changed during reconciliation")
}

func (m *neoOrbManager) orbReconcileOwnerCredentialsAtRevision(ctx context.Context, client neoOrbProviderClient, containerID, ownerUserID string) (neoOrbCredentialSelection, string, error) {
	credentials, revision, found, revoked, err := m.runtime.ownerOrbCredentials(ownerUserID)
	if err != nil {
		return neoOrbCredentialSelection{}, "", err
	}
	if revoked {
		return m.orbReconcileRevokedOwnerCredentials(ctx, client, containerID, revision)
	}
	if !found {
		return m.orbReconcileAbsentOwnerCredentials(ctx, client, containerID)
	}
	selection := neoOrbCredentialSelection{
		Found:        true,
		HasGitHub:    credentials.GitHubToken != "",
		HasUsableSSH: credentials.SSH != nil && len(credentials.SSH.Identities) > 0 && neoOrbKnownHostsIncludesGitHub(credentials.SSH.KnownHosts),
	}
	appliedRevision, appliedRevisionFound, err := m.orbReadOwnerCredentialMarker(ctx, client, containerID, neoOrbOwnerCredentialRevisionPath)
	if err != nil {
		return selection, "", err
	}
	sshMarker, sshMarkerFound, err := m.orbReadOwnerCredentialMarker(ctx, client, containerID, neoOrbOwnerSSHRevisionPath)
	if err != nil {
		return selection, "", err
	}
	githubMarker, githubMarkerFound, err := m.orbReadOwnerCredentialMarker(ctx, client, containerID, neoOrbOwnerGHRevisionPath)
	if err != nil {
		return selection, "", err
	}
	managedSSH := sshMarkerFound && neoOrbCredentialRevisionPattern.MatchString(sshMarker)
	managedGitHub := githubMarkerFound && neoOrbCredentialRevisionPattern.MatchString(githubMarker)
	desiredSSH := credentials.SSH != nil
	desiredGitHub := credentials.GitHubToken != ""
	installSSH := desiredSSH && (!managedSSH || sshMarker != revision)
	installGitHub := desiredGitHub && (!managedGitHub || githubMarker != revision)
	removeSSH := !desiredSSH && managedSSH
	removeGitHub := !desiredGitHub && managedGitHub
	channelsCurrent := !installSSH && !installGitHub && !removeSSH && !removeGitHub
	if appliedRevisionFound && appliedRevision == revision && channelsCurrent {
		return selection, revision, nil
	}
	if !channelsCurrent {
		prepare := "set -eu\numask 077\nmkdir -p /root/.config\nrm -rf " + neoOrbOwnerSSHStagePath + " " + neoOrbOwnerSSHBackupPath + " " + neoOrbOwnerGHStagePath + " " + neoOrbOwnerGHBackupPath + " " + neoOrbOwnerGitConfigBackupPath
		if installSSH {
			prepare += "\nmkdir -m 0700 " + neoOrbOwnerSSHStagePath
		}
		if installGitHub {
			prepare += "\nmkdir -m 0700 " + neoOrbOwnerGHStagePath
		}
		result, execErr := client.Exec(ctx, containerID, []string{"/bin/sh", "-lc", prepare}, nil, "/")
		if execErr != nil {
			return selection, "", execErr
		}
		if result.ExitCode != 0 {
			return selection, "", errors.New("owner credential staging failed")
		}
		cleanupStages := true
		defer func() {
			if cleanupStages {
				m.orbCleanupOwnerCredentialStages(context.WithoutCancel(ctx), client, containerID)
			}
		}()
		if installSSH {
			if err := m.orbStageOwnerSSHCredentials(ctx, client, containerID, credentials.SSH, revision); err != nil {
				return selection, "", err
			}
		}
		if installGitHub {
			if err := client.CopyFileToContainer(ctx, containerID, neoOrbOwnerGHStagePath+"/hosts.yml", neoOrbGitHubHosts(credentials.GitHubToken), 0o600); err != nil {
				return selection, "", errors.New("owner GitHub credential staging failed")
			}
			if err := client.CopyFileToContainer(ctx, containerID, neoOrbOwnerGHStagePath+"/"+neoOrbOwnerChannelRevisionFile, []byte(revision+"\n"), 0o600); err != nil {
				return selection, "", errors.New("owner GitHub credential staging failed")
			}
		}
		expectedSSHRevision := ""
		if managedSSH {
			expectedSSHRevision = sshMarker
		}
		expectedGitHubRevision := ""
		if managedGitHub {
			expectedGitHubRevision = githubMarker
		}
		replace := neoOrbOwnerCredentialReplacementScript(selection, installSSH, installGitHub, removeSSH, removeGitHub, expectedSSHRevision, expectedGitHubRevision)
		result, execErr = client.Exec(ctx, containerID, []string{"/bin/sh", "-lc", replace}, nil, "/")
		if execErr != nil {
			return selection, "", execErr
		}
		if result.ExitCode != 0 {
			return selection, "", errors.New("owner credential replacement failed")
		}
		cleanupStages = false
	}
	if err := client.CopyFileToContainer(ctx, containerID, neoOrbOwnerCredentialRevisionPath, []byte(revision+"\n"), 0o600); err != nil {
		return selection, "", fmt.Errorf("owner credential revision could not be persisted: %w", err)
	}
	return selection, revision, nil
}

func neoOrbOwnerCredentialReplacementScript(selection neoOrbCredentialSelection, installSSH, installGitHub, removeSSH, removeGitHub bool, expectedSSHRevision, expectedGitHubRevision string) string {
	replaceSSH := installSSH || removeSSH
	replaceGitHub := installGitHub || removeGitHub
	configureGitHub := replaceGitHub || replaceSSH && selection.HasGitHub
	var script strings.Builder
	script.WriteString("set -eu\numask 077\nssh_touched=0\ngithub_touched=0\ngitconfig_touched=0\nrollback() {\n  status=$?\n  trap - EXIT")
	if replaceSSH {
		script.WriteString("\n  if [ \"$ssh_touched\" = 1 ]; then rm -rf /root/.ssh; if [ -e " + neoOrbOwnerSSHBackupPath + " ] || [ -L " + neoOrbOwnerSSHBackupPath + " ]; then mv " + neoOrbOwnerSSHBackupPath + " /root/.ssh; fi; fi")
	}
	if replaceGitHub {
		script.WriteString("\n  if [ \"$github_touched\" = 1 ]; then rm -rf /root/.config/gh; if [ -e " + neoOrbOwnerGHBackupPath + " ] || [ -L " + neoOrbOwnerGHBackupPath + " ]; then mv " + neoOrbOwnerGHBackupPath + " /root/.config/gh; fi; fi")
	}
	if configureGitHub {
		script.WriteString("\n  if [ \"$gitconfig_touched\" = 1 ]; then rm -rf /root/.gitconfig; if [ -e " + neoOrbOwnerGitConfigBackupPath + " ] || [ -L " + neoOrbOwnerGitConfigBackupPath + " ]; then mv " + neoOrbOwnerGitConfigBackupPath + " /root/.gitconfig; fi; fi")
	}
	script.WriteString("\n  rm -rf " + neoOrbOwnerSSHStagePath + " " + neoOrbOwnerGHStagePath + "\n  exit \"$status\"\n}\ntrap rollback EXIT")
	if replaceSSH {
		if expectedSSHRevision != "" {
			script.WriteString("\n[ \"$(cat " + neoOrbOwnerSSHRevisionPath + ")\" = '" + expectedSSHRevision + "' ]")
		}
		script.WriteString("\nif [ -e /root/.ssh ] || [ -L /root/.ssh ]; then mv /root/.ssh " + neoOrbOwnerSSHBackupPath + "; fi\nssh_touched=1")
	}
	if replaceGitHub {
		if expectedGitHubRevision != "" {
			script.WriteString("\n[ \"$(cat " + neoOrbOwnerGHRevisionPath + ")\" = '" + expectedGitHubRevision + "' ]")
		}
		script.WriteString("\nif [ -e /root/.config/gh ] || [ -L /root/.config/gh ]; then mv /root/.config/gh " + neoOrbOwnerGHBackupPath + "; fi\ngithub_touched=1")
	}
	if configureGitHub {
		script.WriteString("\nif [ -e /root/.gitconfig ] || [ -L /root/.gitconfig ]; then cp -a /root/.gitconfig " + neoOrbOwnerGitConfigBackupPath + "; fi\ngitconfig_touched=1")
	}
	if installSSH {
		script.WriteString("\nmv " + neoOrbOwnerSSHStagePath + " /root/.ssh\nchmod 0700 /root/.ssh")
	}
	if installGitHub {
		script.WriteString("\nmv " + neoOrbOwnerGHStagePath + " /root/.config/gh\nchmod 0700 /root/.config/gh")
	}
	if configureGitHub {
		script.WriteByte('\n')
		script.WriteString(neoOrbOwnerGitHubAuthScript(selection))
	}
	script.WriteString("\ntrap - EXIT\nrm -rf")
	if replaceSSH {
		script.WriteString(" " + neoOrbOwnerSSHBackupPath)
	}
	if replaceGitHub {
		script.WriteString(" " + neoOrbOwnerGHBackupPath)
	}
	if configureGitHub {
		script.WriteString(" " + neoOrbOwnerGitConfigBackupPath)
	}
	return script.String()
}

func (m *neoOrbManager) orbReconcileRevokedOwnerCredentials(ctx context.Context, client neoOrbProviderClient, containerID, revision string) (neoOrbCredentialSelection, string, error) {
	selection := neoOrbCredentialSelection{Revoked: true}
	appliedRevision, appliedRevisionFound, err := m.orbReadOwnerCredentialMarker(ctx, client, containerID, neoOrbOwnerCredentialRevisionPath)
	if err != nil {
		return selection, "", err
	}
	sshRevision, githubRevision, err := m.orbManagedOwnerCredentialChannels(ctx, client, containerID)
	if err != nil {
		return selection, "", err
	}
	if appliedRevisionFound && appliedRevision == neoOrbCredentialRevokedRevision && sshRevision == "" && githubRevision == "" {
		return selection, revision, nil
	}
	if err := m.orbRemoveManagedOwnerCredentialChannels(ctx, client, containerID, sshRevision, githubRevision); err != nil {
		return selection, "", errors.New("owner credential revocation cleanup failed")
	}
	if appliedRevisionFound && appliedRevision == neoOrbCredentialRevokedRevision {
		return selection, revision, nil
	}
	if err := client.CopyFileToContainer(ctx, containerID, neoOrbOwnerCredentialRevisionPath, []byte(revision+"\n"), 0o600); err != nil {
		return selection, "", errors.New("owner credential revocation marker could not be persisted")
	}
	return selection, revision, nil
}

func (m *neoOrbManager) orbReconcileAbsentOwnerCredentials(ctx context.Context, client neoOrbProviderClient, containerID string) (neoOrbCredentialSelection, string, error) {
	selection := neoOrbCredentialSelection{}
	sshRevision, githubRevision, err := m.orbManagedOwnerCredentialChannels(ctx, client, containerID)
	if err != nil {
		return selection, "", err
	}
	if sshRevision == "" && githubRevision == "" {
		return selection, neoOrbCredentialAbsentRevision, nil
	}
	if err := m.orbRemoveManagedOwnerCredentialChannels(ctx, client, containerID, sshRevision, githubRevision); err != nil {
		return selection, "", errors.New("owner credential absence cleanup failed")
	}
	return selection, neoOrbCredentialAbsentRevision, nil
}

func (m *neoOrbManager) orbManagedOwnerCredentialChannels(ctx context.Context, client neoOrbProviderClient, containerID string) (string, string, error) {
	sshMarker, sshMarkerFound, err := m.orbReadOwnerCredentialMarker(ctx, client, containerID, neoOrbOwnerSSHRevisionPath)
	if err != nil {
		return "", "", err
	}
	githubMarker, githubMarkerFound, err := m.orbReadOwnerCredentialMarker(ctx, client, containerID, neoOrbOwnerGHRevisionPath)
	if err != nil {
		return "", "", err
	}
	if !sshMarkerFound || !neoOrbCredentialRevisionPattern.MatchString(sshMarker) {
		sshMarker = ""
	}
	if !githubMarkerFound || !neoOrbCredentialRevisionPattern.MatchString(githubMarker) {
		githubMarker = ""
	}
	return sshMarker, githubMarker, nil
}

func (m *neoOrbManager) orbRemoveManagedOwnerCredentialChannels(ctx context.Context, client neoOrbProviderClient, containerID, sshRevision, githubRevision string) error {
	cleanup := "set -eu\numask 077\nmkdir -p /root/.config\nrm -rf " + neoOrbOwnerSSHStagePath + " " + neoOrbOwnerSSHBackupPath + " " + neoOrbOwnerGHStagePath + " " + neoOrbOwnerGHBackupPath + " " + neoOrbOwnerGitConfigBackupPath
	if sshRevision != "" || githubRevision != "" {
		cleanup += "\n" + neoOrbOwnerCredentialReplacementScript(neoOrbCredentialSelection{}, false, false, sshRevision != "", githubRevision != "", sshRevision, githubRevision)
	}
	result, err := client.Exec(ctx, containerID, []string{"/bin/sh", "-lc", cleanup}, nil, "/")
	if err != nil {
		return err
	}
	if result.ExitCode != 0 {
		return errors.New("owner credential cleanup failed")
	}
	return nil
}

func (m *neoOrbManager) orbCleanupOwnerCredentialStages(ctx context.Context, client neoOrbProviderClient, containerID string) {
	_, _ = client.Exec(ctx, containerID, []string{"rm", "-rf", neoOrbOwnerSSHStagePath, neoOrbOwnerGHStagePath}, nil, "/")
}

func (m *neoOrbManager) orbReadOwnerCredentialMarker(ctx context.Context, client neoOrbProviderClient, containerID, markerPath string) (string, bool, error) {
	result, err := client.Exec(ctx, containerID, []string{"cat", markerPath}, nil, "/")
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return "", false, ctxErr
		}
		return "", false, neoOrbOwnerCredentialMarkerInspectionError(markerPath)
	}
	if result.ExitCode == 0 {
		return strings.TrimSpace(result.Stdout), true, nil
	}
	missing, err := client.Exec(ctx, containerID, []string{"/bin/sh", "-lc", "if [ ! -e \"$1\" ] && [ ! -L \"$1\" ]; then exit 0; fi; exit 1", "owner-credential-marker-probe", markerPath}, nil, "/")
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return "", false, ctxErr
		}
		return "", false, neoOrbOwnerCredentialMarkerInspectionError(markerPath)
	}
	if missing.ExitCode == 0 {
		return "", false, nil
	}
	return "", false, neoOrbOwnerCredentialMarkerInspectionError(markerPath)
}

func neoOrbOwnerCredentialMarkerInspectionError(markerPath string) error {
	switch markerPath {
	case neoOrbOwnerCredentialRevisionPath:
		return errors.New("owner credential revision marker inspection failed")
	case neoOrbOwnerSSHRevisionPath:
		return errors.New("owner SSH credential marker inspection failed")
	case neoOrbOwnerGHRevisionPath:
		return errors.New("owner GitHub credential marker inspection failed")
	default:
		return errors.New("owner credential marker inspection failed")
	}
}

func neoOrbKnownHostsIncludesGitHub(content []byte) bool {
	remaining := content
	for len(bytes.TrimSpace(remaining)) > 0 {
		marker, hosts, _, _, rest, err := ssh.ParseKnownHosts(remaining)
		if err != nil || len(rest) >= len(remaining) {
			return false
		}
		if marker == "" {
			for _, host := range hosts {
				if neoOrbKnownHostMatchesGitHub(host) {
					return true
				}
			}
		}
		remaining = rest
	}
	return false
}

func neoOrbKnownHostMatchesGitHub(host string) bool {
	if strings.EqualFold(host, "github.com") || strings.EqualFold(host, "[github.com]:22") {
		return true
	}
	parts := strings.Split(host, "|")
	if len(parts) != 4 || parts[0] != "" || parts[1] != "1" {
		return false
	}
	salt, saltErr := base64.StdEncoding.DecodeString(parts[2])
	want, hashErr := base64.StdEncoding.DecodeString(parts[3])
	if saltErr != nil || hashErr != nil || len(salt) == 0 || len(want) != sha1.Size {
		return false
	}
	for _, candidate := range []string{"github.com", "[github.com]:22"} {
		mac := hmac.New(sha1.New, salt)
		_, _ = mac.Write([]byte(candidate))
		if hmac.Equal(mac.Sum(nil), want) {
			return true
		}
	}
	return false
}

func (m *neoOrbManager) orbStageOwnerSSHCredentials(ctx context.Context, client neoOrbProviderClient, containerID string, sshCredentials *orbcredentials.DecodedSSH, revision string) error {
	files := map[string][]byte{neoOrbOwnerChannelRevisionFile: []byte(revision + "\n")}
	configLines := []string{
		"Host *",
		"    BatchMode yes",
		"    IdentitiesOnly yes",
		"    StrictHostKeyChecking yes",
		"    UserKnownHostsFile /root/.ssh/known_hosts",
	}
	if sshCredentials != nil {
		for _, identity := range sshCredentials.Identities {
			files[identity.Name] = identity.PrivateKey
			if len(identity.PublicKey) > 0 {
				files[identity.Name+".pub"] = identity.PublicKey
			}
			configLines = append(configLines, "    IdentityFile /root/.ssh/"+identity.Name)
		}
		if len(sshCredentials.KnownHosts) > 0 {
			files["known_hosts"] = sshCredentials.KnownHosts
		}
	}
	files["config"] = []byte(strings.Join(configLines, "\n") + "\n")
	if err := client.CopyTarToContainer(ctx, containerID, neoOrbOwnerSSHStagePath, files, 0o600); err != nil {
		return errors.New("owner SSH credential staging failed")
	}
	return nil
}

func (m *neoOrbManager) orbConfigureOwnerGitHubAuth(ctx context.Context, client neoOrbProviderClient, containerID string, selection neoOrbCredentialSelection) error {
	script := "set -eu\numask 077\n" + neoOrbOwnerGitHubAuthScript(selection)
	result, err := client.Exec(ctx, containerID, []string{"/bin/sh", "-lc", script}, nil, "/")
	if err != nil {
		return err
	}
	if result.ExitCode != 0 {
		return fmt.Errorf("owner GitHub credential configuration exited %d", result.ExitCode)
	}
	return nil
}

func neoOrbOwnerGitHubAuthScript(selection neoOrbCredentialSelection) string {
	script := `export HOME=/root
export GH_CONFIG_DIR=/root/.config/gh
if [ -f /root/.gitconfig ]; then
  temp="$(mktemp)"
  awk '
    /^\[url "https:\/\/x-access-token:.*@github\.com\/"\]$/ { skip=1; next }
    /^\[/ { skip=0 }
    !skip { print }
  ' /root/.gitconfig > "$temp"
  chmod 0600 "$temp"
  mv "$temp" /root/.gitconfig
fi
git config --global --unset-all url.https://github.com/.insteadOf 2>/dev/null || true`
	if selection.HasGitHub {
		script += "\ngh auth setup-git --hostname github.com"
		if !selection.HasUsableSSH {
			script += "\ngit config --global --add url.https://github.com/.insteadOf git@github.com:"
		}
	}
	return script
}

func neoOrbGitHubHosts(token string) []byte {
	return []byte("github.com:\n    oauth_token: " + strconv.Quote(token) + "\n    git_protocol: https\n")
}
