package amp

import (
	"context"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/orbconfig"
)

func TestNeoOrbOwnerConfigReconcileExactReplacementExtensionsAndNoOp(t *testing.T) {
	runtime, fake := newNeoOrbTestRuntime(t)
	manager := runtime.orbManager
	ownerUserID := "owner_user"
	commit := strings.Repeat("a", 40)
	bundle, err := orbconfig.New([]orbconfig.DecodedFile{
		{Path: "checks/review.md", Content: []byte("review")},
		{Path: "skills/demo/SKILL.md", Content: []byte("---\nname: demo\n---\n")},
	}, []orbconfig.Extension{{Repo: "owner/gh-demo", Version: commit}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.orbConfigStore.put(ownerUserID, bundle); err != nil {
		t.Fatal(err)
	}
	fake.execHandler = func(command []string) neoOrbExecResult {
		joined := strings.Join(command, " ")
		if joined == "cat "+neoOrbOwnerConfigDigestPath {
			fake.mu.Lock()
			digest := string(fake.copiedFiles[neoOrbOwnerConfigDigestPath])
			fake.mu.Unlock()
			if digest == "" {
				return neoOrbExecResult{ExitCode: 1}
			}
			return neoOrbExecResult{Stdout: digest}
		}
		if strings.Contains(joined, "find /root/.local/share/gh/extensions") {
			return neoOrbExecResult{Stdout: "gh-demo\ngh-old-extension\n"}
		}
		return neoOrbExecResult{}
	}
	found, err := manager.orbReconcileOwnerConfig(context.Background(), runtime.configSnapshot(), fake, "container", ownerUserID)
	if err != nil || !found {
		t.Fatalf("reconcile found=%t err=%v", found, err)
	}
	joinedCalls := strings.Join(fake.calls, "\n")
	for _, want := range []string{
		"exec:gh extension install owner/gh-demo --pin " + commit + " --force",
		"exec:gh extension remove gh-old-extension",
		"rm -f " + neoOrbOwnerConfigDigestPath,
		"copy-tar:" + neoOrbOwnerConfigStagePath + "/checks:review.md",
		"copy-tar:" + neoOrbOwnerConfigStagePath + "/skills/demo:SKILL.md",
		"mv " + neoOrbAgentsConfigPath + " " + neoOrbOwnerConfigBackupPath,
		"mv " + neoOrbOwnerConfigStagePath + " " + neoOrbAgentsConfigPath,
		"copy:" + neoOrbOwnerConfigDigestPath + ":65:384",
	} {
		if !strings.Contains(joinedCalls, want) {
			t.Fatalf("missing call %q in:\n%s", want, joinedCalls)
		}
	}
	replaceIndex := strings.Index(joinedCalls, "mv "+neoOrbOwnerConfigStagePath+" "+neoOrbAgentsConfigPath)
	extensionIndex := strings.Index(joinedCalls, "exec:gh extension install owner/gh-demo --pin "+commit+" --force")
	digestIndex := strings.Index(joinedCalls, "copy:"+neoOrbOwnerConfigDigestPath)
	if replaceIndex < 0 || extensionIndex < replaceIndex || digestIndex < extensionIndex {
		t.Fatalf("reconcile order is not files, extensions, digest:\n%s", joinedCalls)
	}
	beforeNoOp := len(fake.calls)
	found, err = manager.orbReconcileOwnerConfig(context.Background(), runtime.configSnapshot(), fake, "container", ownerUserID)
	if err != nil || !found {
		t.Fatalf("no-op reconcile found=%t err=%v", found, err)
	}
	if got := len(fake.calls) - beforeNoOp; got != 1 || !strings.HasPrefix(fake.calls[beforeNoOp], "exec:cat ") {
		t.Fatalf("unchanged reconcile calls = %#v", fake.calls[beforeNoOp:])
	}

	empty, err := orbconfig.New(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.orbConfigStore.put(ownerUserID, empty); err != nil {
		t.Fatal(err)
	}
	beforeClear := len(fake.calls)
	found, err = manager.orbReconcileOwnerConfig(context.Background(), runtime.configSnapshot(), fake, "container", ownerUserID)
	if err != nil || !found {
		t.Fatalf("empty reconcile found=%t err=%v", found, err)
	}
	clearCalls := strings.Join(fake.calls[beforeClear:], "\n")
	if strings.Contains(clearCalls, "copy-tar:") || !strings.Contains(clearCalls, "rm -rf "+neoOrbOwnerConfigBackupPath) || !strings.Contains(clearCalls, "mv "+neoOrbOwnerConfigStagePath+" "+neoOrbAgentsConfigPath) {
		t.Fatalf("empty exact replacement calls:\n%s", clearCalls)
	}
}

func TestNeoOrbOwnerConfigMissingUsesFallbackContract(t *testing.T) {
	runtime, fake := newNeoOrbTestRuntime(t)
	found, err := runtime.orbManager.orbReconcileOwnerConfig(context.Background(), runtime.configSnapshot(), fake, "container", "owner_without_bundle")
	if err != nil || found {
		t.Fatalf("missing bundle found=%t err=%v", found, err)
	}
	if len(fake.calls) != 0 {
		t.Fatalf("missing bundle touched orb: %#v", fake.calls)
	}
}

func TestNeoOrbOwnerConfigReconcileRetriesChangedDigest(t *testing.T) {
	runtime, fake := newNeoOrbTestRuntime(t)
	ownerUserID := "owner_digest_retry"
	first, err := orbconfig.New([]orbconfig.DecodedFile{{Path: "checks/review.md", Content: []byte("first")}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	second, err := orbconfig.New([]orbconfig.DecodedFile{{Path: "checks/review.md", Content: []byte("second")}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.orbConfigStore.put(ownerUserID, first); err != nil {
		t.Fatal(err)
	}
	replaced := false
	fake.execHandler = func(command []string) neoOrbExecResult {
		joined := strings.Join(command, " ")
		if joined == "cat "+neoOrbOwnerConfigDigestPath {
			fake.mu.Lock()
			digest := string(fake.copiedFiles[neoOrbOwnerConfigDigestPath])
			fake.mu.Unlock()
			if digest == "" {
				return neoOrbExecResult{ExitCode: 1}
			}
			return neoOrbExecResult{Stdout: digest}
		}
		if !replaced && strings.Contains(joined, "mv "+neoOrbOwnerConfigStagePath+" "+neoOrbAgentsConfigPath) {
			replaced = true
			if _, err := runtime.orbConfigStore.put(ownerUserID, second); err != nil {
				t.Fatalf("replace owner config during reconcile: %v", err)
			}
		}
		return neoOrbExecResult{}
	}
	found, err := runtime.orbManager.orbReconcileOwnerConfig(context.Background(), runtime.configSnapshot(), fake, "container", ownerUserID)
	if err != nil || !found {
		t.Fatalf("reconcile found=%t err=%v", found, err)
	}
	if got := strings.TrimSpace(string(fake.copiedFiles[neoOrbOwnerConfigDigestPath])); got != second.Digest {
		t.Fatalf("applied digest = %q, want %q", got, second.Digest)
	}
	if got := strings.Count(strings.Join(fake.calls, "\n"), "mv "+neoOrbOwnerConfigStagePath+" "+neoOrbAgentsConfigPath); got != 2 {
		t.Fatalf("owner config replacements = %d, want 2", got)
	}
}

func TestNeoOrbOwnerConfigReconcileRejectsRepeatedDigestChurn(t *testing.T) {
	runtime, fake := newNeoOrbTestRuntime(t)
	ownerUserID := "owner_digest_churn"
	bundles := make([]orbconfig.Bundle, 3)
	for index, content := range []string{"first", "second", "third"} {
		bundle, err := orbconfig.New([]orbconfig.DecodedFile{{Path: "checks/review.md", Content: []byte(content)}}, nil)
		if err != nil {
			t.Fatal(err)
		}
		bundles[index] = bundle
	}
	if _, err := runtime.orbConfigStore.put(ownerUserID, bundles[0]); err != nil {
		t.Fatal(err)
	}
	replacements := 0
	fake.execHandler = func(command []string) neoOrbExecResult {
		joined := strings.Join(command, " ")
		if joined == "cat "+neoOrbOwnerConfigDigestPath {
			fake.mu.Lock()
			digest := string(fake.copiedFiles[neoOrbOwnerConfigDigestPath])
			fake.mu.Unlock()
			if digest == "" {
				return neoOrbExecResult{ExitCode: 1}
			}
			return neoOrbExecResult{Stdout: digest}
		}
		if strings.Contains(joined, "mv "+neoOrbOwnerConfigStagePath+" "+neoOrbAgentsConfigPath) {
			replacements++
			if _, err := runtime.orbConfigStore.put(ownerUserID, bundles[replacements]); err != nil {
				t.Fatalf("churn owner config during reconcile: %v", err)
			}
		}
		return neoOrbExecResult{}
	}
	found, err := runtime.orbManager.orbReconcileOwnerConfig(context.Background(), runtime.configSnapshot(), fake, "container", ownerUserID)
	if err == nil || err.Error() != "owner orb configuration changed during reconciliation" || found {
		t.Fatalf("reconcile found=%t err=%v", found, err)
	}
	if replacements != 2 {
		t.Fatalf("owner config replacements = %d, want 2", replacements)
	}
}
