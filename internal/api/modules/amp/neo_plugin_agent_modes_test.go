package amp

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/ampplugins"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
)

func neoTestSyncedPluginAgentMode(key string) ampplugins.AgentMode {
	return ampplugins.AgentMode{
		Key:                  key,
		Label:                "Synced " + key,
		Description:          "synced from the local broker",
		PluginName:           key,
		PluginPath:           "~/.config/amp/plugins/" + key + ".ts",
		PluginRepositoryName: neoPluginUserRepository,
		PluginScope:          neoPluginAgentModeUserScope,
		AgentName:            key + "-agent",
		AgentModel:           "openai/gpt-5",
		AgentInstructions:    "You are the " + key + " agent.",
		ReasoningEffort:      "high",
	}
}

func TestNeoBrokerPluginAgentModesSyncServeAndExpiry(t *testing.T) {
	useTempNeoThreadStore(t)
	oldPluginsDir := neoAmpUserPluginsDir
	neoAmpUserPluginsDir = func() string { return t.TempDir() }
	t.Cleanup(func() { neoAmpUserPluginsDir = oldPluginsDir })
	rt := newNeoRuntime(&config.Config{})
	userActor, _, allowed := rt.store.upsertForOwner(map[string]any{"name": "userActor", "key": neoLocalOwnerUserID}, true, neoLocalOwnerUserID)
	if !allowed || userActor == nil {
		t.Fatal("owner user actor was not created")
	}
	emptyThreads := []string{}
	runners := []neoLocalBrokerHeartbeatRunner{{RunnerID: "modes-runner", WorkingDirectory: t.TempDir(), RunningThreads: &emptyThreads}}
	modes := []ampplugins.AgentMode{
		neoTestSyncedPluginAgentMode("deep-blue"),
		neoTestSyncedPluginAgentMode("kimi-k3"),
		neoTestSyncedPluginAgentMode("high"),
		neoTestSyncedPluginAgentMode(neoDeepOneAgentModeKey),
	}
	heartbeat := neoLocalBrokerHeartbeatRequest{
		BrokerID:          "modes-broker",
		SessionID:         "modes-session",
		SessionGeneration: 1,
		Hostname:          "Test Host",
		PID:               1234,
		Runners:           &runners,
		PluginAgentModes:  &modes,
	}
	if _, err := userActor.syncLocalBrokerHeartbeatResult(heartbeat); err != nil {
		t.Fatalf("heartbeat with plugin agent modes: %v", err)
	}
	listed, err := rt.neoWebLocalServerPluginAgentModes(context.Background())
	if err != nil {
		t.Fatalf("list server plugin agent modes: %v", err)
	}
	keys := map[string]bool{}
	for _, raw := range listed {
		entry := mapValue(raw)
		keys[stringValue(entry["key"])] = true
		if stringValue(entry["key"]) == "deep-blue" {
			if stringValue(entry["pluginPath"]) != "~/.config/amp/plugins/deep-blue.ts" || stringValue(entry["pluginScope"]) != neoPluginAgentModeUserScope || stringValue(entry["label"]) != "Synced deep-blue" {
				t.Fatalf("synced deep-blue metadata = %#v", entry)
			}
		}
	}
	for _, want := range []string{neoDeepOneAgentModeKey, neoDeepTwoAgentModeKey, neoDeepThreeAgentModeKey, "deep-blue", "kimi-k3"} {
		if !keys[want] {
			t.Fatalf("listed modes %v missing %q", keys, want)
		}
	}
	if got := userActor.syncedPluginAgentMode("high"); got != nil {
		t.Fatalf("reserved synced mode was stored: %#v", got)
	}
	mode, err := rt.loadNeoPluginAgentModeForOwner(neoLocalOwnerUserID, "kimi-k3")
	if err != nil {
		t.Fatalf("load synced plugin agent mode: %v", err)
	}
	if mode.AgentModel != "openai/gpt-5" || mode.AgentInstructions == "" || mode.ReasoningEffort != "high" {
		t.Fatalf("synced mode definition = %#v", mode)
	}
	if _, err := rt.loadNeoPluginAgentModeForOwner(neoLocalOwnerUserID, "missing-mode"); err == nil {
		t.Fatal("missing synced mode unexpectedly resolved")
	}

	heartbeat.PluginAgentModes = nil
	if _, err := userActor.syncLocalBrokerHeartbeatResult(heartbeat); err != nil {
		t.Fatalf("heartbeat without plugin agent modes: %v", err)
	}
	if got := userActor.syncedPluginAgentModes(); len(got) != 0 {
		t.Fatalf("synced modes after field removal = %#v", got)
	}

	if _, err := userActor.syncLocalBrokerHeartbeatResult(neoLocalBrokerHeartbeatRequest{
		BrokerID: "modes-broker", SessionID: "modes-session", SessionGeneration: 1,
		Hostname: "Test Host", PID: 1234, Runners: &runners, PluginAgentModes: &modes,
	}); err != nil {
		t.Fatalf("heartbeat re-adding plugin agent modes: %v", err)
	}
	userActor.mu.Lock()
	synced := userActor.userPluginAgentModes["modes-broker"]
	synced.updatedAt = time.Now().Add(-neoLocalBrokerHeartbeatTTL - time.Second)
	userActor.userPluginAgentModes["modes-broker"] = synced
	userActor.mu.Unlock()
	if got := userActor.syncedPluginAgentModes(); len(got) != 0 {
		t.Fatalf("expired synced modes = %#v", got)
	}
}

func TestValidateNeoBrokerPluginAgentModes(t *testing.T) {
	valid := neoTestSyncedPluginAgentMode("deep-blue")
	validTools := neoTestSyncedPluginAgentMode("kimi-k3")
	validTools.AgentTools = map[string]any{"include": []any{"shell_command"}}
	if err := validateNeoBrokerPluginAgentModes(&[]ampplugins.AgentMode{valid, validTools}); err != nil {
		t.Fatalf("valid payload rejected: %v", err)
	}

	cases := []struct {
		name   string
		mutate func(mode *ampplugins.AgentMode)
	}{
		{"invalid key", func(mode *ampplugins.AgentMode) { mode.Key = "Deep Blue" }},
		{"empty label", func(mode *ampplugins.AgentMode) { mode.Label = "" }},
		{"absolute plugin path", func(mode *ampplugins.AgentMode) { mode.PluginPath = "/Users/test/.config/amp/plugins/deep-blue.ts" }},
		{"wrong scope", func(mode *ampplugins.AgentMode) { mode.PluginScope = "workspace" }},
		{"missing model", func(mode *ampplugins.AgentMode) { mode.AgentModel = "" }},
		{"missing instructions", func(mode *ampplugins.AgentMode) { mode.AgentInstructions = "" }},
		{"invalid reasoning effort", func(mode *ampplugins.AgentMode) { mode.ReasoningEffort = "ludicrous" }},
		{"invalid tools", func(mode *ampplugins.AgentMode) { mode.AgentTools = map[string]any{"only": "shell_command"} }},
		{"oversized instructions", func(mode *ampplugins.AgentMode) {
			mode.AgentInstructions = strings.Repeat("x", neoBrokerPluginAgentModeInstructionsLimit+1)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mode := neoTestSyncedPluginAgentMode("deep-blue")
			tc.mutate(&mode)
			if err := validateNeoBrokerPluginAgentModes(&[]ampplugins.AgentMode{mode}); err == nil {
				t.Fatal("invalid payload was accepted")
			}
		})
	}

	duplicates := []ampplugins.AgentMode{neoTestSyncedPluginAgentMode("deep-blue"), neoTestSyncedPluginAgentMode("deep-blue")}
	if err := validateNeoBrokerPluginAgentModes(&duplicates); err == nil {
		t.Fatal("duplicate keys were accepted")
	}
	tooMany := make([]ampplugins.AgentMode, 0, neoBrokerPluginAgentModeLimit+1)
	for index := 0; index < neoBrokerPluginAgentModeLimit+1; index++ {
		mode := neoTestSyncedPluginAgentMode(fmt.Sprintf("synced-%d", index))
		tooMany = append(tooMany, mode)
	}
	if err := validateNeoBrokerPluginAgentModes(&tooMany); err == nil {
		t.Fatal("oversized mode list was accepted")
	}
}
