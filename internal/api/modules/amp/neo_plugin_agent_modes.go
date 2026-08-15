package amp

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/ampplugins"
)

const (
	neoDeepOneAgentModeKey      = "deep-1"
	neoDeepTwoAgentModeKey      = "deep-2"
	neoDeepThreeAgentModeKey    = "deep-3"
	neoDeepRedAgentModeKey      = "deep-red"
	neoDeepRedReasoningEffort   = "max"
	neoPluginUserRepository     = ampplugins.UserRepositoryName
	neoPluginAgentModeUserScope = ampplugins.UserScope
)

const (
	neoBrokerPluginAgentModeLimit             = 32
	neoBrokerPluginAgentModeLabelLimit        = 256
	neoBrokerPluginAgentModeTextLimit         = 1024
	neoBrokerPluginAgentModeInstructionsLimit = 64 * 1024
	neoBrokerPluginAgentModeToolsLimit        = 8 * 1024
)

var neoWebLocalDeepAgentModes = []struct {
	Key         string
	Label       string
	Description string
	Effort      string
}{
	{Key: neoDeepOneAgentModeKey, Label: "Deep 1", Description: "Focused deep reasoning for complex tasks.", Effort: "low"},
	{Key: neoDeepTwoAgentModeKey, Label: "Deep 2", Description: "Extended deep reasoning for difficult tasks.", Effort: "medium"},
	{Key: neoDeepThreeAgentModeKey, Label: "Deep 3", Description: "Maximum local deep reasoning for the hardest tasks.", Effort: "xhigh"},
}

var neoAmpUserPluginsDir = ampplugins.DefaultPluginsDir

type neoPluginAgentMode = ampplugins.AgentMode

type neoSyncedPluginAgentModes struct {
	sessionID         string
	sessionGeneration uint64
	modes             []*neoPluginAgentMode
	updatedAt         time.Time
}

func neoPluginReservedAgentModeKey(key string) bool {
	_, _, localDeepMode := neoWebLocalDeepAgentMode(key)
	return validNeoClientAgentMode(key) || localDeepMode
}

func neoWebLocalDeepAgentMode(key string) (string, string, bool) {
	key = strings.ToLower(strings.TrimSpace(key))
	for _, mode := range neoWebLocalDeepAgentModes {
		if mode.Key == key {
			return "deep", mode.Effort, true
		}
	}
	return "", "", false
}

func parseNeoPluginAgentModeSource(src string) (*neoPluginAgentMode, error) {
	return ampplugins.ParseSource(src, neoPluginReservedAgentModeKey)
}

func parseNeoPluginAgentModeSources(src string) ([]*neoPluginAgentMode, error) {
	return ampplugins.ParseSources(src, neoPluginReservedAgentModeKey)
}

func discoverNeoPluginAgentModes() ([]*neoPluginAgentMode, error) {
	return ampplugins.Discover(neoAmpUserPluginsDir(), neoPluginReservedAgentModeKey)
}

func loadNeoPluginAgentMode(key string) (*neoPluginAgentMode, error) {
	key = strings.ToLower(strings.TrimSpace(key))
	if key == "" {
		return nil, fmt.Errorf("plugin agent mode key is empty")
	}
	modes, err := discoverNeoPluginAgentModes()
	if err != nil {
		return nil, err
	}
	for _, mode := range modes {
		if strings.EqualFold(mode.Key, key) {
			return mode, nil
		}
	}
	return nil, fmt.Errorf("plugin agent mode %q is unavailable", key)
}

func loadNeoDeepRedPluginAgentMode() (*neoPluginAgentMode, error) {
	return loadNeoPluginAgentMode(neoDeepRedAgentModeKey)
}

func neoWebLocalServerPluginAgentModes() ([]any, error) {
	modes := make([]any, 0, len(neoWebLocalDeepAgentModes)+1)
	seen := map[string]bool{}
	for _, mode := range neoWebLocalDeepAgentModes {
		modes = append(modes, map[string]any{
			"key":                  mode.Key,
			"label":                mode.Label,
			"description":          mode.Description,
			"pluginName":           "cliproxy-local-modes",
			"pluginPath":           "local://cliproxy/deep",
			"pluginRepositoryName": "CLIProxyAPI",
			"pluginScope":          neoPluginAgentModeUserScope,
		})
		seen[mode.Key] = true
	}
	pluginModes, err := discoverNeoPluginAgentModes()
	if err != nil {
		return nil, err
	}
	for _, mode := range pluginModes {
		if seen[mode.Key] {
			continue
		}
		modes = append(modes, mode.Metadata())
		seen[mode.Key] = true
	}
	return modes, nil
}

func (rt *neoRuntime) neoWebLocalServerPluginAgentModes(ctx context.Context) ([]any, error) {
	modes, err := neoWebLocalServerPluginAgentModes()
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	for _, raw := range modes {
		if entry, ok := raw.(map[string]any); ok {
			seen[stringValue(entry["key"])] = true
		}
	}
	if rt == nil || rt.store == nil {
		return modes, nil
	}
	ownerUserID := firstNonEmptyString(rt.neoRequestOwnerUserID(ctx), neoLocalOwnerUserID)
	actor := rt.store.userActorForOwner(ownerUserID)
	if actor == nil {
		return modes, nil
	}
	for _, mode := range actor.syncedPluginAgentModes() {
		if seen[mode.Key] {
			continue
		}
		modes = append(modes, mode.Metadata())
		seen[mode.Key] = true
	}
	return modes, nil
}

func (rt *neoRuntime) loadNeoPluginAgentModeForOwner(ownerUserID, key string) (*neoPluginAgentMode, error) {
	mode, err := loadNeoPluginAgentMode(key)
	if err == nil {
		return mode, nil
	}
	if rt == nil || rt.store == nil {
		return nil, err
	}
	actor := rt.store.userActorForOwner(firstNonEmptyString(ownerUserID, neoLocalOwnerUserID))
	if actor == nil {
		return nil, err
	}
	if synced := actor.syncedPluginAgentMode(key); synced != nil {
		return synced, nil
	}
	return nil, err
}

func (rt *neoRuntime) loadNeoPluginAgentModeForRequest(ctx context.Context, key string) (*neoPluginAgentMode, error) {
	if rt == nil {
		return loadNeoPluginAgentMode(key)
	}
	return rt.loadNeoPluginAgentModeForOwner(rt.neoRequestOwnerUserID(ctx), key)
}

func (a *neoActor) syncedPluginAgentMode(key string) *neoPluginAgentMode {
	key = strings.ToLower(strings.TrimSpace(key))
	if key == "" {
		return nil
	}
	for _, mode := range a.syncedPluginAgentModes() {
		if strings.EqualFold(mode.Key, key) {
			return mode
		}
	}
	return nil
}

func (a *neoActor) syncedPluginAgentModes() []*neoPluginAgentMode {
	if a == nil {
		return nil
	}
	cutoff := time.Now().Add(-neoLocalBrokerHeartbeatTTL)
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.userPluginAgentModes) == 0 {
		return nil
	}
	brokerIDs := make([]string, 0, len(a.userPluginAgentModes))
	for brokerID, synced := range a.userPluginAgentModes {
		if synced.updatedAt.Before(cutoff) {
			delete(a.userPluginAgentModes, brokerID)
			continue
		}
		brokerIDs = append(brokerIDs, brokerID)
	}
	sort.Strings(brokerIDs)
	out := make([]*neoPluginAgentMode, 0)
	seen := map[string]bool{}
	for _, brokerID := range brokerIDs {
		for _, mode := range a.userPluginAgentModes[brokerID].modes {
			if seen[mode.Key] || neoPluginReservedAgentModeKey(mode.Key) {
				continue
			}
			seen[mode.Key] = true
			out = append(out, mode)
		}
	}
	return out
}

func (a *neoActor) storeSyncedPluginAgentModesLocked(brokerID, sessionID string, sessionGeneration uint64, modes *[]ampplugins.AgentMode, now time.Time) {
	if a.userPluginAgentModes == nil {
		a.userPluginAgentModes = map[string]neoSyncedPluginAgentModes{}
	}
	if modes == nil {
		delete(a.userPluginAgentModes, brokerID)
		return
	}
	accepted := make([]*neoPluginAgentMode, 0, len(*modes))
	for index := range *modes {
		mode := (*modes)[index]
		if neoPluginReservedAgentModeKey(mode.Key) {
			continue
		}
		if _, ok := normalizeNeoCustomAgentDefinition(mode.AgentDefinition()); !ok {
			continue
		}
		accepted = append(accepted, &mode)
	}
	a.userPluginAgentModes[brokerID] = neoSyncedPluginAgentModes{
		sessionID:         sessionID,
		sessionGeneration: sessionGeneration,
		modes:             accepted,
		updatedAt:         now,
	}
}

func validateNeoBrokerPluginAgentModes(modes *[]ampplugins.AgentMode) error {
	if modes == nil {
		return nil
	}
	if len(*modes) > neoBrokerPluginAgentModeLimit {
		return fmt.Errorf("pluginAgentModes contains too many entries")
	}
	seen := map[string]bool{}
	for index := range *modes {
		mode := &(*modes)[index]
		if !neoCustomAgentModePattern.MatchString(mode.Key) {
			return fmt.Errorf("pluginAgentModes contains an invalid key")
		}
		if seen[mode.Key] {
			return fmt.Errorf("pluginAgentModes contains a duplicate key")
		}
		seen[mode.Key] = true
		if !neoLocalBrokerSafeText(mode.Label, neoBrokerPluginAgentModeLabelLimit, false) ||
			!neoLocalBrokerSafeText(mode.Description, neoBrokerPluginAgentModeTextLimit, true) ||
			!neoLocalBrokerSafeText(mode.Color, 64, true) ||
			!neoLocalBrokerSafeText(mode.PluginName, neoBrokerPluginAgentModeLabelLimit, false) ||
			!neoLocalBrokerSafeText(mode.PluginRepositoryName, neoBrokerPluginAgentModeLabelLimit, false) ||
			!neoLocalBrokerSafeText(mode.AgentName, neoBrokerPluginAgentModeLabelLimit, true) ||
			!neoLocalBrokerSafeText(mode.AgentModel, neoBrokerPluginAgentModeLabelLimit, false) ||
			!neoLocalBrokerSafeText(mode.AgentInstructions, neoBrokerPluginAgentModeInstructionsLimit, false) {
			return fmt.Errorf("pluginAgentModes entry %q is invalid", mode.Key)
		}
		if mode.PluginScope != neoPluginAgentModeUserScope {
			return fmt.Errorf("pluginAgentModes entry %q has an invalid scope", mode.Key)
		}
		if !neoLocalBrokerSafeText(mode.PluginPath, neoBrokerPluginAgentModeTextLimit, false) || !strings.HasPrefix(mode.PluginPath, "~/") {
			return fmt.Errorf("pluginAgentModes entry %q has an invalid path", mode.Key)
		}
		if mode.ReasoningEffort != "" && ampplugins.NormalizeReasoningEffort(mode.ReasoningEffort) != mode.ReasoningEffort {
			return fmt.Errorf("pluginAgentModes entry %q has an invalid reasoning effort", mode.Key)
		}
		if mode.AgentTools != nil {
			normalized, ok := normalizeNeoCustomAgentToolSelector(mode.AgentTools)
			if !ok {
				return fmt.Errorf("pluginAgentModes entry %q has an invalid tool selector", mode.Key)
			}
			mode.AgentTools = normalized
			encoded, err := json.Marshal(mode.AgentTools)
			if err != nil || len(encoded) > neoBrokerPluginAgentModeToolsLimit {
				return fmt.Errorf("pluginAgentModes entry %q has an oversized tool selector", mode.Key)
			}
		}
	}
	return nil
}
