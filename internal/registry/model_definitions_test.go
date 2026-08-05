package registry

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestCodexFreeModelsIncludeGPT55(t *testing.T) {
	model := findModelInfo(GetCodexFreeModels(), "gpt-5.5")
	if model == nil {
		t.Fatal("expected codex free tier to include gpt-5.5")
	}
	assertGPT55ModelInfo(t, "free", model)
}

func TestCodexStaticModelsIncludeGPT55(t *testing.T) {
	tierModels := map[string][]*ModelInfo{
		"team": GetCodexTeamModels(),
		"plus": GetCodexPlusModels(),
		"pro":  GetCodexProModels(),
	}

	for tier, models := range tierModels {
		t.Run(tier, func(t *testing.T) {
			model := findModelInfo(models, "gpt-5.5")
			if model == nil {
				t.Fatalf("expected codex %s tier to include gpt-5.5", tier)
			}
			assertGPT55ModelInfo(t, tier, model)
		})
	}

	model := LookupStaticModelInfo("gpt-5.5")
	if model == nil {
		t.Fatal("expected LookupStaticModelInfo to find gpt-5.5")
	}
	assertGPT55ModelInfo(t, "lookup", model)
}

func TestChatGPTWebFallbackUsesProviderQualifiedID(t *testing.T) {
	models := GetChatGPTWebModels()
	if len(models) != 1 || models[0].ID != "chatgpt-web/gpt-5-6-pro" {
		t.Fatalf("ChatGPT Web fallback models = %#v", models)
	}
	model := LookupStaticModelInfo("chatgpt-web/gpt-5-6-pro")
	if model == nil || model.ID != "chatgpt-web/gpt-5-6-pro" || model.ContextLength != 1048576 || model.MaxCompletionTokens != 128000 {
		t.Fatalf("ChatGPT Web static lookup = %#v", model)
	}
	if model := LookupStaticModelInfo("gpt-5-6-pro"); model != nil && model.Type == "chatgpt-web" {
		t.Fatalf("bare model lookup was classified as ChatGPT Web: %#v", model)
	}
}

func findModelInfo(models []*ModelInfo, id string) *ModelInfo {
	for _, model := range models {
		if model != nil && model.ID == id {
			return model
		}
	}
	return nil
}

func assertGPT55ModelInfo(t *testing.T, source string, model *ModelInfo) {
	t.Helper()

	if model.ID != "gpt-5.5" {
		t.Fatalf("%s id mismatch: got %q", source, model.ID)
	}
	if model.Object != "model" {
		t.Fatalf("%s object mismatch: got %q", source, model.Object)
	}
	if model.Created != 1776902400 {
		t.Fatalf("%s created timestamp mismatch: got %d", source, model.Created)
	}
	if model.OwnedBy != "openai" {
		t.Fatalf("%s owned_by mismatch: got %q", source, model.OwnedBy)
	}
	if model.Type != "openai" {
		t.Fatalf("%s type mismatch: got %q", source, model.Type)
	}
	if model.DisplayName != "GPT 5.5" {
		t.Fatalf("%s display name mismatch: got %q", source, model.DisplayName)
	}
	if model.Version != "gpt-5.5" {
		t.Fatalf("%s version mismatch: got %q", source, model.Version)
	}
	if model.Description != "Frontier model for complex coding, research, and real-world work." {
		t.Fatalf("%s description mismatch: got %q", source, model.Description)
	}
	if model.ContextLength != 400000 {
		t.Fatalf("%s context length mismatch: got %d", source, model.ContextLength)
	}
	if model.MaxCompletionTokens != 128000 {
		t.Fatalf("%s max completion tokens mismatch: got %d", source, model.MaxCompletionTokens)
	}
	if len(model.SupportedParameters) != 1 || model.SupportedParameters[0] != "tools" {
		t.Fatalf("%s supported parameters mismatch: got %v", source, model.SupportedParameters)
	}
	if model.Thinking == nil {
		t.Fatalf("%s missing thinking support", source)
	}

	want := []string{"low", "medium", "high", "xhigh"}
	if len(model.Thinking.Levels) != len(want) {
		t.Fatalf("%s thinking level count mismatch: got %d, want %d", source, len(model.Thinking.Levels), len(want))
	}
	for i, level := range want {
		if model.Thinking.Levels[i] != level {
			t.Fatalf("%s thinking level %d mismatch: got %q, want %q", source, i, model.Thinking.Levels[i], level)
		}
	}
}

func TestStaticModelDefinitionsIncludeOverrides(t *testing.T) {
	claudeModels := GetClaudeModels()
	for _, tc := range []struct {
		id      string
		context int
		maxOut  int
	}{
		{id: "claude-sonnet-4-6", context: 1000000, maxOut: 64000},
		{id: "claude-opus-4-6", context: 332000, maxOut: 32000},
		{id: "claude-opus-4-7", context: 332000, maxOut: 32000},
		{id: "claude-opus-4-8", context: 332000, maxOut: 32000},
		{id: "claude-opus-5", context: 1000000, maxOut: 128000},
	} {
		t.Run(tc.id, func(t *testing.T) {
			model := findModelInfo(claudeModels, tc.id)
			if model == nil {
				t.Fatalf("expected claude model %s", tc.id)
			}
			assertModelLimits(t, model, tc.context, tc.maxOut)
		})
	}

	codexProModels := GetCodexProModels()
	for _, tc := range []struct {
		id      string
		context int
		maxOut  int
	}{
		{id: "gpt-5.4", context: 400000, maxOut: 128000},
		{id: "gpt-5.4-pro", context: 1050000, maxOut: 128000},
		{id: "gpt-5.5", context: 400000, maxOut: 128000},
		{id: "gpt-5.5-pro", context: 1050000, maxOut: 128000},
	} {
		t.Run(tc.id, func(t *testing.T) {
			model := findModelInfo(codexProModels, tc.id)
			if model == nil {
				t.Fatalf("expected codex pro model %s", tc.id)
			}
			assertModelLimits(t, model, tc.context, tc.maxOut)
		})
	}
}

func TestLoadModelsFromBytesAppliesModelOverrides(t *testing.T) {
	oldModels := getModels()
	t.Cleanup(func() {
		modelsCatalogStore.mu.Lock()
		modelsCatalogStore.data = oldModels
		modelsCatalogStore.mu.Unlock()
	})

	dummyModels := []*ModelInfo{{ID: "dummy"}}
	staleClaude := func(id string) *ModelInfo {
		return &ModelInfo{ID: id, ContextLength: 1000000, MaxCompletionTokens: 128000}
	}
	staleGPT := func(id string, context int) *ModelInfo {
		return &ModelInfo{
			ID:                  id,
			Object:              "model",
			OwnedBy:             "openai",
			Type:                "openai",
			DisplayName:         id,
			Version:             id,
			ContextLength:       context,
			MaxCompletionTokens: 128000,
			SupportedParameters: []string{"tools"},
			Thinking:            &ThinkingSupport{Levels: []string{"low", "medium", "high", "xhigh"}},
		}
	}
	catalog := staticModelsJSON{
		Claude: []*ModelInfo{
			staleClaude("claude-sonnet-4-6"),
			staleClaude("claude-opus-4-6"),
			staleClaude("claude-opus-4-7"),
			staleClaude("claude-opus-4-8"),
		},
		Gemini:      dummyModels,
		Vertex:      dummyModels,
		GeminiCLI:   dummyModels,
		AIStudio:    dummyModels,
		CodexFree:   []*ModelInfo{staleGPT("gpt-5.5", 272000)},
		CodexTeam:   []*ModelInfo{staleGPT("gpt-5.4", 1050000), staleGPT("gpt-5.5", 272000)},
		CodexPlus:   []*ModelInfo{staleGPT("gpt-5.4", 1050000), staleGPT("gpt-5.5", 272000)},
		CodexPro:    []*ModelInfo{staleGPT("gpt-5.4", 1050000), staleGPT("gpt-5.5", 272000)},
		Kimi:        dummyModels,
		Antigravity: dummyModels,
	}
	raw, err := json.Marshal(catalog)
	if err != nil {
		t.Fatalf("marshal catalog: %v", err)
	}
	if err := loadModelsFromBytes(raw, "test"); err != nil {
		t.Fatalf("loadModelsFromBytes error: %v", err)
	}

	assertModelLimits(t, findModelInfo(GetClaudeModels(), "claude-opus-4-8"), 332000, 32000)
	opus5 := findModelInfo(GetClaudeModels(), "claude-opus-5")
	assertModelLimits(t, opus5, 1000000, 128000)
	if opus5.ID != "claude-opus-5" || opus5.Object != "model" || opus5.OwnedBy != "anthropic" || opus5.Type != "claude" {
		t.Fatalf("claude-opus-5 identity metadata mismatch: %#v", opus5)
	}
	if opus5.Thinking == nil {
		t.Fatal("claude-opus-5 missing thinking support")
	}
	if opus5.Thinking.Min != 0 || opus5.Thinking.Max != 0 {
		t.Fatalf("claude-opus-5 should be adaptive-only, got thinking range [%d,%d]", opus5.Thinking.Min, opus5.Thinking.Max)
	}
	wantLevels := []string{"low", "medium", "high", "xhigh", "max"}
	if len(opus5.Thinking.Levels) != len(wantLevels) {
		t.Fatalf("claude-opus-5 thinking levels = %v, want %v", opus5.Thinking.Levels, wantLevels)
	}
	for i, level := range wantLevels {
		if opus5.Thinking.Levels[i] != level {
			t.Fatalf("claude-opus-5 thinking level %d = %q, want %q", i, opus5.Thinking.Levels[i], level)
		}
	}
	assertModelLimits(t, findModelInfo(GetCodexFreeModels(), "gpt-5.5"), 400000, 128000)
	assertModelLimits(t, findModelInfo(GetCodexProModels(), "gpt-5.4"), 400000, 128000)
	assertModelLimits(t, findModelInfo(GetCodexProModels(), "gpt-5.4-pro"), 1050000, 128000)
	assertModelLimits(t, findModelInfo(GetCodexProModels(), "gpt-5.5-pro"), 1050000, 128000)
	if model := findModelInfo(GetCodexPlusModels(), "gpt-5.5-pro"); model != nil {
		t.Fatalf("gpt-5.5-pro should only be added to codex pro models, got %#v", model)
	}

	remoteOpus5 := &ModelInfo{
		ID:                  " Claude-Opus-5 ",
		Object:              "model",
		Created:             1784851201,
		OwnedBy:             "anthropic",
		Type:                "claude",
		DisplayName:         "Remote Claude Opus 5",
		Description:         "Remote catalog description",
		ContextLength:       1000000,
		MaxCompletionTokens: 128000,
		Thinking:            &ThinkingSupport{Levels: wantLevels},
	}
	catalog.Claude = append(catalog.Claude, remoteOpus5)
	raw, err = json.Marshal(catalog)
	if err != nil {
		t.Fatalf("marshal remote catalog: %v", err)
	}
	if err := loadModelsFromBytes(raw, "remote-test"); err != nil {
		t.Fatalf("load remote catalog: %v", err)
	}
	remoteClaudeModels := GetClaudeModels()
	normalizedMatches := 0
	for _, model := range remoteClaudeModels {
		if model != nil && strings.EqualFold(strings.TrimSpace(model.ID), "claude-opus-5") {
			normalizedMatches++
		}
	}
	if normalizedMatches != 1 {
		t.Fatalf("claude-opus-5 normalized match count = %d, want 1", normalizedMatches)
	}
	gotRemoteOpus5 := findModelInfo(remoteClaudeModels, "claude-opus-5")
	if gotRemoteOpus5 == nil {
		t.Fatal("remote claude-opus-5 missing after load")
	}
	if gotRemoteOpus5.Created != remoteOpus5.Created || gotRemoteOpus5.DisplayName != remoteOpus5.DisplayName || gotRemoteOpus5.Description != remoteOpus5.Description || gotRemoteOpus5.ContextLength != remoteOpus5.ContextLength || gotRemoteOpus5.MaxCompletionTokens != remoteOpus5.MaxCompletionTokens {
		t.Fatalf("remote claude-opus-5 metadata was replaced: got %#v, want %#v", gotRemoteOpus5, remoteOpus5)
	}
	if len(gotRemoteOpus5.Thinking.Levels) != len(wantLevels) {
		t.Fatalf("remote claude-opus-5 thinking levels = %v, want %v", gotRemoteOpus5.Thinking.Levels, wantLevels)
	}
	for i, level := range wantLevels {
		if gotRemoteOpus5.Thinking.Levels[i] != level {
			t.Fatalf("remote claude-opus-5 thinking level %d = %q, want %q", i, gotRemoteOpus5.Thinking.Levels[i], level)
		}
	}

	catalog.Claude = append(catalog.Claude, &ModelInfo{ID: "CLAUDE-OPUS-5"})
	raw, err = json.Marshal(catalog)
	if err != nil {
		t.Fatalf("marshal duplicate remote catalog: %v", err)
	}
	if err := loadModelsFromBytes(raw, "duplicate-remote-test"); err == nil {
		t.Fatal("expected normalized duplicate claude-opus-5 IDs to be rejected")
	}

	catalog.Claude = catalog.Claude[:len(catalog.Claude)-1]
	remoteOpus5.ID = "claude-opuſ-5"
	raw, err = json.Marshal(catalog)
	if err != nil {
		t.Fatalf("marshal Unicode near-miss catalog: %v", err)
	}
	if err := loadModelsFromBytes(raw, "unicode-near-miss-test"); err != nil {
		t.Fatalf("load Unicode near-miss catalog: %v", err)
	}
	nearMissClaudeModels := GetClaudeModels()
	if findModelInfo(nearMissClaudeModels, "claude-opuſ-5") == nil {
		t.Fatal("Unicode near-miss remote model should remain distinct")
	}
	assertModelLimits(t, findModelInfo(nearMissClaudeModels, "claude-opus-5"), 1000000, 128000)
}

func assertModelLimits(t *testing.T, model *ModelInfo, context, maxOut int) {
	t.Helper()

	if model == nil {
		t.Fatal("model is nil")
	}
	if model.ContextLength != context {
		t.Fatalf("%s context length mismatch: got %d, want %d", model.ID, model.ContextLength, context)
	}
	if model.MaxCompletionTokens != maxOut {
		t.Fatalf("%s max completion tokens mismatch: got %d, want %d", model.ID, model.MaxCompletionTokens, maxOut)
	}
}
