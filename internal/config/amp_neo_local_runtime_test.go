package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestLoadConfigOptional_AmpCodexWebsocketsExperiment(t *testing.T) {
	tests := []struct {
		name       string
		configYAML string
		want       bool
	}{
		{name: "default off", configYAML: "ampcode: {}\n"},
		{name: "explicitly enabled", configYAML: "ampcode:\n  codex-websockets-experiment: true\n", want: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			configPath := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(configPath, []byte(tc.configYAML), 0o600); err != nil {
				t.Fatalf("failed to write config: %v", err)
			}

			cfg, err := LoadConfigOptional(configPath, false)
			if err != nil {
				t.Fatalf("LoadConfigOptional() error = %v", err)
			}
			if got := cfg.AmpCode.CodexWebsocketsExperiment; got != tc.want {
				t.Fatalf("CodexWebsocketsExperiment = %t, want %t", got, tc.want)
			}
		})
	}
}

func TestLoadConfigOptional_AmpNeoModeModelsAcceptsScalarAndSequence(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	configYAML := []byte(`
ampcode:
  neo-local-runtime:
    mode-models:
      smart: anthropic/claude-fable-5
      ultra:
        - anthropic/claude-fable-5
        - chatgpt-web/gpt-5-6-pro
        - openai/gpt-5.6-sol
`)
	if err := os.WriteFile(configPath, configYAML, 0o600); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}

	cfg, err := LoadConfigOptional(configPath, false)
	if err != nil {
		t.Fatalf("LoadConfigOptional() error = %v", err)
	}
	if got, want := cfg.AmpCode.NeoLocalRuntime.ModeModelRoutes["smart"], (ModelRouteList{"anthropic/claude-fable-5"}); !slices.Equal(got, want) {
		t.Fatalf("smart mode models = %#v, want %#v", got, want)
	}
	if got, want := cfg.AmpCode.NeoLocalRuntime.ModeModelRoutes["ultra"], (ModelRouteList{"anthropic/claude-fable-5", "chatgpt-web/gpt-5-6-pro", "openai/gpt-5.6-sol"}); !slices.Equal(got, want) {
		t.Fatalf("ultra mode models = %#v, want %#v", got, want)
	}
	if got := cfg.AmpCode.NeoLocalRuntime.ModeModels["smart"]; got != "anthropic/claude-fable-5" {
		t.Fatalf("legacy smart mode model = %q", got)
	}
}

func TestLoadConfigOptional_AmpNeoModeModelsRejectsNonStrings(t *testing.T) {
	tests := map[string]string{
		"scalar":   "smart: 123",
		"sequence": "smart: [anthropic/claude-fable-5, false]",
	}
	for name, modeModels := range tests {
		t.Run(name, func(t *testing.T) {
			configPath := filepath.Join(t.TempDir(), "config.yaml")
			configYAML := []byte("ampcode:\n  neo-local-runtime:\n    mode-models:\n      " + modeModels + "\n")
			if err := os.WriteFile(configPath, configYAML, 0o600); err != nil {
				t.Fatalf("failed to write config: %v", err)
			}
			if _, err := LoadConfigOptional(configPath, false); err == nil {
				t.Fatal("LoadConfigOptional() error = nil, want non-string model route rejection")
			}
		})
	}
}

func TestAmpNeoModeModelsJSONAcceptsScalarAndSequence(t *testing.T) {
	var runtime AmpNeoLocalRuntime
	if err := json.Unmarshal([]byte(`{"mode-models":{"smart":"anthropic/claude-fable-5","ultra":["chatgpt-web/gpt-5-6-pro","openai/gpt-5.6-sol"]}}`), &runtime); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if got, want := runtime.ModeModelRoutes["smart"], (ModelRouteList{"anthropic/claude-fable-5"}); !slices.Equal(got, want) {
		t.Fatalf("smart mode models = %#v, want %#v", got, want)
	}
	if got, want := runtime.ModeModelRoutes["ultra"], (ModelRouteList{"chatgpt-web/gpt-5-6-pro", "openai/gpt-5.6-sol"}); !slices.Equal(got, want) {
		t.Fatalf("ultra mode models = %#v, want %#v", got, want)
	}
}

func TestAmpNeoModeModelsLegacyGoField(t *testing.T) {
	runtime := AmpNeoLocalRuntime{ModeModels: map[string]string{"smart": "anthropic/claude-fable-5"}}
	if got, want := runtime.ModeRoutesFor("smart"), (ModelRouteList{"anthropic/claude-fable-5"}); !slices.Equal(got, want) {
		t.Fatalf("legacy smart mode routes = %#v, want %#v", got, want)
	}
	payload, err := json.Marshal(runtime)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(payload), `"mode-models":{"smart":["anthropic/claude-fable-5"]}`) {
		t.Fatalf("legacy mode models JSON = %s", payload)
	}
}

func TestAmpNeoModeModelsMatchModeCaseInsensitively(t *testing.T) {
	runtime := AmpNeoLocalRuntime{
		ModeModels:      map[string]string{"HIGH": "anthropic/legacy"},
		ModeModelRoutes: map[string]ModelRouteList{"Smart": {"openai/primary", "anthropic/fallback"}},
	}
	if got, want := runtime.ModeRoutesFor("smart"), (ModelRouteList{"openai/primary", "anthropic/fallback"}); !slices.Equal(got, want) {
		t.Fatalf("smart mode routes = %#v, want %#v", got, want)
	}
	if got, want := runtime.ModeRoutesFor("high"), (ModelRouteList{"anthropic/legacy"}); !slices.Equal(got, want) {
		t.Fatalf("high mode routes = %#v, want %#v", got, want)
	}
}

func TestLoadConfigOptional_AmpNeoSubagentModels(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	configYAML := []byte(`
ampcode:
  neo-local-runtime:
    subagent-models:
      oracle:
        - chatgpt-web/gpt-5-6-pro
        - openai/gpt-5.6-sol
        - anthropic/claude-fable-5
`)
	if err := os.WriteFile(configPath, configYAML, 0o600); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}

	cfg, err := LoadConfigOptional(configPath, false)
	if err != nil {
		t.Fatalf("LoadConfigOptional() error = %v", err)
	}

	want := []string{
		"chatgpt-web/gpt-5-6-pro",
		"openai/gpt-5.6-sol",
		"anthropic/claude-fable-5",
	}
	if got := cfg.AmpCode.NeoLocalRuntime.SubagentModels["oracle"]; !slices.Equal(got, want) {
		t.Fatalf("oracle subagent models = %#v, want %#v", got, want)
	}
}

func TestLoadConfigOptional_AmpNeoSubagentModelsRejectsScalar(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	configYAML := []byte(`
ampcode:
  neo-local-runtime:
    subagent-models:
      oracle: chatgpt-web/gpt-5-6-pro
`)
	if err := os.WriteFile(configPath, configYAML, 0o600); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}

	if _, err := LoadConfigOptional(configPath, false); err == nil {
		t.Fatal("LoadConfigOptional() error = nil, want scalar subagent model rejection")
	}
}
