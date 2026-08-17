package ampplugins

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDiscoverFindsAndStampsPluginAgentModes(t *testing.T) {
	dir := t.TempDir()
	plugin := `// @amp-agent-mode {"key":"deep-blue","label":"Deep Blue","description":"research mode"}
const agent = amp.createAgent({
  name: "deep-blue",
  model: "openai/gpt-5",
  instructions: "You are deep blue.",
  reasoningEffort: "high",
})
amp.registerAgentMode({ key: "deep-blue", label: "Deep Blue", agent })
`
	if err := os.WriteFile(filepath.Join(dir, "deep-blue.ts"), []byte(plugin), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("not a plugin"), 0o600); err != nil {
		t.Fatal(err)
	}
	modes, err := Discover(dir, nil)
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(modes) != 1 {
		t.Fatalf("discovered modes = %#v", modes)
	}
	mode := modes[0]
	if mode.Key != "deep-blue" || mode.Label != "Deep Blue" || mode.Description != "research mode" {
		t.Fatalf("mode header = %#v", mode)
	}
	if mode.AgentName != "deep-blue" || mode.AgentModel != "openai/gpt-5" || mode.AgentInstructions != "You are deep blue." || mode.ReasoningEffort != "high" {
		t.Fatalf("mode definition = %#v", mode)
	}
	if mode.PluginName != "deep-blue" || mode.PluginRepositoryName != UserRepositoryName || mode.PluginScope != UserScope {
		t.Fatalf("mode plugin metadata = %#v", mode)
	}
	definition := mode.AgentDefinition()
	if definition["kind"] != "agent-definition" || definition["agentMode"] != "deep-blue" || definition["model"] != "openai/gpt-5" {
		t.Fatalf("mode agent definition = %#v", definition)
	}
	metadata := mode.Metadata()
	if metadata["key"] != "deep-blue" || metadata["pluginScope"] != UserScope {
		t.Fatalf("mode metadata = %#v", metadata)
	}
}

func TestDiscoverHonorsReservedKeys(t *testing.T) {
	dir := t.TempDir()
	plugin := `// @amp-agent-mode {"key":"high","label":"High"}
const agent = amp.createAgent({
  name: "high",
  model: "openai/gpt-5",
  instructions: "You are high.",
})
amp.registerAgentMode({ key: "high", label: "High", agent })
`
	if err := os.WriteFile(filepath.Join(dir, "high.ts"), []byte(plugin), 0o600); err != nil {
		t.Fatal(err)
	}
	reserved := func(key string) bool { return key == "high" }
	if modes, err := Discover(dir, reserved); err != nil || len(modes) != 0 {
		t.Fatalf("Discover with reserved key = %#v, %v", modes, err)
	}
	modes, err := Discover(dir, nil)
	if err != nil || len(modes) != 1 {
		t.Fatalf("Discover without reserved callback = %#v, %v", modes, err)
	}
}

func TestDiscoverMissingDirectory(t *testing.T) {
	modes, err := Discover(filepath.Join(t.TempDir(), "missing"), nil)
	if err != nil || modes != nil {
		t.Fatalf("Discover missing directory = %#v, %v", modes, err)
	}
	if modes, err := Discover("", nil); err != nil || modes != nil {
		t.Fatalf("Discover empty directory = %#v, %v", modes, err)
	}
}
