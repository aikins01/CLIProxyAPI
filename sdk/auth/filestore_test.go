package auth

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestFileTokenStore_ListSkipsOperationalLogs(t *testing.T) {
	t.Parallel()

	baseDir := t.TempDir()
	store := NewFileTokenStore()
	store.SetBaseDir(baseDir)

	if err := os.WriteFile(filepath.Join(baseDir, "codex.json"), []byte(`{"type":"codex","email":"user@example.com"}`), 0o600); err != nil {
		t.Fatalf("seed auth file: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(baseDir, "nested"), 0o700); err != nil {
		t.Fatalf("create nested auth dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(baseDir, "nested", "claude.json"), []byte(`{"type":"claude","email":"claude@example.com"}`), 0o600); err != nil {
		t.Fatalf("seed nested auth file: %v", err)
	}
	cacheAuthDir := filepath.Join(baseDir, "nested", "cache")
	if err := os.MkdirAll(cacheAuthDir, 0o700); err != nil {
		t.Fatalf("create nested cache auth dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(cacheAuthDir, "openai.json"), []byte(`{"type":"openai","email":"openai@example.com"}`), 0o600); err != nil {
		t.Fatalf("seed nested cache auth file: %v", err)
	}
	captureDir := filepath.Join(baseDir, "logs", "neo-provider-requests")
	if err := os.MkdirAll(captureDir, 0o700); err != nil {
		t.Fatalf("create capture dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(captureDir, "neo-provider-request-20260601T184002Z.json"), []byte(`{"request":{"model":"claude"}}`), 0o600); err != nil {
		t.Fatalf("seed capture file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(baseDir, "neo-provider-request-20260601T184003Z.json"), []byte(`{"request":{"model":"claude"}}`), 0o600); err != nil {
		t.Fatalf("seed misplaced capture file: %v", err)
	}

	auths, err := store.List(context.Background())
	if err != nil {
		t.Fatalf("List() error: %v", err)
	}
	if len(auths) != 3 {
		t.Fatalf("List() returned %d auths, want 3: %#v", len(auths), auths)
	}
	seen := make(map[string]string, len(auths))
	for _, auth := range auths {
		seen[auth.ID] = auth.Provider
	}
	if seen["codex.json"] != "codex" {
		t.Fatalf("codex auth missing or wrong provider: %#v", seen)
	}
	if seen[filepath.Join("nested", "claude.json")] != "claude" {
		t.Fatalf("nested claude auth missing or wrong provider: %#v", seen)
	}
	if seen[filepath.Join("nested", "cache", "openai.json")] != "openai" {
		t.Fatalf("nested cache auth missing or wrong provider: %#v", seen)
	}
}

func TestExtractAccessToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		metadata map[string]any
		expected string
	}{
		{
			"antigravity top-level access_token",
			map[string]any{"access_token": "tok-abc"},
			"tok-abc",
		},
		{
			"gemini nested token.access_token",
			map[string]any{
				"token": map[string]any{"access_token": "tok-nested"},
			},
			"tok-nested",
		},
		{
			"top-level takes precedence over nested",
			map[string]any{
				"access_token": "tok-top",
				"token":        map[string]any{"access_token": "tok-nested"},
			},
			"tok-top",
		},
		{
			"empty metadata",
			map[string]any{},
			"",
		},
		{
			"whitespace-only access_token",
			map[string]any{"access_token": "   "},
			"",
		},
		{
			"wrong type access_token",
			map[string]any{"access_token": 12345},
			"",
		},
		{
			"token is not a map",
			map[string]any{"token": "not-a-map"},
			"",
		},
		{
			"nested whitespace-only",
			map[string]any{
				"token": map[string]any{"access_token": "  "},
			},
			"",
		},
		{
			"fallback to nested when top-level empty",
			map[string]any{
				"access_token": "",
				"token":        map[string]any{"access_token": "tok-fallback"},
			},
			"tok-fallback",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := extractAccessToken(tt.metadata)
			if got != tt.expected {
				t.Errorf("extractAccessToken() = %q, want %q", got, tt.expected)
			}
		})
	}
}
