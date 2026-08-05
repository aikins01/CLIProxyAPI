package executor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/runtime/geminicli"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	"golang.org/x/oauth2"
)

func TestGeminiCLITokenMetadataUsesManagerUpdater(t *testing.T) {
	auth := &cliproxyauth.Auth{
		ID:       "auth-1",
		Provider: "gemini-cli",
		Metadata: map[string]any{"access_token": "original"},
	}
	executor := NewGeminiCLIExecutor(nil)
	var calls int
	executor.SetAuthMetadataUpdater(func(_ context.Context, expected *cliproxyauth.Auth, updates map[string]any) (*cliproxyauth.Auth, error) {
		calls++
		if expected != auth {
			t.Fatalf("expected auth = %p, want %p", expected, auth)
		}
		updated := expected.Clone()
		for key, value := range updates {
			updated.Metadata[key] = value
		}
		return updated, nil
	})
	token := &oauth2.Token{
		AccessToken:  "rotated",
		RefreshToken: "refresh",
		TokenType:    "Bearer",
		Expiry:       time.Now().Add(time.Hour),
	}

	if err := executor.updateGeminiCLITokenMetadata(t.Context(), auth, nil, token); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("metadata updater calls = %d, want 1", calls)
	}
	if auth.Metadata["access_token"] != "original" {
		t.Fatalf("execution snapshot was mutated: %#v", auth.Metadata)
	}
}

func TestGeminiCLITokenMetadataDoesNotPublishRejectedSharedState(t *testing.T) {
	shared := geminicli.NewSharedCredential("auth-parent", "", map[string]any{"access_token": "original"}, []string{"project-1"})
	auth := &cliproxyauth.Auth{
		ID:       "auth-virtual",
		Provider: "gemini-cli",
		Runtime:  geminicli.NewVirtualCredential("project-1", shared),
	}
	executor := NewGeminiCLIExecutor(nil)
	wantErr := errors.New("persist failed")
	executor.SetAuthMetadataUpdater(func(context.Context, *cliproxyauth.Auth, map[string]any) (*cliproxyauth.Auth, error) {
		return nil, wantErr
	})
	token := &oauth2.Token{AccessToken: "rotated", RefreshToken: "refresh", TokenType: "Bearer", Expiry: time.Now().Add(time.Hour)}

	if err := executor.updateGeminiCLITokenMetadata(t.Context(), auth, nil, token); !errors.Is(err, wantErr) {
		t.Fatalf("metadata update error = %v, want %v", err, wantErr)
	}
	if snapshot := shared.MetadataSnapshot(); snapshot["access_token"] != "original" {
		t.Fatalf("rejected shared metadata was published: %#v", snapshot)
	}
}

func TestGeminiCLITokenMetadataRejectsMissingCommittedSharedState(t *testing.T) {
	shared := geminicli.NewSharedCredential("auth-parent", "", map[string]any{"access_token": "original"}, []string{"project-1"})
	auth := &cliproxyauth.Auth{
		ID:       "auth-virtual",
		Provider: "gemini-cli",
		Runtime:  geminicli.NewVirtualCredential("project-1", shared),
	}
	executor := NewGeminiCLIExecutor(nil)
	executor.SetAuthMetadataUpdater(func(context.Context, *cliproxyauth.Auth, map[string]any) (*cliproxyauth.Auth, error) {
		return nil, nil
	})
	token := &oauth2.Token{AccessToken: "rotated", RefreshToken: "refresh", TokenType: "Bearer", Expiry: time.Now().Add(time.Hour)}

	if err := executor.updateGeminiCLITokenMetadata(t.Context(), auth, nil, token); err == nil {
		t.Fatal("metadata update succeeded without committed state")
	}
	if snapshot := shared.MetadataSnapshot(); snapshot["access_token"] != "original" {
		t.Fatalf("missing committed metadata was published: %#v", snapshot)
	}
}
