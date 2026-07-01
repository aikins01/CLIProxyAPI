package amp

import (
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
)

func TestAmpClientVersionFromOutput(t *testing.T) {
	output := "0.0.1780359918-g778c3a (released 2026-06-02T00:25:18.000Z, 34m ago)"
	if got := ampClientVersionFromOutput(output); got != "0.0.1780359918-g778c3a" {
		t.Fatalf("version = %q", got)
	}
}

func TestAmpUpstreamClientVersionProviderExplicit(t *testing.T) {
	provider := ampUpstreamClientVersionProvider(&config.AmpCode{
		UpstreamClientVersionOverride: " 0.0.1780359918-g778c3a ",
	})
	if provider == nil {
		t.Fatal("provider is nil")
	}
	if got := provider(); got != "0.0.1780359918-g778c3a" {
		t.Fatalf("version = %q", got)
	}
}

func TestAmpUpstreamClientVersionProviderEmpty(t *testing.T) {
	if provider := ampUpstreamClientVersionProvider(&config.AmpCode{}); provider != nil {
		t.Fatal("provider should be nil for empty override")
	}
}
