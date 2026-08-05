package util

import (
	"context"
	"testing"
)

func TestLocalNeoInferenceCapabilityRejectsPublicMarker(t *testing.T) {
	capability := LocalNeoInferenceCapability()
	if capability == "" || capability == "1" {
		t.Fatal("local Neo inference capability was not generated")
	}
	if !ValidLocalNeoInferenceCapability(capability) {
		t.Fatal("generated local Neo inference capability was rejected")
	}
	if MaskSensitiveHeaderValue(LocalNeoInferenceTokenHeaderName, capability) == capability {
		t.Fatal("local Neo inference capability was not masked for request logging")
	}
	for _, forged := range []string{"", "1", capability + "x"} {
		if ValidLocalNeoInferenceCapability(forged) {
			t.Fatal("forged local Neo inference capability was accepted")
		}
	}
}

func TestLocalNeoInferenceTrustUsesTypedContext(t *testing.T) {
	if IsTrustedLocalNeoInference(context.Background()) {
		t.Fatal("background context must not be trusted")
	}
	if IsTrustedLocalNeoInference(nil) {
		t.Fatal("nil context must not be trusted")
	}
	ctx := WithTrustedLocalNeoInference(context.Background())
	if !IsTrustedLocalNeoInference(ctx) {
		t.Fatal("trusted local Neo context was not recognized")
	}
}
