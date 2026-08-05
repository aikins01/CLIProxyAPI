package util

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"strings"
	"sync"
)

const (
	LocalNeoInferenceHeaderName      = "X-CLIProxyAPI-Local-Neo-Inference"
	LocalNeoInferenceTokenHeaderName = "X-CLIProxyAPI-Local-Neo-Token"
)

var localNeoInferenceCapability = sync.OnceValue(func() string {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(raw)
})

type localNeoInferenceContextKey struct{}

func LocalNeoInferenceCapability() string {
	return localNeoInferenceCapability()
}

func ValidLocalNeoInferenceCapability(value string) bool {
	expected := LocalNeoInferenceCapability()
	value = strings.TrimSpace(value)
	if expected == "" || len(value) != len(expected) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(value), []byte(expected)) == 1
}

func WithTrustedLocalNeoInference(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, localNeoInferenceContextKey{}, true)
}

func IsTrustedLocalNeoInference(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	trusted, _ := ctx.Value(localNeoInferenceContextKey{}).(bool)
	return trusted
}
