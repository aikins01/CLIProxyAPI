package configaccess

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestProviderAuthenticateAcceptsRivetToken(t *testing.T) {
	provider := newProvider("test", []string{"local-key"})
	tests := []struct {
		name       string
		configure  func(*http.Request)
		wantSource string
	}{
		{
			name: "query",
			configure: func(req *http.Request) {
				req.URL.RawQuery = "rvt-token=local-key"
			},
			wantSource: "query-rivet-token",
		},
		{
			name: "header",
			configure: func(req *http.Request) {
				req.Header.Set("X-Rivet-Token", "local-key")
			},
			wantSource: "x-rivet-token",
		},
		{
			name: "websocket subprotocol",
			configure: func(req *http.Request) {
				req.Header.Set("Sec-WebSocket-Protocol", "rivet, rivet_token.local-key, rivet_encoding.json")
			},
			wantSource: "subprotocol-rivet-token",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/gateway/threadActor", nil)
			tt.configure(req)

			result, authErr := provider.Authenticate(context.Background(), req)
			if authErr != nil {
				t.Fatalf("Authenticate returned error: %v", authErr)
			}
			if result == nil {
				t.Fatalf("Authenticate returned nil result")
			}
			if result.Principal != "local-key" {
				t.Fatalf("Principal = %q, want local-key", result.Principal)
			}
			if result.Metadata["source"] != tt.wantSource {
				t.Fatalf("source = %q, want %q", result.Metadata["source"], tt.wantSource)
			}
		})
	}
}
