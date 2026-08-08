package wsrelay

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestDecodeErrorPreservesStatus(t *testing.T) {
	err := decodeError(map[string]any{"error": "invalid api key", "status": float64(http.StatusUnauthorized)})
	var statusError interface{ StatusCode() int }
	if !errors.As(err, &statusError) {
		t.Fatalf("decodeError() does not expose status: %v", err)
	}
	if statusError.StatusCode() != http.StatusUnauthorized {
		t.Fatalf("StatusCode() = %d, want %d", statusError.StatusCode(), http.StatusUnauthorized)
	}
	if err.Error() != "invalid api key (status=401)" {
		t.Fatalf("Error() = %q", err.Error())
	}
}

func TestNonStreamRejectsRelayCloseBeforeTerminalEvent(t *testing.T) {
	connected := make(chan struct{}, 1)
	manager := NewManager(Options{
		ProviderFactory: func(*http.Request) (string, error) { return "test-provider", nil },
		OnConnected:     func(string) { connected <- struct{}{} },
	})
	server := httptest.NewServer(manager.Handler())
	defer server.Close()
	t.Cleanup(func() { _ = manager.Stop(context.Background()) })

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + manager.Path()
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial relay: %v", err)
	}
	defer func() { _ = conn.Close() }()
	select {
	case <-connected:
	case <-time.After(5 * time.Second):
		t.Fatal("relay provider did not connect")
	}

	clientDone := make(chan error, 1)
	go func() {
		var request Message
		if errRead := conn.ReadJSON(&request); errRead != nil {
			clientDone <- errRead
			return
		}
		if errWrite := conn.WriteJSON(Message{ID: request.ID, Type: MessageTypeStreamStart, Payload: map[string]any{"status": http.StatusOK}}); errWrite != nil {
			clientDone <- errWrite
			return
		}
		if errWrite := conn.WriteJSON(Message{ID: request.ID, Type: MessageTypeStreamChunk, Payload: map[string]any{"data": "partial"}}); errWrite != nil {
			clientDone <- errWrite
			return
		}
		clientDone <- conn.Close()
	}()

	_, err = manager.NonStream(t.Context(), "test-provider", &HTTPRequest{Method: http.MethodPost, URL: "https://example.com"})
	if err == nil {
		t.Fatalf("NonStream() error = %v, want incomplete relay response failure", err)
	}
	if errClient := <-clientDone; errClient != nil {
		t.Fatalf("relay client error: %v", errClient)
	}
}
