package executor

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	log "github.com/sirupsen/logrus"
)

const (
	codexBenchmarkHandshakeDelay = 2 * time.Millisecond
	codexBenchmarkTurnDelay      = 500 * time.Microsecond
)

func BenchmarkCodexResponseTransports(b *testing.B) {
	var connections atomic.Int64
	var httpRequests atomic.Int64
	var wsHandshakes atomic.Int64
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if websocket.IsWebSocketUpgrade(r) {
			wsHandshakes.Add(1)
			time.Sleep(codexBenchmarkHandshakeDelay)
			conn, errUpgrade := upgrader.Upgrade(w, r, nil)
			if errUpgrade != nil {
				return
			}
			defer func() { _ = conn.Close() }()
			for {
				if _, _, errRead := conn.ReadMessage(); errRead != nil {
					return
				}
				time.Sleep(codexBenchmarkTurnDelay)
				if errWrite := conn.WriteMessage(websocket.TextMessage, codexWebsocketTestCompleted); errWrite != nil {
					return
				}
			}
		}

		httpRequests.Add(1)
		time.Sleep(codexBenchmarkTurnDelay)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: " + string(codexWebsocketTestCompleted) + "\n\n"))
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
	}
	server.Start()
	defer server.Close()

	previousLogLevel := log.GetLevel()
	log.SetLevel(log.ErrorLevel)
	defer log.SetLevel(previousLogLevel)

	cfg := &config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}}
	auth := &cliproxyauth.Auth{
		ID:       "benchmark-auth",
		Provider: "codex",
		Attributes: map[string]string{
			"api_key":  "sk-test",
			"base_url": server.URL,
		},
	}
	req := cliproxyexecutor.Request{
		Model:   "gpt-5-codex",
		Payload: []byte(`{"model":"gpt-5-codex","input":[]}`),
	}
	baseOptions := cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("codex")}

	b.Run("codex_http", func(b *testing.B) {
		exec := NewCodexExecutor(cfg)
		benchmarkCodexResponseTransport(b, &connections, &httpRequests, &wsHandshakes, func() (*cliproxyexecutor.StreamResult, error) {
			return exec.ExecuteStream(context.Background(), auth, req, baseOptions)
		})
	})

	b.Run("codex_websocket_without_reuse", func(b *testing.B) {
		exec := newIsolatedCodexWebsocketsExecutor()
		benchmarkCodexResponseTransport(b, &connections, &httpRequests, &wsHandshakes, func() (*cliproxyexecutor.StreamResult, error) {
			return exec.ExecuteStream(context.Background(), auth, req, baseOptions)
		})
	})

	b.Run("codex_websocket_with_reuse", func(b *testing.B) {
		exec := newIsolatedCodexWebsocketsExecutor()
		sessionID := "benchmark-session"
		options := baseOptions
		options.Metadata = map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: sessionID}
		b.Cleanup(func() { exec.CloseExecutionSession(sessionID) })
		benchmarkCodexResponseTransport(b, &connections, &httpRequests, &wsHandshakes, func() (*cliproxyexecutor.StreamResult, error) {
			return exec.ExecuteStream(context.Background(), auth, req, options)
		})
	})
}

func benchmarkCodexResponseTransport(
	b *testing.B,
	connections *atomic.Int64,
	httpRequests *atomic.Int64,
	wsHandshakes *atomic.Int64,
	execute func() (*cliproxyexecutor.StreamResult, error),
) {
	b.Helper()
	connectionsBefore := connections.Load()
	httpRequestsBefore := httpRequests.Load()
	wsHandshakesBefore := wsHandshakes.Load()
	var firstChunkTotal time.Duration

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		started := time.Now()
		result, err := execute()
		if err != nil {
			b.Fatalf("ExecuteStream() error = %v", err)
		}
		firstChunk := true
		for chunk := range result.Chunks {
			if firstChunk {
				firstChunkTotal += time.Since(started)
				firstChunk = false
			}
			if chunk.Err != nil {
				b.Fatalf("stream error = %v", chunk.Err)
			}
		}
		if firstChunk {
			b.Fatal("stream completed without a chunk")
		}
	}
	b.StopTimer()

	operations := float64(b.N)
	b.ReportMetric(float64(firstChunkTotal.Nanoseconds())/operations, "first-chunk-ns/op")
	b.ReportMetric(float64(connections.Load()-connectionsBefore)/operations, "connections/op")
	b.ReportMetric(float64(httpRequests.Load()-httpRequestsBefore)/operations, "http-requests/op")
	b.ReportMetric(float64(wsHandshakes.Load()-wsHandshakesBefore)/operations, "ws-handshakes/op")
}
