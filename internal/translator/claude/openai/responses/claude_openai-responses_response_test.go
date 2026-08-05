package responses

import (
	"context"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func TestConvertClaudeResponseToOpenAIResponsesFramesCompleteEvents(t *testing.T) {
	var param any
	chunks := ConvertClaudeResponseToOpenAIResponses(
		context.Background(),
		"claude-test",
		nil,
		nil,
		[]byte(`data: {"type":"message_start","message":{"id":"msg-1","usage":{"input_tokens":1,"output_tokens":0}}}`),
		&param,
	)
	if len(chunks) != 2 {
		t.Fatalf("chunk count = %d, want 2", len(chunks))
	}
	for _, chunk := range chunks {
		frame := strings.TrimSuffix(string(chunk), "\n\n")
		if frame == string(chunk) {
			t.Fatalf("chunk is not a complete SSE event: %q", chunk)
		}
		lines := strings.Split(frame, "\n")
		if len(lines) != 2 || !strings.HasPrefix(lines[0], "event: ") || !strings.HasPrefix(lines[1], "data: ") {
			t.Fatalf("invalid SSE event framing: %q", chunk)
		}
		if payload := strings.TrimPrefix(lines[1], "data: "); !gjson.Valid(payload) {
			t.Fatalf("invalid SSE event JSON: %q", payload)
		}
	}
}
