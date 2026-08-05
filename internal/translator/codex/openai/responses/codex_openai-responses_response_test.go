package responses

import (
	"context"
	"testing"
)

func TestConvertCodexResponseToOpenAIResponsesPreservesFramingWithoutInferringBoundary(t *testing.T) {
	var param any
	input := []byte("data: {\"type\":\"response.created\"}\n\n")
	got := ConvertCodexResponseToOpenAIResponses(context.Background(), "model", nil, nil, input, &param)
	if len(got) != 1 {
		t.Fatalf("chunk count = %d, want 1", len(got))
	}
	if string(got[0]) != string(input) {
		t.Fatalf("chunk = %q, want %q", got[0], input)
	}

	unframed := []byte(`data: {"type":"response.created"}`)
	got = ConvertCodexResponseToOpenAIResponses(context.Background(), "model", nil, nil, unframed, &param)
	if len(got) != 1 || string(got[0]) != string(unframed) {
		t.Fatalf("translator inferred an event boundary: %q", got)
	}
}
