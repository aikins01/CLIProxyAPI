package amp

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestRewriteModelInResponse_TopLevel(t *testing.T) {
	rw := &ResponseRewriter{originalModel: "gpt-5.2-codex"}

	input := []byte(`{"id":"resp_1","model":"gpt-5.3-codex","output":[]}`)
	result := rw.rewriteModelInResponse(input)

	expected := `{"id":"resp_1","model":"gpt-5.2-codex","output":[]}`
	if string(result) != expected {
		t.Errorf("expected %s, got %s", expected, string(result))
	}
}

func TestRewriteModelInResponse_ResponseModel(t *testing.T) {
	rw := &ResponseRewriter{originalModel: "gpt-5.2-codex"}

	input := []byte(`{"type":"response.completed","response":{"id":"resp_1","model":"gpt-5.3-codex","status":"completed"}}`)
	result := rw.rewriteModelInResponse(input)

	expected := `{"type":"response.completed","response":{"id":"resp_1","model":"gpt-5.2-codex","status":"completed"}}`
	if string(result) != expected {
		t.Errorf("expected %s, got %s", expected, string(result))
	}
}

func TestRewriteModelInResponse_ResponseCreated(t *testing.T) {
	rw := &ResponseRewriter{originalModel: "gpt-5.2-codex"}

	input := []byte(`{"type":"response.created","response":{"id":"resp_1","model":"gpt-5.3-codex","status":"in_progress"}}`)
	result := rw.rewriteModelInResponse(input)

	expected := `{"type":"response.created","response":{"id":"resp_1","model":"gpt-5.2-codex","status":"in_progress"}}`
	if string(result) != expected {
		t.Errorf("expected %s, got %s", expected, string(result))
	}
}

func TestRewriteModelInResponse_NoModelField(t *testing.T) {
	rw := &ResponseRewriter{originalModel: "gpt-5.2-codex"}

	input := []byte(`{"type":"response.output_item.added","item":{"id":"item_1","type":"message"}}`)
	result := rw.rewriteModelInResponse(input)

	if string(result) != string(input) {
		t.Errorf("expected no modification, got %s", string(result))
	}
}

func TestRewriteModelInResponse_EmptyOriginalModel(t *testing.T) {
	rw := &ResponseRewriter{originalModel: ""}

	input := []byte(`{"model":"gpt-5.3-codex"}`)
	result := rw.rewriteModelInResponse(input)

	if string(result) != string(input) {
		t.Errorf("expected no modification when originalModel is empty, got %s", string(result))
	}
}

func TestRewriteStreamChunk_SSEWithResponseModel(t *testing.T) {
	rw := &ResponseRewriter{originalModel: "gpt-5.2-codex"}

	chunk := []byte("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"model\":\"gpt-5.3-codex\",\"status\":\"completed\"}}\n\n")
	result := rw.rewriteStreamChunk(chunk)

	expected := "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"model\":\"gpt-5.2-codex\",\"status\":\"completed\"}}\n\n"
	if string(result) != expected {
		t.Errorf("expected %s, got %s", expected, string(result))
	}
}

func TestRewriteStreamChunk_MultipleEvents(t *testing.T) {
	rw := &ResponseRewriter{originalModel: "gpt-5.2-codex"}

	chunk := []byte("data: {\"type\":\"response.created\",\"response\":{\"model\":\"gpt-5.3-codex\"}}\n\ndata: {\"type\":\"response.output_item.added\",\"item\":{\"id\":\"item_1\"}}\n\n")
	result := rw.rewriteStreamChunk(chunk)

	if string(result) == string(chunk) {
		t.Error("expected response.model to be rewritten in SSE stream")
	}
	if !contains(result, []byte(`"model":"gpt-5.2-codex"`)) {
		t.Errorf("expected rewritten model in output, got %s", string(result))
	}
}

func TestRewriteStreamChunk_MessageModel(t *testing.T) {
	rw := &ResponseRewriter{originalModel: "claude-opus-4.5"}

	chunk := []byte("data: {\"message\":{\"model\":\"claude-sonnet-4\",\"role\":\"assistant\"}}\n\n")
	result := rw.rewriteStreamChunk(chunk)

	expected := "data: {\"message\":{\"model\":\"claude-opus-4.5\",\"role\":\"assistant\"}}\n\n"
	if string(result) != expected {
		t.Errorf("expected %s, got %s", expected, string(result))
	}
}

func TestRewriteStreamChunk_PreservesThinkingWithSignatureInjection(t *testing.T) {
	rw := &ResponseRewriter{}

	chunk := []byte("event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"thinking\",\"thinking\":\"\"}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"thinking_delta\",\"thinking\":\"abc\"}}\n\nevent: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\nevent: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"tool_use\",\"name\":\"bash\",\"input\":{}}}\n\n")
	result := rw.rewriteStreamChunk(chunk)

	// Streaming mode preserves thinking blocks (does NOT suppress them)
	// to avoid breaking SSE index alignment and TUI rendering
	if !contains(result, []byte(`"content_block":{"type":"thinking"`)) {
		t.Fatalf("expected thinking content_block_start to be preserved, got %s", string(result))
	}
	if !contains(result, []byte(`"delta":{"type":"thinking_delta"`)) {
		t.Fatalf("expected thinking_delta to be preserved, got %s", string(result))
	}
	if !contains(result, []byte(`"type":"content_block_stop","index":0`)) {
		t.Fatalf("expected content_block_stop for thinking block to be preserved, got %s", string(result))
	}
	if !contains(result, []byte(`"content_block":{"type":"tool_use"`)) {
		t.Fatalf("expected tool_use content_block frame to remain, got %s", string(result))
	}
	// Signature should be injected into both thinking and tool_use blocks
	if count := strings.Count(string(result), `"signature":""`); count != 2 {
		t.Fatalf("expected 2 signature injections, but got %d in %s", count, string(result))
	}
}

func TestRewriteStreamChunkClearsCrossProviderThinkingSignature(t *testing.T) {
	rw := &ResponseRewriter{suppressThinking: true}
	chunk := []byte("event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"thinking\",\"thinking\":\"\",\"signature\":\"foreign-start\"}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"thinking_delta\",\"thinking\":\"plan\"}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"signature_delta\",\"signature\":\"foreign-delta\"}}\n\n")

	result := rw.rewriteStreamChunk(chunk)
	if contains(result, []byte("foreign-start")) || contains(result, []byte("foreign-delta")) {
		t.Fatalf("cross-provider stream retained a replayable signature: %s", result)
	}
	if !contains(result, []byte(`"thinking":"plan"`)) || strings.Count(string(result), `"signature":""`) != 2 {
		t.Fatalf("cross-provider stream did not preserve visible thinking with empty signatures: %s", result)
	}
}

func TestLooksLikeSSEChunkDetectsEventStreamFrames(t *testing.T) {
	tests := []struct {
		name string
		data string
		want bool
	}{
		{name: "data frame", data: "data: {\"type\":\"message_delta\"}\n\n", want: true},
		{name: "event frame", data: "event: content_block_delta\n", want: true},
		{name: "whitespace before frame", data: "  data: {\"ok\":true}\n", want: true},
		{name: "json field named data", data: "{\"data\":\"not an SSE frame\"}", want: false},
		{name: "plain text mention", data: "this line mentions data: but is not a frame", want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := looksLikeSSEChunk([]byte(tc.data)); got != tc.want {
				t.Fatalf("looksLikeSSEChunk() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestResponseRewriterWriteDetectsHeaderlessSSEAndRewritesChunk(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.GET("/stream", func(c *gin.Context) {
		rw := NewResponseRewriter(c.Writer, "gpt-5.2-codex")
		c.Header("Content-Type", "application/octet-stream")
		_, err := rw.Write([]byte("data: {\"response\":{\"model\":\"gpt-5.3-codex\"}}\n\n"))
		if err != nil {
			c.String(http.StatusInternalServerError, err.Error())
			return
		}
		rw.Flush()
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/stream", nil)
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"model":"gpt-5.2-codex"`) {
		t.Fatalf("SSE chunk was not rewritten after frame detection: %s", rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "gpt-5.3-codex") {
		t.Fatalf("SSE chunk leaked mapped model: %s", rec.Body.String())
	}
}

func TestResponseRewriterBuffersSplitSSEFramesBeforeRewriting(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	context, _ := gin.CreateTestContext(recorder)
	rw := NewResponseRewriter(context.Writer, "")
	rw.suppressThinking = true
	rw.Header().Set("Content-Type", "text/event-stream")

	parts := []string{
		`event: content_block_start` + "\n" + `data: {"type":"content_block_start","content_block":{"type":"thinking","thinking":"plan","sign`,
		`ature":"foreign-signature"}}` + "\n",
		"\n",
	}
	for _, part := range parts {
		if n, err := rw.Write([]byte(part)); err != nil || n != len(part) {
			t.Fatalf("Write() = (%d, %v), want (%d, nil)", n, err, len(part))
		}
	}

	body := recorder.Body.String()
	if strings.Contains(body, "foreign-signature") {
		t.Fatalf("split SSE frame retained a foreign signature: %s", body)
	}
	if !strings.Contains(body, `"signature":""`) {
		t.Fatalf("split SSE frame did not receive an empty signature: %s", body)
	}
}

func TestResponseRewriterRejectsOversizedFragmentedSSEFrame(t *testing.T) {
	recorder := httptest.NewRecorder()
	context, _ := gin.CreateTestContext(recorder)
	rw := NewResponseRewriter(context.Writer, "")
	rw.Header().Set("Content-Type", "text/event-stream")
	first := "data: " + strings.Repeat("x", maxBufferedResponseBytes-7)
	if n, err := rw.Write([]byte(first)); err != nil || n != len(first) {
		t.Fatalf("first Write() = (%d, %v), want (%d, nil)", n, err, len(first))
	}
	if n, err := rw.Write([]byte("x")); err == nil || n != 0 {
		t.Fatalf("overflow Write() = (%d, %v), want (0, error)", n, err)
	}
	if rw.streamPending.Len() != 0 {
		t.Fatalf("overflow retained %d buffered bytes", rw.streamPending.Len())
	}
}

func TestResponseRewriterParsesAllSSELineEndings(t *testing.T) {
	for name, frame := range map[string]string{
		"CR only": "data: {\"response\":{\"model\":\"mapped\"}}\r\r",
		"mixed":   "event: response.completed\r\ndata: {\"response\":{\"model\":\"mapped\"}}\n\r",
	} {
		t.Run(name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			context, _ := gin.CreateTestContext(recorder)
			rw := NewResponseRewriter(context.Writer, "original")
			rw.Header().Set("Content-Type", "text/event-stream")
			if n, err := rw.Write([]byte(frame)); err != nil || n != len(frame) {
				t.Fatalf("Write() = (%d, %v), want (%d, nil)", n, err, len(frame))
			}
			body := recorder.Body.String()
			if !strings.Contains(body, `"model":"original"`) || strings.Contains(body, `"model":"mapped"`) {
				t.Fatalf("rewritten frame = %q", body)
			}
		})
	}
}

func TestSanitizeAmpRequestBody_RemovesWhitespaceAndNonStringSignatures(t *testing.T) {
	input := []byte(`{"messages":[{"role":"assistant","content":[{"type":"thinking","thinking":"drop-whitespace","signature":"   "},{"type":"thinking","thinking":"drop-number","signature":123},{"type":"thinking","thinking":"keep-valid","signature":"valid-signature"},{"type":"text","text":"keep-text"}]}]}`)
	result := SanitizeAmpRequestBody(input)

	if contains(result, []byte("drop-whitespace")) {
		t.Fatalf("expected whitespace-only signature block to be removed, got %s", string(result))
	}
	if contains(result, []byte("drop-number")) {
		t.Fatalf("expected non-string signature block to be removed, got %s", string(result))
	}
	if !contains(result, []byte("keep-valid")) {
		t.Fatalf("expected valid thinking block to remain, got %s", string(result))
	}
	if !contains(result, []byte("keep-text")) {
		t.Fatalf("expected non-thinking content to remain, got %s", string(result))
	}
}

func TestSanitizeAmpRequestBody_StripsSignatureFromToolUseBlocks(t *testing.T) {
	input := []byte(`{"messages":[{"role":"assistant","content":[{"type":"thinking","thinking":"thought","signature":"valid-sig"},{"type":"tool_use","id":"toolu_01","name":"Bash","input":{"cmd":"ls"},"signature":""}]}]}`)
	result := SanitizeAmpRequestBody(input)

	if contains(result, []byte(`"signature":""`)) {
		t.Fatalf("expected signature to be stripped from tool_use block, got %s", string(result))
	}
	if !contains(result, []byte(`"valid-sig"`)) {
		t.Fatalf("expected thinking signature to remain, got %s", string(result))
	}
	if !contains(result, []byte(`"tool_use"`)) {
		t.Fatalf("expected tool_use block to remain, got %s", string(result))
	}
}

func TestSanitizeAmpRequestBody_MixedInvalidThinkingAndToolUseSignature(t *testing.T) {
	input := []byte(`{"messages":[{"role":"assistant","content":[{"type":"thinking","thinking":"drop-me","signature":""},{"type":"tool_use","id":"toolu_01","name":"Bash","input":{"cmd":"ls"},"signature":""}]}]}`)
	result := SanitizeAmpRequestBody(input)

	if contains(result, []byte("drop-me")) {
		t.Fatalf("expected invalid thinking block to be removed, got %s", string(result))
	}
	if contains(result, []byte(`"signature"`)) {
		t.Fatalf("expected signature to be stripped from tool_use block, got %s", string(result))
	}
	if !contains(result, []byte(`"tool_use"`)) {
		t.Fatalf("expected tool_use block to remain, got %s", string(result))
	}
}

func TestNormalizeAmpToolNames_NonStreaming(t *testing.T) {
	input := []byte(`{"content":[{"type":"tool_use","id":"toolu_01","name":"bash","input":{"cmd":"ls"}},{"type":"tool_use","id":"toolu_02","name":"read","input":{"path":"/tmp"}},{"type":"text","text":"hello"}]}`)
	result := normalizeAmpToolNames(input)

	if !contains(result, []byte(`"name":"Bash"`)) {
		t.Errorf("expected bash->Bash, got %s", string(result))
	}
	if !contains(result, []byte(`"name":"Read"`)) {
		t.Errorf("expected read->Read, got %s", string(result))
	}
	if contains(result, []byte(`"name":"bash"`)) {
		t.Errorf("expected lowercase bash to be replaced, got %s", string(result))
	}
}

func TestNormalizeAmpToolNames_Streaming(t *testing.T) {
	input := []byte(`{"type":"content_block_start","index":1,"content_block":{"type":"tool_use","name":"grep","id":"toolu_01","input":{}}}`)
	result := normalizeAmpToolNames(input)

	if !contains(result, []byte(`"name":"Grep"`)) {
		t.Errorf("expected grep->Grep in streaming, got %s", string(result))
	}
}

func TestNormalizeAmpToolNames_AlreadyCorrect(t *testing.T) {
	input := []byte(`{"content":[{"type":"tool_use","id":"toolu_01","name":"Bash","input":{"cmd":"ls"}}]}`)
	result := normalizeAmpToolNames(input)

	if string(result) != string(input) {
		t.Errorf("expected no modification for correctly-cased tool, got %s", string(result))
	}
}

func TestNormalizeAmpToolNames_GlobPreserved(t *testing.T) {
	input := []byte(`{"content":[{"type":"tool_use","id":"toolu_01","name":"glob","input":{"pattern":"*.go"}}]}`)
	result := normalizeAmpToolNames(input)

	if string(result) != string(input) {
		t.Errorf("expected glob to remain lowercase, got %s", string(result))
	}
}

func TestNormalizeAmpToolNames_UnknownToolUntouched(t *testing.T) {
	input := []byte(`{"content":[{"type":"tool_use","id":"toolu_01","name":"edit_file","input":{"path":"/tmp/x"}}]}`)
	result := normalizeAmpToolNames(input)

	if string(result) != string(input) {
		t.Errorf("expected no modification for unknown tool, got %s", string(result))
	}
}

func contains(data, substr []byte) bool {
	for i := 0; i <= len(data)-len(substr); i++ {
		if string(data[i:i+len(substr)]) == string(substr) {
			return true
		}
	}
	return false
}
