package amp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// ResponseRewriter wraps a gin.ResponseWriter to intercept and modify the response body
// It is used to rewrite model names in responses when model mapping is used
// and to keep Amp-compatible response shapes.
type ResponseRewriter struct {
	gin.ResponseWriter
	body             *bytes.Buffer
	originalModel    string
	isStreaming      bool
	sseStreaming     bool
	streamPending    bytes.Buffer
	suppressThinking bool
}

// NewResponseRewriter creates a new response rewriter for model name substitution.
func NewResponseRewriter(w gin.ResponseWriter, originalModel string) *ResponseRewriter {
	return &ResponseRewriter{
		ResponseWriter: w,
		body:           &bytes.Buffer{},
		originalModel:  originalModel,
	}
}

const maxBufferedResponseBytes = 2 * 1024 * 1024 // 2MB safety cap

func looksLikeSSEChunk(data []byte) bool {
	data = normalizeSSELineEndings(data)
	for _, line := range bytes.Split(data, []byte("\n")) {
		trimmed := bytes.TrimSpace(line)
		if bytes.HasPrefix(trimmed, []byte("data:")) ||
			bytes.HasPrefix(trimmed, []byte("event:")) {
			return true
		}
	}
	return false
}

func (rw *ResponseRewriter) enableStreaming(reason string) error {
	if rw.isStreaming {
		return nil
	}
	rw.isStreaming = true

	if rw.body != nil && rw.body.Len() > 0 {
		buf := rw.body.Bytes()
		toFlush := make([]byte, len(buf))
		copy(toFlush, buf)
		rw.body.Reset()

		if rw.sseStreaming {
			if err := rw.writeSSEFrames(toFlush); err != nil {
				return err
			}
		} else {
			if _, err := rw.ResponseWriter.Write(rw.rewriteStreamChunk(toFlush)); err != nil {
				return err
			}
			if flusher, ok := rw.ResponseWriter.(http.Flusher); ok {
				flusher.Flush()
			}
		}
	}

	log.Debugf("amp response rewriter: switched to streaming (%s)", reason)
	return nil
}

func (rw *ResponseRewriter) Write(data []byte) (int, error) {
	if !rw.isStreaming && rw.body.Len() == 0 {
		contentType := rw.Header().Get("Content-Type")
		rw.sseStreaming = strings.Contains(contentType, "text/event-stream")
		rw.isStreaming = rw.sseStreaming ||
			strings.Contains(contentType, "stream")
	}

	if !rw.isStreaming {
		if looksLikeSSEChunk(data) {
			rw.sseStreaming = true
			if err := rw.enableStreaming("sse heuristic"); err != nil {
				return 0, err
			}
		} else if rw.body.Len()+len(data) > maxBufferedResponseBytes {
			log.Warnf("amp response rewriter: buffer exceeded %d bytes, switching to streaming", maxBufferedResponseBytes)
			if err := rw.enableStreaming("buffer limit"); err != nil {
				return 0, err
			}
		}
	}

	if rw.isStreaming {
		if rw.sseStreaming {
			if err := rw.writeSSEFrames(data); err != nil {
				return 0, err
			}
			return len(data), nil
		}
		rewritten := rw.rewriteStreamChunk(data)
		_, err := rw.ResponseWriter.Write(rewritten)
		if err == nil {
			if flusher, ok := rw.ResponseWriter.(http.Flusher); ok {
				flusher.Flush()
			}
		}
		if err != nil {
			return 0, err
		}
		return len(data), nil
	}
	return rw.body.Write(data)
}

func (rw *ResponseRewriter) writeSSEFrames(data []byte) error {
	wroteFrame := false
	for len(data) > 0 {
		available := maxBufferedResponseBytes - rw.streamPending.Len()
		if available <= 0 {
			rw.streamPending.Reset()
			return fmt.Errorf("SSE frame exceeded %d bytes", maxBufferedResponseBytes)
		}
		writeBytes := min(available, len(data))
		_, _ = rw.streamPending.Write(data[:writeBytes])
		data = data[writeBytes:]
		for {
			frameEnd := sseFrameEnd(rw.streamPending.Bytes())
			if frameEnd == 0 {
				break
			}
			frame := rw.streamPending.Next(frameEnd)
			if _, err := rw.ResponseWriter.Write(rw.rewriteStreamChunk(frame)); err != nil {
				return err
			}
			wroteFrame = true
		}
		if rw.streamPending.Len() >= maxBufferedResponseBytes {
			rw.streamPending.Reset()
			return fmt.Errorf("SSE frame exceeded %d bytes", maxBufferedResponseBytes)
		}
	}
	if wroteFrame {
		if flusher, ok := rw.ResponseWriter.(http.Flusher); ok {
			flusher.Flush()
		}
	}
	return nil
}

func sseFrameEnd(data []byte) int {
	lineStart := 0
	for lineStart < len(data) {
		lineEnd := lineStart
		for lineEnd < len(data) && data[lineEnd] != '\r' && data[lineEnd] != '\n' {
			lineEnd++
		}
		if lineEnd == len(data) {
			return 0
		}
		nextLine := lineEnd + 1
		if data[lineEnd] == '\r' && nextLine < len(data) && data[nextLine] == '\n' {
			nextLine++
		}
		if lineEnd == lineStart {
			return nextLine
		}
		lineStart = nextLine
	}
	return 0
}

func (rw *ResponseRewriter) Flush() {
	if rw.isStreaming {
		if flusher, ok := rw.ResponseWriter.(http.Flusher); ok {
			flusher.Flush()
		}
		return
	}
	if rw.body.Len() > 0 {
		rewritten := rw.rewriteModelInResponse(rw.body.Bytes())
		// Update Content-Length to match the rewritten body size, since
		// signature injection and model name changes alter the payload length.
		rw.ResponseWriter.Header().Set("Content-Length", fmt.Sprintf("%d", len(rewritten)))
		if _, err := rw.ResponseWriter.Write(rewritten); err != nil {
			log.Warnf("amp response rewriter: failed to write rewritten response: %v", err)
		}
	}
}

var modelFieldPaths = []string{"message.model", "model", "modelVersion", "response.model", "response.modelVersion"}

// ampCanonicalToolNames maps tool names to the exact casing expected by the
// Amp mode tool whitelist (case-sensitive match).
var ampCanonicalToolNames = map[string]string{
	"bash":  "Bash",
	"read":  "Read",
	"grep":  "Grep",
	"glob":  "glob",
	"task":  "Task",
	"check": "Check",
}

// normalizeAmpToolNames fixes tool_use block names to match Amp's canonical casing.
// Some upstream models return lowercase tool names (e.g. "bash" instead of "Bash")
// which causes Amp's case-sensitive mode whitelist to reject them.
func normalizeAmpToolNames(data []byte) []byte {
	// Non-streaming: content[].name in tool_use blocks
	for index, block := range gjson.GetBytes(data, "content").Array() {
		if block.Get("type").String() != "tool_use" {
			continue
		}
		name := block.Get("name").String()
		if canonical, ok := ampCanonicalToolNames[strings.ToLower(name)]; ok && name != canonical {
			path := fmt.Sprintf("content.%d.name", index)
			var err error
			data, err = sjson.SetBytes(data, path, canonical)
			if err != nil {
				log.Warnf("Amp ResponseRewriter: failed to normalize tool name %q to %q: %v", name, canonical, err)
			}
		}
	}

	// Streaming: content_block.name in content_block_start events
	if gjson.GetBytes(data, "content_block.type").String() == "tool_use" {
		name := gjson.GetBytes(data, "content_block.name").String()
		if canonical, ok := ampCanonicalToolNames[strings.ToLower(name)]; ok && name != canonical {
			var err error
			data, err = sjson.SetBytes(data, "content_block.name", canonical)
			if err != nil {
				log.Warnf("Amp ResponseRewriter: failed to normalize streaming tool name %q to %q: %v", name, canonical, err)
			}
		}
	}

	return data
}

// ensureAmpSignature injects empty signature fields into tool_use/thinking blocks
// in API responses so that the Amp TUI does not crash on P.signature.length.
func ensureAmpSignature(data []byte) []byte {
	for index, block := range gjson.GetBytes(data, "content").Array() {
		blockType := block.Get("type").String()
		if blockType != "tool_use" && blockType != "thinking" {
			continue
		}
		signaturePath := fmt.Sprintf("content.%d.signature", index)
		if gjson.GetBytes(data, signaturePath).Exists() {
			continue
		}
		var err error
		data, err = sjson.SetBytes(data, signaturePath, "")
		if err != nil {
			log.Warnf("Amp ResponseRewriter: failed to add empty signature to %s block: %v", blockType, err)
			break
		}
	}

	contentBlockType := gjson.GetBytes(data, "content_block.type").String()
	if (contentBlockType == "tool_use" || contentBlockType == "thinking") && !gjson.GetBytes(data, "content_block.signature").Exists() {
		var err error
		data, err = sjson.SetBytes(data, "content_block.signature", "")
		if err != nil {
			log.Warnf("Amp ResponseRewriter: failed to add empty signature to streaming %s block: %v", contentBlockType, err)
		}
	}

	return data
}

func (rw *ResponseRewriter) suppressAmpThinking(data []byte) []byte {
	if !rw.suppressThinking {
		return data
	}
	if gjson.GetBytes(data, `content.#(type=="tool_use")`).Exists() {
		filtered := gjson.GetBytes(data, `content.#(type!="thinking")#`)
		if filtered.Exists() {
			originalCount := gjson.GetBytes(data, "content.#").Int()
			filteredCount := filtered.Get("#").Int()
			if originalCount > filteredCount {
				var err error
				data, err = sjson.SetBytes(data, "content", filtered.Value())
				if err != nil {
					log.Warnf("Amp ResponseRewriter: failed to suppress thinking blocks: %v", err)
				}
			}
		}
	}

	return data
}

func (rw *ResponseRewriter) rewriteModelInResponse(data []byte) []byte {
	data = ensureAmpSignature(data)
	data = normalizeAmpToolNames(data)
	data = rw.suppressAmpThinking(data)
	if len(data) == 0 {
		return data
	}

	if rw.originalModel == "" {
		return data
	}
	for _, path := range modelFieldPaths {
		if gjson.GetBytes(data, path).Exists() {
			data, _ = sjson.SetBytes(data, path, rw.originalModel)
		}
	}
	return data
}

func (rw *ResponseRewriter) rewriteStreamChunk(chunk []byte) []byte {
	chunk = normalizeSSELineEndings(chunk)
	lines := bytes.Split(chunk, []byte("\n"))
	var out [][]byte

	i := 0
	for i < len(lines) {
		line := lines[i]
		trimmed := bytes.TrimSpace(line)

		// Case 1: "event:" line - look ahead for its "data:" line
		if bytes.HasPrefix(trimmed, []byte("event: ")) {
			// Scan forward past blank lines to find the data: line
			dataIdx := -1
			for j := i + 1; j < len(lines); j++ {
				t := bytes.TrimSpace(lines[j])
				if len(t) == 0 {
					continue
				}
				if bytes.HasPrefix(t, []byte("data: ")) {
					dataIdx = j
				}
				break
			}

			if dataIdx >= 0 {
				// Found event+data pair - process through rewriter
				jsonData := bytes.TrimPrefix(bytes.TrimSpace(lines[dataIdx]), []byte("data: "))
				if len(jsonData) > 0 && jsonData[0] == '{' {
					rewritten := rw.rewriteStreamEvent(jsonData)
					if rewritten == nil {
						i = dataIdx + 1
						continue
					}
					// Emit event line
					out = append(out, line)
					// Emit blank lines between event and data
					for k := i + 1; k < dataIdx; k++ {
						out = append(out, lines[k])
					}
					// Emit rewritten data
					out = append(out, append([]byte("data: "), rewritten...))
					i = dataIdx + 1
					continue
				}
			}

			// No data line found (orphan event from cross-chunk split)
			// Pass it through as-is - the data will arrive in the next chunk
			out = append(out, line)
			i++
			continue
		}

		// Case 2: standalone "data:" line (no preceding event: in this chunk)
		if bytes.HasPrefix(trimmed, []byte("data: ")) {
			jsonData := bytes.TrimPrefix(trimmed, []byte("data: "))
			if len(jsonData) > 0 && jsonData[0] == '{' {
				rewritten := rw.rewriteStreamEvent(jsonData)
				if rewritten != nil {
					out = append(out, append([]byte("data: "), rewritten...))
				}
				i++
				continue
			}
		}

		// Case 3: everything else
		out = append(out, line)
		i++
	}

	return bytes.Join(out, []byte("\n"))
}

func normalizeSSELineEndings(data []byte) []byte {
	data = bytes.ReplaceAll(data, []byte("\r\n"), []byte("\n"))
	return bytes.ReplaceAll(data, []byte("\r"), []byte("\n"))
}

// rewriteStreamEvent processes a single JSON event in the SSE stream.
// It rewrites model names and ensures signature fields exist.
// NOTE: streaming mode does NOT suppress thinking blocks - they are
// passed through with signature injection to avoid breaking SSE index
// alignment and TUI rendering.
func (rw *ResponseRewriter) rewriteStreamEvent(data []byte) []byte {
	// Inject empty signature where needed
	data = ensureAmpSignature(data)
	if rw.suppressThinking {
		data = clearAmpStreamingThinkingSignature(data)
	}

	// Normalize tool names to canonical casing
	data = normalizeAmpToolNames(data)

	// Rewrite model name
	if rw.originalModel != "" {
		for _, path := range modelFieldPaths {
			if gjson.GetBytes(data, path).Exists() {
				data, _ = sjson.SetBytes(data, path, rw.originalModel)
			}
		}
	}

	return data
}

func clearAmpStreamingThinkingSignature(data []byte) []byte {
	if gjson.GetBytes(data, "content_block.type").String() == "thinking" {
		data, _ = sjson.SetBytes(data, "content_block.signature", "")
	}
	if gjson.GetBytes(data, "delta.type").String() == "signature_delta" {
		data, _ = sjson.SetBytes(data, "delta.signature", "")
	}
	return data
}

// SanitizeAmpRequestBody removes thinking blocks with empty/missing/invalid signatures
// and strips the proxy-injected "signature" field from tool_use blocks in the messages
// array before forwarding to the upstream API.
// This prevents 400 errors from the API which requires valid signatures on thinking
// blocks and does not accept a signature field on tool_use blocks.
func SanitizeAmpRequestBody(body []byte) []byte {
	messages := gjson.GetBytes(body, "messages")
	if !messages.Exists() || !messages.IsArray() {
		return body
	}

	modified := false
	for msgIdx, msg := range messages.Array() {
		if msg.Get("role").String() != "assistant" {
			continue
		}
		content := msg.Get("content")
		if !content.Exists() || !content.IsArray() {
			continue
		}

		var keepBlocks []interface{}
		contentModified := false

		for _, block := range content.Array() {
			blockType := block.Get("type").String()
			if blockType == "thinking" {
				sig := block.Get("signature")
				if !sig.Exists() || sig.Type != gjson.String || strings.TrimSpace(sig.String()) == "" {
					contentModified = true
					continue
				}
			}

			// Use raw JSON to prevent float64 rounding of large integers in tool_use inputs
			blockRaw := []byte(block.Raw)
			if blockType == "tool_use" && block.Get("signature").Exists() {
				blockRaw, _ = sjson.DeleteBytes(blockRaw, "signature")
				contentModified = true
			}

			// sjson.SetBytes supports raw JSON strings if wrapped in gjson.Raw
			keepBlocks = append(keepBlocks, json.RawMessage(blockRaw))
		}

		if contentModified {
			contentPath := fmt.Sprintf("messages.%d.content", msgIdx)
			var err error
			if len(keepBlocks) == 0 {
				body, err = sjson.SetBytes(body, contentPath, []interface{}{})
			} else {
				body, err = sjson.SetBytes(body, contentPath, keepBlocks)
			}
			if err != nil {
				log.Warnf("Amp RequestSanitizer: failed to sanitize message %d: %v", msgIdx, err)
				continue
			}
			modified = true
		}
	}

	if modified {
		log.Debugf("Amp RequestSanitizer: sanitized request body")
	}
	return body
}
