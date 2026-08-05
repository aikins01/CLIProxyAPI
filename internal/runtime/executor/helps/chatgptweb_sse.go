package helps

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

const chatGPTWebSSEMaxEventBytes = 4 << 20

type chatGPTWebMessage struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	Author struct {
		Role string `json:"role"`
	} `json:"author"`
	Content struct {
		Parts []any `json:"parts"`
	} `json:"content"`
	Metadata struct {
		// Weight is a pointer so an omitted value stays distinguishable
		// from an explicit zero (replay marker).
		Weight        *float64 `json:"weight,omitempty"`
		FinishDetails *struct {
			Type string `json:"type"`
		} `json:"finish_details"`
	} `json:"metadata"`
}

func (m *chatGPTWebMessage) text() (string, error) {
	var buf bytes.Buffer
	for i, part := range m.Content.Parts {
		s, ok := part.(string)
		if !ok {
			return "", fmt.Errorf("chatgpt-web: assistant content part %d is not text", i)
		}
		buf.WriteString(s)
	}
	return buf.String(), nil
}

func (m *chatGPTWebMessage) hasLiveStatus() bool {
	return m.Status == "in_progress"
}

func (m *chatGPTWebMessage) hasFinishedStatus() bool {
	return m.Status == "finished_successfully"
}

func chatGPTWebStatusAllowsFinishDetails(status string) bool {
	switch strings.TrimSpace(status) {
	case "", "in_progress", "finished_successfully":
		return true
	default:
		return false
	}
}

type chatGPTWebStreamEnvelope struct {
	ConversationID string             `json:"conversation_id"`
	Type           string             `json:"type"`
	Message        *chatGPTWebMessage `json:"message"`
	Token          json.RawMessage    `json:"token"`
	Data           json.RawMessage    `json:"data"`
	P              *string            `json:"p"`
	O              *string            `json:"o"`
	V              json.RawMessage    `json:"v"`
}

// ChatGPTWebSSEParser converts ChatGPT web SSE into assistant text deltas. It supports
// both whole-message cumulative events and the "v1" JSON-patch delta encoding
// (declared via the delta_encoding control event). Text is emitted as deltas
// sliced from cumulative state.
type ChatGPTWebSSEParser struct {
	v1                   bool
	pendingDeltaEncoding bool
	pendingErrorEvent    bool
	eventData            []byte
	hasEventData         bool
	accumulated          string
	emitted              int
	convID               string
	resumeToken          string
	handoff              bool
	terminal             bool
	diverged             bool
	finishedByStatus     bool
	finishedByDetails    bool
	finishType           string
	seenLive             bool
	seenAssistant        bool
	pendingFinal         string
	pendingFinalID       string
	pendingFinalSet      bool
	pendingReplay        bool
	v1MessageRole        string
	v1MessageStatus      string
	v1State              chatGPTWebV1PatchState
}

type chatGPTWebV1Patch struct {
	P *string         `json:"p"`
	O *string         `json:"o"`
	V json.RawMessage `json:"v"`
}

type chatGPTWebV1ResolvedPatch struct {
	P string
	O string
	V json.RawMessage
}

type chatGPTWebV1PatchState struct {
	path         string
	operation    string
	pathSet      bool
	operationSet bool
	children     []chatGPTWebV1PatchState
}

func (p *ChatGPTWebSSEParser) ConversationID() string { return p.convID }
func (p *ChatGPTWebSSEParser) ResumeToken() string    { return p.resumeToken }
func (p *ChatGPTWebSSEParser) Handoff() bool          { return p.handoff }
func (p *ChatGPTWebSSEParser) Terminal() bool         { return p.terminal }
func (p *ChatGPTWebSSEParser) Finished() bool {
	return (p.finishedByStatus || p.finishedByDetails) && p.seenAssistant
}
func (p *ChatGPTWebSSEParser) FinishType() string  { return p.finishType }
func (p *ChatGPTWebSSEParser) Text() string        { return p.accumulated }
func (p *ChatGPTWebSSEParser) Diverged() bool      { return p.diverged }
func (p *ChatGPTWebSSEParser) SeenAssistant() bool { return p.seenAssistant }

// Continuation creates a parser for the next HTTP segment of the same v1 stream.
func (p *ChatGPTWebSSEParser) Continuation() *ChatGPTWebSSEParser {
	if p == nil {
		return &ChatGPTWebSSEParser{}
	}
	next := *p
	next.pendingDeltaEncoding = false
	next.pendingErrorEvent = false
	next.eventData = nil
	next.hasEventData = false
	next.resumeToken = ""
	next.handoff = false
	next.terminal = false
	next.finishedByStatus = false
	next.finishedByDetails = false
	next.finishType = ""
	next.v1State = p.v1State.clone()
	return &next
}

func (s chatGPTWebV1PatchState) clone() chatGPTWebV1PatchState {
	cloned := s
	if len(s.children) != 0 {
		cloned.children = make([]chatGPTWebV1PatchState, len(s.children))
		for i := range s.children {
			cloned.children[i] = s.children[i].clone()
		}
	}
	return cloned
}

func (s *chatGPTWebV1PatchState) resolve(op chatGPTWebV1Patch) (chatGPTWebV1ResolvedPatch, error) {
	if op.P != nil {
		s.path = *op.P
		s.pathSet = true
	}
	if op.O != nil {
		s.operation = *op.O
		s.operationSet = true
	}
	if !s.pathSet || !s.operationSet {
		if op.P == nil && op.O == nil && !s.pathSet && !s.operationSet {
			return chatGPTWebV1ResolvedPatch{V: op.V}, nil
		}
		return chatGPTWebV1ResolvedPatch{}, fmt.Errorf("chatgpt-web: v1 patch continuation omitted operation state")
	}
	return chatGPTWebV1ResolvedPatch{P: s.path, O: s.operation, V: op.V}, nil
}

// FeedLine processes one complete SSE control or data line. It returns a text
// delta when one was produced.
func (p *ChatGPTWebSSEParser) FeedLine(line []byte) (delta string, terminal bool, err error) {
	line = bytes.TrimRight(line, "\r")
	if len(line) == 0 {
		return "", false, nil
	}
	if bytes.HasPrefix(line, []byte("event:")) {
		event := bytes.TrimSpace(line[len("event:"):])
		p.pendingDeltaEncoding = bytes.Equal(event, []byte("delta_encoding"))
		p.pendingErrorEvent = bytes.Equal(event, []byte("error"))
		return "", false, nil
	}
	if !bytes.HasPrefix(line, []byte("data:")) {
		return "", false, nil
	}
	payload := bytes.TrimSpace(line[len("data:"):])
	if len(payload) == 0 {
		return "", false, nil
	}
	return p.feedPayload(payload)
}

func (p *ChatGPTWebSSEParser) feedRawLine(line []byte) (delta string, terminal bool, err error) {
	line = bytes.TrimRight(line, "\r")
	if len(line) == 0 {
		return p.dispatchEvent()
	}
	if bytes.HasPrefix(line, []byte("event:")) {
		event := bytes.TrimSpace(line[len("event:"):])
		p.pendingDeltaEncoding = bytes.Equal(event, []byte("delta_encoding"))
		p.pendingErrorEvent = bytes.Equal(event, []byte("error"))
		return "", false, nil
	}
	if !bytes.HasPrefix(line, []byte("data:")) {
		return "", false, nil
	}
	payload := line[len("data:"):]
	if len(payload) > 0 && payload[0] == ' ' {
		payload = payload[1:]
	}
	additional := len(payload)
	if p.hasEventData {
		additional++
	}
	if len(p.eventData)+additional > chatGPTWebSSEMaxEventBytes {
		return "", false, fmt.Errorf("chatgpt-web: SSE data event exceeds %d bytes", chatGPTWebSSEMaxEventBytes)
	}
	if p.hasEventData {
		p.eventData = append(p.eventData, '\n')
	}
	p.eventData = append(p.eventData, payload...)
	p.hasEventData = true
	return "", false, nil
}

func (p *ChatGPTWebSSEParser) dispatchEvent() (delta string, terminal bool, err error) {
	if !p.hasEventData {
		p.pendingDeltaEncoding = false
		p.pendingErrorEvent = false
		return "", false, nil
	}
	payload := p.eventData
	p.eventData = nil
	p.hasEventData = false
	return p.feedPayload(payload)
}

func (p *ChatGPTWebSSEParser) feedPayload(payload []byte) (delta string, terminal bool, err error) {
	if p.pendingErrorEvent {
		p.pendingErrorEvent = false
		p.pendingDeltaEncoding = false
		return "", false, fmt.Errorf("chatgpt-web: upstream stream returned an error event")
	}
	if bytes.Equal(bytes.TrimSpace(payload), []byte("[DONE]")) {
		delta := p.flush()
		if !p.seenAssistant && !p.handoff {
			return "", false, fmt.Errorf("chatgpt-web: stream ended without an assistant response")
		}
		p.terminal = true
		return delta, true, nil
	}
	if p.pendingDeltaEncoding {
		p.pendingDeltaEncoding = false
		if string(payload) == `"v1"` {
			p.v1 = true
		}
		return "", false, nil
	}

	var env chatGPTWebStreamEnvelope
	if errDecode := json.Unmarshal(payload, &env); errDecode != nil {
		if p.v1 {
			return "", false, fmt.Errorf("chatgpt-web: decode SSE patch event: %w", errDecode)
		}
		return "", false, fmt.Errorf("chatgpt-web: decode SSE data event: %w", errDecode)
	}
	if env.ConversationID != "" {
		p.convID = env.ConversationID
	}

	if env.Type != "" {
		switch env.Type {
		case "delta_encoding":
			if string(env.Data) == `"v1"` {
				p.v1 = true
			}
			return "", false, nil
		case "resume_conversation_token":
			if len(env.Token) == 0 {
				return "", false, fmt.Errorf("chatgpt-web: resume token event omitted token")
			}
			var token string
			if err := json.Unmarshal(env.Token, &token); err != nil {
				return "", false, fmt.Errorf("chatgpt-web: decode resume token event: %w", err)
			}
			if token == "" {
				return "", false, fmt.Errorf("chatgpt-web: resume token event returned an empty token")
			}
			p.resumeToken = token
			return "", false, nil
		case "stream_handoff":
			p.handoff = true
			return "", false, nil
		case "error":
			// Upstream failure events carry no message; surfacing them
			// keeps a following [DONE] from reporting success.
			return "", false, fmt.Errorf("chatgpt-web: upstream stream returned an error event")
		default:
			return "", false, nil
		}
	}

	if p.v1 {
		return p.feedV1(chatGPTWebV1Patch{P: env.P, O: env.O, V: env.V})
	}
	return p.feedWholeMessage(env)
}

func (p *ChatGPTWebSSEParser) feedWholeMessage(env chatGPTWebStreamEnvelope) (string, bool, error) {
	if env.ConversationID != "" {
		p.convID = env.ConversationID
	}
	m := env.Message
	if m == nil || m.Author.Role != "assistant" {
		return "", false, nil
	}
	replay := m.Metadata.Weight != nil && *m.Metadata.Weight == 0
	text := ""
	if !replay {
		var err error
		text, err = m.text()
		if err != nil {
			return "", false, err
		}
	}
	if !replay && strings.TrimSpace(m.Status) != "" && !m.hasLiveStatus() && !m.hasFinishedStatus() {
		p.finishedByStatus = false
		p.finishedByDetails = false
		p.finishType = ""
		p.pendingFinal = ""
		p.pendingFinalID = ""
		p.pendingFinalSet = false
		p.pendingReplay = false
		return "", false, nil
	}
	if m.hasLiveStatus() && !replay {
		p.seenAssistant = true
		p.seenLive = true
		p.acceptCumulative(text)
		p.finishedByStatus = false
		p.finishedByDetails = false
		p.finishType = ""
		if m.Metadata.FinishDetails != nil && strings.TrimSpace(m.Metadata.FinishDetails.Type) != "" && chatGPTWebStatusAllowsFinishDetails(m.Status) {
			p.finishedByDetails = true
			p.finishType = m.Metadata.FinishDetails.Type
		}
		return p.emitDelta(), false, nil
	}
	// An explicit in-progress assistant event preceded this finished one,
	// so it is the final cumulative value even when the status field was
	// omitted mid-stream.
	if p.seenLive {
		if replay {
			return "", false, nil
		}
		p.seenAssistant = true
		p.acceptCumulative(text)
		p.finishedByStatus = m.hasFinishedStatus()
		p.finishedByDetails = false
		p.finishType = ""
		if m.Metadata.FinishDetails != nil && strings.TrimSpace(m.Metadata.FinishDetails.Type) != "" && chatGPTWebStatusAllowsFinishDetails(m.Status) {
			p.finishedByDetails = true
			p.finishType = m.Metadata.FinishDetails.Type
		}
		return p.emitDelta(), false, nil
	}
	if replay {
		if !p.pendingFinalSet {
			p.pendingFinalID = m.ID
			p.pendingFinalSet = true
			p.pendingReplay = true
		}
	} else {
		p.pendingFinalID = m.ID
		p.pendingFinal = text
		p.pendingFinalSet = true
		p.pendingReplay = false
		p.seenAssistant = true
	}
	// A replay's finish_details belongs to its old turn; only a non-replay
	// pending final may mark the current turn finished. A replay-flagged
	// finish must not let an abruptly truncated stream pass the executor's
	// ended-before-completion guard.
	if m.hasFinishedStatus() && !replay {
		p.finishedByStatus = true
	}
	if m.Metadata.FinishDetails != nil && strings.TrimSpace(m.Metadata.FinishDetails.Type) != "" && !replay && chatGPTWebStatusAllowsFinishDetails(m.Status) {
		p.finishedByDetails = true
		p.finishType = m.Metadata.FinishDetails.Type
	}
	return "", false, nil
}

// acceptCumulative records the latest cumulative message text. A revision
// shorter than or disjoint from what was already emitted cannot be retracted
// on the wire, so the stream is marked diverged: subsequent deltas are
// suppressed while Text() keeps tracking the authoritative value.
func (p *ChatGPTWebSSEParser) acceptCumulative(text string) {
	if text == p.accumulated {
		return
	}
	// Once diverged the prefix comparison is unsound: emitted may exceed
	// the revised accumulation length. Divergence is irreversible anyway.
	if !p.diverged && (p.emitted > len(text) || !strings.HasPrefix(text, p.accumulated[:p.emitted])) {
		p.diverged = true
	}
	p.accumulated = text
}

func (p *ChatGPTWebSSEParser) feedV1(op chatGPTWebV1Patch) (string, bool, error) {
	previousPath := p.v1State.path
	previousPathSet := p.v1State.pathSet
	previousOperation := p.v1State.operation
	previousOperationSet := p.v1State.operationSet
	resolved, err := p.v1State.resolve(op)
	if err != nil {
		return "", false, err
	}
	if resolved.O == "patch" {
		if !previousOperationSet || previousOperation != "patch" || (op.P != nil && previousPathSet && *op.P != previousPath) {
			p.v1State.children = nil
		}
		if len(resolved.V) == 0 {
			return "", false, nil
		}
		var ops []chatGPTWebV1Patch
		if err := json.Unmarshal(resolved.V, &ops); err != nil {
			return "", false, fmt.Errorf("chatgpt-web: decode SSE patch batch: %w", err)
		}
		for len(p.v1State.children) < len(ops) {
			p.v1State.children = append(p.v1State.children, chatGPTWebV1PatchState{})
		}
		var out strings.Builder
		for i, sub := range ops {
			if sub.P == nil && sub.O == nil && !p.v1State.children[i].pathSet && !p.v1State.children[i].operationSet {
				return "", false, fmt.Errorf("chatgpt-web: v1 batch child omitted operation state")
			}
			resolvedSub, errResolve := p.v1State.children[i].resolve(sub)
			if errResolve != nil {
				return "", false, errResolve
			}
			if len(resolvedSub.V) == 0 {
				continue
			}
			d, err := p.applyV1Op(resolvedSub)
			if err != nil {
				return "", false, err
			}
			out.WriteString(d)
		}
		return out.String(), false, nil
	}
	p.v1State.children = nil
	if len(resolved.V) == 0 {
		return "", false, nil
	}
	d, err := p.applyV1Op(resolved)
	return d, false, err
}

func (p *ChatGPTWebSSEParser) applyV1Op(op chatGPTWebV1ResolvedPatch) (string, error) {
	switch {
	case op.P == "" && op.O == "":
		if delta, recognized, err := p.applyV1RootValue(op.V); recognized {
			return delta, err
		}
		return "", fmt.Errorf("chatgpt-web: unrecognized v1 patch value for message path")
	case op.P == "" && (op.O == "add" || op.O == "replace"):
		if delta, recognized, err := p.applyV1RootValue(op.V); recognized {
			return delta, err
		}
		return "", fmt.Errorf("chatgpt-web: unrecognized v1 root patch value")
	case op.P == "/message" && (op.O == "add" || op.O == "replace"):
		var message chatGPTWebMessage
		if err := json.Unmarshal(op.V, &message); err != nil {
			return "", fmt.Errorf("chatgpt-web: decode v1 message patch: %w", err)
		}
		return p.applyV1Message(&message)
	case op.P == "/message/content/parts/0" && op.O == "append":
		var s string
		if err := json.Unmarshal(op.V, &s); err != nil {
			return "", fmt.Errorf("chatgpt-web: decode v1 append patch: %w", err)
		}
		return p.applyV1Content(s, false), nil
	case op.P == "/message/content/parts/0" && op.O == "replace":
		var s string
		if err := json.Unmarshal(op.V, &s); err != nil {
			return "", fmt.Errorf("chatgpt-web: decode v1 replace patch: %w", err)
		}
		return p.applyV1Content(s, true), nil
	case op.P == "/conversation_id":
		var s string
		if err := json.Unmarshal(op.V, &s); err != nil {
			return "", fmt.Errorf("chatgpt-web: decode v1 conversation_id patch: %w", err)
		}
		p.convID = s
	case op.P == "/message/author/role" && (op.O == "add" || op.O == "replace"):
		var role string
		if err := json.Unmarshal(op.V, &role); err != nil {
			return "", fmt.Errorf("chatgpt-web: decode v1 message role patch: %w", err)
		}
		if role != "assistant" {
			p.clearV1AssistantState(role)
			return "", nil
		}
		p.v1MessageRole = role
		return "", nil
	case op.P == "/message/metadata/finish_details" && (op.O == "add" || op.O == "replace"):
		if p.v1MessageRole != "" && p.v1MessageRole != "assistant" || !chatGPTWebStatusAllowsFinishDetails(p.v1MessageStatus) {
			return "", nil
		}
		var fd struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(op.V, &fd); err != nil {
			return "", fmt.Errorf("chatgpt-web: decode v1 finish_details patch: %w", err)
		}
		if strings.TrimSpace(fd.Type) == "" {
			return "", fmt.Errorf("chatgpt-web: v1 finish_details patch omitted type")
		}
		p.finishedByDetails = true
		p.finishType = fd.Type
	case op.P == "/message/metadata/finish_details" && op.O == "remove":
		p.finishedByDetails = false
		p.finishType = ""
	case op.P == "/message/status" && (op.O == "add" || op.O == "replace"):
		if p.v1MessageRole != "" && p.v1MessageRole != "assistant" {
			return "", nil
		}
		var status string
		if err := json.Unmarshal(op.V, &status); err != nil {
			return "", fmt.Errorf("chatgpt-web: decode v1 message status patch: %w", err)
		}
		p.v1MessageStatus = status
		p.finishedByStatus = status == "finished_successfully"
		if !chatGPTWebStatusAllowsFinishDetails(status) {
			p.finishedByDetails = false
			p.finishType = ""
		}
	case op.P == "/message/content/parts/0" || op.P == "/message/metadata/finish_details" || op.P == "/message/status":
		return "", fmt.Errorf("chatgpt-web: unsupported v1 operation %q for path %q", op.O, op.P)
	}
	// Unknown operations are ignored for forward compatibility.
	return "", nil
}

func (p *ChatGPTWebSSEParser) applyV1RootValue(raw json.RawMessage) (string, bool, error) {
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return p.applyV1Content(s, false), true, nil
	}
	var message chatGPTWebMessage
	if err := json.Unmarshal(raw, &message); err == nil && message.Author.Role == "assistant" {
		delta, err := p.applyV1Message(&message)
		return delta, true, err
	}
	var root struct {
		ConversationID string             `json:"conversation_id"`
		Message        *chatGPTWebMessage `json:"message"`
	}
	if err := json.Unmarshal(raw, &root); err != nil || (root.ConversationID == "" && root.Message == nil) {
		return "", false, nil
	}
	if root.ConversationID != "" {
		p.convID = root.ConversationID
	}
	if root.Message == nil {
		return "", true, nil
	}
	if root.Message.Author.Role != "assistant" && p.v1MessageRole != "assistant" && !p.seenAssistant && p.accumulated == "" && !p.finishedByStatus && !p.finishedByDetails {
		return "", true, nil
	}
	delta, err := p.applyV1Message(root.Message)
	return delta, true, err
}

func (p *ChatGPTWebSSEParser) applyV1Message(message *chatGPTWebMessage) (string, error) {
	if message.Metadata.Weight != nil && *message.Metadata.Weight == 0 {
		return "", nil
	}
	p.v1MessageRole = message.Author.Role
	p.v1MessageStatus = message.Status
	if p.v1MessageRole != "assistant" {
		p.clearV1AssistantState(p.v1MessageRole)
		return "", nil
	}
	text, err := message.text()
	if err != nil {
		return "", err
	}
	p.seenAssistant = true
	p.seenLive = true
	p.acceptCumulative(text)
	p.finishedByStatus = message.hasFinishedStatus()
	p.finishedByDetails = false
	p.finishType = ""
	if finishDetails := message.Metadata.FinishDetails; finishDetails != nil && strings.TrimSpace(finishDetails.Type) != "" && chatGPTWebStatusAllowsFinishDetails(message.Status) {
		p.finishedByDetails = true
		p.finishType = finishDetails.Type
	}
	return p.emitDelta(), nil
}

func (p *ChatGPTWebSSEParser) clearV1AssistantState(role string) {
	p.v1MessageRole = role
	p.v1MessageStatus = ""
	p.acceptCumulative("")
	p.seenAssistant = false
	p.seenLive = false
	p.finishedByStatus = false
	p.finishedByDetails = false
	p.finishType = ""
}

func (p *ChatGPTWebSSEParser) applyV1Content(text string, replace bool) string {
	if !chatGPTWebStatusAllowsFinishDetails(p.v1MessageStatus) || p.v1MessageRole != "" && p.v1MessageRole != "assistant" {
		return ""
	}
	// Content patches with no declared role target the streaming assistant
	// message; only an explicit non-assistant role suppresses them.
	p.v1MessageRole = "assistant"
	p.seenAssistant = true
	p.seenLive = true
	if replace {
		p.acceptCumulative(text)
		return p.emitDelta()
	}
	p.accumulated += text
	if p.diverged {
		return ""
	}
	p.emitted = len(p.accumulated)
	return text
}

func (p *ChatGPTWebSSEParser) emitDelta() string {
	if !p.seenLive || p.diverged {
		return ""
	}
	if len(p.accumulated) <= p.emitted {
		return ""
	}
	d := p.accumulated[p.emitted:]
	p.emitted = len(p.accumulated)
	return d
}

func (p *ChatGPTWebSSEParser) flush() string {
	if !p.seenLive && p.accumulated == "" && p.pendingFinal != "" && !p.pendingReplay {
		p.accumulated = p.pendingFinal
		p.pendingFinal = ""
		p.pendingFinalSet = false
		p.seenLive = true
	}
	return p.emitDelta()
}

// DrainChatGPTWebSSE reads all SSE events from r into p, emitting deltas through
// onDelta. An EOF without a [DONE] marker flushes the parser the same way;
// callers inspect p afterwards for final text and parser state.
func DrainChatGPTWebSSE(r io.Reader, p *ChatGPTWebSSEParser, onDelta func(string)) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), chatGPTWebSSEMaxEventBytes+64*1024)
	for scanner.Scan() {
		delta, terminal, err := p.feedRawLine(scanner.Bytes())
		if err != nil {
			return err
		}
		if delta != "" && onDelta != nil {
			onDelta(delta)
		}
		if terminal {
			return nil
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	delta, terminal, err := p.dispatchEvent()
	if err != nil {
		return err
	}
	if delta != "" && onDelta != nil {
		onDelta(delta)
	}
	if terminal {
		return nil
	}
	if delta := p.flush(); delta != "" && onDelta != nil {
		onDelta(delta)
	}
	return nil
}
