// Package ampplugins discovers and parses user-installed Amp plugin agent modes.
package ampplugins

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	log "github.com/sirupsen/logrus"
)

const (
	// SupportHeader advertises that a CLIProxyAPI server accepts plugin agent modes in broker heartbeats.
	SupportHeader      = "X-Cliproxy-Plugin-Agent-Modes"
	modeHeaderMarker   = "// @amp-agent-mode "
	UserRepositoryName = "amp-user-plugins"
	UserScope          = "user"
)

var KeyPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,127}$`)

func NormalizeReasoningEffort(effort string) string {
	switch strings.ToLower(strings.TrimSpace(effort)) {
	case "none", "minimal", "low", "medium", "high", "xhigh", "max":
		return strings.ToLower(strings.TrimSpace(effort))
	default:
		return ""
	}
}

var (
	pluginCallRe = map[string]*regexp.Regexp{
		"createAgent":       regexp.MustCompile(`\bamp\s*\.\s*(?:experimental\s*\.\s*)?createAgent\s*\(\s*\{`),
		"registerAgentMode": regexp.MustCompile(`\bamp\s*\.\s*(?:experimental\s*\.\s*)?registerAgentMode\s*\(\s*\{`),
	}
	pluginAssignmentRe        = regexp.MustCompile(`(?s)(?:(const|let|var)\s+)?([A-Za-z_$][A-Za-z0-9_$]*)\s*=\s*$`)
	pluginFunctionBodyBraceRe = regexp.MustCompile(`(?s)(?:\bfunction(?:\s+[A-Za-z_$][A-Za-z0-9_$]*)?\s*\([^{}]*\)|=>)\s*$`)
	pluginIdentifierRe        = regexp.MustCompile(`^[A-Za-z_$][A-Za-z0-9_$]*$`)
)

type AgentMode struct {
	Key                  string `json:"key"`
	Label                string `json:"label"`
	Description          string `json:"description,omitempty"`
	Color                string `json:"color,omitempty"`
	PluginName           string `json:"pluginName"`
	PluginPath           string `json:"pluginPath"`
	PluginRepositoryName string `json:"pluginRepositoryName"`
	PluginScope          string `json:"pluginScope"`
	AgentName            string `json:"agentName"`
	AgentModel           string `json:"agentModel"`
	AgentTools           any    `json:"agentTools,omitempty"`
	AgentInstructions    string `json:"agentInstructions"`
	ReasoningEffort      string `json:"reasoningEffort,omitempty"`
}

type modeHeader struct {
	Key         string `json:"key"`
	Label       string `json:"label"`
	Description string `json:"description"`
	Color       string `json:"color"`
}

type callObject struct {
	position int
	body     string
}

func (mode *AgentMode) Metadata() map[string]any {
	value := map[string]any{
		"key":                  mode.Key,
		"label":                mode.Label,
		"pluginName":           mode.PluginName,
		"pluginPath":           mode.PluginPath,
		"pluginRepositoryName": mode.PluginRepositoryName,
		"pluginScope":          mode.PluginScope,
	}
	if mode.Description != "" {
		value["description"] = mode.Description
	}
	if mode.Color != "" {
		value["color"] = mode.Color
	}
	return value
}

func (mode *AgentMode) AgentDefinition() map[string]any {
	definition := map[string]any{
		"kind":         "agent-definition",
		"agentMode":    mode.Key,
		"name":         mode.AgentName,
		"model":        mode.AgentModel,
		"instructions": mode.AgentInstructions,
	}
	if mode.AgentTools != nil {
		definition["tools"] = mode.AgentTools
	}
	if mode.ReasoningEffort != "" {
		definition["reasoningEffort"] = mode.ReasoningEffort
	}
	return definition
}

func DefaultPluginsDir() string {
	if configHome := strings.TrimSpace(os.Getenv("XDG_CONFIG_HOME")); configHome != "" {
		return filepath.Join(configHome, "amp", "plugins")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".config", "amp", "plugins")
}

func Discover(dir string, reserved func(string) bool) ([]*AgentMode, error) {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return nil, nil
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read amp user plugins directory %s: %w", dir, err)
	}
	modes := make([]*AgentMode, 0)
	seen := map[string]string{}
	for _, entry := range entries {
		if entry.IsDir() || !pluginSourceExtension(filepath.Ext(entry.Name())) {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			log.WithError(err).Warnf("amp plugin agent modes: read %s", path)
			continue
		}
		parsed, err := ParseSources(string(data), reserved)
		if err != nil {
			if strings.Contains(string(data), modeHeaderMarker) {
				log.WithError(err).Warnf("amp plugin agent modes: parse %s", path)
			}
			continue
		}
		for _, mode := range parsed {
			if previous := seen[mode.Key]; previous != "" {
				log.Warnf("amp plugin agent modes: duplicate key %q in %s and %s", mode.Key, previous, path)
				continue
			}
			mode.PluginName = strings.TrimSuffix(entry.Name(), filepath.Ext(entry.Name()))
			mode.PluginPath = pluginDisplayPath(path)
			mode.PluginRepositoryName = UserRepositoryName
			mode.PluginScope = UserScope
			seen[mode.Key] = path
			modes = append(modes, mode)
		}
	}
	return modes, nil
}

func pluginSourceExtension(extension string) bool {
	switch strings.ToLower(extension) {
	case ".ts", ".js", ".mts", ".mjs", ".cts", ".cjs":
		return true
	default:
		return false
	}
}

func pluginDisplayPath(path string) string {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return path
	}
	rel, err := filepath.Rel(home, path)
	if err != nil || rel == "." || strings.HasPrefix(rel, "..") {
		return path
	}
	return "~/" + filepath.ToSlash(rel)
}

func ParseSource(src string, reserved func(string) bool) (*AgentMode, error) {
	modes, err := ParseSources(src, reserved)
	if err != nil {
		return nil, err
	}
	if len(modes) != 1 {
		return nil, fmt.Errorf("expected exactly one agent mode, found %d", len(modes))
	}
	return modes[0], nil
}

func ParseSources(src string, reserved func(string) bool) ([]*AgentMode, error) {
	src = strings.ReplaceAll(src, "\r\n", "\n")
	src = strings.ReplaceAll(src, "\r", "\n")
	headers, err := pluginParseModeHeaders(src)
	if err != nil {
		return nil, err
	}
	if len(headers) == 0 {
		return nil, nil
	}
	registrations, err := pluginParseModeRegistrations(src)
	if err != nil {
		return nil, err
	}
	if len(registrations) != len(headers) {
		return nil, fmt.Errorf("found %d agent mode headers and %d registrations", len(headers), len(registrations))
	}
	byKey := make(map[string]*AgentMode, len(registrations))
	for _, mode := range registrations {
		if mode.Key == "" {
			return nil, fmt.Errorf("agent mode registration requires key")
		}
		if !KeyPattern.MatchString(mode.Key) {
			return nil, fmt.Errorf("invalid agent mode key %q", mode.Key)
		}
		if reserved != nil && reserved(mode.Key) {
			return nil, fmt.Errorf("agent mode key %q is reserved", mode.Key)
		}
		if byKey[mode.Key] != nil {
			return nil, fmt.Errorf("duplicate agent mode registration %q", mode.Key)
		}
		byKey[mode.Key] = mode
	}
	out := make([]*AgentMode, 0, len(headers))
	for _, header := range headers {
		mode := byKey[header.Key]
		if mode == nil {
			return nil, fmt.Errorf("agent mode header %q has no matching registration", header.Key)
		}
		if mode.Label == "" {
			mode.Label = header.Label
		}
		if mode.Label != header.Label {
			return nil, fmt.Errorf("agent mode registration %q/%q does not match header %q/%q", mode.Key, mode.Label, header.Key, header.Label)
		}
		if mode.Description == "" {
			mode.Description = header.Description
		}
		if mode.Color == "" {
			mode.Color = header.Color
		}
		out = append(out, mode)
	}
	return out, nil
}

func pluginParseModeHeaders(src string) ([]modeHeader, error) {
	headers := make([]modeHeader, 0)
	seen := map[string]bool{}
	for _, line := range strings.Split(src, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, modeHeaderMarker) {
			continue
		}
		header := modeHeader{}
		if err := json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(line, modeHeaderMarker))), &header); err != nil {
			return nil, fmt.Errorf("invalid agent mode header metadata: %w", err)
		}
		header.Key = strings.ToLower(strings.TrimSpace(header.Key))
		header.Label = strings.TrimSpace(header.Label)
		if header.Key == "" || header.Label == "" {
			return nil, fmt.Errorf("agent mode header requires key and label")
		}
		if seen[header.Key] {
			return nil, fmt.Errorf("duplicate agent mode header %q", header.Key)
		}
		seen[header.Key] = true
		headers = append(headers, header)
	}
	return headers, nil
}

func pluginParseModeRegistrations(src string) ([]*AgentMode, error) {
	registrationCalls, err := pluginCallObjects(src, "registerAgentMode")
	if err != nil {
		return nil, err
	}
	agentCalls, err := pluginCallObjects(src, "createAgent")
	if err != nil {
		return nil, err
	}
	searchable, err := pluginJSSearchableSource(src)
	if err != nil {
		return nil, err
	}
	modes := make([]*AgentMode, 0, len(registrationCalls))
	for _, call := range registrationCalls {
		agentVariable, err := pluginRegistrationAgentVariable(call.body)
		if err != nil {
			return nil, err
		}
		agentCall, ok := pluginResolveAgentCall(searchable, agentCalls, call.position, agentVariable)
		if !ok {
			return nil, fmt.Errorf("registerAgentMode agent %q must be assigned from amp.createAgent", agentVariable)
		}
		mode, err := pluginParseAgentCall(src, agentCall)
		if err != nil {
			return nil, err
		}
		copyMode := *mode
		for key, destination := range map[string]*string{
			"key": &copyMode.Key, "label": &copyMode.Label, "description": &copyMode.Description, "color": &copyMode.Color,
		} {
			if raw, ok := pluginObjectValue(call.body, key); ok {
				value, err := pluginResolveStringAt(src, raw, call.position)
				if err != nil {
					return nil, fmt.Errorf("registerAgentMode %s: %w", key, err)
				}
				*destination = value
			}
		}
		copyMode.Key = strings.ToLower(strings.TrimSpace(copyMode.Key))
		copyMode.Label = strings.TrimSpace(copyMode.Label)
		modes = append(modes, &copyMode)
	}
	return modes, nil
}

func pluginRegistrationAgentVariable(body string) (string, error) {
	agentVariable := ""
	if raw, ok := pluginObjectValue(body, "agent"); ok {
		agentVariable = strings.TrimSpace(raw)
		agentVariable = strings.TrimSuffix(agentVariable, ".definition")
	} else if pluginObjectShorthand(body, "agent") {
		agentVariable = "agent"
	}
	if !pluginIdentifierRe.MatchString(agentVariable) {
		return "", fmt.Errorf("registerAgentMode must reference a createAgent variable")
	}
	return agentVariable, nil
}

func pluginResolveAgentCall(searchable string, calls []callObject, usePosition int, variable string) (callObject, bool) {
	useScope := pluginScopeStack(searchable, usePosition)
	useFunctionScope := pluginVarScopeStack(searchable, usePosition)
	bestDepth := -1
	bestPosition := -1
	var best callObject
	for _, call := range calls {
		if call.position >= usePosition {
			continue
		}
		assignment := pluginAssignmentRe.FindStringSubmatchIndex(searchable[:call.position])
		if assignment == nil || searchable[assignment[4]:assignment[5]] != variable {
			continue
		}
		if pluginPropertyAssignment(searchable, assignment[0]) {
			continue
		}
		declarationScope := pluginScopeStack(searchable, call.position)
		if assignment[2] >= 0 {
			if searchable[assignment[2]:assignment[3]] == "var" {
				declarationScope = pluginVarScopeStack(searchable, call.position)
			}
		} else {
			if !slices.Equal(pluginVarScopeStack(searchable, call.position), useFunctionScope) {
				continue
			}
			var ok bool
			declarationScope, ok = pluginReassignmentScope(searchable, assignment[0], variable)
			if !ok {
				continue
			}
		}
		if !pluginScopeVisible(declarationScope, useScope) {
			continue
		}
		if len(declarationScope) > bestDepth || len(declarationScope) == bestDepth && call.position > bestPosition {
			best = call
			bestDepth = len(declarationScope)
			bestPosition = call.position
		}
	}
	return best, bestDepth >= 0
}

func pluginPropertyAssignment(searchable string, assignmentStart int) bool {
	for assignmentStart > 0 {
		assignmentStart--
		switch searchable[assignmentStart] {
		case ' ', '\t', '\n', '\r':
			continue
		case '.':
			return true
		}
		break
	}
	return false
}

func pluginReassignmentScope(searchable string, assignmentPosition int, variable string) ([]int, bool) {
	declarationRe := regexp.MustCompile(`\b(const|let|var)\s+` + regexp.QuoteMeta(variable) + `\s*=`)
	assignmentScope := pluginScopeStack(searchable, assignmentPosition)
	assignmentFunctionScope := pluginVarScopeStack(searchable, assignmentPosition)
	bestDepth := -1
	bestPosition := -1
	var best []int
	for _, match := range declarationRe.FindAllStringSubmatchIndex(searchable[:assignmentPosition], -1) {
		declarationScope := pluginScopeStack(searchable, match[0])
		if searchable[match[2]:match[3]] == "var" {
			declarationScope = pluginVarScopeStack(searchable, match[0])
		}
		if !pluginScopeVisible(declarationScope, assignmentScope) || !slices.Equal(pluginVarScopeStack(searchable, match[0]), assignmentFunctionScope) {
			continue
		}
		if len(declarationScope) > bestDepth || len(declarationScope) == bestDepth && match[0] > bestPosition {
			best = declarationScope
			bestDepth = len(declarationScope)
			bestPosition = match[0]
		}
	}
	return best, bestDepth >= 0
}

func pluginParseAgentCall(src string, call callObject) (*AgentMode, error) {
	mode := &AgentMode{}
	var err error
	if raw, ok := pluginObjectValue(call.body, "name"); ok {
		mode.AgentName, err = pluginResolveStringAt(src, raw, call.position)
		if err != nil {
			return nil, fmt.Errorf("createAgent name: %w", err)
		}
	}
	if raw, ok := pluginObjectValue(call.body, "model"); ok {
		mode.AgentModel, err = pluginResolveStringAt(src, raw, call.position)
		if err != nil {
			return nil, fmt.Errorf("createAgent model: %w", err)
		}
	}
	if raw, ok := pluginObjectValue(call.body, "instructions"); ok {
		mode.AgentInstructions, err = pluginResolveStringAt(src, raw, call.position)
		if err != nil {
			return nil, fmt.Errorf("createAgent instructions: %w", err)
		}
	}
	if raw, ok := pluginObjectValue(call.body, "tools"); ok {
		mode.AgentTools, err = pluginParseToolSelector(raw)
		if err != nil {
			return nil, fmt.Errorf("createAgent tools: %w", err)
		}
	}
	if raw, ok := pluginObjectValue(call.body, "reasoningEffort"); ok {
		mode.ReasoningEffort, err = pluginResolveStringAt(src, raw, call.position)
		if err != nil {
			return nil, fmt.Errorf("createAgent reasoningEffort: %w", err)
		}
		if NormalizeReasoningEffort(mode.ReasoningEffort) != mode.ReasoningEffort {
			return nil, fmt.Errorf("unsupported reasoning effort %q", mode.ReasoningEffort)
		}
	}
	if mode.AgentModel == "" || mode.AgentInstructions == "" {
		return nil, fmt.Errorf("custom agent requires model and instructions")
	}
	return mode, nil
}

func pluginCallObjects(src, name string) ([]callObject, error) {
	callRe := pluginCallRe[name]
	if callRe == nil {
		return nil, fmt.Errorf("unsupported plugin call %q", name)
	}
	searchable, err := pluginJSSearchableSource(src)
	if err != nil {
		return nil, err
	}
	matches := callRe.FindAllStringIndex(searchable, -1)
	calls := make([]callObject, 0, len(matches))
	for _, match := range matches {
		bodyStart := match[1]
		bodyEnd, err := pluginObjectEnd(src, bodyStart)
		if err != nil {
			return nil, fmt.Errorf("amp.%s: %w", name, err)
		}
		rest := strings.TrimLeft(src[bodyEnd+1:], " \t\n")
		if !strings.HasPrefix(rest, ")") {
			return nil, fmt.Errorf("amp.%s call must close the object literal with })", name)
		}
		calls = append(calls, callObject{position: match[0], body: src[bodyStart:bodyEnd]})
	}
	return calls, nil
}

func pluginJSSearchableSource(src string) (string, error) {
	searchable := []byte(src)
	for index := 0; index < len(src); {
		end := index + 1
		switch src[index] {
		case '\'', '"', '`':
			var err error
			end, err = pluginSkipJSLiteral(src, index)
			if err != nil {
				return "", err
			}
		case '/':
			var err error
			var ok bool
			end, ok, err = pluginSkipJSSlash(src, index)
			if err != nil {
				return "", err
			}
			if !ok {
				end = index + 1
			}
		}
		if end > index+1 {
			for masked := index; masked < end; masked++ {
				if searchable[masked] != '\n' && searchable[masked] != '\r' {
					searchable[masked] = ' '
				}
			}
		}
		index = end
	}
	return string(searchable), nil
}

func pluginScopeStack(searchable string, position int) []int {
	if position > len(searchable) {
		position = len(searchable)
	}
	stack := make([]int, 0, 4)
	for index := 0; index < position; index++ {
		switch searchable[index] {
		case '{':
			stack = append(stack, index)
		case '}':
			if len(stack) > 0 {
				stack = stack[:len(stack)-1]
			}
		}
	}
	return stack
}

func pluginScopeVisible(declaration, use []int) bool {
	if len(declaration) > len(use) {
		return false
	}
	for index := range declaration {
		if declaration[index] != use[index] {
			return false
		}
	}
	return true
}

func pluginVarScopeStack(searchable string, position int) []int {
	stack := pluginScopeStack(searchable, position)
	for index := len(stack) - 1; index >= 0; index-- {
		brace := stack[index]
		if pluginFunctionBodyBraceRe.MatchString(searchable[:brace]) {
			return append([]int(nil), stack[:index+1]...)
		}
	}
	return nil
}

func pluginObjectEnd(src string, bodyStart int) (int, error) {
	depth := 1
	for index := bodyStart; index < len(src); index++ {
		switch src[index] {
		case '\'', '"', '`':
			next, err := pluginSkipJSLiteral(src, index)
			if err != nil {
				return 0, err
			}
			index = next - 1
		case '/':
			next, ok, err := pluginSkipJSSlash(src, index)
			if err != nil {
				return 0, err
			}
			if ok {
				index = next - 1
			}
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return index, nil
			}
		}
	}
	return 0, fmt.Errorf("object literal is unterminated")
}

func pluginObjectValue(body, key string) (string, bool) {
	start := pluginObjectValueStart(body, key)
	if start < 0 {
		return "", false
	}
	for start < len(body) && (body[start] == ' ' || body[start] == '\t' || body[start] == '\n') {
		start++
	}
	depth := 0
	for index := start; index < len(body); index++ {
		switch body[index] {
		case '\'', '"', '`':
			next, err := pluginSkipJSLiteral(body, index)
			if err != nil {
				return "", false
			}
			index = next - 1
		case '/':
			next, ok, err := pluginSkipJSSlash(body, index)
			if err != nil {
				return "", false
			}
			if ok {
				index = next - 1
			}
		case '[', '{', '(':
			depth++
		case ']', '}', ')':
			depth--
		case ',':
			if depth == 0 {
				return strings.TrimSpace(body[start:index]), true
			}
		}
	}
	value := strings.TrimSpace(body[start:])
	return value, value != ""
}

func pluginObjectValueStart(body, key string) int {
	depth := 0
	for index := 0; index < len(body); index++ {
		switch body[index] {
		case '\'', '"', '`':
			next, err := pluginSkipJSLiteral(body, index)
			if err != nil {
				return -1
			}
			if depth == 0 {
				property, _, parseErr := pluginParseJSLiteral(body[index:next])
				colon := pluginPropertyColon(body, next)
				if parseErr == nil && property == key && colon >= 0 {
					return colon + 1
				}
			}
			index = next - 1
		case '/':
			next, ok, err := pluginSkipJSSlash(body, index)
			if err != nil {
				return -1
			}
			if ok {
				index = next - 1
			}
		case '[', '{', '(':
			depth++
		case ']', '}', ')':
			depth--
		default:
			if depth != 0 || !pluginIdentifierStart(body[index]) {
				continue
			}
			end := index + 1
			for end < len(body) && pluginIdentifierPart(body[end]) {
				end++
			}
			colon := pluginPropertyColon(body, end)
			if body[index:end] == key && colon >= 0 {
				return colon + 1
			}
			index = end - 1
		}
	}
	return -1
}

func pluginPropertyColon(body string, start int) int {
	for start < len(body) && (body[start] == ' ' || body[start] == '\t' || body[start] == '\n') {
		start++
	}
	if start < len(body) && body[start] == ':' {
		return start
	}
	return -1
}

func pluginIdentifierStart(value byte) bool {
	return value == '_' || value == '$' || value >= 'A' && value <= 'Z' || value >= 'a' && value <= 'z'
}

func pluginIdentifierPart(value byte) bool {
	return pluginIdentifierStart(value) || value >= '0' && value <= '9'
}

func pluginObjectShorthand(body, key string) bool {
	pattern := regexp.MustCompile(`(?:^|,)\s*` + regexp.QuoteMeta(key) + `\s*(?:,|$)`)
	return pattern.MatchString(body)
}

func pluginResolveString(src, raw string) (string, error) {
	return pluginResolveStringAt(src, raw, len(src))
}

func pluginResolveStringAt(src, raw string, usePosition int) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("string value is empty")
	}
	if strings.ContainsRune("'\"`", rune(raw[0])) {
		value, consumed, err := pluginParseJSLiteral(raw)
		if err != nil {
			return "", err
		}
		if strings.TrimSpace(raw[consumed:]) != "" {
			return "", fmt.Errorf("unsupported string expression %q", raw)
		}
		return value, nil
	}
	if !pluginIdentifierRe.MatchString(raw) {
		return "", fmt.Errorf("unsupported string expression %q", raw)
	}
	searchable, err := pluginJSSearchableSource(src)
	if err != nil {
		return "", err
	}
	declarationRe := regexp.MustCompile(`\bconst\s+` + regexp.QuoteMeta(raw) + `\s*=`)
	matches := declarationRe.FindAllStringIndex(searchable, -1)
	if usePosition > len(searchable) {
		usePosition = len(searchable)
	}
	useScope := pluginScopeStack(searchable, usePosition)
	bestDepth := -1
	bestPosition := -1
	var match []int
	for _, candidate := range matches {
		declarationScope := pluginScopeStack(searchable, candidate[0])
		if !pluginScopeVisible(declarationScope, useScope) {
			continue
		}
		if candidate[0] >= usePosition && len(declarationScope) >= len(useScope) {
			continue
		}
		if len(declarationScope) > bestDepth || len(declarationScope) == bestDepth && candidate[0] < usePosition && candidate[0] > bestPosition {
			match = candidate
			bestDepth = len(declarationScope)
			bestPosition = candidate[0]
		}
	}
	if match == nil {
		return "", fmt.Errorf("unresolved string constant %s", raw)
	}
	initializer := strings.TrimLeft(src[match[1]:], " \t")
	value, consumed, err := pluginParseJSLiteral(initializer)
	if err != nil {
		return "", fmt.Errorf("string constant %s: %w", raw, err)
	}
	remaining := initializer[consumed:]
	sawNewline := false
	for {
		trimmed := strings.TrimLeft(remaining, " \t")
		if len(trimmed) < len(remaining) {
			remaining = trimmed
		}
		if strings.HasPrefix(remaining, "//") {
			if newline := strings.IndexByte(remaining, '\n'); newline >= 0 {
				remaining = remaining[newline:]
				sawNewline = true
			} else {
				remaining = ""
			}
			continue
		}
		if strings.HasPrefix(remaining, "/*") {
			end := strings.Index(remaining[2:], "*/")
			if end < 0 {
				return "", fmt.Errorf("string constant %s: block comment is unterminated", raw)
			}
			if strings.ContainsRune(remaining[:end+4], '\n') {
				sawNewline = true
			}
			remaining = remaining[end+4:]
			continue
		}
		if strings.HasPrefix(remaining, "\n") {
			sawNewline = true
			remaining = strings.TrimLeft(remaining, "\n \t")
			continue
		}
		break
	}
	if remaining != "" && remaining[0] != ';' && (!sawNewline || strings.ContainsRune(".+-*/%&|^<>=!?[(", rune(remaining[0]))) {
		return "", fmt.Errorf("unsupported string expression for constant %s", raw)
	}
	return value, nil
}

func pluginParseJSLiteral(raw string) (string, int, error) {
	if raw == "" || !strings.ContainsRune("'\"`", rune(raw[0])) {
		return "", 0, fmt.Errorf("expected a string literal")
	}
	end, err := pluginSkipJSLiteral(raw, 0)
	if err != nil {
		return "", 0, err
	}
	quote := raw[0]
	content := raw[1 : end-1]
	if quote == '`' && strings.Contains(content, "${") {
		return "", 0, fmt.Errorf("template literal interpolation is unsupported")
	}
	var value strings.Builder
	for index := 0; index < len(content); index++ {
		if content[index] != '\\' {
			value.WriteByte(content[index])
			continue
		}
		index++
		if index >= len(content) {
			return "", 0, fmt.Errorf("string literal has a trailing escape")
		}
		switch content[index] {
		case 'n':
			value.WriteByte('\n')
		case 'r':
			value.WriteByte('\r')
		case 't':
			value.WriteByte('\t')
		case 'b':
			value.WriteByte('\b')
		case 'f':
			value.WriteByte('\f')
		case 'v':
			value.WriteByte('\v')
		case '\\', '\'', '"', '`':
			value.WriteByte(content[index])
		case '\n':
		default:
			return "", 0, fmt.Errorf("unsupported string escape \\%c", content[index])
		}
	}
	return value.String(), end, nil
}

func pluginSkipJSLiteral(src string, start int) (int, error) {
	if start >= len(src) {
		return 0, fmt.Errorf("string literal is unterminated")
	}
	quote := src[start]
	for index := start + 1; index < len(src); index++ {
		if src[index] == '\\' {
			index++
			continue
		}
		if src[index] == quote {
			return index + 1, nil
		}
	}
	return 0, fmt.Errorf("string literal is unterminated")
}

func pluginSkipJSComment(src string, start int) (int, bool, error) {
	if start+1 >= len(src) || src[start] != '/' {
		return start, false, nil
	}
	switch src[start+1] {
	case '/':
		if end := strings.IndexByte(src[start+2:], '\n'); end >= 0 {
			return start + end + 3, true, nil
		}
		return len(src), true, nil
	case '*':
		end := strings.Index(src[start+2:], "*/")
		if end < 0 {
			return 0, true, fmt.Errorf("block comment is unterminated")
		}
		return start + end + 4, true, nil
	default:
		return start, false, nil
	}
}

func pluginSkipJSSlash(src string, start int) (int, bool, error) {
	if end, ok, err := pluginSkipJSComment(src, start); ok || err != nil {
		return end, ok, err
	}
	if !pluginRegexLiteralStart(src, start) {
		return start, false, nil
	}
	inClass := false
	for index := start + 1; index < len(src); index++ {
		switch src[index] {
		case '\\':
			index++
		case '\n', '\r':
			return 0, true, fmt.Errorf("regular expression literal is unterminated")
		case '[':
			inClass = true
		case ']':
			inClass = false
		case '/':
			if inClass {
				continue
			}
			end := index + 1
			for end < len(src) && pluginIdentifierPart(src[end]) {
				end++
			}
			return end, true, nil
		}
	}
	return 0, true, fmt.Errorf("regular expression literal is unterminated")
}

func pluginRegexLiteralStart(src string, start int) bool {
	if start < 0 || start >= len(src) || src[start] != '/' || start+1 >= len(src) || src[start+1] == '=' {
		return false
	}
	previous := pluginPreviousSignificantByte(src, start)
	if previous < 0 {
		return true
	}
	if strings.ContainsRune("=([{,:;!?&|+-*%^~<>", rune(src[previous])) {
		return true
	}
	if !pluginIdentifierPart(src[previous]) {
		return false
	}
	startWord := previous
	for startWord > 0 && pluginIdentifierPart(src[startWord-1]) {
		startWord--
	}
	switch src[startWord : previous+1] {
	case "await", "case", "delete", "do", "else", "in", "instanceof", "of", "return", "throw", "typeof", "void", "yield":
		return true
	default:
		return false
	}
}

func pluginPreviousSignificantByte(src string, start int) int {
	previous := start - 1
	for previous >= 0 {
		for previous >= 0 && (src[previous] == ' ' || src[previous] == '\t' || src[previous] == '\n' || src[previous] == '\r') {
			previous--
		}
		if previous < 1 || src[previous] != '/' || src[previous-1] != '*' {
			return previous
		}
		commentStart := strings.LastIndex(src[:previous-1], "/*")
		if commentStart < 0 {
			return previous
		}
		previous = commentStart - 1
	}
	return -1
}

func pluginParseToolSelector(raw string) (any, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("tool selector is empty")
	}
	if strings.ContainsRune("'\"", rune(raw[0])) {
		return pluginResolveString("", raw)
	}
	if raw[0] == '[' {
		if !strings.HasSuffix(raw, "]") {
			return nil, fmt.Errorf("tool selector array is unterminated")
		}
		parts, err := pluginSplitTopLevel(raw[1 : len(raw)-1])
		if err != nil {
			return nil, err
		}
		values := make([]any, 0, len(parts))
		for _, part := range parts {
			if strings.TrimSpace(part) == "" {
				continue
			}
			value, err := pluginResolveString("", part)
			if err != nil {
				return nil, err
			}
			values = append(values, value)
		}
		return values, nil
	}
	if raw[0] == '{' {
		if !strings.HasSuffix(raw, "}") {
			return nil, fmt.Errorf("tool selector object is unterminated")
		}
		body := raw[1 : len(raw)-1]
		selector := map[string]any{}
		for _, key := range []string{"include", "exclude"} {
			value, ok := pluginObjectValue(body, key)
			if !ok {
				continue
			}
			parsed, err := pluginParseToolSelector(value)
			if err != nil {
				return nil, err
			}
			selector[key] = parsed
		}
		if len(selector) == 0 {
			return nil, fmt.Errorf("tool selector object requires include or exclude")
		}
		return selector, nil
	}
	return nil, fmt.Errorf("unsupported tool selector %q", raw)
}

func pluginSplitTopLevel(value string) ([]string, error) {
	parts := make([]string, 0)
	start := 0
	depth := 0
	for index := 0; index < len(value); index++ {
		switch value[index] {
		case '\'', '"', '`':
			next, err := pluginSkipJSLiteral(value, index)
			if err != nil {
				return nil, err
			}
			index = next - 1
		case '/':
			next, ok, err := pluginSkipJSSlash(value, index)
			if err != nil {
				return nil, err
			}
			if ok {
				index = next - 1
			}
		case '[', '{', '(':
			depth++
		case ']', '}', ')':
			depth--
		case ',':
			if depth == 0 {
				parts = append(parts, value[start:index])
				start = index + 1
			}
		}
		if depth < 0 {
			return nil, fmt.Errorf("unbalanced tool selector")
		}
	}
	if depth != 0 {
		return nil, fmt.Errorf("unbalanced tool selector")
	}
	parts = append(parts, value[start:])
	return parts, nil
}
