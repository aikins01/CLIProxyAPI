package amp

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
	neoDeepOneAgentModeKey      = "deep-1"
	neoDeepTwoAgentModeKey      = "deep-2"
	neoDeepThreeAgentModeKey    = "deep-3"
	neoDeepRedAgentModeKey      = "deep-red"
	neoDeepRedReasoningEffort   = "max"
	neoPluginModeHeaderMarker   = "// @amp-agent-mode "
	neoPluginUserRepository     = "amp-user-plugins"
	neoPluginAgentModeUserScope = "user"
)

var neoWebLocalDeepAgentModes = []struct {
	Key         string
	Label       string
	Description string
	Effort      string
}{
	{Key: neoDeepOneAgentModeKey, Label: "Deep 1", Description: "Focused deep reasoning for complex tasks.", Effort: "low"},
	{Key: neoDeepTwoAgentModeKey, Label: "Deep 2", Description: "Extended deep reasoning for difficult tasks.", Effort: "medium"},
	{Key: neoDeepThreeAgentModeKey, Label: "Deep 3", Description: "Maximum local deep reasoning for the hardest tasks.", Effort: "xhigh"},
}

var (
	neoAmpUserPluginsDir = defaultNeoAmpUserPluginsDir
	neoPluginCallRe      = map[string]*regexp.Regexp{
		"createAgent":       regexp.MustCompile(`\bamp\s*\.\s*(?:experimental\s*\.\s*)?createAgent\s*\(\s*\{`),
		"registerAgentMode": regexp.MustCompile(`\bamp\s*\.\s*(?:experimental\s*\.\s*)?registerAgentMode\s*\(\s*\{`),
	}
	neoPluginAssignmentRe        = regexp.MustCompile(`(?s)(?:(const|let|var)\s+)?([A-Za-z_$][A-Za-z0-9_$]*)\s*=\s*$`)
	neoPluginFunctionBodyBraceRe = regexp.MustCompile(`(?s)(?:\bfunction(?:\s+[A-Za-z_$][A-Za-z0-9_$]*)?\s*\([^{}]*\)|=>)\s*$`)
	neoPluginIdentifierRe        = regexp.MustCompile(`^[A-Za-z_$][A-Za-z0-9_$]*$`)
)

type neoPluginAgentMode struct {
	Key                  string
	Label                string
	Description          string
	Color                string
	PluginName           string
	PluginPath           string
	PluginRepositoryName string
	PluginScope          string
	AgentName            string
	AgentModel           string
	AgentTools           any
	AgentInstructions    string
	ReasoningEffort      string
}

type neoPluginModeHeader struct {
	Key         string `json:"key"`
	Label       string `json:"label"`
	Description string `json:"description"`
	Color       string `json:"color"`
}

type neoPluginCallObject struct {
	position int
	body     string
}

func (mode *neoPluginAgentMode) metadata() map[string]any {
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

func (mode *neoPluginAgentMode) agentDefinition() map[string]any {
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

func defaultNeoAmpUserPluginsDir() string {
	if configHome := strings.TrimSpace(os.Getenv("XDG_CONFIG_HOME")); configHome != "" {
		return filepath.Join(configHome, "amp", "plugins")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".config", "amp", "plugins")
}

func neoWebLocalServerPluginAgentModes() ([]any, error) {
	modes := make([]any, 0, len(neoWebLocalDeepAgentModes)+1)
	seen := map[string]bool{}
	for _, mode := range neoWebLocalDeepAgentModes {
		modes = append(modes, map[string]any{
			"key":                  mode.Key,
			"label":                mode.Label,
			"description":          mode.Description,
			"pluginName":           "cliproxy-local-modes",
			"pluginPath":           "local://cliproxy/deep",
			"pluginRepositoryName": "CLIProxyAPI",
			"pluginScope":          neoPluginAgentModeUserScope,
		})
		seen[mode.Key] = true
	}
	pluginModes, err := discoverNeoPluginAgentModes()
	if err != nil {
		return nil, err
	}
	for _, mode := range pluginModes {
		if seen[mode.Key] {
			continue
		}
		modes = append(modes, mode.metadata())
		seen[mode.Key] = true
	}
	return modes, nil
}

func neoWebLocalDeepAgentMode(key string) (string, string, bool) {
	key = strings.ToLower(strings.TrimSpace(key))
	for _, mode := range neoWebLocalDeepAgentModes {
		if mode.Key == key {
			return "deep", mode.Effort, true
		}
	}
	return "", "", false
}

func loadNeoPluginAgentMode(key string) (*neoPluginAgentMode, error) {
	key = strings.ToLower(strings.TrimSpace(key))
	if key == "" {
		return nil, fmt.Errorf("plugin agent mode key is empty")
	}
	modes, err := discoverNeoPluginAgentModes()
	if err != nil {
		return nil, err
	}
	for _, mode := range modes {
		if strings.EqualFold(mode.Key, key) {
			return mode, nil
		}
	}
	return nil, fmt.Errorf("plugin agent mode %q is unavailable", key)
}

func loadNeoDeepRedPluginAgentMode() (*neoPluginAgentMode, error) {
	return loadNeoPluginAgentMode(neoDeepRedAgentModeKey)
}

func discoverNeoPluginAgentModes() ([]*neoPluginAgentMode, error) {
	dir := strings.TrimSpace(neoAmpUserPluginsDir())
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
	modes := make([]*neoPluginAgentMode, 0)
	seen := map[string]string{}
	for _, entry := range entries {
		if entry.IsDir() || !neoPluginSourceExtension(filepath.Ext(entry.Name())) {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			log.WithError(err).Warnf("amp plugin agent modes: read %s", path)
			continue
		}
		parsed, err := parseNeoPluginAgentModeSources(string(data))
		if err != nil {
			if strings.Contains(string(data), neoPluginModeHeaderMarker) {
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
			mode.PluginPath = neoPluginDisplayPath(path)
			mode.PluginRepositoryName = neoPluginUserRepository
			mode.PluginScope = neoPluginAgentModeUserScope
			seen[mode.Key] = path
			modes = append(modes, mode)
		}
	}
	return modes, nil
}

func neoPluginSourceExtension(extension string) bool {
	switch strings.ToLower(extension) {
	case ".ts", ".js", ".mts", ".mjs", ".cts", ".cjs":
		return true
	default:
		return false
	}
}

func neoPluginDisplayPath(path string) string {
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

func parseNeoPluginAgentModeSource(src string) (*neoPluginAgentMode, error) {
	modes, err := parseNeoPluginAgentModeSources(src)
	if err != nil {
		return nil, err
	}
	if len(modes) != 1 {
		return nil, fmt.Errorf("expected exactly one agent mode, found %d", len(modes))
	}
	return modes[0], nil
}

func parseNeoPluginAgentModeSources(src string) ([]*neoPluginAgentMode, error) {
	src = strings.ReplaceAll(src, "\r\n", "\n")
	src = strings.ReplaceAll(src, "\r", "\n")
	headers, err := neoPluginParseModeHeaders(src)
	if err != nil {
		return nil, err
	}
	if len(headers) == 0 {
		return nil, nil
	}
	registrations, err := neoPluginParseModeRegistrations(src)
	if err != nil {
		return nil, err
	}
	if len(registrations) != len(headers) {
		return nil, fmt.Errorf("found %d agent mode headers and %d registrations", len(headers), len(registrations))
	}
	byKey := make(map[string]*neoPluginAgentMode, len(registrations))
	for _, mode := range registrations {
		if mode.Key == "" {
			return nil, fmt.Errorf("agent mode registration requires key")
		}
		if !neoCustomAgentModePattern.MatchString(mode.Key) {
			return nil, fmt.Errorf("invalid agent mode key %q", mode.Key)
		}
		_, _, localDeepMode := neoWebLocalDeepAgentMode(mode.Key)
		if validNeoClientAgentMode(mode.Key) || localDeepMode {
			return nil, fmt.Errorf("agent mode key %q is reserved", mode.Key)
		}
		if byKey[mode.Key] != nil {
			return nil, fmt.Errorf("duplicate agent mode registration %q", mode.Key)
		}
		byKey[mode.Key] = mode
	}
	out := make([]*neoPluginAgentMode, 0, len(headers))
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

func neoPluginParseModeHeaders(src string) ([]neoPluginModeHeader, error) {
	headers := make([]neoPluginModeHeader, 0)
	seen := map[string]bool{}
	for _, line := range strings.Split(src, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, neoPluginModeHeaderMarker) {
			continue
		}
		header := neoPluginModeHeader{}
		if err := json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(line, neoPluginModeHeaderMarker))), &header); err != nil {
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

func neoPluginParseModeRegistrations(src string) ([]*neoPluginAgentMode, error) {
	registrationCalls, err := neoPluginCallObjects(src, "registerAgentMode")
	if err != nil {
		return nil, err
	}
	agentCalls, err := neoPluginCallObjects(src, "createAgent")
	if err != nil {
		return nil, err
	}
	searchable, err := neoPluginJSSearchableSource(src)
	if err != nil {
		return nil, err
	}
	modes := make([]*neoPluginAgentMode, 0, len(registrationCalls))
	for _, call := range registrationCalls {
		agentVariable, err := neoPluginRegistrationAgentVariable(call.body)
		if err != nil {
			return nil, err
		}
		agentCall, ok := neoPluginResolveAgentCall(searchable, agentCalls, call.position, agentVariable)
		if !ok {
			return nil, fmt.Errorf("registerAgentMode agent %q must be assigned from amp.createAgent", agentVariable)
		}
		mode, err := neoPluginParseAgentCall(src, agentCall)
		if err != nil {
			return nil, err
		}
		copyMode := *mode
		for key, destination := range map[string]*string{
			"key": &copyMode.Key, "label": &copyMode.Label, "description": &copyMode.Description, "color": &copyMode.Color,
		} {
			if raw, ok := neoPluginObjectValue(call.body, key); ok {
				value, err := neoPluginResolveStringAt(src, raw, call.position)
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

func neoPluginRegistrationAgentVariable(body string) (string, error) {
	agentVariable := ""
	if raw, ok := neoPluginObjectValue(body, "agent"); ok {
		agentVariable = strings.TrimSpace(raw)
		agentVariable = strings.TrimSuffix(agentVariable, ".definition")
	} else if neoPluginObjectShorthand(body, "agent") {
		agentVariable = "agent"
	}
	if !neoPluginIdentifierRe.MatchString(agentVariable) {
		return "", fmt.Errorf("registerAgentMode must reference a createAgent variable")
	}
	return agentVariable, nil
}

func neoPluginResolveAgentCall(searchable string, calls []neoPluginCallObject, usePosition int, variable string) (neoPluginCallObject, bool) {
	useScope := neoPluginScopeStack(searchable, usePosition)
	useFunctionScope := neoPluginVarScopeStack(searchable, usePosition)
	bestDepth := -1
	bestPosition := -1
	var best neoPluginCallObject
	for _, call := range calls {
		if call.position >= usePosition {
			continue
		}
		assignment := neoPluginAssignmentRe.FindStringSubmatchIndex(searchable[:call.position])
		if assignment == nil || searchable[assignment[4]:assignment[5]] != variable {
			continue
		}
		if neoPluginPropertyAssignment(searchable, assignment[0]) {
			continue
		}
		declarationScope := neoPluginScopeStack(searchable, call.position)
		if assignment[2] >= 0 {
			if searchable[assignment[2]:assignment[3]] == "var" {
				declarationScope = neoPluginVarScopeStack(searchable, call.position)
			}
		} else {
			if !slices.Equal(neoPluginVarScopeStack(searchable, call.position), useFunctionScope) {
				continue
			}
			var ok bool
			declarationScope, ok = neoPluginReassignmentScope(searchable, assignment[0], variable)
			if !ok {
				continue
			}
		}
		if !neoPluginScopeVisible(declarationScope, useScope) {
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

func neoPluginPropertyAssignment(searchable string, assignmentStart int) bool {
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

func neoPluginReassignmentScope(searchable string, assignmentPosition int, variable string) ([]int, bool) {
	declarationRe := regexp.MustCompile(`\b(const|let|var)\s+` + regexp.QuoteMeta(variable) + `\s*=`)
	assignmentScope := neoPluginScopeStack(searchable, assignmentPosition)
	assignmentFunctionScope := neoPluginVarScopeStack(searchable, assignmentPosition)
	bestDepth := -1
	bestPosition := -1
	var best []int
	for _, match := range declarationRe.FindAllStringSubmatchIndex(searchable[:assignmentPosition], -1) {
		declarationScope := neoPluginScopeStack(searchable, match[0])
		if searchable[match[2]:match[3]] == "var" {
			declarationScope = neoPluginVarScopeStack(searchable, match[0])
		}
		if !neoPluginScopeVisible(declarationScope, assignmentScope) || !slices.Equal(neoPluginVarScopeStack(searchable, match[0]), assignmentFunctionScope) {
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

func neoPluginParseAgentCall(src string, call neoPluginCallObject) (*neoPluginAgentMode, error) {
	mode := &neoPluginAgentMode{}
	var err error
	if raw, ok := neoPluginObjectValue(call.body, "name"); ok {
		mode.AgentName, err = neoPluginResolveStringAt(src, raw, call.position)
		if err != nil {
			return nil, fmt.Errorf("createAgent name: %w", err)
		}
	}
	if raw, ok := neoPluginObjectValue(call.body, "model"); ok {
		mode.AgentModel, err = neoPluginResolveStringAt(src, raw, call.position)
		if err != nil {
			return nil, fmt.Errorf("createAgent model: %w", err)
		}
	}
	if raw, ok := neoPluginObjectValue(call.body, "instructions"); ok {
		mode.AgentInstructions, err = neoPluginResolveStringAt(src, raw, call.position)
		if err != nil {
			return nil, fmt.Errorf("createAgent instructions: %w", err)
		}
	}
	if raw, ok := neoPluginObjectValue(call.body, "tools"); ok {
		mode.AgentTools, err = neoPluginParseToolSelector(raw)
		if err != nil {
			return nil, fmt.Errorf("createAgent tools: %w", err)
		}
	}
	if raw, ok := neoPluginObjectValue(call.body, "reasoningEffort"); ok {
		mode.ReasoningEffort, err = neoPluginResolveStringAt(src, raw, call.position)
		if err != nil {
			return nil, fmt.Errorf("createAgent reasoningEffort: %w", err)
		}
		if normalizeNeoProtocolReasoningEffort(mode.ReasoningEffort) != mode.ReasoningEffort {
			return nil, fmt.Errorf("unsupported reasoning effort %q", mode.ReasoningEffort)
		}
	}
	if mode.AgentModel == "" || mode.AgentInstructions == "" {
		return nil, fmt.Errorf("custom agent requires model and instructions")
	}
	return mode, nil
}

func neoPluginCallObjects(src, name string) ([]neoPluginCallObject, error) {
	callRe := neoPluginCallRe[name]
	if callRe == nil {
		return nil, fmt.Errorf("unsupported plugin call %q", name)
	}
	searchable, err := neoPluginJSSearchableSource(src)
	if err != nil {
		return nil, err
	}
	matches := callRe.FindAllStringIndex(searchable, -1)
	calls := make([]neoPluginCallObject, 0, len(matches))
	for _, match := range matches {
		bodyStart := match[1]
		bodyEnd, err := neoPluginObjectEnd(src, bodyStart)
		if err != nil {
			return nil, fmt.Errorf("amp.%s: %w", name, err)
		}
		rest := strings.TrimLeft(src[bodyEnd+1:], " \t\n")
		if !strings.HasPrefix(rest, ")") {
			return nil, fmt.Errorf("amp.%s call must close the object literal with })", name)
		}
		calls = append(calls, neoPluginCallObject{position: match[0], body: src[bodyStart:bodyEnd]})
	}
	return calls, nil
}

func neoPluginJSSearchableSource(src string) (string, error) {
	searchable := []byte(src)
	for index := 0; index < len(src); {
		end := index + 1
		switch src[index] {
		case '\'', '"', '`':
			var err error
			end, err = neoPluginSkipJSLiteral(src, index)
			if err != nil {
				return "", err
			}
		case '/':
			var err error
			var ok bool
			end, ok, err = neoPluginSkipJSSlash(src, index)
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

func neoPluginScopeStack(searchable string, position int) []int {
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

func neoPluginScopeVisible(declaration, use []int) bool {
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

func neoPluginVarScopeStack(searchable string, position int) []int {
	stack := neoPluginScopeStack(searchable, position)
	for index := len(stack) - 1; index >= 0; index-- {
		brace := stack[index]
		if neoPluginFunctionBodyBraceRe.MatchString(searchable[:brace]) {
			return append([]int(nil), stack[:index+1]...)
		}
	}
	return nil
}

func neoPluginObjectEnd(src string, bodyStart int) (int, error) {
	depth := 1
	for index := bodyStart; index < len(src); index++ {
		switch src[index] {
		case '\'', '"', '`':
			next, err := neoPluginSkipJSLiteral(src, index)
			if err != nil {
				return 0, err
			}
			index = next - 1
		case '/':
			next, ok, err := neoPluginSkipJSSlash(src, index)
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

func neoPluginObjectValue(body, key string) (string, bool) {
	start := neoPluginObjectValueStart(body, key)
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
			next, err := neoPluginSkipJSLiteral(body, index)
			if err != nil {
				return "", false
			}
			index = next - 1
		case '/':
			next, ok, err := neoPluginSkipJSSlash(body, index)
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

func neoPluginObjectValueStart(body, key string) int {
	depth := 0
	for index := 0; index < len(body); index++ {
		switch body[index] {
		case '\'', '"', '`':
			next, err := neoPluginSkipJSLiteral(body, index)
			if err != nil {
				return -1
			}
			if depth == 0 {
				property, _, parseErr := neoPluginParseJSLiteral(body[index:next])
				colon := neoPluginPropertyColon(body, next)
				if parseErr == nil && property == key && colon >= 0 {
					return colon + 1
				}
			}
			index = next - 1
		case '/':
			next, ok, err := neoPluginSkipJSSlash(body, index)
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
			if depth != 0 || !neoPluginIdentifierStart(body[index]) {
				continue
			}
			end := index + 1
			for end < len(body) && neoPluginIdentifierPart(body[end]) {
				end++
			}
			colon := neoPluginPropertyColon(body, end)
			if body[index:end] == key && colon >= 0 {
				return colon + 1
			}
			index = end - 1
		}
	}
	return -1
}

func neoPluginPropertyColon(body string, start int) int {
	for start < len(body) && (body[start] == ' ' || body[start] == '\t' || body[start] == '\n') {
		start++
	}
	if start < len(body) && body[start] == ':' {
		return start
	}
	return -1
}

func neoPluginIdentifierStart(value byte) bool {
	return value == '_' || value == '$' || value >= 'A' && value <= 'Z' || value >= 'a' && value <= 'z'
}

func neoPluginIdentifierPart(value byte) bool {
	return neoPluginIdentifierStart(value) || value >= '0' && value <= '9'
}

func neoPluginObjectShorthand(body, key string) bool {
	pattern := regexp.MustCompile(`(?:^|,)\s*` + regexp.QuoteMeta(key) + `\s*(?:,|$)`)
	return pattern.MatchString(body)
}

func neoPluginResolveString(src, raw string) (string, error) {
	return neoPluginResolveStringAt(src, raw, len(src))
}

func neoPluginResolveStringAt(src, raw string, usePosition int) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("string value is empty")
	}
	if strings.ContainsRune("'\"`", rune(raw[0])) {
		value, consumed, err := neoPluginParseJSLiteral(raw)
		if err != nil {
			return "", err
		}
		if strings.TrimSpace(raw[consumed:]) != "" {
			return "", fmt.Errorf("unsupported string expression %q", raw)
		}
		return value, nil
	}
	if !neoPluginIdentifierRe.MatchString(raw) {
		return "", fmt.Errorf("unsupported string expression %q", raw)
	}
	searchable, err := neoPluginJSSearchableSource(src)
	if err != nil {
		return "", err
	}
	declarationRe := regexp.MustCompile(`\bconst\s+` + regexp.QuoteMeta(raw) + `\s*=`)
	matches := declarationRe.FindAllStringIndex(searchable, -1)
	if usePosition > len(searchable) {
		usePosition = len(searchable)
	}
	useScope := neoPluginScopeStack(searchable, usePosition)
	bestDepth := -1
	bestPosition := -1
	var match []int
	for _, candidate := range matches {
		declarationScope := neoPluginScopeStack(searchable, candidate[0])
		if !neoPluginScopeVisible(declarationScope, useScope) {
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
	value, consumed, err := neoPluginParseJSLiteral(initializer)
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

func neoPluginParseJSLiteral(raw string) (string, int, error) {
	if raw == "" || !strings.ContainsRune("'\"`", rune(raw[0])) {
		return "", 0, fmt.Errorf("expected a string literal")
	}
	end, err := neoPluginSkipJSLiteral(raw, 0)
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

func neoPluginSkipJSLiteral(src string, start int) (int, error) {
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

func neoPluginSkipJSComment(src string, start int) (int, bool, error) {
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

func neoPluginSkipJSSlash(src string, start int) (int, bool, error) {
	if end, ok, err := neoPluginSkipJSComment(src, start); ok || err != nil {
		return end, ok, err
	}
	if !neoPluginRegexLiteralStart(src, start) {
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
			for end < len(src) && neoPluginIdentifierPart(src[end]) {
				end++
			}
			return end, true, nil
		}
	}
	return 0, true, fmt.Errorf("regular expression literal is unterminated")
}

func neoPluginRegexLiteralStart(src string, start int) bool {
	if start < 0 || start >= len(src) || src[start] != '/' || start+1 >= len(src) || src[start+1] == '=' {
		return false
	}
	previous := neoPluginPreviousSignificantByte(src, start)
	if previous < 0 {
		return true
	}
	if strings.ContainsRune("=([{,:;!?&|+-*%^~<>", rune(src[previous])) {
		return true
	}
	if !neoPluginIdentifierPart(src[previous]) {
		return false
	}
	startWord := previous
	for startWord > 0 && neoPluginIdentifierPart(src[startWord-1]) {
		startWord--
	}
	switch src[startWord : previous+1] {
	case "await", "case", "delete", "do", "else", "in", "instanceof", "of", "return", "throw", "typeof", "void", "yield":
		return true
	default:
		return false
	}
}

func neoPluginPreviousSignificantByte(src string, start int) int {
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

func neoPluginParseToolSelector(raw string) (any, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("tool selector is empty")
	}
	if strings.ContainsRune("'\"", rune(raw[0])) {
		return neoPluginResolveString("", raw)
	}
	if raw[0] == '[' {
		if !strings.HasSuffix(raw, "]") {
			return nil, fmt.Errorf("tool selector array is unterminated")
		}
		parts, err := neoPluginSplitTopLevel(raw[1 : len(raw)-1])
		if err != nil {
			return nil, err
		}
		values := make([]any, 0, len(parts))
		for _, part := range parts {
			if strings.TrimSpace(part) == "" {
				continue
			}
			value, err := neoPluginResolveString("", part)
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
			value, ok := neoPluginObjectValue(body, key)
			if !ok {
				continue
			}
			parsed, err := neoPluginParseToolSelector(value)
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

func neoPluginSplitTopLevel(value string) ([]string, error) {
	parts := make([]string, 0)
	start := 0
	depth := 0
	for index := 0; index < len(value); index++ {
		switch value[index] {
		case '\'', '"', '`':
			next, err := neoPluginSkipJSLiteral(value, index)
			if err != nil {
				return nil, err
			}
			index = next - 1
		case '/':
			next, ok, err := neoPluginSkipJSSlash(value, index)
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
