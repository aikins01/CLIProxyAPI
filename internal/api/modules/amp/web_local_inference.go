package amp

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
)

const (
	ampWebLocalInferenceCORSContextKey = "amp_web_local_inference_cors"
	ampWebLocalInferenceHeader         = "X-CLIProxyAPI-Web-Local-Inference"
	ampWebLocalInferenceAPIKeyQuery    = "cliproxy-api-key"
	ampWebLocalInferenceDefaultBaseURL = "http://127.0.0.1:8317"
)

var defaultAmpWebLocalInferenceOrigins = []string{
	"https://ampcode.com",
	"https://www.ampcode.com",
}

func (m *AmpModule) webLocalInferenceCORSMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if !m.webLocalInferenceCORSAllowed(c.Request) {
			c.Next()
			return
		}

		origin := strings.TrimSpace(c.Request.Header.Get("Origin"))
		c.Set(ampWebLocalInferenceCORSContextKey, true)
		c.Header("Vary", appendVaryHeader(c.Writer.Header().Get("Vary"), "Origin"))
		c.Header("Access-Control-Allow-Origin", origin)
		c.Header("Access-Control-Allow-Credentials", "true")
		c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		c.Header("Access-Control-Allow-Private-Network", "true")
		c.Header("Access-Control-Allow-Headers", "Authorization, Content-Type, "+ampWebLocalInferenceHeader)
		c.Header("Access-Control-Max-Age", "600")

		if c.Request.Method == http.MethodOptions {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}

		c.Next()
	}
}

func (m *AmpModule) webLocalInferenceQueryAuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.GetBool(ampWebLocalInferenceCORSContextKey) {
			query := c.Request.URL.Query()
			apiKey := strings.TrimSpace(query.Get(ampWebLocalInferenceAPIKeyQuery))
			if apiKey != "" && strings.TrimSpace(c.GetHeader("Authorization")) == "" {
				c.Request.Header.Set("Authorization", "Bearer "+apiKey)
			}
			if _, ok := query[ampWebLocalInferenceAPIKeyQuery]; ok {
				query.Del(ampWebLocalInferenceAPIKeyQuery)
				c.Request.URL.RawQuery = query.Encode()
			}
		}
		c.Next()
	}
}

func (m *AmpModule) webLocalInferenceCORSAllowed(r *http.Request) bool {
	if m == nil || r == nil || r.URL == nil {
		return false
	}
	settings := m.webLocalInferenceSettings()
	if !settings.Enabled {
		return false
	}
	origin := strings.TrimSpace(r.Header.Get("Origin"))
	if origin == "" || !ampWebLocalInferenceOriginAllowed(origin, settings.AllowedOrigins) {
		return false
	}
	return ampWebLocalInferencePath(r.URL.Path)
}

func (m *AmpModule) webLocalInferenceSettings() config.AmpWebLocalInference {
	m.configMu.RLock()
	defer m.configMu.RUnlock()
	if m.lastConfig == nil {
		return config.AmpWebLocalInference{}
	}
	return m.lastConfig.WebLocalInference
}

func ampWebLocalInferenceOriginAllowed(origin string, allowedOrigins []string) bool {
	normalizedOrigin := normalizeAmpWebLocalInferenceOrigin(origin)
	if normalizedOrigin == "" {
		return false
	}
	if allowedOrigins == nil {
		allowedOrigins = defaultAmpWebLocalInferenceOrigins
	}
	for _, allowed := range allowedOrigins {
		if normalizeAmpWebLocalInferenceOrigin(allowed) == normalizedOrigin {
			return true
		}
	}
	return false
}

func normalizeAmpWebLocalInferenceOrigin(origin string) string {
	origin = strings.TrimSpace(origin)
	if origin == "" {
		return ""
	}
	parsed, err := url.Parse(origin)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return ""
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return ""
	}
	if parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return ""
	}
	return strings.ToLower(parsed.Scheme + "://" + parsed.Host)
}

func ampWebLocalInferencePath(path string) bool {
	path = "/" + strings.Trim(path, "/")
	switch {
	case path == "/api/internal":
		return true
	case path == "/ampcode/local-projects.json":
		return true
	case path == "/api/thread-actors" || strings.HasPrefix(path, "/api/thread-actors/"):
		return true
	case ampWebLocalInferenceRemotePath(path):
		return true
	case path == "/metadata":
		return true
	case path == "/actors" || strings.HasPrefix(path, "/actors/"):
		return true
	case path == "/gateway" || strings.HasPrefix(path, "/gateway/"):
		return true
	default:
		return false
	}
}

func ampWebLocalInferenceRemotePath(path string) bool {
	_, _, ok := neoWebLocalRemoteEndpoint(path)
	return ok
}

func AmpWebLocalInferencePath(path string) bool {
	return ampWebLocalInferencePath(path)
}

func appendVaryHeader(existing, value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return existing
	}
	for _, part := range strings.Split(existing, ",") {
		if strings.EqualFold(strings.TrimSpace(part), value) {
			return existing
		}
	}
	if strings.TrimSpace(existing) == "" {
		return value
	}
	return existing + ", " + value
}

func (m *AmpModule) serveWebLocalInferenceUserscript(c *gin.Context) {
	settings := m.webLocalInferenceSettings()
	if !settings.Enabled {
		c.Status(http.StatusNotFound)
		return
	}
	c.Header("Content-Type", "application/javascript; charset=utf-8")
	defaultBaseURL := strings.TrimSpace(settings.BaseURL)
	if defaultBaseURL == "" {
		defaultBaseURL = ampWebLocalInferenceDefaultBaseURL
	}
	c.String(http.StatusOK, ampWebLocalInferenceUserscript(defaultBaseURL, settings.AllowedOrigins))
}

func (m *AmpModule) serveWebLocalProjects(c *gin.Context) {
	if c.Request.Method != http.MethodGet {
		c.JSON(http.StatusMethodNotAllowed, gin.H{"error": "method not allowed"})
		return
	}
	if m == nil || m.neoRuntime == nil || !c.GetBool(ampWebLocalInferenceCORSContextKey) || strings.TrimSpace(c.GetHeader(ampWebLocalInferenceHeader)) == "" {
		c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
		return
	}
	cfg := m.neoThreadConfigSnapshot()
	if cfg == nil || !neoRuntimeEnabled(cfg) {
		c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "projects": m.neoRuntime.reloadNeoWebLocalProjectCache(), "defaultWorkingDirectory": neoDefaultWebLocalWorkingDirectory()})
}

func ampWebLocalInferenceUserscript(defaultBaseURL string, allowedOrigins []string) string {
	baseURL := ampWebLocalInferenceSafeBaseURL(defaultBaseURL)
	userscriptURL := strings.NewReplacer("\r", "", "\n", "").Replace(baseURL + "/ampcode/local-inference.user.js")
	matchLines := ampWebLocalInferenceUserscriptMatches(allowedOrigins)
	return fmt.Sprintf(`// ==UserScript==
// @name CLIProxyAPI Amp Local Inference
// @namespace https://github.com/router-for-me/CLIProxyAPI
// @version 0.1.39
%s
// @updateURL %s
// @downloadURL %s
// @run-at document-start
// @sandbox raw
// @grant none
// ==/UserScript==
(() => {
	"use strict";

	const bridgeHeader = %s;
	const apiKeyStorageKey = "cliproxyapi.ampLocalInference.apiKey";
	const workingDirectoryStorageKey = "cliproxyapi.ampLocalInference.workingDirectory";
	const selectedLocalProjectStorageKey = "cliproxyapi.ampLocalInference.selectedLocalProject";
	const localThreadIDsStorageKey = "cliproxyapi.ampLocalInference.localThreadIDs";
	const threadWorkingDirectoriesStorageKey = "cliproxyapi.ampLocalInference.threadWorkingDirectories";
	const threadSettingsStorageKey = "cliproxyapi.ampLocalInference.threadSettings";
	const localProjectsEndpointPath = "/ampcode/local-projects.json";
	const defaultBaseURL = %s;
	let defaultWorkingDirectory = "";
	const originalJSONParse = globalThis.JSON.parse.bind(globalThis.JSON);
	const originalFetch = globalThis.fetch.bind(globalThis);
	const NativeWebSocket = globalThis.WebSocket;
	const diagnostics = {
		decodedConfigPatchCount: 0,
		fetchRewriteCount: 0,
		webSocketRewriteCount: 0,
		webSocketBootstrapCount: 0,
		webSocketOpenCount: 0,
		webSocketCloseCount: 0,
		webSocketErrorCount: 0,
		activeWebSocketCount: 0,
		menuIntegrationCount: 0,
		commandPaletteIntegrationCount: 0,
		localProjectFetchCount: 0,
		localProjectPickerIntegrationCount: 0,
		localProjectActivatorIntegrationCount: 0,
		localThreadPickerOpenCount: 0,
		removedLocalThreadControlCount: 0,
		remoteShellCreateCount: 0,
		lastPatchedThreadActorBaseURL: "",
		lastPatchedThreadID: "",
		lastLocalThreadNavigationThreadID: "",
		lastLocalThreadAgentMode: "",
		lastLocalThreadReasoningEffort: "",
		lastVisibleThreadModeBadge: "",
		lastVisibleThreadModeAgentMode: "",
		lastVisibleThreadModeReasoningEffort: "",
		lastLocalThreadChoice: "",
		lastLocalProjectChoice: "",
		lastLocalProjectWorkingDirectory: "",
		lastWebSocketHost: "",
		lastWebSocketPath: "",
		lastWebSocketThreadKey: "",
		lastWebSocketBootstrapped: false,
		lastWebSocketProtocols: "",
		lastWebSocketState: "",
		lastWebSocketReadyState: -1,
		lastWebSocketCloseCode: 0,
		lastWebSocketCloseReason: "",
		lastInheritedWorkingDirectory: "",
		lastInheritedWorkingDirectoryThreadID: "",
		lastObservedThreadID: "",
	};
	let observedThreadID = "";
	let localProjectsCache = { at: 0, projects: [], promise: null };

	function localBaseURL() {
		return new URL(defaultBaseURL);
	}

	function localBaseURLString() {
		return localBaseURL().href.replace(/\/+$/, "");
	}

	function storedLocalAPIKey() {
		return globalThis.sessionStorage.getItem(apiKeyStorageKey) || "";
	}

	function localAPIKey(rememberCancel = true) {
		const apiKey = storedLocalAPIKey();
		if (apiKey) {
			return apiKey;
		}
		const promptedKey = "cliproxyapi.ampLocalInference.promptedAPIKey";
		if (rememberCancel && globalThis.sessionStorage.getItem(promptedKey) === "1") {
			return "";
		}
		if (rememberCancel) {
			globalThis.sessionStorage.setItem(promptedKey, "1");
		}
		const promptedAPIKey = (globalThis.prompt("CLIProxyAPI API key") || "").trim();
		if (promptedAPIKey) {
			globalThis.sessionStorage.setItem(apiKeyStorageKey, promptedAPIKey);
		}
		return promptedAPIKey;
	}

	function localProjectLookupAPIKey() {
		const apiKey = storedLocalAPIKey();
		if (apiKey) {
			return apiKey;
		}
		const promptedKey = "cliproxyapi.ampLocalInference.promptedProjectsAPIKey";
		if (globalThis.sessionStorage.getItem(promptedKey) === "1") {
			return "";
		}
		globalThis.sessionStorage.setItem(promptedKey, "1");
		const promptedAPIKey = (globalThis.prompt("CLIProxyAPI API key") || "").trim();
		if (promptedAPIKey) {
			globalThis.sessionStorage.setItem(apiKeyStorageKey, promptedAPIKey);
			globalThis.sessionStorage.removeItem(promptedKey);
		}
		return promptedAPIKey;
	}

	function localWorkingDirectory() {
		const activeDirectory = activeThreadWorkingDirectory();
		if (activeDirectory) {
			return activeDirectory;
		}
		const selectedDirectory = selectedLocalProjectWorkingDirectory();
		if (selectedDirectory) {
			return selectedDirectory;
		}
		return storedLocalWorkingDirectory() || defaultLocalWorkingDirectory();
	}

	function storedLocalWorkingDirectory() {
		return normalizeWorkingDirectory(globalThis.localStorage.getItem(workingDirectoryStorageKey) || "");
	}

	function defaultLocalWorkingDirectory() {
		return normalizeWorkingDirectory(defaultWorkingDirectory);
	}

	function ensureDefaultWorkingDirectory(promptForKey = false) {
		const workingDirectory = defaultLocalWorkingDirectory();
		if (workingDirectory) {
			return Promise.resolve(workingDirectory);
		}
		return fetchLocalProjects(promptForKey).then(() => defaultLocalWorkingDirectory());
	}

	function selectedLocalProjectWorkingDirectory() {
		try {
			const parsed = originalJSONParse(globalThis.sessionStorage.getItem(selectedLocalProjectStorageKey) || "{}");
			const selectedAt = Number(parsed.selectedAt || 0);
			if (!selectedAt || Date.now() - selectedAt > 10 * 60 * 1000) {
				return "";
			}
			return normalizeWorkingDirectory(parsed.workingDirectory);
		} catch {
			return "";
		}
	}

	function rememberSelectedLocalProject(project) {
		const workingDirectory = normalizeWorkingDirectory(project?.workingDirectory);
		if (!workingDirectory) {
			return;
		}
		const selected = {
			id: firstString(project.id, project.projectID, project.projectId, project.project_id),
			name: firstString(project.name, project.projectName, pathBaseName(workingDirectory), "local"),
			workingDirectory,
			selectedAt: Date.now(),
		};
		globalThis.sessionStorage.setItem(selectedLocalProjectStorageKey, JSON.stringify(selected));
		globalThis.localStorage.setItem(workingDirectoryStorageKey, workingDirectory);
		diagnostics.lastLocalProjectChoice = selected.name;
		diagnostics.lastLocalProjectWorkingDirectory = workingDirectory;
	}

	function clearSelectedLocalProject(useDefaultWorkingDirectory = false) {
		globalThis.sessionStorage.removeItem(selectedLocalProjectStorageKey);
		if (useDefaultWorkingDirectory) {
			const workingDirectory = defaultLocalWorkingDirectory();
			if (workingDirectory) {
				globalThis.localStorage.setItem(workingDirectoryStorageKey, workingDirectory);
			} else {
				globalThis.localStorage.removeItem(workingDirectoryStorageKey);
			}
		}
	}

	function activeThreadWorkingDirectory() {
		const threadID = activeThreadID();
		if (!threadID) {
			return "";
		}
		return (threadWorkingDirectories()[threadID] || "").trim();
	}

	function localThreadIDs() {
		try {
			const raw = globalThis.localStorage.getItem(localThreadIDsStorageKey) || "[]";
			const parsed = originalJSONParse(raw);
			return Array.isArray(parsed) ? parsed.filter((value) => typeof value === "string" && value.startsWith("T-")) : [];
		} catch {
			return [];
		}
	}

	function threadWorkingDirectories() {
		try {
			const raw = globalThis.localStorage.getItem(threadWorkingDirectoriesStorageKey) || "{}";
			const parsed = originalJSONParse(raw);
			return isPlainObject(parsed) ? parsed : {};
		} catch {
			return {};
		}
	}

	function threadSettings() {
		try {
			const raw = globalThis.localStorage.getItem(threadSettingsStorageKey) || "{}";
			const parsed = originalJSONParse(raw);
			return isPlainObject(parsed) ? parsed : {};
		} catch {
			return {};
		}
	}

	function defaultReasoningEffort(agentMode) {
		switch (normalizeAgentMode(agentMode)) {
		case "smart":
			return "high";
		case "rush":
		case "agg-man":
			return "none";
		case "deep":
		case "review":
			return "medium";
		case "nostromo":
			return "low";
		default:
			return "";
		}
	}

	function normalizeAgentMode(value) {
		const mode = typeof value === "string" ? value.trim().toLowerCase() : "";
		return ["smart", "large", "rush", "deep", "review", "agg-man", "nostromo"].includes(mode) ? mode : "";
	}

	function normalizeReasoningEffort(agentMode, value) {
		const mode = normalizeAgentMode(agentMode);
		const effort = typeof value === "string" ? value.trim().toLowerCase() : "";
		const allowed = {
			smart: ["high", "xhigh", "max"],
			large: [],
			rush: ["none"],
			deep: ["low", "medium", "xhigh"],
			review: ["low", "medium", "high"],
			"agg-man": ["none"],
			nostromo: ["low"],
		};
		if (allowed[mode]?.includes(effort)) {
			return effort;
		}
		return defaultReasoningEffort(mode);
	}

	function normalizeExplicitReasoningEffort(agentMode, value) {
		const mode = normalizeAgentMode(agentMode);
		const effort = typeof value === "string" ? value.trim().toLowerCase() : "";
		const allowed = {
			smart: ["high", "xhigh", "max"],
			large: [],
			rush: ["none"],
			deep: ["low", "medium", "xhigh"],
			review: ["low", "medium", "high"],
			"agg-man": ["none"],
			nostromo: ["low"],
		};
		return allowed[mode]?.includes(effort) ? effort : "";
	}

	function normalizedThreadSettings(source) {
		if (!isPlainObject(source)) {
			return {};
		}
		const mode = normalizeAgentMode(source.agentMode || source.mode);
		const effort = normalizeExplicitReasoningEffort(mode, source.reasoningEffort || source.reasoning_effort || source["reasoning.effort"]);
		const out = {};
		if (mode) {
			out.agentMode = mode;
		}
		if (effort) {
			out.reasoningEffort = effort;
		}
		return out;
	}

	function rememberThreadSettings(threadID, source) {
		threadID = typeof threadID === "string" ? threadID.trim() : "";
		const settings = normalizedThreadSettings(source);
		if (!threadID || !threadID.startsWith("T-") || (!settings.agentMode && !settings.reasoningEffort)) {
			return;
		}
		const byThread = threadSettings();
		byThread[threadID] = Object.assign({}, byThread[threadID] || {}, settings);
		globalThis.localStorage.setItem(threadSettingsStorageKey, JSON.stringify(byThread));
	}

	function activeThreadSettings() {
		const threadID = activeThreadID();
		if (!threadID) {
			return {};
		}
		return normalizedThreadSettings(threadSettings()[threadID]);
	}

	function localThreadModeOptions(override) {
		const explicit = normalizedThreadSettings(override);
		if (explicit.agentMode) {
			return {
				agentMode: explicit.agentMode,
				reasoningEffort: normalizeReasoningEffort(explicit.agentMode, explicit.reasoningEffort),
		};
		}
		const settings = activeThreadSettings();
		const visible = visibleThreadModeOptions();
		const agentMode = settings.agentMode || visible.agentMode || "smart";
		return {
			agentMode,
			reasoningEffort: normalizeReasoningEffort(agentMode, settings.reasoningEffort || visible.reasoningEffort),
		};
	}

	function localThreadModeChoices() {
		const inherited = localThreadModeOptions();
		return [
			{ id: "inherit", label: "Use current", detail: localThreadModeLabel(inherited), options: {} },
			{ id: "smart-high", label: "Smart 1", detail: "High", options: { agentMode: "smart", reasoningEffort: "high" } },
			{ id: "smart-xhigh", label: "Smart 2", detail: "XHigh", options: { agentMode: "smart", reasoningEffort: "xhigh" } },
			{ id: "smart-max", label: "Smart 3", detail: "Max", options: { agentMode: "smart", reasoningEffort: "max" } },
			{ id: "deep-low", label: "Deep 1", detail: "Low", options: { agentMode: "deep", reasoningEffort: "low" } },
			{ id: "deep-medium", label: "Deep 2", detail: "Medium", options: { agentMode: "deep", reasoningEffort: "medium" } },
			{ id: "deep-xhigh", label: "Deep 3", detail: "XHigh", options: { agentMode: "deep", reasoningEffort: "xhigh" } },
			{ id: "large", label: "Large", detail: "Claude", options: { agentMode: "large" } },
			{ id: "rush", label: "Rush", detail: "Fast", options: { agentMode: "rush", reasoningEffort: "none" } },
			{ id: "nostromo", label: "Nostromo", detail: "Amp", options: { agentMode: "nostromo", reasoningEffort: "low" } },
		];
	}

	function localThreadModeLabel(options) {
		const mode = normalizeAgentMode(options?.agentMode);
		const effort = normalizeReasoningEffort(mode, options?.reasoningEffort);
		const labels = {
			"smart:high": "Smart 1",
			"smart:xhigh": "Smart 2",
			"smart:max": "Smart 3",
			"deep:low": "Deep 1",
			"deep:medium": "Deep 2",
			"deep:xhigh": "Deep 3",
			"rush:none": "Rush",
			"review:medium": "Review",
			"agg-man:none": "Agg-man",
			"nostromo:low": "Nostromo",
			"large:": "Large",
		};
		return labels[mode + ":" + effort] || (mode ? mode[0].toUpperCase() + mode.slice(1) : "Smart 1");
	}

	function visibleThreadModeOptions() {
		if (!globalThis.document?.querySelectorAll) {
			return {};
		}
		const selector = "button,[role='button'],[aria-label],span,div,p";
		for (const element of globalThis.document.querySelectorAll(selector)) {
			if (!elementVisible(element)) {
				continue;
			}
			const text = (element.textContent || "").trim();
			const options = modeOptionsFromBadgeText(text);
			if (options.agentMode) {
				diagnostics.lastVisibleThreadModeBadge = text;
				diagnostics.lastVisibleThreadModeAgentMode = options.agentMode;
				diagnostics.lastVisibleThreadModeReasoningEffort = options.reasoningEffort || "";
				return options;
			}
		}
		return {};
	}

	function modeOptionsFromBadgeText(text) {
		let normalized = typeof text === "string" ? text.trim().toLowerCase() : "";
		normalized = normalized
			.replace(/\u00b9/g, "1")
			.replace(/\u00b2/g, "2")
			.replace(/\u00b3/g, "3")
			.replace(/\s+/g, "");
		const match = normalized.match(/^(smart|large|rush|deep|review|agg-man|nostromo)([123])?$/);
		if (!match) {
			return {};
		}
		const agentMode = normalizeAgentMode(match[1]);
		if (!agentMode) {
			return {};
		}
		return {
			agentMode,
			reasoningEffort: badgeLevelReasoningEffort(agentMode, match[2] || ""),
		};
	}

	function badgeLevelReasoningEffort(agentMode, level) {
		const efforts = {
			smart: { 1: "high", 2: "xhigh", 3: "max" },
			rush: { 1: "none" },
			deep: { 1: "low", 2: "medium", 3: "xhigh" },
			nostromo: { 1: "low" },
		};
		return efforts[normalizeAgentMode(agentMode)]?.[level] || defaultReasoningEffort(agentMode);
	}

	function rememberThreadWorkingDirectory(threadID, workingDirectory) {
		threadID = typeof threadID === "string" ? threadID.trim() : "";
		workingDirectory = normalizeWorkingDirectory(workingDirectory);
		if (!threadID || !threadID.startsWith("T-") || !workingDirectory) {
			return;
		}
		const directories = threadWorkingDirectories();
		directories[threadID] = workingDirectory;
		globalThis.localStorage.setItem(threadWorkingDirectoriesStorageKey, JSON.stringify(directories));
		diagnostics.lastInheritedWorkingDirectory = workingDirectory;
		diagnostics.lastInheritedWorkingDirectoryThreadID = threadID;
	}

	function rememberLocalThreadID(threadID) {
		if (!threadID || !threadID.startsWith("T-")) {
			return;
		}
		const ids = localThreadIDs().filter((value) => value !== threadID);
		ids.unshift(threadID);
		globalThis.localStorage.setItem(localThreadIDsStorageKey, JSON.stringify(ids.slice(0, 100)));
	}

	function rememberedLocalThreadID(threadID) {
		return !!threadID && localThreadIDs().includes(threadID);
	}

	function validThreadID(value) {
		return typeof value === "string" && /^T-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/.test(value.trim());
	}

	function rememberObservedThreadID(threadID) {
		threadID = typeof threadID === "string" ? threadID.trim() : "";
		if (!validThreadID(threadID)) {
			return "";
		}
		observedThreadID = threadID;
		diagnostics.lastObservedThreadID = threadID;
		return threadID;
	}

	function firstThreadIDFromText(value) {
			if (typeof value !== "string") {
				return "";
			}
			for (const part of value.split(",")) {
				const candidate = part.trim();
				if (validThreadID(candidate)) {
					return candidate;
			}
			}
			const match = value.match(/T-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}/);
			return match ? match[0] : "";
		}

	function parseJSONText(value) {
			if (typeof value !== "string" || value.trim() === "") {
				return null;
			}
			try {
				return originalJSONParse(value);
			} catch {
				return null;
			}
		}

	function firstThreadIDFromValue(value, seen = new WeakSet()) {
			if (validThreadID(value)) {
				return value.trim();
			}
			if (typeof value === "string") {
				return firstThreadIDFromText(value);
			}
			if (value === null || typeof value !== "object" || seen.has(value)) {
				return "";
			}
			seen.add(value);
			if (isPlainObject(value)) {
				for (const key of ["threadId", "threadID", "thread_id", "id", "thread"]) {
					const threadID = firstThreadIDFromValue(value[key], seen);
					if (threadID) {
						return threadID;
			}
			}
				const input = typeof value.input === "string" ? parseJSONText(value.input) : value.input;
				if (input !== value.input) {
					const threadID = firstThreadIDFromValue(input, seen);
					if (threadID) {
						return threadID;
			}
			}
			}
			const children = Array.isArray(value) ? value : Object.values(value);
			for (const child of children) {
				const threadID = firstThreadIDFromValue(child, seen);
				if (threadID) {
					return threadID;
			}
			}
			return "";
		}

	function isPlainObject(value) {
		return value !== null && typeof value === "object" && !Array.isArray(value);
	}

	function normalizeWorkingDirectory(value) {
		if (typeof value !== "string") {
			return "";
		}
		value = value.trim();
		if (!value) {
			return "";
		}
		if (value.startsWith("file://")) {
			try {
				const parsed = new URL(value);
				return decodeURIComponent(parsed.pathname || "").trim();
			} catch {
				return "";
			}
		}
		return value;
	}

	function firstWorkingDirectory(...values) {
		for (const value of values) {
			const workingDirectory = normalizeWorkingDirectory(value);
			if (workingDirectory) {
				return workingDirectory;
			}
		}
		return "";
	}

	function firstString(...values) {
		for (const value of values) {
			const text = typeof value === "string" ? value.trim() : "";
			if (text) {
				return text;
			}
		}
		return "";
	}

	function ensureDevalueStringIndex(values, text) {
		for (let i = 0; i < values.length; i += 1) {
			if (values[i] === text) {
				return i;
			}
		}
		values.push(text);
		return values.length - 1;
	}

	function ensureDevalueValueIndex(values, target) {
		for (let i = 0; i < values.length; i += 1) {
			if (values[i] === target) {
				return i;
			}
		}
		values.push(target);
		return values.length - 1;
	}

	function devalueThreadID(values, thread) {
		if (!isPlainObject(thread) || !Number.isInteger(thread.id)) {
			return "";
		}
		const id = values[thread.id];
		return rememberObservedThreadID(id);
	}

	function devalueField(values, object, key) {
		if (!isPlainObject(object) || !Number.isInteger(object[key])) {
			return undefined;
		}
		return values[object[key]];
	}

	function devalueStringField(values, object, key) {
		const value = devalueField(values, object, key);
		return typeof value === "string" ? value : "";
	}

	function devalueObjectField(values, object, key) {
		const value = devalueField(values, object, key);
		return isPlainObject(value) ? value : null;
	}

	function devalueObjectFromRaw(values, raw) {
		const value = Number.isInteger(raw) ? values[raw] : raw;
		return isPlainObject(value) ? value : null;
	}

	function devalueArrayField(values, object, key) {
		const value = devalueField(values, object, key);
		return Array.isArray(value) ? value : [];
	}

	function devalueTreesWorkingDirectory(values, ...containers) {
		for (const container of containers) {
			if (!isPlainObject(container)) {
				continue;
			}
			for (const rawTree of devalueArrayField(values, container, "trees")) {
				const tree = devalueObjectFromRaw(values, rawTree);
				if (!tree) {
					continue;
				}
				const workingDirectory = firstWorkingDirectory(
					devalueStringField(values, tree, "workingDirectory"),
					devalueStringField(values, tree, "workspaceRoot"),
					devalueStringField(values, tree, "uri"),
				);
				if (workingDirectory) {
					return workingDirectory;
				}
			}
		}
		return "";
	}

	function devalueThreadWorkingDirectory(values, thread) {
		const env = devalueObjectField(values, thread, "env") || devalueObjectField(values, thread, "environment") || {};
		const initial = devalueObjectField(values, env, "initial") || {};
		const workspace = devalueObjectField(values, thread, "workspace") || {};
		return firstWorkingDirectory(
			devalueStringField(values, thread, "workingDirectory"),
			devalueStringField(values, thread, "workspaceRoot"),
			devalueStringField(values, env, "workingDirectory"),
			devalueStringField(values, env, "workspaceRoot"),
			devalueStringField(values, initial, "workingDirectory"),
			devalueStringField(values, initial, "workspaceRoot"),
			devalueStringField(values, workspace, "workingDirectory"),
			devalueStringField(values, workspace, "workspaceRoot"),
			devalueStringField(values, workspace, "uri"),
			devalueTreesWorkingDirectory(values, env, initial),
		);
	}

	function devalueThreadSettings(values, thread) {
		const settings = devalueObjectField(values, thread, "settings") || {};
		return {
			agentMode: firstString(
				devalueStringField(values, thread, "agentMode"),
				devalueStringField(values, thread, "mode"),
				devalueStringField(values, settings, "agentMode"),
			),
			reasoningEffort: firstString(
				devalueStringField(values, thread, "reasoningEffort"),
				devalueStringField(values, thread, "reasoning_effort"),
				devalueStringField(values, settings, "reasoning.effort"),
				devalueStringField(values, settings, "reasoningEffort"),
			),
		};
	}

	function rememberDevalueThreadSettingsMessage(values, entry) {
		if (devalueStringField(values, entry, "type") !== "thread_settings") {
			return;
		}
		const settings = devalueObjectField(values, entry, "settings") || entry;
		const threadID = firstString(
			devalueStringField(values, entry, "threadId"),
			devalueStringField(values, entry, "threadID"),
			activeThreadID(),
		);
		rememberThreadSettings(threadID, devalueThreadSettings(values, settings));
	}

	function devalueThreadActorConfigLike(config) {
		return isPlainObject(config) &&
			(typeof config.threadId === "number" ||
				typeof config.baseURL === "number" ||
				typeof config.ampURL === "number" ||
				typeof config.wsToken === "number");
	}

	function devalueThreadActorConfigHasBridgeFields(config) {
		return isPlainObject(config) &&
			(typeof config.baseURL === "number" ||
				typeof config.ampURL === "number" ||
				typeof config.wsToken === "number");
	}

	function patchDevalueThreadActorConfig(values, index, baseIndex) {
		if (!Number.isInteger(index) || index < 0 || index >= values.length) {
			return false;
		}
		const config = values[index];
		if (!devalueThreadActorConfigLike(config)) {
			return false;
		}
		if (Number.isInteger(config.threadId)) {
			const threadID = rememberObservedThreadID(values[config.threadId]);
			if (threadID && threadID === pathThreadID() && devalueThreadActorConfigHasBridgeFields(config)) {
				rememberLocalThreadID(threadID);
			}
		}
		let patched = false;
		if (config.baseURL !== baseIndex) {
			config.baseURL = baseIndex;
			patched = true;
		}
		if (config.ampURL !== baseIndex) {
			config.ampURL = baseIndex;
			patched = true;
		}
		return patched;
	}

	function rememberDevalueThreadRuntime(values, threadIndex) {
		if (!Number.isInteger(threadIndex) || threadIndex < 0 || threadIndex >= values.length) {
			return false;
		}
		const thread = values[threadIndex];
		const activeThread = activeThreadID();
		if (!isPlainObject(thread) || !activeThread || devalueThreadID(values, thread) !== activeThread) {
			return false;
		}
		rememberThreadWorkingDirectory(activeThread, devalueThreadWorkingDirectory(values, thread));
		rememberThreadSettings(activeThread, devalueThreadSettings(values, thread));
		return false;
	}

	function patchDevalueThreadActorConfigs(values, localBase) {
		if (!Array.isArray(values)) {
			return false;
		}
		let patched = false;
		let baseIndex = -1;
		const getBaseIndex = () => {
			if (baseIndex === -1) {
				baseIndex = ensureDevalueStringIndex(values, localBase);
			}
			return baseIndex;
		};
		const originalLength = values.length;
		const activeThread = activeThreadID();
		for (let i = 0; i < originalLength; i += 1) {
			const entry = values[i];
			if (!isPlainObject(entry)) {
				continue;
			}
			if (activeThread && devalueThreadID(values, entry) === activeThread) {
				rememberThreadSettings(activeThread, devalueThreadSettings(values, entry));
				rememberDevalueThreadRuntime(values, i);
			}
			rememberDevalueThreadSettingsMessage(values, entry);
			if (Number.isInteger(entry.threadActorConfig)) {
				const config = values[entry.threadActorConfig];
				if (devalueThreadActorConfigLike(config)) {
					patched = patchDevalueThreadActorConfig(values, entry.threadActorConfig, getBaseIndex()) || patched;
					if (Number.isInteger(entry.thread)) {
						rememberDevalueThreadRuntime(values, entry.thread);
					}
				}
			}
			if (devalueThreadActorConfigLike(entry)) {
				patched = patchDevalueThreadActorConfig(values, i, getBaseIndex()) || patched;
			}
		}
		return patched;
	}

	function plainThreadMatchesActive(thread) {
		const activeThread = activeThreadID();
		return isPlainObject(thread) && !!activeThread && thread.id === activeThread;
	}

	function rememberPlainThreadRuntime(thread) {
		if (!plainThreadMatchesActive(thread)) {
			return false;
		}
		rememberThreadWorkingDirectory(thread.id, plainThreadWorkingDirectory(thread));
		rememberThreadSettings(thread.id, plainThreadSettings(thread));
		return false;
	}

	function plainThreadActorConfigLike(config) {
		return isPlainObject(config) &&
			(typeof config.threadId === "string" ||
				typeof config.baseURL === "string" ||
				typeof config.ampURL === "string" ||
				typeof config.wsToken === "string");
	}

	function plainThreadActorConfigHasBridgeFields(config) {
		return isPlainObject(config) &&
			(typeof config.baseURL === "string" ||
				typeof config.ampURL === "string" ||
				typeof config.wsToken === "string");
	}

	function plainTreesWorkingDirectory(...containers) {
		for (const container of containers) {
			if (!isPlainObject(container) || !Array.isArray(container.trees)) {
				continue;
			}
			for (const tree of container.trees) {
				if (!isPlainObject(tree)) {
					continue;
				}
				const workingDirectory = firstWorkingDirectory(tree.workingDirectory, tree.workspaceRoot, tree.uri);
				if (workingDirectory) {
					return workingDirectory;
				}
			}
		}
		return "";
	}

	function plainThreadWorkingDirectory(thread) {
		const env = isPlainObject(thread.env) ? thread.env : isPlainObject(thread.environment) ? thread.environment : {};
		const initial = isPlainObject(env.initial) ? env.initial : {};
		const workspace = isPlainObject(thread.workspace) ? thread.workspace : {};
		return firstWorkingDirectory(
			thread.workingDirectory,
			thread.workspaceRoot,
			env.workingDirectory,
			env.workspaceRoot,
			initial.workingDirectory,
			initial.workspaceRoot,
			workspace.workingDirectory,
			workspace.workspaceRoot,
			workspace.uri,
			plainTreesWorkingDirectory(env, initial),
		);
	}

	function plainThreadSettings(thread) {
		const settings = isPlainObject(thread.settings) ? thread.settings : {};
		return {
			agentMode: firstString(thread.agentMode, thread.mode, settings.agentMode),
			reasoningEffort: firstString(thread.reasoningEffort, thread.reasoning_effort, thread["reasoning.effort"], settings["reasoning.effort"], settings.reasoningEffort),
		};
	}

	function rememberPlainThreadSettingsMessage(value) {
		if (!isPlainObject(value) || value.type !== "thread_settings") {
			return;
		}
		const threadID = firstString(value.threadId, value.threadID, activeThreadID());
		const settings = isPlainObject(value.settings) ? value.settings : value;
		rememberThreadSettings(threadID, plainThreadSettings(settings));
	}

	function patchPlainThreadActorConfig(config, localBase) {
		if (!plainThreadActorConfigLike(config)) {
			return false;
		}
		const threadID = rememberObservedThreadID(firstString(config.threadId, config.threadID, config.thread_id));
		if (threadID && threadID === pathThreadID() && plainThreadActorConfigHasBridgeFields(config)) {
			rememberLocalThreadID(threadID);
		}
		let patched = false;
		if (config.baseURL !== localBase) {
			config.baseURL = localBase;
			patched = true;
		}
		if (config.ampURL !== localBase) {
			config.ampURL = localBase;
			patched = true;
		}
		return patched;
	}

	function patchPlainThreadActorConfigs(value, seen, localBase) {
		if (value === null || typeof value !== "object" || seen.has(value)) {
			return false;
		}
		seen.add(value);
		let patched = false;
		if (isPlainObject(value.threadActorConfig)) {
			patched = patchPlainThreadActorConfig(value.threadActorConfig, localBase) || patched;
			rememberPlainThreadRuntime(value.thread);
		}
		patched = patchPlainThreadActorConfig(value, localBase) || patched;
		rememberPlainThreadRuntime(value);
		rememberPlainThreadSettingsMessage(value);
		const children = Array.isArray(value) ? value : Object.values(value);
		for (const child of children) {
			patched = patchPlainThreadActorConfigs(child, seen, localBase) || patched;
		}
		return patched;
	}

	function parsedTextLocalInferencePatchOptions(text) {
		const source = typeof text === "string" ? text : "";
		if (!source) {
			return { configs: false };
		}
		return {
			configs: source.includes("threadActorConfig") || source.includes("wsToken") || source.includes("thread_settings") || source.includes("baseURL") || source.includes("ampURL") || source.includes("threadId") || source.includes("threadID") || source.includes("thread_id") || source.includes("\"id\"") || source.includes("workingDirectory") || source.includes("workspaceRoot") || source.includes("workspace"),
		};
	}

	function patchDecodedLocalInference(value, options) {
		const patchOptions = options || { configs: true };
		if (patchOptions.configs) {
			const localBase = localBaseURLString();
			const patched = patchDevalueThreadActorConfigs(value, localBase) ||
				patchPlainThreadActorConfigs(value, new WeakSet(), localBase);
			if (patched) {
				diagnostics.decodedConfigPatchCount += 1;
				diagnostics.lastPatchedThreadActorBaseURL = localBase;
			}
		}
	}

	function threadActorAPIPath(path) {
		return threadActorCreatePath(path) || threadActorInstancePath(path);
	}

	function threadActorCreatePath(path) {
		return path === "/api/thread-actors";
	}

	function threadActorInstancePath(path) {
		return path.startsWith("/api/thread-actors/");
	}

	function svelteKitRemoteEndpoint(path) {
		path = "/" + String(path || "").replace(/^\/+|\/+$/g, "");
		const prefix = "/_app/remote/";
		if (!path.startsWith(prefix)) {
			return "";
		}
		const remoteID = path.slice(prefix.length);
		if (!remoteID || remoteID.includes("//")) {
			return "";
		}
		const parts = remoteID.split("/");
		if (parts.length !== 2 || !parts[0] || !parts[1]) {
			return "";
		}
		switch (parts[1]) {
		case "createProjectThread":
		case "prewarmProjectThread":
			return parts[1];
		default:
			return "";
		}
	}

	function svelteKitRemotePath(path) {
		const endpoint = svelteKitRemoteEndpoint(path);
		return endpoint === "createProjectThread" || endpoint === "prewarmProjectThread";
	}

	function svelteKitRemoteCommandPath(path) {
		return svelteKitRemotePath(path);
	}

	function createProjectThreadRemotePath(path) {
		return svelteKitRemoteEndpoint(path) === "createProjectThread";
	}

	function shouldBridgeHTTP(url) {
		if (!threadActorAPIPath(url.pathname) && !internalAPIPath(url.pathname) && !svelteKitRemotePath(url.pathname)) {
			return false;
		}
		if (internalAPIPath(url.pathname) && !activeLocalThreadID()) {
			return false;
		}
		const base = localBaseURL();
		return url.origin === globalThis.location.origin || sameLocalHTTPBase(url, base);
	}

	function shouldBridgeWebSocket(url) {
		return shouldBootstrapExecutor(url) || shouldBridgeUserActorWebSocket(url);
	}

	function sameLocalHTTPBase(url, base) {
		return url.protocol === base.protocol && url.host === base.host;
	}

	function shouldRewriteHTTP(url) {
		return url.origin === globalThis.location.origin && (threadActorAPIPath(url.pathname) || internalAPIPath(url.pathname) || svelteKitRemotePath(url.pathname));
	}

	function internalAPIPath(path) {
		return path === "/api/internal";
	}

	function shouldRewriteWebSocket(url, base) {
		return url.origin === globalThis.location.origin && shouldBridgeWebSocket(url) ||
			sameLocalWebSocketBase(url, base) && shouldBridgeWebSocket(url);
	}

	function gatewayActorPath(path) {
		const normalized = path.startsWith("/actors/gateway/") ? path.slice("/actors".length) : path;
		return normalized === "/gateway" || normalized.startsWith("/gateway/");
	}

	function gatewayUserActorPath(path) {
		const normalized = path.startsWith("/actors/gateway/") ? path.slice("/actors".length) : path;
		if (!normalized.startsWith("/gateway/")) {
			return false;
		}
		const target = normalized.slice("/gateway/".length).split("/")[0].split("@")[0];
		return target === "userActor" || target === "user-actor";
	}

	function pathThreadID() {
		const match = globalThis.location.pathname.match(/^\/threads\/([^/?#]+)/);
		return match && validThreadID(decodeURIComponent(match[1])) ? decodeURIComponent(match[1]) : "";
	}

	function activeThreadID() {
		const pathThread = pathThreadID();
		if (observedThreadID && (!pathThread || observedThreadID === pathThread || rememberedLocalThreadID(observedThreadID))) {
			return observedThreadID;
		}
		return pathThread;
	}

	function activeLocalThreadID() {
		const threadID = activeThreadID();
		return rememberedLocalThreadID(threadID) ? threadID : "";
	}

	function threadIDFromGatewayURL(url) {
		const fromKey = firstThreadIDFromText(url.searchParams.get("rvt-key") || url.searchParams.get("key") || "");
		if (fromKey) {
			return fromKey;
		}
		const encodedInput = url.searchParams.get("rvt-input") || "";
		const inputText = decodeBase64Text(encodedInput);
		const input = parseJSONText(inputText);
		return firstThreadIDFromValue(input);
	}

	function shouldBootstrapExecutor(url) {
		if (!gatewayActorPath(url.pathname)) {
			return false;
		}
		const threadID = rememberObservedThreadID(threadIDFromGatewayURL(url));
		const activeThread = activeThreadID();
		return !!threadID && (!activeThread || activeThread === threadID || rememberedLocalThreadID(threadID));
	}

	function shouldBridgeUserActorWebSocket(url) {
		if (!gatewayUserActorPath(url.pathname)) {
			return false;
		}
		const threadID = pathThreadID();
		return !!threadID && rememberedLocalThreadID(threadID);
	}

	function localHTTPURL(url, body) {
		const base = localBaseURL();
		const local = new URL(url.pathname + url.search + url.hash, base);
		if (createProjectThreadRemotePath(url.pathname)) {
			const workingDirectory = remoteCreateProjectThreadWorkingDirectory(body);
			if (workingDirectory && !local.searchParams.has("cliproxy-working-directory")) {
				local.searchParams.set("cliproxy-working-directory", workingDirectory);
			}
		}
		return local.href;
	}

	function decodeBase64Text(value) {
		value = typeof value === "string" ? value.trim() : "";
		if (!value) {
			return "";
		}
		value = value.replace(/-/g, "+").replace(/_/g, "/");
		while (value.length %% 4 !== 0) {
			value += "=";
		}
		try {
			const binary = globalThis.atob(value);
			if (typeof globalThis.TextDecoder !== "function") {
				return binary;
			}
			const bytes = Uint8Array.from(binary, (char) => char.charCodeAt(0));
			return new globalThis.TextDecoder().decode(bytes);
		} catch {
			return "";
		}
	}

	function decodeDevalueString(text) {
		let values;
		try {
			values = originalJSONParse(text);
		} catch {
			return undefined;
		}
		if (!Array.isArray(values)) {
			return values;
		}
		const seen = new Set();
		const decodeIndex = (index) => {
			if (!Number.isInteger(index) || index < 0 || index >= values.length || seen.has(index)) {
				return undefined;
			}
			seen.add(index);
			const value = values[index];
			let decoded;
			if (Array.isArray(value)) {
				decoded = value.map((item) => decodeIndex(item));
			} else if (isPlainObject(value)) {
				decoded = {};
				for (const [key, item] of Object.entries(value)) {
					decoded[key] = decodeIndex(item);
				}
			} else {
				decoded = value;
			}
			seen.delete(index);
			return decoded;
		};
		return decodeIndex(0);
	}

	function decodeRemoteCommandBody(body) {
		if (typeof body !== "string" || !body) {
			return null;
		}
		try {
			const envelope = originalJSONParse(body);
			const payload = isPlainObject(envelope) ? decodeBase64Text(envelope.payload) : "";
			return payload ? decodeDevalueString(payload) : null;
		} catch {
			return null;
		}
	}

	function remoteContentText(content) {
		if (typeof content === "string") {
			return content;
		}
		if (Array.isArray(content)) {
			return content.map((item) => isPlainObject(item) ? firstString(item.text, item.content) : "").filter(Boolean).join("\n");
		}
		if (isPlainObject(content)) {
			return firstString(content.text, content.content);
		}
		return "";
	}

	function threadMentionWorkingDirectory(text) {
		if (typeof text !== "string" || !text) {
			return "";
		}
		const directories = threadWorkingDirectories();
		const pattern = /T-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}/g;
		let match;
		while ((match = pattern.exec(text))) {
			const workingDirectory = normalizeWorkingDirectory(directories[match[0]]);
			if (workingDirectory) {
				return workingDirectory;
			}
		}
		return "";
	}

	function pathBaseName(path) {
		path = normalizeWorkingDirectory(path).replace(/[\\/]+$/, "");
		const parts = path.split(/[\\/]+/);
		return parts[parts.length - 1] || "";
	}

	function visibleProjectName() {
		for (const element of globalThis.document.querySelectorAll("button,[role='button'],[aria-haspopup]")) {
			const text = (element.innerText || element.textContent || "").replace(/\s+/g, " ").trim();
			const match = text.match(/(?:^|\b)Project:\s*([^\n]+?)(?:\s{2,}|\s*[⌃⌥⇧⌘]|$)/);
			if (match && match[1]) {
				return match[1].trim();
			}
		}
		return "";
	}

	function visibleProjectWorkingDirectory() {
		const name = visibleProjectName();
		if (!name) {
			return "";
		}
		const matches = new Set();
		for (const workingDirectory of Object.values(threadWorkingDirectories())) {
			const normalized = normalizeWorkingDirectory(workingDirectory);
			if (normalized && pathBaseName(normalized) === name) {
				matches.add(normalized);
			}
		}
		return matches.size === 1 ? [...matches][0] : "";
	}

	function remoteCreateProjectThreadWorkingDirectory(body) {
		const decoded = decodeRemoteCommandBody(body);
		const fromMention = threadMentionWorkingDirectory(remoteContentText(isPlainObject(decoded) ? decoded.content : null));
		if (fromMention) {
			return fromMention;
		}
		const projectID = isPlainObject(decoded) ? firstString(decoded.projectID, decoded.projectId, decoded.project_id) : "";
		if (projectID) {
			return "";
		}
		const fromSelectedLocalProject = selectedLocalProjectWorkingDirectory();
		if (fromSelectedLocalProject) {
			return fromSelectedLocalProject;
		}
		const fromVisibleProject = visibleProjectWorkingDirectory();
		if (fromVisibleProject) {
			return fromVisibleProject;
		}
		return localWorkingDirectory();
	}

	function rememberRemoteCreateProjectThread(sourceURL, body, response) {
		if (!createProjectThreadRemotePath(sourceURL.pathname)) {
			return;
		}
		const workingDirectory = remoteCreateProjectThreadWorkingDirectory(body);
		if (!response || !response.ok || typeof response.clone !== "function") {
			return;
		}
		const decodedRequest = decodeRemoteCommandBody(body);
		response.clone().text().then((text) => {
			try {
				const envelope = originalJSONParse(text || "{}");
				const result = decodeDevalueString(envelope.data || "");
				const value = isPlainObject(result) ? result._ : null;
				const threadID = responseThreadID(value);
				if (!threadID) {
					return;
				}
				const resolvedWorkingDirectory = responseWorkingDirectory(value) || workingDirectory;
				clearSelectedLocalProject();
				rememberLocalThreadID(threadID);
				if (resolvedWorkingDirectory) {
					rememberThreadWorkingDirectory(threadID, resolvedWorkingDirectory);
				}
				rememberThreadSettings(threadID, {
					agentMode: firstString(isPlainObject(decodedRequest) ? decodedRequest.agentMode : ""),
					reasoningEffort: firstString(isPlainObject(decodedRequest) ? decodedRequest.reasoningEffort : ""),
				});
			} catch {
			}
		});
	}

	function bridgeRequestBody(sourceURL, method, body) {
		const workingDirectory = localWorkingDirectory();
		const isThreadActorRequest = threadActorAPIPath(sourceURL.pathname);
		if ((!workingDirectory || !isThreadActorRequest) && sourceURL.pathname !== "/api/internal") {
			return body;
		}
		if (method.toUpperCase() !== "POST" || typeof body !== "string") {
			return body;
		}
		try {
			const payload = JSON.parse(body);
			if (!isPlainObject(payload)) {
				return body;
			}
			if (sourceURL.pathname === "/api/internal") {
				if (activeLocalThreadID() && isPlainObject(payload.params) && !payload.params.thread && !payload.params.threadID && !payload.params.threadId) {
					payload.params.thread = activeLocalThreadID();
				}
				return JSON.stringify(payload);
			}
			if (!isThreadActorRequest) {
				return body;
			}
			if (payload.workingDirectory || payload.cwd || payload.workspaceRoot) {
				return body;
			}
			payload.workingDirectory = workingDirectory;
			payload.workspaceRoot = workingDirectory;
			return JSON.stringify(payload);
		} catch {
			return body;
		}
	}

	function bodyNeedsDuplex(body) {
		return typeof ReadableStream !== "undefined" && body instanceof ReadableStream;
	}

	function bodyNeedsTextBridge(sourceURL, method, request, init) {
		return request && init?.body === undefined && request.body && method.toUpperCase() === "POST" &&
			(threadActorAPIPath(sourceURL.pathname) || sourceURL.pathname === "/api/internal" || svelteKitRemoteCommandPath(sourceURL.pathname));
	}

	function localFetchHeaders(contentType, promptForKey = true) {
		const headers = new Headers();
		if (contentType) {
			headers.set("Content-Type", contentType);
		}
		headers.set(bridgeHeader, "1");
		const apiKey = promptForKey ? localAPIKey() : storedLocalAPIKey();
		if (apiKey) {
			headers.set("Authorization", "Bearer " + apiKey);
		}
		return headers;
	}

	function normalizeLocalProject(project) {
		if (!isPlainObject(project)) {
			return null;
		}
		const workingDirectory = normalizeWorkingDirectory(firstString(project.workingDirectory, project.workspaceRoot, project.cwd));
		if (!workingDirectory) {
			return null;
		}
		return {
			id: firstString(project.id, project.projectID, project.projectId, project.project_id),
			name: firstString(project.name, project.projectName, pathBaseName(workingDirectory), "local"),
			namespace: firstString(project.namespace, project.projectNamespace, "local"),
			repositoryURL: firstString(project.repositoryURL, project.repoURL),
			workingDirectory,
		};
	}

	function fetchLocalProjects(promptForKey = false) {
		const now = Date.now();
		if (localProjectsCache.promise) {
			return localProjectsCache.promise;
		}
		if (now - localProjectsCache.at < 10000) {
			return Promise.resolve(localProjectsCache.projects);
		}
		const headers = localFetchHeaders("", false);
		if (!headers.get("Authorization") && promptForKey) {
			const apiKey = localProjectLookupAPIKey();
			if (apiKey) {
				headers.set("Authorization", "Bearer " + apiKey);
			}
		}
		if (!headers.get("Authorization")) {
			return Promise.resolve([]);
		}
		diagnostics.localProjectFetchCount += 1;
		localProjectsCache.promise = originalFetch(localBaseURLString() + localProjectsEndpointPath, {
			method: "GET",
			headers,
			mode: "cors",
			credentials: "omit",
		}).then((response) => {
			if (!response.ok) {
				return [];
			}
			return response.json().catch(() => ({}));
		}).then((decoded) => {
			const responseDefaultWorkingDirectory = normalizeWorkingDirectory(firstString(decoded.defaultWorkingDirectory, decoded.homeDirectory));
			if (responseDefaultWorkingDirectory) {
				defaultWorkingDirectory = responseDefaultWorkingDirectory;
			}
			const projects = Array.isArray(decoded.projects) ? decoded.projects.map(normalizeLocalProject).filter(Boolean) : [];
			localProjectsCache = { at: projects.length ? Date.now() : 0, projects, promise: null };
			return projects;
		}).catch(() => {
			localProjectsCache.promise = null;
			return [];
		});
		return localProjectsCache.promise;
	}

	async function promptLocalThread() {
		const promptText = globalThis.prompt("Start local Amp thread");
		if (promptText === null) {
			return;
		}
		let workingDirectory = localWorkingDirectory();
		if (!workingDirectory) {
			workingDirectory = await ensureDefaultWorkingDirectory(false);
		}
		if (!workingDirectory) {
			workingDirectory = (globalThis.prompt("Working directory for local inference", "") || "").trim();
			if (workingDirectory) {
				globalThis.localStorage.setItem(workingDirectoryStorageKey, workingDirectory);
			}
		}
		showLocalThreadPicker(null, promptText.trim(), workingDirectory);
	}

	function openLocalThreadFromMenu(modeOptions) {
		createLocalThread("", localWorkingDirectory(), modeOptions).catch((error) => {
			globalThis.alert("Local Amp thread failed: " + (error && error.message ? error.message : String(error)));
		});
	}

	function localThreadPayload(promptText, workingDirectory, modeOptions) {
		const mode = localThreadModeOptions(modeOptions);
		const settings = {
			agentMode: mode.agentMode,
		};
		if (mode.reasoningEffort) {
			settings["reasoning.effort"] = mode.reasoningEffort;
		}
		const payload = {
			agentMode: mode.agentMode,
			reasoningEffort: mode.reasoningEffort,
			executorType: "local-client",
			usesThreadActors: true,
			settings,
			threadMeta: {
				ampcodeConnectorLocalNeo: true,
				cliProxyAPILocalNeo: true,
				ampcodeLocalRuntime: true,
				ampcodeConnectorMode: "local-neo",
				agentMode: mode.agentMode,
				reasoningEffort: mode.reasoningEffort,
			},
		};
		if (workingDirectory) {
			payload.workingDirectory = workingDirectory;
			payload.workspaceRoot = workingDirectory;
		}
		if (promptText) {
			payload.prompt = promptText;
		}
		return payload;
	}

	async function createLocalThread(promptText, workingDirectory, modeOptions) {
		const headers = localFetchHeaders("application/json");
		workingDirectory = normalizeWorkingDirectory(workingDirectory) || await ensureDefaultWorkingDirectory(false);
		const payload = localThreadPayload(promptText, workingDirectory, modeOptions);
		const shellThreadID = await createRemoteThreadShell(payload);
		payload.threadId = shellThreadID;
		payload.threadID = shellThreadID;
		const response = await originalFetch(localBaseURLString() + "/api/thread-actors/" + encodeURIComponent(shellThreadID), {
			method: "POST",
			headers,
			mode: "cors",
			credentials: "omit",
			body: JSON.stringify(payload),
		});
		const decoded = await readJSONResponse(response, "local thread response");
		const threadID = responseThreadID(decoded) || shellThreadID;
		rememberLocalThreadID(threadID);
		rememberThreadWorkingDirectory(threadID, workingDirectory);
		rememberThreadSettings(threadID, { agentMode: payload.agentMode, reasoningEffort: payload.reasoningEffort });
		diagnostics.lastLocalThreadAgentMode = payload.agentMode || "";
		diagnostics.lastLocalThreadReasoningEffort = payload.reasoningEffort || "";
		diagnostics.lastLocalThreadChoice = localThreadModeLabel(payload);
		navigateToThread(threadID);
	}

	async function createRemoteThreadShell(localPayload) {
		const shellPayload = {
			agentMode: localPayload.agentMode,
			reasoningEffort: localPayload.reasoningEffort,
			usesThreadActors: true,
			settings: localPayload.settings,
			threadMeta: Object.assign({}, localPayload.threadMeta || {}, {
				cliProxyAPIWebLocalShell: true,
			}),
		};
		const response = await originalFetch(globalThis.location.origin + "/api/thread-actors", {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			credentials: "same-origin",
			body: JSON.stringify(shellPayload),
		});
		const decoded = await readJSONResponse(response, "Amp thread shell response");
		const threadID = responseThreadID(decoded);
		if (!threadID) {
			throw new Error("Amp thread shell response missing threadId");
		}
		diagnostics.remoteShellCreateCount += 1;
		return threadID;
	}

	async function readJSONResponse(response, label) {
		const text = await response.text();
		let decoded = {};
		try {
			decoded = text ? JSON.parse(text) : {};
		} catch {
			throw new Error(text || ("invalid " + label));
		}
		if (!response.ok) {
			throw new Error(decoded.error || decoded.message || text || (label + " HTTP " + response.status));
		}
		return decoded;
	}

	function responseThreadID(value) {
		const directThreadID = (object) => {
			if (!isPlainObject(object)) {
				return "";
			}
			for (const key of ["threadId", "threadID", "thread_id", "id"]) {
				const candidate = object[key];
				if (typeof candidate === "string" && candidate.startsWith("T-")) {
					return candidate;
				}
			}
			return "";
		};
		if (typeof value === "string") {
			return value.startsWith("T-") ? value : "";
		}
		if (!isPlainObject(value)) {
			return "";
		}
		let threadID = directThreadID(value);
		if (threadID) {
			return threadID;
		}
		const result = isPlainObject(value.result) ? value.result : {};
		threadID = directThreadID(result);
		if (threadID) {
			return threadID;
		}
		for (const key of ["thread", "threadActor", "actor"]) {
			threadID = directThreadID(result[key]);
			if (threadID) {
				return threadID;
			}
		}
		return "";
	}

	function responseWorkingDirectory(value) {
		const directWorkingDirectory = (object) => {
			if (!isPlainObject(object)) {
				return "";
			}
			return normalizeWorkingDirectory(firstString(object.workingDirectory, object.workspaceRoot, object.cwd));
		};
		if (!isPlainObject(value)) {
			return "";
		}
		let workingDirectory = directWorkingDirectory(value);
		if (workingDirectory) {
			return workingDirectory;
		}
		const result = isPlainObject(value.result) ? value.result : {};
		workingDirectory = directWorkingDirectory(result);
		if (workingDirectory) {
			return workingDirectory;
		}
		for (const key of ["thread", "threadActor", "actor"]) {
			workingDirectory = directWorkingDirectory(result[key]);
			if (workingDirectory) {
				return workingDirectory;
			}
		}
		return "";
	}

	function navigateToThread(threadID) {
		diagnostics.lastLocalThreadNavigationThreadID = threadID;
		globalThis.location.assign("/threads/" + encodeURIComponent(threadID));
	}

	function removeLocalThreadPicker() {
		const cleanup = globalThis.__cliproxyAmpLocalInferencePickerCleanup;
		if (typeof cleanup === "function") {
			cleanup();
		}
		globalThis.document.getElementById("cliproxy-amp-local-thread-picker")?.remove();
	}

	function showLocalThreadPicker(anchor, promptText = "", workingDirectory = localWorkingDirectory()) {
		removeLocalThreadPicker();
		const panel = globalThis.document.createElement("div");
		panel.id = "cliproxy-amp-local-thread-picker";
		panel.setAttribute("role", "menu");
		panel.setAttribute("aria-label", "Local thread mode");
		panel.style.cssText = [
			"position:fixed",
			"z-index:2147483647",
			"min-width:220px",
			"max-width:calc(100vw - 16px)",
			"max-height:calc(100vh - 16px)",
			"overflow:auto",
			"padding:6px",
			"border:1px solid rgba(128,128,128,.26)",
			"border-radius:8px",
			"background:Canvas",
			"color:CanvasText",
			"box-shadow:0 18px 48px rgba(0,0,0,.22)",
			"font:14px system-ui,-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif",
		].join(";");
		for (const choice of localThreadModeChoices()) {
			panel.appendChild(buildLocalThreadChoiceButton(choice, promptText, workingDirectory));
		}
		globalThis.document.body.appendChild(panel);
		positionLocalThreadPicker(panel, anchor);
		const closeOnPointerDown = (event) => {
			if (panel.contains(event.target) || (anchor instanceof Element && anchor.contains(event.target))) {
				return;
			}
			removeLocalThreadPicker();
		};
		const closeOnKeyDown = (event) => {
			if (event.key === "Escape") {
				removeLocalThreadPicker();
			}
		};
		const cleanup = () => {
			globalThis.document.removeEventListener("pointerdown", closeOnPointerDown, true);
			globalThis.document.removeEventListener("keydown", closeOnKeyDown, true);
			if (globalThis.__cliproxyAmpLocalInferencePickerCleanup === cleanup) {
				globalThis.__cliproxyAmpLocalInferencePickerCleanup = null;
			}
		};
		globalThis.__cliproxyAmpLocalInferencePickerCleanup = cleanup;
		setTimeout(() => {
			globalThis.document.addEventListener("pointerdown", closeOnPointerDown, true);
			globalThis.document.addEventListener("keydown", closeOnKeyDown, true);
		}, 0);
		diagnostics.localThreadPickerOpenCount += 1;
	}

	function buildLocalThreadChoiceButton(choice, promptText, workingDirectory) {
		const button = globalThis.document.createElement("button");
		button.type = "button";
		button.setAttribute("role", "menuitem");
		button.style.cssText = [
			"display:flex",
			"align-items:center",
			"justify-content:space-between",
			"gap:18px",
			"width:100%%",
			"padding:9px 10px",
			"border:0",
			"border-radius:6px",
			"background:transparent",
			"color:inherit",
			"font:inherit",
			"text-align:left",
			"cursor:default",
		].join(";");
		const label = globalThis.document.createElement("span");
		label.textContent = choice.label;
		const detail = globalThis.document.createElement("span");
		detail.textContent = choice.detail || "";
		detail.style.cssText = "opacity:.62;font-size:12px;white-space:nowrap";
		button.append(label, detail);
		button.addEventListener("pointerenter", () => {
			button.style.background = "rgba(127,127,127,.14)";
		});
		button.addEventListener("pointerleave", () => {
			button.style.background = "transparent";
		});
		button.addEventListener("click", (event) => {
			event.preventDefault();
			event.stopPropagation();
			removeLocalThreadPicker();
			setTimeout(() => createLocalThread(promptText || "", workingDirectory || "", choice.options).catch((error) => {
				globalThis.alert("Local Amp thread failed: " + (error && error.message ? error.message : String(error)));
			}), 0);
		}, true);
		return button;
	}

	function positionLocalThreadPicker(panel, anchor) {
		const gap = 6;
		const margin = 8;
		const width = panel.offsetWidth || 220;
		const height = panel.offsetHeight || 320;
		let left = Math.max(margin, Math.round((globalThis.innerWidth - width) / 2));
		let top = Math.max(margin, Math.round((globalThis.innerHeight - height) / 2));
		if (anchor instanceof Element) {
			const rect = anchor.getBoundingClientRect();
			left = rect.right + gap;
			top = rect.top;
			if (left + width > globalThis.innerWidth - margin) {
				left = rect.left - width - gap;
			}
			if (left < margin) {
				left = Math.min(Math.max(margin, rect.left), Math.max(margin, globalThis.innerWidth - width - margin));
			}
			if (top + height > globalThis.innerHeight - margin) {
				top = Math.max(margin, globalThis.innerHeight - height - margin);
			}
		}
		panel.style.left = Math.round(left) + "px";
		panel.style.top = Math.round(top) + "px";
	}

	function installLocalThreadControls() {
		removeInjectedLocalThreadControls();
		installLocalProjectPickerIntegration();
	}

	function removeStaleLocalThreadButton() {
		globalThis.document.getElementById("cliproxy-amp-local-thread-button")?.remove();
	}

	function removeInjectedLocalThreadControls() {
		removeStaleLocalThreadButton();
		removeLocalThreadPicker();
		let removed = 0;
		globalThis.document.querySelectorAll("[data-cliproxy-local-thread-command-item],[data-cliproxy-local-thread-menu-item],[data-cliproxy-local-project-item]").forEach((element) => {
			element.remove();
			removed += 1;
		});
		for (const key of ["__cliproxyAmpLocalInferenceMenuObserver", "__cliproxyAmpLocalInferenceCommandPaletteObserver", "__cliproxyAmpLocalInferenceProjectPickerObserver"]) {
			const observer = globalThis[key];
			if (observer && typeof observer.disconnect === "function") {
				observer.disconnect();
			}
			globalThis[key] = null;
		}
		diagnostics.removedLocalThreadControlCount += removed;
	}

	function installLocalProjectPickerIntegration() {
		if (globalThis.__cliproxyAmpLocalInferenceProjectPickerObserver) {
			integrateLocalProjectActivators(globalThis.document);
			integrateLocalProjectPickers(globalThis.document);
			return;
		}
		const observer = new MutationObserver((mutations) => {
			for (const mutation of mutations) {
				for (const node of mutation.addedNodes) {
					if (node instanceof Element) {
						integrateLocalProjectActivators(node);
						integrateLocalProjectPickers(node);
					}
				}
			}
		});
		observer.observe(globalThis.document.documentElement, { childList: true, subtree: true });
		globalThis.__cliproxyAmpLocalInferenceProjectPickerObserver = observer;
		integrateLocalProjectActivators(globalThis.document);
		integrateLocalProjectPickers(globalThis.document);
	}

	function integrateLocalProjectActivators(root) {
		const workingDirectory = activeThreadWorkingDirectory() || visibleProjectWorkingDirectory() || selectedLocalProjectWorkingDirectory() || localWorkingDirectory();
		if (!workingDirectory) {
			return;
		}
		for (const activator of localProjectActivatorCandidates(root)) {
			if (!localProjectActivatorNeedsPatch(activator, workingDirectory) || activator.dataset.cliproxyLocalProjectActivatorLoading === "1") {
				continue;
			}
			activator.dataset.cliproxyLocalProjectActivatorLoading = "1";
			fetchLocalProjects(false).then((projects) => {
				delete activator.dataset.cliproxyLocalProjectActivatorLoading;
				if (!globalThis.document.contains(activator) || !localProjectActivatorNeedsPatch(activator, workingDirectory)) {
					return;
				}
				const project = localProjectByWorkingDirectory(projects, workingDirectory);
				const label = firstString(project?.name, pathBaseName(workingDirectory));
				if (!label) {
					return;
				}
				patchLocalProjectActivator(activator, label, workingDirectory);
			});
		}
	}

	function localProjectActivatorCandidates(root) {
		const out = [];
		const add = (element) => {
			if (element instanceof Element && !out.includes(element)) {
				out.push(element);
			}
		};
		const selector = "button,[role='button']";
		if (root instanceof Element) {
			if (root.matches(selector)) {
				add(root);
			}
			root.querySelectorAll?.(selector).forEach(add);
		} else {
			root.querySelectorAll?.(selector).forEach(add);
		}
		return out;
	}

	function localProjectActivatorLooksUnset(activator) {
		if (!elementVisible(activator)) {
			return false;
		}
		const text = (activator.innerText || activator.textContent || "").replace(/\s+/g, " ").trim();
		return /\bProject:\s*No Project\b/.test(text);
	}

	function localProjectActivatorNeedsPatch(activator, workingDirectory) {
		if (!elementVisible(activator)) {
			return false;
		}
		if (localProjectActivatorLooksUnset(activator)) {
			return true;
		}
		if (activator.dataset?.cliproxyLocalProjectActivator !== "1") {
			return false;
		}
		return normalizeWorkingDirectory(activator.dataset.cliproxyLocalProjectWorkingDirectory) !== normalizeWorkingDirectory(workingDirectory);
	}

	function patchLocalProjectActivator(activator, label, workingDirectory) {
		const walker = globalThis.document.createTreeWalker(activator, NodeFilter.SHOW_TEXT);
		let changed = false;
		const replacementTargets = ["No Project", activator.dataset.cliproxyLocalProjectLabel].filter((target) => target && target !== label);
		for (let node = walker.nextNode(); node; node = walker.nextNode()) {
			if (!node.nodeValue) {
				continue;
			}
			let nextValue = node.nodeValue;
			for (const target of replacementTargets) {
				if (nextValue.includes(target)) {
					nextValue = nextValue.split(target).join(label);
				}
			}
			if (nextValue !== node.nodeValue) {
				node.nodeValue = nextValue;
				changed = true;
			}
		}
		if (!changed) {
			let text = activator.textContent || "";
			for (const target of replacementTargets) {
				if (text.includes(target)) {
					text = text.split(target).join(label);
					changed = true;
				}
			}
			if (changed) {
				activator.textContent = text;
			}
		}
		const ariaLabel = activator.getAttribute("aria-label");
		if (ariaLabel) {
			let nextAriaLabel = ariaLabel;
			for (const target of replacementTargets) {
				if (nextAriaLabel.includes(target)) {
					nextAriaLabel = nextAriaLabel.split(target).join(label);
				}
			}
			if (nextAriaLabel !== ariaLabel) {
				activator.setAttribute("aria-label", nextAriaLabel);
			}
		}
		activator.dataset.cliproxyLocalProjectActivator = "1";
		activator.dataset.cliproxyLocalProjectLabel = label;
		activator.dataset.cliproxyLocalProjectWorkingDirectory = workingDirectory;
		diagnostics.localProjectActivatorIntegrationCount += 1;
	}

	function refreshLocalProjectActivators(project) {
		const workingDirectory = normalizeWorkingDirectory(project?.workingDirectory);
		if (!workingDirectory) {
			return;
		}
		const label = firstString(project?.name, pathBaseName(workingDirectory));
		if (!label) {
			return;
		}
		for (const activator of localProjectActivatorCandidates(globalThis.document)) {
			if (localProjectActivatorLooksUnset(activator) || activator.dataset?.cliproxyLocalProjectActivator === "1") {
				patchLocalProjectActivator(activator, label, workingDirectory);
			}
		}
	}

	function localProjectByWorkingDirectory(projects, workingDirectory) {
		const target = normalizeWorkingDirectory(workingDirectory);
		for (const project of projects || []) {
			if (normalizeWorkingDirectory(project?.workingDirectory) === target) {
				return project;
			}
		}
		return null;
	}

	function integrateLocalProjectPickers(root) {
		for (const picker of localProjectPickerCandidates(root)) {
			if (!localProjectPickerLooksLikeProjectPicker(picker)) {
				continue;
			}
			const list = commandPaletteList(picker);
			if (list) {
				installLocalProjectPickerKeyboardNavigation(picker, list);
				installLocalProjectNoProjectSelectionHandler(picker);
				autoSelectCurrentProjectInPicker(picker, list);
			}
			if (!list || list.querySelector("[data-cliproxy-local-project-item]") || list.dataset.cliproxyLocalProjectLoading === "1") {
				continue;
			}
			list.dataset.cliproxyLocalProjectLoading = "1";
			fetchLocalProjects(true).then((projects) => {
				delete list.dataset.cliproxyLocalProjectLoading;
				const displayProjects = localProjectPickerDisplayProjects(projects);
				if (!projects.length || !globalThis.document.contains(picker) || !localProjectPickerLooksLikeProjectPicker(picker)) {
					const attempts = Number(list.dataset.cliproxyLocalProjectAttempts || "0");
					if (attempts < 3 && globalThis.document.contains(picker)) {
						list.dataset.cliproxyLocalProjectAttempts = String(attempts + 1);
						setTimeout(() => integrateLocalProjectPickers(picker), 750 * (attempts + 1));
					}
					return;
				}
				if (!displayProjects.length) {
					return;
				}
				delete list.dataset.cliproxyLocalProjectAttempts;
				injectLocalProjectPickerItems(picker, list, displayProjects);
				autoSelectCurrentProjectInPicker(picker, list);
			});
		}
	}

	function localProjectPickerCandidates(root) {
		const candidates = commandPaletteCandidates(root);
		if (root instanceof Element && !candidates.includes(root)) {
			candidates.push(root);
		}
		return candidates;
	}

	function localProjectPickerLooksLikeProjectPicker(picker) {
		if (!elementVisible(picker)) {
			return false;
		}
		const text = (picker.innerText || picker.textContent || "").replace(/\s+/g, " ").trim();
		return text.includes("No Project") && /\bProjects?\b/.test(text);
	}

	function injectLocalProjectPickerItems(picker, list, projects) {
		if (list.querySelector("[data-cliproxy-local-project-item]")) {
			return;
		}
		const before = localProjectPickerActionsRow(list);
		for (const project of projects) {
			list.insertBefore(buildLocalProjectPickerItem(picker, project), before);
			diagnostics.localProjectPickerIntegrationCount += 1;
		}
	}

	function buildLocalProjectPickerItem(picker, project) {
		const template = localProjectPickerTemplateItem(picker);
		const item = globalThis.document.createElement("button");
		const isCurrent = normalizeWorkingDirectory(project.workingDirectory) === activeThreadWorkingDirectory();
		const label = firstString(project.name, pathBaseName(project.workingDirectory), "local");
		item.type = "button";
		item.dataset.cliproxyLocalProjectItem = "1";
		item.dataset.cliproxyLocalProjectWorkingDirectory = normalizeWorkingDirectory(project.workingDirectory);
		item.setAttribute("data-value", label);
		item.setAttribute("aria-label", label);
		item.setAttribute("role", "option");
		item.setAttribute("aria-selected", "false");
		item.setAttribute("aria-disabled", "false");
		item.dataset.selected = "false";
		item.className = template?.className || "group flex w-full gap-3 rounded-xl px-3.5 py-2.5 text-left hover:bg-black/5 data-[selected=true]:bg-black/6 dark:hover:bg-white/8 dark:data-[selected=true]:bg-white/10 items-center";
		fillLocalProjectPickerItem(item, label, project.workingDirectory, isCurrent);
		const activate = (event) => {
			activateLocalProjectPickerItem(picker, item, event);
		};
		item.addEventListener("pointerdown", activate, true);
		item.addEventListener("click", activate, true);
		item.addEventListener("keydown", (event) => {
			if (event.key === "Enter") {
				activate(event);
			}
		}, true);
		return item;
	}

	function fillLocalProjectPickerItem(item, labelText, workingDirectory, isCurrent) {
		item.replaceChildren();
		const left = globalThis.document.createElement("div");
		left.className = "min-w-0 flex-1";
		const title = globalThis.document.createElement("div");
		title.className = "flex min-w-0 gap-1.5 text-lg pointer-coarse:text-[15px] pointer-coarse:leading-snug font-medium items-baseline";
		const label = globalThis.document.createElement("span");
		label.textContent = labelText;
		label.className = "min-w-0 truncate";
		title.append(label);
		left.append(title);

		const right = globalThis.document.createElement("div");
		right.className = "flex shrink-0 items-center gap-2 text-muted-foreground";
		const meta = globalThis.document.createElement("span");
		meta.className = "flex max-w-[min(45vw,32rem)] min-w-0 items-center gap-1.5 text-sm font-medium";
		const badge = globalThis.document.createElement("span");
		badge.textContent = isCurrent ? "Current" : "Git";
		badge.className = "shrink-0 rounded-md bg-foreground/8 px-1.5 py-0.5 text-[0.65rem] leading-none font-medium text-muted-foreground";
		const detail = globalThis.document.createElement("span");
		detail.textContent = localProjectPickerPathDisplay(workingDirectory);
		detail.title = workingDirectory;
		detail.className = "truncate";
		const check = globalThis.document.createElement("span");
		check.className = "flex size-4 shrink-0 items-center justify-center";
		check.dataset.slot = "project-check";
		meta.append(badge, detail);
		right.append(meta, check);
		item.append(left, right);
	}

	function localProjectPickerTemplateItem(picker) {
		for (const element of picker.querySelectorAll('[cmdk-item],[data-cmdk-item],[data-slot="command-item"],[role="option"],button,[role="button"]')) {
			const text = projectPickerItemPrimaryText(element);
			if (element.dataset?.cliproxyLocalProjectItem || /^No Project\b/.test(text) || /^(Actions|Create Project)\b/.test(text)) {
				continue;
			}
			return element;
		}
		return null;
	}

	function localProjectPickerPathDisplay(workingDirectory) {
		const normalized = normalizeWorkingDirectory(workingDirectory);
		if (normalized.startsWith("/Users/")) {
			const parts = normalized.split("/");
			if (parts.length > 3) {
				return "~/" + parts.slice(3).join("/");
			}
		}
		return normalized;
	}

	function localProjectPickerDisplayProjects(projects) {
		const activeDirectory = activeThreadWorkingDirectory();
		const selectedDirectory = selectedLocalProjectWorkingDirectory();
		const fallbackDirectory = normalizeWorkingDirectory(globalThis.localStorage.getItem(workingDirectoryStorageKey) || "");
		const visibleName = visibleProjectName();
		const rank = (project) => {
			const dir = normalizeWorkingDirectory(project.workingDirectory);
			if (activeDirectory && dir === activeDirectory) {
				return 0;
			}
			if (selectedDirectory && dir === selectedDirectory) {
				return 1;
			}
			if (visibleName && firstString(project.name, pathBaseName(project.workingDirectory)) === visibleName) {
				return 2;
			}
			if (fallbackDirectory && dir === fallbackDirectory) {
				return 3;
			}
			return 4;
		};
		const seen = new Set();
		const projectDirs = projects.map((project) => normalizeWorkingDirectory(project.workingDirectory)).filter((dir) => localProjectPickerDirectoryDisplayable(dir, false));
		return projects.filter((project) => {
			const dir = normalizeWorkingDirectory(project.workingDirectory);
			const projectRank = rank(project);
			const name = firstString(project.name, pathBaseName(project.workingDirectory));
			if (!localProjectPickerDirectoryDisplayable(dir, projectRank < 4) || seen.has(dir)) {
				return false;
			}
			if (projectRank >= 4 && localProjectPickerHasAncestorDirectory(dir, projectDirs)) {
				return false;
			}
			seen.add(dir);
			return true;
		}).sort((left, right) => {
			const leftRank = rank(left);
			const rightRank = rank(right);
			if (leftRank !== rightRank) {
				return leftRank - rightRank;
			}
			return firstString(left.name, pathBaseName(left.workingDirectory)).localeCompare(firstString(right.name, pathBaseName(right.workingDirectory)));
		}).slice(0, 50);
	}

	function localProjectPickerDirectoryDisplayable(dir, keepHidden) {
		dir = normalizeWorkingDirectory(dir);
		if (!dir || dir === "/" || /^[A-Za-z]:[\\/]?$/.test(dir)) {
			return false;
		}
		if (/^\/Users\/[^/]+$/.test(dir) || /^\/Users\/[^/]+\/Developer$/.test(dir)) {
			return false;
		}
		if (!keepHidden && pathBaseName(dir).startsWith(".")) {
			return false;
		}
		return true;
	}

	function localProjectPickerHasAncestorDirectory(dir, dirs) {
		for (const other of dirs) {
			if (other && other !== dir && dir.startsWith(other.replace(/[\\/]+$/, "") + "/")) {
				return true;
			}
		}
		return false;
	}

	function normalizeProjectPickerName(value) {
		return String(value || "").replace(/\s+/g, " ").trim().toLowerCase();
	}

	function installLocalProjectNoProjectSelectionHandler(picker) {
		const noProject = localProjectPickerNoProjectItem(picker);
		if (!noProject || noProject.dataset.cliproxyNoProjectHandler === "1") {
			return;
		}
		noProject.dataset.cliproxyNoProjectHandler = "1";
		const clear = () => {
			if (noProject.dataset.cliproxySuppressLocalProjectClear === "1") {
				return;
			}
			clearSelectedLocalProject(true);
		};
		noProject.addEventListener("pointerdown", clear, true);
		noProject.addEventListener("click", clear, true);
	}

	function localProjectPickerActionsRow(list) {
		for (const element of Array.from(list.children || [])) {
			const text = (element.innerText || element.textContent || "").replace(/\s+/g, " ").trim();
			if (/^(Actions|Create Project)\b/.test(text)) {
				return element;
			}
		}
		return null;
	}

	function localProjectPickerNoProjectItem(picker) {
		for (const element of picker.querySelectorAll('[cmdk-item],[data-cmdk-item],[data-slot="command-item"],[role="option"],button,[role="button"]')) {
			const text = (element.innerText || element.textContent || "").replace(/\s+/g, " ").trim();
			if (/^No Project\b/.test(text) && !element.dataset.cliproxyLocalProjectItem) {
				return element;
			}
		}
		return null;
	}

	function installLocalProjectPickerKeyboardNavigation(picker, list) {
		if (list.dataset.cliproxyLocalProjectKeyboardNavigation === "1") {
			return;
		}
		list.dataset.cliproxyLocalProjectKeyboardNavigation = "1";
		const target = localProjectPickerKeyboardTarget(picker, list);
		if (!target || target.dataset.cliproxyLocalProjectKeyboardNavigation === "1") {
			return;
		}
		target.dataset.cliproxyLocalProjectKeyboardNavigation = "1";
		target.addEventListener("keydown", (event) => {
			const activeList = localProjectPickerKeyboardList(target, list);
			if (activeList) {
				handleLocalProjectPickerKeydown(event, target, activeList);
			}
		}, true);
	}

	function localProjectPickerKeyboardTarget(picker, list) {
		if (!(list instanceof Element)) {
			return null;
		}
		return list.closest('[role="dialog"],[data-slot="dialog-content"],[cmdk-root],[data-cmdk-root]') ||
			(picker instanceof Element ? picker : null) ||
			list;
	}

	function localProjectPickerKeyboardList(target, fallback) {
		const localItemSelector = "[data-cliproxy-local-project-item]";
		if (fallback instanceof Element && globalThis.document.contains(fallback) && elementVisible(fallback) && fallback.querySelector(localItemSelector)) {
			return fallback;
		}
		const lists = [];
		const listSelector = '[cmdk-list],[data-cmdk-list],[data-slot="command-list"],[role="listbox"]';
		if (target instanceof Element && target.matches(listSelector)) {
			lists.push(target);
		}
		target.querySelectorAll?.(listSelector).forEach((element) => lists.push(element));
		return lists.find((element) => elementVisible(element) && element.querySelector(localItemSelector)) || null;
	}

	function handleLocalProjectPickerKeydown(event, picker, list) {
		if (!list.querySelector("[data-cliproxy-local-project-item]")) {
			return;
		}
		if (event.key !== "ArrowDown" && event.key !== "ArrowUp" && event.key !== "Enter" && event.key !== " ") {
			return;
		}
		if (event.key === " " && projectPickerEditableTarget(event.target)) {
			return;
		}
		const items = projectPickerNavigableItems(list);
		if (!items.length) {
			return;
		}
		const selected = items.find(projectPickerItemSelected) || items[0];
		if (event.key === "ArrowDown" || event.key === "ArrowUp") {
			event.preventDefault();
			event.stopPropagation();
			const direction = event.key === "ArrowDown" ? 1 : -1;
			const index = Math.max(0, items.indexOf(selected));
			setProjectPickerSelectedItem(list, items[(index + direction + items.length) %% items.length]);
			return;
		}
		event.preventDefault();
		event.stopPropagation();
		if (selected.dataset?.cliproxyLocalProjectItem) {
			activateLocalProjectPickerItem(picker, selected, event);
			return;
		}
		selected.dispatchEvent(new MouseEvent("pointerdown", { bubbles: true, cancelable: true, view: globalThis }));
		selected.dispatchEvent(new MouseEvent("click", { bubbles: true, cancelable: true, view: globalThis }));
	}

	function projectPickerNavigableItems(list) {
		return Array.from(list.querySelectorAll('[role="option"],button,[role="button"]')).filter((element) =>
			elementVisible(element) &&
			element.getAttribute("aria-disabled") !== "true" &&
			element.dataset?.disabled !== "true" &&
			!element.disabled);
	}

	function setProjectPickerSelectedItem(list, selected) {
		for (const item of projectPickerNavigableItems(list)) {
			const active = item === selected;
			item.setAttribute("aria-selected", active ? "true" : "false");
			item.dataset.selected = active ? "true" : "false";
		}
		selected?.scrollIntoView?.({ block: "nearest" });
	}

	function activateLocalProjectPickerItem(picker, item, event) {
		event?.preventDefault?.();
		event?.stopPropagation?.();
		const workingDirectory = normalizeWorkingDirectory(item.dataset?.cliproxyLocalProjectWorkingDirectory);
		if (!workingDirectory) {
			return;
		}
		const selectedProject = {
			name: projectPickerItemPrimaryText(item),
			workingDirectory,
		};
		rememberSelectedLocalProject(selectedProject);
		refreshLocalProjectActivators(selectedProject);
		setTimeout(() => {
			closeLocalProjectPickerViaNoProject(picker);
			setTimeout(() => refreshLocalProjectActivators(selectedProject), 50);
		}, 0);
	}

	function autoSelectCurrentProjectInPicker(picker, list) {
		if (!list || list.dataset.cliproxyCurrentProjectAutoSelected === "1") {
			return;
		}
		const noProject = localProjectPickerNoProjectItem(picker);
		if (!noProject || !projectPickerItemSelected(noProject)) {
			return;
		}
		const item = currentProjectPickerItem(picker);
		if (!item || item === noProject) {
			return;
		}
		list.dataset.cliproxyCurrentProjectAutoSelected = "1";
		setTimeout(() => {
			if (!globalThis.document.contains(item) || !localProjectPickerLooksLikeProjectPicker(picker)) {
				return;
			}
			setProjectPickerSelectedItem(list, item);
		}, 0);
	}

	function projectPickerItemSelected(item) {
		return item?.getAttribute("aria-selected") === "true" || item?.dataset?.selected === "true";
	}

	function currentProjectPickerItem(picker) {
		const workingDirectory = localWorkingDirectory();
		const targetNames = new Set();
		if (workingDirectory) {
			targetNames.add(normalizeProjectPickerName(pathBaseName(workingDirectory)));
		}
		const visibleName = visibleProjectName();
		if (visibleName) {
			targetNames.add(normalizeProjectPickerName(visibleName));
		}
		const matches = [];
		for (const element of picker.querySelectorAll('[cmdk-item],[data-cmdk-item],[data-slot="command-item"],[role="option"],button,[role="button"]')) {
			if (!elementVisible(element) || /^No Project\b/.test(projectPickerItemPrimaryText(element))) {
				continue;
			}
			if (element.dataset?.cliproxyLocalProjectItem) {
				if (normalizeWorkingDirectory(element.dataset.cliproxyLocalProjectWorkingDirectory) === workingDirectory) {
					return element;
				}
				continue;
			}
			if (targetNames.has(normalizeProjectPickerName(projectPickerItemPrimaryText(element)))) {
				matches.push(element);
			}
		}
		return matches.length === 1 ? matches[0] : null;
	}

	function projectPickerItemPrimaryText(element) {
		return (element.innerText || element.textContent || "").split(/\n+/).map((line) => line.trim()).filter(Boolean)[0] || "";
	}

	function projectPickerEditableTarget(target) {
		return target instanceof Element &&
			(target.matches("input,textarea,[contenteditable=true]") || !!target.closest("input,textarea,[contenteditable=true]"));
	}

	function closeLocalProjectPickerViaNoProject(picker) {
		const noProject = localProjectPickerNoProjectItem(picker);
		if (noProject) {
			noProject.dataset.cliproxySuppressLocalProjectClear = "1";
			try {
				noProject.dispatchEvent(new MouseEvent("pointerdown", { bubbles: true, cancelable: true, view: globalThis }));
				noProject.dispatchEvent(new MouseEvent("click", { bubbles: true, cancelable: true, view: globalThis }));
			} finally {
				delete noProject.dataset.cliproxySuppressLocalProjectClear;
			}
			return;
		}
		globalThis.document.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true, cancelable: true }));
	}

	function installLocalThreadKeyboardShortcut() {
		if (globalThis.__cliproxyAmpLocalInferenceKeyboardShortcut) {
			return;
		}
		globalThis.__cliproxyAmpLocalInferenceKeyboardShortcut = true;
		globalThis.document.addEventListener("keydown", (event) => {
			if (event.altKey && event.key === "Enter") {
				event.preventDefault();
				promptLocalThread();
			}
		}, true);
	}

	function installThreadMenuIntegration() {
		if (globalThis.__cliproxyAmpLocalInferenceMenuObserver) {
			integrateThreadMenus(globalThis.document);
			return;
		}
		const observer = new MutationObserver((mutations) => {
			for (const mutation of mutations) {
				for (const node of mutation.addedNodes) {
					if (node instanceof Element) {
						integrateThreadMenus(node);
					}
				}
			}
		});
		observer.observe(globalThis.document.documentElement, { childList: true, subtree: true });
		globalThis.__cliproxyAmpLocalInferenceMenuObserver = observer;
		integrateThreadMenus(globalThis.document);
	}

	function installCommandPaletteIntegration() {
		if (globalThis.__cliproxyAmpLocalInferenceCommandPaletteObserver) {
			integrateCommandPalettes(globalThis.document);
			return;
		}
		const observer = new MutationObserver((mutations) => {
			for (const mutation of mutations) {
				for (const node of mutation.addedNodes) {
					if (node instanceof Element) {
						integrateCommandPalettes(node);
					}
				}
			}
		});
		observer.observe(globalThis.document.documentElement, { childList: true, subtree: true });
		globalThis.__cliproxyAmpLocalInferenceCommandPaletteObserver = observer;
		integrateCommandPalettes(globalThis.document);
	}

	function integrateCommandPalettes(root) {
		for (const palette of commandPaletteCandidates(root)) {
			if (!commandPaletteLooksLikePalette(palette)) {
				continue;
			}
			const list = commandPaletteList(palette);
			if (!list || list.querySelector("[data-cliproxy-local-thread-command-item]")) {
				continue;
			}
			const before = list.firstElementChild;
			for (const choice of localThreadModeChoices()) {
				const item = buildCommandPaletteItem(palette, choice);
				if (!item) {
					continue;
				}
				list.insertBefore(item, before);
				diagnostics.commandPaletteIntegrationCount += 1;
			}
		}
	}

	function commandPaletteCandidates(root) {
		const out = [];
		const add = (value) => {
			if (value instanceof Element && !out.includes(value)) {
				out.push(value);
			}
		};
		const selector = '[cmdk-root],[data-cmdk-root],[role="dialog"]';
		if (root instanceof Element) {
			if (root.matches(selector)) {
				add(root);
			}
			root.querySelectorAll?.(selector).forEach(add);
		} else {
			root.querySelectorAll?.(selector).forEach(add);
		}
		return out;
	}

	function commandPaletteLooksLikePalette(palette) {
		if (!elementVisible(palette) || !palette.querySelector("input,textarea,[contenteditable=true]")) {
			return false;
		}
		return !!commandPaletteList(palette) &&
			(!!palette.querySelector('[cmdk-item],[data-cmdk-item],[data-slot="command-item"],[role="option"]') ||
				!!palette.querySelector('[cmdk-input],[data-cmdk-input],[data-slot="command-input"]'));
	}

	function commandPaletteList(palette) {
		if (!(palette instanceof Element)) {
			return null;
		}
		if (palette.matches('[cmdk-list],[data-cmdk-list],[data-slot="command-list"],[role="listbox"]')) {
			return palette;
		}
		return palette.querySelector('[cmdk-list],[data-cmdk-list],[data-slot="command-list"],[role="listbox"]');
	}

	function buildCommandPaletteItem(palette, choice) {
		const template = palette.querySelector('[cmdk-item],[data-cmdk-item],[data-slot="command-item"],[role="option"]');
		const item = template ? template.cloneNode(true) : globalThis.document.createElement("div");
		const label = "Local thread: " + (choice.id === "inherit" ? choice.label + " (" + choice.detail + ")" : choice.label);
		item.dataset.cliproxyLocalThreadCommandItem = "1";
		item.removeAttribute("id");
		item.removeAttribute("aria-selected");
		item.removeAttribute("data-selected");
		item.removeAttribute("aria-disabled");
		item.removeAttribute("data-disabled");
		item.removeAttribute("hidden");
		item.removeAttribute("disabled");
		item.setAttribute("data-value", label);
		item.setAttribute("aria-label", label);
		item.setAttribute("role", item.getAttribute("role") || "option");
		item.setAttribute("tabindex", "-1");
		if (!template) {
			item.style.cssText = "display:flex;align-items:center;gap:12px;width:100%%;padding:10px 12px;border:0;background:transparent;color:inherit;font:inherit;text-align:left;cursor:default";
		}
		replaceMenuItemText(item, label);
		stripCommandPaletteShortcut(item);
		const activate = (event) => {
			event.preventDefault();
			event.stopPropagation();
			setTimeout(() => openLocalThreadFromMenu(choice.options), 0);
		};
		item.addEventListener("pointerdown", activate, true);
		item.addEventListener("click", activate, true);
		item.addEventListener("keydown", (event) => {
			if (event.key === "Enter") {
				activate(event);
			}
		}, true);
		return item;
	}

	function stripCommandPaletteShortcut(item) {
		item.removeAttribute("aria-keyshortcuts");
		item.querySelectorAll("kbd,[aria-keyshortcuts],[cmdk-shortcut],[data-cmdk-shortcut],[data-slot='command-shortcut']").forEach((element) => element.remove());
		item.querySelectorAll("*").forEach((element) => {
			if (element.children.length > 0) {
				return;
			}
			const className = String(element.className || "").toLowerCase();
			const text = (element.textContent || "").replace(/\s+/g, " ").trim();
			if (className.includes("shortcut") || commandShortcutText(text)) {
				element.remove();
			}
		});
	}

	function commandShortcutText(text) {
		const compact = text.replace(/\s+/g, "").toLowerCase();
		return compact.length > 0 && compact.length <= 16 &&
			(/[\u2318\u21e7\u2325\u2303]/.test(text) ||
				compact.includes("cmd") ||
				compact.includes("ctrl") ||
				compact.includes("alt") ||
				compact.includes("shift") ||
				compact.includes("enter"));
	}

	function elementVisible(element) {
		if (!(element instanceof Element)) {
			return false;
		}
		const rect = element.getBoundingClientRect();
		return rect.width > 0 && rect.height > 0;
	}

	function integrateThreadMenus(root) {
		for (const menu of threadMenuCandidates(root)) {
			if (!threadMenuLooksLikeThreadMenu(menu) || menu.querySelector("[data-cliproxy-local-thread-menu-item]")) {
				continue;
			}
			const item = buildThreadMenuItem(menu);
			if (!item) {
				continue;
			}
			const before = findMenuRowByText(menu, "Generate Diagnostic Report");
			if (before && before.parentElement === menu) {
				menu.insertBefore(item, before);
			} else {
				menu.appendChild(item);
			}
			diagnostics.menuIntegrationCount += 1;
		}
	}

	function threadMenuCandidates(root) {
		const out = [];
		const add = (value) => {
			if (value instanceof Element && !out.includes(value)) {
				out.push(value);
			}
		};
		if (root instanceof Element) {
			if (root.matches('[role="menu"],[data-radix-menu-content],[data-slot="dropdown-menu-content"]')) {
				add(root);
			}
			root.querySelectorAll?.('[role="menu"],[data-radix-menu-content],[data-slot="dropdown-menu-content"]').forEach(add);
		} else {
			root.querySelectorAll?.('[role="menu"],[data-radix-menu-content],[data-slot="dropdown-menu-content"]').forEach(add);
		}
		return out;
	}

	function threadMenuLooksLikeThreadMenu(menu) {
		const text = (menu.innerText || menu.textContent || "").replace(/\s+/g, " ").trim();
		return text.includes("Copy") &&
			text.includes("Export") &&
			text.includes("Archive") &&
			text.includes("Delete") &&
			(text.includes("Generate Diagnostic Report") || text.includes("Visibility"));
	}

	function buildThreadMenuItem(menu) {
		const template = findMenuRowByText(menu, "Rename") ||
			findMenuRowByText(menu, "Pin") ||
			findMenuRowByText(menu, "Generate Diagnostic Report");
		const item = template ? template.cloneNode(true) : globalThis.document.createElement("button");
		item.dataset.cliproxyLocalThreadMenuItem = "1";
		item.removeAttribute("id");
		item.removeAttribute("data-state");
		item.removeAttribute("aria-expanded");
		item.setAttribute("role", item.getAttribute("role") || "menuitem");
		item.setAttribute("tabindex", "-1");
		if (!template) {
			item.type = "button";
			item.style.cssText = "display:flex;align-items:center;gap:14px;width:100%%;padding:10px 14px;border:0;background:transparent;color:inherit;font:inherit;text-align:left";
		}
		replaceMenuItemText(item, "Local thread");
		appendMenuItemChevron(item);
		const openPicker = (event) => {
			event.preventDefault();
			event.stopPropagation();
			showLocalThreadPicker(item);
		};
		item.addEventListener("pointerdown", openPicker, true);
		item.addEventListener("click", openPicker, true);
		item.addEventListener("keydown", (event) => {
			if (event.key === "Enter" || event.key === " ") {
				openPicker(event);
			}
		}, true);
		return item;
	}

	function findMenuRowByText(menu, text) {
		const walker = globalThis.document.createTreeWalker(menu, NodeFilter.SHOW_ELEMENT);
		let node = walker.currentNode;
		while (node) {
			if (node !== menu && menuRowText(node) === text) {
				return menuRowElement(node, menu);
			}
			node = walker.nextNode();
		}
		return null;
	}

	function menuRowText(element) {
		const text = (element.innerText || element.textContent || "").replace(/\s+/g, " ").trim();
		return text;
	}

	function menuRowElement(element, menu) {
		let current = element;
		while (current && current.parentElement && current.parentElement !== menu) {
			current = current.parentElement;
		}
		return current || element;
	}

	function replaceMenuItemText(item, text) {
		const walker = globalThis.document.createTreeWalker(item, NodeFilter.SHOW_TEXT);
		let fallback = null;
		let node = walker.nextNode();
		while (node) {
			const value = node.nodeValue.replace(/\s+/g, " ").trim();
			if (value) {
				if (!fallback) {
					fallback = node;
				}
				if (["Rename", "Pin", "Generate Diagnostic Report"].includes(value)) {
					node.nodeValue = node.nodeValue.replace(value, text);
					return;
				}
			}
			node = walker.nextNode();
		}
		if (fallback) {
			fallback.nodeValue = text;
			return;
		}
		item.textContent = text;
	}

	function appendMenuItemChevron(item) {
		if (item.querySelector("[data-cliproxy-local-thread-menu-chevron]")) {
			return;
		}
		const chevron = globalThis.document.createElement("span");
		chevron.dataset.cliproxyLocalThreadMenuChevron = "1";
		chevron.setAttribute("aria-hidden", "true");
		chevron.style.cssText = [
			"display:inline-block",
			"width:7px",
			"height:7px",
			"margin-left:auto",
			"border-right:1.8px solid currentColor",
			"border-bottom:1.8px solid currentColor",
			"opacity:.62",
			"transform:rotate(-45deg)",
			"flex:0 0 auto",
		].join(";");
		item.appendChild(chevron);
	}

	function handleNewThreadIntent() {
		const url = new URL(globalThis.location.href);
		if (url.searchParams.get("newThread") !== "1") {
			return;
		}
		url.searchParams.delete("newThread");
		globalThis.history.replaceState(globalThis.history.state, "", url.pathname + url.search + url.hash);
		setTimeout(promptLocalThread, 50);
	}

	function sameLocalWebSocketBase(url, base) {
		const protocol = base.protocol === "https:" ? "wss:" : "ws:";
		return url.protocol === protocol && url.host === base.host;
	}

	function localWebSocketURL(rawURL) {
		const source = new URL(String(rawURL), globalThis.location.href);
		const base = localBaseURL();
		if (!shouldRewriteWebSocket(source, base)) {
			return rawURL;
		}
		const alreadyLocal = sameLocalWebSocketBase(source, base);
		const local = alreadyLocal ? new URL(source.href) : new URL(source.pathname + source.search + source.hash, base);
		const userActorSocket = shouldBridgeUserActorWebSocket(source);
		const apiKey = local.searchParams.get("cliproxy-api-key") || (userActorSocket ? storedLocalAPIKey() : localAPIKey());
		if (userActorSocket && !apiKey) {
			return rawURL;
		}
		if (apiKey && !local.searchParams.has("cliproxy-api-key")) {
			local.searchParams.set("cliproxy-api-key", apiKey);
		}
		const threadID = rememberObservedThreadID(threadIDFromGatewayURL(source));
		local.protocol = base.protocol === "https:" ? "wss:" : "ws:";
		diagnostics.webSocketRewriteCount += 1;
		diagnostics.lastWebSocketHost = local.host;
		diagnostics.lastWebSocketPath = local.pathname;
		diagnostics.lastWebSocketThreadKey = threadID || local.searchParams.get("rvt-key") || "";
		diagnostics.lastWebSocketBootstrapped = false;
		if (shouldBootstrapExecutor(source)) {
			const workingDirectory = localWorkingDirectory();
			if (workingDirectory && !local.searchParams.has("cliproxy-working-directory")) {
				local.searchParams.set("cliproxy-working-directory", workingDirectory);
			}
			const mode = localThreadModeOptions();
			if (mode.agentMode && !local.searchParams.has("cliproxy-agent-mode")) {
				local.searchParams.set("cliproxy-agent-mode", mode.agentMode);
			}
			if (mode.reasoningEffort && !local.searchParams.has("cliproxy-reasoning-effort")) {
				local.searchParams.set("cliproxy-reasoning-effort", mode.reasoningEffort);
			}
			local.searchParams.set("cliproxy-client", "amp-web-local-inference");
			local.searchParams.set("cliproxy-bootstrap-executor", "true");
			diagnostics.webSocketBootstrapCount += 1;
			diagnostics.lastWebSocketBootstrapped = true;
		}
		return local.href;
	}

	globalThis.JSON.parse = function(text, reviver) {
		const parsed = originalJSONParse(text, reviver);
		try {
			const patchOptions = parsedTextLocalInferencePatchOptions(text);
			if (patchOptions.configs) {
				patchDecodedLocalInference(parsed, patchOptions);
			}
		} catch {
		}
		return parsed;
	};

	globalThis.fetch = function(input, init) {
		const request = input instanceof Request ? input : null;
		const sourceURL = new URL(request ? request.url : String(input), globalThis.location.href);
		if (!shouldBridgeHTTP(sourceURL)) {
			return originalFetch(input, init);
		}

		diagnostics.fetchRewriteCount += 1;
		const sourceHeaders = new Headers(init?.headers || request?.headers || undefined);
		const headers = localFetchHeaders(sourceHeaders.get("Content-Type"));

		const requestOptions = request ? {
			method: request.method,
			body: request.body,
			cache: request.cache,
			redirect: request.redirect,
			referrer: request.referrer,
			integrity: request.integrity,
			keepalive: request.keepalive,
			signal: request.signal,
		} : {};
		const method = init?.method || request?.method || "GET";
		const targetURLForBody = (body) => shouldRewriteHTTP(sourceURL) ? localHTTPURL(sourceURL, body) : sourceURL.href;
		const makeOptions = (body) => {
			const options = Object.assign({}, requestOptions, init || {}, { headers, body, mode: "cors", credentials: "omit" });
			if (bodyNeedsDuplex(body)) {
				options.duplex = "half";
			}
			return options;
		};
		if (bodyNeedsTextBridge(sourceURL, method, request, init)) {
			try {
				return request.clone().text().then((text) => {
					const body = bridgeRequestBody(sourceURL, method, text);
					return originalFetch(targetURLForBody(body), makeOptions(body)).then((response) => {
						rememberRemoteCreateProjectThread(sourceURL, body, response);
						return response;
					});
				});
			} catch {
			}
		}
		const body = bridgeRequestBody(sourceURL, method, init?.body ?? requestOptions.body);
		const options = makeOptions(body);
		return originalFetch(targetURLForBody(body), options).then((response) => {
			rememberRemoteCreateProjectThread(sourceURL, body, response);
			return response;
		});
	};

	globalThis.WebSocket = new Proxy(NativeWebSocket, {
		construct(target, args, newTarget) {
			diagnostics.lastWebSocketProtocols = JSON.stringify(args.length > 1 ? args[1] : "");
			if (args.length > 0) {
				args[0] = localWebSocketURL(args[0]);
			}
			const socket = Reflect.construct(target, args, newTarget);
			try {
				diagnostics.lastWebSocketState = "constructed";
				diagnostics.lastWebSocketReadyState = Number(socket.readyState);
				socket.addEventListener("open", () => {
					diagnostics.webSocketOpenCount += 1;
					diagnostics.activeWebSocketCount += 1;
					diagnostics.lastWebSocketState = "open";
					diagnostics.lastWebSocketReadyState = Number(socket.readyState);
				});
				socket.addEventListener("close", (event) => {
					diagnostics.webSocketCloseCount += 1;
					diagnostics.activeWebSocketCount = Math.max(0, diagnostics.activeWebSocketCount - 1);
					diagnostics.lastWebSocketState = "closed";
					diagnostics.lastWebSocketReadyState = Number(socket.readyState);
					diagnostics.lastWebSocketCloseCode = Number(event?.code || 0);
					diagnostics.lastWebSocketCloseReason = String(event?.reason || "");
				});
				socket.addEventListener("error", () => {
					diagnostics.webSocketErrorCount += 1;
					diagnostics.lastWebSocketState = "error";
					diagnostics.lastWebSocketReadyState = Number(socket.readyState);
				});
				globalThis.setTimeout(() => {
					diagnostics.lastWebSocketReadyState = Number(socket.readyState);
					if (diagnostics.lastWebSocketState === "constructed") {
						diagnostics.lastWebSocketState = ["connecting", "open", "closing", "closed"][socket.readyState] || "unknown";
					}
				}, 3000);
			} catch {
			}
			return socket;
		},
	});

	globalThis.__cliproxyAmpLocalInference = {
		apiKeyStorageKey,
		workingDirectoryStorageKey,
		selectedLocalProjectStorageKey,
		localThreadIDsStorageKey,
		threadWorkingDirectoriesStorageKey,
		defaultBaseURL,
		diagnostics,
		removeInjectedLocalThreadControls,
	};

	if (globalThis.document.readyState === "loading") {
		globalThis.document.addEventListener("DOMContentLoaded", () => {
			installLocalThreadControls();
		}, { once: true });
	} else {
		installLocalThreadControls();
	}

	})();
`, matchLines, userscriptURL, userscriptURL, strconv.Quote(ampWebLocalInferenceHeader), strconv.Quote(baseURL))
}

func ampWebLocalInferenceUserscriptMatches(allowedOrigins []string) string {
	if allowedOrigins == nil {
		allowedOrigins = defaultAmpWebLocalInferenceOrigins
	}
	lines := make([]string, 0, len(allowedOrigins))
	seen := map[string]bool{}
	for _, origin := range allowedOrigins {
		normalized := normalizeAmpWebLocalInferenceOrigin(origin)
		if normalized == "" || seen[normalized] {
			continue
		}
		seen[normalized] = true
		lines = append(lines, "// @match "+normalized+"/*")
	}
	return strings.Join(lines, "\n")
}

func ampWebLocalInferenceSafeBaseURL(rawBaseURL string) string {
	parsed, err := url.Parse(strings.TrimSpace(rawBaseURL))
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return ampWebLocalInferenceDefaultBaseURL
	}
	if parsed.Path != "" && parsed.Path != "/" {
		return ampWebLocalInferenceDefaultBaseURL
	}
	parsed.RawQuery = ""
	parsed.Fragment = ""
	parsed.Path = ""
	return parsed.String()
}
