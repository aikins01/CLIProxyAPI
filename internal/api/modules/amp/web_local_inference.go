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

func ampWebLocalInferenceUserscript(defaultBaseURL string, allowedOrigins []string) string {
	baseURL := ampWebLocalInferenceSafeBaseURL(defaultBaseURL)
	userscriptURL := strings.NewReplacer("\r", "", "\n", "").Replace(baseURL + "/ampcode/local-inference.user.js")
	matchLines := ampWebLocalInferenceUserscriptMatches(allowedOrigins)
	return fmt.Sprintf(`// ==UserScript==
// @name CLIProxyAPI Amp Local Inference
// @namespace https://github.com/router-for-me/CLIProxyAPI
// @version 0.1.30
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
	const localThreadIDsStorageKey = "cliproxyapi.ampLocalInference.localThreadIDs";
	const threadWorkingDirectoriesStorageKey = "cliproxyapi.ampLocalInference.threadWorkingDirectories";
	const threadSettingsStorageKey = "cliproxyapi.ampLocalInference.threadSettings";
	const defaultBaseURL = %s;
	const originalJSONParse = globalThis.JSON.parse.bind(globalThis.JSON);
	const originalFetch = globalThis.fetch.bind(globalThis);
	const NativeWebSocket = globalThis.WebSocket;
	const diagnostics = {
		decodedConfigPatchCount: 0,
		fetchRewriteCount: 0,
		webSocketRewriteCount: 0,
		webSocketBootstrapCount: 0,
		menuIntegrationCount: 0,
		commandPaletteIntegrationCount: 0,
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
		lastWebSocketHost: "",
		lastWebSocketPath: "",
		lastWebSocketThreadKey: "",
		lastWebSocketBootstrapped: false,
		lastInheritedWorkingDirectory: "",
		lastInheritedWorkingDirectoryThreadID: "",
		lastObservedThreadID: "",
		localSidebarSeedCount: 0,
	};
	let observedThreadID = "";

	function localBaseURL() {
		return new URL(defaultBaseURL);
	}

	function localBaseURLString() {
		return localBaseURL().href.replace(/\/+$/, "");
	}

	function storedLocalAPIKey() {
		return globalThis.sessionStorage.getItem(apiKeyStorageKey) || "";
	}

	function localAPIKey() {
		const apiKey = storedLocalAPIKey();
		if (apiKey) {
			return apiKey;
		}
		const promptedKey = "cliproxyapi.ampLocalInference.promptedAPIKey";
		if (globalThis.sessionStorage.getItem(promptedKey) === "1") {
			return "";
		}
		globalThis.sessionStorage.setItem(promptedKey, "1");
		const promptedAPIKey = (globalThis.prompt("CLIProxyAPI API key") || "").trim();
		if (promptedAPIKey) {
			globalThis.sessionStorage.setItem(apiKeyStorageKey, promptedAPIKey);
		}
		return promptedAPIKey;
	}

	function localWorkingDirectory() {
		const activeDirectory = activeThreadWorkingDirectory();
		if (activeDirectory) {
			return activeDirectory;
		}
		return (globalThis.localStorage.getItem(workingDirectoryStorageKey) || "").trim();
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
		seedLocalSidebarProjectForWorkingDirectory(workingDirectory);
		scheduleLocalSidebarProjectsRefresh(workingDirectory);
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

	function patchDevalueThreadActorConfig(values, index, baseIndex) {
		if (!Number.isInteger(index) || index < 0 || index >= values.length) {
			return false;
		}
		const config = values[index];
		if (!devalueThreadActorConfigLike(config)) {
			return false;
		}
		if (Number.isInteger(config.threadId)) {
			rememberObservedThreadID(values[config.threadId]);
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

	function patchDevalueThreadRuntime(values, threadIndex, executorTypeIndex, connectedIndex) {
		if (!Number.isInteger(threadIndex) || threadIndex < 0 || threadIndex >= values.length) {
			return false;
		}
		const thread = values[threadIndex];
		const activeThread = activeThreadID();
		if (!isPlainObject(thread) || !activeThread || devalueThreadID(values, thread) !== activeThread) {
			return false;
		}
		rememberThreadWorkingDirectory(activeThread, devalueThreadWorkingDirectory(values, thread));
		let patched = false;
		if (thread.hasExecutor !== connectedIndex) {
			thread.hasExecutor = connectedIndex;
			patched = true;
		}
		if (thread.executorConnected !== connectedIndex) {
			thread.executorConnected = connectedIndex;
			patched = true;
		}
		if (Number.isInteger(thread.meta)) {
			const meta = values[thread.meta];
			if (isPlainObject(meta) && meta.executorType !== executorTypeIndex) {
				meta.executorType = executorTypeIndex;
				patched = true;
			}
		}
		if (patched) {
			diagnostics.lastPatchedThreadID = activeThread;
		}
		return patched;
	}

	function patchDevalueThreadActorConfigs(values, localBase) {
		if (!Array.isArray(values)) {
			return false;
		}
		let patched = false;
		let baseIndex = -1;
		let executorTypeIndex = -1;
		const getBaseIndex = () => {
			if (baseIndex === -1) {
				baseIndex = ensureDevalueStringIndex(values, localBase);
			}
			return baseIndex;
		};
		const getExecutorTypeIndex = () => {
			if (executorTypeIndex === -1) {
				executorTypeIndex = ensureDevalueStringIndex(values, "local-client");
			}
			return executorTypeIndex;
		};
		let connectedIndex = -1;
		const getConnectedIndex = () => {
			if (connectedIndex === -1) {
				connectedIndex = ensureDevalueValueIndex(values, true);
			}
			return connectedIndex;
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
				patched = patchDevalueThreadRuntime(values, i, getExecutorTypeIndex(), getConnectedIndex()) || patched;
			}
			rememberDevalueThreadSettingsMessage(values, entry);
			if (Number.isInteger(entry.threadActorConfig)) {
				const config = values[entry.threadActorConfig];
				if (devalueThreadActorConfigLike(config)) {
					patched = patchDevalueThreadActorConfig(values, entry.threadActorConfig, getBaseIndex()) || patched;
					if (Number.isInteger(entry.thread)) {
						patched = patchDevalueThreadRuntime(values, entry.thread, getExecutorTypeIndex(), getConnectedIndex()) || patched;
			}
			}
			}
			if (devalueThreadActorConfigLike(entry)) {
				patched = patchDevalueThreadActorConfig(values, i, getBaseIndex()) || patched;
			}
		}
		return patched;
	}

	let cachedLocalSidebarProjects = [];
	let cachedLocalSidebarRecentThreads = [];
	let localSidebarRefreshWorkingDirectory = "";
	let localSidebarRefreshTimer = 0;

	function localGoFilePathEscape(part) {
		return encodeURIComponent(part)
			.replace(/[!'()*]/g, (value) => "%%" + value.charCodeAt(0).toString(16).toUpperCase())
			.replace(/%%24/g, "$")
			.replace(/%%26/g, "&")
			.replace(/%%2B/gi, "+")
			.replace(/%%2C/gi, ",")
			.replace(/%%3B/gi, ";")
			.replace(/%%3D/gi, "=")
			.replace(/%%3A/gi, ":")
			.replace(/%%40/g, "@");
	}

	function localFileURLForDirectory(workingDirectory) {
		return "file://" + normalizeWorkingDirectory(workingDirectory).split("/").map(localGoFilePathEscape).join("/");
	}

	function localUTF8Bytes(value) {
		const text = String(value);
		const bytes = [];
		for (let i = 0; i < text.length; i += 1) {
			let code = text.charCodeAt(i);
			if (code >= 0xd800 && code <= 0xdbff && i + 1 < text.length) {
				const next = text.charCodeAt(i + 1);
				if (next >= 0xdc00 && next <= 0xdfff) {
					code = 0x10000 + ((code - 0xd800) << 10) + (next - 0xdc00);
					i += 1;
				}
			}
			if (code < 0x80) {
				bytes.push(code);
			} else if (code < 0x800) {
				bytes.push(0xc0 | (code >> 6), 0x80 | (code & 0x3f));
			} else if (code < 0x10000) {
				bytes.push(0xe0 | (code >> 12), 0x80 | ((code >> 6) & 0x3f), 0x80 | (code & 0x3f));
			} else {
				bytes.push(0xf0 | (code >> 18), 0x80 | ((code >> 12) & 0x3f), 0x80 | ((code >> 6) & 0x3f), 0x80 | (code & 0x3f));
			}
		}
		return bytes;
	}

	function localRotateLeft(value, bits) {
		return (value << bits) | (value >>> (32 - bits));
	}

	function localSHA1Bytes(bytes) {
		const words = [];
		const bitLength = bytes.length * 8;
		for (let i = 0; i < bytes.length; i += 1) {
			words[i >> 2] = (words[i >> 2] || 0) | (bytes[i] << (24 - ((i %% 4) * 8)));
		}
		words[bytes.length >> 2] = (words[bytes.length >> 2] || 0) | (0x80 << (24 - ((bytes.length %% 4) * 8)));
		const lengthOffset = (((bytes.length + 8) >> 6) << 4) + 14;
		words[lengthOffset] = Math.floor(bitLength / 0x100000000);
		words[lengthOffset + 1] = bitLength >>> 0;
		let h0 = 0x67452301;
		let h1 = 0xefcdab89;
		let h2 = 0x98badcfe;
		let h3 = 0x10325476;
		let h4 = 0xc3d2e1f0;
		for (let i = 0; i < words.length; i += 16) {
			const w = [];
			for (let j = 0; j < 16; j += 1) {
				w[j] = words[i + j] || 0;
			}
			for (let j = 16; j < 80; j += 1) {
				w[j] = localRotateLeft(w[j - 3] ^ w[j - 8] ^ w[j - 14] ^ w[j - 16], 1);
			}
			let a = h0;
			let b = h1;
			let c = h2;
			let d = h3;
			let e = h4;
			for (let j = 0; j < 80; j += 1) {
				let f;
				let k;
				if (j < 20) {
					f = (b & c) | ((~b) & d);
					k = 0x5a827999;
				} else if (j < 40) {
					f = b ^ c ^ d;
					k = 0x6ed9eba1;
				} else if (j < 60) {
					f = (b & c) | (b & d) | (c & d);
					k = 0x8f1bbcdc;
				} else {
					f = b ^ c ^ d;
					k = 0xca62c1d6;
				}
				const temp = (localRotateLeft(a, 5) + f + e + k + w[j]) | 0;
				e = d;
				d = c;
				c = localRotateLeft(b, 30);
				b = a;
				a = temp;
			}
			h0 = (h0 + a) | 0;
			h1 = (h1 + b) | 0;
			h2 = (h2 + c) | 0;
			h3 = (h3 + d) | 0;
			h4 = (h4 + e) | 0;
		}
		const digest = [];
		for (const word of [h0, h1, h2, h3, h4]) {
			digest.push((word >>> 24) & 0xff, (word >>> 16) & 0xff, (word >>> 8) & 0xff, word & 0xff);
		}
		return digest;
	}

	function localHexByte(value) {
		return value.toString(16).padStart(2, "0");
	}

	function localProjectIDForWorkingDirectory(name, repositoryURL, workingDirectory) {
		const keyParts = [name, repositoryURL, workingDirectory].map((value) => typeof value === "string" ? value : "");
		let key = keyParts.join("\x00");
		if (keyParts.every((value) => value.trim() === "")) {
			key = "local";
		}
		const namespaceURL = [0x6b, 0xa7, 0xb8, 0x11, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8];
		const hash = localSHA1Bytes(namespaceURL.concat(localUTF8Bytes(key))).slice(0, 16);
		hash[6] = (hash[6] & 0x0f) | 0x50;
		hash[8] = (hash[8] & 0x3f) | 0x80;
		const hex = hash.map(localHexByte);
		return hex.slice(0, 4).join("") + "-" + hex.slice(4, 6).join("") + "-" + hex.slice(6, 8).join("") + "-" + hex.slice(8, 10).join("") + "-" + hex.slice(10, 16).join("");
	}

	function seedLocalSidebarProjectForWorkingDirectory(workingDirectory) {
		workingDirectory = normalizeWorkingDirectory(workingDirectory);
		if (!workingDirectory) {
			return;
		}
		const name = pathBaseName(workingDirectory) || "local";
		const repositoryURL = localFileURLForDirectory(workingDirectory);
		const projectID = localProjectIDForWorkingDirectory(name, repositoryURL, workingDirectory);
		if (cachedLocalSidebarProjects.some((project) => isPlainObject(project) && normalizeWorkingDirectory(project.workingDirectory) === workingDirectory)) {
			return;
		}
		cachedLocalSidebarProjects = [{
			id: projectID,
			projectID,
			name,
			namespace: "local",
			repositoryURL,
			workingDirectory,
		}, ...cachedLocalSidebarProjects];
		diagnostics.localSidebarSeedCount += 1;
		diagnostics.localSidebarProjectCount = cachedLocalSidebarProjects.length;
	}

	function seedLocalSidebarProjects() {
		seedLocalSidebarProjectForWorkingDirectory(localWorkingDirectory());
	}

	function scheduleLocalSidebarProjectsRefresh(workingDirectory) {
		workingDirectory = normalizeWorkingDirectory(workingDirectory);
		if (!workingDirectory || workingDirectory === localSidebarRefreshWorkingDirectory) {
			return;
		}
		localSidebarRefreshWorkingDirectory = workingDirectory;
		if (localSidebarRefreshTimer) {
			return;
		}
		localSidebarRefreshTimer = setTimeout(() => {
			localSidebarRefreshTimer = 0;
			localSidebarRefreshWorkingDirectory = "";
			refreshLocalSidebarProjects();
		}, 0);
	}

	function refreshLocalSidebarProjects() {
		seedLocalSidebarProjects();
		const base = localBaseURLString();
		if (!base || !storedLocalAPIKey()) {
			return;
		}
		const url = new URL(base + "/_app/remote/cliproxy/listThreadListSidebar");
		url.searchParams.set("payload", "e30");
		const workingDirectory = localWorkingDirectory();
		if (workingDirectory) {
			url.searchParams.set("cliproxy-working-directory", workingDirectory);
		}
		originalFetch(url.href, {
			headers: localFetchHeaders("", false),
			mode: "cors",
			credentials: "omit",
		}).then((response) => response.ok ? response.text() : "").then((text) => {
			const envelope = text ? originalJSONParse(text) : null;
			const decoded = isPlainObject(envelope) && typeof envelope.data === "string" ? decodeDevalueString(envelope.data) : null;
			const result = isPlainObject(decoded) ? decoded._ : null;
			const projects = isPlainObject(result) && Array.isArray(result.projects) ? result.projects : null;
			const recentThreads = isPlainObject(result) && Array.isArray(result.recentThreads) ? result.recentThreads : null;
			if (!projects && !recentThreads) {
				return;
			}
			cachedLocalSidebarProjects = (projects || []).filter((project) => isPlainObject(project) && typeof project.name === "string" && project.name !== "");
			cachedLocalSidebarRecentThreads = (recentThreads || []).filter((thread) => isPlainObject(thread) && validThreadID(firstString(thread.id, thread.threadId, thread.threadID)));
			diagnostics.localSidebarProjectCount = cachedLocalSidebarProjects.length;
			diagnostics.localSidebarRecentThreadCount = cachedLocalSidebarRecentThreads.length;
		}).catch(() => {
		});
	}

	const sidebarDateFields = new Set(["updatedAt", "firstSyncAt", "lastUserMessageAt", "createdAt", "userLastInteractedAt"]);

	function sidebarDateISOString(value) {
		let date = null;
		if (value instanceof Date) {
			date = value;
		} else if (typeof value === "string" && value.trim() !== "") {
			date = new Date(value);
		} else if (typeof value === "number" && Number.isFinite(value) && value > 0) {
			date = new Date(value);
		}
		if (!date || !Number.isFinite(date.getTime())) {
			return "";
		}
		return date.toISOString();
	}

	function appendDevalueSidebarDateValue(values, value) {
		const iso = sidebarDateISOString(value);
		if (!iso) {
			return -1;
		}
		values.push(["Date", iso]);
		return values.length - 1;
	}

	function appendDevalueSidebarValue(values, value, key = "") {
		if (sidebarDateFields.has(key)) {
			const dateIndex = appendDevalueSidebarDateValue(values, value);
			if (dateIndex >= 0) {
				return dateIndex;
			}
		}
		if (value instanceof Date) {
			const dateIndex = appendDevalueSidebarDateValue(values, value);
			return dateIndex >= 0 ? dateIndex : appendDevalueSidebarValue(values, null);
		}
		if (value === null || typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
			values.push(value);
			return values.length - 1;
		}
		if (Array.isArray(value)) {
			const list = value.map((item) => appendDevalueSidebarValue(values, item));
			values.push(list);
			return values.length - 1;
		}
		if (!isPlainObject(value)) {
			values.push(null);
			return values.length - 1;
		}
		const object = {};
		for (const [key, child] of Object.entries(value)) {
			if (typeof child === "undefined") {
				continue;
			}
			object[key] = appendDevalueSidebarValue(values, child, key);
		}
		values.push(object);
		return values.length - 1;
	}

	function appendDevalueSidebarProject(values, project) {
		const object = {};
		for (const [key, value] of Object.entries(project)) {
			if (typeof value !== "string" || value === "") {
				continue;
			}
			object[key] = ensureDevalueStringIndex(values, value);
		}
		values.push(object);
		return values.length - 1;
	}

	function devalueSidebarProjectKeys(values, list) {
		const keys = new Set();
		for (const ref of list) {
			const project = Number.isInteger(ref) ? values[ref] : null;
			if (!isPlainObject(project)) {
				continue;
			}
			for (const field of ["projectID", "id", "name"]) {
				const value = Number.isInteger(project[field]) ? values[project[field]] : null;
				if (typeof value === "string" && value !== "") {
					keys.add(field === "name" ? value.toLowerCase() : value);
				}
			}
		}
		return keys;
	}

	function devalueSidebarThreadIDs(values, list) {
		const ids = new Set();
		for (const ref of list) {
			const thread = Number.isInteger(ref) ? values[ref] : null;
			if (!isPlainObject(thread)) {
				continue;
			}
			for (const field of ["id", "threadId", "threadID"]) {
				const value = Number.isInteger(thread[field]) ? values[thread[field]] : null;
				if (validThreadID(value)) {
					ids.add(value);
				}
			}
		}
		return ids;
	}

	function mergeDevalueSidebarProjects(values) {
		if (!Array.isArray(values) || (cachedLocalSidebarProjects.length === 0 && cachedLocalSidebarRecentThreads.length === 0)) {
			return false;
		}
		let merged = false;
		const originalLength = values.length;
		for (let i = 0; i < originalLength; i += 1) {
			const entry = values[i];
			if (!isPlainObject(entry) || !Number.isInteger(entry.projects) || !Number.isInteger(entry.recentThreads)) {
				continue;
			}
			const list = values[entry.projects];
			if (!Array.isArray(list)) {
				continue;
			}
			const existing = devalueSidebarProjectKeys(values, list);
			for (const project of cachedLocalSidebarProjects) {
				const nameKey = project.name.toLowerCase();
				if (existing.has(nameKey) || (typeof project.projectID === "string" && existing.has(project.projectID))) {
					continue;
				}
				list.push(appendDevalueSidebarProject(values, project));
				existing.add(nameKey);
				merged = true;
			}
			const recentThreads = values[entry.recentThreads];
			if (Array.isArray(recentThreads)) {
				const existingThreadIDs = devalueSidebarThreadIDs(values, recentThreads);
				for (const thread of cachedLocalSidebarRecentThreads) {
					const threadID = firstString(thread.id, thread.threadId, thread.threadID);
					if (!validThreadID(threadID) || existingThreadIDs.has(threadID)) {
						continue;
					}
					recentThreads.unshift(appendDevalueSidebarValue(values, thread));
					existingThreadIDs.add(threadID);
					merged = true;
				}
			}
		}
		if (merged) {
			diagnostics.sidebarProjectMergeCount = (diagnostics.sidebarProjectMergeCount || 0) + 1;
		}
		return merged;
	}

	function plainThreadMatchesActive(thread) {
		const activeThread = activeThreadID();
		return isPlainObject(thread) && !!activeThread && thread.id === activeThread;
	}

	function patchPlainThreadRuntime(thread) {
		if (!plainThreadMatchesActive(thread)) {
			return false;
		}
		rememberThreadWorkingDirectory(thread.id, plainThreadWorkingDirectory(thread));
		rememberThreadSettings(thread.id, plainThreadSettings(thread));
		let patched = false;
		if (thread.hasExecutor !== true) {
			thread.hasExecutor = true;
			patched = true;
		}
		if (thread.executorConnected !== true) {
			thread.executorConnected = true;
			patched = true;
		}
		if (isPlainObject(thread.meta) && thread.meta.executorType !== "local-client") {
			thread.meta.executorType = "local-client";
			patched = true;
		}
		if (patched) {
			diagnostics.lastPatchedThreadID = thread.id;
		}
		return patched;
	}

	function plainThreadActorConfigLike(config) {
		return isPlainObject(config) &&
			(typeof config.threadId === "string" ||
				typeof config.baseURL === "string" ||
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
		rememberObservedThreadID(firstString(config.threadId, config.threadID, config.thread_id));
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
			patched = patchPlainThreadRuntime(value.thread) || patched;
		}
		patched = patchPlainThreadActorConfig(value, localBase) || patched;
		patched = patchPlainThreadRuntime(value) || patched;
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
			return { configs: false, sidebar: false };
		}
		return {
			configs: source.includes("threadActorConfig") || source.includes("wsToken") || source.includes("hasExecutor") || source.includes("executorConnected") || source.includes("thread_settings") || source.includes("baseURL") || source.includes("ampURL") || source.includes("threadId") || source.includes("threadID") || source.includes("thread_id") || source.includes("\"id\"") || source.includes("workingDirectory") || source.includes("workspaceRoot") || source.includes("workspace"),
			sidebar: (cachedLocalSidebarProjects.length > 0 || cachedLocalSidebarRecentThreads.length > 0) && source.includes("projects") && source.includes("recentThreads"),
		};
	}

	function patchDecodedLocalInference(value, options) {
		const patchOptions = options || { configs: true, sidebar: true };
		if (patchOptions.configs) {
			const localBase = localBaseURLString();
			const patched = patchDevalueThreadActorConfigs(value, localBase) ||
				patchPlainThreadActorConfigs(value, new WeakSet(), localBase);
			if (patched) {
				diagnostics.decodedConfigPatchCount += 1;
				diagnostics.lastPatchedThreadActorBaseURL = localBase;
			}
		}
		if (patchOptions.sidebar) {
			mergeDevalueSidebarProjects(value);
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
		case "listThreadListSidebar":
		case "listUserExecutorDaemons":
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
		return shouldBootstrapExecutor(url);
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
		for (const project of cachedLocalSidebarProjects) {
			if (!isPlainObject(project)) {
				continue;
			}
			const normalized = normalizeWorkingDirectory(project.workingDirectory);
			const projectName = firstString(project.name);
			if (normalized && (projectName === name || pathBaseName(normalized) === name)) {
				matches.add(normalized);
			}
		}
		return matches.size === 1 ? [...matches][0] : "";
	}

	function cachedProjectWorkingDirectory(projectID) {
		projectID = firstString(projectID);
		if (!projectID) {
			return "";
		}
		for (const project of cachedLocalSidebarProjects) {
			if (!isPlainObject(project) || ![project.id, project.projectID, project.projectId, project.project_id].some((value) => firstString(value) === projectID)) {
				continue;
			}
			const workingDirectory = normalizeWorkingDirectory(project.workingDirectory);
			if (workingDirectory) {
				return workingDirectory;
			}
		}
		return "";
	}

	function remoteCreateProjectThreadWorkingDirectory(body) {
		const decoded = decodeRemoteCommandBody(body);
		const fromMention = threadMentionWorkingDirectory(remoteContentText(isPlainObject(decoded) ? decoded.content : null));
		if (fromMention) {
			return fromMention;
		}
		const projectID = isPlainObject(decoded) ? firstString(decoded.projectID, decoded.projectId, decoded.project_id) : "";
		const fromCachedProject = cachedProjectWorkingDirectory(projectID);
		if (fromCachedProject) {
			return fromCachedProject;
		}
		const fromVisibleProject = visibleProjectWorkingDirectory();
		if (fromVisibleProject) {
			return fromVisibleProject;
		}
		if (projectID) {
			return "";
		}
		return localWorkingDirectory();
	}

	function rememberRemoteCreateProjectThread(sourceURL, body, response) {
		if (!createProjectThreadRemotePath(sourceURL.pathname)) {
			return;
		}
		const workingDirectory = remoteCreateProjectThreadWorkingDirectory(body);
		if (!workingDirectory || !response || !response.ok || typeof response.clone !== "function") {
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
				rememberLocalThreadID(threadID);
				rememberThreadWorkingDirectory(threadID, workingDirectory);
				rememberThreadSettings(threadID, {
					agentMode: firstString(isPlainObject(decodedRequest) ? decodedRequest.agentMode : ""),
					reasoningEffort: firstString(isPlainObject(decodedRequest) ? decodedRequest.reasoningEffort : ""),
			});
				refreshLocalSidebarProjects();
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

	function promptLocalThread() {
		const promptText = globalThis.prompt("Start local Amp thread");
		if (promptText === null) {
			return;
		}
		let workingDirectory = localWorkingDirectory();
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
		const payload = localThreadPayload(promptText, workingDirectory, modeOptions);
		const shellThreadID = await createRemoteThreadShell(payload);
		payload.threadId = shellThreadID;
		payload.threadID = shellThreadID;
		const response = await originalFetch(localBaseURLString() + "/api/thread-actors/" + encodeURIComponent(shellThreadID), {
			method: "POST",
			headers: localFetchHeaders("application/json"),
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
	}

	function removeStaleLocalThreadButton() {
		globalThis.document.getElementById("cliproxy-amp-local-thread-button")?.remove();
	}

	function removeInjectedLocalThreadControls() {
		removeStaleLocalThreadButton();
		removeLocalThreadPicker();
		let removed = 0;
		globalThis.document.querySelectorAll("[data-cliproxy-local-thread-command-item],[data-cliproxy-local-thread-menu-item]").forEach((element) => {
			element.remove();
			removed += 1;
		});
		for (const key of ["__cliproxyAmpLocalInferenceMenuObserver", "__cliproxyAmpLocalInferenceCommandPaletteObserver"]) {
			const observer = globalThis[key];
			if (observer && typeof observer.disconnect === "function") {
				observer.disconnect();
			}
			globalThis[key] = null;
		}
		diagnostics.removedLocalThreadControlCount += removed;
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
		const threadID = rememberObservedThreadID(threadIDFromGatewayURL(source));
		local.protocol = base.protocol === "https:" ? "wss:" : "ws:";
		diagnostics.webSocketRewriteCount += 1;
		diagnostics.lastWebSocketHost = local.host;
		diagnostics.lastWebSocketPath = local.pathname;
		diagnostics.lastWebSocketThreadKey = threadID || local.searchParams.get("rvt-key") || "";
		diagnostics.lastWebSocketBootstrapped = false;
		if (shouldBootstrapExecutor(source)) {
			const apiKey = localAPIKey();
			if (apiKey && !local.searchParams.has("cliproxy-api-key")) {
				local.searchParams.set("cliproxy-api-key", apiKey);
			}
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
			if (patchOptions.configs || patchOptions.sidebar) {
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
			if (args.length > 0) {
				args[0] = localWebSocketURL(args[0]);
			}
			return Reflect.construct(target, args, newTarget);
		},
	});

	globalThis.__cliproxyAmpLocalInference = {
		apiKeyStorageKey,
		workingDirectoryStorageKey,
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

	refreshLocalSidebarProjects();
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
