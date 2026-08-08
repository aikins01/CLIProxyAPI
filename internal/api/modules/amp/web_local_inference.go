package amp

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	log "github.com/sirupsen/logrus"
)

const (
	ampWebLocalInferenceCORSContextKey = "amp_web_local_inference_cors"
	ampWebLocalInferenceHeader         = "X-CLIProxyAPI-Web-Local-Inference"
	ampWebLocalInferenceAPIKeyQuery    = "cliproxy-api-key"
	ampWebLocalThreadSummaryQuery      = "cliproxy-summary-only"
	ampWebLocalSidebarThreadIDQuery    = "cliproxy-sidebar-thread-id"
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
		c.Header("Access-Control-Allow-Methods", "GET, HEAD, POST, PUT, DELETE, OPTIONS")
		c.Header("Access-Control-Allow-Private-Network", "true")
		c.Header("Access-Control-Allow-Headers", "Authorization, Content-Type, "+ampWebLocalInferenceHeader+", X-Rivet-Actor, X-Rivet-Conn-Params, X-Rivet-Encoding, X-Rivet-Skip-Ready-Wait, X-Rivet-Target, X-Rivet-Token")
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
			apiKeyFromRivetToken := false
			if apiKey == "" && strings.EqualFold(strings.TrimSpace(c.GetHeader("Upgrade")), "websocket") {
				apiKey = strings.TrimSpace(query.Get("rvt-token"))
				apiKeyFromRivetToken = apiKey != ""
			}
			if apiKey != "" && strings.TrimSpace(c.GetHeader("Authorization")) == "" {
				c.Request.Header.Set("Authorization", "Bearer "+apiKey)
			}
			queryChanged := false
			if apiKeyFromRivetToken {
				query.Del("rvt-token")
				queryChanged = true
			}
			if _, ok := query[ampWebLocalInferenceAPIKeyQuery]; ok {
				query.Del(ampWebLocalInferenceAPIKeyQuery)
				queryChanged = true
			}
			if queryChanged {
				c.Request.URL.RawQuery = query.Encode()
			}
		}
		c.Next()
	}
}

func webLocalInferenceBearerToken(r *http.Request) string {
	if r == nil {
		return ""
	}
	parts := strings.Fields(strings.TrimSpace(r.Header.Get("Authorization")))
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return ""
	}
	return strings.TrimSpace(parts[1])
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
	if origin == "" || !ampWebLocalInferenceRequestOriginAllowed(origin, settings) {
		return false
	}
	return ampWebLocalInferencePath(r.URL.Path)
}

func ampWebLocalInferenceRequestOriginAllowed(origin string, settings config.AmpWebLocalInference) bool {
	if ampWebLocalInferenceOriginAllowed(origin, settings.AllowedOrigins) {
		return true
	}
	baseOrigin := ampWebLocalInferenceBaseOrigin(settings.BaseURL)
	return baseOrigin != "" && baseOrigin == normalizeAmpWebLocalInferenceOrigin(origin)
}

func ampWebLocalInferenceBaseOrigin(rawBaseURL string) string {
	parsed, err := url.Parse(ampWebLocalInferenceSafeBaseURL(rawBaseURL))
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.User != nil {
		return ""
	}
	return strings.ToLower(parsed.Scheme + "://" + parsed.Host)
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
	case path == "/ampcode/local-project-details.json":
		return true
	case path == "/ampcode/local-activity.json":
		return true
	case path == "/ampcode/local-thread-search.json":
		return true
	case path == "/ampcode/local-thread-data.json":
		return true
	case path == "/api/threads/find":
		return true
	case neoDiffCaptureBrowserReadPath(path):
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
	c.Header("Cache-Control", "no-store, max-age=0")
	c.Header("Pragma", "no-cache")
	defaultBaseURL := strings.TrimSpace(settings.BaseURL)
	if defaultBaseURL == "" {
		defaultBaseURL = ampWebLocalInferenceDefaultBaseURL
	}
	script := ampWebLocalInferenceUserscript(defaultBaseURL, settings.AllowedOrigins)
	c.Header("Content-Length", strconv.Itoa(len(script)))
	if c.Request.Method == http.MethodHead {
		c.Status(http.StatusOK)
		return
	}
	c.Data(http.StatusOK, "application/javascript; charset=utf-8", []byte(script))
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
	projects := m.neoRuntime.reloadNeoWebLocalProjectCache()
	threads, archivedThreadIDs := m.neoRuntime.neoWebLocalSidebarThreadSummaries(c.Request.Context(), 50)
	visibleThreads := m.neoRuntime.neoWebLocalSidebarThreadSummariesForOwner(c.Request.Context(), c.QueryArray(ampWebLocalSidebarThreadIDQuery))
	seenThreadIDs := make(map[string]bool, len(threads))
	archivedThreadIDSet := make(map[string]bool, len(archivedThreadIDs))
	for _, threadID := range archivedThreadIDs {
		archivedThreadIDSet[threadID] = true
	}
	for _, rawThread := range threads {
		if threadID := strings.TrimSpace(firstNonEmptyString(mapValue(rawThread)["id"], mapValue(rawThread)["threadId"])); threadID != "" {
			seenThreadIDs[threadID] = true
		}
	}
	for _, rawThread := range visibleThreads {
		thread := mapValue(rawThread)
		threadID := strings.TrimSpace(firstNonEmptyString(thread["id"], thread["threadId"]))
		if threadID == "" || seenThreadIDs[threadID] {
			continue
		}
		if boolValue(thread["archived"]) {
			if !archivedThreadIDSet[threadID] {
				archivedThreadIDSet[threadID] = true
				archivedThreadIDs = append(archivedThreadIDs, threadID)
			}
			continue
		}
		seenThreadIDs[threadID] = true
		threads = append(threads, rawThread)
	}
	sort.Strings(archivedThreadIDs)
	threadTitles := make(map[string]string, len(visibleThreads))
	for _, rawThread := range visibleThreads {
		visibleThread := mapValue(rawThread)
		threadID := strings.TrimSpace(firstNonEmptyString(visibleThread["id"], visibleThread["threadId"]))
		title := strings.TrimSpace(stringValue(visibleThread["title"]))
		if threadID != "" && title != "" && !strings.EqualFold(title, "Untitled") {
			threadTitles[threadID] = title
		}
	}
	thread := m.neoRuntime.neoWebLocalThreadSummaryForOwner(c.Request.Context(), c.Query("cliproxy-thread-id"))
	if neoPuckThreadStatus(thread) {
		thread = nil
	}
	c.JSON(http.StatusOK, gin.H{
		"ok":                      true,
		"projects":                projects,
		"thread":                  thread,
		"threads":                 threads,
		"threadTitles":            threadTitles,
		"archivedThreadIDs":       archivedThreadIDs,
		"defaultWorkingDirectory": neoDefaultWebLocalWorkingDirectory(),
		"orbsEnabled":             neoOrbsEnabled(cfg),
	})
}

func (m *AmpModule) serveWebLocalProjectDetails(c *gin.Context) {
	if c.Request.Method != http.MethodGet {
		c.Header("Allow", http.MethodGet)
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
	details, err := m.neoRuntime.neoWebLocalProjectDetails(c.Request.Context(), c.Query("namespace"), c.Query("project"), c.Query("projectID"), c.Query("repository"), c.Query("workingDirectory"), c.Query("includeThreads") == "1")
	if err != nil {
		if errors.Is(err, errNeoWebLocalProjectSelector) {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		if errors.Is(err, errNeoWebLocalProjectAmbiguous) {
			c.JSON(http.StatusConflict, gin.H{"error": "local project is ambiguous; provide projectID, repository, or workingDirectory", "code": "ambiguous_project"})
			return
		}
		status, code, message := neoWebLocalProjectPublicError(err)
		log.WithFields(log.Fields{
			"namespace": c.Query("namespace"),
			"project":   c.Query("project"),
			"projectID": c.Query("projectID"),
			"code":      code,
		}).WithError(err).Warn("amp neo local project details failed")
		c.JSON(status, gin.H{"error": message, "code": code})
		return
	}
	if details == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "local project not found"})
		return
	}
	c.JSON(http.StatusOK, details)
}

func neoWebLocalProjectPublicError(err error) (int, string, string) {
	switch {
	case errors.Is(err, errNeoWebLocalProjectWorktree):
		return http.StatusUnprocessableEntity, "not_git_worktree", "Selected checkout is not a Git worktree."
	case errors.Is(err, errNeoWebLocalProjectCommits):
		return http.StatusInternalServerError, "commits_unavailable", "Unable to read commits for the selected checkout."
	case errors.Is(err, errNeoWebLocalProjectFiles):
		return http.StatusInternalServerError, "files_unavailable", "Unable to list files for the selected checkout."
	case errors.Is(err, errNeoWebLocalProjectStatus):
		return http.StatusInternalServerError, "status_unavailable", "Unable to read status for the selected checkout."
	default:
		return http.StatusInternalServerError, "repository_unavailable", "Selected checkout is unavailable."
	}
}

type ampWebJSONResponseWriter struct {
	writer         io.Writer
	pendingNewline bool
}

func (w *ampWebJSONResponseWriter) Write(data []byte) (int, error) {
	length := len(data)
	if length == 0 {
		return 0, nil
	}
	if w.pendingNewline {
		w.pendingNewline = false
		if _, err := io.WriteString(w.writer, "\n"); err != nil {
			return 0, err
		}
	}
	if data[len(data)-1] == '\n' {
		data = data[:len(data)-1]
		w.pendingNewline = true
	}
	if len(data) == 0 {
		return length, nil
	}
	written, err := w.writer.Write(data)
	if err != nil {
		w.pendingNewline = false
		return written, err
	}
	if written != len(data) {
		w.pendingNewline = false
		return written, io.ErrShortWrite
	}
	return length, nil
}

func (m *AmpModule) serveWebLocalActivity(c *gin.Context) {
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
	c.JSON(http.StatusOK, m.neoRuntime.neoWebLocalActivityResponseContext(c.Request.Context(), c.Request.URL.Query()))
}

func (m *AmpModule) serveWebLocalThreadSearch(c *gin.Context) {
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
	ownerUserID := m.neoRuntime.neoRequestOwnerUserID(c.Request.Context())
	if !neoRequestOwnerScopeResolved(c.Request.Context(), ownerUserID) {
		c.JSON(http.StatusOK, gin.H{"threads": []any{}, "hasMore": false})
		return
	}
	response, ok := m.neoRuntime.localThreadSearchResponseWithMaxLimitForOwnerContext(c.Request.Context(), c.Request.URL.Query(), 75, ownerUserID)
	if !ok {
		response = map[string]any{"threads": []any{}, "hasMore": false}
	}
	c.JSON(http.StatusOK, response)
}

func (m *AmpModule) serveWebLocalThreadData(c *gin.Context) {
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
	if c.Query(ampWebLocalThreadSummaryQuery) == "1" {
		thread := m.neoRuntime.neoWebLocalThreadSummaryForOwner(c.Request.Context(), c.Query("cliproxy-thread-id"))
		if thread == nil {
			c.JSON(http.StatusNotFound, gin.H{"message": "thread not found"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"type": "result", "thread": thread})
		return
	}
	threadData := m.neoRuntime.neoWebLocalThreadDataForOwner(c.Request.Context(), c.Query("cliproxy-thread-id"), neoWebLocalRequestBaseURL(c.Request), webLocalInferenceBearerToken(c.Request))
	if threadData == nil {
		c.JSON(http.StatusNotFound, gin.H{"message": "thread not found"})
		return
	}
	payload, err := json.Marshal(threadData)
	if err != nil {
		_ = c.Error(err)
		c.JSON(http.StatusInternalServerError, gin.H{"message": "failed to encode thread"})
		return
	}
	c.Data(http.StatusOK, "application/json; charset=utf-8", payload)
}

func ampWebLocalInferenceUserscript(defaultBaseURL string, allowedOrigins []string) string {
	baseURL := ampWebLocalInferenceSafeBaseURL(defaultBaseURL)
	userscriptURL := strings.NewReplacer("\r", "", "\n", "").Replace(baseURL + "/ampcode/local-inference.user.js")
	matchOrigins := append([]string(nil), allowedOrigins...)
	if allowedOrigins == nil {
		matchOrigins = append(matchOrigins, defaultAmpWebLocalInferenceOrigins...)
	}
	matchOrigins = append(matchOrigins, baseURL)
	matchLines := ampWebLocalInferenceUserscriptMatches(matchOrigins)
	return fmt.Sprintf(`// ==UserScript==
// @name CLIProxyAPI Amp Local Inference
// @namespace https://github.com/router-for-me/CLIProxyAPI
// @version 0.1.200
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
	const userscriptVersion = "0.1.200";
	const apiKeyStorageKey = "cliproxyapi.ampLocalInference.apiKey";
	const scopedAPIKeyStorageKeyPrefix = apiKeyStorageKey + ".user.";
	const authenticatedAmpUserIDStorageKey = apiKeyStorageKey + ".authenticatedAmpUserID";
	const promptedAPIKeyStorageKeyPrefix = "cliproxyapi.ampLocalInference.promptedAPIKey.user.";
	const promptedProjectsAPIKeyStorageKeyPrefix = "cliproxyapi.ampLocalInference.promptedProjectsAPIKey.user.";
	const workingDirectoryStorageKey = "cliproxyapi.ampLocalInference.workingDirectory";
	const selectedLocalProjectStorageKey = "cliproxyapi.ampLocalInference.selectedLocalProject";
	const localThreadIDsStorageKey = "cliproxyapi.ampLocalInference.localThreadIDs";
	const threadWorkingDirectoriesStorageKey = "cliproxyapi.ampLocalInference.threadWorkingDirectories";
	const threadSettingsStorageKey = "cliproxyapi.ampLocalInference.threadSettings";
	const threadMenuSelector = '[role="menu"],[data-radix-menu-content],[data-slot="dropdown-menu-content"]';
	const commandPaletteSelector = '[cmdk-root],[data-cmdk-root],[role="dialog"]';
	const sidebarTitlesStorageKeyPrefix = "cliproxyapi.ampLocalInference.sidebarTitles.v3";
	let sidebarTitlesStorageKey = sidebarTitlesStorageKeyPrefix + ".anonymous";
	const localSidebarHydrationAttribute = "data-cliproxy-local-sidebar-hydrating";
	const localSidebarVersionAttribute = "data-cliproxy-local-inference-version";
	const localThreadStorageLimit = 100;
	const localSidebarTitleStorageLimit = 500;
	const localSidebarHydrationTimeout = 2500;
	const localSidebarProjectRegroupStabilityDelay = 3000;
	const localActivityEndpointPath = "/ampcode/local-activity.json";
	const localThreadSearchEndpointPath = "/ampcode/local-thread-search.json";
	const localProjectsEndpointPath = "/ampcode/local-projects.json";
	const localProjectDetailsEndpointPath = "/ampcode/local-project-details.json";
	const localThreadDataEndpointPath = "/ampcode/local-thread-data.json";
	const localPinnedOverrideField = "cliProxyAPILocalPinnedOverride";
	const localInferencePatchFieldPattern = /"(id|v|messages|threadActorConfig|wsToken|thread_settings|baseURL|ampURL|threadId|threadID|thread_id|hasExecutor|executorConnected|workingDirectory|workspaceRoot|workspace)"\s*:|:\s*"(thread_settings)"/g;
	const localRunnerActionNames = new Set(["registerRunner", "runnerHeartbeat", "unregisterRunner", "listRunners"]);
	const loadedThreadBaseVersionLimit = 64;
	const localPinnedOverrideLimit = 64;
	const defaultBaseURL = %s;
	let defaultWorkingDirectory = "";
	const originalJSONParse = globalThis.JSON.parse.bind(globalThis.JSON);
	const originalFetch = globalThis.fetch.bind(globalThis);
	const originalResponseJSON = globalThis.Response?.prototype?.json;
	const NativeWebSocket = globalThis.WebSocket;
	const loadedThreadBaseVersions = new Map();
	const localThreadDiscoveryPromises = new Map();
	const localThreadSockets = new Map();
	const localInferencePatchScanMaxChars = 512 * 1024;
	const activityFilterResponseContexts = new WeakMap();
	const threadSearchResponseContexts = new WeakMap();
	const localPinnedOverrides = new Map();
	let authenticatedAmpUserID = "";
	let authenticatedAmpUser = {};
	let pendingLocalAPIKey = "";
	let pendingLocalAPIKeyRememberConsent = false;
	const pendingLocalAPIKeyPromptSuppressions = new Set();
	let localThreadViewRedirectTarget = "";
	const diagnostics = {
		authenticatedAmpUserIDCaptureCount: 0,
		decodedConfigPatchCount: 0,
		decodedGraphPassCount: 0,
		decodedGraphVisitCount: 0,
		responseJSONPatchCount: 0,
		fetchRewriteCount: 0,
		webSocketRewriteCount: 0,
		webSocketBootstrapCount: 0,
		webSocketOpenCount: 0,
		webSocketCloseCount: 0,
		webSocketErrorCount: 0,
		activeWebSocketCount: 0,
		trackedLocalThreadSocketCount: 0,
		staleLocalThreadSocketCloseCount: 0,
		hiddenPageSocketPauseCount: 0,
		hiddenPageSocketConstructionSuppressionCount: 0,
		hiddenPageDeferredSocketCount: 0,
		hiddenPageSocketResumeCount: 0,
		loadedThreadBaseCaptureCount: 0,
		loadedThreadBaseVersionEntryCount: 0,
		clientResumeObservedCount: 0,
		clientResumeRewriteCount: 0,
		menuIntegrationCount: 0,
		commandPaletteIntegrationCount: 0,
		localProjectFetchCount: 0,
		localProjectFetchFailureCount: 0,
		localProjectChangesWorkflowCacheUpdateCount: 0,
		localProjectPageIntegrationCount: 0,
		localThreadDiscoveryCount: 0,
		localThreadViewRedirectCount: 0,
		localActivityFetchCount: 0,
		localActivityMergeCount: 0,
		localActivityDOMIntegrationCount: 0,
		localActivityDOMFilterIntegrationCount: 0,
		localSidebarProjectMergeCount: 0,
		localSidebarThreadMergeCount: 0,
		localSidebarTitlePatchCount: 0,
		localThreadArchiveBadgePatchCount: 0,
		localThreadSearchTitlePatchCount: 0,
		localThreadSearchProjectPatchCount: 0,
		localThreadSearchFetchCount: 0,
		localThreadSearchMergeCount: 0,
		localUsageTitlePatchCount: 0,
		localProjectPickerIntegrationCount: 0,
		localProjectActivatorIntegrationCount: 0,
		projectMutationCandidateCount: 0,
		projectMutationIgnoredCount: 0,
		projectMutationCoalescedCount: 0,
		projectMutationFlushCount: 0,
		projectMutationPendingRootCount: 0,
		localThreadStatusPatchCount: 0,
		localThreadPickerOpenCount: 0,
		removedLocalThreadControlCount: 0,
		remoteShellCreateCount: 0,
		lastPatchedThreadActorBaseURL: "",
		lastPatchedThreadID: "",
		lastLocalThreadNavigationThreadID: "",
		lastLocalThreadViewRedirect: "",
		lastLocalThreadAgentMode: "",
		lastLocalThreadReasoningEffort: "",
		lastVisibleThreadModeBadge: "",
		lastVisibleThreadModeAgentMode: "",
		lastVisibleThreadModeReasoningEffort: "",
		lastLocalThreadChoice: "",
		lastLocalProjectChoice: "",
		lastLocalProjectWorkingDirectory: "",
		lastLocalProjectFetchFailure: "",
		lastWebSocketHost: "",
		lastWebSocketPath: "",
		lastWebSocketThreadKey: "",
		lastWebSocketBootstrapped: false,
		lastWebSocketProtocols: "",
		lastWebSocketState: "",
		lastWebSocketReadyState: -1,
		lastWebSocketCloseCode: 0,
		lastWebSocketCloseReason: "",
		lastLoadedThreadBaseThreadID: "",
		lastLoadedThreadBaseVersion: 0,
		lastClientResumeObservedVersion: 0,
		lastClientResumeThreadID: "",
		lastClientResumeBaseVersion: 0,
		lastInheritedWorkingDirectory: "",
		lastInheritedWorkingDirectoryThreadID: "",
		lastObservedThreadID: "",
	};
	let observedThreadID = "";
	let pendingLocalBootstrapThreadID = "";
	let pendingLocalSidebarThreadID = "";
	let localSidebarTitleCache = storedLocalSidebarTitles();
	let localProjectsCache = { at: 0, projects: [], threadID: "", thread: null, threads: [], threadTitles: Object.assign({}, localSidebarTitleCache), sidebarTitleKey: "", promise: null };
	let localOrbsEnabled = false;
	let localProjectsCacheGeneration = 0;
	let localActivityCache = { at: 0, key: "", value: null, promise: null };
	let localActivityDOMRefreshPending = false;
	let localActivityObservedSection = null;
	let localProjectListDecorationPending = false;
	let localProjectPageGeneration = 0;
	let localSidebarDOMRefreshPending = false;
	let localSidebarDOMRefreshRequested = false;
	const localSidebarProjectRegroupPendingThreadIDs = new Set();
	const localSidebarProjectRegroupStableSince = new Map();
	let localSidebarProjectRegroupStableTimer;
	let localSidebarHydrationRevealScheduled = false;
	let localSidebarPassiveHydrationScope = "";
	let localSidebarHydrationProjectsReady = false;
	let localSidebarHydrationRefreshRequested = false;
	let localSidebarHydrationExpectedThreadIDs = new Set();
	let localSidebarHydrationTimer = null;
	let localSidebarHydrationObserver = null;
	let archivedLocalSidebarThreadIDs = new Set();
	let localProjectsArchiveStateLoaded = false;
	const cloudProjectPaths = new Set();
	const localProjectCheckoutsByPath = new Map();
	const localProjectWebIDs = new Map();
	let localProjectIntegrationGeneration = 0;
	installLocalSidebarHydrationGate();

	function resetLocalProjectsCache(threadID = "") {
		localProjectsCache = { at: 0, projects: [], threadID, thread: null, threads: [], threadTitles: Object.assign({}, localSidebarTitleCache), sidebarTitleKey: "", promise: null };
	}

	function installLocalSidebarHydrationGate() {
		const root = globalThis.document?.documentElement;
		if (!root || typeof root.setAttribute !== "function") {
			return;
		}
		root.setAttribute(localSidebarHydrationAttribute, "1");
		root.setAttribute(localSidebarVersionAttribute, userscriptVersion);
		const styleParent = globalThis.document.head || root;
		if (typeof globalThis.document.createElement === "function" && typeof styleParent.appendChild === "function") {
			const style = globalThis.document.createElement("style");
			style.textContent = "html[" + localSidebarHydrationAttribute + "=\"1\"] ul[data-slot=\"sidebar-menu\"] { visibility: hidden !important; }";
			styleParent.appendChild(style);
		}
		if (typeof globalThis.MutationObserver === "function") {
			localSidebarHydrationObserver = new MutationObserver(requestLocalSidebarHydrationRefresh);
			localSidebarHydrationObserver.observe(root, { childList: true, subtree: true, attributes: true, attributeFilter: ["data-sidebar-group-id"] });
		}
		localSidebarHydrationTimer = globalThis.setTimeout(revealLocalSidebarHydration, localSidebarHydrationTimeout);
		if (typeof localSidebarHydrationTimer?.unref === "function") {
			localSidebarHydrationTimer.unref();
		}
	}

	function revealLocalSidebarHydration() {
		const root = globalThis.document?.documentElement;
		if (root && typeof root.removeAttribute === "function") {
			root.removeAttribute(localSidebarHydrationAttribute);
		}
		if (localSidebarHydrationTimer !== null) {
			globalThis.clearTimeout(localSidebarHydrationTimer);
			localSidebarHydrationTimer = null;
		}
		localSidebarHydrationObserver?.disconnect();
		localSidebarHydrationObserver = null;
		localSidebarHydrationRevealScheduled = false;
	}

	function startLocalSidebarHydration() {
		if (!globalThis.document?.documentElement?.hasAttribute?.(localSidebarHydrationAttribute)) {
			return;
		}
		captureAuthenticatedAmpUserFromBootstrap();
		if (!storedLocalAPIKey()) {
			revealLocalSidebarHydration();
			return;
		}
		startPassiveLocalSidebarHydration();
	}

	function startPassiveLocalSidebarHydration() {
		const scope = localAPIKeyScopeSuffix();
		if (!scope || scope === localSidebarPassiveHydrationScope || !storedLocalAPIKey()) {
			return;
		}
		localSidebarPassiveHydrationScope = scope;
		renderLocalSidebarMetadata();
		requestLocalSidebarHydrationRefresh();
		const failureCount = diagnostics.localProjectFetchFailureCount;
		fetchLocalProjects(false).then(() => {
			if (globalThis.__cliproxyAmpLocalInference?.diagnostics !== diagnostics || localAPIKeyScopeSuffix() !== scope) {
				return;
			}
			if (diagnostics.localProjectFetchFailureCount !== failureCount) {
				localSidebarPassiveHydrationScope = "";
				revealLocalSidebarHydration();
				return;
			}
			renderLocalSidebarMetadata();
			requestLocalSidebarProjectRegroup();
			integrateLocalProjectActivators(globalThis.document);
			integrateLocalProjectPickers(globalThis.document);
			scheduleLocalProjectListDecoration();
			if ((localProjectsCache.threads || []).length === 0 && archivedLocalSidebarThreadIDs.size === 0) {
				revealLocalSidebarHydration();
				return;
			}
			localSidebarHydrationProjectsReady = true;
			requestLocalSidebarHydrationRefresh();
		}, () => {
			if (globalThis.__cliproxyAmpLocalInference?.diagnostics !== diagnostics || localAPIKeyScopeSuffix() !== scope) {
				return;
			}
			localSidebarPassiveHydrationScope = "";
			revealLocalSidebarHydration();
		});
	}

	function requestLocalSidebarHydrationRefresh() {
		const rows = globalThis.document?.querySelectorAll?.("[data-sidebar-thread-id]") || [];
		if (localSidebarHydrationRefreshRequested || rows.length === 0) {
			return;
		}
		renderLocalSidebarMetadata();
		localSidebarHydrationExpectedThreadIDs = new Set();
		for (const row of rows) {
			const threadID = firstString(row?.dataset?.sidebarThreadId, row?.getAttribute?.("data-sidebar-thread-id"));
			if (validThreadID(threadID) && !archivedLocalSidebarThreadIDs.has(threadID)) {
				localSidebarHydrationExpectedThreadIDs.add(threadID);
			}
		}
		if (!localSidebarHydrationProjectsReady && localSidebarPassiveHydrationScope && localSidebarUntitledThreadIDs().length === 0) {
			scheduleLocalSidebarHydrationReveal();
			return;
		}
		if (!localSidebarHydrationProjectsReady) {
			return;
		}
		localSidebarHydrationRefreshRequested = true;
		const refresh = () => {
			try {
				globalThis.dispatchEvent(new Event("pageshow"));
			} catch {
				revealLocalSidebarHydration();
			}
		};
		if (typeof globalThis.requestAnimationFrame === "function") {
			globalThis.requestAnimationFrame(refresh);
			return;
		}
		globalThis.setTimeout(refresh, 0);
	}

	function scheduleLocalSidebarHydrationReveal() {
		if (localSidebarHydrationRevealScheduled) {
			return;
		}
		localSidebarHydrationRevealScheduled = true;
		const check = () => {
			if (!globalThis.document?.documentElement?.hasAttribute?.(localSidebarHydrationAttribute)) {
				return;
			}
			renderLocalSidebarMetadata();
			const rows = globalThis.document?.querySelectorAll?.("[data-sidebar-thread-id]") || [];
			const visibleThreadIDs = new Set(Array.from(rows, (row) => firstString(row?.dataset?.sidebarThreadId, row?.getAttribute?.("data-sidebar-thread-id"))).filter(validThreadID));
			const complete = rows.length > 0 &&
				Array.from(localSidebarHydrationExpectedThreadIDs).every((threadID) => visibleThreadIDs.has(threadID)) &&
				localSidebarUntitledThreadIDs().length === 0;
			if (complete) {
				if (typeof globalThis.requestAnimationFrame === "function") {
					globalThis.requestAnimationFrame(revealLocalSidebarHydration);
					return;
				}
				revealLocalSidebarHydration();
				return;
			}
			if (typeof globalThis.requestAnimationFrame === "function") {
				globalThis.requestAnimationFrame(check);
				return;
			}
			globalThis.setTimeout(check, 16);
		};
		check();
	}

	function storedLocalSidebarTitles(storageKey = sidebarTitlesStorageKey) {
		try {
			const parsed = originalJSONParse(globalThis.localStorage.getItem(storageKey) || "{}");
			if (!isPlainObject(parsed)) {
				return {};
			}
			const titles = {};
			for (const [threadID, title] of Object.entries(parsed)) {
				if (!validThreadID(threadID) || typeof title !== "string" || !title.trim() || title.trim().toLowerCase() === "untitled") {
					continue;
				}
				titles[threadID] = title.trim();
				if (Object.keys(titles).length >= localSidebarTitleStorageLimit) {
					break;
				}
			}
			return titles;
		} catch {
			return {};
		}
	}

	function activateLocalSidebarTitleOwner(userID) {
		userID = firstString(userID);
		const nextStorageKey = userID ? sidebarTitlesStorageKeyPrefix + "." + localAPIKeyScopeSuffixForUser(userID) : sidebarTitlesStorageKeyPrefix + ".anonymous";
		if (nextStorageKey === sidebarTitlesStorageKey) {
			return;
		}
		sidebarTitlesStorageKey = nextStorageKey;
		localSidebarTitleCache = storedLocalSidebarTitles();
		localSidebarPassiveHydrationScope = "";
		localProjectsCacheGeneration += 1;
		resetLocalProjectsCache();
		archivedLocalSidebarThreadIDs = new Set();
		localProjectsArchiveStateLoaded = false;
		if (isPlainObject(globalThis.__cliproxyAmpLocalInference)) {
			globalThis.__cliproxyAmpLocalInference.sidebarTitlesStorageKey = sidebarTitlesStorageKey;
		}
	}

	function rememberLocalSidebarTitles(threadTitles, threads) {
		const titles = {};
		let count = 0;
		const append = (threadID, title) => {
			threadID = firstString(threadID);
			title = firstString(title);
			if (!validThreadID(threadID) || !title || title.toLowerCase() === "untitled" || Object.prototype.hasOwnProperty.call(titles, threadID) || count >= localSidebarTitleStorageLimit) {
				return;
			}
			titles[threadID] = title;
			count += 1;
		};
		for (const [threadID, title] of Object.entries(isPlainObject(threadTitles) ? threadTitles : {})) {
			append(threadID, title);
		}
		for (const thread of Array.isArray(threads) ? threads : []) {
			append(firstString(thread?.id, thread?.threadId, thread?.threadID), thread?.title);
		}
		for (const [threadID, title] of Object.entries(localSidebarTitleCache)) {
			append(threadID, title);
		}
		const changed = Object.keys(titles).length !== Object.keys(localSidebarTitleCache).length ||
			Object.entries(titles).some(([threadID, title]) => localSidebarTitleCache[threadID] !== title);
		localSidebarTitleCache = titles;
		if (changed) {
			try {
				globalThis.localStorage.setItem(sidebarTitlesStorageKey, JSON.stringify(titles));
			} catch {
			}
		}
		return Object.assign({}, titles);
	}

	function forgetLocalSidebarTitle(threadID) {
		if (!Object.prototype.hasOwnProperty.call(localSidebarTitleCache, threadID)) {
			return;
		}
		delete localSidebarTitleCache[threadID];
		try {
			globalThis.localStorage.setItem(sidebarTitlesStorageKey, JSON.stringify(localSidebarTitleCache));
		} catch {
		}
	}

	function localBaseURL() {
		return new URL(defaultBaseURL);
	}

	function localBaseURLString() {
		return localBaseURL().href.replace(/\/+$/, "");
	}

	function localAPIKeyScopeSuffixForUser(userID) {
		userID = firstString(userID);
		if (!userID) {
			return "";
		}
		return encodeURIComponent(userID) + "." + encodeURIComponent(localBaseURLString());
	}

	function localAPIKeyScopeSuffix() {
		return localAPIKeyScopeSuffixForUser(authenticatedAmpUserID);
	}

	function sessionLocalAPIKeyStorageKey() {
		const scope = localAPIKeyScopeSuffix();
		return scope ? scopedAPIKeyStorageKeyPrefix + scope : "";
	}

	function persistentLocalAPIKeyStorageKey() {
		const scope = localAPIKeyScopeSuffix();
		return scope ? scopedAPIKeyStorageKeyPrefix + scope : "";
	}

	function localAPIKeyPromptStorageKey(kind) {
		const scope = localAPIKeyScopeSuffix();
		if (!scope) {
			return "";
		}
		return (kind === "projects" ? promptedProjectsAPIKeyStorageKeyPrefix : promptedAPIKeyStorageKeyPrefix) + scope;
	}

	function localAPIKeyPromptSuppressed(kind) {
		const storageKey = localAPIKeyPromptStorageKey(kind);
		return storageKey ? globalThis.sessionStorage.getItem(storageKey) === "1" : pendingLocalAPIKeyPromptSuppressions.has(kind);
	}

	function suppressLocalAPIKeyPrompt(kind) {
		const storageKey = localAPIKeyPromptStorageKey(kind);
		if (storageKey) {
			globalThis.sessionStorage.setItem(storageKey, "1");
			return;
		}
		pendingLocalAPIKeyPromptSuppressions.add(kind);
	}

	function clearLocalAPIKeyPromptSuppression(kind) {
		pendingLocalAPIKeyPromptSuppressions.delete(kind);
		const storageKey = localAPIKeyPromptStorageKey(kind);
		if (storageKey) {
			globalThis.sessionStorage.removeItem(storageKey);
		}
	}

	function promotePendingLocalAPIKey() {
		const sessionStorageKey = sessionLocalAPIKeyStorageKey();
		const persistentStorageKey = persistentLocalAPIKeyStorageKey();
		if (!sessionStorageKey || !persistentStorageKey) {
			return;
		}
		const legacySessionAPIKey = globalThis.sessionStorage.getItem(apiKeyStorageKey) || "";
		const legacyPersistentAPIKey = globalThis.localStorage.getItem(apiKeyStorageKey) || "";
		if (pendingLocalAPIKey) {
			globalThis.sessionStorage.setItem(sessionStorageKey, pendingLocalAPIKey);
			if (pendingLocalAPIKeyRememberConsent) {
				globalThis.localStorage.setItem(persistentStorageKey, pendingLocalAPIKey);
			} else {
				globalThis.localStorage.removeItem(persistentStorageKey);
			}
			pendingLocalAPIKey = "";
			pendingLocalAPIKeyRememberConsent = false;
		} else if ((legacySessionAPIKey || legacyPersistentAPIKey) && !globalThis.sessionStorage.getItem(sessionStorageKey) && !globalThis.localStorage.getItem(persistentStorageKey)) {
			globalThis.sessionStorage.setItem(sessionStorageKey, legacySessionAPIKey || legacyPersistentAPIKey);
			if (legacyPersistentAPIKey) {
				globalThis.localStorage.setItem(persistentStorageKey, legacyPersistentAPIKey);
			}
		}
		globalThis.sessionStorage.removeItem(apiKeyStorageKey);
		globalThis.localStorage.removeItem(apiKeyStorageKey);
		for (const kind of pendingLocalAPIKeyPromptSuppressions) {
			const storageKey = localAPIKeyPromptStorageKey(kind);
			if (storageKey) {
				globalThis.sessionStorage.setItem(storageKey, "1");
			}
		}
		pendingLocalAPIKeyPromptSuppressions.clear();
	}

	function localAPIKeyAwaitingIdentity() {
		if (authenticatedAmpUserID) {
			return false;
		}
		const hintedUserID = firstString(globalThis.sessionStorage.getItem(authenticatedAmpUserIDStorageKey));
		const hintedScope = localAPIKeyScopeSuffixForUser(hintedUserID);
		const hintedStorageKey = hintedScope ? scopedAPIKeyStorageKeyPrefix + hintedScope : "";
		if (hintedStorageKey && (globalThis.sessionStorage.getItem(hintedStorageKey) || globalThis.localStorage.getItem(hintedStorageKey))) {
			return true;
		}
		return !!(globalThis.sessionStorage.getItem(apiKeyStorageKey) || globalThis.localStorage.getItem(apiKeyStorageKey));
	}

	function storedLocalAPIKey() {
		const sessionStorageKey = sessionLocalAPIKeyStorageKey();
		const apiKey = sessionStorageKey ? globalThis.sessionStorage.getItem(sessionStorageKey) : pendingLocalAPIKey;
		if (apiKey) {
			return apiKey;
		}
		const persistentStorageKey = persistentLocalAPIKeyStorageKey();
		return persistentStorageKey ? globalThis.localStorage.getItem(persistentStorageKey) || "" : "";
	}

	function rememberLocalAPIKey(apiKey) {
		const remember = typeof globalThis.confirm === "function" && globalThis.confirm("Remember this API key for this Amp account in this browser? It will be stored in localStorage until you clear it.");
		const sessionStorageKey = sessionLocalAPIKeyStorageKey();
		const persistentStorageKey = persistentLocalAPIKeyStorageKey();
		if (!sessionStorageKey || !persistentStorageKey) {
			pendingLocalAPIKey = apiKey;
			pendingLocalAPIKeyRememberConsent = remember;
			return;
		}
		globalThis.sessionStorage.setItem(sessionStorageKey, apiKey);
		globalThis.localStorage.removeItem(apiKeyStorageKey);
		if (remember) {
			globalThis.localStorage.setItem(persistentStorageKey, apiKey);
		} else {
			globalThis.localStorage.removeItem(persistentStorageKey);
		}
		localSidebarPassiveHydrationScope = "";
		localProjectsCacheGeneration += 1;
		resetLocalProjectsCache();
		globalThis.setTimeout(startPassiveLocalSidebarHydration, 0);
	}

	function forgetLocalAPIKey() {
		pendingLocalAPIKey = "";
		pendingLocalAPIKeyRememberConsent = false;
		globalThis.sessionStorage.removeItem(apiKeyStorageKey);
		globalThis.localStorage.removeItem(apiKeyStorageKey);
		const sessionStorageKey = sessionLocalAPIKeyStorageKey();
		if (sessionStorageKey) {
			globalThis.sessionStorage.removeItem(sessionStorageKey);
		}
		const persistentStorageKey = persistentLocalAPIKeyStorageKey();
		if (persistentStorageKey) {
			globalThis.localStorage.removeItem(persistentStorageKey);
		}
		localSidebarPassiveHydrationScope = "";
		localProjectsCacheGeneration += 1;
		resetLocalProjectsCache();
	}

	function localAPIKey(rememberCancel = true) {
		const apiKey = storedLocalAPIKey();
		if (apiKey) {
			return apiKey;
		}
		if (localAPIKeyAwaitingIdentity()) {
			return "";
		}
		if (rememberCancel && localAPIKeyPromptSuppressed("api")) {
			return "";
		}
		if (rememberCancel) {
			suppressLocalAPIKeyPrompt("api");
		}
		const promptedAPIKey = (globalThis.prompt("CLIProxyAPI API key") || "").trim();
		if (promptedAPIKey) {
			rememberLocalAPIKey(promptedAPIKey);
		} else {
			forgetLocalAPIKey();
		}
		return promptedAPIKey;
	}

	function localProjectLookupAPIKey() {
		const apiKey = storedLocalAPIKey();
		if (apiKey) {
			return apiKey;
		}
		if (localAPIKeyAwaitingIdentity()) {
			return "";
		}
		if (localAPIKeyPromptSuppressed("projects")) {
			return "";
		}
		suppressLocalAPIKeyPrompt("projects");
		const promptedAPIKey = (globalThis.prompt("CLIProxyAPI API key") || "").trim();
		if (promptedAPIKey) {
			rememberLocalAPIKey(promptedAPIKey);
			clearLocalAPIKeyPromptSuppression("projects");
		} else {
			forgetLocalAPIKey();
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

	function newLocalThreadWorkingDirectory() {
		const selectedProject = selectedLocalProject();
		const selectedDirectory = normalizeWorkingDirectory(selectedProject?.workingDirectory);
		if (localProjectIsNoProject(selectedProject)) {
			return selectedDirectory;
		}
		return visibleProjectWorkingDirectory(visibleCreateThreadProjectName()) ||
			selectedDirectory ||
			storedLocalWorkingDirectory() ||
			defaultLocalWorkingDirectory();
	}

	function currentLocalProjectWorkingDirectory() {
		return selectedLocalProjectWorkingDirectory() ||
			activeThreadWorkingDirectory() ||
			visibleProjectWorkingDirectory() ||
			storedLocalWorkingDirectory() ||
			defaultLocalWorkingDirectory();
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
		return normalizeWorkingDirectory(selectedLocalProject()?.workingDirectory);
	}

	function selectedLocalProject() {
		try {
			const parsed = originalJSONParse(globalThis.sessionStorage.getItem(selectedLocalProjectStorageKey) || "{}");
			const selectedAt = Number(parsed.selectedAt || 0);
			if (!selectedAt || Date.now() - selectedAt > 10 * 60 * 1000) {
				return null;
			}
			return parsed;
		} catch {
			return null;
		}
	}

	function localProjectIsNoProject(project) {
		const name = normalizeProjectPickerName(project?.name);
		const workingDirectory = normalizeWorkingDirectory(project?.workingDirectory);
		return (name === "no project" || name === "~") &&
			!!workingDirectory &&
			workingDirectory === defaultLocalWorkingDirectory();
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

	function boundedThreadValues(values, currentThreadID = "") {
		const ids = localThreadIDs().filter((threadID) => threadID !== currentThreadID);
		if (currentThreadID && currentThreadID.startsWith("T-")) {
			ids.unshift(currentThreadID);
		}
		const bounded = {};
		for (const threadID of ids.slice(0, localThreadStorageLimit)) {
			if (Object.prototype.hasOwnProperty.call(values, threadID)) {
				bounded[threadID] = values[threadID];
			}
		}
		return bounded;
	}

	function defaultReasoningEffort(agentMode) {
		const mode = normalizeAgentMode(agentMode);
		switch (mode) {
		case "low":
			return "medium";
		case "medium":
			return "medium";
		case "high":
			return "xhigh";
		case "ultra":
			return "high";
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
		case "eni":
		case "deep-red":
			return "max";
		default:
			return "";
		}
	}

	function normalizeAgentMode(value) {
		const mode = typeof value === "string" ? value.trim().toLowerCase() : "";
		return /^[a-z0-9][a-z0-9._-]{0,127}$/.test(mode) ? mode : "";
	}

	function normalizeReasoningEffort(agentMode, value) {
		const mode = normalizeAgentMode(agentMode);
		const effort = typeof value === "string" ? value.trim().toLowerCase() : "";
		const allowed = {
			low: ["medium"],
			medium: ["medium"],
			high: ["xhigh"],
			ultra: ["high"],
			smart: ["high", "xhigh", "max"],
			rush: ["none"],
			deep: ["low", "medium", "xhigh"],
			review: ["none", "low", "medium", "high"],
			"agg-man": ["none"],
			nostromo: ["low"],
			eni: ["max"],
			"deep-red": ["max"],
		};
		if (allowed[mode]?.includes(effort) || (!Object.prototype.hasOwnProperty.call(allowed, mode) && ["none", "minimal", "low", "medium", "high", "xhigh", "max"].includes(effort))) {
			return effort;
		}
		return defaultReasoningEffort(mode);
	}

	function normalizeExplicitReasoningEffort(agentMode, value) {
		const mode = normalizeAgentMode(agentMode);
		const effort = typeof value === "string" ? value.trim().toLowerCase() : "";
		const allowed = {
			low: ["medium"],
			medium: ["medium"],
			high: ["xhigh"],
			ultra: ["high"],
			smart: ["high", "xhigh", "max"],
			rush: ["none"],
			deep: ["low", "medium", "xhigh"],
			review: ["none", "low", "medium", "high"],
			"agg-man": ["none"],
			nostromo: ["low"],
			eni: ["max"],
			"deep-red": ["max"],
		};
		if (allowed[mode]?.includes(effort)) {
			return effort;
		}
		return !Object.prototype.hasOwnProperty.call(allowed, mode) && ["none", "minimal", "low", "medium", "high", "xhigh", "max"].includes(effort) ? effort : "";
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
		globalThis.localStorage.setItem(threadSettingsStorageKey, JSON.stringify(boundedThreadValues(byThread, threadID)));
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
		const agentMode = settings.agentMode || visible.agentMode || "medium";
		return {
			agentMode,
			reasoningEffort: normalizeReasoningEffort(agentMode, settings.reasoningEffort || visible.reasoningEffort),
		};
	}

	function localThreadModeChoices() {
		const inherited = localThreadModeOptions();
		return [
			{ id: "inherit", label: "Use current", detail: localThreadModeLabel(inherited), options: {} },
			{ id: "low", label: "Low", detail: "Medium", options: { agentMode: "low", reasoningEffort: "medium" } },
			{ id: "medium", label: "Medium", detail: "Medium", options: { agentMode: "medium", reasoningEffort: "medium" } },
			{ id: "high", label: "High", detail: "XHigh", options: { agentMode: "high", reasoningEffort: "xhigh" } },
			{ id: "ultra", label: "Ultra", detail: "High", options: { agentMode: "ultra", reasoningEffort: "high" } },
			{ id: "deep-1", label: "Deep 1", detail: "Low", options: { agentMode: "deep", reasoningEffort: "low" } },
			{ id: "deep-2", label: "Deep 2", detail: "Medium", options: { agentMode: "deep", reasoningEffort: "medium" } },
			{ id: "deep-3", label: "Deep 3", detail: "XHigh", options: { agentMode: "deep", reasoningEffort: "xhigh" } },
			{ id: "nostromo", label: "Nostromo", detail: "Amp", options: { agentMode: "nostromo", reasoningEffort: "low" } },
		];
	}

	function localThreadModeLabel(options) {
		const mode = normalizeAgentMode(options?.agentMode);
		const effort = normalizeReasoningEffort(mode, options?.reasoningEffort);
		const labels = {
			"low:medium": "Low",
			"medium:medium": "Medium",
			"high:xhigh": "High",
			"ultra:high": "Ultra",
			"smart:high": "Smart 1",
			"smart:xhigh": "Smart 2",
			"smart:max": "Smart 3",
			"deep:low": "Deep 1",
			"deep:medium": "Deep 2",
			"deep:xhigh": "Deep 3",
			"rush:none": "Rush",
			"review:none": "Review",
			"review:medium": "Review",
			"agg-man:none": "Agg-man",
			"nostromo:low": "Nostromo",
			"eni:max": "Deep Red",
			"deep-red:max": "Deep Red",
		};
		return labels[mode + ":" + effort] || (mode ? mode[0].toUpperCase() + mode.slice(1) : "Medium");
	}

	function visibleThreadModeOptions() {
		if (!globalThis.document?.querySelectorAll) {
			return {};
		}
		const selector = "button,[role='button'],[aria-haspopup],[aria-label]";
		for (const element of globalThis.document.querySelectorAll(selector)) {
			if (!modeBadgeElement(element)) {
				continue;
			}
			const text = modeBadgeElementText(element);
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

	function modeBadgeElement(element) {
		if (!(element instanceof Element) || !elementVisible(element)) {
			return false;
		}
		if (element.matches?.('[cmdk-item],[data-cmdk-item],[data-slot="command-item"],[role="option"]') || element.closest?.('[cmdk-list],[data-cmdk-list],[data-slot="command-list"],[role="listbox"]')) {
			return false;
		}
		if (element.closest?.("#cliproxy-amp-local-thread-picker,[data-cliproxy-local-thread-command-item],[data-cliproxy-local-thread-menu-item],[data-cliproxy-local-project-item]")) {
			return false;
		}
		const text = modeBadgeElementText(element);
		if (!modeOptionsFromBadgeText(text).agentMode) {
			return false;
		}
		const tag = String(element.tagName || "").toLowerCase();
		const role = String(element.getAttribute?.("role") || "").toLowerCase();
		const aria = String(element.getAttribute?.("aria-label") || "").toLowerCase();
		const hasPopup = element.hasAttribute?.("aria-haspopup") || element.hasAttribute?.("aria-expanded");
		if (!genericDialModeBadgeText(text)) {
			return tag === "button" || role === "button" || hasPopup || /\b(agent|mode|model|reasoning|dial)\b/.test(aria);
		}
		return hasPopup || /\b(agent|mode|model|reasoning|dial)\b/.test(aria);
	}

	function modeBadgeElementText(element) {
		const visible = String(element.textContent || "").trim();
		if (modeOptionsFromBadgeText(visible).agentMode) {
			return visible;
		}
		const aria = String(element.getAttribute?.("aria-label") || "").trim();
		if (modeOptionsFromBadgeText(aria).agentMode) {
			return aria;
		}
		return aria || visible;
	}

	function genericDialModeBadgeText(text) {
		const normalized = String(text || "").trim().toLowerCase().replace(/\s+/g, "");
		return /^(low|medium|high|ultra)$/.test(normalized);
	}

	function modeOptionsFromBadgeText(text) {
		let normalized = typeof text === "string" ? text.trim().toLowerCase() : "";
		normalized = normalized
			.replace(/\u00b9/g, "1")
			.replace(/\u00b2/g, "2")
			.replace(/\u00b3/g, "3")
			.replace(/\s+/g, "")
			.replace(/^deep-([123])$/, "deep$1");
		const match = normalized.match(/^(low|medium|high|ultra|smart|large|rush|deep|deep-?red|eni|review|agg-man|nostromo)([123])?$/);
		if (!match) {
			return {};
		}
		const agentMode = normalizeAgentMode(match[1] === "deepred" ? "deep-red" : match[1]);
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
			low: { 1: "medium" },
			medium: { 1: "medium" },
			high: { 1: "xhigh" },
			ultra: { 1: "high" },
			smart: { 1: "high", 2: "xhigh", 3: "max" },
			rush: { 1: "none" },
			deep: { 1: "low", 2: "medium", 3: "xhigh" },
			nostromo: { 1: "low" },
			eni: { 1: "max" },
			"deep-red": { 1: "max" },
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
		globalThis.localStorage.setItem(threadWorkingDirectoriesStorageKey, JSON.stringify(boundedThreadValues(directories, threadID)));
		diagnostics.lastInheritedWorkingDirectory = workingDirectory;
		diagnostics.lastInheritedWorkingDirectoryThreadID = threadID;
	}

	function rememberLocalThreadID(threadID) {
		if (!threadID || !threadID.startsWith("T-")) {
			return;
		}
		const ids = localThreadIDs().filter((value) => value !== threadID);
		ids.unshift(threadID);
		globalThis.localStorage.setItem(localThreadIDsStorageKey, JSON.stringify(ids.slice(0, localThreadStorageLimit)));
		globalThis.localStorage.setItem(threadWorkingDirectoriesStorageKey, JSON.stringify(boundedThreadValues(threadWorkingDirectories(), threadID)));
		globalThis.localStorage.setItem(threadSettingsStorageKey, JSON.stringify(boundedThreadValues(threadSettings(), threadID)));
		normalizeLocalThreadViewPath(threadID);
	}

	function forgetLocalThreadID(threadID) {
		if (!validThreadID(threadID)) {
			return;
		}
		globalThis.localStorage.setItem(localThreadIDsStorageKey, JSON.stringify(localThreadIDs().filter((value) => value !== threadID)));
		const directories = threadWorkingDirectories();
		delete directories[threadID];
		globalThis.localStorage.setItem(threadWorkingDirectoriesStorageKey, JSON.stringify(directories));
		const settings = threadSettings();
		delete settings[threadID];
		globalThis.localStorage.setItem(threadSettingsStorageKey, JSON.stringify(settings));
	}

	function rememberedLocalThreadID(threadID) {
		return !!threadID && localThreadIDs().includes(threadID);
	}

	function normalizeLocalThreadViewPath(threadID) {
		threadID = normalizeThreadIDValue(threadID);
		const match = String(globalThis.location.pathname || "").match(/^\/threads\/([^/?#]+)\/view\/?$/);
		if (!threadID || !match || decodedThreadID(match[1]) !== threadID) {
			return false;
		}
		const target = "/threads/" + encodeURIComponent(threadID);
		if (localThreadViewRedirectTarget === target) {
			return true;
		}
		localThreadViewRedirectTarget = target;
		const navigate = () => {
			const current = String(globalThis.location.pathname || "").match(/^\/threads\/([^/?#]+)\/view\/?$/);
			if (!current || decodedThreadID(current[1]) !== threadID) {
				return true;
			}
			if (typeof globalThis.location?.replace === "function") {
				diagnostics.localThreadViewRedirectCount += 1;
				diagnostics.lastLocalThreadViewRedirect = target;
				globalThis.location.replace(target);
				return true;
			}
			const anchor = globalThis.document?.createElement?.("a");
			const root = globalThis.document?.body || globalThis.document?.documentElement;
			if (anchor && root && typeof root.appendChild === "function" && typeof anchor.click === "function") {
				anchor.href = target;
				anchor.hidden = true;
				root.appendChild(anchor);
				diagnostics.localThreadViewRedirectCount += 1;
				diagnostics.lastLocalThreadViewRedirect = target;
				anchor.click();
				anchor.parentElement?.removeChild?.(anchor);
				return true;
			}
			if (typeof globalThis.history?.replaceState === "function") {
				diagnostics.localThreadViewRedirectCount += 1;
				diagnostics.lastLocalThreadViewRedirect = target;
				globalThis.history.replaceState(globalThis.history.state, "", target);
				return true;
			}
			localThreadViewRedirectTarget = "";
			return false;
		};
		const schedule = () => globalThis.setTimeout(navigate, 0);
		if (globalThis.document?.readyState === "loading" && typeof globalThis.document.addEventListener === "function") {
			globalThis.document.addEventListener("DOMContentLoaded", schedule, { once: true });
		} else {
			schedule();
		}
		return true;
	}

	function discoverLocalThreadID(threadID) {
		threadID = normalizeThreadIDValue(threadID);
		if (!threadID) {
			return Promise.resolve(false);
		}
		if (rememberedLocalThreadID(threadID)) {
			normalizeLocalThreadViewPath(threadID);
			return Promise.resolve(true);
		}
		if (localThreadDiscoveryPromises.has(threadID)) {
			return localThreadDiscoveryPromises.get(threadID);
		}
		const headers = localFetchHeaders("", false);
		if (!headers.get("Authorization")) {
			return Promise.resolve(false);
		}
		const url = new URL(localBaseURLString() + localThreadDataEndpointPath);
		url.searchParams.set("cliproxy-thread-id", threadID);
		url.searchParams.set("cliproxy-summary-only", "1");
		diagnostics.localThreadDiscoveryCount += 1;
		const promise = originalFetch(url.href, {
			method: "GET",
			headers,
			mode: "cors",
			credentials: "omit",
		}).then((response) => {
			if (!response.ok) {
				return false;
			}
			return response.text().then((text) => {
				const decoded = originalJSONParse(text || "{}");
				const localThreadID = firstNormalizedThreadID(decoded?.thread?.id, decoded?.thread?.threadId, decoded?.thread?.threadID);
				if (localThreadID !== threadID) {
					return false;
				}
				rememberLocalThreadID(threadID);
				return true;
			});
		}).catch(() => false).finally(() => {
			localThreadDiscoveryPromises.delete(threadID);
		});
		localThreadDiscoveryPromises.set(threadID, promise);
		return promise;
	}

	function validThreadID(value) {
		return typeof value === "string" && /^T-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/.test(value.trim());
	}

	function decodedThreadID(value) {
		try {
			return normalizeThreadIDValue(decodeURIComponent(value));
		} catch {
			return "";
		}
	}

	function normalizeThreadIDValue(value) {
		value = typeof value === "string" ? value.trim() : "";
		return validThreadID(value) ? value : "";
	}

	function firstNormalizedThreadID(...values) {
		for (const value of values) {
			const threadID = normalizeThreadIDValue(value);
			if (threadID) {
				return threadID;
			}
		}
		return "";
	}

	function rememberObservedThreadID(threadID) {
		threadID = normalizeThreadIDValue(threadID);
		if (!threadID) {
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

	function firstThreadIDFromValue(value, seen = new WeakSet(), allowDirect = false) {
			if (allowDirect && validThreadID(value)) {
				return value.trim();
			}
			if (value === null || typeof value !== "object" || seen.has(value)) {
				return "";
			}
			seen.add(value);
				if (Array.isArray(value)) {
					for (const child of value) {
						const threadID = firstThreadIDFromValue(child, seen, false);
						if (threadID) {
							return threadID;
				}
				}
					return "";
			}
			if (!isPlainObject(value)) {
				return "";
				}
				for (const key of ["threadId", "threadID", "thread_id", "thread"]) {
					const threadID = firstThreadIDFromValue(value[key], seen, true);
					if (threadID) {
						return threadID;
			}
			}
			for (const key of ["input", "params", "args", "data", "payload"]) {
					const raw = value[key];
				const decoded = typeof raw === "string" ? parseJSONText(raw) : raw;
				if (decoded !== null && decoded !== undefined) {
					const threadID = firstThreadIDFromValue(decoded, seen, false);
						if (threadID) {
							return threadID;
			}
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

	function ampUserContextLike(value) {
		return isPlainObject(value) && [
			"userWorkspace",
			"userFeatures",
			"workspaces",
			"viewState",
			"localAmpCheckoutRoot",
		].some((key) => Object.prototype.hasOwnProperty.call(value, key));
	}

	function ampUserIdentity(value) {
		if (!isPlainObject(value)) {
			return "";
		}
		const id = firstString(value.id, value.userId, value.userID);
		if (!id || id.length > 256) {
			return "";
		}
		return id;
	}

	function ampUserProfile(value) {
		if (!isPlainObject(value)) {
			return null;
		}
		const id = ampUserIdentity(value);
		if (!id) {
			return null;
		}
		return {
			id,
			username: firstString(value.username, value.handle),
			firstName: firstString(value.firstName, value.first_name),
			lastName: firstString(value.lastName, value.last_name),
			email: firstString(value.email),
			profilePictureUrl: firstString(value.profilePictureUrl, value.profilePictureURL, value.avatarUrl, value.avatarURL, value.picture),
		};
	}

	function bootstrapAssignedJSONString(source, assignment) {
		const start = source.indexOf(assignment);
		if (start < 0 || source[start + assignment.length] !== '"') {
			return "";
		}
		let escaped = false;
		for (let i = start + assignment.length + 1; i < source.length; i += 1) {
			const character = source[i];
			if (escaped) {
				escaped = false;
				continue;
			}
			if (character === "\\") {
				escaped = true;
				continue;
			}
			if (character !== '"') {
				continue;
			}
			try {
				return firstString(originalJSONParse(source.slice(start + assignment.length, i + 1)));
			} catch {
				return "";
			}
		}
		return "";
	}

	function bootstrapObjectLiteral(source, start) {
		if (source[start] !== "{") {
			return "";
		}
		let depth = 0;
		let quote = "";
		let escaped = false;
		for (let i = start; i < source.length; i += 1) {
			const character = source[i];
			if (quote) {
				if (escaped) {
					escaped = false;
				} else if (character === "\\") {
					escaped = true;
				} else if (character === quote) {
					quote = "";
				}
				continue;
			}
			if (character === '"' || character === "'" || character.charCodeAt(0) === 96) {
				quote = character;
				continue;
			}
			if (character === "{") {
				depth += 1;
			} else if (character === "}" && --depth === 0) {
				return source.slice(start, i + 1);
			}
		}
		return "";
	}

	function bootstrapObjectLiteralProfile(source) {
		const userPattern = /(?:^|[,{])\s*["']?user["']?\s*:\s*\{/g;
		let matchedProfile = null;
		for (const match of source.matchAll(userPattern)) {
			const objectStart = match.index + match[0].lastIndexOf("{");
			const objectSource = bootstrapObjectLiteral(source, objectStart);
			if (!objectSource) {
				continue;
			}
			const profile = {};
			for (const field of ["id", "username", "firstName", "lastName", "email", "profilePictureUrl"]) {
				const propertyMatch = objectSource.match(new RegExp("(?:^|[,{])\\s*[\"']?" + field + "[\"']?\\s*:\\s*(\\\"(?:\\\\.|[^\\\"\\\\])*\\\")"));
				if (!propertyMatch) {
					continue;
				}
				try {
					profile[field] = firstString(originalJSONParse(propertyMatch[1]));
				} catch {
				}
			}
			const normalizedProfile = ampUserProfile(profile);
			if (!normalizedProfile || !firstString(normalizedProfile.username, normalizedProfile.email, normalizedProfile.profilePictureUrl)) {
				continue;
			}
			if (matchedProfile && matchedProfile.id !== normalizedProfile.id) {
				return null;
			}
			matchedProfile = normalizedProfile;
		}
		return matchedProfile;
	}

	function captureAuthenticatedAmpUserFromBootstrap() {
		if (authenticatedAmpUserID) {
			return authenticatedAmpUserID;
		}
		for (const script of globalThis.document?.querySelectorAll?.("script:not([src])") || []) {
			const source = typeof script?.textContent === "string" ? script.textContent : "";
			if (!/(?:^|[,{])\s*["']?initialProjects["']?\s*:/.test(source) || !/(?:^|[,{])\s*["']?userFeatures["']?\s*:/.test(source)) {
				continue;
			}
			const functionMatch = source.match(/\.data\s*=\s*\(function\(([$A-Z_a-z][$\w]*)\)\{/);
			if (functionMatch) {
				const returnIndex = source.indexOf("return", functionMatch.index);
				if (returnIndex >= 0) {
					const prefix = source.slice(functionMatch.index, returnIndex);
					const objectPrefix = functionMatch[1] + ".";
					const profile = {};
					for (const field of ["id", "username", "firstName", "lastName", "profilePictureUrl"]) {
						const value = bootstrapAssignedJSONString(prefix, objectPrefix + field + "=");
						if (value) {
							profile[field] = value;
						}
					}
					if (ampUserProfile(profile)) {
						return rememberAuthenticatedAmpUser(profile);
					}
				}
			}
			const dataMatch = source.match(/\b__sveltekit[$\w]*\.data\s*=\s*\{/);
			if (dataMatch) {
				const keyIndexAfter = (key, from) => {
					const match = source.slice(from).match(new RegExp("(?:^|[,{])\\s*[\"']?" + key + "[\"']?\\s*:"));
					return match ? from + match.index : -1;
				};
				const initialProjectsIndex = keyIndexAfter("initialProjects", dataMatch.index);
				const userFeaturesIndex = initialProjectsIndex >= 0 ? keyIndexAfter("userFeatures", initialProjectsIndex) : -1;
				if (initialProjectsIndex > dataMatch.index && userFeaturesIndex > initialProjectsIndex) {
					const objectLiteralProfile = bootstrapObjectLiteralProfile(source.slice(dataMatch.index, initialProjectsIndex));
					if (objectLiteralProfile) {
						return rememberAuthenticatedAmpUser(objectLiteralProfile);
					}
				}
			}
		}
		return authenticatedAmpUserID;
	}

	function rememberAuthenticatedAmpUser(value) {
		const profile = ampUserProfile(value);
		if (!profile) {
			return authenticatedAmpUserID;
		}
		const previousProfile = JSON.stringify([authenticatedAmpUserID, authenticatedAmpUser]);
		const identityChanged = profile.id !== authenticatedAmpUserID;
		if (identityChanged) {
			authenticatedAmpUserID = profile.id;
			authenticatedAmpUser = {};
			diagnostics.authenticatedAmpUserIDCaptureCount += 1;
			try {
				globalThis.sessionStorage.setItem(authenticatedAmpUserIDStorageKey, authenticatedAmpUserID);
			} catch {
			}
			activateLocalSidebarTitleOwner(authenticatedAmpUserID);
			promotePendingLocalAPIKey();
		}
		authenticatedAmpUser = Object.assign({}, authenticatedAmpUser, Object.fromEntries(
			Object.entries(profile).filter(([, item]) => typeof item === "string" && item.trim() !== ""),
		));
		if (previousProfile !== JSON.stringify([authenticatedAmpUserID, authenticatedAmpUser])) {
			localActivityCache = { at: 0, key: "", value: null, promise: null };
			if (globalThis.location.pathname === "/feed") {
				scheduleLocalActivityRefresh();
			}
		}
		if (identityChanged) {
			startPassiveLocalSidebarHydration();
		}
		return authenticatedAmpUserID;
	}

	function rememberAuthenticatedAmpUserIDValue(value) {
		const userID = ampUserIdentity({ id: value });
		if (!userID) {
			return authenticatedAmpUserID;
		}
		return rememberAuthenticatedAmpUser({ id: userID });
	}

	function authenticatedAmpUserFromPlain(value) {
		if (!isPlainObject(value) || !isPlainObject(value.user)) {
			return null;
		}
		if (!ampUserContextLike(value)) {
			return null;
		}
		return ampUserProfile(value.user);
	}

	function authenticatedAmpUserFromDevalue(values) {
		if (!Array.isArray(values)) {
			return null;
		}
		for (const entry of values) {
			if (!isPlainObject(entry) || !Number.isInteger(entry.user)) {
				continue;
			}
			const user = values[entry.user];
			if (!isPlainObject(user)) {
				continue;
			}
			if (!ampUserContextLike(entry)) {
				continue;
			}
			const field = (name) => Number.isInteger(user[name]) ? firstString(values[user[name]]) : firstString(user[name]);
			const profile = ampUserProfile({
				id: field("id"),
				username: field("username"),
				firstName: field("firstName"),
				lastName: field("lastName"),
				email: field("email"),
				profilePictureUrl: field("profilePictureUrl"),
			});
			if (profile) {
				return profile;
			}
		}
		return null;
	}

	function rememberAuthenticatedAmpUserID(decoded) {
		if (decoded === null || typeof decoded !== "object") {
			return authenticatedAmpUserID;
		}
		const pending = [decoded];
		const seen = new WeakSet();
		let visits = 0;
		while (pending.length > 0 && visits < 512) {
			const value = pending.pop();
			if (value === null || typeof value !== "object" || seen.has(value)) {
				continue;
			}
			seen.add(value);
			visits += 1;
			const user = authenticatedAmpUserFromPlain(value) || authenticatedAmpUserFromDevalue(value);
			if (user) {
				return rememberAuthenticatedAmpUser(user);
			}
			if (Array.isArray(value)) {
				for (const child of value) {
					if (child !== null && typeof child === "object") {
						pending.push(child);
					}
				}
				continue;
			}
			for (const key of ["data", "nodes", "result", "current", "_"]) {
				const child = value[key];
				if (child !== null && typeof child === "object") {
					pending.push(child);
				} else if (typeof child === "string" && child.length <= 2 * 1024 * 1024 && /^\s*[\[{]/.test(child)) {
					try {
						const decoded = originalJSONParse(child);
						if (decoded !== null && typeof decoded === "object") {
							pending.push(decoded);
						}
					} catch {
					}
				}
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

	function localPlainThreadActorConfig(threadID, localBase) {
		return {
			threadId: threadID,
			wsToken: storedLocalAPIKey() || "local-neo",
			ampURL: localBase,
			baseURL: localBase,
			capability: "write",
			poolName: "default",
			requiresSudoForWrite: false,
			requiresSudoForTerminal: false,
			threadActorTransport: "json-rpc",
		};
	}

	function localDevalueThreadActorConfig(values, threadID, baseIndex) {
		return {
			threadId: ensureDevalueStringIndex(values, threadID),
			wsToken: ensureDevalueStringIndex(values, storedLocalAPIKey() || "local-neo"),
			ampURL: baseIndex,
			baseURL: baseIndex,
			capability: ensureDevalueStringIndex(values, "write"),
			poolName: ensureDevalueStringIndex(values, "default"),
			requiresSudoForWrite: ensureDevalueValueIndex(values, false),
			requiresSudoForTerminal: ensureDevalueValueIndex(values, false),
			threadActorTransport: ensureDevalueStringIndex(values, "json-rpc"),
		};
	}

	function devalueThreadID(values, thread) {
		return rememberObservedThreadID(devalueStringField(values, thread, "id"));
	}

	function devalueContainerThreadIDValue(values, entry) {
		if (!devalueThreadContainerLike(values, entry)) {
			return "";
		}
		const direct = devalueAnyThreadID(values, entry);
		if (direct) {
			return direct;
		}
		if (!isPlainObject(entry) || !Number.isInteger(entry.thread)) {
			return "";
		}
		return devalueAnyThreadID(values, values[entry.thread]);
	}

	function patchDevalueLocalThreadOwner(values, entry) {
		if (!authenticatedAmpUserID) {
			return false;
		}
		const threadID = devalueContainerThreadIDValue(values, entry);
		if (!threadID || !rememberedLocalThreadID(threadID)) {
			return false;
		}
		const thread = devalueDirectThreadRecordLike(values, entry) ? entry : devalueObjectField(values, entry, "thread");
		if (!isPlainObject(thread)) {
			return false;
		}
		const userIndex = ensureDevalueStringIndex(values, authenticatedAmpUserID);
		let patched = false;
		if (thread.creatorUserID !== userIndex) {
			thread.creatorUserID = userIndex;
			patched = true;
		}
		if (thread.ownerUserId !== userIndex) {
			thread.ownerUserId = userIndex;
			patched = true;
		}
		const creatorIndex = appendDevalueSidebarValue(values, Object.assign({}, authenticatedAmpUser, { id: authenticatedAmpUserID }));
		for (const container of thread === entry ? [thread] : [thread, entry]) {
			if (container.creator !== creatorIndex) {
				container.creator = creatorIndex;
				patched = true;
			}
		}
		return patched;
	}

	function devalueField(values, object, key) {
		if (!isPlainObject(object) || !Number.isInteger(object[key])) {
			return undefined;
		}
		return values[object[key]];
	}

	function positiveThreadVersion(value) {
		return Number.isSafeInteger(value) && value > 0 ? value : 0;
	}

	function rememberLocalPinnedOverride(threadID, pinned) {
		threadID = normalizeThreadIDValue(threadID);
		if (!threadID || typeof pinned !== "boolean") {
			return;
		}
		localPinnedOverrides.delete(threadID);
		localPinnedOverrides.set(threadID, pinned);
		while (localPinnedOverrides.size > localPinnedOverrideLimit) {
			localPinnedOverrides.delete(localPinnedOverrides.keys().next().value);
		}
	}

	function rememberLoadedThreadBase(threadID, version, hasMessages) {
		threadID = normalizeThreadIDValue(threadID);
		version = positiveThreadVersion(version);
		if (!threadID || (!rememberedLocalThreadID(threadID) && threadID !== activeThreadID()) || !version || !hasMessages) {
			return;
		}
		const current = loadedThreadBaseVersions.get(threadID) || 0;
		if (version === current) {
			return;
		}
		loadedThreadBaseVersions.set(threadID, version);
		while (loadedThreadBaseVersions.size > loadedThreadBaseVersionLimit) {
			loadedThreadBaseVersions.delete(loadedThreadBaseVersions.keys().next().value);
		}
		diagnostics.loadedThreadBaseCaptureCount += 1;
		diagnostics.loadedThreadBaseVersionEntryCount = loadedThreadBaseVersions.size;
		diagnostics.lastLoadedThreadBaseThreadID = threadID;
		diagnostics.lastLoadedThreadBaseVersion = version;
	}

	function rememberDevalueLoadedThreadBase(values, thread) {
		if (!isPlainObject(thread)) {
			return;
		}
		const data = devalueObjectField(values, thread, "data");
		const threadID = devalueAnyThreadID(values, thread) || devalueAnyThreadID(values, data);
		const version = positiveThreadVersion(devalueField(values, thread, "v")) || positiveThreadVersion(devalueField(values, data, "v"));
		const messages = devalueField(values, thread, "messages");
		const dataMessages = devalueField(values, data, "messages");
		rememberLoadedThreadBase(threadID, version, Array.isArray(messages) || Array.isArray(dataMessages));
	}

	function devalueStringField(values, object, key) {
		const value = devalueField(values, object, key);
		return typeof value === "string" ? value : "";
	}

	function devalueAnyThreadID(values, thread) {
		if (!isPlainObject(thread)) {
			return "";
		}
		return firstNormalizedThreadID(
			devalueStringField(values, thread, "id"),
			devalueStringField(values, thread, "threadId"),
			devalueStringField(values, thread, "threadID"),
			devalueStringField(values, thread, "thread_id"),
		);
	}

	function devalueDirectThreadRecordLike(values, thread) {
		if (!validThreadID(devalueAnyThreadID(values, thread))) {
			return false;
		}
		return Object.prototype.hasOwnProperty.call(thread, "threadActorConfig") ||
			Object.prototype.hasOwnProperty.call(thread, "title") ||
			Object.prototype.hasOwnProperty.call(thread, "v") ||
			Object.prototype.hasOwnProperty.call(thread, "messages") ||
			Object.prototype.hasOwnProperty.call(thread, "data") ||
			Object.prototype.hasOwnProperty.call(thread, "meta") ||
			Object.prototype.hasOwnProperty.call(thread, "env") ||
			Object.prototype.hasOwnProperty.call(thread, "environment") ||
			Object.prototype.hasOwnProperty.call(thread, "workspace");
	}

	function devalueThreadContainerLike(values, entry) {
		if (devalueDirectThreadRecordLike(values, entry)) {
			return true;
		}
		if (!isPlainObject(entry) || !Number.isInteger(entry.thread)) {
			return false;
		}
		const thread = values[entry.thread];
		return devalueDirectThreadRecordLike(values, thread) ||
			(Object.prototype.hasOwnProperty.call(entry, "threadActorConfig") && validThreadID(devalueAnyThreadID(values, thread)));
	}

	function devalueContainerAnyThreadID(values, entry) {
		if (!devalueThreadContainerLike(values, entry)) {
			return "";
		}
		const direct = devalueAnyThreadID(values, entry);
		if (direct) {
			return direct;
		}
		if (!isPlainObject(entry) || !Number.isInteger(entry.thread)) {
			return "";
		}
		return devalueAnyThreadID(values, values[entry.thread]);
	}

	function devalueFalseField(values, object, key) {
		if (!isPlainObject(object) || !Number.isInteger(object[key])) {
			return false;
		}
		return values[object[key]] === false;
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

	function devalueStandaloneThreadActorConfigLike(values, config) {
		if (!devalueThreadActorConfigLike(config) || !validThreadID(devalueStringField(values, config, "threadId")) || !devalueThreadActorConfigHasBridgeFields(config)) {
			return false;
		}
		return devalueStringField(values, config, "threadActorTransport") === "json-rpc" ||
			(typeof config.capability === "number" && typeof config.poolName === "number");
	}

	function patchDevalueThreadActorConfig(values, index, baseIndex, referenced = false) {
		if (!Number.isInteger(index) || index < 0 || index >= values.length) {
			return false;
		}
		const config = values[index];
		if (!(referenced ? devalueThreadActorConfigLike(config) : devalueStandaloneThreadActorConfigLike(values, config))) {
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
		const apiKey = storedLocalAPIKey();
		if (apiKey) {
			const tokenIndex = ensureDevalueStringIndex(values, apiKey);
			if (config.wsToken !== tokenIndex) {
				config.wsToken = tokenIndex;
				patched = true;
			}
		}
		return patched;
	}

	function ensureDevalueLocalThreadActorConfig(values, thread, threadID, getBaseIndex) {
		if (!isPlainObject(thread) || !threadID || !rememberedLocalThreadID(threadID)) {
			return false;
		}
		if (Number.isInteger(thread.threadActorConfig) && devalueThreadActorConfigLike(values[thread.threadActorConfig])) {
			return false;
		}
		thread.threadActorConfig = ensureDevalueValueIndex(values, localDevalueThreadActorConfig(values, threadID, getBaseIndex()));
		diagnostics.lastPatchedThreadID = threadID;
		return true;
	}

	function rememberDevalueThreadRuntime(values, threadIndex) {
		if (!Number.isInteger(threadIndex) || threadIndex < 0 || threadIndex >= values.length) {
			return false;
		}
		const thread = values[threadIndex];
		rememberDevalueLoadedThreadBase(values, thread);
		const activeThread = activeThreadID();
		if (!isPlainObject(thread) || !activeThread || devalueThreadID(values, thread) !== activeThread) {
			return false;
		}
		rememberThreadWorkingDirectory(activeThread, devalueThreadWorkingDirectory(values, thread));
		rememberThreadSettings(activeThread, devalueThreadSettings(values, thread));
		return false;
	}

	function clearDevalueStaleLocalThreadExecutorState(values, thread) {
		const threadID = devalueContainerAnyThreadID(values, thread);
		if (!threadID || !rememberedLocalThreadID(threadID)) {
			return false;
		}
		let patched = false;
		if (devalueFalseField(values, thread, "hasExecutor")) {
			delete thread.hasExecutor;
			patched = true;
		}
		if (devalueFalseField(values, thread, "executorConnected")) {
			delete thread.executorConnected;
			patched = true;
		}
		if (patched) {
			diagnostics.localThreadStatusPatchCount += 1;
			diagnostics.lastPatchedThreadID = threadID;
		}
		return patched;
	}

	function patchDevalueLocalThreadArchivedState(values, entry) {
		const threadID = devalueContainerAnyThreadID(values, entry);
		const archived = activeLocalArchiveState(threadID);
		if (typeof archived !== "boolean") {
			return false;
		}
		const thread = devalueDirectThreadRecordLike(values, entry) ? entry : devalueObjectField(values, entry, "thread");
		if (!isPlainObject(thread)) {
			return false;
		}
		let patched = false;
		for (const container of thread === entry ? [thread] : [thread, entry]) {
			if (!isPlainObject(container) || container !== thread && !Object.prototype.hasOwnProperty.call(container, "archived")) {
				continue;
			}
			const current = Number.isInteger(container.archived) ? values[container.archived] : container.archived;
			if (current === archived || !archived && current !== true) {
				continue;
			}
			container.archived = ensureDevalueValueIndex(values, archived);
			patched = true;
		}
		return patched;
	}

	function patchDevalueThreadActorConfigs(values, localBase, stats) {
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
		const candidateIndexes = [];
		let excludedIndexes = null;
		const pendingExcludedIndexes = [];
		const markExcludedIndex = (index) => {
			if (!Number.isInteger(index) || index < 0 || index >= originalLength) {
				return;
			}
			if (excludedIndexes === null) {
				excludedIndexes = new Uint8Array(originalLength);
			}
			if (excludedIndexes[index] === 0) {
				excludedIndexes[index] = 1;
				pendingExcludedIndexes.push(index);
			}
		};
		for (let i = 0; i < originalLength; i += 1) {
			const entry = values[i];
			if (!isPlainObject(entry)) {
				continue;
			}
			rememberDevalueLoadedThreadBase(values, entry);
			rememberDevalueThreadSettingsMessage(values, entry);
			if (devalueThreadContainerLike(values, entry) ||
				Number.isInteger(entry.threadActorConfig) ||
				devalueStandaloneThreadActorConfigLike(values, entry)) {
				candidateIndexes.push(i);
			}
			const type = devalueStringField(values, entry, "type");
			const role = devalueStringField(values, entry, "role");
			if (type === "tool_use" || type === "tool_result" ||
				(role && Object.prototype.hasOwnProperty.call(entry, "content"))) {
				markExcludedIndex(i);
			}
			if (Number.isInteger(entry.messages)) {
				markExcludedIndex(entry.messages);
			}
			if (Number.isInteger(entry.compactionRecords)) {
				markExcludedIndex(entry.compactionRecords);
			}
		}
		while (pendingExcludedIndexes.length > 0) {
			const excluded = values[pendingExcludedIndexes.pop()];
			if (Array.isArray(excluded)) {
				for (const raw of excluded) {
					markExcludedIndex(raw);
				}
				continue;
			}
			if (!isPlainObject(excluded)) {
				continue;
			}
			for (const key in excluded) {
				if (Object.prototype.hasOwnProperty.call(excluded, key)) {
					markExcludedIndex(excluded[key]);
				}
			}
		}
		for (const i of candidateIndexes) {
			if (excludedIndexes !== null && excludedIndexes[i] !== 0) {
				continue;
			}
			const entry = values[i];
			patched = clearDevalueStaleLocalThreadExecutorState(values, entry) || patched;
			patched = patchDevalueLocalThreadArchivedState(values, entry) || patched;
			const entryThreadID = activeThread ? devalueContainerThreadIDValue(values, entry) : "";
			if (entryThreadID && entryThreadID === activeThread) {
				rememberObservedThreadID(entryThreadID);
				rememberThreadSettings(activeThread, devalueThreadSettings(values, entry));
				rememberDevalueThreadRuntime(values, i);
				patched = patchDevalueLocalThreadOwner(values, entry) || patched;
				if (!rememberedLocalThreadID(activeThread) &&
					(devalueFalseField(values, entry, "hasExecutor") ||
						devalueFalseField(values, entry, "executorConnected") ||
						!(Number.isInteger(entry.threadActorConfig) && devalueThreadActorConfigLike(values[entry.threadActorConfig])))) {
					stats.pendingDevalueLocalityValues.push({ values, entry, threadID: activeThread });
				} else {
					patched = ensureDevalueLocalThreadActorConfig(values, entry, activeThread, getBaseIndex) || patched;
				}
			}
			if (Number.isInteger(entry.threadActorConfig)) {
				let configIndex = entry.threadActorConfig;
				let config = values[configIndex];
				if (devalueThreadActorConfigLike(config)) {
					if (excludedIndexes !== null && configIndex < originalLength && excludedIndexes[configIndex] !== 0) {
						config = { ...config };
						values.push(config);
						configIndex = values.length - 1;
						entry.threadActorConfig = configIndex;
						patched = true;
					}
					patched = patchDevalueThreadActorConfig(values, configIndex, getBaseIndex(), true) || patched;
					if (Number.isInteger(entry.thread)) {
						rememberDevalueThreadRuntime(values, entry.thread);
					}
				}
			}
			if (devalueStandaloneThreadActorConfigLike(values, entry)) {
				patched = patchDevalueThreadActorConfig(values, i, getBaseIndex()) || patched;
			}
		}
		return patched;
	}

	function plainThreadMatchesActive(thread) {
		const activeThread = activeThreadID();
		return !!activeThread && plainDirectThreadRecordLike(thread) && plainAnyThreadID(thread) === activeThread;
	}

	function plainContainerThreadID(entry) {
		if (!plainThreadContainerLike(entry)) {
			return "";
		}
		const activeThread = activeThreadID();
		const direct = plainAnyThreadID(entry);
		if (activeThread && direct === activeThread) {
			return direct;
		}
		if (isPlainObject(entry) && plainThreadMatchesActive(entry.thread)) {
			return plainAnyThreadID(entry.thread);
		}
		return "";
	}

	function rememberPlainThreadRuntime(thread) {
		if (isPlainObject(thread)) {
			const data = isPlainObject(thread.data) ? thread.data : null;
			rememberLoadedThreadBase(
				normalizeThreadIDValue(thread.id) || normalizeThreadIDValue(data?.id),
				positiveThreadVersion(thread.v) || positiveThreadVersion(data?.v),
				Array.isArray(thread.messages) || Array.isArray(data?.messages),
			);
		}
		if (!plainThreadMatchesActive(thread)) {
			return false;
		}
		const threadID = plainAnyThreadID(thread);
		rememberThreadWorkingDirectory(threadID, plainThreadWorkingDirectory(thread));
		rememberThreadSettings(threadID, plainThreadSettings(thread));
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

	function plainStandaloneThreadActorConfigLike(config) {
		if (!plainThreadActorConfigLike(config) || !validThreadID(firstString(config.threadId, config.threadID, config.thread_id)) || !plainThreadActorConfigHasBridgeFields(config)) {
			return false;
		}
		return config.threadActorTransport === "json-rpc" ||
			(typeof config.capability === "string" && typeof config.poolName === "string");
	}

	function plainTreesWorkingDirectories(...containers) {
		const directories = [];
		for (const container of containers) {
			if (!isPlainObject(container) || !Array.isArray(container.trees)) {
				continue;
			}
			for (const tree of container.trees) {
				if (!isPlainObject(tree)) {
					continue;
				}
				const workingDirectory = firstWorkingDirectory(tree.workingDirectory, tree.workspaceRoot, tree.uri);
				if (workingDirectory && !directories.includes(workingDirectory)) {
					directories.push(workingDirectory);
				}
			}
		}
		return directories;
	}

	function workingDirectoryWithinWorkspace(workspaceRoot, workingDirectory) {
		workspaceRoot = normalizeWorkingDirectory(workspaceRoot).replaceAll("\\", "/");
		workingDirectory = normalizeWorkingDirectory(workingDirectory).replaceAll("\\", "/").replace(/\/+$/, "");
		if (!workspaceRoot || !workingDirectory) {
			return false;
		}
		workspaceRoot = workspaceRoot === "/" ? workspaceRoot : workspaceRoot.replace(/\/+$/, "");
		if (/^[a-z]:/i.test(workspaceRoot) || /^[a-z]:/i.test(workingDirectory)) {
			workspaceRoot = workspaceRoot.toLowerCase();
			workingDirectory = workingDirectory.toLowerCase();
		}
		return workspaceRoot === "/" || workingDirectory === workspaceRoot || workingDirectory.startsWith(workspaceRoot + "/");
	}

	function plainThreadWorkingDirectory(thread) {
		const env = isPlainObject(thread.env) ? thread.env : isPlainObject(thread.environment) ? thread.environment : {};
		const initial = isPlainObject(env.initial) ? env.initial : {};
		const workspace = isPlainObject(thread.workspace) ? thread.workspace : {};
		const explicitWorkingDirectory = firstWorkingDirectory(
			thread.workingDirectory,
			thread.workspaceRoot,
			env.workingDirectory,
			env.workspaceRoot,
			initial.workingDirectory,
			initial.workspaceRoot,
			workspace.workingDirectory,
			workspace.workspaceRoot,
		);
		const treeWorkingDirectories = [
			firstWorkingDirectory(workspace.workspaceRoot, workspace.uri),
			...plainTreesWorkingDirectories(env, initial),
		].filter((workingDirectory, index, directories) => workingDirectory && directories.indexOf(workingDirectory) === index);
		if (treeWorkingDirectories.length === 0) {
			return explicitWorkingDirectory;
		}
		if (treeWorkingDirectories.some((workspaceRoot) => workingDirectoryWithinWorkspace(workspaceRoot, explicitWorkingDirectory))) {
			return explicitWorkingDirectory;
		}
		return treeWorkingDirectories[0];
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

	function plainAnyThreadID(value) {
		if (!isPlainObject(value)) {
			return "";
		}
		return firstNormalizedThreadID(value.id, value.threadId, value.threadID, value.thread_id);
	}

	function patchPlainLocalThreadOwner(value) {
		if (!authenticatedAmpUserID) {
			return false;
		}
		const threadID = plainContainerAnyThreadID(value);
		if (!threadID || !rememberedLocalThreadID(threadID)) {
			return false;
		}
		const thread = plainDirectThreadRecordLike(value) ? value : isPlainObject(value.thread) ? value.thread : null;
		if (!thread) {
			return false;
		}
		let patched = false;
		if (thread.creatorUserID !== authenticatedAmpUserID) {
			thread.creatorUserID = authenticatedAmpUserID;
			patched = true;
		}
		if (thread.ownerUserId !== authenticatedAmpUserID) {
			thread.ownerUserId = authenticatedAmpUserID;
			patched = true;
		}
		for (const container of thread === value ? [thread] : [thread, value]) {
			if (!isPlainObject(container.creator)) {
				container.creator = Object.assign({}, authenticatedAmpUser, { id: authenticatedAmpUserID });
				patched = true;
				continue;
			}
			const creator = Object.assign({}, container.creator, authenticatedAmpUser, { id: authenticatedAmpUserID });
			if (JSON.stringify(container.creator) !== JSON.stringify(creator)) {
				container.creator = creator;
				patched = true;
			}
		}
		return patched;
	}

	function plainDirectThreadRecordLike(thread) {
		if (!validThreadID(plainAnyThreadID(thread))) {
			return false;
		}
		return Object.prototype.hasOwnProperty.call(thread, "threadActorConfig") ||
			Object.prototype.hasOwnProperty.call(thread, "title") ||
			Object.prototype.hasOwnProperty.call(thread, "v") ||
			Object.prototype.hasOwnProperty.call(thread, "messages") ||
			Object.prototype.hasOwnProperty.call(thread, "data") ||
			Object.prototype.hasOwnProperty.call(thread, "meta") ||
			Object.prototype.hasOwnProperty.call(thread, "env") ||
			Object.prototype.hasOwnProperty.call(thread, "environment") ||
			Object.prototype.hasOwnProperty.call(thread, "workspace");
	}

	function plainThreadContainerLike(entry) {
		if (plainDirectThreadRecordLike(entry)) {
			return true;
		}
		if (!isPlainObject(entry) || !isPlainObject(entry.thread)) {
			return false;
		}
		return plainDirectThreadRecordLike(entry.thread) ||
			(Object.prototype.hasOwnProperty.call(entry, "threadActorConfig") && validThreadID(plainAnyThreadID(entry.thread)));
	}

	function plainContainerAnyThreadID(entry) {
		if (!plainThreadContainerLike(entry)) {
			return "";
		}
		const direct = plainAnyThreadID(entry);
		if (direct) {
			return direct;
		}
		return isPlainObject(entry) ? plainAnyThreadID(entry.thread) : "";
	}

	function clearPlainStaleLocalThreadExecutorState(thread) {
		const threadID = plainContainerAnyThreadID(thread);
		if (!threadID || !rememberedLocalThreadID(threadID)) {
			return false;
		}
		let patched = false;
		if (thread.hasExecutor === false) {
			delete thread.hasExecutor;
			patched = true;
		}
		if (thread.executorConnected === false) {
			delete thread.executorConnected;
			patched = true;
		}
		if (patched) {
			diagnostics.localThreadStatusPatchCount += 1;
			diagnostics.lastPatchedThreadID = threadID;
		}
		return patched;
	}

	function patchPlainLocalThreadArchivedState(entry) {
		const threadID = plainContainerAnyThreadID(entry);
		const archived = activeLocalArchiveState(threadID);
		if (typeof archived !== "boolean") {
			return false;
		}
		const thread = plainDirectThreadRecordLike(entry) ? entry : isPlainObject(entry?.thread) ? entry.thread : null;
		if (!thread) {
			return false;
		}
		let patched = false;
		for (const container of thread === entry ? [thread] : [thread, entry]) {
			if (!isPlainObject(container) || container !== thread && !Object.prototype.hasOwnProperty.call(container, "archived")) {
				continue;
			}
			if (container.archived === archived || !archived && container.archived !== true) {
				continue;
			}
			container.archived = archived;
			patched = true;
		}
		return patched;
	}

	function patchPlainThreadActorConfig(config, localBase, referenced = false) {
		if (!(referenced ? plainThreadActorConfigLike(config) : plainStandaloneThreadActorConfigLike(config))) {
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
		const apiKey = storedLocalAPIKey();
		if (apiKey && config.wsToken !== apiKey) {
			config.wsToken = apiKey;
			patched = true;
		}
		return patched;
	}

	function ensurePlainLocalThreadActorConfig(thread, localBase) {
		const threadID = plainContainerThreadID(thread);
		if (!threadID || !rememberedLocalThreadID(threadID) || plainThreadActorConfigLike(thread.threadActorConfig)) {
			return false;
		}
		thread.threadActorConfig = localPlainThreadActorConfig(threadID, localBase);
		diagnostics.lastPatchedThreadID = threadID;
		return true;
	}

	function plainThreadActorConfigValueNeedsLocality(value) {
		if (!isPlainObject(value)) {
			return false;
		}
		const threadID = plainContainerAnyThreadID(value);
		if (!threadID || rememberedLocalThreadID(threadID)) {
			return false;
		}
		return value.hasExecutor === false ||
			value.executorConnected === false ||
			(!!plainContainerThreadID(value) && !plainThreadActorConfigLike(value.threadActorConfig));
	}

	function patchPlainThreadActorConfigValue(value, localBase, stats) {
		let patched = false;
		patched = patchPlainLocalThreadOwner(value) || patched;
		patched = patchPlainLocalThreadArchivedState(value) || patched;
		if (plainThreadActorConfigValueNeedsLocality(value)) {
			stats.pendingLocalityValues.push(value);
		} else {
			patched = clearPlainStaleLocalThreadExecutorState(value) || patched;
			patched = ensurePlainLocalThreadActorConfig(value, localBase) || patched;
		}
		if (isPlainObject(value.threadActorConfig)) {
			patched = patchPlainThreadActorConfig(value.threadActorConfig, localBase, true) || patched;
			rememberPlainThreadRuntime(value.thread);
		}
		patched = patchPlainThreadActorConfig(value, localBase) || patched;
		rememberPlainThreadRuntime(value);
		rememberPlainThreadSettingsMessage(value);
		return patched;
	}

	function plainTranscriptOrToolValueLike(value) {
		if (!isPlainObject(value)) {
			return false;
		}
		return value.type === "tool_use" ||
			value.type === "tool_result" ||
			(typeof value.role === "string" && Object.prototype.hasOwnProperty.call(value, "content"));
	}

	function patchDecodedLocalInferenceGraph(value, seen, localBase, stats) {
		if (value === null || typeof value !== "object" || seen.has(value)) {
			return false;
		}
		seen.add(value);
		stats.visits += 1;
		if (plainTranscriptOrToolValueLike(value)) {
			return false;
		}
		let patched = false;
		if (Array.isArray(value)) {
			patched = patchDevalueThreadActorConfigs(value, localBase, stats) || patched;
		}
		patched = patchPlainThreadActorConfigValue(value, localBase, stats) || patched;
		if (Array.isArray(value)) {
			for (const child of value) {
				patched = patchDecodedLocalInferenceGraph(child, seen, localBase, stats) || patched;
			}
			return patched;
		}
		for (const key in value) {
			if (Object.prototype.hasOwnProperty.call(value, key) && key !== "messages" && key !== "compactionRecords") {
				patched = patchDecodedLocalInferenceGraph(value[key], seen, localBase, stats) || patched;
			}
		}
		return patched;
	}

	function parsedTextLocalInferencePatchOptions(text) {
		const source = typeof text === "string" ? text : "";
		if (!source) {
			return { configs: false };
		}
		let hasID = false;
		let hasVersion = false;
		let hasMessages = false;
		localInferencePatchFieldPattern.lastIndex = 0;
		for (let match = localInferencePatchFieldPattern.exec(source); match; match = localInferencePatchFieldPattern.exec(source)) {
			if (match[2]) {
				return { configs: true };
			}
			switch (match[1]) {
			case "id":
				hasID = true;
				break;
			case "v":
				hasVersion = true;
				break;
			case "messages":
				hasMessages = true;
				break;
			default:
				return { configs: true };
			}
			if (hasID && hasVersion && hasMessages) {
				return { configs: true };
			}
		}
		return { configs: false };
	}

	function patchDecodedLocalInference(value, options) {
		const patchOptions = options || { configs: true };
		if (patchOptions.configs) {
			const localBase = localBaseURLString();
			const stats = { visits: 0, pendingLocalityValues: [], pendingDevalueLocalityValues: [] };
			let patched = patchDecodedLocalInferenceGraph(value, new WeakSet(), localBase, stats);
			for (const pending of stats.pendingDevalueLocalityValues) {
				patched = clearDevalueStaleLocalThreadExecutorState(pending.values, pending.entry) || patched;
				patched = ensureDevalueLocalThreadActorConfig(
					pending.values,
					pending.entry,
					pending.threadID,
					() => ensureDevalueStringIndex(pending.values, localBase),
				) || patched;
			}
			for (const pending of stats.pendingLocalityValues) {
				patched = clearPlainStaleLocalThreadExecutorState(pending) || patched;
				patched = ensurePlainLocalThreadActorConfig(pending, localBase) || patched;
			}
			diagnostics.decodedGraphPassCount += 1;
			diagnostics.decodedGraphVisitCount += stats.visits;
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
		case "addThreadLabel":
		case "archiveThreadCommand":
		case "createProjectThread":
		case "deleteThreadCommand":
		case "getPersonalCreditsUsage":
		case "getPersonalDailySpendRows":
		case "getPersonalThreadsUsageTable":
		case "getPersonalTotalTokens":
		case "listServerPluginAgentModes":
		case "listProjects":
		case "listThreadListSidebar":
		case "listUserExecutorRunners":
		case "markThreadUnreadCommand":
		case "openPuckThread":
		case "pinThreadCommand":
		case "prewarmProjectThread":
		case "removeThreadLabel":
		case "searchFeedRepositories":
		case "searchFeedUsers":
		case "searchThreads":
		case "updateOwnedProjectChangesWorkflow":
			return parts[1];
		default:
			return "";
		}
	}

	function svelteKitRemotePath(path) {
		const endpoint = svelteKitRemoteEndpoint(path);
		return endpoint === "addThreadLabel" || endpoint === "archiveThreadCommand" || endpoint === "createProjectThread" || endpoint === "deleteThreadCommand" || endpoint === "listServerPluginAgentModes" || endpoint === "listUserExecutorRunners" || endpoint === "markThreadUnreadCommand" || endpoint === "pinThreadCommand" || endpoint === "prewarmProjectThread" || endpoint === "removeThreadLabel" || endpoint === "updateOwnedProjectChangesWorkflow";
	}

	function svelteKitRemoteCommandPath(path) {
		const endpoint = svelteKitRemoteEndpoint(path);
		return endpoint === "addThreadLabel" || endpoint === "archiveThreadCommand" || endpoint === "createProjectThread" || endpoint === "deleteThreadCommand" || endpoint === "markThreadUnreadCommand" || endpoint === "pinThreadCommand" || endpoint === "prewarmProjectThread" || endpoint === "removeThreadLabel" || endpoint === "updateOwnedProjectChangesWorkflow";
	}

	function localThreadMutationRemotePath(path) {
		const endpoint = svelteKitRemoteEndpoint(path);
		return endpoint === "addThreadLabel" || endpoint === "archiveThreadCommand" || endpoint === "deleteThreadCommand" || endpoint === "markThreadUnreadCommand" || endpoint === "pinThreadCommand" || endpoint === "removeThreadLabel";
	}

	function localThreadMutationRemoteThreadID(path, body) {
		if (!localThreadMutationRemotePath(path)) {
			return "";
		}
		const decoded = decodeRemoteCommandBody(body);
		return isPlainObject(decoded) ? firstString(decoded.threadID, decoded.threadId, decoded.id) : "";
	}

	function localThreadMutationRemoteBody(path, body) {
		const threadID = localThreadMutationRemoteThreadID(path, body);
		return validThreadID(threadID) && (rememberedLocalThreadID(threadID) || localSidebarCachedThreadID(threadID) || archivedLocalSidebarThreadIDs.has(threadID));
	}

	function createProjectThreadRemotePath(path) {
		return svelteKitRemoteEndpoint(path) === "createProjectThread";
	}

	function openPuckThreadRemotePath(path) {
		return svelteKitRemoteEndpoint(path) === "openPuckThread";
	}

	function rebuiltJSONResponse(response, value) {
		const bodyResponse = new Response(JSON.stringify(value), { headers: response.headers });
		const bodyMethods = new Set(["arrayBuffer", "blob", "bytes", "formData", "json", "text"]);
		const immutableMetadata = { url: response.url, type: response.type, redirected: response.redirected };
		const wrap = (source, body) => new Proxy(source, {
			get(target, property) {
				if (Object.prototype.hasOwnProperty.call(immutableMetadata, property)) {
					return immutableMetadata[property];
				}
				if (property === "body" || property === "bodyUsed") {
					return Reflect.get(body, property, body);
				}
				if (property === "clone") {
					return () => wrap(target.clone(), body.clone());
				}
				if (bodyMethods.has(property) && typeof body[property] === "function") {
					return body[property].bind(body);
				}
				const result = Reflect.get(target, property, target);
				return typeof result === "function" ? result.bind(target) : result;
			},
		});
		return wrap(response, bodyResponse);
	}

	function patchRemotePuckThreadActorConfig(value, threadID) {
		const localBase = localBaseURLString();
		if (Array.isArray(value)) {
			const root = isPlainObject(value[0]) ? value[0] : null;
			const session = root && Number.isInteger(root._) ? devalueObjectFromRaw(value, root._) : null;
			if (session && !(Number.isInteger(session.threadActorConfig) && devalueThreadActorConfigLike(value[session.threadActorConfig]))) {
				session.threadActorConfig = ensureDevalueValueIndex(value, localDevalueThreadActorConfig(value, threadID, ensureDevalueStringIndex(value, localBase)));
			}
		} else {
			const session = isPlainObject(value?._) ? value._ : value;
			if (isPlainObject(session) && !plainThreadActorConfigLike(session.threadActorConfig)) {
				session.threadActorConfig = localPlainThreadActorConfig(threadID, localBase);
			}
		}
		patchDecodedLocalInference(value, { configs: true });
	}

	async function rememberRemotePuckThread(sourceURL, response) {
		if (!openPuckThreadRemotePath(sourceURL.pathname) || !response?.ok) {
			return response;
		}
		try {
			const envelope = originalJSONParse(await response.clone().text());
			const raw = originalJSONParse(envelope?.data || "");
			const decoded = Array.isArray(raw) ? decodeDevalueString(envelope.data) : raw;
			const session = isPlainObject(decoded?._) ? decoded._ : decoded;
			const thread = isPlainObject(session?.thread) ? session.thread : null;
			const threadID = firstNormalizedThreadID(thread?.id, thread?.threadId, thread?.threadID, session?.threadId, session?.threadID);
			if (threadID && (storedLocalAPIKey() || localAPIKey())) {
				rememberLocalThreadID(threadID);
				rememberThreadSettings(threadID, { agentMode: "puck", reasoningEffort: "none" });
				patchRemotePuckThreadActorConfig(raw, threadID);
				envelope.data = JSON.stringify(raw);
				return rebuiltJSONResponse(response, envelope);
			} else if (threadID) {
				forgetLocalThreadID(threadID);
			}
		} catch {
		}
		return response;
	}

	function updateProjectChangesWorkflowRemotePath(path) {
		return svelteKitRemoteEndpoint(path) === "updateOwnedProjectChangesWorkflow";
	}

	function localProjectChangesWorkflowRequest(body) {
		const decoded = decodeRemoteCommandBody(body);
		if (!isPlainObject(decoded)) {
			return null;
		}
		const projectID = firstString(decoded.projectID, decoded.projectId, decoded.project_id);
		const changesWorkflow = normalizeChangesWorkflow(decoded.changesWorkflow);
		return projectID && changesWorkflow ? { projectID, changesWorkflow } : null;
	}

	function localProjectChangesWorkflowRemoteBody(body) {
		const request = localProjectChangesWorkflowRequest(body);
		return request && localProjectByID(localProjectsCache.projects, request.projectID) ? request : null;
	}

	function threadSearchAPIPath(path) {
		return path === "/api/threads/find";
	}

	function diffCaptureReadThreadID(path) {
		const match = String(path || "").match(/^\/api\/threads\/([^/?#]+)\/diff-captures\/(?:latest|diff|blob\/(?:[0-9a-fA-F]{40}|[0-9a-fA-F]{64}))\/?$/);
		if (!match) {
			return "";
		}
		return decodedThreadID(match[1]);
	}

	function activityDataPath(path) {
		return /^\/feed\/__data(?:\.json)?\/?$/.test(String(path || ""));
	}

	function rivetMetadataPath(path) {
		return path === "/metadata" || path === "/actors/metadata";
	}

	function threadPageDataPath(path) {
		const match = String(path || "").match(/^\/threads\/([^/?#]+)\/(?:view\/)?__data(?:\.json)?\/?$/);
		return !!match && !!decodedThreadID(match[1]);
	}

	function localThreadResourceDataThreadID(path) {
		const match = String(path || "").match(/^\/threads\/([^/?#]+)\/(?:view\/)?__data\/?$/);
		if (!match) {
			return "";
		}
		return decodedThreadID(match[1]);
	}

	function shouldPatchResponseJSON(response) {
		try {
			if (!response || !response.url) {
				return false;
			}
			const url = new URL(response.url, globalThis.location.href);
			if (url.pathname === localThreadDataEndpointPath) {
				return sameLocalHTTPBase(url, localBaseURL());
			}
			return threadPageDataPath(url.pathname) && url.origin === globalThis.location.origin;
		} catch {
			return false;
		}
	}

	function shouldPatchSidebarResponseJSON(response) {
		try {
			if (!response || !response.url) {
				return false;
			}
			const url = new URL(response.url, globalThis.location.href);
			return svelteKitRemoteEndpoint(url.pathname) === "listThreadListSidebar" && url.origin === globalThis.location.origin;
		} catch {
			return false;
		}
	}

	function shouldPatchThreadSearchResponseJSON(response) {
		try {
			if (!response || !response.url) {
				return false;
			}
			const url = new URL(response.url, globalThis.location.href);
			return svelteKitRemoteEndpoint(url.pathname) === "searchThreads" && url.origin === globalThis.location.origin;
		} catch {
			return false;
		}
	}

	function shouldPatchUsageResponseJSON(response) {
		try {
			if (!response || !response.url) {
				return false;
			}
			const url = new URL(response.url, globalThis.location.href);
			const endpoint = svelteKitRemoteEndpoint(url.pathname);
			return url.origin === globalThis.location.origin && (endpoint === "getPersonalCreditsUsage" || endpoint === "getPersonalDailySpendRows" || endpoint === "getPersonalThreadsUsageTable" || endpoint === "getPersonalTotalTokens");
		} catch {
			return false;
		}
	}

	function shouldPatchProjectListResponseJSON(response) {
		try {
			if (!response || !response.url) {
				return false;
			}
			const url = new URL(response.url, globalThis.location.href);
			return svelteKitRemoteEndpoint(url.pathname) === "listProjects" && url.origin === globalThis.location.origin;
		} catch {
			return false;
		}
	}

	function shouldPatchActivityResponseJSON(response) {
		try {
			if (!response || !response.url) {
				return false;
			}
			const url = new URL(response.url, globalThis.location.href);
			return activityDataPath(url.pathname) && url.origin === globalThis.location.origin;
		} catch {
			return false;
		}
	}

	function activityFilterRemoteEndpoint(response) {
		try {
			if (!response || !response.url) {
				return "";
			}
			const url = new URL(response.url, globalThis.location.href);
			const endpoint = svelteKitRemoteEndpoint(url.pathname);
			return url.origin === globalThis.location.origin && (endpoint === "searchFeedRepositories" || endpoint === "searchFeedUsers") ? endpoint : "";
		} catch {
			return "";
		}
	}

	function shouldCaptureAuthenticatedAmpUserID(response) {
		try {
			if (!response || !response.url) {
				return false;
			}
			const url = new URL(response.url, globalThis.location.href);
			return url.origin === globalThis.location.origin && /\/__data(?:\.json)?\/?$/.test(url.pathname);
		} catch {
			return false;
		}
	}

	function shouldBridgeHTTP(url) {
		const localThreadDataID = localThreadResourceDataThreadID(url.pathname);
		const diffCaptureThreadID = diffCaptureReadThreadID(url.pathname);
		if (!threadActorAPIPath(url.pathname) && !internalAPIPath(url.pathname) && !threadSearchAPIPath(url.pathname) && !svelteKitRemotePath(url.pathname) && !rivetMetadataPath(url.pathname) && !gatewayUserActorActionPath(url.pathname) && !localThreadDataID && !diffCaptureThreadID) {
			return false;
		}
		if ((localThreadDataID && !rememberedLocalThreadID(localThreadDataID)) || (diffCaptureThreadID && !rememberedLocalThreadID(diffCaptureThreadID))) {
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
		return (url.origin === globalThis.location.origin || sameLocalHTTPBase(url, localBaseURL())) &&
			(threadActorAPIPath(url.pathname) || internalAPIPath(url.pathname) || threadSearchAPIPath(url.pathname) || svelteKitRemotePath(url.pathname) || rivetMetadataPath(url.pathname) || gatewayUserActorActionPath(url.pathname) || rememberedLocalThreadID(localThreadResourceDataThreadID(url.pathname)) || rememberedLocalThreadID(diffCaptureReadThreadID(url.pathname)));
	}

	function internalAPIPath(path) {
		return path === "/api/internal";
	}

	function samePageWebSocketBase(url) {
		const page = new URL(globalThis.location.href);
		const protocol = page.protocol === "https:" ? "wss:" : "ws:";
		return url.origin === page.origin || (url.protocol === protocol && url.host === page.host);
	}

	function shouldRewriteWebSocket(url, base) {
		return (samePageWebSocketBase(url) || sameLocalWebSocketBase(url, base)) && shouldBridgeWebSocket(url);
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

	function gatewayUserActorActionPath(path) {
		const normalized = path.startsWith("/actors/gateway/") ? path.slice("/actors".length) : path;
		const parts = normalized.split("/").filter(Boolean);
		if (parts.length !== 4 || parts[0] !== "gateway" || parts[2] !== "action") {
			return false;
		}
		const target = parts[1].split("@")[0];
		return target === "userActor" || target === "user-actor" || localRunnerActionNames.has(parts[3]);
	}

	function pathThreadID() {
		const match = globalThis.location.pathname.match(/^\/threads\/([^/?#]+)/);
		return match ? decodedThreadID(match[1]) : "";
	}

	function activeThreadID() {
		const pathThread = pathThreadID();
		if (pathThread) {
			return pathThread;
		}
		return observedThreadID;
	}

	function activeLocalThreadID() {
		const threadID = activeThreadID();
		return rememberedLocalThreadID(threadID) ? threadID : "";
	}

	function activeLocalArchiveState(threadID) {
		const activeThread = activeThreadID();
		if (!localProjectsArchiveStateLoaded || !threadID || threadID !== activeThread ||
			(!rememberedLocalThreadID(threadID) && !archivedLocalSidebarThreadIDs.has(threadID))) {
			return undefined;
		}
		return archivedLocalSidebarThreadIDs.has(threadID);
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
		return !!threadID && (rememberedLocalThreadID(threadID) || pathThreadID() === threadID);
	}

	function shouldBridgeUserActorWebSocket(url) {
		if (!gatewayUserActorPath(url.pathname)) {
			return false;
		}
		return !!storedLocalAPIKey();
	}

	function localHTTPURL(url, body, createProjectName = "") {
		const base = localBaseURL();
		const localThreadDataID = localThreadResourceDataThreadID(url.pathname);
		if (localThreadDataID && rememberedLocalThreadID(localThreadDataID)) {
			const localThreadDataURL = new URL(localThreadDataEndpointPath, base);
			localThreadDataURL.searchParams.set("cliproxy-thread-id", localThreadDataID);
			return localThreadDataURL.href;
		}
		const local = new URL(url.pathname + url.search + url.hash, base);
		if (createProjectThreadRemotePath(url.pathname)) {
			const workingDirectory = remoteCreateProjectThreadWorkingDirectory(body, createProjectName);
			if (workingDirectory && !local.searchParams.has("cliproxy-working-directory")) {
				local.searchParams.set("cliproxy-working-directory", workingDirectory);
			}
			if (workingDirectory && workingDirectory === selectedLocalProjectWorkingDirectory()) {
				local.searchParams.set("cliproxy-local-project", "1");
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
		return visibleProjectNameFromDocument();
	}

	function visibleCreateProjectName(localOnly = false) {
		const dialogSelector = '[cmdk-root],[data-cmdk-root],[data-slot="dialog-content"],[role="dialog"]';
		for (const element of globalThis.document.querySelectorAll("button,[role='button'],[aria-haspopup],a")) {
			if (!element.closest(dialogSelector) || !elementVisible(element) || (localOnly && element.dataset?.cliproxyLocalProjectActivator !== "1")) {
				continue;
			}
			const label = firstString(element.dataset?.cliproxyLocalProjectLabel, localProjectActivatorProjectLabel(element), projectHeaderProjectLabel(element));
			if (label) {
				return label;
			}
		}
		return "";
	}

	function visibleCreateLocalProjectWorkingDirectory() {
		const selectedProject = selectedLocalProject();
		const homeDirectory = defaultLocalWorkingDirectory();
		if (!homeDirectory || !localProjectIsNoProject(selectedProject)) {
			return "";
		}
		const dialogSelector = '[cmdk-root],[data-cmdk-root],[data-slot="dialog-content"],[role="dialog"]';
		for (const element of globalThis.document.querySelectorAll("button,[role='button']")) {
			if (!element.closest(dialogSelector) || element.dataset?.cliproxyLocalProjectActivator !== "1" || !elementVisible(element)) {
				continue;
			}
			const workingDirectory = normalizeWorkingDirectory(element.dataset.cliproxyLocalProjectWorkingDirectory);
			if (workingDirectory === homeDirectory) {
				return workingDirectory;
			}
		}
		return "";
	}

	function visibleProjectNameFromDocument() {
		for (const element of globalThis.document.querySelectorAll("button,[role='button'],[aria-haspopup],a")) {
			if (element.closest('[cmdk-root],[data-cmdk-root],[data-slot="dialog-content"],[role="dialog"],article,[data-message-id],[data-message]')) {
				continue;
			}
			const label = localProjectActivatorProjectLabel(element) || projectHeaderProjectLabel(element);
			if (label) {
				return label;
			}
		}
		return "";
	}

	function visibleCreateThreadProjectName() {
		const createName = visibleCreateProjectName(false);
		const pageName = visibleProjectName();
		if (!createName) {
			return pageName;
		}
		const createDirectory = visibleLocalProjectWorkingDirectory(createName);
		if (createDirectory || !pageName || normalizeProjectPickerName(createName) === normalizeProjectPickerName(pageName)) {
			return createName;
		}
		return pageName;
	}

	function visibleCreateThreadLookupProjectName() {
		return visibleCreateProjectName(false) || visibleProjectName();
	}

	function unresolvedCreateThreadDialogProject() {
		const createName = visibleCreateProjectName(false);
		const pageName = visibleProjectName();
		const selectedProject = selectedLocalProject();
		if (localProjectIsNoProject(selectedProject)) {
			return false;
		}
		return !!createName && !!pageName &&
			normalizeProjectPickerName(createName) !== normalizeProjectPickerName(pageName) &&
			!visibleLocalProjectWorkingDirectory(createName);
	}

	function compactElementText(element) {
		if (!(element instanceof Element)) {
			return "";
		}
		return firstString(element.innerText, element.textContent, element.getAttribute?.("aria-label"), element.getAttribute?.("title")).replace(/\s+/g, " ").trim();
	}

	function projectHeaderProjectLabel(element) {
		if (!(element instanceof Element)) {
			return "";
		}
		const href = firstString(element.getAttribute?.("href"));
		if (!/^https?:\/\/github\.com\/[^/]+\/[^/]+\/tree\/.+/.test(href)) {
			return "";
		}
		const label = compactElementText(element);
		return label && projectHeaderContainer(element, label) ? label : "";
	}

	function projectHeaderContainer(element, label) {
		for (let current = element.parentElement, depth = 0; current && depth < 7; current = current.parentElement, depth += 1) {
			if (projectHeaderSearchBoundary(current)) {
				return null;
			}
			const text = compactElementText(current);
			const controls = Array.from(current.querySelectorAll?.("button,[role='button'],[aria-haspopup]") || []);
			const hasActions = text.includes("More Actions") || controls.some((candidate) => compactElementText(candidate) === "More Actions");
			if (text.includes(label) && hasActions) {
				return current;
			}
		}
		return null;
	}

	function projectHeaderSearchBoundary(element) {
		return element === globalThis.document.body ||
			element === globalThis.document.documentElement ||
			element.matches?.("main,[role='main'],article,section,[data-message-id],[data-message]");
	}

	function visibleProjectWorkingDirectory(name = visibleProjectName()) {
		if (!name) {
			return "";
		}
		const projectDirectory = visibleLocalProjectWorkingDirectory(name);
		if (projectDirectory) {
			return projectDirectory;
		}
		if (selectedLocalProjectWorkingDirectory() || storedLocalWorkingDirectory()) {
			return "";
		}
		const matches = new Set();
		for (const workingDirectory of Object.values(threadWorkingDirectories())) {
			const normalized = normalizeWorkingDirectory(workingDirectory);
			if (normalized && pathBaseName(normalized) === name) {
				matches.add(normalized);
			}
		}
		if (matches.size === 1) {
			return [...matches][0];
		}
		return "";
	}

	function visibleLocalProjectWorkingDirectory(name = visibleProjectName()) {
		if (!name) {
			return "";
		}
		const project = localProjectByVisibleName(localProjectsCache.projects, name);
		return normalizeWorkingDirectory(project?.workingDirectory);
	}

	function remoteCreateProjectThreadWorkingDirectory(body, createProjectName = "") {
		const decoded = decodeRemoteCommandBody(body);
		const fromMention = threadMentionWorkingDirectory(remoteContentText(isPlainObject(decoded) ? decoded.content : null));
		if (fromMention) {
			return fromMention;
		}
		const selectedProject = selectedLocalProject();
		const selectedDirectory = normalizeWorkingDirectory(selectedProject?.workingDirectory);
		const projectID = isPlainObject(decoded) ? firstString(decoded.projectID, decoded.projectId, decoded.project_id) : "";
		if (projectID) {
			const fromProjectID = normalizeWorkingDirectory(localProjectByID(localProjectsCache.projects, projectID)?.workingDirectory);
			if (fromProjectID) {
				return fromProjectID;
			}
			if (localProjectIsNoProject(selectedProject) && ["no project", "~"].includes(normalizeProjectPickerName(createProjectName))) {
				return selectedDirectory;
			}
			return "";
		}
		const fromCapturedProject = visibleProjectWorkingDirectory(createProjectName);
		if (fromCapturedProject) {
			return fromCapturedProject;
		}
		const fromVisibleLocalSelection = visibleCreateLocalProjectWorkingDirectory();
		if (fromVisibleLocalSelection) {
			return fromVisibleLocalSelection;
		}
		if (localProjectIsNoProject(selectedProject)) {
			return selectedDirectory;
		}
		if (unresolvedCreateThreadDialogProject()) {
			return "";
		}
		const fromVisibleProject = visibleProjectWorkingDirectory(createProjectName || visibleCreateThreadProjectName());
		if (fromVisibleProject) {
			return fromVisibleProject;
		}
		const fromSelectedLocalProject = selectedLocalProjectWorkingDirectory();
		if (fromSelectedLocalProject) {
			return fromSelectedLocalProject;
		}
		return localWorkingDirectory();
	}

	function ensureVisibleProjectLookupForRemoteCreate(sourceURL, projectName = "") {
		projectName = projectName || visibleCreateThreadLookupProjectName();
		if (!createProjectThreadRemotePath(sourceURL.pathname) || visibleProjectWorkingDirectory(projectName) || !projectName) {
			return Promise.resolve();
		}
		return fetchLocalProjects(false).then(() => undefined, () => undefined);
	}

	function rememberRemoteCreateProjectThread(sourceURL, body, response, createProjectName = "") {
		if (!createProjectThreadRemotePath(sourceURL.pathname)) {
			return Promise.resolve(response);
		}
		const workingDirectory = remoteCreateProjectThreadWorkingDirectory(body, createProjectName);
		if (!response || !response.ok || typeof response.clone !== "function") {
			return Promise.resolve(response);
		}
		const decodedRequest = decodeRemoteCommandBody(body);
		return response.clone().text().then((text) => {
			try {
				const envelope = originalJSONParse(text || "{}");
				const result = decodeDevalueString(envelope.data || "");
				const value = isPlainObject(result) ? result._ : null;
				if (isPlainObject(value) && value.ok === false) {
					return;
				}
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
					agentMode: firstString(value.agentMode, isPlainObject(decodedRequest) ? decodedRequest.agentMode : ""),
					reasoningEffort: firstString(value.reasoningEffort, isPlainObject(decodedRequest) ? decodedRequest.reasoningEffort : ""),
				});
				pendingLocalSidebarThreadID = threadID;
				const thread = normalizeLocalSidebarThread(isPlainObject(value.threadData) ? value.threadData.thread : null);
				localProjectsCache = {
					at: 0,
					projects: localProjectsCache.projects,
					threadID,
					thread,
					threads: normalizeLocalSidebarThreads([thread, ...(localProjectsCache.threads || [])], null),
					threadTitles: localProjectsCache.threadTitles,
					sidebarTitleKey: localProjectsCache.sidebarTitleKey,
					promise: null,
				};
			} catch {
			}
		}).then(() => response, () => response);
	}

	function invalidateLocalSidebarAfterThreadMutation(sourceURL, body, response) {
		if (localThreadMutationRemotePath(sourceURL.pathname) && response?.ok) {
			const endpoint = svelteKitRemoteEndpoint(sourceURL.pathname);
			const decoded = decodeRemoteCommandBody(body);
			const threadID = isPlainObject(decoded) ? firstString(decoded.threadID, decoded.threadId, decoded.id) : "";
			if (endpoint === "archiveThreadCommand") {
				if (validThreadID(threadID) && typeof decoded.archived === "boolean") {
					if (decoded.archived) {
						archivedLocalSidebarThreadIDs.add(threadID);
						localProjectsCache.threads = (localProjectsCache.threads || []).filter((thread) => firstString(thread?.id, thread?.threadId, thread?.threadID) !== threadID);
					} else {
						archivedLocalSidebarThreadIDs.delete(threadID);
					}
				}
			}
			if (endpoint === "pinThreadCommand" && validThreadID(threadID) && typeof decoded.pinned === "boolean") {
				rememberLocalPinnedOverride(threadID, decoded.pinned);
				for (const thread of [localProjectsCache.thread, ...(localProjectsCache.threads || [])]) {
					if (isPlainObject(thread) && firstString(thread.id, thread.threadId, thread.threadID) === threadID) {
						thread.pinned = decoded.pinned;
						thread[localPinnedOverrideField] = decoded.pinned;
					}
				}
			}
			if (endpoint === "markThreadUnreadCommand" && validThreadID(threadID)) {
				for (const thread of [localProjectsCache.thread, ...(localProjectsCache.threads || [])]) {
					if (isPlainObject(thread) && firstString(thread.id, thread.threadId, thread.threadID) === threadID) {
						thread.hasUnreadMessages = true;
						thread.unreadStatusUpdatedAt = new Date().toISOString();
					}
				}
			}
			if (endpoint === "deleteThreadCommand" && validThreadID(threadID)) {
				forgetLocalThreadID(threadID);
				forgetLocalSidebarTitle(threadID);
				archivedLocalSidebarThreadIDs.delete(threadID);
				localPinnedOverrides.delete(threadID);
				if (pendingLocalSidebarThreadID === threadID) {
					pendingLocalSidebarThreadID = "";
				}
				localProjectsCache.threads = (localProjectsCache.threads || []).filter((thread) => firstString(thread?.id, thread?.threadId, thread?.threadID) !== threadID);
			}
			localProjectsCacheGeneration += 1;
			localProjectsCache.at = 0;
		}
		return response;
	}

	function cloudThreadDeleteConfirmed(response) {
		if (!response?.ok) {
			return Promise.resolve(false);
		}
		return response.clone().json().then((envelope) => {
			const result = decodeDevalueString(envelope?.data || "")?._;
			return isPlainObject(result) && (result.ok === true || result.ok === false && firstString(result.error?.code).toLowerCase() === "thread-not-found");
		}).catch(() => false);
	}

	function mirrorLocalThreadMutation(sourceURL, input, init, localRequest) {
		if (sourceURL.origin !== globalThis.location.origin || !localThreadMutationRemotePath(sourceURL.pathname)) {
			return localRequest();
		}
		const cloudRequest = originalFetch(input, init);
		if (svelteKitRemoteEndpoint(sourceURL.pathname) === "deleteThreadCommand") {
			return cloudRequest.then((response) => cloudThreadDeleteConfirmed(response).then((confirmed) => confirmed ? localRequest() : response));
		}
		return Promise.all([localRequest(), cloudRequest.catch(() => null)]).then(([response]) => response);
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

	function requestUsesLocalBridge(request, init) {
		try {
			const headers = new Headers(typeof init?.headers !== "undefined" ? init.headers : request?.headers);
			return headers.get(bridgeHeader) === "1";
		} catch {
			return false;
		}
	}

	function normalizeLocalProject(project) {
		if (!isPlainObject(project)) {
			return null;
		}
		const workingDirectory = normalizeWorkingDirectory(firstString(project.workingDirectory, project.workspaceRoot, project.cwd));
		if (!workingDirectory) {
			return null;
		}
		const normalized = {
			id: firstString(project.id, project.projectID, project.projectId, project.project_id),
			name: firstString(project.name, project.projectName, pathBaseName(workingDirectory), "local"),
			namespace: firstString(project.namespace, project.projectNamespace, "local"),
			repositoryURL: firstString(project.repositoryURL, project.repoURL),
			additionalRepositories: Array.isArray(project.additionalRepositories) ? project.additionalRepositories.slice() : [],
			workingDirectory,
			localOnly: project.localOnly === true,
		};
		const changesWorkflow = normalizeChangesWorkflow(project.changesWorkflow);
		if (changesWorkflow) {
			normalized.changesWorkflow = changesWorkflow;
		}
		return normalized;
	}

	function normalizeChangesWorkflow(value) {
		return value === "merge-to-main" || value === "push-to-branch" ? value : "";
	}

	function localProjectChangesWorkflow(project) {
		if (!normalizeWorkingDirectory(project?.workingDirectory)) {
			return "";
		}
		return normalizeChangesWorkflow(project?.changesWorkflow) || "push-to-branch";
	}

	function remoteQueryInput(response) {
		try {
			const url = new URL(response?.url || "", globalThis.location.href);
			const payload = decodeBase64Text(url.searchParams.get("payload") || "");
			const decoded = payload ? decodeDevalueString(payload) : null;
			return isPlainObject(decoded) ? decoded : {};
		} catch {
			return {};
		}
	}

	function threadSearchRequestGraph(sourceURL) {
		try {
			const url = sourceURL instanceof URL ? new URL(sourceURL.href) : new URL(String(sourceURL || ""), globalThis.location.href);
			if (svelteKitRemoteEndpoint(url.pathname) !== "searchThreads" || url.origin !== globalThis.location.origin) {
				return null;
			}
			const payload = decodeBase64Text(url.searchParams.get("payload") || "");
			const values = payload ? originalJSONParse(payload) : null;
			if (!Array.isArray(values)) {
				return null;
			}
			const input = values.find((value) => isPlainObject(value) && Object.prototype.hasOwnProperty.call(value, "query"));
			if (!input) {
				return null;
			}
			return { input, url, values };
		} catch {
			return null;
		}
	}

	function threadSearchRequestContext(sourceURL) {
		const graph = threadSearchRequestGraph(sourceURL);
		if (!graph) {
			return null;
		}
		const field = (key) => {
			const ref = graph.input[key];
			return Number.isInteger(ref) && ref >= 0 && ref < graph.values.length ? graph.values[ref] : ref;
		};
		const query = typeof field("query") === "string" ? field("query").trim() : "";
		if (!query) {
			return null;
		}
		const requestedLimit = Number(field("limit"));
		const requestedOffset = Number(field("offset"));
		return {
			archived: typeof field("archived") === "boolean" ? field("archived") : undefined,
			limit: Number.isInteger(requestedLimit) && requestedLimit > 0 ? Math.min(requestedLimit, 75) : 20,
			offset: Number.isInteger(requestedOffset) && requestedOffset > 0 ? Math.min(requestedOffset, 10000) : 0,
			query,
		};
	}

	function threadSearchWindowURL(sourceURL, context) {
		const graph = threadSearchRequestGraph(sourceURL);
		if (!graph || !isPlainObject(context) || context.offset <= 0 || context.offset + context.limit > 75) {
			return graph?.url || sourceURL;
		}
		graph.values.push(0);
		graph.input.offset = graph.values.length - 1;
		graph.values.push(context.offset + context.limit);
		graph.input.limit = graph.values.length - 1;
		graph.url.searchParams.set("payload", encodeBase64URLText(JSON.stringify(graph.values)));
		return graph.url;
	}

	function localProjectRepositoryKey(project) {
		let repositoryURL = firstString(project?.repositoryURL, project?.repoURL).trim();
		if (!repositoryURL) {
			return "";
		}
		if (!repositoryURL.includes("://") && repositoryURL.includes(":")) {
			repositoryURL = repositoryURL.slice(repositoryURL.indexOf(":") + 1);
		} else {
			try {
				const parsed = new URL(repositoryURL);
				repositoryURL = parsed.protocol === "file:" ? parsed.href : parsed.host + parsed.pathname;
			} catch {
			}
		}
		return repositoryURL.replace(/^\/+|\/+$/g, "").replace(/\.git$/i, "").toLowerCase();
	}

	function ampProjectPathIdentity(namespace, name) {
		namespace = firstString(namespace).trim().toLowerCase();
		name = firstString(name).trim().toLowerCase();
		return namespace && name ? namespace + "\u0000" + name : "";
	}

	function ampProjectURLParts(rawURL) {
		try {
			const url = new URL(rawURL, globalThis.location.href);
			if (url.origin !== globalThis.location.origin) {
				return null;
			}
			const match = url.pathname.match(/^\/@([^/]+)\/([^/]+)(\/settings)?\/?$/);
			if (!match) {
				return null;
			}
			const namespace = decodeURIComponent(match[1]);
			const name = decodeURIComponent(match[2]);
			return {
				identity: ampProjectPathIdentity(namespace, name),
				name,
				namespace,
				settings: !!match[3],
			};
		} catch {
			return null;
		}
	}

	function devalueProjectRecord(values, ref) {
		const project = Number.isInteger(ref) ? values[ref] : null;
		if (!isPlainObject(project)) {
			return null;
		}
		const id = firstString(devalueFieldValue(values, project, "id"), devalueFieldValue(values, project, "projectID"));
		const name = firstString(devalueFieldValue(values, project, "name"));
		if (!id || !name) {
			return null;
		}
		return {
			id,
			name,
			namespace: firstString(devalueFieldValue(values, project, "namespace")),
			repositoryURL: firstString(devalueFieldValue(values, project, "repositoryURL"), devalueFieldValue(values, project, "repoURL")),
			local: devalueFieldValue(values, project, "cliProxyAPILocalProject") === true,
			localCheckout: devalueFieldValue(values, project, "cliProxyAPILocalCheckout") === true,
			project,
		};
	}

	function localProjectWebRecord(project, owner, webProjectID = "") {
		const repositoryURL = firstString(project.repositoryURL, project.repoURL);
		const changesWorkflow = localProjectChangesWorkflow(project);
		const projectID = firstString(webProjectID, project.id, project.projectID);
		return {
			id: projectID,
			projectID,
			name: firstString(project.name, pathBaseName(project.workingDirectory), "local"),
			namespace: firstString(project.namespace, "local"),
			repositoryURL,
			remoteURLs: repositoryURL ? [repositoryURL] : [],
			repositoryMode: "mapped",
			changesWorkflow,
			additionalRepositories: Array.isArray(project.additionalRepositories) ? project.additionalRepositories.slice() : [],
			workingDirectory: project.workingDirectory,
			owner: isPlainObject(owner) ? owner : { type: "user", userID: authenticatedAmpUserID },
			creatorUserID: firstString(owner?.userID, authenticatedAmpUserID, "local-user"),
			cliProxyAPILocalProject: true,
		};
	}

	function localProjectListWebID(project, existingIDs) {
		const projectID = firstString(project?.id, project?.projectID);
		if (projectID && !existingIDs.has(projectID)) {
			existingIDs.add(projectID);
			return projectID;
		}
		const seed = firstString(project?.workingDirectory, project?.repositoryURL, project?.namespace, project?.name);
		let hash = 2166136261;
		for (let i = 0; i < seed.length; i += 1) {
			hash ^= seed.charCodeAt(i);
			hash = Math.imul(hash, 16777619);
		}
		const base = projectID ? projectID + "-local-" + (hash >>> 0).toString(36) : "local-" + (hash >>> 0).toString(36);
		let webProjectID = base;
		let suffix = 2;
		while (existingIDs.has(webProjectID)) {
			webProjectID = base + "-" + suffix;
			suffix += 1;
		}
		existingIDs.add(webProjectID);
		localProjectWebIDs.set(webProjectID, project);
		return webProjectID;
	}

	function localProjectCheckoutIdentity(project) {
		const repositoryKey = localProjectRepositoryKey(project);
		const workingDirectory = firstWorkingDirectory(project?.workingDirectory, project?.workspaceRoot);
		if (repositoryKey && workingDirectory) {
			return "repository\u0000" + repositoryKey + "\u0000directory\u0000" + workingDirectory;
		}
		if (repositoryKey) {
			return "repository\u0000" + repositoryKey;
		}
		if (workingDirectory) {
			return "directory\u0000" + workingDirectory;
		}
		const projectID = firstString(project?.id, project?.projectID, project?.projectId, project?.project_id);
		return projectID ? "project\u0000" + projectID : "";
	}

	function localProjectForSidebarThread(thread) {
		const workingDirectory = plainThreadWorkingDirectory(thread);
		if (workingDirectory) {
			const checkout = localProjectByWorkingDirectory(localProjectsCache.projects, workingDirectory);
			if (checkout) {
				return checkout;
			}
		}
		const meta = isPlainObject(thread?.meta) ? thread.meta : {};
		const repositoryKey = localProjectRepositoryKey(thread) || localProjectRepositoryKey(meta);
		if (repositoryKey) {
			const repositoryProject = localProjectsCache.projects.find((project) => localProjectRepositoryKey(project) === repositoryKey);
			if (repositoryProject) {
				return repositoryProject;
			}
		}
		const projectID = firstString(thread?.projectID, thread?.projectId, thread?.project_id, meta.projectID, meta.projectId, meta.project_id);
		if (projectID) {
			const project = localProjectByID(localProjectsCache.projects, projectID);
			if (project) {
				return project;
			}
		}
		return localProjectByID(localProjectsCache.projects, projectID);
	}

	function localSidebarProjectMatches(record, project) {
		const recordRepository = localProjectRepositoryKey(record);
		const projectRepository = localProjectRepositoryKey(project);
		const recordDirectory = firstWorkingDirectory(record?.workingDirectory, record?.workspaceRoot);
		const projectDirectory = firstWorkingDirectory(project?.workingDirectory, project?.workspaceRoot);
		if (recordDirectory && projectDirectory) {
			if (recordDirectory !== projectDirectory) {
				return false;
			}
			return !recordRepository || !projectRepository || recordRepository === projectRepository;
		}
		if (recordRepository || projectRepository) {
			return !!recordRepository && recordRepository === projectRepository;
		}
		if (recordDirectory || projectDirectory) {
			return !!recordDirectory && recordDirectory === projectDirectory;
		}
		return firstString(record?.id, record?.projectID) === firstString(project?.id, project?.projectID);
	}

	function localProjectListRank(project) {
		const workingDirectory = normalizeWorkingDirectory(project?.workingDirectory);
		const name = firstString(project?.name).toLowerCase();
		const basenamePenalty = workingDirectory && pathBaseName(workingDirectory).toLowerCase() === name ? 0 : 20;
		return localProjectDirectoryRank(workingDirectory) * 100 + basenamePenalty + workingDirectory.split("/").filter(Boolean).length;
	}

	function localProjectListEntries(projects) {
		const byIdentity = new Map();
		for (const project of projects || []) {
			const identity = localProjectCheckoutIdentity(project);
			if (!identity) {
				continue;
			}
			const existing = byIdentity.get(identity);
			if (!existing || localProjectListRank(project) < localProjectListRank(existing)) {
				byIdentity.set(identity, project);
			}
		}
		return [...byIdentity.values()];
	}

	function mergeProjectListResponse(parsed, response) {
		if (!isPlainObject(parsed) || typeof parsed.data !== "string" || !Array.isArray(localProjectsCache.projects)) {
			return false;
		}
		const input = remoteQueryInput(response);
		const owner = isPlainObject(input.owner) ? input.owner : null;
		if (owner?.type && owner.type !== "user" || Number(input.offset || 0) > 0) {
			return false;
		}
		try {
			const values = originalJSONParse(parsed.data);
			let merged = 0;
			cloudProjectPaths.clear();
			localProjectCheckoutsByPath.clear();
			localProjectWebIDs.clear();
			const originalLength = values.length;
			for (let i = 0; i < originalLength; i += 1) {
				const container = values[i];
				const projectRefs = isPlainObject(container) && Number.isInteger(container.projects) ? values[container.projects] : null;
				if (!Array.isArray(projectRefs)) {
					continue;
				}
				const upstreamRefs = new Set(projectRefs);
				const existing = projectRefs.map((ref) => devalueProjectRecord(values, ref)).filter(Boolean);
				const upstreamProjects = existing.slice();
				const existingIDs = new Set(existing.map((project) => project.id));
				for (const project of existing) {
					const identity = ampProjectPathIdentity(project.namespace, project.name);
					if (!project.local && identity) cloudProjectPaths.add(identity);
				}
				for (const localProject of localProjectListEntries(localProjectsCache.projects)) {
					const localID = firstString(localProject?.id, localProject?.projectID);
					const repositoryKey = localProjectRepositoryKey(localProject);
					const localName = firstString(localProject.name).toLowerCase();
					const localNamespace = firstString(localProject.namespace).toLowerCase();
					const repositoryMatch = repositoryKey ? upstreamProjects.find((project) => localProjectRepositoryKey(project) === repositoryKey) : null;
					const pathMatch = localName && localNamespace ? upstreamProjects.find((project) => project.name.toLowerCase() === localName && project.namespace.toLowerCase() === localNamespace) : null;
					const pathRepositoryKey = pathMatch ? localProjectRepositoryKey(pathMatch) : "";
					const compatiblePathMatch = repositoryKey && pathRepositoryKey && repositoryKey !== pathRepositoryKey ? null : pathMatch;
					const idMatch = upstreamProjects.find((project) => project.id === localID);
					const idRepositoryKey = idMatch ? localProjectRepositoryKey(idMatch) : "";
					const compatibleIDMatch = idMatch && (!repositoryKey || !idRepositoryKey || repositoryKey === idRepositoryKey) ? idMatch : null;
					const matched = repositoryMatch || compatiblePathMatch || compatibleIDMatch;
					if (matched) {
						const identity = ampProjectPathIdentity(matched.namespace, matched.name);
						if (identity) localProjectCheckoutsByPath.set(identity, localProject);
						if (localProject.workingDirectory && !Number.isInteger(matched.project.workingDirectory)) {
							matched.project.workingDirectory = appendDevalueSidebarValue(values, localProject.workingDirectory, "workingDirectory");
							merged += 1;
						}
						if (devalueFieldValue(values, matched.project, "cliProxyAPILocalCheckout") !== true) {
							matched.project.cliProxyAPILocalCheckout = appendDevalueSidebarValue(values, true, "cliProxyAPILocalCheckout");
							merged += 1;
						}
						continue;
					}
					const webProjectID = localProjectListWebID(localProject, existingIDs);
					const addedRef = appendDevalueSidebarValue(values, localProjectWebRecord(localProject, owner, webProjectID));
					projectRefs.push(addedRef);
					const addedProject = devalueProjectRecord(values, addedRef);
					existing.push(addedProject);
					const identity = ampProjectPathIdentity(addedProject?.namespace, addedProject?.name);
					if (identity) localProjectCheckoutsByPath.set(identity, localProject);
					merged += 1;
				}
				const uniqueRefs = [];
				const refByIdentity = new Map();
				for (const ref of projectRefs) {
					const project = devalueProjectRecord(values, ref);
					const projectPath = project ? project.namespace.toLowerCase() + "\u0000" + project.name.toLowerCase() : "";
					const identity = project?.local && (!projectPath || !refByIdentity.has(projectPath)) ? "local\u0000" + project.id : projectPath;
					const priorRef = identity ? refByIdentity.get(identity) : undefined;
					if (typeof priorRef === "undefined") {
						uniqueRefs.push(ref);
						if (identity) refByIdentity.set(identity, ref);
						continue;
					}
					if (upstreamRefs.has(ref) && !upstreamRefs.has(priorRef)) {
						uniqueRefs[uniqueRefs.indexOf(priorRef)] = ref;
						refByIdentity.set(identity, ref);
					}
					merged += 1;
				}
				projectRefs.splice(0, projectRefs.length, ...uniqueRefs);
				projectRefs.sort((left, right) => {
					const leftProject = devalueProjectRecord(values, left);
					const rightProject = devalueProjectRecord(values, right);
					return firstString(leftProject?.name).localeCompare(firstString(rightProject?.name));
				});
			}
			if (merged === 0) {
				scheduleLocalProjectListDecoration();
				return false;
			}
			parsed.data = JSON.stringify(values);
			scheduleLocalProjectListDecoration();
			return true;
		} catch {
			return false;
		}
	}

	function localProjectForProjectRecord(project) {
		const projectID = firstString(project?.id, project?.projectID, project?.projectId, project?.project_id);
		const exact = localProjectByID(localProjectsCache.projects, projectID);
		if (exact) {
			return exact;
		}
		const checkout = localProjectByWorkingDirectory(localProjectsCache.projects, firstWorkingDirectory(project?.workingDirectory, project?.workspaceRoot));
		if (checkout) {
			return checkout;
		}
		const repositoryKey = localProjectRepositoryKey(project);
		if (!repositoryKey) {
			return null;
		}
		return localProjectsCache.projects.find((candidate) => localProjectRepositoryKey(candidate) === repositoryKey) || null;
	}

	function patchDevalueThreadProjects(values) {
		if (!Array.isArray(values)) {
			return false;
		}
		let patched = false;
		const originalLength = values.length;
		for (let i = 0; i < originalLength; i += 1) {
			const entry = values[i];
			if (!isPlainObject(entry)) {
				continue;
			}
			const thread = Number.isInteger(entry.thread) ? values[entry.thread] : null;
			let project = Number.isInteger(entry.project) ? values[entry.project] : null;
			if (!isPlainObject(project) && Object.prototype.hasOwnProperty.call(entry, "project") && isPlainObject(thread)) {
				const meta = devalueObjectField(values, thread, "meta") || {};
				const projectID = firstString(devalueStringField(values, thread, "projectID"), devalueStringField(values, meta, "projectID"));
				const localProject = localProjectByID(localProjectsCache.projects, projectID) ||
					localProjectByWorkingDirectory(localProjectsCache.projects, devalueThreadWorkingDirectory(values, thread));
				if (localProject) {
					entry.project = appendDevalueSidebarValue(values, localProjectWebRecord(localProject, null));
					project = values[entry.project];
					patched = true;
				}
			}
			if (!isPlainObject(project)) {
				continue;
			}
			const plainProject = {
				id: devalueFieldValue(values, project, "id"),
				projectID: devalueFieldValue(values, project, "projectID"),
				repositoryURL: devalueFieldValue(values, project, "repositoryURL"),
				workingDirectory: devalueFieldValue(values, project, "workingDirectory"),
			};
			const localProject = localProjectForProjectRecord(plainProject) ||
				(isPlainObject(thread) ? localProjectByWorkingDirectory(localProjectsCache.projects, devalueThreadWorkingDirectory(values, thread)) : null);
			const changesWorkflow = localProjectChangesWorkflow(localProject);
			if (changesWorkflow && devalueFieldValue(values, project, "changesWorkflow") !== changesWorkflow) {
				project.changesWorkflow = appendDevalueSidebarValue(values, changesWorkflow, "changesWorkflow");
				patched = true;
			}
		}
		return patched;
	}

	function patchPlainThreadProjects(value, seen = new WeakSet()) {
		if (value === null || typeof value !== "object" || seen.has(value)) {
			return false;
		}
		seen.add(value);
		let patched = false;
		if (isPlainObject(value)) {
			if (value.project === null && isPlainObject(value.thread)) {
				const meta = isPlainObject(value.thread.meta) ? value.thread.meta : {};
				const localProject = localProjectByID(localProjectsCache.projects, firstString(value.thread.projectID, meta.projectID)) ||
					localProjectByWorkingDirectory(localProjectsCache.projects, plainThreadWorkingDirectory(value.thread));
				if (localProject) {
					value.project = localProjectWebRecord(localProject, null);
					patched = true;
				}
			}
			if (isPlainObject(value.project)) {
				const localProject = localProjectForProjectRecord(value.project) ||
					(isPlainObject(value.thread) ? localProjectByWorkingDirectory(localProjectsCache.projects, plainThreadWorkingDirectory(value.thread)) : null);
				const changesWorkflow = localProjectChangesWorkflow(localProject);
				if (changesWorkflow && value.project.changesWorkflow !== changesWorkflow) {
					value.project.changesWorkflow = changesWorkflow;
					patched = true;
				}
			}
		}
		for (const child of Array.isArray(value) ? value : Object.values(value)) {
			patched = patchPlainThreadProjects(child, seen) || patched;
		}
		return patched;
	}

	function patchThreadProjectChangesWorkflow(parsed) {
		if (!isPlainObject(parsed)) {
			return false;
		}
		let patched = patchPlainThreadProjects(parsed);
		if (typeof parsed.data !== "string") {
			return patched;
		}
		try {
			const values = originalJSONParse(parsed.data);
			if (patchDevalueThreadProjects(values)) {
				parsed.data = JSON.stringify(values);
				patched = true;
			}
		} catch {
		}
		return patched;
	}

	function normalizeLocalSidebarThread(thread) {
		if (!isPlainObject(thread)) {
			return null;
		}
		const threadID = firstString(thread.id, thread.threadId, thread.threadID);
		if (!validThreadID(threadID)) {
			return null;
		}
		const normalized = Object.assign({}, thread, { id: threadID, threadId: threadID });
		if (typeof normalized[localPinnedOverrideField] === "boolean") {
			rememberLocalPinnedOverride(threadID, normalized[localPinnedOverrideField]);
		}
		delete normalized[localPinnedOverrideField];
		if (authenticatedAmpUserID) {
			normalized.creatorUserID = authenticatedAmpUserID;
			normalized.ownerUserId = authenticatedAmpUserID;
			normalized.creator = Object.assign({}, isPlainObject(thread.creator) ? thread.creator : {}, authenticatedAmpUser, { id: authenticatedAmpUserID });
		}
		return normalized;
	}

	function normalizeLocalSidebarThreads(threads, currentThread) {
		const normalized = [];
		const seen = new Set();
		const append = (thread) => {
			thread = normalizeLocalSidebarThread(thread);
			if (!thread || seen.has(thread.id)) {
				return;
			}
			seen.add(thread.id);
			normalized.push(thread);
		};
		if (Array.isArray(threads)) {
			for (const thread of threads) {
				append(thread);
			}
		}
		currentThread = normalizeLocalSidebarThread(currentThread);
		if (currentThread && !seen.has(currentThread.id)) {
			normalized.unshift(currentThread);
		}
		return normalized;
	}

	function localSidebarCachedThreadID(threadID) {
		return validThreadID(threadID) && Array.isArray(localProjectsCache.threads) && localProjectsCache.threads.some((thread) => isPlainObject(thread) && firstString(thread.id, thread.threadId, thread.threadID) === threadID);
	}

	function localSidebarCachedThreadMetadataComplete(threadID) {
		if (!validThreadID(threadID)) {
			return false;
		}
		const thread = [localProjectsCache.thread, ...(localProjectsCache.threads || [])].find((candidate) =>
			isPlainObject(candidate) && firstString(candidate.id, candidate.threadId, candidate.threadID) === threadID
		);
		if (!thread) {
			return false;
		}
		let title = firstString(thread.title);
		if (!title || title.toLowerCase() === "untitled") {
			title = firstString(localProjectsCache.threadTitles?.[threadID]);
		}
		if (!title || title.toLowerCase() === "untitled") {
			return false;
		}
		const meta = isPlainObject(thread.meta) ? thread.meta : {};
		const project = isPlainObject(thread.project) ? thread.project : {};
		const projectID = firstString(thread.projectID, thread.projectId, thread.project_id, meta.projectID, meta.projectId, meta.project_id, project.id, project.projectID, project.projectId);
		const projectAssociation = projectID || firstString(
			thread.projectName,
			thread.repositoryURL,
			thread.repoURL,
			thread.workingDirectory,
			thread.workspaceRoot,
			meta.projectName,
			meta.repositoryURL,
			meta.repoURL,
			meta.workingDirectory,
			meta.workspaceRoot,
			project.name,
			project.repositoryURL,
			project.repoURL,
			project.workingDirectory,
			project.workspaceRoot,
		);
		if (!projectAssociation) {
			return true;
		}
		if (projectID && localProjectByID(localProjectsCache.projects, projectID)) {
			return true;
		}
		const groupName = firstString(localSidebarRepositoryGroupName(thread)).toLowerCase();
		return !!groupName && groupName !== "no project" && groupName !== "~";
	}

	function localSidebarArchivedThreadID(threadID) {
		return validThreadID(threadID) && archivedLocalSidebarThreadIDs.has(threadID);
	}

	function rememberLocalProjectFetchFailure(reason) {
		diagnostics.localProjectFetchFailureCount += 1;
		diagnostics.lastLocalProjectFetchFailure = String(reason || "unknown").slice(0, 32);
	}

	function localProjectJSONRequest(url, headers) {
		if (typeof globalThis.XMLHttpRequest !== "function") {
			return originalFetch(url, {
				method: "GET",
				headers,
				mode: "cors",
				credentials: "omit",
			});
		}
		return new Promise((resolve, reject) => {
			const request = new globalThis.XMLHttpRequest();
			request.open("GET", url, true);
			for (const [name, value] of headers.entries()) request.setRequestHeader(name, value);
			request.onload = () => resolve({
				ok: request.status >= 200 && request.status < 300,
				status: request.status,
				json: () => Promise.resolve(originalJSONParse(request.responseText || "null")),
			});
			request.onerror = () => reject(new TypeError("local project request failed"));
			request.onabort = () => reject(new DOMException("local project request aborted", "AbortError"));
			request.send();
		});
	}

	function fetchLocalProjects(promptForKey = false, additionalSidebarThreadIDs = []) {
		reconcileActiveLocalThreadArchiveBadge();
		const now = Date.now();
		const activeThread = activeLocalThreadID();
		let threadID = validThreadID(pendingLocalSidebarThreadID) ? pendingLocalSidebarThreadID : activeThread;
		const requestedSidebarTitleIDs = [];
		const seenRequestedSidebarTitleIDs = new Set();
		for (const sidebarThreadID of [...(Array.isArray(additionalSidebarThreadIDs) ? additionalSidebarThreadIDs : []), ...localSidebarVisibleThreadIDs()]) {
			if (!validThreadID(sidebarThreadID) || seenRequestedSidebarTitleIDs.has(sidebarThreadID)) {
				continue;
			}
			seenRequestedSidebarTitleIDs.add(sidebarThreadID);
			requestedSidebarTitleIDs.push(sidebarThreadID);
			if (requestedSidebarTitleIDs.length >= 75) {
				break;
			}
		}
		if (activeThread && activeThread === pendingLocalSidebarThreadID) {
			pendingLocalSidebarThreadID = "";
			threadID = activeThread;
		}
		const coveredSidebarTitleIDs = new Set();
		if (localProjectsCache.threadID === threadID) {
			for (const sidebarThreadID of String(localProjectsCache.sidebarTitleKey || "").split("\u0000")) {
				if (validThreadID(sidebarThreadID)) {
					coveredSidebarTitleIDs.add(sidebarThreadID);
				}
			}
		}
		const sidebarTitleIDs = requestedSidebarTitleIDs.slice();
		const sidebarTitleIDSet = new Set(sidebarTitleIDs);
		for (const sidebarThreadID of Array.from(coveredSidebarTitleIDs).sort()) {
			if (sidebarTitleIDs.length >= 75) {
				break;
			}
			if (!sidebarTitleIDSet.has(sidebarThreadID)) {
				sidebarTitleIDSet.add(sidebarThreadID);
				sidebarTitleIDs.push(sidebarThreadID);
			}
		}
		sidebarTitleIDs.sort();
		const sidebarTitleKey = sidebarTitleIDs.join("\u0000");
		const additionalSidebarThreadIDSet = new Set((Array.isArray(additionalSidebarThreadIDs) ? additionalSidebarThreadIDs : []).filter(validThreadID));
		const additionalSidebarMetadataMissing = Array.from(additionalSidebarThreadIDSet).some((sidebarThreadID) =>
			!coveredSidebarTitleIDs.has(sidebarThreadID) &&
			!localSidebarCachedThreadMetadataComplete(sidebarThreadID) &&
			!archivedLocalSidebarThreadIDs.has(sidebarThreadID)
		);
		const visibleLocalSidebarMetadataMissing = requestedSidebarTitleIDs.some((sidebarThreadID) =>
			rememberedLocalThreadID(sidebarThreadID) &&
			!coveredSidebarTitleIDs.has(sidebarThreadID) &&
			!localSidebarCachedThreadMetadataComplete(sidebarThreadID) &&
			!archivedLocalSidebarThreadIDs.has(sidebarThreadID)
		);
		const sidebarMetadataMissing = additionalSidebarThreadIDSet.size > 0 ? additionalSidebarMetadataMissing : visibleLocalSidebarMetadataMissing;
		if (localProjectsCache.promise) {
			if (localProjectsCache.threadID === threadID && !sidebarMetadataMissing) {
				return localProjectsCache.promise;
			}
			return localProjectsCache.promise.then(() => fetchLocalProjects(promptForKey, additionalSidebarThreadIDs));
		}
		if (localProjectsCache.threadID === threadID && !sidebarMetadataMissing && now - localProjectsCache.at < 10000) {
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
		let responseFailed = false;
		const url = new URL(localBaseURLString() + localProjectsEndpointPath);
		if (threadID) {
			url.searchParams.set("cliproxy-thread-id", threadID);
		}
		for (const sidebarThreadID of sidebarTitleIDs) {
			url.searchParams.append("cliproxy-sidebar-thread-id", sidebarThreadID);
		}
		const generation = localProjectsCacheGeneration;
		let requestPromise;
		const refetchAfterInvalidation = () => {
			if (generation === localProjectsCacheGeneration) {
				return null;
			}
			if (localProjectsCache.promise === requestPromise) {
				localProjectsCache.promise = null;
				localProjectsCache.at = 0;
			}
			return fetchLocalProjects(promptForKey, additionalSidebarThreadIDs);
		};
		localProjectsCache.threadID = threadID;
		localProjectsCache.sidebarTitleKey = sidebarTitleKey;
		requestPromise = localProjectJSONRequest(url.href, headers).then((response) => {
			if (!response.ok) {
				responseFailed = true;
				rememberLocalProjectFetchFailure("http_" + String(response.status || 0));
				return null;
			}
			return response.json().catch(() => {
				responseFailed = true;
				rememberLocalProjectFetchFailure("invalid_json");
				return null;
			});
		}).then((decoded) => {
			const refetch = refetchAfterInvalidation();
			if (refetch) {
				return refetch;
			}
				if (!isPlainObject(decoded) || !Array.isArray(decoded.projects)) {
					if (!responseFailed) {
						rememberLocalProjectFetchFailure("invalid_payload");
					}
					resetLocalProjectsCache(threadID);
					localProjectsCache.at = Date.now();
					return [];
				}
			const responseDefaultWorkingDirectory = normalizeWorkingDirectory(firstString(decoded.defaultWorkingDirectory, decoded.homeDirectory));
			if (responseDefaultWorkingDirectory) {
				defaultWorkingDirectory = responseDefaultWorkingDirectory;
			}
				const projects = decoded.projects.map(normalizeLocalProject).filter(Boolean);
				const thread = normalizeLocalSidebarThread(decoded.thread);
				const threadTitles = {};
				if (isPlainObject(decoded.threadTitles)) {
					for (const [threadID, title] of Object.entries(decoded.threadTitles)) {
						if (validThreadID(threadID) && typeof title === "string" && title.trim() && title.trim().toLowerCase() !== "untitled") {
							threadTitles[threadID] = title.trim();
						}
					}
				}
				archivedLocalSidebarThreadIDs = new Set(
					(Array.isArray(decoded.archivedThreadIDs) ? decoded.archivedThreadIDs : []).filter(validThreadID),
				);
				localProjectsArchiveStateLoaded = true;
			localOrbsEnabled = decoded.orbsEnabled === true;
			const threads = normalizeLocalSidebarThreads(decoded.threads, thread);
			localProjectsCache = {
				at: Date.now(),
				projects,
				threadID,
				thread,
				threads,
				threadTitles: rememberLocalSidebarTitles(threadTitles, threads),
				sidebarTitleKey,
				promise: null,
			};
			reconcileActiveLocalThreadArchiveBadge();
			renderLocalSidebarMetadata();
			return projects;
			}).catch(() => {
				const refetch = refetchAfterInvalidation();
				if (refetch) {
					return refetch;
				}
				rememberLocalProjectFetchFailure("network_error");
				resetLocalProjectsCache(threadID);
				return [];
			});
		localProjectsCache.promise = requestPromise;
		return requestPromise;
	}

	function localActivitySourceURL(sourceURL) {
		try {
			const source = sourceURL instanceof URL ? sourceURL : new URL(String(sourceURL || globalThis.location.href), globalThis.location.href);
			return source.pathname === "/feed" || source.pathname.startsWith("/feed/") ? source : new URL(globalThis.location.href);
		} catch {
			return new URL(globalThis.location.href);
		}
	}

	function fetchLocalActivity(sourceURL = globalThis.location.href) {
		const source = localActivitySourceURL(sourceURL);
		const key = source.searchParams.toString() + "\u0000" + JSON.stringify([authenticatedAmpUserID, authenticatedAmpUser]);
		const now = Date.now();
		if (localActivityCache.promise && localActivityCache.key === key) {
			return localActivityCache.promise;
		}
		if (localActivityCache.value && localActivityCache.key === key && now - localActivityCache.at < 5000) {
			return Promise.resolve(localActivityCache.value);
		}
		const headers = localFetchHeaders("", false);
		if (!headers.get("Authorization")) {
			return Promise.resolve(null);
		}
		const url = new URL(localBaseURLString() + localActivityEndpointPath);
		for (const key of ["repo", "user", "time", "status", "q", "offset", "repoPinned", "userPinned"]) {
			for (const value of source.searchParams.getAll(key)) {
				url.searchParams.append(key, value);
			}
		}
		diagnostics.localActivityFetchCount += 1;
		const requestPromise = originalFetch(url.href, {
			method: "GET",
			headers,
			mode: "cors",
			credentials: "omit",
		}).then((response) => response.ok ? response.json() : null).then((decoded) => {
			if (!isPlainObject(decoded) || !Array.isArray(decoded.threads)) {
				return null;
			}
			const value = Object.assign({}, decoded, {
				threads: normalizeLocalSidebarThreads(decoded.threads, null),
				repositories: Array.isArray(decoded.repositories) ? decoded.repositories.filter(isPlainObject) : [],
				users: Array.isArray(decoded.users) ? decoded.users.filter(isPlainObject) : [],
				usersMap: isPlainObject(decoded.usersMap) ? decoded.usersMap : {},
			});
			if (localActivityCache.key === key) {
				localActivityCache = { at: Date.now(), key, value, promise: null };
			}
			return value;
		}).catch(() => null).finally(() => {
			if (localActivityCache.promise === requestPromise) {
				localActivityCache.promise = null;
			}
		});
		localActivityCache = { at: 0, key, value: null, promise: requestPromise };
		return requestPromise;
	}

	function fetchLocalThreadSearch(context) {
		if (!isPlainObject(context) || !context.query || !storedLocalAPIKey()) {
			return Promise.resolve(null);
		}
		const headers = localFetchHeaders("", false);
		if (!headers.get("Authorization")) {
			return Promise.resolve(null);
		}
		const url = new URL(localBaseURLString() + localThreadSearchEndpointPath);
		url.searchParams.set("q", context.query);
		url.searchParams.set("offset", "0");
		url.searchParams.set("limit", String(Math.min(75, context.offset + context.limit)));
		diagnostics.localThreadSearchFetchCount += 1;
		return localProjectJSONRequest(url.href, headers).then((response) => response.ok ? response.json() : null).then((decoded) => {
			if (!isPlainObject(decoded) || !Array.isArray(decoded.threads)) {
				return null;
			}
			const threads = decoded.threads.filter((thread) => isPlainObject(thread) && (context.archived === true ? thread.archived === true : context.archived === false ? thread.archived !== true : true));
			return { hasMore: decoded.hasMore === true, threads };
		}).catch(() => null);
	}

	function devalueFieldValue(values, entry, key) {
		return isPlainObject(entry) && Number.isInteger(entry[key]) ? values[entry[key]] : undefined;
	}

	function mergeDevalueActivityOptions(values, listRef, options, keyField) {
		const list = Number.isInteger(listRef) ? values[listRef] : null;
		if (!Array.isArray(list) || !Array.isArray(options)) {
			return 0;
		}
		let merged = 0;
		for (const option of options) {
			if (!isPlainObject(option)) {
				continue;
			}
			const optionKey = firstString(option[keyField]);
			if (!optionKey) {
				continue;
			}
			const existingRef = list.find((ref) => firstString(devalueFieldValue(values, Number.isInteger(ref) ? values[ref] : null, keyField)) === optionKey);
			if (Number.isInteger(existingRef) && isPlainObject(values[existingRef])) {
				const existing = values[existingRef];
				for (const [key, value] of Object.entries(option)) {
					if (key === "count") {
						const current = Number(devalueFieldValue(values, existing, key) || 0);
						existing[key] = appendDevalueSidebarValue(values, current + Number(value || 0), key);
					} else if (!Number.isInteger(existing[key])) {
						existing[key] = appendDevalueSidebarValue(values, value, key);
					}
				}
			} else {
				list.push(appendDevalueSidebarValue(values, option));
			}
			merged += 1;
		}
		return merged;
	}

	function mergeDevalueActivityData(values, activity) {
		if (!Array.isArray(values) || !isPlainObject(activity)) {
			return 0;
		}
		const originalLength = values.length;
		let merged = 0;
		for (let i = 0; i < originalLength; i += 1) {
			const entry = values[i];
			if (!isPlainObject(entry) || !Number.isInteger(entry.threads) || !Array.isArray(values[entry.threads])) {
				continue;
			}
			const threadRefs = values[entry.threads];
			for (const thread of activity.threads) {
				const threadID = firstString(thread?.id, thread?.threadId, thread?.threadID);
				if (!validThreadID(threadID)) {
					continue;
				}
				const existingRef = threadRefs.find((ref) => devalueSidebarThreadID(values, ref) === threadID);
				if (!Number.isInteger(existingRef) || !isPlainObject(values[existingRef])) {
					continue;
				}
				for (const [key, value] of Object.entries(thread)) {
					if (typeof value !== "undefined") {
						values[existingRef][key] = appendDevalueSidebarValue(values, value, key);
					}
				}
				merged += 1;
			}
			const usersMap = Number.isInteger(entry.usersMap) ? values[entry.usersMap] : null;
			if (isPlainObject(usersMap) && isPlainObject(activity.usersMap)) {
				for (const [userID, user] of Object.entries(activity.usersMap)) {
					if (!Number.isInteger(usersMap[userID])) {
						usersMap[userID] = appendDevalueSidebarValue(values, user);
					}
				}
			}
		}
		return merged;
	}

	function mergeActivityResponse(parsed, activity) {
		if (!isPlainObject(parsed) || !isPlainObject(activity)) {
			return false;
		}
		const dataSets = [];
		if (Array.isArray(parsed.data)) {
			dataSets.push(parsed.data);
		}
		if (Array.isArray(parsed.nodes)) {
			for (const node of parsed.nodes) {
				if (isPlainObject(node) && Array.isArray(node.data)) {
					dataSets.push(node.data);
				}
			}
		}
		let merged = 0;
		for (const values of dataSets) {
			merged += mergeDevalueActivityData(values, activity);
		}
		if (merged > 0) {
			diagnostics.localActivityMergeCount += merged;
			return true;
		}
		return false;
	}

	function activityFilterSearchQuery(sourceURL) {
		try {
			return new URL(String(sourceURL || ""), globalThis.location.href).searchParams.get("q")?.trim().toLowerCase() || "";
		} catch {
			return "";
		}
	}

	function rememberActivityFilterResponse(response, endpoint, body) {
		const context = decodeRemoteCommandBody(body);
		if (response && isPlainObject(context)) {
			activityFilterResponseContexts.set(response, { ...context, endpoint });
		}
		return response;
	}

	function activityFilterSourceURL(response, context) {
		if (!isPlainObject(context)) {
			return response?.url || globalThis.location.href;
		}
		const source = new URL(globalThis.location.href);
		source.pathname = "/feed";
		for (const [sourceKey, targetKey] of [["time", "time"], ["repo", "repo"], ["userID", "user"]]) {
			if (typeof context[sourceKey] !== "string") {
				continue;
			}
			const value = context[sourceKey].trim();
			if (value) {
				source.searchParams.set(targetKey, value);
			} else {
				source.searchParams.delete(targetKey);
			}
		}
		return source.href;
	}

	function mergeActivityFilterResponse(parsed, endpoint, activity, query) {
		if (!isPlainObject(parsed) || !isPlainObject(activity)) {
			return false;
		}
		const source = endpoint === "searchFeedRepositories" ? activity.repositories : activity.users;
		const options = source.filter((option) => !query || Object.values(option).some((value) => typeof value === "string" && value.toLowerCase().includes(query)));
		if (options.length === 0) {
			return false;
		}
		try {
			if (typeof parsed.data === "string") {
				const values = originalJSONParse(parsed.data);
				const root = values[0];
				const listRef = isPlainObject(root) && Number.isInteger(root._) ? root._ : 0;
				if (Array.isArray(values[listRef])) {
					const merged = mergeDevalueActivityOptions(values, listRef, options, endpoint === "searchFeedRepositories" ? "key" : "id");
					if (merged > 0) {
						parsed.data = JSON.stringify(values);
						diagnostics.localActivityMergeCount += merged;
						return true;
					}
				}
			}
		} catch {
		}
		const values = [{ _: 1 }, []];
		mergeDevalueActivityOptions(values, 1, options, endpoint === "searchFeedRepositories" ? "key" : "id");
		parsed.data = JSON.stringify(values);
		diagnostics.localActivityMergeCount += options.length;
		return true;
	}

	const sidebarDateFields = new Set(["updatedAt", "firstSyncAt", "lastUserMessageAt", "createdAt", "userLastInteractedAt"]);

	function appendDevalueSidebarValue(values, value, key = "") {
		if (sidebarDateFields.has(key) && typeof value === "string") {
			const date = new Date(value);
			if (Number.isFinite(date.getTime())) {
				values.push(["Date", date.toISOString()]);
				return values.length - 1;
			}
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
		for (const [childKey, child] of Object.entries(value)) {
			if (typeof child !== "undefined") {
				object[childKey] = appendDevalueSidebarValue(values, child, childKey);
			}
		}
		values.push(object);
		return values.length - 1;
	}

	function devalueSidebarThreadID(values, ref) {
		const entry = Number.isInteger(ref) ? values[ref] : null;
		if (!isPlainObject(entry)) {
			return "";
		}
		const thread = Number.isInteger(entry.thread) && isPlainObject(values[entry.thread]) ? values[entry.thread] : entry;
		for (const field of ["id", "threadId", "threadID"]) {
			const value = Number.isInteger(thread[field]) ? values[thread[field]] : null;
			if (validThreadID(value)) {
				return value;
			}
		}
		return "";
	}

	function removeDevalueSidebarThreadIDs(values, excludedThreadIDs) {
		if (!Array.isArray(values) || excludedThreadIDs.size === 0) {
			return 0;
		}
		let removed = 0;
		for (const entry of values) {
			if (!isPlainObject(entry) || !Number.isInteger(entry.recentThreads)) {
				continue;
			}
			const recentThreads = values[entry.recentThreads];
			if (!Array.isArray(recentThreads)) {
				continue;
			}
			for (let i = recentThreads.length - 1; i >= 0; i -= 1) {
				if (excludedThreadIDs.has(devalueSidebarThreadID(values, recentThreads[i]))) {
					recentThreads.splice(i, 1);
					removed += 1;
				}
			}
		}
		return removed;
	}

	function localSidebarLastActivityTimestamp(source) {
		for (const value of [source.lastActivityTimestamp, source.lastUserMessageAt, source.updatedAt, source.createdAt, source.created]) {
			if (typeof value === "number" && Number.isFinite(value) && value > 0) {
				return value;
			}
			if (typeof value === "string" && value.trim()) {
				const timestamp = Date.parse(value);
				if (Number.isFinite(timestamp)) {
					return timestamp;
				}
			}
		}
		return 0;
	}

	function localSidebarProjectName(source) {
		const meta = isPlainObject(source?.meta) ? source.meta : {};
		const projectID = firstString(source?.projectID, source?.projectId, source?.project_id, meta.projectID, meta.projectId, meta.project_id);
		return firstString(
			source?.projectName,
			source?.project?.name,
			meta.projectName,
			localProjectByID(localProjectsCache.projects, projectID)?.name,
		) || null;
	}

	function localSidebarRepositoryShortName(source) {
		const meta = isPlainObject(source?.meta) ? source.meta : {};
		let repositoryURL = firstString(source?.repositoryURL, source?.repoURL, meta.repositoryURL, meta.repoURL);
		if (!repositoryURL || repositoryURL.toLowerCase().startsWith("file:")) {
			return null;
		}
		const scpSeparator = repositoryURL.indexOf(":");
		if (!repositoryURL.includes("://") && scpSeparator >= 0) {
			repositoryURL = repositoryURL.slice(scpSeparator + 1);
		} else {
			try {
				repositoryURL = new URL(repositoryURL).pathname;
			} catch {
				return null;
			}
		}
		return pathBaseName(repositoryURL.replace(/\/+$/, "").replace(/\.git$/i, "")) || null;
	}

	function localSidebarRepositoryGroupName(source) {
		return firstString(
			localSidebarRepositoryShortName(source),
			localSidebarProjectName(source),
			pathBaseName(plainThreadWorkingDirectory(source)),
			"No project",
		);
	}

	function localSidebarVerifiedGitProject(source) {
		const meta = isPlainObject(source?.meta) ? source.meta : {};
		const repositoryURL = firstString(source?.repositoryURL, source?.repoURL, meta.repositoryURL, meta.repoURL);
		const namespace = firstString(source?.namespace, source?.projectNamespace, meta.namespace, meta.projectNamespace);
		const projectName = localSidebarProjectName(source);
		if (!projectName || !repositoryURL || repositoryURL.toLowerCase().startsWith("file:") || namespace.toLowerCase() === "local") {
			return null;
		}
		return {
			projectName,
			namespace,
			repositoryURL,
			projectID: firstString(source?.projectID, source?.projectId, source?.project_id, meta.projectID, meta.projectId, meta.project_id),
		};
	}

	function devalueSidebarText(values, entry, key) {
		const value = entry?.[key];
		return Number.isInteger(value) ? firstString(values[value]) : firstString(value);
	}

	function mergeMissingDevalueObjectFields(values, targetRef, source) {
		const target = Number.isInteger(targetRef) ? values[targetRef] : null;
		if (!isPlainObject(target) || !isPlainObject(source)) {
			return;
		}
		for (const [key, value] of Object.entries(source)) {
			const existingRef = target[key];
			const existingValue = Number.isInteger(existingRef) ? values[existingRef] : existingRef;
			if ((existingValue === null || typeof existingValue === "undefined" || existingValue === "") && typeof value !== "undefined") {
				target[key] = appendDevalueSidebarValue(values, value, key);
			}
		}
	}

	const canonicalSidebarThreadFields = new Set([
		"created", "createdAt", "env", "meta", "project", "projectID", "projectId", "project_id",
		"projectName", "repositoryURL", "repoURL", "workspace", "workingDirectory", "workspaceRoot",
	]);

	// Fields safe to hand to Amp's sidebar derivation. Project/repository/path
	// identity is intentionally excluded: it feeds Amp's grouping resolver and
	// can keep mergeThreadData/sidebarDataForDisplay from ever settling.
	const sidebarPresentationThreadFields = [
		"title", "created", "createdAt", "updatedAt", "lastUserMessageAt", "userLastInteractedAt",
		"lastActivityTimestamp", "state", "agentState", "messageCount", "summaryStats", "labels",
		"unread", "archived", "pinned", "creator", "creatorUserID", "ownerUserId", "executorType",
	];

	function localSidebarPresentationThread(source, threadID, projectName, repositoryGroupName) {
		const thread = { id: threadID, threadId: threadID };
		for (const key of sidebarPresentationThreadFields) {
			if (typeof source[key] !== "undefined") {
				thread[key] = source[key];
			}
		}
		if (projectName) {
			thread.projectName = projectName;
		}
		if (repositoryGroupName && repositoryGroupName !== "No project") {
			thread.repositoryGroupName = repositoryGroupName;
		}
		thread.automation = isPlainObject(source.automation) ? source.automation : null;
		const sourceMeta = isPlainObject(source.meta) ? source.meta : {};
		const meta = {};
		const metaProjectID = firstString(source.projectID, sourceMeta.projectID, sourceMeta.projectId, sourceMeta.project_id);
		if (metaProjectID) {
			meta.projectID = metaProjectID;
			thread.projectID = metaProjectID;
		}
		if (projectName) {
			meta.projectName = projectName;
		}
		if (repositoryGroupName && repositoryGroupName !== "No project") {
			meta.repositoryGroupName = repositoryGroupName;
		}
		thread.meta = meta;
		return thread;
	}

	function mergeDevalueSidebarThread(values, source) {
		if (!Array.isArray(values) || !isPlainObject(source)) {
			return false;
		}
		const threadID = firstString(source.id, source.threadId, source.threadID);
		if (!validThreadID(threadID)) {
			return false;
		}
		const persistedPinnedOverride = typeof source[localPinnedOverrideField] === "boolean" ? source[localPinnedOverrideField] : undefined;
		const pinnedOverride = typeof persistedPinnedOverride === "boolean" ? persistedPinnedOverride : localPinnedOverrides.get(threadID);
		const originalLength = values.length;
		for (let i = 0; i < originalLength; i += 1) {
			const entry = values[i];
			if (!isPlainObject(entry) || !Number.isInteger(entry.recentThreads)) {
				continue;
			}
			const recentThreads = values[entry.recentThreads];
			if (!Array.isArray(recentThreads)) {
				continue;
			}
			const existingRef = recentThreads.find((ref) => devalueSidebarThreadID(values, ref) === threadID);
			if (Number.isInteger(existingRef)) {
				const existingEntry = values[existingRef];
				const threadRef = Number.isInteger(existingEntry.thread) && isPlainObject(values[existingEntry.thread]) ? existingEntry.thread : existingRef;
				const existingThread = values[threadRef];
				for (const [key, value] of Object.entries(source)) {
					if (typeof value === "undefined") {
						continue;
					}
					if (key === localPinnedOverrideField || key === "automation" || key === "title" && (typeof value !== "string" || !value.trim() || value.trim().toLowerCase() === "untitled")) {
						continue;
					}
					if (key === "pinned" && Object.prototype.hasOwnProperty.call(existingThread, key) && typeof pinnedOverride !== "boolean") {
						continue;
					}
					if (canonicalSidebarThreadFields.has(key) && Number.isInteger(existingThread[key])) {
						if (key === "meta") {
							mergeMissingDevalueObjectFields(values, existingThread[key], value);
						}
						continue;
					}
					existingThread[key] = appendDevalueSidebarValue(values, value, key);
				}
				const existingAutomation = Number.isInteger(existingThread.automation) ? values[existingThread.automation] : existingThread.automation;
				if (isPlainObject(source.automation)) {
					existingThread.automation = appendDevalueSidebarValue(values, source.automation, "automation");
				} else if (!isPlainObject(existingAutomation)) {
					existingThread.automation = appendDevalueSidebarValue(values, null, "automation");
				}
				if (typeof pinnedOverride === "boolean") {
					const existingPinned = Number.isInteger(existingThread.pinned) ? values[existingThread.pinned] : existingThread.pinned;
					existingThread.pinned = appendDevalueSidebarValue(values, pinnedOverride, "pinned");
					if (typeof persistedPinnedOverride !== "boolean" && existingPinned === pinnedOverride) {
						localPinnedOverrides.delete(threadID);
					}
				}
				const existingMeta = Number.isInteger(existingThread.meta) ? values[existingThread.meta] : null;
				const sourceProjectName = localSidebarProjectName(source);
				const sourceRepositoryGroupName = localSidebarRepositoryGroupName(source);
				const verifiedGitProject = localSidebarVerifiedGitProject(source);
				const existingMetaProjectName = devalueSidebarText(values, existingMeta, "projectName");
				if (isPlainObject(existingMeta) && sourceProjectName && (!existingMetaProjectName || existingMetaProjectName === "No project" || verifiedGitProject)) {
					existingMeta.projectName = appendDevalueSidebarValue(values, sourceProjectName, "projectName");
				}
				const existingThreadProjectName = devalueSidebarText(values, existingThread, "projectName");
				if (sourceProjectName && (!existingThreadProjectName || existingThreadProjectName === "No project" || verifiedGitProject)) {
					existingThread.projectName = appendDevalueSidebarValue(values, sourceProjectName, "projectName");
				}
				const existingThreadRepositoryGroupName = devalueSidebarText(values, existingThread, "repositoryGroupName");
				if (sourceRepositoryGroupName !== "No project" && (!existingThreadRepositoryGroupName || existingThreadRepositoryGroupName === "No project" || verifiedGitProject)) {
					existingThread.repositoryGroupName = appendDevalueSidebarValue(values, sourceRepositoryGroupName, "repositoryGroupName");
				}
				if (isPlainObject(existingMeta) && verifiedGitProject) {
					existingMeta.namespace = appendDevalueSidebarValue(values, verifiedGitProject.namespace, "namespace");
					existingMeta.repositoryURL = appendDevalueSidebarValue(values, verifiedGitProject.repositoryURL, "repositoryURL");
					if (verifiedGitProject.projectID) {
						existingMeta.projectID = appendDevalueSidebarValue(values, verifiedGitProject.projectID, "projectID");
					}
					existingThread.repositoryURL = appendDevalueSidebarValue(values, verifiedGitProject.repositoryURL, "repositoryURL");
					for (const key of ["env", "workspace", "workingDirectory", "workspaceRoot"]) {
						if (typeof source[key] !== "undefined") {
							existingThread[key] = appendDevalueSidebarValue(values, source[key], key);
						}
					}
					if (verifiedGitProject.projectID) {
						existingThread.projectID = appendDevalueSidebarValue(values, verifiedGitProject.projectID, "projectID");
					}
				}
				if (threadRef !== existingRef) {
					if (!Number.isInteger(existingEntry.lastActivityTimestamp)) {
						existingEntry.lastActivityTimestamp = appendDevalueSidebarValue(values, localSidebarLastActivityTimestamp(source), "lastActivityTimestamp");
					}
					const existingEntryProjectName = devalueSidebarText(values, existingEntry, "projectName");
					if (sourceProjectName && (!existingEntryProjectName || existingEntryProjectName === "No project" || verifiedGitProject)) {
						existingEntry.projectName = appendDevalueSidebarValue(values, sourceProjectName, "projectName");
					}
					const existingRepositoryGroupName = devalueSidebarText(values, existingEntry, "repositoryGroupName");
					if (sourceRepositoryGroupName !== "No project" && (!existingRepositoryGroupName || existingRepositoryGroupName === "No project" || verifiedGitProject)) {
						existingEntry.repositoryGroupName = appendDevalueSidebarValue(values, sourceRepositoryGroupName, "repositoryGroupName");
					}
				}
				return true;
			}
			const wrappedRows = recentThreads.some((ref) => {
				const row = Number.isInteger(ref) ? values[ref] : null;
				return isPlainObject(row) && Number.isInteger(row.thread) && isPlainObject(values[row.thread]);
			});
			const insertedProjectName = localSidebarProjectName(source);
			const insertedRepositoryGroupName = localSidebarRepositoryGroupName(source);
			const insertedThread = localSidebarPresentationThread(source, threadID, insertedProjectName, insertedRepositoryGroupName);
			if (typeof pinnedOverride === "boolean") {
				insertedThread.pinned = pinnedOverride;
			}
			const inserted = wrappedRows ? {
				thread: insertedThread,
				lastActivityTimestamp: localSidebarLastActivityTimestamp(source),
				projectName: insertedProjectName,
				repositoryGroupName: insertedRepositoryGroupName,
			} : insertedThread;
			recentThreads.unshift(appendDevalueSidebarValue(values, inserted));
			return true;
		}
		return false;
	}

	function mergeReferencedSidebarProjects(values, threads) {
		const referencedProjects = new Map();
		for (const thread of threads) {
			const project = localProjectForSidebarThread(thread);
			const identity = localProjectCheckoutIdentity(project);
			if (project && identity) {
				const referenced = referencedProjects.get(identity) || { project, threads: [] };
				referenced.threads.push(thread);
				referencedProjects.set(identity, referenced);
			}
		}
		if (referencedProjects.size === 0) {
			return 0;
		}
		let merged = 0;
		const originalLength = values.length;
		for (let i = 0; i < originalLength; i += 1) {
			const entry = values[i];
			if (!isPlainObject(entry) || !Number.isInteger(entry.projects)) {
				continue;
			}
			const projectRefs = values[entry.projects];
			if (!Array.isArray(projectRefs)) {
				continue;
			}
			const existingProjects = [];
			const existingProjectIDs = new Set();
			for (const ref of projectRefs) {
				const encodedProject = Number.isInteger(ref) ? values[ref] : null;
				if (!isPlainObject(encodedProject)) {
					continue;
				}
				const project = {
					id: devalueFieldValue(values, encodedProject, "id"),
					projectID: devalueFieldValue(values, encodedProject, "projectID"),
					repositoryURL: devalueFieldValue(values, encodedProject, "repositoryURL"),
					repoURL: devalueFieldValue(values, encodedProject, "repoURL"),
					workingDirectory: devalueFieldValue(values, encodedProject, "workingDirectory"),
					workspaceRoot: devalueFieldValue(values, encodedProject, "workspaceRoot"),
				};
				const projectID = firstString(project.id, project.projectID);
				if (projectID) {
					existingProjectIDs.add(projectID);
					existingProjects.push(project);
				}
			}
			for (const { project, threads: projectThreads } of referencedProjects.values()) {
				const existingProject = existingProjects.find((candidate) => localSidebarProjectMatches(candidate, project));
				let presentedProjectID = firstString(existingProject?.id, existingProject?.projectID);
				if (!presentedProjectID) {
					presentedProjectID = localProjectListWebID(project, existingProjectIDs);
					projectRefs.push(appendDevalueSidebarValue(values, localProjectWebRecord(project, null, presentedProjectID)));
					existingProjects.push({
						id: presentedProjectID,
						repositoryURL: project.repositoryURL,
						repoURL: project.repoURL,
						workingDirectory: project.workingDirectory,
					});
					merged += 1;
				}
				if (presentedProjectID !== firstString(project?.id, project?.projectID)) {
					localProjectWebIDs.set(presentedProjectID, project);
				}
				for (const thread of projectThreads) {
					thread.projectID = presentedProjectID;
					if (isPlainObject(thread.meta)) {
						thread.meta.projectID = presentedProjectID;
					}
				}
			}
		}
		return merged;
	}

	function sidebarResponseThreadIDs(parsed) {
		if (!isPlainObject(parsed) || typeof parsed.data !== "string") {
			return [];
		}
		try {
			const values = originalJSONParse(parsed.data);
			const threadIDs = new Set();
			for (const entry of values) {
				if (!isPlainObject(entry) || !Number.isInteger(entry.recentThreads)) {
					continue;
				}
				const recentThreads = values[entry.recentThreads];
				if (!Array.isArray(recentThreads)) {
					continue;
				}
				for (const ref of recentThreads) {
					const threadID = devalueSidebarThreadID(values, ref);
					if (validThreadID(threadID)) {
						threadIDs.add(threadID);
					}
				}
			}
			return Array.from(threadIDs).sort();
		} catch {
			return [];
		}
	}

	function mergeSidebarResponse(parsed) {
		if (!isPlainObject(parsed) || typeof parsed.data !== "string") {
			return false;
		}
		const threads = Array.isArray(localProjectsCache.threads) && localProjectsCache.threads.length > 0
			? localProjectsCache.threads
			: isPlainObject(localProjectsCache.thread) ? [localProjectsCache.thread] : [];
		const activeThreads = threads
			.filter((thread) => !archivedLocalSidebarThreadIDs.has(firstString(thread?.id, thread?.threadId, thread?.threadID)))
			.map((thread) => Object.assign({}, thread, isPlainObject(thread?.meta) ? { meta: Object.assign({}, thread.meta) } : {}));
		if (activeThreads.length === 0 && archivedLocalSidebarThreadIDs.size === 0) {
			return false;
		}
		try {
			const values = originalJSONParse(parsed.data);
			const mergedProjects = mergeReferencedSidebarProjects(values, activeThreads);
			const removed = removeDevalueSidebarThreadIDs(values, archivedLocalSidebarThreadIDs);
			let merged = 0;
			for (let i = activeThreads.length - 1; i >= 0; i -= 1) {
				if (mergeDevalueSidebarThread(values, activeThreads[i])) {
					merged += 1;
				}
			}
			if (mergedProjects === 0 && merged === 0 && removed === 0) {
				return false;
			}
			parsed.data = JSON.stringify(values);
			diagnostics.localSidebarProjectMergeCount += mergedProjects;
			diagnostics.localSidebarThreadMergeCount += merged;
			return true;
		} catch {
			return false;
		}
	}

	function localUsageThreadTitle(threadID) {
		const cachedTitle = firstString(localProjectsCache.threadTitles?.[threadID], localSidebarTitleCache[threadID]);
		if (cachedTitle && cachedTitle.toLowerCase() !== "untitled") {
			return cachedTitle;
		}
		for (const thread of localUsageThreads(threadID)) {
			const title = firstString(thread?.title);
			if (title && title.toLowerCase() !== "untitled") {
				return title;
			}
		}
		return "";
	}

	function localUsageThreads(threadID) {
		return [localProjectsCache.thread, ...(localProjectsCache.threads || [])].filter((thread) =>
			firstString(thread?.id, thread?.threadId, thread?.threadID) === threadID,
		);
	}

	function localUsageThreadProjectName(threadID) {
		for (const thread of localUsageThreads(threadID)) {
			const projectName = firstString(localSidebarProjectName(thread));
			if (projectName && projectName.toLowerCase() !== "no project" && projectName !== "~") {
				return projectName;
			}
		}
		return "";
	}

	function localThreadSearchResult(source) {
		if (!isPlainObject(source)) {
			return null;
		}
		const threadID = firstString(source.id, source.threadId, source.threadID);
		if (!validThreadID(threadID)) {
			return null;
		}
		const meta = isPlainObject(source.meta) ? Object.assign({}, source.meta) : {};
		const settings = normalizedThreadSettings(threadSettings()[threadID]);
		if (!firstString(meta.agentMode) && settings.agentMode) {
			meta.agentMode = settings.agentMode;
		}
		const summaryStats = isPlainObject(source.summaryStats) ? Object.assign({}, source.summaryStats) : {};
		if (!isPlainObject(summaryStats.diffStats)) {
			summaryStats.diffStats = {};
		}
		const creatorUserID = firstString(source.creatorUserID, source.ownerUserId, meta.creatorUserID, meta.ownerUserId, authenticatedAmpUserID);
		const creator = isPlainObject(source.creator) ? Object.assign({}, source.creator) :
			(isPlainObject(authenticatedAmpUser) ? Object.assign({}, authenticatedAmpUser) : {});
		if (!firstString(creator.id) && creatorUserID) {
			creator.id = creatorUserID;
		}
		const projectName = firstString(source.projectName, meta.projectName, localUsageThreadProjectName(threadID), "No Project");
		return Object.assign({}, source, {
			archived: source.archived === true,
			creator,
			creatorUserID,
			href: "/threads/" + threadID,
			id: threadID,
			meta,
			projectName,
			summaryStats,
			title: firstString(source.title, localUsageThreadTitle(threadID), "Untitled"),
		});
	}

	function devalueThreadSearchResults(values) {
		if (!Array.isArray(values)) {
			return [];
		}
		const results = [];
		for (const result of values) {
			if (!isPlainObject(result) || !Object.prototype.hasOwnProperty.call(result, "hasMore") || !Number.isInteger(result.threads)) {
				continue;
			}
			const threadRefs = values[result.threads];
			if (Array.isArray(threadRefs)) {
				results.push({ result, threadRefs });
			}
		}
		return results;
	}

	function devalueThreadSearchEntries(values) {
		const entries = [];
		const seenRefs = new Set();
		for (const { threadRefs } of devalueThreadSearchResults(values)) {
			for (const ref of threadRefs) {
				if (!Number.isInteger(ref) || seenRefs.has(ref) || !isPlainObject(values[ref])) {
					continue;
				}
				const threadID = devalueSidebarThreadID(values, ref);
				if (!validThreadID(threadID)) {
					continue;
				}
				seenRefs.add(ref);
				entries.push({ entry: values[ref], threadID });
			}
		}
		return entries;
	}

	function copyDevalueThreadSearchRef(sourceValues, sourceRef, targetValues, copied) {
		if (Number.isInteger(sourceRef) && sourceRef < 0) {
			return sourceRef;
		}
		if (!Number.isInteger(sourceRef) || sourceRef >= sourceValues.length) {
			return null;
		}
		if (copied.has(sourceRef)) {
			return copied.get(sourceRef);
		}
		const targetRef = targetValues.length;
		copied.set(sourceRef, targetRef);
		targetValues.push(null);
		const source = sourceValues[sourceRef];
		if (Array.isArray(source)) {
			const tag = source[0];
			if (tag === -7) {
				const target = source.slice();
				for (let i = 3; i < target.length; i += 2) {
					target[i] = copyDevalueThreadSearchRef(sourceValues, target[i], targetValues, copied);
				}
				targetValues[targetRef] = target;
			} else if (typeof tag !== "string") {
				targetValues[targetRef] = source.map((ref) => copyDevalueThreadSearchRef(sourceValues, ref, targetValues, copied));
			} else if (tag === "Set" || tag === "Map") {
				targetValues[targetRef] = [tag, ...source.slice(1).map((ref) => copyDevalueThreadSearchRef(sourceValues, ref, targetValues, copied))];
			} else if (tag === "Object") {
				targetValues[targetRef] = [tag, copyDevalueThreadSearchRef(sourceValues, source[1], targetValues, copied)];
			} else if (tag === "null") {
				const target = source.slice();
				for (let i = 2; i < target.length; i += 2) {
					target[i] = copyDevalueThreadSearchRef(sourceValues, target[i], targetValues, copied);
				}
				targetValues[targetRef] = target;
			} else if (/^(?:Int8|Uint8|Uint8Clamped|Int16|Uint16|Float16|Int32|Uint32|Float32|Float64|BigInt64|BigUint64)Array$/.test(tag) || tag === "DataView") {
				const target = source.slice();
				target[1] = copyDevalueThreadSearchRef(sourceValues, target[1], targetValues, copied);
				targetValues[targetRef] = target;
			} else if (["Date", "RegExp", "BigInt", "ArrayBuffer", "URL", "URLSearchParams"].includes(tag) || tag.startsWith("Temporal.")) {
				targetValues[targetRef] = source.slice();
			} else {
				targetValues[targetRef] = [tag, copyDevalueThreadSearchRef(sourceValues, source[1], targetValues, copied)];
			}
		} else if (isPlainObject(source)) {
			const target = {};
			targetValues[targetRef] = target;
			for (const [key, ref] of Object.entries(source)) {
				target[key] = copyDevalueThreadSearchRef(sourceValues, ref, targetValues, copied);
			}
		} else {
			targetValues[targetRef] = source;
		}
		return targetRef;
	}

	function copyThreadSearchWindow(targetValues, windowEnvelope) {
		if (!isPlainObject(windowEnvelope) || typeof windowEnvelope.data !== "string") {
			return null;
		}
		try {
			const sourceValues = originalJSONParse(windowEnvelope.data);
			const sourceWindow = devalueThreadSearchResults(sourceValues)[0];
			if (!sourceWindow) {
				return null;
			}
			const copied = new Map();
			return {
				hasMore: devalueFieldValue(sourceValues, sourceWindow.result, "hasMore") === true,
				threadRefs: sourceWindow.threadRefs.map((ref) => copyDevalueThreadSearchRef(sourceValues, ref, targetValues, copied)).filter(Number.isInteger),
			};
		} catch {
			return null;
		}
	}

	function mergeThreadSearchResponse(parsed, localSearch, context, cloudWindowEnvelope) {
		if (!isPlainObject(parsed) || typeof parsed.data !== "string" || !isPlainObject(context)) {
			return false;
		}
		try {
			const values = originalJSONParse(parsed.data);
			const localThreads = Array.isArray(localSearch?.threads) ? localSearch.threads.map(localThreadSearchResult).filter(Boolean) : [];
			const cloudWindow = context.offset > 0 ? copyThreadSearchWindow(values, cloudWindowEnvelope) : null;
			if (context.offset > 0 && !cloudWindow) {
				return false;
			}
			let merged = 0;
			let changed = false;
			for (const { result, threadRefs } of devalueThreadSearchResults(values)) {
				const combined = [];
				const seen = new Set();
				for (const thread of localThreads) {
					if (seen.has(thread.id)) {
						continue;
					}
					seen.add(thread.id);
					combined.push(appendDevalueSidebarValue(values, thread));
					merged += 1;
				}
				for (const ref of cloudWindow?.threadRefs || threadRefs) {
					const threadID = devalueSidebarThreadID(values, ref);
					if (!validThreadID(threadID) || seen.has(threadID)) {
						continue;
					}
					seen.add(threadID);
					combined.push(ref);
				}
				const start = Math.min(context.offset, combined.length);
				const end = Math.min(start + context.limit, combined.length);
				const upstreamHasMore = cloudWindow ? cloudWindow.hasMore : devalueFieldValue(values, result, "hasMore") === true;
				const hasMore = localSearch?.hasMore === true || upstreamHasMore || end < combined.length;
				threadRefs.splice(0, threadRefs.length, ...combined.slice(start, end));
				result.hasMore = appendDevalueSidebarValue(values, hasMore, "hasMore");
				changed = true;
			}
			if (!changed) {
				return false;
			}
			parsed.data = JSON.stringify(values);
			diagnostics.localThreadSearchMergeCount += merged;
			return true;
		} catch {
			return false;
		}
	}

	function threadSearchResponseMissingMetadataIDs(parsed) {
		if (!isPlainObject(parsed) || typeof parsed.data !== "string") {
			return [];
		}
		try {
			const values = originalJSONParse(parsed.data);
			return devalueThreadSearchEntries(values)
				.filter(({ entry }) => {
					const title = devalueFieldValue(values, entry, "title");
					const projectName = devalueFieldValue(values, entry, "projectName");
					return typeof title !== "string" || !title.trim() || title.trim().toLowerCase() === "untitled" ||
						typeof projectName !== "string" || !projectName.trim() || ["no project", "~"].includes(projectName.trim().toLowerCase());
				})
				.map(({ threadID }) => threadID);
		} catch {
			return [];
		}
	}

	function patchThreadSearchResponse(parsed) {
		if (!isPlainObject(parsed) || typeof parsed.data !== "string") {
			return false;
		}
		try {
			const values = originalJSONParse(parsed.data);
			let patchedTitles = 0;
			let patchedProjects = 0;
			for (const { entry, threadID } of devalueThreadSearchEntries(values)) {
				const currentTitle = devalueFieldValue(values, entry, "title");
				if (typeof currentTitle !== "string" || !currentTitle.trim() || currentTitle.trim().toLowerCase() === "untitled") {
					const title = localUsageThreadTitle(threadID);
					if (title) {
						entry.title = appendDevalueSidebarValue(values, title, "title");
						patchedTitles += 1;
					}
				}
				const currentProjectName = devalueFieldValue(values, entry, "projectName");
				if (typeof currentProjectName !== "string" || !currentProjectName.trim() || ["no project", "~"].includes(currentProjectName.trim().toLowerCase())) {
					const projectName = localUsageThreadProjectName(threadID);
					if (projectName) {
						entry.projectName = appendDevalueSidebarValue(values, projectName, "projectName");
						patchedProjects += 1;
					}
				}
			}
			if (patchedTitles === 0 && patchedProjects === 0) {
				return false;
			}
			parsed.data = JSON.stringify(values);
			diagnostics.localThreadSearchTitlePatchCount += patchedTitles;
			diagnostics.localThreadSearchProjectPatchCount += patchedProjects;
			return true;
		} catch {
			return false;
		}
	}

	function patchUsageResponse(parsed) {
		if (!isPlainObject(parsed) || typeof parsed.data !== "string") {
			return false;
		}
		try {
			const values = originalJSONParse(parsed.data);
			if (!Array.isArray(values)) {
				return false;
			}
			let patched = 0;
			const originalLength = values.length;
			for (let i = 0; i < originalLength; i += 1) {
				const entry = values[i];
				if (!isPlainObject(entry)) {
					continue;
				}
				const threadID = firstNormalizedThreadID(
					devalueFieldValue(values, entry, "threadID"),
					devalueFieldValue(values, entry, "threadId"),
					devalueFieldValue(values, entry, "id"),
				);
				const title = localUsageThreadTitle(threadID);
				if (!title || devalueFieldValue(values, entry, "title") === title) {
					continue;
				}
				entry.title = appendDevalueSidebarValue(values, title, "title");
				patched += 1;
			}
			if (patched === 0) {
				return false;
			}
			parsed.data = JSON.stringify(values);
			diagnostics.localUsageTitlePatchCount += patched;
			return true;
		} catch {
			return false;
		}
	}

	async function promptLocalThread() {
		const promptText = globalThis.prompt("Start local Amp thread");
		if (promptText === null) {
			return;
		}
		let workingDirectory = newLocalThreadWorkingDirectory();
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
		createLocalThread("", newLocalThreadWorkingDirectory(), modeOptions).catch((error) => {
			globalThis.alert("Local Amp thread failed: " + (error && error.message ? error.message : String(error)));
		});
	}

	function localProjectRepositoryURLForDirectory(workingDirectory) {
		const directory = normalizeWorkingDirectory(workingDirectory);
		if (!directory) {
			return "";
		}
		for (const project of localProjectsCache.projects || []) {
			const normalized = normalizeLocalProject(project);
			if (normalized && normalized.workingDirectory === directory) {
				return firstString(normalized.repositoryURL);
			}
		}
		return "";
	}

	function localThreadPayload(promptText, workingDirectory, modeOptions, executor) {
		const mode = localThreadModeOptions(modeOptions);
		const settings = {
			agentMode: mode.agentMode,
		};
		if (mode.reasoningEffort) {
			settings["reasoning.effort"] = mode.reasoningEffort;
		}
		const orbExecutor = executor === "orb";
		const payload = {
			agentMode: mode.agentMode,
			reasoningEffort: mode.reasoningEffort,
			executorType: orbExecutor ? "sandbox" : "local-client",
			usesThreadActors: true,
			settings,
			threadMeta: {
				ampcodeConnectorLocalNeo: true,
				cliProxyAPILocalNeo: true,
				ampcodeLocalRuntime: true,
				ampcodeConnectorMode: "local-neo",
				agentMode: mode.agentMode,
				reasoningEffort: mode.reasoningEffort,
				executorType: orbExecutor ? "sandbox" : "local-client",
			},
		};
		if (orbExecutor) {
			const repositoryURL = localProjectRepositoryURLForDirectory(workingDirectory);
			if (repositoryURL) {
				payload.repositoryURL = repositoryURL;
				payload.threadMeta.repositoryURL = repositoryURL;
			}
		}
		if (workingDirectory) {
			payload.workingDirectory = workingDirectory;
			payload.workspaceRoot = workingDirectory;
		}
		if (promptText) {
			payload.prompt = promptText;
		}
		return payload;
	}

	async function createLocalThread(promptText, workingDirectory, modeOptions, executor) {
		const headers = localFetchHeaders("application/json");
		workingDirectory = normalizeWorkingDirectory(workingDirectory) || await ensureDefaultWorkingDirectory(false);
		const payload = localThreadPayload(promptText, workingDirectory, modeOptions, executor);
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
		if (localPayload.workingDirectory) {
			shellPayload.workingDirectory = localPayload.workingDirectory;
			shellPayload.workspaceRoot = localPayload.workspaceRoot || localPayload.workingDirectory;
		}
		if (localPayload.repositoryURL) {
			shellPayload.repositoryURL = localPayload.repositoryURL;
		}
		const response = await originalFetch(localBaseURLString() + "/api/thread-actors", {
			method: "POST",
			headers: localFetchHeaders("application/json"),
			mode: "cors",
			credentials: "omit",
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
		const anchor = globalThis.document.createElement("a");
		anchor.href = "/threads/" + encodeURIComponent(threadID);
		anchor.hidden = true;
		globalThis.document.body.appendChild(anchor);
		anchor.click();
		anchor.remove();
	}

	function removeLocalThreadPicker() {
		const cleanup = globalThis.__cliproxyAmpLocalInferencePickerCleanup;
		if (typeof cleanup === "function") {
			cleanup();
		}
		globalThis.document.getElementById("cliproxy-amp-local-thread-picker")?.remove();
	}

	function showLocalThreadPicker(anchor, promptText = "", workingDirectory = newLocalThreadWorkingDirectory()) {
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
		const executorSelection = { value: localOrbsEnabled ? localThreadExecutorChoice() : "local" };
		if (localOrbsEnabled) {
			panel.appendChild(buildLocalExecutorToggle(executorSelection));
		}
		for (const choice of localThreadModeChoices()) {
			panel.appendChild(buildLocalThreadChoiceButton(choice, promptText, workingDirectory, executorSelection));
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

	function localThreadExecutorChoice() {
		try {
			return globalThis.localStorage.getItem("cliproxyapi.ampLocalInference.executor") === "orb" ? "orb" : "local";
		} catch {
			return "local";
		}
	}

	function buildLocalExecutorToggle(selection) {
		const row = globalThis.document.createElement("div");
		row.style.cssText = "display:flex;gap:4px;padding:2px 2px 8px;margin-bottom:4px;border-bottom:1px solid rgba(128,128,128,.18)";
		const buttons = [];
		const paint = () => {
			for (const entry of buttons) {
				const active = entry.option.id === selection.value;
				entry.button.style.background = active ? "rgba(127,127,127,.18)" : "transparent";
				entry.button.style.opacity = active ? "1" : ".6";
			}
		};
		for (const option of [{ id: "local", label: "Local machine" }, { id: "orb", label: "New Orb" }]) {
			const button = globalThis.document.createElement("button");
			button.type = "button";
			button.textContent = option.label;
			button.style.cssText = "flex:1;padding:5px 8px;border:0;border-radius:5px;background:transparent;color:inherit;font:inherit;font-size:12px;cursor:default";
			button.addEventListener("click", (event) => {
				event.preventDefault();
				event.stopPropagation();
				selection.value = option.id;
				try {
					globalThis.localStorage.setItem("cliproxyapi.ampLocalInference.executor", option.id);
				} catch {
				}
				paint();
			}, true);
			buttons.push({ option, button });
			if (option.id === "orb") {
				button.title = "Run in a sandboxed container on this server";
			}
			row.appendChild(button);
		}
		paint();
		return row;
	}

	function buildLocalThreadChoiceButton(choice, promptText, workingDirectory, executorSelection) {
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
			const executor = executorSelection && executorSelection.value === "orb" ? "orb" : "local";
			setTimeout(() => createLocalThread(promptText || "", workingDirectory || "", choice.options, executor).catch((error) => {
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

	function encodeBase64URLText(text) {
		const bytes = new globalThis.TextEncoder().encode(String(text));
		let binary = "";
		for (const byte of bytes) {
			binary += String.fromCharCode(byte);
		}
		return globalThis.btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
	}

	function localProjectChangesWorkflowBody(projectID, changesWorkflow) {
		return JSON.stringify({
			payload: encodeBase64URLText(JSON.stringify([
				{ projectID: 1, changesWorkflow: 2 },
				projectID,
				changesWorkflow,
			])),
			refreshes: [],
		});
	}

	async function saveLocalProjectChangesWorkflow(project, changesWorkflow) {
		const projectID = firstString(project?.id, project?.projectID);
		changesWorkflow = normalizeChangesWorkflow(changesWorkflow);
		if (!projectID || !changesWorkflow) {
			throw new Error("Invalid local project setting");
		}
		const response = await globalThis.fetch(globalThis.location.origin + "/_app/remote/127n0b0/updateOwnedProjectChangesWorkflow", {
			method: "POST",
			headers: localFetchHeaders("application/json"),
			body: localProjectChangesWorkflowBody(projectID, changesWorkflow),
		});
		if (!response.ok) {
			throw new Error("Local runtime rejected the project setting");
		}
		const envelope = await response.clone().json();
		const result = decodeDevalueString(envelope?.data || "")?._;
		if (!isPlainObject(result) || result.success !== true) {
			throw new Error(firstString(result?.message, "Unable to save the project setting"));
		}
		project.changesWorkflow = changesWorkflow;
		return result.project;
	}

	function closeLocalProjectSettings() {
		globalThis.__cliproxyAmpLocalProjectSettingsCleanup?.();
	}

	function openLocalProjectSettings(project) {
		closeLocalProjectSettings();
		const backdrop = globalThis.document.createElement("div");
		backdrop.dataset.cliproxyLocalProjectSettings = "1";
		backdrop.style.cssText = "position:fixed;inset:0;z-index:2147483646;display:flex;align-items:center;justify-content:center;padding:20px;background:rgba(0,0,0,.48)";
		const panel = globalThis.document.createElement("div");
		panel.setAttribute("role", "dialog");
		panel.setAttribute("aria-modal", "true");
		panel.setAttribute("aria-label", "Local project settings");
		panel.style.cssText = "width:min(560px,100%%);border:1px solid rgba(127,127,127,.28);border-radius:12px;background:var(--background,#111);color:var(--foreground,#eee);box-shadow:0 24px 70px rgba(0,0,0,.4);font:inherit";
		const header = globalThis.document.createElement("div");
		header.style.cssText = "display:flex;align-items:center;justify-content:space-between;gap:16px;padding:18px 20px;border-bottom:1px solid rgba(127,127,127,.22)";
		const title = globalThis.document.createElement("div");
		title.innerHTML = "<div style=\"font-size:16px;font-weight:600\"></div><div style=\"margin-top:3px;font-size:12px;opacity:.62\">Local checkout settings</div>";
		title.firstElementChild.textContent = firstString(project.name, "Project") + " Settings";
		const close = globalThis.document.createElement("button");
		close.type = "button";
		close.setAttribute("aria-label", "Close local project settings");
		close.textContent = "×";
		close.style.cssText = "border:0;background:transparent;color:inherit;font-size:24px;line-height:1;cursor:pointer;opacity:.72";
		header.append(title, close);
		const content = globalThis.document.createElement("div");
		content.style.cssText = "padding:20px";
		const details = globalThis.document.createElement("div");
		details.style.cssText = "display:grid;grid-template-columns:130px minmax(0,1fr);gap:9px 16px;padding-bottom:20px;font-size:13px";
		for (const [label, value] of [["Git repository", project.repositoryURL], ["Working directory", project.workingDirectory]]) {
			const key = globalThis.document.createElement("div");
			key.textContent = label;
			key.style.opacity = ".62";
			const text = globalThis.document.createElement("div");
			text.textContent = firstString(value, "—");
			text.style.cssText = "overflow:hidden;text-overflow:ellipsis;white-space:nowrap";
			details.append(key, text);
		}
		const settingTitle = globalThis.document.createElement("div");
		settingTitle.textContent = "Changes workflow";
		settingTitle.style.cssText = "font-size:14px;font-weight:600";
		const description = globalThis.document.createElement("div");
		description.textContent = "Choose the main action shown for local runtime threads in this checkout.";
		description.style.cssText = "margin-top:4px;font-size:12px;opacity:.62";
		const options = globalThis.document.createElement("div");
		options.style.cssText = "display:grid;gap:8px;margin-top:14px";
		const status = globalThis.document.createElement("div");
		status.style.cssText = "min-height:18px;margin-top:12px;font-size:12px;opacity:.72";
		const render = () => {
			options.replaceChildren();
			for (const [value, label, detail] of [
				["merge-to-main", "Ship", "Ask Amp to commit and push changes to the target integration branch."],
				["push-to-branch", "Push to Branch", "Ask Amp to commit changes and push the current branch, then return the pull request URL."],
			]) {
				const button = globalThis.document.createElement("button");
				button.type = "button";
				button.style.cssText = "display:flex;align-items:flex-start;gap:10px;width:100%%;padding:12px;border:1px solid rgba(127,127,127,.28);border-radius:8px;background:transparent;color:inherit;text-align:left;cursor:pointer";
				const selected = (normalizeChangesWorkflow(project.changesWorkflow) || "push-to-branch") === value;
				const mark = globalThis.document.createElement("span");
				mark.textContent = selected ? "●" : "○";
				mark.style.cssText = "padding-top:1px;font-size:12px";
				const copy = globalThis.document.createElement("span");
				copy.innerHTML = "<span style=\"display:block;font-size:13px;font-weight:600\"></span><span style=\"display:block;margin-top:3px;font-size:12px;opacity:.62\"></span>";
				copy.children[0].textContent = label;
				copy.children[1].textContent = detail;
				button.append(mark, copy);
				button.addEventListener("click", async () => {
					for (const candidate of options.querySelectorAll("button")) candidate.disabled = true;
					status.textContent = "Saving…";
					try {
						await saveLocalProjectChangesWorkflow(project, value);
						status.textContent = "Saved";
						render();
					} catch (error) {
						status.textContent = error?.message || String(error);
						for (const candidate of options.querySelectorAll("button")) candidate.disabled = false;
					}
				});
				options.append(button);
			}
		};
		render();
		content.append(details, settingTitle, description, options, status);
		panel.append(header, content);
		backdrop.append(panel);
		globalThis.document.body.append(backdrop);
		const onKeyDown = (event) => {
			if (event.key === "Escape") closeLocalProjectSettings();
		};
		const cleanup = () => {
			globalThis.document.removeEventListener("keydown", onKeyDown, true);
			backdrop.remove();
			if (globalThis.__cliproxyAmpLocalProjectSettingsCleanup === cleanup) globalThis.__cliproxyAmpLocalProjectSettingsCleanup = null;
		};
		globalThis.__cliproxyAmpLocalProjectSettingsCleanup = cleanup;
		close.addEventListener("click", cleanup);
		backdrop.addEventListener("click", (event) => {
			if (event.target === backdrop) cleanup();
		});
		globalThis.document.addEventListener("keydown", onKeyDown, true);
	}

	function localProjectScopeForAmpProjectURL(rawURL) {
		const parts = ampProjectURLParts(rawURL);
		if (!parts) {
			return null;
		}
		const mappedProject = localProjectCheckoutsByPath.get(parts.identity);
		const directProject = mappedProject || localProjectsCache.projects.find((project) =>
			ampProjectPathIdentity(project?.namespace, project?.name) === parts.identity,
		);
		const cloud = cloudProjectPaths.has(parts.identity);
		return directProject || cloud ? { cloud, parts, project: directProject || null } : null;
	}

	function projectSettingsAnchor(row, identity) {
		for (const anchor of row?.querySelectorAll?.("a[href]") || []) {
			const parts = ampProjectURLParts(anchor.href);
			if (parts?.settings && parts.identity === identity) {
				return anchor;
			}
		}
		return null;
	}

	function projectListRow(anchor, identity) {
		let row = anchor?.parentElement;
		for (let depth = 0; row && depth < 6; depth += 1, row = row.parentElement) {
			const settings = projectSettingsAnchor(row, identity);
			if (settings) {
				return { row, settings };
			}
		}
		return null;
	}

	function projectScopeBadge(label, scope) {
		const badge = globalThis.document.createElement("span");
		badge.dataset.cliproxyProjectScopeDecoration = "1";
		badge.textContent = label;
		badge.style.cssText = [
			"display:inline-flex",
			"align-items:center",
			"height:20px",
			"padding:0 7px",
			"border:1px solid " + (scope === "local" ? "rgba(82,140,255,.32)" : "rgba(127,127,127,.28)"),
			"border-radius:999px",
			"background:" + (scope === "local" ? "rgba(82,140,255,.1)" : "rgba(127,127,127,.08)"),
			"font-size:11px",
			"font-weight:500",
			"line-height:1",
			"white-space:nowrap",
		].join(";");
		return badge;
	}

	function decorateLocalProjectList(root = globalThis.document) {
		if (!/^\/projects\/?$/.test(globalThis.location.pathname || "")) {
			return;
		}
		const anchors = [];
		if (root instanceof Element && root.matches("a[href]")) anchors.push(root);
		for (const anchor of root.querySelectorAll?.("a[href]") || []) anchors.push(anchor);
		for (const anchor of anchors) {
			const parts = ampProjectURLParts(anchor.href);
			if (!parts || parts.settings) {
				continue;
			}
			const scope = localProjectScopeForAmpProjectURL(anchor.href);
			if (!scope) {
				continue;
			}
			const projectRow = projectListRow(anchor, parts.identity);
			if (!projectRow) {
				continue;
			}
			const { row, settings } = projectRow;
			const workingDirectory = normalizeWorkingDirectory(scope.project?.workingDirectory);
			const signature = [userscriptVersion, scope.cloud ? "cloud" : "", workingDirectory].join(":");
			if (row.dataset.cliproxyProjectScope === signature && row.querySelector("[data-cliproxy-project-scope-decoration]")) {
				continue;
			}
			for (const decoration of row.querySelectorAll("[data-cliproxy-project-scope-decoration]")) decoration.remove();
			row.dataset.cliproxyProjectScope = signature;
			if (!scope.cloud) {
				anchor.setAttribute("data-sveltekit-preload-data", "off");
				settings.setAttribute("data-sveltekit-preload-data", "off");
			}
			const titleRow = anchor.parentElement;
			if (scope.cloud) titleRow?.append(projectScopeBadge("Amp Cloud", "cloud"));
			if (scope.project) titleRow?.append(projectScopeBadge(scope.cloud ? "Local checkout" : "Local", "local"));
			if (scope.project && workingDirectory && titleRow?.parentElement) {
				const path = globalThis.document.createElement("div");
				path.dataset.cliproxyProjectScopeDecoration = "1";
				path.textContent = localProjectPickerPathDisplay(workingDirectory);
				path.title = workingDirectory;
				path.style.cssText = "margin-top:3px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:12px;opacity:.62";
				titleRow.parentElement.append(path);
			}
			if (scope.cloud) {
				settings.setAttribute("aria-label", "Amp Cloud Project Settings " + parts.name);
				settings.title = "Amp Cloud project settings";
			} else {
				settings.setAttribute("aria-label", "Local Project Settings " + parts.name);
				settings.title = "Local project settings";
			}
			if (scope.cloud && scope.project) {
				const localSettings = globalThis.document.createElement("button");
				localSettings.type = "button";
				localSettings.dataset.cliproxyProjectScopeDecoration = "1";
				localSettings.dataset.cliproxyLocalProjectSettings = parts.identity;
				localSettings.setAttribute("aria-label", "Local Checkout Settings " + parts.name);
				localSettings.title = "Local checkout settings";
				localSettings.textContent = "Local settings";
				localSettings.style.cssText = "position:relative;z-index:10;display:inline-flex;align-items:center;height:30px;padding:0 9px;border:1px solid rgba(82,140,255,.28);border-radius:7px;background:rgba(82,140,255,.08);color:inherit;font:inherit;font-size:12px;white-space:nowrap;cursor:pointer";
				row.insertBefore(localSettings, settings);
			}
		}
	}

	function scheduleLocalProjectListDecoration() {
		if (localProjectListDecorationPending) {
			return;
		}
		localProjectListDecorationPending = true;
		const render = () => {
			localProjectListDecorationPending = false;
			decorateLocalProjectList();
		};
		if (typeof globalThis.requestAnimationFrame === "function") {
			globalThis.requestAnimationFrame(render);
		} else {
			globalThis.setTimeout(render, 0);
		}
	}

	function localProjectPageElement(tagName, className = "", text = "") {
		const element = globalThis.document.createElement(tagName);
		if (className) element.className = className;
		if (text) element.textContent = text;
		return element;
	}

	function localProjectPageSection(title) {
		const section = localProjectPageElement("section", "min-w-0 space-y-2");
		const header = localProjectPageElement("div", "flex items-center justify-between gap-3 px-1");
		const heading = localProjectPageElement("h2", "min-w-0 truncate text-base font-medium tracking-tight", title);
		header.append(heading);
		const card = localProjectPageElement("div", "flex flex-col rounded-xl border border-border/60 bg-card/80 shadow-sm");
		const body = localProjectPageElement("div", "flex flex-1 flex-col p-2");
		card.append(body);
		section.append(header, card);
		return { body, header, heading, section };
	}

	function localProjectPageEmpty(label) {
		return localProjectPageElement("div", "flex flex-1 items-center justify-center px-2 py-10 text-center text-base font-medium tracking-tight text-muted-foreground", label);
	}

	function localProjectPageTopLevelFiles(files) {
		const entries = new Map();
		for (const rawPath of files || []) {
			const parts = String(rawPath || "").split("/").filter(Boolean);
			if (!parts.length) continue;
			const type = parts.length > 1 ? "dir" : "file";
			if (entries.get(parts[0]) !== "dir") entries.set(parts[0], type);
		}
		return [...entries].map(([name, type]) => ({ name, type })).sort((left, right) => left.type === right.type ? left.name.localeCompare(right.name) : left.type === "dir" ? -1 : 1);
	}

	function localProjectPageCommitURL(project, commit) {
		const repositoryURL = firstString(project?.repositoryURL, project?.repoURL).replace(/\.git$/i, "");
		return /^https:\/\/github\.com\//i.test(repositoryURL) && commit?.sha ? repositoryURL + "/commit/" + encodeURIComponent(commit.sha) : "";
	}

	function localProjectPageFetch(parts, project, includeThreads = false) {
		const headers = localFetchHeaders("", false);
		if (!headers.get("Authorization")) return Promise.reject(new Error("local API key unavailable"));
		const url = new URL(localBaseURLString() + localProjectDetailsEndpointPath);
		url.searchParams.set("namespace", parts.namespace);
		url.searchParams.set("project", parts.name);
		const presentedProjectID = firstString(project?.id, project?.projectID);
		const canonicalProject = localProjectWebIDs.get(presentedProjectID) || project;
		const projectID = firstString(canonicalProject?.id, canonicalProject?.projectID);
		if (projectID) url.searchParams.set("projectID", projectID);
		const repositoryURL = firstString(canonicalProject?.repositoryURL, canonicalProject?.repoURL);
		if (repositoryURL) url.searchParams.set("repository", repositoryURL);
		const workingDirectory = firstString(canonicalProject?.workingDirectory, project?.workingDirectory);
		if (workingDirectory) url.searchParams.set("workingDirectory", workingDirectory);
		if (includeThreads) url.searchParams.set("includeThreads", "1");
		return localProjectJSONRequest(url.href, headers).then((response) => response.json().catch(() => ({})).then((details) => {
			if (!response.ok) throw new Error(firstString(details?.error, "Local project details returned " + String(response.status || 0)));
			return details;
		})).then((details) => {
			if (!isPlainObject(details) || details.ok !== true || !isPlainObject(details.project)) throw new Error("invalid local project details");
			return details;
		});
	}

	function localProjectPageCachedThreads(project) {
		return (localProjectsCache.threads || []).filter((thread) => {
			const threadProject = localProjectForSidebarThread(thread);
			return !!threadProject && localSidebarProjectMatches(threadProject, project);
		}).slice(0, 20);
	}

	function populateLocalProjectPage(overlay, details) {
		const content = overlay.querySelector("[data-cliproxy-local-project-page-content]");
		if (!content) return;
		content.replaceChildren();
		const project = details.project;
		const commits = Array.isArray(details.commits) ? details.commits : [];
		const files = localProjectPageTopLevelFiles(Array.isArray(details.files) ? details.files : []);
		const threads = Array.isArray(details.threads) ? details.threads : [];

		const repository = localProjectPageElement("div", "flex min-w-0 items-center gap-2 rounded-xl border border-border/60 bg-card/80 px-3 py-2 text-sm shadow-sm");
		const source = localProjectPageElement("span", "shrink-0 rounded-full border px-2 py-0.5 text-xs font-medium", "Local checkout");
		const path = localProjectPageElement("span", "min-w-0 flex-1 truncate font-mono text-xs text-muted-foreground", localProjectPickerPathDisplay(project.workingDirectory));
		path.title = project.workingDirectory;
		const currentBranch = firstString(details.currentBranch);
		const branchLabel = currentBranch || (details.detachedAt ? "Detached at " + String(details.detachedAt) : "Detached HEAD");
		const branch = localProjectPageElement("span", "shrink-0 text-xs text-muted-foreground", branchLabel);
		if (details.hasLocalChanges === true) branch.textContent += " · changes";
		repository.append(source, path, branch);
		content.append(repository);

		const commitSection = localProjectPageSection("Commits to " + firstString(details.defaultBranch, "main"));
		content.append(commitSection.section);
		if (!commits.length) {
			commitSection.body.append(localProjectPageEmpty("No Commits Yet"));
		} else {
			let page = 0;
			const pageSize = 5;
			const controls = localProjectPageElement("div", "ml-auto flex items-center gap-1");
			const previous = localProjectPageElement("button", "rounded px-2 py-1 text-sm text-muted-foreground hover:bg-muted/50", "‹");
			const next = localProjectPageElement("button", "rounded px-2 py-1 text-sm text-muted-foreground hover:bg-muted/50", "›");
			previous.setAttribute("aria-label", "Previous commits page");
			next.setAttribute("aria-label", "Next commits page");
			controls.append(previous, next);
			commitSection.header.append(controls);
			const renderCommits = () => {
				commitSection.body.replaceChildren();
				for (const commit of commits.slice(page * pageSize, (page + 1) * pageSize)) {
					const row = localProjectPageElement("div", "flex min-w-0 items-start gap-2 rounded-lg px-2 py-1.5 hover:bg-muted/30");
					const avatar = localProjectPageElement("span", "mt-0.5 flex size-5 shrink-0 items-center justify-center rounded-full border bg-muted text-[10px] font-medium text-muted-foreground", firstString(commit.authorName, commit.authorEmail, "?").slice(0, 1).toUpperCase());
					const body = localProjectPageElement("div", "min-w-0 flex-1");
					body.append(localProjectPageElement("div", "truncate text-sm font-medium", firstString(commit.message, commit.shortSha)));
					const metadata = localProjectPageElement("div", "flex min-w-0 items-center gap-2 text-[10px] text-muted-foreground");
					const commitURL = localProjectPageCommitURL(project, commit);
					const sha = localProjectPageElement(commitURL ? "a" : "span", "font-mono", firstString(commit.shortSha, String(commit.sha || "").slice(0, 8)));
					if (commitURL) {
						sha.setAttribute("href", commitURL);
						sha.setAttribute("target", "_blank");
						sha.setAttribute("rel", "noreferrer");
					}
					metadata.append(sha, localProjectPageElement("span", "truncate", firstString(commit.authorName, commit.authorEmail)), localProjectPageElement("span", "shrink-0", commit.committedAt ? new Date(commit.committedAt).toLocaleString() : ""));
					body.append(metadata);
					row.append(avatar, body);
					commitSection.body.append(row);
				}
				previous.disabled = page === 0;
				next.disabled = (page + 1) * pageSize >= commits.length;
			};
			previous.addEventListener("click", () => { page = Math.max(0, page - 1); renderCommits(); });
			next.addEventListener("click", () => { page = Math.min(Math.ceil(commits.length / pageSize) - 1, page + 1); renderCommits(); });
			renderCommits();
		}

		const fileSectionTitle = details.filesTruncated === true ? "Files (first " + String(Number(details.filesLimit || 2000).toLocaleString()) + " tracked)" : "Files";
		const fileSection = localProjectPageSection(fileSectionTitle);
		content.append(fileSection.section);
		if (!files.length) {
			fileSection.body.append(localProjectPageEmpty("No Files Yet"));
		} else {
			for (const file of files) {
				const row = localProjectPageElement("div", "flex h-7 min-w-0 items-center gap-2 rounded-lg px-2 text-sm");
				row.append(localProjectPageElement("span", "w-4 shrink-0 text-center text-muted-foreground", file.type === "dir" ? "›" : "·"), localProjectPageElement("span", "min-w-0 truncate", file.name));
				fileSection.body.append(row);
			}
		}

		const threadSection = localProjectPageSection("Recent Threads");
		content.append(threadSection.section);
		if (!threads.length) {
			threadSection.body.append(localProjectPageEmpty("No Threads Yet"));
		} else {
			for (const thread of threads.slice(0, 10)) {
				const threadID = firstString(thread.id, thread.threadId, thread.threadID);
				if (!validThreadID(threadID)) continue;
				const row = localProjectPageElement("a", "flex min-w-0 items-start gap-2 rounded-lg px-2 py-1.5 hover:bg-muted/30");
				row.setAttribute("href", "/threads/" + encodeURIComponent(threadID));
				const creator = isPlainObject(thread.creator) ? thread.creator : {};
				const pictureURL = firstString(creator.profilePictureUrl, authenticatedAmpUser.profilePictureUrl);
				const creatorName = firstString(creator.username, creator.firstName, creator.email, authenticatedAmpUser.username, authenticatedAmpUser.firstName, authenticatedAmpUser.email, "Local user");
				const avatar = localProjectPageElement(pictureURL ? "img" : "span", pictureURL ? "mt-0.5 size-5 shrink-0 rounded-full border object-cover" : "mt-0.5 flex size-5 shrink-0 items-center justify-center rounded-full border bg-muted text-[10px] font-medium text-muted-foreground", pictureURL ? "" : creatorName.slice(0, 1).toUpperCase());
				if (pictureURL) {
					avatar.src = pictureURL;
					avatar.alt = creatorName;
				}
				const body = localProjectPageElement("div", "min-w-0 flex-1");
				const title = localProjectPageElement("div", "flex min-w-0 items-center gap-2");
				title.append(localProjectPageElement("p", "min-w-0 flex-1 truncate text-sm font-medium", firstString(thread.title, "Untitled")));
				if (thread.archived === true) title.append(localProjectPageElement("span", "rounded bg-muted px-1.5 py-0.5 text-[10px] text-muted-foreground", "Archived"));
				const metadata = localProjectPageElement("div", "flex items-center gap-2 text-[10px] text-muted-foreground");
				metadata.append(localProjectPageElement("span", "truncate", creatorName), localProjectPageElement("span", "shrink-0", localActivityThreadTime(thread)), localProjectPageElement("span", "shrink-0", String(Number(thread.messageCount || thread.summaryStats?.messageCount || 0)) + " messages"));
				body.append(title, metadata);
				row.append(avatar, body);
				threadSection.body.append(row);
			}
		}
	}

	function closeLocalProjectPage() {
		localProjectPageGeneration += 1;
		const overlay = globalThis.document.querySelector("[data-cliproxy-local-project-page]");
		if (!overlay) return;
		if (overlay.dataset.cliproxyPreviousTitle) globalThis.document.title = overlay.dataset.cliproxyPreviousTitle;
		overlay.remove();
	}

	function synchronizeLocalProjectPageRoute() {
		const scope = localProjectScopeForAmpProjectURL(globalThis.location.href);
		const overlay = globalThis.document.querySelector("[data-cliproxy-local-project-page]");
		if (scope?.project && !scope.cloud && !scope.parts.settings) {
			if (overlay?.dataset.cliproxyLocalProjectPage !== scope.parts.identity) openLocalProjectPage(scope.project, scope.parts, false);
			return;
		}
		closeLocalProjectPage();
	}

	function openLocalProjectPage(project, parts, pushHistory = true) {
		const host = globalThis.document.querySelector('main[data-slot="sidebar-inset"]');
		if (!host || !project || !parts) return;
		closeLocalProjectPage();
		const generation = ++localProjectPageGeneration;
		const targetPath = "/@" + encodeURIComponent(parts.namespace) + "/" + encodeURIComponent(parts.name);
		const previousPath = globalThis.location.pathname + globalThis.location.search;
		if (pushHistory && globalThis.location.pathname !== targetPath) globalThis.history.pushState({ cliproxyLocalProject: parts.identity }, "", targetPath);
		const overlay = localProjectPageElement("div", "absolute inset-0 z-30 flex h-full flex-col bg-background text-foreground");
		overlay.dataset.cliproxyLocalProjectPage = parts.identity;
		overlay.dataset.cliproxyPreviousPath = previousPath;
		overlay.dataset.cliproxyPreviousTitle = globalThis.document.title;
		const header = localProjectPageElement("div", "app-title-bar flex h-9 shrink-0 items-center gap-2 border-b px-3");
		const projects = localProjectPageElement("button", "shrink-0 text-base text-muted-foreground hover:underline", "Projects");
		projects.type = "button";
		projects.addEventListener("click", () => {
			if (pushHistory) globalThis.history.back();
			else globalThis.location.href = "/projects";
		});
		header.append(projects, localProjectPageElement("span", "text-muted-foreground", "/"), localProjectPageElement("h2", "min-w-0 flex-1 truncate text-base font-medium", parts.name));
		const settings = localProjectPageElement("button", "rounded-sm px-2 py-1 text-sm text-foreground/80 hover:bg-foreground/5", "Settings");
		settings.type = "button";
		settings.addEventListener("click", () => openLocalProjectSettings(project));
		const newThread = localProjectPageElement("button", "rounded-sm px-2 py-1 text-sm font-medium text-foreground/80 hover:bg-foreground/5", "New Thread");
		newThread.type = "button";
		newThread.addEventListener("click", () => {
			rememberSelectedLocalProject(project);
			const nativeNewThread = Array.from(globalThis.document.querySelectorAll("button")).find((button) => !overlay.contains(button) && (button.getAttribute("aria-label") === "New Thread" || button.textContent?.trim() === "New Thread"));
			nativeNewThread?.click();
		});
		header.append(settings, newThread);
		const scroll = localProjectPageElement("div", "flex min-h-0 flex-1 flex-col overflow-y-auto");
		const content = localProjectPageElement("div", "flex flex-col gap-4 p-3");
		content.dataset.cliproxyLocalProjectPageContent = "1";
		content.append(localProjectPageEmpty("Loading local project…"));
		scroll.append(content);
		overlay.append(header, scroll);
		host.append(overlay);
		globalThis.document.title = parts.name + " - Amp";
		diagnostics.localProjectPageIntegrationCount += 1;
		localProjectPageFetch(parts, project, true).then((details) => {
			if (generation !== localProjectPageGeneration || !globalThis.document.contains(overlay)) return;
			if (!Array.isArray(details.threads) || details.threads.length === 0) details.threads = localProjectPageCachedThreads(details.project);
			populateLocalProjectPage(overlay, details);
		}).catch((error) => {
			if (generation !== localProjectPageGeneration || !globalThis.document.contains(overlay)) return;
			content.replaceChildren(localProjectPageEmpty("Failed to load local project: " + String(error?.message || error)));
		});
	}

	function installLocalProjectPageIntegration() {
		if (!globalThis.__cliproxyAmpLocalProjectPagePopstate && typeof globalThis.addEventListener === "function") {
			const onPopstate = synchronizeLocalProjectPageRoute;
			globalThis.addEventListener("popstate", onPopstate);
			globalThis.__cliproxyAmpLocalProjectPagePopstate = onPopstate;
		}
		fetchLocalProjects(false).then(() => {
			const openMissingLocalProjectPage = () => {
				const scope = localProjectScopeForAmpProjectURL(globalThis.location.href);
				if (!scope?.project || scope.cloud || scope.parts.settings) return false;
				const host = globalThis.document.querySelector('main[data-slot="sidebar-inset"]');
				const missingPage = localProjectNativeMissingPage(host);
				if (!missingPage) return false;
				openLocalProjectPage(scope.project, scope.parts, false);
				return true;
			};
			if (openMissingLocalProjectPage()) return;
			const scope = localProjectScopeForAmpProjectURL(globalThis.location.href);
			if (!scope?.project || scope.cloud || scope.parts.settings || typeof globalThis.MutationObserver !== "function") return;
			const observer = new MutationObserver(() => {
				if (openMissingLocalProjectPage()) observer.disconnect();
			});
			observer.observe(globalThis.document.documentElement, { childList: true, subtree: true });
			globalThis.setTimeout(() => observer.disconnect(), 5000);
		});
	}

	function localProjectNativeMissingPage(host) {
		const missing = /^(?:404(?:\s*[:|-].*)?|(?:project\s+)?not found|failed to load project)(?:\s*[-|·]\s*amp)?$/i;
		if (missing.test(String(globalThis.document.title || "").trim())) return true;
		const candidates = Array.from(host?.querySelectorAll?.("h1") || []);
		for (const selector of ["[data-error-page]", '[data-status-code="404"]']) {
			const marker = host?.querySelector?.(selector);
			if (marker) candidates.push(marker);
		}
		return candidates.some((element) => missing.test(String(element.textContent || "").trim()));
	}

	function installLocalProjectSettingsIntegration() {
		if (globalThis.__cliproxyAmpLocalProjectSettingsClick) {
			scheduleLocalProjectListDecoration();
			return;
		}
		const onClick = (event) => {
			const target = event.target instanceof Element ? event.target : null;
			const localSettings = target?.closest("[data-cliproxy-local-project-settings]");
			if (localSettings) {
				const project = localProjectCheckoutsByPath.get(localSettings.dataset.cliproxyLocalProjectSettings);
				if (project) {
					event.preventDefault();
					event.stopPropagation();
					openLocalProjectSettings(project);
				}
				return;
			}
			const anchor = target?.closest("a[href]");
			const scope = anchor ? localProjectScopeForAmpProjectURL(anchor.href) : null;
			if (anchor && ((event.button != null && event.button !== 0) || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey)) {
				return;
			}
			if (scope?.project && !scope.cloud && !scope.parts.settings) {
				event.preventDefault();
				event.stopPropagation();
				event.stopImmediatePropagation?.();
				openLocalProjectPage(scope.project, scope.parts, true);
				return;
			}
			if (!scope?.parts.settings || !scope.project || scope.cloud) {
				return;
			}
			event.preventDefault();
			event.stopPropagation();
			openLocalProjectSettings(scope.project);
		};
		globalThis.document.addEventListener("click", onClick, true);
		globalThis.__cliproxyAmpLocalProjectSettingsClick = onClick;
		installLocalProjectPageIntegration();
		if (typeof globalThis.MutationObserver === "function") {
			const observer = new MutationObserver(() => {
				if (/^\/projects\/?$/.test(globalThis.location.pathname || "")) {
					scheduleLocalProjectListDecoration();
				}
			});
			observer.observe(globalThis.document.documentElement, { childList: true, subtree: true });
			globalThis.__cliproxyAmpLocalProjectSettingsObserver = observer;
		}
		fetchLocalProjects(false).finally(scheduleLocalProjectListDecoration);
	}

	function installLocalThreadControls() {
		removeInjectedLocalThreadControls();
		startLocalSidebarHydration();
		installLocalProjectPickerIntegration();
		installLocalProjectSettingsIntegration();
		installLocalSidebarMetadataIntegration();
		installLocalActivityIntegration();
	}

	function removeStaleLocalThreadButton() {
		globalThis.document.getElementById("cliproxy-amp-local-thread-button")?.remove();
	}

	function removeInjectedLocalThreadControls() {
		removeStaleLocalThreadButton();
		removeLocalThreadPicker();
		closeLocalProjectSettings();
		if (globalThis.__cliproxyAmpLocalProjectSettingsClick) {
			globalThis.document.removeEventListener("click", globalThis.__cliproxyAmpLocalProjectSettingsClick, true);
			globalThis.__cliproxyAmpLocalProjectSettingsClick = null;
		}
		globalThis.document.querySelectorAll("[data-cliproxy-project-scope-decoration]").forEach((element) => element.remove());
		globalThis.document.querySelectorAll("[data-cliproxy-project-scope]").forEach((element) => delete element.dataset.cliproxyProjectScope);
		let removed = 0;
		globalThis.document.querySelectorAll("[data-cliproxy-local-thread-command-item],[data-cliproxy-local-thread-menu-item],[data-cliproxy-local-project-item]").forEach((element) => {
			element.remove();
			removed += 1;
		});
		globalThis.document.querySelectorAll("[data-cliproxy-local-project-activator-loading],[data-cliproxy-local-project-loading],[data-cliproxy-local-project-attempts]").forEach((element) => {
			delete element.dataset.cliproxyLocalProjectActivatorLoading;
			delete element.dataset.cliproxyLocalProjectLoading;
			delete element.dataset.cliproxyLocalProjectAttempts;
		});
		resetLocalProjectsCache();
		for (const key of ["__cliproxyAmpLocalInferenceMenuObserver", "__cliproxyAmpLocalInferenceCommandPaletteObserver", "__cliproxyAmpLocalInferenceProjectPickerObserver", "__cliproxyAmpLocalProjectSettingsObserver", "__cliproxyAmpLocalSidebarObserver", "__cliproxyAmpLocalActivityObserver"]) {
			const observer = globalThis[key];
			if (observer && typeof observer.cliproxyCleanup === "function") {
				observer.cliproxyCleanup();
			} else if (observer && typeof observer.disconnect === "function") {
				observer.disconnect();
			}
			globalThis[key] = null;
		}
		localProjectListDecorationPending = false;
		diagnostics.removedLocalThreadControlCount += removed;
	}

	function installLocalProjectPickerIntegration() {
		if (globalThis.__cliproxyAmpLocalInferenceProjectPickerObserver) {
			integrateLocalProjectActivators(globalThis.document);
			integrateLocalProjectPickers(globalThis.document);
			return;
		}
		localProjectIntegrationGeneration += 1;
		const pendingRoots = new Set();
		let flushScheduled = false;
		let animationFrameID = null;
		let fallbackTimerID = null;
		const cancelScheduledFlush = () => {
			if (animationFrameID !== null && typeof globalThis.cancelAnimationFrame === "function") {
				globalThis.cancelAnimationFrame(animationFrameID);
			}
			if (fallbackTimerID !== null) {
				clearTimeout(fallbackTimerID);
			}
			animationFrameID = null;
			fallbackTimerID = null;
		};
		const flushPendingRoots = () => {
			if (!flushScheduled) {
				return;
			}
			flushScheduled = false;
			cancelScheduledFlush();
			if (globalThis.__cliproxyAmpLocalInferenceProjectPickerObserver !== observer) {
				pendingRoots.clear();
				diagnostics.projectMutationPendingRootCount = 0;
				return;
			}
			const roots = Array.from(pendingRoots);
			pendingRoots.clear();
			diagnostics.projectMutationPendingRootCount = 0;
			let context = null;
			for (const root of roots) {
				if (!globalThis.document.contains(root)) {
					diagnostics.projectMutationIgnoredCount += 1;
					continue;
				}
				context ||= localProjectActivatorContext();
				integrateLocalProjectActivators(root, context);
				integrateLocalProjectPickers(root);
			}
			diagnostics.projectMutationFlushCount += 1;
		};
		const scheduleFlush = () => {
			if (flushScheduled) {
				return;
			}
			flushScheduled = true;
			if (typeof globalThis.requestAnimationFrame === "function") {
				animationFrameID = globalThis.requestAnimationFrame(flushPendingRoots);
				fallbackTimerID = setTimeout(flushPendingRoots, 100);
			} else {
				fallbackTimerID = setTimeout(flushPendingRoots, 0);
			}
		};
		const queueRoot = (root) => {
			root = localProjectIntegrationRoot(root);
			if (!localProjectIntegrationRootRelevant(root)) {
				diagnostics.projectMutationIgnoredCount += 1;
				return;
			}
			for (const pending of pendingRoots) {
				if (pending === root || pending.contains(root)) {
					diagnostics.projectMutationCoalescedCount += 1;
					return;
				}
				if (root.contains(pending)) {
					pendingRoots.delete(pending);
					diagnostics.projectMutationCoalescedCount += 1;
				}
			}
			if (pendingRoots.size >= 32) {
				pendingRoots.clear();
				pendingRoots.add(globalThis.document.documentElement);
				diagnostics.projectMutationCoalescedCount += 1;
				diagnostics.projectMutationPendingRootCount = 1;
				scheduleFlush();
				return;
			}
			pendingRoots.add(root);
			diagnostics.projectMutationPendingRootCount = pendingRoots.size;
			scheduleFlush();
		};
		const observer = new MutationObserver((mutations) => {
			for (const mutation of mutations) {
				for (const node of mutation.addedNodes) {
					if (node instanceof Element) {
						diagnostics.projectMutationCandidateCount += 1;
						queueRoot(node);
					}
				}
			}
		});
		observer.cliproxyCleanup = () => {
			observer.disconnect();
			localProjectIntegrationGeneration += 1;
			flushScheduled = false;
			cancelScheduledFlush();
			pendingRoots.clear();
			diagnostics.projectMutationPendingRootCount = 0;
		};
		observer.observe(globalThis.document.documentElement, { childList: true, subtree: true });
		globalThis.__cliproxyAmpLocalInferenceProjectPickerObserver = observer;
		integrateLocalProjectActivators(globalThis.document);
		integrateLocalProjectPickers(globalThis.document);
	}

	function localProjectIntegrationRoot(root) {
		if (!(root instanceof Element) || root.closest("article,[data-message-id],[data-message]")) {
			return root;
		}
		return root.closest('[cmdk-root],[data-cmdk-root],[data-slot="dialog-content"],[role="dialog"]') || root;
	}

	function localProjectIntegrationRootRelevant(root) {
		if (!(root instanceof Element) || root.closest("article,[data-message-id],[data-message]")) {
			return false;
		}
		const selector = "button,[role='button'],[cmdk-root],[data-cmdk-root],[data-slot='dialog-content'],[role='dialog'],[cmdk-list],[data-cmdk-list],[data-slot='command-list'],[role='listbox']";
		if (root.matches(selector)) {
			return true;
		}
		for (const candidate of root.querySelectorAll(selector)) {
			if (!candidate.closest("article,[data-message-id],[data-message]")) {
				return true;
			}
		}
		return false;
	}

	function localProjectActivatorContext() {
		const visibleName = visibleProjectName();
		const visibleWorkingDirectory = visibleProjectWorkingDirectory(visibleName);
		const selectedProject = selectedLocalProject();
		const workingDirectory = normalizeWorkingDirectory(selectedProject?.workingDirectory) ||
			activeThreadWorkingDirectory() ||
			visibleWorkingDirectory ||
			storedLocalWorkingDirectory() ||
			defaultLocalWorkingDirectory();
		const noProject = localProjectIsNoProject(selectedProject) ||
			(!visibleName && workingDirectory === defaultLocalWorkingDirectory());
		return {
			workingDirectory,
			visibleName,
			visibleNeedsLookup: !!visibleName && !visibleWorkingDirectory,
			targetLabel: noProject ? "No Project" : firstString(visibleName, pathBaseName(workingDirectory)),
		};
	}

	function integrateLocalProjectActivators(root, context = localProjectActivatorContext()) {
		const { workingDirectory, visibleName, visibleNeedsLookup, targetLabel } = context;
		if (!workingDirectory && !visibleName) {
			return;
		}
		for (const activator of localProjectActivatorCandidates(root)) {
			if ((!visibleNeedsLookup && workingDirectory && !localProjectActivatorNeedsPatch(activator, workingDirectory, targetLabel)) || activator.dataset.cliproxyLocalProjectActivatorLoading === "1") {
				continue;
			}
			activator.dataset.cliproxyLocalProjectActivatorLoading = "1";
			const generation = localProjectIntegrationGeneration;
			fetchLocalProjects(false).then((projects) => {
				if (generation !== localProjectIntegrationGeneration) {
					return;
				}
				delete activator.dataset.cliproxyLocalProjectActivatorLoading;
				let project = localProjectByVisibleName(projects, visibleName);
				const resolvedWorkingDirectory = normalizeWorkingDirectory(project?.workingDirectory) || workingDirectory;
				if (!project) {
					project = localProjectByWorkingDirectory(projects, resolvedWorkingDirectory);
				}
				const resolvedNoProject = !visibleName && resolvedWorkingDirectory === defaultLocalWorkingDirectory();
				const label = resolvedNoProject ? "No Project" : firstString(project?.name, targetLabel, pathBaseName(resolvedWorkingDirectory));
				if (!resolvedWorkingDirectory || !globalThis.document.contains(activator) || !localProjectActivatorNeedsPatch(activator, resolvedWorkingDirectory, label)) {
					return;
				}
				if (!label) {
					return;
				}
				patchLocalProjectActivator(activator, label, resolvedWorkingDirectory);
			});
		}
	}

	function localProjectActivatorCandidates(root) {
		const out = [];
		const add = (element) => {
			if (element instanceof Element && !element.closest("article,[data-message-id],[data-message]") && !out.includes(element)) {
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
		return localProjectActivatorProjectLabel(activator) === "No Project";
	}

	function localProjectActivatorProjectLabel(activator) {
		if (!(activator instanceof Element)) {
			return "";
		}
		const text = (activator.innerText || activator.textContent || "").replace(/\s+/g, " ").trim();
		const match = text.match(/(?:^|\b)Project:\s*([^\n]+?)(?:\s{2,}|\s*[⌃⌥⇧⌘]|$)/);
		return match && match[1] ? match[1].trim() : "";
	}

	function localProjectActivatorNeedsPatch(activator, workingDirectory, label = "") {
		if (!elementVisible(activator)) {
			return false;
		}
		if (localProjectActivatorLooksUnset(activator)) {
			return true;
		}
		const currentLabel = localProjectActivatorProjectLabel(activator);
		if (label && currentLabel && normalizeProjectPickerName(currentLabel) !== normalizeProjectPickerName(label)) {
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
		const replacementTargets = ["No Project", activator.dataset.cliproxyLocalProjectLabel, localProjectActivatorProjectLabel(activator)].filter((target) => target && target !== label);
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

	function refreshLocalProjectActivators(project, root = globalThis.document, includeProjectLabels = false) {
		const workingDirectory = normalizeWorkingDirectory(project?.workingDirectory);
		if (!workingDirectory) {
			return;
		}
		const label = firstString(project?.name, pathBaseName(workingDirectory));
		if (!label) {
			return;
		}
		for (const activator of localProjectActivatorCandidates(root)) {
			if (localProjectActivatorLooksUnset(activator) || activator.dataset?.cliproxyLocalProjectActivator === "1" || (includeProjectLabels && localProjectActivatorProjectLabel(activator))) {
				patchLocalProjectActivator(activator, label, workingDirectory);
			}
		}
	}

	function refreshCreateThreadProjectActivators(project) {
		for (const palette of commandPaletteCandidates(globalThis.document)) {
			if (!elementVisible(palette)) {
				continue;
			}
			const text = (palette.innerText || palette.textContent || "").replace(/\s+/g, " ").trim();
			if (!text.includes("Create Thread") || !/\bProject:/.test(text)) {
				continue;
			}
			refreshLocalProjectActivators(project, palette, true);
		}
	}

	function refreshCreateThreadProjectActivatorsAfterSelection(project) {
		for (const delay of [0, 50, 150]) {
			setTimeout(() => refreshCreateThreadProjectActivators(project), delay);
		}
	}

	function localProjectByWorkingDirectory(projects, workingDirectory) {
		const target = normalizeWorkingDirectory(workingDirectory);
		let best = null;
		let bestLength = -1;
		for (const project of projects || []) {
			const projectDirectory = normalizeWorkingDirectory(project?.workingDirectory);
			if (projectDirectory === target) {
				return project;
			}
			if (workingDirectoryWithinWorkspace(projectDirectory, target) && projectDirectory.length > bestLength) {
				best = project;
				bestLength = projectDirectory.length;
			}
		}
		return best;
	}

	function localProjectByID(projects, projectID) {
		const target = firstString(projectID);
		if (!target) {
			return null;
		}
		const webProject = localProjectWebIDs.get(target);
		if (webProject) {
			return webProject;
		}
		for (const project of projects || []) {
			if (firstString(project?.id, project?.projectID, project?.projectId, project?.project_id) === target) {
				return project;
			}
		}
		return null;
	}

	function localProjectByVisibleName(projects, visibleName = visibleProjectName()) {
		const target = normalizeProjectPickerName(visibleName);
		if (!target) {
			return null;
		}
		const matches = [];
		for (const project of projects || []) {
			if (normalizeProjectPickerName(firstString(project?.name, pathBaseName(project?.workingDirectory))) === target) {
				matches.push(project);
			}
		}
		if (matches.length <= 1) {
			return matches[0] || null;
		}
		for (const workingDirectory of [activeThreadWorkingDirectory(), selectedLocalProjectWorkingDirectory(), storedLocalWorkingDirectory()]) {
			const project = localProjectByWorkingDirectory(matches, workingDirectory);
			if (project) {
				return project;
			}
		}
		matches.sort((left, right) => localProjectDirectoryRank(left?.workingDirectory) - localProjectDirectoryRank(right?.workingDirectory));
		return matches[0] || null;
	}

	function localProjectDirectoryRank(workingDirectory) {
		const dir = normalizeWorkingDirectory(workingDirectory);
		if (!dir) {
			return 99;
		}
		const activeDirectory = activeThreadWorkingDirectory();
		if (activeDirectory && dir === activeDirectory) {
			return 0;
		}
		if (/^\/Users\/[^/]+\/Developer\//.test(dir)) {
			return 1;
		}
		if (/^\/Users\//.test(dir)) {
			return 2;
		}
		if (/^(\/private)?\/tmp\//.test(dir) || /^\/private\/var\/folders\//.test(dir) || /^\/var\/folders\//.test(dir)) {
			return 4;
		}
		return 3;
	}

	function integrateLocalProjectPickers(root) {
		for (const picker of localProjectPickerCandidates(root)) {
			if (!localProjectPickerLooksLikeProjectPicker(picker)) {
				continue;
			}
			const list = commandPaletteList(picker);
			if (list) {
				installLocalProjectPickerKeyboardNavigation(picker, list);
				installLocalProjectPickerSearch(picker, list);
				installLocalProjectNoProjectSelectionHandler(picker);
				autoSelectCurrentProjectInPicker(picker, list);
			}
			if (!list || list.querySelector("[data-cliproxy-local-project-item]") || list.dataset.cliproxyLocalProjectLoading === "1") {
				continue;
			}
			list.dataset.cliproxyLocalProjectLoading = "1";
			const generation = localProjectIntegrationGeneration;
			fetchLocalProjects(true).then((projects) => {
				if (generation !== localProjectIntegrationGeneration) {
					return;
				}
				delete list.dataset.cliproxyLocalProjectLoading;
				installLocalProjectNoProjectSelectionHandler(picker);
				const displayProjects = localProjectPickerDisplayProjects(projects);
				if (!projects.length || !globalThis.document.contains(picker) || !localProjectPickerLooksLikeProjectPicker(picker)) {
					const attempts = Number(list.dataset.cliproxyLocalProjectAttempts || "0");
					if (attempts < 3 && globalThis.document.contains(picker)) {
						list.dataset.cliproxyLocalProjectAttempts = String(attempts + 1);
						setTimeout(() => {
							if (generation === localProjectIntegrationGeneration) {
								integrateLocalProjectPickers(picker);
							}
						}, 750 * (attempts + 1));
					}
					return;
				}
				if (!displayProjects.length) {
					return;
				}
				delete list.dataset.cliproxyLocalProjectAttempts;
				injectLocalProjectPickerItems(picker, list, displayProjects);
				filterLocalProjectPickerItems(picker, list);
				autoSelectCurrentProjectInPicker(picker, list, true);
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
		if (!commandPaletteLooksLikePalette(picker)) {
			return false;
		}
		const text = (picker.innerText || picker.textContent || "").replace(/\s+/g, " ").trim();
		return !!localProjectPickerNoProjectItem(picker) && /\bProjects?\b/.test(text);
	}

	function injectLocalProjectPickerItems(picker, list, projects) {
		if (list.querySelector("[data-cliproxy-local-project-item]")) {
			return;
		}
		const before = localProjectPickerActionsRow(list);
		const currentDirectory = localProjectPickerCurrentDirectory(projects);
		for (const project of projects) {
			list.insertBefore(buildLocalProjectPickerItem(picker, project, currentDirectory), before);
			diagnostics.localProjectPickerIntegrationCount += 1;
		}
	}

	function installLocalProjectPickerSearch(picker, list) {
		const target = localProjectPickerSearchTarget(picker);
		if (!target || target.dataset.cliproxyLocalProjectSearch === "1") {
			return;
		}
		target.dataset.cliproxyLocalProjectSearch = "1";
		const apply = () => {
			filterLocalProjectPickerItems(picker, list);
			setTimeout(() => filterLocalProjectPickerItems(picker, list), 0);
		};
		target.addEventListener("input", apply, true);
		target.addEventListener("change", apply, true);
	}

	function localProjectPickerSearchTarget(picker) {
		return picker.querySelector('input,[role="combobox"],textarea,[contenteditable="true"]');
	}

	function localProjectPickerSearchQuery(picker) {
		const target = localProjectPickerSearchTarget(picker);
		if (!target) {
			return "";
		}
		if ("value" in target) {
			return normalizeProjectPickerName(target.value);
		}
		return normalizeProjectPickerName(target.textContent || "");
	}

	function filterLocalProjectPickerItems(picker, list) {
		const query = localProjectPickerSearchQuery(picker);
		const visible = [];
		for (const item of list.querySelectorAll("[data-cliproxy-local-project-item]")) {
			const searchText = normalizeProjectPickerName(item.dataset.cliproxyLocalProjectSearchText);
			const matches = !query || searchText.includes(query);
			item.hidden = !matches;
			item.style.display = matches ? "flex" : "none";
			item.setAttribute("aria-hidden", matches ? "false" : "true");
			if (matches) {
				visible.push(item);
			}
		}
		if (!query || !visible.length) {
			return;
		}
		const selected = visible.find((item) => normalizeProjectPickerName(item.dataset.cliproxyLocalProjectLabel) === query) || visible[0];
		setProjectPickerSelectedItem(list, selected);
	}

	function buildLocalProjectPickerItem(picker, project, currentDirectory = currentLocalProjectWorkingDirectory()) {
		const template = localProjectPickerTemplateItem(picker);
		const item = template && typeof template.cloneNode === "function" ? template.cloneNode(false) : globalThis.document.createElement("button");
		const workingDirectory = normalizeWorkingDirectory(project.workingDirectory);
		const displayPath = localProjectPickerPathDisplay(workingDirectory);
		const label = firstString(project.name, pathBaseName(workingDirectory), "local");
		const accessibleLabel = [label, "Local", displayPath].filter(Boolean).join(", ");
		const isCurrent = workingDirectory === normalizeWorkingDirectory(currentDirectory);
		if ("type" in item) {
			item.type = "button";
		}
		item.removeAttribute("id");
		item.dataset.cliproxyLocalProjectItem = "1";
		item.dataset.cliproxyLocalProjectLabel = label;
		item.dataset.cliproxyLocalProjectWorkingDirectory = workingDirectory;
		item.dataset.cliproxyLocalProjectSearchText = [
			label,
			firstString(project.namespace),
			firstString(project.repositoryURL, project.repoURL),
			workingDirectory,
			displayPath,
			"local",
		].filter(Boolean).join(" ");
		item.dataset.cliproxyLocalProjectCurrent = isCurrent ? "1" : "0";
		item.setAttribute("data-value", accessibleLabel);
		item.setAttribute("aria-label", accessibleLabel);
		item.setAttribute("role", "option");
		item.setAttribute("aria-selected", "false");
		item.setAttribute("aria-disabled", "false");
		item.setAttribute("data-disabled", "false");
		item.removeAttribute("disabled");
		item.dataset.selected = "false";
		item.className = template?.className || "group flex w-full gap-3 rounded-xl px-3.5 py-2.5 text-left hover:bg-black/5 data-[selected=true]:bg-black/6 dark:hover:bg-white/8 dark:data-[selected=true]:bg-white/10 items-center";
		applyLocalProjectPickerItemFrame(item);
		fillLocalProjectPickerItem(item, label, workingDirectory, isCurrent);
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

	function applyLocalProjectPickerItemFrame(item) {
		item.style.display = "flex";
		item.style.alignItems = "center";
		item.style.gap = "12px";
		item.style.width = "100%%";
		item.style.minHeight = "44px";
		item.style.padding = "10px 14px";
		item.style.boxSizing = "border-box";
		item.style.border = "0";
		item.style.font = "inherit";
		item.style.textAlign = "left";
	}

	function fillLocalProjectPickerItem(item, labelText, workingDirectory, isCurrent) {
		item.replaceChildren();
		const left = globalThis.document.createElement("div");
		left.className = "min-w-0 flex-1";
		left.style.cssText = "min-width:0;flex:1 1 auto";
		const title = globalThis.document.createElement("div");
		title.className = "flex min-w-0 gap-1.5 text-lg pointer-coarse:text-[15px] pointer-coarse:leading-snug font-medium items-baseline";
		title.style.cssText = "min-width:0;font-size:16px;line-height:24px;font-weight:500";
		const label = globalThis.document.createElement("span");
		label.textContent = labelText;
		label.className = "min-w-0 truncate";
		label.style.cssText = "display:block;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap";
		title.append(label);
		left.append(title);

		const right = globalThis.document.createElement("div");
		right.className = "flex shrink-0 items-center gap-2 text-muted-foreground";
		right.style.cssText = "display:flex;align-items:center;gap:8px;min-width:0;flex:0 1 auto";
		const meta = globalThis.document.createElement("span");
		meta.className = "flex max-w-[min(45vw,32rem)] min-w-0 items-center gap-1.5 text-sm font-medium";
		meta.style.cssText = "display:flex;align-items:center;gap:6px;min-width:0;max-width:min(45vw,32rem);font-size:12px;line-height:16px;font-weight:500";
		const badge = globalThis.document.createElement("span");
		badge.textContent = "Local";
		badge.className = "shrink-0 rounded-md bg-foreground/8 px-1.5 py-0.5 text-[0.65rem] leading-none font-medium text-muted-foreground";
		badge.style.cssText = "flex:0 0 auto;border-radius:6px;padding:2px 6px;background:color-mix(in srgb,currentColor 10%%,transparent);font-size:10.4px;line-height:1;font-weight:500";
		const detail = globalThis.document.createElement("span");
		detail.textContent = localProjectPickerPathDisplay(workingDirectory);
		detail.title = workingDirectory;
		detail.className = "truncate";
		detail.style.cssText = "display:block;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap";
		const check = globalThis.document.createElement("span");
		check.className = "flex size-4 shrink-0 items-center justify-center";
		check.dataset.slot = "project-check";
		check.textContent = isCurrent ? "\u2713" : "";
		check.setAttribute("aria-hidden", "true");
		check.style.cssText = "display:flex;align-items:center;justify-content:center;width:16px;height:16px;flex:0 0 auto;font-size:12px;line-height:1";
		meta.append(badge, detail);
		right.append(meta, check);
		item.append(left, right);
	}

	function localProjectPickerTemplateItem(picker) {
		const list = commandPaletteList(picker) || picker;
		let noProject = null;
		for (const element of list.querySelectorAll('[cmdk-item],[data-cmdk-item],[data-slot="command-item"],[role="option"],button,[role="button"]')) {
			const text = projectPickerItemPrimaryText(element);
			if (element.dataset?.cliproxyLocalProjectItem || /^(Actions|Create Project)\b/.test(text)) {
				continue;
			}
			if (/^No Project\b/.test(text)) {
				noProject = element;
				continue;
			}
			return element;
		}
		return noProject;
	}

	function localProjectPickerPathDisplay(workingDirectory) {
		const normalized = normalizeWorkingDirectory(workingDirectory);
		if (normalized && normalized === defaultLocalWorkingDirectory() && /^\/Users\/[^/]+$/.test(normalized)) {
			return "~";
		}
		if (normalized.startsWith("/Users/")) {
			const parts = normalized.split("/");
			if (parts.length > 3) {
				return "~/" + parts.slice(3).join("/");
			}
		}
		return normalized;
	}

	function localProjectPickerDisplayProjects(projects) {
		const visibleName = visibleProjectName();
		const activeDirectory = activeThreadWorkingDirectory();
		const visibleDirectory = normalizeWorkingDirectory(localProjectByVisibleName(projects, visibleName)?.workingDirectory);
		const selectedDirectory = selectedLocalProjectWorkingDirectory();
		const fallbackDirectory = normalizeWorkingDirectory(globalThis.localStorage.getItem(workingDirectoryStorageKey) || "");
		const rank = (project) => {
			const dir = normalizeWorkingDirectory(project.workingDirectory);
			if (activeDirectory && dir === activeDirectory) {
				return 0;
			}
			if (visibleDirectory && dir === visibleDirectory) {
				return 1;
			}
			if (selectedDirectory && dir === selectedDirectory) {
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

	function localProjectPickerCurrentDirectory(projects) {
		const selectedProject = selectedLocalProject();
		const selectedDirectory = normalizeWorkingDirectory(selectedProject?.workingDirectory);
		if (localProjectIsNoProject(selectedProject)) {
			return selectedDirectory;
		}
		const activeDirectory = activeThreadWorkingDirectory();
		if (activeDirectory) {
			return activeDirectory;
		}
		const visibleDirectory = normalizeWorkingDirectory(localProjectByVisibleName(projects)?.workingDirectory);
		if (visibleDirectory) {
			return visibleDirectory;
		}
		return selectedLocalProjectWorkingDirectory() ||
			normalizeWorkingDirectory(globalThis.localStorage.getItem(workingDirectoryStorageKey) || "") ||
			defaultLocalWorkingDirectory();
	}

	function normalizeProjectPickerName(value) {
		return String(value || "").replace(/\s+/g, " ").trim().toLowerCase();
	}

	function installLocalProjectNoProjectSelectionHandler(picker) {
		const noProject = localProjectPickerNoProjectItem(picker);
		if (!noProject) {
			return;
		}
		decorateLocalProjectNoProjectItem(noProject, defaultLocalWorkingDirectory());
		if (noProject.dataset.cliproxyNoProjectHandler === "1") {
			return;
		}
		noProject.dataset.cliproxyNoProjectHandler = "1";
		const clear = () => {
			if (noProject.dataset.cliproxySuppressLocalProjectClear === "1") {
				return;
			}
			const workingDirectory = defaultLocalWorkingDirectory();
			if (!workingDirectory) {
				clearSelectedLocalProject(true);
				return;
			}
			const project = { name: "No Project", workingDirectory };
			rememberSelectedLocalProject(project);
			const list = commandPaletteList(picker);
			if (list) {
				setProjectPickerSelectedItem(list, noProject);
				setLocalProjectPickerCurrentDirectory(list, workingDirectory);
			}
			refreshLocalProjectActivators(project, picker, true);
			setTimeout(() => refreshLocalProjectActivators(project, picker, true), 50);
			refreshCreateThreadProjectActivatorsAfterSelection(project);
		};
		noProject.addEventListener("pointerdown", clear, true);
		noProject.addEventListener("click", clear, true);
	}

	function decorateLocalProjectNoProjectItem(noProject, workingDirectory) {
		workingDirectory = normalizeWorkingDirectory(workingDirectory);
		if (!workingDirectory) {
			return;
		}
		const check = noProject.querySelector('[data-slot="project-check"]');
		const right = check?.parentElement;
		if (!right) {
			return;
		}
		let detail = right.querySelector("[data-cliproxy-no-project-path]");
		if (!detail) {
			detail = globalThis.document.createElement("span");
			detail.dataset.cliproxyNoProjectPath = "1";
			detail.className = "shrink-0 text-sm font-medium text-muted-foreground";
			detail.style.cssText = "flex:0 0 auto;font-size:12px;line-height:16px;font-weight:500";
			right.insertBefore(detail, check);
		}
		detail.textContent = localProjectPickerPathDisplay(workingDirectory);
		detail.title = workingDirectory;
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
			element.hidden !== true &&
			element.style?.display !== "none" &&
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

	function setLocalProjectPickerCurrentDirectory(list, workingDirectory) {
		const currentDirectory = normalizeWorkingDirectory(workingDirectory);
		for (const item of list.querySelectorAll("[data-cliproxy-local-project-item]")) {
			const isCurrent = normalizeWorkingDirectory(item.dataset.cliproxyLocalProjectWorkingDirectory) === currentDirectory;
			item.dataset.cliproxyLocalProjectCurrent = isCurrent ? "1" : "0";
			const check = item.querySelector('[data-slot="project-check"]');
			if (check) {
				check.textContent = isCurrent ? "✓" : "";
			}
		}
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
		const list = commandPaletteList(picker);
		if (list) {
			setProjectPickerSelectedItem(list, item);
			setLocalProjectPickerCurrentDirectory(list, workingDirectory);
		}
		refreshLocalProjectActivators(selectedProject, picker, true);
		refreshCreateThreadProjectActivatorsAfterSelection(selectedProject);
		setTimeout(() => {
			closeLocalProjectPickerViaNoProject(picker);
			setTimeout(() => refreshLocalProjectActivators(selectedProject, picker, true), 50);
		}, 0);
	}

	function autoSelectCurrentProjectInPicker(picker, list, force = false) {
		if (!list || (!force && list.dataset.cliproxyCurrentProjectAutoSelected === "1")) {
			return;
		}
		const item = currentProjectPickerItem(picker);
		if (!item || projectPickerItemSelected(item)) {
			return;
		}
		list.dataset.cliproxyCurrentProjectAutoSelected = "1";
		setTimeout(() => {
			if (!globalThis.document.contains(item) || !localProjectPickerLooksLikeProjectPicker(picker)) {
				return;
			}
			setProjectPickerSelectedItem(list, item);
			const workingDirectory = normalizeWorkingDirectory(item.dataset?.cliproxyLocalProjectWorkingDirectory);
			if (workingDirectory) {
				setLocalProjectPickerCurrentDirectory(list, workingDirectory);
				const selectedProject = {
					name: projectPickerItemPrimaryText(item),
					workingDirectory,
				};
				rememberSelectedLocalProject(selectedProject);
				refreshLocalProjectActivators(selectedProject, picker, true);
			}
		}, 0);
	}

	function projectPickerItemSelected(item) {
		return item?.getAttribute("aria-selected") === "true" || item?.dataset?.selected === "true";
	}

	function currentProjectPickerItem(picker) {
		const selectedProject = selectedLocalProject();
		const workingDirectory = localProjectPickerCurrentDirectory(localProjectsCache.projects);
		const visibleName = visibleProjectName();
		if (localProjectIsNoProject(selectedProject) ||
			(!visibleName && workingDirectory === defaultLocalWorkingDirectory())) {
			return localProjectPickerNoProjectItem(picker);
		}
		const targetNames = new Set();
		if (workingDirectory) {
			targetNames.add(normalizeProjectPickerName(pathBaseName(workingDirectory)));
		}
		const visibleTargetName = normalizeProjectPickerName(visibleName);
		if (visibleName) {
			targetNames.add(visibleTargetName);
		}
		const matches = [];
		const localNameMatches = [];
		let localDirectoryMatch = null;
		for (const element of picker.querySelectorAll('[cmdk-item],[data-cmdk-item],[data-slot="command-item"],[role="option"],button,[role="button"]')) {
			if (!elementVisible(element) || /^No Project\b/.test(projectPickerItemPrimaryText(element))) {
				continue;
			}
			const itemName = normalizeProjectPickerName(projectPickerItemPrimaryText(element));
			if (element.dataset?.cliproxyLocalProjectItem) {
				if (visibleTargetName && itemName === visibleTargetName) {
					localNameMatches.push(element);
				}
				if (normalizeWorkingDirectory(element.dataset.cliproxyLocalProjectWorkingDirectory) === workingDirectory && !localDirectoryMatch) {
					localDirectoryMatch = element;
				}
				continue;
			}
			if (targetNames.has(itemName)) {
				matches.push(element);
			}
		}
		if (localDirectoryMatch) {
			return localDirectoryMatch;
		}
		if (localNameMatches.length === 1) {
			return localNameMatches[0];
		}
		return matches.length === 1 ? matches[0] : null;
	}

	function projectPickerItemPrimaryText(element) {
		if (element?.dataset?.cliproxyLocalProjectLabel) {
			return element.dataset.cliproxyLocalProjectLabel;
		}
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

	function localActivitySection() {
		if (globalThis.location.pathname !== "/feed") {
			return null;
		}
		for (const section of globalThis.document.querySelectorAll("main section")) {
			const heading = Array.from(section.children).find((child) => child.textContent?.trim() === "Threads" || child.firstElementChild?.textContent?.trim() === "Threads");
			if (heading) {
				return { section, heading };
			}
		}
		return null;
	}

	function localActivityThreadTime(thread) {
		for (const value of [thread?.updatedAt, thread?.userLastInteractedAt, thread?.createdAt, thread?.created]) {
			const timestamp = typeof value === "number" ? value : Date.parse(String(value || ""));
			if (!Number.isFinite(timestamp) || timestamp <= 0) {
				continue;
			}
			const seconds = Math.max(0, Math.floor((Date.now() - timestamp) / 1000));
			if (seconds < 60) return "now";
			if (seconds < 3600) return Math.floor(seconds / 60) + "m ago";
			if (seconds < 86400) return Math.floor(seconds / 3600) + "h ago";
			if (seconds < 604800) return Math.floor(seconds / 86400) + "d ago";
			return Math.floor(seconds / 604800) + "w ago";
		}
		return "recently";
	}

	function buildLocalActivityThread(thread) {
		const threadID = firstString(thread?.id, thread?.threadId, thread?.threadID);
		if (!validThreadID(threadID)) {
			return null;
		}
		const anchor = globalThis.document.createElement("a");
		anchor.href = "/threads/" + encodeURIComponent(threadID);
		anchor.className = "flex min-w-0 items-start gap-2.5 border-b px-3 py-2 text-sm last:border-b-0 hover:bg-accent/50";
		anchor.dataset.cliproxyLocalActivityThread = threadID;
		anchor.dataset.cliproxyLocalActivityVersion = userscriptVersion;
		anchor.dataset.cliproxyLocalActivitySignature = JSON.stringify([
			threadID,
			thread.title,
			thread.archived,
			thread.updatedAt,
			thread.userLastInteractedAt,
			thread.creator,
			localSidebarProjectName(thread),
		]);
		anchor.setAttribute("data-sveltekit-keepfocus", "");
		const avatarWrap = globalThis.document.createElement("div");
		avatarWrap.className = "mt-0.5 size-5 shrink-0";
		const creator = isPlainObject(thread.creator) ? thread.creator : {};
		const pictureURL = firstString(creator.profilePictureUrl);
		const avatar = globalThis.document.createElement(pictureURL ? "img" : "span");
		avatar.className = pictureURL ?
			"size-5 rounded-full border object-cover" :
			"flex size-5 items-center justify-center rounded-full border bg-muted text-[10px] font-medium text-muted-foreground";
		if (pictureURL) {
			avatar.src = pictureURL;
			avatar.alt = firstString(creator.firstName, creator.username, "User");
		} else {
			avatar.textContent = firstString(creator.firstName, creator.email, creator.username, "?").trim().slice(0, 1).toUpperCase();
		}
		avatarWrap.appendChild(avatar);
		const content = globalThis.document.createElement("div");
		content.className = "min-w-0 flex-1 space-y-1";
		const titleRow = globalThis.document.createElement("div");
		titleRow.className = "flex min-w-0 items-center gap-1.5";
		const title = globalThis.document.createElement("div");
		title.className = "truncate font-medium text-foreground";
		title.textContent = firstString(thread.title, "Untitled");
		titleRow.appendChild(title);
		if (thread.archived === true) {
			const archived = globalThis.document.createElement("span");
			archived.className = "inline-flex shrink-0 items-center rounded-xs border border-transparent bg-muted px-1.25 py-0.5 text-xs text-muted-foreground";
			archived.textContent = "Archived";
			titleRow.appendChild(archived);
		}
		const meta = globalThis.document.createElement("div");
		meta.className = "flex min-w-0 flex-wrap items-center gap-x-1.5 gap-y-0.5 text-xs text-muted-foreground";
		const values = [firstString(creator.username, creator.firstName, creator.email, "User"), localActivityThreadTime(thread)];
		const projectName = localSidebarProjectName(thread);
		if (projectName) {
			values.push(projectName);
		}
		values.forEach((value, index) => {
			if (index > 0) {
				const separator = globalThis.document.createElement("span");
				separator.textContent = "·";
				meta.appendChild(separator);
			}
			const item = globalThis.document.createElement("span");
			item.className = "truncate";
			item.textContent = value;
			meta.appendChild(item);
		});
		content.append(titleRow, meta);
		anchor.append(avatarWrap, content);
		return anchor;
	}

	function localActivityFilterGroup(label) {
		const aside = globalThis.document.querySelector("main aside");
		if (!aside) {
			return null;
		}
		for (const group of aside.children) {
			const heading = group.firstElementChild;
			if (heading?.textContent?.trim() !== label) {
				continue;
			}
			const border = Array.from(group.children).find((child) => child.classList?.contains("border"));
			const options = border ? Array.from(border.children).find((child) => child.classList?.contains("overflow-y-auto")) : null;
			const all = border ? Array.from(border.children).find((child) => child.tagName === "A") : null;
			return border && options && all ? { all, options } : null;
		}
		return null;
	}

	function localActivityFilterHref(key, value) {
		const url = new URL(globalThis.location.href);
		url.pathname = "/feed";
		url.hash = "";
		url.searchParams.delete("offset");
		url.searchParams.delete(key + "Pinned");
		if (value) {
			url.searchParams.set(key, value);
		} else {
			url.searchParams.delete(key);
		}
		return url.pathname + (url.searchParams.size > 0 ? "?" + url.searchParams.toString() : "");
	}

	function localActivityFilterOptionKey(anchor, key) {
		try {
			return new URL(anchor.href, globalThis.location.href).searchParams.get(key) || "";
		} catch {
			return "";
		}
	}

	function updateLocalActivityFilterCount(anchor, localCount, integrate) {
		const count = Array.from(anchor.querySelectorAll("span")).at(-1);
		if (!count) {
			return;
		}
		if (!integrate) {
			delete anchor.dataset.cliproxyLocalActivityCount;
			return;
		}
		const previous = Number(anchor.dataset.cliproxyLocalActivityCount || 0);
		const displayed = Number((count.textContent || "").trim());
		const base = Number.isFinite(displayed) ? Math.max(0, displayed - previous) : 0;
		const next = Math.max(0, Number(localCount || 0));
		const text = String(base + next);
		const changed = (count.textContent || "").trim() !== text || anchor.dataset.cliproxyLocalActivityCount !== String(next);
		if ((count.textContent || "").trim() !== text) {
			count.textContent = text;
		}
		anchor.dataset.cliproxyLocalActivityCount = String(next);
		if (changed) {
			diagnostics.localActivityDOMFilterIntegrationCount += 1;
		}
	}

	function buildLocalActivityFilterOption(key, option, label) {
		const value = firstString(key === "repo" ? option?.key : key === "user" ? option?.id : option?.[key]);
		if (!value) {
			return null;
		}
		const anchor = globalThis.document.createElement("a");
		anchor.href = localActivityFilterHref(key, value);
		anchor.className = "flex items-center justify-between gap-2 px-2 py-1.5 hover:bg-accent/50 data-[active=true]:bg-accent data-[active=true]:text-accent-foreground";
		anchor.dataset.cliproxyLocalActivityFilter = key + ":" + value;
		anchor.dataset.active = new URL(globalThis.location.href).searchParams.get(key) === value ? "true" : "false";
		const name = globalThis.document.createElement("span");
		name.className = "truncate";
		name.textContent = label;
		const count = globalThis.document.createElement("span");
		count.className = "shrink-0 text-xs text-muted-foreground";
		count.textContent = String(Math.max(0, Number(option?.count || 0)));
		anchor.append(name, count);
		return anchor;
	}

	function updateInjectedLocalActivityFilterOption(anchor, key, option, label) {
		const value = firstString(key === "repo" ? option?.key : key === "user" ? option?.id : option?.[key]);
		const href = localActivityFilterHref(key, value);
		if (anchor.getAttribute("href") !== href) {
			anchor.setAttribute("href", href);
		}
		anchor.dataset.active = new URL(globalThis.location.href).searchParams.get(key) === value ? "true" : "false";
		const spans = Array.from(anchor.querySelectorAll("span"));
		if (spans[0] && spans[0].textContent !== label) {
			spans[0].textContent = label;
		}
		const count = String(Math.max(0, Number(option?.count || 0)));
		if (spans.at(-1) && spans.at(-1).textContent !== count) {
			spans.at(-1).textContent = count;
		}
	}

	function renderLocalActivityFilters(activity, nativeThreadIDs) {
		const repositoryGroup = localActivityFilterGroup("Repositories");
		const userGroup = localActivityFilterGroup("Users");
		if (!repositoryGroup || !userGroup) {
			return;
		}
		const repositories = Array.isArray(activity?.repositories) ? activity.repositories : [];
		const users = Array.isArray(activity?.users) ? activity.users : [];
		const injected = new Map(Array.from(globalThis.document.querySelectorAll("[data-cliproxy-local-activity-filter]")).map((anchor) => [anchor.dataset.cliproxyLocalActivityFilter, anchor]));
		const nativeRepositories = new Map(Array.from(repositoryGroup.options.querySelectorAll("a")).filter((anchor) => !anchor.dataset.cliproxyLocalActivityFilter).map((anchor) => [localActivityFilterOptionKey(anchor, "repo").toLowerCase(), anchor]).filter(([key]) => key));
		const localThreadIDs = (Array.isArray(activity?.threads) ? activity.threads : []).map((thread) => firstString(thread?.id, thread?.threadId, thread?.threadID)).filter(validThreadID);
		const localThreadOverlapCount = localThreadIDs.filter((threadID) => nativeThreadIDs.has(threadID)).length;
		const nativeHasLocalActivity = localThreadIDs.length > 0 && localThreadIDs.every((threadID) => nativeThreadIDs.has(threadID));
		updateLocalActivityFilterCount(repositoryGroup.all, Math.max(0, Number(activity?.repositoryTotalThreadCount || 0) - localThreadOverlapCount), !nativeHasLocalActivity);
		updateLocalActivityFilterCount(userGroup.all, Math.max(0, Number(activity?.userTotalThreadCount || 0) - localThreadOverlapCount), !nativeHasLocalActivity);
		const repositoryOverlapCounts = new Map();
		for (const thread of Array.isArray(activity?.threads) ? activity.threads : []) {
			const threadID = firstString(thread?.id, thread?.threadId, thread?.threadID);
			const repositoryKey = firstString(thread?.repositoryKey).toLowerCase();
			if (repositoryKey && nativeThreadIDs.has(threadID)) {
				repositoryOverlapCounts.set(repositoryKey, (repositoryOverlapCounts.get(repositoryKey) || 0) + 1);
			}
		}
		const desired = new Set();
		for (const repository of repositories) {
			const key = firstString(repository?.key);
			if (!key) {
				continue;
			}
			const normalizedKey = key.toLowerCase();
			const nativeRepository = nativeRepositories.get(normalizedKey);
			if (nativeRepository) {
				if (!nativeHasLocalActivity) {
					updateLocalActivityFilterCount(nativeRepository, Math.max(0, Number(repository?.count || 0) - Number(repositoryOverlapCounts.get(normalizedKey) || 0)), true);
				}
				continue;
			}
			const signature = "repo:" + key;
			desired.add(signature);
			const label = firstString(repository?.ownerPrefix) + firstString(repository?.name, key);
			const existing = injected.get(signature);
			if (existing) {
				updateInjectedLocalActivityFilterOption(existing, "repo", repository, label);
			} else {
				const anchor = buildLocalActivityFilterOption("repo", repository, label);
				if (anchor) {
					repositoryGroup.options.appendChild(anchor);
				}
			}
		}
		const nativeUsers = new Map(Array.from(userGroup.options.querySelectorAll("a")).filter((anchor) => !anchor.dataset.cliproxyLocalActivityFilter).map((anchor) => [localActivityFilterOptionKey(anchor, "user"), anchor]).filter(([key]) => key));
		for (const user of users) {
			const userID = firstString(user?.id);
			const existing = nativeUsers.get(userID);
			if (existing) {
				updateLocalActivityFilterCount(existing, Math.max(0, Number(user?.count || 0) - localThreadOverlapCount), !nativeHasLocalActivity);
				continue;
			}
			const signature = "user:" + userID;
			desired.add(signature);
			const profile = isPlainObject(activity?.usersMap?.[userID]) ? activity.usersMap[userID] : {};
			const label = firstString(user?.name, profile.username, profile.firstName, profile.email, userID);
			const injectedUser = injected.get(signature);
			if (injectedUser) {
				updateInjectedLocalActivityFilterOption(injectedUser, "user", user, label);
			} else {
				const anchor = buildLocalActivityFilterOption("user", user, label);
				if (anchor) {
					userGroup.options.appendChild(anchor);
				}
			}
		}
		for (const [signature, anchor] of injected) {
			if (!desired.has(signature)) {
				anchor.remove();
			}
		}
	}

	function renderLocalActivity(activity) {
		const found = localActivitySection();
		if (!found) {
			return;
		}
		const { section, heading } = found;
		const currentItems = Array.from(section.querySelectorAll("[data-cliproxy-local-activity-thread]"));
		const nativeThreadIDs = new Set(Array.from(section.querySelectorAll('a[href^="/threads/"]')).filter((anchor) => !anchor.dataset.cliproxyLocalActivityThread).map((anchor) => decodedThreadID(anchor.getAttribute("href").split("/")[2] || "")).filter(Boolean));
		renderLocalActivityFilters(activity, nativeThreadIDs);
		const threads = Array.isArray(activity?.threads) ? activity.threads.filter((thread) => {
			const threadID = firstString(thread?.id, thread?.threadId, thread?.threadID);
			return validThreadID(threadID) && !nativeThreadIDs.has(threadID);
		}) : [];
		const desiredThreadIDs = threads.map((thread) => firstString(thread?.id, thread?.threadId, thread?.threadID));
		const currentThreadIDs = currentItems.map((item) => item.dataset.cliproxyLocalActivityThread);
		const desiredSignatures = threads.map((thread) => JSON.stringify([
			firstString(thread?.id, thread?.threadId, thread?.threadID),
			thread?.title,
			thread?.archived,
			thread?.updatedAt,
			thread?.userLastInteractedAt,
			thread?.creator,
			localSidebarProjectName(thread),
		]));
		const currentSignatures = currentItems.map((item) => item.dataset.cliproxyLocalActivitySignature || "");
		if (desiredThreadIDs.length === currentThreadIDs.length && desiredThreadIDs.every((threadID, index) => threadID === currentThreadIDs[index] && desiredSignatures[index] === currentSignatures[index])) {
			return;
		}
		currentItems.forEach((element) => element.remove());
		const emptyHeading = Array.from(section.querySelectorAll("h1,h2,h3")).find((element) => element.textContent?.trim() === "No Threads");
		if (threads.length === 0) {
			if (emptyHeading?.dataset.cliproxyLocalActivityHidden === "1") {
				emptyHeading.style.removeProperty("display");
				delete emptyHeading.dataset.cliproxyLocalActivityHidden;
			}
			section.querySelector("[data-cliproxy-local-activity-list]")?.remove();
			return;
		}
		if (emptyHeading) {
			emptyHeading.style.display = "none";
			emptyHeading.dataset.cliproxyLocalActivityHidden = "1";
		}
		let list = section.querySelector("div.overflow-hidden.rounded-md.border");
		if (!list) {
			list = globalThis.document.createElement("div");
			list.className = "overflow-hidden rounded-md border";
			list.dataset.cliproxyLocalActivityList = "1";
			heading.insertAdjacentElement("afterend", list);
		}
		const localItems = globalThis.document.createDocumentFragment();
		for (const thread of threads) {
			const item = buildLocalActivityThread(thread);
			if (!item) {
				continue;
			}
			rememberLocalThreadID(item.dataset.cliproxyLocalActivityThread);
			localItems.appendChild(item);
			diagnostics.localActivityDOMIntegrationCount += 1;
		}
		list.insertBefore(localItems, list.firstChild);
	}

	function scheduleLocalActivityRefresh() {
		if (localActivityDOMRefreshPending) {
			return;
		}
		localActivityDOMRefreshPending = true;
		globalThis.setTimeout(() => {
			localActivityDOMRefreshPending = false;
			if (globalThis.location.pathname !== "/feed") {
				observeLocalActivitySection(globalThis.__cliproxyAmpLocalActivityObserver);
				return;
			}
			observeLocalActivitySection(globalThis.__cliproxyAmpLocalActivityObserver);
			fetchLocalActivity(globalThis.location.href).then((activity) => {
				renderLocalActivity(activity);
				observeLocalActivitySection(globalThis.__cliproxyAmpLocalActivityObserver);
			}, () => undefined);
		}, 0);
	}

	function renderLocalSidebarMetadata() {
		const threadsByID = new Map();
		if (authenticatedAmpUserID) {
			for (const [threadID, title] of Object.entries(localSidebarTitleCache)) {
				if (validThreadID(threadID) && typeof title === "string" && title.trim() && title.trim().toLowerCase() !== "untitled") {
					threadsByID.set(threadID, title.trim());
				}
			}
		}
		for (const thread of localProjectsCache.threads || []) {
			const threadID = firstString(thread?.id, thread?.threadId, thread?.threadID);
			const title = firstString(thread?.title);
			if (validThreadID(threadID) && title && title.toLowerCase() !== "untitled") {
				threadsByID.set(threadID, title);
			}
		}
		if (localProjectsCache.at > 0) {
			for (const [threadID, title] of Object.entries(localProjectsCache.threadTitles || {})) {
				if (validThreadID(threadID) && typeof title === "string" && title.trim() && title.trim().toLowerCase() !== "untitled") {
					threadsByID.set(threadID, title.trim());
				}
			}
		}
		if (threadsByID.size === 0) {
			return;
		}
		for (const anchor of globalThis.document.querySelectorAll("[data-sidebar-thread-id]")) {
			const title = threadsByID.get(firstString(anchor?.dataset?.sidebarThreadId));
			if (!title) {
				continue;
			}
			const titleElement = localSidebarTitleElement(anchor);
			if (!titleElement || (titleElement.textContent || "").trim() === title) {
				continue;
			}
			titleElement.textContent = title;
			diagnostics.localSidebarTitlePatchCount += 1;
		}
	}

	function localSidebarTitleElement(anchor) {
		if (!(anchor instanceof Element)) {
			return null;
		}
		return Array.from(anchor.querySelectorAll("span")).find((span) =>
			!span.closest?.("button") &&
			!span.closest?.('[role="img"]') &&
			!span.closest?.("[data-thread-row-actions]") &&
			(span.textContent || "").trim()
		) || null;
	}

	function reconcileActiveLocalThreadTitle() {
		const threadID = activeLocalThreadID();
		if (!threadID) {
			return;
		}
		const cachedThread = [localProjectsCache.thread, ...(localProjectsCache.threads || [])].find((thread) =>
			isPlainObject(thread) && firstString(thread?.id, thread?.threadId, thread?.threadID) === threadID
		);
		const authoritativeTitle = firstString(cachedThread?.title, localProjectsCache.at > 0 ? localProjectsCache.threadTitles?.[threadID] : "");
		if (!authoritativeTitle || authoritativeTitle.toLowerCase() === "untitled") {
			return;
		}
		const anchor = Array.from(globalThis.document.querySelectorAll("[data-sidebar-thread-id]")).find((element) => firstString(element?.dataset?.sidebarThreadId) === threadID);
		const sidebarTitle = localSidebarTitleElement(anchor);
		if (sidebarTitle && (sidebarTitle.textContent || "").trim() !== authoritativeTitle) {
			sidebarTitle.textContent = authoritativeTitle;
			diagnostics.localSidebarTitlePatchCount += 1;
		}
	}

	function reconcileActiveLocalThreadArchiveBadge() {
		if (activeLocalArchiveState(activeThreadID()) !== false) {
			return;
		}
		const titleBar = globalThis.document?.querySelector?.(".thread-title-bar-container .app-title-bar");
		if (!titleBar) {
			return;
		}
		for (const badge of titleBar.querySelectorAll?.("span") || []) {
			if (badge.children.length !== 0 || (badge.textContent || "").trim() !== "Archived" || typeof badge.remove !== "function") {
				continue;
			}
			badge.remove();
			diagnostics.localThreadArchiveBadgePatchCount += 1;
		}
	}

	function localSidebarUntitledThreadIDs() {
		const threadIDs = [];
		const seen = new Set();
		for (const anchor of globalThis.document.querySelectorAll("[data-sidebar-thread-id]")) {
			const threadID = firstString(anchor?.dataset?.sidebarThreadId);
			if (!validThreadID(threadID) || seen.has(threadID) || (localSidebarTitleElement(anchor)?.textContent || "").trim().toLowerCase() !== "untitled") {
				continue;
			}
			seen.add(threadID);
			threadIDs.push(threadID);
			if (threadIDs.length >= 75) {
				break;
			}
		}
		return threadIDs.sort();
	}

	function localSidebarVisibleThreadIDs() {
		const threadIDs = [];
		const seen = new Set();
		for (const anchor of globalThis.document.querySelectorAll("[data-sidebar-thread-id]")) {
			const threadID = firstString(anchor?.dataset?.sidebarThreadId);
			if (!validThreadID(threadID) || seen.has(threadID)) {
				continue;
			}
			seen.add(threadID);
			threadIDs.push(threadID);
			if (threadIDs.length >= 75) {
				break;
			}
		}
		return threadIDs.sort();
	}

	function localSidebarAllVisibleThreadIDs() {
		const threadIDs = [];
		const seen = new Set();
		for (const anchor of globalThis.document.querySelectorAll("[data-sidebar-thread-id]")) {
			const threadID = firstString(anchor?.dataset?.sidebarThreadId);
			if (!validThreadID(threadID) || seen.has(threadID)) {
				continue;
			}
			seen.add(threadID);
			threadIDs.push(threadID);
		}
		return threadIDs.sort();
	}

	function localSidebarMissingMetadataThreadIDs() {
		const coveredThreadIDs = new Set(String(localProjectsCache.sidebarTitleKey || "").split("\u0000").filter(validThreadID));
		return localSidebarAllVisibleThreadIDs().filter((threadID) =>
			!coveredThreadIDs.has(threadID) &&
			!localSidebarCachedThreadMetadataComplete(threadID) &&
			!archivedLocalSidebarThreadIDs.has(threadID)
		);
	}

	function scheduleLocalSidebarMetadataRefresh() {
		renderLocalSidebarMetadata();
		if (localSidebarDOMRefreshPending) {
			localSidebarDOMRefreshRequested = true;
			return;
		}
		if (localSidebarMissingMetadataThreadIDs().length === 0) {
			return;
		}
		localSidebarDOMRefreshPending = true;
		globalThis.setTimeout(() => {
			const missingThreadIDs = localSidebarMissingMetadataThreadIDs();
			const finishCycle = () => {
				localSidebarDOMRefreshPending = false;
				if (localSidebarDOMRefreshRequested) {
					localSidebarDOMRefreshRequested = false;
					scheduleLocalSidebarMetadataRefresh();
				}
			};
			const fetchBatch = (offset) => {
				if (offset >= missingThreadIDs.length) {
					finishCycle();
					return;
				}
				fetchLocalProjects(false, missingThreadIDs.slice(offset, offset + 75)).then(() => {
					renderLocalSidebarMetadata();
					requestLocalSidebarProjectRegroup();
					fetchBatch(offset + 75);
				}, () => {
					localSidebarDOMRefreshPending = false;
					localSidebarDOMRefreshRequested = false;
				});
			};
			fetchBatch(0);
		}, 0);
	}

	function localSidebarProjectRegroupMismatchThreadIDs() {
		const regroupThreadIDs = new Set();
		const threadsByID = new Map();
		for (const thread of localProjectsCache.threads || []) {
			const threadID = firstString(thread?.id, thread?.threadId, thread?.threadID);
			if (threadID) {
				threadsByID.set(threadID, thread);
			}
		}
		for (const anchor of globalThis.document.querySelectorAll("[data-sidebar-group-id]")) {
			if (firstString(anchor?.dataset?.sidebarGroupId, anchor?.getAttribute?.("data-sidebar-group-id")) !== "project:No project") {
				continue;
			}
			const threadID = firstString(anchor?.dataset?.sidebarThreadId);
			const thread = threadsByID.get(threadID);
			if (thread && localSidebarRepositoryGroupName(thread) !== "No project") {
				regroupThreadIDs.add(threadID);
			}
		}
		return regroupThreadIDs;
	}

	function scheduleLocalSidebarProjectRegroupStability(regroupThreadIDs) {
		const now = Date.now();
		for (const threadID of localSidebarProjectRegroupPendingThreadIDs) {
			if (regroupThreadIDs.has(threadID)) {
				localSidebarProjectRegroupStableSince.delete(threadID);
			} else if (!localSidebarProjectRegroupStableSince.has(threadID)) {
				localSidebarProjectRegroupStableSince.set(threadID, now);
			}
		}
		if (localSidebarProjectRegroupStableTimer !== undefined) {
			globalThis.clearTimeout(localSidebarProjectRegroupStableTimer);
			localSidebarProjectRegroupStableTimer = undefined;
		}
		let nextDelay = Infinity;
		for (const [threadID, stableSince] of localSidebarProjectRegroupStableSince) {
			if (!localSidebarProjectRegroupPendingThreadIDs.has(threadID)) {
				localSidebarProjectRegroupStableSince.delete(threadID);
				continue;
			}
			nextDelay = Math.min(nextDelay, Math.max(0, localSidebarProjectRegroupStabilityDelay - (now - stableSince)));
		}
		if (!Number.isFinite(nextDelay)) {
			return;
		}
		localSidebarProjectRegroupStableTimer = globalThis.setTimeout(() => {
			localSidebarProjectRegroupStableTimer = undefined;
			const currentRegroupThreadIDs = localSidebarProjectRegroupMismatchThreadIDs();
			const settledAt = Date.now();
			for (const threadID of localSidebarProjectRegroupPendingThreadIDs) {
				if (currentRegroupThreadIDs.has(threadID)) {
					localSidebarProjectRegroupStableSince.delete(threadID);
					continue;
				}
				const stableSince = localSidebarProjectRegroupStableSince.get(threadID);
				if (typeof stableSince === "number" && settledAt - stableSince >= localSidebarProjectRegroupStabilityDelay) {
					localSidebarProjectRegroupPendingThreadIDs.delete(threadID);
					localSidebarProjectRegroupStableSince.delete(threadID);
				}
			}
			scheduleLocalSidebarProjectRegroupStability(currentRegroupThreadIDs);
		}, nextDelay);
	}

	function requestLocalSidebarProjectRegroup() {
		const regroupThreadIDs = localSidebarProjectRegroupMismatchThreadIDs();
		scheduleLocalSidebarProjectRegroupStability(regroupThreadIDs);
		const newRegroupThreadIDs = Array.from(regroupThreadIDs).filter((threadID) => !localSidebarProjectRegroupPendingThreadIDs.has(threadID));
		if (newRegroupThreadIDs.length === 0) {
			return;
		}
		for (const threadID of newRegroupThreadIDs) {
			localSidebarProjectRegroupPendingThreadIDs.add(threadID);
		}
		let refreshRequested = false;
		let fallbackTimer;
		const refresh = () => {
			if (refreshRequested) {
				return;
			}
			refreshRequested = true;
			if (fallbackTimer !== undefined) {
				globalThis.clearTimeout(fallbackTimer);
			}
			try {
				globalThis.dispatchEvent(new Event("pageshow"));
			} catch {
				for (const threadID of newRegroupThreadIDs) {
					localSidebarProjectRegroupPendingThreadIDs.delete(threadID);
				}
			}
		};
		if (typeof globalThis.requestAnimationFrame === "function") {
			fallbackTimer = globalThis.setTimeout(refresh, 100);
			globalThis.requestAnimationFrame(refresh);
			return;
		}
		globalThis.setTimeout(refresh, 0);
	}

	function localSidebarElementHasThread(element) {
		return element instanceof Element && (element.matches("[data-sidebar-thread-id]") || !!element.querySelector("[data-sidebar-thread-id]"));
	}

	function localSidebarMutationsNeedRefresh(mutations) {
		for (const mutation of mutations) {
			const mutationTarget = mutation.target instanceof Element ? mutation.target : mutation.target?.parentElement;
			const target = mutationTarget instanceof Element ? mutationTarget.closest("[data-sidebar-thread-id]") : null;
			if (localSidebarElementHasThread(target)) {
				return true;
			}
			for (const node of mutation.addedNodes || []) {
				if (localSidebarElementHasThread(node)) {
					return true;
				}
			}
		}
		return false;
	}

	function installLocalSidebarMetadataIntegration() {
		if (globalThis.__cliproxyAmpLocalSidebarObserver) {
			scheduleLocalSidebarMetadataRefresh();
			return;
		}
		const observer = new MutationObserver((mutations) => {
			if (!localSidebarMutationsNeedRefresh(mutations)) {
				return;
			}
			reconcileActiveLocalThreadTitle();
			scheduleLocalSidebarMetadataRefresh();
		});
		observer.observe(globalThis.document.documentElement, { childList: true, subtree: true });
		globalThis.__cliproxyAmpLocalSidebarObserver = observer;
		reconcileActiveLocalThreadTitle();
		if (localProjectsCache.promise) {
			localProjectsCache.promise.then(renderLocalSidebarMetadata, () => undefined);
			return;
		}
		scheduleLocalSidebarMetadataRefresh();
	}

	function observeLocalActivitySection(observer) {
		if (!observer) {
			return null;
		}
		const found = globalThis.location.pathname === "/feed" ? localActivitySection() : null;
		const section = found?.section || null;
		if (section === localActivityObservedSection) {
			return section;
		}
		observer.disconnect();
		localActivityObservedSection = section;
		observer.observe(section || globalThis.document.documentElement, { childList: true, subtree: true });
		return section;
	}

	function installLocalActivityIntegration() {
		if (globalThis.__cliproxyAmpLocalActivityObserver) {
			scheduleLocalActivityRefresh();
			return;
		}
		const observer = new MutationObserver(() => {
			if (globalThis.location.pathname !== "/feed") {
				return;
			}
			const section = observeLocalActivitySection(observer);
			if (!section) {
				return;
			}
			if (localActivityCache.value) {
				renderLocalActivity(localActivityCache.value);
				return;
			}
			scheduleLocalActivityRefresh();
		});
		observer.observe(globalThis.document.documentElement, { childList: true, subtree: true });
		const navigationHandler = () => scheduleLocalActivityRefresh();
		if (typeof globalThis.addEventListener === "function") {
			globalThis.addEventListener("popstate", navigationHandler);
			globalThis.addEventListener("hashchange", navigationHandler);
		}
		observer.cliproxyCleanup = () => {
			observer.disconnect();
			localActivityObservedSection = null;
			if (typeof globalThis.removeEventListener === "function") {
				globalThis.removeEventListener("popstate", navigationHandler);
				globalThis.removeEventListener("hashchange", navigationHandler);
			}
		};
		globalThis.__cliproxyAmpLocalActivityObserver = observer;
		observeLocalActivitySection(observer);
		scheduleLocalActivityRefresh();
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
			reconcileActiveLocalThreadArchiveBadge();
			integrateThreadMenus(globalThis.document);
			return;
		}
		const observer = new MutationObserver((mutations) => {
			scheduleIntegrateThreadMenus(mutations);
		});
		observer.observe(globalThis.document.documentElement, { childList: true, subtree: true });
		globalThis.__cliproxyAmpLocalInferenceMenuObserver = observer;
		reconcileActiveLocalThreadArchiveBadge();
		integrateThreadMenus(globalThis.document);
	}

	function scheduleIntegrateThreadMenus(mutations) {
		if (globalThis.document?.visibilityState === "hidden") {
			return;
		}
		const roots = mutationAddedRoots(mutations, threadMenuSelector);
		const pending = globalThis.__cliproxyAmpLocalInferenceMenuPendingRoots ||= [];
		if (roots === null) {
			pending.push(globalThis.document);
		} else {
			pending.push(...roots);
		}
		if (globalThis.__cliproxyAmpLocalInferenceMenuIntegrateScheduled) {
			return;
		}
		globalThis.__cliproxyAmpLocalInferenceMenuIntegrateScheduled = true;
		globalThis.requestAnimationFrame(() => {
			globalThis.__cliproxyAmpLocalInferenceMenuIntegrateScheduled = false;
			globalThis.__cliproxyAmpLocalInferenceMenuPendingRoots = [];
			reconcileActiveLocalThreadArchiveBadge();
			for (const root of pending) {
				integrateThreadMenus(root);
			}
		});
	}

	function mutationAddedRoots(mutations, selector) {
		if (!mutations || typeof mutations[Symbol.iterator] !== "function") {
			return null;
		}
		const roots = [];
		for (const mutation of mutations) {
			for (const node of mutation.addedNodes || []) {
				if (!(node instanceof Element) || roots.some((root) => root === node || root.contains(node))) {
					continue;
				}
				if (node.matches?.(selector) || node.querySelector?.(selector)) {
					roots.push(node);
				}
			}
		}
		return roots;
	}

	function installCommandPaletteIntegration() {
		if (globalThis.__cliproxyAmpLocalInferenceCommandPaletteObserver) {
			integrateCommandPalettes(globalThis.document);
			return;
		}
		const observer = new MutationObserver((mutations) => {
			scheduleIntegrateCommandPalettes(mutations);
		});
		observer.observe(globalThis.document.documentElement, { childList: true, subtree: true });
		globalThis.__cliproxyAmpLocalInferenceCommandPaletteObserver = observer;
		integrateCommandPalettes(globalThis.document);
	}

	function scheduleIntegrateCommandPalettes(mutations) {
		if (globalThis.document?.visibilityState === "hidden") {
			return;
		}
		const roots = mutationAddedRoots(mutations, commandPaletteSelector);
		const pending = globalThis.__cliproxyAmpLocalInferenceCommandPalettePendingRoots ||= [];
		if (roots === null) {
			pending.push(globalThis.document);
		} else {
			pending.push(...roots);
		}
		if (globalThis.__cliproxyAmpLocalInferenceCommandPaletteIntegrateScheduled || pending.length === 0) {
			return;
		}
		globalThis.__cliproxyAmpLocalInferenceCommandPaletteIntegrateScheduled = true;
		globalThis.requestAnimationFrame(() => {
			globalThis.__cliproxyAmpLocalInferenceCommandPaletteIntegrateScheduled = false;
			globalThis.__cliproxyAmpLocalInferenceCommandPalettePendingRoots = [];
			for (const root of pending) {
				integrateCommandPalettes(root);
			}
		});
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
		const selector = commandPaletteSelector;
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
			if (root.matches(threadMenuSelector)) {
				add(root);
			}
			root.querySelectorAll?.(threadMenuSelector).forEach(add);
		} else {
			root.querySelectorAll?.(threadMenuSelector).forEach(add);
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
		return (url.protocol === protocol && url.host === base.host) ||
			((url.protocol === "http:" || url.protocol === "https:") && sameLocalHTTPBase(url, base));
	}

	function localWebSocketURL(rawURL) {
		pendingLocalBootstrapThreadID = "";
		const source = new URL(String(rawURL), globalThis.location.href);
		const base = localBaseURL();
		if (!shouldRewriteWebSocket(source, base)) {
			return rawURL;
		}
		const bootstrapExecutor = shouldBootstrapExecutor(source);
		if (bootstrapExecutor && !storedLocalAPIKey()) {
			return rawURL;
		}
		const alreadyLocal = sameLocalWebSocketBase(source, base);
		const local = alreadyLocal ? new URL(source.href) : new URL(source.pathname + source.search + source.hash, base);
		const userActorSocket = shouldBridgeUserActorWebSocket(source);
		if (userActorSocket) {
			rememberAuthenticatedAmpUserIDValue(firstString(source.searchParams.get("rvt-key"), source.searchParams.get("key")));
		}
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
		if (bootstrapExecutor && !userActorSocket && threadID && threadID === pathThreadID()) {
			pendingLocalBootstrapThreadID = threadID;
		}
		if (bootstrapExecutor) {
			const workingDirectory = normalizeWorkingDirectory(threadWorkingDirectories()[threadID]);
			if (workingDirectory && !local.searchParams.has("cliproxy-working-directory")) {
				local.searchParams.set("cliproxy-working-directory", workingDirectory);
			}
			const mode = normalizedThreadSettings(threadSettings()[threadID]);
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

	function webSocketProtocolDiagnostics(protocols) {
		const values = Array.isArray(protocols) ? protocols : [protocols];
		return JSON.stringify(values.map((value) => {
			const protocol = String(value || "");
			if (protocol.startsWith("rivet_token.")) {
				return "rivet_token.<redacted>";
			}
			if (protocol.startsWith("rivet_conn_params.")) {
				return "rivet_conn_params.<redacted>";
			}
			return protocol;
		}));
	}

	function rewriteClientResumePayload(payload, threadID) {
		if (typeof payload !== "string" || !threadID) {
			return payload;
		}
		const baseVersion = positiveThreadVersion(loadedThreadBaseVersions.get(threadID));
		if (!baseVersion) {
			return payload;
		}
		let parsed;
		try {
			parsed = originalJSONParse(payload);
		} catch {
			return payload;
		}
		let resume = null;
		if (isPlainObject(parsed) && parsed.type === "client_resume") {
			resume = parsed;
		} else if (isPlainObject(parsed) && parsed.method === "client_resume" && isPlainObject(parsed.params)) {
			resume = parsed.params;
		}
		if (!resume || !Number.isSafeInteger(resume.version) || resume.version < 0) {
			return payload;
		}
		diagnostics.clientResumeObservedCount += 1;
		diagnostics.lastClientResumeObservedVersion = resume.version;
		if (resume.version >= baseVersion) {
			return payload;
		}
		resume.version = baseVersion;
		diagnostics.clientResumeRewriteCount += 1;
		diagnostics.lastClientResumeThreadID = threadID;
		diagnostics.lastClientResumeBaseVersion = baseVersion;
		return JSON.stringify(parsed);
	}

	function updateTrackedLocalThreadSocketCount() {
		let count = 0;
		for (const sockets of localThreadSockets.values()) {
			count += sockets.size;
		}
		diagnostics.trackedLocalThreadSocketCount = count;
	}

	function forgetLocalThreadSocket(threadID, socket) {
		const sockets = localThreadSockets.get(threadID);
		if (!sockets) {
			return;
		}
		sockets.delete(socket);
		if (sockets.size === 0) {
			localThreadSockets.delete(threadID);
		}
		updateTrackedLocalThreadSocketCount();
	}

	function localThreadSocketPersistsOutsideThreadRoute(threadID) {
		return normalizedThreadSettings(threadSettings()[threadID]).agentMode === "puck";
	}

	function closeInactiveLocalThreadSockets(activeThreadID = pathThreadID()) {
		for (const [threadID, sockets] of localThreadSockets) {
			if ((activeThreadID && threadID === activeThreadID) || localThreadSocketPersistsOutsideThreadRoute(threadID)) {
				continue;
			}
			for (const socket of sockets) {
				if (socket.readyState === NativeWebSocket.CONNECTING || socket.readyState === NativeWebSocket.OPEN) {
					try {
						socket.close(1000, "thread navigation");
						diagnostics.staleLocalThreadSocketCloseCount += 1;
					} catch {
					}
				}
			}
			localThreadSockets.delete(threadID);
		}
		updateTrackedLocalThreadSocketCount();
	}

	function trackLocalThreadSocket(threadID, socket) {
		if (!threadID || !socket) {
			return;
		}
		const activeThreadID = pathThreadID();
		if ((!activeThreadID || threadID !== activeThreadID) && !localThreadSocketPersistsOutsideThreadRoute(threadID)) {
			if (socket.readyState === NativeWebSocket.CONNECTING || socket.readyState === NativeWebSocket.OPEN) {
				try {
					socket.close(1000, "thread navigation");
					diagnostics.staleLocalThreadSocketCloseCount += 1;
				} catch {
				}
			}
			return;
		}
		closeInactiveLocalThreadSockets(activeThreadID);
		let sockets = localThreadSockets.get(threadID);
		if (!sockets) {
			sockets = new Set();
			localThreadSockets.set(threadID, sockets);
		}
		sockets.add(socket);
		updateTrackedLocalThreadSocketCount();
	}

	function installLocalNavigationRefresh() {
		for (const method of ["pushState", "replaceState"]) {
			const nativeMethod = globalThis.history?.[method];
			if (typeof nativeMethod !== "function") {
				continue;
			}
			globalThis.history[method] = function(...args) {
				const result = Reflect.apply(nativeMethod, this, args);
				globalThis.queueMicrotask(() => {
					closeInactiveLocalThreadSockets();
					scheduleLocalActivityRefresh();
					synchronizeLocalProjectPageRoute();
				});
				return result;
			};
		}
		if (typeof globalThis.addEventListener === "function") {
			installHiddenPageSocketPause();
			globalThis.addEventListener("popstate", () => {
				closeInactiveLocalThreadSockets();
				scheduleLocalActivityRefresh();
				synchronizeLocalProjectPageRoute();
			});
			globalThis.addEventListener("pagehide", () => closeInactiveLocalThreadSockets(""));
		}
	}

	const hiddenPageSocketPauseGraceMs = 1500;
	const hiddenPageDeferredSocketLimit = 64;
	let hiddenPageSocketPaused = false;
	const hiddenPageDeferredSocketReleases = new Map();
	const hiddenPageDeferredSocketPrototypes = new WeakMap();

	function isIOSClient() {
		const nav = globalThis.navigator;
		if (!nav) {
			return false;
		}
		const platform = firstString(nav.platform, nav?.userAgentData?.platform);
		if (/iP(hone|ad|od)/.test(platform)) {
			return true;
		}
		return platform === "MacIntel" && (nav.maxTouchPoints || 0) > 1;
	}

	function pauseLocalThreadSocketsForHiddenPage() {
		hiddenPageSocketPaused = true;
		for (const [threadID, sockets] of localThreadSockets) {
			for (const socket of sockets) {
				if (socket.readyState === NativeWebSocket.CONNECTING || socket.readyState === NativeWebSocket.OPEN) {
					try {
						socket.close(1000, "page hidden");
						diagnostics.hiddenPageSocketPauseCount += 1;
					} catch {
					}
				}
			}
			localThreadSockets.delete(threadID);
		}
		updateTrackedLocalThreadSocketCount();
	}

	function hiddenPageSocketCloseEvent(code = 1006, reason = "page visible", wasClean = false) {
		if (typeof globalThis.CloseEvent === "function") {
			return new globalThis.CloseEvent("close", { code, reason, wasClean });
		}
		const event = new globalThis.Event("close");
		Object.defineProperties(event, {
			code: { value: code },
			reason: { value: reason },
			wasClean: { value: wasClean },
		});
		return event;
	}

	function hiddenPageDeferredSocket(url, prototype) {
		let readyState = NativeWebSocket.CONNECTING;
		let released = false;
		const socket = new globalThis.EventTarget();
		const addEventListener = socket.addEventListener.bind(socket);
		const removeEventListener = socket.removeEventListener.bind(socket);
		const dispatchEvent = socket.dispatchEvent.bind(socket);
		const eventHandlers = {};
		const eventHandler = (type) => ({
			get: () => eventHandlers[type] || null,
			set: (handler) => {
				if (eventHandlers[type]) {
					removeEventListener(type, eventHandlers[type]);
				}
				eventHandlers[type] = typeof handler === "function" ? handler : null;
				if (eventHandlers[type]) {
					addEventListener(type, eventHandlers[type]);
				}
			},
		});
		hiddenPageDeferredSocketPrototypes.set(socket, prototype || NativeWebSocket.prototype);
		Object.defineProperties(socket, {
			url: { value: String(url), enumerable: true },
			readyState: { get: () => readyState, enumerable: true },
			CONNECTING: { value: NativeWebSocket.CONNECTING },
			OPEN: { value: NativeWebSocket.OPEN },
			CLOSING: { value: NativeWebSocket.CLOSING },
			CLOSED: { value: NativeWebSocket.CLOSED },
			bufferedAmount: { value: 0, enumerable: true },
			extensions: { value: "", enumerable: true },
			protocol: { value: "", enumerable: true },
			binaryType: { value: "blob", writable: true, enumerable: true },
			[Symbol.toStringTag]: { value: "WebSocket" },
			onopen: eventHandler("open"),
			onmessage: eventHandler("message"),
			onerror: eventHandler("error"),
			onclose: eventHandler("close"),
			addEventListener: { value: addEventListener },
			removeEventListener: { value: removeEventListener },
			dispatchEvent: { value: dispatchEvent },
			send: { value() {
				const error = new Error("WebSocket is not open");
				error.name = "InvalidStateError";
				throw error;
			}, writable: true },
			close: { value(code = 1000, reason = "") {
				if (readyState === NativeWebSocket.CLOSED) {
					return;
				}
				readyState = NativeWebSocket.CLOSED;
				released = true;
				hiddenPageDeferredSocketReleases.delete(socket);
				diagnostics.hiddenPageDeferredSocketCount = hiddenPageDeferredSocketReleases.size;
				globalThis.queueMicrotask(() => dispatchEvent(hiddenPageSocketCloseEvent(Number(code), String(reason), true)));
			}, writable: true },
		});
		const release = (notify = true) => {
			if (released || readyState === NativeWebSocket.CLOSED) {
				return;
			}
			released = true;
			readyState = NativeWebSocket.CLOSED;
			if (notify) {
				diagnostics.hiddenPageSocketResumeCount += 1;
				dispatchEvent(hiddenPageSocketCloseEvent());
			}
		};
		if (hiddenPageDeferredSocketReleases.size >= hiddenPageDeferredSocketLimit) {
			const oldest = hiddenPageDeferredSocketReleases.entries().next().value;
			hiddenPageDeferredSocketReleases.delete(oldest[0]);
			oldest[1](false);
		}
		hiddenPageDeferredSocketReleases.set(socket, release);
		diagnostics.hiddenPageSocketConstructionSuppressionCount += 1;
		diagnostics.hiddenPageDeferredSocketCount = hiddenPageDeferredSocketReleases.size;
		return socket;
	}

	function resumeHiddenPageSockets() {
		if (!hiddenPageSocketPaused) {
			return;
		}
		hiddenPageSocketPaused = false;
		const releases = Array.from(hiddenPageDeferredSocketReleases.values());
		hiddenPageDeferredSocketReleases.clear();
		diagnostics.hiddenPageDeferredSocketCount = 0;
		for (const release of releases) {
			globalThis.queueMicrotask(release);
		}
	}

	function installHiddenPageSocketPause() {
		if (!isIOSClient()) {
			return;
		}
		const doc = globalThis.document;
		if (!doc || typeof doc.addEventListener !== "function") {
			return;
		}
		let hiddenPauseTimer = null;
		const cancelTimer = () => {
			if (hiddenPauseTimer !== null) {
				globalThis.clearTimeout(hiddenPauseTimer);
				hiddenPauseTimer = null;
			}
		};
		if (doc.visibilityState === "hidden") {
			hiddenPageSocketPaused = true;
		}
		doc.addEventListener("visibilitychange", () => {
			if (doc.visibilityState === "hidden") {
				hiddenPageSocketPaused = true;
				cancelTimer();
				hiddenPauseTimer = globalThis.setTimeout(() => {
					hiddenPauseTimer = null;
					if (doc.visibilityState === "hidden") {
						pauseLocalThreadSocketsForHiddenPage();
					}
				}, hiddenPageSocketPauseGraceMs);
			} else {
				cancelTimer();
				resumeHiddenPageSockets();
			}
		});
		globalThis.addEventListener("pagehide", () => {
			cancelTimer();
			pauseLocalThreadSocketsForHiddenPage();
		});
		globalThis.addEventListener("pageshow", () => {
			if (doc.visibilityState !== "hidden") {
				cancelTimer();
				resumeHiddenPageSockets();
			}
		});
	}

	globalThis.JSON.parse = function(text, reviver) {
		const parsed = originalJSONParse(text, reviver);
		if (typeof text === "string" && text.includes('"userWorkspace"') && text.includes('"userFeatures"')) {
			try {
				rememberAuthenticatedAmpUserID(parsed);
			} catch {
			}
		}
		// Skip the local-inference regex scan on oversized payloads; full thread
		// transcripts dominate mobile CPU and are handled via Response.json anyway.
		if (typeof text !== "string" || text.length > localInferencePatchScanMaxChars) {
			return parsed;
		}
		try {
			const patchOptions = parsedTextLocalInferencePatchOptions(text);
			if (patchOptions.configs) {
				patchDecodedLocalInference(parsed, patchOptions);
			}
		} catch {
		}
		return parsed;
	};

	if (typeof originalResponseJSON === "function") {
		globalThis.Response.prototype.json = function(...args) {
			const response = this;
			const promise = originalResponseJSON.apply(response, args);
			const patchThreadData = shouldPatchResponseJSON(response);
			const patchSidebar = shouldPatchSidebarResponseJSON(response);
			const patchThreadSearch = shouldPatchThreadSearchResponseJSON(response);
			const patchUsage = shouldPatchUsageResponseJSON(response);
			const patchProjectList = shouldPatchProjectListResponseJSON(response);
			const patchActivity = shouldPatchActivityResponseJSON(response);
			const threadSearchContext = patchThreadSearch ? threadSearchResponseContexts.get(response) || threadSearchRequestContext(response.url) : null;
			const activityFilterContext = activityFilterResponseContexts.get(response);
			const activityFilterEndpoint = firstString(activityFilterContext?.endpoint, activityFilterRemoteEndpoint(response));
			const activityFilterQuery = activityFilterEndpoint ?
				(typeof activityFilterContext?.query === "string" ? activityFilterContext.query.trim().toLowerCase() : activityFilterSearchQuery(response.url)) : "";
			const captureAuthenticatedUser = shouldCaptureAuthenticatedAmpUserID(response);
			if (!patchThreadData && !patchSidebar && !patchThreadSearch && !patchUsage && !patchProjectList && !patchActivity && !activityFilterEndpoint && !captureAuthenticatedUser) {
				return promise;
			}
			const projectsReady = !patchSidebar && (patchThreadData || patchUsage || patchProjectList) ? fetchLocalProjects(false) : Promise.resolve();
			return promise.then((parsed) => {
				try {
					if (captureAuthenticatedUser) {
						rememberAuthenticatedAmpUserID(parsed);
					}
				} catch {
				}
				const sidebarProjectsReady = patchSidebar ? fetchLocalProjects(false, sidebarResponseThreadIDs(parsed)) : projectsReady;
				const activityReady = patchActivity || activityFilterEndpoint ? fetchLocalActivity(activityFilterSourceURL(response, activityFilterContext)) : Promise.resolve(null);
				const localThreadSearchReady = threadSearchContext?.localSearchPromise || (threadSearchContext ? fetchLocalThreadSearch(threadSearchContext) : Promise.resolve(null));
				const cloudThreadSearchWindowReady = threadSearchContext?.cloudWindowPromise || Promise.resolve(null);
				return Promise.all([sidebarProjectsReady, activityReady, localThreadSearchReady, cloudThreadSearchWindowReady]).then(([, activity, localThreadSearch, cloudThreadSearchWindow]) => {
					if (patchThreadSearch && threadSearchContext) {
						mergeThreadSearchResponse(parsed, localThreadSearch, threadSearchContext, cloudThreadSearchWindow);
					}
					const missingThreadSearchMetadataIDs = patchThreadSearch ? threadSearchResponseMissingMetadataIDs(parsed) : [];
					const threadSearchProjectsReady = missingThreadSearchMetadataIDs.length > 0 ? fetchLocalProjects(false, missingThreadSearchMetadataIDs) : Promise.resolve();
					return threadSearchProjectsReady.then(() => {
				try {
					if (patchThreadData) {
						patchDecodedLocalInference(parsed, { configs: true });
						patchThreadProjectChangesWorkflow(parsed);
					}
					if (patchSidebar) {
						mergeSidebarResponse(parsed);
						scheduleLocalSidebarHydrationReveal();
					}
					if (patchThreadSearch) {
						patchThreadSearchResponse(parsed);
					}
					if (patchUsage) {
						patchUsageResponse(parsed);
					}
					if (patchProjectList) {
						mergeProjectListResponse(parsed, response);
					}
					if (patchActivity) {
						mergeActivityResponse(parsed, activity);
					}
					if (activityFilterEndpoint) {
						mergeActivityFilterResponse(parsed, activityFilterEndpoint, activity, activityFilterQuery);
					}
					diagnostics.responseJSONPatchCount += 1;
				} catch {
				}
				return parsed;
					});
				});
			});
		};
	}

	function fetchLocalThreadResourceWithRetry(url, options, retries = 2) {
		return originalFetch(url, options).then((response) => {
			if (response.status !== 404 || retries <= 0 || options?.signal?.aborted) {
				return response;
			}
			return new Promise((resolve) => globalThis.setTimeout(resolve, retries === 2 ? 50 : 100)).then(() => fetchLocalThreadResourceWithRetry(url, options, retries - 1));
		}, (error) => {
			if (retries <= 0 || options?.signal?.aborted) {
				throw error;
			}
			return new Promise((resolve) => globalThis.setTimeout(resolve, retries === 2 ? 50 : 100)).then(() => fetchLocalThreadResourceWithRetry(url, options, retries - 1));
		});
	}

	globalThis.fetch = function(input, init) {
		const request = input instanceof Request ? input : null;
		const sourceURL = new URL(request ? request.url : String(input), globalThis.location.href);
		const method = init?.method || request?.method || "GET";
		const threadSearchContext = storedLocalAPIKey() ? threadSearchRequestContext(sourceURL) : null;
		if (threadSearchContext) {
			threadSearchContext.localSearchPromise = fetchLocalThreadSearch(threadSearchContext);
			const targetURL = threadSearchWindowURL(sourceURL, threadSearchContext);
			if (targetURL.href !== sourceURL.href) {
				const options = request ? Object.assign({
					method: request.method,
					headers: request.headers,
					cache: request.cache,
					credentials: request.credentials,
					mode: request.mode,
					redirect: request.redirect,
					referrer: request.referrer,
					referrerPolicy: request.referrerPolicy,
					integrity: request.integrity,
					keepalive: request.keepalive,
					signal: request.signal,
				}, init || {}) : init;
				threadSearchContext.cloudWindowPromise = originalFetch(targetURL.href, options).then((response) => {
					if (!response.ok) {
						return null;
					}
					return response.text().then((text) => originalJSONParse(text));
				}).catch(() => null);
			}
			return originalFetch(input, init).then((response) => {
				threadSearchResponseContexts.set(response, threadSearchContext);
				return response;
			});
		}
		if (openPuckThreadRemotePath(sourceURL.pathname) && sourceURL.origin === globalThis.location.origin) {
			return originalFetch(input, init).then((response) => rememberRemotePuckThread(sourceURL, response));
		}
		if ((threadActorAPIPath(sourceURL.pathname) || threadSearchAPIPath(sourceURL.pathname) || rivetMetadataPath(sourceURL.pathname)) && sourceURL.origin === globalThis.location.origin && !storedLocalAPIKey()) {
			return originalFetch(input, init);
		}
		if (updateProjectChangesWorkflowRemotePath(sourceURL.pathname) && sourceURL.origin === globalThis.location.origin) {
			if (!requestUsesLocalBridge(request, init)) {
				return originalFetch(input, init);
			}
			const bridgeProjectUpdate = (body) => fetchLocalProjects(false).then(() => {
				const localUpdate = localProjectChangesWorkflowRemoteBody(body);
				const forwarded = Object.assign({}, request ? {
					method: request.method,
					headers: request.headers,
					cache: request.cache,
					credentials: request.credentials,
					mode: request.mode,
					redirect: request.redirect,
					referrer: request.referrer,
					referrerPolicy: request.referrerPolicy,
					integrity: request.integrity,
					keepalive: request.keepalive,
					signal: request.signal,
				} : {}, init || {}, { body });
				const localURL = new URL(sourceURL.pathname + sourceURL.search, localBaseURLString());
				return globalThis.fetch(localURL.href, forwarded).then(async (response) => {
					if (!response?.ok || !localUpdate) {
						return response;
					}
					try {
						const envelope = await response.clone().json();
						const result = decodeDevalueString(envelope?.data || "")?._;
						if (isPlainObject(result) && result.success === true) {
							const project = localProjectByID(localProjectsCache.projects, localUpdate.projectID);
							if (project) {
								project.changesWorkflow = localUpdate.changesWorkflow;
							}
							localProjectsCache.at = Date.now();
							diagnostics.localProjectChangesWorkflowCacheUpdateCount += 1;
						}
					} catch {
					}
					return response;
				});
			});
			if (request && init?.body === undefined && request.body) {
				try {
					return request.clone().text().then(bridgeProjectUpdate);
				} catch {
				}
			}
			return bridgeProjectUpdate(init?.body);
		}
		const activityFilterEndpoint = svelteKitRemoteEndpoint(sourceURL.pathname);
		if (activityFilterEndpoint === "searchFeedRepositories" || activityFilterEndpoint === "searchFeedUsers") {
			if (request && init?.body === undefined && request.body) {
				try {
					return request.clone().text().then((body) => originalFetch(input, init).then((response) => rememberActivityFilterResponse(response, activityFilterEndpoint, body)));
				} catch {
				}
			}
			return originalFetch(input, init).then((response) => rememberActivityFilterResponse(response, activityFilterEndpoint, init?.body));
		}
		if (localThreadMutationRemotePath(sourceURL.pathname)) {
			if (request && init?.body === undefined && request.body) {
				try {
					return request.clone().text().then((text) => {
						const forwarded = Object.assign({}, {
							method: request.method,
							headers: request.headers,
							cache: request.cache,
							credentials: request.credentials,
							mode: request.mode,
							redirect: request.redirect,
							referrer: request.referrer,
							referrerPolicy: request.referrerPolicy,
							integrity: request.integrity,
							keepalive: request.keepalive,
							signal: request.signal,
						}, init || {}, { body: text });
						if (localThreadMutationRemoteBody(sourceURL.pathname, text)) {
							return globalThis.fetch(sourceURL.href, forwarded);
						}
						const threadID = localThreadMutationRemoteThreadID(sourceURL.pathname, text);
						if (!validThreadID(threadID)) {
							return originalFetch(input, init);
						}
						return discoverLocalThreadID(threadID).then((discovered) => discovered ? globalThis.fetch(sourceURL.href, forwarded) : originalFetch(input, init));
					});
				} catch {
					return originalFetch(input, init);
				}
			}
			if (!localThreadMutationRemoteBody(sourceURL.pathname, init?.body)) {
				const threadID = localThreadMutationRemoteThreadID(sourceURL.pathname, init?.body);
				if (!validThreadID(threadID)) {
					return originalFetch(input, init);
				}
				return discoverLocalThreadID(threadID).then((discovered) => discovered ? globalThis.fetch(input, init) : originalFetch(input, init));
			}
		}
		const candidateLocalThreadID = firstString(localThreadResourceDataThreadID(sourceURL.pathname), diffCaptureReadThreadID(sourceURL.pathname));
		if (candidateLocalThreadID && !rememberedLocalThreadID(candidateLocalThreadID)) {
			return discoverLocalThreadID(candidateLocalThreadID).then((discovered) => {
				if (!discovered) {
					return originalFetch(input, init);
				}
				return globalThis.fetch(input, init);
			});
		}
		if (!shouldBridgeHTTP(sourceURL)) {
			return originalFetch(input, init);
		}

		diagnostics.fetchRewriteCount += 1;
		const sourceHeaders = new Headers(init?.headers || request?.headers || undefined);
		const headers = localFetchHeaders(sourceHeaders.get("Content-Type"));
		const forwardedHeaderNames = new Set([
			"accept",
			"x-rivet-actor",
			"x-rivet-conn-params",
			"x-rivet-encoding",
			"x-rivet-skip-ready-wait",
			"x-rivet-target",
			"x-rivet-token",
		]);
		for (const [name, value] of sourceHeaders.entries()) {
			if (forwardedHeaderNames.has(name.toLowerCase()) && !headers.has(name)) {
				headers.set(name, value);
			}
		}

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
		const createProjectName = createProjectThreadRemotePath(sourceURL.pathname) ? visibleCreateProjectName(false) : "";
		const targetURLForBody = (body) => shouldRewriteHTTP(sourceURL) ? localHTTPURL(sourceURL, body, createProjectName) : sourceURL.href;
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
					return ensureVisibleProjectLookupForRemoteCreate(sourceURL, createProjectName).then(() => {
						return originalFetch(targetURLForBody(body), makeOptions(body)).then((response) => {
								return rememberRemoteCreateProjectThread(sourceURL, body, invalidateLocalSidebarAfterThreadMutation(sourceURL, body, response), createProjectName);
						});
					});
				});
			} catch {
			}
		}
		const body = bridgeRequestBody(sourceURL, method, init?.body ?? requestOptions.body);
		const options = makeOptions(body);
		const localRequest = () => ensureVisibleProjectLookupForRemoteCreate(sourceURL, createProjectName).then(() => {
			const targetURL = targetURLForBody(body);
			const requestPromise = candidateLocalThreadID ? fetchLocalThreadResourceWithRetry(targetURL, options) : originalFetch(targetURL, options);
			return requestPromise.then((response) => {
					return rememberRemoteCreateProjectThread(sourceURL, body, invalidateLocalSidebarAfterThreadMutation(sourceURL, body, response), createProjectName);
			});
		});
		return mirrorLocalThreadMutation(sourceURL, input, init, localRequest);
	};

	function LocalInferenceWebSocket(...args) {
		if (!new.target) {
			throw new TypeError("WebSocket constructor must be called with 'new'");
		}
		diagnostics.lastWebSocketProtocols = webSocketProtocolDiagnostics(args.length > 1 ? args[1] : "");
		let rememberLocalThreadIDOnOpen = "";
		let resumeThreadID = "";
		let suppressForHiddenPage = false;
		let socketOpened = false;
		if (args.length > 0) {
			const source = new URL(String(args[0]), globalThis.location.href);
			args[0] = localWebSocketURL(args[0]);
			rememberLocalThreadIDOnOpen = pendingLocalBootstrapThreadID;
			pendingLocalBootstrapThreadID = "";
			const rewritten = new URL(String(args[0]), globalThis.location.href);
			if (gatewayActorPath(source.pathname) && !gatewayUserActorPath(source.pathname) && sameLocalWebSocketBase(rewritten, localBaseURL())) {
				resumeThreadID = threadIDFromGatewayURL(source) || threadIDFromGatewayURL(rewritten);
				if (!resumeThreadID && (source.searchParams.has("cliproxy-api-key") || rewritten.searchParams.has("cliproxy-api-key"))) {
					resumeThreadID = activeLocalThreadID();
				}
				suppressForHiddenPage = !!resumeThreadID && hiddenPageSocketPaused && globalThis.document?.visibilityState === "hidden";
			}
		}
		const socket = suppressForHiddenPage
			? hiddenPageDeferredSocket(args[0], new.target.prototype)
			: new NativeWebSocket(...args);
		if (!suppressForHiddenPage && new.target !== LocalInferenceWebSocket && new.target.prototype) {
			Object.setPrototypeOf(socket, new.target.prototype);
		}
		try {
			if (resumeThreadID && !suppressForHiddenPage) {
				trackLocalThreadSocket(resumeThreadID, socket);
			}
			if (resumeThreadID && typeof socket.send === "function") {
				const nativeSend = socket.send.bind(socket);
				socket.send = (payload) => nativeSend(rewriteClientResumePayload(payload, resumeThreadID));
			}
			diagnostics.lastWebSocketState = suppressForHiddenPage ? "hidden-suppressed" : "constructed";
			diagnostics.lastWebSocketReadyState = Number(socket.readyState);
			socket.addEventListener("open", () => {
				diagnostics.webSocketOpenCount += 1;
				if (!socketOpened) {
					socketOpened = true;
					diagnostics.activeWebSocketCount += 1;
				}
				diagnostics.lastWebSocketState = "open";
				diagnostics.lastWebSocketReadyState = Number(socket.readyState);
				if (rememberLocalThreadIDOnOpen) {
					rememberLocalThreadID(rememberLocalThreadIDOnOpen);
				}
			});
			socket.addEventListener("close", (event) => {
				if (resumeThreadID) {
					forgetLocalThreadSocket(resumeThreadID, socket);
				}
				diagnostics.webSocketCloseCount += 1;
				if (socketOpened) {
					socketOpened = false;
					diagnostics.activeWebSocketCount = Math.max(0, diagnostics.activeWebSocketCount - 1);
				}
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
	}
	Object.setPrototypeOf(LocalInferenceWebSocket, NativeWebSocket);
	LocalInferenceWebSocket.prototype = NativeWebSocket.prototype;
	Object.defineProperty(LocalInferenceWebSocket, Symbol.hasInstance, {
		value(instance) {
			const prototype = hiddenPageDeferredSocketPrototypes.get(instance);
			if (prototype) {
				return this === LocalInferenceWebSocket || this.prototype === prototype ||
					(this.prototype !== null && typeof this.prototype === "object" && Object.prototype.isPrototypeOf.call(this.prototype, prototype));
			}
			return Function.prototype[Symbol.hasInstance].call(this, instance);
		},
	});
	globalThis.WebSocket = LocalInferenceWebSocket;

	globalThis.__cliproxyAmpLocalInference = {
		userscriptVersion,
		apiKeyStorageKey,
		workingDirectoryStorageKey,
		selectedLocalProjectStorageKey,
		localThreadIDsStorageKey,
		threadWorkingDirectoriesStorageKey,
		threadSettingsStorageKey,
		sidebarTitlesStorageKey,
		localThreadStorageLimit,
		defaultBaseURL,
		diagnostics,
		rememberLocalThreadID,
		removeInjectedLocalThreadControls,
	};
	installLocalNavigationRefresh();
	if (/\/view\/?$/.test(globalThis.location.pathname || "")) {
		discoverLocalThreadID(pathThreadID());
	}

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
