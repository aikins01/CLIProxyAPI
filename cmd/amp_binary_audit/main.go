package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	snapshotSchema              = 29
	minSupportedSnapshotSchema  = 28
	minReleaseBinarySizeBytes   = 1_000_000
	threadReadSearchTestCommand = `go test -count=1 -run 'TestNeoRuntimeDoesNotServeThreadReadSearchHTTP|TestNeoActorPasses.*ThreadToolResultThrough|TestRegisterManagementRoutesDoesNotServeThreadDiscoveryLocallyWithoutProxy|TestRegisterManagementRoutesPassesInternalRPCsUpstreamWhenProxyExists|TestRegisterManagementRoutesPassesThreadGETsUpstreamWhenProxyExists|TestRegisterManagementRoutesPassesThreadReaderToolsUpstreamWhenProxyExists' ./internal/api/modules/amp`
)

var (
	defaultAmpBinaryPath                   = filepath.Join(os.Getenv("HOME"), ".amp", "bin", "amp")
	defaultBaselinePath                    = filepath.Join("dev", "amp-binary-parity-baseline.json")
	auditStdout                  io.Writer = os.Stdout
	errUnsupportedSnapshotSchema           = errors.New("unsupported baseline schema")

	versionPattern                       = regexp.MustCompile(`\b0\.0\.[0-9]+-g[0-9a-f]{6,}\b`)
	timestampPattern                     = regexp.MustCompile(`\b20[0-9]{2}-[0-9]{2}-[0-9]{2}T[0-9:.]+Z\b`)
	routePattern                         = regexp.MustCompile(`/[A-Za-z0-9._~:/?#[\]@!$&'()*+,;=%-]+`)
	eventPattern                         = regexp.MustCompile(`\b(?:user|assistant|thread|tool|message|agent|environment|title|max-tokens|main-thread|reasoning-effort)[a-z0-9-]*(?::[a-z][a-z0-9-]*)+\b`)
	threadProtocolLiteralPattern         = regexp.MustCompile(`type:[A-Za-z0-9_$]+\.literal\("([A-Za-z][A-Za-z0-9_:.-]*)"\)`)
	cancelPattern                        = regexp.MustCompile(`\b(?:user|system):[a-z][a-z0-9-]*\b`)
	modelPattern                         = regexp.MustCompile(`\b(?:gpt|claude|gemini|codex)-[A-Za-z0-9._-]+\b|\bamp-nostromo-[A-Za-z0-9._-]+\b|\bo[1345](?:-[A-Za-z0-9._-]+)+\b`)
	modelLimitPattern                    = regexp.MustCompile(`([A-Z][A-Z0-9_]+):\{provider:(?:K|X0)\.([A-Z0-9_]+),name:"([^"]+)",displayName:"([^"]+)",contextWindow:([0-9]+(?:e[0-9]+)?),maxOutputTokens:([0-9]+)`)
	largeContextAliasPattern             = regexp.MustCompile(`IOR="([^"]+)",JpT=([0-9]+(?:e[0-9]+)?),QpT=([0-9]+)`)
	largeContextNamedAliasPattern        = regexp.MustCompile(`\b[A-Za-z0-9_$]+="([^"]+-1m)"`)
	largeContextReturnPattern            = regexp.MustCompile(`enableLargeContext&&[A-Za-z0-9_$]+\([A-Za-z0-9_$]+\)===([A-Za-z0-9_$]+)\)return ([A-Za-z0-9_$]+)`)
	largeContextModelPattern             = regexp.MustCompile(`\b[A-Za-z0-9_$]+=A9\.([A-Z0-9_]+)\.name`)
	adaptiveThinkingModelsPattern        = regexp.MustCompile(`ZB=A9\.([A-Z0-9_]+)\.name,YpT=A9\.([A-Z0-9_]+)\.name,DpT=A9\.([A-Z0-9_]+)\.name`)
	adaptiveThinkingEffortPattern        = regexp.MustCompile(`if\(FOR\(R\)\)\{let e=\[([^\]]+)\]\.includes\(T\.reasoningEffort\)\?T\.reasoningEffort:"([^"]+)"`)
	adaptiveThinkingCurrentEffortPattern = regexp.MustCompile(`if\(([A-Za-z0-9_$]+)\(R\)\)\{let [A-Za-z0-9_$]+=\[([^\]]+)\]\.includes\(T\.reasoningEffort\)\?T\.reasoningEffort:"([^"]+)"`)
	adaptiveThinkingOutputPattern        = regexp.MustCompile(`thinking:\{type:"([^"]+)",display:"([^"]+)"\}.*output_config:\{effort:e\}`)
	providerReasoningPattern             = regexp.MustCompile(`case"anthropic":return [A-Za-z0-9_$]+\(c\)\?\?a\?\?\([A-Za-z0-9_$]+===A9\.([A-Z0-9_]+)\.name\?"([^"]+)":"([^"]+)"\);case"openai":return [A-Za-z0-9_$]+\(c\)\?\?a\?\?"([^"]+)";case"vertexai":return T\["([^"]+)"\]\?\?a\?\?"([^"]+)"`)
	ampHeaderConstantsPattern            = regexp.MustCompile(`Vw="([^"]+)",QRT="([^"]+)",LU="([^"]+)",ART="([^"]+)",RTT="([^"]+)",TTT="([^"]+)"`)
	anthropicFastModeBetaPattern         = regexp.MustCompile(`VpT="([^"]+)"`)
	anthropicThinkingBetaPattern         = regexp.MustCompile(`if\(\(R\["([^"]+)"\]\?\?!0\)&&R\["([^"]+)"\]&&!FOR\(e\)\)a\.push\("([^"]+)"\)`)
	anthropicProviderPattern             = regexp.MustCompile(`if\(R\["([^"]+)"\]\)c=R\["([^"]+)"\]`)
	anthropicSpeedPattern                = regexp.MustCompile(`if\(XpT\(e,R\["([^"]+)"\]\)==="([^"]+)"\)a\.push\(VpT\),c="([^"]+)"`)
	anthropicBetaHeaderPattern           = regexp.MustCompile(`\{"([^"]+)":a\.join\(","\)\}`)
	anthropicOverridePattern             = regexp.MustCompile(`\{"([^"]+)":c\}`)
	ampFeaturePattern                    = regexp.MustCompile(`\[Vw\]:"([^"]+)"`)
	settingKeyPattern                    = regexp.MustCompile(`"?([A-Za-z][A-Za-z0-9]*(?:\.[A-Za-z0-9]+)*)"?\s*:\s*\{value:`)
	settingDefaultPattern                = regexp.MustCompile(`"?([A-Za-z][A-Za-z0-9]*(?:\.[A-Za-z0-9]+)*)"?\s*:\s*\{value:((?:void 0|!0|!1|true|false|null|-?[0-9]+(?:\.[0-9]+)?|"[^"]{0,160}"|\[(?:"[^"]{0,160}"(?:,"[^"]{0,160}")*)?\]))`)
	threadIDPattern                      = regexp.MustCompile(`T-[0-9a-fA-Fx]{8,}-[0-9a-fA-Fx-]{8,}`)

	primaryModelRefPattern      = regexp.MustCompile(`primaryModel:[A-Za-z0-9_$]+\("([A-Z0-9_]+)"\)`)
	includeToolsRefPattern      = regexp.MustCompile(`includeTools:([A-Za-z0-9_$]+)`)
	reasoningEffortPattern      = regexp.MustCompile(`reasoningEffort:"([^"]+)"`)
	reasoningLevelsBlockPattern = regexp.MustCompile(`reasoningEffortControl:\{levels:\[([^\]]*)\]`)
	quotedStringPattern         = regexp.MustCompile(`"([^"]+)"`)
	httpMethodPropertyPattern   = regexp.MustCompile(`method\s*:\s*["']?\s*(GET|POST|PUT|PATCH|DELETE|OPTIONS|HEAD)\b`)
	httpQuotedMethodArgPattern  = regexp.MustCompile("[\"'](GET|POST|PUT|PATCH|DELETE|OPTIONS|HEAD)[\"']\\s*,\\s*[\"'`]?$")
	httpMethodCallPrefixPattern = regexp.MustCompile("(?i)\\.(get|post|put|patch|delete|options|head)\\s*\\(\\s*[\"'`]?$")
)

var knownDeltaNames = []string{
	"agent-mode",
	"environment",
	"main-thread",
	"max-tokens",
	"reasoning-effort",
	"title",
}

var knownThreadDeltaEvents = []string{
	"assistant:message",
	"assistant:message-update",
	"thread:truncate",
	"tool:data",
	"tool:processed",
	"user:message",
	"user:message-queue:dequeue",
	"user:message-queue:discard",
	"user:message-queue:enqueue",
	"user:message:append-content",
	"user:message:interrupt",
	"user:tool-input",
}

var knownRawThreadDeltaEventValues = map[string]struct{}{
	"agent-mode":                                {},
	"agent_state":                               {},
	"cancelled":                                 {},
	"client_append_manual_bash_invocation":      {},
	"client_append_user_msg":                    {},
	"client_cancel":                             {},
	"client_dismiss_active_error":               {},
	"client_edit_message":                       {},
	"client_filesystem_read_directory":          {},
	"client_filesystem_read_directory_result":   {},
	"client_filesystem_read_file":               {},
	"client_filesystem_read_file_result":        {},
	"client_git_command":                        {},
	"client_git_command_result":                 {},
	"client_mark_message_read":                  {},
	"client_mark_message_unread":                {},
	"client_remove_queued_msg":                  {},
	"client_resume":                             {},
	"client_retry":                              {},
	"client_set_thread_title":                   {},
	"client_spawn_executor":                     {},
	"client_steer_queued_msg":                   {},
	"client_tool_approval_response":             {},
	"client_update_thread_settings":             {},
	"client_upsert_notification_subscription":   {},
	"compaction_complete":                       {},
	"compaction_records":                        {},
	"compaction_started":                        {},
	"delta":                                     {},
	"edit_rejected":                             {},
	"environment":                               {},
	"environment_update":                        {},
	"error":                                     {},
	"error_cleared":                             {},
	"error_set":                                 {},
	"executor_connect":                          {},
	"executor_connected":                        {},
	"executor_environment_snapshot":             {},
	"executor_environment_update":               {},
	"executor_error":                            {},
	"executor_filesystem_read_directory":        {},
	"executor_filesystem_read_directory_result": {},
	"executor_filesystem_read_file":             {},
	"executor_filesystem_read_file_result":      {},
	"executor_git_command":                      {},
	"executor_git_command_result":               {},
	"executor_guidance_discovery":               {},
	"executor_guidance_snapshot":                {},
	"executor_plugin_message":                   {},
	"executor_skill_snapshot":                   {},
	"executor_status":                           {},
	"executor_tool_approval_request":            {},
	"executor_tool_approval_response":           {},
	"executor_tool_lease_ack":                   {},
	"executor_tool_lease_revoked":               {},
	"executor_tool_result":                      {},
	"executor_tool_result_ack":                  {},
	"executor_tools_bootstrap_complete":         {},
	"executor_tools_register":                   {},
	"executor_tools_unregister":                 {},
	"executor_workspace_maybe_changed":          {},
	"inference_tools":                           {},
	"message_added":                             {},
	"message_updated":                           {},
	"observers":                                 {},
	"plugin_message":                            {},
	"queued_message_added":                      {},
	"queued_message_dequeued":                   {},
	"queued_message_removed":                    {},
	"queued_messages":                           {},
	"retry_cancelled":                           {},
	"retry_scheduled":                           {},
	"retry_started":                             {},
	"thread_relationships":                      {},
	"thread_settings":                           {},
	"thread_status":                             {},
	"thread_title":                              {},
	"thread_truncated":                          {},
	"title":                                     {},
	"tool_approval_queue":                       {},
	"tool_lease":                                {},
	"tool_progress":                             {},
}

var knownThreadProtocolEvents = []string{
	"agent_state",
	"cancelled",
	"client_append_manual_bash_invocation",
	"client_append_user_msg",
	"client_cancel",
	"client_dismiss_active_error",
	"client_edit_message",
	"client_filesystem_read_directory",
	"client_filesystem_read_directory_result",
	"client_filesystem_read_file",
	"client_filesystem_read_file_result",
	"client_git_command",
	"client_git_command_result",
	"client_mark_message_read",
	"client_mark_message_unread",
	"client_remove_queued_msg",
	"client_resume",
	"client_retry",
	"client_set_thread_title",
	"client_spawn_executor",
	"client_steer_queued_msg",
	"client_tool_approval_response",
	"client_update_thread_settings",
	"client_upsert_notification_subscription",
	"compaction_complete",
	"compaction_records",
	"compaction_started",
	"delta",
	"edit_rejected",
	"environment_update",
	"error",
	"error_cleared",
	"error_set",
	"executor_connect",
	"executor_connected",
	"executor_environment_snapshot",
	"executor_environment_update",
	"executor_error",
	"executor_filesystem_read_directory",
	"executor_filesystem_read_directory_result",
	"executor_filesystem_read_file",
	"executor_filesystem_read_file_result",
	"executor_git_command",
	"executor_git_command_result",
	"executor_guidance_discovery",
	"executor_guidance_snapshot",
	"executor_plugin_message",
	"executor_skill_snapshot",
	"executor_status",
	"executor_tool_approval_request",
	"executor_tool_approval_response",
	"executor_tool_lease_ack",
	"executor_tool_lease_revoked",
	"executor_tool_result",
	"executor_tool_result_ack",
	"executor_tools_bootstrap_complete",
	"executor_tools_register",
	"executor_tools_unregister",
	"executor_workspace_maybe_changed",
	"inference_tools",
	"message_added",
	"message_updated",
	"observers",
	"plugin_message",
	"queued_message_added",
	"queued_message_dequeued",
	"queued_message_removed",
	"queued_messages",
	"retry_cancelled",
	"retry_scheduled",
	"retry_started",
	"thread_relationships",
	"thread_settings",
	"thread_status",
	"thread_title",
	"thread_truncated",
	"tool_approval_queue",
	"tool_lease",
	"tool_progress",
}

var allowedEventSegments = map[string]struct{}{
	"append-content": {},
	"data":           {},
	"dequeue":        {},
	"discard":        {},
	"enqueue":        {},
	"input":          {},
	"interrupt":      {},
	"message":        {},
	"message-queue":  {},
	"processed":      {},
	"tool":           {},
	"truncate":       {},
	"update":         {},
}

var ignoredThreadProtocolLiterals = map[string]struct{}{
	"tool_result": {},
	"tool_use":    {},
}

var threadDeltaAreas = map[string]string{
	"agent-mode":                  "settings",
	"agent_state":                 "execution-state",
	"assistant:message":           "assistant-message",
	"assistant:message-update":    "assistant-message",
	"cancelled":                   "execution-state",
	"compaction_complete":         "compaction",
	"compaction_records":          "compaction",
	"compaction_started":          "compaction",
	"delta":                       "assistant-message",
	"edit_rejected":               "error",
	"environment":                 "environment",
	"environment_update":          "environment",
	"error":                       "error",
	"error_cleared":               "error",
	"error_set":                   "error",
	"inference_tools":             "tool-state",
	"main-thread":                 "relationship",
	"max-tokens":                  "settings",
	"message_added":               "message",
	"message_updated":             "message",
	"observers":                   "observer",
	"plugin_message":              "plugin",
	"queued_message_added":        "queue",
	"queued_message_dequeued":     "queue",
	"queued_message_removed":      "queue",
	"queued_messages":             "queue",
	"reasoning-effort":            "settings",
	"retry_cancelled":             "retry",
	"retry_scheduled":             "retry",
	"retry_started":               "retry",
	"thread_relationships":        "relationship",
	"thread_settings":             "settings",
	"thread_status":               "status",
	"thread_title":                "metadata",
	"thread_truncated":            "history",
	"thread:truncate":             "history",
	"title":                       "metadata",
	"tool:data":                   "tool-result",
	"tool_approval_queue":         "tool-input",
	"tool_lease":                  "tool-state",
	"tool:processed":              "tool-result",
	"tool_progress":               "tool-result",
	"user:message":                "user-message",
	"user:message-queue:dequeue":  "queue",
	"user:message-queue:discard":  "queue",
	"user:message-queue:enqueue":  "queue",
	"user:message:append-content": "user-message",
	"user:message:interrupt":      "user-message",
	"user:tool-input":             "tool-input",
}

var toolCancelReasonAreas = map[string]string{
	"system:disposed":                 "runtime-dispose",
	"system:edited":                   "history-edit",
	"system:non-terminal-tool-result": "restore-cleanup",
	"system:safety":                   "system-safety",
	"user:cancelled":                  "user-cancel",
	"user:interrupted":                "user-interrupt",
}

var knownToolRunStatuses = []string{
	"blocked-on-user",
	"cancellation-requested",
	"cancelled",
	"done",
	"error",
	"in-progress",
	"queued",
	"rejected-by-user",
}

var toolRunStatusAreas = map[string]string{
	"blocked-on-user":        "pending",
	"cancellation-requested": "running",
	"cancelled":              "terminal-cancel",
	"done":                   "terminal-success",
	"error":                  "terminal-error",
	"in-progress":            "running",
	"queued":                 "pending",
	"rejected-by-user":       "terminal-error",
}

var toolCatalogMarkers = map[string]string{
	"Glob":                           "legacy-file-search",
	"Grep":                           "legacy-file-search",
	"Read":                           "legacy-file-read",
	"applyPatchFreeform":             "file-edit",
	"browser_navigate":               "browser",
	"browser_take_screenshot":        "browser",
	"builtin:edit_file":              "file-edit",
	"disableTools":                   "tool-spec-overrides",
	"enableToolSpecs":                "tool-spec-overrides",
	"experimental.tools":             "tool-filter",
	"file_tree":                      "file-search",
	"glob":                           "file-search",
	"mcpServers":                     "mcp",
	"mcp__server__tool":              "mcp",
	"read_file":                      "file-read",
	"ripgrep":                        "file-search",
	"skills.disableClaudeCodeSkills": "skills",
	"skills.path":                    "skills",
	"toolbox.path":                   "toolbox",
	"tools.disable":                  "tool-filter",
	"tools.enable":                   "tool-filter",
	"view_media":                     "media",
}

var knownToolCancelReasonValues = map[string]struct{}{
	"system:disposed":                 {},
	"system:edited":                   {},
	"system:non-terminal-tool-result": {},
	"system:safety":                   {},
	"user:cancelled":                  {},
	"user:interrupted":                {},
}

var knownToolRunStatusValues = map[string]struct{}{
	"blocked-on-user":        {},
	"cancellation-requested": {},
	"cancelled":              {},
	"done":                   {},
	"error":                  {},
	"in-progress":            {},
	"queued":                 {},
	"rejected-by-user":       {},
}

var knownToolCatalogMarkerValues = map[string]struct{}{
	"Glob":                           {},
	"Grep":                           {},
	"Read":                           {},
	"browser_navigate":               {},
	"browser_take_screenshot":        {},
	"builtin:edit_file":              {},
	"disableTools":                   {},
	"enableToolSpecs":                {},
	"experimental.tools":             {},
	"file_tree":                      {},
	"glob":                           {},
	"mcpServers":                     {},
	"mcp__server__tool":              {},
	"read_file":                      {},
	"ripgrep":                        {},
	"skills.disableClaudeCodeSkills": {},
	"skills.path":                    {},
	"toolbox.path":                   {},
	"tools.disable":                  {},
	"tools.enable":                   {},
	"view_media":                     {},
}

var threadReaderMarkers = map[string]string{
	"getThread":             "internal-rpc",
	"getThreadLabels":       "internal-rpc",
	"getThreadLinkInfo":     "internal-rpc",
	"getThreadMeta":         "internal-rpc",
	"getThreadTail":         "internal-rpc",
	"listThreads":           "internal-rpc",
	"loadThreadTail":        "internal-rpc",
	"loadThreads":           "internal-rpc",
	"message_stats":         "message-reader-route",
	"readThread":            "internal-rpc",
	"read_messages":         "message-reader-route",
	"search_messages":       "message-reader-route",
	"threadDisplayCostInfo": "internal-rpc",
}

var knownThreadReaderMarkerValues = map[string]struct{}{
	"getThread":             {},
	"getThreadLabels":       {},
	"getThreadLinkInfo":     {},
	"getThreadMeta":         {},
	"getThreadTail":         {},
	"listThreads":           {},
	"loadThreadTail":        {},
	"loadThreads":           {},
	"message_stats":         {},
	"readThread":            {},
	"read_messages":         {},
	"search_messages":       {},
	"threadDisplayCostInfo": {},
}

var expectedThreadReaderMarkerValues = map[string]struct{}{
	"getThreadTail":   {},
	"listThreads":     {},
	"loadThreadTail":  {},
	"loadThreads":     {},
	"message_stats":   {},
	"read_messages":   {},
	"search_messages": {},
}

var streamJSONMarkers = map[string]string{
	"--stream-json":          "cli-flag",
	"--stream-json-input":    "cli-flag",
	"--stream-json-thinking": "cli-flag",
	"agent_mode":             "init-field",
	"duration_ms":            "result-field",
	"error_during_execution": "error-subtype",
	"is_error":               "result-field",
	"mcp_servers":            "init-field",
	"num_turns":              "result-field",
	"reasoning_effort":       "init-field",
	"session_id":             "shared-field",
	"stream-json":            "execute-mode",
}

var modeSettingMarkers = map[string]string{
	"agentMode":                 "thread-metadata",
	"anthropic.speed":           "provider-speed",
	"draftThreadSettings":       "draft-settings",
	"explicitEffort":            "session-default",
	"gemini.thinkingLevel":      "provider-thinking",
	"internal.model":            "thread-setting",
	"lastReasoningEffortByMode": "session-default",
	"lastSpeedByMode":           "session-default",
	"openai.speed":              "provider-speed",
	"reasoning.effort":          "thread-setting",
	"reasoningEffort":           "thread-metadata",
	"sessionAgentMode":          "session-default",
}

var providerProtocolMarkers = map[string]string{
	"2023-06-01":           "anthropic-version-value",
	"amp.chat":             "amp-feature",
	"amp.image-generation": "amp-feature",
	"amp.painter":          "amp-feature",
	"amp.read-thread":      "amp-feature",
	"amp.review":           "amp-feature",
	"anthropic-beta":       "anthropic-header",
	"anthropic-dangerous-direct-browser-access": "anthropic-header",
	"anthropic-version":                         "anthropic-header",
	"fast-mode-2026-02-01":                      "anthropic-beta",
	"files-api-2025-04-14":                      "anthropic-beta",
	"google-upload-url":                         "google-upload",
	"interleaved-thinking-2025-05-14":           "anthropic-beta",
	"message-batches-2024-09-24":                "anthropic-beta",
	"nightly-2025-12-10":                        "anthropic-beta",
	"openai-poll-after-ms":                      "openai-protocol",
	"openai-websocket":                          "openai-protocol",
	"skills-2025-10-02":                         "anthropic-beta",
	"structured-outputs-2025-12-15":             "anthropic-beta",
	"token-counting-2024-11-01":                 "anthropic-beta",
	"x-amp-client-application":                  "amp-client-header",
	"x-amp-client-type":                         "amp-client-header",
	"x-amp-client-version":                      "amp-client-header",
	"x-amp-device-fingerprint":                  "amp-client-header",
	"x-amp-feature":                             "amp-provider-header",
	"x-amp-installation-id":                     "amp-client-header",
	"x-amp-message-id":                          "amp-provider-header",
	"x-amp-override-provider":                   "amp-provider-header",
	"x-amp-thread-id":                           "amp-provider-header",
	"x-amp-user":                                "amp-provider-header",
}

var knownProviderReasoningRuleValues = map[string]struct{}{
	"anthropic|sources=setting:reasoning.effort,mode:reasoningEffort,model-default|setting=|default=high|special=CLAUDE_OPUS_4_7/claude-opus-4-7:medium": {},
	"openai|sources=setting:reasoning.effort,mode:reasoningEffort,provider-default|setting=|default=medium":                                              {},
	"vertexai|sources=setting:gemini.thinkingLevel,mode:reasoningEffort,provider-default|setting=gemini.thinkingLevel|default=medium":                    {},
}

var knownProviderHeaderRuleValues = map[string]struct{}{
	"anthropic|feature=x-amp-feature:amp.chat|thread_id=x-amp-thread-id<-thread.id|message_id=x-amp-message-id<-message-id-argument|beta_header=anthropic-beta|interleaved=interleaved-thinking-2025-05-14@anthropic.thinking.enabled+anthropic.interleavedThinking.enabled+skip_adaptive=true|override=x-amp-override-provider<-anthropic.provider|fast=fast-mode-2026-02-01@anthropic.speed=fast=>anthropic": {},
}

var knownProviderFeatureRuleValues = map[string]struct{}{
	"amp.chat|provider=anthropic|callsite=anthropic-chat|tool=|header=x-amp-feature|default=true":                           {},
	"amp.chat|provider=google|callsite=google-client-default|tool=|header=x-amp-feature|default=true":                       {},
	"amp.chat|provider=openai|callsite=openai-compatible-client-default|tool=|header=x-amp-feature|default=true":            {},
	"amp.image-generation|provider=google|callsite=google-image-generation-default|tool=|header=x-amp-feature|default=true": {},
	"amp.image-generation|provider=openai|callsite=openai-image-generation-default|tool=|header=x-amp-feature|default=true": {},
	"amp.painter|provider=google|callsite=painter-gemini-image|tool=painter|header=x-amp-feature|default=false":             {},
	"amp.painter|provider=openai|callsite=painter-openai-image|tool=painter|header=x-amp-feature|default=false":             {},
	"amp.read-thread|provider=google|callsite=thread-reader|tool=read_thread|header=x-amp-feature|default=false":            {},
	"amp.review|provider=google|callsite=code-review|tool=code_review|header=x-amp-feature|default=false":                   {},
}

var knownLargeContextRuleValues = map[string]struct{}{
	"CLAUDE_OPUS_4_6|alias=claude-opus-4-6-1m|context=1000000|max_out=32000|max_input=968000|requires_enable=true": {},
}

var knownAdaptiveThinkingRuleValues = map[string]struct{}{
	"CLAUDE_OPUS_4_6,CLAUDE_OPUS_4_7,CLAUDE_OPUS_4_8|models=claude-opus-4-6,claude-opus-4-7,claude-opus-4-8|levels=low,medium,high,xhigh,max|default=medium|type=adaptive|display=summarized|output_config=true": {},
}

var knownCompactionRuleValues = map[string]struct{}{
	"anthropic-tool-runner|provider=anthropic|trigger=observed-usage|timing=post-response|threshold=100000|usage=input_tokens,cache_creation_input_tokens,cache_read_input_tokens,output_tokens|summary_prompt=continuation-summary|history_role=user|tail_assistant=strip-tool-use-blocks|helper=x-stainless-helper:compaction": {},
}

var knownPromptTagCountValues = map[string]int{
	"prompt/code-review":   4,
	"prompt/compaction":    13,
	"prompt/guidance":      22,
	"prompt/painter":       1,
	"prompt/skills":        17,
	"prompt/system-prompt": 12,
	"prompt/tools":         75,
	"source/artifacts":     4,
	"source/code-review":   9,
	"source/compaction":    32,
	"source/guidance":      41,
	"source/painter":       14,
	"source/settings":      14,
	"source/skills":        49,
	"source/system-prompt": 7,
	"source/tools":         200,
}

var knownPromptKindCountValues = map[string]int{
	"prompt": 119,
	"source": 241,
}

type agentModeMarker struct {
	Name  string
	Token string
}

var agentModeMarkers = []agentModeMarker{
	{Name: "deep", Token: `DEEP:{key:"deep"`},
	{Name: "smart", Token: `SMART:{key:"smart"`},
	{Name: "rush", Token: `RUSH:{key:"rush"`},
	{Name: "agg-man", Token: `AGG:{key:"agg-man"`},
	{Name: "large", Token: `LARGE:{key:"large"`},
	{Name: "nostromo", Token: `NOSTROMO:{key:`},
}

var agentModeScopes = map[string]string{
	"agg-man":  "server-only",
	"deep":     "local-runtime",
	"large":    "local-runtime",
	"nostromo": "local-runtime",
	"rush":     "local-runtime",
	"smart":    "local-runtime",
}

var knownAgentModeProfileValues = map[string]struct{}{
	"agg-man|primary=CLAUDE_OPUS_4_6|reasoning=|levels=|include=present|deferred=false|visible=false|visibleInV2=false|serverOnly=true":               {},
	"deep|primary=GPT_5_5|reasoning=medium|levels=low,medium,xhigh|include=present|deferred=true|visible=true|visibleInV2=true|serverOnly=false":      {},
	"large|primary=CLAUDE_OPUS_4_6|reasoning=|levels=|include=present|deferred=true|visible=true|visibleInV2=false|serverOnly=false":                  {},
	"nostromo|primary=AMP_NOSTROMO|reasoning=low|levels=|include=present|deferred=false|visible=true|visibleInV2=true|serverOnly=false":               {},
	"rush|primary=GPT_5_5|reasoning=none|levels=|include=present|deferred=false|visible=true|visibleInV2=false|serverOnly=false":                      {},
	"smart|primary=CLAUDE_OPUS_4_7|reasoning=high|levels=high,max,xhigh|include=present|deferred=true|visible=true|visibleInV2=true|serverOnly=false": {},
}

var knownAgentModeRouteValues = map[string]struct{}{
	"agg-man|provider=anthropic|model=claude-opus-4-6|primary=CLAUDE_OPUS_4_6|reasoning=|context=332000|max_out=32000":                                                                                   {},
	"deep|provider=openai|model=gpt-5.5|primary=GPT_5_5|reasoning=medium|context=400000|max_out=128000":                                                                                                  {},
	"large|provider=anthropic|model=claude-opus-4-6|primary=CLAUDE_OPUS_4_6|reasoning=|context=332000|max_out=32000|effective_context=1000000|effective_max_input=968000|large_alias=claude-opus-4-6-1m": {},
	"nostromo|provider=openai|model=amp-nostromo-v1|primary=AMP_NOSTROMO|reasoning=low|context=400000|max_out=128000":                                                                                    {},
	"rush|provider=openai|model=gpt-5.5|primary=GPT_5_5|reasoning=none|context=400000|max_out=128000":                                                                                                    {},
	"smart|provider=anthropic|model=claude-opus-4-7|primary=CLAUDE_OPUS_4_7|reasoning=high|context=332000|max_out=32000":                                                                                 {},
}

var ignoredToolCancelReasonTokens = map[string]struct{}{
	"system:this": {},
}

var standaloneSettingNames = map[string]struct{}{
	"bitbucketToken":      {},
	"dangerouslyAllowAll": {},
	"defaultVisibility":   {},
	"gauge":               {},
	"mcpServers":          {},
	"permissions":         {},
	"proxy":               {},
	"showCosts":           {},
	"submitOnEnter":       {},
	"systemPrompt":        {},
	"url":                 {},
}

var settingScopes = map[string]string{
	"agent.skipTitleGenerationIfMessageContains":    "local-runtime",
	"anthropic.interleavedThinking.enabled":         "local-runtime",
	"anthropic.provider":                            "local-runtime",
	"anthropic.speed":                               "local-runtime",
	"anthropic.temperature":                         "local-runtime",
	"anthropic.thinking.enabled":                    "local-runtime",
	"bitbucketToken":                                "amp-owned",
	"dangerouslyAllowAll":                           "amp-owned",
	"defaultVisibility":                             "amp-owned",
	"experimental.applyPatchFreeform.enabled":       "local-runtime",
	"experimental.cli.nativeSecretsStorage.enabled": "amp-owned",
	"experimental.modes":                            "local-runtime",
	"experimental.tools":                            "local-runtime",
	"fuzzy.alwaysIncludePaths":                      "local-runtime",
	"gauge":                                         "remote-web",
	"gemini.thinkingLevel":                          "local-runtime",
	"git.commit.ampThread.enabled":                  "amp-owned",
	"git.commit.coauthor.enabled":                   "amp-owned",
	"guardedFiles.allowlist":                        "local-runtime",
	"jetbrains.skipInstall":                         "amp-owned",
	"mcpServers":                                    "local-runtime",
	"network.timeout":                               "amp-owned",
	"notifications.enabled":                         "amp-owned",
	"notifications.system.enabled":                  "amp-owned",
	"openai.speed":                                  "local-runtime",
	"painter.model":                                 "local-runtime",
	"permissions":                                   "local-runtime",
	"proxy":                                         "amp-owned",
	"showCosts":                                     "remote-web",
	"skills.disableClaudeCodeSkills":                "local-runtime",
	"skills.path":                                   "local-runtime",
	"submitOnEnter":                                 "remote-web",
	"systemPrompt":                                  "local-runtime",
	"terminal.animation":                            "remote-web",
	"terminal.copyOnSelect":                         "remote-web",
	"terminal.theme":                                "remote-web",
	"toolbox.path":                                  "local-runtime",
	"tools.disable":                                 "local-runtime",
	"tools.enable":                                  "local-runtime",
	"updates.mode":                                  "amp-owned",
	"url":                                           "amp-owned",
}

var knownSettingDefaultValues = map[string]string{
	"agent.skipTitleGenerationIfMessageContains":    "[]",
	"anthropic.interleavedThinking.enabled":         "false",
	"anthropic.provider":                            "anthropic",
	"anthropic.speed":                               "undefined",
	"anthropic.temperature":                         "1",
	"anthropic.thinking.enabled":                    "false",
	"bitbucketToken":                                "undefined",
	"dangerouslyAllowAll":                           "false",
	"experimental.applyPatchFreeform.enabled":       "false",
	"experimental.cli.nativeSecretsStorage.enabled": "false",
	"experimental.modes":                            "[]",
	"experimental.tools":                            "[]",
	"fuzzy.alwaysIncludePaths":                      "[]",
	"gauge":                                         "cost",
	"gemini.thinkingLevel":                          "undefined",
	"git.commit.ampThread.enabled":                  "true",
	"git.commit.coauthor.enabled":                   "true",
	"guardedFiles.allowlist":                        "[]",
	"jetbrains.skipInstall":                         "false",
	"network.timeout":                               "30",
	"notifications.enabled":                         "true",
	"notifications.system.enabled":                  "true",
	"openai.speed":                                  "undefined",
	"painter.model":                                 "gpt-image-2",
	"proxy":                                         "undefined",
	"showCosts":                                     "true",
	"skills.disableClaudeCodeSkills":                "false",
	"skills.path":                                   "undefined",
	"submitOnEnter":                                 "true",
	"systemPrompt":                                  "undefined",
	"terminal.animation":                            "true",
	"terminal.copyOnSelect":                         "true",
	"terminal.theme":                                "terminal",
	"toolbox.path":                                  "undefined",
	"tools.disable":                                 `["browser_navigate","builtin:edit_file"]`,
	"tools.enable":                                  "undefined",
	"updates.mode":                                  "auto",
	"url":                                           "https://ampcode.com",
}

var knownRawModelValues = map[string]struct{}{
	"amp-nostromo-v1":             {},
	"claude-1.3":                  {},
	"claude-1.3-100k":             {},
	"claude-2.0":                  {},
	"claude-2.1":                  {},
	"claude-3-5-haiku-20241022":   {},
	"claude-3-5-haiku-latest":     {},
	"claude-3-7-sonnet-20250219":  {},
	"claude-3-7-sonnet-latest":    {},
	"claude-3-opus-20240229":      {},
	"claude-3-sonnet-20240229":    {},
	"claude-4-opus-20250514":      {},
	"claude-haiku-4-5-20251001":   {},
	"claude-instant-1.1":          {},
	"claude-instant-1.1-100k":     {},
	"claude-instant-1.2":          {},
	"claude-opus-4":               {},
	"claude-opus-4-0":             {},
	"claude-opus-4-1":             {},
	"claude-opus-4-1-20250805":    {},
	"claude-opus-4-1-20250805-v1": {},
	"claude-opus-4-20250514":      {},
	"claude-opus-4-20250514-v1":   {},
	"claude-opus-4-5-20251101":    {},
	"claude-opus-4-6":             {},
	"claude-opus-4-6-1m":          {},
	"claude-opus-4-7":             {},
	"claude-opus-4-8":             {},
	"claude-sonnet-4-20250514":    {},
	"claude-sonnet-4-5-20250929":  {},
	"claude-sonnet-4-6":           {},
	"gemini-3-flash-preview":      {},
	"gemini-3-pro-image":          {},
	"gemini-3-pro-image-preview":  {},
	"gemini-3-pro-preview":        {},
	"gemini-3.1-pro-preview":      {},
	"gemini-3.5-flash":            {},
	"gemini-embedding-001":        {},
	"gemini-embedding-2":          {},
	"gpt-5":                       {},
	"gpt-5-codex":                 {},
	"gpt-5-mini":                  {},
	"gpt-5-nano":                  {},
	"gpt-5.1":                     {},
	"gpt-5.1-codex":               {},
	"gpt-5.2":                     {},
	"gpt-5.2-codex":               {},
	"gpt-5.3-codex":               {},
	"gpt-5.4":                     {},
	"gpt-5.4-pro":                 {},
	"gpt-5.5":                     {},
	"gpt-5.5-pro":                 {},
	"gpt-image-2":                 {},
	"gpt-oss-120b":                {},
	"o3-mini":                     {},
}

var knownModelLimitValues = map[string]modelLimitExpectation{
	"accounts/fireworks/models/glm-4p6":                        {Enum: "FIREWORKS_GLM_4P6", Provider: "fireworks", DisplayName: "GLM 4P6", ContextWindow: 162752, MaxOutputTokens: 40000},
	"accounts/fireworks/models/glm-5":                          {Enum: "FIREWORKS_GLM_5", Provider: "fireworks", DisplayName: "GLM 5", ContextWindow: 202800, MaxOutputTokens: 40000},
	"accounts/fireworks/models/kimi-k2-instruct-0905":          {Enum: "FIREWORKS_KIMI_K2_INSTRUCT", Provider: "fireworks", DisplayName: "Kimi K2 Instruct", ContextWindow: 230144, MaxOutputTokens: 32000},
	"accounts/fireworks/models/minimax-m2p5":                   {Enum: "FIREWORKS_MINIMAX_M2P5", Provider: "fireworks", DisplayName: "MiniMax M2.5", ContextWindow: 200000, MaxOutputTokens: 32000},
	"accounts/fireworks/models/qwen3-235b-a22b-instruct-2507":  {Enum: "FIREWORKS_QWEN3_235B", Provider: "fireworks", DisplayName: "Qwen3 235B", ContextWindow: 230144, MaxOutputTokens: 32000},
	"accounts/fireworks/models/qwen3-coder-480b-a35b-instruct": {Enum: "FIREWORKS_QWEN3_CODER_480B", Provider: "fireworks", DisplayName: "Qwen3 Coder 480B", ContextWindow: 230144, MaxOutputTokens: 32000},
	"amp-nostromo-v1":            {Enum: "AMP_NOSTROMO", Provider: "openai", DisplayName: "nostromo", ContextWindow: 400000, MaxOutputTokens: 128000},
	"claude-haiku-4-5-20251001":  {Enum: "CLAUDE_HAIKU_4_5", Provider: "anthropic", DisplayName: "Claude Haiku 4.5", ContextWindow: 200000, MaxOutputTokens: 64000},
	"claude-opus-4-1-20250805":   {Enum: "CLAUDE_OPUS_4_1", Provider: "anthropic", DisplayName: "Claude Opus 4.1", ContextWindow: 200000, MaxOutputTokens: 32000},
	"claude-opus-4-20250514":     {Enum: "CLAUDE_OPUS_4", Provider: "anthropic", DisplayName: "Claude Opus 4", ContextWindow: 200000, MaxOutputTokens: 32000},
	"claude-opus-4-5-20251101":   {Enum: "CLAUDE_OPUS_4_5", Provider: "anthropic", DisplayName: "Claude Opus 4.5", ContextWindow: 200000, MaxOutputTokens: 32000},
	"claude-opus-4-6":            {Enum: "CLAUDE_OPUS_4_6", Provider: "anthropic", DisplayName: "Claude Opus 4.6", ContextWindow: 332000, MaxOutputTokens: 32000},
	"claude-opus-4-7":            {Enum: "CLAUDE_OPUS_4_7", Provider: "anthropic", DisplayName: "Claude Opus 4.7", ContextWindow: 332000, MaxOutputTokens: 32000},
	"claude-opus-4-8":            {Enum: "CLAUDE_OPUS_4_8", Provider: "anthropic", DisplayName: "Claude Opus 4.8", ContextWindow: 332000, MaxOutputTokens: 32000},
	"claude-sonnet-4-20250514":   {Enum: "CLAUDE_SONNET_4", Provider: "anthropic", DisplayName: "Claude Sonnet 4", ContextWindow: 1000000, MaxOutputTokens: 32000},
	"claude-sonnet-4-5-20250929": {Enum: "CLAUDE_SONNET_4_5", Provider: "anthropic", DisplayName: "Claude Sonnet 4.5", ContextWindow: 1000000, MaxOutputTokens: 32000},
	"claude-sonnet-4-6":          {Enum: "CLAUDE_SONNET_4_6", Provider: "anthropic", DisplayName: "Claude Sonnet 4.6", ContextWindow: 1000000, MaxOutputTokens: 64000},
	"gemini-3-flash-preview":     {Enum: "GEMINI3_FLASH_PREVIEW", Provider: "google", DisplayName: "Gemini 3 Flash Preview", ContextWindow: 1048576, MaxOutputTokens: 65535},
	"gemini-3-pro-image-preview": {Enum: "GEMINI_3_PRO_IMAGE", Provider: "google", DisplayName: "Gemini 3 Pro Image", ContextWindow: 1048576, MaxOutputTokens: 65535},
	"gemini-3-pro-preview":       {Enum: "GEMINI_3_PRO_PREVIEW", Provider: "google", DisplayName: "Gemini 3 Pro Preview", ContextWindow: 1048576, MaxOutputTokens: 65535},
	"gemini-3.1-pro-preview":     {Enum: "GEMINI_3_1_PRO_PREVIEW", Provider: "google", DisplayName: "Gemini 3.1 Pro Preview", ContextWindow: 1048576, MaxOutputTokens: 65535},
	"gemini-3.5-flash":           {Enum: "GEMINI_3_5_FLASH", Provider: "google", DisplayName: "Gemini 3.5 Flash", ContextWindow: 1048576, MaxOutputTokens: 65535},
	"gpt-5":                      {Enum: "GPT_5", Provider: "openai", DisplayName: "GPT-5", ContextWindow: 400000, MaxOutputTokens: 128000},
	"gpt-5-codex":                {Enum: "GPT_5_CODEX", Provider: "openai", DisplayName: "GPT-5 Codex", ContextWindow: 400000, MaxOutputTokens: 128000},
	"gpt-5-mini":                 {Enum: "GPT_5_MINI", Provider: "openai", DisplayName: "GPT-5 Mini", ContextWindow: 400000, MaxOutputTokens: 128000},
	"gpt-5-nano":                 {Enum: "GPT_5_NANO", Provider: "openai", DisplayName: "GPT-5 Nano", ContextWindow: 400000, MaxOutputTokens: 128000},
	"gpt-5.1":                    {Enum: "GPT_5_1", Provider: "openai", DisplayName: "GPT-5.1", ContextWindow: 400000, MaxOutputTokens: 128000},
	"gpt-5.1-codex":              {Enum: "GPT_5_1_CODEX", Provider: "openai", DisplayName: "GPT-5.1 Codex", ContextWindow: 400000, MaxOutputTokens: 128000},
	"gpt-5.2":                    {Enum: "GPT_5_2", Provider: "openai", DisplayName: "GPT-5.2", ContextWindow: 400000, MaxOutputTokens: 128000},
	"gpt-5.2-codex":              {Enum: "GPT_5_2_CODEX", Provider: "openai", DisplayName: "GPT-5.2 Codex", ContextWindow: 400000, MaxOutputTokens: 128000},
	"gpt-5.3-codex":              {Enum: "GPT_5_3_CODEX", Provider: "openai", DisplayName: "GPT-5.3 Codex", ContextWindow: 400000, MaxOutputTokens: 128000},
	"gpt-5.4":                    {Enum: "GPT_5_4", Provider: "openai", DisplayName: "GPT-5.4", ContextWindow: 400000, MaxOutputTokens: 128000},
	"gpt-5.4-pro":                {Enum: "GPT_5_4_PRO", Provider: "openai", DisplayName: "GPT-5.4-Pro", ContextWindow: 1050000, MaxOutputTokens: 128000},
	"gpt-5.5":                    {Enum: "GPT_5_5", Provider: "openai", DisplayName: "GPT-5.5", ContextWindow: 400000, MaxOutputTokens: 128000},
	"gpt-5.5-pro":                {Enum: "GPT_5_5_PRO", Provider: "openai", DisplayName: "GPT-5.5-Pro", ContextWindow: 1050000, MaxOutputTokens: 128000},
	"grok-code-fast-1":           {Enum: "GROK_CODE_FAST_1", Provider: "xai", DisplayName: "Grok Code Fast 1", ContextWindow: 256000, MaxOutputTokens: 32000},
	"kimi-k2-instruct-0905":      {Enum: "KIMI_K2_INSTRUCT", Provider: "moonshotai", DisplayName: "Kimi K2 Instruct", ContextWindow: 1000000, MaxOutputTokens: 32000},
	"moonshotai/Kimi-K2.5":       {Enum: "BASETEN_KIMI_K2P5", Provider: "baseten", DisplayName: "Kimi K2.5", ContextWindow: 262144, MaxOutputTokens: 32000},
	"moonshotai/kimi-k2-0905":    {Enum: "OPENROUTER_KIMI_K2_0905", Provider: "openrouter", DisplayName: "Kimi K2 0905 (OpenRouter)", ContextWindow: 262144, MaxOutputTokens: 32000},
	"o3":                         {Enum: "O3", Provider: "openai", DisplayName: "o3", ContextWindow: 200000, MaxOutputTokens: 1},
	"o3-mini":                    {Enum: "O3_MINI", Provider: "openai", DisplayName: "o3-mini", ContextWindow: 200000, MaxOutputTokens: 1},
	"openai/gpt-oss-120b":        {Enum: "GPT_OSS_120B", Provider: "openai", DisplayName: "GPT OSS 120B", ContextWindow: 128000, MaxOutputTokens: 32000},
	"qwen/qwen3-235b-a22b-2507":  {Enum: "OPENROUTER_QWEN3_235B", Provider: "openrouter", DisplayName: "Qwen3 235B A22B (OpenRouter)", ContextWindow: 262144, MaxOutputTokens: 32000},
	"qwen/qwen3-coder":           {Enum: "OPENROUTER_QWEN3_CODER_480B", Provider: "openrouter", DisplayName: "Qwen3 Coder 480B (OpenRouter)", ContextWindow: 262144, MaxOutputTokens: 32000},
	"sonoma-sky-alpha":           {Enum: "SONOMA_SKY_ALPHA", Provider: "openrouter", DisplayName: "Sonoma Sky Alpha", ContextWindow: 256000, MaxOutputTokens: 32000},
	"z-ai/glm-4.6":               {Enum: "OPENROUTER_GLM_4_6", Provider: "openrouter", DisplayName: "OpenRouter GLM 4.6", ContextWindow: 131000, MaxOutputTokens: 40000},
	"zai-glm-4.7":                {Enum: "Z_AI_GLM_4_7", Provider: "cerebras", DisplayName: "Z.ai GLM 4.7", ContextWindow: 131000, MaxOutputTokens: 40000},
}

var routeScopes = []routeScopeRule{
	{Prefix: "/api/internal/github-proxy", Scope: "amp-owned"},
	{Prefix: "/api/provider", Scope: "local-runtime"},
	{Prefix: "/api/thread-actors", Scope: "local-runtime"},
	{Prefix: "/api/attachments", Scope: "local-runtime"},
	{Prefix: "/actors", Scope: "local-runtime"},
	{Prefix: "/gateway", Scope: "local-runtime"},
	{Prefix: "/metadata", Scope: "local-runtime"},
	{Prefix: "/api/internal", Scope: "remote-web"},
	{Prefix: "/api/threads", Scope: "amp-owned"},
	{Prefix: "/threads", Scope: "amp-owned"},
	{Prefix: "/auth", Scope: "amp-owned"},
	{Prefix: "/docs", Scope: "amp-owned"},
	{Prefix: "/api/telemetry", Scope: "amp-owned"},
	{Prefix: "/api/user-actor-credentials", Scope: "amp-owned"},
}

var knownRouteValues = map[string]struct{}{
	"/actors":                     {},
	"/actors/":                    {},
	"/actors?actor_ids=":          {},
	"/actors?name=":               {},
	"/api/attachments":            {},
	"/api/internal":               {},
	"/api/internal/github-proxy/": {},
	"/api/internal?":              {},
	"/api/provider/anthropic":     {},
	"/api/provider/google":        {},
	"/api/provider/openai/v1":     {},
	"/api/telemetry":              {},
	"/api/thread-actors":          {},
	"/api/thread-actors/":         {},
	"/api/threads/":               {},
	"/api/threads/find?":          {},
	"/api/user-actor-credentials": {},
	"/auth":                       {},
	"/auth/callback":              {},
	"/auth/cli-login?authToken=":  {},
	"/docs":                       {},
	"/gateway/":                   {},
	"/metadata":                   {},
	"/threads":                    {},
	"/threads/":                   {},
	"/threads/runs":               {},
}

var knownRouteMethodValues = map[string]struct{}{
	"/actors=POST,PUT=local-runtime":             {},
	"/actors/=DELETE,GET=local-runtime":          {},
	"/actors?actor_ids==GET=local-runtime":       {},
	"/actors?name==GET=local-runtime":            {},
	"/api/attachments=POST=local-runtime":        {},
	"/api/internal=POST=remote-web":              {},
	"/api/internal?=POST=remote-web":             {},
	"/api/telemetry=POST=amp-owned":              {},
	"/api/thread-actors=POST=local-runtime":      {},
	"/api/thread-actors/=POST=local-runtime":     {},
	"/api/user-actor-credentials=POST=amp-owned": {},
	"/metadata=GET=local-runtime":                {},
	"/threads=POST=amp-owned":                    {},
	"/threads/runs=POST=amp-owned":               {},
}

var actorMarkerAreas = map[string]string{
	"RivetKit":               "actor-protocol",
	"clientApplication":      "client-metadata",
	"durable-thread-workers": "amp-owned",
	"executorType":           "local-runtime",
	"getOrCreate":            "local-runtime",
	"getRecentThreads":       "local-runtime",
	"local-client":           "local-runtime",
	"pingIntervalMs":         "actor-protocol",
	"rivet_encoding.json":    "actor-protocol",
	"rvt-key":                "actor-protocol",
	"rvt-method":             "actor-protocol",
	"rvt-runner":             "actor-protocol",
	"rvt-token":              "actor-protocol",
	"skipReadyWait":          "actor-protocol",
	"threadActor":            "local-runtime",
	"threadActorTransport":   "actor-protocol",
	"threadStatusUpdated":    "local-runtime",
	"userActor":              "local-runtime",
	"usesThreadActors":       "local-runtime",
	"wsToken":                "local-runtime",
}

var knownActorMarkerValues = map[string]struct{}{
	"RivetKit":             {},
	"clientApplication":    {},
	"executorType":         {},
	"getOrCreate":          {},
	"getRecentThreads":     {},
	"local-client":         {},
	"pingIntervalMs":       {},
	"rvt-key":              {},
	"rvt-method":           {},
	"rvt-runner":           {},
	"rvt-token":            {},
	"skipReadyWait":        {},
	"threadActor":          {},
	"threadActorTransport": {},
	"threadStatusUpdated":  {},
	"userActor":            {},
	"usesThreadActors":     {},
	"wsToken":              {},
}

var routePrefixes = []string{
	"/actors",
	"/api/1.0",
	"/api/attachments",
	"/api/internal",
	"/api/meta",
	"/api/otel",
	"/api/provider",
	"/api/tab",
	"/api/telemetry",
	"/api/thread-actors",
	"/api/threads",
	"/api/user",
	"/api/user-actor-credentials",
	"/api/v2",
	"/auth",
	"/docs",
	"/durable-thread-workers",
	"/gateway",
	"/metadata",
	"/settings",
	"/tab",
	"/threads",
}

var ignoredRouteExtensions = map[string]struct{}{
	".cpp":  {},
	".h":    {},
	".html": {},
	".js":   {},
	".json": {},
	".md":   {},
	".ts":   {},
	".zig":  {},
}

var actorMarkers = []string{
	"RivetKit",
	"clientApplication",
	"durable-thread-workers",
	"executorType",
	"getOrCreate",
	"getRecentThreads",
	"local-client",
	"pingIntervalMs",
	"rivet_encoding.json",
	"rvt-key",
	"rvt-method",
	"rvt-runner",
	"rvt-token",
	"skipReadyWait",
	"threadActor",
	"threadActorTransport",
	"threadStatusUpdated",
	"userActor",
	"usesThreadActors",
	"wsToken",
}

type Snapshot struct {
	Schema    int        `json:"schema"`
	Source    SourceInfo `json:"source"`
	Signals   Signals    `json:"signals"`
	Generated string     `json:"generated_at"`
}

type SourceInfo struct {
	Path           string   `json:"path"`
	SHA256         string   `json:"sha256"`
	SizeBytes      int64    `json:"size_bytes"`
	ModTime        string   `json:"mod_time"`
	Versions       []string `json:"versions,omitempty"`
	BuildStamps    []string `json:"build_stamps,omitempty"`
	StringsScanned int      `json:"strings_scanned"`
}

type Signals struct {
	Routes               []string                `json:"routes"`
	RouteMethods         []RouteMethods          `json:"route_methods"`
	RouteCoverage        []RouteCoverage         `json:"route_coverage"`
	ThreadDeltaEvents    []string                `json:"thread_delta_events"`
	ThreadDeltaCoverage  []ThreadDeltaCoverage   `json:"thread_delta_coverage"`
	ThreadReaderMarkers  []string                `json:"thread_reader_markers"`
	ThreadReaderCoverage []ThreadReaderCoverage  `json:"thread_reader_coverage"`
	ToolCancelReasons    []string                `json:"tool_cancel_reasons"`
	ToolCancelCoverage   []ToolCancelCoverage    `json:"tool_cancel_coverage"`
	ToolRunStatuses      []string                `json:"tool_run_statuses"`
	ToolRunCoverage      []ToolRunCoverage       `json:"tool_run_coverage"`
	ToolCatalog          []string                `json:"tool_catalog_markers"`
	ToolCatalogCoverage  []ToolCatalogCoverage   `json:"tool_catalog_coverage"`
	StreamJSONMarkers    []string                `json:"stream_json_markers"`
	StreamJSONCoverage   []StreamJSONCoverage    `json:"stream_json_coverage"`
	ModeSettingMarkers   []string                `json:"mode_setting_markers"`
	ModeSettingCoverage  []ModeSettingCoverage   `json:"mode_setting_coverage"`
	ProviderProtocol     []string                `json:"provider_protocol_markers"`
	ProviderCoverage     []ProviderCoverage      `json:"provider_protocol_coverage"`
	AgentModeProfiles    []AgentModeProfile      `json:"agent_mode_profiles"`
	AgentModeRoutes      []AgentModeRoute        `json:"agent_mode_routes"`
	AgentModeCoverage    []AgentModeCoverage     `json:"agent_mode_coverage"`
	Settings             []string                `json:"settings"`
	SettingDefaults      []SettingDefault        `json:"setting_defaults"`
	SettingCoverage      []SettingCoverage       `json:"setting_coverage"`
	Models               []string                `json:"models"`
	ModelLimits          []ModelLimit            `json:"model_limits"`
	LargeContextRules    []LargeContextRule      `json:"large_context_rules"`
	AdaptiveThinking     []AdaptiveThinkingRule  `json:"adaptive_thinking_rules"`
	ProviderReasoning    []ProviderReasoningRule `json:"provider_reasoning_rules"`
	ProviderHeaders      []ProviderHeaderRule    `json:"provider_header_rules"`
	ProviderFeatures     []ProviderFeatureRule   `json:"provider_feature_rules"`
	CompactionRules      []CompactionRule        `json:"compaction_rules"`
	ModelCoverage        []ModelCoverage         `json:"model_coverage"`
	ActorRuntime         []string                `json:"actor_runtime_markers"`
	ActorCoverage        []ActorCoverage         `json:"actor_runtime_coverage"`
	PromptFingerprints   []PromptFingerprint     `json:"prompt_fingerprints"`
	PromptTagCounts      []PromptTagCount        `json:"prompt_tag_counts"`
}

type routeScopeRule struct {
	Prefix string
	Scope  string
}

type RouteCoverage struct {
	Name  string `json:"name"`
	Scope string `json:"scope"`
}

type RouteMethods struct {
	Name    string   `json:"name"`
	Methods []string `json:"methods"`
	Scope   string   `json:"scope"`
}

type ThreadDeltaCoverage struct {
	Name string `json:"name"`
	Area string `json:"area"`
}

type ThreadReaderCoverage struct {
	Name string `json:"name"`
	Area string `json:"area"`
}

type ToolCancelCoverage struct {
	Name string `json:"name"`
	Area string `json:"area"`
}

type ToolRunCoverage struct {
	Name string `json:"name"`
	Area string `json:"area"`
}

type ToolCatalogCoverage struct {
	Name string `json:"name"`
	Area string `json:"area"`
}

type StreamJSONCoverage struct {
	Name string `json:"name"`
	Area string `json:"area"`
}

type ModeSettingCoverage struct {
	Name string `json:"name"`
	Area string `json:"area"`
}

type ProviderCoverage struct {
	Name string `json:"name"`
	Area string `json:"area"`
}

type AgentModeProfile struct {
	Name            string   `json:"name"`
	PrimaryModel    string   `json:"primary_model,omitempty"`
	ReasoningEffort string   `json:"reasoning_effort,omitempty"`
	ReasoningLevels []string `json:"reasoning_levels,omitempty"`
	IncludeTools    string   `json:"include_tools,omitempty"`
	DeferredTools   bool     `json:"deferred_tools,omitempty"`
	Visible         bool     `json:"visible"`
	VisibleInV2     bool     `json:"visible_in_v2"`
	ServerOnly      bool     `json:"server_only"`
}

type AgentModeRoute struct {
	Name                    string `json:"name"`
	Provider                string `json:"provider,omitempty"`
	Model                   string `json:"model,omitempty"`
	PrimaryModel            string `json:"primary_model,omitempty"`
	ReasoningEffort         string `json:"reasoning_effort,omitempty"`
	ContextWindow           int    `json:"context_window,omitempty"`
	MaxOutputTokens         int    `json:"max_output_tokens,omitempty"`
	EffectiveContextWindow  int    `json:"effective_context_window,omitempty"`
	EffectiveMaxInputTokens int    `json:"effective_max_input_tokens,omitempty"`
	LargeContextAlias       string `json:"large_context_alias,omitempty"`
}

type AgentModeCoverage struct {
	Name  string `json:"name"`
	Scope string `json:"scope"`
}

type SettingDefault struct {
	Name  string `json:"name"`
	Value string `json:"value"`
	Scope string `json:"scope"`
}

type SettingCoverage struct {
	Name  string `json:"name"`
	Scope string `json:"scope"`
}

type ModelCoverage struct {
	Name     string `json:"name"`
	Provider string `json:"provider"`
	Family   string `json:"family"`
}

type ModelLimit struct {
	Enum            string `json:"enum"`
	Name            string `json:"name"`
	Provider        string `json:"provider"`
	DisplayName     string `json:"display_name"`
	ContextWindow   int    `json:"context_window"`
	MaxOutputTokens int    `json:"max_output_tokens"`
}

type modelLimitExpectation struct {
	Enum            string
	Provider        string
	DisplayName     string
	ContextWindow   int
	MaxOutputTokens int
}

type LargeContextRule struct {
	PrimaryModel               string `json:"primary_model,omitempty"`
	Alias                      string `json:"alias,omitempty"`
	ContextWindow              int    `json:"context_window,omitempty"`
	MaxOutputTokens            int    `json:"max_output_tokens,omitempty"`
	MaxInputTokens             int    `json:"max_input_tokens,omitempty"`
	RequiresEnableLargeContext bool   `json:"requires_enable_large_context"`
}

type AdaptiveThinkingRule struct {
	ModelEnums       []string `json:"model_enums,omitempty"`
	Models           []string `json:"models,omitempty"`
	EffortLevels     []string `json:"effort_levels,omitempty"`
	DefaultEffort    string   `json:"default_effort,omitempty"`
	ThinkingType     string   `json:"thinking_type,omitempty"`
	Display          string   `json:"display,omitempty"`
	UsesOutputConfig bool     `json:"uses_output_config"`
}

type ProviderReasoningRule struct {
	Provider           string   `json:"provider"`
	Sources            []string `json:"sources,omitempty"`
	Setting            string   `json:"setting,omitempty"`
	DefaultEffort      string   `json:"default_effort,omitempty"`
	SpecialModelEnum   string   `json:"special_model_enum,omitempty"`
	SpecialModel       string   `json:"special_model,omitempty"`
	SpecialModelEffort string   `json:"special_model_effort,omitempty"`
}

type ProviderHeaderRule struct {
	Provider                    string `json:"provider"`
	FeatureHeader               string `json:"feature_header,omitempty"`
	Feature                     string `json:"feature,omitempty"`
	ThreadIDHeader              string `json:"thread_id_header,omitempty"`
	ThreadIDSource              string `json:"thread_id_source,omitempty"`
	MessageIDHeader             string `json:"message_id_header,omitempty"`
	MessageIDSource             string `json:"message_id_source,omitempty"`
	BetaHeader                  string `json:"beta_header,omitempty"`
	InterleavedBeta             string `json:"interleaved_beta,omitempty"`
	ThinkingEnabledSetting      string `json:"thinking_enabled_setting,omitempty"`
	InterleavedThinkingSetting  string `json:"interleaved_thinking_setting,omitempty"`
	SkipsAdaptiveThinkingModels bool   `json:"skips_adaptive_thinking_models"`
	OverrideProviderHeader      string `json:"override_provider_header,omitempty"`
	OverrideProviderSetting     string `json:"override_provider_setting,omitempty"`
	FastModeBeta                string `json:"fast_mode_beta,omitempty"`
	FastModeSetting             string `json:"fast_mode_setting,omitempty"`
	FastModeValue               string `json:"fast_mode_value,omitempty"`
	FastModeOverrideProvider    string `json:"fast_mode_override_provider,omitempty"`
}

type ProviderFeatureRule struct {
	Feature  string `json:"feature"`
	Header   string `json:"header,omitempty"`
	Provider string `json:"provider,omitempty"`
	Callsite string `json:"callsite,omitempty"`
	Tool     string `json:"tool,omitempty"`
	Default  bool   `json:"default"`
}

type CompactionRule struct {
	Name                     string   `json:"name"`
	Provider                 string   `json:"provider,omitempty"`
	Trigger                  string   `json:"trigger,omitempty"`
	Timing                   string   `json:"timing,omitempty"`
	DefaultThresholdTokens   int      `json:"default_threshold_tokens,omitempty"`
	UsageFields              []string `json:"usage_fields,omitempty"`
	SummaryPrompt            string   `json:"summary_prompt,omitempty"`
	HistoryReplacementRole   string   `json:"history_replacement_role,omitempty"`
	TrailingAssistantToolUse string   `json:"trailing_assistant_tool_use,omitempty"`
	HelperHeader             string   `json:"helper_header,omitempty"`
	HelperHeaderValue        string   `json:"helper_header_value,omitempty"`
}

type ActorCoverage struct {
	Name string `json:"name"`
	Area string `json:"area"`
}

type PromptFingerprint struct {
	SHA256 string   `json:"sha256"`
	Length int      `json:"length"`
	Kind   string   `json:"kind,omitempty"`
	Tags   []string `json:"tags"`
}

type PromptTagCount struct {
	Kind  string `json:"kind,omitempty"`
	Tag   string `json:"tag"`
	Count int    `json:"count"`
}

type categoryDiff struct {
	Name    string
	Added   []string
	Removed []string
}

type promptDiff struct {
	Added   []PromptFingerprint
	Removed []PromptFingerprint
}

type promptExcerpt struct {
	Fingerprint PromptFingerprint
	Excerpt     string
}

type auditDiff struct {
	Categories []categoryDiff
	Prompts    promptDiff
}

type lifecycleCheck struct {
	Area     string
	Scope    string
	Trigger  string
	Commands []string
	Files    []string
}

func main() {
	os.Exit(runAudit(os.Args[1:], os.Stdout, os.Stderr))
}

func runAudit(args []string, stdout, stderr io.Writer) int {
	previousStdout := auditStdout
	auditStdout = stdout
	defer func() {
		auditStdout = previousStdout
	}()

	flags := flag.NewFlagSet("amp_binary_audit", flag.ContinueOnError)
	flags.SetOutput(stderr)
	binaryPath := flags.String("binary", defaultAmpBinaryPath, "path to the Amp binary to inspect")
	baselinePath := flags.String("baseline", defaultBaselinePath, "path to the parity baseline JSON")
	writeBaseline := flags.Bool("write-baseline", false, "write the current snapshot to -baseline")
	allowUnknownBaseline := flags.Bool("allow-unknown-baseline", false, "allow -write-baseline even when coverage ownership is unknown")
	acceptBaselineDiff := flags.Bool("accept-baseline-diff", false, "allow -write-baseline when the current Amp binary differs from the existing baseline after review")
	strict := flags.Bool("strict", false, "exit non-zero when signal differences or unclassified ownership are found")
	jsonOut := flags.Bool("json", false, "print the current snapshot as JSON")
	checklist := flags.Bool("checklist", false, "print the lifecycle parity checklist for the current binary signals")
	promptDiffExcerpts := flags.Bool("prompt-diff-excerpts", false, "print bounded excerpts for changed prompt/source fingerprints in the current binary")
	if err := flags.Parse(args); err != nil {
		return 2
	}

	snapshot, err := BuildSnapshot(*binaryPath)
	if err != nil {
		fmt.Fprintf(stderr, "amp binary audit failed: %v\n", err)
		return 1
	}

	if *jsonOut {
		if err := writeJSON(stdout, snapshot); err != nil {
			fmt.Fprintf(stderr, "write JSON: %v\n", err)
			return 1
		}
		return 0
	}

	if *writeBaseline {
		var baselineDiff auditDiff
		var problems []string
		if baseline, err := readSnapshotFile(*baselinePath); err == nil {
			baselineDiff = diffSnapshots(baseline, snapshot)
			if baseline.Schema < snapshotSchema {
				if !*acceptBaselineDiff {
					fmt.Fprintln(stderr, "refusing to write Amp binary parity baseline:")
					fmt.Fprintf(stderr, "  - existing baseline uses a stale schema: schema %d, current schema %d\n", baseline.Schema, snapshotSchema)
					fmt.Fprintln(stderr, "rerun with -accept-baseline-diff only after confirming the current binary behavior is intentionally covered")
					return 2
				}
				fmt.Fprintf(stderr, "existing baseline uses a stale schema; rewriting after explicit -accept-baseline-diff: schema %d, current schema %d\n", baseline.Schema, snapshotSchema)
			} else {
				existingProblems := prefixedSnapshotAuditProblems("existing baseline", baseline)
				if len(existingProblems) > 0 && (!*acceptBaselineDiff || !acceptableExistingBaselineAuditProblems(existingProblems)) {
					problems = append(problems, existingProblems...)
				} else if len(existingProblems) > 0 {
					fmt.Fprintf(stderr, "existing baseline has stale audit values; rewriting after explicit -accept-baseline-diff: %s\n", strings.Join(existingProblems, "; "))
				}
			}
		} else if errors.Is(err, errUnsupportedSnapshotSchema) {
			if !*acceptBaselineDiff {
				fmt.Fprintln(stderr, "refusing to write Amp binary parity baseline:")
				fmt.Fprintf(stderr, "  - existing baseline uses a stale schema: %v\n", err)
				fmt.Fprintln(stderr, "rerun with -accept-baseline-diff only after confirming the current binary behavior is intentionally covered")
				return 2
			}
			fmt.Fprintf(stderr, "existing baseline uses a stale schema; rewriting after explicit -accept-baseline-diff: %v\n", err)
		} else if !errors.Is(err, os.ErrNotExist) {
			fmt.Fprintf(stderr, "read existing baseline before writing: %v\n", err)
			return 1
		}
		problems = append(problems, baselineWriteProblems(snapshot, baselineDiff, *allowUnknownBaseline, *acceptBaselineDiff)...)
		if len(problems) > 0 {
			fmt.Fprintln(stderr, "refusing to write Amp binary parity baseline:")
			for _, problem := range problems {
				fmt.Fprintf(stderr, "  - %s\n", problem)
			}
			fmt.Fprintln(stderr, "run the audit/checklist first, classify new coverage, and use explicit accept flags only after intentionally covering the new binary behavior")
			return 2
		}
		if err := writeSnapshotFile(*baselinePath, snapshot); err != nil {
			fmt.Fprintf(stderr, "write baseline: %v\n", err)
			return 1
		}
		fmt.Fprintf(stdout, "wrote Amp binary parity baseline: %s\n", *baselinePath)
		printSnapshotSummary(snapshot)
		if *checklist {
			printLifecycleChecklist(lifecycleChecklist(snapshot, auditDiff{}, true))
		}
		return 0
	}

	baseline, err := readSnapshotFile(*baselinePath)
	if err != nil {
		fmt.Fprintf(stderr, "read baseline: %v\n", err)
		return 1
	}
	diff := diffSnapshots(baseline, snapshot)
	printReport(baseline, snapshot, diff)
	if *promptDiffExcerpts {
		catalog, err := BuildPromptCatalog(*binaryPath)
		if err != nil {
			fmt.Fprintf(stderr, "build prompt catalog: %v\n", err)
			return 1
		}
		printPromptDiffExcerpts(diff.Prompts, catalog)
	}
	if *checklist {
		printLifecycleChecklist(lifecycleChecklist(snapshot, diff, true))
	}
	if *strict && strictAuditFailedWithBaseline(baseline, snapshot, diff) {
		return 2
	}
	return 0
}

func BuildSnapshot(path string) (Snapshot, error) {
	if strings.TrimSpace(path) == "" {
		return Snapshot{}, errors.New("binary path is empty")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return Snapshot{}, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return Snapshot{}, err
	}
	strs := extractASCIIStrings(raw, 4)
	hash := sha256.Sum256(raw)
	signals := classifyStrings(strs)
	return Snapshot{
		Schema: snapshotSchema,
		Source: SourceInfo{
			Path:           path,
			SHA256:         hex.EncodeToString(hash[:]),
			SizeBytes:      info.Size(),
			ModTime:        info.ModTime().UTC().Format(time.RFC3339),
			Versions:       firstN(sortedSet(matchesFromStrings(strs, versionPattern)), 8),
			BuildStamps:    firstN(sortedSet(matchesFromStrings(strs, timestampPattern)), 12),
			StringsScanned: len(strs),
		},
		Signals:   signals,
		Generated: time.Now().UTC().Format(time.RFC3339),
	}, nil
}

func auditPrintln(args ...any) {
	fmt.Fprintln(auditStdout, args...)
}

func auditPrintf(format string, args ...any) {
	fmt.Fprintf(auditStdout, format, args...)
}

func BuildPromptCatalog(path string) (map[string]promptExcerpt, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("binary path is empty")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	catalog := map[string]promptExcerpt{}
	for _, s := range extractASCIIStrings(raw, 4) {
		for _, candidate := range promptLikeSegments(s) {
			fp, ok := promptFingerprint(candidate)
			if !ok {
				continue
			}
			if _, exists := catalog[fp.SHA256]; exists {
				continue
			}
			catalog[fp.SHA256] = promptExcerpt{
				Fingerprint: fp,
				Excerpt:     promptExcerptText(candidate, 600),
			}
		}
	}
	return catalog, nil
}

func extractASCIIStrings(raw []byte, minLen int) []string {
	var out []string
	start := -1
	flush := func(end int) {
		if start >= 0 && end-start >= minLen {
			out = append(out, string(raw[start:end]))
		}
		start = -1
	}
	for i, b := range raw {
		if b >= 32 && b <= 126 {
			if start < 0 {
				start = i
			}
			continue
		}
		flush(i)
	}
	flush(len(raw))
	return out
}

func classifyStrings(strs []string) Signals {
	routes := map[string]struct{}{}
	routeMethods := map[string]map[string]struct{}{}
	events := map[string]struct{}{}
	threadReaders := map[string]struct{}{}
	cancelReasons := map[string]struct{}{}
	toolRunStatuses := map[string]struct{}{}
	toolCatalog := map[string]struct{}{}
	streamMarkers := map[string]struct{}{}
	modeSettings := map[string]struct{}{}
	providerMarkers := map[string]struct{}{}
	agentProfiles := map[string]AgentModeProfile{}
	settings := map[string]struct{}{}
	settingDefaults := map[string]SettingDefault{}
	models := map[string]struct{}{}
	modelLimits := map[string]ModelLimit{}
	largeContextRule := LargeContextRule{}
	adaptiveThinkingRule := AdaptiveThinkingRule{}
	providerReasoningRules := map[string]ProviderReasoningRule{}
	providerHeaderRule := ProviderHeaderRule{}
	providerFeatureRules := map[string]ProviderFeatureRule{}
	compactionRules := map[string]CompactionRule{}
	actors := map[string]struct{}{}
	prompts := map[string]PromptFingerprint{}

	knownDelta := make(map[string]struct{}, len(knownDeltaNames))
	for _, name := range knownDeltaNames {
		knownDelta[name] = struct{}{}
	}
	knownEvents := make(map[string]struct{}, len(knownThreadDeltaEvents))
	for _, event := range knownThreadDeltaEvents {
		knownEvents[event] = struct{}{}
	}
	knownProtocolEvents := make(map[string]struct{}, len(knownThreadProtocolEvents))
	for _, event := range knownThreadProtocolEvents {
		knownProtocolEvents[event] = struct{}{}
	}

	for _, s := range strs {
		for _, loc := range routePattern.FindAllStringIndex(s, -1) {
			raw := s[loc[0]:loc[1]]
			if route := normalizeRoute(raw); route != "" {
				routes[route] = struct{}{}
				addRouteMethods(routeMethods, route, routeMethodsNear(s, loc[0], loc[1]))
			}
		}
		for _, event := range eventPattern.FindAllString(s, -1) {
			if plausibleThreadDeltaEvent(event, knownEvents) {
				events[event] = struct{}{}
			}
		}
		for _, match := range threadProtocolLiteralPattern.FindAllStringSubmatch(s, -1) {
			if len(match) < 2 {
				continue
			}
			event := match[1]
			if plausibleThreadProtocolEvent(event, knownProtocolEvents) {
				events[event] = struct{}{}
			}
		}
		for _, event := range knownThreadProtocolEvents {
			if containsThreadProtocolEventMarker(s, event) {
				events[event] = struct{}{}
			}
		}
		for _, reason := range cancelPattern.FindAllString(s, -1) {
			if plausibleToolCancelReason(reason) {
				cancelReasons[reason] = struct{}{}
			}
		}
		for _, status := range knownToolRunStatuses {
			if containsToolRunStatusMarker(s, status) {
				toolRunStatuses[status] = struct{}{}
			}
		}
		for marker := range threadReaderMarkers {
			if containsThreadReaderMarker(s, marker) {
				threadReaders[marker] = struct{}{}
			}
		}
		for marker := range toolCatalogMarkers {
			if containsToolCatalogMarker(s, marker) {
				toolCatalog[marker] = struct{}{}
			}
		}
		for marker := range streamJSONMarkers {
			if strings.Contains(s, marker) {
				streamMarkers[marker] = struct{}{}
			}
		}
		for marker := range modeSettingMarkers {
			if strings.Contains(s, marker) {
				modeSettings[marker] = struct{}{}
			}
		}
		lower := strings.ToLower(s)
		for marker := range providerProtocolMarkers {
			if strings.Contains(lower, marker) {
				providerMarkers[marker] = struct{}{}
			}
		}
		for _, profile := range extractAgentModeProfiles(s) {
			agentProfiles[profile.Name] = profile
		}
		for _, match := range settingKeyPattern.FindAllStringSubmatch(s, -1) {
			if len(match) < 2 {
				continue
			}
			if setting := normalizeSettingKey(match[1]); setting != "" {
				settings[setting] = struct{}{}
			}
		}
		for _, match := range settingDefaultPattern.FindAllStringSubmatch(s, -1) {
			if len(match) < 3 {
				continue
			}
			setting := normalizeSettingKey(match[1])
			value := normalizeSettingDefaultValue(match[2])
			if setting == "" || value == "" {
				continue
			}
			key := setting + "\x00" + value
			settingDefaults[key] = SettingDefault{
				Name:  setting,
				Value: value,
				Scope: settingScope(setting),
			}
		}
		for _, token := range splitSignalTokens(s) {
			if _, ok := knownDelta[token]; ok {
				events[token] = struct{}{}
			}
		}
		for _, model := range modelPattern.FindAllString(s, -1) {
			if plausibleModelName(model) {
				models[model] = struct{}{}
			}
		}
		for _, limit := range extractModelLimits(s) {
			modelLimits[limit.Name] = limit
		}
		mergeLargeContextRule(&largeContextRule, s)
		mergeAdaptiveThinkingRule(&adaptiveThinkingRule, s)
		mergeProviderReasoningRules(providerReasoningRules, s)
		mergeProviderHeaderRule(&providerHeaderRule, s)
		mergeProviderFeatureRules(providerFeatureRules, s)
		mergeCompactionRules(compactionRules, s)
		for _, marker := range actorMarkers {
			if strings.Contains(lower, strings.ToLower(marker)) {
				actors[marker] = struct{}{}
			}
		}
		for _, candidate := range promptLikeSegments(s) {
			if fp, ok := promptFingerprint(candidate); ok {
				prompts[fp.SHA256] = fp
			}
		}
	}
	if _, ok := providerMarkers["amp.read-thread"]; ok {
		addProviderFeatureRule(providerFeatureRules, "amp.read-thread", "google", "thread-reader", "read_thread", false)
	}

	agentModeProfileList := sortedAgentModeProfiles(agentProfiles)
	modelLimitList := sortedModelLimits(modelLimits)
	largeContextRules := sortedLargeContextRules(largeContextRule, modelLimitList)
	adaptiveThinkingRules := sortedAdaptiveThinkingRules(adaptiveThinkingRule, modelLimitList)
	providerReasoningRuleList := sortedProviderReasoningRules(providerReasoningRules, modelLimitList)
	providerHeaderRules := sortedProviderHeaderRules(providerHeaderRule)
	providerFeatureRuleList := sortedProviderFeatureRules(providerFeatureRules)
	compactionRuleList := sortedCompactionRules(compactionRules)

	return Signals{
		Routes:               sortedKeys(routes),
		RouteMethods:         sortedRouteMethods(routeMethods),
		RouteCoverage:        classifyRouteCoverage(sortedKeys(routes)),
		ThreadDeltaEvents:    sortedKeys(events),
		ThreadDeltaCoverage:  classifyThreadDeltaCoverage(sortedKeys(events)),
		ThreadReaderMarkers:  sortedKeys(threadReaders),
		ThreadReaderCoverage: classifyThreadReaderCoverage(sortedKeys(threadReaders)),
		ToolCancelReasons:    sortedKeys(cancelReasons),
		ToolCancelCoverage:   classifyToolCancelCoverage(sortedKeys(cancelReasons)),
		ToolRunStatuses:      sortedKeys(toolRunStatuses),
		ToolRunCoverage:      classifyToolRunCoverage(sortedKeys(toolRunStatuses)),
		ToolCatalog:          sortedKeys(toolCatalog),
		ToolCatalogCoverage:  classifyToolCatalogCoverage(sortedKeys(toolCatalog)),
		StreamJSONMarkers:    sortedKeys(streamMarkers),
		StreamJSONCoverage:   classifyStreamJSONCoverage(sortedKeys(streamMarkers)),
		ModeSettingMarkers:   sortedKeys(modeSettings),
		ModeSettingCoverage:  classifyModeSettingCoverage(sortedKeys(modeSettings)),
		ProviderProtocol:     sortedKeys(providerMarkers),
		ProviderCoverage:     classifyProviderCoverage(sortedKeys(providerMarkers)),
		AgentModeProfiles:    agentModeProfileList,
		AgentModeRoutes:      deriveAgentModeRoutes(agentModeProfileList, modelLimitList, largeContextRules),
		AgentModeCoverage:    classifyAgentModeCoverage(sortedAgentModeProfileNames(agentProfiles)),
		Settings:             sortedKeys(settings),
		SettingDefaults:      sortedSettingDefaults(settingDefaults),
		SettingCoverage:      classifySettingCoverage(sortedKeys(settings)),
		Models:               sortedKeys(models),
		ModelLimits:          modelLimitList,
		LargeContextRules:    largeContextRules,
		AdaptiveThinking:     adaptiveThinkingRules,
		ProviderReasoning:    providerReasoningRuleList,
		ProviderHeaders:      providerHeaderRules,
		ProviderFeatures:     providerFeatureRuleList,
		CompactionRules:      compactionRuleList,
		ModelCoverage:        classifyModelCoverage(sortedKeys(models)),
		ActorRuntime:         sortedKeys(actors),
		ActorCoverage:        classifyActorCoverage(sortedKeys(actors)),
		PromptFingerprints:   sortedPromptFingerprints(prompts),
		PromptTagCounts:      promptTagCounts(prompts),
	}
}

func deriveAgentModeRoutes(profiles []AgentModeProfile, limits []ModelLimit, largeRules []LargeContextRule) []AgentModeRoute {
	limitsByEnum := make(map[string]ModelLimit, len(limits))
	for _, limit := range limits {
		if limit.Enum != "" {
			limitsByEnum[limit.Enum] = limit
		}
	}
	largeRulesByEnum := make(map[string]LargeContextRule, len(largeRules))
	for _, rule := range largeRules {
		if rule.PrimaryModel != "" {
			largeRulesByEnum[rule.PrimaryModel] = rule
		}
	}

	routes := make([]AgentModeRoute, 0, len(profiles))
	for _, profile := range profiles {
		if profile.Name == "" {
			continue
		}
		route := AgentModeRoute{
			Name:            profile.Name,
			PrimaryModel:    profile.PrimaryModel,
			ReasoningEffort: profile.ReasoningEffort,
		}
		if limit, ok := limitsByEnum[profile.PrimaryModel]; ok {
			route.Provider = limit.Provider
			route.Model = limit.Name
			route.ContextWindow = limit.ContextWindow
			route.MaxOutputTokens = limit.MaxOutputTokens
		}
		if strings.EqualFold(profile.Name, "large") {
			if rule, ok := largeRulesByEnum[profile.PrimaryModel]; ok && rule.ContextWindow > 0 {
				route.EffectiveContextWindow = rule.ContextWindow
				route.EffectiveMaxInputTokens = rule.MaxInputTokens
				route.LargeContextAlias = rule.Alias
			}
		}
		routes = append(routes, route)
	}
	sort.Slice(routes, func(i, j int) bool {
		return routes[i].Name < routes[j].Name
	})
	return routes
}

func mergeLargeContextRule(rule *LargeContextRule, s string) {
	if match := largeContextAliasPattern.FindStringSubmatch(s); len(match) == 4 {
		rule.Alias = strings.TrimSpace(match[1])
		rule.ContextWindow = parseModelLimitNumber(match[2])
		rule.MaxOutputTokens = parseModelLimitNumber(match[3])
		if rule.ContextWindow > 0 && rule.MaxOutputTokens > 0 {
			rule.MaxInputTokens = rule.ContextWindow - rule.MaxOutputTokens
		}
	}
	if rule.Alias == "" {
		if match := largeContextNamedAliasPattern.FindStringSubmatch(s); len(match) == 2 {
			rule.Alias = strings.TrimSpace(match[1])
		}
	}
	if match := largeContextModelPattern.FindStringSubmatch(s); len(match) == 2 {
		rule.PrimaryModel = strings.TrimSpace(match[1])
	}
	if strings.Contains(s, "enableLargeContext&&Sf(R)===ZB") {
		rule.RequiresEnableLargeContext = true
	}
	if match := largeContextReturnPattern.FindStringSubmatchIndex(s); len(match) == 6 {
		rule.RequiresEnableLargeContext = true
		modelVar := s[match[2]:match[3]]
		contextVar := s[match[4]:match[5]]
		enum := assignedModelEnumBefore(s, modelVar, match[0])
		if enum == "" {
			enum = assignedModelEnum(s, modelVar)
		}
		if enum != "" {
			rule.PrimaryModel = enum
		}
		contextWindow := assignedNumberBefore(s, contextVar, match[0])
		if contextWindow == 0 {
			contextWindow = assignedNumber(s, contextVar)
		}
		if contextWindow > 0 {
			rule.ContextWindow = contextWindow
		}
	}
	if rule.Alias == "claude-opus-4-6-1m" {
		rule.PrimaryModel = "CLAUDE_OPUS_4_6"
		if rule.ContextWindow == 0 {
			rule.ContextWindow = 1_000_000
		}
	}
}

func sortedLargeContextRules(rule LargeContextRule, limits []ModelLimit) []LargeContextRule {
	if rule.Alias == "" && rule.PrimaryModel == "" && !rule.RequiresEnableLargeContext {
		return nil
	}
	if rule.MaxOutputTokens == 0 && rule.PrimaryModel != "" {
		for _, limit := range limits {
			if limit.Enum == rule.PrimaryModel {
				rule.MaxOutputTokens = limit.MaxOutputTokens
				break
			}
		}
	}
	if rule.ContextWindow > 0 && rule.MaxOutputTokens > 0 {
		rule.MaxInputTokens = rule.ContextWindow - rule.MaxOutputTokens
	}
	return []LargeContextRule{rule}
}

func mergeAdaptiveThinkingRule(rule *AdaptiveThinkingRule, s string) {
	if match := adaptiveThinkingModelsPattern.FindStringSubmatch(s); len(match) == 4 {
		rule.ModelEnums = []string{
			strings.TrimSpace(match[1]),
			strings.TrimSpace(match[2]),
			strings.TrimSpace(match[3]),
		}
	}
	if match := adaptiveThinkingEffortPattern.FindStringSubmatch(s); len(match) == 3 {
		rule.EffortLevels = quotedValuesInOrder(match[1])
		rule.DefaultEffort = strings.TrimSpace(match[2])
	}
	if match := adaptiveThinkingCurrentEffortPattern.FindStringSubmatchIndex(s); len(match) == 8 {
		if len(rule.ModelEnums) == 0 {
			rule.ModelEnums = modelEnumsFromPredicateBefore(s, s[match[2]:match[3]], match[0])
		}
		rule.EffortLevels = quotedValuesInOrder(s[match[4]:match[5]])
		rule.DefaultEffort = strings.TrimSpace(s[match[6]:match[7]])
	}
	if match := adaptiveThinkingOutputPattern.FindStringSubmatch(s); len(match) == 3 {
		rule.ThinkingType = strings.TrimSpace(match[1])
		rule.Display = strings.TrimSpace(match[2])
		rule.UsesOutputConfig = true
	}
	if rule.ThinkingType == "adaptive" && len(rule.ModelEnums) == 0 {
		rule.ModelEnums = []string{"CLAUDE_OPUS_4_6", "CLAUDE_OPUS_4_7", "CLAUDE_OPUS_4_8"}
	}
}

func assignedModelEnum(s, varName string) string {
	varName = strings.TrimSpace(varName)
	if varName == "" {
		return ""
	}
	pattern := regexp.MustCompile(`\b` + regexp.QuoteMeta(varName) + `=A9\.([A-Z0-9_]+)\.name`)
	return firstSubmatch(pattern, s)
}

func assignedNumber(s, varName string) int {
	varName = strings.TrimSpace(varName)
	if varName == "" {
		return 0
	}
	pattern := regexp.MustCompile(`\b` + regexp.QuoteMeta(varName) + `=([0-9]+(?:e[0-9]+)?)`)
	return parseModelLimitNumber(firstSubmatch(pattern, s))
}

func assignedModelEnumBefore(s, varName string, before int) string {
	varName = strings.TrimSpace(varName)
	if varName == "" {
		return ""
	}
	pattern := regexp.MustCompile(`\b` + regexp.QuoteMeta(varName) + `=A9\.([A-Z0-9_]+)\.name`)
	matches := pattern.FindAllStringSubmatchIndex(s, -1)
	value := ""
	for _, match := range matches {
		if len(match) < 4 || match[0] >= before {
			continue
		}
		value = s[match[2]:match[3]]
	}
	return value
}

func assignedNumberBefore(s, varName string, before int) int {
	varName = strings.TrimSpace(varName)
	if varName == "" {
		return 0
	}
	pattern := regexp.MustCompile(`\b` + regexp.QuoteMeta(varName) + `=([0-9]+(?:e[0-9]+)?)`)
	matches := pattern.FindAllStringSubmatchIndex(s, -1)
	value := ""
	for _, match := range matches {
		if len(match) < 4 || match[0] >= before {
			continue
		}
		value = s[match[2]:match[3]]
	}
	return parseModelLimitNumber(value)
}

func modelEnumsFromPredicateBefore(s, predicateName string, before int) []string {
	predicateName = strings.TrimSpace(predicateName)
	if predicateName == "" {
		return nil
	}
	pattern := regexp.MustCompile(`function ` + regexp.QuoteMeta(predicateName) + `\([^)]*\)\{[^{}]*return([^{}]+?)\}`)
	matches := pattern.FindAllStringSubmatchIndex(s, -1)
	var match []int
	for _, candidate := range matches {
		if len(candidate) < 4 || candidate[0] >= before {
			continue
		}
		match = candidate
	}
	if len(match) != 4 {
		return nil
	}
	varPattern := regexp.MustCompile(`===([A-Za-z0-9_$]+)`)
	body := s[match[2]:match[3]]
	varMatches := varPattern.FindAllStringSubmatch(body, -1)
	values := make([]string, 0, len(varMatches))
	for _, varMatch := range varMatches {
		if len(varMatch) != 2 {
			continue
		}
		enum := assignedModelEnumBefore(s, varMatch[1], match[0])
		if enum == "" {
			enum = assignedModelEnum(s, varMatch[1])
		}
		if enum != "" {
			values = append(values, enum)
		}
	}
	return values
}

func sortedAdaptiveThinkingRules(rule AdaptiveThinkingRule, limits []ModelLimit) []AdaptiveThinkingRule {
	if len(rule.ModelEnums) == 0 && len(rule.EffortLevels) == 0 && rule.ThinkingType == "" && !rule.UsesOutputConfig {
		return nil
	}
	limitsByEnum := make(map[string]ModelLimit, len(limits))
	for _, limit := range limits {
		if limit.Enum != "" {
			limitsByEnum[limit.Enum] = limit
		}
	}
	rule.ModelEnums = sortedSet(rule.ModelEnums)
	rule.Models = make([]string, 0, len(rule.ModelEnums))
	for _, enum := range rule.ModelEnums {
		if limit, ok := limitsByEnum[enum]; ok && limit.Name != "" {
			rule.Models = append(rule.Models, limit.Name)
		}
	}
	sort.Strings(rule.Models)
	return []AdaptiveThinkingRule{rule}
}

func mergeProviderReasoningRules(rules map[string]ProviderReasoningRule, s string) {
	match := providerReasoningPattern.FindStringSubmatch(s)
	if len(match) != 7 {
		return
	}
	rules["anthropic"] = ProviderReasoningRule{
		Provider:           "anthropic",
		Sources:            []string{"setting:reasoning.effort", "mode:reasoningEffort", "model-default"},
		DefaultEffort:      strings.TrimSpace(match[3]),
		SpecialModelEnum:   strings.TrimSpace(match[1]),
		SpecialModelEffort: strings.TrimSpace(match[2]),
	}
	rules["openai"] = ProviderReasoningRule{
		Provider:      "openai",
		Sources:       []string{"setting:reasoning.effort", "mode:reasoningEffort", "provider-default"},
		DefaultEffort: strings.TrimSpace(match[4]),
	}
	rules["vertexai"] = ProviderReasoningRule{
		Provider:      "vertexai",
		Sources:       []string{"setting:gemini.thinkingLevel", "mode:reasoningEffort", "provider-default"},
		Setting:       strings.TrimSpace(match[5]),
		DefaultEffort: strings.TrimSpace(match[6]),
	}
}

func sortedProviderReasoningRules(rules map[string]ProviderReasoningRule, limits []ModelLimit) []ProviderReasoningRule {
	if len(rules) == 0 {
		return nil
	}
	limitsByEnum := make(map[string]ModelLimit, len(limits))
	for _, limit := range limits {
		if limit.Enum != "" {
			limitsByEnum[limit.Enum] = limit
		}
	}
	values := make([]ProviderReasoningRule, 0, len(rules))
	for _, rule := range rules {
		if rule.SpecialModelEnum != "" {
			if limit, ok := limitsByEnum[rule.SpecialModelEnum]; ok {
				rule.SpecialModel = limit.Name
			}
		}
		values = append(values, rule)
	}
	sort.Slice(values, func(i, j int) bool {
		return values[i].Provider < values[j].Provider
	})
	return values
}

func mergeProviderHeaderRule(rule *ProviderHeaderRule, s string) {
	if match := ampHeaderConstantsPattern.FindStringSubmatch(s); len(match) == 7 {
		rule.Provider = "anthropic"
		rule.FeatureHeader = strings.TrimSpace(match[1])
		rule.ThreadIDHeader = strings.TrimSpace(match[2])
		rule.MessageIDHeader = strings.TrimSpace(match[3])
	}
	if match := anthropicFastModeBetaPattern.FindStringSubmatch(s); len(match) == 2 {
		rule.Provider = "anthropic"
		rule.FastModeBeta = strings.TrimSpace(match[1])
	}
	if strings.Contains(s, `"anthropic.thinking.enabled"`) &&
		strings.Contains(s, `"anthropic.interleavedThinking.enabled"`) &&
		strings.Contains(s, `"anthropic.provider"`) &&
		strings.Contains(s, `"anthropic.speed"`) &&
		strings.Contains(s, `"interleaved-thinking-2025-05-14"`) &&
		strings.Contains(s, `:"amp.chat"`) {
		rule.Provider = "anthropic"
		rule.FeatureHeader = "x-amp-feature"
		rule.Feature = "amp.chat"
		rule.ThreadIDHeader = "x-amp-thread-id"
		rule.ThreadIDSource = "thread.id"
		rule.MessageIDHeader = "x-amp-message-id"
		rule.MessageIDSource = "message-id-argument"
		rule.BetaHeader = "anthropic-beta"
		rule.InterleavedBeta = "interleaved-thinking-2025-05-14"
		rule.ThinkingEnabledSetting = "anthropic.thinking.enabled"
		rule.InterleavedThinkingSetting = "anthropic.interleavedThinking.enabled"
		rule.SkipsAdaptiveThinkingModels = true
		rule.OverrideProviderHeader = "x-amp-override-provider"
		rule.OverrideProviderSetting = "anthropic.provider"
		rule.FastModeBeta = "fast-mode-2026-02-01"
		rule.FastModeSetting = "anthropic.speed"
		rule.FastModeValue = "fast"
		rule.FastModeOverrideProvider = "anthropic"
	}
	constructor, ok := anthropicHeaderConstructor(s)
	if !ok {
		return
	}
	if match := anthropicThinkingBetaPattern.FindStringSubmatch(constructor); len(match) == 4 {
		rule.Provider = "anthropic"
		rule.ThinkingEnabledSetting = strings.TrimSpace(match[1])
		rule.InterleavedThinkingSetting = strings.TrimSpace(match[2])
		rule.InterleavedBeta = strings.TrimSpace(match[3])
		rule.SkipsAdaptiveThinkingModels = strings.Contains(constructor, "&&!FOR(e)")
	}
	if match := anthropicProviderPattern.FindStringSubmatch(constructor); len(match) == 3 && match[1] == match[2] {
		rule.Provider = "anthropic"
		rule.OverrideProviderSetting = strings.TrimSpace(match[1])
	}
	if match := anthropicSpeedPattern.FindStringSubmatch(constructor); len(match) == 4 {
		rule.Provider = "anthropic"
		rule.FastModeSetting = strings.TrimSpace(match[1])
		rule.FastModeValue = strings.TrimSpace(match[2])
		rule.FastModeOverrideProvider = strings.TrimSpace(match[3])
	}
	if match := anthropicBetaHeaderPattern.FindStringSubmatch(constructor); len(match) == 2 {
		rule.Provider = "anthropic"
		rule.BetaHeader = strings.TrimSpace(match[1])
	}
	if match := anthropicOverridePattern.FindStringSubmatch(constructor); len(match) == 2 {
		rule.Provider = "anthropic"
		rule.OverrideProviderHeader = strings.TrimSpace(match[1])
	}
	if match := ampFeaturePattern.FindStringSubmatch(constructor); len(match) == 2 {
		rule.Provider = "anthropic"
		rule.Feature = strings.TrimSpace(match[1])
	}
	if strings.Contains(constructor, "i!=null?{[LU]:String(i)}:{}") {
		rule.Provider = "anthropic"
		rule.MessageIDSource = "message-id-argument"
	}
	if strings.Contains(constructor, "...pU(T)") {
		rule.Provider = "anthropic"
		rule.ThreadIDSource = "thread.id"
	}
}

func anthropicHeaderConstructor(s string) (string, bool) {
	start := strings.Index(s, "function ZpT(")
	if start < 0 {
		return "", false
	}
	body := s[start:]
	if end := strings.Index(body, "function XOR("); end > 0 {
		body = body[:end]
	}
	return body, true
}

func sortedProviderHeaderRules(rule ProviderHeaderRule) []ProviderHeaderRule {
	if rule.Feature == "" && rule.InterleavedBeta == "" && rule.FastModeBeta == "" && rule.OverrideProviderSetting == "" && rule.BetaHeader == "" {
		return nil
	}
	if rule.Provider == "" {
		rule.Provider = "anthropic"
	}
	return []ProviderHeaderRule{rule}
}

func addProviderFeatureRule(rules map[string]ProviderFeatureRule, feature, provider, callsite, tool string, def bool) {
	feature = strings.TrimSpace(feature)
	provider = strings.TrimSpace(provider)
	callsite = strings.TrimSpace(callsite)
	if feature == "" || callsite == "" {
		return
	}
	rule := ProviderFeatureRule{
		Feature:  feature,
		Header:   "x-amp-feature",
		Provider: provider,
		Callsite: callsite,
		Tool:     strings.TrimSpace(tool),
		Default:  def,
	}
	rules[provider+"\x00"+feature+"\x00"+callsite] = rule
}

func mergeProviderFeatureRules(rules map[string]ProviderFeatureRule, s string) {
	if strings.Contains(s, "function ZpT(") && strings.Contains(s, `[Vw]:"amp.chat"`) ||
		strings.Contains(s, `"anthropic.thinking.enabled"`) && strings.Contains(s, `:"amp.chat"`) {
		addProviderFeatureRule(rules, "amp.chat", "anthropic", "anthropic-chat", "", true)
	}
	if strings.Contains(s, "function jE(") && strings.Contains(s, `T?.featureHeader??"amp.chat"`) ||
		strings.Contains(s, `featureHeader??"amp.chat"`) {
		addProviderFeatureRule(rules, "amp.chat", "google", "google-client-default", "", true)
	}
	if strings.Contains(s, "function VhT(") && strings.Contains(s, `[Vw]:"amp.chat"`) ||
		strings.Contains(s, "function VhT(") && strings.Contains(s, `:"amp.chat"`) ||
		strings.Contains(s, "function I_T(") && strings.Contains(s, `[Vw]:"amp.chat"`) {
		addProviderFeatureRule(rules, "amp.chat", "openai", "openai-compatible-client-default", "", true)
	}
	if strings.Contains(s, `"amp.review"`) && strings.Contains(s, "expert software engineer reviewing code changes") {
		addProviderFeatureRule(rules, "amp.review", "google", "code-review", "code_review", false)
	}
	if strings.Contains(s, `w7T="amp.read-thread"`) && strings.Contains(s, "xM(ojR") {
		addProviderFeatureRule(rules, "amp.read-thread", "google", "thread-reader", "read_thread", false)
	}
	if strings.Contains(s, "function P_T(") && strings.Contains(s, `c??"amp.image-generation"`) ||
		strings.Contains(s, "L_T(A9.GEMINI_3_PRO_IMAGE.name") && strings.Contains(s, `c??"amp.image-generation"`) {
		addProviderFeatureRule(rules, "amp.image-generation", "google", "google-image-generation-default", "", true)
	}
	if strings.Contains(s, "async function L_T(") && strings.Contains(s, `featureHeader:c??"amp.image-generation"`) {
		addProviderFeatureRule(rules, "amp.image-generation", "google", "google-image-generation-default", "", true)
	}
	if strings.Contains(s, "L_T(A9.GEMINI_3_PRO_IMAGE.name") && strings.Contains(s, `"amp.image-generation"`) {
		addProviderFeatureRule(rules, "amp.image-generation", "google", "google-image-generation-default", "", true)
	}
	if strings.Contains(s, `featureHeader:c??"amp.image-generation"`) && strings.Contains(s, ".models.generateContent(") {
		addProviderFeatureRule(rules, "amp.image-generation", "google", "google-image-generation-default", "", true)
	}
	if strings.Contains(s, "function DhT(") && strings.Contains(s, `c??"amp.image-generation"`) {
		addProviderFeatureRule(rules, "amp.image-generation", "openai", "openai-image-generation-default", "", true)
	}
	if strings.Contains(s, "function J_T(") && strings.Contains(s, `[Vw]:c??"amp.image-generation"`) && strings.Contains(s, ".images.") {
		addProviderFeatureRule(rules, "amp.image-generation", "openai", "openai-image-generation-default", "", true)
	}
	if strings.Contains(s, `"amp.painter"`) && strings.Contains(s, "P_T(A9.GEMINI_3_PRO_IMAGE.name") ||
		strings.Contains(s, `"amp.painter"`) && strings.Contains(s, "L_T(A9.GEMINI_3_PRO_IMAGE.name") ||
		strings.Contains(s, `"amp.painter"`) && strings.Contains(s, "xhT(A9.GEMINI_3_PRO_IMAGE.name") {
		addProviderFeatureRule(rules, "amp.painter", "google", "painter-gemini-image", "painter", false)
	}
	if strings.Contains(s, `"amp.painter"`) && strings.Contains(s, "DhT(l,R.prompt") ||
		strings.Contains(s, `"amp.painter"`) && strings.Contains(s, "J_T(l,R.prompt") {
		addProviderFeatureRule(rules, "amp.painter", "openai", "painter-openai-image", "painter", false)
	}
}

func sortedProviderFeatureRules(rules map[string]ProviderFeatureRule) []ProviderFeatureRule {
	values := make([]ProviderFeatureRule, 0, len(rules))
	for _, rule := range rules {
		values = append(values, rule)
	}
	sort.Slice(values, func(i, j int) bool {
		if values[i].Feature != values[j].Feature {
			return values[i].Feature < values[j].Feature
		}
		if values[i].Provider != values[j].Provider {
			return values[i].Provider < values[j].Provider
		}
		return values[i].Callsite < values[j].Callsite
	})
	return values
}

func mergeCompactionRules(rules map[string]CompactionRule, s string) {
	if !strings.Contains(s, "compactionControl") || !strings.Contains(s, "contextTokenThreshold??") {
		return
	}
	required := []string{
		"usage.input_tokens",
		"usage.cache_creation_input_tokens",
		"usage.cache_read_input_tokens",
		"usage.output_tokens",
		`"x-stainless-helper":"compaction"`,
	}
	for _, marker := range required {
		if !strings.Contains(s, marker) {
			return
		}
	}
	if !strings.Contains(s, `.content.filter((`) || !strings.Contains(s, `type!=="tool_use"`) {
		return
	}
	if !strings.Contains(s, `params.messages=[{role:"user",content:`) {
		return
	}
	threshold := 0
	if match := regexp.MustCompile(`jpT=([0-9]+(?:e[0-9]+)?)`).FindStringSubmatch(s); len(match) > 1 {
		threshold = parseModelLimitNumber(match[1])
	}
	if match := regexp.MustCompile(`contextTokenThreshold\?\?([A-Za-z0-9_$]+)`).FindStringSubmatch(s); len(match) > 1 {
		if parsed := assignedNumber(s, match[1]); parsed > 0 {
			threshold = parsed
		}
	}
	if threshold <= 0 {
		threshold = 100000
	}
	rule := CompactionRule{
		Name:                   "anthropic-tool-runner",
		Provider:               "anthropic",
		Trigger:                "observed-usage",
		Timing:                 "post-response",
		DefaultThresholdTokens: threshold,
		UsageFields: []string{
			"input_tokens",
			"cache_creation_input_tokens",
			"cache_read_input_tokens",
			"output_tokens",
		},
		SummaryPrompt:            "continuation-summary",
		HistoryReplacementRole:   "user",
		TrailingAssistantToolUse: "strip-tool-use-blocks",
		HelperHeader:             "x-stainless-helper",
		HelperHeaderValue:        "compaction",
	}
	rules[rule.Name] = rule
}

func sortedCompactionRules(rules map[string]CompactionRule) []CompactionRule {
	values := make([]CompactionRule, 0, len(rules))
	for _, rule := range rules {
		values = append(values, rule)
	}
	sort.Slice(values, func(i, j int) bool {
		return values[i].Name < values[j].Name
	})
	return values
}

func normalizeSettingKey(key string) string {
	key = strings.TrimSpace(strings.Trim(key, `"'`))
	if key == "" || key == "null" {
		return ""
	}
	if strings.Contains(key, ".") {
		return key
	}
	if _, ok := standaloneSettingNames[key]; ok {
		return key
	}
	return ""
}

func normalizeSettingDefaultValue(value string) string {
	value = strings.TrimSpace(value)
	switch value {
	case "void 0":
		return "undefined"
	case "!0", "true":
		return "true"
	case "!1", "false":
		return "false"
	}
	if strings.HasPrefix(value, `"`) && strings.HasSuffix(value, `"`) {
		var decoded string
		if err := json.Unmarshal([]byte(value), &decoded); err == nil {
			return decoded
		}
		return strings.Trim(value, `"`)
	}
	if strings.HasPrefix(value, "[") && strings.HasSuffix(value, "]") {
		return strings.Join(strings.Fields(value), "")
	}
	return value
}

func classifyRouteCoverage(routes []string) []RouteCoverage {
	coverage := make([]RouteCoverage, 0, len(routes))
	for _, route := range routes {
		scope := routeScope(route)
		if scope == "" {
			scope = "unknown"
		}
		coverage = append(coverage, RouteCoverage{Name: route, Scope: scope})
	}
	sort.Slice(coverage, func(i, j int) bool {
		return coverage[i].Name < coverage[j].Name
	})
	return coverage
}

func routeScope(route string) string {
	path := route
	if queryIndex := strings.Index(path, "?"); queryIndex >= 0 {
		path = path[:queryIndex]
	}
	path = strings.TrimRight(path, "/")
	if path == "" {
		path = "/"
	}
	for _, rule := range routeScopes {
		prefix := strings.TrimRight(rule.Prefix, "/")
		if path == prefix || strings.HasPrefix(path, prefix+"/") {
			return rule.Scope
		}
	}
	return ""
}

func classifyThreadDeltaCoverage(events []string) []ThreadDeltaCoverage {
	coverage := make([]ThreadDeltaCoverage, 0, len(events))
	for _, event := range events {
		area := threadDeltaArea(event)
		if area == "" {
			area = "unknown"
		}
		coverage = append(coverage, ThreadDeltaCoverage{Name: event, Area: area})
	}
	sort.Slice(coverage, func(i, j int) bool {
		return coverage[i].Name < coverage[j].Name
	})
	return coverage
}

func classifyThreadReaderCoverage(markers []string) []ThreadReaderCoverage {
	coverage := make([]ThreadReaderCoverage, 0, len(markers))
	for _, marker := range markers {
		area := threadReaderMarkers[marker]
		if area == "" {
			area = "unknown"
		}
		coverage = append(coverage, ThreadReaderCoverage{Name: marker, Area: area})
	}
	sort.Slice(coverage, func(i, j int) bool {
		return coverage[i].Name < coverage[j].Name
	})
	return coverage
}

func classifyToolCancelCoverage(reasons []string) []ToolCancelCoverage {
	coverage := make([]ToolCancelCoverage, 0, len(reasons))
	for _, reason := range reasons {
		area := toolCancelReasonArea(reason)
		if area == "" {
			area = "unknown"
		}
		coverage = append(coverage, ToolCancelCoverage{Name: reason, Area: area})
	}
	sort.Slice(coverage, func(i, j int) bool {
		return coverage[i].Name < coverage[j].Name
	})
	return coverage
}

func classifyToolRunCoverage(statuses []string) []ToolRunCoverage {
	coverage := make([]ToolRunCoverage, 0, len(statuses))
	for _, status := range statuses {
		area := toolRunStatusAreas[status]
		if area == "" {
			area = "unknown"
		}
		coverage = append(coverage, ToolRunCoverage{Name: status, Area: area})
	}
	sort.Slice(coverage, func(i, j int) bool {
		return coverage[i].Name < coverage[j].Name
	})
	return coverage
}

func classifyToolCatalogCoverage(markers []string) []ToolCatalogCoverage {
	coverage := make([]ToolCatalogCoverage, 0, len(markers))
	for _, marker := range markers {
		area := toolCatalogMarkers[marker]
		if area == "" {
			area = "unknown"
		}
		coverage = append(coverage, ToolCatalogCoverage{Name: marker, Area: area})
	}
	sort.Slice(coverage, func(i, j int) bool {
		return coverage[i].Name < coverage[j].Name
	})
	return coverage
}

func classifyStreamJSONCoverage(markers []string) []StreamJSONCoverage {
	coverage := make([]StreamJSONCoverage, 0, len(markers))
	for _, marker := range markers {
		area := streamJSONMarkers[marker]
		if area == "" {
			area = "unknown"
		}
		coverage = append(coverage, StreamJSONCoverage{Name: marker, Area: area})
	}
	sort.Slice(coverage, func(i, j int) bool {
		return coverage[i].Name < coverage[j].Name
	})
	return coverage
}

func classifyModeSettingCoverage(markers []string) []ModeSettingCoverage {
	coverage := make([]ModeSettingCoverage, 0, len(markers))
	for _, marker := range markers {
		area := modeSettingMarkers[marker]
		if area == "" {
			area = "unknown"
		}
		coverage = append(coverage, ModeSettingCoverage{Name: marker, Area: area})
	}
	sort.Slice(coverage, func(i, j int) bool {
		return coverage[i].Name < coverage[j].Name
	})
	return coverage
}

func classifyProviderCoverage(markers []string) []ProviderCoverage {
	coverage := make([]ProviderCoverage, 0, len(markers))
	for _, marker := range markers {
		area := providerProtocolMarkers[marker]
		if area == "" {
			area = "unknown"
		}
		coverage = append(coverage, ProviderCoverage{Name: marker, Area: area})
	}
	sort.Slice(coverage, func(i, j int) bool {
		return coverage[i].Name < coverage[j].Name
	})
	return coverage
}

func classifyAgentModeCoverage(modes []string) []AgentModeCoverage {
	coverage := make([]AgentModeCoverage, 0, len(modes))
	for _, mode := range modes {
		scope := agentModeScopes[mode]
		if scope == "" {
			scope = "unknown"
		}
		coverage = append(coverage, AgentModeCoverage{Name: mode, Scope: scope})
	}
	sort.Slice(coverage, func(i, j int) bool {
		return coverage[i].Name < coverage[j].Name
	})
	return coverage
}

func threadDeltaArea(event string) string {
	event = strings.TrimSpace(event)
	if area := threadDeltaAreas[event]; area != "" {
		return area
	}
	switch {
	case strings.HasPrefix(event, "assistant:message"):
		return "assistant-message"
	case strings.HasPrefix(event, "client_"):
		return "client-command"
	case strings.HasPrefix(event, "compaction_"):
		return "compaction"
	case strings.HasPrefix(event, "environment_"):
		return "environment"
	case strings.HasPrefix(event, "error_"):
		return "error"
	case strings.HasPrefix(event, "executor_"):
		return "executor-bridge"
	case strings.HasPrefix(event, "message_"):
		return "message"
	case strings.HasPrefix(event, "queued_message"):
		return "queue"
	case strings.HasPrefix(event, "retry_"):
		return "retry"
	case strings.HasPrefix(event, "thread_"):
		return "thread-state"
	case strings.HasPrefix(event, "thread:truncate"):
		return "history"
	case strings.HasPrefix(event, "tool_"):
		return "tool-state"
	case strings.HasPrefix(event, "tool:"):
		return "tool-result"
	case strings.HasPrefix(event, "user:message-queue:"):
		return "queue"
	case strings.HasPrefix(event, "user:message"):
		return "user-message"
	case strings.HasPrefix(event, "user:tool-input"):
		return "tool-input"
	default:
		return ""
	}
}

func toolCancelReasonArea(reason string) string {
	return toolCancelReasonAreas[strings.TrimSpace(reason)]
}

func classifySettingCoverage(settings []string) []SettingCoverage {
	coverage := make([]SettingCoverage, 0, len(settings))
	for _, setting := range settings {
		coverage = append(coverage, SettingCoverage{Name: setting, Scope: settingScope(setting)})
	}
	sort.Slice(coverage, func(i, j int) bool {
		return coverage[i].Name < coverage[j].Name
	})
	return coverage
}

func settingScope(setting string) string {
	scope := settingScopes[setting]
	if scope == "" {
		return "unknown"
	}
	return scope
}

func classifyModelCoverage(models []string) []ModelCoverage {
	coverage := make([]ModelCoverage, 0, len(models))
	for _, model := range models {
		provider, family := modelProviderAndFamily(model)
		coverage = append(coverage, ModelCoverage{Name: model, Provider: provider, Family: family})
	}
	sort.Slice(coverage, func(i, j int) bool {
		return coverage[i].Name < coverage[j].Name
	})
	return coverage
}

func extractModelLimits(s string) []ModelLimit {
	matches := modelLimitPattern.FindAllStringSubmatch(s, -1)
	if len(matches) == 0 {
		return nil
	}
	limits := make([]ModelLimit, 0, len(matches))
	for _, match := range matches {
		if len(match) < 7 {
			continue
		}
		contextWindow := parseModelLimitNumber(match[5])
		maxOutputTokens := parseModelLimitNumber(match[6])
		if contextWindow <= 0 || maxOutputTokens <= 0 {
			continue
		}
		name := strings.TrimSpace(match[3])
		if name == "" {
			continue
		}
		limits = append(limits, ModelLimit{
			Enum:            strings.TrimSpace(match[1]),
			Name:            name,
			Provider:        modelLimitProvider(match[2]),
			DisplayName:     strings.TrimSpace(match[4]),
			ContextWindow:   contextWindow,
			MaxOutputTokens: maxOutputTokens,
		})
	}
	return limits
}

func parseModelLimitNumber(value string) int {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if strings.ContainsAny(value, "eE") {
		parsed, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return 0
		}
		return int(parsed)
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0
	}
	return parsed
}

func modelLimitProvider(value string) string {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "ANTHROPIC":
		return "anthropic"
	case "BASENTEN", "BASETEN":
		return "baseten"
	case "CEREBRAS":
		return "cerebras"
	case "FIREWORKS":
		return "fireworks"
	case "MOONSHOT":
		return "moonshotai"
	case "OPENAI":
		return "openai"
	case "OPENROUTER":
		return "openrouter"
	case "VERTEXAI":
		return "google"
	case "XAI":
		return "xai"
	default:
		return strings.ToLower(strings.TrimSpace(value))
	}
}

func classifyActorCoverage(markers []string) []ActorCoverage {
	coverage := make([]ActorCoverage, 0, len(markers))
	for _, marker := range markers {
		area := actorMarkerAreas[marker]
		if area == "" {
			area = "unknown"
		}
		coverage = append(coverage, ActorCoverage{Name: marker, Area: area})
	}
	sort.Slice(coverage, func(i, j int) bool {
		return coverage[i].Name < coverage[j].Name
	})
	return coverage
}

func modelProviderAndFamily(model string) (string, string) {
	lower := strings.ToLower(strings.TrimSpace(model))
	switch {
	case strings.HasPrefix(lower, "claude-"):
		return "anthropic", claudeModelFamily(lower)
	case strings.HasPrefix(lower, "gemini-"):
		return "google", geminiModelFamily(lower)
	case strings.HasPrefix(lower, "amp-nostromo-"):
		return "openai", "amp-nostromo"
	case strings.HasPrefix(lower, "gpt-image-"):
		return "openai", "gpt-image"
	case strings.HasPrefix(lower, "gpt-oss-"):
		return "openai", "gpt-oss"
	case strings.HasPrefix(lower, "gpt-") && strings.Contains(lower, "codex"):
		return "openai", "gpt-codex"
	case strings.HasPrefix(lower, "gpt-"):
		return "openai", "gpt"
	case strings.HasPrefix(lower, "codex-"):
		return "openai", "codex"
	case len(lower) >= 2 && lower[0] == 'o' && lower[1] >= '0' && lower[1] <= '9':
		return "openai", "o-series"
	default:
		return "unknown", "unknown"
	}
}

func claudeModelFamily(model string) string {
	switch {
	case strings.Contains(model, "opus"):
		return "claude-opus"
	case strings.Contains(model, "sonnet"):
		return "claude-sonnet"
	case strings.Contains(model, "haiku"):
		return "claude-haiku"
	case strings.Contains(model, "instant"):
		return "claude-instant"
	default:
		return "claude-other"
	}
}

func geminiModelFamily(model string) string {
	switch {
	case strings.Contains(model, "embedding"):
		return "gemini-embedding"
	case strings.Contains(model, "image"):
		return "gemini-image"
	case strings.Contains(model, "flash"):
		return "gemini-flash"
	case strings.Contains(model, "pro"):
		return "gemini-pro"
	default:
		return "gemini-other"
	}
}

func normalizeRoute(raw string) string {
	route := strings.Trim(raw, " \t\r\n\"'`),;]}>{")
	route = strings.TrimRight(route, "$")
	if route == "" {
		return ""
	}
	route = strings.TrimRight(route, ".")
	if strings.Contains(route, "#") {
		return ""
	}
	route = threadIDPattern.ReplaceAllString(route, ":threadID")
	if strings.Contains(route, "://") {
		return ""
	}
	if strings.ContainsAny(route, "\\{}") {
		return ""
	}
	routePath := route
	if queryIndex := strings.Index(routePath, "?"); queryIndex >= 0 {
		routePath = routePath[:queryIndex]
	}
	if _, ok := ignoredRouteExtensions[strings.ToLower(filepath.Ext(routePath))]; ok {
		return ""
	}
	for _, prefix := range routePrefixes {
		if route == prefix || strings.HasPrefix(route, prefix+"/") || strings.HasPrefix(route, prefix+"?") {
			return route
		}
	}
	return ""
}

func plausibleThreadDeltaEvent(event string, knownEvents map[string]struct{}) bool {
	if _, ok := knownEvents[event]; ok {
		return true
	}
	parts := strings.Split(event, ":")
	if len(parts) < 2 {
		return false
	}
	switch parts[0] {
	case "assistant", "thread", "tool", "user":
	default:
		return false
	}
	for _, part := range parts[1:] {
		if _, ok := allowedEventSegments[part]; !ok {
			return false
		}
	}
	return true
}

func plausibleThreadProtocolEvent(event string, knownEvents map[string]struct{}) bool {
	if _, ignored := ignoredThreadProtocolLiterals[event]; ignored {
		return false
	}
	if _, ok := knownEvents[event]; ok {
		return true
	}
	switch {
	case strings.HasPrefix(event, "client_"):
		return true
	case strings.HasPrefix(event, "compaction_"):
		return true
	case strings.HasPrefix(event, "environment_"):
		return true
	case strings.HasPrefix(event, "error_"):
		return true
	case strings.HasPrefix(event, "executor_"):
		return true
	case strings.HasPrefix(event, "message_"):
		return true
	case strings.HasPrefix(event, "queued_message"):
		return true
	case strings.HasPrefix(event, "retry_"):
		return true
	case strings.HasPrefix(event, "thread_"):
		return true
	case strings.HasPrefix(event, "tool_"):
		return true
	case event == "agent_state" || event == "cancelled" || event == "delta" || event == "edit_rejected" || event == "inference_tools" || event == "observers" || event == "plugin_message":
		return true
	default:
		return false
	}
}

func containsThreadProtocolEventMarker(s, event string) bool {
	quoted := `"` + event + `"`
	return strings.Contains(s, "case"+quoted) ||
		strings.Contains(s, "type:"+quoted) ||
		strings.Contains(s, `"type":`+quoted) ||
		strings.Contains(s, ".literal("+quoted+")") ||
		strings.Contains(s, ".type==="+quoted) ||
		strings.Contains(s, ".type!=="+quoted)
}

func plausibleToolCancelReason(reason string) bool {
	reason = strings.TrimSpace(reason)
	if _, ignored := ignoredToolCancelReasonTokens[reason]; ignored {
		return false
	}
	if _, ok := toolCancelReasonAreas[reason]; ok {
		return true
	}
	parts := strings.Split(reason, ":")
	if len(parts) != 2 {
		return false
	}
	switch parts[0] {
	case "system", "user":
	default:
		return false
	}
	return len(parts[1]) >= 4
}

func containsToolRunStatusMarker(s, status string) bool {
	quoted := `"` + status + `"`
	return strings.Contains(s, "case"+quoted) ||
		strings.Contains(s, "status==="+quoted) ||
		strings.Contains(s, "status:"+quoted) ||
		strings.Contains(s, `status.trim().toLowerCase()).includes(`+quoted)
}

func containsThreadReaderMarker(s, marker string) bool {
	if strings.ContainsAny(marker, "_.-") {
		return strings.Contains(s, marker)
	}
	return strings.Contains(s, `"`+marker+`"`) ||
		strings.Contains(s, `'`+marker+`'`) ||
		strings.Contains(s, marker+":") ||
		strings.Contains(s, marker+"=") ||
		strings.Contains(s, "?"+marker)
}

func containsToolCatalogMarker(s, marker string) bool {
	if strings.ContainsAny(marker, ":_.-") {
		return strings.Contains(s, marker)
	}
	return strings.Contains(s, `"`+marker+`"`) ||
		strings.Contains(s, `'`+marker+`'`) ||
		strings.Contains(s, "`"+marker+"`") ||
		strings.Contains(s, marker+":") ||
		strings.Contains(s, marker+"=")
}

type agentModeMarkerPosition struct {
	Name  string
	Token string
	Index int
}

func extractAgentModeProfiles(s string) []AgentModeProfile {
	positions := agentModeMarkerPositions(s)
	if len(positions) == 0 {
		return nil
	}
	profiles := make([]AgentModeProfile, 0, len(positions))
	for i, position := range positions {
		end := len(s)
		if i+1 < len(positions) {
			end = positions[i+1].Index
		} else if relative := agentModeTableEnd(s[position.Index:]); relative >= 0 {
			end = position.Index + relative
		}
		if end <= position.Index {
			continue
		}
		segment := s[position.Index:end]
		profiles = append(profiles, AgentModeProfile{
			Name:            position.Name,
			PrimaryModel:    firstSubmatch(primaryModelRefPattern, segment),
			ReasoningEffort: firstSubmatch(reasoningEffortPattern, segment),
			ReasoningLevels: quotedValues(firstSubmatch(reasoningLevelsBlockPattern, segment)),
			IncludeTools:    firstSubmatch(includeToolsRefPattern, segment),
			DeferredTools:   strings.Contains(segment, "deferredTools:"),
			Visible:         strings.Contains(segment, "visible:!0"),
			VisibleInV2:     strings.Contains(segment, "visibleInV2:!0"),
			ServerOnly:      strings.Contains(segment, "serverOnly:!0"),
		})
	}
	return profiles
}

func agentModeTableEnd(s string) int {
	ends := []string{"}},Fc=", "}},uU="}
	best := -1
	for _, marker := range ends {
		if index := strings.Index(s, marker); index >= 0 && (best == -1 || index < best) {
			best = index
		}
	}
	return best
}

func agentModeMarkerPositions(s string) []agentModeMarkerPosition {
	var positions []agentModeMarkerPosition
	for _, marker := range agentModeMarkers {
		offset := 0
		for {
			index := strings.Index(s[offset:], marker.Token)
			if index < 0 {
				break
			}
			absolute := offset + index
			positions = append(positions, agentModeMarkerPosition{Name: marker.Name, Token: marker.Token, Index: absolute})
			offset = absolute + len(marker.Token)
		}
	}
	sort.Slice(positions, func(i, j int) bool {
		return positions[i].Index < positions[j].Index
	})
	return positions
}

func firstSubmatch(pattern *regexp.Regexp, s string) string {
	match := pattern.FindStringSubmatch(s)
	if len(match) < 2 {
		return ""
	}
	return match[1]
}

func quotedValues(s string) []string {
	var values []string
	for _, match := range quotedStringPattern.FindAllStringSubmatch(s, -1) {
		if len(match) >= 2 {
			values = append(values, match[1])
		}
	}
	sort.Strings(values)
	return values
}

func quotedValuesInOrder(s string) []string {
	var values []string
	for _, match := range quotedStringPattern.FindAllStringSubmatch(s, -1) {
		if len(match) >= 2 {
			values = append(values, match[1])
		}
	}
	return values
}

func splitSignalTokens(s string) []string {
	return strings.FieldsFunc(s, func(r rune) bool {
		return !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == ':')
	})
}

func plausibleModelName(model string) bool {
	if len(model) > 96 || !strings.ContainsAny(model, "0123456789") {
		return false
	}
	if strings.Contains(model, "__") || strings.Contains(model, ".constructor") || strings.Contains(model, "://") {
		return false
	}
	return true
}

func promptLikeSegments(s string) []string {
	var out []string
	if len(s) >= 160 && len(s) <= 20000 {
		out = append(out, s)
	}
	if len(s) > 20000 {
		for _, segment := range splitLargeStringLiterals(s) {
			if len(segment) >= 160 && len(segment) <= 20000 {
				out = append(out, segment)
			}
		}
	}
	return out
}

func splitLargeStringLiterals(s string) []string {
	var out []string
	var quote rune
	start := -1
	escaped := false
	for i, r := range s {
		if quote == 0 {
			if r == '"' || r == '\'' || r == '`' {
				quote = r
				start = i + len(string(r))
			}
			continue
		}
		if escaped {
			escaped = false
			continue
		}
		if r == '\\' && quote != '`' {
			escaped = true
			continue
		}
		if r == quote {
			if start >= 0 && i > start {
				out = append(out, s[start:i])
			}
			quote = 0
			start = -1
		}
	}
	return out
}

func promptFingerprint(s string) (PromptFingerprint, bool) {
	tags := promptTags(s)
	if len(tags) == 0 {
		return PromptFingerprint{}, false
	}
	if strings.Count(s, " ") < 12 {
		return PromptFingerprint{}, false
	}
	sum := sha256.Sum256([]byte(s))
	return PromptFingerprint{
		SHA256: hex.EncodeToString(sum[:]),
		Length: len(s),
		Kind:   promptFingerprintKind(s),
		Tags:   tags,
	}, true
}

func promptFingerprintKind(s string) string {
	if looksLikeMinifiedSource(s) {
		return "source"
	}
	return "prompt"
}

func looksLikeMinifiedSource(s string) bool {
	if len(s) < 500 {
		return false
	}
	sourceMarkers := 0
	for _, marker := range []string{
		"async function ",
		"function ",
		"=>",
		"?.",
		"??",
		"process.exit",
		"await ",
		"return ",
		"this.",
		"new ",
	} {
		if strings.Contains(s, marker) {
			sourceMarkers++
		}
	}
	if sourceMarkers < 2 {
		return false
	}
	lineBreaks := strings.Count(s, "\n") + strings.Count(s, `\n`)
	syntaxChars := countAnyByte(s, "{}[]();=<>|&")
	return lineBreaks <= 8 && syntaxChars*100 >= len(s)*4
}

func countAnyByte(s, chars string) int {
	set := map[byte]struct{}{}
	for i := 0; i < len(chars); i++ {
		set[chars[i]] = struct{}{}
	}
	count := 0
	for i := 0; i < len(s); i++ {
		if _, ok := set[s[i]]; ok {
			count++
		}
	}
	return count
}

func promptExcerptText(s string, limit int) string {
	decoded := strings.NewReplacer(`\n`, "\n", `\r`, "\r", `\t`, "\t").Replace(s)
	decoded = strings.ReplaceAll(decoded, "\r\n", "\n")
	decoded = strings.ReplaceAll(decoded, "\r", "\n")
	cleaned := strings.TrimSpace(decoded)
	if cleaned == "" || strings.Count(cleaned, "\n") <= 1 {
		cleaned = strings.Join(strings.Fields(decoded), " ")
	} else {
		lines := strings.Split(cleaned, "\n")
		kept := make([]string, 0, len(lines))
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if line != "" {
				kept = append(kept, line)
			}
		}
		cleaned = strings.Join(kept, "\n")
	}
	if limit <= 0 || len(cleaned) <= limit {
		return cleaned
	}
	anchor := promptExcerptAnchor(cleaned)
	start := 0
	if anchor > limit/3 {
		start = anchor - limit/3
	}
	if start+limit > len(cleaned) {
		start = len(cleaned) - limit
	}
	if start < 0 {
		start = 0
	}
	excerpt := strings.TrimSpace(cleaned[start : start+limit])
	if start > 0 {
		excerpt = "..." + excerpt
	}
	if start+limit < len(cleaned) {
		excerpt += "..."
	}
	return excerpt
}

func promptExcerptAnchor(s string) int {
	lower := strings.ToLower(s)
	best := -1
	for _, needle := range []string{
		"code review",
		"review the current",
		"compaction",
		"compact",
		"summarize",
		"system prompt",
		"you are amp",
		"guidance",
		"instruction",
		"reasoning effort",
		"agent mode",
		"thinking level",
		"skill",
		"skills.path",
		"tool call",
		"toolbox",
		"painter",
		"image generation",
		"gpt-image",
		"artifact",
		".amp/in/artifacts",
	} {
		idx := strings.Index(lower, needle)
		if idx >= 0 && (best == -1 || idx < best) {
			best = idx
		}
	}
	if best < 0 {
		return 0
	}
	return best
}

func promptTags(s string) []string {
	lower := strings.ToLower(s)
	tagChecks := map[string][]string{
		"artifacts":     {"artifact", ".amp/in/artifacts"},
		"code-review":   {"code review", "review the current"},
		"compaction":    {"compaction", "compact", "summarize"},
		"guidance":      {"guidance", "instruction"},
		"painter":       {"painter", "image generation", "gpt-image"},
		"settings":      {"reasoning effort", "agent mode", "thinking level"},
		"skills":        {"skill", "skills.path"},
		"system-prompt": {"system prompt", "you are amp", "you are an"},
		"tools":         {"tool", "toolbox", "tool call"},
	}
	var tags []string
	for tag, needles := range tagChecks {
		for _, needle := range needles {
			if strings.Contains(lower, needle) {
				tags = append(tags, tag)
				break
			}
		}
	}
	sort.Strings(tags)
	return tags
}

func matchesFromStrings(strs []string, pattern *regexp.Regexp) []string {
	found := map[string]struct{}{}
	for _, s := range strs {
		for _, match := range pattern.FindAllString(s, -1) {
			found[match] = struct{}{}
		}
	}
	return sortedKeys(found)
}

func sortedSet(values []string) []string {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	return sortedKeys(set)
}

func sortedKeys(set map[string]struct{}) []string {
	values := make([]string, 0, len(set))
	for value := range set {
		values = append(values, value)
	}
	sort.Strings(values)
	return values
}

func addRouteMethods(index map[string]map[string]struct{}, route string, methods []string) {
	if route == "" || len(methods) == 0 {
		return
	}
	if index[route] == nil {
		index[route] = map[string]struct{}{}
	}
	for _, method := range methods {
		method = strings.ToUpper(strings.TrimSpace(method))
		if method != "" {
			index[route][method] = struct{}{}
		}
	}
}

func routeMethodsNear(s string, start, end int) []string {
	methods := map[string]struct{}{}

	headStart := start - 160
	if headStart < 0 {
		headStart = 0
	}
	head := s[headStart:start]
	if match := httpQuotedMethodArgPattern.FindStringSubmatch(head); len(match) == 2 {
		methods[strings.ToUpper(match[1])] = struct{}{}
	}
	if match := httpMethodCallPrefixPattern.FindStringSubmatch(head); len(match) == 2 {
		methods[strings.ToUpper(match[1])] = struct{}{}
	}

	tailEnd := end + 240
	if tailEnd > len(s) {
		tailEnd = len(s)
	}
	tail := s[end:tailEnd]
	if nextRoute := routePattern.FindStringIndex(tail); nextRoute != nil {
		tail = tail[:nextRoute[0]]
	}
	for _, match := range httpMethodPropertyPattern.FindAllStringSubmatch(tail, -1) {
		if len(match) == 2 {
			methods[strings.ToUpper(match[1])] = struct{}{}
		}
	}

	return sortedKeys(methods)
}

func sortedRouteMethods(set map[string]map[string]struct{}) []RouteMethods {
	values := make([]RouteMethods, 0, len(set))
	for route, methods := range set {
		if len(methods) == 0 {
			continue
		}
		scope := routeScope(route)
		if scope == "" {
			scope = "unknown"
		}
		values = append(values, RouteMethods{
			Name:    route,
			Methods: sortedKeys(methods),
			Scope:   scope,
		})
	}
	sort.Slice(values, func(i, j int) bool {
		return values[i].Name < values[j].Name
	})
	return values
}

func sortedSettingDefaults(set map[string]SettingDefault) []SettingDefault {
	values := make([]SettingDefault, 0, len(set))
	for _, value := range set {
		values = append(values, value)
	}
	sort.Slice(values, func(i, j int) bool {
		if values[i].Name == values[j].Name {
			return values[i].Value < values[j].Value
		}
		return values[i].Name < values[j].Name
	})
	return values
}

func sortedModelLimits(set map[string]ModelLimit) []ModelLimit {
	values := make([]ModelLimit, 0, len(set))
	for _, value := range set {
		values = append(values, value)
	}
	sort.Slice(values, func(i, j int) bool {
		return values[i].Name < values[j].Name
	})
	return values
}

func sortedPromptFingerprints(set map[string]PromptFingerprint) []PromptFingerprint {
	values := make([]PromptFingerprint, 0, len(set))
	for _, value := range set {
		sort.Strings(value.Tags)
		values = append(values, value)
	}
	sort.Slice(values, func(i, j int) bool {
		return values[i].SHA256 < values[j].SHA256
	})
	return values
}

func sortedAgentModeProfiles(set map[string]AgentModeProfile) []AgentModeProfile {
	values := make([]AgentModeProfile, 0, len(set))
	for _, value := range set {
		sort.Strings(value.ReasoningLevels)
		values = append(values, value)
	}
	sort.Slice(values, func(i, j int) bool {
		return values[i].Name < values[j].Name
	})
	return values
}

func sortedAgentModeProfileNames(set map[string]AgentModeProfile) []string {
	names := make([]string, 0, len(set))
	for name := range set {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func promptTagCounts(set map[string]PromptFingerprint) []PromptTagCount {
	counts := map[string]int{}
	for _, fp := range set {
		tagSet := map[string]struct{}{}
		for _, tag := range fp.Tags {
			if tag != "" {
				tagSet[tag] = struct{}{}
			}
		}
		kind := promptKindOrDefault(fp.Kind)
		for tag := range tagSet {
			counts[kind+"\x00"+tag]++
		}
	}
	values := make([]PromptTagCount, 0, len(counts))
	for key, count := range counts {
		kind, tag, ok := strings.Cut(key, "\x00")
		if !ok {
			kind = "prompt"
			tag = key
		}
		values = append(values, PromptTagCount{Kind: kind, Tag: tag, Count: count})
	}
	sort.Slice(values, func(i, j int) bool {
		if values[i].Kind == values[j].Kind {
			return values[i].Tag < values[j].Tag
		}
		return values[i].Kind < values[j].Kind
	})
	return values
}

func firstN(values []string, n int) []string {
	if len(values) <= n {
		return values
	}
	return values[:n]
}

func readSnapshotFile(path string) (Snapshot, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Snapshot{}, err
	}
	var snapshot Snapshot
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		return Snapshot{}, err
	}
	if snapshot.Schema < minSupportedSnapshotSchema || snapshot.Schema > snapshotSchema {
		return Snapshot{}, unsupportedSnapshotSchemaError{Schema: snapshot.Schema}
	}
	return snapshot, nil
}

type unsupportedSnapshotSchemaError struct {
	Schema int
}

func (err unsupportedSnapshotSchemaError) Error() string {
	return fmt.Sprintf("%v %d, supported range is %d-%d", errUnsupportedSnapshotSchema, err.Schema, minSupportedSnapshotSchema, snapshotSchema)
}

func (err unsupportedSnapshotSchemaError) Is(target error) bool {
	return target == errUnsupportedSnapshotSchema
}

func writeSnapshotFile(path string, snapshot Snapshot) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if err := writeJSON(f, snapshot); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func writeJSON(w io.Writer, value any) error {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func diffSnapshots(old, current Snapshot) auditDiff {
	return auditDiff{
		Categories: []categoryDiff{
			diffStringCategory("binary-source", sourceIdentityStrings(old.Source), sourceIdentityStrings(current.Source)),
			diffStringCategory("routes", old.Signals.Routes, current.Signals.Routes),
			diffStringCategory("route-methods", routeMethodStrings(old.Signals.RouteMethods), routeMethodStrings(current.Signals.RouteMethods)),
			diffStringCategory("route-coverage", routeCoverageStrings(old.Signals.RouteCoverage), routeCoverageStrings(current.Signals.RouteCoverage)),
			diffStringCategory("thread-delta-events", old.Signals.ThreadDeltaEvents, current.Signals.ThreadDeltaEvents),
			diffStringCategory("thread-delta-coverage", threadDeltaCoverageStrings(old.Signals.ThreadDeltaCoverage), threadDeltaCoverageStrings(current.Signals.ThreadDeltaCoverage)),
			diffStringCategory("thread-reader-markers", old.Signals.ThreadReaderMarkers, current.Signals.ThreadReaderMarkers),
			diffStringCategory("thread-reader-coverage", threadReaderCoverageStrings(old.Signals.ThreadReaderCoverage), threadReaderCoverageStrings(current.Signals.ThreadReaderCoverage)),
			diffStringCategory("tool-cancel-reasons", old.Signals.ToolCancelReasons, current.Signals.ToolCancelReasons),
			diffStringCategory("tool-cancel-coverage", toolCancelCoverageStrings(old.Signals.ToolCancelCoverage), toolCancelCoverageStrings(current.Signals.ToolCancelCoverage)),
			diffStringCategory("tool-run-statuses", old.Signals.ToolRunStatuses, current.Signals.ToolRunStatuses),
			diffStringCategory("tool-run-coverage", toolRunCoverageStrings(old.Signals.ToolRunCoverage), toolRunCoverageStrings(current.Signals.ToolRunCoverage)),
			diffStringCategory("tool-catalog-markers", old.Signals.ToolCatalog, current.Signals.ToolCatalog),
			diffStringCategory("tool-catalog-coverage", toolCatalogCoverageStrings(old.Signals.ToolCatalogCoverage), toolCatalogCoverageStrings(current.Signals.ToolCatalogCoverage)),
			diffStringCategory("stream-json-markers", old.Signals.StreamJSONMarkers, current.Signals.StreamJSONMarkers),
			diffStringCategory("stream-json-coverage", streamJSONCoverageStrings(old.Signals.StreamJSONCoverage), streamJSONCoverageStrings(current.Signals.StreamJSONCoverage)),
			diffStringCategory("mode-setting-markers", old.Signals.ModeSettingMarkers, current.Signals.ModeSettingMarkers),
			diffStringCategory("mode-setting-coverage", modeSettingCoverageStrings(old.Signals.ModeSettingCoverage), modeSettingCoverageStrings(current.Signals.ModeSettingCoverage)),
			diffStringCategory("provider-protocol-markers", old.Signals.ProviderProtocol, current.Signals.ProviderProtocol),
			diffStringCategory("provider-protocol-coverage", providerCoverageStrings(old.Signals.ProviderCoverage), providerCoverageStrings(current.Signals.ProviderCoverage)),
			diffStringCategory("agent-mode-profiles", agentModeProfileStrings(old.Signals.AgentModeProfiles), agentModeProfileStrings(current.Signals.AgentModeProfiles)),
			diffStringCategory("agent-mode-routes", agentModeRouteStrings(old.Signals.AgentModeRoutes), agentModeRouteStrings(current.Signals.AgentModeRoutes)),
			diffStringCategory("agent-mode-coverage", agentModeCoverageStrings(old.Signals.AgentModeCoverage), agentModeCoverageStrings(current.Signals.AgentModeCoverage)),
			diffStringCategory("settings", old.Signals.Settings, current.Signals.Settings),
			diffStringCategory("setting-defaults", settingDefaultStrings(old.Signals.SettingDefaults), settingDefaultStrings(current.Signals.SettingDefaults)),
			diffStringCategory("setting-coverage", settingCoverageStrings(old.Signals.SettingCoverage), settingCoverageStrings(current.Signals.SettingCoverage)),
			diffStringCategory("models", old.Signals.Models, current.Signals.Models),
			diffStringCategory("model-limits", modelLimitStrings(old.Signals.ModelLimits), modelLimitStrings(current.Signals.ModelLimits)),
			diffStringCategory("large-context-rules", largeContextRuleStrings(old.Signals.LargeContextRules), largeContextRuleStrings(current.Signals.LargeContextRules)),
			diffStringCategory("adaptive-thinking-rules", adaptiveThinkingRuleStrings(old.Signals.AdaptiveThinking), adaptiveThinkingRuleStrings(current.Signals.AdaptiveThinking)),
			diffStringCategory("provider-reasoning-rules", providerReasoningRuleStrings(old.Signals.ProviderReasoning), providerReasoningRuleStrings(current.Signals.ProviderReasoning)),
			diffStringCategory("provider-header-rules", providerHeaderRuleStrings(old.Signals.ProviderHeaders), providerHeaderRuleStrings(current.Signals.ProviderHeaders)),
			diffStringCategory("provider-feature-rules", providerFeatureRuleStrings(old.Signals.ProviderFeatures), providerFeatureRuleStrings(current.Signals.ProviderFeatures)),
			diffStringCategory("compaction-rules", compactionRuleStrings(old.Signals.CompactionRules), compactionRuleStrings(current.Signals.CompactionRules)),
			diffStringCategory("model-coverage", modelCoverageStrings(old.Signals.ModelCoverage), modelCoverageStrings(current.Signals.ModelCoverage)),
			diffStringCategory("actor-runtime-markers", old.Signals.ActorRuntime, current.Signals.ActorRuntime),
			diffStringCategory("actor-runtime-coverage", actorCoverageStrings(old.Signals.ActorCoverage), actorCoverageStrings(current.Signals.ActorCoverage)),
			diffStringCategory("prompt-fingerprint-metadata", promptFingerprintMetadataStrings(old.Signals.PromptFingerprints), promptFingerprintMetadataStrings(current.Signals.PromptFingerprints)),
			diffStringCategory("prompt-kind-counts", promptKindCountStrings(old.Signals.PromptFingerprints), promptKindCountStrings(current.Signals.PromptFingerprints)),
			diffStringCategory("prompt-tag-counts", promptTagCountStrings(old.Signals.PromptTagCounts), promptTagCountStrings(current.Signals.PromptTagCounts)),
		},
		Prompts: diffPromptFingerprints(old.Signals.PromptFingerprints, current.Signals.PromptFingerprints),
	}
}

func sourceIdentityStrings(source SourceInfo) []string {
	var values []string
	if strings.TrimSpace(source.SHA256) != "" {
		values = append(values, "sha256="+source.SHA256)
	}
	if source.SizeBytes > 0 {
		values = append(values, fmt.Sprintf("size_bytes=%d", source.SizeBytes))
	}
	if len(source.Versions) > 0 {
		values = append(values, "versions="+strings.Join(source.Versions, ","))
	}
	if len(source.BuildStamps) > 0 {
		values = append(values, "build_stamps="+strings.Join(source.BuildStamps, ","))
	}
	if source.StringsScanned > 0 {
		values = append(values, fmt.Sprintf("strings_scanned=%d", source.StringsScanned))
	}
	sort.Strings(values)
	return values
}

func diffStringCategory(name string, old, current []string) categoryDiff {
	oldSet := sliceSet(old)
	currentSet := sliceSet(current)
	var added, removed []string
	for value := range currentSet {
		if _, ok := oldSet[value]; !ok {
			added = append(added, value)
		}
	}
	for value := range oldSet {
		if _, ok := currentSet[value]; !ok {
			removed = append(removed, value)
		}
	}
	sort.Strings(added)
	sort.Strings(removed)
	return categoryDiff{Name: name, Added: added, Removed: removed}
}

func diffPromptFingerprints(old, current []PromptFingerprint) promptDiff {
	oldSet := promptSet(old)
	currentSet := promptSet(current)
	var added, removed []PromptFingerprint
	for hash, fp := range currentSet {
		if _, ok := oldSet[hash]; !ok {
			added = append(added, fp)
		}
	}
	for hash, fp := range oldSet {
		if _, ok := currentSet[hash]; !ok {
			removed = append(removed, fp)
		}
	}
	sort.Slice(added, func(i, j int) bool { return added[i].SHA256 < added[j].SHA256 })
	sort.Slice(removed, func(i, j int) bool { return removed[i].SHA256 < removed[j].SHA256 })
	return promptDiff{Added: added, Removed: removed}
}

func sliceSet(values []string) map[string]struct{} {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	return set
}

func settingCoverageStrings(coverage []SettingCoverage) []string {
	values := make([]string, 0, len(coverage))
	for _, item := range coverage {
		values = append(values, item.Name+"="+item.Scope)
	}
	sort.Strings(values)
	return values
}

func settingDefaultStrings(defaults []SettingDefault) []string {
	values := make([]string, 0, len(defaults))
	for _, item := range defaults {
		values = append(values, item.Name+"="+item.Value+"="+item.Scope)
	}
	sort.Strings(values)
	return values
}

func routeCoverageStrings(coverage []RouteCoverage) []string {
	values := make([]string, 0, len(coverage))
	for _, item := range coverage {
		values = append(values, item.Name+"="+item.Scope)
	}
	sort.Strings(values)
	return values
}

func routeMethodStrings(methods []RouteMethods) []string {
	values := make([]string, 0, len(methods))
	for _, item := range methods {
		values = append(values, routeMethodString(item))
	}
	sort.Strings(values)
	return values
}

func routeMethodString(item RouteMethods) string {
	methods := append([]string{}, item.Methods...)
	sort.Strings(methods)
	return item.Name + "=" + strings.Join(methods, ",") + "=" + item.Scope
}

func threadDeltaCoverageStrings(coverage []ThreadDeltaCoverage) []string {
	values := make([]string, 0, len(coverage))
	for _, item := range coverage {
		values = append(values, item.Name+"="+item.Area)
	}
	sort.Strings(values)
	return values
}

func threadReaderCoverageStrings(coverage []ThreadReaderCoverage) []string {
	values := make([]string, 0, len(coverage))
	for _, item := range coverage {
		values = append(values, item.Name+"="+item.Area)
	}
	sort.Strings(values)
	return values
}

func toolCancelCoverageStrings(coverage []ToolCancelCoverage) []string {
	values := make([]string, 0, len(coverage))
	for _, item := range coverage {
		values = append(values, item.Name+"="+item.Area)
	}
	sort.Strings(values)
	return values
}

func toolRunCoverageStrings(coverage []ToolRunCoverage) []string {
	values := make([]string, 0, len(coverage))
	for _, item := range coverage {
		values = append(values, item.Name+"="+item.Area)
	}
	sort.Strings(values)
	return values
}

func toolCatalogCoverageStrings(coverage []ToolCatalogCoverage) []string {
	values := make([]string, 0, len(coverage))
	for _, item := range coverage {
		values = append(values, item.Name+"="+item.Area)
	}
	sort.Strings(values)
	return values
}

func streamJSONCoverageStrings(coverage []StreamJSONCoverage) []string {
	values := make([]string, 0, len(coverage))
	for _, item := range coverage {
		values = append(values, item.Name+"="+item.Area)
	}
	sort.Strings(values)
	return values
}

func modeSettingCoverageStrings(coverage []ModeSettingCoverage) []string {
	values := make([]string, 0, len(coverage))
	for _, item := range coverage {
		values = append(values, item.Name+"="+item.Area)
	}
	sort.Strings(values)
	return values
}

func providerCoverageStrings(coverage []ProviderCoverage) []string {
	values := make([]string, 0, len(coverage))
	for _, item := range coverage {
		values = append(values, item.Name+"="+item.Area)
	}
	sort.Strings(values)
	return values
}

func agentModeProfileStrings(profiles []AgentModeProfile) []string {
	values := make([]string, 0, len(profiles))
	for _, profile := range profiles {
		values = append(values, strings.Join([]string{
			profile.Name,
			"primary=" + profile.PrimaryModel,
			"reasoning=" + profile.ReasoningEffort,
			"levels=" + strings.Join(profile.ReasoningLevels, ","),
			"include=" + normalizedAgentModeIncludeTools(profile.IncludeTools),
			fmt.Sprintf("deferred=%t", profile.DeferredTools),
			fmt.Sprintf("visible=%t", profile.Visible),
			fmt.Sprintf("visibleInV2=%t", profile.VisibleInV2),
			fmt.Sprintf("serverOnly=%t", profile.ServerOnly),
		}, "|"))
	}
	sort.Strings(values)
	return values
}

func normalizedAgentModeIncludeTools(value string) string {
	if strings.TrimSpace(value) == "" {
		return ""
	}
	return "present"
}

func agentModeRouteStrings(routes []AgentModeRoute) []string {
	values := make([]string, 0, len(routes))
	for _, route := range routes {
		parts := []string{
			route.Name,
			"provider=" + route.Provider,
			"model=" + route.Model,
			"primary=" + route.PrimaryModel,
			"reasoning=" + route.ReasoningEffort,
			fmt.Sprintf("context=%d", route.ContextWindow),
			fmt.Sprintf("max_out=%d", route.MaxOutputTokens),
		}
		if route.EffectiveContextWindow > 0 {
			parts = append(parts,
				fmt.Sprintf("effective_context=%d", route.EffectiveContextWindow),
				fmt.Sprintf("effective_max_input=%d", route.EffectiveMaxInputTokens),
			)
		}
		if route.LargeContextAlias != "" {
			parts = append(parts, "large_alias="+route.LargeContextAlias)
		}
		values = append(values, strings.Join(parts, "|"))
	}
	sort.Strings(values)
	return values
}

func agentModeCoverageStrings(coverage []AgentModeCoverage) []string {
	values := make([]string, 0, len(coverage))
	for _, item := range coverage {
		values = append(values, item.Name+"="+item.Scope)
	}
	sort.Strings(values)
	return values
}

func modelLimitStrings(limits []ModelLimit) []string {
	values := make([]string, 0, len(limits))
	for _, limit := range limits {
		values = append(values, modelLimitString(limit))
	}
	sort.Strings(values)
	return values
}

func modelLimitString(limit ModelLimit) string {
	return strings.Join([]string{
		limit.Name,
		"enum=" + limit.Enum,
		"provider=" + limit.Provider,
		"display=" + limit.DisplayName,
		fmt.Sprintf("context=%d", limit.ContextWindow),
		fmt.Sprintf("max_out=%d", limit.MaxOutputTokens),
	}, "|")
}

func largeContextRuleStrings(rules []LargeContextRule) []string {
	values := make([]string, 0, len(rules))
	for _, rule := range rules {
		values = append(values, strings.Join([]string{
			rule.PrimaryModel,
			"alias=" + rule.Alias,
			fmt.Sprintf("context=%d", rule.ContextWindow),
			fmt.Sprintf("max_out=%d", rule.MaxOutputTokens),
			fmt.Sprintf("max_input=%d", rule.MaxInputTokens),
			fmt.Sprintf("requires_enable=%t", rule.RequiresEnableLargeContext),
		}, "|"))
	}
	sort.Strings(values)
	return values
}

func adaptiveThinkingRuleStrings(rules []AdaptiveThinkingRule) []string {
	values := make([]string, 0, len(rules))
	for _, rule := range rules {
		values = append(values, strings.Join([]string{
			strings.Join(rule.ModelEnums, ","),
			"models=" + strings.Join(rule.Models, ","),
			"levels=" + strings.Join(rule.EffortLevels, ","),
			"default=" + rule.DefaultEffort,
			"type=" + rule.ThinkingType,
			"display=" + rule.Display,
			fmt.Sprintf("output_config=%t", rule.UsesOutputConfig),
		}, "|"))
	}
	sort.Strings(values)
	return values
}

func providerReasoningRuleStrings(rules []ProviderReasoningRule) []string {
	values := make([]string, 0, len(rules))
	for _, rule := range rules {
		parts := []string{
			rule.Provider,
			"sources=" + strings.Join(rule.Sources, ","),
			"setting=" + rule.Setting,
			"default=" + rule.DefaultEffort,
		}
		if rule.SpecialModelEnum != "" || rule.SpecialModel != "" || rule.SpecialModelEffort != "" {
			parts = append(parts, "special="+rule.SpecialModelEnum+"/"+rule.SpecialModel+":"+rule.SpecialModelEffort)
		}
		values = append(values, strings.Join(parts, "|"))
	}
	sort.Strings(values)
	return values
}

func providerHeaderRuleStrings(rules []ProviderHeaderRule) []string {
	values := make([]string, 0, len(rules))
	for _, rule := range rules {
		parts := []string{
			rule.Provider,
			"feature=" + rule.FeatureHeader + ":" + rule.Feature,
			"thread_id=" + rule.ThreadIDHeader + "<-" + rule.ThreadIDSource,
			"message_id=" + rule.MessageIDHeader + "<-" + rule.MessageIDSource,
			"beta_header=" + rule.BetaHeader,
			"interleaved=" + rule.InterleavedBeta + "@" + rule.ThinkingEnabledSetting + "+" + rule.InterleavedThinkingSetting + fmt.Sprintf("+skip_adaptive=%t", rule.SkipsAdaptiveThinkingModels),
			"override=" + rule.OverrideProviderHeader + "<-" + rule.OverrideProviderSetting,
			"fast=" + rule.FastModeBeta + "@" + rule.FastModeSetting + "=" + rule.FastModeValue + "=>" + rule.FastModeOverrideProvider,
		}
		values = append(values, strings.Join(parts, "|"))
	}
	sort.Strings(values)
	return values
}

func providerFeatureRuleStrings(rules []ProviderFeatureRule) []string {
	values := make([]string, 0, len(rules))
	for _, rule := range rules {
		values = append(values, strings.Join([]string{
			rule.Feature,
			"provider=" + rule.Provider,
			"callsite=" + rule.Callsite,
			"tool=" + rule.Tool,
			"header=" + rule.Header,
			fmt.Sprintf("default=%t", rule.Default),
		}, "|"))
	}
	sort.Strings(values)
	return values
}

func compactionRuleStrings(rules []CompactionRule) []string {
	values := make([]string, 0, len(rules))
	for _, rule := range rules {
		values = append(values, strings.Join([]string{
			rule.Name,
			"provider=" + rule.Provider,
			"trigger=" + rule.Trigger,
			"timing=" + rule.Timing,
			fmt.Sprintf("threshold=%d", rule.DefaultThresholdTokens),
			"usage=" + strings.Join(rule.UsageFields, ","),
			"summary_prompt=" + rule.SummaryPrompt,
			"history_role=" + rule.HistoryReplacementRole,
			"tail_assistant=" + rule.TrailingAssistantToolUse,
			"helper=" + rule.HelperHeader + ":" + rule.HelperHeaderValue,
		}, "|"))
	}
	sort.Strings(values)
	return values
}

func modelCoverageStrings(coverage []ModelCoverage) []string {
	values := make([]string, 0, len(coverage))
	for _, item := range coverage {
		values = append(values, item.Name+"="+item.Provider+"/"+item.Family)
	}
	sort.Strings(values)
	return values
}

func actorCoverageStrings(coverage []ActorCoverage) []string {
	values := make([]string, 0, len(coverage))
	for _, item := range coverage {
		values = append(values, item.Name+"="+item.Area)
	}
	sort.Strings(values)
	return values
}

func promptTagCountStrings(counts []PromptTagCount) []string {
	values := make([]string, 0, len(counts))
	for _, item := range counts {
		values = append(values, fmt.Sprintf("%s/%s=%d", promptKindOrDefault(item.Kind), item.Tag, item.Count))
	}
	sort.Strings(values)
	return values
}

func promptFingerprintMetadataStrings(fingerprints []PromptFingerprint) []string {
	values := make([]string, 0, len(fingerprints))
	for _, fp := range fingerprints {
		tags := append([]string{}, fp.Tags...)
		sort.Strings(tags)
		values = append(values, fmt.Sprintf("%s|len=%d|kind=%s|tags=%s", fp.SHA256, fp.Length, promptKindOrDefault(fp.Kind), strings.Join(tags, ",")))
	}
	sort.Strings(values)
	return values
}

func promptKindCountStrings(fingerprints []PromptFingerprint) []string {
	counts := map[string]int{}
	for _, fp := range fingerprints {
		counts[promptKindOrDefault(fp.Kind)]++
	}
	values := make([]string, 0, len(counts))
	for kind, count := range counts {
		values = append(values, fmt.Sprintf("%s=%d", kind, count))
	}
	sort.Strings(values)
	return values
}

func promptSet(values []PromptFingerprint) map[string]PromptFingerprint {
	set := make(map[string]PromptFingerprint, len(values))
	for _, value := range values {
		set[value.SHA256] = value
	}
	return set
}

func hasDiff(diff auditDiff) bool {
	for _, category := range diff.Categories {
		if len(category.Added) > 0 || len(category.Removed) > 0 {
			return true
		}
	}
	return len(diff.Prompts.Added) > 0 || len(diff.Prompts.Removed) > 0
}

func strictAuditFailed(snapshot Snapshot, diff auditDiff) bool {
	return hasDiff(diff) || len(snapshotAuditProblems(snapshot)) > 0
}

func strictAuditFailedWithBaseline(baseline, current Snapshot, diff auditDiff) bool {
	return strictAuditFailed(current, diff) || len(snapshotAuditProblems(baseline)) > 0
}

func baselineWriteProblems(snapshot Snapshot, diff auditDiff, allowUnknown, acceptDiff bool) []string {
	var problems []string
	problems = append(problems, snapshotSourceMetadataProblems(snapshot)...)
	problems = append(problems, snapshotDuplicateProblems(snapshot)...)
	problems = append(problems, snapshotCompletenessProblems(snapshot)...)
	problems = append(problems, snapshotRawSignalProblems(snapshot)...)
	problems = append(problems, snapshotCoverageMismatchProblems(snapshot)...)
	problems = append(problems, snapshotExactValueProblems(snapshot)...)
	if !allowUnknown {
		problems = append(problems, snapshotCoverageProblems(snapshot)...)
	}
	problems = append(problems, snapshotPromptTagProblems(snapshot)...)
	if !acceptDiff && hasDiff(diff) {
		problems = append(problems, "baseline differs from current Amp binary: run the parity audit checklist before refreshing the baseline")
	}
	return problems
}

func prefixedSnapshotAuditProblems(prefix string, snapshot Snapshot) []string {
	problems := snapshotAuditProblems(snapshot)
	if len(problems) == 0 {
		return nil
	}
	prefixed := make([]string, 0, len(problems))
	for _, problem := range problems {
		prefixed = append(prefixed, prefix+": "+problem)
	}
	return prefixed
}

func acceptableExistingBaselineAuditProblems(problems []string) bool {
	if len(problems) == 0 {
		return true
	}
	for _, problem := range problems {
		if strings.Contains(problem, "unexpected prompt/source kind counts:") ||
			strings.Contains(problem, "unexpected prompt/source tag counts:") {
			continue
		}
		return false
	}
	return true
}

func snapshotAuditProblems(snapshot Snapshot) []string {
	var problems []string
	problems = append(problems, snapshotSourceMetadataProblems(snapshot)...)
	problems = append(problems, snapshotDuplicateProblems(snapshot)...)
	problems = append(problems, snapshotCompletenessProblems(snapshot)...)
	problems = append(problems, snapshotRawSignalProblems(snapshot)...)
	problems = append(problems, snapshotCoverageMismatchProblems(snapshot)...)
	problems = append(problems, snapshotExactValueProblems(snapshot)...)
	problems = append(problems, snapshotCoverageProblems(snapshot)...)
	problems = append(problems, snapshotPromptTagProblems(snapshot)...)
	return problems
}

func snapshotSourceMetadataProblems(snapshot Snapshot) []string {
	if snapshot.Source.SizeBytes < minReleaseBinarySizeBytes {
		return nil
	}
	var missing []string
	if strings.TrimSpace(snapshot.Source.SHA256) == "" {
		missing = append(missing, "sha256")
	}
	if len(snapshot.Source.Versions) == 0 {
		missing = append(missing, "versions")
	}
	if snapshot.Source.StringsScanned == 0 {
		missing = append(missing, "strings_scanned")
	}
	if len(missing) == 0 {
		return nil
	}
	return []string{"missing release source metadata: " + strings.Join(missing, ", ")}
}

func snapshotDuplicateProblems(snapshot Snapshot) []string {
	if duplicates := duplicateSnapshotValues(snapshot); len(duplicates) > 0 {
		return []string{"duplicate snapshot values: " + strings.Join(firstN(duplicates, 16), ", ")}
	}
	return nil
}

func duplicateSnapshotValues(snapshot Snapshot) []string {
	signals := snapshot.Signals
	var duplicates []string
	duplicates = append(duplicates, duplicateStrings("source_versions", snapshot.Source.Versions)...)
	duplicates = append(duplicates, duplicateStrings("source_build_stamps", snapshot.Source.BuildStamps)...)
	duplicates = append(duplicates, duplicateStrings("routes", signals.Routes)...)
	duplicates = append(duplicates, duplicateStrings("route_methods", routeMethodStrings(signals.RouteMethods))...)
	duplicates = append(duplicates, duplicateRouteMethodValues(signals.RouteMethods)...)
	duplicates = append(duplicates, duplicateStrings("route_coverage", routeCoverageStrings(signals.RouteCoverage))...)
	duplicates = append(duplicates, duplicateStrings("thread_delta_events", signals.ThreadDeltaEvents)...)
	duplicates = append(duplicates, duplicateStrings("thread_delta_coverage", threadDeltaCoverageStrings(signals.ThreadDeltaCoverage))...)
	duplicates = append(duplicates, duplicateStrings("thread_reader_markers", signals.ThreadReaderMarkers)...)
	duplicates = append(duplicates, duplicateStrings("thread_reader_coverage", threadReaderCoverageStrings(signals.ThreadReaderCoverage))...)
	duplicates = append(duplicates, duplicateStrings("tool_cancel_reasons", signals.ToolCancelReasons)...)
	duplicates = append(duplicates, duplicateStrings("tool_cancel_coverage", toolCancelCoverageStrings(signals.ToolCancelCoverage))...)
	duplicates = append(duplicates, duplicateStrings("tool_run_statuses", signals.ToolRunStatuses)...)
	duplicates = append(duplicates, duplicateStrings("tool_run_coverage", toolRunCoverageStrings(signals.ToolRunCoverage))...)
	duplicates = append(duplicates, duplicateStrings("tool_catalog_markers", signals.ToolCatalog)...)
	duplicates = append(duplicates, duplicateStrings("tool_catalog_coverage", toolCatalogCoverageStrings(signals.ToolCatalogCoverage))...)
	duplicates = append(duplicates, duplicateStrings("stream_json_markers", signals.StreamJSONMarkers)...)
	duplicates = append(duplicates, duplicateStrings("stream_json_coverage", streamJSONCoverageStrings(signals.StreamJSONCoverage))...)
	duplicates = append(duplicates, duplicateStrings("mode_setting_markers", signals.ModeSettingMarkers)...)
	duplicates = append(duplicates, duplicateStrings("mode_setting_coverage", modeSettingCoverageStrings(signals.ModeSettingCoverage))...)
	duplicates = append(duplicates, duplicateStrings("provider_protocol_markers", signals.ProviderProtocol)...)
	duplicates = append(duplicates, duplicateStrings("provider_protocol_coverage", providerCoverageStrings(signals.ProviderCoverage))...)
	duplicates = append(duplicates, duplicateStrings("agent_mode_profiles", agentModeProfileStrings(signals.AgentModeProfiles))...)
	duplicates = append(duplicates, duplicateAgentModeProfileValues(signals.AgentModeProfiles)...)
	duplicates = append(duplicates, duplicateStrings("agent_mode_routes", agentModeRouteStrings(signals.AgentModeRoutes))...)
	duplicates = append(duplicates, duplicateStrings("agent_mode_coverage", agentModeCoverageStrings(signals.AgentModeCoverage))...)
	duplicates = append(duplicates, duplicateStrings("settings", signals.Settings)...)
	duplicates = append(duplicates, duplicateStrings("setting_defaults", settingDefaultStrings(signals.SettingDefaults))...)
	duplicates = append(duplicates, duplicateStrings("setting_coverage", settingCoverageStrings(signals.SettingCoverage))...)
	duplicates = append(duplicates, duplicateStrings("models", signals.Models)...)
	duplicates = append(duplicates, duplicateStrings("model_limits", modelLimitStrings(signals.ModelLimits))...)
	duplicates = append(duplicates, duplicateStrings("large_context_rules", largeContextRuleStrings(signals.LargeContextRules))...)
	duplicates = append(duplicates, duplicateStrings("adaptive_thinking_rules", adaptiveThinkingRuleStrings(signals.AdaptiveThinking))...)
	duplicates = append(duplicates, duplicateAdaptiveThinkingRuleValues(signals.AdaptiveThinking)...)
	duplicates = append(duplicates, duplicateStrings("provider_reasoning_rules", providerReasoningRuleStrings(signals.ProviderReasoning))...)
	duplicates = append(duplicates, duplicateProviderReasoningRuleValues(signals.ProviderReasoning)...)
	duplicates = append(duplicates, duplicateStrings("provider_header_rules", providerHeaderRuleStrings(signals.ProviderHeaders))...)
	duplicates = append(duplicates, duplicateStrings("provider_feature_rules", providerFeatureRuleStrings(signals.ProviderFeatures))...)
	duplicates = append(duplicates, duplicateStrings("compaction_rules", compactionRuleStrings(signals.CompactionRules))...)
	duplicates = append(duplicates, duplicateCompactionRuleValues(signals.CompactionRules)...)
	duplicates = append(duplicates, duplicateStrings("model_coverage", modelCoverageStrings(signals.ModelCoverage))...)
	duplicates = append(duplicates, duplicateStrings("actor_runtime_markers", signals.ActorRuntime)...)
	duplicates = append(duplicates, duplicateStrings("actor_runtime_coverage", actorCoverageStrings(signals.ActorCoverage))...)
	duplicates = append(duplicates, duplicatePromptFingerprints(signals.PromptFingerprints)...)
	duplicates = append(duplicates, duplicatePromptTagCounts(signals.PromptTagCounts)...)
	sort.Strings(duplicates)
	return duplicates
}

func duplicateRouteMethodValues(methods []RouteMethods) []string {
	var duplicates []string
	for _, item := range methods {
		duplicates = append(duplicates, duplicateNestedStrings("route_method_methods", item.Name, item.Methods)...)
	}
	return duplicates
}

func duplicateAgentModeProfileValues(profiles []AgentModeProfile) []string {
	var duplicates []string
	for _, profile := range profiles {
		duplicates = append(duplicates, duplicateNestedStrings("agent_mode_profile_reasoning_levels", profile.Name, profile.ReasoningLevels)...)
	}
	return duplicates
}

func duplicateAdaptiveThinkingRuleValues(rules []AdaptiveThinkingRule) []string {
	var duplicates []string
	for _, rule := range rules {
		owner := strings.Join(rule.ModelEnums, ",")
		if strings.TrimSpace(owner) == "" {
			owner = strings.Join(rule.Models, ",")
		}
		if strings.TrimSpace(owner) == "" {
			owner = rule.DefaultEffort
		}
		duplicates = append(duplicates, duplicateNestedStrings("adaptive_thinking_model_enums", owner, rule.ModelEnums)...)
		duplicates = append(duplicates, duplicateNestedStrings("adaptive_thinking_models", owner, rule.Models)...)
		duplicates = append(duplicates, duplicateNestedStrings("adaptive_thinking_effort_levels", owner, rule.EffortLevels)...)
	}
	return duplicates
}

func duplicateProviderReasoningRuleValues(rules []ProviderReasoningRule) []string {
	var duplicates []string
	for _, rule := range rules {
		duplicates = append(duplicates, duplicateNestedStrings("provider_reasoning_sources", rule.Provider, rule.Sources)...)
	}
	return duplicates
}

func duplicateCompactionRuleValues(rules []CompactionRule) []string {
	var duplicates []string
	for _, rule := range rules {
		duplicates = append(duplicates, duplicateNestedStrings("compaction_usage_fields", rule.Name, rule.UsageFields)...)
	}
	return duplicates
}

func duplicateNestedStrings(category, owner string, values []string) []string {
	owner = strings.TrimSpace(owner)
	if owner == "" {
		owner = "unknown"
	}
	seen := map[string]struct{}{}
	duplicates := map[string]struct{}{}
	for _, value := range values {
		if _, ok := seen[value]; ok {
			duplicates[category+":"+owner+":"+value] = struct{}{}
			continue
		}
		seen[value] = struct{}{}
	}
	return sortedKeys(duplicates)
}

func duplicateStrings(category string, values []string) []string {
	seen := map[string]struct{}{}
	duplicates := map[string]struct{}{}
	for _, value := range values {
		if _, ok := seen[value]; ok {
			duplicates[category+":"+value] = struct{}{}
			continue
		}
		seen[value] = struct{}{}
	}
	return sortedKeys(duplicates)
}

func duplicatePromptFingerprints(fingerprints []PromptFingerprint) []string {
	seen := map[string]struct{}{}
	duplicates := map[string]struct{}{}
	for _, fp := range fingerprints {
		key := strings.TrimSpace(fp.SHA256)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			duplicates["prompt_fingerprints:"+key] = struct{}{}
			continue
		}
		seen[key] = struct{}{}
		duplicates = mergeDuplicateSets(duplicates, duplicateNestedStringSet("prompt_fingerprint_tags", key, fp.Tags))
	}
	return sortedKeys(duplicates)
}

func duplicateNestedStringSet(category, owner string, values []string) map[string]struct{} {
	result := map[string]struct{}{}
	for _, duplicate := range duplicateNestedStrings(category, owner, values) {
		result[duplicate] = struct{}{}
	}
	return result
}

func mergeDuplicateSets(left, right map[string]struct{}) map[string]struct{} {
	for value := range right {
		left[value] = struct{}{}
	}
	return left
}

func duplicatePromptTagCounts(counts []PromptTagCount) []string {
	seen := map[string]struct{}{}
	duplicates := map[string]struct{}{}
	for _, count := range counts {
		key := promptTagCountKey(promptKindOrDefault(count.Kind), count.Tag)
		if _, ok := seen[key]; ok {
			duplicates["prompt_tag_counts:"+key] = struct{}{}
			continue
		}
		seen[key] = struct{}{}
	}
	return sortedKeys(duplicates)
}

func snapshotCompletenessProblems(snapshot Snapshot) []string {
	missing := missingReleaseSignalCategories(snapshot)
	if len(missing) == 0 {
		return nil
	}
	return []string{"missing release signal categories: " + strings.Join(firstN(missing, 16), ", ")}
}

func snapshotRawSignalProblems(snapshot Snapshot) []string {
	var problems []string
	if missing := missingExpectedRawReleaseSignals(snapshot); len(missing) > 0 {
		problems = append(problems, "missing expected raw release signals: "+strings.Join(firstN(missing, 16), ", "))
	}
	if unexpected := unexpectedRawReleaseSignals(snapshot); len(unexpected) > 0 {
		problems = append(problems, "unexpected raw release signals: "+strings.Join(firstN(unexpected, 16), ", "))
	}
	return problems
}

func missingExpectedRawReleaseSignals(snapshot Snapshot) []string {
	if !snapshotLooksLikeReleaseBinary(snapshot) {
		return nil
	}
	signals := snapshot.Signals
	var missing []string
	missing = append(missing, missingStringSet("routes", signals.Routes, knownRouteValues)...)
	missing = append(missing, missingStringSet("route_methods", routeMethodStrings(signals.RouteMethods), knownRouteMethodValues)...)
	missing = append(missing, missingStringSet("thread_delta_events", signals.ThreadDeltaEvents, knownRawThreadDeltaEventValues)...)
	missing = append(missing, missingStringSet("thread_reader_markers", signals.ThreadReaderMarkers, expectedThreadReaderMarkerValues)...)
	missing = append(missing, missingStringSet("tool_cancel_reasons", signals.ToolCancelReasons, knownToolCancelReasonValues)...)
	missing = append(missing, missingStringSet("tool_run_statuses", signals.ToolRunStatuses, knownToolRunStatusValues)...)
	missing = append(missing, missingStringSet("tool_catalog_markers", signals.ToolCatalog, knownToolCatalogMarkerValues)...)
	missing = append(missing, missingStringMapKeys("stream_json_markers", signals.StreamJSONMarkers, streamJSONMarkers)...)
	missing = append(missing, missingStringMapKeys("mode_setting_markers", signals.ModeSettingMarkers, modeSettingMarkers)...)
	missing = append(missing, missingStringMapKeys("provider_protocol_markers", signals.ProviderProtocol, providerProtocolMarkers)...)
	missing = append(missing, missingStringMapKeys("settings", signals.Settings, settingScopes)...)
	missing = append(missing, missingStringSet("models", signals.Models, knownRawModelValues)...)
	missing = append(missing, missingStringSet("actor_runtime_markers", signals.ActorRuntime, knownActorMarkerValues)...)
	sort.Strings(missing)
	return missing
}

func missingStringSet(category string, actual []string, expected map[string]struct{}) []string {
	actualSet := sliceSet(actual)
	var missing []string
	for value := range expected {
		if _, ok := actualSet[value]; !ok {
			missing = append(missing, category+":"+value)
		}
	}
	return missing
}

func missingStringMapKeys(category string, actual []string, expected map[string]string) []string {
	actualSet := sliceSet(actual)
	var missing []string
	for value := range expected {
		if _, ok := actualSet[value]; !ok {
			missing = append(missing, category+":"+value)
		}
	}
	return missing
}

func unexpectedRawReleaseSignals(snapshot Snapshot) []string {
	if !snapshotLooksLikeReleaseBinary(snapshot) {
		return nil
	}
	signals := snapshot.Signals
	var unexpected []string
	for _, route := range signals.Routes {
		if !knownRouteValue(route) || routeScope(route) == "" {
			unexpected = append(unexpected, "routes:"+route)
		}
	}
	for _, event := range signals.ThreadDeltaEvents {
		if !knownThreadDeltaEventName(event) {
			unexpected = append(unexpected, "thread_delta_events:"+event)
		}
	}
	for _, marker := range signals.ThreadReaderMarkers {
		if !knownThreadReaderMarkerValue(marker) || threadReaderMarkers[marker] == "" {
			unexpected = append(unexpected, "thread_reader_markers:"+marker)
		}
	}
	for _, reason := range signals.ToolCancelReasons {
		if !knownToolCancelReasonValue(reason) || toolCancelReasonArea(reason) == "" {
			unexpected = append(unexpected, "tool_cancel_reasons:"+reason)
		}
	}
	for _, status := range signals.ToolRunStatuses {
		if !knownToolRunStatusValue(status) || toolRunStatusAreas[status] == "" {
			unexpected = append(unexpected, "tool_run_statuses:"+status)
		}
	}
	for _, marker := range signals.ToolCatalog {
		if !knownToolCatalogMarkerValue(marker) || toolCatalogMarkers[marker] == "" {
			unexpected = append(unexpected, "tool_catalog_markers:"+marker)
		}
	}
	for _, marker := range signals.StreamJSONMarkers {
		if streamJSONMarkers[marker] == "" {
			unexpected = append(unexpected, "stream_json_markers:"+marker)
		}
	}
	for _, marker := range signals.ModeSettingMarkers {
		if modeSettingMarkers[marker] == "" {
			unexpected = append(unexpected, "mode_setting_markers:"+marker)
		}
	}
	for _, marker := range signals.ProviderProtocol {
		if providerProtocolMarkers[marker] == "" {
			unexpected = append(unexpected, "provider_protocol_markers:"+marker)
		}
	}
	for _, setting := range signals.Settings {
		if settingScopes[setting] == "" {
			unexpected = append(unexpected, "settings:"+setting)
		}
	}
	for _, model := range signals.Models {
		provider, family := modelProviderAndFamily(model)
		if provider == "unknown" || family == "unknown" {
			unexpected = append(unexpected, "models:"+model)
		}
	}
	for _, marker := range signals.ActorRuntime {
		if !knownActorMarkerValue(marker) || actorMarkerAreas[marker] == "" {
			unexpected = append(unexpected, "actor_runtime_markers:"+marker)
		}
	}
	sort.Strings(unexpected)
	return unexpected
}

func missingReleaseSignalCategories(snapshot Snapshot) []string {
	if !snapshotLooksLikeReleaseBinary(snapshot) {
		return nil
	}
	signals := snapshot.Signals
	checks := []struct {
		name    string
		missing bool
	}{
		{name: "routes", missing: len(signals.Routes) == 0},
		{name: "route_methods", missing: len(signals.RouteMethods) == 0},
		{name: "route_coverage", missing: len(signals.RouteCoverage) == 0},
		{name: "thread_delta_events", missing: len(signals.ThreadDeltaEvents) == 0},
		{name: "thread_delta_coverage", missing: len(signals.ThreadDeltaCoverage) == 0},
		{name: "thread_reader_markers", missing: len(signals.ThreadReaderMarkers) == 0},
		{name: "thread_reader_coverage", missing: len(signals.ThreadReaderCoverage) == 0},
		{name: "tool_cancel_reasons", missing: len(signals.ToolCancelReasons) == 0},
		{name: "tool_cancel_coverage", missing: len(signals.ToolCancelCoverage) == 0},
		{name: "tool_run_statuses", missing: len(signals.ToolRunStatuses) == 0},
		{name: "tool_run_coverage", missing: len(signals.ToolRunCoverage) == 0},
		{name: "tool_catalog_markers", missing: len(signals.ToolCatalog) == 0},
		{name: "tool_catalog_coverage", missing: len(signals.ToolCatalogCoverage) == 0},
		{name: "stream_json_markers", missing: len(signals.StreamJSONMarkers) == 0},
		{name: "stream_json_coverage", missing: len(signals.StreamJSONCoverage) == 0},
		{name: "mode_setting_markers", missing: len(signals.ModeSettingMarkers) == 0},
		{name: "mode_setting_coverage", missing: len(signals.ModeSettingCoverage) == 0},
		{name: "provider_protocol_markers", missing: len(signals.ProviderProtocol) == 0},
		{name: "provider_protocol_coverage", missing: len(signals.ProviderCoverage) == 0},
		{name: "agent_mode_profiles", missing: len(signals.AgentModeProfiles) == 0},
		{name: "agent_mode_routes", missing: len(signals.AgentModeRoutes) == 0},
		{name: "agent_mode_coverage", missing: len(signals.AgentModeCoverage) == 0},
		{name: "settings", missing: len(signals.Settings) == 0},
		{name: "setting_defaults", missing: len(signals.SettingDefaults) == 0},
		{name: "setting_coverage", missing: len(signals.SettingCoverage) == 0},
		{name: "models", missing: len(signals.Models) == 0},
		{name: "model_limits", missing: len(signals.ModelLimits) == 0},
		{name: "large_context_rules", missing: len(signals.LargeContextRules) == 0},
		{name: "adaptive_thinking_rules", missing: len(signals.AdaptiveThinking) == 0},
		{name: "provider_reasoning_rules", missing: len(signals.ProviderReasoning) == 0},
		{name: "provider_header_rules", missing: len(signals.ProviderHeaders) == 0},
		{name: "provider_feature_rules", missing: len(signals.ProviderFeatures) == 0},
		{name: "compaction_rules", missing: len(signals.CompactionRules) == 0},
		{name: "model_coverage", missing: len(signals.ModelCoverage) == 0},
		{name: "actor_runtime_markers", missing: len(signals.ActorRuntime) == 0},
		{name: "actor_runtime_coverage", missing: len(signals.ActorCoverage) == 0},
	}
	var missing []string
	for _, check := range checks {
		if check.missing {
			missing = append(missing, check.name)
		}
	}
	return missing
}

func snapshotCoverageProblems(snapshot Snapshot) []string {
	var problems []string
	if unknown := unknownRoutes(snapshot.Signals.RouteCoverage); len(unknown) > 0 {
		problems = append(problems, "unknown routes: "+strings.Join(firstN(unknown, 12), ", "))
	}
	if unknown := unknownThreadDeltas(snapshot.Signals.ThreadDeltaCoverage); len(unknown) > 0 {
		problems = append(problems, "unknown thread deltas: "+strings.Join(firstN(unknown, 12), ", "))
	}
	if unknown := unknownThreadReaders(snapshot.Signals.ThreadReaderCoverage); len(unknown) > 0 {
		problems = append(problems, "unknown thread reader markers: "+strings.Join(firstN(unknown, 12), ", "))
	}
	if unknown := unknownToolCancelReasons(snapshot.Signals.ToolCancelCoverage); len(unknown) > 0 {
		problems = append(problems, "unknown tool cancellation reasons: "+strings.Join(firstN(unknown, 12), ", "))
	}
	if unknown := unknownToolRunStatuses(snapshot.Signals.ToolRunCoverage); len(unknown) > 0 {
		problems = append(problems, "unknown tool run statuses: "+strings.Join(firstN(unknown, 12), ", "))
	}
	if unknown := unknownToolCatalogMarkers(snapshot.Signals.ToolCatalogCoverage); len(unknown) > 0 {
		problems = append(problems, "unknown tool catalog markers: "+strings.Join(firstN(unknown, 12), ", "))
	}
	if unknown := unknownStreamJSONMarkers(snapshot.Signals.StreamJSONCoverage); len(unknown) > 0 {
		problems = append(problems, "unknown stream-json markers: "+strings.Join(firstN(unknown, 12), ", "))
	}
	if unknown := unknownModeSettingMarkers(snapshot.Signals.ModeSettingCoverage); len(unknown) > 0 {
		problems = append(problems, "unknown mode setting markers: "+strings.Join(firstN(unknown, 12), ", "))
	}
	if unknown := unknownProviderProtocols(snapshot.Signals.ProviderCoverage); len(unknown) > 0 {
		problems = append(problems, "unknown provider protocol markers: "+strings.Join(firstN(unknown, 12), ", "))
	}
	if unknown := unknownAgentModes(snapshot.Signals.AgentModeCoverage); len(unknown) > 0 {
		problems = append(problems, "unknown agent modes: "+strings.Join(firstN(unknown, 12), ", "))
	}
	if unknown := unknownSettings(snapshot.Signals.SettingCoverage); len(unknown) > 0 {
		problems = append(problems, "unknown settings: "+strings.Join(firstN(unknown, 12), ", "))
	}
	if unknown := unknownModels(snapshot.Signals.ModelCoverage); len(unknown) > 0 {
		problems = append(problems, "unknown models: "+strings.Join(firstN(unknown, 12), ", "))
	}
	if unknown := unknownActors(snapshot.Signals.ActorCoverage); len(unknown) > 0 {
		problems = append(problems, "unknown actor markers: "+strings.Join(firstN(unknown, 12), ", "))
	}
	return problems
}

func snapshotCoverageMismatchProblems(snapshot Snapshot) []string {
	signals := snapshot.Signals
	var problems []string
	if missing := missingExpectedCoverageValues(snapshot); len(missing) > 0 {
		problems = append(problems, "missing expected coverage values: "+strings.Join(firstN(missing, 16), ", "))
	}
	if unexpected := unexpectedRouteMethods(signals.RouteMethods); len(unexpected) > 0 {
		problems = append(problems, "unexpected route methods: "+strings.Join(firstN(unexpected, 12), ", "))
	}
	if unexpected := unexpectedRouteCoverage(signals.RouteCoverage); len(unexpected) > 0 {
		problems = append(problems, "unexpected route coverage: "+strings.Join(firstN(unexpected, 12), ", "))
	}
	if unexpected := unexpectedThreadDeltaCoverage(signals.ThreadDeltaCoverage); len(unexpected) > 0 {
		problems = append(problems, "unexpected thread delta coverage: "+strings.Join(firstN(unexpected, 12), ", "))
	}
	if unexpected := unexpectedThreadReaderCoverage(signals.ThreadReaderCoverage); len(unexpected) > 0 {
		problems = append(problems, "unexpected thread reader coverage: "+strings.Join(firstN(unexpected, 12), ", "))
	}
	if unexpected := unexpectedToolCancelCoverage(signals.ToolCancelCoverage); len(unexpected) > 0 {
		problems = append(problems, "unexpected tool cancellation coverage: "+strings.Join(firstN(unexpected, 12), ", "))
	}
	if unexpected := unexpectedToolRunCoverage(signals.ToolRunCoverage); len(unexpected) > 0 {
		problems = append(problems, "unexpected tool run coverage: "+strings.Join(firstN(unexpected, 12), ", "))
	}
	if unexpected := unexpectedToolCatalogCoverage(signals.ToolCatalogCoverage); len(unexpected) > 0 {
		problems = append(problems, "unexpected tool catalog coverage: "+strings.Join(firstN(unexpected, 12), ", "))
	}
	if unexpected := unexpectedStreamJSONCoverage(signals.StreamJSONCoverage); len(unexpected) > 0 {
		problems = append(problems, "unexpected stream-json coverage: "+strings.Join(firstN(unexpected, 12), ", "))
	}
	if unexpected := unexpectedModeSettingCoverage(signals.ModeSettingCoverage); len(unexpected) > 0 {
		problems = append(problems, "unexpected mode setting coverage: "+strings.Join(firstN(unexpected, 12), ", "))
	}
	if unexpected := unexpectedProviderCoverage(signals.ProviderCoverage); len(unexpected) > 0 {
		problems = append(problems, "unexpected provider protocol coverage: "+strings.Join(firstN(unexpected, 12), ", "))
	}
	if unexpected := unexpectedAgentModeCoverage(signals.AgentModeCoverage); len(unexpected) > 0 {
		problems = append(problems, "unexpected agent mode coverage: "+strings.Join(firstN(unexpected, 12), ", "))
	}
	if unexpected := unexpectedSettingCoverage(signals.SettingCoverage); len(unexpected) > 0 {
		problems = append(problems, "unexpected setting coverage: "+strings.Join(firstN(unexpected, 12), ", "))
	}
	if unexpected := unexpectedModelCoverage(signals.ModelCoverage); len(unexpected) > 0 {
		problems = append(problems, "unexpected model coverage: "+strings.Join(firstN(unexpected, 12), ", "))
	}
	if unexpected := unexpectedActorCoverage(signals.ActorCoverage); len(unexpected) > 0 {
		problems = append(problems, "unexpected actor coverage: "+strings.Join(firstN(unexpected, 12), ", "))
	}
	return problems
}

func missingExpectedCoverageValues(snapshot Snapshot) []string {
	if !snapshotLooksLikeReleaseBinary(snapshot) {
		return nil
	}
	signals := snapshot.Signals
	var missing []string
	missing = append(missing, missingStringSet("route_coverage", routeCoverageStrings(signals.RouteCoverage), expectedCoverageValuesFromSet(knownRouteValues, routeScope))...)
	missing = append(missing, missingStringSet("thread_delta_coverage", threadDeltaCoverageStrings(signals.ThreadDeltaCoverage), expectedCoverageValuesFromSet(knownRawThreadDeltaEventValues, threadDeltaArea))...)
	missing = append(missing, missingStringSet("thread_reader_coverage", threadReaderCoverageStrings(signals.ThreadReaderCoverage), expectedCoverageValuesFromSet(expectedThreadReaderMarkerValues, func(name string) string {
		return threadReaderMarkers[name]
	}))...)
	missing = append(missing, missingStringSet("tool_cancel_coverage", toolCancelCoverageStrings(signals.ToolCancelCoverage), expectedCoverageValuesFromSet(knownToolCancelReasonValues, toolCancelReasonArea))...)
	missing = append(missing, missingStringSet("tool_run_coverage", toolRunCoverageStrings(signals.ToolRunCoverage), expectedCoverageValuesFromSet(knownToolRunStatusValues, func(name string) string {
		return toolRunStatusAreas[name]
	}))...)
	missing = append(missing, missingStringSet("tool_catalog_coverage", toolCatalogCoverageStrings(signals.ToolCatalogCoverage), expectedCoverageValuesFromSet(knownToolCatalogMarkerValues, func(name string) string {
		return toolCatalogMarkers[name]
	}))...)
	missing = append(missing, missingStringSet("stream_json_coverage", streamJSONCoverageStrings(signals.StreamJSONCoverage), expectedCoverageValuesFromMap(streamJSONMarkers))...)
	missing = append(missing, missingStringSet("mode_setting_coverage", modeSettingCoverageStrings(signals.ModeSettingCoverage), expectedCoverageValuesFromMap(modeSettingMarkers))...)
	missing = append(missing, missingStringSet("provider_protocol_coverage", providerCoverageStrings(signals.ProviderCoverage), expectedCoverageValuesFromMap(providerProtocolMarkers))...)
	missing = append(missing, missingStringSet("agent_mode_coverage", agentModeCoverageStrings(signals.AgentModeCoverage), expectedCoverageValuesFromMap(agentModeScopes))...)
	missing = append(missing, missingStringSet("setting_coverage", settingCoverageStrings(signals.SettingCoverage), expectedCoverageValuesFromMap(settingScopes))...)
	missing = append(missing, missingStringSet("model_coverage", modelCoverageStrings(signals.ModelCoverage), expectedCoverageValuesFromSet(knownRawModelValues, func(name string) string {
		provider, family := modelProviderAndFamily(name)
		return provider + "/" + family
	}))...)
	missing = append(missing, missingStringSet("actor_runtime_coverage", actorCoverageStrings(signals.ActorCoverage), expectedCoverageValuesFromSet(knownActorMarkerValues, func(name string) string {
		return actorMarkerAreas[name]
	}))...)
	sort.Strings(missing)
	return missing
}

func expectedCoverageValuesFromSet(names map[string]struct{}, value func(string) string) map[string]struct{} {
	expected := make(map[string]struct{}, len(names))
	for name := range names {
		coverageValue := value(name)
		if strings.TrimSpace(coverageValue) == "" {
			continue
		}
		expected[name+"="+coverageValue] = struct{}{}
	}
	return expected
}

func expectedCoverageValuesFromMap(values map[string]string) map[string]struct{} {
	expected := make(map[string]struct{}, len(values))
	for name, value := range values {
		if strings.TrimSpace(value) == "" {
			continue
		}
		expected[name+"="+value] = struct{}{}
	}
	return expected
}

func snapshotExactValueProblems(snapshot Snapshot) []string {
	var problems []string
	if missing := missingExpectedExactReleaseValues(snapshot); len(missing) > 0 {
		problems = append(problems, "missing expected exact release values: "+strings.Join(firstN(missing, 16), ", "))
	}
	if unexpected := unexpectedAgentModeProfiles(snapshot.Signals.AgentModeProfiles); len(unexpected) > 0 {
		problems = append(problems, "unexpected agent mode profiles: "+strings.Join(firstN(unexpected, 12), ", "))
	}
	if unexpected := unexpectedAgentModeRoutes(snapshot.Signals.AgentModeRoutes); len(unexpected) > 0 {
		problems = append(problems, "unexpected agent mode routes: "+strings.Join(firstN(unexpected, 12), ", "))
	}
	if unexpected := unexpectedSettingDefaults(snapshot.Signals.SettingDefaults); len(unexpected) > 0 {
		problems = append(problems, "unexpected setting defaults: "+strings.Join(firstN(unexpected, 12), ", "))
	}
	if unexpected := unexpectedModelLimits(snapshot.Signals.ModelLimits); len(unexpected) > 0 {
		problems = append(problems, "unexpected model limits: "+strings.Join(firstN(unexpected, 12), ", "))
	}
	if unexpected := unexpectedLargeContextRules(snapshot.Signals.LargeContextRules); len(unexpected) > 0 {
		problems = append(problems, "unexpected large-context rules: "+strings.Join(firstN(unexpected, 12), ", "))
	}
	if unexpected := unexpectedAdaptiveThinkingRules(snapshot.Signals.AdaptiveThinking); len(unexpected) > 0 {
		problems = append(problems, "unexpected adaptive-thinking rules: "+strings.Join(firstN(unexpected, 12), ", "))
	}
	if unexpected := unexpectedProviderReasoningRules(snapshot.Signals.ProviderReasoning); len(unexpected) > 0 {
		problems = append(problems, "unexpected provider reasoning rules: "+strings.Join(firstN(unexpected, 12), ", "))
	}
	if unexpected := unexpectedProviderHeaderRules(snapshot.Signals.ProviderHeaders); len(unexpected) > 0 {
		problems = append(problems, "unexpected provider header rules: "+strings.Join(firstN(unexpected, 12), ", "))
	}
	if unexpected := unexpectedProviderFeatureRules(snapshot.Signals.ProviderFeatures); len(unexpected) > 0 {
		problems = append(problems, "unexpected provider feature rules: "+strings.Join(firstN(unexpected, 12), ", "))
	}
	if unexpected := unexpectedCompactionRules(snapshot.Signals.CompactionRules); len(unexpected) > 0 {
		problems = append(problems, "unexpected compaction rules: "+strings.Join(firstN(unexpected, 12), ", "))
	}
	return problems
}

func missingExpectedExactReleaseValues(snapshot Snapshot) []string {
	if !snapshotLooksLikeReleaseBinary(snapshot) {
		return nil
	}
	signals := snapshot.Signals
	var missing []string
	missing = append(missing, missingExactValueSet("agent_mode_profiles", agentModeProfileStrings(signals.AgentModeProfiles), knownAgentModeProfileValues, func(value string) string {
		return exactValueName(value, "profile")
	})...)
	missing = append(missing, missingExactValueSet("agent_mode_routes", agentModeRouteStrings(signals.AgentModeRoutes), knownAgentModeRouteValues, func(value string) string {
		return exactValueName(value, "route")
	})...)
	missing = append(missing, missingExpectedSettingDefaults(signals.SettingDefaults)...)
	missing = append(missing, missingExpectedModelLimits(signals.ModelLimits)...)
	missing = append(missing, missingExactValueSet("large_context_rules", largeContextRuleStrings(signals.LargeContextRules), knownLargeContextRuleValues, func(value string) string {
		return exactValueName(value, "large-context")
	})...)
	missing = append(missing, missingExactValueSet("adaptive_thinking_rules", adaptiveThinkingRuleStrings(signals.AdaptiveThinking), knownAdaptiveThinkingRuleValues, func(value string) string {
		return exactValueName(value, "adaptive")
	})...)
	missing = append(missing, missingExactValueSet("provider_reasoning_rules", providerReasoningRuleStrings(signals.ProviderReasoning), knownProviderReasoningRuleValues, func(value string) string {
		return providerRuleDriftName("provider-reasoning-rules", value)
	})...)
	missing = append(missing, missingExactValueSet("provider_header_rules", providerHeaderRuleStrings(signals.ProviderHeaders), knownProviderHeaderRuleValues, func(value string) string {
		return providerRuleDriftName("provider-header-rules", value)
	})...)
	missing = append(missing, missingExactValueSet("provider_feature_rules", providerFeatureRuleStrings(signals.ProviderFeatures), knownProviderFeatureRuleValues, func(value string) string {
		return providerRuleDriftName("provider-feature-rules", value)
	})...)
	missing = append(missing, missingExactValueSet("compaction_rules", compactionRuleStrings(signals.CompactionRules), knownCompactionRuleValues, func(value string) string {
		return exactValueName(value, "compaction")
	})...)
	sort.Strings(missing)
	return missing
}

func missingExactValueSet(category string, actual []string, expected map[string]struct{}, label func(string) string) []string {
	actualSet := sliceSet(actual)
	var missing []string
	for value := range expected {
		if _, ok := actualSet[value]; !ok {
			missing = append(missing, category+":"+label(value))
		}
	}
	return missing
}

func missingExpectedSettingDefaults(defaults []SettingDefault) []string {
	actual := make(map[string]struct{}, len(defaults))
	for _, item := range defaults {
		actual[item.Name] = struct{}{}
	}
	var missing []string
	for name := range knownSettingDefaultValues {
		if _, ok := actual[name]; !ok {
			missing = append(missing, "setting_defaults:"+name)
		}
	}
	return missing
}

func missingExpectedModelLimits(limits []ModelLimit) []string {
	actual := make(map[string]struct{}, len(limits))
	for _, item := range limits {
		actual[item.Name] = struct{}{}
	}
	var missing []string
	for name := range knownModelLimitValues {
		if _, ok := actual[name]; !ok {
			missing = append(missing, "model_limits:"+name)
		}
	}
	return missing
}

func unexpectedAgentModeProfiles(profiles []AgentModeProfile) []string {
	values := agentModeProfileStrings(profiles)
	var unexpected []string
	for _, value := range values {
		if _, ok := knownAgentModeProfileValues[value]; !ok {
			unexpected = append(unexpected, exactValueName(value, "profile"))
		}
	}
	return unexpected
}

func unexpectedAgentModeRoutes(routes []AgentModeRoute) []string {
	values := agentModeRouteStrings(routes)
	var unexpected []string
	for _, value := range values {
		if _, ok := knownAgentModeRouteValues[value]; !ok {
			unexpected = append(unexpected, exactValueName(value, "route"))
		}
	}
	return unexpected
}

func unexpectedSettingDefaults(defaults []SettingDefault) []string {
	var unexpected []string
	for _, item := range defaults {
		expectedScope := settingScopes[item.Name]
		expectedValue, hasExpectedValue := knownSettingDefaultValues[item.Name]
		if expectedScope == "" || item.Scope != expectedScope {
			unexpected = append(unexpected, coverageMismatchString(item.Name, item.Scope, expectedScope))
			continue
		}
		if !hasExpectedValue || item.Value != expectedValue {
			unexpected = append(unexpected, item.Name+"="+item.Value+" want "+expectedValue)
		}
	}
	sort.Strings(unexpected)
	return unexpected
}

func unexpectedModelLimits(limits []ModelLimit) []string {
	var unexpected []string
	for _, item := range limits {
		expected, ok := knownModelLimitValues[item.Name]
		if !ok {
			unexpected = append(unexpected, item.Name+" limit")
			continue
		}
		if item.Enum != expected.Enum ||
			item.Provider != expected.Provider ||
			item.DisplayName != expected.DisplayName ||
			item.ContextWindow != expected.ContextWindow ||
			item.MaxOutputTokens != expected.MaxOutputTokens {
			unexpected = append(unexpected, modelLimitString(item)+" want "+modelLimitExpectationString(item.Name, expected))
		}
	}
	sort.Strings(unexpected)
	return unexpected
}

func modelLimitExpectationString(name string, expected modelLimitExpectation) string {
	return modelLimitString(ModelLimit{
		Name:            name,
		Enum:            expected.Enum,
		Provider:        expected.Provider,
		DisplayName:     expected.DisplayName,
		ContextWindow:   expected.ContextWindow,
		MaxOutputTokens: expected.MaxOutputTokens,
	})
}

func unexpectedLargeContextRules(rules []LargeContextRule) []string {
	values := largeContextRuleStrings(rules)
	var unexpected []string
	for _, value := range values {
		if _, ok := knownLargeContextRuleValues[value]; !ok {
			unexpected = append(unexpected, exactValueName(value, "large-context"))
		}
	}
	return unexpected
}

func unexpectedAdaptiveThinkingRules(rules []AdaptiveThinkingRule) []string {
	values := adaptiveThinkingRuleStrings(rules)
	var unexpected []string
	for _, value := range values {
		if _, ok := knownAdaptiveThinkingRuleValues[value]; !ok {
			unexpected = append(unexpected, exactValueName(value, "adaptive"))
		}
	}
	return unexpected
}

func unexpectedProviderReasoningRules(rules []ProviderReasoningRule) []string {
	values := providerReasoningRuleStrings(rules)
	var unexpected []string
	for _, value := range values {
		if _, ok := knownProviderReasoningRuleValues[value]; !ok {
			unexpected = append(unexpected, providerRuleDriftName("provider-reasoning-rules", value))
		}
	}
	return unexpected
}

func unexpectedProviderHeaderRules(rules []ProviderHeaderRule) []string {
	values := providerHeaderRuleStrings(rules)
	var unexpected []string
	for _, value := range values {
		if _, ok := knownProviderHeaderRuleValues[value]; !ok {
			unexpected = append(unexpected, providerRuleDriftName("provider-header-rules", value))
		}
	}
	return unexpected
}

func unexpectedProviderFeatureRules(rules []ProviderFeatureRule) []string {
	values := providerFeatureRuleStrings(rules)
	var unexpected []string
	for _, value := range values {
		if _, ok := knownProviderFeatureRuleValues[value]; !ok {
			unexpected = append(unexpected, providerRuleDriftName("provider-feature-rules", value))
		}
	}
	return unexpected
}

func unexpectedCompactionRules(rules []CompactionRule) []string {
	values := compactionRuleStrings(rules)
	var unexpected []string
	for _, value := range values {
		if _, ok := knownCompactionRuleValues[value]; !ok {
			unexpected = append(unexpected, exactValueName(value, "compaction"))
		}
	}
	return unexpected
}

func exactValueName(value, suffix string) string {
	name, _, _ := strings.Cut(value, "|")
	name = strings.TrimSpace(name)
	if name == "" {
		return strings.TrimSpace(suffix)
	}
	return name + " " + suffix
}

func unexpectedRouteMethods(methods []RouteMethods) []string {
	var unexpected []string
	for _, item := range methods {
		value := routeMethodString(item)
		if _, ok := knownRouteMethodValues[value]; ok {
			continue
		}
		name, _, scope := routeMethodDiffParts(value)
		expectedScope := routeScope(name)
		if expectedScope == "" || isUnknownCoverageValue(scope) || scope != expectedScope || !knownRouteValue(name) {
			unexpected = append(unexpected, coverageMismatchString(name, scope, expectedScope))
			continue
		}
		expectedValue := knownRouteMethodValueForRoute(name)
		if expectedValue == "" {
			unexpected = append(unexpected, value)
			continue
		}
		unexpected = append(unexpected, value+" want "+expectedValue)
	}
	sort.Strings(unexpected)
	return unexpected
}

func knownRouteMethodValueForRoute(route string) string {
	for value := range knownRouteMethodValues {
		name, _, _ := routeMethodDiffParts(value)
		if name == route {
			return value
		}
	}
	return ""
}

func unexpectedRouteCoverage(coverage []RouteCoverage) []string {
	var unexpected []string
	for _, item := range coverage {
		if isUnknownCoverageValue(item.Scope) {
			continue
		}
		expected := routeScope(item.Name)
		if expected == "" || item.Scope != expected || !knownRouteValue(item.Name) {
			unexpected = append(unexpected, coverageMismatchString(item.Name, item.Scope, expected))
		}
	}
	sort.Strings(unexpected)
	return unexpected
}

func unexpectedThreadDeltaCoverage(coverage []ThreadDeltaCoverage) []string {
	var unexpected []string
	for _, item := range coverage {
		if isUnknownCoverageValue(item.Area) {
			continue
		}
		expected := threadDeltaArea(item.Name)
		if expected == "" || item.Area != expected || !knownThreadDeltaEventName(item.Name) {
			unexpected = append(unexpected, coverageMismatchString(item.Name, item.Area, expected))
		}
	}
	sort.Strings(unexpected)
	return unexpected
}

func unexpectedThreadReaderCoverage(coverage []ThreadReaderCoverage) []string {
	var unexpected []string
	for _, item := range coverage {
		if isUnknownCoverageValue(item.Area) {
			continue
		}
		expected := threadReaderMarkers[item.Name]
		if expected == "" || item.Area != expected || !knownThreadReaderMarkerValue(item.Name) {
			unexpected = append(unexpected, coverageMismatchString(item.Name, item.Area, expected))
		}
	}
	sort.Strings(unexpected)
	return unexpected
}

func unexpectedToolCancelCoverage(coverage []ToolCancelCoverage) []string {
	var unexpected []string
	for _, item := range coverage {
		if isUnknownCoverageValue(item.Area) {
			continue
		}
		expected := toolCancelReasonArea(item.Name)
		if expected == "" || item.Area != expected || !knownToolCancelReasonValue(item.Name) {
			unexpected = append(unexpected, coverageMismatchString(item.Name, item.Area, expected))
		}
	}
	sort.Strings(unexpected)
	return unexpected
}

func unexpectedToolRunCoverage(coverage []ToolRunCoverage) []string {
	var unexpected []string
	for _, item := range coverage {
		if isUnknownCoverageValue(item.Area) {
			continue
		}
		expected := toolRunStatusAreas[item.Name]
		if expected == "" || item.Area != expected || !knownToolRunStatusValue(item.Name) {
			unexpected = append(unexpected, coverageMismatchString(item.Name, item.Area, expected))
		}
	}
	sort.Strings(unexpected)
	return unexpected
}

func unexpectedToolCatalogCoverage(coverage []ToolCatalogCoverage) []string {
	var unexpected []string
	for _, item := range coverage {
		if isUnknownCoverageValue(item.Area) {
			continue
		}
		expected := toolCatalogMarkers[item.Name]
		if expected == "" || item.Area != expected || !knownToolCatalogMarkerValue(item.Name) {
			unexpected = append(unexpected, coverageMismatchString(item.Name, item.Area, expected))
		}
	}
	sort.Strings(unexpected)
	return unexpected
}

func unexpectedStreamJSONCoverage(coverage []StreamJSONCoverage) []string {
	var unexpected []string
	for _, item := range coverage {
		if isUnknownCoverageValue(item.Area) {
			continue
		}
		expected := streamJSONMarkers[item.Name]
		if expected == "" || item.Area != expected {
			unexpected = append(unexpected, coverageMismatchString(item.Name, item.Area, expected))
		}
	}
	sort.Strings(unexpected)
	return unexpected
}

func unexpectedModeSettingCoverage(coverage []ModeSettingCoverage) []string {
	var unexpected []string
	for _, item := range coverage {
		if isUnknownCoverageValue(item.Area) {
			continue
		}
		expected := modeSettingMarkers[item.Name]
		if expected == "" || item.Area != expected {
			unexpected = append(unexpected, coverageMismatchString(item.Name, item.Area, expected))
		}
	}
	sort.Strings(unexpected)
	return unexpected
}

func unexpectedProviderCoverage(coverage []ProviderCoverage) []string {
	var unexpected []string
	for _, item := range coverage {
		if isUnknownCoverageValue(item.Area) {
			continue
		}
		expected := providerProtocolMarkers[item.Name]
		if expected == "" || item.Area != expected {
			unexpected = append(unexpected, coverageMismatchString(item.Name, item.Area, expected))
		}
	}
	sort.Strings(unexpected)
	return unexpected
}

func unexpectedAgentModeCoverage(coverage []AgentModeCoverage) []string {
	var unexpected []string
	for _, item := range coverage {
		if isUnknownCoverageValue(item.Scope) {
			continue
		}
		expected := agentModeScopes[item.Name]
		if expected == "" || item.Scope != expected {
			unexpected = append(unexpected, coverageMismatchString(item.Name, item.Scope, expected))
		}
	}
	sort.Strings(unexpected)
	return unexpected
}

func unexpectedSettingCoverage(coverage []SettingCoverage) []string {
	var unexpected []string
	for _, item := range coverage {
		if isUnknownCoverageValue(item.Scope) {
			continue
		}
		expected := settingScopes[item.Name]
		if expected == "" || item.Scope != expected {
			unexpected = append(unexpected, coverageMismatchString(item.Name, item.Scope, expected))
		}
	}
	sort.Strings(unexpected)
	return unexpected
}

func unexpectedModelCoverage(coverage []ModelCoverage) []string {
	var unexpected []string
	for _, item := range coverage {
		if isUnknownCoverageValue(item.Provider) || isUnknownCoverageValue(item.Family) {
			continue
		}
		expectedProvider, expectedFamily := modelProviderAndFamily(item.Name)
		if expectedProvider == "unknown" || expectedFamily == "unknown" || item.Provider != expectedProvider || item.Family != expectedFamily {
			unexpected = append(unexpected, coverageMismatchString(item.Name, item.Provider+"/"+item.Family, expectedProvider+"/"+expectedFamily))
		}
	}
	sort.Strings(unexpected)
	return unexpected
}

func unexpectedActorCoverage(coverage []ActorCoverage) []string {
	var unexpected []string
	for _, item := range coverage {
		if isUnknownCoverageValue(item.Area) {
			continue
		}
		expected := actorMarkerAreas[item.Name]
		if expected == "" || item.Area != expected || !knownActorMarkerValue(item.Name) {
			unexpected = append(unexpected, coverageMismatchString(item.Name, item.Area, expected))
		}
	}
	sort.Strings(unexpected)
	return unexpected
}

func isUnknownCoverageValue(value string) bool {
	value = strings.TrimSpace(value)
	return value == "" || value == "unknown"
}

func coverageMismatchString(name, actual, expected string) string {
	if strings.TrimSpace(expected) == "" || strings.Contains(expected, "unknown") {
		return name + "=" + actual
	}
	return name + "=" + actual + " want " + expected
}

func snapshotPromptTagProblems(snapshot Snapshot) []string {
	var promptTags, sourceTags []string
	unknownKindSet := map[string]struct{}{}
	unexpectedKindCountSet := map[string]struct{}{}
	unexpectedCountSet := map[string]struct{}{}
	kindCounts := map[string]int{}
	for _, fp := range snapshot.Signals.PromptFingerprints {
		kind := promptKindOrDefault(fp.Kind)
		if !knownPromptKind(kind) {
			unknownKindSet[kind] = struct{}{}
		}
		kindCounts[kind]++
	}
	for kind, count := range kindCounts {
		if !knownPromptKind(kind) {
			continue
		}
		if expected, ok := knownPromptKindCountValues[kind]; !ok || count != expected {
			unexpectedKindCountSet[fmt.Sprintf("%s=%d", kind, count)] = struct{}{}
		}
	}
	for _, count := range snapshot.Signals.PromptTagCounts {
		kind := promptKindOrDefault(count.Kind)
		switch kind {
		case "prompt":
			promptTags = append(promptTags, count.Tag)
		case "source":
			sourceTags = append(sourceTags, count.Tag)
		default:
			unknownKindSet[kind] = struct{}{}
		}
		if knownPromptTag(kind, count.Tag) {
			key := promptTagCountKey(kind, count.Tag)
			if expected, ok := knownPromptTagCountValues[key]; !ok || count.Count != expected {
				unexpectedCountSet[fmt.Sprintf("%s=%d", key, count.Count)] = struct{}{}
			}
		}
	}
	if snapshotLooksLikeReleaseBinary(snapshot) {
		if len(snapshot.Signals.PromptFingerprints) == 0 {
			unexpectedKindCountSet["missing"] = struct{}{}
		}
		if len(snapshot.Signals.PromptTagCounts) == 0 {
			unexpectedCountSet["missing"] = struct{}{}
		}
	}
	var problems []string
	if unknown := sortedKeys(unknownKindSet); len(unknown) > 0 {
		problems = append(problems, "unknown prompt kinds: "+strings.Join(firstN(unknown, 12), ", "))
	}
	if unknown := unknownPromptReviewTags(promptTags); len(unknown) > 0 {
		problems = append(problems, "unknown prompt tags: "+strings.Join(firstN(unknown, 12), ", "))
	}
	if unknown := unknownSourceReviewTags(sourceTags); len(unknown) > 0 {
		problems = append(problems, "unknown source tags: "+strings.Join(firstN(unknown, 12), ", "))
	}
	if unexpected := sortedKeys(unexpectedKindCountSet); len(unexpected) > 0 {
		problems = append(problems, "unexpected prompt/source kind counts: "+strings.Join(firstN(unexpected, 12), ", "))
	}
	if unexpected := sortedKeys(unexpectedCountSet); len(unexpected) > 0 {
		problems = append(problems, "unexpected prompt/source tag counts: "+strings.Join(firstN(unexpected, 12), ", "))
	}
	return problems
}

func snapshotLooksLikeReleaseBinary(snapshot Snapshot) bool {
	return snapshot.Source.SizeBytes >= minReleaseBinarySizeBytes
}

func printReport(old, current Snapshot, diff auditDiff) {
	auditPrintln("Amp binary parity audit")
	auditPrintf("current:  %s sha=%s versions=%s\n", current.Source.Path, shortHash(current.Source.SHA256), strings.Join(current.Source.Versions, ","))
	auditPrintf("baseline: %s sha=%s versions=%s\n", old.Source.Path, shortHash(old.Source.SHA256), strings.Join(old.Source.Versions, ","))
	printSnapshotCounts(current)
	for _, problem := range prefixedSnapshotAuditProblems("baseline", old) {
		auditPrintln(problem)
	}
	for _, problem := range prefixedSnapshotAuditProblems("current", current) {
		auditPrintln(problem)
	}
	if !hasDiff(diff) {
		auditPrintln("signals: no changes from baseline")
		return
	}
	auditPrintln("signals: changes detected")
	for _, category := range diff.Categories {
		printCategoryDiff(category)
	}
	printPromptDiff(diff.Prompts)
	printSuggestions(diff)
	printLifecycleChecklist(lifecycleChecklist(current, diff, false))
}

func printSnapshotSummary(snapshot Snapshot) {
	auditPrintf("binary: %s sha=%s versions=%s\n", snapshot.Source.Path, shortHash(snapshot.Source.SHA256), strings.Join(snapshot.Source.Versions, ","))
	printSnapshotCounts(snapshot)
}

func printSnapshotCounts(snapshot Snapshot) {
	routeScopeCounts := routeScopeCounts(snapshot.Signals.RouteCoverage)
	threadDeltaAreaCounts := threadDeltaAreaCounts(snapshot.Signals.ThreadDeltaCoverage)
	threadReaderAreaCounts := threadReaderAreaCounts(snapshot.Signals.ThreadReaderCoverage)
	toolCancelAreaCounts := toolCancelAreaCounts(snapshot.Signals.ToolCancelCoverage)
	toolRunAreaCounts := toolRunAreaCounts(snapshot.Signals.ToolRunCoverage)
	toolCatalogAreaCounts := toolCatalogAreaCounts(snapshot.Signals.ToolCatalogCoverage)
	streamJSONAreaCounts := streamJSONAreaCounts(snapshot.Signals.StreamJSONCoverage)
	modeSettingAreaCounts := modeSettingAreaCounts(snapshot.Signals.ModeSettingCoverage)
	providerAreaCounts := providerAreaCounts(snapshot.Signals.ProviderCoverage)
	agentModeScopeCounts := agentModeScopeCounts(snapshot.Signals.AgentModeCoverage)
	scopeCounts := settingScopeCounts(snapshot.Signals.SettingCoverage)
	modelProviderCounts := modelProviderCounts(snapshot.Signals.ModelCoverage)
	modelLimitProviderCounts := modelLimitProviderCounts(snapshot.Signals.ModelLimits)
	actorAreaCounts := actorAreaCounts(snapshot.Signals.ActorCoverage)
	auditPrintf("counts: routes=%d route_methods=%d events=%d thread_reader_markers=%d tool_cancel_reasons=%d tool_run_statuses=%d tool_catalog_markers=%d stream_json_markers=%d mode_setting_markers=%d provider_protocol_markers=%d agent_modes=%d agent_mode_routes=%d settings=%d setting_defaults=%d models=%d model_limits=%d large_context_rules=%d adaptive_thinking_rules=%d provider_reasoning_rules=%d provider_header_rules=%d provider_feature_rules=%d compaction_rules=%d actor_markers=%d prompt_fingerprints=%d strings=%d\n",
		len(snapshot.Signals.Routes),
		len(snapshot.Signals.RouteMethods),
		len(snapshot.Signals.ThreadDeltaEvents),
		len(snapshot.Signals.ThreadReaderMarkers),
		len(snapshot.Signals.ToolCancelReasons),
		len(snapshot.Signals.ToolRunStatuses),
		len(snapshot.Signals.ToolCatalog),
		len(snapshot.Signals.StreamJSONMarkers),
		len(snapshot.Signals.ModeSettingMarkers),
		len(snapshot.Signals.ProviderProtocol),
		len(snapshot.Signals.AgentModeProfiles),
		len(snapshot.Signals.AgentModeRoutes),
		len(snapshot.Signals.Settings),
		len(snapshot.Signals.SettingDefaults),
		len(snapshot.Signals.Models),
		len(snapshot.Signals.ModelLimits),
		len(snapshot.Signals.LargeContextRules),
		len(snapshot.Signals.AdaptiveThinking),
		len(snapshot.Signals.ProviderReasoning),
		len(snapshot.Signals.ProviderHeaders),
		len(snapshot.Signals.ProviderFeatures),
		len(snapshot.Signals.CompactionRules),
		len(snapshot.Signals.ActorRuntime),
		len(snapshot.Signals.PromptFingerprints),
		snapshot.Source.StringsScanned,
	)
	if len(routeScopeCounts) > 0 {
		auditPrintf("route scopes: local-runtime=%d remote-web=%d amp-owned=%d unknown=%d\n",
			routeScopeCounts["local-runtime"],
			routeScopeCounts["remote-web"],
			routeScopeCounts["amp-owned"],
			routeScopeCounts["unknown"],
		)
	}
	if len(threadDeltaAreaCounts) > 0 {
		auditPrintf("thread delta areas: message=%d user-message=%d assistant-message=%d queue=%d tool-input=%d tool-result=%d tool-state=%d history=%d settings=%d environment=%d metadata=%d relationship=%d status=%d execution-state=%d compaction=%d retry=%d error=%d client-command=%d executor-bridge=%d observer=%d plugin=%d unknown=%d\n",
			threadDeltaAreaCounts["message"],
			threadDeltaAreaCounts["user-message"],
			threadDeltaAreaCounts["assistant-message"],
			threadDeltaAreaCounts["queue"],
			threadDeltaAreaCounts["tool-input"],
			threadDeltaAreaCounts["tool-result"],
			threadDeltaAreaCounts["tool-state"],
			threadDeltaAreaCounts["history"],
			threadDeltaAreaCounts["settings"],
			threadDeltaAreaCounts["environment"],
			threadDeltaAreaCounts["metadata"],
			threadDeltaAreaCounts["relationship"],
			threadDeltaAreaCounts["status"],
			threadDeltaAreaCounts["execution-state"],
			threadDeltaAreaCounts["compaction"],
			threadDeltaAreaCounts["retry"],
			threadDeltaAreaCounts["error"],
			threadDeltaAreaCounts["client-command"],
			threadDeltaAreaCounts["executor-bridge"],
			threadDeltaAreaCounts["observer"],
			threadDeltaAreaCounts["plugin"],
			threadDeltaAreaCounts["unknown"],
		)
	}
	if len(threadReaderAreaCounts) > 0 {
		auditPrintf("thread reader areas: internal-rpc=%d message-reader-route=%d unknown=%d\n",
			threadReaderAreaCounts["internal-rpc"],
			threadReaderAreaCounts["message-reader-route"],
			threadReaderAreaCounts["unknown"],
		)
	}
	if len(toolCancelAreaCounts) > 0 {
		auditPrintf("tool cancel areas: user-cancel=%d user-interrupt=%d system-safety=%d history-edit=%d restore-cleanup=%d runtime-dispose=%d unknown=%d\n",
			toolCancelAreaCounts["user-cancel"],
			toolCancelAreaCounts["user-interrupt"],
			toolCancelAreaCounts["system-safety"],
			toolCancelAreaCounts["history-edit"],
			toolCancelAreaCounts["restore-cleanup"],
			toolCancelAreaCounts["runtime-dispose"],
			toolCancelAreaCounts["unknown"],
		)
	}
	if len(toolRunAreaCounts) > 0 {
		auditPrintf("tool run areas: terminal-success=%d terminal-error=%d terminal-cancel=%d running=%d pending=%d unknown=%d\n",
			toolRunAreaCounts["terminal-success"],
			toolRunAreaCounts["terminal-error"],
			toolRunAreaCounts["terminal-cancel"],
			toolRunAreaCounts["running"],
			toolRunAreaCounts["pending"],
			toolRunAreaCounts["unknown"],
		)
	}
	if len(toolCatalogAreaCounts) > 0 {
		auditPrintf("tool catalog areas: file-read=%d file-edit=%d file-search=%d legacy-file-read=%d legacy-file-search=%d browser=%d media=%d mcp=%d skills=%d toolbox=%d tool-filter=%d tool-spec-overrides=%d unknown=%d\n",
			toolCatalogAreaCounts["file-read"],
			toolCatalogAreaCounts["file-edit"],
			toolCatalogAreaCounts["file-search"],
			toolCatalogAreaCounts["legacy-file-read"],
			toolCatalogAreaCounts["legacy-file-search"],
			toolCatalogAreaCounts["browser"],
			toolCatalogAreaCounts["media"],
			toolCatalogAreaCounts["mcp"],
			toolCatalogAreaCounts["skills"],
			toolCatalogAreaCounts["toolbox"],
			toolCatalogAreaCounts["tool-filter"],
			toolCatalogAreaCounts["tool-spec-overrides"],
			toolCatalogAreaCounts["unknown"],
		)
	}
	if len(streamJSONAreaCounts) > 0 {
		auditPrintf("stream-json areas: cli-flag=%d execute-mode=%d init-field=%d result-field=%d shared-field=%d error-subtype=%d unknown=%d\n",
			streamJSONAreaCounts["cli-flag"],
			streamJSONAreaCounts["execute-mode"],
			streamJSONAreaCounts["init-field"],
			streamJSONAreaCounts["result-field"],
			streamJSONAreaCounts["shared-field"],
			streamJSONAreaCounts["error-subtype"],
			streamJSONAreaCounts["unknown"],
		)
	}
	if len(modeSettingAreaCounts) > 0 {
		auditPrintf("mode setting areas: thread-setting=%d thread-metadata=%d provider-speed=%d provider-thinking=%d draft-settings=%d session-default=%d unknown=%d\n",
			modeSettingAreaCounts["thread-setting"],
			modeSettingAreaCounts["thread-metadata"],
			modeSettingAreaCounts["provider-speed"],
			modeSettingAreaCounts["provider-thinking"],
			modeSettingAreaCounts["draft-settings"],
			modeSettingAreaCounts["session-default"],
			modeSettingAreaCounts["unknown"],
		)
	}
	if len(providerAreaCounts) > 0 {
		auditPrintf("provider protocol areas: anthropic-header=%d anthropic-version-value=%d anthropic-beta=%d amp-provider-header=%d amp-client-header=%d amp-feature=%d openai-protocol=%d google-upload=%d unknown=%d\n",
			providerAreaCounts["anthropic-header"],
			providerAreaCounts["anthropic-version-value"],
			providerAreaCounts["anthropic-beta"],
			providerAreaCounts["amp-provider-header"],
			providerAreaCounts["amp-client-header"],
			providerAreaCounts["amp-feature"],
			providerAreaCounts["openai-protocol"],
			providerAreaCounts["google-upload"],
			providerAreaCounts["unknown"],
		)
	}
	if len(agentModeScopeCounts) > 0 {
		auditPrintf("agent mode scopes: local-runtime=%d server-only=%d unknown=%d\n",
			agentModeScopeCounts["local-runtime"],
			agentModeScopeCounts["server-only"],
			agentModeScopeCounts["unknown"],
		)
	}
	if len(scopeCounts) > 0 {
		auditPrintf("setting scopes: local-runtime=%d remote-web=%d amp-owned=%d unknown=%d\n",
			scopeCounts["local-runtime"],
			scopeCounts["remote-web"],
			scopeCounts["amp-owned"],
			scopeCounts["unknown"],
		)
	}
	if len(modelProviderCounts) > 0 {
		auditPrintf("model providers: anthropic=%d google=%d openai=%d unknown=%d\n",
			modelProviderCounts["anthropic"],
			modelProviderCounts["google"],
			modelProviderCounts["openai"],
			modelProviderCounts["unknown"],
		)
	}
	if len(modelLimitProviderCounts) > 0 {
		auditPrintf("model limit providers: anthropic=%d baseten=%d cerebras=%d fireworks=%d google=%d moonshotai=%d openai=%d openrouter=%d xai=%d unknown=%d\n",
			modelLimitProviderCounts["anthropic"],
			modelLimitProviderCounts["baseten"],
			modelLimitProviderCounts["cerebras"],
			modelLimitProviderCounts["fireworks"],
			modelLimitProviderCounts["google"],
			modelLimitProviderCounts["moonshotai"],
			modelLimitProviderCounts["openai"],
			modelLimitProviderCounts["openrouter"],
			modelLimitProviderCounts["xai"],
			modelLimitProviderCounts["unknown"],
		)
	}
	if len(actorAreaCounts) > 0 {
		auditPrintf("actor areas: actor-protocol=%d local-runtime=%d client-metadata=%d amp-owned=%d unknown=%d\n",
			actorAreaCounts["actor-protocol"],
			actorAreaCounts["local-runtime"],
			actorAreaCounts["client-metadata"],
			actorAreaCounts["amp-owned"],
			actorAreaCounts["unknown"],
		)
	}
	if len(snapshot.Signals.PromptFingerprints) > 0 {
		auditPrintf("prompt kinds: %s\n", strings.Join(promptKindCountStrings(snapshot.Signals.PromptFingerprints), " "))
	}
	if len(snapshot.Signals.PromptTagCounts) > 0 {
		auditPrintf("prompt tags: %s\n", strings.Join(promptTagCountStrings(snapshot.Signals.PromptTagCounts), " "))
	}
}

func routeScopeCounts(coverage []RouteCoverage) map[string]int {
	counts := map[string]int{}
	for _, item := range coverage {
		scope := item.Scope
		if scope == "" {
			scope = "unknown"
		}
		counts[scope]++
	}
	return counts
}

func threadDeltaAreaCounts(coverage []ThreadDeltaCoverage) map[string]int {
	counts := map[string]int{}
	for _, item := range coverage {
		area := item.Area
		if area == "" {
			area = "unknown"
		}
		counts[area]++
	}
	return counts
}

func threadReaderAreaCounts(coverage []ThreadReaderCoverage) map[string]int {
	counts := map[string]int{}
	for _, item := range coverage {
		area := item.Area
		if area == "" {
			area = "unknown"
		}
		counts[area]++
	}
	return counts
}

func toolCancelAreaCounts(coverage []ToolCancelCoverage) map[string]int {
	counts := map[string]int{}
	for _, item := range coverage {
		area := item.Area
		if area == "" {
			area = "unknown"
		}
		counts[area]++
	}
	return counts
}

func toolRunAreaCounts(coverage []ToolRunCoverage) map[string]int {
	counts := map[string]int{}
	for _, item := range coverage {
		area := item.Area
		if area == "" {
			area = "unknown"
		}
		counts[area]++
	}
	return counts
}

func toolCatalogAreaCounts(coverage []ToolCatalogCoverage) map[string]int {
	counts := map[string]int{}
	for _, item := range coverage {
		area := item.Area
		if area == "" {
			area = "unknown"
		}
		counts[area]++
	}
	return counts
}

func streamJSONAreaCounts(coverage []StreamJSONCoverage) map[string]int {
	counts := map[string]int{}
	for _, item := range coverage {
		area := item.Area
		if area == "" {
			area = "unknown"
		}
		counts[area]++
	}
	return counts
}

func modeSettingAreaCounts(coverage []ModeSettingCoverage) map[string]int {
	counts := map[string]int{}
	for _, item := range coverage {
		area := item.Area
		if area == "" {
			area = "unknown"
		}
		counts[area]++
	}
	return counts
}

func providerAreaCounts(coverage []ProviderCoverage) map[string]int {
	counts := map[string]int{}
	for _, item := range coverage {
		area := item.Area
		if area == "" {
			area = "unknown"
		}
		counts[area]++
	}
	return counts
}

func agentModeScopeCounts(coverage []AgentModeCoverage) map[string]int {
	counts := map[string]int{}
	for _, item := range coverage {
		scope := item.Scope
		if scope == "" {
			scope = "unknown"
		}
		counts[scope]++
	}
	return counts
}

func settingScopeCounts(coverage []SettingCoverage) map[string]int {
	counts := map[string]int{}
	for _, item := range coverage {
		scope := item.Scope
		if scope == "" {
			scope = "unknown"
		}
		counts[scope]++
	}
	return counts
}

func modelProviderCounts(coverage []ModelCoverage) map[string]int {
	counts := map[string]int{}
	for _, item := range coverage {
		provider := item.Provider
		if provider == "" {
			provider = "unknown"
		}
		counts[provider]++
	}
	return counts
}

func modelLimitProviderCounts(limits []ModelLimit) map[string]int {
	counts := map[string]int{}
	for _, item := range limits {
		provider := item.Provider
		if provider == "" {
			provider = "unknown"
		}
		counts[provider]++
	}
	return counts
}

func actorAreaCounts(coverage []ActorCoverage) map[string]int {
	counts := map[string]int{}
	for _, item := range coverage {
		area := item.Area
		if area == "" {
			area = "unknown"
		}
		counts[area]++
	}
	return counts
}

func unknownRoutes(coverage []RouteCoverage) []string {
	var unknown []string
	for _, item := range coverage {
		if item.Scope == "unknown" || item.Scope == "" {
			unknown = append(unknown, item.Name)
		}
	}
	sort.Strings(unknown)
	return unknown
}

func unknownThreadDeltas(coverage []ThreadDeltaCoverage) []string {
	var unknown []string
	for _, item := range coverage {
		if item.Area == "unknown" || item.Area == "" {
			unknown = append(unknown, item.Name)
		}
	}
	sort.Strings(unknown)
	return unknown
}

func unknownThreadReaders(coverage []ThreadReaderCoverage) []string {
	var unknown []string
	for _, item := range coverage {
		if item.Area == "unknown" || item.Area == "" {
			unknown = append(unknown, item.Name)
		}
	}
	sort.Strings(unknown)
	return unknown
}

func unknownToolCancelReasons(coverage []ToolCancelCoverage) []string {
	var unknown []string
	for _, item := range coverage {
		if item.Area == "unknown" || item.Area == "" {
			unknown = append(unknown, item.Name)
		}
	}
	sort.Strings(unknown)
	return unknown
}

func unknownToolRunStatuses(coverage []ToolRunCoverage) []string {
	var unknown []string
	for _, item := range coverage {
		if item.Area == "unknown" || item.Area == "" {
			unknown = append(unknown, item.Name)
		}
	}
	sort.Strings(unknown)
	return unknown
}

func unknownToolCatalogMarkers(coverage []ToolCatalogCoverage) []string {
	var unknown []string
	for _, item := range coverage {
		if item.Area == "unknown" || item.Area == "" {
			unknown = append(unknown, item.Name)
		}
	}
	sort.Strings(unknown)
	return unknown
}

func unknownStreamJSONMarkers(coverage []StreamJSONCoverage) []string {
	var unknown []string
	for _, item := range coverage {
		if item.Area == "unknown" || item.Area == "" {
			unknown = append(unknown, item.Name)
		}
	}
	sort.Strings(unknown)
	return unknown
}

func unknownModeSettingMarkers(coverage []ModeSettingCoverage) []string {
	var unknown []string
	for _, item := range coverage {
		if item.Area == "unknown" || item.Area == "" {
			unknown = append(unknown, item.Name)
		}
	}
	sort.Strings(unknown)
	return unknown
}

func unknownProviderProtocols(coverage []ProviderCoverage) []string {
	var unknown []string
	for _, item := range coverage {
		if item.Area == "unknown" || item.Area == "" {
			unknown = append(unknown, item.Name)
		}
	}
	sort.Strings(unknown)
	return unknown
}

func unknownAgentModes(coverage []AgentModeCoverage) []string {
	var unknown []string
	for _, item := range coverage {
		if item.Scope == "unknown" || item.Scope == "" {
			unknown = append(unknown, item.Name)
		}
	}
	sort.Strings(unknown)
	return unknown
}

func unknownModels(coverage []ModelCoverage) []string {
	var unknown []string
	for _, item := range coverage {
		if item.Provider == "unknown" || item.Provider == "" || item.Family == "unknown" || item.Family == "" {
			unknown = append(unknown, item.Name)
		}
	}
	sort.Strings(unknown)
	return unknown
}

func unknownActors(coverage []ActorCoverage) []string {
	var unknown []string
	for _, item := range coverage {
		if item.Area == "unknown" || item.Area == "" {
			unknown = append(unknown, item.Name)
		}
	}
	sort.Strings(unknown)
	return unknown
}

func unknownSettings(coverage []SettingCoverage) []string {
	var unknown []string
	for _, item := range coverage {
		if item.Scope == "unknown" || item.Scope == "" {
			unknown = append(unknown, item.Name)
		}
	}
	sort.Strings(unknown)
	return unknown
}

func printCategoryDiff(diff categoryDiff) {
	if len(diff.Added) == 0 && len(diff.Removed) == 0 {
		return
	}
	auditPrintf("\n%s:\n", diff.Name)
	for _, value := range firstN(diff.Added, 40) {
		auditPrintf("  + %s\n", value)
	}
	if len(diff.Added) > 40 {
		auditPrintf("  + ... %d more\n", len(diff.Added)-40)
	}
	for _, value := range firstN(diff.Removed, 40) {
		auditPrintf("  - %s\n", value)
	}
	if len(diff.Removed) > 40 {
		auditPrintf("  - ... %d more\n", len(diff.Removed)-40)
	}
}

func printPromptDiff(diff promptDiff) {
	if len(diff.Added) == 0 && len(diff.Removed) == 0 {
		return
	}
	auditPrintln("\nprompt-fingerprints:")
	for _, fp := range firstPromptN(diff.Added, 30) {
		auditPrintf("  + %s len=%d kind=%s tags=%s\n", shortHash(fp.SHA256), fp.Length, promptKindOrDefault(fp.Kind), strings.Join(fp.Tags, ","))
	}
	if len(diff.Added) > 30 {
		auditPrintf("  + ... %d more\n", len(diff.Added)-30)
	}
	for _, fp := range firstPromptN(diff.Removed, 30) {
		auditPrintf("  - %s len=%d kind=%s tags=%s\n", shortHash(fp.SHA256), fp.Length, promptKindOrDefault(fp.Kind), strings.Join(fp.Tags, ","))
	}
	if len(diff.Removed) > 30 {
		auditPrintf("  - ... %d more\n", len(diff.Removed)-30)
	}
}

func printPromptDiffExcerpts(diff promptDiff, catalog map[string]promptExcerpt) {
	writePromptDiffExcerpts(auditStdout, diff, catalog)
}

func writePromptDiffExcerpts(w io.Writer, diff promptDiff, catalog map[string]promptExcerpt) {
	if len(diff.Added) == 0 && len(diff.Removed) == 0 {
		return
	}
	fmt.Fprintln(w, "\nprompt/source excerpts:")
	for _, fp := range firstPromptN(diff.Added, 20) {
		fmt.Fprintf(w, "  + %s len=%d kind=%s tags=%s\n", shortHash(fp.SHA256), fp.Length, promptKindOrDefault(fp.Kind), strings.Join(fp.Tags, ","))
		if excerpt, ok := catalog[fp.SHA256]; ok && strings.TrimSpace(excerpt.Excerpt) != "" {
			fmt.Fprintf(w, "    %s\n", indentPromptExcerpt(excerpt.Excerpt))
		} else {
			fmt.Fprintln(w, "    excerpt unavailable in current binary")
		}
	}
	if len(diff.Added) > 20 {
		fmt.Fprintf(w, "  + ... %d more\n", len(diff.Added)-20)
	}
	for _, fp := range firstPromptN(diff.Removed, 20) {
		fmt.Fprintf(w, "  - %s len=%d kind=%s tags=%s\n", shortHash(fp.SHA256), fp.Length, promptKindOrDefault(fp.Kind), strings.Join(fp.Tags, ","))
		if excerpt, ok := catalog[fp.SHA256]; ok && strings.TrimSpace(excerpt.Excerpt) != "" {
			fmt.Fprintf(w, "    %s\n", indentPromptExcerpt(excerpt.Excerpt))
		} else {
			fmt.Fprintln(w, "    excerpt unavailable; fingerprint is not present in the current binary")
		}
	}
	if len(diff.Removed) > 20 {
		fmt.Fprintf(w, "  - ... %d more\n", len(diff.Removed)-20)
	}
}

func indentPromptExcerpt(excerpt string) string {
	return strings.ReplaceAll(strings.TrimSpace(excerpt), "\n", "\n    ")
}

func firstPromptN(values []PromptFingerprint, n int) []PromptFingerprint {
	if len(values) <= n {
		return values
	}
	return values[:n]
}

func promptKindOrDefault(kind string) string {
	if strings.TrimSpace(kind) == "" {
		return "prompt"
	}
	return kind
}

func printSuggestions(diff auditDiff) {
	var suggestions []string
	for _, category := range diff.Categories {
		if len(category.Added) == 0 && len(category.Removed) == 0 {
			continue
		}
		switch category.Name {
		case "binary-source":
			suggestions = append(suggestions, "binary source changed: inspect current audit output, run the lifecycle checklist, then refresh the baseline only after coverage is intentional")
		case "routes", "actor-runtime-markers":
			suggestions = append(suggestions, "actor/route changed: run go test -count=1 -run 'TestRegisterManagementRoutes|TestReverseProxy|TestNeoRuntime' ./internal/api/modules/amp")
		case "route-methods":
			suggestions = append(suggestions, "route methods changed: compare binary HTTP verbs for actor/provider/internal routes before changing local handlers or refreshing the baseline")
		case "route-coverage":
			suggestions = append(suggestions, "route coverage changed: classify endpoint ownership before adding local handlers or refreshing the baseline")
		case "thread-delta-events":
			suggestions = append(suggestions, "thread delta changed: audit neo runtime delta handlers and run focused thread lifecycle tests")
		case "thread-delta-coverage":
			suggestions = append(suggestions, "thread delta coverage changed: classify lifecycle area before refreshing the baseline")
		case "thread-reader-markers":
			suggestions = append(suggestions, "thread reader markers changed: compare binary read/search/thread lookup behavior against local Neo tool handling before refreshing the baseline")
		case "thread-reader-coverage":
			suggestions = append(suggestions, "thread reader coverage changed: classify upstream-owned thread reader surface before refreshing the baseline")
		case "tool-cancel-reasons":
			suggestions = append(suggestions, "tool cancellation reasons changed: compare binary cancellation text and run interrupted-tool/code-review cleanup tests")
		case "tool-cancel-coverage":
			suggestions = append(suggestions, "tool cancellation coverage changed: classify cancellation reason area before refreshing the baseline")
		case "tool-run-statuses":
			suggestions = append(suggestions, "tool run statuses changed: compare binary status mapping and run tool result/status tests")
		case "tool-run-coverage":
			suggestions = append(suggestions, "tool run status coverage changed: classify status area before refreshing the baseline")
		case "tool-catalog-markers":
			suggestions = append(suggestions, "tool catalog markers changed: compare binary built-in/browser/MCP/tool override surfaces against local Neo tool handling")
		case "tool-catalog-coverage":
			suggestions = append(suggestions, "tool catalog coverage changed: classify tool surface area before refreshing the baseline")
		case "stream-json-markers":
			suggestions = append(suggestions, "stream-json markers changed: inspect execute-mode init/result output and approval handling before refreshing the baseline")
		case "stream-json-coverage":
			suggestions = append(suggestions, "stream-json coverage changed: classify execute-mode marker area before refreshing the baseline")
		case "mode-setting-markers":
			suggestions = append(suggestions, "mode setting markers changed: audit agent mode, reasoning effort, and provider speed defaults")
		case "mode-setting-coverage":
			suggestions = append(suggestions, "mode setting coverage changed: classify mode setting area before refreshing the baseline")
		case "provider-protocol-markers":
			suggestions = append(suggestions, "provider protocol markers changed: compare binary provider headers, Anthropic betas, and feature tags against local Neo request builders")
		case "provider-protocol-coverage":
			suggestions = append(suggestions, "provider protocol coverage changed: classify provider header/beta ownership before refreshing the baseline")
		case "agent-mode-profiles":
			suggestions = append(suggestions, "agent mode profiles changed: compare binary primary models, reasoning defaults, visible modes, and tool-set references")
		case "agent-mode-routes":
			suggestions = append(suggestions, "agent mode routes changed: compare binary default models/providers against selectNeoModelRoute and reasoning defaults")
		case "agent-mode-coverage":
			suggestions = append(suggestions, "agent mode coverage changed: classify mode ownership before refreshing the baseline")
		case "settings":
			suggestions = append(suggestions, "settings changed: audit mode/reasoning/config handling and remote-web controls")
		case "setting-defaults":
			suggestions = append(suggestions, "setting defaults changed: compare binary default values for mode, reasoning, tools, UI, and provider settings before refreshing the baseline")
		case "setting-coverage":
			suggestions = append(suggestions, "setting coverage changed: refresh the baseline only after the ownership classification is intentional")
		case "models":
			suggestions = append(suggestions, "model list changed: check registry/routing tables, thinking support, and provider fallbacks")
		case "model-limits":
			suggestions = append(suggestions, "model limits changed: compare binary context windows and max output tokens against Neo runtime and static registry overrides")
		case "large-context-rules":
			suggestions = append(suggestions, "large context rule changed: compare binary Opus large-context alias and enableLargeContext behavior against neoEffectiveContextWindow")
		case "adaptive-thinking-rules":
			suggestions = append(suggestions, "adaptive thinking rule changed: compare binary Opus adaptive model set and effort fallback against Anthropic request builders")
		case "provider-reasoning-rules":
			suggestions = append(suggestions, "provider reasoning rule changed: compare binary provider fallback order against neoProviderReasoningEffort and Google thinking fallback")
		case "provider-header-rules":
			suggestions = append(suggestions, "provider header rule changed: compare binary Anthropic beta, override, feature, thread, and message header construction against Neo request builders")
		case "provider-feature-rules":
			suggestions = append(suggestions, "provider feature rule changed: compare binary X-Amp-Feature callsites for chat, review, thread reader, painter, and image generation")
		case "compaction-rules":
			suggestions = append(suggestions, "compaction rule changed: compare binary trigger threshold, usage formula, helper headers, and summary history role against Neo compaction")
		case "model-coverage":
			suggestions = append(suggestions, "model coverage changed: classify provider/family before refreshing the baseline")
		case "actor-runtime-coverage":
			suggestions = append(suggestions, "actor runtime coverage changed: classify marker area before refreshing the baseline")
		case "prompt-fingerprint-metadata":
			suggestions = append(suggestions, "prompt/source fingerprint metadata changed: compare exact hash length, kind, and tags before refreshing the baseline")
		case "prompt-kind-counts":
			suggestions = append(suggestions, "prompt/source kind counts changed: compare exact prompt/source fingerprint totals before refreshing the baseline")
		case "prompt-tag-counts":
			suggestions = append(suggestions, "prompt/source tag counts changed: use the changed kind/tag pairs to pick focused compaction/tools/skills/painter checks")
		}
	}
	if promptDiffHasKind(diff.Prompts, "prompt") {
		suggestions = append(suggestions, "prompt/guidance changed: inspect binary prompt markers, then run compaction/system-prompt/code-review tests")
	}
	if promptDiffHasKind(diff.Prompts, "source") {
		suggestions = append(suggestions, "bundled source prompt-like strings changed: inspect only if route/model/setting markers also moved or the source excerpt mentions a local-runtime surface")
	}
	if unknown := unknownPromptKindsFromDiff(diff.Prompts); len(unknown) > 0 {
		suggestions = append(suggestions, "unknown prompt fingerprint kinds detected: classify the prompt/source kind before refreshing the baseline: "+strings.Join(firstN(unknown, 8), ", "))
	}
	if unknown := unknownPromptKindsFromMetadataDiff(diff); len(unknown) > 0 {
		suggestions = append(suggestions, "unknown prompt fingerprint metadata kinds detected: classify the prompt/source kind before refreshing the baseline: "+strings.Join(firstN(unknown, 8), ", "))
	}
	if unknown := unknownPromptKindsFromTagCountDiff(diff); len(unknown) > 0 {
		suggestions = append(suggestions, "unknown prompt tag-count kinds detected: classify the prompt/source kind before refreshing the baseline: "+strings.Join(firstN(unknown, 8), ", "))
	}
	if unknown := unknownPromptKindCountValuesFromDiff(diff); len(unknown) > 0 {
		suggestions = append(suggestions, "unknown prompt/source kind count values detected: compare exact prompt/source fingerprint totals against the Amp binary before refreshing the baseline: "+strings.Join(firstN(unknown, 8), ", "))
	}
	if unknown := unknownPromptReviewTags(promptTagsFromDiff(diff.Prompts, "prompt")); len(unknown) > 0 {
		suggestions = append(suggestions, "unknown prompt tags detected: classify the prompt surface before refreshing the baseline: "+strings.Join(firstN(unknown, 8), ", "))
	}
	if unknown := unknownPromptReviewTags(promptTagsFromMetadataDiff(diff, "prompt")); len(unknown) > 0 {
		suggestions = append(suggestions, "unknown prompt metadata tags detected: classify the prompt surface before refreshing the baseline: "+strings.Join(firstN(unknown, 8), ", "))
	}
	if unknown := unknownSourceReviewTags(promptTagsFromDiff(diff.Prompts, "source")); len(unknown) > 0 {
		suggestions = append(suggestions, "unknown source tags detected: classify the bundled source surface before refreshing the baseline: "+strings.Join(firstN(unknown, 8), ", "))
	}
	if unknown := unknownSourceReviewTags(promptTagsFromMetadataDiff(diff, "source")); len(unknown) > 0 {
		suggestions = append(suggestions, "unknown source metadata tags detected: classify the bundled source surface before refreshing the baseline: "+strings.Join(firstN(unknown, 8), ", "))
	}
	if unknown := unknownPromptReviewTags(promptTagsFromTagCountDiff(diff, "prompt")); len(unknown) > 0 {
		suggestions = append(suggestions, "unknown prompt tag counts detected: classify the prompt surface before refreshing the baseline: "+strings.Join(firstN(unknown, 8), ", "))
	}
	if unknown := unknownSourceReviewTags(promptTagsFromTagCountDiff(diff, "source")); len(unknown) > 0 {
		suggestions = append(suggestions, "unknown source tag counts detected: classify the bundled source surface before refreshing the baseline: "+strings.Join(firstN(unknown, 8), ", "))
	}
	if unknown := unknownPromptTagCountValuesFromDiff(diff); len(unknown) > 0 {
		suggestions = append(suggestions, "unknown prompt/source tag count values detected: compare exact prompt/source fingerprint counts against the Amp binary before refreshing the baseline: "+strings.Join(firstN(unknown, 8), ", "))
	}
	if unknown := unknownSettingsFromCoverage(diff); len(unknown) > 0 {
		suggestions = append(suggestions, "unknown settings detected: classify ownership before refreshing the baseline: "+strings.Join(firstN(unknown, 8), ", "))
	}
	if unknown := unknownSettingDefaultInternalsFromCoverage(diff); len(unknown) > 0 {
		suggestions = append(suggestions, "unknown setting default values detected: classify default value and ownership before refreshing the baseline: "+strings.Join(firstN(unknown, 8), ", "))
	}
	if unknown := unknownRoutesFromCoverage(diff); len(unknown) > 0 {
		suggestions = append(suggestions, "unknown routes detected: classify ownership before refreshing the baseline: "+strings.Join(firstN(unknown, 8), ", "))
	}
	if unknown := unknownThreadDeltasFromCoverage(diff); len(unknown) > 0 {
		suggestions = append(suggestions, "unknown thread deltas detected: classify lifecycle area before refreshing the baseline: "+strings.Join(firstN(unknown, 8), ", "))
	}
	if unknown := unknownThreadReadersFromCoverage(diff); len(unknown) > 0 {
		suggestions = append(suggestions, "unknown thread reader markers detected: classify upstream-owned thread reader surface before refreshing the baseline: "+strings.Join(firstN(unknown, 8), ", "))
	}
	if unknown := unknownToolCancelsFromCoverage(diff); len(unknown) > 0 {
		suggestions = append(suggestions, "unknown tool cancellation reasons detected: classify cancellation area before refreshing the baseline: "+strings.Join(firstN(unknown, 8), ", "))
	}
	if unknown := unknownToolRunStatusesFromCoverage(diff); len(unknown) > 0 {
		suggestions = append(suggestions, "unknown tool run statuses detected: classify status area before refreshing the baseline: "+strings.Join(firstN(unknown, 8), ", "))
	}
	if unknown := unknownToolCatalogMarkersFromCoverage(diff); len(unknown) > 0 {
		suggestions = append(suggestions, "unknown tool catalog markers detected: classify tool surface area before refreshing the baseline: "+strings.Join(firstN(unknown, 8), ", "))
	}
	if unknown := unknownStreamJSONMarkersFromCoverage(diff); len(unknown) > 0 {
		suggestions = append(suggestions, "unknown stream-json markers detected: classify execute-mode marker area before refreshing the baseline: "+strings.Join(firstN(unknown, 8), ", "))
	}
	if unknown := unknownModeSettingMarkersFromCoverage(diff); len(unknown) > 0 {
		suggestions = append(suggestions, "unknown mode setting markers detected: classify mode setting area before refreshing the baseline: "+strings.Join(firstN(unknown, 8), ", "))
	}
	if unknown := unknownProviderProtocolsFromCoverage(diff); len(unknown) > 0 {
		suggestions = append(suggestions, "unknown provider protocol markers, features, headers, tools, betas, settings, sources, or efforts detected: classify provider header/beta/feature/reasoning area before refreshing the baseline: "+strings.Join(firstN(unknown, 8), ", "))
	}
	if unknown := unknownAgentModesFromCoverage(diff); len(unknown) > 0 {
		suggestions = append(suggestions, "unknown agent modes detected: classify ownership before refreshing the baseline: "+strings.Join(firstN(unknown, 8), ", "))
	}
	if unknown := unknownAgentModeInternalsFromCoverage(diff); len(unknown) > 0 {
		suggestions = append(suggestions, "unknown agent mode values detected: classify primary model, route provider/model, reasoning, visibility, tools, or large-context fields before refreshing the baseline: "+strings.Join(firstN(unknown, 8), ", "))
	}
	if unknown := unknownModelsFromCoverage(diff); len(unknown) > 0 {
		suggestions = append(suggestions, "unknown models detected: classify provider/family before refreshing the baseline: "+strings.Join(firstN(unknown, 8), ", "))
	}
	if unknown := unknownModelLimitInternalsFromCoverage(diff); len(unknown) > 0 {
		suggestions = append(suggestions, "unknown model limit values detected: classify enum/name shape, display name, context window, and max output before refreshing the baseline: "+strings.Join(firstN(unknown, 8), ", "))
	}
	if unknown := unknownLargeContextInternalsFromCoverage(diff); len(unknown) > 0 {
		suggestions = append(suggestions, "unknown large-context rule values detected: classify primary model, alias, context window, token math, and enable gate before refreshing the baseline: "+strings.Join(firstN(unknown, 8), ", "))
	}
	if unknown := unknownAdaptiveThinkingInternalsFromCoverage(diff); len(unknown) > 0 {
		suggestions = append(suggestions, "unknown adaptive thinking rule values detected: classify effort/type/display/output behavior before refreshing the baseline: "+strings.Join(firstN(unknown, 8), ", "))
	}
	if unknown := unknownCompactionRuleInternalsFromCoverage(diff); len(unknown) > 0 {
		suggestions = append(suggestions, "unknown compaction rule values detected: classify provider, threshold, usage formula, summary, and helper behavior before refreshing the baseline: "+strings.Join(firstN(unknown, 8), ", "))
	}
	if unknown := unknownActorsFromCoverage(diff); len(unknown) > 0 {
		suggestions = append(suggestions, "unknown actor markers detected: classify marker area before refreshing the baseline: "+strings.Join(firstN(unknown, 8), ", "))
	}
	if len(suggestions) == 0 {
		return
	}
	auditPrintln("\nnext checks:")
	for _, suggestion := range sortedSet(suggestions) {
		auditPrintf("  - %s\n", suggestion)
	}
}

func lifecycleChecklist(snapshot Snapshot, diff auditDiff, full bool) []lifecycleCheck {
	changed := changedCategorySet(diff)
	sourceOnlyDrift := len(changed) == 1 && changed["binary-source"]
	full = full || changed["binary-source"]
	routeChanged := changed["routes"] || changed["route-methods"] || changed["route-coverage"]
	threadReaderChanged := changed["thread-reader-markers"] || changed["thread-reader-coverage"]
	providerProtocolChanged := changed["provider-protocol-markers"] || changed["provider-protocol-coverage"] || changed["provider-header-rules"] || changed["provider-feature-rules"]
	toolCatalogChanged := changed["tool-catalog-markers"] || changed["tool-catalog-coverage"]
	toolLifecycleChanged := changed["tool-cancel-reasons"] || changed["tool-cancel-coverage"] || changed["tool-run-statuses"] || changed["tool-run-coverage"] || toolCatalogChanged
	remoteWebLifecycleChanged := changed["thread-delta-events"] || changed["thread-delta-coverage"] || threadReaderChanged || toolLifecycleChanged
	remoteWebModeChanged := changed["agent-mode-profiles"] || changed["agent-mode-routes"] || changed["agent-mode-coverage"] || changed["mode-setting-markers"] || changed["mode-setting-coverage"] || changed["adaptive-thinking-rules"] || changed["provider-reasoning-rules"]
	remoteWebActorChanged := changed["actor-runtime-markers"] || changed["actor-runtime-coverage"]
	changedPromptTags := promptTagsFromDiff(diff.Prompts, "prompt")
	changedPromptMetadataTags := promptTagsFromMetadataDiff(diff, "prompt")
	changedPromptCountTags := promptTagsFromTagCountDiff(diff, "prompt")
	changedSourceTags := promptTagsFromDiff(diff.Prompts, "source")
	changedSourceMetadataTags := promptTagsFromMetadataDiff(diff, "source")
	changedSourceCountTags := promptTagsFromTagCountDiff(diff, "source")
	hasPromptDiff := promptDiffHasKind(diff.Prompts, "prompt")
	hasPromptMetadataDiff := promptMetadataDiffHasKind(diff, "prompt")
	hasSourceDiff := promptDiffHasKind(diff.Prompts, "source")
	hasSourceMetadataDiff := promptMetadataDiffHasKind(diff, "source")
	changedPromptTags = append(changedPromptTags, changedPromptMetadataTags...)
	changedSourceTags = append(changedSourceTags, changedSourceMetadataTags...)
	changedPromptSurfaceTags := append(append([]string{}, changedPromptTags...), changedSourceTags...)
	changedPromptSurfaceCountTags := append(append([]string{}, changedPromptCountTags...), changedSourceCountTags...)
	anyPromptFingerprintChanged := hasPromptDiff || hasSourceDiff || hasPromptMetadataDiff || hasSourceMetadataDiff
	sourceSettingsChanged := (hasSourceDiff || hasSourceMetadataDiff) && anyTag(changedSourceTags, "settings") || changed["prompt-tag-counts"] && anyTag(changedSourceCountTags, "settings")
	sourceArtifactsChanged := (hasSourceDiff || hasSourceMetadataDiff) && anyTag(changedSourceTags, "artifacts") || changed["prompt-tag-counts"] && anyTag(changedSourceCountTags, "artifacts")
	toolPromptSurfaceChanged := anyPromptFingerprintChanged && anyTag(changedPromptSurfaceTags, "tools") || changed["prompt-tag-counts"] && anyTag(changedPromptSurfaceCountTags, "tools")
	streamingPromptSurfaceChanged := anyPromptFingerprintChanged && anyTag(changedPromptSurfaceTags, "tools", "guidance", "system-prompt") || changed["prompt-tag-counts"] && anyTag(changedPromptSurfaceCountTags, "tools", "guidance", "system-prompt")
	unknownPromptTags := unknownPromptReviewTags(changedPromptTags)
	unknownPromptCountTags := unknownPromptReviewTags(changedPromptCountTags)
	unknownSourceTags := unknownSourceReviewTags(changedSourceTags)
	unknownSourceCountTags := unknownSourceReviewTags(changedSourceCountTags)
	unknownPromptKinds := unknownPromptKindsFromDiff(diff.Prompts)
	unknownPromptMetadataKinds := unknownPromptKindsFromMetadataDiff(diff)
	unknownPromptCountKinds := unknownPromptKindsFromTagCountDiff(diff)
	unknownDiffSignals := len(unknownSettingsFromCoverage(diff)) > 0 ||
		len(unknownSettingDefaultInternalsFromCoverage(diff)) > 0 ||
		len(unknownRoutesFromCoverage(diff)) > 0 ||
		len(unknownThreadDeltasFromCoverage(diff)) > 0 ||
		len(unknownThreadReadersFromCoverage(diff)) > 0 ||
		len(unknownToolCancelsFromCoverage(diff)) > 0 ||
		len(unknownToolRunStatusesFromCoverage(diff)) > 0 ||
		len(unknownToolCatalogMarkersFromCoverage(diff)) > 0 ||
		len(unknownStreamJSONMarkersFromCoverage(diff)) > 0 ||
		len(unknownModeSettingMarkersFromCoverage(diff)) > 0 ||
		len(unknownProviderProtocolsFromCoverage(diff)) > 0 ||
		len(unknownAgentModesFromCoverage(diff)) > 0 ||
		len(unknownAgentModeInternalsFromCoverage(diff)) > 0 ||
		len(unknownModelsFromCoverage(diff)) > 0 ||
		len(unknownModelLimitInternalsFromCoverage(diff)) > 0 ||
		len(unknownLargeContextInternalsFromCoverage(diff)) > 0 ||
		len(unknownAdaptiveThinkingInternalsFromCoverage(diff)) > 0 ||
		len(unknownCompactionRuleInternalsFromCoverage(diff)) > 0 ||
		len(unknownActorsFromCoverage(diff)) > 0 ||
		len(unknownPromptKindCountValuesFromDiff(diff)) > 0 ||
		len(unknownPromptTagCountValuesFromDiff(diff)) > 0
	var checks []lifecycleCheck
	add := func(check lifecycleCheck, triggered bool) {
		if full || triggered {
			checks = append(checks, check)
		}
	}

	add(lifecycleCheck{
		Area:    "actor route and websocket bridge",
		Scope:   "local-runtime",
		Trigger: "/actors, /gateway, /metadata, /api/thread-actors, or Rivet actor markers changed",
		Commands: []string{
			`go test -count=1 -run 'TestRegisterManagementRoutes|TestReverseProxy|TestNeoRuntimeGatewayWebSocket|TestNeoRuntimeWebSocket' ./internal/api/modules/amp`,
		},
		Files: []string{
			"internal/api/modules/amp/routes.go",
			"internal/api/modules/amp/proxy.go",
			"internal/api/modules/amp/neo_runtime.go",
		},
	}, routeChanged || changed["actor-runtime-markers"] || changed["actor-runtime-coverage"])

	add(lifecycleCheck{
		Area:    "thread delta reducer",
		Scope:   "local-runtime",
		Trigger: "thread delta names changed",
		Commands: []string{
			`go test -count=1 -run 'TestNeoActorHandles.*Deltas|TestNeoActorProtocolDeltaSequences|TestNeoRuntimeProtocolDeltaNormalizesLikeBinary' ./internal/api/modules/amp`,
		},
		Files: []string{
			"internal/api/modules/amp/neo_runtime.go",
			"internal/api/modules/amp/neo_runtime_test.go",
		},
	}, full || changed["thread-delta-events"] || changed["thread-delta-coverage"])

	add(lifecycleCheck{
		Area:    "queue, steering, and interruption",
		Scope:   "local-runtime",
		Trigger: "queue-related deltas or thread lifecycle markers changed",
		Commands: []string{
			`go test -count=1 -run 'TestNeoActor.*Queue|TestNeoActor.*Steer|TestNeoActorHandlesBinaryUserThreadDeltas|TestNeoActorCancel' ./internal/api/modules/amp`,
		},
		Files: []string{
			"internal/api/modules/amp/neo_runtime.go",
			"internal/api/modules/amp/neo_runtime_test.go",
		},
	}, full || changed["thread-delta-events"] || changed["thread-delta-coverage"])

	add(lifecycleCheck{
		Area:    "tool cancellation and restore cleanup",
		Scope:   "local-runtime",
		Trigger: "tool cancellation reason markers changed",
		Commands: []string{
			`go test -count=1 -run 'TestNeoActor.*Cancel|TestNeoActorBinaryUserMessageCancelsActiveToolProgress|TestNeoHistoryConvertsNonTerminalToolResultLikeBinary|TestOpenAINeoMessagesSkipsFullyInterruptedToolOnlyAssistant' ./internal/api/modules/amp`,
		},
		Files: []string{
			"internal/api/modules/amp/neo_runtime.go",
			"internal/api/modules/amp/neo_runtime_test.go",
		},
	}, changed["tool-cancel-reasons"] || changed["tool-cancel-coverage"])

	add(lifecycleCheck{
		Area:    "tool run status mapping",
		Scope:   "local-runtime",
		Trigger: "tool run status markers changed",
		Commands: []string{
			`go test -count=1 -run 'TestNeoActor.*ToolResult|TestNeoActor.*ToolProgress|TestNeoHistoryConvertsNonTerminalToolResultLikeBinary|TestOpenAINeoMessagesSkipsFullyInterruptedToolOnlyAssistant' ./internal/api/modules/amp`,
			`go run ./cmd/amp_runtime_drift_scan -since-homebrew-runtime -json`,
		},
		Files: []string{
			"cmd/amp_runtime_drift_scan",
			"internal/api/modules/amp/neo_runtime.go",
			"internal/api/modules/amp/neo_runtime_test.go",
			"dev/neo-remote-ui/src/routes/+page.svelte",
		},
	}, changed["tool-run-statuses"] || changed["tool-run-coverage"])

	add(lifecycleCheck{
		Area:    "stream-json execute lifecycle",
		Scope:   "amp-owned",
		Trigger: "stream-json execute markers changed",
		Commands: []string{
			`go test -count=1 -run 'TestNeoActor.*Tool|TestNeoActor.*Reasoning|TestNeoRuntime.*Stream' ./internal/api/modules/amp`,
		},
		Files: []string{
			"cmd/amp_binary_audit/main.go",
			"dev/amp-binary-parity-baseline.json",
		},
	}, changed["stream-json-markers"] || changed["stream-json-coverage"])

	add(lifecycleCheck{
		Area:    "provider protocol headers and betas",
		Scope:   "local-runtime",
		Trigger: "provider protocol headers, Anthropic betas, or Amp feature tags changed",
		Commands: []string{
			`go test -count=1 -run 'TestInferNeo.*ProviderHeaders|TestInferNeoAnthropic.*Header|TestNeoAnthropicProviderHeaders|TestUploadNeoCloudThreadUsesAmpInternalClientHeaders|TestInferNeoLocalFireworksAppliesBinaryProviderSettings' ./internal/api/modules/amp`,
		},
		Files: []string{
			"internal/api/modules/amp/neo_runtime.go",
			"internal/api/modules/amp/neo_runtime_test.go",
			"internal/runtime/executor/claude_executor.go",
		},
	}, providerProtocolChanged || changed["adaptive-thinking-rules"] || changed["provider-reasoning-rules"])

	add(lifecycleCheck{
		Area:    "streaming assistant and tool edits",
		Scope:   "local-runtime",
		Trigger: "assistant/tool deltas, tool lifecycle/catalog markers, provider routes, models, reasoning, or streaming prompt markers changed",
		Commands: []string{
			`go test -count=1 -run 'TestNeoRuntimeWebSocketStreaming|TestInferNeo.*Stream|TestForwardResponsesStream|TestRewriteStreamChunk' ./internal/api/modules/amp ./sdk/api/handlers/openai`,
		},
		Files: []string{
			"internal/api/modules/amp/neo_runtime.go",
			"sdk/api/handlers/openai",
		},
	}, full || changed["thread-delta-events"] || changed["thread-delta-coverage"] || toolLifecycleChanged || streamingPromptSurfaceChanged || routeChanged || providerProtocolChanged || changed["models"] || changed["agent-mode-routes"] || changed["adaptive-thinking-rules"] || changed["provider-reasoning-rules"] || changed["settings"])

	add(lifecycleCheck{
		Area:    "compaction and continuation prompts",
		Scope:   "local-runtime",
		Trigger: "compaction rules, guidance, system-prompt fingerprints, model limits, or mode model routes changed",
		Commands: []string{
			`go run ./cmd/amp_binary_audit -prompt-diff-excerpts`,
			`go run ./cmd/amp_runtime_drift_scan -since-homebrew-runtime -json`,
			`go test -count=1 -run 'TestNeo.*Compaction|TestInferNeo.*Compaction|TestOpenAIResponsesCompact|TestResponsesWebsocketCompaction|TestInputContainsFullTranscriptDetectsCompactionItem' ./internal/api/modules/amp ./sdk/api/handlers/openai`,
		},
		Files: []string{
			"cmd/amp_runtime_drift_scan",
			"internal/api/modules/amp/neo_runtime.go",
			"sdk/api/handlers/openai/openai_responses_compact_test.go",
			"sdk/api/handlers/openai/openai_responses_websocket_test.go",
		},
	}, full || providerProtocolChanged || changed["compaction-rules"] || changed["large-context-rules"] || changed["model-limits"] || changed["agent-mode-routes"] || anyPromptFingerprintChanged && anyTag(changedPromptSurfaceTags, "compaction", "guidance", "system-prompt") || changed["prompt-tag-counts"] && anyTag(changedPromptSurfaceCountTags, "compaction", "guidance", "system-prompt"))

	add(lifecycleCheck{
		Area:    "tools, code review, skills, and images",
		Scope:   "local-runtime",
		Trigger: "tool/code-review/skills/painter fingerprints or related settings changed",
		Commands: []string{
			`go run ./cmd/amp_binary_audit -prompt-diff-excerpts`,
			`go run ./cmd/amp_runtime_drift_scan -since-homebrew-runtime -json`,
			`go test -count=1 -run 'TestNeo.*Tool|TestNeo.*CodeReview|TestNeo.*Skill|TestNeo.*Image|TestImages' ./internal/api/modules/amp ./sdk/api/handlers/openai`,
		},
		Files: []string{
			"cmd/amp_runtime_drift_scan",
			"internal/api/modules/amp/neo_runtime.go",
			"internal/api/modules/amp/routes.go",
			"sdk/api/handlers/openai/openai_images_handlers_test.go",
		},
	}, full || providerProtocolChanged || toolCatalogChanged || changed["tool-cancel-reasons"] || changed["tool-cancel-coverage"] || changed["tool-run-statuses"] || changed["tool-run-coverage"] || (changed["settings"] || changed["setting-defaults"] || changed["setting-coverage"]) && (anySetting(snapshot.Signals.Settings, "painter.model", "skills.path", "tools.enable", "tools.disable", "toolbox.path", "mcpServers") || diffHasAnySetting(diff, "painter.model", "skills.path", "tools.enable", "tools.disable", "toolbox.path", "mcpServers")) || anyPromptFingerprintChanged && anyTag(changedPromptSurfaceTags, "tools", "code-review", "skills", "painter") || changed["prompt-tag-counts"] && anyTag(changedPromptSurfaceCountTags, "tools", "code-review", "skills", "painter"))

	add(lifecycleCheck{
		Area:    "model routing, modes, and reasoning",
		Scope:   "local-runtime",
		Trigger: "models, reasoning settings, mode settings, or setting ownership changed",
		Commands: []string{
			`go test -count=1 -run 'Test.*Model|Test.*Reasoning|Test.*Thinking|Test.*Settings|TestInferNeo.*Applies' ./internal/api/modules/amp ./sdk/api/handlers/openai ./internal/registry ./internal/thinking/...`,
		},
		Files: []string{
			"internal/api/modules/amp/neo_runtime.go",
			"internal/registry",
			"internal/thinking",
			"sdk/api/handlers/openai",
		},
	}, full || providerProtocolChanged || changed["models"] || changed["model-limits"] || changed["model-coverage"] || changed["large-context-rules"] || changed["adaptive-thinking-rules"] || changed["provider-reasoning-rules"] || changed["settings"] || changed["setting-defaults"] || changed["setting-coverage"] || sourceSettingsChanged || changed["mode-setting-markers"] || changed["mode-setting-coverage"] || changed["agent-mode-profiles"] || changed["agent-mode-routes"] || changed["agent-mode-coverage"])

	add(lifecycleCheck{
		Area:    "upstream-owned thread read and search",
		Scope:   "amp-owned",
		Trigger: "/threads, /api/threads/find, thread reader RPC/route markers, amp.read-thread provider feature, or tool prompt/source fingerprints changed",
		Commands: []string{
			threadReadSearchTestCommand,
		},
		Files: []string{
			"internal/api/modules/amp/routes.go",
			"internal/api/modules/amp/neo_runtime.go",
		},
	}, full || threadReaderChanged || routeChanged && (hasAnyRoute(snapshot.Signals.Routes, "/threads", "/api/threads") || diffHasAnyRoute(diff, "/threads", "/api/threads")) || diffHasAnyProviderFeature(diff, "amp.read-thread") || toolPromptSurfaceChanged)

	add(lifecycleCheck{
		Area:    "remote web control surface",
		Scope:   "remote-web",
		Trigger: "remote-web settings, production proxy prefixes, /api/internal, attachment route, artifact rendering, thread lifecycle/status rendering, tool lifecycle/catalog rendering, tool prompt/source fingerprints, mode/reasoning controls, actor runtime markers, or thread control bridge changed",
		Commands: []string{
			`bun run --cwd dev/neo-remote-ui check`,
			`bun run --cwd dev/neo-remote-ui smoke`,
			`go test -count=1 -run 'TestRegisterManagementRoutes|TestDecodeNeoAttachmentPayloadMatchesBinaryImageLimits|TestNeoClient' ./internal/api/modules/amp`,
		},
		Files: []string{
			"dev/neo-remote-ui/server.ts",
			"dev/neo-remote-ui/vite.config.ts",
			"dev/neo-remote-ui/src/routes/+page.svelte",
			"dev/neo-remote-ui/smoke.mjs",
			"internal/api/modules/amp/routes.go",
		},
	}, full || remoteWebLifecycleChanged || remoteWebModeChanged || remoteWebActorChanged || sourceSettingsChanged || sourceArtifactsChanged || toolPromptSurfaceChanged || (changed["settings"] || changed["setting-defaults"] || changed["setting-coverage"]) && (hasScope(snapshot.Signals.SettingCoverage, "remote-web") || diffHasSettingScope(diff, "remote-web")) || routeChanged && (hasAnyRoute(snapshot.Signals.Routes, "/api/internal", "/api/attachments", "/actors", "/gateway", "/metadata", "/threads") || diffHasAnyRoute(diff, "/api/internal", "/api/attachments", "/actors", "/gateway", "/metadata", "/threads")))

	add(lifecycleCheck{
		Area:    "unknown signal triage",
		Scope:   "baseline",
		Trigger: "unknown route, lifecycle, tool, provider, mode, model, setting, actor, or prompt/source taxonomy changed",
		Commands: []string{
			`go run ./cmd/amp_binary_audit -prompt-diff-excerpts`,
			`go run ./cmd/amp_binary_audit -checklist`,
			`go run ./cmd/amp_binary_audit -strict`,
		},
		Files: []string{
			"cmd/amp_binary_audit/main.go",
			"dev/amp-binary-parity-baseline.json",
		},
	}, full || unknownDiffSignals || len(unknownRoutes(snapshot.Signals.RouteCoverage)) > 0 || len(unknownThreadDeltas(snapshot.Signals.ThreadDeltaCoverage)) > 0 || len(unknownThreadReaders(snapshot.Signals.ThreadReaderCoverage)) > 0 || len(unknownToolCancelReasons(snapshot.Signals.ToolCancelCoverage)) > 0 || len(unknownToolRunStatuses(snapshot.Signals.ToolRunCoverage)) > 0 || len(unknownToolCatalogMarkers(snapshot.Signals.ToolCatalogCoverage)) > 0 || len(unknownStreamJSONMarkers(snapshot.Signals.StreamJSONCoverage)) > 0 || len(unknownModeSettingMarkers(snapshot.Signals.ModeSettingCoverage)) > 0 || len(unknownProviderProtocols(snapshot.Signals.ProviderCoverage)) > 0 || len(unknownAgentModes(snapshot.Signals.AgentModeCoverage)) > 0 || len(unknownSettings(snapshot.Signals.SettingCoverage)) > 0 || len(unknownModels(snapshot.Signals.ModelCoverage)) > 0 || len(unknownActors(snapshot.Signals.ActorCoverage)) > 0 || len(unknownPromptKinds) > 0 || len(unknownPromptMetadataKinds) > 0 || changed["prompt-tag-counts"] && len(unknownPromptCountKinds) > 0 || (hasPromptDiff || hasPromptMetadataDiff) && (len(changedPromptTags) == 0 || len(unknownPromptTags) > 0) || len(unknownSourceTags) > 0 || changed["prompt-kind-counts"] && len(unknownPromptKindCountValuesFromDiff(diff)) > 0 || changed["prompt-tag-counts"] && (len(unknownPromptCountTags) > 0 || len(unknownSourceCountTags) > 0 || len(unknownPromptTagCountValuesFromDiff(diff)) > 0))

	for i := range checks {
		if sourceOnlyDrift {
			checks[i].Trigger = "Amp binary source changed; run the full parity review even though extracted lifecycle signals did not change"
			continue
		}
		if checks[i].Trigger == "" {
			checks[i].Trigger = "current Amp binary exposes this lifecycle surface"
		}
	}
	sort.SliceStable(checks, func(i, j int) bool {
		if checks[i].Scope == checks[j].Scope {
			return checks[i].Area < checks[j].Area
		}
		return checks[i].Scope < checks[j].Scope
	})
	return checks
}

func changedCategorySet(diff auditDiff) map[string]bool {
	changed := map[string]bool{}
	for _, category := range diff.Categories {
		if len(category.Added) > 0 || len(category.Removed) > 0 {
			changed[category.Name] = true
		}
	}
	if len(diff.Prompts.Added) > 0 || len(diff.Prompts.Removed) > 0 {
		changed["prompt-fingerprints"] = true
	}
	return changed
}

func promptTagsFromSnapshot(fingerprints []PromptFingerprint, kinds ...string) []string {
	set := map[string]struct{}{}
	kindSet := sliceSet(kinds)
	for _, fp := range fingerprints {
		if len(kindSet) > 0 {
			if _, ok := kindSet[promptKindOrDefault(fp.Kind)]; !ok {
				continue
			}
		}
		for _, tag := range fp.Tags {
			set[tag] = struct{}{}
		}
	}
	return sortedKeys(set)
}

func promptTagsFromDiff(diff promptDiff, kinds ...string) []string {
	set := map[string]struct{}{}
	kindSet := sliceSet(kinds)
	for _, fp := range diff.Added {
		if len(kindSet) > 0 {
			if _, ok := kindSet[promptKindOrDefault(fp.Kind)]; !ok {
				continue
			}
		}
		for _, tag := range fp.Tags {
			set[tag] = struct{}{}
		}
	}
	for _, fp := range diff.Removed {
		if len(kindSet) > 0 {
			if _, ok := kindSet[promptKindOrDefault(fp.Kind)]; !ok {
				continue
			}
		}
		for _, tag := range fp.Tags {
			set[tag] = struct{}{}
		}
	}
	return sortedKeys(set)
}

func promptTagsFromTagCountDiff(diff auditDiff, kinds ...string) []string {
	set := map[string]struct{}{}
	kindSet := sliceSet(kinds)
	for _, category := range diff.Categories {
		if category.Name != "prompt-tag-counts" {
			continue
		}
		for _, value := range append(category.Added, category.Removed...) {
			left, _, _ := strings.Cut(value, "=")
			kind, tag, ok := strings.Cut(left, "/")
			if !ok {
				kind = "prompt"
				tag = left
			}
			if len(kindSet) > 0 {
				if _, ok := kindSet[promptKindOrDefault(kind)]; !ok {
					continue
				}
			}
			if strings.TrimSpace(tag) != "" {
				set[tag] = struct{}{}
			}
		}
	}
	return sortedKeys(set)
}

func promptTagsFromMetadataDiff(diff auditDiff, kinds ...string) []string {
	set := map[string]struct{}{}
	kindSet := sliceSet(kinds)
	for _, category := range diff.Categories {
		if category.Name != "prompt-fingerprint-metadata" {
			continue
		}
		for _, value := range changedDiffValues(category) {
			kind, tags := promptFingerprintMetadataKindAndTags(value)
			if len(kindSet) > 0 {
				if _, ok := kindSet[promptKindOrDefault(kind)]; !ok {
					continue
				}
			}
			for _, tag := range tags {
				if strings.TrimSpace(tag) != "" {
					set[tag] = struct{}{}
				}
			}
		}
	}
	return sortedKeys(set)
}

func promptDiffHasKind(diff promptDiff, kind string) bool {
	kind = promptKindOrDefault(kind)
	for _, fp := range diff.Added {
		if promptKindOrDefault(fp.Kind) == kind {
			return true
		}
	}
	for _, fp := range diff.Removed {
		if promptKindOrDefault(fp.Kind) == kind {
			return true
		}
	}
	return false
}

func promptMetadataDiffHasKind(diff auditDiff, kind string) bool {
	kind = promptKindOrDefault(kind)
	for _, category := range diff.Categories {
		if category.Name != "prompt-fingerprint-metadata" {
			continue
		}
		for _, value := range changedDiffValues(category) {
			valueKind, _ := promptFingerprintMetadataKindAndTags(value)
			if promptKindOrDefault(valueKind) == kind {
				return true
			}
		}
	}
	return false
}

func promptFingerprintMetadataKindAndTags(value string) (string, []string) {
	kind := "prompt"
	if _, right, ok := strings.Cut(value, "|kind="); ok {
		kind, _, _ = strings.Cut(right, "|")
	}
	var tags []string
	if _, right, ok := strings.Cut(value, "|tags="); ok {
		for _, tag := range strings.Split(right, ",") {
			if strings.TrimSpace(tag) != "" {
				tags = append(tags, strings.TrimSpace(tag))
			}
		}
	}
	return promptKindOrDefault(kind), tags
}

func unknownPromptKindsFromDiff(diff promptDiff) []string {
	set := map[string]struct{}{}
	for _, fp := range append(diff.Added, diff.Removed...) {
		kind := promptKindOrDefault(fp.Kind)
		if !knownPromptKind(kind) {
			set[kind] = struct{}{}
		}
	}
	return sortedKeys(set)
}

func unknownPromptKindsFromMetadataDiff(diff auditDiff) []string {
	set := map[string]struct{}{}
	for _, category := range diff.Categories {
		if category.Name != "prompt-fingerprint-metadata" {
			continue
		}
		for _, value := range changedDiffValues(category) {
			kind, _ := promptFingerprintMetadataKindAndTags(value)
			if !knownPromptKind(kind) {
				set[kind] = struct{}{}
			}
		}
	}
	return sortedKeys(set)
}

func unknownPromptKindsFromTagCountDiff(diff auditDiff) []string {
	set := map[string]struct{}{}
	for _, category := range diff.Categories {
		if category.Name != "prompt-tag-counts" {
			continue
		}
		for _, value := range append(category.Added, category.Removed...) {
			left, _, _ := strings.Cut(value, "=")
			kind, _, ok := strings.Cut(left, "/")
			if !ok {
				kind = "prompt"
			}
			kind = promptKindOrDefault(kind)
			if !knownPromptKind(kind) {
				set[kind] = struct{}{}
			}
		}
	}
	return sortedKeys(set)
}

func unknownPromptKindCountValuesFromDiff(diff auditDiff) []string {
	seen := map[string]struct{}{}
	for _, category := range diff.Categories {
		if category.Name != "prompt-kind-counts" {
			continue
		}
		for _, value := range category.Added {
			kind, count, ok := promptKindCountValueParts(value)
			if !ok {
				seen[value] = struct{}{}
				continue
			}
			expected, known := knownPromptKindCountValues[kind]
			if !known || count != expected {
				seen[fmt.Sprintf("%s=%d", kind, count)] = struct{}{}
			}
		}
		for _, value := range category.Removed {
			kind, count, ok := promptKindCountValueParts(value)
			if !ok {
				seen[value] = struct{}{}
				continue
			}
			seen[fmt.Sprintf("%s=%d", kind, count)] = struct{}{}
		}
	}
	return sortedKeys(seen)
}

func promptKindCountValueParts(value string) (string, int, bool) {
	kind, countValue, ok := strings.Cut(value, "=")
	if !ok {
		return "", 0, false
	}
	kind = promptKindOrDefault(strings.TrimSpace(kind))
	count, err := strconv.Atoi(strings.TrimSpace(countValue))
	if kind == "" || err != nil {
		return "", 0, false
	}
	return kind, count, true
}

func unknownPromptTagCountValuesFromDiff(diff auditDiff) []string {
	seen := map[string]struct{}{}
	for _, category := range diff.Categories {
		if category.Name != "prompt-tag-counts" {
			continue
		}
		for _, value := range category.Added {
			key, count, ok := promptTagCountValueParts(value)
			if !ok {
				seen[value] = struct{}{}
				continue
			}
			expected, known := knownPromptTagCountValues[key]
			if !known || count != expected {
				seen[fmt.Sprintf("%s=%d", key, count)] = struct{}{}
			}
		}
		for _, value := range category.Removed {
			key, count, ok := promptTagCountValueParts(value)
			if !ok {
				seen[value] = struct{}{}
				continue
			}
			seen[fmt.Sprintf("%s=%d", key, count)] = struct{}{}
		}
	}
	return sortedKeys(seen)
}

func promptTagCountValueParts(value string) (string, int, bool) {
	left, right, ok := strings.Cut(value, "=")
	if !ok {
		return "", 0, false
	}
	kind, tag, ok := strings.Cut(left, "/")
	if !ok {
		kind = "prompt"
		tag = left
	}
	kind = promptKindOrDefault(strings.TrimSpace(kind))
	tag = strings.TrimSpace(tag)
	count, err := strconv.Atoi(strings.TrimSpace(right))
	if kind == "" || tag == "" || err != nil {
		return "", 0, false
	}
	return kind + "/" + tag, count, true
}

func promptTagCountKey(kind, tag string) string {
	return promptKindOrDefault(strings.TrimSpace(kind)) + "/" + strings.TrimSpace(tag)
}

func knownPromptKind(kind string) bool {
	switch promptKindOrDefault(kind) {
	case "prompt", "source":
		return true
	default:
		return false
	}
}

func knownPromptTag(kind, tag string) bool {
	switch promptKindOrDefault(kind) {
	case "prompt":
		return len(unknownPromptReviewTags([]string{tag})) == 0
	case "source":
		return len(unknownSourceReviewTags([]string{tag})) == 0
	default:
		return false
	}
}

func anyTag(tags []string, names ...string) bool {
	set := sliceSet(tags)
	for _, name := range names {
		if _, ok := set[name]; ok {
			return true
		}
	}
	return false
}

func unknownPromptReviewTags(tags []string) []string {
	return unknownReviewTags(tags, []string{
		"code-review",
		"compaction",
		"guidance",
		"painter",
		"skills",
		"system-prompt",
		"tools",
	})
}

func unknownSourceReviewTags(tags []string) []string {
	return unknownReviewTags(tags, []string{
		"artifacts",
		"code-review",
		"compaction",
		"guidance",
		"painter",
		"settings",
		"skills",
		"system-prompt",
		"tools",
	})
}

func unknownReviewTags(tags, knownTags []string) []string {
	known := sliceSet(knownTags)
	var unknown []string
	for _, tag := range tags {
		if _, ok := known[tag]; !ok {
			unknown = append(unknown, tag)
		}
	}
	sort.Strings(unknown)
	return unknown
}

func anySetting(settings []string, names ...string) bool {
	set := sliceSet(settings)
	for _, name := range names {
		if _, ok := set[name]; ok {
			return true
		}
	}
	return false
}

func diffHasAnySetting(diff auditDiff, names ...string) bool {
	nameSet := sliceSet(names)
	for _, category := range diff.Categories {
		switch category.Name {
		case "settings":
			for _, value := range append(category.Added, category.Removed...) {
				if _, ok := nameSet[value]; ok {
					return true
				}
			}
		case "setting-defaults", "setting-coverage":
			for _, value := range append(category.Added, category.Removed...) {
				for name := range nameSet {
					if strings.HasPrefix(value, name+"=") {
						return true
					}
				}
			}
		}
	}
	return false
}

func hasScope(coverage []SettingCoverage, scope string) bool {
	for _, item := range coverage {
		if item.Scope == scope {
			return true
		}
	}
	return false
}

func diffHasSettingScope(diff auditDiff, scope string) bool {
	for _, category := range diff.Categories {
		if category.Name != "setting-defaults" && category.Name != "setting-coverage" {
			continue
		}
		for _, value := range append(category.Added, category.Removed...) {
			if strings.HasSuffix(value, "="+scope) {
				return true
			}
		}
	}
	return false
}

func diffHasAnyProviderFeature(diff auditDiff, names ...string) bool {
	nameSet := sliceSet(names)
	for _, category := range diff.Categories {
		if category.Name != "provider-feature-rules" {
			continue
		}
		for _, value := range append(category.Added, category.Removed...) {
			feature, _, _ := strings.Cut(value, "|")
			if _, ok := nameSet[feature]; ok {
				return true
			}
		}
	}
	return false
}

func hasAnyRoute(routes []string, prefixes ...string) bool {
	for _, route := range routes {
		if routeMatchesAnyPrefix(route, prefixes...) {
			return true
		}
	}
	return false
}

func diffHasAnyRoute(diff auditDiff, prefixes ...string) bool {
	for _, category := range diff.Categories {
		switch category.Name {
		case "routes":
			for _, value := range append(category.Added, category.Removed...) {
				if routeMatchesAnyPrefix(value, prefixes...) {
					return true
				}
			}
		case "route-methods":
			for _, value := range append(category.Added, category.Removed...) {
				name, _, _ := routeMethodDiffParts(value)
				if routeMatchesAnyPrefix(name, prefixes...) {
					return true
				}
			}
		case "route-coverage":
			for _, value := range append(category.Added, category.Removed...) {
				name, _ := routeCoverageDiffParts(value)
				if routeMatchesAnyPrefix(name, prefixes...) {
					return true
				}
			}
		}
	}
	return false
}

func routeMatchesAnyPrefix(route string, prefixes ...string) bool {
	for _, prefix := range prefixes {
		if route == prefix || strings.HasPrefix(route, prefix+"/") || strings.HasPrefix(route, prefix+"?") || strings.HasPrefix(route, prefix+"=") {
			return true
		}
	}
	return false
}

func printLifecycleChecklist(checks []lifecycleCheck) {
	if len(checks) == 0 {
		return
	}
	auditPrintln("\nlifecycle parity checklist:")
	for _, check := range checks {
		auditPrintf("  - %s [%s]\n", check.Area, check.Scope)
		auditPrintf("    trigger: %s\n", check.Trigger)
		if len(check.Commands) > 0 {
			auditPrintln("    commands:")
			for _, command := range check.Commands {
				auditPrintf("      %s\n", command)
			}
		}
		if len(check.Files) > 0 {
			auditPrintln("    files:")
			for _, file := range check.Files {
				auditPrintf("      %s\n", file)
			}
		}
	}
}

func unknownSettingsFromCoverage(diff auditDiff) []string {
	seen := map[string]struct{}{}
	for _, category := range diff.Categories {
		switch category.Name {
		case "settings":
			for _, setting := range changedDiffValues(category) {
				if settingScopes[setting] == "" {
					seen[setting] = struct{}{}
				}
			}
		case "setting-defaults", "setting-coverage":
			for _, value := range changedDiffValues(category) {
				name := coverageName(value)
				actual := coverageValue(value)
				expected := settingScopes[name]
				if expected == "" || actual == "unknown" || actual != expected {
					seen[name] = struct{}{}
				}
			}
		}
	}
	return sortedKeys(seen)
}

func unknownSettingDefaultInternalsFromCoverage(diff auditDiff) []string {
	seen := map[string]struct{}{}
	for _, category := range diff.Categories {
		if category.Name != "setting-defaults" {
			continue
		}
		for _, value := range changedDiffValues(category) {
			name, defaultValue, scope := settingDefaultDiffParts(value)
			if strings.TrimSpace(name) == "" {
				seen[value] = struct{}{}
				continue
			}
			if settingScopes[name] == "" {
				seen[name] = struct{}{}
				continue
			}
			if scope != settingScopes[name] {
				seen[name+" scope="+scope] = struct{}{}
			}
			if want, ok := knownSettingDefaultValues[name]; !ok || defaultValue != want {
				seen[name+"="+defaultValue] = struct{}{}
			}
		}
	}
	return sortedKeys(seen)
}

func settingDefaultDiffParts(value string) (string, string, string) {
	name, rest, ok := strings.Cut(value, "=")
	if !ok {
		return value, "", ""
	}
	index := strings.LastIndex(rest, "=")
	if index < 0 {
		return name, rest, ""
	}
	return name, rest[:index], rest[index+1:]
}

func unknownRoutesFromCoverage(diff auditDiff) []string {
	seen := map[string]struct{}{}
	for _, category := range diff.Categories {
		switch category.Name {
		case "routes":
			for _, route := range changedDiffValues(category) {
				if routeScope(route) == "" || !knownRouteValue(route) {
					seen[route] = struct{}{}
				}
			}
		case "route-methods":
			for _, value := range changedDiffValues(category) {
				if _, ok := knownRouteMethodValues[value]; !ok {
					name, methods, scope := routeMethodDiffParts(value)
					if strings.TrimSpace(name) == "" {
						seen[value] = struct{}{}
						continue
					}
					expected := routeScope(name)
					if expected == "" || scope == "unknown" || scope != expected {
						seen[name] = struct{}{}
						continue
					}
					seen[name+" methods="+methods] = struct{}{}
				}
			}
		case "route-coverage":
			for _, value := range changedDiffValues(category) {
				name, actual := routeCoverageDiffParts(value)
				expected := routeScope(name)
				if expected == "" || actual == "unknown" || actual != expected || !knownRouteValue(name) {
					seen[name] = struct{}{}
				}
			}
		}
	}
	return sortedKeys(seen)
}

func knownRouteValue(route string) bool {
	_, ok := knownRouteValues[strings.TrimSpace(route)]
	return ok
}

func routeMethodDiffParts(value string) (string, string, string) {
	scopeIndex := strings.LastIndex(value, "=")
	if scopeIndex < 0 {
		return value, "", ""
	}
	methodIndex := strings.LastIndex(value[:scopeIndex], "=")
	if methodIndex < 0 {
		return value[:scopeIndex], "", value[scopeIndex+1:]
	}
	return value[:methodIndex], value[methodIndex+1 : scopeIndex], value[scopeIndex+1:]
}

func routeCoverageDiffParts(value string) (string, string) {
	scopeIndex := strings.LastIndex(value, "=")
	if scopeIndex < 0 {
		return value, ""
	}
	return value[:scopeIndex], value[scopeIndex+1:]
}

func unknownThreadDeltasFromCoverage(diff auditDiff) []string {
	seen := map[string]struct{}{}
	for _, category := range diff.Categories {
		switch category.Name {
		case "thread-delta-events":
			for _, event := range changedDiffValues(category) {
				if !knownThreadDeltaEventName(event) {
					seen[event] = struct{}{}
				}
			}
		case "thread-delta-coverage":
			for _, value := range changedDiffValues(category) {
				name := coverageName(value)
				actual := coverageValue(value)
				expected := threadDeltaArea(name)
				if !knownThreadDeltaEventName(name) || expected == "" || actual == "unknown" || actual != expected {
					seen[name] = struct{}{}
				}
			}
		}
	}
	return sortedKeys(seen)
}

func unknownThreadReadersFromCoverage(diff auditDiff) []string {
	seen := map[string]struct{}{}
	for _, category := range diff.Categories {
		switch category.Name {
		case "thread-reader-markers":
			for _, marker := range changedDiffValues(category) {
				if threadReaderMarkers[marker] == "" || !knownThreadReaderMarkerValue(marker) {
					seen[marker] = struct{}{}
				}
			}
		case "thread-reader-coverage":
			for _, value := range changedDiffValues(category) {
				name := coverageName(value)
				actual := coverageValue(value)
				expected := threadReaderMarkers[name]
				if expected == "" || actual == "unknown" || actual != expected || !knownThreadReaderMarkerValue(name) {
					seen[name] = struct{}{}
				}
			}
		}
	}
	return sortedKeys(seen)
}

func knownThreadDeltaEventName(event string) bool {
	event = strings.TrimSpace(event)
	if event == "" {
		return false
	}
	if _, ok := threadDeltaAreas[event]; ok {
		return true
	}
	for _, group := range [][]string{knownDeltaNames, knownThreadDeltaEvents, knownThreadProtocolEvents} {
		for _, known := range group {
			if event == known {
				return true
			}
		}
	}
	return false
}

func unknownToolCancelsFromCoverage(diff auditDiff) []string {
	seen := map[string]struct{}{}
	for _, category := range diff.Categories {
		switch category.Name {
		case "tool-cancel-reasons":
			for _, reason := range changedDiffValues(category) {
				if toolCancelReasonArea(reason) == "" || !knownToolCancelReasonValue(reason) {
					seen[reason] = struct{}{}
				}
			}
		case "tool-cancel-coverage":
			for _, value := range changedDiffValues(category) {
				name := coverageName(value)
				actual := coverageValue(value)
				expected := toolCancelReasonArea(name)
				if expected == "" || actual == "unknown" || actual != expected || !knownToolCancelReasonValue(name) {
					seen[name] = struct{}{}
				}
			}
		}
	}
	return sortedKeys(seen)
}

func knownToolCancelReasonValue(reason string) bool {
	_, ok := knownToolCancelReasonValues[strings.TrimSpace(reason)]
	return ok
}

func unknownToolRunStatusesFromCoverage(diff auditDiff) []string {
	seen := map[string]struct{}{}
	for _, category := range diff.Categories {
		switch category.Name {
		case "tool-run-statuses":
			for _, status := range changedDiffValues(category) {
				if toolRunStatusAreas[status] == "" || !knownToolRunStatusValue(status) {
					seen[status] = struct{}{}
				}
			}
		case "tool-run-coverage":
			for _, value := range changedDiffValues(category) {
				name := coverageName(value)
				actual := coverageValue(value)
				expected := toolRunStatusAreas[name]
				if expected == "" || actual == "unknown" || actual != expected || !knownToolRunStatusValue(name) {
					seen[name] = struct{}{}
				}
			}
		}
	}
	return sortedKeys(seen)
}

func knownToolRunStatusValue(status string) bool {
	_, ok := knownToolRunStatusValues[strings.TrimSpace(status)]
	return ok
}

func unknownToolCatalogMarkersFromCoverage(diff auditDiff) []string {
	seen := map[string]struct{}{}
	for _, category := range diff.Categories {
		switch category.Name {
		case "tool-catalog-markers":
			for _, marker := range changedDiffValues(category) {
				if toolCatalogMarkers[marker] == "" || !knownToolCatalogMarkerValue(marker) {
					seen[marker] = struct{}{}
				}
			}
		case "tool-catalog-coverage":
			for _, value := range changedDiffValues(category) {
				name := coverageName(value)
				actual := coverageValue(value)
				expected := toolCatalogMarkers[name]
				if expected == "" || actual == "unknown" || actual != expected || !knownToolCatalogMarkerValue(name) {
					seen[name] = struct{}{}
				}
			}
		}
	}
	return sortedKeys(seen)
}

func knownToolCatalogMarkerValue(marker string) bool {
	_, ok := knownToolCatalogMarkerValues[strings.TrimSpace(marker)]
	return ok
}

func knownThreadReaderMarkerValue(marker string) bool {
	_, ok := knownThreadReaderMarkerValues[strings.TrimSpace(marker)]
	return ok
}

func unknownStreamJSONMarkersFromCoverage(diff auditDiff) []string {
	seen := map[string]struct{}{}
	for _, category := range diff.Categories {
		switch category.Name {
		case "stream-json-markers":
			for _, marker := range changedDiffValues(category) {
				if streamJSONMarkers[marker] == "" {
					seen[marker] = struct{}{}
				}
			}
		case "stream-json-coverage":
			for _, value := range changedDiffValues(category) {
				name := coverageName(value)
				actual := coverageValue(value)
				expected := streamJSONMarkers[name]
				if expected == "" || actual == "unknown" || actual != expected {
					seen[name] = struct{}{}
				}
			}
		}
	}
	return sortedKeys(seen)
}

func unknownModeSettingMarkersFromCoverage(diff auditDiff) []string {
	seen := map[string]struct{}{}
	for _, category := range diff.Categories {
		switch category.Name {
		case "mode-setting-markers":
			for _, marker := range changedDiffValues(category) {
				if modeSettingMarkers[marker] == "" {
					seen[marker] = struct{}{}
				}
			}
		case "mode-setting-coverage":
			for _, value := range changedDiffValues(category) {
				name := coverageName(value)
				actual := coverageValue(value)
				expected := modeSettingMarkers[name]
				if expected == "" || actual == "unknown" || actual != expected {
					seen[name] = struct{}{}
				}
			}
		}
	}
	return sortedKeys(seen)
}

func unknownProviderProtocolsFromCoverage(diff auditDiff) []string {
	seen := map[string]struct{}{}
	for _, category := range diff.Categories {
		switch category.Name {
		case "provider-protocol-markers":
			for _, marker := range changedDiffValues(category) {
				if providerProtocolMarkers[marker] == "" {
					seen[marker] = struct{}{}
				}
			}
		case "provider-protocol-coverage":
			for _, value := range changedDiffValues(category) {
				name := coverageName(value)
				actual := coverageValue(value)
				expected := providerProtocolMarkers[name]
				if expected == "" || actual == "unknown" || actual != expected {
					seen[name] = struct{}{}
				}
			}
		case "provider-reasoning-rules", "provider-header-rules", "provider-feature-rules":
			for _, value := range changedDiffValues(category) {
				if !knownProviderRuleValue(category.Name, value) {
					seen[providerRuleDriftName(category.Name, value)] = struct{}{}
				}
				provider := providerRuleProviderFromDiffValue(category.Name, value)
				if strings.TrimSpace(provider) != "" && !knownProviderRuleProvider(provider) {
					seen[provider] = struct{}{}
				}
				feature := providerRuleFeatureFromDiffValue(category.Name, value)
				if strings.TrimSpace(feature) != "" && !knownProviderRuleFeature(feature) {
					seen[feature] = struct{}{}
				}
				for _, provider := range providerRuleExtraProvidersFromDiffValue(category.Name, value) {
					if strings.TrimSpace(provider) != "" && !knownProviderRuleProvider(provider) {
						seen[provider] = struct{}{}
					}
				}
				for _, header := range providerRuleHeadersFromDiffValue(category.Name, value) {
					if strings.TrimSpace(header) != "" && !knownProviderRuleHeader(header) {
						seen[header] = struct{}{}
					}
				}
				for _, beta := range providerRuleBetasFromDiffValue(category.Name, value) {
					if strings.TrimSpace(beta) != "" && !knownProviderRuleBeta(beta) {
						seen[beta] = struct{}{}
					}
				}
				for _, setting := range providerRuleSettingsFromDiffValue(category.Name, value) {
					if strings.TrimSpace(setting) != "" && !knownProviderRuleSetting(setting) {
						seen[setting] = struct{}{}
					}
				}
				for _, source := range providerRuleSourcesFromDiffValue(category.Name, value) {
					if strings.TrimSpace(source) != "" && !knownProviderRuleSource(source) {
						seen[source] = struct{}{}
					}
				}
				for _, source := range providerReasoningSourcesFromDiffValue(category.Name, value) {
					if strings.TrimSpace(source) != "" && !knownProviderReasoningSource(source) {
						seen[source] = struct{}{}
					}
				}
				tool := providerRuleToolFromDiffValue(category.Name, value)
				if strings.TrimSpace(tool) != "" && !knownProviderRuleTool(tool) {
					seen[tool] = struct{}{}
				}
				fastValue := providerRuleFastValueFromDiffValue(category.Name, value)
				if strings.TrimSpace(fastValue) != "" && !knownProviderRuleFastValue(fastValue) {
					seen[fastValue] = struct{}{}
				}
				for _, effort := range providerReasoningEffortsFromDiffValue(category.Name, value) {
					if strings.TrimSpace(effort) != "" && !knownProviderReasoningEffort(effort) {
						seen[effort] = struct{}{}
					}
				}
			}
		}
	}
	return sortedKeys(seen)
}

func knownProviderRuleValue(categoryName, value string) bool {
	switch categoryName {
	case "provider-reasoning-rules":
		_, ok := knownProviderReasoningRuleValues[value]
		return ok
	case "provider-header-rules":
		_, ok := knownProviderHeaderRuleValues[value]
		return ok
	case "provider-feature-rules":
		_, ok := knownProviderFeatureRuleValues[value]
		return ok
	default:
		return true
	}
}

func providerRuleDriftName(categoryName, value string) string {
	switch categoryName {
	case "provider-feature-rules":
		feature, _, _ := strings.Cut(value, "|")
		provider := providerRuleProviderFromDiffValue(categoryName, value)
		if strings.TrimSpace(provider) != "" {
			return feature + "/" + provider
		}
		return feature
	case "provider-header-rules", "provider-reasoning-rules":
		name, _, _ := strings.Cut(value, "|")
		return name
	default:
		return value
	}
}

func unknownAgentModesFromCoverage(diff auditDiff) []string {
	seen := map[string]struct{}{}
	for _, category := range diff.Categories {
		switch category.Name {
		case "agent-mode-profiles", "agent-mode-routes", "agent-mode-coverage":
			for _, item := range changedDiffValues(category) {
				name := agentModeNameFromDiffValue(item)
				if agentModeScopes[name] == "" {
					seen[name] = struct{}{}
					continue
				}
				if category.Name == "agent-mode-coverage" {
					actual := coverageValue(item)
					if actual == "unknown" || actual != agentModeScopes[name] {
						seen[name] = struct{}{}
					}
				}
			}
		}
	}
	return sortedKeys(seen)
}

func unknownAgentModeInternalsFromCoverage(diff auditDiff) []string {
	seen := map[string]struct{}{}
	for _, category := range diff.Categories {
		switch category.Name {
		case "agent-mode-profiles":
			for _, value := range changedDiffValues(category) {
				name := agentModeNameFromDiffValue(value)
				if _, ok := knownAgentModeProfileValues[value]; !ok {
					seen[strings.TrimSpace(name)+" profile"] = struct{}{}
				}
				if strings.TrimSpace(name) != "" && agentModeScopes[name] == "" {
					seen[name] = struct{}{}
				}
				primary := pipeFieldValue(value, "primary")
				if strings.TrimSpace(primary) != "" && !knownAgentModePrimary(primary) {
					seen["primary="+primary] = struct{}{}
				}
				if effort := pipeFieldValue(value, "reasoning"); strings.TrimSpace(effort) != "" && !knownAgentModeReasoningEffort(effort) {
					seen["reasoning="+effort] = struct{}{}
				}
				for _, effort := range commaFieldValues(value, "levels") {
					if !knownAgentModeReasoningEffort(effort) || strings.TrimSpace(effort) == "none" {
						seen["level="+effort] = struct{}{}
					}
				}
				if include := pipeFieldValue(value, "include"); strings.TrimSpace(include) != "" && !knownAgentModeIncludeTools(include) {
					seen["include="+include] = struct{}{}
				}
				for _, field := range []string{"deferred", "visible", "visibleInV2", "serverOnly"} {
					if flag := pipeFieldValue(value, field); strings.TrimSpace(flag) != "" && !knownBooleanString(flag) {
						seen[field+"="+flag] = struct{}{}
					}
				}
				if !knownAgentModeProfileFlags(name, pipeFieldValue(value, "visible"), pipeFieldValue(value, "visibleInV2"), pipeFieldValue(value, "serverOnly")) {
					seen[name+" visibility"] = struct{}{}
				}
			}
		case "agent-mode-routes":
			for _, value := range changedDiffValues(category) {
				name := agentModeNameFromDiffValue(value)
				if _, ok := knownAgentModeRouteValues[value]; !ok {
					seen[strings.TrimSpace(name)+" route"] = struct{}{}
				}
				if strings.TrimSpace(name) != "" && agentModeScopes[name] == "" {
					seen[name] = struct{}{}
				}
				provider := pipeFieldValue(value, "provider")
				if strings.TrimSpace(provider) != "" && !knownModelLimitProvider(provider) {
					seen["provider="+provider] = struct{}{}
				}
				model := pipeFieldValue(value, "model")
				if strings.TrimSpace(model) != "" && !knownAgentModeRouteModel(provider, model) {
					seen["model="+model] = struct{}{}
				}
				primary := pipeFieldValue(value, "primary")
				if strings.TrimSpace(primary) != "" && !knownAgentModePrimary(primary) {
					seen["primary="+primary] = struct{}{}
				}
				if effort := pipeFieldValue(value, "reasoning"); strings.TrimSpace(effort) != "" && !knownAgentModeReasoningEffort(effort) {
					seen["reasoning="+effort] = struct{}{}
				}
				for _, field := range []string{"context", "max_out"} {
					parsed, ok := parsePositiveIntField(pipeFieldValue(value, field))
					if !ok || field == "context" && !knownModelLimitContext(parsed) || field == "max_out" && !knownModelLimitMaxOutput(parsed) {
						seen[field+"="+pipeFieldValue(value, field)] = struct{}{}
					}
				}
				if name == "large" {
					if effectiveContext := pipeFieldValue(value, "effective_context"); strings.TrimSpace(effectiveContext) != "" && !knownLargeContextNumericField("context", effectiveContext) {
						seen["effective_context="+effectiveContext] = struct{}{}
					}
					if effectiveMaxInput := pipeFieldValue(value, "effective_max_input"); strings.TrimSpace(effectiveMaxInput) != "" && !knownLargeContextNumericField("max_input", effectiveMaxInput) {
						seen["effective_max_input="+effectiveMaxInput] = struct{}{}
					}
					if alias := pipeFieldValue(value, "large_alias"); strings.TrimSpace(alias) != "" && !knownLargeContextAlias(alias) {
						seen["large_alias="+alias] = struct{}{}
					}
				}
			}
		}
	}
	return sortedKeys(seen)
}

func knownAgentModePrimary(primary string) bool {
	switch strings.TrimSpace(primary) {
	case "AMP_NOSTROMO", "CLAUDE_OPUS_4_6", "CLAUDE_OPUS_4_7", "GPT_5_5":
		return true
	default:
		return false
	}
}

func knownAgentModeReasoningEffort(effort string) bool {
	switch strings.TrimSpace(effort) {
	case "none", "low", "medium", "high", "xhigh", "max":
		return true
	default:
		return false
	}
}

func knownAgentModeIncludeTools(include string) bool {
	switch strings.TrimSpace(include) {
	case "present":
		return true
	default:
		return false
	}
}

func knownBooleanString(value string) bool {
	switch strings.TrimSpace(value) {
	case "true", "false":
		return true
	default:
		return false
	}
}

func knownAgentModeProfileFlags(name, visible, visibleInV2, serverOnly string) bool {
	type flags struct {
		visible     string
		visibleInV2 string
		serverOnly  string
	}
	expected := map[string]flags{
		"agg-man":  {visible: "false", visibleInV2: "false", serverOnly: "true"},
		"deep":     {visible: "true", visibleInV2: "true", serverOnly: "false"},
		"large":    {visible: "true", visibleInV2: "false", serverOnly: "false"},
		"nostromo": {visible: "true", visibleInV2: "true", serverOnly: "false"},
		"rush":     {visible: "true", visibleInV2: "false", serverOnly: "false"},
		"smart":    {visible: "true", visibleInV2: "true", serverOnly: "false"},
	}
	want, ok := expected[strings.TrimSpace(name)]
	if !ok {
		return false
	}
	return strings.TrimSpace(visible) == want.visible &&
		strings.TrimSpace(visibleInV2) == want.visibleInV2 &&
		strings.TrimSpace(serverOnly) == want.serverOnly
}

func knownAgentModeRouteModel(expectedProvider, model string) bool {
	provider, family := modelProviderAndFamily(model)
	if provider == "unknown" || family == "unknown" || provider != strings.TrimSpace(expectedProvider) {
		return false
	}
	return knownModelLimitNameShape(provider, model)
}

func unknownModelsFromCoverage(diff auditDiff) []string {
	seen := map[string]struct{}{}
	for _, category := range diff.Categories {
		switch category.Name {
		case "models":
			for _, model := range changedDiffValues(category) {
				provider, family := modelProviderAndFamily(model)
				if provider == "unknown" || family == "unknown" {
					seen[model] = struct{}{}
				}
			}
		case "model-coverage":
			for _, value := range changedDiffValues(category) {
				name := coverageName(value)
				provider, family, ok := strings.Cut(coverageValue(value), "/")
				expectedProvider, expectedFamily := modelProviderAndFamily(name)
				if !ok || provider == "unknown" || family == "unknown" ||
					expectedProvider == "unknown" || expectedFamily == "unknown" ||
					provider != expectedProvider || family != expectedFamily {
					seen[name] = struct{}{}
				}
			}
		case "model-limits":
			for _, value := range changedDiffValues(category) {
				if name, unknown := unknownModelLimitName(value); unknown {
					seen[name] = struct{}{}
				}
			}
		case "large-context-rules":
			for _, value := range changedDiffValues(category) {
				alias := pipeFieldValue(value, "alias")
				provider, family := modelProviderAndFamily(alias)
				if provider == "unknown" || family == "unknown" {
					seen[alias] = struct{}{}
				}
			}
		case "adaptive-thinking-rules":
			for _, value := range changedDiffValues(category) {
				for _, model := range commaFieldValues(value, "models") {
					provider, family := modelProviderAndFamily(model)
					if provider == "unknown" || family == "unknown" {
						seen[model] = struct{}{}
					}
				}
			}
		case "provider-reasoning-rules":
			for _, value := range changedDiffValues(category) {
				model := providerReasoningSpecialModelFromDiffValue(value)
				if strings.TrimSpace(model) == "" {
					continue
				}
				provider, family := modelProviderAndFamily(model)
				if provider == "unknown" || family == "unknown" {
					seen[model] = struct{}{}
				}
			}
		case "agent-mode-routes":
			for _, value := range changedDiffValues(category) {
				model := agentModeRouteModelFromDiffValue(value)
				provider, family := modelProviderAndFamily(model)
				if provider == "unknown" || family == "unknown" {
					seen[model] = struct{}{}
				}
			}
		}
	}
	return sortedKeys(seen)
}

func unknownAdaptiveThinkingInternalsFromCoverage(diff auditDiff) []string {
	seen := map[string]struct{}{}
	for _, category := range diff.Categories {
		if category.Name != "adaptive-thinking-rules" {
			continue
		}
		for _, value := range changedDiffValues(category) {
			if _, ok := knownAdaptiveThinkingRuleValues[value]; !ok {
				name, _, _ := strings.Cut(value, "|")
				seen[strings.TrimSpace(name)+" adaptive"] = struct{}{}
			}
			for _, effort := range commaFieldValues(value, "levels") {
				if !knownAdaptiveThinkingEffort(effort) {
					seen[effort] = struct{}{}
				}
			}
			if effort := pipeFieldValue(value, "default"); strings.TrimSpace(effort) != "" && !knownAdaptiveThinkingEffort(effort) {
				seen[effort] = struct{}{}
			}
			if thinkingType := pipeFieldValue(value, "type"); strings.TrimSpace(thinkingType) != "" && !knownAdaptiveThinkingType(thinkingType) {
				seen[thinkingType] = struct{}{}
			}
			if display := pipeFieldValue(value, "display"); strings.TrimSpace(display) != "" && !knownAdaptiveThinkingDisplay(display) {
				seen[display] = struct{}{}
			}
			if outputConfig := pipeFieldValue(value, "output_config"); strings.TrimSpace(outputConfig) != "" && !knownAdaptiveThinkingOutputConfig(outputConfig) {
				seen["output_config="+outputConfig] = struct{}{}
			}
		}
	}
	return sortedKeys(seen)
}

func knownAdaptiveThinkingEffort(effort string) bool {
	switch strings.TrimSpace(effort) {
	case "low", "medium", "high", "xhigh", "max":
		return true
	default:
		return false
	}
}

func knownAdaptiveThinkingType(thinkingType string) bool {
	return strings.TrimSpace(thinkingType) == "adaptive"
}

func knownAdaptiveThinkingDisplay(display string) bool {
	return strings.TrimSpace(display) == "summarized"
}

func knownAdaptiveThinkingOutputConfig(outputConfig string) bool {
	return strings.TrimSpace(outputConfig) == "true"
}

func unknownLargeContextInternalsFromCoverage(diff auditDiff) []string {
	seen := map[string]struct{}{}
	for _, category := range diff.Categories {
		if category.Name != "large-context-rules" {
			continue
		}
		for _, value := range changedDiffValues(category) {
			if _, ok := knownLargeContextRuleValues[value]; !ok {
				primary := agentModeNameFromDiffValue(value)
				seen[strings.TrimSpace(primary)+" large-context"] = struct{}{}
			}
			primary := agentModeNameFromDiffValue(value)
			if strings.TrimSpace(primary) != "" && !knownLargeContextPrimary(primary) {
				seen[primary] = struct{}{}
			}
			alias := pipeFieldValue(value, "alias")
			if strings.TrimSpace(alias) != "" && !knownLargeContextAlias(alias) {
				seen[alias] = struct{}{}
			}
			for _, field := range []string{"context", "max_out", "max_input"} {
				fieldValue := pipeFieldValue(value, field)
				if strings.TrimSpace(fieldValue) != "" && !knownLargeContextNumericField(field, fieldValue) {
					seen[field+"="+fieldValue] = struct{}{}
				}
			}
			if requiresEnable := pipeFieldValue(value, "requires_enable"); strings.TrimSpace(requiresEnable) != "" && !knownLargeContextRequiresEnable(requiresEnable) {
				seen["requires_enable="+requiresEnable] = struct{}{}
			}
		}
	}
	return sortedKeys(seen)
}

func knownLargeContextPrimary(primary string) bool {
	return strings.TrimSpace(primary) == "CLAUDE_OPUS_4_6"
}

func knownLargeContextAlias(alias string) bool {
	return strings.TrimSpace(alias) == "claude-opus-4-6-1m"
}

func knownLargeContextNumericField(field, value string) bool {
	parsed, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil {
		return false
	}
	switch field {
	case "context":
		return parsed == 1000000
	case "max_out":
		return parsed == 32000
	case "max_input":
		return parsed == 968000
	default:
		return false
	}
}

func knownLargeContextRequiresEnable(requiresEnable string) bool {
	return strings.TrimSpace(requiresEnable) == "true"
}

func unknownModelLimitInternalsFromCoverage(diff auditDiff) []string {
	seen := map[string]struct{}{}
	for _, category := range diff.Categories {
		if category.Name != "model-limits" {
			continue
		}
		for _, value := range changedDiffValues(category) {
			name := agentModeNameFromDiffValue(value)
			provider := pipeFieldValue(value, "provider")
			enum := pipeFieldValue(value, "enum")
			display := pipeFieldValue(value, "display")
			if strings.TrimSpace(enum) == "" || !knownModelLimitEnum(provider, enum) {
				seen["enum="+enum] = struct{}{}
			}
			if strings.TrimSpace(name) == "" || !knownModelLimitNameShape(provider, name) {
				seen[name] = struct{}{}
			}
			if strings.TrimSpace(display) == "" {
				seen[name+" display"] = struct{}{}
			}
			context, contextOK := parsePositiveIntField(pipeFieldValue(value, "context"))
			if !contextOK || !knownModelLimitContext(context) {
				seen["context="+pipeFieldValue(value, "context")] = struct{}{}
			}
			maxOutput, maxOutputOK := parsePositiveIntField(pipeFieldValue(value, "max_out"))
			if !maxOutputOK || !knownModelLimitMaxOutput(maxOutput) {
				seen["max_out="+pipeFieldValue(value, "max_out")] = struct{}{}
			}
			if contextOK && maxOutputOK && context < maxOutput {
				seen["context<max_out"] = struct{}{}
			}
			if contextOK && maxOutputOK {
				if expected, ok := knownModelLimitValues[name]; ok {
					if strings.TrimSpace(enum) != expected.Enum {
						seen["enum="+enum] = struct{}{}
					}
					if strings.TrimSpace(provider) != expected.Provider {
						seen[name+" provider="+provider] = struct{}{}
					}
					if strings.TrimSpace(display) != expected.DisplayName {
						seen[name+" display="+display] = struct{}{}
					}
					if context != expected.ContextWindow {
						seen["context="+pipeFieldValue(value, "context")] = struct{}{}
					}
					if maxOutput != expected.MaxOutputTokens {
						seen["max_out="+pipeFieldValue(value, "max_out")] = struct{}{}
					}
				} else {
					seen[name+" limit"] = struct{}{}
				}
			}
		}
	}
	return sortedKeys(seen)
}

func knownModelLimitEnum(provider, enum string) bool {
	enum = strings.TrimSpace(enum)
	switch strings.TrimSpace(provider) {
	case "anthropic":
		return strings.HasPrefix(enum, "CLAUDE_")
	case "baseten":
		return strings.HasPrefix(enum, "BASETEN_")
	case "cerebras":
		return strings.HasPrefix(enum, "Z_AI_")
	case "fireworks":
		return strings.HasPrefix(enum, "FIREWORKS_")
	case "google":
		return strings.HasPrefix(enum, "GEMINI")
	case "moonshotai":
		return strings.HasPrefix(enum, "KIMI_")
	case "openai":
		return strings.HasPrefix(enum, "GPT_") || enum == "O3" || strings.HasPrefix(enum, "O3_") || strings.HasPrefix(enum, "O4_") || strings.HasPrefix(enum, "AMP_")
	case "openrouter":
		return strings.HasPrefix(enum, "OPENROUTER_") || strings.HasPrefix(enum, "SONOMA_")
	case "xai":
		return strings.HasPrefix(enum, "GROK_")
	default:
		return false
	}
}

func knownModelLimitNameShape(provider, name string) bool {
	name = strings.TrimSpace(name)
	switch strings.TrimSpace(provider) {
	case "anthropic":
		return strings.HasPrefix(name, "claude-")
	case "baseten":
		return strings.Contains(name, "/")
	case "cerebras":
		return strings.HasPrefix(name, "zai-")
	case "fireworks":
		return strings.HasPrefix(name, "accounts/fireworks/models/")
	case "google":
		return strings.HasPrefix(name, "gemini-")
	case "moonshotai":
		return strings.HasPrefix(name, "kimi-")
	case "openai":
		return strings.HasPrefix(name, "gpt-") || strings.HasPrefix(name, "o3") || strings.HasPrefix(name, "o4") || strings.HasPrefix(name, "openai/") || strings.HasPrefix(name, "amp-")
	case "openrouter":
		return strings.Contains(name, "/") || strings.HasPrefix(name, "sonoma-")
	case "xai":
		return strings.HasPrefix(name, "grok-")
	default:
		return false
	}
}

func parsePositiveIntField(value string) (int, bool) {
	parsed, err := strconv.Atoi(strings.TrimSpace(value))
	return parsed, err == nil && parsed > 0
}

func knownModelLimitContext(context int) bool {
	switch context {
	case 128000, 131000, 162752, 200000, 202800, 230144, 256000, 262144, 332000, 400000, 1000000, 1048576, 1050000:
		return true
	default:
		return false
	}
}

func knownModelLimitMaxOutput(maxOutput int) bool {
	switch maxOutput {
	case 1, 32000, 40000, 64000, 65535, 128000:
		return true
	default:
		return false
	}
}

func unknownCompactionRuleInternalsFromCoverage(diff auditDiff) []string {
	seen := map[string]struct{}{}
	for _, category := range diff.Categories {
		if category.Name != "compaction-rules" {
			continue
		}
		for _, value := range changedDiffValues(category) {
			if _, ok := knownCompactionRuleValues[value]; !ok {
				name, _, _ := strings.Cut(value, "|")
				seen[strings.TrimSpace(name)+" compaction"] = struct{}{}
			}
			name, _, _ := strings.Cut(value, "|")
			if strings.TrimSpace(name) != "" && !knownCompactionRuleName(name) {
				seen[name] = struct{}{}
			}
			provider := pipeFieldValue(value, "provider")
			if strings.TrimSpace(provider) != "" && !knownProviderRuleProvider(provider) {
				seen[provider] = struct{}{}
			}
			for _, field := range []string{"trigger", "timing", "summary_prompt", "history_role", "tail_assistant"} {
				fieldValue := pipeFieldValue(value, field)
				if strings.TrimSpace(fieldValue) != "" && !knownCompactionRuleFieldValue(field, fieldValue) {
					seen[field+"="+fieldValue] = struct{}{}
				}
			}
			if threshold := pipeFieldValue(value, "threshold"); strings.TrimSpace(threshold) != "" && !knownCompactionThreshold(threshold) {
				seen["threshold="+threshold] = struct{}{}
			}
			for _, field := range commaFieldValues(value, "usage") {
				if !knownCompactionUsageField(field) {
					seen[field] = struct{}{}
				}
			}
			helper := pipeFieldValue(value, "helper")
			if strings.TrimSpace(helper) != "" {
				header, helperValue, ok := strings.Cut(helper, ":")
				if !ok || !knownCompactionHelperHeader(header) {
					seen["helper="+helper] = struct{}{}
				}
				if ok && !knownCompactionHelperValue(helperValue) {
					seen["helper="+helper] = struct{}{}
				}
			}
		}
	}
	return sortedKeys(seen)
}

func knownCompactionRuleName(name string) bool {
	return strings.TrimSpace(name) == "anthropic-tool-runner"
}

func knownCompactionRuleFieldValue(field, value string) bool {
	switch field {
	case "trigger":
		return strings.TrimSpace(value) == "observed-usage"
	case "timing":
		return strings.TrimSpace(value) == "post-response"
	case "summary_prompt":
		return strings.TrimSpace(value) == "continuation-summary"
	case "history_role":
		return strings.TrimSpace(value) == "user"
	case "tail_assistant":
		return strings.TrimSpace(value) == "strip-tool-use-blocks"
	default:
		return false
	}
}

func knownCompactionThreshold(threshold string) bool {
	parsed, err := strconv.Atoi(strings.TrimSpace(threshold))
	return err == nil && parsed == 100000
}

func knownCompactionUsageField(field string) bool {
	switch strings.TrimSpace(field) {
	case "input_tokens", "cache_creation_input_tokens", "cache_read_input_tokens", "output_tokens":
		return true
	default:
		return false
	}
}

func knownCompactionHelperHeader(header string) bool {
	return strings.TrimSpace(header) == "x-stainless-helper"
}

func knownCompactionHelperValue(value string) bool {
	return strings.TrimSpace(value) == "compaction"
}

func unknownModelLimitName(value string) (string, bool) {
	name := agentModeNameFromDiffValue(value)
	if strings.TrimSpace(name) == "" {
		name = value
	}
	if _, ok := knownModelLimitValues[name]; ok {
		return "", false
	}
	provider := pipeFieldValue(value, "provider")
	if !knownModelLimitProvider(provider) {
		return name, true
	}
	switch provider {
	case "anthropic", "google", "openai":
		inferredProvider, family := modelProviderAndFamily(name)
		if inferredProvider != provider || family == "unknown" {
			return name, true
		}
	}
	return "", false
}

func knownModelLimitProvider(provider string) bool {
	switch strings.TrimSpace(provider) {
	case "anthropic", "baseten", "cerebras", "fireworks", "google", "moonshotai", "openai", "openrouter", "xai":
		return true
	default:
		return false
	}
}

func unknownActorsFromCoverage(diff auditDiff) []string {
	seen := map[string]struct{}{}
	for _, category := range diff.Categories {
		switch category.Name {
		case "actor-runtime-markers":
			for _, marker := range changedDiffValues(category) {
				if actorMarkerAreas[marker] == "" || !knownActorMarkerValue(marker) {
					seen[marker] = struct{}{}
				}
			}
		case "actor-runtime-coverage":
			for _, value := range changedDiffValues(category) {
				name := coverageName(value)
				actual := coverageValue(value)
				expected := actorMarkerAreas[name]
				if expected == "" || actual == "unknown" || actual != expected || !knownActorMarkerValue(name) {
					seen[name] = struct{}{}
				}
			}
		}
	}
	return sortedKeys(seen)
}

func knownActorMarkerValue(marker string) bool {
	_, ok := knownActorMarkerValues[strings.TrimSpace(marker)]
	return ok
}

func changedDiffValues(category categoryDiff) []string {
	values := append([]string{}, category.Added...)
	return append(values, category.Removed...)
}

func agentModeNameFromDiffValue(value string) string {
	if name, _, ok := strings.Cut(value, "|"); ok {
		return name
	}
	return coverageName(value)
}

func agentModeRouteModelFromDiffValue(value string) string {
	model := pipeFieldValue(value, "model")
	if strings.TrimSpace(model) != "" {
		return model
	}
	return pipeFieldValue(value, "primary")
}

func providerReasoningSpecialModelFromDiffValue(value string) string {
	special := pipeFieldValue(value, "special")
	if strings.TrimSpace(special) == "" {
		return ""
	}
	_, rest, ok := strings.Cut(special, "/")
	if !ok {
		return ""
	}
	model, _, _ := strings.Cut(rest, ":")
	return model
}

func providerRuleProviderFromDiffValue(categoryName, value string) string {
	if categoryName == "provider-feature-rules" {
		return pipeFieldValue(value, "provider")
	}
	provider, _, _ := strings.Cut(value, "|")
	return strings.TrimSpace(provider)
}

func providerRuleFeatureFromDiffValue(categoryName, value string) string {
	switch categoryName {
	case "provider-feature-rules":
		feature, _, _ := strings.Cut(value, "|")
		return strings.TrimSpace(feature)
	case "provider-header-rules":
		raw := pipeFieldValue(value, "feature")
		_, feature, ok := strings.Cut(raw, ":")
		if ok {
			return strings.TrimSpace(feature)
		}
		return strings.TrimSpace(raw)
	default:
		return ""
	}
}

func providerRuleHeaderFromDiffValue(categoryName, value string) string {
	switch categoryName {
	case "provider-feature-rules":
		return pipeFieldValue(value, "header")
	case "provider-header-rules":
		raw := pipeFieldValue(value, "feature")
		header, _, ok := strings.Cut(raw, ":")
		if ok {
			return strings.TrimSpace(header)
		}
		return strings.TrimSpace(raw)
	default:
		return ""
	}
}

func providerRuleHeadersFromDiffValue(categoryName, value string) []string {
	var headers []string
	if header := providerRuleHeaderFromDiffValue(categoryName, value); strings.TrimSpace(header) != "" {
		headers = append(headers, header)
	}
	if categoryName != "provider-header-rules" {
		return headers
	}
	for _, field := range []string{"thread_id", "message_id", "override"} {
		header, _, _ := strings.Cut(pipeFieldValue(value, field), "<-")
		if strings.TrimSpace(header) != "" {
			headers = append(headers, strings.TrimSpace(header))
		}
	}
	if header := pipeFieldValue(value, "beta_header"); strings.TrimSpace(header) != "" {
		headers = append(headers, strings.TrimSpace(header))
	}
	return headers
}

func providerRuleBetasFromDiffValue(categoryName, value string) []string {
	if categoryName != "provider-header-rules" {
		return nil
	}
	var betas []string
	for _, field := range []string{"interleaved", "fast"} {
		beta, _, _ := strings.Cut(pipeFieldValue(value, field), "@")
		if strings.TrimSpace(beta) != "" {
			betas = append(betas, strings.TrimSpace(beta))
		}
	}
	return betas
}

func providerRuleSettingsFromDiffValue(categoryName, value string) []string {
	if categoryName == "provider-reasoning-rules" {
		if setting := pipeFieldValue(value, "setting"); strings.TrimSpace(setting) != "" {
			return []string{strings.TrimSpace(setting)}
		}
		return nil
	}
	if categoryName != "provider-header-rules" {
		return nil
	}
	var settings []string
	if _, settingValues, ok := strings.Cut(pipeFieldValue(value, "interleaved"), "@"); ok {
		for _, setting := range strings.Split(settingValues, "+") {
			setting = strings.TrimSpace(setting)
			if setting == "" || strings.HasPrefix(setting, "skip_adaptive=") {
				continue
			}
			settings = append(settings, setting)
		}
	}
	if _, setting, ok := strings.Cut(pipeFieldValue(value, "override"), "<-"); ok && strings.TrimSpace(setting) != "" {
		settings = append(settings, strings.TrimSpace(setting))
	}
	if _, rest, ok := strings.Cut(pipeFieldValue(value, "fast"), "@"); ok {
		setting, _, _ := strings.Cut(rest, "=")
		if strings.TrimSpace(setting) != "" {
			settings = append(settings, strings.TrimSpace(setting))
		}
	}
	return settings
}

func providerReasoningSourcesFromDiffValue(categoryName, value string) []string {
	if categoryName != "provider-reasoning-rules" {
		return nil
	}
	return commaFieldValues(value, "sources")
}

func providerReasoningEffortsFromDiffValue(categoryName, value string) []string {
	if categoryName != "provider-reasoning-rules" {
		return nil
	}
	var efforts []string
	if effort := pipeFieldValue(value, "default"); strings.TrimSpace(effort) != "" {
		efforts = append(efforts, strings.TrimSpace(effort))
	}
	if special := pipeFieldValue(value, "special"); strings.TrimSpace(special) != "" {
		_, effort, ok := strings.Cut(special, ":")
		if !ok {
			efforts = append(efforts, strings.TrimSpace(special))
		} else if strings.TrimSpace(effort) != "" {
			efforts = append(efforts, strings.TrimSpace(effort))
		}
	}
	return efforts
}

func providerRuleSourcesFromDiffValue(categoryName, value string) []string {
	if categoryName != "provider-header-rules" {
		return nil
	}
	var sources []string
	for _, field := range []string{"thread_id", "message_id"} {
		_, source, ok := strings.Cut(pipeFieldValue(value, field), "<-")
		if ok && strings.TrimSpace(source) != "" {
			sources = append(sources, strings.TrimSpace(source))
		}
	}
	return sources
}

func providerRuleExtraProvidersFromDiffValue(categoryName, value string) []string {
	if categoryName != "provider-header-rules" {
		return nil
	}
	_, provider, ok := strings.Cut(pipeFieldValue(value, "fast"), "=>")
	if ok && strings.TrimSpace(provider) != "" {
		return []string{strings.TrimSpace(provider)}
	}
	return nil
}

func providerRuleFastValueFromDiffValue(categoryName, value string) string {
	if categoryName != "provider-header-rules" {
		return ""
	}
	_, rest, ok := strings.Cut(pipeFieldValue(value, "fast"), "@")
	if !ok {
		return ""
	}
	_, valueAndProvider, ok := strings.Cut(rest, "=")
	if !ok {
		return ""
	}
	valueOnly, _, _ := strings.Cut(valueAndProvider, "=>")
	return strings.TrimSpace(valueOnly)
}

func providerRuleToolFromDiffValue(categoryName, value string) string {
	if categoryName != "provider-feature-rules" {
		return ""
	}
	return pipeFieldValue(value, "tool")
}

func knownProviderRuleProvider(provider string) bool {
	switch strings.TrimSpace(provider) {
	case "anthropic", "google", "openai", "vertexai":
		return true
	default:
		return false
	}
}

func knownProviderRuleFeature(feature string) bool {
	return providerProtocolMarkers[strings.TrimSpace(feature)] == "amp-feature"
}

func knownProviderRuleHeader(header string) bool {
	switch providerProtocolMarkers[strings.TrimSpace(header)] {
	case "amp-provider-header", "anthropic-header":
		return true
	default:
		return false
	}
}

func knownProviderRuleBeta(beta string) bool {
	return providerProtocolMarkers[strings.TrimSpace(beta)] == "anthropic-beta"
}

func knownProviderRuleSetting(setting string) bool {
	return settingScopes[strings.TrimSpace(setting)] != ""
}

func knownProviderRuleSource(source string) bool {
	switch strings.TrimSpace(source) {
	case "message-id-argument", "thread.id":
		return true
	default:
		return false
	}
}

func knownProviderReasoningSource(source string) bool {
	source = strings.TrimSpace(source)
	if strings.HasPrefix(source, "setting:") {
		setting := strings.TrimPrefix(source, "setting:")
		return settingScopes[setting] != "" || modeSettingMarkers[setting] != ""
	}
	switch source {
	case "mode:reasoningEffort", "model-default", "provider-default":
		return true
	default:
		return false
	}
}

func knownProviderRuleTool(tool string) bool {
	switch strings.TrimSpace(tool) {
	case "code_review", "painter", "read_thread":
		return true
	default:
		return false
	}
}

func knownProviderRuleFastValue(value string) bool {
	return strings.TrimSpace(value) == "fast"
}

func knownProviderReasoningEffort(effort string) bool {
	switch strings.TrimSpace(effort) {
	case "low", "medium", "high":
		return true
	default:
		return false
	}
}

func pipeFieldValue(value, field string) string {
	prefix := field + "="
	for _, part := range strings.Split(value, "|") {
		if strings.HasPrefix(part, prefix) {
			return strings.TrimPrefix(part, prefix)
		}
	}
	return ""
}

func commaFieldValues(value, field string) []string {
	raw := pipeFieldValue(value, field)
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	var values []string
	for _, part := range strings.Split(raw, ",") {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			values = append(values, trimmed)
		}
	}
	return values
}

func coverageName(value string) string {
	name, _, ok := strings.Cut(value, "=")
	if !ok {
		return value
	}
	return name
}

func coverageValue(value string) string {
	index := strings.LastIndex(value, "=")
	if index < 0 {
		return ""
	}
	return value[index+1:]
}

func shortHash(hash string) string {
	if len(hash) <= 12 {
		return hash
	}
	return hash[:12]
}
