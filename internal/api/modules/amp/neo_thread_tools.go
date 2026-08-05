package amp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
)

type neoExclusiveThreadCreationContextKey struct{}

func isNeoThreadTool(name string) bool {
	switch strings.TrimSpace(name) {
	case "find_thread", "list_agent_modes", "list_runners", "create_thread", "get_current_user_identity", "thread_interact", "get_thread_metadata", "update_thread", "rename_thread", "set_thread_pinned", "add_thread_labels", "remove_thread_labels", "archive_current_thread", "archive_thread", "archive_threads", "unarchive_thread", "send_message_to_thread", "download_thread_file", "upload_thread_file", "get_schedule", "set_schedule", "update_schedule", "clear_schedule":
		return true
	default:
		return false
	}
}

func neoThreadToolSpec(toolName string) (neoToolSpec, bool) {
	name := strings.TrimSpace(toolName)
	threadID := map[string]any{"type": "string", "description": "Amp thread ID or thread URL."}
	switch name {
	case "find_thread":
		return neoToolSpec{
			Name:        name,
			Description: "Search the signed-in user's Amp threads by title, message text, repository, project, file, label, or task terms. Returns concise thread summaries and IDs; use read_thread when exact conversation details are needed.",
			InputSchema: neoThreadToolSchema(map[string]any{
				"query":  map[string]any{"type": "string", "description": "Search terms. Use author:me to list the user's threads without text terms."},
				"limit":  map[string]any{"type": "integer", "description": "Maximum results from 1 to 50. Default: 20."},
				"offset": map[string]any{"type": "integer", "description": "Result offset. Default: 0."},
				"time":   map[string]any{"type": "string", "description": "Optional recent-activity window supported by Amp, such as 7d or 24h."},
			}, []any{"query"}),
			Meta: map[string]any{"source": "server"},
		}, true
	case "list_agent_modes":
		return neoToolSpec{
			Name:        name,
			Description: "List the built-in and plugin-provided Amp agent modes available for new threads.",
			InputSchema: neoThreadToolSchema(map[string]any{}, nil),
			Meta:        map[string]any{"source": "server"},
		}, true
	case "list_runners":
		return neoToolSpec{
			Name:        name,
			Description: "List Amp runners currently available to start threads, including their IDs, hosts, and working directories.",
			InputSchema: neoThreadToolSchema(map[string]any{}, nil),
			Meta:        map[string]any{"source": "server"},
		}, true
	case "get_current_user_identity":
		return neoToolSpec{
			Name:        name,
			Description: "Get the signed-in user's Amp identity.",
			InputSchema: neoThreadToolSchema(map[string]any{}, nil),
			Meta:        map[string]any{"source": "server"},
		}, true
	case "create_thread":
		return neoToolSpec{
			Name:        name,
			Description: "Create a new Amp thread and relate it to the current thread. The child inherits the current project, workspace, agent mode, and executor when they are not specified. A child created by the server-only Puck mode defaults to Medium instead of creating another Puck task.",
			InputSchema: neoThreadToolSchema(map[string]any{
				"threadId":        map[string]any{"type": "string", "description": "Optional thread ID to create. Omit to create a new local thread ID."},
				"prompt":          map[string]any{"type": "string", "description": "Optional initial task for the child agent."},
				"content":         map[string]any{"type": "array", "items": map[string]any{"type": "object"}, "description": "Optional initial text and image content blocks. Use this instead of prompt when forwarding an attachment."},
				"agentMode":       map[string]any{"type": "string", "description": "Optional agent mode for the child, such as low, medium, high, or ultra."},
				"reasoningEffort": map[string]any{"type": "string", "description": "Optional reasoning effort for the selected mode."},
				"projectID":       map[string]any{"type": "string", "description": "Optional Amp project ID."},
				"runnerId":        map[string]any{"type": "string", "description": "Optional runner ID returned by list_runners."},
				"workingDirectory": map[string]any{
					"type":        "string",
					"description": "Optional workspace directory. With a runner it must match the runner workspace; otherwise it must exist on the proxy host.",
				},
				"executor": map[string]any{
					"description": "Optional executor selection as local, a runner ID, or a runner object. The string runner requires the separate top-level runnerId field.",
					"anyOf": []any{
						map[string]any{"type": "string"},
						map[string]any{
							"type": "object",
							"properties": map[string]any{
								"type": map[string]any{"type": "string", "const": "local"},
							},
							"required":             []any{"type"},
							"additionalProperties": false,
						},
						map[string]any{
							"type": "object",
							"properties": map[string]any{
								"type": map[string]any{"type": "string", "const": "runner"},
								"id":   map[string]any{"type": "string"},
							},
							"required":             []any{"type", "id"},
							"additionalProperties": false,
						},
						map[string]any{
							"type": "object",
							"properties": map[string]any{
								"type":     map[string]any{"type": "string", "const": "runner"},
								"runnerId": map[string]any{"type": "string"},
							},
							"required":             []any{"type", "runnerId"},
							"additionalProperties": false,
						},
						map[string]any{
							"type": "object",
							"properties": map[string]any{
								"type":     map[string]any{"type": "string", "const": "runner"},
								"runnerID": map[string]any{"type": "string"},
							},
							"required":             []any{"type", "runnerID"},
							"additionalProperties": false,
						},
					},
				},
				"spawnExecutor": map[string]any{"type": "boolean", "description": "Whether to start the selected executor when work is queued. Default: true."},
				"agent":         map[string]any{"type": "object", "description": "Optional custom agent definition."},
				"comment":       map[string]any{"type": "string", "description": "Optional relationship note."},
			}, nil),
			Meta: map[string]any{"source": "server"},
		}, true
	case "get_thread_metadata":
		return neoToolSpec{
			Name:        name,
			Description: "Get the title, pinned state, labels, archive state, project, agent mode, and workspace metadata for the current Amp thread or another thread owned by the signed-in user.",
			InputSchema: neoThreadToolSchema(map[string]any{
				"threadId":       threadID,
				"threadID":       threadID,
				"targetThreadId": threadID,
				"url":            threadID,
			}, nil),
			Meta: map[string]any{"source": "server"},
		}, true
	case "thread_interact":
		return neoToolSpec{
			Name:        name,
			Description: "Read thread metadata, send a message to another thread, or archive or unarchive a thread.",
			InputSchema: neoThreadToolSchema(map[string]any{
				"action":         map[string]any{"type": "string", "enum": []any{"get", "message", "archive", "unarchive"}},
				"thread":         threadID,
				"threadId":       threadID,
				"threadID":       threadID,
				"targetThreadId": threadID,
				"url":            threadID,
				"content":        map[string]any{"type": "array", "items": map[string]any{"type": "object"}, "description": "Message content blocks for the message action."},
				"message":        map[string]any{"type": "string", "description": "Text to send when content is omitted."},
				"workflow":       map[string]any{"type": "string", "description": "Optional server workflow such as code_review or merge_changes."},
				"parentToolCallId": map[string]any{
					"type":        "string",
					"description": "Optional parent tool call ID for transcript linkage.",
				},
				"sourceThreadId": map[string]any{
					"type":        "string",
					"description": "Optional source thread ID; local runtime derives this from the current thread.",
				},
				"comment": map[string]any{"type": "string", "description": "Optional relationship note."},
			}, []any{"action"}),
			Meta: map[string]any{"source": "server"},
		}, true
	case "update_thread":
		return neoToolSpec{
			Name:        name,
			Description: "Update the title, pinned state, or labels of the current Amp thread or another thread owned by the signed-in user. Include only fields the user explicitly asked to change.",
			InputSchema: neoThreadToolSchema(map[string]any{
				"threadId":       threadID,
				"threadID":       threadID,
				"targetThreadId": threadID,
				"url":            threadID,
				"title":          map[string]any{"type": "string", "description": "New thread title, from 1 to 256 characters."},
				"pinned":         map[string]any{"type": "boolean", "description": "True to pin the thread; false to unpin it."},
				"labels":         map[string]any{"type": "array", "items": map[string]any{"type": "string"}, "description": "Complete replacement label list."},
				"addLabels":      map[string]any{"type": "array", "items": map[string]any{"type": "string"}, "description": "Labels to add while preserving existing labels."},
				"removeLabels":   map[string]any{"type": "array", "items": map[string]any{"type": "string"}, "description": "Labels to remove while preserving all others."},
			}, nil),
			Meta: map[string]any{"source": "server"},
		}, true
	case "rename_thread":
		return neoToolSpec{
			Name:        name,
			Description: "Rename the current Amp thread or another thread owned by the signed-in user. Use only when the user explicitly asks to rename a thread.",
			InputSchema: neoThreadToolSchema(map[string]any{
				"threadId":       threadID,
				"threadID":       threadID,
				"targetThreadId": threadID,
				"url":            threadID,
				"title":          map[string]any{"type": "string", "description": "New thread title, from 1 to 256 characters."},
			}, []any{"title"}),
			Meta: map[string]any{"source": "server"},
		}, true
	case "set_thread_pinned":
		return neoToolSpec{
			Name:        name,
			Description: "Pin or unpin the current Amp thread or another thread owned by the signed-in user. Use only when the user explicitly asks for that change.",
			InputSchema: neoThreadToolSchema(map[string]any{
				"threadId":       threadID,
				"threadID":       threadID,
				"targetThreadId": threadID,
				"url":            threadID,
				"pinned":         map[string]any{"type": "boolean", "description": "True to pin the thread; false to unpin it."},
			}, []any{"pinned"}),
			Meta: map[string]any{"source": "server"},
		}, true
	case "add_thread_labels", "remove_thread_labels":
		action := "Add only the named labels to"
		if name == "remove_thread_labels" {
			action = "Remove only the named labels from"
		}
		return neoToolSpec{
			Name:        name,
			Description: action + " the current Amp thread or another thread owned by the signed-in user. Preserve all other labels and use only when the user explicitly asks for that change.",
			InputSchema: neoThreadToolSchema(map[string]any{
				"threadId":       threadID,
				"threadID":       threadID,
				"targetThreadId": threadID,
				"url":            threadID,
				"labels":         map[string]any{"type": "array", "items": map[string]any{"type": "string"}, "description": "One or more label names."},
			}, []any{"labels"}),
			Meta: map[string]any{"source": "server"},
		}, true
	case "archive_current_thread":
		return neoToolSpec{
			Name:        name,
			Description: "Archive the current Amp thread.",
			InputSchema: neoThreadToolSchema(map[string]any{}, nil),
			Meta:        map[string]any{"source": "server"},
		}, true
	case "archive_thread", "unarchive_thread":
		return neoToolSpec{
			Name:        name,
			Description: "Archive or unarchive an Amp thread.",
			InputSchema: neoThreadToolSchema(map[string]any{
				"threadId":       threadID,
				"threadID":       threadID,
				"targetThreadId": threadID,
				"url":            threadID,
			}, nil),
			Meta: map[string]any{"source": "server"},
		}, true
	case "archive_threads":
		return neoToolSpec{
			Name:        name,
			Description: "Archive multiple Amp threads.",
			InputSchema: neoThreadToolSchema(map[string]any{
				"threadIds": map[string]any{"type": "array", "items": threadID, "description": "Amp thread IDs or thread URLs to archive."},
				"threadIDs": map[string]any{"type": "array", "items": threadID, "description": "Alias for threadIds."},
				"threads":   map[string]any{"type": "array", "items": threadID, "description": "Alias for threadIds."},
			}, nil),
			Meta: map[string]any{"source": "server"},
		}, true
	case "send_message_to_thread":
		return neoToolSpec{
			Name:        name,
			Description: "Send a message or workflow instruction to another Amp thread.",
			InputSchema: neoThreadToolSchema(map[string]any{
				"threadId":       threadID,
				"threadID":       threadID,
				"targetThreadId": threadID,
				"thread":         threadID,
				"content":        map[string]any{"type": "array", "items": map[string]any{"type": "object"}, "description": "Message content blocks to send."},
				"message":        map[string]any{"type": "string", "description": "Text message to send when content is omitted."},
				"workflow":       map[string]any{"type": "string", "description": "Optional server workflow such as code_review or merge_changes."},
				"parentToolCallId": map[string]any{
					"type":        "string",
					"description": "Optional parent tool call ID for transcript linkage.",
				},
				"sourceThreadId": map[string]any{
					"type":        "string",
					"description": "Optional source thread ID; local runtime derives this from the current thread.",
				},
				"comment": map[string]any{"type": "string", "description": "Optional relationship note."},
			}, nil),
			Meta: map[string]any{"source": "server"},
		}, true
	case "download_thread_file":
		return neoToolSpec{
			Name:        name,
			Description: "Download one file of up to 4 MiB from another of the user's own local Amp threads when both workspaces are available on the proxy host. The source path is relative to that thread's workspace root, for example .amp/data.db.",
			InputSchema: neoThreadToolSchema(map[string]any{
				"thread":      map[string]any{"type": "string", "description": "Source thread URL or ID (T-12345678-0000-0000-0000-000000000000)."},
				"path":        map[string]any{"type": "string", "description": "File path relative to the source thread's workspace root."},
				"destination": map[string]any{"type": "string", "description": "Local destination file path. Defaults to the remote file's base name in the current thread's working directory."},
				"overwrite":   map[string]any{"type": "boolean", "description": "Overwrite an existing destination file. Default: false."},
			}, []any{"thread", "path"}),
			Meta: map[string]any{"source": "server"},
		}, true
	case "upload_thread_file":
		return neoToolSpec{
			Name:        name,
			Description: "Upload one file of up to 4 MiB from the current workspace to another of the user's own local Amp threads when both workspaces are available on the proxy host. The destination is relative to the target workspace root and its parent directory must already exist. Existing files are preserved unless overwrite is true.",
			InputSchema: neoThreadToolSchema(map[string]any{
				"thread":      map[string]any{"type": "string", "description": "Target thread URL or ID."},
				"path":        map[string]any{"type": "string", "description": "Source file in the current workspace."},
				"destination": map[string]any{"type": "string", "description": "Workspace-relative target path. Defaults to the source file's basename."},
				"overwrite":   map[string]any{"type": "boolean", "description": "Overwrite an existing target. Default: false."},
			}, []any{"thread", "path"}),
			Meta: map[string]any{"source": "server"},
		}, true
	case "get_schedule", "set_schedule", "update_schedule", "clear_schedule":
		return neoScheduleToolSpec(name), true
	default:
		return neoToolSpec{}, false
	}
}

func neoThreadToolSchema(properties map[string]any, required []any) map[string]any {
	if required == nil {
		required = []any{}
	}
	return map[string]any{"type": "object", "properties": properties, "required": required}
}

func (a *neoActor) runLocalThreadActorTool(pending neoPendingTool, generation int) {
	a.receiveToolResult(map[string]any{
		"type":       "executor_tool_result",
		"toolCallId": pending.ID,
		"run": map[string]any{
			"status":   "in-progress",
			"progress": map[string]any{"output": neoThreadToolProgressText(pending.Name, pending.Input)},
		},
	})
	result, err := a.executeLocalThreadTool(pending)
	if a.subagentGenerationStale(generation) {
		return
	}
	var run map[string]any
	if err != nil {
		run = map[string]any{"status": "error", "error": map[string]any{"message": err.Error()}}
	} else {
		run = map[string]any{"status": "done", "result": result}
	}
	a.receiveToolResult(map[string]any{"type": "executor_tool_result", "toolCallId": pending.ID, "run": run})
}

func neoThreadToolProgressText(name string, input map[string]any) string {
	target := firstNonEmptyString(input["thread"], input["threadId"], input["threadID"], input["targetThreadId"], input["url"])
	switch strings.TrimSpace(name) {
	case "find_thread":
		return "Searching threads"
	case "list_agent_modes":
		return "Listing agent modes"
	case "list_runners":
		return "Listing runners"
	case "get_current_user_identity":
		return "Reading current user identity"
	case "create_thread":
		return "Creating thread"
	case "thread_interact":
		switch strings.ToLower(strings.TrimSpace(stringValue(input["action"]))) {
		case "get":
			return "Reading thread metadata"
		case "message":
			if target != "" {
				return "Sending message to thread: " + target
			}
			return "Sending message to thread"
		case "archive":
			if target != "" {
				return "Archiving thread: " + target
			}
			return "Archiving this thread"
		case "unarchive":
			if target != "" {
				return "Unarchiving thread: " + target
			}
			return "Unarchiving this thread"
		default:
			return "Interacting with thread"
		}
	case "get_thread_metadata":
		return "Reading thread metadata"
	case "update_thread":
		return "Updating thread"
	case "rename_thread":
		return "Renaming thread"
	case "set_thread_pinned":
		if pinned, ok := input["pinned"].(bool); ok && !pinned {
			return "Unpinning thread"
		}
		return "Pinning thread"
	case "add_thread_labels":
		return "Adding thread labels"
	case "remove_thread_labels":
		return "Removing thread labels"
	case "archive_current_thread":
		return "Archiving current thread"
	case "archive_thread", "archive_threads":
		if target != "" {
			return "Archiving thread: " + target
		}
		return "Archiving thread"
	case "unarchive_thread":
		if target != "" {
			return "Unarchiving thread: " + target
		}
		return "Unarchiving thread"
	case "send_message_to_thread":
		if target != "" {
			return "Sending message to thread: " + target
		}
		return "Sending message to thread"
	case "download_thread_file":
		return "Downloading file from thread"
	case "upload_thread_file":
		return "Uploading file to thread"
	case "get_schedule":
		return "Reading schedule"
	case "set_schedule":
		return "Setting schedule"
	case "update_schedule":
		return "Updating schedule"
	case "clear_schedule":
		return "Clearing schedule"
	default:
		return strings.TrimSpace(name)
	}
}

func (a *neoActor) executeLocalThreadTool(pending neoPendingTool) (map[string]any, error) {
	switch strings.TrimSpace(pending.Name) {
	case "find_thread":
		return a.executeLocalFindThreadTool(pending.Input)
	case "list_agent_modes":
		return a.executeLocalListAgentModesTool()
	case "list_runners":
		return a.executeLocalListRunnersTool()
	case "get_current_user_identity":
		return a.executeLocalGetCurrentUserIdentityTool(pending)
	case "create_thread":
		return a.executeLocalCreateThreadTool(pending)
	case "thread_interact":
		return a.executeLocalThreadInteractTool(pending)
	case "get_thread_metadata":
		return a.executeLocalGetThreadMetadataTool(pending.Input)
	case "update_thread":
		return a.executeLocalUpdateThreadTool(pending.Input)
	case "rename_thread":
		return a.executeLocalRenameThreadTool(pending.Input)
	case "set_thread_pinned":
		return a.executeLocalSetThreadPinnedTool(pending.Input)
	case "add_thread_labels":
		return a.executeLocalUpdateThreadLabelsTool(pending.Input, true)
	case "remove_thread_labels":
		return a.executeLocalUpdateThreadLabelsTool(pending.Input, false)
	case "archive_current_thread":
		a.archiveThread(true, nil)
		return map[string]any{"threadId": a.threadID, "archived": true}, nil
	case "archive_thread":
		return a.executeLocalArchiveThreadTool(pending.Input, true)
	case "unarchive_thread":
		return a.executeLocalArchiveThreadTool(pending.Input, false)
	case "archive_threads":
		return a.executeLocalArchiveThreadsTool(pending.Input)
	case "send_message_to_thread":
		return a.executeLocalSendMessageToThreadTool(pending)
	case "download_thread_file":
		return a.executeLocalDownloadThreadFileTool(pending.Input)
	case "upload_thread_file":
		return a.executeLocalUploadThreadFileTool(pending.Input)
	case "get_schedule", "set_schedule", "update_schedule", "clear_schedule":
		return a.executeLocalScheduleTool(pending.Name, pending.Input)
	default:
		return nil, errors.New("unsupported thread tool")
	}
}

func (a *neoActor) executeLocalGetCurrentUserIdentityTool(pending neoPendingTool) (map[string]any, error) {
	identity := map[string]any{"id": a.threadToolOwnerID()}
	if a.runtime != nil && strings.TrimSpace(pending.ClientAPIKey) != "" {
		profile := a.runtime.neoWebLocalCachedUserProfile(neoContextWithClientAPIKey(context.Background(), pending.ClientAPIKey))
		for _, key := range []string{"id", "username", "firstName", "lastName", "email", "profilePictureUrl"} {
			if value := strings.TrimSpace(stringValue(profile[key])); value != "" {
				identity[key] = value
			}
		}
	}
	identity["displayName"] = neoWebLocalActivityUserDisplayName(identity)
	return identity, nil
}

func (a *neoActor) executeLocalFindThreadTool(input map[string]any) (map[string]any, error) {
	if a.runtime == nil {
		return nil, errors.New("find_thread missing local runtime")
	}
	query := strings.TrimSpace(stringValue(input["query"]))
	if query == "" {
		return nil, errors.New("find_thread requires query")
	}
	values := make(url.Values)
	values.Set("q", query)
	if limit := numberFrom(input["limit"]); limit > 0 {
		values.Set("limit", strconv.Itoa(limit))
	}
	if offset := numberFrom(input["offset"]); offset > 0 {
		values.Set("offset", strconv.Itoa(offset))
	}
	if window := strings.TrimSpace(stringValue(input["time"])); window != "" {
		values.Set("time", window)
	}
	result, ok := a.runtime.localThreadSearchResponseWithMaxLimitForOwner(values, 50, a.threadToolOwnerID())
	if !ok {
		return nil, errors.New("find_thread local search is unavailable")
	}
	return result, nil
}

func (a *neoActor) executeLocalListAgentModesTool() (map[string]any, error) {
	modes := []any{
		map[string]any{"key": "low", "label": "Low", "type": "builtin"},
		map[string]any{"key": "medium", "label": "Medium", "type": "builtin"},
		map[string]any{"key": "high", "label": "High", "type": "builtin"},
		map[string]any{"key": "ultra", "label": "Ultra", "type": "builtin"},
		map[string]any{"key": "smart", "label": "Smart", "type": "builtin"},
		map[string]any{"key": "large", "label": "Large", "type": "builtin"},
		map[string]any{"key": "rush", "label": "Rush", "type": "builtin"},
		map[string]any{"key": "deep", "label": "Deep", "type": "builtin"},
		map[string]any{"key": "nostromo", "label": "Nostromo", "type": "builtin"},
	}
	pluginModes, err := neoWebLocalServerPluginAgentModes()
	if err != nil {
		return nil, err
	}
	for _, item := range pluginModes {
		mode := cloneMap(mapValue(item))
		mode["type"] = "plugin"
		modes = append(modes, mode)
	}
	return map[string]any{"modes": modes}, nil
}

func (a *neoActor) executeLocalListRunnersTool() (map[string]any, error) {
	if a.runtime == nil || a.runtime.store == nil {
		return nil, errors.New("list_runners missing local runtime")
	}
	return map[string]any{"runners": a.runtime.store.userExecutorRunnersForOwner(a.threadToolOwnerID())}, nil
}

func (a *neoActor) executeLocalCreateThreadTool(pending neoPendingTool) (map[string]any, error) {
	input := pending.Input
	if a.runtime == nil || a.runtime.store == nil {
		return nil, errors.New("create_thread missing local runtime")
	}
	threadID := firstNonEmptyString(input["threadId"], input["threadID"], input["thread_id"])
	if threadID == "" {
		threadID = "T-" + randomUUIDLike()
	}
	if !neoThreadIDExactPattern.MatchString(threadID) {
		return nil, errors.New("create_thread requires a valid threadId")
	}
	if threadID == a.threadID || a.runtime.store.lookupThreadActor(threadID) != nil {
		return nil, errors.New("create_thread threadId already exists")
	}
	if a.runtime.threadDir != "" {
		if _, exists := loadNeoThreadFromDir(threadID, a.runtime.threadDir); exists {
			return nil, errors.New("create_thread threadId already exists")
		}
	}
	body, runnerID, spawnExecutor, err := a.localCreateThreadBody(input, threadID)
	if err != nil {
		return nil, err
	}
	ctx := context.WithValue(context.Background(), neoExclusiveThreadCreationContextKey{}, true)
	if pending.ClientAPIKey != "" {
		ctx = context.WithValue(ctx, clientAPIKeyContextKey{}, pending.ClientAPIKey)
	}
	ownerID := a.threadToolOwnerID()
	response, status := a.runtime.localThreadActorManagementResponseForOwner(ctx, body, "", ownerID)
	if status < 200 || status >= 300 {
		message := firstNonEmptyString(response["message"], response["error"])
		if message == "" {
			message = "thread creation failed"
		}
		return nil, errors.New(message)
	}
	a.handleCreateThread(map[string]any{"type": "create_thread", "threadId": threadID, "comment": stringValue(input["comment"])})
	child := a.runtime.store.lookupThreadActor(threadID)
	if child == nil {
		return nil, errors.New("create_thread could not resolve child thread")
	}
	runnerExecutorRequested := false
	runnerExecutorError := ""
	localExecutorAttempted := false
	localExecutorStatus := map[string]any(nil)
	if runnerID != "" && spawnExecutor {
		_, _, hasPendingWork := child.pendingWebLocalExecutorRequest()
		if hasPendingWork {
			runnerExecutorRequested = a.runtime.store.requestUserExecutorRunnerThreadForOwner(ownerID, runnerID, threadID)
			if !runnerExecutorRequested {
				runnerExecutorError = "selected runner is no longer available"
			}
		}
	} else if spawnExecutor {
		_, _, localExecutorAttempted = child.pendingWebLocalExecutorRequest()
		if localExecutorAttempted {
			localExecutorStatus = child.maybeSpawnWebLocalExecutorForPendingWork()
		}
	}
	result := map[string]any{
		"threadId":        threadID,
		"threadID":        threadID,
		"url":             "https://ampcode.com/threads/" + threadID,
		"created":         true,
		"agentMode":       response["agentMode"],
		"reasoningEffort": response["reasoningEffort"],
	}
	if runnerID != "" {
		result["runnerId"] = runnerID
		result["executorStarted"] = false
		result["executorRequested"] = runnerExecutorRequested
		if runnerExecutorError != "" {
			result["executorError"] = runnerExecutorError
		}
	} else if localExecutorAttempted {
		localExecutorStarted := strings.EqualFold(stringValue(localExecutorStatus["status"]), "running") || strings.EqualFold(stringValue(localExecutorStatus["status"]), "starting")
		result["executorStarted"] = localExecutorStarted
		if !localExecutorStarted {
			result["executorError"] = firstNonEmptyString(localExecutorStatus["message"], localExecutorStatus["error"], "local executor was not started")
		}
	}
	if workingDirectory := stringValue(body["workingDirectory"]); workingDirectory != "" {
		result["workingDirectory"] = workingDirectory
	}
	if projectID := stringValue(body["projectID"]); projectID != "" {
		result["projectID"] = projectID
	}
	return result, nil
}

func (a *neoActor) localCreateThreadBody(input map[string]any, threadID string) (map[string]any, string, bool, error) {
	a.mu.Lock()
	parentEnvironment := cloneMap(a.environment)
	parentMeta := cloneMap(a.meta)
	parentSettings := cloneMap(a.settings)
	parentMode := a.agentModeLocked()
	parentEffort := a.reasoningEffortForModeLocked(parentMode)
	parentThreadID := a.threadID
	ownerID := firstNonEmptyString(parentMeta["ownerUserId"], parentMeta["ownerUserID"], parentMeta["creatorUserID"], parentMeta["creatorUserId"])
	if ownerID == "" {
		ownerID = neoLocalOwnerUserID
	}
	a.mu.Unlock()
	parentWorkingDirectory, parentWorkspaceRoot := neoResolvedEnvironmentWorkspacePaths(parentEnvironment)
	parentWorkingDirectory = neoExistingDirectory(parentWorkingDirectory)
	parentWorkspaceRoot = neoExistingDirectory(parentWorkspaceRoot)

	executor := strings.TrimSpace(stringValue(input["executor"]))
	runnerID := firstNonEmptyString(input["runnerId"], input["runnerID"], input["runner_id"])
	if executorMap := mapValue(input["executor"]); len(executorMap) > 0 {
		executorType := strings.ToLower(strings.TrimSpace(stringValue(executorMap["type"])))
		switch executorType {
		case "runner":
			runnerID = firstNonEmptyString(runnerID, executorMap["id"], executorMap["runnerId"], executorMap["runnerID"])
		case "orb":
			return nil, "", false, errors.New("create_thread cannot provision Amp-hosted orbs from the local runtime")
		case "", "local":
		default:
			return nil, "", false, fmt.Errorf("create_thread received unsupported executor type %q", executorType)
		}
		executor = executorType
	}
	if strings.EqualFold(executor, "orb") {
		return nil, "", false, errors.New("create_thread cannot provision Amp-hosted orbs from the local runtime")
	}
	if strings.EqualFold(executor, "local") && runnerID != "" {
		return nil, "", false, errors.New("create_thread executor local conflicts with runnerId")
	}
	if strings.EqualFold(executor, "runner") && runnerID == "" {
		return nil, "", false, errors.New("create_thread executor runner requires runnerId")
	}
	if executor != "" && !strings.EqualFold(executor, "local") && !strings.EqualFold(executor, "runner") {
		if runnerID != "" && runnerID != executor {
			return nil, "", false, errors.New("create_thread executor conflicts with runnerId")
		}
		runnerID = executor
	}
	explicitRunnerSelection := runnerID != ""
	var ownedProjectDirectories map[string]map[string]struct{}
	getOwnedProjectDirectories := func() map[string]map[string]struct{} {
		if ownedProjectDirectories == nil {
			ownedProjectDirectories = neoCreateThreadOwnedProjectWorkingDirectories(a.runtime, ownerID)
		}
		return ownedProjectDirectories
	}

	requestedProjectID := firstNonEmptyString(input["projectID"], input["projectId"], input["project_id"])
	projectID := requestedProjectID
	requestedProjectWorkingDirectory := ""
	if projectID == "" {
		projectID = firstNonEmptyString(parentMeta["projectID"], parentMeta["projectId"])
	} else if !neoUUIDExactPattern.MatchString(projectID) {
		return nil, "", false, errors.New("create_thread requires a valid projectID")
	}
	if requestedProjectID != "" {
		project := neoWebLocalProjectByID(a.runtime.neoWebLocalProjectCache(), requestedProjectID)
		if len(project) == 0 {
			project = neoWebLocalProjectByID(a.runtime.reloadNeoWebLocalProjectCache(), requestedProjectID)
		}
		if len(project) == 0 {
			return nil, "", false, errors.New("create_thread projectID is not available in the local runtime")
		}
		requestedProjectWorkingDirectory = firstNonEmptyString(project["workingDirectory"], project["workspaceRoot"], project["cwd"])
	}

	rawWorkingDirectory := firstNonEmptyString(input["workingDirectory"], input["working_directory"], input["workspaceRoot"], input["cwd"])
	if runnerID == "" && executor == "" {
		runnerID = firstNonEmptyString(parentMeta["runnerId"], parentMeta["runnerID"])
	}
	workingDirectory := ""
	if runnerID != "" {
		runnerWorkingDirectory := a.runtime.store.userExecutorRunnerWorkingDirectoryForOwner(ownerID, runnerID)
		if runnerWorkingDirectory == "" {
			return nil, "", false, errors.New("create_thread selected runner is not available")
		} else {
			if rawWorkingDirectory != "" {
				requestedWorkingDirectory := neoUserExecutorRunnerWorkingDirectory(rawWorkingDirectory)
				if requestedWorkingDirectory == "" || requestedWorkingDirectory != runnerWorkingDirectory {
					return nil, "", false, errors.New("create_thread workingDirectory does not match the selected runner")
				}
			}
			if requestedProjectWorkingDirectory != "" {
				projectWorkingDirectory := neoUserExecutorRunnerWorkingDirectory(requestedProjectWorkingDirectory)
				if projectWorkingDirectory == "" || projectWorkingDirectory != runnerWorkingDirectory {
					return nil, "", false, errors.New("create_thread projectID does not match the selected runner")
				}
			}
			workingDirectory = runnerWorkingDirectory
		}
	}
	if runnerID == "" {
		workingDirectory = neoExistingDirectory(rawWorkingDirectory)
		if rawWorkingDirectory != "" && workingDirectory == "" {
			return nil, "", false, errors.New("create_thread workingDirectory must be an existing directory on the proxy host")
		}
		if rawWorkingDirectory != "" && requestedProjectID == "" && !neoCreateThreadLocalWorkspaceAllowed(a.runtime, parentEnvironment, getOwnedProjectDirectories(), workingDirectory) {
			return nil, "", false, errors.New("create_thread workingDirectory must be within the current or an indexed project workspace")
		}
	}
	if requestedProjectID != "" && runnerID == "" {
		projectDirectories := getOwnedProjectDirectories()[requestedProjectID]
		if workingDirectory != "" {
			if _, ok := projectDirectories[workingDirectory]; !ok {
				return nil, "", false, errors.New("create_thread workingDirectory does not match projectID")
			}
		} else if len(projectDirectories) > 1 {
			return nil, "", false, errors.New("create_thread workingDirectory is required when multiple local project checkouts are available")
		} else {
			for projectWorkingDirectory := range projectDirectories {
				workingDirectory = projectWorkingDirectory
			}
		}
		if workingDirectory == "" {
			return nil, "", false, errors.New("create_thread projectID does not have a local checkout available to the thread owner")
		}
	}
	parentRunnerID := firstNonEmptyString(parentMeta["runnerId"], parentMeta["runnerID"])
	if workingDirectory == "" && requestedProjectID == "" {
		if strings.EqualFold(executor, "local") && parentRunnerID != "" {
			if projectID != "" {
				var ambiguous bool
				workingDirectory, ambiguous = neoCreateThreadOwnedProjectWorkingDirectory(getOwnedProjectDirectories(), projectID)
				if ambiguous {
					return nil, "", false, errors.New("create_thread workingDirectory is required when multiple local project checkouts are available")
				}
			}
		} else {
			workingDirectory = parentWorkingDirectory
			if workingDirectory == "" && projectID != "" {
				var ambiguous bool
				workingDirectory, ambiguous = neoCreateThreadOwnedProjectWorkingDirectory(getOwnedProjectDirectories(), projectID)
				if ambiguous {
					return nil, "", false, errors.New("create_thread workingDirectory is required when multiple local project checkouts are available")
				}
			}
		}
	}
	if runnerID == "" && workingDirectory != "" {
		workingDirectory = neoExistingDirectory(workingDirectory)
	}
	workspaceOverridesInheritedProject := false
	workspaceProject := map[string]any(nil)
	if requestedProjectID == "" && (rawWorkingDirectory != "" || explicitRunnerSelection) && workingDirectory != "" {
		parentWorkspace := firstNonEmptyString(parentWorkspaceRoot, parentWorkingDirectory)
		indexedProject := neoCreateThreadIndexedProjectForWorkingDirectory(a.runtime, workingDirectory)
		indexedProjectID := firstNonEmptyString(indexedProject["id"], indexedProject["projectID"])
		ownedParentProjectWorkspace := neoCreateThreadOwnedProjectWorkspaceContains(getOwnedProjectDirectories(), projectID, workingDirectory)
		outsideParentWorkspace := !ownedParentProjectWorkspace && (parentWorkspace == "" || !neoThreadFilePathWithin(parentWorkspace, workingDirectory))
		distinctIndexedProject := indexedProjectID != "" && indexedProjectID != projectID
		if outsideParentWorkspace || distinctIndexedProject {
			workspaceOverridesInheritedProject = true
			workspaceProject = indexedProject
			if len(workspaceProject) == 0 {
				workspaceProject = a.runtime.neoWebLocalProjectForWorkingDirectory(workingDirectory)
			}
			projectID = firstNonEmptyString(workspaceProject["id"], workspaceProject["projectID"])
		}
	}
	workspaceRoot := workingDirectory
	if runnerID == "" && requestedProjectID == "" && !workspaceOverridesInheritedProject && parentWorkspaceRoot != "" && neoThreadFilePathWithin(parentWorkspaceRoot, workingDirectory) {
		workspaceRoot = parentWorkspaceRoot
	}

	agentMode := strings.ToLower(strings.TrimSpace(firstNonEmptyString(input["agentMode"], input["mode"])))
	if agentMode == "" {
		if strings.EqualFold(parentMode, "puck") {
			agentMode = "medium"
		} else {
			agentMode = parentMode
		}
	}
	reasoningEffort := strings.ToLower(strings.TrimSpace(firstNonEmptyString(input["reasoningEffort"], input["reasoning_effort"])))
	agent := mapValue(input["agent"])
	if baseMode, defaultEffort, ok := neoWebLocalDeepAgentMode(agentMode); ok {
		agentMode = baseMode
		if reasoningEffort == "" {
			reasoningEffort = defaultEffort
		}
	}
	if len(agent) == 0 && neoCustomAgentModeMatches(parentSettings, agentMode) {
		agent = neoCustomAgentDefinitionFromSettings(parentSettings)
	}
	if len(agent) == 0 && agentMode != "" && !validNeoClientAgentMode(agentMode) {
		pluginMode, pluginErr := loadNeoPluginAgentMode(agentMode)
		if pluginErr != nil {
			return nil, "", false, fmt.Errorf("create_thread received unsupported agentMode %q", agentMode)
		}
		agent = pluginMode.agentDefinition()
		if reasoningEffort == "" {
			reasoningEffort = pluginMode.ReasoningEffort
		}
	}
	if reasoningEffort == "" {
		if agentMode == parentMode {
			reasoningEffort = parentEffort
		} else if strings.EqualFold(parentMode, "puck") {
			reasoningEffort = defaultNeoReasoningEffort(agentMode)
		}
	}
	initialContent := []any(nil)
	if rawContent, exists := input["content"]; exists {
		var ok bool
		initialContent, ok = normalizeNeoClientUserContent(rawContent)
		if !ok || !neoClientUserContentHasMeaningfulBlock(initialContent) {
			return nil, "", false, errors.New("create_thread content must contain valid text or image blocks")
		}
	}
	prompt := strings.TrimSpace(firstNonEmptyString(input["prompt"], input["message"], input["task"]))
	if prompt == "" && len(initialContent) > 0 {
		prompt = strings.TrimSpace(neoWebLocalProjectThreadContentText(initialContent))
	}
	spawnExecutor := true
	if rawSpawn, exists := input["spawnExecutor"]; exists {
		value, ok := rawSpawn.(bool)
		if !ok {
			return nil, "", false, errors.New("create_thread spawnExecutor must be a boolean")
		}
		spawnExecutor = value
	}
	if spawnExecutor && runnerID == "" && strings.EqualFold(executor, "local") && workingDirectory == "" {
		return nil, "", false, errors.New("create_thread local executor requires an existing workingDirectory on the proxy host")
	}

	repositoryURL := firstNonEmptyString(parentMeta["repositoryURL"], parentMeta["repositoryUrl"])
	if workspaceOverridesInheritedProject {
		repositoryURL = firstNonEmptyString(workspaceProject["repositoryURL"], workspaceProject["repoURL"])
	} else if requestedProjectID != "" {
		repositoryURL = neoCreateThreadProjectRepositoryURL(a.runtime, projectID, workingDirectory)
	}
	threadMeta := map[string]any{
		"cliProxyAPILocalNeo": true,
		"ampcodeLocalRuntime": true,
		"visibility":          "private",
		"sharedGroupIDs":      []any{},
		"parentThreadID":      parentThreadID,
		"ownerUserId":         ownerID,
		"creatorUserID":       ownerID,
		"projectID":           omitEmpty(projectID),
		"runnerId":            omitEmpty(runnerID),
		"agentMode":           omitEmpty(agentMode),
		"reasoningEffort":     omitEmpty(reasoningEffort),
		"repositoryURL":       omitEmpty(repositoryURL),
	}
	body := map[string]any{
		"threadId":         threadID,
		"threadID":         threadID,
		"usesThreadActors": true,
		"agentMode":        omitEmpty(agentMode),
		"reasoningEffort":  omitEmpty(reasoningEffort),
		"projectID":        omitEmpty(projectID),
		"threadMeta":       threadMeta,
		"prompt":           omitEmpty(prompt),
	}
	if len(initialContent) > 0 {
		body["content"] = initialContent
		delete(body, "prompt")
	}
	if len(agent) > 0 {
		body["agent"] = agent
	}
	if workingDirectory != "" {
		body["workingDirectory"] = workingDirectory
		body["workspaceRoot"] = workspaceRoot
	}
	if spawnExecutor && (runnerID != "" || workingDirectory != "" || strings.EqualFold(executor, "local")) {
		body["executorType"] = "local-client"
		threadMeta["executorType"] = "local-client"
	}
	return body, runnerID, spawnExecutor, nil
}

func neoCreateThreadLocalWorkspaceAllowed(rt *neoRuntime, parentEnvironment map[string]any, ownedProjectDirectories map[string]map[string]struct{}, workingDirectory string) bool {
	workingDirectory = neoWebLocalProjectDirectory(workingDirectory)
	if workingDirectory == "" || rt == nil {
		return false
	}
	parentWorkingDirectory, parentWorkspaceRoot := neoResolvedEnvironmentWorkspacePaths(parentEnvironment)
	parentWorkspace := neoWebLocalProjectDirectory(firstNonEmptyString(parentWorkspaceRoot, parentWorkingDirectory))
	if parentWorkspace != "" && neoThreadFilePathWithin(parentWorkspace, workingDirectory) {
		return true
	}
	for _, projectDirectories := range ownedProjectDirectories {
		for projectDirectory := range projectDirectories {
			if neoThreadFilePathWithin(projectDirectory, workingDirectory) {
				return true
			}
		}
	}
	projects := rt.neoWebLocalProjectCache()
	for attempt := 0; attempt < 2; attempt++ {
		for _, rawProject := range projects {
			project := mapValue(rawProject)
			projectDirectory := neoWebLocalProjectDirectory(firstNonEmptyString(project["workingDirectory"], project["workspaceRoot"], project["cwd"]))
			if projectDirectory == "" || !neoThreadFilePathWithin(projectDirectory, workingDirectory) {
				continue
			}
			projectID := firstNonEmptyString(project["id"], project["projectID"], project["projectId"], project["project_id"])
			if _, ok := ownedProjectDirectories[projectID][projectDirectory]; ok {
				return true
			}
		}
		projects = rt.reloadNeoWebLocalProjectCache()
	}
	return false
}

func neoCreateThreadOwnedProjectWorkspaceContains(ownedProjectDirectories map[string]map[string]struct{}, projectID, workingDirectory string) bool {
	for projectDirectory := range ownedProjectDirectories[projectID] {
		if neoThreadFilePathWithin(projectDirectory, workingDirectory) {
			return true
		}
	}
	return false
}

func neoCreateThreadOwnedProjectWorkingDirectory(ownedProjectDirectories map[string]map[string]struct{}, projectID string) (string, bool) {
	directories := ownedProjectDirectories[projectID]
	if len(directories) != 1 {
		return "", len(directories) > 1
	}
	for directory := range directories {
		return directory, false
	}
	return "", false
}

func neoCreateThreadOwnedProjectWorkingDirectories(rt *neoRuntime, ownerID string) map[string]map[string]struct{} {
	directories := map[string]map[string]struct{}{}
	if rt == nil || rt.store == nil {
		return directories
	}
	add := func(projectID, directory string) {
		if directories[projectID] == nil {
			directories[projectID] = map[string]struct{}{}
		}
		directories[projectID][directory] = struct{}{}
	}
	for _, actor := range rt.store.threadActors(0) {
		actor.mu.Lock()
		meta := cloneMap(actor.meta)
		environment := cloneMap(actor.environment)
		actor.mu.Unlock()
		if firstNonEmptyString(meta["runnerId"], meta["runnerID"]) != "" {
			continue
		}
		projectID := neoThreadProjectID(meta)
		if projectID != "" && rt.neoThreadOwnedByRequestUser(map[string]any{"meta": meta}, ownerID) {
			workingDirectory, workspaceRoot := neoResolvedEnvironmentWorkspacePaths(environment)
			if projectDirectory := neoWebLocalProjectDirectory(firstNonEmptyString(workspaceRoot, workingDirectory)); projectDirectory != "" {
				add(projectID, projectDirectory)
			}
		}
	}
	if !rt.localThreadSnapshotsEnabled() {
		return directories
	}
	entries, err := os.ReadDir(rt.threadDir)
	if err != nil {
		return directories
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		threadID := strings.TrimSuffix(entry.Name(), ".json")
		thread, ok := loadNeoThreadFromDir(threadID, rt.threadDir)
		meta := mapValue(thread["meta"])
		data := mapValue(thread["data"])
		dataMeta := mapValue(data["meta"])
		projectID := neoThreadProjectID(meta)
		if !ok || !rt.neoThreadOwnedByRequestUser(thread, ownerID) || projectID == "" {
			continue
		}
		if firstNonEmptyString(thread["runnerId"], thread["runnerID"], meta["runnerId"], meta["runnerID"], data["runnerId"], data["runnerID"], dataMeta["runnerId"], dataMeta["runnerID"]) != "" {
			continue
		}
		environment := mapValue(thread["env"])
		if len(environment) == 0 {
			environment = mapValue(mapValue(thread["data"])["env"])
		}
		workingDirectory, workspaceRoot := neoResolvedEnvironmentWorkspacePaths(environment)
		if projectDirectory := neoWebLocalProjectDirectory(firstNonEmptyString(workspaceRoot, workingDirectory)); projectDirectory != "" {
			add(projectID, projectDirectory)
		}
	}
	return directories
}

func neoCreateThreadIndexedProjectForWorkingDirectory(rt *neoRuntime, workingDirectory string) map[string]any {
	if rt == nil {
		return nil
	}
	if project := neoWebLocalProjectByWorkingDirectory(rt.neoWebLocalProjectCache(), workingDirectory); len(project) > 0 {
		return cloneMap(project)
	}
	if project := neoWebLocalProjectByWorkingDirectory(rt.reloadNeoWebLocalProjectCache(), workingDirectory); len(project) > 0 {
		return cloneMap(project)
	}
	return nil
}

func neoCreateThreadProjectRepositoryURL(rt *neoRuntime, projectID, workingDirectory string) string {
	if rt == nil || projectID == "" {
		return ""
	}
	project := neoWebLocalProjectByID(rt.neoWebLocalProjectCache(), projectID)
	if len(project) == 0 {
		project = neoWebLocalProjectByID(rt.reloadNeoWebLocalProjectCache(), projectID)
	}
	if repositoryURL := strings.TrimSpace(firstNonEmptyString(project["repositoryURL"], project["repoURL"])); repositoryURL != "" {
		return repositoryURL
	}
	if neoExistingDirectory(workingDirectory) == "" {
		return ""
	}
	repositoryURL, _, _ := neoWebLocalCanonicalProjectRepository("", workingDirectory)
	return repositoryURL
}

func neoCustomAgentDefinitionFromSettings(settings map[string]any) map[string]any {
	model := strings.TrimSpace(stringValue(settings[neoCustomAgentModelSetting]))
	instructions := strings.TrimSpace(stringValue(settings[neoCustomAgentInstructionsSetting]))
	if model == "" || instructions == "" {
		return nil
	}
	agent := map[string]any{
		"kind":         "agent-definition",
		"name":         stringValue(settings[neoCustomAgentConfigKeySetting]),
		"model":        model,
		"instructions": instructions,
	}
	if mode := strings.ToLower(strings.TrimSpace(stringValue(settings[neoCustomAgentModeSetting]))); neoCustomAgentModePattern.MatchString(mode) && !validNeoClientAgentMode(mode) {
		agent["agentMode"] = mode
	}
	if tools, exists := settings[neoCustomAgentToolsSetting]; exists {
		agent["tools"] = tools
	}
	return agent
}

func (a *neoActor) executeLocalArchiveThreadTool(input map[string]any, archive bool) (map[string]any, error) {
	target, threadID, err := a.threadToolTargetActor(input, true)
	if err != nil {
		return nil, err
	}
	target.archiveThread(archive, nil)
	return map[string]any{"threadId": threadID, "archived": archive}, nil
}

func (a *neoActor) executeLocalGetThreadMetadataTool(input map[string]any) (map[string]any, error) {
	target, threadID, err := a.threadToolTargetActor(input, true)
	if err != nil {
		return nil, err
	}
	return neoLocalThreadMetadata(target, threadID), nil
}

func (a *neoActor) executeLocalUpdateThreadTool(input map[string]any) (map[string]any, error) {
	target, threadID, err := a.threadToolTargetActor(input, true)
	if err != nil {
		return nil, err
	}
	titleRaw, titleExists := input["title"]
	title := ""
	if titleExists {
		var ok bool
		title, ok = normalizeNeoClientThreadTitle(titleRaw)
		if !ok {
			return nil, errors.New("update_thread title must contain 1 to 256 characters")
		}
	}
	pinnedRaw, pinnedExists := input["pinned"]
	pinned, pinnedOK := pinnedRaw.(bool)
	if pinnedExists && !pinnedOK {
		return nil, errors.New("update_thread pinned must be a boolean")
	}
	labels, labelsExists, err := neoThreadToolOptionalLabels(input, "labels")
	if err != nil {
		return nil, err
	}
	addLabels, addLabelsExists, err := neoThreadToolOptionalLabels(input, "addLabels")
	if err != nil {
		return nil, err
	}
	removeLabels, removeLabelsExists, err := neoThreadToolOptionalLabels(input, "removeLabels")
	if err != nil {
		return nil, err
	}
	if labelsExists && (addLabelsExists || removeLabelsExists) {
		return nil, errors.New("update_thread labels cannot be combined with addLabels or removeLabels")
	}
	if !titleExists && !pinnedExists && !labelsExists && !addLabelsExists && !removeLabelsExists {
		return nil, errors.New("update_thread requires title, pinned, labels, addLabels, or removeLabels")
	}
	target.metadataMutationMu.Lock()
	defer target.metadataMutationMu.Unlock()
	target.mu.Lock()
	titleChanged := titleExists && target.title != title
	if titleExists {
		target.title = title
		target.titleSource = "explicit"
	}
	if pinnedExists {
		target.pinned = pinned
		value := pinned
		target.pinnedOverride = &value
	}
	if target.meta == nil {
		target.meta = map[string]any{}
	}
	updatedLabels := neoThreadLabelsFromAny(target.meta["labels"])
	if labelsExists {
		updatedLabels = neoNormalizeThreadLabels(labels)
	} else {
		if addLabelsExists {
			updatedLabels = neoNormalizeThreadLabels(append(updatedLabels, addLabels...))
		}
		if removeLabelsExists {
			remove := make(map[string]bool, len(removeLabels))
			for _, label := range removeLabels {
				remove[strings.ToLower(label)] = true
			}
			kept := updatedLabels[:0]
			for _, label := range updatedLabels {
				if !remove[strings.ToLower(label)] {
					kept = append(kept, label)
				}
			}
			updatedLabels = kept
		}
	}
	if labelsExists || addLabelsExists || removeLabelsExists {
		if len(updatedLabels) == 0 {
			delete(target.meta, "labels")
		} else {
			target.meta["labels"] = updatedLabels
		}
	}
	metadata := neoLocalThreadMetadataLocked(target, threadID)
	target.mu.Unlock()
	if titleChanged {
		target.broadcast(map[string]any{"type": "thread_title", "title": title})
	}
	if target.runtime != nil && target.runtime.store != nil {
		target.runtime.store.broadcastThreadStatusUpdated(target)
	}
	target.syncCloudAsync()
	return metadata, nil
}

func neoThreadToolOptionalLabels(input map[string]any, key string) ([]string, bool, error) {
	raw, exists := input[key]
	if !exists {
		return nil, false, nil
	}
	switch typed := raw.(type) {
	case []any:
		for _, label := range typed {
			if _, ok := label.(string); !ok {
				return nil, false, fmt.Errorf("update_thread %s must contain only string labels", key)
			}
		}
	case []string:
	default:
		return nil, false, fmt.Errorf("update_thread %s must be an array of labels", key)
	}
	labels := neoThreadLabelsFromAny(raw)
	if key != "labels" && len(labels) == 0 {
		return nil, false, fmt.Errorf("update_thread %s must contain at least one label", key)
	}
	return labels, true, nil
}

func neoLocalThreadMetadata(target *neoActor, threadID string) map[string]any {
	target.mu.Lock()
	metadata := neoLocalThreadMetadataLocked(target, threadID)
	target.mu.Unlock()
	return metadata
}

func neoLocalThreadMetadataLocked(target *neoActor, threadID string) map[string]any {
	return map[string]any{
		"threadId":         threadID,
		"title":            target.title,
		"pinned":           target.pinned,
		"labels":           neoThreadLabelsFromAny(target.meta["labels"]),
		"archived":         target.archived,
		"projectID":        firstNonEmptyString(target.meta["projectID"], target.meta["projectId"], target.meta["project_id"]),
		"repositoryURL":    firstNonEmptyString(target.meta["repositoryURL"], target.meta["repositoryUrl"], target.meta["repoURL"]),
		"agentMode":        firstNonEmptyString(target.settings["agentMode"], target.meta["agentMode"]),
		"reasoningEffort":  firstNonEmptyString(target.settings["reasoning.effort"], target.settings["reasoningEffort"], target.meta["reasoningEffort"]),
		"workingDirectory": neoWorkingDirectoryFromEnvironment(target.environment),
	}
}

func (a *neoActor) executeLocalRenameThreadTool(input map[string]any) (map[string]any, error) {
	target, threadID, err := a.threadToolTargetActor(input, true)
	if err != nil {
		return nil, err
	}
	title, ok := normalizeNeoClientThreadTitle(input["title"])
	if !ok {
		return nil, errors.New("rename_thread requires a title from 1 to 256 characters")
	}
	target.setTitle(title)
	return map[string]any{"threadId": threadID, "title": title}, nil
}

func (a *neoActor) executeLocalSetThreadPinnedTool(input map[string]any) (map[string]any, error) {
	target, threadID, err := a.threadToolTargetActor(input, true)
	if err != nil {
		return nil, err
	}
	pinned, ok := input["pinned"].(bool)
	if !ok {
		return nil, errors.New("set_thread_pinned requires pinned")
	}
	target.setPinned(pinned)
	return map[string]any{"threadId": threadID, "pinned": pinned}, nil
}

func (a *neoActor) executeLocalUpdateThreadLabelsTool(input map[string]any, add bool) (map[string]any, error) {
	target, threadID, err := a.threadToolTargetActor(input, true)
	if err != nil {
		return nil, err
	}
	requested := neoThreadLabelsFromAny(input["labels"])
	if len(requested) == 0 {
		return nil, errors.New("thread label tool requires labels")
	}
	labels := requested
	if add {
		labels = target.addThreadLabels(requested)
	} else {
		labels = target.removeThreadLabels(requested)
	}
	return map[string]any{"threadId": threadID, "labels": labels}, nil
}

func (a *neoActor) executeLocalArchiveThreadsTool(input map[string]any) (map[string]any, error) {
	threadIDs := neoThreadToolInputThreadIDs(input)
	if len(threadIDs) == 0 {
		return nil, errors.New("archive_threads requires threadIds")
	}
	if a.runtime == nil || a.runtime.store == nil {
		return nil, errors.New("archive_threads missing local runtime")
	}
	targets := make([]*neoActor, 0, len(threadIDs))
	for _, threadID := range threadIDs {
		if !neoThreadIDExactPattern.MatchString(threadID) {
			return nil, errors.New("archive_threads received an invalid threadIds entry")
		}
		target, _, err := a.threadToolTargetActor(map[string]any{"threadId": threadID}, true)
		if err != nil {
			return nil, err
		}
		targets = append(targets, target)
	}
	archived := make([]any, 0, len(threadIDs))
	for index, target := range targets {
		target.archiveThread(true, nil)
		archived = append(archived, threadIDs[index])
	}
	return map[string]any{"archived": archived}, nil
}

func (a *neoActor) executeLocalThreadInteractTool(pending neoPendingTool) (map[string]any, error) {
	action := strings.ToLower(strings.TrimSpace(stringValue(pending.Input["action"])))
	var result map[string]any
	var err error
	switch action {
	case "get":
		result, err = a.executeLocalGetThreadMetadataTool(pending.Input)
	case "message":
		result, err = a.executeLocalSendMessageToThreadTool(pending)
	case "archive":
		result, err = a.executeLocalArchiveThreadTool(pending.Input, true)
	case "unarchive":
		result, err = a.executeLocalArchiveThreadTool(pending.Input, false)
	default:
		return nil, errors.New("thread_interact action must be get, message, archive, or unarchive")
	}
	if err != nil {
		return nil, err
	}
	threadID := firstNonEmptyString(result["threadID"], result["threadId"], neoThreadToolInputThreadID(pending.Input))
	if threadID == "" && action != "message" {
		threadID = a.threadID
	}
	if threadID != "" {
		result["threadId"] = threadID
		result["threadID"] = threadID
		result["url"] = "https://ampcode.com/threads/" + threadID
	}
	return result, nil
}

func (a *neoActor) executeLocalSendMessageToThreadTool(pending neoPendingTool) (map[string]any, error) {
	input := pending.Input
	threadID := neoThreadToolInputThreadID(input)
	if !neoThreadIDExactPattern.MatchString(threadID) {
		return nil, errors.New("send_message_to_thread requires a valid target threadId")
	}
	if threadID == a.threadID {
		return nil, errors.New("send_message_to_thread cannot target the current thread")
	}
	if a.runtime == nil || a.runtime.store == nil {
		return nil, errors.New("send_message_to_thread missing local runtime")
	}
	if _, _, err := a.threadToolTargetActor(map[string]any{"threadId": threadID}, false); err != nil {
		return nil, err
	}
	workflow := neoSendMessageToThreadWorkflow(input)
	content := input["content"]
	if !neoMessageContentPresent(content) && strings.TrimSpace(stringValue(input["message"])) != "" {
		content = []any{map[string]any{"type": "text", "text": strings.TrimSpace(stringValue(input["message"]))}}
	}
	if workflow == "" && !neoMessageContentPresent(content) {
		return nil, errors.New("send_message_to_thread requires content, message, or workflow")
	}
	parentToolCallID := firstNonEmptyString(input["parentToolCallId"], input["parentToolUseId"], input["parent_tool_use_id"], pending.ParentToolCallID, pending.ID)
	a.handleSendMessageToThread(map[string]any{
		"type":             "send_message_to_thread",
		"targetThreadId":   threadID,
		"workflow":         workflow,
		"content":          content,
		"comment":          stringValue(input["comment"]),
		"messageId":        firstNonEmptyString(input["messageId"], newNeoMessageID()),
		"parentToolCallId": parentToolCallID,
	})
	return map[string]any{"threadId": threadID, "workflow": workflow, "sent": true}, nil
}

func (a *neoActor) executeLocalDownloadThreadFileTool(input map[string]any) (map[string]any, error) {
	target, threadID, err := a.threadToolTargetActor(input, false)
	if err != nil {
		return nil, err
	}
	sourceRoot := target.localThreadToolWorkingDirectory()
	destinationRoot := a.localThreadToolDestinationWorkingDirectory()
	if sourceRoot == "" || destinationRoot == "" {
		return nil, errors.New("download_thread_file requires both thread workspaces to be available on the proxy host")
	}
	sourceInput := strings.TrimSpace(stringValue(input["path"]))
	if sourceInput == "" {
		return nil, errors.New("download_thread_file requires path")
	}
	sourcePath, _, err := neoThreadFileReadPath(sourceRoot, sourceInput)
	if err != nil {
		return nil, fmt.Errorf("download_thread_file: %w", err)
	}
	destinationInput := strings.TrimSpace(stringValue(input["destination"]))
	if destinationInput == "" {
		destinationInput = filepath.Base(sourceInput)
	}
	destinationPath, err := neoThreadFileWritePath(destinationRoot, destinationInput, boolValue(input["overwrite"]), true)
	if err != nil {
		return nil, fmt.Errorf("download_thread_file: %w", err)
	}
	written, err := neoCopyThreadFile(sourceRoot, sourcePath, destinationRoot, destinationPath, boolValue(input["overwrite"]))
	if err != nil {
		return nil, fmt.Errorf("download_thread_file: %w", err)
	}
	return map[string]any{"threadId": threadID, "path": sourceInput, "destination": destinationPath, "localPath": destinationPath, "remotePath": sourceInput, "bytes": written}, nil
}

func (a *neoActor) executeLocalUploadThreadFileTool(input map[string]any) (map[string]any, error) {
	target, threadID, err := a.threadToolTargetActor(input, false)
	if err != nil {
		return nil, err
	}
	sourceRoot := a.localThreadToolWorkingDirectory()
	destinationRoot := target.localThreadToolWorkingDirectory()
	if sourceRoot == "" || destinationRoot == "" {
		return nil, errors.New("upload_thread_file requires both thread workspaces to be available on the proxy host")
	}
	sourceInput := strings.TrimSpace(stringValue(input["path"]))
	if sourceInput == "" {
		return nil, errors.New("upload_thread_file requires path")
	}
	sourcePath, _, err := neoThreadFileReadPath(sourceRoot, sourceInput)
	if err != nil {
		return nil, fmt.Errorf("upload_thread_file: %w", err)
	}
	destinationInput := strings.TrimSpace(stringValue(input["destination"]))
	if destinationInput == "" {
		destinationInput = filepath.Base(sourceInput)
	}
	if filepath.IsAbs(destinationInput) {
		return nil, errors.New("upload_thread_file destination must be relative to the target workspace")
	}
	destinationPath, err := neoThreadFileWritePath(destinationRoot, destinationInput, boolValue(input["overwrite"]), false)
	if err != nil {
		return nil, fmt.Errorf("upload_thread_file: %w", err)
	}
	written, err := neoCopyThreadFile(sourceRoot, sourcePath, destinationRoot, destinationPath, boolValue(input["overwrite"]))
	if err != nil {
		return nil, fmt.Errorf("upload_thread_file: %w", err)
	}
	return map[string]any{"threadId": threadID, "path": sourceInput, "destination": destinationInput, "localPath": sourcePath, "remotePath": destinationInput, "bytes": written}, nil
}

func (a *neoActor) localThreadToolWorkingDirectory() string {
	if a == nil {
		return ""
	}
	a.mu.Lock()
	runnerID := firstNonEmptyString(a.meta["runnerId"], a.meta["runnerID"])
	workingDirectory, workspaceRoot := neoResolvedEnvironmentWorkspacePaths(a.environment)
	a.mu.Unlock()
	if runnerID != "" {
		return ""
	}
	workingDirectory = neoExistingDirectory(workingDirectory)
	workspaceRoot = neoExistingDirectory(workspaceRoot)
	if workspaceRoot != "" && (workingDirectory == "" || neoThreadFilePathWithin(workspaceRoot, workingDirectory)) {
		return workspaceRoot
	}
	return workingDirectory
}

func (a *neoActor) localThreadToolDestinationWorkingDirectory() string {
	if a == nil {
		return ""
	}
	a.mu.Lock()
	runnerID := firstNonEmptyString(a.meta["runnerId"], a.meta["runnerID"])
	workingDirectory := neoWorkingDirectoryFromEnvironment(a.environment)
	a.mu.Unlock()
	if runnerID != "" {
		return ""
	}
	return workingDirectory
}

func neoThreadFileReadPath(root, value string) (string, os.FileInfo, error) {
	realRoot, target, err := neoThreadFileScopedPath(root, value, true)
	if err != nil {
		return "", nil, err
	}
	realTarget, err := filepath.EvalSymlinks(target)
	if err != nil {
		return "", nil, err
	}
	if !neoThreadFilePathWithin(realRoot, realTarget) {
		return "", nil, errors.New("source resolves outside the workspace")
	}
	info, err := os.Stat(realTarget)
	if err != nil {
		return "", nil, err
	}
	if !info.Mode().IsRegular() {
		return "", nil, errors.New("source is not a regular file")
	}
	if info.Size() > neoFilesystemWriteMaxBytes {
		return "", nil, fmt.Errorf("source exceeds the %d MiB limit", neoFilesystemWriteMaxBytes/(1024*1024))
	}
	return realTarget, info, nil
}

func neoThreadFileWritePath(root, value string, overwrite, createParents bool) (string, error) {
	realRoot, target, err := neoThreadFileScopedPath(root, value, false)
	if err != nil {
		return "", err
	}
	if createParents {
		rootPath, err := filepath.Abs(root)
		if err != nil {
			return "", err
		}
		relativeTarget, err := filepath.Rel(rootPath, target)
		if err != nil || relativeTarget == ".." || strings.HasPrefix(relativeTarget, ".."+string(filepath.Separator)) {
			return "", errors.New("destination is outside the workspace")
		}
		rootedDestination, err := os.OpenRoot(realRoot)
		if err != nil {
			return "", err
		}
		err = rootedDestination.MkdirAll(filepath.Dir(relativeTarget), 0o755)
		if errClose := rootedDestination.Close(); errClose != nil {
			log.Errorf("amp thread file: close destination workspace root: %v", errClose)
		}
		if err != nil {
			return "", err
		}
	}
	realParent, err := filepath.EvalSymlinks(filepath.Dir(target))
	if err != nil {
		return "", errors.New("destination parent directory must already exist")
	}
	if !neoThreadFilePathWithin(realRoot, realParent) {
		return "", errors.New("destination resolves outside the workspace")
	}
	target = filepath.Join(realParent, filepath.Base(target))
	if info, statErr := os.Lstat(target); statErr == nil {
		if !overwrite {
			return "", errors.New("destination already exists")
		}
		if info.IsDir() {
			return "", errors.New("destination is a directory")
		}
		if realTarget, evalErr := filepath.EvalSymlinks(target); evalErr != nil || !neoThreadFilePathWithin(realRoot, realTarget) {
			return "", errors.New("destination resolves outside the workspace")
		}
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return "", statErr
	}
	return target, nil
}

func neoThreadFileScopedPath(root, value string, requireExisting bool) (string, string, error) {
	root = neoExistingDirectory(root)
	value = strings.TrimSpace(value)
	if root == "" || value == "" {
		return "", "", errors.New("workspace path is unavailable")
	}
	rootPath, err := filepath.Abs(root)
	if err != nil {
		return "", "", err
	}
	target := value
	if !filepath.IsAbs(target) {
		target = filepath.Join(rootPath, target)
	}
	target, err = filepath.Abs(target)
	if err != nil {
		return "", "", err
	}
	if !neoThreadFilePathWithin(rootPath, target) {
		return "", "", errors.New("path is outside the workspace")
	}
	realRoot, err := filepath.EvalSymlinks(rootPath)
	if err != nil {
		return "", "", err
	}
	if requireExisting {
		if _, err := os.Stat(target); err != nil {
			return "", "", err
		}
	}
	return realRoot, target, nil
}

func neoThreadFilePathWithin(root, target string) bool {
	relative, err := filepath.Rel(root, target)
	if err != nil {
		return false
	}
	return relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func neoCopyThreadFile(sourceRoot, sourcePath, destinationRoot, destinationPath string, overwrite bool) (int, error) {
	realSourceRoot, err := filepath.EvalSymlinks(sourceRoot)
	if err != nil {
		return 0, err
	}
	sourceRelative, err := filepath.Rel(realSourceRoot, sourcePath)
	if err != nil || sourceRelative == ".." || strings.HasPrefix(sourceRelative, ".."+string(filepath.Separator)) {
		return 0, errors.New("source is outside the workspace")
	}
	rootedSource, err := os.OpenRoot(realSourceRoot)
	if err != nil {
		return 0, err
	}
	defer func() {
		if err := rootedSource.Close(); err != nil {
			log.Errorf("amp thread file: close source workspace root: %v", err)
		}
	}()
	source, err := rootedSource.Open(sourceRelative)
	if err != nil {
		return 0, err
	}
	defer func() {
		if err := source.Close(); err != nil {
			log.Errorf("amp thread file: close source file: %v", err)
		}
	}()
	sourceInfo, err := source.Stat()
	if err != nil {
		return 0, err
	}
	if !sourceInfo.Mode().IsRegular() {
		return 0, errors.New("source is not a regular file")
	}
	if sourceInfo.Size() > neoFilesystemWriteMaxBytes {
		return 0, fmt.Errorf("source exceeds the %d MiB limit", neoFilesystemWriteMaxBytes/(1024*1024))
	}
	if destinationInfo, err := os.Stat(destinationPath); err == nil && os.SameFile(sourceInfo, destinationInfo) {
		return 0, errors.New("source and destination are the same file")
	}
	raw, err := io.ReadAll(io.LimitReader(source, int64(neoFilesystemWriteMaxBytes)+1))
	if err != nil {
		return 0, err
	}
	if len(raw) > neoFilesystemWriteMaxBytes {
		return 0, fmt.Errorf("source exceeds the %d MiB limit", neoFilesystemWriteMaxBytes/(1024*1024))
	}
	mode := sourceInfo.Mode().Perm()
	if mode == 0 {
		mode = 0o600
	}
	realDestinationRoot, err := filepath.EvalSymlinks(destinationRoot)
	if err != nil {
		return 0, err
	}
	destinationRelative, err := filepath.Rel(realDestinationRoot, destinationPath)
	if err != nil || destinationRelative == ".." || strings.HasPrefix(destinationRelative, ".."+string(filepath.Separator)) {
		return 0, errors.New("destination is outside the workspace")
	}
	rootedDestination, err := os.OpenRoot(realDestinationRoot)
	if err != nil {
		return 0, err
	}
	defer func() {
		if err := rootedDestination.Close(); err != nil {
			log.Errorf("amp thread file: close destination workspace root: %v", err)
		}
	}()
	if overwrite {
		if destinationInfo, err := rootedDestination.Stat(destinationRelative); err == nil {
			if destinationInfo.IsDir() {
				return 0, errors.New("destination is a directory")
			}
			if destinationMode := destinationInfo.Mode().Perm(); destinationMode != 0 {
				mode = destinationMode
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return 0, err
		}
	}
	if !overwrite {
		destination, err := rootedDestination.OpenFile(destinationRelative, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
		if err != nil {
			return 0, err
		}
		removeDestination := true
		defer func() {
			if removeDestination {
				if errRemove := rootedDestination.Remove(destinationRelative); errRemove != nil && !errors.Is(errRemove, os.ErrNotExist) {
					log.Errorf("amp thread file: remove failed destination file: %v", errRemove)
				}
			}
		}()
		if _, err := destination.Write(raw); err != nil {
			if errClose := destination.Close(); errClose != nil {
				log.Errorf("amp thread file: close failed destination file: %v", errClose)
			}
			return 0, err
		}
		if err := destination.Sync(); err != nil {
			if errClose := destination.Close(); errClose != nil {
				log.Errorf("amp thread file: close failed destination file: %v", errClose)
			}
			return 0, err
		}
		if err := destination.Close(); err != nil {
			return 0, err
		}
		removeDestination = false
		return len(raw), nil
	}
	tempRelative := filepath.Join(filepath.Dir(destinationRelative), ".cliproxy-thread-file-"+randomBase62(16)+".tmp")
	temp, err := rootedDestination.OpenFile(tempRelative, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return 0, err
	}
	removeTemp := true
	defer func() {
		if removeTemp {
			if errRemove := rootedDestination.Remove(tempRelative); errRemove != nil && !errors.Is(errRemove, os.ErrNotExist) {
				log.Errorf("amp thread file: remove failed temporary file: %v", errRemove)
			}
		}
	}()
	if _, err := temp.Write(raw); err != nil {
		if errClose := temp.Close(); errClose != nil {
			log.Errorf("amp thread file: close failed temporary file: %v", errClose)
		}
		return 0, err
	}
	if err := temp.Sync(); err != nil {
		if errClose := temp.Close(); errClose != nil {
			log.Errorf("amp thread file: close failed temporary file: %v", errClose)
		}
		return 0, err
	}
	if err := temp.Close(); err != nil {
		return 0, err
	}
	err = rootedDestination.Rename(tempRelative, destinationRelative)
	if err != nil {
		return 0, err
	}
	removeTemp = false
	return len(raw), nil
}

func (a *neoActor) threadToolOwnerID() string {
	if a == nil {
		return ""
	}
	a.mu.Lock()
	ownerID := firstNonEmptyString(a.meta["ownerUserId"], a.meta["ownerUserID"], a.meta["creatorUserID"], a.meta["creatorUserId"])
	a.mu.Unlock()
	if ownerID == "" {
		return neoLocalOwnerUserID
	}
	return ownerID
}

func (a *neoActor) ensureOwnedThreadActor(threadID string) (*neoActor, error) {
	if a == nil || a.runtime == nil || a.runtime.store == nil {
		return nil, errors.New("thread actor missing local runtime")
	}
	ownerID := a.threadToolOwnerID()
	if target := a.runtime.store.lookupThreadActor(threadID); target != nil {
		if target.threadToolOwnerID() != ownerID && !a.runtime.claimNeoLegacyThreadActorOwner(target, ownerID) {
			return nil, errors.New("thread target is not owned by the current user")
		}
		return target, nil
	}
	if a.runtime.threadDir != "" {
		if thread, exists := loadNeoThreadFromDir(threadID, a.runtime.threadDir); exists {
			if !a.runtime.neoThreadOwnedByRequestUser(thread, ownerID) {
				return nil, errors.New("thread target is not owned by the current user")
			}
			target := a.runtime.store.ensureThreadActor(threadID)
			if target == nil || target.threadToolOwnerID() != ownerID && !a.runtime.claimNeoLegacyThreadActorOwner(target, ownerID) {
				return nil, errors.New("thread target is not owned by the current user")
			}
			return target, nil
		}
	}
	target := a.runtime.store.ensureThreadActorWithLocalImport(threadID, false)
	if target == nil {
		return nil, errors.New("thread actor could not be created")
	}
	target.mu.Lock()
	targetOwnerID := neoThreadOwnerUserID(map[string]any{"meta": target.meta})
	if targetOwnerID == "" {
		target.meta["creatorUserID"] = ownerID
		target.meta["ownerUserId"] = ownerID
	}
	target.mu.Unlock()
	if targetOwnerID != "" && targetOwnerID != ownerID {
		return nil, errors.New("thread target is not owned by the current user")
	}
	return target, nil
}

func (a *neoActor) threadToolTargetActor(input map[string]any, allowCurrent bool) (*neoActor, string, error) {
	threadID := neoThreadToolInputThreadID(input)
	if threadID == "" && allowCurrent {
		threadID = a.threadID
	}
	if !neoThreadIDExactPattern.MatchString(threadID) {
		return nil, "", errors.New("thread tool requires a valid threadId")
	}
	if threadID == a.threadID {
		if !allowCurrent {
			return nil, "", errors.New("thread tool cannot target the current thread")
		}
		return a, threadID, nil
	}
	if a.runtime == nil || a.runtime.store == nil {
		return nil, "", errors.New("thread tool missing local runtime")
	}
	target := a.runtime.store.lookupThreadActor(threadID)
	if (target == nil || !target.hasLocalThreadState()) && a.runtime.threadDir != "" {
		if thread, exists := loadNeoThreadFromDir(threadID, a.runtime.threadDir); exists {
			if target == nil {
				target = a.runtime.store.ensureThreadActorWithLocalImport(threadID, false)
			}
			if err := target.importThreadLocalOnlyIfEmpty(thread); err != nil {
				return nil, "", fmt.Errorf("thread tool could not load target thread: %w", err)
			}
		}
	}
	if target == nil {
		return nil, "", errors.New("thread tool could not resolve target thread")
	}
	ownerID := a.threadToolOwnerID()
	if target.threadToolOwnerID() != ownerID && !a.runtime.claimNeoLegacyThreadActorOwner(target, ownerID) {
		return nil, "", errors.New("thread tool target is not owned by the current user")
	}
	return target, threadID, nil
}

func neoThreadToolInputThreadID(input map[string]any) string {
	raw := firstNonEmptyString(input["threadID"], input["threadId"], input["thread_id"], input["targetThreadId"], input["targetThreadID"], input["target_thread_id"], input["thread"], input["url"])
	return neoThreadIDFromString(raw)
}

func neoThreadToolInputThreadIDs(input map[string]any) []string {
	rawItems := firstArray(input["threadIds"], input["threadIDs"], input["thread_ids"], input["threads"])
	threadIDs := make([]string, 0, len(rawItems))
	seen := map[string]bool{}
	for _, item := range rawItems {
		threadID := ""
		if m, ok := asMap(item); ok {
			threadID = neoThreadToolInputThreadID(m)
		} else {
			threadID = neoThreadIDFromString(stringValue(item))
		}
		if threadID == "" || seen[threadID] {
			continue
		}
		seen[threadID] = true
		threadIDs = append(threadIDs, threadID)
	}
	return threadIDs
}

func neoThreadIDFromString(raw string) string {
	raw = strings.TrimSpace(neoBinaryThreadInputTrim.ReplaceAllString(raw, ""))
	if raw == "" {
		return ""
	}
	return neoThreadIDFromURLOrValue(raw)
}

func neoThreadIDFromURLOrValue(raw string) string {
	if neoThreadIDExactPattern.MatchString(raw) || neoBinaryThreadIDExactPattern.MatchString(raw) {
		return raw
	}
	return neoThreadIDFromURL(raw)
}

func neoThreadIDFromURL(raw string) string {
	input := map[string]any{"threadID": raw}
	return neoToolInputThreadID(input)
}
