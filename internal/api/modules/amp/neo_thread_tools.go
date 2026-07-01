package amp

import (
	"errors"
	"strings"
)

func isNeoThreadTool(name string) bool {
	switch strings.TrimSpace(name) {
	case "create_thread", "archive_thread", "archive_threads", "unarchive_thread", "send_message_to_thread":
		return true
	default:
		return false
	}
}

func neoThreadToolSpec(toolName string) (neoToolSpec, bool) {
	name := strings.TrimSpace(toolName)
	threadID := map[string]any{"type": "string", "description": "Amp thread ID or thread URL."}
	switch name {
	case "create_thread":
		return neoToolSpec{
			Name:        name,
			Description: "Create a new Amp thread and relate it to the current thread.",
			InputSchema: neoThreadToolSchema(map[string]any{
				"threadId": map[string]any{"type": "string", "description": "Optional thread ID to create. Omit to create a new local thread ID."},
				"comment":  map[string]any{"type": "string", "description": "Optional relationship note."},
			}, nil),
			Meta: map[string]any{"source": "server"},
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
	target := firstNonEmptyString(input["threadId"], input["threadID"], input["targetThreadId"], input["url"])
	switch strings.TrimSpace(name) {
	case "create_thread":
		return "Creating thread"
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
	default:
		return strings.TrimSpace(name)
	}
}

func (a *neoActor) executeLocalThreadTool(pending neoPendingTool) (map[string]any, error) {
	switch strings.TrimSpace(pending.Name) {
	case "create_thread":
		return a.executeLocalCreateThreadTool(pending.Input)
	case "archive_thread":
		return a.executeLocalArchiveThreadTool(pending.Input, true)
	case "unarchive_thread":
		return a.executeLocalArchiveThreadTool(pending.Input, false)
	case "archive_threads":
		return a.executeLocalArchiveThreadsTool(pending.Input)
	case "send_message_to_thread":
		return a.executeLocalSendMessageToThreadTool(pending)
	default:
		return nil, errors.New("unsupported thread tool")
	}
}

func (a *neoActor) executeLocalCreateThreadTool(input map[string]any) (map[string]any, error) {
	threadID := firstNonEmptyString(input["threadId"], input["threadID"], input["thread_id"])
	if threadID == "" {
		threadID = "T-" + randomUUIDLike()
	}
	if !neoThreadIDExactPattern.MatchString(threadID) {
		return nil, errors.New("create_thread requires a valid threadId")
	}
	a.handleCreateThread(map[string]any{"type": "create_thread", "threadId": threadID, "comment": stringValue(input["comment"])})
	return map[string]any{"threadId": threadID, "created": true}, nil
}

func (a *neoActor) executeLocalArchiveThreadTool(input map[string]any, archive bool) (map[string]any, error) {
	target, threadID, err := a.threadToolTargetActor(input, true)
	if err != nil {
		return nil, err
	}
	target.archiveThread(archive, nil)
	return map[string]any{"threadId": threadID, "archived": archive}, nil
}

func (a *neoActor) executeLocalArchiveThreadsTool(input map[string]any) (map[string]any, error) {
	threadIDs := neoThreadToolInputThreadIDs(input)
	if len(threadIDs) == 0 {
		return nil, errors.New("archive_threads requires threadIds")
	}
	if a.runtime == nil || a.runtime.store == nil {
		return nil, errors.New("archive_threads missing local runtime")
	}
	archived := make([]any, 0, len(threadIDs))
	for _, threadID := range threadIDs {
		if !neoThreadIDExactPattern.MatchString(threadID) {
			return nil, errors.New("archive_threads received an invalid threadIds entry")
		}
		target := a.runtime.store.ensureThreadActor(threadID)
		if target == nil {
			return nil, errors.New("archive_threads could not resolve thread")
		}
		target.archiveThread(true, nil)
		archived = append(archived, threadID)
	}
	return map[string]any{"archived": archived}, nil
}

func (a *neoActor) executeLocalSendMessageToThreadTool(pending neoPendingTool) (map[string]any, error) {
	input := pending.Input
	threadID := neoThreadToolInputThreadID(input)
	if !neoThreadIDExactPattern.MatchString(threadID) {
		return nil, errors.New("send_message_to_thread requires a valid target threadId")
	}
	if a.runtime == nil || a.runtime.store == nil {
		return nil, errors.New("send_message_to_thread missing local runtime")
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

func (a *neoActor) threadToolTargetActor(input map[string]any, allowCurrent bool) (*neoActor, string, error) {
	threadID := neoThreadToolInputThreadID(input)
	if threadID == "" && allowCurrent {
		threadID = a.threadID
	}
	if !neoThreadIDExactPattern.MatchString(threadID) {
		return nil, "", errors.New("thread tool requires a valid threadId")
	}
	if threadID == a.threadID {
		return a, threadID, nil
	}
	if a.runtime == nil || a.runtime.store == nil {
		return nil, "", errors.New("thread tool missing local runtime")
	}
	target := a.runtime.store.ensureThreadActor(threadID)
	if target == nil {
		return nil, "", errors.New("thread tool could not resolve target thread")
	}
	return target, threadID, nil
}

func neoThreadToolInputThreadID(input map[string]any) string {
	raw := firstNonEmptyString(input["threadID"], input["threadId"], input["thread_id"], input["targetThreadId"], input["targetThreadID"], input["target_thread_id"], input["url"])
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
