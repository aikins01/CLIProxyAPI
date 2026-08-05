package amp

import "strings"

func neoSyntheticLocalToolSpec(toolName string) (neoToolSpec, bool) {
	switch strings.TrimSpace(toolName) {
	case "read_thread":
		return neoReadThreadToolSpec(), true
	case "edit_file":
		return neoEditFileToolSpec(), true
	case "run_check":
		return neoRunCheckToolSpec(), true
	case "submit_review":
		return neoSubmitReviewToolSpec(), true
	case "find_thread", "list_agent_modes", "list_runners", "create_thread", "get_current_user_identity", "thread_interact", "get_thread_metadata", "update_thread", "rename_thread", "set_thread_pinned", "add_thread_labels", "remove_thread_labels", "archive_current_thread", "archive_thread", "archive_threads", "unarchive_thread", "send_message_to_thread", "download_thread_file", "upload_thread_file", "get_schedule", "set_schedule", "update_schedule", "clear_schedule":
		return neoThreadToolSpec(toolName)
	case "read_github", "search_github", "commit_search", "diff", "list_directory_github", "list_repositories", "glob_github", "github_repo_ci_status":
		return neoGitHubToolSpec(toolName)
	default:
		return neoSubagentExposureSpec(toolName)
	}
}

// neoEditFileToolSpec mirrors the Amp binary's builtin edit_file spec. the
// binary's default tools.disable contains "builtin:edit_file" because in
// Amp's cloud architecture the server owns the model-facing edit_file spec
// and the client's builtin copy would be a duplicate advertisement. locally
// we are the server, so we synthesize the spec ourselves; the source is
// deliberately not "builtin" so the default suppression of the client copy
// does not also remove this server-side spec (an explicit "edit_file"
// disable pattern still matches by name).
func neoEditFileToolSpec() neoToolSpec {
	return neoToolSpec{
		Name: "edit_file",
		Description: "Make edits to a text file.\n\n" +
			"Replaces `old_str` with `new_str` in the given file.\n\n" +
			"Returns a git-style diff showing the changes made as formatted markdown, along with the line range ([startLine, endLine]) of the changed content. The diff is also shown to the user.\n\n" +
			"The file specified by `path` MUST exist, and it MUST be an absolute path. If you need to create a new file, use `create_file` instead.\n\n" +
			"`old_str` MUST exist in the file. Use tools like `Read` to understand the files you are editing before changing them.\n\n" +
			"`old_str` and `new_str` MUST be different from each other.\n\n" +
			"Set `replace_all` to true to replace all occurrences of `old_str` in the file. Else, `old_str` MUST be unique within the file or the edit will fail. Additional lines of context can be added to make the string more unique.\n\n" +
			"If you need to replace the entire contents of a file, use `create_file` instead, since it requires less tokens for the same action (since you won't have to repeat the contents before replacing).\n\n" +
			"If you see `[REDACTED:_____]` in your inputs and edits fail, Amp's secret redaction may have changed the text; ask the user to manually make the edit.\n",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"path":        map[string]any{"type": "string", "description": "The absolute path to the file (MUST be absolute, not relative). File must exist. ALWAYS generate this argument first."},
				"old_str":     map[string]any{"type": "string", "description": "Text to search for. Must match exactly."},
				"new_str":     map[string]any{"type": "string", "description": "Text to replace old_str with."},
				"replace_all": map[string]any{"type": "boolean", "default": false, "description": "Set to true to replace all matches of old_str. Else, old_str must be an unique match."},
			},
			"required": []any{"path", "old_str", "new_str"},
		},
		Meta: map[string]any{"source": "server"},
	}
}

func neoReadThreadToolSpec() neoToolSpec {
	return neoToolSpec{
		Name: "read_thread",
		Description: "Read and extract relevant content from another Amp thread by its ID or ampcode.com URL.\n\n" +
			"This tool fetches a thread (locally or from the server if synced), renders it as markdown, and uses AI to extract only the information relevant to your specific goal. This keeps context concise while preserving important details.\n\n" +
			"## When to use this tool\n\n" +
			"- When the user pastes or references an Amp thread URL on ampcode.com whose last path segment is a thread ID (for example https://ampcode.com/threads/T-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx or https://ampcode.com/v2/workspace/project/T-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx) in their message\n" +
			"- When the user references a thread ID (format: T-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx or @T-abc123)\n" +
			"- When you need to search or extract specific information from the current thread without loading the whole current thread into context\n" +
			"- When the user asks to \"apply the same approach from [thread URL]\"\n" +
			"- When the user says \"do what we did in [thread URL]\"\n" +
			"- When the user says \"implement the plan we devised in [thread URL]\"\n" +
			"- When you need to extract specific information from a referenced thread\n\n" +
			"## When NOT to use this tool\n\n" +
			"- When neither a referenced thread nor older current-thread context is needed\n" +
			"- When the needed current-thread context is already visible in the conversation\n\n" +
			"- **threadID**: Optional for the current thread. For another thread, use the thread identifier in format T-{uuid}, or an ampcode.com URL whose last path segment is T-{uuid} (e.g., \"T-a38f981d-52da-47b1-818c-fbaa9ab56e0c\" or \"https://ampcode.com/v2/workspace/project/T-a38f981d-52da-47b1-818c-fbaa9ab56e0c\")\n" +
			"- **goal**: A clear description of what information you're looking for in that thread. Be specific about what you need to extract.\n\n" +
			"Examples:\n" +
			"- User asks \"Implement the plan we devised in https://ampcode.com/threads/T-3f1beb2b-bded-4fda-96cc-1af7192f24b6\"\n" +
			"```json\n{\"threadID\":\"T-3f1beb2b-bded-4fda-96cc-1af7192f24b6\",\"goal\":\"Extract the implementation plan, design decisions, architecture approach, and any code patterns or examples discussed\"}\n```\n" +
			"- User asks: \"Do what we did in https://ampcode.com/threads/T-f916b832-c070-4853-8ab3-5e7596953bec, but for the Oracle tool\"\n" +
			"```json\n{\"threadID\":\"T-f916b832-c070-4853-8ab3-5e7596953bec\",\"goal\":\"Extract the implementation approach, code patterns, techniques used, and any relevant code examples that can be adapted for the Oracle tool\"}\n```\n" +
			"- User asks: \"Take the SQL queries from https://ampcode.com/threads/T-95e73a95-f4fe-4f22-8d5c-6297467c97a5 and turn it into a reusable script\"\n" +
			"```json\n{\"threadID\":\"T-95e73a95-f4fe-4f22-8d5c-6297467c97a5\",\"goal\":\"Extract all SQL queries, their purpose, parameters, and any context needed to understand how to make them reusable\"}\n```\n" +
			"- User asks: \"Apply the same fix from https://ampcode.com/v2/amp/amp/T-019d01b5-f70d-73ea-9445-f6d358f7213e to this issue\"\n" +
			"```json\n{\"threadID\":\"https://ampcode.com/v2/amp/amp/T-019d01b5-f70d-73ea-9445-f6d358f7213e\",\"goal\":\"Extract the bug description, root cause, the fix or solution, and relevant code changes\"}\n```\n" +
			"- User asks: \"Apply the same fix from @T-95e73a95-f4fe-4f22-8d5c-6297467c97a5 to this issue\"\n" +
			"```json\n{\"threadID\":\"T-95e73a95-f4fe-4f22-8d5c-6297467c97a5\",\"goal\":\"Extract the bug description, root cause, the fix/solution, and relevant code changes\"}\n```",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"threadID": map[string]any{"type": "string", "description": "Optional for the current thread. For another thread, use the thread ID in format T-{uuid}, or an ampcode.com URL whose last path segment is T-{uuid} (e.g., \"T-a38f981d-52da-47b1-818c-fbaa9ab56e0c\" or \"https://ampcode.com/v2/workspace/project/T-a38f981d-52da-47b1-818c-fbaa9ab56e0c\")"},
				"goal":     map[string]any{"type": "string", "description": "A clear description of what information you need from the thread. Be specific about what to extract."},
			},
			"required": []any{"goal"},
		},
		Meta: map[string]any{"source": "builtin"},
	}
}
