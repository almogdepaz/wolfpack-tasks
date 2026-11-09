/**
 * Every Claude Code name the adapter, plugin manifests, hooks and tests agree on.
 * Hook server names follow Claude Code's `plugin:<plugin>:<server>`; tool names follow `mcp__plugin_<plugin>_<server>__<tool>`.
 */
export const CLAUDE_PLUGIN = Object.freeze({
	marketplace: "wolfpack-tasks",
	plugin: "wolfpack-tasks",
	/** Marketplace-relative plugin root. */
	pluginSource: "./claude-plugin",
	/** `mcpServers` key in the plugin manifest. */
	serverKey: "tasks",
	serverName: "wolfpack-tasks",
	hookServer: "plugin:wolfpack-tasks:tasks",
	toolPrefix: "mcp__plugin_wolfpack-tasks_tasks__",
	/** Hook-only tool; model calls are rejected. */
	lifecycleTool: "lifecycle",
	skill: "wolfpack-task-delegation",
	/** Plugin-root-relative bundle path. */
	bundle: "dist/server.mjs",
	/** Plugin-root-relative path of the fail-closed `PreToolUse` command hook script. */
	gateBundle: "dist/worker-gate.mjs",
	channelMethod: "notifications/claude/channel",
	channelCapability: "claude/channel",
	/** The only channel content; task payloads are returned by agent_task_inbox, never pushed. */
	pointer: "wolfpack task mail: call agent_task_inbox",
	/** Claude Code `_meta` key on model tool calls; hook-originated calls do not carry it. */
	toolUseMeta: "claudecode/toolUseId",
	/** Client protocol versions below this date are echoed; others are answered with the fallback version. */
	protocolCeiling: "2026-07-28",
	fallbackProtocolVersion: "2025-11-25",
	/** State-directory-relative status file directory. */
	statusDirectory: "wolfpack-tasks/claude",
	/** The worker gate socket lives in this mode-0700 subdirectory of the status directory as `<hash>.sock`. */
	gateSocketDirectory: "gate",
	/** Hex characters of the session id's SHA-256 in the gate socket name. */
	gateSocketHashChars: 16,
});

/** Claude Code harness tools a worker may use before its assignment: ToolSearch loads the deferred task tools. */
export const HARNESS_PRE_ASSIGNMENT_TOOLS: ReadonlySet<string> = new Set(["ToolSearch"]);
