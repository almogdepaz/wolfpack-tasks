import { createAgentTaskRuntime } from "../agent-task-runtime";
import type { AgentTaskHost, AgentTaskRuntime, AgentTaskRuntimeContext, AgentTaskToolDefinition, AgentTaskToolResult, ConfiguredCoreFactory } from "../agent-task-runtime";
import { createConfiguredTaskCore } from "../configured-task-core";
import type { TaskCore } from "../task-core";
import { TaskEnvelopeKind, TaskProtocolError } from "../task-protocol";
import type { TaskInboxItem } from "../task-core";
import { CLAUDE_PLUGIN } from "./constants";
import type { ClaudeEnvironment } from "./environment";
import { isRecord } from "./json-rpc";
import { createStatusFile } from "./status-file";
import type { ClaudeStatusFile } from "./status-file";
import { validateToolArguments } from "./validation";
import { createWakeController } from "./wake";
import type { CommandRunner, WakeController, WakeTimers, WakeTiming } from "./wake";

export interface McpToolResult {
	readonly content: readonly { readonly type: "text"; readonly text: string }[];
	readonly structuredContent?: Record<string, unknown>;
	readonly isError?: true;
}

export interface McpToolListing {
	readonly name: string;
	readonly title: string;
	readonly description: string;
	readonly inputSchema: unknown;
	readonly _meta?: Record<string, unknown>;
}

export interface ClaudeTaskAdapterOptions {
	readonly environment: ClaudeEnvironment;
	/** Test seam: a fixed core instead of the configured Wolfpack transport. */
	readonly core?: TaskCore;
	readonly createCore?: ConfiguredCoreFactory;
	sendChannel(meta: Readonly<Record<string, string>>): void;
	readonly runCommand: CommandRunner;
	readonly timers: WakeTimers;
	readonly timing?: WakeTiming;
	readonly log: (message: string) => void;
}

export interface ClaudeTaskAdapter {
	readonly tools: readonly McpToolListing[];
	/** Channel capability is declared only for a ready Wolfpack session. */
	readonly channel: boolean;
	start(protocol: { readonly protocolVersion: string; readonly clientProtocolVersion: string | null }): Promise<void>;
	callTool(name: string, args: unknown, signal: AbortSignal): Promise<McpToolResult>;
	/** Hook endpoint: returns hook output text, empty for no decision. */
	hook(input: unknown): Promise<string>;
	shutdown(): Promise<void>;
}

/** Tools a worker may need before its first assignment, beyond the runtime's own pre-assignment set. */
const HARNESS_PRE_ASSIGNMENT_TOOLS = new Set(["ToolSearch"]);
const INBOX_TOOL = "agent_task_inbox";
/** Events the plugin's hooks/hooks.json sends to the lifecycle tool. */
export const LIFECYCLE_HOOK_EVENTS = Object.freeze(["prompt", "pre_tool", "tool_batch", "stop", "stop_failure"] as const);
const LIFECYCLE_HOOK_FIELDS = Object.freeze(["session_id", "tool_name", "agent_id"] as const);

export function createClaudeTaskAdapter(options: ClaudeTaskAdapterOptions): ClaudeTaskAdapter {
	const environment = options.environment;
	if (environment.kind !== "ready") return inactiveAdapter(environment);

	const entries: unknown[] = [];
	const sessionManager = { getEntries: (): readonly unknown[] => entries };
	/** Model-visible messages inserted since the last inbox result. */
	const pendingMessages: string[] = [];
	const notes: string[] = [];
	let presenting = false;
	let lastIntake: readonly TaskInboxItem[] = [];
	let status: ClaudeStatusFile | undefined;
	let wake: WakeController | undefined;
	let started = false;
	let closed = false;

	const pointerMeta = (details?: unknown): Record<string, string> => {
		if (isRecord(details) && typeof details.taskId === "string") return { task_id: details.taskId, ...(typeof details.eventId === "string" && { event_id: details.eventId }) };
		// Hint only: the first model-facing item of the latest intake.
		for (const item of lastIntake) {
			if (item.kind === "local") return { task_id: item.event.taskId, event_id: item.event.eventId };
			if (item.delivery.envelope.kind !== TaskEnvelopeKind.intent) return { task_id: item.delivery.envelope.taskId };
		}
		return {};
	};
	const requestPointer = (details?: unknown): void => { if (!presenting) wake?.requestPointer(pointerMeta(details)); };

	// Pi entry shapes, so the shared evidence index (incorporation, wakes, records) reads them unchanged.
	const host: AgentTaskHost = {
		sendMessage(message, delivery) {
			entries.push({ type: "custom_message", customType: message.customType, content: message.content, display: message.display, details: message.details });
			if (!delivery.triggerTurn) pendingMessages.push(message.content);
			requestPointer(message.details);
		},
		appendEntry(customType, data) { entries.push({ type: "custom", customType, data }); },
	};
	// Background refreshes are never idle: they hold model-visible mail and point at it. Only an inbox call presents.
	const context: AgentTaskRuntimeContext = {
		isIdle(): boolean {
			if (presenting) return true;
			requestPointer();
			return false;
		},
		hasPendingMessages: (): boolean => !presenting && pendingMessages.length > 0,
		sessionManager,
		ui: {
			setStatus(_key, text) { status?.update({ lastStatus: text ?? null }); },
			notify(message, type = "info") { notes.push(`${type}: ${message}`); },
			theme: { fg: (_color, text) => text },
		},
	};
	const observeIntake = (core: TaskCore): TaskCore => ({
		...core,
		async intakeInbox(signal, work) {
			const items = await core.intakeInbox(signal, work);
			lastIntake = items;
			return items;
		},
	});
	const createCore: ConfiguredCoreFactory = options.createCore ?? ((signal, rebind) => createConfiguredTaskCore({ sessionName: environment.sessionName, rebind }, signal));
	const runtime = options.core
		? createAgentTaskRuntime(host, observeIntake(options.core))
		: createAgentTaskRuntime(host, undefined, async (signal, rebind) => observeIntake(await createCore(signal, rebind)) as Awaited<ReturnType<ConfiguredCoreFactory>>);
	const tools = new Map(runtime.tools.map((tool) => [tool.name, tool]));

	let inboxQueue: Promise<unknown> = Promise.resolve();
	const runInbox = (tool: AgentTaskToolDefinition, params: unknown, signal: AbortSignal): Promise<McpToolResult> => {
		const run = inboxQueue.then(async (): Promise<McpToolResult> => {
			const pointed = wake?.pointerOutstanding === true;
			wake?.inboxCalled();
			presenting = true;
			try {
				let result = await tool.execute("inbox", params as never, signal, undefined, context);
				// A refresh already in flight may have held the pointed mail before presentation began; one more pass presents it.
				if (pointed && pendingMessages.length === 0 && !signal.aborted) result = await tool.execute("inbox", params as never, signal, undefined, context);
				const delivered = pendingMessages.splice(0);
				const noted = notes.splice(0);
				return mcpResult(result, [...delivered, ...noted]);
			} finally {
				presenting = false;
			}
		});
		inboxQueue = run.catch(() => undefined);
		return run;
	};

	return {
		tools: listing(runtime, true),
		channel: true,
		async start(protocol) {
			if (started || closed) return;
			started = true;
			const now = options.timers.now();
			status = createStatusFile(environment.statusPath, {
				sessionId: environment.sessionId, sessionName: environment.sessionName, pid: process.pid, wake: "channel",
				protocolVersion: protocol.protocolVersion, clientProtocolVersion: protocol.clientProtocolVersion, startedAt: now,
				lastPointerAt: null, lastInboxAt: null, lastNudgeAt: null, fallbackAt: null, lastNudgeError: null, statusFailures: 0, wakeStalledAt: null, lastStatus: null,
			}, options.timers.now, options.log);
			wake = createWakeController({
				sessionId: environment.sessionId,
				sendChannel: options.sendChannel,
				runCommand: options.runCommand,
				timers: options.timers,
				...(options.timing && { timing: options.timing }),
				onChange: (patch) => status?.update(patch),
			});
			await runtime.lifecycle.session_start({}, context);
		},
		async callTool(name, args, signal) {
			const tool = tools.get(name);
			if (!tool) return errorResult(`unknown tool ${JSON.stringify(name.slice(0, 64))}`);
			let params: unknown;
			try { params = validateToolArguments(name, tool.parameters, args ?? {}); } catch (error) { return errorResult(error instanceof Error ? error.message : String(error)); }
			// A model tool call means the session is working, even if the activity hooks are unavailable.
			wake?.activity("busy");
			const decision = await runtime.lifecycle.tool_call({ toolName: name, input: params }, { sessionManager, signal });
			if (decision?.block) return errorResult(decision.reason);
			try {
				if (name === INBOX_TOOL) return await runInbox(tool, params, signal);
				return mcpResult(await tool.execute(name, params as never, signal, undefined, context), []);
			} catch (error) {
				return errorResult(error instanceof Error ? error.message : String(error));
			}
		},
		async hook(input) {
			if (!isRecord(input) || closed) return "";
			switch (input.event) {
				case "prompt":
					wake?.activity("busy");
					return "";
				case "pre_tool": {
					wake?.activity("busy");
					const toolName = typeof input.tool_name === "string" ? input.tool_name : "";
					// Own tools are gated inside tools/call with their validated input.
					if (!runtime.workerGateEnabled || toolName.startsWith(CLAUDE_PLUGIN.toolPrefix) || HARNESS_PRE_ASSIGNMENT_TOOLS.has(toolName)) return "";
					const decision = await runtime.lifecycle.tool_call({ toolName, input: {} }, { sessionManager });
					return decision?.block ? JSON.stringify({ hookSpecificOutput: { hookEventName: "PreToolUse", permissionDecision: "deny", permissionDecisionReason: decision.reason } }) : "";
				}
				case "tool_batch":
					void runtime.lifecycle.turn_end({}, context);
					return "";
				case "stop":
				case "stop_failure":
					wake?.activity("idle");
					if (started) void runtime.lifecycle.agent_end({}, context);
					return "";
				default:
					return "";
			}
		},
		async shutdown() {
			if (closed) return;
			closed = true;
			wake?.close();
			await runtime.lifecycle.session_shutdown({}, context);
		},
	};
}

/** No identity or a misconfiguration: the same tool list, no runtime start, no contact, no files. */
function inactiveAdapter(environment: Exclude<ClaudeEnvironment, { kind: "ready" }>): ClaudeTaskAdapter {
	const refuse = async (): Promise<never> => { throw new TaskProtocolError("RELAY_UNAVAILABLE", "task runtime is inactive", { retryable: false }); };
	const inert: AgentTaskHost = { sendMessage() { /* inactive */ }, appendEntry() { /* inactive */ } };
	const runtime = createAgentTaskRuntime(inert, undefined, refuse);
	const message = environment.kind === "inactive"
		? "not a wolfpack session: WOLFPACK_SESSION_ID and WOLFPACK_SESSION_NAME are required; task tools are unavailable"
		: `wolfpack tasks misconfigured: ${environment.variable} ${environment.reason}`;
	return {
		tools: listing(runtime, false),
		channel: false,
		async start() { /* inactive */ },
		async callTool() { return errorResult(message); },
		async hook() { return ""; },
		async shutdown() { /* inactive */ },
	};
}

function listing(runtime: AgentTaskRuntime, active: boolean): McpToolListing[] {
	const tools: McpToolListing[] = runtime.tools.map((tool) => ({
		name: tool.name,
		title: tool.label,
		description: tool.description,
		inputSchema: JSON.parse(JSON.stringify(tool.parameters)) as unknown,
		// The pointer names agent_task_inbox, so it must not be deferred behind tool search in a live session.
		...(active && tool.name === INBOX_TOOL && { _meta: { "anthropic/alwaysLoad": true } }),
	}));
	tools.push({
		name: CLAUDE_PLUGIN.lifecycleTool,
		title: "Task lifecycle hook endpoint",
		description: "Internal endpoint for this plugin's Claude Code hooks. Model calls are rejected.",
		inputSchema: {
			type: "object",
			additionalProperties: false,
			required: ["event"],
			properties: { event: { type: "string", enum: [...LIFECYCLE_HOOK_EVENTS] }, ...Object.fromEntries(LIFECYCLE_HOOK_FIELDS.map((key) => [key, { type: "string", maxLength: 256 }])) },
		},
		_meta: { "anthropic/alwaysLoad": false },
	});
	return tools;
}

function mcpResult(result: AgentTaskToolResult, delivered: readonly string[]): McpToolResult {
	const details = isRecord(result.details) ? result.details : { value: result.details };
	const isError = isRecord(details.error);
	return {
		content: [...delivered, ...result.content.map((part) => part.text)].map((text) => ({ type: "text" as const, text })),
		structuredContent: delivered.length > 0 ? { ...details, delivered } : details,
		...(isError && { isError: true as const }),
	};
}

function errorResult(text: string): McpToolResult {
	return { content: [{ type: "text", text }], isError: true };
}
