import { createAgentTaskRuntime, WORKER_GATE_DENIAL_CODE } from "../agent-task-runtime";
import type { AgentTaskHost, AgentTaskRuntime, AgentTaskRuntimeContext, AgentTaskToolDefinition, AgentTaskToolResult, ConfiguredCoreFactory } from "../agent-task-runtime";
import { createConfiguredTaskCore } from "../configured-task-core";
import type { TaskCore } from "../task-core";
import { TaskEnvelopeKind, TaskProtocolError } from "../task-protocol";
import type { TaskInboxItem } from "../task-core";
import { CLAUDE_PLUGIN, HARNESS_PRE_ASSIGNMENT_TOOLS } from "./constants";
import type { ClaudeEnvironment } from "./environment";
import { isRecord } from "./json-rpc";
import { NOTIFY_PARENT_LISTING, NOTIFY_PARENT_TOOL, notifyParent, notifyParentAvailable } from "./notify-parent";
import { createStatusFile } from "./status-file";
import type { ClaudeStatusFile } from "./status-file";
import { validateToolArguments } from "./validation";
import { createWakeController } from "./wake";
import { listenWorkerGate, WORKER_GATE_ANSWER_WAIT_MS } from "./worker-gate";
import type { WorkerGateAnswer, WorkerGateListener } from "./worker-gate";
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
	/** The process environment, for local server auth. */
	readonly env: Readonly<Record<string, string | undefined>>;
	/** Test seam: the local server request of agent_notify_parent. */
	readonly fetch?: typeof fetch;
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

const INBOX_TOOL = "agent_task_inbox";
/** Any native tool name: the runtime gates every non-task tool alike, so this probe stands for all of them. */
const GATE_PROBE_TOOL = "Bash";
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
	/**
	 * Non-triggering records (relay-loss notes) inserted outside a presentation. They wait for the next inbox
	 * result, request no pointer, and never count as pending, so they cannot hold back later triggering mail.
	 */
	const deferredMessages: string[] = [];
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
			if (delivery.triggerTurn) requestPointer(message.details);
			else if (presenting) pendingMessages.push(message.content);
			else deferredMessages.push(message.content);
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

	// Worker mode only: the gate decision that the fail-closed PreToolUse command hook asks for over the gate socket
	// (see worker-gate.ts). It lives only in this process, so no answer can come from a previous server lifetime.
	const gateEnabled = runtime.workerGateEnabled;
	const GATE_STOPPED: WorkerGateAnswer = { authorized: false, reason: `${WORKER_GATE_DENIAL_CODE}: the wolfpack-tasks MCP server stopped` };
	let gateAnswer: WorkerGateAnswer = { authorized: false, reason: `${WORKER_GATE_DENIAL_CODE}: the worker gate has not been evaluated` };
	let gateListener: WorkerGateListener | undefined;
	let gateQueue: Promise<void> = Promise.resolve();
	let queuedGateRefresh: Promise<void> | undefined;
	/**
	 * Re-evaluates the gate. Serialized, so an older evaluation never overwrites a newer one. A refresh that has not
	 * started yet is shared: it still runs after every earlier change, and a hung evaluation cannot grow the queue.
	 */
	const refreshGate = (): Promise<void> => {
		if (!gateEnabled) return gateQueue;
		if (queuedGateRefresh) return queuedGateRefresh;
		const run = gateQueue.then(async () => {
			queuedGateRefresh = undefined;
			if (closed) return;
			let decision: { readonly block: boolean; readonly reason: string } | undefined;
			try {
				decision = await runtime.lifecycle.tool_call({ toolName: GATE_PROBE_TOOL, input: {} }, { sessionManager });
			} catch (error) {
				decision = { block: true, reason: error instanceof Error ? error.message : String(error) };
			}
			gateAnswer = decision?.block ? { authorized: false, reason: decision.reason } : { authorized: true, reason: null };
		});
		queuedGateRefresh = run;
		gateQueue = run;
		return run;
	};
	/** Answers one hook request: a fresh evaluation when it completes in time, otherwise the latest one. */
	const answerGate = async (): Promise<WorkerGateAnswer> => {
		if (closed) return GATE_STOPPED;
		let cancelWait: (() => void) | undefined;
		await Promise.race([refreshGate(), new Promise<void>((resolve) => { cancelWait = options.timers.setTimeout(resolve, WORKER_GATE_ANSWER_WAIT_MS); })]);
		cancelWait?.();
		return closed ? GATE_STOPPED : gateAnswer;
	};

	const notifyAvailable = notifyParentAvailable(environment, runtime.workerGateEnabled);
	const executeTool = async (name: string, args: unknown, signal: AbortSignal): Promise<McpToolResult> => {
		if (name === NOTIFY_PARENT_TOOL) {
			if (!notifyAvailable) return errorResult(`${NOTIFY_PARENT_TOOL} is not available: it requires WOLFPACK_PARENT_SESSION_ID and a session that is not a task worker`);
			wake?.activity("busy");
			return notifyParent(args, { environment, env: options.env, fetch: options.fetch ?? fetch, signal });
		}
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
	};

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
				const delivered = [...deferredMessages.splice(0), ...pendingMessages.splice(0)];
				const noted = notes.splice(0);
				return mcpResult(result, [...delivered, ...noted], true);
			} finally {
				presenting = false;
			}
		});
		inboxQueue = run.catch(() => undefined);
		return run;
	};

	return {
		tools: listing(runtime, true, notifyAvailable),
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
			if (gateEnabled && !closed) {
				await refreshGate();
				const listener = await listenWorkerGate({ path: environment.gateSocketPath, sessionId: environment.sessionId, answer: answerGate, log: options.log });
				// A shutdown during listen did not see this listener.
				if (closed) await listener?.close();
				else gateListener = listener;
			}
		},
		async callTool(name, args, signal) {
			const result = await executeTool(name, args, signal);
			// The next native tool call must see the authorization this result granted or revoked.
			await refreshGate();
			return result;
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
			await gateListener?.close();
			gateListener = undefined;
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
		tools: listing(runtime, false, false),
		channel: false,
		async start() { /* inactive */ },
		async callTool() { return errorResult(message); },
		async hook() { return ""; },
		async shutdown() { /* inactive */ },
	};
}

function listing(runtime: AgentTaskRuntime, active: boolean, notify: boolean): McpToolListing[] {
	const tools: McpToolListing[] = runtime.tools.map((tool) => ({
		name: tool.name,
		title: tool.label,
		description: tool.description,
		inputSchema: JSON.parse(JSON.stringify(tool.parameters)) as unknown,
		// The pointer names agent_task_inbox, so it must not be deferred behind tool search in a live session.
		...(active && tool.name === INBOX_TOOL && { _meta: { "anthropic/alwaysLoad": true } }),
	}));
	if (notify) tools.push(NOTIFY_PARENT_LISTING);
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

function mcpResult(result: AgentTaskToolResult, delivered: readonly string[], inbox = false): McpToolResult {
	const details = isRecord(result.details) ? result.details : { value: result.details };
	// Inbox summaries must not duplicate full event histories; agent_task_status still returns the full record.
	const structured = inbox && Array.isArray(details.tasks)
		? { ...details, tasks: details.tasks.map((task) => isRecord(task) ? { taskId: task.taskId, status: task.status } : task) }
		: details;
	const isError = isRecord(details.error);
	return {
		content: [...delivered, ...result.content.map((part) => part.text)].map((text) => ({ type: "text" as const, text })),
		structuredContent: delivered.length > 0 ? { ...structured, delivered } : structured,
		...(isError && { isError: true as const }),
	};
}

function errorResult(text: string): McpToolResult {
	return { content: [{ type: "text", text }], isError: true };
}
