import type { ConfiguredCoreFactory } from "../agent-task-runtime";
import type { TaskCore } from "../task-core";
import { createClaudeTaskAdapter } from "./adapter";
import type { ClaudeTaskAdapter, McpToolResult } from "./adapter";
import { CLAUDE_PLUGIN } from "./constants";
import { readClaudeEnvironment } from "./environment";
import { isJsonRpcError, isRecord, JSON_RPC_ERROR, parseJsonRpc } from "./json-rpc";
import type { JsonRpcError, JsonRpcId, JsonRpcMessage, JsonRpcRequest } from "./json-rpc";
import type { CommandRunner, WakeTimers, WakeTiming } from "./wake";

export const SERVER_VERSION = "0.2.0";

export const SERVER_INSTRUCTIONS = [
	"Wolfpack task delegation tools (agent_task_*), the same tools and semantics as the pi extension.",
	`A <channel source="${CLAUDE_PLUGIN.serverName}"> message is a wolfpack task notification. It carries only a pointer, never the task content.`,
	"When one arrives, call agent_task_inbox; the inbox result contains the delivered task messages and any assignment.",
	"The same pointer may also be typed into the prompt as a fallback when the channel does not wake the session; treat it the same way.",
].join("\n");

export interface ClaudeTaskServerOptions {
	readonly env: Readonly<Record<string, string | undefined>>;
	send(message: JsonRpcMessage): void;
	/** Test seam: a fixed task core instead of the configured Wolfpack transport. */
	readonly core?: TaskCore;
	readonly createCore?: ConfiguredCoreFactory;
	readonly runCommand?: CommandRunner;
	readonly timers?: WakeTimers;
	readonly timing?: WakeTiming;
	readonly log?: (message: string) => void;
}

export interface ClaudeTaskServer {
	handle(message: unknown): Promise<void>;
	handleLine(line: string): Promise<void>;
	shutdown(): Promise<void>;
}

/** Echo the client's protocol revision below the ceiling; answer the pinned revision otherwise. ISO dates compare lexically. */
export function negotiateProtocolVersion(proposed: unknown): string {
	return typeof proposed === "string" && /^\d{4}-\d{2}-\d{2}$/.test(proposed) && proposed < CLAUDE_PLUGIN.protocolCeiling ? proposed : CLAUDE_PLUGIN.fallbackProtocolVersion;
}

const realTimers: WakeTimers = {
	now: () => Date.now(),
	setTimeout(fire, ms) {
		const timer = setTimeout(fire, ms);
		timer.unref?.();
		return () => clearTimeout(timer);
	},
};

const noCommands: CommandRunner = async () => { throw new Error("no command runner configured"); };

/** Hand-rolled MCP over newline-delimited JSON-RPC. No SDK: the surface is initialize, ping, tools and one notification. */
export function createClaudeTaskServer(options: ClaudeTaskServerOptions): ClaudeTaskServer {
	const environment = readClaudeEnvironment(options.env);
	const send = options.send;
	const adapter: ClaudeTaskAdapter = createClaudeTaskAdapter({
		environment,
		...(options.core && { core: options.core }),
		...(options.createCore && { createCore: options.createCore }),
		sendChannel: (meta) => send({ jsonrpc: "2.0", method: CLAUDE_PLUGIN.channelMethod, params: { content: CLAUDE_PLUGIN.pointer, meta } }),
		runCommand: options.runCommand ?? noCommands,
		timers: options.timers ?? realTimers,
		...(options.timing && { timing: options.timing }),
		log: options.log ?? (() => undefined),
	});
	let negotiated: { protocolVersion: string; clientProtocolVersion: string | null } | undefined;
	const inFlight = new Map<JsonRpcId, AbortController>();
	let closed = false;

	const reply = (id: JsonRpcId, result: unknown): void => send({ jsonrpc: "2.0", id, result });
	const fail = (id: JsonRpcId | null, error: JsonRpcError): void => send({ jsonrpc: "2.0", id, error });

	const callTool = async (id: JsonRpcId, params: unknown): Promise<void> => {
		if (!isRecord(params) || typeof params.name !== "string") { fail(id, { code: JSON_RPC_ERROR.invalidParams, message: "tools/call requires a tool name" }); return; }
		const meta = isRecord(params._meta) ? params._meta : {};
		let result: McpToolResult;
		if (params.name === CLAUDE_PLUGIN.lifecycleTool) {
			// Hooks call without a tool-use id; a model call always carries one.
			result = meta[CLAUDE_PLUGIN.toolUseMeta] !== undefined
				? { content: [{ type: "text", text: "lifecycle is reserved for this plugin's Claude Code hooks" }], isError: true }
				: { content: [{ type: "text", text: await adapter.hook(params.arguments) }] };
		} else {
			const controller = new AbortController();
			inFlight.set(id, controller);
			try { result = await adapter.callTool(params.name, params.arguments, controller.signal); } finally { inFlight.delete(id); }
		}
		reply(id, result);
	};

	const dispatch = async (request: JsonRpcRequest): Promise<void> => {
		const { id, method, params } = request;
		if (id === undefined) {
			if (method === "notifications/initialized" && negotiated) await adapter.start(negotiated);
			else if (method === "notifications/cancelled" && isRecord(params) && (typeof params.requestId === "string" || typeof params.requestId === "number")) inFlight.get(params.requestId)?.abort();
			return;
		}
		switch (method) {
			case "initialize": {
				const proposed = isRecord(params) ? params.protocolVersion : undefined;
				negotiated = { protocolVersion: negotiateProtocolVersion(proposed), clientProtocolVersion: typeof proposed === "string" ? proposed : null };
				reply(id, {
					protocolVersion: negotiated.protocolVersion,
					capabilities: { tools: {}, ...(adapter.channel && { experimental: { [CLAUDE_PLUGIN.channelCapability]: {} } }) },
					serverInfo: { name: CLAUDE_PLUGIN.serverName, version: SERVER_VERSION },
					instructions: SERVER_INSTRUCTIONS,
				});
				return;
			}
			case "ping":
				reply(id, {});
				return;
			case "tools/list":
				reply(id, { tools: adapter.tools });
				return;
			case "tools/call":
				await callTool(id, params);
				return;
			default:
				fail(id, { code: JSON_RPC_ERROR.methodNotFound, message: `method not found: ${method.slice(0, 64)}` });
		}
	};

	const server: ClaudeTaskServer = {
		handle(message) { return server.handleLine(JSON.stringify(message)); },
		async handleLine(line) {
			if (closed || line.trim() === "") return;
			const parsed = parseJsonRpc(line);
			if (parsed === undefined) return;
			if (isJsonRpcError(parsed)) { fail(null, parsed); return; }
			try { await dispatch(parsed); } catch (error) {
				if (parsed.id !== undefined) fail(parsed.id, { code: JSON_RPC_ERROR.internal, message: error instanceof Error ? error.message : String(error) });
			}
		},
		async shutdown() {
			if (closed) return;
			closed = true;
			for (const controller of inFlight.values()) controller.abort();
			await adapter.shutdown();
		},
	};
	return server;
}
