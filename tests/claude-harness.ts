import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { createClaudeTaskServer } from "../src/claude/mcp-server";
import type { ClaudeTaskServerOptions } from "../src/claude/mcp-server";
import type { JsonRpcMessage } from "../src/claude/json-rpc";

export const SESSION_ID = "0b6f3a52-6a39-4c55-9a0e-1c2d3e4f5a6b";

/** Wolfpack identity plus an isolated state directory. */
export function wolfpackEnv(overrides: Readonly<Record<string, string | undefined>> = {}): Record<string, string | undefined> {
	return { WOLFPACK_SESSION_ID: SESSION_ID, WOLFPACK_SESSION_NAME: "claude-worker", WOLFPACK_DIR: mkdtempSync(join(tmpdir(), "wolfpack-tasks-claude-")), HOME: "/home/test", ...overrides };
}

export interface Harness {
	readonly sent: JsonRpcMessage[];
	readonly server: ReturnType<typeof createClaudeTaskServer>;
	request(method: string, params?: unknown): Promise<JsonRpcMessage>;
	notify(method: string, params?: unknown): Promise<void>;
	callTool(name: string, args?: unknown, meta?: Record<string, unknown>): Promise<ToolCallResult>;
	hook(input: Record<string, string>): Promise<string>;
	notifications(method?: string): JsonRpcMessage[];
	initialize(protocolVersion?: string): Promise<JsonRpcMessage>;
}

export interface ToolCallResult {
	readonly content: readonly { readonly type: string; readonly text: string }[];
	readonly structuredContent?: Record<string, unknown>;
	readonly isError?: boolean;
}

export function startHarness(options: Omit<ClaudeTaskServerOptions, "send">): Harness {
	const sent: JsonRpcMessage[] = [];
	const server = createClaudeTaskServer({ ...options, send: (message) => { sent.push(message); } });
	let id = 0;
	const request = async (method: string, params?: unknown): Promise<JsonRpcMessage> => {
		const requestId = ++id;
		await server.handle({ jsonrpc: "2.0", id: requestId, method, ...(params !== undefined && { params }) });
		const response = sent.find((message) => "id" in message && message.id === requestId && !("method" in message));
		if (!response) throw new Error(`no response to ${method}`);
		return response;
	};
	let toolUse = 0;
	const harness: Harness = {
		sent,
		server,
		request,
		async notify(method, params) { await server.handle({ jsonrpc: "2.0", method, ...(params !== undefined && { params }) }); },
		async callTool(name, args = {}, meta = { "claudecode/toolUseId": `toolu_${++toolUse}` }) {
			const response = await request("tools/call", { name, arguments: args, _meta: meta });
			if (!("result" in response)) throw new Error(`tools/call failed: ${JSON.stringify(response)}`);
			return response.result as ToolCallResult;
		},
		async hook(input) {
			const result = await harness.callTool("lifecycle", input, {});
			if (result.isError) throw new Error(`lifecycle hook failed: ${result.content[0]?.text}`);
			return result.content[0]?.text ?? "";
		},
		notifications(method) { return sent.filter((message) => "method" in message && !("id" in message) && (method === undefined || message.method === method)); },
		initialize(protocolVersion = "2025-11-25") {
			return request("initialize", { protocolVersion, capabilities: {}, clientInfo: { name: "claude-code", version: "test" } });
		},
	};
	return harness;
}

/** Polls a condition on real time; the runtime's own refreshes are not virtualized. */
export async function waitFor(condition: () => boolean | Promise<boolean>, timeoutMs = 3_000): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (!await condition()) {
		if (Date.now() > deadline) throw new Error("waitFor: condition not met");
		await new Promise((resolve) => setTimeout(resolve, 5));
	}
}

export function sequence(prefix: string): () => string {
	let number = 0;
	return (): string => `${prefix}-${++number}`;
}
