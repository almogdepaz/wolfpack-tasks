import { describe, expect, test } from "bun:test";
import { existsSync, readdirSync } from "node:fs";

import { CLAUDE_PLUGIN } from "../src/claude/constants";
import { createInMemoryTaskRelay } from "../src/in-memory-task-relay";
import { createTaskCore } from "../src/task-core";
import { createTaskStore } from "../src/task-store";
import contract from "./fixtures/agent-task-tools.json";
import { sequence, startHarness, wolfpackEnv } from "./claude-harness";

const forbiddenCore = async (): Promise<never> => { throw new Error("no-op mode must not create a task core"); };
const forbiddenCommand = async (): Promise<never> => { throw new Error("no-op mode must not run commands"); };

function readyHarness(env = wolfpackEnv()) {
	const relay = createInMemoryTaskRelay("memory");
	const core = createTaskCore({ endpoint: { relay: "memory", id: "worker" }, relay, store: createTaskStore(), ids: sequence("worker") });
	return startHarness({ env, core, runCommand: forbiddenCommand });
}

describe("initialize", () => {
	test.each([
		["2025-06-18", "2025-06-18"],
		["2025-11-25", "2025-11-25"],
		["2026-07-27", "2026-07-27"],
		["2026-07-28", "2025-11-25"],
		["2027-01-01", "2025-11-25"],
		["draft", "2025-11-25"],
		[undefined, "2025-11-25"],
	] as const)("client protocol %p is answered with %p", async (proposed, answered) => {
		const harness = readyHarness();
		const response = await harness.request("initialize", { ...(proposed !== undefined && { protocolVersion: proposed }), capabilities: {}, clientInfo: { name: "claude-code", version: "test" } });
		expect(response).toMatchObject({ result: { protocolVersion: answered } });
	});

	test("declares tools, the claude channel capability and pointer instructions", async () => {
		const harness = readyHarness();
		const response = await harness.initialize();
		const result = (response as { result: { capabilities: Record<string, unknown>; instructions: string; serverInfo: { name: string } } }).result;
		expect(result.capabilities.tools).toEqual({});
		expect(result.capabilities.experimental).toEqual({ "claude/channel": {} });
		expect(result.instructions).toContain('<channel source="');
		expect(result.instructions).toContain("agent_task_inbox");
		expect(result.serverInfo.name).toBe(CLAUDE_PLUGIN.serverName);
	});
});

describe("json-rpc", () => {
	test("server/discover returns a JSON-RPC error before and after initialize", async () => {
		const harness = readyHarness();
		expect(await harness.request("server/discover")).toMatchObject({ error: { code: -32601 } });
		await harness.initialize();
		expect(await harness.request("server/discover")).toMatchObject({ error: { code: -32601 } });
	});

	test("unknown methods and malformed lines return JSON-RPC errors", async () => {
		const harness = readyHarness();
		expect(await harness.request("resources/list")).toMatchObject({ error: { code: -32601 } });
		await harness.server.handleLine("{not json");
		expect(harness.sent.at(-1)).toEqual({ jsonrpc: "2.0", id: null, error: { code: -32700, message: "parse error" } });
		await harness.server.handleLine("");
		expect(harness.sent).toHaveLength(2);
	});

	test("ping answers an empty result", async () => {
		expect(await readyHarness().request("ping")).toMatchObject({ result: {} });
	});
});

describe("without wolfpack identity", () => {
	test.each([
		["no identity", {}],
		["no session name", { WOLFPACK_SESSION_ID: "0b6f3a52-6a39-4c55-9a0e-1c2d3e4f5a6b" }],
		["no session id", { WOLFPACK_SESSION_NAME: "claude-worker" }],
		["blank identity", { WOLFPACK_SESSION_ID: " ", WOLFPACK_SESSION_NAME: " " }],
	] as const)("%s: registers nothing, contacts nothing and every tool says so", async (_label, identity) => {
		const env: Record<string, string | undefined> = { ...wolfpackEnv(), WOLFPACK_SESSION_ID: undefined, WOLFPACK_SESSION_NAME: undefined, ...identity };
		const harness = startHarness({ env, createCore: forbiddenCore, runCommand: forbiddenCommand });
		const initialized = await harness.initialize();
		await harness.notify("notifications/initialized");
		expect((initialized as { result: { capabilities: Record<string, unknown> } }).result.capabilities.experimental).toBeUndefined();
		const listed = await harness.request("tools/list");
		const names = (listed as { result: { tools: { name: string }[] } }).result.tools.map((tool) => tool.name);
		expect(names).toEqual([...contract.order.filter((entry) => entry.startsWith("tool:")).map((entry) => entry.slice(5)), CLAUDE_PLUGIN.lifecycleTool]);
		for (const name of names.filter((name) => name !== CLAUDE_PLUGIN.lifecycleTool)) {
			const result = await harness.callTool(name, {});
			expect(result.isError).toBe(true);
			expect(result.content[0]?.text).toContain("not a wolfpack session");
		}
		expect(await harness.hook({ event: "pre_tool", tool_name: "Bash" })).toBe("");
		expect(await harness.hook({ event: "stop" })).toBe("");
		await harness.server.shutdown();
		expect(harness.notifications()).toEqual([]);
		expect(readdirSync(env.WOLFPACK_DIR!)).toEqual([]);
	});
});

describe("misconfigured identity", () => {
	test.each([
		["WOLFPACK_PORT", { WOLFPACK_PORT: "http" }],
		["WOLFPACK_PORT", { WOLFPACK_PORT: "70000" }],
		["WOLFPACK_JWT_SECRET", { WOLFPACK_JWT_SECRET: "too-short" }],
		["WOLFPACK_DIR", { WOLFPACK_DIR: "relative/state" }],
		["WOLFPACK_SESSION_ID", { WOLFPACK_SESSION_ID: "../escape" }],
	] as const)("tools name %s", async (variable, overrides) => {
		const env = wolfpackEnv(overrides);
		const harness = startHarness({ env, createCore: forbiddenCore, runCommand: forbiddenCommand });
		await harness.initialize();
		await harness.notify("notifications/initialized");
		const result = await harness.callTool("agent_task_inbox");
		expect(result.isError).toBe(true);
		expect(result.content[0]?.text).toContain("misconfigured");
		expect(result.content[0]?.text).toContain(variable);
		expect(harness.notifications()).toEqual([]);
		if (env.WOLFPACK_DIR?.startsWith("/")) expect(existsSync(`${env.WOLFPACK_DIR}/wolfpack-tasks`)).toBe(false);
	});
});

describe("tool contract", () => {
	test("tools/list publishes the pi tool names, descriptions and parameters", async () => {
		const harness = readyHarness();
		await harness.initialize();
		const listed = await harness.request("tools/list");
		const tools = (listed as { result: { tools: { name: string; title: string; description: string; inputSchema: unknown }[] } }).result.tools;
		const taskTools = tools.filter((tool) => tool.name !== CLAUDE_PLUGIN.lifecycleTool);
		expect(taskTools.map((tool) => tool.name)).toEqual(contract.order.filter((entry) => entry.startsWith("tool:")).map((entry) => entry.slice(5)));
		const pi = contract.tools as Record<string, { label: string; description: string; parameters: unknown }>;
		for (const tool of taskTools) {
			expect({ title: tool.title, description: tool.description, inputSchema: tool.inputSchema }).toEqual({ title: pi[tool.name]!.label, description: pi[tool.name]!.description, inputSchema: pi[tool.name]!.parameters });
		}
	});

	test("invalid arguments fail with pi's validation message", async () => {
		const harness = readyHarness();
		await harness.initialize();
		const result = await harness.callTool("agent_task_send", { task: "no target" });
		expect(result.isError).toBe(true);
		expect(result.content[0]?.text).toStartWith('Validation failed for tool "agent_task_send":\n  - to: ');
		expect(result.content[0]?.text).toContain('Received arguments:\n{\n  "task": "no target"\n}');
	});

	test("unknown tools are tool errors, not protocol errors", async () => {
		const harness = readyHarness();
		await harness.initialize();
		const result = await harness.callTool("agent_task_unknown");
		expect(result.isError).toBe(true);
	});

	test("the hook endpoint rejects model calls", async () => {
		const harness = readyHarness();
		await harness.initialize();
		const result = await harness.callTool(CLAUDE_PLUGIN.lifecycleTool, { event: "stop" });
		expect(result.isError).toBe(true);
		expect(result.content[0]?.text).toContain("hooks");
	});
});
