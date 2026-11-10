import { afterEach, expect, test } from "bun:test";

import { SERVER_INSTRUCTIONS } from "../src/claude/mcp-server";
import { createInMemoryTaskRelay } from "../src/in-memory-task-relay";
import { createTaskCore } from "../src/task-core";
import { createTaskStore } from "../src/task-store";
import { sequence, startHarness, wolfpackEnv } from "./claude-harness";
import type { Harness } from "./claude-harness";

const TOOL = "agent_notify_parent";
const SECRET = "0123456789abcdef0123456789abcdef";
const noCommands = async (): Promise<never> => { throw new Error("unexpected command"); };
const harnesses: Harness[] = [];
afterEach(async () => {
	for (const harness of harnesses.splice(0)) await harness.server.shutdown();
	delete process.env.WOLFPACK_TASK_WORKER;
});

interface Call { readonly url: string; readonly method: string | undefined; readonly headers: Headers; readonly body: unknown }
function server(answer: (call: Call) => Response | Promise<Response>) {
	const calls: Call[] = [];
	const fetcher = Object.assign(async (input: Parameters<typeof fetch>[0], init?: RequestInit) => {
		const call = { url: String(input), method: init?.method, headers: new Headers(init?.headers), body: JSON.parse(String(init?.body)) as unknown };
		calls.push(call);
		return answer(call);
	}, { preconnect: fetch.preconnect }) as typeof fetch;
	return { calls, fetcher };
}

async function child(env: Record<string, string | undefined>, fetcher: typeof fetch = server(() => { throw new Error("unexpected fetch"); }).fetcher, worker = false) {
	if (worker) process.env.WOLFPACK_TASK_WORKER = "1";
	const relay = createInMemoryTaskRelay("memory");
	const core = createTaskCore({ endpoint: { relay: "memory", id: "child" }, relay, store: createTaskStore(), ids: sequence("child") });
	await core.connect();
	const harness = startHarness({ env, core, runCommand: noCommands, fetch: fetcher });
	harnesses.push(harness);
	await harness.initialize("2025-11-25");
	await harness.notify("notifications/initialized");
	return harness;
}

const listed = async (harness: Harness) => ((await harness.request("tools/list")) as { result: { tools: { name: string; description: string; inputSchema: unknown }[] } }).result.tools;
const parentEnv = (overrides: Record<string, string | undefined> = {}) => wolfpackEnv({ WOLFPACK_PARENT_SESSION_ID: " parent-session-1 ", WOLFPACK_PARENT_SESSION_NAME: " lead ", ...overrides });

test("agent_notify_parent is listed only for a non-worker session with a parent session id", async () => {
	expect((await listed(await child(wolfpackEnv()))).map(tool => tool.name)).not.toContain(TOOL);
	const tools = await listed(await child(parentEnv()));
	const tool = tools.find(entry => entry.name === TOOL);
	expect(tool?.description).toContain("wolfpack agent notify-parent");
	expect(tool?.inputSchema).toMatchObject({ type: "object", required: ["message"] });
	const worker = await child(parentEnv(), undefined, true);
	expect((await listed(worker)).map(entry => entry.name)).not.toContain(TOOL);
	const refused = await worker.callTool(TOOL, { message: "done" });
	expect(refused.isError).toBe(true);
	expect(refused.content[0]?.text).toContain("not available");
});

test("agent_notify_parent posts the CLI body to the local server with local auth and returns the server result", async () => {
	const result = { ok: true, delivered: 0, sent: 0, failed: 0, parentInbox: { queued: true, seq: 7 } };
	const local = server(() => Response.json(result));
	const harness = await child(parentEnv({ WOLFPACK_PORT: "4321", WOLFPACK_JWT_SECRET: SECRET }), local.fetcher);
	const answer = await harness.callTool(TOOL, { message: "phase 1 committed" });
	expect(answer.isError).toBeUndefined();
	expect(answer.structuredContent).toEqual(result);
	expect(answer.content[0]?.text).toContain('"queued":true');
	expect(local.calls).toHaveLength(1);
	const [call] = local.calls;
	expect([call!.url, call!.method]).toEqual(["http://127.0.0.1:4321/api/notify", "POST"]);
	expect(call!.body).toEqual({ message: "phase 1 committed", from: "claude-worker", sessionId: "parent-session-1", sessionName: "lead" });
	expect(call!.headers.get("content-type")).toBe("application/json");
	expect(call!.headers.get("authorization")).toMatch(/^Bearer [\w-]+\.[\w-]+\.[\w-]+$/);
});

test("agent_notify_parent sends no parent target unless both parent id and name are set", async () => {
	const local = server(() => Response.json({ ok: true, parentInbox: { queued: false, reason: "no_parent" } }));
	const harness = await child(parentEnv({ WOLFPACK_PARENT_SESSION_NAME: " " }), local.fetcher);
	const answer = await harness.callTool(TOOL, { message: "hello" });
	expect(answer.structuredContent).toEqual({ ok: true, parentInbox: { queued: false, reason: "no_parent" } });
	expect(local.calls[0]!.body).toEqual({ message: "hello", from: "claude-worker" });
	expect(local.calls[0]!.headers.has("authorization")).toBe(false);
});

test("agent_notify_parent accepts at most 500 code points and refuses empty messages", async () => {
	const local = server(() => Response.json({ ok: true }));
	const harness = await child(parentEnv(), local.fetcher);
	expect((await harness.callTool(TOOL, { message: "🐺".repeat(500) })).isError).toBeUndefined();
	const tooLong = await harness.callTool(TOOL, { message: "x".repeat(501) });
	expect(tooLong.isError).toBe(true);
	expect(tooLong.content[0]?.text).toContain("500");
	expect((await harness.callTool(TOOL, { message: "" })).isError).toBe(true);
	expect((await harness.callTool(TOOL, {})).isError).toBe(true);
	expect(local.calls).toHaveLength(1);
});

test("agent_notify_parent reports an unreachable server and a server refusal as errors", async () => {
	const down = server(() => { throw Object.assign(new TypeError("Unable to connect. Is the computer able to access the url?"), { code: "ConnectionRefused" }); });
	const unreachable = await (await child(parentEnv({ WOLFPACK_PORT: "4321" }), down.fetcher)).callTool(TOOL, { message: "done" });
	expect(unreachable.isError).toBe(true);
	expect(unreachable.content[0]?.text).toContain("wolfpack server unreachable at http://127.0.0.1:4321/api/notify");
	const limited = server(() => Response.json({ error: "rate limit exceeded" }, { status: 429 }));
	const refused = await (await child(parentEnv(), limited.fetcher)).callTool(TOOL, { message: "done" });
	expect(refused.isError).toBe(true);
	expect(refused.content[0]?.text).toContain("429");
	expect(refused.content[0]?.text).toContain("rate limit exceeded");
});

test("the MCP server instructions name agent_notify_parent as the claude replacement for notify-parent", () => {
	expect(SERVER_INSTRUCTIONS).toContain(TOOL);
	expect(SERVER_INSTRUCTIONS).toContain("wolfpack agent notify-parent");
});
