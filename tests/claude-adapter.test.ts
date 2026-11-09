import { afterEach, describe, expect, test } from "bun:test";
import { existsSync, lstatSync, mkdirSync, readFileSync, statSync } from "node:fs";
import { dirname, join } from "node:path";

import { CLAUDE_PLUGIN } from "../src/claude/constants";
import { readClaudeEnvironment } from "../src/claude/environment";
import { queryWorkerGate } from "../src/claude/worker-gate";
import { createInMemoryTaskRelay } from "../src/in-memory-task-relay";
import { createTaskCore } from "../src/task-core";
import type { TaskCore } from "../src/task-core";
import { createTaskStore } from "../src/task-store";
import { TaskProtocolError } from "../src/task-protocol";
import { runTaskRelayConformance } from "../src/task-relay-conformance";
import { WORKER_COORDINATION_FORBIDDEN_CODE, WORKER_GATE_DENIAL_CODE } from "../src/agent-task-runtime";
import { SESSION_ID, sequence, startHarness, waitFor, wolfpackEnv } from "./claude-harness";
import type { Harness } from "./claude-harness";

const META_KEY = /^[a-zA-Z_][a-zA-Z0-9_]*$/;
const noCommands = async (): Promise<never> => { throw new Error("unexpected command"); };
const harnesses: Harness[] = [];
/** The relay behind every claude-path test below; the claude adapter adds no relay of its own. */
const claudePathRelay = () => createInMemoryTaskRelay("memory");

describe("claude adapter path relay conformance", () => {
	runTaskRelayConformance(claudePathRelay);
});

afterEach(async () => {
	for (const harness of harnesses.splice(0)) await harness.server.shutdown();
	delete process.env.WOLFPACK_TASK_WORKER;
});

async function fixture(options: { worker?: boolean; env?: Record<string, string | undefined> } = {}) {
	if (options.worker) process.env.WOLFPACK_TASK_WORKER = "1";
	const relay = claudePathRelay();
	const origin = createTaskCore({ endpoint: { relay: "memory", id: "parent" }, relay, store: createTaskStore(), ids: sequence("parent") });
	const worker = createTaskCore({ endpoint: { relay: "memory", id: "worker" }, relay, store: createTaskStore(), ids: sequence("worker") });
	await origin.connect();
	await worker.connect();
	const env = options.env ?? wolfpackEnv();
	const harness = startHarness({ env, core: worker, runCommand: noCommands });
	harnesses.push(harness);
	await harness.initialize("2025-11-25");
	await harness.notify("notifications/initialized");
	return { origin, worker, harness, env };
}

const pointers = (harness: Harness) => harness.notifications(CLAUDE_PLUGIN.channelMethod);

async function assign(origin: TaskCore, worker: TaskCore, harness: Harness, task: string): Promise<string> {
	const { taskId } = await origin.createTask({ target: worker.endpoint, task, timeoutMs: 60_000 });
	await harness.hook({ event: "stop" });
	return taskId;
}

test("task mail emits one pointer until agent_task_inbox returns it", async () => {
	const { origin, worker, harness } = await fixture();
	const first = await assign(origin, worker, harness, "first assignment body");
	await waitFor(() => pointers(harness).length === 1);
	const [pointer] = pointers(harness) as { params: { content: string; meta: Record<string, unknown> } }[];
	expect(pointer!.params.content).toBe(CLAUDE_PLUGIN.pointer);
	expect(pointer!.params.content).not.toContain("first assignment body");
	for (const [key, value] of Object.entries(pointer!.params.meta)) {
		expect(key).toMatch(META_KEY);
		expect(typeof value).toBe("string");
	}
	expect(pointer!.params.meta.task_id).toBe(first);

	await assign(origin, worker, harness, "second assignment body");
	await new Promise((resolve) => setTimeout(resolve, 50));
	expect(pointers(harness)).toHaveLength(1);

	const inbox = await harness.callTool("agent_task_inbox");
	expect(inbox.isError).toBeUndefined();
	const text = inbox.content.map((part) => part.text).join("\n");
	expect(text).toContain("first assignment body");
	expect(text).toContain("second assignment body");

	await assign(origin, worker, harness, "third assignment body");
	await waitFor(() => pointers(harness).length === 2);

	// The origin learns the wake was accepted only after the inbox returned the assignment.
	await waitFor(async () => {
		await origin.receive();
		return origin.getTask(first)!.events.some((event) => event.type === "task.delivery_receipt" && event.payload.stage === "wake_accepted");
	});
});

test("an inbox result returns each assignment once", async () => {
	const { origin, worker, harness } = await fixture();
	await assign(origin, worker, harness, "only once body");
	await waitFor(() => pointers(harness).length === 1);
	const first = await harness.callTool("agent_task_inbox");
	const second = await harness.callTool("agent_task_inbox");
	expect(first.content.map((part) => part.text).join("\n").match(/## task assignment\ntask:/g)).toHaveLength(1);
	expect(second.content.map((part) => part.text).join("\n")).not.toContain("## task assignment\ntask:");
	expect(first.structuredContent).toMatchObject({ tasks: [{ status: "active" }] });
});

test("the status file records the wake mode and protocol version", async () => {
	const { env } = await fixture();
	const path = join(env.WOLFPACK_DIR!, "wolfpack-tasks", "claude", `${SESSION_ID}.json`);
	const status = JSON.parse(readFileSync(path, "utf8")) as Record<string, unknown>;
	expect(status).toMatchObject({ sessionId: SESSION_ID, sessionName: "claude-worker", wake: "channel", protocolVersion: "2025-11-25", clientProtocolVersion: "2025-11-25", statusFailures: 0, wakeStalledAt: null });
	expect(typeof status.startedAt).toBe("number");
	expect(statSync(path).mode & 0o777).toBe(0o600);
});

test("the worker gate denies other tools through the PreToolUse hook until an assignment is returned", async () => {
	const { origin, worker, harness } = await fixture({ worker: true });
	const denied = JSON.parse(await harness.hook({ event: "pre_tool", tool_name: "Bash" })) as { hookSpecificOutput: Record<string, string> };
	expect(denied.hookSpecificOutput).toMatchObject({ hookEventName: "PreToolUse", permissionDecision: "deny" });
	expect(denied.hookSpecificOutput.permissionDecisionReason).toContain(WORKER_GATE_DENIAL_CODE);
	expect(await harness.hook({ event: "pre_tool", tool_name: "ToolSearch" })).toBe("");
	expect(await harness.hook({ event: "pre_tool", tool_name: `${CLAUDE_PLUGIN.toolPrefix}agent_task_inbox` })).toBe("");

	const send = await harness.callTool("agent_task_send", { to: origin.endpoint, task: "nested" });
	expect(send.isError).toBe(true);
	expect(send.content[0]?.text).toContain(WORKER_COORDINATION_FORBIDDEN_CODE);
	const message = await harness.callTool("agent_task_message", { taskId: "unknown", type: "question", message: "hello" });
	expect(message.content[0]?.text).toContain(WORKER_GATE_DENIAL_CODE);

	const taskId = await assign(origin, worker, harness, "gated assignment");
	await waitFor(() => pointers(harness).length === 1);
	await harness.callTool("agent_task_inbox");
	expect(await harness.hook({ event: "pre_tool", tool_name: "Bash" })).toBe("");
	const done = await harness.callTool("agent_task_done", { taskId, status: "completed", summary: "done" });
	expect(done.isError).toBeUndefined();
});

/** A worker whose first core fails `connect` on demand; the rebind returns a fresh endpoint. */
async function relayLossFixture() {
	process.env.WOLFPACK_TASK_WORKER = "1";
	const relay = claudePathRelay();
	const origin = createTaskCore({ endpoint: { relay: "memory", id: "parent" }, relay, store: createTaskStore(), ids: sequence("parent") });
	const worker = createTaskCore({ endpoint: { relay: "memory", id: "worker" }, relay, store: createTaskStore(), ids: sequence("worker") });
	const fresh = createTaskCore({ endpoint: { relay: "memory", id: "fresh-worker" }, relay, store: createTaskStore(), ids: sequence("fresh") });
	await origin.connect();
	let connectionError: string | undefined;
	let factoryCalls = 0;
	const harness = startHarness({
		env: wolfpackEnv(), runCommand: noCommands,
		async createCore() {
			factoryCalls++;
			if (factoryCalls === 1) {
				await worker.connect();
				return { ...worker, async connect() {
					if (connectionError) throw new TaskProtocolError(connectionError, "fixture connection failure", { retryable: false });
					await worker.connect();
				} };
			}
			await fresh.connect();
			return fresh;
		},
	});
	harnesses.push(harness);
	await harness.initialize("2025-11-25");
	await harness.notify("notifications/initialized");
	const { taskId: lost } = await origin.createTask({ target: worker.endpoint, task: "assignment before the reset", timeoutMs: 60_000 });
	await harness.hook({ event: "stop" });
	await waitFor(() => pointers(harness).length === 1);
	expect((await harness.callTool("agent_task_inbox")).content.map((part) => part.text).join("\n")).toContain("assignment before the reset");
	return {
		origin, fresh, harness, lost,
		async reset() {
			connectionError = "RELAY_RESET";
			await harness.hook({ event: "stop" });
			await waitFor(async () => (await harness.hook({ event: "pre_tool", tool_name: "Bash" })).includes("RELAY_STATE_LOST") && factoryCalls === 2);
			await new Promise((resolve) => setTimeout(resolve, 50));
		},
	};
}

test("a relay-loss note waits for the next inbox result and requests no pointer", async () => {
	const { harness, lost, reset } = await relayLossFixture();
	await reset();
	expect(pointers(harness)).toHaveLength(1);
	const inbox = (await harness.callTool("agent_task_inbox")).content.map((part) => part.text).join("\n");
	expect(inbox).toContain("Relay state was lost");
	expect(inbox).toContain(lost);
});

test("a buffered relay-loss note does not suppress the wake for a later assignment", async () => {
	const { origin, fresh, harness, lost, reset } = await relayLossFixture();
	await reset();
	const { taskId: next } = await origin.createTask({ target: fresh.endpoint, task: "assignment after the reset", timeoutMs: 60_000 });
	await harness.hook({ event: "stop" });
	await waitFor(() => pointers(harness).some((pointer) => (pointer as { params: { meta: Record<string, string> } }).params.meta.task_id === next));
	const inbox = (await harness.callTool("agent_task_inbox")).content.map((part) => part.text).join("\n");
	expect(inbox).toContain("Relay state was lost");
	expect(inbox).toContain(lost);
	expect(inbox).toContain("assignment after the reset");
});

const gateSocket = (env: Record<string, string | undefined>): string => {
	const environment = readClaudeEnvironment(env);
	if (environment.kind !== "ready") throw new Error(`environment is ${environment.kind}`);
	return environment.gateSocketPath;
};
const queryGate = (env: Record<string, string | undefined>) => queryWorkerGate(gateSocket(env), { sessionId: SESSION_ID, toolName: "Bash" });

test("a worker server answers the gate socket from its current decision and closes and unlinks it on shutdown", async () => {
	const { origin, worker, harness, env } = await fixture({ worker: true });
	expect(await queryGate(env)).toEqual({ ok: true, authorized: false, reason: WORKER_GATE_DENIAL_CODE });

	const taskId = await assign(origin, worker, harness, "gated assignment");
	await waitFor(() => pointers(harness).length === 1);
	await harness.callTool("agent_task_inbox");
	expect(await queryGate(env)).toEqual({ ok: true, authorized: true, reason: null });

	await harness.callTool("agent_task_done", { taskId, status: "completed", summary: "done" });
	expect(await queryGate(env)).toMatchObject({ ok: true, authorized: false });

	await harness.server.shutdown();
	expect(existsSync(gateSocket(env))).toBe(false);
	expect(await queryGate(env)).toEqual({ ok: false, detail: expect.stringContaining("task server socket is missing") });
});

test("the gate answer follows a parent cancellation without a task tool call", async () => {
	const { origin, worker, harness, env } = await fixture({ worker: true });
	const taskId = await assign(origin, worker, harness, "cancelled assignment");
	await waitFor(() => pointers(harness).length === 1);
	await harness.callTool("agent_task_inbox");
	expect(await queryGate(env)).toMatchObject({ ok: true, authorized: true });
	await origin.submitIntent({ taskId, type: "task.cancelled", payload: {} });
	await harness.hook({ event: "stop" });
	await waitFor(async () => (await queryGate(env)).ok && !(await queryGate(env) as { authorized: boolean }).authorized);
});

test("an ordinary server opens no gate socket", async () => {
	const { origin, worker, harness, env } = await fixture();
	await assign(origin, worker, harness, "ordinary assignment");
	await waitFor(() => pointers(harness).length === 1);
	await harness.callTool("agent_task_inbox");
	expect(existsSync(dirname(gateSocket(env)))).toBe(false);
});

test("a restarted worker server replaces the stale socket of a killed server", async () => {
	const env = wolfpackEnv();
	const path = gateSocket(env);
	mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
	// A killed server's socket: bound, then abandoned without close.
	const killed = Bun.spawn([process.execPath, "-e", `require("node:net").createServer().listen(${JSON.stringify(path)}, () => console.log("ready"))`], { stdout: "pipe" });
	await killed.stdout.getReader().read();
	killed.kill("SIGKILL");
	await killed.exited;
	expect(lstatSync(path).isSocket()).toBe(true);
	expect(await queryGate(env)).toEqual({ ok: false, detail: expect.stringContaining("connection refused") });
	await fixture({ worker: true, env });
	expect(await queryGate(env)).toEqual({ ok: true, authorized: false, reason: WORKER_GATE_DENIAL_CODE });
});

const node = Bun.which("node");

test.skipIf(!node)("the bundled gate hook asks the running server: deny before assignment, allow while assigned, deny after completion and shutdown", async () => {
	const { origin, worker, harness, env } = await fixture({ worker: true });
	const runGate = async (): Promise<{ exitCode: number; stderr: string }> => {
		const child = Bun.spawn([node!, join(import.meta.dir, "..", "claude-plugin", CLAUDE_PLUGIN.gateBundle)], {
			stdin: "pipe", stdout: "pipe", stderr: "pipe",
			env: { PATH: process.env.PATH ?? "", WOLFPACK_SESSION_ID: SESSION_ID, WOLFPACK_SESSION_NAME: "claude-worker", WOLFPACK_DIR: env.WOLFPACK_DIR!, WOLFPACK_TASK_WORKER: "1" },
		});
		child.stdin.write(JSON.stringify({ tool_name: "Bash" }));
		await child.stdin.end();
		const [stderr, exitCode] = await Promise.all([new Response(child.stderr).text(), child.exited]);
		return { exitCode, stderr };
	};
	expect(await runGate()).toMatchObject({ exitCode: 2, stderr: expect.stringContaining(WORKER_GATE_DENIAL_CODE) });
	const taskId = await assign(origin, worker, harness, "gated assignment");
	await waitFor(() => pointers(harness).length === 1);
	await harness.callTool("agent_task_inbox");
	expect(await runGate()).toEqual({ exitCode: 0, stderr: "" });
	await harness.callTool("agent_task_done", { taskId, status: "completed", summary: "done" });
	expect((await runGate()).exitCode).toBe(2);
	await harness.callTool("agent_task_inbox");
	await harness.server.shutdown();
	expect(await runGate()).toMatchObject({ exitCode: 2, stderr: expect.stringContaining("task server socket is missing") });
}, 15_000);
