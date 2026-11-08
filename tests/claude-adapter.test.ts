import { afterEach, describe, expect, test } from "bun:test";
import { readFileSync, statSync } from "node:fs";
import { join } from "node:path";

import { CLAUDE_PLUGIN } from "../src/claude/constants";
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

async function fixture(options: { worker?: boolean } = {}) {
	if (options.worker) process.env.WOLFPACK_TASK_WORKER = "1";
	const relay = claudePathRelay();
	const origin = createTaskCore({ endpoint: { relay: "memory", id: "parent" }, relay, store: createTaskStore(), ids: sequence("parent") });
	const worker = createTaskCore({ endpoint: { relay: "memory", id: "worker" }, relay, store: createTaskStore(), ids: sequence("worker") });
	await origin.connect();
	await worker.connect();
	const env = wolfpackEnv();
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
