import { afterEach, expect, jest, test } from "bun:test";
import { SessionManager, type ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { validateToolArguments } from "@earendil-works/pi-ai";

import { createChildEventQueue } from "../src/child-events";
import type { ChildEvent } from "../src/child-events";
import { CLAUDE_PLUGIN } from "../src/claude/constants";
import { registerAgentTaskTools } from "../src/extension";
import { createInMemoryTaskRelay } from "../src/in-memory-task-relay";
import { createTaskCore } from "../src/task-core";
import type { TaskCore } from "../src/task-core";
import { createTaskStore } from "../src/task-store";
import { sequence, startHarness, waitFor, wolfpackEnv } from "./claude-harness";
import type { Harness } from "./claude-harness";

/**
 * Tracked delegated work end to end through the real runtimes: task mail and canonical terminals are the only
 * routine parent wakes; raw child runtime observations between tasks are inspection-only.
 */

type Result = { readonly content: readonly { readonly text: string }[]; readonly details: Record<string, any>; readonly isError?: boolean };
interface Tool { readonly name: string; readonly description: string; readonly parameters: any; execute(id: string, params: Record<string, unknown>, signal: AbortSignal, update: undefined, context: unknown): Promise<Result> }
interface Sent { readonly customType: string; readonly content: string; readonly details: any; readonly options: any }
const noCommands = async (): Promise<never> => { throw new Error("unexpected command"); };
const harnesses: Harness[] = [];
afterEach(async () => {
	jest.useRealTimers();
	for (const harness of harnesses.splice(0)) await harness.server.shutdown();
});

let childSeq = 0;
function childEvent(event: ChildEvent["event"], extra: Partial<ChildEvent> = {}): ChildEvent {
	const seq = ++childSeq;
	return { eventId: `child:boot:${seq}`, bootId: "boot", seq, childSessionId: "child-session", childName: "worker", parentSessionId: "parent-session",
		event, state: event, observedAt: new Date().toISOString(), ...extra };
}

function cores(clock?: { now: () => number }) {
	const relay = createInMemoryTaskRelay("memory");
	const queue = createChildEventQueue();
	const parent = Object.assign(createTaskCore({ endpoint: { relay: "memory", id: "parent" }, relay, store: createTaskStore(), ids: sequence("parent"), ...(clock && { clock }) }), { childEvents: queue });
	const child = createTaskCore({ endpoint: { relay: "memory", id: "child" }, relay, store: createTaskStore(), ids: sequence("child") });
	/** Stands in for the Wolfpack relay delivering a child event to the parent's configured core. */
	const observe = (event: ChildEvent) => queue.accept({ kind: "event", envelopeId: `wolfpack.child_event:${event.eventId}`, event });
	return { parent, child, observe };
}

function piSession(core: TaskCore) {
	const sent: Sent[] = [];
	const history = SessionManager.inMemory();
	const handlers: Record<string, (event: unknown, context: unknown) => Promise<unknown>> = {};
	const tools: Record<string, Tool> = {};
	registerAgentTaskTools({
		on(name: string, handler: (event: unknown, context: unknown) => Promise<unknown>) { handlers[name] = handler; },
		registerTool(tool: unknown) { const value = tool as Tool; tools[value.name] = value; },
		sendMessage(message: Omit<Sent, "options">, options: unknown) {
			sent.push({ ...message, options });
			history.appendCustomMessageEntry(message.customType, message.content, true, message.details);
		},
		appendEntry(customType: string, data: unknown) { history.appendCustomEntry(customType, data); },
	} as unknown as ExtensionAPI, core);
	const context = {
		isIdle: () => true, hasPendingMessages: () => false, sessionManager: history,
		ui: { setStatus() { undefined; }, notify() { undefined; }, theme: { fg: (_color: string, text: string) => text } },
	};
	const signal = new AbortController().signal;
	return {
		sent, tools, context, handlers,
		wakes: () => sent.filter(item => item.options?.triggerTurn === true),
		call: (name: string, params: Record<string, unknown>) => tools[name]!.execute("call", params, signal, undefined, context),
		settle: () => handlers.agent_end!({}, context),
	};
}
const presented = (session: ReturnType<typeof piSession>) => session.sent.filter(item => item.customType === "pi-tasks-event").map(item => item.content).join("\n");

test("pi: assignment, passive progress, question, answer, terminal and ACK, then a new task on the same worker", async () => {
	const { parent, child, observe } = cores();
	const p = piSession(parent), c = piSession(child);
	try {
		await p.handlers.session_start!({}, p.context);
		await c.handlers.session_start!({}, c.context);
		const first = (await p.call("agent_task_send", { to: child.endpoint, task: "first assignment", timeoutMs: 3_600_000 })).details.taskId as string;
		await c.settle();
		expect(c.wakes()).toHaveLength(1);
		expect(presented(c)).toContain("first assignment");

		await c.call("agent_task_message", { taskId: first, type: "information", message: "first progress" });
		await p.settle(); await p.settle();
		expect(p.wakes()).toHaveLength(0);
		await c.call("agent_task_message", { taskId: first, type: "question", message: "which branch?" });
		await p.settle();
		expect(p.wakes()).toHaveLength(1);
		expect(presented(p)).toContain("which branch?");
		expect(presented(p)).not.toContain("first progress");

		await p.call("agent_task_message", { taskId: first, type: "answer", message: "use main" });
		await c.settle();
		expect(c.wakes()).toHaveLength(2);
		expect(presented(c)).toContain("use main");

		await c.call("agent_task_done", { taskId: first, status: "completed", summary: "first finished" });
		await p.settle();
		expect(p.wakes()).toHaveLength(2);
		expect(presented(p)).toContain("first finished");
		expect((await p.call("agent_task_ack", { taskId: first })).details.error).toBeUndefined();
		await c.settle();
		expect(c.wakes()).toHaveLength(2);

		// Between tasks: the worker's unrelated user turns settle and it waits on a permission prompt. No parent wake.
		observe(childEvent("done"));
		observe(childEvent("blocked", { inputKind: "permission" }));
		observe(childEvent("done", { state: "idle" }));
		await p.settle();
		expect(p.wakes()).toHaveLength(2);

		const second = (await p.call("agent_task_send", { to: child.endpoint, task: "second assignment", timeoutMs: 3_600_000 })).details.taskId as string;
		await c.settle();
		expect(c.wakes()).toHaveLength(3);
		expect(presented(c)).toContain("second assignment");
		await c.call("agent_task_done", { taskId: second, status: "failed", summary: "second blocked on credentials" });
		await p.settle();
		expect(p.wakes()).toHaveLength(3);
		expect(presented(p)).toContain("second blocked on credentials");

		// A genuine process failure stays visible.
		observe(childEvent("exited", { reason: "exit", exitCode: 1 }));
		await p.settle();
		expect(p.wakes()).toHaveLength(4);
		expect(p.sent.at(-2)!.customType).toBe("pi-tasks-child-events");
	} finally {
		await p.handlers.session_shutdown!({}, p.context);
		await c.handlers.session_shutdown!({}, c.context);
	}
});

test("claude: assignment, passive progress, question, answer, terminal and ACK, then a new task on the same worker", async () => {
	const { parent, child, observe } = cores();
	const start = async (core: TaskCore, name: string) => {
		const harness = startHarness({ env: wolfpackEnv({ WOLFPACK_SESSION_NAME: name }), core, runCommand: noCommands });
		harnesses.push(harness);
		await harness.initialize("2025-11-25");
		await harness.notify("notifications/initialized");
		return harness;
	};
	const p = await start(parent, "claude-parent"), c = await start(child, "claude-child");
	const pointers = (harness: Harness) => harness.notifications(CLAUDE_PLUGIN.channelMethod).length;
	const inbox = async (harness: Harness) => (await harness.callTool("agent_task_inbox")).content.map(part => part.text).join("\n");
	/** A stop refresh that settled without a pointer: the delivery was ACKed (archived) or a pointer appeared. */
	const quietStop = async (harness: Harness, core: TaskCore, expected: number) => {
		await harness.hook({ event: "stop" });
		await harness.hook({ event: "stop" });
		await new Promise(resolve => setTimeout(resolve, 30));
		expect(pointers(harness)).toBe(expected);
		void core;
	};

	const first = (await p.callTool("agent_task_send", { to: child.endpoint, task: "first assignment", timeoutMs: 3_600_000 })).structuredContent!.taskId as string;
	await c.hook({ event: "stop" });
	await waitFor(() => pointers(c) === 1);
	expect(await inbox(c)).toContain("first assignment");

	await c.callTool("agent_task_message", { taskId: first, type: "information", message: "first progress" });
	await quietStop(p, parent, 0);
	await c.callTool("agent_task_message", { taskId: first, type: "question", message: "which branch?" });
	await p.hook({ event: "stop" });
	await waitFor(() => pointers(p) === 1);
	const asked = await inbox(p);
	expect(asked).toContain("which branch?");
	expect(asked).not.toContain("first progress");

	await p.callTool("agent_task_message", { taskId: first, type: "answer", message: "use main" });
	await c.hook({ event: "stop" });
	await waitFor(() => pointers(c) === 2);
	expect(await inbox(c)).toContain("use main");

	await c.callTool("agent_task_done", { taskId: first, status: "completed", summary: "first finished" });
	await p.hook({ event: "stop" });
	await waitFor(() => pointers(p) === 2);
	expect(await inbox(p)).toContain("first finished");
	expect((await p.callTool("agent_task_ack", { taskId: first })).isError).toBeUndefined();

	observe(childEvent("done"));
	observe(childEvent("blocked", { inputKind: "permission" }));
	await quietStop(p, parent, 2);
	expect(await inbox(p)).not.toContain("wolfpack child events");

	const second = (await p.callTool("agent_task_send", { to: child.endpoint, task: "second assignment", timeoutMs: 3_600_000 })).structuredContent!.taskId as string;
	await c.hook({ event: "stop" });
	await waitFor(() => pointers(c) === 3);
	expect(await inbox(c)).toContain("second assignment");
	await c.callTool("agent_task_done", { taskId: second, status: "completed", summary: "second finished" });
	await p.hook({ event: "stop" });
	await waitFor(() => pointers(p) === 3);
	expect(await inbox(p)).toContain("second finished");

	observe(childEvent("exited", { reason: "exit", exitCode: 1 }));
	await p.hook({ event: "stop" });
	await waitFor(() => pointers(p) === 4);
	expect(await inbox(p)).toContain("wolfpack child events");
});

const longSummary = "s".repeat(1_201);

test("pi: a schema-refused terminal leaves the task active and unsubmitted, and a valid retry completes it", async () => {
	const { parent, child } = cores();
	const p = piSession(parent), c = piSession(child);
	try {
		await p.handlers.session_start!({}, p.context);
		await c.handlers.session_start!({}, c.context);
		const taskId = (await p.call("agent_task_send", { to: child.endpoint, task: "assignment", timeoutMs: 3_600_000 })).details.taskId as string;
		await c.settle();
		// Pi validates arguments against the registered schema before execute; a refusal never reaches the core.
		expect(() => validateToolArguments(c.tools.agent_task_done!, { type: "toolCall", id: "call", name: "agent_task_done", arguments: { taskId, status: "completed", summary: longSummary } })).toThrow();
		expect(child.getTask(taskId)).toMatchObject({ status: "active", terminalDelivery: { state: "not_submitted" } });
		// A passive runtime done after the refusal is not completion.
		await p.settle();
		expect(parent.getTask(taskId)?.status).toBe("active");
		expect(p.wakes()).toHaveLength(0);

		const done = await c.call("agent_task_done", { taskId, status: "completed", summary: "valid retry" });
		expect(done.details.error).toBeUndefined();
		await p.settle();
		expect(parent.getTask(taskId)?.status).toBe("completed");
		expect(p.wakes()).toHaveLength(1);
	} finally {
		await p.handlers.session_shutdown!({}, p.context);
		await c.handlers.session_shutdown!({}, c.context);
	}
});

test("claude: a schema-refused terminal from a task worker leaves the task active and unsubmitted, and a valid retry completes it", async () => {
	const { parent, child, observe } = cores();
	process.env.WOLFPACK_TASK_WORKER = "1";
	try {
		const c = startHarness({ env: wolfpackEnv(), core: child, runCommand: noCommands });
		harnesses.push(c);
		await c.initialize("2025-11-25");
		await c.notify("notifications/initialized");
		await parent.connect();
		const { taskId } = await parent.createTask({ target: child.endpoint, task: "assignment", timeoutMs: 3_600_000 });
		await c.hook({ event: "stop" });
		await waitFor(() => c.notifications(CLAUDE_PLUGIN.channelMethod).length === 1);
		await c.callTool("agent_task_inbox");

		const refused = await c.callTool("agent_task_done", { taskId, status: "completed", summary: longSummary });
		expect(refused.isError).toBe(true);
		expect(child.getTask(taskId)).toMatchObject({ status: "active", terminalDelivery: { state: "not_submitted" } });
		observe(childEvent("done"));
		await parent.receive();
		expect(parent.getTask(taskId)?.status).toBe("active");

		const done = await c.callTool("agent_task_done", { taskId, status: "completed", summary: "valid retry" });
		expect(done.isError).toBeUndefined();
		await waitFor(async () => { await parent.receive(); return parent.getTask(taskId)?.status === "completed"; });
	} finally { delete process.env.WOLFPACK_TASK_WORKER; }
});

async function settleMicrotasks() { for (let i = 0; i < 400; i++) await Promise.resolve(); }

test("pi: an omitted terminal times out on the idle parent's background poll and wakes it once", async () => {
	let now = Date.UTC(2026, 10, 17);
	const { parent, child, observe } = cores({ now: () => now });
	const p = piSession(parent);
	jest.useFakeTimers();
	try {
		await p.handlers.session_start!({}, p.context);
		await child.connect();
		const taskId = (await p.call("agent_task_send", { to: child.endpoint, task: "never finishes", timeoutMs: 1_000 })).details.taskId as string;
		await child.receive();
		// The worker settles without a terminal (for example after a refused manual report). Passive only.
		observe(childEvent("done"));
		jest.advanceTimersByTime(10_000); await settleMicrotasks();
		expect(parent.getTask(taskId)?.status).toBe("active");
		expect(p.wakes()).toHaveLength(0);

		now += 1_001;
		for (let i = 0; i < 4 && p.wakes().length === 0; i++) { jest.advanceTimersByTime(10_000); await settleMicrotasks(); }
		expect(parent.getTask(taskId)?.status).toBe("timed_out");
		expect(p.wakes()).toHaveLength(1);
		expect(p.wakes()[0]!.details).toMatchObject({ taskId });
		jest.advanceTimersByTime(30_000); await settleMicrotasks();
		expect(p.wakes()).toHaveLength(1);
	} finally {
		jest.useRealTimers();
		await p.handlers.session_shutdown!({}, p.context);
	}
});

test("claude: an omitted terminal times out on the idle parent's background poll and requests one pointer", async () => {
	let now = Date.UTC(2026, 10, 17);
	const { parent, child, observe } = cores({ now: () => now });
	const p = startHarness({ env: wolfpackEnv({ WOLFPACK_SESSION_NAME: "claude-parent" }), core: parent, runCommand: noCommands });
	harnesses.push(p);
	await p.initialize("2025-11-25");
	await p.notify("notifications/initialized");
	await child.connect();
	const taskId = (await p.callTool("agent_task_send", { to: child.endpoint, task: "never finishes", timeoutMs: 1_000 })).structuredContent!.taskId as string;
	await child.receive();
	observe(childEvent("done"));
	jest.useFakeTimers();
	jest.advanceTimersByTime(10_000); await settleMicrotasks();
	expect(parent.getTask(taskId)?.status).toBe("active");
	expect(p.notifications(CLAUDE_PLUGIN.channelMethod)).toHaveLength(0);
	now += 1_001;
	for (let i = 0; i < 4 && p.notifications(CLAUDE_PLUGIN.channelMethod).length === 0; i++) { jest.advanceTimersByTime(10_000); await settleMicrotasks(); }
	jest.useRealTimers();
	expect(parent.getTask(taskId)?.status).toBe("timed_out");
	expect(p.notifications(CLAUDE_PLUGIN.channelMethod)).toHaveLength(1);
	const inbox = (await p.callTool("agent_task_inbox")).content.map(part => part.text).join("\n");
	expect(inbox).toContain(taskId);
	expect(inbox).toContain("timed_out");
});
