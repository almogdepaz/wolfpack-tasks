import { expect, test } from "bun:test";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { registerAgentTaskTools, WORKER_GATE_DENIAL_CODE, WORKER_COORDINATION_FORBIDDEN_CODE } from "../src/extension";
import { createInMemoryTaskRelay } from "../src/in-memory-task-relay";
import { createTaskCore } from "../src/task-core";
import { createTaskStore } from "../src/task-store";
import { TaskProtocolError } from "../src/task-protocol";

const ORIGIN = { relay: "memory", id: "origin" };
const WORKER = { relay: "memory", id: "worker" };
const FRESH = { relay: "memory", id: "fresh-worker" };

async function fixture(taskIds: readonly string[], rebindUnavailable = false) {
	const relay = createInMemoryTaskRelay("memory");
	const originStore = createTaskStore(), workerStore = createTaskStore(), freshStore = createTaskStore();
	const origin = createTaskCore({ endpoint: ORIGIN, relay, store: originStore });
	const worker = createTaskCore({ endpoint: WORKER, relay, store: workerStore });
	const fresh = createTaskCore({ endpoint: FRESH, relay, store: freshStore });
	await origin.connect();
	await worker.connect();
	const created: string[] = [];
	for (const taskId of taskIds) {
		// Explicit task IDs exercise the existing opaque-ID protocol, including long IDs.
		workerStore.putTask({ taskId, protocolVersion: "pi-tasks/v2", origin: ORIGIN, target: WORKER, task: "private assignment body", status: "active", createdAt: Date.now(), expiresAt: Date.now() + 60_000 });
		const event = { eventId: `assignment-${created.length}`, taskId, type: "task.created" as const, sequence: "1", source: ORIGIN, target: WORKER, occurredAt: Date.now(), payload: { task: "private assignment body" } };
		workerStore.appendEvent(event);
		created.push(taskId);
	}
	const entries: any[] = taskIds.map((taskId, index) => ({ type: "custom_message", customType: "pi-tasks-event", details: { taskId, eventId: `assignment-${index}` } }));
	const messages: any[] = [], events: Record<string, (...args: any[]) => any> = {};
	let connectionError: string | undefined, factoryCalls = 0;
	const context = { isIdle: () => true, hasPendingMessages: () => false, sessionManager: { getEntries: () => entries }, ui: {
		setStatus() {}, notify() {}, theme: { fg: (_color: string, text: string) => text },
	} };
	const previous = process.env.PI_TASK_WORKER;
	process.env.PI_TASK_WORKER = "1";
	try {
		registerAgentTaskTools({
			on: (name: string, handler: (...args: any[]) => any) => { events[name] = handler; }, registerTool() {},
			sendMessage(message: any) { messages.push(message); entries.push({ type: "custom_message", ...message }); }, appendEntry() {},
		} as unknown as ExtensionAPI, undefined, async () => {
			factoryCalls++;
			if (factoryCalls === 1) return { ...worker, async connect() {
				if (connectionError) throw new TaskProtocolError(connectionError, "fixture connection failure", { retryable: false });
				await worker.connect();
			}, async close() { workerStore.close(); } };
			if (rebindUnavailable) throw new TaskProtocolError("RELAY_UNAVAILABLE", "fixture rebind failure");
			await fresh.connect();
			return { ...fresh, async close() { freshStore.close(); } };
		});
	} finally {
		if (previous === undefined) delete process.env.PI_TASK_WORKER; else process.env.PI_TASK_WORKER = previous;
	}
	await events.session_start!({}, context);
	return {
		origin, fresh, messages, entries, workerStore,
		call: (toolName: string, input: unknown = {}) => events.tool_call!({ toolName, input, toolCallId: "probe" }, context),
		async fail(code: string) { connectionError = code; await events.agent_settled!({}, context); },
		async close() { await events.session_shutdown!({}, context); originStore.close(); workerStore.close(); freshStore.close(); },
		async restart() { await events.session_shutdown!({}, context); await events.session_start!({}, context); },
	};
}

test.each(["RELAY_RESET", "RELAY_REBIND_REQUIRED"])("confirmed %s correlates lost assignments but never authorizes work", async code => {
	const f = await fixture(["lost-assignment"]);
	try {
		expect(await f.call("bash")).toBeUndefined();
		const record = f.workerStore.getTask("lost-assignment")!;
		f.workerStore.putTask({ ...record, taskId: "foreign-active", target: ORIGIN });
		f.workerStore.putTask({ ...record, taskId: "completed-assignment", status: "completed" });
		await f.fail(code);
		for (const [tool, input] of [["bash", {}], ["agent_task_message", { taskId: "lost-assignment" }], ["agent_task_done", { taskId: "lost-assignment" }]] as const) {
			const denial = await f.call(tool, input);
			expect(denial.block).toBe(true);
			expect(denial.reason).toStartWith(WORKER_GATE_DENIAL_CODE);
			expect(denial.reason).toContain("RELAY_STATE_LOST");
			expect(denial.reason).toContain("lost-assignment");
			expect(denial.reason).toContain("Parent/coordinator");
			expect(denial.reason).toContain("taskEndpoint");
			expect(denial.reason).toContain("new explicit assignment");
			expect(denial.reason).toContain("remote outcome is unknown");
			expect(denial.reason).not.toContain("private assignment body");
			expect(denial.reason).not.toContain("foreign-active");
			expect(denial.reason).not.toContain("completed-assignment");
		}
		expect(f.fresh.listTasks()).toEqual([]);
		for (const tool of ["agent_task_inbox", "agent_task_status", "agent_task_wait"]) expect(await f.call(tool)).toBeUndefined();
		for (const tool of ["agent_task_send", "agent_task_cancel", "agent_task_ack"]) expect(await f.call(tool)).toEqual({ block: true, reason: WORKER_COORDINATION_FORBIDDEN_CODE });
		const loss = f.messages.find(message => message.customType === "pi-tasks-relay-loss" && message.details.taskId === "lost-assignment");
		expect(loss.details).toMatchObject({ taskId: "lost-assignment", status: "failed", remoteOutcome: "unknown", error: { code: "RELAY_STATE_LOST" } });
		expect(loss.content).toContain("Parent/coordinator");
		expect(loss.content).toContain("new explicit assignment");
		// Only a fresh live assignment plus structured incorporation reopens the gate.
		const next = await f.origin.createTask({ target: FRESH, task: "new authorized scope", timeoutMs: 60_000 });
		await f.fresh.receive();
		const assignment = f.fresh.getTask(next.taskId)!.events.find(event => event.type === "task.created")!;
		f.entries.push({ type: "custom_message", customType: "pi-tasks-event", details: { taskId: next.taskId, eventId: assignment.eventId } });
		expect(await f.call("bash")).toBeUndefined();
		expect((await f.call("agent_task_done", { taskId: "lost-assignment" })).block).toBe(true);
	} finally { await f.close(); }
});

test("lost-assignment correlation is bounded and survives failed rebinding, not session replacement", async () => {
	const ids = Array.from({ length: 12 }, (_, index) => `lost-${index}-` + "x".repeat(300));
	const f = await fixture(ids, true);
	try {
		await f.fail("RELAY_RESET");
		const denial = await f.call("bash");
		expect(denial.reason).toContain("lost-0-");
		expect(denial.reason).not.toContain("lost-8-");
		expect(denial.reason).toContain("truncated");
		expect(denial.reason).toContain("additional IDs omitted");
		expect(denial.reason).not.toContain("x".repeat(129));
		expect(denial.reason.length).toBeLessThan(8192);
		await f.restart();
		expect(await f.call("bash")).toEqual({ block: true, reason: WORKER_GATE_DENIAL_CODE });
	} finally { await f.close(); }
});

test("opaque lost IDs are escaped and remain bounded even at maximum JSON expansion", async () => {
	const id = "\u0001".repeat(127) + "\n";
	const f = await fixture(Array.from({ length: 8 }, (_, index) => id + index));
	try {
		await f.fail("RELAY_RESET");
		const denial = await f.call("bash");
		expect(denial.reason).toContain("\\u0001");
		expect(denial.reason).toContain("\\n");
		expect(denial.reason).not.toContain("\u0001");
		expect(denial.reason).not.toContain("\n");
		expect(denial.reason.length).toBeLessThan(8192);
	} finally { await f.close(); }
});

test.each(["RELAY_UNAVAILABLE", "RELAY_AUTH_REQUIRED", "RELAY_CAPACITY", "ABORTED", "RELAY_PROFILE_REQUIRED", "INVALID_RESPONSE"])("%s does not discard or label a current assignment as lost", async code => {
	const f = await fixture(["retained-assignment"]);
	try {
		await f.fail(code);
		expect(await f.call("bash")).toBeUndefined();
		expect(await f.call("agent_task_message", { taskId: "unknown" })).toEqual({ block: true, reason: WORKER_GATE_DENIAL_CODE });
		expect(f.messages.filter(message => message.customType === "pi-tasks-relay-loss")).toEqual([]);
	} finally { await f.close(); }
});

test.each(["RELAY_UNAVAILABLE", "RELAY_AUTH_REQUIRED", "RELAY_CAPACITY", "ABORTED", "RELAY_PROFILE_REQUIRED", "INVALID_RESPONSE", "RELAY_RESET"])("unassigned denial stays unchanged after %s without lost assignments", async code => {
	const f = await fixture([]);
	try {
		await f.fail(code);
		expect(await f.call("bash")).toEqual({ block: true, reason: WORKER_GATE_DENIAL_CODE });
		expect(f.messages.filter(message => message.customType === "pi-tasks-relay-loss")).toEqual([]);
	} finally { await f.close(); }
});
