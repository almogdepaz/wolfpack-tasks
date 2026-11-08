import { afterEach, expect, test } from "bun:test";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";

import { registerAgentTaskTools, WORKER_GATE_DENIAL_CODE } from "../src/extension";
import { createInMemoryTaskRelay } from "../src/in-memory-task-relay";
import { createTaskCore } from "../src/task-core";
import type { TaskCore } from "../src/task-core";
import { createTaskStore } from "../src/task-store";
import { deliverTaskInbox } from "../src/task-inbox";
import { TaskProtocolError } from "../src/task-protocol";
import type { TaskEvent, TaskRelay } from "../src/task-protocol";

interface ToolCallResult {
	readonly block: true;
	readonly reason?: string;
}

interface SessionAssignmentEntry {
	readonly type: "custom_message";
	readonly customType: string;
	readonly details: { readonly taskId: string; readonly eventId: string };
	readonly content: string;
}

type ToolCallHandler = (event: { readonly toolName: string; readonly toolCallId: string; readonly input: unknown }, context: { readonly sessionManager: { readonly getEntries: () => readonly unknown[] } }) => Promise<ToolCallResult | undefined> | ToolCallResult | undefined;

const ORIGIN = { relay: "memory", id: "origin" } as const;
const WORKER = { relay: "memory", id: "worker" } as const;
const DENIAL_CODE = WORKER_GATE_DENIAL_CODE;
const COORDINATION_DENIAL_CODE = "PI_TASK_WORKER_COORDINATION_FORBIDDEN";
const temporaryDirectories: string[] = [];

afterEach(() => {
	for (const directory of temporaryDirectories.splice(0)) rmSync(directory, { recursive: true, force: true });
});

test("activated workers fail closed before assignment while exact opt-out sessions remain unchanged", async () => {
	const relay = createInMemoryTaskRelay("memory");
	const worker = createTaskCore({ endpoint: WORKER, relay, store: createTaskStore(), ids: sequence("worker") });
	const gate = registerGate(worker, "1");

	let executions = 0;
	expect(await runTool(gate, "bash", { command: "pwd" }, [], () => { executions += 1; })).toEqual({ block: true, reason: DENIAL_CODE });
	expect(executions).toBe(0);
	expect(await call(gate, "future_tool", {}, [])).toEqual({ block: true, reason: DENIAL_CODE });
	expect(await call(gate, "agent_task_status", { taskId: "unknown" }, [])).toBeUndefined();
	expect(await call(gate, "agent_task_wait", { taskId: "unknown" }, [])).toBeUndefined();
	expect(await call(gate, "agent_task_inbox", {}, [])).toBeUndefined();

	const inactiveGate = registerGate(worker, "true");
	expect(await call(inactiveGate, "bash", { command: "pwd" }, [])).toBeUndefined();
	const ordinaryGate = registerGate(worker, undefined);
	expect(await call(ordinaryGate, "bash", { command: "pwd" }, [])).toBeUndefined();
});

test("WOLFPACK_TASK_WORKER=1 activates the same worker gate as PI_TASK_WORKER=1", async () => {
	const relay = createInMemoryTaskRelay("memory");
	const worker = createTaskCore({ endpoint: WORKER, relay, store: createTaskStore(), ids: sequence("worker") });
	const neutralGate = registerGate(worker, undefined, "1");
	expect(await call(neutralGate, "bash", { command: "pwd" }, [])).toEqual({ block: true, reason: DENIAL_CODE });
	expect(await call(neutralGate, "agent_task_send", {}, [])).toEqual({ block: true, reason: COORDINATION_DENIAL_CODE });
	expect(await call(neutralGate, "agent_task_inbox", {}, [])).toBeUndefined();
	const inactiveGate = registerGate(worker, undefined, "true");
	expect(await call(inactiveGate, "bash", { command: "pwd" }, [])).toBeUndefined();
});

test("assigned workers cannot call coordinator-only task tools", async () => {
	const fixture = await assignedFixture(1);
	const taskId = requiredTaskId(fixture.taskIds, 0);
	const assignment = assignmentEntry(fixture.worker, taskId);
	const gate = registerGate(fixture.worker, "1");

	for (const [toolName, input] of [
		["agent_task_send", { to: WORKER, task: "delegate recursively" }],
		["agent_task_cancel", { taskId }],
		["agent_task_ack", { taskId }],
	] as const) {
		expect(await call(gate, toolName, input, [assignment])).toEqual({ block: true, reason: COORDINATION_DENIAL_CODE });
	}
});

test("worker task messages require an eligible incorporated assignment matching the input task id", async () => {
	const fixture = await assignedFixture(2);
	const firstTaskId = requiredTaskId(fixture.taskIds, 0);
	const secondTaskId = requiredTaskId(fixture.taskIds, 1);
	const firstAssignment = assignmentEntry(fixture.worker, firstTaskId);
	const gate = registerGate(fixture.worker, "1");

	expect(await call(gate, "agent_task_message", { taskId: firstTaskId, type: "information", message: "update" }, [firstAssignment])).toBeUndefined();
	expect(await call(gate, "agent_task_message", { taskId: secondTaskId, type: "information", message: "wrong task" }, [firstAssignment])).toEqual({ block: true, reason: DENIAL_CODE });
});

test("only structured assignment evidence matching a local active worker task opens the gate", async () => {
	const fixture = await assignedFixture(1);
	const gate = registerGate(fixture.worker, "1");
	const taskId = requiredTaskId(fixture.taskIds, 0);
	const assignment = assignmentEntry(fixture.worker, taskId);

	expect(await call(gate, "read", { path: "README.md" }, [assignment])).toBeUndefined();
	expect(await call(gate, "read", { path: "README.md" }, [{ type: "message", content: "## task assignment" }])).toEqual({ block: true, reason: DENIAL_CODE });
	expect(await call(gate, "read", { path: "README.md" }, [{ ...assignment, details: { taskId: 1, eventId: null } }])).toEqual({ block: true, reason: DENIAL_CODE });
	const foreignEvent: TaskEvent = {
		eventId: "foreign-event", taskId: "foreign-task", type: "task.created", sequence: "1", source: ORIGIN,
		target: { relay: "memory", id: "other-worker" }, occurredAt: 1, payload: { task: "foreign" },
	};
	fixture.workerStore.putTask({
		taskId: foreignEvent.taskId, protocolVersion: "pi-tasks/v2", origin: ORIGIN, target: foreignEvent.target,
		task: "foreign", createdAt: 1, expiresAt: 2, status: "active",
	});
	fixture.workerStore.appendEvent(foreignEvent);
	expect(await call(gate, "read", { path: "README.md" }, [{ ...assignment, details: { taskId: foreignEvent.taskId, eventId: foreignEvent.eventId } }])).toEqual({ block: true, reason: DENIAL_CODE });
	expect(await call(gate, "read", { path: "README.md" }, [{ ...assignment, details: { taskId: "unknown-task", eventId: "unknown-event" } }])).toEqual({ block: true, reason: DENIAL_CODE });
	expect(await call(gate, "read", { path: "README.md" }, [{ ...assignment, customType: "other-extension" }])).toEqual({ block: true, reason: DENIAL_CODE });

	await fixture.worker.submitIntent({ taskId, type: "task.completed", payload: { summary: "finished" } });
	await fixture.origin.receive();
	await fixture.worker.receive();
	expect(fixture.worker.getTask(taskId)?.status).toBe("completed");
	expect(await call(gate, "read", { path: "README.md" }, [assignment])).toEqual({ block: true, reason: DENIAL_CODE });
	const terminalEvent = fixture.worker.getTask(taskId)?.events.find((event) => event.type === "task.completed");
	if (terminalEvent === undefined) throw new Error("expected terminal event");
	const terminalOnly = { ...assignment, details: { taskId, eventId: terminalEvent.eventId } };
	expect(await call(gate, "read", { path: "README.md" }, [terminalOnly])).toEqual({ block: true, reason: DENIAL_CODE });
	expect(await call(gate, "read", { path: "README.md" }, [terminalOnly, terminalOnly])).toEqual({ block: true, reason: DENIAL_CODE });
});

test("queued assignment intake records RAM state without opening the worker gate and queued terminal intake closes it", async () => {
	const relay = createInMemoryTaskRelay("memory");
	const originStore = createTaskStore(), workerStore = createTaskStore();
	const origin = createTaskCore({ endpoint: ORIGIN, relay, store: originStore, ids: sequence("queued-origin") });
	const worker = createTaskCore({ endpoint: WORKER, relay, store: workerStore, ids: sequence("queued-worker") });
	const gate = registerGate(worker, "1");
	const entries: unknown[] = [];
	const pi = {
		sendMessage(message: { readonly customType: string; readonly details: unknown }): void { entries.push({ type: "custom_message", ...message }); },
		appendEntry(customType: string, data: unknown): void { entries.push({ type: "custom", customType, data }); },
	};
	let pending = true;
	const context = { isIdle: (): boolean => true, hasPendingMessages: (): boolean => pending, sessionManager: { getEntries: (): readonly unknown[] => entries } };
	try {
		await origin.connect(); await worker.connect();
		const created = await origin.createTask({ target: WORKER, task: "wait for structured incorporation", timeoutMs: 60_000 });
		await deliverTaskInbox(pi, worker, context);
		await deliverTaskInbox(pi, worker, context);
		expect(worker.getTask(created.taskId)?.status).toBe("active");
		expect(entries).toEqual([]);
		expect(workerStore.getReceiveCursor()).toBe("0");
		expect(await call(gate, "bash", { command: "pwd" }, entries)).toEqual({ block: true, reason: DENIAL_CODE });

		pending = false;
		await deliverTaskInbox(pi, worker, context);
		expect(await call(gate, "bash", { command: "pwd" }, entries)).toBeUndefined();
		await origin.submitIntent({ taskId: created.taskId, type: "task.cancelled", payload: {} });
		pending = true;
		const incorporatedEntries = [...entries];
		await deliverTaskInbox(pi, worker, context);
		expect(worker.getTask(created.taskId)?.status).toBe("cancelled");
		expect(entries).toEqual(incorporatedEntries);
		expect(await call(gate, "bash", { command: "pwd" }, entries)).toEqual({ block: true, reason: DENIAL_CODE });
	} finally { originStore.close(); workerStore.close(); }
});

test("restored session history cannot reopen the worker gate after endpoint RAM loss", async () => {
	const directory = mkdtempSync("/tmp/pi-tasks-worker-gate-");
	temporaryDirectories.push(directory);
	const path = join(directory, "tasks.sqlite");
	const relay = createInMemoryTaskRelay("memory");
	const origin = createTaskCore({ endpoint: ORIGIN, relay, store: createTaskStore(), ids: sequence("origin") });
	const workerStore = createTaskStore();
	const worker = createTaskCore({ endpoint: WORKER, relay, store: workerStore, ids: sequence("worker") });
	await origin.connect();
	await worker.connect();
	const created = await origin.createTask({ target: WORKER, task: "durable assignment", timeoutMs: 1_000 });
	await worker.receive();
	const assignment = assignmentEntry(worker, created.taskId);
	workerStore.close();

	const restartedStore = createTaskStore();
	const restarted = createTaskCore({ endpoint: WORKER, relay, store: restartedStore, ids: sequence("restart") });
	const restartedGate = registerGate(restarted, "1");
	expect(restarted.listTasks()).toEqual([]);
	expect(restarted.getTask(created.taskId)).toBeUndefined();
	expect(await call(restartedGate, "bash", { command: "pwd" }, [assignment])).toEqual({ block: true, reason: DENIAL_CODE });
	restartedStore.close();
});

test("delivery-blocked terminal state survives gate restart and permits only its done retry", async () => {
	const relayState = { blockOrigin: false };
	const relay = expiringOriginRelay(relayState);
	const origin = createTaskCore({ endpoint: ORIGIN, relay, store: createTaskStore(), ids: sequence("origin") });
	const worker = createTaskCore({ endpoint: WORKER, relay, store: createTaskStore(), ids: sequence("worker") });
	await origin.connect();
	await worker.connect();
	const created = await origin.createTask({ target: WORKER, task: "blocked terminal", timeoutMs: 1_000 });
	await worker.receive();
	const assignment = assignmentEntry(worker, created.taskId);
	relayState.blockOrigin = true;
	await expect(worker.submitIntent({ taskId: created.taskId, type: "task.completed", payload: { summary: "finished" } })).rejects.toMatchObject({ code: "TARGET_NOT_REGISTERED" });
	const restartedGate = registerGate(worker, "1");

	expect(await call(restartedGate, "bash", { command: "pwd" }, [assignment])).toEqual({ block: true, reason: DENIAL_CODE });
	expect(await call(restartedGate, "agent_task_done", { taskId: created.taskId, status: "completed", summary: "finished" }, [assignment])).toBeUndefined();
});

test("terminal preflight closes only its assigned task and permits only idempotent done retries", async () => {
	const fixture = await assignedFixture(2);
	const firstTaskId = requiredTaskId(fixture.taskIds, 0);
	const secondTaskId = requiredTaskId(fixture.taskIds, 1);
	const firstAssignment = assignmentEntry(fixture.worker, firstTaskId);
	const secondAssignment = assignmentEntry(fixture.worker, secondTaskId);
	const gate = registerGate(fixture.worker, "1");

	expect(await call(gate, "agent_task_done", { taskId: firstTaskId, status: "completed", summary: "finished" }, [firstAssignment])).toBeUndefined();
	expect(await call(gate, "bash", { command: "pwd" }, [firstAssignment])).toEqual({ block: true, reason: DENIAL_CODE });
	expect(await call(gate, "agent_task_done", { taskId: firstTaskId, status: "completed", summary: "finished" }, [firstAssignment])).toBeUndefined();
	expect(await call(gate, "agent_task_done", { taskId: "foreign-task", status: "completed", summary: "finished" }, [firstAssignment])).toEqual({ block: true, reason: DENIAL_CODE });

	expect(await call(gate, "bash", { command: "pwd" }, [firstAssignment, secondAssignment])).toBeUndefined();
	expect(await call(gate, "agent_task_done", { taskId: secondTaskId, status: "completed", summary: "finished" }, [firstAssignment, secondAssignment])).toBeUndefined();
	expect(await call(gate, "bash", { command: "pwd" }, [firstAssignment, secondAssignment])).toEqual({ block: true, reason: DENIAL_CODE });
});

function registerGate(core: TaskCore, workerMode: string | undefined, neutralWorkerMode?: string): ToolCallHandler {
	const previous = process.env.PI_TASK_WORKER;
	const previousNeutral = process.env.WOLFPACK_TASK_WORKER;
	if (workerMode === undefined) delete process.env.PI_TASK_WORKER;
	else process.env.PI_TASK_WORKER = workerMode;
	if (neutralWorkerMode === undefined) delete process.env.WOLFPACK_TASK_WORKER;
	else process.env.WOLFPACK_TASK_WORKER = neutralWorkerMode;
	let handler: ToolCallHandler | undefined;
	try {
		registerAgentTaskTools({
			on(event: string, candidate: unknown): void {
				if (event === "tool_call") handler = candidate as ToolCallHandler;
			},
			registerTool(): void { undefined; },
		} as unknown as ExtensionAPI, core);
	} finally {
		if (previous === undefined) delete process.env.PI_TASK_WORKER;
		else process.env.PI_TASK_WORKER = previous;
		if (previousNeutral === undefined) delete process.env.WOLFPACK_TASK_WORKER;
		else process.env.WOLFPACK_TASK_WORKER = previousNeutral;
	}
	if (handler === undefined) throw new Error("expected the worker tool gate to register");
	return handler;
}

async function call(handler: ToolCallHandler, toolName: string, input: unknown, entries: readonly unknown[]): Promise<ToolCallResult | undefined> {
	return handler({ toolName, toolCallId: "call", input }, { sessionManager: { getEntries: () => entries } });
}

async function runTool(handler: ToolCallHandler, toolName: string, input: unknown, entries: readonly unknown[], execute: () => void): Promise<ToolCallResult | undefined> {
	const decision = await call(handler, toolName, input, entries);
	if (decision?.block !== true) execute();
	return decision;
}

async function assignedFixture(taskCount: number): Promise<{ readonly origin: TaskCore; readonly worker: TaskCore; readonly workerStore: ReturnType<typeof createTaskStore>; readonly taskIds: readonly string[] }> {
	const relay = createInMemoryTaskRelay("memory");
	const origin = createTaskCore({ endpoint: ORIGIN, relay, store: createTaskStore(), ids: sequence("origin") });
	const workerStore = createTaskStore();
	const worker = createTaskCore({ endpoint: WORKER, relay, store: workerStore, ids: sequence("worker") });
	await origin.connect();
	await worker.connect();
	const taskIds: string[] = [];
	for (let index = 0; index < taskCount; index += 1) {
		const created = await origin.createTask({ target: WORKER, task: `assignment ${index + 1}`, timeoutMs: 1_000 });
		taskIds.push(created.taskId);
	}
	await worker.receive();
	return { origin, worker, workerStore, taskIds };
}

function assignmentEntry(worker: TaskCore, taskId: string): SessionAssignmentEntry {
	const event = worker.getTask(taskId)?.events.find((candidate) => candidate.type === "task.created");
	if (event === undefined) throw new Error("expected persisted assignment event");
	return {
		type: "custom_message",
		customType: "pi-tasks-event",
		details: { taskId, eventId: event.eventId },
		content: "rendered content is not authorization evidence",
	};
}

function expiringOriginRelay(state: { blockOrigin: boolean }): TaskRelay {
	const relay = createInMemoryTaskRelay("memory");
	return {
		id: relay.id,
		async connect(input) { return relay.connect(input); },
		async resolve(input) { return relay.resolve(input); },
		async send(input) {
			if (state.blockOrigin && input.target.id === ORIGIN.id) throw new TaskProtocolError("TARGET_NOT_REGISTERED", "origin is inactive", { retryable: false, details: { targetId: input.target.id } });
			return relay.send(input);
		},
		async receive(input) { return relay.receive(input); },
		async acknowledgeDelivery(input) { await relay.acknowledgeDelivery(input); },
	};
}

function requiredTaskId(taskIds: readonly string[], index: number): string {
	const taskId = taskIds[index];
	if (taskId === undefined) throw new Error(`expected task id at index ${index}`);
	return taskId;
}

function sequence(prefix: string): () => string {
	let number = 0;
	return (): string => `${prefix}-${++number}`;
}
