import { expect, spyOn, test } from "bun:test";

import { createInMemoryTaskRelay } from "../src/in-memory-task-relay";
import { createTaskCore } from "../src/index";
import type { TaskCore } from "../src/index";
import { deliverTaskInbox } from "../src/task-inbox";
import { TASK_PROTOCOL_VERSION } from "../src/task-protocol";
import { createTaskStore } from "../src/task-store";

const origin = { relay: "memory", id: "origin" };
const receiver = { relay: "memory", id: "receiver" };

function presentation() {
	const entries: any[] = [];
	return {
		entries,
		pi: {
			sendMessage(message: unknown) { entries.push({ type: "custom_message", ...message as object }); },
			appendEntry(customType: string, data: unknown) { entries.push({ type: "custom", customType, data }); },
		},
		context: { isIdle: () => true, hasPendingMessages: () => false, sessionManager: { getEntries: () => entries } },
		messages: () => entries.filter(entry => entry.type === "custom_message"),
		records: () => entries.filter(entry => entry.customType === "pi-tasks-event-record"),
	};
}

async function fixture() {
	const relay = createInMemoryTaskRelay("memory");
	const parentStore = createTaskStore(), childStore = createTaskStore();
	let now = 1_000;
	const clock = { now: () => now };
	const parent = createTaskCore({ endpoint: origin, relay, store: parentStore, clock });
	const child = createTaskCore({ endpoint: receiver, relay, store: childStore, clock });
	await parent.connect(); await child.connect();
	const { taskId } = await parent.createTask({ target: receiver, task: "terminal echo regression", timeoutMs: 60_000 });
	const worker = presentation(), coordinator = presentation();
	await deliverTaskInbox(worker.pi, child, worker.context);
	await deliverTaskInbox(coordinator.pi, parent, coordinator.context);
	worker.entries.length = 0;
	return { relay, parent, child, childStore, taskId, worker, coordinator, expire: () => { now += 60_001; } };
}

test.each(["task.information", "task.question", "task.answer", "task.completed", "task.failed", "task.cancelled"])("public TaskCore retains isSelfAuthoredConversation for correlated %s echoes", async type => {
	const { parent, child, taskId } = await fixture();
	const publicCore: TaskCore = child;
	await child.submitIntent({ taskId, type, payload: { message: "authored locally", summary: "done" } });
	await parent.receive();
	const canonical = parent.getTask(taskId)!.events.find(event => event.type === type)!;
	expect(canonical).toBeDefined();
	expect(typeof publicCore.isSelfAuthoredConversation).toBe("function");
	expect(publicCore.isSelfAuthoredConversation(canonical)).toBe(true);
	expect(parent.isSelfAuthoredConversation(canonical)).toBe(false);
});

test.each([
	["task.completed", "completed"],
	["task.failed", "failed"],
	["task.cancelled", "cancelled"],
] as const)("archives self-authored %s without insertion, wake or receipt; parent still receives it", async (type, status) => {
	const { relay, parent, child, childStore, taskId, worker, coordinator } = await fixture();
	await child.submitIntent({ taskId, type, payload: { summary: "worker finished" } });
	await deliverTaskInbox(coordinator.pi, parent, coordinator.context);
	const terminal = parent.getTask(taskId)!.events.find(event => event.type === type)!;
	expect(terminal).toBeDefined();
	expect(coordinator.messages().map(entry => [entry.customType, entry.details.eventId])).toEqual([
		["pi-tasks-event", terminal.eventId], ["pi-tasks-wake", terminal.eventId],
	]);
	const outboundBeforeEcho = relay.envelopesFor(origin).length;
	await deliverTaskInbox(worker.pi, child, worker.context);
	expect(child.getTask(taskId)!.status).toBe(status);
	expect(worker.messages()).toEqual([]);
	expect(worker.records().map(entry => entry.data.event)).toEqual([terminal]);
	expect(relay.envelopesFor(origin)).toHaveLength(outboundBeforeEcho);
	expect(childStore.pendingRelayDeliveries(receiver)).toEqual([]);
	expect((await relay.receive({ endpoint: receiver, cursor: "0", limit: 100 })).deliveries).toEqual([]);

	// A replay with a new transport identity must not duplicate the archive or produce a turn.
	const canonical = relay.envelopesFor(receiver).find(envelope => envelope.kind === "canonical_event" && JSON.parse(envelope.payload).eventId === terminal.eventId)!;
	await relay.send({ ...canonical, envelopeId: "terminal-replay" });
	await deliverTaskInbox(worker.pi, child, worker.context);
	await deliverTaskInbox(coordinator.pi, parent, coordinator.context);
	expect(worker.messages()).toEqual([]);
	expect(worker.records()).toHaveLength(1);
	expect(coordinator.messages()).toHaveLength(2);
	expect(relay.envelopesFor(origin)).toHaveLength(outboundBeforeEcho);
	expect(childStore.pendingRelayDeliveries(receiver)).toEqual([]);
});

test("archives a terminal echo while busy, before ACK failure, and repairs the ACK without a turn", async () => {
	const { relay, parent, child, childStore, taskId, worker } = await fixture();
	await child.submitIntent({ taskId, type: "task.completed", payload: { summary: "done" } });
	await parent.receive();
	const acknowledge = relay.acknowledgeDelivery.bind(relay);
	let fail = true;
	const ack = spyOn(relay, "acknowledgeDelivery").mockImplementation(async input => {
		if (input.endpoint.id === receiver.id && fail) {
			fail = false;
			expect(worker.records()).toHaveLength(1);
			throw new Error("fixture ACK unavailable");
		}
		await acknowledge(input);
	});
	const busy = { ...worker.context, isIdle: () => false, hasPendingMessages: () => true };
	try {
		await expect(deliverTaskInbox(worker.pi, child, busy)).rejects.toThrow("fixture ACK unavailable");
		expect(childStore.pendingRelayDeliveries(receiver)).toHaveLength(1);
		await deliverTaskInbox(worker.pi, child, busy);
		expect(childStore.pendingRelayDeliveries(receiver)).toEqual([]);
		expect(worker.records()).toHaveLength(1);
		expect(worker.messages()).toEqual([]);
	} finally { ack.mockRestore(); }
});

test.each([
	["task.cancelled", "cancelled"],
	["task.timed_out", "timed_out"],
] as const)("delivers external %s and information even after a local terminal intent", async (type, status) => {
	const { parent, child, taskId, worker, expire } = await fixture();
	await child.submitIntent({ taskId, type: "task.completed", payload: { summary: "local completion not yet canonicalized" } });
	// The origin wins the terminal race before receiving the worker intent.
	if (type === "task.timed_out") { expire(); await parent.evaluateTimeouts(); }
	else await parent.submitIntent({ taskId, type, payload: {} });
	await parent.receive();
	await parent.submitIntent({ taskId, type: "task.information", payload: { message: "external information after closing" } });
	await deliverTaskInbox(worker.pi, child, worker.context);
	expect(worker.messages().filter(entry => entry.customType === "pi-tasks-event").map(entry => entry.details.event.type)).toEqual([type, "task.information"]);
	expect(worker.messages().filter(entry => entry.customType === "pi-tasks-wake")).toHaveLength(2);
	expect(child.getTask(taskId)!.status).toBe(status);
});

test.each(["missing", "unknown", "other-task", "other-type"])("does not suppress terminal payload claims with %s local intent correlation", async mismatch => {
	const { relay, parent, child, taskId, worker } = await fixture();
	let intentId: string | undefined;
	if (mismatch === "unknown") intentId = "not-a-local-intent";
	if (mismatch === "other-task" || mismatch === "other-type") {
		const intentTaskId = mismatch === "other-task"
			? (await parent.createTask({ target: receiver, task: "unrelated task", timeoutMs: 60_000 })).taskId : taskId;
		await deliverTaskInbox(worker.pi, child, worker.context);
		worker.entries.length = 0;
		const type = mismatch === "other-task" ? "task.completed" : "task.information";
		await child.submitIntent({ taskId: intentTaskId, type, payload: { message: "local intent" } });
		intentId = JSON.parse(relay.envelopesFor(origin).at(-1)!.payload).intentId;
		expect(typeof intentId).toBe("string");
	}
	// Exercise actual envelope validation and inbox presentation, not the classifier in isolation.
	await relay.send({
		envelopeId: "unmatched-terminal", protocolVersion: TASK_PROTOCOL_VERSION, source: origin, target: receiver, taskId, kind: "canonical_event",
		payload: JSON.stringify({ eventId: "unmatched-event", taskId, type: "task.completed", sequence: "3", source: origin, target: receiver, occurredAt: 2_000, payload: { ...(intentId && { intentId }), summary: "external completion" } }),
	});
	await deliverTaskInbox(worker.pi, child, worker.context);
	expect(worker.messages().map(entry => entry.customType)).toEqual(["pi-tasks-event", "pi-tasks-wake"]);
	expect(worker.records()).toEqual([]);
});
