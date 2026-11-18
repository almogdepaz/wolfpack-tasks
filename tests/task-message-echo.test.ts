import { expect, spyOn, test } from "bun:test";
import { createInMemoryTaskRelay } from "../src/in-memory-task-relay";
import { createTaskCore, type TaskInboxItem } from "../src/task-core";
import { deliverTaskInbox, type TaskEventDetails } from "../src/task-inbox";
import { TaskEnvelopeKind, TaskProtocolError, type TaskEvent } from "../src/task-protocol";
import { createTaskStore } from "../src/task-store";

const origin = { relay: "memory", id: "origin" };
const receiver = { relay: "memory", id: "receiver" };
const conversationTypes: Array<"task.information" | "task.question" | "task.answer"> = ["task.information", "task.question", "task.answer"];
// Receiver-authored information is worker progress: inspection-only at the origin.
const originActionableTypes = conversationTypes.filter(type => type !== "task.information");
function ids(prefix: string) { let next = 0; return () => `${prefix}-${++next}`; }
function relayCursor(item: TaskInboxItem | undefined): string { if (item?.kind !== "relay") throw new Error("expected a relay item"); return item.delivery.cursor; }
function presentation() {
	const messages: Array<{ customType: string; details: TaskEventDetails }> = [];
	const records: Array<{ customType: string; data: { cursor: string } | TaskEventDetails }> = [];
	const entries: unknown[] = [];
	return {
		messages, records, entries,
		pi: {
			sendMessage(message: { customType: string; details: TaskEventDetails }) { messages.push(message); entries.push({ type: "custom_message", ...message }); },
			appendEntry(customType: string, data: { cursor: string } | TaskEventDetails) { records.push({ customType, data }); entries.push({ type: "custom", customType, data }); },
		},
		context: { isIdle: () => true, hasPendingMessages: () => false, sessionManager: { getEntries: () => entries } },
	};
}
async function fixture() {
	const relay = createInMemoryTaskRelay("memory"), parentStore = createTaskStore(), childStore = createTaskStore();
	const parent = createTaskCore({ endpoint: origin, relay, store: parentStore, ids: ids("parent") });
	const child = createTaskCore({ endpoint: receiver, relay, store: childStore, ids: ids("child") });
	await parent.connect(); await child.connect();
	const task = await parent.createTask({ target: receiver, task: "conversation", timeoutMs: 60_000 });
	await child.acknowledgeRelayDelivery(relayCursor((await child.receive())[0]));
	return { relay, parentStore, childStore, parent, child, taskId: task.taskId };
}
function conversation(core: ReturnType<typeof createTaskCore>, taskId: string, type: string): TaskEvent {
	const event = core.getTask(taskId)?.events.find(event => event.type === type);
	if (!event) throw new Error("expected canonical conversation");
	return event;
}

test("archives receiver-authored task.information at both ends: origin progress record and receiver self-echo", async () => {
	const f = await fixture(), parent = presentation(), child = presentation();
	await f.child.submitIntent({ taskId: f.taskId, type: "task.information", payload: { message: "phase 1 halfway" } });
	await deliverTaskInbox(parent.pi, f.parent, parent.context);
	const event = conversation(f.parent, f.taskId, "task.information");
	expect(parent.messages).toEqual([]);
	expect(parent.records).toContainEqual({ customType: "pi-tasks-event-record", data: { taskId: f.taskId, eventId: event.eventId, event } });
	await deliverTaskInbox(child.pi, f.child, child.context);
	expect(child.messages).toEqual([]);
	expect(child.records).toContainEqual({ customType: "pi-tasks-event-record", data: { taskId: f.taskId, eventId: event.eventId, event } });
	await deliverTaskInbox(parent.pi, f.parent, parent.context);
	expect(parent.messages).toEqual([]);
	expect(parent.records.filter(record => record.customType === "pi-tasks-event-record")).toHaveLength(1);
	expect(f.parent.getTask(f.taskId)?.events).toContainEqual(event); // Still inspectable through task status.
	f.parentStore.close(); f.childStore.close();
});

test.each(originActionableTypes)("archives receiver-authored %s without a self wake, preserving origin insertion and wake", async type => {
	const f = await fixture(), parent = presentation(), child = presentation();
	await f.child.submitIntent({ taskId: f.taskId, type, payload: { message: "same prose in either direction" } });
	await deliverTaskInbox(parent.pi, f.parent, parent.context); // Canonicalize the raw intent and present it locally in the same refresh.
	const event = conversation(f.parent, f.taskId, type);
	expect(parent.messages.map(message => [message.customType, message.details.eventId])).toEqual([["pi-tasks-event", event.eventId], ["pi-tasks-wake", event.eventId]]);
	await deliverTaskInbox(child.pi, f.child, child.context);
	expect(child.messages).toEqual([]);
	expect(child.records).toContainEqual({ customType: "pi-tasks-event-record", data: { taskId: f.taskId, eventId: event.eventId, event } });
	expect(f.child.getTask(f.taskId)?.events).toContainEqual(event);
	expect(f.childStore.outbox("accepted")).toHaveLength(1); // No fabricated insertion/wake receipt for an archived echo.
	expect(await f.child.receive()).toEqual([]);
	await deliverTaskInbox(parent.pi, f.parent, parent.context);
	expect(parent.messages.map(message => message.customType)).toEqual(["pi-tasks-event", "pi-tasks-wake"]);
	// Origin-local presentation emits no relay delivery receipts for the origin's own view.
	const evidence = f.parent.getTask(f.taskId)!.events.filter(candidate => candidate.type === "task.delivery_receipt" && candidate.payload.eventId === event.eventId);
	expect(evidence).toEqual([]);
	f.parentStore.close(); f.childStore.close();
});

test.each(conversationTypes)("origin-authored %s remains actionable at receiver despite matching prose and spoofed author/intent fields", async type => {
	const f = await fixture(), child = presentation(), parent = presentation();
	await f.child.submitIntent({ taskId: f.taskId, type, payload: { message: "identical" } });
	await f.parent.receive();
	const selfEvent = conversation(f.parent, f.taskId, type);
	await f.parent.submitIntent({ taskId: f.taskId, type, payload: { message: "identical", intentId: selfEvent.payload.intentId, author: receiver, authoredBy: receiver } });
	const peerEvent = f.parent.getTask(f.taskId)!.events.filter(event => event.type === type).at(-1)!;
	expect(peerEvent.payload.intentId).not.toBe(selfEvent.payload.intentId);
	await deliverTaskInbox(child.pi, f.child, child.context);
	expect(child.messages.map(message => [message.customType, message.details.eventId])).toEqual([["pi-tasks-event", peerEvent.eventId], ["pi-tasks-wake", peerEvent.eventId]]);
	await deliverTaskInbox(parent.pi, f.parent, parent.context);
	if (type === "task.information") {
		expect(parent.messages).toEqual([]);
		expect(parent.records).toContainEqual({ customType: "pi-tasks-event-record", data: { taskId: f.taskId, eventId: selfEvent.eventId, event: selfEvent } });
	} else {
		expect(parent.messages.map(message => message.details.eventId)).toEqual([selfEvent.eventId, selfEvent.eventId]);
	}
	f.parentStore.close(); f.childStore.close();
});

test("uses trusted receiver intent identity rather than a payload override and deduplicates archive before failed ACK/replacement", async () => {
	const f = await fixture(), child = presentation();
	await f.child.submitIntent({ taskId: f.taskId, type: "task.question", payload: { message: "question", intentId: { malformed: true }, author: origin } });
	const intentEnvelope = f.childStore.outbox("accepted").find(record => record.envelope.kind === TaskEnvelopeKind.intent)!.envelope;
	await f.parent.receive();
	const event = conversation(f.parent, f.taskId, "task.question");
	expect(event.payload.intentId).toBe(JSON.parse(intentEnvelope.payload).intentId);
	const acknowledge = f.child.acknowledgeRelayDelivery.bind(f.child);
	let failed = false;
	const ack = spyOn(f.child, "acknowledgeRelayDelivery").mockImplementation(async cursor => {
		expect(child.records.some(record => record.customType === "pi-tasks-event-record" && "eventId" in record.data && record.data.eventId === event.eventId)).toBe(true);
		if (!failed) { failed = true; throw new Error("fixture ACK unavailable"); }
		return acknowledge(cursor);
	});
	try { await expect(deliverTaskInbox(child.pi, f.child, child.context)).rejects.toThrow("fixture ACK unavailable"); } finally { ack.mockRestore(); }
	const replacement = createTaskCore({ endpoint: receiver, relay: f.relay, store: f.childStore, ids: ids("replacement") });
	await deliverTaskInbox(child.pi, replacement, child.context);
	await deliverTaskInbox(child.pi, replacement, child.context);
	expect(child.messages).toEqual([]);
	expect(child.records.filter(record => record.customType === "pi-tasks-event-record")).toHaveLength(1);
	expect(replacement.getTask(f.taskId)?.events.filter(candidate => candidate.eventId === event.eventId)).toHaveLength(1);
	f.parentStore.close(); f.childStore.close();
});

test.each(["RELAY_UNAVAILABLE", "DELIVERY_UNCONFIRMED"])("lost acceptance (%s) retains same-lifetime authorship, but fresh RAM cannot recover it from session history", async code => {
	const f = await fixture(), child = presentation();
	const send = f.relay.send.bind(f.relay);
	let fail = true;
	const sending = spyOn(f.relay, "send").mockImplementation(async envelope => {
		const result = await send(envelope);
		if (fail && envelope.kind === TaskEnvelopeKind.intent && envelope.source.id === receiver.id) throw new TaskProtocolError(code, "fixture lost acceptance", { retryable: code === "RELAY_UNAVAILABLE" });
		return result;
	});
	try {
		await expect(f.child.submitIntent({ taskId: f.taskId, type: "task.answer", payload: { message: "answer" } })).rejects.toThrow("fixture lost acceptance");
		await f.parent.receive();
		await deliverTaskInbox(child.pi, f.child, child.context);
		expect(child.messages).toEqual([]);
		expect(f.childStore.outbox("pending")).toHaveLength(code === "RELAY_UNAVAILABLE" ? 1 : 0);
		expect(f.childStore.quarantinedOutbox()).toHaveLength(code === "DELIVERY_UNCONFIRMED" ? 1 : 0);
		fail = false;
		await f.child.flushOutbox();
		expect(f.childStore.outbox("pending")).toEqual([]);
		const event = conversation(f.parent, f.taskId, "task.answer");
		await f.relay.send({ envelopeId: "replay-after-loss", protocolVersion: "pi-tasks/v2", source: origin, target: receiver, taskId: f.taskId, kind: TaskEnvelopeKind.canonicalEvent, payload: JSON.stringify(event) });
		const fresh = createTaskCore({ endpoint: receiver, relay: f.relay, store: createTaskStore(), ids: ids("fresh") });
		await expect(deliverTaskInbox(child.pi, fresh, child.context)).rejects.toMatchObject({ code: "UNKNOWN_TASK", retryable: false });
		expect(fresh.listTasks()).toEqual([]);
		expect(child.messages).toEqual([]);
	} finally { sending.mockRestore(); f.parentStore.close(); f.childStore.close(); }
});

test.each([undefined, null, 42, "", "unrecorded"])("unproven canonical provenance (%s) cannot suppress a peer conversation", async intentId => {
	const f = await fixture(), child = presentation();
	await f.relay.send({ envelopeId: "peer-envelope", protocolVersion: "pi-tasks/v2", source: origin, target: receiver, taskId: f.taskId, kind: TaskEnvelopeKind.canonicalEvent,
		payload: JSON.stringify({ eventId: "peer-event", taskId: f.taskId, type: "task.information", sequence: "2", source: origin, target: receiver, occurredAt: 1, payload: { message: "peer", intentId, author: receiver, authoredBy: receiver } }) });
	await deliverTaskInbox(child.pi, f.child, child.context);
	expect(child.messages.map(message => message.customType)).toEqual(["pi-tasks-event", "pi-tasks-wake"]);
	f.parentStore.close(); f.childStore.close();
});

test("local intent correlation is task/type scoped and canonical replay stays non-waking", async () => {
	const f = await fixture(), child = presentation();
	await f.child.submitIntent({ taskId: f.taskId, type: "task.question", payload: { message: "self" } });
	await f.parent.receive();
	const selfEvent = conversation(f.parent, f.taskId, "task.question");
	await deliverTaskInbox(child.pi, f.child, child.context);
	await f.relay.send({ envelopeId: "self-replay", protocolVersion: "pi-tasks/v2", source: origin, target: receiver, taskId: f.taskId, kind: TaskEnvelopeKind.canonicalEvent, payload: JSON.stringify(selfEvent) });
	await deliverTaskInbox(child.pi, f.child, child.context);
	expect(child.messages).toEqual([]);
	expect(child.records.filter(record => record.customType === "pi-tasks-event-record")).toHaveLength(1);
	const second = await f.parent.createTask({ target: receiver, task: "another task", timeoutMs: 60_000 });
	await f.child.acknowledgeRelayDelivery(relayCursor((await f.child.receive())[0]));
	for (const [taskId, type, eventId] of [[f.taskId, "task.answer", "wrong-type"], [second.taskId, "task.question", "wrong-task"]]) {
		await f.relay.send({ envelopeId: `${eventId}-envelope`, protocolVersion: "pi-tasks/v2", source: origin, target: receiver, taskId: taskId!, kind: TaskEnvelopeKind.canonicalEvent,
			payload: JSON.stringify({ ...selfEvent, taskId, type, eventId, sequence: "3" }) });
	}
	await deliverTaskInbox(child.pi, f.child, child.context);
	expect(child.messages.map(message => message.details.eventId)).toEqual(["wrong-type", "wrong-type", "wrong-task", "wrong-task"]);
	f.parentStore.close(); f.childStore.close();
});
