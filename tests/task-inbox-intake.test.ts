import { expect, test } from "bun:test";
import { createInMemoryTaskRelay } from "../src/in-memory-task-relay";
import { createTaskCore } from "../src/task-core";
import { deliverTaskInbox, type TaskEventDetails } from "../src/task-inbox";
import { TaskEnvelopeKind, TaskProtocolError, type RelayDelivery, type RelayEnvelope, type TaskEndpoint, type TaskRelay } from "../src/task-protocol";
import { createTaskStore } from "../src/task-store";
import { TASK_WORK_LIMITS } from "../src/task-work-scope";

const parent = { relay: "memory", id: "parent" }, peer = { relay: "memory", id: "peer" }, child = { relay: "memory", id: "child" };

/** Fake Pi session whose idle and queued-message state the test controls. */
function session() {
	const entries: unknown[] = [];
	const state = { idle: true, queued: false };
	return {
		entries, state,
		pi: {
			sendMessage(message: { customType: string; details: TaskEventDetails }) { entries.push({ type: "custom_message", ...message }); },
			appendEntry(customType: string, data: unknown) { entries.push({ type: "custom", customType, data }); },
		},
		context: { isIdle: () => state.idle, hasPendingMessages: () => state.queued, sessionManager: { getEntries: () => entries } },
		/** `taskId:type` of model-visible insertions, in insertion order. */
		presented(): string[] {
			return entries.flatMap(entry => {
				const item = entry as { customType?: string; details?: { taskId: string; event?: { type: string } } };
				return item.customType === "pi-tasks-event" ? [`${item.details!.taskId}:${item.details!.event!.type}`] : [];
			});
		},
		archived(): string[] {
			return entries.flatMap(entry => {
				const item = entry as { customType?: string; data?: { taskId: string; event?: { type: string } } };
				return item.customType === "pi-tasks-event-record" ? [`${item.data!.taskId}:${item.data!.event!.type}`] : [];
			});
		},
	};
}

/** In-memory relay with a page-size override and a record of every fetch the endpoint makes. */
function pagedRelay(pageSize: number) {
	const backing = createInMemoryTaskRelay("memory");
	const fetches: Array<{ cursor: string; deliveries: string[] }> = [];
	let onFetch: ((count: number, signal?: AbortSignal) => Promise<void>) | undefined;
	let onSend: ((envelope: RelayEnvelope, signal?: AbortSignal) => Promise<void>) | undefined;
	const transport: TaskRelay = {
		id: backing.id,
		connect: input => backing.connect(input), resolve: input => backing.resolve(input),
		send: async (input, signal) => { await onSend?.(input, signal); return backing.send(input); },
		acknowledgeDelivery: input => backing.acknowledgeDelivery(input),
		receive: async (input, signal) => {
			await onFetch?.(fetches.length + 1, signal);
			const page = await backing.receive({ ...input, limit: Math.min(input.limit, pageSize) });
			fetches.push({ cursor: input.cursor, deliveries: page.deliveries.map(delivery => delivery.cursor) });
			return page;
		},
	};
	/** Unacknowledged relay mail for an endpoint, read straight from the backing relay. */
	const unacked = async (endpoint: TaskEndpoint): Promise<readonly RelayDelivery[]> => (await backing.receive({ endpoint, cursor: "0", limit: 100_000 })).deliveries;
	return { backing, transport, fetches, unacked, intercept(handler: typeof onFetch) { onFetch = handler; }, interceptSend(handler: typeof onSend) { onSend = handler; } };
}

test("intents beyond the first page are canonicalized and ACKed while a visible assignment is held; visible order holds once idle", async () => {
	const relay = pagedRelay(2), stores = [createTaskStore(), createTaskStore(), createTaskStore()];
	const p = createTaskCore({ endpoint: parent, relay: relay.transport, store: stores[0]! });
	const q = createTaskCore({ endpoint: peer, relay: relay.backing, store: stores[1]! });
	const c = createTaskCore({ endpoint: child, relay: relay.backing, store: stores[2]! });
	try {
		await p.connect(); await q.connect(); await c.connect();
		const own = await p.createTask({ target: child, task: "parent work", timeoutMs: 60_000 });
		await c.receive();
		const held = await q.createTask({ target: parent, task: "assignment for parent", timeoutMs: 60_000 });
		for (const message of ["one", "two", "three"]) await c.submitIntent({ taskId: own.taskId, type: "task.question", payload: { message } });
		const pi = session(); pi.state.idle = false;

		await deliverTaskInbox(pi.pi, p, pi.context);
		expect(p.getTask(own.taskId)!.events.filter(event => event.type === "task.question").map(event => event.payload.message)).toEqual(["one", "two", "three"]);
		expect((await relay.unacked(parent)).map(delivery => delivery.envelope.taskId)).toEqual([held.taskId]);
		expect(pi.presented()).toEqual([]);
		expect(stores[0]!.getReceiveCursor()).toBe("0");

		pi.state.idle = true;
		await deliverTaskInbox(pi.pi, p, pi.context);
		expect(pi.presented()).toEqual([`${held.taskId}:task.created`, ...Array(3).fill(`${own.taskId}:task.question`)]);
		expect(await relay.unacked(parent)).toEqual([]);
	} finally { for (const store of stores) store.close(); }
});

test("control facts behind a held visible event are archived and ACKed while Pi has queued messages", async () => {
	const relay = pagedRelay(50), stores = [createTaskStore(), createTaskStore()];
	const origin = createTaskCore({ endpoint: parent, relay: relay.backing, store: stores[0]! });
	const worker = createTaskCore({ endpoint: child, relay: relay.transport, store: stores[1]! });
	try {
		await origin.connect(); await worker.connect();
		const { taskId } = await origin.createTask({ target: child, task: "finish", timeoutMs: 60_000 });
		const pi = session();
		await deliverTaskInbox(pi.pi, worker, pi.context);
		await origin.submitIntent({ taskId, type: "task.information", payload: { message: "external message held before the echo" } });
		await worker.submitIntent({ taskId, type: "task.completed", payload: { summary: "done" } });
		await origin.receive();
		await origin.acknowledgeParent(taskId);
		expect((await relay.unacked(child)).length).toBe(3);
		pi.state.idle = false; pi.state.queued = true;

		await deliverTaskInbox(pi.pi, worker, pi.context);
		expect(pi.archived()).toEqual([`${taskId}:task.completed`, `${taskId}:task.parent_acknowledged`]);
		expect(pi.presented()).toEqual([`${taskId}:task.created`]);
		const remaining = await relay.unacked(child);
		expect(remaining.map(delivery => JSON.parse(delivery.envelope.payload).type)).toEqual(["task.information"]);
		expect(BigInt(stores[1]!.getReceiveCursor())).toBeLessThan(BigInt(remaining[0]!.cursor));

		pi.state.idle = true; pi.state.queued = false;
		await deliverTaskInbox(pi.pi, worker, pi.context);
		expect(pi.presented()).toEqual([`${taskId}:task.created`, `${taskId}:task.information`]);
		expect(await relay.unacked(child)).toEqual([]);
	} finally { for (const store of stores) store.close(); }
});

test("a full pending delivery set pauses fetching and resumes once ACKs drain it", async () => {
	const cap = TASK_WORK_LIMITS.pendingDeliveries, relay = pagedRelay(50), stores = [createTaskStore(), createTaskStore()];
	const p = createTaskCore({ endpoint: parent, relay: relay.transport, store: stores[0]! });
	const q = createTaskCore({ endpoint: peer, relay: relay.backing, store: stores[1]! });
	try {
		await p.connect(); await q.connect();
		const created: string[] = [];
		for (let i = 0; i < cap + 5; i++) created.push((await q.createTask({ target: parent, task: `assignment ${i}`, timeoutMs: 60_000 })).taskId);
		const pi = session(); pi.state.idle = false;

		await deliverTaskInbox(pi.pi, p, pi.context);
		expect(relay.fetches.flatMap(fetch => fetch.deliveries)).toHaveLength(cap);
		const fetched = relay.fetches.length;
		await deliverTaskInbox(pi.pi, p, pi.context);
		expect(relay.fetches).toHaveLength(fetched);

		pi.state.idle = true;
		for (let refresh = 0; refresh < 30 && (await relay.unacked(parent)).length > 0; refresh++) await deliverTaskInbox(pi.pi, p, pi.context);
		expect(await relay.unacked(parent)).toEqual([]);
		expect(relay.fetches.flatMap(fetch => fetch.deliveries)).toHaveLength(cap + 5);
		expect(pi.presented()).toEqual(created.map(taskId => `${taskId}:task.created`));
	} finally { for (const store of stores) store.close(); }
});

test("an abort during a later page fetch loses nothing; the next refresh fetches from the intake cursor", async () => {
	const relay = pagedRelay(1), stores = [createTaskStore(), createTaskStore()];
	const p = createTaskCore({ endpoint: parent, relay: relay.transport, store: stores[0]! });
	const q = createTaskCore({ endpoint: peer, relay: relay.backing, store: stores[1]! });
	try {
		await p.connect(); await q.connect();
		const created: string[] = [];
		for (let i = 0; i < 3; i++) created.push((await q.createTask({ target: parent, task: `assignment ${i}`, timeoutMs: 60_000 })).taskId);
		const pi = session(), controller = new AbortController();
		let aborted = false;
		relay.intercept(async (count, signal) => {
			if (count !== 2) return;
			aborted = true; controller.abort();
			throw signal?.reason ?? new TaskProtocolError("ABORTED", "aborted");
		});
		await deliverTaskInbox(pi.pi, p, pi.context, controller.signal).catch(() => undefined);
		expect(aborted).toBe(true);
		relay.intercept(undefined);
		const first = relay.fetches[0]!.deliveries[0]!;

		await deliverTaskInbox(pi.pi, p, pi.context);
		// Fetching resumes after the persisted first page instead of re-reading it.
		expect(relay.fetches[1]!.cursor).toBe(first);
		for (let refresh = 0; refresh < 5 && (await relay.unacked(parent)).length > 0; refresh++) await deliverTaskInbox(pi.pi, p, pi.context);
		expect(await relay.unacked(parent)).toEqual([]);
		expect(pi.presented()).toEqual(created.map(taskId => `${taskId}:task.created`));
	} finally { for (const store of stores) store.close(); }
});

test("an abort during the ship phase keeps every fetched delivery tracked, withholds its ACK and re-drives it in order", async () => {
	const relay = pagedRelay(50), stores = [createTaskStore(), createTaskStore()];
	const p = createTaskCore({ endpoint: parent, relay: relay.transport, store: stores[0]! });
	const c = createTaskCore({ endpoint: child, relay: relay.backing, store: stores[1]! });
	try {
		await p.connect(); await c.connect();
		const { taskId } = await p.createTask({ target: child, task: "parent work", timeoutMs: 60_000 });
		await c.receive();
		for (const message of ["one", "two", "three"]) await c.submitIntent({ taskId, type: "task.information", payload: { message } });
		const pi = session(), controller = new AbortController();
		let fanout = 0;
		// Intake persisted the whole page; the second fanout send of the ship phase hangs until the refresh is aborted.
		relay.interceptSend(async (envelope, signal) => {
			if (envelope.kind !== TaskEnvelopeKind.canonicalEvent || ++fanout !== 2) return;
			setTimeout(() => controller.abort(), 0);
			await new Promise((_, reject) => signal?.addEventListener("abort", () => reject(new TaskProtocolError("ABORTED", "aborted")), { once: true }));
		});
		await deliverTaskInbox(pi.pi, p, pi.context, controller.signal).catch(() => undefined);
		expect(fanout).toBe(2);
		relay.interceptSend(undefined);
		const [first, second, third] = relay.fetches[0]!.deliveries;
		expect(stores[0]!.pendingRelayDeliveries(parent).map(pending => pending.cursor)).toEqual([first!, second!, third!]);
		expect(await relay.unacked(parent)).toHaveLength(3);
		// Only the intent whose fan-out was accepted before the abort has an authorized ACK.
		expect(stores[0]!.requestedRelayAcknowledgements(parent, Number.MAX_SAFE_INTEGER, Number.MAX_SAFE_INTEGER)).toEqual([first!]);

		await deliverTaskInbox(pi.pi, p, pi.context);
		// Every fetched delivery is tracked: fetching resumes after the third and the unsent ones are re-driven.
		expect(relay.fetches[1]!.cursor).toBe(third!);
		for (let refresh = 0; refresh < 5 && (await relay.unacked(parent)).length > 0; refresh++) await deliverTaskInbox(pi.pi, p, pi.context);
		expect(await relay.unacked(parent)).toEqual([]);
		expect(first).toBeDefined();
		expect(p.getTask(taskId)!.events.filter(event => event.type === "task.information").map(event => event.payload.message)).toEqual(["one", "two", "three"]);
		// The aborted fan-out was shipped again before its ACK: the worker receives every canonical update, in order.
		await c.receive();
		expect(c.getTask(taskId)!.events.filter(event => event.type === "task.information").map(event => event.payload.message)).toEqual(["one", "two", "three"]);
	} finally { for (const store of stores) store.close(); }
});
