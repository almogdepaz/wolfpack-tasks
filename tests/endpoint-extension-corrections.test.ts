import { expect, jest, spyOn, test } from "bun:test";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

import piTasks, { ACTIVE_POLL_MS, IDLE_POLL_MS, createConfiguredCoreLoader, createSingleFlightInboxRefresh, registerAgentTaskTools } from "../src/extension";
import { createInMemoryTaskRelay } from "../src/in-memory-task-relay";
import { createTaskCore } from "../src/task-core";
import type { TaskCore } from "../src/task-core";
import { createTaskStore } from "../src/task-store";
import { TASK_PROTOCOL_VERSION, TaskEnvelopeKind, TaskOutboxDeliveryError, TaskProtocolError } from "../src/task-protocol";

interface Tool {
	readonly name: string;
	execute(id: string, parameters: Record<string, unknown>, signal: AbortSignal, update: undefined, context: unknown): Promise<{ readonly content: readonly { readonly text: string }[]; readonly details: unknown }>;
}

test("default extension uses the configured Wolfpack relay adapter rather than an in-memory or missing relay", async () => {
	const previousSession = process.env.WOLFPACK_SESSION_NAME;
	delete process.env.WOLFPACK_SESSION_NAME;
	const tools: Record<string, Tool> = {};
	piTasks({ on: () => undefined, registerCommand: () => undefined, registerTool(tool: unknown) { const value = tool as Tool; tools[value.name] = value; } } as unknown as ExtensionAPI);

	const result = await tools.agent_task_send!.execute("call", { to: { relay: "wolfpack-pi-tasks-v2", id: "opaque" }, task: "implement" }, new AbortController().signal, undefined, {});

	if (previousSession === undefined) delete process.env.WOLFPACK_SESSION_NAME;
	else process.env.WOLFPACK_SESSION_NAME = previousSession;
	expect(result.content[0]?.text).toContain("WOLFPACK_SESSION_NAME is required");
});

test("returns structured relay metadata and the persisted task id after assignment delivery fails", async () => {
	const tools: Record<string, Tool> = {};
	const core = {
		async createTask(): Promise<never> {
			throw new TaskProtocolError("TARGET_NOT_REGISTERED", "target is inactive", { retryable: false, details: { taskId: "persisted-task" } });
		},
		hasPendingWork: () => false,
	} as unknown as TaskCore;
	registerAgentTaskTools({
		on(): void { undefined; },
		registerTool(tool: unknown): void { const value = tool as Tool; tools[value.name] = value; },
	} as unknown as ExtensionAPI, core);

	const result = await tools.agent_task_send!.execute("call", { to: { relay: "wolfpack-pi-tasks-v2", id: "inactive" }, task: "persist first" }, new AbortController().signal, undefined, {});

	expect(result.details).toEqual({
		taskId: "persisted-task",
		error: { code: "TARGET_NOT_REGISTERED", message: "target is inactive", retryable: false },
	});
});

test("reports canonical origin cancellation with a structured blocked-delivery warning", async () => {
	const tools: Record<string, Tool> = {};
	const origin = { relay: "memory", id: "origin" } as const;
	const target = { relay: "memory", id: "receiver" } as const;
	const core = {
		endpoint: origin,
		async submitIntent(): Promise<never> {
			throw new TaskOutboxDeliveryError("TARGET_NOT_REGISTERED", "target is inactive", {
				retryable: false,
				details: { taskId: "task-1", envelopeId: "cancel-envelope", target, blockedAt: 1, targetId: target.id },
			});
		},
		getTask(taskId: string) {
			return { taskId, origin, target, status: "cancelled" };
		},
		hasPendingWork: () => false,
	} as unknown as TaskCore;
	registerAgentTaskTools({
		on(): void { undefined; },
		registerTool(tool: unknown): void { const value = tool as Tool; tools[value.name] = value; },
	} as unknown as ExtensionAPI, core);

	const result = await tools.agent_task_cancel!.execute("call", { taskId: "task-1" }, new AbortController().signal, undefined, {});

	expect(result.details).toEqual({
		taskId: "task-1",
		status: "cancelled",
		warnings: [{
			code: "TARGET_NOT_REGISTERED",
			message: "target is inactive",
			retryable: false,
			delivery: "blocked",
			target,
			details: { targetId: target.id },
		}],
	});
	expect(result.content[0]?.text).toContain("canonical status: cancelled");
	expect(result.content[0]?.text).toContain("receiver incorporation not confirmed");
});

test("continues timeout and inbox processing while reporting degraded outbox delivery", async () => {
	let sessionStart: ((event: unknown, context: unknown) => Promise<unknown>) | undefined;
	let sessionShutdown: (() => void) | undefined;
	const calls: string[] = [];
	const statuses: Array<string | undefined> = [];
	const core = {
		async connect(): Promise<void> { calls.push("connect"); },
		async flushOutbox(): Promise<void> {
			calls.push("flush");
			throw new TaskOutboxDeliveryError("TARGET_NOT_REGISTERED", "target is inactive", { retryable: false });
		},
		async evaluateTimeouts(): Promise<void> { calls.push("timeout"); },
		async intakeInbox(this: Pick<TaskCore, "receive">) { return this.receive(); },
		async shipRequired() {},
		async repairAcknowledgements() {},
		async receive(): Promise<readonly []> { calls.push("receive"); return []; },
		hasPendingWork: () => false,
	} as unknown as TaskCore;
	const context = {
		hasPendingMessages: (): boolean => false,
		sessionManager: { getEntries: (): readonly unknown[] => [] },
		ui: {
			setStatus: (_key: string, value: string | undefined): void => { statuses.push(value); },
			theme: { fg: (_color: string, text: string): string => text },
		},
	};
	registerAgentTaskTools({
		on(event: string, handler: unknown): void {
			if (event === "session_start") sessionStart = handler as (event: unknown, context: unknown) => Promise<unknown>;
			if (event === "session_shutdown") sessionShutdown = handler as () => void;
		},
		registerTool(): void { undefined; },
	} as unknown as ExtensionAPI, core);

	try {
		await sessionStart!({}, context);
		expect(calls).toEqual(["connect", "timeout", "receive", "flush"]);
		expect(statuses.at(-1)).toBe("tasks: outbox degraded");
	} finally {
		sessionShutdown?.();
	}
});

test("continues inbox receive and reports degradation when timeout delivery fails", async () => {
	let sessionStart: ((event: unknown, context: unknown) => Promise<unknown>) | undefined;
	let sessionShutdown: (() => void) | undefined;
	const calls: string[] = [];
	const statuses: Array<string | undefined> = [];
	const core = {
		async connect(): Promise<void> { calls.push("connect"); },
		async flushOutbox(): Promise<void> { calls.push("flush"); },
		async evaluateTimeouts(): Promise<void> {
			calls.push("timeout");
			throw new TaskOutboxDeliveryError("TARGET_NOT_REGISTERED", "timed-out task target is inactive", { retryable: false });
		},
		async intakeInbox(this: Pick<TaskCore, "receive">) { return this.receive(); },
		async shipRequired() {},
		async repairAcknowledgements() {},
		async receive(): Promise<readonly []> { calls.push("receive"); return []; },
		hasPendingWork: () => false,
	} as unknown as TaskCore;
	const context = {
		hasPendingMessages: (): boolean => false,
		sessionManager: { getEntries: (): readonly unknown[] => [] },
		ui: {
			setStatus: (_key: string, value: string | undefined): void => { statuses.push(value); },
			theme: { fg: (_color: string, text: string): string => text },
		},
	};
	registerAgentTaskTools({
		on(event: string, handler: unknown): void {
			if (event === "session_start") sessionStart = handler as (event: unknown, context: unknown) => Promise<unknown>;
			if (event === "session_shutdown") sessionShutdown = handler as () => void;
		},
		registerTool(): void { undefined; },
	} as unknown as ExtensionAPI, core);

	try {
		await sessionStart!({}, context);
		expect(calls).toEqual(["connect", "timeout", "receive", "flush"]);
		expect(statuses.at(-1)).toBe("tasks: outbox degraded");
	} finally {
		sessionShutdown?.();
	}
});

test("reports receive-created delivery failures as outbox degradation", async () => {
	let sessionStart: ((event: unknown, context: unknown) => Promise<unknown>) | undefined;
	let sessionShutdown: (() => void) | undefined;
	const statuses: Array<string | undefined> = [];
	const core = {
		async connect(): Promise<void> { undefined; },
		async flushOutbox(): Promise<void> { undefined; },
		async evaluateTimeouts(): Promise<void> { undefined; },
		async intakeInbox(this: Pick<TaskCore, "receive">) { return this.receive(); },
		async shipRequired() {},
		async repairAcknowledgements() {},
		async receive(): Promise<readonly []> {
			throw new TaskOutboxDeliveryError("TARGET_NOT_REGISTERED", "canonical event target is inactive", { retryable: false });
		},
		hasPendingWork: () => false,
	} as unknown as TaskCore;
	const context = {
		hasPendingMessages: (): boolean => false,
		sessionManager: { getEntries: (): readonly unknown[] => [] },
		ui: {
			setStatus: (_key: string, value: string | undefined): void => { statuses.push(value); },
			theme: { fg: (_color: string, text: string): string => text },
		},
	};
	registerAgentTaskTools({
		on(event: string, handler: unknown): void {
			if (event === "session_start") sessionStart = handler as (event: unknown, context: unknown) => Promise<unknown>;
			if (event === "session_shutdown") sessionShutdown = handler as () => void;
		},
		registerTool(): void { undefined; },
	} as unknown as ExtensionAPI, core);

	try {
		await sessionStart!({}, context);
		expect(statuses.at(-1)).toBe("tasks: outbox degraded");
	} finally {
		sessionShutdown?.();
	}
});

test("reports inbox delivery evidence failure as outbox degradation after advancing the event", async () => {
	let sessionStart: ((event: unknown, context: unknown) => Promise<unknown>) | undefined;
	let sessionShutdown: (() => void) | undefined;
	const statuses: Array<string | undefined> = [];
	const entries: unknown[] = [];
	const wakeOrder: string[] = [];
	let wakeRequests = 0;
	let wakeFlushes = 0;
	let acknowledgements = 0;
	const source = { relay: "memory", id: "receiver" };
	const target = { relay: "memory", id: "origin" };
	const delivery = {
		cursor: "1",
		envelope: {
			envelopeId: "event-envelope", protocolVersion: TASK_PROTOCOL_VERSION, source, target, taskId: "task-1", kind: TaskEnvelopeKind.canonicalEvent,
			payload: JSON.stringify({ eventId: "event-1", taskId: "task-1", type: "task.information", sequence: "2", source: target, target: source, occurredAt: 1, payload: { message: "peer reply" } }),
		},
	} as const;
	const core: TaskCore = {
		endpoint: source,
		async createTask() { return { taskId: "created-task" }; },
		getTask() { return undefined; },
		listTasks() { return []; },
		hasPendingWork() { return false; },
		onChange() { return () => undefined; },
		async connect(): Promise<void> { undefined; },
		async flushOutbox(): Promise<void> { undefined; },
		async evaluateTimeouts(): Promise<void> { undefined; },
		async intakeInbox(this: Pick<TaskCore, "receive">) { return this.receive(); },
		async shipRequired() {},
		async repairAcknowledgements() {},
		async receive() { return acknowledgements === 0 ? [{ kind: "relay" as const, delivery, order: 1 }] : []; },
		isSelfAuthoredConversation() { return false; },
		async recordInsertion(): Promise<void> { undefined; },
		async submitIntent(): Promise<void> { undefined; },
		recordWakeRequest(_input, work) {
			wakeRequests += 1;
			wakeOrder.push("request");
			// The common path's only receipt is the wake evidence; the ship phase finds its delivery blocked.
			return {
				async flush(): Promise<void> {
					wakeFlushes += 1; wakeOrder.push("flush");
					work!.shipments.push({ envelopeIds: [], shipped: true, failure: new TaskOutboxDeliveryError("DELIVERY_UNCONFIRMED", "peer delivery outcome is unknown", {
						retryable: false,
						details: { mayHaveBeenDelivered: true },
					}) });
				},
				discard(): void { undefined; },
			};
		},
		async acknowledgeRelayDelivery(): Promise<void> { acknowledgements += 1; },
		async acknowledgeParent(): Promise<void> { undefined; },
		completeLocalPresentation(): void { throw new Error("relay-only fixture has no local presentations"); },
	};
	const context = {
		isIdle: (): boolean => true,
		hasPendingMessages: (): boolean => false,
		sessionManager: { getEntries: (): readonly unknown[] => entries },
		ui: {
			setStatus: (_key: string, value: string | undefined): void => { statuses.push(value); },
			theme: { fg: (_color: string, text: string): string => text },
		},
	};
	registerAgentTaskTools({
		on(event: string, handler: unknown): void {
			if (event === "session_start") sessionStart = handler as (event: unknown, context: unknown) => Promise<unknown>;
			if (event === "session_shutdown") sessionShutdown = handler as () => void;
		},
		registerTool(): void { undefined; },
		sendMessage(message: { readonly customType: string; readonly details: unknown }): void {
			wakeOrder.push(message.customType === "pi-tasks-wake" ? "wake" : "event");
			entries.push({ type: "custom_message", customType: message.customType, details: message.details });
		},
		appendEntry(customType: string, data: unknown): void { entries.push({ type: "custom", customType, data }); },
	} as unknown as ExtensionAPI, core);

	try {
		await sessionStart!({}, context);
		expect(acknowledgements).toBe(1);
		expect(statuses.at(-1)).toBe("tasks: outbox degraded");
		expect(wakeRequests).toBe(1);
		expect(wakeFlushes).toBe(1);
		expect(wakeOrder).toEqual(["event", "request", "wake", "flush"]);
		expect(entries).toContainEqual(expect.objectContaining({ type: "custom_message", customType: "pi-tasks-event" }));
	} finally {
		sessionShutdown?.();
	}
});

test("does not mislabel relay failures during outbox delivery as degradation", async () => {
	let sessionStart: ((event: unknown, context: unknown) => Promise<unknown>) | undefined;
	let sessionShutdown: (() => void) | undefined;
	const statuses: Array<string | undefined> = [];
	const core = {
		async connect(): Promise<void> { undefined; },
		async flushOutbox(): Promise<void> {
			throw new TaskOutboxDeliveryError("RELAY_UNAVAILABLE", "relay is unavailable");
		},
		async evaluateTimeouts(): Promise<void> { undefined; },
		async intakeInbox(this: Pick<TaskCore, "receive">) { return this.receive(); },
		async shipRequired() {},
		async repairAcknowledgements() {},
		async receive(): Promise<readonly []> { return []; },
		hasPendingWork: () => false,
	} as unknown as TaskCore;
	const context = {
		hasPendingMessages: (): boolean => true,
		sessionManager: { getEntries: (): readonly unknown[] => [] },
		ui: {
			setStatus: (_key: string, value: string | undefined): void => { statuses.push(value); },
			theme: { fg: (_color: string, text: string): string => text },
		},
	};
	registerAgentTaskTools({
		on(event: string, handler: unknown): void {
			if (event === "session_start") sessionStart = handler as (event: unknown, context: unknown) => Promise<unknown>;
			if (event === "session_shutdown") sessionShutdown = handler as () => void;
		},
		registerTool(): void { undefined; },
	} as unknown as ExtensionAPI, core);

	try {
		await sessionStart!({}, context);
		expect(statuses.at(-1)).toBe("tasks: relay unavailable");
	} finally {
		sessionShutdown?.();
	}
});

test("names the class, code and relay operation of a capacity failure on send", async () => {
	let sessionStart: ((event: unknown, context: unknown) => Promise<unknown>) | undefined;
	let sessionShutdown: (() => void) | undefined;
	const statuses: Array<string | undefined> = [];
	const core = {
		async connect(): Promise<void> { undefined; },
		async flushOutbox(): Promise<void> {
			throw new TaskProtocolError("RELAY_CAPACITY", "relay is at capacity", { details: { operation: "send", retryAfterMs: 10 } });
		},
		async evaluateTimeouts(): Promise<void> { undefined; },
		async intakeInbox(this: Pick<TaskCore, "receive">) { return this.receive(); },
		async shipRequired() {},
		async repairAcknowledgements() {},
		async receive(): Promise<readonly []> { return []; },
		hasPendingWork: () => false,
	} as unknown as TaskCore;
	const context = {
		hasPendingMessages: (): boolean => true,
		sessionManager: { getEntries: (): readonly unknown[] => [] },
		ui: {
			setStatus: (_key: string, value: string | undefined): void => { statuses.push(value); },
			theme: { fg: (_color: string, text: string): string => text },
		},
	};
	registerAgentTaskTools({
		on(event: string, handler: unknown): void {
			if (event === "session_start") sessionStart = handler as (event: unknown, context: unknown) => Promise<unknown>;
			if (event === "session_shutdown") sessionShutdown = handler as () => void;
		},
		registerTool(): void { undefined; },
	} as unknown as ExtensionAPI, core);

	try {
		await sessionStart!({}, context);
		expect(statuses.at(-1)).toBe("tasks: capacity (RELAY_CAPACITY) on send");
	} finally {
		sessionShutdown?.();
	}
});

test("does not mislabel relay receive failures as outbox degradation", async () => {
	let sessionStart: ((event: unknown, context: unknown) => Promise<unknown>) | undefined;
	let sessionShutdown: (() => void) | undefined;
	const statuses: Array<string | undefined> = [];
	const core = {
		async connect(): Promise<void> { undefined; },
		async flushOutbox(): Promise<void> { undefined; },
		async evaluateTimeouts(): Promise<void> { undefined; },
		async intakeInbox(this: Pick<TaskCore, "receive">) { return this.receive(); },
		async shipRequired() {},
		async repairAcknowledgements() {},
		async receive(): Promise<readonly []> {
			throw new TaskProtocolError("TARGET_NOT_REGISTERED", "relay receive registration is absent", { retryable: false });
		},
		hasPendingWork: () => false,
	} as unknown as TaskCore;
	const context = {
		hasPendingMessages: (): boolean => false,
		sessionManager: { getEntries: (): readonly unknown[] => [] },
		ui: {
			setStatus: (_key: string, value: string | undefined): void => { statuses.push(value); },
			theme: { fg: (_color: string, text: string): string => text },
		},
	};
	registerAgentTaskTools({
		on(event: string, handler: unknown): void {
			if (event === "session_start") sessionStart = handler as (event: unknown, context: unknown) => Promise<unknown>;
			if (event === "session_shutdown") sessionShutdown = handler as () => void;
		},
		registerTool(): void { undefined; },
	} as unknown as ExtensionAPI, core);

	try {
		await sessionStart!({}, context);
		expect(statuses.at(-1)).toBe("tasks: relay unavailable");
	} finally {
		sessionShutdown?.();
	}
});

test("starts polling after rejected startup and clears the warning after autonomous recovery", async () => {
	let sessionStart: ((event: unknown, context: unknown) => Promise<unknown>) | undefined;
	let sessionShutdown: (() => void) | undefined;
	let attempts = 0;
	let recovered: (() => void) | undefined;
	const recovery = new Promise<void>((resolve) => { recovered = resolve; });
	const statuses: Array<string | undefined> = [];
	jest.useFakeTimers();
	const healthyCore = {
		async connect(): Promise<void> { undefined; },
		async flushOutbox(): Promise<void> { undefined; },
		async evaluateTimeouts(): Promise<void> { undefined; },
		async intakeInbox(this: Pick<TaskCore, "receive">) { return this.receive(); },
		async shipRequired() {},
		async repairAcknowledgements() {},
		async receive(): Promise<readonly []> { return []; },
		hasPendingWork: () => false,
	} as unknown as TaskCore;
	const context = {
		hasPendingMessages: (): boolean => false,
		sessionManager: { getEntries: (): readonly unknown[] => [] },
		ui: {
			setStatus: (_key: string, value: string | undefined): void => {
				statuses.push(value);
				if (value === undefined) recovered?.();
			},
			theme: { fg: (_color: string, text: string): string => text },
		},
	};

	try {
		registerAgentTaskTools({
			on(event: string, handler: unknown): void {
				if (event === "session_start") sessionStart = handler as (event: unknown, context: unknown) => Promise<unknown>;
				if (event === "session_shutdown") sessionShutdown = handler as () => void;
			},
			registerTool(): void { undefined; },
		} as unknown as ExtensionAPI, undefined, async () => {
			attempts += 1;
			if (attempts === 1) throw new Error("transient relay failure");
			return healthyCore;
		});

		await sessionStart!({}, context);
		expect(statuses).toEqual(["tasks: relay unavailable"]);
		jest.advanceTimersByTime(IDLE_POLL_MS); // core load failed, so the idle cadence applies
		await recovery;
		await Promise.resolve();
		expect(attempts).toBe(2);
		expect(statuses.at(-1)).toBeUndefined();
	} finally {
		sessionShutdown?.();
		jest.useRealTimers();
	}
});

test("renews registration and receives real completion while pending Pi messages defer presentation", async () => {
	let sessionStart: ((event: unknown, context: unknown) => Promise<unknown>) | undefined;
	let sessionShutdown: (() => void) | undefined;
	let statusUpdates = 0;
	let polled!: () => void;
	const pollCompleted = new Promise<void>((resolve) => { polled = resolve; });
	jest.useFakeTimers();
	const relay = createInMemoryTaskRelay("memory");
	const parentStore = createTaskStore(), childStore = createTaskStore();
	const parent = createTaskCore({ endpoint: { relay: "memory", id: "parent" }, relay, store: parentStore });
	const child = createTaskCore({ endpoint: { relay: "memory", id: "child" }, relay, store: childStore });
	const connects = spyOn(parent, "connect");
	const entries: unknown[] = [];
	const statuses: Array<string | undefined> = [];
	const context = {
		isIdle: (): boolean => false,
		hasPendingMessages: (): boolean => true,
		sessionManager: { getEntries: (): readonly unknown[] => entries },
		ui: { setStatus(_key: string, status: string | undefined): void { statuses.push(status); if (++statusUpdates === 2) polled(); }, theme: { fg: (_color: string, text: string): string => text } },
	};

	try {
		await parent.connect(); await child.connect();
		const task = await parent.createTask({ target: child.endpoint, task: "finish during queued follow-ups", timeoutMs: 60_000 });
		await child.receive();
		connects.mockClear();
		registerAgentTaskTools({
			on(event: string, handler: unknown): void {
				if (event === "session_start") sessionStart = handler as (event: unknown, context: unknown) => Promise<unknown>;
				if (event === "session_shutdown") sessionShutdown = handler as () => void;
			},
			registerTool(): void { undefined; },
			sendMessage(message: unknown): void { entries.push(message); },
			appendEntry(customType: string, data: unknown): void { entries.push({ customType, data }); },
		} as unknown as ExtensionAPI, parent);

		await sessionStart!({}, context);
		await child.submitIntent({ taskId: task.taskId, type: "task.completed", payload: { summary: "done" } });
		jest.advanceTimersByTime(ACTIVE_POLL_MS); // the parent still holds an active task
		await pollCompleted;
		expect(connects).toHaveBeenCalledTimes(2);
		expect(statuses).toEqual([undefined, undefined]);
		expect(parent.getTask(task.taskId)?.status).toBe("completed");
		expect(parent.getTask(task.taskId)?.events.filter(event => event.type === "task.completed")).toHaveLength(1);
		expect(entries).toEqual([]);
	} finally {
		sessionShutdown?.();
		connects.mockRestore();
		jest.useRealTimers();
		parentStore.close(); childStore.close();
	}
});

test.each(["shutdown", "context replacement"] as const)("fences real held assignment presentation and ACK during %s", async (replacement) => {
	let sessionStart!: (event: unknown, context: unknown) => Promise<unknown>;
	let agentEnd!: (event: unknown, context: unknown) => Promise<unknown>;
	let agentSettled!: (event: unknown, context: unknown) => Promise<unknown>;
	let sessionShutdown!: () => Promise<unknown>;
	const relay = createInMemoryTaskRelay("memory");
	const parentStore = createTaskStore(), childStore = createTaskStore();
	const parent = createTaskCore({ endpoint: { relay: "memory", id: "parent" }, relay, store: parentStore });
	const child = createTaskCore({ endpoint: { relay: "memory", id: "child" }, relay, store: childStore });
	const entries: unknown[] = [];
	let successorInsertion = false;
	const ui = { setStatus(): void { undefined; }, theme: { fg: (_color: string, text: string): string => text } };
	const successor = { isIdle: (): boolean => true, hasPendingMessages: (): boolean => false, sessionManager: { getEntries: (): readonly unknown[] => entries }, ui };
	let replacementStarted = false;
	let replacing: Promise<unknown> | undefined;
	const prior = {
		isIdle: (): boolean => true,
		hasPendingMessages(): boolean {
			if (!replacementStarted) {
				replacementStarted = true;
				replacing = replacement === "shutdown" ? sessionShutdown() : agentEnd({}, successor);
			}
			return false;
		},
		sessionManager: { getEntries: (): readonly unknown[] => [] }, ui,
	};
	registerAgentTaskTools({
		on(event: string, handler: unknown): void {
			if (event === "session_start") sessionStart = handler as typeof sessionStart;
			if (event === "agent_end") agentEnd = handler as typeof agentEnd;
			if (event === "agent_settled") agentSettled = handler as typeof agentSettled;
			if (event === "session_shutdown") sessionShutdown = handler as typeof sessionShutdown;
		},
		registerTool(): void { undefined; },
		sendMessage(message: { readonly customType: string; readonly details: unknown }): void { if (!successorInsertion) throw new Error("stale Pi insertion"); entries.push({ type: "custom_message", ...message }); },
		appendEntry(customType: string, data: unknown): void { if (!successorInsertion) throw new Error("stale Pi archive/ACK"); entries.push({ type: "custom", customType, data }); },
	} as unknown as ExtensionAPI, child);
	try {
		await parent.connect(); await child.connect();
		const created = await parent.createTask({ target: child.endpoint, task: "hold across lifecycle replacement", timeoutMs: 60_000 });
		await sessionStart({}, prior);
		await replacing;
		expect(replacementStarted).toBe(true);
		expect(entries).toEqual([]);
		expect(child.getTask(created.taskId)?.status).toBe("active");
		expect(childStore.getReceiveCursor()).toBe("0");
		expect((await relay.receive({ endpoint: child.endpoint, cursor: "0", limit: 100 })).deliveries).toHaveLength(1);
		successorInsertion = true;
		if (replacement === "shutdown") await sessionStart({}, successor);
		else await agentSettled({}, successor);
		expect(entries).toEqual(expect.arrayContaining([expect.objectContaining({ type: "custom_message", customType: "pi-tasks-event" })]));
		expect(childStore.getReceiveCursor()).not.toBe("0");
	} finally { await sessionShutdown(); parentStore.close(); childStore.close(); }
});

test("continues autonomous polling after Pi replaces the extension context at agent_end", async () => {
	let sessionStart: ((event: unknown, context: unknown) => Promise<unknown>) | undefined;
	let agentEnd: ((event: unknown, context: unknown) => Promise<unknown>) | undefined;
	let sessionShutdown: (() => void) | undefined;
	let refreshes = 0;
	let polled: (() => void) | undefined;
	const pollCompleted = new Promise<void>((resolve) => { polled = resolve; });
	jest.useFakeTimers();
	const core = {
		async connect(): Promise<void> { undefined; },
		async flushOutbox(): Promise<void> {
			refreshes += 1;
			if (refreshes === 3) polled?.();
		},
		async evaluateTimeouts(): Promise<void> { undefined; },
		async intakeInbox(this: Pick<TaskCore, "receive">) { return this.receive(); },
		async shipRequired() {},
		async repairAcknowledgements() {},
		async receive(): Promise<readonly []> { return []; },
		hasPendingWork: () => false,
	} as unknown as TaskCore;
	const context = () => ({
		hasPendingMessages: (): boolean => false,
		sessionManager: { getEntries: (): readonly unknown[] => [] },
		ui: { setStatus: (): void => undefined, theme: { fg: (_color: string, text: string): string => text } },
	});

	try {
		registerAgentTaskTools({
			on(event: string, handler: unknown): void {
				if (event === "session_start") sessionStart = handler as (event: unknown, context: unknown) => Promise<unknown>;
				if (event === "agent_end") agentEnd = handler as (event: unknown, context: unknown) => Promise<unknown>;
				if (event === "session_shutdown") sessionShutdown = handler as () => void;
			},
			registerTool(): void { undefined; },
		} as unknown as ExtensionAPI, core);

		await sessionStart!({}, context());
		await agentEnd!({}, context());
		jest.advanceTimersByTime(IDLE_POLL_MS);
		await Promise.race([pollCompleted, Bun.sleep(100).then(() => { throw new Error("background poll did not refresh"); })]);
		expect(refreshes).toBe(3);
	} finally {
		sessionShutdown?.();
		jest.useRealTimers();
	}
});

test("keeps missing idle insertion evidence unacknowledged and retryable after pending delivery clears", async () => {
	let sessionStart: ((event: unknown, context: unknown) => Promise<unknown>) | undefined;
	let agentSettled: ((event: unknown, context: unknown) => Promise<unknown>) | undefined;
	let sessionShutdown: (() => void) | undefined;
	let pending = true;
	let insertionAttempts = 0;
	let recordedInsertions = 0;
	let acknowledgements = 0;
	let wakes = 0;
	const deliveryEvidence = new Set<string>();
	const source = { relay: "memory", id: "parent" };
	const target = { relay: "memory", id: "receiver" };
	const delivery = {
		cursor: "1",
		envelope: {
			envelopeId: "event-envelope", protocolVersion: TASK_PROTOCOL_VERSION, source, target, taskId: "task-1", kind: TaskEnvelopeKind.canonicalEvent,
			payload: JSON.stringify({ eventId: "event-1", taskId: "task-1", type: "task.created", sequence: "1", source, target, occurredAt: 1, payload: { task: "pending persistence" } }),
		},
	} as const;
	const core = {
		async connect(): Promise<void> { undefined; },
		async flushOutbox(): Promise<void> { undefined; },
		async evaluateTimeouts(): Promise<void> { undefined; },
		async intakeInbox(this: Pick<TaskCore, "receive">) { return this.receive(); },
		async shipRequired() {},
		async repairAcknowledgements() {},
		async receive() { return [{ kind: "relay" as const, delivery, order: 1 }]; },
		isSelfAuthoredConversation() { return false; },
		async submitIntent(input: { readonly payload: { readonly stage: string; readonly state: string } }): Promise<void> { deliveryEvidence.add(`${input.payload.stage}:${input.payload.state}`); },
		async recordInsertion(): Promise<void> { recordedInsertions += 1; },
		async acknowledgeRelayDelivery(): Promise<void> { acknowledgements += 1; },
		hasPendingWork: () => false,
	} as unknown as TaskCore;
	const context = {
		isIdle: (): boolean => true,
		hasPendingMessages: (): boolean => pending,
		sessionManager: { getEntries: (): readonly unknown[] => [] },
		ui: { setStatus: (): void => undefined, theme: { fg: (_color: string, text: string): string => text } },
	};
	registerAgentTaskTools({
		on(event: string, handler: unknown): void {
			if (event === "session_start") sessionStart = handler as (event: unknown, context: unknown) => Promise<unknown>;
			if (event === "agent_settled") agentSettled = handler as (event: unknown, context: unknown) => Promise<unknown>;
			if (event === "session_shutdown") sessionShutdown = handler as () => void;
		},
		registerTool(): void { undefined; },
		sendMessage(message: { readonly customType: string }): void {
			if (message.customType === "pi-tasks-event") insertionAttempts += 1;
			if (message.customType === "pi-tasks-wake") wakes += 1;
		},
	} as unknown as ExtensionAPI, core);

	try {
		await sessionStart!({}, context);
		expect(insertionAttempts).toBe(0);

		pending = false;
		await agentSettled!({}, context);
		await agentSettled!({}, context);
		expect(insertionAttempts).toBe(2);
		expect(recordedInsertions).toBe(0);
		expect(acknowledgements).toBe(0);
		expect(wakes).toBe(0);
		expect([...deliveryEvidence]).toEqual(["receiver_recorded:confirmed", "pi_insertion:blocked"]);
	} finally {
		sessionShutdown?.();
	}
});

test("persists an idle task event before sending one separate wake", async () => {
	let sessionStart: ((event: unknown, context: unknown) => Promise<unknown>) | undefined;
	let agentSettled: ((event: unknown, context: unknown) => Promise<unknown>) | undefined;
	let sessionShutdown: (() => void) | undefined;
	let idle = false;
	let acknowledgements = 0;
	let recordedInsertions = 0;
	let releaseWakeFlush!: () => void;
	let signalWakeFlush!: () => void;
	const wakeFlush = new Promise<void>((resolve) => { releaseWakeFlush = resolve; });
	const wakeFlushObserved = new Promise<void>((resolve) => { signalWakeFlush = resolve; });
	const wakeOrder: string[] = [];
	const deliveryEvidence = new Set<string>();
	const entries: unknown[] = [];
	const sent: Array<{ readonly customType: string; readonly triggerTurn: boolean | undefined }> = [];
	const source = { relay: "memory", id: "parent" };
	const target = { relay: "memory", id: "receiver" };
	const delivery = {
		cursor: "1",
		envelope: {
			envelopeId: "assignment-envelope", protocolVersion: TASK_PROTOCOL_VERSION, source, target, taskId: "task-1", kind: TaskEnvelopeKind.canonicalEvent,
			payload: JSON.stringify({ eventId: "event-1", taskId: "task-1", type: "task.created", sequence: "1", source, target, occurredAt: 1, payload: { task: "implement safely" } }),
		},
	} as const;
	const core: TaskCore = {
		endpoint: target,
		async createTask() { return { taskId: "created-task" }; },
		getTask() { return undefined; },
		listTasks() { return []; },
		hasPendingWork() { return false; },
		onChange() { return () => undefined; },
		async connect(): Promise<void> { undefined; },
		async flushOutbox(): Promise<void> { undefined; },
		async evaluateTimeouts(): Promise<void> { undefined; },
		async intakeInbox(this: Pick<TaskCore, "receive">) { return this.receive(); },
		async shipRequired() {},
		async repairAcknowledgements() {},
		async receive() { return acknowledgements === 0 ? [{ kind: "relay" as const, delivery, order: 1 }] : []; },
		isSelfAuthoredConversation() { return false; },
		async submitIntent(input): Promise<void> {
			const { stage, state } = input.payload;
			if (typeof stage === "string" && typeof state === "string") deliveryEvidence.add(`${stage}:${state}`);
		},
		async recordInsertion(): Promise<void> { recordedInsertions = 1; },
		recordWakeRequest() {
			deliveryEvidence.add("wake_accepted:confirmed");
			wakeOrder.push("request");
			return {
				flush: async (): Promise<void> => {
					wakeOrder.push("flush");
					signalWakeFlush();
					await wakeFlush;
				},
				discard: (): void => { deliveryEvidence.delete("wake_accepted:confirmed"); },
			};
		},
		async acknowledgeRelayDelivery(): Promise<void> { acknowledgements += 1; },
		async acknowledgeParent(): Promise<void> { undefined; },
		completeLocalPresentation(): void { throw new Error("relay-only fixture has no local presentations"); },
	};
	const context = {
		isIdle: (): boolean => idle,
		hasPendingMessages: (): boolean => false,
		sessionManager: { getEntries: (): readonly unknown[] => entries },
		ui: { setStatus: (): void => undefined, theme: { fg: (_color: string, text: string): string => text } },
	};
	registerAgentTaskTools({
		on(event: string, handler: unknown): void {
			if (event === "session_start") sessionStart = handler as (event: unknown, context: unknown) => Promise<unknown>;
			if (event === "agent_settled") agentSettled = handler as (event: unknown, context: unknown) => Promise<unknown>;
			if (event === "session_shutdown") sessionShutdown = handler as () => void;
		},
		registerTool(): void { undefined; },
		sendMessage(message: { readonly customType: string; readonly details?: unknown }, options?: { readonly triggerTurn?: boolean }): void {
			sent.push({ customType: message.customType, triggerTurn: options?.triggerTurn });
			if (message.customType === "pi-tasks-event") entries.push({ type: "custom_message", customType: message.customType, details: message.details });
			if (message.customType === "pi-tasks-wake") { wakeOrder.push("wake"); entries.push({ type: "custom_message", customType: message.customType, details: message.details }); }
		},
		appendEntry(customType: string, data: unknown): void { entries.push({ type: "custom", customType, data }); },
	} as unknown as ExtensionAPI, core);

	try {
		await sessionStart!({}, context);
		expect(sent).toEqual([]);
		expect(acknowledgements).toBe(0);

		idle = true;
		const settled = agentSettled!({}, context);
		await wakeFlushObserved;
		expect(sent).toEqual([
			{ customType: "pi-tasks-event", triggerTurn: false },
			{ customType: "pi-tasks-wake", triggerTurn: true },
		]);
		expect(entries.filter((entry) => typeof entry === "object" && entry !== null && "customType" in entry && entry.customType === "pi-tasks-event")).toHaveLength(1);
		expect(recordedInsertions).toBe(0); // The wake evidence implies the insertion.
		expect(wakeOrder).toEqual(["request", "wake", "flush"]);
		expect(acknowledgements).toBe(0);

		releaseWakeFlush();
		await settled;
		expect(sent).toHaveLength(2);
		expect(recordedInsertions).toBe(0);
		expect(acknowledgements).toBe(1);
		expect([...deliveryEvidence]).toEqual(["receiver_recorded:confirmed", "wake_accepted:confirmed"]);
	} finally {
		sessionShutdown?.();
	}
});

test("does not flush or acknowledge a captured wake request after session shutdown replaces its lifecycle", async () => {
	let sessionStart: ((event: unknown, context: unknown) => Promise<unknown>) | undefined;
	let sessionShutdown: (() => Promise<unknown>) | undefined;
	let acknowledgements = 0;
	let wakeRequests = 0;
	let wakeFlushes = 0;
	const entries: unknown[] = [];
	const source = { relay: "memory", id: "parent" };
	const target = { relay: "memory", id: "receiver" };
	const delivery = {
		cursor: "1",
		envelope: {
			envelopeId: "assignment-envelope", protocolVersion: TASK_PROTOCOL_VERSION, source, target, taskId: "task-1", kind: TaskEnvelopeKind.canonicalEvent,
			payload: JSON.stringify({ eventId: "event-1", taskId: "task-1", type: "task.created", sequence: "1", source, target, occurredAt: 1, payload: { task: "stop before relay" } }),
		},
	} as const;
	const core: TaskCore = {
		endpoint: target,
		async createTask() { return { taskId: "created-task" }; },
		getTask() { return undefined; },
		listTasks() { return []; },
		hasPendingWork() { return false; },
		onChange() { return () => undefined; },
		async connect(): Promise<void> { undefined; },
		async flushOutbox(): Promise<void> { undefined; },
		async evaluateTimeouts(): Promise<void> { undefined; },
		async intakeInbox(this: Pick<TaskCore, "receive">) { return this.receive(); },
		async shipRequired() {},
		async repairAcknowledgements() {},
		async receive() { return acknowledgements === 0 ? [{ kind: "relay" as const, delivery, order: 1 }] : []; },
		isSelfAuthoredConversation() { return false; },
		async recordInsertion(): Promise<void> { undefined; },
		recordWakeRequest() {
			wakeRequests += 1;
			return { async flush(): Promise<void> { wakeFlushes += 1; }, discard(): void { undefined; } };
		},
		async submitIntent(): Promise<void> { undefined; },
		async acknowledgeRelayDelivery(): Promise<void> { acknowledgements += 1; },
		async acknowledgeParent(): Promise<void> { undefined; },
		completeLocalPresentation(): void { throw new Error("relay-only fixture has no local presentations"); },
	};
	const context = {
		isIdle: (): boolean => true,
		hasPendingMessages: (): boolean => false,
		sessionManager: { getEntries: (): readonly unknown[] => entries },
		ui: { setStatus: (): void => undefined, theme: { fg: (_color: string, text: string): string => text } },
	};
	registerAgentTaskTools({
		on(event: string, handler: unknown): void {
			if (event === "session_start") sessionStart = handler as (event: unknown, context: unknown) => Promise<unknown>;
			if (event === "session_shutdown") sessionShutdown = handler as () => Promise<unknown>;
		},
		registerTool(): void { undefined; },
		sendMessage(message: { readonly customType: string; readonly details?: unknown }): void {
			entries.push({ type: "custom_message", customType: message.customType, details: message.details });
			if (message.customType === "pi-tasks-wake") void sessionShutdown?.();
		},
		appendEntry(customType: string, data: unknown): void { entries.push({ type: "custom", customType, data }); },
	} as unknown as ExtensionAPI, core);

	await sessionStart!({}, context);
	try {
		expect(wakeRequests).toBe(1);
		expect(wakeFlushes).toBe(0);
		expect(acknowledgements).toBe(0);
	} finally {
		await sessionShutdown?.();
	}
});

test("invalidates a pending lifecycle poll when the session shuts down", async () => {
	let sessionStart: ((event: unknown, context: unknown) => Promise<unknown>) | undefined;
	let agentEnd: ((event: unknown, context: unknown) => Promise<unknown>) | undefined;
	let sessionShutdown: (() => void) | undefined;
	let receiveCalls = 0;
	let releasePoll: (() => void) | undefined;
	let pollStarted: (() => void) | undefined;
	const started = new Promise<void>((resolve) => { pollStarted = resolve; });
	const pendingPoll = new Promise<void>((resolve) => { releasePoll = resolve; });
	const statuses: Array<string | undefined> = [];
	let deliveries = 0;
	jest.useFakeTimers();
	const source = { relay: "memory", id: "parent" };
	const target = { relay: "memory", id: "receiver" };
	const core = {
		async connect(): Promise<void> { undefined; },
		async flushOutbox(): Promise<void> { undefined; },
		async evaluateTimeouts(): Promise<void> { undefined; },
		async intakeInbox(this: Pick<TaskCore, "receive">) { return this.receive(); },
		async shipRequired() {},
		async repairAcknowledgements() {},
		async receive() {
			receiveCalls += 1;
			if (receiveCalls === 1) return [];
			pollStarted?.();
			await pendingPoll;
			return [{ kind: "relay", order: 1, delivery: {
				cursor: "1",
				envelope: {
					envelopeId: "late-event", protocolVersion: TASK_PROTOCOL_VERSION, source, target, taskId: "task-1", kind: TaskEnvelopeKind.canonicalEvent,
					payload: JSON.stringify({ eventId: "event-1", taskId: "task-1", type: "task.information", sequence: "1", source, target, occurredAt: 1, payload: { message: "late" } }),
				},
			} }] as const;
		},
		hasPendingWork: () => false,
	} as unknown as TaskCore;
	const context = {
		hasPendingMessages: (): boolean => false,
		sessionManager: { getEntries: (): readonly unknown[] => [] },
		ui: {
			setStatus: (_key: string, value: string | undefined): void => { statuses.push(value); },
			theme: { fg: (_color: string, text: string): string => text },
		},
	};

	try {
		registerAgentTaskTools({
			on(event: string, handler: unknown): void {
				if (event === "session_start") sessionStart = handler as (event: unknown, context: unknown) => Promise<unknown>;
				if (event === "agent_end") agentEnd = handler as (event: unknown, context: unknown) => Promise<unknown>;
				if (event === "session_shutdown") sessionShutdown = handler as () => void;
			},
			registerTool(): void { undefined; },
			sendMessage(): void { deliveries += 1; },
		} as unknown as ExtensionAPI, core);

		await sessionStart!({}, context);
		const statusesBeforeShutdown = [...statuses];
		jest.advanceTimersByTime(IDLE_POLL_MS);
		await started;
		const joinedRefresh = agentEnd!({}, context);
		sessionShutdown?.();
		releasePoll?.();
		await expect(joinedRefresh).resolves.toBeUndefined();
		expect(deliveries).toBe(0);
		expect(statuses).toEqual(statusesBeforeShutdown);
	} finally {
		sessionShutdown?.();
		jest.useRealTimers();
	}
});

test("contains agent_end relay failures and leaves the unavailable warning", async () => {
	let sessionStart: ((event: unknown, context: unknown) => Promise<unknown>) | undefined;
	let agentEnd: ((event: unknown, context: unknown) => Promise<unknown>) | undefined;
	let sessionShutdown: (() => void) | undefined;
	const statuses: Array<string | undefined> = [];
	const context = {
		hasPendingMessages: (): boolean => false,
		sessionManager: { getEntries: (): readonly unknown[] => [] },
		ui: {
			setStatus: (_key: string, value: string | undefined): void => { statuses.push(value); },
			theme: { fg: (_color: string, text: string): string => text },
		},
	};
	registerAgentTaskTools({
		on(event: string, handler: unknown): void {
			if (event === "session_start") sessionStart = handler as (event: unknown, context: unknown) => Promise<unknown>;
			if (event === "agent_end") agentEnd = handler as (event: unknown, context: unknown) => Promise<unknown>;
			if (event === "session_shutdown") sessionShutdown = handler as () => void;
		},
		registerTool(): void { undefined; },
	} as unknown as ExtensionAPI, undefined, async () => { throw new Error("relay unavailable"); });

	try {
		await sessionStart!({}, context);
		await expect(agentEnd!({}, context)).resolves.toBeUndefined();
		expect(statuses.at(-1)).toBe("tasks: relay unavailable");
	} finally {
		sessionShutdown?.();
	}
});

test.each(["RELAY_RESET", "RELAY_REBIND_REQUIRED"] as const)("automatically replaces a relay-loss lifetime and records active tasks as locally lost: %s", async (relayLossCode) => {
	let sessionStart: ((event: unknown, context: unknown) => Promise<unknown>) | undefined;
	let agentSettled: ((event: unknown, context: unknown) => Promise<unknown>) | undefined;
	let sessionShutdown: (() => Promise<void>) | undefined;
	let oldConnects = 0;
	let oldFlushes = 0;
	let oldEvaluations = 0;
	let oldReceives = 0;
	let oldCloses = 0;
	let oldClosing: Promise<void> | undefined;
	let freshCreates = 0;
	let freshConnectionError: TaskProtocolError | undefined;
	const factoryModes: boolean[] = [];
	const tools: Record<string, Tool> = {};
	const messages: Array<{ readonly message: Record<string, unknown>; readonly options: Record<string, unknown> }> = [];
	const notifications: Array<{ readonly message: string; readonly level: string }> = [];
	const statuses: Array<string | undefined> = [];
	const oldCore = {
		endpoint: { relay: "wolfpack-pi-tasks-v2", id: "old-endpoint" },
		async connect(): Promise<void> {
			oldConnects += 1;
			if (oldConnects > 1 && relayLossCode === "RELAY_RESET") throw new TaskProtocolError(relayLossCode, "relay lifetime was replaced", { retryable: false });
		},
		async flushOutbox(): Promise<void> {
			oldFlushes += 1;
			if (oldFlushes > 1 && relayLossCode === "RELAY_REBIND_REQUIRED") throw new TaskProtocolError(relayLossCode, "relay lifetime was replaced", { retryable: false });
		},
		async evaluateTimeouts(): Promise<void> { oldEvaluations += 1; },
		async intakeInbox(this: Pick<TaskCore, "receive">) { return this.receive(); },
		async shipRequired() {},
		async repairAcknowledgements() {},
		async receive(): Promise<readonly []> { oldReceives += 1; return []; },
		listTasks: () => [
			{ taskId: "active-task", status: "active" },
			{ taskId: "completed-task", status: "completed" },
		],
		close(): Promise<void> { if (!oldClosing) { oldCloses += 1; oldClosing = Promise.resolve(); } return oldClosing; },
		hasPendingWork: () => true,
	} as unknown as TaskCore & { close(): Promise<void> };
	const freshCore = {
		endpoint: { relay: "wolfpack-pi-tasks-v2", id: "fresh-endpoint" },
		async connect(): Promise<void> { if (freshConnectionError) throw freshConnectionError; },
		async flushOutbox(): Promise<void> { undefined; },
		async evaluateTimeouts(): Promise<void> { undefined; },
		async intakeInbox(this: Pick<TaskCore, "receive">) { return this.receive(); },
		async shipRequired() {},
		async repairAcknowledgements() {},
		async receive(): Promise<readonly []> { return []; },
		listTasks: () => [],
		getTask: () => undefined,
		async createTask(): Promise<{ readonly taskId: string }> { freshCreates += 1; return { taskId: "fresh-task" }; },
		async close(): Promise<void> { undefined; },
		hasPendingWork: () => false,
	} as unknown as TaskCore & { close(): Promise<void> };
	const context = {
		isIdle: (): boolean => true,
		hasPendingMessages: (): boolean => false,
		sessionManager: { getEntries: (): readonly unknown[] => [] },
		ui: {
			setStatus: (_key: string, value: string | undefined): void => { statuses.push(value); },
			theme: { fg: (_color: string, text: string): string => text },
			notify: (message: string, level: string): void => { notifications.push({ message, level }); },
		},
	};
	registerAgentTaskTools({
		on(event: string, handler: unknown): void {
			if (event === "session_start") sessionStart = handler as (event: unknown, context: unknown) => Promise<unknown>;
			if (event === "agent_settled") agentSettled = handler as (event: unknown, context: unknown) => Promise<unknown>;
			if (event === "session_shutdown") sessionShutdown = handler as () => Promise<void>;
		},
		registerTool(tool: unknown): void { const value = tool as Tool; tools[value.name] = value; },
		sendMessage(message: Record<string, unknown>, options: Record<string, unknown>): void { messages.push({ message, options }); },
	} as unknown as ExtensionAPI, undefined, async (_signal, rebind) => {
		factoryModes.push(rebind === true);
		if (factoryModes.length === 1) return oldCore;
		if (factoryModes.length === 2) throw new TaskProtocolError("RELAY_UNAVAILABLE", "fresh registration unavailable");
		return freshCore;
	});

	try {
		await sessionStart!({}, context);
		await agentSettled!({}, context);

		expect(factoryModes).toEqual([false, true]);
		expect(oldEvaluations).toBe(relayLossCode === "RELAY_RESET" ? 1 : 2);
		expect(oldReceives).toBe(relayLossCode === "RELAY_RESET" ? 1 : 2);
		expect(oldCloses).toBe(1);
		expect(messages).toEqual([{
			message: {
				customType: "pi-tasks-relay-loss",
				content: expect.stringContaining("## task failed locally\ntask: `active-task`\n\nRelay state was lost; the remote outcome is unknown."),
				display: true,
				details: { taskId: "active-task", status: "failed", error: { code: "RELAY_STATE_LOST", message: "relay state was lost; remote outcome is unknown", retryable: false }, remoteOutcome: "unknown" },
			},
			options: { triggerTurn: false },
		}]);
		expect(notifications).toEqual([]);
		expect(statuses.at(-1)).toBe("tasks: relay unavailable");

		await agentSettled!({}, context);
		expect(factoryModes).toEqual([false, true, false]);
		expect(messages).toHaveLength(1);
		expect(notifications).toEqual([{ message: "Relay state was lost; automatically bound a fresh endpoint. 1 active task was marked locally failed with unknown remote outcome.", level: "warning" }]);
		expect(statuses.at(-1)).toBeUndefined();
		const historical = await tools.agent_task_status!.execute("status", { taskId: "active-task" }, new AbortController().signal, undefined, context);
		expect((historical.details as { readonly error: { readonly code: string } }).error.code).toBe("UNKNOWN_TASK");

		const sent = await tools.agent_task_send!.execute("fresh", { to: freshCore.endpoint, task: "new work" }, new AbortController().signal, undefined, context);
		expect(sent.details).toEqual({ taskId: "fresh-task" });
		expect(freshCreates).toBe(1);

		freshConnectionError = new TaskProtocolError("RELAY_AUTH_REQUIRED", "authentication required", { retryable: false });
		await agentSettled!({}, context);
		expect(factoryModes).toEqual([false, true, false]);
		expect(messages).toHaveLength(1);
		expect(notifications).toHaveLength(1);
		expect(statuses.at(-1)).toBe("tasks: Wolfpack authentication required");
	} finally {
		await sessionShutdown?.();
	}
});

test("retries a rejected configured core during a later lifecycle inbox refresh", async () => {
	let sessionStart: ((event: unknown, context: unknown) => Promise<unknown>) | undefined;
	let agentEnd: ((event: unknown, context: unknown) => Promise<unknown>) | undefined;
	let attempts = 0;
	const calls: string[] = [];
	const healthyCore = {
		async connect(): Promise<void> { calls.push("connect"); },
		async flushOutbox(): Promise<void> { calls.push("flush"); },
		async evaluateTimeouts(): Promise<void> { calls.push("timeout"); },
		async intakeInbox(this: Pick<TaskCore, "receive">) { return this.receive(); },
		async shipRequired() {},
		async repairAcknowledgements() {},
		async receive(): Promise<readonly []> { calls.push("receive"); return []; },
		hasPendingWork: () => false,
	} as unknown as TaskCore;
	const context = {
		hasPendingMessages: (): boolean => false,
		sessionManager: { getEntries: (): readonly unknown[] => [] },
		ui: { setStatus: (): void => undefined, theme: { fg: (_color: string, text: string): string => text } },
	};
	registerAgentTaskTools({
		on(event: string, handler: unknown): void {
			if (event === "session_start") sessionStart = handler as (event: unknown, context: unknown) => Promise<unknown>;
			if (event === "agent_end") agentEnd = handler as (event: unknown, context: unknown) => Promise<unknown>;
		},
		registerTool(): void { undefined; },
	} as unknown as ExtensionAPI, undefined, async () => {
		attempts += 1;
		if (attempts === 1) throw new Error("transient relay failure");
		return healthyCore;
	});

	await sessionStart!({}, context);
	await agentEnd!({}, context);

	expect(attempts).toBe(2);
	expect(calls).toEqual(["connect", "timeout", "receive", "flush"]);
});

test("clears a rejected configured core before retrying it", async () => {
	let attempts = 0;
	const core = {} as TaskCore;
	const configuredCore = createConfiguredCoreLoader(async () => {
		attempts += 1;
		if (attempts === 1) throw new Error("transient relay failure");
		return core;
	});

	await expect(configuredCore()).rejects.toThrow("transient relay failure");
	await expect(configuredCore()).resolves.toBe(core);
	expect(attempts).toBe(2);
});

test("serializes concurrent inbox refreshes and accepts a new refresh only after the prior lifecycle completes", async () => {
	let calls = 0;
	let release: (() => void) | undefined;
	const refresh = createSingleFlightInboxRefresh(async () => {
		calls += 1;
		await new Promise<void>((resolve) => { release = resolve; });
	});

	const first = refresh();
	const second = refresh();
	expect(second).toBe(first);
	expect(calls).toBe(1);
	release?.();
	await first;

	const third = refresh();
	expect(third).not.toBe(first);
	expect(calls).toBe(2);
	release?.();
	await third;
});
