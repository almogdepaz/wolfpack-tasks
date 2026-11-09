import { expect, test } from "bun:test";
import { SessionManager } from "@earendil-works/pi-coding-agent";
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { openTaskJournal } from "../src/task-journal";
import { createTaskStore } from "../src/task-store";
import { createTaskCore } from "../src/task-core";
import { createInMemoryTaskRelay } from "../src/in-memory-task-relay";
import { deliverTaskInbox } from "../src/task-inbox";

test("real durable Pi entries reconcile after journal reopen without another assignment insertion or wake", async () => {
  const root = mkdtempSync("/tmp/pi-recovery-evidence-"), owner = { sessionId: crypto.randomUUID(), url: "http://127.0.0.1:1" };
  const path = join(root, owner.sessionId, "tasks.sqlite"), sessions = join(root, "sessions");
  const open = () => createTaskStore({}, openTaskJournal(path, owner)); let store = open();
  const originStore = createTaskStore(), relay = createInMemoryTaskRelay("fixture"), target = { relay: relay.id, id: "target" };
  const origin = createTaskCore({ endpoint: { relay: relay.id, id: "source" }, relay, store: originStore });
  let receiver = createTaskCore({ endpoint: target, relay, store });
  try {
    await origin.connect(); await receiver.connect(); const { taskId } = await origin.createTask({ target, task: "incorporated once", timeoutMs: 60_000 }); await receiver.receive();
    const eventId = receiver.getTask(taskId)!.events[0]!.eventId; receiver.recordWakeRequest({ taskId, eventId });
    const manager = SessionManager.create(root, sessions);
    manager.appendMessage({ role: "assistant", content: [{ type: "text", text: "existing session" }], api: "anthropic-messages", provider: "anthropic", model: "test", usage: { input: 1, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 1, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } }, stopReason: "stop", timestamp: Date.now() });
    manager.appendCustomMessageEntry("pi-tasks-event", "assignment", true, { taskId, eventId });
    manager.appendCustomMessageEntry("pi-tasks-wake", "Process the pending Pi task event.", false, { taskId, eventId });
    const reopened = SessionManager.open(manager.getSessionFile()!, sessions, root);
    store.close(); store = open(); receiver = createTaskCore({ endpoint: target, relay, store });
    let emissions = 0;
    const host = { sendMessage() { emissions++; }, appendEntry(type: string, data: unknown) { reopened.appendCustomEntry(type, data); } };
    const context = { isIdle: () => true, hasPendingMessages: () => false, sessionManager: reopened };
    await deliverTaskInbox(host, receiver, context); await deliverTaskInbox(host, receiver, context); await origin.receive();
    expect(emissions).toBe(0);
    expect(origin.getTask(taskId)!.events.filter(event => event.type === "task.delivery_receipt" && event.payload.stage === "wake_accepted")).toHaveLength(1);
  } finally { store.close(); originStore.close(); rmSync(root, { recursive: true, force: true }); }
});

test("an emitted but unobserved wake retains uncertainty instead of retrying automatic execution", async () => {
  const root = mkdtempSync(join(tmpdir(), "unobserved-wake-")), owner = { sessionId: crypto.randomUUID(), url: "http://127.0.0.1:1" };
  const store = createTaskStore({}, openTaskJournal(join(root, owner.sessionId, "tasks.sqlite"), owner));
  const originStore = createTaskStore(), relay = createInMemoryTaskRelay("fixture"), target = { relay: relay.id, id: "target" };
  const origin = createTaskCore({ endpoint: { relay: relay.id, id: "source" }, relay, store: originStore }), receiver = createTaskCore({ endpoint: target, relay, store });
  const entries: any[] = []; let emitted = 0;
  const host = { sendMessage(message: any) { if (message.customType === "pi-tasks-wake") emitted++; else entries.push({ type: "custom_message", ...message }); }, appendEntry() {} };
  const context = { isIdle: () => true, hasPendingMessages: () => false, sessionManager: { getEntries: () => entries } };
  try {
    await origin.connect(); await receiver.connect(); await origin.createTask({ target, task: "one emission", timeoutMs: 60_000 });
    await expect(deliverTaskInbox(host, receiver, context)).rejects.toMatchObject({ code: "TASK_PRESENTATION_UNCONFIRMED" });
    await expect(deliverTaskInbox(host, receiver, context)).rejects.toMatchObject({ code: "TASK_PRESENTATION_UNCONFIRMED" });
    expect(emitted).toBe(1);
    expect(store.outbox("accepted").some(row => JSON.parse(row.envelope.payload).payload?.stage === "wake_accepted")).toBe(false);
  } finally { store.close(); originStore.close(); rmSync(root, { recursive: true, force: true }); }
});

test("interrupted wake without durable harness evidence is explicit unknown, never automatic assignment execution", async () => {
  const root = mkdtempSync(join(tmpdir(), "recovery-presentation-"));
  const relay = createInMemoryTaskRelay("fixture"), source = { relay: relay.id, id: "source" }, target = { relay: relay.id, id: "target" };
  const owner = { sessionId: crypto.randomUUID(), url: "http://127.0.0.1:1" };
  const path = join(root, owner.sessionId, "tasks.sqlite");
  const open = () => createTaskStore({}, openTaskJournal(path, owner));
  let store = open();
  const originStore = createTaskStore();
  try {
    const origin = createTaskCore({ endpoint: source, relay, store: originStore });
    let receiver = createTaskCore({ endpoint: target, relay, store });
    await origin.connect(); await receiver.connect();
    const { taskId } = await origin.createTask({ target, task: "execute only once", timeoutMs: 60_000 });
    await receiver.receive();
    const event = receiver.getTask(taskId)!.events[0]!;
    // Crash cut: protocol write completed; the harness's wake outcome cannot be proven.
    receiver.recordWakeRequest({ taskId, eventId: event.eventId });
    store.close(); store = open(); receiver = createTaskCore({ endpoint: target, relay, store });
    const messages: any[] = [], entries: any[] = [{ type: "custom_message", customType: "pi-tasks-event", details: { taskId, eventId: event.eventId } }];
    const host = { sendMessage(message: any) { messages.push(message); entries.push({ type: "custom_message", ...message }); }, appendEntry() {} };
    const context = { isIdle: () => true, hasPendingMessages: () => false, sessionManager: { getEntries: () => entries } };
    await expect(deliverTaskInbox(host, receiver, context)).rejects.toMatchObject({ code: "TASK_PRESENTATION_UNCONFIRMED" });
    expect(messages.filter(message => message.customType === "pi-tasks-wake")).toEqual([]);
  } finally { store.close(); originStore.close(); rmSync(root, { recursive: true, force: true }); }
});
