import { expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { openTaskJournal } from "../src/task-journal";
import { createTaskStore } from "../src/task-store";
import { createTaskCore } from "../src/task-core";
import { createInMemoryTaskRelay } from "../src/in-memory-task-relay";
import { deliverTaskInbox } from "../src/task-inbox";

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
