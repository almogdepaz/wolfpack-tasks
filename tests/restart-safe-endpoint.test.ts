import { expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { createConfiguredTaskCore } from "../src/configured-task-core";

// Network boundary only: task core, journal, operation reservations and transport are real.
test("configured endpoint reopen retains assignment, pending message IDs and parent ACK progression", async () => {
  const root = mkdtempSync(join(tmpdir(), "restart-safe-endpoint-"));
  const sessionId = crypto.randomUUID(), epoch = crypto.randomUUID();
  const endpoint = { relay: "wolfpack-pi-tasks-v2", id: crypto.randomUUID() };
  const sent: any[] = []; let unavailable = false;
  const fetcher = Object.assign(async (_url: any, init?: RequestInit) => {
    const body = JSON.parse(String(init?.body));
    if (body.operation === "send") { sent.push(body.envelope); if (unavailable) throw new Error("response lost"); }
    const value = body.operation === "connect" ? { kind: "connected", endpoint, sessionId, leaseExpiresAt: new Date(Date.now() + 60_000).toISOString() }
      : body.operation === "resolve" ? { kind: "resolved", endpoint: body.target }
      : body.operation === "send" ? { kind: "accepted", envelopeId: body.envelope.envelopeId, acceptanceId: crypto.randomUUID(), duplicate: false, forwarding: "local" }
      : { kind: "page", deliveries: [], nextCursor: body.cursor ?? "0", hasMore: false };
    return Response.json({ ok: true, profile: body.profile, epoch, value });
  }, { preconnect: fetch.preconnect }) as typeof fetch;
  const options = { sessionName: "reusable-name", sessionId, stateRoot: root, baseUrl: "http://127.0.0.1:1", fetch: fetcher };
  let core: Awaited<ReturnType<typeof createConfiguredTaskCore>> | undefined;
  try {
    core = await createConfiguredTaskCore(options);
    const task = await core.createTask({ target: endpoint, task: "do not execute twice", timeoutMs: 60_000 });
    const eventId = core.getTask(task.taskId)!.events[0]!.eventId;
    unavailable = true;
    await core.submitIntent({ taskId: task.taskId, type: "task.information", payload: { message: "persist before send" } }).catch(() => undefined);
    const ids = sent.map(item => item.envelopeId);
    await core.close(); core = await createConfiguredTaskCore(options);
    expect(core.listTasks().map(item => item.taskId)).toEqual([task.taskId]);
    unavailable = false; await core.flushOutbox();
    expect(sent.at(-1).envelopeId).toBe(ids.at(-1));
    expect(core.getTask(task.taskId)?.events[0]?.eventId).toBe(eventId);
  } finally { await core?.close(); rmSync(root, { recursive: true, force: true }); }
});
