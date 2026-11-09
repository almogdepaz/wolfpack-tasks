import { expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { createTaskStore } from "../src/task-store";
import { openTaskJournal } from "../src/task-journal";
import { createRestartSafeTaskSession } from "../src/volatile-task-session";

const failures = [
  { code: "RELAY_STORAGE_UNCERTAIN", retryable: true, ownerFatal: true, mayHaveBeenDelivered: true },
  { code: "RELAY_STORAGE_BUSY", retryable: true, ownerFatal: true },
  { code: "RELAY_STORAGE_UNAVAILABLE", retryable: true, ownerFatal: true },
  { code: "RELAY_STORAGE_INVALID", retryable: false, ownerFatal: true },
  { code: "RELAY_STORAGE_RECOVERY_BLOCKED", retryable: false, ownerFatal: true, mayHaveBeenDelivered: true },
  { code: "RELAY_STORAGE_RECOVERING", retryable: true, retryAfterMs: 10 },
] as const;
for (const failure of failures) test(`${failure.code} keeps the recorded envelope pending for repair after owner reopen`, async () => {
  const root = mkdtempSync("/tmp/task-owner-failure-"), callerSession = crypto.randomUUID(), epoch = crypto.randomUUID();
  const url = "http://127.0.0.1:1/api/task-relay/restart-safe-v1", endpoint = { relay: "wolfpack-pi-tasks-v2", id: crypto.randomUUID() };
  const store = createTaskStore({}, openTaskJournal(join(root, callerSession, "tasks.sqlite"), { sessionId: callerSession, url }));
  let failed = true; const sends: string[] = [];
  const fetcher = Object.assign(async (_url: unknown, init?: RequestInit) => {
    const body = JSON.parse(String(init?.body));
    if (body.operation === "send") {
      sends.push(JSON.stringify(body.envelope));
      if (failed) return Response.json({ ok: false, profile: "restart-safe-v1", error: failure }, { status: 503 });
      return Response.json({ ok: true, profile: "restart-safe-v1", epoch, value: { kind: "accepted", envelopeId: body.envelope.envelopeId, acceptanceId: crypto.randomUUID(), duplicate: true, forwarding: "local" } });
    }
    return Response.json({ ok: true, profile: "restart-safe-v1", epoch, value: body.operation === "connect"
      ? { kind: "connected", endpoint, sessionId: callerSession, leaseExpiresAt: new Date(Date.now() + 60_000).toISOString() }
      : { kind: "resolved", endpoint: body.target } });
  }, { preconnect: fetch.preconnect }) as typeof fetch;
  const session = createRestartSafeTaskSession({ url, callerSession, store, fetch: fetcher });
  try {
    const core = await session.connect();
    const error = await core.createTask({ target: endpoint, task: "one logical task despite poisoned relay owner", timeoutMs: 60_000 }).catch(error => error);
    const taskId = error.details?.taskId;
    expect(error.code).toBe(failure.code); expect(error.retryable).toBe(failure.retryable);
    if ("ownerFatal" in failure) expect(error.details.ownerFatal).toBe(true);
    if ("mayHaveBeenDelivered" in failure) expect(error.details.mayHaveBeenDelivered).toBe(true);
    expect(typeof taskId).toBe("string");
    expect(store.outbox("pending")).toHaveLength(1); expect(store.getRelayTransportBinding()?.reset).toBeUndefined();
    failed = false; await core.flushOutbox();
    expect(sends).toHaveLength(2); expect(sends[1]).toBe(sends[0]);
    expect(core.listTasks().map(task => task.taskId)).toEqual([taskId]);
  } finally { session.close(); store.close(); rmSync(root, { recursive: true, force: true }); }
});
