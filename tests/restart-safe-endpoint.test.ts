import { expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { openTaskJournal } from "../src/task-journal";
import { createTaskStore } from "../src/task-store";
import { createRestartSafeTaskSession } from "../src/volatile-task-session";
import { createConfiguredTaskCore } from "../src/configured-task-core";

test("inconsistent persisted generation is rejected before registration instead of silently repaired", async () => {
  const root = mkdtempSync(join(tmpdir(), "generation-recovery-")), sessionId = crypto.randomUUID();
  const url = "http://127.0.0.1:1/api/task-relay/restart-safe-v1", endpoint = { relay: "wolfpack-pi-tasks-v2", id: crypto.randomUUID() };
  const store = createTaskStore({}, openTaskJournal(join(root, sessionId, "tasks.sqlite"), { sessionId, url }));
  let calls = 0;
  const fetcher = Object.assign(async () => { calls++; return Response.json({ ok: true, profile: "restart-safe-v1", epoch, value: { kind: "connected", endpoint, sessionId, leaseExpiresAt: new Date(Date.now() + 60_000).toISOString() } }); }, { preconnect: fetch.preconnect }) as typeof fetch;
  const epoch = crypto.randomUUID(), generation = crypto.randomUUID();
  store.transaction(() => { store.setEndpointGeneration(crypto.randomUUID()); store.setEndpointBinding(endpoint); store.setRelayTransportBinding({ profile: "restart-safe-v1", epoch, endpoint, generation, callerSession: sessionId, url }); });
  const session = createRestartSafeTaskSession({ url, callerSession: sessionId, store, fetch: fetcher });
  try { await expect(session.connect()).rejects.toMatchObject({ code: "INVALID_RELAY_METADATA" }); expect(calls).toBe(0); }
  finally { session.close(); store.close(); rmSync(root, { recursive: true, force: true }); }
});

test("session-name reuse selects a distinct UUID journal, never the prior assignment", async () => {
  const root = mkdtempSync(join(tmpdir(), "session-name-recovery-")), endpoints = new Map<string, { relay: string; id: string }>(), epoch = crypto.randomUUID();
  const fetcher = Object.assign(async (_url: unknown, init?: RequestInit) => {
    const body = JSON.parse(String(init?.body));
    if (!endpoints.has(body.callerSession)) endpoints.set(body.callerSession, { relay: "wolfpack-pi-tasks-v2", id: crypto.randomUUID() });
    const value = body.operation === "connect" ? { kind: "connected", endpoint: endpoints.get(body.callerSession), sessionId: body.callerSession, leaseExpiresAt: new Date(Date.now() + 60_000).toISOString() }
      : body.operation === "resolve" ? { kind: "resolved", endpoint: body.target }
      : { kind: "accepted", envelopeId: body.envelope.envelopeId, acceptanceId: crypto.randomUUID(), duplicate: false, forwarding: "local" };
    return Response.json({ ok: true, profile: body.profile, epoch, value });
  }, { preconnect: fetch.preconnect }) as typeof fetch;
  const options = { sessionName: "reused-name", stateRoot: root, baseUrl: "http://127.0.0.1:1", fetch: fetcher }, firstId = crypto.randomUUID();
  let core = await createConfiguredTaskCore({ ...options, sessionId: firstId });
  try {
    const endpoint = core.endpoint, { taskId } = await core.createTask({ target: core.endpoint, task: "belongs only to the original UUID", timeoutMs: 60_000 });
    await core.close(); core = await createConfiguredTaskCore({ ...options, sessionId: crypto.randomUUID() });
    expect(core.listTasks()).toEqual([]); expect(core.endpoint).not.toEqual(endpoint);
    await core.close(); core = await createConfiguredTaskCore({ ...options, sessionId: firstId });
    expect(core.endpoint).toEqual(endpoint); expect(core.getTask(taskId)?.taskId).toBe(taskId);
  } finally { await core.close(); rmSync(root, { recursive: true, force: true }); }
});

test("old/missing server profiles are explicit refusal with no volatile or legacy fallback", async () => {
  for (const missingPath of [false, true]) {
    const root = mkdtempSync(join(tmpdir(), "profile-recovery-")), calls: string[] = [];
    const fetcher = Object.assign(async (url: unknown) => { calls.push(String(url)); return missingPath ? new Response("not found", { status: 404 }) : Response.json({ ok: false, profile: "volatile-v1", error: { code: "RELAY_PROFILE_REQUIRED", retryable: false } }, { status: 409 }); }, { preconnect: fetch.preconnect }) as typeof fetch;
    try {
      await expect(createConfiguredTaskCore({ sessionId: crypto.randomUUID(), stateRoot: root, baseUrl: "http://127.0.0.1:1", fetch: fetcher })).rejects.toMatchObject({ code: "RELAY_PROFILE_REQUIRED", retryable: false });
      expect(calls).toEqual(["http://127.0.0.1:1/api/task-relay/restart-safe-v1"]);
    } finally { rmSync(root, { recursive: true, force: true }); }
  }
});

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
