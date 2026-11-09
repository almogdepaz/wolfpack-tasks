import { expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createConfiguredTaskCore } from "../src/configured-task-core";
import { createConfiguredCoreLoader } from "../src/extension";
import { createTaskStore } from "../src/task-store";
import type { TaskCore } from "../src/task-core";

const profile = "volatile-v1", epoch = "00000000-0000-4000-8000-000000000001";
const endpoint = { relay: "wolfpack-pi-tasks-v2", id: "00000000-0000-4000-8000-000000000002" };
function fixture(handler?: (body: any, init: RequestInit) => Promise<Response> | Response) {
  const root = mkdtempSync(join(tmpdir(), "tasks-configured-"));
  const path = join(root, "tasks.sqlite"), calls: any[] = [];
  const registrations = new Map<string, typeof endpoint>();
  const fetcher = Object.assign(async (url: unknown, init?: RequestInit) => {
    expect(String(url)).toBe("http://127.0.0.1:1/api/task-relay/volatile-v1");
    const body = JSON.parse(String(init?.body)); calls.push(body);
    if (handler) return handler(body, init!);
    if (!registrations.has(body.generation)) registrations.set(body.generation, { ...endpoint, id: crypto.randomUUID() });
    const value = body.operation === "connect"
      ? { kind: "connected", endpoint: registrations.get(body.generation), leaseExpiresAt: new Date(Date.now() + 60_000).toISOString() }
      : body.operation === "resolve"
        ? { kind: "resolved", endpoint: body.target }
        : body.operation === "send"
          ? { kind: "accepted", envelopeId: body.envelope.envelopeId, acceptanceId: crypto.randomUUID(), duplicate: false, forwarding: "local" }
          : { kind: "page", deliveries: [], nextCursor: "0", hasMore: false };
    return Response.json({ ok: true, profile, epoch, value });
  }, { preconnect: fetch.preconnect }) as typeof fetch;
  return { path, calls, options: { sessionName: "fixture", baseUrl: "http://127.0.0.1:1", fetch: fetcher }, cleanup: () => rmSync(root, { recursive: true, force: true }) };
}

test("configured close discards RAM and the next lifecycle registers a fresh identity without replay", async () => {
  const f = fixture();
  try {
    const first = await createConfiguredTaskCore(f.options);
    await first.receive(); await first.close(); await first.close();
    expect(() => first.listTasks()).toThrow("task session is closed");
    const second = await createConfiguredTaskCore(f.options);
    expect(second.endpoint).not.toEqual(first.endpoint); expect(second.listTasks()).toEqual([]); await second.close();
    const connects = f.calls.filter(c => c.operation === "connect");
    expect(connects).toHaveLength(2); expect(connects[1].generation).not.toBe(connects[0].generation);
    expect(connects[1].epoch).toBeUndefined(); expect(f.calls.some(c => c.operation === "send")).toBe(false);
  } finally { f.cleanup(); }
});

test("configured close fences a captured wake-request flush before it can relay", async () => {
  const f = fixture();
  try {
    const core = await createConfiguredTaskCore(f.options);
    const task = await core.createTask({ target: core.endpoint, task: "wake once", timeoutMs: 1_000 });
    const request = core.recordWakeRequest({ taskId: task.taskId, eventId: "event-1" });
    const sendsBeforeClose = f.calls.filter(call => call.operation === "send").length;
    await core.close();

    expect(() => request.flush()).toThrow(expect.objectContaining({ code: "RELAY_CLOSED", retryable: false }));
    expect(f.calls.filter(call => call.operation === "send")).toHaveLength(sendsBeforeClose);
  } finally { f.cleanup(); }
});

test("profile refusal does not trigger downgrade or bind an unbound store", async () => {
  const f = fixture(() => Response.json({ ok: false, profile: "durable-v2", error: { code: "RELAY_PROFILE_REQUIRED", retryable: false } }, { status: 409 }));
  try {
    await expect(createConfiguredTaskCore(f.options)).rejects.toMatchObject({ code: "RELAY_PROFILE_REQUIRED" });
    expect(f.calls).toHaveLength(1);
    const store = createTaskStore(); expect(store.getRelayTransportBinding()).toBeUndefined(); store.close();
  } finally { f.cleanup(); }
});

test("owned close aborts pending receive before discarding RAM and fences new work", async () => {
  let started!: () => void;
  const receiving = new Promise<void>(resolve => { started = resolve; });
  const f = fixture(body => {
    if (body.operation === "connect") return Response.json({ ok: true, profile, epoch, value: { kind: "connected", endpoint, leaseExpiresAt: new Date(Date.now() + 60_000).toISOString() } });
    started(); return new Promise<Response>(() => {}); // Even a noncooperative fetch must be fenced.
  });
  try {
    const core = await createConfiguredTaskCore(f.options);
    const result = core.receive().then(() => "unexpected", error => error.code);
    await receiving; await core.close();
    expect(await result).toBe("RELAY_RESET");
    expect(() => core.receive()).toThrow("task session is closed");
    expect(() => core.recordWakeRequest({ taskId: "closed-task", eventId: "closed-event" })).toThrow("task session is closed");
    const fresh = createTaskStore(); expect(fresh.getReceiveCursor()).toBe("0"); fresh.close();
  } finally { f.cleanup(); }
});

test("loader cancels setup and disposes a late core without resurrecting it", async () => {
  let release!: (core: TaskCore) => void, signal: AbortSignal | undefined, closed = 0;
  const loader = createConfiguredCoreLoader(async s => { signal = s; return new Promise<TaskCore>(resolve => { release = resolve; }); });
  const pending = loader().then(() => "unexpected", e => e.code);
  await Promise.resolve();
  const closing = loader.close(); expect(signal!.aborted).toBe(true);
  release({ close: async () => { closed++; } } as unknown as TaskCore);
  await closing; expect(await pending).toBe("RELAY_CLOSED"); expect(closed).toBe(1);
  await expect(loader()).rejects.toMatchObject({ code: "RELAY_CLOSED" });
});

test("loader closes cached ownership once per lifecycle and starts a fresh instance", async () => {
  let opens = 0, closes = 0;
  const loader = createConfiguredCoreLoader(async () => ({ instance: ++opens, close: async () => { closes++; } }) as unknown as TaskCore);
  const first = await loader(); expect(await loader()).toBe(first);
  await loader.close(); await loader.close(); expect(closes).toBe(1);
  await loader.start(); expect(await loader()).not.toBe(first); expect(opens).toBe(2);
  await loader.close(); expect(closes).toBe(2);
});

test("loader startup waits for prior lifetime disposal and a new shutdown fences that startup", async () => {
  let release!: () => void;
  const loader = createConfiguredCoreLoader(async () => ({ close: () => new Promise<void>(resolve => { release = resolve; }) }) as unknown as TaskCore);
  await loader();
  const closing = loader.close();
  await Promise.resolve();
  let started = false;
  const starting = loader.start().then(() => { started = true; return "unexpected"; }, error => error.code);
  await Promise.resolve(); expect(started).toBe(false);
  const stoppedAgain = loader.close(); release();
  await Promise.all([closing, stoppedAgain]);
  expect(await starting).toBe("RELAY_CLOSED");
  await expect(loader()).rejects.toMatchObject({ code: "RELAY_CLOSED" });
});
