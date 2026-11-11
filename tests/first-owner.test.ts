import { afterEach, expect, jest, test } from "bun:test";
import { createVolatileTaskSession, VOLATILE_PROFILE } from "../src/volatile-task-session";
import { createTaskStore } from "../src/task-store";
import { createConfiguredTaskCore } from "../src/configured-task-core";
import { createConfiguredCoreLoader } from "../src/agent-task-runtime";

const epoch = "00000000-0000-4000-8000-000000000001";
const endpoint = { relay: "wolfpack-pi-tasks-v2", id: "00000000-0000-4000-8000-000000000002" };
const connected = () => Response.json({ ok: true, profile: VOLATILE_PROFILE, epoch, value: {
  kind: "connected", endpoint, leaseExpiresAt: new Date(Date.now() + 60_000).toISOString(),
} });
const refused = (retryAfterMs = 60_000) => Response.json({ ok: false, profile: VOLATILE_PROFILE, epoch,
  error: { code: "SESSION_ALREADY_REGISTERED", retryable: true, retryAfterMs } }, { status: 503 });
const disconnected = () => Response.json({ ok: true, profile: VOLATILE_PROFILE, epoch, value: { kind: "disconnected" } });
async function settle() { for (let i = 0; i < 200; i++) await Promise.resolve(); }
afterEach(() => jest.useRealTimers());
function fixture(handler: (body: any) => Response | Promise<Response>) {
  const calls: any[] = [];
  const fetcher = Object.assign(async (_url: unknown, init?: RequestInit) => {
    const body = JSON.parse(String(init?.body)); calls.push(body); return handler(body);
  }, { preconnect: fetch.preconnect }) as typeof fetch;
  const store = createTaskStore();
  const options = { url: "http://127.0.0.1:1/relay", callerSession: crypto.randomUUID(), store, fetch: fetcher };
  const session = createVolatileTaskSession(options);
  return { session, store, calls, fetcher, options, async close() { await session.close(); store.close(); } };
}

test("a refused copy waits the lease delay before retrying, then can register", async () => {
  jest.useFakeTimers();
  let attempts = 0;
  const f = fixture(body => body.operation === "disconnect" ? disconnected() : ++attempts === 1 ? refused() : connected());
  try {
    const result = f.session.connect().then(value => value, error => error);
    await settle(); expect(attempts).toBe(1);
    jest.advanceTimersByTime(59_999); await settle(); expect(attempts).toBe(1);
    jest.advanceTimersByTime(1); await settle();
    expect((await result).endpoint).toEqual(endpoint); expect(attempts).toBe(2);
  } finally { await f.close(); }
});

test("a live owner disables the copy after 70 seconds, including explicit rebind", async () => {
  jest.useFakeTimers();
  const f = fixture(() => refused());
  try {
    let failure: any;
    const result = f.session.connect().catch(error => { failure = error; });
    await settle();
    jest.advanceTimersByTime(60_000); await settle(); expect(f.calls).toHaveLength(2);
    jest.advanceTimersByTime(9_999); await settle(); expect(failure).toBeUndefined();
    jest.advanceTimersByTime(1); await settle(); await result;
    expect(failure).toMatchObject({ code: "SESSION_ALREADY_REGISTERED", retryable: false });
    expect(failure.message).toContain("SESSION_ALREADY_REGISTERED");
    expect(f.session.status().binding).toBeUndefined();
    await expect(f.session.connect()).rejects.toMatchObject({ code: "SESSION_ALREADY_REGISTERED", retryable: false });
    await expect(f.session.rebind()).rejects.toMatchObject({ code: "SESSION_ALREADY_REGISTERED", retryable: false });
    expect(f.calls).toHaveLength(2);
    const recreated = createVolatileTaskSession(f.options);
    const again = recreated.connect().catch(error => error);
    await settle();
    expect(f.calls).toHaveLength(2);
    expect(await again).toMatchObject({ code: "SESSION_ALREADY_REGISTERED", retryable: false });
    await recreated.close();
  } finally { await f.close(); }
});

test("the configured adapter does not recreate a disabled transport on lifecycle restart", async () => {
  jest.useFakeTimers();
  const f = fixture(() => refused());
  let factories = 0;
  const loader = createConfiguredCoreLoader(signal => {
    factories++;
    return createConfiguredTaskCore({ sessionName: "copy", baseUrl: "http://127.0.0.1:1", fetch: f.fetcher }, signal);
  });
  try {
    const result = loader().catch(error => error);
    await settle(); jest.advanceTimersByTime(60_000); await settle(); jest.advanceTimersByTime(10_000); await settle();
    expect(await result).toMatchObject({ code: "SESSION_ALREADY_REGISTERED", retryable: false });
    await loader.close(); await loader.start();
    const next = loader().catch(error => error);
    await settle();
    expect(factories).toBe(2);
    expect(f.calls).toHaveLength(2);
    expect(await next).toMatchObject({ code: "SESSION_ALREADY_REGISTERED", retryable: false });
  } finally { await loader.close(); await f.close(); }
});

test("a lost initial connect reply retries the same generation across configured setup attempts", async () => {
  jest.useFakeTimers();
  let generation: string | undefined;
  const f = fixture(body => {
    if (body.operation === "disconnect") return disconnected();
    if (!generation) { generation = body.generation; throw new Error("accepted reply lost"); }
    return body.generation === generation ? connected() : refused();
  });
  const options = { sessionName: crypto.randomUUID(), baseUrl: "http://127.0.0.1:1", fetch: f.fetcher };
  try {
    await expect(createConfiguredTaskCore(options)).rejects.toMatchObject({ code: "RELAY_UNAVAILABLE" });
    const pending = createConfiguredTaskCore(options);
    await settle();
    expect(f.calls[1].generation).toBe(f.calls[0].generation);
    const core = await pending;
    expect(core.endpoint).toEqual(endpoint);
    await core.close();
  } finally { await f.close(); }
});

test("cancelled setup cannot restart the process retry window or retry before the lease delay", async () => {
  jest.useFakeTimers();
  const f = fixture(() => refused());
  const abort = new AbortController();
  const first = f.session.connect(abort.signal).catch(error => error);
  await settle(); jest.advanceTimersByTime(10_000); abort.abort();
  expect(await first).toMatchObject({ code: "ABORTED" });
  await f.session.close();
  const next = createVolatileTaskSession(f.options);
  try {
    const second = next.connect().catch(error => error);
    await settle(); expect(f.calls).toHaveLength(1);
    jest.advanceTimersByTime(50_000); await settle(); expect(f.calls).toHaveLength(2);
    jest.advanceTimersByTime(10_000); await settle();
    expect(await second).toMatchObject({ code: "SESSION_ALREADY_REGISTERED", retryable: false });
  } finally { await next.close(); await f.close(); }
});

test("clean close disconnects exactly the owned binding once before a new lifecycle", async () => {
  const f = fixture(body => body.operation === "connect" ? connected() : disconnected());
  try {
    await f.session.connect(); await f.session.close(); await f.session.close();
    expect(f.calls.map(body => body.operation)).toEqual(["connect", "disconnect"]);
    expect(f.calls[1]).toMatchObject({ epoch, endpoint, callerSession: f.options.callerSession });
  } finally { await f.close(); }
});

test("explicit rebind disconnects the old generation before a fresh connect", async () => {
  let owner: string | undefined;
  const f = fixture(body => {
    if (body.operation === "disconnect") { owner = undefined; return disconnected(); }
    if (owner && body.generation !== owner) return refused();
    owner = body.generation; return connected();
  });
  try {
    await f.session.connect(); const first = owner;
    await f.session.rebind();
    expect(f.calls.map(body => body.operation)).toEqual(["connect", "disconnect", "connect"]);
    expect(owner).not.toBe(first);
  } finally { await f.close(); }
});

test("shutdown bounds a noncooperative disconnect to one second", async () => {
  jest.useFakeTimers();
  const f = fixture(body => body.operation === "connect" ? connected() : new Promise(() => {}));
  try {
    await f.session.connect();
    let closed = false;
    const closing = Promise.resolve(f.session.close()).then(() => { closed = true; });
    await settle(); expect(closed).toBe(false);
    jest.advanceTimersByTime(1000); await settle(); await closing;
    expect(closed).toBe(true);
    expect(f.calls.at(-1).operation).toBe("disconnect");
  } finally { await f.close(); }
});

test("closing a refused copy cancels its retry and never disconnects the real owner", async () => {
  jest.useFakeTimers();
  const f = fixture(() => refused());
  const pending = f.session.connect().catch(error => error);
  await settle(); await f.session.close(); await pending;
  jest.advanceTimersByTime(120_000); await settle();
  expect(f.calls.map(body => body.operation)).toEqual(["connect"]);
  await f.close();
});
