import { expect, test } from "bun:test";
import { createTaskStore } from "../src/task-store";
import { createVolatileTaskSession } from "../src/volatile-task-session";
const profile = "volatile-v1", epoch = "00000000-0000-4000-8000-000000000001";
const endpoint = { relay: "wolfpack-pi-tasks-v2", id: "00000000-0000-4000-8000-000000000002" };
const reply = (value: unknown) => Response.json({ ok: true, profile, epoch, value });
const connected = () => reply({ kind: "connected", endpoint, leaseExpiresAt: new Date(Date.now() + 60_000).toISOString() });
function setup(handler: (body: any) => Response, requestTimeoutMs = 12_000) {
  const store = createTaskStore(); let calls = 0;
  const fetcher = Object.assign(async (_url: unknown, init?: RequestInit) => { calls++; return handler(JSON.parse(String(init?.body))); }, { preconnect: fetch.preconnect }) as typeof fetch;
  const options = { url: "http://127.0.0.1:1/volatile", callerSession: "fixture", store, fetch: fetcher, requestTimeoutMs };
  const session = createVolatileTaskSession(options);
  return { store, session, options, calls: () => calls, close: () => { session.close(); store.close(); } };
}

for (const [code, retryable] of [
  ["RELAY_STORAGE_BUSY", true], ["RELAY_STORAGE_UNAVAILABLE", true], ["RELAY_STORAGE_UNCERTAIN", true],
  ["RELAY_STORAGE_INVALID", false], ["RELAY_STORAGE_RECOVERY_BLOCKED", false], ["RELAY_STORAGE_RECOVERING", true],
] as const) test(`epochless ${code} preserves its typed diagnosis and the same RAM binding`, async () => {
  let rejecting = true;
  const f = setup(() => rejecting
    ? Response.json({ ok: false, profile, error: { code, retryable, ...(code !== "RELAY_STORAGE_RECOVERING" && { ownerFatal: true }), retryAfterMs: 1000 } }, { status: 503 })
    : connected());
  try {
    await expect(f.session.connect()).rejects.toMatchObject({ code, retryable, details: { retryAfterMs: 1000 } });
    const generation = f.store.getEndpointGeneration();
    rejecting = false;
    await f.session.connect();
    expect(f.store.getRelayTransportBinding()).toMatchObject({ generation, profile, epoch });
    expect(f.store.quarantinedOutbox()).toEqual([]);
  } finally { f.close(); }
});

test("typed uncertain storage failure retains a live RAM task and retries only the original envelope", async () => {
  let rejecting = true;
  const sends: string[] = [];
  const f = setup(body => {
    if (body.operation === "connect") return connected();
    if (body.operation === "resolve") return reply({ kind: "resolved", endpoint });
    sends.push(JSON.stringify(body.envelope));
    return rejecting ? Response.json({ ok: false, profile, error: { code: "RELAY_STORAGE_UNCERTAIN", retryable: true, ownerFatal: true, mayHaveBeenDelivered: true } }, { status: 503 })
      : reply({ kind: "accepted", envelopeId: body.envelope.envelopeId, acceptanceId: epoch, duplicate: true, forwarding: "local" });
  });
  try {
    const core = await f.session.connect(), binding = f.store.getRelayTransportBinding();
    await expect(core.createTask({ target: endpoint, task: "retain this task", timeoutMs: 60_000 })).rejects.toMatchObject({
      code: "RELAY_STORAGE_UNCERTAIN", retryable: true, details: { ownerFatal: true, mayHaveBeenDelivered: true },
    });
    const pending = f.store.outbox("pending"); expect(pending).toHaveLength(1);
    expect(core.listTasks()).toHaveLength(1); expect(f.store.quarantinedOutbox()).toEqual([]);
    expect(f.store.getRelayTransportBinding()).toEqual(binding);
    rejecting = false; await core.flushOutbox();
    expect(sends).toHaveLength(2); expect(sends[1]).toBe(sends[0]);
    expect(f.store.outbox("accepted")).toHaveLength(1);
    expect(await f.session.connect()).toBe(core);
  } finally { f.close(); }
});

const httpFailures = [
  { name: "generic JSON 429", status: 429, body: JSON.stringify({ error: "rate limit exceeded" }), code: "RELAY_CAPACITY", retryable: true },
  { name: "empty 429", status: 429, body: null, code: "RELAY_CAPACITY", retryable: true },
  { name: "non-JSON 429", status: 429, body: "slow down", code: "RELAY_CAPACITY", retryable: true },
  { name: "401 authentication failure", status: 401, body: "unauthorized", code: "RELAY_AUTH_REQUIRED", retryable: false },
] as const;

function loopback(handler: (request: Request) => Promise<Response>): {
  readonly store: ReturnType<typeof createTaskStore>;
  readonly session: ReturnType<typeof createVolatileTaskSession>;
  readonly close: () => void;
} {
  const server = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: handler });
  const store = createTaskStore();
  const session = createVolatileTaskSession({ url: new URL("/volatile", server.url).href, callerSession: "fixture", store });
  return { store, session, close: () => { session.close(); store.close(); void server.stop(true); } };
}

for (const rejection of httpFailures) {
  test(`${rejection.name} during initial connect preserves generation for an explicit retry`, async () => {
    const requests: string[] = [];
    const f = loopback(async request => {
      requests.push(await request.text());
      return requests.length === 1 ? new Response(rejection.body, { status: rejection.status }) : connected();
    });
    try {
      await expect(f.session.connect()).rejects.toMatchObject({ code: rejection.code, retryable: rejection.retryable });
      expect(f.session.status()).toEqual({ state: "unbound", binding: undefined });
      expect(f.store.getEndpointBinding()).toBeUndefined();
      expect(f.store.getReceiveCursor()).toBe("0");
      expect(f.store.outbox("accepted")).toEqual([]);
      const generation = f.store.getEndpointGeneration();
      expect(generation).toBeDefined();

      const core = await f.session.connect();
      expect(core.endpoint).toEqual(endpoint);
      expect(f.session.status().state).toBe("ready");
      expect(f.store.getRelayTransportBinding()).toMatchObject({ epoch, endpoint, generation });
      expect(requests).toHaveLength(2);
      expect(requests[1]).toBe(requests[0]);
    } finally { f.close(); }
  });

  test(`${rejection.name} preserves a live binding and pending envelope until explicit acceptance`, async () => {
    let rejecting = true, connects = 0;
    const sends: string[] = [];
    const f = loopback(async request => {
      const encoded = await request.text();
      // These requests come from the real transport's JSON serialization.
      const body = JSON.parse(encoded) as { readonly operation: string; readonly envelope?: { readonly envelopeId: string }; readonly cursor?: string };
      if (body.operation === "connect") { connects++; return connected(); }
      if (body.operation === "resolve") return reply({ kind: "resolved", endpoint });
      if (body.operation === "send") sends.push(encoded);
      if (rejecting) return new Response(rejection.body, { status: rejection.status });
      if (body.operation === "send") return reply({ kind: "accepted", envelopeId: body.envelope?.envelopeId, acceptanceId: epoch, duplicate: false, forwarding: "local" });
      if (body.operation === "receive") return reply({ kind: "page", deliveries: [], nextCursor: body.cursor, hasMore: false });
      throw new Error(`unexpected fixture operation: ${body.operation}`);
    });
    try {
      const core = await f.session.connect();
      f.store.setReceiveCursor("42");
      const binding = f.store.getRelayTransportBinding();
      await expect(core.createTask({ target: endpoint, task: "retry the same envelope", timeoutMs: 60_000 }))
        .rejects.toMatchObject({ code: rejection.code, retryable: rejection.retryable });
      const pending = f.store.outbox("pending");
      expect(pending).toHaveLength(1);
      const tasks = core.listTasks();
      expect(tasks).toHaveLength(1);
      expect(tasks[0]?.status).toBe("active");

      // More sequential failures than the transport's active-request limit:
      // capacity errors must release slots rather than poison later retries.
      for (let attempt = 0; attempt < 10; attempt++) {
        await expect(core.flushOutbox()).rejects.toMatchObject({ code: rejection.status === 429 ? "TASK_DELIVERY_DEFERRED" : rejection.code, retryable: rejection.retryable });
      }
      await expect(core.receive()).rejects.toMatchObject({ code: rejection.code, retryable: rejection.retryable });
      expect(f.store.getReceiveCursor()).toBe("42");
      expect(f.session.status()).toEqual({ state: "ready", binding });
      expect(f.store.getEndpointBinding()).toEqual(endpoint);
      expect(f.store.outbox("pending")).toEqual(pending);
      expect(f.store.outbox("accepted")).toEqual([]);
      expect(f.store.quarantinedOutbox()).toEqual([]);
      expect(core.listTasks()).toEqual(tasks);
      expect(sends).toHaveLength(rejection.status === 429 ? 1 : 12);

      rejecting = false;
      // Simulate the eligibility boundary rather than sleeping in a regression fixture.
      if (rejection.status === 429) f.store.postponeOutbox!(pending[0]!.envelope.envelopeId, 0);
      await core.flushOutbox();
      expect(sends).toHaveLength(rejection.status === 429 ? 2 : 13);
      expect(sends.every(encoded => encoded === sends[0])).toBe(true);
      expect(f.store.outbox("pending")).toEqual([]);
      expect(f.store.outbox("accepted")).toEqual(pending.map(record => ({ ...record, state: "accepted" })));
      expect(await core.receive()).toEqual([]);
      expect(await f.session.connect()).toBe(core);
      expect(connects).toBe(1);
      expect(f.session.status()).toEqual({ state: "ready", binding });
      expect(f.store.getReceiveCursor()).toBe("42");
      expect(core.listTasks()).toEqual(tasks);
    } finally { f.close(); }
  });
}

for (const status of [429, 401]) {
  test(`HTTP ${status} cancels an unread body without waiting for its deadline`, async () => {
    let cancelled = false;
    const f = setup(() => new Response(new ReadableStream<Uint8Array>({
      start(controller) { controller.enqueue(new TextEncoder().encode("unfinished response")); },
      cancel() { cancelled = true; },
    }), { status }), 100);
    try {
      await expect(f.session.connect()).rejects.toMatchObject({
        code: status === 429 ? "RELAY_CAPACITY" : "RELAY_AUTH_REQUIRED", retryable: status === 429,
      });
      expect(cancelled).toBe(true);
      expect(f.store.getRelayTransportBinding()).toBeUndefined();
    } finally { f.close(); }
  });
}

test("permanent profile refusal retires/quarantines only the current lifetime without an epoch or downgrade", async () => {
  for (const selected of ["durable-v2", profile]) {
    let connects = 0;
    const f = setup(body => body.operation === "connect" ? (++connects, connected()) : body.operation === "resolve" ? reply({ kind: "resolved", endpoint })
      : Response.json({ ok: false, profile: selected, error: { code: "RELAY_PROFILE_REQUIRED", retryable: false } }, { status: 409 }));
    try {
      const core = await f.session.connect();
      await expect(core.createTask({ target: endpoint, task: "preserve identity", timeoutMs: 60_000 })).rejects.toMatchObject({ code: "RELAY_RESET", retryable: false });
      expect(f.session.status().state).toBe("reset"); expect(f.store.getRelayTransportBinding()).toMatchObject({ epoch, endpoint, reset: true });
      expect(f.store.outbox("pending")).toEqual([]); expect(f.store.outbox("accepted")).toEqual([]); expect(f.store.quarantinedOutbox()).toHaveLength(1);
      expect(core.listTasks()).toHaveLength(1);
      const reopened = createVolatileTaskSession(f.options), before = f.calls();
      try { await expect(reopened.connect()).rejects.toMatchObject({ code: "RELAY_REBIND_REQUIRED", retryable: false }); expect(f.calls()).toBe(before); }
      finally { reopened.close(); }
      expect(connects).toBe(1);
    } finally { f.close(); }
  }
  for (const selected of ["durable-v2", profile]) {
    const unbound = setup(() => Response.json({ ok: false, profile: selected, error: { code: "RELAY_PROFILE_REQUIRED", retryable: false } }, { status: 409 }));
    try { await expect(unbound.session.connect()).rejects.toMatchObject({ code: "RELAY_PROFILE_REQUIRED", retryable: false }); expect(unbound.store.getRelayTransportBinding()).toBeUndefined(); }
    finally { unbound.close(); }
  }
});

test("bounded pre-admission epochless errors retain their codes but never authorize success or arbitrary errors", async () => {
  for (const [code, retryable] of [["RELAY_CAPACITY", true], ["RELAY_UNAVAILABLE", true], ["INVALID_REQUEST", false], ["PEER_POLICY_REQUIRED", false]] as const) {
    const f = setup(body => body.operation === "connect" ? connected() : Response.json({ ok: false, profile, error: { code, retryable: false } }, { status: 503 }));
    try {
      const core = await f.session.connect();
      await expect(core.receive()).rejects.toMatchObject({ code, retryable, ...(code === "RELAY_UNAVAILABLE" && { details: { mayHaveBeenDelivered: true } }) });
      expect(f.store.getReceiveCursor()).toBe("0"); expect(f.session.status().state).toBe("ready");
    } finally { f.close(); }
  }
  for (const invalid of [
    { ok: false, profile, error: { code: "UNRECOGNIZED", retryable: false } },
    { ok: false, profile, epoch: "malformed", error: { code: "RELAY_CAPACITY", retryable: true } },
    { ok: true, profile, value: { kind: "page", deliveries: [], nextCursor: "0", hasMore: false } },
  ]) {
    const f = setup(body => body.operation === "connect" ? connected() : Response.json(invalid));
    try { const core = await f.session.connect(); await expect(core.receive()).rejects.toMatchObject({ code: "INVALID_RESPONSE", retryable: true }); expect(f.store.getReceiveCursor()).toBe("0"); }
    finally { f.close(); }
  }
});
