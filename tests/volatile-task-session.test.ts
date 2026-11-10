import { expect, test } from "bun:test";
import { createTaskStore } from "../src/task-store";
import { createVolatileTaskSession, VOLATILE_PROFILE, type VolatileTaskSessionOptions } from "../src/volatile-task-session";
import type { ChildEventIntake } from "../src/child-events";
import { TASK_PROTOCOL_VERSION } from "../src/task-protocol";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const epoch = "00000000-0000-4000-8000-000000000001";
const otherEpoch = "00000000-0000-4000-8000-000000000002";
const endpoint = { relay: "wolfpack-pi-tasks-v2", id: "00000000-0000-4000-8000-000000000003" };
const nextEndpoint = { ...endpoint, id: "00000000-0000-4000-8000-000000000004" };
const reply = (value: unknown, selectedEpoch = epoch) => Response.json({ ok: true, profile: VOLATILE_PROFILE, epoch: selectedEpoch, value });
const connected = (ep = endpoint, selectedEpoch = epoch) => reply({ kind: "connected", endpoint: ep, leaseExpiresAt: new Date(Date.now() + 60_000).toISOString() }, selectedEpoch);
function fixture(handler: (body: any) => Response | Promise<Response>, timeout = 1000, extra: Partial<VolatileTaskSessionOptions> = {}) {
  const store = createTaskStore();
  const requests: any[] = [];
  const fetcher = Object.assign(async (_input: Parameters<typeof fetch>[0], init?: RequestInit) => { const body = JSON.parse(String(init?.body)); requests.push(body); return handler(body); }, { preconnect: fetch.preconnect }) as typeof fetch;
  const session = createVolatileTaskSession({ url: "http://127.0.0.1:1/staged", callerSession: "fixture", store, fetch: fetcher, requestTimeoutMs: timeout, ...extra });
  return { store, session, requests, fetcher, close: () => { session.close(); store.close(); } };
}
function assignment(cursor: string) {
  const source = { ...endpoint, id: "00000000-0000-4000-8000-000000000005" };
  const taskId = `task-${cursor}`;
  return { cursor, envelope: { envelopeId: `envelope-${cursor}`, protocolVersion: 2, source, target: endpoint, createdAt: new Date().toISOString(), payload: { taskId, kind: "assignment", payload: {
    task: { taskId, protocolVersion: TASK_PROTOCOL_VERSION, origin: source, target: endpoint, task: "synthetic", createdAt: 1, expiresAt: 2, status: "active" },
    event: { eventId: `event-${cursor}`, taskId, type: "task.created", sequence: "1", source, target: endpoint, occurredAt: 1, payload: { task: "synthetic" } },
  } } } };
}

test("negotiates and persists scope, keeps true sparse bigint cursors, and ACKs actual IDs", async () => {
  const cursors = ["9007199254740993", "99999999999999999999999999999999"];
  // Intake follows `hasMore` from the intake cursor; the relay's next page is empty.
  const f = fixture(body => body.operation === "connect" ? connected() : body.operation === "receive"
    ? reply(body.cursor === "0" ? { kind: "page", deliveries: cursors.map(assignment), nextCursor: cursors[1], hasMore: true } : { kind: "page", deliveries: [], nextCursor: body.cursor, hasMore: false })
    : reply({ kind: "acknowledged", duplicate: false }));
  try {
    const core = await f.session.connect();
    expect(Object.isFrozen(core.endpoint)).toBe(true);
    expect(f.store.getRelayTransportBinding()).toMatchObject({ profile: VOLATILE_PROFILE, epoch, endpoint });
    const page = await core.receive();
    expect(page.map(item => item.kind === "relay" ? item.delivery.cursor : undefined)).toEqual(cursors);
    expect(f.requests[1]).toMatchObject({ epoch, endpoint, operation: "receive", cursor: "0", limit: 50 });
    expect(f.requests[2]).toMatchObject({ epoch, endpoint, operation: "receive", cursor: cursors[1], limit: 50 });
    await core.acknowledgeRelayDelivery(cursors[1]!);
    expect(f.store.getReceiveCursor()).toBe("0");
    expect(f.requests.at(-1).envelopeId).toBe(`envelope-${cursors[1]}`);
    await core.acknowledgeRelayDelivery(cursors[0]!);
    expect(f.store.getReceiveCursor()).toBe(cursors[1]!);
  } finally { f.close(); }
});

test("rejects duplicate, out-of-order, truncated, or mismatched page cursors before checkpoint writes", async () => {
  const invalidPages = [
    { deliveries: [assignment("2"), assignment("2")], nextCursor: "2", hasMore: false },
    { deliveries: [assignment("3"), assignment("2")], nextCursor: "2", hasMore: false },
    { deliveries: [assignment("2")], nextCursor: "9", hasMore: true },
    { deliveries: [], nextCursor: "0", hasMore: true },
    { deliveries: Array.from({ length: 51 }, (_, i) => assignment(String(i + 1))), nextCursor: "51", hasMore: false },
  ];
  for (const page of invalidPages) {
    const f = fixture(body => body.operation === "connect" ? connected() : reply({ kind: "page", ...page }));
    try {
      const core = await f.session.connect();
      await expect(core.receive()).rejects.toMatchObject({ code: "INVALID_INBOX" });
      expect(f.store.getReceiveCursor()).toBe("0"); expect(f.store.listTasks()).toEqual([]);
    } finally { f.close(); }
  }
});

test("reset latches in RAM; explicit rebind discards old state without adopting task authority", async () => {
  let reset = false;
  const f = fixture(body => {
    if (body.operation === "connect") return connected(reset ? nextEndpoint : endpoint, reset ? otherEpoch : epoch);
    if (body.operation === "resolve") return reply({ kind: "resolved", endpoint: body.target });
    reset = true; return reply({ kind: "accepted", envelopeId: body.envelope.envelopeId, acceptanceId: epoch, duplicate: false, forwarding: "local" }, otherEpoch);
  });
  try {
    const old = await f.session.connect();
    await expect(old.createTask({ target: endpoint, task: "possibly accepted", timeoutMs: 60_000 })).rejects.toMatchObject({ code: "RELAY_RESET" });
    expect(f.session.status().state).toBe("reset");
    expect(f.store.getRelayTransportBinding()?.reset).toBe(true);
    const blocked = f.store.quarantinedOutbox()[0]!;
    expect(blocked).toMatchObject({ errorCode: "RELAY_RESET", details: { mayHaveBeenDelivered: true }, priorState: "pending" });
    const count = f.requests.length;
    await expect(f.session.connect()).rejects.toMatchObject({ code: "RELAY_RESET" });
    expect(f.requests).toHaveLength(count);
    const rebound = await f.session.rebind();
    expect(rebound.endpoint).toEqual(nextEndpoint);
    expect(f.store.getReceiveCursor()).toBe("0");
    expect(f.store.quarantinedOutbox()).toEqual([]); expect(rebound.listTasks()).toEqual([]);
    expect(f.requests.at(-1).epoch).toBeUndefined();
    // Neither retained old handles nor a new endpoint may mutate old task authority.
    const before = JSON.stringify(f.store.getTask(blocked.envelope.taskId));
    await expect(old.submitIntent({ taskId: blocked.envelope.taskId, type: "task.cancelled", payload: {} })).rejects.toMatchObject({ code: "RELAY_RESET" });
    await expect(rebound.submitIntent({ taskId: blocked.envelope.taskId, type: "task.cancelled", payload: {} })).rejects.toMatchObject({ code: "UNKNOWN_TASK" });
    expect(JSON.stringify(f.store.getTask(blocked.envelope.taskId))).toBe(before);
  } finally { f.close(); }
});

test("terminal unconfirmed response retains unknown-outcome evidence for this lifetime", async () => {
  const f = fixture(body => body.operation === "connect" ? connected() : body.operation === "resolve" ? reply({ kind: "resolved", endpoint: body.target })
    : Response.json({ ok: false, profile: VOLATILE_PROFILE, epoch, error: { code: "DELIVERY_UNCONFIRMED", retryable: false, mayHaveBeenDelivered: true } }));
  try {
    const core = await f.session.connect();
    await expect(core.createTask({ target: endpoint, task: "unconfirmed", timeoutMs: 60_000 })).rejects.toMatchObject({ code: "DELIVERY_UNCONFIRMED", retryable: false, details: { mayHaveBeenDelivered: true } });
    expect(f.store.outbox("accepted")).toEqual([]);
    expect(f.store.quarantinedOutbox()).toHaveLength(1);
    const count = f.requests.length; await core.flushOutbox(); expect(f.requests).toHaveLength(count);
    expect(core.listTasks()[0]!.status).toBe("active");
  } finally { f.close(); }
});

test("pending confirmations, absent profile, and oversized responses cannot become acceptance", async () => {
  const f = fixture(body => body.operation === "connect" ? connected() : body.operation === "resolve" ? reply({ kind: "resolved", endpoint: body.target })
    : reply({ kind: "accepted", envelopeId: body.envelope.envelopeId, acceptanceId: epoch, duplicate: false, forwarding: "pending" }));
  try {
    const core = await f.session.connect();
    await expect(core.createTask({ target: endpoint, task: "not confirmed", timeoutMs: 60_000 })).rejects.toMatchObject({ code: "INVALID_ACCEPTANCE" });
    expect(f.store.outbox("pending")).toHaveLength(1); expect(f.store.outbox("accepted")).toEqual([]);
  } finally { f.close(); }
  for (const response of [Response.json({ ok: true, endpoint }), new Response("x".repeat(301 * 1024))]) {
    const bad = fixture(() => response);
    try { await expect(bad.session.connect()).rejects.toBeDefined(); expect(bad.store.getRelayTransportBinding()).toBeUndefined(); }
    finally { bad.close(); }
  }
});

test("explicit rebind fences an in-flight old send and its late acceptance", async () => {
  let connects = 0;
  let release!: (value: Response) => void;
  let started!: () => void;
  const sending = new Promise<void>(resolve => { started = resolve; });
  let sent: any;
  const f = fixture(body => {
    if (body.operation === "connect") return ++connects === 1 ? connected() : connected(nextEndpoint, otherEpoch);
    if (body.operation === "resolve") return reply({ kind: "resolved", endpoint: body.target });
    sent = body; started(); return new Promise<Response>(resolve => { release = resolve; });
  });
  try {
    const old = await f.session.connect();
    const operation = old.createTask({ target: endpoint, task: "in flight", timeoutMs: 60_000 });
    // Attach a rejection handler without entering Bun's async matcher before
    // the concurrent rebind has actually been issued.
    const settled = operation.then(() => undefined, error => error);
    await sending;
    const current = await f.session.rebind();
    expect(await settled).toMatchObject({ code: "RELAY_RESET" });
    expect(current.endpoint).toEqual(nextEndpoint);
    release(reply({ kind: "accepted", envelopeId: sent.envelope.envelopeId, acceptanceId: epoch, duplicate: false, forwarding: "local" }));
    await new Promise(resolve => setTimeout(resolve, 10));
    expect(f.store.outbox("accepted")).toEqual([]);
    expect(f.store.quarantinedOutbox()).toEqual([]);
    expect(f.store.getRelayTransportBinding()).toMatchObject({ epoch: otherEpoch, endpoint: nextEndpoint });
    expect(f.store.getReceiveCursor()).toBe("0");
    expect(f.session.status().state).toBe("ready");
  } finally { f.close(); }
});

test("a late reset from another controller cannot quarantine a successor's pending work", async () => {
  let connects = 0, release!: (value: Response) => void, started!: () => void;
  const sending = new Promise<void>(resolve => { started = resolve; });
  const f = fixture(body => {
    if (body.operation === "connect") return ++connects === 1 ? connected() : connected(nextEndpoint, otherEpoch);
    const isNew = body.endpoint.id === nextEndpoint.id;
    if (body.operation === "resolve") return reply({ kind: "resolved", endpoint: body.target }, isNew ? otherEpoch : epoch);
    if (isNew) return Response.json({ ok: false, profile: VOLATILE_PROFILE, epoch: otherEpoch, error: { code: "PEER_UNREACHABLE", retryable: true } });
    started(); return new Promise<Response>(resolve => { release = resolve; });
  });
  const other = createVolatileTaskSession({ url: "http://127.0.0.1:1/staged", callerSession: "fixture", store: f.store, fetch: f.fetcher });
  try {
    const old = await f.session.connect();
    const outcome = old.createTask({ target: endpoint, task: "old request", timeoutMs: 60_000 }).then(() => undefined, error => error);
    await sending;
    const oldTaskId = old.listTasks()[0]!.taskId;
    const current = await other.rebind();
    await expect(current.createTask({ target: nextEndpoint, task: "new pending", timeoutMs: 60_000 })).rejects.toMatchObject({ code: "PEER_UNREACHABLE" });
    release(reply({ kind: "ignored" }, otherEpoch));
    expect(await outcome).toMatchObject({ code: "RELAY_RESET" });
    expect(f.store.getRelayTransportBinding()).toMatchObject({ epoch: otherEpoch, endpoint: nextEndpoint });
    expect(f.store.getRelayTransportBinding()?.reset).toBeUndefined();
    expect(f.store.outbox("pending")).toHaveLength(1);
    expect(f.store.outbox("pending")[0]!.envelope.source).toEqual(nextEndpoint);
    expect(f.store.quarantinedOutbox()).toEqual([]);
    await expect(old.submitIntent({ taskId: oldTaskId, type: "task.cancelled", payload: {} })).rejects.toMatchObject({ code: "RELAY_RESET" });
    expect(other.status().state).toBe("ready");
  } finally { other.close(); f.close(); }
});

test("a late initial handshake cannot overwrite another controller's installed binding", async () => {
  let connects = 0, release!: (value: Response) => void, started!: () => void;
  const starting = new Promise<void>(resolve => { started = resolve; });
  const f = fixture(() => ++connects === 1 ? (started(), new Promise<Response>(resolve => { release = resolve; })) : connected(nextEndpoint, otherEpoch));
  const other = createVolatileTaskSession({ url: "http://127.0.0.1:1/staged", callerSession: "fixture", store: f.store, fetch: f.fetcher });
  try {
    const first = f.session.connect().then(() => undefined, error => error);
    await starting; await other.rebind();
    release(connected());
    expect(await first).toMatchObject({ code: "RELAY_RESET" });
    expect(f.store.getRelayTransportBinding()).toMatchObject({ epoch: otherEpoch, endpoint: nextEndpoint });
    expect(f.store.getRelayTransportBinding()?.reset).toBeUndefined();
  } finally { other.close(); f.close(); }
});

test("fresh endpoint memory cannot inherit a prior binding or cursor", () => {
  const prior = createTaskStore(); prior.setEndpointBinding(endpoint); prior.setReceiveCursor("42");
  prior.setRelayTransportBinding({ profile: VOLATILE_PROFILE, epoch, endpoint, generation: "g", callerSession: "fixture", url: "http://127.0.0.1:1/staged" });
  prior.close();
  const fresh = createTaskStore();
  expect(fresh.getEndpointBinding()).toBeUndefined(); expect(fresh.getRelayTransportBinding()).toBeUndefined(); expect(fresh.getReceiveCursor()).toBe("0");
  fresh.close();
});

test("cancellation before explicit rebind does not retire a healthy binding", async () => {
  const f = fixture(() => connected());
  try {
    await f.session.connect(); const before = f.store.getRelayTransportBinding();
    const abort = new AbortController(); abort.abort();
    await expect(f.session.rebind(abort.signal)).rejects.toMatchObject({ code: "ABORTED" });
    expect(f.session.status().state).toBe("ready"); expect(f.store.getRelayTransportBinding()).toEqual(before);
    expect(f.requests).toHaveLength(1);
  } finally { f.close(); }
});

test("bounds ignored aborts and streamed bodies; a late handshake cannot install a binding", async () => {
  let release!: (value: Response) => void;
  const f = fixture(() => new Promise<Response>(resolve => { release = resolve; }), 10);
  try {
    await expect(f.session.connect()).rejects.toMatchObject({ code: "RELAY_TIMEOUT" });
    release(connected()); await new Promise(resolve => setTimeout(resolve, 10));
    expect(f.store.getRelayTransportBinding()).toBeUndefined();
  } finally { f.close(); }
  const streamed = fixture(() => new Response(new ReadableStream({ start(controller) { controller.enqueue(new TextEncoder().encode('{"ok":')); } })), 10);
  try { await expect(streamed.session.connect()).rejects.toMatchObject({ code: "RELAY_TIMEOUT" }); }
  finally { streamed.close(); }
});

async function until(condition: () => boolean): Promise<void> {
  for (let i = 0; i < 500 && !condition(); i++) await new Promise(resolve => setTimeout(resolve, 1));
  expect(condition()).toBe(true);
}

test("in-flight request cap queues the ninth request instead of failing locally, and aborted waiters leak no slot", async () => {
  let gate = Promise.withResolvers<void>();
  const f = fixture(async body => {
    if (body.operation === "connect") return connected();
    if (body.operation === "resolve") { await gate.promise; return reply({ kind: "resolved", endpoint: body.target }); }
    return reply({ kind: "accepted", envelopeId: body.envelope.envelopeId, acceptanceId: epoch, duplicate: false, forwarding: "local" });
  });
  const resolves = () => f.requests.filter(body => body.operation === "resolve").length;
  try {
    const core = await f.session.connect();
    const create = (signal?: AbortSignal) => core.createTask({ target: endpoint, task: "queued", timeoutMs: 60_000 }, signal);
    const first = Array.from({ length: 9 }, () => create());
    await until(() => resolves() === 8);
    await Bun.sleep(5);
    expect(resolves()).toBe(8); // the ninth waits for a slot rather than reaching fetch
    gate.resolve();
    expect((await Promise.all(first)).length).toBe(9); // the ninth completes once a slot frees

    gate = Promise.withResolvers<void>();
    const held = Array.from({ length: 8 }, () => create());
    await until(() => resolves() === 17);
    const controller = new AbortController();
    const waiting = create(controller.signal);
    await Bun.sleep(5);
    expect(resolves()).toBe(17);
    controller.abort();
    await expect(waiting).rejects.toMatchObject({ code: "ABORTED" });
    gate.resolve(); await Promise.all(held);

    gate = Promise.withResolvers<void>();
    const again = Array.from({ length: 9 }, () => create());
    await until(() => resolves() === 25); // all eight slots are free again after the aborted wait
    await Bun.sleep(5);
    expect(resolves()).toBe(25);
    gate.resolve(); await Promise.all(again);
  } finally { gate.resolve(); f.close(); }
});

test("transport failures name the relay operation for the status line", async () => {
  const f = fixture(body => body.operation === "connect" ? connected() : body.operation === "resolve" ? reply({ kind: "resolved", endpoint: body.target })
    : body.operation === "send" ? Response.json({ ok: false, profile: VOLATILE_PROFILE, epoch, error: { code: "RELAY_CAPACITY", retryable: true, retryAfterMs: 10 } })
    : new Response("not json"));
  try {
    const core = await f.session.connect();
    await expect(core.createTask({ target: endpoint, task: "busy", timeoutMs: 60_000 })).rejects.toMatchObject({ code: "RELAY_CAPACITY", retryable: true, details: { operation: "send", retryAfterMs: 10 } });
    // A malformed reply surfaces as the catch-all transport failure, still naming its operation.
    await expect(core.receive()).rejects.toMatchObject({ details: { operation: "receive" } });
  } finally { f.close(); }
});

const hostSource = { ...endpoint, id: "00000000-0000-4000-8000-000000000006" };
function childEvent(cursor: string, payload: Record<string, unknown> = {}) {
  const eventId = `child-1:boot-1:${cursor}`;
  return { cursor, envelope: { envelopeId: `wolfpack.child_event:${eventId}`, protocolVersion: 2, source: hostSource, target: endpoint, createdAt: new Date().toISOString(), payload: {
    kind: "wolfpack.child_event/v1", eventId, bootId: "boot-1", seq: Number(cursor), childSessionId: "child-1", childName: "worker", parentSessionId: "parent-1",
    event: "done", state: "done", observedAt: new Date().toISOString(), futureField: { ignored: true }, ...payload,
  } } };
}
/** Server mailbox semantics: items after the cursor, an ACK removes the item for good. */
function mailbox(items: Array<{ cursor: string; envelope: { envelopeId: string } }>, pageSize = 50, failAcks = 0) {
  const acks: string[] = [];
  const handler = (body: any): Response => {
    if (body.operation === "connect") return connected();
    if (body.operation === "acknowledge") {
      acks.push(body.envelopeId);
      if (failAcks > 0) { failAcks--; return Response.json({ ok: false, profile: VOLATILE_PROFILE, epoch, error: { code: "RELAY_UNAVAILABLE", retryable: true } }); }
      const index = items.findIndex(item => item.envelope.envelopeId === body.envelopeId);
      if (index >= 0) items.splice(index, 1);
      return reply({ kind: "acknowledged", duplicate: index < 0 });
    }
    const after = items.filter(item => BigInt(item.cursor) > BigInt(body.cursor));
    const page = after.slice(0, Math.min(pageSize, body.limit));
    return reply({ kind: "page", deliveries: page, nextCursor: page.at(-1)?.cursor ?? body.cursor, hasMore: after.length > page.length });
  };
  return { handler, acks };
}
const relayCursors = (items: readonly { kind: string; delivery?: { cursor: string } }[]) => items.map(item => item.kind === "relay" ? item.delivery!.cursor : undefined);

test("advertises the child-event capability only to a consumer", async () => {
  const consumer = fixture(body => connected(), 1000, { onChildEvent: () => undefined });
  const plain = fixture(body => connected());
  try {
    await consumer.session.connect(); await plain.session.connect();
    expect(consumer.requests[0]).toMatchObject({ operation: "connect", protocolVersions: [2, 1001] });
    expect(plain.requests[0]).toMatchObject({ operation: "connect", protocolVersions: [2] });
  } finally { consumer.close(); plain.close(); }
});

test("a child event beside task mail never enters task core and is consumed once", async () => {
  for (const order of [["1", "2", "3"], ["3", "1", "2"]]) {
    // Child event first, between, and last on the page: the task page contract must still hold.
    const items = order.map((cursor, index) => index === 1 ? assignment(cursor) : childEvent(cursor)).sort((a, b) => Number(a.cursor) - Number(b.cursor));
    const childIds = items.filter(item => item.envelope.envelopeId.startsWith("wolfpack.child_event:")).map(item => item.envelope.envelopeId);
    const box = mailbox(items);
    const seen: ChildEventIntake[] = [];
    const f = fixture(box.handler, 1000, { onChildEvent: intake => seen.push(intake) });
    try {
      const core = await f.session.connect();
      const page = await core.receive();
      expect(relayCursors(page)).toEqual([order[1]]);
      expect(seen.map(intake => intake.kind)).toEqual(["event", "event"]);
      const first = seen[0]!;
      if (first.kind !== "event") throw new Error("expected an event");
      expect(first.event).toMatchObject({ childName: "worker", event: "done", state: "done", childSessionId: "child-1" });
      expect(first.event).not.toHaveProperty("futureField");
      expect(box.acks.sort()).toEqual(childIds.sort());
      expect(f.store.listTasks()).toHaveLength(1);
    } finally { f.close(); }
  }
});

test("a page of only child events with more pending does not break task intake", async () => {
  const items = Array.from({ length: 5 }, (_, index) => childEvent(String(index + 1)));
  items.push(assignment("6") as never);
  const box = mailbox(items, 2);
  const seen: ChildEventIntake[] = [];
  const f = fixture(box.handler, 1000, { onChildEvent: intake => seen.push(intake) });
  try {
    const core = await f.session.connect();
    const page = [...await core.receive(), ...await core.receive()];
    expect(relayCursors(page)).toContain("6");
    expect(seen).toHaveLength(5);
  } finally { f.close(); }
});

test("a malformed child event is dropped with a diagnostic and the rest of the page continues", async () => {
  const items = [childEvent("1", { event: "exploded", childName: 7 }), assignment("2"), childEvent("3")];
  const childIds = [items[0]!.envelope.envelopeId, items[2]!.envelope.envelopeId];
  const box = mailbox(items);
  const seen: ChildEventIntake[] = [];
  const f = fixture(box.handler, 1000, { onChildEvent: intake => seen.push(intake) });
  try {
    const core = await f.session.connect();
    expect(relayCursors(await core.receive())).toEqual(["2"]);
    expect(seen.map(intake => intake.kind)).toEqual(["malformed", "event"]);
    expect(box.acks).toEqual(childIds);
  } finally { f.close(); }
});

test("an unconfirmed child-event ACK retries on redelivery without presenting the event twice", async () => {
  const box = mailbox([childEvent("1")], 50, 1);
  const seen: ChildEventIntake[] = [];
  const f = fixture(box.handler, 1000, { onChildEvent: intake => seen.push(intake) });
  try {
    const core = await f.session.connect();
    expect(await core.receive()).toEqual([]);
    expect(await core.receive()).toEqual([]);
    expect(seen).toHaveLength(1);
    expect(box.acks).toHaveLength(2);
    expect(await core.receive()).toEqual([]);
    expect(box.acks).toHaveLength(2);
  } finally { f.close(); }
});
