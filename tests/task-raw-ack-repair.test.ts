import { expect, test } from "bun:test";
import { createInMemoryTaskRelay } from "../src/in-memory-task-relay";
import { createTaskCore } from "../src/task-core";
import { deliverTaskInbox } from "../src/task-inbox";
import { TaskProtocolError, type RelayDeliveryAck, type RelayEnvelope, type TaskRelay } from "../src/task-protocol";
import { createTaskStore } from "../src/task-store";
import { createTaskWorkScope } from "../src/task-work-scope";
import { virtualTime } from "./virtual-time";

const origin = { relay: "memory", id: "origin" }, peer = { relay: "memory", id: "peer" };

async function fixture() {
  const base: TaskRelay = createInMemoryTaskRelay("memory"), store = createTaskStore(), peerStore = createTaskStore(), time = virtualTime();
  let mode: "fail" | "slow" | "accept" | "capacity" = "fail", blocked: "none" | "full" | "partial" = "none";
  let release!: () => void, started!: () => void, settled!: () => void;
  const gate = new Promise<void>(resolve => { release = resolve; });
  const ackStarted = new Promise<void>(resolve => { started = resolve; });
  const ackSettled = new Promise<void>(resolve => { settled = resolve; });
  const attempts: RelayDeliveryAck[] = [], canonicalAttempts: RelayEnvelope[] = [], pages: { count: number; hasMore: boolean; kinds: string[] }[] = [];
  const relay: TaskRelay = {
    id: base.id, connect: (input, signal) => base.connect(input, signal), resolve: (input, signal) => base.resolve(input, signal),
    send: (input, signal) => {
      if (input.kind === "canonical_event") canonicalAttempts.push(input);
      if (blocked !== "none" && input.kind === "canonical_event" && (blocked === "full" || input.target.id === peer.id)) {
        throw new TaskProtocolError("RELAY_UNAVAILABLE", "required canonical replica unavailable", { retryable: true });
      }
      return base.send(input, signal);
    },
    receive: async (input, signal) => {
      const page = await base.receive(input, signal);
      pages.push({ count: page.deliveries.length, hasMore: page.hasMore, kinds: page.deliveries.map(item => item.envelope.kind) });
      return page;
    },
    acknowledgeDelivery: async (input, signal) => {
      attempts.push(input);
      if (mode === "fail") throw new TaskProtocolError("RELAY_UNAVAILABLE", "raw ACK service unavailable", { retryable: true });
      if (mode === "capacity") throw new TaskProtocolError("RELAY_CAPACITY", "raw ACK capacity unavailable", { retryable: true, details: { retryAfterMs: 10_000 } });
      if (mode === "slow") {
        started(); await gate;
        // Deliberately ignore abort and complete real remote acceptance after the caller yields.
        await base.acknowledgeDelivery(input); settled(); return;
      }
      await base.acknowledgeDelivery(input, signal);
    },
  };
  const core = createTaskCore({ endpoint: origin, store, relay, timers: time.timers });
  const child = createTaskCore({ endpoint: peer, store: peerStore, relay: base });
  await core.connect(); await child.connect();
  const old = await core.createTask({ target: peer, task: "outgoing assignment", timeoutMs: 60_000 });
  await child.receive();
  await child.submitIntent({ taskId: old.taskId, type: "task.question", payload: { message: "original raw update" } });
  await expect(core.receive()).rejects.toMatchObject({ code: "RELAY_UNAVAILABLE" });
  const requested = store.requestedRelayAcknowledgements(origin);
  const originals = store.outbox("accepted").filter(row => row.envelope.kind === "canonical_event").map(row => row.envelope);
  // Each raw intent now yields one canonical replica (to the peer); the origin view is a local presentation.
  expect(requested).toHaveLength(1); expect(originals).toHaveLength(1);
  const originalAck = attempts[0]!;
  // This intent has NOT been canonicalized yet. Staging only the old replay is insufficient.
  await child.submitIntent({ taskId: old.taskId, type: "task.question", payload: { message: "new raw update" } });
  const fresh = await child.createTask({ target: origin, task: "fresh assignment in the same page", timeoutMs: 60_000 });
  const before = pages.length;
  const pending = () => store.requestedRelayAcknowledgements(origin, Number.MAX_SAFE_INTEGER, Number.MAX_SAFE_INTEGER).map(cursor => ({ cursor, envelopeId: store.pendingRelayEnvelopeId(origin, cursor), retryAt: store.relayAcknowledgementRetryAt!(origin, cursor) }));
  const canonical = () => store.outbox("accepted").filter(row => row.envelope.kind === "canonical_event").map(row => row.envelope);
  const retained = () => canonical().filter(envelope => originals.some(oldEnvelope => oldEnvelope.envelopeId === envelope.envelopeId));
  const refresh = (signal?: AbortSignal) => {
    const work = createTaskWorkScope(signal, "phased", time.timers);
    const pi = { sendMessage() { throw new Error("repair failure must stop presentation"); }, appendEntry() { throw new Error("repair failure must stop archive presentation"); } };
    const context = { isIdle: () => false, hasPendingMessages: () => true, sessionManager: { getEntries: () => [] } };
    return { work, run: async () => {
      await core.connect(work.signal, work); await core.evaluateTimeouts(work.signal, work);
      await deliverTaskInbox(pi, core, context, work.signal, work);
    } };
  };
  return { base, store, peerStore, core, time, fresh, requested, originals, originalAck, attempts, canonicalAttempts, pages, before,
    pending, canonical, retained, refresh, gate, ackStarted, ackSettled, release,
    mode(value: typeof mode) { mode = value; }, block(value: typeof blocked) { blocked = value; },
    close() { store.close(); peerStore.close(); } };
}

test("raw ACK replay failure cannot gate initial intake behind a second new raw intent", async () => {
  const f = await fixture();
  try {
    for (let cycle = 0; cycle < 4; cycle++) {
      const refresh = f.refresh();
      await expect(refresh.run()).rejects.toMatchObject({ code: "RELAY_UNAVAILABLE", retryable: true, message: "raw ACK service unavailable" });
      expect(refresh.work.attemptedAcknowledgements.size).toBe(1);
    }
    expect(f.pages.length - f.before).toBe(4);
    // The original raw intent is pending and re-driven from the local inbox; intake fetches only after it.
    expect(f.pages[f.before]).toEqual({ count: 2, hasMore: false, kinds: ["intent", "assignment"] });
    // Newly accepted canonical copies can join subsequent initial pages; none needs read-ahead.
    expect(f.pages.slice(f.before).every(page => page.count < 50 && !page.hasMore)).toBe(true);
    expect(f.retained()).toEqual(f.originals);
    // Only the original subset is immutable: new raw facts legitimately add replicas/ACK requests.
    expect(f.pending()).toContainEqual({ cursor: f.originalAck.cursor, envelopeId: f.originalAck.envelopeId, retryAt: 0 });
    expect(f.store.getReceiveCursor()).toBe("0");
    expect(f.core.getTask(f.fresh.taskId)?.status).toBe("active");
    expect(f.canonical()).toHaveLength(2); expect(f.pending()).toHaveLength(2);
    expect(f.attempts.slice(1).every(attempt => JSON.stringify(attempt) === JSON.stringify(f.originalAck))).toBe(true);
  } finally { f.close(); }
});

for (const cancellation of ["background bound", "caller"] as const) {
  test(`ignored-abort raw replay repair preserves intake and uncertainty on ${cancellation}`, async () => {
    const f = await fixture(), controller = new AbortController(); f.mode("slow");
    const refresh = f.refresh(controller.signal);
    const drive = refresh.run().then(() => undefined, error => error as TaskProtocolError);
    try {
      await f.ackStarted;
      if (cancellation === "caller") {
        expect(f.core.getTask(f.fresh.taskId)?.status).toBe("active");
        controller.abort();
      }
      // The held repair ignores abort: the refresh yields at the 2s background bound, or at once when the caller cancels.
      const outcome = await f.time.run(drive, 3_000);
      expect(outcome).toMatchObject({ code: "ABORTED", details: { mayHaveBeenDelivered: true } });
      expect(f.time.timers.elapsed()).toBe(cancellation === "caller" ? 0 : 2_000);
      expect(f.core.getTask(f.fresh.taskId)?.status).toBe("active");
      expect(f.retained()).toEqual(f.originals); expect(f.canonical()).toHaveLength(2);
      expect(f.pending()).toHaveLength(2); expect(refresh.work.attemptedAcknowledgements.size).toBe(1);
      expect(f.pages.length - f.before).toBe(1);
      expect(f.pending()).toContainEqual({ cursor: f.originalAck.cursor, envelopeId: f.originalAck.envelopeId, retryAt: 0 });
      const pending = f.pending(), canonical = f.canonical();
      f.release(); await f.ackSettled; await Promise.resolve();
      expect(f.store.getReceiveCursor()).toBe("0"); expect(f.pending()).toEqual(pending); expect(f.canonical()).toEqual(canonical);
      const remote = await f.base.receive({ endpoint: origin, cursor: "0", limit: 50 });
      expect(remote.deliveries.some(delivery => delivery.envelope.envelopeId === f.originalAck.envelopeId)).toBe(false);
      f.mode("accept"); await f.refresh().run();
      expect(f.pending()).toEqual([]); expect(f.retained()).toEqual(f.originals);
      // The visible assignment still lacks Pi evidence, even after authorized raw repair recovers.
      const visible = await f.base.receive({ endpoint: origin, cursor: "0", limit: 50 });
      expect(visible.deliveries.some(delivery => delivery.envelope.taskId === f.fresh.taskId && delivery.envelope.kind === "assignment")).toBe(true);
    } finally { controller.abort(); f.release(); await drive; f.close(); }
  });
}

for (const blocked of ["full", "partial"] as const) {
  test(`${blocked} required fanout still withholds the new raw ACK while later intake proceeds`, async () => {
    const f = await fixture(); f.block(blocked);
    try {
      const attempts = f.attempts.length;
      for (let cycle = 0; cycle < 2; cycle++) {
        const before = f.canonicalAttempts.length;
        await expect(f.refresh().run()).rejects.toMatchObject({ code: "RELAY_UNAVAILABLE", retryable: true, message: "required canonical replica unavailable" });
        // Required replicas now run before speculation, reporting their actual refusal.
        // Final speculation must not send the same failed identity again in this scope.
        // With no origin self-replica, "partial" (peer replica blocked) blocks the only required replica, like "full".
        const sent = f.canonicalAttempts.slice(before).map(envelope => envelope.envelopeId);
        expect(sent).toHaveLength(1);
        expect(new Set(sent).size).toBe(sent.length);
      }
      // A refused fanout no longer stops intake of later deliveries or repair of the staged original ACK (one per refresh); it only withholds that intent's ACK.
      expect(f.core.getTask(f.fresh.taskId)?.status).toBe("active");
      expect(f.attempts.slice(attempts).map(ack => ack.cursor)).toEqual([f.originalAck.cursor, f.originalAck.cursor]);
      expect(f.store.requestedRelayAcknowledgements(origin)).toEqual(f.requested);
      expect(f.retained()).toEqual(f.originals); expect(f.store.getReceiveCursor()).toBe("0");
      expect(f.canonical()).toHaveLength(1);
      expect(f.store.outbox("pending")).toHaveLength(1);
    } finally { f.close(); }
  });
}

test("staged raw ACKs share the remaining cycle budget and report pending exhaustion after intake", async () => {
  const f = await fixture(); f.mode("accept");
  try {
    const refresh = f.refresh();
    // Earlier ACK work has consumed 47 slots in this same production scope.
    for (let i = 0; i < 47; i++) expect(refresh.work.claimAcknowledgement(String(1000 + i))).toBe(true);
    await expect(refresh.run()).rejects.toMatchObject({ code: "TASK_DELIVERY_DEFERRED", retryable: true });
    expect(f.core.getTask(f.fresh.taskId)?.status).toBe("active");
    expect(refresh.work.attemptedAcknowledgements.size).toBe(48);
    expect(f.attempts).toHaveLength(2); // Initial refused ACK, then exactly one accepted raw ACK.
    expect(f.pending()).toHaveLength(1); expect(f.canonical()).toHaveLength(2); expect(f.retained()).toEqual(f.originals);
    await f.refresh().run(); expect(f.pending()).toEqual([]);
  } finally { f.close(); }
});

test("raw replay staging preserves ACK cooldown while eligible later raw repair progresses", async () => {
  const f = await fixture(); f.mode("capacity");
  try {
    await expect(f.refresh().run()).rejects.toMatchObject({ code: "RELAY_CAPACITY", retryable: true });
    expect(f.core.getTask(f.fresh.taskId)?.status).toBe("active");
    const retryAt = f.store.relayAcknowledgementRetryAt!(origin, f.originalAck.cursor);
    expect(retryAt).toBe(f.time.now() + 10_000);
    f.mode("accept");
    await expect(f.refresh().run()).rejects.toMatchObject({ code: "TASK_DELIVERY_DEFERRED", retryable: true });
    expect(f.pending()).toEqual([{ cursor: f.originalAck.cursor, envelopeId: f.originalAck.envelopeId, retryAt }]);
    expect(f.attempts).toHaveLength(3); expect(f.attempts[2]!.cursor).not.toBe(f.originalAck.cursor);
    expect(f.retained()).toEqual(f.originals); expect(f.canonical()).toHaveLength(2); expect(f.store.getReceiveCursor()).toBe("0");
  } finally { f.close(); }
});
