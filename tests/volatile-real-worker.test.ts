import { expect, test } from "bun:test";
import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { isAbsolute, join } from "node:path";
import { createTaskStore } from "../src/task-store";
import { createVolatileTaskSession } from "../src/volatile-task-session";
import type { TaskInboxItem } from "../src/task-core";
import type { RelayDelivery } from "../src/task-protocol";

const relayDeliveries = (items: readonly TaskInboxItem[]): RelayDelivery[] => items.flatMap(item => item.kind === "relay" ? [item.delivery] : []);
const wolfpack = process.env.PI_TASKS_WOLFPACK_SOURCE;
const revision = process.env.PI_TASKS_WOLFPACK_REVISION;

// Bun's eager .rejects matcher can starve same-process HTTP/worker callbacks.
// Settle through normal JS await, then assert the identical structured failure.
const failure = (pending: Promise<unknown>): Promise<unknown> => pending.then(() => undefined, (error: unknown) => error);

test.skipIf(!wolfpack)("actual volatile adapter/HTTP/workers: sparse ACKs, peer confirmation loss, same-lifetime reconnect and explicit epoch rebind", async () => {
  expect(isAbsolute(wolfpack!)).toBe(true);
  expect(revision).toMatch(/^[0-9a-f]{40}$/);
  expect(execFileSync("git", ["rev-parse", "HEAD"], { cwd: wolfpack, encoding: "utf8" }).trim()).toBe(revision!);
  expect(execFileSync("git", ["status", "--porcelain", "--untracked-files=no"], { cwd: wolfpack, encoding: "utf8" }).trim()).toBe("");
  const { WorkerRelayGateway } = await import(join(wolfpack!, "src/task-relay/worker-client.ts"));
  const root = mkdtempSync(join(tmpdir(), "tasks-volatile-workers-"));
  const stores: ReturnType<typeof createTaskStore>[] = [];
  const sessions: ReturnType<typeof createVolatileTaskSession>[] = [];
  const servers: ReturnType<typeof Bun.serve>[] = [];
  const workers: Array<{ close(): Promise<void> }> = [];
  const origins = new Map<string, string>();
  const peerFrames: unknown[] = [];
  let loseConfirmation = true, loseAck = true;
  const make = async (name: string) => {
    const origin = `https://${name}.tail123.ts.net`;
    const createWorker = () => new WorkerRelayGateway({ profile: "volatile-v1", root: join(root, name), peerOrigin: origin,
      inspectSession: async (selector: string) => ({ ok: true, session: selector, sessionId: selector, projectPath: root, harness: "pi", alive: true }),
      peerFetch: async (input: Parameters<typeof fetch>[0], init?: RequestInit) => {
        const parsed = new URL(String(input)), loopback = origins.get(parsed.origin);
        if (!loopback) throw new Error("unknown fixture origin");
        return fetch(loopback + parsed.pathname, init);
      },
    });
    let worker = createWorker(); workers.push(worker); await worker.initialize();
    const calls: any[] = [];
    const server = Bun.serve({ hostname: "127.0.0.1", port: 0, maxRequestBodySize: 64 * 1024, async fetch(request) {
      const body = await request.json();
      const path = new URL(request.url).pathname;
      if (path === "/api/task-relay/volatile-v1/peer") {
        // Test-only trusted-peer ingress. No production authentication claim.
        peerFrames.push(structuredClone(body));
        const result = await worker.volatilePeer(body);
        if (loseConfirmation) { loseConfirmation = false; return new Response("confirmation discarded after acceptance", { status: 503 }); }
        return Response.json(result, { status: result.ok ? 200 : 409 });
      }
      if (path !== "/staged") return new Response(null, { status: 404 });
      calls.push(structuredClone(body));
      const result = await worker.volatile(body);
      if (name === "b" && (body as { operation?: string }).operation === "acknowledge" && result.ok && loseAck) {
        loseAck = false; return new Response("ACK confirmation discarded after acceptance", { status: 503 });
      }
      return Response.json(result, { status: result.ok ? 200 : 409 });
    } }); servers.push(server);
    const baseUrl = `http://127.0.0.1:${server.port}`; origins.set(origin, baseUrl);
    return { origin, url: `${baseUrl}/staged`, calls, get worker() { return worker; }, async restart() {
      await worker.close(); worker = createWorker(); workers.push(worker); await worker.initialize();
    } };
  };
  try {
    const a = await make("a"), b = await make("b");
    let aStore = createTaskStore(); stores.push(aStore);
    let bStore = createTaskStore(); stores.push(bStore);
    let aSession = createVolatileTaskSession({ url: a.url, callerSession: "origin", store: aStore }); sessions.push(aSession);
    let bSession = createVolatileTaskSession({ url: b.url, callerSession: "receiver", store: bStore }); sessions.push(bSession);
    let ac = await aSession.connect(); let bc = await bSession.connect();
    const aBinding = aStore.getRelayTransportBinding()!, bBinding = bStore.getRelayTransportBinding()!;
    const resolved = await a.worker.volatileTopology({ operation: "resolvePeer", profile: aBinding.profile, epoch: aBinding.epoch,
      callerSession: "origin", endpoint: aBinding.endpoint, origin: b.origin, peerEpoch: bBinding.epoch, target: bc.endpoint });
    expect(resolved.ok).toBe(true);
    if (!resolved.ok || resolved.value.kind !== "resolved") throw new Error("fixture topology failed");
    const target = resolved.value.endpoint;
    expect(await failure(ac.createTask({ target, task: "first", timeoutMs: 60_000 }))).toMatchObject({ code: "PEER_UNREACHABLE", retryable: true });
    expect(aStore.outbox("pending")).toHaveLength(1); expect(aStore.outbox("accepted")).toEqual([]);
    await aSession.close(); // Reconnect the transport within this RAM-owned endpoint lifetime.
    aSession = createVolatileTaskSession({ url: a.url, callerSession: "origin", store: aStore }); sessions.push(aSession);
    ac = await aSession.connect();
    expect(aStore.getRelayTransportBinding()).toEqual(aBinding);
    await new Promise(resolve => setTimeout(resolve, 1050));
    await ac.flushOutbox();
    expect(peerFrames).toHaveLength(2); expect(peerFrames[1]).toEqual(peerFrames[0]);
    expect(aStore.outbox("pending")).toEqual([]); expect(aStore.outbox("accepted")).toHaveLength(1);
    await ac.createTask({ target, task: "second", timeoutMs: 60_000 });
    await ac.createTask({ target, task: "third", timeoutMs: 60_000 });
    const first = relayDeliveries(await bc.receive()); expect(first.map(d => d.cursor)).toEqual(["1", "2", "3"]);
    expect(await failure(bc.acknowledgeRelayDelivery("2"))).toMatchObject({ code: "RELAY_UNAVAILABLE" });
    expect(bStore.getReceiveCursor()).toBe("0");
    await bSession.close(); // Reconnect the transport within this RAM-owned endpoint lifetime.
    bSession = createVolatileTaskSession({ url: b.url, callerSession: "receiver", store: bStore }); sessions.push(bSession);
    bc = await bSession.connect(); // RAM-owned, epoch-bound ACK intent retries without a RAM map.
    const ackCalls = b.calls.filter(call => call.operation === "acknowledge");
    expect(ackCalls).toHaveLength(2); expect(ackCalls[1]).toEqual(ackCalls[0]);
    expect(bStore.getReceiveCursor()).toBe("0");
    const sparse = relayDeliveries(await bc.receive()); expect(sparse.map(d => d.cursor)).toEqual(["1", "3"]);
    expect(sparse[1]!.envelope.envelopeId).toBe(first[2]!.envelope.envelopeId);
    await bc.acknowledgeRelayDelivery("3");
    await bSession.close(); // Reconnect the transport within this RAM-owned endpoint lifetime.
    bSession = createVolatileTaskSession({ url: b.url, callerSession: "receiver", store: bStore }); sessions.push(bSession);
    bc = await bSession.connect();
    expect(bStore.getRelayTransportBinding()).toEqual(bBinding);
    const pending = relayDeliveries(await bc.receive()); expect(pending.map(d => d.cursor)).toEqual(["1"]);
    await bc.acknowledgeRelayDelivery("1"); expect(bStore.getReceiveCursor()).toBe("3");
    await b.restart();
    expect(await failure(bc.receive())).toMatchObject({ code: "RELAY_RESET", retryable: false });
    expect(bStore.getRelayTransportBinding()?.reset).toBe(true);
    expect(await failure(ac.createTask({ target, task: "old peer lifetime", timeoutMs: 60_000 }))).toMatchObject({ code: "DELIVERY_UNCONFIRMED", retryable: false, details: { mayHaveBeenDelivered: true } });
    expect(aStore.quarantinedOutbox()).toHaveLength(1);
    const attempts = peerFrames.length; await ac.flushOutbox(); expect(peerFrames).toHaveLength(attempts);
    expect(aSession.status().state).toBe("ready"); // Peer failure does not rotate this source.
    await bSession.close(); // Reconnect the transport within this RAM-owned endpoint lifetime.
    bSession = createVolatileTaskSession({ url: b.url, callerSession: "receiver", store: bStore }); sessions.push(bSession);
    const beforeConnect = b.calls.length;
    await expect(bSession.connect()).rejects.toMatchObject({ code: "RELAY_REBIND_REQUIRED" });
    expect(b.calls).toHaveLength(beforeConnect);
    const fresh = await bSession.rebind();
    expect(fresh.endpoint).not.toEqual(bBinding.endpoint);
    expect(bStore.getRelayTransportBinding()?.epoch).not.toBe(bBinding.epoch);
    expect(bStore.getReceiveCursor()).toBe("0");
    expect(fresh.listTasks()).toEqual([]); // History belongs to the Pi session, not a new endpoint.
    await expect(fresh.submitIntent({ taskId: first[0]!.envelope.taskId, type: "task.completed", payload: { summary: "not authorized for retired endpoint" } })).rejects.toMatchObject({ code: "UNKNOWN_TASK" });
    await fresh.createTask({ target: fresh.endpoint, task: "new lifetime", timeoutMs: 60_000 });
    expect(relayDeliveries(await fresh.receive()).map(d => d.cursor)).toEqual(["1"]);
  } finally {
    await Promise.all(sessions.map(session => session.close()));
    for (const store of stores) store.close();
    await Promise.all(servers.map(server => server.stop(true)));
    await Promise.all(workers.map(worker => worker.close()));
    rmSync(root, { recursive: true, force: true });
  }
}, 20_000);
