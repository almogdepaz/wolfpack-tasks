import { createTaskCore } from "./task-core";
import type { TaskCore } from "./task-core";
import { RESET_CODES, RETRY_CODES, taskErrorMessage } from "./task-errors";
import { TASK_PROTOCOL_VERSION, TaskProtocolError } from "./task-protocol";
import type { RelayTransportBinding, TaskEndpoint, TaskRelay } from "./task-protocol";
import type { TaskStore } from "./task-store";
import { fromWolfpackEnvelope, toWolfpackEnvelope, WOLFPACK_TASK_RELAY_ID } from "./wolfpack-task-relay";

export const VOLATILE_PROFILE = "volatile-v1";
export const RESTART_SAFE_PROFILE = "restart-safe-v1";
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const CURSOR = /^(0|[1-9][0-9]{0,31})$/;

export interface VolatileTaskSessionOptions {
  /** Explicit trusted endpoint ingress URL. No default route or installed server. */
  readonly url: string;
  readonly callerSession: string;
  readonly profile?: RelayTransportBinding["profile"];
  /** Caller owns this endpoint's RAM state and closes it after session.close(). */
  readonly store: TaskStore;
  readonly fetch?: typeof fetch;
  readonly requestTimeoutMs?: number;
}
export interface VolatileTaskSession {
  connect(signal?: AbortSignal): Promise<TaskCore>;
  /** Explicitly retire the prior binding; never migrate historical task identities. */
  rebind(signal?: AbortSignal): Promise<TaskCore>;
  status(): { readonly state: "unbound" | "ready" | "reset" | "closed"; readonly binding: RelayTransportBinding | undefined };
  close(): void;
}

/** Epoch-bound transport used by the normal extension; caller owns the store. */
export function createVolatileTaskSession(options: VolatileTaskSessionOptions): VolatileTaskSession {
  const url = new URL(options.url);
  if (url.username || url.password || url.search || url.hash || !(url.protocol === "https:" || (url.protocol === "http:" && ["127.0.0.1", "[::1]", "localhost"].includes(url.hostname)))) throw new TypeError("explicit trusted HTTPS or loopback relay URL required");
  if (!text(options.callerSession)) throw new TypeError("caller session is required");
  const timeoutMs = options.requestTimeoutMs ?? 12_000;
  if (!Number.isInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 15_000) throw new TypeError("invalid relay request deadline");
  const requestFetch = options.fetch ?? fetch, store = options.store, profile = options.profile ?? VOLATILE_PROFILE;
  if (profile === RESTART_SAFE_PROFILE && !UUID.test(options.callerSession)) throw new TypeError("restart-safe tasks require the authoritative session UUID");
  let transport: Transport | undefined, core: TaskCore | undefined, busy = false, closed = false;
  let reset = false;
  const saved = () => store.getRelayTransportBinding();
  const validateSaved = (binding: RelayTransportBinding) => {
    if (binding.profile !== profile || !UUID.test(binding.epoch) || !localEndpoint(binding.endpoint)
      || !text(binding.generation) || typeof binding.url !== "string" || !text(binding.callerSession) || (binding.reset !== undefined && binding.reset !== true)
      || !same(store.getEndpointBinding(), binding.endpoint)) throw failure("INVALID_RELAY_METADATA", false);
  };
  const quarantine = (prior: TaskEndpoint | undefined, code: string) => {
    if (!prior) return;
    for (const record of store.outbox("pending")) if (same(record.envelope.source, prior)) store.quarantineOutbox(record.envelope.envelopeId, {
      errorCode: code, reason: "relay binding retired; delivery outcome may be unknown", details: { priorEndpoint: prior, mayHaveBeenDelivered: true }, quarantinedAt: Date.now(),
    });
  };
  const onReset = (expected: RelayTransportBinding | undefined) => {
    reset = true;
    const binding = saved();
    // Another controller may already have installed a successor. A stale
    // response must never retire or quarantine that successor's work.
    if (binding && matchesBinding(binding, expected)) store.transaction(() => { store.setRelayTransportBinding({ ...binding, reset: true }); quarantine(binding.endpoint, "RELAY_RESET"); });
  };
  const open = async (replace: boolean, signal?: AbortSignal): Promise<TaskCore> => {
    if (closed) throw failure("RELAY_CLOSED", false);
    if (signal?.aborted) throw failure("ABORTED", true);
    if (busy) throw failure("RELAY_BUSY", true);
    if (!replace && core) { transport!.assertStoredBinding(saved()); await core.connect(signal); return core; }
    busy = true;
    try {
      let prior = saved(), priorEndpoint = store.getEndpointBinding();
      if (prior) validateSaved(prior);
      if (!replace && (reset || prior?.reset || (!prior && priorEndpoint) || (prior && (prior.url !== url.href || prior.callerSession !== options.callerSession)))) { reset = true; throw failure("RELAY_REBIND_REQUIRED", false); }
      if (replace && profile === RESTART_SAFE_PROFILE) throw failure("RELAY_RESET", false, { recovery: "journal retained; authoritative binding requires investigation" });
      if (replace) {
        // Explicit acceptance of loss starts empty. Session history is the only
        // archive; old task identities/ACKs/outbox must never enter the new scope.
        transport?.stop(); core = undefined; reset = true;
        store.clear(); prior = undefined; priorEndpoint = undefined;
      }
      const expectedBinding = saved();
      const generation = replace ? crypto.randomUUID() : prior?.generation ?? store.getEndpointGeneration() ?? crypto.randomUUID();
      store.transaction(() => store.setEndpointGeneration(generation));
      const selected = new Transport(url.href, options.callerSession, generation, replace ? undefined : prior, requestFetch, timeoutMs, onReset, profile);
      transport = selected;
      const binding = await selected.register(signal);
      if (closed) throw failure("RELAY_CLOSED", false);
      store.transaction(() => {
        const current = saved(), currentEndpoint = store.getEndpointBinding();
        const unchanged = expectedBinding ? matchesBinding(current, expectedBinding) && current?.reset === expectedBinding.reset : current === undefined;
        if (!unchanged || store.getEndpointGeneration() !== generation || (priorEndpoint ? !same(currentEndpoint, priorEndpoint) : currentEndpoint !== undefined)) {
          selected.stop(); reset = true; throw failure("RELAY_RESET", false);
        }
        if (replace || !prior) store.setReceiveCursor("0");
        store.setEndpointBinding(binding.endpoint); store.setRelayTransportBinding(binding);
      });
      reset = false;
      // Retained old core handles cannot mutate a successor lifetime even if
      // they resume after await. The RAM store remains controller-owned.
      const guard = () => selected.assertStoredBinding(saved());
      const guarded: TaskStore = { ...store, transaction<T>(operation: () => T): T {
        guard();
        return store.transaction(() => { guard(); const value = operation(); guard(); return value; });
      } };
      const relay = selected.relay();
      const guardedRelay: TaskRelay = { id: relay.id,
        connect: (input, signal) => { guard(); return relay.connect(input, signal); },
        resolve: (input, signal) => { guard(); return relay.resolve(input, signal); },
        send: (input, signal) => { guard(); return relay.send(input, signal); },
        receive: (input, signal) => { guard(); return relay.receive(input, signal); },
        acknowledgeDelivery: (input, signal) => { guard(); return relay.acknowledgeDelivery(input, signal); },
      };
      core = createTaskCore({ endpoint: binding.endpoint, relay: guardedRelay, store: guarded });
      await core.connect(signal);
      return core;
    } finally { busy = false; }
  };
  return {
    connect: signal => open(false, signal), rebind: signal => open(true, signal),
    status: () => ({ state: closed ? "closed" : reset || saved()?.reset ? "reset" : core ? "ready" : "unbound", binding: saved() }),
    close() { closed = true; transport?.stop(); },
  };
}

class Transport {
  private readonly stopSignal = new AbortController();
  private binding: RelayTransportBinding | undefined;
  private expiresAt = 0;
  private renewing: Promise<RelayTransportBinding> | undefined;
  private active = 0;
  private readonly waiters: Array<{ reserved: boolean; grant(): void }> = [];
  constructor(private url: string, private caller: string, private generation: string, prior: RelayTransportBinding | undefined,
    private fetcher: typeof fetch, private timeoutMs: number, private onReset: (expected: RelayTransportBinding | undefined) => void, private profile: RelayTransportBinding["profile"]) { this.binding = prior; }
  stop(): void { this.stopSignal.abort(); }
  assertActive(): void { if (this.stopSignal.signal.aborted) throw failure("RELAY_RESET", false, { mayHaveBeenDelivered: true }); }
  assertStoredBinding(binding: RelayTransportBinding | undefined): void {
    this.assertActive();
    if (!matchesBinding(binding, this.binding) || binding?.reset) this.retire();
  }
  private retire(): never { this.stop(); this.onReset(this.binding); throw failure("RELAY_RESET", false, { mayHaveBeenDelivered: true }); }
  async register(signal?: AbortSignal): Promise<RelayTransportBinding> {
    this.assertActive();
    if (this.binding && this.expiresAt - Date.now() > this.timeoutMs + 10_000) return this.binding;
    if (this.renewing) return this.renewing;
    this.renewing = (async () => {
      const reply = await this.request({ operation: "connect", profile: this.profile, callerSession: this.caller, generation: this.generation,
        protocolVersions: [2], leaseMs: 60_000, ...(this.binding && { epoch: this.binding.epoch }) }, signal);
      const value = reply.value;
      if (!record(value) || value.kind !== "connected" || !localEndpoint(value.endpoint) || typeof value.leaseExpiresAt !== "string" || !(Date.parse(value.leaseExpiresAt) > Date.now())) throw failure("INVALID_CONNECTION", true);
      if (this.profile === RESTART_SAFE_PROFILE && value.sessionId !== this.caller) throw failure("SOURCE_MISMATCH", false);
      if (this.binding && !same(this.binding.endpoint, value.endpoint)) this.retire();
      this.binding = Object.freeze({ profile: this.profile, epoch: reply.epoch, endpoint: Object.freeze({ ...value.endpoint }), generation: this.generation, callerSession: this.caller, url: this.url });
      this.expiresAt = Math.min(Date.parse(value.leaseExpiresAt), Date.now() + 60_000);
      return this.binding;
    })();
    try { return await this.renewing; } finally { this.renewing = undefined; }
  }
  private async operation(operation: string, body: Record<string, unknown>, signal?: AbortSignal): Promise<Record<string, unknown>> {
    const binding = await this.register(signal);
    const result = await this.request({ operation, profile: binding.profile, epoch: binding.epoch, endpoint: binding.endpoint, callerSession: this.caller, ...body }, signal);
    if (!record(result.value)) throw failure("INVALID_RESPONSE", true);
    return result.value;
  }
  relay(): TaskRelay {
    const own = (endpoint: TaskEndpoint) => { this.assertActive(); if (!same(endpoint, this.binding?.endpoint)) throw failure("SOURCE_MISMATCH", false); };
    return {
      id: WOLFPACK_TASK_RELAY_ID,
      connect: async (input, signal) => {
        own(input.endpoint);
        if (input.protocolVersion !== TASK_PROTOCOL_VERSION) throw failure("INCOMPATIBLE_PROTOCOL", false);
        await this.register(signal); return { endpoint: input.endpoint, receiveCursor: input.receiveCursor };
      },
      resolve: async (input, signal) => {
        const value = await this.operation("resolve", { target: { relay: input.relay, id: input.reference } }, signal);
        if (value.kind !== "resolved" || !endpoint(value.endpoint) || value.endpoint.relay !== input.relay || value.endpoint.id !== input.reference) throw failure("INVALID_TARGET", true);
        return value.endpoint;
      },
      send: async (input, signal) => {
        own(input.source);
        const envelope = toWolfpackEnvelope(input); // Preserve original persisted wire content.
        const value = await this.operation("send", { envelope }, signal);
        if (value.kind !== "accepted" || value.envelopeId !== input.envelopeId || !uuid(value.acceptanceId) || typeof value.duplicate !== "boolean" || value.forwarding !== (envelope.target.relay === WOLFPACK_TASK_RELAY_ID ? "local" : "forwarded")) throw failure("INVALID_ACCEPTANCE", true);
        return { envelopeId: input.envelopeId };
      },
      receive: async (input, signal) => {
        own(input.endpoint);
        if (!cursor(input.cursor) || !Number.isInteger(input.limit) || input.limit < 1) throw failure("INVALID_CURSOR", false);
        const limit = Math.min(input.limit, 50);
        const value = await this.operation("receive", { cursor: input.cursor, limit }, signal);
        if (value.kind !== "page" || !Array.isArray(value.deliveries) || value.deliveries.length > limit || !cursor(value.nextCursor) || typeof value.hasMore !== "boolean") throw failure("INVALID_INBOX", true);
        let last = BigInt(input.cursor);
        const ids = new Set<string>();
        const deliveries = value.deliveries.map((item: unknown) => {
          if (!record(item) || !cursor(item.cursor) || BigInt(item.cursor) <= last || !record(item.envelope)) throw failure("INVALID_INBOX", true);
          if (!endpoint(item.envelope.source) || !localEndpoint(item.envelope.target)) throw failure("INVALID_INBOX", true);
          const envelope = fromWolfpackEnvelope(item.envelope as unknown as Parameters<typeof fromWolfpackEnvelope>[0]);
          if (!same(envelope.target, input.endpoint) || ids.has(envelope.envelopeId)) throw failure("INVALID_INBOX", true);
          ids.add(envelope.envelopeId); last = BigInt(item.cursor);
          return { cursor: item.cursor, envelope };
        });
        if (value.nextCursor !== (deliveries.at(-1)?.cursor ?? input.cursor) || (!deliveries.length && value.hasMore)) throw failure("INVALID_INBOX", true);
        return { deliveries, nextCursor: value.nextCursor, hasMore: value.hasMore };
      },
      acknowledgeDelivery: async (input, signal) => {
        own(input.endpoint);
        if (!cursor(input.cursor) || !text(input.envelopeId)) throw failure("INVALID_CURSOR", false);
        const value = await this.operation("acknowledge", { envelopeId: input.envelopeId }, signal);
        if (value.kind !== "acknowledged" || typeof value.duplicate !== "boolean") throw failure("INVALID_RESPONSE", true);
      },
    };
  }
  // FIFO in-flight slots: 8 for ordinary requests, 2 more held for acknowledge/connect.
  // A waiter has sent nothing, so its abort is a plain retryable ABORTED; RELAY_CAPACITY
  // only ever comes from the server.
  private fits(reserved: boolean): boolean { return this.active < (reserved ? 10 : 8); }
  private acquire(reserved: boolean, signal?: AbortSignal): Promise<void> {
    if (this.fits(reserved)) { this.active++; return Promise.resolve(); }
    return new Promise((resolve, reject) => {
      const cleanup = () => { signal?.removeEventListener("abort", cancel); this.stopSignal.signal.removeEventListener("abort", cancel); };
      const waiter = { reserved, grant: () => { cleanup(); this.active++; resolve(); } };
      const cancel = () => {
        cleanup(); this.waiters.splice(this.waiters.indexOf(waiter), 1);
        reject(this.stopSignal.signal.aborted ? failure("RELAY_RESET", false, { mayHaveBeenDelivered: true }) : failure("ABORTED", true));
      };
      this.waiters.push(waiter);
      signal?.addEventListener("abort", cancel, { once: true }); this.stopSignal.signal.addEventListener("abort", cancel, { once: true });
      if (signal?.aborted) cancel();
    });
  }
  private release(): void {
    this.active--;
    for (const waiter of [...this.waiters]) if (this.fits(waiter.reserved)) { this.waiters.splice(this.waiters.indexOf(waiter), 1); waiter.grant(); }
  }
  private async request(body: Record<string, unknown>, signal?: AbortSignal): Promise<{ epoch: string; value: unknown }> {
    this.assertActive();
    const reserved = body.operation === "acknowledge" || body.operation === "connect";
    const encoded = JSON.stringify(body);
    if (Buffer.byteLength(encoded) > 64 * 1024) throw failure("PAYLOAD_TOO_LARGE", false);
    await this.acquire(reserved, signal);
    const controller = new AbortController();
    let timedOut = false;
    const abort = () => controller.abort();
    signal?.addEventListener("abort", abort, { once: true }); this.stopSignal.signal.addEventListener("abort", abort, { once: true });
    if (signal?.aborted || this.stopSignal.signal.aborted) abort();
    const timer = setTimeout(() => { timedOut = true; abort(); }, this.timeoutMs);
    const abortError = () => failure(this.stopSignal.signal.aborted ? "RELAY_RESET" : timedOut ? "RELAY_TIMEOUT" : "ABORTED", !this.stopSignal.signal.aborted, { mayHaveBeenDelivered: true });
    let rejectAbort!: () => void;
    const cancelled = new Promise<never>((_, reject) => { rejectAbort = () => reject(abortError()); controller.signal.addEventListener("abort", rejectAbort, { once: true }); if (controller.signal.aborted) rejectAbort(); });
    try {
      const result = await Promise.race([cancelled, (async () => {
        if (controller.signal.aborted) throw abortError();
        const response = await this.fetcher(this.url, { method: "POST", headers: { "content-type": "application/json" }, body: encoded, redirect: "error", signal: controller.signal });
        if (controller.signal.aborted) { void response.body?.cancel().catch(() => undefined); throw abortError(); }
        if (this.profile === RESTART_SAFE_PROFILE && response.status === 404) {
          void response.body?.cancel().catch(() => undefined);
          throw failure("RELAY_PROFILE_REQUIRED", false);
        }
        const rateLimited = response.status === 429;
        if (response.status === 401 || rateLimited) {
          // Admission failures need no relay profile. Best-effort cleanup must not mask their status.
          void response.body?.cancel().catch(() => undefined);
          throw failure(rateLimited ? "RELAY_CAPACITY" : "RELAY_AUTH_REQUIRED", rateLimited);
        }
        const reader = response.body?.getReader();
        if (!reader) throw failure("INVALID_RESPONSE", true);
        const cancel = () => { void reader.cancel().catch(() => undefined); };
        controller.signal.addEventListener("abort", cancel, { once: true });
        const chunks: Uint8Array[] = []; let bytes = 0;
        try {
          while (true) {
            const part = await reader.read();
            if (controller.signal.aborted) throw abortError();
            if (part.done) break;
            bytes += part.value.byteLength;
            if (bytes > 300 * 1024) throw failure("INVALID_RESPONSE", true);
            chunks.push(part.value);
          }
          return { ok: response.ok, value: JSON.parse(Buffer.concat(chunks).toString("utf8")) as unknown };
        } finally { controller.signal.removeEventListener("abort", cancel); cancel(); }
      })()]);
      this.assertActive();
      const data = result.value;
      // An explicit permanent profile refusal retires an existing lifetime even
      // when the selected server engine has no volatile epoch to return.
      if (record(data) && (data.profile === VOLATILE_PROFILE || data.profile === RESTART_SAFE_PROFILE || data.profile === "durable-v2")
        && data.ok === false && record(data.error) && data.error.code === "RELAY_PROFILE_REQUIRED" && data.error.retryable === false) {
        if (this.binding) this.retire();
        throw failure("RELAY_PROFILE_REQUIRED", false);
      }
      if (!record(data) || data.profile !== this.profile) throw failure("RELAY_PROFILE_REQUIRED", false);
      if (this.binding && uuid(data.epoch) && data.epoch !== this.binding.epoch) this.retire();
      if (data.ok === false && record(data.error) && text(data.error.code) && typeof data.error.retryable === "boolean") {
        if (this.profile === RESTART_SAFE_PROFILE && data.error.code === "REGISTRATION_EXPIRED") {
          // No operation was admitted. Renew the same persisted generation on the next pass.
          this.expiresAt = 0;
          throw failure("RELAY_UNAVAILABLE", true);
        }
        if (RESET_CODES.has(data.error.code)) this.retire();
        const epochlessRejection = data.epoch === undefined && ["RELAY_CAPACITY", "RELAY_UNAVAILABLE", "INVALID_REQUEST", "PEER_POLICY_REQUIRED"].includes(data.error.code);
        if ((!uuid(data.epoch) && !epochlessRejection) || (this.binding && uuid(data.epoch) && data.epoch !== this.binding.epoch)) throw failure("INVALID_RESPONSE", true);
        const details = { ...((data.error.mayHaveBeenDelivered === true || data.error.code === "RELAY_UNAVAILABLE") && { mayHaveBeenDelivered: true }),
          ...(typeof data.error.retryAfterMs === "number" && Number.isFinite(data.error.retryAfterMs) && data.error.retryAfterMs >= 0 && { retryAfterMs: data.error.retryAfterMs }) };
        throw failure(data.error.code, data.error.retryable || RETRY_CODES.has(data.error.code), details);
      }
      if (!result.ok || data.ok !== true || !uuid(data.epoch)) throw failure("INVALID_RESPONSE", true);
      return { epoch: data.epoch, value: data.value };
    } catch (error) {
      // Every failure of an admitted request names its relay operation for the status line.
      const known = error instanceof TaskProtocolError ? error : failure("RELAY_UNAVAILABLE", true, { mayHaveBeenDelivered: true });
      throw new TaskProtocolError(known.code, known.message, { retryable: known.retryable, details: { ...known.details, operation: body.operation } });
    } finally {
      clearTimeout(timer); this.release(); signal?.removeEventListener("abort", abort); this.stopSignal.signal.removeEventListener("abort", abort); controller.signal.removeEventListener("abort", rejectAbort);
    }
  }
}
function failure(code: string, retryable: boolean, details?: Record<string, unknown>): TaskProtocolError { return new TaskProtocolError(code, taskErrorMessage(code), { retryable, details }); }
function record(value: unknown): value is Record<string, unknown> { return value !== null && typeof value === "object" && !Array.isArray(value); }
function text(value: unknown): value is string { return typeof value === "string" && value.length > 0 && value.length <= 512; }
function uuid(value: unknown): value is string { return typeof value === "string" && UUID.test(value); }
function endpoint(value: unknown): value is TaskEndpoint { return record(value) && text(value.relay) && uuid(value.id); }
function localEndpoint(value: unknown): value is TaskEndpoint { return endpoint(value) && value.relay === WOLFPACK_TASK_RELAY_ID; }
function same(a: TaskEndpoint | undefined, b: TaskEndpoint | undefined): boolean { return !!a && !!b && a.relay === b.relay && a.id === b.id; }
function matchesBinding(a: RelayTransportBinding | undefined, b: RelayTransportBinding | undefined): boolean {
  return !!a && !!b && a.profile === b.profile && a.epoch === b.epoch && a.generation === b.generation && a.callerSession === b.callerSession && a.url === b.url && same(a.endpoint, b.endpoint);
}
function cursor(value: unknown): value is string { return typeof value === "string" && CURSOR.test(value); }
