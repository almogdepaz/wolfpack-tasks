import { TASK_WORK_LIMITS } from "./task-work-scope";
import { TASK_EVENT_TYPES, TERMINAL_INTENT_OPERATION, TaskProtocolError } from "./task-protocol";
import type { RelayTransportBinding, RelayEnvelope, TaskEndpoint, TaskEvent, TaskRecord, TaskSnapshot, TerminalDeliveryState, TerminalTaskIntentType } from "./task-protocol";

export interface TaskStoreOptions {
  /** Lower-only per-endpoint limits; session history, not this store, is the archive. */
  readonly maxEntries?: number;
  readonly maxBytes?: number;
}
export interface OutboxRecord { readonly envelope: RelayEnvelope; readonly state: "pending" | "accepted"; }
export interface OutboxQuarantineInput {
  readonly errorCode: string; readonly reason: string;
  readonly details: Readonly<Record<string, unknown>>; readonly quarantinedAt: number;
}
export interface OutboxQuarantineRecord extends OutboxQuarantineInput { readonly envelope: RelayEnvelope; readonly priorState: OutboxRecord["state"]; }
export interface TaskOperationRecord {
  readonly taskId: string; readonly operation: string; readonly logicalId: string;
  readonly logicalType: string; readonly envelopeIds: readonly string[];
}
export interface PresentationRecord { readonly taskId: string; readonly eventId: string; }
export interface PendingPresentation extends PresentationRecord { readonly order: number; }
export interface TaskStore {
  transaction<T>(operation: () => T): T;
  /** Synchronous hook after each top-level commit or non-transactional write; a throwing listener is isolated and logged. */
  onChange(listener: () => void): () => void;
  putTask(task: Omit<TaskRecord, "events">): void;
  getTask(taskId: string): TaskSnapshot | undefined;
  listTasks(): readonly TaskSnapshot[];
  /** Whether any task of this lifetime is active; indexed, no snapshot is built. */
  hasActiveTasks(): boolean;
  setStatus(taskId: string, status: TaskRecord["status"]): void;
  /** Follows a reassignment; existing envelopes keep their addresses. */
  setTarget(taskId: string, target: TaskEndpoint): void;
  appendEvent(event: TaskEvent): boolean;
  putOutbox(envelope: RelayEnvelope): void;
  outbox(state: OutboxRecord["state"]): readonly OutboxRecord[];
  /** Indexed lookup, fair rotation and cooldown are required store capabilities. */
  getOutboxRecord(envelopeId: string): OutboxRecord | undefined;
  hasPendingOutbox(): boolean;
  /** Rotates fairly but yields only each target's oldest pending envelope (per-target FIFO). */
  selectPendingOutbox(limit: number, now: number, maxInspections?: number): readonly OutboxRecord[];
  /** Older pending envelopes to the same target, oldest first; the envelope may be sent only once this is empty. */
  pendingOutboxAhead(envelopeId: string): readonly string[];
  outboxRetryAt(envelopeId: string): number;
  postponeOutbox(envelopeId: string, until: number): void;
  markOutboxAccepted(envelopeId: string): void;
  quarantineOutbox(envelopeId: string, input: OutboxQuarantineInput): void;
  quarantinedOutbox(): readonly OutboxQuarantineRecord[];
  /** Indexed lookup of one quarantined envelope. */
  getQuarantine(envelopeId: string): OutboxQuarantineRecord | undefined;
  persistInbox(envelope: RelayEnvelope, cursor: string): boolean;
  /** Store arrival order of a persisted inbox envelope; shares the presentation order sequence. */
  inboxOrder(envelopeId: string): number | undefined;
  inboxEnvelope(envelopeId: string): RelayEnvelope | undefined;
  /** Origin-local canonical events awaiting Pi presentation; ids only, events stay in the task log. */
  enqueuePresentation(details: PresentationRecord): void;
  pendingPresentations(): readonly PendingPresentation[];
  completePresentation(eventId: string): void;
  getReceiveCursor(): string;
  setReceiveCursor(cursor: string): void;
  trackRelayDeliveries(endpoint: TaskEndpoint, deliveries: readonly { readonly cursor: string; readonly envelopeId: string }[]): void;
  pendingRelayEnvelopeId(endpoint: TaskEndpoint, cursor: string): string | undefined;
  /** Tracked, unacknowledged deliveries in this endpoint scope, oldest cursor first. */
  pendingRelayDeliveries(endpoint: TaskEndpoint): readonly { readonly cursor: string; readonly envelopeId: string }[];
  /** Highest tracked cursor in this endpoint scope; intake fetches after it. */
  relayIntakeCursor(endpoint: TaskEndpoint): string;
  requestRelayAcknowledgement(endpoint: TaskEndpoint, cursor: string, retryAt?: number): void;
  requestedRelayAcknowledgements(endpoint: TaskEndpoint, limit?: number, now?: number): readonly string[];
  relayAcknowledgementRetryAt(endpoint: TaskEndpoint, cursor: string): number;
  acknowledgeRelayCursor(endpoint: TaskEndpoint, cursor: string): void;
  putIntent(intentId: string, taskId: string, envelopeId: string): void;
  /** Locally submitted intent only; retained through acceptance or quarantine in this RAM lifetime. */
  getIntentEnvelope(intentId: string): RelayEnvelope | undefined;
  putInsertionReceipt(taskId: string, eventId: string): boolean;
  reserveTaskOperation(input: TaskOperationRecord): { readonly created: boolean; readonly record: TaskOperationRecord };
  /** Withdraws a reservation whose envelopes were never offered to the relay, with its pending envelopes and intent row. */
  discardUnsentOperation(taskId: string, operation: string): void;
  getEndpointGeneration(): string | undefined;
  setEndpointGeneration(generation: string): void;
  getEndpointBinding(): TaskEndpoint | undefined;
  setEndpointBinding(endpoint: TaskEndpoint): void;
  getRelayTransportBinding(): RelayTransportBinding | undefined;
  setRelayTransportBinding(binding: RelayTransportBinding): void;
  clear(): void;
  close(): void;
}

const LIMITS = { entries: 16_384, bytes: 32 * 1024 * 1024 };
// `text` is the immutable source of truth and of the byte charge; `value` is its lazily parsed, deep-frozen form.
type Cell = { readonly text: string; readonly bytes: number; readonly order: number; value?: unknown };
type Undo = { readonly name: string; readonly table: Map<string, Cell>; readonly key: string; readonly prior: Cell | undefined };
interface Checkpoint { frontier: string; highWater: string; pending: Record<string, string>; requested: Record<string, number>; }
const freeze = <T>(value: T): T => {
  if (value && typeof value === "object" && !Object.isFrozen(value)) { Object.freeze(value); for (const child of Object.values(value)) freeze(child); }
  return value;
};
const read = <T>(row: Cell): T => (row.value ??= freeze(JSON.parse(row.text))) as T;

/** One endpoint lifetime. No SQLite, files, migrations, startup reads or replay. */
export function createTaskStore(options: TaskStoreOptions = {}): TaskStore {
  if (Object.keys(options).some(key => key !== "maxEntries" && key !== "maxBytes")) throw new TypeError("task state is memory-only; persistence options are unsupported");
  const maxEntries = options.maxEntries ?? LIMITS.entries, maxBytes = options.maxBytes ?? LIMITS.bytes;
  if (!Number.isInteger(maxEntries) || maxEntries < 1 || maxEntries > LIMITS.entries || !Number.isInteger(maxBytes) || maxBytes < 1 || maxBytes > LIMITS.bytes) throw new TypeError("invalid task memory limit");
  const tables = new Map<string, Map<string, Cell>>();
  const frames: Map<string, Undo>[] = [];
  // ID-only retry rotation, bounded by the existing pending outbox; no payload copy.
  const pendingOutbox = new Map<string, number>();
  // Per-target FIFO: pending ids grouped by target; outbox cell order is creation order.
  const pendingByTarget = new Map<string, Set<string>>(), pendingTarget = new Map<string, string>();
  const syncPendingOutbox = (id: string) => {
    const row = tables.get("outbox")?.get(id);
    const record = row && read<OutboxRecord>(row);
    if (record?.state === "pending") {
      if (!pendingOutbox.has(id)) pendingOutbox.set(id, 0);
      if (!pendingTarget.has(id)) {
        const key = pair(record.envelope.target.relay, record.envelope.target.id);
        pendingTarget.set(id, key);
        const ids = pendingByTarget.get(key) ?? new Set<string>(); ids.add(id); pendingByTarget.set(key, ids);
      }
    } else {
      pendingOutbox.delete(id);
      const key = pendingTarget.get(id);
      if (key !== undefined) { pendingTarget.delete(id); const ids = pendingByTarget.get(key)!; ids.delete(id); if (!ids.size) pendingByTarget.delete(key); }
    }
  };
  const outboxOrder = (id: string) => tables.get("outbox")!.get(id)!.order;
  const pendingAhead = (id: string): string[] => {
    const key = pendingTarget.get(id); if (key === undefined) return [];
    const order = outboxOrder(id);
    return [...pendingByTarget.get(key)!].filter(other => outboxOrder(other) < order).sort((a, b) => outboxOrder(a) - outboxOrder(b));
  };
  // Cell-derived indexes, updated with every cell change including rollback; never a second copy of the data.
  const eventsByTask = new Map<string, Set<string>>(), activeTasks = new Set<string>();
  const reindex = (name: string, key: string, before: Cell | undefined, after: Cell | undefined) => {
    if (name === "tasks") {
      if (after && read<TaskRecord>(after).status === "active") activeTasks.add(key); else activeTasks.delete(key);
    } else if (name === "events") {
      if (before) { const taskId = read<TaskEvent>(before).taskId, ids = eventsByTask.get(taskId); ids?.delete(key); if (ids?.size === 0) eventsByTask.delete(taskId); }
      if (after) { const taskId = read<TaskEvent>(after).taskId, ids = eventsByTask.get(taskId) ?? new Set<string>(); ids.add(key); eventsByTask.set(taskId, ids); }
    }
  };
  let bytes = 0, entries = 0, sequence = 0, closed = false;
  const listeners = new Set<() => void>();
  // Listeners run after state is final; a failure is logged, not rethrown, because the write already committed.
  const notify = () => { for (const listener of [...listeners]) { try { listener(); } catch (error) { console.error("pi-tasks: task store change listener failed", error); } } };
  // Top-level writes refresh the pending-outbox index before notifying so listeners see committed state.
  const committed = (name: string, key: string) => { if (frames.length) return; if (name === "outbox") syncPendingOutbox(key); notify(); };
  const remember = (name: string, key: string, target: Map<string, Cell>, prior: Cell | undefined) => {
    const frame = frames.at(-1), identity = JSON.stringify([name, key]);
    if (frame && !frame.has(identity)) frame.set(identity, { name, table: target, key, prior });
  };
  const check = () => { if (closed) throw new TaskProtocolError("RELAY_CLOSED", "task state is closed", { retryable: false }); };
  const table = (name: string) => { check(); let value = tables.get(name); if (!value) { value = new Map(); tables.set(name, value); } return value; };
  const get = <T>(name: string, key: string): T | undefined => { const row = table(name).get(key); return row ? read<T>(row) : undefined; };
  const rows = <T>(name: string): T[] => [...table(name).values()].sort((a, b) => a.order - b.order).map(row => read<T>(row));
  const taskEvents = (taskId: string): TaskEvent[] => { const events = table("events"); return [...eventsByTask.get(taskId) ?? []].map(id => read<TaskEvent>(events.get(id)!)); };
  const put = (name: string, key: string, value: unknown, onlyNew = false): boolean => {
    const target = table(name), prior = target.get(key);
    if (onlyNew && prior) return false;
    const text = JSON.stringify(value), size = Buffer.byteLength(text) + Buffer.byteLength(key) + 64;
    if (bytes - (prior?.bytes ?? 0) + size > maxBytes || (!prior && entries >= maxEntries)) throw new TaskProtocolError("TASK_CAPACITY", "active task state is full", { retryable: false });
    remember(name, key, target, prior);
    const cell = { text, bytes: size, order: prior?.order ?? sequence++ };
    target.set(key, cell); reindex(name, key, prior, cell);
    bytes += size - (prior?.bytes ?? 0); if (!prior) entries++;
    committed(name, key);
    return true;
  };
  const remove = (name: string, key: string) => {
    const target = table(name), prior = target.get(key); if (!prior) return;
    remember(name, key, target, prior);
    target.delete(key); reindex(name, key, prior, undefined); bytes -= prior.bytes; entries--;
    committed(name, key);
  };
  const pair = (a: string, b: string) => JSON.stringify([a, b]);
  const requireTask = (id: string) => { if (!get("tasks", id)) throw new TaskProtocolError("TASK_NOT_FOUND", "task is not active in this endpoint lifetime"); };
  const sortEvents = (a: TaskEvent, b: TaskEvent) => BigInt(a.sequence) < BigInt(b.sequence) ? -1 : BigInt(a.sequence) > BigInt(b.sequence) ? 1 : a.eventId.localeCompare(b.eventId);
  const terminal = (task: Omit<TaskRecord, "events">): TerminalDeliveryState => {
    const record = get<TaskOperationRecord>("operations", pair(task.taskId, TERMINAL_INTENT_OPERATION));
    if (!record) return { state: "not_submitted" };
    const type = TASK_EVENT_TYPES[record.logicalType];
    if (!type?.terminal || !type.intent || record.envelopeIds.length !== 1) throw new Error("terminal operation is malformed");
    const identity = { intentId: record.logicalId, intentType: record.logicalType as TerminalTaskIntentType, envelopeId: record.envelopeIds[0]!, origin: task.origin };
    const outbox = get<OutboxRecord>("outbox", identity.envelopeId);
    if (outbox) return { state: outbox.state, ...identity };
    const blocked = get<OutboxQuarantineRecord>("quarantine", identity.envelopeId);
    if (!blocked) throw new Error("terminal operation has no delivery record");
    return { state: "delivery_blocked", ...identity, blockedAt: blocked.quarantinedAt, error: { code: blocked.errorCode, retryable: false, details: blocked.details } };
  };
  const snapshot = (task: Omit<TaskRecord, "events">, events: TaskEvent[]): TaskSnapshot => ({ ...task, events: events.sort(sortEvents), terminalDelivery: terminal(task) });
  const state = <T>(key: string) => get<T>("state", key);
  const checkpointKey = (endpoint: TaskEndpoint) => {
    const bound = store.getEndpointBinding();
    if (bound && (bound.relay !== endpoint.relay || bound.id !== endpoint.id)) throw new TaskProtocolError("RELAY_RESET", "delivery checkpoint belongs to a retired endpoint", { retryable: false });
    return JSON.stringify([endpoint.relay, endpoint.id, store.getRelayTransportBinding()?.epoch]);
  };
  // A mutable copy: callers edit it before writing it back, and the cached cell value is frozen.
  const checkpoint = (endpoint: TaskEndpoint): Checkpoint => structuredClone(get<Checkpoint>("checkpoints", checkpointKey(endpoint))) ?? { frontier: store.getReceiveCursor(), highWater: store.getReceiveCursor(), pending: {}, requested: {} };
  const writeCheckpoint = (endpoint: TaskEndpoint, value: Checkpoint) => { put("checkpoints", checkpointKey(endpoint), value); };
  const store: TaskStore = {
    transaction<T>(operation: () => T): T {
      check(); const priorBytes = bytes, priorEntries = entries, priorSequence = sequence;
      const frame = new Map<string, Undo>(); frames.push(frame);
      let result: T, topLevelChange = false;
      try {
        result = operation();
        if (result instanceof Promise) throw new TypeError("task transactions must be synchronous");
        const parent = frames.at(-2);
        if (parent) {
          for (const [key, item] of frame) if (!parent.has(key)) parent.set(key, item);
        } else {
          for (const item of frame.values()) if (item.table === tables.get("outbox")) syncPendingOutbox(item.key);
          topLevelChange = frame.size > 0;
        }
      } catch (error) {
        for (const item of [...frame.values()].reverse()) {
          const current = item.table.get(item.key);
          if (item.prior) item.table.set(item.key, item.prior); else item.table.delete(item.key);
          reindex(item.name, item.key, current, item.prior);
        }
        bytes = priorBytes; entries = priorEntries; sequence = priorSequence;
        throw error;
      } finally { frames.pop(); }
      if (topLevelChange) notify();
      return result;
    },
    onChange(listener) { const entry = () => listener(); listeners.add(entry); return () => { listeners.delete(entry); }; },
    putTask(task) { put("tasks", task.taskId, task, true); },
    getTask(id) { const task = get<Omit<TaskRecord, "events">>("tasks", id); return task ? snapshot(task, taskEvents(id)) : undefined; },
    listTasks() {
      return rows<Omit<TaskRecord, "events">>("tasks").sort((a, b) => a.createdAt - b.createdAt || a.taskId.localeCompare(b.taskId)).map(task => snapshot(task, taskEvents(task.taskId)));
    },
    hasActiveTasks() { check(); return activeTasks.size > 0; },
    setStatus(id, status) { const task = get<Omit<TaskRecord, "events">>("tasks", id); if (task) put("tasks", id, { ...task, status }); },
    setTarget(id, target) { const task = get<Omit<TaskRecord, "events">>("tasks", id); if (task) put("tasks", id, { ...task, target }); },
    appendEvent(event) { requireTask(event.taskId); return put("events", event.eventId, event, true); },
    putOutbox(envelope) { put("outbox", envelope.envelopeId, { envelope, state: "pending" }, true); if (!frames.length) syncPendingOutbox(envelope.envelopeId); },
    outbox(status) { return rows<OutboxRecord>("outbox").filter(item => item.state === status); },
    getOutboxRecord(id) { return get<OutboxRecord>("outbox", id); },
    hasPendingOutbox() { check(); return pendingOutbox.size > 0; },
    selectPendingOutbox(limit, now, maxInspections = TASK_WORK_LIMITS.retryInspections) {
      check();
      const selected: OutboxRecord[] = [];
      // Also bound inspection when the queue is cooling down. Rotate even skipped IDs.
      const inspections = Math.min(pendingOutbox.size, TASK_WORK_LIMITS.retryInspections, maxInspections);
      for (let i = 0; i < inspections && selected.length < limit; i++) {
        const [id, retryAt] = pendingOutbox.entries().next().value!;
        pendingOutbox.delete(id); pendingOutbox.set(id, retryAt);
        // Only a target's oldest pending envelope is sendable; inspecting any of its ids offers the head.
        const head = pendingAhead(id)[0] ?? id;
        if (pendingOutbox.get(head)! <= now && !selected.some(record => record.envelope.envelopeId === head)) {
          const record = get<OutboxRecord>("outbox", head);
          if (record?.state === "pending") selected.push(record);
        }
      }
      return selected;
    },
    pendingOutboxAhead(id) { check(); return pendingAhead(id); },
    outboxRetryAt(id) { check(); return pendingOutbox.get(id) ?? 0; },
    postponeOutbox(id, until) { check(); if (pendingOutbox.has(id)) pendingOutbox.set(id, until); },
    markOutboxAccepted(id) { const prior = get<OutboxRecord>("outbox", id); if (prior) put("outbox", id, { ...prior, state: "accepted" }); if (!frames.length) syncPendingOutbox(id); },
    quarantineOutbox(id, input) {
      store.transaction(() => { const prior = get<OutboxRecord>("outbox", id); if (!prior) return;
        put("quarantine", id, { ...input, envelope: prior.envelope, priorState: prior.state }, true); remove("outbox", id);
      });
    },
    quarantinedOutbox() { return rows<OutboxQuarantineRecord>("quarantine"); },
    getQuarantine(id) { return get<OutboxQuarantineRecord>("quarantine", id); },
    persistInbox(envelope, cursor) { return put("inbox", envelope.envelopeId, { envelope, cursor }, true); },
    inboxOrder(id) { return table("inbox").get(id)?.order; },
    inboxEnvelope(id) { return get<{ envelope: RelayEnvelope }>("inbox", id)?.envelope; },
    enqueuePresentation(details) { requireTask(details.taskId); put("presentation", details.eventId, { taskId: details.taskId, eventId: details.eventId }, true); },
    pendingPresentations() { return [...table("presentation").values()].sort((a, b) => a.order - b.order).map(row => ({ ...read<PresentationRecord>(row), order: row.order })); },
    completePresentation(eventId) { remove("presentation", eventId); },
    getReceiveCursor() { return state<string>("cursor") ?? "0"; },
    setReceiveCursor(cursor) { put("state", "cursor", cursor); },
    trackRelayDeliveries(endpoint, deliveries) {
      const value = checkpoint(endpoint), highWater = BigInt(value.highWater);
      for (const delivery of deliveries) {
        const prior = value.pending[delivery.cursor];
        if (prior !== undefined && prior !== delivery.envelopeId) throw new TaskProtocolError("INVALID_CURSOR", "relay cursor changed envelope identity");
        if (BigInt(delivery.cursor) > highWater) value.pending[delivery.cursor] = delivery.envelopeId;
        if (BigInt(delivery.cursor) > BigInt(value.highWater)) value.highWater = delivery.cursor;
      }
      writeCheckpoint(endpoint, value);
    },
    pendingRelayEnvelopeId(endpoint, cursor) { return checkpoint(endpoint).pending[cursor]; },
    pendingRelayDeliveries(endpoint) {
      return Object.entries(checkpoint(endpoint).pending).map(([cursor, envelopeId]) => ({ cursor, envelopeId }))
        .sort((left, right) => BigInt(left.cursor) < BigInt(right.cursor) ? -1 : 1);
    },
    relayIntakeCursor(endpoint) { return checkpoint(endpoint).highWater; },
    requestRelayAcknowledgement(endpoint, cursor, retryAt = 0) {
      const value = checkpoint(endpoint);
      if (value.pending[cursor] === undefined) throw new TaskProtocolError("INVALID_CURSOR", "relay delivery is not pending in this endpoint scope");
      value.requested[cursor] = retryAt; writeCheckpoint(endpoint, value);
    },
    requestedRelayAcknowledgements(endpoint, limit = Number.MAX_SAFE_INTEGER, now = Date.now()) {
      const value = checkpoint(endpoint);
      return Object.keys(value.requested).filter(cursor => value.requested[cursor]! <= now).slice(0, limit);
    },
    relayAcknowledgementRetryAt(endpoint, cursor) { return checkpoint(endpoint).requested[cursor] ?? 0; },
    acknowledgeRelayCursor(endpoint, cursor) {
      store.transaction(() => {
        const value = checkpoint(endpoint); if (value.pending[cursor] === undefined) return;
        delete value.pending[cursor]; delete value.requested[cursor];
        const pending = Object.keys(value.pending);
        if (!pending.length) value.frontier = value.highWater;
        else if (BigInt(cursor) > BigInt(value.frontier) && pending.every(other => BigInt(other) > BigInt(cursor))) value.frontier = cursor;
        writeCheckpoint(endpoint, value); store.setReceiveCursor(value.frontier);
      });
    },
    putIntent(intentId, taskId, envelopeId) { put("intents", intentId, { taskId, envelopeId }, true); },
    getIntentEnvelope(intentId) {
      const intent = get<{ taskId: string; envelopeId: string }>("intents", intentId);
      if (!intent) return undefined;
      const envelope = (get<OutboxRecord>("outbox", intent.envelopeId) ?? get<OutboxQuarantineRecord>("quarantine", intent.envelopeId))?.envelope;
      return envelope?.taskId === intent.taskId ? envelope : undefined;
    },
    putInsertionReceipt(taskId, eventId) { requireTask(taskId); return put("insertions", pair(taskId, eventId), true, true); },
    reserveTaskOperation(input) { requireTask(input.taskId); const key = pair(input.taskId, input.operation); const created = put("operations", key, input, true); return { created, record: get<TaskOperationRecord>("operations", key)! }; },
    discardUnsentOperation(taskId, operation) {
      store.transaction(() => {
        const key = pair(taskId, operation), record = get<TaskOperationRecord>("operations", key); if (!record) return;
        for (const id of record.envelopeIds) {
          if (get<OutboxRecord>("outbox", id)?.state !== "pending") throw new Error("only a pending, never-sent operation can be discarded");
          remove("outbox", id);
        }
        remove("intents", record.logicalId); remove("operations", key);
      });
    },
    getEndpointGeneration() { return state<string>("generation"); },
    setEndpointGeneration(value) { put("state", "generation", value); },
    getEndpointBinding() { return state<TaskEndpoint>("endpoint"); },
    setEndpointBinding(value) { put("state", "endpoint", value); },
    getRelayTransportBinding() { return state<RelayTransportBinding>("transport"); },
    setRelayTransportBinding(value) { put("state", "transport", value); },
    clear() { check(); if (frames.length) throw new Error("cannot clear task state during a transaction"); tables.clear(); eventsByTask.clear(); activeTasks.clear(); pendingOutbox.clear(); pendingByTarget.clear(); pendingTarget.clear(); bytes = entries = sequence = 0; },
    close() { if (closed) return; store.clear(); closed = true; },
  };
  return store;
}
