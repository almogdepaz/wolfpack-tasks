import {
	MAX_RELAY_PAYLOAD_BYTES,
	ORIGIN_CANCELLATION_OPERATION,
	PARENT_ACKNOWLEDGMENT_OPERATION,
	TASK_EVENT_TYPES,
	TASK_PROTOCOL_VERSION,
	TERMINAL_INTENT_OPERATION,
	TaskDeliveryEvidenceState,
	TaskDeliveryStage,
	TaskEnvelopeKind,
	TaskOutboxDeliveryError,
	TaskProtocolError,
	isBlockedOutboxDeliveryCode,
	taskEventTypes,
} from "./task-protocol";
import type {
	RelayDelivery,
	RelayEnvelope,
	TaskEndpoint,
	TaskEvent,
	TaskIntent,
	TaskRecord,
	TaskRelay,
	TaskSnapshot,
} from "./task-protocol";
import type { TaskStore, OutboxRecord, OutboxQuarantineRecord } from "./task-store";
import { SYSTEM_TASK_TIMERS, TASK_WORK_LIMITS, capacityRetryAt, createTaskWorkScope, deferredTaskDelivery } from "./task-work-scope";
import type { TaskShipment, TaskTimers, TaskWorkScope } from "./task-work-scope";

const RECEIVE_PAGE_SIZE = TASK_WORK_LIMITS.pageItems;
const DELIVERY_EVIDENCE_OPERATION = "delivery_evidence";
const RECEIVED_INTENT_OPERATION = "received_intent";
const TERMINAL_STATUSES: ReadonlySet<TaskRecord["status"]> = new Set(Object.values(TASK_EVENT_TYPES).flatMap(type => type.status ?? []));
const TERMINAL_EVENTS = taskEventTypes(type => type.terminal);
const CONVERSATIONAL_EVENTS = taskEventTypes(type => type.conversational);
const CANONICAL_EVENTS = taskEventTypes(() => true);
const RECEIVER_INTENT_TYPES = taskEventTypes(type => type.intent);
const IGNORE_OUTBOX_FAILURES: ReadonlySet<string> = new Set();
/** The blocked-send code that records a target loss, the precondition of a reassignment. */
const TARGET_LOSS_CODE = "TARGET_NOT_REGISTERED";
const REASSIGNED_EVENT = "task.reassigned";

export interface TaskCoreOptions {
	readonly endpoint: TaskEndpoint;
	readonly relay: TaskRelay;
	readonly store: TaskStore;
	readonly clock?: { readonly now: () => number };
	/** Delivery budget time (deadline, slices, cooldowns); defaults to system time. Separate from `clock`, which stamps task records. */
	readonly timers?: TaskTimers;
	readonly ids?: () => string;
}

export interface CreateTaskInput {
	readonly target: TaskEndpoint;
	readonly task: string;
	readonly timeoutMs: number;
}

export interface SubmitIntentInput {
	readonly taskId: string;
	readonly type: TaskIntent["type"];
	readonly payload: Record<string, unknown>;
}

export type SubmitIntentOutcome =
	| { readonly authority: "origin"; readonly canonicalEvent: { readonly type: string; readonly reused: boolean } }
	| { readonly authority: "receiver" };

/** Local `wake_accepted` evidence is persisted before Pi wake emission; flush delivers it once the wake is observed. */
export interface WakeRequest {
	flush(signal?: AbortSignal): Promise<void>;
	/** Withdraws the never-sent record when Pi did not accept the wake; call synchronously after the wake attempt. */
	discard(): void;
}

/** One presentable inbox item, merged by store arrival order. Local items are origin-canonicalized events that never travel through the relay to the origin. */
export type TaskInboxItem =
	| { readonly kind: "relay"; readonly delivery: RelayDelivery; readonly order: number }
	| { readonly kind: "local"; readonly event: TaskEvent; readonly order: number };

export interface TaskCore {
	readonly endpoint: TaskEndpoint;
	connect(signal?: AbortSignal, work?: TaskWorkScope): Promise<void>;
	createTask(input: CreateTaskInput, signal?: AbortSignal, work?: TaskWorkScope): Promise<{ readonly taskId: string }>;
	getTask(taskId: string): TaskSnapshot | undefined;
	listTasks(): readonly TaskSnapshot[];
	/** True while any task is active or the outbox holds pending envelopes; drives the background poll cadence. */
	hasPendingWork(): boolean;
	/** Synchronous notification after each committed store change; returns the unsubscribe function. */
	onChange(listener: () => void): () => void;
	/** Correlate a receiver conversation with a locally recorded outbound intent, never session history or payload author claims. */
	isSelfAuthoredConversation(event: TaskEvent): boolean;
	/** Offers pending mail with bounded old-work retry; in a phased pass this is the explicit final retry phase. */
	flushOutbox(signal?: AbortSignal, work?: TaskWorkScope): Promise<void>;
	receive(signal?: AbortSignal, work?: TaskWorkScope): Promise<readonly TaskInboxItem[]>;
	/** Intake phase of a phased pass: persists and canonicalizes without sending or ACKing; required fan-out joins `work.shipments`. */
	intakeInbox(signal?: AbortSignal, work?: TaskWorkScope): Promise<readonly TaskInboxItem[]>;
	/** Ship phase of a phased pass: sends `work.shipments` in persist order and records each outcome; only abort throws. */
	shipRequired(signal?: AbortSignal, work?: TaskWorkScope): Promise<void>;
	/** Acknowledge-phase repair: retries staged raw ACKs, then reports local deferral of a raw intent still pending. */
	repairAcknowledgements(signal?: AbortSignal, work?: TaskWorkScope): Promise<void>;
	/** Retire a local item after its Pi insertion and wake evidence exist; no relay ACK or receipt. */
	completeLocalPresentation(eventId: string): void;
	acknowledgeRelayDelivery(cursor: string, signal?: AbortSignal, work?: TaskWorkScope): Promise<void>;
	submitIntent(input: SubmitIntentInput, signal?: AbortSignal, work?: TaskWorkScope): Promise<void>;
	submitIntentWithOutcome?(input: SubmitIntentInput, signal?: AbortSignal, work?: TaskWorkScope): Promise<SubmitIntentOutcome>;
	recordInsertion(input: { readonly taskId: string; readonly eventId: string }, signal?: AbortSignal, work?: TaskWorkScope): Promise<void>;
	recordWakeRequest(input: { readonly taskId: string; readonly eventId: string }, work?: TaskWorkScope): WakeRequest;
	evaluateTimeouts(signal?: AbortSignal, work?: TaskWorkScope): Promise<void>;
	acknowledgeParent(taskId: string, signal?: AbortSignal, work?: TaskWorkScope): Promise<void>;
	/**
	 * Origin-only: after a recorded TARGET_NOT_REGISTERED loss of the current target, resolves the lost
	 * target's successor and sends it a `task.reassigned` assignment carrying the task history.
	 */
	reassignTask?(taskId: string, signal?: AbortSignal, work?: TaskWorkScope): Promise<{ readonly taskId: string; readonly target: TaskEndpoint }>;
}

type ReceivedEnvelope =
	| { readonly kind: "assignment"; readonly task: Omit<TaskRecord, "events">; readonly event: TaskEvent; readonly history: readonly TaskEvent[] }
	| { readonly kind: "intent"; readonly task: TaskRecord; readonly intent: TaskIntent }
	| { readonly kind: "canonical_event"; readonly task: TaskRecord; readonly event: TaskEvent };

export function createTaskCore(options: TaskCoreOptions): TaskCore {
	const clock = options.clock ?? { now: (): number => Date.now() };
	const timers = options.timers ?? SYSTEM_TASK_TIMERS;
	const workScope = (signal?: AbortSignal, mode?: "background" | "foreground" | "phased") => createTaskWorkScope(signal, mode, timers);
	const ids = options.ids ?? (() => crypto.randomUUID());
	const flush = createOutboxFlusher(options);
	const acknowledgeWithSignal = async (cursor: string, signal: AbortSignal, work: TaskWorkScope): Promise<void> => {
		if (!decimal(cursor)) throw new TaskProtocolError("INVALID_CURSOR", "relay cursor must be a non-negative integer");
		const envelopeId = options.store.pendingRelayEnvelopeId(options.endpoint, cursor);
		if (envelopeId === undefined) throw new TaskProtocolError("INVALID_CURSOR", "relay delivery is not pending in this endpoint scope");
		const retryAt = options.store.relayAcknowledgementRetryAt(options.endpoint, cursor);
		if (retryAt > timers.now() || !work.claimAcknowledgement(cursor)) throw deferredTaskDelivery({ cursor, retryAfterMs: Math.max(0, retryAt - timers.now()) });
		options.store.transaction(() => options.store.requestRelayAcknowledgement(options.endpoint, cursor));
		try {
			await waitForRelayOperation(options.relay.acknowledgeDelivery({ endpoint: options.endpoint, cursor, envelopeId }, signal), signal);
			throwIfAborted(signal);
			options.store.transaction(() => options.store.acknowledgeRelayCursor(options.endpoint, cursor));
		} catch (error) {
			if (signal.aborted) throw error;
			const retryAt = capacityRetryAt(error, timers.now());
			if (retryAt !== undefined) options.store.transaction(() => options.store.requestRelayAcknowledgement(options.endpoint, cursor, retryAt));
			throw error;
		}
	};
	const acknowledgeRelayDelivery = (cursor: string, signal?: AbortSignal, work = workScope(signal)): Promise<void> => acknowledgeWithSignal(cursor, work.signal, work);
	const retryRequestedAcknowledgements = async (work: TaskWorkScope): Promise<void> => {
		for (const cursor of options.store.requestedRelayAcknowledgements(options.endpoint, TASK_WORK_LIMITS.acknowledgements, timers.now())) {
			if (!work.canAcknowledge() || !work.canRepairAcknowledgement()) break;
			if (!work.attemptedAcknowledgements.has(cursor) && options.store.pendingRelayEnvelopeId(options.endpoint, cursor) !== undefined) await acknowledgeWithSignal(cursor, work.backgroundSignal(), work);
		}
	};
	/** Sends required envelopes now, or defers them to the ship phase of a phased pass. */
	const ship = async (signal: AbortSignal, envelopeIds: readonly string[], work: TaskWorkScope, verify?: () => void): Promise<void> => {
		if (work.phased) { work.shipments.push({ envelopeIds, verify, shipped: false }); return; }
		try {
			if (envelopeIds.length > 0) await flush(signal, new Set(envelopeIds), work);
		} catch (error) {
			verify?.();
			throw error;
		}
		verify?.();
	};
	/** Ship phase: sends each shipment in persist order and records its outcome; only abort throws. */
	const shipAll = async (shipments: readonly TaskShipment[], work: TaskWorkScope): Promise<void> => {
		for (const shipment of shipments) {
			if (shipment.shipped) continue;
			let failure: unknown;
			try { if (shipment.envelopeIds.length > 0) await flush(work.signal, new Set(shipment.envelopeIds), work, false); } catch (error) {
				if (work.signal.aborted) throw error;
				failure = error;
			}
			// A blocked-delivery error replaces the send failure, as for an immediate send.
			try { shipment.verify?.(); } catch (error) { failure = error; }
			shipment.shipped = true;
			shipment.failure = failure;
			if (failure === undefined) shipment.accepted?.();
		}
	};
	/** Acknowledge-phase repair of staged raw ACKs; a raw intent still pending afterwards reports local deferral. */
	const repairAcknowledgements = async (work: TaskWorkScope): Promise<void> => {
		await retryRequestedAcknowledgements(work);
		const pendingRaw = options.store.pendingRelayDeliveries(options.endpoint).find(pending => options.store.inboxEnvelope(pending.envelopeId)?.kind === TaskEnvelopeKind.intent);
		if (pendingRaw) throw deferredTaskDelivery({ cursor: pendingRaw.cursor,
			retryAfterMs: Math.max(0, options.store.relayAcknowledgementRetryAt(options.endpoint, pendingRaw.cursor) - timers.now()) });
	};
	/**
	 * Intake phase: re-drives tracked, unacknowledged relay deliveries from the local inbox, then fetches
	 * after the intake cursor while the pending set has room. It persists and canonicalizes without sending:
	 * each raw intent's required fan-out becomes a shipment whose acceptance stages that intent's ACK.
	 */
	const intake = async (signal: AbortSignal, shipments: TaskShipment[]): Promise<readonly TaskInboxItem[]> => {
		const visibleDeliveries: RelayDelivery[] = [];
		const settle = (delivery: RelayDelivery, received: ReceivedEnvelope, persistedEnvelopeIds: readonly string[]): void => {
			if (received.kind !== "intent") { visibleDeliveries.push(delivery); return; }
			// Accepted fan-out authorizes the ACK; the checkpoint owns the staged request.
			shipments.push({ envelopeIds: persistedEnvelopeIds, shipped: false, accepted: () => options.store.transaction(() => options.store.requestRelayAcknowledgement(
				options.endpoint, delivery.cursor, options.store.relayAcknowledgementRetryAt(options.endpoint, delivery.cursor))) });
		};
		// A staged ACK request already proves accepted fanout; re-drive the rest.
		const staged = new Set(options.store.requestedRelayAcknowledgements(options.endpoint, Number.MAX_SAFE_INTEGER, Number.MAX_SAFE_INTEGER));
		for (const pending of options.store.pendingRelayDeliveries(options.endpoint)) {
			if (staged.has(pending.cursor)) continue;
			const envelope = options.store.inboxEnvelope(pending.envelopeId);
			if (envelope === undefined) throw new Error("pending relay delivery was not persisted");
			const received = validateReceivedEnvelope(options, envelope);
			const persistedEnvelopeIds = received.kind === "intent" ? options.store.transaction(() => persistReceivedEnvelope(options, clock.now, ids, received, envelope.envelopeId)) : [];
			settle({ cursor: pending.cursor, envelope }, received, persistedEnvelopeIds);
		}
		let cursor = options.store.relayIntakeCursor(options.endpoint);
		for (let room = TASK_WORK_LIMITS.pendingDeliveries - options.store.pendingRelayDeliveries(options.endpoint).length; room > 0;
			room = TASK_WORK_LIMITS.pendingDeliveries - options.store.pendingRelayDeliveries(options.endpoint).length) {
			const limit = Math.min(RECEIVE_PAGE_SIZE, room);
			const page = await options.relay.receive({ endpoint: options.endpoint, cursor, limit }, signal);
			throwIfAborted(signal);
			if (!isInboxPage(page) || page.deliveries.length > limit) throw new TaskProtocolError("INVALID_INBOX", "relay returned an invalid inbox page");
			let previous = BigInt(cursor);
			for (const delivery of page.deliveries) {
				if (!isDelivery(delivery) || !sameEndpoint(delivery.envelope.target, options.endpoint) || BigInt(delivery.cursor) <= previous) throw new TaskProtocolError("INVALID_DELIVERY", "relay returned an invalid or unordered delivery");
				previous = BigInt(delivery.cursor);
			}
			if (page.nextCursor !== (page.deliveries.at(-1)?.cursor ?? cursor) || (!page.deliveries.length && page.hasMore)) throw new TaskProtocolError("INVALID_INBOX", "relay page cursor does not describe its deliveries");
			for (const delivery of page.deliveries) {
				const received = validateReceivedEnvelope(options, delivery.envelope);
				let persistedEnvelopeIds: readonly string[] = [];
				options.store.transaction(() => {
					// Track only with persistence: the intake cursor never passes an unpersisted delivery,
					// so an abort or a rejected envelope leaves the suffix to be fetched again.
					options.store.trackRelayDeliveries(options.endpoint, [{ cursor: delivery.cursor, envelopeId: delivery.envelope.envelopeId }]);
					if (!options.store.persistInbox(delivery.envelope, delivery.cursor) && received.kind !== "intent") return;
					persistedEnvelopeIds = persistReceivedEnvelope(options, clock.now, ids, received, delivery.envelope.envelopeId);
				});
				settle(delivery, received, persistedEnvelopeIds);
			}
			cursor = page.nextCursor;
			if (!page.hasMore) break;
		}
		return inboxItems(options.store, visibleDeliveries);
	};
	const submitIntentWithOutcome = async (input: SubmitIntentInput, signal?: AbortSignal, work = workScope(signal, "foreground")): Promise<SubmitIntentOutcome> => {
		signal = work.signal;
		if (!RECEIVER_INTENT_TYPES.has(input.type) || !isRecord(input.payload)) throw new TaskProtocolError("INVALID_INTENT", "intent type or payload is invalid");
		let persistedEnvelopeIds: readonly string[] = [];
		let outcome: SubmitIntentOutcome | undefined;
		let receiverTerminal = false;
		let originCancellation = false;
		const evidence = deliveryEvidence(input);
		options.store.transaction(() => {
			const task = requiredTask(options.store, input.taskId);
			receiverTerminal = !sameEndpoint(options.endpoint, task.origin) && TERMINAL_EVENTS.has(input.type);
			originCancellation = sameEndpoint(options.endpoint, task.origin) && input.type === "task.cancelled";
			const persisted = persistIntent(options, clock.now, ids, task, input, evidence === undefined ? undefined : deliveryEvidenceOperation(evidence));
			persistedEnvelopeIds = persisted.envelopeIds;
			outcome = persisted.outcome;
		});
		if (evidence !== undefined) {
			await ship(signal, persistedEnvelopeIds, work, () => throwIfDeliveryEvidenceBlocked(options.store, input.taskId, persistedEnvelopeIds, evidence));
		} else {
			try {
				await flush(signal, new Set(persistedEnvelopeIds), work);
			} catch (error) {
				if (originCancellation) throwIfCancellationDeliveryBlocked(options, input.taskId, persistedEnvelopeIds);
				throw error;
			}
			if (originCancellation) throwIfCancellationDeliveryBlocked(options, input.taskId, persistedEnvelopeIds);
		}
		if (receiverTerminal) throwIfTerminalDeliveryBlocked(options.store, input.taskId);
		if (outcome === undefined) throw new TaskProtocolError("INVALID_INTENT", "task intent outcome was not persisted");
		return outcome;
	};

	return {
		endpoint: options.endpoint,
		async connect(signal, work = workScope(signal)) {
			signal = work.signal;
			const connection = await options.relay.connect({ endpoint: options.endpoint, protocolVersion: TASK_PROTOCOL_VERSION, receiveCursor: options.store.getReceiveCursor() }, signal);
			if (!sameEndpoint(connection.endpoint, options.endpoint)) throw new TaskProtocolError("INVALID_CONNECTION", "relay connected a different endpoint");
			throwIfAborted(signal);
			// A phased pass repairs in its acknowledge phase, after intake.
			if (!work.phased) await retryRequestedAcknowledgements(work);
		},
		async createTask(input, signal, work = workScope(signal, "foreground")) {
			signal = work.signal;
			if (input.task.length === 0 || !Number.isInteger(input.timeoutMs) || input.timeoutMs < 1) throw new TaskProtocolError("INVALID_TASK", "task and a positive timeout are required");
			const target = await options.relay.resolve({ relay: input.target.relay, reference: input.target.id }, signal);
			throwIfAborted(signal);
			if (!isEndpoint(target)) throw new TaskProtocolError("INVALID_TARGET", "relay resolved an invalid task endpoint");
			const now = clock.now();
			const taskId = ids();
			const task: Omit<TaskRecord, "events"> = {
				taskId, protocolVersion: TASK_PROTOCOL_VERSION, origin: options.endpoint, target, task: input.task,
				createdAt: now, expiresAt: now + input.timeoutMs, status: "active",
			};
			const created = event(task, ids(), "task.created", "1", options.endpoint, target, now, { task: input.task });
			const assignment = envelope(ids(), options.endpoint, target, taskId, TaskEnvelopeKind.assignment, { task, event: created }, now);
			options.store.transaction(() => {
				options.store.putTask(task);
				options.store.appendEvent(created);
				options.store.putOutbox(assignment);
			});
			try {
				await flush(signal, new Set([assignment.envelopeId]), work);
			} catch (error) {
				throw operationError(error, taskId);
			}
			return { taskId };
		},
		async reassignTask(taskId, signal, work = workScope(signal, "foreground")) {
			signal = work.signal;
			const lost = lostTarget(options, taskId).target;
			const target = await options.relay.resolve({ relay: lost.relay, reference: lost.id }, signal);
			throwIfAborted(signal);
			if (!isEndpoint(target)) throw new TaskProtocolError("INVALID_TARGET", "relay resolved an invalid task endpoint");
			if (sameEndpoint(target, lost)) throw reassignmentRefused(taskId, "the lost target resolved to itself");
			const assignmentId = ids();
			options.store.transaction(() => {
				// Re-checked here: a concurrent reassignment already moved the target, so one loss allows one reassignment.
				const task = lostTarget(options, taskId);
				if (!sameEndpoint(task.target, lost)) throw reassignmentRefused(taskId, "the task target changed during resolution");
				const now = clock.now();
				const record: Omit<TaskRecord, "events"> = {
					taskId, protocolVersion: task.protocolVersion, origin: task.origin, target, task: task.task,
					createdAt: task.createdAt, expiresAt: task.expiresAt, status: task.status,
				};
				const reassigned = event(record, ids(), REASSIGNED_EVENT, String(task.events.length + 1), options.endpoint, target, now, { task: task.task, previousTarget: lost });
				// Origin-local delivery evidence never travels to a target.
				const history = task.events.filter(candidate => TASK_EVENT_TYPES[candidate.type]?.evidence !== true);
				options.store.setTarget(taskId, target);
				options.store.appendEvent(reassigned);
				options.store.putOutbox(envelope(assignmentId, options.endpoint, target, taskId, TaskEnvelopeKind.assignment, reassignmentPayload(record, reassigned, history), now));
				if (!sameEndpoint(target, options.endpoint)) options.store.enqueuePresentation({ taskId, eventId: reassigned.eventId });
			});
			try {
				await flush(signal, new Set([assignmentId]), work);
			} catch (error) {
				throw operationError(error, taskId);
			}
			return { taskId, target };
		},
		getTask(taskId) { return options.store.getTask(taskId); },
		listTasks() { return options.store.listTasks(); },
		onChange(listener) { return options.store.onChange(listener); },
		hasPendingWork() { return options.store.hasPendingOutbox() || options.store.hasActiveTasks(); },
		isSelfAuthoredConversation(event) {
			if (!CONVERSATIONAL_EVENTS.has(event.type) || typeof event.payload.intentId !== "string") return false;
			const task = options.store.getTask(event.taskId);
			if (!task || !sameEndpoint(options.endpoint, task.target) || sameEndpoint(options.endpoint, task.origin)
				|| !sameEndpoint(event.source, task.origin) || !sameEndpoint(event.target, task.target)) return false;
			const localEnvelope = options.store.getIntentEnvelope(event.payload.intentId);
			if (!localEnvelope || localEnvelope.kind !== TaskEnvelopeKind.intent || localEnvelope.taskId !== task.taskId
				|| !sameEndpoint(localEnvelope.source, options.endpoint) || !sameEndpoint(localEnvelope.target, task.origin)) return false;
			const intent = parsePayload(localEnvelope);
			return isTaskIntent(intent) && intent.taskId === event.taskId && intent.type === event.type && intent.intentId === event.payload.intentId;
		},
		async flushOutbox(signal, work = workScope(signal)) { await flush(work.signal, undefined, work, true); },
		async receive(signal, work = workScope(signal)) {
			// The phases of a pass without presentation; the first fan-out or repair failure surfaces after them.
			const shipments: TaskShipment[] = [];
			let items: readonly TaskInboxItem[], failure: unknown;
			try {
				items = await intake(work.signal, shipments);
				await shipAll(shipments, work);
				failure = shipments.find(shipment => shipment.failure !== undefined)?.failure;
				try { await repairAcknowledgements(work); } catch (error) {
					if (work.signal.aborted) throw error;
					failure ??= error;
				}
			} finally {
				// A phased driver owns the later retry share; direct receive retains its own.
				if (!work.phased && !work.signal.aborted) await flush(work.signal, IGNORE_OUTBOX_FAILURES, work, true);
			}
			if (failure !== undefined) throw failure;
			return items;
		},
		async intakeInbox(signal, work = workScope(signal, "phased")) { return intake(work.signal, work.shipments); },
		async shipRequired(signal, work = workScope(signal, "phased")) { await shipAll(work.shipments, work); },
		async repairAcknowledgements(signal, work = workScope(signal)) { await repairAcknowledgements(work); },
		acknowledgeRelayDelivery,
		completeLocalPresentation(eventId) { options.store.completePresentation(eventId); },
		async submitIntent(input, signal, work) { await submitIntentWithOutcome(input, signal, work); },
		submitIntentWithOutcome,
		async recordInsertion(input, signal, work = workScope(signal)) {
			signal = work.signal;
			const task = requiredTask(options.store, input.taskId);
			const evidence: DeliveryEvidence = { eventId: input.eventId, stage: TaskDeliveryStage.piInserted, state: TaskDeliveryEvidenceState.confirmed };
			let persistedEnvelopeIds: readonly string[] = [];
			options.store.transaction(() => {
				options.store.putInsertionReceipt(input.taskId, input.eventId);
				persistedEnvelopeIds = persistIntent(options, clock.now, ids, task, {
					taskId: input.taskId,
					type: "task.delivery_receipt",
					payload: { eventId: input.eventId, stage: evidence.stage, state: evidence.state },
				}, deliveryEvidenceOperation(evidence)).envelopeIds;
			});
			await ship(signal, persistedEnvelopeIds, work, () => throwIfDeliveryEvidenceBlocked(options.store, input.taskId, persistedEnvelopeIds, evidence));
		},
		recordWakeRequest(input, work) {
			const evidence: DeliveryEvidence = { eventId: input.eventId, stage: TaskDeliveryStage.wakeAccepted, state: TaskDeliveryEvidenceState.confirmed };
			const operation = deliveryEvidenceOperation(evidence);
			const persist = () => options.store.transaction(() => {
				options.store.putInsertionReceipt(input.taskId, input.eventId);
				return persistIntent(options, clock.now, ids, requiredTask(options.store, input.taskId), {
					taskId: input.taskId,
					type: "task.delivery_receipt",
					payload: { eventId: input.eventId, stage: evidence.stage, state: evidence.state },
				}, operation);
			});
			// An origin canonicalizes evidence straight into its task log, which cannot be withdrawn,
			// so it records on flush; a receiver writes its outbound record before the wake.
			let persisted = sameEndpoint(options.endpoint, requiredTask(options.store, input.taskId).origin) ? undefined : persist();
			return {
				flush: async (signal?: AbortSignal): Promise<void> => {
					const { envelopeIds } = persisted ??= persist();
					const activeWork = work ?? workScope(signal);
					await ship(activeWork.signal, envelopeIds, activeWork, () => throwIfDeliveryEvidenceBlocked(options.store, input.taskId, envelopeIds, evidence));
				},
				discard: () => {
					// No await separates the write from this call, so the envelope was never offered to the relay.
					if (persisted?.created) options.store.discardUnsentOperation(input.taskId, operation);
					persisted = undefined;
				},
			};
		},
		async evaluateTimeouts(signal, work = workScope(signal)) {
			signal = work.signal;
			const persistedEnvelopeIds: string[] = [];
			for (const candidate of options.store.listTasks()) {
				if (!sameEndpoint(candidate.origin, options.endpoint) || TERMINAL_STATUSES.has(candidate.status) || candidate.expiresAt > clock.now()) continue;
				options.store.transaction(() => {
					const task = options.store.getTask(candidate.taskId);
					if (!task || !sameEndpoint(task.origin, options.endpoint) || TERMINAL_STATUSES.has(task.status) || task.expiresAt > clock.now()) return;
					persistedEnvelopeIds.push(...canonicalize(options, clock.now, ids, task, { intentId: ids(), taskId: task.taskId, type: "task.cancelled", payload: {} }, "task.timed_out").envelopeIds);
				});
			}
			if (persistedEnvelopeIds.length > 0) await ship(signal, persistedEnvelopeIds, work);
		},
		async acknowledgeParent(taskId, signal, work = workScope(signal)) {
			signal = work.signal;
			let persistedEnvelopeIds: readonly string[] = [];
			options.store.transaction(() => {
				const task = requiredTask(options.store, taskId);
				if (!sameEndpoint(task.origin, options.endpoint)) throw new TaskProtocolError("NOT_ORIGIN", "only origin may acknowledge a task");
				if (!TERMINAL_STATUSES.has(task.status)) throw new TaskProtocolError("TASK_NOT_TERMINAL", "only a terminal task may be acknowledged", { retryable: false });
				persistedEnvelopeIds = canonicalize(options, clock.now, ids, task, { intentId: ids(), taskId, type: "task.information", payload: {} }, "task.parent_acknowledged", PARENT_ACKNOWLEDGMENT_OPERATION).envelopeIds;
			});
			await flush(signal, new Set(persistedEnvelopeIds), work);
		},
	};
}

function validateReceivedEnvelope(options: TaskCoreOptions, relayEnvelope: unknown): ReceivedEnvelope {
	if (!isEnvelope(relayEnvelope)) throw new TaskProtocolError("INVALID_ENVELOPE", "relay envelope is malformed");
	if (relayEnvelope.protocolVersion !== TASK_PROTOCOL_VERSION) throw new TaskProtocolError("INCOMPATIBLE_PROTOCOL", "received an incompatible task envelope");
	if (!sameEndpoint(relayEnvelope.target, options.endpoint)) throw new TaskProtocolError("INVALID_TARGET", "relay envelope targets another endpoint");
	const payload = parsePayload(relayEnvelope);
	if (relayEnvelope.kind === TaskEnvelopeKind.assignment) return validateAssignment(relayEnvelope, payload);
	if (relayEnvelope.kind === TaskEnvelopeKind.intent) return validateIntent(options, relayEnvelope, payload);
	return validateCanonicalEvent(options, relayEnvelope, payload);
}

function validateAssignment(envelope: RelayEnvelope, payload: unknown): ReceivedEnvelope {
	if (!isRecord(payload) || !isTaskRecordInput(payload.task) || !isTaskEvent(payload.event)) throw new TaskProtocolError("INVALID_ASSIGNMENT", "assignment payload is malformed");
	if (payload.event.type === REASSIGNED_EVENT) return validateReassignment(envelope, payload, payload.task, payload.event);
	const task = payload.task;
	const created = payload.event;
	if (envelope.taskId !== task.taskId || envelope.taskId !== created.taskId || task.protocolVersion !== TASK_PROTOCOL_VERSION
		|| task.status !== "active" || task.expiresAt < task.createdAt || created.type !== "task.created" || created.sequence !== "1" || created.payload.task !== task.task
		|| !sameEndpoint(envelope.source, task.origin) || !sameEndpoint(envelope.target, task.target)
		|| !sameEndpoint(created.source, task.origin) || !sameEndpoint(created.target, task.target)) {
		throw new TaskProtocolError("INVALID_ASSIGNMENT", "assignment envelope headers do not match its payload");
	}
	return { kind: "assignment", task, event: created, history: [] };
}

/**
 * A reassignment carries history addressed to earlier targets, so each carried event is checked
 * against the task and its origin, not the current target. Without `historyTruncated` it starts at `task.created`.
 */
function validateReassignment(envelope: RelayEnvelope, payload: Record<string, unknown>, task: Omit<TaskRecord, "events">, reassigned: TaskEvent): ReceivedEnvelope {
	const truncated = payload.historyTruncated === true;
	const history = payload.history;
	if (!Array.isArray(history) || !history.every(isTaskEvent) || (payload.historyTruncated !== undefined && !truncated)) throw new TaskProtocolError("INVALID_ASSIGNMENT", "reassignment payload is malformed");
	let previous = 0n;
	const carried = history.every(event => {
		const ordered = BigInt(event.sequence) > previous;
		previous = BigInt(event.sequence);
		return ordered && event.taskId === task.taskId && CANONICAL_EVENTS.has(event.type) && TASK_EVENT_TYPES[event.type]?.evidence !== true && sameEndpoint(event.source, task.origin);
	});
	const first = history[0];
	if (envelope.taskId !== task.taskId || envelope.taskId !== reassigned.taskId || task.protocolVersion !== TASK_PROTOCOL_VERSION
		|| task.status !== "active" || task.expiresAt < task.createdAt || reassigned.payload.task !== task.task
		|| !sameEndpoint(envelope.source, task.origin) || !sameEndpoint(envelope.target, task.target)
		|| !sameEndpoint(reassigned.source, task.origin) || !sameEndpoint(reassigned.target, task.target)
		|| !carried || previous >= BigInt(reassigned.sequence)
		|| (!truncated && (first?.type !== "task.created" || first.sequence !== "1" || first.payload.task !== task.task))) {
		throw new TaskProtocolError("INVALID_ASSIGNMENT", "reassignment headers, payload or history do not match");
	}
	return { kind: "assignment", task, event: reassigned, history };
}

/** The task, its reassignment and as much of the latest history as fits the relay payload limit. */
function reassignmentPayload(task: Omit<TaskRecord, "events">, reassigned: TaskEvent, history: readonly TaskEvent[]): unknown {
	const bytes = (value: unknown): number => new TextEncoder().encode(JSON.stringify(value)).byteLength;
	const complete = { task, event: reassigned, history };
	if (bytes(complete) <= MAX_RELAY_PAYLOAD_BYTES) return complete;
	// Keep the newest suffix: each kept event costs its JSON plus a separating comma.
	let size = bytes({ task, event: reassigned, history: [], historyTruncated: true });
	let start = history.length;
	while (start > 0) {
		const next = size + bytes(history[start - 1]) + (start === history.length ? 0 : 1);
		if (next > MAX_RELAY_PAYLOAD_BYTES) break;
		size = next;
		start--;
	}
	if (size > MAX_RELAY_PAYLOAD_BYTES) throw new TaskProtocolError("PAYLOAD_TOO_LARGE", "reassignment exceeds the relay payload limit", { retryable: false });
	return { task, event: reassigned, history: history.slice(start), historyTruncated: true };
}

/** The origin's active task whose current target has a recorded TARGET_NOT_REGISTERED loss. */
function lostTarget(options: TaskCoreOptions, taskId: string): TaskSnapshot {
	const task = requiredTask(options.store, taskId);
	if (!sameEndpoint(task.origin, options.endpoint)) throw new TaskProtocolError("NOT_ORIGIN", "only the origin may reassign a task", { retryable: false });
	if (task.status !== "active") throw reassignmentRefused(taskId, "the task is not active");
	// Keyed on the current target: leftover envelopes to an earlier target never re-trigger.
	const lost = options.store.quarantinedOutbox().some(record => record.errorCode === TARGET_LOSS_CODE && record.envelope.taskId === taskId && sameEndpoint(record.envelope.target, task.target));
	if (!lost) throw reassignmentRefused(taskId, "no loss of the current target is recorded");
	return task;
}

function reassignmentRefused(taskId: string, reason: string): TaskProtocolError {
	return new TaskProtocolError("REASSIGNMENT_REFUSED", `task cannot be reassigned: ${reason}`, { retryable: false, details: { taskId } });
}

function validateIntent(options: TaskCoreOptions, envelope: RelayEnvelope, payload: unknown): ReceivedEnvelope {
	const task = requiredTask(options.store, envelope.taskId);
	if (!sameEndpoint(task.origin, options.endpoint) || !sameEndpoint(envelope.source, task.target) || !sameEndpoint(envelope.target, task.origin)) {
		throw new TaskProtocolError("UNAUTHORIZED_INTENT", "only the assigned receiver may submit an intent to origin");
	}
	if (!isTaskIntent(payload) || payload.taskId !== envelope.taskId || !RECEIVER_INTENT_TYPES.has(payload.type)) {
		throw new TaskProtocolError("INVALID_INTENT", "intent envelope headers or payload are invalid");
	}
	return { kind: "intent", task, intent: payload };
}

function validateCanonicalEvent(options: TaskCoreOptions, envelope: RelayEnvelope, payload: unknown): ReceivedEnvelope {
	const task = requiredTask(options.store, envelope.taskId);
	if (!isTaskEvent(payload) || !CANONICAL_EVENTS.has(payload.type) || payload.taskId !== envelope.taskId
		|| !sameEndpoint(envelope.source, task.origin) || (!sameEndpoint(envelope.target, task.target) && !sameEndpoint(envelope.target, task.origin))
		|| !sameEndpoint(payload.source, task.origin) || !sameEndpoint(payload.target, task.target)) {
		throw new TaskProtocolError("INVALID_EVENT", "canonical event envelope headers or payload are invalid");
	}
	if (!sameEndpoint(envelope.target, options.endpoint)) throw new TaskProtocolError("INVALID_TARGET", "canonical event targets another endpoint");
	return { kind: "canonical_event", task, event: payload };
}

function persistReceivedEnvelope(options: TaskCoreOptions, now: () => number, ids: () => string, received: ReceivedEnvelope, envelopeId: string): readonly string[] {
	if (received.kind === "assignment") {
		options.store.putTask(received.task);
		// A known task follows the reassignment; carried history is append-only and skips events already held.
		if (received.event.type === REASSIGNED_EVENT) options.store.setTarget(received.task.taskId, received.task.target);
		for (const carried of received.history) options.store.appendEvent(carried);
		options.store.appendEvent(received.event);
		return [];
	}
	if (received.kind === "intent") return canonicalize(options, now, ids, received.task, received.intent, received.intent.type, `${RECEIVED_INTENT_OPERATION}:${envelopeId}`).envelopeIds;
	if (options.store.appendEvent(received.event) && TERMINAL_EVENTS.has(received.event.type)) options.store.setStatus(received.task.taskId, statusFor(received.event.type));
	return [];
}

interface PersistedIntent {
	readonly envelopeIds: readonly string[];
	readonly outcome: SubmitIntentOutcome;
	/** False when an existing operation reservation was reused. */
	readonly created: boolean;
}

function persistIntent(options: TaskCoreOptions, now: () => number, ids: () => string, task: TaskRecord, input: SubmitIntentInput, reservedOperation?: string): PersistedIntent {
	if (!sameEndpoint(options.endpoint, task.origin) && !sameEndpoint(options.endpoint, task.target)) throw new TaskProtocolError("NOT_PARTICIPANT", "historical task belongs to a different endpoint", { retryable: false });
	if (sameEndpoint(options.endpoint, task.origin)) {
		const operation = input.type === "task.cancelled" ? ORIGIN_CANCELLATION_OPERATION : reservedOperation;
		// The origin already recorded its own conversational event; present only peer-authored conversations for model incorporation.
		const presentToOrigin = !CONVERSATIONAL_EVENTS.has(input.type);
		const canonical = canonicalize(options, now, ids, task, { intentId: ids(), taskId: input.taskId, type: input.type, payload: input.payload }, input.type, operation, presentToOrigin);
		return { envelopeIds: canonical.envelopeIds, outcome: { authority: "origin", canonicalEvent: { type: canonical.eventType, reused: canonical.reused } }, created: !canonical.reused };
	}
	const envelopeId = ids();
	const intent: TaskIntent = { intentId: ids(), taskId: input.taskId, type: input.type, payload: input.payload };
	const operation = TERMINAL_EVENTS.has(input.type) ? TERMINAL_INTENT_OPERATION : reservedOperation;
	if (operation !== undefined) {
		const reservation = options.store.reserveTaskOperation({
			taskId: input.taskId,
			operation,
			logicalId: intent.intentId,
			logicalType: intent.type,
			envelopeIds: [envelopeId],
		});
		if (!reservation.created) {
			if (operation === TERMINAL_INTENT_OPERATION && reservation.record.logicalType !== intent.type) {
				throw new TaskProtocolError("TERMINAL_INTENT_CONFLICT", "terminal task intent conflicts with the existing terminal action", {
					retryable: false,
					details: { taskId: input.taskId, existingType: reservation.record.logicalType, requestedType: intent.type },
				});
			}
			return { envelopeIds: reservation.record.envelopeIds, outcome: { authority: "receiver" }, created: false };
		}
	}
	options.store.putIntent(intent.intentId, input.taskId, envelopeId);
	options.store.putOutbox(envelope(envelopeId, options.endpoint, task.origin, input.taskId, TaskEnvelopeKind.intent, intent, now()));
	return { envelopeIds: [envelopeId], outcome: { authority: "receiver" }, created: true };
}

interface DeliveryEvidence {
	readonly eventId: string;
	readonly stage: TaskDeliveryStage;
	readonly state: TaskDeliveryEvidenceState;
	readonly retryable?: true;
}

function deliveryEvidence(input: SubmitIntentInput): DeliveryEvidence | undefined {
	if (input.type !== "task.delivery_receipt" || typeof input.payload.eventId !== "string") return undefined;
	const stage = input.payload.stage;
	const state = input.payload.state;
	if (stage === TaskDeliveryStage.piInsertion && state === TaskDeliveryEvidenceState.blocked && input.payload.retryable === true) {
		return { eventId: input.payload.eventId, stage, state, retryable: true };
	}
	if ((stage === TaskDeliveryStage.receiverRecorded || stage === TaskDeliveryStage.piInserted || stage === TaskDeliveryStage.wakeRequested || stage === TaskDeliveryStage.wakeAccepted)
		&& state === TaskDeliveryEvidenceState.confirmed) {
		return { eventId: input.payload.eventId, stage, state };
	}
	return undefined;
}

function deliveryEvidenceOperation(input: DeliveryEvidence): string {
	return `${DELIVERY_EVIDENCE_OPERATION}:${input.eventId}:${input.stage}:${input.state}`;
}

interface Canonicalization {
	readonly envelopeIds: readonly string[];
	readonly eventType: string;
	readonly reused: boolean;
}

/** Origin-only. The origin sees its canonical event through the local presentation queue, never a relay envelope to itself. */
function canonicalize(options: TaskCoreOptions, now: () => number, ids: () => string, task: TaskRecord, intent: TaskIntent, requestedType: string, operation?: string, presentToOrigin = true): Canonicalization {
	const terminal = TERMINAL_EVENTS.has(requestedType);
	const type = terminal && TERMINAL_STATUSES.has(task.status) ? "task.late_terminal" : requestedType;
	const sequence = String(task.events.length + 1);
	const canonical = event(task, ids(), type, sequence, options.endpoint, task.target, now(), { ...intent.payload, intentId: intent.intentId });
	// A self-targeted task presents through its target envelope; suppress that for its own conversations.
	const selfTarget = sameEndpoint(task.origin, task.target);
	// Delivery evidence is origin-local status: no fan-out to the target and no presentation.
	const originLocal = TASK_EVENT_TYPES[requestedType]?.evidence === true;
	const targetEnvelope = originLocal || (!presentToOrigin && selfTarget) ? undefined : envelope(ids(), options.endpoint, task.target, task.taskId, TaskEnvelopeKind.canonicalEvent, canonical, canonical.occurredAt);
	const persistedEnvelopeIds = targetEnvelope === undefined ? [] : [targetEnvelope.envelopeId];
	if (operation !== undefined) {
		const reservation = options.store.reserveTaskOperation({ taskId: task.taskId, operation, logicalId: canonical.eventId, logicalType: canonical.type, envelopeIds: persistedEnvelopeIds });
		if (!reservation.created) return { envelopeIds: reservation.record.envelopeIds, eventType: reservation.record.logicalType, reused: true };
	}
	options.store.appendEvent(canonical);
	if (TERMINAL_EVENTS.has(type)) options.store.setStatus(task.taskId, statusFor(type));
	if (targetEnvelope !== undefined) options.store.putOutbox(targetEnvelope);
	if (presentToOrigin && !selfTarget && !originLocal) options.store.enqueuePresentation({ taskId: task.taskId, eventId: canonical.eventId });
	return { envelopeIds: persistedEnvelopeIds, eventType: type, reused: false };
}

function inboxItems(store: TaskStore, deliveries: readonly RelayDelivery[]): readonly TaskInboxItem[] {
	const items: TaskInboxItem[] = deliveries.map((delivery) => {
		const order = store.inboxOrder(delivery.envelope.envelopeId);
		if (order === undefined) throw new Error("visible relay delivery was not persisted");
		return { kind: "relay", delivery, order };
	});
	for (const pending of store.pendingPresentations()) {
		const event = store.getTask(pending.taskId)?.events.find((candidate) => candidate.eventId === pending.eventId);
		if (event === undefined) throw new Error("local presentation event is not in the task log");
		items.push({ kind: "local", event, order: pending.order });
	}
	return items.sort((left, right) => left.order - right.order);
}

/** Sends the reported envelopes, then at most once per scope offers bounded old-work retry when `retry` (default: outside a phased pass). */
type OutboxFlusher = (signal: AbortSignal | undefined, reportedEnvelopeIds: ReadonlySet<string> | undefined, work: TaskWorkScope, retry?: boolean) => Promise<void>;

function createOutboxFlusher(options: TaskCoreOptions): OutboxFlusher {
	const timers = options.timers ?? SYSTEM_TASK_TIMERS;
	const inFlight = new Map<string, Promise<void>>();
	const send = async (record: OutboxRecord, retry: boolean, work: TaskWorkScope): Promise<void> => {
		const id = record.envelope.envelopeId;
		// Per-target FIFO: older pending mail to the same target goes first. Wait out an
		// older send already in flight instead of racing it; otherwise defer behind it.
		for (let ahead = options.store.pendingOutboxAhead(id); ahead.length; ahead = options.store.pendingOutboxAhead(id)) {
			const older = ahead.map(olderId => inFlight.get(olderId)).find(Boolean);
			if (!older) throw deferredTaskDelivery({ envelopeId: id, blockedBy: ahead[0], retryAfterMs: Math.max(0, options.store.outboxRetryAt(ahead[0]!) - timers.now()) });
			await waitForRelayOperation(older.catch(() => undefined), work.signal);
		}
		const joined = inFlight.get(id);
		if (joined) { await waitForRelayOperation(joined, work.signal); return; }
		const retryAt = options.store.outboxRetryAt(id);
		if (retryAt > timers.now() || !work.claimSend(id, retry)) throw deferredTaskDelivery({ envelopeId: id, retryAfterMs: Math.max(0, retryAt - timers.now()) });
		const signal = retry ? work.backgroundSignal() : work.signal;
		const delivery = (async (): Promise<void> => {
			try {
				await waitForRelayOperation(options.relay.send(record.envelope, signal), signal);
				throwIfAborted(signal);
				options.store.transaction(() => options.store.markOutboxAccepted(id));
			} catch (error) {
				if (signal.aborted) throw error;
				const retryAt = capacityRetryAt(error, timers.now());
				if (retryAt !== undefined) {
					options.store.postponeOutbox(id, retryAt);
					// A large foreground refusal does not prove smaller old mail cannot fit.
					if (retry) work.blockedRetryTargets.add(JSON.stringify(record.envelope.target));
				}
				if (error instanceof TaskProtocolError && isBlockedOutboxDeliveryCode(error.code) && !error.retryable) options.store.transaction(() => {
					options.store.quarantineOutbox(id, { errorCode: error.code, reason: error.message, details: error.details ?? {}, quarantinedAt: timers.now() });
				});
				throw error;
			}
		})();
		inFlight.set(id, delivery);
		try { await delivery; } finally { if (inFlight.get(id) === delivery) inFlight.delete(id); }
	};
	return async (signal, reportedEnvelopeIds, work, retry = !work.phased) => {
		throwIfAborted(signal);
		let failure: unknown;
		if (reportedEnvelopeIds) for (const id of reportedEnvelopeIds) {
			const record = options.store.getOutboxRecord(id);
			if (record?.state === "accepted") continue;
			if (!record) {
				const quarantined = options.store.getQuarantine(id);
				failure ??= quarantined ? new TaskProtocolError(quarantined.errorCode, quarantined.reason, { retryable: false, details: quarantined.details }) : deferredTaskDelivery({ envelopeId: id });
				continue;
			}
			// Offer older pending mail to the same target first, as bounded old work (retry
			// budget and background window); its failure leaves this envelope deferred.
			for (const olderId of options.store.pendingOutboxAhead(id)) {
				const older = options.store.getOutboxRecord(olderId);
				if (!older || inFlight.has(olderId)) continue;
				if (work.blockedRetryTargets.has(JSON.stringify(older.envelope.target))) break;
				try { await send(older, true, work); } catch (error) { if (work.signal.aborted) throw error; break; }
			}
			try { await send(record, false, work); } catch (error) {
				if (work.signal.aborted) throw error;
				failure ??= error;
				if (error instanceof TaskProtocolError && error.code === "TASK_DELIVERY_DEFERRED") break;
			}
		}
		if (retry && !work.retryStarted) {
			work.retryStarted = true;
			let attempts = 0;
			for (let inspected = 0; inspected < TASK_WORK_LIMITS.retryInspections && attempts < TASK_WORK_LIMITS.retries; inspected++) {
				if (!options.store.hasPendingOutbox() || !work.canRetry()) break;
				// Advance one attempted/actually skipped ID, never an unattempted batch suffix.
				const record = options.store.selectPendingOutbox(1, timers.now(), 1)[0];
				if (!record) continue;
				const id = record.envelope.envelopeId;
				if (reportedEnvelopeIds?.has(id) || work.attemptedSends.has(id) || inFlight.has(id) || work.blockedRetryTargets.has(JSON.stringify(record.envelope.target))) continue;
				attempts++;
				try { await send(record, true, work); } catch (error) {
					if (work.signal.aborted) throw error;
					if (reportedEnvelopeIds === undefined) failure ??= error;
				}
			}
		}
		if (failure === undefined && reportedEnvelopeIds === undefined && options.store.hasPendingOutbox()) failure = deferredTaskDelivery();
		if (failure !== undefined) throw outboxDeliveryError(failure);
	};
}

async function waitForRelayOperation(delivery: Promise<unknown>, signal: AbortSignal): Promise<void> {
	// Observe the operation first: an abort found here must not leave its later rejection unhandled.
	delivery.catch(() => undefined);
	throwIfAborted(signal);
	let abort!: () => void;
	const cancelled = new Promise<never>((_, reject) => { abort = () => reject(new TaskProtocolError("ABORTED", "task operation aborted", { details: { mayHaveBeenDelivered: true } })); signal.addEventListener("abort", abort, { once: true }); });
	try { await Promise.race([delivery, cancelled]); throwIfAborted(signal); }
	finally { signal.removeEventListener("abort", abort); }
}

function outboxDeliveryError(error: unknown): TaskOutboxDeliveryError {
	if (error instanceof TaskProtocolError) {
		return new TaskOutboxDeliveryError(error.code, error.message, { retryable: error.retryable, details: error.details });
	}
	return new TaskOutboxDeliveryError("TASK_ERROR", error instanceof Error ? error.message : "task delivery failed");
}

function operationError(error: unknown, taskId: string): TaskProtocolError {
	if (error instanceof TaskProtocolError) {
		return new TaskProtocolError(error.code, error.message, { retryable: error.retryable, details: { ...error.details, taskId } });
	}
	return new TaskProtocolError("TASK_ERROR", error instanceof Error ? error.message : "task operation failed", { details: { taskId } });
}

function event(task: Omit<TaskRecord, "events">, eventId: string, type: string, sequence: string, source: TaskEndpoint, target: TaskEndpoint, occurredAt: number, payload: Record<string, unknown>): TaskEvent {
	return { eventId, taskId: task.taskId, type, sequence, source, target, occurredAt, payload };
}

function envelope(envelopeId: string, source: TaskEndpoint, target: TaskEndpoint, taskId: string, kind: RelayEnvelope["kind"], payload: unknown, createdAt: number): RelayEnvelope {
	return { envelopeId, protocolVersion: TASK_PROTOCOL_VERSION, source, target, taskId, kind, payload: JSON.stringify(payload), createdAt: new Date(createdAt).toISOString() };
}

function parsePayload(envelope: RelayEnvelope): unknown {
	try { return JSON.parse(envelope.payload) as unknown; } catch { throw new TaskProtocolError("INVALID_PAYLOAD", "relay envelope payload is not valid task protocol JSON"); }
}

/** First of `envelopeIds`, in their order, that is quarantined and matches; indexed per id, not a quarantine scan. */
function firstQuarantined(store: TaskStore, envelopeIds: readonly string[], matches: (record: OutboxQuarantineRecord) => boolean = () => true): OutboxQuarantineRecord | undefined {
	for (const id of envelopeIds) { const record = store.getQuarantine(id); if (record && matches(record)) return record; }
	return undefined;
}

function throwIfCancellationDeliveryBlocked(options: TaskCoreOptions, taskId: string, envelopeIds: readonly string[]): void {
	const task = requiredTask(options.store, taskId);
	const blocked = firstQuarantined(options.store, envelopeIds, (record) => sameEndpoint(record.envelope.target, task.target));
	if (blocked === undefined) return;
	throw new TaskOutboxDeliveryError(blocked.errorCode, blocked.reason, {
		retryable: false,
		details: { ...blocked.details, taskId, envelopeId: blocked.envelope.envelopeId, target: task.target, blockedAt: blocked.quarantinedAt },
	});
}

function throwIfDeliveryEvidenceBlocked(store: TaskStore, taskId: string, envelopeIds: readonly string[], evidence: DeliveryEvidence): void {
	const blocked = firstQuarantined(store, envelopeIds);
	if (blocked === undefined) return;
	throw new TaskOutboxDeliveryError(blocked.errorCode, "task delivery evidence is blocked", {
		retryable: false,
		details: {
			...blocked.details,
			taskId,
			eventId: evidence.eventId,
			stage: evidence.stage,
			state: evidence.state,
			envelopeId: blocked.envelope.envelopeId,
			target: blocked.envelope.target,
			blockedAt: blocked.quarantinedAt,
		},
	});
}

function throwIfTerminalDeliveryBlocked(store: TaskStore, taskId: string): void {
	const delivery = requiredTask(store, taskId).terminalDelivery;
	if (delivery.state !== "delivery_blocked") return;
	throw new TaskOutboxDeliveryError(delivery.error.code, "terminal task delivery is blocked", {
		retryable: false,
		details: { ...delivery.error.details, taskId, envelopeId: delivery.envelopeId, origin: delivery.origin, blockedAt: delivery.blockedAt },
	});
}

function requiredTask(store: TaskStore, taskId: string): TaskSnapshot {
	const task = store.getTask(taskId);
	if (!task) throw new TaskProtocolError("UNKNOWN_TASK", `unknown task: ${taskId}`, { retryable: false });
	return task;
}

function statusFor(type: string): TaskRecord["status"] {
	return TASK_EVENT_TYPES[type]?.status ?? "timed_out";
}

function isInboxPage(value: unknown): value is { readonly deliveries: readonly RelayDelivery[]; readonly nextCursor: string; readonly hasMore: boolean } {
	return isRecord(value) && Array.isArray(value.deliveries) && decimal(value.nextCursor) && typeof value.hasMore === "boolean";
}

function isDelivery(value: unknown): value is RelayDelivery {
	return isRecord(value) && decimal(value.cursor) && isEnvelope(value.envelope);
}

function isEnvelope(value: unknown): value is RelayEnvelope {
	return isRecord(value) && nonEmpty(value.envelopeId) && value.protocolVersion === TASK_PROTOCOL_VERSION && nonEmpty(value.taskId)
		&& isEndpoint(value.source) && isEndpoint(value.target) && isEnvelopeKind(value.kind)
		&& typeof value.payload === "string" && new TextEncoder().encode(value.payload).byteLength <= MAX_RELAY_PAYLOAD_BYTES;
}

function isTaskRecordInput(value: unknown): value is Omit<TaskRecord, "events"> {
	return isRecord(value) && nonEmpty(value.taskId) && value.protocolVersion === TASK_PROTOCOL_VERSION && isEndpoint(value.origin) && isEndpoint(value.target)
		&& nonEmpty(value.task) && finiteNumber(value.createdAt) && finiteNumber(value.expiresAt) && isTaskStatus(value.status);
}

function isTaskEvent(value: unknown): value is TaskEvent {
	return isRecord(value) && nonEmpty(value.eventId) && nonEmpty(value.taskId) && nonEmpty(value.type) && positiveDecimal(value.sequence)
		&& isEndpoint(value.source) && isEndpoint(value.target) && finiteNumber(value.occurredAt) && isRecord(value.payload);
}

function isTaskIntent(value: unknown): value is TaskIntent {
	return isRecord(value) && nonEmpty(value.intentId) && nonEmpty(value.taskId) && typeof value.type === "string" && isRecord(value.payload);
}

function isEndpoint(value: unknown): value is TaskEndpoint {
	return isRecord(value) && nonEmpty(value.relay) && nonEmpty(value.id);
}

function isEnvelopeKind(value: unknown): value is RelayEnvelope["kind"] {
	return value === TaskEnvelopeKind.assignment || value === TaskEnvelopeKind.intent || value === TaskEnvelopeKind.canonicalEvent;
}

function isTaskStatus(value: unknown): value is TaskRecord["status"] {
	return value === "active" || value === "completed" || value === "failed" || value === "cancelled" || value === "timed_out";
}

function sameEndpoint(left: TaskEndpoint, right: TaskEndpoint): boolean {
	return left.relay === right.relay && left.id === right.id;
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return value !== null && typeof value === "object" && !Array.isArray(value);
}

function nonEmpty(value: unknown): value is string {
	return typeof value === "string" && value.length > 0;
}

function finiteNumber(value: unknown): value is number {
	return typeof value === "number" && Number.isFinite(value);
}

function decimal(value: unknown): value is string {
	return typeof value === "string" && /^(0|[1-9][0-9]*)$/.test(value);
}

function positiveDecimal(value: unknown): value is string {
	return typeof value === "string" && /^[1-9][0-9]*$/.test(value);
}

function throwIfAborted(signal: AbortSignal | undefined): void {
	if (signal?.aborted) throw new TaskProtocolError("ABORTED", "task relay connection was cancelled");
}
