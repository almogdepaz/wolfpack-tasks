import type { Static, TSchema } from "typebox";

import { abortableSleep } from "./abortable-sleep";
import { DoneParams, InboxParams, MessageParams, SendParams, TaskIdParams, WaitParams } from "./agent-task-tool-schemas";
import { createTaskWorkScope } from "./task-work-scope";
import { taskWorkerModeEnabled, WORKER_GATE_DENIAL_CODE } from "./task-worker-mode";
import { deliverTaskInbox, taskEntryEvidence, type TaskEntryEvidence } from "./task-inbox";
import { RELAY_LOSS_CODES, taskErrorClass } from "./task-errors";
import { TASK_EVENT_TYPES, TaskDeliveryEvidenceState, TaskDeliveryStage, TaskOutboxDeliveryError, TaskProtocolError, isBlockedOutboxDeliveryCode } from "./task-protocol";
import { createConfiguredTaskCore } from "./configured-task-core";
import type { OwnedTaskCore } from "./configured-task-core";
import type { SubmitIntentInput, SubmitIntentOutcome, TaskCore } from "./task-core";
import type { TaskEndpoint, TaskSnapshot } from "./task-protocol";

const DEFAULT_TASK_TIMEOUT_MS = 30 * 60 * 1_000;
/** Background cadence while work is pending (active task, pending outbox, or a worker awaiting assignment). */
export const ACTIVE_POLL_MS = 1_000;
/** Background cadence otherwise. */
export const IDLE_POLL_MS = 5_000;
const TARGET_NOT_REGISTERED_CODE = "TARGET_NOT_REGISTERED";
/** Events that assign a task to their target: the original assignment and a reassignment to a lost target's successor. */
const ASSIGNMENT_EVENTS: ReadonlySet<string> = new Set(["task.created", "task.reassigned"]);
const RELAY_STATE_LOST_CODE = "RELAY_STATE_LOST";
const RELAY_LOSS_CUSTOM_TYPE = "pi-tasks-relay-loss";
const LOST_ASSIGNMENT_MAX_IDS = 8;
const LOST_ASSIGNMENT_ID_MAX_CHARS = 128;
const RELAY_LOSS_RECOVERY_GUIDANCE = "Parent/coordinator: independently verify work and side effects, obtain the worker's fresh taskEndpoint from structured Wolfpack session status, then send a new explicit assignment for the remaining scope. Do not replay the old task ID or infer a canonical outcome from local failure. Worker must wait for that assignment; old history does not authorize work.";
const WAIT_BACKOFF_INITIAL_MS = 250;
const WAIT_BACKOFF_MAX_MS = 2_000;
const PRE_ASSIGNMENT_TOOLS = new Set(["agent_task_inbox", "agent_task_status", "agent_task_wait"]);
const COORDINATOR_ONLY_TASK_TOOLS = new Set(["agent_task_send", "agent_task_cancel", "agent_task_ack"]);
export { WORKER_GATE_DENIAL_CODE };
export const WORKER_COORDINATION_FORBIDDEN_CODE = "PI_TASK_WORKER_COORDINATION_FORBIDDEN";

/** Host operations the runtime needs: insert a model-visible message (optionally waking the model) and append a custom history entry. */
export interface AgentTaskHost {
	sendMessage(
		message: { readonly customType: string; readonly content: string; readonly display: boolean; readonly details: unknown },
		options: { readonly triggerTurn: false } | { readonly triggerTurn: true; readonly deliverAs: "followUp" },
	): void;
	appendEntry(customType: string, data: unknown): void;
}

/** The subset of a host session context the runtime reads. Pi's `ExtensionContext` satisfies it structurally. */
export interface AgentTaskRuntimeContext {
	isIdle(): boolean;
	hasPendingMessages(): boolean;
	readonly sessionManager: { readonly getEntries: () => readonly unknown[] };
	readonly ui: {
		setStatus(key: string, text: string | undefined): void;
		notify(message: string, type?: "info" | "warning" | "error"): void;
		readonly theme: { fg(color: "warning", text: string): string };
	};
	readonly signal?: AbortSignal | undefined;
}

export interface AgentTaskToolResult {
	readonly content: { readonly type: "text"; readonly text: string }[];
	readonly details: unknown;
	readonly terminate?: boolean;
}

export interface AgentTaskToolDefinition<TParams extends TSchema = TSchema> {
	readonly name: string;
	readonly label: string;
	readonly description: string;
	readonly parameters: TParams;
	execute(toolCallId: string, params: Static<TParams>, signal: AbortSignal | undefined, onUpdate: ((update: AgentTaskToolResult) => void) | undefined, context: AgentTaskRuntimeContext): Promise<AgentTaskToolResult>;
}

export interface ToolCallGateEvent {
	readonly toolName: string;
	readonly input: unknown;
}
export interface ToolCallGateDecision {
	readonly block: true;
	readonly reason: string;
}

/** Host lifecycle hooks. Pi maps them one to one to its extension events; other hosts map their own lifecycle onto them. */
export interface AgentTaskLifecycle {
	tool_call(event: ToolCallGateEvent, context: Pick<AgentTaskRuntimeContext, "sessionManager" | "signal">): Promise<ToolCallGateDecision | undefined>;
	session_start(event: unknown, context: AgentTaskRuntimeContext): Promise<void>;
	agent_end(event: unknown, context: AgentTaskRuntimeContext): Promise<void>;
	agent_settled(event: unknown, context: AgentTaskRuntimeContext): Promise<void>;
	turn_end(event?: unknown, context?: AgentTaskRuntimeContext): Promise<void>;
	session_shutdown(event?: unknown, context?: AgentTaskRuntimeContext): Promise<void>;
}

export interface AgentTaskRuntime {
	readonly lifecycle: AgentTaskLifecycle;
	/** Tools in registration order. */
	readonly tools: readonly AgentTaskToolDefinition[];
	/** True when `PI_TASK_WORKER=1` or `WOLFPACK_TASK_WORKER=1` activated the worker gate. */
	readonly workerGateEnabled: boolean;
}

function defineTool<TParams extends TSchema>(tool: AgentTaskToolDefinition<TParams>): AgentTaskToolDefinition {
	return tool as unknown as AgentTaskToolDefinition;
}

export type ConfiguredCoreFactory = (signal?: AbortSignal, rebind?: boolean) => Promise<TaskCore & Partial<Pick<OwnedTaskCore, "close">>>;
export interface ConfiguredCoreLoader extends ConfiguredCoreFactory {
	close(): Promise<void>;
	start(): Promise<void>;
}
export const defaultCoreFactory: ConfiguredCoreFactory = (signal, rebind) => createConfiguredTaskCore({ rebind }, signal);

/** One lifecycle owns setup and its core. Late setup cannot resurrect a closed lifecycle. */
export function createConfiguredCoreLoader(factory: ConfiguredCoreFactory): ConfiguredCoreLoader {
	let corePromise: ReturnType<ConfiguredCoreFactory> | undefined;
	let controller = new AbortController();
	let epoch = 0, closed = false;
	let closing: Promise<void> = Promise.resolve();
	const load: ConfiguredCoreLoader = async (signal, rebind) => {
		if (closed) throw new TaskProtocolError("RELAY_CLOSED", "task lifecycle is closed", { retryable: false });
		if (corePromise) return corePromise;
		const current = epoch;
		const lifetimeSignal = controller.signal;
		const combined = signal ? AbortSignal.any([signal, lifetimeSignal]) : lifetimeSignal;
		const candidate = Promise.resolve().then(async () => {
			if (combined.aborted) throw new TaskProtocolError("ABORTED", "task setup was cancelled");
			const value = await factory(combined, rebind);
			if (current !== epoch || closed || combined.aborted) {
				await value.close?.();
				throw new TaskProtocolError("RELAY_CLOSED", "task lifecycle was replaced", { retryable: false });
			}
			return value;
		});
		corePromise = candidate;
		try { return await candidate; }
		catch (error) { if (corePromise === candidate) corePromise = undefined; throw error; }
	};
	load.start = async () => {
		const current = epoch;
		await closing;
		if (current !== epoch) throw new TaskProtocolError("RELAY_CLOSED", "task lifecycle was stopped during startup", { retryable: false });
		if (closed) { closed = false; controller = new AbortController(); }
	};
	load.close = () => {
		closed = true; epoch++; controller.abort();
		const prior = corePromise; corePromise = undefined;
		const dispose = async () => {
			let value: Awaited<ReturnType<ConfiguredCoreFactory>> | undefined;
			try { value = await prior; } catch { /* Setup failure already owns cleanup. */ }
			await value?.close?.();
		};
		closing = Promise.all([closing, dispose()]).then(() => undefined);
		return closing;
	};
	return load;
}

/** Creates the host-neutral endpoint-owned task runtime using the memory-owned Wolfpack transport by default. */
export function createAgentTaskRuntime(host: AgentTaskHost, core: TaskCore | undefined = undefined, createCore: ConfiguredCoreFactory = defaultCoreFactory): AgentTaskRuntime {
	let inboxContext: AgentTaskRuntimeContext | undefined;
	let backgroundTimer: ReturnType<typeof setTimeout> | undefined;
	let lifecycleEpoch = 0;
	const closingTaskIds = new Set<string>();
	const workerGateEnabled = taskWorkerModeEnabled();
	const ownedLoader = core === undefined ? createConfiguredCoreLoader(createCore) : undefined;
	const configuredCore: ConfiguredCoreFactory = ownedLoader ?? (async (): Promise<TaskCore> => core!);
	let relayLossRecovery: Promise<void> | undefined;
	let pendingRelayLossCount: number | undefined;
	// Bounded diagnostic text from confirmed loss only; never consulted as authorization evidence.
	let lostWorkerAssignmentDiagnostic: string | undefined;
	const workerAssignmentDenial = (): { block: true; reason: string } => ({
		block: true,
		reason: lostWorkerAssignmentDiagnostic === undefined ? WORKER_GATE_DENIAL_CODE : `${WORKER_GATE_DENIAL_CODE}: ${lostWorkerAssignmentDiagnostic}`,
	});
	const refreshInbox = createSingleFlightInboxRefresh(async (signal) => {
		const context = inboxContext;
		const epoch = lifecycleEpoch;
		const isCurrent = (): boolean => inboxContext === context && lifecycleEpoch === epoch;
		if (!context) return undefined;
		// Phased: intake runs ahead of speculative backlog work and ACK repair.
		const work = createTaskWorkScope(signal, "phased");
		signal = work.signal;
		const activeCore = await configuredCore(signal);
		if (!isCurrent()) return undefined;
		let outboxError: unknown;
		try {
			await activeCore.connect(signal, work);
		} catch (error) {
			if (isRelayLoss(error)) return error;
			outboxError = error;
		}
		if (!isCurrent()) return undefined;
		try {
			await activeCore.evaluateTimeouts(signal, work);
		} catch (error) {
			if (isRelayLoss(error)) return error;
			outboxError ??= error;
		}
		if (!isCurrent()) return undefined;
		const guardedCore: TaskCore = {
			...activeCore,
			async receive(receiveSignal, receiveWork) {
				if (!isCurrent()) return [];
				const deliveries = await activeCore.receive(receiveSignal, receiveWork);
				return isCurrent() ? deliveries : [];
			},
			async intakeInbox(intakeSignal, intakeWork) {
				if (!isCurrent()) return [];
				const items = await activeCore.intakeInbox(intakeSignal, intakeWork);
				return isCurrent() ? items : [];
			},
			async shipRequired(shipSignal, shipWork) {
				if (isCurrent()) await activeCore.shipRequired(shipSignal, shipWork);
			},
			async repairAcknowledgements(repairSignal, repairWork) {
				if (isCurrent()) await activeCore.repairAcknowledgements(repairSignal, repairWork);
			},
			async acknowledgeRelayDelivery(cursor, acknowledgementSignal, acknowledgementWork) {
				if (isCurrent()) await activeCore.acknowledgeRelayDelivery(cursor, acknowledgementSignal, acknowledgementWork);
			},
			completeLocalPresentation(eventId) {
				if (isCurrent()) activeCore.completeLocalPresentation(eventId);
			},
			async recordInsertion(input, insertionSignal, insertionWork) {
				if (isCurrent()) await activeCore.recordInsertion(input, insertionSignal, insertionWork);
			},
			async submitIntent(input, intentSignal, intentWork) {
				if (isCurrent()) await activeCore.submitIntent(input, intentSignal, intentWork);
			},
			async flushOutbox(flushSignal, flushWork) {
				if (isCurrent()) await activeCore.flushOutbox(flushSignal, flushWork);
			},
			recordWakeRequest(input, wakeWork) {
				if (!isCurrent()) throw new TaskProtocolError("RELAY_CLOSED", "task lifecycle was replaced", { retryable: false });
				const request = activeCore.recordWakeRequest(input, wakeWork);
				return {
					flush: async (flushSignal?: AbortSignal): Promise<void> => {
						if (!isCurrent()) throw new TaskProtocolError("RELAY_CLOSED", "task lifecycle was replaced", { retryable: false });
						await request.flush(flushSignal);
					},
					// A replaced lifecycle discards its own state; there is nothing left to withdraw.
					discard() { if (isCurrent()) request.discard(); },
				};
			},
		};
		const inboxOutboxError = await deliverTaskInbox({
			sendMessage(message, options) { if (isCurrent()) host.sendMessage(message, options); },
			appendEntry(customType, data) { if (isCurrent()) host.appendEntry(customType, data); },
		}, guardedCore, {
			isIdle: (): boolean => isCurrent() && context.isIdle(),
			hasPendingMessages: (): boolean => !isCurrent() || context.hasPendingMessages(),
			sessionManager: context.sessionManager,
		}, signal, work);
		return outboxError ?? inboxOutboxError;
	});
	const cancelBackgroundRefresh = (): void => {
		if (backgroundTimer) clearTimeout(backgroundTimer);
		backgroundTimer = undefined;
	};
	/** Keep exactly one pending background timer; each lifecycle refresh re-arms it with the next cadence. */
	const scheduleBackgroundRefresh = (epoch: number, delayMs: number): void => {
		cancelBackgroundRefresh();
		backgroundTimer = setTimeout(() => {
			backgroundTimer = undefined;
			if (inboxContext && lifecycleEpoch === epoch) void requestRefresh();
		}, delayMs);
	};
	// Active cadence while a task is active, the outbox is pending, or a gate-enabled worker lacks an eligible assignment.
	// An eligible assignment implies an active task, so the worker clause together with the first reduces to `workerGateEnabled`.
	const nextPollDelay = (activeCore: TaskCore | undefined): number => activeCore !== undefined && (workerGateEnabled || activeCore.hasPendingWork()) ? ACTIVE_POLL_MS : IDLE_POLL_MS;
	const startBackgroundRefresh = (context: AgentTaskRuntimeContext, epoch: number): void => {
		inboxContext = context;
		scheduleBackgroundRefresh(epoch, IDLE_POLL_MS);
	};
	let lifecycleRefresh: Promise<void> | undefined;
	let trailingRefresh = false;
	/** Immediate single-flight lifecycle refresh; requests made while one runs coalesce into one trailing run. */
	const requestRefresh = (): Promise<void> => {
		if (lifecycleRefresh) {
			trailingRefresh = true;
			return lifecycleRefresh;
		}
		const current = (async (): Promise<void> => {
			do {
				trailingRefresh = false;
				const context = inboxContext;
				if (!context) return;
				try {
					await refreshLifecycle(context, lifecycleEpoch);
				} catch (error) {
					// Callers fire and forget, so a failure escaping refreshLifecycle (e.g. a throwing status UI) must not reject.
					console.error("pi-tasks: lifecycle refresh failed", error);
					if (inboxContext === context) {
						try {
							context.ui.setStatus("pi-tasks", context.ui.theme.fg("warning", `tasks: refresh failed: ${error instanceof Error ? error.message : String(error)}`));
						} catch {
							// Status is best effort; the failure is already logged above.
						}
					}
				}
			} while (trailingRefresh);
		})().finally(() => {
			if (lifecycleRefresh === current) lifecycleRefresh = undefined;
		});
		lifecycleRefresh = current;
		return current;
	};
	const announceFreshBinding = (context: AgentTaskRuntimeContext): void => {
		if (pendingRelayLossCount === undefined) return;
		const count = pendingRelayLossCount;
		pendingRelayLossCount = undefined;
		const taskSummary = count === 0 ? "No active tasks required local failure records." : `${count} active ${count === 1 ? "task was" : "tasks were"} marked locally failed with unknown remote outcome.`;
		context.ui.notify(`Relay state was lost; automatically bound a fresh endpoint. ${taskSummary}`, "warning");
	};
	const recoverRelayLoss = async (context: AgentTaskRuntimeContext, epoch: number, activeCore: TaskCore | undefined): Promise<boolean> => {
		if (!ownedLoader || lifecycleEpoch !== epoch || inboxContext !== context) return false;
		// A durable binding failure is not permission to discard assignments or adopt a fresh endpoint.
		if (activeCore?.recoveryProfile === "restart-safe-v1") {
			context.ui.setStatus("pi-tasks", context.ui.theme.fg("warning", "tasks: recovery blocked; journal retained, outcome unknown"));
			return true;
		}
		if (relayLossRecovery) { await relayLossRecovery; return true; }
		const pending = (async (): Promise<void> => {
			const recoveryEpoch = ++lifecycleEpoch;
			const activeTasks = activeCore?.listTasks().filter((task) => task.status === "active") ?? [];
			if (workerGateEnabled && activeCore) {
				const lostAssignments = activeTasks.filter((task) => sameEndpoint(task.target, activeCore.endpoint));
				if (lostAssignments.length > 0) {
					const ids = lostAssignments.slice(0, LOST_ASSIGNMENT_MAX_IDS).map(({ taskId }) => JSON.stringify(taskId.length > LOST_ASSIGNMENT_ID_MAX_CHARS ? `${taskId.slice(0, LOST_ASSIGNMENT_ID_MAX_CHARS)}…` : taskId));
					lostWorkerAssignmentDiagnostic = `${RELAY_STATE_LOST_CODE}; lost assignment IDs (at most ${LOST_ASSIGNMENT_MAX_IDS}, IDs truncated at ${LOST_ASSIGNMENT_ID_MAX_CHARS} chars): ${ids.join(", ")}${lostAssignments.length > ids.length ? "; additional IDs omitted" : ""}. The remote outcome is unknown. ${RELAY_LOSS_RECOVERY_GUIDANCE}`;
				}
			}
			inboxContext = undefined;
			cancelBackgroundRefresh();
			await ownedLoader.close();
			if (recoveryEpoch !== lifecycleEpoch) return;
			for (const task of activeTasks) {
				host.sendMessage({
					customType: RELAY_LOSS_CUSTOM_TYPE,
					content: `## task failed locally\ntask: \`${task.taskId}\`\n\nRelay state was lost; the remote outcome is unknown.\n\n${RELAY_LOSS_RECOVERY_GUIDANCE}`,
					display: true,
					details: { taskId: task.taskId, status: "failed", error: { code: RELAY_STATE_LOST_CODE, message: "relay state was lost; remote outcome is unknown", retryable: false }, remoteOutcome: "unknown" },
				}, { triggerTurn: false });
			}
			pendingRelayLossCount = (pendingRelayLossCount ?? 0) + activeTasks.length;
			closingTaskIds.clear();
			try {
				await ownedLoader.start();
				if (recoveryEpoch !== lifecycleEpoch) return;
				await ownedLoader(undefined, true);
				if (recoveryEpoch !== lifecycleEpoch) return;
				announceFreshBinding(context);
				context.ui.setStatus("pi-tasks", undefined);
			} catch (error) {
				if (recoveryEpoch !== lifecycleEpoch) return;
				context.ui.setStatus("pi-tasks", context.ui.theme.fg("warning", outboxFailureStatus(error)));
			}
			if (recoveryEpoch === lifecycleEpoch) startBackgroundRefresh(context, recoveryEpoch);
		})();
		relayLossRecovery = pending;
		try { await pending; } finally { if (relayLossRecovery === pending) relayLossRecovery = undefined; }
		return true;
	};
	const refreshLifecycle = async (context: AgentTaskRuntimeContext, epoch: number): Promise<void> => {
		const isCurrent = (): boolean => lifecycleEpoch === epoch && inboxContext === context;
		let activeCore: (TaskCore & Partial<Pick<OwnedTaskCore, "close">>) | undefined;
		try {
			activeCore = await configuredCore();
			if (!isCurrent()) return;
			const outboxError = await refreshInbox();
			if (!isCurrent()) return;
			if (isRelayLoss(outboxError) && await recoverRelayLoss(context, epoch, activeCore)) return;
			const lifecycleError = outboxError;
			const status = lifecycleError === undefined ? undefined : outboxFailureStatus(lifecycleError);
			context.ui.setStatus("pi-tasks", status === undefined ? undefined : context.ui.theme.fg("warning", status));
			if (lifecycleError === undefined) announceFreshBinding(context);
		} catch (error) {
			if (isCurrent() && isRelayLoss(error) && await recoverRelayLoss(context, epoch, activeCore)) return;
			if (isCurrent()) context.ui.setStatus("pi-tasks", context.ui.theme.fg("warning", outboxFailureStatus(error)));
		} finally {
			if (isCurrent()) {
				// The chain must outlive a failed cadence evaluation (e.g. an unreadable task snapshot): err toward polling and surface the fault.
				let delay = ACTIVE_POLL_MS;
				let delayError: unknown;
				try {
					delay = nextPollDelay(activeCore);
				} catch (error) {
					delayError = error;
				}
				scheduleBackgroundRefresh(epoch, delay); // Armed before the status write, which may itself throw.
				if (delayError !== undefined) context.ui.setStatus("pi-tasks", context.ui.theme.fg("warning", `tasks: local task state unreadable: ${delayError instanceof Error ? delayError.message : String(delayError)}`));
			}
		}
	};

	const toolCall: AgentTaskLifecycle["tool_call"] = async (event, context) => {
		if (!workerGateEnabled || PRE_ASSIGNMENT_TOOLS.has(event.toolName)) return undefined;
		if (COORDINATOR_ONLY_TASK_TOOLS.has(event.toolName)) return { block: true, reason: WORKER_COORDINATION_FORBIDDEN_CODE };
		try {
			const activeCore = await configuredCore(context.signal);
			const assignments = assignedWorkerTasks(activeCore, taskEntryEvidence(context.sessionManager));
			const taskId = inputTaskId(event.input);
			if (event.toolName === "agent_task_done") {
				const assigned = taskId === undefined ? undefined : assignments.find((task) => task.taskId === taskId);
				if (assigned === undefined || (!eligibleWorkerTask(assigned, closingTaskIds) && assigned.terminalDelivery.state === "not_submitted" && !closingTaskIds.has(assigned.taskId))) {
					return workerAssignmentDenial();
				}
				closingTaskIds.add(assigned.taskId);
				return undefined;
			}
			if (event.toolName === "agent_task_message") {
				const assigned = taskId === undefined ? undefined : assignments.find((task) => task.taskId === taskId);
				return assigned !== undefined && eligibleWorkerTask(assigned, closingTaskIds) ? undefined : workerAssignmentDenial();
			}
			if (assignments.some((task) => eligibleWorkerTask(task, closingTaskIds))) return undefined;
		} catch {
			// Authorization evidence unavailable or malformed: fail closed with the stable public reason.
		}
		return workerAssignmentDenial();
	};

	const lifecycle: AgentTaskLifecycle = { tool_call: toolCall, async session_start(_event, context) {
		closingTaskIds.clear();
		lostWorkerAssignmentDiagnostic = undefined;
		const epoch = ++lifecycleEpoch;
		await ownedLoader?.start();
		if (epoch !== lifecycleEpoch) return;
		startBackgroundRefresh(context, epoch);
		await requestRefresh();
	}, async agent_end(_event, context) {
		if (!inboxContext) return;
		inboxContext = context;
		await requestRefresh();
	}, async agent_settled(_event, context) {
		if (!inboxContext) return;
		inboxContext = context;
		await requestRefresh();
	}, async turn_end() {
		void requestRefresh();
	}, async session_shutdown() {
		closingTaskIds.clear();
		lostWorkerAssignmentDiagnostic = undefined;
		lifecycleEpoch += 1;
		cancelBackgroundRefresh();
		inboxContext = undefined;
		pendingRelayLossCount = undefined;
		await ownedLoader?.close();
	} };

	const tools: AgentTaskToolDefinition[] = [];
	tools.push(defineTool({
		name: "agent_task_send", label: "Send Agent Task", description: "Create a task in this endpoint's RAM and submit its assignment; restart loses active state.", parameters: SendParams,
		async execute(_id, params, signal) {
			try {
				const sent = await (await configuredCore(signal)).createTask({ target: params.to, task: params.task, timeoutMs: params.timeoutMs ?? DEFAULT_TASK_TIMEOUT_MS }, signal);
				void requestRefresh();
				return toolResult(sent, `## task accepted\n- task: \`${sent.taskId}\`\n- target: \`${params.to.relay}/${params.to.id}\`\n- delivery: relay acceptance only`);
			} catch (error) { return taskError(error); }
		},
	}));
	tools.push(defineTool({
		name: "agent_task_status", label: "Task Status", description: "Read local endpoint-owned task state; status is unavailable while its origin is offline.", parameters: TaskIdParams,
		async execute(_id, params, signal, _onUpdate, context) {
			try {
				const activeCore = await configuredCore(signal);
				const task = activeCore.getTask(params.taskId);
				if (!task) return taskError(new TaskProtocolError("UNKNOWN_TASK", `unknown task: ${params.taskId}`, { retryable: false }));
				const deliveryEvidence = taskDeliveryEvidence(task);
				return toolResult({ ...task, deliveryEvidence }, `## task status\n- task: \`${task.taskId}\`\n- status: ${task.status}${receiverAssignment(activeCore, task, context)}${deliveryEvidenceText(deliveryEvidence)}`);
			} catch (error) { return taskError(error); }
		},
	}));
	tools.push(defineTool({
		name: "agent_task_wait", label: "Wait Agent Task", description: "Wait for a locally-known endpoint-owned task to reach a terminal state.", parameters: WaitParams,
		async execute(_id, params, signal, onUpdate) {
			const deadline = Date.now() + (params.timeoutMs ?? DEFAULT_TASK_TIMEOUT_MS);
			try {
				// Fetching belongs to the background cadence; the wait re-checks local state on store changes or backoff.
				void requestRefresh();
				for (let backoff = WAIT_BACKOFF_INITIAL_MS; ; backoff = Math.min(backoff * 2, WAIT_BACKOFF_MAX_MS)) {
					// Resolve the core each round so a relay-loss replacement is re-subscribed.
					const activeCore = await configuredCore(signal);
					const task = activeCore.getTask(params.taskId);
					if (!task) return taskError(new TaskProtocolError("UNKNOWN_TASK", `unknown task: ${params.taskId}`, { retryable: false }));
					if (terminal(task.status)) return toolResult(task, `## task status\n- task: \`${task.taskId}\`\n- status: ${task.status}`);
					if (Date.now() >= deadline) return toolResult({ taskId: params.taskId, status: task.status }, `## task wait\n- task: \`${params.taskId}\`\n- status: ${task.status}\n- wait timed out`);
					onUpdate?.({ content: [{ type: "text", text: `waiting for ${params.taskId}...` }], details: {} });
					await abortableSleep(Math.max(1, Math.min(backoff, deadline - Date.now())), signal, () => new Error("task wait was cancelled"), (wake) => activeCore.onChange(wake));
				}
			} catch (error) { return taskError(error); }
		},
	}));
	tools.push(defineTool({
		name: "agent_task_inbox", label: "Task Inbox", description: "Read local task records after processing relay deliveries without acknowledging task lifecycle.", parameters: InboxParams,
		async execute(_id, _params, signal, _onUpdate, context) {
			try {
				await refreshInbox(signal);
				const activeCore = await configuredCore(signal);
				const tasks = activeCore.listTasks();
				return toolResult({ tasks }, `## task inbox\n${tasks.map((task) => `- \`${task.taskId}\`: ${task.status}${receiverAssignment(activeCore, task, context)}`).join("\n") || "- empty"}`);
			} catch (error) { return taskError(error); }
		},
	}));
	tools.push(defineTool({
		name: "agent_task_message", label: "Message Agent Task", description: "Record a message intent in this lifetime before relay submission.", parameters: MessageParams,
		async execute(_id, params, signal) { try { await (await configuredCore(signal)).submitIntent({ taskId: params.taskId, type: `task.${params.type}`, payload: { message: params.message } }, signal); void requestRefresh(); return toolResult({ taskId: params.taskId }, `## task ${params.type}\n- task: \`${params.taskId}\``); } catch (error) { return taskError(error); } },
	}));
	tools.push(defineTool({
		name: "agent_task_cancel", label: "Cancel Agent Task", description: "Record cancellation in this lifetime before relay submission.", parameters: TaskIdParams,
		async execute(_id, params, signal) {
			try {
				const activeCore = await configuredCore(signal);
				try {
					await activeCore.submitIntent({ taskId: params.taskId, type: "task.cancelled", payload: {} }, signal);
					void requestRefresh();
					return toolResult({ taskId: params.taskId }, `## task cancellation requested\n- task: \`${params.taskId}\``);
				} catch (error) {
					return blockedCancellationResult(activeCore, params.taskId, error) ?? taskError(error);
				}
			} catch (error) { return taskError(error); }
		},
	}));
	tools.push(defineTool({
		name: "agent_task_ack", label: "Acknowledge Agent Task", description: "Record parent acknowledgement as an origin-owned logical event.", parameters: TaskIdParams,
		async execute(_id, params, signal) { try { await (await configuredCore(signal)).acknowledgeParent(params.taskId, signal); void requestRefresh(); return toolResult({ taskId: params.taskId }, `## task acknowledged\n- task: \`${params.taskId}\``); } catch (error) { return taskError(error); } },
	}));
	tools.push(defineTool({
		name: "agent_task_done", label: "Complete Agent Task", description: "Record a terminal intent in this lifetime before relay submission.", parameters: DoneParams,
		async execute(_id, params, signal) {
			try {
				const activeCore = await configuredCore(signal);
				let submission: SubmitIntentOutcome | undefined;
				try {
					const input: SubmitIntentInput = { taskId: params.taskId, type: `task.${params.status}`, payload: { summary: params.summary, ...(params.result === undefined ? {} : { result: params.result }), ...(params.error === undefined ? {} : { error: params.error }), ...(params.artifacts === undefined ? {} : { artifacts: params.artifacts }) } };
					if (activeCore.submitIntentWithOutcome) submission = await activeCore.submitIntentWithOutcome(input, signal);
					else await activeCore.submitIntent(input, signal);
				} catch (error) {
					return blockedDoneResult(activeCore, params.taskId, params.status, error) ?? taskError(error);
				}
				void requestRefresh();
				const task = activeCore.getTask(params.taskId);
				if (!task) return taskError(new TaskProtocolError("UNKNOWN_TASK", `unknown task: ${params.taskId}`, { retryable: false }));
				if (submission?.authority === "origin" && submission.canonicalEvent.type === "task.late_terminal") {
					return {
						...toolResult({ taskId: params.taskId, requestedStatus: params.status, observedCanonicalStatus: task.status, canonicalEvent: submission.canonicalEvent }, `## canonical late terminal recorded\n- task: \`${params.taskId}\`\n- requested status: ${params.status}\n- canonical status remains: ${task.status}\n- ${params.summary}`),
						terminate: true,
					};
				}
				if (submission?.authority === "origin" && task.status === params.status) {
					return {
						...toolResult({ taskId: params.taskId, requestedStatus: params.status, observedCanonicalStatus: task.status, canonicalCompletion: { state: "confirmed", status: task.status } }, `## task ${task.status}\n- task: \`${params.taskId}\`\n- canonical status: ${task.status}\n- ${params.summary}`),
						terminate: true,
					};
				}
				const deliveryOutcome = task.terminalDelivery.state === "accepted" ? "accepted" : "recorded";
				const result = toolResult({ taskId: params.taskId, requestedStatus: params.status, observedCanonicalStatus: task.status, terminalDelivery: task.terminalDelivery }, `## terminal intent ${deliveryOutcome}\n- task: \`${params.taskId}\`\n- requested status: ${params.status}\n- observed canonical status: ${task.status}\n- ${params.summary}`);
				return task.terminalDelivery.state === "accepted" || terminal(task.status) ? { ...result, terminate: true } : result;
			} catch (error) { return taskError(error); }
		},
	}));
	return { lifecycle, tools, workerGateEnabled };
}

export function createSingleFlightInboxRefresh<TValue>(refresh: (signal?: AbortSignal) => Promise<TValue>): (signal?: AbortSignal) => Promise<TValue> {
	let inFlight: Promise<TValue> | undefined;
	return (signal): Promise<TValue> => {
		if (inFlight) return inFlight;
		const current = refresh(signal).finally(() => {
			if (inFlight === current) inFlight = undefined;
		});
		inFlight = current;
		return current;
	};
}

interface TaskDeliveryEvidence {
	readonly receiverReceipt: "not_confirmed" | "confirmed";
	readonly piInsertion: "not_confirmed" | "blocked" | "confirmed";
	readonly wakeAcceptance: "not_confirmed" | "pending" | "confirmed";
	readonly modelExecution: "not_evidenced";
}

function taskDeliveryEvidence(task: TaskSnapshot): TaskDeliveryEvidence {
	let receiverReceipt: TaskDeliveryEvidence["receiverReceipt"] = "not_confirmed";
	let piInsertion: TaskDeliveryEvidence["piInsertion"] = "not_confirmed";
	let wakeAcceptance: TaskDeliveryEvidence["wakeAcceptance"] = "not_confirmed";
	// After a reassignment, evidence describes the latest assignment.
	const assignmentEventId = task.events.findLast((event) => ASSIGNMENT_EVENTS.has(event.type))?.eventId;
	for (const event of task.events) {
		if (event.type !== "task.delivery_receipt" || event.payload.eventId !== assignmentEventId) continue;
		if (event.payload.stage === TaskDeliveryStage.receiverRecorded) receiverReceipt = "confirmed";
		if (event.payload.stage === TaskDeliveryStage.piInsertion && event.payload.state === TaskDeliveryEvidenceState.blocked) piInsertion = "blocked";
		if (event.payload.stage === TaskDeliveryStage.piInserted || event.payload.stage === undefined) {
			receiverReceipt = "confirmed";
			piInsertion = "confirmed";
		}
		// Legacy v0.1.15 receivers still send wake_requested; current receivers send only the highest stage.
		if (event.payload.stage === TaskDeliveryStage.wakeRequested) wakeAcceptance = "pending";
		if (event.payload.stage === TaskDeliveryStage.wakeAccepted) {
			receiverReceipt = "confirmed";
			piInsertion = "confirmed";
			wakeAcceptance = "confirmed";
		}
	}
	return { receiverReceipt, piInsertion, wakeAcceptance, modelExecution: "not_evidenced" };
}

function deliveryEvidenceText(evidence: TaskDeliveryEvidence): string {
	const insertion = evidence.piInsertion === "blocked" ? "blocked; retryable" : evidence.piInsertion;
	return `\n- receiver receipt (RAM): ${evidence.receiverReceipt}\n- Pi insertion: ${insertion}\n- wake acceptance: ${evidence.wakeAcceptance}\n- model execution: ${evidence.modelExecution}`;
}

function toolResult(details: unknown, markdown: string): AgentTaskToolResult {
	return { content: [{ type: "text", text: markdown }], details };
}

function taskError(error: unknown): AgentTaskToolResult {
	const message = error instanceof Error ? error.message : "task operation failed";
	if (error instanceof TaskProtocolError) {
		return toolResult({ ...error.details, error: { code: error.code, message, retryable: error.retryable } }, `## task error\n- ${message}`);
	}
	return toolResult({ error: { code: "TASK_ERROR", message, retryable: true } }, `## task error\n- ${message}`);
}

function blockedCancellationResult(core: TaskCore, taskId: string, error: unknown): AgentTaskToolResult | undefined {
	if (!(error instanceof TaskOutboxDeliveryError) || error.details?.taskId !== taskId) return undefined;
	const task = core.getTask(taskId);
	const warning = task === undefined ? undefined : blockedDeliveryWarning(error, task.target);
	if (task?.status !== "cancelled" || !sameEndpoint(task.origin, core.endpoint) || warning === undefined) return undefined;
	return toolResult({ taskId, status: "cancelled", warnings: [warning] }, `## task cancelled\n- task: \`${taskId}\`\n- canonical status: cancelled\n- target delivery: blocked; receiver incorporation not confirmed`);
}

function blockedDoneResult(core: TaskCore, taskId: string, requestedStatus: string, error: unknown): AgentTaskToolResult | undefined {
	if (!(error instanceof TaskOutboxDeliveryError)) return undefined;
	const task = core.getTask(taskId);
	if (task === undefined) return undefined;
	if (!sameEndpoint(task.origin, core.endpoint) && task.terminalDelivery.state === "delivery_blocked" && task.terminalDelivery.intentType === `task.${requestedStatus}`) {
		return toolResult({
			taskId,
			status: task.status,
			terminalDelivery: task.terminalDelivery,
			error: { code: error.code, message: error.message, retryable: error.retryable },
		}, `## terminal intent delivery blocked\n- task: \`${taskId}\`\n- canonical status: ${task.status}\n- canonical completion not confirmed; retry remains allowed`);
	}
	const warning = blockedDeliveryWarning(error, task.target);
	if (!sameEndpoint(task.origin, core.endpoint) || task.status !== requestedStatus || warning === undefined) return undefined;
	return {
		...toolResult({ taskId, status: task.status, warnings: [warning] }, `## task ${task.status}\n- task: \`${taskId}\`\n- canonical status: ${task.status}\n- target delivery: blocked; receiver incorporation not confirmed`),
		terminate: true,
	};
}

function blockedDeliveryWarning(error: TaskOutboxDeliveryError, target: TaskEndpoint): Readonly<Record<string, unknown>> | undefined {
	if (error.code !== TARGET_NOT_REGISTERED_CODE || error.retryable) return undefined;
	const details = { ...error.details };
	delete details.taskId;
	delete details.envelopeId;
	delete details.target;
	delete details.blockedAt;
	return { code: error.code, message: error.message, retryable: false, delivery: "blocked", target, details };
}

/**
 * Status-line text for a failed refresh. Actionable relay-state labels come first; otherwise
 * `tasks: <class> (<CODE>) on <operation>` when the failed relay operation is known.
 */
export function outboxFailureStatus(error: unknown): string {
	if (error instanceof TaskProtocolError) {
		if (RELAY_LOSS_CODES.has(error.code)) return "tasks: relay state lost; rebinding automatically";
		if (error.code === "RELAY_PROFILE_REQUIRED") return "tasks: compatible memory-owned relay required";
		if (error.code === "RELAY_AUTH_REQUIRED") return "tasks: Wolfpack authentication required";
		const operation = error.details?.operation;
		if (typeof operation === "string") return `tasks: ${taskErrorClass(error.code)} (${error.code}) on ${operation}`;
	}
	return error instanceof TaskOutboxDeliveryError && isBlockedOutboxDeliveryCode(error.code) ? "tasks: outbox degraded" : "tasks: relay unavailable";
}

function isRelayLoss(error: unknown): error is TaskProtocolError {
	return error instanceof TaskProtocolError && RELAY_LOSS_CODES.has(error.code);
}

function terminal(status: string): boolean {
	return Object.values(TASK_EVENT_TYPES).some(type => type.status === status);
}

function receiverAssignment(core: TaskCore, task: TaskSnapshot, context: Pick<AgentTaskRuntimeContext, "sessionManager">): string {
	if (task.status !== "active" || sameEndpoint(task.origin, core.endpoint) || !sameEndpoint(task.target, core.endpoint)) return "";
	const incorporated = taskEntryEvidence(context.sessionManager).incorporated.get(task.taskId);
	if (!task.events.some((event) => ASSIGNMENT_EVENTS.has(event.type) && sameEndpoint(event.target, core.endpoint) && incorporated?.has(event.eventId) === true)) return "";
	return `\n\n## task assignment\n${task.task}`;
}

/** Session evidence is memoized; tasks are read fresh on every call so the gate follows current task state. */
function assignedWorkerTasks(core: TaskCore, evidence: TaskEntryEvidence): readonly TaskSnapshot[] {
	const assignments: TaskSnapshot[] = [];
	for (const [taskId, eventIds] of evidence.incorporated) {
		const task = core.getTask(taskId);
		if (!task || !sameEndpoint(task.target, core.endpoint)) continue;
		if (task.events.some((event) => eventIds.has(event.eventId) && ASSIGNMENT_EVENTS.has(event.type) && sameEndpoint(event.target, core.endpoint) && sameEndpoint(event.source, task.origin))) assignments.push(task);
	}
	return assignments;
}

function eligibleWorkerTask(task: TaskSnapshot, closingTaskIds: ReadonlySet<string>): boolean {
	return task.status === "active" && task.terminalDelivery.state === "not_submitted" && !closingTaskIds.has(task.taskId);
}

function sameEndpoint(left: TaskEndpoint, right: TaskEndpoint): boolean {
	return left.relay === right.relay && left.id === right.id;
}

function inputTaskId(input: unknown): string | undefined {
	return isRecord(input) && typeof input.taskId === "string" ? input.taskId : undefined;
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return value !== null && typeof value === "object" && !Array.isArray(value);
}
