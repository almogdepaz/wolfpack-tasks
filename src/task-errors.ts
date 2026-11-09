// The single error-code registry. It imports nothing from task-protocol so that module can re-export from here.

export type TaskErrorClass = "transport" | "capacity" | "protocol" | "state" | "auth" | "delivery";

/** Class for a code missing from the registry, e.g. a newer server's. */
export const UNKNOWN_TASK_ERROR_CLASS = "error";

interface TaskErrorSpec {
	readonly class: TaskErrorClass;
	/** Default for throwers without better information; an explicit per-throw `retryable` always wins. */
	readonly retryable: boolean;
	readonly message: string;
	/** A transport rejection with this code is retryable whatever the server flag says. */
	readonly forceRetry?: true;
	/** The volatile binding behind this response is gone; the session retires it. */
	readonly resetsBinding?: true;
	/** Relay task state was lost; the extension rebinds automatically. */
	readonly relayLoss?: true;
	/** A non-retryable send outcome that quarantines the envelope as blocked delivery. */
	readonly blocksDelivery?: true;
}

export const TASK_ERRORS: Readonly<Record<string, TaskErrorSpec>> = {
	// transport
	RELAY_STORAGE_BUSY: { class: "capacity", retryable: true, forceRetry: true, message: "another process owns the relay journal" },
	RELAY_STORAGE_RECOVERING: { class: "capacity", retryable: true, forceRetry: true, message: "relay owner recovery is cooling down; request was not admitted" },
	RELAY_STORAGE_RECOVERY_BLOCKED: { class: "state", retryable: false, message: "relay owner termination is unconfirmed; storage remains reserved" },
	RELAY_STORAGE_INVALID: { class: "state", retryable: false, message: "relay journal validation failed; committed state is retained" },
	RELAY_STORAGE_UNAVAILABLE: { class: "transport", retryable: true, forceRetry: true, message: "relay storage is temporarily unavailable; recorded operations are retained" },
	RELAY_STORAGE_UNCERTAIN: { class: "transport", retryable: true, forceRetry: true, message: "relay journal commit outcome is unknown; retry only the recorded protocol identity" },
	RELAY_UNAVAILABLE: { class: "transport", retryable: true, forceRetry: true, message: "relay is unavailable" },
	PEER_UNREACHABLE: { class: "transport", retryable: true, forceRetry: true, message: "peer relay is unreachable" },
	RELAY_TIMEOUT: { class: "transport", retryable: true, message: "relay request timed out" },
	RELAY_BUSY: { class: "transport", retryable: true, message: "relay handshake is already in progress" },
	ABORTED: { class: "transport", retryable: true, message: "request was aborted" },
	INVALID_RESPONSE: { class: "transport", retryable: true, message: "relay response is malformed" },
	// capacity
	RELAY_CAPACITY: { class: "capacity", retryable: true, forceRetry: true, message: "relay is at capacity" },
	TASK_CAPACITY: { class: "capacity", retryable: false, message: "active task state is full" },
	PAYLOAD_TOO_LARGE: { class: "capacity", retryable: false, message: "task payload is too large" },
	TASK_DELIVERY_DEFERRED: { class: "capacity", retryable: true, message: "task delivery deferred by the local work budget or retry cooldown" },
	// auth and policy
	RELAY_AUTH_REQUIRED: { class: "auth", retryable: false, message: "Wolfpack authentication required" },
	PEER_POLICY_REQUIRED: { class: "auth", retryable: false, message: "peer relay policy does not allow this request" },
	UNAUTHORIZED_INTENT: { class: "auth", retryable: false, message: "intent is not authorized for this task" },
	PI_TASK_WORKER_ASSIGNMENT_REQUIRED: { class: "auth", retryable: false, message: "worker tools require an active assignment" },
	PI_TASK_WORKER_COORDINATION_FORBIDDEN: { class: "auth", retryable: false, message: "workers cannot coordinate other tasks" },
	// relay and task state
	TASK_STORAGE_INVALID: { class: "state", retryable: false, message: "task journal validation failed" },
	TASK_STORAGE_BUSY: { class: "state", retryable: true, message: "another process owns this task journal" },
	TASK_STORAGE_UNCERTAIN: { class: "state", retryable: false, message: "task journal commit outcome is unknown; reopen and inspect the stable task ID" },
	TASK_PRESENTATION_UNCONFIRMED: { class: "state", retryable: false, message: "prior harness presentation has no authoritative outcome; automatic execution is fenced" },	RELAY_RESET: { class: "state", retryable: false, resetsBinding: true, relayLoss: true, message: "relay lifetime was reset" },
	RELAY_REBIND_REQUIRED: { class: "state", retryable: false, relayLoss: true, message: "relay binding must be replaced" },
	RELAY_STATE_LOST: { class: "state", retryable: false, message: "relay task state was lost" },
	REGISTRATION_EXPIRED: { class: "state", retryable: false, resetsBinding: true, message: "relay registration expired" },
	SOURCE_MISMATCH: { class: "state", retryable: false, resetsBinding: true, message: "relay source does not match the binding" },
	CALLER_DEAD: { class: "state", retryable: false, resetsBinding: true, message: "calling session is gone" },
	CALLER_NOT_FOUND: { class: "state", retryable: false, resetsBinding: true, message: "calling session is not found" },
	RELAY_CLOSED: { class: "state", retryable: false, message: "task lifetime is closed" },
	UNKNOWN_TASK: { class: "state", retryable: false, message: "unknown task" },
	TASK_NOT_FOUND: { class: "state", retryable: false, message: "task is not active in this endpoint lifetime" },
	TASK_NOT_TERMINAL: { class: "state", retryable: false, message: "only a terminal task may be acknowledged" },
	TERMINAL_INTENT_CONFLICT: { class: "state", retryable: false, message: "a different terminal intent was already submitted" },
	NOT_ORIGIN: { class: "state", retryable: false, message: "only the origin may perform this operation" },
	NOT_PARTICIPANT: { class: "state", retryable: false, message: "endpoint is not a task participant" },
	REASSIGNMENT_REFUSED: { class: "state", retryable: false, message: "task cannot be reassigned" },
	TASK_ERROR: { class: "state", retryable: true, message: "task operation failed" },
	// delivery
	TARGET_NOT_REGISTERED: { class: "delivery", retryable: false, blocksDelivery: true, message: "target is not registered" },
	INVALID_RELAY_METADATA: { class: "delivery", retryable: false, blocksDelivery: true, message: "relay metadata is invalid" },
	DELIVERY_UNCONFIRMED: { class: "delivery", retryable: false, blocksDelivery: true, message: "peer delivery outcome is unknown" },
	ENVELOPE_EXPIRED: { class: "delivery", retryable: false, blocksDelivery: true, message: "envelope expired before delivery" },
	ENVELOPE_CONFLICT: { class: "delivery", retryable: false, blocksDelivery: true, message: "envelope id was reused with different content" },
	CROSS_RELAY_ENDPOINT: { class: "delivery", retryable: false, blocksDelivery: true, message: "endpoint belongs to another relay" },
	UNREGISTERED_TARGET: { class: "delivery", retryable: true, message: "target is not registered for this protocol" },
	UNREGISTERED_SOURCE: { class: "delivery", retryable: true, message: "source is not registered for this protocol" },
	UNKNOWN_RELAY: { class: "delivery", retryable: true, message: "target belongs to another relay" },
	// protocol
	RELAY_PROFILE_REQUIRED: { class: "protocol", retryable: false, message: "compatible task relay profile required" },
	INCOMPATIBLE_PROTOCOL: { class: "protocol", retryable: false, message: "incompatible task protocol" },
	INVALID_REQUEST: { class: "protocol", retryable: false, message: "relay rejected the request as invalid" },
	INVALID_CONNECTION: { class: "protocol", retryable: true, message: "relay connected a different endpoint" },
	INVALID_ACCEPTANCE: { class: "protocol", retryable: true, message: "relay acceptance is malformed" },
	INVALID_INBOX: { class: "protocol", retryable: true, message: "relay inbox page is malformed" },
	INVALID_DELIVERY: { class: "protocol", retryable: true, message: "relay returned an invalid or unordered delivery" },
	INVALID_CURSOR: { class: "protocol", retryable: true, message: "relay cursor is invalid" },
	INVALID_ENVELOPE: { class: "protocol", retryable: true, message: "relay envelope is malformed" },
	INVALID_PAYLOAD: { class: "protocol", retryable: true, message: "relay envelope payload is invalid" },
	INVALID_TARGET: { class: "protocol", retryable: true, message: "target is invalid" },
	INVALID_ENDPOINT: { class: "protocol", retryable: true, message: "endpoint is invalid" },
	INVALID_ENDPOINT_REFERENCE: { class: "protocol", retryable: true, message: "endpoint reference is invalid" },
	INVALID_TASK: { class: "protocol", retryable: true, message: "task and a positive timeout are required" },
	INVALID_ASSIGNMENT: { class: "protocol", retryable: true, message: "assignment is invalid" },
	INVALID_INTENT: { class: "protocol", retryable: true, message: "task intent is invalid" },
	INVALID_EVENT: { class: "protocol", retryable: true, message: "task event is invalid" },
	INVALID_INBOX_EVENT: { class: "protocol", retryable: true, message: "relay envelope does not contain a valid model event" },
	UNKNOWN_EVENT: { class: "protocol", retryable: true, message: "unknown task event type" },
};

function codes(flag: (spec: TaskErrorSpec) => boolean | undefined): ReadonlySet<string> {
	return new Set(Object.entries(TASK_ERRORS).flatMap(([code, spec]) => flag(spec) ? [code] : []));
}

export const RETRY_CODES = codes(spec => spec.forceRetry);
export const RESET_CODES = codes(spec => spec.resetsBinding);
export const RELAY_LOSS_CODES = codes(spec => spec.relayLoss);
const BLOCKED_OUTBOX_DELIVERY_CODES = codes(spec => spec.blocksDelivery);

export function isBlockedOutboxDeliveryCode(code: string): boolean {
	return BLOCKED_OUTBOX_DELIVERY_CODES.has(code);
}

export function taskErrorClass(code: string): TaskErrorClass | typeof UNKNOWN_TASK_ERROR_CLASS {
	return TASK_ERRORS[code]?.class ?? UNKNOWN_TASK_ERROR_CLASS;
}

/** The registry message, or the code itself for an unregistered code. */
export function taskErrorMessage(code: string): string {
	return TASK_ERRORS[code]?.message ?? code;
}
