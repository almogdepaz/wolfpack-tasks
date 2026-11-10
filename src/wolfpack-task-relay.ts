import { wolfpackEndpointView } from "./wolfpack-endpoint-view";
import { INVALID_RELAY_METADATA, TASK_PROTOCOL_VERSION, TaskProtocolError } from "./task-protocol";
import type { RelayEnvelope, TaskEndpoint } from "./task-protocol";

export { WOLFPACK_TASK_RELAY_ID } from "./wolfpack-endpoint-view";
export const WIRE_VERSION = 2;
interface WolfpackRelayEnvelope {
  readonly envelopeId: string; readonly protocolVersion: number;
  readonly source: TaskEndpoint; readonly target: TaskEndpoint;
  readonly payload: unknown; readonly createdAt: string;
}

/** Wire codec only. The retired durable HTTP adapter is intentionally absent. */
export function toWolfpackEnvelope(envelope: RelayEnvelope): WolfpackRelayEnvelope {
  const createdAt = envelope.createdAt;
  if (typeof createdAt !== "string" || !Number.isFinite(Date.parse(createdAt)) || new Date(Date.parse(createdAt)).toISOString() !== createdAt) {
    throw new TaskProtocolError(INVALID_RELAY_METADATA, "relay envelope requires an immutable creation timestamp", {
      retryable: false, details: { envelopeId: envelope.envelopeId, reason: createdAt === undefined ? "missing" : "invalid" },
    });
  }
  let payload: unknown;
  try { payload = JSON.parse(envelope.payload) as unknown; }
  catch { throw new TaskProtocolError("INVALID_PAYLOAD", "task relay envelope payload is not valid JSON"); }
  return { envelopeId: envelope.envelopeId, protocolVersion: WIRE_VERSION, source: envelope.source, target: envelope.target,
    payload: { taskId: envelope.taskId, kind: envelope.kind, payload }, createdAt };
}

export function fromWolfpackEnvelope(envelope: WolfpackRelayEnvelope): RelayEnvelope {
  if (!text(envelope.envelopeId) || envelope.protocolVersion !== WIRE_VERSION || !endpoint(envelope.source) || !endpoint(envelope.target)) throw new TaskProtocolError("INVALID_ENVELOPE", "Wolfpack relay returned an invalid envelope");
  const body = envelope.payload;
  if (!record(body) || !text(body.taskId) || !["assignment", "intent", "canonical_event"].includes(String(body.kind)) || !("payload" in body)) throw new TaskProtocolError("INVALID_ENVELOPE", "Wolfpack relay payload does not identify a task envelope");
  const kind = body.kind as RelayEnvelope["kind"];
  const payload = JSON.stringify(wolfpackEndpointView(envelope.source, envelope.target, kind, body.payload));
  if (payload === undefined) throw new TaskProtocolError("INVALID_PAYLOAD", "Wolfpack relay envelope payload is not JSON-serializable");
  return { envelopeId: envelope.envelopeId, protocolVersion: TASK_PROTOCOL_VERSION, source: envelope.source, target: envelope.target, taskId: body.taskId, kind, payload, createdAt: envelope.createdAt };
}
const text = (value: unknown): value is string => typeof value === "string" && value.length > 0;
const record = (value: unknown): value is Record<string, unknown> => value !== null && typeof value === "object" && !Array.isArray(value);
const endpoint = (value: unknown): value is TaskEndpoint => record(value) && text(value.relay) && text(value.id);
