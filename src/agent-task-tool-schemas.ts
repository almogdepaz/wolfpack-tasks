import { Type } from "typebox";

/** Same JSON as `StringEnum` from pi-ai without options; kept local so non-pi hosts need no pi packages. */
function stringEnum<const T extends readonly string[]>(values: T) {
	return Type.Unsafe<T[number]>({ type: "string", enum: values });
}

export const MIN_TASK_TIMEOUT_MS = 1_000;
export const MAX_TASK_TIMEOUT_MS = 86_400_000;
export const SUMMARY_MAX_CHARS = 1_200;

const EndpointParams = Type.Object({
	relay: Type.String({ minLength: 1, description: "relay identifier" }),
	id: Type.String({ minLength: 1, description: "opaque endpoint identifier" }),
});
export const SendParams = Type.Object({
	to: EndpointParams,
	task: Type.String({ minLength: 1, maxLength: 16 * 1024 }),
	timeoutMs: Type.Optional(Type.Integer({ minimum: MIN_TASK_TIMEOUT_MS, maximum: MAX_TASK_TIMEOUT_MS })),
});
export const TaskIdParams = Type.Object({ taskId: Type.String({ minLength: 1 }) });
export const WaitParams = Type.Object({ taskId: Type.String({ minLength: 1 }), timeoutMs: Type.Optional(Type.Integer({ minimum: MIN_TASK_TIMEOUT_MS, maximum: MAX_TASK_TIMEOUT_MS })) });
export const InboxParams = Type.Object({});
export const MessageParams = Type.Object({ taskId: Type.String({ minLength: 1 }), type: stringEnum(["question", "answer", "information"] as const), message: Type.String({ minLength: 1, maxLength: 16 * 1024 }) });
export const DoneParams = Type.Object({
	taskId: Type.String({ minLength: 1 }), status: stringEnum(["completed", "failed", "cancelled"] as const), summary: Type.String({ minLength: 1, maxLength: SUMMARY_MAX_CHARS }),
	result: Type.Optional(Type.Record(Type.String(), Type.Unknown())),
	error: Type.Optional(Type.Object({ code: Type.String(), message: Type.String(), retryable: Type.Boolean() })),
	artifacts: Type.Optional(Type.Array(Type.Object({ path: Type.String({ minLength: 1, description: "artifact paths are receiver-project-relative regular files" }), mimeType: Type.Optional(Type.String()), description: Type.Optional(Type.String()) }))),
});
