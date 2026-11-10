import { expect, test } from "bun:test";
import { Glob } from "bun";
import { outboxFailureStatus as taskFailureStatus } from "../src/extension";
import { RELAY_LOSS_CODES, RESET_CODES, RETRY_CODES, TASK_ERRORS, isBlockedOutboxDeliveryCode, taskErrorClass } from "../src/task-errors";
import { TaskOutboxDeliveryError, TaskProtocolError } from "../src/task-protocol";

test("derived code sets preserve baseline resets/loss and add only typed relay storage retries", () => {
	expect([...RETRY_CODES].sort()).toEqual(["PEER_UNREACHABLE", "RELAY_CAPACITY", "RELAY_STORAGE_BUSY", "RELAY_STORAGE_RECOVERING", "RELAY_STORAGE_UNAVAILABLE", "RELAY_STORAGE_UNCERTAIN", "RELAY_UNAVAILABLE"]);
	expect([...RESET_CODES].sort()).toEqual(["CALLER_DEAD", "CALLER_NOT_FOUND", "REGISTRATION_EXPIRED", "RELAY_RESET", "SOURCE_MISMATCH"]);
	expect([...RELAY_LOSS_CODES].sort()).toEqual(["RELAY_REBIND_REQUIRED", "RELAY_RESET"]);
	const blocked = Object.keys(TASK_ERRORS).filter(isBlockedOutboxDeliveryCode).sort();
	expect(blocked).toEqual(["CROSS_RELAY_ENDPOINT", "DELIVERY_UNCONFIRMED", "ENVELOPE_CONFLICT", "ENVELOPE_EXPIRED", "INVALID_RELAY_METADATA", "TARGET_NOT_REGISTERED"]);
	// Local deferral is a retryable budget outcome, never blocked delivery.
	expect(isBlockedOutboxDeliveryCode("TASK_DELIVERY_DEFERRED")).toBe(false);
	expect(isBlockedOutboxDeliveryCode("SOMETHING_NEW")).toBe(false);
});

test("the registry covers every error code src/ throws or branches on", async () => {
	const thrown = new Set<string>();
	for await (const file of new Glob("src/**/*.ts").scan(".")) {
		const source = await Bun.file(file).text();
		// The literal code argument(s) of every error constructor and transport failure, including ternaries.
		for (const call of source.matchAll(/(?:new TaskProtocolError|new TaskOutboxDeliveryError|\bfailure)\(([^,]*),/g)) for (const code of call[1]!.matchAll(/"([A-Z][A-Z0-9_]+)"/g)) thrown.add(code[1]!);
		for (const constant of source.matchAll(/const \w+_CODE = "([A-Z][A-Z0-9_]+)"/g)) thrown.add(constant[1]!);
		for (const reply of source.matchAll(/error: \{ code: "([A-Z][A-Z0-9_]+)"/g)) thrown.add(reply[1]!);
	}
	expect(thrown.size).toBeGreaterThan(40);
	expect(thrown).toContain("RELAY_CAPACITY");
	expect([...thrown].filter(code => !(code in TASK_ERRORS)).sort()).toEqual([]);
	// Server codes the transport branches on without throwing them literally.
	for (const code of ["CALLER_DEAD", "CALLER_NOT_FOUND", "INVALID_REQUEST", "PEER_POLICY_REQUIRED", "PEER_UNREACHABLE"]) expect(TASK_ERRORS).toHaveProperty(code);
});

test("the status line names class, code and operation, and an unknown code falls back to a generic class", () => {
	expect(taskFailureStatus(new TaskProtocolError("RELAY_CAPACITY", "relay is at capacity", { details: { operation: "send" } }))).toBe("tasks: capacity (RELAY_CAPACITY) on send");
	expect(taskFailureStatus(new TaskOutboxDeliveryError("TARGET_NOT_REGISTERED", "target is inactive", { retryable: false, details: { operation: "send" } }))).toBe("tasks: delivery (TARGET_NOT_REGISTERED) on send");
	expect(taskErrorClass("SOMETHING_NEW")).toBe("error");
	expect(taskFailureStatus(new TaskProtocolError("SOMETHING_NEW", "new server code", { details: { operation: "receive" } }))).toBe("tasks: error (SOMETHING_NEW) on receive");
	// Without a known operation the established labels remain.
	expect(taskFailureStatus(new TaskOutboxDeliveryError("TARGET_NOT_REGISTERED", "target is inactive", { retryable: false }))).toBe("tasks: outbox degraded");
	expect(taskFailureStatus(new TaskProtocolError("RELAY_CAPACITY", "relay is at capacity"))).toBe("tasks: relay unavailable");
	expect(taskFailureStatus(new Error("transient"))).toBe("tasks: relay unavailable");
	// Actionable relay-state labels take precedence over the generic format.
	expect(taskFailureStatus(new TaskProtocolError("RELAY_RESET", "reset", { retryable: false, details: { operation: "send" } }))).toBe("tasks: relay state lost; rebinding automatically");
	expect(taskFailureStatus(new TaskProtocolError("RELAY_AUTH_REQUIRED", "auth", { retryable: false, details: { operation: "connect" } }))).toBe("tasks: Wolfpack authentication required");
	expect(taskFailureStatus(new TaskProtocolError("RELAY_PROFILE_REQUIRED", "profile", { retryable: false, details: { operation: "connect" } }))).toBe("tasks: compatible memory-owned relay required");
});
