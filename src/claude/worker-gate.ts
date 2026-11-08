import { chmodSync, mkdirSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";

import { taskWorkerModeEnabled, WORKER_GATE_DENIAL_CODE } from "../task-worker-mode";
import { CLAUDE_PLUGIN, HARNESS_PRE_ASSIGNMENT_TOOLS } from "./constants";
import { readClaudeEnvironment } from "./environment";

/**
 * Fail-closed worker gate for Claude Code native tools.
 *
 * The `mcp_tool` `PreToolUse` hook fails open when the MCP server is disconnected, errors or times out. A `command`
 * hook with `onFailure: "block"` does not: Claude Code counts a hook that cannot start, times out, or exits with a code
 * other than 0 or 2 as exit code 2, which blocks the tool call. That hook runs `decideWorkerGateHook`, which reads the
 * gate state the MCP server writes and denies unless the state proves a current authorization. This module and its
 * script import no task runtime, so the script starts quickly and does not depend on the MCP connection.
 */

export const WORKER_GATE_FILE_VERSION = 1;
/** The server re-evaluates and rewrites the gate state at this interval while it runs. */
export const WORKER_GATE_HEARTBEAT_MS = 2_000;
/** Gate state older than this denies, so a hung or stopped server cannot leave a stale authorization behind. */
export const WORKER_GATE_TTL_MS = 10_000;
/** Bound on the denial text the hook prints; Claude Code shows it to the model. */
export const WORKER_GATE_REASON_MAX_CHARS = 8_192;

export interface WorkerGateState {
	readonly version: typeof WORKER_GATE_FILE_VERSION;
	readonly sessionId: string;
	/** The MCP server process that evaluated the state; the hook denies when it is not running. */
	readonly pid: number;
	/** Diagnostic only: the server's parent (expected to be Claude Code). The hook does not enforce it. */
	readonly parentPid: number;
	readonly authorized: boolean;
	/** The runtime's denial reason when unauthorized; null when authorized. */
	readonly reason: string | null;
	readonly evaluatedAt: number;
	readonly expiresAt: number;
}

export interface WorkerGateHookInput {
	readonly env: Readonly<Record<string, string | undefined>>;
	/** Read only in a Wolfpack worker session. */
	readStdin(): string;
	readFile(path: string): string;
	pidAlive(pid: number): boolean;
	readonly now: number;
}

export interface WorkerGateHookResult {
	/** 0 allows; 2 blocks the tool call and shows `stderr` to the model. */
	readonly exitCode: 0 | 2;
	readonly stderr: string;
}

const ALLOW: WorkerGateHookResult = Object.freeze({ exitCode: 0, stderr: "" });

function deny(reason: string): WorkerGateHookResult {
	const text = reason.startsWith(WORKER_GATE_DENIAL_CODE) ? reason : `${WORKER_GATE_DENIAL_CODE}: ${reason}`;
	return { exitCode: 2, stderr: text.length > WORKER_GATE_REASON_MAX_CHARS ? `${text.slice(0, WORKER_GATE_REASON_MAX_CHARS - 1)}…` : text };
}

const blocked = (detail: string): WorkerGateHookResult => deny(`native tools are blocked: ${detail}; call agent_task_inbox for an assignment, or check that the wolfpack-tasks MCP server is running`);

/** Decides one `PreToolUse` call. Outside a Wolfpack worker session it allows without reading input or files. */
export function decideWorkerGateHook(input: WorkerGateHookInput): WorkerGateHookResult {
	if (!taskWorkerModeEnabled(input.env)) return ALLOW;
	const environment = readClaudeEnvironment(input.env);
	if (environment.kind === "inactive") return ALLOW;
	if (environment.kind === "misconfigured") return blocked(`wolfpack tasks misconfigured: ${environment.variable} ${environment.reason}`);

	let toolName: unknown;
	try { toolName = (JSON.parse(input.readStdin()) as { tool_name?: unknown } | null)?.tool_name; } catch { /* handled below */ }
	if (typeof toolName !== "string" || toolName === "") return blocked("hook input is unreadable");
	// Own tools are gated inside the MCP server with their validated input.
	if (toolName.startsWith(CLAUDE_PLUGIN.toolPrefix) || HARNESS_PRE_ASSIGNMENT_TOOLS.has(toolName)) return ALLOW;

	let text: string;
	try { text = input.readFile(environment.gatePath); } catch { return blocked("gate state is missing or unreadable"); }
	let state: unknown;
	try { state = JSON.parse(text); } catch { return blocked("gate state is malformed"); }
	if (!isGateState(state)) return blocked("gate state is malformed");
	if (state.sessionId !== environment.sessionId) return blocked("gate state belongs to another session");
	if (!input.pidAlive(state.pid)) return blocked("task server is not running");
	if (input.now >= state.expiresAt) return blocked("gate state expired");
	if (!state.authorized) return deny(state.reason ?? WORKER_GATE_DENIAL_CODE);
	return ALLOW;
}

function isGateState(value: unknown): value is WorkerGateState {
	if (typeof value !== "object" || value === null) return false;
	const state = value as Record<string, unknown>;
	return state.version === WORKER_GATE_FILE_VERSION
		&& typeof state.sessionId === "string"
		&& Number.isSafeInteger(state.pid) && (state.pid as number) > 0
		&& typeof state.authorized === "boolean"
		&& (state.reason === null || typeof state.reason === "string")
		&& Number.isFinite(state.evaluatedAt)
		&& Number.isFinite(state.expiresAt);
}

/** Only a successful signal-0 probe counts; EPERM means another user's process, which is not this server. */
export function pidAlive(pid: number): boolean {
	try { process.kill(pid, 0); return true; } catch { return false; }
}

/**
 * Writes the gate state atomically (mode 0600). When the write fails it removes the old file, so a failed revocation
 * denies instead of leaving the previous authorization in place. Returns false on failure.
 */
export function writeWorkerGateState(path: string, state: WorkerGateState): boolean {
	try {
		mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
		const temporary = `${path}.${process.pid}.tmp`;
		writeFileSync(temporary, `${JSON.stringify(state)}\n`, { mode: 0o600 });
		chmodSync(temporary, 0o600);
		renameSync(temporary, path);
		return true;
	} catch {
		try { rmSync(path, { force: true }); } catch { /* the TTL still expires the old state */ }
		return false;
	}
}
