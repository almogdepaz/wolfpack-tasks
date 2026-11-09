import { randomBytes } from "node:crypto";
import { chmodSync, lstatSync, mkdirSync, renameSync, rmSync, unlinkSync } from "node:fs";
import { createConnection, createServer } from "node:net";
import type { Socket } from "node:net";
import { dirname, join } from "node:path";

import { taskWorkerModeEnabled, WORKER_GATE_DENIAL_CODE } from "../task-worker-mode";
import { CLAUDE_PLUGIN, HARNESS_PRE_ASSIGNMENT_TOOLS } from "./constants";
import { readClaudeEnvironment } from "./environment";

/**
 * Fail-closed worker gate for Claude Code native tools.
 *
 * The `mcp_tool` `PreToolUse` hook fails open when the MCP server is disconnected, errors or times out. A `command`
 * hook with `onFailure: "block"` does not: Claude Code counts a hook that cannot start, times out, or exits with a code
 * other than 0 or 2 as exit code 2, which blocks the tool call. That hook runs `decideWorkerGateHook`, which asks the
 * running MCP server over a unix-domain socket and denies unless the server answers now with an authorization for this
 * session. Only a listening server can answer, so a stopped, killed or hung server denies: there is no stored
 * authorization to outlive it. This module and its script import no task runtime, so the script starts quickly.
 *
 * Protocol (one line each way, JSON, newline-terminated):
 *   request `{"v":1,"session":<WOLFPACK_SESSION_ID>,"tool":<tool name>}`
 *   reply   `{"v":1,"session":<server session id>,"authorized":<boolean>,"reason":<string|null>}`
 */

export const WORKER_GATE_PROTOCOL_VERSION = 1;
/** The hook denies when the server has not answered within this time; the hook itself has a 10 s Claude Code timeout. */
export const WORKER_GATE_TIMEOUT_MS = 3_000;
/** The server waits at most this long for a fresh evaluation, then answers from the latest one. */
export const WORKER_GATE_ANSWER_WAIT_MS = 1_000;
export const WORKER_GATE_REQUEST_MAX_BYTES = 4_096;
export const WORKER_GATE_REPLY_MAX_BYTES = 16_384;
/** Bound on the denial text the hook prints; Claude Code shows it to the model. */
export const WORKER_GATE_REASON_MAX_CHARS = 8_192;
/** macOS `sun_path` holds 104 bytes including the terminating NUL; Linux holds 108. The smaller limit applies. */
export const UNIX_SOCKET_PATH_MAX_BYTES = 103;
const TOOL_NAME_MAX_CHARS = 256;

export const socketPathFits = (path: string): boolean => Buffer.byteLength(path) <= UNIX_SOCKET_PATH_MAX_BYTES;

export interface WorkerGateAnswer {
	readonly authorized: boolean;
	/** The runtime's denial reason when unauthorized; null when authorized or when the runtime gave none. */
	readonly reason: string | null;
}

export type WorkerGateQueryResult = ({ readonly ok: true } & WorkerGateAnswer) | { readonly ok: false; readonly detail: string };

export interface WorkerGateHookInput {
	readonly env: Readonly<Record<string, string | undefined>>;
	/** Read only in a Wolfpack worker session. */
	readStdin(): string;
	readonly timeoutMs?: number;
}

export interface WorkerGateHookResult {
	/** 0 allows; 2 blocks the tool call and shows `stderr` to the model. */
	readonly exitCode: 0 | 2;
	readonly stderr: string;
}

const ALLOW: WorkerGateHookResult = Object.freeze({ exitCode: 0, stderr: "" });

function bound(text: string, max: number): string {
	return text.length > max ? `${text.slice(0, max - 1)}…` : text;
}

function deny(reason: string): WorkerGateHookResult {
	const text = reason.startsWith(WORKER_GATE_DENIAL_CODE) ? reason : `${WORKER_GATE_DENIAL_CODE}: ${reason}`;
	return { exitCode: 2, stderr: bound(text, WORKER_GATE_REASON_MAX_CHARS) };
}

const blocked = (detail: string): WorkerGateHookResult => deny(`native tools are blocked: ${detail}; call agent_task_inbox for an assignment, or check that the wolfpack-tasks MCP server is running`);

/** Decides one `PreToolUse` call. Outside a Wolfpack worker session it allows without reading input or contacting a server. */
export async function decideWorkerGateHook(input: WorkerGateHookInput): Promise<WorkerGateHookResult> {
	if (!taskWorkerModeEnabled(input.env)) return ALLOW;
	const environment = readClaudeEnvironment(input.env);
	if (environment.kind === "inactive") return ALLOW;
	if (environment.kind === "misconfigured") return blocked(`wolfpack tasks misconfigured: ${environment.variable} ${environment.reason}`);

	let toolName: unknown;
	try { toolName = (JSON.parse(input.readStdin()) as { tool_name?: unknown } | null)?.tool_name; } catch { /* handled below */ }
	if (typeof toolName !== "string" || toolName === "") return blocked("hook input is unreadable");
	// Own tools are gated inside the MCP server with their validated input.
	if (toolName.startsWith(CLAUDE_PLUGIN.toolPrefix) || HARNESS_PRE_ASSIGNMENT_TOOLS.has(toolName)) return ALLOW;

	const path = environment.gateSocketPath;
	if (!socketPathFits(path)) return blocked(`task server socket path is longer than ${UNIX_SOCKET_PATH_MAX_BYTES} bytes: ${path}`);
	const answer = await queryWorkerGate(path, { sessionId: environment.sessionId, toolName }, input.timeoutMs ?? WORKER_GATE_TIMEOUT_MS);
	if (!answer.ok) return blocked(answer.detail);
	if (!answer.authorized) return deny(answer.reason ?? WORKER_GATE_DENIAL_CODE);
	return ALLOW;
}

/** Asks the server at `path` once. Every failure, including no answer within `timeoutMs`, is a result, never a throw. */
export function queryWorkerGate(path: string, request: { readonly sessionId: string; readonly toolName: string }, timeoutMs = WORKER_GATE_TIMEOUT_MS): Promise<WorkerGateQueryResult> {
	return new Promise((resolve) => {
		let settled = false;
		let received = Buffer.alloc(0);
		const socket = createConnection(path);
		const finish = (result: WorkerGateQueryResult): void => {
			if (settled) return;
			settled = true;
			clearTimeout(timer);
			socket.destroy();
			resolve(result);
		};
		const failed = (detail: string): void => finish({ ok: false, detail });
		const timer = setTimeout(() => failed(`task server did not answer within ${timeoutMs} ms`), timeoutMs);
		socket.on("connect", () => {
			socket.write(`${JSON.stringify({ v: WORKER_GATE_PROTOCOL_VERSION, session: request.sessionId, tool: request.toolName.slice(0, TOOL_NAME_MAX_CHARS) })}\n`);
		});
		socket.on("data", (chunk: Buffer) => {
			received = Buffer.concat([received, chunk]);
			const end = received.indexOf(0x0a);
			if (end > WORKER_GATE_REPLY_MAX_BYTES || (end < 0 && received.length > WORKER_GATE_REPLY_MAX_BYTES)) { failed(`task server sent an oversized answer (more than ${WORKER_GATE_REPLY_MAX_BYTES} bytes)`); return; }
			if (end >= 0) finish(parseReply(received.subarray(0, end).toString("utf8"), request.sessionId));
		});
		socket.on("end", () => failed("task server closed the connection without an answer"));
		socket.on("close", () => failed("task server closed the connection without an answer"));
		socket.on("error", (error: NodeJS.ErrnoException) => failed(connectionFailure(error)));
	});
}

function connectionFailure(error: NodeJS.ErrnoException): string {
	if (error.code === "ENOENT") return "task server socket is missing (the server is not running)";
	if (error.code === "ECONNREFUSED") return "task server is not running (connection refused)";
	return `cannot reach the task server (${error.code ?? error.message})`;
}

function parseReply(line: string, sessionId: string): WorkerGateQueryResult {
	let reply: unknown;
	try { reply = JSON.parse(line); } catch { return { ok: false, detail: "task server sent a malformed answer" }; }
	if (typeof reply !== "object" || reply === null) return { ok: false, detail: "task server sent a malformed answer" };
	const fields = reply as Record<string, unknown>;
	if (fields.v !== WORKER_GATE_PROTOCOL_VERSION) return { ok: false, detail: `task server speaks protocol version ${bound(JSON.stringify(fields.v) ?? "none", 32)}, expected ${WORKER_GATE_PROTOCOL_VERSION}` };
	if (fields.session !== sessionId) return { ok: false, detail: "task server answered for another session" };
	if (typeof fields.authorized !== "boolean" || !(fields.reason === null || typeof fields.reason === "string")) return { ok: false, detail: "task server sent a malformed answer" };
	return { ok: true, authorized: fields.authorized, reason: fields.reason };
}

export interface WorkerGateListener {
	/** Stops answering, ends open connections, and unlinks the socket unless a newer listener replaced it. */
	close(): Promise<void>;
}

export interface WorkerGateListenOptions {
	readonly path: string;
	readonly sessionId: string;
	/** The server's current gate decision; a rejection answers with a denial carrying its message. */
	answer(): Promise<WorkerGateAnswer>;
	log(message: string): void;
}

/**
 * Listens for gate requests at `path` in a mode-0700 directory. Returns undefined, after logging why, when it cannot
 * listen; the hook then denies. The socket is bound at a temporary name and renamed into place: the rename replaces a
 * stale socket atomically, and closing unlinks only the temporary name (Node and Bun unlink a unix socket's bound path
 * on close), so a closing listener cannot remove a newer listener's socket.
 */
export async function listenWorkerGate(options: WorkerGateListenOptions): Promise<WorkerGateListener | undefined> {
	const { path, sessionId, log } = options;
	const failed = (reason: string): undefined => { log(`wolfpack-tasks: worker gate is not listening, native tools stay blocked: ${reason}`); return undefined; };
	if (!socketPathFits(path)) return failed(`socket path is longer than ${UNIX_SOCKET_PATH_MAX_BYTES} bytes: ${path}`);
	const directory = dirname(path);
	try {
		mkdirSync(directory, { recursive: true, mode: 0o700 });
		chmodSync(directory, 0o700);
	} catch (error) {
		return failed(`cannot prepare ${directory}: ${message(error)}`);
	}
	try {
		if (!lstatSync(path).isSocket()) return failed(`${path} exists and is not a socket`);
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code !== "ENOENT") return failed(`cannot inspect ${path}: ${message(error)}`);
	}

	const sockets = new Set<Socket>();
	const server = createServer((socket) => {
		sockets.add(socket);
		socket.on("close", () => sockets.delete(socket));
		socket.on("error", () => undefined);
		socket.setTimeout(WORKER_GATE_TIMEOUT_MS, () => socket.destroy());
		let received = Buffer.alloc(0);
		let handled = false;
		socket.on("data", (chunk: Buffer) => {
			if (handled) return;
			received = Buffer.concat([received, chunk]);
			const end = received.indexOf(0x0a);
			if (end > WORKER_GATE_REQUEST_MAX_BYTES || (end < 0 && received.length > WORKER_GATE_REQUEST_MAX_BYTES)) { handled = true; socket.destroy(); return; }
			if (end < 0) return;
			handled = true;
			void respond(received.subarray(0, end).toString("utf8")).then((answer) => {
				if (socket.destroyed) return;
				const reason = answer.reason === null ? null : bound(answer.reason, WORKER_GATE_REASON_MAX_CHARS);
				socket.end(`${JSON.stringify({ v: WORKER_GATE_PROTOCOL_VERSION, session: sessionId, authorized: answer.authorized, reason })}\n`);
			});
		});
	});
	const respond = async (line: string): Promise<WorkerGateAnswer> => {
		let request: unknown;
		try { request = JSON.parse(line); } catch { return refuse("malformed request"); }
		if (typeof request !== "object" || request === null) return refuse("malformed request");
		const fields = request as Record<string, unknown>;
		if (fields.v !== WORKER_GATE_PROTOCOL_VERSION) return refuse(`unsupported protocol version ${bound(JSON.stringify(fields.v) ?? "none", 32)}`);
		if (fields.session !== sessionId) return refuse("request for another session");
		if (typeof fields.tool !== "string") return refuse("malformed request");
		try { return await options.answer(); } catch (error) { return refuse(`evaluation failed: ${message(error)}`); }
	};

	const temporary = join(directory, `${randomBytes(8).toString("hex")}.tmp`);
	try {
		await new Promise<void>((resolve, reject) => { server.once("error", reject); server.listen(temporary, () => { server.off("error", reject); resolve(); }); });
		chmodSync(temporary, 0o600);
		renameSync(temporary, path);
	} catch (error) {
		await closeServer(server, sockets);
		try { rmSync(temporary, { force: true }); } catch { /* best effort */ }
		return failed(`cannot listen at ${path}: ${message(error)}`);
	}
	const inode = lstatSync(path).ino;
	server.on("error", (error) => log(`wolfpack-tasks: worker gate socket error: ${message(error)}`));

	let closing: Promise<void> | undefined;
	return {
		close() {
			closing ??= (async () => {
				// Unlink first so new hooks see a missing socket, but only our own: a newer listener may have replaced it.
				try { if (lstatSync(path).ino === inode) unlinkSync(path); } catch { /* already gone */ }
				await closeServer(server, sockets);
			})();
			return closing;
		},
	};
}

function closeServer(server: ReturnType<typeof createServer>, sockets: Set<Socket>): Promise<void> {
	return new Promise((resolve) => {
		for (const socket of sockets) socket.destroy();
		if (!server.listening) { resolve(); return; }
		server.close(() => resolve());
	});
}

const refuse = (reason: string): WorkerGateAnswer => ({ authorized: false, reason: `${WORKER_GATE_DENIAL_CODE}: worker gate ${reason}` });
const message = (error: unknown): string => error instanceof Error ? error.message : String(error);
