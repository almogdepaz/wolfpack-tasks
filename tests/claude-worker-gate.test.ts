import { afterEach, expect, test } from "bun:test";
import { existsSync, lstatSync, mkdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { createConnection, createServer } from "node:net";
import type { Socket } from "node:net";
import { dirname, join } from "node:path";

import { CLAUDE_PLUGIN } from "../src/claude/constants";
import { readClaudeEnvironment } from "../src/claude/environment";
import { decideWorkerGateHook, listenWorkerGate, queryWorkerGate, UNIX_SOCKET_PATH_MAX_BYTES } from "../src/claude/worker-gate";
import type { WorkerGateAnswer } from "../src/claude/worker-gate";
import { WORKER_GATE_DENIAL_CODE } from "../src/agent-task-runtime";
import { SESSION_ID, shortTempDir } from "./claude-harness";

type Env = Record<string, string>;
const identity = (stateDir: string): Env => ({ WOLFPACK_SESSION_ID: SESSION_ID, WOLFPACK_SESSION_NAME: "claude-worker", WOLFPACK_DIR: stateDir });
const workerEnv = (stateDir = shortTempDir()): Env => ({ ...identity(stateDir), WOLFPACK_TASK_WORKER: "1" });
const socketPath = (env: Env): string => {
	const environment = readClaudeEnvironment(env);
	if (environment.kind !== "ready") throw new Error(`environment is ${environment.kind}`);
	return environment.gateSocketPath;
};
const AUTHORIZED: WorkerGateAnswer = { authorized: true, reason: null };

const cleanups: (() => unknown)[] = [];
afterEach(async () => {
	for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
});

async function decide(env: Env, options: { stdin?: string; timeoutMs?: number } = {}) {
	let read = false;
	const result = await decideWorkerGateHook({
		env,
		readStdin: () => { read = true; return options.stdin ?? JSON.stringify({ tool_name: "Bash" }); },
		...(options.timeoutMs !== undefined && { timeoutMs: options.timeoutMs }),
	});
	return { ...result, read };
}

async function serve(env: Env, answer: () => WorkerGateAnswer | Promise<WorkerGateAnswer>, log: string[] = []) {
	const listener = await listenWorkerGate({ path: socketPath(env), sessionId: SESSION_ID, answer: async () => answer(), log: (message) => { log.push(message); } });
	if (listener) cleanups.push(() => listener.close());
	return listener;
}

/** A listener at the gate path that is not the task server: it handles each connection with `onConnection`. */
async function rawListener(path: string, onConnection: (socket: Socket, request: Promise<string>) => void): Promise<void> {
	mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
	const sockets = new Set<Socket>();
	const server = createServer((socket) => {
		sockets.add(socket);
		socket.on("error", () => undefined);
		const request = new Promise<string>((resolve) => {
			let text = "";
			socket.on("data", (chunk) => { text += chunk; const end = text.indexOf("\n"); if (end >= 0) resolve(text.slice(0, end)); });
		});
		onConnection(socket, request);
	});
	await new Promise<void>((resolve, reject) => { server.once("error", reject); server.listen(path, resolve); });
	cleanups.push(() => new Promise<void>((resolve) => { for (const socket of sockets) socket.destroy(); server.close(() => resolve()); }));
}

/** Sends one raw line to the gate socket and returns everything the server writes before it closes. */
function exchange(path: string, line: string): Promise<string> {
	return new Promise((resolve) => {
		const socket = createConnection(path);
		let text = "";
		socket.on("connect", () => socket.write(line));
		socket.on("data", (chunk) => { text += chunk; });
		socket.on("close", () => resolve(text));
		// The server drops an oversized request; a reset then ends the exchange with what was received.
		socket.on("error", () => resolve(text));
	});
}

// A separate task server process that authorizes every request, so SIGKILL leaves its socket file behind.
const AUTHORIZING_SERVER = `
const net = require("node:net"), fs = require("node:fs"), path = require("node:path");
fs.mkdirSync(path.dirname(process.env.GATE_PATH), { recursive: true, mode: 0o700 });
net.createServer((socket) => socket.once("data", () => socket.end(JSON.stringify({ v: 1, session: process.env.GATE_SESSION, authorized: true, reason: null }) + "\\n")))
	.listen(process.env.GATE_PATH, () => console.log("ready"));
`;

async function startAuthorizingServer(path: string) {
	const child = Bun.spawn([process.execPath, "-e", AUTHORIZING_SERVER], { env: { ...process.env, GATE_PATH: path, GATE_SESSION: SESSION_ID }, stdout: "pipe", stderr: "inherit" });
	cleanups.push(() => { child.kill("SIGKILL"); });
	const reader = child.stdout.getReader();
	let text = "";
	while (!text.includes("ready")) {
		const { value, done } = await reader.read();
		if (done) throw new Error("authorizing server exited before it listened");
		text += new TextDecoder().decode(value);
	}
	reader.releaseLock();
	return child;
}

test.each([
	["no worker variable", identity("/state")],
	["a worker variable without wolfpack identity", { WOLFPACK_TASK_WORKER: "1" }],
	["a worker variable other than 1", { ...identity("/state"), PI_TASK_WORKER: "true" }],
])("outside a wolfpack worker session the hook allows silently: %s", async (_name, env) => {
	expect(await decide(env, { stdin: "not json" })).toEqual({ exitCode: 0, stderr: "", read: false });
});

test.each([`${CLAUDE_PLUGIN.toolPrefix}agent_task_inbox`, "ToolSearch"])("%s is allowed without a task server", async (tool) => {
	expect(await decide(workerEnv(), { stdin: JSON.stringify({ tool_name: tool }) })).toEqual({ exitCode: 0, stderr: "", read: true });
});

test.each([
	["unparseable hook input", { stdin: "not json" }, "hook input is unreadable"],
	["hook input without a tool name", { stdin: "{}" }, "hook input is unreadable"],
] as const)("worker mode denies %s", async (_name, options, detail) => {
	const result = await decide(workerEnv(), options);
	expect(result.exitCode).toBe(2);
	expect(result.stderr).toStartWith(WORKER_GATE_DENIAL_CODE);
	expect(result.stderr).toContain(detail);
});

test("worker mode denies a misconfigured state directory", async () => {
	const result = await decide({ ...workerEnv(), WOLFPACK_DIR: "relative" });
	expect(result).toMatchObject({ exitCode: 2, stderr: expect.stringContaining("WOLFPACK_DIR") });
});

// R1: a server that exited without revoking leaves its socket file, but no process answers on it.
test("a task server killed without revocation leaves a stale socket that denies", async () => {
	const env = workerEnv();
	const path = socketPath(env);
	const server = await startAuthorizingServer(path);
	expect(await decide(env)).toMatchObject({ exitCode: 0, stderr: "" });
	server.kill("SIGKILL");
	await server.exited;
	expect(lstatSync(path).isSocket()).toBe(true);
	const result = await decide(env);
	expect(result.exitCode).toBe(2);
	expect(result.stderr).toStartWith(WORKER_GATE_DENIAL_CODE);
	expect(result.stderr).toContain("connection refused");
});

test("a missing socket denies: the task server is not running", async () => {
	const result = await decide(workerEnv());
	expect(result).toMatchObject({ exitCode: 2, stderr: expect.stringContaining("task server socket is missing") });
});

test("a regular file at the socket path denies", async () => {
	const env = workerEnv();
	const path = socketPath(env);
	mkdirSync(dirname(path), { recursive: true });
	writeFileSync(path, JSON.stringify({ v: 1, session: SESSION_ID, authorized: true, reason: null }));
	const result = await decide(env);
	expect(result).toMatchObject({ exitCode: 2, stderr: expect.stringContaining("cannot reach the task server") });
});

test("a live listener that does not answer denies after the timeout", async () => {
	const env = workerEnv();
	await rawListener(socketPath(env), () => { /* accepts and never answers */ });
	const started = Date.now();
	const result = await decide(env, { timeoutMs: 200 });
	expect(Date.now() - started).toBeLessThan(2_000);
	expect(result).toMatchObject({ exitCode: 2, stderr: expect.stringContaining("task server did not answer within 200 ms") });
});

test("a listener that closes without an answer denies", async () => {
	const env = workerEnv();
	await rawListener(socketPath(env), (socket, request) => { void request.then(() => socket.end()); });
	expect(await decide(env)).toMatchObject({ exitCode: 2, stderr: expect.stringContaining("closed the connection without an answer") });
});

test("the hook sends one bounded request line with the protocol version, session id and tool name", async () => {
	const env = workerEnv();
	const requests: string[] = [];
	await rawListener(socketPath(env), (socket, request) => {
		void request.then((line) => { requests.push(line); socket.end(`${JSON.stringify({ v: 1, session: SESSION_ID, authorized: true, reason: null })}\n`); });
	});
	expect(await decide(env, { stdin: JSON.stringify({ tool_name: `Bash${"x".repeat(5_000)}` }) })).toMatchObject({ exitCode: 0 });
	expect(requests).toHaveLength(1);
	const request = JSON.parse(requests[0]!) as { v: number; session: string; tool: string };
	expect(request).toMatchObject({ v: 1, session: SESSION_ID });
	expect(request.tool).toStartWith("Bashx");
	expect(request.tool.length).toBeLessThanOrEqual(256);
});

test("the task server's authorized answer allows; its unauthorized answer denies with its reason, bounded", async () => {
	const env = workerEnv();
	let answer: WorkerGateAnswer = AUTHORIZED;
	expect(await serve(env, () => answer)).toBeDefined();
	expect(await decide(env)).toEqual({ exitCode: 0, stderr: "", read: true });
	expect(await decide({ ...identity(env.WOLFPACK_DIR!), PI_TASK_WORKER: "1" })).toMatchObject({ exitCode: 0 });

	answer = { authorized: false, reason: `${WORKER_GATE_DENIAL_CODE}: RELAY_STATE_LOST; ${"x".repeat(20_000)}` };
	const denied = await decide(env);
	expect(denied.exitCode).toBe(2);
	expect(denied.stderr).toStartWith(`${WORKER_GATE_DENIAL_CODE}: RELAY_STATE_LOST`);
	expect(denied.stderr.length).toBeLessThanOrEqual(8_192);

	answer = { authorized: false, reason: null };
	expect(await decide(env)).toMatchObject({ exitCode: 2, stderr: WORKER_GATE_DENIAL_CODE });
});

test("a failed evaluation in the task server denies with its message", async () => {
	const env = workerEnv();
	await serve(env, () => { throw new Error("evaluation exploded"); });
	expect(await decide(env)).toMatchObject({ exitCode: 2, stderr: expect.stringContaining("evaluation exploded") });
});

test.each([
	["a reply for another session", `${JSON.stringify({ v: 1, session: "other-session", authorized: true, reason: null })}\n`, "answered for another session"],
	["a reply of another protocol version", `${JSON.stringify({ v: 2, session: SESSION_ID, authorized: true, reason: null })}\n`, "protocol version 2, expected 1"],
	["an unparseable reply", "authorized\n", "malformed answer"],
	["a reply of the wrong shape", `${JSON.stringify({ v: 1, session: SESSION_ID, authorized: "yes", reason: null })}\n`, "malformed answer"],
	["an oversized reply", `${JSON.stringify({ v: 1, session: SESSION_ID, authorized: true, reason: "x".repeat(40_000) })}\n`, "oversized answer"],
	["an oversized reply without a newline", "x".repeat(40_000), "oversized answer"],
])("the hook denies %s", async (_name, reply, detail) => {
	const env = workerEnv();
	await rawListener(socketPath(env), (socket, request) => { void request.then(() => socket.end(reply)); });
	const result = await decide(env);
	expect(result.exitCode).toBe(2);
	expect(result.stderr).toStartWith(WORKER_GATE_DENIAL_CODE);
	expect(result.stderr).toContain(detail);
});

test("the task server refuses requests for another session or protocol version and drops oversized requests", async () => {
	const env = workerEnv();
	await serve(env, () => AUTHORIZED);
	const path = socketPath(env);
	const reply = async (line: string) => JSON.parse(await exchange(path, line)) as { v: number; session: string; authorized: boolean; reason: string | null };
	expect(await reply(`${JSON.stringify({ v: 1, session: SESSION_ID, tool: "Bash" })}\n`)).toEqual({ v: 1, session: SESSION_ID, authorized: true, reason: null });
	expect(await reply(`${JSON.stringify({ v: 1, session: "other-session", tool: "Bash" })}\n`)).toMatchObject({ v: 1, session: SESSION_ID, authorized: false, reason: expect.stringContaining("another session") });
	expect(await reply(`${JSON.stringify({ v: 9, session: SESSION_ID, tool: "Bash" })}\n`)).toMatchObject({ authorized: false, reason: expect.stringContaining("protocol version 9") });
	expect(await reply("not json\n")).toMatchObject({ authorized: false, reason: expect.stringContaining("malformed request") });
	expect(await exchange(path, "x".repeat(10_000))).toBe("");
	expect(await queryWorkerGate(path, { sessionId: "other-session", toolName: "Bash" })).toEqual({ ok: false, detail: expect.stringContaining("another session") });
});

test("a socket path longer than the unix limit is never listened on, and the hook denies naming it", async () => {
	const stateDir = join(shortTempDir(), "d".repeat(80));
	const env = workerEnv(stateDir);
	const path = socketPath(env);
	expect(Buffer.byteLength(path)).toBeGreaterThan(UNIX_SOCKET_PATH_MAX_BYTES);
	const log: string[] = [];
	expect(await serve(env, () => AUTHORIZED, log)).toBeUndefined();
	expect(log).toEqual([expect.stringContaining(path)]);
	expect(existsSync(stateDir)).toBe(false);
	const result = await decide(env);
	expect(result.exitCode).toBe(2);
	expect(result.stderr).toContain(`longer than ${UNIX_SOCKET_PATH_MAX_BYTES} bytes`);
	expect(result.stderr).toContain(path);
});

test("the listener uses a 0700 directory and a 0600 socket, and close unlinks the socket", async () => {
	const env = workerEnv();
	const path = socketPath(env);
	const listener = await serve(env, () => AUTHORIZED);
	expect(statSync(dirname(path)).mode & 0o777).toBe(0o700);
	expect(lstatSync(path).isSocket()).toBe(true);
	expect(lstatSync(path).mode & 0o777).toBe(0o600);
	await listener!.close();
	expect(existsSync(path)).toBe(false);
	expect(await decide(env)).toMatchObject({ exitCode: 2, stderr: expect.stringContaining("task server socket is missing") });
});

test("a restarted listener removes the stale socket of a killed server and answers again", async () => {
	const env = workerEnv();
	const path = socketPath(env);
	const killed = await startAuthorizingServer(path);
	killed.kill("SIGKILL");
	await killed.exited;
	expect(lstatSync(path).isSocket()).toBe(true);
	expect(await serve(env, () => ({ authorized: false, reason: `${WORKER_GATE_DENIAL_CODE}: fresh lifetime` }))).toBeDefined();
	expect(await decide(env)).toMatchObject({ exitCode: 2, stderr: `${WORKER_GATE_DENIAL_CODE}: fresh lifetime` });
});

test("closing an older listener leaves the socket of a newer listener for the same session", async () => {
	const env = workerEnv();
	const older = await serve(env, () => ({ authorized: false, reason: "older" }));
	await serve(env, () => AUTHORIZED);
	await older!.close();
	expect(await decide(env)).toMatchObject({ exitCode: 0 });
});

test("a non-socket file at the socket path is kept and the listener does not start", async () => {
	const env = workerEnv();
	const path = socketPath(env);
	mkdirSync(dirname(path), { recursive: true });
	writeFileSync(path, "keep");
	const log: string[] = [];
	expect(await serve(env, () => AUTHORIZED, log)).toBeUndefined();
	expect(readFileSync(path, "utf8")).toBe("keep");
	expect(log).toEqual([expect.stringContaining("not a socket")]);
});
