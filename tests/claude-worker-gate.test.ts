import { expect, test } from "bun:test";

import { CLAUDE_PLUGIN } from "../src/claude/constants";
import { WORKER_GATE_FILE_VERSION, decideWorkerGateHook } from "../src/claude/worker-gate";
import type { WorkerGateState } from "../src/claude/worker-gate";
import { WORKER_GATE_DENIAL_CODE } from "../src/agent-task-runtime";
import { SESSION_ID } from "./claude-harness";

const NOW = 1_000_000;
const SERVER_PID = 4242;
const STATE_DIR = "/state";
const GATE_PATH = `${STATE_DIR}/${CLAUDE_PLUGIN.statusDirectory}/${SESSION_ID}${CLAUDE_PLUGIN.gateFileSuffix}`;
const IDENTITY = { WOLFPACK_SESSION_ID: SESSION_ID, WOLFPACK_SESSION_NAME: "claude-worker", WOLFPACK_DIR: STATE_DIR };
const WORKER = { ...IDENTITY, WOLFPACK_TASK_WORKER: "1" };
const authorized: WorkerGateState = { version: WORKER_GATE_FILE_VERSION, sessionId: SESSION_ID, pid: SERVER_PID, parentPid: 1, authorized: true, reason: null, evaluatedAt: NOW - 1_000, expiresAt: NOW + 9_000 };

function decide(options: { env?: Record<string, string>; stdin?: string; file?: string | Error; alive?: boolean } = {}) {
	const reads: string[] = [];
	const result = decideWorkerGateHook({
		env: options.env ?? WORKER,
		readStdin: () => options.stdin ?? JSON.stringify({ tool_name: "Bash" }),
		readFile(path) {
			reads.push(path);
			const file = options.file ?? JSON.stringify(authorized);
			if (file instanceof Error) throw file;
			return file;
		},
		pidAlive: (pid) => pid === SERVER_PID && options.alive !== false,
		now: NOW,
	});
	return { ...result, reads };
}

test.each([
	["no worker variable", IDENTITY],
	["a worker variable without wolfpack identity", { WOLFPACK_TASK_WORKER: "1" }],
	["a worker variable other than 1", { ...IDENTITY, PI_TASK_WORKER: "true" }],
])("outside a wolfpack worker session the hook allows silently: %s", (_name, env) => {
	expect(decide({ env, stdin: "not json", file: new Error("unread") })).toEqual({ exitCode: 0, stderr: "", reads: [] });
});

test("a live, unexpired authorization for this session allows native tools", () => {
	expect(decide()).toEqual({ exitCode: 0, stderr: "", reads: [GATE_PATH] });
	expect(decide({ env: { ...IDENTITY, PI_TASK_WORKER: "1" } }).exitCode).toBe(0);
});

test.each([`${CLAUDE_PLUGIN.toolPrefix}agent_task_inbox`, "ToolSearch"])("%s is allowed without gate state", (tool) => {
	expect(decide({ stdin: JSON.stringify({ tool_name: tool }), file: new Error("ENOENT") })).toEqual({ exitCode: 0, stderr: "", reads: [] });
});

test.each([
	["missing state", { file: new Error("ENOENT: no such file") }, "gate state is missing or unreadable"],
	["unparseable state", { file: "{" }, "gate state is malformed"],
	["state of the wrong shape", { file: JSON.stringify({ ...authorized, authorized: "yes" }) }, "gate state is malformed"],
	["state of another version", { file: JSON.stringify({ ...authorized, version: 99 }) }, "gate state is malformed"],
	["state of another session", { file: JSON.stringify({ ...authorized, sessionId: "other-session" }) }, "another session"],
	["a dead server", { alive: false }, "task server is not running"],
	["expired state", { file: JSON.stringify({ ...authorized, expiresAt: NOW }) }, "gate state expired"],
	["unparseable hook input", { stdin: "not json" }, "hook input is unreadable"],
	["hook input without a tool name", { stdin: "{}" }, "hook input is unreadable"],
	["a misconfigured state directory", { env: { ...WORKER, WOLFPACK_DIR: "relative" } }, "WOLFPACK_DIR"],
] as const)("worker mode denies on %s", (_name, options, detail) => {
	const result = decide(options);
	expect(result.exitCode).toBe(2);
	expect(result.stderr).toStartWith(WORKER_GATE_DENIAL_CODE);
	expect(result.stderr).toContain(detail);
});

test("an unauthorized state denies with the server's reason, bounded and prefixed with the denial code", () => {
	const reason = `${WORKER_GATE_DENIAL_CODE}: RELAY_STATE_LOST; ${"x".repeat(20_000)}`;
	const result = decide({ file: JSON.stringify({ ...authorized, authorized: false, reason }) });
	expect(result.exitCode).toBe(2);
	expect(result.stderr).toStartWith(`${WORKER_GATE_DENIAL_CODE}: RELAY_STATE_LOST`);
	expect(result.stderr.length).toBeLessThanOrEqual(8_192);
	const bare = decide({ file: JSON.stringify({ ...authorized, authorized: false, reason: null }) });
	expect(bare).toMatchObject({ exitCode: 2, stderr: WORKER_GATE_DENIAL_CODE });
});
