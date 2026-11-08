import { execFile } from "node:child_process";
import { createInterface } from "node:readline";

import { createClaudeTaskServer } from "./mcp-server";
import type { CommandRunner } from "./wake";

/** Runs an argv array without a shell. A non-zero exit is a result, not an error: callers read stdout either way. */
const runCommand: CommandRunner = (argv, options) => new Promise((resolve, reject) => {
	const [file, ...args] = argv;
	if (!file) { reject(new Error("empty command")); return; }
	execFile(file, args, { timeout: options.timeoutMs, maxBuffer: 1024 * 1024, encoding: "utf8", windowsHide: true }, (error, stdout, stderr) => {
		if (error && typeof error.code !== "number") { reject(error); return; }
		resolve({ exitCode: error ? Number(error.code) : 0, stdout, stderr });
	});
});

// Stdout carries only JSON-RPC. Route stray console output from any bundled code to stderr.
console.log = console.info = console.debug = (...values: unknown[]) => console.error(...values);

const server = createClaudeTaskServer({
	env: process.env,
	send: (message) => { process.stdout.write(`${JSON.stringify(message)}\n`); },
	runCommand,
	log: (message) => { process.stderr.write(`${message}\n`); },
});

let stopping: Promise<void> | undefined;
const stop = (): Promise<void> => {
	stopping ??= server.shutdown().catch(() => undefined).finally(() => process.exit(0));
	return stopping;
};

// Requests run concurrently: a long agent_task_wait must not block pings, cancellations or other tools.
createInterface({ input: process.stdin, crlfDelay: Number.POSITIVE_INFINITY })
	.on("line", (line) => { void server.handleLine(line); })
	.on("close", () => { void stop(); });
for (const signal of ["SIGINT", "SIGTERM", "SIGHUP"] as const) process.on(signal, () => { void stop(); });
