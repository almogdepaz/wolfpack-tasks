import { readFileSync } from "node:fs";

import { WORKER_GATE_DENIAL_CODE } from "../task-worker-mode";
import { decideWorkerGateHook, pidAlive } from "./worker-gate";

// Entry point of the plugin's fail-closed PreToolUse command hook (dist/worker-gate.mjs). Exit 2 blocks the tool call.
let result;
try {
	result = decideWorkerGateHook({
		env: process.env,
		readStdin: () => readFileSync(0, "utf8"),
		readFile: (path) => readFileSync(path, "utf8"),
		pidAlive,
		now: Date.now(),
	});
} catch (error) {
	// Unreachable by design; any other exit code would also block under onFailure: "block".
	result = { exitCode: 2 as const, stderr: `${WORKER_GATE_DENIAL_CODE}: native tools are blocked: worker gate failed: ${error instanceof Error ? error.message : String(error)}` };
}
if (result.stderr) process.stderr.write(`${result.stderr}\n`);
process.exitCode = result.exitCode;
