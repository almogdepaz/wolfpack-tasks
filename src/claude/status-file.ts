import { chmodSync, mkdirSync, renameSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";

import type { WakeMode } from "./wake";

export const STATUS_FILE_VERSION = 1;

export interface ClaudeStatus {
	readonly version: typeof STATUS_FILE_VERSION;
	readonly sessionId: string;
	readonly sessionName: string;
	readonly pid: number;
	readonly wake: WakeMode;
	/** The MCP protocol version this server answered. */
	readonly protocolVersion: string;
	/** The version the client proposed, when it was a string. */
	readonly clientProtocolVersion: string | null;
	readonly startedAt: number;
	readonly updatedAt: number;
	readonly lastPointerAt: number | null;
	readonly lastInboxAt: number | null;
	readonly lastNudgeAt: number | null;
	readonly fallbackAt: number | null;
	readonly lastNudgeError: string | null;
	/** Failed wolfpack status checks for the current pointer; a positive value means the fallback is retrying. */
	readonly statusFailures: number;
	/** When a pointer's fallback was given up after its status retries; null after the next inbox call. */
	readonly wakeStalledAt: number | null;
	/** The runtime's last status-line text; null when clear. */
	readonly lastStatus: string | null;
}

export type ClaudeStatusPatch = Partial<Omit<ClaudeStatus, "version" | "sessionId" | "sessionName" | "pid" | "startedAt" | "updatedAt">>;

export interface ClaudeStatusFile {
	readonly path: string;
	update(patch: ClaudeStatusPatch): void;
}

/** Diagnostic status for operators and tests, rewritten atomically on change; never read back as task state. */
export function createStatusFile(path: string, initial: Omit<ClaudeStatus, "version" | "updatedAt">, now: () => number, log: (message: string) => void): ClaudeStatusFile {
	let status: ClaudeStatus = { version: STATUS_FILE_VERSION, ...initial, updatedAt: now() };
	let failed = false;
	const write = (): void => {
		try {
			mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
			const temporary = `${path}.${process.pid}.tmp`;
			writeFileSync(temporary, `${JSON.stringify(status, null, 2)}\n`, { mode: 0o600 });
			chmodSync(temporary, 0o600);
			renameSync(temporary, path);
			failed = false;
		} catch (error) {
			// Best effort: report the first failure of a streak, never fail task work over diagnostics.
			if (!failed) log(`wolfpack-tasks: status file write failed: ${error instanceof Error ? error.message : String(error)}`);
			failed = true;
		}
	};
	write();
	return {
		path,
		update(patch) {
			status = { ...status, ...patch, updatedAt: now() };
			write();
		},
	};
}
