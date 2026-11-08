import { CLAUDE_PLUGIN } from "./constants";

/** Relay-side bound for a held delivery to reach its receiver's model; the fallback must finish well inside it. */
export const RELAY_WAKE_DEADLINE_MS = 120_000;

export interface WakeTiming {
	/** Channel mode: how long a pointer may stay unanswered before the fallback is considered. */
	readonly channelGraceMs: number;
	/** Fallback mode: the same delay once the channel was found not to wake the model. */
	readonly fallbackGraceMs: number;
	/** After a Stop hook, the quiet period before an idle session is nudged. */
	readonly settleMs: number;
	/** Per wolfpack CLI command. */
	readonly commandTimeoutMs: number;
}

export const DEFAULT_WAKE_TIMING: WakeTiming = Object.freeze({ channelGraceMs: 15_000, fallbackGraceMs: 2_000, settleMs: 3_000, commandTimeoutMs: 10_000 });

export type WakeMode = "channel" | "fallback";
export type SessionActivity = "busy" | "idle";

export interface WakeTimers {
	now(): number;
	/** Returns a cancel function. */
	setTimeout(fire: () => void, ms: number): () => void;
}

export interface CommandResult {
	readonly exitCode: number;
	readonly stdout: string;
	readonly stderr: string;
}

/** Runs an argv array without a shell. */
export type CommandRunner = (argv: readonly string[], options: { readonly timeoutMs: number }) => Promise<CommandResult>;

export interface WakeStatusPatch {
	readonly wake?: WakeMode;
	readonly lastPointerAt?: number;
	readonly lastInboxAt?: number;
	readonly lastNudgeAt?: number;
	readonly fallbackAt?: number;
	readonly lastNudgeError?: string;
}

export interface WakeControllerOptions {
	readonly sessionId: string;
	sendChannel(meta: Readonly<Record<string, string>>): void;
	readonly runCommand: CommandRunner;
	readonly timers: WakeTimers;
	readonly timing?: WakeTiming;
	onChange(patch: WakeStatusPatch): void;
}

export interface WakeController {
	readonly mode: WakeMode;
	/** True from a pointer request until `inboxCalled`. */
	readonly pointerOutstanding: boolean;
	/** Coalesced: at most one pointer is outstanding until `inboxCalled`. */
	requestPointer(meta: Readonly<Record<string, string>>): void;
	inboxCalled(): void;
	activity(state: SessionActivity): void;
	close(): void;
}

interface Outstanding {
	readonly at: number;
	/** The once-per-pointer nudge ran its sends (successfully or not). */
	nudged: boolean;
	nudging: boolean;
}

/**
 * Channel pointer plus the D6 fallback. Activity comes from Claude Code hooks: UserPromptSubmit and PreToolUse mean
 * busy, Stop means idle. A fresh session starts idle because no turn has begun. The fallback never runs while busy.
 */
export function createWakeController(options: WakeControllerOptions): WakeController {
	const timing = options.timing ?? DEFAULT_WAKE_TIMING;
	let mode: WakeMode = "channel";
	let activity: SessionActivity = "idle";
	let outstanding: Outstanding | undefined;
	let cancelTimer: (() => void) | undefined;
	let closed = false;

	const grace = (): number => mode === "channel" ? timing.channelGraceMs : timing.fallbackGraceMs;
	const schedule = (delayMs: number): void => {
		cancelTimer?.();
		cancelTimer = options.timers.setTimeout(() => { cancelTimer = undefined; check(); }, Math.max(0, delayMs));
	};
	const check = (): void => {
		const pending = outstanding;
		if (closed || !pending || pending.nudged || pending.nudging || activity === "busy") return;
		const remaining = pending.at + grace() - options.timers.now();
		if (remaining > 0) { schedule(remaining); return; }
		void nudge(pending);
	};
	const command = async (argv: readonly string[]): Promise<CommandResult> => options.runCommand(argv, { timeoutMs: timing.commandTimeoutMs });
	const nudge = async (pending: Outstanding): Promise<void> => {
		pending.nudging = true;
		try {
			const status = await command(["wolfpack", "session", "status", options.sessionId, "--json"]);
			const reason = readyFailure(status, options.sessionId);
			if (reason !== undefined) { options.onChange({ lastNudgeError: reason }); return; }
			// Busy or acknowledged meanwhile: the next Stop hook re-arms the check.
			if (closed || outstanding !== pending || activity === "busy") return;
			pending.nudged = true;
			const at = options.timers.now();
			const paste = await command(["wolfpack", "session", "send", options.sessionId, CLAUDE_PLUGIN.pointer, "--json"]);
			if (paste.exitCode !== 0) { options.onChange({ lastNudgeAt: at, lastNudgeError: `session send exited ${paste.exitCode}` }); return; }
			if (mode === "channel") { mode = "fallback"; options.onChange({ wake: mode, fallbackAt: at }); }
			const submit = await command(["wolfpack", "session", "send", options.sessionId, "\r", "--no-enter", "--json"]);
			options.onChange(submit.exitCode === 0 ? { lastNudgeAt: at } : { lastNudgeAt: at, lastNudgeError: `session submit exited ${submit.exitCode}` });
		} catch (error) {
			options.onChange({ lastNudgeError: error instanceof Error ? error.message : String(error) });
		} finally {
			pending.nudging = false;
		}
	};

	return {
		get mode() { return mode; },
		get pointerOutstanding() { return outstanding !== undefined; },
		requestPointer(meta) {
			if (closed || outstanding) return;
			const at = options.timers.now();
			outstanding = { at, nudged: false, nudging: false };
			if (mode === "channel") options.sendChannel(meta);
			options.onChange({ lastPointerAt: at });
			schedule(grace());
		},
		inboxCalled() {
			outstanding = undefined;
			cancelTimer?.();
			cancelTimer = undefined;
			if (!closed) options.onChange({ lastInboxAt: options.timers.now() });
		},
		activity(state) {
			activity = state;
			if (state === "busy") { cancelTimer?.(); cancelTimer = undefined; return; }
			const pending = outstanding;
			if (closed || !pending || pending.nudged) return;
			schedule(Math.max(timing.settleMs, pending.at + grace() - options.timers.now()));
		},
		close() {
			closed = true;
			cancelTimer?.();
			cancelTimer = undefined;
		},
	};
}

/** Undefined when `wolfpack session status --json` shows this live, ready terminal; otherwise the reason. */
function readyFailure(result: CommandResult, sessionId: string): string | undefined {
	let status: unknown;
	try { status = JSON.parse(result.stdout); } catch { return `session status unreadable (exit ${result.exitCode})`; }
	if (!isRecord(status) || status.ok !== true || status.sessionId !== sessionId) return "session status does not match this session";
	const terminal = status.terminal;
	if (status.state !== "active" || !isRecord(terminal) || terminal.exists !== true || terminal.alive !== true || terminal.status !== "ready") return "session terminal is not live and ready";
	return undefined;
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return value !== null && typeof value === "object" && !Array.isArray(value);
}
