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
	/** Spacing between status checks for the same pointer after a failed or unreadable status. */
	readonly statusRetryMs: number;
	/** Status checks per pointer before its fallback is given up and the pointer is released. */
	readonly statusAttempts: number;
}

export const DEFAULT_WAKE_TIMING: WakeTiming = Object.freeze({ channelGraceMs: 15_000, fallbackGraceMs: 2_000, settleMs: 3_000, commandTimeoutMs: 10_000, statusRetryMs: 5_000, statusAttempts: 3 });

/**
 * A nudged pointer that is still unanswered this long after its nudge is replaced by the next pointer request, so an
 * uncertain or ignored nudge cannot hold later mail forever. It also bounds a repeated paste to one per period.
 */
export const NUDGED_POINTER_EXPIRY_MS = RELAY_WAKE_DEADLINE_MS;

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
	/** Failed status checks for the current pointer. */
	readonly statusFailures?: number;
	/** Set when a pointer's fallback was given up after its status retries; cleared by the next inbox call. */
	readonly wakeStalledAt?: number | null;
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
	/** The once-per-pointer nudge started its terminal sends (successfully or not). */
	nudged: boolean;
	nudgedAt?: number;
	nudging: boolean;
	statusFailures: number;
}

/**
 * Channel pointer plus the D6 fallback. Activity comes from Claude Code hooks: UserPromptSubmit and PreToolUse mean
 * busy, Stop means idle. A fresh session starts idle because no turn has begun. The fallback never runs while busy.
 *
 * Failure policy: a status check has no terminal side effect, so a failed, unreadable or not-ready status (or a runner
 * error such as ENOENT) is retried for the same idle pointer up to `statusAttempts`; then the pointer is released and
 * the next pointer request starts over. A paste or submit that fails, times out or throws is uncertain: the text may
 * already be in the prompt input. It is never repeated and never followed by a submit for that pointer; the pointer
 * expires `NUDGED_POINTER_EXPIRY_MS` after the nudge.
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
	/** Why a nudge for `pending` must not take its next terminal step, or undefined when it may. */
	const fenced = (pending: Outstanding): string | undefined => {
		if (closed) return "controller closed";
		if (outstanding !== pending) return "pointer acknowledged";
		return activity === "busy" ? "session busy" : undefined;
	};
	/** Status patches after an await: a closed controller writes nothing more. */
	const report = (patch: WakeStatusPatch): void => { if (!closed) options.onChange(patch); };
	const command = async (argv: readonly string[]): Promise<CommandResult> => options.runCommand(argv, { timeoutMs: timing.commandTimeoutMs });
	const statusFailed = (pending: Outstanding, reason: string): void => {
		pending.statusFailures++;
		if (closed || outstanding !== pending) return;
		if (pending.statusFailures < timing.statusAttempts) {
			report({ lastNudgeError: reason, statusFailures: pending.statusFailures });
			if (activity === "idle") schedule(timing.statusRetryMs);
			return;
		}
		outstanding = undefined;
		report({ lastNudgeError: `${reason}; fallback given up after ${pending.statusFailures} status checks`, statusFailures: pending.statusFailures, wakeStalledAt: options.timers.now() });
	};
	const nudge = async (pending: Outstanding): Promise<void> => {
		pending.nudging = true;
		try {
			let reason: string | undefined;
			try {
				reason = readyFailure(await command(["wolfpack", "session", "status", options.sessionId, "--json"]), options.sessionId);
			} catch (error) {
				reason = error instanceof Error ? error.message : String(error);
			}
			if (reason !== undefined) { statusFailed(pending, reason); return; }
			// Busy or acknowledged meanwhile: the next Stop hook re-arms the check.
			if (fenced(pending) !== undefined) return;
			const at = options.timers.now();
			pending.nudged = true;
			pending.nudgedAt = at;
			const paste = await command(["wolfpack", "session", "send", options.sessionId, CLAUDE_PLUGIN.pointer, "--json"]);
			if (paste.exitCode !== 0) { report({ lastNudgeAt: at, lastNudgeError: `session send exited ${paste.exitCode}` }); return; }
			// The paste is out; the submit is a second terminal side effect and must not reach a session that changed meanwhile.
			// The pointer text stays unsubmitted in the prompt input, the same best-effort residue as any appended nudge.
			const fence = fenced(pending);
			if (fence !== undefined) { report({ lastNudgeAt: at, lastNudgeError: `submit skipped: ${fence} during paste` }); return; }
			if (mode === "channel") { mode = "fallback"; report({ wake: mode, fallbackAt: at }); }
			const submit = await command(["wolfpack", "session", "send", options.sessionId, "\r", "--no-enter", "--json"]);
			report(submit.exitCode === 0 ? { lastNudgeAt: at } : { lastNudgeAt: at, lastNudgeError: `session submit exited ${submit.exitCode}` });
		} catch (error) {
			report({ lastNudgeError: error instanceof Error ? error.message : String(error) });
		} finally {
			pending.nudging = false;
		}
	};

	return {
		get mode() { return mode; },
		get pointerOutstanding() { return outstanding !== undefined; },
		requestPointer(meta) {
			if (closed) return;
			const at = options.timers.now();
			if (outstanding && !(outstanding.nudgedAt !== undefined && at - outstanding.nudgedAt >= NUDGED_POINTER_EXPIRY_MS)) return;
			outstanding = { at, nudged: false, nudging: false, statusFailures: 0 };
			options.sendChannel(meta);
			options.onChange({ lastPointerAt: at, statusFailures: 0 });
			schedule(grace());
		},
		inboxCalled() {
			outstanding = undefined;
			cancelTimer?.();
			cancelTimer = undefined;
			if (!closed) options.onChange({ lastInboxAt: options.timers.now(), wakeStalledAt: null });
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
