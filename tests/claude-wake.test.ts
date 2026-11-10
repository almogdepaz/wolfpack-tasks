import { expect, test } from "bun:test";

import { CLAUDE_PLUGIN } from "../src/claude/constants";
import { createWakeController, DEFAULT_WAKE_TIMING, RELAY_WAKE_DEADLINE_MS } from "../src/claude/wake";
import type { CommandResult, WakeStatusPatch } from "../src/claude/wake";
import { virtualTime } from "./virtual-time";

const SESSION_ID = "0b6f3a52-6a39-4c55-9a0e-1c2d3e4f5a6b";
const READY = JSON.stringify({ ok: true, sessionId: SESSION_ID, state: "active", terminal: { exists: true, alive: true, status: "ready" } });
const STATUS = ["wolfpack", "session", "status", SESSION_ID, "--json"];
const PASTE = ["wolfpack", "session", "send", SESSION_ID, CLAUDE_PLUGIN.pointer, "--json"];
const SUBMIT = ["wolfpack", "session", "send", SESSION_ID, "\r", "--no-enter", "--json"];

type Runner = (argv: readonly string[]) => Promise<CommandResult>;

const answer = (status: string): Runner => async (argv) => ({ exitCode: 0, stdout: argv[2] === "status" ? status : "{\"ok\":true}", stderr: "" });

function fixture(status: string = READY, run: Runner = answer(status)) {
	const vt = virtualTime();
	const channel: Record<string, string>[] = [];
	const commands: string[][] = [];
	const patches: WakeStatusPatch[] = [];
	const wake = createWakeController({
		sessionId: SESSION_ID,
		sendChannel(meta) { channel.push(meta); },
		async runCommand(argv): Promise<CommandResult> {
			commands.push([...argv]);
			return run(argv);
		},
		timers: {
			now: vt.now,
			setTimeout(fire, ms) {
				const controller = new AbortController();
				vt.sleep(ms, controller.signal).then(fire, () => undefined);
				return () => controller.abort();
			},
		},
		onChange(patch) { patches.push(patch); },
	});
	return { vt, wake, channel, commands, patches };
}

test("fallback timing, including every status retry, stays inside the relay wake deadline", () => {
	const t = DEFAULT_WAKE_TIMING;
	expect(t.statusAttempts).toBeGreaterThan(1);
	// Grace, the settle after a Stop hook, every status attempt timing out with its retry spacing, then paste and submit.
	const worst = t.channelGraceMs + t.settleMs + t.statusAttempts * t.commandTimeoutMs + (t.statusAttempts - 1) * t.statusRetryMs + 2 * t.commandTimeoutMs;
	expect(worst).toBeLessThan(RELAY_WAKE_DEADLINE_MS);
});

test("one channel pointer is outstanding until the inbox is called", async () => {
	const f = fixture();
	expect(f.wake.pointerOutstanding).toBe(false);
	f.wake.requestPointer({ task_id: "t1", event_id: "e1" });
	f.wake.requestPointer({ task_id: "t2", event_id: "e2" });
	expect(f.channel).toEqual([{ task_id: "t1", event_id: "e1" }]);
	expect(f.wake.pointerOutstanding).toBe(true);
	f.wake.inboxCalled();
	expect(f.wake.pointerOutstanding).toBe(false);
	f.wake.requestPointer({ task_id: "t3", event_id: "e3" });
	expect(f.channel).toHaveLength(2);
	f.wake.close();
});

test("an idle session that ignores the pointer is nudged once through wolfpack, then stays in fallback mode", async () => {
	const f = fixture();
	f.wake.requestPointer({ task_id: "t1" });
	await f.vt.advance(DEFAULT_WAKE_TIMING.channelGraceMs - 1);
	expect(f.commands).toEqual([]);
	await f.vt.advance(1);
	expect(f.commands).toEqual([STATUS, PASTE, SUBMIT]);
	expect(f.wake.mode).toBe("fallback");
	expect(f.patches.some((patch) => patch.wake === "fallback" && typeof patch.fallbackAt === "number")).toBe(true);
	await f.vt.advance(10 * 60_000);
	expect(f.commands).toHaveLength(3);

	// Fallback mode: still emits a channel pointer, with the shorter grace and once per pointer.
	f.wake.inboxCalled();
	f.wake.requestPointer({ task_id: "t2" });
	expect(f.channel).toEqual([{ task_id: "t1" }, { task_id: "t2" }]);
	await f.vt.advance(DEFAULT_WAKE_TIMING.fallbackGraceMs);
	expect(f.commands).toEqual([STATUS, PASTE, SUBMIT, STATUS, PASTE, SUBMIT]);
	f.wake.close();
});

test("fallback mail emits a channel pointer even when its submit is skipped", async () => {
	const paste = suspendedPaste();
	let suspend = false;
	const f = fixture(READY, (argv) => suspend ? paste.run(argv) : answer(READY)(argv));
	f.wake.requestPointer({ task_id: "t1" });
	await f.vt.advance(DEFAULT_WAKE_TIMING.channelGraceMs);
	expect(f.wake.mode).toBe("fallback");
	f.wake.inboxCalled();
	suspend = true;
	f.wake.requestPointer({ task_id: "t2" });
	await f.vt.advance(DEFAULT_WAKE_TIMING.fallbackGraceMs);
	f.wake.activity("busy");
	paste.release();
	await f.vt.advance(1);
	expect(f.commands).toEqual([STATUS, PASTE, SUBMIT, STATUS, PASTE]);
	expect(f.patches.at(-1)?.lastNudgeError).toBe("submit skipped: session busy during paste");
	expect(f.channel).toEqual([{ task_id: "t1" }, { task_id: "t2" }]);
	f.wake.close();
});

test("an unnudged pointer answered in fallback restores channel mode and its full grace", async () => {
	const f = fixture();
	f.wake.requestPointer({ task_id: "t1" });
	await f.vt.advance(DEFAULT_WAKE_TIMING.channelGraceMs);
	f.wake.inboxCalled();
	expect(f.wake.mode).toBe("fallback");
	// An unsolicited inbox call is not evidence that a new channel pointer worked.
	f.wake.inboxCalled();
	expect(f.wake.mode).toBe("fallback");
	f.wake.requestPointer({ task_id: "t2" });
	await f.vt.advance(DEFAULT_WAKE_TIMING.fallbackGraceMs - 1);
	f.wake.inboxCalled();
	expect(f.wake.mode).toBe("channel");
	expect(f.patches.at(-1)).toMatchObject({ wake: "channel", lastInboxAt: f.vt.now() });
	f.wake.requestPointer({ task_id: "t3" });
	await f.vt.advance(DEFAULT_WAKE_TIMING.channelGraceMs - 1);
	expect(f.commands).toEqual([STATUS, PASTE, SUBMIT]);
	await f.vt.advance(1);
	expect(f.commands).toEqual([STATUS, PASTE, SUBMIT, STATUS, PASTE, SUBMIT]);
	f.wake.close();
});

test("a working session is never nudged; the nudge waits for the settled idle hook", async () => {
	const f = fixture();
	f.wake.activity("busy");
	f.wake.requestPointer({ task_id: "t1" });
	await f.vt.advance(5 * 60_000);
	expect(f.commands).toEqual([]);
	f.wake.activity("idle");
	await f.vt.advance(DEFAULT_WAKE_TIMING.settleMs - 1);
	expect(f.commands).toEqual([]);
	f.wake.activity("busy");
	await f.vt.advance(5 * 60_000);
	expect(f.commands).toEqual([]);
	f.wake.activity("idle");
	await f.vt.advance(DEFAULT_WAKE_TIMING.settleMs);
	expect(f.commands).toEqual([STATUS, PASTE, SUBMIT]);
	f.wake.close();
});

test("an inbox call inside the grace cancels the nudge", async () => {
	const f = fixture();
	f.wake.requestPointer({ task_id: "t1" });
	await f.vt.advance(1_000);
	f.wake.inboxCalled();
	await f.vt.advance(5 * 60_000);
	expect(f.commands).toEqual([]);
	expect(f.wake.mode).toBe("channel");
	f.wake.close();
});

test.each([
	["a dead terminal", JSON.stringify({ ok: true, sessionId: SESSION_ID, state: "active", terminal: { exists: true, alive: false, status: "dead" } })],
	["another session", JSON.stringify({ ok: true, sessionId: "other", state: "active", terminal: { exists: true, alive: true, status: "ready" } })],
	["unparseable status", "not json"],
])("%s is not nudged, retried a bounded number of times, and keeps channel mode", async (_label, status) => {
	const f = fixture(status);
	f.wake.requestPointer({ task_id: "t1" });
	await f.vt.advance(DEFAULT_WAKE_TIMING.channelGraceMs);
	expect(f.commands).toEqual([STATUS]);
	await f.vt.advance(10 * 60_000);
	expect(f.commands).toEqual(Array.from({ length: DEFAULT_WAKE_TIMING.statusAttempts }, () => STATUS));
	expect(f.wake.mode).toBe("channel");
	expect(f.patches.some((patch) => typeof patch.lastNudgeError === "string")).toBe(true);
	f.wake.close();
});

/** Suspends the paste command until the test releases it, so lifecycle changes can land between paste and submit. */
function suspendedPaste() {
	let release!: () => void;
	const pasted = new Promise<void>((resolve) => { release = resolve; });
	const ready = answer(READY);
	const run: Runner = async (argv) => {
		if (argv[4] === CLAUDE_PLUGIN.pointer) await pasted;
		return ready(argv);
	};
	return { run, release };
}

test.each([
	["the session turns busy", (f: ReturnType<typeof fixture>) => f.wake.activity("busy")],
	["the inbox is called", (f: ReturnType<typeof fixture>) => f.wake.inboxCalled()],
	["the controller closes", (f: ReturnType<typeof fixture>) => f.wake.close()],
])("no submit follows a paste during which %s", async (_label, change) => {
	const paste = suspendedPaste();
	const f = fixture(READY, paste.run);
	f.wake.requestPointer({ task_id: "t1" });
	await f.vt.advance(DEFAULT_WAKE_TIMING.channelGraceMs);
	expect(f.commands).toEqual([STATUS, PASTE]);
	change(f);
	paste.release();
	await f.vt.advance(5 * 60_000);
	expect(f.commands).toEqual([STATUS, PASTE]);
	// A fenced paste is not a completed nudge: the channel keeps its mode.
	expect(f.wake.mode).toBe("channel");
	f.wake.close();
});

/** Fails the first `failures` status commands with `fail`, then answers like a ready session. */
function flakyStatus(failures: number, fail: () => Promise<CommandResult>): Runner {
	const ready = answer(READY);
	let left = failures;
	return async (argv) => {
		if (argv[2] === "status" && left > 0) { left--; return fail(); }
		return ready(argv);
	};
}
const unavailable = async (): Promise<CommandResult> => ({ exitCode: 1, stdout: "", stderr: "daemon unavailable" });
const missingCli = async (): Promise<CommandResult> => { throw Object.assign(new Error("spawn wolfpack ENOENT"), { code: "ENOENT" }); };

test.each([
	["a failed status response", unavailable],
	["a missing wolfpack CLI (ENOENT)", missingCli],
])("%s is retried while the pointer is outstanding and the session idle, inside the relay deadline", async (_label, fail) => {
	const f = fixture(READY, flakyStatus(1, fail));
	const start = f.vt.now();
	f.wake.requestPointer({ task_id: "t1" });
	await f.vt.advance(DEFAULT_WAKE_TIMING.channelGraceMs);
	expect(f.commands).toEqual([STATUS]);
	expect(f.wake.pointerOutstanding).toBe(true);
	await f.vt.advance(DEFAULT_WAKE_TIMING.statusRetryMs);
	expect(f.commands).toEqual([STATUS, STATUS, PASTE, SUBMIT]);
	expect(f.vt.now() - start).toBeLessThan(RELAY_WAKE_DEADLINE_MS);
	expect(f.wake.mode).toBe("fallback");
	expect(f.patches.some((patch) => patch.statusFailures === 1)).toBe(true);
	f.wake.close();
});

test("a retry waits while the session is busy and resumes after the idle settle", async () => {
	const f = fixture(READY, flakyStatus(1, unavailable));
	f.wake.requestPointer({ task_id: "t1" });
	await f.vt.advance(DEFAULT_WAKE_TIMING.channelGraceMs);
	f.wake.activity("busy");
	await f.vt.advance(5 * 60_000);
	expect(f.commands).toEqual([STATUS]);
	f.wake.activity("idle");
	await f.vt.advance(DEFAULT_WAKE_TIMING.settleMs);
	expect(f.commands).toEqual([STATUS, STATUS, PASTE, SUBMIT]);
	f.wake.close();
});

test("exhausted status retries release the pointer, so later mail points again and can still be nudged", async () => {
	const f = fixture(READY, flakyStatus(DEFAULT_WAKE_TIMING.statusAttempts, missingCli));
	f.wake.requestPointer({ task_id: "t1" });
	await f.vt.advance(10 * 60_000);
	expect(f.commands).toHaveLength(DEFAULT_WAKE_TIMING.statusAttempts);
	expect(f.wake.pointerOutstanding).toBe(false);
	expect(f.patches.some((patch) => typeof patch.wakeStalledAt === "number")).toBe(true);

	f.wake.requestPointer({ task_id: "t2" });
	expect(f.channel).toEqual([{ task_id: "t1" }, { task_id: "t2" }]);
	await f.vt.advance(DEFAULT_WAKE_TIMING.channelGraceMs);
	expect(f.commands.slice(DEFAULT_WAKE_TIMING.statusAttempts)).toEqual([STATUS, PASTE, SUBMIT]);
	f.wake.inboxCalled();
	expect(f.patches.at(-1)).toMatchObject({ wakeStalledAt: null });
	f.wake.close();
});

test.each([
	["exits non-zero", async (): Promise<CommandResult> => ({ exitCode: 1, stdout: "", stderr: "" })],
	["times out", async (): Promise<CommandResult> => { throw new Error("Command failed: wolfpack session send (timed out)"); }],
])("a paste that %s is uncertain: no submit, no repeat for that pointer, and the pointer expires at the relay deadline", async (_label, failPaste) => {
	const ready = answer(READY);
	let pasteFails = true;
	const f = fixture(READY, async (argv) => argv[4] === CLAUDE_PLUGIN.pointer && pasteFails ? failPaste() : ready(argv));
	f.wake.requestPointer({ task_id: "t1" });
	await f.vt.advance(DEFAULT_WAKE_TIMING.channelGraceMs);
	expect(f.commands).toEqual([STATUS, PASTE]);
	f.wake.activity("idle");
	await f.vt.advance(RELAY_WAKE_DEADLINE_MS - 1);
	// Still the same pointer: coalesced, not re-pasted.
	f.wake.requestPointer({ task_id: "t2" });
	expect(f.channel).toHaveLength(1);
	expect(f.commands).toEqual([STATUS, PASTE]);

	pasteFails = false;
	await f.vt.advance(1);
	f.wake.requestPointer({ task_id: "t3" });
	expect(f.channel).toEqual([{ task_id: "t1" }, { task_id: "t3" }]);
	await f.vt.advance(DEFAULT_WAKE_TIMING.channelGraceMs);
	expect(f.commands).toEqual([STATUS, PASTE, STATUS, PASTE, SUBMIT]);
	f.wake.close();
});
