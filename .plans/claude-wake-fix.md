# claude wake and inbox fix

status: approved, implementation assigned (2026-11-11)
branch: `claude-wake-fix` from `origin/main` `0b11e8e`
worktree: `/Users/home/Dev/.worktrees/wolfpack-pi-tasks/claude-wake-fix`
scope: Claude Code adapter only (`src/claude/`, its tests, README). do not change the shared runtime, the Pi extension, or the task protocol.

## problems (observed on a live Claude parent session, 2026-11-11)

1. **one-way fallback latch.** one channel pointer unanswered for 15s, then a successful paste, sets `mode = "fallback"` for the process lifetime (`src/claude/wake.ts:160`). in fallback mode `requestPointer` sends no channel pointer (`wake.ts:178`). every later wake is typed into the terminal. when the submit is skipped (`wake.ts:159`), `nudged` stays true, `activity` cannot re-nudge (`wake.ts:192`), and a new pointer waits for the 120s expiry (`wake.ts:176`). the user saw stray typed `wolfpack task mail` text in their prompt.
2. **oversized inbox result.** `agent_task_inbox` returned 12 historical tasks (~69k characters) and exceeded Claude Code's tool output limit. the bulk is `structuredContent` with full `listTasks()` snapshots and every event (`src/claude/adapter.ts:336-344`). acknowledged tasks are never evicted.
3. **stale diagnostic (nit).** a successful nudge does not clear `lastNudgeError` (`wake.ts:162`), so the status file shows an old error.

## decided design

- keep the content-free channel pointer plus `agent_task_inbox`. do not push task content in the channel: Claude acknowledges insertion only through an inbox tool result, and an MCP notification has no delivery ack.
- steps, each with a regression test observed failing before the change (TDD):
  1. **trim the inbox result.** in the Claude adapter `mcpResult` path, `structuredContent.tasks` becomes `{ taskId, status }` per task; keep `delivered` intact; keep the active assignment text the runtime re-shows on each inbox call. full records stay available through `agent_task_status`. test: many tasks with long event histories give a result under a fixed byte bound, with `delivered` and active assignment text present. existing `tests/claude-adapter.test.ts` expectations that still hold must keep passing; change only those that pin the old full snapshot.
  2. **send the channel pointer in both modes.** fallback mode only shortens the grace to `fallbackGraceMs`. tests: invert the assertion in `tests/claude-wake.test.ts` (~line 79) that pins no channel pointer in fallback; a skipped submit in fallback mode still emitted a channel pointer for that mail.
  3. **reversible latch.** when `inboxCalled` arrives for an outstanding pointer that was not nudged, return to `mode = "channel"` and report it in the status file. test: fallback, then a pointer answered before any nudge restores `channel` and the 15s grace.
  4. **clear `lastNudgeError`** on a successful submit and on `inboxCalled` (`wake.ts:162,186`). test via the reported status changes.
  5. **README.** update the Claude wake/fallback bullets (`README.md` ~125-126) to match.
- rebuild the bundled Claude plugin with `bun run build:claude` if `claude-plugin/dist` is tracked, and commit the rebuilt output in the same commit as its source change or as a final build commit.

## verification

- focused: `bun test tests/claude-wake.test.ts tests/claude-adapter.test.ts`, `bun run typecheck`.
- final gate once on the last commit: `bun test` (full suite). report actual counts and exit codes; save full output to a private artifact file and declare its path.
- do not contact the live wolfpack server (port 18790), do not create or close sessions, do not install packages (`node_modules` is a symlink to the agent-comms worktree's installed dependencies).

## commit boundary

commit each step separately with a clear message (`git -c commit.gpgsign=false commit`). do not push, do not merge, do not change remotes or branch config.

## pre-authorized implementer decisions

- exact field names and status-file key for the mode restore, within the existing `WakeStatus` shape.
- the byte bound used in the inbox size test.
- whether step 4 shares a commit with step 3.

anything outside `src/claude/`, its tests, the README, and `claude-plugin/dist` needs the coordinator first.

## status

- [x] implementation (pi implementer, gpt-6.1-sol:high)
  - implemented steps 1–5 with red/green regression evidence and separate commits; rebuilt the tracked Claude plugin. focused tests: 36 pass, 0 fail; typecheck and build: exit 0. verification unsets inherited worker-mode flags for the test process. review and release remain separate coordinator gates.
- [ ] review (pi reviewer, gpt-6-astra:high, read-only)
- [ ] release, reinstall, live smoke test (coordinator, needs user approval)
