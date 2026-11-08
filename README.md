# wolfpack-tasks

**give another pi agent a task, exchange messages, and get a structured result back.**

wolfpack-tasks adds coordination tools to [pi](https://pi.dev). a parent agent can hand work to another session, keep working, and receive a follow-up when the worker asks a question or finishes. it works between sessions on one machine and, through wolfpack's trusted tailnet routing, across machines.

for example: an implementer works on a change while the parent investigates something else. the implementer reports its result; the parent checks the diff and tests, then acknowledges the task.

**wolfpack-tasks coordinates work. it does not create agent sessions or execute the assignment itself.** wolfpack manages sessions and carries messages; the agents do the work.

## how it works

```text
parent agent  ── assignment ──>  worker agent
              <── questions ──
              ─── answers ───>
              <─── result ────
              ─ acknowledgment >
```

1. the parent sends instructions to the worker's **task endpoint**: an opaque address for that running session.
2. the relay carries the assignment. the worker's pi extension queues it as a follow-up, without interrupting an active turn.
3. the worker does the work, optionally exchanges messages, and submits a structured completion or failure.
4. the parent receives the result, verifies it, and acknowledges the task.

completion ends the **task**, not the **session**. keep a worker for another assignment, or close it through wolfpack when you no longer need it.

## use with wolfpack

this is the ready-made integration. you need pi, a compatible wolfpack memory-only server, and this package loaded in every participating pi session.

### 1. install the pi package

```bash
pi install npm:@sgtbeatdown/wolfpack-tasks
```

start new pi sessions after installation. wolfpack supplies `WOLFPACK_SESSION_NAME` to its sessions; the extension uses it to register the current process. the local control port defaults to `18790` (`WOLFPACK_PORT` overrides it).

### 2. open a worker

from a pi parent running under wolfpack:

```bash
# choose an available pi model explicitly.
MODEL="openai-codex/gpt-6.1-sol:medium"
wolfpack agent spawn --project-dir /absolute/worktree \
  --name task-implementation --model "$MODEL" \
  --task-worker --readiness-timeout-ms 30000 --json
```

replace `/absolute/worktree` with the directory the worker should use. this starts a prompt-free worker and returns a stable `sessionId` and a registered `taskEndpoint`. registration means the session can receive mail—not that its model has executed anything.

for an existing session, use `wolfpack session status <sessionId> --json`. copy its returned `taskEndpoint` unchanged; never construct an endpoint from a session name.

### 3. assign work

ask the parent agent to call `agent_task_send` with that endpoint:

```json
{
  "to": {
    "relay": "wolfpack-pi-tasks-v2",
    "id": "target-opaque-endpoint-id"
  },
  "task": "implement the narrow change and run focused tests",
  "timeoutMs": 3600000
}
```

replace `to` with the actual returned `{ relay, id }`. put the complete assignment in `task`; do not also give the worker a startup prompt. the timeout is a failure deadline, not a polling interval. allow at least an hour for coding work.

### 4. receive and verify the result

continue useful work after sending. when none remains, yield the turn: structured follow-ups bring questions and results back to the parent. do not repeatedly poll just to wait.

| tool | purpose |
| --- | --- |
| `agent_task_send` | assign work to an endpoint |
| `agent_task_message` | send a question, answer, or progress update |
| `agent_task_done` | worker submits its final status and result |
| `agent_task_ack` | parent acknowledges a terminal task after verification |
| `agent_task_status` / `agent_task_inbox` | inspect locally known task state |
| `agent_task_cancel` | parent cancels its task |
| `agent_task_wait` | explicitly block for a terminal result when requested |

workers report changed source paths in `result.changedFiles`. artifact declarations point to files for the parent to inspect; they do **not** transfer file contents.

when finished with a session you created, run `wolfpack kill <sessionId> --json`, then verify that exact ID is absent from `wolfpack list --json`.

### across machines

select a configured tailnet peer with `wolfpack --machine <peer>`. remote child spawning requires a parent on that machine; for a worker coordinated from here, use remote top-level creation:

```bash
wolfpack --machine <peer> session create --project-dir /absolute/remote/worktree \
  --harness pi --task-worker --readiness-timeout-ms 30000 --json
```

paths are on the remote host. the CLI qualifies the remote endpoint through your local coordinator and returns a locally routable `taskEndpoint`. use that returned address in the same send flow. if it reports `taskEndpointError`, inspect the retained `sessionId` rather than blindly creating another session. use the same `--machine` selector for cleanup.

see the [delegation workflow](skills/wolfpack-task-delegation/SKILL.md) for role reuse, worker restrictions, readiness errors, and complete lifecycle rules.

### claude code sessions

the same tools are available in claude code through a plugin in this repository. the plugin runs a small MCP server (`claude-plugin/dist/server.mjs`, run by node) over the same host-neutral runtime as the pi extension. tool names, parameters, validation messages, and the worker gate are the same.

```bash
claude plugin marketplace add /absolute/path/to/wolfpack-tasks
claude plugin install wolfpack-tasks@wolfpack-tasks
```

- **identity gate.** the server activates only when both `WOLFPACK_SESSION_ID` and `WOLFPACK_SESSION_NAME` are set, as wolfpack does for its sessions. otherwise it registers nothing, contacts nothing, writes no files or output, and every task tool returns "not a wolfpack session". with identity present, an invalid `WOLFPACK_PORT`, `WOLFPACK_JWT_SECRET`, `WOLFPACK_DIR`, or `HOME` makes every tool return "misconfigured" with the variable name.
- **wake.** new task mail sends one `notifications/claude/channel` pointer (`wolfpack task mail: call agent_task_inbox`, with string `task_id` meta). the pointer never contains task content. the model calls `agent_task_inbox`, and the inbox result contains the messages. further mail does not send another pointer until that call.
- **fallback.** if the pointer stays unanswered for 15s while the session is idle, the server checks `wolfpack session status <id> --json` and types the pointer into the session once (`wolfpack session send`, then a separate submit). this is best effort: the text is appended to any unsubmitted prompt input. after one fallback the server uses fallback mode for the rest of the process lifetime. it never types into a working session. idle state comes from the plugin's `UserPromptSubmit`, `PreToolUse`, and `Stop` hooks.
- **status.** each session writes `<wolfpack state dir>/wolfpack-tasks/claude/<WOLFPACK_SESSION_ID>.json` (mode 0600; the state dir is `WOLFPACK_DIR` or `~/.wolfpack`). it records the wake mode, the negotiated MCP protocol version, and pointer, inbox, and nudge timestamps. it is diagnostic only and never task state.
- **memory only.** as with pi, restarting the claude session loses its active tasks.

the channel is a claude code research-preview surface. claude code must allow this plugin's channel (wolfpack provides a managed-settings drop-in); without it, the fallback is the wake path.

## use without wolfpack

**the task core is transport-independent; the default pi extension is not.** you can import the core into your own application and supply a `TaskRelay`. there is no bundled alternative production network relay or configuration switch that makes the default extension wolfpack-free.

### try the core in one process

with the package available to your bun project, save this as `task-demo.ts` and run `bun task-demo.ts`. it uses the exported in-memory relay fixture, so it needs neither wolfpack nor a model:

```ts
import {
  createInMemoryTaskRelay,
  createTaskCore,
  createTaskStore,
} from "@sgtbeatdown/wolfpack-tasks";

const relay = createInMemoryTaskRelay("demo");
const parentStore = createTaskStore();
const workerStore = createTaskStore();
const parent = createTaskCore({
  endpoint: { relay: relay.id, id: "parent" },
  relay,
  store: parentStore,
});
const worker = createTaskCore({
  endpoint: { relay: relay.id, id: "worker" },
  relay,
  store: workerStore,
});

try {
  await parent.connect();
  await worker.connect();
  const { taskId } = await parent.createTask({
    target: worker.endpoint,
    task: "summarize the change",
    timeoutMs: 60_000,
  });

  // a real application would consume the assignment and do the work here.
  for (const item of await worker.receive()) {
    if (item.kind === "relay") await worker.acknowledgeRelayDelivery(item.delivery.cursor);
  }
  await worker.submitIntent({
    taskId,
    type: "task.completed",
    payload: { summary: "the change adds task coordination" },
  });
  // relay items carry peer mail; local items are the parent's own view of events it canonicalized.
  for (const item of await parent.receive()) {
    if (item.kind === "relay") await parent.acknowledgeRelayDelivery(item.delivery.cursor);
    else parent.completeLocalPresentation(item.event.eventId);
  }

  console.log(parent.getTask(taskId)?.status); // completed
  await parent.acknowledgeParent(taskId);
} finally {
  parentStore.close();
  workerStore.close();
}
```

this demonstrates task coordination, not agent execution. both endpoints share one in-process fixture; it does not connect separate processes or machines.

### integrate your own transport

implement the exported [`TaskRelay` interface](src/task-protocol.ts) to connect endpoints, resolve targets, send envelopes, receive mail, and acknowledge individual deliveries. provide a separate `createTaskStore()` and `createTaskCore()` for each endpoint. `runTaskRelayConformance` is available to check a relay implementation.

`receive()` returns items in order: `relay` items wrap a delivery you acknowledge with `acknowledgeRelayDelivery`, and `local` items carry an event the origin itself canonicalized, which you complete with `completeLocalPresentation` once presented.

`createTaskCore({ timers })` accepts a `TaskTimers` clock for the delivery budget (deadline, retry slices, cooldowns), which lets tests run those bounds on virtual time; task records still use `clock`. an origin can move an active task off a target whose loss the relay reported with `reassignTask`; the [reference](docs/protocol-reference.md#memory-owned-lifecycle) covers its preconditions and why the default extension does not call it yet.

your application owns polling, retries, timeout evaluation, delivery acknowledgments, and shutdown. it also owns how assignments reach an agent and how that agent runs them. the core alone does not install pi tools or wake a model.

## important limits

- **active state lives in ram.** a pi restart loses that endpoint's tasks; a relay restart can lose even accepted mail. pi session history remains readable, but does not restore tasks or authorize workers after restart.
- **accepted is not executed.** relay acceptance, receiver receipt, pi follow-up insertion, model execution, and task completion are separate stages.
- **the parent owns task status.** a worker submits a result; the origin processes it before its task becomes terminal. status reads are local snapshots.
- **use a trusted network.** the wolfpack integration assumes trusted tailnet machines. do not expose owner APIs to the public internet or an untrusted proxy.

## reference and development

- [protocol and operations reference](docs/protocol-reference.md): memory lifecycle, restart loss, authentication, worker gate, delivery evidence, and acknowledgment details.
- [wolfpack relay control API](https://github.com/almogdepaz/wolfpack/blob/main/docs/control-api-schema.md#pi-tasks-relay-v2-boundary): transport contract.

```bash
bun install
bun test
bun run typecheck
bun run build:claude   # rebuild the committed claude plugin bundle and skill copy; a test fails when they are stale
```

optional real-worker checks and their prerequisites are documented in the [reference](docs/protocol-reference.md#development-and-verification).
