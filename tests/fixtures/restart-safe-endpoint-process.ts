import { createConfiguredTaskCore } from "../../src/configured-task-core";
const [phase, baseUrl, root, sessionId] = process.argv.slice(2);
if (!phase || !baseUrl || !root || !sessionId || process.env.HOME !== root || process.env.WOLFPACK_DIR !== `${root}/state`) throw new Error("private endpoint fixture required");
const core = await createConfiguredTaskCore({ baseUrl, sessionId, sessionName: "same-name", stateRoot: `${root}/journal` });
if (phase === "create") {
  await core.createTask({ target: core.endpoint, task: "recorded once before process crash", timeoutMs: 60_000 }).catch(() => undefined);
} else await core.flushOutbox();
process.stdout.write(JSON.stringify({ endpoint: core.endpoint, tasks: core.listTasks() }) + "\n");
// Deliberately skip close: prove the OS releases owner locks and SQLite commits survive process replacement.
process.exit(0);
