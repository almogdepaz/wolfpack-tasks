import { expect, test } from "bun:test";
import { rmSync } from "node:fs";
import { join } from "node:path";
import { readClaudeEnvironment } from "../src/claude/environment";
import { openTaskJournal } from "../src/task-journal";
import { createTaskStore } from "../src/task-store";
import { createTaskCore } from "../src/task-core";
import { createInMemoryTaskRelay } from "../src/in-memory-task-relay";
import { shortTempDir, startHarness, SESSION_ID } from "./claude-harness";

test("Claude identity refuses a reusable name in place of the authoritative broker UUID", () => {
  expect(readClaudeEnvironment({ HOME: "/tmp/private-fixture", WOLFPACK_DIR: "/tmp/private-fixture", WOLFPACK_SESSION_ID: "same-name", WOLFPACK_SESSION_NAME: "same-name" })).toMatchObject({ kind: "misconfigured", variable: "WOLFPACK_SESSION_ID" });
});

test("MCP-process replacement retains protocol state but fences unproven response consumption without replaying the assignment", async () => {
  const root = shortTempDir("wpt-recover-"), prior = process.env.WOLFPACK_TASK_WORKER;
  process.env.WOLFPACK_TASK_WORKER = "1";
  const identity = { sessionId: SESSION_ID, url: "http://127.0.0.1:1" }, path = join(root, SESSION_ID, "tasks.sqlite");
  const open = () => createTaskStore({}, openTaskJournal(path, identity));
  let store = open();
  const relay = createInMemoryTaskRelay("fixture"), source = { relay: relay.id, id: "source" }, target = { relay: relay.id, id: "target" };
  const originStore = createTaskStore(), origin = createTaskCore({ endpoint: source, relay, store: originStore });
  const env = { HOME: root, WOLFPACK_DIR: root, WOLFPACK_SESSION_ID: SESSION_ID, WOLFPACK_SESSION_NAME: "same-name", WOLFPACK_PORT: "1" };
  const boot = async () => {
    const core = createTaskCore({ endpoint: target, relay, store }); await core.connect();
    const harness = startHarness({ env, core, runCommand: async () => { throw new Error("no terminal commands in recovery fixture"); } });
    await harness.initialize(); await harness.notify("notifications/initialized"); return harness;
  };
  let harness: Awaited<ReturnType<typeof boot>> | undefined;
  try {
    await origin.connect(); harness = await boot();
    const { taskId } = await origin.createTask({ target, task: "never automatically repeat this assignment body", timeoutMs: 60_000 });
    const first = await harness.callTool("agent_task_inbox"); expect(first.isError).toBeUndefined();
    expect(first.content.map(part => part.text).join("\n")).toContain("never automatically repeat this assignment body");
    expect(await harness.hook({ event: "pre_tool", tool_name: "Bash" })).toBe("");
    await harness.server.shutdown(); store.close(); store = open(); harness = await boot();
    expect(store.getTask(taskId)?.status).toBe("active");
    const resumed = await harness.callTool("agent_task_inbox");
    expect(resumed.structuredContent).toMatchObject({ error: { code: "TASK_PRESENTATION_UNCONFIRMED", retryable: false } });
    expect(resumed.content.map(part => part.text).join("\n")).not.toContain("never automatically repeat this assignment body");
    expect(await harness.hook({ event: "pre_tool", tool_name: "Bash" })).toContain("TASK_PRESENTATION_UNCONFIRMED");
  } finally {
    await harness?.server.shutdown(); store.close(); originStore.close(); rmSync(root, { recursive: true, force: true });
    if (prior === undefined) delete process.env.WOLFPACK_TASK_WORKER; else process.env.WOLFPACK_TASK_WORKER = prior;
  }
});
