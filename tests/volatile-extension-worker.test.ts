import { expect, test } from "bun:test";
import { execFileSync } from "node:child_process";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { isAbsolute, join } from "node:path";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import piTasks from "../src/extension";
import { createConfiguredTaskCore, type OwnedTaskCore } from "../src/configured-task-core";

const wolfpack = process.env.PI_TASKS_WOLFPACK_SOURCE, revision = process.env.PI_TASKS_WOLFPACK_REVISION;

test.skipIf(!wolfpack)("normal extension starts, polls, closes, reopens and automatically replaces a reset memory worker lifetime", async () => {
  if (!process.env.PI_TASKS_EXTENSION_FIXTURE_HOME) {
    // Bun may cache homedir(): a HOME change after imports is not isolation.
    const home = mkdtempSync(join(tmpdir(), "tasks-extension-home-"));
    try {
      execFileSync(process.execPath, ["test", import.meta.path], { timeout: 15_000, stdio: "pipe", env: {
        PATH: process.env.PATH, HOME: home, PI_TASKS_EXTENSION_FIXTURE_HOME: home,
        PI_TASKS_WOLFPACK_SOURCE: wolfpack, PI_TASKS_WOLFPACK_REVISION: revision,
        PI_TELEMETRY: "0", WOLFPACK_PORT: "1",
      } });
    } finally { rmSync(home, { recursive: true, force: true }); }
    return;
  }
  expect(process.env.HOME).toBe(process.env.PI_TASKS_EXTENSION_FIXTURE_HOME);
  expect(isAbsolute(wolfpack!)).toBe(true); expect(revision).toMatch(/^[0-9a-f]{40}$/);
  expect(execFileSync("git", ["rev-parse", "HEAD"], { cwd: wolfpack, encoding: "utf8" }).trim()).toBe(revision!);
  expect(execFileSync("git", ["status", "--porcelain", "--untracked-files=no"], { cwd: wolfpack, encoding: "utf8" }).trim()).toBe("");
  const { WorkerRelayGateway } = await import(join(wolfpack!, "src/task-relay/worker-client.ts"));
  const root = mkdtempSync(join(tmpdir(), "tasks-extension-worker-"));
  const previous = { HOME: process.env.HOME, WOLFPACK_SESSION_NAME: process.env.WOLFPACK_SESSION_NAME, WOLFPACK_PORT: process.env.WOLFPACK_PORT, PI_TASK_WORKER: process.env.PI_TASK_WORKER };
  const makeWorker = () => new WorkerRelayGateway({ profile: "volatile-v1", root: join(root, "relay"),
    inspectSession: async (selector: string) => ({ ok: true, session: selector, sessionId: selector, projectPath: root, harness: "pi", alive: true }) });
  let worker = makeWorker();
  let peerCore: OwnedTaskCore | undefined;
  const frames: any[] = [], registrations: any[] = [];
  const server = Bun.serve({ hostname: "127.0.0.1", port: 0, async fetch(request) {
    expect(new URL(request.url).pathname).toBe("/api/task-relay/volatile-v1");
    const body = await request.json() as Record<string, unknown>; frames.push(body);
    const result = await worker.volatile(body);
    if (body.operation === "connect" && result.ok) registrations.push({ caller: body.callerSession, epoch: result.epoch, endpoint: result.value.endpoint });
    return Response.json(result, { status: result.ok ? 200 : 409 });
  } });
  const events: Record<string, (event: any, context: any) => any> = {}, tools: Record<string, any> = {}, commands: Record<string, any> = {};
  const statuses: any[] = [], notifications: string[] = [], messages: any[] = [], entries: any[] = [];
  const context = { isIdle: () => true, hasPendingMessages: () => false, sessionManager: { getEntries: () => entries }, ui: {
    setStatus: (_key: string, value: unknown) => statuses.push(value), theme: { fg: (_color: string, text: string) => text }, notify: (text: string) => notifications.push(text),
  } };
  const api = { on: (event: string, handler: any) => { events[event] = handler; }, registerTool: (tool: any) => { tools[tool.name] = tool; },
    registerCommand: (name: string, command: any) => { commands[name] = command; },
    sendMessage: (message: any) => { messages.push(message); entries.push({ type: "custom_message", ...message }); },
    appendEntry: (customType: string, data: any) => { entries.push({ type: "custom", customType, data }); },
  } as unknown as ExtensionAPI;
  try {
    await worker.initialize();
    process.env.WOLFPACK_PORT = String(server.port); process.env.WOLFPACK_SESSION_NAME = "fixture-extension"; delete process.env.PI_TASK_WORKER;
    piTasks(api);
    await events.session_start!({}, context);
    const prior = registrations.at(-1)!;
    expect(prior.caller).toBe("fixture-extension"); expect(statuses.at(-1)).toBeUndefined();
    expect(existsSync(join(process.env.HOME!, ".pi", "tasks"))).toBe(false);
    const result = await tools.agent_task_send.execute("test", { to: prior.endpoint, task: "historical self task", timeoutMs: 60_000 }, undefined);
    expect(result.details.taskId).toBeString();
    await events.agent_settled!({}, context);
    expect(messages.length).toBeGreaterThan(0);
    expect(frames.some(frame => frame.operation === "acknowledge")).toBe(true);
    // A self-task can reduce intents locally and miss a dropped method receiver.
    // Use a distinct configured endpoint so the parent must process a wire intent.
    peerCore = await createConfiguredTaskCore({ sessionName: "fixture-peer", baseUrl: server.url.origin });
    const remote = await tools.agent_task_send.execute("peer-task", { to: peerCore.endpoint, task: "exercise wire intent ACK through the owned core receiver", timeoutMs: 60_000 }, undefined);
    expect(remote.isError).not.toBe(true);
    await peerCore.receive();
    await peerCore.submitIntent({ taskId: remote.details.taskId, type: "task.completed", payload: { summary: "completed by distinct endpoint" } });
    await events.agent_settled!({}, context);
    expect(statuses.at(-1)).toBeUndefined();
    await peerCore.receive();
    expect(peerCore.getTask(remote.details.taskId)?.status).toBe("completed");
    await peerCore.close();
    await events.session_shutdown!({}, context);
    const calls = frames.length;
    await events.agent_end!({}, context); await events.agent_settled!({}, context);
    expect(frames).toHaveLength(calls);
    await events.session_start!({}, context);
    const resetEndpoint = registrations.at(-1).endpoint;
    expect(resetEndpoint).not.toEqual(prior.endpoint);
    expect(messages.some(message => message.details?.taskId === result.details.taskId)).toBe(true);
    const lost = await tools.agent_task_send.execute("lost-task", { to: resetEndpoint, task: "active when relay resets", timeoutMs: 60_000 }, undefined);
    expect(lost.isError).not.toBe(true);
    await worker.close(); worker = makeWorker(); await worker.initialize();
    await events.agent_settled!({}, context);
    expect(commands["task-relay-rebind"]).toBeUndefined();
    expect(registrations.at(-1).epoch).not.toBe(prior.epoch);
    expect(registrations.at(-1).endpoint).not.toEqual(resetEndpoint);
    expect(messages.some(message => message.customType === "pi-tasks-relay-loss" && message.details?.taskId === lost.details.taskId)).toBe(true);
    expect(notifications.at(-1)).toContain("automatically bound a fresh endpoint");
    expect(statuses.at(-1)).toBeUndefined();
    expect(existsSync(join(process.env.HOME!, ".pi", "tasks"))).toBe(false);
    const historical = await tools.agent_task_message.execute("old", { taskId: result.details.taskId, type: "information", message: "must not adopt" }, undefined);
    expect(historical.details.error.code).toBe("UNKNOWN_TASK");
    expect(statuses.at(-1)).toBeUndefined();
  } finally {
    await events.session_shutdown?.({}, context);
    await peerCore?.close();
    await server.stop(true); await worker.close();
    for (const [key, value] of Object.entries(previous)) { if (value === undefined) delete process.env[key]; else process.env[key] = value; }
    rmSync(root, { recursive: true, force: true });
  }
}, 20_000);
