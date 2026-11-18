import { afterEach, expect, test } from "bun:test";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

import { STATUS_MESSAGE_PAGE_MAX_UNITS } from "../src/agent-task-tool-schemas";
import { createInMemoryTaskRelay } from "../src/in-memory-task-relay";
import { registerAgentTaskTools } from "../src/extension";
import { createTaskCore } from "../src/task-core";
import type { TaskCore } from "../src/task-core";
import { createTaskStore } from "../src/task-store";
import { sequence, startHarness, wolfpackEnv } from "./claude-harness";
import type { Harness } from "./claude-harness";

type Result = { readonly content: readonly { readonly text: string }[]; readonly details: Record<string, any> };
interface Tool { readonly name: string; execute(id: string, params: Record<string, unknown>, signal: AbortSignal, update: undefined, context: unknown): Promise<Result> }
const noCommands = async (): Promise<never> => { throw new Error("unexpected command"); };
const harnesses: Harness[] = [];
afterEach(async () => { for (const harness of harnesses.splice(0)) await harness.server.shutdown(); });

/** An origin with one active task whose worker sent `messages` in order (all `information` unless prefixed `?`). */
async function fixture(messages: readonly string[]) {
	const relay = createInMemoryTaskRelay("memory");
	const origin = createTaskCore({ endpoint: { relay: "memory", id: "parent" }, relay, store: createTaskStore(), ids: sequence("parent") });
	const worker = createTaskCore({ endpoint: { relay: "memory", id: "worker" }, relay, store: createTaskStore(), ids: sequence("worker") });
	await origin.connect(); await worker.connect();
	const { taskId } = await origin.createTask({ target: worker.endpoint, task: "assigned work", timeoutMs: 60_000 });
	await worker.receive();
	for (const message of messages) {
		const question = message.startsWith("?");
		await worker.submitIntent({ taskId, type: question ? "task.question" : "task.information", payload: { message: question ? message.slice(1) : message } });
	}
	await origin.receive();
	return { origin, worker, taskId };
}

function piTools(core: TaskCore): Record<string, Tool> {
	const tools: Record<string, Tool> = {};
	registerAgentTaskTools({ on: () => undefined, registerTool(tool: unknown) { const value = tool as Tool; tools[value.name] = value; } } as unknown as ExtensionAPI, core);
	return tools;
}
const text = (result: { readonly content: readonly { readonly text?: string }[] }) => result.content.map(part => part.text).join("\n");
const ids = (page: { items: { eventId: string }[] }) => page.items.map(item => item.eventId);

test("pi: default status omits message history from text and details but keeps lifecycle and delivery fields", async () => {
	const f = await fixture(["progress one", "progress two", "?which branch?"]);
	const status = await piTools(f.origin).agent_task_status!.execute("call", { taskId: f.taskId }, new AbortController().signal, undefined, {});
	expect(text(status)).not.toContain("progress one");
	expect(text(status)).not.toContain("which branch?");
	expect(text(status)).toContain("- messages: 3");
	expect(status.details).not.toHaveProperty("events");
	expect(status.details).not.toHaveProperty("messages");
	expect(JSON.stringify(status.details)).not.toContain("progress one");
	expect(status.details).toMatchObject({
		taskId: f.taskId, status: "active", task: "assigned work", messageCount: 3,
		origin: f.origin.endpoint, target: f.worker.endpoint,
		terminalDelivery: { state: "not_submitted" },
		deliveryEvidence: { receiverReceipt: expect.any(String), modelExecution: "not_evidenced" },
	});
});

test("pi: status pages complete messages in order by count, continues exclusively, and ends with an empty page", async () => {
	const messages = Array.from({ length: 7 }, (_, index) => `progress ${index}`);
	const f = await fixture([...messages, "?ready for review?"]);
	const status = piTools(f.origin).agent_task_status!;
	const signal = new AbortController().signal;
	const read = (params: Record<string, unknown>) => status.execute("call", { taskId: f.taskId, ...params }, signal, undefined, {});

	const first = await read({ messageLimit: 3 });
	expect(text(first)).toContain("progress 0");
	expect(text(first)).toContain("progress 2");
	expect(text(first)).not.toContain("progress 3");
	expect(first.details.messages).toMatchObject({ remaining: 5 });
	expect(first.details.messages.items).toHaveLength(3);
	// Message text appears once per result: in the model-readable text, not again in structured details.
	expect(JSON.stringify(first.details)).not.toContain("progress 0");
	const after = first.details.messages.nextAfter as string;
	expect(after).toBe(first.details.messages.items[2].eventId);

	const second = await read({ messagesAfter: after, messageLimit: 3 });
	expect(text(second)).not.toContain("progress 2");
	expect(text(second)).toContain("progress 3");
	expect(text(second)).toContain("progress 5");
	expect(second.details.messages).toMatchObject({ after, remaining: 2 });

	// The default page size reaches the question; types are labeled.
	const third = await read({ messagesAfter: second.details.messages.nextAfter });
	expect(text(third)).toContain("progress 6");
	expect(text(third)).toContain("question");
	expect(text(third)).toContain("ready for review?");
	expect(third.details.messages).toMatchObject({ remaining: 0 });
	expect(third.details.messages).not.toHaveProperty("nextAfter");
	expect(new Set([...ids(first.details.messages), ...ids(second.details.messages), ...ids(third.details.messages)]).size).toBe(8);

	const empty = await read({ messagesAfter: ids(third.details.messages).at(-1) });
	expect(empty.details.messages).toMatchObject({ items: [], remaining: 0 });
	expect(text(empty)).toContain("no messages");

	const unknown = await read({ messagesAfter: "not-an-event" });
	expect(unknown.details.error).toMatchObject({ code: "UNKNOWN_MESSAGE_CURSOR", retryable: false });
	expect(unknown.details).not.toHaveProperty("messages");
});

test("pi: a status page stops before its size bound without truncating a message", async () => {
	const large = Math.floor(STATUS_MESSAGE_PAGE_MAX_UNITS / 3.5);
	const f = await fixture(["a".repeat(large), "b".repeat(large), "c".repeat(large), "d".repeat(large), "e".repeat(16 * 1024), "f".repeat(16 * 1024)]);
	const status = piTools(f.origin).agent_task_status!;
	const signal = new AbortController().signal;
	const first = await status.execute("call", { taskId: f.taskId, messageLimit: 20 }, signal, undefined, {});
	expect(first.details.messages.items).toHaveLength(3);
	expect(first.details.messages.remaining).toBe(3);
	expect(text(first)).toContain("c".repeat(large));
	expect(text(first)).not.toContain("dddd");
	expect(text(first).length).toBeLessThan(STATUS_MESSAGE_PAGE_MAX_UNITS + 2_000);
	const second = await status.execute("call", { taskId: f.taskId, messagesAfter: first.details.messages.nextAfter, messageLimit: 20 }, signal, undefined, {});
	expect(second.details.messages.items).toHaveLength(2);
	expect(text(second)).toContain(`${"e".repeat(16 * 1024)}\n`);
	expect(text(second)).not.toContain("ffff");
	const third = await status.execute("call", { taskId: f.taskId, messagesAfter: second.details.messages.nextAfter, messageLimit: 20 }, signal, undefined, {});
	expect(third.details.messages).toMatchObject({ remaining: 0 });
	expect(text(third)).toContain("f".repeat(16 * 1024));
});

// The 48 KiB protocol payload limit (not the 16 KiB tool schema) admits one message above the page target.
test("pi: a single message above the page size target is returned whole on its own page, then paging continues", async () => {
	const large = "x".repeat(40_000);
	const f = await fixture([large, "next complete message"]);
	const status = piTools(f.origin).agent_task_status!;
	const signal = new AbortController().signal;
	const first = await status.execute("call", { taskId: f.taskId, messageLimit: 20 }, signal, undefined, {});
	expect(first.details.error).toBeUndefined();
	expect(first.details.messages.items).toHaveLength(1);
	expect(first.details.messages.remaining).toBe(1);
	expect(text(first)).toContain(large);
	expect(text(first).length).toBeGreaterThan(STATUS_MESSAGE_PAGE_MAX_UNITS);
	const second = await status.execute("call", { taskId: f.taskId, messagesAfter: first.details.messages.nextAfter, messageLimit: 20 }, signal, undefined, {});
	expect(second.details.messages).toMatchObject({ remaining: 0 });
	expect(second.details.messages.items).toHaveLength(1);
	expect(text(second)).toContain("next complete message");
	expect(text(second)).not.toContain("xxxx");
});

test("pi: message paging belongs to the task origin", async () => {
	const f = await fixture(["progress"]);
	const status = await piTools(f.worker).agent_task_status!.execute("call", { taskId: f.taskId, messageLimit: 5 }, new AbortController().signal, undefined, { sessionManager: { getEntries: () => [] } });
	expect(status.details.error).toMatchObject({ code: "TASK_MESSAGES_ORIGIN_ONLY", retryable: false });
});

test("claude: status structured content carries no history and paging text is not duplicated in structured content", async () => {
	const f = await fixture(["claude progress one", "claude progress two"]);
	const harness = startHarness({ env: wolfpackEnv(), core: f.origin, runCommand: noCommands });
	harnesses.push(harness);
	await harness.initialize("2025-11-25");
	await harness.notify("notifications/initialized");

	const plain = await harness.callTool("agent_task_status", { taskId: f.taskId });
	expect(plain.isError).toBeUndefined();
	expect(JSON.stringify(plain)).not.toContain("claude progress");
	expect(plain.structuredContent).not.toHaveProperty("events");
	expect(plain.structuredContent).toMatchObject({ messageCount: 2, deliveryEvidence: expect.any(Object), terminalDelivery: expect.any(Object) });

	const page = await harness.callTool("agent_task_status", { taskId: f.taskId, messageLimit: 1 });
	expect(text(page)).toContain("claude progress one");
	expect(text(page)).not.toContain("claude progress two");
	expect(JSON.stringify(page.structuredContent)).not.toContain("claude progress");
	const next = await harness.callTool("agent_task_status", { taskId: f.taskId, messagesAfter: (page.structuredContent as any).messages.nextAfter });
	expect(text(next)).toContain("claude progress two");
	expect((next.structuredContent as any).messages).toMatchObject({ remaining: 0 });

	const invalid = await harness.callTool("agent_task_status", { taskId: f.taskId, messageLimit: 21 });
	expect(invalid.isError).toBe(true);
});
