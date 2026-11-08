import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";

import { buildClaudeBundle, PLUGIN_ROOT, SKILL_COPY, SKILL_SOURCE } from "../scripts/build-claude";
import { createClaudeTaskServer, SERVER_VERSION } from "../src/claude/mcp-server";
import { LIFECYCLE_HOOK_EVENTS } from "../src/claude/adapter";
import { CLAUDE_PLUGIN } from "../src/claude/constants";
import type { JsonRpcMessage } from "../src/claude/json-rpc";
import packageJson from "../package.json";

const readJson = (path: string): unknown => JSON.parse(readFileSync(path, "utf8"));
const bundlePath = join(PLUGIN_ROOT, CLAUDE_PLUGIN.bundle);

describe("claude plugin manifests", () => {
	test("the repository marketplace lists the plugin from its subdirectory", () => {
		expect(readJson(".claude-plugin/marketplace.json")).toMatchObject({
			name: CLAUDE_PLUGIN.marketplace,
			plugins: [{ name: CLAUDE_PLUGIN.plugin, source: CLAUDE_PLUGIN.pluginSource }],
		});
		expect(join(".", CLAUDE_PLUGIN.pluginSource)).toBe("claude-plugin");
	});

	test("plugin.json runs the committed bundle with node under the shared version", () => {
		const manifest = readJson(join(PLUGIN_ROOT, ".claude-plugin", "plugin.json")) as Record<string, unknown>;
		expect(manifest).toMatchObject({ name: CLAUDE_PLUGIN.plugin, version: packageJson.version });
		expect(manifest.mcpServers).toEqual({ [CLAUDE_PLUGIN.serverKey]: { command: "node", args: [`\${CLAUDE_PLUGIN_ROOT}/${CLAUDE_PLUGIN.bundle}`] } });
		expect(SERVER_VERSION).toBe(packageJson.version);
	});

	test("derived Claude Code names follow the plugin and server keys", () => {
		expect(CLAUDE_PLUGIN.hookServer).toBe(`plugin:${CLAUDE_PLUGIN.plugin}:${CLAUDE_PLUGIN.serverKey}`);
		expect(CLAUDE_PLUGIN.toolPrefix).toBe(`mcp__plugin_${CLAUDE_PLUGIN.plugin}_${CLAUDE_PLUGIN.serverKey}__`);
	});

	test("every hook calls the lifecycle tool with known events and schema fields", () => {
		const sent: JsonRpcMessage[] = [];
		const server = createClaudeTaskServer({ env: {}, send: (message) => { sent.push(message); } });
		void server.handle({ jsonrpc: "2.0", id: 1, method: "tools/list" });
		const tools = (sent[0] as { result: { tools: { name: string; inputSchema: { properties: Record<string, unknown> } }[] } }).result.tools;
		const lifecycle = tools.find((tool) => tool.name === CLAUDE_PLUGIN.lifecycleTool)!;
		const fields = Object.keys(lifecycle.inputSchema.properties);

		const hooks = (readJson(join(PLUGIN_ROOT, "hooks", "hooks.json")) as { hooks: Record<string, { matcher?: string; hooks: Record<string, unknown>[] }[]> }).hooks;
		expect(Object.keys(hooks).sort()).toEqual(["PostToolBatch", "PreToolUse", "Stop", "StopFailure", "UserPromptSubmit"]);
		const events = new Map<string, string>();
		for (const [hookEvent, groups] of Object.entries(hooks)) {
			for (const group of groups) {
				for (const hook of group.hooks) {
					expect(hook).toMatchObject({ type: "mcp_tool", server: CLAUDE_PLUGIN.hookServer, tool: CLAUDE_PLUGIN.lifecycleTool });
					const input = hook.input as Record<string, string>;
					for (const key of Object.keys(input)) expect(fields).toContain(key);
					expect(LIFECYCLE_HOOK_EVENTS).toContain(input.event as never);
					events.set(hookEvent, input.event!);
				}
			}
		}
		expect(Object.fromEntries(events)).toEqual({ UserPromptSubmit: "prompt", PreToolUse: "pre_tool", PostToolBatch: "tool_batch", Stop: "stop", StopFailure: "stop_failure" });
		expect(hooks.PreToolUse![0]).toMatchObject({ matcher: "*", hooks: [{ input: { tool_name: "${tool_name}" } }] });
	});
});

describe("committed plugin artifacts", () => {
	test("the bundle matches a fresh build (run bun run build:claude)", async () => {
		expect(readFileSync(bundlePath, "utf8") === await buildClaudeBundle()).toBe(true);
	});

	test("the bundled skill is a byte copy of the single skill source (run bun run build:claude)", () => {
		expect(readFileSync(SKILL_COPY).equals(readFileSync(SKILL_SOURCE))).toBe(true);
	});

	test("the bundle contains no pi packages and no local paths", () => {
		const bundle = readFileSync(bundlePath, "utf8");
		expect(bundle).not.toMatch(/@(mariozechner|earendil-works)\/pi-/);
		expect(bundle).not.toContain("node_modules/");
		expect(bundle).not.toContain(process.cwd());
	});
});

const node = Bun.which("node");

describe("bundle under node", () => {
	test.skipIf(!node)("without wolfpack identity it answers MCP, refuses tools, and writes nothing to stderr", async () => {
		const env: Record<string, string> = {};
		for (const [key, value] of Object.entries(process.env)) if (value !== undefined && !key.startsWith("WOLFPACK_")) env[key] = value;
		const child = Bun.spawn([node!, bundlePath], { stdin: "pipe", stdout: "pipe", stderr: "pipe", env });
		const requests = [
			{ jsonrpc: "2.0", id: 1, method: "initialize", params: { protocolVersion: "2025-11-25", capabilities: {}, clientInfo: { name: "claude-code", version: "test" } } },
			{ jsonrpc: "2.0", method: "notifications/initialized" },
			{ jsonrpc: "2.0", id: 2, method: "tools/list" },
			{ jsonrpc: "2.0", id: 3, method: "tools/call", params: { name: "agent_task_inbox", arguments: {}, _meta: { [CLAUDE_PLUGIN.toolUseMeta]: "toolu_1" } } },
		];
		child.stdin.write(requests.map((request) => `${JSON.stringify(request)}\n`).join(""));
		await child.stdin.end();
		const [stdout, stderr, exitCode] = await Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text(), child.exited]);
		const responses = stdout.trim().split("\n").map((line) => JSON.parse(line) as Record<string, unknown>);
		expect(responses.map((response) => response.id)).toEqual([1, 2, 3]);
		expect(responses[0]).toMatchObject({ result: { protocolVersion: "2025-11-25", capabilities: { tools: {} } } });
		expect((responses[0] as { result: { capabilities: Record<string, unknown> } }).result.capabilities.experimental).toBeUndefined();
		expect((responses[1] as { result: { tools: { name: string }[] } }).result.tools.map((tool) => tool.name)).toContain("agent_task_inbox");
		expect(responses[2]).toMatchObject({ result: { isError: true, content: [{ type: "text", text: expect.stringContaining("not a wolfpack session") }] } });
		expect(stderr).toBe("");
		expect(exitCode).toBe(0);
	}, 15_000);
});
