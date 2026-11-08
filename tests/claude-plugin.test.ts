import { describe, expect, test } from "bun:test";
import { mkdirSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { buildClaudeBundle, buildWorkerGateBundle, PLUGIN_ROOT, SKILL_COPY, SKILL_SOURCE } from "../scripts/build-claude";
import { createClaudeTaskServer, SERVER_VERSION } from "../src/claude/mcp-server";
import { LIFECYCLE_HOOK_EVENTS } from "../src/claude/adapter";
import { CLAUDE_PLUGIN } from "../src/claude/constants";
import type { JsonRpcMessage } from "../src/claude/json-rpc";
import packageJson from "../package.json";

const readJson = (path: string): unknown => JSON.parse(readFileSync(path, "utf8"));
const bundlePath = join(PLUGIN_ROOT, CLAUDE_PLUGIN.bundle);
const gateBundlePath = join(PLUGIN_ROOT, CLAUDE_PLUGIN.gateBundle);

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

	test("every MCP hook calls the lifecycle tool with known events and schema fields", () => {
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
					if (hook.type === "command") continue;
					expect(hook).toMatchObject({ type: "mcp_tool", server: CLAUDE_PLUGIN.hookServer, tool: CLAUDE_PLUGIN.lifecycleTool });
					const input = hook.input as Record<string, string>;
					for (const key of Object.keys(input)) expect(fields).toContain(key);
					expect(LIFECYCLE_HOOK_EVENTS).toContain(input.event as never);
					events.set(hookEvent, input.event!);
				}
			}
		}
		expect(Object.fromEntries(events)).toEqual({ UserPromptSubmit: "prompt", PreToolUse: "pre_tool", PostToolBatch: "tool_batch", Stop: "stop", StopFailure: "stop_failure" });
		expect(hooks.PreToolUse![0]).toMatchObject({ matcher: "*", hooks: [{ input: { tool_name: "${tool_name}" } }, {}] });
	});

	test("PreToolUse also runs the fail-closed worker gate as a blocking exec-form command hook", () => {
		const hooks = (readJson(join(PLUGIN_ROOT, "hooks", "hooks.json")) as { hooks: Record<string, { matcher?: string; hooks: Record<string, unknown>[] }[]> }).hooks;
		const commands = Object.entries(hooks).flatMap(([event, groups]) => groups.flatMap((group) => group.hooks.filter((hook) => hook.type === "command").map((hook) => ({ event, matcher: group.matcher, hook }))));
		expect(commands).toEqual([{ event: "PreToolUse", matcher: "*", hook: { type: "command", command: "node", args: [`\${CLAUDE_PLUGIN_ROOT}/${CLAUDE_PLUGIN.gateBundle}`], onFailure: "block", timeout: 10 } }]);
	});
});

describe("committed plugin artifacts", () => {
	test("the bundle matches a fresh build (run bun run build:claude)", async () => {
		expect(readFileSync(bundlePath, "utf8") === await buildClaudeBundle()).toBe(true);
	});

	test("the worker gate bundle matches a fresh build (run bun run build:claude)", async () => {
		expect(readFileSync(gateBundlePath, "utf8") === await buildWorkerGateBundle()).toBe(true);
	});

	test("the bundled skill is a byte copy of the single skill source (run bun run build:claude)", () => {
		expect(readFileSync(SKILL_COPY).equals(readFileSync(SKILL_SOURCE))).toBe(true);
	});

	test.each([bundlePath, gateBundlePath])("%s contains no pi packages and no local paths", (path) => {
		const bundle = readFileSync(path, "utf8");
		expect(bundle).not.toMatch(/@(mariozechner|earendil-works)\/pi-/);
		expect(bundle).not.toContain("node_modules/");
		expect(bundle).not.toContain(process.cwd());
	});

	test("the worker gate bundle stays small and does not bundle the task runtime", () => {
		const bundle = readFileSync(gateBundlePath, "utf8");
		expect(bundle.length).toBeLessThan(20_000);
		expect(bundle).not.toContain("createAgentTaskRuntime");
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

describe("worker gate bundle under node", () => {
	const baseEnv = (): Record<string, string> => {
		const env: Record<string, string> = {};
		for (const [key, value] of Object.entries(process.env)) if (value !== undefined && !key.startsWith("WOLFPACK_") && key !== "PI_TASK_WORKER") env[key] = value;
		return env;
	};
	const run = async (env: Record<string, string>, stdin: string) => {
		const child = Bun.spawn([node!, gateBundlePath], { stdin: "pipe", stdout: "pipe", stderr: "pipe", env });
		child.stdin.write(stdin);
		await child.stdin.end();
		const [stdout, stderr, exitCode] = await Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text(), child.exited]);
		return { stdout, stderr, exitCode };
	};
	const sessionId = "0b6f3a52-6a39-4c55-9a0e-1c2d3e4f5a6b";
	const workerEnv = (stateDir: string) => ({ ...baseEnv(), WOLFPACK_SESSION_ID: sessionId, WOLFPACK_SESSION_NAME: "claude-worker", WOLFPACK_DIR: stateDir, WOLFPACK_TASK_WORKER: "1" });
	const bash = JSON.stringify({ tool_name: "Bash" });

	test.skipIf(!node)("an ordinary session exits 0 with no output, even for unreadable input", async () => {
		expect(await run(baseEnv(), "not json")).toEqual({ stdout: "", stderr: "", exitCode: 0 });
	}, 15_000);

	test.skipIf(!node)("a worker session without gate state exits 2 with the denial code", async () => {
		const result = await run(workerEnv(mkdtempSync(join(tmpdir(), "wolfpack-gate-"))), bash);
		expect(result.exitCode).toBe(2);
		expect(result.stdout).toBe("");
		expect(result.stderr).toStartWith("PI_TASK_WORKER_ASSIGNMENT_REQUIRED");
	}, 15_000);

	test.skipIf(!node)("a worker session with live authorized state exits 0; own tools need no state", async () => {
		const stateDir = mkdtempSync(join(tmpdir(), "wolfpack-gate-"));
		const directory = join(stateDir, CLAUDE_PLUGIN.statusDirectory);
		mkdirSync(directory, { recursive: true });
		const now = Date.now();
		writeFileSync(join(directory, `${sessionId}${CLAUDE_PLUGIN.gateFileSuffix}`), JSON.stringify({ version: 1, sessionId, pid: process.pid, parentPid: process.ppid, authorized: true, reason: null, evaluatedAt: now, expiresAt: now + 60_000 }));
		expect(await run(workerEnv(stateDir), bash)).toEqual({ stdout: "", stderr: "", exitCode: 0 });
		expect(await run(workerEnv(mkdtempSync(join(tmpdir(), "wolfpack-gate-"))), JSON.stringify({ tool_name: `${CLAUDE_PLUGIN.toolPrefix}agent_task_inbox` }))).toEqual({ stdout: "", stderr: "", exitCode: 0 });
	}, 15_000);
});
