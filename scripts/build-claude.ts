#!/usr/bin/env bun
import { copyFileSync, mkdirSync } from "node:fs";
import { dirname, join } from "node:path";

import { CLAUDE_PLUGIN } from "../src/claude/constants";

const repositoryRoot = join(import.meta.dirname, "..");
export const PLUGIN_ROOT = join(repositoryRoot, "claude-plugin");
export const SKILL_SOURCE = join(repositoryRoot, "skills", CLAUDE_PLUGIN.skill, "SKILL.md");
export const SKILL_COPY = join(PLUGIN_ROOT, "skills", CLAUDE_PLUGIN.skill, "SKILL.md");

/**
 * Builds the self-contained Node MCP server for the Claude plugin. TypeBox is compiled in, so an installed plugin needs
 * only Node. Whitespace minification drops dependency path comments, keeping the bytes independent of the local
 * node_modules layout; identifiers and syntax stay readable for review.
 */
export function buildClaudeBundle(): Promise<string> {
	return bundle("server.ts");
}

/** Builds the fail-closed PreToolUse worker gate script, a separate small bundle without the task runtime. */
export function buildWorkerGateBundle(): Promise<string> {
	return bundle("worker-gate-hook.ts");
}

async function bundle(entry: string): Promise<string> {
	const result = await Bun.build({
		root: repositoryRoot,
		entrypoints: [join(repositoryRoot, "src", "claude", entry)],
		format: "esm",
		target: "node",
		minify: { whitespace: true, identifiers: false, syntax: false },
		sourcemap: "none",
	});
	const [output] = result.outputs;
	if (!result.success || !output) throw new Error(`claude bundle ${entry} failed: ${result.logs.map((log) => log.message).join("; ")}`);
	return output.text();
}

if (import.meta.main) {
	const bundlePath = join(PLUGIN_ROOT, CLAUDE_PLUGIN.bundle);
	mkdirSync(dirname(bundlePath), { recursive: true });
	await Bun.write(bundlePath, await buildClaudeBundle());
	await Bun.write(join(PLUGIN_ROOT, CLAUDE_PLUGIN.gateBundle), await buildWorkerGateBundle());
	mkdirSync(dirname(SKILL_COPY), { recursive: true });
	copyFileSync(SKILL_SOURCE, SKILL_COPY);
}
