import { expect, test } from "bun:test";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

import { registerAgentTaskTools } from "../src/extension";
import type { TaskCore } from "../src/task-core";
import contract from "./fixtures/agent-task-tools.json";

/** Pi-visible tool contract: registration order, labels, descriptions and JSON parameter schemas. */
test("pi registers the agent task tools with an unchanged contract", () => {
	const order: string[] = [];
	const tools: Record<string, unknown> = {};
	registerAgentTaskTools({
		on(event: string): void { order.push(`on:${event}`); },
		registerTool(tool: { name: string; label: string; description: string; parameters: unknown }): void {
			order.push(`tool:${tool.name}`);
			tools[tool.name] = { label: tool.label, description: tool.description, parameters: JSON.parse(JSON.stringify(tool.parameters)) };
		},
	} as unknown as ExtensionAPI, {} as TaskCore);
	expect({ order, tools }).toEqual(contract);
});
