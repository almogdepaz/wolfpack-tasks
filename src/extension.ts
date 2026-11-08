import type { AgentToolResult, ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { Text } from "@earendil-works/pi-tui";

import { createAgentTaskRuntime, defaultCoreFactory } from "./agent-task-runtime";
import type { AgentTaskToolResult, ConfiguredCoreFactory } from "./agent-task-runtime";
import type { TaskCore } from "./task-core";

export {
	ACTIVE_POLL_MS,
	IDLE_POLL_MS,
	WORKER_COORDINATION_FORBIDDEN_CODE,
	WORKER_GATE_DENIAL_CODE,
	createConfiguredCoreLoader,
	createSingleFlightInboxRefresh,
	outboxFailureStatus,
} from "./agent-task-runtime";
export type { ConfiguredCoreFactory, ConfiguredCoreLoader } from "./agent-task-runtime";

/** Registers endpoint-owned tools using the memory-owned Wolfpack transport by default. */
export function registerAgentTaskTools(pi: ExtensionAPI, core: TaskCore | undefined = undefined, createCore: ConfiguredCoreFactory = defaultCoreFactory): void {
	// Delegate at call time: hosts and tests may replace these methods after registration.
	const { lifecycle, tools } = createAgentTaskRuntime({
		sendMessage(message, options) { pi.sendMessage(message, options); },
		appendEntry(customType, data) { pi.appendEntry(customType, data); },
	}, core, createCore);
	pi.on("tool_call", lifecycle.tool_call);
	pi.on("session_start", lifecycle.session_start);
	pi.on("agent_end", lifecycle.agent_end);
	pi.on("agent_settled", lifecycle.agent_settled);
	pi.on("turn_end", lifecycle.turn_end);
	pi.on("session_shutdown", lifecycle.session_shutdown);
	for (const tool of tools) {
		pi.registerTool({
			name: tool.name,
			label: tool.label,
			description: tool.description,
			parameters: tool.parameters,
			execute: (toolCallId, params, signal, onUpdate, context): Promise<AgentToolResult<unknown>> => tool.execute(toolCallId, params, signal, onUpdate, context),
			renderResult(result, _options, theme) { return new Text(theme.fg("accent", text(result))); },
		});
	}
}

export default function piTasks(pi: ExtensionAPI): void {
	registerAgentTaskTools(pi);
}

function text(result: Pick<AgentTaskToolResult, "content"> | { readonly content?: readonly unknown[] }): string {
	const first = result.content?.[0];
	return first && typeof first === "object" && "type" in first && first.type === "text" && "text" in first && typeof first.text === "string" ? first.text : "";
}
