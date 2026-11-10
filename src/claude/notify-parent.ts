import { Type } from "typebox";

import { wolfpackLocalHeaders } from "../wolfpack-local-auth";
import type { ClaudeEnvironment } from "./environment";
import { isRecord } from "./json-rpc";
import { validateToolArguments } from "./validation";

/**
 * `agent_notify_parent`: a claude child's report to its parent session. It sends the request `wolfpack agent
 * notify-parent` sends (`POST /api/notify` on the local server, same body, local auth) and only reports: it carries no
 * task authority and does not change task state.
 */
export const NOTIFY_PARENT_TOOL = "agent_notify_parent";
/** The server keeps the first 500 characters; the tool refuses longer messages instead of truncating them silently. */
export const NOTIFY_PARENT_MESSAGE_CODE_POINTS = 500;
const NOTIFY_PATH = "/api/notify";
const NOTIFY_TIMEOUT_MS = 10_000;
const ERROR_BODY_CODE_POINTS = 500;

const parameters = Type.Object({
	message: Type.String({ minLength: 1, description: `Report for the parent session, at most ${NOTIFY_PARENT_MESSAGE_CODE_POINTS} characters.` }),
}, { additionalProperties: false });

export const NOTIFY_PARENT_LISTING = {
	name: NOTIFY_PARENT_TOOL,
	title: "Notify parent session",
	description: "Send a short report to the wolfpack session that spawned this one. It replaces `wolfpack agent notify-parent` for claude sessions. "
		+ "Report only: it does not complete, block or change any task. Task workers report through agent_task_done and agent_task_message instead.",
	inputSchema: JSON.parse(JSON.stringify(parameters)) as unknown,
} as const;

type ReadyEnvironment = Extract<ClaudeEnvironment, { kind: "ready" }>;
export interface NotifyParentResult {
	readonly content: readonly { readonly type: "text"; readonly text: string }[];
	readonly structuredContent?: Record<string, unknown>;
	readonly isError?: true;
}

/** Listed and callable only for a session that has a parent and is not a task worker. */
export function notifyParentAvailable(environment: ReadyEnvironment, worker: boolean): boolean {
	return environment.parentSessionId !== undefined && !worker;
}

export async function notifyParent(args: unknown, options: {
	readonly environment: ReadyEnvironment;
	readonly env: Readonly<Record<string, string | undefined>>;
	readonly fetch: typeof fetch;
	readonly signal: AbortSignal;
}): Promise<NotifyParentResult> {
	let message: string;
	try { ({ message } = validateToolArguments(NOTIFY_PARENT_TOOL, parameters, args ?? {}) as { message: string }); } catch (error) { return failure(error instanceof Error ? error.message : String(error)); }
	if ([...message].length > NOTIFY_PARENT_MESSAGE_CODE_POINTS) return failure(`message must contain at most ${NOTIFY_PARENT_MESSAGE_CODE_POINTS} characters`);
	const { environment } = options;
	const url = `http://127.0.0.1:${environment.port}${NOTIFY_PATH}`;
	// The `wolfpack agent notify-parent` body: the parent target only when both id and name are known.
	const body = {
		message,
		from: environment.sessionName,
		...(environment.parentSessionId !== undefined && environment.parentSessionName !== undefined && { sessionId: environment.parentSessionId, sessionName: environment.parentSessionName }),
	};
	let headers: Headers;
	try { headers = wolfpackLocalHeaders(url, { "content-type": "application/json" }, options.env); } catch (error) { return failure(error instanceof Error ? error.message : String(error)); }
	let response: Response;
	try {
		response = await options.fetch(url, {
			method: "POST",
			headers,
			body: JSON.stringify(body),
			signal: AbortSignal.any([options.signal, AbortSignal.timeout(NOTIFY_TIMEOUT_MS)]),
		});
	} catch (error) {
		return failure(`wolfpack server unreachable at ${url}: ${error instanceof Error ? error.message : String(error)}`);
	}
	const text = await response.text().catch(() => "");
	if (!response.ok) return failure(`wolfpack server refused the notification (HTTP ${response.status}): ${bounded(text)}`);
	let value: unknown;
	try { value = JSON.parse(text) as unknown; } catch { return failure(`wolfpack server returned a non-JSON notification result: ${bounded(text)}`); }
	return { content: [{ type: "text", text: JSON.stringify(value) }], structuredContent: isRecord(value) ? value : { value } };
}

function bounded(text: string): string {
	const codePoints = [...text];
	return codePoints.length <= ERROR_BODY_CODE_POINTS ? text : `${codePoints.slice(0, ERROR_BODY_CODE_POINTS).join("")}…`;
}

function failure(text: string): NotifyParentResult {
	return { content: [{ type: "text", text }], isError: true };
}
