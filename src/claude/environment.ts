import { createHash } from "node:crypto";
import { isAbsolute, join } from "node:path";

import { CLAUDE_PLUGIN } from "./constants";

/** Default local Wolfpack control port, the same default the configured task core uses. */
export const DEFAULT_WOLFPACK_PORT = "18790";
const JWT_SECRET_MIN_CHARS = 32;
const SESSION_ID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

export type ClaudeEnvironment =
	/** Not a Wolfpack session: no registration, no relay or CLI contact, no files, no output. */
	| { readonly kind: "inactive" }
	/** Wolfpack identity is present but a required value is missing or invalid. */
	| { readonly kind: "misconfigured"; readonly variable: string; readonly reason: string }
	| { readonly kind: "ready"; readonly sessionId: string; readonly sessionName: string; readonly port: string; readonly stateDir: string; readonly statusPath: string; readonly gateSocketPath: string };

/**
 * The identity gate is exactly WOLFPACK_SESSION_ID plus WOLFPACK_SESSION_NAME. With identity present, the values the
 * configured core reads later are validated up front so a misconfiguration is named instead of failing per call.
 */
export function readClaudeEnvironment(env: Readonly<Record<string, string | undefined>>): ClaudeEnvironment {
	const sessionId = env.WOLFPACK_SESSION_ID?.trim();
	const sessionName = env.WOLFPACK_SESSION_NAME?.trim();
	if (!sessionId || !sessionName) return { kind: "inactive" };
	if (!SESSION_ID_PATTERN.test(sessionId)) return misconfigured("WOLFPACK_SESSION_ID", "must be the broker-injected Wolfpack session UUID; names cannot recover task state");
	const port = env.WOLFPACK_PORT ?? DEFAULT_WOLFPACK_PORT;
	if (!/^[0-9]+$/.test(port) || Number(port) < 1 || Number(port) > 65535) return misconfigured("WOLFPACK_PORT", "must be a TCP port from 1 to 65535");
	const secret = env.WOLFPACK_JWT_SECRET?.trim();
	if (secret && secret.length < JWT_SECRET_MIN_CHARS) return misconfigured("WOLFPACK_JWT_SECRET", `must contain at least ${JWT_SECRET_MIN_CHARS} characters when set`);
	let stateDir: string;
	if (env.WOLFPACK_DIR !== undefined) {
		if (!isAbsolute(env.WOLFPACK_DIR)) return misconfigured("WOLFPACK_DIR", "must be an absolute path when set");
		stateDir = env.WOLFPACK_DIR;
	} else {
		if (!env.HOME || !isAbsolute(env.HOME)) return misconfigured("HOME", "must be an absolute path when WOLFPACK_DIR is unset");
		stateDir = join(env.HOME, ".wolfpack");
	}
	const directory = join(stateDir, CLAUDE_PLUGIN.statusDirectory);
	return { kind: "ready", sessionId, sessionName, port, stateDir, statusPath: join(directory, `${sessionId}.json`), gateSocketPath: gateSocketPath(directory, sessionId) };
}

/**
 * The worker gate socket of a session. A session id can be 128 characters, so the file name is a short hash of it: unix
 * socket paths are limited to 103 bytes on macOS. A longer path is refused by both the server and the hook.
 */
function gateSocketPath(directory: string, sessionId: string): string {
	const name = createHash("sha256").update(sessionId).digest("hex").slice(0, CLAUDE_PLUGIN.gateSocketHashChars);
	return join(directory, CLAUDE_PLUGIN.gateSocketDirectory, `${name}.sock`);
}

function misconfigured(variable: string, reason: string): ClaudeEnvironment {
	return { kind: "misconfigured", variable, reason };
}
