/** Minimal JSON-RPC 2.0 shapes for newline-delimited MCP stdio. */
export type JsonRpcId = string | number;

export type JsonRpcMessage =
	| { readonly jsonrpc: "2.0"; readonly id: JsonRpcId; readonly method: string; readonly params?: unknown }
	| { readonly jsonrpc: "2.0"; readonly method: string; readonly params?: unknown }
	| { readonly jsonrpc: "2.0"; readonly id: JsonRpcId; readonly result: unknown }
	| { readonly jsonrpc: "2.0"; readonly id: JsonRpcId | null; readonly error: JsonRpcError };

export interface JsonRpcError {
	readonly code: number;
	readonly message: string;
}

export const JSON_RPC_ERROR = Object.freeze({
	parse: -32700,
	invalidRequest: -32600,
	methodNotFound: -32601,
	invalidParams: -32602,
	internal: -32603,
});

export interface JsonRpcRequest {
	readonly id?: JsonRpcId;
	readonly method: string;
	readonly params?: unknown;
}

/** Returns the request or notification, or the error to answer. Responses from the client are ignored (undefined). */
export function parseJsonRpc(line: string): JsonRpcRequest | JsonRpcError | undefined {
	let value: unknown;
	try {
		value = JSON.parse(line);
	} catch {
		return { code: JSON_RPC_ERROR.parse, message: "parse error" };
	}
	if (!isRecord(value) || value.jsonrpc !== "2.0") return { code: JSON_RPC_ERROR.invalidRequest, message: "invalid request" };
	const id = value.id;
	if (id !== undefined && typeof id !== "string" && typeof id !== "number") return { code: JSON_RPC_ERROR.invalidRequest, message: "invalid request id" };
	if (typeof value.method !== "string") return "result" in value || "error" in value ? undefined : { code: JSON_RPC_ERROR.invalidRequest, message: "invalid request" };
	return { ...(id !== undefined && { id }), method: value.method, ...(value.params !== undefined && { params: value.params }) };
}

export function isJsonRpcError(value: JsonRpcRequest | JsonRpcError): value is JsonRpcError {
	return "code" in value;
}

export function isRecord(value: unknown): value is Record<string, unknown> {
	return value !== null && typeof value === "object" && !Array.isArray(value);
}
