import { Compile } from "typebox/compile";
import type { TSchema } from "typebox";
import { Value } from "typebox/value";

const validators = new WeakMap<TSchema, ReturnType<typeof Compile>>();

/**
 * Mirrors Pi's tool argument validation (pi-ai `validateToolArguments` for TypeBox schemas): clone, convert, check,
 * and the same error text, so a Claude model sees the same feedback for the same invalid call.
 */
export function validateToolArguments(name: string, schema: TSchema, input: unknown): unknown {
	const args = structuredClone(input);
	Value.Convert(schema, args);
	let validator = validators.get(schema);
	if (!validator) { validator = Compile(schema); validators.set(schema, validator); }
	if (validator.Check(args)) return args;
	const errors = validator.Errors(args).map((error) => `  - ${validationPath(error)}: ${error.message}`).join("\n") || "Unknown validation error";
	throw new Error(`Validation failed for tool "${name}":\n${errors}\n\nReceived arguments:\n${JSON.stringify(input, null, 2)}`);
}

function validationPath(error: { readonly keyword: string; readonly instancePath: string; readonly params: unknown }): string {
	const base = error.instancePath.replace(/^\//, "").replace(/\//g, ".");
	if (error.keyword === "required") {
		const property = (error.params as { readonly requiredProperties?: readonly string[] }).requiredProperties?.[0];
		if (property) return base ? `${base}.${property}` : property;
	}
	return base || "root";
}
