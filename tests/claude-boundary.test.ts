import { expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";

const IMPORT = /\bfrom\s+["']([^"']+)["']|\bimport\s*\(\s*["']([^"']+)["']\s*\)|^import\s+["']([^"']+)["']/gm;
const imports = (path: string): string[] => [...readFileSync(path, "utf8").matchAll(IMPORT)].map((match) => match[1] ?? match[2] ?? match[3]!);
const sources = (dir: string): string[] => readdirSync(dir).filter((name) => name.endsWith(".ts")).map((name) => join(dir, name));

test("the claude adapter imports no pi packages and nothing from the pi extension", () => {
	for (const file of sources("src/claude")) {
		for (const specifier of imports(file)) {
			expect(`${file}: ${specifier}`).not.toMatch(/@(mariozechner|earendil-works)\/pi-|\/extension$/);
		}
	}
});

test("the host-neutral core does not import the claude adapter", () => {
	for (const file of sources("src")) {
		for (const specifier of imports(file)) expect(`${file}: ${specifier}`).not.toMatch(/(^|\/)claude(\/|$)/);
	}
});
