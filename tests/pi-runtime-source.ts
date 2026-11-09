import { isAbsolute, join } from "node:path";
import { readFileSync } from "node:fs";
import { pathToFileURL } from "node:url";

/** Explicit existing SDK selection. Never installs or rewrites a vendor package. */
export const piRuntimeSource = process.env.PI_TASKS_PI_RUNTIME;
export async function loadSelectedPiRuntime() {
  if (!piRuntimeSource || !isAbsolute(piRuntimeSource)) throw new Error("PI_TASKS_PI_RUNTIME must select an existing absolute Pi package root");
  const manifest = JSON.parse(readFileSync(join(piRuntimeSource, "package.json"), "utf8"));
  const expected = process.env.PI_TASKS_PI_RUNTIME_VERSION;
  if (!expected || manifest.name !== "@earendil-works/pi-coding-agent" || manifest.version !== expected) throw new Error("explicit Pi runtime version pin does not match source");
  const sdk = await import(pathToFileURL(join(piRuntimeSource, "dist/index.js")).href);
  const ai = await import(pathToFileURL(join(piRuntimeSource, "../pi-ai/dist/index.js")).href);
  return { sdk, ai, version: manifest.version as string };
}
