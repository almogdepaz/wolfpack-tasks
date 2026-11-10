import { afterEach, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const repositoryRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const temporaryDirectories: string[] = [];

afterEach(() => {
	for (const directory of temporaryDirectories.splice(0)) rmSync(directory, { recursive: true, force: true });
});

async function runPi(): Promise<{ exitCode: number; stdout: string; stderr: string }> {
	const home = mkdtempSync("/tmp/pi-tasks-node-runtime-");
	temporaryDirectories.push(home);
	// The extension registers on the relay named by WOLFPACK_SESSION_NAME/WOLFPACK_PORT. Inheriting them would replace the
	// caller's own task endpoint, so the spawned pi gets no wolfpack identity and is never a task worker.
	const env: Record<string, string> = { HOME: home };
	for (const [key, value] of Object.entries(process.env)) if (value !== undefined && !key.startsWith("WOLFPACK_") && key !== "PI_TASK_WORKER" && key !== "HOME") env[key] = value;
	const subprocess = Bun.spawn([
		join(repositoryRoot, "node_modules", ".bin", "pi"),
		"--no-extensions",
		"--extension", join(repositoryRoot, "src", "extension.ts"),
		"--mode", "rpc",
		"--no-session",
		"--no-skills",
		"--no-prompt-templates",
		"--no-context-files",
	], {
		cwd: repositoryRoot,
		env,
		stdin: "pipe",
		stdout: "pipe",
		stderr: "pipe",
	});
	subprocess.stdin.write('{"type":"get_state"}\n');
	subprocess.stdin.end();

	const [exitCode, stdout, stderr] = await Promise.all([
		subprocess.exited,
		new Response(subprocess.stdout).text(),
		new Response(subprocess.stderr).text(),
	]);
	return { exitCode, stdout, stderr };
}

test("loads the default extension through Pi's Node runtime", async () => {
	const { exitCode, stdout, stderr } = await runPi();
	const messages = stdout.split("\n").filter((line) => line.trim().length > 0).map((line) => JSON.parse(line) as unknown);
	expect(stderr).not.toContain("Failed to load extension");
	expect(exitCode).toBe(0);
	expect(messages).toContainEqual(expect.objectContaining({ type: "response", command: "get_state", success: true }));
});

test("the spawned pi does not use the caller's wolfpack identity", async () => {
	// A caller inside a wolfpack session: its relay is a listener this test owns, never a live server.
	const requests: string[] = [];
	const relay = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: async (request) => {
		requests.push(`${request.method} ${new URL(request.url).pathname} ${await request.text()}`);
		return Response.json({ ok: false, error: { code: "RELAY_UNAVAILABLE", retryable: true } }, { status: 503 });
	} });
	const caller = { WOLFPACK_SESSION_NAME: "caller-session", WOLFPACK_PORT: String(relay.port) };
	const previous = Object.fromEntries(Object.keys(caller).map((key) => [key, process.env[key]]));
	Object.assign(process.env, caller);
	try {
		const { exitCode, stderr } = await runPi();
		expect(stderr).not.toContain("Failed to load extension");
		expect(exitCode).toBe(0);
		expect(requests).toEqual([]);
	} finally {
		for (const [key, value] of Object.entries(previous)) { if (value === undefined) delete process.env[key]; else process.env[key] = value; }
		await relay.stop(true);
	}
});
