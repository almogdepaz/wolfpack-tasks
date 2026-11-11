import { expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";

const root = join(import.meta.dir, "..");
const epoch = "00000000-0000-4000-8000-000000000001";
const endpoint = { relay: "wolfpack-pi-tasks-v2", id: "00000000-0000-4000-8000-000000000002" };

for (const harness of ["pi", "claude"] as const) {
  for (const exit of harness === "pi" ? ["stdin"] as const : ["stdin", "SIGTERM", "SIGINT"] as const) {
    test(`${harness} ${exit} releases its owned relay lease before exit`, async () => {
      const home = mkdtempSync("/tmp/tasks-shutdown-");
      const calls: any[] = [];
      let ready!: () => void;
      const receiving = new Promise<void>(resolve => { ready = resolve; });
      const relay = Bun.serve({ hostname: "127.0.0.1", port: 0, async fetch(request) {
        const body = await request.json() as any; calls.push(body);
        const value = body.operation === "connect"
          ? { kind: "connected", endpoint, leaseExpiresAt: new Date(Date.now() + 60_000).toISOString() }
          : body.operation === "disconnect" ? { kind: "disconnected" }
            : { kind: "page", deliveries: [], nextCursor: "0", hasMore: false };
        if (body.operation === "receive") ready();
        return Response.json({ ok: true, profile: "volatile-v1", epoch, value });
      } });
      // Explicit fixture identity only. No inherited credentials, worker flags, or parent session.
      const env = { PATH: process.env.PATH ?? "", HOME: home, WOLFPACK_DIR: join(home, ".wolfpack"),
        WOLFPACK_SESSION_ID: crypto.randomUUID(), WOLFPACK_SESSION_NAME: "shutdown-fixture", WOLFPACK_PORT: String(relay.port) };
      const argv = harness === "pi" ? [join(root, "node_modules", ".bin", "pi"), "--no-extensions", "--extension", join(root, "src/extension.ts"),
        "--mode", "rpc", "--no-session", "--no-skills", "--no-prompt-templates", "--no-context-files"]
        : [Bun.which("node")!, join(root, "claude-plugin/dist/server.mjs")];
      const child = Bun.spawn(argv, { cwd: root, env, stdin: "pipe", stdout: "pipe", stderr: "pipe" });
      const output = Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text()]);
      let deadline: ReturnType<typeof setTimeout> | undefined;
      const timeout = new Promise<never>((_, reject) => { deadline = setTimeout(() => reject(new Error("harness shutdown deadline")), 10_000); });
      try {
        if (harness === "claude") {
          child.stdin.write(JSON.stringify({ jsonrpc: "2.0", id: 1, method: "initialize", params: { protocolVersion: "2025-11-25", capabilities: {}, clientInfo: { name: "test", version: "1" } } }) + "\n");
          child.stdin.write(JSON.stringify({ jsonrpc: "2.0", method: "notifications/initialized" }) + "\n");
        } else child.stdin.write('{"type":"get_state"}\n');
        await Promise.race([receiving, timeout, child.exited.then(code => { throw new Error(`harness exited before registration: ${code}`); })]);
        if (exit === "stdin") await child.stdin.end(); else child.kill(exit);
        expect(await Promise.race([child.exited, timeout])).toBe(0);
        const [, stderr] = await output;
        expect(stderr).not.toContain("Failed to load extension");
        expect(calls.filter(body => body.operation === "disconnect")).toEqual([{
          operation: "disconnect", profile: "volatile-v1", epoch, endpoint, callerSession: "shutdown-fixture",
        }]);
      } finally {
        clearTimeout(deadline);
        if (child.exitCode === null) child.kill("SIGKILL");
        await child.exited; await output; await relay.stop(true);
        rmSync(home, { recursive: true, force: true });
      }
    }, 15_000);
  }
}
