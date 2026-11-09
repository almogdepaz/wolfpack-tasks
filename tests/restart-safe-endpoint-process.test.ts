import { expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";

test("actual endpoint process replacement rehydrates the same journal in Bun and native Node SQLite without replaying task creation", async () => {
  const root = mkdtempSync(join(tmpdir(), "endpoint-process-recovery-"));
  const sessionId = crypto.randomUUID(), epoch = crypto.randomUUID(), endpoint = { relay: "wolfpack-pi-tasks-v2", id: crypto.randomUUID() };
  const accepted = new Map<string, string>(); const generations: string[] = []; let loseReply = true;
  const server = Bun.serve({ hostname: "127.0.0.1", port: 0, async fetch(request) {
    const body = await request.json() as any;
    if (body.operation === "connect") generations.push(body.generation);
    let value: any;
    if (body.operation === "connect") value = { kind: "connected", endpoint, sessionId, leaseExpiresAt: new Date(Date.now() + 60_000).toISOString() };
    else if (body.operation === "resolve") value = { kind: "resolved", endpoint: body.target };
    else if (body.operation === "send") {
      const text = JSON.stringify(body.envelope), prior = accepted.get(body.envelope.envelopeId);
      if (prior && prior !== text) throw new Error("retry changed persisted wire content");
      accepted.set(body.envelope.envelopeId, text);
      if (loseReply) { loseReply = false; return Response.json({ ok: false, profile: body.profile, epoch, error: { code: "RELAY_UNAVAILABLE", retryable: true, mayHaveBeenDelivered: true } }, { status: 503 }); }
      value = { kind: "accepted", envelopeId: body.envelope.envelopeId, acceptanceId: crypto.randomUUID(), duplicate: prior !== undefined, forwarding: "local" };
    } else value = { kind: "page", deliveries: [], nextCursor: body.cursor ?? "0", hasMore: false };
    return Response.json({ ok: true, profile: body.profile, epoch, value });
  } });
  const fixture = join(import.meta.dir, "fixtures/restart-safe-endpoint-process.ts");
  const run = async (runtime: string, script: string, phase: string) => {
    const child = Bun.spawn([runtime, script, phase, `http://127.0.0.1:${server.port}`, root, sessionId], { cwd: root,
      env: { PATH: process.env.PATH, HOME: root, WOLFPACK_DIR: join(root, "state"), WOLFPACK_TEST: "1", WOLFPACK_PORT: "1" }, stdout: "pipe", stderr: "pipe" });
    const [stdout, stderr, status] = await Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text(), child.exited]);
    expect(status, stderr).toBe(0); return JSON.parse(stdout);
  };
  try {
    const first = await run(process.execPath, fixture, "create"); expect(first.tasks).toHaveLength(1); expect(accepted.size).toBe(1);
    const resumed = await run(process.execPath, fixture, "resume"); expect(resumed).toEqual(first); expect(accepted.size).toBe(1);
    const node = Bun.which("node"); expect(node).not.toBeNull();
    const built = await Bun.build({ entrypoints: [fixture], target: "node", outdir: root, naming: "endpoint.mjs" });
    expect(built.success, JSON.stringify(built.logs)).toBe(true);
    const nodeResumed = await run(node!, join(root, "endpoint.mjs"), "resume");
    expect(nodeResumed).toEqual(first); expect(new Set(generations).size).toBe(1); expect(accepted.size).toBe(1);
  } finally { await server.stop(true); rmSync(root, { recursive: true, force: true }); }
}, 30_000);
