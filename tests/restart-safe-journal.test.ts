import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { chmodSync, mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { openTaskJournal } from "../src/task-journal";
import { createTaskStore } from "../src/task-store";
import { createTaskCore } from "../src/task-core";
import { TaskProtocolError } from "../src/task-protocol";
import { createInMemoryTaskRelay } from "../src/in-memory-task-relay";

function fixture() {
  const root = mkdtempSync(join(tmpdir(), "task-journal-"));
  const identity = { sessionId: crypto.randomUUID(), url: "http://127.0.0.1:1" }, path = join(root, "session", "tasks.sqlite");
  return { root, path, identity, open: () => createTaskStore({}, openTaskJournal(path, identity)), cleanup: () => rmSync(root, { recursive: true, force: true }) };
}

test("terminal-before-parent-ACK reopens both replicas and retries the same logical terminal and ACK", async () => {
  const a = fixture(), b = fixture(), relay = createInMemoryTaskRelay("fixture");
  const source = { relay: relay.id, id: "source" }, target = { relay: relay.id, id: "target" };
  let originStore = a.open(), receiverStore = b.open();
  try {
    let origin = createTaskCore({ endpoint: source, relay, store: originStore }), receiver = createTaskCore({ endpoint: target, relay, store: receiverStore });
    await origin.connect(); await receiver.connect();
    const { taskId } = await origin.createTask({ target, task: "complete once", timeoutMs: 60_000 });
    await receiver.receive();
    await receiver.submitIntent({ taskId, type: "task.completed", payload: { summary: "done" } });
    const terminal = receiverStore.outbox("accepted").find(row => row.envelope.kind === "intent")!.envelope.envelopeId;
    await origin.receive(); expect(origin.getTask(taskId)!.status).toBe("completed");
    const eventIds = origin.getTask(taskId)!.events.map(event => event.eventId);
    originStore.close(); receiverStore.close(); originStore = a.open(); receiverStore = b.open();
    origin = createTaskCore({ endpoint: source, relay, store: originStore }); receiver = createTaskCore({ endpoint: target, relay, store: receiverStore });
    await receiver.submitIntent({ taskId, type: "task.completed", payload: { summary: "retry must not replace original" } });
    expect(receiverStore.outbox("accepted").filter(row => row.envelope.envelopeId === terminal)).toHaveLength(1);
    expect(origin.getTask(taskId)!.events.map(event => event.eventId)).toEqual(eventIds);
    await origin.acknowledgeParent(taskId);
    const ackIds = origin.getTask(taskId)!.events.filter(event => event.type === "task.parent_acknowledged").map(event => event.eventId);
    expect(ackIds).toHaveLength(1);
    originStore.close(); originStore = a.open(); origin = createTaskCore({ endpoint: source, relay, store: originStore });
    await origin.acknowledgeParent(taskId); expect(origin.getTask(taskId)!.events.filter(event => event.type === "task.parent_acknowledged").map(event => event.eventId)).toEqual(ackIds);
  } finally { originStore.close(); receiverStore.close(); a.cleanup(); b.cleanup(); }
});

test("owner lease fences concurrent opens; rollback is atomic on disk; modes remain private", () => {
  const f = fixture(); const store = f.open();
  try {
    expect(() => f.open()).toThrow();
    store.setEndpointGeneration("stable-generation");
    expect(() => store.transaction(() => { store.setEndpointGeneration("rolled-back"); store.setReceiveCursor("42"); throw new Error("cut"); })).toThrow("cut");
    store.close(); const reopened = f.open();
    expect(reopened.getEndpointGeneration()).toBe("stable-generation"); expect(reopened.getReceiveCursor()).toBe("0"); reopened.close();
    expect(statSync(f.path).mode & 0o777).toBe(0o600); expect(statSync(join(f.root, "session")).mode & 0o777).toBe(0o700);
  } finally { store.close(); f.cleanup(); }
});

test("corrupt cells, unsupported versions, identity mismatch and permissive paths fail without clearing storage", () => {
  for (const kind of ["corrupt", "version", "identity", "permission"] as const) {
    const f = fixture(); const store = f.open(); store.setEndpointGeneration("original"); store.close();
    try {
      if (kind === "permission") chmodSync(f.path, 0o644);
      else if (kind !== "identity") {
        const db = new Database(f.path);
        db.exec(kind === "version" ? "PRAGMA user_version=99" : "UPDATE cells SET value='\"tampered\"'"); db.close();
      }
      const before = readFileSync(f.path);
      expect(() => kind === "identity" ? openTaskJournal(f.path, { ...f.identity, sessionId: crypto.randomUUID() }) : f.open()).toThrow();
      expect(readFileSync(f.path)).toEqual(before);
    } finally { f.cleanup(); }
  }
});

test("an unsupported WAL-mode database is refused without silently converting its storage", () => {
  const f = fixture(), store = f.open(); store.close();
  const db = new Database(f.path); db.exec("PRAGMA journal_mode=WAL; PRAGMA wal_checkpoint(TRUNCATE)"); db.close();
  // This private writer has closed and checkpointed; keep only the incompatible main-file mode.
  rmSync(`${f.path}-wal`, { force: true }); rmSync(`${f.path}-shm`, { force: true });
  const before = readFileSync(f.path);
  try { expect(() => { const reopened = f.open(); reopened.close(); }).toThrow(); expect(readFileSync(f.path)).toEqual(before); }
  finally { f.cleanup(); }
});

test("an existing permissive rollback journal is refused before SQLite can recover or remove it", () => {
  const f = fixture(), store = f.open(); store.setEndpointGeneration("original"); store.close();
  const journal = `${f.path}-journal`; writeFileSync(journal, "untrusted rollback bytes", { mode: 0o600 }); chmodSync(journal, 0o644);
  try { expect(() => { const reopened = f.open(); reopened.close(); }).toThrow(); expect(readFileSync(journal, "utf8")).toBe("untrusted rollback bytes"); }
  finally { f.cleanup(); }
});

test("uncertain creation commit reports its stable task ID and does not submit transport", async () => {
  const f = fixture(), native = openTaskJournal(f.path, f.identity);
  let poison = false;
  const store = createTaskStore({}, { ...native, assertHealthy() { if (poison) throw new Error("poisoned"); native.assertHealthy(); }, commit(rows, removed) { native.commit(rows, removed); poison = true; throw new TaskProtocolError("TASK_STORAGE_UNCERTAIN", "commit outcome unknown", { retryable: false }); } });
  const relay = createInMemoryTaskRelay("fixture"), endpoint = { relay: relay.id, id: "source" };
  const core = createTaskCore({ endpoint, relay, store });
  try {
    await core.connect();
    const error = await core.createTask({ target: endpoint, task: "uncertain commit", timeoutMs: 60_000 }).catch(error => error);
    expect(error.code).toBe("TASK_STORAGE_UNCERTAIN"); expect(error.details.taskId).toBeString();
    expect(relay.envelopesFor(endpoint)).toEqual([]);
    store.close(); const reopened = f.open(); expect(reopened.listTasks().map(task => task.taskId)).toEqual([error.details.taskId]); reopened.close();
  } finally { store.close(); f.cleanup(); }
});

test("crash with an uncommitted SQLite write recovers the committed generation", async () => {
  const f = fixture(); const store = f.open(); store.setEndpointGeneration("committed"); store.close();
  const script = join(f.root, "crash.ts");
  writeFileSync(script, `import { Database } from "bun:sqlite"; const db = new Database(${JSON.stringify(f.path)}); db.exec("BEGIN EXCLUSIVE; UPDATE cells SET value='broken'"); process.exit(0);`, { mode: 0o600 });
  try {
    const child = Bun.spawn([process.execPath, script], { cwd: f.root, env: { HOME: f.root, WOLFPACK_DIR: join(f.root, "state"), PATH: process.env.PATH }, stdout: "pipe", stderr: "pipe" });
    const output = await new Response(child.stderr).text(); expect(await child.exited, output).toBe(0);
    const reopened = f.open(); expect(reopened.getEndpointGeneration()).toBe("committed"); reopened.close();
  } finally { f.cleanup(); }
});
