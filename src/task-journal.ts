import { createHash } from "node:crypto";
import { closeSync, fsyncSync, lstatSync, mkdirSync, openSync } from "node:fs";
import { dirname } from "node:path";
import { openSqliteDatabase } from "./sqlite-database";
import { TaskProtocolError } from "./task-protocol";

export interface JournalRow { readonly table: string; readonly key: string; readonly text: string; readonly order: number; }
export interface TaskJournal {
  assertHealthy(): void;
  load(): readonly JournalRow[];
  commit(rows: readonly JournalRow[], removed: readonly { readonly table: string; readonly key: string }[]): void;
  close(): void;
}
const FORMAT = "wolfpack-task-endpoint/restart-safe-v1";
const digest = (row: JournalRow) => createHash("sha256").update(JSON.stringify([row.table, row.key, row.text, row.order])).digest("hex");
function invalid(message: string): never { throw new TaskProtocolError("TASK_STORAGE_INVALID", message, { retryable: false }); }
function privatePath(path: string, directory: boolean): void {
  const stat = lstatSync(path);
  if (stat.isSymbolicLink() || (directory ? !stat.isDirectory() : !stat.isFile()) || (stat.mode & 0o077) !== 0 || (process.getuid && stat.uid !== process.getuid()) || (!directory && stat.nlink !== 1)) invalid("task storage requires owner-private regular paths");
}
/** A new format, never a migration of the retired task SQLite databases. */
export function openTaskJournal(path: string, identity: { readonly sessionId: string; readonly url: string }): TaskJournal {
  const firstCreated = mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
  if (firstCreated) for (let directory = dirname(path);; directory = dirname(directory)) {
    const fd = openSync(directory, "r"); try { fsyncSync(fd); } finally { closeSync(fd); }
    if (directory === dirname(firstCreated)) break;
  }
  privatePath(dirname(dirname(path)), true); privatePath(dirname(path), true);
  let created = false;
  try { const fd = openSync(path, "wx", 0o600); closeSync(fd); created = true; }
  catch (error) { if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error; }
  privatePath(path, false);
  for (const suffix of ["-journal", "-wal", "-shm"]) {
    try {
      privatePath(path + suffix, false);
      if (suffix !== "-journal") invalid("unsupported task SQLite journal mode");
    } catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
  }
  const db = openSqliteDatabase(path);
  let closed = false, poisoned = false;
  try {
    // EXCLUSIVE is deliberately held across commits: two endpoint processes cannot own this journal.
    // DELETE bounds the rollback journal by one transaction and inherits the private database mode.
    db.exec("PRAGMA busy_timeout=0; PRAGMA locking_mode=EXCLUSIVE;");
    if (!created && (db.query("PRAGMA journal_mode").get() as { journal_mode: string }).journal_mode !== "delete") invalid("unsupported task SQLite journal mode");
    db.exec("PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL; PRAGMA max_page_count=32768; BEGIN EXCLUSIVE;");
    if (created) {
      db.exec("CREATE TABLE metadata (id INTEGER PRIMARY KEY CHECK(id=1), format TEXT NOT NULL, identity TEXT NOT NULL); CREATE TABLE cells (bucket TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL, ordinal INTEGER NOT NULL, digest TEXT NOT NULL, PRIMARY KEY(bucket,key)); PRAGMA user_version=1;");
      db.query("INSERT INTO metadata VALUES (1,?,?)").run(FORMAT, JSON.stringify(identity));
    } else {
      const version = db.query("PRAGMA user_version").get() as { user_version: number };
      const meta = db.query("SELECT * FROM metadata WHERE id=1").get() as { format?: string; identity?: string } | undefined;
      if (version.user_version !== 1 || meta?.format !== FORMAT || meta.identity !== JSON.stringify(identity)) invalid("unsupported task storage format or owner mismatch");
      const integrity = db.query("PRAGMA integrity_check").all();
      if (integrity.length !== 1 || Object.values(integrity[0] as object)[0] !== "ok") invalid("task storage integrity check failed");
    }
    db.exec("COMMIT");
  } catch (error) {
    try { db.exec("ROLLBACK"); } catch {} db.close();
    if (error instanceof TaskProtocolError) throw error;
    const code = (error as { code?: string; errcode?: number }).code, busy = code === "SQLITE_BUSY" || (error as { errcode?: number }).errcode === 5;
    throw new TaskProtocolError(busy ? "TASK_STORAGE_BUSY" : "TASK_STORAGE_INVALID", busy ? "another process owns this task journal" : "task journal could not be validated", { retryable: busy });
  }
  const upsert = db.query("INSERT INTO cells VALUES (?,?,?,?,?) ON CONFLICT(bucket,key) DO UPDATE SET value=excluded.value, ordinal=excluded.ordinal, digest=excluded.digest");
  const remove = db.query("DELETE FROM cells WHERE bucket=? AND key=?");
  const check = () => { if (closed || poisoned) invalid("task journal is closed or has an uncertain commit; reopen required"); };
  return {
    assertHealthy: check,
    load() {
      check();
      const rows = db.query("SELECT bucket AS 'table', key, value AS text, ordinal AS 'order', digest FROM cells ORDER BY ordinal").all() as (JournalRow & { digest: string })[];
      for (const row of rows) if (typeof row.table !== "string" || typeof row.key !== "string" || typeof row.text !== "string" || !Number.isSafeInteger(row.order) || row.order < 0 || digest(row) !== row.digest) invalid("task storage cell is corrupt");
      return rows;
    },
    commit(rows, removed) {
      check();
      try {
        db.exec("BEGIN EXCLUSIVE");
        for (const row of rows) upsert.run(row.table, row.key, row.text, row.order, digest(row));
        for (const row of removed) remove.run(row.table, row.key);
        db.exec("COMMIT");
      } catch (error) {
        // Even rollback failure must not allow later sends based on uncertain RAM.
        poisoned = true; try { db.exec("ROLLBACK"); } catch {}
        throw new TaskProtocolError("TASK_STORAGE_UNCERTAIN", "task journal commit outcome is unknown; reopen before retrying", { retryable: false });
      }
    },
    close() { if (!closed) { closed = true; db.close(); } },
  };
}
