// Synchronous native adapters: Bun in Pi, Node 22+ in Claude Code.
import { createRequire } from "node:module";
export interface SqliteStatement { run(...parameters: unknown[]): unknown; get(...parameters: unknown[]): unknown; all(...parameters: unknown[]): unknown[]; }
export interface SqliteDatabase { exec(sql: string): void; query(sql: string): SqliteStatement; close(): void; }
const require = createRequire(import.meta.url);
export function openSqliteDatabase(path: string): SqliteDatabase {
  if (process.versions.bun) return new (require("bun:sqlite").Database)(path, { create: false, strict: true }) as SqliteDatabase;
  const database = new (require("node:sqlite").DatabaseSync)(path);
  return { exec: sql => database.exec(sql), query: sql => database.prepare(sql), close: () => database.close() };
}
