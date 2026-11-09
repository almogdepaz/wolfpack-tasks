import { homedir } from "node:os";
import { join } from "node:path";
import { openTaskJournal } from "./task-journal";
import type { TaskCore, WakeRequest } from "./task-core";
import { TaskProtocolError } from "./task-protocol";
import { createTaskStore } from "./task-store";
import { createVolatileTaskSession, createRestartSafeTaskSession, RESTART_SAFE_PROFILE } from "./volatile-task-session";
import { wolfpackLocalHeaders } from "./wolfpack-local-auth";

export interface OwnedTaskCore extends TaskCore {
  /** Fence new work, abort requests, settle active calls, then close without deleting the journal. */
  close(): Promise<void>;
}
export interface ConfiguredTaskCoreOptions {
  readonly sessionName?: string;
  readonly sessionId?: string;
  readonly stateRoot?: string;
  /** Explicit legacy memory-only mode; never selected as a recovery fallback. */
  readonly profile?: "volatile-v1" | "restart-safe-v1";
  readonly baseUrl?: string;
  readonly fetch?: typeof fetch;
  readonly requestTimeoutMs?: number;
  /** Explicit volatile-only fresh binding. Restart-safe journals refuse identity replacement. */
  readonly rebind?: boolean;
}

/** Normal extension transport. Never probe/fallback to the legacy durable relay. */
export async function createConfiguredTaskCore(options: ConfiguredTaskCoreOptions = {}, signal?: AbortSignal): Promise<OwnedTaskCore> {
  const profile = options.profile ?? process.env.WOLFPACK_TASK_RELAY_PROFILE ?? RESTART_SAFE_PROFILE;
  if (profile !== RESTART_SAFE_PROFILE && profile !== "volatile-v1") throw new TaskProtocolError("RELAY_PROFILE_REQUIRED", "unsupported explicit task relay profile", { retryable: false });
  const sessionName = options.sessionName ?? process.env.WOLFPACK_SESSION_NAME;
  const sessionId = options.sessionId ?? process.env.WOLFPACK_SESSION_ID;
  if (profile === RESTART_SAFE_PROFILE && (!sessionId || !/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(sessionId))) throw new TaskProtocolError("SOURCE_MISMATCH", "broker-injected WOLFPACK_SESSION_ID UUID is required; names cannot recover task state", { retryable: false });
  if (profile === "volatile-v1" && !sessionName?.trim()) throw new TaskProtocolError("RELAY_UNAVAILABLE", "WOLFPACK_SESSION_NAME is required for the volatile adapter");
  const port = process.env.WOLFPACK_PORT ?? "18790";
  if (options.baseUrl === undefined && (!/^[0-9]+$/.test(port) || Number(port) < 1 || Number(port) > 65535)) throw new TypeError("invalid WOLFPACK_PORT");
  const base = new URL(options.baseUrl ?? `http://127.0.0.1:${port}`);
  if (base.username || base.password || base.search || base.hash || base.pathname !== "/") throw new TypeError("relay base URL must be an origin");
  const url = new URL(`/api/task-relay/${profile}`, base).href;
  const stateRoot = options.stateRoot ?? join(process.env.WOLFPACK_DIR ?? join(homedir(), ".wolfpack"), "task-endpoints", RESTART_SAFE_PROFILE);
  const journal = profile === RESTART_SAFE_PROFILE ? openTaskJournal(join(stateRoot, sessionId!, "tasks.sqlite"), { sessionId: sessionId!, url }) : undefined;
  const store = createTaskStore({}, journal);
  let session: ReturnType<typeof createVolatileTaskSession> | undefined;
  try {
    const requestFetch = options.fetch ?? fetch;
    const authenticated = Object.assign((input: Parameters<typeof fetch>[0], init?: RequestInit) => requestFetch(input, { ...init, headers: wolfpackLocalHeaders(url, init?.headers) }), { preconnect: requestFetch.preconnect }) as typeof fetch;
    session = (profile === RESTART_SAFE_PROFILE ? createRestartSafeTaskSession : createVolatileTaskSession)({ url, callerSession: profile === RESTART_SAFE_PROFILE ? sessionId! : sessionName!, store, fetch: authenticated, ...(options.requestTimeoutMs !== undefined && { requestTimeoutMs: options.requestTimeoutMs }) });
    const core = await (options.rebind ? session.rebind(signal) : session.connect(signal));
    const active = new Set<Promise<unknown>>();
    let closed = false, closing: Promise<void> | undefined;
    const owned = { ...core, ...(profile === RESTART_SAFE_PROFILE && { recoveryProfile: RESTART_SAFE_PROFILE }) } as OwnedTaskCore;
    const track = <T>(result: Promise<T>): Promise<T> => {
      active.add(result);
      void result.then(() => active.delete(result), () => active.delete(result));
      return result;
    };
    for (const [key, value] of Object.entries(core)) {
      if (typeof value !== "function") continue;
      Object.defineProperty(owned, key, { enumerable: true, value: (...args: unknown[]) => {
        if (closed) throw new TaskProtocolError("RELAY_CLOSED", "task session is closed", { retryable: false });
        // Core methods may call sibling methods through `this` (notably intent ACK).
        // Keep their original receiver while the outer promise owns the full operation.
        const result: unknown = value.apply(core, args);
        if (key === "recordWakeRequest") {
          const request = result as WakeRequest;
          return {
            outcome: request.outcome,
            flush: (signal?: AbortSignal): Promise<void> => {
              if (closed) throw new TaskProtocolError("RELAY_CLOSED", "task session is closed", { retryable: false });
              return track(request.flush(signal));
            },
            // A closed owner cannot withdraw durable operations from a successor process.
            discard: () => { if (!closed) request.discard(); },
          };
        }
        return result instanceof Promise ? track(result) : result;
      } });
    }
    owned.close = () => {
      if (closing) return closing;
      closed = true;
      session!.close();
      closing = Promise.allSettled([...active]).then(() => { store.close(); });
      return closing;
    };
    return owned;
  } catch (error) {
    session?.close(); store.close(); throw error;
  }
}
