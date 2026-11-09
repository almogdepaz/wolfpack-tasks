import type { TaskCore, WakeRequest } from "./task-core";
import { TaskProtocolError } from "./task-protocol";
import { createTaskStore } from "./task-store";
import { createVolatileTaskSession } from "./volatile-task-session";
import { wolfpackLocalHeaders } from "./wolfpack-local-auth";

export interface OwnedTaskCore extends TaskCore {
  /** Fence new work, abort requests, settle active calls, then discard RAM state. */
  close(): Promise<void>;
}
export interface ConfiguredTaskCoreOptions {
  readonly sessionName?: string;
  readonly baseUrl?: string;
  readonly fetch?: typeof fetch;
  readonly requestTimeoutMs?: number;
  /** Fresh-lifetime registration after accepted state loss; the extension uses it only for structured relay-loss evidence. */
  readonly rebind?: boolean;
}

/** Normal extension transport. Never probe/fallback to the legacy durable relay. */
export async function createConfiguredTaskCore(options: ConfiguredTaskCoreOptions = {}, signal?: AbortSignal): Promise<OwnedTaskCore> {
  const sessionName = options.sessionName ?? process.env.WOLFPACK_SESSION_NAME;
  if (!sessionName?.trim()) throw new TaskProtocolError("RELAY_UNAVAILABLE", "WOLFPACK_SESSION_NAME is required for the Wolfpack task adapter");
  const port = process.env.WOLFPACK_PORT ?? "18790";
  if (options.baseUrl === undefined && (!/^[0-9]+$/.test(port) || Number(port) < 1 || Number(port) > 65535)) throw new TypeError("invalid WOLFPACK_PORT");
  const base = new URL(options.baseUrl ?? `http://127.0.0.1:${port}`);
  if (base.username || base.password || base.search || base.hash || base.pathname !== "/") throw new TypeError("relay base URL must be an origin");
  const url = new URL("/api/task-relay/volatile-v1", base).href;
  const store = createTaskStore();
  let session: ReturnType<typeof createVolatileTaskSession> | undefined;
  try {
    const requestFetch = options.fetch ?? fetch;
    const authenticated = Object.assign((input: Parameters<typeof fetch>[0], init?: RequestInit) => requestFetch(input, { ...init, headers: wolfpackLocalHeaders(url, init?.headers) }), { preconnect: requestFetch.preconnect }) as typeof fetch;
    session = createVolatileTaskSession({ url, callerSession: sessionName, store, fetch: authenticated, ...(options.requestTimeoutMs !== undefined && { requestTimeoutMs: options.requestTimeoutMs }) });
    const core = await (options.rebind ? session.rebind(signal) : session.connect(signal));
    const active = new Set<Promise<unknown>>();
    let closed = false, closing: Promise<void> | undefined;
    const owned = { ...core } as OwnedTaskCore;
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
            flush: (signal?: AbortSignal): Promise<void> => {
              if (closed) throw new TaskProtocolError("RELAY_CLOSED", "task session is closed", { retryable: false });
              return track(request.flush(signal));
            },
            // A closed session drops its store; there is nothing left to withdraw.
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
