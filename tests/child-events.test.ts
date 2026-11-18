import { expect, test } from "bun:test";
import { createChildEventQueue, renderChildEvents } from "../src/child-events";
import type { ChildEvent } from "../src/child-events";

function event(seq: number, kind: ChildEvent["event"]): ChildEvent {
  return { eventId: `child:boot:${seq}`, bootId: "boot", seq, childSessionId: "child", childName: "worker",
    parentSessionId: "parent", event: kind, state: "done", observedAt: "2026-11-16T00:00:00Z" };
}

test("actionable and passive child queues have independent bounds and drop counts", () => {
  const queue = createChildEventQueue();
  for (let i = 0; i < 60; i++) {
    queue.accept({ kind: "event", envelopeId: `done-${i}`, event: event(i, "done") });
    if (i < 52) queue.accept({ kind: "event", envelopeId: `error-${i}`, event: event(i + 100, "error") });
  }
  const records = queue.takeRecords();
  expect(records.events).toHaveLength(50);
  expect(records.dropped).toBe(10);
  expect(records.events[0]!.seq).toBe(10);
  expect(queue.hasEvents()).toBe(true);
  const actionable = queue.takeEvents();
  expect(actionable.events).toHaveLength(50);
  expect(actionable.dropped).toBe(2);
  expect(actionable.events[0]!.seq).toBe(102);
  expect(queue.hasEvents()).toBe(false);
  expect(queue.takeRecords()).toEqual({ events: [], dropped: 0 });
  expect(queue.takeEvents()).toEqual({ events: [], dropped: 0 });
});

test.each(["done", "blocked", "error", "message", "exited"] as const)("child rendering preserves main's concise format: %s", kind => {
  const rendered = renderChildEvents({ events: [event(1, kind)], dropped: 0 });
  expect(rendered).toBe([
    "## wolfpack child events (1)",
    `- child "worker" (session "child"): event ${kind}, state done${kind === "done" ? ", runtime settled" : ""}`,
    "",
    "Full records: `wolfpack agent inbox --json`.",
  ].join("\n"));
});
