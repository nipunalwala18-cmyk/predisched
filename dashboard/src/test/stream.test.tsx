import { act, renderHook } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { StreamStore } from "../stream/store";
import { throttle } from "../stream/throttle";
import { useStomp, type StompLike } from "../stream/useStomp";

/** A STOMP client whose connection the test drives. */
class FakeClient implements StompLike {
  onConnect: (frame: unknown) => void = () => {};
  onWebSocketClose: (event: unknown) => void = () => {};
  onStompError: (frame: unknown) => void = () => {};
  subscriptions = new Map<string, (m: { body: string }) => void>();
  activated = 0;
  deactivated = 0;
  subscribe(destination: string, callback: (m: { body: string }) => void) {
    this.subscriptions.set(destination, callback);
    return {};
  }
  activate() { this.activated++; }
  deactivate() { this.deactivated++; }
}

describe("useStomp", () => {
  it("goes live, reconnects after a drop, and routes batches to topics", () => {
    const fake = new FakeClient();
    const received: [string, unknown[]][] = [];
    const factory = () => fake;
    const { result, unmount } = renderHook(() =>
      useStomp("ws://x/ws/stream", ["events", "metrics"], (t, items) => received.push([t, items]), factory));
    expect(result.current.status).toBe("connecting");
    expect(fake.activated).toBe(1);

    act(() => fake.onConnect({}));
    expect(result.current.status).toBe("live");
    expect([...fake.subscriptions.keys()]).toEqual(["/topic/events", "/topic/metrics"]);

    act(() => fake.subscriptions.get("/topic/events")!({ body: JSON.stringify([{ type: "CHAOS" }, { type: "ADMIN" }]) }));
    expect(received).toEqual([["events", [{ type: "CHAOS" }, { type: "ADMIN" }]]]);

    // The socket drops: the client retries by itself; the hook shows it and counts the reconnect.
    act(() => fake.onWebSocketClose({}));
    expect(result.current.status).toBe("reconnecting");
    act(() => fake.onConnect({}));
    expect(result.current.status).toBe("live");
    expect(result.current.reconnects).toBe(1);

    unmount();
    expect(fake.deactivated).toBe(1);
  });

  it("reports offline when it never connected", () => {
    const fake = new FakeClient();
    const { result } = renderHook(() => useStomp("ws://x", ["events"], () => {}, () => fake));
    act(() => fake.onWebSocketClose({}));
    expect(result.current.status).toBe("offline");
  });
});

describe("throttle", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it("caps the update rate and keeps the last update", () => {
    const fn = vi.fn();
    const t = throttle(fn, 4);
    // 200 calls over 2 s: at most 4 a second (plus the first).
    for (let i = 0; i < 200; i++) {
      t();
      vi.advanceTimersByTime(10);
    }
    vi.advanceTimersByTime(500);
    expect(fn.mock.calls.length).toBeLessThanOrEqual(9);
    expect(fn.mock.calls.length).toBeGreaterThanOrEqual(8);
  });
});

describe("StreamStore", () => {
  it("keeps five minutes, turns chaos and elections into markers, one election marker per second", () => {
    const store = new StreamStore();
    const now = 1_000_000_000;
    store.ingest("events", [
      { type: "LEADER_ELECTED", node: "scheduler-1", wall_ms: now - 1000, lamport: 1, physical_ms: 0, task_id: "", trace: "" },
      { type: "LEADER_ELECTED", node: "scheduler-2", wall_ms: now - 900, lamport: 1, physical_ms: 0, task_id: "", trace: "" },
      { type: "CHAOS", action: "crash", node: "scheduler-5", wall_ms: now - 2000, lamport: 2, physical_ms: 0, task_id: "", trace: "" },
      { type: "DISPATCH", node: "scheduler-1", wall_ms: now - 400_000, lamport: 3, physical_ms: 0, task_id: "t", trace: "" },
    ], now);
    expect(store.markers.map((m) => m.kind).sort()).toEqual(["chaos", "election"]);
    expect(store.events.map((e) => e.type)).not.toContain("DISPATCH");  // older than 5 min
    store.ingest("metrics", [{ atMs: now, queueDepth: 3, running: 2, workers: [] }], now);
    expect(store.metrics).toHaveLength(1);
  });

  it("marks a leader change the event stream missed, once", () => {
    const store = new StreamStore();
    const now = 2_000_000_000;
    store.noteLeader(5, now);
    expect(store.markers).toHaveLength(0);          // the first leader seen is not an election
    store.noteLeader(5, now + 1000);
    store.noteLeader(4, now + 5000);
    expect(store.markers).toEqual([{ t: now + 5000, label: "leader scheduler-4", kind: "election" }]);
    store.noteLeader(3, now + 8000);                 // within 15 s of the last election marker
    expect(store.markers).toHaveLength(1);
  });
});
