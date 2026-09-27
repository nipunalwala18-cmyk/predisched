import { useMemo, useSyncExternalStore } from "react";
import type { StreamEvent, WorkerLive } from "../api/types";
import { throttle } from "./throttle";

/** A vertical marker on every time-series chart: chaos, elections, failures, scaling (§15.7). */
export interface Marker {
  t: number;
  label: string;
  kind: "chaos" | "election" | "failure" | "admin" | "scale" | "drift";
}

export interface MetricsPoint {
  t: number;
  queueDepth: number;
  running: number;
  workers: WorkerLive[];
}

const WINDOW_MS = 5 * 60 * 1000; // the bounded client buffer (spec §15.11)
const MAX_EVENTS = 3000;

const MARKER_KINDS: Record<string, Marker["kind"]> = {
  CHAOS: "chaos",
  LEADER_ELECTED: "election",
  WORKER_DEAD: "failure",
  ADMIN: "admin",
  SCALE_UP: "scale",
  SCALE_DOWN: "scale",
  DRIFT: "drift",
};

function markerLabel(e: StreamEvent): string {
  switch (e.type) {
    case "CHAOS":
      return `${String(e.action ?? "chaos")} ${String(e.node ?? "")}`.trim();
    case "LEADER_ELECTED":
      return `leader ${String(e.leader ?? e.node)}`;
    case "WORKER_DEAD":
      return `${String(e.worker)} dead`;
    case "ADMIN":
      return String(e.action ?? "admin");
    default:
      return e.type.toLowerCase().replace("_", " ");
  }
}

/** Everything the live stream delivered in the last five minutes. */
export class StreamStore {
  events: StreamEvent[] = [];
  decisions: StreamEvent[] = [];
  metrics: MetricsPoint[] = [];
  markers: Marker[] = [];
  version = 0;
  /** Bumped only when the markers change, so charts can skip the other stream updates. */
  markersVersion = 0;
  private listeners = new Set<() => void>();
  private notifyThrottled = throttle(() => this.emit(), 4);

  ingest(topic: string, batch: unknown[], now = Date.now()) {
    if (topic === "metrics") {
      for (const m of batch as Record<string, any>[]) {
        this.metrics.push({ t: Number(m.atMs ?? now), queueDepth: m.queueDepth ?? 0,
          running: m.running ?? 0, workers: m.workers ?? [] });
      }
    } else if (topic === "decisions") {
      this.decisions.push(...(batch as StreamEvent[]));
    } else {
      for (const e of batch as StreamEvent[]) {
        this.events.push(e);
        const kind = MARKER_KINDS[e.type];
        if (kind) {
          const t = Number(e.wall_ms ?? now);
          // Every follower logs the same election: one marker per second is enough.
          const dup = this.markers.some((m) => m.kind === kind && Math.abs(m.t - t) < 1000
            && (kind === "election" || m.label === markerLabel(e)));
          if (!dup) { this.markers.push({ t, label: markerLabel(e), kind }); this.markersVersion++; }
        }
      }
    }
    this.trim(now);
    this.version++;
    this.notifyThrottled();
  }

  private leader: number | null = null;

  /**
   * The leader the overview reports. The API's event stream follows the primary, so the new
   * primary's LEADER_ELECTED is usually logged before the stream reconnects to it; a change seen
   * here marks the election instead, unless the stream already did.
   */
  noteLeader(leaderId: number | null | undefined, now = Date.now()) {
    if (leaderId == null) return;
    const previous = this.leader;
    this.leader = leaderId;
    if (previous == null || previous === leaderId) return;
    if (this.markers.some((m) => m.kind === "election" && now - m.t < 15_000)) return;
    this.markers.push({ t: now, label: `leader scheduler-${leaderId}`, kind: "election" });
    this.markersVersion++;
    this.version++;
    this.notifyThrottled();
  }

  trim(now = Date.now()) {
    const cutoff = now - WINDOW_MS;
    this.metrics = this.metrics.filter((m) => m.t >= cutoff);
    const kept = this.markers.filter((m) => m.t >= cutoff);
    if (kept.length !== this.markers.length) this.markersVersion++;
    this.markers = kept;
    this.events = this.events.filter((e) => Number(e.wall_ms ?? now) >= cutoff).slice(-MAX_EVENTS);
    this.decisions = this.decisions.slice(-500);
  }

  subscribe = (listener: () => void) => {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  };

  getVersion = () => this.version;

  private emit() {
    this.listeners.forEach((l) => l());
  }
}

export const stream = new StreamStore();

/** Re-renders at most ~4 times a second, whatever the stream rate. */
export function useStreamVersion(): number {
  return useSyncExternalStore(stream.subscribe, stream.getVersion);
}

/**
 * The markers since a time, as an array that keeps its identity until a marker is added or
 * expires, so memoised charts do not redraw on every stream update. `since` is bucketed to 10 s.
 */
export function useMarkers(since: number): Marker[] {
  useStreamVersion();
  const bucket = Math.floor(since / 10_000) * 10_000;
  const version = stream.markersVersion;
  // eslint-disable-next-line react-hooks/exhaustive-deps
  return useMemo(() => stream.markers.filter((m) => m.t >= bucket), [version, bucket]);
}
