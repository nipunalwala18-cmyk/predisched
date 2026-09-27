import type { Overview } from "./api/types";
import { kpiPoint, type KpiPoint } from "./components/KpiStrip";

const WINDOW_MS = 5 * 60 * 1000;

/** Polled overview samples, bounded to five minutes: KPI sparklines and the leader timeline. */
class History {
  kpis: KpiPoint[] = [];
  leaders: { t: number; leader: number | null }[] = [];

  record(o: Overview, t = Date.now()) {
    this.kpis.push(kpiPoint(o, t));
    this.leaders.push({ t, leader: o.cluster?.leaderId ?? null });
    const cutoff = t - WINDOW_MS;
    this.kpis = this.kpis.filter((k) => k.t >= cutoff);
    this.leaders = this.leaders.filter((l) => l.t >= cutoff);
  }
}

export const history = new History();
