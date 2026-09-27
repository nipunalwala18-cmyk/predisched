import type { Overview } from "../api/types";
import { Sparkline } from "./ui/charts";

export interface KpiPoint {
  t: number;
  tasksPerSecond: number;
  queueDepth: number | null;
  meanLatencyMs: number | null;
  p95LatencyMs: number | null;
  activeWorkers: number | null;
  slaCompliancePct: number | null;
}

export function kpiPoint(o: Overview, t = Date.now()): KpiPoint {
  return { t, ...o.kpis };
}

const fmt = (v: number | null | undefined, digits = 0, unit = "") =>
  v === null || v === undefined || !Number.isFinite(v) ? "–" : `${v.toFixed(digits)}${unit}`;

/** Six headline numbers with their five-minute trend (spec §15.2). */
export function KpiStrip({ overview, history }: { overview: Overview; history: KpiPoint[] }) {
  const k = overview.kpis;
  const cards: { label: string; value: string; series: number[]; hint: string }[] = [
    { label: "Tasks / s", value: fmt(k.tasksPerSecond, 2), hint: `completed, last ${k.windowSeconds} s`,
      series: history.map((h) => h.tasksPerSecond) },
    { label: "Queue depth", value: fmt(k.queueDepth), hint: "waiting in the scheduler",
      series: history.map((h) => h.queueDepth ?? 0) },
    { label: "Mean latency", value: fmt(k.meanLatencyMs, 0, " ms"), hint: "submit → completion",
      series: history.map((h) => h.meanLatencyMs ?? 0) },
    { label: "P95 latency", value: fmt(k.p95LatencyMs, 0, " ms"), hint: "tail",
      series: history.map((h) => h.p95LatencyMs ?? 0) },
    { label: "Active workers", value: fmt(k.activeWorkers), hint: "healthy",
      series: history.map((h) => h.activeWorkers ?? 0) },
    { label: "SLA compliance", value: fmt(k.slaCompliancePct, 1, " %"), hint: "of tasks with a deadline",
      series: history.map((h) => h.slaCompliancePct ?? 0) },
  ];
  return (
    <div className="grid grid-cols-2 gap-3 md:grid-cols-3 xl:grid-cols-6" data-testid="kpi-strip">
      {cards.map((c) => (
        <div key={c.label} className="rounded-lg border border-rule bg-panel p-3">
          <div className="text-xs text-muted">{c.label}</div>
          <div className="tabular mt-1 text-2xl font-semibold text-ink">{c.value}</div>
          <Sparkline data={c.series} />
          <div className="text-[11px] text-muted">{c.hint}</div>
        </div>
      ))}
    </div>
  );
}
